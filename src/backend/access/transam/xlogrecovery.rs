//! WAL crash-recovery driver. Translated from backend/access/transam/xlogrecovery.c.
//!
//! The read/replay side of the WAL: after a crash the running system's control
//! file is not in the cleanly-shut-down state, so on startup the recovery loop
//! reads the WAL from the last checkpoint's redo point to the end of valid WAL
//! and re-applies every record to the pages it changed. Each record is dispatched
//! by its resource-manager id ([`RmgrId`]) to that manager's redo routine (the
//! `RmgrTable`/`rm_redo` role): heap, btree, transaction, clog, and the xlog
//! checkpoint records. Applying a heap/btree record re-does the page change (or
//! restores its full-page image); applying a transaction commit marks the xid
//! committed in clog, which is what makes the replayed rows visible.
//!
//! The redo loop is bounded by the reader itself: `read_record` stops at the first
//! torn or invalid record (end of the durable WAL), so a partial final record
//! cannot spin the loop. The reader is a synchronous CPU-only decoder fed by a
//! blocking segment-file page reader; the per-record apply is async (buffer and
//! clog I/O), so records are read one at a time and applied between reads. No
//! buffer or clog lock is held across an `.await`.

use std::sync::Arc;

use crate::access::rmgrlist::RmgrId;
use crate::access::xlogdefs::{XLogRecPtr, INVALID_XLOG_REC_PTR};
use crate::access::xlogreader::DecodedXLogRecord;
use crate::backend::access::transam::xlog::XLogCtl;
use crate::backend::access::transam::xlogreader::XLogReader;
use crate::catalog::pg_control::{DBState, XLOG_CHECKPOINT_ONLINE, XLOG_CHECKPOINT_SHUTDOWN};
use crate::shared_state::SharedState;

/// Hard cap on records replayed in one recovery run. The reader already stops at
/// the first torn/invalid record (end of durable WAL); this additionally bounds a
/// pathological reader that never advances.
const MAX_RECORDS: u64 = 100_000_000;

/// Outcome of a recovery run: where replay ended and how many records applied.
#[derive(Debug, Clone, Copy)]
pub struct RecoveryResult {
    /// LSN just past the last successfully replayed record.
    pub end_of_log: XLogRecPtr,
    /// Number of records dispatched to a redo routine.
    pub records_replayed: u64,
}

/// PG `StartupXLOG`: the recovery decision + entry point.
///
/// Reads the control file; if the cluster was not cleanly shut down (crash), runs
/// [`perform_wal_recovery`] from the last checkpoint's redo point to the end of
/// valid WAL, then records the recovered state. Returns the recovery result, or
/// `None` when no recovery was needed (clean shutdown) or possible (no WAL).
///
/// `start_lsn_override` lets a caller (a test, or a cluster with no checkpoint
/// machinery yet) force the redo start point; when `None`, the redo point comes
/// from the control file's checkpoint copy.
pub async fn startup_xlog(
    shared: &Arc<SharedState>,
    start_lsn_override: Option<XLogRecPtr>,
) -> Option<RecoveryResult> {
    let xlog = shared.xlog();
    let control = xlog.read_control_file().await;

    // Crash detection: recovery is needed unless the control file says the cluster
    // was cleanly shut down. A missing control file (fresh cluster) also means
    // "not cleanly shut down" -- but with no checkpoint we only recover if the
    // caller supplied a start LSN.
    let was_shutdown = matches!(
        control.as_ref().map(|c| c.state),
        Some(DBState::SHUTDOWNED | DBState::SHUTDOWNED_IN_RECOVERY)
    );

    let redo_point = start_lsn_override
        .or_else(|| control.as_ref().map(|c| c.checkPointCopy.redo))
        .filter(|p| p.is_valid());

    if was_shutdown && start_lsn_override.is_none() {
        // Clean shutdown: no redo needed.
        return None;
    }
    let redo_point = redo_point?;

    // Enter crash recovery, run the redo loop, then mark the cluster in production.
    xlog.write_control_file(DBState::IN_CRASH_RECOVERY, redo_point, redo_point)
        .await;
    let result = perform_wal_recovery(shared, redo_point).await;
    xlog.write_control_file(DBState::IN_PRODUCTION, redo_point, result.end_of_log)
        .await;
    Some(result)
}

/// PG `PerformWalRecovery`: the redo loop. Reads records from `redo_point` to the
/// end of valid WAL, dispatching each to its resource manager's redo routine and
/// tracking the replay LSN. Stops cleanly at the first torn/invalid record.
pub async fn perform_wal_recovery(
    shared: &Arc<SharedState>,
    redo_point: XLogRecPtr,
) -> RecoveryResult {
    let xlog = shared.xlog();
    let wal_seg_size = xlog.wal_segment_size();
    let Some(page_read) = xlog.make_recovery_page_reader() else {
        return RecoveryResult { end_of_log: redo_point, records_replayed: 0 };
    };

    let mut reader = XLogReader::new(wal_seg_size, page_read);
    reader.set_system_identifier(0); // skip the sysid cross-check in recovery
    reader.begin_read(redo_point);

    let mut end_of_log = redo_point;
    let mut count = 0u64;

    // The reader stops at the first invalid record (end of durable WAL), so the
    // loop is bounded; MAX_RECORDS additionally guards a non-advancing reader.
    while count < MAX_RECORDS {
        // Clean end of WAL (torn/absent final record) or a decode error: stop.
        let Ok(Some(rec)) = reader.read_record() else {
            break;
        };
        let decoded = rec.clone();
        apply_wal_record(shared, &decoded).await;
        end_of_log = decoded.next_lsn;
        count += 1;
    }

    let _ = INVALID_XLOG_REC_PTR;
    RecoveryResult { end_of_log, records_replayed: count }
}

/// PG `ApplyWalRecord` + the `RmgrTable[rmid].rm_redo` dispatch: hand a decoded
/// record to the redo routine of its resource manager. Unimplemented managers are
/// staged (a catchable error), not silently skipped.
pub async fn apply_wal_record(shared: &Arc<SharedState>, record: &DecodedXLogRecord) {
    let rmid = record.header.rmid;
    if rmid == RmgrId::Heap as u8 {
        crate::backend::access::heap::heapam_xlog::heap_redo(shared, record).await;
    } else if rmid == RmgrId::Heap2 as u8 {
        crate::backend::access::heap::heapam_xlog::heap2_redo(shared, record).await;
    } else if rmid == RmgrId::Btree as u8 {
        crate::backend::access::nbtree::nbtxlog::btree_redo(shared, record).await;
    } else if rmid == RmgrId::Xact as u8 {
        crate::backend::access::transam::xact::xact_redo_async(shared, record).await;
    } else if rmid == RmgrId::Clog as u8 {
        let main = record.get_data().unwrap_or_default();
        crate::backend::access::transam::clog::clog_redo(shared.clog(), record.info(), main).await;
    } else if rmid == RmgrId::Xlog as u8 {
        xlog_redo(record);
    } else {
        // Standby, smgr, multixact, and the other managers are not exercised by
        // the foundation's write path; reaching one is staged.
        crate::elog!(
            crate::utils::elog::ERROR,
            format!("apply_wal_record: no redo routine for rmid {rmid} (staged)")
        );
    }
}

/// PG `xlog_redo`: RM_XLOG records. Checkpoint records carry no page changes to
/// re-apply during replay (the checkpoint machinery consumes them separately);
/// full-page-image (`XLOG_FPI`) records are restored via the block-0 image, which
/// the reader/redo-buffer path already handles when a caller reads the block.
/// Here the checkpoint/switch/noop records are accepted as no-ops; an unhandled
/// info raises a catchable error.
fn xlog_redo(record: &DecodedXLogRecord) {
    use crate::catalog::pg_control::{
        XLOG_CHECKPOINT_REDO, XLOG_END_OF_RECOVERY, XLOG_FPI, XLOG_FPI_FOR_HINT, XLOG_NEXTOID,
        XLOG_NOOP, XLOG_PARAMETER_CHANGE, XLOG_SWITCH,
    };
    let info = record.info() & 0xF0;
    // Checkpoint/switch/noop/param/nextoid records carry no page change to
    // re-apply during replay (they are consumed by the checkpoint/xid machinery,
    // not by page redo); FPI records are restored via their block-0 image when a
    // caller reads the block. All are no-ops here.
    match info {
        XLOG_CHECKPOINT_SHUTDOWN | XLOG_CHECKPOINT_ONLINE | XLOG_NOOP | XLOG_SWITCH | XLOG_FPI
        | XLOG_FPI_FOR_HINT | XLOG_CHECKPOINT_REDO | XLOG_NEXTOID | XLOG_PARAMETER_CHANGE
        | XLOG_END_OF_RECOVERY => {}
        other => crate::elog!(
            crate::utils::elog::ERROR,
            format!("xlog_redo: unimplemented xlog opcode {other:#x} (staged)")
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::access::clog::XidStatus;
    use crate::access::xlogdefs::INVALID_XLOG_REC_PTR;
    use crate::backend::access::common::heaptuple::heap_form_tuple;
    use crate::backend::access::heap::heapam::heap_insert;
    use crate::backend::access::transam::xact::{
        CommitTransactionCommand, GetCurrentTransactionId, StartTransactionCommand,
    };
    use crate::backend::access::transam::xloginsert::with_insertion;
    use crate::backend::utils::time::combocid::combocid_scope;
    use crate::backend::utils::time::snapmgr::snapmgr_scope;
    use crate::catalog::pg_class::{RELKIND_RELATION, RELPERSISTENCE_PERMANENT};
    use crate::common::relpath::ForkNumber;
    use crate::postgres::Int32GetDatum;
    use crate::postgres_ext::Oid;
    use crate::shared_state::{SharedState, SharedStateConfig};
    use crate::storage::relfilelocator::RelFileLocator;
    use crate::utils::rel::{LockInfoData, LockRelId, RelationData};
    use std::sync::atomic::{AtomicU32, Ordering};

    static COUNTER: AtomicU32 = AtomicU32::new(0);

    /// A shared temp data dir shared by two SharedState "instances" (crash + restart).
    fn temp_data_dir(tag: &str) -> std::path::PathBuf {
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!("pepperdb-recovery-{tag}-{}-{}", std::process::id(), n))
    }

    fn make_shared(dir: &std::path::Path) -> Arc<SharedState> {
        let _ = std::fs::create_dir_all(dir.join(crate::access::xlog_internal::XLOGDIR));
        let _ = std::fs::create_dir_all(dir.join("global"));
        let _ = std::fs::create_dir_all(dir.join("base").join("90000"));
        SharedState::new(SharedStateConfig {
            data_dir: Some(dir.to_string_lossy().into_owned()),
            nbuffers: 64,
            ..Default::default()
        })
    }

    fn rloc(rel: u32) -> RelFileLocator {
        RelFileLocator { spcOid: Oid::new(1663), dbOid: Oid::new(90000), relNumber: Oid::new(80000 + rel) }
    }

    fn two_int4_desc() -> crate::access::tupdesc::TupleDesc {
        use crate::access::tupdesc::TupleDescData;
        const INT4OID: Oid = Oid::new(23);
        let mut d = TupleDescData::create_template(2);
        d.init_builtin_entry(1, "a", INT4OID, -1, 0);
        d.init_builtin_entry(2, "b", INT4OID, -1, 0);
        Arc::new(d)
    }

    fn make_relation(locator: RelFileLocator) -> Arc<RelationData> {
        use crate::catalog::pg_class::FormData_pg_class;
        // SAFETY: FormData_pg_class is repr(C) POD; all-zero is valid, we patch it.
        let mut form: Box<FormData_pg_class> = Box::new(unsafe { core::mem::zeroed() });
        form.relkind = RELKIND_RELATION;
        form.relpersistence = RELPERSISTENCE_PERMANENT;
        form.relnatts = 2;
        form.relam = Oid::new(2);
        let mut rel = RelationData::blank();
        rel.rd_locator = locator;
        rel.rd_refcnt.store(1, Ordering::Relaxed);
        rel.rd_isvalid.store(true, Ordering::Relaxed);
        rel.rd_rel = Some(form);
        rel.rd_att = Some(two_int4_desc());
        rel.rd_id = locator.relNumber;
        rel.rd_lockInfo = LockInfoData {
            lockRelId: LockRelId { relId: locator.relNumber, dbId: locator.dbOid },
        };
        rel.rd_amhandler = Oid::new(2);
        Arc::new(rel)
    }

    async fn create_main_fork(shared: &Arc<SharedState>, locator: RelFileLocator) {
        let mut smgr = crate::storage::smgr::SmgrRelation::open(
            locator,
            crate::storage::procnumber::INVALID_PROC_NUMBER,
        );
        smgr.create(shared, ForkNumber::MAIN_FORKNUM, false).await;
    }

    /// Run `f` inside the full per-task scope stack heap_insert/commit rely on.
    async fn in_scopes<F, Fut, T>(shared: Arc<SharedState>, f: F) -> T
    where
        F: FnOnce(Arc<SharedState>) -> Fut,
        Fut: std::future::Future<Output = T>,
    {
        let sess = Arc::new(crate::session::Session::new(crate::miscadmin::BackendType::BACKEND));
        let owner = crate::backend::utils::resowner::resowner::ResourceOwner::create(None, "RecoveryTest");
        crate::session::scope(
            sess,
            crate::backend::utils::resowner::resowner::scope(
                owner,
                crate::backend::access::transam::xact::xact_scope(snapmgr_scope(combocid_scope(
                    with_insertion(f(shared)),
                ))),
            ),
        )
        .await
    }

    /// Read raw block 0 of a relation's main fork straight from the on-disk file
    /// (bypassing the buffer pool), returning the page bytes or None if absent.
    fn read_disk_block0(dir: &std::path::Path, loc: RelFileLocator) -> Option<Vec<u8>> {
        let path = dir
            .join("base")
            .join(loc.dbOid.get().to_string())
            .join(loc.relNumber.get().to_string());
        let bytes = std::fs::read(&path).ok()?;
        if bytes.len() < 8192 {
            return None;
        }
        Some(bytes[..8192].to_vec())
    }

    /// Search a page's bytes for a big-endian... no: the tuple stores int4 in
    /// native (LE) order. Return true if both little-endian i32 values appear.
    fn page_contains_pair(page: &[u8], a: i32, b: i32) -> bool {
        let ab = a.to_ne_bytes();
        let bb = b.to_ne_bytes();
        page.windows(4).any(|w| w == ab) && page.windows(4).any(|w| w == bb)
    }

    /// THE MILESTONE: insert + commit (WAL flushed) -> crash (drop instance
    /// WITHOUT flushing the dirty heap page) -> restart on the SAME data dir ->
    /// StartupXLOG replays the WAL -> the inserted row is present on the page and
    /// its commit is recorded in clog.
    #[tokio::test(flavor = "multi_thread")]
    async fn crash_recovery_replays_insert_and_commit() {
        let dir = temp_data_dir("e2e");
        let loc = rloc(1);

        // --- Instance 1: insert + commit, flush WAL, then "crash" (drop). ---
        let (redo_start, committed_xid) = {
            let shared = make_shared(&dir);
            shared.clog().boot_strap_clog().await;
            let result = Box::pin(in_scopes(shared.clone(), |shared| async move {
                StartTransactionCommand(&shared).await;
                create_main_fork(&shared, loc).await;
                let rel = make_relation(loc);

                // Redo must start at the first record we write.
                let redo_start = shared.xlog().get_xlog_insert_rec_ptr();

                let desc = rel.rd_att.clone().unwrap();
                let values = [Int32GetDatum(1234), Int32GetDatum(5678)];
                let isnull = [false, false];
                let mut tuple = heap_form_tuple(&desc, &values, &isnull);
                let cid = crate::backend::access::transam::xact::GetCurrentCommandId(true);
                heap_insert(&shared, &rel, &mut tuple, cid, 0).await;
                let xid = GetCurrentTransactionId(&shared).await;

                // Commit, then flush the WAL to disk (durability). A crash after
                // this point must still recover the row.
                CommitTransactionCommand(&shared).await;
                let end = shared.xlog().get_xlog_insert_rec_ptr();
                shared.xlog().xlog_flush(end).await;
                (redo_start, xid)
            }))
            .await;

            // The heap page the insert extended was written to disk ZEROED at
            // extend time; the modified page lives only in the (now-dropped)
            // buffer pool -- i.e. the row is NOT on disk, only in the WAL.
            let page = read_disk_block0(&dir, loc).expect("fork block 0 on disk");
            assert!(
                !page_contains_pair(&page, 1234, 5678),
                "row must NOT be on the heap page before recovery (only in WAL)"
            );
            result
            // shared dropped here == crash without checkpoint / clean shutdown.
        };

        // --- Instance 2: restart on the SAME dir, run recovery. ---
        let shared2 = make_shared(&dir);
        shared2.clog().boot_strap_clog().await;

        let recovery = Box::pin(in_scopes(shared2.clone(), |shared2| async move {
            let r = startup_xlog(&shared2, Some(redo_start))
                .await
                .expect("recovery should run (crash state)");
            assert!(r.records_replayed >= 2, "expected insert + commit records replayed, got {}", r.records_replayed);
            r
        }))
        .await;
        assert!(recovery.end_of_log.0 > redo_start.0, "replay LSN advanced");

        // The row is now on the heap page (heap_redo re-applied the insert).
        let page = read_disk_after_flush(&shared2, loc).await;
        assert!(
            page_contains_pair(&page, 1234, 5678),
            "row must be present on the heap page after redo"
        );

        // The commit is recorded in clog (xact_redo marked it committed).
        let (status, _) = shared2.clog().get_status(committed_xid).await;
        assert_eq!(status, XidStatus::Committed, "replayed commit must be in clog");

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Flush block 0 of a relation from the buffer pool to disk, then read it back
    /// from the file (so we observe the redo-applied page image).
    async fn read_disk_after_flush(shared: &Arc<SharedState>, loc: RelFileLocator) -> Vec<u8> {
        // The redo pinned+dirtied block 0 in the buffer pool. Flush it so the disk
        // file reflects the replayed change, then read the file.
        let mut smgr = crate::storage::smgr::SmgrRelation::open(
            loc,
            crate::storage::procnumber::INVALID_PROC_NUMBER,
        );
        let buffer = crate::backend::storage::buffer::bufmgr::read_buffer_common(
            shared,
            &mut smgr,
            RELPERSISTENCE_PERMANENT,
            ForkNumber::MAIN_FORKNUM,
            0,
            crate::storage::bufmgr::ReadBufferMode::NORMAL,
            None,
        )
        .await;
        let pool = shared.buffers();
        let buf_id = buffer.as_global().expect("shared buffer") as i32;
        pool.flush_buffer(shared, buf_id, Some(&mut smgr)).await;
        pool.release_buffer(buffer);
        read_disk_block0_for(shared, loc)
    }

    fn read_disk_block0_for(shared: &Arc<SharedState>, loc: RelFileLocator) -> Vec<u8> {
        let dir = shared.config().data_dir().expect("data dir");
        let dir = std::path::PathBuf::from(dir);
        read_disk_block0(&dir, loc).expect("fork block 0 on disk after flush")
    }

    /// The tuple offset (`lp_off`) of the first line pointer on a heap page, read
    /// from the page bytes: page header is 24 bytes, then ItemIdData entries; the
    /// first ItemId's low 15 bits are `lp_off`.
    fn first_tuple_offset(page: &[u8]) -> usize {
        let item_id = u32::from_ne_bytes([page[24], page[25], page[26], page[27]]);
        (item_id & 0x7FFF) as usize
    }

    /// Crash recovery of a heap DELETE: insert+commit a row, then delete+commit it,
    /// crash without flushing the dirty page, restart and replay. The recovered
    /// page must show the tuple's xmax stamped with the deleting xid (heap_xlog_
    /// delete applied) and both xacts committed in clog.
    #[tokio::test(flavor = "multi_thread")]
    async fn crash_recovery_replays_delete() {
        let dir = temp_data_dir("del");
        let loc = rloc(3);

        let (redo_start, del_xid) = {
            let shared = make_shared(&dir);
            shared.clog().boot_strap_clog().await;
            Box::pin(in_scopes(shared.clone(), |shared| async move {
                let redo_start = shared.xlog().get_xlog_insert_rec_ptr();
                // Txn 1: insert + commit.
                StartTransactionCommand(&shared).await;
                create_main_fork(&shared, loc).await;
                let rel = make_relation(loc);
                let desc = rel.rd_att.clone().unwrap();
                let mut tuple = heap_form_tuple(&desc, &[Int32GetDatum(7), Int32GetDatum(7)], &[false, false]);
                let cid = crate::backend::access::transam::xact::GetCurrentCommandId(true);
                heap_insert(&shared, &rel, &mut tuple, cid, 0).await;
                let tid = tuple.t_self;
                CommitTransactionCommand(&shared).await;

                // Txn 2: delete the row + commit.
                StartTransactionCommand(&shared).await;
                let cid2 = crate::backend::access::transam::xact::GetCurrentCommandId(true);
                crate::backend::access::heap::heapam::heap_delete(
                    &shared, &rel, &tid, cid2, None, true, false,
                )
                .await;
                let del_xid = GetCurrentTransactionId(&shared).await;
                CommitTransactionCommand(&shared).await;
                shared.xlog().xlog_flush(shared.xlog().get_xlog_insert_rec_ptr()).await;
                (redo_start, del_xid)
            }))
            .await
        };

        // Restart + recover.
        let shared2 = make_shared(&dir);
        shared2.clog().boot_strap_clog().await;
        let page = Box::pin(in_scopes(shared2.clone(), |shared2| async move {
            let r = startup_xlog(&shared2, Some(redo_start)).await.expect("recover");
            assert!(r.records_replayed >= 4, "insert+commit+delete+commit, got {}", r.records_replayed);
            read_disk_after_flush(&shared2, loc).await
        }))
        .await;

        // The tuple is on the page and its xmax (bytes 4..8 of the header) equals
        // the deleting xid -- heap_xlog_delete stamped it during redo.
        assert!(page_contains_pair(&page, 7, 7), "row present after redo");
        let toff = first_tuple_offset(&page);
        let xmax = u32::from_ne_bytes([page[toff + 4], page[toff + 5], page[toff + 6], page[toff + 7]]);
        assert_eq!(xmax, del_xid.0, "delete redo must stamp xmax with the deleting xid");
        assert_eq!(shared2.clog().get_status(del_xid).await.0, XidStatus::Committed);

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The LSN guard: replaying a record whose page LSN already meets/exceeds the
    /// record's end LSN is a no-op (idempotent replay). Proven by replaying the
    /// SAME WAL twice and getting the same page.
    #[tokio::test(flavor = "multi_thread")]
    async fn recovery_is_idempotent_lsn_guard() {
        let dir = temp_data_dir("idem");
        let loc = rloc(2);

        let redo_start = {
            let shared = make_shared(&dir);
            shared.clog().boot_strap_clog().await;
            Box::pin(in_scopes(shared.clone(), |shared| async move {
                StartTransactionCommand(&shared).await;
                create_main_fork(&shared, loc).await;
                let rel = make_relation(loc);
                let redo_start = shared.xlog().get_xlog_insert_rec_ptr();
                let desc = rel.rd_att.clone().unwrap();
                let mut tuple = heap_form_tuple(&desc, &[Int32GetDatum(9), Int32GetDatum(9)], &[false, false]);
                let cid = crate::backend::access::transam::xact::GetCurrentCommandId(true);
                heap_insert(&shared, &rel, &mut tuple, cid, 0).await;
                CommitTransactionCommand(&shared).await;
                shared.xlog().xlog_flush(shared.xlog().get_xlog_insert_rec_ptr()).await;
                redo_start
            }))
            .await
        };

        // Recover once.
        let shared2 = make_shared(&dir);
        shared2.clog().boot_strap_clog().await;
        let page_after_first = Box::pin(in_scopes(shared2.clone(), |shared2| async move {
            startup_xlog(&shared2, Some(redo_start)).await.expect("recover once");
            read_disk_after_flush(&shared2, loc).await
        }))
        .await;
        assert!(page_contains_pair(&page_after_first, 9, 9));

        // Recover AGAIN over the same page: the LSN guard makes every record a
        // no-op, so the page is byte-identical (no double insert / corruption).
        let shared3 = make_shared(&dir);
        shared3.clog().boot_strap_clog().await;
        let page_after_second = Box::pin(in_scopes(shared3.clone(), |shared3| async move {
            let r = startup_xlog(&shared3, Some(redo_start)).await.expect("recover twice");
            // Records are still read + dispatched, but each apply is a no-op.
            assert!(r.records_replayed >= 1);
            read_disk_after_flush(&shared3, loc).await
        }))
        .await;
        assert_eq!(page_after_first, page_after_second, "idempotent replay: page unchanged");

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// StartupXLOG makes no changes when the control file says the cluster shut
    /// down cleanly (no override): recovery is skipped.
    #[tokio::test(flavor = "multi_thread")]
    async fn clean_shutdown_skips_recovery() {
        let dir = temp_data_dir("clean");
        let shared = make_shared(&dir);
        // Write a clean-shutdown control file.
        shared
            .xlog()
            .write_control_file(DBState::SHUTDOWNED, INVALID_XLOG_REC_PTR, INVALID_XLOG_REC_PTR)
            .await;
        let r = startup_xlog(&shared, None).await;
        assert!(r.is_none(), "clean shutdown -> no recovery");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A staged resource manager (e.g. a heap2 record) raises a catchable error
    /// during apply rather than silently dropping the change.
    #[tokio::test(flavor = "multi_thread")]
    async fn staged_heap2_redo_is_catchable() {
        use crate::access::rmgrlist::RmgrId;
        use crate::access::xlogreader::{DecodedBkpBlock, DecodedXLogRecord};
        use crate::access::xlogrecord::XLogRecord;

        let dir = temp_data_dir("staged");
        let shared = make_shared(&dir);

        // Hand-build a minimal HEAP2 record (a staged opcode).
        let rec = DecodedXLogRecord {
            size: 0,
            oversized: false,
            lsn: XLogRecPtr(0x1000),
            next_lsn: XLogRecPtr(0x1020),
            header: XLogRecord {
                tot_len: 24,
                xid: crate::c::TransactionId(5),
                prev: INVALID_XLOG_REC_PTR,
                info: crate::access::heapam_xlog::XLOG_HEAP2_PRUNE_ON_ACCESS,
                rmid: RmgrId::Heap2 as u8,
                crc: 0,
            },
            record_origin: crate::access::xlogdefs::RepOriginId(0),
            toplevel_xid: crate::c::InvalidTransactionId,
            main_data: None,
            main_data_len: 0,
            max_block_id: -1,
            blocks: Vec::<DecodedBkpBlock>::new(),
        };

        // The ERROR-level elog unwinds; a spawned task turns that unwind into a
        // JoinError (catchable), proving the staged path does NOT abort the process.
        let shared_c = shared.clone();
        let handle = tokio::spawn(async move {
            apply_wal_record(&shared_c, &rec).await;
        });
        let res = handle.await;
        assert!(res.is_err(), "staged heap2 redo must raise a catchable error");

        let _ = std::fs::remove_dir_all(&dir);
    }
}
