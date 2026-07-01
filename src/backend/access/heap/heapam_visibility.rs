//! Tuple visibility routines. Translated from
//! backend/access/heap/heapam_visibility.c.
//!
//! M2 scope (step 12): `HeapTupleSatisfiesMVCC`, the only satisfies-* variant
//! exercised by a forward seqscan under an MVCC snapshot. The other variants
//! (Self/Any/Toast/Dirty/HistoricMVCC/NonVacuumable) are grow guards added when
//! their callers land.
//!
//! Async coloring (rules.md s5): the visibility test runs on an already-pinned
//! (and, in the scan, share-locked) page, but it reaches the clog/subtrans SLRU
//! and pg_subtrans via `XidInMVCCSnapshot`/`transaction_id_did_commit`, which are
//! async leaves. So `HeapTupleSatisfiesMVCC` is `async`. The caller passes the
//! tuple header by shared reference (read-only); a buffer content lock is NEVER
//! held across these `.await`s -- the caller drops the content lock and keeps
//! only the pin before the visibility scan (PG's page-at-a-time contract).
//!
//! Hint bits (`SetHintBits`): PG opportunistically stashes XMIN/XMAX
//! committed/invalid bits on the page to skip future clog probes. That write
//! happens under a shared content lock via the un-WAL-logged "dirty hint"
//! mechanism. It is a pure optimization and never affects correctness, so it is
//! deferred here (`TODO(hint-bits)`); visibility is computed from clog/snapshot
//! directly every time.

use std::sync::Arc;

use crate::access::htup_details::{
    HeapTupleHeaderData, HEAP_MOVED_IN, HEAP_MOVED_OFF, HEAP_XMAX_COMMITTED, HEAP_XMAX_INVALID,
    HEAP_XMAX_IS_LOCKED_ONLY, HEAP_XMAX_IS_MULTI,
};
use crate::access::tableam::TM_Result;
use crate::backend::access::transam::transam::transaction_id_did_commit;
use crate::backend::access::transam::xact::TransactionIdIsCurrentTransactionId;
use crate::backend::storage::ipc::procarray::transaction_xmin;
use crate::backend::utils::time::combocid::{HeapTupleHeaderGetCmax, HeapTupleHeaderGetCmin};
use crate::backend::utils::time::snapmgr::XidInMVCCSnapshot;
use crate::c::{CommandId, TransactionId};
use crate::shared_state::SharedState;
use crate::utils::snapshot::SnapshotData;

/// `HeapTupleSatisfiesMVCC`: is `tuple` visible to `snapshot`?
///
/// `tuple` is the on-page (or in-memory copy of the) heap tuple header. The
/// `buffer` argument of the C signature is dropped: it exists only to address
/// the page for `SetHintBits`/`MarkBufferDirtyHint`, which we defer (see module
/// docs). Correctness is unchanged.
///
/// Mirrors the C control flow exactly (the `HEAP_MOVED_*` pre-9.0 upgrade arms
/// included), translating `TransactionIdDidCommit`/`XidInMVCCSnapshot` to their
/// async foundation forms.
#[allow(
    clippy::too_many_lines,
    clippy::if_not_else,
    clippy::collapsible_if,
    reason = "faithful 1:1 translation of HeapTupleSatisfiesMVCC's branch structure; reshaping would obscure the correspondence to the C"
)]
pub async fn heap_tuple_satisfies_mvcc(
    shared: &Arc<SharedState>,
    tuple: &HeapTupleHeaderData,
    snapshot: &SnapshotData,
) -> bool {
    if !tuple.xmin_committed() {
        if tuple.xmin_invalid() {
            return false;
        }

        // Used by pre-9.0 binary upgrades.
        if (tuple.t_infomask & HEAP_MOVED_OFF) != 0 {
            let xvac = tuple.get_xvac();
            if TransactionIdIsCurrentTransactionId(xvac) {
                return false;
            }
            if !XidInMVCCSnapshot(shared, xvac, snapshot).await {
                if did_commit(shared, xvac).await {
                    return false;
                }
                // else: treat xmin as committed (hint bit deferred).
            }
        }
        // Used by pre-9.0 binary upgrades.
        else if (tuple.t_infomask & HEAP_MOVED_IN) != 0 {
            let xvac = tuple.get_xvac();
            if !TransactionIdIsCurrentTransactionId(xvac) {
                if XidInMVCCSnapshot(shared, xvac, snapshot).await {
                    return false;
                }
                if !did_commit(shared, xvac).await {
                    return false;
                }
            }
        } else if TransactionIdIsCurrentTransactionId(tuple.get_raw_xmin()) {
            if tuple.get_raw_command_id() >= snapshot.curcid {
                return false; // inserted after scan started
            }

            if (tuple.t_infomask & HEAP_XMAX_INVALID) != 0 {
                return true; // xid invalid
            }

            if HEAP_XMAX_IS_LOCKED_ONLY(tuple.t_infomask) {
                return true; // not deleter
            }

            if (tuple.t_infomask & HEAP_XMAX_IS_MULTI) != 0 {
                let xmax = tuple.get_update_xid();
                // not LOCKED_ONLY, so it has to have an xmax.
                debug_assert!(xmax.is_valid());

                if !TransactionIdIsCurrentTransactionId(xmax) {
                    // updating subtransaction must have aborted
                    return true;
                } else if tuple.get_raw_command_id() >= snapshot.curcid {
                    return true; // updated after scan started
                }
                return false; // updated before scan started
            }

            if !TransactionIdIsCurrentTransactionId(tuple.get_raw_xmax()) {
                // deleting subtransaction must have aborted
                return true;
            }

            if tuple.get_raw_command_id() >= snapshot.curcid {
                return true; // deleted after scan started
            }
            return false; // deleted before scan started
        } else if XidInMVCCSnapshot(shared, tuple.get_raw_xmin(), snapshot).await {
            return false;
        } else if did_commit(shared, tuple.get_raw_xmin()).await {
            // xmin committed (hint bit deferred).
        } else {
            // it must have aborted or crashed
            return false;
        }
    } else {
        // xmin is committed, but maybe not according to our snapshot
        if !tuple.xmin_frozen() && XidInMVCCSnapshot(shared, tuple.get_raw_xmin(), snapshot).await {
            return false; // treat as still in progress
        }
    }

    // by here, the inserting transaction has committed

    if (tuple.t_infomask & HEAP_XMAX_INVALID) != 0 {
        return true; // xid invalid or aborted
    }

    if HEAP_XMAX_IS_LOCKED_ONLY(tuple.t_infomask) {
        return true;
    }

    if (tuple.t_infomask & HEAP_XMAX_IS_MULTI) != 0 {
        let xmax = tuple.get_update_xid();
        // not LOCKED_ONLY, so it has to have an xmax.
        debug_assert!(xmax.is_valid());

        if TransactionIdIsCurrentTransactionId(xmax) {
            if tuple.get_raw_command_id() >= snapshot.curcid {
                return true; // deleted after scan started
            }
            return false; // deleted before scan started
        }
        if XidInMVCCSnapshot(shared, xmax, snapshot).await {
            return true;
        }
        if did_commit(shared, xmax).await {
            return false; // updating transaction committed
        }
        // it must have aborted or crashed
        return true;
    }

    if (tuple.t_infomask & HEAP_XMAX_COMMITTED) == 0 {
        if TransactionIdIsCurrentTransactionId(tuple.get_raw_xmax()) {
            if tuple.get_raw_command_id() >= snapshot.curcid {
                return true; // deleted after scan started
            }
            return false; // deleted before scan started
        }

        if XidInMVCCSnapshot(shared, tuple.get_raw_xmax(), snapshot).await {
            return true;
        }

        if !did_commit(shared, tuple.get_raw_xmax()).await {
            // it must have aborted or crashed
            return true;
        }
        // xmax transaction committed (hint bit deferred).
    } else {
        // xmax is committed, but maybe not according to our snapshot
        if XidInMVCCSnapshot(shared, tuple.get_raw_xmax(), snapshot).await {
            return true; // treat as still in progress
        }
    }

    // xmax transaction committed
    false
}

/// `TransactionIdDidCommit` over the foundation clog/subtrans SLRUs, with the
/// per-backend `TransactionXmin` horizon. A small wrapper so the visibility
/// logic above reads like the C (which spells it `TransactionIdDidCommit(xid)`).
async fn did_commit(shared: &Arc<SharedState>, xid: TransactionId) -> bool {
    transaction_id_did_commit(shared.clog(), shared.subtrans(), xid, transaction_xmin()).await
}

/// `TransactionIdIsInProgress` over the procarray (consults clog/subtrans).
async fn xid_is_in_progress(shared: &Arc<SharedState>, xid: TransactionId) -> bool {
    shared
        .proc_array()
        .transaction_id_is_in_progress(
            shared.variable_cache(),
            shared.clog(),
            shared.subtrans(),
            xid,
        )
        .await
}

/// `HeapTupleSatisfiesUpdate`: classify whether `tuple` (the on-page header,
/// located at `t_self`) is updatable/deletable by the command `curcid`, returning
/// a `TM_Result`. Translated from `HeapTupleSatisfiesUpdate` in
/// backend/access/heap/heapam_visibility.c.
///
/// The C signature is `(HeapTuple htup, CommandId curcid, Buffer buffer)`; here
/// the header + its `t_self` line-pointer TID are passed directly (the `buffer`
/// existed only to address the page for `SetHintBits`, which we defer like the
/// MVCC variant). `Updated` vs `Deleted` is decided by comparing `t_self` against
/// the header's forward `ctid`.
///
/// Async (clog/subtrans/procarray probes). The caller holds the buffer content
/// lock + pin; since the header is passed by value (copied out of the page) there
/// is no lock held across these `.await`s.
///
/// Staged (multixact, rules.md s4): the `HEAP_XMAX_IS_MULTI` arms call
/// `MultiXactIdIsRunning`/`HeapTupleGetUpdateXid`, which are not yet reachable;
/// they `unimplemented!()` with a clear message. The common single-xid
/// locker/updater paths are complete.
#[allow(
    clippy::too_many_lines,
    clippy::if_not_else,
    clippy::collapsible_if,
    reason = "faithful 1:1 translation of HeapTupleSatisfiesUpdate's branch structure; reshaping would obscure the correspondence to the C"
)]
pub async fn HeapTupleSatisfiesUpdate(
    shared: &Arc<SharedState>,
    tuple: &HeapTupleHeaderData,
    t_self: &crate::storage::itemptr::ItemPointerData,
    curcid: CommandId,
) -> TM_Result {
    if !tuple.xmin_committed() {
        if tuple.xmin_invalid() {
            return TM_Result::Invisible;
        }

        // Used by pre-9.0 binary upgrades.
        if (tuple.t_infomask & HEAP_MOVED_OFF) != 0 {
            let xvac = tuple.get_xvac();
            if TransactionIdIsCurrentTransactionId(xvac) {
                return TM_Result::Invisible;
            }
            if !xid_is_in_progress(shared, xvac).await {
                if did_commit(shared, xvac).await {
                    return TM_Result::Invisible;
                }
                // else: treat xmin as committed (hint bit deferred).
            }
        }
        // Used by pre-9.0 binary upgrades.
        else if (tuple.t_infomask & HEAP_MOVED_IN) != 0 {
            let xvac = tuple.get_xvac();
            if !TransactionIdIsCurrentTransactionId(xvac) {
                if xid_is_in_progress(shared, xvac).await {
                    return TM_Result::Invisible;
                }
                if !did_commit(shared, xvac).await {
                    return TM_Result::Invisible;
                }
            }
        } else if TransactionIdIsCurrentTransactionId(tuple.get_raw_xmin()) {
            if HeapTupleHeaderGetCmin(tuple).0 >= curcid.0 {
                return TM_Result::Invisible; // inserted after scan started
            }

            if (tuple.t_infomask & HEAP_XMAX_INVALID) != 0 {
                return TM_Result::Ok; // xid invalid
            }

            if HEAP_XMAX_IS_LOCKED_ONLY(tuple.t_infomask) {
                // Even though this tuple was created by our own transaction, it
                // might be locked by other transactions, if the original version
                // was key-share locked when we updated it.
                if (tuple.t_infomask & HEAP_XMAX_IS_MULTI) != 0 {
                    unimplemented!(
                        "HeapTupleSatisfiesUpdate: own-xact LOCKED_ONLY multixact -- staged with multixact (step 33)"
                    );
                }

                // If the locker is gone, nothing of interest is left in this
                // Xmax; otherwise report the tuple as locked/updated.
                if !xid_is_in_progress(shared, tuple.get_raw_xmax()).await {
                    return TM_Result::Ok;
                }
                return TM_Result::BeingModified;
            }

            if (tuple.t_infomask & HEAP_XMAX_IS_MULTI) != 0 {
                unimplemented!(
                    "HeapTupleSatisfiesUpdate: own-xact updater multixact -- staged with multixact (step 33)"
                );
            }

            if !TransactionIdIsCurrentTransactionId(tuple.get_raw_xmax()) {
                // deleting subtransaction must have aborted
                return TM_Result::Ok;
            }

            if HeapTupleHeaderGetCmax(tuple).0 >= curcid.0 {
                return TM_Result::SelfModified; // updated after scan started
            }
            return TM_Result::Invisible; // updated before scan started
        } else if xid_is_in_progress(shared, tuple.get_raw_xmin()).await {
            return TM_Result::Invisible;
        } else if did_commit(shared, tuple.get_raw_xmin()).await {
            // xmin committed (hint bit deferred).
        } else {
            // it must have aborted or crashed
            return TM_Result::Invisible;
        }
    }

    // by here, the inserting transaction has committed

    if (tuple.t_infomask & HEAP_XMAX_INVALID) != 0 {
        return TM_Result::Ok; // xid invalid or aborted
    }

    if (tuple.t_infomask & HEAP_XMAX_COMMITTED) != 0 {
        if HEAP_XMAX_IS_LOCKED_ONLY(tuple.t_infomask) {
            return TM_Result::Ok;
        }
        if *t_self != tuple.ctid {
            return TM_Result::Updated; // updated by other
        }
        return TM_Result::Deleted; // deleted by other
    }

    if (tuple.t_infomask & HEAP_XMAX_IS_MULTI) != 0 {
        unimplemented!(
            "HeapTupleSatisfiesUpdate: committed-xmin multixact xmax -- staged with multixact (step 33)"
        );
    }

    if TransactionIdIsCurrentTransactionId(tuple.get_raw_xmax()) {
        if HEAP_XMAX_IS_LOCKED_ONLY(tuple.t_infomask) {
            return TM_Result::BeingModified;
        }
        if HeapTupleHeaderGetCmax(tuple).0 >= curcid.0 {
            return TM_Result::SelfModified; // updated after scan started
        }
        return TM_Result::Invisible; // updated before scan started
    }

    if xid_is_in_progress(shared, tuple.get_raw_xmax()).await {
        return TM_Result::BeingModified;
    }

    if !did_commit(shared, tuple.get_raw_xmax()).await {
        // it must have aborted or crashed
        return TM_Result::Ok;
    }

    // xmax transaction committed

    if HEAP_XMAX_IS_LOCKED_ONLY(tuple.t_infomask) {
        return TM_Result::Ok;
    }

    if *t_self != tuple.ctid {
        return TM_Result::Updated; // updated by other
    }
    TM_Result::Deleted // deleted by other
}

/// `HeapTupleSatisfiesVacuum`: classify `tuple` for VACUUM relative to the
/// `oldest_xmin` horizon. Returns the `HTSV_Result` deciding whether the tuple can
/// be removed (`Dead`), must be kept (`Live`/`RecentlyDead`), or belongs to an
/// in-progress xact (`Insert`/`DeleteInProgress`). Translated from
/// `HeapTupleSatisfiesVacuum` (via `HeapTupleSatisfiesVacuumHorizon`) in
/// backend/access/heap/heapam_visibility.c.
///
/// A tuple is `Dead` (removable) when its inserting xact committed and its deleting
/// xmax committed at an xid that precedes `oldest_xmin` -- no snapshot can see it.
///
/// Async (clog/procarray probes); the caller passes the header by value (copied out
/// of the page), so no buffer content lock is held across the `.await`s.
///
/// Staged (rules.md s4): the `HEAP_XMAX_IS_MULTI` arms (a multixact xmax) require
/// `MultiXactIdGetUpdateXid`/`MultiXactIdIsRunning`, not yet reachable, so they
/// `unimplemented!()`. The single-xid insert/delete path (the M2 heap's only
/// producer) is complete.
#[allow(
    clippy::too_many_lines,
    clippy::if_not_else,
    clippy::collapsible_if,
    reason = "faithful 1:1 translation of HeapTupleSatisfiesVacuumHorizon's branch structure"
)]
pub async fn HeapTupleSatisfiesVacuum(
    shared: &Arc<SharedState>,
    tuple: &HeapTupleHeaderData,
    oldest_xmin: TransactionId,
) -> crate::access::heapam::HTSV_Result {
    use crate::access::heapam::HTSV_Result;

    // Has inserting transaction committed?
    if !tuple.xmin_committed() {
        if tuple.xmin_invalid() {
            return HTSV_Result::Dead;
        }
        // Used by pre-9.0 binary upgrades -- staged (not produced by the M2 heap).
        if (tuple.t_infomask & (HEAP_MOVED_OFF | HEAP_MOVED_IN)) != 0 {
            unimplemented!("HeapTupleSatisfiesVacuum: HEAP_MOVED_* (pre-9.0 upgrade) not on the M13 path");
        } else if TransactionIdIsCurrentTransactionId(tuple.get_raw_xmin()) {
            return HTSV_Result::InsertInProgress;
        } else if xid_is_in_progress(shared, tuple.get_raw_xmin()).await {
            // xmin is in-progress; a delete by the same xact can't make it dead.
            return HTSV_Result::InsertInProgress;
        } else if did_commit(shared, tuple.get_raw_xmin()).await {
            // inserting xact committed (hint bit deferred), fall through to xmax.
        } else {
            // inserting xact aborted (or crashed): the tuple is dead.
            return HTSV_Result::Dead;
        }
    }

    // Okay, the inserter committed, so it was good at some point. Now what about
    // the deleting transaction?
    if (tuple.t_infomask & HEAP_XMAX_INVALID) != 0 {
        return HTSV_Result::Live;
    }

    if HEAP_XMAX_IS_LOCKED_ONLY(tuple.t_infomask) {
        // A lock-only xmax (SELECT FOR UPDATE/SHARE) does not delete the tuple.
        return HTSV_Result::Live;
    }

    if (tuple.t_infomask & HEAP_XMAX_IS_MULTI) != 0 {
        unimplemented!("HeapTupleSatisfiesVacuum: multixact xmax -- staged with multixact (step 33)");
    }

    // "Deleting" xact is a plain xid.
    let xmax = tuple.get_raw_xmax();
    if !xmax.is_normal() {
        return HTSV_Result::Live;
    }

    if TransactionIdIsCurrentTransactionId(xmax) {
        return HTSV_Result::DeleteInProgress;
    }
    if xid_is_in_progress(shared, xmax).await {
        return HTSV_Result::DeleteInProgress;
    }
    if !did_commit(shared, xmax).await {
        // deleting xact aborted: the tuple is (still) live.
        return HTSV_Result::Live;
    }

    // Deleter committed. The tuple is dead only once its xmax precedes the oldest
    // xmin any live snapshot could see; until then it is RecentlyDead (kept).
    if !xmax.precedes(oldest_xmin) {
        return HTSV_Result::RecentlyDead;
    }
    HTSV_Result::Dead
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::access::transam::FROZEN_TRANSACTION_ID;
    use crate::c::CommandId;
    use crate::utils::snapshot::SnapshotType;

    /// A zeroed header (all infomask bits clear, choice = 0). Tests then set the
    /// handful of fields the branch under test needs.
    fn zeroed_header() -> HeapTupleHeaderData {
        // SAFETY: HeapTupleHeaderData is repr(C) over a union of POD integer
        // fields + integer scalars; all-zero is a valid bit pattern.
        unsafe { core::mem::zeroed() }
    }

    fn mvcc_snapshot(xmin: u32, xmax: u32, curcid: u32) -> SnapshotData {
        SnapshotData {
            snapshot_type: SnapshotType::Mvcc,
            xmin: TransactionId(xmin),
            xmax: TransactionId(xmax),
            xip: Vec::new(),
            subxip: Vec::new(),
            suboverflowed: false,
            taken_during_recovery: false,
            copied: false,
            curcid: CommandId(curcid),
            speculative_token: 0,
            vistest: None,
            active_count: 0,
            regd_count: 1,
            snap_xact_completion_count: 0,
        }
    }

    // A frozen tuple with no xmax is visible to any MVCC snapshot without ever
    // touching clog (the early-out path). Exercises the committed/frozen-xmin
    // branch and the xmax-invalid exit.
    #[tokio::test(flavor = "multi_thread")]
    async fn frozen_no_xmax_is_visible() {
        use crate::shared_state::{SharedState, SharedStateConfig};
        let shared = SharedState::new(SharedStateConfig::default());

        let mut hdr = zeroed_header();
        hdr.set_xmin(FROZEN_TRANSACTION_ID);
        hdr.set_xmin_frozen();
        hdr.t_infomask |= HEAP_XMAX_INVALID;

        let snap = mvcc_snapshot(100, 200, 0);
        assert!(heap_tuple_satisfies_mvcc(&shared, &hdr, &snap).await);
    }

    // A tuple inserted by a future transaction (xmin >= snapshot.xmax) is NOT
    // visible: xmin is uncommitted-per-snapshot. Exercises the
    // !xmin_committed -> XidInMVCCSnapshot(follows xmax) -> false path.
    #[tokio::test(flavor = "multi_thread")]
    async fn future_xmin_not_visible() {
        use crate::shared_state::{SharedState, SharedStateConfig};
        let shared = SharedState::new(SharedStateConfig::default());

        let mut hdr = zeroed_header();
        hdr.set_xmin(TransactionId(500)); // >= snapshot.xmax
        hdr.t_infomask |= HEAP_XMAX_INVALID;

        let snap = mvcc_snapshot(100, 200, 0);
        assert!(!heap_tuple_satisfies_mvcc(&shared, &hdr, &snap).await);
    }

    use crate::access::htup_details::HEAP_XMAX_COMMITTED;
    use crate::access::tableam::TM_Result;
    use crate::storage::itemptr::ItemPointerData;

    fn tid(block: u32, off: u16) -> ItemPointerData {
        let mut ip = ItemPointerData {
            blkid: crate::storage::block::BlockIdData { hi: 0, lo: 0 },
            posid: 0,
        };
        ip.set(block, off);
        ip
    }

    // HeapTupleSatisfiesUpdate: a committed-xmin tuple with no xmax is updatable
    // (TM_Ok) -- the committed-frozen-xmin early path, no clog probe.
    #[tokio::test(flavor = "multi_thread")]
    async fn htsu_live_committed_is_ok() {
        use crate::shared_state::{SharedState, SharedStateConfig};
        let shared = SharedState::new(SharedStateConfig::default());

        let mut hdr = zeroed_header();
        hdr.set_xmin(FROZEN_TRANSACTION_ID);
        hdr.set_xmin_frozen();
        hdr.t_infomask |= HEAP_XMAX_INVALID;
        let self_tid = tid(0, 1);
        hdr.ctid = self_tid;

        assert_eq!(
            HeapTupleSatisfiesUpdate(&shared, &hdr, &self_tid, CommandId(5)).await,
            TM_Result::Ok
        );
    }

    // A committed-xmin tuple whose xmax is committed and whose t_ctid points at a
    // DIFFERENT tuple -> TM_Updated (updated by another xact). ctid == self would
    // be TM_Deleted.
    #[tokio::test(flavor = "multi_thread")]
    async fn htsu_committed_xmax_updated_vs_deleted() {
        use crate::shared_state::{SharedState, SharedStateConfig};
        let shared = SharedState::new(SharedStateConfig::default());
        let self_tid = tid(0, 1);

        // xmax committed, ctid -> different tuple: updated by other.
        let mut upd = zeroed_header();
        upd.set_xmin(FROZEN_TRANSACTION_ID);
        upd.set_xmin_frozen();
        upd.set_xmax(TransactionId(50));
        upd.t_infomask |= HEAP_XMAX_COMMITTED;
        upd.ctid = tid(0, 2); // forward link
        assert_eq!(
            HeapTupleSatisfiesUpdate(&shared, &upd, &self_tid, CommandId(5)).await,
            TM_Result::Updated
        );

        // xmax committed, ctid self-points: deleted by other.
        let mut del = zeroed_header();
        del.set_xmin(FROZEN_TRANSACTION_ID);
        del.set_xmin_frozen();
        del.set_xmax(TransactionId(50));
        del.t_infomask |= HEAP_XMAX_COMMITTED;
        del.ctid = self_tid;
        assert_eq!(
            HeapTupleSatisfiesUpdate(&shared, &del, &self_tid, CommandId(5)).await,
            TM_Result::Deleted
        );
    }

    // An xmin-invalid tuple is TM_Invisible.
    #[tokio::test(flavor = "multi_thread")]
    async fn htsu_xmin_invalid_is_invisible() {
        use crate::access::htup_details::HEAP_XMIN_INVALID;
        use crate::shared_state::{SharedState, SharedStateConfig};
        let shared = SharedState::new(SharedStateConfig::default());

        let mut hdr = zeroed_header();
        hdr.set_xmin(TransactionId(10));
        hdr.t_infomask |= HEAP_XMIN_INVALID;
        let self_tid = tid(0, 1);

        assert_eq!(
            HeapTupleSatisfiesUpdate(&shared, &hdr, &self_tid, CommandId(5)).await,
            TM_Result::Invisible
        );
    }
}
