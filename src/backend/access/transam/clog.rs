//! Translated from PostgreSQL src/backend/access/transam/clog.c
//!
//! The transaction-commit-log manager: two bits of commit/abort status per xid
//! over the SLRU async leaf (`SlruCtl`). The CLOG `SlruCtl` lives on
//! `SharedState`; reached via `shared.clog()`.
//!
//! Simplifications (design step14 section 6):
//!  * TODO(step15): `TransactionGroupUpdateXidStatus` batches concurrent clog
//!    status updates from many backends into one bank-lock acquisition by the
//!    group leader, via per-proc `PGPROC.clogGroup*` fields linked through the
//!    atomic `ProcGlobal.clogGroupFirst` head. Both require the real PGPROC/
//!    ProcGlobal that step 15 builds (the gating MyProc xid check is also a
//!    step-15 concern), so statuses are set directly under the bank lock here.
//!  * clog_redo is deferred (recovery is out of foundation); see clog_redo.

use std::sync::Arc;

use crate::access::clog::XidStatus;
use crate::access::transam::FIRST_NORMAL_TRANSACTION_ID;
#[cfg(test)]
use crate::access::xlogdefs::INVALID_XLOG_REC_PTR;
use crate::access::xlogdefs::XLogRecPtr;
use crate::backend::access::transam::slru::{SLRU_BANK_SIZE, SlruCtl, autotune_buffers};
use crate::backend::access::transam::transam::VariableCache;
use crate::c::TransactionId;
use crate::pg_config::BLCKSZ;
use crate::storage::sync::SyncRequestHandler;

// clog.c page geometry: 2 bits per xact -> 4 xacts per byte.
const CLOG_BITS_PER_XACT: u32 = 2;
const CLOG_XACTS_PER_BYTE: u32 = 4;
const CLOG_XACTS_PER_PAGE: u32 = BLCKSZ * CLOG_XACTS_PER_BYTE;
const CLOG_XACT_BITMASK: u8 = (1 << CLOG_BITS_PER_XACT) - 1;

// Async-commit LSN grouping.
const CLOG_XACTS_PER_LSN_GROUP: u32 = 32;
const CLOG_LSNS_PER_PAGE: usize = (CLOG_XACTS_PER_PAGE / CLOG_XACTS_PER_LSN_GROUP) as usize;

#[inline]
fn xid_to_page(xid: TransactionId) -> i64 {
    i64::from(xid.0) / i64::from(CLOG_XACTS_PER_PAGE)
}
#[inline]
fn xid_to_pgindex(xid: TransactionId) -> u32 {
    xid.0 % CLOG_XACTS_PER_PAGE
}
#[inline]
fn xid_to_byte(xid: TransactionId) -> usize {
    (xid_to_pgindex(xid) / CLOG_XACTS_PER_BYTE) as usize
}
#[inline]
fn xid_to_bindex(xid: TransactionId) -> u32 {
    xid.0 % CLOG_XACTS_PER_BYTE
}
#[inline]
fn lsn_group(xid: TransactionId) -> usize {
    (xid_to_pgindex(xid) / CLOG_XACTS_PER_LSN_GROUP) as usize
}

fn status_ordinal(s: XidStatus) -> u8 {
    s as i32 as u8
}

/// clog.c CLOGShmemBuffers: default to autotune (2MB/1GB of shared_buffers, cap
/// 8MB), at least 16. transaction_buffers GUC is unset (0). TODO(guc).
fn clog_shmem_buffers(nbuffers: usize) -> usize {
    autotune_buffers(nbuffers, 512, 1024)
}

/// clog.c CLOGShmemSize.
pub fn clog_shmem_size(nbuffers: usize) -> usize {
    // SLRU shmem sizing is moot under the Arc model; report the slot count *
    // page size as an estimate for callers that still ask.
    clog_shmem_buffers(nbuffers) * BLCKSZ as usize
}

/// clog.c CLOGShmemInit: build the CLOG SlruCtl over `pg_xact`. Takes I/O
/// handles (not `Arc<SharedState>`) to avoid an Arc cycle (SharedState owns this).
pub fn clog_shmem_init_handles(
    nbuffers: usize,
    fd: Arc<crate::backend::storage::file::fd::FdManager>,
    xlog: Arc<crate::backend::access::transam::xlog::XLogCtl>,
    sync_requests: Arc<crate::storage::sync::SyncRequests>,
    data_dir: Option<String>,
) -> Arc<SlruCtl> {
    // autotune_buffers already returns a multiple of SLRU_BANK_SIZE >= one bank.
    let nslots = clog_shmem_buffers(nbuffers);
    debug_assert!(nslots.is_multiple_of(SLRU_BANK_SIZE));
    SlruCtl::new(
        nslots,
        CLOG_LSNS_PER_PAGE,
        "pg_xact",
        SyncRequestHandler::Clog,
        false,
        clog_page_precedes,
        fd,
        xlog,
        sync_requests,
        data_dir,
    )
}

/// clog.c CLOGPagePrecedes: decide whether a CLOG page is "older" for
/// truncation. Offsets so the compared xids are normal.
fn clog_page_precedes(page1: i64, page2: i64) -> bool {
    let base = FIRST_NORMAL_TRANSACTION_ID.0.wrapping_add(1);
    let xid1 = TransactionId(
        (page1 as u32)
            .wrapping_mul(CLOG_XACTS_PER_PAGE)
            .wrapping_add(base),
    );
    let xid2 = TransactionId(
        (page2 as u32)
            .wrapping_mul(CLOG_XACTS_PER_PAGE)
            .wrapping_add(base),
    );
    let xid2_hi = TransactionId(xid2.0.wrapping_add(CLOG_XACTS_PER_PAGE - 1));
    xid1.precedes(xid2) && xid1.precedes(xid2_hi)
}

impl SlruCtl {
    /// clog.c TransactionIdSetTreeStatus: record the final commit/abort status
    /// for `xid` and its `subxids` (`&self` = the clog SLRU). Async commits pass
    /// the commit-record `lsn`.
    pub async fn set_tree_status(
        &self,
        xid: TransactionId,
        subxids: &[TransactionId],
        status: XidStatus,
        lsn: XLogRecPtr,
    ) {
        debug_assert!(matches!(status, XidStatus::Committed | XidStatus::Aborted));
        let pageno = xid_to_page(xid);

        // Count subxids on the parent's page.
        let on_first = subxids
            .iter()
            .take_while(|s| xid_to_page(**s) == pageno)
            .count();

        if on_first == subxids.len() {
            self.set_page_status(xid, subxids, status, lsn, pageno)
                .await;
        } else {
            // Commit: subcommit the off-page subxids first (intermediate state).
            if matches!(status, XidStatus::Committed) {
                self.set_status_by_pages(&subxids[on_first..], XidStatus::SubCommitted, lsn)
                    .await;
            }
            self.set_page_status(xid, &subxids[..on_first], status, lsn, pageno)
                .await;
            self.set_status_by_pages(&subxids[on_first..], status, lsn)
                .await;
        }
    }

    /// clog.c set_status_by_pages: chunk subxids by clog page.
    async fn set_status_by_pages(
        &self,
        subxids: &[TransactionId],
        status: XidStatus,
        lsn: XLogRecPtr,
    ) {
        let mut i = 0;
        while i < subxids.len() {
            let pageno = xid_to_page(subxids[i]);
            let start = i;
            while i < subxids.len() && xid_to_page(subxids[i]) == pageno {
                i += 1;
            }
            self.set_page_status(TransactionId(0), &subxids[start..i], status, lsn, pageno)
                .await;
        }
    }

    /// clog.c TransactionIdSetPageStatus(Internal): set the status bits for the
    /// (optional) main xid + subxids that all lie on `pageno`. TODO(step15): the
    /// `TransactionGroupUpdateXidStatus` batching (PGPROC.clogGroup* +
    /// ProcGlobal.clogGroupFirst) lands with the real PGPROC array; this is the
    /// direct path.
    async fn set_page_status(
        &self,
        xid: TransactionId,
        subxids: &[TransactionId],
        status: XidStatus,
        lsn: XLogRecPtr,
        pageno: i64,
    ) {
        // Async commit (valid lsn): must not let the update reach disk before the
        // WAL flush, so do not return a write-busy page (write_ok = lsn invalid).
        let write_ok = lsn.is_invalid();
        let main_valid = xid.is_valid();
        // Set bits + group LSN under one held write lock (closes the eviction race).
        self.read_page_with(pageno, write_ok, xid, |mut page| {
            let buf = page.buf_mut();
            if main_valid {
                // Subcommit subxids first on a commit (atomicity, clog.c).
                if matches!(status, XidStatus::Committed) {
                    for s in subxids {
                        set_status_bit(buf, *s, XidStatus::SubCommitted);
                    }
                }
                set_status_bit(buf, xid, status);
            }
            for s in subxids {
                set_status_bit(buf, *s, status);
            }
            // Update group LSN (async commit).
            if lsn.is_valid() {
                if main_valid {
                    page.set_group_lsn(lsn_group(xid), lsn.0);
                }
                for s in subxids {
                    page.set_group_lsn(lsn_group(*s), lsn.0);
                }
            }
        })
        .await;
    }

    /// clog.c TransactionIdGetStatus: read the commit status + group LSN for `xid`.
    pub async fn get_status(&self, xid: TransactionId) -> (XidStatus, XLogRecPtr) {
        let pageno = xid_to_page(xid);
        let byteno = xid_to_byte(xid);
        let bshift = xid_to_bindex(xid) * CLOG_BITS_PER_XACT;

        // Read status bits + group LSN under one held shared lock.
        let (raw, lsn) = self
            .read_page_readonly_with(pageno, xid, |page| {
                let raw = (page.buf()[byteno] >> bshift) & CLOG_XACT_BITMASK;
                (raw, page.group_lsn(lsn_group(xid)))
            })
            .await;
        (status_from_ordinal(raw), XLogRecPtr(lsn))
    }

    /// clog.c BootStrapCLOG: create + zero + flush the first CLOG page.
    pub async fn boot_strap_clog(&self) {
        let slot = self.zero_page(0).await;
        self.write_page(slot).await;
    }

    /// clog.c StartupCLOG: set latest_page_number from nextXid.
    pub fn startup_clog(&self, vc: &VariableCache) {
        let next = vc.with(|v| v.next_xid);
        let xid = crate::access::transam::xid_from_full_transaction_id(next);
        self.set_latest_page_number(xid_to_page(xid));
    }

    /// clog.c TrimCLOG: zero the tail of the current clog page (safety after redo).
    pub async fn trim_clog(&self, vc: &VariableCache) {
        let next = vc.with(|v| v.next_xid);
        let xid = crate::access::transam::xid_from_full_transaction_id(next);
        let pageno = xid_to_page(xid);
        if xid_to_pgindex(xid) != 0 {
            let byteno = xid_to_byte(xid);
            let bshift = xid_to_bindex(xid) * CLOG_BITS_PER_XACT;
            self.read_page_with(pageno, false, xid, |mut page| {
                let buf = page.buf_mut();
                buf[byteno] &= (1u8 << bshift).wrapping_sub(1);
                for b in &mut buf[byteno + 1..] {
                    *b = 0;
                }
            })
            .await;
        }
    }

    /// clog.c CheckPointCLOG: flush dirty CLOG pages.
    pub async fn check_point_clog(&self) {
        self.write_all().await;
    }

    /// clog.c ExtendCLOG: zero a fresh clog page at a page boundary.
    pub async fn extend_clog(&self, newest_xact: TransactionId) {
        if xid_to_pgindex(newest_xact) != 0 && newest_xact.0 != FIRST_NORMAL_TRANSACTION_ID.0 {
            return;
        }
        let pageno = xid_to_page(newest_xact);
        // ZeroCLOGPage(pageno, true): zeroes the page; the WAL ZEROPAGE record is
        // a redo aid only (TODO(recovery): emit it once redo replays clog).
        let _slot = self.zero_page(pageno).await;
    }

    /// clog.c TruncateCLOG: remove clog segments before `oldest_xact`'s page.
    pub async fn truncate_clog(
        &self,
        vc: &VariableCache,
        oldest_xact: TransactionId,
        _oldest_xid_datoid: crate::postgres_ext::Oid,
    ) {
        let cutoff_page = xid_to_page(oldest_xact);
        // Advance oldestClogXid before truncating (concurrent lookups).
        vc.advance_oldest_clog_xid(oldest_xact);
        self.truncate(cutoff_page).await;
    }
}

/// clog.c TransactionIdSetStatusBit (no LSN handling; done by caller).
fn set_status_bit(buf: &mut [u8], xid: TransactionId, status: XidStatus) {
    let byteno = xid_to_byte(xid);
    let bshift = xid_to_bindex(xid) * CLOG_BITS_PER_XACT;
    let mut byteval = buf[byteno];
    byteval &= !(CLOG_XACT_BITMASK << bshift);
    byteval |= status_ordinal(status) << bshift;
    buf[byteno] = byteval;
}

fn status_from_ordinal(v: u8) -> XidStatus {
    match v {
        0x00 => XidStatus::InProgress,
        0x01 => XidStatus::Committed,
        0x02 => XidStatus::Aborted,
        _ => XidStatus::SubCommitted,
    }
}

/// clog.c clogsyncfiletag: fsync entrypoint for sync.c.
pub async fn clogsyncfiletag(
    clog: &SlruCtl,
    ftag: &crate::storage::sync::FileTag,
) -> std::io::Result<String> {
    clog.sync_file_tag(ftag).await
}

/// clog.c clog_redo: replay clog WAL. TODO(recovery): the rmgr redo path is
/// sync and out of foundation; deferred to recovery.
pub fn clog_redo() {
    unimplemented!("clog_redo deferred to recovery (step out of foundation)")
}

/// clog.c clog_identify: name the clog WAL opcode.
pub fn clog_identify(info: u8) -> Option<&'static str> {
    match info & 0xF0 {
        crate::access::clog::CLOG_ZEROPAGE => Some("ZEROPAGE"),
        crate::access::clog::CLOG_TRUNCATE => Some("TRUNCATE"),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shared_state::{SharedState, SharedStateConfig};

    fn temp_shared(tag: &str) -> Arc<SharedState> {
        let dir = std::env::temp_dir().join(format!(
            "pepperdb_clog_{tag}_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        // data_dir must be set in the config so the clog SLRU resolves pg_xact
        // under the tempdir at construction (not the relative default).
        SharedState::new(SharedStateConfig {
            data_dir: Some(dir.to_string_lossy().into_owned()),
            ..SharedStateConfig::default()
        })
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn set_get_roundtrip() {
        let shared = temp_shared("rt");
        let clog = shared.clog();
        // Page 0 must be zeroed before use (BootStrapCLOG does this on install).
        clog.boot_strap_clog().await;
        let xid = TransactionId(10);
        clog.set_tree_status(xid, &[], XidStatus::Committed, INVALID_XLOG_REC_PTR)
            .await;
        let (st, _) = clog.get_status(xid).await;
        assert_eq!(st, XidStatus::Committed);

        let xid2 = TransactionId(11);
        clog.set_tree_status(xid2, &[], XidStatus::Aborted, INVALID_XLOG_REC_PTR)
            .await;
        assert_eq!(clog.get_status(xid2).await.0, XidStatus::Aborted);
        // Untouched xid stays in-progress.
        assert_eq!(
            clog.get_status(TransactionId(12)).await.0,
            XidStatus::InProgress
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn across_page_boundary_extend() {
        let shared = temp_shared("page");
        let clog = shared.clog();
        clog.boot_strap_clog().await;
        // First xid of page 1.
        let boundary = TransactionId(CLOG_XACTS_PER_PAGE);
        clog.extend_clog(boundary).await;
        clog.set_tree_status(boundary, &[], XidStatus::Committed, INVALID_XLOG_REC_PTR)
            .await;
        assert_eq!(clog.get_status(boundary).await.0, XidStatus::Committed);
        // A neighbor on page 1.
        let nb = TransactionId(CLOG_XACTS_PER_PAGE + 7);
        clog.set_tree_status(nb, &[], XidStatus::Aborted, INVALID_XLOG_REC_PTR)
            .await;
        assert_eq!(clog.get_status(nb).await.0, XidStatus::Aborted);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn subxids_on_other_pages_commit() {
        let shared = temp_shared("sub");
        let clog = shared.clog();
        clog.boot_strap_clog().await;
        let parent = TransactionId(5);
        let sub_same = TransactionId(6);
        let sub_other = TransactionId(CLOG_XACTS_PER_PAGE + 1);
        clog.extend_clog(TransactionId(CLOG_XACTS_PER_PAGE)).await;
        clog.set_tree_status(
            parent,
            &[sub_same, sub_other],
            XidStatus::Committed,
            INVALID_XLOG_REC_PTR,
        )
        .await;
        assert_eq!(clog.get_status(parent).await.0, XidStatus::Committed);
        assert_eq!(clog.get_status(sub_same).await.0, XidStatus::Committed);
        assert_eq!(clog.get_status(sub_other).await.0, XidStatus::Committed);
    }
}
