//! Translated from PostgreSQL src/backend/access/transam/transam.c
//!
//! High-level transaction-status access: xid commit/abort tests over clog +
//! subtrans, the modulo-2^32 xid comparators, and the commit-tree setters.
//! Also home to `VariableCache` (the ex-`TransamVariables` shared struct,
//! design step14 section 3), reached via `shared.variable_cache()`.
//!
//! The single-item TransactionLogFetch cache (transam.c `cachedFetchXid` /
//! `cachedFetchXidStatus` / `cachedCommitLSN`) is a per-task `task_local`
//! `RefCell` here: it is per-backend state held across `.await` (rules s6.1/
//! s11), never thread_local. Only resolved (COMMITTED/ABORTED) statuses are
//! cached, exactly as PG. The borrow is never held across the clog/subtrans
//! `.await` (borrow to check, drop, await, borrow to update).

use std::cell::RefCell;
use std::sync::Mutex;

use crate::access::clog::XidStatus;
use crate::access::transam::{FullTransactionId, TransamVariablesData};
use crate::access::xlogdefs::{INVALID_XLOG_REC_PTR, XLogRecPtr};
use crate::backend::access::transam::slru::SlruCtl;
use crate::c::TransactionId;

/// The ex-`TransamVariables` shared struct (varsup/transam globals). One `Mutex`
/// for the whole struct is acceptable for the foundation: contention is low now
/// (no concurrent OLTP). NEVER hold the guard across an `.await` (design s3).
pub struct VariableCache {
    inner: Mutex<TransamVariablesData>,
}

impl VariableCache {
    /// varsup.c VarsupShmemInit: zero-initialized state.
    pub fn new() -> Self {
        VariableCache {
            inner: Mutex::new(TransamVariablesData {
                next_oid: crate::postgres_ext::Oid(0),
                oid_count: 0,
                next_xid: crate::access::transam::FIRST_NORMAL_FULL_TRANSACTION_ID,
                oldest_xid: crate::access::transam::FIRST_NORMAL_TRANSACTION_ID,
                xid_vac_limit: TransactionId(0),
                xid_warn_limit: TransactionId(0),
                xid_stop_limit: TransactionId(0),
                xid_wrap_limit: TransactionId(0),
                oldest_xid_db: crate::postgres_ext::Oid(0),
                oldest_commit_ts_xid: TransactionId(0),
                newest_commit_ts_xid: TransactionId(0),
                latest_completed_xid: crate::access::transam::FIRST_NORMAL_FULL_TRANSACTION_ID,
                // procarray.c ProcArrayShmemInit starts this at 1 (0 means "no
                // snapshot built yet" and disables the GetSnapshotDataReuse path).
                xact_completion_count: 1,
                oldest_clog_xid: crate::access::transam::FIRST_NORMAL_TRANSACTION_ID,
            }),
        }
    }

    /// Run `f` under the variable-cache lock. Caller must not `.await` inside.
    pub fn with<R>(&self, f: impl FnOnce(&mut TransamVariablesData) -> R) -> R {
        f(&mut self.inner.lock().unwrap())
    }
}

// Process-wide VariableCache accessor (single-process model: one per process),
// published by `SharedState::new`. proc.c's `ProcKill` reaches it the way PG
// reached the shmem `TransamVariables`, without a SharedState handle. New code
// should prefer `shared.variable_cache()`.
static VARIABLE_CACHE: std::sync::OnceLock<std::sync::Arc<VariableCache>> = std::sync::OnceLock::new();

/// Publish the process-wide `VariableCache` (called once by `SharedState::new`).
/// First publisher wins (tests building multiple SharedStates do not panic).
pub fn set_variable_cache(vc: std::sync::Arc<VariableCache>) {
    let _ = VARIABLE_CACHE.set(vc);
}

/// The process-wide `VariableCache`, if published.
pub fn current_variable_cache() -> Option<std::sync::Arc<VariableCache>> {
    VARIABLE_CACHE.get().cloned()
}

impl Default for VariableCache {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// xid comparison: the modular comparators now live as TransactionId methods
// (access/transam.rs). These free-fn shims remain for cross-reference.
// ---------------------------------------------------------------------------

#[deprecated(note = "use id1.precedes(id2)")]
#[inline]
pub fn transaction_id_precedes(id1: TransactionId, id2: TransactionId) -> bool {
    id1.precedes(id2)
}

#[deprecated(note = "use id1.precedes_or_equals(id2)")]
#[inline]
pub fn transaction_id_precedes_or_equals(id1: TransactionId, id2: TransactionId) -> bool {
    id1.precedes_or_equals(id2)
}

#[deprecated(note = "use id1.follows(id2)")]
#[inline]
pub fn transaction_id_follows(id1: TransactionId, id2: TransactionId) -> bool {
    id1.follows(id2)
}

#[deprecated(note = "use id1.follows_or_equals(id2)")]
#[inline]
pub fn transaction_id_follows_or_equals(id1: TransactionId, id2: TransactionId) -> bool {
    id1.follows_or_equals(id2)
}

/// transam.c TransactionIdLatest: newest xid among a main xact and children.
pub fn transaction_id_latest(mainxid: TransactionId, xids: &[TransactionId]) -> TransactionId {
    let mut result = mainxid;
    for &x in xids.iter().rev() {
        if result.precedes(x) {
            result = x;
        }
    }
    result
}

// ---------------------------------------------------------------------------
// commit-status fetch (transam.c TransactionLogFetch / DidCommit / DidAbort)
// ---------------------------------------------------------------------------

/// transam.c single-item cache for `TransactionLogFetch` (cachedFetchXid +
/// cachedFetchXidStatus + cachedCommitLSN). Per-task: only the owning backend
/// reads/writes it, and it must persist across `.await` (rules s6.1/s11).
#[derive(Clone, Copy)]
struct FetchCache {
    xid: TransactionId,
    status: XidStatus,
    commit_lsn: XLogRecPtr,
}

tokio::task_local! {
    static FETCH_CACHE: RefCell<FetchCache>;
}

/// Publish the per-task xid-status cache for `f` (backend entry). Mirrors PG's
/// process-lifetime `cachedFetchXid` statics.
pub async fn fetch_cache_scope<F, T>(f: F) -> T
where
    F: std::future::Future<Output = T>,
{
    FETCH_CACHE
        .scope(
            RefCell::new(FetchCache {
                xid: crate::access::transam::INVALID_TRANSACTION_ID,
                status: XidStatus::InProgress,
                commit_lsn: INVALID_XLOG_REC_PTR,
            }),
            f,
        )
        .await
}

/// Cache hit: the resolved (status, lsn) for `xid`, or None. Borrow dropped on
/// return (never held across `.await`).
fn fetch_cache_get(xid: TransactionId) -> Option<(XidStatus, XLogRecPtr)> {
    FETCH_CACHE
        .try_with(|c| {
            let c = c.borrow();
            (c.xid.0 == xid.0).then_some((c.status, c.commit_lsn))
        })
        .unwrap_or(None)
}

/// Cache a resolved status for `xid`. Caller must only pass COMMITTED/ABORTED
/// (transam.c never caches IN_PROGRESS or SUB_COMMITTED).
fn fetch_cache_put(xid: TransactionId, status: XidStatus, commit_lsn: XLogRecPtr) {
    let _ = FETCH_CACHE.try_with(|c| {
        *c.borrow_mut() = FetchCache {
            xid,
            status,
            commit_lsn,
        };
    });
}

/// transam.c TransactionLogFetch: fetch the commit status + commit LSN for
/// `xid`. Checks the single-item cache first, then permanent-xid fast paths,
/// then reads clog; resolved statuses are cached.
async fn transaction_log_fetch(clog: &SlruCtl, xid: TransactionId) -> (XidStatus, XLogRecPtr) {
    if let Some(hit) = fetch_cache_get(xid) {
        return hit;
    }
    if !xid.is_normal() {
        if xid.0 == crate::access::transam::BOOTSTRAP_TRANSACTION_ID.0
            || xid.0 == crate::access::transam::FROZEN_TRANSACTION_ID.0
        {
            return (XidStatus::Committed, INVALID_XLOG_REC_PTR);
        }
        return (XidStatus::Aborted, INVALID_XLOG_REC_PTR);
    }
    let (status, lsn) = clog.get_status(xid).await;
    // Cache only guaranteed-final statuses (never IN_PROGRESS / SUB_COMMITTED).
    if status != XidStatus::InProgress && status != XidStatus::SubCommitted {
        fetch_cache_put(xid, status, lsn);
    }
    (status, lsn)
}

/// transam.c TransactionIdDidCommit. `transaction_xmin` is the per-backend
/// oldest-visible xid (snapmgr); below it we can't consult subtrans.
pub async fn transaction_id_did_commit(
    clog: &SlruCtl,
    subtrans: &SlruCtl,
    xid: TransactionId,
    transaction_xmin: TransactionId,
) -> bool {
    match transaction_log_fetch(clog, xid).await.0 {
        XidStatus::Committed => true,
        XidStatus::SubCommitted => {
            if xid.precedes(transaction_xmin) {
                return false;
            }
            let parent = subtrans.sub_trans_get_parent(xid).await;
            if !parent.is_valid() {
                // see transam.c notes: parent crashed without cleanup.
                return false;
            }
            Box::pin(transaction_id_did_commit(
                clog,
                subtrans,
                parent,
                transaction_xmin,
            ))
            .await
        }
        _ => false,
    }
}

/// transam.c TransactionIdDidAbort.
pub async fn transaction_id_did_abort(
    clog: &SlruCtl,
    subtrans: &SlruCtl,
    xid: TransactionId,
    transaction_xmin: TransactionId,
) -> bool {
    match transaction_log_fetch(clog, xid).await.0 {
        XidStatus::Aborted => true,
        XidStatus::SubCommitted => {
            if xid.precedes(transaction_xmin) {
                return true;
            }
            let parent = subtrans.sub_trans_get_parent(xid).await;
            if !parent.is_valid() {
                return true;
            }
            Box::pin(transaction_id_did_abort(
                clog,
                subtrans,
                parent,
                transaction_xmin,
            ))
            .await
        }
        _ => false,
    }
}

// ---------------------------------------------------------------------------
// commit-tree setters (transam.c TransactionIdCommitTree / AbortTree / Async)
// ---------------------------------------------------------------------------

/// transam.c TransactionIdCommitTree: mark the tree committed (sync commit).
pub async fn transaction_id_commit_tree(
    clog: &SlruCtl,
    xid: TransactionId,
    xids: &[TransactionId],
) {
    clog.set_tree_status(xid, xids, XidStatus::Committed, INVALID_XLOG_REC_PTR)
        .await;
}

/// transam.c TransactionIdAsyncCommitTree: async commit (commit-record LSN).
pub async fn transaction_id_async_commit_tree(
    clog: &SlruCtl,
    xid: TransactionId,
    xids: &[TransactionId],
    lsn: XLogRecPtr,
) {
    clog.set_tree_status(xid, xids, XidStatus::Committed, lsn)
        .await;
}

/// transam.c TransactionIdAbortTree: mark the tree aborted.
pub async fn transaction_id_abort_tree(clog: &SlruCtl, xid: TransactionId, xids: &[TransactionId]) {
    clog.set_tree_status(xid, xids, XidStatus::Aborted, INVALID_XLOG_REC_PTR)
        .await;
}

impl SlruCtl {
    /// transam.c TransactionIdGetCommitLSN: an LSN late enough to guarantee the
    /// commit record is durable once flushed (`&self` = the clog SLRU). Almost all
    /// callers pass an xid just resolved by TransactionLogFetch, so the cache
    /// usually hits (and we skip the clog read). The clog read here is not cached
    /// (PG does not cache it either).
    pub async fn get_commit_lsn(&self, xid: TransactionId) -> XLogRecPtr {
        if let Some((_, lsn)) = fetch_cache_get(xid) {
            return lsn;
        }
        if !xid.is_normal() {
            return INVALID_XLOG_REC_PTR;
        }
        self.get_status(xid).await.1
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::access::transam::{
        BOOTSTRAP_TRANSACTION_ID, FIRST_NORMAL_TRANSACTION_ID, FROZEN_TRANSACTION_ID,
    };

    #[test]
    fn precedes_permanent_xids_use_plain_unsigned() {
        // Bootstrap (1) and frozen (2) sort before any normal xid by plain <.
        assert!(BOOTSTRAP_TRANSACTION_ID.precedes(FIRST_NORMAL_TRANSACTION_ID));
        assert!(FROZEN_TRANSACTION_ID.precedes(FIRST_NORMAL_TRANSACTION_ID));
        assert!(BOOTSTRAP_TRANSACTION_ID.precedes(FROZEN_TRANSACTION_ID));
        // A permanent xid never "follows" a normal one.
        assert!(!FROZEN_TRANSACTION_ID.follows(FIRST_NORMAL_TRANSACTION_ID));
    }

    #[test]
    fn precedes_normal_xids_use_modulo_2_32() {
        let a = TransactionId(100);
        let b = TransactionId(200);
        assert!(a.precedes(b));
        assert!(b.follows(a));
        assert!(a.precedes_or_equals(a));
        assert!(a.follows_or_equals(a));
    }

    #[test]
    fn precedes_handles_wraparound() {
        // A small xid just after wraparound logically follows a huge one near the
        // top of the space (modulo-2^32 comparison).
        let high = TransactionId(0xFFFF_FF00);
        let low = TransactionId(10); // 10 is normal (>= 3)
        assert!(high.precedes(low));
        assert!(low.follows(high));
    }

    #[test]
    fn latest_picks_newest() {
        let main = TransactionId(50);
        let subs = [TransactionId(40), TransactionId(70), TransactionId(60)];
        assert_eq!(transaction_id_latest(main, &subs).0, 70);
    }

    #[test]
    fn is_valid_is_normal_edges() {
        use crate::access::transam::INVALID_TRANSACTION_ID;
        assert!(!INVALID_TRANSACTION_ID.is_valid());
        assert!(BOOTSTRAP_TRANSACTION_ID.is_valid());
        assert!(!BOOTSTRAP_TRANSACTION_ID.is_normal());
        assert!(FIRST_NORMAL_TRANSACTION_ID.is_normal());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn fetch_cache_hits_committed_and_aborted() {
        use crate::shared_state::{SharedState, SharedStateConfig};
        let dir = std::env::temp_dir().join(format!(
            "pepperdb_transam_cache_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let shared = SharedState::new(SharedStateConfig {
            data_dir: Some(dir.to_string_lossy().into_owned()),
            ..SharedStateConfig::default()
        });
        fetch_cache_scope(async {
            let clog = shared.clog();
            clog.boot_strap_clog().await;
            let xid = TransactionId(10);
            clog.set_tree_status(xid, &[], XidStatus::Committed, INVALID_XLOG_REC_PTR)
                .await;
            // First fetch misses, reads clog, caches the resolved status.
            assert_eq!(
                transaction_log_fetch(clog, xid).await.0,
                XidStatus::Committed
            );
            assert!(fetch_cache_get(xid).is_some());
            // Overwrite the clog bits to ABORTED but keep the cache: a hit must
            // return the cached COMMITTED, proving the clog read was skipped.
            clog.set_tree_status(xid, &[], XidStatus::Aborted, INVALID_XLOG_REC_PTR)
                .await;
            assert_eq!(
                transaction_log_fetch(clog, xid).await.0,
                XidStatus::Committed
            );

            // An in-progress (untouched) xid is never cached.
            let other = TransactionId(11);
            assert_eq!(
                transaction_log_fetch(clog, other).await.0,
                XidStatus::InProgress
            );
            assert!(fetch_cache_get(other).is_none());
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn no_scope_bypasses_cache() {
        // Outside fetch_cache_scope the accessors no-op: get misses, put is a
        // no-op, so the slow path always reads clog (still correct).
        assert!(fetch_cache_get(TransactionId(10)).is_none());
        fetch_cache_put(
            TransactionId(10),
            XidStatus::Committed,
            INVALID_XLOG_REC_PTR,
        );
        assert!(fetch_cache_get(TransactionId(10)).is_none());
    }

    #[test]
    fn full_transaction_id_ord_is_total_numeric() {
        use crate::access::transam::full_transaction_id_from_u64;
        let a = full_transaction_id_from_u64(10);
        let b = full_transaction_id_from_u64(20);
        assert!(a < b);
        assert!(b > a);
        assert!(a <= a);
        assert!(a >= a);
        // No wraparound: a huge value is genuinely greater (unlike modular xids).
        let huge = full_transaction_id_from_u64(0xFFFF_FFFF_0000_0000);
        assert!(b < huge);
    }
}
