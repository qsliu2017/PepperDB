//! Translated from PostgreSQL src/backend/access/transam/transam.c
//!
//! High-level transaction-status access: xid commit/abort tests over clog +
//! subtrans, the modulo-2^32 xid comparators, and the commit-tree setters.
//! Also home to `VariableCache` (the ex-`TransamVariables` shared struct,
//! design step14 section 3), reached via `shared.variable_cache()`.
//!
//! The single-item TransactionLogFetch cache is dropped for the foundation
//! (design step14 section 4: simplest correct is to always read clog); add it
//! back as a per-task task_local later if profiling wants it. TODO(perf).

use std::sync::Mutex;

use crate::access::clog::XidStatus;
use crate::access::transam::{
    transaction_id_is_normal, transaction_id_is_valid, FullTransactionId, TransamVariablesData,
};
use crate::access::xlogdefs::{XLogRecPtr, INVALID_XLOG_REC_PTR};
use crate::c::TransactionId;
use crate::shared_state::SharedState;
use std::sync::Arc;

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
                xact_completion_count: 0,
                oldest_clog_xid: crate::access::transam::FIRST_NORMAL_TRANSACTION_ID,
            }),
        }
    }

    /// Run `f` under the variable-cache lock. Caller must not `.await` inside.
    pub fn with<R>(&self, f: impl FnOnce(&mut TransamVariablesData) -> R) -> R {
        f(&mut self.inner.lock().unwrap())
    }
}

impl Default for VariableCache {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// xid comparison (transam.c TransactionIdPrecedes/Follows[OrEquals])
// ---------------------------------------------------------------------------

/// transam.c TransactionIdPrecedes: id1 logically < id2 (modulo-2^32 for normal
/// xids; plain unsigned for permanent xids).
pub fn transaction_id_precedes(id1: TransactionId, id2: TransactionId) -> bool {
    if !transaction_id_is_normal(id1) || !transaction_id_is_normal(id2) {
        return id1.0 < id2.0;
    }
    (id1.0.wrapping_sub(id2.0) as i32) < 0
}

pub fn transaction_id_precedes_or_equals(id1: TransactionId, id2: TransactionId) -> bool {
    if !transaction_id_is_normal(id1) || !transaction_id_is_normal(id2) {
        return id1.0 <= id2.0;
    }
    (id1.0.wrapping_sub(id2.0) as i32) <= 0
}

pub fn transaction_id_follows(id1: TransactionId, id2: TransactionId) -> bool {
    if !transaction_id_is_normal(id1) || !transaction_id_is_normal(id2) {
        return id1.0 > id2.0;
    }
    (id1.0.wrapping_sub(id2.0) as i32) > 0
}

pub fn transaction_id_follows_or_equals(id1: TransactionId, id2: TransactionId) -> bool {
    if !transaction_id_is_normal(id1) || !transaction_id_is_normal(id2) {
        return id1.0 >= id2.0;
    }
    (id1.0.wrapping_sub(id2.0) as i32) >= 0
}

/// transam.c TransactionIdLatest: newest xid among a main xact and children.
pub fn transaction_id_latest(mainxid: TransactionId, xids: &[TransactionId]) -> TransactionId {
    let mut result = mainxid;
    for &x in xids.iter().rev() {
        if transaction_id_precedes(result, x) {
            result = x;
        }
    }
    result
}

// ---------------------------------------------------------------------------
// commit-status fetch (transam.c TransactionLogFetch / DidCommit / DidAbort)
// ---------------------------------------------------------------------------

/// transam.c TransactionLogFetch: fetch the commit status for `xid`. Permanent
/// xids resolve without touching clog. No single-item cache (TODO(perf)).
async fn transaction_log_fetch(shared: &Arc<SharedState>, xid: TransactionId) -> XidStatus {
    if !transaction_id_is_normal(xid) {
        if xid.0 == crate::access::transam::BOOTSTRAP_TRANSACTION_ID.0
            || xid.0 == crate::access::transam::FROZEN_TRANSACTION_ID.0
        {
            return XidStatus::Committed;
        }
        return XidStatus::Aborted;
    }
    crate::backend::access::transam::clog::TransactionIdGetStatus(shared, xid).await.0
}

/// transam.c TransactionIdDidCommit. `transaction_xmin` is the per-backend
/// oldest-visible xid (snapmgr); below it we can't consult subtrans.
pub async fn transaction_id_did_commit(
    shared: &Arc<SharedState>,
    xid: TransactionId,
    transaction_xmin: TransactionId,
) -> bool {
    match transaction_log_fetch(shared, xid).await {
        XidStatus::Committed => true,
        XidStatus::SubCommitted => {
            if transaction_id_precedes(xid, transaction_xmin) {
                return false;
            }
            let parent =
                crate::backend::access::transam::subtrans::sub_trans_get_parent(shared, xid).await;
            if !transaction_id_is_valid(parent) {
                // see transam.c notes: parent crashed without cleanup.
                return false;
            }
            Box::pin(transaction_id_did_commit(shared, parent, transaction_xmin)).await
        }
        _ => false,
    }
}

/// transam.c TransactionIdDidAbort.
pub async fn transaction_id_did_abort(
    shared: &Arc<SharedState>,
    xid: TransactionId,
    transaction_xmin: TransactionId,
) -> bool {
    match transaction_log_fetch(shared, xid).await {
        XidStatus::Aborted => true,
        XidStatus::SubCommitted => {
            if transaction_id_precedes(xid, transaction_xmin) {
                return true;
            }
            let parent =
                crate::backend::access::transam::subtrans::sub_trans_get_parent(shared, xid).await;
            if !transaction_id_is_valid(parent) {
                return true;
            }
            Box::pin(transaction_id_did_abort(shared, parent, transaction_xmin)).await
        }
        _ => false,
    }
}

// ---------------------------------------------------------------------------
// commit-tree setters (transam.c TransactionIdCommitTree / AbortTree / Async)
// ---------------------------------------------------------------------------

/// transam.c TransactionIdCommitTree: mark the tree committed (sync commit).
pub async fn transaction_id_commit_tree(
    shared: &Arc<SharedState>,
    xid: TransactionId,
    xids: &[TransactionId],
) {
    crate::backend::access::transam::clog::TransactionIdSetTreeStatus(
        shared,
        xid,
        xids,
        XidStatus::Committed,
        INVALID_XLOG_REC_PTR,
    )
    .await;
}

/// transam.c TransactionIdAsyncCommitTree: async commit (commit-record LSN).
pub async fn transaction_id_async_commit_tree(
    shared: &Arc<SharedState>,
    xid: TransactionId,
    xids: &[TransactionId],
    lsn: XLogRecPtr,
) {
    crate::backend::access::transam::clog::TransactionIdSetTreeStatus(
        shared,
        xid,
        xids,
        XidStatus::Committed,
        lsn,
    )
    .await;
}

/// transam.c TransactionIdAbortTree: mark the tree aborted.
pub async fn transaction_id_abort_tree(
    shared: &Arc<SharedState>,
    xid: TransactionId,
    xids: &[TransactionId],
) {
    crate::backend::access::transam::clog::TransactionIdSetTreeStatus(
        shared,
        xid,
        xids,
        XidStatus::Aborted,
        INVALID_XLOG_REC_PTR,
    )
    .await;
}

/// transam.c TransactionIdGetCommitLSN: an LSN late enough to guarantee the
/// commit record is durable once flushed. No cache (TODO(perf)).
pub async fn transaction_id_get_commit_lsn(
    shared: &Arc<SharedState>,
    xid: TransactionId,
) -> XLogRecPtr {
    if !transaction_id_is_normal(xid) {
        return INVALID_XLOG_REC_PTR;
    }
    crate::backend::access::transam::clog::TransactionIdGetStatus(shared, xid).await.1
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
        assert!(transaction_id_precedes(BOOTSTRAP_TRANSACTION_ID, FIRST_NORMAL_TRANSACTION_ID));
        assert!(transaction_id_precedes(FROZEN_TRANSACTION_ID, FIRST_NORMAL_TRANSACTION_ID));
        assert!(transaction_id_precedes(BOOTSTRAP_TRANSACTION_ID, FROZEN_TRANSACTION_ID));
        // A permanent xid never "follows" a normal one.
        assert!(!transaction_id_follows(FROZEN_TRANSACTION_ID, FIRST_NORMAL_TRANSACTION_ID));
    }

    #[test]
    fn precedes_normal_xids_use_modulo_2_32() {
        let a = TransactionId(100);
        let b = TransactionId(200);
        assert!(transaction_id_precedes(a, b));
        assert!(transaction_id_follows(b, a));
        assert!(transaction_id_precedes_or_equals(a, a));
        assert!(transaction_id_follows_or_equals(a, a));
    }

    #[test]
    fn precedes_handles_wraparound() {
        // A small xid just after wraparound logically follows a huge one near the
        // top of the space (modulo-2^32 comparison).
        let high = TransactionId(0xFFFF_FF00);
        let low = TransactionId(10); // 10 is normal (>= 3)
        assert!(transaction_id_precedes(high, low));
        assert!(transaction_id_follows(low, high));
    }

    #[test]
    fn latest_picks_newest() {
        let main = TransactionId(50);
        let subs = [TransactionId(40), TransactionId(70), TransactionId(60)];
        assert_eq!(transaction_id_latest(main, &subs).0, 70);
    }
}
