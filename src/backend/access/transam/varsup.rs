//! Translated from PostgreSQL src/backend/access/transam/varsup.c
//!
//! OID & XID generation over the `VariableCache` (ex-`TransamVariables`).
//! Reached via `shared.variable_cache()`. The XidGenLock / OidGenLock split
//! collapses to the single VariableCache `Mutex`; we compute what's needed under
//! the lock, drop it, then await the async clog/subtrans extends (design s3).
//!
//! Staging (design step14 section 0): recording the new xid into MyProc /
//! ProcGlobal is owned by step 15; that store is omitted here (the proc array is
//! still a stub). The wraparound warn/stop signaling and get_database_name land
//! on their absence (autovacuum/syscache are later) -- we keep the limit math
//! and the stop-limit ERROR, and drop the WARNING database-name lookups.

use std::sync::Arc;

use crate::access::transam::{
    full_transaction_id_advance, transaction_id_advance, xid_from_full_transaction_id,
    FullTransactionId, FIRST_GENBKI_OBJECT_ID, FIRST_NORMAL_OBJECT_ID, FIRST_NORMAL_TRANSACTION_ID,
    FIRST_UNPINNED_OBJECT_ID, MAX_TRANSACTION_ID,
};
use crate::backend::access::transam::transam::{
    transaction_id_follows_or_equals, transaction_id_precedes,
};
use crate::c::TransactionId;
use crate::postgres_ext::Oid;
use crate::shared_state::SharedState;

/// Number of OIDs to prefetch per XLOG write (varsup.c VAR_OID_PREFETCH).
const VAR_OID_PREFETCH: u32 = 8192;

/// GUC default (TODO(guc): autovacuum_freeze_max_age). 200M.
const AUTOVACUUM_FREEZE_MAX_AGE: u32 = 200_000_000;

/// varsup.c VarsupShmemSize (estimate under the Arc model).
pub fn varsup_shmem_size() -> usize {
    std::mem::size_of::<crate::access::transam::TransamVariablesData>()
}

/// varsup.c GetNewTransactionId: allocate the next FullTransactionId.
///
/// Compute the new xid + which limits/extends are needed under the lock, drop
/// the lock, run the async clog/subtrans extends, then re-take the lock to bump
/// nextXid (varsup.c bumps after a successful ExtendCLOG). `is_sub_xact` only
/// affects the (deferred) proc-array bookkeeping.
pub async fn GetNewTransactionId(
    shared: &Arc<SharedState>,
    _is_sub_xact: bool,
) -> FullTransactionId {
    let vc = shared.variable_cache();

    // Snapshot nextXid + the wrap limits under the lock.
    let (full_xid, xid, stop_limit, past_stop) = vc.with(|v| {
        let full = v.next_xid;
        let xid = xid_from_full_transaction_id(full);
        let past_stop = transaction_id_follows_or_equals(xid, v.xid_stop_limit)
            && crate::access::transam::transaction_id_is_valid(v.xid_stop_limit);
        (full, xid, v.xid_stop_limit, past_stop)
    });
    let _ = stop_limit;

    // Refuse to assign past the stop limit (wraparound protection). varsup.c
    // does the database-name lookup for the message; that needs syscache (later).
    if past_stop {
        panic!(
            "database is not accepting commands that assign new transaction IDs \
             to avoid wraparound data loss (xid {})",
            xid.0
        );
    }

    // Extend clog/subtrans for the page this xid lands on (no-ops except at a
    // page boundary). These await SLRU I/O, so they run with the lock dropped.
    crate::backend::access::transam::clog::ExtendCLOG(shared, xid).await;
    crate::backend::access::transam::subtrans::extend_subtrans(shared, xid).await;

    // Now advance nextXid (only after a successful extend). Re-read to be safe
    // under concurrency; advance from the value we extended for if unchanged.
    vc.with(|v| {
        if xid_from_full_transaction_id(v.next_xid).0 == xid.0 {
            full_transaction_id_advance(&mut v.next_xid);
        }
    });

    // TODO(step15): store xid into MyProc->xid / ProcGlobal->xids[].
    full_xid
}

/// varsup.c ReadNextFullTransactionId: read nextXid without allocating.
pub fn ReadNextFullTransactionId(shared: &Arc<SharedState>) -> FullTransactionId {
    shared.variable_cache().with(|v| v.next_xid)
}

/// varsup.c AdvanceNextFullTransactionIdPastXid (recovery / two-phase startup).
pub fn AdvanceNextFullTransactionIdPastXid(shared: &Arc<SharedState>, xid: TransactionId) {
    let vc = shared.variable_cache();
    vc.with(|v| {
        let next_xid = xid_from_full_transaction_id(v.next_xid);
        if !transaction_id_follows_or_equals(xid, next_xid) {
            return;
        }
        let mut advanced = xid;
        transaction_id_advance(&mut advanced);
        let mut epoch = crate::access::transam::epoch_from_full_transaction_id(v.next_xid);
        if advanced.0 < next_xid.0 {
            epoch += 1;
        }
        v.next_xid =
            crate::access::transam::full_transaction_id_from_epoch_and_xid(epoch, advanced);
    });
}

/// varsup.c AdvanceOldestClogXid.
pub fn AdvanceOldestClogXid(shared: &Arc<SharedState>, oldest_datfrozenxid: TransactionId) {
    shared.variable_cache().with(|v| {
        if transaction_id_precedes(v.oldest_clog_xid, oldest_datfrozenxid) {
            v.oldest_clog_xid = oldest_datfrozenxid;
        }
    });
}

/// varsup.c SetTransactionIdLimit: recompute the wrap/stop/warn/vac limits.
pub fn SetTransactionIdLimit(
    shared: &Arc<SharedState>,
    oldest_datfrozenxid: TransactionId,
    oldest_datoid: Oid,
) {
    debug_assert!(crate::access::transam::transaction_id_is_normal(oldest_datfrozenxid));

    let mut xid_wrap_limit = oldest_datfrozenxid.0.wrapping_add(MAX_TRANSACTION_ID.0 >> 1);
    if xid_wrap_limit < FIRST_NORMAL_TRANSACTION_ID.0 {
        xid_wrap_limit = xid_wrap_limit.wrapping_add(FIRST_NORMAL_TRANSACTION_ID.0);
    }
    let mut xid_stop_limit = xid_wrap_limit.wrapping_sub(3_000_000);
    if xid_stop_limit < FIRST_NORMAL_TRANSACTION_ID.0 {
        xid_stop_limit = xid_stop_limit.wrapping_sub(FIRST_NORMAL_TRANSACTION_ID.0);
    }
    let mut xid_warn_limit = xid_wrap_limit.wrapping_sub(40_000_000);
    if xid_warn_limit < FIRST_NORMAL_TRANSACTION_ID.0 {
        xid_warn_limit = xid_warn_limit.wrapping_sub(FIRST_NORMAL_TRANSACTION_ID.0);
    }
    let mut xid_vac_limit = oldest_datfrozenxid.0.wrapping_add(AUTOVACUUM_FREEZE_MAX_AGE);
    if xid_vac_limit < FIRST_NORMAL_TRANSACTION_ID.0 {
        xid_vac_limit = xid_vac_limit.wrapping_add(FIRST_NORMAL_TRANSACTION_ID.0);
    }

    shared.variable_cache().with(|v| {
        v.oldest_xid = oldest_datfrozenxid;
        v.xid_vac_limit = TransactionId(xid_vac_limit);
        v.xid_warn_limit = TransactionId(xid_warn_limit);
        v.xid_stop_limit = TransactionId(xid_stop_limit);
        v.xid_wrap_limit = TransactionId(xid_wrap_limit);
        v.oldest_xid_db = oldest_datoid;
    });
    // TODO(autovacuum): autovac-force signaling + wrap warnings deferred.
}

/// varsup.c ForceTransactionIdLimitUpdate: does the wrap-limit data need
/// recompute? The syscache database-existence check is deferred (TODO).
pub fn ForceTransactionIdLimitUpdate(shared: &Arc<SharedState>) -> bool {
    shared.variable_cache().with(|v| {
        let next_xid = xid_from_full_transaction_id(v.next_xid);
        if !crate::access::transam::transaction_id_is_normal(v.oldest_xid) {
            return true;
        }
        if !crate::access::transam::transaction_id_is_valid(v.xid_vac_limit) {
            return true;
        }
        if transaction_id_follows_or_equals(next_xid, v.xid_vac_limit) {
            return true;
        }
        // TODO(syscache): force update if oldestXidDB no longer exists.
        false
    })
}

/// varsup.c GetNewObjectId: allocate a new OID.
pub fn GetNewObjectId(shared: &Arc<SharedState>) -> Oid {
    let vc = shared.variable_cache();
    let (result, need_log, new_next) = vc.with(|v| {
        // Wraparound / first post-initdb assignment handling (standalone path;
        // the postmaster-environment fork is a later concern). TODO(guc).
        if v.next_oid.0 < FIRST_NORMAL_OBJECT_ID && v.next_oid.0 < FIRST_GENBKI_OBJECT_ID {
            v.next_oid = Oid(FIRST_NORMAL_OBJECT_ID);
            v.oid_count = 0;
        }
        let need_log = v.oid_count == 0;
        if need_log {
            // XLogPutNextOid is the durability hook (deferred); just bump count.
            // TODO(recovery): emit XLOG_NEXTOID.
            v.oid_count = VAR_OID_PREFETCH;
        }
        let result = v.next_oid;
        v.next_oid = Oid(v.next_oid.0.wrapping_add(1));
        v.oid_count -= 1;
        (result, need_log, v.next_oid)
    });
    let _ = (need_log, new_next);
    result
}

/// varsup.c SetNextObjectId (initdb only).
fn set_next_object_id(shared: &Arc<SharedState>, next_oid: Oid) {
    shared.variable_cache().with(|v| {
        if v.next_oid.0 > next_oid.0 {
            panic!("too late to advance OID counter to {}, it is now {}", next_oid.0, v.next_oid.0);
        }
        v.next_oid = next_oid;
        v.oid_count = 0;
    });
}

/// varsup.c StopGeneratingPinnedObjectIds (initdb only).
pub fn StopGeneratingPinnedObjectIds(shared: &Arc<SharedState>) {
    set_next_object_id(shared, Oid(FIRST_UNPINNED_OBJECT_ID));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shared_state::{SharedState, SharedStateConfig};

    fn temp_shared(tag: &str) -> Arc<SharedState> {
        let dir = std::env::temp_dir().join(format!(
            "pepperdb_varsup_{tag}_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        // data_dir must be set in the config so the clog/subtrans SLRUs (which
        // GetNewTransactionId extends) resolve under the tempdir at construction.
        SharedState::new(SharedStateConfig {
            data_dir: Some(dir.to_string_lossy().into_owned()),
            ..SharedStateConfig::default()
        })
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn xid_generation_advances() {
        let shared = temp_shared("xidgen");
        crate::backend::access::transam::clog::BootStrapCLOG(&shared).await;
        crate::backend::access::transam::subtrans::boot_strap_subtrans(&shared).await;
        let a = GetNewTransactionId(&shared, false).await;
        let b = GetNewTransactionId(&shared, false).await;
        assert_eq!(
            xid_from_full_transaction_id(b).0,
            xid_from_full_transaction_id(a).0 + 1
        );
    }

    #[test]
    fn oid_generation_skips_invalid() {
        let shared = temp_shared("oidgen");
        let a = GetNewObjectId(&shared);
        let b = GetNewObjectId(&shared);
        assert_ne!(a.0, 0);
        assert_eq!(b.0, a.0 + 1);
    }
}
