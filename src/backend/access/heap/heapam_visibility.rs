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
use crate::backend::access::transam::transam::transaction_id_did_commit;
use crate::backend::access::transam::xact::TransactionIdIsCurrentTransactionId;
use crate::backend::storage::ipc::procarray::transaction_xmin;
use crate::backend::utils::time::snapmgr::XidInMVCCSnapshot;
use crate::c::TransactionId;
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
}
