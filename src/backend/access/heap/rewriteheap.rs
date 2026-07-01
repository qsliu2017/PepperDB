//! Support for rewriting a heap into a new relation. Translated from the
//! M13-reachable core of `src/backend/access/heap/rewriteheap.c`.
//!
//! CLUSTER and VACUUM FULL rebuild a table by copying its live tuples into a fresh
//! heap (a new relfilenode) and then swapping the physical files. This module owns
//! the "copy live tuples into a new heap" half: `begin_heap_rewrite` opens the
//! rewrite over the new heap, `rewrite_heap_tuple` reforms one old tuple with a
//! clean, frozen header and stores it in the new heap (`raw_heap_insert`), and
//! `end_heap_rewrite` finishes and returns the tuple count. The driver
//! (`commands/cluster.rs`) scans the old heap (physical or index order), tests each
//! tuple's liveness against the VACUUM cutoff, and feeds the survivors here.
//!
//! Freezing (rules.md s4): a rewritten tuple's inserting transaction is, by
//! construction, older than the rewrite's `oldest_xmin` cutoff (only such tuples
//! are live-and-visible-to-everyone), so the new copy is stamped
//! `HEAP_INSERT_FROZEN` -- it is unconditionally visible without a clog probe. This
//! matches PG's rewrite freeze of tuples below the freeze cutoff.
//!
//! Staged (rules.md s4): the logical-decoding rewrite-mapping (`logical_*` in the C
//! file, which records old-TID -> new-TID for in-progress logical slots), the TOAST
//! chain preservation (`raw_heap_insert`'s toast handling), and the hand-assembled
//! page buffering the C `raw_heap_insert` uses to bypass the buffer manager. The M13
//! `raw_heap_insert` routes through `heap_insert` (which assembles pages, extends
//! the relation, and WAL-logs): correct, just not the bulk-load fast path. No
//! logical slots or TOAST values are on the M13 path, so the mapping/toast staging
//! is unreachable, not skipped.
//!
//! Async coloring (rules.md s5): `heap_insert` reaches the buffer pool + WAL, so the
//! rewrite is `async`; the rewrite state holds only owned/`Send` data (a borrowed
//! `Sync` relation handle + counts), so it is `Send` across `.await`.

use std::sync::Arc;

use crate::access::heapam::HEAP_INSERT_FROZEN;
use crate::access::htup::HeapTupleData;
use crate::backend::access::common::heaptuple::{heap_deform_tuple, heap_form_tuple, heap_freetuple};
use crate::backend::access::heap::heapam::heap_insert;
use crate::backend::access::transam::xact::GetCurrentCommandId;
use crate::c::TransactionId;
use crate::shared_state::SharedState;
use crate::utils::rel::RelationData;

/// `RewriteStateData`: the state threaded through a heap rewrite. Borrows the new
/// heap relation (its `Arc` owner lives in the caller's frame) and carries the
/// freeze cutoff + running tuple count. Auto-`Send`: a `&RelationData` (`Sync`)
/// plus `Copy` scalars.
pub struct RewriteState<'rel> {
    /// The heap being written into (a fresh relfilenode).
    new_heap: &'rel RelationData,
    /// Freeze cutoff: a tuple whose xmin precedes this is frozen in the new heap.
    _freeze_xid: TransactionId,
    /// Live tuples copied into the new heap so far.
    tuples_written: u64,
}

/// `begin_heap_rewrite`: start rewriting into `new_heap`. `freeze_xid` is the
/// cutoff below which rewritten tuples are frozen (PG passes the VACUUM
/// `OldestXmin`; every tuple the driver forwards is older, so all are frozen).
#[must_use]
pub fn begin_heap_rewrite(new_heap: &RelationData, freeze_xid: TransactionId) -> RewriteState<'_> {
    RewriteState { new_heap, _freeze_xid: freeze_xid, tuples_written: 0 }
}

/// `rewrite_heap_tuple`: copy one live `old_tuple` (from the old heap) into the new
/// heap. The tuple is deformed and reformed so the new copy carries a clean header
/// (the old visibility bits / TID are dropped); `raw_heap_insert` stamps it frozen
/// and stores it. `old_desc` describes the old tuple's rowtype (identical to the new
/// heap's, since the rewrite preserves the tupdesc).
pub async fn rewrite_heap_tuple(
    shared: &Arc<SharedState>,
    state: &mut RewriteState<'_>,
    old_tuple: &HeapTupleData,
    old_desc: &crate::access::tupdesc::TupleDesc,
) {
    // Reform the tuple against the (identical) new-heap descriptor, so the stored
    // copy gets a fresh header with no leftover xmin/xmax/ctid from the old heap.
    // SAFETY: old_tuple is a live owned tuple matching old_desc.
    let (values, isnull) = unsafe { heap_deform_tuple(old_tuple, old_desc) };
    let mut new_tuple = heap_form_tuple(old_desc, &values, &isnull);
    raw_heap_insert(shared, state, &mut new_tuple).await;
    heap_freetuple(new_tuple);
    state.tuples_written += 1;
}

/// `end_heap_rewrite`: finish the rewrite, returning the number of live tuples
/// written into the new heap. (The C form flushes the last buffered page; here
/// `heap_insert` already persisted every tuple, so there is nothing to flush.)
#[allow(
    clippy::needless_pass_by_value,
    reason = "consumes the rewrite state (mirrors C end_heap_rewrite freeing RewriteState); no reuse after finish"
)]
pub fn end_heap_rewrite(state: RewriteState<'_>) -> u64 {
    state.tuples_written
}

/// `raw_heap_insert`: store one prepared tuple in the new heap. The C form
/// hand-assembles pages in a private buffer to bypass the buffer manager for the
/// bulk load; the M13 form routes through `heap_insert` with `HEAP_INSERT_FROZEN`
/// (freeze the copy) + `HEAP_INSERT_SKIP_FSM` (append to the end, the rewrite fills
/// pages front-to-back). Correct and durable; the private-buffer fast path is staged.
async fn raw_heap_insert(
    shared: &Arc<SharedState>,
    state: &RewriteState<'_>,
    tuple: &mut HeapTupleData,
) {
    use crate::access::heapam::HEAP_INSERT_SKIP_FSM;
    let cid = GetCurrentCommandId(true);
    heap_insert(shared, state.new_heap, tuple, cid, HEAP_INSERT_FROZEN | HEAP_INSERT_SKIP_FSM).await;
}
