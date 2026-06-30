//! Heap table AM handler. Translated from
//! backend/access/heap/heapam_handler.c.
//!
//! In the closed-enum AM model (`access/tableam.rs` `TableAmKind`), the C
//! `heapam_methods` vtable of fn pointers becomes this set of `pub` handler
//! functions; `table_*` dispatch (in `table/tableam.rs`) matches on
//! `TableAmKind::Heap` and calls them. M2 wires the slot/scan/insert callbacks
//! the executor needs for a CREATE/INSERT/SELECT round-trip; M8 wires
//! update/delete/lock (the slot fetch/store half stays staged with the executor
//! slot routines); the rest are grow guards added at later milestones (M6 index
//! fetch, plus analyze/vacuum/copy/sample).
//!
//! Async coloring: the handlers that touch storage (`heapam_tuple_insert`,
//! `heap_getnextslot`) are `async`, delegating to the async `heapam` core.

use std::sync::Arc;

use crate::access::htup::HeapTupleData;
use crate::access::sdir::ScanDirection;
use crate::access::tableam::{TM_FailureData, TM_Result, TU_UpdateIndexes};
use crate::backend::access::heap::heapam::{
    heap_delete, heap_getnext, heap_lock_tuple, heap_update, HeapScanDescData,
};
use crate::c::CommandId;
use crate::executor::tuptable::{TupleTableSlot, TupleTableSlotOps};
use crate::nodes::lockoptions::{LockTupleMode, LockWaitPolicy};
use crate::shared_state::SharedState;
use crate::storage::buf::Buffer;
use crate::storage::itemptr::ItemPointerData;
use crate::utils::rel::RelationData;
use crate::utils::snapshot::SnapshotData;

/// `heapam_slot_callbacks`: the slot implementation suitable for heap tuples
/// (`TTSOpsBufferHeapTuple` in C -- a buffer-pinned heap-tuple slot).
///
/// Grow guard: the buffer-heap-tuple slot ops are an executor concern not yet
/// ported (only `TTSOpsVirtual` exists today). The slot-based scan path that
/// needs it is staged together with the executor slot-store routines; the
/// heap-level scan (`heap_getnext`) is the complete M2 path.
pub fn heapam_slot_callbacks(_relation: &RelationData) -> &'static dyn TupleTableSlotOps {
    unimplemented!("heapam_slot_callbacks: TTSOpsBufferHeapTuple (executor slot ops, staged with the slot path)")
}

/// `heapam_tuple_insert`: insert the tuple materialized in `slot` into
/// `relation`, copying the resulting TID back into the slot.
///
/// Grow guard on the slot half: fetching the heap tuple out of the slot
/// (`ExecFetchSlotHeapTuple`) is an executor routine not yet ported. The storage
/// half (`heap_insert`) is complete; callers with a built `HeapTuple` use
/// `heap_insert` directly until the slot store/fetch lands.
pub async fn heapam_tuple_insert(
    _shared: &Arc<SharedState>,
    _relation: &RelationData,
    _slot: &mut TupleTableSlot,
    _cid: CommandId,
    _options: i32,
) {
    unimplemented!("heapam_tuple_insert: ExecFetchSlotHeapTuple (executor slot fetch, staged); use heap_insert directly")
}

/// `heap_getnextslot`: advance `scan` and store the next visible tuple in `slot`;
/// returns false at end of scan.
///
/// Grow guard on the slot half: `ExecStoreBufferHeapTuple` / `ExecClearTuple`
/// are executor routines not yet ported. The scan/visibility half
/// (`heap_getnext`) is complete; recover tuples via `heap_getnext` +
/// `heap_deform_tuple` until the slot-store routines land.
pub async fn heap_getnextslot(
    shared: &Arc<SharedState>,
    scan: &mut HeapScanDescData<'_, '_>,
    direction: ScanDirection,
    _slot: &mut TupleTableSlot,
) -> bool {
    let tuple = heap_getnext(shared, scan, direction).await;
    if tuple.is_none() {
        // ExecClearTuple(slot) -- staged with the executor slot routines.
        return false;
    }
    // ExecStoreBufferHeapTuple(&scan.ctup, slot, scan.cbuf) -- staged.
    unimplemented!("heap_getnextslot: ExecStoreBufferHeapTuple (executor slot store, staged)")
}

// ---------------------------------------------------------------------------
// Grow guards: later-milestone vtable slots.
// ---------------------------------------------------------------------------

/// `heapam_index_fetch_begin` and friends: M6 (index scan -> heap fetch).
pub fn heapam_index_fetch_begin() {
    unimplemented!("heapam_index_fetch_*: M6 (index fetch)")
}

/// `heapam_tuple_delete`: table-AM delete callback. Thin wrapper over
/// `heap_delete` (the C body adds only an index-cleanup comment).
#[allow(clippy::too_many_arguments, reason = "mirrors the C heapam_tuple_delete vtable signature")]
pub async fn heapam_tuple_delete(
    shared: &Arc<SharedState>,
    relation: &RelationData,
    tid: &ItemPointerData,
    cid: CommandId,
    _snapshot: Option<&SnapshotData>,
    crosscheck: Option<&SnapshotData>,
    wait: bool,
    changing_part: bool,
) -> (TM_Result, TM_FailureData) {
    heap_delete(shared, relation, tid, cid, crosscheck, wait, changing_part).await
}

/// `heapam_tuple_update`: table-AM update callback. Wraps `heap_update`.
///
/// Grow guard on the slot half: the C callback fetches the heap tuple out of the
/// slot (`ExecFetchSlotHeapTuple`) and copies the resulting TID back into the
/// slot's `tts_tid`. Those executor slot routines are not yet ported (the same
/// staging as `heapam_tuple_insert`); callers with a built `HeapTuple` use this
/// form, which takes the new tuple directly. The new tuple's `t_self` is updated
/// in place by `heap_update`.
#[allow(clippy::too_many_arguments, reason = "mirrors the C heapam_tuple_update vtable signature")]
pub async fn heapam_tuple_update(
    shared: &Arc<SharedState>,
    relation: &RelationData,
    otid: &ItemPointerData,
    newtup: &mut HeapTupleData,
    cid: CommandId,
    _snapshot: Option<&SnapshotData>,
    crosscheck: Option<&SnapshotData>,
    wait: bool,
) -> (TM_Result, LockTupleMode, TU_UpdateIndexes) {
    heap_update(shared, relation, otid, newtup, cid, crosscheck, wait).await
}

/// `heapam_tuple_lock`: table-AM row-lock callback. Wraps `heap_lock_tuple`.
///
/// Grow guard on the slot half: the C callback addresses the tuple via a
/// `BufferHeapTupleTableSlot` and runs the update-chain-follow retry loop
/// (`TUPLE_LOCK_FLAG_FIND_LAST_VERSION`). The slot store + chain-follow are staged
/// (with the executor slot routines and the multixact/wait path); the
/// single-locker form takes the tuple (with `t_self` set) directly and returns the
/// pinned buffer.
#[allow(clippy::too_many_arguments, reason = "mirrors the C heapam_tuple_lock vtable signature")]
pub async fn heapam_tuple_lock(
    shared: &Arc<SharedState>,
    relation: &RelationData,
    tuple: &mut HeapTupleData,
    _snapshot: Option<&SnapshotData>,
    cid: CommandId,
    mode: LockTupleMode,
    wait_policy: LockWaitPolicy,
    follow_updates: bool,
) -> (TM_Result, TM_FailureData, Buffer) {
    heap_lock_tuple(shared, relation, tuple, cid, mode, wait_policy, follow_updates).await
}

/// `heap_multi_insert`: M5 (bulk COPY/INSERT...SELECT path).
pub fn heap_multi_insert() {
    unimplemented!("heap_multi_insert: M5")
}
