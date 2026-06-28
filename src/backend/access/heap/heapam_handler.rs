//! Heap table AM handler. Translated from
//! backend/access/heap/heapam_handler.c.
//!
//! In the closed-enum AM model (`access/tableam.rs` `TableAmKind`), the C
//! `heapam_methods` vtable of fn pointers becomes this set of `pub` handler
//! functions; `table_*` dispatch (in `table/tableam.rs`) matches on
//! `TableAmKind::Heap` and calls them. M2 wires the slot/scan/insert callbacks
//! the executor needs for a CREATE/INSERT/SELECT round-trip; the rest are grow
//! guards added at later milestones (M6 index fetch, M8 update/delete/lock, plus
//! analyze/vacuum/copy/sample).
//!
//! Async coloring: the handlers that touch storage (`heapam_tuple_insert`,
//! `heap_getnextslot`) are `async`, delegating to the async `heapam` core.

use std::sync::Arc;

use crate::access::sdir::ScanDirection;
use crate::backend::access::heap::heapam::{heap_getnext, HeapScanDescData, SendRelation};
use crate::c::CommandId;
use crate::executor::tuptable::{TupleTableSlot, TupleTableSlotOps};
use crate::shared_state::SharedState;
use crate::utils::relcache::Relation;

/// `heapam_slot_callbacks`: the slot implementation suitable for heap tuples
/// (`TTSOpsBufferHeapTuple` in C -- a buffer-pinned heap-tuple slot).
///
/// Grow guard: the buffer-heap-tuple slot ops are an executor concern not yet
/// ported (only `TTSOpsVirtual` exists today). The slot-based scan path that
/// needs it is staged together with the executor slot-store routines; the
/// heap-level scan (`heap_getnext`) is the complete M2 path.
pub fn heapam_slot_callbacks(_relation: Relation) -> &'static dyn TupleTableSlotOps {
    unimplemented!("heapam_slot_callbacks: TTSOpsBufferHeapTuple (executor slot ops, staged with the slot path)")
}

/// `heapam_tuple_insert`: insert the tuple materialized in `slot` into
/// `relation`, copying the resulting TID back into the slot.
///
/// Grow guard on the slot half: fetching the heap tuple out of the slot
/// (`ExecFetchSlotHeapTuple`) is an executor routine not yet ported. The storage
/// half (`heap_insert`) is complete; callers with a built `HeapTuple` use
/// `heap_insert` directly until the slot store/fetch lands.
#[allow(
    clippy::future_not_send,
    reason = "staged: the executor TupleTableSlot is !Send and ExecFetchSlotHeapTuple is unported; revisit when the executor slot machinery (Send-correct) lands. The M2-complete insert path (heap_insert) is Send."
)]
pub async fn heapam_tuple_insert(
    _shared: &Arc<SharedState>,
    _relation: SendRelation,
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
#[allow(
    clippy::future_not_send,
    reason = "staged: the executor TupleTableSlot is !Send and ExecStoreBufferHeapTuple is unported; revisit when the executor slot machinery (Send-correct) lands. The M2-complete scan path (heap_getnext) is Send."
)]
pub async fn heap_getnextslot(
    shared: &Arc<SharedState>,
    scan: &mut HeapScanDescData,
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

/// `heapam_tuple_update`: M8.
pub fn heapam_tuple_update() {
    unimplemented!("heapam_tuple_update: M8")
}

/// `heapam_tuple_delete`: M8.
pub fn heapam_tuple_delete() {
    unimplemented!("heapam_tuple_delete: M8")
}

/// `heapam_tuple_lock`: M8.
pub fn heapam_tuple_lock() {
    unimplemented!("heapam_tuple_lock: M8")
}

/// `heap_multi_insert`: M5 (bulk COPY/INSERT...SELECT path).
pub fn heap_multi_insert() {
    unimplemented!("heap_multi_insert: M5")
}
