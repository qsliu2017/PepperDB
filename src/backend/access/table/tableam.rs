//! Table AM dispatch wrappers. Translated from
//! backend/access/table/tableam.c plus the `table_*` static-inline wrappers in
//! tableam.h.
//!
//! In the closed-enum AM model (`access/tableam.rs` `TableAmKind`), each
//! `rel->rd_tableam->callback(...)` indirection becomes a match on the relation's
//! AM kind dispatching to the heap handler (`access/heap/heapam_handler.rs`).
//! Heap is the only in-tree AM, so M2 dispatches straight to it.
//!
//! Async coloring: the scan/insert wrappers reach the async heap core, so they
//! are `async`.

use std::sync::Arc;

use crate::access::sdir::ScanDirection;
use crate::access::tableam::{ScanOptions, TableAmKind};
use crate::backend::access::heap::heapam::{
    heap_beginscan, heap_endscan, heap_getnext, heap_rescan, HeapScanDescData, SendRelation,
};
use crate::backend::access::table::tableamapi::get_table_am_routine;
use crate::c::CommandId;
use crate::executor::tuptable::TupleTableSlot;
use crate::shared_state::SharedState;
use crate::utils::relcache::Relation;
use crate::utils::snapshot::SnapshotData;

/// The AM kind of a relation (its `rd_tableam`, resolved through the closed
/// enum). M2: every relation that reaches table AM is heap.
fn rel_am_kind(relation: Relation) -> TableAmKind {
    // SAFETY: live relation.
    let amhandler = unsafe { (*relation).rd_amhandler };
    get_table_am_routine(amhandler)
}

/// `table_tuple_insert`: insert the tuple in `slot` into `rel`.
///
/// Grow guard on the slot half (executor `ExecFetchSlotHeapTuple` is staged);
/// dispatches to the heap handler. Callers holding a built `HeapTuple` use
/// `heap_insert` directly for M2.
#[allow(
    clippy::future_not_send,
    reason = "staged: dispatches to the executor-slot insert (TupleTableSlot is !Send, unported); the M2-complete insert path (heap_insert) is Send"
)]
pub async fn table_tuple_insert(
    shared: &Arc<SharedState>,
    rel: SendRelation,
    slot: &mut TupleTableSlot,
    cid: CommandId,
    options: i32,
) {
    match rel_am_kind(rel.get()) {
        TableAmKind::Heap => {
            crate::backend::access::heap::heapam_handler::heapam_tuple_insert(
                shared, rel, slot, cid, options,
            )
            .await;
        }
    }
}

/// `table_beginscan`: start a sequential scan of `rel` under `snapshot`. Returns
/// the heap scan descriptor (the AM-specific handle). M2: forward seqscan,
/// page-at-a-time, no scan keys.
pub fn table_beginscan(rel: SendRelation, snapshot: Arc<SnapshotData>) -> Box<HeapScanDescData> {
    let flags = ScanOptions::TYPE_SEQSCAN
        | ScanOptions::ALLOW_STRAT
        | ScanOptions::ALLOW_SYNC
        | ScanOptions::ALLOW_PAGEMODE;
    match rel_am_kind(rel.get()) {
        TableAmKind::Heap => heap_beginscan(rel, snapshot, 0, flags),
    }
}

/// `table_endscan`: release a scan.
pub fn table_endscan(shared: &Arc<SharedState>, scan: &mut HeapScanDescData) {
    match rel_am_kind(scan.base.rs_rd) {
        TableAmKind::Heap => heap_endscan(shared, scan),
    }
}

/// `table_rescan`: restart a scan from the beginning.
pub fn table_rescan(shared: &Arc<SharedState>, scan: &mut HeapScanDescData) {
    match rel_am_kind(scan.base.rs_rd) {
        TableAmKind::Heap => heap_rescan(shared, scan),
    }
}

/// `table_scan_getnextslot`: fetch the next tuple of `scan` into `slot`; false at
/// end of scan.
///
/// Grow guard on the slot store (executor `ExecStoreBufferHeapTuple` staged);
/// the scan/visibility core is reachable via `table_scan_getnext` /
/// `heap_getnext`.
#[allow(
    clippy::future_not_send,
    reason = "staged: dispatches to the executor-slot store (TupleTableSlot is !Send, unported); the M2-complete scan path (table_scan_getnext / heap_getnext) is Send"
)]
pub async fn table_scan_getnextslot(
    shared: &Arc<SharedState>,
    scan: &mut HeapScanDescData,
    direction: ScanDirection,
    slot: &mut TupleTableSlot,
) -> bool {
    // C sets slot->tts_tableOid = RelationGetRelid(scan->rs_rd) here.
    // SAFETY: live relation.
    slot.tableOid = unsafe { (*scan.base.rs_rd).rd_id };
    match rel_am_kind(scan.base.rs_rd) {
        TableAmKind::Heap => {
            crate::backend::access::heap::heapam_handler::heap_getnextslot(
                shared, scan, direction, slot,
            )
            .await
        }
    }
}

/// `table_scan_getnext` (heap-level convenience, the complete M2 path): the next
/// visible tuple as a `HeapTuple`, or `None` at end of scan. The slot-based
/// `table_scan_getnextslot` layers on top once the executor slot store lands.
pub async fn table_scan_getnext(
    shared: &Arc<SharedState>,
    scan: &mut HeapScanDescData,
    direction: ScanDirection,
) -> Option<crate::access::htup::HeapTuple> {
    match rel_am_kind(scan.base.rs_rd) {
        TableAmKind::Heap => heap_getnext(shared, scan, direction).await,
    }
}
