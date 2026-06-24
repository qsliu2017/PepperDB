//! Translated from PostgreSQL src/include/access/rewriteheap.h

use crate::access::htup::HeapTuple;
use crate::c::{MultiXactId, TransactionId};
use crate::storage::itemptr::ItemPointerData;
use crate::storage::relfilelocator::RelFileLocator;
use crate::utils::relcache::Relation;

/// Opaque rewrite state (definition private to rewriteheap.c).
pub struct RewriteStateData {
    _private: [u8; 0],
}
pub type RewriteState = *mut RewriteStateData; // TODO(ptr)

pub fn begin_heap_rewrite(
    _old_heap: Relation,
    _new_heap: Relation,
    _oldest_xmin: TransactionId,
    _freeze_xid: TransactionId,
    _cutoff_multi: MultiXactId,
) -> RewriteState {
    unimplemented!()
}
pub fn end_heap_rewrite(_state: RewriteState) {
    unimplemented!()
}
pub fn rewrite_heap_tuple(_state: RewriteState, _old_tuple: HeapTuple, _new_tuple: HeapTuple) {
    unimplemented!()
}
pub fn rewrite_heap_dead_tuple(_state: RewriteState, _old_tuple: HeapTuple) -> bool {
    unimplemented!()
}

/// On-disk data format for an individual logical rewrite mapping.
#[repr(C)]
pub struct LogicalRewriteMappingData {
    pub old_locator: RelFileLocator,
    pub new_locator: RelFileLocator,
    pub old_tid: ItemPointerData,
    pub new_tid: ItemPointerData,
}

pub const LOGICAL_REWRITE_FORMAT: &str = "map-%x-%x-%X_%X-%x-%x";

pub fn CheckPointLogicalRewriteHeap() {
    unimplemented!()
}
