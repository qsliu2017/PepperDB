//! Translated from PostgreSQL src/include/catalog/indexing.h

use crate::access::htup::HeapTuple;
use crate::nodes::execnodes::ResultRelInfo;
use crate::executor::tuptable::TupleTableSlot;
use crate::storage::itemptr::ItemPointerData;
use crate::utils::rel::Relation;

/// State used by CatalogOpenIndexes and friends; aliases the executor's
/// ResultRelInfo but kept distinct to decouple callers.
pub type CatalogIndexState = *mut ResultRelInfo; // TODO(ptr)

/// Cap on bytes allocated for multi-inserts with system catalogs.
pub const MAX_CATALOG_MULTI_INSERT_BYTES: usize = 65535;

pub fn CatalogOpenIndexes(heap_rel: &Relation) -> CatalogIndexState {
    let _ = heap_rel;
    unimplemented!()
}

pub fn CatalogCloseIndexes(indstate: CatalogIndexState) {
    let _ = indstate;
    unimplemented!()
}

pub fn CatalogTupleInsert(heap_rel: &Relation, tup: HeapTuple) {
    let _ = (heap_rel, tup);
    unimplemented!()
}

pub fn CatalogTupleInsertWithInfo(
    heap_rel: &Relation,
    tup: HeapTuple,
    indstate: CatalogIndexState,
) {
    let _ = (heap_rel, tup, indstate);
    unimplemented!()
}

pub fn CatalogTuplesMultiInsertWithInfo(
    heap_rel: &Relation,
    slot: &mut [*mut TupleTableSlot],
    indstate: CatalogIndexState,
) {
    let _ = (heap_rel, slot, indstate);
    unimplemented!()
}

pub fn CatalogTupleUpdate(heap_rel: &Relation, otid: &ItemPointerData, tup: HeapTuple) {
    let _ = (heap_rel, otid, tup);
    unimplemented!()
}

pub fn CatalogTupleUpdateWithInfo(
    heap_rel: &Relation,
    otid: &ItemPointerData,
    tup: HeapTuple,
    indstate: CatalogIndexState,
) {
    let _ = (heap_rel, otid, tup, indstate);
    unimplemented!()
}

pub fn CatalogTupleDelete(heap_rel: &Relation, tid: &ItemPointerData) {
    let _ = (heap_rel, tid);
    unimplemented!()
}

// The DECLARE_*_INDEX/DECLARE_OID bootstrap macros in catalog/indexing.h are BKI
// metadata emitted by genbki; not modeled here (handled by build.rs later).
