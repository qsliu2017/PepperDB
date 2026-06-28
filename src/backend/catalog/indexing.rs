//! The universal catalog-row insert. Translated from the M2-reachable parts of
//! `src/backend/catalog/indexing.c`.
//!
//! Every catalog write goes through here: `CatalogTupleInsert` does a
//! `heap_insert` into the catalog heap, then `CatalogIndexInsert` adds the row to
//! each of the catalog's indexes (so the index scans the syscache uses stay in
//! sync). `CatalogOpenIndexes` gathers the relation's open indexes once so a
//! batch of inserts amortizes the lookup (`CatalogTupleInsertWithInfo`).
//!
//! How the index list is found: in PG `CatalogOpenIndexes` calls
//! `RelationGetIndexList` + `ExecOpenIndices`, reading pg_index. The M2 relcache
//! does not populate `rd_indexlist` for the nailed catalogs, so the catalog
//! indexes are tracked in a per-task registry ([`register_catalog_index`]) that
//! `index_create` (and the bootstrap index build) writes; `CatalogOpenIndexes`
//! reads it. This is the same data PG keeps in `rd_indexlist`, sourced from
//! `index_create` instead of a pg_index scan.
//!
//! Async coloring (rules.md s5): `heap_insert` and `index_insert` reach the buffer
//! pool, so `CatalogTupleInsert*` are `async` and thread `&Arc<SharedState>`.

#![allow(
    clippy::future_not_send,
    reason = "rules.md s5: catalog inserts hold per-backend raw Relation/HeapTuple handles task-confined for the operation; same contract as relcache/genam"
)]
#![allow(
    clippy::not_unsafe_ptr_arg_deref,
    reason = "catalog routines take raw Relation/HeapTuple pointers per the C API; faithful to C"
)]

use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::Arc;

use crate::access::htup::{HeapTuple, HeapTupleData};
use crate::backend::access::common::heaptuple::heap_getattr;
use crate::backend::access::heap::heapam::{heap_insert, SendPtr};
use crate::backend::access::index::indexam::index_insert;
use crate::backend::access::transam::xact::GetCurrentCommandId;
use crate::nodes::execnodes::IndexInfo;
use crate::postgres_ext::Oid;
use crate::shared_state::SharedState;
use crate::storage::itemptr::ItemPointerData;
use crate::utils::relcache::Relation;

/// Cap on bytes allocated for multi-inserts with system catalogs (PG
/// `MAX_CATALOG_MULTI_INSERT_BYTES`). Re-exported by the header.
pub const MAX_CATALOG_MULTI_INSERT_BYTES: usize = 65535;

// ---------------------------------------------------------------------------
// Per-task catalog-index registry (the rd_indexlist stand-in)
// ---------------------------------------------------------------------------

/// One registered catalog index: the open index `Relation` and the `IndexInfo`
/// describing its key columns. The registry maps a heap relid to its indexes.
struct RegisteredIndex {
    index: Relation,
    info: IndexInfo,
}

// SAFETY: the raw `Relation` pointer + the IndexInfo's opaque cache/context are
// per-task state, task-confined for the registry's lifetime (the whole initdb /
// DDL operation runs on one task); never raced. Same contract as the genam/relcache
// Send impls.
#[allow(
    clippy::non_send_fields_in_send_ty,
    reason = "the raw Relation handle + IndexInfo opaque fields are task-confined for the registry's lifetime; see the SAFETY note"
)]
unsafe impl Send for RegisteredIndex {}

tokio::task_local! {
    static CATALOG_INDEXES: RefCell<HashMap<u32, Vec<RegisteredIndex>>>;
}

/// Establish the per-task catalog-index registry and run `fut`. Bootstrap and the
/// catalog tests scope this around the work that creates + uses catalog indexes.
pub async fn scope_async<F, T>(fut: F) -> T
where
    F: std::future::Future<Output = T>,
{
    CATALOG_INDEXES.scope(RefCell::new(HashMap::new()), fut).await
}

/// Whether the catalog-index registry is established on this task.
#[must_use]
pub fn registry_present() -> bool {
    CATALOG_INDEXES.try_with(|_| ()).is_ok()
}

/// Record that `index` (with `info`) is an index of the heap relation `heap_relid`.
/// Called by `index_create` / the bootstrap index build so `CatalogOpenIndexes` can
/// find a catalog's indexes (PG's `rd_indexlist`).
pub fn register_catalog_index(heap_relid: Oid, index: Relation, info: IndexInfo) {
    let _ = CATALOG_INDEXES.try_with(|cell| {
        cell.borrow_mut()
            .entry(heap_relid.0)
            .or_default()
            .push(RegisteredIndex { index, info });
    });
}

/// The open indexes (Relation + IndexInfo key columns) of a heap relation, as
/// (index, key-attnums). Empty if none registered (heap-only catalogs).
fn lookup_catalog_indexes(heap_relid: Oid) -> Vec<(Relation, Vec<i32>)> {
    CATALOG_INDEXES
        .try_with(|cell| {
            cell.borrow()
                .get(&heap_relid.0)
                .map(|v| {
                    v.iter()
                        .map(|ri| {
                            let keys =
                                ri.info.index_attr_numbers.iter().map(|&a| i32::from(a)).collect();
                            (ri.index, keys)
                        })
                        .collect()
                })
                .unwrap_or_default()
        })
        .unwrap_or_default()
}

// ---------------------------------------------------------------------------
// CatalogIndexState: the open-indexes handle (PG ResultRelInfo stand-in)
// ---------------------------------------------------------------------------

/// State used by `CatalogTupleInsertWithInfo` and friends. PG aliases the
/// executor's `ResultRelInfo`; the M2 port carries the heap relation + its open
/// indexes (Relation + key columns) directly. An owned box handle, not a pointer.
pub struct CatalogIndexState {
    heap_rel: Relation,
    indexes: Vec<(Relation, Vec<i32>)>,
}

// SAFETY: the raw `Relation` pointers are per-task relcache handles, task-confined
// for the state's lifetime; never raced. Same contract as the genam Send impl.
unsafe impl Send for CatalogIndexState {}

/// `CatalogOpenIndexes`: gather a catalog's open indexes for a batch of inserts
/// (PG builds a `ResultRelInfo` + `ExecOpenIndices`). Returns the heap relation
/// paired with its registered indexes.
#[must_use]
pub fn catalog_open_indexes(heap_rel: Relation) -> CatalogIndexState {
    // SAFETY: caller passes a live, open relation.
    let relid = unsafe { (*heap_rel).rd_id };
    CatalogIndexState { heap_rel, indexes: lookup_catalog_indexes(relid) }
}

/// `CatalogCloseIndexes`: release the open-indexes handle. The relcache owns the
/// index relations (they stay open for the task), so this just drops the box.
pub fn catalog_close_indexes(_indstate: CatalogIndexState) {}

/// `CatalogIndexInsert`: add `heap_tuple` to every index in `indstate`. For each
/// index, project its key columns out of the heap tuple (`FormIndexDatum`) and
/// `index_insert` with the heap TID.
async fn catalog_index_insert(
    shared: &Arc<SharedState>,
    indstate: &CatalogIndexState,
    heap_tuple: &HeapTupleData,
) {
    if indstate.indexes.is_empty() {
        return;
    }
    // Clone the heap descriptor (Arc, Send) so no `&RelationData` borrow crosses
    // the `.await`.
    // SAFETY: live open heap relation with a descriptor.
    let heap_td = unsafe { (*indstate.heap_rel).rd_att.clone() }
        .unwrap_or_else(|| unreachable!("catalog heap has a descriptor"));
    let tid = heap_tuple.t_self;

    for (index, keycols) in &indstate.indexes {
        // FormIndexDatum: extract the index key column values + null flags.
        let mut values = Vec::with_capacity(keycols.len());
        let mut isnull = Vec::with_capacity(keycols.len());
        for &attno in keycols {
            // SAFETY: attno is a valid heap attribute number; heap_tuple is live.
            let (v, n) = unsafe { heap_getattr(heap_tuple, attno, &heap_td) };
            values.push(v);
            isnull.push(n);
        }
        index_insert(shared, *index, &values, &isnull, &tid).await;
    }
}

// ---------------------------------------------------------------------------
// CatalogTupleInsert / WithInfo / Update
// ---------------------------------------------------------------------------

/// `simple_heap_insert` + `CatalogIndexInsert` is the core; the public entries
/// differ only in whether they open the indexes themselves.
async fn simple_catalog_insert(
    shared: &Arc<SharedState>,
    heap_rel: Relation,
    tup: &mut HeapTupleData,
    indstate: &CatalogIndexState,
) {
    // simple_heap_insert: a plain heap_insert with the current command id (PG
    // simple_heap_insert passes cid = GetCurrentCommandId(true), options = 0).
    let cid = GetCurrentCommandId(true);
    heap_insert(shared, SendPtr(heap_rel), SendPtr(std::ptr::from_mut(tup)), cid, 0).await;
    // tup.t_self now holds the stored TID; add to the indexes.
    catalog_index_insert(shared, indstate, tup).await;
}

/// `CatalogTupleInsert`: insert one tuple into a catalog heap + all its indexes,
/// opening/closing the index set itself. `tup.t_self` is updated to the stored
/// location.
pub async fn catalog_tuple_insert(
    shared: &Arc<SharedState>,
    heap_rel: Relation,
    tup: &mut HeapTupleData,
) {
    let indstate = catalog_open_indexes(heap_rel);
    simple_catalog_insert(shared, heap_rel, tup, &indstate).await;
    catalog_close_indexes(indstate);
}

/// `CatalogTupleInsertWithInfo`: like [`catalog_tuple_insert`] but reuses a
/// caller-opened `CatalogIndexState` (amortized over a batch).
pub async fn catalog_tuple_insert_with_info(
    shared: &Arc<SharedState>,
    heap_rel: Relation,
    tup: &mut HeapTupleData,
    indstate: &CatalogIndexState,
) {
    simple_catalog_insert(shared, heap_rel, tup, indstate).await;
}

/// `CatalogTupleUpdate`: STAGED. The M2 path only inserts catalog rows (CREATE);
/// in-place catalog updates (e.g. relhasindex via index_update_stats, type shell
/// fill-in) land with the update milestone. Until then a faithful update needs
/// `simple_heap_update` (the heap update AM, a grow guard).
pub async fn catalog_tuple_update(
    shared: &Arc<SharedState>,
    heap_rel: Relation,
    otid: &ItemPointerData,
    tup: &mut HeapTupleData,
) {
    let _ = (shared, heap_rel, otid, tup);
    unimplemented!("CatalogTupleUpdate: needs simple_heap_update (heap update AM, M6)")
}

