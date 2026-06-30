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


use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::Arc;

use crate::access::htup::{HeapTuple, HeapTupleData};
use crate::backend::access::common::heaptuple::heap_getattr;
use crate::backend::access::heap::heapam::heap_insert;
use crate::backend::access::index::indexam::index_insert;
use crate::backend::access::transam::xact::GetCurrentCommandId;
use crate::nodes::execnodes::IndexInfo;
use crate::postgres_ext::Oid;
use crate::shared_state::SharedState;
use crate::storage::itemptr::ItemPointerData;
use crate::utils::rel::RelationData;

/// Cap on bytes allocated for multi-inserts with system catalogs (PG
/// `MAX_CATALOG_MULTI_INSERT_BYTES`). Re-exported by the header.
pub const MAX_CATALOG_MULTI_INSERT_BYTES: usize = 65535;

// ---------------------------------------------------------------------------
// Per-task catalog-index registry (the rd_indexlist stand-in)
// ---------------------------------------------------------------------------

/// One registered catalog index: the open index `Arc<RelationData>` (the registry owns this
/// `Arc`) and the `IndexInfo` describing its key columns. The registry maps a heap
/// relid to its indexes. Auto-`Send`: `Arc<RelationData>` is `Send` (`RelationData:
/// Sync`) and `IndexInfo`'s fields are all `Send`.
struct RegisteredIndex {
    index: Arc<RelationData>,
    info: IndexInfo,
}

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
pub fn register_catalog_index(heap_relid: Oid, index: Arc<RelationData>, info: IndexInfo) {
    let _ = CATALOG_INDEXES.try_with(|cell| {
        cell.borrow_mut()
            .entry(heap_relid.0)
            .or_default()
            .push(RegisteredIndex { index, info });
    });
}

/// Remove the index `indexoid` from the per-task registry (`index_drop`'s effect on
/// the rd_indexlist stand-in). Searches every heap entry; returns the heap relid the
/// index belonged to, or `None` if it was not registered.
pub fn unregister_catalog_index(indexoid: Oid) -> Option<Oid> {
    CATALOG_INDEXES
        .try_with(|cell| {
            let mut map = cell.borrow_mut();
            for (&heap, indexes) in map.iter_mut() {
                let pos = indexes.iter().position(|ri| ri.index.rd_id == indexoid);
                if let Some(pos) = pos {
                    indexes.remove(pos);
                    return Some(Oid(heap));
                }
            }
            None
        })
        .ok()
        .flatten()
}

/// The open indexes of a heap relation as owned `Arc`s + their key-attnums. The
/// `Arc`s are clones of the registry's (a refcount bump pinning each index relation);
/// this owned `Vec` lives in the caller's frame and is what [`CatalogIndexState`]
/// borrows from. Empty if none registered (heap-only catalogs).
fn lookup_catalog_indexes(heap_relid: Oid) -> Vec<(Arc<RelationData>, Vec<i32>)> {
    CATALOG_INDEXES
        .try_with(|cell| {
            cell.borrow()
                .get(&heap_relid.0)
                .map(|v| {
                    v.iter()
                        .map(|ri| {
                            let keys =
                                ri.info.index_attr_numbers.iter().map(|&a| i32::from(a)).collect();
                            (Arc::clone(&ri.index), keys)
                        })
                        .collect()
                })
                .unwrap_or_default()
        })
        .unwrap_or_default()
}

/// One index of a heap relation, as the planner / executor need it: the open index
/// relation (an `Arc` clone, a refcount bump pinning it) and its key heap-attnums.
pub struct RegisteredIndexInfo {
    pub index: Arc<RelationData>,
    pub key_attnums: Vec<i16>,
    pub unique: bool,
}

/// PG `RelationGetIndexList` (the registry-backed M6 form): the indexes of a heap
/// relation `heap_relid`. Used by the planner's `get_relation_info` (to build the
/// `IndexOptInfo` list) and the wire executor (to open the index relations). Returns
/// owned `Arc` clones + each index's key columns; empty if none registered. The
/// pg_index-scan-backed `rd_indexlist` grows when pg_index is an on-disk catalog.
#[must_use]
pub fn relation_get_index_list(heap_relid: Oid) -> Vec<RegisteredIndexInfo> {
    CATALOG_INDEXES
        .try_with(|cell| {
            cell.borrow()
                .get(&heap_relid.0)
                .map(|v| {
                    v.iter()
                        .map(|ri| RegisteredIndexInfo {
                            index: Arc::clone(&ri.index),
                            key_attnums: ri.info.index_attr_numbers.clone(),
                            unique: ri.info.unique,
                        })
                        .collect()
                })
                .unwrap_or_default()
        })
        .unwrap_or_default()
}

/// The open index relation with OID `indexoid`, if it is registered (by
/// `index_create`). An `Arc` clone (a refcount bump). Used by the wire executor to
/// open the index relations a planned index/bitmap scan references -- the registry
/// holds the fully-initialized index relation (rd_index + opclass support), which a
/// relcache rebuild from the staged pg_index cannot reconstruct on M6.
#[must_use]
pub fn find_registered_index(indexoid: Oid) -> Option<Arc<RelationData>> {
    CATALOG_INDEXES
        .try_with(|cell| {
            cell.borrow().values().flatten().find_map(|ri| {
                (ri.index.rd_id == indexoid).then(|| Arc::clone(&ri.index))
            })
        })
        .ok()
        .flatten()
}

// ---------------------------------------------------------------------------
// CatalogIndexState: the open-indexes handle (PG ResultRelInfo stand-in)
// ---------------------------------------------------------------------------

/// State used by `CatalogTupleInsertWithInfo` and friends. PG aliases the
/// executor's `ResultRelInfo`; the M2 port carries the heap relation + its open
/// indexes (relation + key columns) directly, all BORROWED: the heap `Arc` is owned
/// by the caller and the index `Arc`s by a `[(Arc<RelationData>, Vec<i32>)]` owner the caller
/// holds (from [`lookup_catalog_indexes`]). Auto-`Send`: it holds only borrows of
/// `Sync` data.
pub struct CatalogIndexState<'rel> {
    heap_rel: &'rel RelationData,
    indexes: Vec<(&'rel RelationData, &'rel [i32])>,
}

/// `CatalogOpenIndexes`: gather a catalog's open indexes for a batch of inserts (PG
/// builds a `ResultRelInfo` + `ExecOpenIndices`). Borrows the heap relation + the
/// caller-owned index list (its `Arc`s + key columns), pairing the heap with each
/// index relation.
#[must_use]
pub fn catalog_open_indexes<'rel>(
    heap_rel: &'rel RelationData,
    index_owner: &'rel [(Arc<RelationData>, Vec<i32>)],
) -> CatalogIndexState<'rel> {
    let indexes = index_owner
        .iter()
        .map(|(idx, keys)| (&**idx, keys.as_slice()))
        .collect();
    CatalogIndexState { heap_rel, indexes }
}

/// `CatalogCloseIndexes`: release the open-indexes handle. The caller's frame owns
/// the index relations (they stay open for the task), so this just drops the borrows.
pub fn catalog_close_indexes(_indstate: CatalogIndexState<'_>) {}

/// `CatalogIndexInsert`: add `heap_tuple` to every index in `indstate`. For each
/// index, project its key columns out of the heap tuple (`FormIndexDatum`) and
/// `index_insert` with the heap TID.
async fn catalog_index_insert(
    shared: &Arc<SharedState>,
    indstate: &CatalogIndexState<'_>,
    heap_tuple: &HeapTupleData,
) {
    if indstate.indexes.is_empty() {
        return;
    }
    // Clone the heap descriptor (Arc, Send) so no `&RelationData` borrow crosses
    // the `.await`.
    let heap_td = indstate.heap_rel.rd_att.clone()
        .unwrap_or_else(|| unreachable!("catalog heap has a descriptor"));
    let tid = heap_tuple.t_self;

    for (index, keycols) in &indstate.indexes {
        // FormIndexDatum: extract the index key column values + null flags.
        let mut values = Vec::with_capacity(keycols.len());
        let mut isnull = Vec::with_capacity(keycols.len());
        for &attno in *keycols {
            // SAFETY: attno is a valid heap attribute number; heap_tuple is live.
            let (v, n) = unsafe { heap_getattr(heap_tuple, attno, &heap_td) };
            values.push(v);
            isnull.push(n);
        }
        index_insert(shared, index, &values, &isnull, &tid).await;
    }
}

// ---------------------------------------------------------------------------
// CatalogTupleInsert / WithInfo / Update
// ---------------------------------------------------------------------------

/// `simple_heap_insert` + `CatalogIndexInsert` is the core; the public entries
/// differ only in whether they open the indexes themselves.
async fn simple_catalog_insert(
    shared: &Arc<SharedState>,
    heap_rel: &RelationData,
    tup: &mut HeapTupleData,
    indstate: &CatalogIndexState<'_>,
) {
    // simple_heap_insert: a plain heap_insert with the current command id (PG
    // simple_heap_insert passes cid = GetCurrentCommandId(true), options = 0).
    let cid = GetCurrentCommandId(true);
    heap_insert(shared, heap_rel, tup, cid, 0).await;
    // tup.t_self now holds the stored TID; add to the indexes.
    catalog_index_insert(shared, indstate, tup).await;
}

/// `CatalogTupleInsert`: insert one tuple into a catalog heap + all its indexes,
/// opening/closing the index set itself. `tup.t_self` is updated to the stored
/// location. The heap `Arc` is owned by the caller; the index `Arc`s are owned by a
/// frame-local `Vec` here (clones of the registry's), which the borrowing
/// `CatalogIndexState` reads from.
pub async fn catalog_tuple_insert(
    shared: &Arc<SharedState>,
    heap_rel: &RelationData,
    tup: &mut HeapTupleData,
) {
    let index_owner = lookup_catalog_indexes(heap_rel.rd_id);
    let indstate = catalog_open_indexes(heap_rel, &index_owner);
    simple_catalog_insert(shared, heap_rel, tup, &indstate).await;
    catalog_close_indexes(indstate);
}

/// `CatalogTupleInsertWithInfo`: like [`catalog_tuple_insert`] but reuses a
/// caller-opened `CatalogIndexState` (amortized over a batch).
pub async fn catalog_tuple_insert_with_info(
    shared: &Arc<SharedState>,
    heap_rel: &RelationData,
    tup: &mut HeapTupleData,
    indstate: &CatalogIndexState<'_>,
) {
    simple_catalog_insert(shared, heap_rel, tup, indstate).await;
}

/// `CatalogTupleUpdate`: update a catalog row in place (`simple_heap_update`) then
/// re-add it to the catalog's indexes. Mirrors the insert path: open the indexes,
/// `heap_update`, re-index the new tuple. PG only re-indexes when the update touched
/// an indexed column; the M10 path always re-indexes (correct, just not amortized).
pub async fn catalog_tuple_update(
    shared: &Arc<SharedState>,
    heap_rel: &RelationData,
    otid: &ItemPointerData,
    tup: &mut HeapTupleData,
) {
    let cid = GetCurrentCommandId(true);
    // simple_heap_update: heap_update with wait=true; the M10 catalog path is
    // single-backend, so the update never collides.
    let (_result, _lockmode, _idx) = crate::backend::access::heap::heapam::heap_update(
        shared, heap_rel, otid, tup, cid, None, true,
    )
    .await;
    let index_owner = lookup_catalog_indexes(heap_rel.rd_id);
    let indstate = catalog_open_indexes(heap_rel, &index_owner);
    catalog_index_insert(shared, &indstate, tup).await;
    catalog_close_indexes(indstate);
}

/// `CatalogTupleDelete`: delete a catalog row (`simple_heap_delete`). The catalog
/// indexes are not pruned eagerly -- index entries pointing at a dead heap TID are
/// skipped by the MVCC visibility check on the next scan (PG relies on VACUUM /
/// index bloat for the physical reclaim), and the M10 read path is heap-scan-based.
pub async fn catalog_tuple_delete(
    shared: &Arc<SharedState>,
    heap_rel: &RelationData,
    tid: &ItemPointerData,
) {
    let cid = GetCurrentCommandId(true);
    // simple_heap_delete: heap_delete with wait=true; single-backend on M10.
    let (_result, _fdata) =
        crate::backend::access::heap::heapam::heap_delete(shared, heap_rel, tid, cid, None, true, false)
            .await;
}
