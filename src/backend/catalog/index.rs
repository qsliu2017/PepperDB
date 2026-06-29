//! Index creation + build. Translated from the M2-reachable parts of
//! `src/backend/catalog/index.c`.
//!
//! `index_create` makes a btree index relation (its tuple descriptor, storage,
//! and pg_class row) and registers it as an index of its heap so catalog inserts
//! keep it current; `index_build` populates it by calling the btree AM build
//! (`btbuild`). Used for BOTH the catalog unique indexes (built by the initdb
//! pass) and user indexes (M6), so the index path lands once here.
//!
//! Async coloring (rules.md s5): storage create, the pg_class insert, and the
//! btree build all reach the buffer pool, so the path is `async`.
//!
//! M2 scope: a plain btree index on simple heap columns -- no constraints, no
//! concurrency, no partitioning, no expressions/predicates. The pg_index row +
//! the index's pg_attribute rows are staged (pg_index is not a nailed catalog in
//! M2; see the note in `index_create`); the index is functional for inserts +
//! scans via the catalog-index registry + the built btree.

#![allow(
    clippy::too_many_arguments,
    reason = "index_create mirrors the C signature 1:1 (port-inherent)"
)]

use std::sync::Arc;

use crate::access::tupdesc::{TupleDesc, TupleDescData};
use crate::backend::access::nbtree::nbtree::btbuild;
use crate::backend::catalog::catalog::get_new_rel_file_number;
use crate::backend::catalog::heap::heap_create;
use crate::backend::catalog::indexing::{catalog_tuple_insert, register_catalog_index};
use crate::backend::utils::cache::relcache::{
    index_init_opclass_support, relation_close, relation_id_get_relation,
    relation_init_index_access_info,
};
use crate::catalog::pg_class::{RelationRelationId, RELKIND_INDEX};
use crate::nodes::execnodes::IndexInfo;
use crate::postgres_ext::{InvalidOid, Oid};
use crate::shared_state::SharedState;
use crate::utils::rel::RelationData;

const BTREE_AM_OID: Oid = Oid(403);

/// Build an [`IndexInfo`] for a simple-column btree index over `key_attnums`
/// (1-based heap attnums), with `unique`. M2: no expressions/predicates/exclusion.
#[must_use]
pub fn make_index_info(key_attnums: &[i16], unique: bool) -> IndexInfo {
    let n = key_attnums.len() as i32;
    IndexInfo {
        num_index_attrs: n,
        num_index_key_attrs: n,
        index_attr_numbers: key_attnums.to_vec(),
        expressions: Vec::new(),
        expressions_state: Vec::new(),
        predicate: Vec::new(),
        predicate_state: None,
        exclusion_ops: Vec::new(),
        exclusion_procs: Vec::new(),
        exclusion_strats: Vec::new(),
        unique_ops: Vec::new(),
        unique_procs: Vec::new(),
        unique_strats: Vec::new(),
        unique,
        nulls_not_distinct: false,
        ready_for_inserts: true,
        checked_unchanged: false,
        index_unchanged: false,
        concurrent: false,
        broken_hot_chain: false,
        summarizing: false,
        without_overlaps: false,
        parallel_workers: 0,
        am: BTREE_AM_OID,
        am_cache: crate::nodes::execnodes::OpaqueState::default(),
        context: crate::nodes::execnodes::MemoryContext,
    }
}

/// `ConstructTupleDescriptor` (M2 simple-column form): build the index's tuple
/// descriptor by copying each key column's fixed part from the heap descriptor.
/// Index expressions + opclass keytype overrides are later scope.
fn construct_tuple_descriptor(
    heap_relation: &RelationData,
    index_info: &IndexInfo,
    index_col_names: &[String],
) -> TupleDesc {
    let heap_td = heap_relation.rd_att.clone()
        .unwrap_or_else(|| unreachable!("heap relation has a descriptor"));

    let numatts = index_info.num_index_attrs;
    let mut desc = TupleDescData::create_template(numatts);
    desc.tdtypmod = -1;

    for i in 0..(numatts as usize) {
        let atnum = index_info.index_attr_numbers[i];
        debug_assert!(atnum > 0, "M2 index columns are simple heap columns");
        let from = heap_td.attr((atnum - 1) as usize);
        let to = &mut desc.attrs[i];
        // Copy the fixed type properties from the heap column.
        to.atttypid = from.atttypid;
        to.attlen = from.attlen;
        to.attndims = from.attndims;
        to.atttypmod = from.atttypmod;
        to.attbyval = from.attbyval;
        to.attalign = from.attalign;
        to.attstorage = from.attstorage;
        to.attcompression = from.attcompression;
        to.attnum = (i + 1) as i16;
        to.attislocal = true;
        to.attcollation = from.attcollation;
        // Name from the supplied list (fallback to the heap column name).
        let nm = index_col_names.get(i).map_or_else(
            || {
                let mut s = String::new();
                for &b in &from.attname.data {
                    if b == 0 {
                        break;
                    }
                    s.push(b as char);
                }
                s
            },
            std::clone::Clone::clone,
        );
        to.attname = crate::backend::catalog::heap::name_data(&nm);
        desc.populate_compact_attribute(i);
    }
    Arc::new(desc)
}

/// `index_create` (M2 form): create a btree index relation over `heap_relation`'s
/// `index_info` key columns, then (unless `skip_build`) build it. Returns the new
/// index relation's OID.
///
/// Writes the index's pg_class row (nailed catalog, writable) and registers the
/// index with the heap so `CatalogTupleInsert` maintains it. STAGED (rules.md s4):
/// the pg_index row + the index's pg_attribute rows -- pg_index is not a nailed M2
/// catalog (its heap is created by the broader initdb that builds every non-nailed
/// catalog heap + cooks the int2vector/oidvector columns). The returned index is
/// fully usable for inserts/scans via the registry + the built btree.
pub async fn index_create(
    shared: &Arc<SharedState>,
    heap_relation: &RelationData,
    index_relation_name: &str,
    index_relation_id: Oid,
    rel_file_number: Oid,
    index_info: &IndexInfo,
    index_col_names: &[String],
    access_method_id: Oid,
    table_space_id: Oid,
    collation_ids: &[Oid],
    opclass_ids: &[Oid],
    coloptions: &[i16],
    skip_build: bool,
) -> Oid {
    let (heap_relid, namespace_id, relpersistence, shared_relation) = {
        let h: &RelationData = heap_relation;
        let form = h.form();
        (h.rd_id, form.relnamespace, form.relpersistence, form.relisshared)
    };

    assert!(index_info.num_index_attrs >= 1, "index must have at least one column");

    let pg_class = relation_id_get_relation(RelationRelationId)
        .unwrap_or_else(|| unreachable!("pg_class is nailed/open"));

    // Build the index tuple descriptor.
    let index_tup_desc = construct_tuple_descriptor(heap_relation, index_info, index_col_names);

    // Assign the index OID / relfilenumber.
    let index_relation_id = if index_relation_id.0 != 0 {
        index_relation_id
    } else {
        get_new_rel_file_number(shared, table_space_id, Some(Arc::clone(&pg_class)), relpersistence).await
    };
    let rel_file_number = if rel_file_number.0 != 0 { rel_file_number } else { index_relation_id };

    // Storage-level create of the index relation.
    let index_relation = heap_create(
        shared,
        index_relation_name,
        namespace_id,
        table_space_id,
        index_relation_id,
        rel_file_number,
        access_method_id,
        index_tup_desc,
        RELKIND_INDEX,
        relpersistence,
        shared_relation,
        true,
    )
    .await;
    let mut index_relation = index_relation;

    // Attach a fabricated pg_index Form (rd_index) so the btree AM can read the
    // index's key-attribute counts; then wire the access-method arrays from the
    // opclasses so the comparator resolves. The index relation is unshared here
    // (just built; refcount 1), so mutate it in place via `Arc::get_mut`.
    {
        let idx = Arc::get_mut(&mut index_relation)
            .unwrap_or_else(|| unreachable!("freshly built index relation is unshared"));
        attach_rd_index(idx, index_info);
        relation_init_index_access_info(idx);
        alloc_and_fill_index_support(idx, index_info, opclass_ids, collation_ids, coloptions);

        // Set the index relation's owner/am to match the heap, then write its
        // pg_class row (pg_class is nailed and writable).
        let owner = heap_relation.form().relowner;
        let idx_form = idx.rd_rel.as_deref_mut()
            .unwrap_or_else(|| unreachable!("freshly built index relation has a pg_class form"));
        idx_form.relowner = owner;
        idx_form.relam = access_method_id;
    }
    crate::backend::catalog::heap::insert_pg_class_tuple_pub(
        shared,
        &pg_class,
        &index_relation,
        index_relation_id,
        None,
        None,
    )
    .await;

    // STAGED (rules.md s4): UpdateIndexRelation (the pg_index row with the
    // int2vector indkey / oidvector indclass) + AppendAttributeTuples for the index
    // columns + dependency recording. pg_index is not nailed in M2.

    // Register the index with its heap so CatalogTupleInsert maintains it.
    register_catalog_index(heap_relid, Arc::clone(&index_relation), clone_index_info(index_info));

    relation_close(pg_class);

    // Build (populate) the index unless asked to skip. The heap + index `Arc`s are
    // owned here (this frame); the build borrows them.
    if !skip_build {
        index_build(shared, heap_relation, &index_relation, index_info).await;
    }

    index_relation_id
}

/// `index_build` (M2 form): populate the index by calling the btree AM build
/// (`btbuild`), which scans the heap, sorts the key columns, and packs the btree.
/// STAGED: `index_update_stats` (pg_class relpages/reltuples/relhasindex) needs the
/// in-place catalog update (CatalogTupleUpdate, M6); the build result counts are
/// otherwise unused on the M2 path.
pub async fn index_build(
    shared: &Arc<SharedState>,
    heap_relation: &RelationData,
    index_relation: &RelationData,
    index_info: &IndexInfo,
) {
    let _stats = btbuild(shared, heap_relation, index_relation, index_info).await;
    // STAGED (rules.md s4): index_update_stats(heap, true, stats.heap_tuples);
    // index_update_stats(index, false, stats.index_tuples); needs CatalogTupleUpdate.
}

/// Fabricate the index's pg_index Form (`rd_index`) from `index_info` so the btree
/// AM can read its key-attribute counts. M2 fills only the fields the build/scan
/// read (indnatts/indnkeyatts/indisunique/indkey-equivalent); the on-disk pg_index
/// row is staged.
fn attach_rd_index(index_relation: &mut RelationData, index_info: &IndexInfo) {
    use crate::catalog::pg_index::FormData_pg_index;
    // SAFETY: FormData_pg_index is repr(C) POD; zero then patch the fixed fields.
    let mut idx: Box<FormData_pg_index> = Box::new(unsafe { core::mem::zeroed() });
    idx.indnatts = index_info.num_index_attrs as i16;
    idx.indnkeyatts = index_info.num_index_key_attrs as i16;
    idx.indisunique = index_info.unique;
    idx.indisready = true;
    idx.indisvalid = true;
    idx.indislive = true;
    idx.indimmediate = true;
    index_relation.rd_index = Some(idx);
}

/// Fill the index relation's opclass support (the arrays were allocated by
/// `relation_init_index_access_info` from `rd_index`).
fn alloc_and_fill_index_support(
    index_relation: &mut RelationData,
    _index_info: &IndexInfo,
    opclass_ids: &[Oid],
    collation_ids: &[Oid],
    coloptions: &[i16],
) {
    index_init_opclass_support(index_relation, opclass_ids, collation_ids, coloptions);
}

/// Clone an IndexInfo's M2-relevant fields (it is not `Clone`; only the key columns
/// + flags the registry uses are needed).
fn clone_index_info(ii: &IndexInfo) -> IndexInfo {
    make_index_info(&ii.index_attr_numbers, ii.unique)
}
