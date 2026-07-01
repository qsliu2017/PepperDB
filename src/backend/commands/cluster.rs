//! CLUSTER and the VACUUM FULL rewrite. Translated from the M13-reachable core of
//! `src/backend/commands/cluster.c`.
//!
//! CLUSTER and VACUUM FULL both rebuild a table by copying its live tuples into a
//! fresh heap (a new relfilenode) and swapping the physical files, which reclaims
//! all the dead space a plain (in-place) VACUUM only marks reusable. CLUSTER copies
//! the tuples in a chosen index's order (physically ordering the heap by the index);
//! VACUUM FULL copies in physical order. The reclaim comes from dropping every tuple
//! that is dead relative to the VACUUM cutoff `OldestXmin`.
//!
//! M13 scope (step 47):
//! - `cluster` -- the `ClusterStmt` driver (resolve the target + cluster index).
//! - `cluster_rel` -- rebuild one relation (index order, or physical order when no
//!   index -- the VACUUM FULL path).
//! - `rebuild_relation` -- create the transient heap (`make_new_heap`), copy the
//!   live data into it (`copy_table_data`), swap the physical files + rebuild the
//!   indexes + drop the transient heap (`finish_heap_swap`).
//! - `make_new_heap` -- a fresh heap with the old heap's tupdesc + a new relfilenode.
//! - `copy_table_data` -- scan the old heap (index or physical order), keep the live
//!   tuples (`HeapTupleSatisfiesVacuum != Dead`), rewrite them into the new heap.
//! - `swap_relation_files` -- exchange the relfilenode (+ relpages/reltuples) of the
//!   two pg_class rows so the new storage becomes the table's.
//! - `finish_heap_swap` -- swap, rebuild the indexes over the new storage, drop the
//!   transient relation (unlinks the old storage), refresh the relcache.
//! - `mark_index_clustered` -- set pg_index.indisclustered on the cluster index.
//!
//! Locks (task): CLUSTER / VACUUM FULL take AccessExclusiveLock; the single-writer
//! chassis makes the heavyweight lock a no-op (as elsewhere in the command layer),
//! so the relations are opened through the relcache directly.
//!
//! Staged (rules.md s4): CLUSTER with no relation (re-cluster every marked table),
//! TOAST-table rewrite + `swap_toast_by_content`, the mapped-relation (nailed
//! catalog) swap path, MultiXact cutoffs, the tablespace / access-method change
//! forms (ALTER TABLE rewrite), and logical-decoding rewrite mappings -- none are on
//! the M13 CLUSTER / VACUUM FULL path (surfaced as clean `not_yet_reachable` where a
//! statement can still reach them).
//!
//! Async coloring (rules.md s5): the whole rewrite reaches the buffer pool + WAL, so
//! every entry point is `async`; no content lock is held across an `.await`.

#![allow(
    clippy::cast_ptr_alignment,
    reason = "faithful GETSTRUCT reinterpretation of a heap tuple to a Form_* struct (MAXALIGN'd body covers the Form alignment)"
)]

use std::sync::Arc;

use crate::access::sdir::ScanDirection;
use crate::access::htup_details::SizeofHeapTupleHeader;
use crate::backend::access::common::heaptuple::heap_freetuple;
use crate::backend::access::heap::heapam_visibility::HeapTupleSatisfiesVacuum;
use crate::backend::access::heap::rewriteheap::{
    begin_heap_rewrite, end_heap_rewrite, rewrite_heap_tuple,
};
use crate::backend::access::transam::xact::CommandCounterIncrement;
use crate::backend::catalog::heap::heap_create_with_catalog;
use crate::backend::catalog::indexing::{catalog_tuple_update, relation_get_index_list};
use crate::backend::utils::cache::relcache::{
    relation_build_desc, relation_close, relation_forget_relation, relation_id_get_relation,
};
use crate::access::heapam::HTSV_Result;
use crate::catalog::pg_class::{self as pc, RELKIND_RELATION};
use crate::nodes::parsenodes::ClusterStmt;
use crate::postgres_ext::{InvalidOid, Oid};
use crate::shared_state::SharedState;
use crate::storage::block::BlockNumber;
use crate::storage::itemptr::ItemPointerData;
use crate::utils::elog::{ERROR, WARNING};
use crate::utils::rel::RelationData;

/// PG `cluster`: the `ClusterStmt` entry from ProcessUtility. Resolve the target
/// relation + the cluster index, then `cluster_rel`. `CLUSTER` with no relation
/// (re-cluster every marked table) is staged.
pub async fn cluster(shared: &Arc<SharedState>, stmt: &ClusterStmt) {
    let Some(rangevar) = stmt.relation.as_deref() else {
        crate::elog!(ERROR, "CLUSTER without a table (re-cluster all marked tables) is not yet supported -- step 47");
        return;
    };
    let Some(relname) = rangevar.relname.as_deref() else {
        unreachable!("CLUSTER RangeVar carries a relation name");
    };

    let relid = crate::backend::catalog::namespace::range_var_get_relid(
        shared,
        rangevar.schemaname.as_deref(),
        relname,
    )
    .await;
    let Some(relid) = relid.filter(|oid| oid.is_valid()) else {
        crate::ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(crate::utils::errcodes::ERRCODE_UNDEFINED_TABLE)
                .errmsg(format!("relation \"{relname}\" does not exist"));
        });
        unreachable!("ereport(ERROR) diverges");
    };

    // Resolve the named cluster index to its OID (an index registered on this heap).
    let index_oid = stmt.indexname.as_deref().map(|idxname| resolve_cluster_index(relid, idxname));

    cluster_rel(shared, relid, index_oid).await;
}

/// The OID of the index named `idxname` on heap `relid`, from the index registry.
/// A name that is not an index of the relation is an ERROR (PG `get_relname_relid`
/// + the RELKIND / owning-table checks).
fn resolve_cluster_index(relid: Oid, idxname: &str) -> Oid {
    for ri in relation_get_index_list(relid) {
        let form = ri.index.form();
        if relname_eq(&form.relname, idxname) {
            return ri.index.rd_id;
        }
    }
    crate::ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
        e.errcode(crate::utils::errcodes::ERRCODE_UNDEFINED_OBJECT)
            .errmsg(format!("index \"{idxname}\" for table does not exist"));
    });
    unreachable!("ereport(ERROR) diverges");
}

/// Compare a pg_class `relname` (`NameData`, NUL-padded) to a Rust string.
fn relname_eq(name: &crate::c::NameData, s: &str) -> bool {
    let bytes = crate::c::NameStr(name);
    let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
    bytes[..end] == *s.as_bytes()
}

/// PG `cluster_rel`: rebuild one relation. `index_oid` names the cluster index
/// (`Some` = physically order the heap by it), or `None` for a physical-order
/// rewrite (the VACUUM FULL path). Only plain heaps are handled; matviews / TOAST /
/// mapped catalogs are staged. Takes AccessExclusiveLock in PG (a no-op here).
pub async fn cluster_rel(shared: &Arc<SharedState>, relid: Oid, index_oid: Option<Oid>) {
    // Open the target heap (build it into the relcache first, then pin it).
    relation_build_desc(shared, relid).await;
    let Some(old_heap) = relation_id_get_relation(relid) else {
        crate::elog!(WARNING, "skipping cluster/vacuum full: relation vanished");
        return;
    };

    let relkind = old_heap.form().relkind;
    if relkind != RELKIND_RELATION {
        // matviews / toast / partitioned are staged; nothing to do for others.
        relation_close(old_heap);
        return;
    }

    rebuild_relation(shared, &old_heap, index_oid).await;

    relation_close(old_heap);
}

/// PG `rebuild_relation`: the rewrite driver. Mark the cluster index (if any),
/// create the transient heap, copy the live data into it, then swap the files +
/// rebuild the indexes + drop the transient heap.
async fn rebuild_relation(shared: &Arc<SharedState>, old_heap: &RelationData, index_oid: Option<Oid>) {
    let table_oid = old_heap.rd_id;

    // Mark the correct index as clustered (CLUSTER only).
    if let Some(idx) = index_oid {
        mark_index_clustered(table_oid, idx);
    }

    // Create the transient heap that receives the reordered / compacted data. It has
    // the old heap's tupdesc but a fresh relfilenode.
    let new_heap_oid = make_new_heap(shared, old_heap).await;
    // Make the transient relation's catalog rows visible to relation_build_desc.
    CommandCounterIncrement();

    relation_build_desc(shared, new_heap_oid).await;
    let new_heap = relation_id_get_relation(new_heap_oid)
        .unwrap_or_else(|| unreachable!("transient heap {new_heap_oid:?} just built"));

    // Copy the heap data into the new table in the desired order.
    let num_tuples = copy_table_data(shared, &new_heap, old_heap, index_oid).await;

    // The compacted heap's physical size (-> pg_class.relpages after the swap).
    let new_pages = heap_nblocks(shared, &new_heap).await;

    relation_close(new_heap);

    // Swap the physical files, rebuild the target's indexes, throw away the
    // transient table.
    finish_heap_swap(shared, table_oid, new_heap_oid, num_tuples, new_pages).await;
}

/// PG `make_new_heap`: create the transient table that receives the rewritten data.
/// It duplicates the old heap's tupdesc; the caller loads it then swaps. Returns the
/// new heap's OID (a fresh relfilenode).
async fn make_new_heap(shared: &Arc<SharedState>, old_heap: &RelationData) -> Oid {
    let form = old_heap.form();
    let namespace_id = form.relnamespace;
    let owner = form.relowner;
    let access_method = form.relam;
    let reltablespace = form.reltablespace;
    let relpersistence = form.relpersistence;
    let tupdesc = old_heap
        .rd_att
        .clone()
        .unwrap_or_else(|| unreachable!("old heap has a tuple descriptor"));

    // A temporary name in the same namespace (PG: "pg_temp_<oldoid>").
    let new_name = format!("pg_temp_{}", old_heap.rd_id.get());

    heap_create_with_catalog(
        shared,
        &new_name,
        namespace_id,
        reltablespace,
        InvalidOid, // fresh relation OID (== fresh relfilenode)
        InvalidOid, // fresh rowtype OID
        owner,
        access_method,
        tupdesc,
        RELKIND_RELATION,
        relpersistence,
        false, // not shared
    )
    .await
}

/// PG `copy_table_data`: scan the old heap and rewrite its live tuples into the new
/// heap. `index_oid` chooses the order: `Some` scans through the index (physical
/// order becomes index order); `None` scans the heap physically. A tuple is kept iff
/// it is not dead relative to the VACUUM cutoff (`HeapTupleSatisfiesVacuum != Dead`),
/// which is where VACUUM FULL / CLUSTER reclaim the dead space. Returns the live
/// tuple count.
async fn copy_table_data(
    shared: &Arc<SharedState>,
    new_heap: &RelationData,
    old_heap: &RelationData,
    index_oid: Option<Oid>,
) -> u64 {
    // The freeze/deadness cutoff (PG's OldestXmin). A tuple that no snapshot can see
    // is dropped; the survivors are all older than this, so the rewrite freezes them.
    let oldest_xmin = shared.proc_array().get_oldest_non_removable_transaction_id(
        shared.variable_cache(),
        Some(old_heap),
    );

    let old_desc = old_heap
        .rd_att
        .clone()
        .unwrap_or_else(|| unreachable!("old heap has a tuple descriptor"));

    let mut state = begin_heap_rewrite(new_heap, oldest_xmin);

    match index_oid {
        Some(idx_oid) => {
            // CLUSTER: rewrite the live tuples in the index's key order. PG offers two
            // engines here -- an index scan and a tuplesort; the M13 form uses the
            // sort (collect the live tuples with their index-key values, sort by the
            // index's comparator, rewrite in order). The sort avoids depending on a
            // keyless full index scan and matches PG's `use_sort` CLUSTER path.
            let Some(ri) = relation_get_index_list(old_heap.rd_id)
                .into_iter()
                .find(|ri| ri.index.rd_id == idx_oid)
            else {
                unreachable!("cluster index {idx_oid:?} registered on heap");
            };
            copy_sorted_by_index(
                shared, &mut state, old_heap, &old_desc, &ri.index, &ri.key_attnums, oldest_xmin,
            )
            .await;
        }
        None => {
            // VACUUM FULL: scan the old heap in physical order.
            copy_physical(shared, &mut state, old_heap, &old_desc, oldest_xmin).await;
        }
    }

    end_heap_rewrite(state)
}

/// Physical-order copy: rewrite every live tuple in the old heap's physical order.
async fn copy_physical(
    shared: &Arc<SharedState>,
    state: &mut crate::backend::access::heap::rewriteheap::RewriteState<'_>,
    old_heap: &RelationData,
    old_desc: &crate::access::tupdesc::TupleDesc,
    oldest_xmin: crate::c::TransactionId,
) {
    let live = collect_live_tuples(shared, old_heap, oldest_xmin).await;
    for tuple in &live {
        rewrite_heap_tuple(shared, state, tuple, old_desc).await;
    }
}

/// Index-order copy (CLUSTER): collect the live tuples, sort them by the cluster
/// index's key columns (the index's btree comparator, honoring NULLS / DESC options),
/// then rewrite in that order so the new heap is physically ordered by the index.
async fn copy_sorted_by_index(
    shared: &Arc<SharedState>,
    state: &mut crate::backend::access::heap::rewriteheap::RewriteState<'_>,
    old_heap: &RelationData,
    old_desc: &crate::access::tupdesc::TupleDesc,
    index: &RelationData,
    key_attnums: &[i16],
    oldest_xmin: crate::c::TransactionId,
) {
    let mut live = collect_live_tuples(shared, old_heap, oldest_xmin).await;
    sort_tuples_by_index(index, key_attnums, &mut live, old_desc);
    for tuple in &live {
        rewrite_heap_tuple(shared, state, tuple, old_desc).await;
    }
}

/// Walk every old-heap page, testing each line pointer's tuple against the deadness
/// cutoff, and return the owned live tuples (physical order). Mirrors
/// `heap_page_prune`'s raw page walk: a scan filtered by our own snapshot would drop
/// recently-dead tuples that must be preserved for other snapshots; the vacuum test
/// (`HeapTupleSatisfiesVacuum != Dead`) is the right filter.
async fn collect_live_tuples(
    shared: &Arc<SharedState>,
    old_heap: &RelationData,
    oldest_xmin: crate::c::TransactionId,
) -> Vec<crate::access::htup::HeapTupleData> {
    use crate::storage::off::FIRST_OFFSET_NUMBER;

    let nblocks = heap_nblocks(shared, old_heap).await;
    let pool = shared.buffers();
    let mut out = Vec::new();

    for block in 0..nblocks {
        let buffer = read_heap_block(shared, old_heap, block).await;

        // Snapshot the page's line count under a brief share lock.
        let maxoff = {
            let _g = pool.content_share(buffer);
            pool.buffer_get_page(buffer).get_max_offset_number()
        };

        for offnum in FIRST_OFFSET_NUMBER..=maxoff {
            // Read the whole tuple (header + body) out under a brief share lock, then
            // release it before the awaiting deadness test.
            let tuple = {
                let _g = pool.content_share(buffer);
                let page = pool.buffer_get_page(buffer);
                let item_id = page.get_item_id(offnum);
                if item_id.is_normal() {
                    let item = page.get_item(&item_id);
                    debug_assert!(item.len() >= SizeofHeapTupleHeader);
                    let mut tid = ItemPointerData {
                        blkid: crate::storage::block::BlockIdData { hi: 0, lo: 0 },
                        posid: 0,
                    };
                    tid.set(block, offnum);
                    // SAFETY: a normal heap item is a HeapTupleHeaderData + body;
                    // copy the bytes into an owned tuple.
                    Some(unsafe { copy_page_item_to_tuple(item, tid, old_heap.rd_id) })
                } else {
                    None
                }
            };

            let Some(tuple) = tuple else { continue };
            if tuple_is_live(shared, &tuple, oldest_xmin).await {
                out.push(tuple);
            }
        }

        pool.release_buffer(buffer);
    }
    out
}

/// Sort `tuples` in the cluster index's key order (the btree comparator per key
/// column). Mirrors nbtsort's `sort_build_tuples`: resolve each key column's BTORDER
/// support proc from the index's opclass, compare the tuples' key datums
/// lexicographically, honoring the per-column NULLS-first / DESC index options.
fn sort_tuples_by_index(
    index: &RelationData,
    key_attnums: &[i16],
    tuples: &mut [crate::access::htup::HeapTupleData],
    old_desc: &crate::access::tupdesc::TupleDesc,
) {
    use crate::access::nbtree::{BTORDER_PROC, SK_BT_DESC, SK_BT_INDOPTION_SHIFT, SK_BT_NULLS_FIRST};
    use crate::backend::access::common::heaptuple::heap_getattr;
    use crate::backend::access::index::indexam::index_getprocinfo;
    use crate::fmgr::FunctionCall2Coll;

    struct Col {
        attno: i16,
        proc: std::cell::RefCell<crate::fmgr::FmgrInfo>,
        flags: i32,
        collation: Oid,
    }
    let cols: Vec<Col> = key_attnums
        .iter()
        .enumerate()
        .map(|(i, &attno)| {
            let proc =
                std::cell::RefCell::new(index_getprocinfo(index, (i + 1) as i32, BTORDER_PROC));
            let indoption = index.rd_indoption.get(i).copied().unwrap_or(0);
            let collation = index.rd_indcollation.get(i).copied().unwrap_or(InvalidOid);
            Col { attno, proc, flags: i32::from(indoption) << SK_BT_INDOPTION_SHIFT, collation }
        })
        .collect();

    tuples.sort_by(|a, b| {
        for col in &cols {
            // SAFETY: a/b own valid heap bodies matching old_desc; attno is a heap attr.
            let (av, an) = unsafe { heap_getattr(a, i32::from(col.attno), old_desc) };
            let (bv, bn) = unsafe { heap_getattr(b, i32::from(col.attno), old_desc) };
            let nulls_first = col.flags & SK_BT_NULLS_FIRST != 0;
            let ord = match (an, bn) {
                (true, true) => std::cmp::Ordering::Equal,
                (true, false) => {
                    if nulls_first { std::cmp::Ordering::Less } else { std::cmp::Ordering::Greater }
                }
                (false, true) => {
                    if nulls_first { std::cmp::Ordering::Greater } else { std::cmp::Ordering::Less }
                }
                (false, false) => {
                    let raw = {
                        let mut f = col.proc.borrow_mut();
                        let d = FunctionCall2Coll(&mut f, col.collation, av, bv)
                            .unwrap_or_else(|| unreachable!("comparator returned NULL"));
                        crate::postgres::DatumGetInt32(d)
                    };
                    let raw = if col.flags & SK_BT_DESC != 0 { -raw } else { raw };
                    raw.cmp(&0)
                }
            };
            if ord != std::cmp::Ordering::Equal {
                return ord;
            }
        }
        std::cmp::Ordering::Equal
    });
}

/// Copy a page item (a stored heap tuple: header + null bitmap + data) into an owned
/// `HeapTupleData` with the given self-TID + table OID.
///
/// # Safety
/// `item` must be a live normal heap item (at least `SizeofHeapTupleHeader` bytes).
unsafe fn copy_page_item_to_tuple(
    item: &[u8],
    tid: ItemPointerData,
    table_oid: Oid,
) -> crate::access::htup::HeapTupleData {
    use crate::access::htup::HeapTupleData;
    let len = item.len();
    // An 8-aligned owned body sized to hold the item bytes.
    let words = len.div_ceil(8);
    let mut body: Box<[u64]> = vec![0u64; words].into_boxed_slice();
    // SAFETY: body has words*8 >= len bytes; copy the item in.
    unsafe {
        core::ptr::copy_nonoverlapping(item.as_ptr(), body.as_mut_ptr().cast::<u8>(), len);
    }
    HeapTupleData {
        t_len: len as u32,
        t_self: tid,
        t_tableOid: table_oid,
        body: Some(body),
    }
}

/// Whether `tuple` should survive the rewrite: live iff its VACUUM status is not
/// `Dead` (PG keeps LIVE / RECENTLY_DEAD / INSERT|DELETE_IN_PROGRESS; only DEAD is
/// dropped). Reads the tuple's header.
async fn tuple_is_live(
    shared: &Arc<SharedState>,
    tuple: &crate::access::htup::HeapTupleData,
    oldest_xmin: crate::c::TransactionId,
) -> bool {
    // SAFETY: `tuple` owns a valid heap body; its header is the first bytes.
    let hdr = unsafe { &*tuple.t_data() };
    HeapTupleSatisfiesVacuum(shared, hdr, oldest_xmin).await != HTSV_Result::Dead
}

/// PG `mark_index_clustered`: set pg_index.indisclustered on `index_oid` and clear
/// it on the heap's other indexes (a table has at most one clustered index). pg_index
/// is not an on-disk catalog in this port, so the flag persists in the index registry
/// ([`set_index_clustered`]) -- the same data pg_index would hold.
fn mark_index_clustered(heap_relid: Oid, index_oid: Oid) {
    crate::backend::catalog::indexing::set_index_clustered(heap_relid, index_oid);
}

/// PG `swap_relation_files` (M13 non-mapped subset): exchange the relfilenode +
/// relpages/reltuples of the two relations' pg_class rows, so the new storage becomes
/// the table's and the transient relation ends up owning the old storage (which
/// `finish_heap_swap` then drops). Returns nothing; the callers refresh the relcache.
async fn swap_relation_files(shared: &Arc<SharedState>, r1: Oid, r2: Oid, r1_tuples: u64, r1_pages: i32) {
    let (rel1, form1) = read_pg_class_form(shared, r1).await;
    let (rel2, form2) = read_pg_class_form(shared, r2).await;

    // Swap the physical file numbers (+ tablespace) of the two rows. The target (r1)
    // now points at the compacted storage, so its relpages/reltuples reflect that.
    let new_r1 = PgClassPhys {
        relfilenode: form2.relfilenode,
        reltablespace: form2.reltablespace,
        relpages: r1_pages,
        reltuples: r1_tuples as f32,
    };
    let new_r2 = PgClassPhys {
        relfilenode: form1.relfilenode,
        reltablespace: form1.reltablespace,
        relpages: form1.relpages,
        reltuples: form1.reltuples,
    };
    write_pg_class_phys(shared, r1, &rel1.0, &rel1.1, &new_r1).await;
    write_pg_class_phys(shared, r2, &rel2.0, &rel2.1, &new_r2).await;

    // The row copies (rel1.1 / rel2.1) were owned; free them.
    heap_freetuple(rel1.1);
    heap_freetuple(rel2.1);
}

/// PG `finish_heap_swap`: swap the physical files, rebuild the target's indexes over
/// the new storage, drop the transient relation (unlinking the old storage), and
/// refresh the relcache so the next open sees the new relfilenode.
async fn finish_heap_swap(
    shared: &Arc<SharedState>,
    old_heap_oid: Oid,
    new_heap_oid: Oid,
    num_tuples: u64,
    new_pages: BlockNumber,
) {
    // Swap the relfilenode of the two pg_class rows.
    swap_relation_files(shared, old_heap_oid, new_heap_oid, num_tuples, new_pages as i32).await;
    CommandCounterIncrement();

    // Forget the relcache entries so they rebuild from the swapped pg_class rows (the
    // physical address is recomputed from the now-swapped relfilenode).
    relation_forget_relation(old_heap_oid);
    relation_forget_relation(new_heap_oid);

    // Reopen the target heap at its new storage and rebuild every index over it.
    relation_build_desc(shared, old_heap_oid).await;
    let heap = relation_id_get_relation(old_heap_oid)
        .unwrap_or_else(|| unreachable!("target heap {old_heap_oid:?} reopened after swap"));
    rebuild_indexes(shared, &heap).await;
    relation_close(heap);

    // Drop the transient relation, which now owns the old (pre-cluster) storage; this
    // unlinks that storage and deletes the transient catalog rows.
    crate::backend::commands::tablecmds::heap_drop_with_catalog(shared, new_heap_oid).await;
    CommandCounterIncrement();
}

/// Rebuild every index registered on `heap` over its (now-rewritten) storage. Each
/// index's btree is rebuilt from scratch by `index_build` (btbuild), which packs the
/// index anew from block 0 -- so it picks up the compacted, reordered heap.
async fn rebuild_indexes(shared: &Arc<SharedState>, heap: &RelationData) {
    use crate::backend::catalog::index::{index_build, make_index_info};
    for ri in relation_get_index_list(heap.rd_id) {
        let index_info = make_index_info(&ri.key_attnums, ri.unique);
        index_build(shared, heap, &ri.index, &index_info).await;
    }
}

// ---------------------------------------------------------------------------
// pg_class physical-column read/write helpers
// ---------------------------------------------------------------------------

/// The physical columns swap_relation_files exchanges.
struct PgClassPhys {
    relfilenode: Oid,
    reltablespace: Oid,
    relpages: i32,
    reltuples: f32,
}

/// A pg_class row's owned copy: the tuple (for its TID) and its deformed form.
type PgClassRow = (
    ItemPointerData,
    crate::access::htup::HeapTupleData,
);

/// Read the pg_class form of `relid` (its physical columns + the row TID) as an
/// owned copy. Returns `((tid, tuple), form_copy)`.
async fn read_pg_class_form(
    shared: &Arc<SharedState>,
    relid: Oid,
) -> (PgClassRow, Box<pc::FormData_pg_class>) {
    use crate::access::htup_details::GETSTRUCT;
    let rows = crate::backend::commands::tablecmds::scan_catalog_rows_by_oid(
        shared,
        pc::RelationRelationId,
        pc::Anum_pg_class_oid,
        relid,
    )
    .await;
    let mut it = rows.into_iter();
    let row = loop {
        let Some(row) = it.next() else {
            unreachable!("pg_class row for {relid:?} exists");
        };
        // SAFETY: owned tuple; verify the oid matches (skip stale/dead rows).
        let p = GETSTRUCT(&row.tuple).cast::<pc::FormData_pg_class>();
        if unsafe { (*p).oid } == relid {
            break row;
        }
        heap_freetuple(row.tuple);
    };
    // Free any remaining rows.
    for extra in it {
        heap_freetuple(extra.tuple);
    }
    // SAFETY: owned tuple; copy out the fixed pg_class part.
    let form: Box<pc::FormData_pg_class> = {
        let p = GETSTRUCT(&row.tuple).cast::<pc::FormData_pg_class>();
        Box::new(unsafe { core::ptr::read(p) })
    };
    ((row.tid, row.tuple), form)
}

/// Write the physical columns `phys` into the pg_class row at `tid` (whose current
/// image is `tuple`), via CatalogTupleUpdate.
async fn write_pg_class_phys(
    shared: &Arc<SharedState>,
    _relid: Oid,
    tid: &ItemPointerData,
    tuple: &crate::access::htup::HeapTupleData,
    phys: &PgClassPhys,
) {
    use crate::backend::access::common::heaptuple::{heap_deform_tuple, heap_form_tuple};
    use crate::postgres::{Float4GetDatum, Int32GetDatum, ObjectIdGetDatum};

    let pg_class = relation_id_get_relation(pc::RelationRelationId)
        .unwrap_or_else(|| unreachable!("pg_class is nailed/open"));
    let desc = pg_class.rd_att.clone().unwrap_or_else(|| unreachable!("pg_class descriptor"));

    // SAFETY: owned tuple + matching descriptor.
    let (mut vals, mut nulls) = unsafe { heap_deform_tuple(tuple, &desc) };
    let set = |vals: &mut [crate::postgres::Datum], nulls: &mut [bool], anum: i32, d| {
        vals[(anum - 1) as usize] = d;
        nulls[(anum - 1) as usize] = false;
    };
    set(&mut vals, &mut nulls, pc::Anum_pg_class_relfilenode, ObjectIdGetDatum(phys.relfilenode));
    set(&mut vals, &mut nulls, pc::Anum_pg_class_reltablespace, ObjectIdGetDatum(phys.reltablespace));
    set(&mut vals, &mut nulls, pc::Anum_pg_class_relpages, Int32GetDatum(phys.relpages));
    set(&mut vals, &mut nulls, pc::Anum_pg_class_reltuples, Float4GetDatum(phys.reltuples));

    let mut newtup = heap_form_tuple(&desc, &vals, &nulls);
    catalog_tuple_update(shared, &pg_class, tid, &mut newtup).await;
    heap_freetuple(newtup);
    relation_close(pg_class);
}

// ---------------------------------------------------------------------------
// heap block helpers (private copies of the vacuumlazy ones)
// ---------------------------------------------------------------------------

/// The block count of `relation`'s main fork.
async fn heap_nblocks(shared: &Arc<SharedState>, relation: &RelationData) -> BlockNumber {
    use crate::common::relpath::ForkNumber;
    let smgr_ptr = relation.smgr();
    // SAFETY: relcache-owned smgr handle, valid while the rel is open.
    let smgr = unsafe { &mut *smgr_ptr };
    smgr.nblocks(shared, ForkNumber::MAIN_FORKNUM).await
}

/// Read a main-fork block of `relation` into a pinned buffer.
async fn read_heap_block(
    shared: &Arc<SharedState>,
    relation: &RelationData,
    block: BlockNumber,
) -> crate::storage::buf::Buffer {
    use crate::common::relpath::ForkNumber;
    use crate::storage::bufmgr::ReadBufferMode;
    let relpersistence = relation.form().relpersistence;
    let smgr_ptr = relation.smgr();
    // SAFETY: relcache-owned smgr handle valid while rel is open.
    let smgr = unsafe { &mut *smgr_ptr };
    crate::backend::storage::buffer::bufmgr::read_buffer_common(
        shared,
        smgr,
        relpersistence,
        ForkNumber::MAIN_FORKNUM,
        block,
        ReadBufferMode::NORMAL,
        None,
    )
    .await
}

#[cfg(test)]
#[path = "cluster_tests.rs"]
mod cluster_tests;
