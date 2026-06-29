//! Routines for accessing the system catalogs for the planner. Translated from
//! backend/optimizer/util/plancat.c.
//!
//! Non-type-centric free functions; bodies here as snake_case `pub fn`s,
//! re-exported from `crate::optimizer::plancat` under the C names.
//!
//! Disposition: `grow`. M2's live path is `get_relation_info` for a plain heap
//! relation: open the (already-locked, relcache-resident) relation, fill the
//! `RelOptInfo`'s attribute range / tablespace / size estimates, and leave the
//! index list empty (no index paths until M6). The open is a synchronous relcache
//! lookup (the relation was locked during parse analysis); `estimate_rel_size`
//! reads the cached `relpages`/`reltuples` from pg_class for M2 rather than the
//! live smgr block count (which would color the planner async) -- the live
//! block-count path grows when stale-stats handling matters. The selectivity /
//! FDW / trigger / generated-column helpers remain grow guards (rules.md s4).

use crate::access::attnum::AttrNumber;
use crate::nodes::pathnodes::{PlannerInfo, RelOptInfo};
use crate::postgres_ext::{InvalidOid, Oid};
use crate::storage::block::BlockNumber;
use crate::utils::rel::RelationData;

/// PG `FirstLowInvalidHeapAttributeNumber` (sysattr.h): the lowest valid system
/// attribute number is -7, so `min_attr = FirstLowInvalidHeapAttributeNumber + 1`.
const FIRST_LOW_INVALID_HEAP_ATTRIBUTE_NUMBER: AttrNumber = -7;

/// PG `get_relation_info`: gather the planner's per-relation info into `rel` from
/// the relcache entry for `relation_object_id`. M2 covers a plain heap relation:
/// attribute range, tablespace, and size estimates. The relation was already
/// locked during parse analysis, so the open is a sync relcache lookup (NoLock).
pub fn get_relation_info(
    root: &mut PlannerInfo,
    relation_object_id: Oid,
    inhparent: bool,
    rel: &mut RelOptInfo,
) {
    if inhparent {
        not_yet_reachable("get_relation_info: inheritance parent");
    }

    // table_open(relationObjectId, NoLock): the lock is already held, so this is a
    // pure relcache lookup. The entry was built during parse analysis.
    let relation = crate::backend::utils::cache::relcache::relation_id_get_relation(relation_object_id)
        .unwrap_or_else(|| not_yet_reachable("get_relation_info: relation not in relcache"));

    let rel_data: &RelationData = &relation;
    let relkind = rel_data.form().relkind;
    let reltablespace = rel_data.form().reltablespace;

    if !is_scannable_relkind(relkind) {
        not_yet_reachable("get_relation_info: non-heap relation kind");
    }

    rel.min_attr = FIRST_LOW_INVALID_HEAP_ATTRIBUTE_NUMBER + 1;
    rel.max_attr = rel_data.number_of_attributes();
    rel.reltablespace = reltablespace;
    crate::assert!(rel.max_attr >= rel.min_attr);

    // attr_needed / attr_widths span min_attr..=max_attr; zero-initialized.
    let span = (rel.max_attr - rel.min_attr + 1) as usize;
    rel.attr_needed = vec![None; span];
    rel.attr_widths = vec![0; span];

    // Estimate relation size (pages/tuples/allvisfrac) from cached pg_class stats.
    let est = estimate_rel_size(rel_data, &rel.attr_widths);
    rel.pages = est.pages;
    rel.tuples = est.tuples;
    rel.allvisfrac = est.allvisfrac;
    rel.attr_widths = est.attr_widths;

    // Index list (M6): build an IndexOptInfo per registered index of this heap. PG
    // reads pg_index via RelationGetIndexList + opens each index; the M6 port reads
    // the index registry (the rd_indexlist stand-in, written by index_create).
    rel.indexlist = build_index_opt_infos(rel, &relation);
    rel.statlist = Vec::new();
    rel.rel_parallel_workers = -1;
    rel.amflags = crate::nodes::pathnodes::AmFlags::empty();

    crate::backend::utils::cache::relcache::relation_close(relation);
    let _ = root;
}

/// Build the `IndexOptInfo` list for `rel` from the heap's registered indexes (the
/// `rd_indexlist` stand-in). Each index contributes one `IndexOptInfo` carrying the
/// key columns, the btree opfamily/opclass-input-type per key column, the access
/// method, and the index targetlist (the indexed columns as INDEX_VAR Vars). M6
/// covers simple-column btree indexes; expression / partial / multi-AM indexes grow.
#[allow(
    clippy::vec_box,
    reason = "1:1 PG port: RelOptInfo.indexlist is a List* of IndexOptInfo pointers -> Vec<Box<_>>"
)]
fn build_index_opt_infos(
    rel: &crate::nodes::pathnodes::RelOptInfo,
    heap: &RelationData,
) -> Vec<Box<crate::nodes::pathnodes::IndexOptInfo>> {
    use crate::backend::catalog::indexing::relation_get_index_list;
    use crate::nodes::pathnodes::IndexOptInfo;

    let registered = relation_get_index_list(heap.rd_id);
    let mut out = Vec::with_capacity(registered.len());
    for ri in registered {
        let index = &ri.index;
        let nkeys = ri.key_attnums.len();
        let indexkeys: Vec<i32> = ri.key_attnums.iter().map(|&a| i32::from(a)).collect();

        // Per-key-column opclass-input type + btree opfamily. The relcache index's
        // rd_opcintype was filled by index_init_opclass_support; opfamily is derived
        // from the column type (the builtin btree families).
        let mut opcintype = Vec::with_capacity(nkeys);
        let mut opfamily = Vec::with_capacity(nkeys);
        let heap_desc = heap.rd_att.as_ref();
        for &attno in &ri.key_attnums {
            let coltype = heap_desc.map_or(InvalidOid, |d| d.attr((attno - 1) as usize).atttypid);
            opcintype.push(coltype);
            opfamily.push(btree_opfamily_for_type(coltype));
        }

        // indextlist: the index columns as INDEX_VAR Vars (PG build_index_tlist).
        let indextlist = build_index_tlist(heap, &ri.key_attnums);

        // Size estimate: the index pages/tuples are not tracked in pg_class on M6
        // (index_update_stats is staged), so estimate the index size from the heap's
        // tuple count (one index tuple per heap tuple; a shallow btree).
        let index_tuples = rel.tuples;
        let index_pages = estimate_index_pages(index_tuples);

        out.push(Box::new(IndexOptInfo {
            indexoid: index.rd_id,
            reltablespace: index.form().reltablespace,
            rel: None,
            pages: index_pages,
            tuples: index_tuples,
            tree_height: -1,
            ncolumns: nkeys as i32,
            nkeycolumns: nkeys as i32,
            indexkeys,
            indexcollations: vec![InvalidOid; nkeys],
            opfamily,
            opcintype,
            sortopfamily: Vec::new(),
            reverse_sort: vec![false; nkeys],
            nulls_first: vec![false; nkeys],
            opclassoptions: vec![None; nkeys],
            canreturn: vec![true; nkeys],
            relam: crate::backend::commands::indexcmds::BTREE_AM_OID_PUB,
            indexprs: Vec::new(),
            indpred: Vec::new(),
            indextlist,
            // PG fills indrestrictinfo in check_index_predicates (before path gen);
            // M6 sets it in create_index_paths from the rel's baserestrictinfo (the
            // WHERE quals are distributed after get_relation_info runs).
            indrestrictinfo: Vec::new(),
            pred_ok: false,
            unique: ri.unique,
            nullsnotdistinct: false,
            immediate: true,
            hypothetical: false,
            amcanorderbyop: false,
            amoptionalkey: true,
            amsearcharray: false,
            amsearchnulls: false,
            amhasgettuple: true,
            amhasgetbitmap: true,
            amcanparallel: false,
            amcanmarkpos: false,
        }));
    }
    out
}

/// The btree opfamily OID for a builtin type (the `btree/integer_ops` etc. families
/// SEED_PG_OPCLASS encodes). M6 builtin set; InvalidOid otherwise.
fn btree_opfamily_for_type(type_id: Oid) -> Oid {
    match type_id.0 {
        20 | 21 | 23 => Oid(1976), // int2/int4/int8 -> integer_ops btree family
        26 => Oid(1989),           // oid -> oid_ops btree family
        25 => Oid(1994),           // text -> text_ops btree family
        _ => InvalidOid,
    }
}

/// PG `build_index_tlist` (M6 simple-column subset): a TargetEntry per index key
/// column, each an INDEX_VAR Var over the heap column's type.
fn build_index_tlist(heap: &RelationData, key_attnums: &[i16]) -> Vec<crate::nodes::nodes::Node> {
    use crate::backend::nodes::makefuncs::{make_target_entry, make_var};
    use crate::nodes::nodes::Node;
    use crate::nodes::primnodes::INDEX_VAR;

    let Some(desc) = heap.rd_att.as_ref() else {
        return Vec::new();
    };
    key_attnums
        .iter()
        .enumerate()
        .map(|(i, &attno)| {
            let att = desc.attr((attno - 1) as usize);
            let var = make_var(
                INDEX_VAR,
                (i + 1) as i16,
                att.atttypid,
                att.atttypmod,
                att.attcollation,
                0,
            );
            let tle = make_target_entry(Some(Node::Var(Box::new(var))), (i + 1) as i16, None, false);
            Node::TargetEntry(Box::new(tle))
        })
        .collect()
}

/// Rough index page estimate from the index tuple count (a shallow btree: ~200
/// int4 index tuples per 8 KB page). At least one page when nonempty.
fn estimate_index_pages(tuples: f64) -> BlockNumber {
    if tuples <= 0.0 {
        1
    } else {
        ((tuples / 200.0).ceil() as BlockNumber).max(1)
    }
}

/// Out-params of `estimate_rel_size` folded into a struct.
pub struct RelSizeEstimate {
    pub attr_widths: Vec<i32>,
    pub pages: BlockNumber,
    pub tuples: f64,
    pub allvisfrac: f64,
}

/// PG `estimate_rel_size`: page/tuple/all-visible estimates. Reads the cached
/// `relpages`/`reltuples`/`relallvisible` from pg_class (set by CREATE/ANALYZE).
///
/// A never-analyzed relation has `relpages == 0` and `reltuples == -1`; PG then uses
/// the live smgr block count to estimate. The sync planner cannot reach the smgr
/// here, so the port uses PG's same zero-data fallback (`estimate_rel_size`'s
/// `curpages == 0` branch): assume a minimum of 10 pages and derive the tuple count
/// from the per-page tuple density. This keeps the cost model meaningful before
/// ANALYZE; the live block-count refinement grows when the planner gains an async
/// stats path (rules.md s4).
pub fn estimate_rel_size(rel: &RelationData, cur_attr_widths: &[i32]) -> RelSizeEstimate {
    let form = rel.form();
    let relpages = form.relpages;
    let reltuples = form.reltuples;
    let relallvisible = form.relallvisible;

    // PG: curpages = the live block count; here the cached relpages, with the
    // never-analyzed (relpages==0) fallback to a 10-page minimum guess.
    let analyzed = relpages > 0 && reltuples >= 0.0;
    let (pages, tuples) = if analyzed {
        (relpages as BlockNumber, f64::from(reltuples))
    } else {
        // estimate_rel_size: with no data, guess 10 pages and derive tuples from the
        // tuple density. The row size is the heap-tuple header (MAXALIGNed 24 bytes)
        // plus the summed column widths + one 4-byte item pointer; per-page tuple
        // count = usable page bytes / row size.
        let curpages: BlockNumber = 10;
        let row_width = cur_attr_widths.iter().map(|w| (*w).max(0)).sum::<i32>().max(0) as usize;
        let tuple_size = crate::c::MAXALIGN(24 + row_width).max(crate::c::MAXALIGN(24 + 4));
        let usable_per_page = 8192usize.saturating_sub(24); // BLCKSZ - page header
        let per_page = (usable_per_page / (tuple_size + 4)).max(1);
        let tuples = (f64::from(curpages) * per_page as f64).max(1.0);
        (curpages, tuples)
    };

    let allvisfrac = if pages == 0 {
        0.0
    } else {
        (f64::from(relallvisible) / f64::from(pages)).clamp(0.0, 1.0)
    };

    RelSizeEstimate {
        attr_widths: cur_attr_widths.to_vec(),
        pages,
        tuples,
        allvisfrac,
    }
}

/// Is this relkind a plain scannable table for M2?
fn is_scannable_relkind(relkind: i8) -> bool {
    relkind == crate::catalog::pg_class::RELKIND_RELATION
}

#[cold]
fn not_yet_reachable(what: &str) -> ! {
    unimplemented!("{what}: not yet translated for this milestone");
}
