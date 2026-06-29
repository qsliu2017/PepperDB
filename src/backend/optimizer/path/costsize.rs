//! Routines to compute (and set) relation and path costs. Translated from
//! backend/optimizer/path/costsize.c.
//!
//! Non-type-centric free functions; bodies here as snake_case `pub fn`s,
//! re-exported from `crate::optimizer::cost` under the C names.
//!
//! Disposition: `grow`. M2's live path is `cost_seqscan` for a plain base-rel
//! sequential scan: disk cost (`seq_page_cost * pages`) + CPU cost
//! (`cpu_tuple_cost * tuples` + tlist/qual eval). The cost GUCs use their compiled
//! defaults (the GUC subsystem is M9); parallelism, qual-cost evaluation of real
//! restriction clauses, and the many other `cost_*` estimators are grow guards
//! (rules.md s4).

use crate::nodes::pathnodes::{
    BitmapHeapPath, IndexPath, ParamPathInfo, Path, PlannerInfo, RelOptInfo,
};
use crate::nodes::parsenodes::RTEKind;
use crate::optimizer::cost::{
    DEFAULT_CPU_INDEX_TUPLE_COST, DEFAULT_CPU_OPERATOR_COST, DEFAULT_CPU_TUPLE_COST,
    DEFAULT_RANDOM_PAGE_COST, DEFAULT_SEQ_PAGE_COST,
};

/// PG `cost_seqscan`: set `path`'s startup/total cost and row estimate for a
/// sequential scan of `baserel`. M2 uses the default tablespace page cost and the
/// default per-tuple CPU cost; there are no restriction quals (no WHERE yet), so
/// the qual cost is zero. Parallelism is not considered.
pub fn cost_seqscan(
    path: &mut Path,
    _root: &mut PlannerInfo,
    baserel: &RelOptInfo,
    param_info: Option<&ParamPathInfo>,
) {
    // Should only be applied to base relations.
    crate::assert!(baserel.relid > 0);
    crate::assert!(baserel.rtekind == RTEKind::RELATION);

    if param_info.is_some() {
        not_yet_reachable("cost_seqscan: parameterized path (ppi_rows)");
    }
    path.rows = baserel.rows;

    // Disk costs: seq_page_cost * pages (default tablespace page cost).
    let disk_run_cost = DEFAULT_SEQ_PAGE_COST * f64::from(baserel.pages);

    // CPU costs: no restriction quals in M2, so qpqual cost is zero.
    let mut startup_cost = 0.0;
    let cpu_per_tuple = DEFAULT_CPU_TUPLE_COST;
    let mut cpu_run_cost = cpu_per_tuple * baserel.tuples;

    // tlist eval costs are paid per output row.
    let target = path
        .pathtarget
        .as_ref()
        .unwrap_or_else(|| not_yet_reachable("cost_seqscan: missing pathtarget"));
    startup_cost += target.cost.startup;
    cpu_run_cost = target.cost.per_tuple.mul_add(path.rows, cpu_run_cost);

    if path.parallel_workers > 0 {
        not_yet_reachable("cost_seqscan: parallel sequential scan");
    }

    // disabled_nodes: enable_seqscan defaults true, so 0.
    path.disabled_nodes = 0;
    path.startup_cost = startup_cost;
    path.total_cost = startup_cost + cpu_run_cost + disk_run_cost;
}

/// PG `cost_index` (M6 rough form): cost an index scan. The scan reads
/// `indexselectivity * index_pages` index pages (random access) plus the matching
/// heap pages, and processes `selectivity * tuples` rows. The estimate is rough but
/// cost-comparable to `cost_seqscan`: a selective qual touches few index + heap
/// pages, so the index path wins; a non-selective qual touches most of the heap at
/// random-page cost, so the seqscan path wins.
pub fn cost_index(path: &mut IndexPath, _root: &mut PlannerInfo, _loop_count: f64, _partial_path: bool) {
    let baserel = path
        .path
        .parent
        .as_ref()
        .unwrap_or_else(|| not_yet_reachable("cost_index: missing parent rel"));
    crate::assert!(baserel.relid > 0);
    crate::assert!(baserel.rtekind == RTEKind::RELATION);

    let index = &path.indexinfo;
    let selectivity = path.indexselectivity.clamp(0.0, 1.0);

    // Rows returned: selectivity * the rel's tuple count (at least one).
    let tuples_fetched = (selectivity * baserel.tuples).max(1.0);
    path.path.rows = tuples_fetched;

    // Index access: descend the (shallow) btree + scan the selected index leaf
    // pages, at random-page cost. At least one index page is touched.
    let index_pages_read = (selectivity * f64::from(index.pages)).max(1.0);
    let index_cpu = (DEFAULT_CPU_INDEX_TUPLE_COST + DEFAULT_CPU_OPERATOR_COST) * tuples_fetched;
    let index_io = DEFAULT_RANDOM_PAGE_COST * index_pages_read;
    let indextotalcost = index_io + index_cpu;
    path.indextotalcost = indextotalcost;

    // Heap access: one random heap page fetch per selected tuple (no correlation
    // modeling on M6 -- the pessimistic upper bound, capped at the heap size).
    let heap_pages = f64::from(baserel.pages).max(1.0);
    let heap_pages_fetched = tuples_fetched.min(heap_pages);
    let heap_io = DEFAULT_RANDOM_PAGE_COST * heap_pages_fetched;

    // CPU: per-tuple processing of the fetched rows + the tlist eval.
    let mut startup_cost = indextotalcost - index_cpu; // index descent is paid up front-ish
    let mut run_cost = DEFAULT_CPU_TUPLE_COST.mul_add(tuples_fetched, heap_io) + index_cpu;

    let target = path
        .path
        .pathtarget
        .as_ref()
        .unwrap_or_else(|| not_yet_reachable("cost_index: missing pathtarget"));
    startup_cost += target.cost.startup;
    run_cost = target.cost.per_tuple.mul_add(tuples_fetched, run_cost);

    path.path.disabled_nodes = 0;
    path.path.startup_cost = startup_cost;
    path.path.total_cost = startup_cost + run_cost;
}

/// PG `cost_bitmap_tree_node`: the index-access cost + selectivity of a bitmap
/// producer subpath. The producer is an IndexPath whose `index_detail` carries the
/// index-access-only cost (`indextotalcost`, excluding the per-tuple heap fetch) and
/// the index selectivity -- exactly what the bitmap-heap caller needs.
#[must_use]
pub fn cost_bitmap_tree_node(path: &Path) -> (f64, f64) {
    // A producer with no detail (not reachable on M6) falls back to its total cost.
    path.index_detail
        .as_ref()
        .map_or((path.total_cost, 1.0), |d| (d.indextotalcost, d.indexselectivity))
}

/// PG `cost_bitmap_heap_scan` (M6 rough form): cost a bitmap heap scan. The bitmap is
/// produced by `path.bitmapqual` (an IndexPath); the heap is then fetched in physical
/// page order, so the heap I/O is at sequential-page cost over the selected pages
/// (cheaper than the per-tuple random fetch of a plain index scan when many rows
/// match). Cost-comparable to cost_seqscan + cost_index.
pub fn cost_bitmap_heap_scan(
    path: &mut BitmapHeapPath,
    _root: &mut PlannerInfo,
    baserel: &RelOptInfo,
    _loop_count: f64,
) {
    crate::assert!(baserel.relid > 0);
    crate::assert!(baserel.rtekind == RTEKind::RELATION);

    // The bitmap producer's index-access cost + selectivity.
    let (bitmap_cost, selectivity) = cost_bitmap_tree_node(&path.bitmapqual);
    let selectivity = selectivity.clamp(0.0, 1.0);
    let tuples_fetched = (selectivity * baserel.tuples).max(1.0);
    path.path.rows = tuples_fetched;

    // Heap pages fetched: the selected fraction of the heap, read in page order at
    // sequential-page cost (Mackert-Lohman would refine this; M6 uses the linear
    // fraction). At least one page.
    let heap_pages = f64::from(baserel.pages).max(1.0);
    let pages_fetched = (selectivity * heap_pages).max(1.0);
    let heap_io = DEFAULT_SEQ_PAGE_COST * pages_fetched;

    let startup_cost = bitmap_cost;
    let mut run_cost = DEFAULT_CPU_TUPLE_COST.mul_add(tuples_fetched, heap_io);

    let target = path
        .path
        .pathtarget
        .as_ref()
        .unwrap_or_else(|| not_yet_reachable("cost_bitmap_heap_scan: missing pathtarget"));
    run_cost = target.cost.per_tuple.mul_add(tuples_fetched, run_cost);

    path.path.disabled_nodes = 0;
    path.path.startup_cost = startup_cost + target.cost.startup;
    path.path.total_cost = path.path.startup_cost + run_cost;
}

#[cold]
fn not_yet_reachable(what: &str) -> ! {
    unimplemented!("{what}: not yet translated for this milestone");
}
