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

use crate::nodes::nodes::{Cardinality, Cost, JoinType, Node};
use crate::nodes::pathnodes::{
    BitmapHeapPath, HashPath, IndexPath, JoinCostWorkspace, JoinPathExtraData, MergePath, NestPath,
    ParamPathInfo, Path, PathType, PlannerInfo, QualCost, RelOptInfo, SpecialJoinInfo,
};
use crate::nodes::parsenodes::RTEKind;
use crate::optimizer::cost::{
    DEFAULT_CPU_INDEX_TUPLE_COST, DEFAULT_CPU_OPERATOR_COST, DEFAULT_CPU_TUPLE_COST,
    DEFAULT_RANDOM_PAGE_COST, DEFAULT_SEQ_PAGE_COST,
};

/// PG GUC `work_mem` default (4MB, in KB). The GUC subsystem is M9; until it sets
/// `work_mem`, the join/sort cost uses the compiled default so the in-memory vs
/// spill decision is realistic.
const DEFAULT_WORK_MEM_KB: f64 = 4096.0;
/// PG `BLCKSZ`: bytes per page, for the disk-sort page-count estimate.
const BLCKSZ: f64 = 8192.0;
/// PG sizeof(HeapTupleHeaderData) + per-tuple pointer overhead used by
/// `relation_byte_size` (the MAXALIGN'd tuple-header + item pointer).
const TUPLE_OVERHEAD_BYTES: f64 = 24.0;

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

/// PG `clamp_row_est`: force a row estimate to be at least 1.0, finite, and an
/// integer. (1e100 is PG's `MAXIMUM_ROWCOUNT`.)
#[must_use]
pub fn clamp_row_est(nrows: f64) -> f64 {
    const MAXIMUM_ROWCOUNT: f64 = 1e100;
    if nrows > MAXIMUM_ROWCOUNT || nrows.is_nan() {
        MAXIMUM_ROWCOUNT
    } else if nrows <= 1.0 {
        1.0
    } else {
        nrows.round()
    }
}

/// PG `relation_byte_size`: `tuples * (MAXALIGN(width) + MAXALIGN(tupleheader))`.
fn relation_byte_size(tuples: f64, width: i32) -> f64 {
    let w = f64::from(maxalign(i64::from(width.max(0))) as i32);
    tuples * (w + TUPLE_OVERHEAD_BYTES)
}

/// 8-byte MAXALIGN.
const fn maxalign(n: i64) -> i64 {
    (n + 7) & !7
}

// ---------------------------------------------------------------------------
// Sort cost
// ---------------------------------------------------------------------------

/// PG `cost_tuplesort` (the in-memory/disk core of `cost_sort`): startup is the
/// comparison cost (~N log2 N), run is one operator-cost per extracted tuple. The
/// disk-spill branch adds page I/O when the sort doesn't fit in `work_mem`. Used by
/// the mergejoin cost to price an explicit Sort of an input. LIMIT pushdown
/// (`limit_tuples`) and incremental sort are not used by the M7 join cost.
fn cost_tuplesort(tuples_in: f64, width: i32, comparison_cost_in: f64, sort_mem_kb: f64) -> (Cost, Cost) {
    let tuples = if tuples_in < 2.0 { 2.0 } else { tuples_in };
    let comparison_cost = 2.0f64.mul_add(DEFAULT_CPU_OPERATOR_COST, comparison_cost_in);
    let input_bytes = relation_byte_size(tuples, width);
    let sort_mem_bytes = sort_mem_kb * 1024.0;

    let startup_cost = if input_bytes > sort_mem_bytes {
        // Disk-based external sort: N log2 N comparisons + page I/O.
        let npages = (input_bytes / BLCKSZ).ceil();
        // Merge order approximation (PG `tuplesort_merge_order`); a fixed small fan-in
        // is enough for the M7 cost comparison.
        let mergeorder = (sort_mem_bytes / (2.0 * BLCKSZ)).max(2.0);
        let nruns = input_bytes / sort_mem_bytes;
        let log_runs = if nruns > mergeorder { nruns.log(mergeorder).ceil() } else { 1.0 };
        let npageaccesses = 2.0 * npages * log_runs;
        comparison_cost.mul_add(
            tuples * tuples.log2(),
            npageaccesses * DEFAULT_RANDOM_PAGE_COST.mul_add(0.25, DEFAULT_SEQ_PAGE_COST * 0.75),
        )
    } else {
        // In-memory quicksort.
        comparison_cost * tuples * tuples.log2()
    };
    let run_cost = DEFAULT_CPU_OPERATOR_COST * tuples;
    (startup_cost, run_cost)
}

/// PG `cost_sort`: fill a dummy `Path` with the cost of sorting `input_cost`'s
/// output by `pathkeys`. M7 uses the plain (non-incremental, no-LIMIT) form that
/// the mergejoin cost needs. `pathkeys` only affects whether a sort is needed
/// (decided by the caller); here it is informational.
#[allow(clippy::too_many_arguments, reason = "1:1 PG cost_sort signature")]
pub fn cost_sort(
    path: &mut Path,
    _root: &mut PlannerInfo,
    _pathkeys: &[Node],
    input_disabled_nodes: i32,
    input_cost: Cost,
    tuples: f64,
    width: i32,
    comparison_cost: Cost,
    sort_mem: i32,
    _limit_tuples: f64,
) {
    let sort_mem_kb = if sort_mem > 0 { f64::from(sort_mem) } else { DEFAULT_WORK_MEM_KB };
    let (sort_startup, run_cost) = cost_tuplesort(tuples, width, comparison_cost, sort_mem_kb);
    let mut startup_cost = input_cost + sort_startup;
    // enable_sort defaults true -> no disabled node from the Sort itself.
    path.disabled_nodes = input_disabled_nodes;
    startup_cost += 0.0;
    path.startup_cost = startup_cost;
    path.total_cost = startup_cost + run_cost;
}

// ---------------------------------------------------------------------------
// Qual eval cost
// ---------------------------------------------------------------------------

/// PG `cost_qual_eval` (M7 rough form): the startup + per-tuple cost of evaluating
/// a list of clauses. PG walks each clause's expression tree charging per operator/
/// function; for M7 we charge one `cpu_operator_cost` per clause (each join/restrict
/// clause is a single comparison OpExpr), which is enough to make the three join
/// methods comparable. The full per-node walk grows with the expression cost model.
#[must_use]
pub fn cost_qual_eval(quals: &[Node], _root: &mut PlannerInfo) -> QualCost {
    let per_tuple = DEFAULT_CPU_OPERATOR_COST * quals.len() as f64;
    QualCost { startup: 0.0, per_tuple }
}

/// `cost_qual_eval` over a RestrictInfo list (the join cost callers hold
/// `Vec<Box<RestrictInfo>>`). Charges one operator cost per clause.
fn cost_restrictinfo_eval(quals: &[Box<crate::nodes::pathnodes::RestrictInfo>]) -> QualCost {
    QualCost { startup: 0.0, per_tuple: DEFAULT_CPU_OPERATOR_COST * quals.len() as f64 }
}

// ---------------------------------------------------------------------------
// Nestloop cost
// ---------------------------------------------------------------------------

/// PG `initial_cost_nestloop`: cheap lower bound on a nestloop's cost (the source-
/// data scan + per-outer-row rescans of the inner). The CPU qual cost is deferred to
/// `final_cost_nestloop`. M7 inner join: no rescan discount (`cost_rescan` for a
/// plain scan is just the inner total cost), no SEMI/ANTI early stop.
pub fn initial_cost_nestloop(
    _root: &mut PlannerInfo,
    workspace: &mut JoinCostWorkspace,
    jointype: JoinType,
    outer_path: &Path,
    inner_path: &Path,
    extra: &JoinPathExtraData,
) {
    if jointype == JoinType::SEMI || jointype == JoinType::ANTI || extra.inner_unique {
        not_yet_reachable("initial_cost_nestloop: SEMI/ANTI/inner_unique early stop");
    }
    // enable_nestloop defaults true.
    let disabled_nodes = inner_path.disabled_nodes + outer_path.disabled_nodes;
    let outer_path_rows = outer_path.rows;

    // Rescan cost of the inner: for a plain scan, the same as a fresh scan.
    let inner_rescan_start_cost = inner_path.startup_cost;
    let inner_rescan_total_cost = inner_path.total_cost;

    let startup_cost = outer_path.startup_cost + inner_path.startup_cost;
    let mut run_cost = outer_path.total_cost - outer_path.startup_cost;
    if outer_path_rows > 1.0 {
        run_cost = (outer_path_rows - 1.0).mul_add(inner_rescan_start_cost, run_cost);
    }
    let inner_run_cost = inner_path.total_cost - inner_path.startup_cost;
    let inner_rescan_run_cost = inner_rescan_total_cost - inner_rescan_start_cost;

    // Normal case: scan the whole inner rel for each outer row.
    run_cost += inner_run_cost;
    if outer_path_rows > 1.0 {
        run_cost = (outer_path_rows - 1.0).mul_add(inner_rescan_run_cost, run_cost);
    }

    workspace.disabled_nodes = disabled_nodes;
    workspace.startup_cost = startup_cost;
    workspace.total_cost = startup_cost + run_cost;
    workspace.run_cost = run_cost;
    workspace.inner_run_cost = inner_run_cost;
    workspace.inner_rescan_run_cost = inner_rescan_run_cost;
}

/// PG `final_cost_nestloop`: finalize the nestloop cost + row estimate, adding the
/// CPU cost of evaluating the join quals once per processed (outer x inner) tuple.
pub fn final_cost_nestloop(
    root: &mut PlannerInfo,
    path: &mut NestPath,
    workspace: &JoinCostWorkspace,
    _extra: &JoinPathExtraData,
) {
    let _ = root;
    let outer_path_rows = path.jpath.outerjoinpath.rows.max(1.0);
    let inner_path_rows = path.jpath.innerjoinpath.rows.max(1.0);
    let mut startup_cost = workspace.startup_cost;
    let mut run_cost = workspace.run_cost;

    path.jpath.path.disabled_nodes = workspace.disabled_nodes;
    path.jpath.path.rows = path
        .jpath
        .path
        .parent
        .as_ref()
        .map_or(workspace.outer_rows, |p| p.rows);

    // Number of tuples processed = full Cartesian product (inner join, no early stop).
    let ntuples = outer_path_rows * inner_path_rows;

    let restrict_qual_cost = cost_restrictinfo_eval(&path.jpath.joinrestrictinfo);
    startup_cost += restrict_qual_cost.startup;
    let cpu_per_tuple = DEFAULT_CPU_TUPLE_COST + restrict_qual_cost.per_tuple;
    run_cost = cpu_per_tuple.mul_add(ntuples, run_cost);

    let target_cost = path.jpath.path.pathtarget.as_ref().map_or(QualCost { startup: 0.0, per_tuple: 0.0 }, |t| t.cost);
    startup_cost += target_cost.startup;
    run_cost = target_cost.per_tuple.mul_add(path.jpath.path.rows, run_cost);

    path.jpath.path.startup_cost = startup_cost;
    path.jpath.path.total_cost = startup_cost + run_cost;
}

// ---------------------------------------------------------------------------
// Mergejoin cost
// ---------------------------------------------------------------------------

/// PG `initial_cost_mergejoin`: cheap lower bound on a mergejoin's cost -- the
/// (possibly Sorted) source-data scans of both inputs. M7 inner join uses the
/// clauseless selectivity branch (`mergejoinscansel` is selfuncs/step 31): scan
/// both inputs in full (startsel 0, endsel 1).
#[allow(clippy::too_many_arguments, reason = "1:1 PG initial_cost_mergejoin signature")]
pub fn initial_cost_mergejoin(
    root: &mut PlannerInfo,
    workspace: &mut JoinCostWorkspace,
    _jointype: JoinType,
    _mergeclauses: &[Box<crate::nodes::pathnodes::RestrictInfo>],
    outer_path: &Path,
    inner_path: &Path,
    outersortkeys: &[Box<crate::nodes::pathnodes::PathKey>],
    innersortkeys: &[Box<crate::nodes::pathnodes::PathKey>],
    _outer_presorted_keys: i32,
    _extra: &JoinPathExtraData,
) {
    let outer_path_rows = outer_path.rows.max(1.0);
    let inner_path_rows = inner_path.rows.max(1.0);

    // M7: mergejoinscansel (step 31) deferred -> scan both inputs in full.
    let (outerstartsel, outerendsel) = (0.0_f64, 1.0_f64);
    let (innerstartsel, innerendsel) = (0.0_f64, 1.0_f64);

    let outer_skip_rows = (outer_path_rows * outerstartsel).round();
    let inner_skip_rows = (inner_path_rows * innerstartsel).round();
    let outer_rows = clamp_row_est(outer_path_rows * outerendsel);
    let inner_rows = clamp_row_est(inner_path_rows * innerendsel);

    // enable_mergejoin defaults true.
    let mut disabled_nodes = 0;
    let mut startup_cost = 0.0;
    let mut run_cost = 0.0;

    // Outer side: sort if outersortkeys given.
    if outersortkeys.is_empty() {
        disabled_nodes += outer_path.disabled_nodes;
        startup_cost += outer_path.startup_cost;
        startup_cost = (outer_path.total_cost - outer_path.startup_cost).mul_add(outerstartsel, startup_cost);
        run_cost = (outer_path.total_cost - outer_path.startup_cost).mul_add(outerendsel - outerstartsel, run_cost);
    } else {
        let mut sort_path = dummy_path();
        let width = outer_path.pathtarget.as_ref().map_or(0, |t| t.width);
        cost_sort(&mut sort_path, root, &[], outer_path.disabled_nodes, outer_path.total_cost, outer_path_rows, width, 0.0, -1, -1.0);
        disabled_nodes += sort_path.disabled_nodes;
        startup_cost += sort_path.startup_cost;
        startup_cost = (sort_path.total_cost - sort_path.startup_cost).mul_add(outerstartsel, startup_cost);
        run_cost = (sort_path.total_cost - sort_path.startup_cost).mul_add(outerendsel - outerstartsel, run_cost);
    }

    // Inner side: sort if innersortkeys given. The inner run cost is held separately.
    let inner_run_cost = if innersortkeys.is_empty() {
        disabled_nodes += inner_path.disabled_nodes;
        startup_cost += inner_path.startup_cost;
        startup_cost = (inner_path.total_cost - inner_path.startup_cost).mul_add(innerstartsel, startup_cost);
        (inner_path.total_cost - inner_path.startup_cost) * (innerendsel - innerstartsel)
    } else {
        let mut sort_path = dummy_path();
        let width = inner_path.pathtarget.as_ref().map_or(0, |t| t.width);
        cost_sort(&mut sort_path, root, &[], inner_path.disabled_nodes, inner_path.total_cost, inner_path_rows, width, 0.0, -1, -1.0);
        disabled_nodes += sort_path.disabled_nodes;
        startup_cost += sort_path.startup_cost;
        startup_cost = (sort_path.total_cost - sort_path.startup_cost).mul_add(innerstartsel, startup_cost);
        (sort_path.total_cost - sort_path.startup_cost) * (innerendsel - innerstartsel)
    };

    workspace.disabled_nodes = disabled_nodes;
    workspace.startup_cost = startup_cost;
    workspace.total_cost = startup_cost + run_cost + inner_run_cost;
    workspace.run_cost = run_cost;
    workspace.inner_run_cost = inner_run_cost;
    workspace.outer_rows = outer_rows;
    workspace.inner_rows = inner_rows;
    workspace.outer_skip_rows = outer_skip_rows;
    workspace.inner_skip_rows = inner_skip_rows;
}

/// PG `final_cost_mergejoin`: finalize the mergejoin cost + row estimate. M7 inner
/// join: no mark/restore skip, materialize the inner if cheaper (mat_inner_cost).
/// The merge-qual / qp-qual CPU cost is charged over the scanned tuples.
pub fn final_cost_mergejoin(
    root: &mut PlannerInfo,
    path: &mut MergePath,
    workspace: &JoinCostWorkspace,
    _extra: &JoinPathExtraData,
) {
    let _ = root;
    let inner_path_rows = path.jpath.innerjoinpath.rows.max(1.0);
    let mut startup_cost = workspace.startup_cost;
    let mut run_cost = workspace.run_cost;
    let inner_run_cost = workspace.inner_run_cost;
    let outer_rows = workspace.outer_rows;
    let inner_rows = workspace.inner_rows;
    let outer_skip_rows = workspace.outer_skip_rows;
    let inner_skip_rows = workspace.inner_skip_rows;

    path.jpath.path.disabled_nodes = workspace.disabled_nodes;
    path.jpath.path.rows = path.jpath.path.parent.as_ref().map_or(outer_rows, |p| p.rows);

    let merge_qual_cost = cost_restrictinfo_eval(&path.path_mergeclauses);
    let mut qp_qual_cost = cost_restrictinfo_eval(&path.jpath.joinrestrictinfo);
    qp_qual_cost.startup -= merge_qual_cost.startup;
    qp_qual_cost.per_tuple -= merge_qual_cost.per_tuple;

    // M7: no SEMI/ANTI/inner_unique -> always mark/restore.
    path.skip_mark_restore = false;

    // Approx # tuples passing the mergequals = the join's row estimate (JOIN_INNER).
    let mergejointuples = path.jpath.path.rows;

    // Rescanned tuples = join size - inner size, clamped to >= 0.
    let rescannedtuples = (mergejointuples - inner_path_rows).max(0.0);
    let rescanratio = 1.0 + (rescannedtuples / inner_rows);

    let bare_inner_cost = inner_run_cost * rescanratio;
    let mat_inner_cost = (DEFAULT_CPU_OPERATOR_COST * inner_rows).mul_add(rescanratio, inner_run_cost);
    // enable_material defaults true: materialize the inner if cheaper.
    path.materialize_inner = mat_inner_cost < bare_inner_cost;
    run_cost += if path.materialize_inner { mat_inner_cost } else { bare_inner_cost };

    // Merge-qual comparisons over outer + inner (+ rescans).
    startup_cost += merge_qual_cost.startup;
    startup_cost = merge_qual_cost.per_tuple.mul_add(inner_skip_rows.mul_add(rescanratio, outer_skip_rows), startup_cost);
    run_cost = merge_qual_cost.per_tuple.mul_add((inner_rows - inner_skip_rows).mul_add(rescanratio, outer_rows - outer_skip_rows), run_cost);

    // Per output tuple: cpu_tuple_cost + the remaining restriction quals.
    startup_cost += qp_qual_cost.startup;
    let cpu_per_tuple = DEFAULT_CPU_TUPLE_COST + qp_qual_cost.per_tuple;
    run_cost = cpu_per_tuple.mul_add(mergejointuples, run_cost);

    let target_cost = path.jpath.path.pathtarget.as_ref().map_or(QualCost { startup: 0.0, per_tuple: 0.0 }, |t| t.cost);
    startup_cost += target_cost.startup;
    run_cost = target_cost.per_tuple.mul_add(path.jpath.path.rows, run_cost);

    path.jpath.path.startup_cost = startup_cost;
    path.jpath.path.total_cost = startup_cost + run_cost;
}

// ---------------------------------------------------------------------------
// Hashjoin cost
// ---------------------------------------------------------------------------

/// PG `initial_cost_hashjoin`: cheap lower bound -- scan the outer, build the hash
/// over the whole inner (one hash-fn cost per column per row + a tuple cost per inner
/// row to insert), probe with one hash-fn cost per outer row. M7 assumes a single
/// in-memory batch (no spill); the multi-batch I/O charge grows with the executor's
/// real `ExecChooseHashTableSize`.
#[allow(clippy::too_many_arguments, reason = "1:1 PG initial_cost_hashjoin signature")]
pub fn initial_cost_hashjoin(
    _root: &mut PlannerInfo,
    workspace: &mut JoinCostWorkspace,
    _jointype: JoinType,
    hashclauses: &[Box<crate::nodes::pathnodes::RestrictInfo>],
    outer_path: &Path,
    inner_path: &Path,
    _extra: &JoinPathExtraData,
    _parallel_hash: bool,
) {
    let outer_path_rows = outer_path.rows;
    let inner_path_rows = inner_path.rows;
    let num_hashclauses = hashclauses.len() as f64;

    // enable_hashjoin defaults true.
    let disabled_nodes = inner_path.disabled_nodes + outer_path.disabled_nodes;

    let mut startup_cost = outer_path.startup_cost;
    let mut run_cost = outer_path.total_cost - outer_path.startup_cost;
    startup_cost += inner_path.total_cost;

    // Hash build: per inner row, one operator cost per hashclause + a tuple cost.
    startup_cost = DEFAULT_CPU_OPERATOR_COST.mul_add(num_hashclauses, DEFAULT_CPU_TUPLE_COST).mul_add(inner_path_rows, startup_cost);
    // Probe: per outer row, one operator cost per hashclause.
    run_cost = (DEFAULT_CPU_OPERATOR_COST * num_hashclauses).mul_add(outer_path_rows, run_cost);

    // M7: single in-memory batch (numbatches = 1); the spill I/O charge is staged.
    workspace.disabled_nodes = disabled_nodes;
    workspace.startup_cost = startup_cost;
    workspace.total_cost = startup_cost + run_cost;
    workspace.run_cost = run_cost;
    workspace.numbuckets = 1024;
    workspace.numbatches = 1;
    workspace.inner_rows_total = inner_path_rows;
}

/// PG `final_cost_hashjoin`: finalize the hashjoin cost + row estimate. M7 inner
/// join uses a default inner bucket size (the real bucketsize is selfuncs/step 31):
/// the probe compares each outer row against the inner-bucket occupancy.
pub fn final_cost_hashjoin(
    root: &mut PlannerInfo,
    path: &mut HashPath,
    workspace: &JoinCostWorkspace,
    _extra: &JoinPathExtraData,
) {
    let _ = root;
    let outer_path_rows = path.jpath.outerjoinpath.rows;
    let inner_path_rows = path.jpath.innerjoinpath.rows;
    let numbuckets = f64::from(workspace.numbuckets);
    let numbatches = f64::from(workspace.numbatches);
    let mut startup_cost = workspace.startup_cost;
    let mut run_cost = workspace.run_cost;

    path.jpath.path.disabled_nodes = workspace.disabled_nodes;
    path.jpath.path.rows = path.jpath.path.parent.as_ref().map_or(outer_path_rows, |p| p.rows);
    path.num_batches = workspace.numbatches;
    path.inner_rows_total = workspace.inner_rows_total;

    let virtualbuckets = numbuckets * numbatches;
    // M7: default inner bucket fraction (1/virtualbuckets) -- the real per-clause
    // bucketsize estimate is selfuncs/step 31.
    let innerbucketsize = (1.0 / virtualbuckets).max(1e-9);

    let hash_qual_cost = cost_restrictinfo_eval(&path.path_hashclauses);
    let mut qp_qual_cost = cost_restrictinfo_eval(&path.jpath.joinrestrictinfo);
    qp_qual_cost.startup -= hash_qual_cost.startup;
    qp_qual_cost.per_tuple -= hash_qual_cost.per_tuple;

    // Probe cost: outer rows x typical bucket occupancy, halved (hash codes filter).
    startup_cost += hash_qual_cost.startup;
    run_cost = (hash_qual_cost.per_tuple * outer_path_rows * clamp_row_est(inner_path_rows * innerbucketsize)).mul_add(0.5, run_cost);

    let hashjointuples = path.jpath.path.rows;
    startup_cost += qp_qual_cost.startup;
    let cpu_per_tuple = DEFAULT_CPU_TUPLE_COST + qp_qual_cost.per_tuple;
    run_cost = cpu_per_tuple.mul_add(hashjointuples, run_cost);

    let target_cost = path.jpath.path.pathtarget.as_ref().map_or(QualCost { startup: 0.0, per_tuple: 0.0 }, |t| t.cost);
    startup_cost += target_cost.startup;
    run_cost = target_cost.per_tuple.mul_add(path.jpath.path.rows, run_cost);

    path.jpath.path.startup_cost = startup_cost;
    path.jpath.path.total_cost = startup_cost + run_cost;
}

// ---------------------------------------------------------------------------
// Join-rel size estimation
// ---------------------------------------------------------------------------

/// PG `calc_joinrel_size_estimate`: rows = Cartesian product x join selectivity.
/// M7 inner join only (the outer-join clamps grow with outer joins). FK-based
/// selectivity (step: FK matching) is skipped; the join clause selectivity comes
/// from `clauselist_selectivity` (clausesel.rs, the step-31-stubbed default).
fn calc_joinrel_size_estimate(
    root: &mut PlannerInfo,
    _joinrel: &RelOptInfo,
    outer_rows: f64,
    inner_rows: f64,
    sjinfo: &SpecialJoinInfo,
    restrictlist: &[Node],
) -> f64 {
    let jointype = sjinfo.jointype;
    if jointype != JoinType::INNER {
        not_yet_reachable("calc_joinrel_size_estimate: outer/semi/anti join size");
    }
    let jselec = crate::backend::optimizer::path::clausesel::clauselist_selectivity(
        root,
        restrictlist.to_vec(),
        0,
        jointype,
        Some(sjinfo),
    );
    clamp_row_est(outer_rows * inner_rows * jselec)
}

/// PG `set_joinrel_size_estimates`: set the joinrel's `rows` from the Cartesian
/// product of its inputs times the join-clause selectivity.
pub fn set_joinrel_size_estimates(
    root: &mut PlannerInfo,
    rel: &mut RelOptInfo,
    outer_rel: &RelOptInfo,
    inner_rel: &RelOptInfo,
    sjinfo: &SpecialJoinInfo,
    restrictlist: &[Node],
) {
    let rows = calc_joinrel_size_estimate(root, rel, outer_rel.rows, inner_rel.rows, sjinfo, restrictlist);
    rel.rows = rows;
}

/// A zeroed `Path` for use as the dummy result of `cost_sort` (PG's `sort_path`).
fn dummy_path() -> Path {
    Path {
        pathtype: PathType::Sort,
        parent: None,
        pathtarget: None,
        param_info: None,
        parallel_aware: false,
        parallel_safe: false,
        parallel_workers: 0,
        rows: 0.0,
        disabled_nodes: 0,
        startup_cost: 0.0,
        total_cost: 0.0,
        pathkeys: Vec::new(),
        index_detail: None,
        join_detail: None,
    }
}

#[cold]
fn not_yet_reachable(what: &str) -> ! {
    unimplemented!("{what}: not yet translated for this milestone");
}
