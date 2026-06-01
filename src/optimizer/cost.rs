//! cost.h - prototypes for costsize.c and clausesel.c.

use std::ffi::c_int;

use crate::nodes::nodes::{Cost, Selectivity};
use crate::nodes::nodes::Node;
use crate::nodes::pg_list::List;
use crate::nodes::pathnodes::{
    AggClauseCosts, AppendPath, BitmapAndPath, BitmapOrPath, BlockNumber, GatherMergePath,
    GatherPath, HashPath, IndexPath, JoinCostWorkspace, JoinPathExtraData, MergePath, NestPath,
    ParamPathInfo, Path, PathTarget, PlannerInfo, QualCost, RelOptInfo, SemiAntiJoinFactors,
    SpecialJoinInfo, SubqueryScanPath,
};
use crate::nodes::parsenodes::WindowClause;
use crate::nodes::plannodes::Plan;
use crate::nodes::primnodes::SubPlan;

// TODO: dedup when nodes.h / plannodes.h projections land.
pub type JoinType = c_int;
pub type AggStrategy = c_int;

/* defaults for costsize.c's Cost parameters */
/* NB: cost-estimation code should use the variables, not these constants! */
/* If you change these, update backend/utils/misc/postgresql.conf.sample */
pub const DEFAULT_SEQ_PAGE_COST: f64 = 1.0;
pub const DEFAULT_RANDOM_PAGE_COST: f64 = 4.0;
pub const DEFAULT_CPU_TUPLE_COST: f64 = 0.01;
pub const DEFAULT_CPU_INDEX_TUPLE_COST: f64 = 0.005;
pub const DEFAULT_CPU_OPERATOR_COST: f64 = 0.0025;
pub const DEFAULT_PARALLEL_TUPLE_COST: f64 = 0.1;
pub const DEFAULT_PARALLEL_SETUP_COST: f64 = 1000.0;

/* defaults for non-Cost parameters */
pub const DEFAULT_RECURSIVE_WORKTABLE_FACTOR: f64 = 10.0;
pub const DEFAULT_EFFECTIVE_CACHE_SIZE: c_int = 524288; /* measured in pages */

/* ConstraintExclusionType enum (project convention: c_int + consts) */
pub type ConstraintExclusionType = c_int;
pub const CONSTRAINT_EXCLUSION_OFF: ConstraintExclusionType = 0; /* do not use c_e */
pub const CONSTRAINT_EXCLUSION_ON: ConstraintExclusionType = 1; /* apply c_e to all rels */
pub const CONSTRAINT_EXCLUSION_PARTITION: ConstraintExclusionType = 2; /* apply c_e to otherrels only */

/*
 * prototypes for costsize.c
 *	  routines to compute costs and sizes
 */

/* parameter variables and flags (see also optimizer.h) */
pub static mut disable_cost: Cost = 0.0;
pub static mut max_parallel_workers_per_gather: c_int = 0;
pub static mut enable_seqscan: bool = false;
pub static mut enable_indexscan: bool = false;
pub static mut enable_indexonlyscan: bool = false;
pub static mut enable_bitmapscan: bool = false;
pub static mut enable_tidscan: bool = false;
pub static mut enable_sort: bool = false;
pub static mut enable_incremental_sort: bool = false;
pub static mut enable_hashagg: bool = false;
pub static mut enable_nestloop: bool = false;
pub static mut enable_material: bool = false;
pub static mut enable_memoize: bool = false;
pub static mut enable_mergejoin: bool = false;
pub static mut enable_hashjoin: bool = false;
pub static mut enable_gathermerge: bool = false;
pub static mut enable_partitionwise_join: bool = false;
pub static mut enable_partitionwise_aggregate: bool = false;
pub static mut enable_parallel_append: bool = false;
pub static mut enable_parallel_hash: bool = false;
pub static mut enable_partition_pruning: bool = false;
pub static mut enable_presorted_aggregate: bool = false;
pub static mut enable_async_append: bool = false;
pub static mut constraint_exclusion: c_int = 0;

pub unsafe fn index_pages_fetched(
    tuples_fetched: f64,
    pages: BlockNumber,
    index_pages: f64,
    root: *mut PlannerInfo,
) -> f64 {
    unimplemented!()
}

pub unsafe fn cost_seqscan(
    path: *mut Path,
    root: *mut PlannerInfo,
    baserel: *mut RelOptInfo,
    param_info: *mut ParamPathInfo,
) {
    unimplemented!()
}

pub unsafe fn cost_samplescan(
    path: *mut Path,
    root: *mut PlannerInfo,
    baserel: *mut RelOptInfo,
    param_info: *mut ParamPathInfo,
) {
    unimplemented!()
}

pub unsafe fn cost_index(
    path: *mut IndexPath,
    root: *mut PlannerInfo,
    loop_count: f64,
    partial_path: bool,
) {
    unimplemented!()
}

pub unsafe fn cost_bitmap_heap_scan(
    path: *mut Path,
    root: *mut PlannerInfo,
    baserel: *mut RelOptInfo,
    param_info: *mut ParamPathInfo,
    bitmapqual: *mut Path,
    loop_count: f64,
) {
    unimplemented!()
}

pub unsafe fn cost_bitmap_and_node(path: *mut BitmapAndPath, root: *mut PlannerInfo) {
    unimplemented!()
}

pub unsafe fn cost_bitmap_or_node(path: *mut BitmapOrPath, root: *mut PlannerInfo) {
    unimplemented!()
}

pub unsafe fn cost_bitmap_tree_node(
    path: *mut Path,
    cost: *mut Cost,
    selec: *mut Selectivity,
) {
    unimplemented!()
}

pub unsafe fn cost_tidscan(
    path: *mut Path,
    root: *mut PlannerInfo,
    baserel: *mut RelOptInfo,
    tidquals: *mut List,
    param_info: *mut ParamPathInfo,
) {
    unimplemented!()
}

pub unsafe fn cost_tidrangescan(
    path: *mut Path,
    root: *mut PlannerInfo,
    baserel: *mut RelOptInfo,
    tidrangequals: *mut List,
    param_info: *mut ParamPathInfo,
) {
    unimplemented!()
}

pub unsafe fn cost_subqueryscan(
    path: *mut SubqueryScanPath,
    root: *mut PlannerInfo,
    baserel: *mut RelOptInfo,
    param_info: *mut ParamPathInfo,
    trivial_pathtarget: bool,
) {
    unimplemented!()
}

pub unsafe fn cost_functionscan(
    path: *mut Path,
    root: *mut PlannerInfo,
    baserel: *mut RelOptInfo,
    param_info: *mut ParamPathInfo,
) {
    unimplemented!()
}

pub unsafe fn cost_valuesscan(
    path: *mut Path,
    root: *mut PlannerInfo,
    baserel: *mut RelOptInfo,
    param_info: *mut ParamPathInfo,
) {
    unimplemented!()
}

pub unsafe fn cost_tablefuncscan(
    path: *mut Path,
    root: *mut PlannerInfo,
    baserel: *mut RelOptInfo,
    param_info: *mut ParamPathInfo,
) {
    unimplemented!()
}

pub unsafe fn cost_ctescan(
    path: *mut Path,
    root: *mut PlannerInfo,
    baserel: *mut RelOptInfo,
    param_info: *mut ParamPathInfo,
) {
    unimplemented!()
}

pub unsafe fn cost_namedtuplestorescan(
    path: *mut Path,
    root: *mut PlannerInfo,
    baserel: *mut RelOptInfo,
    param_info: *mut ParamPathInfo,
) {
    unimplemented!()
}

pub unsafe fn cost_resultscan(
    path: *mut Path,
    root: *mut PlannerInfo,
    baserel: *mut RelOptInfo,
    param_info: *mut ParamPathInfo,
) {
    unimplemented!()
}

pub unsafe fn cost_recursive_union(runion: *mut Path, nrterm: *mut Path, rterm: *mut Path) {
    unimplemented!()
}

pub unsafe fn cost_sort(
    path: *mut Path,
    root: *mut PlannerInfo,
    pathkeys: *mut List,
    input_disabled_nodes: c_int,
    input_cost: Cost,
    tuples: f64,
    width: c_int,
    comparison_cost: Cost,
    sort_mem: c_int,
    limit_tuples: f64,
) {
    unimplemented!()
}

pub unsafe fn cost_incremental_sort(
    path: *mut Path,
    root: *mut PlannerInfo,
    pathkeys: *mut List,
    presorted_keys: c_int,
    input_disabled_nodes: c_int,
    input_startup_cost: Cost,
    input_total_cost: Cost,
    input_tuples: f64,
    width: c_int,
    comparison_cost: Cost,
    sort_mem: c_int,
    limit_tuples: f64,
) {
    unimplemented!()
}

pub unsafe fn cost_append(apath: *mut AppendPath) {
    unimplemented!()
}

pub unsafe fn cost_merge_append(
    path: *mut Path,
    root: *mut PlannerInfo,
    pathkeys: *mut List,
    n_streams: c_int,
    input_disabled_nodes: c_int,
    input_startup_cost: Cost,
    input_total_cost: Cost,
    tuples: f64,
) {
    unimplemented!()
}

pub unsafe fn cost_material(
    path: *mut Path,
    input_disabled_nodes: c_int,
    input_startup_cost: Cost,
    input_total_cost: Cost,
    tuples: f64,
    width: c_int,
) {
    unimplemented!()
}

pub unsafe fn cost_agg(
    path: *mut Path,
    root: *mut PlannerInfo,
    aggstrategy: AggStrategy,
    aggcosts: *const AggClauseCosts,
    numGroupCols: c_int,
    numGroups: f64,
    quals: *mut List,
    disabled_nodes: c_int,
    input_startup_cost: Cost,
    input_total_cost: Cost,
    input_tuples: f64,
    input_width: f64,
) {
    unimplemented!()
}

pub unsafe fn cost_windowagg(
    path: *mut Path,
    root: *mut PlannerInfo,
    windowFuncs: *mut List,
    winclause: *mut WindowClause,
    input_disabled_nodes: c_int,
    input_startup_cost: Cost,
    input_total_cost: Cost,
    input_tuples: f64,
) {
    unimplemented!()
}

pub unsafe fn cost_group(
    path: *mut Path,
    root: *mut PlannerInfo,
    numGroupCols: c_int,
    numGroups: f64,
    quals: *mut List,
    input_disabled_nodes: c_int,
    input_startup_cost: Cost,
    input_total_cost: Cost,
    input_tuples: f64,
) {
    unimplemented!()
}

pub unsafe fn initial_cost_nestloop(
    root: *mut PlannerInfo,
    workspace: *mut JoinCostWorkspace,
    jointype: JoinType,
    outer_path: *mut Path,
    inner_path: *mut Path,
    extra: *mut JoinPathExtraData,
) {
    unimplemented!()
}

pub unsafe fn final_cost_nestloop(
    root: *mut PlannerInfo,
    path: *mut NestPath,
    workspace: *mut JoinCostWorkspace,
    extra: *mut JoinPathExtraData,
) {
    unimplemented!()
}

pub unsafe fn initial_cost_mergejoin(
    root: *mut PlannerInfo,
    workspace: *mut JoinCostWorkspace,
    jointype: JoinType,
    mergeclauses: *mut List,
    outer_path: *mut Path,
    inner_path: *mut Path,
    outersortkeys: *mut List,
    innersortkeys: *mut List,
    outer_presorted_keys: c_int,
    extra: *mut JoinPathExtraData,
) {
    unimplemented!()
}

pub unsafe fn final_cost_mergejoin(
    root: *mut PlannerInfo,
    path: *mut MergePath,
    workspace: *mut JoinCostWorkspace,
    extra: *mut JoinPathExtraData,
) {
    unimplemented!()
}

pub unsafe fn initial_cost_hashjoin(
    root: *mut PlannerInfo,
    workspace: *mut JoinCostWorkspace,
    jointype: JoinType,
    hashclauses: *mut List,
    outer_path: *mut Path,
    inner_path: *mut Path,
    extra: *mut JoinPathExtraData,
    parallel_hash: bool,
) {
    unimplemented!()
}

pub unsafe fn final_cost_hashjoin(
    root: *mut PlannerInfo,
    path: *mut HashPath,
    workspace: *mut JoinCostWorkspace,
    extra: *mut JoinPathExtraData,
) {
    unimplemented!()
}

pub unsafe fn cost_gather(
    path: *mut GatherPath,
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    param_info: *mut ParamPathInfo,
    rows: *mut f64,
) {
    unimplemented!()
}

pub unsafe fn cost_gather_merge(
    path: *mut GatherMergePath,
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    param_info: *mut ParamPathInfo,
    input_disabled_nodes: c_int,
    input_startup_cost: Cost,
    input_total_cost: Cost,
    rows: *mut f64,
) {
    unimplemented!()
}

pub unsafe fn cost_subplan(root: *mut PlannerInfo, subplan: *mut SubPlan, plan: *mut Plan) {
    unimplemented!()
}

pub unsafe fn cost_qual_eval(cost: *mut QualCost, quals: *mut List, root: *mut PlannerInfo) {
    unimplemented!()
}

pub unsafe fn cost_qual_eval_node(cost: *mut QualCost, qual: *mut Node, root: *mut PlannerInfo) {
    unimplemented!()
}

pub unsafe fn compute_semi_anti_join_factors(
    root: *mut PlannerInfo,
    joinrel: *mut RelOptInfo,
    outerrel: *mut RelOptInfo,
    innerrel: *mut RelOptInfo,
    jointype: JoinType,
    sjinfo: *mut SpecialJoinInfo,
    restrictlist: *mut List,
    semifactors: *mut SemiAntiJoinFactors,
) {
    unimplemented!()
}

pub unsafe fn set_baserel_size_estimates(root: *mut PlannerInfo, rel: *mut RelOptInfo) {
    unimplemented!()
}

pub unsafe fn get_parameterized_baserel_size(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    param_clauses: *mut List,
) -> f64 {
    unimplemented!()
}

pub unsafe fn get_parameterized_joinrel_size(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    outer_path: *mut Path,
    inner_path: *mut Path,
    sjinfo: *mut SpecialJoinInfo,
    restrict_clauses: *mut List,
) -> f64 {
    unimplemented!()
}

pub unsafe fn set_joinrel_size_estimates(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    outer_rel: *mut RelOptInfo,
    inner_rel: *mut RelOptInfo,
    sjinfo: *mut SpecialJoinInfo,
    restrictlist: *mut List,
) {
    unimplemented!()
}

pub unsafe fn set_subquery_size_estimates(root: *mut PlannerInfo, rel: *mut RelOptInfo) {
    unimplemented!()
}

pub unsafe fn set_function_size_estimates(root: *mut PlannerInfo, rel: *mut RelOptInfo) {
    unimplemented!()
}

pub unsafe fn set_values_size_estimates(root: *mut PlannerInfo, rel: *mut RelOptInfo) {
    unimplemented!()
}

pub unsafe fn set_cte_size_estimates(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    cte_rows: f64,
) {
    unimplemented!()
}

pub unsafe fn set_tablefunc_size_estimates(root: *mut PlannerInfo, rel: *mut RelOptInfo) {
    unimplemented!()
}

pub unsafe fn set_namedtuplestore_size_estimates(root: *mut PlannerInfo, rel: *mut RelOptInfo) {
    unimplemented!()
}

pub unsafe fn set_result_size_estimates(root: *mut PlannerInfo, rel: *mut RelOptInfo) {
    unimplemented!()
}

pub unsafe fn set_foreign_size_estimates(root: *mut PlannerInfo, rel: *mut RelOptInfo) {
    unimplemented!()
}

pub unsafe fn set_pathtarget_cost_width(
    root: *mut PlannerInfo,
    target: *mut PathTarget,
) -> *mut PathTarget {
    unimplemented!()
}

pub unsafe fn compute_bitmap_pages(
    root: *mut PlannerInfo,
    baserel: *mut RelOptInfo,
    bitmapqual: *mut Path,
    loop_count: f64,
    cost_p: *mut Cost,
    tuples_p: *mut f64,
) -> f64 {
    unimplemented!()
}

pub unsafe fn compute_gather_rows(path: *mut Path) -> f64 {
    unimplemented!()
}
