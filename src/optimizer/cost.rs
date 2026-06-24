//! Translated from PostgreSQL src/include/optimizer/cost.h
//! prototypes for costsize.c and clausesel.c.

use crate::nodes::nodes::{Cost, JoinType, Node, Selectivity};
use crate::nodes::nodes::{AggStrategy};
use crate::nodes::parsenodes::WindowClause;
use crate::nodes::pathnodes::{
    AggClauseCosts, AppendPath, BitmapAndPath, BitmapOrPath, GatherMergePath, GatherPath, HashPath,
    IndexPath, JoinCostWorkspace, JoinPathExtraData, MergePath, NestPath, ParamPathInfo, Path,
    PathTarget, PlannerInfo, QualCost, RelOptInfo, Relids, SemiAntiJoinFactors, SpecialJoinInfo,
};
use crate::nodes::plannodes::Plan;
use crate::nodes::primnodes::SubPlan;
use crate::storage::block::BlockNumber;

/* defaults for costsize.c's Cost parameters */
pub const DEFAULT_SEQ_PAGE_COST: f64 = 1.0;
pub const DEFAULT_RANDOM_PAGE_COST: f64 = 4.0;
pub const DEFAULT_CPU_TUPLE_COST: f64 = 0.01;
pub const DEFAULT_CPU_INDEX_TUPLE_COST: f64 = 0.005;
pub const DEFAULT_CPU_OPERATOR_COST: f64 = 0.0025;
pub const DEFAULT_PARALLEL_TUPLE_COST: f64 = 0.1;
pub const DEFAULT_PARALLEL_SETUP_COST: f64 = 1000.0;

/* defaults for non-Cost parameters */
pub const DEFAULT_RECURSIVE_WORKTABLE_FACTOR: f64 = 10.0;
/// measured in pages
pub const DEFAULT_EFFECTIVE_CACHE_SIZE: i32 = 524288;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConstraintExclusionType {
    /// do not use c_e
    Off,
    /// apply c_e to all rels
    On,
    /// apply c_e to otherrels only
    Partition,
}

/* parameter variables and flags (see also optimizer.h) */
// TODO(global): GUC/session state, currently process-global statics.
pub static mut disable_cost: Cost = 0.0;
pub static mut max_parallel_workers_per_gather: i32 = 0;
pub static mut enable_seqscan: bool = true;
pub static mut enable_indexscan: bool = true;
pub static mut enable_indexonlyscan: bool = true;
pub static mut enable_bitmapscan: bool = true;
pub static mut enable_tidscan: bool = true;
pub static mut enable_sort: bool = true;
pub static mut enable_incremental_sort: bool = true;
pub static mut enable_hashagg: bool = true;
pub static mut enable_nestloop: bool = true;
pub static mut enable_material: bool = true;
pub static mut enable_memoize: bool = true;
pub static mut enable_mergejoin: bool = true;
pub static mut enable_hashjoin: bool = true;
pub static mut enable_gathermerge: bool = true;
pub static mut enable_partitionwise_join: bool = false;
pub static mut enable_partitionwise_aggregate: bool = false;
pub static mut enable_parallel_append: bool = true;
pub static mut enable_parallel_hash: bool = true;
pub static mut enable_partition_pruning: bool = true;
pub static mut enable_presorted_aggregate: bool = true;
pub static mut enable_async_append: bool = true;
pub static mut constraint_exclusion: i32 = 0;

pub fn index_pages_fetched(
    tuples_fetched: f64,
    pages: BlockNumber,
    index_pages: f64,
    root: &mut PlannerInfo,
) -> f64 {
    unimplemented!()
}

pub fn cost_seqscan(
    path: &mut Path,
    root: &mut PlannerInfo,
    baserel: &RelOptInfo,
    param_info: Option<&ParamPathInfo>,
) {
    unimplemented!()
}

pub fn cost_samplescan(
    path: &mut Path,
    root: &mut PlannerInfo,
    baserel: &RelOptInfo,
    param_info: Option<&ParamPathInfo>,
) {
    unimplemented!()
}

pub fn cost_index(path: &mut IndexPath, root: &mut PlannerInfo, loop_count: f64, partial_path: bool) {
    unimplemented!()
}

pub fn cost_bitmap_heap_scan(
    path: &mut Path,
    root: &mut PlannerInfo,
    baserel: &RelOptInfo,
    param_info: Option<&ParamPathInfo>,
    bitmapqual: &Path,
    loop_count: f64,
) {
    unimplemented!()
}

pub fn cost_bitmap_and_node(path: &mut BitmapAndPath, root: &mut PlannerInfo) {
    unimplemented!()
}

pub fn cost_bitmap_or_node(path: &mut BitmapOrPath, root: &mut PlannerInfo) {
    unimplemented!()
}

/// C out-params `cost`, `selec` -> returned tuple.
pub fn cost_bitmap_tree_node(path: &Path) -> (Cost, Selectivity) {
    unimplemented!()
}

pub fn cost_tidscan(
    path: &mut Path,
    root: &mut PlannerInfo,
    baserel: &RelOptInfo,
    tidquals: &[Box<Node>],
    param_info: Option<&ParamPathInfo>,
) {
    unimplemented!()
}

pub fn cost_tidrangescan(
    path: &mut Path,
    root: &mut PlannerInfo,
    baserel: &RelOptInfo,
    tidrangequals: &[Box<Node>],
    param_info: Option<&ParamPathInfo>,
) {
    unimplemented!()
}

pub fn cost_subqueryscan(
    path: &mut Path,
    root: &mut PlannerInfo,
    baserel: &RelOptInfo,
    param_info: Option<&ParamPathInfo>,
    trivial_pathtarget: bool,
) {
    unimplemented!()
}

pub fn cost_functionscan(
    path: &mut Path,
    root: &mut PlannerInfo,
    baserel: &RelOptInfo,
    param_info: Option<&ParamPathInfo>,
) {
    unimplemented!()
}

pub fn cost_valuesscan(
    path: &mut Path,
    root: &mut PlannerInfo,
    baserel: &RelOptInfo,
    param_info: Option<&ParamPathInfo>,
) {
    unimplemented!()
}

pub fn cost_tablefuncscan(
    path: &mut Path,
    root: &mut PlannerInfo,
    baserel: &RelOptInfo,
    param_info: Option<&ParamPathInfo>,
) {
    unimplemented!()
}

pub fn cost_ctescan(
    path: &mut Path,
    root: &mut PlannerInfo,
    baserel: &RelOptInfo,
    param_info: Option<&ParamPathInfo>,
) {
    unimplemented!()
}

pub fn cost_namedtuplestorescan(
    path: &mut Path,
    root: &mut PlannerInfo,
    baserel: &RelOptInfo,
    param_info: Option<&ParamPathInfo>,
) {
    unimplemented!()
}

pub fn cost_resultscan(
    path: &mut Path,
    root: &mut PlannerInfo,
    baserel: &RelOptInfo,
    param_info: Option<&ParamPathInfo>,
) {
    unimplemented!()
}

pub fn cost_recursive_union(runion: &mut Path, nrterm: &Path, rterm: &Path) {
    unimplemented!()
}

pub fn cost_sort(
    path: &mut Path,
    root: &mut PlannerInfo,
    pathkeys: &[Box<Node>],
    input_disabled_nodes: i32,
    input_cost: Cost,
    tuples: f64,
    width: i32,
    comparison_cost: Cost,
    sort_mem: i32,
    limit_tuples: f64,
) {
    unimplemented!()
}

pub fn cost_incremental_sort(
    path: &mut Path,
    root: &mut PlannerInfo,
    pathkeys: &[Box<Node>],
    presorted_keys: i32,
    input_disabled_nodes: i32,
    input_startup_cost: Cost,
    input_total_cost: Cost,
    input_tuples: f64,
    width: i32,
    comparison_cost: Cost,
    sort_mem: i32,
    limit_tuples: f64,
) {
    unimplemented!()
}

pub fn cost_append(apath: &mut AppendPath) {
    unimplemented!()
}

pub fn cost_merge_append(
    path: &mut Path,
    root: &mut PlannerInfo,
    pathkeys: &[Box<Node>],
    n_streams: i32,
    input_disabled_nodes: i32,
    input_startup_cost: Cost,
    input_total_cost: Cost,
    tuples: f64,
) {
    unimplemented!()
}

pub fn cost_material(
    path: &mut Path,
    input_disabled_nodes: i32,
    input_startup_cost: Cost,
    input_total_cost: Cost,
    tuples: f64,
    width: i32,
) {
    unimplemented!()
}

pub fn cost_agg(
    path: &mut Path,
    root: &mut PlannerInfo,
    aggstrategy: AggStrategy,
    aggcosts: Option<&AggClauseCosts>,
    num_group_cols: i32,
    num_groups: f64,
    quals: &[Box<Node>],
    disabled_nodes: i32,
    input_startup_cost: Cost,
    input_total_cost: Cost,
    input_tuples: f64,
    input_width: f64,
) {
    unimplemented!()
}

pub fn cost_windowagg(
    path: &mut Path,
    root: &mut PlannerInfo,
    window_funcs: &[Box<Node>],
    winclause: &WindowClause,
    input_disabled_nodes: i32,
    input_startup_cost: Cost,
    input_total_cost: Cost,
    input_tuples: f64,
) {
    unimplemented!()
}

pub fn cost_group(
    path: &mut Path,
    root: &mut PlannerInfo,
    num_group_cols: i32,
    num_groups: f64,
    quals: &[Box<Node>],
    input_disabled_nodes: i32,
    input_startup_cost: Cost,
    input_total_cost: Cost,
    input_tuples: f64,
) {
    unimplemented!()
}

pub fn initial_cost_nestloop(
    root: &mut PlannerInfo,
    workspace: &mut JoinCostWorkspace,
    jointype: JoinType,
    outer_path: &Path,
    inner_path: &Path,
    extra: &JoinPathExtraData,
) {
    unimplemented!()
}

pub fn final_cost_nestloop(
    root: &mut PlannerInfo,
    path: &mut NestPath,
    workspace: &JoinCostWorkspace,
    extra: &JoinPathExtraData,
) {
    unimplemented!()
}

pub fn initial_cost_mergejoin(
    root: &mut PlannerInfo,
    workspace: &mut JoinCostWorkspace,
    jointype: JoinType,
    mergeclauses: &[Box<Node>],
    outer_path: &Path,
    inner_path: &Path,
    outersortkeys: &[Box<Node>],
    innersortkeys: &[Box<Node>],
    outer_presorted_keys: i32,
    extra: &JoinPathExtraData,
) {
    unimplemented!()
}

pub fn final_cost_mergejoin(
    root: &mut PlannerInfo,
    path: &mut MergePath,
    workspace: &JoinCostWorkspace,
    extra: &JoinPathExtraData,
) {
    unimplemented!()
}

pub fn initial_cost_hashjoin(
    root: &mut PlannerInfo,
    workspace: &mut JoinCostWorkspace,
    jointype: JoinType,
    hashclauses: &[Box<Node>],
    outer_path: &Path,
    inner_path: &Path,
    extra: &JoinPathExtraData,
    parallel_hash: bool,
) {
    unimplemented!()
}

pub fn final_cost_hashjoin(
    root: &mut PlannerInfo,
    path: &mut HashPath,
    workspace: &JoinCostWorkspace,
    extra: &JoinPathExtraData,
) {
    unimplemented!()
}

/// C out-param `rows` -> returned f64.
pub fn cost_gather(
    path: &mut GatherPath,
    root: &mut PlannerInfo,
    rel: &RelOptInfo,
    param_info: Option<&ParamPathInfo>,
) -> f64 {
    unimplemented!()
}

/// C out-param `rows` -> returned f64.
pub fn cost_gather_merge(
    path: &mut GatherMergePath,
    root: &mut PlannerInfo,
    rel: &RelOptInfo,
    param_info: Option<&ParamPathInfo>,
    input_disabled_nodes: i32,
    input_startup_cost: Cost,
    input_total_cost: Cost,
) -> f64 {
    unimplemented!()
}

pub fn cost_subplan(root: &mut PlannerInfo, subplan: &mut SubPlan, plan: &Plan) {
    unimplemented!()
}

/// C out-param `cost` -> returned QualCost.
pub fn cost_qual_eval(quals: &[Box<Node>], root: &mut PlannerInfo) -> QualCost {
    unimplemented!()
}

/// C out-param `cost` -> returned QualCost.
pub fn cost_qual_eval_node(qual: Option<&Node>, root: &mut PlannerInfo) -> QualCost {
    unimplemented!()
}

/// C out-param `semifactors` -> returned struct.
pub fn compute_semi_anti_join_factors(
    root: &mut PlannerInfo,
    joinrel: &RelOptInfo,
    outerrel: &RelOptInfo,
    innerrel: &RelOptInfo,
    jointype: JoinType,
    sjinfo: &SpecialJoinInfo,
    restrictlist: &[Box<Node>],
) -> SemiAntiJoinFactors {
    unimplemented!()
}

pub fn set_baserel_size_estimates(root: &mut PlannerInfo, rel: &mut RelOptInfo) {
    unimplemented!()
}

pub fn get_parameterized_baserel_size(
    root: &mut PlannerInfo,
    rel: &RelOptInfo,
    param_clauses: &[Box<Node>],
) -> f64 {
    unimplemented!()
}

pub fn get_parameterized_joinrel_size(
    root: &mut PlannerInfo,
    rel: &RelOptInfo,
    outer_path: &Path,
    inner_path: &Path,
    sjinfo: &SpecialJoinInfo,
    restrict_clauses: &[Box<Node>],
) -> f64 {
    unimplemented!()
}

pub fn set_joinrel_size_estimates(
    root: &mut PlannerInfo,
    rel: &mut RelOptInfo,
    outer_rel: &RelOptInfo,
    inner_rel: &RelOptInfo,
    sjinfo: &SpecialJoinInfo,
    restrictlist: &[Box<Node>],
) {
    unimplemented!()
}

pub fn set_subquery_size_estimates(root: &mut PlannerInfo, rel: &mut RelOptInfo) {
    unimplemented!()
}

pub fn set_function_size_estimates(root: &mut PlannerInfo, rel: &mut RelOptInfo) {
    unimplemented!()
}

pub fn set_values_size_estimates(root: &mut PlannerInfo, rel: &mut RelOptInfo) {
    unimplemented!()
}

pub fn set_cte_size_estimates(root: &mut PlannerInfo, rel: &mut RelOptInfo, cte_rows: f64) {
    unimplemented!()
}

pub fn set_tablefunc_size_estimates(root: &mut PlannerInfo, rel: &mut RelOptInfo) {
    unimplemented!()
}

pub fn set_namedtuplestore_size_estimates(root: &mut PlannerInfo, rel: &mut RelOptInfo) {
    unimplemented!()
}

pub fn set_result_size_estimates(root: &mut PlannerInfo, rel: &mut RelOptInfo) {
    unimplemented!()
}

pub fn set_foreign_size_estimates(root: &mut PlannerInfo, rel: &mut RelOptInfo) {
    unimplemented!()
}

pub fn set_pathtarget_cost_width(root: &mut PlannerInfo, target: &mut PathTarget) -> Box<PathTarget> {
    unimplemented!()
}

/// C out-params `cost_p`, `tuples_p` -> returned tuple (pages, cost, tuples).
pub fn compute_bitmap_pages(
    root: &mut PlannerInfo,
    baserel: &RelOptInfo,
    bitmapqual: &Path,
    loop_count: f64,
) -> (f64, Cost, f64) {
    unimplemented!()
}

pub fn compute_gather_rows(path: &Path) -> f64 {
    unimplemented!()
}
