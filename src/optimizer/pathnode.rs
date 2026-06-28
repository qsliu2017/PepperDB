//! Translated from PostgreSQL src/include/optimizer/pathnode.h
//! prototypes for pathnode.c, relnode.c.

#![allow(clippy::boxed_local, reason = "1:1 PG port: Node/Box<Path> mirrors PG pointer-passed nodes")]
#![allow(clippy::needless_pass_by_value, reason = "1:1 PG port: stubs take owned node values matching PG C signatures; consumed once implemented")]

use crate::access::cmptype::CompareType;
use crate::access::sdir::ScanDirection;
use crate::nodes::bitmapset::Bitmapset;
use crate::nodes::nodes::{
    AggSplit, AggStrategy, CmdType, JoinType, Node, SetOpCmd, SetOpStrategy,
};
use crate::nodes::nodes::{Cost, LimitOption};
use crate::nodes::pathnodes::{
    AggClauseCosts, AggPath, AppendPath, AppendRelInfo, BitmapAndPath, BitmapHeapPath, BitmapOrPath,
    CostSelector, UpperRelationKind,
    ForeignPath, GatherMergePath, GatherPath, GroupPath, GroupResultPath, GroupingSetsPath,
    HashPath, IncrementalSortPath, IndexOptInfo, IndexPath, JoinCostWorkspace, JoinPathExtraData,
    LimitPath, LockRowsPath, MaterialPath, MemoizePath, MergeAppendPath, MergePath, MinMaxAggPath,
    ModifyTablePath, NestPath, ParamPathInfo, Path, PathTarget, PlannerInfo, ProjectSetPath,
    ProjectionPath, RecursiveUnionPath, RelOptInfo, Relids, RecursiveUnionPath as _RU, SetOpPath,
    SortPath, SpecialJoinInfo, SubqueryScanPath, TidPath, TidRangePath, UniquePath, UpperUniquePath,
    WindowAggPath,
};
use crate::nodes::primnodes::OnConflictExpr;
use crate::nodes::parsenodes::WindowClause;

/*
 * prototypes for pathnode.c
 */
pub fn compare_path_costs(path1: &Path, path2: &Path, criterion: CostSelector) -> i32 {
    unimplemented!()
}

pub fn compare_fractional_path_costs(path1: &Path, path2: &Path, fraction: f64) -> i32 {
    unimplemented!()
}

/// PG `set_cheapest`. See `crate::backend::optimizer::util::pathnode`.
pub use crate::backend::optimizer::util::pathnode::set_cheapest;

/// PG `add_path`. See `crate::backend::optimizer::util::pathnode`.
pub use crate::backend::optimizer::util::pathnode::add_path;

pub fn add_path_precheck(
    parent_rel: &RelOptInfo,
    disabled_nodes: i32,
    startup_cost: Cost,
    total_cost: Cost,
    pathkeys: &[Node],
    required_outer: Relids,
) -> bool {
    unimplemented!()
}

pub fn add_partial_path(parent_rel: &mut RelOptInfo, new_path: Box<Path>) {
    unimplemented!()
}

pub fn add_partial_path_precheck(
    parent_rel: &RelOptInfo,
    disabled_nodes: i32,
    total_cost: Cost,
    pathkeys: &[Node],
) -> bool {
    unimplemented!()
}

pub fn create_seqscan_path(
    root: &mut PlannerInfo,
    rel: &RelOptInfo,
    required_outer: Relids,
    parallel_workers: i32,
) -> Box<Path> {
    unimplemented!()
}

pub fn create_samplescan_path(
    root: &mut PlannerInfo,
    rel: &RelOptInfo,
    required_outer: Relids,
) -> Box<Path> {
    unimplemented!()
}

pub fn create_index_path(
    root: &mut PlannerInfo,
    index: &IndexOptInfo,
    indexclauses: Vec<Node>,
    indexorderbys: Vec<Node>,
    indexorderbycols: Vec<i32>,
    pathkeys: Vec<Node>,
    indexscandir: ScanDirection,
    indexonly: bool,
    required_outer: Relids,
    loop_count: f64,
    partial_path: bool,
) -> Box<IndexPath> {
    unimplemented!()
}

pub fn create_bitmap_heap_path(
    root: &mut PlannerInfo,
    rel: &RelOptInfo,
    bitmapqual: Box<Path>,
    required_outer: Relids,
    loop_count: f64,
    parallel_degree: i32,
) -> Box<BitmapHeapPath> {
    unimplemented!()
}

pub fn create_bitmap_and_path(
    root: &mut PlannerInfo,
    rel: &RelOptInfo,
    bitmapquals: Vec<Box<Path>>,
) -> Box<BitmapAndPath> {
    unimplemented!()
}

pub fn create_bitmap_or_path(
    root: &mut PlannerInfo,
    rel: &RelOptInfo,
    bitmapquals: Vec<Box<Path>>,
) -> Box<BitmapOrPath> {
    unimplemented!()
}

pub fn create_tidscan_path(
    root: &mut PlannerInfo,
    rel: &RelOptInfo,
    tidquals: Vec<Node>,
    required_outer: Relids,
) -> Box<TidPath> {
    unimplemented!()
}

pub fn create_tidrangescan_path(
    root: &mut PlannerInfo,
    rel: &RelOptInfo,
    tidrangequals: Vec<Node>,
    required_outer: Relids,
) -> Box<TidRangePath> {
    unimplemented!()
}

pub fn create_append_path(
    root: &mut PlannerInfo,
    rel: &RelOptInfo,
    subpaths: Vec<Box<Path>>,
    partial_subpaths: Vec<Box<Path>>,
    pathkeys: Vec<Node>,
    required_outer: Relids,
    parallel_workers: i32,
    parallel_aware: bool,
    rows: f64,
) -> Box<AppendPath> {
    unimplemented!()
}

pub fn create_merge_append_path(
    root: &mut PlannerInfo,
    rel: &RelOptInfo,
    subpaths: Vec<Box<Path>>,
    pathkeys: Vec<Node>,
    required_outer: Relids,
) -> Box<MergeAppendPath> {
    unimplemented!()
}

/// PG `create_group_result_path`. See `crate::backend::optimizer::util::pathnode`.
pub use crate::backend::optimizer::util::pathnode::create_group_result_path;

pub fn create_material_path(rel: &RelOptInfo, subpath: Box<Path>) -> Box<MaterialPath> {
    unimplemented!()
}

pub fn create_memoize_path(
    root: &mut PlannerInfo,
    rel: &RelOptInfo,
    subpath: Box<Path>,
    param_exprs: Vec<Node>,
    hash_operators: Vec<Node>,
    singlerow: bool,
    binary_mode: bool,
    calls: f64,
) -> Box<MemoizePath> {
    unimplemented!()
}

pub fn create_unique_path(
    root: &mut PlannerInfo,
    rel: &RelOptInfo,
    subpath: Box<Path>,
    sjinfo: &SpecialJoinInfo,
) -> Box<UniquePath> {
    unimplemented!()
}

/// C in-out param `rows` -> `&mut f64`.
pub fn create_gather_path(
    root: &mut PlannerInfo,
    rel: &RelOptInfo,
    subpath: Box<Path>,
    target: &PathTarget,
    required_outer: Relids,
    rows: &mut f64,
) -> Box<GatherPath> {
    unimplemented!()
}

/// C in-out param `rows` -> `&mut f64`.
pub fn create_gather_merge_path(
    root: &mut PlannerInfo,
    rel: &RelOptInfo,
    subpath: Box<Path>,
    target: &PathTarget,
    pathkeys: Vec<Node>,
    required_outer: Relids,
    rows: &mut f64,
) -> Box<GatherMergePath> {
    unimplemented!()
}

pub fn create_subqueryscan_path(
    root: &mut PlannerInfo,
    rel: &RelOptInfo,
    subpath: Box<Path>,
    trivial_pathtarget: bool,
    pathkeys: Vec<Node>,
    required_outer: Relids,
) -> Box<SubqueryScanPath> {
    unimplemented!()
}

pub fn create_functionscan_path(
    root: &mut PlannerInfo,
    rel: &RelOptInfo,
    pathkeys: Vec<Node>,
    required_outer: Relids,
) -> Box<Path> {
    unimplemented!()
}

pub fn create_valuesscan_path(
    root: &mut PlannerInfo,
    rel: &RelOptInfo,
    required_outer: Relids,
) -> Box<Path> {
    unimplemented!()
}

pub fn create_tablefuncscan_path(
    root: &mut PlannerInfo,
    rel: &RelOptInfo,
    required_outer: Relids,
) -> Box<Path> {
    unimplemented!()
}

pub fn create_ctescan_path(
    root: &mut PlannerInfo,
    rel: &RelOptInfo,
    pathkeys: Vec<Node>,
    required_outer: Relids,
) -> Box<Path> {
    unimplemented!()
}

pub fn create_namedtuplestorescan_path(
    root: &mut PlannerInfo,
    rel: &RelOptInfo,
    required_outer: Relids,
) -> Box<Path> {
    unimplemented!()
}

pub fn create_resultscan_path(
    root: &mut PlannerInfo,
    rel: &RelOptInfo,
    required_outer: Relids,
) -> Box<Path> {
    unimplemented!()
}

pub fn create_worktablescan_path(
    root: &mut PlannerInfo,
    rel: &RelOptInfo,
    required_outer: Relids,
) -> Box<Path> {
    unimplemented!()
}

pub fn create_foreignscan_path(
    root: &mut PlannerInfo,
    rel: &RelOptInfo,
    target: &PathTarget,
    rows: f64,
    disabled_nodes: i32,
    startup_cost: Cost,
    total_cost: Cost,
    pathkeys: Vec<Node>,
    required_outer: Relids,
    fdw_outerpath: Option<Box<Path>>,
    fdw_restrictinfo: Vec<Node>,
    fdw_private: Vec<Node>,
) -> Box<ForeignPath> {
    unimplemented!()
}

pub fn create_foreign_join_path(
    root: &mut PlannerInfo,
    rel: &RelOptInfo,
    target: &PathTarget,
    rows: f64,
    disabled_nodes: i32,
    startup_cost: Cost,
    total_cost: Cost,
    pathkeys: Vec<Node>,
    required_outer: Relids,
    fdw_outerpath: Option<Box<Path>>,
    fdw_restrictinfo: Vec<Node>,
    fdw_private: Vec<Node>,
) -> Box<ForeignPath> {
    unimplemented!()
}

pub fn create_foreign_upper_path(
    root: &mut PlannerInfo,
    rel: &RelOptInfo,
    target: &PathTarget,
    rows: f64,
    disabled_nodes: i32,
    startup_cost: Cost,
    total_cost: Cost,
    pathkeys: Vec<Node>,
    fdw_outerpath: Option<Box<Path>>,
    fdw_restrictinfo: Vec<Node>,
    fdw_private: Vec<Node>,
) -> Box<ForeignPath> {
    unimplemented!()
}

pub fn calc_nestloop_required_outer(
    outerrelids: Relids,
    outer_paramrels: Relids,
    innerrelids: Relids,
    inner_paramrels: Relids,
) -> Relids {
    unimplemented!()
}

pub fn calc_non_nestloop_required_outer(outer_path: &Path, inner_path: &Path) -> Relids {
    unimplemented!()
}

pub fn create_nestloop_path(
    root: &mut PlannerInfo,
    joinrel: &RelOptInfo,
    jointype: JoinType,
    workspace: &JoinCostWorkspace,
    extra: &JoinPathExtraData,
    outer_path: Box<Path>,
    inner_path: Box<Path>,
    restrict_clauses: Vec<Node>,
    pathkeys: Vec<Node>,
    required_outer: Relids,
) -> Box<NestPath> {
    unimplemented!()
}

pub fn create_mergejoin_path(
    root: &mut PlannerInfo,
    joinrel: &RelOptInfo,
    jointype: JoinType,
    workspace: &JoinCostWorkspace,
    extra: &JoinPathExtraData,
    outer_path: Box<Path>,
    inner_path: Box<Path>,
    restrict_clauses: Vec<Node>,
    pathkeys: Vec<Node>,
    required_outer: Relids,
    mergeclauses: Vec<Node>,
    outersortkeys: Vec<Node>,
    innersortkeys: Vec<Node>,
    outer_presorted_keys: i32,
) -> Box<MergePath> {
    unimplemented!()
}

pub fn create_hashjoin_path(
    root: &mut PlannerInfo,
    joinrel: &RelOptInfo,
    jointype: JoinType,
    workspace: &JoinCostWorkspace,
    extra: &JoinPathExtraData,
    outer_path: Box<Path>,
    inner_path: Box<Path>,
    parallel_hash: bool,
    restrict_clauses: Vec<Node>,
    required_outer: Relids,
    hashclauses: Vec<Node>,
) -> Box<HashPath> {
    unimplemented!()
}

pub fn create_projection_path(
    root: &mut PlannerInfo,
    rel: &RelOptInfo,
    subpath: Box<Path>,
    target: &PathTarget,
) -> Box<ProjectionPath> {
    unimplemented!()
}

pub fn apply_projection_to_path(
    root: &mut PlannerInfo,
    rel: &RelOptInfo,
    path: Box<Path>,
    target: &PathTarget,
) -> Box<Path> {
    unimplemented!()
}

pub fn create_set_projection_path(
    root: &mut PlannerInfo,
    rel: &RelOptInfo,
    subpath: Box<Path>,
    target: &PathTarget,
) -> Box<ProjectSetPath> {
    unimplemented!()
}

pub fn create_sort_path(
    root: &mut PlannerInfo,
    rel: &RelOptInfo,
    subpath: Box<Path>,
    pathkeys: Vec<Node>,
    limit_tuples: f64,
) -> Box<SortPath> {
    unimplemented!()
}

pub fn create_incremental_sort_path(
    root: &mut PlannerInfo,
    rel: &RelOptInfo,
    subpath: Box<Path>,
    pathkeys: Vec<Node>,
    presorted_keys: i32,
    limit_tuples: f64,
) -> Box<IncrementalSortPath> {
    unimplemented!()
}

pub fn create_group_path(
    root: &mut PlannerInfo,
    rel: &RelOptInfo,
    subpath: Box<Path>,
    group_clause: Vec<Node>,
    qual: Vec<Node>,
    num_groups: f64,
) -> Box<GroupPath> {
    unimplemented!()
}

pub fn create_upper_unique_path(
    root: &mut PlannerInfo,
    rel: &RelOptInfo,
    subpath: Box<Path>,
    num_cols: i32,
    num_groups: f64,
) -> Box<UpperUniquePath> {
    unimplemented!()
}

pub fn create_agg_path(
    root: &mut PlannerInfo,
    rel: &RelOptInfo,
    subpath: Box<Path>,
    target: &PathTarget,
    aggstrategy: AggStrategy,
    aggsplit: AggSplit,
    group_clause: Vec<Node>,
    qual: Vec<Node>,
    aggcosts: Option<&AggClauseCosts>,
    num_groups: f64,
) -> Box<AggPath> {
    unimplemented!()
}

pub fn create_groupingsets_path(
    root: &mut PlannerInfo,
    rel: &RelOptInfo,
    subpath: Box<Path>,
    having_qual: Vec<Node>,
    aggstrategy: AggStrategy,
    rollups: Vec<Node>,
    agg_costs: Option<&AggClauseCosts>,
) -> Box<GroupingSetsPath> {
    unimplemented!()
}

pub fn create_minmaxagg_path(
    root: &mut PlannerInfo,
    rel: &RelOptInfo,
    target: &PathTarget,
    mmaggregates: Vec<Node>,
    quals: Vec<Node>,
) -> Box<MinMaxAggPath> {
    unimplemented!()
}

pub fn create_windowagg_path(
    root: &mut PlannerInfo,
    rel: &RelOptInfo,
    subpath: Box<Path>,
    target: &PathTarget,
    window_funcs: Vec<Node>,
    run_condition: Vec<Node>,
    winclause: &WindowClause,
    qual: Vec<Node>,
    topwindow: bool,
) -> Box<WindowAggPath> {
    unimplemented!()
}

pub fn create_setop_path(
    root: &mut PlannerInfo,
    rel: &RelOptInfo,
    leftpath: Box<Path>,
    rightpath: Box<Path>,
    cmd: SetOpCmd,
    strategy: SetOpStrategy,
    group_list: Vec<Node>,
    num_groups: f64,
    output_rows: f64,
) -> Box<SetOpPath> {
    unimplemented!()
}

pub fn create_recursiveunion_path(
    root: &mut PlannerInfo,
    rel: &RelOptInfo,
    leftpath: Box<Path>,
    rightpath: Box<Path>,
    target: &PathTarget,
    distinct_list: Vec<Node>,
    wt_param: i32,
    num_groups: f64,
) -> Box<RecursiveUnionPath> {
    unimplemented!()
}

pub fn create_lockrows_path(
    root: &mut PlannerInfo,
    rel: &RelOptInfo,
    subpath: Box<Path>,
    row_marks: Vec<Node>,
    epq_param: i32,
) -> Box<LockRowsPath> {
    unimplemented!()
}

pub fn create_modifytable_path(
    root: &mut PlannerInfo,
    rel: &RelOptInfo,
    subpath: Box<Path>,
    operation: CmdType,
    can_set_tag: bool,
    nominal_relation: usize,
    root_relation: usize,
    part_cols_updated: bool,
    result_relations: Vec<i32>,
    update_colnos_lists: Vec<Node>,
    with_check_option_lists: Vec<Node>,
    returning_lists: Vec<Node>,
    row_marks: Vec<Node>,
    onconflict: Option<Box<OnConflictExpr>>,
    merge_action_lists: Vec<Node>,
    merge_join_conditions: Vec<Node>,
    epq_param: i32,
) -> Box<ModifyTablePath> {
    unimplemented!()
}

pub fn create_limit_path(
    root: &mut PlannerInfo,
    rel: &RelOptInfo,
    subpath: Box<Path>,
    limit_offset: Option<Node>,
    limit_count: Option<Node>,
    limit_option: LimitOption,
    offset_est: i64,
    count_est: i64,
) -> Box<LimitPath> {
    unimplemented!()
}

/// C in-out params `rows`, `startup_cost`, `total_cost` -> `&mut`.
pub fn adjust_limit_rows_costs(
    rows: &mut f64,
    startup_cost: &mut Cost,
    total_cost: &mut Cost,
    offset_est: i64,
    count_est: i64,
) {
    unimplemented!()
}

pub fn reparameterize_path(
    root: &mut PlannerInfo,
    path: Box<Path>,
    required_outer: Relids,
    loop_count: f64,
) -> Option<Box<Path>> {
    unimplemented!()
}

pub fn reparameterize_path_by_child(
    root: &mut PlannerInfo,
    path: Box<Path>,
    child_rel: &RelOptInfo,
) -> Option<Box<Path>> {
    unimplemented!()
}

pub fn path_is_reparameterizable_by_child(path: &Path, child_rel: &RelOptInfo) -> bool {
    unimplemented!()
}

/*
 * prototypes for relnode.c
 */
pub fn setup_simple_rel_arrays(root: &mut PlannerInfo) {
    unimplemented!()
}

pub fn expand_planner_arrays(root: &mut PlannerInfo, add_size: i32) {
    unimplemented!()
}

pub fn build_simple_rel(
    root: &mut PlannerInfo,
    relid: i32,
    parent: Option<&RelOptInfo>,
) -> Box<RelOptInfo> {
    unimplemented!()
}

pub fn find_base_rel(root: &mut PlannerInfo, relid: i32) -> Box<RelOptInfo> {
    unimplemented!()
}

pub fn find_base_rel_noerr(root: &mut PlannerInfo, relid: i32) -> Option<Box<RelOptInfo>> {
    unimplemented!()
}

pub fn find_base_rel_ignore_join(root: &mut PlannerInfo, relid: i32) -> Box<RelOptInfo> {
    unimplemented!()
}

pub fn find_join_rel(root: &mut PlannerInfo, relids: Relids) -> Option<Box<RelOptInfo>> {
    unimplemented!()
}

/// C out-param `restrictlist_ptr` -> second element of returned tuple.
pub fn build_join_rel(
    root: &mut PlannerInfo,
    joinrelids: Relids,
    outer_rel: &RelOptInfo,
    inner_rel: &RelOptInfo,
    sjinfo: &SpecialJoinInfo,
    pushed_down_joins: Vec<Node>,
) -> (Box<RelOptInfo>, Vec<Node>) {
    unimplemented!()
}

pub fn min_join_parameterization(
    root: &mut PlannerInfo,
    joinrelids: Relids,
    outer_rel: &RelOptInfo,
    inner_rel: &RelOptInfo,
) -> Relids {
    unimplemented!()
}

pub fn fetch_upper_rel(
    root: &mut PlannerInfo,
    kind: UpperRelationKind,
    relids: Relids,
) -> Box<RelOptInfo> {
    unimplemented!()
}

pub fn find_childrel_parents(root: &mut PlannerInfo, rel: &RelOptInfo) -> Relids {
    unimplemented!()
}

pub fn get_baserel_parampathinfo(
    root: &mut PlannerInfo,
    baserel: &RelOptInfo,
    required_outer: Relids,
) -> Option<Box<ParamPathInfo>> {
    unimplemented!()
}

/// C out-param `restrict_clauses` -> second element of returned tuple.
pub fn get_joinrel_parampathinfo(
    root: &mut PlannerInfo,
    joinrel: &RelOptInfo,
    outer_path: &Path,
    inner_path: &Path,
    sjinfo: &SpecialJoinInfo,
    required_outer: Relids,
) -> (Option<Box<ParamPathInfo>>, Vec<Node>) {
    unimplemented!()
}

pub fn get_appendrel_parampathinfo(
    appendrel: &RelOptInfo,
    required_outer: Relids,
) -> Option<Box<ParamPathInfo>> {
    unimplemented!()
}

pub fn find_param_path_info(
    rel: &RelOptInfo,
    required_outer: Relids,
) -> Option<Box<ParamPathInfo>> {
    unimplemented!()
}

pub fn get_param_path_clause_serials(path: &Path) -> Bitmapset {
    unimplemented!()
}

pub fn build_child_join_rel(
    root: &mut PlannerInfo,
    outer_rel: &RelOptInfo,
    inner_rel: &RelOptInfo,
    parent_joinrel: &RelOptInfo,
    restrictlist: Vec<Node>,
    sjinfo: &SpecialJoinInfo,
    appinfos: &[&AppendRelInfo],
) -> Box<RelOptInfo> {
    unimplemented!()
}
