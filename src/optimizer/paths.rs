//! Translated from PostgreSQL src/include/optimizer/paths.h

use crate::access::cmptype::CompareType;
use crate::access::sdir::ScanDirection;
use crate::nodes::nodes::{JoinType, Node};
use crate::nodes::parsenodes::RangeTblEntry;
use crate::nodes::pathnodes::{
    AppendRelInfo, CostSelector, EquivalenceClass, EquivalenceMember, EquivalenceMemberIterator,
    ForeignKeyOptInfo, IndexOptInfo, JoinDomain, JoinPathExtraData, Path, PathKey, PlannerInfo,
    RelOptInfo, Relids, RestrictInfo, SpecialJoinInfo,
};
use crate::nodes::primnodes::{Expr, Index};
use crate::postgres_ext::Oid;

/*
 * allpaths.c
 */
pub static mut ENABLE_GEQO: bool = false;
pub static mut GEQO_THRESHOLD: i32 = 0;
pub static mut MIN_PARALLEL_TABLE_SCAN_SIZE: i32 = 0;
pub static mut MIN_PARALLEL_INDEX_SCAN_SIZE: i32 = 0;
pub static mut ENABLE_GROUP_BY_REORDERING: bool = false;

/// Hook for plugins to get control in set_rel_pathlist().
pub type SetRelPathlistHookType =
    fn(root: &mut PlannerInfo, rel: &mut RelOptInfo, rti: Index, rte: &RangeTblEntry);
pub static mut SET_REL_PATHLIST_HOOK: Option<SetRelPathlistHookType> = None;

/// Hook for plugins to get control in add_paths_to_joinrel().
pub type SetJoinPathlistHookType = fn(
    root: &mut PlannerInfo,
    joinrel: &mut RelOptInfo,
    outerrel: &RelOptInfo,
    innerrel: &RelOptInfo,
    jointype: JoinType,
    extra: &JoinPathExtraData,
);
pub static mut SET_JOIN_PATHLIST_HOOK: Option<SetJoinPathlistHookType> = None;

/// Hook for plugins to replace standard_join_search().
pub type JoinSearchHookType =
    fn(root: &mut PlannerInfo, levels_needed: i32, initial_rels: &[RelOptInfo]) -> RelOptInfo;
pub static mut JOIN_SEARCH_HOOK: Option<JoinSearchHookType> = None;

pub fn make_one_rel(root: &mut PlannerInfo, joinlist: &[Node]) -> RelOptInfo {
    unimplemented!()
}

pub fn standard_join_search(
    root: &mut PlannerInfo,
    levels_needed: i32,
    initial_rels: &[RelOptInfo],
) -> RelOptInfo {
    unimplemented!()
}

pub fn generate_gather_paths(root: &mut PlannerInfo, rel: &mut RelOptInfo, override_rows: bool) {
    unimplemented!()
}

pub fn generate_useful_gather_paths(root: &mut PlannerInfo, rel: &mut RelOptInfo, override_rows: bool) {
    unimplemented!()
}

pub fn compute_parallel_worker(
    rel: &RelOptInfo,
    heap_pages: f64,
    index_pages: f64,
    max_workers: i32,
) -> i32 {
    unimplemented!()
}

pub fn create_partial_bitmap_paths(root: &mut PlannerInfo, rel: &mut RelOptInfo, bitmapqual: &Path) {
    unimplemented!()
}

pub fn generate_partitionwise_join_paths(root: &mut PlannerInfo, rel: &mut RelOptInfo) {
    unimplemented!()
}

/*
 * indxpath.c -- routines to generate index paths
 */
pub fn create_index_paths(root: &mut PlannerInfo, rel: &mut RelOptInfo) {
    unimplemented!()
}

pub fn relation_has_unique_index_for(
    root: &mut PlannerInfo,
    rel: &RelOptInfo,
    restrictlist: &[RestrictInfo],
    exprlist: &[Expr],
    oprlist: &[Oid],
) -> bool {
    unimplemented!()
}

/// out-param `extra_clauses` folded into the returned tuple.
pub fn relation_has_unique_index_ext(
    root: &mut PlannerInfo,
    rel: &RelOptInfo,
    restrictlist: &[RestrictInfo],
    exprlist: &[Expr],
    oprlist: &[Oid],
) -> (bool, Vec<RestrictInfo>) {
    unimplemented!()
}

pub fn indexcol_is_bool_constant_for_query(
    root: &mut PlannerInfo,
    index: &IndexOptInfo,
    indexcol: i32,
) -> bool {
    unimplemented!()
}

pub fn match_index_to_operand(operand: &Node, indexcol: i32, index: &IndexOptInfo) -> bool {
    unimplemented!()
}

pub fn strip_phvs_in_index_operand(operand: Box<Node>) -> Box<Node> {
    unimplemented!()
}

pub fn check_index_predicates(root: &mut PlannerInfo, rel: &mut RelOptInfo) {
    unimplemented!()
}

/*
 * tidpath.c -- routines to generate tid paths
 */
pub fn create_tidscan_paths(root: &mut PlannerInfo, rel: &mut RelOptInfo) -> bool {
    unimplemented!()
}

/*
 * joinpath.c -- routines to create join paths
 */
pub fn add_paths_to_joinrel(
    root: &mut PlannerInfo,
    joinrel: &mut RelOptInfo,
    outerrel: &RelOptInfo,
    innerrel: &RelOptInfo,
    jointype: JoinType,
    sjinfo: &SpecialJoinInfo,
    restrictlist: &[RestrictInfo],
) {
    unimplemented!()
}

/*
 * joinrels.c -- routines to determine which relations to join
 */
pub fn join_search_one_level(root: &mut PlannerInfo, level: i32) {
    unimplemented!()
}

pub fn make_join_rel(root: &mut PlannerInfo, rel1: &RelOptInfo, rel2: &RelOptInfo) -> RelOptInfo {
    unimplemented!()
}

/// out-param `pushed_down_joins` folded into the returned tuple.
pub fn add_outer_joins_to_relids(
    root: &mut PlannerInfo,
    input_relids: Relids,
    sjinfo: &SpecialJoinInfo,
) -> (Relids, Vec<SpecialJoinInfo>) {
    unimplemented!()
}

pub fn have_join_order_restriction(
    root: &mut PlannerInfo,
    rel1: &RelOptInfo,
    rel2: &RelOptInfo,
) -> bool {
    unimplemented!()
}

pub fn mark_dummy_rel(rel: &mut RelOptInfo) {
    unimplemented!()
}

pub fn init_dummy_sjinfo(sjinfo: &mut SpecialJoinInfo, left_relids: Relids, right_relids: Relids) {
    unimplemented!()
}

/*
 * equivclass.c -- routines for managing EquivalenceClasses
 */
pub type EcMatchesCallbackType = fn(
    root: &mut PlannerInfo,
    rel: &RelOptInfo,
    ec: &EquivalenceClass,
    em: &EquivalenceMember,
) -> bool;

/// in-out param `p_restrictinfo` modeled as `&mut`.
pub fn process_equivalence(
    root: &mut PlannerInfo,
    p_restrictinfo: &mut RestrictInfo,
    jdomain: &JoinDomain,
) -> bool {
    unimplemented!()
}

pub fn canonicalize_ec_expression(expr: Box<Expr>, req_type: Oid, req_collation: Oid) -> Box<Expr> {
    unimplemented!()
}

pub fn reconsider_outer_join_clauses(root: &mut PlannerInfo) {
    unimplemented!()
}

pub fn rebuild_eclass_attr_needed(root: &mut PlannerInfo) {
    unimplemented!()
}

pub fn get_eclass_for_sort_expr(
    root: &mut PlannerInfo,
    expr: &Expr,
    opfamilies: &[Oid],
    opcintype: Oid,
    collation: Oid,
    sortref: Index,
    rel: Relids,
    create_it: bool,
) -> Option<EquivalenceClass> {
    unimplemented!()
}

pub fn find_ec_member_matching_expr(
    ec: &EquivalenceClass,
    expr: &Expr,
    relids: Relids,
) -> Option<EquivalenceMember> {
    unimplemented!()
}

pub fn find_computable_ec_member(
    root: &mut PlannerInfo,
    ec: &EquivalenceClass,
    exprs: &[Expr],
    relids: Relids,
    require_parallel_safe: bool,
) -> Option<EquivalenceMember> {
    unimplemented!()
}

pub fn relation_can_be_sorted_early(
    root: &mut PlannerInfo,
    rel: &RelOptInfo,
    ec: &EquivalenceClass,
    require_parallel_safe: bool,
) -> bool {
    unimplemented!()
}

pub fn generate_base_implied_equalities(root: &mut PlannerInfo) {
    unimplemented!()
}

pub fn generate_join_implied_equalities(
    root: &mut PlannerInfo,
    join_relids: Relids,
    outer_relids: Relids,
    inner_rel: &RelOptInfo,
    sjinfo: &SpecialJoinInfo,
) -> Vec<RestrictInfo> {
    unimplemented!()
}

pub fn generate_join_implied_equalities_for_ecs(
    root: &mut PlannerInfo,
    eclasses: &[EquivalenceClass],
    join_relids: Relids,
    outer_relids: Relids,
    inner_rel: &RelOptInfo,
) -> Vec<RestrictInfo> {
    unimplemented!()
}

pub fn exprs_known_equal(root: &mut PlannerInfo, item1: &Node, item2: &Node, opfamily: Oid) -> bool {
    unimplemented!()
}

pub fn match_eclasses_to_foreign_key_col(
    root: &mut PlannerInfo,
    fkinfo: &ForeignKeyOptInfo,
    colno: i32,
) -> Option<EquivalenceClass> {
    unimplemented!()
}

pub fn find_derived_clause_for_ec_member(
    root: &mut PlannerInfo,
    ec: &EquivalenceClass,
    em: &EquivalenceMember,
) -> Option<RestrictInfo> {
    unimplemented!()
}

pub fn add_child_rel_equivalences(
    root: &mut PlannerInfo,
    appinfo: &AppendRelInfo,
    parent_rel: &RelOptInfo,
    child_rel: &mut RelOptInfo,
) {
    unimplemented!()
}

pub fn add_child_join_rel_equivalences(
    root: &mut PlannerInfo,
    appinfos: &[AppendRelInfo],
    parent_joinrel: &RelOptInfo,
    child_joinrel: &mut RelOptInfo,
) {
    unimplemented!()
}

pub fn add_setop_child_rel_equivalences(
    root: &mut PlannerInfo,
    child_rel: &mut RelOptInfo,
    child_tlist: &[crate::nodes::primnodes::TargetEntry],
    setop_pathkeys: &[PathKey],
) {
    unimplemented!()
}

pub fn setup_eclass_member_iterator(
    it: &mut EquivalenceMemberIterator,
    ec: &EquivalenceClass,
    child_relids: Relids,
) {
    unimplemented!()
}

pub fn eclass_member_iterator_next(it: &mut EquivalenceMemberIterator) -> Option<EquivalenceMember> {
    unimplemented!()
}

pub fn generate_implied_equalities_for_column(
    root: &mut PlannerInfo,
    rel: &RelOptInfo,
    callback: EcMatchesCallbackType,
    prohibited_rels: Relids,
) -> Vec<RestrictInfo> {
    unimplemented!()
}

pub fn have_relevant_eclass_joinclause(
    root: &mut PlannerInfo,
    rel1: &RelOptInfo,
    rel2: &RelOptInfo,
) -> bool {
    unimplemented!()
}

pub fn has_relevant_eclass_joinclause(root: &mut PlannerInfo, rel1: &RelOptInfo) -> bool {
    unimplemented!()
}

pub fn eclass_useful_for_merging(
    root: &mut PlannerInfo,
    eclass: &EquivalenceClass,
    rel: &RelOptInfo,
) -> bool {
    unimplemented!()
}

pub fn is_redundant_derived_clause(rinfo: &RestrictInfo, clauselist: &[RestrictInfo]) -> bool {
    unimplemented!()
}

pub fn is_redundant_with_indexclauses(rinfo: &RestrictInfo, indexclauses: &[Node]) -> bool {
    unimplemented!()
}

pub fn ec_clear_derived_clauses(ec: &mut EquivalenceClass) {
    unimplemented!()
}

/*
 * pathkeys.c -- utilities for matching and building path keys
 */
pub enum PathKeysComparison {
    Equal,     // pathkeys are identical
    Better1,   // pathkey 1 is a superset of pathkey 2
    Better2,   // vice versa
    Different, // neither pathkey includes the other
}

pub fn compare_pathkeys(keys1: &[PathKey], keys2: &[PathKey]) -> PathKeysComparison {
    unimplemented!()
}

pub fn pathkeys_contained_in(keys1: &[PathKey], keys2: &[PathKey]) -> bool {
    unimplemented!()
}

/// out-param `n_common` folded into the returned tuple.
pub fn pathkeys_count_contained_in(keys1: &[PathKey], keys2: &[PathKey]) -> (bool, i32) {
    unimplemented!()
}

pub fn get_useful_group_keys_orderings(root: &mut PlannerInfo, path: &Path) -> Vec<Node> {
    unimplemented!()
}

pub fn get_cheapest_path_for_pathkeys(
    paths: &[Path],
    pathkeys: &[PathKey],
    required_outer: Relids,
    cost_criterion: CostSelector,
    require_parallel_safe: bool,
) -> Option<Path> {
    unimplemented!()
}

pub fn get_cheapest_fractional_path_for_pathkeys(
    paths: &[Path],
    pathkeys: &[PathKey],
    required_outer: Relids,
    fraction: f64,
) -> Option<Path> {
    unimplemented!()
}

pub fn get_cheapest_parallel_safe_total_inner(paths: &[Path]) -> Option<Path> {
    unimplemented!()
}

pub fn build_index_pathkeys(
    root: &mut PlannerInfo,
    index: &IndexOptInfo,
    scandir: ScanDirection,
) -> Vec<PathKey> {
    unimplemented!()
}

/// out-param `partialkeys` folded into the returned tuple.
pub fn build_partition_pathkeys(
    root: &mut PlannerInfo,
    partrel: &RelOptInfo,
    scandir: ScanDirection,
) -> (Vec<PathKey>, bool) {
    unimplemented!()
}

pub fn build_expression_pathkey(
    root: &mut PlannerInfo,
    expr: &Expr,
    opno: Oid,
    rel: Relids,
    create_it: bool,
) -> Vec<PathKey> {
    unimplemented!()
}

pub fn convert_subquery_pathkeys(
    root: &mut PlannerInfo,
    rel: &RelOptInfo,
    subquery_pathkeys: &[PathKey],
    subquery_tlist: &[crate::nodes::primnodes::TargetEntry],
) -> Vec<PathKey> {
    unimplemented!()
}

pub fn build_join_pathkeys(
    root: &mut PlannerInfo,
    joinrel: &RelOptInfo,
    jointype: JoinType,
    outer_pathkeys: &[PathKey],
) -> Vec<PathKey> {
    unimplemented!()
}

pub fn make_pathkeys_for_sortclauses(
    root: &mut PlannerInfo,
    sortclauses: &[crate::nodes::parsenodes::SortGroupClause],
    tlist: &[crate::nodes::primnodes::TargetEntry],
) -> Vec<PathKey> {
    unimplemented!()
}

/// in-out `sortclauses` and out `sortable` folded into the returned tuple.
pub fn make_pathkeys_for_sortclauses_extended(
    root: &mut PlannerInfo,
    sortclauses: &mut Vec<crate::nodes::parsenodes::SortGroupClause>,
    tlist: &[crate::nodes::primnodes::TargetEntry],
    remove_redundant: bool,
    remove_group_rtindex: bool,
    set_ec_sortref: bool,
) -> (Vec<PathKey>, bool) {
    unimplemented!()
}

pub fn initialize_mergeclause_eclasses(root: &mut PlannerInfo, restrictinfo: &mut RestrictInfo) {
    unimplemented!()
}

pub fn update_mergeclause_eclasses(root: &mut PlannerInfo, restrictinfo: &mut RestrictInfo) {
    unimplemented!()
}

pub fn find_mergeclauses_for_outer_pathkeys(
    root: &mut PlannerInfo,
    pathkeys: &[PathKey],
    restrictinfos: &[RestrictInfo],
) -> Vec<RestrictInfo> {
    unimplemented!()
}

pub fn select_outer_pathkeys_for_merge(
    root: &mut PlannerInfo,
    mergeclauses: &[RestrictInfo],
    joinrel: &RelOptInfo,
) -> Vec<PathKey> {
    unimplemented!()
}

pub fn make_inner_pathkeys_for_merge(
    root: &mut PlannerInfo,
    mergeclauses: &[RestrictInfo],
    outer_pathkeys: &[PathKey],
) -> Vec<PathKey> {
    unimplemented!()
}

pub fn trim_mergeclauses_for_inner_pathkeys(
    root: &mut PlannerInfo,
    mergeclauses: &[RestrictInfo],
    pathkeys: &[PathKey],
) -> Vec<RestrictInfo> {
    unimplemented!()
}

pub fn truncate_useless_pathkeys(
    root: &mut PlannerInfo,
    rel: &RelOptInfo,
    pathkeys: &[PathKey],
) -> Vec<PathKey> {
    unimplemented!()
}

pub fn has_useful_pathkeys(root: &mut PlannerInfo, rel: &RelOptInfo) -> bool {
    unimplemented!()
}

pub fn append_pathkeys(target: Vec<PathKey>, source: &[PathKey]) -> Vec<PathKey> {
    unimplemented!()
}

pub fn make_canonical_pathkey(
    root: &mut PlannerInfo,
    eclass: &EquivalenceClass,
    opfamily: Oid,
    cmptype: CompareType,
    nulls_first: bool,
) -> PathKey {
    unimplemented!()
}

pub fn add_paths_to_append_rel(
    root: &mut PlannerInfo,
    rel: &mut RelOptInfo,
    live_childrels: &[RelOptInfo],
) {
    unimplemented!()
}
