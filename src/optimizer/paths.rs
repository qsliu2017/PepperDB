//! Translated from PostgreSQL src/include/optimizer/paths.h

#![allow(clippy::boxed_local, reason = "1:1 PG port: Node/Box<Path> mirrors PG pointer-passed nodes")]
#![allow(clippy::needless_pass_by_value, reason = "1:1 PG port: stubs take owned node values matching PG C signatures; consumed once implemented")]
#![allow(clippy::ptr_arg, reason = "1:1 PG port: &mut Vec matches PG list mutation API")]

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
/// PG `create_index_paths`. See `crate::backend::optimizer::path::indxpath`.
pub use crate::backend::optimizer::path::indxpath::create_index_paths;

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

pub fn strip_phvs_in_index_operand(operand: Node) -> Node {
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

// equivclass.c bodies live in backend/optimizer/path/equivclass.rs. The
// INNER-JOIN path is fully translated; outer-join/appendrel/setop/FK/index
// helpers are staged stubs inside that module.
pub use crate::backend::optimizer::path::equivclass::{
    add_child_join_rel_equivalences, add_child_rel_equivalences, add_setop_child_rel_equivalences,
    canonicalize_ec_expression, ec_clear_derived_clauses, eclass_member_iterator_next,
    eclass_useful_for_merging, exprs_known_equal, find_computable_ec_member,
    find_derived_clause_for_ec_member, find_ec_member_matching_expr,
    generate_base_implied_equalities, generate_implied_equalities_for_column,
    generate_join_implied_equalities, generate_join_implied_equalities_for_ecs,
    get_eclass_for_sort_expr, has_relevant_eclass_joinclause, have_relevant_eclass_joinclause,
    is_redundant_derived_clause, is_redundant_with_indexclauses, match_eclasses_to_foreign_key_col,
    process_equivalence, rebuild_eclass_attr_needed, reconsider_outer_join_clauses,
    relation_can_be_sorted_early, setup_eclass_member_iterator,
};

/*
 * pathkeys.c -- utilities for matching and building path keys
 */
pub enum PathKeysComparison {
    Equal,     // pathkeys are identical
    Better1,   // pathkey 1 is a superset of pathkey 2
    Better2,   // vice versa
    Different, // neither pathkey includes the other
}

// pathkeys.c bodies live in backend/optimizer/path/pathkeys.rs. The INNER-JOIN +
// ORDER BY path (canonical pathkeys, sortclause -> pathkeys, containment, index
// pathkeys, merge-clause reasoning, usefulness checks) is fully translated;
// partitioning/subquery/group-by-reordering helpers are staged stubs there.
// PathKey identity uses value equality (cloned EC snapshot) instead of PG's
// pointer identity -- see the module's representation note.
pub use crate::backend::optimizer::path::pathkeys::{
    append_pathkeys, build_expression_pathkey, build_index_pathkeys, build_join_pathkeys,
    build_partition_pathkeys, compare_pathkeys, convert_subquery_pathkeys,
    find_mergeclauses_for_outer_pathkeys, get_cheapest_fractional_path_for_pathkeys,
    get_cheapest_parallel_safe_total_inner, get_cheapest_path_for_pathkeys,
    get_useful_group_keys_orderings, has_useful_pathkeys, initialize_mergeclause_eclasses,
    make_canonical_pathkey, make_inner_pathkeys_for_merge, make_pathkeys_for_sortclauses,
    make_pathkeys_for_sortclauses_extended, pathkeys_contained_in, pathkeys_count_contained_in,
    select_outer_pathkeys_for_merge, trim_mergeclauses_for_inner_pathkeys,
    truncate_useless_pathkeys, update_mergeclause_eclasses,
};

pub fn add_paths_to_append_rel(
    root: &mut PlannerInfo,
    rel: &mut RelOptInfo,
    live_childrels: &[RelOptInfo],
) {
    unimplemented!()
}
