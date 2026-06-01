//! paths.h - prototypes for various files in optimizer/path
//
// Faithful 1:1 translation of postgres/src/include/optimizer/paths.h.
// Function prototypes become `pub unsafe fn ... { unimplemented!() }`.

use std::ffi::c_int;
use std::ffi::c_void;

use crate::c::Index;
use crate::nodes::nodes::{JoinType, Node};
use crate::nodes::pathnodes::{
    AppendRelInfo, CostSelector, EquivalenceClass, EquivalenceMember,
    EquivalenceMemberIterator, ForeignKeyOptInfo, IndexOptInfo, JoinDomain,
    JoinPathExtraData, Path, PathKey, PlannerInfo, RelOptInfo, Relids,
    RestrictInfo, SpecialJoinInfo,
};
use crate::nodes::parsenodes::RangeTblEntry;
use crate::nodes::pg_list::List;
use crate::nodes::primnodes::Expr;
use crate::postgres_ext::Oid;

// ScanDirection lives in access/sdir.h, CompareType in access/cmptype.h.
use crate::access::sdir::ScanDirection;
use crate::access::cmptype::CompareType;

/*
 * allpaths.c
 */
// extern PGDLLIMPORT bool enable_geqo;
pub static mut enable_geqo: bool = false;
// extern PGDLLIMPORT int geqo_threshold;
pub static mut geqo_threshold: c_int = 0;
// extern PGDLLIMPORT int min_parallel_table_scan_size;
pub static mut min_parallel_table_scan_size: c_int = 0;
// extern PGDLLIMPORT int min_parallel_index_scan_size;
pub static mut min_parallel_index_scan_size: c_int = 0;
// extern PGDLLIMPORT bool enable_group_by_reordering;
pub static mut enable_group_by_reordering: bool = false;

/* Hook for plugins to get control in set_rel_pathlist() */
pub type set_rel_pathlist_hook_type = Option<
    unsafe extern "C" fn(
        root: *mut PlannerInfo,
        rel: *mut RelOptInfo,
        rti: Index,
        rte: *mut RangeTblEntry,
    ),
>;
// extern PGDLLIMPORT set_rel_pathlist_hook_type set_rel_pathlist_hook;
pub static mut set_rel_pathlist_hook: set_rel_pathlist_hook_type = None;

/* Hook for plugins to get control in add_paths_to_joinrel() */
pub type set_join_pathlist_hook_type = Option<
    unsafe extern "C" fn(
        root: *mut PlannerInfo,
        joinrel: *mut RelOptInfo,
        outerrel: *mut RelOptInfo,
        innerrel: *mut RelOptInfo,
        jointype: JoinType,
        extra: *mut JoinPathExtraData,
    ),
>;
// extern PGDLLIMPORT set_join_pathlist_hook_type set_join_pathlist_hook;
pub static mut set_join_pathlist_hook: set_join_pathlist_hook_type = None;

/* Hook for plugins to replace standard_join_search() */
pub type join_search_hook_type = Option<
    unsafe extern "C" fn(
        root: *mut PlannerInfo,
        levels_needed: c_int,
        initial_rels: *mut List,
    ) -> *mut RelOptInfo,
>;
// extern PGDLLIMPORT join_search_hook_type join_search_hook;
pub static mut join_search_hook: join_search_hook_type = None;

pub unsafe fn make_one_rel(root: *mut PlannerInfo, joinlist: *mut List) -> *mut RelOptInfo {
    unimplemented!()
}

pub unsafe fn standard_join_search(
    root: *mut PlannerInfo,
    levels_needed: c_int,
    initial_rels: *mut List,
) -> *mut RelOptInfo {
    unimplemented!()
}

pub unsafe fn generate_gather_paths(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    override_rows: bool,
) {
    unimplemented!()
}

pub unsafe fn generate_useful_gather_paths(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    override_rows: bool,
) {
    unimplemented!()
}

pub unsafe fn compute_parallel_worker(
    rel: *mut RelOptInfo,
    heap_pages: f64,
    index_pages: f64,
    max_workers: c_int,
) -> c_int {
    unimplemented!()
}

pub unsafe fn create_partial_bitmap_paths(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    bitmapqual: *mut Path,
) {
    unimplemented!()
}

pub unsafe fn generate_partitionwise_join_paths(root: *mut PlannerInfo, rel: *mut RelOptInfo) {
    unimplemented!()
}

/*
 * indxpath.c
 *	  routines to generate index paths
 */
pub unsafe fn create_index_paths(root: *mut PlannerInfo, rel: *mut RelOptInfo) {
    unimplemented!()
}

pub unsafe fn relation_has_unique_index_for(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    restrictlist: *mut List,
    exprlist: *mut List,
    oprlist: *mut List,
) -> bool {
    unimplemented!()
}

pub unsafe fn relation_has_unique_index_ext(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    restrictlist: *mut List,
    exprlist: *mut List,
    oprlist: *mut List,
    extra_clauses: *mut *mut List,
) -> bool {
    unimplemented!()
}

pub unsafe fn indexcol_is_bool_constant_for_query(
    root: *mut PlannerInfo,
    index: *mut IndexOptInfo,
    indexcol: c_int,
) -> bool {
    unimplemented!()
}

pub unsafe fn match_index_to_operand(
    operand: *mut Node,
    indexcol: c_int,
    index: *mut IndexOptInfo,
) -> bool {
    unimplemented!()
}

pub unsafe fn strip_phvs_in_index_operand(operand: *mut Node) -> *mut Node {
    unimplemented!()
}

pub unsafe fn check_index_predicates(root: *mut PlannerInfo, rel: *mut RelOptInfo) {
    unimplemented!()
}

/*
 * tidpath.c
 *	  routines to generate tid paths
 */
pub unsafe fn create_tidscan_paths(root: *mut PlannerInfo, rel: *mut RelOptInfo) -> bool {
    unimplemented!()
}

/*
 * joinpath.c
 *	   routines to create join paths
 */
pub unsafe fn add_paths_to_joinrel(
    root: *mut PlannerInfo,
    joinrel: *mut RelOptInfo,
    outerrel: *mut RelOptInfo,
    innerrel: *mut RelOptInfo,
    jointype: JoinType,
    sjinfo: *mut SpecialJoinInfo,
    restrictlist: *mut List,
) {
    unimplemented!()
}

/*
 * joinrels.c
 *	  routines to determine which relations to join
 */
pub unsafe fn join_search_one_level(root: *mut PlannerInfo, level: c_int) {
    unimplemented!()
}

pub unsafe fn make_join_rel(
    root: *mut PlannerInfo,
    rel1: *mut RelOptInfo,
    rel2: *mut RelOptInfo,
) -> *mut RelOptInfo {
    unimplemented!()
}

pub unsafe fn add_outer_joins_to_relids(
    root: *mut PlannerInfo,
    input_relids: Relids,
    sjinfo: *mut SpecialJoinInfo,
    pushed_down_joins: *mut *mut List,
) -> Relids {
    unimplemented!()
}

pub unsafe fn have_join_order_restriction(
    root: *mut PlannerInfo,
    rel1: *mut RelOptInfo,
    rel2: *mut RelOptInfo,
) -> bool {
    unimplemented!()
}

pub unsafe fn mark_dummy_rel(rel: *mut RelOptInfo) {
    unimplemented!()
}

pub unsafe fn init_dummy_sjinfo(
    sjinfo: *mut SpecialJoinInfo,
    left_relids: Relids,
    right_relids: Relids,
) {
    unimplemented!()
}

/*
 * equivclass.c
 *	  routines for managing EquivalenceClasses
 */
pub type ec_matches_callback_type = Option<
    unsafe extern "C" fn(
        root: *mut PlannerInfo,
        rel: *mut RelOptInfo,
        ec: *mut EquivalenceClass,
        em: *mut EquivalenceMember,
        arg: *mut c_void,
    ) -> bool,
>;

pub unsafe fn process_equivalence(
    root: *mut PlannerInfo,
    p_restrictinfo: *mut *mut RestrictInfo,
    jdomain: *mut JoinDomain,
) -> bool {
    unimplemented!()
}

pub unsafe fn canonicalize_ec_expression(
    expr: *mut Expr,
    req_type: Oid,
    req_collation: Oid,
) -> *mut Expr {
    unimplemented!()
}

pub unsafe fn reconsider_outer_join_clauses(root: *mut PlannerInfo) {
    unimplemented!()
}

pub unsafe fn rebuild_eclass_attr_needed(root: *mut PlannerInfo) {
    unimplemented!()
}

pub unsafe fn get_eclass_for_sort_expr(
    root: *mut PlannerInfo,
    expr: *mut Expr,
    opfamilies: *mut List,
    opcintype: Oid,
    collation: Oid,
    sortref: Index,
    rel: Relids,
    create_it: bool,
) -> *mut EquivalenceClass {
    unimplemented!()
}

pub unsafe fn find_ec_member_matching_expr(
    ec: *mut EquivalenceClass,
    expr: *mut Expr,
    relids: Relids,
) -> *mut EquivalenceMember {
    unimplemented!()
}

pub unsafe fn find_computable_ec_member(
    root: *mut PlannerInfo,
    ec: *mut EquivalenceClass,
    exprs: *mut List,
    relids: Relids,
    require_parallel_safe: bool,
) -> *mut EquivalenceMember {
    unimplemented!()
}

pub unsafe fn relation_can_be_sorted_early(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    ec: *mut EquivalenceClass,
    require_parallel_safe: bool,
) -> bool {
    unimplemented!()
}

pub unsafe fn generate_base_implied_equalities(root: *mut PlannerInfo) {
    unimplemented!()
}

pub unsafe fn generate_join_implied_equalities(
    root: *mut PlannerInfo,
    join_relids: Relids,
    outer_relids: Relids,
    inner_rel: *mut RelOptInfo,
    sjinfo: *mut SpecialJoinInfo,
) -> *mut List {
    unimplemented!()
}

pub unsafe fn generate_join_implied_equalities_for_ecs(
    root: *mut PlannerInfo,
    eclasses: *mut List,
    join_relids: Relids,
    outer_relids: Relids,
    inner_rel: *mut RelOptInfo,
) -> *mut List {
    unimplemented!()
}

pub unsafe fn exprs_known_equal(
    root: *mut PlannerInfo,
    item1: *mut Node,
    item2: *mut Node,
    opfamily: Oid,
) -> bool {
    unimplemented!()
}

pub unsafe fn match_eclasses_to_foreign_key_col(
    root: *mut PlannerInfo,
    fkinfo: *mut ForeignKeyOptInfo,
    colno: c_int,
) -> *mut EquivalenceClass {
    unimplemented!()
}

pub unsafe fn find_derived_clause_for_ec_member(
    root: *mut PlannerInfo,
    ec: *mut EquivalenceClass,
    em: *mut EquivalenceMember,
) -> *mut RestrictInfo {
    unimplemented!()
}

pub unsafe fn add_child_rel_equivalences(
    root: *mut PlannerInfo,
    appinfo: *mut AppendRelInfo,
    parent_rel: *mut RelOptInfo,
    child_rel: *mut RelOptInfo,
) {
    unimplemented!()
}

pub unsafe fn add_child_join_rel_equivalences(
    root: *mut PlannerInfo,
    nappinfos: c_int,
    appinfos: *mut *mut AppendRelInfo,
    parent_joinrel: *mut RelOptInfo,
    child_joinrel: *mut RelOptInfo,
) {
    unimplemented!()
}

pub unsafe fn add_setop_child_rel_equivalences(
    root: *mut PlannerInfo,
    child_rel: *mut RelOptInfo,
    child_tlist: *mut List,
    setop_pathkeys: *mut List,
) {
    unimplemented!()
}

pub unsafe fn setup_eclass_member_iterator(
    it: *mut EquivalenceMemberIterator,
    ec: *mut EquivalenceClass,
    child_relids: Relids,
) {
    unimplemented!()
}

pub unsafe fn eclass_member_iterator_next(
    it: *mut EquivalenceMemberIterator,
) -> *mut EquivalenceMember {
    unimplemented!()
}

pub unsafe fn generate_implied_equalities_for_column(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    callback: ec_matches_callback_type,
    callback_arg: *mut c_void,
    prohibited_rels: Relids,
) -> *mut List {
    unimplemented!()
}

pub unsafe fn have_relevant_eclass_joinclause(
    root: *mut PlannerInfo,
    rel1: *mut RelOptInfo,
    rel2: *mut RelOptInfo,
) -> bool {
    unimplemented!()
}

pub unsafe fn has_relevant_eclass_joinclause(
    root: *mut PlannerInfo,
    rel1: *mut RelOptInfo,
) -> bool {
    unimplemented!()
}

pub unsafe fn eclass_useful_for_merging(
    root: *mut PlannerInfo,
    eclass: *mut EquivalenceClass,
    rel: *mut RelOptInfo,
) -> bool {
    unimplemented!()
}

pub unsafe fn is_redundant_derived_clause(rinfo: *mut RestrictInfo, clauselist: *mut List) -> bool {
    unimplemented!()
}

pub unsafe fn is_redundant_with_indexclauses(
    rinfo: *mut RestrictInfo,
    indexclauses: *mut List,
) -> bool {
    unimplemented!()
}

pub unsafe fn ec_clear_derived_clauses(ec: *mut EquivalenceClass) {
    unimplemented!()
}

/*
 * pathkeys.c
 *	  utilities for matching and building path keys
 */
// typedef enum PathKeysComparison (project convention: c_int + consts)
pub type PathKeysComparison = c_int;
pub const PATHKEYS_EQUAL: PathKeysComparison = 0; /* pathkeys are identical */
pub const PATHKEYS_BETTER1: PathKeysComparison = 1; /* pathkey 1 is a superset of pathkey 2 */
pub const PATHKEYS_BETTER2: PathKeysComparison = 2; /* vice versa */
pub const PATHKEYS_DIFFERENT: PathKeysComparison = 3; /* neither pathkey includes the other */

pub unsafe fn compare_pathkeys(keys1: *mut List, keys2: *mut List) -> PathKeysComparison {
    unimplemented!()
}

pub unsafe fn pathkeys_contained_in(keys1: *mut List, keys2: *mut List) -> bool {
    unimplemented!()
}

pub unsafe fn pathkeys_count_contained_in(
    keys1: *mut List,
    keys2: *mut List,
    n_common: *mut c_int,
) -> bool {
    unimplemented!()
}

pub unsafe fn get_useful_group_keys_orderings(root: *mut PlannerInfo, path: *mut Path) -> *mut List {
    unimplemented!()
}

pub unsafe fn get_cheapest_path_for_pathkeys(
    paths: *mut List,
    pathkeys: *mut List,
    required_outer: Relids,
    cost_criterion: CostSelector,
    require_parallel_safe: bool,
) -> *mut Path {
    unimplemented!()
}

pub unsafe fn get_cheapest_fractional_path_for_pathkeys(
    paths: *mut List,
    pathkeys: *mut List,
    required_outer: Relids,
    fraction: f64,
) -> *mut Path {
    unimplemented!()
}

pub unsafe fn get_cheapest_parallel_safe_total_inner(paths: *mut List) -> *mut Path {
    unimplemented!()
}

pub unsafe fn build_index_pathkeys(
    root: *mut PlannerInfo,
    index: *mut IndexOptInfo,
    scandir: ScanDirection,
) -> *mut List {
    unimplemented!()
}

pub unsafe fn build_partition_pathkeys(
    root: *mut PlannerInfo,
    partrel: *mut RelOptInfo,
    scandir: ScanDirection,
    partialkeys: *mut bool,
) -> *mut List {
    unimplemented!()
}

pub unsafe fn build_expression_pathkey(
    root: *mut PlannerInfo,
    expr: *mut Expr,
    opno: Oid,
    rel: Relids,
    create_it: bool,
) -> *mut List {
    unimplemented!()
}

pub unsafe fn convert_subquery_pathkeys(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    subquery_pathkeys: *mut List,
    subquery_tlist: *mut List,
) -> *mut List {
    unimplemented!()
}

pub unsafe fn build_join_pathkeys(
    root: *mut PlannerInfo,
    joinrel: *mut RelOptInfo,
    jointype: JoinType,
    outer_pathkeys: *mut List,
) -> *mut List {
    unimplemented!()
}

pub unsafe fn make_pathkeys_for_sortclauses(
    root: *mut PlannerInfo,
    sortclauses: *mut List,
    tlist: *mut List,
) -> *mut List {
    unimplemented!()
}

pub unsafe fn make_pathkeys_for_sortclauses_extended(
    root: *mut PlannerInfo,
    sortclauses: *mut *mut List,
    tlist: *mut List,
    remove_redundant: bool,
    remove_group_rtindex: bool,
    sortable: *mut bool,
    set_ec_sortref: bool,
) -> *mut List {
    unimplemented!()
}

pub unsafe fn initialize_mergeclause_eclasses(root: *mut PlannerInfo, restrictinfo: *mut RestrictInfo) {
    unimplemented!()
}

pub unsafe fn update_mergeclause_eclasses(root: *mut PlannerInfo, restrictinfo: *mut RestrictInfo) {
    unimplemented!()
}

pub unsafe fn find_mergeclauses_for_outer_pathkeys(
    root: *mut PlannerInfo,
    pathkeys: *mut List,
    restrictinfos: *mut List,
) -> *mut List {
    unimplemented!()
}

pub unsafe fn select_outer_pathkeys_for_merge(
    root: *mut PlannerInfo,
    mergeclauses: *mut List,
    joinrel: *mut RelOptInfo,
) -> *mut List {
    unimplemented!()
}

pub unsafe fn make_inner_pathkeys_for_merge(
    root: *mut PlannerInfo,
    mergeclauses: *mut List,
    outer_pathkeys: *mut List,
) -> *mut List {
    unimplemented!()
}

pub unsafe fn trim_mergeclauses_for_inner_pathkeys(
    root: *mut PlannerInfo,
    mergeclauses: *mut List,
    pathkeys: *mut List,
) -> *mut List {
    unimplemented!()
}

pub unsafe fn truncate_useless_pathkeys(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    pathkeys: *mut List,
) -> *mut List {
    unimplemented!()
}

pub unsafe fn has_useful_pathkeys(root: *mut PlannerInfo, rel: *mut RelOptInfo) -> bool {
    unimplemented!()
}

pub unsafe fn append_pathkeys(target: *mut List, source: *mut List) -> *mut List {
    unimplemented!()
}

pub unsafe fn make_canonical_pathkey(
    root: *mut PlannerInfo,
    eclass: *mut EquivalenceClass,
    opfamily: Oid,
    cmptype: CompareType,
    nulls_first: bool,
) -> *mut PathKey {
    unimplemented!()
}

pub unsafe fn add_paths_to_append_rel(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    live_childrels: *mut List,
) {
    unimplemented!()
}
