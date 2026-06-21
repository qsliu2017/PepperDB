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

pub unsafe fn make_one_rel(root: *mut PlannerInfo, joinlist: *mut List) -> *mut RelOptInfo { crate::optimizer::path::allpaths::make_one_rel(root as _, joinlist as _) as _ }

pub unsafe fn standard_join_search(
    root: *mut PlannerInfo,
    levels_needed: c_int,
    initial_rels: *mut List,
) -> *mut RelOptInfo { crate::optimizer::path::allpaths::standard_join_search(root as _, levels_needed, initial_rels as _) as _ }

pub unsafe fn generate_gather_paths(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    override_rows: bool,
) { crate::optimizer::path::allpaths::generate_gather_paths(root as _, rel as _, override_rows) }

pub unsafe fn generate_useful_gather_paths(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    override_rows: bool,
) { crate::optimizer::path::allpaths::generate_useful_gather_paths(root as _, rel as _, override_rows) }

pub unsafe fn compute_parallel_worker(
    rel: *mut RelOptInfo,
    heap_pages: f64,
    index_pages: f64,
    max_workers: c_int,
) -> c_int { crate::optimizer::path::allpaths::compute_parallel_worker(rel as _, heap_pages, index_pages, max_workers) }

pub unsafe fn create_partial_bitmap_paths(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    bitmapqual: *mut Path,
) { crate::optimizer::path::allpaths::create_partial_bitmap_paths(root as _, rel as _, bitmapqual as _) }

pub unsafe fn generate_partitionwise_join_paths(root: *mut PlannerInfo, rel: *mut RelOptInfo) { crate::optimizer::path::allpaths::generate_partitionwise_join_paths(root as _, rel as _) }

/*
 * indxpath.c
 *	  routines to generate index paths
 */
pub unsafe fn create_index_paths(root: *mut PlannerInfo, rel: *mut RelOptInfo) { crate::optimizer::path::allpaths::create_index_paths(root as _, rel as _) }

pub unsafe fn relation_has_unique_index_for(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    restrictlist: *mut List,
    exprlist: *mut List,
    oprlist: *mut List,
) -> bool { crate::optimizer::path::indxpath::relation_has_unique_index_for(root as _, rel as _, restrictlist as _, exprlist as _, oprlist as _) }

pub unsafe fn relation_has_unique_index_ext(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    restrictlist: *mut List,
    exprlist: *mut List,
    oprlist: *mut List,
    extra_clauses: *mut *mut List,
) -> bool { crate::optimizer::path::indxpath::relation_has_unique_index_ext(root as _, rel as _, restrictlist as _, exprlist as _, oprlist as _, extra_clauses as _) }

pub unsafe fn indexcol_is_bool_constant_for_query(
    root: *mut PlannerInfo,
    index: *mut IndexOptInfo,
    indexcol: c_int,
) -> bool { crate::optimizer::path::indxpath::indexcol_is_bool_constant_for_query(root as _, index as _, indexcol) }

pub unsafe fn match_index_to_operand(
    operand: *mut Node,
    indexcol: c_int,
    index: *mut IndexOptInfo,
) -> bool {
    unimplemented!()
}

pub unsafe fn strip_phvs_in_index_operand(operand: *mut Node) -> *mut Node { crate::optimizer::path::indxpath::strip_phvs_in_index_operand(operand as _) as _ }

pub unsafe fn check_index_predicates(root: *mut PlannerInfo, rel: *mut RelOptInfo) { crate::optimizer::path::allpaths::check_index_predicates(root as _, rel as _) }

/*
 * tidpath.c
 *	  routines to generate tid paths
 */
pub unsafe fn create_tidscan_paths(root: *mut PlannerInfo, rel: *mut RelOptInfo) -> bool { crate::optimizer::path::tidpath::create_tidscan_paths(root as _, rel as _) }

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
) { crate::optimizer::path::joinpath::add_paths_to_joinrel(root as _, joinrel as _, outerrel as _, innerrel as _, jointype, sjinfo as _, restrictlist as _) }

/*
 * joinrels.c
 *	  routines to determine which relations to join
 */
pub unsafe fn join_search_one_level(root: *mut PlannerInfo, level: c_int) { crate::optimizer::path::allpaths::join_search_one_level(root as _, level) }

pub unsafe fn make_join_rel(
    root: *mut PlannerInfo,
    rel1: *mut RelOptInfo,
    rel2: *mut RelOptInfo,
) -> *mut RelOptInfo { crate::optimizer::path::joinrels::make_join_rel(root as _, rel1 as _, rel2 as _) as _ }

pub unsafe fn add_outer_joins_to_relids(
    root: *mut PlannerInfo,
    input_relids: Relids,
    sjinfo: *mut SpecialJoinInfo,
    pushed_down_joins: *mut *mut List,
) -> Relids { crate::optimizer::path::joinrels::add_outer_joins_to_relids(root as _, input_relids, sjinfo as _, pushed_down_joins as _) }

pub unsafe fn have_join_order_restriction(
    root: *mut PlannerInfo,
    rel1: *mut RelOptInfo,
    rel2: *mut RelOptInfo,
) -> bool { crate::optimizer::path::joinrels::have_join_order_restriction(root as _, rel1 as _, rel2 as _) }

pub unsafe fn mark_dummy_rel(rel: *mut RelOptInfo) { crate::optimizer::path::allpaths::mark_dummy_rel(rel as _) }

pub unsafe fn init_dummy_sjinfo(
    sjinfo: *mut SpecialJoinInfo,
    left_relids: Relids,
    right_relids: Relids,
) { crate::optimizer::path::costsize::init_dummy_sjinfo(sjinfo as _, left_relids, right_relids) }

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
) -> bool { crate::optimizer::path::equivclass::process_equivalence(root as _, p_restrictinfo as _, jdomain as _) }

pub unsafe fn canonicalize_ec_expression(
    expr: *mut Expr,
    req_type: Oid,
    req_collation: Oid,
) -> *mut Expr { crate::optimizer::path::equivclass::canonicalize_ec_expression(expr as _, req_type, req_collation) as _ }

pub unsafe fn reconsider_outer_join_clauses(root: *mut PlannerInfo) { crate::optimizer::path::equivclass::reconsider_outer_join_clauses(root as _) }

pub unsafe fn rebuild_eclass_attr_needed(root: *mut PlannerInfo) { crate::optimizer::path::equivclass::rebuild_eclass_attr_needed(root as _) }

pub unsafe fn get_eclass_for_sort_expr(
    root: *mut PlannerInfo,
    expr: *mut Expr,
    opfamilies: *mut List,
    opcintype: Oid,
    collation: Oid,
    sortref: Index,
    rel: Relids,
    create_it: bool,
) -> *mut EquivalenceClass { crate::optimizer::path::equivclass::get_eclass_for_sort_expr(root as _, expr as _, opfamilies as _, opcintype, collation, sortref, rel, create_it) as _ }

pub unsafe fn find_ec_member_matching_expr(
    ec: *mut EquivalenceClass,
    expr: *mut Expr,
    relids: Relids,
) -> *mut EquivalenceMember { crate::optimizer::path::equivclass::find_ec_member_matching_expr(ec as _, expr as _, relids) as _ }

pub unsafe fn find_computable_ec_member(
    root: *mut PlannerInfo,
    ec: *mut EquivalenceClass,
    exprs: *mut List,
    relids: Relids,
    require_parallel_safe: bool,
) -> *mut EquivalenceMember { crate::optimizer::path::equivclass::find_computable_ec_member(root as _, ec as _, exprs as _, relids, require_parallel_safe) as _ }

pub unsafe fn relation_can_be_sorted_early(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    ec: *mut EquivalenceClass,
    require_parallel_safe: bool,
) -> bool { crate::optimizer::path::allpaths::relation_can_be_sorted_early(root as _, rel as _, ec as _, require_parallel_safe) }

pub unsafe fn generate_base_implied_equalities(root: *mut PlannerInfo) { crate::optimizer::path::equivclass::generate_base_implied_equalities(root as _) }

pub unsafe fn generate_join_implied_equalities(
    root: *mut PlannerInfo,
    join_relids: Relids,
    outer_relids: Relids,
    inner_rel: *mut RelOptInfo,
    sjinfo: *mut SpecialJoinInfo,
) -> *mut List { crate::optimizer::path::equivclass::generate_join_implied_equalities(root as _, join_relids, outer_relids, inner_rel as _, sjinfo as _) as _ }

pub unsafe fn generate_join_implied_equalities_for_ecs(
    root: *mut PlannerInfo,
    eclasses: *mut List,
    join_relids: Relids,
    outer_relids: Relids,
    inner_rel: *mut RelOptInfo,
) -> *mut List { crate::optimizer::path::equivclass::generate_join_implied_equalities_for_ecs(root as _, eclasses as _, join_relids, outer_relids, inner_rel as _) as _ }

pub unsafe fn exprs_known_equal(
    root: *mut PlannerInfo,
    item1: *mut Node,
    item2: *mut Node,
    opfamily: Oid,
) -> bool { crate::optimizer::path::equivclass::exprs_known_equal(root as _, item1 as _, item2 as _, opfamily) }

pub unsafe fn match_eclasses_to_foreign_key_col(
    root: *mut PlannerInfo,
    fkinfo: *mut ForeignKeyOptInfo,
    colno: c_int,
) -> *mut EquivalenceClass { crate::optimizer::path::equivclass::match_eclasses_to_foreign_key_col(root as _, fkinfo as _, colno) as _ }

pub unsafe fn find_derived_clause_for_ec_member(
    root: *mut PlannerInfo,
    ec: *mut EquivalenceClass,
    em: *mut EquivalenceMember,
) -> *mut RestrictInfo { crate::optimizer::path::costsize::find_derived_clause_for_ec_member(root as _, ec as _, em as _) as _ }

pub unsafe fn add_child_rel_equivalences(
    root: *mut PlannerInfo,
    appinfo: *mut AppendRelInfo,
    parent_rel: *mut RelOptInfo,
    child_rel: *mut RelOptInfo,
) { crate::optimizer::path::allpaths::add_child_rel_equivalences(root as _, appinfo as _, parent_rel as _, child_rel as _) }

pub unsafe fn add_child_join_rel_equivalences(
    root: *mut PlannerInfo,
    nappinfos: c_int,
    appinfos: *mut *mut AppendRelInfo,
    parent_joinrel: *mut RelOptInfo,
    child_joinrel: *mut RelOptInfo,
) { crate::optimizer::path::equivclass::add_child_join_rel_equivalences(root as _, nappinfos, appinfos as _, parent_joinrel as _, child_joinrel as _) }

pub unsafe fn add_setop_child_rel_equivalences(
    root: *mut PlannerInfo,
    child_rel: *mut RelOptInfo,
    child_tlist: *mut List,
    setop_pathkeys: *mut List,
) { crate::optimizer::path::equivclass::add_setop_child_rel_equivalences(root as _, child_rel as _, child_tlist as _, setop_pathkeys as _) }

pub unsafe fn setup_eclass_member_iterator(
    it: *mut EquivalenceMemberIterator,
    ec: *mut EquivalenceClass,
    child_relids: Relids,
) { crate::optimizer::path::equivclass::setup_eclass_member_iterator(it as _, ec as _, child_relids) }

pub unsafe fn eclass_member_iterator_next(
    it: *mut EquivalenceMemberIterator,
) -> *mut EquivalenceMember { crate::optimizer::path::equivclass::eclass_member_iterator_next(it as _) as _ }

pub unsafe fn generate_implied_equalities_for_column(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    callback: ec_matches_callback_type,
    callback_arg: *mut c_void,
    prohibited_rels: Relids,
) -> *mut List { crate::optimizer::path::equivclass::generate_implied_equalities_for_column(root as _, rel as _, core::mem::transmute(callback), callback_arg as _, prohibited_rels) as _ }

pub unsafe fn have_relevant_eclass_joinclause(
    root: *mut PlannerInfo,
    rel1: *mut RelOptInfo,
    rel2: *mut RelOptInfo,
) -> bool { crate::optimizer::util::joininfo::have_relevant_eclass_joinclause(root as _, rel1 as _, rel2 as _) }

pub unsafe fn has_relevant_eclass_joinclause(
    root: *mut PlannerInfo,
    rel1: *mut RelOptInfo,
) -> bool { crate::optimizer::util::joininfo::has_relevant_eclass_joinclause(root as _, rel1 as _) }

pub unsafe fn eclass_useful_for_merging(
    root: *mut PlannerInfo,
    eclass: *mut EquivalenceClass,
    rel: *mut RelOptInfo,
) -> bool { crate::optimizer::path::equivclass::eclass_useful_for_merging(root as _, eclass as _, rel as _) }

pub unsafe fn is_redundant_derived_clause(rinfo: *mut RestrictInfo, clauselist: *mut List) -> bool { crate::optimizer::path::equivclass::is_redundant_derived_clause(rinfo as _, clauselist as _) }

pub unsafe fn is_redundant_with_indexclauses(
    rinfo: *mut RestrictInfo,
    indexclauses: *mut List,
) -> bool { crate::optimizer::path::costsize::is_redundant_with_indexclauses(rinfo as _, indexclauses as _) }

pub unsafe fn ec_clear_derived_clauses(ec: *mut EquivalenceClass) { crate::optimizer::path::equivclass::ec_clear_derived_clauses(ec as _) }

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

pub unsafe fn compare_pathkeys(keys1: *mut List, keys2: *mut List) -> PathKeysComparison { crate::optimizer::path::pathkeys::compare_pathkeys(keys1 as _, keys2 as _) }

pub unsafe fn pathkeys_contained_in(keys1: *mut List, keys2: *mut List) -> bool { crate::optimizer::path::costsize::pathkeys_contained_in(keys1 as _, keys2 as _) }

pub unsafe fn pathkeys_count_contained_in(
    keys1: *mut List,
    keys2: *mut List,
    n_common: *mut c_int,
) -> bool { crate::optimizer::path::pathkeys::pathkeys_count_contained_in(keys1 as _, keys2 as _, n_common as _) }

pub unsafe fn get_useful_group_keys_orderings(root: *mut PlannerInfo, path: *mut Path) -> *mut List { crate::optimizer::path::pathkeys::get_useful_group_keys_orderings(root as _, path as _) as _ }

pub unsafe fn get_cheapest_path_for_pathkeys(
    paths: *mut List,
    pathkeys: *mut List,
    required_outer: Relids,
    cost_criterion: CostSelector,
    require_parallel_safe: bool,
) -> *mut Path { crate::optimizer::path::pathkeys::get_cheapest_path_for_pathkeys(paths as _, pathkeys as _, required_outer, cost_criterion, require_parallel_safe) as _ }

pub unsafe fn get_cheapest_fractional_path_for_pathkeys(
    paths: *mut List,
    pathkeys: *mut List,
    required_outer: Relids,
    fraction: f64,
) -> *mut Path { crate::optimizer::path::pathkeys::get_cheapest_fractional_path_for_pathkeys(paths as _, pathkeys as _, required_outer, fraction) as _ }

pub unsafe fn get_cheapest_parallel_safe_total_inner(paths: *mut List) -> *mut Path { crate::optimizer::path::pathkeys::get_cheapest_parallel_safe_total_inner(paths as _) as _ }

pub unsafe fn build_index_pathkeys(
    root: *mut PlannerInfo,
    index: *mut IndexOptInfo,
    scandir: ScanDirection,
) -> *mut List { crate::optimizer::path::pathkeys::build_index_pathkeys(root as _, index as _, scandir) as _ }

pub unsafe fn build_partition_pathkeys(
    root: *mut PlannerInfo,
    partrel: *mut RelOptInfo,
    scandir: ScanDirection,
    partialkeys: *mut bool,
) -> *mut List { crate::optimizer::path::pathkeys::build_partition_pathkeys(root as _, partrel as _, scandir, partialkeys as _) as _ }

pub unsafe fn build_expression_pathkey(
    root: *mut PlannerInfo,
    expr: *mut Expr,
    opno: Oid,
    rel: Relids,
    create_it: bool,
) -> *mut List { crate::optimizer::path::pathkeys::build_expression_pathkey(root as _, expr as _, opno, rel, create_it) as _ }

pub unsafe fn convert_subquery_pathkeys(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    subquery_pathkeys: *mut List,
    subquery_tlist: *mut List,
) -> *mut List { crate::optimizer::path::pathkeys::convert_subquery_pathkeys(root as _, rel as _, subquery_pathkeys as _, subquery_tlist as _) as _ }

pub unsafe fn build_join_pathkeys(
    root: *mut PlannerInfo,
    joinrel: *mut RelOptInfo,
    jointype: JoinType,
    outer_pathkeys: *mut List,
) -> *mut List { crate::optimizer::path::pathkeys::build_join_pathkeys(root as _, joinrel as _, jointype, outer_pathkeys as _) as _ }

pub unsafe fn make_pathkeys_for_sortclauses(
    root: *mut PlannerInfo,
    sortclauses: *mut List,
    tlist: *mut List,
) -> *mut List { crate::optimizer::path::pathkeys::make_pathkeys_for_sortclauses(root as _, sortclauses as _, tlist as _) as _ }

pub unsafe fn make_pathkeys_for_sortclauses_extended(
    root: *mut PlannerInfo,
    sortclauses: *mut *mut List,
    tlist: *mut List,
    remove_redundant: bool,
    remove_group_rtindex: bool,
    sortable: *mut bool,
    set_ec_sortref: bool,
) -> *mut List { crate::optimizer::path::pathkeys::make_pathkeys_for_sortclauses_extended(root as _, sortclauses as _, tlist as _, remove_redundant, remove_group_rtindex, sortable as _, set_ec_sortref) as _ }

pub unsafe fn initialize_mergeclause_eclasses(root: *mut PlannerInfo, restrictinfo: *mut RestrictInfo) { crate::optimizer::path::pathkeys::initialize_mergeclause_eclasses(root as _, restrictinfo as _) }

pub unsafe fn update_mergeclause_eclasses(root: *mut PlannerInfo, restrictinfo: *mut RestrictInfo) { crate::optimizer::path::pathkeys::update_mergeclause_eclasses(root as _, restrictinfo as _) }

pub unsafe fn find_mergeclauses_for_outer_pathkeys(
    root: *mut PlannerInfo,
    pathkeys: *mut List,
    restrictinfos: *mut List,
) -> *mut List { crate::optimizer::path::pathkeys::find_mergeclauses_for_outer_pathkeys(root as _, pathkeys as _, restrictinfos as _) as _ }

pub unsafe fn select_outer_pathkeys_for_merge(
    root: *mut PlannerInfo,
    mergeclauses: *mut List,
    joinrel: *mut RelOptInfo,
) -> *mut List { crate::optimizer::path::pathkeys::select_outer_pathkeys_for_merge(root as _, mergeclauses as _, joinrel as _) as _ }

pub unsafe fn make_inner_pathkeys_for_merge(
    root: *mut PlannerInfo,
    mergeclauses: *mut List,
    outer_pathkeys: *mut List,
) -> *mut List { crate::optimizer::path::pathkeys::make_inner_pathkeys_for_merge(root as _, mergeclauses as _, outer_pathkeys as _) as _ }

pub unsafe fn trim_mergeclauses_for_inner_pathkeys(
    root: *mut PlannerInfo,
    mergeclauses: *mut List,
    pathkeys: *mut List,
) -> *mut List { crate::optimizer::path::pathkeys::trim_mergeclauses_for_inner_pathkeys(root as _, mergeclauses as _, pathkeys as _) as _ }

pub unsafe fn truncate_useless_pathkeys(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    pathkeys: *mut List,
) -> *mut List { crate::optimizer::path::pathkeys::truncate_useless_pathkeys(root as _, rel as _, pathkeys as _) as _ }

pub unsafe fn has_useful_pathkeys(root: *mut PlannerInfo, rel: *mut RelOptInfo) -> bool { crate::optimizer::path::pathkeys::has_useful_pathkeys(root as _, rel as _) }

pub unsafe fn append_pathkeys(target: *mut List, source: *mut List) -> *mut List { crate::optimizer::path::pathkeys::append_pathkeys(target as _, source as _) as _ }

pub unsafe fn make_canonical_pathkey(
    root: *mut PlannerInfo,
    eclass: *mut EquivalenceClass,
    opfamily: Oid,
    cmptype: CompareType,
    nulls_first: bool,
) -> *mut PathKey { crate::optimizer::path::pathkeys::make_canonical_pathkey(root as _, eclass as _, opfamily, cmptype, nulls_first) as _ }

pub unsafe fn add_paths_to_append_rel(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    live_childrels: *mut List,
) { crate::optimizer::path::allpaths::add_paths_to_append_rel(root as _, rel as _, live_childrels as _) }
