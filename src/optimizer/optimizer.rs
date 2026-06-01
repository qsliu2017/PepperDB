//! optimizer.h - External API for the Postgres planner.
//!
//! This header is meant to define everything that the core planner exposes for
//! use by non-planner modules.

use std::ffi::{c_char, c_int, c_long};

use crate::c::{int32, int64, Index};
use crate::nodes::bitmapset::Bitmapset;
use crate::nodes::nodes::{Cardinality, JoinType, Node, Selectivity};
use crate::nodes::parsenodes::{Query, SortGroupClause};
use crate::nodes::pg_list::List;
use crate::nodes::primnodes::{Expr, ScalarArrayOpExpr, TargetEntry};
use crate::postgres_ext::Oid;

// We don't want to include nodes/pathnodes.h here, because non-planner code
// should generally treat PlannerInfo as an opaque typedef.  Likewise for
// IndexOptInfo and SpecialJoinInfo.
// TODO: dedup when pathnodes.h lands.
#[repr(C)]
pub struct PlannerInfo {
    _opaque: [u8; 0],
}

// TODO: dedup when pathnodes.h lands.
#[repr(C)]
pub struct IndexOptInfo {
    _opaque: [u8; 0],
}

// TODO: dedup when pathnodes.h lands.
#[repr(C)]
pub struct SpecialJoinInfo {
    _opaque: [u8; 0],
}

// It also seems best not to include plannodes.h, params.h, or htup.h here.
// Forward-declared opaque structs.
// TODO: dedup when plannodes.h / params.h / htup.h land.
#[repr(C)]
pub struct PlannedStmt {
    _opaque: [u8; 0],
}

// TODO: dedup when params.h lands.
#[repr(C)]
pub struct ParamListInfoData {
    _opaque: [u8; 0],
}

// TODO: dedup when htup.h lands.
#[repr(C)]
pub struct HeapTupleData {
    _opaque: [u8; 0],
}

/* in path/clausesel.c: */

pub unsafe fn clause_selectivity(
    root: *mut PlannerInfo,
    clause: *mut Node,
    varRelid: c_int,
    jointype: JoinType,
    sjinfo: *mut SpecialJoinInfo,
) -> Selectivity {
    unimplemented!()
}

pub unsafe fn clause_selectivity_ext(
    root: *mut PlannerInfo,
    clause: *mut Node,
    varRelid: c_int,
    jointype: JoinType,
    sjinfo: *mut SpecialJoinInfo,
    use_extended_stats: bool,
) -> Selectivity {
    unimplemented!()
}

pub unsafe fn clauselist_selectivity(
    root: *mut PlannerInfo,
    clauses: *mut List,
    varRelid: c_int,
    jointype: JoinType,
    sjinfo: *mut SpecialJoinInfo,
) -> Selectivity {
    unimplemented!()
}

pub unsafe fn clauselist_selectivity_ext(
    root: *mut PlannerInfo,
    clauses: *mut List,
    varRelid: c_int,
    jointype: JoinType,
    sjinfo: *mut SpecialJoinInfo,
    use_extended_stats: bool,
) -> Selectivity {
    unimplemented!()
}

/* in path/costsize.c: */

/* widely used cost parameters */
pub static mut seq_page_cost: f64 = 0.0;
pub static mut random_page_cost: f64 = 0.0;
pub static mut cpu_tuple_cost: f64 = 0.0;
pub static mut cpu_index_tuple_cost: f64 = 0.0;
pub static mut cpu_operator_cost: f64 = 0.0;
pub static mut parallel_tuple_cost: f64 = 0.0;
pub static mut parallel_setup_cost: f64 = 0.0;
pub static mut recursive_worktable_factor: f64 = 0.0;
pub static mut effective_cache_size: c_int = 0;

pub unsafe fn clamp_row_est(nrows: f64) -> f64 {
    unimplemented!()
}

pub unsafe fn clamp_width_est(tuple_width: int64) -> int32 {
    unimplemented!()
}

pub unsafe fn clamp_cardinality_to_long(x: Cardinality) -> c_long {
    unimplemented!()
}

/* in path/indxpath.c: */

pub unsafe fn is_pseudo_constant_for_index(
    root: *mut PlannerInfo,
    expr: *mut Node,
    index: *mut IndexOptInfo,
) -> bool {
    unimplemented!()
}

/* in plan/planner.c: */

/* possible values for debug_parallel_query */
pub type DebugParallelMode = c_int;
pub const DEBUG_PARALLEL_OFF: DebugParallelMode = 0;
pub const DEBUG_PARALLEL_ON: DebugParallelMode = 1;
pub const DEBUG_PARALLEL_REGRESS: DebugParallelMode = 2;

/* GUC parameters */
pub static mut debug_parallel_query: c_int = 0;
pub static mut parallel_leader_participation: bool = false;
pub static mut enable_distinct_reordering: bool = false;

pub unsafe fn planner(
    parse: *mut Query,
    query_string: *const c_char,
    cursorOptions: c_int,
    boundParams: *mut ParamListInfoData,
) -> *mut PlannedStmt {
    unimplemented!()
}

pub unsafe fn expression_planner(expr: *mut Expr) -> *mut Expr {
    unimplemented!()
}

pub unsafe fn expression_planner_with_deps(
    expr: *mut Expr,
    relationOids: *mut *mut List,
    invalItems: *mut *mut List,
) -> *mut Expr {
    unimplemented!()
}

pub unsafe fn plan_cluster_use_sort(tableOid: Oid, indexOid: Oid) -> bool {
    unimplemented!()
}

pub unsafe fn plan_create_index_workers(tableOid: Oid, indexOid: Oid) -> c_int {
    unimplemented!()
}

/* in plan/setrefs.c: */

pub unsafe fn extract_query_dependencies(
    query: *mut Node,
    relationOids: *mut *mut List,
    invalItems: *mut *mut List,
    hasRowSecurity: *mut bool,
) {
    unimplemented!()
}

/* in prep/prepqual.c: */

pub unsafe fn negate_clause(node: *mut Node) -> *mut Node {
    unimplemented!()
}

pub unsafe fn canonicalize_qual(qual: *mut Expr, is_check: bool) -> *mut Expr {
    unimplemented!()
}

/* in util/clauses.c: */

pub unsafe fn contain_mutable_functions(clause: *mut Node) -> bool {
    unimplemented!()
}

pub unsafe fn contain_mutable_functions_after_planning(expr: *mut Expr) -> bool {
    unimplemented!()
}

pub unsafe fn contain_volatile_functions(clause: *mut Node) -> bool {
    unimplemented!()
}

pub unsafe fn contain_volatile_functions_after_planning(expr: *mut Expr) -> bool {
    unimplemented!()
}

pub unsafe fn contain_volatile_functions_not_nextval(clause: *mut Node) -> bool {
    unimplemented!()
}

pub unsafe fn eval_const_expressions(root: *mut PlannerInfo, node: *mut Node) -> *mut Node {
    unimplemented!()
}

pub unsafe fn convert_saop_to_hashed_saop(node: *mut Node) {
    unimplemented!()
}

pub unsafe fn estimate_expression_value(root: *mut PlannerInfo, node: *mut Node) -> *mut Node {
    unimplemented!()
}

pub unsafe fn evaluate_expr(
    expr: *mut Expr,
    result_type: Oid,
    result_typmod: int32,
    result_collation: Oid,
) -> *mut Expr {
    unimplemented!()
}

pub unsafe fn expand_function_arguments(
    args: *mut List,
    include_out_arguments: bool,
    result_type: Oid,
    func_tuple: *mut HeapTupleData,
) -> *mut List {
    unimplemented!()
}

pub unsafe fn make_SAOP_expr(
    oper: Oid,
    leftexpr: *mut Node,
    coltype: Oid,
    arraycollid: Oid,
    inputcollid: Oid,
    exprs: *mut List,
    haveNonConst: bool,
) -> *mut ScalarArrayOpExpr {
    unimplemented!()
}

/* in util/predtest.c: */

pub unsafe fn predicate_implied_by(
    predicate_list: *mut List,
    clause_list: *mut List,
    weak: bool,
) -> bool {
    unimplemented!()
}

pub unsafe fn predicate_refuted_by(
    predicate_list: *mut List,
    clause_list: *mut List,
    weak: bool,
) -> bool {
    unimplemented!()
}

/* in util/tlist.c: */

pub unsafe fn count_nonjunk_tlist_entries(tlist: *mut List) -> c_int {
    unimplemented!()
}

pub unsafe fn get_sortgroupref_tle(sortref: Index, targetList: *mut List) -> *mut TargetEntry {
    unimplemented!()
}

pub unsafe fn get_sortgroupclause_tle(
    sgClause: *mut SortGroupClause,
    targetList: *mut List,
) -> *mut TargetEntry {
    unimplemented!()
}

pub unsafe fn get_sortgroupclause_expr(
    sgClause: *mut SortGroupClause,
    targetList: *mut List,
) -> *mut Node {
    unimplemented!()
}

pub unsafe fn get_sortgrouplist_exprs(sgClauses: *mut List, targetList: *mut List) -> *mut List {
    unimplemented!()
}

pub unsafe fn get_sortgroupref_clause(
    sortref: Index,
    clauses: *mut List,
) -> *mut SortGroupClause {
    unimplemented!()
}

pub unsafe fn get_sortgroupref_clause_noerr(
    sortref: Index,
    clauses: *mut List,
) -> *mut SortGroupClause {
    unimplemented!()
}

/* in util/var.c: */

/* Bits that can be OR'd into the flags argument of pull_var_clause() */
/// include Aggrefs in output list
pub const PVC_INCLUDE_AGGREGATES: c_int = 0x0001;
/// recurse into Aggref arguments
pub const PVC_RECURSE_AGGREGATES: c_int = 0x0002;
/// include WindowFuncs in output list
pub const PVC_INCLUDE_WINDOWFUNCS: c_int = 0x0004;
/// recurse into WindowFunc arguments
pub const PVC_RECURSE_WINDOWFUNCS: c_int = 0x0008;
/// include PlaceHolderVars in output list
pub const PVC_INCLUDE_PLACEHOLDERS: c_int = 0x0010;
/// recurse into PlaceHolderVar arguments
pub const PVC_RECURSE_PLACEHOLDERS: c_int = 0x0020;
/// include ConvertRowtypeExprs in output list
pub const PVC_INCLUDE_CONVERTROWTYPES: c_int = 0x0040;

pub unsafe fn pull_varnos(root: *mut PlannerInfo, node: *mut Node) -> *mut Bitmapset {
    unimplemented!()
}

pub unsafe fn pull_varnos_of_level(
    root: *mut PlannerInfo,
    node: *mut Node,
    levelsup: c_int,
) -> *mut Bitmapset {
    unimplemented!()
}

pub unsafe fn pull_varattnos(node: *mut Node, varno: Index, varattnos: *mut *mut Bitmapset) {
    unimplemented!()
}

pub unsafe fn pull_vars_of_level(node: *mut Node, levelsup: c_int) -> *mut List {
    unimplemented!()
}

pub unsafe fn contain_var_clause(node: *mut Node) -> bool {
    unimplemented!()
}

pub unsafe fn contain_vars_of_level(node: *mut Node, levelsup: c_int) -> bool {
    unimplemented!()
}

pub unsafe fn contain_vars_returning_old_or_new(node: *mut Node) -> bool {
    unimplemented!()
}

pub unsafe fn locate_var_of_level(node: *mut Node, levelsup: c_int) -> c_int {
    unimplemented!()
}

pub unsafe fn pull_var_clause(node: *mut Node, flags: c_int) -> *mut List {
    unimplemented!()
}

pub unsafe fn flatten_join_alias_vars(
    root: *mut PlannerInfo,
    query: *mut Query,
    node: *mut Node,
) -> *mut Node {
    unimplemented!()
}

pub unsafe fn flatten_group_exprs(
    root: *mut PlannerInfo,
    query: *mut Query,
    node: *mut Node,
) -> *mut Node {
    unimplemented!()
}
