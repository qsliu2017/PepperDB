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
    crate::optimizer::path::clausesel::clause_selectivity(
        root as _,
        clause as _,
        varRelid as _,
        jointype,
        sjinfo as _,
    ) as _
}

pub unsafe fn clause_selectivity_ext(
    root: *mut PlannerInfo,
    clause: *mut Node,
    varRelid: c_int,
    jointype: JoinType,
    sjinfo: *mut SpecialJoinInfo,
    use_extended_stats: bool,
) -> Selectivity {
    crate::optimizer::path::clausesel::clause_selectivity_ext(
        root as _,
        clause as _,
        varRelid as _,
        jointype,
        sjinfo as _,
        use_extended_stats,
    ) as _
}

pub unsafe fn clauselist_selectivity(
    root: *mut PlannerInfo,
    clauses: *mut List,
    varRelid: c_int,
    jointype: JoinType,
    sjinfo: *mut SpecialJoinInfo,
) -> Selectivity {
    crate::optimizer::path::clausesel::clauselist_selectivity(
        root as _,
        clauses as _,
        varRelid as _,
        jointype,
        sjinfo as _,
    ) as _
}

pub unsafe fn clauselist_selectivity_ext(
    root: *mut PlannerInfo,
    clauses: *mut List,
    varRelid: c_int,
    jointype: JoinType,
    sjinfo: *mut SpecialJoinInfo,
    use_extended_stats: bool,
) -> Selectivity {
    crate::optimizer::path::clausesel::clauselist_selectivity_ext(
        root as _,
        clauses as _,
        varRelid as _,
        jointype,
        sjinfo as _,
        use_extended_stats,
    ) as _
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
    crate::optimizer::path::costsize::clamp_row_est(nrows)
}

pub unsafe fn clamp_width_est(tuple_width: int64) -> int32 {
    const MAX_ALLOC_SIZE: int64 = 0x3fffffff;
    if tuple_width < 0 {
        0
    } else if tuple_width > MAX_ALLOC_SIZE {
        MAX_ALLOC_SIZE as int32
    } else {
        tuple_width as int32
    }
}

pub unsafe fn clamp_cardinality_to_long(x: Cardinality) -> c_long {
    crate::optimizer::path::costsize::clamp_cardinality_to_long(x as _) as _
}

/* in path/indxpath.c: */

pub unsafe fn is_pseudo_constant_for_index(
    root: *mut PlannerInfo,
    expr: *mut Node,
    index: *mut IndexOptInfo,
) -> bool {
    crate::optimizer::path::indxpath::is_pseudo_constant_for_index(root as _, expr as _, index as _)
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
    crate::optimizer::plan::planner::planner(
        parse as _,
        query_string as _,
        cursorOptions as _,
        boundParams as _,
    ) as _
}

pub unsafe fn expression_planner(expr: *mut Expr) -> *mut Expr {
    crate::optimizer::plan::planner::expression_planner(expr as _) as _
}

pub unsafe fn expression_planner_with_deps(
    expr: *mut Expr,
    relationOids: *mut *mut List,
    invalItems: *mut *mut List,
) -> *mut Expr {
    crate::optimizer::plan::planner::expression_planner_with_deps(
        expr as _,
        relationOids as _,
        invalItems as _,
    ) as _
}

pub unsafe fn plan_cluster_use_sort(tableOid: Oid, indexOid: Oid) -> bool {
    crate::optimizer::plan::planner::plan_cluster_use_sort(tableOid as _, indexOid as _)
}

pub unsafe fn plan_create_index_workers(tableOid: Oid, indexOid: Oid) -> c_int {
    crate::optimizer::plan::planner::plan_create_index_workers(tableOid as _, indexOid as _) as _
}

/* in plan/setrefs.c: */

pub unsafe fn extract_query_dependencies(
    query: *mut Node,
    relationOids: *mut *mut List,
    invalItems: *mut *mut List,
    hasRowSecurity: *mut bool,
) {
    crate::optimizer::plan::setrefs::extract_query_dependencies(
        query as _,
        relationOids as _,
        invalItems as _,
        hasRowSecurity as _,
    )
}

/* in prep/prepqual.c: */

pub unsafe fn negate_clause(node: *mut Node) -> *mut Node {
    crate::optimizer::prep::prepqual::negate_clause(node as _) as _
}

pub unsafe fn canonicalize_qual(qual: *mut Expr, is_check: bool) -> *mut Expr {
    crate::optimizer::prep::prepqual::canonicalize_qual(qual as _, is_check) as _
}

/* in util/clauses.c: */

pub unsafe fn contain_mutable_functions(clause: *mut Node) -> bool {
    crate::optimizer::util::clauses::contain_mutable_functions(clause as _)
}

pub unsafe fn contain_mutable_functions_after_planning(expr: *mut Expr) -> bool {
    crate::optimizer::util::clauses::contain_mutable_functions_after_planning(expr as _)
}

pub unsafe fn contain_volatile_functions(clause: *mut Node) -> bool {
    crate::optimizer::util::clauses::contain_volatile_functions(clause as _)
}

pub unsafe fn contain_volatile_functions_after_planning(expr: *mut Expr) -> bool {
    crate::optimizer::util::clauses::contain_volatile_functions_after_planning(expr as _)
}

pub unsafe fn contain_volatile_functions_not_nextval(clause: *mut Node) -> bool {
    crate::optimizer::util::clauses::contain_volatile_functions_not_nextval(clause as _)
}

pub unsafe fn eval_const_expressions(root: *mut PlannerInfo, node: *mut Node) -> *mut Node {
    crate::optimizer::util::clauses::eval_const_expressions(root as _, node as _) as _
}

pub unsafe fn convert_saop_to_hashed_saop(node: *mut Node) {
    crate::optimizer::util::clauses::convert_saop_to_hashed_saop(node as _)
}

pub unsafe fn estimate_expression_value(root: *mut PlannerInfo, node: *mut Node) -> *mut Node {
    crate::optimizer::util::clauses::estimate_expression_value(root as _, node as _) as _
}

pub unsafe fn evaluate_expr(
    expr: *mut Expr,
    result_type: Oid,
    result_typmod: int32,
    result_collation: Oid,
) -> *mut Expr {
    crate::optimizer::util::clauses::evaluate_expr(
        expr as _,
        result_type as _,
        result_typmod as _,
        result_collation as _,
    ) as _
}

pub unsafe fn expand_function_arguments(
    args: *mut List,
    include_out_arguments: bool,
    result_type: Oid,
    func_tuple: *mut HeapTupleData,
) -> *mut List {
    crate::optimizer::util::clauses::expand_function_arguments(
        args as _,
        include_out_arguments,
        result_type as _,
        func_tuple as _,
    ) as _
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
    crate::optimizer::util::clauses::make_SAOP_expr(
        oper as _,
        leftexpr as _,
        coltype as _,
        arraycollid as _,
        inputcollid as _,
        exprs as _,
        haveNonConst,
    ) as _
}

/* in util/predtest.c: */

pub unsafe fn predicate_implied_by(
    predicate_list: *mut List,
    clause_list: *mut List,
    weak: bool,
) -> bool {
    crate::optimizer::util::predtest::predicate_implied_by(
        predicate_list as _,
        clause_list as _,
        weak,
    )
}

pub unsafe fn predicate_refuted_by(
    predicate_list: *mut List,
    clause_list: *mut List,
    weak: bool,
) -> bool {
    crate::optimizer::util::predtest::predicate_refuted_by(
        predicate_list as _,
        clause_list as _,
        weak,
    )
}

/* in util/tlist.c: */

pub unsafe fn count_nonjunk_tlist_entries(tlist: *mut List) -> c_int {
    crate::optimizer::util::tlist::count_nonjunk_tlist_entries(tlist as _) as _
}

pub unsafe fn get_sortgroupref_tle(sortref: Index, targetList: *mut List) -> *mut TargetEntry {
    crate::optimizer::util::tlist::get_sortgroupref_tle(sortref as _, targetList as _) as _
}

pub unsafe fn get_sortgroupclause_tle(
    sgClause: *mut SortGroupClause,
    targetList: *mut List,
) -> *mut TargetEntry {
    crate::optimizer::util::tlist::get_sortgroupclause_tle(sgClause as _, targetList as _) as _
}

pub unsafe fn get_sortgroupclause_expr(
    sgClause: *mut SortGroupClause,
    targetList: *mut List,
) -> *mut Node {
    crate::optimizer::util::tlist::get_sortgroupclause_expr(sgClause as _, targetList as _) as _
}

pub unsafe fn get_sortgrouplist_exprs(sgClauses: *mut List, targetList: *mut List) -> *mut List {
    crate::optimizer::util::tlist::get_sortgrouplist_exprs(sgClauses as _, targetList as _) as _
}

pub unsafe fn get_sortgroupref_clause(
    sortref: Index,
    clauses: *mut List,
) -> *mut SortGroupClause {
    crate::optimizer::util::tlist::get_sortgroupref_clause(sortref as _, clauses as _) as _
}

pub unsafe fn get_sortgroupref_clause_noerr(
    sortref: Index,
    clauses: *mut List,
) -> *mut SortGroupClause {
    crate::optimizer::util::tlist::get_sortgroupref_clause_noerr(sortref as _, clauses as _) as _
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
    crate::optimizer::util::var::pull_varnos(root as _, node as _) as _
}

pub unsafe fn pull_varnos_of_level(
    root: *mut PlannerInfo,
    node: *mut Node,
    levelsup: c_int,
) -> *mut Bitmapset {
    crate::optimizer::util::var::pull_varnos_of_level(root as _, node as _, levelsup as _) as _
}

pub unsafe fn pull_varattnos(node: *mut Node, varno: Index, varattnos: *mut *mut Bitmapset) {
    crate::optimizer::util::var::pull_varattnos(node as _, varno as _, varattnos as _)
}

pub unsafe fn pull_vars_of_level(node: *mut Node, levelsup: c_int) -> *mut List {
    crate::optimizer::util::var::pull_vars_of_level(node as _, levelsup as _) as _
}

pub unsafe fn contain_var_clause(node: *mut Node) -> bool {
    crate::optimizer::util::var::contain_var_clause(node as _)
}

pub unsafe fn contain_vars_of_level(node: *mut Node, levelsup: c_int) -> bool {
    crate::optimizer::util::var::contain_vars_of_level(node as _, levelsup as _)
}

pub unsafe fn contain_vars_returning_old_or_new(node: *mut Node) -> bool {
    crate::optimizer::util::var::contain_vars_returning_old_or_new(node as _)
}

pub unsafe fn locate_var_of_level(node: *mut Node, levelsup: c_int) -> c_int {
    crate::optimizer::util::var::locate_var_of_level(node as _, levelsup as _) as _
}

pub unsafe fn pull_var_clause(node: *mut Node, flags: c_int) -> *mut List {
    crate::optimizer::util::var::pull_var_clause(node as _, flags as _) as _
}

pub unsafe fn flatten_join_alias_vars(
    root: *mut PlannerInfo,
    query: *mut Query,
    node: *mut Node,
) -> *mut Node {
    crate::optimizer::util::var::flatten_join_alias_vars(root as _, query as _, node as _) as _
}

pub unsafe fn flatten_group_exprs(
    root: *mut PlannerInfo,
    query: *mut Query,
    node: *mut Node,
) -> *mut Node {
    crate::optimizer::util::var::flatten_group_exprs(root as _, query as _, node as _) as _
}
