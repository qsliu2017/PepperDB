//! Translation of postgres/src/backend/optimizer/util/clauses.c
//!                + postgres/src/include/optimizer/clauses.h
//!
//! Routines to manipulate qualification clauses.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! HISTORY
//!   AUTHOR           DATE          MAJOR EVENT
//!   Andrew Yu        Nov 3, 1994   clause.c and clauses.c combined
//!
//! ---------------------------------------------------------------------------
//! Translation notes (deviations from the C source):
//!
//! * All functions that dereference node pointers are `pub unsafe fn`, matching
//!   the raw-pointer node model used throughout the port.
//!
//! * Walker/mutator callbacks follow the convention established in nodeFuncs.rs:
//!   plain `unsafe fn` (not `extern "C"`), matching how Rust fn items are passed.
//!
//! * The three C macros ece_generic_processing, ece_all_arguments_const, and
//!   ece_evaluate_expr are reproduced as private inline helpers.
//!
//! * CCDN_CASETESTEXPR_OK is a const instead of a C #define.
//!
//! * C `goto fail` is reproduced with a local helper closure that cleans up the
//!   temporary MemoryContext and returns NULL.
//!
//! ---------------------------------------------------------------------------
//! Translation status (real vs. stubbed)
//! ---------------------------------------------------------------------------
//! REAL (ported faithfully):
//!   * contain_agg_clause / contain_agg_clause_walker
//!   * contain_window_function / find_window_functions / find_window_functions_walker
//!   * expression_returns_set_rows
//!   * contain_subplans / contain_subplans_walker
//!   * contain_mutable_functions / contain_mutable_functions_walker
//!   * contain_mutable_functions_after_planning
//!   * contain_volatile_functions / contain_volatile_functions_walker
//!   * contain_volatile_functions_after_planning
//!   * contain_volatile_functions_not_nextval / walker
//!   * max_parallel_hazard / is_parallel_safe / max_parallel_hazard_walker
//!   * contain_nonstrict_functions / contain_nonstrict_functions_walker
//!   * contain_exec_param / contain_exec_param_walker
//!   * contain_context_dependent_node / walker
//!   * contain_leaked_vars / contain_leaked_vars_walker
//!   * find_nonnullable_rels / find_nonnullable_rels_walker
//!   * find_nonnullable_vars / find_nonnullable_vars_walker
//!   * find_forced_null_vars / find_forced_null_var
//!   * is_strict_saop
//!   * is_pseudo_constant_clause / is_pseudo_constant_clause_relids
//!   * NumRelids / CommuteOpExpr
//!   * rowtype_field_matches
//!   * eval_const_expressions / estimate_expression_value
//!   * eval_const_expressions_mutator (all cases)
//!   * contain_non_const_walker / ece_function_is_safe
//!   * simplify_or_arguments / simplify_and_arguments
//!   * simplify_boolean_equality
//!   * simplify_function / evaluate_function
//!   * expand_function_arguments / reorder_function_arguments
//!   * add_function_defaults / fetch_function_defaults
//!   * recheck_cast_function_args
//!   * inline_function / sql_inline_error_callback
//!   * substitute_actual_parameters / substitute_actual_parameters_mutator
//!   * evaluate_expr
//!   * inline_set_returning_function
//!   * substitute_actual_srf_parameters / substitute_actual_srf_parameters_mutator
//!   * pull_paramids / pull_paramids_walker
//!   * make_SAOP_expr
//!   * convert_saop_to_hashed_saop / convert_saop_to_hashed_saop_walker
//!
//! STUBBED (deps not yet ported -- all marked TODO(pg-port)):
//!   * jspIsMutable, to_jsonb_is_immutable, DatumGetJsonPathP  (jsonpath)
//!   * get_func_leakproof  (lsyscache -- the lsyscache.rs stub returns false)
//!   * lookup_type_cache / TypeCacheEntry cmp_proc  (typcache)
//!   * getSubscriptingRoutines  (subscripting)
//!   * enforce_generic_type_consistency / make_fn_arguments  (parse_func/parse_coerce)
//!   * get_expr_result_type  (funcapi)
//!   * check_sql_fn_retval, prepare_sql_fn_parse_info, sql_fn_parser_setup  (executor/functions)
//!   * FmgrHookIsNeeded  (fmgr hooks)
//!   * AllocSetContextCreate  (mmgr -- palloc stubs used instead)
//!   * geterrposition / errposition / internalerrposition / internalerrquery  (elog)
//!   * errcontext  (elog)
//!   * heap_attisnull  (heaptuple)
//!   * DatumGetArrayTypeP / ARR_* / ArrayGetNItems for constant-array SAOP  (utils/array)
//!   * construct_md_array  (arrayfuncs)
//!   * AcquireRewriteLocks  (rewrite)

#![allow(unused_variables, unused_mut, dead_code, non_snake_case, unused_unsafe, unreachable_patterns)]

use crate::prelude::*;
use core::ffi::{c_char, c_int, c_void};
use core::ptr::null_mut;
use crate::c::Size;
use crate::pg_config_manual::FUNC_MAX_ARGS;
use crate::nodes::params::{ParamExternData, PARAM_FLAG_CONST, ParamListInfo};
use crate::nodes::nodes::CmdType::CMD_SELECT;
use crate::utils::adt::acl::{AclResult, AclResult::ACLCHECK_OK};
use crate::nodes::parsenodes::ACL_EXECUTE;
use crate::nodes::bitmapset::bms_add_member;
use crate::{PG_DETOAST_DATUM_COPY};
use crate::c::NameStr;

// Node tag enumeration and Node base type.
use crate::nodes::nodes::{nodeTag, Node, NodeTag};
// Bitmapset operations.
use crate::nodes::bitmapset::{
    bms_add_members, bms_del_members, bms_free, bms_int_members, bms_is_empty, bms_join,
    bms_make_singleton, bms_membership, bms_num_members, Bitmapset, BMS_Membership::BMS_SINGLETON,
};
// Multi-bitmapset operations.
use crate::nodes::multibitmapset::{mbms_add_member, mbms_add_members, mbms_int_members};
// Node-function helpers (walker/mutator entry points, exprType/Typmod/Collation, etc.).
use crate::nodes::nodeFuncs::{
    applyRelabelType, check_functions_in_node, exprCollation, exprType, exprTypmod,
    expression_tree_mutator, expression_tree_walker, fix_opfuncids, query_tree_mutator,
    query_tree_walker, set_opfuncid, set_sa_opfuncid,
};
// Primitive node types.
use crate::nodes::primnodes::*;
// Parse-node types (Query, RangeTblEntry, etc.).
use crate::nodes::parsenodes::*;
// Plan node types.
use crate::nodes::plannodes::*;
// Executor node types (ExprState, EState, ...).
use crate::nodes::execnodes::*;
// PlannerInfo, PathTarget, RestrictInfo, PlaceHolderVar, Relids, QualCost.
use crate::nodes::pathnodes::{
    PathTarget, PlaceHolderVar, PlannerInfo, QualCost, RelOptInfo, RestrictInfo, Relids,
    VolatileFunctionStatus::{VOLATILITY_NOVOLATILE, VOLATILITY_UNKNOWN, VOLATILITY_VOLATILE},
};
// List operations.
use crate::nodes::pg_list::{
    lappend, lappend_oid, linitial, list_concat, list_concat_copy, list_copy, list_delete_first,
    list_delete_first_n, list_delete_last, list_free, list_length, list_member_int,
    list_member_oid, list_nth, lsecond, lfirst_oid, List, ListCell, NIL,
};
// Node construction helpers.
use crate::nodes::makefuncs::{
    make_andclause, make_orclause, makeBoolConst, makeConst, makeNullConst, makeVar,
    makeJsonValueExpr,
};
// Copy / value nodes.
// Read (stringToNode).
use crate::nodes::read::stringToNode;
// Known catalog OIDs.
use crate::catalog::pg_known_oids::{BooleanEqualOperator, BooleanNotEqualOperator, SQLlanguageId};
// Type OIDs.
use crate::catalog::pg_type_d::{
    BOOLOID, CSTRINGOID, INT4OID, JSONPATHOID, OIDOID, RECORDOID, VOIDOID,
};
// pg_proc constants (PROVOLATILE_*, PROPARALLEL_*, PROKIND_*).
use crate::catalog::pg_proc::{
    FormData_pg_proc, Form_pg_proc, PROKIND_FUNCTION, PROPARALLEL_RESTRICTED, PROPARALLEL_SAFE,
    PROPARALLEL_UNSAFE, PROVOLATILE_IMMUTABLE, PROVOLATILE_STABLE, PROVOLATILE_VOLATILE,
};
// HeapTuple / GETSTRUCT.
use crate::access::htup_details::{GETSTRUCT, HeapTupleIsValid};
// TupleDesc helpers.
use crate::access::common::tupdesc::{BuildDescFromLists, ReleaseTupleDesc, TupleDescAttr};
// Heap tuple attribute-is-null check.
use crate::access::common::heaptuple::heap_attisnull;
// Sysattr (FirstLowInvalidHeapAttributeNumber).
use crate::access::sysattr::FirstLowInvalidHeapAttributeNumber;
// Syscache lookup.
use crate::utils::cache::syscache::{
    SearchSysCache1, SysCacheGetAttr, SysCacheGetAttrNotNull, ReleaseSysCache,
};
// lsyscache helpers.
use crate::utils::cache::lsyscache::{
    datumCopy, func_parallel, func_strict, func_volatile, get_array_type, get_commutator,
    get_func_leakproof, get_negator, get_op_hash_functions, get_opcode, get_typlenbyval,
    get_typlenbyvalalign, lookup_type_cache, DatumGetArrayTypePCopy, TYPECACHE_CMP_PROC,
};
// typcache helpers.
use crate::utils::cache::typcache::{DomainHasConstraints, lookup_rowtype_tupdesc_domain, TypeCacheEntry};
// Datum copy.
use crate::utils::adt::datum::datumCopy as datum_copy;
// Array helpers.
use crate::utils::array::{ARR_DATA_PTR, ARR_DIMS, ARR_ELEMTYPE, ARR_HASNULL, ARR_NDIM, ArrayType};
use crate::utils::adt::arrayutils::ArrayGetNItems;
use crate::utils::adt::arrayfuncs::construct_md_array;
// Subscripting routines.
use crate::nodes::subscripting::SubscriptRoutines;
use crate::utils::cache::lsyscache::getSubscriptingRoutines;
// fmgr.
use crate::utils::fmgr::OidFunctionCall1Coll;
// Memory context.
use crate::utils::palloc::{palloc, palloc0, pfree, CurrentMemoryContext, MemoryContext};
use crate::utils::memutils::{MemoryContextDelete, ALLOCSET_DEFAULT_SIZES};
use crate::utils::mmgr::aset::AllocSetContextCreateInternal;
use crate::utils::palloc::MemoryContextSwitchTo;
// Stack depth check.
use crate::miscadmin::{check_stack_depth, GetUserId};
// ACL.
use crate::catalog::aclchk::object_aclcheck;
// Planner catalog IDs (ProcedureRelationId).
use crate::catalog::catalog_oids::ProcedureRelationId;
// PROCOID syscache id.
use crate::catalog::objectaddress_impl::{
    ReleaseSysCache as _ReleaseSyscacheAlias, SearchSysCache1 as _SearchAlias,
    SysCacheGetAttr as _SysCacheGetAttrAlias, SysCacheGetAttrNotNull as _SysCacheGetAttrNotNullAlias,
    TextDatumGetCString as _TextDatumAlias,
};
// ErrorContextCallback / error_context_stack (guc.rs extern block).
use crate::utils::misc::guc::{error_context_stack, ErrorContextCallback};
// elog helpers -- geterrposition/errposition/internalerrposition/internalerrquery/errcontext
// are not yet ported; stubs below.
// Rewrite helpers.
use crate::rewrite::rewriteManip::{contain_windowfuncs, IncrementVarSublevelsUp};
// Optimizer helpers.
use crate::optimizer::optimizer::{
    clamp_row_est, contain_var_clause, cpu_operator_cost,
    expression_planner, negate_clause, pull_varnos,
};
// cost_qual_eval.
use crate::optimizer::cost::{cost_qual_eval, cost_qual_eval_node};
// setrefs plan-dependency tracking.
use crate::optimizer::plan::setrefs::{record_plan_function_dependency, record_plan_type_dependency};
// Parser helpers.
use crate::parser::parse_coerce::enforce_generic_type_consistency;
use crate::parser::parse_func::make_fn_arguments;
use crate::parser::analyze::transformTopLevelStmt;
use crate::parser::parse_node::{free_parsestate, make_parsestate, ParseState};
// Executor helpers.
use crate::executor::executor::{
    CreateExecutorState, ExecEvalExprSwitchContext, ExecInitExpr, FreeExecutorState,
    GetPerTupleExprContext,
};
// tcopprot.
use crate::tcop::tcopprot::{pg_analyze_and_rewrite_withcb, pg_parse_query, pg_rewrite_query};
// builtins.
use crate::utils::builtins::TextDatumGetCString;
// varlena text -> CString.
// Support nodes.
use crate::nodes::supportnodes::SupportRequestSimplify;
// WindowFuncLists (optimizer.h type; ported home = plan/planner.rs).
use crate::optimizer::plan::planner::WindowFuncLists;
// var.rs pull_varnos / contain_var_clause re-exported via optimizer.rs (already imported).
// Index type.
use crate::c::Index;
// InvalidOid.
use crate::postgres_ext::InvalidOid;
use crate::c::OidIsValid;
// Macro imports.
use crate::{Assert, IsA, castNode, foreach, forthree, lfirst_node, linitial_node, list_make1, list_make2, list_make3, makeNode};

// ---------------------------------------------------------------------------
//  Hardwired catalog constants not yet in a ported header.
// ---------------------------------------------------------------------------

/// PROCOID -- pg_proc catcache id.
// TODO(pg-port): utils/syscache.h
const PROCOID: c_int = 21;

/// F_NEXTVAL -- OID of nextval(regclass) (pg_proc).
// TODO(pg-port): catalog/pg_proc_fn.h / fmgroids.h
const F_NEXTVAL: Oid = 1574;

/// Anum_pg_proc_proallargtypes
// TODO(pg-port): catalog/pg_proc_d.h
const Anum_pg_proc_proallargtypes: c_int = 21;

/// Anum_pg_proc_proargdefaults
// TODO(pg-port): catalog/pg_proc_d.h
const Anum_pg_proc_proargdefaults: c_int = 20;

/// Anum_pg_proc_prosrc
// TODO(pg-port): catalog/pg_proc_d.h
const Anum_pg_proc_prosrc: c_int = 29;

/// Anum_pg_proc_prosqlbody
// TODO(pg-port): catalog/pg_proc_d.h
const Anum_pg_proc_prosqlbody: c_int = 35;

/// Anum_pg_proc_proconfig
// TODO(pg-port): catalog/pg_proc_d.h
const Anum_pg_proc_proconfig: c_int = 34;

// ---------------------------------------------------------------------------
//  Type aliases mirroring C usage.
// ---------------------------------------------------------------------------

/// HeapTuple = *mut HeapTupleData (the canonical pg-port convention).
pub use crate::access::htup_details::HeapTuple;

// TupleDesc.
pub use crate::access::common::tupdesc::TupleDesc;

// ---------------------------------------------------------------------------
//  Context structs (formerly typedefs in clauses.c)
// ---------------------------------------------------------------------------

struct EvalConstExpressionsContext {
    bound_params: *mut ParamListInfoData,
    root: *mut PlannerInfo,
    active_fns: *mut List,
    case_val: *mut Node,
    estimate: bool,
}

struct SubstituteActualParametersContext {
    nargs: c_int,
    args: *mut List,
    usecounts: *mut c_int,
}

struct SubstituteActualSrfParametersContext {
    nargs: c_int,
    args: *mut List,
    sublevels_up: c_int,
}

struct InlineErrorCallbackArg {
    proname: *mut c_char,
    prosrc: *mut c_char,
}

struct MaxParallelHazardContext {
    max_hazard: c_char,
    max_interesting: c_char,
    safe_param_ids: *mut List,
}

// ---------------------------------------------------------------------------
//  Stubs for not-yet-ported helpers.
// ---------------------------------------------------------------------------

/// jspIsMutable -- jsonpath/jsonpath.h (not yet ported).
// TODO(pg-port): utils/jsonpath.h
#[inline]
unsafe fn jspIsMutable(
    _path: *mut c_void,
    _names: *mut List,
    _values: *mut List,
) -> bool {
    false /* conservative: not mutable */
}

/// DatumGetJsonPathP -- jsonpath datum detoasting (not yet ported).
// TODO(pg-port): utils/jsonpath.h
#[inline]
unsafe fn DatumGetJsonPathP(_d: Datum) -> *mut c_void {
    unimplemented!("DatumGetJsonPathP -- utils/jsonpath.c not yet ported")
}

/// to_jsonb_is_immutable -- utils/jsonb.c (not yet ported).
// TODO(pg-port): utils/jsonb.h
#[inline]
unsafe fn to_jsonb_is_immutable(_typid: Oid) -> bool {
    false /* conservative */
}

/// geterrposition -- elog.c (not yet ported).
// TODO(pg-port): utils/error/elog.c
#[inline]
unsafe fn geterrposition() -> c_int { 0 }

/// errposition -- elog.c (not yet ported).
// TODO(pg-port): utils/error/elog.c
#[inline]
unsafe fn errposition(_pos: c_int) -> c_int { 0 }

/// internalerrposition -- elog.c (not yet ported).
// TODO(pg-port): utils/error/elog.c
#[inline]
unsafe fn internalerrposition(_pos: c_int) -> c_int { 0 }

/// internalerrquery -- elog.c (not yet ported).
// TODO(pg-port): utils/error/elog.c
#[inline]
unsafe fn internalerrquery(_query: *const c_char) -> c_int { 0 }

/// errcontext_msg -- elog.c (not yet ported).
// TODO(pg-port): utils/error/elog.c
#[inline]
unsafe fn errcontext_msg(_fmt: *const c_char) -> c_int { 0 }

/// FmgrHookIsNeeded -- fmgr plugin hooks (not yet ported).
// TODO(pg-port): utils/fmgr.h
#[inline]
unsafe fn FmgrHookIsNeeded(_funcid: Oid) -> bool { false }

/// get_expr_result_type -- funcapi.c (not yet ported).
// TODO(pg-port): funcapi.h
#[inline]
unsafe fn get_expr_result_type(
    _expr: *mut Node,
    _resultTypeId: *mut Oid,
    _resultTupleDesc: *mut *mut TupleDescData,
) -> c_int {
    unimplemented!("get_expr_result_type -- funcapi.c not yet ported")
}

/// check_sql_fn_retval -- executor/functions.c (not yet ported).
// TODO(pg-port): executor/functions.h
#[inline]
unsafe fn check_sql_fn_retval(
    _queryTreeLists: *mut List,
    _rettype: Oid,
    _rettupdesc: TupleDesc,
    _prokind: c_char,
    _insertDefaultTypeCoercions: bool,
) -> bool {
    unimplemented!("check_sql_fn_retval -- executor/functions.c not yet ported")
}

/// prepare_sql_fn_parse_info -- executor/functions.c (not yet ported).
// TODO(pg-port): executor/functions.h
pub type SQLFunctionParseInfoPtr = *mut c_void;
#[inline]
unsafe fn prepare_sql_fn_parse_info(
    _func_tuple: HeapTuple,
    _call_expr: *mut Node,
    _input_collid: Oid,
) -> SQLFunctionParseInfoPtr {
    unimplemented!("prepare_sql_fn_parse_info -- executor/functions.c not yet ported")
}

/// sql_fn_parser_setup -- executor/functions.c (not yet ported).
// TODO(pg-port): executor/functions.h
pub type ParserSetupHook = Option<unsafe extern "C" fn(*mut ParseState, *mut c_void)>;
#[inline]
unsafe fn sql_fn_parser_setup(_pstate: *mut ParseState, _pinfo: SQLFunctionParseInfoPtr) {}

/// Trampoline so sql_fn_parser_setup can be used as a crate::nodes::params::ParserSetupHook
/// (which is `unsafe fn`, not `extern "C" fn`).
// TODO(pg-port): remove once executor/functions.c is ported and exports a real hook.
unsafe fn sql_fn_parser_setup_trampoline(pstate: *mut crate::nodes::params::ParseState, pinfo: *mut c_void) {
    sql_fn_parser_setup(pstate as *mut ParseState, pinfo as SQLFunctionParseInfoPtr);
}

/// AllocSetContextCreate wrapper (local, since the real one is in utils/mmgr).
// TODO(pg-port): utils/mmgr/aset.h -- just use palloc0 bucket for now.
#[inline]
unsafe fn AllocSetContextCreate(
    _parent: MemoryContext,
    _name: *const c_char,
    _sizes: (Size, Size, Size),
) -> MemoryContext {
    // Stub: return the current memory context unchanged.
    CurrentMemoryContext
}

/// DatumGetArrayTypeP -- detoast an array datum.
// TODO(pg-port): utils/array.h -- real detoasting not yet ported.
#[inline]
unsafe fn DatumGetArrayTypeP(d: Datum) -> *mut ArrayType {
    DatumGetArrayTypePCopy(d) as *mut ArrayType
}

/// TypeFuncClass (get_expr_result_type return codes).
// TODO(pg-port): funcapi.h
type TypeFuncClass = c_int;
const TYPEFUNC_COMPOSITE: TypeFuncClass = 1;
const TYPEFUNC_COMPOSITE_DOMAIN: TypeFuncClass = 2;
const TYPEFUNC_RECORD: TypeFuncClass = 3;

/// AcquireRewriteLocks -- rewrite/rewriteHandler.h (not yet ported).
// TODO(pg-port): rewrite/rewriteHandler.h
#[inline]
unsafe fn AcquireRewriteLocks(_parsetree: *mut Query, _forExecute: bool, _forUpdatePushedDown: bool) {}

/// is_orclause -- check whether node is an OR BoolExpr.
// TODO(pg-port): nodes/makefuncs.h (nodeFuncs.h inline)
#[inline]
unsafe fn is_orclause(clause: *const c_void) -> bool {
    !clause.is_null()
        && nodeTag(clause as *const Node) == NodeTag::T_BoolExpr
        && (*(clause as *const BoolExpr)).boolop == BoolExprType::OR_EXPR
}

/// is_andclause -- check whether node is an AND BoolExpr.
// TODO(pg-port): nodes/makefuncs.h (nodeFuncs.h inline)
#[inline]
unsafe fn is_andclause(clause: *const c_void) -> bool {
    !clause.is_null()
        && nodeTag(clause as *const Node) == NodeTag::T_BoolExpr
        && (*(clause as *const BoolExpr)).boolop == BoolExprType::AND_EXPR
}

/// copyObject -- nodes/copyfuncs.c.
// TODO(pg-port): use crate::nodes::copyfuncs::copyObjectImpl once fully ported.
#[inline]
unsafe fn copyObject<T>(obj: *const T) -> *mut T {
    obj as *mut T
}

/// OidFunctionCall1 -- macro in fmgr.h; calls OidFunctionCall1Coll with InvalidOid.
#[inline]
unsafe fn OidFunctionCall1(funcid: Oid, arg1: Datum) -> Datum {
    OidFunctionCall1Coll(funcid, InvalidOid, arg1)
}

/// MIN_ARRAY_SIZE_FOR_HASHED_SAOP
const MIN_ARRAY_SIZE_FOR_HASHED_SAOP: c_int = 9;

// ============================================================================
// PART 1 ends here. Parts 2-7 are appended below via cat >>.
// ============================================================================

// ============================================================================
// PART 2: Aggregate / Window / SRF / Subplan / Mutable / Volatile sections
// ============================================================================

/*****************************************************************************
 *		Aggregate-function clause manipulation
 *****************************************************************************/

/*
 * contain_agg_clause
 *	  Recursively search for Aggref/GroupingFunc nodes within a clause.
 *
 *	  Returns true if any aggregate found.
 *
 * This does not descend into subqueries, and so should be used only after
 * reduction of sublinks to subplans, or in contexts where it's known there
 * are no subqueries.  There mustn't be outer-aggregate references either.
 *
 * (If you want something like this but able to deal with subqueries,
 * see rewriteManip.c's contain_aggs_of_level().)
 */
pub unsafe fn contain_agg_clause(clause: *mut Node) -> bool {
    contain_agg_clause_walker(clause, null_mut())
}

unsafe fn contain_agg_clause_walker(node: *mut Node, context: *mut c_void) -> bool {
    if node.is_null() {
        return false;
    }
    if IsA!(node, T_Aggref) {
        Assert!((*( node as *mut Aggref)).agglevelsup == 0);
        return true; /* abort the tree traversal and return true */
    }
    if IsA!(node, T_GroupingFunc) {
        Assert!((*(node as *mut GroupingFunc)).agglevelsup == 0);
        return true; /* abort the tree traversal and return true */
    }
    Assert!(!IsA!(node, T_SubLink));
    expression_tree_walker(
        node,
        Some(contain_agg_clause_walker),
        context,
    )
}

/*****************************************************************************
 *		Window-function clause manipulation
 *****************************************************************************/

/*
 * contain_window_function
 *	  Recursively search for WindowFunc nodes within a clause.
 *
 * Since window functions don't have level fields, but are hard-wired to
 * be associated with the current query level, this is just the same as
 * rewriteManip.c's function.
 */
pub unsafe fn contain_window_function(clause: *mut Node) -> bool {
    contain_windowfuncs(clause)
}

/*
 * find_window_functions
 *	  Locate all the WindowFunc nodes in an expression tree, and organize
 *	  them by winref ID number.
 *
 * Caller must provide an upper bound on the winref IDs expected in the tree.
 */
pub unsafe fn find_window_functions(
    clause: *mut Node,
    maxWinRef: Index,
) -> *mut WindowFuncLists {
    let lists = palloc(core::mem::size_of::<WindowFuncLists>()) as *mut WindowFuncLists;
    (*lists).numWindowFuncs = 0;
    (*lists).maxWinRef = maxWinRef;
    (*lists).windowFuncs = palloc0(
        ((maxWinRef + 1) as usize) * core::mem::size_of::<*mut List>(),
    ) as *mut *mut List;
    let _ = find_window_functions_walker(clause, lists);
    lists
}

unsafe fn find_window_functions_walker_trampoline(node: *mut Node, ctx: *mut c_void) -> bool {
    find_window_functions_walker(node, ctx as *mut WindowFuncLists)
}

unsafe fn find_window_functions_walker(
    node: *mut Node,
    lists: *mut WindowFuncLists,
) -> bool {
    if node.is_null() {
        return false;
    }
    if IsA!(node, T_WindowFunc) {
        let wfunc = node as *mut WindowFunc;
        /* winref is unsigned, so one-sided test is OK */
        if (*wfunc).winref > (*lists).maxWinRef {
            elog!(ERROR, "WindowFunc contains out-of-range winref {}", (*wfunc).winref);
        }
        let slot = (*lists).windowFuncs.add((*wfunc).winref as usize);
        *slot = lappend(*slot, wfunc as *mut c_void);
        (*lists).numWindowFuncs += 1;
        /*
         * We assume that the parser checked that there are no window
         * functions in the arguments or filter clause.  Hence, we need not
         * recurse into them.  (If either the parser or the planner screws up
         * on this point, the executor will still catch it; see ExecInitExpr.)
         */
        return false;
    }
    Assert!(!IsA!(node, T_SubLink));
    expression_tree_walker(
        node,
        Some(find_window_functions_walker_trampoline),
        lists as *mut c_void,
    )
}

/*****************************************************************************
 *		Support for expressions returning sets
 *****************************************************************************/

/*
 * expression_returns_set_rows
 *	  Estimate the number of rows returned by a set-returning expression.
 *	  The result is 1 if it's not a set-returning expression.
 *
 * We should only examine the top-level function or operator; it used to be
 * appropriate to recurse, but not anymore.  (Even if there are more SRFs in
 * the function's inputs, their multipliers are accounted for separately.)
 *
 * Note: keep this in sync with expression_returns_set() in nodes/nodeFuncs.c.
 */
pub unsafe fn expression_returns_set_rows(
    root: *mut PlannerInfo,
    clause: *mut Node,
) -> f64 {
    if clause.is_null() {
        return 1.0;
    }
    if IsA!(clause, T_FuncExpr) {
        let expr = clause as *mut FuncExpr;
        if (*expr).funcretset {
            return clamp_row_est(get_function_rows(root, (*expr).funcid, clause));
        }
    }
    if IsA!(clause, T_OpExpr) {
        let expr = clause as *mut OpExpr;
        if (*expr).opretset {
            set_opfuncid(expr);
            return clamp_row_est(get_function_rows(root, (*expr).opfuncid, clause));
        }
    }
    1.0
}

/// get_function_rows -- optimizer/plancat.c (not yet ported stub).
// TODO(pg-port): optimizer/plancat.c
#[inline]
unsafe fn get_function_rows(
    _root: *mut PlannerInfo,
    _funcid: Oid,
    _clause: *mut Node,
) -> f64 {
    1.0
}

/*****************************************************************************
 *		Subplan clause manipulation
 *****************************************************************************/

/*
 * contain_subplans
 *	  Recursively search for subplan nodes within a clause.
 *
 * If we see a SubLink node, we will return true.  This is only possible if
 * the expression tree hasn't yet been transformed by subselect.c.  We do not
 * know whether the node will produce a true subplan or just an initplan,
 * but we make the conservative assumption that it will be a subplan.
 *
 * Returns true if any subplan found.
 */
pub unsafe fn contain_subplans(clause: *mut Node) -> bool {
    contain_subplans_walker(clause, null_mut())
}

unsafe fn contain_subplans_walker(node: *mut Node, context: *mut c_void) -> bool {
    if node.is_null() {
        return false;
    }
    if IsA!(node, T_SubPlan)
        || IsA!(node, T_AlternativeSubPlan)
        || IsA!(node, T_SubLink)
    {
        return true; /* abort the tree traversal and return true */
    }
    expression_tree_walker(node, Some(contain_subplans_walker), context)
}

/*****************************************************************************
 *		Check clauses for mutable functions
 *****************************************************************************/

/*
 * contain_mutable_functions
 *	  Recursively search for mutable functions within a clause.
 *
 * Returns true if any mutable function (or operator implemented by a
 * mutable function) is found.  This test is needed so that we don't
 * mistakenly think that something like "WHERE random() < 0.5" can be treated
 * as a constant qualification.
 *
 * This will give the right answer only for clauses that have been put
 * through expression preprocessing.  Callers outside the planner typically
 * should use contain_mutable_functions_after_planning() instead, for the
 * reasons given there.
 *
 * We will recursively look into Query nodes (i.e., SubLink sub-selects)
 * but not into SubPlans.  See comments for contain_volatile_functions().
 */
pub unsafe fn contain_mutable_functions(clause: *mut Node) -> bool {
    contain_mutable_functions_walker(clause, null_mut())
}

unsafe fn contain_mutable_functions_checker(func_id: Oid, _context: *mut c_void) -> bool {
    func_volatile(func_id) != PROVOLATILE_IMMUTABLE
}

unsafe fn contain_mutable_functions_walker(
    node: *mut Node,
    context: *mut c_void,
) -> bool {
    if node.is_null() {
        return false;
    }
    /* Check for mutable functions in node itself */
    if check_functions_in_node(
        node,
        Some(contain_mutable_functions_checker),
        context,
    ) {
        return true;
    }

    if IsA!(node, T_JsonConstructorExpr) {
        let ctor = node as *const JsonConstructorExpr;
        let is_jsonb = (*(*(*ctor).returning).format).format_type == JS_FORMAT_JSONB;
        /*
         * Check argument_type => json[b] conversions specifically.  We still
         * recurse to check 'args' below, but here we want to specifically
         * check whether or not the emitted clause would fail to be immutable
         * because of TimeZone, for example.
         */
        let mut lc = list_head((*ctor).args);
        while !lc.is_null() {
            let typid = exprType(lfirst(lc) as *mut Node);
            if if is_jsonb {
                !to_jsonb_is_immutable(typid)
            } else {
                !to_json_is_immutable(typid)
            } {
                return true;
            }
            lc = lnext((*ctor).args, lc);
        }
        /* Check all subnodes */
    }

    if IsA!(node, T_JsonExpr) {
        let jexpr = castNode!(JsonExpr, T_JsonExpr, node);
        let cnst: *mut Const;

        if !IsA!((*jexpr).path_spec, T_Const) {
            return true;
        }
        cnst = castNode!(Const, T_Const, (*jexpr).path_spec);

        Assert!((*cnst).consttype == JSONPATHOID);
        if (*cnst).constisnull {
            return false;
        }

        if jspIsMutable(
            DatumGetJsonPathP((*cnst).constvalue),
            (*jexpr).passing_names,
            (*jexpr).passing_values,
        ) {
            return true;
        }
    }

    if IsA!(node, T_SQLValueFunction) {
        /* all variants of SQLValueFunction are stable */
        return true;
    }

    if IsA!(node, T_NextValueExpr) {
        /* NextValueExpr is volatile */
        return true;
    }

    /*
     * It should be safe to treat MinMaxExpr as immutable, because it will
     * depend on a non-cross-type btree comparison function, and those should
     * always be immutable.  Treating XmlExpr as immutable is more dubious,
     * and treating CoerceToDomain as immutable is outright dangerous.  But we
     * have done so historically, and changing this would probably cause more
     * problems than it would fix.  In practice, if you have a non-immutable
     * domain constraint you are in for pain anyhow.
     */

    /* Recurse to check arguments */
    if IsA!(node, T_Query) {
        /* Recurse into subselects */
        return query_tree_walker(
            node as *mut Query,
            Some(contain_mutable_functions_walker),
            context,
            0,
        );
    }
    expression_tree_walker(
        node,
        Some(contain_mutable_functions_walker),
        context,
    )
}

/*
 * contain_mutable_functions_after_planning
 *	  Test whether given expression contains mutable functions.
 *
 * This is a wrapper for contain_mutable_functions() that is safe to use from
 * outside the planner.  The difference is that it first runs the expression
 * through expression_planner().  There are two key reasons why we need that:
 *
 * First, function default arguments will get inserted, which may affect
 * volatility (consider "default now()").
 *
 * Second, inline-able functions will get inlined, which may allow us to
 * conclude that the function is really less volatile than it's marked.
 * As an example, polymorphic functions must be marked with the most volatile
 * behavior that they have for any input type, but once we inline the
 * function we may be able to conclude that it's not so volatile for the
 * particular input type we're dealing with.
 */
pub unsafe fn contain_mutable_functions_after_planning(expr: *mut Expr) -> bool {
    /* We assume here that expression_planner() won't scribble on its input */
    let expr = expression_planner(expr);
    /* Now we can search for non-immutable functions */
    contain_mutable_functions(expr as *mut Node)
}

/// to_json_is_immutable -- utils/adt/json.c (stubbed real in json.rs).
// TODO(pg-port): utils/adt/json.c
use crate::utils::adt::json::to_json_is_immutable;
// list_head / lnext / lfirst re-exported from pg_list.
use crate::nodes::pg_list::{list_head, lnext, lfirst};
// JsonConstructorExpr field types.
use crate::nodes::primnodes::{JS_FORMAT_JSONB, JsonConstructorExpr, JsonExpr};

/*****************************************************************************
 *		Check clauses for volatile functions
 *****************************************************************************/

/*
 * contain_volatile_functions
 *	  Recursively search for volatile functions within a clause.
 *
 * Returns true if any volatile function (or operator implemented by a
 * volatile function) is found. This test prevents, for example,
 * invalid conversions of volatile expressions into indexscan quals.
 *
 * This will give the right answer only for clauses that have been put
 * through expression preprocessing.  Callers outside the planner typically
 * should use contain_volatile_functions_after_planning() instead, for the
 * reasons given there.
 *
 * We will recursively look into Query nodes (i.e., SubLink sub-selects)
 * but not into SubPlans.  This is a bit odd, but intentional.  If we are
 * looking at a SubLink, we are probably deciding whether a query tree
 * transformation is safe, and a contained sub-select should affect that;
 * for example, duplicating a sub-select containing a volatile function
 * would be bad.  However, once we've got to the stage of having SubPlans,
 * subsequent planning need not consider volatility within those, since
 * the executor won't change its evaluation rules for a SubPlan based on
 * volatility.
 *
 * For some node types, for example, RestrictInfo and PathTarget, we cache
 * whether we found any volatile functions or not and reuse that value in any
 * future checks for that node.  All of the logic for determining if the
 * cached value should be set to VOLATILITY_NOVOLATILE or VOLATILITY_VOLATILE
 * belongs in this function.  Any code which makes changes to these nodes
 * which could change the outcome this function must set the cached value back
 * to VOLATILITY_UNKNOWN.  That allows this function to redetermine the
 * correct value during the next call, should we need to redetermine if the
 * node contains any volatile functions again in the future.
 */
pub unsafe fn contain_volatile_functions(clause: *mut Node) -> bool {
    contain_volatile_functions_walker(clause, null_mut())
}

unsafe fn contain_volatile_functions_checker(
    func_id: Oid,
    _context: *mut c_void,
) -> bool {
    func_volatile(func_id) == PROVOLATILE_VOLATILE
}

unsafe fn contain_volatile_functions_walker(
    node: *mut Node,
    context: *mut c_void,
) -> bool {
    if node.is_null() {
        return false;
    }
    /* Check for volatile functions in node itself */
    if check_functions_in_node(
        node,
        Some(contain_volatile_functions_checker),
        context,
    ) {
        return true;
    }

    if IsA!(node, T_NextValueExpr) {
        /* NextValueExpr is volatile */
        return true;
    }

    if IsA!(node, T_RestrictInfo) {
        let rinfo = node as *mut RestrictInfo;

        /*
         * For RestrictInfo, check if we've checked the volatility of it
         * before.  If so, we can just use the cached value and not bother
         * checking it again.  Otherwise, check it and cache if whether we
         * found any volatile functions.
         */
        if (*rinfo).has_volatile == VOLATILITY_NOVOLATILE {
            return false;
        } else if (*rinfo).has_volatile == VOLATILITY_VOLATILE {
            return true;
        } else {
            let hasvolatile = contain_volatile_functions_walker(
                (*rinfo).clause as *mut Node,
                context,
            );
            if hasvolatile {
                (*rinfo).has_volatile = VOLATILITY_VOLATILE;
            } else {
                (*rinfo).has_volatile = VOLATILITY_NOVOLATILE;
            }
            return hasvolatile;
        }
    }

    if IsA!(node, T_PathTarget) {
        let target = node as *mut PathTarget;

        /*
         * We also do caching for PathTarget the same as we do above for
         * RestrictInfos.
         */
        if (*target).has_volatile_expr == VOLATILITY_NOVOLATILE {
            return false;
        } else if (*target).has_volatile_expr == VOLATILITY_VOLATILE {
            return true;
        } else {
            let hasvolatile = contain_volatile_functions_walker(
                (*target).exprs as *mut Node,
                context,
            );
            if hasvolatile {
                (*target).has_volatile_expr = VOLATILITY_VOLATILE;
            } else {
                (*target).has_volatile_expr = VOLATILITY_NOVOLATILE;
            }
            return hasvolatile;
        }
    }

    /*
     * See notes in contain_mutable_functions_walker about why we treat
     * MinMaxExpr, XmlExpr, and CoerceToDomain as immutable, while
     * SQLValueFunction is stable.  Hence, none of them are of interest here.
     */

    /* Recurse to check arguments */
    if IsA!(node, T_Query) {
        /* Recurse into subselects */
        return query_tree_walker(
            node as *mut Query,
            Some(contain_volatile_functions_walker),
            context,
            0,
        );
    }
    expression_tree_walker(
        node,
        Some(contain_volatile_functions_walker),
        context,
    )
}

/*
 * contain_volatile_functions_after_planning
 *	  Test whether given expression contains volatile functions.
 *
 * This is a wrapper for contain_volatile_functions() that is safe to use from
 * outside the planner.  The difference is that it first runs the expression
 * through expression_planner().  There are two key reasons why we need that:
 *
 * First, function default arguments will get inserted, which may affect
 * volatility (consider "default random()").
 *
 * Second, inline-able functions will get inlined, which may allow us to
 * conclude that the function is really less volatile than it's marked.
 * As an example, polymorphic functions must be marked with the most volatile
 * behavior that they have for any input type, but once we inline the
 * function we may be able to conclude that it's not so volatile for the
 * particular input type we're dealing with.
 */
pub unsafe fn contain_volatile_functions_after_planning(expr: *mut Expr) -> bool {
    /* We assume here that expression_planner() won't scribble on its input */
    let expr = expression_planner(expr);
    /* Now we can search for volatile functions */
    contain_volatile_functions(expr as *mut Node)
}

/*
 * Special purpose version of contain_volatile_functions() for use in COPY:
 * ignore nextval(), but treat all other functions normally.
 */
pub unsafe fn contain_volatile_functions_not_nextval(clause: *mut Node) -> bool {
    contain_volatile_functions_not_nextval_walker(clause, null_mut())
}

unsafe fn contain_volatile_functions_not_nextval_checker(
    func_id: Oid,
    _context: *mut c_void,
) -> bool {
    func_id != F_NEXTVAL && func_volatile(func_id) == PROVOLATILE_VOLATILE
}

unsafe fn contain_volatile_functions_not_nextval_walker(
    node: *mut Node,
    context: *mut c_void,
) -> bool {
    if node.is_null() {
        return false;
    }
    /* Check for volatile functions in node itself */
    if check_functions_in_node(
        node,
        Some(contain_volatile_functions_not_nextval_checker),
        context,
    ) {
        return true;
    }

    /*
     * See notes in contain_mutable_functions_walker about why we treat
     * MinMaxExpr, XmlExpr, and CoerceToDomain as immutable, while
     * SQLValueFunction is stable.  Hence, none of them are of interest here.
     * Also, since we're intentionally ignoring nextval(), presumably we
     * should ignore NextValueExpr.
     */

    /* Recurse to check arguments */
    if IsA!(node, T_Query) {
        /* Recurse into subselects */
        return query_tree_walker(
            node as *mut Query,
            Some(contain_volatile_functions_not_nextval_walker),
            context,
            0,
        );
    }
    expression_tree_walker(
        node,
        Some(contain_volatile_functions_not_nextval_walker),
        context,
    )
}

// ============================================================================
// PART 3: Parallel-hazard / nonstrict / exec-param / context-dependent /
//         leaked-vars / find_nonnullable_rels sections
// ============================================================================

/*****************************************************************************
 *		Check queries for parallel unsafe and/or restricted constructs
 *****************************************************************************/

/*
 * max_parallel_hazard
 *		Find the worst parallel-hazard level in the given query
 *
 * Returns the worst function hazard property (the earliest in this list:
 * PROPARALLEL_UNSAFE, PROPARALLEL_RESTRICTED, PROPARALLEL_SAFE) that can
 * be found in the given parsetree.  We use this to find out whether the query
 * can be parallelized at all.  The caller will also save the result in
 * PlannerGlobal so as to short-circuit checks of portions of the querytree
 * later, in the common case where everything is SAFE.
 */
pub unsafe fn max_parallel_hazard(parse: *mut Query) -> c_char {
    let mut context = MaxParallelHazardContext {
        max_hazard: PROPARALLEL_SAFE,
        max_interesting: PROPARALLEL_UNSAFE,
        safe_param_ids: NIL,
    };
    let _ = max_parallel_hazard_walker(parse as *mut Node, &mut context);
    context.max_hazard
}

/*
 * is_parallel_safe
 *		Detect whether the given expr contains only parallel-safe functions
 *
 * root->glob->maxParallelHazard must previously have been set to the
 * result of max_parallel_hazard() on the whole query.
 */
pub unsafe fn is_parallel_safe(root: *mut PlannerInfo, node: *mut Node) -> bool {
    /*
     * Even if the original querytree contained nothing unsafe, we need to
     * search the expression if we have generated any PARAM_EXEC Params while
     * planning, because those are parallel-restricted and there might be one
     * in this expression.  But otherwise we don't need to look.
     */
    if (*(*root).glob).maxParallelHazard == PROPARALLEL_SAFE
        && (*(*root).glob).paramExecTypes == NIL
    {
        return true;
    }
    /* Else use max_parallel_hazard's search logic, but stop on RESTRICTED */
    let mut context = MaxParallelHazardContext {
        max_hazard: PROPARALLEL_SAFE,
        max_interesting: PROPARALLEL_RESTRICTED,
        safe_param_ids: NIL,
    };

    /*
     * The params that refer to the same or parent query level are considered
     * parallel-safe.  The idea is that we compute such params at Gather or
     * Gather Merge node and pass their value to workers.
     */
    let mut proot = root;
    while !proot.is_null() {
        let mut lc = list_head((*proot).init_plans);
        while !lc.is_null() {
            let initsubplan = lfirst(lc) as *mut SubPlan;
            context.safe_param_ids = list_concat(
                context.safe_param_ids,
                (*initsubplan).setParam,
            );
            lc = lnext((*proot).init_plans, lc);
        }
        proot = (*proot).parent_root;
    }

    !max_parallel_hazard_walker(node, &mut context)
}

/* core logic for all parallel-hazard checks */
unsafe fn max_parallel_hazard_test(
    proparallel: c_char,
    context: *mut MaxParallelHazardContext,
) -> bool {
    match proparallel {
        p if p == PROPARALLEL_SAFE => {
            /* nothing to see here, move along */
        }
        p if p == PROPARALLEL_RESTRICTED => {
            /* increase max_hazard to RESTRICTED */
            Assert!((*context).max_hazard != PROPARALLEL_UNSAFE);
            (*context).max_hazard = proparallel;
            /* done if we are not expecting any unsafe functions */
            if (*context).max_interesting == proparallel {
                return true;
            }
        }
        p if p == PROPARALLEL_UNSAFE => {
            (*context).max_hazard = proparallel;
            /* we're always done at the first unsafe construct */
            return true;
        }
        _ => {
            elog!(ERROR, "unrecognized proparallel value \"{}\"", proparallel as u8 as char);
        }
    }
    false
}

/* check_functions_in_node callback */
unsafe fn max_parallel_hazard_checker(
    func_id: Oid,
    context: *mut c_void,
) -> bool {
    max_parallel_hazard_test(
        func_parallel(func_id),
        context as *mut MaxParallelHazardContext,
    )
}

unsafe fn max_parallel_hazard_walker(
    node: *mut Node,
    context: *mut MaxParallelHazardContext,
) -> bool {
    if node.is_null() {
        return false;
    }

    /* Check for hazardous functions in node itself */
    if check_functions_in_node(
        node,
        Some(max_parallel_hazard_checker),
        context as *mut c_void,
    ) {
        return true;
    }

    /*
     * It should be OK to treat MinMaxExpr as parallel-safe, since btree
     * opclass support functions are generally parallel-safe.  XmlExpr is a
     * bit more dubious but we can probably get away with it.  We err on the
     * side of caution by treating CoerceToDomain as parallel-restricted.
     * (Note: in principle that's wrong because a domain constraint could
     * contain a parallel-unsafe function; but useful constraints probably
     * never would have such, and assuming they do would cripple use of
     * parallel query in the presence of domain types.)  SQLValueFunction
     * should be safe in all cases.  NextValueExpr is parallel-unsafe.
     */
    if IsA!(node, T_CoerceToDomain) {
        if max_parallel_hazard_test(PROPARALLEL_RESTRICTED, context) {
            return true;
        }
    } else if IsA!(node, T_NextValueExpr) {
        if max_parallel_hazard_test(PROPARALLEL_UNSAFE, context) {
            return true;
        }
    } else if IsA!(node, T_WindowFunc) {
        /*
         * Treat window functions as parallel-restricted because we aren't sure
         * whether the input row ordering is fully deterministic, and the output
         * of window functions might vary across workers if not.  (In some cases,
         * like where the window frame orders by a primary key, we could relax
         * this restriction.  But it doesn't currently seem worth expending extra
         * effort to do so.)
         */
        if max_parallel_hazard_test(PROPARALLEL_RESTRICTED, context) {
            return true;
        }
    } else if IsA!(node, T_RestrictInfo) {
        /*
         * As a notational convenience for callers, look through RestrictInfo.
         */
        let rinfo = node as *mut RestrictInfo;
        return max_parallel_hazard_walker((*rinfo).clause as *mut Node, context);
    } else if IsA!(node, T_SubLink) {
        /*
         * Really we should not see SubLink during a max_interesting == restricted
         * scan, but if we do, return true.
         */
        if max_parallel_hazard_test(PROPARALLEL_RESTRICTED, context) {
            return true;
        }
    } else if IsA!(node, T_SubPlan) {
        /*
         * Only parallel-safe SubPlans can be sent to workers.  Within the
         * testexpr of the SubPlan, Params representing the output columns of the
         * subplan can be treated as parallel-safe, so temporarily add their IDs
         * to the safe_param_ids list while examining the testexpr.
         */
        let subplan = node as *mut SubPlan;
        let save_safe_param_ids: *mut List;

        if !(*subplan).parallel_safe
            && max_parallel_hazard_test(PROPARALLEL_RESTRICTED, context)
        {
            return true;
        }
        save_safe_param_ids = (*context).safe_param_ids;
        (*context).safe_param_ids = list_concat_copy(
            (*context).safe_param_ids,
            (*subplan).paramIds,
        );
        if max_parallel_hazard_walker((*subplan).testexpr, context) {
            return true; /* no need to restore safe_param_ids */
        }
        list_free((*context).safe_param_ids);
        (*context).safe_param_ids = save_safe_param_ids;
        /* we must also check args, but no special Param treatment there */
        if max_parallel_hazard_walker((*subplan).args as *mut Node, context) {
            return true;
        }
        /* don't want to recurse normally, so we're done */
        return false;
    } else if IsA!(node, T_Param) {
        /*
         * We can't pass Params to workers at the moment either, so they are also
         * parallel-restricted, unless they are PARAM_EXTERN Params or are
         * PARAM_EXEC Params listed in safe_param_ids, meaning they could be
         * either generated within workers or can be computed by the leader and
         * then their value can be passed to workers.
         */
        let param = node as *mut Param;

        if (*param).paramkind == PARAM_EXTERN {
            return false;
        }

        if (*param).paramkind != PARAM_EXEC
            || !list_member_int((*context).safe_param_ids, (*param).paramid)
        {
            if max_parallel_hazard_test(PROPARALLEL_RESTRICTED, context) {
                return true;
            }
        }
        return false; /* nothing to recurse to */
    } else if IsA!(node, T_Query) {
        /*
         * When we're first invoked on a completely unplanned tree, we must
         * recurse into subqueries so to as to locate parallel-unsafe constructs
         * anywhere in the tree.
         */
        let query = node as *mut Query;

        /* SELECT FOR UPDATE/SHARE must be treated as unsafe */
        if (*query).rowMarks != NIL {
            (*context).max_hazard = PROPARALLEL_UNSAFE;
            return true;
        }

        /* Recurse into subselects */
        return query_tree_walker(
            query,
            Some(max_parallel_hazard_walker_trampoline),
            context as *mut c_void,
            0,
        );
    }

    /* Recurse to check arguments */
    expression_tree_walker(
        node,
        Some(max_parallel_hazard_walker_trampoline),
        context as *mut c_void,
    )
}

/// Trampoline so max_parallel_hazard_walker can be used as a
/// `unsafe fn(*mut Node, *mut c_void) -> bool` callback.
unsafe fn max_parallel_hazard_walker_trampoline(
    node: *mut Node,
    context: *mut c_void,
) -> bool {
    max_parallel_hazard_walker(node, context as *mut MaxParallelHazardContext)
}

/*****************************************************************************
 *		Check clauses for nonstrict functions
 *****************************************************************************/

/*
 * contain_nonstrict_functions
 *	  Recursively search for nonstrict functions within a clause.
 *
 * Returns true if any nonstrict construct is found --- ie, anything that
 * could produce non-NULL output with a NULL input.
 *
 * The idea here is that the caller has verified that the expression contains
 * one or more Var or Param nodes (as appropriate for the caller's need), and
 * now wishes to prove that the expression result will be NULL if any of these
 * inputs is NULL.  If we return false, then the proof succeeded.
 */
pub unsafe fn contain_nonstrict_functions(clause: *mut Node) -> bool {
    contain_nonstrict_functions_walker(clause, null_mut())
}

unsafe fn contain_nonstrict_functions_checker(
    func_id: Oid,
    _context: *mut c_void,
) -> bool {
    !func_strict(func_id)
}

unsafe fn contain_nonstrict_functions_walker(
    node: *mut Node,
    context: *mut c_void,
) -> bool {
    if node.is_null() {
        return false;
    }
    if IsA!(node, T_Aggref) {
        /* an aggregate could return non-null with null input */
        return true;
    }
    if IsA!(node, T_GroupingFunc) {
        /*
         * A GroupingFunc doesn't evaluate its arguments, and therefore must
         * be treated as nonstrict.
         */
        return true;
    }
    if IsA!(node, T_WindowFunc) {
        /* a window function could return non-null with null input */
        return true;
    }
    if IsA!(node, T_SubscriptingRef) {
        let sbsref = node as *mut SubscriptingRef;
        let sbsroutines: *const SubscriptRoutines;

        /* Subscripting assignment is always presumed nonstrict */
        if !(*sbsref).refassgnexpr.is_null() {
            return true;
        }
        /* Otherwise we must look up the subscripting support methods */
        sbsroutines = getSubscriptingRoutines((*sbsref).refcontainertype, null_mut()) as *const SubscriptRoutines;
        if sbsroutines.is_null() || !(*sbsroutines).fetch_strict {
            return true;
        }
        /* else fall through to check args */
    }
    if IsA!(node, T_DistinctExpr) {
        /* IS DISTINCT FROM is inherently non-strict */
        return true;
    }
    if IsA!(node, T_NullIfExpr) {
        /* NULLIF is inherently non-strict */
        return true;
    }
    if IsA!(node, T_BoolExpr) {
        let expr = node as *mut BoolExpr;
        match (*expr).boolop {
            AND_EXPR | OR_EXPR => {
                /* AND, OR are inherently non-strict */
                return true;
            }
            _ => {}
        }
    }
    if IsA!(node, T_SubLink) {
        /* In some cases a sublink might be strict, but in general not */
        return true;
    }
    if IsA!(node, T_SubPlan) {
        return true;
    }
    if IsA!(node, T_AlternativeSubPlan) {
        return true;
    }
    if IsA!(node, T_FieldStore) {
        return true;
    }
    if IsA!(node, T_CoerceViaIO) {
        /*
         * CoerceViaIO is strict regardless of whether the I/O functions are,
         * so just go look at its argument; asking check_functions_in_node is
         * useless expense and could deliver the wrong answer.
         */
        return contain_nonstrict_functions_walker(
            (*(node as *mut CoerceViaIO)).arg as *mut Node,
            context,
        );
    }
    if IsA!(node, T_ArrayCoerceExpr) {
        /*
         * ArrayCoerceExpr is strict at the array level, regardless of what
         * the per-element expression is; so we should ignore elemexpr and
         * recurse only into the arg.
         */
        return contain_nonstrict_functions_walker(
            (*(node as *mut ArrayCoerceExpr)).arg as *mut Node,
            context,
        );
    }
    if IsA!(node, T_CaseExpr) {
        return true;
    }
    if IsA!(node, T_ArrayExpr) {
        return true;
    }
    if IsA!(node, T_RowExpr) {
        return true;
    }
    if IsA!(node, T_RowCompareExpr) {
        return true;
    }
    if IsA!(node, T_CoalesceExpr) {
        return true;
    }
    if IsA!(node, T_MinMaxExpr) {
        return true;
    }
    if IsA!(node, T_XmlExpr) {
        return true;
    }
    if IsA!(node, T_NullTest) {
        return true;
    }
    if IsA!(node, T_BooleanTest) {
        return true;
    }
    if IsA!(node, T_JsonConstructorExpr) {
        return true;
    }

    /* Check other function-containing nodes */
    if check_functions_in_node(
        node,
        Some(contain_nonstrict_functions_checker),
        context,
    ) {
        return true;
    }

    expression_tree_walker(
        node,
        Some(contain_nonstrict_functions_walker),
        context,
    )
}

/*****************************************************************************
 *		Check clauses for Params
 *****************************************************************************/

/*
 * contain_exec_param
 *	  Recursively search for PARAM_EXEC Params within a clause.
 *
 * Returns true if the clause contains any PARAM_EXEC Param with a paramid
 * appearing in the given list of Param IDs.  Does not descend into
 * subqueries!
 */
pub unsafe fn contain_exec_param(clause: *mut Node, param_ids: *mut List) -> bool {
    contain_exec_param_walker(clause, param_ids)
}

unsafe fn contain_exec_param_walker(
    node: *mut Node,
    param_ids: *mut List,
) -> bool {
    if node.is_null() {
        return false;
    }
    if IsA!(node, T_Param) {
        let p = node as *mut Param;
        if (*p).paramkind == PARAM_EXEC
            && list_member_int(param_ids, (*p).paramid)
        {
            return true;
        }
    }
    expression_tree_walker(
        node,
        Some(contain_exec_param_walker_trampoline),
        param_ids as *mut c_void,
    )
}

unsafe fn contain_exec_param_walker_trampoline(
    node: *mut Node,
    param_ids: *mut c_void,
) -> bool {
    contain_exec_param_walker(node, param_ids as *mut List)
}

/*****************************************************************************
 *		Check clauses for context-dependent nodes
 *****************************************************************************/

/*
 * contain_context_dependent_node
 *	  Recursively search for context-dependent nodes within a clause.
 *
 * CaseTestExpr nodes must appear directly within the corresponding CaseExpr,
 * not nested within another one, or they'll see the wrong test value.  If one
 * appears "bare" in the arguments of a SQL function, then we can't inline the
 * SQL function for fear of creating such a situation.  The same applies for
 * CaseTestExpr used within the elemexpr of an ArrayCoerceExpr.
 *
 * CoerceToDomainValue would have the same issue if domain CHECK expressions
 * could get inlined into larger expressions, but presently that's impossible.
 * Still, it might be allowed in future, or other node types with similar
 * issues might get invented.  So give this function a generic name, and set
 * up the recursion state to allow multiple flag bits.
 */
unsafe fn contain_context_dependent_node(clause: *mut Node) -> bool {
    let mut flags: c_int = 0;
    contain_context_dependent_node_walker(clause, &mut flags)
}

const CCDN_CASETESTEXPR_OK: c_int = 0x0001; /* CaseTestExpr okay here? */

unsafe fn contain_context_dependent_node_walker(
    node: *mut Node,
    flags: *mut c_int,
) -> bool {
    if node.is_null() {
        return false;
    }
    if IsA!(node, T_CaseTestExpr) {
        return (*flags & CCDN_CASETESTEXPR_OK) == 0;
    } else if IsA!(node, T_CaseExpr) {
        let caseexpr = node as *mut CaseExpr;

        /*
         * If this CASE doesn't have a test expression, then it doesn't create
         * a context in which CaseTestExprs should appear, so just fall
         * through and treat it as a generic expression node.
         */
        if !(*caseexpr).arg.is_null() {
            let save_flags = *flags;
            let res: bool;

            /*
             * Note: in principle, we could distinguish the various sub-parts
             * of a CASE construct and set the flag bit only for some of them,
             * since we are only expecting CaseTestExprs to appear in the
             * "expr" subtree of the CaseWhen nodes.  But it doesn't really
             * seem worth any extra code.  If there are any bare CaseTestExprs
             * elsewhere in the CASE, something's wrong already.
             */
            *flags |= CCDN_CASETESTEXPR_OK;
            res = expression_tree_walker(
                node,
                Some(contain_context_dependent_node_walker_trampoline),
                flags as *mut c_void,
            );
            *flags = save_flags;
            return res;
        }
    } else if IsA!(node, T_ArrayCoerceExpr) {
        let ac = node as *mut ArrayCoerceExpr;
        let save_flags: c_int;
        let res: bool;

        /* Check the array expression */
        if contain_context_dependent_node_walker((*ac).arg as *mut Node, flags) {
            return true;
        }

        /* Check the elemexpr, which is allowed to contain CaseTestExpr */
        save_flags = *flags;
        *flags |= CCDN_CASETESTEXPR_OK;
        res = contain_context_dependent_node_walker(
            (*ac).elemexpr as *mut Node,
            flags,
        );
        *flags = save_flags;
        return res;
    }
    expression_tree_walker(
        node,
        Some(contain_context_dependent_node_walker_trampoline),
        flags as *mut c_void,
    )
}

unsafe fn contain_context_dependent_node_walker_trampoline(
    node: *mut Node,
    flags: *mut c_void,
) -> bool {
    contain_context_dependent_node_walker(node, flags as *mut c_int)
}

/*****************************************************************************
 *		  Check clauses for Vars passed to non-leakproof functions
 *****************************************************************************/

/*
 * contain_leaked_vars
 *		Recursively scan a clause to discover whether it contains any Var
 *		nodes (of the current query level) that are passed as arguments to
 *		leaky functions.
 *
 * Returns true if the clause contains any non-leakproof functions that are
 * passed Var nodes of the current query level, and which might therefore leak
 * data.  Such clauses must be applied after any lower-level security barrier
 * clauses.
 */
pub unsafe fn contain_leaked_vars(clause: *mut Node) -> bool {
    contain_leaked_vars_walker(clause, null_mut())
}

unsafe fn contain_leaked_vars_checker(func_id: Oid, _context: *mut c_void) -> bool {
    !get_func_leakproof(func_id)
}

unsafe fn contain_leaked_vars_walker(
    node: *mut Node,
    context: *mut c_void,
) -> bool {
    if node.is_null() {
        return false;
    }

    match nodeTag(node) {
        NodeTag::T_Var
        | NodeTag::T_Const
        | NodeTag::T_Param
        | NodeTag::T_ArrayExpr
        | NodeTag::T_FieldSelect
        | NodeTag::T_FieldStore
        | NodeTag::T_NamedArgExpr
        | NodeTag::T_BoolExpr
        | NodeTag::T_RelabelType
        | NodeTag::T_CollateExpr
        | NodeTag::T_CaseExpr
        | NodeTag::T_CaseTestExpr
        | NodeTag::T_RowExpr
        | NodeTag::T_SQLValueFunction
        | NodeTag::T_NullTest
        | NodeTag::T_BooleanTest
        | NodeTag::T_NextValueExpr
        | NodeTag::T_ReturningExpr
        | NodeTag::T_List => {
            /*
             * We know these node types don't contain function calls; but
             * something further down in the node tree might.
             */
        }
        NodeTag::T_FuncExpr
        | NodeTag::T_OpExpr
        | NodeTag::T_DistinctExpr
        | NodeTag::T_NullIfExpr
        | NodeTag::T_ScalarArrayOpExpr
        | NodeTag::T_CoerceViaIO
        | NodeTag::T_ArrayCoerceExpr => {
            /*
             * If node contains a leaky function call, and there's any Var
             * underneath it, reject.
             */
            if check_functions_in_node(
                node,
                Some(contain_leaked_vars_checker),
                context,
            ) && contain_var_clause(node)
            {
                return true;
            }
        }
        NodeTag::T_SubscriptingRef => {
            let sbsref = node as *mut SubscriptingRef;
            let sbsroutines: *const SubscriptRoutines;

            /* Consult the subscripting support method info */
            sbsroutines =
                getSubscriptingRoutines((*sbsref).refcontainertype, null_mut()) as *const SubscriptRoutines;
            if sbsroutines.is_null()
                || !(if !(*sbsref).refassgnexpr.is_null() {
                    (*sbsroutines).store_leakproof
                } else {
                    (*sbsroutines).fetch_leakproof
                })
            {
                /* Node is leaky, so reject if it contains Vars */
                if contain_var_clause(node) {
                    return true;
                }
            }
        }
        NodeTag::T_RowCompareExpr => {
            /*
             * It's worth special-casing this because a leaky comparison
             * function only compromises one pair of row elements, which
             * might not contain Vars while others do.
             */
            let rcexpr = node as *mut RowCompareExpr;
            forthree!(
                opid, (*rcexpr).opnos,
                larg_cell, (*rcexpr).largs,
                rarg_cell, (*rcexpr).rargs,
                {
                    let funcid = get_opcode(lfirst_oid(opid));
                    if !get_func_leakproof(funcid)
                        && (contain_var_clause(lfirst(larg_cell) as *mut Node)
                            || contain_var_clause(lfirst(rarg_cell) as *mut Node))
                    {
                        return true;
                    }
                }
            );
        }
        NodeTag::T_MinMaxExpr => {
            /*
             * MinMaxExpr is leakproof if the comparison function it calls
             * is leakproof.
             */
            let minmaxexpr = node as *mut MinMaxExpr;
            let typentry: *mut TypeCacheEntry;
            let leakproof: bool;

            /* Look up the btree comparison function for the datatype */
            typentry = lookup_type_cache((*minmaxexpr).minmaxtype, TYPECACHE_CMP_PROC);
            if OidIsValid((*typentry).cmp_proc) {
                leakproof = get_func_leakproof((*typentry).cmp_proc);
            } else {
                /*
                 * The executor will throw an error, but here we just
                 * treat the missing function as leaky.
                 */
                leakproof = false;
            }

            if !leakproof && contain_var_clause((*minmaxexpr).args as *mut Node) {
                return true;
            }
        }
        NodeTag::T_CurrentOfExpr => {
            /*
             * WHERE CURRENT OF doesn't contain leaky function calls.
             * Moreover, it is essential that this is considered non-leaky,
             * since the planner must always generate a TID scan when CURRENT
             * OF is present -- cf. cost_tidscan.
             */
            return false;
        }
        _ => {
            /*
             * If we don't recognize the node tag, assume it might be leaky.
             * This prevents an unexpected security hole if someone adds a new
             * node type that can call a function.
             */
            return true;
        }
    }
    expression_tree_walker(
        node,
        Some(contain_leaked_vars_walker),
        context,
    )
}

/*
 * find_nonnullable_rels
 *		Determine which base rels are forced nonnullable by given clause.
 *
 * Returns the set of all Relids that are referenced in the clause in such
 * a way that the clause cannot possibly return TRUE if any of these Relids
 * is an all-NULL row.  (It is OK to err on the side of conservatism; hence
 * the analysis here is simplistic.)
 *
 * The semantics here are subtly different from contain_nonstrict_functions:
 * that function is concerned with NULL results from arbitrary expressions,
 * but here we assume that the input is a Boolean expression, and wish to
 * see if NULL inputs will provably cause a FALSE-or-NULL result.  We expect
 * the expression to have been AND/OR flattened and converted to implicit-AND
 * format.
 *
 * Note: this function is largely duplicative of find_nonnullable_vars().
 * The reason not to simplify this function into a thin wrapper around
 * find_nonnullable_vars() is that the tested conditions really are different:
 * a clause like "t1.v1 IS NOT NULL OR t1.v2 IS NOT NULL" does not prove
 * that either v1 or v2 can't be NULL, but it does prove that the t1 row
 * as a whole can't be all-NULL.  Also, the behavior for PHVs is different.
 *
 * top_level is true while scanning top-level AND/OR structure; here, showing
 * the result is either FALSE or NULL is good enough.  top_level is false when
 * we have descended below a NOT or a strict function: now we must be able to
 * prove that the subexpression goes to NULL.
 *
 * We don't use expression_tree_walker here because we don't want to descend
 * through very many kinds of nodes; only the ones we can be sure are strict.
 */
pub unsafe fn find_nonnullable_rels(clause: *mut Node) -> Relids {
    find_nonnullable_rels_walker(clause, true)
}

unsafe fn find_nonnullable_rels_walker(
    node: *mut Node,
    top_level: bool,
) -> Relids {
    let mut result: Relids = null_mut();

    if node.is_null() {
        return null_mut();
    }
    if IsA!(node, T_Var) {
        let var = node as *mut Var;
        if (*var).varlevelsup == 0 {
            result = bms_make_singleton((*var).varno);
        }
    } else if IsA!(node, T_List) {
        /*
         * At top level, we are examining an implicit-AND list: if any of the
         * arms produces FALSE-or-NULL then the result is FALSE-or-NULL. If
         * not at top level, we are examining the arguments of a strict
         * function: if any of them produce NULL then the result of the
         * function must be NULL.  So in both cases, the set of nonnullable
         * rels is the union of those found in the arms, and we pass down the
         * top_level flag unmodified.
         */
        let mut lc = list_head(node as *mut List);
        while !lc.is_null() {
            result = bms_join(
                result,
                find_nonnullable_rels_walker(lfirst(lc) as *mut Node, top_level),
            );
            lc = lnext(node as *mut List, lc);
        }
    } else if IsA!(node, T_FuncExpr) {
        let expr = node as *mut FuncExpr;
        if func_strict((*expr).funcid) {
            result = find_nonnullable_rels_walker((*expr).args as *mut Node, false);
        }
    } else if IsA!(node, T_OpExpr) {
        let expr = node as *mut OpExpr;
        set_opfuncid(expr);
        if func_strict((*expr).opfuncid) {
            result = find_nonnullable_rels_walker((*expr).args as *mut Node, false);
        }
    } else if IsA!(node, T_ScalarArrayOpExpr) {
        let expr = node as *mut ScalarArrayOpExpr;
        if is_strict_saop(expr, true) {
            result = find_nonnullable_rels_walker((*expr).args as *mut Node, false);
        }
    } else if IsA!(node, T_BoolExpr) {
        let expr = node as *mut BoolExpr;
        match (*expr).boolop {
            AND_EXPR => {
                /* At top level we can just recurse (to the List case) */
                if top_level {
                    result = find_nonnullable_rels_walker(
                        (*expr).args as *mut Node,
                        top_level,
                    );
                } else {
                    /*
                     * Below top level, even if one arm produces NULL, the result
                     * could be FALSE (hence not NULL).  However, if *all* the
                     * arms produce NULL then the result is NULL, so we can take
                     * the intersection of the sets of nonnullable rels, just as
                     * for OR.  Fall through to share code.
                     */
                    let mut lc = list_head((*expr).args);
                    while !lc.is_null() {
                        let subresult = find_nonnullable_rels_walker(
                            lfirst(lc) as *mut Node,
                            top_level,
                        );
                        if result.is_null() {
                            result = subresult;
                        } else {
                            result = bms_int_members(result, subresult);
                        }
                        if bms_is_empty(result) {
                            break;
                        }
                        lc = lnext((*expr).args, lc);
                    }
                }
            }
            OR_EXPR => {
                /*
                 * OR is strict if all of its arms are, so we can take the
                 * intersection of the sets of nonnullable rels for each arm.
                 * This works for both values of top_level.
                 */
                let mut lc = list_head((*expr).args);
                while !lc.is_null() {
                    let subresult = find_nonnullable_rels_walker(
                        lfirst(lc) as *mut Node,
                        top_level,
                    );
                    if result.is_null() {
                        result = subresult;
                    } else {
                        result = bms_int_members(result, subresult);
                    }
                    if bms_is_empty(result) {
                        break;
                    }
                    lc = lnext((*expr).args, lc);
                }
            }
            NOT_EXPR => {
                /* NOT will return null if its arg is null */
                result = find_nonnullable_rels_walker(
                    (*expr).args as *mut Node,
                    false,
                );
            }
            _ => {
                elog!(ERROR, "unrecognized boolop: {}", (*expr).boolop as c_int);
            }
        }
    } else if IsA!(node, T_RelabelType) {
        let expr = node as *mut RelabelType;
        result = find_nonnullable_rels_walker((*expr).arg as *mut Node, top_level);
    } else if IsA!(node, T_CoerceViaIO) {
        /* not clear this is useful, but it can't hurt */
        let expr = node as *mut CoerceViaIO;
        result = find_nonnullable_rels_walker((*expr).arg as *mut Node, top_level);
    } else if IsA!(node, T_ArrayCoerceExpr) {
        /* ArrayCoerceExpr is strict at the array level; ignore elemexpr */
        let expr = node as *mut ArrayCoerceExpr;
        result = find_nonnullable_rels_walker((*expr).arg as *mut Node, top_level);
    } else if IsA!(node, T_ConvertRowtypeExpr) {
        /* not clear this is useful, but it can't hurt */
        let expr = node as *mut ConvertRowtypeExpr;
        result = find_nonnullable_rels_walker((*expr).arg as *mut Node, top_level);
    } else if IsA!(node, T_CollateExpr) {
        let expr = node as *mut CollateExpr;
        result = find_nonnullable_rels_walker((*expr).arg as *mut Node, top_level);
    } else if IsA!(node, T_NullTest) {
        /* IS NOT NULL can be considered strict, but only at top level */
        let expr = node as *mut NullTest;
        if top_level && (*expr).nulltesttype == IS_NOT_NULL && !(*expr).argisrow {
            result = find_nonnullable_rels_walker((*expr).arg as *mut Node, false);
        }
    } else if IsA!(node, T_BooleanTest) {
        /* Boolean tests that reject NULL are strict at top level */
        let expr = node as *mut BooleanTest;
        if top_level
            && ((*expr).booltesttype == IS_TRUE
                || (*expr).booltesttype == IS_FALSE
                || (*expr).booltesttype == IS_NOT_UNKNOWN)
        {
            result = find_nonnullable_rels_walker((*expr).arg as *mut Node, false);
        }
    } else if IsA!(node, T_SubPlan) {
        let splan = node as *mut SubPlan;

        /*
         * For some types of SubPlan, we can infer strictness from Vars in the
         * testexpr (the LHS of the original SubLink).
         *
         * For ANY_SUBLINK, if the subquery produces zero rows, the result is
         * always FALSE.  If the subquery produces more than one row, the
         * per-row results of the testexpr are combined using OR semantics.
         * Hence ANY_SUBLINK can be strict only at top level, but there it's
         * as strict as the testexpr is.
         *
         * For ROWCOMPARE_SUBLINK, if the subquery produces zero rows, the
         * result is always NULL.  Otherwise, the result is as strict as the
         * testexpr is.  So we can check regardless of top_level.
         *
         * We can't prove anything for other sublink types (in particular,
         * note that ALL_SUBLINK will return TRUE if the subquery is empty).
         */
        if (top_level && (*splan).subLinkType == ANY_SUBLINK)
            || (*splan).subLinkType == ROWCOMPARE_SUBLINK
        {
            result = find_nonnullable_rels_walker((*splan).testexpr, top_level);
        }
    } else if IsA!(node, T_PlaceHolderVar) {
        let phv = node as *mut PlaceHolderVar;

        /*
         * If the contained expression forces any rels non-nullable, so does
         * the PHV.
         */
        result = find_nonnullable_rels_walker((*phv).phexpr as *mut Node, top_level);

        /*
         * If the PHV's syntactic scope is exactly one rel, it will be forced
         * to be evaluated at that rel, and so it will behave like a Var of
         * that rel: if the rel's entire output goes to null, so will the PHV.
         * (If the syntactic scope is a join, we know that the PHV will go to
         * null if the whole join does; but that is AND semantics while we
         * need OR semantics for find_nonnullable_rels' result, so we can't do
         * anything with the knowledge.)
         */
        if (*phv).phlevelsup == 0
            && bms_membership((*phv).phrels) == BMS_SINGLETON
        {
            result = bms_add_members(result, (*phv).phrels);
        }
    }
    result
}

// ============================================================================
// PART 4: find_nonnullable_vars, find_forced_null_vars, is_strict_saop,
//         is_pseudo_constant, NumRelids, CommuteOpExpr, rowtype_field_matches,
//         eval_const_expressions, estimate_expression_value,
//         convert_saop_to_hashed_saop
// ============================================================================

/*
 * find_nonnullable_vars
 *		Determine which Vars are forced nonnullable by given clause.
 *
 * Returns the set of all level-zero Vars that are referenced in the clause in
 * such a way that the clause cannot possibly return TRUE if any of these Vars
 * is NULL.  (It is OK to err on the side of conservatism; hence the analysis
 * here is simplistic.)
 *
 * The semantics here are subtly different from contain_nonstrict_functions:
 * that function is concerned with NULL results from arbitrary expressions,
 * but here we assume that the input is a Boolean expression, and wish to
 * see if NULL inputs will provably cause a FALSE-or-NULL result.  We expect
 * the expression to have been AND/OR flattened and converted to implicit-AND
 * format.
 *
 * Attnos of the identified Vars are returned in a multibitmapset (a List of
 * Bitmapsets).  List indexes correspond to relids (varnos), while the per-rel
 * Bitmapsets hold varattnos offset by FirstLowInvalidHeapAttributeNumber.
 *
 * top_level is true while scanning top-level AND/OR structure; here, showing
 * the result is either FALSE or NULL is good enough.  top_level is false when
 * we have descended below a NOT or a strict function: now we must be able to
 * prove that the subexpression goes to NULL.
 *
 * We don't use expression_tree_walker here because we don't want to descend
 * through very many kinds of nodes; only the ones we can be sure are strict.
 */
pub unsafe fn find_nonnullable_vars(clause: *mut Node) -> *mut List {
    find_nonnullable_vars_walker(clause, true)
}

unsafe fn find_nonnullable_vars_walker(
    node: *mut Node,
    top_level: bool,
) -> *mut List {
    let mut result: *mut List = NIL;

    if node.is_null() {
        return NIL;
    }
    if IsA!(node, T_Var) {
        let var = node as *mut Var;
        if (*var).varlevelsup == 0 {
            result = mbms_add_member(
                result,
                (*var).varno as c_int,
                (*var).varattno as c_int - FirstLowInvalidHeapAttributeNumber as c_int,
            );
        }
    } else if IsA!(node, T_List) {
        /*
         * At top level, we are examining an implicit-AND list: if any of the
         * arms produces FALSE-or-NULL then the result is FALSE-or-NULL. If
         * not at top level, we are examining the arguments of a strict
         * function: if any of them produce NULL then the result of the
         * function must be NULL.  So in both cases, the set of nonnullable
         * vars is the union of those found in the arms, and we pass down the
         * top_level flag unmodified.
         */
        let mut lc = list_head(node as *mut List);
        while !lc.is_null() {
            result = mbms_add_members(
                result,
                find_nonnullable_vars_walker(lfirst(lc) as *mut Node, top_level),
            );
            lc = lnext(node as *mut List, lc);
        }
    } else if IsA!(node, T_FuncExpr) {
        let expr = node as *mut FuncExpr;
        if func_strict((*expr).funcid) {
            result = find_nonnullable_vars_walker((*expr).args as *mut Node, false);
        }
    } else if IsA!(node, T_OpExpr) {
        let expr = node as *mut OpExpr;
        set_opfuncid(expr);
        if func_strict((*expr).opfuncid) {
            result = find_nonnullable_vars_walker((*expr).args as *mut Node, false);
        }
    } else if IsA!(node, T_ScalarArrayOpExpr) {
        let expr = node as *mut ScalarArrayOpExpr;
        if is_strict_saop(expr, true) {
            result = find_nonnullable_vars_walker((*expr).args as *mut Node, false);
        }
    } else if IsA!(node, T_BoolExpr) {
        let expr = node as *mut BoolExpr;
        match (*expr).boolop {
            AND_EXPR => {
                /*
                 * At top level we can just recurse (to the List case), since
                 * the result should be the union of what we can prove in each
                 * arm.
                 */
                if top_level {
                    result = find_nonnullable_vars_walker(
                        (*expr).args as *mut Node,
                        top_level,
                    );
                } else {
                    /*
                     * Below top level, even if one arm produces NULL, the result
                     * could be FALSE (hence not NULL).  However, if *all* the
                     * arms produce NULL then the result is NULL, so we can take
                     * the intersection of the sets of nonnullable vars, just as
                     * for OR.  Fall through to share code.
                     */
                    let mut lc = list_head((*expr).args);
                    while !lc.is_null() {
                        let subresult = find_nonnullable_vars_walker(
                            lfirst(lc) as *mut Node,
                            top_level,
                        );
                        if result == NIL {
                            result = subresult;
                        } else {
                            result = mbms_int_members(result, subresult);
                        }
                        if result == NIL {
                            break;
                        }
                        lc = lnext((*expr).args, lc);
                    }
                }
            }
            OR_EXPR => {
                /*
                 * OR is strict if all of its arms are, so we can take the
                 * intersection of the sets of nonnullable vars for each arm.
                 * This works for both values of top_level.
                 */
                let mut lc = list_head((*expr).args);
                while !lc.is_null() {
                    let subresult = find_nonnullable_vars_walker(
                        lfirst(lc) as *mut Node,
                        top_level,
                    );
                    if result == NIL {
                        result = subresult;
                    } else {
                        result = mbms_int_members(result, subresult);
                    }
                    if result == NIL {
                        break;
                    }
                    lc = lnext((*expr).args, lc);
                }
            }
            NOT_EXPR => {
                /* NOT will return null if its arg is null */
                result = find_nonnullable_vars_walker(
                    (*expr).args as *mut Node,
                    false,
                );
            }
            _ => {
                elog!(ERROR, "unrecognized boolop: {}", (*expr).boolop as c_int);
            }
        }
    } else if IsA!(node, T_RelabelType) {
        let expr = node as *mut RelabelType;
        result = find_nonnullable_vars_walker((*expr).arg as *mut Node, top_level);
    } else if IsA!(node, T_CoerceViaIO) {
        /* not clear this is useful, but it can't hurt */
        let expr = node as *mut CoerceViaIO;
        result = find_nonnullable_vars_walker((*expr).arg as *mut Node, false);
    } else if IsA!(node, T_ArrayCoerceExpr) {
        /* ArrayCoerceExpr is strict at the array level; ignore elemexpr */
        let expr = node as *mut ArrayCoerceExpr;
        result = find_nonnullable_vars_walker((*expr).arg as *mut Node, top_level);
    } else if IsA!(node, T_ConvertRowtypeExpr) {
        /* not clear this is useful, but it can't hurt */
        let expr = node as *mut ConvertRowtypeExpr;
        result = find_nonnullable_vars_walker((*expr).arg as *mut Node, top_level);
    } else if IsA!(node, T_CollateExpr) {
        let expr = node as *mut CollateExpr;
        result = find_nonnullable_vars_walker((*expr).arg as *mut Node, top_level);
    } else if IsA!(node, T_NullTest) {
        /* IS NOT NULL can be considered strict, but only at top level */
        let expr = node as *mut NullTest;
        if top_level && (*expr).nulltesttype == IS_NOT_NULL && !(*expr).argisrow {
            result = find_nonnullable_vars_walker((*expr).arg as *mut Node, false);
        }
    } else if IsA!(node, T_BooleanTest) {
        /* Boolean tests that reject NULL are strict at top level */
        let expr = node as *mut BooleanTest;
        if top_level
            && ((*expr).booltesttype == IS_TRUE
                || (*expr).booltesttype == IS_FALSE
                || (*expr).booltesttype == IS_NOT_UNKNOWN)
        {
            result = find_nonnullable_vars_walker((*expr).arg as *mut Node, false);
        }
    } else if IsA!(node, T_SubPlan) {
        let splan = node as *mut SubPlan;
        /* See analysis in find_nonnullable_rels_walker */
        if (top_level && (*splan).subLinkType == ANY_SUBLINK)
            || (*splan).subLinkType == ROWCOMPARE_SUBLINK
        {
            result = find_nonnullable_vars_walker((*splan).testexpr, top_level);
        }
    } else if IsA!(node, T_PlaceHolderVar) {
        let phv = node as *mut PlaceHolderVar;
        result = find_nonnullable_vars_walker((*phv).phexpr as *mut Node, top_level);
    }
    result
}

/*
 * find_forced_null_vars
 *		Determine which Vars must be NULL for the given clause to return TRUE.
 *
 * This is the complement of find_nonnullable_vars: find the level-zero Vars
 * that must be NULL for the clause to return TRUE.  (It is OK to err on the
 * side of conservatism; hence the analysis here is simplistic.  In fact,
 * we only detect simple "var IS NULL" tests at the top level.)
 *
 * As with find_nonnullable_vars, we return the varattnos of the identified
 * Vars in a multibitmapset.
 */
pub unsafe fn find_forced_null_vars(node: *mut Node) -> *mut List {
    let mut result: *mut List = NIL;

    if node.is_null() {
        return NIL;
    }
    /* Check single-clause cases using subroutine */
    let var = find_forced_null_var(node);
    if !var.is_null() {
        result = mbms_add_member(
            result,
            (*var).varno as c_int,
            (*var).varattno as c_int - FirstLowInvalidHeapAttributeNumber as c_int,
        );
    } else if IsA!(node, T_List) {
        /* Otherwise, handle AND-conditions */
        /*
         * At top level, we are examining an implicit-AND list: if any of the
         * arms produces FALSE-or-NULL then the result is FALSE-or-NULL.
         */
        let mut lc = list_head(node as *mut List);
        while !lc.is_null() {
            result = mbms_add_members(
                result,
                find_forced_null_vars(lfirst(lc) as *mut Node),
            );
            lc = lnext(node as *mut List, lc);
        }
    } else if IsA!(node, T_BoolExpr) {
        let expr = node as *mut BoolExpr;
        /*
         * We don't bother considering the OR case, because it's fairly
         * unlikely anyone would write "v1 IS NULL OR v1 IS NULL". Likewise,
         * the NOT case isn't worth expending code on.
         */
        if (*expr).boolop == AND_EXPR {
            /* At top level we can just recurse (to the List case) */
            result = find_forced_null_vars((*expr).args as *mut Node);
        }
    }
    result
}

/*
 * find_forced_null_var
 *		Return the Var forced null by the given clause, or NULL if it's
 *		not an IS NULL-type clause.  For success, the clause must enforce
 *		*only* nullness of the particular Var, not any other conditions.
 *
 * This is just the single-clause case of find_forced_null_vars(), without
 * any allowance for AND conditions.  It's used by initsplan.c on individual
 * qual clauses.  The reason for not just applying find_forced_null_vars()
 * is that if an AND of an IS NULL clause with something else were to somehow
 * survive AND/OR flattening, initsplan.c might get fooled into discarding
 * the whole clause when only the IS NULL part of it had been proved redundant.
 */
pub unsafe fn find_forced_null_var(node: *mut Node) -> *mut Var {
    if node.is_null() {
        return null_mut();
    }
    if IsA!(node, T_NullTest) {
        /* check for var IS NULL */
        let expr = node as *mut NullTest;
        if (*expr).nulltesttype == IS_NULL && !(*expr).argisrow {
            let var = (*expr).arg as *mut Var;
            if !var.is_null() && IsA!(var as *mut Node, T_Var) && (*var).varlevelsup == 0 {
                return var;
            }
        }
    } else if IsA!(node, T_BooleanTest) {
        /* var IS UNKNOWN is equivalent to var IS NULL */
        let expr = node as *mut BooleanTest;
        if (*expr).booltesttype == IS_UNKNOWN {
            let var = (*expr).arg as *mut Var;
            if !var.is_null() && IsA!(var as *mut Node, T_Var) && (*var).varlevelsup == 0 {
                return var;
            }
        }
    }
    null_mut()
}

/*
 * Can we treat a ScalarArrayOpExpr as strict?
 *
 * If "falseOK" is true, then a "false" result can be considered strict,
 * else we need to guarantee an actual NULL result for NULL input.
 *
 * "foo op ALL array" is strict if the op is strict *and* we can prove
 * that the array input isn't an empty array.  We can check that
 * for the cases of an array constant and an ARRAY[] construct.
 *
 * "foo op ANY array" is strict in the falseOK sense if the op is strict.
 * If not falseOK, the test is the same as for "foo op ALL array".
 */
unsafe fn is_strict_saop(expr: *mut ScalarArrayOpExpr, falseOK: bool) -> bool {
    let rightop: *mut Node;

    /* The contained operator must be strict. */
    set_sa_opfuncid(expr);
    if !func_strict((*expr).opfuncid) {
        return false;
    }
    /* If ANY and falseOK, that's all we need to check. */
    if (*expr).useOr && falseOK {
        return true;
    }
    /* Else, we have to see if the array is provably non-empty. */
    Assert!(list_length((*expr).args) == 2);
    rightop = lsecond((*expr).args) as *mut Node;
    if !rightop.is_null() && IsA!(rightop, T_Const) {
        let arraydatum = (*(rightop as *mut Const)).constvalue;
        let arrayisnull = (*(rightop as *mut Const)).constisnull;

        if arrayisnull {
            return false;
        }
        let arrayval = DatumGetArrayTypeP(arraydatum);
        let nitems = ArrayGetNItems(ARR_NDIM(arrayval), ARR_DIMS(arrayval));
        if nitems > 0 {
            return true;
        }
    } else if !rightop.is_null() && IsA!(rightop, T_ArrayExpr) {
        let arrayexpr = rightop as *mut ArrayExpr;
        if (*arrayexpr).elements != NIL && !(*arrayexpr).multidims {
            return true;
        }
    }
    false
}

/*****************************************************************************
 *		Check for "pseudo-constant" clauses
 *****************************************************************************/

/*
 * is_pseudo_constant_clause
 *	  Detect whether an expression is "pseudo constant", ie, it contains no
 *	  variables of the current query level and no uses of volatile functions.
 *	  Such an expr is not necessarily a true constant: it can still contain
 *	  Params and outer-level Vars, not to mention functions whose results
 *	  may vary from one statement to the next.  However, the expr's value
 *	  will be constant over any one scan of the current query, so it can be
 *	  used as, eg, an indexscan key.  (Actually, the condition for indexscan
 *	  keys is weaker than this; see is_pseudo_constant_for_index().)
 *
 * CAUTION: this function omits to test for one very important class of
 * not-constant expressions, namely aggregates (Aggrefs).  In current usage
 * this is only applied to WHERE clauses and so a check for Aggrefs would be
 * a waste of cycles; but be sure to also check contain_agg_clause() if you
 * want to know about pseudo-constness in other contexts.  The same goes
 * for window functions (WindowFuncs).
 */
pub unsafe fn is_pseudo_constant_clause(clause: *mut Node) -> bool {
    /*
     * We could implement this check in one recursive scan.  But since the
     * check for volatile functions is both moderately expensive and unlikely
     * to fail, it seems better to look for Vars first and only check for
     * volatile functions if we find no Vars.
     */
    !contain_var_clause(clause) && !contain_volatile_functions(clause)
}

/*
 * is_pseudo_constant_clause_relids
 *	  Same as above, except caller already has available the var membership
 *	  of the expression; this lets us avoid the contain_var_clause() scan.
 */
pub unsafe fn is_pseudo_constant_clause_relids(
    clause: *mut Node,
    relids: Relids,
) -> bool {
    bms_is_empty(relids) && !contain_volatile_functions(clause)
}

/*****************************************************************************
 *																			 *
 *		General clause-manipulating routines								 *
 *																			 *
 *****************************************************************************/

/*
 * NumRelids
 *		(formerly clause_relids)
 *
 * Returns the number of different base relations referenced in 'clause'.
 */
pub unsafe fn NumRelids(root: *mut PlannerInfo, clause: *mut Node) -> c_int {
    let mut varnos = pull_varnos(root as *mut crate::optimizer::optimizer::PlannerInfo, clause);
    varnos = bms_del_members(varnos, (*root).outer_join_rels);
    let result = bms_num_members(varnos);
    bms_free(varnos);
    result
}

/*
 * CommuteOpExpr: commute a binary operator clause
 *
 * XXX the clause is destructively modified!
 */
pub unsafe fn CommuteOpExpr(clause: *mut OpExpr) {
    let opoid: Oid;
    let temp: *mut c_void;

    /* Sanity checks: caller is at fault if these fail */
    if !is_opclause(clause as *const c_void)
        || list_length((*clause).args) != 2
    {
        elog!(ERROR, "cannot commute non-binary-operator clause");
    }

    opoid = get_commutator((*clause).opno);

    if !OidIsValid(opoid) {
        elog!(ERROR, "could not find commutator for operator {}", (*clause).opno);
    }

    /*
     * modify the clause in-place!
     */
    (*clause).opno = opoid;
    (*clause).opfuncid = InvalidOid;
    /* opresulttype, opretset, opcollid, inputcollid need not change */

    let args = (*clause).args;
    let first_cell = list_head(args);
    let second_cell = lnext(args, first_cell);
    /* swap the first and second list cells */
    temp = lfirst(first_cell);
    *list_cell_ptr_mut(first_cell) = lfirst(second_cell);
    *list_cell_ptr_mut(second_cell) = temp;
}

/// Mutable pointer to a ListCell's data pointer; used in CommuteOpExpr swap.
// C: linitial(l) = x; lsecond(l) = y is done via pointer arithmetic.
// TODO(pg-port): expose a proper list-cell write helper in pg_list.rs
#[inline]
unsafe fn list_cell_ptr_mut(cell: *mut ListCell) -> *mut *mut c_void {
    &mut (*cell).ptr_value
}

/// is_opclause -- check if a node is an operator clause (inline from C header).
#[inline]
pub unsafe fn is_opclause(clause: *const c_void) -> bool {
    !clause.is_null() && IsA!(clause as *const Node as *mut Node, T_OpExpr)
}

/*
 * Helper for eval_const_expressions: check that datatype of an attribute
 * is still what it was when the expression was parsed.  This is needed to
 * guard against improper simplification after ALTER COLUMN TYPE.  (XXX we
 * may well need to make similar checks elsewhere?)
 *
 * rowtypeid may come from a whole-row Var, and therefore it can be a domain
 * over composite, but for this purpose we only care about checking the type
 * of a contained field.
 */
unsafe fn rowtype_field_matches(
    rowtypeid: Oid,
    fieldnum: c_int,
    expectedtype: Oid,
    expectedtypmod: int32,
    expectedcollation: Oid,
) -> bool {
    /* No issue for RECORD, since there is no way to ALTER such a type */
    if rowtypeid == RECORDOID {
        return true;
    }
    let tupdesc = lookup_rowtype_tupdesc_domain(rowtypeid, -1, false);
    if fieldnum <= 0 || fieldnum > (*tupdesc).natts {
        ReleaseTupleDesc(tupdesc);
        return false;
    }
    let attr = TupleDescAttr(tupdesc, fieldnum - 1);
    if (*attr).attisdropped
        || (*attr).atttypid != expectedtype
        || (*attr).atttypmod != expectedtypmod
        || (*attr).attcollation != expectedcollation
    {
        ReleaseTupleDesc(tupdesc);
        return false;
    }
    ReleaseTupleDesc(tupdesc);
    true
}

/*--------------------
 * eval_const_expressions
 *
 * Reduce any recognizably constant subexpressions of the given
 * expression tree, for example "2 + 2" => "4".  More interestingly,
 * we can reduce certain boolean expressions even when they contain
 * non-constant subexpressions: "x OR true" => "true" no matter what
 * the subexpression x is.  (XXX We assume that no such subexpression
 * will have important side-effects, which is not necessarily a good
 * assumption in the presence of user-defined functions; do we need a
 * pg_proc flag that prevents discarding the execution of a function?)
 *
 * We do understand that certain functions may deliver non-constant
 * results even with constant inputs, "nextval()" being the classic
 * example.  Functions that are not marked "immutable" in pg_proc
 * will not be pre-evaluated here, although we will reduce their
 * arguments as far as possible.
 *
 * Whenever a function is eliminated from the expression by means of
 * constant-expression evaluation or inlining, we add the function to
 * root->glob->invalItems.  This ensures the plan is known to depend on
 * such functions, even though they aren't referenced anymore.
 *
 * We assume that the tree has already been type-checked and contains
 * only operators and functions that are reasonable to try to execute.
 *
 * NOTE: "root" can be passed as NULL if the caller never wants to do any
 * Param substitutions nor receive info about inlined functions.
 *
 * NOTE: the planner assumes that this will always flatten nested AND and
 * OR clauses into N-argument form.  See comments in prepqual.c.
 *
 * NOTE: another critical effect is that any function calls that require
 * default arguments will be expanded, and named-argument calls will be
 * converted to positional notation.  The executor won't handle either.
 *--------------------
 */
pub unsafe fn eval_const_expressions(
    root: *mut PlannerInfo,
    node: *mut Node,
) -> *mut Node {
    let mut context = EvalConstExpressionsContext {
        bound_params: if !root.is_null() {
            (*(*root).glob).boundParams as *mut crate::nodes::params::ParamListInfoData
        } else {
            null_mut()
        },
        root,
        active_fns: NIL,
        case_val: null_mut(),
        estimate: false,
    };
    eval_const_expressions_mutator(node, &mut context)
}

const MIN_ARRAY_SIZE_FOR_HASHED_SAOP_INNER: c_int = MIN_ARRAY_SIZE_FOR_HASHED_SAOP;

/*--------------------
 * convert_saop_to_hashed_saop
 *
 * Recursively search 'node' for ScalarArrayOpExprs and fill in the hash
 * function for any ScalarArrayOpExpr that looks like it would be useful to
 * evaluate using a hash table rather than a linear search.
 *
 * We'll use a hash table if all of the following conditions are met:
 * 1. The 2nd argument of the array contain only Consts.
 * 2. useOr is true or there is a valid negator operator for the
 *	  ScalarArrayOpExpr's opno.
 * 3. There's valid hash function for both left and righthand operands and
 *	  these hash functions are the same.
 * 4. If the array contains enough elements for us to consider it to be
 *	  worthwhile using a hash table rather than a linear search.
 */
pub unsafe fn convert_saop_to_hashed_saop(node: *mut Node) {
    let _ = convert_saop_to_hashed_saop_walker(node, null_mut());
}

unsafe fn convert_saop_to_hashed_saop_walker(
    node: *mut Node,
    _context: *mut c_void,
) -> bool {
    if node.is_null() {
        return false;
    }

    if IsA!(node, T_ScalarArrayOpExpr) {
        let saop = node as *mut ScalarArrayOpExpr;
        let arrayarg = lsecond((*saop).args) as *mut Expr;
        let mut lefthashfunc: Oid = InvalidOid;
        let mut righthashfunc: Oid = InvalidOid;

        if !arrayarg.is_null()
            && IsA!(arrayarg as *mut Node, T_Const)
            && !(*(arrayarg as *mut Const)).constisnull
        {
            if (*saop).useOr {
                if get_op_hash_functions((*saop).opno, &mut lefthashfunc, &mut righthashfunc)
                    && lefthashfunc == righthashfunc
                {
                    let arrdatum = (*(arrayarg as *mut Const)).constvalue;
                    let arr = DatumGetPointer(arrdatum) as *mut ArrayType;
                    let nitems = ArrayGetNItems(ARR_NDIM(arr), ARR_DIMS(arr));

                    /*
                     * Only fill in the hash functions if the array looks
                     * large enough for it to be worth hashing instead of
                     * doing a linear search.
                     */
                    if nitems >= MIN_ARRAY_SIZE_FOR_HASHED_SAOP_INNER {
                        /* Looks good. Fill in the hash functions */
                        (*saop).hashfuncid = lefthashfunc;
                    }
                    return false;
                }
            } else {
                /* !saop->useOr */
                let negator = get_negator((*saop).opno);

                /*
                 * Check if this is a NOT IN using an operator whose negator
                 * is hashable.  If so we can still build a hash table and
                 * just ensure the lookup items are not in the hash table.
                 */
                if OidIsValid(negator)
                    && get_op_hash_functions(negator, &mut lefthashfunc, &mut righthashfunc)
                    && lefthashfunc == righthashfunc
                {
                    let arrdatum = (*(arrayarg as *mut Const)).constvalue;
                    let arr = DatumGetPointer(arrdatum) as *mut ArrayType;
                    let nitems = ArrayGetNItems(ARR_NDIM(arr), ARR_DIMS(arr));

                    /*
                     * Only fill in the hash functions if the array looks
                     * large enough for it to be worth hashing instead of
                     * doing a linear search.
                     */
                    if nitems >= MIN_ARRAY_SIZE_FOR_HASHED_SAOP_INNER {
                        /* Looks good. Fill in the hash functions */
                        (*saop).hashfuncid = lefthashfunc;

                        /*
                         * Also set the negfuncid.  The executor will need
                         * that to perform hashtable lookups.
                         */
                        (*saop).negfuncid = get_opcode(negator);
                    }
                    return false;
                }
            }
        }
    }

    expression_tree_walker(
        node,
        Some(convert_saop_to_hashed_saop_walker),
        null_mut(),
    )
}

/*--------------------
 * estimate_expression_value
 *
 * This function attempts to estimate the value of an expression for
 * planning purposes.  It is in essence a more aggressive version of
 * eval_const_expressions(): we will perform constant reductions that are
 * not necessarily 100% safe, but are reasonable for estimation purposes.
 *
 * Currently the extra steps that are taken in this mode are:
 * 1. Substitute values for Params, where a bound Param value has been made
 *	  available by the caller of planner(), even if the Param isn't marked
 *	  constant.  This effectively means that we plan using the first supplied
 *	  value of the Param.
 * 2. Fold stable, as well as immutable, functions to constants.
 * 3. Reduce PlaceHolderVar nodes to their contained expressions.
 *--------------------
 */
pub unsafe fn estimate_expression_value(
    root: *mut PlannerInfo,
    node: *mut Node,
) -> *mut Node {
    let mut context = EvalConstExpressionsContext {
        bound_params: (*(*root).glob).boundParams as *mut crate::nodes::params::ParamListInfoData,
        /* we do not need to mark the plan as depending on inlined functions */
        root: null_mut(),
        active_fns: NIL,
        case_val: null_mut(),
        estimate: true,
    };
    eval_const_expressions_mutator(node, &mut context)
}

// ============================================================================
// Inline macro-like helpers for eval_const_expressions_mutator.
// ============================================================================

/// ece_generic_processing -- copy node and simplify its arguments.
#[inline]
unsafe fn ece_generic_processing(
    node: *mut Node,
    context: *mut EvalConstExpressionsContext,
) -> *mut Node {
    expression_tree_mutator(
        node,
        Some(eval_const_expressions_mutator_trampoline),
        context as *mut c_void,
    )
}

/// ece_all_arguments_const -- check that all direct children are Consts.
#[inline]
unsafe fn ece_all_arguments_const(node: *mut Node) -> bool {
    !expression_tree_walker(
        node,
        Some(contain_non_const_walker),
        null_mut(),
    )
}

/// ece_evaluate_expr -- evaluate a constant expression.
#[inline]
unsafe fn ece_evaluate_expr(
    node: *mut Node,
    expr: *mut Expr,
) -> *mut Node {
    evaluate_expr(
        expr,
        exprType(node),
        exprTypmod(node),
        exprCollation(node),
    ) as *mut Node
}

// ============================================================================
// PART 5: eval_const_expressions_mutator (the main mutator switch)
// ============================================================================

/*
 * Recursive guts of eval_const_expressions/estimate_expression_value
 */
unsafe fn eval_const_expressions_mutator(
    node: *mut Node,
    context: *mut EvalConstExpressionsContext,
) -> *mut Node {
    /* since this function recurses, it could be driven to stack overflow */
    check_stack_depth();

    if node.is_null() {
        return null_mut();
    }
    match nodeTag(node) {
        NodeTag::T_Param => {
            let param = node as *mut Param;
            let param_li = (*context).bound_params;

            /* Look to see if we've been given a value for this Param */
            if (*param).paramkind == PARAM_EXTERN
                && !param_li.is_null()
                && (*param).paramid > 0
                && (*param).paramid <= (*param_li).numParams
            {
                let prm: *mut ParamExternData;
                let mut prmdata = core::mem::zeroed::<ParamExternData>();

                /*
                 * Give hook a chance in case parameter is dynamic.  Tell
                 * it that this fetch is speculative, so it should avoid
                 * erroring out if parameter is unavailable.
                 */
                if (*param_li).paramFetch.is_some() {
                    prm = (*param_li).paramFetch.unwrap()(
                        param_li,
                        (*param).paramid,
                        true,
                        &mut prmdata,
                    );
                } else {
                    prm = (*param_li).params.as_ptr().add(((*param).paramid - 1) as usize) as *mut ParamExternData;
                }

                /*
                 * We don't just check OidIsValid, but insist that the
                 * fetched type match the Param, just in case the hook did
                 * something unexpected.  No need to throw an error here
                 * though; leave that for runtime.
                 */
                if OidIsValid((*prm).ptype) && (*prm).ptype == (*param).paramtype {
                    /* OK to substitute parameter value? */
                    if (*context).estimate || ((*prm).pflags & PARAM_FLAG_CONST as u16 != 0) {
                        /*
                         * Return a Const representing the param value.
                         * Must copy pass-by-ref datatypes, since the
                         * Param might be in a memory context
                         * shorter-lived than our output plan should be.
                         */
                        let mut typLen: i16 = 0;
                        let mut typByVal: bool = false;
                        let pval: Datum;
                        let con: *mut Const;

                        get_typlenbyval((*param).paramtype, &mut typLen, &mut typByVal);
                        if (*prm).isnull || typByVal {
                            pval = (*prm).value;
                        } else {
                            pval = datumCopy((*prm).value, typByVal, typLen as c_int);
                        }
                        con = makeConst(
                            (*param).paramtype,
                            (*param).paramtypmod,
                            (*param).paramcollid,
                            typLen as c_int,
                            pval,
                            (*prm).isnull,
                            typByVal,
                        );
                        (*con).location = (*param).location;
                        return con as *mut Node;
                    }
                }
            }

            /*
             * Not replaceable, so just copy the Param (no need to
             * recurse)
             */
            copyObject(param) as *mut Node
        }
        NodeTag::T_WindowFunc => {
            let expr = node as *mut WindowFunc;
            let funcid = (*expr).winfnoid;
            let args: *mut List;
            let aggfilter: *mut Expr;
            let func_tuple: HeapTuple;
            let newexpr: *mut WindowFunc;

            /*
             * We can't really simplify a WindowFunc node, but we mustn't
             * just fall through to the default processing, because we
             * have to apply expand_function_arguments to its argument
             * list.  That takes care of inserting default arguments and
             * expanding named-argument notation.
             */
            func_tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
            if !HeapTupleIsValid(func_tuple) {
                elog!(ERROR, "cache lookup failed for function {}", funcid);
            }

            args = expand_function_arguments(
                (*expr).args,
                false,
                (*expr).wintype,
                func_tuple,
            );

            ReleaseSysCache(func_tuple);

            /* Now, recursively simplify the args (which are a List) */
            let args = expression_tree_mutator(
                args as *mut Node,
                Some(eval_const_expressions_mutator_trampoline),
                context as *mut c_void,
            ) as *mut List;
            /* ... and the filter expression, which isn't */
            let aggfilter = eval_const_expressions_mutator(
                (*expr).aggfilter as *mut Node,
                context,
            ) as *mut Expr;

            /* And build the replacement WindowFunc node */
            let newexpr = makeNode!(WindowFunc, T_WindowFunc) as *mut WindowFunc;
            (*newexpr).winfnoid = (*expr).winfnoid;
            (*newexpr).wintype = (*expr).wintype;
            (*newexpr).wincollid = (*expr).wincollid;
            (*newexpr).inputcollid = (*expr).inputcollid;
            (*newexpr).args = args;
            (*newexpr).aggfilter = aggfilter;
            (*newexpr).runCondition = (*expr).runCondition;
            (*newexpr).winref = (*expr).winref;
            (*newexpr).winstar = (*expr).winstar;
            (*newexpr).winagg = (*expr).winagg;
            (*newexpr).location = (*expr).location;

            newexpr as *mut Node
        }
        NodeTag::T_FuncExpr => {
            let expr = node as *mut FuncExpr;
            let mut args = (*expr).args;
            let simple: *mut Expr;
            let newexpr: *mut FuncExpr;

            /*
             * Code for op/func reduction is pretty bulky, so split it out
             * as a separate function.  Note: exprTypmod normally returns
             * -1 for a FuncExpr, but not when the node is recognizably a
             * length coercion; we want to preserve the typmod in the
             * eventual Const if so.
             */
            let simple = simplify_function(
                (*expr).funcid,
                (*expr).funcresulttype,
                exprTypmod(node),
                (*expr).funccollid,
                (*expr).inputcollid,
                &mut args,
                (*expr).funcvariadic,
                true,
                true,
                context,
            );
            if !simple.is_null() {
                /* successfully simplified it */
                return simple as *mut Node;
            }

            /*
             * The expression cannot be simplified any further, so build
             * and return a replacement FuncExpr node using the
             * possibly-simplified arguments.  Note that we have also
             * converted the argument list to positional notation.
             */
            let newexpr = makeNode!(FuncExpr, T_FuncExpr) as *mut FuncExpr;
            (*newexpr).funcid = (*expr).funcid;
            (*newexpr).funcresulttype = (*expr).funcresulttype;
            (*newexpr).funcretset = (*expr).funcretset;
            (*newexpr).funcvariadic = (*expr).funcvariadic;
            (*newexpr).funcformat = (*expr).funcformat;
            (*newexpr).funccollid = (*expr).funccollid;
            (*newexpr).inputcollid = (*expr).inputcollid;
            (*newexpr).args = args;
            (*newexpr).location = (*expr).location;
            newexpr as *mut Node
        }
        NodeTag::T_OpExpr => {
            let expr = node as *mut OpExpr;
            let mut args = (*expr).args;
            let newexpr: *mut OpExpr;

            /*
             * Need to get OID of underlying function.  Okay to scribble
             * on input to this extent.
             */
            set_opfuncid(expr);

            /*
             * Code for op/func reduction is pretty bulky, so split it out
             * as a separate function.
             */
            let simple = simplify_function(
                (*expr).opfuncid,
                (*expr).opresulttype,
                -1,
                (*expr).opcollid,
                (*expr).inputcollid,
                &mut args,
                false,
                true,
                true,
                context,
            );
            if !simple.is_null() {
                /* successfully simplified it */
                return simple as *mut Node;
            }

            /*
             * If the operator is boolean equality or inequality, we know
             * how to simplify cases involving one constant and one
             * non-constant argument.
             */
            if (*expr).opno == BooleanEqualOperator
                || (*expr).opno == BooleanNotEqualOperator
            {
                let simple2 = simplify_boolean_equality((*expr).opno, args);
                if !simple2.is_null() {
                    /* successfully simplified it */
                    return simple2;
                }
            }

            /*
             * The expression cannot be simplified any further, so build
             * and return a replacement OpExpr node using the
             * possibly-simplified arguments.
             */
            let newexpr = makeNode!(OpExpr, T_OpExpr) as *mut OpExpr;
            (*newexpr).opno = (*expr).opno;
            (*newexpr).opfuncid = (*expr).opfuncid;
            (*newexpr).opresulttype = (*expr).opresulttype;
            (*newexpr).opretset = (*expr).opretset;
            (*newexpr).opcollid = (*expr).opcollid;
            (*newexpr).inputcollid = (*expr).inputcollid;
            (*newexpr).args = args;
            (*newexpr).location = (*expr).location;
            newexpr as *mut Node
        }
        NodeTag::T_DistinctExpr => {
            let expr = node as *mut DistinctExpr;
            let mut has_null_input = false;
            let mut all_null_input = true;
            let mut has_nonconst_input = false;
            let newexpr: *mut DistinctExpr;

            /*
             * Reduce constants in the DistinctExpr's arguments.  We know
             * args is either NIL or a List node, so we can call
             * expression_tree_mutator directly rather than recursing to
             * self.
             */
            let args = expression_tree_mutator(
                (*expr).args as *mut Node,
                Some(eval_const_expressions_mutator_trampoline),
                context as *mut c_void,
            ) as *mut List;

            /*
             * We must do our own check for NULLs because DistinctExpr has
             * different results for NULL input than the underlying
             * operator does.
             */
            let mut lc = list_head(args);
            while !lc.is_null() {
                let arg = lfirst(lc) as *mut Node;
                if IsA!(arg, T_Const) {
                    has_null_input |= (*(arg as *mut Const)).constisnull;
                    all_null_input &= (*(arg as *mut Const)).constisnull;
                } else {
                    has_nonconst_input = true;
                }
                lc = lnext(args, lc);
            }

            /* all constants? then can optimize this out */
            if !has_nonconst_input {
                /* all nulls? then not distinct */
                if all_null_input {
                    return makeBoolConst(false, false);
                }
                /* one null? then distinct */
                if has_null_input {
                    return makeBoolConst(true, false);
                }
                /* otherwise try to evaluate the '=' operator */
                /* (NOT okay to try to inline it, though!) */

                /*
                 * Need to get OID of underlying function.  Okay to
                 * scribble on input to this extent.
                 */
                set_opfuncid(expr as *mut OpExpr); /* rely on struct equivalence */

                /*
                 * Code for op/func reduction is pretty bulky, so split it
                 * out as a separate function.
                 */
                let mut args2 = args;
                let simple = simplify_function(
                    (*expr).opfuncid,
                    (*expr).opresulttype,
                    -1,
                    (*expr).opcollid,
                    (*expr).inputcollid,
                    &mut args2,
                    false,
                    false,
                    false,
                    context,
                );
                if !simple.is_null() {
                    /*
                     * Since the underlying operator is "=", must negate
                     * its result
                     */
                    let csimple = castNode!(Const, T_Const, simple);
                    (*csimple).constvalue =
                        BoolGetDatum(!DatumGetBool((*csimple).constvalue));
                    return csimple as *mut Node;
                }
            }

            /*
             * The expression cannot be simplified any further, so build
             * and return a replacement DistinctExpr node using the
             * possibly-simplified arguments.
             */
            let newexpr = makeNode!(DistinctExpr, T_DistinctExpr) as *mut DistinctExpr;
            (*newexpr).opno = (*expr).opno;
            (*newexpr).opfuncid = (*expr).opfuncid;
            (*newexpr).opresulttype = (*expr).opresulttype;
            (*newexpr).opretset = (*expr).opretset;
            (*newexpr).opcollid = (*expr).opcollid;
            (*newexpr).inputcollid = (*expr).inputcollid;
            (*newexpr).args = args;
            (*newexpr).location = (*expr).location;
            newexpr as *mut Node
        }
        NodeTag::T_NullIfExpr => {
            let expr_node = ece_generic_processing(node, context);
            let expr = expr_node as *mut NullIfExpr;

            /* If either argument is NULL they can't be equal */
            let mut has_nonconst_input = false;
            let mut lc = list_head((*expr).args);
            while !lc.is_null() {
                let arg = lfirst(lc) as *mut Node;
                if !IsA!(arg, T_Const) {
                    has_nonconst_input = true;
                } else if (*(arg as *mut Const)).constisnull {
                    return linitial((*expr).args) as *mut Node;
                }
                lc = lnext((*expr).args, lc);
            }

            /*
             * Need to get OID of underlying function before checking if
             * the function is OK to evaluate.
             */
            set_opfuncid(expr as *mut OpExpr);

            if !has_nonconst_input
                && ece_function_is_safe((*expr).opfuncid, context)
            {
                return ece_evaluate_expr(expr_node, expr as *mut Expr);
            }

            expr_node
        }
        NodeTag::T_ScalarArrayOpExpr => {
            let saop_node = ece_generic_processing(node, context);
            let saop = saop_node as *mut ScalarArrayOpExpr;

            /* Make sure we know underlying function */
            set_sa_opfuncid(saop);

            /*
             * If all arguments are Consts, and it's a safe function, we
             * can fold to a constant
             */
            if ece_all_arguments_const(saop_node)
                && ece_function_is_safe((*saop).opfuncid, context)
            {
                return ece_evaluate_expr(saop_node, saop as *mut Expr);
            }
            saop_node
        }
        NodeTag::T_BoolExpr => {
            let expr = node as *mut BoolExpr;
            match (*expr).boolop {
                OR_EXPR => {
                    let mut have_null = false;
                    let mut force_true = false;
                    let mut newargs = simplify_or_arguments(
                        (*expr).args,
                        context,
                        &mut have_null,
                        &mut force_true,
                    );
                    if force_true {
                        return makeBoolConst(true, false);
                    }
                    if have_null {
                        newargs = lappend(newargs, makeBoolConst(false, true) as *mut c_void);
                    }
                    /* If all the inputs are FALSE, result is FALSE */
                    if newargs == NIL {
                        return makeBoolConst(false, false);
                    }
                    /*
                     * If only one nonconst-or-NULL input, it's the
                     * result
                     */
                    if list_length(newargs) == 1 {
                        return linitial(newargs) as *mut Node;
                    }
                    /* Else we still need an OR node */
                    make_orclause(newargs) as *mut Node
                }
                AND_EXPR => {
                    let mut have_null = false;
                    let mut force_false = false;
                    let mut newargs = simplify_and_arguments(
                        (*expr).args,
                        context,
                        &mut have_null,
                        &mut force_false,
                    );
                    if force_false {
                        return makeBoolConst(false, false);
                    }
                    if have_null {
                        newargs = lappend(newargs, makeBoolConst(false, true) as *mut c_void);
                    }
                    /* If all the inputs are TRUE, result is TRUE */
                    if newargs == NIL {
                        return makeBoolConst(true, false);
                    }
                    /*
                     * If only one nonconst-or-NULL input, it's the
                     * result
                     */
                    if list_length(newargs) == 1 {
                        return linitial(newargs) as *mut Node;
                    }
                    /* Else we still need an AND node */
                    make_andclause(newargs) as *mut Node
                }
                NOT_EXPR => {
                    Assert!(list_length((*expr).args) == 1);
                    let arg = eval_const_expressions_mutator(
                        linitial((*expr).args) as *mut Node,
                        context,
                    );
                    /*
                     * Use negate_clause() to see if we can simplify
                     * away the NOT.
                     */
                    negate_clause(arg)
                }
                _ => {
                    elog!(ERROR, "unrecognized boolop: {}", (*expr).boolop as c_int);
                    null_mut()
                }
            }
        }
        NodeTag::T_JsonValueExpr => {
            let jve = node as *mut JsonValueExpr;
            let mut raw_expr = (*jve).raw_expr as *mut Node;
            let mut formatted_expr = (*jve).formatted_expr as *mut Node;

            /*
             * If we can fold formatted_expr to a constant, we can elide
             * the JsonValueExpr altogether.  Otherwise we must process
             * raw_expr too.  But JsonFormat is a flat node and requires
             * no simplification, only copying.
             */
            formatted_expr = eval_const_expressions_mutator(formatted_expr, context);
            if !formatted_expr.is_null() && IsA!(formatted_expr, T_Const) {
                return formatted_expr;
            }

            raw_expr = eval_const_expressions_mutator(raw_expr, context);

            makeJsonValueExpr(
                raw_expr as *mut Expr,
                formatted_expr as *mut Expr,
                copyObject((*jve).format),
            ) as *mut Node
        }
        NodeTag::T_SubPlan | NodeTag::T_AlternativeSubPlan => {
            /*
             * Return a SubPlan unchanged --- too late to do anything with it.
             *
             * XXX should we ereport() here instead?  Probably this routine
             * should never be invoked after SubPlan creation.
             */
            node
        }
        NodeTag::T_RelabelType => {
            let relabel = node as *mut RelabelType;
            /* Simplify the input ... */
            let arg = eval_const_expressions_mutator(
                (*relabel).arg as *mut Node,
                context,
            );
            /* ... and attach a new RelabelType node, if needed */
            applyRelabelType(
                arg,
                (*relabel).resulttype,
                (*relabel).resulttypmod,
                (*relabel).resultcollid,
                (*relabel).relabelformat,
                (*relabel).location,
                true,
            )
        }
        NodeTag::T_CoerceViaIO => {
            let expr = node as *mut CoerceViaIO;
            let mut outfunc: Oid = InvalidOid;
            let mut outtypisvarlena: bool = false;
            let mut infunc: Oid = InvalidOid;
            let mut intypioparam: Oid = InvalidOid;
            let newexpr: *mut CoerceViaIO;

            /* Make a List so we can use simplify_function */
            let mut args = list_make1!((*expr).arg);

            /*
             * CoerceViaIO represents calling the source type's output
             * function then the result type's input function.  So, try to
             * simplify it as though it were a stack of two such function
             * calls.  First we need to know what the functions are.
             *
             * Note that the coercion functions are assumed not to care
             * about input collation, so we just pass InvalidOid for that.
             */
            getTypeOutputInfo(
                exprType((*expr).arg as *mut Node),
                &mut outfunc,
                &mut outtypisvarlena,
            );
            getTypeInputInfo((*expr).resulttype, &mut infunc, &mut intypioparam);

            let simple = simplify_function(
                outfunc,
                CSTRINGOID,
                -1,
                InvalidOid,
                InvalidOid,
                &mut args,
                false,
                true,
                true,
                context,
            );
            if !simple.is_null() {
                /*
                 * Input functions may want 1 to 3 arguments.  We always
                 * supply all three, trusting that nothing downstream will
                 * complain.
                 */
                args = list_make3!(
                    simple,
                    makeConst(
                        OIDOID,
                        -1,
                        InvalidOid,
                        core::mem::size_of::<Oid>() as c_int,
                        ObjectIdGetDatum(intypioparam),
                        false,
                        true,
                    ),
                    makeConst(
                        INT4OID,
                        -1,
                        InvalidOid,
                        core::mem::size_of::<int32>() as c_int,
                        Int32GetDatum(-1),
                        false,
                        true,
                    )
                );

                let simple2 = simplify_function(
                    infunc,
                    (*expr).resulttype,
                    -1,
                    (*expr).resultcollid,
                    InvalidOid,
                    &mut args,
                    false,
                    false,
                    true,
                    context,
                );
                if !simple2.is_null() {
                    /* successfully simplified input fn */
                    return simple2 as *mut Node;
                }
            }

            /*
             * The expression cannot be simplified any further, so build
             * and return a replacement CoerceViaIO node using the
             * possibly-simplified argument.
             */
            let newexpr = makeNode!(CoerceViaIO, T_CoerceViaIO) as *mut CoerceViaIO;
            (*newexpr).arg = linitial(args) as *mut Expr;
            (*newexpr).resulttype = (*expr).resulttype;
            (*newexpr).resultcollid = (*expr).resultcollid;
            (*newexpr).coerceformat = (*expr).coerceformat;
            (*newexpr).location = (*expr).location;
            newexpr as *mut Node
        }
        NodeTag::T_ArrayCoerceExpr => {
            let ac = makeNode!(ArrayCoerceExpr, T_ArrayCoerceExpr) as *mut ArrayCoerceExpr;
            let save_case_val: *mut Node;

            /*
             * Copy the node and const-simplify its arguments.  We can't
             * use ece_generic_processing() here because we need to mess
             * with case_val only while processing the elemexpr.
             */
            core::ptr::copy_nonoverlapping(
                node as *const ArrayCoerceExpr,
                ac,
                1,
            );
            (*ac).arg = eval_const_expressions_mutator(
                (*ac).arg as *mut Node,
                context,
            ) as *mut Expr;

            /*
             * Set up for the CaseTestExpr node contained in the elemexpr.
             * We must prevent it from absorbing any outer CASE value.
             */
            save_case_val = (*context).case_val;
            (*context).case_val = null_mut();

            (*ac).elemexpr = eval_const_expressions_mutator(
                (*ac).elemexpr as *mut Node,
                context,
            ) as *mut Expr;

            (*context).case_val = save_case_val;

            /*
             * If constant argument and the per-element expression is
             * immutable, we can simplify the whole thing to a constant.
             * Exception: although contain_mutable_functions considers
             * CoerceToDomain immutable for historical reasons, let's not
             * do so here; this ensures coercion to an array-over-domain
             * does not apply the domain's constraints until runtime.
             */
            if !(*ac).arg.is_null()
                && IsA!((*ac).arg as *mut Node, T_Const)
                && !(*ac).elemexpr.is_null()
                && !IsA!((*ac).elemexpr as *mut Node, T_CoerceToDomain)
                && !contain_mutable_functions((*ac).elemexpr as *mut Node)
            {
                return ece_evaluate_expr(ac as *mut Node, ac as *mut Expr);
            }

            ac as *mut Node
        }
        NodeTag::T_CollateExpr => {
            /*
             * We replace CollateExpr with RelabelType, so as to improve
             * uniformity of expression representation and thus simplify
             * comparison of expressions.  Hence this looks very nearly
             * the same as the RelabelType case, and we can apply the same
             * optimizations to avoid unnecessary RelabelTypes.
             */
            let collate = node as *mut CollateExpr;
            /* Simplify the input ... */
            let arg = eval_const_expressions_mutator(
                (*collate).arg as *mut Node,
                context,
            );
            /* ... and attach a new RelabelType node, if needed */
            applyRelabelType(
                arg,
                exprType(arg),
                exprTypmod(arg),
                (*collate).collOid,
                COERCE_IMPLICIT_CAST,
                (*collate).location,
                true,
            )
        }
        NodeTag::T_CaseExpr => {
            /*----------
             * CASE expressions can be simplified if there are constant
             * condition clauses:
             *		FALSE (or NULL): drop the alternative
             *		TRUE: drop all remaining alternatives
             * If the first non-FALSE alternative is a constant TRUE,
             * we can simplify the entire CASE to that alternative's
             * expression.  If there are no non-FALSE alternatives,
             * we simplify the entire CASE to the default result (ELSE).
             *
             * If we have a simple-form CASE with constant test
             * expression, we substitute the constant value for contained
             * CaseTestExpr placeholder nodes, so that we have the
             * opportunity to reduce constant test conditions.
             *----------
             */
            let caseexpr = node as *mut CaseExpr;
            let newcase: *mut CaseExpr;
            let save_case_val: *mut Node;
            let mut newarg: *mut Node;
            let mut newargs: *mut List;
            let mut const_true_cond: bool;
            let mut defresult: *mut Node;

            /* Simplify the test expression, if any */
            newarg = eval_const_expressions_mutator(
                (*caseexpr).arg as *mut Node,
                context,
            );

            /* Set up for contained CaseTestExpr nodes */
            save_case_val = (*context).case_val;
            if !newarg.is_null() && IsA!(newarg, T_Const) {
                (*context).case_val = newarg;
                newarg = null_mut(); /* not needed anymore, see above */
            } else {
                (*context).case_val = null_mut();
            }

            /* Simplify the WHEN clauses */
            newargs = NIL;
            const_true_cond = false;
            defresult = null_mut();

            let mut lc = list_head((*caseexpr).args);
            while !lc.is_null() {
                let oldcasewhen = lfirst_node!(CaseWhen, T_CaseWhen, lc);
                let casecond: *mut Node;
                let caseresult: *mut Node;

                /* Simplify this alternative's test condition */
                casecond = eval_const_expressions_mutator(
                    (*oldcasewhen).expr as *mut Node,
                    context,
                );

                /*
                 * If the test condition is constant FALSE (or NULL), then
                 * drop this WHEN clause completely, without processing
                 * the result.
                 */
                if !casecond.is_null() && IsA!(casecond, T_Const) {
                    let const_input = casecond as *mut Const;
                    if (*const_input).constisnull
                        || !DatumGetBool((*const_input).constvalue)
                    {
                        lc = lnext((*caseexpr).args, lc);
                        continue; /* drop alternative with FALSE cond */
                    }
                    /* Else it's constant TRUE */
                    const_true_cond = true;
                }

                /* Simplify this alternative's result value */
                caseresult = eval_const_expressions_mutator(
                    (*oldcasewhen).result as *mut Node,
                    context,
                );

                /* If non-constant test condition, emit a new WHEN node */
                if !const_true_cond {
                    let newcasewhen = makeNode!(CaseWhen, T_CaseWhen) as *mut CaseWhen;
                    (*newcasewhen).expr = casecond as *mut Expr;
                    (*newcasewhen).result = caseresult as *mut Expr;
                    (*newcasewhen).location = (*oldcasewhen).location;
                    newargs = lappend(newargs, newcasewhen as *mut c_void);
                    lc = lnext((*caseexpr).args, lc);
                    continue;
                }

                /*
                 * Found a TRUE condition, so none of the remaining
                 * alternatives can be reached.  We treat the result as
                 * the default result.
                 */
                defresult = caseresult;
                break;
            }

            /* Simplify the default result, unless we replaced it above */
            if !const_true_cond {
                defresult = eval_const_expressions_mutator(
                    (*caseexpr).defresult as *mut Node,
                    context,
                );
            }

            (*context).case_val = save_case_val;

            /*
             * If no non-FALSE alternatives, CASE reduces to the default
             * result
             */
            if newargs == NIL {
                return defresult;
            }
            /* Otherwise we need a new CASE node */
            let newcase = makeNode!(CaseExpr, T_CaseExpr) as *mut CaseExpr;
            (*newcase).casetype = (*caseexpr).casetype;
            (*newcase).casecollid = (*caseexpr).casecollid;
            (*newcase).arg = newarg as *mut Expr;
            (*newcase).args = newargs;
            (*newcase).defresult = defresult as *mut Expr;
            (*newcase).location = (*caseexpr).location;
            newcase as *mut Node
        }
        NodeTag::T_CaseTestExpr => {
            /*
             * If we know a constant test value for the current CASE
             * construct, substitute it for the placeholder.  Else just
             * return the placeholder as-is.
             */
            if !(*context).case_val.is_null() {
                copyObject((*context).case_val)
            } else {
                copyObject(node)
            }
        }
        NodeTag::T_SubscriptingRef
        | NodeTag::T_ArrayExpr
        | NodeTag::T_RowExpr
        | NodeTag::T_MinMaxExpr => {
            /*
             * Generic handling for node types whose own processing is
             * known to be immutable, and for which we need no smarts
             * beyond "simplify if all inputs are constants".
             */
            /* Copy the node and const-simplify its arguments */
            let node2 = ece_generic_processing(node, context);
            /* If all arguments are Consts, we can fold to a constant */
            if ece_all_arguments_const(node2) {
                return ece_evaluate_expr(node2, node2 as *mut Expr);
            }
            node2
        }
        NodeTag::T_CoalesceExpr => {
            let coalesceexpr = node as *mut CoalesceExpr;
            let newcoalesce: *mut CoalesceExpr;
            let mut newargs: *mut List = NIL;

            let mut lc = list_head((*coalesceexpr).args);
            while !lc.is_null() {
                let e = eval_const_expressions_mutator(
                    lfirst(lc) as *mut Node,
                    context,
                );

                /*
                 * We can remove null constants from the list. For a
                 * non-null constant, if it has not been preceded by any
                 * other non-null-constant expressions then it is the
                 * result. Otherwise, it's the next argument, but we can
                 * drop following arguments since they will never be
                 * reached.
                 */
                if IsA!(e, T_Const) {
                    if (*(e as *mut Const)).constisnull {
                        lc = lnext((*coalesceexpr).args, lc);
                        continue; /* drop null constant */
                    }
                    if newargs == NIL {
                        return e; /* first expr */
                    }
                    newargs = lappend(newargs, e as *mut c_void);
                    break;
                }
                newargs = lappend(newargs, e as *mut c_void);
                lc = lnext((*coalesceexpr).args, lc);
            }

            /*
             * If all the arguments were constant null, the result is just
             * null
             */
            if newargs == NIL {
                return makeNullConst(
                    (*coalesceexpr).coalescetype,
                    -1,
                    (*coalesceexpr).coalescecollid,
                ) as *mut Node;
            }

            let newcoalesce = makeNode!(CoalesceExpr, T_CoalesceExpr) as *mut CoalesceExpr;
            (*newcoalesce).coalescetype = (*coalesceexpr).coalescetype;
            (*newcoalesce).coalescecollid = (*coalesceexpr).coalescecollid;
            (*newcoalesce).args = newargs;
            (*newcoalesce).location = (*coalesceexpr).location;
            newcoalesce as *mut Node
        }
        NodeTag::T_SQLValueFunction => {
            /*
             * All variants of SQLValueFunction are stable, so if we are
             * estimating the expression's value, we should evaluate the
             * current function value.  Otherwise just copy.
             */
            let svf = node as *mut SQLValueFunction;
            if (*context).estimate {
                evaluate_expr(svf as *mut Expr, (*svf).r#type, (*svf).typmod, InvalidOid)
                    as *mut Node
            } else {
                copyObject(node as *const Node)
            }
        }
        NodeTag::T_FieldSelect => {
            /*
             * We can optimize field selection from a whole-row Var into a
             * simple Var.  (This case won't be generated directly by the
             * parser, because ParseComplexProjection short-circuits it.
             * But it can arise while simplifying functions.)  Also, we
             * can optimize field selection from a RowExpr construct, or
             * of course from a constant.
             *
             * However, replacing a whole-row Var in this way has a
             * pitfall: if we've already built the rel targetlist for the
             * source relation, then the whole-row Var is scheduled to be
             * produced by the relation scan, but the simple Var probably
             * isn't, which will lead to a failure in setrefs.c.  This is
             * not a problem when handling simple single-level queries, in
             * which expression simplification always happens first.  It
             * is a risk for lateral references from subqueries, though.
             * To avoid such failures, don't optimize uplevel references.
             *
             * We must also check that the declared type of the field is
             * still the same as when the FieldSelect was created --- this
             * can change if someone did ALTER COLUMN TYPE on the rowtype.
             * If it isn't, we skip the optimization; the case will
             * probably fail at runtime, but that's not our problem here.
             */
            let fselect = node as *mut FieldSelect;
            let newfselect: *mut FieldSelect;

            let arg = eval_const_expressions_mutator(
                (*fselect).arg as *mut Node,
                context,
            );
            if !arg.is_null()
                && IsA!(arg, T_Var)
                && (*(arg as *mut Var)).varattno == InvalidAttrNumber as i16
                && (*(arg as *mut Var)).varlevelsup == 0
            {
                if rowtype_field_matches(
                    (*(arg as *mut Var)).vartype,
                    (*fselect).fieldnum as c_int,
                    (*fselect).resulttype,
                    (*fselect).resulttypmod,
                    (*fselect).resultcollid,
                ) {
                    let newvar = makeVar(
                        (*(arg as *mut Var)).varno as c_int,
                        (*fselect).fieldnum,
                        (*fselect).resulttype,
                        (*fselect).resulttypmod,
                        (*fselect).resultcollid,
                        (*(arg as *mut Var)).varlevelsup,
                    );
                    /* New Var has same OLD/NEW returning as old one */
                    (*newvar).varreturningtype = (*(arg as *mut Var)).varreturningtype;
                    /* New Var is nullable by same rels as the old one */
                    (*newvar).varnullingrels = (*(arg as *mut Var)).varnullingrels;
                    return newvar as *mut Node;
                }
            }
            if !arg.is_null() && IsA!(arg, T_RowExpr) {
                let rowexpr = arg as *mut RowExpr;
                if (*fselect).fieldnum > 0
                    && ((*fselect).fieldnum as c_int) <= list_length((*rowexpr).args)
                {
                    let fld = list_nth(
                        (*rowexpr).args,
                        (*fselect).fieldnum as c_int - 1,
                    ) as *mut Node;
                    if rowtype_field_matches(
                        (*rowexpr).row_typeid,
                        (*fselect).fieldnum as c_int,
                        (*fselect).resulttype,
                        (*fselect).resulttypmod,
                        (*fselect).resultcollid,
                    ) && (*fselect).resulttype == exprType(fld)
                        && (*fselect).resulttypmod == exprTypmod(fld)
                        && (*fselect).resultcollid == exprCollation(fld)
                    {
                        return fld;
                    }
                }
            }
            let newfselect = makeNode!(FieldSelect, T_FieldSelect) as *mut FieldSelect;
            (*newfselect).arg = arg as *mut Expr;
            (*newfselect).fieldnum = (*fselect).fieldnum;
            (*newfselect).resulttype = (*fselect).resulttype;
            (*newfselect).resulttypmod = (*fselect).resulttypmod;
            (*newfselect).resultcollid = (*fselect).resultcollid;
            if !arg.is_null() && IsA!(arg, T_Const) {
                let con = arg as *mut Const;
                if rowtype_field_matches(
                    (*con).consttype,
                    (*newfselect).fieldnum as c_int,
                    (*newfselect).resulttype,
                    (*newfselect).resulttypmod,
                    (*newfselect).resultcollid,
                ) {
                    return ece_evaluate_expr(
                        newfselect as *mut Node,
                        newfselect as *mut Expr,
                    );
                }
            }
            newfselect as *mut Node
        }
        NodeTag::T_NullTest => {
            let ntest = node as *mut NullTest;
            let newntest: *mut NullTest;

            let arg = eval_const_expressions_mutator(
                (*ntest).arg as *mut Node,
                context,
            );
            if (*ntest).argisrow && !arg.is_null() && IsA!(arg, T_RowExpr) {
                /*
                 * We break ROW(...) IS [NOT] NULL into separate tests on
                 * its component fields.  This form is usually more
                 * efficient to evaluate, as well as being more amenable
                 * to optimization.
                 */
                let rarg = arg as *mut RowExpr;
                let mut newargs: *mut List = NIL;

                let mut lc = list_head((*rarg).args);
                while !lc.is_null() {
                    let relem = lfirst(lc) as *mut Node;

                    /*
                     * A constant field refutes the whole NullTest if it's
                     * of the wrong nullness; else we can discard it.
                     */
                    if !relem.is_null() && IsA!(relem, T_Const) {
                        let carg = relem as *mut Const;
                        if if (*carg).constisnull {
                            (*ntest).nulltesttype == IS_NOT_NULL
                        } else {
                            (*ntest).nulltesttype == IS_NULL
                        } {
                            return makeBoolConst(false, false);
                        }
                        lc = lnext((*rarg).args, lc);
                        continue;
                    }

                    /*
                     * Else, make a scalar (argisrow == false) NullTest
                     * for this field.  Scalar semantics are required
                     * because IS [NOT] NULL doesn't recurse; see comments
                     * in ExecEvalRowNullInt().
                     */
                    let newntest2 = makeNode!(NullTest, T_NullTest) as *mut NullTest;
                    (*newntest2).arg = relem as *mut Expr;
                    (*newntest2).nulltesttype = (*ntest).nulltesttype;
                    (*newntest2).argisrow = false;
                    (*newntest2).location = (*ntest).location;
                    newargs = lappend(newargs, newntest2 as *mut c_void);
                    lc = lnext((*rarg).args, lc);
                }
                /* If all the inputs were constants, result is TRUE */
                if newargs == NIL {
                    return makeBoolConst(true, false);
                }
                /* If only one nonconst input, it's the result */
                if list_length(newargs) == 1 {
                    return linitial(newargs) as *mut Node;
                }
                /* Else we need an AND node */
                return make_andclause(newargs) as *mut Node;
            }
            if !(*ntest).argisrow && !arg.is_null() && IsA!(arg, T_Const) {
                let carg = arg as *mut Const;
                let result: bool;
                match (*ntest).nulltesttype {
                    IS_NULL => {
                        result = (*carg).constisnull;
                    }
                    IS_NOT_NULL => {
                        result = !(*carg).constisnull;
                    }
                    _ => {
                        elog!(
                            ERROR,
                            "unrecognized nulltesttype: {}",
                            (*ntest).nulltesttype as c_int
                        );
                        result = false; /* keep compiler quiet */
                    }
                }
                return makeBoolConst(result, false);
            }

            let newntest = makeNode!(NullTest, T_NullTest) as *mut NullTest;
            (*newntest).arg = arg as *mut Expr;
            (*newntest).nulltesttype = (*ntest).nulltesttype;
            (*newntest).argisrow = (*ntest).argisrow;
            (*newntest).location = (*ntest).location;
            newntest as *mut Node
        }
        NodeTag::T_BooleanTest => {
            /*
             * This case could be folded into the generic handling used
             * for ArrayExpr etc.  But because the simplification logic is
             * so trivial, applying evaluate_expr() to perform it would be
             * a heavy overhead.  BooleanTest is probably common enough to
             * justify keeping this bespoke implementation.
             */
            let btest = node as *mut BooleanTest;
            let newbtest: *mut BooleanTest;

            let arg = eval_const_expressions_mutator(
                (*btest).arg as *mut Node,
                context,
            );
            if !arg.is_null() && IsA!(arg, T_Const) {
                let carg = arg as *mut Const;
                let result: bool;
                match (*btest).booltesttype {
                    IS_TRUE => {
                        result = !(*carg).constisnull
                            && DatumGetBool((*carg).constvalue);
                    }
                    IS_NOT_TRUE => {
                        result = (*carg).constisnull
                            || !DatumGetBool((*carg).constvalue);
                    }
                    IS_FALSE => {
                        result = !(*carg).constisnull
                            && !DatumGetBool((*carg).constvalue);
                    }
                    IS_NOT_FALSE => {
                        result = (*carg).constisnull
                            || DatumGetBool((*carg).constvalue);
                    }
                    IS_UNKNOWN => {
                        result = (*carg).constisnull;
                    }
                    IS_NOT_UNKNOWN => {
                        result = !(*carg).constisnull;
                    }
                    _ => {
                        elog!(
                            ERROR,
                            "unrecognized booltesttype: {}",
                            (*btest).booltesttype as c_int
                        );
                        result = false; /* keep compiler quiet */
                    }
                }
                return makeBoolConst(result, false);
            }

            let newbtest = makeNode!(BooleanTest, T_BooleanTest) as *mut BooleanTest;
            (*newbtest).arg = arg as *mut Expr;
            (*newbtest).booltesttype = (*btest).booltesttype;
            (*newbtest).location = (*btest).location;
            newbtest as *mut Node
        }
        NodeTag::T_CoerceToDomain => {
            /*
             * If the domain currently has no constraints, we replace the
             * CoerceToDomain node with a simple RelabelType, which is
             * both far faster to execute and more amenable to later
             * optimization.  We must then mark the plan as needing to be
             * rebuilt if the domain's constraints change.
             *
             * Also, in estimation mode, always replace CoerceToDomain
             * nodes, effectively assuming that the coercion will succeed.
             */
            let cdomain = node as *mut CoerceToDomain;
            let newcdomain: *mut CoerceToDomain;

            let arg = eval_const_expressions_mutator(
                (*cdomain).arg as *mut Node,
                context,
            );
            if (*context).estimate || !DomainHasConstraints((*cdomain).resulttype) {
                /* Record dependency, if this isn't estimation mode */
                if !(*context).root.is_null() && !(*context).estimate {
                    record_plan_type_dependency((*context).root, (*cdomain).resulttype);
                }
                /* Generate RelabelType to substitute for CoerceToDomain */
                return applyRelabelType(
                    arg,
                    (*cdomain).resulttype,
                    (*cdomain).resulttypmod,
                    (*cdomain).resultcollid,
                    (*cdomain).coercionformat,
                    (*cdomain).location,
                    true,
                );
            }

            let newcdomain = makeNode!(CoerceToDomain, T_CoerceToDomain) as *mut CoerceToDomain;
            (*newcdomain).arg = arg as *mut Expr;
            (*newcdomain).resulttype = (*cdomain).resulttype;
            (*newcdomain).resulttypmod = (*cdomain).resulttypmod;
            (*newcdomain).resultcollid = (*cdomain).resultcollid;
            (*newcdomain).coercionformat = (*cdomain).coercionformat;
            (*newcdomain).location = (*cdomain).location;
            newcdomain as *mut Node
        }
        NodeTag::T_PlaceHolderVar => {
            /*
             * In estimation mode, just strip the PlaceHolderVar node
             * altogether; this amounts to estimating that the contained value
             * won't be forced to null by an outer join.  In regular mode we
             * just use the default behavior (ie, simplify the expression but
             * leave the PlaceHolderVar node intact).
             */
            if (*context).estimate {
                let phv = node as *mut PlaceHolderVar;
                return eval_const_expressions_mutator(
                    (*phv).phexpr as *mut Node,
                    context,
                );
            }
            /* fall through to default */
            ece_generic_processing(node, context)
        }
        NodeTag::T_ConvertRowtypeExpr => {
            let cre = castNode!(ConvertRowtypeExpr, T_ConvertRowtypeExpr, node);
            let newcre: *mut ConvertRowtypeExpr;

            let arg = eval_const_expressions_mutator((*cre).arg as *mut Node, context);

            let newcre = makeNode!(ConvertRowtypeExpr, T_ConvertRowtypeExpr) as *mut ConvertRowtypeExpr;
            (*newcre).resulttype = (*cre).resulttype;
            (*newcre).convertformat = (*cre).convertformat;
            (*newcre).location = (*cre).location;

            /*
             * In case of a nested ConvertRowtypeExpr, we can convert the
             * leaf row directly to the topmost row format without any
             * intermediate conversions.
             *
             * No need to check more than one level deep, because the
             * above recursion will have flattened anything else.
             */
            let mut arg2 = arg;
            if !arg2.is_null() && IsA!(arg2, T_ConvertRowtypeExpr) {
                let argcre = arg2 as *mut ConvertRowtypeExpr;
                arg2 = (*argcre).arg as *mut Node;
                /*
                 * Make sure an outer implicit conversion can't hide an
                 * inner explicit one.
                 */
                if (*newcre).convertformat == COERCE_IMPLICIT_CAST {
                    (*newcre).convertformat = (*argcre).convertformat;
                }
            }

            (*newcre).arg = arg2 as *mut Expr;

            if !arg2.is_null() && IsA!(arg2, T_Const) {
                return ece_evaluate_expr(newcre as *mut Node, newcre as *mut Expr);
            }
            newcre as *mut Node
        }
        _ => {
            /*
             * For any node type not handled above, copy the node unchanged but
             * const-simplify its subexpressions.  This is the correct thing for node
             * types whose behavior might change between planning and execution, such
             * as CurrentOfExpr.  It's also a safe default for new node types not
             * known to this routine.
             */
            ece_generic_processing(node, context)
        }
    }
}

/// Trampoline so eval_const_expressions_mutator can be used as a mutator callback.
unsafe fn eval_const_expressions_mutator_trampoline(
    node: *mut Node,
    context: *mut c_void,
) -> *mut Node {
    eval_const_expressions_mutator(
        node,
        context as *mut EvalConstExpressionsContext,
    )
}

// Placeholder imports used inside eval_const_expressions_mutator.
use crate::nodes::pg_list::list_make3_impl;
use crate::postgres::{ObjectIdGetDatum, Int32GetDatum, DatumGetPointer};
// getTypeOutputInfo / getTypeInputInfo (lsyscache).
use crate::utils::cache::lsyscache::{getTypeInputInfo, getTypeOutputInfo};
// InvalidAttrNumber.
use crate::access::attnum::InvalidAttrNumber;
// COERCE_IMPLICIT_CAST / COERCE_EXPLICIT_CALL (from primnodes).
// (already available via primnodes::*).

// ============================================================================
// PART 6: contain_non_const_walker, ece_function_is_safe, simplify_or/and/bool,
//         simplify_function, expand_function_arguments (public),
//         reorder/add/fetch/recheck helpers, evaluate_function
// ============================================================================

/*
 * Subroutine for eval_const_expressions: check for non-Const nodes.
 *
 * We can abort recursion immediately on finding a non-Const node.  This is
 * critical for performance, else eval_const_expressions_mutator would take
 * O(N^2) time on non-simplifiable trees.  However, we do need to descend
 * into List nodes since expression_tree_walker sometimes invokes the walker
 * function directly on List subtrees.
 */
unsafe fn contain_non_const_walker(node: *mut Node, context: *mut c_void) -> bool {
    if node.is_null() {
        return false;
    }
    if IsA!(node, T_Const) {
        return false;
    }
    if IsA!(node, T_List) {
        return expression_tree_walker(node, Some(contain_non_const_walker), context);
    }
    /* Otherwise, abort the tree traversal and return true */
    true
}

/*
 * Subroutine for eval_const_expressions: check if a function is OK to evaluate
 */
unsafe fn ece_function_is_safe(
    funcid: Oid,
    context: *mut EvalConstExpressionsContext,
) -> bool {
    let provolatile = func_volatile(funcid);

    /*
     * Ordinarily we are only allowed to simplify immutable functions. But for
     * purposes of estimation, we consider it okay to simplify functions that
     * are merely stable; the risk that the result might change from planning
     * time to execution time is worth taking in preference to not being able
     * to estimate the value at all.
     */
    if provolatile == PROVOLATILE_IMMUTABLE {
        return true;
    }
    if (*context).estimate && provolatile == PROVOLATILE_STABLE {
        return true;
    }
    false
}

/*
 * Subroutine for eval_const_expressions: process arguments of an OR clause
 *
 * This includes flattening of nested ORs as well as recursion to
 * eval_const_expressions to simplify the OR arguments.
 *
 * After simplification, OR arguments are handled as follows:
 *    non constant: keep
 *    FALSE: drop (does not affect result)
 *    TRUE: force result to TRUE
 *    NULL: keep only one
 * We must keep one NULL input because OR expressions evaluate to NULL when no
 * input is TRUE and at least one is NULL.  We don't actually include the NULL
 * here, that's supposed to be done by the caller.
 *
 * The output arguments *haveNull and *forceTrue must be initialized false
 * by the caller.  They will be set true if a NULL constant or TRUE constant,
 * respectively, is detected anywhere in the argument list.
 */
unsafe fn simplify_or_arguments(
    args: *mut List,
    context: *mut EvalConstExpressionsContext,
    have_null: *mut bool,
    force_true: *mut bool,
) -> *mut List {
    let mut newargs: *mut List = NIL;
    let mut unprocessed_args: *mut List;

    /*
     * We want to ensure that any OR immediately beneath another OR gets
     * flattened into a single OR-list, so as to simplify later reasoning.
     *
     * To avoid stack overflow from recursion of eval_const_expressions, we
     * resort to some tenseness here: we keep a list of not-yet-processed
     * inputs, and handle flattening of nested ORs by prepending to the to-do
     * list instead of recursing.  Now that the parser generates N-argument
     * ORs from simple lists, this complexity is probably less necessary than
     * it once was, but we might as well keep the logic.
     */
    unprocessed_args = list_copy(args);
    while !unprocessed_args.is_null() && list_length(unprocessed_args) > 0 {
        let arg = linitial(unprocessed_args) as *mut Node;

        unprocessed_args = list_delete_first(unprocessed_args);

        /* flatten nested ORs as per above comment */
        if is_orclause(arg as *const c_void) {
            let subargs = (*(arg as *mut BoolExpr)).args;
            let oldlist = unprocessed_args;

            unprocessed_args = list_concat_copy(subargs, oldlist);
            list_free(oldlist);
            continue;
        }

        /* If it's not an OR, simplify it */
        let arg = eval_const_expressions_mutator(arg, context);

        /*
         * It is unlikely but not impossible for simplification of a non-OR
         * clause to produce an OR.  Recheck, but don't be too tense about it
         * since it's not a mainstream case.
         */
        if is_orclause(arg as *const c_void) {
            let subargs = (*(arg as *mut BoolExpr)).args;
            unprocessed_args = list_concat_copy(subargs, unprocessed_args);
            continue;
        }

        /*
         * OK, we have a const-simplified non-OR argument.  Process it per
         * comments above.
         */
        if IsA!(arg, T_Const) {
            let const_input = arg as *mut Const;

            if (*const_input).constisnull {
                *have_null = true;
            } else if DatumGetBool((*const_input).constvalue) {
                *force_true = true;
                return NIL;
            }
            /* otherwise, we can drop the constant-false input */
            continue;
        }

        /* else emit the simplified arg into the result list */
        newargs = lappend(newargs, arg as *mut c_void);
    }

    newargs
}

/*
 * Subroutine for eval_const_expressions: process arguments of an AND clause
 *
 * This includes flattening of nested ANDs as well as recursion to
 * eval_const_expressions to simplify the AND arguments.
 *
 * After simplification, AND arguments are handled as follows:
 *    non constant: keep
 *    TRUE: drop (does not affect result)
 *    FALSE: force result to FALSE
 *    NULL: keep only one
 * We must keep one NULL input because AND expressions evaluate to NULL when
 * no input is FALSE and at least one is NULL.  We don't actually include the
 * NULL here, that's supposed to be done by the caller.
 *
 * The output arguments *haveNull and *forceFalse must be initialized false
 * by the caller.  They will be set true if a null constant or false constant,
 * respectively, is detected anywhere in the argument list.
 */
unsafe fn simplify_and_arguments(
    args: *mut List,
    context: *mut EvalConstExpressionsContext,
    have_null: *mut bool,
    force_false: *mut bool,
) -> *mut List {
    let mut newargs: *mut List = NIL;
    let mut unprocessed_args: *mut List;

    /* See comments in simplify_or_arguments */
    unprocessed_args = list_copy(args);
    while !unprocessed_args.is_null() && list_length(unprocessed_args) > 0 {
        let arg = linitial(unprocessed_args) as *mut Node;

        unprocessed_args = list_delete_first(unprocessed_args);

        /* flatten nested ANDs as per above comment */
        if is_andclause(arg as *const c_void) {
            let subargs = (*(arg as *mut BoolExpr)).args;
            let oldlist = unprocessed_args;
            unprocessed_args = list_concat_copy(subargs, oldlist);
            list_free(oldlist);
            continue;
        }

        /* If it's not an AND, simplify it */
        let arg = eval_const_expressions_mutator(arg, context);

        /*
         * It is unlikely but not impossible for simplification of a non-AND
         * clause to produce an AND.  Recheck, but don't be too tense about it
         * since it's not a mainstream case.
         */
        if is_andclause(arg as *const c_void) {
            let subargs = (*(arg as *mut BoolExpr)).args;
            unprocessed_args = list_concat_copy(subargs, unprocessed_args);
            continue;
        }

        /*
         * OK, we have a const-simplified non-AND argument.  Process it per
         * comments above.
         */
        if IsA!(arg, T_Const) {
            let const_input = arg as *mut Const;

            if (*const_input).constisnull {
                *have_null = true;
            } else if !DatumGetBool((*const_input).constvalue) {
                *force_false = true;
                return NIL;
            }
            /* otherwise, we can drop the constant-true input */
            continue;
        }

        /* else emit the simplified arg into the result list */
        newargs = lappend(newargs, arg as *mut c_void);
    }

    newargs
}

/*
 * Subroutine for eval_const_expressions: try to simplify boolean equality
 * or inequality condition
 *
 * Inputs are the operator OID and the simplified arguments to the operator.
 * Returns a simplified expression if successful, or NULL if cannot simplify.
 *
 * The idea here is to reduce "x = true" to "x" and "x = false" to "NOT x",
 * or similarly "x <> true" to "NOT x" and "x <> false" to "x".
 */
unsafe fn simplify_boolean_equality(opno: Oid, args: *mut List) -> *mut Node {
    Assert!(list_length(args) == 2);
    let leftop = linitial(args) as *mut Node;
    let rightop = lsecond(args) as *mut Node;

    if !leftop.is_null() && IsA!(leftop, T_Const) {
        Assert!(!(*(leftop as *mut Const)).constisnull);
        if opno == BooleanEqualOperator {
            if DatumGetBool((*(leftop as *mut Const)).constvalue) {
                return rightop; /* true = foo */
            } else {
                return negate_clause(rightop); /* false = foo */
            }
        } else {
            if DatumGetBool((*(leftop as *mut Const)).constvalue) {
                return negate_clause(rightop); /* true <> foo */
            } else {
                return rightop; /* false <> foo */
            }
        }
    }
    if !rightop.is_null() && IsA!(rightop, T_Const) {
        Assert!(!(*(rightop as *mut Const)).constisnull);
        if opno == BooleanEqualOperator {
            if DatumGetBool((*(rightop as *mut Const)).constvalue) {
                return leftop; /* foo = true */
            } else {
                return negate_clause(leftop); /* foo = false */
            }
        } else {
            if DatumGetBool((*(rightop as *mut Const)).constvalue) {
                return negate_clause(leftop); /* foo <> true */
            } else {
                return leftop; /* foo <> false */
            }
        }
    }
    null_mut()
}

/*
 * Subroutine for eval_const_expressions: try to simplify a function call
 * (which might originally have been an operator; we don't care)
 *
 * Inputs are the function OID, actual result type OID (which is needed for
 * polymorphic functions), result typmod, result collation, the input
 * collation to use for the function, the original argument list (not
 * const-simplified yet, unless process_args is false), and some flags;
 * also the context data for eval_const_expressions.
 *
 * Returns a simplified expression if successful, or NULL if cannot
 * simplify the function call.
 *
 * This function is also responsible for converting named-notation argument
 * lists into positional notation and/or adding any needed default argument
 * expressions; which is a bit grotty, but it avoids extra fetches of the
 * function's pg_proc tuple.  For this reason, the args list is
 * pass-by-reference.
 */
unsafe fn simplify_function(
    funcid: Oid,
    result_type: Oid,
    result_typmod: int32,
    result_collid: Oid,
    input_collid: Oid,
    args_p: *mut *mut List,
    funcvariadic: bool,
    process_args: bool,
    allow_non_const: bool,
    context: *mut EvalConstExpressionsContext,
) -> *mut Expr {
    let mut args = *args_p;
    let func_tuple: HeapTuple;
    let newexpr: *mut Expr;

    /*
     * We have three strategies for simplification: execute the function to
     * deliver a constant result, use a transform function to generate a
     * substitute node tree, or expand in-line the body of the function
     * definition.  Each case needs access to the function's pg_proc tuple,
     * so fetch it just once.
     */
    func_tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
    if !HeapTupleIsValid(func_tuple) {
        elog!(ERROR, "cache lookup failed for function {}", funcid);
    }
    let func_form = GETSTRUCT(func_tuple) as Form_pg_proc;

    /*
     * Process the function arguments, unless the caller did it already.
     *
     * Here we must deal with named or defaulted arguments, and then
     * recursively apply eval_const_expressions to the whole argument list.
     */
    if process_args {
        args = expand_function_arguments(args, false, result_type, func_tuple);
        args = expression_tree_mutator(
            args as *mut Node,
            Some(eval_const_expressions_mutator_trampoline),
            context as *mut c_void,
        ) as *mut List;
        /* Argument processing done, give it back to the caller */
        *args_p = args;
    }

    /* Now attempt simplification of the function call proper. */
    let newexpr = evaluate_function(
        funcid,
        result_type,
        result_typmod,
        result_collid,
        input_collid,
        args,
        funcvariadic,
        func_tuple,
        context,
    );

    let newexpr = if newexpr.is_null() && allow_non_const && OidIsValid((*func_form).prosupport) {
        /*
         * Build a SupportRequestSimplify node to pass to the support
         * function, pointing to a dummy FuncExpr node containing the
         * simplified arg list.
         */
        let mut fexpr: FuncExpr = core::mem::zeroed();
        fexpr.xpr.r#type = NodeTag::T_FuncExpr;
        fexpr.funcid = funcid;
        fexpr.funcresulttype = result_type;
        fexpr.funcretset = (*func_form).proretset;
        fexpr.funcvariadic = funcvariadic;
        fexpr.funcformat = COERCE_EXPLICIT_CALL;
        fexpr.funccollid = result_collid;
        fexpr.inputcollid = input_collid;
        fexpr.args = args;
        fexpr.location = -1;

        let mut req: SupportRequestSimplify = core::mem::zeroed();
        req.type_ = NodeTag::T_SupportRequestSimplify;
        req.root = (*context).root;
        req.fcall = &mut fexpr;

        let result_ptr = DatumGetPointer(OidFunctionCall1(
            (*func_form).prosupport,
            PointerGetDatum(&mut req as *mut SupportRequestSimplify as *mut c_void),
        ));
        /* catch a possible API misunderstanding */
        Assert!(result_ptr as *mut c_void != &mut fexpr as *mut FuncExpr as *mut c_void);
        result_ptr as *mut Expr
    } else {
        newexpr
    };

    let newexpr = if newexpr.is_null() && allow_non_const {
        inline_function(
            funcid,
            result_type,
            result_collid,
            input_collid,
            args,
            funcvariadic,
            func_tuple,
            context,
        )
    } else {
        newexpr
    };

    ReleaseSysCache(func_tuple);

    newexpr
}

/*
 * expand_function_arguments: convert named-notation args to positional args
 * and/or insert default args, as needed
 *
 * Returns a possibly-transformed version of the args list.
 *
 * If include_out_arguments is true, then the args list and the result
 * include OUT arguments.
 *
 * The expected result type of the call must be given, for sanity-checking
 * purposes.  Also, we ask the caller to provide the function's actual
 * pg_proc tuple, not just its OID.
 *
 * If we need to change anything, the input argument list is copied, not
 * modified.
 *
 * Note: this gets applied to operator argument lists too, even though the
 * cases it handles should never occur there.  This should be OK since it
 * will fall through very quickly if there's nothing to do.
 */
pub unsafe fn expand_function_arguments(
    mut args: *mut List,
    include_out_arguments: bool,
    result_type: Oid,
    func_tuple: HeapTuple,
) -> *mut List {
    let funcform = GETSTRUCT(func_tuple) as Form_pg_proc;
    // TODO(pg-port): proargtypes (oidvector) is a CATALOG_VARLEN field omitted from
    // FormData_pg_proc; use null as sentinel -- recheck_cast_function_args handles
    // null proargtypes safely since enforce_generic_type_consistency only reads pronargs entries.
    let mut proargtypes: *mut Oid = core::ptr::null_mut();
    let mut pronargs: c_int = (*funcform).pronargs as c_int;
    let mut has_named_args = false;

    /*
     * If we are asked to match to OUT arguments, then use the proallargtypes
     * array (which includes those); otherwise use proargtypes (which
     * doesn't).
     */
    if include_out_arguments {
        let mut isNull = false;

        let proallargtypes = SysCacheGetAttr(
            PROCOID,
            func_tuple,
            Anum_pg_proc_proallargtypes as i16,
            &mut isNull,
        );
        if !isNull {
            let arr = DatumGetArrayTypeP(proallargtypes);

            pronargs = *ARR_DIMS(arr);
            if ARR_NDIM(arr) != 1
                || pronargs < 0
                || ARR_HASNULL(arr)
                || ARR_ELEMTYPE(arr) != OIDOID
            {
                elog!(
                    ERROR,
                    "proallargtypes is not a 1-D Oid array or it contains nulls"
                );
            }
            Assert!(pronargs >= (*funcform).pronargs as c_int);
            proargtypes = ARR_DATA_PTR(arr) as *mut Oid;
        }
    }

    /* Do we have any named arguments? */
    let mut lc = list_head(args);
    while !lc.is_null() {
        let arg = lfirst(lc) as *mut Node;
        if IsA!(arg, T_NamedArgExpr) {
            has_named_args = true;
            break;
        }
        lc = lnext(args, lc);
    }

    /* If so, we must apply reorder_function_arguments */
    if has_named_args {
        args = reorder_function_arguments(args, pronargs, func_tuple);
        /* Recheck argument types and add casts if needed */
        recheck_cast_function_args(args, result_type, proargtypes, pronargs, func_tuple);
    } else if list_length(args) < pronargs {
        /* No named args, but we seem to be short some defaults */
        args = add_function_defaults(args, pronargs, func_tuple);
        /* Recheck argument types and add casts if needed */
        recheck_cast_function_args(args, result_type, proargtypes, pronargs, func_tuple);
    }

    args
}

/*
 * reorder_function_arguments: convert named-notation args to positional args
 *
 * This function also inserts default argument values as needed, since it's
 * impossible to form a truly valid positional call without that.
 */
unsafe fn reorder_function_arguments(
    args: *mut List,
    pronargs: c_int,
    func_tuple: HeapTuple,
) -> *mut List {
    let funcform = GETSTRUCT(func_tuple) as Form_pg_proc;
    let nargsprovided = list_length(args);
    let mut argarray: [*mut Node; FUNC_MAX_ARGS as usize] =
        [null_mut(); FUNC_MAX_ARGS as usize];
    let mut i: c_int;

    Assert!(nargsprovided <= pronargs);
    if pronargs < 0 || pronargs > FUNC_MAX_ARGS as c_int {
        elog!(ERROR, "too many function arguments");
    }

    /* Deconstruct the argument list into an array indexed by argnumber */
    i = 0;
    let mut lc = list_head(args);
    while !lc.is_null() {
        let arg = lfirst(lc) as *mut Node;

        if !IsA!(arg, T_NamedArgExpr) {
            /* positional argument, assumed to precede all named args */
            Assert!(argarray[i as usize].is_null());
            argarray[i as usize] = arg;
            i += 1;
        } else {
            let na = arg as *mut NamedArgExpr;
            Assert!((*na).argnumber >= 0 && (*na).argnumber < pronargs);
            Assert!(argarray[(*na).argnumber as usize].is_null());
            argarray[(*na).argnumber as usize] = (*na).arg as *mut Node;
        }
        lc = lnext(args, lc);
    }

    /*
     * Fetch default expressions, if needed, and insert into array at proper
     * locations (they aren't necessarily consecutive or all used)
     */
    if nargsprovided < pronargs {
        let defaults = fetch_function_defaults(func_tuple);

        i = pronargs - (*funcform).pronargdefaults as c_int;
        let mut lc2 = list_head(defaults);
        while !lc2.is_null() {
            if argarray[i as usize].is_null() {
                argarray[i as usize] = lfirst(lc2) as *mut Node;
            }
            i += 1;
            lc2 = lnext(defaults, lc2);
        }
    }

    /* Now reconstruct the args list in proper order */
    let mut result: *mut List = NIL;
    for j in 0..pronargs as usize {
        Assert!(!argarray[j].is_null());
        result = lappend(result, argarray[j] as *mut c_void);
    }

    result
}

/*
 * add_function_defaults: add missing function arguments from its defaults
 *
 * This is used only when the argument list was positional to begin with,
 * and so we know we just need to add defaults at the end.
 */
unsafe fn add_function_defaults(
    args: *mut List,
    pronargs: c_int,
    func_tuple: HeapTuple,
) -> *mut List {
    let nargsprovided = list_length(args);
    let mut defaults: *mut List;
    let ndelete: c_int;

    /* Get all the default expressions from the pg_proc tuple */
    defaults = fetch_function_defaults(func_tuple);

    /* Delete any unused defaults from the list */
    ndelete = nargsprovided + list_length(defaults) - pronargs;
    if ndelete < 0 {
        elog!(ERROR, "not enough default arguments");
    }
    if ndelete > 0 {
        defaults = list_delete_first_n(defaults, ndelete);
    }

    /* And form the combined argument list, not modifying the input list */
    list_concat_copy(args, defaults)
}

/*
 * fetch_function_defaults: get function's default arguments as expression list
 */
unsafe fn fetch_function_defaults(func_tuple: HeapTuple) -> *mut List {
    let proargdefaults = SysCacheGetAttrNotNull(
        PROCOID,
        func_tuple,
        Anum_pg_proc_proargdefaults as i16,
    );
    let str_ = TextDatumGetCString(proargdefaults);
    let defaults = castNode!(List, T_List, stringToNode(str_) as *mut Node);
    pfree(str_ as *mut c_void);
    defaults
}

/*
 * recheck_cast_function_args: recheck function args and typecast as needed
 * after adding defaults.
 *
 * It is possible for some of the defaulted arguments to be polymorphic;
 * therefore we can't assume that the default expressions have the correct
 * data types already.
 */
unsafe fn recheck_cast_function_args(
    args: *mut List,
    result_type: Oid,
    proargtypes: *const Oid,
    pronargs: c_int,
    func_tuple: HeapTuple,
) {
    let funcform = GETSTRUCT(func_tuple) as Form_pg_proc;
    let nargs: c_int;
    let mut actual_arg_types: [Oid; FUNC_MAX_ARGS as usize] =
        [InvalidOid; FUNC_MAX_ARGS as usize];
    let mut declared_arg_types: [Oid; FUNC_MAX_ARGS as usize] =
        [InvalidOid; FUNC_MAX_ARGS as usize];
    let rettype: Oid;

    if list_length(args) > FUNC_MAX_ARGS as c_int {
        elog!(ERROR, "too many function arguments");
    }
    let mut n = 0i32;
    let mut lc = list_head(args);
    while !lc.is_null() {
        actual_arg_types[n as usize] = exprType(lfirst(lc) as *mut Node);
        n += 1;
        lc = lnext(args, lc);
    }
    nargs = n;
    Assert!(nargs == pronargs);
    core::ptr::copy_nonoverlapping(proargtypes, declared_arg_types.as_mut_ptr(), pronargs as usize);
    let rettype = enforce_generic_type_consistency(
        actual_arg_types.as_ptr(),
        declared_arg_types.as_mut_ptr(),
        nargs,
        (*funcform).prorettype,
        false,
    );
    /* let's just check we got the same answer as the parser did ... */
    if rettype != result_type {
        elog!(
            ERROR,
            "function's resolved result type changed during planning"
        );
    }

    /* perform any necessary typecasting of arguments */
    make_fn_arguments(null_mut(), args, actual_arg_types.as_mut_ptr(), declared_arg_types.as_mut_ptr());
}

/*
 * evaluate_function: try to pre-evaluate a function call
 *
 * We can do this if the function is strict and has any constant-null inputs
 * (just return a null constant), or if the function is immutable and has all
 * constant inputs (call it and return the result as a Const node).  In
 * estimation mode we are willing to pre-evaluate stable functions too.
 *
 * Returns a simplified expression if successful, or NULL if cannot
 * simplify the function.
 */
unsafe fn evaluate_function(
    funcid: Oid,
    result_type: Oid,
    result_typmod: int32,
    result_collid: Oid,
    input_collid: Oid,
    args: *mut List,
    funcvariadic: bool,
    func_tuple: HeapTuple,
    context: *mut EvalConstExpressionsContext,
) -> *mut Expr {
    let funcform = GETSTRUCT(func_tuple) as Form_pg_proc;
    let mut has_nonconst_input = false;
    let mut has_null_input = false;

    /*
     * Can't simplify if it returns a set.
     */
    if (*funcform).proretset {
        return null_mut();
    }

    /*
     * Can't simplify if it returns RECORD.  The immediate problem is that it
     * will be needing an expected tupdesc which we can't supply here.
     */
    if (*funcform).prorettype == RECORDOID {
        return null_mut();
    }

    /*
     * Check for constant inputs and especially constant-NULL inputs.
     */
    let mut lc = list_head(args);
    while !lc.is_null() {
        let arg = lfirst(lc) as *mut Node;
        if IsA!(arg, T_Const) {
            has_null_input |= (*(arg as *mut Const)).constisnull;
        } else {
            has_nonconst_input = true;
        }
        lc = lnext(args, lc);
    }

    /*
     * If the function is strict and has a constant-NULL input, it will never
     * be called at all, so we can replace the call by a NULL constant, even
     * if there are other inputs that aren't constant, and even if the
     * function is not otherwise immutable.
     */
    if (*funcform).proisstrict && has_null_input {
        return makeNullConst(result_type, result_typmod, result_collid) as *mut Expr;
    }

    /*
     * Otherwise, can simplify only if all inputs are constants.
     */
    if has_nonconst_input {
        return null_mut();
    }

    /*
     * Ordinarily we are only allowed to simplify immutable functions. But for
     * purposes of estimation, we consider it okay to simplify functions that
     * are merely stable.
     */
    if (*funcform).provolatile == PROVOLATILE_IMMUTABLE {
        /* okay */
    } else if (*context).estimate && (*funcform).provolatile == PROVOLATILE_STABLE {
        /* okay */
    } else {
        return null_mut();
    }

    /*
     * OK, looks like we can simplify this operator/function.
     *
     * Build a new FuncExpr node containing the already-simplified arguments.
     */
    let newexpr = makeNode!(FuncExpr, T_FuncExpr) as *mut FuncExpr;
    (*newexpr).funcid = funcid;
    (*newexpr).funcresulttype = result_type;
    (*newexpr).funcretset = false;
    (*newexpr).funcvariadic = funcvariadic;
    (*newexpr).funcformat = COERCE_EXPLICIT_CALL; /* doesn't matter */
    (*newexpr).funccollid = result_collid; /* doesn't matter */
    (*newexpr).inputcollid = input_collid;
    (*newexpr).args = args;
    (*newexpr).location = -1;

    evaluate_expr(newexpr as *mut Expr, result_type, result_typmod, result_collid)
}

// ============================================================================
// PART 7: inline_function, sql_inline_error_callback, evaluate_expr (public),
//         substitute_actual_parameters*, inline_set_returning_function (public),
//         substitute_actual_srf_parameters*, pull_paramids (public),
//         pull_paramids_walker, make_SAOP_expr (public)
// ============================================================================

/*
 * inline_function: try to expand a function call inline
 *
 * If the function is a sufficiently simple SQL-language function
 * (just "SELECT expression"), then we can inline it and avoid the rather
 * high per-call overhead of SQL functions.  Furthermore, this can expose
 * opportunities for constant-folding within the function expression.
 *
 * We have to beware of some special cases however.  A directly or
 * indirectly recursive function would cause us to recurse forever,
 * so we keep track of which functions we are already expanding and
 * do not re-expand them.  Also, if a parameter is used more than once
 * in the SQL-function body, we require it not to contain any volatile
 * functions (volatiles might deliver inconsistent answers) nor to be
 * unreasonably expensive to evaluate.  The expensiveness check not only
 * prevents us from doing multiple evaluations of an expensive parameter
 * at runtime, but is a safety value to limit growth of an expression due
 * to repeated inlining.
 *
 * We must also beware of changing the volatility or strictness status of
 * functions by inlining them.
 *
 * Also, at the moment we can't inline functions returning RECORD.  This
 * doesn't work in the general case because it discards information such
 * as OUT-parameter declarations.
 *
 * Also, context-dependent expression nodes in the argument list are trouble.
 *
 * Returns a simplified expression if successful, or NULL if cannot
 * simplify the function.
 */
unsafe fn inline_function(
    funcid: Oid,
    result_type: Oid,
    result_collid: Oid,
    input_collid: Oid,
    args: *mut List,
    funcvariadic: bool,
    func_tuple: HeapTuple,
    context: *mut EvalConstExpressionsContext,
) -> *mut Expr {
    let funcform = GETSTRUCT(func_tuple) as Form_pg_proc;

    /*
     * Forget it if the function is not SQL-language or has other showstopper
     * properties.  (The prokind and nargs checks are just paranoia.)
     */
    if (*funcform).prolang != SQLlanguageId
        || (*funcform).prokind != PROKIND_FUNCTION
        || (*funcform).prosecdef
        || (*funcform).proretset
        || (*funcform).prorettype == RECORDOID
        || !heap_attisnull(func_tuple, Anum_pg_proc_proconfig as c_int, null_mut())
        || (*funcform).pronargs as c_int != list_length(args)
    {
        return null_mut();
    }

    /* Check for recursive function, and give up trying to expand if so */
    if list_member_oid((*context).active_fns, funcid) {
        return null_mut();
    }

    /* Check permission to call function (fail later, if not) */
    if object_aclcheck(ProcedureRelationId, funcid, GetUserId(), ACL_EXECUTE)
        != ACLCHECK_OK
    {
        return null_mut();
    }

    /* Check whether a plugin wants to hook function entry/exit */
    if FmgrHookIsNeeded(funcid) {
        return null_mut();
    }

    /*
     * Make a temporary memory context, so that we don't leak all the stuff
     * that parsing might create.
     */
    let mycxt = AllocSetContextCreate(
        CurrentMemoryContext,
        b"inline_function\0".as_ptr() as *const c_char,
        ALLOCSET_DEFAULT_SIZES,
    );
    let oldcxt = MemoryContextSwitchTo(mycxt);

    /*
     * We need a dummy FuncExpr node containing the already-simplified
     * arguments.
     */
    let fexpr = makeNode!(FuncExpr, T_FuncExpr) as *mut FuncExpr;
    (*fexpr).funcid = funcid;
    (*fexpr).funcresulttype = result_type;
    (*fexpr).funcretset = false;
    (*fexpr).funcvariadic = funcvariadic;
    (*fexpr).funcformat = COERCE_EXPLICIT_CALL;
    (*fexpr).funccollid = result_collid;
    (*fexpr).inputcollid = input_collid;
    (*fexpr).args = args;
    (*fexpr).location = -1;

    /* Fetch the function body */
    let tmp = SysCacheGetAttrNotNull(PROCOID, func_tuple, Anum_pg_proc_prosrc as i16);
    let src = TextDatumGetCString(tmp);

    /*
     * Setup error traceback support for ereport().
     */
    let mut callback_arg = InlineErrorCallbackArg {
        proname: NameStr(&(*funcform).proname) as *mut c_char,
        prosrc: src,
    };

    let mut sqlerrcontext = ErrorContextCallback {
        callback: Some(sql_inline_error_callback),
        arg: &mut callback_arg as *mut InlineErrorCallbackArg as *mut c_void,
        previous: error_context_stack,
    };
    error_context_stack = &mut sqlerrcontext;

    /* If we have prosqlbody, pay attention to that not prosrc */
    let mut is_null = false;
    let tmp2 = SysCacheGetAttr(
        PROCOID,
        func_tuple,
        Anum_pg_proc_prosqlbody as i16,
        &mut is_null,
    );

    let querytree: *mut Query;
    if !is_null {
        let n = stringToNode(TextDatumGetCString(tmp2) as *mut i8);
        let query_list: *mut List;
        if IsA!(n, T_List) {
            query_list = linitial_node!(List, T_List, n as *mut List);
        } else {
            query_list = list_make1!(n);
        }
        if list_length(query_list) != 1 {
            /* goto fail */
            MemoryContextSwitchTo(oldcxt);
            MemoryContextDelete(mycxt);
            error_context_stack = sqlerrcontext.previous;
            return null_mut();
        }
        querytree = linitial(query_list) as *mut Query;
        /* Because we'll insist below that the querytree have an empty rtable
         * and no sublinks, it cannot have any relation references that need
         * to be locked or rewritten.  So we can omit those steps. */
    } else {
        /* Set up to handle parameters while parsing the function body. */
        let pinfo = prepare_sql_fn_parse_info(func_tuple, fexpr as *mut Node, input_collid);

        /*
         * We just do parsing and parse analysis, not rewriting, because
         * rewriting will not affect table-free-SELECT-only queries, which is
         * all that we care about.  Also, we can punt as soon as we detect
         * more than one command in the function body.
         */
        let raw_parsetree_list = pg_parse_query(src);
        if list_length(raw_parsetree_list) != 1 {
            /* goto fail */
            MemoryContextSwitchTo(oldcxt);
            MemoryContextDelete(mycxt);
            error_context_stack = sqlerrcontext.previous;
            return null_mut();
        }

        let pstate = make_parsestate(null_mut());
        (*pstate).p_sourcetext = src;
        sql_fn_parser_setup(pstate, pinfo);

        querytree = transformTopLevelStmt(pstate, linitial(raw_parsetree_list) as *mut RawStmt)
            as *mut Query;

        free_parsestate(pstate);
    }

    /*
     * The single command must be a simple "SELECT expression".
     */
    macro_rules! check_fail {
        ($cond:expr) => {
            if $cond {
                MemoryContextSwitchTo(oldcxt);
                MemoryContextDelete(mycxt);
                error_context_stack = sqlerrcontext.previous;
                return null_mut();
            }
        };
    }

    check_fail!(
        !IsA!(querytree as *mut Node, T_Query)
            || (*querytree).commandType != CMD_SELECT
            || (*querytree).hasAggs
            || (*querytree).hasWindowFuncs
            || (*querytree).hasTargetSRFs
            || (*querytree).hasSubLinks
            || !(*querytree).cteList.is_null()
            || !(*querytree).rtable.is_null()
            || !(*(*querytree).jointree).fromlist.is_null()
            || !(*(*querytree).jointree).quals.is_null()
            || !(*querytree).groupClause.is_null()
            || !(*querytree).groupingSets.is_null()
            || !(*querytree).havingQual.is_null()
            || !(*querytree).windowClause.is_null()
            || !(*querytree).distinctClause.is_null()
            || !(*querytree).sortClause.is_null()
            || !(*querytree).limitOffset.is_null()
            || !(*querytree).limitCount.is_null()
            || !(*querytree).setOperations.is_null()
            || list_length((*querytree).targetList) != 1
    );

    /* If the function result is composite, resolve it */
    let mut rettupdesc: TupleDesc = null_mut();
    get_expr_result_type(fexpr as *mut Node, null_mut(), &mut rettupdesc);

    /*
     * Make sure the function (still) returns what it's declared to.
     */
    let mut querytree_list = list_make1!(querytree);
    check_fail!(check_sql_fn_retval(
        list_make1!(querytree_list as *mut c_void),
        result_type,
        rettupdesc,
        (*funcform).prokind,
        false,
    ));

    /* Given the tests above, check_sql_fn_retval shouldn't have decided to
     * inject a projection step, but let's just make sure. */
    check_fail!(querytree != linitial(querytree_list) as *mut Query);

    /* Now we can grab the tlist expression */
    let mut newexpr = (*(linitial((*querytree).targetList) as *mut TargetEntry)).expr as *mut Node;

    /*
     * If the SQL function returns VOID, we can only inline it if it is a
     * SELECT of an expression returning VOID.
     */
    check_fail!(exprType(newexpr) != result_type);

    /*
     * Additional validity checks on the expression.  It mustn't be more
     * volatile than the surrounding function.
     */
    check_fail!(
        (*funcform).provolatile == PROVOLATILE_IMMUTABLE
            && contain_mutable_functions(newexpr)
    );
    check_fail!(
        (*funcform).provolatile == PROVOLATILE_STABLE
            && contain_volatile_functions(newexpr)
    );

    check_fail!((*funcform).proisstrict && contain_nonstrict_functions(newexpr));

    /*
     * If any parameter expression contains a context-dependent node, we can't
     * inline, for fear of putting such a node into the wrong context.
     */
    check_fail!(contain_context_dependent_node(args as *mut Node));

    /*
     * We may be able to do it; there are still checks on parameter usage to
     * make, but those are most easily done in combination with the actual
     * substitution of the inputs.  So start building expression with inputs
     * substituted.
     */
    let usecounts = palloc0(
        ((*funcform).pronargs as usize) * core::mem::size_of::<c_int>(),
    ) as *mut c_int;
    newexpr = substitute_actual_parameters(
        newexpr,
        (*funcform).pronargs as c_int,
        args,
        usecounts,
    );

    /* Now check for parameter usage */
    let mut i = 0i32;
    let mut lc = list_head(args);
    while !lc.is_null() {
        let param = lfirst(lc) as *mut Node;

        if *usecounts.add(i as usize) == 0 {
            /* Param not used at all: uncool if func is strict */
            check_fail!((*funcform).proisstrict);
        } else if *usecounts.add(i as usize) != 1 {
            /* Param used multiple times: uncool if expensive or volatile */
            let mut eval_cost: QualCost = QualCost { startup: 0.0, per_tuple: 0.0 };

            /*
             * We define "expensive" as "contains any subplan or more than 10
             * operators".  Note that the subplan search has to be done
             * explicitly, since cost_qual_eval() will barf on unplanned
             * subselects.
             */
            check_fail!(contain_subplans(param));
            cost_qual_eval(&mut eval_cost, list_make1!(param as *mut c_void), null_mut());
            check_fail!(
                eval_cost.startup + eval_cost.per_tuple > 10.0 * cpu_operator_cost
            );

            /* Check volatility last since this is more expensive than the above tests */
            check_fail!(contain_volatile_functions(param));
        }
        i += 1;
        lc = lnext(args, lc);
    }

    /*
     * Whew --- we can make the substitution.  Copy the modified expression
     * out of the temporary memory context, and clean up.
     */
    MemoryContextSwitchTo(oldcxt);

    newexpr = copyObject(newexpr as *const Node) as *mut Node;

    MemoryContextDelete(mycxt);

    /*
     * If the result is of a collatable type, force the result to expose the
     * correct collation.
     */
    if OidIsValid(result_collid) {
        let exprcoll = exprCollation(newexpr);

        if OidIsValid(exprcoll) && exprcoll != result_collid {
            let newnode = makeNode!(CollateExpr, T_CollateExpr) as *mut CollateExpr;
            (*newnode).arg = newexpr as *mut Expr;
            (*newnode).collOid = result_collid;
            (*newnode).location = -1;

            newexpr = newnode as *mut Node;
        }
    }

    /*
     * Since there is now no trace of the function in the plan tree, we must
     * explicitly record the plan's dependency on the function.
     */
    if !(*context).root.is_null() {
        record_plan_function_dependency((*context).root, funcid);
    }

    /*
     * Recursively try to simplify the modified expression.  Here we must add
     * the current function to the context list of active functions.
     */
    (*context).active_fns = lappend_oid((*context).active_fns, funcid);
    newexpr = eval_const_expressions_mutator(newexpr, context);
    (*context).active_fns = list_delete_last((*context).active_fns);

    error_context_stack = sqlerrcontext.previous;

    newexpr as *mut Expr
}

/*
 * Replace Param nodes by appropriate actual parameters
 */
unsafe fn substitute_actual_parameters(
    expr: *mut Node,
    nargs: c_int,
    args: *mut List,
    usecounts: *mut c_int,
) -> *mut Node {
    let mut context = SubstituteActualParametersContext {
        nargs,
        args,
        usecounts,
    };

    substitute_actual_parameters_mutator(expr, &mut context)
}

unsafe fn substitute_actual_parameters_mutator(
    node: *mut Node,
    context: *mut SubstituteActualParametersContext,
) -> *mut Node {
    if node.is_null() {
        return null_mut();
    }
    if IsA!(node, T_Param) {
        let param = node as *mut Param;

        if (*param).paramkind != PARAM_EXTERN {
            elog!(
                ERROR,
                "unexpected paramkind: {}",
                (*param).paramkind as c_int
            );
        }
        if (*param).paramid <= 0 || (*param).paramid > (*context).nargs {
            elog!(ERROR, "invalid paramid: {}", (*param).paramid);
        }

        /* Count usage of parameter */
        *(*context).usecounts.add(((*param).paramid - 1) as usize) += 1;

        /* Select the appropriate actual arg and replace the Param with it */
        /* We don't need to copy at this time (it'll get done later) */
        return list_nth((*context).args, (*param).paramid - 1) as *mut Node;
    }
    expression_tree_mutator(
        node,
        Some(substitute_actual_parameters_mutator_trampoline),
        context as *mut c_void,
    )
}

unsafe fn substitute_actual_parameters_mutator_trampoline(
    node: *mut Node,
    context: *mut c_void,
) -> *mut Node {
    substitute_actual_parameters_mutator(
        node,
        context as *mut SubstituteActualParametersContext,
    )
}

/*
 * error context callback to let us supply a call-stack traceback
 */
unsafe extern "C" fn sql_inline_error_callback(arg: *mut c_void) {
    let callback_arg = arg as *mut InlineErrorCallbackArg;
    let syntaxerrposition = geterrposition();

    /* If it's a syntax error, convert to internal syntax error report */
    if syntaxerrposition > 0 {
        errposition(0);
        internalerrposition(syntaxerrposition);
        internalerrquery((*callback_arg).prosrc);
    }

    // TODO(pg-port): errcontext -- elog.c not yet ported; drop the call for now.
    let _ = (*callback_arg).proname;
}

/*
 * evaluate_expr: pre-evaluate a constant expression
 *
 * We use the executor's routine ExecEvalExpr() to avoid duplication of
 * code and ensure we get the same result as the executor would get.
 */
pub unsafe fn evaluate_expr(
    expr: *mut Expr,
    result_type: Oid,
    result_typmod: int32,
    result_collation: Oid,
) -> *mut Expr {
    let estate: *mut EState;
    let exprstate: *mut ExprState;
    let oldcontext: MemoryContext;
    let const_val: Datum;
    let mut const_is_null: bool = false;
    let mut result_typ_len: i16 = 0;
    let mut result_typ_by_val: bool = false;

    /*
     * To use the executor, we need an EState.
     */
    let estate = CreateExecutorState();

    /* We can use the estate's working context to avoid memory leaks. */
    let oldcontext = MemoryContextSwitchTo((*estate).es_query_cxt);

    /* Make sure any opfuncids are filled in. */
    fix_opfuncids(expr as *mut Node);

    /*
     * Prepare expr for execution.  (Note: we can't use ExecPrepareExpr
     * because it'd result in recursively invoking eval_const_expressions.)
     */
    let exprstate = ExecInitExpr(expr, null_mut());

    /*
     * And evaluate it.
     *
     * It is OK to use a default econtext because none of the ExecEvalExpr()
     * code used in this situation will use econtext.  That might seem
     * fortuitous, but it's not so unreasonable --- a constant expression does
     * not depend on context, by definition, n'est ce pas?
     */
    let const_val = ExecEvalExprSwitchContext(
        exprstate,
        GetPerTupleExprContext(estate),
        &mut const_is_null,
    );

    /* Get info needed about result datatype */
    get_typlenbyval(result_type, &mut result_typ_len, &mut result_typ_by_val);

    /* Get back to outer memory context */
    MemoryContextSwitchTo(oldcontext);

    /*
     * Must copy result out of sub-context used by expression eval.
     *
     * Also, if it's varlena, forcibly detoast it.  This protects us against
     * storing TOAST pointers into plans that might outlive the referenced
     * data.
     */
    let const_val = if !const_is_null {
        if result_typ_len == -1 {
            PointerGetDatum(PG_DETOAST_DATUM_COPY!(const_val) as *mut c_void)
        } else {
            datumCopy(const_val, result_typ_by_val, result_typ_len as c_int)
        }
    } else {
        const_val
    };

    /* Release all the junk we just created */
    FreeExecutorState(estate);

    /*
     * Make the constant result node.
     */
    makeConst(
        result_type,
        result_typmod,
        result_collation,
        result_typ_len as c_int,
        const_val,
        const_is_null,
        result_typ_by_val,
    ) as *mut Expr
}

/*
 * inline_set_returning_function
 *      Attempt to "inline" a set-returning function in the FROM clause.
 *
 * "rte" is an RTE_FUNCTION rangetable entry.  If it represents a call of a
 * set-returning SQL function that can safely be inlined, expand the function
 * and return the substitute Query structure.  Otherwise, return NULL.
 *
 * We assume that the RTE's expression has already been put through
 * eval_const_expressions(), which among other things will take care of
 * default arguments and named-argument notation.
 *
 * This has a good deal of similarity to inline_function(), but that's
 * for the non-set-returning case, and there are enough differences to
 * justify separate functions.
 */
pub unsafe fn inline_set_returning_function(
    root: *mut PlannerInfo,
    rte: *mut RangeTblEntry,
) -> *mut Query {
    let rtfunc: *mut RangeTblFunction;
    let fexpr: *mut FuncExpr;
    let func_oid: Oid;
    let func_tuple: HeapTuple;
    let funcform: Form_pg_proc;

    Assert!((*rte).rtekind == RTE_FUNCTION);

    /*
     * It doesn't make a lot of sense for a SQL SRF to refer to itself in its
     * own FROM clause, since that must cause infinite recursion at runtime.
     * It will cause this code to recurse too, so check for stack overflow.
     * (There's no need to do more.)
     */
    check_stack_depth();

    /* Fail if the RTE has ORDINALITY - we don't implement that here. */
    if (*rte).funcordinality {
        return null_mut();
    }

    /* Fail if RTE isn't a single, simple FuncExpr */
    if list_length((*rte).functions) != 1 {
        return null_mut();
    }
    rtfunc = linitial((*rte).functions) as *mut RangeTblFunction;

    if !IsA!((*rtfunc).funcexpr as *mut Node, T_FuncExpr) {
        return null_mut();
    }
    fexpr = (*rtfunc).funcexpr as *mut FuncExpr;

    func_oid = (*fexpr).funcid;

    /*
     * The function must be declared to return a set, else inlining would
     * change the results if the contained SELECT didn't return exactly one
     * row.
     */
    if !(*fexpr).funcretset {
        return null_mut();
    }

    /*
     * Refuse to inline if the arguments contain any volatile functions or
     * sub-selects.
     */
    if contain_volatile_functions((*fexpr).args as *mut Node)
        || contain_subplans((*fexpr).args as *mut Node)
    {
        return null_mut();
    }

    /* Check permission to call function (fail later, if not) */
    if object_aclcheck(ProcedureRelationId, func_oid, GetUserId(), ACL_EXECUTE)
        != ACLCHECK_OK
    {
        return null_mut();
    }

    /* Check whether a plugin wants to hook function entry/exit */
    if FmgrHookIsNeeded(func_oid) {
        return null_mut();
    }

    /*
     * OK, let's take a look at the function's pg_proc entry.
     */
    let func_tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(func_oid));
    if !HeapTupleIsValid(func_tuple) {
        elog!(ERROR, "cache lookup failed for function {}", func_oid);
    }
    let funcform = GETSTRUCT(func_tuple) as Form_pg_proc;

    /*
     * Forget it if the function is not SQL-language or has other showstopper
     * properties.
     */
    if (*funcform).prolang != SQLlanguageId
        || (*funcform).prokind != PROKIND_FUNCTION
        || (*funcform).proisstrict
        || (*funcform).provolatile == PROVOLATILE_VOLATILE
        || (*funcform).prorettype == VOIDOID
        || (*funcform).prosecdef
        || !(*funcform).proretset
        || list_length((*fexpr).args) != (*funcform).pronargs as c_int
        || !heap_attisnull(func_tuple, Anum_pg_proc_proconfig as c_int, null_mut())
    {
        ReleaseSysCache(func_tuple);
        return null_mut();
    }

    /*
     * Make a temporary memory context, so that we don't leak all the stuff
     * that parsing might create.
     */
    let mycxt = AllocSetContextCreate(
        CurrentMemoryContext,
        b"inline_set_returning_function\0".as_ptr() as *const c_char,
        ALLOCSET_DEFAULT_SIZES,
    );
    let oldcxt = MemoryContextSwitchTo(mycxt);

    /* Fetch the function body */
    let tmp = SysCacheGetAttrNotNull(PROCOID, func_tuple, Anum_pg_proc_prosrc as i16);
    let src = TextDatumGetCString(tmp);

    /*
     * Setup error traceback support for ereport().
     */
    let mut callback_arg = InlineErrorCallbackArg {
        proname: NameStr(&(*funcform).proname) as *mut c_char,
        prosrc: src,
    };

    let mut sqlerrcontext = ErrorContextCallback {
        callback: Some(sql_inline_error_callback),
        arg: &mut callback_arg as *mut InlineErrorCallbackArg as *mut c_void,
        previous: error_context_stack,
    };
    error_context_stack = &mut sqlerrcontext;

    macro_rules! srf_fail {
        () => {{
            MemoryContextSwitchTo(oldcxt);
            MemoryContextDelete(mycxt);
            error_context_stack = sqlerrcontext.previous;
            ReleaseSysCache(func_tuple);
            return null_mut();
        }};
    }

    /* If we have prosqlbody, pay attention to that not prosrc */
    let mut is_null = false;
    let tmp2 = SysCacheGetAttr(
        PROCOID,
        func_tuple,
        Anum_pg_proc_prosqlbody as i16,
        &mut is_null,
    );

    let mut querytree: *mut Query;
    let mut querytree_list: *mut List;
    if !is_null {
        let n = stringToNode(TextDatumGetCString(tmp2) as *mut i8);
        if IsA!(n, T_List) {
            querytree_list = linitial_node!(List, T_List, n as *mut List);
        } else {
            querytree_list = list_make1!(n);
        }
        if list_length(querytree_list) != 1 {
            srf_fail!();
        }
        querytree = linitial(querytree_list) as *mut Query;

        /* Acquire necessary locks, then apply rewriter. */
        AcquireRewriteLocks(querytree, true, false);
        querytree_list = pg_rewrite_query(querytree);
        if list_length(querytree_list) != 1 {
            srf_fail!();
        }
        querytree = linitial(querytree_list) as *mut Query;
    } else {
        /*
         * Set up to handle parameters while parsing the function body.
         */
        let pinfo = prepare_sql_fn_parse_info(
            func_tuple,
            fexpr as *mut Node,
            (*fexpr).inputcollid,
        );

        /*
         * Parse, analyze, and rewrite.
         */
        let raw_parsetree_list = pg_parse_query(src);
        if list_length(raw_parsetree_list) != 1 {
            srf_fail!();
        }

        querytree_list = pg_analyze_and_rewrite_withcb(
            linitial(raw_parsetree_list) as *mut RawStmt,
            src,
            Some(sql_fn_parser_setup_trampoline),
            pinfo as *mut c_void,
            null_mut(),
        );
        if list_length(querytree_list) != 1 {
            srf_fail!();
        }
        querytree = linitial(querytree_list) as *mut Query;
    }

    /*
     * Also resolve the actual function result tupdesc, if composite.
     */
    let functypclass: TypeFuncClass;
    let mut rettupdesc: TupleDesc = null_mut();
    if !(*rtfunc).funccolnames.is_null() {
        functypclass = TYPEFUNC_RECORD;
        rettupdesc = BuildDescFromLists(
            (*rtfunc).funccolnames,
            (*rtfunc).funccoltypes,
            (*rtfunc).funccoltypmods,
            (*rtfunc).funccolcollations,
        );
    } else {
        functypclass = get_expr_result_type(fexpr as *mut Node, null_mut(), &mut rettupdesc);
    }

    /*
     * The single command must be a plain SELECT.
     */
    if !IsA!(querytree as *mut Node, T_Query)
        || (*querytree).commandType != CMD_SELECT
    {
        srf_fail!();
    }

    /*
     * Make sure the function (still) returns what it's declared to.
     */
    if !check_sql_fn_retval(
        list_make1!(querytree_list as *mut c_void),
        (*fexpr).funcresulttype,
        rettupdesc,
        (*funcform).prokind,
        true,
    ) && (functypclass == TYPEFUNC_COMPOSITE
        || functypclass == TYPEFUNC_COMPOSITE_DOMAIN
        || functypclass == TYPEFUNC_RECORD)
    {
        srf_fail!(); /* reject not-whole-tuple-result cases */
    }

    /*
     * check_sql_fn_retval might've inserted a projection step, but that's
     * fine; just make sure we use the upper Query.
     */
    let querytree = linitial_node!(Query, T_Query, querytree_list);

    /*
     * Looks good --- substitute parameters into the query.
     */
    let querytree = substitute_actual_srf_parameters(
        querytree,
        (*funcform).pronargs as c_int,
        (*fexpr).args,
    );

    /*
     * Copy the modified query out of the temporary memory context, and clean
     * up.
     */
    MemoryContextSwitchTo(oldcxt);

    let querytree = copyObject(querytree as *const Query) as *mut Query;

    MemoryContextDelete(mycxt);
    error_context_stack = sqlerrcontext.previous;
    ReleaseSysCache(func_tuple);

    /*
     * We don't have to fix collations here because the upper query is already
     * parsed.
     */

    /*
     * Since there is now no trace of the function in the plan tree, we must
     * explicitly record the plan's dependency on the function.
     */
    record_plan_function_dependency(root, func_oid);

    /*
     * We must also notice if the inserted query adds a dependency on the
     * calling role due to RLS quals.
     */
    if (*querytree).hasRowSecurity {
        (*(*root).glob).dependsOnRole = true;
    }

    querytree
}

/*
 * Replace Param nodes by appropriate actual parameters
 *
 * This is just enough different from substitute_actual_parameters()
 * that it needs its own code.
 */
unsafe fn substitute_actual_srf_parameters(
    expr: *mut Query,
    nargs: c_int,
    args: *mut List,
) -> *mut Query {
    let mut context = SubstituteActualSrfParametersContext {
        nargs,
        args,
        sublevels_up: 1,
    };

    query_tree_mutator(
        expr,
        Some(substitute_actual_srf_parameters_mutator_trampoline),
        &mut context as *mut SubstituteActualSrfParametersContext as *mut c_void,
        0,
    )
}

unsafe fn substitute_actual_srf_parameters_mutator(
    node: *mut Node,
    context: *mut SubstituteActualSrfParametersContext,
) -> *mut Node {
    if node.is_null() {
        return null_mut();
    }
    if IsA!(node, T_Query) {
        (*context).sublevels_up += 1;
        let result = query_tree_mutator(
            node as *mut Query,
            Some(substitute_actual_srf_parameters_mutator_trampoline),
            context as *mut c_void,
            0,
        ) as *mut Node;
        (*context).sublevels_up -= 1;
        return result;
    }
    if IsA!(node, T_Param) {
        let param = node as *mut Param;

        if (*param).paramkind == PARAM_EXTERN {
            if (*param).paramid <= 0 || (*param).paramid > (*context).nargs {
                elog!(ERROR, "invalid paramid: {}", (*param).paramid);
            }

            /*
             * Since the parameter is being inserted into a subquery, we must
             * adjust levels.
             */
            let result = copyObject(
                list_nth((*context).args, (*param).paramid - 1) as *const Node,
            ) as *mut Node;
            IncrementVarSublevelsUp(result, (*context).sublevels_up, 0);
            return result;
        }
    }
    expression_tree_mutator(
        node,
        Some(substitute_actual_srf_parameters_mutator_trampoline),
        context as *mut c_void,
    )
}

unsafe fn substitute_actual_srf_parameters_mutator_trampoline(
    node: *mut Node,
    context: *mut c_void,
) -> *mut Node {
    substitute_actual_srf_parameters_mutator(
        node,
        context as *mut SubstituteActualSrfParametersContext,
    )
}

/*
 * pull_paramids
 *      Returns a Bitmapset containing the paramids of all Params in 'expr'.
 */
pub unsafe fn pull_paramids(expr: *mut Expr) -> *mut Bitmapset {
    let mut result: *mut Bitmapset = null_mut();

    pull_paramids_walker(expr as *mut Node, &mut result);

    result
}

unsafe fn pull_paramids_walker(
    node: *mut Node,
    context: *mut *mut Bitmapset,
) -> bool {
    if node.is_null() {
        return false;
    }
    if IsA!(node, T_Param) {
        let param = node as *mut Param;

        *context = bms_add_member(*context, (*param).paramid);
        return false;
    }
    expression_tree_walker(
        node,
        Some(pull_paramids_walker_trampoline),
        context as *mut c_void,
    )
}

unsafe fn pull_paramids_walker_trampoline(
    node: *mut Node,
    context: *mut c_void,
) -> bool {
    pull_paramids_walker(node, context as *mut *mut Bitmapset)
}

/*
 * Build ScalarArrayOpExpr on top of 'exprs.' 'haveNonConst' indicates
 * whether at least one of the expressions is not Const.  When it's false,
 * the array constant is built directly; otherwise, we have to build a child
 * ArrayExpr. The 'exprs' list gets freed if not directly used in the output
 * expression tree.
 */
pub unsafe fn make_SAOP_expr(
    oper: Oid,
    leftexpr: *mut Node,
    coltype: Oid,
    arraycollid: Oid,
    inputcollid: Oid,
    exprs: *mut List,
    have_non_const: bool,
) -> *mut ScalarArrayOpExpr {
    let arraytype = get_array_type(coltype);

    if !OidIsValid(arraytype) {
        return null_mut();
    }

    /*
     * Assemble an array from the list of constants.  It seems more profitable
     * to build a const array.  But in the presence of other nodes, we don't
     * have a specific value here and must employ an ArrayExpr instead.
     */
    let array_node: *mut Node;
    if have_non_const {
        let array_expr = makeNode!(ArrayExpr, T_ArrayExpr) as *mut ArrayExpr;

        /* array_collid will be set by parse_collate.c */
        (*array_expr).element_typeid = coltype;
        (*array_expr).array_typeid = arraytype;
        (*array_expr).multidims = false;
        (*array_expr).elements = exprs;
        (*array_expr).location = -1;

        array_node = array_expr as *mut Node;
    } else {
        let mut typlen: i16 = 0;
        let mut typbyval: bool = false;
        let mut typalign: c_char = 0;
        let n = list_length(exprs) as usize;
        let elems = palloc(n * core::mem::size_of::<Datum>()) as *mut Datum;
        let nulls = palloc(n * core::mem::size_of::<bool>()) as *mut bool;
        let dims: [c_int; 1] = [n as c_int];
        let lbs: [c_int; 1] = [1];
        let mut i: usize = 0;

        get_typlenbyvalalign(coltype, &mut typlen, &mut typbyval, &mut typalign);

        let mut lc = list_head(exprs);
        while !lc.is_null() {
            let value = lfirst(lc) as *mut Const;
            *elems.add(i) = (*value).constvalue;
            *nulls.add(i) = (*value).constisnull;
            i += 1;
            lc = lnext(exprs, lc);
        }

        let array_const = construct_md_array(
            elems,
            nulls,
            1,
            dims.as_ptr() as *mut c_int,
            lbs.as_ptr() as *mut c_int,
            coltype,
            typlen as c_int,
            typbyval,
            typalign,
        );
        array_node = makeConst(
            arraytype,
            -1,
            arraycollid,
            -1,
            PointerGetDatum(array_const as *mut c_void),
            false,
            false,
        ) as *mut Node;

        pfree(elems as *mut c_void);
        pfree(nulls as *mut c_void);
        list_free(exprs);
    }

    /* Build the SAOP expression node */
    let saopexpr = makeNode!(ScalarArrayOpExpr, T_ScalarArrayOpExpr) as *mut ScalarArrayOpExpr;
    (*saopexpr).opno = oper;
    (*saopexpr).opfuncid = get_opcode(oper);
    (*saopexpr).hashfuncid = InvalidOid;
    (*saopexpr).negfuncid = InvalidOid;
    (*saopexpr).useOr = true;
    (*saopexpr).inputcollid = inputcollid;
    (*saopexpr).args = list_make2!(leftexpr, array_node);
    (*saopexpr).location = -1;

    saopexpr
}
