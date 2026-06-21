//! optimizer/plan/subselect.rs
//!   Planning routines for subselects.
//!
//! This module deals with SubLinks and CTEs, but not subquery RTEs (i.e.,
//! not sub-SELECT-in-FROM cases).
//!
//! Translated 1:1 from postgres/src/backend/optimizer/plan/subselect.c
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!   src/backend/optimizer/plan/subselect.c

#![allow(unused_variables)]
#![allow(unreachable_code)]
#![allow(unreachable_patterns)]
#![allow(unused_mut)]
#![allow(dead_code)]
#![allow(unused_assignments)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]

use crate::prelude::*;
use crate::{
    foreach, forboth, forfour, current_cell, makeNode, IsA, castNode, lfirst_node,
    Assert, elog, list_make1, list_make1_int,
};
use std::ptr;
use std::ffi::{c_int, c_char, c_void};

use crate::postgres_ext::Oid;
use crate::nodes::nodes::{
    Node, NodeTag, nodeTag,
    CMD_SELECT, CMD_UPDATE, CMD_DELETE, CMD_MERGE,
    Cost,
    JoinType::*,
};
use crate::nodes::pg_list::{
    List, ListCell, NIL,
    lfirst, lfirst_int, lfirst_oid,
    list_length, list_head, list_nth, list_nth_cell, lnext, lcons,
    lappend, lappend_int, lappend_oid, list_concat, list_copy, list_copy_head,
    list_make1_impl, list_free, list_member_int,
    linitial, linitial_int, linitial_oid, lsecond,
};
use crate::nodes::parsenodes::{
    Query, RangeTblEntry, CommonTableExpr, RangeTblFunction,
    RTE_CTE, RTE_VALUES, RTE_SUBQUERY, RTE_RESULT, RTE_GROUP,
    CTEMaterializeNever, CTEMaterializeDefault,
};
use crate::nodes::primnodes::{
    Var, Const, Aggref, GroupingFunc, TargetEntry, Expr,
    Param, PARAM_SUBLINK, PARAM_EXEC,
    SubLink, SubPlan, AlternativeSubPlan,
    SubLinkType,
    EXISTS_SUBLINK, EXPR_SUBLINK, ARRAY_SUBLINK, ROWCOMPARE_SUBLINK,
    MULTIEXPR_SUBLINK, ANY_SUBLINK, ALL_SUBLINK, CTE_SUBLINK,
    ScalarArrayOpExpr, OpExpr, BoolExpr,
    ReturningExpr, MergeSupportFunc,
};
use crate::nodes::pathnodes::{
    PlannerInfo, PlannerGlobal, RelOptInfo, Path, PathTarget,
    PlannerParamItem, PlaceHolderVar,
    UpperRelationKind::UPPERREL_FINAL,
};
use crate::nodes::plannodes::{
    Plan, SubqueryScan, FunctionScan, TableFuncScan, ValuesScan, CteScan,
    WorkTableScan, ForeignScan, CustomScan, ModifyTable, Append, MergeAppend,
    BitmapAnd, BitmapOr, NestLoop, NestLoopParam, MergeJoin, HashJoin, Hash,
    Limit, RecursiveUnion, LockRows, Agg, WindowAgg, Gather, GatherMerge, Memoize,
    IndexScan, IndexOnlyScan, BitmapIndexScan, BitmapHeapScan,
    TidScan, TidRangeScan, NamedTuplestoreScan, SampleScan, Result as PlanResult,
    Join,
};
use crate::nodes::nodes::{
    T_Result, T_SeqScan, T_SampleScan, T_IndexScan, T_IndexOnlyScan,
    T_BitmapIndexScan, T_BitmapHeapScan, T_TidScan, T_TidRangeScan,
    T_SubqueryScan, T_FunctionScan, T_TableFuncScan, T_ValuesScan, T_CteScan,
    T_WorkTableScan, T_NamedTuplestoreScan, T_ForeignScan, T_CustomScan,
    T_ModifyTable, T_Append, T_MergeAppend, T_BitmapAnd, T_BitmapOr,
    T_NestLoop, T_MergeJoin, T_HashJoin, T_Hash, T_Limit, T_RecursiveUnion,
    T_LockRows, T_Agg, T_WindowAgg, T_Gather, T_GatherMerge, T_Memoize,
    T_ProjectSet, T_Material, T_Sort, T_IncrementalSort, T_Unique, T_SetOp, T_Group,
    T_SubPlan,
    AGG_HASHED,
};
use crate::nodes::bitmapset::{
    Bitmapset,
    bms_add_member, bms_del_member, bms_del_members, bms_is_member, bms_is_subset,
    bms_is_empty, bms_equal, bms_make_singleton, bms_next_member, bms_num_members,
    bms_free, bms_difference, bms_union, bms_add_members, bms_join, bms_copy,
};
use crate::c::Index;

// ---------------------------------------------------------------------------
// Context structs (local to this file)
// ---------------------------------------------------------------------------

struct convert_testexpr_context {
    root: *mut PlannerInfo,
    subst_nodes: *mut List, /* Nodes to substitute for Params */
}

struct process_sublinks_context {
    root: *mut PlannerInfo,
    isTopQual: bool,
}

struct finalize_primnode_context {
    root: *mut PlannerInfo,
    paramids: *mut Bitmapset, /* Non-local PARAM_EXEC paramids found */
}

struct inline_cte_walker_context {
    ctename: *const c_char, /* name and relative level of target CTE */
    levelsup: c_int,
    ctequery: *mut Query, /* query to substitute */
}

// ---------------------------------------------------------------------------
// Stubs for dependencies not yet ported.  TODO(pg-port)
// ---------------------------------------------------------------------------

/// TODO(pg-port): optimizer/cost.h
unsafe fn cost_subplan(root: *mut PlannerInfo, splan: *mut SubPlan, plan: *mut Plan) {
    crate::optimizer::path::costsize::cost_subplan(root as _, splan as _, plan as _)
}

/// TODO(pg-port): optimizer/planner.h -- subquery_planner
unsafe fn subquery_planner(
    glob: *mut PlannerGlobal,
    subquery: *mut Query,
    parent_root: *mut PlannerInfo,
    hasRecursion: bool,
    tuple_fraction: f64,
    result_hook: *mut c_void,
) -> *mut PlannerInfo {
    crate::optimizer::plan::planner::subquery_planner(
        glob as _,
        subquery as _,
        parent_root as _,
        hasRecursion as _,
        tuple_fraction as _,
        result_hook as _,
    ) as _
}

/// TODO(pg-port): optimizer/planner.h -- fetch_upper_rel
unsafe fn fetch_upper_rel(
    root: *mut PlannerInfo,
    kind: crate::nodes::pathnodes::UpperRelationKind,
    relids: *mut Bitmapset,
) -> *mut RelOptInfo {
    crate::optimizer::util::relnode::fetch_upper_rel(root as _, kind as _, relids as _) as _
}

/// TODO(pg-port): optimizer/planner.h -- get_cheapest_fractional_path
unsafe fn get_cheapest_fractional_path(
    rel: *mut RelOptInfo,
    tuple_fraction: f64,
) -> *mut Path {
    crate::optimizer::plan::planner::get_cheapest_fractional_path(rel as _, tuple_fraction as _) as _
}

/// TODO(pg-port): optimizer/planmain.h -- create_plan
unsafe fn create_plan(root: *mut PlannerInfo, best_path: *mut Path) -> *mut Plan {
    crate::optimizer::plan::createplan::create_plan(root as _, best_path as _) as _
}

/// TODO(pg-port): executor/executor.h -- ExecMaterializesOutput
unsafe fn ExecMaterializesOutput(nodeTag: NodeTag) -> bool {
    crate::executor::execAmi::ExecMaterializesOutput(nodeTag as _) as _
}

/// TODO(pg-port): optimizer/planmain.h -- materialize_finished_plan
unsafe fn materialize_finished_plan(plan: *mut Plan) -> *mut Plan {
    crate::optimizer::plan::createplan::materialize_finished_plan(plan as _) as _
}

/// TODO(pg-port): optimizer/paramassign.h -- generate_new_exec_param
unsafe fn generate_new_exec_param(
    root: *mut PlannerInfo,
    paramtype: Oid,
    paramtypmod: i32,
    paramcollation: Oid,
) -> *mut Param {
    crate::optimizer::util::paramassign::generate_new_exec_param(
        root as _,
        paramtype as _,
        paramtypmod as _,
        paramcollation as _,
    ) as _
}

/// TODO(pg-port): optimizer/paramassign.h -- assign_special_exec_param
unsafe fn assign_special_exec_param(root: *mut PlannerInfo) -> c_int {
    crate::optimizer::util::paramassign::assign_special_exec_param(root as _) as _
}

/// TODO(pg-port): optimizer/paramassign.h -- replace_outer_var
unsafe fn replace_outer_var(root: *mut PlannerInfo, var: *mut Var) -> *mut Param {
    crate::optimizer::util::paramassign::replace_outer_var(root as _, var as _) as _
}

/// TODO(pg-port): optimizer/paramassign.h -- replace_outer_placeholdervar
unsafe fn replace_outer_placeholdervar(
    root: *mut PlannerInfo,
    phv: *mut PlaceHolderVar,
) -> *mut Param {
    crate::optimizer::util::paramassign::replace_outer_placeholdervar(root as _, phv as _) as _
}

/// TODO(pg-port): optimizer/paramassign.h -- replace_outer_agg
unsafe fn replace_outer_agg(root: *mut PlannerInfo, agg: *mut Aggref) -> *mut Param {
    crate::optimizer::util::paramassign::replace_outer_agg(root as _, agg as _) as _
}

/// TODO(pg-port): optimizer/paramassign.h -- replace_outer_grouping
unsafe fn replace_outer_grouping(
    root: *mut PlannerInfo,
    grp: *mut GroupingFunc,
) -> *mut Param {
    crate::optimizer::util::paramassign::replace_outer_grouping(root as _, grp as _) as _
}

/// TODO(pg-port): optimizer/paramassign.h -- replace_outer_merge_support
unsafe fn replace_outer_merge_support(
    root: *mut PlannerInfo,
    msf: *mut MergeSupportFunc,
) -> *mut Param {
    crate::optimizer::util::paramassign::replace_outer_merge_support(root as _, msf as _) as _
}

/// TODO(pg-port): optimizer/paramassign.h -- replace_outer_returning
unsafe fn replace_outer_returning(
    root: *mut PlannerInfo,
    re: *mut ReturningExpr,
) -> *mut Param {
    crate::optimizer::util::paramassign::replace_outer_returning(root as _, re as _) as _
}

/// TODO(pg-port): optimizer/paramassign.h -- planner_subplan_get_plan
unsafe fn planner_subplan_get_plan(
    _root: *mut PlannerInfo,
    _subplan: *mut SubPlan,
) -> *mut Plan {
    unimplemented!()
}

/// TODO(pg-port): optimizer/paramassign.h -- find_minmax_agg_replacement_param
unsafe fn find_minmax_agg_replacement_param(
    root: *mut PlannerInfo,
    aggref: *mut Aggref,
) -> *mut Param {
    crate::optimizer::plan::setrefs::find_minmax_agg_replacement_param(root as _, aggref as _) as _
}

/// TODO(pg-port): optimizer/util/clauses.h -- contain_volatile_functions
unsafe fn contain_volatile_functions(node: *mut Node) -> bool {
    crate::optimizer::util::clauses::contain_volatile_functions(node as _) as _
}

/// TODO(pg-port): optimizer/util/clauses.h -- contain_var_clause
unsafe fn contain_var_clause(node: *mut Node) -> bool {
    crate::optimizer::util::var::contain_var_clause(node as _) as _
}

/// TODO(pg-port): optimizer/util/clauses.h -- contain_vars_of_level
unsafe fn contain_vars_of_level(node: *mut Node, levelsup: c_int) -> bool {
    crate::optimizer::util::var::contain_vars_of_level(node as _, levelsup as _) as _
}

/// TODO(pg-port): optimizer/util/clauses.h -- contain_subplans
unsafe fn contain_subplans(node: *mut Node) -> bool {
    crate::optimizer::util::clauses::contain_subplans(node as _) as _
}

/// TODO(pg-port): optimizer/util/clauses.h -- contain_exec_param
unsafe fn contain_exec_param(node: *mut Node, param_ids: *mut List) -> bool {
    crate::optimizer::util::clauses::contain_exec_param(node as _, param_ids as _) as _
}

/// TODO(pg-port): optimizer/util/clauses.h -- contain_aggs_of_level
unsafe fn contain_aggs_of_level(node: *mut Node, levelsup: c_int) -> bool {
    crate::rewrite::rewriteManip::contain_aggs_of_level(node as _, levelsup as _) as _
}

/// TODO(pg-port): optimizer/util/clauses.h -- eval_const_expressions
unsafe fn eval_const_expressions(root: *mut PlannerInfo, node: *mut Node) -> *mut Node {
    crate::optimizer::util::clauses::eval_const_expressions(root as _, node as _) as _
}

/// TODO(pg-port): optimizer/util/clauses.h -- is_andclause
unsafe fn is_andclause(_node: *mut Node) -> bool {
    unimplemented!()
}

/// TODO(pg-port): optimizer/util/clauses.h -- is_orclause
unsafe fn is_orclause(_node: *mut Node) -> bool {
    unimplemented!()
}

/// TODO(pg-port): optimizer/util/clauses.h -- make_andclause
unsafe fn make_andclause(args: *mut List) -> *mut BoolExpr {
    crate::nodes::makefuncs::make_andclause(args as _) as _
}

/// TODO(pg-port): optimizer/util/clauses.h -- make_orclause
unsafe fn make_orclause(args: *mut List) -> *mut BoolExpr {
    crate::nodes::makefuncs::make_orclause(args as _) as _
}

/// TODO(pg-port): optimizer/util/clauses.h -- make_ands_explicit
unsafe fn make_ands_explicit(andclauses: *mut List) -> *mut Expr {
    crate::nodes::makefuncs::make_ands_explicit(andclauses as _) as _
}

/// TODO(pg-port): optimizer/util/clauses.h -- make_ands_implicit
unsafe fn make_ands_implicit(clause: *mut Expr) -> *mut List {
    crate::nodes::makefuncs::make_ands_implicit(clause as _) as _
}

/// TODO(pg-port): optimizer/util/clauses.h -- canonicalize_qual
unsafe fn canonicalize_qual(qual: *mut Expr, is_check: bool) -> *mut Expr {
    crate::optimizer::prep::prepqual::canonicalize_qual(qual as _, is_check as _) as _
}

/// TODO(pg-port): optimizer/util/clauses.h -- get_hash_memory_limit
unsafe fn get_hash_memory_limit() -> f64 {
    crate::executor::nodeHash::get_hash_memory_limit() as _
}

/// TODO(pg-port): optimizer/util/var.h -- pull_varnos
unsafe fn pull_varnos(root: *mut PlannerInfo, node: *mut Node) -> *mut Bitmapset {
    crate::optimizer::util::var::pull_varnos(root as _, node as _) as _
}

/// TODO(pg-port): optimizer/util/var.h -- pull_varnos_of_level
unsafe fn pull_varnos_of_level(
    root: *mut PlannerInfo,
    node: *mut Node,
    levelsup: c_int,
) -> *mut Bitmapset {
    crate::optimizer::util::var::pull_varnos_of_level(root as _, node as _, levelsup as _) as _
}

/// TODO(pg-port): optimizer/util/var.h -- find_base_rel
unsafe fn find_base_rel(root: *mut PlannerInfo, relid: c_int) -> *mut RelOptInfo {
    crate::optimizer::util::relnode::find_base_rel(root as _, relid as _) as _
}

/// TODO(pg-port): nodes/nodeFuncs.h -- exprType
unsafe fn exprType(expr: *mut Node) -> Oid {
    crate::nodes::nodeFuncs::exprType(expr as _) as _
}

/// TODO(pg-port): nodes/nodeFuncs.h -- exprTypmod
unsafe fn exprTypmod(expr: *mut Node) -> i32 {
    crate::nodes::nodeFuncs::exprTypmod(expr as _) as _
}

/// TODO(pg-port): nodes/nodeFuncs.h -- exprCollation
unsafe fn exprCollation(expr: *mut Node) -> Oid {
    crate::nodes::nodeFuncs::exprCollation(expr as _) as _
}

/// TODO(pg-port): nodes/nodeFuncs.h -- expression_tree_mutator
unsafe fn expression_tree_mutator(
    node: *mut Node,
    mutator: unsafe fn(*mut Node, *mut c_void) -> *mut Node,
    context: *mut c_void,
) -> *mut Node {
    crate::nodes::nodeFuncs::expression_tree_mutator(node as _, core::mem::transmute(mutator), context as _) as _
}

/// TODO(pg-port): nodes/nodeFuncs.h -- expression_tree_walker
unsafe fn expression_tree_walker(
    node: *mut Node,
    walker: unsafe fn(*mut Node, *mut c_void) -> bool,
    context: *mut c_void,
) -> bool {
    crate::nodes::nodeFuncs::expression_tree_walker(node as _, core::mem::transmute(walker), context as _) as _
}

/// TODO(pg-port): nodes/nodeFuncs.h -- query_tree_walker
unsafe fn query_tree_walker(
    query: *mut Query,
    walker: unsafe fn(*mut Node, *mut c_void) -> bool,
    context: *mut c_void,
    flags: c_int,
) -> bool {
    crate::nodes::nodeFuncs::query_tree_walker(query as _, core::mem::transmute(walker), context as _, flags as _) as _
}

/// TODO(pg-port): rewrite/rewriteManip.h -- OffsetVarNodes
unsafe fn OffsetVarNodes(node: *mut Node, offset: c_int, sublevels_up: c_int) {
    crate::rewrite::rewriteManip::OffsetVarNodes(node as _, offset as _, sublevels_up as _)
}

/// TODO(pg-port): rewrite/rewriteManip.h -- IncrementVarSublevelsUp
unsafe fn IncrementVarSublevelsUp(
    node: *mut Node,
    delta_sublevels_up: c_int,
    min_sublevels_up: c_int,
) {
    crate::rewrite::rewriteManip::IncrementVarSublevelsUp(node as _, delta_sublevels_up as _, min_sublevels_up as _)
}

/// TODO(pg-port): parser/parse_relation.h -- make_parsestate
unsafe fn make_parsestate(parent_pstate: *mut c_void) -> *mut c_void {
    crate::parser::parse_node::make_parsestate(parent_pstate as _) as _
}

/// TODO(pg-port): parser/parse_relation.h -- addRangeTableEntryForSubquery
unsafe fn addRangeTableEntryForSubquery(
    pstate: *mut c_void,
    subquery: *mut Query,
    alias: *mut c_void,
    lateral: bool,
    inFromCl: bool,
) -> *mut c_void {
    crate::parser::parse_relation::addRangeTableEntryForSubquery(
        pstate as _,
        subquery as _,
        alias as _,
        lateral as _,
        inFromCl as _,
    ) as _
}

/// TODO(pg-port): nodes/makefuncs.h -- makeAlias
unsafe fn makeAlias(aliasname: *const c_char, colnames: *mut List) -> *mut c_void {
    crate::nodes::makefuncs::makeAlias(aliasname as _, colnames as _) as _
}

/// TODO(pg-port): nodes/makefuncs.h -- makeVarFromTargetEntry
unsafe fn makeVarFromTargetEntry(varno: Index, tle: *mut TargetEntry) -> *mut Var {
    crate::nodes::makefuncs::makeVarFromTargetEntry(varno as _, tle as _) as _
}

/// TODO(pg-port): nodes/makefuncs.h -- makeTargetEntry
unsafe fn makeTargetEntry(
    expr: *mut Expr,
    resno: i16,
    resname: *mut c_char,
    resjunk: bool,
) -> *mut TargetEntry {
    crate::nodes::makefuncs::makeTargetEntry(expr as _, resno as _, resname as _, resjunk as _) as _
}

/// TODO(pg-port): nodes/makefuncs.h -- makeNullConst
unsafe fn makeNullConst(consttype: Oid, consttypmod: i32, constcollid: Oid) -> *mut Const {
    crate::nodes::makefuncs::makeNullConst(consttype as _, consttypmod as _, constcollid as _) as _
}

/// TODO(pg-port): nodes/makefuncs.h -- make_opclause
unsafe fn make_opclause(
    opno: Oid,
    opresulttype: Oid,
    opretset: bool,
    leftop: *mut Expr,
    rightop: *mut Expr,
    opcollid: Oid,
    inputcollid: Oid,
) -> *mut Expr {
    crate::nodes::makefuncs::make_opclause(
        opno as _,
        opresulttype as _,
        opretset as _,
        leftop as _,
        rightop as _,
        opcollid as _,
        inputcollid as _,
    ) as _
}

/// TODO(pg-port): nodes/makefuncs.h -- make_SAOP_expr
unsafe fn make_SAOP_expr(
    opno: Oid,
    leftop: *mut Node,
    righttype: Oid,
    rightcollid: Oid,
    inputcollid: Oid,
    exprs: *mut List,
    useOr: bool,
) -> *mut ScalarArrayOpExpr {
    crate::optimizer::util::clauses::make_SAOP_expr(
        opno as _,
        leftop as _,
        righttype as _,
        rightcollid as _,
        inputcollid as _,
        exprs as _,
        useOr as _,
    ) as _
}

/// TODO(pg-port): utils/lsyscache.h -- get_promoted_array_type
unsafe fn get_promoted_array_type(typid: Oid) -> Oid {
    crate::utils::cache::lsyscache::get_promoted_array_type(typid as _) as _
}

/// TODO(pg-port): utils/lsyscache.h -- op_hashjoinable
unsafe fn op_hashjoinable(opno: Oid, inputtype: Oid) -> bool {
    crate::utils::cache::lsyscache::op_hashjoinable(opno as _, inputtype as _) as _
}

/// TODO(pg-port): utils/lsyscache.h -- get_commutator
unsafe fn get_commutator(opno: Oid) -> Oid {
    crate::utils::cache::lsyscache::get_commutator(opno as _) as _
}

/// TODO(pg-port): utils/lsyscache.h -- func_strict
unsafe fn func_strict(funcid: Oid) -> bool {
    crate::utils::cache::lsyscache::func_strict(funcid as _) as _
}

/// TODO(pg-port): utils/syscache.h -- SearchSysCache1
unsafe fn SearchSysCache1(cacheId: c_int, key1: u64) -> *mut crate::access::htup_details::HeapTupleData {
    crate::utils::cache::syscache::SearchSysCache1(cacheId as _, key1 as _) as _
}

/// TODO(pg-port): utils/syscache.h -- ReleaseSysCache
unsafe fn ReleaseSysCache(tup: *mut crate::access::htup_details::HeapTupleData) {
    crate::utils::cache::syscache::ReleaseSysCache(tup as _)
}

/// TODO(pg-port): utils/syscache.h -- HeapTupleIsValid
unsafe fn HeapTupleIsValid(tup: *mut crate::access::htup_details::HeapTupleData) -> bool {
    crate::access::htup_details::HeapTupleIsValid(tup as _) as _
}

/// TODO(pg-port): utils/builtins.h -- format_type_be
unsafe fn format_type_be(typid: Oid) -> *mut c_char {
    crate::utils::adt::format_type::format_type_be(typid as _) as _
}

/// TODO(pg-port): utils/builtins.h -- psprintf
unsafe fn psprintf(_fmt: *const c_char) -> *mut c_char {
    core::ptr::null_mut()
}

/// TODO(pg-port): nodes/copyfuncs.h -- copyObject
unsafe fn copyObject<T>(_obj: *mut T) -> *mut T {
    core::ptr::null_mut()
}

/// TODO(pg-port): miscadmin.h -- OidIsValid
#[inline]
fn OidIsValid(oid: Oid) -> bool {
    oid != 0
}

/// TODO(pg-port): access/htup_details.h -- GETSTRUCT
unsafe fn GETSTRUCT(tup: *mut crate::access::htup_details::HeapTupleData) -> *mut Form_pg_operator {
    crate::access::htup_details::GETSTRUCT(tup as _) as _
}

/// TODO(pg-port): pg_operator.h form
#[repr(C)]
pub struct FormData_pg_operator {
    pub oprcanhash: bool,
    pub oprcode: Oid,
    /* other fields omitted */
}
pub type Form_pg_operator = *mut FormData_pg_operator;

/// TODO(pg-port): nodes/pg_list.h -- list_make2
unsafe fn list_make2(x1: *mut c_void, x2: *mut c_void) -> *mut List {
    crate::list_make2!(x1, x2) as _
}

/// TODO(pg-port): access/attnum.h
type AttrNumber = i16;

/// TODO(pg-port): postgres_ext.h
const InvalidOid: Oid = 0;

/// TODO(pg-port): catalog/pg_type.h OID constants
const BOOLOID: Oid = 16;
const INT8OID: Oid = 20;
const RECORDOID: Oid = 2249;
const VOIDOID: Oid = 2278;

/// TODO(pg-port): catalog/pg_operator.h OID constants
const ARRAY_EQ_OP: Oid = 375;
const RECORD_EQ_OP: Oid = 2988;

/// TODO(pg-port): nodes/pg_list.h -- MAXALIGN
unsafe fn MAXALIGN(x: f64) -> f64 {
    ((x as usize + 7) & !7) as f64
}

/// TODO(pg-port): access/htup_details.h -- SizeofHeapTupleHeader
const SizeofHeapTupleHeader: f64 = 23.0;

/// TODO(pg-port): utils/datum.h -- DatumGetInt64
unsafe fn DatumGetInt64(d: u64) -> i64 {
    d as i64
}

/// TODO(pg-port): catalog/pg_type.h -- ObjectIdGetDatum
fn ObjectIdGetDatum(oid: Oid) -> u64 {
    oid as u64
}

/// TODO(pg-port): utils/syscache.h -- OPEROID cache id
const OPEROID: c_int = 40;

/// TODO(pg-port): nodes/parsenodes.h QTW flags
const QTW_EXAMINE_RTES_BEFORE: c_int = 0x0010;
const QTW_EXAMINE_RTES_AFTER: c_int = 0x0020;

// JOIN_SEMI / JOIN_ANTI come from crate::nodes::nodes::JoinType::* (imported above)

/// TODO(pg-port): parser/parse_relation.h -- CombineRangeTables
unsafe fn CombineRangeTables(
    dst_rtable: *mut *mut List,
    dst_rteperminfos: *mut *mut List,
    src_rtable: *mut List,
    src_rteperminfos: *mut List,
) {
    crate::rewrite::rewriteManip::CombineRangeTables(
        dst_rtable as _,
        dst_rteperminfos as _,
        src_rtable as _,
        src_rteperminfos as _,
    )
}

/// TODO(pg-port): optimizer/prep.h -- replace_empty_jointree
unsafe fn replace_empty_jointree(subselect: *mut Query) {
    crate::optimizer::prep::prepjointree::replace_empty_jointree(subselect as _)
}

/// TODO(pg-port): nodes/primnodes.h JoinExpr
#[repr(C)]
pub struct JoinExpr {
    pub xpr: crate::nodes::primnodes::Expr,
    pub jointype: c_int,
    pub isNatural: bool,
    pub larg: *mut Node,
    pub rarg: *mut Node,
    pub usingClause: *mut List,
    pub join_using_alias: *mut c_void,
    pub quals: *mut Node,
    pub alias: *mut c_void,
    pub rtindex: c_int,
}

/// TODO(pg-port): nodes/primnodes.h RangeTblRef
#[repr(C)]
pub struct RangeTblRefLocal {
    pub xpr: crate::nodes::primnodes::Expr,
    pub rtindex: c_int,
}
type RangeTblRef = RangeTblRefLocal;

// ---------------------------------------------------------------------------
// enable_material GUC (miscadmin)
// ---------------------------------------------------------------------------
pub static mut enable_material: bool = true;

// ===========================================================================
// Part 1 ends here.  The actual functions follow in parts 2-4.
// ===========================================================================

/*
 * Get the datatype/typmod/collation of the first column of the plan's output.
 *
 * This information is stored for ARRAY_SUBLINK execution and for
 * exprType()/exprTypmod()/exprCollation(), which have no way to get at the
 * plan associated with a SubPlan node.  We really only need the info for
 * EXPR_SUBLINK and ARRAY_SUBLINK subplans, but for consistency we save it
 * always.
 */
unsafe fn get_first_col_type(
    plan: *mut Plan,
    coltype: *mut Oid,
    coltypmod: *mut i32,
    colcollation: *mut Oid,
) {
    /* In cases such as EXISTS, tlist might be empty; arbitrarily use VOID */
    if !(*plan).targetlist.is_null() {
        let tent: *mut TargetEntry =
            linitial((*plan).targetlist) as *mut TargetEntry;
        if !(*tent).resjunk {
            *coltype = exprType((*tent).expr as *mut Node);
            *coltypmod = exprTypmod((*tent).expr as *mut Node);
            *colcollation = exprCollation((*tent).expr as *mut Node);
            return;
        }
    }
    *coltype = VOIDOID;
    *coltypmod = -1;
    *colcollation = 0; /* InvalidOid */
}

/*
 * Convert a SubLink (as created by the parser) into a SubPlan.
 */
unsafe fn make_subplan(
    root: *mut PlannerInfo,
    orig_subquery: *mut Query,
    subLinkType: SubLinkType,
    subLinkId: c_int,
    testexpr: *mut Node,
    isTopQual: bool,
) -> *mut Node {
    let mut subquery: *mut Query;
    let mut simple_exists: bool = false;
    let tuple_fraction: f64;
    let subroot: *mut PlannerInfo;
    let final_rel: *mut RelOptInfo;
    let best_path: *mut Path;
    let mut plan: *mut Plan;
    let plan_params: *mut List;
    let mut result: *mut Node;

    /*
     * Copy the source Query node.  This is a quick and dirty kluge to resolve
     * the fact that the parser can generate trees with multiple links to the
     * same sub-Query node, but the planner wants to scribble on the Query.
     * Try to clean this up when we do querytree redesign...
     */
    subquery = copyObject(orig_subquery as *mut c_void) as *mut Query;

    /*
     * If it's an EXISTS subplan, we might be able to simplify it.
     */
    if subLinkType == EXISTS_SUBLINK {
        simple_exists = simplify_EXISTS_query(root, subquery);
    }

    /*
     * For an EXISTS subplan, tell lower-level planner to expect that only the
     * first tuple will be retrieved.  For ALL and ANY subplans, we will be
     * able to stop evaluating if the test condition fails or matches, so very
     * often not all the tuples will be retrieved; for lack of a better idea,
     * specify 50% retrieval.  For EXPR, MULTIEXPR, and ROWCOMPARE subplans,
     * use default behavior (we're only expecting one row out, anyway).
     *
     * NOTE: if you change these numbers, also change cost_subplan() in
     * path/costsize.c.
     *
     * XXX If an ANY subplan is uncorrelated, build_subplan may decide to hash
     * its output.  In that case it would've been better to specify full
     * retrieval.  At present, however, we can only check hashability after
     * we've made the subplan :-(.  (Determining whether it'll fit in hash_mem
     * is the really hard part.)  Therefore, we don't want to be too
     * optimistic about the percentage of tuples retrieved, for fear of
     * selecting a plan that's bad for the materialization case.
     */
    if subLinkType == EXISTS_SUBLINK {
        tuple_fraction = 1.0; /* just like a LIMIT 1 */
    } else if subLinkType == ALL_SUBLINK || subLinkType == ANY_SUBLINK {
        tuple_fraction = 0.5; /* 50% */
    } else {
        tuple_fraction = 0.0; /* default behavior */
    }

    /* plan_params should not be in use in current query level */
    Assert!((*root).plan_params.is_null() || list_length((*root).plan_params) == 0);

    /* Generate Paths for the subquery */
    let subroot = subquery_planner(
        (*root).glob,
        subquery,
        root,
        false,
        tuple_fraction,
        ptr::null_mut(),
    );

    /* Isolate the params needed by this specific subplan */
    plan_params = (*root).plan_params;
    (*root).plan_params = NIL;

    /*
     * Select best Path and turn it into a Plan.  At least for now, there
     * seems no reason to postpone doing that.
     */
    final_rel = fetch_upper_rel(subroot, UPPERREL_FINAL, ptr::null_mut());
    best_path = get_cheapest_fractional_path(final_rel, tuple_fraction);

    plan = create_plan(subroot, best_path);

    /* And convert to SubPlan or InitPlan format. */
    result = build_subplan(
        root,
        plan,
        best_path,
        subroot,
        plan_params,
        subLinkType,
        subLinkId,
        testexpr,
        NIL,
        isTopQual,
    );

    /*
     * If it's a correlated EXISTS with an unimportant targetlist, we might be
     * able to transform it to the equivalent of an IN and then implement it
     * by hashing.  We don't have enough information yet to tell which way is
     * likely to be better (it depends on the expected number of executions of
     * the EXISTS qual, and we are much too early in planning the outer query
     * to be able to guess that).  So we generate both plans, if possible, and
     * leave it to setrefs.c to decide which to use.
     */
    if simple_exists && IsA!(result, T_SubPlan) {
        let mut newtestexpr: *mut Node = ptr::null_mut();
        let mut paramIds: *mut List = ptr::null_mut();

        /* Make a second copy of the original subquery */
        subquery = copyObject(orig_subquery as *mut c_void) as *mut Query;
        /* and re-simplify */
        simple_exists = simplify_EXISTS_query(root, subquery);
        Assert!(simple_exists);
        /* See if it can be converted to an ANY query */
        subquery =
            convert_EXISTS_to_ANY(root, subquery, &mut newtestexpr, &mut paramIds);
        if !subquery.is_null() {
            /* Generate Paths for the ANY subquery; we'll need all rows */
            let subroot2 = subquery_planner(
                (*root).glob,
                subquery,
                root,
                false,
                0.0,
                ptr::null_mut(),
            );

            /* Isolate the params needed by this specific subplan */
            let plan_params2 = (*root).plan_params;
            (*root).plan_params = NIL;

            /* Select best Path */
            let final_rel2 = fetch_upper_rel(subroot2, UPPERREL_FINAL, ptr::null_mut());
            let best_path2 = (*final_rel2).cheapest_total_path;

            /* Now we can check if it'll fit in hash_mem */
            if subpath_is_hashable(best_path2) {
                /* OK, finish planning the ANY subquery */
                let plan2 = create_plan(subroot2, best_path2);

                /* ... and convert to SubPlan format */
                let hashplan = castNode!(SubPlan, T_SubPlan, build_subplan(
                    root,
                    plan2,
                    best_path2,
                    subroot2,
                    plan_params2,
                    ANY_SUBLINK,
                    0,
                    newtestexpr,
                    paramIds,
                    true,
                ) as *mut c_void) as *mut SubPlan;
                /* Check we got what we expected */
                Assert!((*hashplan).parParam.is_null() || list_length((*hashplan).parParam) == 0);
                Assert!((*hashplan).useHashTable);

                /* Leave it to setrefs.c to decide which plan to use */
                let asplan: *mut AlternativeSubPlan =
                    makeNode!(AlternativeSubPlan, T_AlternativeSubPlan) as *mut AlternativeSubPlan;
                (*asplan).subplans = list_make2(result as *mut c_void, hashplan as *mut c_void);
                result = asplan as *mut Node;
                (*root).hasAlternativeSubPlans = true;
            }
        }
    }

    result
}

/*
 * Build a SubPlan node given the raw inputs --- subroutine for make_subplan
 *
 * Returns either the SubPlan, or a replacement expression if we decide to
 * make it an InitPlan, as explained in the comments for make_subplan.
 */
unsafe fn build_subplan(
    root: *mut PlannerInfo,
    mut plan: *mut Plan,
    path: *mut Path,
    subroot: *mut PlannerInfo,
    plan_params: *mut List,
    subLinkType: SubLinkType,
    subLinkId: c_int,
    testexpr: *mut Node,
    testexpr_paramids: *mut List,
    unknownEqFalse: bool,
) -> *mut Node {
    let mut result: *mut Node;
    let splan: *mut SubPlan;
    let isInitPlan: bool;
    let mut lc: *mut ListCell;

    /*
     * Initialize the SubPlan node.  Note plan_id, plan_name, and cost fields
     * are set further down.
     */
    splan = makeNode!(SubPlan, T_SubPlan) as *mut SubPlan;
    (*splan).subLinkType = subLinkType;
    (*splan).testexpr = ptr::null_mut();
    (*splan).paramIds = NIL;
    get_first_col_type(
        plan,
        &mut (*splan).firstColType,
        &mut (*splan).firstColTypmod,
        &mut (*splan).firstColCollation,
    );
    (*splan).useHashTable = false;
    (*splan).unknownEqFalse = unknownEqFalse;
    (*splan).parallel_safe = (*plan).parallel_safe;
    (*splan).setParam = NIL;
    (*splan).parParam = NIL;
    (*splan).args = NIL;

    /*
     * Make parParam and args lists of param IDs and expressions that current
     * query level will pass to this child plan.
     */
    foreach!(lc, plan_params, {
        let pitem: *mut PlannerParamItem =
            lfirst(crate::current_cell!(lc)) as *mut PlannerParamItem;
        let mut arg: *mut Node = (*pitem).item as *mut Node;

        /*
         * The Var, PlaceHolderVar, Aggref, GroupingFunc, or ReturningExpr has
         * already been adjusted to have the correct varlevelsup, phlevelsup,
         * agglevelsup, or retlevelsup.
         *
         * If it's a PlaceHolderVar, Aggref, GroupingFunc, or ReturningExpr,
         * its arguments might contain SubLinks, which have not yet been
         * processed (see the comments for SS_replace_correlation_vars).  Do
         * that now.
         */
        if IsA!(arg, T_PlaceHolderVar)
            || IsA!(arg, T_Aggref)
            || IsA!(arg, T_GroupingFunc)
            || IsA!(arg, T_ReturningExpr)
        {
            arg = SS_process_sublinks(root, arg, false);
        }

        (*splan).parParam = lappend_int((*splan).parParam, (*pitem).paramId);
        (*splan).args = lappend((*splan).args, arg as *mut c_void);
    });

    /*
     * Un-correlated or undirect correlated plans of EXISTS, EXPR, ARRAY,
     * ROWCOMPARE, or MULTIEXPR types can be used as initPlans.  For EXISTS,
     * EXPR, or ARRAY, we return a Param referring to the result of evaluating
     * the initPlan.  For ROWCOMPARE, we must modify the testexpr tree to
     * contain PARAM_EXEC Params instead of the PARAM_SUBLINK Params emitted
     * by the parser, and then return that tree.  For MULTIEXPR, we return a
     * null constant: the resjunk targetlist item containing the SubLink does
     * not need to return anything useful, since the referencing Params are
     * elsewhere.
     */
    let parParam_empty = (*splan).parParam.is_null()
        || list_length((*splan).parParam) == 0;

    if parParam_empty && subLinkType == EXISTS_SUBLINK {
        let prm: *mut Param;

        Assert!(testexpr.is_null());
        prm = generate_new_exec_param(root, BOOLOID, -1, 0);
        (*splan).setParam = list_make1_int!((*prm).paramid);
        isInitPlan = true;
        result = prm as *mut Node;
    } else if parParam_empty && subLinkType == EXPR_SUBLINK {
        let te: *mut TargetEntry = linitial((*plan).targetlist) as *mut TargetEntry;
        let prm: *mut Param;

        Assert!(!(*te).resjunk);
        Assert!(testexpr.is_null());
        prm = generate_new_exec_param(
            root,
            exprType((*te).expr as *mut Node),
            exprTypmod((*te).expr as *mut Node),
            exprCollation((*te).expr as *mut Node),
        );
        (*splan).setParam = list_make1_int!((*prm).paramid);
        isInitPlan = true;
        result = prm as *mut Node;
    } else if parParam_empty && subLinkType == ARRAY_SUBLINK {
        let te: *mut TargetEntry = linitial((*plan).targetlist) as *mut TargetEntry;
        let arraytype: Oid;
        let prm: *mut Param;

        Assert!(!(*te).resjunk);
        Assert!(testexpr.is_null());
        arraytype = get_promoted_array_type(exprType((*te).expr as *mut Node));
        if !OidIsValid(arraytype) {
            elog!(
                crate::utils::elog::ERROR,
                "could not find array type for datatype {}",
                /* format_type_be result: */ ""
            );
        }
        prm = generate_new_exec_param(
            root,
            arraytype,
            exprTypmod((*te).expr as *mut Node),
            exprCollation((*te).expr as *mut Node),
        );
        (*splan).setParam = list_make1_int!((*prm).paramid);
        isInitPlan = true;
        result = prm as *mut Node;
    } else if parParam_empty && subLinkType == ROWCOMPARE_SUBLINK {
        /* Adjust the Params */
        let params: *mut List;

        Assert!(!testexpr.is_null());
        params = generate_subquery_params(
            root,
            (*plan).targetlist,
            &mut (*splan).paramIds,
        );
        result = convert_testexpr(root, testexpr, params);
        (*splan).setParam = list_copy((*splan).paramIds);
        isInitPlan = true;

        /*
         * The executable expression is returned to become part of the outer
         * plan's expression tree; it is not kept in the initplan node.
         */
    } else if subLinkType == MULTIEXPR_SUBLINK {
        /*
         * Whether it's an initplan or not, it needs to set a PARAM_EXEC Param
         * for each output column.
         */
        let params: *mut List;

        Assert!(testexpr.is_null());
        params = generate_subquery_params(
            root,
            (*plan).targetlist,
            &mut (*splan).setParam,
        );

        /*
         * Save the list of replacement Params in the n'th cell of
         * root->multiexpr_params; setrefs.c will use it to replace
         * PARAM_MULTIEXPR Params.
         */
        while list_length((*root).multiexpr_params) < subLinkId {
            (*root).multiexpr_params = lappend((*root).multiexpr_params, NIL as *mut c_void);
        }
        lc = list_nth_cell((*root).multiexpr_params, subLinkId - 1);
        Assert!(lfirst(lc) == (NIL as *mut c_void));
        /* set the cell value */
        (*lc).ptr_value = params as *mut c_void;

        /* It can be an initplan if there are no parParams. */
        if parParam_empty {
            isInitPlan = true;
            result = makeNullConst(RECORDOID, -1, 0) as *mut Node;
        } else {
            isInitPlan = false;
            result = splan as *mut Node;
        }
    } else {
        /*
         * Adjust the Params in the testexpr, unless caller already took care
         * of it (as indicated by passing a list of Param IDs).
         */
        let te_paramids_empty = testexpr_paramids.is_null()
            || list_length(testexpr_paramids) == 0;
        if !testexpr.is_null() && te_paramids_empty {
            let params: *mut List;

            params = generate_subquery_params(
                root,
                (*plan).targetlist,
                &mut (*splan).paramIds,
            );
            (*splan).testexpr = convert_testexpr(root, testexpr, params);
        } else {
            (*splan).testexpr = testexpr;
            (*splan).paramIds = testexpr_paramids;
        }

        /*
         * We can't convert subplans of ALL_SUBLINK or ANY_SUBLINK types to
         * initPlans, even when they are uncorrelated or undirect correlated,
         * because we need to scan the output of the subplan for each outer
         * tuple.  But if it's a not-direct-correlated IN (= ANY) test, we
         * might be able to use a hashtable to avoid comparing all the tuples.
         */
        if subLinkType == ANY_SUBLINK
            && parParam_empty
            && subplan_is_hashable(plan)
            && testexpr_is_hashable((*splan).testexpr, (*splan).paramIds)
        {
            (*splan).useHashTable = true;
        }
        /*
         * Otherwise, we have the option to tack a Material node onto the top
         * of the subplan, to reduce the cost of reading it repeatedly.  This
         * is pointless for a direct-correlated subplan, since we'd have to
         * recompute its results each time anyway.  For uncorrelated/undirect
         * correlated subplans, we add Material unless the subplan's top plan
         * node would materialize its output anyway.  Also, if enable_material
         * is false, then the user does not want us to materialize anything
         * unnecessarily, so we don't.
         */
        else if parParam_empty && enable_material && !ExecMaterializesOutput(nodeTag(plan)) {
            plan = materialize_finished_plan(plan);
        }

        result = splan as *mut Node;
        isInitPlan = false;
    }

    /*
     * Add the subplan, its path, and its PlannerInfo to the global lists.
     */
    (*(*root).glob).subplans = lappend((*(*root).glob).subplans, plan as *mut c_void);
    (*(*root).glob).subpaths = lappend((*(*root).glob).subpaths, path as *mut c_void);
    (*(*root).glob).subroots = lappend((*(*root).glob).subroots, subroot as *mut c_void);
    (*splan).plan_id = list_length((*(*root).glob).subplans);

    if isInitPlan {
        (*root).init_plans = lappend((*root).init_plans, splan as *mut c_void);
    }

    /*
     * A parameterless subplan (not initplan) should be prepared to handle
     * REWIND efficiently.  If it has direct parameters then there's no point
     * since it'll be reset on each scan anyway; and if it's an initplan then
     * there's no point since it won't get re-run without parameter changes
     * anyway.  The input of a hashed subplan doesn't need REWIND either.
     */
    if parParam_empty && !isInitPlan && !(*splan).useHashTable {
        (*(*root).glob).rewindPlanIDs = bms_add_member(
            (*(*root).glob).rewindPlanIDs,
            (*splan).plan_id,
        );
    }

    /* Label the subplan for EXPLAIN purposes */
    let label = if isInitPlan { "InitPlan\0" } else { "SubPlan\0" };
    (*splan).plan_name = psprintf(b"%s %d\0".as_ptr() as *const c_char);

    /* Lastly, fill in the cost estimates for use later */
    cost_subplan(root, splan, plan);

    result
}

/*
 * generate_subquery_params: build a list of Params representing the output
 * columns of a sublink's sub-select, given the sub-select's targetlist.
 *
 * We also return an integer list of the paramids of the Params.
 */
unsafe fn generate_subquery_params(
    root: *mut PlannerInfo,
    tlist: *mut List,
    paramIds: *mut *mut List,
) -> *mut List {
    let mut result: *mut List = NIL;
    let mut ids: *mut List = NIL;
    let mut lc: *mut ListCell;

    foreach!(lc, tlist, {
        let tent: *mut TargetEntry =
            lfirst(crate::current_cell!(lc)) as *mut TargetEntry;
        let param: *mut Param;

        if (*tent).resjunk {
            continue;
        }

        param = generate_new_exec_param(
            root,
            exprType((*tent).expr as *mut Node),
            exprTypmod((*tent).expr as *mut Node),
            exprCollation((*tent).expr as *mut Node),
        );
        result = lappend(result, param as *mut c_void);
        ids = lappend_int(ids, (*param).paramid);
    });

    *paramIds = ids;
    result
}

/*
 * generate_subquery_vars: build a list of Vars representing the output
 * columns of a sublink's sub-select, given the sub-select's targetlist.
 * The Vars have the specified varno (RTE index).
 */
unsafe fn generate_subquery_vars(
    root: *mut PlannerInfo,
    tlist: *mut List,
    varno: Index,
) -> *mut List {
    let mut result: *mut List = NIL;
    let mut lc: *mut ListCell;

    foreach!(lc, tlist, {
        let tent: *mut TargetEntry =
            lfirst(crate::current_cell!(lc)) as *mut TargetEntry;
        let var: *mut Var;

        if (*tent).resjunk {
            continue;
        }

        var = makeVarFromTargetEntry(varno, tent);
        result = lappend(result, var as *mut c_void);
    });

    result
}

/*
 * convert_testexpr: convert the testexpr given by the parser into
 * actually executable form.  This entails replacing PARAM_SUBLINK Params
 * with Params or Vars representing the results of the sub-select.  The
 * nodes to be substituted are passed in as the List result from
 * generate_subquery_params or generate_subquery_vars.
 */
unsafe fn convert_testexpr(
    root: *mut PlannerInfo,
    testexpr: *mut Node,
    subst_nodes: *mut List,
) -> *mut Node {
    let mut context = convert_testexpr_context {
        root,
        subst_nodes,
    };
    convert_testexpr_mutator(testexpr, &mut context)
}

unsafe fn convert_testexpr_mutator(
    node: *mut Node,
    context: *mut convert_testexpr_context,
) -> *mut Node {
    if node.is_null() {
        return ptr::null_mut();
    }
    if IsA!(node, T_Param) {
        let param: *mut Param = node as *mut Param;

        if (*param).paramkind == PARAM_SUBLINK {
            if (*param).paramid <= 0
                || (*param).paramid > list_length((*context).subst_nodes)
            {
                elog!(
                    crate::utils::elog::ERROR,
                    "unexpected PARAM_SUBLINK ID: {}",
                    (*param).paramid
                );
            }

            /*
             * We copy the list item to avoid having doubly-linked
             * substructure in the modified parse tree.  This is probably
             * unnecessary when it's a Param, but be safe.
             */
            return copyObject(
                list_nth((*context).subst_nodes, (*param).paramid - 1),
            ) as *mut Node;
        }
    }
    if IsA!(node, T_SubLink) {
        /*
         * If we come across a nested SubLink, it is neither necessary nor
         * correct to recurse into it: any PARAM_SUBLINKs we might find inside
         * belong to the inner SubLink not the outer. So just return it as-is.
         *
         * This reasoning depends on the assumption that nothing will pull
         * subexpressions into or out of the testexpr field of a SubLink, at
         * least not without replacing PARAM_SUBLINKs first.  If we did want
         * to do that we'd need to rethink the parser-output representation
         * altogether, since currently PARAM_SUBLINKs are only unique per
         * SubLink not globally across the query.  The whole point of
         * replacing them with Vars or PARAM_EXEC nodes is to make them
         * globally unique before they escape from the SubLink's testexpr.
         *
         * Note: this can't happen when called during SS_process_sublinks,
         * because that recursively processes inner SubLinks first.  It can
         * happen when called from convert_ANY_sublink_to_join, though.
         */
        return node;
    }

    // Trampoline: wrap the typed callback into the *mut c_void form expected by
    // expression_tree_mutator.
    unsafe fn mutator_trampoline(
        node: *mut Node,
        context: *mut c_void,
    ) -> *mut Node {
        convert_testexpr_mutator(node, context as *mut convert_testexpr_context)
    }
    expression_tree_mutator(node, mutator_trampoline, context as *mut c_void)
}

/*
 * subplan_is_hashable: can we implement an ANY subplan by hashing?
 *
 * This is not responsible for checking whether the combining testexpr
 * is suitable for hashing.  We only look at the subquery itself.
 */
unsafe fn subplan_is_hashable(plan: *mut Plan) -> bool {
    let subquery_size: f64;

    /*
     * The estimated size of the subquery result must fit in hash_mem. (Note:
     * we use heap tuple overhead here even though the tuples will actually be
     * stored as MinimalTuples; this provides some fudge factor for hashtable
     * overhead.)
     */
    subquery_size = (*plan).plan_rows
        * (MAXALIGN((*plan).plan_width as f64) + MAXALIGN(SizeofHeapTupleHeader));
    if subquery_size > get_hash_memory_limit() {
        return false;
    }

    true
}

/*
 * subpath_is_hashable: can we implement an ANY subplan by hashing?
 *
 * Identical to subplan_is_hashable, but work from a Path for the subplan.
 */
unsafe fn subpath_is_hashable(path: *mut Path) -> bool {
    let subquery_size: f64;

    /*
     * The estimated size of the subquery result must fit in hash_mem. (Note:
     * we use heap tuple overhead here even though the tuples will actually be
     * stored as MinimalTuples; this provides some fudge factor for hashtable
     * overhead.)
     */
    subquery_size = (*path).rows
        * (MAXALIGN((*(*path).pathtarget).width as f64)
            + MAXALIGN(SizeofHeapTupleHeader));
    if subquery_size > get_hash_memory_limit() {
        return false;
    }

    true
}

/*
 * testexpr_is_hashable: is an ANY SubLink's test expression hashable?
 *
 * To identify LHS vs RHS of the hash expression, we must be given the
 * list of output Param IDs of the SubLink's subquery.
 */
unsafe fn testexpr_is_hashable(testexpr: *mut Node, param_ids: *mut List) -> bool {
    /*
     * The testexpr must be a single OpExpr, or an AND-clause containing only
     * OpExprs, each of which satisfy test_opexpr_is_hashable().
     */
    if !testexpr.is_null() && IsA!(testexpr, T_OpExpr) {
        if test_opexpr_is_hashable(testexpr as *mut OpExpr, param_ids) {
            return true;
        }
    } else if is_andclause(testexpr) {
        let mut l: *mut ListCell;

        foreach!(l, (*(testexpr as *mut BoolExpr)).args, {
            let andarg: *mut Node = lfirst(crate::current_cell!(l)) as *mut Node;

            if !IsA!(andarg, T_OpExpr) {
                return false;
            }
            if !test_opexpr_is_hashable(andarg as *mut OpExpr, param_ids) {
                return false;
            }
        });
        return true;
    }

    false
}

unsafe fn test_opexpr_is_hashable(
    testexpr: *mut OpExpr,
    param_ids: *mut List,
) -> bool {
    /*
     * The combining operator must be hashable and strict.  The need for
     * hashability is obvious, since we want to use hashing.  Without
     * strictness, behavior in the presence of nulls is too unpredictable.  We
     * actually must assume even more than plain strictness: it can't yield
     * NULL for non-null inputs, either (see nodeSubplan.c).  However, hash
     * indexes and hash joins assume that too.
     */
    if !hash_ok_operator(testexpr) {
        return false;
    }

    /*
     * The left and right inputs must belong to the outer and inner queries
     * respectively; hence Params that will be supplied by the subquery must
     * not appear in the LHS, and Vars of the outer query must not appear in
     * the RHS.  (Ordinarily, this must be true because of the way that the
     * parser builds an ANY SubLink's testexpr ... but inlining of functions
     * could have changed the expression's structure, so we have to check.
     * Such cases do not occur often enough to be worth trying to optimize, so
     * we don't worry about trying to commute the clause or anything like
     * that; we just need to be sure not to build an invalid plan.)
     */
    if list_length((*testexpr).args) != 2 {
        return false;
    }
    if contain_exec_param(
        linitial((*testexpr).args) as *mut Node,
        param_ids,
    ) {
        return false;
    }
    if contain_var_clause(lsecond((*testexpr).args) as *mut Node) {
        return false;
    }
    true
}

/*
 * Check expression is hashable + strict
 *
 * We could use op_hashjoinable() and op_strict(), but do it like this to
 * avoid a redundant cache lookup.
 */
unsafe fn hash_ok_operator(expr: *mut OpExpr) -> bool {
    let opid: Oid = (*expr).opno;

    /* quick out if not a binary operator */
    if list_length((*expr).args) != 2 {
        return false;
    }
    if opid == ARRAY_EQ_OP || opid == RECORD_EQ_OP {
        /* these are strict, but must check input type to ensure hashable */
        let leftarg: *mut Node = linitial((*expr).args) as *mut Node;

        return op_hashjoinable(opid, exprType(leftarg));
    } else {
        /* else must look up the operator properties */
        let tup = SearchSysCache1(OPEROID, ObjectIdGetDatum(opid));
        if !HeapTupleIsValid(tup) {
            elog!(
                crate::utils::elog::ERROR,
                "cache lookup failed for operator {}",
                opid
            );
        }
        let optup: Form_pg_operator = *GETSTRUCT(tup);
        if !(*optup).oprcanhash || !func_strict((*optup).oprcode) {
            ReleaseSysCache(tup);
            return false;
        }
        ReleaseSysCache(tup);
        return true;
    }
}


/*
 * SS_process_ctes: process a query's WITH list
 *
 * Consider each CTE in the WITH list and either ignore it (if it's an
 * unreferenced SELECT), "inline" it to create a regular sub-SELECT-in-FROM,
 * or convert it to an initplan.
 *
 * A side effect is to fill in root->cte_plan_ids with a list that
 * parallels root->parse->cteList and provides the subplan ID for
 * each CTE's initplan, or a dummy ID (-1) if we didn't make an initplan.
 */
pub unsafe fn SS_process_ctes(root: *mut PlannerInfo) {
    let mut lc: *mut ListCell;

    Assert!((*root).cte_plan_ids.is_null() || list_length((*root).cte_plan_ids) == 0);

    foreach!(lc, (*(*root).parse).cteList, {
        let cte: *mut CommonTableExpr =
            lfirst(crate::current_cell!(lc)) as *mut CommonTableExpr;
        let cmdType = (*((*cte).ctequery as *mut Query)).commandType;
        let subquery: *mut Query;
        let subroot: *mut PlannerInfo;
        let final_rel: *mut RelOptInfo;
        let best_path: *mut Path;
        let plan: *mut Plan;
        let splan: *mut SubPlan;
        let paramid: c_int;

        /*
         * Ignore SELECT CTEs that are not actually referenced anywhere.
         */
        if (*cte).cterefcount == 0 && cmdType == CMD_SELECT {
            /* Make a dummy entry in cte_plan_ids */
            (*root).cte_plan_ids = lappend_int((*root).cte_plan_ids, -1);
            continue;
        }

        /*
         * Consider inlining the CTE (creating RTE_SUBQUERY RTE(s)) instead of
         * implementing it as a separately-planned CTE.
         *
         * We cannot inline if any of these conditions hold:
         *
         * 1. The user said not to (the CTEMaterializeAlways option).
         *
         * 2. The CTE is recursive.
         *
         * 3. The CTE has side-effects; this includes either not being a plain
         * SELECT, or containing volatile functions.  Inlining might change
         * the side-effects, which would be bad.
         *
         * 4. The CTE is multiply-referenced and contains a self-reference to
         * a recursive CTE outside itself.  Inlining would result in multiple
         * recursive self-references, which we don't support.
         *
         * Otherwise, we have an option whether to inline or not.  That should
         * always be a win if there's just a single reference, but if the CTE
         * is multiply-referenced then it's unclear: inlining adds duplicate
         * computations, but the ability to absorb restrictions from the outer
         * query level could outweigh that.  We do not have nearly enough
         * information at this point to tell whether that's true, so we let
         * the user express a preference.  Our default behavior is to inline
         * only singly-referenced CTEs, but a CTE marked CTEMaterializeNever
         * will be inlined even if multiply referenced.
         *
         * Note: we check for volatile functions last, because that's more
         * expensive than the other tests needed.
         */
        if ((*cte).ctematerialized == CTEMaterializeNever
            || ((*cte).ctematerialized == CTEMaterializeDefault
                && (*cte).cterefcount == 1))
            && !(*cte).cterecursive
            && cmdType == CMD_SELECT
            && !contain_dml((*cte).ctequery as *mut Node)
            && ((*cte).cterefcount <= 1 || !contain_outer_selfref((*cte).ctequery as *mut Node))
            && !contain_volatile_functions((*cte).ctequery as *mut Node)
        {
            inline_cte(root, cte);
            /* Make a dummy entry in cte_plan_ids */
            (*root).cte_plan_ids = lappend_int((*root).cte_plan_ids, -1);
            continue;
        }

        /*
         * Copy the source Query node.  Probably not necessary, but let's keep
         * this similar to make_subplan.
         */
        let subquery = copyObject((*cte).ctequery) as *mut Query;

        /* plan_params should not be in use in current query level */
        Assert!((*root).plan_params.is_null() || list_length((*root).plan_params) == 0);

        /*
         * Generate Paths for the CTE query.  Always plan for full retrieval
         * --- we don't have enough info to predict otherwise.
         */
        let subroot = subquery_planner(
            (*root).glob,
            subquery,
            root,
            (*cte).cterecursive,
            0.0,
            ptr::null_mut(),
        );

        /*
         * Since the current query level doesn't yet contain any RTEs, it
         * should not be possible for the CTE to have requested parameters of
         * this level.
         */
        if !(*root).plan_params.is_null() && list_length((*root).plan_params) > 0 {
            elog!(
                crate::utils::elog::ERROR,
                "unexpected outer reference in CTE query"
            );
        }

        /*
         * Select best Path and turn it into a Plan.  At least for now, there
         * seems no reason to postpone doing that.
         */
        let final_rel = fetch_upper_rel(subroot, UPPERREL_FINAL, ptr::null_mut());
        let best_path = (*final_rel).cheapest_total_path;

        let plan = create_plan(subroot, best_path);

        /*
         * Make a SubPlan node for it.  This is just enough unlike
         * build_subplan that we can't share code.
         *
         * Note plan_id, plan_name, and cost fields are set further down.
         */
        let splan: *mut SubPlan = makeNode!(SubPlan, T_SubPlan) as *mut SubPlan;
        (*splan).subLinkType = CTE_SUBLINK;
        (*splan).testexpr = ptr::null_mut();
        (*splan).paramIds = NIL;
        get_first_col_type(
            plan,
            &mut (*splan).firstColType,
            &mut (*splan).firstColTypmod,
            &mut (*splan).firstColCollation,
        );
        (*splan).useHashTable = false;
        (*splan).unknownEqFalse = false;

        /*
         * CTE scans are not considered for parallelism (cf
         * set_rel_consider_parallel).
         */
        (*splan).parallel_safe = false;
        (*splan).setParam = NIL;
        (*splan).parParam = NIL;
        (*splan).args = NIL;

        /*
         * The node can't have any inputs (since it's an initplan), so the
         * parParam and args lists remain empty.  (It could contain references
         * to earlier CTEs' output param IDs, but CTE outputs are not
         * propagated via the args list.)
         */

        /*
         * Assign a param ID to represent the CTE's output.  No ordinary
         * "evaluation" of this param slot ever happens, but we use the param
         * ID for setParam/chgParam signaling just as if the CTE plan were
         * returning a simple scalar output.  (Also, the executor abuses the
         * ParamExecData slot for this param ID for communication among
         * multiple CteScan nodes that might be scanning this CTE.)
         */
        let paramid = assign_special_exec_param(root);
        (*splan).setParam = list_make1_int!(paramid);

        /*
         * Add the subplan, its path, and its PlannerInfo to the global lists.
         */
        (*(*root).glob).subplans = lappend((*(*root).glob).subplans, plan as *mut c_void);
        (*(*root).glob).subpaths =
            lappend((*(*root).glob).subpaths, best_path as *mut c_void);
        (*(*root).glob).subroots = lappend((*(*root).glob).subroots, subroot as *mut c_void);
        (*splan).plan_id = list_length((*(*root).glob).subplans);

        (*root).init_plans = lappend((*root).init_plans, splan as *mut c_void);

        (*root).cte_plan_ids = lappend_int((*root).cte_plan_ids, (*splan).plan_id);

        /* Label the subplan for EXPLAIN purposes */
        (*splan).plan_name = psprintf(b"CTE %s\0".as_ptr() as *const c_char);

        /* Lastly, fill in the cost estimates for use later */
        cost_subplan(root, splan, plan);
    });
}

/*
 * contain_dml: is any subquery not a plain SELECT?
 *
 * We reject SELECT FOR UPDATE/SHARE as well as INSERT etc.
 */
unsafe fn contain_dml(node: *mut Node) -> bool {
    contain_dml_walker(node, ptr::null_mut())
}

unsafe fn contain_dml_walker(node: *mut Node, context: *mut c_void) -> bool {
    if node.is_null() {
        return false;
    }
    if IsA!(node, T_Query) {
        let query: *mut Query = node as *mut Query;

        if (*query).commandType != CMD_SELECT || !(*query).rowMarks.is_null() && list_length((*query).rowMarks) > 0 {
            return true;
        }

        return query_tree_walker(query, contain_dml_walker_trampoline, context, 0);
    }
    unsafe fn contain_dml_walker_trampoline(node: *mut Node, ctx: *mut c_void) -> bool {
        contain_dml_walker(node, ctx)
    }
    expression_tree_walker(node, contain_dml_walker_trampoline, context)
}

/*
 * contain_outer_selfref: is there an external recursive self-reference?
 */
unsafe fn contain_outer_selfref(node: *mut Node) -> bool {
    let mut depth: Index = 0;

    /*
     * We should be starting with a Query, so that depth will be 1 while
     * examining its immediate contents.
     */
    Assert!(IsA!(node, T_Query));

    contain_outer_selfref_walker(node, &mut depth)
}

unsafe fn contain_outer_selfref_walker(node: *mut Node, depth: *mut Index) -> bool {
    if node.is_null() {
        return false;
    }
    if IsA!(node, T_RangeTblEntry) {
        let rte: *mut RangeTblEntry = node as *mut RangeTblEntry;

        /*
         * Check for a self-reference to a CTE that's above the Query that our
         * search started at.
         */
        if (*rte).rtekind == RTE_CTE && (*rte).self_reference && (*rte).ctelevelsup >= *depth {
            return true;
        }
        return false; /* allow range_table_walker to continue */
    }
    if IsA!(node, T_Query) {
        /* Recurse into subquery, tracking nesting depth properly */
        let query: *mut Query = node as *mut Query;
        let result: bool;

        *depth += 1;

        unsafe fn walker_trampoline(node: *mut Node, ctx: *mut c_void) -> bool {
            contain_outer_selfref_walker(node, ctx as *mut Index)
        }
        result = query_tree_walker(
            query,
            walker_trampoline,
            depth as *mut c_void,
            QTW_EXAMINE_RTES_BEFORE,
        );

        *depth -= 1;

        return result;
    }
    unsafe fn expr_walker_trampoline(node: *mut Node, ctx: *mut c_void) -> bool {
        contain_outer_selfref_walker(node, ctx as *mut Index)
    }
    expression_tree_walker(node, expr_walker_trampoline, depth as *mut c_void)
}

/*
 * inline_cte: convert RTE_CTE references to given CTE into RTE_SUBQUERYs
 */
unsafe fn inline_cte(root: *mut PlannerInfo, cte: *mut CommonTableExpr) {
    let mut context = inline_cte_walker_context {
        ctename: (*cte).ctename,
        /* Start at levelsup = -1 because we'll immediately increment it */
        levelsup: -1,
        ctequery: castNode!(Query, T_Query, (*cte).ctequery as *mut c_void) as *mut Query,
    };

    let _ = inline_cte_walker((*root).parse as *mut Node, &mut context);
}

unsafe fn inline_cte_walker(
    node: *mut Node,
    context: *mut inline_cte_walker_context,
) -> bool {
    if node.is_null() {
        return false;
    }
    if IsA!(node, T_Query) {
        let query: *mut Query = node as *mut Query;

        (*context).levelsup += 1;

        /*
         * Visit the query's RTE nodes after their contents; otherwise
         * query_tree_walker would descend into the newly inlined CTE query,
         * which we don't want.
         */
        unsafe fn walker_trampoline(node: *mut Node, ctx: *mut c_void) -> bool {
            inline_cte_walker(node, ctx as *mut inline_cte_walker_context)
        }
        let _ = query_tree_walker(
            query,
            walker_trampoline,
            context as *mut c_void,
            QTW_EXAMINE_RTES_AFTER,
        );

        (*context).levelsup -= 1;

        return false;
    } else if IsA!(node, T_RangeTblEntry) {
        let rte: *mut RangeTblEntry = node as *mut RangeTblEntry;

        if (*rte).rtekind == RTE_CTE
            && libc_strcmp((*rte).ctename, (*context).ctename) == 0
            && (*rte).ctelevelsup == (*context).levelsup as Index
        {
            /*
             * Found a reference to replace.  Generate a copy of the CTE query
             * with appropriate level adjustment for outer references (e.g.,
             * to other CTEs).
             */
            let newquery: *mut Query =
                copyObject((*context).ctequery as *mut c_void) as *mut Query;

            if (*context).levelsup > 0 {
                IncrementVarSublevelsUp(
                    newquery as *mut Node,
                    (*context).levelsup,
                    1,
                );
            }

            /*
             * Convert the RTE_CTE RTE into a RTE_SUBQUERY.
             *
             * Historically, a FOR UPDATE clause has been treated as extending
             * into views and subqueries, but not into CTEs.  We preserve this
             * distinction by not trying to push rowmarks into the new
             * subquery.
             */
            (*rte).rtekind = RTE_SUBQUERY;
            (*rte).subquery = newquery;
            (*rte).security_barrier = false;

            /* Zero out CTE-specific fields */
            (*rte).ctename = ptr::null_mut();
            (*rte).ctelevelsup = 0;
            (*rte).self_reference = false;
            (*rte).coltypes = NIL;
            (*rte).coltypmods = NIL;
            (*rte).colcollations = NIL;
        }

        return false;
    }

    unsafe fn expr_walker_trampoline(node: *mut Node, ctx: *mut c_void) -> bool {
        inline_cte_walker(node, ctx as *mut inline_cte_walker_context)
    }
    expression_tree_walker(node, expr_walker_trampoline, context as *mut c_void)
}

/// strcmp shim (avoids pulling in libc)
unsafe fn libc_strcmp(a: *const c_char, b: *const c_char) -> c_int {
    let sa = std::ffi::CStr::from_ptr(a);
    let sb = std::ffi::CStr::from_ptr(b);
    match sa.cmp(sb) {
        std::cmp::Ordering::Less => -1,
        std::cmp::Ordering::Equal => 0,
        std::cmp::Ordering::Greater => 1,
    }
}

/*
 * Attempt to transform 'testexpr' over the VALUES subquery into
 * a ScalarArrayOpExpr.  We currently support the transformation only when
 * it ends up with a constant array.  Otherwise, the evaluation of non-hashed
 * SAOP might be slower than the corresponding Hash Join with VALUES.
 *
 * Return transformed ScalarArrayOpExpr or NULL if transformation isn't
 * allowed.
 */
pub unsafe fn convert_VALUES_to_ANY(
    root: *mut PlannerInfo,
    testexpr: *mut Node,
    values: *mut Query,
) -> *mut ScalarArrayOpExpr {
    let rte: *mut RangeTblEntry;
    let leftop: *mut Node;
    let rightop: *mut Node;
    let opno: Oid;
    let mut lc: *mut ListCell;
    let inputcollid: Oid;
    let mut exprs: *mut List = NIL;

    /*
     * Check we have a binary operator over a single-column subquery with no
     * joins and no LIMIT/OFFSET/ORDER BY clauses.
     */
    if !IsA!(testexpr, T_OpExpr)
        || list_length((*(testexpr as *mut OpExpr)).args) != 2
        || list_length((*values).targetList) > 1
        || !(*values).limitCount.is_null()
        || !(*values).limitOffset.is_null()
        || !(*values).sortClause.is_null() && list_length((*values).sortClause) > 0
        || list_length((*values).rtable) != 1
    {
        return ptr::null_mut();
    }

    rte = linitial((*values).rtable) as *mut RangeTblEntry;
    leftop = linitial((*(testexpr as *mut OpExpr)).args) as *mut Node;
    rightop = lsecond((*(testexpr as *mut OpExpr)).args) as *mut Node;
    opno = (*(testexpr as *mut OpExpr)).opno;
    inputcollid = (*(testexpr as *mut OpExpr)).inputcollid;

    /*
     * Also, check that only RTE corresponds to VALUES; the list of values has
     * at least two items and no volatile functions.
     */
    if (*rte).rtekind != RTE_VALUES
        || list_length((*rte).values_lists) < 2
        || contain_volatile_functions((*rte).values_lists as *mut Node)
    {
        return ptr::null_mut();
    }

    foreach!(lc, (*rte).values_lists, {
        let elem: *mut List = lfirst(crate::current_cell!(lc)) as *mut List;
        let mut value: *mut Node = linitial(elem) as *mut Node;

        /*
         * Prepare an evaluation of the right side of the operator with
         * substitution of the given value.
         */
        value = convert_testexpr(root, rightop, list_make1!(value as *mut c_void));

        /*
         * Try to evaluate constant expressions.  We could get Const as a
         * result.
         */
        value = eval_const_expressions(root, value);

        /*
         * As we only support constant output arrays, all the items must also
         * be constant.
         */
        if !IsA!(value, T_Const) {
            return ptr::null_mut();
        }

        exprs = lappend(exprs, value as *mut c_void);
    });

    /* Finally, build ScalarArrayOpExpr at the top of the 'exprs' list. */
    make_SAOP_expr(
        opno,
        leftop,
        exprType(rightop),
        linitial_oid((*rte).colcollations),
        inputcollid,
        exprs,
        false,
    )
}

/*
 * convert_ANY_sublink_to_join: try to convert an ANY SubLink to a join
 *
 * The caller has found an ANY SubLink at the top level of one of the query's
 * qual clauses, but has not checked the properties of the SubLink further.
 * Decide whether it is appropriate to process this SubLink in join style.
 * If so, form a JoinExpr and return it.  Return NULL if the SubLink cannot
 * be converted to a join.
 *
 * The only non-obvious input parameter is available_rels: this is the set
 * of query rels that can safely be referenced in the sublink expression.
 * (We must restrict this to avoid changing the semantics when a sublink
 * is present in an outer join's ON qual.)  The conversion must fail if
 * the converted qual would reference any but these parent-query relids.
 *
 * On success, the returned JoinExpr has larg = NULL and rarg = the jointree
 * item representing the pulled-up subquery.  The caller must set larg to
 * represent the relation(s) on the lefthand side of the new join, and insert
 * the JoinExpr into the upper query's jointree at an appropriate place
 * (typically, where the lefthand relation(s) had been).  Note that the
 * passed-in SubLink must also be removed from its original position in the
 * query quals, since the quals of the returned JoinExpr replace it.
 * (Notionally, we replace the SubLink with a constant TRUE, then elide the
 * redundant constant from the qual.)
 *
 * On success, the caller is also responsible for recursively applying
 * pull_up_sublinks processing to the rarg and quals of the returned JoinExpr.
 * (On failure, there is no need to do anything, since pull_up_sublinks will
 * be applied when we recursively plan the sub-select.)
 *
 * Side effects of a successful conversion include adding the SubLink's
 * subselect to the query's rangetable, so that it can be referenced in
 * the JoinExpr's rarg.
 */
pub unsafe fn convert_ANY_sublink_to_join(
    root: *mut PlannerInfo,
    sublink: *mut SubLink,
    available_rels: *mut Bitmapset,
) -> *mut JoinExpr {
    let result: *mut JoinExpr;
    let parse: *mut Query = (*root).parse;
    let subselect: *mut Query = (*sublink).subselect as *mut Query;
    let upper_varnos: *mut Bitmapset;
    let rtindex: c_int;
    let nsitem: *mut c_void;
    let rte: *mut RangeTblEntry;
    let rtr: *mut RangeTblRefLocal;
    let subquery_vars: *mut List;
    let quals: *mut Node;
    let pstate: *mut c_void;
    let sub_ref_outer_relids: *mut Bitmapset;
    let use_lateral: bool;

    Assert!((*sublink).subLinkType == ANY_SUBLINK);

    /*
     * If the sub-select contains any Vars of the parent query, we treat it as
     * LATERAL.  (Vars from higher levels don't matter here.)
     */
    sub_ref_outer_relids = pull_varnos_of_level(ptr::null_mut(), subselect as *mut Node, 1);
    use_lateral = !bms_is_empty(sub_ref_outer_relids);

    /*
     * Can't convert if the sub-select contains parent-level Vars of relations
     * not in available_rels.
     */
    if !bms_is_subset(sub_ref_outer_relids, available_rels) {
        return ptr::null_mut();
    }

    /*
     * The test expression must contain some Vars of the parent query, else
     * it's not gonna be a join.  (Note that it won't have Vars referring to
     * the subquery, rather Params.)
     */
    let upper_varnos = pull_varnos(root, (*sublink).testexpr);
    if bms_is_empty(upper_varnos) {
        return ptr::null_mut();
    }

    /*
     * However, it can't refer to anything outside available_rels.
     */
    if !bms_is_subset(upper_varnos, available_rels) {
        return ptr::null_mut();
    }

    /*
     * The combining operators and left-hand expressions mustn't be volatile.
     */
    if contain_volatile_functions((*sublink).testexpr) {
        return ptr::null_mut();
    }

    /* Create a dummy ParseState for addRangeTableEntryForSubquery */
    pstate = make_parsestate(ptr::null_mut());

    /*
     * Okay, pull up the sub-select into upper range table.
     *
     * We rely here on the assumption that the outer query has no references
     * to the inner (necessarily true, other than the Vars that we build
     * below). Therefore this is a lot easier than what pull_up_subqueries has
     * to go through.
     */
    nsitem = addRangeTableEntryForSubquery(
        pstate,
        subselect,
        makeAlias(b"ANY_subquery\0".as_ptr() as *const c_char, NIL),
        use_lateral,
        false,
    );
    /* nsitem->p_rte */
    rte = (*(nsitem as *mut ParseNamespaceItemStub)).p_rte;
    (*parse).rtable = lappend((*parse).rtable, rte as *mut c_void);
    rtindex = list_length((*parse).rtable);

    /*
     * Form a RangeTblRef for the pulled-up sub-select.
     */
    rtr = makeNode!(RangeTblRef, T_RangeTblRef) as *mut RangeTblRefLocal;
    (*rtr).rtindex = rtindex;

    /*
     * Build a list of Vars representing the subselect outputs.
     */
    subquery_vars = generate_subquery_vars(root, (*subselect).targetList, rtindex as Index);

    /*
     * Build the new join's qual expression, replacing Params with these Vars.
     */
    quals = convert_testexpr(root, (*sublink).testexpr, subquery_vars);

    /*
     * And finally, build the JoinExpr node.
     */
    let result: *mut JoinExpr = makeNode!(JoinExpr, T_JoinExpr) as *mut JoinExpr;
    (*result).jointype = JOIN_SEMI as c_int;
    (*result).isNatural = false;
    (*result).larg = ptr::null_mut(); /* caller must fill this in */
    (*result).rarg = rtr as *mut Node;
    (*result).usingClause = NIL;
    (*result).join_using_alias = ptr::null_mut();
    (*result).quals = quals;
    (*result).alias = ptr::null_mut();
    (*result).rtindex = 0; /* we don't need an RTE for it */

    result
}

/// Stub for ParseNamespaceItem (opaque pointer layout); only p_rte needed.
#[repr(C)]
struct ParseNamespaceItemStub {
    p_rte: *mut RangeTblEntry,
    /* other fields omitted */
}

/*
 * convert_EXISTS_sublink_to_join: try to convert an EXISTS SubLink to a join
 *
 * The API of this function is identical to convert_ANY_sublink_to_join's,
 * except that we also support the case where the caller has found NOT EXISTS,
 * so we need an additional input parameter "under_not".
 */
pub unsafe fn convert_EXISTS_sublink_to_join(
    root: *mut PlannerInfo,
    sublink: *mut SubLink,
    under_not: bool,
    available_rels: *mut Bitmapset,
) -> *mut JoinExpr {
    let parse: *mut Query = (*root).parse;
    let mut subselect: *mut Query = (*sublink).subselect as *mut Query;
    let whereClause: *mut Node;
    let rtoffset: c_int;
    let mut varno: c_int;
    let clause_varnos: *mut Bitmapset;
    let mut upper_varnos: *mut Bitmapset;

    Assert!((*sublink).subLinkType == EXISTS_SUBLINK);

    /*
     * Can't flatten if it contains WITH.  (We could arrange to pull up the
     * WITH into the parent query's cteList, but that risks changing the
     * semantics, since a WITH ought to be executed once per associated query
     * call.)  Note that convert_ANY_sublink_to_join doesn't have to reject
     * this case, since it just produces a subquery RTE that doesn't have to
     * get flattened into the parent query.
     */
    if !(*subselect).cteList.is_null() && list_length((*subselect).cteList) > 0 {
        return ptr::null_mut();
    }

    /*
     * Copy the subquery so we can modify it safely (see comments in
     * make_subplan).
     */
    subselect = copyObject(subselect as *mut c_void) as *mut Query;

    /*
     * See if the subquery can be simplified based on the knowledge that it's
     * being used in EXISTS().  If we aren't able to get rid of its
     * targetlist, we have to fail, because the pullup operation leaves us
     * with noplace to evaluate the targetlist.
     */
    if !simplify_EXISTS_query(root, subselect) {
        return ptr::null_mut();
    }

    /*
     * Separate out the WHERE clause.  (We could theoretically also remove
     * top-level plain JOIN/ON clauses, but it's probably not worth the
     * trouble.)
     */
    let whereClause = (*(*subselect).jointree).quals;
    (*(*subselect).jointree).quals = ptr::null_mut();

    /*
     * The rest of the sub-select must not refer to any Vars of the parent
     * query.  (Vars of higher levels should be okay, though.)
     */
    if contain_vars_of_level(subselect as *mut Node, 1) {
        return ptr::null_mut();
    }

    /*
     * On the other hand, the WHERE clause must contain some Vars of the
     * parent query, else it's not gonna be a join.
     */
    if !contain_vars_of_level(whereClause, 1) {
        return ptr::null_mut();
    }

    /*
     * We don't risk optimizing if the WHERE clause is volatile, either.
     */
    if contain_volatile_functions(whereClause) {
        return ptr::null_mut();
    }

    /*
     * The subquery must have a nonempty jointree, but we can make it so.
     */
    replace_empty_jointree(subselect);

    /*
     * Prepare to pull up the sub-select into top range table.
     *
     * We rely here on the assumption that the outer query has no references
     * to the inner (necessarily true). Therefore this is a lot easier than
     * what pull_up_subqueries has to go through.
     *
     * In fact, it's even easier than what convert_ANY_sublink_to_join has to
     * do.  The machinations of simplify_EXISTS_query ensured that there is
     * nothing interesting in the subquery except an rtable and jointree, and
     * even the jointree FromExpr no longer has quals.  So we can just append
     * the rtable to our own and use the FromExpr in our jointree. But first,
     * adjust all level-zero varnos in the subquery to account for the rtable
     * merger.
     */
    rtoffset = list_length((*parse).rtable);
    OffsetVarNodes(subselect as *mut Node, rtoffset, 0);
    OffsetVarNodes(whereClause, rtoffset, 0);

    /*
     * Upper-level vars in subquery will now be one level closer to their
     * parent than before; in particular, anything that had been level 1
     * becomes level zero.
     */
    IncrementVarSublevelsUp(subselect as *mut Node, -1, 1);
    IncrementVarSublevelsUp(whereClause, -1, 1);

    /*
     * Now that the WHERE clause is adjusted to match the parent query
     * environment, we can easily identify all the level-zero rels it uses.
     * The ones <= rtoffset belong to the upper query; the ones > rtoffset do
     * not.
     */
    let clause_varnos = pull_varnos(root, whereClause);
    upper_varnos = ptr::null_mut();
    varno = -1;
    loop {
        varno = bms_next_member(clause_varnos, varno);
        if varno < 0 { break; }
        if varno <= rtoffset {
            upper_varnos = bms_add_member(upper_varnos, varno);
        }
    }
    bms_free(clause_varnos);
    Assert!(!bms_is_empty(upper_varnos));

    /*
     * Now that we've got the set of upper-level varnos, we can make the last
     * check: only available_rels can be referenced.
     */
    if !bms_is_subset(upper_varnos, available_rels) {
        return ptr::null_mut();
    }

    /*
     * Now we can attach the modified subquery rtable to the parent. This also
     * adds subquery's RTEPermissionInfos into the upper query.
     */
    CombineRangeTables(
        &mut (*parse).rtable,
        &mut (*parse).rteperminfos,
        (*subselect).rtable,
        (*subselect).rteperminfos,
    );

    /*
     * And finally, build the JoinExpr node.
     */
    let result: *mut JoinExpr = makeNode!(JoinExpr, T_JoinExpr) as *mut JoinExpr;
    (*result).jointype = if under_not { JOIN_ANTI as c_int } else { JOIN_SEMI as c_int };
    (*result).isNatural = false;
    (*result).larg = ptr::null_mut(); /* caller must fill this in */
    /* flatten out the FromExpr node if it's useless */
    if list_length((*(*subselect).jointree).fromlist) == 1 {
        (*result).rarg = linitial((*(*subselect).jointree).fromlist) as *mut Node;
    } else {
        (*result).rarg = (*subselect).jointree as *mut Node;
    }
    (*result).usingClause = NIL;
    (*result).join_using_alias = ptr::null_mut();
    (*result).quals = whereClause;
    (*result).alias = ptr::null_mut();
    (*result).rtindex = 0; /* we don't need an RTE for it */

    result
}

/*
 * simplify_EXISTS_query: remove any useless stuff in an EXISTS's subquery
 *
 * The only thing that matters about an EXISTS query is whether it returns
 * zero or more than zero rows.  Therefore, we can remove certain SQL features
 * that won't affect that.  The only part that is really likely to matter in
 * typical usage is simplifying the targetlist: it's a common habit to write
 * "SELECT * FROM" even though there is no need to evaluate any columns.
 *
 * Note: by suppressing the targetlist we could cause an observable behavioral
 * change, namely that any errors that might occur in evaluating the tlist
 * won't occur, nor will other side-effects of volatile functions.  This seems
 * unlikely to bother anyone in practice.
 *
 * Returns true if was able to discard the targetlist, else false.
 */
unsafe fn simplify_EXISTS_query(root: *mut PlannerInfo, query: *mut Query) -> bool {
    /*
     * We don't try to simplify at all if the query uses set operations,
     * aggregates, grouping sets, SRFs, modifying CTEs, HAVING, OFFSET, or FOR
     * UPDATE/SHARE; none of these seem likely in normal usage and their
     * possible effects are complex.  (Note: we could ignore an "OFFSET 0"
     * clause, but that traditionally is used as an optimization fence, so we
     * don't.)
     */
    if (*query).commandType != CMD_SELECT
        || !(*query).setOperations.is_null()
        || (*query).hasAggs
        || !(*query).groupingSets.is_null() && list_length((*query).groupingSets) > 0
        || (*query).hasWindowFuncs
        || (*query).hasTargetSRFs
        || (*query).hasModifyingCTE
        || !(*query).havingQual.is_null()
        || !(*query).limitOffset.is_null()
        || !(*query).rowMarks.is_null() && list_length((*query).rowMarks) > 0
    {
        return false;
    }

    /*
     * LIMIT with a constant positive (or NULL) value doesn't affect the
     * semantics of EXISTS, so let's ignore such clauses.  This is worth doing
     * because people accustomed to certain other DBMSes may be in the habit
     * of writing EXISTS(SELECT ... LIMIT 1) as an optimization.  If there's a
     * LIMIT with anything else as argument, though, we can't simplify.
     */
    if !(*query).limitCount.is_null() {
        /*
         * The LIMIT clause has not yet been through eval_const_expressions,
         * so we have to apply that here.  It might seem like this is a waste
         * of cycles, since the only case plausibly worth worrying about is
         * "LIMIT 1" ... but what we'll actually see is "LIMIT int8(1::int4)",
         * so we have to fold constants or we're not going to recognize it.
         */
        let mut node = eval_const_expressions(root, (*query).limitCount);
        let limit: *mut Const;

        /* Might as well update the query if we simplified the clause. */
        (*query).limitCount = node;

        if !IsA!(node, T_Const) {
            return false;
        }

        limit = node as *mut Const;
        Assert!((*limit).consttype == INT8OID);
        if !(*limit).constisnull && DatumGetInt64((*limit).constvalue as u64) <= 0 {
            return false;
        }

        /* Whether or not the targetlist is safe, we can drop the LIMIT. */
        (*query).limitCount = ptr::null_mut();
    }

    /*
     * Otherwise, we can throw away the targetlist, as well as any GROUP,
     * WINDOW, DISTINCT, and ORDER BY clauses; none of those clauses will
     * change a nonzero-rows result to zero rows or vice versa.  (Furthermore,
     * since our parsetree representation of these clauses depends on the
     * targetlist, we'd better throw them away if we drop the targetlist.)
     */
    (*query).targetList = NIL;
    (*query).groupClause = NIL;
    (*query).windowClause = NIL;
    (*query).distinctClause = NIL;
    (*query).sortClause = NIL;
    (*query).hasDistinctOn = false;

    /*
     * Since we have thrown away the GROUP BY clauses, we'd better get rid of
     * the RTE_GROUP RTE and clear the hasGroupRTE flag.  To safely get rid of
     * the RTE_GROUP RTE without shifting the index of any subsequent RTE in
     * the rtable, we convert the RTE to be RTE_RESULT type in-place, and zero
     * out RTE_GROUP-specific fields.
     */
    if (*query).hasGroupRTE {
        let mut lc: *mut ListCell;
        foreach!(lc, (*query).rtable, {
            let rte: *mut RangeTblEntry =
                lfirst(crate::current_cell!(lc)) as *mut RangeTblEntry;
            if (*rte).rtekind == RTE_GROUP {
                (*rte).rtekind = RTE_RESULT;
                (*rte).groupexprs = NIL;

                /* A query should only have one RTE_GROUP, so we can stop. */
                break;
            }
        });

        (*query).hasGroupRTE = false;
    }

    true
}

/*
 * convert_EXISTS_to_ANY: try to convert EXISTS to an ANY query
 *
 * The subquery is expected to have been simplified already by
 * simplify_EXISTS_query.
 *
 * On success, the modified subquery is returned, and we store a suitable
 * testexpr in *testexpr and a list of the new Param IDs in *paramIds.
 * On failure, return NULL.
 */
unsafe fn convert_EXISTS_to_ANY(
    root: *mut PlannerInfo,
    subselect: *mut Query,
    testexpr: *mut *mut Node,
    paramIds: *mut *mut List,
) -> *mut Query {
    let mut whereClause: *mut Node;
    let mut leftargs: *mut List;
    let mut rightargs: *mut List;
    let mut opids: *mut List;
    let mut opcollations: *mut List;
    let mut newWhere: *mut List;
    let mut tlist: *mut List;
    let mut testlist: *mut List;
    let mut paramids: *mut List;
    let mut lc: *mut ListCell;
    let mut rc: *mut ListCell;
    let mut oc: *mut ListCell;
    let mut cc: *mut ListCell;
    let mut resno: AttrNumber;

    /*
     * Query must not require a targetlist, since we have to insert a new one.
     * Caller should have dealt with the case already.
     */
    Assert!((*subselect).targetList.is_null() || list_length((*subselect).targetList) == 0);

    /*
     * Separate out the WHERE clause.  (We could theoretically also remove
     * top-level plain JOIN/ON clauses, but it's probably not worth the
     * trouble.)
     */
    whereClause = (*(*subselect).jointree).quals;
    (*(*subselect).jointree).quals = ptr::null_mut();

    /*
     * The rest of the sub-select must not refer to any Vars of the parent
     * query.  (Vars of higher levels should be okay, though.)
     *
     * Note: we need not check for Aggrefs separately because we know the
     * sub-select is as yet unoptimized; any uplevel Aggref must therefore
     * contain an uplevel Var reference.  This is not the case below ...
     */
    if contain_vars_of_level(subselect as *mut Node, 1) {
        return ptr::null_mut();
    }

    /*
     * We don't risk optimizing if the WHERE clause is volatile, either.
     */
    if contain_volatile_functions(whereClause) {
        return ptr::null_mut();
    }

    /*
     * Clean up the WHERE clause by doing const-simplification etc on it.
     * Aside from simplifying the processing we're about to do, this is
     * important for being able to pull chunks of the WHERE clause up into the
     * parent query.  Since we are invoked partway through the parent's
     * preprocess_expression() work, earlier steps of preprocess_expression()
     * wouldn't get applied to the pulled-up stuff unless we do them here. For
     * the parts of the WHERE clause that get put back into the child query,
     * this work is partially duplicative, but it shouldn't hurt.
     *
     * Note: we do not run flatten_join_alias_vars.  This is OK because any
     * parent aliases were flattened already, and we're not going to pull any
     * child Vars (of any description) into the parent.
     *
     * Note: passing the parent's root to eval_const_expressions is
     * technically wrong, but we can get away with it since only the
     * boundParams (if any) are used, and those would be the same in a
     * subroot.
     */
    whereClause = eval_const_expressions(root, whereClause);
    whereClause = canonicalize_qual(whereClause as *mut Expr, false) as *mut Node;
    whereClause = make_ands_implicit(whereClause as *mut Expr) as *mut Node;

    /*
     * We now have a flattened implicit-AND list of clauses, which we try to
     * break apart into "outervar = innervar" hash clauses. Anything that
     * can't be broken apart just goes back into the newWhere list.  Note that
     * we aren't trying hard yet to ensure that we have only outer or only
     * inner on each side; we'll check that if we get to the end.
     */
    leftargs = NIL;
    rightargs = NIL;
    opids = NIL;
    opcollations = NIL;
    newWhere = NIL;
    foreach!(lc, whereClause as *mut List, {
        let expr: *mut OpExpr = lfirst(crate::current_cell!(lc)) as *mut OpExpr;

        if IsA!(expr as *mut Node, T_OpExpr) && hash_ok_operator(expr) {
            let leftarg: *mut Node = linitial((*expr).args) as *mut Node;
            let rightarg: *mut Node = lsecond((*expr).args) as *mut Node;

            if contain_vars_of_level(leftarg, 1) {
                leftargs = lappend(leftargs, leftarg as *mut _);
                rightargs = lappend(rightargs, rightarg as *mut _);
                opids = lappend_oid(opids, (*expr).opno);
                opcollations = lappend_oid(opcollations, (*expr).inputcollid);
                continue;
            }
            if contain_vars_of_level(rightarg, 1) {
                /*
                 * We must commute the clause to put the outer var on the
                 * left, because the hashing code in nodeSubplan.c expects
                 * that.  This probably shouldn't ever fail, since hashable
                 * operators ought to have commutators, but be paranoid.
                 */
                (*expr).opno = get_commutator((*expr).opno);
                if OidIsValid((*expr).opno) && hash_ok_operator(expr) {
                    leftargs = lappend(leftargs, rightarg as *mut _);
                    rightargs = lappend(rightargs, leftarg as *mut _);
                    opids = lappend_oid(opids, (*expr).opno);
                    opcollations = lappend_oid(opcollations, (*expr).inputcollid);
                    continue;
                }
                /* If no commutator, no chance to optimize the WHERE clause */
                return ptr::null_mut();
            }
        }
        /* Couldn't handle it as a hash clause */
        newWhere = lappend(newWhere, expr as *mut _);
    });

    /*
     * If we didn't find anything we could convert, fail.
     */
    if list_length(leftargs) == 0 {
        return ptr::null_mut();
    }

    /*
     * There mustn't be any parent Vars or Aggs in the stuff that we intend to
     * put back into the child query.  Note: you might think we don't need to
     * check for Aggs separately, because an uplevel Agg must contain an
     * uplevel Var in its argument.  But it is possible that the uplevel Var
     * got optimized away by eval_const_expressions.  Consider
     *
     * SUM(CASE WHEN false THEN uplevelvar ELSE 0 END)
     */
    if contain_vars_of_level(newWhere as *mut Node, 1)
        || contain_vars_of_level(rightargs as *mut Node, 1)
    {
        return ptr::null_mut();
    }
    if (*(*root).parse).hasAggs
        && (contain_aggs_of_level(newWhere as *mut Node, 1)
            || contain_aggs_of_level(rightargs as *mut Node, 1))
    {
        return ptr::null_mut();
    }

    /*
     * And there can't be any child Vars in the stuff we intend to pull up.
     * (Note: we'd need to check for child Aggs too, except we know the child
     * has no aggs at all because of simplify_EXISTS_query's check. The same
     * goes for window functions.)
     */
    if contain_vars_of_level(leftargs as *mut Node, 0) {
        return ptr::null_mut();
    }

    /*
     * Also reject sublinks in the stuff we intend to pull up.  (It might be
     * possible to support this, but doesn't seem worth the complication.)
     */
    if contain_subplans(leftargs as *mut Node) {
        return ptr::null_mut();
    }

    /*
     * Okay, adjust the sublevelsup in the stuff we're pulling up.
     */
    IncrementVarSublevelsUp(leftargs as *mut Node, -1, 1);

    /*
     * Put back any child-level-only WHERE clauses.
     */
    if !newWhere.is_null() && list_length(newWhere) > 0 {
        (*(*subselect).jointree).quals = make_ands_explicit(newWhere) as *mut Node;
    }

    /*
     * Build a new targetlist for the child that emits the expressions we
     * need.  Concurrently, build a testexpr for the parent using Params to
     * reference the child outputs.  (Since we generate Params directly here,
     * there will be no need to convert the testexpr in build_subplan.)
     */
    tlist = NIL;
    testlist = NIL;
    paramids = NIL;
    resno = 1;
    forfour!(lc, leftargs, rc, rightargs, oc, opids, cc, opcollations, {
        let leftarg: *mut Node = lfirst(lc) as *mut Node;
        let rightarg: *mut Node = lfirst(rc) as *mut Node;
        let opid: Oid = lfirst_oid(oc);
        let opcollation: Oid = lfirst_oid(cc);
        let param: *mut Param;

        param = generate_new_exec_param(
            root,
            exprType(rightarg),
            exprTypmod(rightarg),
            exprCollation(rightarg),
        );
        tlist = lappend(
            tlist,
            makeTargetEntry(rightarg as *mut Expr, resno, ptr::null_mut(), false) as *mut _,
        );
        resno += 1;
        testlist = lappend(
            testlist,
            make_opclause(
                opid,
                BOOLOID,
                false,
                leftarg as *mut Expr,
                param as *mut Expr,
                InvalidOid,
                opcollation,
            ) as *mut _,
        );
        paramids = lappend_int(paramids, (*param).paramid);
    });

    /* Put everything where it should go, and we're done */
    (*subselect).targetList = tlist;
    *testexpr = make_ands_explicit(testlist) as *mut Node;
    *paramIds = paramids;

    subselect
}

/*
 * Replace correlation vars (uplevel vars) with Params.
 *
 * Uplevel PlaceHolderVars, aggregates, GROUPING() expressions,
 * MergeSupportFuncs, and ReturningExprs are replaced, too.
 *
 * Note: it is critical that this runs immediately after SS_process_sublinks.
 * Since we do not recurse into the arguments of uplevel PHVs and aggregates,
 * they will get copied to the appropriate subplan args list in the parent
 * query with uplevel vars not replaced by Params, but only adjusted in level
 * (see replace_outer_placeholdervar and replace_outer_agg).  That's exactly
 * what we want for the vars of the parent level --- but if a PHV's or
 * aggregate's argument contains any further-up variables, they have to be
 * replaced with Params in their turn. That will happen when the parent level
 * runs SS_replace_correlation_vars.  Therefore it must do so after expanding
 * its sublinks to subplans.  And we don't want any steps in between, else
 * those steps would never get applied to the argument expressions, either in
 * the parent or the child level.
 *
 * Another fairly tricky thing going on here is the handling of SubLinks in
 * the arguments of uplevel PHVs/aggregates.  Those are not touched inside the
 * intermediate query level, either.  Instead, SS_process_sublinks recurses on
 * them after copying the PHV or Aggref expression into the parent plan level
 * (this is actually taken care of in build_subplan).
 */
pub unsafe fn SS_replace_correlation_vars(root: *mut PlannerInfo, expr: *mut Node) -> *mut Node {
    /* No setup needed for tree walk, so away we go */
    replace_correlation_vars_mutator(expr, root)
}

unsafe fn replace_correlation_vars_mutator(node: *mut Node, root: *mut PlannerInfo) -> *mut Node {
    if node.is_null() {
        return ptr::null_mut();
    }
    if IsA!(node, T_Var) {
        if (*(node as *mut Var)).varlevelsup > 0 {
            return replace_outer_var(root, node as *mut Var) as *mut Node;
        }
    }
    if IsA!(node, T_PlaceHolderVar) {
        if (*(node as *mut PlaceHolderVar)).phlevelsup > 0 {
            return replace_outer_placeholdervar(root, node as *mut PlaceHolderVar) as *mut Node;
        }
    }
    if IsA!(node, T_Aggref) {
        if (*(node as *mut Aggref)).agglevelsup > 0 {
            return replace_outer_agg(root, node as *mut Aggref) as *mut Node;
        }
    }
    if IsA!(node, T_GroupingFunc) {
        if (*(node as *mut GroupingFunc)).agglevelsup > 0 {
            return replace_outer_grouping(root, node as *mut GroupingFunc) as *mut Node;
        }
    }
    if IsA!(node, T_MergeSupportFunc) {
        if (*(*root).parse).commandType != CMD_MERGE {
            return replace_outer_merge_support(root, node as *mut MergeSupportFunc) as *mut Node;
        }
    }
    if IsA!(node, T_ReturningExpr) {
        if (*(node as *mut ReturningExpr)).retlevelsup > 0 {
            return replace_outer_returning(root, node as *mut ReturningExpr) as *mut Node;
        }
    }
    unsafe fn replace_correlation_vars_mutator_trampoline(
        n: *mut Node,
        ctx: *mut c_void,
    ) -> *mut Node {
        replace_correlation_vars_mutator(n, ctx as *mut PlannerInfo)
    }
    expression_tree_mutator(node, replace_correlation_vars_mutator_trampoline, root as *mut c_void)
}

/*
 * Expand SubLinks to SubPlans in the given expression.
 *
 * The isQual argument tells whether or not this expression is a WHERE/HAVING
 * qualifier expression.  If it is, any sublinks appearing at top level need
 * not distinguish FALSE from UNKNOWN return values.
 */
pub unsafe fn SS_process_sublinks(
    root: *mut PlannerInfo,
    expr: *mut Node,
    isQual: bool,
) -> *mut Node {
    let mut context = process_sublinks_context {
        root,
        isTopQual: isQual,
    };
    process_sublinks_mutator(expr, &mut context as *mut process_sublinks_context)
}

unsafe fn process_sublinks_mutator(
    node: *mut Node,
    context: *mut process_sublinks_context,
) -> *mut Node {
    let mut locContext = process_sublinks_context {
        root: (*context).root,
        isTopQual: false,
    };

    if node.is_null() {
        return ptr::null_mut();
    }
    if IsA!(node, T_SubLink) {
        let sublink: *mut SubLink = node as *mut SubLink;
        let testexpr: *mut Node;

        /*
         * First, recursively process the lefthand-side expressions, if any.
         * They're not top-level anymore.
         */
        locContext.isTopQual = false;
        testexpr = process_sublinks_mutator(
            (*sublink).testexpr,
            &mut locContext as *mut process_sublinks_context,
        );

        /*
         * Now build the SubPlan node and make the expr to return.
         */
        return make_subplan(
            (*context).root,
            (*sublink).subselect as *mut Query,
            (*sublink).subLinkType,
            (*sublink).subLinkId,
            testexpr,
            (*context).isTopQual,
        );
    }

    /*
     * Don't recurse into the arguments of an outer PHV, Aggref, GroupingFunc,
     * or ReturningExpr here.  Any SubLinks in the arguments have to be dealt
     * with at the outer query level; they'll be handled when build_subplan
     * collects the PHV, Aggref, GroupingFunc, or ReturningExpr into the
     * arguments to be passed down to the current subplan.
     */
    if IsA!(node, T_PlaceHolderVar) {
        if (*(node as *mut PlaceHolderVar)).phlevelsup > 0 {
            return node;
        }
    } else if IsA!(node, T_Aggref) {
        if (*(node as *mut Aggref)).agglevelsup > 0 {
            return node;
        }
    } else if IsA!(node, T_GroupingFunc) {
        if (*(node as *mut GroupingFunc)).agglevelsup > 0 {
            return node;
        }
    } else if IsA!(node, T_ReturningExpr) {
        if (*(node as *mut ReturningExpr)).retlevelsup > 0 {
            return node;
        }
    }

    /*
     * We should never see a SubPlan expression in the input (since this is
     * the very routine that creates 'em to begin with).  We shouldn't find
     * ourselves invoked directly on a Query, either.
     */
    Assert!(!IsA!(node, T_SubPlan));
    Assert!(!IsA!(node, T_AlternativeSubPlan));
    Assert!(!IsA!(node, T_Query));

    /*
     * Because make_subplan() could return an AND or OR clause, we have to
     * take steps to preserve AND/OR flatness of a qual.  We assume the input
     * has been AND/OR flattened and so we need no recursion here.
     *
     * (Due to the coding here, we will not get called on the List subnodes of
     * an AND; and the input is *not* yet in implicit-AND format.  So no check
     * is needed for a bare List.)
     *
     * Anywhere within the top-level AND/OR clause structure, we can tell
     * make_subplan() that NULL and FALSE are interchangeable.  So isTopQual
     * propagates down in both cases.  (Note that this is unlike the meaning
     * of "top level qual" used in most other places in Postgres.)
     */
    if is_andclause(node) {
        let mut newargs: *mut List = NIL;
        let mut l: *mut ListCell;

        /* Still at qual top-level */
        locContext.isTopQual = (*context).isTopQual;

        foreach!(l, (*(node as *mut BoolExpr)).args, {
            let newarg: *mut Node = process_sublinks_mutator(
                lfirst(crate::current_cell!(l)) as *mut Node,
                &mut locContext as *mut process_sublinks_context,
            );
            if is_andclause(newarg) {
                newargs = list_concat(newargs, (*(newarg as *mut BoolExpr)).args);
            } else {
                newargs = lappend(newargs, newarg as *mut _);
            }
        });
        return make_andclause(newargs) as *mut Node;
    }

    if is_orclause(node) {
        let mut newargs: *mut List = NIL;
        let mut l: *mut ListCell;

        /* Still at qual top-level */
        locContext.isTopQual = (*context).isTopQual;

        foreach!(l, (*(node as *mut BoolExpr)).args, {
            let newarg: *mut Node = process_sublinks_mutator(
                lfirst(crate::current_cell!(l)) as *mut Node,
                &mut locContext as *mut process_sublinks_context,
            );
            if is_orclause(newarg) {
                newargs = list_concat(newargs, (*(newarg as *mut BoolExpr)).args);
            } else {
                newargs = lappend(newargs, newarg as *mut _);
            }
        });
        return make_orclause(newargs) as *mut Node;
    }

    /*
     * If we recurse down through anything other than an AND or OR node, we
     * are definitely not at top qual level anymore.
     */
    locContext.isTopQual = false;

    unsafe fn process_sublinks_mutator_trampoline(n: *mut Node, ctx: *mut c_void) -> *mut Node {
        process_sublinks_mutator(n, ctx as *mut process_sublinks_context)
    }
    expression_tree_mutator(
        node,
        process_sublinks_mutator_trampoline,
        &mut locContext as *mut process_sublinks_context as *mut c_void,
    )
}

/*
 * SS_identify_outer_params - identify the Params available from outer levels
 *
 * This must be run after SS_replace_correlation_vars and SS_process_sublinks
 * processing is complete in a given query level as well as all of its
 * descendant levels (which means it's most practical to do it at the end of
 * processing the query level).  We compute the set of paramIds that outer
 * levels will make available to this level+descendants, and record it in
 * root->outer_params for use while computing extParam/allParam sets in final
 * plan cleanup.  (We can't just compute it then, because the upper levels'
 * plan_params lists are transient and will be gone by then.)
 */
pub unsafe fn SS_identify_outer_params(root: *mut PlannerInfo) {
    let mut outer_params: *mut Bitmapset;
    let mut proot: *mut PlannerInfo;
    let mut l: *mut ListCell;

    /*
     * If no parameters have been assigned anywhere in the tree, we certainly
     * don't need to do anything here.
     */
    if list_length((*(*root).glob).paramExecTypes) == 0 {
        return;
    }

    /*
     * Scan all query levels above this one to see which parameters are due to
     * be available from them, either because lower query levels have
     * requested them (via plan_params) or because they will be available from
     * initPlans of those levels.
     */
    outer_params = ptr::null_mut();
    proot = (*root).parent_root;
    while !proot.is_null() {
        /* Include ordinary Var/PHV/Aggref/GroupingFunc/ReturningExpr params. */
        foreach!(l, (*proot).plan_params, {
            let pitem: *mut PlannerParamItem =
                lfirst(crate::current_cell!(l)) as *mut PlannerParamItem;
            outer_params = bms_add_member(outer_params, (*pitem).paramId);
        });
        /* Include any outputs of outer-level initPlans */
        foreach!(l, (*proot).init_plans, {
            let initsubplan: *mut SubPlan = lfirst(crate::current_cell!(l)) as *mut SubPlan;
            let mut l2: *mut ListCell;
            foreach!(l2, (*initsubplan).setParam, {
                outer_params = bms_add_member(outer_params, lfirst_int(crate::current_cell!(l2)));
            });
        });
        /* Include worktable ID, if a recursive query is being planned */
        if (*proot).wt_param_id >= 0 {
            outer_params = bms_add_member(outer_params, (*proot).wt_param_id);
        }
        proot = (*proot).parent_root;
    }
    (*root).outer_params = outer_params;
}

/*
 * SS_charge_for_initplans - account for initplans in Path costs & parallelism
 *
 * If any initPlans have been created in the current query level, they will
 * get attached to the Plan tree created from whichever Path we select from
 * the given rel.  Increment all that rel's Paths' costs to account for them,
 * and if any of the initPlans are parallel-unsafe, mark all the rel's Paths
 * parallel-unsafe as well.
 *
 * This is separate from SS_attach_initplans because we might conditionally
 * create more initPlans during create_plan(), depending on which Path we
 * select.  However, Paths that would generate such initPlans are expected
 * to have included their cost and parallel-safety effects already.
 */
pub unsafe fn SS_charge_for_initplans(root: *mut PlannerInfo, final_rel: *mut RelOptInfo) {
    let mut initplan_cost: Cost = 0.0;
    let mut unsafe_initplans: bool = false;
    let mut lc: *mut ListCell;

    /* Nothing to do if no initPlans */
    if list_length((*root).init_plans) == 0 {
        return;
    }

    /*
     * Compute the cost increment just once, since it will be the same for all
     * Paths.  Also check for parallel-unsafe initPlans.
     */
    SS_compute_initplan_cost(
        (*root).init_plans,
        &mut initplan_cost as *mut Cost,
        &mut unsafe_initplans as *mut bool,
    );

    /*
     * Now adjust the costs and parallel_safe flags.
     */
    foreach!(lc, (*final_rel).pathlist, {
        let path: *mut Path = lfirst(crate::current_cell!(lc)) as *mut Path;
        (*path).startup_cost += initplan_cost;
        (*path).total_cost += initplan_cost;
        if unsafe_initplans {
            (*path).parallel_safe = false;
        }
    });

    /*
     * Adjust partial paths' costs too, or forget them entirely if we must
     * consider the rel parallel-unsafe.
     */
    if unsafe_initplans {
        (*final_rel).partial_pathlist = NIL;
        (*final_rel).consider_parallel = false;
    } else {
        foreach!(lc, (*final_rel).partial_pathlist, {
            let path: *mut Path = lfirst(crate::current_cell!(lc)) as *mut Path;
            (*path).startup_cost += initplan_cost;
            (*path).total_cost += initplan_cost;
        });
    }

    /* We needn't do set_cheapest() here, caller will do it */
}

/*
 * SS_compute_initplan_cost - count up the cost delta for some initplans
 *
 * The total cost returned in *initplan_cost_p should be added to both the
 * startup and total costs of the plan node the initplans get attached to.
 * We also report whether any of the initplans are not parallel-safe.
 *
 * The primary user of this is SS_charge_for_initplans, but it's also
 * used in adjusting costs when we move initplans to another plan node.
 */
pub unsafe fn SS_compute_initplan_cost(
    init_plans: *mut List,
    initplan_cost_p: *mut Cost,
    unsafe_initplans_p: *mut bool,
) {
    let mut initplan_cost: Cost = 0.0;
    let mut unsafe_initplans: bool = false;
    let mut lc: *mut ListCell;

    /*
     * We assume each initPlan gets run once during top plan startup.  This is
     * a conservative overestimate, since in fact an initPlan might be
     * executed later than plan startup, or even not at all.
     */
    initplan_cost = 0.0;
    unsafe_initplans = false;
    foreach!(lc, init_plans, {
        let initsubplan: *mut SubPlan =
            lfirst_node!(SubPlan, T_SubPlan, crate::current_cell!(lc));
        initplan_cost += (*initsubplan).startup_cost + (*initsubplan).per_call_cost;
        if !(*initsubplan).parallel_safe {
            unsafe_initplans = true;
        }
    });
    *initplan_cost_p = initplan_cost;
    *unsafe_initplans_p = unsafe_initplans;
}

/*
 * SS_attach_initplans - attach initplans to topmost plan node
 *
 * Attach any initplans created in the current query level to the specified
 * plan node, which should normally be the topmost node for the query level.
 * (In principle the initPlans could go in any node at or above where they're
 * referenced; but there seems no reason to put them any lower than the
 * topmost node, so we don't bother to track exactly where they came from.)
 *
 * We do not touch the plan node's cost or parallel_safe flag.  The initplans
 * must have been accounted for in SS_charge_for_initplans, or by any later
 * code that adds initplans via SS_make_initplan_from_plan.
 */
pub unsafe fn SS_attach_initplans(root: *mut PlannerInfo, plan: *mut Plan) {
    (*plan).initPlan = (*root).init_plans;
}

/*
 * SS_finalize_plan - do final parameter processing for a completed Plan.
 *
 * This recursively computes the extParam and allParam sets for every Plan
 * node in the given plan tree.  (Oh, and RangeTblFunction.funcparams too.)
 *
 * We assume that SS_finalize_plan has already been run on any initplans or
 * subplans the plan tree could reference.
 */
pub unsafe fn SS_finalize_plan(root: *mut PlannerInfo, plan: *mut Plan) {
    /* No setup needed, just recurse through plan tree. */
    finalize_plan(root, plan, -1, (*root).outer_params, ptr::null_mut());
}

/*
 * Recursive processing of all nodes in the plan tree
 *
 * gather_param is the rescan_param of an ancestral Gather/GatherMerge,
 * or -1 if there is none.
 *
 * valid_params is the set of param IDs supplied by outer plan levels
 * that are valid to reference in this plan node or its children.
 *
 * scan_params is a set of param IDs to force scan plan nodes to reference.
 * This is for EvalPlanQual support, and is always NULL at the top of the
 * recursion.
 *
 * The return value is the computed allParam set for the given Plan node.
 * This is just an internal notational convenience: we can add a child
 * plan's allParams to the set of param IDs of interest to this level
 * in the same statement that recurses to that child.
 *
 * Do not scribble on caller's values of valid_params or scan_params!
 *
 * Note: although we attempt to deal with initPlans anywhere in the tree, the
 * logic is not really right.  The problem is that a plan node might return an
 * output Param of its initPlan as a targetlist item, in which case it's valid
 * for the parent plan level to reference that same Param; the parent's usage
 * will be converted into a Var referencing the child plan node by setrefs.c.
 * But this function would see the parent's reference as out of scope and
 * complain about it.  For now, this does not matter because the planner only
 * attaches initPlans to the topmost plan node in a query level, so the case
 * doesn't arise.  If we ever merge this processing into setrefs.c, maybe it
 * can be handled more cleanly.
 */
unsafe fn finalize_plan(
    root: *mut PlannerInfo,
    plan: *mut Plan,
    gather_param: c_int,
    mut valid_params: *mut Bitmapset,
    mut scan_params: *mut Bitmapset,
) -> *mut Bitmapset {
    let mut context = finalize_primnode_context {
        root,
        paramids: ptr::null_mut(), /* initialize set to empty */
    };
    let mut locally_added_param: c_int = -1; /* there isn't one */
    let mut nestloop_params: *mut Bitmapset = ptr::null_mut(); /* there aren't any */
    let mut initExtParam: *mut Bitmapset;
    let mut initSetParam: *mut Bitmapset;
    let mut child_params: *mut Bitmapset;
    let mut l: *mut ListCell;

    if plan.is_null() {
        return ptr::null_mut();
    }

    /*
     * Examine any initPlans to determine the set of external params they
     * reference and the set of output params they supply.  (We assume
     * SS_finalize_plan was run on them already.)
     */
    initExtParam = ptr::null_mut();
    initSetParam = ptr::null_mut();
    foreach!(l, (*plan).initPlan, {
        let initsubplan: *mut SubPlan = lfirst(crate::current_cell!(l)) as *mut SubPlan;
        let initplan: *mut Plan = planner_subplan_get_plan(root, initsubplan);
        let mut l2: *mut ListCell;

        initExtParam = bms_add_members(initExtParam, (*initplan).extParam);
        foreach!(l2, (*initsubplan).setParam, {
            initSetParam = bms_add_member(initSetParam, lfirst_int(crate::current_cell!(l2)));
        });
    });

    /* Any setParams are validly referenceable in this node and children */
    if !initSetParam.is_null() {
        valid_params = bms_union(valid_params, initSetParam);
    }

    /*
     * When we call finalize_primnode, context.paramids sets are automatically
     * merged together.  But when recursing to self, we have to do it the hard
     * way.  We want the paramids set to include params in subplans as well as
     * at this level.
     */

    /* Find params in targetlist and qual */
    finalize_primnode((*plan).targetlist as *mut Node, &mut context as *mut finalize_primnode_context);
    finalize_primnode((*plan).qual as *mut Node, &mut context as *mut finalize_primnode_context);

    /*
     * If it's a parallel-aware scan node, mark it as dependent on the parent
     * Gather/GatherMerge's rescan Param.
     */
    if (*plan).parallel_aware {
        if gather_param < 0 {
            elog!(ERROR, "parallel-aware plan node is not below a Gather");
        }
        context.paramids = bms_add_member(context.paramids, gather_param);
    }

    /* Check additional node-type-specific fields */
    match nodeTag(plan as *mut Node) {
        T_Result => {
            finalize_primnode(
                (*(plan as *mut PlanResult)).resconstantqual,
                &mut context as *mut finalize_primnode_context,
            );
        }

        T_SeqScan => {
            context.paramids = bms_add_members(context.paramids, scan_params);
        }

        T_SampleScan => {
            finalize_primnode(
                (*(plan as *mut SampleScan)).tablesample as *mut Node,
                &mut context as *mut finalize_primnode_context,
            );
            context.paramids = bms_add_members(context.paramids, scan_params);
        }

        T_IndexScan => {
            finalize_primnode(
                (*(plan as *mut IndexScan)).indexqual as *mut Node,
                &mut context as *mut finalize_primnode_context,
            );
            finalize_primnode(
                (*(plan as *mut IndexScan)).indexorderby as *mut Node,
                &mut context as *mut finalize_primnode_context,
            );
            /*
             * we need not look at indexqualorig, since it will have the same
             * param references as indexqual.  Likewise, we can ignore
             * indexorderbyorig.
             */
            context.paramids = bms_add_members(context.paramids, scan_params);
        }

        T_IndexOnlyScan => {
            finalize_primnode(
                (*(plan as *mut IndexOnlyScan)).indexqual as *mut Node,
                &mut context as *mut finalize_primnode_context,
            );
            finalize_primnode(
                (*(plan as *mut IndexOnlyScan)).recheckqual as *mut Node,
                &mut context as *mut finalize_primnode_context,
            );
            finalize_primnode(
                (*(plan as *mut IndexOnlyScan)).indexorderby as *mut Node,
                &mut context as *mut finalize_primnode_context,
            );
            /*
             * we need not look at indextlist, since it cannot contain Params.
             */
            context.paramids = bms_add_members(context.paramids, scan_params);
        }

        T_BitmapIndexScan => {
            finalize_primnode(
                (*(plan as *mut BitmapIndexScan)).indexqual as *mut Node,
                &mut context as *mut finalize_primnode_context,
            );
            /*
             * we need not look at indexqualorig, since it will have the same
             * param references as indexqual.
             */
        }

        T_BitmapHeapScan => {
            finalize_primnode(
                (*(plan as *mut BitmapHeapScan)).bitmapqualorig as *mut Node,
                &mut context as *mut finalize_primnode_context,
            );
            context.paramids = bms_add_members(context.paramids, scan_params);
        }

        T_TidScan => {
            finalize_primnode(
                (*(plan as *mut TidScan)).tidquals as *mut Node,
                &mut context as *mut finalize_primnode_context,
            );
            context.paramids = bms_add_members(context.paramids, scan_params);
        }

        T_TidRangeScan => {
            finalize_primnode(
                (*(plan as *mut TidRangeScan)).tidrangequals as *mut Node,
                &mut context as *mut finalize_primnode_context,
            );
            context.paramids = bms_add_members(context.paramids, scan_params);
        }

        T_SubqueryScan => {
            let sscan: *mut SubqueryScan = plan as *mut SubqueryScan;
            let rel: *mut RelOptInfo;
            let subquery_params: *mut Bitmapset;

            /* We must run finalize_plan on the subquery */
            rel = find_base_rel(root, (*sscan).scan.scanrelid as c_int);
            let mut sub_outer = (*(*rel).subroot).outer_params;
            if gather_param >= 0 {
                sub_outer = bms_add_member(bms_copy(sub_outer), gather_param);
            }
            finalize_plan((*rel).subroot, (*sscan).subplan, gather_param, sub_outer, ptr::null_mut());

            /* Now we can add its extParams to the parent's params */
            context.paramids =
                bms_add_members(context.paramids, (*(*sscan).subplan).extParam);
            /* We need scan_params too, though */
            context.paramids = bms_add_members(context.paramids, scan_params);
        }

        T_FunctionScan => {
            let fscan: *mut FunctionScan = plan as *mut FunctionScan;
            let mut lc: *mut ListCell;

            /*
             * Call finalize_primnode independently on each function
             * expression, so that we can record which params are
             * referenced in each, in order to decide which need
             * re-evaluating during rescan.
             */
            foreach!(lc, (*fscan).functions, {
                let rtfunc: *mut RangeTblFunction =
                    lfirst(crate::current_cell!(lc)) as *mut RangeTblFunction;
                let mut funccontext = finalize_primnode_context {
                    root: context.root,
                    paramids: context.paramids,
                };
                funccontext.paramids = ptr::null_mut();

                finalize_primnode((*rtfunc).funcexpr as *mut Node, &mut funccontext as *mut finalize_primnode_context);

                /* remember results for execution */
                (*rtfunc).funcparams = funccontext.paramids;

                /* add the function's params to the overall set */
                context.paramids =
                    bms_add_members(context.paramids, funccontext.paramids);
            });

            context.paramids = bms_add_members(context.paramids, scan_params);
        }

        T_TableFuncScan => {
            finalize_primnode(
                (*(plan as *mut TableFuncScan)).tablefunc as *mut Node,
                &mut context as *mut finalize_primnode_context,
            );
            context.paramids = bms_add_members(context.paramids, scan_params);
        }

        T_ValuesScan => {
            finalize_primnode(
                (*(plan as *mut ValuesScan)).values_lists as *mut Node,
                &mut context as *mut finalize_primnode_context,
            );
            context.paramids = bms_add_members(context.paramids, scan_params);
        }

        T_CteScan => {
            /*
             * You might think we should add the node's cteParam to
             * paramids, but we shouldn't because that param is just a
             * linkage mechanism for multiple CteScan nodes for the same
             * CTE; it is never used for changed-param signaling.  What we
             * have to do instead is to find the referenced CTE plan and
             * incorporate its external paramids, so that the correct
             * things will happen if the CTE references outer-level
             * variables.  See test cases for bug #4902.  (We assume
             * SS_finalize_plan was run on the CTE plan already.)
             */
            let plan_id: c_int = (*(plan as *mut CteScan)).ctePlanId;
            let cteplan: *mut Plan;

            /* so, do this ... */
            if plan_id < 1 || plan_id > list_length((*(*root).glob).subplans) {
                elog!(
                    ERROR,
                    /* C also: */
                    "could not find plan for CteScan referencing plan ID %d"
                    /* (plan_id) */
                );
            }
            cteplan = list_nth((*(*root).glob).subplans, plan_id - 1) as *mut Plan;
            context.paramids = bms_add_members(context.paramids, (*cteplan).extParam);

            /* #ifdef NOT_USED: context.paramids = bms_add_member(context.paramids, cteParam); */

            context.paramids = bms_add_members(context.paramids, scan_params);
        }

        T_WorkTableScan => {
            context.paramids = bms_add_member(
                context.paramids,
                (*(plan as *mut WorkTableScan)).wtParam,
            );
            context.paramids = bms_add_members(context.paramids, scan_params);
        }

        T_NamedTuplestoreScan => {
            context.paramids = bms_add_members(context.paramids, scan_params);
        }

        T_ForeignScan => {
            let fscan: *mut ForeignScan = plan as *mut ForeignScan;

            finalize_primnode(
                (*fscan).fdw_exprs as *mut Node,
                &mut context as *mut finalize_primnode_context,
            );
            finalize_primnode(
                (*fscan).fdw_recheck_quals as *mut Node,
                &mut context as *mut finalize_primnode_context,
            );
            /* We assume fdw_scan_tlist cannot contain Params */
            context.paramids = bms_add_members(context.paramids, scan_params);
        }

        T_CustomScan => {
            let cscan: *mut CustomScan = plan as *mut CustomScan;
            let mut lc: *mut ListCell;

            finalize_primnode(
                (*cscan).custom_exprs as *mut Node,
                &mut context as *mut finalize_primnode_context,
            );
            /* We assume custom_scan_tlist cannot contain Params */
            context.paramids = bms_add_members(context.paramids, scan_params);

            /* child nodes if any */
            foreach!(lc, (*cscan).custom_plans, {
                context.paramids = bms_add_members(
                    context.paramids,
                    finalize_plan(
                        root,
                        lfirst(crate::current_cell!(lc)) as *mut Plan,
                        gather_param,
                        valid_params,
                        scan_params,
                    ),
                );
            });
        }

        T_ModifyTable => {
            let mtplan: *mut ModifyTable = plan as *mut ModifyTable;

            /* Force descendant scan nodes to reference epqParam */
            locally_added_param = (*mtplan).epqParam;
            valid_params = bms_add_member(bms_copy(valid_params), locally_added_param);
            scan_params = bms_add_member(bms_copy(scan_params), locally_added_param);
            finalize_primnode(
                (*mtplan).returningLists as *mut Node,
                &mut context as *mut finalize_primnode_context,
            );
            finalize_primnode(
                (*mtplan).onConflictSet as *mut Node,
                &mut context as *mut finalize_primnode_context,
            );
            finalize_primnode(
                (*mtplan).onConflictWhere as *mut Node,
                &mut context as *mut finalize_primnode_context,
            );
            /* exclRelTlist contains only Vars, doesn't need examination */
        }

        T_Append => {
            foreach!(l, (*(plan as *mut Append)).appendplans, {
                context.paramids = bms_add_members(
                    context.paramids,
                    finalize_plan(
                        root,
                        lfirst(crate::current_cell!(l)) as *mut Plan,
                        gather_param,
                        valid_params,
                        scan_params,
                    ),
                );
            });
        }

        T_MergeAppend => {
            foreach!(l, (*(plan as *mut MergeAppend)).mergeplans, {
                context.paramids = bms_add_members(
                    context.paramids,
                    finalize_plan(
                        root,
                        lfirst(crate::current_cell!(l)) as *mut Plan,
                        gather_param,
                        valid_params,
                        scan_params,
                    ),
                );
            });
        }

        T_BitmapAnd => {
            foreach!(l, (*(plan as *mut BitmapAnd)).bitmapplans, {
                context.paramids = bms_add_members(
                    context.paramids,
                    finalize_plan(
                        root,
                        lfirst(crate::current_cell!(l)) as *mut Plan,
                        gather_param,
                        valid_params,
                        scan_params,
                    ),
                );
            });
        }

        T_BitmapOr => {
            foreach!(l, (*(plan as *mut BitmapOr)).bitmapplans, {
                context.paramids = bms_add_members(
                    context.paramids,
                    finalize_plan(
                        root,
                        lfirst(crate::current_cell!(l)) as *mut Plan,
                        gather_param,
                        valid_params,
                        scan_params,
                    ),
                );
            });
        }

        T_NestLoop => {
            finalize_primnode(
                (*(plan as *mut Join)).joinqual as *mut Node,
                &mut context as *mut finalize_primnode_context,
            );
            /* collect set of params that will be passed to right child */
            foreach!(l, (*(plan as *mut NestLoop)).nestParams, {
                let nlp: *mut NestLoopParam = lfirst(crate::current_cell!(l)) as *mut NestLoopParam;
                nestloop_params = bms_add_member(nestloop_params, (*nlp).paramno);
            });
        }

        T_MergeJoin => {
            finalize_primnode(
                (*(plan as *mut Join)).joinqual as *mut Node,
                &mut context as *mut finalize_primnode_context,
            );
            finalize_primnode(
                (*(plan as *mut MergeJoin)).mergeclauses as *mut Node,
                &mut context as *mut finalize_primnode_context,
            );
        }

        T_HashJoin => {
            finalize_primnode(
                (*(plan as *mut Join)).joinqual as *mut Node,
                &mut context as *mut finalize_primnode_context,
            );
            finalize_primnode(
                (*(plan as *mut HashJoin)).hashclauses as *mut Node,
                &mut context as *mut finalize_primnode_context,
            );
        }

        T_Hash => {
            finalize_primnode(
                (*(plan as *mut Hash)).hashkeys as *mut Node,
                &mut context as *mut finalize_primnode_context,
            );
        }

        T_Limit => {
            finalize_primnode(
                (*(plan as *mut Limit)).limitOffset,
                &mut context as *mut finalize_primnode_context,
            );
            finalize_primnode(
                (*(plan as *mut Limit)).limitCount,
                &mut context as *mut finalize_primnode_context,
            );
        }

        T_RecursiveUnion => {
            /* child nodes are allowed to reference wtParam */
            locally_added_param = (*(plan as *mut RecursiveUnion)).wtParam;
            valid_params = bms_add_member(bms_copy(valid_params), locally_added_param);
            /* wtParam does *not* get added to scan_params */
        }

        T_LockRows => {
            /* Force descendant scan nodes to reference epqParam */
            locally_added_param = (*(plan as *mut LockRows)).epqParam;
            valid_params = bms_add_member(bms_copy(valid_params), locally_added_param);
            scan_params = bms_add_member(bms_copy(scan_params), locally_added_param);
        }

        T_Agg => {
            let agg: *mut Agg = plan as *mut Agg;

            /*
             * AGG_HASHED plans need to know which Params are referenced
             * in aggregate calls.  Do a separate scan to identify them.
             */
            if (*agg).aggstrategy == AGG_HASHED {
                let mut aggcontext = finalize_primnode_context {
                    root,
                    paramids: ptr::null_mut(),
                };
                finalize_agg_primnode(
                    (*agg).plan.targetlist as *mut Node,
                    &mut aggcontext as *mut finalize_primnode_context,
                );
                finalize_agg_primnode(
                    (*agg).plan.qual as *mut Node,
                    &mut aggcontext as *mut finalize_primnode_context,
                );
                (*agg).aggParams = aggcontext.paramids;
            }
        }

        T_WindowAgg => {
            finalize_primnode(
                (*(plan as *mut WindowAgg)).startOffset,
                &mut context as *mut finalize_primnode_context,
            );
            finalize_primnode(
                (*(plan as *mut WindowAgg)).endOffset,
                &mut context as *mut finalize_primnode_context,
            );
        }

        T_Gather => {
            /* child nodes are allowed to reference rescan_param, if any */
            locally_added_param = (*(plan as *mut Gather)).rescan_param;
            if locally_added_param >= 0 {
                valid_params = bms_add_member(bms_copy(valid_params), locally_added_param);

                /*
                 * We currently don't support nested Gathers.  The issue so
                 * far as this function is concerned would be how to identify
                 * which child nodes depend on which Gather.
                 */
                Assert!(gather_param < 0);
                /* Pass down rescan_param to child parallel-aware nodes */
                // gather_param shadowed below via function call
                let _ = locally_added_param; /* used below */
            }
            /* rescan_param does *not* get added to scan_params */
        }

        T_GatherMerge => {
            /* child nodes are allowed to reference rescan_param, if any */
            locally_added_param = (*(plan as *mut GatherMerge)).rescan_param;
            if locally_added_param >= 0 {
                valid_params = bms_add_member(bms_copy(valid_params), locally_added_param);

                /*
                 * We currently don't support nested Gathers.  The issue so
                 * far as this function is concerned would be how to identify
                 * which child nodes depend on which Gather.
                 */
                Assert!(gather_param < 0);
                /* Pass down rescan_param to child parallel-aware nodes */
                let _ = locally_added_param; /* used below */
            }
            /* rescan_param does *not* get added to scan_params */
        }

        T_Memoize => {
            finalize_primnode(
                (*(plan as *mut Memoize)).param_exprs as *mut Node,
                &mut context as *mut finalize_primnode_context,
            );
        }

        T_ProjectSet
        | T_Material
        | T_Sort
        | T_IncrementalSort
        | T_Unique
        | T_SetOp
        | T_Group => {
            /* no node-type-specific fields need fixing */
        }

        _ => {
            elog!(ERROR, "unrecognized node type: %d" /* (nodeTag(plan)) */);
        }
    }

    /* Determine actual gather_param for children (may have been updated above) */
    let child_gather_param = if (nodeTag(plan as *mut Node) == T_Gather
        || nodeTag(plan as *mut Node) == T_GatherMerge)
        && locally_added_param >= 0
    {
        locally_added_param
    } else {
        gather_param
    };

    /* Process left and right child plans, if any */
    child_params = finalize_plan(
        root,
        (*plan).lefttree,
        child_gather_param,
        valid_params,
        scan_params,
    );
    context.paramids = bms_add_members(context.paramids, child_params);

    if !nestloop_params.is_null() {
        /* right child can reference nestloop_params as well as valid_params */
        child_params = finalize_plan(
            root,
            (*plan).righttree,
            child_gather_param,
            bms_union(nestloop_params, valid_params),
            scan_params,
        );
        /* ... and they don't count as parameters used at my level */
        child_params = bms_difference(child_params, nestloop_params);
        bms_free(nestloop_params);
    } else {
        /* easy case */
        child_params = finalize_plan(
            root,
            (*plan).righttree,
            child_gather_param,
            valid_params,
            scan_params,
        );
    }
    context.paramids = bms_add_members(context.paramids, child_params);

    /*
     * Any locally generated parameter doesn't count towards its generating
     * plan node's external dependencies.  (Note: if we changed valid_params
     * and/or scan_params, we leak those bitmapsets; not worth the notational
     * trouble to clean them up.)
     */
    if locally_added_param >= 0 {
        context.paramids = bms_del_member(context.paramids, locally_added_param);
    }

    /* Now we have all the paramids referenced in this node and children */

    if !bms_is_subset(context.paramids, valid_params) {
        elog!(ERROR, "plan should not reference subplan's variable");
    }

    /*
     * The plan node's allParam and extParam fields should include all its
     * referenced paramids, plus contributions from any child initPlans.
     * However, any setParams of the initPlans should not be present in the
     * parent node's extParams, only in its allParams.  (It's possible that
     * some initPlans have extParams that are setParams of other initPlans.)
     */

    /* allParam must include initplans' extParams and setParams */
    (*plan).allParam = bms_union(context.paramids, initExtParam);
    (*plan).allParam = bms_add_members((*plan).allParam, initSetParam);
    /* extParam must include any initplan extParams */
    (*plan).extParam = bms_union(context.paramids, initExtParam);
    /* but not any initplan setParams */
    (*plan).extParam = bms_del_members((*plan).extParam, initSetParam);

    (*plan).allParam
}

/*
 * finalize_primnode: add IDs of all PARAM_EXEC params that appear (or will
 * appear) in the given expression tree to the result set.
 */
unsafe fn finalize_primnode(
    node: *mut Node,
    context: *mut finalize_primnode_context,
) -> bool {
    if node.is_null() {
        return false;
    }
    if IsA!(node, T_Param) {
        if (*(node as *mut Param)).paramkind == PARAM_EXEC {
            let paramid: c_int = (*(node as *mut Param)).paramid;
            (*context).paramids = bms_add_member((*context).paramids, paramid);
        }
        return false; /* no more to do here */
    } else if IsA!(node, T_Aggref) {
        /*
         * Check to see if the aggregate will be replaced by a Param
         * referencing a subquery output during setrefs.c.  If so, we must
         * account for that Param here.  (For various reasons, it's not
         * convenient to perform that substitution earlier than setrefs.c, nor
         * to perform this processing after setrefs.c.  Thus we need a wart
         * here.)
         */
        let aggref: *mut Aggref = node as *mut Aggref;
        let aggparam: *mut Param;

        aggparam = find_minmax_agg_replacement_param((*context).root, aggref);
        if !aggparam.is_null() {
            (*context).paramids = bms_add_member((*context).paramids, (*aggparam).paramid);
        }
        /* Fall through to examine the agg's arguments */
    } else if IsA!(node, T_SubPlan) {
        let subplan: *mut SubPlan = node as *mut SubPlan;
        let plan: *mut Plan = planner_subplan_get_plan((*context).root, subplan);
        let mut lc: *mut ListCell;
        let subparamids: *mut Bitmapset;

        /* Recurse into the testexpr, but not into the Plan */
        finalize_primnode((*subplan).testexpr, context);

        /*
         * Remove any param IDs of output parameters of the subplan that were
         * referenced in the testexpr.  These are not interesting for
         * parameter change signaling since we always re-evaluate the subplan.
         * Note that this wouldn't work too well if there might be uses of the
         * same param IDs elsewhere in the plan, but that can't happen because
         * generate_new_exec_param never tries to merge params.
         */
        foreach!(lc, (*subplan).paramIds, {
            (*context).paramids =
                bms_del_member((*context).paramids, lfirst_int(crate::current_cell!(lc)));
        });

        /* Also examine args list */
        finalize_primnode((*subplan).args as *mut Node, context);

        /*
         * Add params needed by the subplan to paramids, but excluding those
         * we will pass down to it.  (We assume SS_finalize_plan was run on
         * the subplan already.)
         */
        let mut subparamids = bms_copy((*plan).extParam);
        foreach!(lc, (*subplan).parParam, {
            subparamids = bms_del_member(subparamids, lfirst_int(crate::current_cell!(lc)));
        });
        (*context).paramids = bms_join((*context).paramids, subparamids);

        return false; /* no more to do here */
    }
    unsafe fn finalize_primnode_trampoline(n: *mut Node, ctx: *mut c_void) -> bool {
        finalize_primnode(n, ctx as *mut finalize_primnode_context)
    }
    expression_tree_walker(node, finalize_primnode_trampoline, context as *mut c_void)
}

/*
 * finalize_agg_primnode: find all Aggref nodes in the given expression tree,
 * and add IDs of all PARAM_EXEC params appearing within their aggregated
 * arguments to the result set.
 */
unsafe fn finalize_agg_primnode(
    node: *mut Node,
    context: *mut finalize_primnode_context,
) -> bool {
    if node.is_null() {
        return false;
    }
    if IsA!(node, T_Aggref) {
        let agg: *mut Aggref = node as *mut Aggref;

        /* we should not consider the direct arguments, if any */
        finalize_primnode((*agg).args as *mut Node, context);
        finalize_primnode((*agg).aggfilter as *mut Node, context);
        return false; /* there can't be any Aggrefs below here */
    }
    unsafe fn finalize_agg_primnode_trampoline(n: *mut Node, ctx: *mut c_void) -> bool {
        finalize_agg_primnode(n, ctx as *mut finalize_primnode_context)
    }
    expression_tree_walker(node, finalize_agg_primnode_trampoline, context as *mut c_void)
}

/*
 * SS_make_initplan_output_param - make a Param for an initPlan's output
 *
 * The plan is expected to return a scalar value of the given type/collation.
 *
 * Note that in some cases the initplan may not ever appear in the finished
 * plan tree.  If that happens, we'll have wasted a PARAM_EXEC slot, which
 * is no big deal.
 */
pub unsafe fn SS_make_initplan_output_param(
    root: *mut PlannerInfo,
    resulttype: Oid,
    resulttypmod: i32,
    resultcollation: Oid,
) -> *mut Param {
    generate_new_exec_param(root, resulttype, resulttypmod, resultcollation)
}

/*
 * SS_make_initplan_from_plan - given a plan tree, make it an InitPlan
 *
 * We build an EXPR_SUBLINK SubPlan node and put it into the initplan
 * list for the outer query level.  A Param that represents the initplan's
 * output has already been assigned using SS_make_initplan_output_param.
 */
pub unsafe fn SS_make_initplan_from_plan(
    root: *mut PlannerInfo,
    subroot: *mut PlannerInfo,
    plan: *mut Plan,
    prm: *mut Param,
) {
    let mut node: *mut SubPlan;

    /*
     * Add the subplan and its PlannerInfo, as well as a dummy path entry, to
     * the global lists.  Ideally we'd save a real path, but right now our
     * sole caller doesn't build a path that exactly matches the plan.  Since
     * we're not currently going to need the path for an initplan, it's not
     * worth requiring construction of such a path.
     */
    (*(*root).glob).subplans = lappend((*(*root).glob).subplans, plan as *mut _);
    (*(*root).glob).subpaths = lappend((*(*root).glob).subpaths, ptr::null_mut());
    (*(*root).glob).subroots = lappend((*(*root).glob).subroots, subroot as *mut _);

    /*
     * Create a SubPlan node and add it to the outer list of InitPlans. Note
     * it has to appear after any other InitPlans it might depend on (see
     * comments in ExecReScan).
     */
    node = makeNode!(SubPlan, T_SubPlan);
    (*node).subLinkType = EXPR_SUBLINK;
    (*node).plan_id = list_length((*(*root).glob).subplans);
    (*node).plan_name = psprintf(b"InitPlan %d\0".as_ptr() as *const c_char);
    get_first_col_type(
        plan,
        &mut (*node).firstColType as *mut Oid,
        &mut (*node).firstColTypmod as *mut i32,
        &mut (*node).firstColCollation as *mut Oid,
    );
    (*node).parallel_safe = (*plan).parallel_safe;
    (*node).setParam = list_make1_int!((*prm).paramid);

    (*root).init_plans = lappend((*root).init_plans, node as *mut _);

    /*
     * The node can't have any inputs (since it's an initplan), so the
     * parParam and args lists remain empty.
     */

    /* Set costs of SubPlan using info from the plan tree */
    cost_subplan(subroot, node, plan);
}
