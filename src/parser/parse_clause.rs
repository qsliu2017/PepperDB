/*-------------------------------------------------------------------------
 *
 * parse_clause.rs
 *   handle clauses in parser
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *   src/backend/parser/parse_clause.c
 *
 *-------------------------------------------------------------------------
 */

#![allow(unused_variables)]
#![allow(dead_code)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(unused_mut)]
#![allow(unused_imports)]

use crate::prelude::*;

use std::ffi::{c_char, c_int, c_void};
use core::mem::size_of;

use crate::{castNode, current_cell, foreach, intVal, lfirst_node, linitial_node, list_make1, list_make2, makeNode, strVal, IsA};

// ---------------------------------------------------------------------------
// Standard library / crate imports
// ---------------------------------------------------------------------------
use crate::postgres_ext::Oid;
use crate::postgres::{Datum, ObjectIdGetDatum, Int32GetDatum};
use crate::c::{OidIsValid, int32};

use crate::nodes::nodes::{nodeTag, Node, NodeTag, NodeTag::*, CmdType, JoinType, OnConflictAction, LimitOption};
use crate::nodes::pg_list::{
    List, NIL,
    lfirst, lfirst_int, lfirst_oid, linitial, lsecond, llast, lnext,
    lappend, lappend_int, lappend_oid, lcons, list_head,
    list_concat, list_length, list_make1_impl,
    list_nth, list_nth_cell, list_truncate, list_member_int,
    list_copy, ListCell,
};
use crate::nodes::bitmapset::{
    Bitmapset, bms_add_member, bms_add_members, bms_is_member, bms_union,
};
use crate::nodes::nodeFuncs::{
    exprType, exprTypmod, exprCollation, exprLocation,
    strip_implicit_coercions,
};
use crate::nodes::makefuncs::{
    makeConst, makeBoolExpr, makeVar, makeTargetEntry,
    makeSimpleA_Expr, makeNullConst, makeRelabelType,
};
use crate::nodes::value::{makeString, String as PgString};
use crate::nodes::primnodes::{
    Expr, Var, Alias, TargetEntry, JoinExpr, RangeTblRef, RangeVar,
    TableFunc, CoalesceExpr, BoolExpr, BoolExprType,
    VarReturningType, VarReturningType::*,
    CoercionForm, CoercionForm::*,
    InferenceElem,
};
use crate::nodes::parsenodes::{
    Query, ColumnRef, A_Const, A_Expr, FuncCall, SortBy, SelectStmt,
    RangeSubselect, RangeFunction, RangeTblEntry, RangeTblFunction,
    RTEKind, RTEKind::*,
    RTEPermissionInfo, RowMarkClause, CommonTableExpr,
    GroupingSet, GroupingSetKind, GroupingSetKind::*,
    WindowDef, WindowClause, SortGroupClause,
    OnConflictClause, InferClause,
    IndexElem,
    RangeTableFunc, RangeTableFuncCol,
    RangeTableSample, TableSampleClause,
    SortByDir, SortByDir::*,
    SortByNulls, SortByNulls::*,
};

use crate::parser::parse_node::{
    cancel_parser_errposition_callback, parser_errposition,
    setup_parser_errposition_callback,
    Index, ParseCallbackState, ParseExprKind, ParseExprKind::*,
    ParseNamespaceColumn, ParseNamespaceItem, ParseState, Relation,
    make_parsestate, free_parsestate,
};
use crate::parser::parse_expr::transformExpr;
use crate::parser::parse_collate::assign_expr_collations;
use crate::parser::parse_type::LookupCollation;
use crate::parser::parse_relation::{
    addRangeTableEntry,
    addRangeTableEntryForRelation,
    addRangeTableEntryForSubquery,
    addRangeTableEntryForFunction,
    addRangeTableEntryForTableFunc,
    addRangeTableEntryForJoin,
    addRangeTableEntryForCTE,
    addRangeTableEntryForENR,
    addNSItemToQuery,
    checkNameSpaceConflicts,
    scanNameSpaceForCTE,
    scanNameSpaceForENR,
    markVarForSelectPriv,
    markNullableIfNeeded,
};

use crate::access::table::table::{table_close, table_open, table_openrv_extended};
use crate::parser::parse_relation::parserOpenTable as parserOpenTable;
use crate::access::common::relation::{relation_open, relation_close};
use crate::access::common::tupdesc::TupleDesc;
use crate::access::attnum::{AttrNumber, InvalidAttrNumber};
use crate::access::htup_details::HeapTuple;

use crate::storage::lockdefs::{AccessShareLock, NoLock, RowExclusiveLock, RowShareLock, LOCKMODE};

use crate::utils::cache::lsyscache::{
    format_type_be,
    get_typcollation,
    pstrdup, palloc, pfree,
};
use crate::utils::mmgr::mcxt::palloc0;
use crate::utils::cache::syscache::{
    ReleaseSysCache, SearchSysCache2, SearchSysCacheList,
};
use crate::access::index::amvalidate::{CatCList, CatCTup};
use crate::access::htup_details::{GETSTRUCT, HeapTupleData};
use crate::utils::rel::{
    RelationGetRelationName, RelationGetRelid, RelationGetNumberOfAttributes,
    RelationData,
};
use crate::miscadmin::check_stack_depth;
use crate::postgres_ext::InvalidOid;

// ---------------------------------------------------------------------------
// OID constants not yet in dedicated modules
// ---------------------------------------------------------------------------
const INT4OID:    Oid = 23;
const INT8OID:    Oid = 20;
const FLOAT8OID:  Oid = 701;
const TEXTOID:    Oid = 25;
const XMLOID:     Oid = 142;
const UNKNOWNOID: Oid = 705;
const INTERNALOID: Oid = 2281;

// access/nbtree.h
const BTREE_AM_OID:   Oid = 403;
const BTINRANGE_PROC: c_int = 6;

// access/tsmapi.h
const TSM_HANDLEROID: Oid = 3310;

// utils/acl.h
type AclMode = u64;
const ACL_SELECT: AclMode = 0x0002;

// nodes/parsenodes.h  FRAMEOPTION_* bitmasks
const FRAMEOPTION_DEFAULTS:         c_int = 0x000;
const FRAMEOPTION_ROWS:             c_int = 0x001;
const FRAMEOPTION_RANGE:            c_int = 0x002;
const FRAMEOPTION_GROUPS:           c_int = 0x004;
const FRAMEOPTION_START_OFFSET:     c_int = 0x040;
const FRAMEOPTION_END_OFFSET:       c_int = 0x080;

// nodes/parsenodes.h  SORTBY_* -- variants imported from SortByDir::* and SortByNulls::*

// catalog/pg_class.h
const RELKIND_RELATION:        c_char = b'r' as c_char;
const RELKIND_MATVIEW:         c_char = b'm' as c_char;
const RELKIND_PARTITIONED_TABLE: c_char = b'p' as c_char;

// catalog/pg_am.h  (AMPROCNUM SysCache index for amproc tuples)
const AMPROCNUM: c_int = 5; // stub: catcache not yet ported

// ---------------------------------------------------------------------------
// Stubs for unported siblings
// ---------------------------------------------------------------------------

// TODO(pg-port): nodes/nodeFuncs.c  contain_vars_of_level
unsafe fn contain_vars_of_level(node: *mut Node, levelsup: c_int) -> bool {
    crate::optimizer::util::var::contain_vars_of_level(node as _, levelsup)
}
// TODO(pg-port): nodes/nodeFuncs.c  locate_var_of_level
unsafe fn locate_var_of_level(node: *mut Node, levelsup: c_int) -> c_int {
    crate::optimizer::util::var::locate_var_of_level(node as _, levelsup)
}
// TODO(pg-port): nodes/nodeFuncs.c  contain_aggs_of_level
unsafe fn contain_aggs_of_level(node: *mut Node, levelsup: c_int) -> bool {
    crate::rewrite::rewriteManip::contain_aggs_of_level(node as _, levelsup)
}
// TODO(pg-port): nodes/nodeFuncs.c  locate_agg_of_level
unsafe fn locate_agg_of_level(node: *mut Node, levelsup: c_int) -> c_int {
    crate::rewrite::rewriteManip::locate_agg_of_level(node as _, levelsup)
}
// TODO(pg-port): nodes/nodeFuncs.c  contain_windowfuncs
unsafe fn contain_windowfuncs(node: *mut Node) -> bool {
    crate::rewrite::rewriteManip::contain_windowfuncs(node as _)
}
// TODO(pg-port): nodes/nodeFuncs.c  locate_windowfunc
unsafe fn locate_windowfunc(node: *mut Node) -> c_int {
    crate::rewrite::rewriteManip::locate_windowfunc(node as _)
}
// TODO(pg-port): nodes/equalfuncs.c  equal
unsafe fn equal(a: *mut c_void, b: *mut c_void) -> bool {
    crate::nodes::equalfuncs::equal(a as _, b as _)
}

// copyObject helper (copyfuncs not yet enabled)
unsafe fn copyObject<T>(node: *mut T) -> *mut T {
    // TODO(pg-port): copyfuncs.c
    node
}

// TODO(pg-port): analyze.c
unsafe fn parse_sub_analyze(
    parseTree: *mut Node,
    parentParseState: *mut ParseState,
    queryEnv: *mut c_void,
    locked_from_parent: bool,
    resolve_unknowns: bool,
) -> *mut Query {
    crate::parser::analyze::parse_sub_analyze(
        parseTree as _,
        parentParseState as _,
        queryEnv as _,
        locked_from_parent as _,
        resolve_unknowns as _,
    ) as _
}

// TODO(pg-port): parser/analyze.c
unsafe fn transformStmt(pstate: *mut ParseState, parseTree: *mut Node) -> *mut Query {
    crate::parser::analyze::transformStmt(pstate as _, parseTree as _) as _
}

// TODO(pg-port): parser/parse_target.c
unsafe fn transformTargetEntry(
    pstate: *mut ParseState,
    node: *mut Node,
    expr: *mut Node,
    exprKind: ParseExprKind,
    colname: *const c_char,
    resjunk: bool,
) -> *mut TargetEntry {
    crate::parser::parse_target::transformTargetEntry(
        pstate as _,
        node as _,
        expr as _,
        exprKind as _,
        colname as _,
        resjunk as _,
    ) as _
}

// TODO(pg-port): parser/parse_oper.c
unsafe fn get_sort_group_operators(
    argtype: Oid,
    needLT: bool,
    needEQ: bool,
    needGT: bool,
    ltOpr: *mut Oid,
    eqOpr: *mut Oid,
    gtOpr: *mut Oid,
    isHashable: *mut bool,
) {
    crate::parser::parse_oper::get_sort_group_operators(
        argtype as _,
        needLT as _,
        needEQ as _,
        needGT as _,
        ltOpr as _,
        eqOpr as _,
        gtOpr as _,
        isHashable as _,
    )
}

// TODO(pg-port): parser/parse_oper.c
unsafe fn compatible_oper_opid(op: *mut List, arg1: Oid, arg2: Oid, noError: bool) -> Oid {
    crate::parser::parse_oper::compatible_oper_opid(op as _, arg1 as _, arg2 as _, noError as _) as _
}

// TODO(pg-port): utils/cache/lsyscache.c
unsafe fn get_equality_op_for_ordering_op(opno: Oid, reverse: *mut bool) -> Oid {
    crate::utils::cache::lsyscache::get_equality_op_for_ordering_op(opno as _, reverse as _) as _
}

// TODO(pg-port): utils/cache/lsyscache.c
unsafe fn op_hashjoinable(opno: Oid, inputtype: Oid) -> bool {
    crate::utils::cache::lsyscache::op_hashjoinable(opno as _, inputtype as _) as _
}

// TODO(pg-port): utils/cache/lsyscache.c
unsafe fn get_commutator(opno: Oid) -> Oid {
    crate::utils::cache::lsyscache::get_commutator(opno as _) as _
}

// TODO(pg-port): optimizer/optimizer.c
unsafe fn get_sortgroupclause_expr(
    sgClause: *mut SortGroupClause,
    targetList: *mut List,
) -> *mut Node {
    crate::optimizer::util::tlist::get_sortgroupclause_expr(sgClause as _, targetList as _) as _
}

// TODO(pg-port): optimizer/optimizer.c
unsafe fn get_sortgroupclause_tle(
    sgClause: *mut SortGroupClause,
    targetList: *mut List,
) -> *mut TargetEntry {
    crate::optimizer::util::tlist::get_sortgroupclause_tle(sgClause as _, targetList as _) as _
}

// TODO(pg-port): optimizer/optimizer.c
unsafe fn get_sortgroupref_tle(sortref: Index, targetList: *mut List) -> *mut TargetEntry {
    crate::optimizer::util::tlist::get_sortgroupref_tle(sortref as _, targetList as _) as _
}

// TODO(pg-port): optimizer/optimizer.c
unsafe fn get_ordering_op_properties(
    opno: Oid,
    opfamily: *mut Oid,
    opcintype: *mut Oid,
    cmptype: *mut c_int,
) -> bool {
    crate::utils::cache::lsyscache::get_ordering_op_properties(
        opno as _,
        opfamily as _,
        opcintype as _,
        cmptype as _,
    ) as _
}

// TODO(pg-port): parser/parse_coerce.c
unsafe fn coerce_to_boolean(
    pstate: *mut ParseState,
    node: *mut Node,
    constructName: *const c_char,
) -> *mut Node {
    crate::parser::parse_coerce::coerce_to_boolean(pstate as _, node as _, constructName as _) as _
}

// TODO(pg-port): parser/parse_coerce.c
unsafe fn coerce_to_specific_type(
    pstate: *mut ParseState,
    node: *mut Node,
    targetTypeId: Oid,
    constructName: *const c_char,
) -> *mut Node {
    crate::parser::parse_coerce::coerce_to_specific_type(
        pstate as _,
        node as _,
        targetTypeId as _,
        constructName as _,
    ) as _
}

// TODO(pg-port): parser/parse_coerce.c
unsafe fn coerce_to_specific_type_typmod(
    pstate: *mut ParseState,
    node: *mut Node,
    targetTypeId: Oid,
    targetTypmod: int32,
    constructName: *const c_char,
) -> *mut Node {
    crate::parser::parse_coerce::coerce_to_specific_type_typmod(
        pstate as _,
        node as _,
        targetTypeId as _,
        targetTypmod as _,
        constructName as _,
    ) as _
}

// TODO(pg-port): parser/parse_coerce.c
unsafe fn coerce_type(
    pstate: *mut ParseState,
    node: *mut Node,
    inputTypeId: Oid,
    targetTypeId: Oid,
    targetTypmod: int32,
    ccontext: c_int,
    cformat: c_int,
    location: c_int,
) -> *mut Node {
    crate::parser::parse_coerce::coerce_type(
        pstate as _,
        node as _,
        inputTypeId as _,
        targetTypeId as _,
        targetTypmod as _,
        core::mem::transmute(ccontext),
        core::mem::transmute(cformat),
        location as _,
    ) as _
}

// TODO(pg-port): parser/parse_coerce.c
unsafe fn can_coerce_type(
    nargs: c_int,
    inputTypeIds: *const Oid,
    targetTypeIds: *const Oid,
    ccontext: c_int,
) -> bool {
    crate::parser::parse_coerce::can_coerce_type(
        nargs as _,
        inputTypeIds as _,
        targetTypeIds as _,
        core::mem::transmute(ccontext),
    ) as _
}

// TODO(pg-port): parser/parse_oper.c  select_common_type
pub unsafe fn select_common_type(
    pstate: *mut ParseState,
    exprs: *mut List,
    context: *const c_char,
    which_expr: *mut *mut Node,
) -> Oid {
    crate::parser::parse_coerce::select_common_type(
        pstate as _,
        exprs as _,
        context as _,
        which_expr as _,
    ) as _
}

// TODO(pg-port): parser/parse_oper.c  select_common_typmod
pub unsafe fn select_common_typmod(
    pstate: *mut ParseState,
    exprs: *mut List,
    common_type: Oid,
) -> int32 {
    crate::parser::parse_coerce::select_common_typmod(pstate as _, exprs as _, common_type as _) as _
}

// TODO(pg-port): parser/parse_relation.c  colNameToVar
unsafe fn colNameToVar(
    pstate: *mut ParseState,
    colname: *const c_char,
    localonly: bool,
    location: c_int,
) -> *mut Node {
    crate::parser::parse_relation::colNameToVar(
        pstate as _,
        colname as _,
        localonly as _,
        location as _,
    ) as _
}

// TODO(pg-port): catalog/namespace.c
unsafe fn LookupFuncName(
    funcname: *mut List,
    nargs: c_int,
    argtypes: *const Oid,
    noError: bool,
) -> Oid {
    crate::parser::parse_func::LookupFuncName(
        funcname as _,
        nargs as _,
        argtypes as _,
        noError as _,
    ) as _
}

// TODO(pg-port): catalog/namespace.c
unsafe fn NameListToString(names: *mut List) -> *const c_char {
    crate::catalog::namespace::NameListToString(names as _) as _
}

// TODO(pg-port): catalog/namespace.c
unsafe fn get_opclass_oid(am_id: Oid, opclass_name: *mut List, missing_ok: bool) -> Oid {
    crate::commands::opclasscmds::get_opclass_oid(am_id, opclass_name as _, missing_ok)
}

// TODO(pg-port): utils/cache/lsyscache.c
unsafe fn get_func_rettype(funcid: Oid) -> Oid {
    crate::utils::cache::lsyscache::get_func_rettype(funcid as _) as _
}

// TODO(pg-port): access/tsmapi.h  TsmRoutine / GetTsmRoutine
use crate::access::tsmapi::TsmRoutine;
unsafe fn GetTsmRoutine(handlerOid: Oid) -> *mut TsmRoutine {
    crate::access::tsmapi::GetTsmRoutine(handlerOid as _) as _
}

// TODO(pg-port): catalog/pg_amproc.h  Form_pg_amproc
#[repr(C)]
struct FormData_pg_amproc {
    amprocfamily:    Oid,
    amprocrighttype: Oid,
    amproc:          Oid,
    amprocnum:       c_int,
}
type Form_pg_amproc = *mut FormData_pg_amproc;

// TODO(pg-port): access/htup_details.h GETSTRUCT on HeapTuple -> Form_pg_amproc
unsafe fn GETSTRUCT_amproc(tup: *mut HeapTuple) -> Form_pg_amproc {
    todo!("GETSTRUCT_amproc")
}

// TODO(pg-port): catalog/pg_constraint.c  get_relation_constraint_attnos
unsafe fn get_relation_constraint_attnos(
    relid: Oid,
    conname: *const c_char,
    missing_ok: bool,
    constraintOid: *mut Oid,
) -> *mut Bitmapset {
    crate::catalog::pg_constraint::get_relation_constraint_attnos(
        relid as _,
        conname as _,
        missing_ok as _,
        constraintOid as _,
    ) as _
}

// TODO(pg-port): catalog/catalog.c  IsCatalogRelation
unsafe fn IsCatalogRelation(rel: Relation) -> bool {
    crate::catalog::catalog::IsCatalogRelation(rel as _)
}

// TODO(pg-port): catalog/catalog.c  RelationIsUsedAsCatalogTable
unsafe fn RelationIsUsedAsCatalogTable(rel: Relation) -> bool {
    false
}

// TODO(pg-port): parser/parse_node.c  isLockedRefname
unsafe fn isLockedRefname(pstate: *mut ParseState, refname: *const c_char) -> bool {
    false
}

// TODO(pg-port): parser/parse_jsontable.c
unsafe fn transformJsonTable(
    pstate: *mut ParseState,
    jt: *mut c_void,
) -> *mut ParseNamespaceItem {
    crate::parser::parse_jsontable::transformJsonTable(pstate as _, jt as _) as _
}

// TODO(pg-port): nodes/makefuncs.c  makeGroupingSet
unsafe fn makeGroupingSet(kind: GroupingSetKind, content: *mut List, location: c_int) -> *mut GroupingSet {
    crate::nodes::makefuncs::makeGroupingSet(kind as _, content as _, location as _) as _
}

// TODO(pg-port): nodes/makefuncs.c  makeFuncCall
unsafe fn makeFuncCall(funcname: *mut List, args: *mut List, coerce: c_int, location: c_int) -> *mut FuncCall {
    crate::nodes::makefuncs::makeFuncCall(
        funcname as _,
        args as _,
        core::mem::transmute(coerce),
        location as _,
    ) as _
}

// TODO(pg-port): parser/parse_func.c  FigureColname
unsafe fn FigureColname(node: *mut Node) -> *const c_char {
    crate::parser::parse_target::FigureColname(node as _) as _
}

// TODO(pg-port): utils/cache/lsyscache.c  assign_list_collations
unsafe fn assign_list_collations(pstate: *mut ParseState, exprs: *mut List) {
    crate::parser::parse_collate::assign_list_collations(pstate as _, exprs as _)
}

// TODO(pg-port): utils/cache/lsyscache.c  SystemFuncName
unsafe fn SystemFuncName(name: *const c_char) -> *mut List {
    todo!("SystemFuncName")
}

// TODO(pg-port): parser/parse_type.c  typenameTypeIdAndMod
unsafe fn typenameTypeIdAndMod(
    pstate: *mut ParseState,
    typename_: *mut c_void,
    typeId_p: *mut Oid,
    typmod_p: *mut int32,
) {
    crate::parser::parse_type::typenameTypeIdAndMod(
        pstate as _,
        typename_ as _,
        typeId_p as _,
        typmod_p as _,
    )
}

// TODO(pg-port): parser/parse_node.c  ParseExprKindName
unsafe fn ParseExprKindName(kind: ParseExprKind) -> *const c_char {
    b"unknown\0".as_ptr() as *const c_char
}

// coerce constants (COERCE_IMPLICIT_CAST from CoercionForm::*, COERCE_EXPLICIT_CALL ditto)
const COERCION_IMPLICIT:     c_int = 0;
const COERCION_EXPLICIT:     c_int = 1;

// ONCONFLICT_UPDATE
const ONCONFLICT_UPDATE: c_int = 1;

extern "C" {
    fn strcmp(a: *const c_char, b: *const c_char) -> c_int;
    fn strlen(s: *const c_char) -> usize;
    fn snprintf(buf: *mut c_char, size: usize, fmt: *const c_char, ...) -> c_int;
}

// list_make1_int helper (parsenodes uses integer lists)
unsafe fn list_make1_int(val: c_int) -> *mut List {
    lappend_int(NIL, val)
}

// ---------------------------------------------------------------------------
// Part 1 ends here -- functions begin in part 2
// ---------------------------------------------------------------------------

/*
 * transformFromClause -
 *    Process the FROM clause and add items to the query's range table,
 *    joinlist, and namespace.
 *
 * Note: we assume that the pstate's p_rtable, p_joinlist, and p_namespace
 * lists were initialized to NIL when the pstate was created.
 * We will add onto any entries already present --- this is needed for rule
 * processing, as well as for UPDATE and DELETE.
 */
pub unsafe fn transformFromClause(pstate: *mut ParseState, frmList: *mut List) {
    /*
     * The grammar will have produced a list of RangeVars, RangeSubselects,
     * RangeFunctions, and/or JoinExprs. Transform each one (possibly adding
     * entries to the rtable), check for duplicate refnames, and then add it
     * to the joinlist and namespace.
     *
     * Note we must process the items left-to-right for proper handling of
     * LATERAL references.
     */
    let mut fl_list = frmList;
    let mut lc: *mut crate::nodes::pg_list::ListCell = core::ptr::null_mut();
    foreach!(lc, fl_list, {
        let n: *mut Node = lfirst(current_cell!(lc)) as *mut Node;
        let mut nsitem: *mut ParseNamespaceItem = core::ptr::null_mut();
        let mut namespace: *mut List = core::ptr::null_mut();

        let n = transformFromClauseItem(pstate, n, &mut nsitem, &mut namespace);

        checkNameSpaceConflicts(pstate, (*pstate).p_namespace, namespace);

        /* Mark the new namespace items as visible only to LATERAL */
        setNamespaceLateralState(namespace, true, true);

        (*pstate).p_joinlist = lappend((*pstate).p_joinlist, n as *mut c_void);
        (*pstate).p_namespace = list_concat((*pstate).p_namespace, namespace);
    });

    /*
     * We're done parsing the FROM list, so make all namespace items
     * unconditionally visible.  Note that this will also reset lateral_only
     * for any namespace items that were already present when we were called;
     * but those should have been that way already.
     */
    setNamespaceLateralState((*pstate).p_namespace, false, true);
}

/*
 * setTargetTable
 *    Add the target relation of INSERT/UPDATE/DELETE/MERGE to the range table,
 *    and make the special links to it in the ParseState.
 *
 *    We also open the target relation and acquire a write lock on it.
 *    This must be done before processing the FROM list, in case the target
 *    is also mentioned as a source relation --- we want to be sure to grab
 *    the write lock before any read lock.
 *
 *    If alsoSource is true, add the target to the query's joinlist and
 *    namespace.  For INSERT, we don't want the target to be joined to;
 *    it's a destination of tuples, not a source.  MERGE is actually
 *    both, but we'll add it separately to joinlist and namespace, so
 *    doing nothing (like INSERT) is correct here.  For UPDATE/DELETE,
 *    we do need to scan or join the target.  (NOTE: we do not bother
 *    to check for namespace conflict; we assume that the namespace was
 *    initially empty in these cases.)
 *
 *    Finally, we mark the relation as requiring the permissions specified
 *    by requiredPerms.
 *
 *    Returns the rangetable index of the target relation.
 */
pub unsafe fn setTargetTable(
    pstate: *mut ParseState,
    relation: *mut RangeVar,
    inh: bool,
    alsoSource: bool,
    requiredPerms: AclMode,
) -> c_int {
    let nsitem: *mut ParseNamespaceItem;

    /*
     * ENRs hide tables of the same name, so we need to check for them first.
     * In contrast, CTEs don't hide tables (for this purpose).
     */
    if (*relation).schemaname.is_null()
        && scanNameSpaceForENR(pstate, (*relation).relname)
    {
        ereport!(
            ERROR,
            errmsg!(
                "relation \"{}\" cannot be the target of a modifying statement",
                std::ffi::CStr::from_ptr((*relation).relname).to_string_lossy()
            )
        );
        /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
    }

    /* Close old target; this could only happen for multi-action rules */
    if !(*pstate).p_target_relation.is_null() {
        table_close((*pstate).p_target_relation as *mut crate::utils::rel::RelationData, NoLock);
    }

    /*
     * Open target rel and grab suitable lock (which we will hold till end of
     * transaction).
     *
     * free_parsestate() will eventually do the corresponding table_close(),
     * but *not* release the lock.
     */
    (*pstate).p_target_relation = parserOpenTable(pstate, relation, RowExclusiveLock);

    /*
     * Now build an RTE and a ParseNamespaceItem.
     */
    let nsitem = addRangeTableEntryForRelation(
        pstate,
        (*pstate).p_target_relation,
        RowExclusiveLock,
        (*relation).alias,
        inh,
        false,
    );

    /* remember the RTE/nsitem as being the query target */
    (*pstate).p_target_nsitem = nsitem;

    /*
     * Override addRangeTableEntry's default ACL_SELECT permissions check, and
     * instead mark target table as requiring exactly the specified
     * permissions.
     *
     * If we find an explicit reference to the rel later during parse
     * analysis, we will add the ACL_SELECT bit back again; see
     * markVarForSelectPriv and its callers.
     */
    (*((*nsitem).p_perminfo as *mut RTEPermissionInfo)).requiredPerms = requiredPerms;

    /*
     * If UPDATE/DELETE, add table to joinlist and namespace.
     */
    if alsoSource {
        addNSItemToQuery(pstate, nsitem, true, true, true);
    }

    (*nsitem).p_rtindex
}

/*
 * Extract all not-in-common columns from column lists of a source table
 *
 * src_nscolumns and src_colnames describe the source table.
 *
 * *src_colnos initially contains the column numbers of the already-merged
 * columns.  We add to it the column number of each additional column.
 * Also append to *res_colnames the name of each additional column,
 * append to *res_colvars a Var for each additional column, and copy the
 * columns' nscolumns data into res_nscolumns[] (which is caller-allocated
 * space that had better be big enough).
 *
 * Returns the number of columns added.
 */
unsafe fn extractRemainingColumns(
    pstate: *mut ParseState,
    src_nscolumns: *mut ParseNamespaceColumn,
    src_colnames: *mut List,
    src_colnos: *mut *mut List,
    res_colnames: *mut *mut List,
    res_colvars: *mut *mut List,
    res_nscolumns: *mut ParseNamespaceColumn,
) -> c_int {
    let mut colcount: c_int = 0;
    let mut prevcols: *mut Bitmapset = core::ptr::null_mut();
    let mut attnum: c_int;
    let mut lc: *mut crate::nodes::pg_list::ListCell = core::ptr::null_mut();

    /*
     * While we could just test "list_member_int(*src_colnos, attnum)" to
     * detect already-merged columns in the loop below, that would be O(N^2)
     * for a wide input table.  Instead build a bitmapset of just the merged
     * USING columns, which we won't add to within the main loop.
     */
    let mut colnos_tmp = *src_colnos;
    foreach!(lc, colnos_tmp, {
        prevcols = bms_add_member(prevcols, lfirst_int(current_cell!(lc)));
    });

    attnum = 0;
    let mut cn_tmp = src_colnames;
    foreach!(lc, cn_tmp, {
        let colname: *const c_char = strVal!(lfirst(current_cell!(lc)) as *mut Node);

        attnum += 1;
        /* Non-dropped and not already merged? */
        if *colname != 0 && !bms_is_member(attnum, prevcols) {
            /* Yes, so emit it as next output column */
            *src_colnos = lappend_int(*src_colnos, attnum);
            *res_colnames = lappend(*res_colnames, lfirst(current_cell!(lc)));
            *res_colvars = lappend(
                *res_colvars,
                buildVarFromNSColumn(pstate, src_nscolumns.offset((attnum - 1) as isize))
                    as *mut c_void,
            );
            /* Copy the input relation's nscolumn data for this column */
            *res_nscolumns.offset(colcount as isize) =
                core::ptr::read(src_nscolumns.offset((attnum - 1) as isize));
            colcount += 1;
        }
    });
    colcount
}

/* transformJoinUsingClause()
 *    Build a complete ON clause from a partially-transformed USING list.
 *    We are given lists of nodes representing left and right match columns.
 *    Result is a transformed qualification expression.
 */
unsafe fn transformJoinUsingClause(
    pstate: *mut ParseState,
    leftVars: *mut List,
    rightVars: *mut List,
) -> *mut Node {
    let mut result: *mut Node;
    let mut andargs: *mut List = NIL;
    let mut lvars: *mut crate::nodes::pg_list::ListCell = core::ptr::null_mut();
    let mut rvars: *mut crate::nodes::pg_list::ListCell = core::ptr::null_mut();

    /*
     * We cheat a little bit here by building an untransformed operator tree
     * whose leaves are the already-transformed Vars.  This requires collusion
     * from transformExpr(), which normally could be expected to complain
     * about already-transformed subnodes.  However, this does mean that we
     * have to mark the columns as requiring SELECT privilege for ourselves;
     * transformExpr() won't do it.
     */
    // forboth(lvars, leftVars, rvars, rightVars)
    {
        let mut lv = crate::nodes::pg_list::list_head(leftVars);
        let mut rv = crate::nodes::pg_list::list_head(rightVars);
        while !lv.is_null() && !rv.is_null() {
            let lvar: *mut Var = lfirst(lv) as *mut Var;
            let rvar: *mut Var = lfirst(rv) as *mut Var;
            let e: *mut A_Expr;

            /* Require read access to the join variables */
            markVarForSelectPriv(pstate, lvar);
            markVarForSelectPriv(pstate, rvar);

            /* Now create the lvar = rvar join condition */
            e = makeSimpleA_Expr(
                crate::nodes::parsenodes::A_Expr_Kind::AEXPR_OP,
                b"=\0".as_ptr() as *mut c_char,
                copyObject(lvar) as *mut Node,
                copyObject(rvar) as *mut Node,
                -1,
            );

            /* Prepare to combine into an AND clause, if multiple join columns */
            andargs = lappend(andargs, e as *mut c_void);

            lv = lnext(leftVars, lv);
            rv = lnext(rightVars, rv);
        }
    }

    /* Only need an AND if there's more than one join column */
    if list_length(andargs) == 1 {
        result = linitial(andargs) as *mut Node;
    } else {
        result = makeBoolExpr(BoolExprType::AND_EXPR, andargs, -1) as *mut Node;
    }

    /*
     * Since the references are already Vars, and are certainly from the input
     * relations, we don't have to go through the same pushups that
     * transformJoinOnClause() does.  Just invoke transformExpr() to fix up
     * the operators, and we're done.
     */
    result = transformExpr(pstate, result, EXPR_KIND_JOIN_USING);

    result = coerce_to_boolean(pstate, result, b"JOIN/USING\0".as_ptr() as *const c_char);

    result
}

/* transformJoinOnClause()
 *    Transform the qual conditions for JOIN/ON.
 *    Result is a transformed qualification expression.
 */
unsafe fn transformJoinOnClause(
    pstate: *mut ParseState,
    j: *mut JoinExpr,
    namespace: *mut List,
) -> *mut Node {
    let result: *mut Node;
    let save_namespace: *mut List;

    /*
     * The namespace that the join expression should see is just the two
     * subtrees of the JOIN plus any outer references from upper pstate
     * levels.  Temporarily set this pstate's namespace accordingly.  (We need
     * not check for refname conflicts, because transformFromClauseItem()
     * already did.)  All namespace items are marked visible regardless of
     * LATERAL state.
     */
    setNamespaceLateralState(namespace, false, true);

    save_namespace = (*pstate).p_namespace;
    (*pstate).p_namespace = namespace;

    let result = transformWhereClause(
        pstate,
        (*j).quals,
        EXPR_KIND_JOIN_ON,
        b"JOIN/ON\0".as_ptr() as *const c_char,
    );

    (*pstate).p_namespace = save_namespace;

    result
}

/*
 * transformTableEntry --- transform a RangeVar (simple relation reference)
 */
unsafe fn transformTableEntry(
    pstate: *mut ParseState,
    r: *mut RangeVar,
) -> *mut ParseNamespaceItem {
    /* addRangeTableEntry does all the work */
    addRangeTableEntry(pstate, r, (*r).alias, (*r).inh, true)
}

/*
 * transformRangeSubselect --- transform a sub-SELECT appearing in FROM
 */
unsafe fn transformRangeSubselect(
    pstate: *mut ParseState,
    r: *mut RangeSubselect,
) -> *mut ParseNamespaceItem {
    let query: *mut Query;

    /*
     * Set p_expr_kind to show this parse level is recursing to a subselect.
     * We can't be nested within any expression, so don't need save-restore
     * logic here.
     */
    // Assert(pstate->p_expr_kind == EXPR_KIND_NONE);
    (*pstate).p_expr_kind = EXPR_KIND_FROM_SUBSELECT;

    /*
     * If the subselect is LATERAL, make lateral_only names of this level
     * visible to it.  (LATERAL can't nest within a single pstate level, so we
     * don't need save/restore logic here.)
     */
    // Assert(!pstate->p_lateral_active);
    (*pstate).p_lateral_active = (*r).lateral;

    /*
     * Analyze and transform the subquery.  Note that if the subquery doesn't
     * have an alias, it can't be explicitly selected for locking, but locking
     * might still be required (if there is an all-tables locking clause).
     */
    let aliasname: *const c_char = if (*r).alias.is_null() {
        core::ptr::null()
    } else {
        (*(*r).alias).aliasname
    };
    let query = parse_sub_analyze(
        (*r).subquery,
        pstate,
        core::ptr::null_mut(),
        isLockedRefname(pstate, aliasname),
        true,
    );

    /* Restore state */
    (*pstate).p_lateral_active = false;
    (*pstate).p_expr_kind = EXPR_KIND_NONE;

    /*
     * Check that we got a SELECT.  Anything else should be impossible given
     * restrictions of the grammar, but check anyway.
     */
    if !IsA!(query as *mut Node, T_Query)
        || (*query).commandType != CmdType::CMD_SELECT
    {
        elog!(ERROR, "unexpected non-SELECT command in subquery in FROM");
    }

    /*
     * OK, build an RTE and nsitem for the subquery.
     */
    addRangeTableEntryForSubquery(pstate, query, (*r).alias, (*r).lateral, true)
}

/*
 * transformRangeFunction --- transform a function call appearing in FROM
 */
unsafe fn transformRangeFunction(
    pstate: *mut ParseState,
    r: *mut RangeFunction,
) -> *mut ParseNamespaceItem {
    let mut funcexprs: *mut List = NIL;
    let mut funcnames: *mut List = NIL;
    let mut coldeflists: *mut List = NIL;
    let mut is_lateral: bool;
    let mut lc: *mut crate::nodes::pg_list::ListCell = core::ptr::null_mut();

    /*
     * We make lateral_only names of this level visible, whether or not the
     * RangeFunction is explicitly marked LATERAL.  This is needed for SQL
     * spec compliance in the case of UNNEST(), and seems useful on
     * convenience grounds for all functions in FROM.
     *
     * (LATERAL can't nest within a single pstate level, so we don't need
     * save/restore logic here.)
     */
    // Assert(!pstate->p_lateral_active);
    (*pstate).p_lateral_active = true;

    /*
     * Transform the raw expressions.
     *
     * While transforming, also save function names for possible use as alias
     * and column names.  We use the same transformation rules as for a SELECT
     * output expression.  For a FuncCall node, the result will be the
     * function name, but it is possible for the grammar to hand back other
     * node types.
     *
     * We have to get this info now, because FigureColname only works on raw
     * parsetrees.  Actually deciding what to do with the names is left up to
     * addRangeTableEntryForFunction.
     *
     * Likewise, collect column definition lists if there were any.  But
     * complain if we find one here and the RangeFunction has one too.
     */
    let mut fn_list = (*r).functions;
    foreach!(lc, fn_list, {
        let pair: *mut List = lfirst(current_cell!(lc)) as *mut List;
        let fexpr: *mut Node;
        let coldeflist: *mut List;
        let mut newfexpr: *mut Node;
        let mut last_srf: *mut Node;

        /* Disassemble the function-call/column-def-list pairs */
        // Assert(list_length(pair) == 2);
        fexpr = linitial(pair) as *mut Node;
        coldeflist = lsecond(pair) as *mut List;

        /*
         * If we find a function call unnest() with more than one argument and
         * no special decoration, transform it into separate unnest() calls on
         * each argument.  This is a kluge, for sure, but it's less nasty than
         * other ways of implementing the SQL-standard UNNEST() syntax.
         *
         * If there is any decoration (including a coldeflist), we don't
         * transform, which probably means a no-such-function error later.  We
         * could alternatively throw an error right now, but that doesn't seem
         * tremendously helpful.  If someone is using any such decoration,
         * then they're not using the SQL-standard syntax, and they're more
         * likely expecting an un-tweaked function call.
         *
         * Note: the transformation changes a non-schema-qualified unnest()
         * function name into schema-qualified pg_catalog.unnest().  This
         * choice is also a bit debatable, but it seems reasonable to force
         * use of built-in unnest() when we make this transformation.
         */
        if IsA!(fexpr, T_FuncCall) {
            let fc: *mut FuncCall = fexpr as *mut FuncCall;

            if list_length((*fc).funcname) == 1
                && strcmp(
                    strVal!(linitial((*fc).funcname) as *mut Node),
                    b"unnest\0".as_ptr() as *const c_char,
                ) == 0
                && list_length((*fc).args) > 1
                && (*fc).agg_order.is_null()
                && (*fc).agg_filter.is_null()
                && (*fc).over.is_null()
                && !(*fc).agg_star
                && !(*fc).agg_distinct
                && !(*fc).func_variadic
                && coldeflist.is_null()
            {
                let mut lc2: *mut crate::nodes::pg_list::ListCell = core::ptr::null_mut();
                let mut args_tmp = (*fc).args;
                foreach!(lc2, args_tmp, {
                    let arg: *mut Node = lfirst(current_cell!(lc2)) as *mut Node;
                    let newfc: *mut FuncCall;

                    last_srf = (*pstate).p_last_srf;

                    newfc = makeFuncCall(
                        SystemFuncName(b"unnest\0".as_ptr() as *const c_char),
                        list_make1!(arg as *mut c_void),
                        COERCE_EXPLICIT_CALL as c_int,
                        (*fc).location,
                    );

                    newfexpr = transformExpr(
                        pstate,
                        newfc as *mut Node,
                        EXPR_KIND_FROM_FUNCTION,
                    );

                    /* nodeFunctionscan.c requires SRFs to be at top level */
                    if (*pstate).p_last_srf != last_srf
                        && (*pstate).p_last_srf != newfexpr
                    {
                        ereport!(
                            ERROR,
                            errmsg!(
                                "set-returning functions must appear at top level of FROM"
                            )
                        );
                        /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                           parser_errposition(pstate, exprLocation(pstate->p_last_srf)) */
                    }

                    funcexprs = lappend(funcexprs, newfexpr as *mut c_void);
                    funcnames = lappend(
                        funcnames,
                        FigureColname(newfc as *mut Node) as *mut c_void,
                    );

                    /* coldeflist is empty, so no error is possible */
                    coldeflists = lappend(coldeflists, coldeflist as *mut c_void);
                });
                // continue -- handled by foreach! iterator naturally
            } else {
                // normal case for FuncCall
                last_srf = (*pstate).p_last_srf;
                newfexpr = transformExpr(pstate, fexpr, EXPR_KIND_FROM_FUNCTION);

                /* nodeFunctionscan.c requires SRFs to be at top level */
                if (*pstate).p_last_srf != last_srf && (*pstate).p_last_srf != newfexpr {
                    ereport!(
                        ERROR,
                        errmsg!(
                            "set-returning functions must appear at top level of FROM"
                        )
                    );
                    /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                       parser_errposition(pstate, exprLocation(pstate->p_last_srf)) */
                }

                funcexprs = lappend(funcexprs, newfexpr as *mut c_void);
                funcnames = lappend(funcnames, FigureColname(fexpr) as *mut c_void);

                if !coldeflist.is_null() && !(*r).coldeflist.is_null() {
                    ereport!(
                        ERROR,
                        errmsg!(
                            "multiple column definition lists are not allowed for the same function"
                        )
                    );
                    /* C also: errcode(ERRCODE_SYNTAX_ERROR),
                       parser_errposition(pstate, exprLocation((Node *) r->coldeflist)) */
                }
                coldeflists = lappend(coldeflists, coldeflist as *mut c_void);
            }
        } else {
            /* normal case ... */
            last_srf = (*pstate).p_last_srf;
            newfexpr = transformExpr(pstate, fexpr, EXPR_KIND_FROM_FUNCTION);

            /* nodeFunctionscan.c requires SRFs to be at top level */
            if (*pstate).p_last_srf != last_srf && (*pstate).p_last_srf != newfexpr {
                ereport!(
                    ERROR,
                    errmsg!(
                        "set-returning functions must appear at top level of FROM"
                    )
                );
                /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                   parser_errposition(pstate, exprLocation(pstate->p_last_srf)) */
            }

            funcexprs = lappend(funcexprs, newfexpr as *mut c_void);
            funcnames = lappend(funcnames, FigureColname(fexpr) as *mut c_void);

            if !coldeflist.is_null() && !(*r).coldeflist.is_null() {
                ereport!(
                    ERROR,
                    errmsg!(
                        "multiple column definition lists are not allowed for the same function"
                    )
                );
                /* C also: errcode(ERRCODE_SYNTAX_ERROR) */
            }
            coldeflists = lappend(coldeflists, coldeflist as *mut c_void);
        }
    });

    (*pstate).p_lateral_active = false;

    /*
     * We must assign collations now so that the RTE exposes correct collation
     * info for Vars created from it.
     */
    assign_list_collations(pstate, funcexprs);

    /*
     * Install the top-level coldeflist if there was one (we already checked
     * that there was no conflicting per-function coldeflist).
     *
     * We only allow this when there's a single function (even after UNNEST
     * expansion) and no WITH ORDINALITY.  The reason for the latter
     * restriction is that it's not real clear whether the ordinality column
     * should be in the coldeflist, and users are too likely to make mistakes
     * in one direction or the other.  Putting the coldeflist inside ROWS
     * FROM() is much clearer in this case.
     */
    if !(*r).coldeflist.is_null() {
        if list_length(funcexprs) != 1 {
            if (*r).is_rowsfrom {
                ereport!(
                    ERROR,
                    errmsg!(
                        "ROWS FROM() with multiple functions cannot have a column definition list"
                    )
                );
                /* C also: errhint("Put a separate column definition list for each function inside ROWS FROM()."),
                   errcode(ERRCODE_SYNTAX_ERROR) */
            } else {
                ereport!(
                    ERROR,
                    errmsg!(
                        "UNNEST() with multiple arguments cannot have a column definition list"
                    )
                );
                /* C also: errhint, errcode(ERRCODE_SYNTAX_ERROR) */
            }
        }
        if (*r).ordinality {
            ereport!(
                ERROR,
                errmsg!("WITH ORDINALITY cannot be used with a column definition list")
            );
            /* C also: errhint("Put the column definition list inside ROWS FROM()."),
               errcode(ERRCODE_SYNTAX_ERROR) */
        }

        coldeflists = list_make1!((*r).coldeflist as *mut c_void);
    }

    /*
     * Mark the RTE as LATERAL if the user said LATERAL explicitly, or if
     * there are any lateral cross-references in it.
     */
    is_lateral = (*r).lateral || contain_vars_of_level(funcexprs as *mut Node, 0);

    /*
     * OK, build an RTE and nsitem for the function.
     */
    addRangeTableEntryForFunction(
        pstate, funcnames, funcexprs, coldeflists, r, is_lateral, true,
    )
}

/*
 * transformRangeTableFunc -
 *          Transform a raw RangeTableFunc into TableFunc.
 *
 * Transform the namespace clauses, the document-generating expression, the
 * row-generating expression, the column-generating expressions, and the
 * default value expressions.
 */
unsafe fn transformRangeTableFunc(
    pstate: *mut ParseState,
    rtf: *mut RangeTableFunc,
) -> *mut ParseNamespaceItem {
    let tf: *mut TableFunc = makeNode!(TableFunc, T_TableFunc) as *mut TableFunc;
    let constructName: *const c_char;
    let docType: Oid;
    let mut is_lateral: bool;
    let mut col: *mut crate::nodes::pg_list::ListCell = core::ptr::null_mut();
    let names: *mut *const c_char;
    let mut colno: c_int;

    /*
     * Currently we only support XMLTABLE here.  See transformJsonTable() for
     * JSON_TABLE support.
     */
    (*tf).functype = crate::nodes::primnodes::TableFuncType::TFT_XMLTABLE;
    constructName = b"XMLTABLE\0".as_ptr() as *const c_char;
    docType = XMLOID;

    /*
     * We make lateral_only names of this level visible, whether or not the
     * RangeTableFunc is explicitly marked LATERAL.  This is needed for SQL
     * spec compliance and seems useful on convenience grounds for all
     * functions in FROM.
     *
     * (LATERAL can't nest within a single pstate level, so we don't need
     * save/restore logic here.)
     */
    // Assert(!pstate->p_lateral_active);
    (*pstate).p_lateral_active = true;

    /* Transform and apply typecast to the row-generating expression ... */
    // Assert(rtf->rowexpr != NULL);
    (*tf).rowexpr = coerce_to_specific_type(
        pstate,
        transformExpr(pstate, (*rtf).rowexpr, EXPR_KIND_FROM_FUNCTION),
        TEXTOID,
        constructName,
    );
    assign_expr_collations(pstate, (*tf).rowexpr);

    /* ... and to the document itself */
    // Assert(rtf->docexpr != NULL);
    (*tf).docexpr = coerce_to_specific_type(
        pstate,
        transformExpr(pstate, (*rtf).docexpr, EXPR_KIND_FROM_FUNCTION),
        docType,
        constructName,
    );
    assign_expr_collations(pstate, (*tf).docexpr);

    /* undef ordinality column number */
    (*tf).ordinalitycol = -1;

    /* Process column specs */
    names = palloc(size_of::<*const c_char>() * list_length((*rtf).columns) as usize)
        as *mut *const c_char;

    colno = 0;
    let mut cols_tmp = (*rtf).columns;
    foreach!(col, cols_tmp, {
        let rawc: *mut RangeTableFuncCol = lfirst(current_cell!(col)) as *mut RangeTableFuncCol;
        let mut typid: Oid = InvalidOid;
        let mut typmod: int32 = -1;
        let mut colexpr: *mut Node;
        let mut coldefexpr: *mut Node;
        let mut j: c_int;

        (*tf).colnames = lappend(
            (*tf).colnames,
            makeString(pstrdup((*rawc).colname)) as *mut c_void,
        );

        /*
         * Determine the type and typmod for the new column. FOR ORDINALITY
         * columns are INTEGER per spec; the others are user-specified.
         */
        if (*rawc).for_ordinality {
            if (*tf).ordinalitycol != -1 {
                ereport!(
                    ERROR,
                    errmsg!("only one FOR ORDINALITY column is allowed")
                );
                /* C also: errcode(ERRCODE_SYNTAX_ERROR),
                   parser_errposition(pstate, rawc->location) */
            }

            typid = INT4OID;
            typmod = -1;
            (*tf).ordinalitycol = colno;
        } else {
            if (*(*rawc).typeName).setof {
                ereport!(
                    ERROR,
                    errmsg!(
                        "column \"{}\" cannot be declared SETOF",
                        std::ffi::CStr::from_ptr((*rawc).colname).to_string_lossy()
                    )
                );
                /* C also: errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                   parser_errposition(pstate, rawc->location) */
            }
            typenameTypeIdAndMod(
                pstate,
                (*rawc).typeName as *mut c_void,
                &mut typid,
                &mut typmod,
            );
        }

        (*tf).coltypes = lappend_oid((*tf).coltypes, typid);
        (*tf).coltypmods = lappend_int((*tf).coltypmods, typmod);
        (*tf).colcollations = lappend_oid((*tf).colcollations, get_typcollation(typid));

        /* Transform the PATH and DEFAULT expressions */
        if !(*rawc).colexpr.is_null() {
            colexpr = coerce_to_specific_type(
                pstate,
                transformExpr(pstate, (*rawc).colexpr, EXPR_KIND_FROM_FUNCTION),
                TEXTOID,
                constructName,
            );
            assign_expr_collations(pstate, colexpr);
        } else {
            colexpr = core::ptr::null_mut();
        }

        if !(*rawc).coldefexpr.is_null() {
            coldefexpr = coerce_to_specific_type_typmod(
                pstate,
                transformExpr(pstate, (*rawc).coldefexpr, EXPR_KIND_FROM_FUNCTION),
                typid,
                typmod,
                constructName,
            );
            assign_expr_collations(pstate, coldefexpr);
        } else {
            coldefexpr = core::ptr::null_mut();
        }

        (*tf).colexprs = lappend((*tf).colexprs, colexpr as *mut c_void);
        (*tf).coldefexprs = lappend((*tf).coldefexprs, coldefexpr as *mut c_void);

        if (*rawc).is_not_null {
            (*tf).notnulls = bms_add_member((*tf).notnulls, colno);
        }

        /* make sure column names are unique */
        j = 0;
        while j < colno {
            if strcmp(*names.offset(j as isize), (*rawc).colname) == 0 {
                ereport!(
                    ERROR,
                    errmsg!("column name \"{}\" is not unique", std::ffi::CStr::from_ptr((*rawc).colname).to_string_lossy())
                );
                /* C also: errcode(ERRCODE_SYNTAX_ERROR),
                   parser_errposition(pstate, rawc->location) */
            }
            j += 1;
        }
        *names.offset(colno as isize) = (*rawc).colname;

        colno += 1;
    });
    pfree(names as *mut c_void);

    /* Namespaces, if any, also need to be transformed */
    if !(*rtf).namespaces.is_null() && !crate::nodes::pg_list::list_head((*rtf).namespaces).is_null() {
        let mut ns: *mut crate::nodes::pg_list::ListCell = core::ptr::null_mut();
        let mut lc2: *mut crate::nodes::pg_list::ListCell = core::ptr::null_mut();
        let mut ns_uris: *mut List = NIL;
        let mut ns_names: *mut List = NIL;
        let mut default_ns_seen: bool = false;

        let mut ns_list = (*rtf).namespaces;
        foreach!(ns, ns_list, {
            let res: *mut crate::nodes::parsenodes::ResTarget =
                lfirst(current_cell!(ns)) as *mut crate::nodes::parsenodes::ResTarget;
            let mut ns_uri: *mut Node;

            // Assert(IsA(r, ResTarget));
            ns_uri = transformExpr(pstate, (*res).val, EXPR_KIND_FROM_FUNCTION);
            ns_uri = coerce_to_specific_type(pstate, ns_uri, TEXTOID, constructName);
            assign_expr_collations(pstate, ns_uri);
            ns_uris = lappend(ns_uris, ns_uri as *mut c_void);

            /* Verify consistency of name list: no dupes, only one DEFAULT */
            if !(*res).name.is_null() {
                let mut ns_names_tmp = ns_names;
                foreach!(lc2, ns_names_tmp, {
                    let ns_node: *mut PgString = lfirst(current_cell!(lc2)) as *mut PgString;

                    if ns_node.is_null() {
                        // continue
                    } else if strcmp(
                        strVal!(ns_node as *mut crate::nodes::nodes::Node),
                        (*res).name,
                    ) == 0
                    {
                        ereport!(
                            ERROR,
                            errmsg!(
                                "namespace name \"{}\" is not unique",
                                std::ffi::CStr::from_ptr((*res).name).to_string_lossy()
                            )
                        );
                        /* C also: errcode(ERRCODE_SYNTAX_ERROR),
                           parser_errposition(pstate, r->location) */
                    }
                });
            } else {
                if default_ns_seen {
                    ereport!(
                        ERROR,
                        errmsg!("only one default namespace is allowed")
                    );
                    /* C also: errcode(ERRCODE_SYNTAX_ERROR),
                       parser_errposition(pstate, r->location) */
                }
                default_ns_seen = true;
            }

            /* We represent DEFAULT by a null pointer */
            let ns_name_node: *mut c_void = if !(*res).name.is_null() {
                makeString((*res).name) as *mut c_void
            } else {
                core::ptr::null_mut()
            };
            ns_names = lappend(ns_names, ns_name_node);
        });

        (*tf).ns_uris = ns_uris;
        (*tf).ns_names = ns_names;
    }

    (*tf).location = (*rtf).location;

    (*pstate).p_lateral_active = false;

    /*
     * Mark the RTE as LATERAL if the user said LATERAL explicitly, or if
     * there are any lateral cross-references in it.
     */
    is_lateral = (*rtf).lateral || contain_vars_of_level(tf as *mut Node, 0);

    addRangeTableEntryForTableFunc(pstate, tf, (*rtf).alias, is_lateral, true)
}

/*
 * transformRangeTableSample --- transform a TABLESAMPLE clause
 *
 * Caller has already transformed rts->relation, we just have to validate
 * the remaining fields and create a TableSampleClause node.
 */
unsafe fn transformRangeTableSample(
    pstate: *mut ParseState,
    rts: *mut RangeTableSample,
) -> *mut TableSampleClause {
    let tablesample: *mut TableSampleClause;
    let mut handlerOid: Oid;
    let mut funcargtypes: [Oid; 1] = [INTERNALOID];
    let tsm: *mut TsmRoutine;
    let mut fargs: *mut List;
    let mut larg: *mut crate::nodes::pg_list::ListCell = core::ptr::null_mut();
    let mut ltyp: *mut crate::nodes::pg_list::ListCell = core::ptr::null_mut();

    /*
     * To validate the sample method name, look up the handler function, which
     * has the same name, one dummy INTERNAL argument, and a result type of
     * tsm_handler.  (Note: tablesample method names are not schema-qualified
     * in the SQL standard; but since they are just functions to us, we allow
     * schema qualification to resolve any potential ambiguity.)
     */
    handlerOid = LookupFuncName((*rts).method, 1, funcargtypes.as_ptr(), true);

    /* we want error to complain about no-such-method, not no-such-function */
    if !OidIsValid(handlerOid) {
        ereport!(
            ERROR,
            errmsg!(
                "tablesample method {} does not exist",
                std::ffi::CStr::from_ptr(NameListToString((*rts).method)).to_string_lossy()
            )
        );
        /* C also: errcode(ERRCODE_UNDEFINED_OBJECT),
           parser_errposition(pstate, rts->location) */
    }

    /* check that handler has correct return type */
    if get_func_rettype(handlerOid) != TSM_HANDLEROID {
        ereport!(
            ERROR,
            errmsg!(
                "function {} must return type {}",
                std::ffi::CStr::from_ptr(NameListToString((*rts).method)).to_string_lossy(),
                "tsm_handler"
            )
        );
        /* C also: errcode(ERRCODE_WRONG_OBJECT_TYPE),
           parser_errposition(pstate, rts->location) */
    }

    /* OK, run the handler to get TsmRoutine, for argument type info */
    let tsm = GetTsmRoutine(handlerOid);

    let tablesample = makeNode!(TableSampleClause, T_TableSampleClause) as *mut TableSampleClause;
    (*tablesample).tsmhandler = handlerOid;

    /* check user provided the expected number of arguments */
    if list_length((*rts).args) != list_length((*tsm).parameterTypes) {
        ereport!(
            ERROR,
            errmsg!(
                "tablesample method {} requires {} argument(s), not {}",
                std::ffi::CStr::from_ptr(NameListToString((*rts).method)).to_string_lossy(),
                list_length((*tsm).parameterTypes),
                list_length((*rts).args)
            )
        );
        /* C also: errcode(ERRCODE_INVALID_TABLESAMPLE_ARGUMENT),
           parser_errposition(pstate, rts->location) */
    }

    /*
     * Transform the arguments, typecasting them as needed.  Note we must also
     * assign collations now, because assign_query_collations() doesn't
     * examine any substructure of RTEs.
     */
    fargs = NIL;
    // forboth(larg, rts->args, ltyp, tsm->parameterTypes)
    {
        let mut la = crate::nodes::pg_list::list_head((*rts).args);
        let mut lt = crate::nodes::pg_list::list_head((*tsm).parameterTypes);
        while !la.is_null() && !lt.is_null() {
            let mut arg: *mut Node = lfirst(la) as *mut Node;
            let argtype: Oid = lfirst_oid(lt);

            arg = transformExpr(pstate, arg, EXPR_KIND_FROM_FUNCTION);
            arg = coerce_to_specific_type(
                pstate,
                arg,
                argtype,
                b"TABLESAMPLE\0".as_ptr() as *const c_char,
            );
            assign_expr_collations(pstate, arg);
            fargs = lappend(fargs, arg as *mut c_void);

            la = lnext((*rts).args, la);
            lt = lnext((*tsm).parameterTypes, lt);
        }
    }
    (*tablesample).args = fargs;

    /* Process REPEATABLE (seed) */
    if !(*rts).repeatable.is_null() {
        let mut arg: *mut Node;

        if !(*tsm).repeatable_across_queries {
            ereport!(
                ERROR,
                errmsg!(
                    "tablesample method {} does not support REPEATABLE",
                    std::ffi::CStr::from_ptr(NameListToString((*rts).method)).to_string_lossy()
                )
            );
            /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
               parser_errposition(pstate, rts->location) */
        }

        arg = transformExpr(pstate, (*rts).repeatable, EXPR_KIND_FROM_FUNCTION);
        arg = coerce_to_specific_type(
            pstate,
            arg,
            FLOAT8OID,
            b"REPEATABLE\0".as_ptr() as *const c_char,
        );
        assign_expr_collations(pstate, arg);
        (*tablesample).repeatable = arg as *mut Expr;
    } else {
        (*tablesample).repeatable = core::ptr::null_mut();
    }

    tablesample
}

/*
 * getNSItemForSpecialRelationTypes
 *
 * If given RangeVar refers to a CTE or an EphemeralNamedRelation,
 * build and return an appropriate ParseNamespaceItem, otherwise return NULL
 */
unsafe fn getNSItemForSpecialRelationTypes(
    pstate: *mut ParseState,
    rv: *mut RangeVar,
) -> *mut ParseNamespaceItem {
    let nsitem: *mut ParseNamespaceItem;
    let cte: *mut CommonTableExpr;
    let mut levelsup: Index = 0;

    /*
     * if it is a qualified name, it can't be a CTE or tuplestore reference
     */
    if !(*rv).schemaname.is_null() {
        return core::ptr::null_mut();
    }

    let cte = scanNameSpaceForCTE(pstate, (*rv).relname, &mut levelsup);
    if !cte.is_null() {
        let nsitem = addRangeTableEntryForCTE(pstate, cte, levelsup, rv, true);
        nsitem
    } else if scanNameSpaceForENR(pstate, (*rv).relname) {
        let nsitem = addRangeTableEntryForENR(pstate, rv, true);
        nsitem
    } else {
        core::ptr::null_mut()
    }
}

/*
 * transformFromClauseItem -
 *    Transform a FROM-clause item, adding any required entries to the
 *    range table list being built in the ParseState, and return the
 *    transformed item ready to include in the joinlist.  Also build a
 *    ParseNamespaceItem list describing the names exposed by this item.
 *    This routine can recurse to handle SQL92 JOIN expressions.
 *
 * The function return value is the node to add to the jointree (a
 * RangeTblRef or JoinExpr).  Additional output parameters are:
 *
 * *top_nsitem: receives the ParseNamespaceItem directly corresponding to the
 * jointree item.  (This is only used during internal recursion, not by
 * outside callers.)
 *
 * *namespace: receives a List of ParseNamespaceItems for the RTEs exposed
 * as table/column names by this item.  (The lateral_only flags in these items
 * are indeterminate and should be explicitly set by the caller before use.)
 */
unsafe fn transformFromClauseItem(
    pstate: *mut ParseState,
    n: *mut Node,
    top_nsitem: *mut *mut ParseNamespaceItem,
    namespace: *mut *mut List,
) -> *mut Node {
    /* Guard against stack overflow due to overly deep subtree */
    check_stack_depth();

    if IsA!(n, T_RangeVar) {
        /* Plain relation reference, or perhaps a CTE reference */
        let rv: *mut RangeVar = n as *mut RangeVar;
        let rtr: *mut RangeTblRef;
        let mut nsitem: *mut ParseNamespaceItem;

        /* Check if it's a CTE or tuplestore reference */
        nsitem = getNSItemForSpecialRelationTypes(pstate, rv);

        /* if not found above, must be a table reference */
        if nsitem.is_null() {
            nsitem = transformTableEntry(pstate, rv);
        }

        *top_nsitem = nsitem;
        *namespace = list_make1!(nsitem as *mut c_void);
        let rtr = makeNode!(RangeTblRef, T_RangeTblRef) as *mut RangeTblRef;
        (*rtr).rtindex = (*nsitem).p_rtindex;
        return rtr as *mut Node;
    } else if IsA!(n, T_RangeSubselect) {
        /* sub-SELECT is like a plain relation */
        let rtr: *mut RangeTblRef;
        let nsitem: *mut ParseNamespaceItem;

        let nsitem = transformRangeSubselect(pstate, n as *mut RangeSubselect);
        *top_nsitem = nsitem;
        *namespace = list_make1!(nsitem as *mut c_void);
        let rtr = makeNode!(RangeTblRef, T_RangeTblRef) as *mut RangeTblRef;
        (*rtr).rtindex = (*nsitem).p_rtindex;
        return rtr as *mut Node;
    } else if IsA!(n, T_RangeFunction) {
        /* function is like a plain relation */
        let rtr: *mut RangeTblRef;
        let nsitem: *mut ParseNamespaceItem;

        let nsitem = transformRangeFunction(pstate, n as *mut RangeFunction);
        *top_nsitem = nsitem;
        *namespace = list_make1!(nsitem as *mut c_void);
        let rtr = makeNode!(RangeTblRef, T_RangeTblRef) as *mut RangeTblRef;
        (*rtr).rtindex = (*nsitem).p_rtindex;
        return rtr as *mut Node;
    } else if IsA!(n, T_RangeTableFunc) || IsA!(n, T_JsonTable) {
        /* table function is like a plain relation */
        let rtr: *mut RangeTblRef;
        let nsitem: *mut ParseNamespaceItem;

        let nsitem = if IsA!(n, T_JsonTable) {
            transformJsonTable(pstate, n as *mut c_void)
        } else {
            transformRangeTableFunc(pstate, n as *mut RangeTableFunc)
        };

        *top_nsitem = nsitem;
        *namespace = list_make1!(nsitem as *mut c_void);
        let rtr = makeNode!(RangeTblRef, T_RangeTblRef) as *mut RangeTblRef;
        (*rtr).rtindex = (*nsitem).p_rtindex;
        return rtr as *mut Node;
    } else if IsA!(n, T_RangeTableSample) {
        /* TABLESAMPLE clause (wrapping some other valid FROM node) */
        let rts: *mut RangeTableSample = n as *mut RangeTableSample;
        let rel: *mut Node;
        let rte: *mut RangeTblEntry;

        /* Recursively transform the contained relation */
        let rel = transformFromClauseItem(pstate, (*rts).relation, top_nsitem, namespace);
        let rte = (**top_nsitem).p_rte as *mut RangeTblEntry;
        /* We only support this on plain relations and matviews */
        if (*rte).rtekind != RTE_RELATION
            || ((*rte).relkind != RELKIND_RELATION
                && (*rte).relkind != RELKIND_MATVIEW
                && (*rte).relkind != RELKIND_PARTITIONED_TABLE)
        {
            ereport!(
                ERROR,
                errmsg!(
                    "TABLESAMPLE clause can only be applied to tables and materialized views"
                )
            );
            /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
               parser_errposition(pstate, exprLocation(rts->relation)) */
        }

        /* Transform TABLESAMPLE details and attach to the RTE */
        (*rte).tablesample = transformRangeTableSample(pstate, rts);
        return rel;
    } else if IsA!(n, T_JoinExpr) {
        /* A newfangled join expression */
        let j: *mut JoinExpr = n as *mut JoinExpr;
        let mut nsitem: *mut ParseNamespaceItem = core::ptr::null_mut();
        let mut l_nsitem: *mut ParseNamespaceItem = core::ptr::null_mut();
        let mut r_nsitem: *mut ParseNamespaceItem = core::ptr::null_mut();
        let mut l_namespace: *mut List = core::ptr::null_mut();
        let mut r_namespace: *mut List = core::ptr::null_mut();
        let mut my_namespace: *mut List;
        let mut l_colnames: *mut List;
        let mut r_colnames: *mut List;
        let mut res_colnames: *mut List = NIL;
        let mut l_colnos: *mut List = NIL;
        let mut r_colnos: *mut List = NIL;
        let mut res_colvars: *mut List = NIL;
        let l_nscolumns: *mut ParseNamespaceColumn;
        let r_nscolumns: *mut ParseNamespaceColumn;
        let res_nscolumns: *mut ParseNamespaceColumn;
        let mut res_colindex: c_int;
        let lateral_ok: bool;
        let sv_namespace_length: c_int;
        let mut k: c_int;

        /*
         * Recursively process the left subtree, then the right.  We must do
         * it in this order for correct visibility of LATERAL references.
         */
        (*j).larg = transformFromClauseItem(pstate, (*j).larg, &mut l_nsitem, &mut l_namespace);

        /*
         * Make the left-side RTEs available for LATERAL access within the
         * right side, by temporarily adding them to the pstate's namespace
         * list.  Per SQL:2008, if the join type is not INNER or LEFT then the
         * left-side names must still be exposed, but it's an error to
         * reference them.  (Stupid design, but that's what it says.)  Hence,
         * we always push them into the namespace, but mark them as not
         * lateral_ok if the jointype is wrong.
         *
         * Notice that we don't require the merged namespace list to be
         * conflict-free.  See the comments for scanNameSpaceForRefname().
         */
        lateral_ok = matches!((*j).jointype, JoinType::JOIN_INNER | JoinType::JOIN_LEFT);
        setNamespaceLateralState(l_namespace, true, lateral_ok);

        sv_namespace_length = list_length((*pstate).p_namespace);
        (*pstate).p_namespace = list_concat((*pstate).p_namespace, l_namespace);

        /* And now we can process the RHS */
        (*j).rarg = transformFromClauseItem(pstate, (*j).rarg, &mut r_nsitem, &mut r_namespace);

        /* Remove the left-side RTEs from the namespace list again */
        (*pstate).p_namespace = list_truncate((*pstate).p_namespace, sv_namespace_length);

        /*
         * Check for conflicting refnames in left and right subtrees. Must do
         * this because higher levels will assume I hand back a self-
         * consistent namespace list.
         */
        checkNameSpaceConflicts(pstate, l_namespace, r_namespace);

        /*
         * Generate combined namespace info for possible use below.
         */
        my_namespace = list_concat(l_namespace, r_namespace);

        /*
         * We'll work from the nscolumns data and eref alias column names for
         * each of the input nsitems.  Note that these include dropped
         * columns, which is helpful because we can keep track of physical
         * input column numbers more easily.
         */
        l_nscolumns = (*l_nsitem).p_nscolumns;
        l_colnames = (*((*l_nsitem).p_names as *mut Alias)).colnames;
        r_nscolumns = (*r_nsitem).p_nscolumns;
        r_colnames = (*((*r_nsitem).p_names as *mut Alias)).colnames;

        /*
         * Natural join does not explicitly specify columns; must generate
         * columns to join. Need to run through the list of columns from each
         * table or join result and match up the column names. Use the first
         * table, and check every column in the second table for a match.
         * (We'll check that the matches were unique later on.) The result of
         * this step is a list of column names just like an explicitly-written
         * USING list.
         */
        if (*j).isNatural {
            let mut rlist: *mut List = NIL;
            let mut lx: *mut crate::nodes::pg_list::ListCell = core::ptr::null_mut();
            let mut rx: *mut crate::nodes::pg_list::ListCell = core::ptr::null_mut();

            // Assert(j->usingClause == NIL); /* shouldn't have USING() too */

            let mut lc_tmp = l_colnames;
            foreach!(lx, lc_tmp, {
                let l_colname: *const c_char = strVal!(lfirst(current_cell!(lx)) as *mut Node);
                let mut m_name: *mut PgString = core::ptr::null_mut();

                if *l_colname == 0 {
                    // continue -- ignore dropped columns
                } else {
                    let mut rc_tmp = r_colnames;
                    foreach!(rx, rc_tmp, {
                        let r_colname: *const c_char = strVal!(lfirst(current_cell!(rx)) as *mut Node);

                        if strcmp(l_colname, r_colname) == 0 {
                            m_name = makeString(l_colname as *mut c_char);
                            // break -- inner foreach ends at cell exhaustion
                        }
                    });

                    /* matched a right column? then keep as join column... */
                    if !m_name.is_null() {
                        rlist = lappend(rlist, m_name as *mut c_void);
                    }
                }
            });

            (*j).usingClause = rlist;
        }

        /*
         * If a USING clause alias was specified, save the USING columns as
         * its column list.
         */
        if !(*j).join_using_alias.is_null() {
            (*(*j).join_using_alias).colnames = (*j).usingClause;
        }

        /*
         * Now transform the join qualifications, if any.
         */
        l_colnos = NIL;
        r_colnos = NIL;
        res_colnames = NIL;
        res_colvars = NIL;

        /* this may be larger than needed, but it's not worth being exact */
        let total_cols = (list_length(l_colnames) + list_length(r_colnames)) as usize;
        res_nscolumns = palloc0(total_cols * size_of::<ParseNamespaceColumn>())
            as *mut ParseNamespaceColumn;
        res_colindex = 0;

        if !(*j).usingClause.is_null() && list_length((*j).usingClause) > 0 {
            /*
             * JOIN/USING (or NATURAL JOIN, as transformed above). Transform
             * the list into an explicit ON-condition.
             */
            let ucols: *mut List = (*j).usingClause;
            let mut l_usingvars: *mut List = NIL;
            let mut r_usingvars: *mut List = NIL;
            let mut ucol: *mut crate::nodes::pg_list::ListCell = core::ptr::null_mut();

            // Assert(j->quals == NULL); /* shouldn't have ON() too */

            let mut uc_tmp = ucols;
            foreach!(ucol, uc_tmp, {
                let u_colname: *const c_char = strVal!(lfirst(current_cell!(ucol)) as *mut Node);
                let mut col: *mut crate::nodes::pg_list::ListCell = core::ptr::null_mut();
                let mut ndx: c_int;
                let mut l_index: c_int = -1;
                let mut r_index: c_int = -1;
                let l_colvar: *mut Var;
                let r_colvar: *mut Var;

                // Assert(u_colname[0] != '\0');

                /* Check for USING(foo,foo) */
                let mut rc_tmp = res_colnames;
                foreach!(col, rc_tmp, {
                    let res_colname: *const c_char = strVal!(lfirst(current_cell!(col)) as *mut Node);

                    if strcmp(res_colname, u_colname) == 0 {
                        ereport!(
                            ERROR,
                            errmsg!(
                                "column name \"{}\" appears more than once in USING clause",
                                std::ffi::CStr::from_ptr(u_colname).to_string_lossy()
                            )
                        );
                        /* C also: errcode(ERRCODE_DUPLICATE_COLUMN) */
                    }
                });

                /* Find it in left input */
                ndx = 0;
                let mut lc_tmp = l_colnames;
                foreach!(col, lc_tmp, {
                    let l_colname2: *const c_char = strVal!(lfirst(current_cell!(col)) as *mut Node);

                    if strcmp(l_colname2, u_colname) == 0 {
                        if l_index >= 0 {
                            ereport!(
                                ERROR,
                                errmsg!(
                                    "common column name \"{}\" appears more than once in left table",
                                    std::ffi::CStr::from_ptr(u_colname).to_string_lossy()
                                )
                            );
                            /* C also: errcode(ERRCODE_AMBIGUOUS_COLUMN) */
                        }
                        l_index = ndx;
                    }
                    ndx += 1;
                });
                if l_index < 0 {
                    ereport!(
                        ERROR,
                        errmsg!(
                            "column \"{}\" specified in USING clause does not exist in left table",
                            std::ffi::CStr::from_ptr(u_colname).to_string_lossy()
                        )
                    );
                    /* C also: errcode(ERRCODE_UNDEFINED_COLUMN) */
                }
                l_colnos = lappend_int(l_colnos, l_index + 1);

                /* Find it in right input */
                ndx = 0;
                let mut rc_tmp2 = r_colnames;
                foreach!(col, rc_tmp2, {
                    let r_colname2: *const c_char = strVal!(lfirst(current_cell!(col)) as *mut Node);

                    if strcmp(r_colname2, u_colname) == 0 {
                        if r_index >= 0 {
                            ereport!(
                                ERROR,
                                errmsg!(
                                    "common column name \"{}\" appears more than once in right table",
                                    std::ffi::CStr::from_ptr(u_colname).to_string_lossy()
                                )
                            );
                            /* C also: errcode(ERRCODE_AMBIGUOUS_COLUMN) */
                        }
                        r_index = ndx;
                    }
                    ndx += 1;
                });
                if r_index < 0 {
                    ereport!(
                        ERROR,
                        errmsg!(
                            "column \"{}\" specified in USING clause does not exist in right table",
                            std::ffi::CStr::from_ptr(u_colname).to_string_lossy()
                        )
                    );
                    /* C also: errcode(ERRCODE_UNDEFINED_COLUMN) */
                }
                r_colnos = lappend_int(r_colnos, r_index + 1);

                /* Build Vars to use in the generated JOIN ON clause */
                let l_colvar = buildVarFromNSColumn(pstate, l_nscolumns.offset(l_index as isize));
                l_usingvars = lappend(l_usingvars, l_colvar as *mut c_void);
                let r_colvar = buildVarFromNSColumn(pstate, r_nscolumns.offset(r_index as isize));
                r_usingvars = lappend(r_usingvars, r_colvar as *mut c_void);

                /*
                 * While we're here, add column names to the res_colnames
                 * list.  It's a bit ugly to do this here while the
                 * corresponding res_colvars entries are not made till later,
                 * but doing this later would require an additional traversal
                 * of the usingClause list.
                 */
                res_colnames = lappend(res_colnames, lfirst(current_cell!(ucol)));
            });

            /* Construct the generated JOIN ON clause */
            (*j).quals = transformJoinUsingClause(pstate, l_usingvars, r_usingvars);
        } else if !(*j).quals.is_null() {
            /* User-written ON-condition; transform it */
            (*j).quals = transformJoinOnClause(pstate, j, my_namespace);
        }
        /* else CROSS JOIN: no quals */

        /*
         * If this is an outer join, now mark the appropriate child RTEs as
         * being nulled by this join.  We have finished processing the child
         * join expressions as well as the current join's quals, which deal in
         * non-nulled input columns.  All future references to those RTEs will
         * see possibly-nulled values, and we should mark generated Vars to
         * account for that.  In particular, the join alias Vars that we're
         * about to build should reflect the nulling effects of this join.
         *
         * A difficulty with doing this is that we need the join's RT index,
         * which we don't officially have yet.  However, no other RTE can get
         * made between here and the addRangeTableEntryForJoin call, so we can
         * predict what the assignment will be.
         */
        (*j).rtindex = list_length((*pstate).p_rtable) + 1;

        match (*j).jointype {
            JoinType::JOIN_INNER => {
                /* nothing to do */
            }
            JoinType::JOIN_LEFT => {
                markRelsAsNulledBy(pstate, (*j).rarg, (*j).rtindex);
            }
            JoinType::JOIN_FULL => {
                markRelsAsNulledBy(pstate, (*j).larg, (*j).rtindex);
                markRelsAsNulledBy(pstate, (*j).rarg, (*j).rtindex);
            }
            JoinType::JOIN_RIGHT => {
                markRelsAsNulledBy(pstate, (*j).larg, (*j).rtindex);
            }
            _ => {
                elog!(ERROR, "unrecognized join type: {}", (*j).jointype as c_int);
            }
        }

        /*
         * Now we can construct join alias expressions for the USING columns.
         */
        if !(*j).usingClause.is_null() && list_length((*j).usingClause) > 0 {
            let mut lc1: *mut crate::nodes::pg_list::ListCell = core::ptr::null_mut();
            let mut lc2: *mut crate::nodes::pg_list::ListCell = core::ptr::null_mut();

            /* Scan the colnos lists to recover info from the previous loop */
            // forboth(lc1, l_colnos, lc2, r_colnos)
            {
                let mut la = crate::nodes::pg_list::list_head(l_colnos);
                let mut lb = crate::nodes::pg_list::list_head(r_colnos);
                while !la.is_null() && !lb.is_null() {
                    let l_index: c_int = lfirst_int(la) - 1;
                    let r_index: c_int = lfirst_int(lb) - 1;
                    let l_colvar2: *mut Var;
                    let r_colvar2: *mut Var;
                    let u_colvar: *mut Node;
                    let res_nscolumn: *mut ParseNamespaceColumn;

                    /*
                     * Note we re-build these Vars: they might have different
                     * varnullingrels than the ones made in the previous loop.
                     */
                    let l_colvar2 =
                        buildVarFromNSColumn(pstate, l_nscolumns.offset(l_index as isize));
                    let r_colvar2 =
                        buildVarFromNSColumn(pstate, r_nscolumns.offset(r_index as isize));

                    /* Construct the join alias Var for this column */
                    let u_colvar = buildMergedJoinVar(pstate, (*j).jointype, l_colvar2, r_colvar2);
                    res_colvars = lappend(res_colvars, u_colvar as *mut c_void);

                    /* Construct column's res_nscolumns[] entry */
                    let res_nscolumn = res_nscolumns.offset(res_colindex as isize);
                    res_colindex += 1;
                    if u_colvar == l_colvar2 as *mut Node {
                        /* Merged column is equivalent to left input */
                        *res_nscolumn = core::ptr::read(l_nscolumns.offset(l_index as isize));
                    } else if u_colvar == r_colvar2 as *mut Node {
                        /* Merged column is equivalent to right input */
                        *res_nscolumn = core::ptr::read(r_nscolumns.offset(r_index as isize));
                    } else {
                        /*
                         * Merged column is not semantically equivalent to either
                         * input, so it needs to be referenced as the join output
                         * column.
                         */
                        (*res_nscolumn).p_varno = (*j).rtindex as u32;
                        (*res_nscolumn).p_varattno = res_colindex as i16;
                        (*res_nscolumn).p_vartype = exprType(u_colvar);
                        (*res_nscolumn).p_vartypmod = exprTypmod(u_colvar);
                        (*res_nscolumn).p_varcollid = exprCollation(u_colvar);
                        (*res_nscolumn).p_varnosyn = (*j).rtindex as u32;
                        (*res_nscolumn).p_varattnosyn = res_colindex as i16;
                    }

                    la = lnext(l_colnos, la);
                    lb = lnext(r_colnos, lb);
                }
            }
        }

        /* Add remaining columns from each side to the output columns */
        res_colindex += extractRemainingColumns(
            pstate,
            l_nscolumns,
            l_colnames,
            &mut l_colnos,
            &mut res_colnames,
            &mut res_colvars,
            res_nscolumns.offset(res_colindex as isize),
        );
        res_colindex += extractRemainingColumns(
            pstate,
            r_nscolumns,
            r_colnames,
            &mut r_colnos,
            &mut res_colnames,
            &mut res_colvars,
            res_nscolumns.offset(res_colindex as isize),
        );

        /* If join has an alias, it syntactically hides all inputs */
        if !(*j).alias.is_null() {
            k = 0;
            while k < res_colindex {
                let nscol = res_nscolumns.offset(k as isize);
                (*nscol).p_varnosyn = (*j).rtindex as u32;
                (*nscol).p_varattnosyn = (k + 1) as i16;
                k += 1;
            }
        }

        /*
         * Now build an RTE and nsitem for the result of the join.
         */
        let nsitem = addRangeTableEntryForJoin(
            pstate,
            res_colnames,
            res_nscolumns,
            (*j).jointype,
            list_length((*j).usingClause),
            res_colvars,
            l_colnos,
            r_colnos,
            (*j).join_using_alias,
            (*j).alias,
            true,
        );

        /* Verify that we correctly predicted the join's RT index */
        // Assert(j->rtindex == nsitem->p_rtindex);
        /* Cross-check number of columns, too */
        // Assert(res_colindex == list_length(nsitem->p_names->colnames));

        /*
         * Save a link to the JoinExpr in the proper element of p_joinexprs.
         * Since we maintain that list lazily, it may be necessary to fill in
         * empty entries before we can add the JoinExpr in the right place.
         */
        k = list_length((*pstate).p_joinexprs) + 1;
        while k < (*j).rtindex {
            (*pstate).p_joinexprs = lappend((*pstate).p_joinexprs, core::ptr::null_mut());
            k += 1;
        }
        (*pstate).p_joinexprs = lappend((*pstate).p_joinexprs, j as *mut c_void);
        // Assert(list_length(pstate->p_joinexprs) == j->rtindex);

        /*
         * If the join has a USING alias, build a ParseNamespaceItem for that
         * and add it to the list of nsitems in the join's input.
         */
        if !(*j).join_using_alias.is_null() {
            let jnsitem = palloc(size_of::<ParseNamespaceItem>()) as *mut ParseNamespaceItem;
            (*jnsitem).p_names = (*j).join_using_alias as *mut c_void;
            (*jnsitem).p_rte = (*nsitem).p_rte;
            (*jnsitem).p_rtindex = (*nsitem).p_rtindex;
            (*jnsitem).p_perminfo = core::ptr::null_mut();
            /* no need to copy the first N columns, just use res_nscolumns */
            (*jnsitem).p_nscolumns = res_nscolumns;
            /* set default visibility flags; might get changed later */
            (*jnsitem).p_rel_visible = true;
            (*jnsitem).p_cols_visible = true;
            (*jnsitem).p_lateral_only = false;
            (*jnsitem).p_lateral_ok = true;
            (*jnsitem).p_returning_type = VAR_RETURNING_DEFAULT as i32;
            /* Per SQL, we must check for alias conflicts */
            checkNameSpaceConflicts(pstate, list_make1!(jnsitem as *mut c_void), my_namespace);
            my_namespace = lappend(my_namespace, jnsitem as *mut c_void);
        }

        /*
         * Prepare returned namespace list.  If the JOIN has an alias then it
         * hides the contained RTEs completely; otherwise, the contained RTEs
         * are still visible as table names, but are not visible for
         * unqualified column-name access.
         *
         * Note: if there are nested alias-less JOINs, the lower-level ones
         * will remain in the list although they have neither p_rel_visible
         * nor p_cols_visible set.  We could delete such list items, but it's
         * unclear that it's worth expending cycles to do so.
         */
        if !(*j).alias.is_null() {
            my_namespace = NIL;
        } else {
            setNamespaceColumnVisibility(my_namespace, false);
        }

        /*
         * The join RTE itself is always made visible for unqualified column
         * names.  It's visible as a relation name only if it has an alias.
         */
        (*nsitem).p_rel_visible = !(*j).alias.is_null();
        (*nsitem).p_cols_visible = true;
        (*nsitem).p_lateral_only = false;
        (*nsitem).p_lateral_ok = true;

        *top_nsitem = nsitem;
        *namespace = lappend(my_namespace, nsitem as *mut c_void);

        return j as *mut Node;
    } else {
        elog!(ERROR, "unrecognized node type: {}", nodeTag(n) as c_int);
        return core::ptr::null_mut(); /* can't get here, keep compiler quiet */
    }
}

/*
 * buildVarFromNSColumn -
 *    build a Var node using ParseNamespaceColumn data
 *
 * This is used to construct joinaliasvars entries.
 * We can assume varlevelsup should be 0, and no location is specified.
 * Note also that no column SELECT privilege is requested here; that would
 * happen only if the column is actually referenced in the query.
 */
unsafe fn buildVarFromNSColumn(
    pstate: *mut ParseState,
    nscol: *mut ParseNamespaceColumn,
) -> *mut Var {
    // Assert(nscol->p_varno > 0); /* i.e., not deleted column */
    let var = makeVar(
        (*nscol).p_varno as c_int,
        (*nscol).p_varattno,
        (*nscol).p_vartype,
        (*nscol).p_vartypmod,
        (*nscol).p_varcollid,
        0,
    );
    /* makeVar doesn't offer parameters for these, so set by hand: */
    (*var).varreturningtype = core::mem::transmute((*nscol).p_varreturningtype);
    (*var).varnosyn = (*nscol).p_varnosyn;
    (*var).varattnosyn = (*nscol).p_varattnosyn;

    /* ... and update varnullingrels */
    markNullableIfNeeded(pstate, var);

    var
}

/*
 * buildMergedJoinVar -
 *    generate a suitable replacement expression for a merged join column
 */
unsafe fn buildMergedJoinVar(
    pstate: *mut ParseState,
    jointype: JoinType,
    l_colvar: *mut Var,
    r_colvar: *mut Var,
) -> *mut Node {
    let mut outcoltype: Oid;
    let mut outcoltypmod: int32;
    let l_node: *mut Node;
    let r_node: *mut Node;
    let res_node: *mut Node;

    let l_list = list_make2!(l_colvar as *mut c_void, r_colvar as *mut c_void);
    let outcoltype = select_common_type(
        pstate,
        l_list,
        b"JOIN/USING\0".as_ptr() as *const c_char,
        core::ptr::null_mut(),
    );
    let outcoltypmod = select_common_typmod(pstate, l_list, outcoltype);

    /*
     * Insert coercion functions if needed.  Note that a difference in typmod
     * can only happen if input has typmod but outcoltypmod is -1. In that
     * case we insert a RelabelType to clearly mark that result's typmod is
     * not same as input.  We never need coerce_type_typmod.
     */
    let l_node: *mut Node = if (*l_colvar).vartype != outcoltype {
        coerce_type(
            pstate,
            l_colvar as *mut Node,
            (*l_colvar).vartype,
            outcoltype,
            outcoltypmod,
            COERCION_IMPLICIT,
            COERCE_IMPLICIT_CAST as c_int,
            -1,
        )
    } else if (*l_colvar).vartypmod != outcoltypmod {
        makeRelabelType(
            l_colvar as *mut Expr,
            outcoltype,
            outcoltypmod,
            InvalidOid, /* fixed below */
            COERCE_IMPLICIT_CAST,
        ) as *mut Node
    } else {
        l_colvar as *mut Node
    };

    let r_node: *mut Node = if (*r_colvar).vartype != outcoltype {
        coerce_type(
            pstate,
            r_colvar as *mut Node,
            (*r_colvar).vartype,
            outcoltype,
            outcoltypmod,
            COERCION_IMPLICIT,
            COERCE_IMPLICIT_CAST as c_int,
            -1,
        )
    } else if (*r_colvar).vartypmod != outcoltypmod {
        makeRelabelType(
            r_colvar as *mut Expr,
            outcoltype,
            outcoltypmod,
            InvalidOid, /* fixed below */
            COERCE_IMPLICIT_CAST,
        ) as *mut Node
    } else {
        r_colvar as *mut Node
    };

    /*
     * Choose what to emit
     */
    let res_node: *mut Node = match jointype {
        JoinType::JOIN_INNER => {
            /*
             * We can use either var; prefer non-coerced one if available.
             */
            if IsA!(l_node, T_Var) {
                l_node
            } else if IsA!(r_node, T_Var) {
                r_node
            } else {
                l_node
            }
        }
        JoinType::JOIN_LEFT => {
            /* Always use left var */
            l_node
        }
        JoinType::JOIN_RIGHT => {
            /* Always use right var */
            r_node
        }
        JoinType::JOIN_FULL => {
            /*
             * Here we must build a COALESCE expression to ensure that the
             * join output is non-null if either input is.
             */
            let c = makeNode!(CoalesceExpr, T_CoalesceExpr) as *mut CoalesceExpr;
            (*c).coalescetype = outcoltype;
            /* coalescecollid will get set below */
            (*c).args = list_make2!(l_node as *mut c_void, r_node as *mut c_void);
            (*c).location = -1;
            c as *mut Node
        }
        _ => {
            elog!(ERROR, "unrecognized join type: {}", jointype as c_int);
            core::ptr::null_mut() /* keep compiler quiet */
        }
    };

    /*
     * Apply assign_expr_collations to fix up the collation info in the
     * coercion and CoalesceExpr nodes, if we made any.  This must be done now
     * so that the join node's alias vars show correct collation info.
     */
    assign_expr_collations(pstate, res_node);

    res_node
}

/*
 * markRelsAsNulledBy -
 *    Mark the given jointree node and its children as nulled by join jindex
 */
unsafe fn markRelsAsNulledBy(pstate: *mut ParseState, n: *mut Node, jindex: c_int) {
    let varno: c_int;
    let mut lc: *mut crate::nodes::pg_list::ListCell = core::ptr::null_mut();

    /* Note: we can't see FromExpr here */
    let varno: c_int = if IsA!(n, T_RangeTblRef) {
        (*(n as *mut RangeTblRef)).rtindex
    } else if IsA!(n, T_JoinExpr) {
        let j: *mut JoinExpr = n as *mut JoinExpr;

        /* recurse to children */
        markRelsAsNulledBy(pstate, (*j).larg, jindex);
        markRelsAsNulledBy(pstate, (*j).rarg, jindex);
        (*j).rtindex
    } else {
        elog!(ERROR, "unrecognized node type: {}", nodeTag(n) as c_int);
        0 /* keep compiler quiet */
    };

    /*
     * Now add jindex to the p_nullingrels set for relation varno.  Since we
     * maintain the p_nullingrels list lazily, we might need to extend it to
     * make the varno'th entry exist.
     */
    while list_length((*pstate).p_nullingrels) < varno {
        (*pstate).p_nullingrels = lappend((*pstate).p_nullingrels, core::ptr::null_mut());
    }
    let lc_nth = list_nth_cell((*pstate).p_nullingrels, varno - 1);
    let old_bms = lfirst(lc_nth) as *mut Bitmapset;
    (*lc_nth).ptr_value = bms_add_member(old_bms, jindex) as *mut c_void; /* HACK: store Bitmapset* in ListCell.ptr_value */
    // Note: lfirst(lc) = bms_add_member(lfirst(lc), jindex) in C
    // For 1:1 translation we store the result back into the cell data.
}

/*
 * setNamespaceColumnVisibility -
 *    Convenience subroutine to update cols_visible flags in a namespace list.
 */
unsafe fn setNamespaceColumnVisibility(namespace: *mut List, cols_visible: bool) {
    let mut lc: *mut crate::nodes::pg_list::ListCell = core::ptr::null_mut();

    let mut ns_tmp = namespace;
    foreach!(lc, ns_tmp, {
        let nsitem: *mut ParseNamespaceItem = lfirst(current_cell!(lc)) as *mut ParseNamespaceItem;
        (*nsitem).p_cols_visible = cols_visible;
    });
}

/*
 * setNamespaceLateralState -
 *    Convenience subroutine to update LATERAL flags in a namespace list.
 */
unsafe fn setNamespaceLateralState(namespace: *mut List, lateral_only: bool, lateral_ok: bool) {
    let mut lc: *mut crate::nodes::pg_list::ListCell = core::ptr::null_mut();

    let mut ns_tmp = namespace;
    foreach!(lc, ns_tmp, {
        let nsitem: *mut ParseNamespaceItem = lfirst(current_cell!(lc)) as *mut ParseNamespaceItem;
        (*nsitem).p_lateral_only = lateral_only;
        (*nsitem).p_lateral_ok = lateral_ok;
    });
}


/*
 * transformWhereClause -
 *    Transform the qualification and make sure it is of type boolean.
 *    Used for WHERE and allied clauses.
 *
 * constructName does not affect the semantics, but is used in error messages
 */
pub unsafe fn transformWhereClause(
    pstate: *mut ParseState,
    clause: *mut Node,
    exprKind: ParseExprKind,
    constructName: *const c_char,
) -> *mut Node {
    let qual: *mut Node;

    if clause.is_null() {
        return core::ptr::null_mut();
    }

    let qual = transformExpr(pstate, clause, exprKind);

    let qual = coerce_to_boolean(pstate, qual, constructName);

    qual
}


/*
 * transformLimitClause -
 *    Transform the expression and make sure it is of type bigint.
 *    Used for LIMIT and allied clauses.
 *
 * Note: as of Postgres 8.2, LIMIT expressions are expected to yield int8,
 * rather than int4 as before.
 *
 * constructName does not affect the semantics, but is used in error messages
 */
pub unsafe fn transformLimitClause(
    pstate: *mut ParseState,
    clause: *mut Node,
    exprKind: ParseExprKind,
    constructName: *const c_char,
    limitOption: LimitOption,
) -> *mut Node {
    let qual: *mut Node;

    if clause.is_null() {
        return core::ptr::null_mut();
    }

    let qual = transformExpr(pstate, clause, exprKind);

    let qual = coerce_to_specific_type(pstate, qual, INT8OID, constructName);

    /* LIMIT can't refer to any variables of the current query */
    checkExprIsVarFree(pstate, qual, constructName);

    /*
     * Don't allow NULLs in FETCH FIRST .. WITH TIES.  This test is ugly and
     * extremely simplistic, in that you can pass a NULL anyway by hiding it
     * inside an expression -- but this protects ruleutils against emitting an
     * unadorned NULL that's not accepted back by the grammar.
     */
    if exprKind == EXPR_KIND_LIMIT
        && limitOption == LimitOption::LIMIT_OPTION_WITH_TIES
        && IsA!(clause, T_A_Const)
        && (*(clause as *mut A_Const)).isnull
    {
        ereport!(
            ERROR,
            errmsg!(
                "row count cannot be null in FETCH FIRST ... WITH TIES clause"
            )
        );
        /* C also: errcode(ERRCODE_INVALID_ROW_COUNT_IN_LIMIT_CLAUSE) */
    }

    qual
}

/*
 * checkExprIsVarFree
 *      Check that given expr has no Vars of the current query level
 *      (aggregates and window functions should have been rejected already).
 *
 * This is used to check expressions that have to have a consistent value
 * across all rows of the query, such as a LIMIT.  Arguably it should reject
 * volatile functions, too, but we don't do that --- whatever value the
 * function gives on first execution is what you get.
 *
 * constructName does not affect the semantics, but is used in error messages
 */
unsafe fn checkExprIsVarFree(
    pstate: *mut ParseState,
    n: *mut Node,
    constructName: *const c_char,
) {
    if contain_vars_of_level(n, 0) {
        ereport!(
            ERROR,
            errmsg!(
                "argument of {} must not contain variables",
                std::ffi::CStr::from_ptr(constructName).to_string_lossy()
            )
        );
        /* C also: errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
           parser_errposition(pstate, locate_var_of_level(n, 0)) */
    }
}


/*
 * checkTargetlistEntrySQL92 -
 *    Validate a targetlist entry found by findTargetlistEntrySQL92
 *
 * When we select a pre-existing tlist entry as a result of syntax such
 * as "GROUP BY 1", we have to make sure it is acceptable for use in the
 * indicated clause type; transformExpr() will have treated it as a regular
 * targetlist item.
 */
unsafe fn checkTargetlistEntrySQL92(
    pstate: *mut ParseState,
    tle: *mut TargetEntry,
    exprKind: ParseExprKind,
) {
    match exprKind {
        EXPR_KIND_GROUP_BY => {
            /* reject aggregates and window functions */
            if (*pstate).p_hasAggs
                && contain_aggs_of_level((*tle).expr as *mut Node, 0)
            {
                ereport!(
                    ERROR,
                    errmsg!(
                        "aggregate functions are not allowed in {}",
                        std::ffi::CStr::from_ptr(ParseExprKindName(exprKind)).to_string_lossy()
                    )
                );
                /* C also: errcode(ERRCODE_GROUPING_ERROR),
                   parser_errposition(pstate, locate_agg_of_level(...)) */
            }
            if (*pstate).p_hasWindowFuncs
                && contain_windowfuncs((*tle).expr as *mut Node)
            {
                ereport!(
                    ERROR,
                    errmsg!(
                        "window functions are not allowed in {}",
                        std::ffi::CStr::from_ptr(ParseExprKindName(exprKind)).to_string_lossy()
                    )
                );
                /* C also: errcode(ERRCODE_WINDOWING_ERROR),
                   parser_errposition(pstate, locate_windowfunc(...)) */
            }
        }
        EXPR_KIND_ORDER_BY => {
            /* no extra checks needed */
        }
        EXPR_KIND_DISTINCT_ON => {
            /* no extra checks needed */
        }
        _ => {
            elog!(ERROR, "unexpected exprKind in checkTargetlistEntrySQL92");
        }
    }
}

/*
 *  findTargetlistEntrySQL92 -
 *    Returns the targetlist entry matching the given (untransformed) node.
 *    If no matching entry exists, one is created and appended to the target
 *    list as a "resjunk" node.
 *
 * This function supports the old SQL92 ORDER BY interpretation, where the
 * expression is an output column name or number.  If we fail to find a
 * match of that sort, we fall through to the SQL99 rules.  For historical
 * reasons, Postgres also allows this interpretation for GROUP BY, though
 * the standard never did.  However, for GROUP BY we prefer a SQL99 match.
 * This function is *not* used for WINDOW definitions.
 *
 * node     the ORDER BY, GROUP BY, or DISTINCT ON expression to be matched
 * tlist    the target list (passed by reference so we can append to it)
 * exprKind identifies clause type being processed
 */
unsafe fn findTargetlistEntrySQL92(
    pstate: *mut ParseState,
    node: *mut Node,
    tlist: *mut *mut List,
    exprKind: ParseExprKind,
) -> *mut TargetEntry {
    let mut tl: *mut crate::nodes::pg_list::ListCell = core::ptr::null_mut();

    /*----------
     * Handle two special cases as mandated by the SQL92 spec:
     *
     * 1. Bare ColumnName (no qualifier or subscripts)
     *    ...
     * 2. IntegerConstant
     *    ...
     *----------
     */
    if IsA!(node, T_ColumnRef)
        && list_length((*(node as *mut ColumnRef)).fields) == 1
        && IsA!(
            linitial((*(node as *mut ColumnRef)).fields) as *mut Node,
            T_String
        )
    {
        let mut name: *const c_char = strVal!(
            linitial((*(node as *mut ColumnRef)).fields) as *mut Node
        );
        let location: c_int = (*(node as *mut ColumnRef)).location;

        if exprKind == EXPR_KIND_GROUP_BY {
            /*
             * In GROUP BY, we must prefer a match against a FROM-clause
             * column to one against the targetlist.  Look to see if there is
             * a matching column.  If so, fall through to use SQL99 rules.
             * NOTE: if name could refer ambiguously to more than one column
             * name exposed by FROM, colNameToVar will ereport(ERROR). That's
             * just what we want here.
             *
             * Small tweak for 7.4.3: ignore matches in upper query levels.
             * This effectively changes the search order for bare names to (1)
             * local FROM variables, (2) local targetlist aliases, (3) outer
             * FROM variables, whereas before it was (1) (3) (2). SQL92 and
             * SQL99 do not allow GROUPing BY an outer reference, so this
             * breaks no cases that are legal per spec, and it seems a more
             * self-consistent behavior.
             */
            if !colNameToVar(pstate, name, true, location).is_null() {
                name = core::ptr::null();
            }
        }

        if !name.is_null() {
            let mut target_result: *mut TargetEntry = core::ptr::null_mut();

            let mut tl_tmp = *tlist;
            foreach!(tl, tl_tmp, {
                let tle: *mut TargetEntry = lfirst(current_cell!(tl)) as *mut TargetEntry;

                if !(*tle).resjunk
                    && !(*tle).resname.is_null()
                    && strcmp((*tle).resname, name) == 0
                {
                    if !target_result.is_null() {
                        if !equal((*target_result).expr as *mut c_void, (*tle).expr as *mut c_void) {
                            ereport!(
                                ERROR,
                                errmsg!(
                                    "{}  \"{}\" is ambiguous",
                                    std::ffi::CStr::from_ptr(ParseExprKindName(exprKind)).to_string_lossy(),
                                    std::ffi::CStr::from_ptr(name).to_string_lossy()
                                )
                            );
                            /* C also: errcode(ERRCODE_AMBIGUOUS_COLUMN),
                               parser_errposition(pstate, location) */
                        }
                    } else {
                        target_result = tle;
                    }
                    /* Stay in loop to check for ambiguity */
                }
            });
            if !target_result.is_null() {
                /* return the first match, after suitable validation */
                checkTargetlistEntrySQL92(pstate, target_result, exprKind);
                return target_result;
            }
        }
    }
    if IsA!(node, T_A_Const) {
        let aconst: *mut A_Const = node as *mut A_Const; /* castNode(A_Const, node) */
        let mut targetlist_pos: c_int = 0;
        let target_pos: c_int;

        if !IsA!(&mut (*aconst).val as *mut _ as *mut Node, T_Integer) {
            ereport!(
                ERROR,
                errmsg!(
                    "non-integer constant in {}",
                    std::ffi::CStr::from_ptr(ParseExprKindName(exprKind)).to_string_lossy()
                )
            );
            /* C also: errcode(ERRCODE_SYNTAX_ERROR),
               parser_errposition(pstate, aconst->location) */
        }

        target_pos = intVal!(&mut (*aconst).val as *mut _ as *mut crate::nodes::nodes::Node);
        let mut tl_tmp = *tlist;
        foreach!(tl, tl_tmp, {
            let tle: *mut TargetEntry = lfirst(current_cell!(tl)) as *mut TargetEntry;

            if !(*tle).resjunk {
                targetlist_pos += 1;
                if targetlist_pos == target_pos {
                    /* return the unique match, after suitable validation */
                    checkTargetlistEntrySQL92(pstate, tle, exprKind);
                    return tle;
                }
            }
        });
        ereport!(
            ERROR,
            errmsg!(
                "{} position {} is not in select list",
                std::ffi::CStr::from_ptr(ParseExprKindName(exprKind)).to_string_lossy(),
                target_pos
            )
        );
        /* C also: errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
           parser_errposition(pstate, aconst->location) */
    }

    /*
     * Otherwise, we have an expression, so process it per SQL99 rules.
     */
    findTargetlistEntrySQL99(pstate, node, tlist, exprKind)
}

/*
 *  findTargetlistEntrySQL99 -
 *    Returns the targetlist entry matching the given (untransformed) node.
 *    If no matching entry exists, one is created and appended to the target
 *    list as a "resjunk" node.
 *
 * This function supports the SQL99 interpretation, wherein the expression
 * is just an ordinary expression referencing input column names.
 *
 * node     the ORDER BY, GROUP BY, etc expression to be matched
 * tlist    the target list (passed by reference so we can append to it)
 * exprKind identifies clause type being processed
 */
unsafe fn findTargetlistEntrySQL99(
    pstate: *mut ParseState,
    node: *mut Node,
    tlist: *mut *mut List,
    exprKind: ParseExprKind,
) -> *mut TargetEntry {
    let mut target_result: *mut TargetEntry = core::ptr::null_mut();
    let mut tl: *mut crate::nodes::pg_list::ListCell = core::ptr::null_mut();
    let expr: *mut Node;

    /*
     * Convert the untransformed node to a transformed expression, and search
     * for a match in the tlist.  NOTE: it doesn't really matter whether there
     * is more than one match.  Also, we are willing to match an existing
     * resjunk target here, though the SQL92 cases above must ignore resjunk
     * targets.
     */
    let expr = transformExpr(pstate, node, exprKind);

    let mut tl_tmp = *tlist;
    foreach!(tl, tl_tmp, {
        let tle: *mut TargetEntry = lfirst(current_cell!(tl)) as *mut TargetEntry;
        let texpr: *mut Node;

        /*
         * Ignore any implicit cast on the existing tlist expression.
         *
         * This essentially allows the ORDER/GROUP/etc item to adopt the same
         * datatype previously selected for a textually-equivalent tlist item.
         * There can't be any implicit cast at top level in an ordinary SELECT
         * tlist at this stage, but the case does arise with ORDER BY in an
         * aggregate function.
         */
        let texpr = strip_implicit_coercions((*tle).expr as *mut Node);

        if equal(expr as *mut c_void, texpr as *mut c_void) {
            return tle;
        }
    });

    /*
     * If no matches, construct a new target entry which is appended to the
     * end of the target list.  This target is given resjunk = true so that it
     * will not be projected into the final tuple.
     */
    let target_result = transformTargetEntry(
        pstate,
        node,
        expr,
        exprKind,
        core::ptr::null(),
        true,
    );

    *tlist = lappend(*tlist, target_result as *mut c_void);

    target_result
}

/*-------------------------------------------------------------------------
 * Flatten out parenthesized sublists in grouping lists, and some cases
 * of nested grouping sets.
 * ...
 *-------------------------------------------------------------------------
 */
unsafe fn flatten_grouping_sets(
    expr: *mut Node,
    toplevel: bool,
    hasGroupingSets: *mut bool,
) -> *mut Node {
    /* just in case of pathological input */
    check_stack_depth();

    if expr == NIL as *mut Node {
        return NIL as *mut Node;
    }

    match nodeTag(expr) {
        T_RowExpr => {
            let r: *mut crate::nodes::primnodes::RowExpr =
                expr as *mut crate::nodes::primnodes::RowExpr;
            if (*r).row_format == COERCE_IMPLICIT_CAST {
                return flatten_grouping_sets((*r).args as *mut Node, false, core::ptr::null_mut());
            }
        }
        T_GroupingSet => {
            let gset: *mut GroupingSet = expr as *mut GroupingSet;
            let mut l2: *mut crate::nodes::pg_list::ListCell = core::ptr::null_mut();
            let mut result_set: *mut List = NIL;

            if !hasGroupingSets.is_null() {
                *hasGroupingSets = true;
            }

            /*
             * at the top level, we skip over all empty grouping sets; the
             * caller can supply the canonical GROUP BY () if nothing is left.
             */
            if toplevel && (*gset).kind == GROUPING_SET_EMPTY {
                return NIL as *mut Node;
            }

            let mut content_tmp = (*gset).content;
            foreach!(l2, content_tmp, {
                let n1: *mut Node = lfirst(current_cell!(l2)) as *mut Node;
                let n2 = flatten_grouping_sets(n1, false, core::ptr::null_mut());

                if IsA!(n1, T_GroupingSet)
                    && (*(n1 as *mut GroupingSet)).kind == GROUPING_SET_SETS
                {
                    result_set = list_concat(result_set, n2 as *mut List);
                } else {
                    result_set = lappend(result_set, n2 as *mut c_void);
                }
            });

            /*
             * At top level, keep the grouping set node; but if we're in a
             * nested grouping set, then we need to concat the flattened
             * result into the outer list if it's simply nested.
             */
            if toplevel || ((*gset).kind != GROUPING_SET_SETS) {
                return makeGroupingSet((*gset).kind, result_set, (*gset).location) as *mut Node;
            } else {
                return result_set as *mut Node;
            }
        }
        T_List => {
            let mut result: *mut List = NIL;
            let mut l: *mut crate::nodes::pg_list::ListCell = core::ptr::null_mut();

            let mut expr_list = expr as *mut List;
            foreach!(l, expr_list, {
                let n =
                    flatten_grouping_sets(lfirst(current_cell!(l)) as *mut Node, toplevel, hasGroupingSets);

                if n != NIL as *mut Node {
                    if IsA!(n, T_List) {
                        result = list_concat(result, n as *mut List);
                    } else {
                        result = lappend(result, n as *mut c_void);
                    }
                }
            });

            return result as *mut Node;
        }
        _ => {}
    }

    expr
}

/*
 * Transform a single expression within a GROUP BY clause or grouping set.
 *
 * Returns the ressortgroupref of the expression.
 */
unsafe fn transformGroupClauseExpr(
    flatresult: *mut *mut List,
    seen_local: *mut Bitmapset,
    pstate: *mut ParseState,
    gexpr: *mut Node,
    targetlist: *mut *mut List,
    sortClause: *mut List,
    exprKind: ParseExprKind,
    useSQL99: bool,
    toplevel: bool,
) -> Index {
    let tle: *mut TargetEntry;
    let mut found: bool = false;

    let tle = if useSQL99 {
        findTargetlistEntrySQL99(pstate, gexpr, targetlist, exprKind)
    } else {
        findTargetlistEntrySQL92(pstate, gexpr, targetlist, exprKind)
    };

    if (*tle).ressortgroupref > 0 {
        let mut sl: *mut crate::nodes::pg_list::ListCell = core::ptr::null_mut();

        /*
         * Eliminate duplicates (GROUP BY x, x) but only at local level.
         * (Duplicates in grouping sets can affect the number of returned
         * rows, so can't be dropped indiscriminately.)
         *
         * Since we don't care about anything except the sortgroupref, we can
         * use a bitmapset rather than scanning lists.
         */
        if bms_is_member((*tle).ressortgroupref as c_int, seen_local) {
            return 0;
        }

        /*
         * If we're already in the flat clause list, we don't need to consider
         * adding ourselves again.
         */
        found = targetIsInSortList(tle, InvalidOid, *flatresult);
        if found {
            return (*tle).ressortgroupref;
        }

        /*
         * If the GROUP BY tlist entry also appears in ORDER BY, copy operator
         * info from the (first) matching ORDER BY item.
         */
        let mut sl_tmp = sortClause;
        foreach!(sl, sl_tmp, {
            let sc: *mut SortGroupClause = lfirst(current_cell!(sl)) as *mut SortGroupClause;

            if (*sc).tleSortGroupRef == (*tle).ressortgroupref {
                let grpc = copyObject(sc);

                if !toplevel {
                    (*grpc).nulls_first = false;
                }
                *flatresult = lappend(*flatresult, grpc as *mut c_void);
                found = true;
                // break
            }
        });
    }

    /*
     * If no match in ORDER BY, just add it to the result using default
     * sort/group semantics.
     */
    if !found {
        *flatresult = addTargetToGroupList(
            pstate,
            tle,
            *flatresult,
            *targetlist,
            exprLocation(gexpr),
        );
    }

    /*
     * _something_ must have assigned us a sortgroupref by now...
     */
    (*tle).ressortgroupref
}

/*
 * Transform a list of expressions within a GROUP BY clause or grouping set.
 *
 * Returns an integer list of ressortgroupref values.
 */
unsafe fn transformGroupClauseList(
    flatresult: *mut *mut List,
    pstate: *mut ParseState,
    list: *mut List,
    targetlist: *mut *mut List,
    sortClause: *mut List,
    exprKind: ParseExprKind,
    useSQL99: bool,
    toplevel: bool,
) -> *mut List {
    let mut seen_local: *mut Bitmapset = core::ptr::null_mut();
    let mut result: *mut List = NIL;
    let mut gl: *mut crate::nodes::pg_list::ListCell = core::ptr::null_mut();

    let mut list_tmp = list;
    foreach!(gl, list_tmp, {
        let gexpr: *mut Node = lfirst(current_cell!(gl)) as *mut Node;

        let r_ef = transformGroupClauseExpr(
            flatresult,
            seen_local,
            pstate,
            gexpr,
            targetlist,
            sortClause,
            exprKind,
            useSQL99,
            toplevel,
        );

        if r_ef > 0 {
            seen_local = bms_add_member(seen_local, r_ef as c_int);
            result = lappend_int(result, r_ef as c_int);
        }
    });

    result
}

/*
 * Transform a grouping set and (recursively) its content.
 *
 * Returns the transformed node, which now contains SIMPLE nodes with lists
 * of ressortgrouprefs rather than expressions.
 */
unsafe fn transformGroupingSet(
    flatresult: *mut *mut List,
    pstate: *mut ParseState,
    gset: *mut GroupingSet,
    targetlist: *mut *mut List,
    sortClause: *mut List,
    exprKind: ParseExprKind,
    useSQL99: bool,
    toplevel: bool,
) -> *mut Node {
    let mut gl: *mut crate::nodes::pg_list::ListCell = core::ptr::null_mut();
    let mut content: *mut List = NIL;

    // Assert(toplevel || gset->kind != GROUPING_SET_SETS);

    let mut content_tmp = (*gset).content;
    foreach!(gl, content_tmp, {
        let n: *mut Node = lfirst(current_cell!(gl)) as *mut Node;

        if IsA!(n, T_List) {
            let l = transformGroupClauseList(
                flatresult,
                pstate,
                n as *mut List,
                targetlist,
                sortClause,
                exprKind,
                useSQL99,
                false,
            );

            content = lappend(
                content,
                makeGroupingSet(GROUPING_SET_SIMPLE, l, exprLocation(n)) as *mut c_void,
            );
        } else if IsA!(n, T_GroupingSet) {
            let gset2: *mut GroupingSet = lfirst(current_cell!(gl)) as *mut GroupingSet;

            content = lappend(
                content,
                transformGroupingSet(
                    flatresult,
                    pstate,
                    gset2,
                    targetlist,
                    sortClause,
                    exprKind,
                    useSQL99,
                    false,
                ) as *mut c_void,
            );
        } else {
            let r_ef = transformGroupClauseExpr(
                flatresult,
                core::ptr::null_mut(),
                pstate,
                n,
                targetlist,
                sortClause,
                exprKind,
                useSQL99,
                false,
            );

            content = lappend(
                content,
                makeGroupingSet(
                    GROUPING_SET_SIMPLE,
                    list_make1_int(r_ef as c_int),
                    exprLocation(n),
                ) as *mut c_void,
            );
        }
    });

    /* Arbitrarily cap the size of CUBE, which has exponential growth */
    if (*gset).kind == GROUPING_SET_CUBE {
        if list_length(content) > 12 {
            ereport!(
                ERROR,
                errmsg!("CUBE is limited to 12 elements")
            );
            /* C also: errcode(ERRCODE_TOO_MANY_COLUMNS),
               parser_errposition(pstate, gset->location) */
        }
    }

    makeGroupingSet((*gset).kind, content, (*gset).location) as *mut Node
}


/*
 * transformGroupClause -
 *    transform a GROUP BY clause
 *
 * GROUP BY items will be added to the targetlist (as resjunk columns)
 * if not already present, so the targetlist must be passed by reference.
 *
 * This is also used for window PARTITION BY clauses (which act almost the
 * same, but are always interpreted per SQL99 rules).
 *
 * Returns the transformed (flat) groupClause.
 */
pub unsafe fn transformGroupClause(
    pstate: *mut ParseState,
    grouplist: *mut List,
    groupingSets: *mut *mut List,
    targetlist: *mut *mut List,
    sortClause: *mut List,
    exprKind: ParseExprKind,
    useSQL99: bool,
) -> *mut List {
    let mut result: *mut List = NIL;
    let flat_grouplist: *mut List;
    let mut gsets: *mut List = NIL;
    let mut gl: *mut crate::nodes::pg_list::ListCell = core::ptr::null_mut();
    let mut hasGroupingSets: bool = false;
    let mut seen_local: *mut Bitmapset = core::ptr::null_mut();

    /*
     * Recursively flatten implicit RowExprs. (Technically this is only needed
     * for GROUP BY, per the syntax rules for grouping sets, but we do it
     * anyway.)
     */
    let flat_grouplist = flatten_grouping_sets(
        grouplist as *mut Node,
        true,
        &mut hasGroupingSets,
    ) as *mut List;

    /*
     * If the list is now empty, but hasGroupingSets is true, it's because we
     * elided redundant empty grouping sets. Restore a single empty grouping
     * set to leave a canonical form: GROUP BY ()
     */
    let flat_grouplist = if flat_grouplist.is_null() && hasGroupingSets {
        list_make1!(
            makeGroupingSet(
                GROUPING_SET_EMPTY,
                NIL,
                exprLocation(grouplist as *mut Node),
            ) as *mut c_void
        )
    } else {
        flat_grouplist
    };

    let mut gl_tmp = flat_grouplist;
    foreach!(gl, gl_tmp, {
        let gexpr: *mut Node = lfirst(current_cell!(gl)) as *mut Node;

        if IsA!(gexpr, T_GroupingSet) {
            let gset: *mut GroupingSet = gexpr as *mut GroupingSet;

            match (*gset).kind {
                GROUPING_SET_EMPTY => {
                    gsets = lappend(gsets, gset as *mut c_void);
                }
                GROUPING_SET_SIMPLE => {
                    /* can't happen */
                    // Assert(false);
                }
                GROUPING_SET_SETS | GROUPING_SET_CUBE | GROUPING_SET_ROLLUP => {
                    gsets = lappend(
                        gsets,
                        transformGroupingSet(
                            &mut result,
                            pstate,
                            gset,
                            targetlist,
                            sortClause,
                            exprKind,
                            useSQL99,
                            true,
                        ) as *mut c_void,
                    );
                }
            }
        } else {
            let r_ef = transformGroupClauseExpr(
                &mut result,
                seen_local,
                pstate,
                gexpr,
                targetlist,
                sortClause,
                exprKind,
                useSQL99,
                true,
            );

            if r_ef > 0 {
                seen_local = bms_add_member(seen_local, r_ef as c_int);
                if hasGroupingSets {
                    gsets = lappend(
                        gsets,
                        makeGroupingSet(
                            GROUPING_SET_SIMPLE,
                            list_make1_int(r_ef as c_int),
                            exprLocation(gexpr),
                        ) as *mut c_void,
                    );
                }
            }
        }
    });

    /* parser should prevent this */
    // Assert(gsets == NIL || groupingSets != NULL);

    if !groupingSets.is_null() {
        *groupingSets = gsets;
    }

    result
}

/*
 * transformSortClause -
 *    transform an ORDER BY clause
 *
 * ORDER BY items will be added to the targetlist (as resjunk columns)
 * if not already present, so the targetlist must be passed by reference.
 *
 * This is also used for window and aggregate ORDER BY clauses (which act
 * almost the same, but are always interpreted per SQL99 rules).
 */
pub unsafe fn transformSortClause(
    pstate: *mut ParseState,
    orderlist: *mut List,
    targetlist: *mut *mut List,
    exprKind: ParseExprKind,
    useSQL99: bool,
) -> *mut List {
    let mut sortlist: *mut List = NIL;
    let mut olitem: *mut crate::nodes::pg_list::ListCell = core::ptr::null_mut();

    let mut ol_tmp = orderlist;
    foreach!(olitem, ol_tmp, {
        let sortby: *mut SortBy = lfirst(current_cell!(olitem)) as *mut SortBy;
        let tle: *mut TargetEntry;

        let tle = if useSQL99 {
            findTargetlistEntrySQL99(pstate, (*sortby).node, targetlist, exprKind)
        } else {
            findTargetlistEntrySQL92(pstate, (*sortby).node, targetlist, exprKind)
        };

        sortlist = addTargetToSortList(pstate, tle, sortlist, *targetlist, sortby);
    });

    sortlist
}

/*
 * transformWindowDefinitions -
 *      transform window definitions (WindowDef to WindowClause)
 */
pub unsafe fn transformWindowDefinitions(
    pstate: *mut ParseState,
    windowdefs: *mut List,
    targetlist: *mut *mut List,
) -> *mut List {
    let mut result: *mut List = NIL;
    let mut winref: Index = 0;
    let mut lc: *mut ListCell = core::ptr::null_mut();

    let mut wdefs_tmp = windowdefs;
    foreach!(lc, wdefs_tmp, {
        let windef: *mut WindowDef = lfirst(current_cell!(lc)) as *mut WindowDef;
        let mut refwc: *mut WindowClause = core::ptr::null_mut();
        let partitionClause: *mut List;
        let orderClause: *mut List;
        let mut rangeopfamily: Oid = InvalidOid;
        let mut rangeopcintype: Oid = InvalidOid;
        let wc: *mut WindowClause;

        winref += 1;

        /* Check for duplicate window names. */
        if !(*windef).name.is_null()
            && !findWindowClause(result, (*windef).name).is_null()
        {
            ereport!(
                ERROR,
                errmsg!(
                    "window \"{}\" is already defined",
                    std::ffi::CStr::from_ptr((*windef).name).to_string_lossy()
                ),
                /* C also: errcode(ERRCODE_WINDOWING_ERROR), parser_errposition */
            );
        }

        /* If it references a previous window, look that up. */
        if !(*windef).refname.is_null() {
            refwc = findWindowClause(result, (*windef).refname);
            if refwc.is_null() {
                ereport!(
                    ERROR,
                    errmsg!(
                        "window \"{}\" does not exist",
                        std::ffi::CStr::from_ptr((*windef).refname).to_string_lossy()
                    ),
                    /* C also: errcode(ERRCODE_UNDEFINED_OBJECT), parser_errposition */
                );
            }
        }

        /*
         * Transform PARTITION and ORDER specs, if any.  These are treated
         * almost exactly like top-level GROUP BY and ORDER BY clauses,
         * including the special handling of nondefault operator semantics.
         */
        orderClause = transformSortClause(
            pstate,
            (*windef).orderClause,
            targetlist,
            EXPR_KIND_WINDOW_ORDER,
            true, /* force SQL99 rules */
        );
        partitionClause = transformGroupClause(
            pstate,
            (*windef).partitionClause,
            core::ptr::null_mut(),
            targetlist,
            orderClause,
            EXPR_KIND_WINDOW_PARTITION,
            true, /* force SQL99 rules */
        );

        /* And prepare the new WindowClause. */
        wc = makeNode!(WindowClause, T_WindowClause);
        (*wc).name = (*windef).name;
        (*wc).refname = (*windef).refname;

        /*
         * Per spec, a windowdef that references a previous one copies the
         * previous partition clause (and mustn't specify its own).  It can
         * specify its own ordering clause, but only if the previous one had
         * none.  It always specifies its own frame clause, and the previous
         * one must not have a frame clause.
         */
        if !refwc.is_null() {
            if !partitionClause.is_null() {
                ereport!(
                    ERROR,
                    errmsg!(
                        "cannot override PARTITION BY clause of window \"{}\"",
                        std::ffi::CStr::from_ptr((*windef).refname).to_string_lossy()
                    ),
                    /* C also: errcode(ERRCODE_WINDOWING_ERROR), parser_errposition */
                );
            }
            (*wc).partitionClause = copyObject((*refwc).partitionClause);
        } else {
            (*wc).partitionClause = partitionClause;
        }

        if !refwc.is_null() {
            if !orderClause.is_null() && !(*refwc).orderClause.is_null() {
                ereport!(
                    ERROR,
                    errmsg!(
                        "cannot override ORDER BY clause of window \"{}\"",
                        std::ffi::CStr::from_ptr((*windef).refname).to_string_lossy()
                    ),
                    /* C also: errcode(ERRCODE_WINDOWING_ERROR), parser_errposition */
                );
            }
            if !orderClause.is_null() {
                (*wc).orderClause = orderClause;
                (*wc).copiedOrder = false;
            } else {
                (*wc).orderClause = copyObject((*refwc).orderClause);
                (*wc).copiedOrder = true;
            }
        } else {
            (*wc).orderClause = orderClause;
            (*wc).copiedOrder = false;
        }

        if !refwc.is_null() && (*refwc).frameOptions != FRAMEOPTION_DEFAULTS {
            /*
             * Use this message if this is a WINDOW clause, or if it's an OVER
             * clause that includes ORDER BY or framing clauses.
             */
            if !(*windef).name.is_null()
                || !orderClause.is_null()
                || (*windef).frameOptions != FRAMEOPTION_DEFAULTS
            {
                ereport!(
                    ERROR,
                    errmsg!(
                        "cannot copy window \"{}\" because it has a frame clause",
                        std::ffi::CStr::from_ptr((*windef).refname).to_string_lossy()
                    ),
                    /* C also: errcode(ERRCODE_WINDOWING_ERROR), parser_errposition */
                );
            }
            /* Else this clause is just OVER (foo), so say this: */
            ereport!(
                ERROR,
                errmsg!(
                    "cannot copy window \"{}\" because it has a frame clause",
                    std::ffi::CStr::from_ptr((*windef).refname).to_string_lossy()
                ),
                /* C also: errhint("Omit the parentheses in this OVER clause."), parser_errposition */
            );
        }
        (*wc).frameOptions = (*windef).frameOptions;

        /*
         * RANGE offset PRECEDING/FOLLOWING requires exactly one ORDER BY
         * column; check that and get its sort opfamily info.
         */
        if ((*wc).frameOptions & FRAMEOPTION_RANGE) != 0
            && ((*wc).frameOptions & (FRAMEOPTION_START_OFFSET | FRAMEOPTION_END_OFFSET)) != 0
        {
            let sortcl: *mut SortGroupClause;
            let sortkey: *mut Node;
            let mut rangecmptype: c_int = 0; /* CompareType */

            if list_length((*wc).orderClause) != 1 {
                ereport!(
                    ERROR,
                    errmsg!(
                        "RANGE with offset PRECEDING/FOLLOWING requires exactly one ORDER BY column"
                    ),
                    /* C also: errcode(ERRCODE_WINDOWING_ERROR), parser_errposition */
                );
            }
            sortcl = linitial_node!(SortGroupClause, T_SortGroupClause, (*wc).orderClause);
            sortkey = get_sortgroupclause_expr(sortcl, *targetlist);
            /* Find the sort operator in pg_amop */
            if !get_ordering_op_properties(
                (*sortcl).sortop,
                &mut rangeopfamily,
                &mut rangeopcintype,
                &mut rangecmptype,
            ) {
                elog!(ERROR, "operator {} is not a valid ordering operator", (*sortcl).sortop);
            }
            /* Record properties of sort ordering */
            (*wc).inRangeColl = exprCollation(sortkey as *mut Node);
            (*wc).inRangeAsc = !(*sortcl).reverse_sort;
            (*wc).inRangeNullsFirst = (*sortcl).nulls_first;
        }

        /* Per spec, GROUPS mode requires an ORDER BY clause */
        if ((*wc).frameOptions & FRAMEOPTION_GROUPS) != 0 {
            if (*wc).orderClause.is_null() {
                ereport!(
                    ERROR,
                    errmsg!("GROUPS mode requires an ORDER BY clause"),
                    /* C also: errcode(ERRCODE_WINDOWING_ERROR), parser_errposition */
                );
            }
        }

        /* Process frame offset expressions */
        (*wc).startOffset = transformFrameOffset(
            pstate,
            (*wc).frameOptions,
            rangeopfamily,
            rangeopcintype,
            &mut (*wc).startInRangeFunc,
            (*windef).startOffset,
        );
        (*wc).endOffset = transformFrameOffset(
            pstate,
            (*wc).frameOptions,
            rangeopfamily,
            rangeopcintype,
            &mut (*wc).endInRangeFunc,
            (*windef).endOffset,
        );
        (*wc).winref = winref;

        result = lappend(result, wc as *mut c_void);
    });

    result
}

/*
 * transformDistinctClause -
 *    transform a DISTINCT clause
 *
 * Since we may need to add items to the query's targetlist, that list
 * is passed by reference.
 *
 * As with GROUP BY, we absorb the sorting semantics of ORDER BY as much as
 * possible into the distinctClause.  This avoids a possible need to re-sort,
 * and allows the user to choose the equality semantics used by DISTINCT,
 * should she be working with a datatype that has more than one equality
 * operator.
 *
 * is_agg is true if we are transforming an aggregate(DISTINCT ...)
 * function call.  This does not affect any behavior, only the phrasing
 * of error messages.
 */
pub unsafe fn transformDistinctClause(
    pstate: *mut ParseState,
    targetlist: *mut *mut List,
    sortClause: *mut List,
    is_agg: bool,
) -> *mut List {
    let mut result: *mut List = NIL;
    let mut slitem: *mut ListCell = core::ptr::null_mut();
    let mut tlitem: *mut ListCell = core::ptr::null_mut();

    /*
     * The distinctClause should consist of all ORDER BY items followed by all
     * other non-resjunk targetlist items.  There must not be any resjunk
     * ORDER BY items --- that would imply that we are sorting by a value that
     * isn't necessarily unique within a DISTINCT group, so the results
     * wouldn't be well-defined.
     */
    let mut sc_tmp = sortClause;
    foreach!(slitem, sc_tmp, {
        let scl: *mut SortGroupClause = lfirst(current_cell!(slitem)) as *mut SortGroupClause;
        let tle: *mut TargetEntry = get_sortgroupclause_tle(scl, *targetlist);

        if (*tle).resjunk {
            ereport!(
                ERROR,
                errmsg!(
                    "{}",
                    if is_agg {
                        "in an aggregate with DISTINCT, ORDER BY expressions must appear in argument list"
                    } else {
                        "for SELECT DISTINCT, ORDER BY expressions must appear in select list"
                    }
                ),
                /* C also: errcode(ERRCODE_INVALID_COLUMN_REFERENCE), parser_errposition */
            );
        }
        result = lappend(result, copyObject(scl) as *mut c_void);
    });

    /*
     * Now add any remaining non-resjunk tlist items, using default sort/group
     * semantics for their data types.
     */
    let mut tl_tmp = *targetlist;
    foreach!(tlitem, tl_tmp, {
        let tle: *mut TargetEntry = lfirst(current_cell!(tlitem)) as *mut TargetEntry;

        if (*tle).resjunk {
            // ignore junk
        } else {
            result = addTargetToGroupList(
                pstate,
                tle,
                result,
                *targetlist,
                exprLocation((*tle).expr as *mut Node),
            );
        }
    });

    /*
     * Complain if we found nothing to make DISTINCT.  Returning an empty list
     * would cause the parsed Query to look like it didn't have DISTINCT, with
     * results that would probably surprise the user.
     */
    if result.is_null() {
        ereport!(
            ERROR,
            errmsg!(
                "{}",
                if is_agg {
                    "an aggregate with DISTINCT must have at least one argument"
                } else {
                    "SELECT DISTINCT must have at least one column"
                }
            ),
            /* C also: errcode(ERRCODE_SYNTAX_ERROR) */
        );
    }

    result
}

/*
 * transformDistinctOnClause -
 *    transform a DISTINCT ON clause
 *
 * Since we may need to add items to the query's targetlist, that list
 * is passed by reference.
 *
 * As with GROUP BY, we absorb the sorting semantics of ORDER BY as much as
 * possible into the distinctClause.  This avoids a possible need to re-sort,
 * and allows the user to choose the equality semantics used by DISTINCT,
 * should she be working with a datatype that has more than one equality
 * operator.
 */
pub unsafe fn transformDistinctOnClause(
    pstate: *mut ParseState,
    distinctlist: *mut List,
    targetlist: *mut *mut List,
    sortClause: *mut List,
) -> *mut List {
    let mut result: *mut List = NIL;
    let mut sortgrouprefs: *mut List = NIL;
    let mut skipped_sortitem: bool = false;
    let mut lc: *mut ListCell = core::ptr::null_mut();
    let mut lc2: *mut ListCell = core::ptr::null_mut();

    /*
     * Add all the DISTINCT ON expressions to the tlist (if not already
     * present, they are added as resjunk items).  Assign sortgroupref numbers
     * to them, and make a list of these numbers.
     */
    let mut dl_tmp = distinctlist;
    foreach!(lc, dl_tmp, {
        let dexpr: *mut Node = lfirst(current_cell!(lc)) as *mut Node;
        let sortgroupref: c_int;
        let tle: *mut TargetEntry;

        let tle = findTargetlistEntrySQL92(
            pstate,
            dexpr,
            targetlist,
            EXPR_KIND_DISTINCT_ON,
        );
        let sgr = assignSortGroupRef(tle, *targetlist);
        sortgrouprefs = lappend_int(sortgrouprefs, sgr as c_int);
    });

    /*
     * If the user writes both DISTINCT ON and ORDER BY, adopt the sorting
     * semantics from ORDER BY items that match DISTINCT ON items, and also
     * adopt their column sort order.
     */
    skipped_sortitem = false;
    let mut sc_tmp = sortClause;
    foreach!(lc, sc_tmp, {
        let scl: *mut SortGroupClause = lfirst(current_cell!(lc)) as *mut SortGroupClause;

        if list_member_int(sortgrouprefs, (*scl).tleSortGroupRef as c_int) {
            if skipped_sortitem {
                ereport!(
                    ERROR,
                    errmsg!(
                        "SELECT DISTINCT ON expressions must match initial ORDER BY expressions"
                    ),
                    /* C also: errcode(ERRCODE_INVALID_COLUMN_REFERENCE), parser_errposition */
                );
            } else {
                result = lappend(result, copyObject(scl) as *mut c_void);
            }
        } else {
            skipped_sortitem = true;
        }
    });

    /*
     * Now add any remaining DISTINCT ON items, using default sort/group
     * semantics for their data types.
     */
    // forboth(lc, distinctlist, lc2, sortgrouprefs)
    {
        let mut lc_a = if !distinctlist.is_null() { list_head(distinctlist) } else { core::ptr::null_mut() };
        let mut lc_b = if !sortgrouprefs.is_null() { list_head(sortgrouprefs) } else { core::ptr::null_mut() };
        while !lc_a.is_null() && !lc_b.is_null() {
            let dexpr: *mut Node = lfirst(lc_a) as *mut Node;
            let sortgroupref: c_int = lfirst_int(lc_b);
            let tle: *mut TargetEntry = get_sortgroupref_tle(sortgroupref as Index, *targetlist);

            if !targetIsInSortList(tle, InvalidOid, result) {
                if skipped_sortitem {
                    ereport!(
                        ERROR,
                        errmsg!(
                            "SELECT DISTINCT ON expressions must match initial ORDER BY expressions"
                        ),
                        /* C also: errcode(ERRCODE_INVALID_COLUMN_REFERENCE), parser_errposition */
                    );
                }
                result = addTargetToGroupList(
                    pstate,
                    tle,
                    result,
                    *targetlist,
                    exprLocation(dexpr),
                );
            }
            lc_a = lnext(distinctlist, lc_a);
            lc_b = lnext(sortgrouprefs, lc_b);
        }
    }

    /*
     * An empty result list is impossible here because of grammar restrictions.
     */
    // Assert(result != NIL);

    result
}

/*
 * get_matching_location
 *      Get the exprLocation of the exprs member corresponding to the
 *      (first) member of sortgrouprefs that equals sortgroupref.
 *
 * This is used so that we can point at a troublesome DISTINCT ON entry.
 */
unsafe fn get_matching_location(
    sortgroupref: c_int,
    sortgrouprefs: *mut List,
    exprs: *mut List,
) -> c_int {
    // forboth(lcs, sortgrouprefs, lce, exprs)
    let mut lcs = if !sortgrouprefs.is_null() { list_head(sortgrouprefs) } else { core::ptr::null_mut() };
    let mut lce = if !exprs.is_null() { list_head(exprs) } else { core::ptr::null_mut() };
    while !lcs.is_null() && !lce.is_null() {
        if lfirst_int(lcs) == sortgroupref {
            return exprLocation(lfirst(lce) as *mut Node);
        }
        lcs = lnext(sortgrouprefs, lcs);
        lce = lnext(exprs, lce);
    }
    /* if no match, caller blew it */
    elog!(ERROR, "get_matching_location: no matching sortgroupref");
    -1 /* keep compiler quiet */
}

/*
 * resolve_unique_index_expr
 *      Infer a unique index from a list of indexElems, for ON CONFLICT clause
 *
 * Perform parse analysis of expressions and columns appearing within ON
 * CONFLICT clause.  During planning, the returned list of expressions is used
 * to infer which unique index to use.
 */
unsafe fn resolve_unique_index_expr(
    pstate: *mut ParseState,
    infer: *mut InferClause,
    heapRel: Relation,
) -> *mut List {
    let mut result: *mut List = NIL;
    let mut l: *mut ListCell = core::ptr::null_mut();

    let mut elems_tmp = (*infer).indexElems;
    foreach!(l, elems_tmp, {
        let ielem: *mut IndexElem = lfirst(current_cell!(l)) as *mut IndexElem;
        let pInfer: *mut InferenceElem = makeNode!(InferenceElem, T_InferenceElem);
        let parse: *mut Node;

        /*
         * Raw grammar re-uses CREATE INDEX infrastructure for unique index
         * inference clause, and so will accept opclasses by name and so on.
         *
         * Make no attempt to match ASC or DESC ordering or NULLS FIRST/NULLS
         * LAST ordering, since those are not significant for inference purposes.
         * Actively reject this as wrong-headed.
         */
        if (*ielem).ordering != SORTBY_DEFAULT {
            ereport!(
                ERROR,
                errmsg!("ASC/DESC is not allowed in ON CONFLICT clause"),
                /* C also: errcode(ERRCODE_INVALID_COLUMN_REFERENCE), parser_errposition */
            );
        }
        if (*ielem).nulls_ordering != SORTBY_NULLS_DEFAULT {
            ereport!(
                ERROR,
                errmsg!("NULLS FIRST/LAST is not allowed in ON CONFLICT clause"),
                /* C also: errcode(ERRCODE_INVALID_COLUMN_REFERENCE), parser_errposition */
            );
        }

        let parse = if (*ielem).expr.is_null() {
            /* Simple index attribute */
            /*
             * Grammar won't have built raw expression for us in event of
             * plain column reference.  Create one directly, and perform
             * expression transformation.  Planner expects this, and performs
             * its own normalization for the purposes of matching against
             * pg_index.
             */
            let n: *mut ColumnRef = makeNode!(ColumnRef, T_ColumnRef);
            (*n).fields = list_make1!(makeString((*ielem).name) as *mut c_void);
            /* Location is approximately that of inference specification */
            (*n).location = (*infer).location;
            n as *mut Node
        } else {
            /* Do parse transformation of the raw expression */
            (*ielem).expr as *mut Node
        };

        /*
         * transformExpr() will reject subqueries, aggregates, window
         * functions, and SRFs, based on being passed
         * EXPR_KIND_INDEX_EXPRESSION.
         */
        (*pInfer).expr = transformExpr(pstate, parse, EXPR_KIND_INDEX_EXPRESSION);

        /* Perform lookup of collation and operator class as required */
        if (*ielem).collation.is_null() {
            (*pInfer).infercollid = InvalidOid;
        } else {
            (*pInfer).infercollid = LookupCollation(
                pstate,
                (*ielem).collation,
                exprLocation((*pInfer).expr as *mut Node),
            );
        }

        if (*ielem).opclass.is_null() {
            (*pInfer).inferopclass = InvalidOid;
        } else {
            (*pInfer).inferopclass = get_opclass_oid(BTREE_AM_OID, (*ielem).opclass, false);
        }

        result = lappend(result, pInfer as *mut c_void);
    });

    result
}

/*
 * transformOnConflictArbiter -
 *      transform arbiter expressions in an ON CONFLICT clause.
 *
 * Transformed expressions used to infer one unique index relation to serve as
 * an ON CONFLICT arbiter.  Partial unique indexes may be inferred using WHERE
 * clause from inference specification clause.
 */
pub unsafe fn transformOnConflictArbiter(
    pstate: *mut ParseState,
    onConflictClause: *mut OnConflictClause,
    arbiterExpr: *mut *mut List,
    arbiterWhere: *mut *mut Node,
    constraint: *mut Oid,
) {
    let infer: *mut InferClause = (*onConflictClause).infer;

    *arbiterExpr = NIL;
    *arbiterWhere = core::ptr::null_mut();
    *constraint = InvalidOid;

    if (*onConflictClause).action == OnConflictAction::ONCONFLICT_UPDATE && infer.is_null() {
        ereport!(
            ERROR,
            errmsg!(
                "ON CONFLICT DO UPDATE requires inference specification or constraint name"
            ),
            /* C also: errcode(ERRCODE_SYNTAX_ERROR),
               errhint("For example, ON CONFLICT (column_name)."),
               parser_errposition */
        );
    }

    /*
     * To simplify certain aspects of its design, speculative insertion into
     * system catalogs is disallowed
     */
    if IsCatalogRelation((*pstate).p_target_relation) {
        ereport!(
            ERROR,
            errmsg!("ON CONFLICT is not supported with system catalog tables"),
            /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED), parser_errposition */
        );
    }

    /* Same applies to table used by logical decoding as catalog table */
    if RelationIsUsedAsCatalogTable((*pstate).p_target_relation) {
        ereport!(
            ERROR,
            errmsg!(
                "ON CONFLICT is not supported on table \"{}\" used as a catalog table",
                std::ffi::CStr::from_ptr(RelationGetRelationName((*pstate).p_target_relation as *mut crate::utils::rel::RelationData))
                    .to_string_lossy()
            ),
            /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED), parser_errposition */
        );
    }

    /* ON CONFLICT DO NOTHING does not require an inference clause */
    if !infer.is_null() {
        if !(*infer).indexElems.is_null() {
            *arbiterExpr = resolve_unique_index_expr(
                pstate,
                infer,
                (*pstate).p_target_relation,
            );
        }

        /* Handling inference WHERE clause (for partial unique index inference) */
        if !(*infer).whereClause.is_null() {
            *arbiterWhere = transformExpr(
                pstate,
                (*infer).whereClause,
                EXPR_KIND_INDEX_PREDICATE,
            );
        }

        /*
         * If the arbiter is specified by constraint name, get the constraint
         * OID and mark the constrained columns as requiring SELECT privilege,
         * in the same way as would have happened if the arbiter had been
         * specified by explicit reference to the constraint's index columns.
         */
        if !(*infer).conname.is_null() {
            let relid: Oid = RelationGetRelid((*pstate).p_target_relation as *mut crate::utils::rel::RelationData);
            let perminfo: *mut RTEPermissionInfo = (*(*pstate).p_target_nsitem).p_perminfo as *mut RTEPermissionInfo;
            let conattnos: *mut Bitmapset;

            let conattnos = get_relation_constraint_attnos(
                relid,
                (*infer).conname,
                false,
                constraint,
            );

            /* Make sure the rel as a whole is marked for SELECT access */
            (*perminfo).requiredPerms |= ACL_SELECT;
            /* Mark the constrained columns as requiring SELECT access */
            (*perminfo).selectedCols = bms_add_members((*perminfo).selectedCols, conattnos);
        }
    }

    /*
     * It's convenient to form a list of expressions based on the
     * representation used by CREATE INDEX, since the same restrictions are
     * appropriate (e.g. on subqueries).  However, from here on, a dedicated
     * primnode representation is used for inference elements, and so
     * assign_query_collations() can be trusted to do the right thing with the
     * post parse analysis query tree inference clause representation.
     */
}

/*
 * addTargetToSortList
 *      If the given targetlist entry isn't already in the SortGroupClause
 *      list, add it to the end of the list, using the given sort ordering
 *      info.
 *
 * Returns the updated SortGroupClause list.
 */
pub unsafe fn addTargetToSortList(
    pstate: *mut ParseState,
    tle: *mut TargetEntry,
    sortlist: *mut List,
    targetlist: *mut List,
    sortby: *mut SortBy,
) -> *mut List {
    let mut sortlist = sortlist;
    let mut restype: Oid = exprType((*tle).expr as *mut Node);
    let mut sortop: Oid = InvalidOid;
    let mut eqop: Oid = InvalidOid;
    let mut hashable: bool = false;
    let mut reverse: bool = false;
    let mut location: c_int;
    let mut pcbstate: ParseCallbackState = core::mem::zeroed();

    /* if tlist item is an UNKNOWN literal, change it to TEXT */
    if restype == UNKNOWNOID {
        (*tle).expr = coerce_type(
            pstate,
            (*tle).expr as *mut Node,
            restype,
            TEXTOID,
            -1,
            COERCION_IMPLICIT,
            COERCE_IMPLICIT_CAST as c_int,
            -1,
        ) as *mut Expr;
        restype = TEXTOID;
    }

    /*
     * Rather than clutter the API of get_sort_group_operators and the other
     * functions we're about to use, make use of error context callback to
     * mark any error reports with a parse position.
     */
    location = (*sortby).location;
    if location < 0 {
        location = exprLocation((*sortby).node);
    }
    setup_parser_errposition_callback(&mut pcbstate, pstate, location);

    /* determine the sortop, eqop, and directionality */
    match (*sortby).sortby_dir {
        x if x == SORTBY_DEFAULT || x == SORTBY_ASC => {
            get_sort_group_operators(
                restype,
                true, true, false,
                &mut sortop, &mut eqop, core::ptr::null_mut(),
                &mut hashable,
            );
            reverse = false;
        }
        x if x == SORTBY_DESC => {
            get_sort_group_operators(
                restype,
                false, true, true,
                core::ptr::null_mut(), &mut eqop, &mut sortop,
                &mut hashable,
            );
            reverse = true;
        }
        x if x == SORTBY_USING => {
            // Assert(sortby->useOp != NIL);
            sortop = compatible_oper_opid(
                (*sortby).useOp,
                restype,
                restype,
                false,
            );

            /*
             * Verify it's a valid ordering operator, fetch the corresponding
             * equality operator, and determine whether to consider it like
             * ASC or DESC.
             */
            eqop = get_equality_op_for_ordering_op(sortop, &mut reverse);
            if !OidIsValid(eqop) {
                ereport!(
                    ERROR,
                    errmsg!(
                        "operator {} is not a valid ordering operator",
                        std::ffi::CStr::from_ptr(strVal!(llast((*sortby).useOp) as *mut crate::nodes::nodes::Node)).to_string_lossy()
                    ),
                    /* C also: errcode(ERRCODE_WRONG_OBJECT_TYPE),
                       errhint("Ordering operators must be \"<\" or \">\" members of btree operator families.") */
                );
            }

            /* Also see if the equality operator is hashable. */
            hashable = op_hashjoinable(eqop, restype);
        }
        _ => {
            elog!(ERROR, "unrecognized sortby_dir: {:?}", (*sortby).sortby_dir);
            sortop = InvalidOid; /* keep compiler quiet */
            eqop = InvalidOid;
            hashable = false;
            reverse = false;
        }
    }

    cancel_parser_errposition_callback(&mut pcbstate);

    /* avoid making duplicate sortlist entries */
    if !targetIsInSortList(tle, sortop, sortlist) {
        let sortcl: *mut SortGroupClause = makeNode!(SortGroupClause, T_SortGroupClause);

        (*sortcl).tleSortGroupRef = assignSortGroupRef(tle, targetlist);
        (*sortcl).eqop = eqop;
        (*sortcl).sortop = sortop;
        (*sortcl).hashable = hashable;
        (*sortcl).reverse_sort = reverse;

        match (*sortby).sortby_nulls {
            x if x == SORTBY_NULLS_DEFAULT => {
                /* NULLS FIRST is default for DESC; other way for ASC */
                (*sortcl).nulls_first = reverse;
            }
            x if x == SORTBY_NULLS_FIRST => {
                (*sortcl).nulls_first = true;
            }
            x if x == SORTBY_NULLS_LAST => {
                (*sortcl).nulls_first = false;
            }
            _ => {
                elog!(ERROR, "unrecognized sortby_nulls: {:?}", (*sortby).sortby_nulls);
            }
        }

        sortlist = lappend(sortlist, sortcl as *mut c_void);
    }

    sortlist
}

/*
 * addTargetToGroupList
 *      If the given targetlist entry isn't already in the SortGroupClause
 *      list, add it to the end of the list, using default sort/group
 *      semantics.
 *
 * This is very similar to addTargetToSortList, except that we allow the
 * case where only a grouping (equality) operator can be found, and that
 * the TLE is considered "already in the list" if it appears there with any
 * sorting semantics.
 *
 * location is the parse location to be fingered in event of trouble.
 *
 * Returns the updated SortGroupClause list.
 */
unsafe fn addTargetToGroupList(
    pstate: *mut ParseState,
    tle: *mut TargetEntry,
    grouplist: *mut List,
    targetlist: *mut List,
    location: c_int,
) -> *mut List {
    let mut grouplist = grouplist;
    let mut restype: Oid = exprType((*tle).expr as *mut Node);

    /* if tlist item is an UNKNOWN literal, change it to TEXT */
    if restype == UNKNOWNOID {
        (*tle).expr = coerce_type(
            pstate,
            (*tle).expr as *mut Node,
            restype,
            TEXTOID,
            -1,
            COERCION_IMPLICIT,
            COERCE_IMPLICIT_CAST as c_int,
            -1,
        ) as *mut Expr;
        restype = TEXTOID;
    }

    /* avoid making duplicate grouplist entries */
    if !targetIsInSortList(tle, InvalidOid, grouplist) {
        let grpcl: *mut SortGroupClause = makeNode!(SortGroupClause, T_SortGroupClause);
        let mut sortop: Oid = InvalidOid;
        let mut eqop: Oid = InvalidOid;
        let mut hashable: bool = false;
        let mut pcbstate: ParseCallbackState = core::mem::zeroed();

        setup_parser_errposition_callback(&mut pcbstate, pstate, location);

        /* determine the eqop and optional sortop */
        get_sort_group_operators(
            restype,
            false, true, false,
            &mut sortop, &mut eqop, core::ptr::null_mut(),
            &mut hashable,
        );

        cancel_parser_errposition_callback(&mut pcbstate);

        (*grpcl).tleSortGroupRef = assignSortGroupRef(tle, targetlist);
        (*grpcl).eqop = eqop;
        (*grpcl).sortop = sortop;
        (*grpcl).reverse_sort = false; /* sortop is "less than", or InvalidOid */
        (*grpcl).nulls_first = false;  /* OK with or without sortop */
        (*grpcl).hashable = hashable;

        grouplist = lappend(grouplist, grpcl as *mut c_void);
    }

    grouplist
}

/*
 * assignSortGroupRef
 *    Assign the targetentry an unused ressortgroupref, if it doesn't
 *    already have one.  Return the assigned or pre-existing refnumber.
 *
 * 'tlist' is the targetlist containing (or to contain) the given targetentry.
 */
pub unsafe fn assignSortGroupRef(tle: *mut TargetEntry, tlist: *mut List) -> Index {
    let mut maxRef: Index = 0;
    let mut l: *mut ListCell = core::ptr::null_mut();

    if (*tle).ressortgroupref != 0 {
        /* already has one? */
        return (*tle).ressortgroupref;
    }

    /* easiest way to pick an unused refnumber: max used + 1 */
    let mut tl_tmp = tlist;
    foreach!(l, tl_tmp, {
        let r#ref: Index = (*(lfirst(current_cell!(l)) as *mut TargetEntry)).ressortgroupref;
        if r#ref > maxRef {
            maxRef = r#ref;
        }
    });
    (*tle).ressortgroupref = maxRef + 1;
    (*tle).ressortgroupref
}

/*
 * targetIsInSortList
 *      Is the given target item already in the sortlist?
 *      If sortop is not InvalidOid, also test for a match to the sortop.
 *
 * It is not an oversight that this function ignores the nulls_first flag.
 * We check sortop when determining if an ORDER BY item is redundant with
 * earlier ORDER BY items, because it's conceivable that "ORDER BY
 * foo USING <, foo USING <<<" is not redundant, if <<< distinguishes
 * values that < considers equal.  We need not check nulls_first
 * however, because a lower-order column with the same sortop but
 * opposite nulls direction is redundant.  Also, we can consider
 * ORDER BY foo ASC, foo DESC redundant, so check for a commutator match.
 *
 * Works for both ordering and grouping lists (sortop would normally be
 * InvalidOid when considering grouping).
 */
pub unsafe fn targetIsInSortList(
    tle: *mut TargetEntry,
    sortop: Oid,
    sortList: *mut List,
) -> bool {
    let r#ref: Index = (*tle).ressortgroupref;
    let mut l: *mut ListCell = core::ptr::null_mut();

    /* no need to scan list if tle has no marker */
    if r#ref == 0 {
        return false;
    }

    let mut sl_tmp = sortList;
    foreach!(l, sl_tmp, {
        let scl: *mut SortGroupClause = lfirst(current_cell!(l)) as *mut SortGroupClause;

        if (*scl).tleSortGroupRef == r#ref
            && (sortop == InvalidOid
                || sortop == (*scl).sortop
                || sortop == get_commutator((*scl).sortop))
        {
            return true;
        }
    });
    false
}

/*
 * findWindowClause
 *      Find the named WindowClause in the list, or return NULL if not there
 */
unsafe fn findWindowClause(wclist: *mut List, name: *const c_char) -> *mut WindowClause {
    let mut l: *mut ListCell = core::ptr::null_mut();

    let mut wl_tmp = wclist;
    foreach!(l, wl_tmp, {
        let wc: *mut WindowClause = lfirst(current_cell!(l)) as *mut WindowClause;

        if !(*wc).name.is_null()
            && strcmp((*wc).name, name) == 0
        {
            return wc;
        }
    });

    core::ptr::null_mut()
}

/*
 * transformFrameOffset
 *      Process a window frame offset expression
 *
 * In RANGE mode, rangeopfamily is the sort opfamily for the input ORDER BY
 * column, and rangeopcintype is the input data type the sort operator is
 * registered with.  We expect the in_range function to be registered with
 * that same type.  (In binary-compatible cases, it might be different from
 * the input column's actual type, so we can't use that for the lookups.)
 * We'll return the OID of the in_range function to *inRangeFunc.
 */
unsafe fn transformFrameOffset(
    pstate: *mut ParseState,
    frameOptions: c_int,
    rangeopfamily: Oid,
    rangeopcintype: Oid,
    inRangeFunc: *mut Oid,
    clause: *mut Node,
) -> *mut Node {
    let mut constructName: *const c_char = core::ptr::null();
    let mut node: *mut Node;

    *inRangeFunc = InvalidOid; /* default result */

    /* Quick exit if no offset expression */
    if clause.is_null() {
        return core::ptr::null_mut();
    }

    if (frameOptions & FRAMEOPTION_ROWS) != 0 {
        /* Transform the raw expression tree */
        node = transformExpr(pstate, clause, EXPR_KIND_WINDOW_FRAME_ROWS);

        /* Like LIMIT clause, simply coerce to int8 */
        constructName = b"ROWS\0".as_ptr() as *const c_char;
        node = coerce_to_specific_type(pstate, node, INT8OID, constructName);
    } else if (frameOptions & FRAMEOPTION_RANGE) != 0 {
        /*
         * We must look up the in_range support function that's to be used,
         * possibly choosing one of several, and coerce the "offset" value to
         * the appropriate input type.
         */
        let nodeType: Oid;
        let preferredType: Oid;
        let mut nfuncs: c_int = 0;
        let mut nmatches: c_int = 0;
        let mut selectedType: Oid = InvalidOid;
        let mut selectedFunc: Oid = InvalidOid;
        let proclist: *mut CatCList;
        let mut i: c_int;

        /* Transform the raw expression tree */
        node = transformExpr(pstate, clause, EXPR_KIND_WINDOW_FRAME_RANGE);
        let nodeType = exprType(node);

        /*
         * If there are multiple candidates, we'll prefer the one that exactly
         * matches nodeType; or if nodeType is as yet unknown, prefer the one
         * that exactly matches the sort column type.
         */
        let preferredType = if nodeType != UNKNOWNOID { nodeType } else { rangeopcintype };

        /* Find the in_range support functions applicable to this case */
        let proclist = SearchSysCacheList(
            AMPROCNUM,
            2,
            ObjectIdGetDatum(rangeopfamily),
            ObjectIdGetDatum(rangeopcintype),
            0, // unused 3rd key
        ) as *mut CatCList;
        i = 0;
        while i < (*proclist).n_members {
            let proctup: *const HeapTupleData =
                (*proclist).member_tuple(i as usize);
            let procform: *mut FormData_pg_amproc =
                GETSTRUCT(proctup) as *mut FormData_pg_amproc;

            /* The search will find all support proc types; ignore others */
            if (*procform).amprocnum != BTINRANGE_PROC {
                i += 1;
                continue;
            }
            nfuncs += 1;

            /* Ignore function if given value can't be coerced to that type */
            if !can_coerce_type(1, &nodeType, &(*procform).amprocrighttype, COERCION_IMPLICIT) {
                i += 1;
                continue;
            }
            nmatches += 1;

            /* Remember preferred match, or any match if didn't find that */
            if selectedType != preferredType {
                selectedType = (*procform).amprocrighttype;
                selectedFunc = (*procform).amproc;
            }
            i += 1;
        }
        // ReleaseCatCacheList stub: catcache not yet ported

        /*
         * Throw error if needed.  It seems worth taking the trouble to
         * distinguish "no support at all" from "you didn't match any
         * available offset type".
         */
        if nfuncs == 0 {
            ereport!(
                ERROR,
                errmsg!(
                    "RANGE with offset PRECEDING/FOLLOWING is not supported for column type {}",
                    std::ffi::CStr::from_ptr(format_type_be(rangeopcintype)).to_string_lossy()
                ),
                /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED), parser_errposition */
            );
        }
        if nmatches == 0 {
            ereport!(
                ERROR,
                errmsg!(
                    "RANGE with offset PRECEDING/FOLLOWING is not supported for column type {} and offset type {}",
                    std::ffi::CStr::from_ptr(format_type_be(rangeopcintype)).to_string_lossy(),
                    std::ffi::CStr::from_ptr(format_type_be(nodeType)).to_string_lossy()
                ),
                /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                   errhint("Cast the offset value to an appropriate type."), parser_errposition */
            );
        }
        if nmatches != 1 && selectedType != preferredType {
            ereport!(
                ERROR,
                errmsg!(
                    "RANGE with offset PRECEDING/FOLLOWING has multiple interpretations for column type {} and offset type {}",
                    std::ffi::CStr::from_ptr(format_type_be(rangeopcintype)).to_string_lossy(),
                    std::ffi::CStr::from_ptr(format_type_be(nodeType)).to_string_lossy()
                ),
                /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                   errhint("Cast the offset value to the exact intended type."), parser_errposition */
            );
        }

        /* OK, coerce the offset to the right type */
        constructName = b"RANGE\0".as_ptr() as *const c_char;
        node = coerce_to_specific_type(pstate, node, selectedType, constructName);
        *inRangeFunc = selectedFunc;
    } else if (frameOptions & FRAMEOPTION_GROUPS) != 0 {
        /* Transform the raw expression tree */
        node = transformExpr(pstate, clause, EXPR_KIND_WINDOW_FRAME_GROUPS);

        /* Like LIMIT clause, simply coerce to int8 */
        constructName = b"GROUPS\0".as_ptr() as *const c_char;
        node = coerce_to_specific_type(pstate, node, INT8OID, constructName);
    } else {
        // Assert(false);
        node = core::ptr::null_mut();
    }

    /* Disallow variables in frame offsets */
    checkExprIsVarFree(pstate, node, constructName);

    node
}

