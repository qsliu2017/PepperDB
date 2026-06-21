/*-------------------------------------------------------------------------
 *
 * analyze.rs
 *   transform the raw parse tree into a query tree
 *
 * For optimizable statements, we are careful to obtain a suitable lock on
 * each referenced table, and other modules of the backend preserve or
 * re-obtain these locks before depending on the results.  It is therefore
 * okay to do significant semantic analysis of these statements.  For
 * utility commands, no locks are obtained here (and if they were, we could
 * not be sure we'd still have them at execution).  Hence the general rule
 * for utility commands is to just dump them into a Query node untransformed.
 * DECLARE CURSOR, EXPLAIN, and CREATE TABLE AS are exceptions because they
 * contain optimizable statements, which we should transform.
 *
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *   src/backend/parser/analyze.c
 *
 *-------------------------------------------------------------------------
 */

#![allow(unused_variables)]
#![allow(dead_code)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(unused_mut)]
#![allow(unused_imports)]
#![allow(unreachable_patterns)]

use crate::prelude::*;

use std::ffi::{c_char, c_int, c_void, CStr};
use core::mem::size_of;

use crate::{
    castNode, current_cell, foreach, forboth, forthree, forfour,
    intVal, lfirst_node, linitial_node, list_make1, list_make2,
    makeNode, strVal, IsA,
};

// ---------------------------------------------------------------------------
// Standard library / crate imports
// ---------------------------------------------------------------------------
use crate::postgres_ext::{Oid, InvalidOid};
use crate::postgres::{Datum, ObjectIdGetDatum, Int32GetDatum};
use crate::c::{OidIsValid, int32};

use crate::nodes::nodes::{
    nodeTag, Node, NodeTag, NodeTag::*,
    CmdType, CmdType::*,
    JoinType, JoinType::*,
    OnConflictAction,
    LimitOption,
};
use crate::nodes::pg_list::{
    List, NIL,
    lfirst, lfirst_int, lfirst_oid, linitial, lsecond, llast, lnext,
    lappend, lappend_int, lappend_oid, lcons, list_head,
    list_concat, list_length, list_make1_impl,
    list_nth, list_nth_cell, list_truncate, list_member_int,
    list_copy, list_delete_last, list_free,
    ListCell,
};
use crate::nodes::bitmapset::{
    Bitmapset, bms_add_member, bms_add_members, bms_is_member,
};
use crate::nodes::nodeFuncs::{
    exprType, exprTypmod, exprCollation, exprLocation,
    expression_returns_set,
    raw_expression_tree_walker,
};
use crate::optimizer::util::var::{contain_vars_of_level, locate_var_of_level};
use crate::parser::parse_relation::isQueryUsingTempRelation;
use crate::parser::parse_param::query_contains_extern_params;
use crate::nodes::makefuncs::{
    makeConst, makeBoolExpr, makeVar, makeTargetEntry,
    makeSimpleA_Expr, makeNullConst, makeAlias, makeFromExpr,
    makeVarFromTargetEntry, RECORDOID,
};
use crate::nodes::value::{makeString, String as PgString};
use crate::nodes::primnodes::{
    Expr, Var, Alias, TargetEntry, JoinExpr, RangeTblRef, RangeVar,
    CoalesceExpr, BoolExpr, BoolExprType,
    VarReturningType, VarReturningType::*,
    CoercionForm, CoercionForm::*,
    CoercionContext, CoercionContext::*,
    InferenceElem, FuncExpr,
    FieldStore, SubscriptingRef,
    SetToDefault, CoerceToDomain,
    RowExpr, Const, Param,
    IntoClause, OnConflictExpr,
};
use crate::nodes::parsenodes::{
    Query, ColumnRef, A_Const, A_Expr, FuncCall, SortBy, SelectStmt,
    RangeSubselect, RangeFunction, RangeTblEntry, RangeTblFunction,
    RTEKind, RTEKind::*,
    RTEPermissionInfo, RowMarkClause, CommonTableExpr,
    GroupingSet, GroupingSetKind, GroupingSetKind::*,
    WindowDef, WindowClause, SortGroupClause,
    OnConflictClause, InferClause,
    IndexElem, LockingClause, ResTarget, ReturningClause, ReturningOption,
    InsertStmt, DeleteStmt, UpdateStmt, MergeStmt, ReturnStmt,
    PLAssignStmt, DeclareCursorStmt, ExplainStmt, CreateTableAsStmt,
    CallStmt, RawStmt, WithClause,
    SortByDir, SortByDir::*,
    SortByNulls, SortByNulls::*,
    AclMode,
    ACL_INSERT, ACL_UPDATE, ACL_DELETE, ACL_SELECT, ACL_SELECT_FOR_UPDATE,
    CURSOR_OPT_SCROLL, CURSOR_OPT_NO_SCROLL, CURSOR_OPT_HOLD,
    CURSOR_OPT_ASENSITIVE, CURSOR_OPT_INSENSITIVE,
    RETURNING_OPTION_OLD, RETURNING_OPTION_NEW,
    SETOP_NONE, SETOP_UNION, SETOP_INTERSECT,
    OBJECT_TABLE, OBJECT_MATVIEW,
    SetOperationStmt, SetOperation, SetOperation::*,
    QuerySource, QuerySource::*,
    DefElem,
};
use crate::nodes::lockoptions::{LockClauseStrength, LockClauseStrength::*, LockWaitPolicy};

use crate::parser::parse_node::{
    cancel_parser_errposition_callback, parser_errposition,
    setup_parser_errposition_callback,
    Index, ParseCallbackState, ParseExprKind, ParseExprKind::*,
    ParseNamespaceColumn, ParseNamespaceItem, ParseState, Relation,
    make_parsestate, free_parsestate,
    QueryEnvironment,
};
use crate::nodes::params::ParserSetupHook;
use crate::parser::parse_relation::{
    refnameNamespaceItem, GetRTEByRangeTablePosn,
    get_parse_rowmark, get_tle_by_resno,
};
use crate::parser::parsetree::rt_fetch;
use crate::parser::parse_expr::transformExpr;
use crate::parser::parse_collate::{
    assign_expr_collations, assign_query_collations,
    assign_list_collations,
};
use crate::parser::parse_clause::{
    transformFromClause, transformWhereClause, transformSortClause,
    transformGroupClause, transformDistinctClause, transformDistinctOnClause,
    transformLimitClause, transformWindowDefinitions,
    setTargetTable,
    transformOnConflictArbiter,
    select_common_type, select_common_typmod,
};
use crate::parser::parse_cte::transformWithClause;
use crate::parser::parse_collate::select_common_collation;
use crate::parser::parse_coerce::coerce_to_common_type;
use crate::parser::parse_relation::{
    addRangeTableEntryForValues, expandNSItemVars, expandNSItemAttrs,
};
use crate::parser::parse_target::{
    transformTargetList, markTargetListOrigins,
    transformAssignedExpr, transformAssignmentIndirection,
    updateTargetListEntry, resolveTargetListUnknowns,
    transformExpressionList,
    checkInsertTargets,
};
use crate::parser::parse_agg::{
    parseCheckAggregates,
};
use crate::parser::parse_relation::{
    addRangeTableEntry,
    addRangeTableEntryForRelation,
    addRangeTableEntryForSubquery,
    addRangeTableEntryForFunction,
    addRangeTableEntryForTableFunc,
    addRangeTableEntryForJoin,
    addRangeTableEntryForCTE,
    addNSItemToQuery,
    checkNameSpaceConflicts,
};
use crate::parser::parse_coerce::coerce_to_target_type;
use crate::parser::parse_cte::analyzeCTETargetList;
use crate::parser::parse_func::ParseFuncOrColumn;
use crate::parser::parse_merge::transformMergeStmt;
use crate::parser::parse_oper::get_sort_group_operators;
use crate::parser::parse_param::{
    setup_parse_fixed_parameters, setup_parse_variable_parameters,
    check_variable_parameters,
};

use crate::access::sysattr::FirstLowInvalidHeapAttributeNumber;
use crate::access::attnum::{AttrNumber, InvalidAttrNumber};
use crate::access::common::tupdesc::{TupleDesc, TupleDescAttr};
use crate::access::htup_details::{HeapTuple, HeapTupleData, GETSTRUCT};

use crate::catalog::pg_proc::{
    PROARGMODE_IN, PROARGMODE_OUT, PROARGMODE_INOUT, PROARGMODE_VARIADIC,
    Form_pg_proc,
};
/* TODO(pg-port): syscache.h PROCOID id */
const PROCOID: c_int = 47;
/* TODO(pg-port): pg_proc.h Anum_pg_proc_proargmodes */
const Anum_pg_proc_proargmodes: crate::access::attnum::AttrNumber = 20;
use crate::catalog::pg_type_d::{CHAROID, INT4OID, RECORDOID as pg_RECORDOID, _RECORDOID as RECORDARRAYOID, UNKNOWNOID};
use crate::catalog::pg_attribute::Form_pg_attribute;
use crate::catalog::pg_class::{RELKIND_RELATION, RELKIND_PARTITIONED_TABLE,
    RELKIND_COMPOSITE_TYPE, RELPERSISTENCE_UNLOGGED};

use crate::utils::cache::syscache::{
    ReleaseSysCache, SearchSysCache1,
    SysCacheGetAttr,
};
use crate::access::htup_details::HeapTupleIsValid;
use crate::utils::cache::lsyscache::{pstrdup, palloc, pfree, format_type_be};
use crate::utils::mmgr::mcxt::palloc0;
use crate::utils::rel::{
    RelationGetRelationName, RelationGetRelid, RelationGetNumberOfAttributes,
    RelationData,
};
use crate::parser::parse_relation::attnameAttNum;
use crate::parser::parse_relation::getRTEPermissionInfo;
/* TODO(pg-port): utils/lsyscache.c ISCOMPLEX */
#[inline] unsafe fn ISCOMPLEX(typid: Oid) -> bool { false /* TODO(pg-port) */ }
/* TODO(pg-port): utils/adt/arrayfuncs.c array helpers not yet ported */
#[repr(C)] pub struct ArrayType { _opaque: u8 }
unsafe fn DatumGetArrayTypeP(d: Datum) -> *mut ArrayType { d as *mut ArrayType }
unsafe fn ARR_NDIM(a: *mut ArrayType) -> c_int { crate::utils::array::ARR_NDIM(a as _) }
unsafe fn ARR_DIMS(a: *mut ArrayType) -> *mut c_int { crate::utils::array::ARR_DIMS(a as _) as _ }
unsafe fn ARR_HASNULL(_a: *mut ArrayType) -> bool { crate::utils::array::ARR_HASNULL(_a as _) }
unsafe fn ARR_ELEMTYPE(_a: *mut ArrayType) -> Oid { crate::utils::array::ARR_ELEMTYPE(_a as _) }
unsafe fn ARR_DATA_PTR(a: *mut ArrayType) -> *mut u8 { crate::utils::array::ARR_DATA_PTR(a as _) as _ }
use crate::optimizer::optimizer::expand_function_arguments;

use crate::nodes::queryjumble::{JumbleState, JumbleQuery};
use crate::miscadmin::check_stack_depth;
use crate::nodes::queryjumble::IsQueryIdEnabled;
use crate::commands::defrem::defGetBoolean;

/* TODO(pg-port): nodes/copyfuncs.c copyObject */
#[inline] unsafe fn copyObject(from: *mut c_void) -> *mut c_void { from }
use crate::utils::activity::backend_status::pgstat_report_query_id;

// ---------------------------------------------------------------------------
// Hook for plugins to get control at end of parse analysis
// ---------------------------------------------------------------------------

pub type post_parse_analyze_hook_type = Option<
    unsafe extern "C" fn(*mut ParseState, *mut Query, *mut JumbleState),
>;

#[no_mangle]
pub static mut post_parse_analyze_hook: post_parse_analyze_hook_type = None;

// ============================================================================
// Part 1 ends here.
// ============================================================================

/*
 * parse_analyze_fixedparams
 *      Analyze a raw parse tree and transform it to Query form.
 *
 * Optionally, information about $n parameter types can be supplied.
 * References to $n indexes not defined by paramTypes[] are disallowed.
 *
 * The result is a Query node.  Optimizable statements require considerable
 * transformation, while utility-type statements are simply hung off
 * a dummy CMD_UTILITY Query node.
 */
pub unsafe fn parse_analyze_fixedparams(
    parseTree: *mut RawStmt,
    sourceText: *const c_char,
    paramTypes: *const Oid,
    numParams: c_int,
    queryEnv: *mut QueryEnvironment,
) -> *mut Query {
    let mut pstate: *mut ParseState = make_parsestate(core::ptr::null_mut());
    let mut query: *mut Query;
    let mut jstate: *mut JumbleState = core::ptr::null_mut();

    /* Assert(sourceText != NULL); -- required as of 8.4 */
    debug_assert!(!sourceText.is_null());

    (*pstate).p_sourcetext = sourceText;

    if numParams > 0 {
        setup_parse_fixed_parameters(pstate as *mut crate::parser::parse_param::ParseState, paramTypes, numParams);
    }

    (*pstate).p_queryEnv = queryEnv;

    query = transformTopLevelStmt(pstate, parseTree);

    if IsQueryIdEnabled() {
        jstate = JumbleQuery(query);
    }

    if let Some(hook) = post_parse_analyze_hook {
        hook(pstate, query, jstate);
    }

    free_parsestate(pstate);

    pgstat_report_query_id((*query).queryId, false);

    query
}

/*
 * parse_analyze_varparams
 *
 * This variant is used when it's okay to deduce information about $n
 * symbol datatypes from context.  The passed-in paramTypes[] array can
 * be modified or enlarged (via repalloc).
 */
pub unsafe fn parse_analyze_varparams(
    parseTree: *mut RawStmt,
    sourceText: *const c_char,
    paramTypes: *mut *mut Oid,
    numParams: *mut c_int,
    queryEnv: *mut QueryEnvironment,
) -> *mut Query {
    let mut pstate: *mut ParseState = make_parsestate(core::ptr::null_mut());
    let mut query: *mut Query;
    let mut jstate: *mut JumbleState = core::ptr::null_mut();

    debug_assert!(!sourceText.is_null());

    (*pstate).p_sourcetext = sourceText;

    setup_parse_variable_parameters(pstate as *mut crate::parser::parse_param::ParseState, paramTypes, numParams);

    (*pstate).p_queryEnv = queryEnv;

    query = transformTopLevelStmt(pstate, parseTree);

    /* make sure all is well with parameter types */
    check_variable_parameters(pstate as *mut crate::parser::parse_param::ParseState, query);

    if IsQueryIdEnabled() {
        jstate = JumbleQuery(query);
    }

    if let Some(hook) = post_parse_analyze_hook {
        hook(pstate, query, jstate);
    }

    free_parsestate(pstate);

    pgstat_report_query_id((*query).queryId, false);

    query
}

/*
 * parse_analyze_withcb
 *
 * This variant is used when the caller supplies their own parser callback to
 * resolve parameters and possibly other things.
 */
pub unsafe fn parse_analyze_withcb(
    parseTree: *mut RawStmt,
    sourceText: *const c_char,
    parserSetup: ParserSetupHook,
    parserSetupArg: *mut c_void,
    queryEnv: *mut QueryEnvironment,
) -> *mut Query {
    let mut pstate: *mut ParseState = make_parsestate(core::ptr::null_mut());
    let mut query: *mut Query;
    let mut jstate: *mut JumbleState = core::ptr::null_mut();

    debug_assert!(!sourceText.is_null());

    (*pstate).p_sourcetext = sourceText;
    (*pstate).p_queryEnv = queryEnv;
    if let Some(hook) = parserSetup { hook(pstate as *mut crate::nodes::params::ParseState, parserSetupArg); }

    query = transformTopLevelStmt(pstate, parseTree);

    if IsQueryIdEnabled() {
        jstate = JumbleQuery(query);
    }

    if let Some(hook) = post_parse_analyze_hook {
        hook(pstate, query, jstate);
    }

    free_parsestate(pstate);

    pgstat_report_query_id((*query).queryId, false);

    query
}

/*
 * parse_sub_analyze
 *      Entry point for recursively analyzing a sub-statement.
 */
pub unsafe fn parse_sub_analyze(
    parseTree: *mut Node,
    parentParseState: *mut ParseState,
    parentCTE: *mut CommonTableExpr,
    locked_from_parent: bool,
    resolve_unknowns: bool,
) -> *mut Query {
    let mut pstate: *mut ParseState = make_parsestate(parentParseState);
    let mut query: *mut Query;

    (*pstate).p_parent_cte = parentCTE as *mut c_void;
    (*pstate).p_locked_from_parent = locked_from_parent;
    (*pstate).p_resolve_unknowns = resolve_unknowns;

    query = transformStmt(pstate, parseTree);

    free_parsestate(pstate);

    query
}

/*
 * transformTopLevelStmt -
 *    transform a Parse tree into a Query tree.
 *
 * This function is just responsible for transferring statement location data
 * from the RawStmt into the finished Query.
 */
pub unsafe fn transformTopLevelStmt(pstate: *mut ParseState, parseTree: *mut RawStmt) -> *mut Query {
    let mut result: *mut Query;

    /* We're at top level, so allow SELECT INTO */
    result = transformOptionalSelectInto(pstate, (*parseTree).stmt);

    (*result).stmt_location = (*parseTree).stmt_location;
    (*result).stmt_len = (*parseTree).stmt_len;

    result
}

/*
 * transformOptionalSelectInto -
 *    If SELECT has INTO, convert it to CREATE TABLE AS.
 *
 * The only thing we do here that we don't do in transformStmt() is to
 * convert SELECT ... INTO into CREATE TABLE AS.  Since utility statements
 * aren't allowed within larger statements, this is only allowed at the top
 * of the parse tree, and so we only try it before entering the recursive
 * transformStmt() processing.
 */
unsafe fn transformOptionalSelectInto(pstate: *mut ParseState, parseTree: *mut Node) -> *mut Query {
    if IsA!(parseTree, T_SelectStmt) {
        let mut stmt: *mut SelectStmt = parseTree as *mut SelectStmt;

        /* If it's a set-operation tree, drill down to leftmost SelectStmt */
        while !stmt.is_null() && (*stmt).op != SETOP_NONE {
            stmt = (*stmt).larg;
        }
        debug_assert!(!stmt.is_null() && IsA!(stmt as *mut Node, T_SelectStmt) && (*stmt).larg.is_null());

        if !(*stmt).intoClause.is_null() {
            let mut ctas: *mut CreateTableAsStmt = makeNode!(CreateTableAsStmt, T_CreateTableAsStmt);

            (*ctas).query = parseTree;
            (*ctas).into = (*stmt).intoClause;
            (*ctas).objtype = OBJECT_TABLE;
            (*ctas).is_select_into = true;

            /*
             * Remove the intoClause from the SelectStmt.  This makes it safe
             * for transformSelectStmt to complain if it finds intoClause set
             * (implying that the INTO appeared in a disallowed place).
             */
            (*stmt).intoClause = core::ptr::null_mut();

            /* parseTree = (Node *) ctas; */
            return transformStmt(pstate, ctas as *mut Node);
        }
    }

    transformStmt(pstate, parseTree)
}

/*
 * transformStmt -
 *    recursively transform a Parse tree into a Query tree.
 */
pub unsafe fn transformStmt(pstate: *mut ParseState, parseTree: *mut Node) -> *mut Query {
    let mut result: *mut Query;

    /*
     * Caution: when changing the set of statement types that have non-default
     * processing here, see also stmt_requires_parse_analysis() and
     * analyze_requires_snapshot().
     */
    match nodeTag(parseTree) {
        /*
         * Optimizable statements
         */
        T_InsertStmt => {
            result = transformInsertStmt(pstate, parseTree as *mut InsertStmt);
        }
        T_DeleteStmt => {
            result = transformDeleteStmt(pstate, parseTree as *mut DeleteStmt);
        }
        T_UpdateStmt => {
            result = transformUpdateStmt(pstate, parseTree as *mut UpdateStmt);
        }
        T_MergeStmt => {
            result = transformMergeStmt(pstate as *mut crate::parser::parse_merge::ParseState, parseTree as *mut MergeStmt);
        }
        T_SelectStmt => {
            let n: *mut SelectStmt = parseTree as *mut SelectStmt;
            if !(*n).valuesLists.is_null() {
                result = transformValuesClause(pstate, n);
            } else if (*n).op == SETOP_NONE {
                result = transformSelectStmt(pstate, n);
            } else {
                result = transformSetOperationStmt(pstate, n);
            }
        }
        T_ReturnStmt => {
            result = transformReturnStmt(pstate, parseTree as *mut ReturnStmt);
        }
        T_PLAssignStmt => {
            result = transformPLAssignStmt(pstate, parseTree as *mut PLAssignStmt);
        }

        /*
         * Special cases
         */
        T_DeclareCursorStmt => {
            result = transformDeclareCursorStmt(pstate, parseTree as *mut DeclareCursorStmt);
        }
        T_ExplainStmt => {
            result = transformExplainStmt(pstate, parseTree as *mut ExplainStmt);
        }
        T_CreateTableAsStmt => {
            result = transformCreateTableAsStmt(pstate, parseTree as *mut CreateTableAsStmt);
        }
        T_CallStmt => {
            result = transformCallStmt(pstate, parseTree as *mut CallStmt);
        }
        _ => {
            /*
             * other statements don't require any transformation; just return
             * the original parsetree with a Query node plastered on top.
             */
            result = makeNode!(Query, T_Query);
            (*result).commandType = CMD_UTILITY;
            (*result).utilityStmt = parseTree;
        }
    }

    /* Mark as original query until we learn differently */
    (*result).querySource = QSRC_ORIGINAL;
    (*result).canSetTag = true;

    result
}

/*
 * stmt_requires_parse_analysis
 *      Returns true if parse analysis will do anything non-trivial
 *      with the given raw parse tree.
 */
pub unsafe fn stmt_requires_parse_analysis(parseTree: *mut RawStmt) -> bool {
    match nodeTag((*parseTree).stmt) {
        /*
         * Optimizable statements
         */
        T_InsertStmt
        | T_DeleteStmt
        | T_UpdateStmt
        | T_MergeStmt
        | T_SelectStmt
        | T_ReturnStmt
        | T_PLAssignStmt => true,

        /*
         * Special cases
         */
        T_DeclareCursorStmt | T_ExplainStmt | T_CreateTableAsStmt | T_CallStmt => true,

        _ => {
            /* all other statements just get wrapped in a CMD_UTILITY Query */
            false
        }
    }
}

/*
 * analyze_requires_snapshot
 *      Returns true if a snapshot must be set before doing parse analysis
 *      on the given raw parse tree.
 */
pub unsafe fn analyze_requires_snapshot(parseTree: *mut RawStmt) -> bool {
    /*
     * Currently, this should return true in exactly the same cases that
     * stmt_requires_parse_analysis() does, so we just invoke that function
     * rather than duplicating it.  We keep the two entry points separate for
     * clarity of callers, since from the callers' standpoint these are
     * different conditions.
     *
     * While there may someday be a statement type for which transformStmt()
     * does something nontrivial and yet no snapshot is needed for that
     * processing, it seems likely that making such a choice would be fragile.
     * If you want to install an exception, document the reasoning for it in a
     * comment.
     */
    stmt_requires_parse_analysis(parseTree)
}

/*
 * query_requires_rewrite_plan()
 *      Returns true if rewriting or planning is non-trivial for this Query.
 *
 * This is much like stmt_requires_parse_analysis(), but applies one step
 * further down the pipeline.
 */
pub unsafe fn query_requires_rewrite_plan(query: *mut Query) -> bool {
    if (*query).commandType != CMD_UTILITY {
        /* All optimizable statements require rewriting/planning */
        true
    } else {
        /* This list should match stmt_requires_parse_analysis() */
        match nodeTag((*query).utilityStmt) {
            T_DeclareCursorStmt | T_ExplainStmt | T_CreateTableAsStmt | T_CallStmt => true,
            _ => false,
        }
    }
}

/*
 * transformDeleteStmt -
 *    transforms a Delete Statement
 */
unsafe fn transformDeleteStmt(pstate: *mut ParseState, stmt: *mut DeleteStmt) -> *mut Query {
    let mut qry: *mut Query = makeNode!(Query, T_Query);
    let mut nsitem: *mut ParseNamespaceItem;
    let mut qual: *mut Node;

    (*qry).commandType = CMD_DELETE;

    /* process the WITH clause independently of all else */
    if !(*stmt).withClause.is_null() {
        (*qry).hasRecursive = (*(*stmt).withClause).recursive;
        (*qry).cteList = transformWithClause(pstate, (*stmt).withClause);
        (*qry).hasModifyingCTE = (*pstate).p_hasModifyingCTE;
    }

    /* set up range table with just the result rel */
    (*qry).resultRelation = setTargetTable(
        pstate,
        (*stmt).relation,
        (*(*stmt).relation).inh,
        true,
        ACL_DELETE,
    );
    nsitem = (*pstate).p_target_nsitem;

    /* there's no DISTINCT in DELETE */
    (*qry).distinctClause = NIL;

    /* subqueries in USING cannot access the result relation */
    (*nsitem).p_lateral_only = true;
    (*nsitem).p_lateral_ok = false;

    /*
     * The USING clause is non-standard SQL syntax, and is equivalent in
     * functionality to the FROM list that can be specified for UPDATE. The
     * USING keyword is used rather than FROM because FROM is already a
     * keyword in the DELETE syntax.
     */
    transformFromClause(pstate, (*stmt).usingClause);

    /* remaining clauses can reference the result relation normally */
    (*nsitem).p_lateral_only = false;
    (*nsitem).p_lateral_ok = true;

    qual = transformWhereClause(
        pstate,
        (*stmt).whereClause,
        EXPR_KIND_WHERE,
        b"WHERE\0".as_ptr() as *const c_char,
    );

    transformReturningClause(pstate, qry, (*stmt).returningClause, EXPR_KIND_RETURNING);

    /* done building the range table and jointree */
    (*qry).rtable = (*pstate).p_rtable;
    (*qry).rteperminfos = (*pstate).p_rteperminfos;
    (*qry).jointree = makeFromExpr((*pstate).p_joinlist, qual);

    (*qry).hasSubLinks = (*pstate).p_hasSubLinks;
    (*qry).hasWindowFuncs = (*pstate).p_hasWindowFuncs;
    (*qry).hasTargetSRFs = (*pstate).p_hasTargetSRFs;
    (*qry).hasAggs = (*pstate).p_hasAggs;

    assign_query_collations(pstate, qry);

    /* this must be done after collations, for reliable comparison of exprs */
    if (*pstate).p_hasAggs {
        parseCheckAggregates(pstate, qry);
    }

    qry
}

/*
 * transformInsertStmt -
 *    transform an Insert Statement
 */
unsafe fn transformInsertStmt(pstate: *mut ParseState, stmt: *mut InsertStmt) -> *mut Query {
    let mut qry: *mut Query = makeNode!(Query, T_Query);
    let mut selectStmt: *mut SelectStmt = (*stmt).selectStmt as *mut SelectStmt;
    let mut exprList: *mut List = NIL;
    let mut isGeneralSelect: bool;
    let mut sub_rtable: *mut List;
    let mut sub_rteperminfos: *mut List;
    let mut sub_namespace: *mut List;
    let mut icolumns: *mut List;
    let mut attrnos: *mut List = NIL;
    let mut nsitem: *mut ParseNamespaceItem;
    let mut perminfo: *mut RTEPermissionInfo;
    let mut icols: *mut ListCell;
    let mut attnos: *mut ListCell;
    let mut lc: *mut ListCell;
    let mut isOnConflictUpdate: bool;
    let mut targetPerms: AclMode;

    /* There can't be any outer WITH to worry about */
    debug_assert!((*pstate).p_ctenamespace.is_null());

    (*qry).commandType = CMD_INSERT;
    (*pstate).p_is_insert = true;

    /* process the WITH clause independently of all else */
    if !(*stmt).withClause.is_null() {
        (*qry).hasRecursive = (*(*stmt).withClause).recursive;
        (*qry).cteList = transformWithClause(pstate, (*stmt).withClause);
        (*qry).hasModifyingCTE = (*pstate).p_hasModifyingCTE;
    }

    (*qry).r#override = (*stmt).r#override;

    isOnConflictUpdate = !(*stmt).onConflictClause.is_null()
        && (*(*stmt).onConflictClause).action == OnConflictAction::ONCONFLICT_UPDATE;

    /*
     * We have three cases to deal with: DEFAULT VALUES (selectStmt == NULL),
     * VALUES list, or general SELECT input.  We special-case VALUES, both for
     * efficiency and so we can handle DEFAULT specifications.
     *
     * The grammar allows attaching ORDER BY, LIMIT, FOR UPDATE, or WITH to a
     * VALUES clause.  If we have any of those, treat it as a general SELECT;
     * so it will work, but you can't use DEFAULT items together with those.
     */
    isGeneralSelect = !selectStmt.is_null()
        && ((*selectStmt).valuesLists.is_null()
            || !(*selectStmt).sortClause.is_null()
            || !(*selectStmt).limitOffset.is_null()
            || !(*selectStmt).limitCount.is_null()
            || !(*selectStmt).lockingClause.is_null()
            || !(*selectStmt).withClause.is_null());

    /*
     * If a non-nil rangetable/namespace was passed in, and we are doing
     * INSERT/SELECT, arrange to pass the rangetable/rteperminfos/namespace
     * down to the SELECT.  This can only happen if we are inside a CREATE
     * RULE, and in that case we want the rule's OLD and NEW rtable entries to
     * appear as part of the SELECT's rtable, not as outer references for it.
     * (Kluge!) The SELECT's joinlist is not affected however.  We must do
     * this before adding the target table to the INSERT's rtable.
     */
    if isGeneralSelect {
        sub_rtable = (*pstate).p_rtable;
        (*pstate).p_rtable = NIL;
        sub_rteperminfos = (*pstate).p_rteperminfos;
        (*pstate).p_rteperminfos = NIL;
        sub_namespace = (*pstate).p_namespace;
        (*pstate).p_namespace = NIL;
    } else {
        sub_rtable = NIL; /* not used, but keep compiler quiet */
        sub_rteperminfos = NIL;
        sub_namespace = NIL;
    }

    /*
     * Must get write lock on INSERT target table before scanning SELECT, else
     * we will grab the wrong kind of initial lock if the target table is also
     * mentioned in the SELECT part.  Note that the target table is not added
     * to the joinlist or namespace.
     */
    targetPerms = ACL_INSERT;
    if isOnConflictUpdate {
        targetPerms |= ACL_UPDATE;
    }
    if std::env::var("PDB_BT").is_ok() { eprintln!("PDB_BT transformInsertStmt: before setTargetTable"); }
    (*qry).resultRelation =
        setTargetTable(pstate, (*stmt).relation, false, false, targetPerms);
    if std::env::var("PDB_BT").is_ok() { eprintln!("PDB_BT transformInsertStmt: after setTargetTable resultRelation={}", (*qry).resultRelation); }

    /* Validate stmt->cols list, or build default list if no list given */
    icolumns = checkInsertTargets(pstate, (*stmt).cols, &mut attrnos);
    if std::env::var("PDB_BT").is_ok() { eprintln!("PDB_BT transformInsertStmt: after checkInsertTargets ncols={}", list_length(icolumns)); }
    debug_assert!(list_length(icolumns) == list_length(attrnos));

    /*
     * Determine which variant of INSERT we have.
     */
    if selectStmt.is_null() {
        /*
         * We have INSERT ... DEFAULT VALUES.  We can handle this case by
         * emitting an empty targetlist --- all columns will be defaulted when
         * the planner expands the targetlist.
         */
        exprList = NIL;
    } else if isGeneralSelect {
        /*
         * We make the sub-pstate a child of the outer pstate so that it can
         * see any Param definitions supplied from above.  Since the outer
         * pstate's rtable and namespace are presently empty, there are no
         * side-effects of exposing names the sub-SELECT shouldn't be able to
         * see.
         */
        let mut sub_pstate: *mut ParseState = make_parsestate(pstate);
        let mut selectQuery: *mut Query;

        /*
         * Process the source SELECT.
         *
         * It is important that this be handled just like a standalone SELECT;
         * otherwise the behavior of SELECT within INSERT might be different
         * from a stand-alone SELECT. (Indeed, Postgres up through 6.5 had
         * bugs of just that nature...)
         *
         * The sole exception is that we prevent resolving unknown-type
         * outputs as TEXT.  This does not change the semantics since if the
         * column type matters semantically, it would have been resolved to
         * something else anyway.  Doing this lets us resolve such outputs as
         * the target column's type, which we handle below.
         */
        (*sub_pstate).p_rtable = sub_rtable;
        (*sub_pstate).p_rteperminfos = sub_rteperminfos;
        (*sub_pstate).p_joinexprs = NIL; /* sub_rtable has no joins */
        (*sub_pstate).p_nullingrels = NIL;
        (*sub_pstate).p_namespace = sub_namespace;
        (*sub_pstate).p_resolve_unknowns = false;

        selectQuery = transformStmt(sub_pstate, (*stmt).selectStmt);

        free_parsestate(sub_pstate);

        /* The grammar should have produced a SELECT */
        if !IsA!(selectQuery as *mut Node, T_Query)
            || (*selectQuery).commandType != CMD_SELECT
        {
            elog!(ERROR, "unexpected non-SELECT command in INSERT ... SELECT");
        }

        /*
         * Make the source be a subquery in the INSERT's rangetable, and add
         * it to the INSERT's joinlist (but not the namespace).
         */
        nsitem = addRangeTableEntryForSubquery(
            pstate,
            selectQuery,
            makeAlias(b"*SELECT*\0".as_ptr() as *const c_char, NIL),
            false,
            false,
        );
        addNSItemToQuery(pstate, nsitem, true, false, false);

        /*----------
         * Generate an expression list for the INSERT that selects all the
         * non-resjunk columns from the subquery.  (INSERT's tlist must be
         * separate from the subquery's tlist because we may add columns,
         * insert datatype coercions, etc.)
         *
         * HACK: unknown-type constants and params in the SELECT's targetlist
         * are copied up as-is rather than being referenced as subquery
         * outputs.  This is to ensure that when we try to coerce them to
         * the target column's datatype, the right things happen (see
         * special cases in coerce_type).  Otherwise, this fails:
         *      INSERT INTO foo SELECT 'bar', ... FROM baz
         *----------
         */
        exprList = NIL;
        foreach!(lc, (*selectQuery).targetList, {
            let tle: *mut TargetEntry = lfirst(current_cell!(lc)) as *mut TargetEntry;
            let mut expr: *mut Expr;

            if (*tle).resjunk {
                /* continue */
            } else if !(*tle).expr.is_null()
                && (IsA!((*tle).expr as *mut Node, T_Const)
                    || IsA!((*tle).expr as *mut Node, T_Param))
                && exprType((*tle).expr as *mut Node) == UNKNOWNOID
            {
                expr = (*tle).expr;
                exprList = lappend(exprList, expr as *mut c_void);
            } else {
                let mut var: *mut Var =
                    makeVarFromTargetEntry((*nsitem).p_rtindex, tle);
                (*var).location = exprLocation((*tle).expr as *mut Node);
                expr = var as *mut Expr;
                exprList = lappend(exprList, expr as *mut c_void);
            }
        });

        /* Prepare row for assignment to target table */
        exprList = transformInsertRow(
            pstate,
            exprList,
            (*stmt).cols,
            icolumns,
            attrnos,
            false,
        );
    } else if list_length((*selectStmt).valuesLists) > 1 {
        /*
         * Process INSERT ... VALUES with multiple VALUES sublists. We
         * generate a VALUES RTE holding the transformed expression lists, and
         * build up a targetlist containing Vars that reference the VALUES
         * RTE.
         */
        let mut exprsLists: *mut List = NIL;
        let mut coltypes: *mut List = NIL;
        let mut coltypmods: *mut List = NIL;
        let mut colcollations: *mut List = NIL;
        let mut sublist_length: c_int = -1;
        let mut lateral: bool = false;

        debug_assert!((*selectStmt).intoClause.is_null());

        foreach!(lc, (*selectStmt).valuesLists, {
            let mut sublist: *mut List = lfirst(current_cell!(lc)) as *mut List;

            /*
             * Do basic expression transformation (same as a ROW() expr, but
             * allow SetToDefault at top level)
             */
            sublist = transformExpressionList(pstate, sublist, EXPR_KIND_VALUES, true);

            /*
             * All the sublists must be the same length, *after*
             * transformation (which might expand '*' into multiple items).
             * The VALUES RTE can't handle anything different.
             */
            if sublist_length < 0 {
                /* Remember post-transformation length of first sublist */
                sublist_length = list_length(sublist);
            } else if sublist_length != list_length(sublist) {
                ereport!(ERROR, errmsg!("VALUES lists must all be the same length")) /* C also: errcode, parser_errposition */;
            }

            /*
             * Prepare row for assignment to target table.  We process any
             * indirection on the target column specs normally but then strip
             * off the resulting field/array assignment nodes, since we don't
             * want the parsed statement to contain copies of those in each
             * VALUES row.  (It's annoying to have to transform the
             * indirection specs over and over like this, but avoiding it
             * would take some really messy refactoring of
             * transformAssignmentIndirection.)
             */
            sublist = transformInsertRow(
                pstate,
                sublist,
                (*stmt).cols,
                icolumns,
                attrnos,
                true,
            );

            /*
             * We must assign collations now because assign_query_collations
             * doesn't process rangetable entries.  We just assign all the
             * collations independently in each row, and don't worry about
             * whether they are consistent vertically.  The outer INSERT query
             * isn't going to care about the collations of the VALUES columns,
             * so it's not worth the effort to identify a common collation for
             * each one here.  (But note this does have one user-visible
             * consequence: INSERT ... VALUES won't complain about conflicting
             * explicit COLLATEs in a column, whereas the same VALUES
             * construct in another context would complain.)
             */
            assign_list_collations(pstate, sublist);

            exprsLists = lappend(exprsLists, sublist as *mut c_void);
        });

        /*
         * Construct column type/typmod/collation lists for the VALUES RTE.
         * Every expression in each column has been coerced to the type/typmod
         * of the corresponding target column or subfield, so it's sufficient
         * to look at the exprType/exprTypmod of the first row.  We don't care
         * about the collation labeling, so just fill in InvalidOid for that.
         */
        foreach!(lc, linitial(exprsLists) as *mut List, {
            let val: *mut Node = lfirst(current_cell!(lc)) as *mut Node;
            coltypes = lappend_oid(coltypes, exprType(val));
            coltypmods = lappend_int(coltypmods, exprTypmod(val));
            colcollations = lappend_oid(colcollations, InvalidOid);
        });

        /*
         * Ordinarily there can't be any current-level Vars in the expression
         * lists, because the namespace was empty ... but if we're inside
         * CREATE RULE, then NEW/OLD references might appear.  In that case we
         * have to mark the VALUES RTE as LATERAL.
         */
        if list_length((*pstate).p_rtable) != 1
            && contain_vars_of_level(exprsLists as *mut Node, 0)
        {
            lateral = true;
        }

        /*
         * Generate the VALUES RTE
         */
        nsitem = addRangeTableEntryForValues(
            pstate,
            exprsLists,
            coltypes,
            coltypmods,
            colcollations,
            core::ptr::null_mut(),
            lateral,
            true,
        );
        addNSItemToQuery(pstate, nsitem, true, false, false);

        /*
         * Generate list of Vars referencing the RTE
         */
        exprList = expandNSItemVars(pstate, nsitem, 0, -1, core::ptr::null_mut());

        /*
         * Re-apply any indirection on the target column specs to the Vars
         */
        exprList = transformInsertRow(pstate, exprList, (*stmt).cols, icolumns, attrnos, false);
    } else {
        /*
         * Process INSERT ... VALUES with a single VALUES sublist.  We treat
         * this case separately for efficiency.  The sublist is just computed
         * directly as the Query's targetlist, with no VALUES RTE.  So it
         * works just like a SELECT without any FROM.
         */
        let mut valuesLists: *mut List = (*selectStmt).valuesLists;

        debug_assert!(list_length(valuesLists) == 1);
        debug_assert!((*selectStmt).intoClause.is_null());

        /*
         * Do basic expression transformation (same as a ROW() expr, but allow
         * SetToDefault at top level)
         */
        exprList = transformExpressionList(
            pstate,
            linitial(valuesLists) as *mut List,
            EXPR_KIND_VALUES_SINGLE,
            true,
        );

        /* Prepare row for assignment to target table */
        exprList = transformInsertRow(pstate, exprList, (*stmt).cols, icolumns, attrnos, false);
    }

    /*
     * Generate query's target list using the computed list of expressions.
     * Also, mark all the target columns as needing insert permissions.
     */
    perminfo = (*(*pstate).p_target_nsitem).p_perminfo as *mut RTEPermissionInfo;
    (*qry).targetList = NIL;
    debug_assert!(list_length(exprList) <= list_length(icolumns));

    /* forthree over exprList / icolumns / attrnos */
    {
        let mut _lc = list_head(exprList);
        let mut _icols = list_head(icolumns);
        let mut _attnos = list_head(attrnos);
        while !_lc.is_null() && !_icols.is_null() && !_attnos.is_null() {
            let expr: *mut Expr = lfirst(_lc) as *mut Expr;
            let col: *mut ResTarget = lfirst(_icols) as *mut ResTarget;
            let attr_num: AttrNumber = lfirst_int(_attnos) as AttrNumber;
            let mut tle: *mut TargetEntry;

            tle = makeTargetEntry(expr, attr_num, (*col).name, false);
            (*qry).targetList = lappend((*qry).targetList, tle as *mut c_void);

            (*perminfo).insertedCols = bms_add_member(
                (*perminfo).insertedCols,
                attr_num as c_int - FirstLowInvalidHeapAttributeNumber as c_int,
            );

            _lc = lnext(exprList, _lc);
            _icols = lnext(icolumns, _icols);
            _attnos = lnext(attrnos, _attnos);
        }
    }

    /*
     * If we have any clauses yet to process, set the query namespace to
     * contain only the target relation, removing any entries added in a
     * sub-SELECT or VALUES list.
     */
    if !(*stmt).onConflictClause.is_null() || !(*stmt).returningClause.is_null() {
        (*pstate).p_namespace = NIL;
        addNSItemToQuery(pstate, (*pstate).p_target_nsitem, false, true, true);
    }

    /* Process ON CONFLICT, if any. */
    if !(*stmt).onConflictClause.is_null() {
        (*qry).onConflict = transformOnConflictClause(pstate, (*stmt).onConflictClause);
    }

    /* Process RETURNING, if any. */
    if !(*stmt).returningClause.is_null() {
        transformReturningClause(pstate, qry, (*stmt).returningClause, EXPR_KIND_RETURNING);
    }

    /* done building the range table and jointree */
    (*qry).rtable = (*pstate).p_rtable;
    (*qry).rteperminfos = (*pstate).p_rteperminfos;
    (*qry).jointree = makeFromExpr((*pstate).p_joinlist, core::ptr::null_mut());

    (*qry).hasTargetSRFs = (*pstate).p_hasTargetSRFs;
    (*qry).hasSubLinks = (*pstate).p_hasSubLinks;

    assign_query_collations(pstate, qry);

    qry
}

/*
 * Prepare an INSERT row for assignment to the target table.
 *
 * exprlist: transformed expressions for source values; these might come from
 * a VALUES row, or be Vars referencing a sub-SELECT or VALUES RTE output.
 * stmtcols: original target-columns spec for INSERT (we just test for NIL)
 * icolumns: effective target-columns spec (list of ResTarget)
 * attrnos: integer column numbers (must be same length as icolumns)
 * strip_indirection: if true, remove any field/array assignment nodes
 */
pub unsafe fn transformInsertRow(
    pstate: *mut ParseState,
    exprlist: *mut List,
    stmtcols: *mut List,
    icolumns: *mut List,
    attrnos: *mut List,
    strip_indirection: bool,
) -> *mut List {
    let mut result: *mut List;
    let mut lc: *mut ListCell;
    let mut icols: *mut ListCell;
    let mut attnos: *mut ListCell;

    /*
     * Check length of expr list.  It must not have more expressions than
     * there are target columns.  We allow fewer, but only if no explicit
     * columns list was given (the remaining columns are implicitly
     * defaulted).  Note we must check this *after* transformation because
     * that could expand '*' into multiple items.
     */
    if list_length(exprlist) > list_length(icolumns) {
        ereport!(ERROR, errmsg!("INSERT has more expressions than target columns")) /* C also: errcode, parser_errposition */;
    }
    if !stmtcols.is_null() && list_length(exprlist) < list_length(icolumns) {
        /*
         * We can get here for cases like INSERT ... SELECT (a,b,c) FROM ...
         * where the user accidentally created a RowExpr instead of separate
         * columns.  Add a suitable hint if that seems to be the problem,
         * because the main error message is quite misleading for this case.
         */
        ereport!(ERROR, errmsg!("INSERT has more target columns than expressions")) /* C also: errcode, /* C also: errhint if count_rowexpr_columns matches */
            parser_errposition */;
    }

    /*
     * Prepare columns for assignment to target table.
     */
    result = NIL;
    {
        let mut _lc = list_head(exprlist);
        let mut _icols = list_head(icolumns);
        let mut _attnos = list_head(attrnos);
        while !_lc.is_null() && !_icols.is_null() && !_attnos.is_null() {
            let mut expr: *mut Expr = lfirst(_lc) as *mut Expr;
            let col: *mut ResTarget = lfirst(_icols) as *mut ResTarget;
            let attno: c_int = lfirst_int(_attnos);

            expr = transformAssignedExpr(
                pstate,
                expr,
                EXPR_KIND_INSERT_TARGET,
                (*col).name,
                attno,
                (*col).indirection,
                (*col).location,
            );

            if strip_indirection {
                /*
                 * We need to remove top-level FieldStores and SubscriptingRefs,
                 * as well as any CoerceToDomain appearing above one of those ---
                 * but not a CoerceToDomain that isn't above one of those.
                 */
                loop {
                    let mut subexpr: *mut Expr = expr;

                    while IsA!(subexpr as *mut Node, T_CoerceToDomain) {
                        subexpr = (*(subexpr as *mut CoerceToDomain)).arg;
                    }
                    if IsA!(subexpr as *mut Node, T_FieldStore) {
                        let fstore: *mut FieldStore = subexpr as *mut FieldStore;
                        expr = linitial((*fstore).newvals) as *mut Expr;
                    } else if IsA!(subexpr as *mut Node, T_SubscriptingRef) {
                        let sbsref: *mut SubscriptingRef = subexpr as *mut SubscriptingRef;
                        if (*sbsref).refassgnexpr.is_null() {
                            break;
                        }
                        expr = (*sbsref).refassgnexpr;
                    } else {
                        break;
                    }
                }
            }

            result = lappend(result, expr as *mut c_void);

            _lc = lnext(exprlist, _lc);
            _icols = lnext(icolumns, _icols);
            _attnos = lnext(attrnos, _attnos);
        }
    }

    result
}

/*
 * transformOnConflictClause -
 *    transforms an OnConflictClause in an INSERT
 */
unsafe fn transformOnConflictClause(
    pstate: *mut ParseState,
    onConflictClause: *mut OnConflictClause,
) -> *mut OnConflictExpr {
    let mut exclNSItem: *mut ParseNamespaceItem = core::ptr::null_mut();
    let mut arbiterElems: *mut List = core::ptr::null_mut();
    let mut arbiterWhere: *mut Node = core::ptr::null_mut();
    let mut arbiterConstraint: Oid = InvalidOid;
    let mut onConflictSet: *mut List = NIL;
    let mut onConflictWhere: *mut Node = core::ptr::null_mut();
    let mut exclRelIndex: c_int = 0;
    let mut exclRelTlist: *mut List = NIL;
    let mut result: *mut OnConflictExpr;

    /*
     * If this is ON CONFLICT ... UPDATE, first create the range table entry
     * for the EXCLUDED pseudo relation, so that that will be present while
     * processing arbiter expressions.  (You can't actually reference it from
     * there, but this provides a useful error message if you try.)
     */
    if (*onConflictClause).action == OnConflictAction::ONCONFLICT_UPDATE {
        let targetrel: *mut crate::utils::rel::RelationData = (*pstate).p_target_relation as *mut crate::utils::rel::RelationData;
        let mut exclRte: *mut RangeTblEntry;

        exclNSItem = addRangeTableEntryForRelation(
            pstate,
            (*pstate).p_target_relation,
            crate::storage::lockdefs::RowExclusiveLock,
            makeAlias(b"excluded\0".as_ptr() as *const c_char, NIL),
            false,
            false,
        );
        exclRte = (*exclNSItem).p_rte as *mut RangeTblEntry;
        exclRelIndex = (*exclNSItem).p_rtindex as c_int;

        /*
         * relkind is set to composite to signal that we're not dealing with
         * an actual relation, and no permission checks are required on it.
         * (We'll check the actual target relation, instead.)
         */
        (*exclRte).relkind = RELKIND_COMPOSITE_TYPE;

        /* Create EXCLUDED rel's targetlist for use by EXPLAIN */
        exclRelTlist = BuildOnConflictExcludedTargetlist(targetrel, exclRelIndex as Index);
    }

    /* Process the arbiter clause, ON CONFLICT ON (...) */
    transformOnConflictArbiter(
        pstate,
        onConflictClause,
        &mut arbiterElems,
        &mut arbiterWhere,
        &mut arbiterConstraint,
    );

    /* Process DO UPDATE */
    if (*onConflictClause).action == OnConflictAction::ONCONFLICT_UPDATE {
        /*
         * Expressions in the UPDATE targetlist need to be handled like UPDATE
         * not INSERT.  We don't need to save/restore this because all INSERT
         * expressions have been parsed already.
         */
        (*pstate).p_is_insert = false;

        /*
         * Add the EXCLUDED pseudo relation to the query namespace, making it
         * available in the UPDATE subexpressions.
         */
        addNSItemToQuery(pstate, exclNSItem, false, true, true);

        /*
         * Now transform the UPDATE subexpressions.
         */
        onConflictSet = transformUpdateTargetList(pstate, (*onConflictClause).targetList);

        onConflictWhere = transformWhereClause(
            pstate,
            (*onConflictClause).whereClause,
            EXPR_KIND_WHERE,
            b"WHERE\0".as_ptr() as *const c_char,
        );

        /*
         * Remove the EXCLUDED pseudo relation from the query namespace, since
         * it's not supposed to be available in RETURNING.  (Maybe someday we
         * could allow that, and drop this step.)
         */
        debug_assert!(
            (llast((*pstate).p_namespace) as *mut ParseNamespaceItem) == exclNSItem
        );
        (*pstate).p_namespace = list_delete_last((*pstate).p_namespace);
    }

    /* Finally, build ON CONFLICT DO [NOTHING | UPDATE] expression */
    result = makeNode!(OnConflictExpr, T_OnConflictExpr);

    (*result).action = (*onConflictClause).action;
    (*result).arbiterElems = arbiterElems;
    (*result).arbiterWhere = arbiterWhere;
    (*result).constraint = arbiterConstraint;
    (*result).onConflictSet = onConflictSet;
    (*result).onConflictWhere = onConflictWhere;
    (*result).exclRelIndex = exclRelIndex;
    (*result).exclRelTlist = exclRelTlist;

    result
}


/*
 * BuildOnConflictExcludedTargetlist
 *      Create target list for the EXCLUDED pseudo-relation of ON CONFLICT,
 *      representing the columns of targetrel with varno exclRelIndex.
 *
 * Note: Exported for use in the rewriter.
 */
pub unsafe fn BuildOnConflictExcludedTargetlist(
    targetrel: *mut crate::utils::rel::RelationData,
    exclRelIndex: Index,
) -> *mut List {
    let mut result: *mut List = NIL;
    let mut attno: c_int;
    let mut var: *mut Var;
    let mut te: *mut TargetEntry;

    /*
     * Note that resnos of the tlist must correspond to attnos of the
     * underlying relation, hence we need entries for dropped columns too.
     */
    attno = 0;
    while attno < RelationGetNumberOfAttributes(targetrel) {
        let attr: Form_pg_attribute =
            TupleDescAttr((*targetrel).rd_att, attno);
        let mut name: *mut c_char;

        if (*attr).attisdropped {
            /*
             * can't use atttypid here, but it doesn't really matter what type
             * the Const claims to be.
             */
            var = makeNullConst(INT4OID, -1, InvalidOid) as *mut Var;
            name = core::ptr::null_mut();
        } else {
            var = makeVar(
                exclRelIndex as c_int,
                (attno + 1) as AttrNumber,
                (*attr).atttypid,
                (*attr).atttypmod,
                (*attr).attcollation,
                0,
            );
            name = pstrdup(
                (*attr).attname.data.as_ptr() as *const c_char,
            );
        }

        te = makeTargetEntry(
            var as *mut Expr,
            (attno + 1) as AttrNumber,
            name,
            false,
        );

        result = lappend(result, te as *mut c_void);
        attno += 1;
    }

    /*
     * Add a whole-row-Var entry to support references to "EXCLUDED.*".  Like
     * the other entries in the EXCLUDED tlist, its resno must match the Var's
     * varattno, else the wrong things happen while resolving references in
     * setrefs.c.  This is against normal conventions for targetlists, but
     * it's okay since we don't use this as a real tlist.
     */
    var = makeVar(
        exclRelIndex as c_int,
        InvalidAttrNumber,
        (*(*targetrel).rd_rel).reltype,
        -1,
        InvalidOid,
        0,
    );
    te = makeTargetEntry(var as *mut Expr, InvalidAttrNumber, core::ptr::null_mut(), true);
    result = lappend(result, te as *mut c_void);

    result
}


/*
 * count_rowexpr_columns -
 *    get number of columns contained in a ROW() expression;
 *    return -1 if expression isn't a RowExpr or a Var referencing one.
 *
 * This is currently used only for hint purposes, so we aren't terribly
 * tense about recognizing all possible cases.  The Var case is interesting
 * because that's what we'll get in the INSERT ... SELECT (...) case.
 */
unsafe fn count_rowexpr_columns(pstate: *mut ParseState, expr: *mut Node) -> c_int {
    if expr.is_null() {
        return -1;
    }
    if IsA!(expr, T_RowExpr) {
        return list_length((*(expr as *mut RowExpr)).args);
    }
    if IsA!(expr, T_Var) {
        let var: *mut Var = expr as *mut Var;
        let attnum: AttrNumber = (*var).varattno;

        if attnum > 0 && (*var).vartype == RECORDOID as Oid {
            let mut rte: *mut RangeTblEntry;

            rte = GetRTEByRangeTablePosn(pstate, (*var).varno, (*var).varlevelsup as c_int);
            if (*rte).rtekind == RTE_SUBQUERY {
                /* Subselect-in-FROM: examine sub-select's output expr */
                let ste: *mut TargetEntry =
                    get_tle_by_resno((*(*rte).subquery).targetList, attnum);

                if ste.is_null() || (*ste).resjunk {
                    return -1;
                }
                let inner_expr: *mut Node = (*ste).expr as *mut Node;
                if IsA!(inner_expr, T_RowExpr) {
                    return list_length((*(inner_expr as *mut RowExpr)).args);
                }
            }
        }
    }
    -1
}


/*
 * transformSelectStmt -
 *    transforms a Select Statement
 *
 * Note: this covers only cases with no set operations and no VALUES lists;
 * see below for the other cases.
 */
unsafe fn transformSelectStmt(pstate: *mut ParseState, stmt: *mut SelectStmt) -> *mut Query {
    let mut qry: *mut Query = makeNode!(Query, T_Query);
    let mut qual: *mut Node;
    let mut l: *mut ListCell;

    (*qry).commandType = CMD_SELECT;

    /* process the WITH clause independently of all else */
    if !(*stmt).withClause.is_null() {
        (*qry).hasRecursive = (*(*stmt).withClause).recursive;
        (*qry).cteList = transformWithClause(pstate, (*stmt).withClause);
        (*qry).hasModifyingCTE = (*pstate).p_hasModifyingCTE;
    }

    /* Complain if we get called from someplace where INTO is not allowed */
    if !(*stmt).intoClause.is_null() {
        ereport!(ERROR, errmsg!("SELECT ... INTO is not allowed here")) /* C also: errcode, parser_errposition */;
    }

    /* make FOR UPDATE/FOR SHARE info available to addRangeTableEntry */
    (*pstate).p_locking_clause = (*stmt).lockingClause;

    /* make WINDOW info available for window functions, too */
    (*pstate).p_windowdefs = (*stmt).windowClause;

    /* process the FROM clause */
    transformFromClause(pstate, (*stmt).fromClause);

    /* transform targetlist */
    (*qry).targetList = transformTargetList(pstate, (*stmt).targetList, EXPR_KIND_SELECT_TARGET);

    /* mark column origins */
    markTargetListOrigins(pstate, (*qry).targetList);

    /* transform WHERE */
    qual = transformWhereClause(
        pstate,
        (*stmt).whereClause,
        EXPR_KIND_WHERE,
        b"WHERE\0".as_ptr() as *const c_char,
    );

    /* initial processing of HAVING clause is much like WHERE clause */
    (*qry).havingQual = transformWhereClause(
        pstate,
        (*stmt).havingClause,
        EXPR_KIND_HAVING,
        b"HAVING\0".as_ptr() as *const c_char,
    );

    /*
     * Transform sorting/grouping stuff.  Do ORDER BY first because both
     * transformGroupClause and transformDistinctClause need the results. Note
     * that these functions can also change the targetList, so it's passed to
     * them by reference.
     */
    (*qry).sortClause = transformSortClause(
        pstate,
        (*stmt).sortClause,
        &mut (*qry).targetList,
        EXPR_KIND_ORDER_BY,
        false, /* allow SQL92 rules */
    );

    (*qry).groupClause = transformGroupClause(
        pstate,
        (*stmt).groupClause,
        &mut (*qry).groupingSets,
        &mut (*qry).targetList,
        (*qry).sortClause,
        EXPR_KIND_GROUP_BY,
        false, /* allow SQL92 rules */
    );
    (*qry).groupDistinct = (*stmt).groupDistinct;

    if (*stmt).distinctClause.is_null() {
        (*qry).distinctClause = NIL;
        (*qry).hasDistinctOn = false;
    } else if linitial((*stmt).distinctClause).is_null() {
        /* We had SELECT DISTINCT */
        (*qry).distinctClause = transformDistinctClause(
            pstate,
            &mut (*qry).targetList,
            (*qry).sortClause,
            false,
        );
        (*qry).hasDistinctOn = false;
    } else {
        /* We had SELECT DISTINCT ON */
        (*qry).distinctClause = transformDistinctOnClause(
            pstate,
            (*stmt).distinctClause,
            &mut (*qry).targetList,
            (*qry).sortClause,
        );
        (*qry).hasDistinctOn = true;
    }

    /* transform LIMIT */
    (*qry).limitOffset = transformLimitClause(
        pstate,
        (*stmt).limitOffset,
        EXPR_KIND_OFFSET,
        b"OFFSET\0".as_ptr() as *const c_char,
        (*stmt).limitOption,
    );
    (*qry).limitCount = transformLimitClause(
        pstate,
        (*stmt).limitCount,
        EXPR_KIND_LIMIT,
        b"LIMIT\0".as_ptr() as *const c_char,
        (*stmt).limitOption,
    );
    (*qry).limitOption = (*stmt).limitOption;

    /* transform window clauses after we have seen all window functions */
    (*qry).windowClause = transformWindowDefinitions(
        pstate,
        (*pstate).p_windowdefs,
        &mut (*qry).targetList,
    );

    /* resolve any still-unresolved output columns as being type text */
    if (*pstate).p_resolve_unknowns {
        resolveTargetListUnknowns(pstate, (*qry).targetList);
    }

    (*qry).rtable = (*pstate).p_rtable;
    (*qry).rteperminfos = (*pstate).p_rteperminfos;
    (*qry).jointree = makeFromExpr((*pstate).p_joinlist, qual);

    (*qry).hasSubLinks = (*pstate).p_hasSubLinks;
    (*qry).hasWindowFuncs = (*pstate).p_hasWindowFuncs;
    (*qry).hasTargetSRFs = (*pstate).p_hasTargetSRFs;
    (*qry).hasAggs = (*pstate).p_hasAggs;

    foreach!(l, (*stmt).lockingClause, {
        transformLockingClause(pstate, qry, lfirst(current_cell!(l)) as *mut LockingClause, false);
    });

    assign_query_collations(pstate, qry);

    /* this must be done after collations, for reliable comparison of exprs */
    if (*pstate).p_hasAggs
        || !(*qry).groupClause.is_null()
        || !(*qry).groupingSets.is_null()
        || !(*qry).havingQual.is_null()
    {
        parseCheckAggregates(pstate, qry);
    }

    qry
}

/*
 * transformValuesClause -
 *    transforms a VALUES clause that's being used as a standalone SELECT
 *
 * We build a Query containing a VALUES RTE, rather as if one had written
 *          SELECT * FROM (VALUES ...) AS "*VALUES*"
 */
unsafe fn transformValuesClause(pstate: *mut ParseState, stmt: *mut SelectStmt) -> *mut Query {
    let mut qry: *mut Query = makeNode!(Query, T_Query);
    let mut exprsLists: *mut List = NIL;
    let mut coltypes: *mut List = NIL;
    let mut coltypmods: *mut List = NIL;
    let mut colcollations: *mut List = NIL;
    let mut colexprs: *mut *mut List = core::ptr::null_mut();
    let mut sublist_length: c_int = -1;
    let mut lateral: bool = false;
    let mut nsitem: *mut ParseNamespaceItem;
    let mut lc: *mut ListCell;
    let mut lc2: *mut ListCell;
    let mut i: c_int;

    (*qry).commandType = CMD_SELECT;

    /* Most SELECT stuff doesn't apply in a VALUES clause */
    debug_assert!((*stmt).distinctClause.is_null());
    debug_assert!((*stmt).intoClause.is_null());
    debug_assert!((*stmt).targetList.is_null());
    debug_assert!((*stmt).fromClause.is_null());
    debug_assert!((*stmt).whereClause.is_null());
    debug_assert!((*stmt).groupClause.is_null());
    debug_assert!((*stmt).havingClause.is_null());
    debug_assert!((*stmt).windowClause.is_null());
    debug_assert!((*stmt).op == SETOP_NONE);

    /* process the WITH clause independently of all else */
    if !(*stmt).withClause.is_null() {
        (*qry).hasRecursive = (*(*stmt).withClause).recursive;
        (*qry).cteList = transformWithClause(pstate, (*stmt).withClause);
        (*qry).hasModifyingCTE = (*pstate).p_hasModifyingCTE;
    }

    /*
     * For each row of VALUES, transform the raw expressions.
     *
     * Note that the intermediate representation we build is column-organized
     * not row-organized.  That simplifies the type and collation processing
     * below.
     */
    foreach!(lc, (*stmt).valuesLists, {
        let mut sublist: *mut List = lfirst(current_cell!(lc)) as *mut List;

        /*
         * Do basic expression transformation (same as a ROW() expr, but here
         * we disallow SetToDefault)
         */
        sublist = transformExpressionList(pstate, sublist, EXPR_KIND_VALUES, false);

        /*
         * All the sublists must be the same length, *after* transformation
         * (which might expand '*' into multiple items).  The VALUES RTE can't
         * handle anything different.
         */
        if sublist_length < 0 {
            /* Remember post-transformation length of first sublist */
            sublist_length = list_length(sublist);
            /* and allocate array for per-column lists */
            colexprs = palloc0(
                sublist_length as usize * size_of::<*mut List>()
            ) as *mut *mut List;
        } else if sublist_length != list_length(sublist) {
            ereport!(ERROR, errmsg!("VALUES lists must all be the same length")) /* C also: errcode, parser_errposition */;
        }

        /* Build per-column expression lists */
        i = 0;
        lc2 = list_head(sublist);
        while !lc2.is_null() {
            let col: *mut Node = lfirst(lc2) as *mut Node;
            *colexprs.offset(i as isize) =
                lappend(*colexprs.offset(i as isize), col as *mut c_void);
            i += 1;
            lc2 = lnext(sublist, lc2);
        }

        /* Release sub-list's cells to save memory */
        list_free(sublist);

        /* Prepare an exprsLists element for this row */
        exprsLists = lappend(exprsLists, NIL as *mut c_void);
    });

    /*
     * Now resolve the common types of the columns, and coerce everything to
     * those types.  Then identify the common typmod and common collation, if
     * any, of each column.
     */
    i = 0;
    while i < sublist_length {
        let mut coltype: Oid;
        let mut coltypmod: int32;
        let mut colcoll: Oid;

        coltype = select_common_type(
            pstate,
            *colexprs.offset(i as isize),
            b"VALUES\0".as_ptr() as *const c_char,
            core::ptr::null_mut(),
        );

        foreach!(lc, *colexprs.offset(i as isize), {
            let mut col: *mut Node = lfirst(current_cell!(lc)) as *mut Node;
            col = coerce_to_common_type(
                pstate,
                col,
                coltype,
                b"VALUES\0".as_ptr() as *const c_char,
            );
            /* lfirst(lc) = col -- store the coerced expr back into the cell */
            (*current_cell!(lc)).ptr_value = col as *mut c_void;
        });

        coltypmod = select_common_typmod(pstate, *colexprs.offset(i as isize), coltype);
        colcoll = select_common_collation(pstate, *colexprs.offset(i as isize), true);

        coltypes = lappend_oid(coltypes, coltype);
        coltypmods = lappend_int(coltypmods, coltypmod);
        colcollations = lappend_oid(colcollations, colcoll);

        i += 1;
    }

    /*
     * Finally, rearrange the coerced expressions into row-organized lists.
     */
    i = 0;
    while i < sublist_length {
        lc = list_head(*colexprs.offset(i as isize));
        lc2 = list_head(exprsLists);
        while !lc.is_null() && !lc2.is_null() {
            let col: *mut Node = lfirst(lc) as *mut Node;
            let mut sublist: *mut List = lfirst(lc2) as *mut List;
            sublist = lappend(sublist, col as *mut c_void);
            /* lfirst(lc2) = sublist */
            (*lc2).ptr_value = sublist as *mut c_void;
            lc = lnext(*colexprs.offset(i as isize), lc);
            lc2 = lnext(exprsLists, lc2);
        }
        list_free(*colexprs.offset(i as isize));
        i += 1;
    }

    /*
     * Ordinarily there can't be any current-level Vars in the expression
     * lists, because the namespace was empty ... but if we're inside CREATE
     * RULE, then NEW/OLD references might appear.  In that case we have to
     * mark the VALUES RTE as LATERAL.
     */
    if !(*pstate).p_rtable.is_null()
        && contain_vars_of_level(exprsLists as *mut Node, 0)
    {
        lateral = true;
    }

    /*
     * Generate the VALUES RTE
     */
    nsitem = addRangeTableEntryForValues(
        pstate,
        exprsLists,
        coltypes,
        coltypmods,
        colcollations,
        core::ptr::null_mut(),
        lateral,
        true,
    );
    addNSItemToQuery(pstate, nsitem, true, true, true);

    /*
     * Generate a targetlist as though expanding "*"
     */
    debug_assert!((*pstate).p_next_resno == 1);
    (*qry).targetList = expandNSItemAttrs(pstate, nsitem, 0, true, -1);

    /*
     * The grammar allows attaching ORDER BY, LIMIT, and FOR UPDATE to a
     * VALUES, so cope.
     */
    (*qry).sortClause = transformSortClause(
        pstate,
        (*stmt).sortClause,
        &mut (*qry).targetList,
        EXPR_KIND_ORDER_BY,
        false, /* allow SQL92 rules */
    );

    (*qry).limitOffset = transformLimitClause(
        pstate,
        (*stmt).limitOffset,
        EXPR_KIND_OFFSET,
        b"OFFSET\0".as_ptr() as *const c_char,
        (*stmt).limitOption,
    );
    (*qry).limitCount = transformLimitClause(
        pstate,
        (*stmt).limitCount,
        EXPR_KIND_LIMIT,
        b"LIMIT\0".as_ptr() as *const c_char,
        (*stmt).limitOption,
    );
    (*qry).limitOption = (*stmt).limitOption;

    if !(*stmt).lockingClause.is_null() {
        ereport!(
            ERROR,
            /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
            /* translator: {} is a SQL row locking clause such as FOR UPDATE */
            errmsg!(
                "{} cannot be applied to VALUES",
                CStr::from_ptr(LCS_asString(
                    (*(linitial((*stmt).lockingClause) as *mut LockingClause)).strength
                )).to_string_lossy()
            )
        );
    }

    (*qry).rtable = (*pstate).p_rtable;
    (*qry).rteperminfos = (*pstate).p_rteperminfos;
    (*qry).jointree = makeFromExpr((*pstate).p_joinlist, core::ptr::null_mut());

    (*qry).hasSubLinks = (*pstate).p_hasSubLinks;

    assign_query_collations(pstate, qry);

    qry
}

/*
 * transformSetOperationStmt -
 *    transforms a set-operations tree
 *
 * A set-operation tree is just a SELECT, but with UNION/INTERSECT/EXCEPT
 * structure to it.  We must transform each leaf SELECT and build up a top-
 * level Query that contains the leaf SELECTs as subqueries in its rangetable.
 * The tree of set operations is converted into the setOperations field of
 * the top-level Query.
 */
unsafe fn transformSetOperationStmt(pstate: *mut ParseState, stmt: *mut SelectStmt) -> *mut Query {
    let mut qry: *mut Query = makeNode!(Query, T_Query);
    let mut leftmostSelect: *mut SelectStmt;
    let mut leftmostRTI: c_int;
    let mut leftmostQuery: *mut Query;
    let mut sostmt: *mut SetOperationStmt;
    let mut sortClause: *mut List;
    let mut limitOffset: *mut Node;
    let mut limitCount: *mut Node;
    let mut lockingClause: *mut List;
    let mut withClause: *mut WithClause;
    let mut node: *mut Node;
    let mut left_tlist: *mut ListCell;
    let mut lct: *mut ListCell;
    let mut lcm: *mut ListCell;
    let mut lcc: *mut ListCell;
    let mut l: *mut ListCell;
    let mut targetvars: *mut List = NIL;
    let mut targetnames: *mut List = NIL;
    let mut sv_namespace: *mut List;
    let mut sv_rtable_length: c_int;
    let mut jnsitem: *mut ParseNamespaceItem;
    let mut sortnscolumns: *mut ParseNamespaceColumn;
    let mut sortcolindex: c_int;
    let mut tllen: c_int;

    (*qry).commandType = CMD_SELECT;

    /*
     * Find leftmost leaf SelectStmt.  We currently only need to do this in
     * order to deliver a suitable error message if there's an INTO clause
     * there, implying the set-op tree is in a context that doesn't allow
     * INTO.
     */
    leftmostSelect = (*stmt).larg;
    while !leftmostSelect.is_null() && (*leftmostSelect).op != SETOP_NONE {
        leftmostSelect = (*leftmostSelect).larg;
    }
    debug_assert!(!leftmostSelect.is_null()
        && IsA!(leftmostSelect as *mut Node, T_SelectStmt)
        && (*leftmostSelect).larg.is_null());
    if !(*leftmostSelect).intoClause.is_null() {
        ereport!(ERROR, errmsg!("SELECT ... INTO is not allowed here")) /* C also: errcode, parser_errposition */;
    }

    /*
     * We need to extract ORDER BY and other top-level clauses here and not
     * let transformSetOperationTree() see them --- else it'll just recurse
     * right back here!
     */
    sortClause = (*stmt).sortClause;
    limitOffset = (*stmt).limitOffset;
    limitCount = (*stmt).limitCount;
    lockingClause = (*stmt).lockingClause;
    withClause = (*stmt).withClause;

    (*stmt).sortClause = NIL;
    (*stmt).limitOffset = core::ptr::null_mut();
    (*stmt).limitCount = core::ptr::null_mut();
    (*stmt).lockingClause = NIL;
    (*stmt).withClause = core::ptr::null_mut();

    /* We don't support FOR UPDATE/SHARE with set ops at the moment. */
    if !lockingClause.is_null() {
        ereport!(
            ERROR,
            /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
            /* translator: {} is a SQL row locking clause such as FOR UPDATE */
            errmsg!(
                "{} is not allowed with UNION/INTERSECT/EXCEPT",
                CStr::from_ptr(LCS_asString((*(linitial(lockingClause) as *mut LockingClause)).strength)).to_string_lossy()
            )
        );
    }

    /* Process the WITH clause independently of all else */
    if !withClause.is_null() {
        (*qry).hasRecursive = (*withClause).recursive;
        (*qry).cteList = transformWithClause(pstate, withClause);
        (*qry).hasModifyingCTE = (*pstate).p_hasModifyingCTE;
    }

    /*
     * Recursively transform the components of the tree.
     */
    sostmt = castNode!(
        SetOperationStmt,
        T_SetOperationStmt,
        transformSetOperationTree(pstate, stmt, true, core::ptr::null_mut())
    );
    debug_assert!(!sostmt.is_null());
    (*qry).setOperations = sostmt as *mut Node;

    /*
     * Re-find leftmost SELECT (now it's a sub-query in rangetable)
     */
    node = (*sostmt).larg;
    while !node.is_null() && IsA!(node, T_SetOperationStmt) {
        node = (*(node as *mut SetOperationStmt)).larg;
    }
    debug_assert!(!node.is_null() && IsA!(node, T_RangeTblRef));
    leftmostRTI = (*(node as *mut RangeTblRef)).rtindex as c_int;
    leftmostQuery = (*rt_fetch(leftmostRTI as Index, (*pstate).p_rtable)).subquery;
    debug_assert!(!leftmostQuery.is_null());

    /*
     * Generate dummy targetlist for outer query using column names of
     * leftmost select and common datatypes/collations of topmost set
     * operation.  Also make lists of the dummy vars and their names for use
     * in parsing ORDER BY.
     */
    (*qry).targetList = NIL;
    targetvars = NIL;
    targetnames = NIL;
    sortnscolumns = palloc0(
        list_length((*sostmt).colTypes) as usize * size_of::<ParseNamespaceColumn>(),
    ) as *mut ParseNamespaceColumn;
    sortcolindex = 0;

    /* forfour over colTypes / colTypmods / colCollations / leftmostQuery.targetList */
    {
        let mut _lct = list_head((*sostmt).colTypes);
        let mut _lcm = list_head((*sostmt).colTypmods);
        let mut _lcc = list_head((*sostmt).colCollations);
        let mut _left_tlist = list_head((*leftmostQuery).targetList);
        while !_lct.is_null()
            && !_lcm.is_null()
            && !_lcc.is_null()
            && !_left_tlist.is_null()
        {
            let colType: Oid = lfirst_oid(_lct);
            let colTypmod: int32 = lfirst_int(_lcm);
            let colCollation: Oid = lfirst_oid(_lcc);
            let lefttle: *mut TargetEntry = lfirst(_left_tlist) as *mut TargetEntry;
            let mut colName: *mut c_char;
            let mut tle: *mut TargetEntry;
            let mut var: *mut Var;

            debug_assert!(!(*lefttle).resjunk);
            colName = pstrdup((*lefttle).resname);
            var = makeVar(
                leftmostRTI as c_int,
                (*lefttle).resno,
                colType,
                colTypmod,
                colCollation,
                0,
            );
            (*var).location = exprLocation((*lefttle).expr as *mut Node);
            tle = makeTargetEntry(
                var as *mut Expr,
                (*pstate).p_next_resno as AttrNumber,
                colName,
                false,
            );
            (*pstate).p_next_resno += 1;
            (*qry).targetList = lappend((*qry).targetList, tle as *mut c_void);
            targetvars = lappend(targetvars, var as *mut c_void);
            targetnames = lappend(
                targetnames,
                makeString(colName) as *mut c_void,
            );
            let sc: *mut ParseNamespaceColumn = sortnscolumns.offset(sortcolindex as isize);
            (*sc).p_varno = leftmostRTI as Index;
            (*sc).p_varattno = (*lefttle).resno;
            (*sc).p_vartype = colType;
            (*sc).p_vartypmod = colTypmod;
            (*sc).p_varcollid = colCollation;
            (*sc).p_varnosyn = leftmostRTI as Index;
            (*sc).p_varattnosyn = (*lefttle).resno;
            sortcolindex += 1;

            _lct = lnext((*sostmt).colTypes, _lct);
            _lcm = lnext((*sostmt).colTypmods, _lcm);
            _lcc = lnext((*sostmt).colCollations, _lcc);
            _left_tlist = lnext((*leftmostQuery).targetList, _left_tlist);
        }
    }

    /*
     * As a first step towards supporting sort clauses that are expressions
     * using the output columns, generate a namespace entry that makes the
     * output columns visible.
     */
    sv_rtable_length = list_length((*pstate).p_rtable);

    jnsitem = addRangeTableEntryForJoin(
        pstate,
        targetnames,
        sortnscolumns,
        JOIN_INNER,
        0,
        targetvars,
        NIL,
        NIL,
        core::ptr::null_mut(),
        core::ptr::null_mut(),
        false,
    );

    sv_namespace = (*pstate).p_namespace;
    (*pstate).p_namespace = NIL;

    /* add jnsitem to column namespace only */
    addNSItemToQuery(pstate, jnsitem, false, false, true);

    /*
     * For now, we don't support resjunk sort clauses on the output of a
     * setOperation tree --- you can only use the SQL92-spec options of
     * selecting an output column by name or number.
     */
    tllen = list_length((*qry).targetList);

    (*qry).sortClause = transformSortClause(
        pstate,
        sortClause,
        &mut (*qry).targetList,
        EXPR_KIND_ORDER_BY,
        false, /* allow SQL92 rules */
    );

    /* restore namespace, remove join RTE from rtable */
    (*pstate).p_namespace = sv_namespace;
    (*pstate).p_rtable =
        list_truncate((*pstate).p_rtable, sv_rtable_length);

    if tllen != list_length((*qry).targetList) {
        ereport!(ERROR, errmsg!("invalid UNION/INTERSECT/EXCEPT ORDER BY clause")) /* C also: errcode, errdetail, errhint, parser_errposition */;
    }

    (*qry).limitOffset = transformLimitClause(
        pstate,
        limitOffset,
        EXPR_KIND_OFFSET,
        b"OFFSET\0".as_ptr() as *const c_char,
        (*stmt).limitOption,
    );
    (*qry).limitCount = transformLimitClause(
        pstate,
        limitCount,
        EXPR_KIND_LIMIT,
        b"LIMIT\0".as_ptr() as *const c_char,
        (*stmt).limitOption,
    );
    (*qry).limitOption = (*stmt).limitOption;

    (*qry).rtable = (*pstate).p_rtable;
    (*qry).rteperminfos = (*pstate).p_rteperminfos;
    (*qry).jointree = makeFromExpr((*pstate).p_joinlist, core::ptr::null_mut());

    (*qry).hasSubLinks = (*pstate).p_hasSubLinks;
    (*qry).hasWindowFuncs = (*pstate).p_hasWindowFuncs;
    (*qry).hasTargetSRFs = (*pstate).p_hasTargetSRFs;
    (*qry).hasAggs = (*pstate).p_hasAggs;

    foreach!(l, lockingClause, {
        transformLockingClause(pstate, qry, lfirst(current_cell!(l)) as *mut LockingClause, false);
    });

    assign_query_collations(pstate, qry);

    /* this must be done after collations, for reliable comparison of exprs */
    if (*pstate).p_hasAggs
        || !(*qry).groupClause.is_null()
        || !(*qry).groupingSets.is_null()
        || !(*qry).havingQual.is_null()
    {
        parseCheckAggregates(pstate, qry);
    }

    qry
}

/*
 * Make a SortGroupClause node for a SetOperationStmt's groupClauses
 *
 * If require_hash is true, the caller is indicating that they need hash
 * support or they will fail.  So look extra hard for hash support.
 */
pub unsafe fn makeSortGroupClauseForSetOp(rescoltype: Oid, require_hash: bool) -> *mut SortGroupClause {
    let mut grpcl: *mut SortGroupClause = makeNode!(SortGroupClause, T_SortGroupClause);
    let mut sortop: Oid = InvalidOid;
    let mut eqop: Oid = InvalidOid;
    let mut hashable: bool = false;

    /* determine the eqop and optional sortop */
    get_sort_group_operators(
        rescoltype,
        false,
        true,
        false,
        &mut sortop,
        &mut eqop,
        core::ptr::null_mut(),
        &mut hashable,
    );

    /*
     * The type cache doesn't believe that record is hashable (see
     * cache_record_field_properties()), but if the caller really needs hash
     * support, we can assume it does.  Worst case, if any components of the
     * record don't support hashing, we will fail at execution.
     */
    if require_hash && (rescoltype == RECORDOID as Oid || rescoltype == RECORDARRAYOID) {
        hashable = true;
    }

    /* we don't have a tlist yet, so can't assign sortgrouprefs */
    (*grpcl).tleSortGroupRef = 0;
    (*grpcl).eqop = eqop;
    (*grpcl).sortop = sortop;
    (*grpcl).reverse_sort = false; /* Sort-op is "less than", or InvalidOid */
    (*grpcl).nulls_first = false;  /* OK with or without sortop */
    (*grpcl).hashable = hashable;

    grpcl
}

/*
 * transformSetOperationTree
 *      Recursively transform leaves and internal nodes of a set-op tree
 */
unsafe fn transformSetOperationTree(
    pstate: *mut ParseState,
    stmt: *mut SelectStmt,
    isTopLevel: bool,
    targetlist: *mut *mut List,
) -> *mut Node {
    let mut isLeaf: bool;

    debug_assert!(!stmt.is_null() && IsA!(stmt as *mut Node, T_SelectStmt));

    /* Guard against stack overflow due to overly complex set-expressions */
    check_stack_depth();

    /*
     * Validity-check both leaf and internal SELECTs for disallowed ops.
     */
    if !(*stmt).intoClause.is_null() {
        ereport!(ERROR, errmsg!("INTO is only allowed on first SELECT of UNION/INTERSECT/EXCEPT")) /* C also: errcode, parser_errposition */;
    }

    /* We don't support FOR UPDATE/SHARE with set ops at the moment. */
    if !(*stmt).lockingClause.is_null() {
        ereport!(
            ERROR,
            /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
            /* translator: {} is a SQL row locking clause such as FOR UPDATE */
            errmsg!(
                "{} is not allowed with UNION/INTERSECT/EXCEPT",
                CStr::from_ptr(LCS_asString(
                    (*(linitial((*stmt).lockingClause) as *mut LockingClause)).strength
                )).to_string_lossy()
            )
        );
    }

    /*
     * If an internal node of a set-op tree has ORDER BY, LIMIT, FOR UPDATE,
     * or WITH clauses attached, we need to treat it like a leaf node to
     * generate an independent sub-Query tree.  Otherwise, it can be
     * represented by a SetOperationStmt node underneath the parent Query.
     */
    if (*stmt).op == SETOP_NONE {
        debug_assert!((*stmt).larg.is_null() && (*stmt).rarg.is_null());
        isLeaf = true;
    } else {
        debug_assert!(!(*stmt).larg.is_null() && !(*stmt).rarg.is_null());
        if !(*stmt).sortClause.is_null()
            || !(*stmt).limitOffset.is_null()
            || !(*stmt).limitCount.is_null()
            || !(*stmt).lockingClause.is_null()
            || !(*stmt).withClause.is_null()
        {
            isLeaf = true;
        } else {
            isLeaf = false;
        }
    }

    if isLeaf {
        /* Process leaf SELECT */
        let mut selectQuery: *mut Query;
        let mut selectName: [c_char; 32] = [0; 32];
        let mut nsitem: *mut ParseNamespaceItem;
        let mut rtr: *mut RangeTblRef;
        let mut tl: *mut ListCell;

        /*
         * Transform SelectStmt into a Query.
         *
         * This works the same as SELECT transformation normally would, except
         * that we prevent resolving unknown-type outputs as TEXT.
         */
        selectQuery = parse_sub_analyze(
            stmt as *mut Node,
            pstate,
            core::ptr::null_mut(),
            false,
            false,
        );

        /*
         * Check for bogus references to Vars on the current query level (but
         * upper-level references are okay). Normally this can't happen
         * because the namespace will be empty, but it could happen if we are
         * inside a rule.
         */
        if !(*pstate).p_namespace.is_null() {
            if contain_vars_of_level(selectQuery as *mut Node, 1) {
                ereport!(ERROR, errmsg!("UNION/INTERSECT/EXCEPT member statement cannot refer to other relations of same query level")) /* C also: errcode, parser_errposition */;
            }
        }

        /*
         * Extract a list of the non-junk TLEs for upper-level processing.
         */
        if !targetlist.is_null() {
            *targetlist = NIL;
            foreach!(tl, (*selectQuery).targetList, {
                let tle: *mut TargetEntry = lfirst(current_cell!(tl)) as *mut TargetEntry;
                if !(*tle).resjunk {
                    *targetlist = lappend(*targetlist, tle as *mut c_void);
                }
            });
        }

        /*
         * Make the leaf query be a subquery in the top-level rangetable.
         */
        {
            extern "C" { fn snprintf(buf: *mut c_char, n: usize, fmt: *const c_char, ...) -> c_int; }
            snprintf(
                selectName.as_mut_ptr(),
                32,
                b"*SELECT* {}\0".as_ptr() as *const c_char,
                list_length((*pstate).p_rtable) + 1,
            );
        }
        nsitem = addRangeTableEntryForSubquery(
            pstate,
            selectQuery,
            makeAlias(selectName.as_ptr(), NIL),
            false,
            false,
        );

        /*
         * Return a RangeTblRef to replace the SelectStmt in the set-op tree.
         */
        rtr = makeNode!(RangeTblRef, T_RangeTblRef);
        (*rtr).rtindex = (*nsitem).p_rtindex;
        rtr as *mut Node
    } else {
        /* Process an internal node (set operation node) */
        let mut op: *mut SetOperationStmt = makeNode!(SetOperationStmt, T_SetOperationStmt);
        let mut ltargetlist: *mut List = core::ptr::null_mut();
        let mut rtargetlist: *mut List = core::ptr::null_mut();
        let mut ltl: *mut ListCell;
        let mut rtl: *mut ListCell;
        let context: *const c_char;
        let recursive: bool = !(*pstate).p_parent_cte.is_null()
            && (*((*pstate).p_parent_cte as *mut crate::nodes::parsenodes::CommonTableExpr)).cterecursive;

        context = if (*stmt).op == SETOP_UNION {
            b"UNION\0".as_ptr() as *const c_char
        } else if (*stmt).op == SETOP_INTERSECT {
            b"INTERSECT\0".as_ptr() as *const c_char
        } else {
            b"EXCEPT\0".as_ptr() as *const c_char
        };

        (*op).op = (*stmt).op;
        (*op).all = (*stmt).all;

        /*
         * Recursively transform the left child node.
         */
        (*op).larg = transformSetOperationTree(pstate, (*stmt).larg, false, &mut ltargetlist);

        /*
         * If we are processing a recursive union query, now is the time to
         * examine the non-recursive term's output columns and mark the
         * containing CTE as having those result columns.
         */
        if isTopLevel && recursive {
            determineRecursiveColTypes(pstate, (*op).larg, ltargetlist);
        }

        /*
         * Recursively transform the right child node.
         */
        (*op).rarg = transformSetOperationTree(pstate, (*stmt).rarg, false, &mut rtargetlist);

        /*
         * Verify that the two children have the same number of non-junk
         * columns, and determine the types of the merged output columns.
         */
        if list_length(ltargetlist) != list_length(rtargetlist) {
            ereport!(ERROR, errmsg!("each {} query must have the same number of columns", CStr::from_ptr(context).to_string_lossy())) /* C also: errcode, parser_errposition */;
        }

        if !targetlist.is_null() {
            *targetlist = NIL;
        }
        (*op).colTypes = NIL;
        (*op).colTypmods = NIL;
        (*op).colCollations = NIL;
        (*op).groupClauses = NIL;

        ltl = list_head(ltargetlist);
        rtl = list_head(rtargetlist);
        while !ltl.is_null() && !rtl.is_null() {
            let ltle: *mut TargetEntry = lfirst(ltl) as *mut TargetEntry;
            let rtle: *mut TargetEntry = lfirst(rtl) as *mut TargetEntry;
            let mut lcolnode: *mut Node = (*ltle).expr as *mut Node;
            let mut rcolnode: *mut Node = (*rtle).expr as *mut Node;
            let lcoltype: Oid = exprType(lcolnode);
            let rcoltype: Oid = exprType(rcolnode);
            let mut bestexpr: *mut Node = core::ptr::null_mut();
            let bestlocation: c_int;
            let rescoltype: Oid;
            let rescoltypmod: int32;
            let rescolcoll: Oid;

            /* select common type, same as CASE et al */
            rescoltype = select_common_type(
                pstate,
                list_make2!(lcolnode, rcolnode),
                context,
                &mut bestexpr,
            );
            bestlocation = exprLocation(bestexpr);

            /*
             * Verify the coercions are actually possible.
             */
            if lcoltype != UNKNOWNOID as Oid {
                lcolnode = coerce_to_common_type(pstate, lcolnode, rescoltype, context);
            } else if IsA!(lcolnode, T_Const) || IsA!(lcolnode, T_Param) {
                lcolnode = coerce_to_common_type(pstate, lcolnode, rescoltype, context);
                (*ltle).expr = lcolnode as *mut Expr;
            }

            if rcoltype != UNKNOWNOID as Oid {
                rcolnode = coerce_to_common_type(pstate, rcolnode, rescoltype, context);
            } else if IsA!(rcolnode, T_Const) || IsA!(rcolnode, T_Param) {
                rcolnode = coerce_to_common_type(pstate, rcolnode, rescoltype, context);
                (*rtle).expr = rcolnode as *mut Expr;
            }

            rescoltypmod = select_common_typmod(
                pstate,
                list_make2!(lcolnode, rcolnode),
                rescoltype,
            );

            /*
             * Select common collation.
             */
            rescolcoll = select_common_collation(
                pstate,
                list_make2!(lcolnode, rcolnode),
                (*op).op == SETOP_UNION && (*op).all,
            );

            /* emit results */
            (*op).colTypes = lappend_oid((*op).colTypes, rescoltype);
            (*op).colTypmods = lappend_int((*op).colTypmods, rescoltypmod);
            (*op).colCollations = lappend_oid((*op).colCollations, rescolcoll);

            /*
             * For all cases except UNION ALL, identify the grouping operators
             * (and, if available, sorting operators) that will be used to
             * eliminate duplicates.
             */
            if (*op).op != SETOP_UNION || !(*op).all {
                let mut pcbstate: ParseCallbackState = core::mem::zeroed();

                setup_parser_errposition_callback(&mut pcbstate, pstate, bestlocation);

                /*
                 * If it's a recursive union, we need to require hashing
                 * support.
                 */
                (*op).groupClauses = lappend(
                    (*op).groupClauses,
                    makeSortGroupClauseForSetOp(rescoltype, recursive) as *mut c_void,
                );

                cancel_parser_errposition_callback(&mut pcbstate);
            }

            /*
             * Construct a dummy tlist entry to return.  We use a SetToDefault
             * node for the expression, since it carries exactly the fields
             * needed.
             */
            if !targetlist.is_null() {
                let mut rescolnode: *mut SetToDefault = makeNode!(SetToDefault, T_SetToDefault);
                let mut restle: *mut TargetEntry;

                (*rescolnode).typeId = rescoltype;
                (*rescolnode).typeMod = rescoltypmod;
                (*rescolnode).collation = rescolcoll;
                (*rescolnode).location = bestlocation;
                restle = makeTargetEntry(
                    rescolnode as *mut Expr,
                    0, /* no need to set resno */
                    core::ptr::null_mut(),
                    false,
                );
                *targetlist = lappend(*targetlist, restle as *mut c_void);
            }

            ltl = lnext(ltargetlist, ltl);
            rtl = lnext(rtargetlist, rtl);
        }

        op as *mut Node
    }
}

/*
 * Process the outputs of the non-recursive term of a recursive union
 * to set up the parent CTE's columns
 */
unsafe fn determineRecursiveColTypes(
    pstate: *mut ParseState,
    larg: *mut Node,
    nrtargetlist: *mut List,
) {
    let mut node: *mut Node;
    let mut leftmostRTI: c_int;
    let mut leftmostQuery: *mut Query;
    let mut targetList: *mut List = NIL;
    let mut left_tlist: *mut ListCell;
    let mut nrtl: *mut ListCell;
    let mut next_resno: c_int;

    /*
     * Find leftmost leaf SELECT
     */
    node = larg;
    while !node.is_null() && IsA!(node, T_SetOperationStmt) {
        node = (*(node as *mut SetOperationStmt)).larg;
    }
    debug_assert!(!node.is_null() && IsA!(node, T_RangeTblRef));
    leftmostRTI = (*(node as *mut RangeTblRef)).rtindex as c_int;
    leftmostQuery = (*rt_fetch(leftmostRTI as Index, (*pstate).p_rtable)).subquery;
    debug_assert!(!leftmostQuery.is_null());

    /*
     * Generate dummy targetlist using column names of leftmost select and
     * dummy result expressions of the non-recursive term.
     */
    targetList = NIL;
    next_resno = 1;

    nrtl = list_head(nrtargetlist);
    left_tlist = list_head((*leftmostQuery).targetList);
    while !nrtl.is_null() && !left_tlist.is_null() {
        let nrtle: *mut TargetEntry = lfirst(nrtl) as *mut TargetEntry;
        let lefttle: *mut TargetEntry = lfirst(left_tlist) as *mut TargetEntry;
        let colName: *mut c_char;
        let tle: *mut TargetEntry;

        debug_assert!(!(*lefttle).resjunk);
        colName = pstrdup((*lefttle).resname);
        tle = makeTargetEntry((*nrtle).expr, next_resno as AttrNumber, colName, false);
        next_resno += 1;
        targetList = lappend(targetList, tle as *mut c_void);

        nrtl = lnext(nrtargetlist, nrtl);
        left_tlist = lnext((*leftmostQuery).targetList, left_tlist);
    }

    /* Now build CTE's output column info using dummy targetlist */
    analyzeCTETargetList(pstate, (*pstate).p_parent_cte as *mut crate::nodes::parsenodes::CommonTableExpr, targetList);
}


/*
 * transformReturnStmt -
 *    transforms a return statement
 */
unsafe fn transformReturnStmt(pstate: *mut ParseState, stmt: *mut ReturnStmt) -> *mut Query {
    let mut qry: *mut Query = makeNode!(Query, T_Query);

    (*qry).commandType = CMD_SELECT;
    (*qry).isReturn = true;

    (*qry).targetList = list_make1!(makeTargetEntry(
        transformExpr(pstate, (*stmt).returnval, EXPR_KIND_SELECT_TARGET) as *mut Expr,
        1,
        core::ptr::null_mut(),
        false,
    ));

    if (*pstate).p_resolve_unknowns {
        resolveTargetListUnknowns(pstate, (*qry).targetList);
    }
    (*qry).rtable = (*pstate).p_rtable;
    (*qry).rteperminfos = (*pstate).p_rteperminfos;
    (*qry).jointree = makeFromExpr((*pstate).p_joinlist, core::ptr::null_mut());
    (*qry).hasSubLinks = (*pstate).p_hasSubLinks;
    (*qry).hasWindowFuncs = (*pstate).p_hasWindowFuncs;
    (*qry).hasTargetSRFs = (*pstate).p_hasTargetSRFs;
    (*qry).hasAggs = (*pstate).p_hasAggs;

    assign_query_collations(pstate, qry);

    qry
}


/*
 * transformUpdateStmt -
 *    transforms an update statement
 */
unsafe fn transformUpdateStmt(pstate: *mut ParseState, stmt: *mut UpdateStmt) -> *mut Query {
    let mut qry: *mut Query = makeNode!(Query, T_Query);
    let mut nsitem: *mut ParseNamespaceItem;
    let mut qual: *mut Node;

    (*qry).commandType = CMD_UPDATE;
    (*pstate).p_is_insert = false;

    /* process the WITH clause independently of all else */
    if !(*stmt).withClause.is_null() {
        (*qry).hasRecursive = (*(*stmt).withClause).recursive;
        (*qry).cteList = transformWithClause(pstate, (*stmt).withClause);
        (*qry).hasModifyingCTE = (*pstate).p_hasModifyingCTE;
    }

    (*qry).resultRelation = setTargetTable(
        pstate,
        (*stmt).relation,
        (*(*stmt).relation).inh,
        true,
        ACL_UPDATE,
    );
    nsitem = (*pstate).p_target_nsitem;

    /* subqueries in FROM cannot access the result relation */
    (*nsitem).p_lateral_only = true;
    (*nsitem).p_lateral_ok = false;

    /*
     * the FROM clause is non-standard SQL syntax. We used to be able to do
     * this with REPLACE in POSTQUEL so we keep the feature.
     */
    transformFromClause(pstate, (*stmt).fromClause);

    /* remaining clauses can reference the result relation normally */
    (*nsitem).p_lateral_only = false;
    (*nsitem).p_lateral_ok = true;

    qual = transformWhereClause(
        pstate,
        (*stmt).whereClause,
        EXPR_KIND_WHERE,
        b"WHERE\0".as_ptr() as *const c_char,
    );

    transformReturningClause(pstate, qry, (*stmt).returningClause, EXPR_KIND_RETURNING);

    /*
     * Now we are done with SELECT-like processing, and can get on with
     * transforming the target list to match the UPDATE target columns.
     */
    (*qry).targetList = transformUpdateTargetList(pstate, (*stmt).targetList);

    (*qry).rtable = (*pstate).p_rtable;
    (*qry).rteperminfos = (*pstate).p_rteperminfos;
    (*qry).jointree = makeFromExpr((*pstate).p_joinlist, qual);

    (*qry).hasTargetSRFs = (*pstate).p_hasTargetSRFs;
    (*qry).hasSubLinks = (*pstate).p_hasSubLinks;

    assign_query_collations(pstate, qry);

    qry
}

/*
 * transformUpdateTargetList -
 *    handle SET clause in UPDATE/MERGE/INSERT ... ON CONFLICT UPDATE
 */
pub unsafe fn transformUpdateTargetList(
    pstate: *mut ParseState,
    origTlist: *mut List,
) -> *mut List {
    let mut tlist: *mut List = NIL;
    let mut target_perminfo: *mut RTEPermissionInfo;
    let mut orig_tl: *mut ListCell;
    let mut tl: *mut ListCell;

    tlist = transformTargetList(pstate, origTlist, EXPR_KIND_UPDATE_SOURCE);

    /* Prepare to assign non-conflicting resnos to resjunk attributes */
    let p_target_rel = (*pstate).p_target_relation as crate::utils::rel::Relation;
    if (*pstate).p_next_resno
        <= RelationGetNumberOfAttributes(p_target_rel)
    {
        (*pstate).p_next_resno =
            RelationGetNumberOfAttributes(p_target_rel) + 1;
    }

    /* Prepare non-junk columns for assignment to target table */
    target_perminfo = (*(*pstate).p_target_nsitem).p_perminfo as *mut RTEPermissionInfo;
    orig_tl = list_head(origTlist);

    foreach!(tl, tlist, {
        let tle: *mut TargetEntry = lfirst(current_cell!(tl)) as *mut TargetEntry;
        let origTarget: *mut ResTarget;
        let attrno: c_int;

        if (*tle).resjunk {
            /*
             * Resjunk nodes need no additional processing, but be sure they
             * have resnos that do not match any target columns; else rewriter
             * or planner might get confused.  They don't need a resname
             * either.
             */
            (*tle).resno = (*pstate).p_next_resno as AttrNumber;
            (*pstate).p_next_resno += 1;
            (*tle).resname = core::ptr::null_mut();
            /* continue to next iteration */
        } else {
            if orig_tl.is_null() {
                elog!(ERROR, "UPDATE target count mismatch --- internal error");
            }
            origTarget = lfirst(orig_tl) as *mut ResTarget;

            attrno = attnameAttNum(
                (*pstate).p_target_relation,
                (*origTarget).name,
                true,
            );
            if attrno == InvalidAttrNumber as c_int {
                ereport!(ERROR, errmsg!(
                        "column \"{}\" of relation \"{}\" does not exist",
                        CStr::from_ptr((*origTarget).name).to_string_lossy(),
                        CStr::from_ptr(RelationGetRelationName(p_target_rel)).to_string_lossy()
                    )) /* C also: errcode, parser_errposition */;
            }

            updateTargetListEntry(
                pstate,
                tle,
                (*origTarget).name,
                attrno,
                (*origTarget).indirection,
                (*origTarget).location,
            );

            /* Mark the target column as requiring update permissions */
            (*target_perminfo).updatedCols = bms_add_member(
                (*target_perminfo).updatedCols,
                attrno - FirstLowInvalidHeapAttributeNumber as c_int,
            );

            orig_tl = lnext(origTlist, orig_tl);
        }
    });
    if !orig_tl.is_null() {
        elog!(ERROR, "UPDATE target count mismatch --- internal error");
    }

    tlist
}

/*
 * addNSItemForReturning -
 *    add a ParseNamespaceItem for the OLD or NEW alias in RETURNING.
 */
unsafe fn addNSItemForReturning(
    pstate: *mut ParseState,
    aliasname: *const c_char,
    returning_type: VarReturningType,
) {
    let mut colnames: *mut List;
    let numattrs: c_int;
    let mut nscolumns: *mut ParseNamespaceColumn;
    let mut nsitem: *mut ParseNamespaceItem;

    /* copy per-column data from the target relation */
    colnames = (*(*((*(*pstate).p_target_nsitem).p_rte as *mut crate::nodes::parsenodes::RangeTblEntry)).eref).colnames;
    numattrs = list_length(colnames);

    nscolumns = palloc(numattrs as usize * size_of::<ParseNamespaceColumn>())
        as *mut ParseNamespaceColumn;

    core::ptr::copy_nonoverlapping(
        (*(*pstate).p_target_nsitem).p_nscolumns,
        nscolumns,
        numattrs as usize,
    );

    /* mark all columns as returning OLD/NEW */
    for i in 0..numattrs as usize {
        (*nscolumns.add(i)).p_varreturningtype = returning_type as c_int;
    }

    /* build the nsitem, copying most fields from the target relation */
    nsitem = palloc(size_of::<ParseNamespaceItem>()) as *mut ParseNamespaceItem;
    (*nsitem).p_names = makeAlias(aliasname, colnames) as *mut c_void;
    (*nsitem).p_rte = (*(*pstate).p_target_nsitem).p_rte;
    (*nsitem).p_rtindex = (*(*pstate).p_target_nsitem).p_rtindex;
    (*nsitem).p_perminfo = (*(*pstate).p_target_nsitem).p_perminfo;
    (*nsitem).p_nscolumns = nscolumns;
    (*nsitem).p_returning_type = returning_type as c_int;

    /* add it to the query namespace as a table-only item */
    addNSItemToQuery(pstate, nsitem, false, true, false);
}

/*
 * transformReturningClause -
 *    handle a RETURNING clause in INSERT/UPDATE/DELETE/MERGE
 */
pub unsafe fn transformReturningClause(
    pstate: *mut ParseState,
    qry: *mut Query,
    returningClause: *mut ReturningClause,
    exprKind: ParseExprKind,
) {
    let save_nslen: c_int = list_length((*pstate).p_namespace);
    let save_next_resno: c_int;

    if returningClause.is_null() {
        return; /* nothing to do */
    }

    /*
     * Scan RETURNING WITH(...) options for OLD/NEW alias names.  Complain if
     * there is any conflict with existing relations.
     */
    foreach!(lc_opt, (*returningClause).options, {
        let option: *mut ReturningOption = lfirst(current_cell!(lc_opt)) as *mut ReturningOption;
        match (*option).option {
            RETURNING_OPTION_OLD => {
                if !(*qry).returningOldAlias.is_null() {
                    ereport!(
                        ERROR,
                        /* C also: errcode(ERRCODE_SYNTAX_ERROR), parser_errposition(pstate, (*option).location) */
                        /* translator: {} is OLD or NEW */
                        errmsg!("{} cannot be specified multiple times", "OLD")
                    );
                }
                (*qry).returningOldAlias = (*option).value;
            }
            RETURNING_OPTION_NEW => {
                if !(*qry).returningNewAlias.is_null() {
                    ereport!(
                        ERROR,
                        /* C also: errcode(ERRCODE_SYNTAX_ERROR), parser_errposition(pstate, (*option).location) */
                        /* translator: {} is OLD or NEW */
                        errmsg!("{} cannot be specified multiple times", "NEW")
                    );
                }
                (*qry).returningNewAlias = (*option).value;
            }
            _ => {
                elog!(ERROR, "unrecognized returning option: {:?}", (*option).option);
            }
        }

        if !refnameNamespaceItem(pstate, core::ptr::null(), (*option).value, -1, core::ptr::null_mut()).is_null() {
            ereport!(ERROR, errmsg!(
                    "table name \"{}\" specified more than once",
                    CStr::from_ptr((*option).value).to_string_lossy()
                )) /* C also: errcode, parser_errposition */;
        }

        addNSItemForReturning(
            pstate,
            (*option).value,
            if (*option).option == RETURNING_OPTION_OLD {
                VAR_RETURNING_OLD
            } else {
                VAR_RETURNING_NEW
            },
        );
    });

    /*
     * If OLD/NEW alias names weren't explicitly specified, use "old"/"new"
     * unless masked by existing relations.
     */
    if (*qry).returningOldAlias.is_null()
        && refnameNamespaceItem(pstate, core::ptr::null(), b"old\0".as_ptr() as *const c_char, -1, core::ptr::null_mut()).is_null()
    {
        (*qry).returningOldAlias = b"old\0".as_ptr() as *mut c_char;
        addNSItemForReturning(pstate, b"old\0".as_ptr() as *const c_char, VAR_RETURNING_OLD);
    }
    if (*qry).returningNewAlias.is_null()
        && refnameNamespaceItem(pstate, core::ptr::null(), b"new\0".as_ptr() as *const c_char, -1, core::ptr::null_mut()).is_null()
    {
        (*qry).returningNewAlias = b"new\0".as_ptr() as *mut c_char;
        addNSItemForReturning(pstate, b"new\0".as_ptr() as *const c_char, VAR_RETURNING_NEW);
    }

    /*
     * We need to assign resnos starting at one in the RETURNING list. Save
     * and restore the main tlist's value of p_next_resno, just in case
     * someone looks at it later (probably won't happen).
     */
    save_next_resno = (*pstate).p_next_resno;
    (*pstate).p_next_resno = 1;

    /* transform RETURNING expressions identically to a SELECT targetlist */
    (*qry).returningList = transformTargetList(pstate, (*returningClause).exprs, exprKind);

    /*
     * Complain if the nonempty tlist expanded to nothing (which is possible
     * if it contains only a star-expansion of a zero-column table).
     */
    if (*qry).returningList.is_null() {
        ereport!(ERROR, errmsg!("RETURNING must have at least one column")) /* C also: errcode, parser_errposition */;
    }

    /* mark column origins */
    markTargetListOrigins(pstate, (*qry).returningList);

    /* resolve any still-unresolved output columns as being type text */
    if (*pstate).p_resolve_unknowns {
        resolveTargetListUnknowns(pstate, (*qry).returningList);
    }

    /* restore state */
    (*pstate).p_namespace = list_truncate((*pstate).p_namespace, save_nslen);
    (*pstate).p_next_resno = save_next_resno;
}


/*
 * transformPLAssignStmt -
 *    transform a PL/pgSQL assignment statement
 */
unsafe fn transformPLAssignStmt(pstate: *mut ParseState, stmt: *mut PLAssignStmt) -> *mut Query {
    let mut qry: *mut Query = makeNode!(Query, T_Query);
    let mut cref: *mut ColumnRef = makeNode!(ColumnRef, T_ColumnRef);
    let mut indirection: *mut List = (*stmt).indirection;
    let mut nnames: c_int = (*stmt).nnames;
    let sstmt: *mut SelectStmt = (*stmt).val;
    let mut target: *mut Node;
    let mut targettype: Oid;
    let mut targettypmod: int32;
    let mut targetcollation: Oid;
    let mut tlist: *mut List;
    let mut tle: *mut TargetEntry;
    let mut type_id: Oid;
    let mut qual: *mut Node;
    let mut l: *mut ListCell;

    /*
     * First, construct a ColumnRef for the target variable.  If the target
     * has more than one dotted name, we have to pull the extra names out of
     * the indirection list.
     */
    (*cref).fields = list_make1!(makeString((*stmt).name));
    (*cref).location = (*stmt).location;
    if nnames > 1 {
        /* avoid munging the raw parsetree */
        indirection = list_copy(indirection);
        nnames -= 1;
        while nnames > 0 && !indirection.is_null() {
            let ind: *mut Node = linitial(indirection) as *mut Node;
            if !IsA!(ind, T_String) {
                elog!(ERROR, "invalid name count in PLAssignStmt");
            }
            (*cref).fields = lappend((*cref).fields, ind as *mut c_void);
            indirection = crate::nodes::pg_list::list_delete_first(indirection);
            nnames -= 1;
        }
    }

    /*
     * Transform the target reference.  Typically we will get back a Param
     * node, but there's no reason to be too picky about its type.
     */
    target = transformExpr(pstate, cref as *mut Node, EXPR_KIND_UPDATE_TARGET);
    targettype = exprType(target);
    targettypmod = exprTypmod(target);
    targetcollation = exprCollation(target);

    /*
     * The rest mostly matches transformSelectStmt, except that we needn't
     * consider WITH or INTO, and we build a targetlist our own way.
     */
    (*qry).commandType = CMD_SELECT;
    (*pstate).p_is_insert = false;

    /* make FOR UPDATE/FOR SHARE info available to addRangeTableEntry */
    (*pstate).p_locking_clause = (*sstmt).lockingClause;

    /* make WINDOW info available for window functions, too */
    (*pstate).p_windowdefs = (*sstmt).windowClause;

    /* process the FROM clause */
    transformFromClause(pstate, (*sstmt).fromClause);

    /* initially transform the targetlist as if in SELECT */
    tlist = transformTargetList(pstate, (*sstmt).targetList, EXPR_KIND_SELECT_TARGET);

    /* we should have exactly one targetlist item */
    if list_length(tlist) != 1 {
        ereport!(ERROR, errmsg!(
                "assignment source returned {} column(s)",
                list_length(tlist)
            )) /* C also: errcode */;
    }

    tle = linitial_node!(TargetEntry, T_TargetEntry, tlist);

    /*
     * This next bit is similar to transformAssignedExpr; the key difference
     * is we use COERCION_PLPGSQL not COERCION_ASSIGNMENT.
     */
    type_id = exprType((*tle).expr as *mut Node);

    (*pstate).p_expr_kind = EXPR_KIND_UPDATE_TARGET;

    if !indirection.is_null() {
        (*tle).expr = transformAssignmentIndirection(
            pstate,
            target,
            (*stmt).name,
            false,
            targettype,
            targettypmod,
            targetcollation,
            indirection,
            list_head(indirection),
            (*tle).expr as *mut Node,
            COERCION_PLPGSQL,
            exprLocation(target),
        ) as *mut Expr;
    } else if targettype != type_id
        && (targettype == RECORDOID as Oid || ISCOMPLEX(targettype))
        && (type_id == RECORDOID as Oid || ISCOMPLEX(type_id))
    {
        /*
         * Hack: do not let coerce_to_target_type() deal with inconsistent
         * composite types.  Just pass the expression result through as-is,
         * and let the PL/pgSQL executor do the conversion its way.
         */
    } else {
        /*
         * For normal non-qualified target column, do type checking and
         * coercion.
         */
        let orig_expr: *mut Node = (*tle).expr as *mut Node;

        (*tle).expr = coerce_to_target_type(
            pstate,
            orig_expr,
            type_id,
            targettype,
            targettypmod,
            COERCION_PLPGSQL,
            COERCE_IMPLICIT_CAST,
            -1,
        ) as *mut Expr;
        /* With COERCION_PLPGSQL, this error is probably unreachable */
        if (*tle).expr.is_null() {
            ereport!(ERROR, errmsg!(
                    "variable \"{}\" is of type {} but expression is of type {}",
                    CStr::from_ptr((*stmt).name).to_string_lossy(),
                    CStr::from_ptr(format_type_be(targettype)).to_string_lossy(),
                    CStr::from_ptr(format_type_be(type_id)).to_string_lossy()
                )) /* C also: errcode, errhint, parser_errposition */;
        }
    }

    (*pstate).p_expr_kind = EXPR_KIND_NONE;

    (*qry).targetList = list_make1!(tle);

    /* transform WHERE */
    qual = transformWhereClause(
        pstate,
        (*sstmt).whereClause,
        EXPR_KIND_WHERE,
        b"WHERE\0".as_ptr() as *const c_char,
    );

    /* initial processing of HAVING clause is much like WHERE clause */
    (*qry).havingQual = transformWhereClause(
        pstate,
        (*sstmt).havingClause,
        EXPR_KIND_HAVING,
        b"HAVING\0".as_ptr() as *const c_char,
    );

    (*qry).sortClause = transformSortClause(
        pstate,
        (*sstmt).sortClause,
        &mut (*qry).targetList,
        EXPR_KIND_ORDER_BY,
        false,
    );

    (*qry).groupClause = transformGroupClause(
        pstate,
        (*sstmt).groupClause,
        &mut (*qry).groupingSets,
        &mut (*qry).targetList,
        (*qry).sortClause,
        EXPR_KIND_GROUP_BY,
        false,
    );
    (*qry).groupDistinct = (*sstmt).groupDistinct;

    if (*sstmt).distinctClause.is_null() {
        (*qry).distinctClause = NIL;
        (*qry).hasDistinctOn = false;
    } else if linitial((*sstmt).distinctClause).is_null() {
        (*qry).distinctClause = transformDistinctClause(
            pstate,
            &mut (*qry).targetList,
            (*qry).sortClause,
            false,
        );
        (*qry).hasDistinctOn = false;
    } else {
        (*qry).distinctClause = transformDistinctOnClause(
            pstate,
            (*sstmt).distinctClause,
            &mut (*qry).targetList,
            (*qry).sortClause,
        );
        (*qry).hasDistinctOn = true;
    }

    (*qry).limitOffset = transformLimitClause(
        pstate,
        (*sstmt).limitOffset,
        EXPR_KIND_OFFSET,
        b"OFFSET\0".as_ptr() as *const c_char,
        (*sstmt).limitOption,
    );
    (*qry).limitCount = transformLimitClause(
        pstate,
        (*sstmt).limitCount,
        EXPR_KIND_LIMIT,
        b"LIMIT\0".as_ptr() as *const c_char,
        (*sstmt).limitOption,
    );
    (*qry).limitOption = (*sstmt).limitOption;

    (*qry).windowClause = transformWindowDefinitions(
        pstate,
        (*pstate).p_windowdefs,
        &mut (*qry).targetList,
    );

    (*qry).rtable = (*pstate).p_rtable;
    (*qry).rteperminfos = (*pstate).p_rteperminfos;
    (*qry).jointree = makeFromExpr((*pstate).p_joinlist, qual);

    (*qry).hasSubLinks = (*pstate).p_hasSubLinks;
    (*qry).hasWindowFuncs = (*pstate).p_hasWindowFuncs;
    (*qry).hasTargetSRFs = (*pstate).p_hasTargetSRFs;
    (*qry).hasAggs = (*pstate).p_hasAggs;

    foreach!(l, (*sstmt).lockingClause, {
        transformLockingClause(pstate, qry, lfirst(current_cell!(l)) as *mut LockingClause, false);
    });

    assign_query_collations(pstate, qry);

    /* this must be done after collations, for reliable comparison of exprs */
    if (*pstate).p_hasAggs
        || !(*qry).groupClause.is_null()
        || !(*qry).groupingSets.is_null()
        || !(*qry).havingQual.is_null()
    {
        parseCheckAggregates(pstate, qry);
    }

    qry
}


/*
 * transformDeclareCursorStmt -
 *    transform a DECLARE CURSOR Statement
 *
 * DECLARE CURSOR is like other utility statements in that we emit it as a
 * CMD_UTILITY Query node; however, we must first transform the contained
 * query.
 */
unsafe fn transformDeclareCursorStmt(
    pstate: *mut ParseState,
    stmt: *mut DeclareCursorStmt,
) -> *mut Query {
    let mut result: *mut Query;
    let mut query: *mut Query;

    if ((*stmt).options & CURSOR_OPT_SCROLL) != 0
        && ((*stmt).options & CURSOR_OPT_NO_SCROLL) != 0
    {
        ereport!(
            ERROR,
            /* C also: errcode(ERRCODE_INVALID_CURSOR_DEFINITION) */
            /* translator: {} is a SQL keyword */
            errmsg!("cannot specify both {} and {}", "SCROLL", "NO SCROLL")
        );
    }

    if ((*stmt).options & CURSOR_OPT_ASENSITIVE) != 0
        && ((*stmt).options & CURSOR_OPT_INSENSITIVE) != 0
    {
        ereport!(
            ERROR,
            /* C also: errcode(ERRCODE_INVALID_CURSOR_DEFINITION) */
            /* translator: {} is a SQL keyword */
            errmsg!("cannot specify both {} and {}", "ASENSITIVE", "INSENSITIVE")
        );
    }

    /* Transform contained query, not allowing SELECT INTO */
    query = transformStmt(pstate, (*stmt).query);
    (*stmt).query = query as *mut Node;

    /* Grammar should not have allowed anything but SELECT */
    if !IsA!(query as *mut Node, T_Query) || (*query).commandType != CMD_SELECT {
        elog!(ERROR, "unexpected non-SELECT command in DECLARE CURSOR");
    }

    /*
     * We also disallow data-modifying WITH in a cursor.
     */
    if (*query).hasModifyingCTE {
        ereport!(ERROR, errmsg!("DECLARE CURSOR must not contain data-modifying statements in WITH")) /* C also: errcode */;
    }

    /* FOR UPDATE and WITH HOLD are not compatible */
    if !(*query).rowMarks.is_null() && ((*stmt).options & CURSOR_OPT_HOLD) != 0 {
        ereport!(
            ERROR,
            /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errdetail("Holdable cursors must be READ ONLY.") */
            /* translator: {} is a SQL row locking clause such as FOR UPDATE */
            errmsg!(
                "DECLARE CURSOR WITH HOLD ... {} is not supported",
                CStr::from_ptr(LCS_asString((*(linitial((*query).rowMarks) as *mut RowMarkClause)).strength)).to_string_lossy()
            )
        );
    }

    /* FOR UPDATE and SCROLL are not compatible */
    if !(*query).rowMarks.is_null() && ((*stmt).options & CURSOR_OPT_SCROLL) != 0 {
        ereport!(
            ERROR,
            /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errdetail("Scrollable cursors must be READ ONLY.") */
            /* translator: {} is a SQL row locking clause such as FOR UPDATE */
            errmsg!(
                "DECLARE SCROLL CURSOR ... {} is not supported",
                CStr::from_ptr(LCS_asString((*(linitial((*query).rowMarks) as *mut RowMarkClause)).strength)).to_string_lossy()
            )
        );
    }

    /* FOR UPDATE and INSENSITIVE are not compatible */
    if !(*query).rowMarks.is_null() && ((*stmt).options & CURSOR_OPT_INSENSITIVE) != 0 {
        ereport!(
            ERROR,
            /* C also: errcode(ERRCODE_INVALID_CURSOR_DEFINITION), errdetail("Insensitive cursors must be READ ONLY.") */
            /* translator: {} is a SQL row locking clause such as FOR UPDATE */
            errmsg!(
                "DECLARE INSENSITIVE CURSOR ... {} is not valid",
                CStr::from_ptr(LCS_asString((*(linitial((*query).rowMarks) as *mut RowMarkClause)).strength)).to_string_lossy()
            )
        );
    }

    /* represent the command as a utility Query */
    result = makeNode!(Query, T_Query);
    (*result).commandType = CMD_UTILITY;
    (*result).utilityStmt = stmt as *mut Node;

    result
}


/*
 * transformExplainStmt -
 *    transform an EXPLAIN Statement
 *
 * EXPLAIN is like other utility statements in that we emit it as a
 * CMD_UTILITY Query node; however, we must first transform the contained
 * query.
 */
unsafe fn transformExplainStmt(pstate: *mut ParseState, stmt: *mut ExplainStmt) -> *mut Query {
    let mut result: *mut Query;
    let mut generic_plan: bool = false;
    let mut paramTypes: *mut Oid = core::ptr::null_mut();
    let mut numParams: c_int = 0;

    /*
     * If we have no external source of parameter definitions, and the
     * GENERIC_PLAN option is specified, then accept variable parameter
     * definitions.
     */
    if (*pstate).p_paramref_hook.is_none() {
        let mut lc: *mut ListCell;
        foreach!(lc, (*stmt).options, {
            let opt: *mut DefElem = lfirst(current_cell!(lc)) as *mut DefElem;
            extern "C" { fn strcmp(a: *const c_char, b: *const c_char) -> c_int; }
            if strcmp((*opt).defname, b"generic_plan\0".as_ptr() as *const c_char) == 0 {
                generic_plan = defGetBoolean(opt);
            }
            /* don't "break", as we want the last value */
        });
        if generic_plan {
            setup_parse_variable_parameters(pstate as *mut crate::parser::parse_param::ParseState, &mut paramTypes, &mut numParams);
        }
    }

    /* transform contained query, allowing SELECT INTO */
    (*stmt).query =
        transformOptionalSelectInto(pstate, (*stmt).query) as *mut Node;

    /* make sure all is well with parameter types */
    if generic_plan {
        check_variable_parameters(pstate as *mut crate::parser::parse_param::ParseState, (*stmt).query as *mut Query);
    }

    /* represent the command as a utility Query */
    result = makeNode!(Query, T_Query);
    (*result).commandType = CMD_UTILITY;
    (*result).utilityStmt = stmt as *mut Node;

    result
}


/*
 * transformCreateTableAsStmt -
 *    transform a CREATE TABLE AS, SELECT ... INTO, or CREATE MATERIALIZED VIEW
 *    Statement
 */
unsafe fn transformCreateTableAsStmt(
    pstate: *mut ParseState,
    stmt: *mut CreateTableAsStmt,
) -> *mut Query {
    let mut result: *mut Query;
    let mut query: *mut Query;

    /* transform contained query, not allowing SELECT INTO */
    query = transformStmt(pstate, (*stmt).query);
    (*stmt).query = query as *mut Node;

    /* additional work needed for CREATE MATERIALIZED VIEW */
    if (*stmt).objtype == OBJECT_MATVIEW {
        /*
         * Prohibit a data-modifying CTE in the query used to create a
         * materialized view.
         */
        if (*query).hasModifyingCTE {
            ereport!(ERROR, errmsg!("materialized views must not use data-modifying statements in WITH")) /* C also: errcode */;
        }

        /*
         * Check whether any temporary database objects are used in the
         * creation query.
         */
        if isQueryUsingTempRelation(query) {
            ereport!(ERROR, errmsg!("materialized views must not use temporary tables or views")) /* C also: errcode */;
        }

        /*
         * A materialized view would either need to save parameters for use in
         * maintaining/loading the data or prohibit them entirely.
         */
        if query_contains_extern_params(query) {
            ereport!(ERROR, errmsg!("materialized views may not be defined using bound parameters")) /* C also: errcode */;
        }

        /*
         * For now, we disallow unlogged materialized views.
         */
        if (*(*stmt).into).rel != core::ptr::null_mut()
            && (*(*(*stmt).into).rel).relpersistence == RELPERSISTENCE_UNLOGGED
        {
            ereport!(ERROR, errmsg!("materialized views cannot be unlogged")) /* C also: errcode */;
        }

        /*
         * At runtime, we'll need a copy of the parsed-but-not-rewritten Query
         * for purposes of creating the view's ON SELECT rule.
         */
        (*(*stmt).into).viewQuery = copyObject(query as *mut c_void) as *mut crate::nodes::parsenodes::Query;
    }

    /* represent the command as a utility Query */
    result = makeNode!(Query, T_Query);
    (*result).commandType = CMD_UTILITY;
    (*result).utilityStmt = stmt as *mut Node;

    result
}

/*
 * transform a CallStmt
 */
unsafe fn transformCallStmt(pstate: *mut ParseState, stmt: *mut CallStmt) -> *mut Query {
    let mut targs: *mut List = NIL;
    let mut lc: *mut ListCell;
    let mut node: *mut Node;
    let mut fexpr: *mut FuncExpr;
    let mut proctup: HeapTuple;
    let mut proargmodes: Datum = 0;
    let mut isNull: bool = false;
    let mut outargs: *mut List = NIL;
    let mut result: *mut Query;

    /*
     * First, do standard parse analysis on the procedure call and its
     * arguments, allowing us to identify the called procedure.
     */
    targs = NIL;
    foreach!(lc, (*(*stmt).funccall).args, {
        targs = lappend(
            targs,
            transformExpr(pstate, lfirst(current_cell!(lc)) as *mut Node, EXPR_KIND_CALL_ARGUMENT)
                as *mut c_void,
        );
    });

    node = ParseFuncOrColumn(
        pstate,
        (*(*stmt).funccall).funcname,
        targs,
        (*pstate).p_last_srf,
        (*stmt).funccall,
        true,
        (*(*stmt).funccall).location,
    );

    assign_expr_collations(pstate, node);

    fexpr = castNode!(FuncExpr, T_FuncExpr, node);

    proctup = SearchSysCache1(PROCOID, ObjectIdGetDatum((*fexpr).funcid));
    if !HeapTupleIsValid(proctup) {
        elog!(ERROR, "cache lookup failed for function {}", (*fexpr).funcid);
    }

    /*
     * Expand the argument list to deal with named-argument notation and
     * default arguments.
     */
    (*fexpr).args = expand_function_arguments(
        (*fexpr).args,
        true,
        (*fexpr).funcresulttype,
        proctup as *mut crate::optimizer::optimizer::HeapTupleData,
    );

    /* Fetch proargmodes; if it's null, there are no output args */
    proargmodes = SysCacheGetAttr(PROCOID, proctup, Anum_pg_proc_proargmodes, &mut isNull);
    if !isNull {
        /*
         * Split the list into input arguments in fexpr->args and output
         * arguments in stmt->outargs.  INOUT arguments appear in both lists.
         */
        let arr: *mut ArrayType = DatumGetArrayTypeP(proargmodes);
        let numargs: c_int = list_length((*fexpr).args);
        if ARR_NDIM(arr) != 1
            || *ARR_DIMS(arr) != numargs
            || ARR_HASNULL(arr)
            || ARR_ELEMTYPE(arr) != CHAROID
        {
            elog!(
                ERROR,
                "proargmodes is not a 1-D char array of length {} or it contains nulls",
                numargs
            );
        }
        let argmodes: *mut c_char = ARR_DATA_PTR(arr) as *mut c_char;

        let mut inargs: *mut List = NIL;
        let mut i: c_int = 0;
        foreach!(lc, (*fexpr).args, {
            let n: *mut Node = lfirst(current_cell!(lc)) as *mut Node;
            match *argmodes.offset(i as isize) {
                PROARGMODE_IN | PROARGMODE_VARIADIC => {
                    inargs = lappend(inargs, n as *mut c_void);
                }
                PROARGMODE_OUT => {
                    outargs = lappend(outargs, n as *mut c_void);
                }
                PROARGMODE_INOUT => {
                    inargs = lappend(inargs, n as *mut c_void);
                    outargs = lappend(outargs, copyObject(n as *mut c_void));
                }
                _ => {
                    /* note we don't support PROARGMODE_TABLE */
                    elog!(
                        ERROR,
                        "invalid argmode {} for procedure",
                        *argmodes.offset(i as isize) as c_int
                    );
                }
            }
            i += 1;
        });
        (*fexpr).args = inargs;
    }

    (*stmt).funcexpr = fexpr;
    (*stmt).outargs = outargs;

    ReleaseSysCache(proctup);

    /* represent the command as a utility Query */
    result = makeNode!(Query, T_Query);
    (*result).commandType = CMD_UTILITY;
    (*result).utilityStmt = stmt as *mut Node;

    result
}

/*
 * Produce a string representation of a LockClauseStrength value.
 * This should only be applied to valid values (not LCS_NONE).
 */
pub unsafe fn LCS_asString(strength: LockClauseStrength) -> *const c_char {
    match strength {
        LCS_NONE => {
            debug_assert!(false);
        }
        LCS_FORKEYSHARE => {
            return b"FOR KEY SHARE\0".as_ptr() as *const c_char;
        }
        LCS_FORSHARE => {
            return b"FOR SHARE\0".as_ptr() as *const c_char;
        }
        LCS_FORNOKEYUPDATE => {
            return b"FOR NO KEY UPDATE\0".as_ptr() as *const c_char;
        }
        LCS_FORUPDATE => {
            return b"FOR UPDATE\0".as_ptr() as *const c_char;
        }
    }
    b"FOR some\0".as_ptr() as *const c_char /* shouldn't happen */
}

/*
 * Check for features that are not supported with FOR [KEY] UPDATE/SHARE.
 *
 * exported so planner can check again after rewriting, query pullup, etc
 */
pub unsafe fn CheckSelectLocking(qry: *mut Query, strength: LockClauseStrength) {
    debug_assert!(strength != LCS_NONE); /* else caller error */

    if !(*qry).setOperations.is_null() {
        ereport!(
            ERROR,
            /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
            /* translator: {} is a SQL row locking clause such as FOR UPDATE */
            errmsg!(
                "{} is not allowed with UNION/INTERSECT/EXCEPT",
                CStr::from_ptr(LCS_asString(strength)).to_string_lossy()
            )
        );
    }
    if !(*qry).distinctClause.is_null() {
        ereport!(
            ERROR,
            /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
            /* translator: {} is a SQL row locking clause such as FOR UPDATE */
            errmsg!(
                "{} is not allowed with DISTINCT clause",
                CStr::from_ptr(LCS_asString(strength)).to_string_lossy()
            )
        );
    }
    if !(*qry).groupClause.is_null() || !(*qry).groupingSets.is_null() {
        ereport!(
            ERROR,
            /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
            /* translator: {} is a SQL row locking clause such as FOR UPDATE */
            errmsg!(
                "{} is not allowed with GROUP BY clause",
                CStr::from_ptr(LCS_asString(strength)).to_string_lossy()
            )
        );
    }
    if !(*qry).havingQual.is_null() {
        ereport!(
            ERROR,
            /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
            /* translator: {} is a SQL row locking clause such as FOR UPDATE */
            errmsg!(
                "{} is not allowed with HAVING clause",
                CStr::from_ptr(LCS_asString(strength)).to_string_lossy()
            )
        );
    }
    if (*qry).hasAggs {
        ereport!(
            ERROR,
            /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
            /* translator: {} is a SQL row locking clause such as FOR UPDATE */
            errmsg!(
                "{} is not allowed with aggregate functions",
                CStr::from_ptr(LCS_asString(strength)).to_string_lossy()
            )
        );
    }
    if (*qry).hasWindowFuncs {
        ereport!(
            ERROR,
            /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
            /* translator: {} is a SQL row locking clause such as FOR UPDATE */
            errmsg!(
                "{} is not allowed with window functions",
                CStr::from_ptr(LCS_asString(strength)).to_string_lossy()
            )
        );
    }
    if (*qry).hasTargetSRFs {
        ereport!(
            ERROR,
            /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
            /* translator: {} is a SQL row locking clause such as FOR UPDATE */
            errmsg!(
                "{} is not allowed with set-returning functions in the target list",
                CStr::from_ptr(LCS_asString(strength)).to_string_lossy()
            )
        );
    }
}

/*
 * Transform a FOR [KEY] UPDATE/SHARE clause
 *
 * This basically involves replacing names by integer relids.
 *
 * NB: if you need to change this, see also markQueryForLocking()
 * in rewriteHandler.c, and isLockedRefname() in parse_relation.c.
 */
unsafe fn transformLockingClause(
    pstate: *mut ParseState,
    qry: *mut Query,
    lc: *mut LockingClause,
    pushedDown: bool,
) {
    let lockedRels: *mut List = (*lc).lockedRels;
    let mut l: *mut ListCell;
    let mut rt: *mut ListCell;
    let mut i: Index;
    let mut allrels: *mut LockingClause;

    CheckSelectLocking(qry, (*lc).strength);

    /* make a clause we can pass down to subqueries to select all rels */
    allrels = makeNode!(LockingClause, T_LockingClause);
    (*allrels).lockedRels = NIL; /* indicates all rels */
    (*allrels).strength = (*lc).strength;
    (*allrels).waitPolicy = (*lc).waitPolicy;

    if lockedRels.is_null() {
        /*
         * Lock all regular tables used in query and its subqueries.  We
         * examine inFromCl to exclude auto-added RTEs, particularly NEW/OLD
         * in rules.
         */
        i = 0;
        foreach!(rt, (*qry).rtable, {
            let rte: *mut RangeTblEntry = lfirst(current_cell!(rt)) as *mut RangeTblEntry;
            i += 1;
            if !(*rte).inFromCl {
                /* continue */
            } else {
                match (*rte).rtekind {
                    RTE_RELATION => {
                        let perminfo: *mut RTEPermissionInfo;
                        applyLockingClause(qry, i, (*lc).strength, (*lc).waitPolicy, pushedDown);
                        perminfo = getRTEPermissionInfo((*qry).rteperminfos, rte);
                        (*perminfo).requiredPerms |= ACL_SELECT_FOR_UPDATE;
                    }
                    RTE_SUBQUERY => {
                        applyLockingClause(qry, i, (*lc).strength, (*lc).waitPolicy, pushedDown);
                        /*
                         * FOR UPDATE/SHARE of subquery is propagated to all of
                         * subquery's rels, too.
                         */
                        transformLockingClause(pstate, (*rte).subquery, allrels, true);
                    }
                    _ => {
                        /* ignore JOIN, SPECIAL, FUNCTION, VALUES, CTE RTEs */
                    }
                }
            }
        });
    } else {
        /*
         * Lock just the named tables.
         */
        foreach!(l, lockedRels, {
            let thisrel: *mut RangeVar = lfirst(current_cell!(l)) as *mut RangeVar;

            /* For simplicity we insist on unqualified alias names here */
            if !(*thisrel).catalogname.is_null() || !(*thisrel).schemaname.is_null() {
                ereport!(
                    ERROR,
                    /* C also: errcode(ERRCODE_SYNTAX_ERROR), parser_errposition(pstate, (*thisrel).location) */
                    /* translator: {} is a SQL row locking clause such as FOR UPDATE */
                    errmsg!(
                        "{} must specify unqualified relation names",
                        CStr::from_ptr(LCS_asString((*lc).strength)).to_string_lossy()
                    )
                );
            }

            i = 0;
            let mut found = false;
            foreach!(rt, (*qry).rtable, {
                let rte: *mut RangeTblEntry = lfirst(current_cell!(rt)) as *mut RangeTblEntry;
                let mut rtename: *const c_char = (*(*rte).eref).aliasname;
                i += 1;
                if !(*rte).inFromCl {
                    /* skip */
                } else {
                    /*
                     * A join RTE without an alias is not visible as a relation
                     * name and needs to be skipped (otherwise it might hide a
                     * base relation with the same name), except if it has a USING
                     * alias.
                     *
                     * Subquery and values RTEs without aliases are never visible
                     * as relation names and must always be skipped.
                     */
                    if (*rte).alias.is_null() {
                        if (*rte).rtekind == RTE_JOIN {
                            if (*rte).join_using_alias.is_null() {
                                /* skip */
                            } else {
                                rtename = (*(*rte).join_using_alias).aliasname;
                            }
                        } else if (*rte).rtekind == RTE_SUBQUERY || (*rte).rtekind == RTE_VALUES {
                            /* skip */
                        }
                    }

                    extern "C" { fn strcmp(a: *const c_char, b: *const c_char) -> c_int; }
                    if strcmp(rtename, (*thisrel).relname) == 0 {
                        match (*rte).rtekind {
                            RTE_RELATION => {
                                let perminfo: *mut RTEPermissionInfo;
                                applyLockingClause(
                                    qry, i, (*lc).strength, (*lc).waitPolicy, pushedDown,
                                );
                                perminfo = getRTEPermissionInfo((*qry).rteperminfos, rte);
                                (*perminfo).requiredPerms |= ACL_SELECT_FOR_UPDATE;
                            }
                            RTE_SUBQUERY => {
                                applyLockingClause(
                                    qry, i, (*lc).strength, (*lc).waitPolicy, pushedDown,
                                );
                                /* see comment above */
                                transformLockingClause(pstate, (*rte).subquery, allrels, true);
                            }
                            RTE_JOIN => {
                                ereport!(
                                    ERROR,
                                    /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED), parser_errposition(pstate, (*thisrel).location) */
                                    /* translator: {} is a SQL row locking clause such as FOR UPDATE */
                                    errmsg!(
                                        "{} cannot be applied to a join",
                                        CStr::from_ptr(LCS_asString((*lc).strength)).to_string_lossy()
                                    )
                                );
                            }
                            RTE_FUNCTION => {
                                ereport!(
                                    ERROR,
                                    /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED), parser_errposition(pstate, (*thisrel).location) */
                                    /* translator: {} is a SQL row locking clause such as FOR UPDATE */
                                    errmsg!(
                                        "{} cannot be applied to a function",
                                        CStr::from_ptr(LCS_asString((*lc).strength)).to_string_lossy()
                                    )
                                );
                            }
                            RTE_TABLEFUNC => {
                                ereport!(
                                    ERROR,
                                    /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED), parser_errposition(pstate, (*thisrel).location) */
                                    /* translator: {} is a SQL row locking clause such as FOR UPDATE */
                                    errmsg!(
                                        "{} cannot be applied to a table function",
                                        CStr::from_ptr(LCS_asString((*lc).strength)).to_string_lossy()
                                    )
                                );
                            }
                            RTE_VALUES => {
                                ereport!(
                                    ERROR,
                                    /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED), parser_errposition(pstate, (*thisrel).location) */
                                    /* translator: {} is a SQL row locking clause such as FOR UPDATE */
                                    errmsg!(
                                        "{} cannot be applied to VALUES",
                                        CStr::from_ptr(LCS_asString((*lc).strength)).to_string_lossy()
                                    )
                                );
                            }
                            RTE_CTE => {
                                ereport!(
                                    ERROR,
                                    /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED), parser_errposition(pstate, (*thisrel).location) */
                                    /* translator: {} is a SQL row locking clause such as FOR UPDATE */
                                    errmsg!(
                                        "{} cannot be applied to a WITH query",
                                        CStr::from_ptr(LCS_asString((*lc).strength)).to_string_lossy()
                                    )
                                );
                            }
                            RTE_NAMEDTUPLESTORE => {
                                ereport!(
                                    ERROR,
                                    /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED), parser_errposition(pstate, (*thisrel).location) */
                                    /* translator: {} is a SQL row locking clause such as FOR UPDATE */
                                    errmsg!(
                                        "{} cannot be applied to a named tuplestore",
                                        CStr::from_ptr(LCS_asString((*lc).strength)).to_string_lossy()
                                    )
                                );
                            }
                            /* Shouldn't be possible to see RTE_RESULT here */
                            _ => {
                                elog!(
                                    ERROR,
                                    "unrecognized RTE type: {}",
                                    (*rte).rtekind as c_int
                                );
                            }
                        }
                        found = true;
                        break;
                    }
                }
            });
            if !found {
                ereport!(
                    ERROR,
                    /* C also: errcode(ERRCODE_UNDEFINED_TABLE), parser_errposition(pstate, (*thisrel).location) */
                    /* translator: {} is a SQL row locking clause such as FOR UPDATE */
                    errmsg!(
                        "relation \"{}\" in {} clause not found in FROM clause",
                        CStr::from_ptr((*thisrel).relname).to_string_lossy(),
                        CStr::from_ptr(LCS_asString((*lc).strength)).to_string_lossy()
                    )
                );
            }
        });
    }
}

/*
 * Record locking info for a single rangetable item
 */
pub unsafe fn applyLockingClause(
    qry: *mut Query,
    rtindex: Index,
    strength: LockClauseStrength,
    waitPolicy: LockWaitPolicy,
    pushedDown: bool,
) {
    let mut rc: *mut RowMarkClause;

    debug_assert!(strength != LCS_NONE); /* else caller error */

    /* If it's an explicit clause, make sure hasForUpdate gets set */
    if !pushedDown {
        (*qry).hasForUpdate = true;
    }

    /* Check for pre-existing entry for same rtindex */
    rc = get_parse_rowmark(qry, rtindex);
    if !rc.is_null() {
        /*
         * If the same RTE is specified with more than one locking strength,
         * use the strongest.
         */
        if ((*rc).strength as c_int) < (strength as c_int) {
            (*rc).strength = strength;
        }
        if ((*rc).waitPolicy as c_int) < (waitPolicy as c_int) {
            (*rc).waitPolicy = waitPolicy;
        }
        (*rc).pushedDown = (*rc).pushedDown && pushedDown;
        return;
    }

    /* Make a new RowMarkClause */
    rc = makeNode!(RowMarkClause, T_RowMarkClause);
    (*rc).rti = rtindex;
    (*rc).strength = strength;
    (*rc).waitPolicy = waitPolicy;
    (*rc).pushedDown = pushedDown;
    (*qry).rowMarks = lappend((*qry).rowMarks, rc as *mut c_void);
}

/*
 * Coverage testing for raw_expression_tree_walker().
 *
 * When enabled, we run raw_expression_tree_walker() over every DML statement
 * submitted to parse analysis.  Without this provision, that function is only
 * applied in limited cases involving CTEs, and we don't really want to have
 * to test everything inside as well as outside a CTE.
 */
#[cfg(feature = "debug_node_tests")]
unsafe fn test_raw_expression_coverage(node: *mut Node, context: *mut c_void) -> bool {
    if node.is_null() {
        return false;
    }
    raw_expression_tree_walker(node, Some(test_raw_expression_coverage), context)
}
