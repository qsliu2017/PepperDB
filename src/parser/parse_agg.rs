/*-------------------------------------------------------------------------
 *
 * parse_agg.rs
 *   handle aggregates and window functions in parser
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *   src/backend/parser/parse_agg.c
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

use std::ffi::{c_char, c_int, c_void};

use crate::{castNode, current_cell, foreach, intVal, lfirst_node, linitial_node, list_make1, list_make2, makeNode, strVal, IsA};

use crate::postgres_ext::Oid;
use crate::postgres::Datum;
use crate::c::{OidIsValid, int32};

use crate::nodes::nodes::{nodeTag, Node, NodeTag, NodeTag::*};
use crate::nodes::pg_list::{
    List, NIL,
    lfirst, lfirst_int, lfirst_oid, linitial, lsecond, llast, lnext,
    lappend, lappend_int, lappend_oid, lcons, list_head,
    list_concat, list_length, list_make1_impl,
    list_nth, list_nth_cell, list_truncate, list_member_int,
    list_copy, list_copy_tail, list_union_int, list_intersection_int,
    list_sort, list_int_cmp, list_delete_cell, ListCell,
};
use crate::nodes::pg_list::list_sort_comparator;
use crate::nodes::bitmapset::{Bitmapset, bms_add_member, bms_int_members, bms_next_member};
use crate::nodes::nodeFuncs::{
    exprType, exprTypmod, exprCollation, exprLocation,
    expression_tree_walker, expression_tree_mutator,
    query_tree_walker, query_tree_mutator,
    QTW_EXAMINE_RTES_BEFORE,
};
use crate::nodes::makefuncs::{
    makeConst, makeBoolConst, makeBoolExpr, makeRangeVar, makeTargetEntry,
    makeSimpleA_Expr, makeVar, makeFuncExpr,
};
use crate::nodes::value::{makeString};
use crate::nodes::primnodes::{
    Expr, Var, Param, ParamKind, PARAM_EXEC, PARAM_SUBLINK,
    CaseTestExpr, NullTest, NullTestType, BooleanTest, BoolTestType,
    OpExpr, RowCompareExpr, CoalesceExpr, MinMaxExpr, ArrayExpr,
    RowExpr, CollateExpr, SubLink, SubLinkType,
    FuncExpr, CoercionForm, CoercionForm::*,
    WindowFunc,
};
use crate::nodes::primnodes::{
    Aggref, TargetEntry, Alias,
    BoolExpr, BoolExprType,
    GroupingFunc, MergeSupportFunc,
    NamedArgExpr,
    CaseExpr, CaseWhen,
};
use crate::nodes::parsenodes::{
    A_Const, A_Expr, FuncCall, SortBy, SelectStmt, ResTarget,
    Query,
    GroupingSet, GroupingSetKind, GroupingSetKind::*,
    WindowDef, WindowClause, SortGroupClause,
    RangeTblEntry, RTEKind, RTEKind::*,
    CommonTableExpr,
};

use crate::parser::parse_node::{
    cancel_parser_errposition_callback, parser_errposition,
    setup_parser_errposition_callback,
    Index, ParseCallbackState, ParseExprKind, ParseExprKind::*,
    ParseNamespaceColumn, ParseNamespaceItem, ParseState,
};
use crate::parser::parse_expr::transformExpr;
use crate::parser::parse_clause::{
    transformSortClause, transformDistinctClause, addTargetToSortList,
};
use crate::parser::parse_relation::{
    addRangeTableEntryForGroup,
    get_rte_attribute_name,
    GetRTEByRangeTablePosn,
};
use crate::parser::parsetree::{rt_fetch};

use crate::catalog::pg_aggregate::AGGKIND_IS_ORDERED_SET;
use crate::catalog::pg_type_d::{BYTEAOID};

use crate::utils::cache::lsyscache::{
    format_type_be,
    pstrdup, palloc, pfree,
};
use crate::postgres_ext::InvalidOid;

// ---------------------------------------------------------------------------
// OID constants not yet in dedicated modules
// ---------------------------------------------------------------------------
const INTERNALOID: Oid = 2281;

// FRAMEOPTION_* bitmasks (nodes/parsenodes.h)
const FRAMEOPTION_DEFAULTS: c_int = 0x000;

// ---------------------------------------------------------------------------
// Context structs
// ---------------------------------------------------------------------------

struct CheckAggArgumentsContext {
    pstate: *mut ParseState,
    min_varlevel: c_int,
    min_agglevel: c_int,
    min_ctelevel: c_int,
    min_cte: *mut RangeTblEntry,
    sublevels_up: c_int,
}

struct SubstituteGroupedColumnsContext {
    pstate: *mut ParseState,
    qry: *mut Query,
    hasJoinRTEs: bool,
    groupClauses: *mut List,
    groupClauseCommonVars: *mut List,
    gset_common: *mut List,
    have_non_var_grouping: bool,
    func_grouped_rels: *mut *mut List,
    sublevels_up: c_int,
    in_agg_direct_args: bool,
}

// ---------------------------------------------------------------------------
// Local stubs (unported dependencies)
// ---------------------------------------------------------------------------

// TODO(pg-port): nodes/nodeFuncs.c contain_vars_of_level
unsafe fn contain_vars_of_level(node: *mut Node, levelsup: c_int) -> bool {
    false
}
// TODO(pg-port): nodes/nodeFuncs.c locate_var_of_level
unsafe fn locate_var_of_level(node: *mut Node, levelsup: c_int) -> c_int {
    -1
}
// TODO(pg-port): nodes/nodeFuncs.c contain_aggs_of_level
unsafe fn contain_aggs_of_level(node: *mut Node, levelsup: c_int) -> bool {
    false
}
// TODO(pg-port): nodes/nodeFuncs.c locate_agg_of_level
unsafe fn locate_agg_of_level(node: *mut Node, levelsup: c_int) -> c_int {
    -1
}
// TODO(pg-port): nodes/nodeFuncs.c contain_windowfuncs
unsafe fn contain_windowfuncs(node: *mut Node) -> bool {
    false
}
// TODO(pg-port): nodes/nodeFuncs.c locate_windowfunc
unsafe fn locate_windowfunc(node: *mut Node) -> c_int {
    -1
}
// TODO(pg-port): optimizer/optimizer.c flatten_join_alias_vars
unsafe fn flatten_join_alias_vars(
    _root: *mut c_void,
    qry: *mut Query,
    node: *mut Node,
) -> *mut Node {
    node
}
// TODO(pg-port): nodes/equalfuncs.c equal
unsafe fn equal(a: *const c_void, b: *const c_void) -> bool {
    false
}
// TODO(pg-port): copyfuncs.c copyObject
unsafe fn copyObject<T>(node: *mut T) -> *mut T {
    node
}
// TODO(pg-port): catalog/pg_constraint.c check_functional_grouping
unsafe fn check_functional_grouping(
    _relid: Oid,
    _varno: Index,
    _varlevelsup: Index,
    _groupClauseCommonVars: *mut List,
    _constraintDeps: *mut *mut List,
) -> bool {
    false
}
// TODO(pg-port): catalog/pg_proc.c get_func_signature
unsafe fn get_func_signature(
    _funcid: Oid,
    _argtypes: *mut *mut Oid,
    _nargs: *mut c_int,
) -> Oid {
    InvalidOid
}
// TODO(pg-port): parser/parse_coerce.c enforce_generic_type_consistency
unsafe fn enforce_generic_type_consistency(
    _actual_arg_types: *mut Oid,
    _declared_arg_types: *mut Oid,
    _nargs: c_int,
    _rettype: Oid,
    _allow_poly: bool,
) -> Oid {
    InvalidOid
}
// TODO(pg-port): catalog/pg_type.h IsPolymorphicType macro
fn IsPolymorphicType(typid: Oid) -> bool {
    false
}
// TODO(pg-port): utils/lsyscache.c SearchSysCache1
unsafe fn SearchSysCache1(_cacheid: c_int, _key: Datum) -> *mut c_void {
    core::ptr::null_mut()
}
// TODO(pg-port): utils/cache/catcache.c ReleaseSysCache
unsafe fn ReleaseSysCache(_tuple: *mut c_void) {}
// TODO(pg-port): access/htup_details.h HeapTupleIsValid
unsafe fn HeapTupleIsValid(tuple: *mut c_void) -> bool {
    !tuple.is_null()
}
// TODO(pg-port): access/htup_details.h GETSTRUCT
unsafe fn GETSTRUCT(tup: *mut c_void) -> *mut c_void {
    core::ptr::null_mut()
}
// TODO(pg-port): catalog/pg_type.h Form_pg_type
struct FormData_pg_type {
    typbyval: bool,
    typsend: Oid,
    typreceive: Oid,
}
type Form_pg_type = *mut FormData_pg_type;

const TYPEOID: c_int = 0; // syscache id stub

// ObjectIdGetDatum stub
unsafe fn ObjectIdGetDatum(oid: Oid) -> Datum {
    oid as Datum
}

// ParseExprKindName -- already in parse_node; re-export as local for this file
unsafe fn ParseExprKindName(kind: ParseExprKind) -> *const c_char {
    b"?\0".as_ptr() as *const c_char
}

// ---------------------------------------------------------------------------
// errmsg / ereport helpers (local shims matching the crate pattern)
// ---------------------------------------------------------------------------
// ereport!(level, errmsg!("...")) is provided by crate prelude.

/*
 * transformAggregateCall -
 *		Finish initial transformation of an aggregate call
 *
 * parse_func.c has recognized the function as an aggregate, and has set up
 * all the fields of the Aggref except aggargtypes, aggdirectargs, args,
 * aggorder, aggdistinct and agglevelsup.  The passed-in args list has been
 * through standard expression transformation and type coercion to match the
 * agg's declared arg types, while the passed-in aggorder list hasn't been
 * transformed at all.
 */
pub unsafe fn transformAggregateCall(
    pstate: *mut ParseState,
    agg: *mut Aggref,
    args: *mut List,
    aggorder: *mut List,
    agg_distinct: bool,
) {
    let mut argtypes: *mut List = NIL;
    let mut tlist: *mut List = NIL;
    let mut torder: *mut List = NIL;
    let mut tdistinct: *mut List = NIL;
    let mut attno: i16 = 1;
    let save_next_resno: c_int;
    let mut lc: *mut ListCell;

    if AGGKIND_IS_ORDERED_SET((*agg).aggkind) {
        /*
         * For an ordered-set agg, the args list includes direct args and
         * aggregated args; we must split them apart.
         */
        let numDirectArgs: c_int = list_length(args) - list_length(aggorder);
        let mut aargs: *mut List;
        let mut lc2: *mut ListCell;

        assert!(numDirectArgs >= 0);

        aargs = list_copy_tail(args, numDirectArgs);
        (*agg).aggdirectargs = list_truncate(args, numDirectArgs);

        /*
         * Build a tlist from the aggregated args, and make a sortlist entry
         * for each one.  Note that the expressions in the SortBy nodes are
         * ignored (they are the raw versions of the transformed args); we are
         * just looking at the sort information in the SortBy nodes.
         */
        // forboth(lc, aargs, lc2, aggorder)
        {
            let mut _lc = list_head(aargs);
            let mut _lc2 = list_head(aggorder);
            while !_lc.is_null() && !_lc2.is_null() {
                let arg: *mut Expr = lfirst(_lc) as *mut Expr;
                let sortby: *mut SortBy = lfirst(_lc2) as *mut SortBy;
                let tle: *mut TargetEntry;

                /* We don't bother to assign column names to the entries */
                tle = makeTargetEntry(arg, attno, core::ptr::null_mut(), false);
                attno += 1;
                tlist = lappend(tlist, tle as *mut c_void);

                torder = addTargetToSortList(pstate, tle, torder, tlist, sortby);

                _lc = lnext(aargs, _lc);
                _lc2 = lnext(aggorder, _lc2);
            }
        }

        /* Never any DISTINCT in an ordered-set agg */
        assert!(!agg_distinct);
    } else {
        /* Regular aggregate, so it has no direct args */
        (*agg).aggdirectargs = NIL;

        /*
         * Transform the plain list of Exprs into a targetlist.
         */
        foreach!(lc, args, {
            let arg: *mut Expr = lfirst(current_cell!(lc)) as *mut Expr;
            let tle: *mut TargetEntry;

            /* We don't bother to assign column names to the entries */
            tle = makeTargetEntry(arg, attno, core::ptr::null_mut(), false);
            attno += 1;
            tlist = lappend(tlist, tle as *mut c_void);
        });

        /*
         * If we have an ORDER BY, transform it.  This will add columns to the
         * tlist if they appear in ORDER BY but weren't already in the arg
         * list.  They will be marked resjunk = true so we can tell them apart
         * from regular aggregate arguments later.
         *
         * We need to mess with p_next_resno since it will be used to number
         * any new targetlist entries.
         */
        save_next_resno = (*pstate).p_next_resno;
        (*pstate).p_next_resno = attno as c_int;

        torder = transformSortClause(
            pstate,
            aggorder,
            &mut tlist,
            EXPR_KIND_ORDER_BY,
            true, /* force SQL99 rules */
        );

        /*
         * If we have DISTINCT, transform that to produce a distinctList.
         */
        if agg_distinct {
            tdistinct = transformDistinctClause(pstate, &mut tlist, torder, true);

            /*
             * Remove this check if executor support for hashed distinct for
             * aggregates is ever added.
             */
            foreach!(lc, tdistinct, {
                let sortcl: *mut SortGroupClause = lfirst(current_cell!(lc)) as *mut SortGroupClause;

                if !OidIsValid((*sortcl).sortop) {
                    let expr: *mut Node = get_sortgroupclause_expr(sortcl, tlist);

                    ereport!(ERROR,
                        errmsg!("could not identify an ordering operator for type {}",
                            std::ffi::CStr::from_ptr(format_type_be(exprType(expr))).to_string_lossy())
                        /* C also: errcode(ERRCODE_UNDEFINED_FUNCTION),
                           errdetail("Aggregates with DISTINCT must be able to sort their inputs."),
                           parser_errposition(pstate, exprLocation(expr)) */
                    );
                }
            });
        }

        (*pstate).p_next_resno = save_next_resno;
    }

    /* Update the Aggref with the transformation results */
    (*agg).args = tlist;
    (*agg).aggorder = torder;
    (*agg).aggdistinct = tdistinct;

    /*
     * Now build the aggargtypes list with the type OIDs of the direct and
     * aggregated args, ignoring any resjunk entries that might have been
     * added by ORDER BY/DISTINCT processing.
     */
    foreach!(lc, (*agg).aggdirectargs, {
        let arg: *mut Expr = lfirst(current_cell!(lc)) as *mut Expr;
        argtypes = lappend_oid(argtypes, exprType(arg as *mut Node));
    });
    foreach!(lc, tlist, {
        let tle: *mut TargetEntry = lfirst(current_cell!(lc)) as *mut TargetEntry;

        if (*tle).resjunk {
            continue; /* ignore junk */
        }
        argtypes = lappend_oid(argtypes, exprType((*tle).expr as *mut Node));
    });
    (*agg).aggargtypes = argtypes;

    check_agglevels_and_constraints(pstate, agg as *mut Node);
}

/*
 * transformGroupingFunc
 *		Transform a GROUPING expression
 */
pub unsafe fn transformGroupingFunc(
    pstate: *mut ParseState,
    p: *mut GroupingFunc,
) -> *mut Node {
    let mut lc: *mut ListCell;
    let args: *mut List = (*p).args;
    let mut result_list: *mut List = NIL;
    let result: *mut GroupingFunc = makeNode!(GroupingFunc, T_GroupingFunc);

    if list_length(args) > 31 {
        ereport!(ERROR,
            errmsg!("GROUPING must have fewer than 32 arguments")
            /* C also: errcode(ERRCODE_TOO_MANY_ARGUMENTS),
               parser_errposition(pstate, (*p).location) */
        );
    }

    foreach!(lc, args, {
        let current_result: *mut Node;

        current_result = transformExpr(pstate, lfirst(current_cell!(lc)) as *mut Node, (*pstate).p_expr_kind);

        /* acceptability of expressions is checked later */

        result_list = lappend(result_list, current_result as *mut c_void);
    });

    (*result).args = result_list;
    (*result).location = (*p).location;

    check_agglevels_and_constraints(pstate, result as *mut Node);

    result as *mut Node
}

/*
 * Aggregate functions and grouping operations are very similar with regard
 * to level and nesting restrictions.  Centralise those restrictions here.
 */
unsafe fn check_agglevels_and_constraints(pstate: *mut ParseState, expr: *mut Node) {
    let mut directargs: *mut List = NIL;
    let mut args: *mut List = NIL;
    let mut filter: *mut Expr = core::ptr::null_mut();
    let min_varlevel: c_int;
    let mut location: c_int = -1;
    let p_levelsup: *mut Index;
    let mut err: *const c_char = core::ptr::null();
    let mut errkind: bool = false;
    let isAgg: bool = IsA!(expr, T_Aggref);

    if isAgg {
        let agg: *mut Aggref = expr as *mut Aggref;

        directargs = (*agg).aggdirectargs;
        args = (*agg).args;
        filter = (*agg).aggfilter;
        location = (*agg).location;
        p_levelsup = &mut (*agg).agglevelsup as *mut Index;
    } else {
        let grp: *mut GroupingFunc = expr as *mut GroupingFunc;

        args = (*grp).args;
        location = (*grp).location;
        p_levelsup = &mut (*grp).agglevelsup as *mut Index;
    }

    /*
     * Check the arguments to compute the aggregate's level and detect
     * improper nesting.
     */
    min_varlevel = check_agg_arguments(pstate, directargs, args, filter, location);

    *p_levelsup = min_varlevel as Index;

    /* Mark the correct pstate level as having aggregates */
    let mut cur_pstate = pstate;
    let mut levels_down = min_varlevel;
    while levels_down > 0 {
        cur_pstate = (*cur_pstate).parentParseState;
        levels_down -= 1;
    }
    (*cur_pstate).p_hasAggs = true;

    /*
     * Check to see if the aggregate function is in an invalid place within
     * its aggregation query.
     */
    err = core::ptr::null();
    errkind = false;

    match (*pstate).p_expr_kind {
        EXPR_KIND_NONE => {
            assert!(false); /* can't happen */
        }
        EXPR_KIND_OTHER => {
            /*
             * Accept aggregate/grouping here; caller must throw error if
             * wanted
             */
        }
        EXPR_KIND_JOIN_ON | EXPR_KIND_JOIN_USING => {
            if isAgg {
                err = b"aggregate functions are not allowed in JOIN conditions\0".as_ptr() as *const c_char;
            } else {
                err = b"grouping operations are not allowed in JOIN conditions\0".as_ptr() as *const c_char;
            }
        }
        EXPR_KIND_FROM_SUBSELECT => {
            /*
             * Aggregate/grouping scope rules make it worth being explicit here
             */
            if isAgg {
                err = b"aggregate functions are not allowed in FROM clause of their own query level\0".as_ptr() as *const c_char;
            } else {
                err = b"grouping operations are not allowed in FROM clause of their own query level\0".as_ptr() as *const c_char;
            }
        }
        EXPR_KIND_FROM_FUNCTION => {
            if isAgg {
                err = b"aggregate functions are not allowed in functions in FROM\0".as_ptr() as *const c_char;
            } else {
                err = b"grouping operations are not allowed in functions in FROM\0".as_ptr() as *const c_char;
            }
        }
        EXPR_KIND_WHERE => { errkind = true; }
        EXPR_KIND_POLICY => {
            if isAgg {
                err = b"aggregate functions are not allowed in policy expressions\0".as_ptr() as *const c_char;
            } else {
                err = b"grouping operations are not allowed in policy expressions\0".as_ptr() as *const c_char;
            }
        }
        EXPR_KIND_HAVING => { /* okay */ }
        EXPR_KIND_FILTER => { errkind = true; }
        EXPR_KIND_WINDOW_PARTITION => { /* okay */ }
        EXPR_KIND_WINDOW_ORDER => { /* okay */ }
        EXPR_KIND_WINDOW_FRAME_RANGE => {
            if isAgg {
                err = b"aggregate functions are not allowed in window RANGE\0".as_ptr() as *const c_char;
            } else {
                err = b"grouping operations are not allowed in window RANGE\0".as_ptr() as *const c_char;
            }
        }
        EXPR_KIND_WINDOW_FRAME_ROWS => {
            if isAgg {
                err = b"aggregate functions are not allowed in window ROWS\0".as_ptr() as *const c_char;
            } else {
                err = b"grouping operations are not allowed in window ROWS\0".as_ptr() as *const c_char;
            }
        }
        EXPR_KIND_WINDOW_FRAME_GROUPS => {
            if isAgg {
                err = b"aggregate functions are not allowed in window GROUPS\0".as_ptr() as *const c_char;
            } else {
                err = b"grouping operations are not allowed in window GROUPS\0".as_ptr() as *const c_char;
            }
        }
        EXPR_KIND_SELECT_TARGET => { /* okay */ }
        EXPR_KIND_INSERT_TARGET | EXPR_KIND_UPDATE_SOURCE | EXPR_KIND_UPDATE_TARGET => {
            errkind = true;
        }
        EXPR_KIND_MERGE_WHEN => {
            if isAgg {
                err = b"aggregate functions are not allowed in MERGE WHEN conditions\0".as_ptr() as *const c_char;
            } else {
                err = b"grouping operations are not allowed in MERGE WHEN conditions\0".as_ptr() as *const c_char;
            }
        }
        EXPR_KIND_GROUP_BY => { errkind = true; }
        EXPR_KIND_ORDER_BY => { /* okay */ }
        EXPR_KIND_DISTINCT_ON => { /* okay */ }
        EXPR_KIND_LIMIT | EXPR_KIND_OFFSET => { errkind = true; }
        EXPR_KIND_RETURNING | EXPR_KIND_MERGE_RETURNING => { errkind = true; }
        EXPR_KIND_VALUES | EXPR_KIND_VALUES_SINGLE => { errkind = true; }
        EXPR_KIND_CHECK_CONSTRAINT | EXPR_KIND_DOMAIN_CHECK => {
            if isAgg {
                err = b"aggregate functions are not allowed in check constraints\0".as_ptr() as *const c_char;
            } else {
                err = b"grouping operations are not allowed in check constraints\0".as_ptr() as *const c_char;
            }
        }
        EXPR_KIND_COLUMN_DEFAULT | EXPR_KIND_FUNCTION_DEFAULT => {
            if isAgg {
                err = b"aggregate functions are not allowed in DEFAULT expressions\0".as_ptr() as *const c_char;
            } else {
                err = b"grouping operations are not allowed in DEFAULT expressions\0".as_ptr() as *const c_char;
            }
        }
        EXPR_KIND_INDEX_EXPRESSION => {
            if isAgg {
                err = b"aggregate functions are not allowed in index expressions\0".as_ptr() as *const c_char;
            } else {
                err = b"grouping operations are not allowed in index expressions\0".as_ptr() as *const c_char;
            }
        }
        EXPR_KIND_INDEX_PREDICATE => {
            if isAgg {
                err = b"aggregate functions are not allowed in index predicates\0".as_ptr() as *const c_char;
            } else {
                err = b"grouping operations are not allowed in index predicates\0".as_ptr() as *const c_char;
            }
        }
        EXPR_KIND_STATS_EXPRESSION => {
            if isAgg {
                err = b"aggregate functions are not allowed in statistics expressions\0".as_ptr() as *const c_char;
            } else {
                err = b"grouping operations are not allowed in statistics expressions\0".as_ptr() as *const c_char;
            }
        }
        EXPR_KIND_ALTER_COL_TRANSFORM => {
            if isAgg {
                err = b"aggregate functions are not allowed in transform expressions\0".as_ptr() as *const c_char;
            } else {
                err = b"grouping operations are not allowed in transform expressions\0".as_ptr() as *const c_char;
            }
        }
        EXPR_KIND_EXECUTE_PARAMETER => {
            if isAgg {
                err = b"aggregate functions are not allowed in EXECUTE parameters\0".as_ptr() as *const c_char;
            } else {
                err = b"grouping operations are not allowed in EXECUTE parameters\0".as_ptr() as *const c_char;
            }
        }
        EXPR_KIND_TRIGGER_WHEN => {
            if isAgg {
                err = b"aggregate functions are not allowed in trigger WHEN conditions\0".as_ptr() as *const c_char;
            } else {
                err = b"grouping operations are not allowed in trigger WHEN conditions\0".as_ptr() as *const c_char;
            }
        }
        EXPR_KIND_PARTITION_BOUND => {
            if isAgg {
                err = b"aggregate functions are not allowed in partition bound\0".as_ptr() as *const c_char;
            } else {
                err = b"grouping operations are not allowed in partition bound\0".as_ptr() as *const c_char;
            }
        }
        EXPR_KIND_PARTITION_EXPRESSION => {
            if isAgg {
                err = b"aggregate functions are not allowed in partition key expressions\0".as_ptr() as *const c_char;
            } else {
                err = b"grouping operations are not allowed in partition key expressions\0".as_ptr() as *const c_char;
            }
        }
        EXPR_KIND_GENERATED_COLUMN => {
            if isAgg {
                err = b"aggregate functions are not allowed in column generation expressions\0".as_ptr() as *const c_char;
            } else {
                err = b"grouping operations are not allowed in column generation expressions\0".as_ptr() as *const c_char;
            }
        }
        EXPR_KIND_CALL_ARGUMENT => {
            if isAgg {
                err = b"aggregate functions are not allowed in CALL arguments\0".as_ptr() as *const c_char;
            } else {
                err = b"grouping operations are not allowed in CALL arguments\0".as_ptr() as *const c_char;
            }
        }
        EXPR_KIND_COPY_WHERE => {
            if isAgg {
                err = b"aggregate functions are not allowed in COPY FROM WHERE conditions\0".as_ptr() as *const c_char;
            } else {
                err = b"grouping operations are not allowed in COPY FROM WHERE conditions\0".as_ptr() as *const c_char;
            }
        }
        EXPR_KIND_CYCLE_MARK => { errkind = true; }
        /*
         * There is intentionally no default case here, so that the compiler
         * will warn if we add a new ParseExprKind without extending this switch.
         */
        _ => { /* treat as EXPR_KIND_OTHER */ }
    }

    if !err.is_null() {
        ereport!(ERROR,
            errmsg!("{}", std::ffi::CStr::from_ptr(err).to_string_lossy())
            /* C also: errcode(ERRCODE_GROUPING_ERROR),
               parser_errposition(pstate, location) */
        );
    }

    if errkind {
        if isAgg {
            /* translator: %s is name of a SQL construct, eg GROUP BY */
            ereport!(ERROR,
                errmsg!("aggregate functions are not allowed in {}",
                    std::ffi::CStr::from_ptr(ParseExprKindName((*pstate).p_expr_kind)).to_string_lossy())
                /* C also: errcode(ERRCODE_GROUPING_ERROR),
                   parser_errposition(pstate, location) */
            );
        } else {
            /* translator: %s is name of a SQL construct, eg GROUP BY */
            ereport!(ERROR,
                errmsg!("grouping operations are not allowed in {}",
                    std::ffi::CStr::from_ptr(ParseExprKindName((*pstate).p_expr_kind)).to_string_lossy())
                /* C also: errcode(ERRCODE_GROUPING_ERROR),
                   parser_errposition(pstate, location) */
            );
        }
    }
}

/*
 * check_agg_arguments
 *	  Scan the arguments of an aggregate function to determine the
 *	  aggregate's semantic level.
 */
unsafe fn check_agg_arguments(
    pstate: *mut ParseState,
    directargs: *mut List,
    args: *mut List,
    filter: *mut Expr,
    agglocation: c_int,
) -> c_int {
    let agglevel: c_int;
    let mut context = CheckAggArgumentsContext {
        pstate,
        min_varlevel: -1, /* signifies nothing found yet */
        min_agglevel: -1,
        min_ctelevel: -1,
        min_cte: core::ptr::null_mut(),
        sublevels_up: 0,
    };

    let _ = check_agg_arguments_walker(args as *mut Node, &mut context as *mut CheckAggArgumentsContext as *mut c_void);
    let _ = check_agg_arguments_walker(filter as *mut Node, &mut context as *mut CheckAggArgumentsContext as *mut c_void);

    /*
     * If we found no vars nor aggs at all, it's a level-zero aggregate;
     * otherwise, its level is the minimum of vars or aggs.
     */
    if context.min_varlevel < 0 {
        if context.min_agglevel < 0 {
            agglevel = 0;
        } else {
            agglevel = context.min_agglevel;
        }
    } else if context.min_agglevel < 0 {
        agglevel = context.min_varlevel;
    } else {
        agglevel = std::cmp::min(context.min_varlevel, context.min_agglevel);
    }

    /*
     * If there's a nested aggregate of the same semantic level, complain.
     */
    if agglevel == context.min_agglevel {
        let mut aggloc: c_int;

        aggloc = locate_agg_of_level(args as *mut Node, agglevel);
        if aggloc < 0 {
            aggloc = locate_agg_of_level(filter as *mut Node, agglevel);
        }
        ereport!(ERROR,
            errmsg!("aggregate function calls cannot be nested")
            /* C also: errcode(ERRCODE_GROUPING_ERROR),
               parser_errposition(pstate, aggloc) */
        );
    }

    /*
     * If there's a non-local CTE that's below the aggregate's semantic level,
     * complain.
     */
    if context.min_ctelevel >= 0 && context.min_ctelevel < agglevel {
        let aliasname = if !context.min_cte.is_null() {
            let eref = (*context.min_cte).eref;
            if !eref.is_null() {
                std::ffi::CStr::from_ptr((*eref).aliasname).to_string_lossy().into_owned()
            } else {
                String::from("?")
            }
        } else {
            String::from("?")
        };
        ereport!(ERROR,
            errmsg!("outer-level aggregate cannot use a nested CTE")
            /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
               errdetail("CTE \"{}\" is below the aggregate's semantic level.", aliasname),
               parser_errposition(pstate, agglocation) */
        );
    }

    /*
     * Now check for vars/aggs in the direct arguments, and throw error if
     * needed.
     */
    if !directargs.is_null() {
        context.min_varlevel = -1;
        context.min_agglevel = -1;
        context.min_ctelevel = -1;
        let _ = check_agg_arguments_walker(
            directargs as *mut Node,
            &mut context as *mut CheckAggArgumentsContext as *mut c_void,
        );
        if context.min_varlevel >= 0 && context.min_varlevel < agglevel {
            ereport!(ERROR,
                errmsg!("outer-level aggregate cannot contain a lower-level variable in its direct arguments")
                /* C also: errcode(ERRCODE_GROUPING_ERROR),
                   parser_errposition(pstate, locate_var_of_level(directargs as *mut Node, context.min_varlevel)) */
            );
        }
        if context.min_agglevel >= 0 && context.min_agglevel <= agglevel {
            ereport!(ERROR,
                errmsg!("aggregate function calls cannot be nested")
                /* C also: errcode(ERRCODE_GROUPING_ERROR),
                   parser_errposition(pstate, locate_agg_of_level(directargs as *mut Node, context.min_agglevel)) */
            );
        }
        if context.min_ctelevel >= 0 && context.min_ctelevel < agglevel {
            ereport!(ERROR,
                errmsg!("outer-level aggregate cannot use a nested CTE")
                /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                   parser_errposition(pstate, agglocation) */
            );
        }
    }
    agglevel
}

unsafe fn check_agg_arguments_walker(
    node: *mut Node,
    context_ptr: *mut c_void,
) -> bool {
    let context: *mut CheckAggArgumentsContext = context_ptr as *mut CheckAggArgumentsContext;

    if node.is_null() {
        return false;
    }
    if IsA!(node, T_Var) {
        let varlevelsup = (*(node as *mut Var)).varlevelsup as c_int;

        /* convert levelsup to frame of reference of original query */
        let varlevelsup = varlevelsup - (*context).sublevels_up;
        /* ignore local vars of subqueries */
        if varlevelsup >= 0 {
            if (*context).min_varlevel < 0 || (*context).min_varlevel > varlevelsup {
                (*context).min_varlevel = varlevelsup;
            }
        }
        return false;
    }
    if IsA!(node, T_Aggref) {
        let agglevelsup = (*(node as *mut Aggref)).agglevelsup as c_int;

        /* convert levelsup to frame of reference of original query */
        let agglevelsup = agglevelsup - (*context).sublevels_up;
        /* ignore local aggs of subqueries */
        if agglevelsup >= 0 {
            if (*context).min_agglevel < 0 || (*context).min_agglevel > agglevelsup {
                (*context).min_agglevel = agglevelsup;
            }
        }
        /* Continue and descend into subtree */
    }
    if IsA!(node, T_GroupingFunc) {
        let agglevelsup = (*(node as *mut GroupingFunc)).agglevelsup as c_int;

        /* convert levelsup to frame of reference of original query */
        let agglevelsup = agglevelsup - (*context).sublevels_up;
        /* ignore local aggs of subqueries */
        if agglevelsup >= 0 {
            if (*context).min_agglevel < 0 || (*context).min_agglevel > agglevelsup {
                (*context).min_agglevel = agglevelsup;
            }
        }
        /* Continue and descend into subtree */
    }

    /*
     * SRFs and window functions can be rejected immediately, unless we are
     * within a sub-select within the aggregate's arguments; in that case
     * they're OK.
     */
    if (*context).sublevels_up == 0 {
        if (IsA!(node, T_FuncExpr) && (*(node as *mut FuncExpr)).funcretset)
            || (IsA!(node, T_OpExpr) && (*(node as *mut OpExpr)).opretset)
        {
            ereport!(ERROR,
                errmsg!("aggregate function calls cannot contain set-returning function calls")
                /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                   errhint("You might be able to move the set-returning function into a LATERAL FROM item."),
                   parser_errposition((*context).pstate, exprLocation(node)) */
            );
        }
        if IsA!(node, T_WindowFunc) {
            ereport!(ERROR,
                errmsg!("aggregate function calls cannot contain window function calls")
                /* C also: errcode(ERRCODE_GROUPING_ERROR),
                   parser_errposition((*context).pstate, (*(node as *mut WindowFunc)).location) */
            );
        }
    }

    if IsA!(node, T_RangeTblEntry) {
        let rte: *mut RangeTblEntry = node as *mut RangeTblEntry;

        if (*rte).rtekind == RTE_CTE {
            let ctelevelsup = (*rte).ctelevelsup as c_int;

            /* convert levelsup to frame of reference of original query */
            let ctelevelsup = ctelevelsup - (*context).sublevels_up;
            /* ignore local CTEs of subqueries */
            if ctelevelsup >= 0 {
                if (*context).min_ctelevel < 0 || (*context).min_ctelevel > ctelevelsup {
                    (*context).min_ctelevel = ctelevelsup;
                    (*context).min_cte = rte;
                }
            }
        }
        return false; /* allow range_table_walker to continue */
    }
    if IsA!(node, T_Query) {
        /* Recurse into subselects */
        let result: bool;

        (*context).sublevels_up += 1;
        result = query_tree_walker(
            node as *mut Query,
            Some(check_agg_arguments_walker),
            context_ptr,
            QTW_EXAMINE_RTES_BEFORE,
        );
        (*context).sublevels_up -= 1;
        return result;
    }

    expression_tree_walker(node, Some(check_agg_arguments_walker), context_ptr)
}

/*
 * transformWindowFuncCall -
 *		Finish initial transformation of a window function call
 */
pub unsafe fn transformWindowFuncCall(
    pstate: *mut ParseState,
    wfunc: *mut WindowFunc,
    windef: *mut WindowDef,
) {
    let mut err: *const c_char = core::ptr::null();
    let mut errkind: bool = false;

    /*
     * A window function call can't contain another one (but aggs are OK).
     */
    if (*pstate).p_hasWindowFuncs && contain_windowfuncs((*wfunc).args as *mut Node) {
        ereport!(ERROR,
            errmsg!("window function calls cannot be nested")
            /* C also: errcode(ERRCODE_WINDOWING_ERROR),
               parser_errposition(pstate, locate_windowfunc((*wfunc).args as *mut Node)) */
        );
    }

    /*
     * Check to see if the window function is in an invalid place within the
     * query.
     */
    err = core::ptr::null();
    errkind = false;
    match (*pstate).p_expr_kind {
        EXPR_KIND_NONE => {
            assert!(false); /* can't happen */
        }
        EXPR_KIND_OTHER => { /* Accept window func here; caller must throw error if wanted */ }
        EXPR_KIND_JOIN_ON | EXPR_KIND_JOIN_USING => {
            err = b"window functions are not allowed in JOIN conditions\0".as_ptr() as *const c_char;
        }
        EXPR_KIND_FROM_SUBSELECT => {
            /* can't get here, but just in case, throw an error */
            errkind = true;
        }
        EXPR_KIND_FROM_FUNCTION => {
            err = b"window functions are not allowed in functions in FROM\0".as_ptr() as *const c_char;
        }
        EXPR_KIND_WHERE => { errkind = true; }
        EXPR_KIND_POLICY => {
            err = b"window functions are not allowed in policy expressions\0".as_ptr() as *const c_char;
        }
        EXPR_KIND_HAVING => { errkind = true; }
        EXPR_KIND_FILTER => { errkind = true; }
        EXPR_KIND_WINDOW_PARTITION
        | EXPR_KIND_WINDOW_ORDER
        | EXPR_KIND_WINDOW_FRAME_RANGE
        | EXPR_KIND_WINDOW_FRAME_ROWS
        | EXPR_KIND_WINDOW_FRAME_GROUPS => {
            err = b"window functions are not allowed in window definitions\0".as_ptr() as *const c_char;
        }
        EXPR_KIND_SELECT_TARGET => { /* okay */ }
        EXPR_KIND_INSERT_TARGET | EXPR_KIND_UPDATE_SOURCE | EXPR_KIND_UPDATE_TARGET => {
            errkind = true;
        }
        EXPR_KIND_MERGE_WHEN => {
            err = b"window functions are not allowed in MERGE WHEN conditions\0".as_ptr() as *const c_char;
        }
        EXPR_KIND_GROUP_BY => { errkind = true; }
        EXPR_KIND_ORDER_BY => { /* okay */ }
        EXPR_KIND_DISTINCT_ON => { /* okay */ }
        EXPR_KIND_LIMIT | EXPR_KIND_OFFSET => { errkind = true; }
        EXPR_KIND_RETURNING | EXPR_KIND_MERGE_RETURNING => { errkind = true; }
        EXPR_KIND_VALUES | EXPR_KIND_VALUES_SINGLE => { errkind = true; }
        EXPR_KIND_CHECK_CONSTRAINT | EXPR_KIND_DOMAIN_CHECK => {
            err = b"window functions are not allowed in check constraints\0".as_ptr() as *const c_char;
        }
        EXPR_KIND_COLUMN_DEFAULT | EXPR_KIND_FUNCTION_DEFAULT => {
            err = b"window functions are not allowed in DEFAULT expressions\0".as_ptr() as *const c_char;
        }
        EXPR_KIND_INDEX_EXPRESSION => {
            err = b"window functions are not allowed in index expressions\0".as_ptr() as *const c_char;
        }
        EXPR_KIND_STATS_EXPRESSION => {
            err = b"window functions are not allowed in statistics expressions\0".as_ptr() as *const c_char;
        }
        EXPR_KIND_INDEX_PREDICATE => {
            err = b"window functions are not allowed in index predicates\0".as_ptr() as *const c_char;
        }
        EXPR_KIND_ALTER_COL_TRANSFORM => {
            err = b"window functions are not allowed in transform expressions\0".as_ptr() as *const c_char;
        }
        EXPR_KIND_EXECUTE_PARAMETER => {
            err = b"window functions are not allowed in EXECUTE parameters\0".as_ptr() as *const c_char;
        }
        EXPR_KIND_TRIGGER_WHEN => {
            err = b"window functions are not allowed in trigger WHEN conditions\0".as_ptr() as *const c_char;
        }
        EXPR_KIND_PARTITION_BOUND => {
            err = b"window functions are not allowed in partition bound\0".as_ptr() as *const c_char;
        }
        EXPR_KIND_PARTITION_EXPRESSION => {
            err = b"window functions are not allowed in partition key expressions\0".as_ptr() as *const c_char;
        }
        EXPR_KIND_CALL_ARGUMENT => {
            err = b"window functions are not allowed in CALL arguments\0".as_ptr() as *const c_char;
        }
        EXPR_KIND_COPY_WHERE => {
            err = b"window functions are not allowed in COPY FROM WHERE conditions\0".as_ptr() as *const c_char;
        }
        EXPR_KIND_GENERATED_COLUMN => {
            err = b"window functions are not allowed in column generation expressions\0".as_ptr() as *const c_char;
        }
        EXPR_KIND_CYCLE_MARK => { errkind = true; }
        /*
         * There is intentionally no default case here.
         */
        _ => { /* treat as EXPR_KIND_OTHER */ }
    }

    if !err.is_null() {
        ereport!(ERROR,
            errmsg!("{}", std::ffi::CStr::from_ptr(err).to_string_lossy())
            /* C also: errcode(ERRCODE_WINDOWING_ERROR),
               parser_errposition(pstate, (*wfunc).location) */
        );
    }
    if errkind {
        ereport!(ERROR,
            errmsg!("window functions are not allowed in {}",
                std::ffi::CStr::from_ptr(ParseExprKindName((*pstate).p_expr_kind)).to_string_lossy())
            /* C also: errcode(ERRCODE_WINDOWING_ERROR),
               parser_errposition(pstate, (*wfunc).location) */
        );
    }

    /*
     * If the OVER clause just specifies a window name, find that WINDOW
     * clause (which had better be present).  Otherwise, try to match all the
     * properties of the OVER clause, and make a new entry in the p_windowdefs
     * list if no luck.
     */
    if !(*windef).name.is_null() {
        let mut winref: Index = 0;

        assert!(
            (*windef).refname.is_null()
                && (*windef).partitionClause.is_null()
                && (*windef).orderClause.is_null()
                && (*windef).frameOptions == FRAMEOPTION_DEFAULTS
        );

        {
            let mut lc: *mut ListCell;
            foreach!(lc, (*pstate).p_windowdefs, {
                let refwin: *mut WindowDef = lfirst(current_cell!(lc)) as *mut WindowDef;

                winref += 1;
                if !(*refwin).name.is_null()
                    && libc_strcmp((*refwin).name, (*windef).name) == 0
                {
                    (*wfunc).winref = winref;
                    break;
                }
            });
        }
        if (*wfunc).winref == 0 {
            /* didn't find it? */
            ereport!(ERROR,
                errmsg!("window \"{}\" does not exist",
                    std::ffi::CStr::from_ptr((*windef).name).to_string_lossy())
                /* C also: errcode(ERRCODE_UNDEFINED_OBJECT),
                   parser_errposition(pstate, (*windef).location) */
            );
        }
    } else {
        let mut winref: Index = 0;
        let mut found_dup: bool = false;

        {
            let mut lc: *mut ListCell;
            foreach!(lc, (*pstate).p_windowdefs, {
                let refwin: *mut WindowDef = lfirst(current_cell!(lc)) as *mut WindowDef;

                winref += 1;
                if !(*refwin).refname.is_null() && !(*windef).refname.is_null() {
                    if libc_strcmp((*refwin).refname, (*windef).refname) != 0 {
                        continue;
                    }
                } else if (*refwin).refname.is_null() && (*windef).refname.is_null() {
                    /* matched, no refname */
                } else {
                    continue;
                }

                /*
                 * Also see similar de-duplication code in optimize_window_clauses
                 */
                if equal((*refwin).partitionClause as *const c_void, (*windef).partitionClause as *const c_void)
                    && equal((*refwin).orderClause as *const c_void, (*windef).orderClause as *const c_void)
                    && (*refwin).frameOptions == (*windef).frameOptions
                    && equal((*refwin).startOffset as *const c_void, (*windef).startOffset as *const c_void)
                    && equal((*refwin).endOffset as *const c_void, (*windef).endOffset as *const c_void)
                {
                    /* found a duplicate window specification */
                    (*wfunc).winref = winref;
                    found_dup = true;
                    break;
                }
            });
        }
        if !found_dup {
            /* didn't find it? */
            (*pstate).p_windowdefs = lappend((*pstate).p_windowdefs, windef as *mut c_void);
            (*wfunc).winref = list_length((*pstate).p_windowdefs) as Index;
        }
    }

    (*pstate).p_hasWindowFuncs = true;
}

// libc strcmp wrapper (avoid pulling in libc crate)
unsafe fn libc_strcmp(a: *const c_char, b: *const c_char) -> c_int {
    let sa = std::ffi::CStr::from_ptr(a);
    let sb = std::ffi::CStr::from_ptr(b);
    match sa.cmp(sb) {
        std::cmp::Ordering::Less => -1,
        std::cmp::Ordering::Equal => 0,
        std::cmp::Ordering::Greater => 1,
    }
}

// get_sortgroupclause_expr stub (real fn lives in parse_clause.rs)
// TODO(pg-port): parse_clause.c get_sortgroupclause_expr
unsafe fn get_sortgroupclause_expr(
    _sgc: *mut SortGroupClause,
    _tlist: *mut List,
) -> *mut Node {
    core::ptr::null_mut()
}

// get_sortgroupclause_tle stub
// TODO(pg-port): parse_clause.c get_sortgroupclause_tle
unsafe fn get_sortgroupclause_tle(
    _sgc: *mut SortGroupClause,
    _tlist: *mut List,
) -> *mut TargetEntry {
    core::ptr::null_mut()
}

/*
 * parseCheckAggregates
 *	Check for aggregates where they shouldn't be and improper grouping, and
 *	replace grouped variables in the targetlist and HAVING clause with Vars
 *	that reference the RTE_GROUP RTE.
 */
pub unsafe fn parseCheckAggregates(pstate: *mut ParseState, qry: *mut Query) {
    let mut gset_common: *mut List = NIL;
    let mut groupClauses: *mut List = NIL;
    let mut groupClauseCommonVars: *mut List = NIL;
    let mut have_non_var_grouping: bool;
    let mut func_grouped_rels: *mut List = NIL;
    let mut l: *mut ListCell;
    let mut hasJoinRTEs: bool;
    let mut hasSelfRefRTEs: bool;
    let mut clause: *mut Node;

    /* This should only be called if we found aggregates or grouping */
    assert!(
        (*pstate).p_hasAggs
            || !(*qry).groupClause.is_null()
            || !(*qry).havingQual.is_null()
            || !(*qry).groupingSets.is_null()
    );

    /*
     * If we have grouping sets, expand them and find the intersection of all
     * sets.
     */
    if !(*qry).groupingSets.is_null() {
        /*
         * The limit of 4096 is arbitrary and exists simply to avoid resource
         * issues from pathological constructs.
         */
        let gsets: *mut List = expand_grouping_sets((*qry).groupingSets, (*qry).groupDistinct, 4096);

        if gsets.is_null() {
            ereport!(ERROR,
                errmsg!("too many grouping sets present (maximum 4096)")
                /* C also: errcode(ERRCODE_STATEMENT_TOO_COMPLEX),
                   parser_errposition(pstate, ...) */
            );
        }

        /*
         * The intersection will often be empty, so help things along by
         * seeding the intersect with the smallest set.
         */
        gset_common = linitial(gsets) as *mut List;

        if !gset_common.is_null() {
            // for_each_from(l, gsets, 1)
            let mut _idx: c_int = 0;
            let mut _lc = list_head(gsets);
            while !_lc.is_null() {
                if _idx >= 1 {
                    gset_common = list_intersection_int(gset_common, lfirst(_lc) as *mut List);
                    if gset_common.is_null() {
                        break;
                    }
                }
                _idx += 1;
                _lc = lnext(gsets, _lc);
            }
        }

        /*
         * If there was only one grouping set in the expansion, AND if the
         * groupClause is non-empty, then we can ditch the grouping set and
         * pretend we just had a normal GROUP BY.
         */
        if list_length(gsets) == 1 && !(*qry).groupClause.is_null() {
            (*qry).groupingSets = NIL;
        }
    }

    /*
     * Scan the range table to see if there are JOIN or self-reference CTE
     * entries.
     */
    hasJoinRTEs = false;
    hasSelfRefRTEs = false;
    foreach!(l, (*pstate).p_rtable, {
        let rte: *mut RangeTblEntry = lfirst(current_cell!(l)) as *mut RangeTblEntry;

        if (*rte).rtekind == RTE_JOIN {
            hasJoinRTEs = true;
        } else if (*rte).rtekind == RTE_CTE && (*rte).self_reference {
            hasSelfRefRTEs = true;
        }
    });

    /*
     * Build a list of the acceptable GROUP BY expressions to save in the
     * RTE_GROUP RTE, and for use by substitute_grouped_columns().
     *
     * We get the TLE, not just the expr, because GROUPING wants to know the
     * sortgroupref.
     */
    foreach!(l, (*qry).groupClause, {
        let grpcl: *mut SortGroupClause = lfirst(current_cell!(l)) as *mut SortGroupClause;
        let expr: *mut TargetEntry;

        expr = get_sortgroupclause_tle(grpcl, (*qry).targetList);
        if expr.is_null() {
            continue; /* probably cannot happen */
        }

        groupClauses = lappend(groupClauses, expr as *mut c_void);
    });

    /*
     * If there are any acceptable GROUP BY expressions, build an RTE and
     * nsitem for the result of the grouping step.
     */
    if !groupClauses.is_null() {
        (*pstate).p_grouping_nsitem =
            addRangeTableEntryForGroup(pstate, groupClauses);

        /* Set qry->rtable again in case it was previously NIL */
        (*qry).rtable = (*pstate).p_rtable;
        /* Mark the Query as having RTE_GROUP RTE */
        (*qry).hasGroupRTE = true;
    }

    /*
     * If there are join alias vars involved, we have to flatten them to the
     * underlying vars.
     */
    if hasJoinRTEs {
        groupClauses = flatten_join_alias_vars(
            core::ptr::null_mut(),
            qry,
            groupClauses as *mut Node,
        ) as *mut List;
    }

    /*
     * Detect whether any of the grouping expressions aren't simple Vars.
     */
    have_non_var_grouping = false;
    foreach!(l, groupClauses, {
        let tle: *mut TargetEntry = lfirst(current_cell!(l)) as *mut TargetEntry;

        if !IsA!((*tle).expr as *mut Node, T_Var) {
            have_non_var_grouping = true;
        } else if (*qry).groupingSets.is_null()
            || list_member_int(gset_common, (*tle).ressortgroupref as c_int)
        {
            groupClauseCommonVars = lappend(groupClauseCommonVars, (*tle).expr as *mut c_void);
        }
    });

    /*
     * Replace grouped variables in the targetlist and HAVING clause with Vars
     * that reference the RTE_GROUP RTE.
     */
    clause = (*qry).targetList as *mut Node;
    finalize_grouping_exprs(clause, pstate, qry, groupClauses, hasJoinRTEs, have_non_var_grouping);
    if hasJoinRTEs {
        clause = flatten_join_alias_vars(core::ptr::null_mut(), qry, clause);
    }
    (*qry).targetList = substitute_grouped_columns(
        clause, pstate, qry,
        groupClauses, groupClauseCommonVars,
        gset_common,
        have_non_var_grouping,
        &mut func_grouped_rels,
    ) as *mut List;

    clause = (*qry).havingQual as *mut Node;
    finalize_grouping_exprs(clause, pstate, qry, groupClauses, hasJoinRTEs, have_non_var_grouping);
    if hasJoinRTEs {
        clause = flatten_join_alias_vars(core::ptr::null_mut(), qry, clause);
    }
    (*qry).havingQual = substitute_grouped_columns(
        clause, pstate, qry,
        groupClauses, groupClauseCommonVars,
        gset_common,
        have_non_var_grouping,
        &mut func_grouped_rels,
    );

    /*
     * Per spec, aggregates can't appear in a recursive term.
     */
    if (*pstate).p_hasAggs && hasSelfRefRTEs {
        ereport!(ERROR,
            errmsg!("aggregate functions are not allowed in a recursive query's recursive term")
            /* C also: errcode(ERRCODE_INVALID_RECURSION),
               parser_errposition(pstate, locate_agg_of_level(qry as *mut Node, 0)) */
        );
    }
}

unsafe fn substitute_grouped_columns(
    node: *mut Node,
    pstate: *mut ParseState,
    qry: *mut Query,
    groupClauses: *mut List,
    groupClauseCommonVars: *mut List,
    gset_common: *mut List,
    have_non_var_grouping: bool,
    func_grouped_rels: *mut *mut List,
) -> *mut Node {
    let mut context = SubstituteGroupedColumnsContext {
        pstate,
        qry,
        hasJoinRTEs: false, /* assume caller flattened join Vars */
        groupClauses,
        groupClauseCommonVars,
        gset_common,
        have_non_var_grouping,
        func_grouped_rels,
        sublevels_up: 0,
        in_agg_direct_args: false,
    };
    substitute_grouped_columns_mutator(node, &mut context as *mut SubstituteGroupedColumnsContext as *mut c_void)
}

unsafe fn substitute_grouped_columns_mutator(
    node: *mut Node,
    context_ptr: *mut c_void,
) -> *mut Node {
    let context: *mut SubstituteGroupedColumnsContext =
        context_ptr as *mut SubstituteGroupedColumnsContext;
    let mut gl: *mut ListCell;

    if node.is_null() {
        return core::ptr::null_mut();
    }

    if IsA!(node, T_Aggref) {
        let mut agg: *mut Aggref = node as *mut Aggref;

        if (*agg).agglevelsup as c_int == (*context).sublevels_up {
            /*
             * If we find an aggregate call of the original level, do not
             * recurse into its normal arguments, ORDER BY arguments, or
             * filter; grouped vars there do not need to be replaced and
             * ungrouped vars there are not an error.  But we should check
             * direct arguments as though they weren't in an aggregate.
             */
            agg = copyObject(agg);

            assert!(!(*context).in_agg_direct_args);
            (*context).in_agg_direct_args = true;
            (*agg).aggdirectargs = substitute_grouped_columns_mutator(
                (*agg).aggdirectargs as *mut Node,
                context_ptr,
            ) as *mut List;
            (*context).in_agg_direct_args = false;
            return agg as *mut Node;
        }

        /*
         * We can skip recursing into aggregates of higher levels altogether.
         */
        if (*agg).agglevelsup as c_int > (*context).sublevels_up {
            return node;
        }
    }

    if IsA!(node, T_GroupingFunc) {
        let grp: *mut GroupingFunc = node as *mut GroupingFunc;

        /* handled GroupingFunc separately, no need to recheck at this level */

        if (*grp).agglevelsup as c_int >= (*context).sublevels_up {
            return node;
        }
    }

    /*
     * If we have any GROUP BY items that are not simple Vars, check to see if
     * subexpression as a whole matches any GROUP BY item.
     */
    if (*context).have_non_var_grouping && (*context).sublevels_up == 0 {
        let mut attnum: c_int = 0;

        foreach!(gl, (*context).groupClauses, {
            let tle: *mut TargetEntry = lfirst(current_cell!(gl)) as *mut TargetEntry;

            attnum += 1;
            if equal(node as *const c_void, (*tle).expr as *const c_void) {
                /* acceptable, replace it with a GROUP Var */
                return buildGroupedVar(attnum, (*tle).ressortgroupref, context) as *mut Node;
            }
        });
    }

    /*
     * Constants are always acceptable.
     */
    if IsA!(node, T_Const) || IsA!(node, T_Param) {
        return node;
    }

    /*
     * If we have an ungrouped Var of the original query level, we have a
     * failure.
     */
    if IsA!(node, T_Var) {
        let var: *mut Var = node as *mut Var;
        let rte: *mut RangeTblEntry;
        let attname: *mut c_char;

        if (*var).varlevelsup as c_int != (*context).sublevels_up {
            return node; /* it's not local to my query, ignore */
        }

        /*
         * Check for a match, if we didn't do it above.
         */
        if !(*context).have_non_var_grouping || (*context).sublevels_up != 0 {
            let mut attnum: c_int = 0;

            foreach!(gl, (*context).groupClauses, {
                let tle: *mut TargetEntry = lfirst(current_cell!(gl)) as *mut TargetEntry;
                let gvar: *mut Var = (*tle).expr as *mut Var;

                attnum += 1;
                if IsA!(gvar as *mut Node, T_Var)
                    && (*gvar).varno == (*var).varno
                    && (*gvar).varattno == (*var).varattno
                    && (*gvar).varlevelsup == 0
                {
                    /* acceptable, replace it with a GROUP Var */
                    return buildGroupedVar(attnum, (*tle).ressortgroupref, context) as *mut Node;
                }
            });
        }

        /*
         * Check whether the Var is known functionally dependent on the GROUP
         * BY columns.
         */
        if list_member_int(*(*context).func_grouped_rels, (*var).varno as c_int) {
            return node; /* previously proven acceptable */
        }

        assert!((*var).varno > 0);
        rte = rt_fetch((*var).varno as Index, (*(*context).pstate).p_rtable);
        if (*rte).rtekind == RTE_RELATION {
            if check_functional_grouping(
                (*rte).relid,
                (*var).varno as Index,
                0,
                (*context).groupClauseCommonVars,
                &mut (*(*context).qry).constraintDeps,
            ) {
                *(*context).func_grouped_rels =
                    lappend_int(*(*context).func_grouped_rels, (*var).varno as c_int);
                return node; /* acceptable */
            }
        }

        /* Found an ungrouped local variable; generate error message */
        attname = get_rte_attribute_name(rte, (*var).varattno);
        if (*context).sublevels_up == 0 {
            let relname = std::ffi::CStr::from_ptr((*(*rte).eref).aliasname).to_string_lossy();
            let colname = std::ffi::CStr::from_ptr(attname).to_string_lossy();
            ereport!(ERROR,
                errmsg!("column \"{}.{}\" must appear in the GROUP BY clause or be used in an aggregate function",
                    relname, colname)
                /* C also: errcode(ERRCODE_GROUPING_ERROR),
                   in_agg_direct_args ? errdetail("Direct arguments of an ordered-set aggregate must use only grouped columns.") : 0,
                   parser_errposition((*context).pstate, (*var).location) */
            );
        } else {
            let relname = std::ffi::CStr::from_ptr((*(*rte).eref).aliasname).to_string_lossy();
            let colname = std::ffi::CStr::from_ptr(attname).to_string_lossy();
            ereport!(ERROR,
                errmsg!("subquery uses ungrouped column \"{}.{}\" from outer query",
                    relname, colname)
                /* C also: errcode(ERRCODE_GROUPING_ERROR),
                   parser_errposition((*context).pstate, (*var).location) */
            );
        }
    }

    if IsA!(node, T_Query) {
        /* Recurse into subselects */
        let newnode: *mut Query;

        (*context).sublevels_up += 1;
        newnode = query_tree_mutator(
            node as *mut Query,
            Some(substitute_grouped_columns_mutator),
            context_ptr,
            0,
        );
        (*context).sublevels_up -= 1;
        return newnode as *mut Node;
    }
    expression_tree_mutator(
        node,
        Some(substitute_grouped_columns_mutator),
        context_ptr,
    )
}

/*
 * finalize_grouping_exprs -
 *	  Scan the given expression tree for GROUPING() and related calls,
 *	  and validate and process their arguments.
 */
unsafe fn finalize_grouping_exprs(
    node: *mut Node,
    pstate: *mut ParseState,
    qry: *mut Query,
    groupClauses: *mut List,
    hasJoinRTEs: bool,
    have_non_var_grouping: bool,
) {
    let mut context = SubstituteGroupedColumnsContext {
        pstate,
        qry,
        hasJoinRTEs,
        groupClauses,
        groupClauseCommonVars: NIL,
        gset_common: NIL,
        have_non_var_grouping,
        func_grouped_rels: core::ptr::null_mut(),
        sublevels_up: 0,
        in_agg_direct_args: false,
    };
    finalize_grouping_exprs_walker(node, &mut context as *mut SubstituteGroupedColumnsContext as *mut c_void);
}

unsafe fn finalize_grouping_exprs_walker(
    node: *mut Node,
    context_ptr: *mut c_void,
) -> bool {
    let context: *mut SubstituteGroupedColumnsContext =
        context_ptr as *mut SubstituteGroupedColumnsContext;
    let mut gl: *mut ListCell;

    if node.is_null() {
        return false;
    }
    if IsA!(node, T_Const) || IsA!(node, T_Param) {
        return false; /* constants are always acceptable */
    }

    if IsA!(node, T_Aggref) {
        let agg: *mut Aggref = node as *mut Aggref;

        if (*agg).agglevelsup as c_int == (*context).sublevels_up {
            /*
             * If we find an aggregate call of the original level, do not
             * recurse into its normal arguments, ORDER BY arguments, or
             * filter; GROUPING exprs of this level are not allowed there. But
             * check direct arguments as though they weren't in an aggregate.
             */
            let result: bool;

            assert!(!(*context).in_agg_direct_args);
            (*context).in_agg_direct_args = true;
            result = finalize_grouping_exprs_walker(
                (*agg).aggdirectargs as *mut Node,
                context_ptr,
            );
            (*context).in_agg_direct_args = false;
            return result;
        }

        /*
         * We can skip recursing into aggregates of higher levels altogether.
         */
        if (*agg).agglevelsup as c_int > (*context).sublevels_up {
            return false;
        }
    }

    if IsA!(node, T_GroupingFunc) {
        let grp: *mut GroupingFunc = node as *mut GroupingFunc;

        /*
         * We only need to check GroupingFunc nodes at the exact level to
         * which they belong, since they cannot mix levels in arguments.
         */

        if (*grp).agglevelsup as c_int == (*context).sublevels_up {
            let mut lc: *mut ListCell;
            let mut ref_list: *mut List = NIL;

            foreach!(lc, (*grp).args, {
                let mut expr: *mut Node = lfirst(current_cell!(lc)) as *mut Node;
                let mut r#ref: Index = 0;

                if (*context).hasJoinRTEs {
                    expr = flatten_join_alias_vars(
                        core::ptr::null_mut(),
                        (*context).qry,
                        expr,
                    );
                }

                /*
                 * Each expression must match a grouping entry at the current
                 * query level.
                 */

                if IsA!(expr, T_Var) {
                    let var: *mut Var = expr as *mut Var;

                    if (*var).varlevelsup as c_int == (*context).sublevels_up {
                        foreach!(gl, (*context).groupClauses, {
                            let tle: *mut TargetEntry = lfirst(current_cell!(gl)) as *mut TargetEntry;
                            let gvar: *mut Var = (*tle).expr as *mut Var;

                            if IsA!(gvar as *mut Node, T_Var)
                                && (*gvar).varno == (*var).varno
                                && (*gvar).varattno == (*var).varattno
                                && (*gvar).varlevelsup == 0
                            {
                                r#ref = (*tle).ressortgroupref;
                                break;
                            }
                        });
                    }
                } else if (*context).have_non_var_grouping && (*context).sublevels_up == 0 {
                    foreach!(gl, (*context).groupClauses, {
                        let tle: *mut TargetEntry = lfirst(current_cell!(gl)) as *mut TargetEntry;

                        if equal(expr as *const c_void, (*tle).expr as *const c_void) {
                            r#ref = (*tle).ressortgroupref;
                            break;
                        }
                    });
                }

                if r#ref == 0 {
                    ereport!(ERROR,
                        errmsg!("arguments to GROUPING must be grouping expressions of the associated query level")
                        /* C also: errcode(ERRCODE_GROUPING_ERROR),
                           parser_errposition((*context).pstate, exprLocation(expr)) */
                    );
                }

                ref_list = lappend_int(ref_list, r#ref as c_int);
            });

            (*grp).refs = ref_list;
        }

        if (*grp).agglevelsup as c_int > (*context).sublevels_up {
            return false;
        }
    }

    if IsA!(node, T_Query) {
        /* Recurse into subselects */
        let result: bool;

        (*context).sublevels_up += 1;
        result = query_tree_walker(
            node as *mut Query,
            Some(finalize_grouping_exprs_walker),
            context_ptr,
            0,
        );
        (*context).sublevels_up -= 1;
        return result;
    }
    expression_tree_walker(node, Some(finalize_grouping_exprs_walker), context_ptr)
}

/*
 * buildGroupedVar -
 *	  build a Var node that references the RTE_GROUP RTE
 */
unsafe fn buildGroupedVar(
    attnum: c_int,
    ressortgroupref: Index,
    context: *const SubstituteGroupedColumnsContext,
) -> *mut Var {
    let grouping_nsitem: *mut ParseNamespaceItem = (*(*context).pstate).p_grouping_nsitem;
    let nscol: *mut ParseNamespaceColumn =
        (*grouping_nsitem).p_nscolumns.add(attnum as usize - 1);

    assert!((*nscol).p_varno == (*grouping_nsitem).p_rtindex as Index);
    assert!((*nscol).p_varattno == attnum as i16);
    let var: *mut Var = makeVar(
        (*nscol).p_varno as c_int,
        (*nscol).p_varattno,
        (*nscol).p_vartype,
        (*nscol).p_vartypmod,
        (*nscol).p_varcollid,
        (*context).sublevels_up as Index,
    );
    /* makeVar doesn't offer parameters for these, so set by hand: */
    (*var).varnosyn = (*nscol).p_varnosyn;
    (*var).varattnosyn = (*nscol).p_varattnosyn;

    if !(*(*context).qry).groupingSets.is_null()
        && !list_member_int((*context).gset_common, ressortgroupref as c_int)
    {
        (*var).varnullingrels =
            bms_add_member((*var).varnullingrels, (*grouping_nsitem).p_rtindex as c_int);
    }

    var
}

/*
 * Given a GroupingSet node, expand it and return a list of lists.
 */
unsafe fn expand_groupingset_node(gs: *mut GroupingSet) -> *mut List {
    let mut result: *mut List = NIL;

    match (*gs).kind {
        GROUPING_SET_EMPTY => {
            result = list_make1!(NIL as *mut c_void);
        }
        GROUPING_SET_SIMPLE => {
            result = list_make1!((*gs).content as *mut c_void);
        }
        GROUPING_SET_ROLLUP => {
            let rollup_val: *mut List = (*gs).content;
            let mut lc: *mut ListCell;
            let mut curgroup_size: c_int = list_length((*gs).content);

            while curgroup_size > 0 {
                let mut current_result: *mut List = NIL;
                let mut i: c_int = curgroup_size;

                foreach!(lc, rollup_val, {
                    let gs_current: *mut GroupingSet = lfirst(current_cell!(lc)) as *mut GroupingSet;

                    assert!((*gs_current).kind == GROUPING_SET_SIMPLE);

                    current_result = list_concat(current_result, (*gs_current).content);

                    /* If we are done with making the current group, break */
                    i -= 1;
                    if i == 0 {
                        break;
                    }
                });

                result = lappend(result, current_result as *mut c_void);
                curgroup_size -= 1;
            }

            result = lappend(result, NIL as *mut c_void);
        }
        GROUPING_SET_CUBE => {
            let cube_list: *mut List = (*gs).content;
            let number_bits: c_int = list_length(cube_list);
            let num_sets: u32;
            let mut i: u32;

            /* parser should cap this much lower */
            assert!(number_bits < 31);

            num_sets = 1u32 << number_bits;

            i = 0;
            while i < num_sets {
                let mut current_result: *mut List = NIL;
                let mut lc: *mut ListCell;
                let mut mask: u32 = 1;

                foreach!(lc, cube_list, {
                    let gs_current: *mut GroupingSet = lfirst(current_cell!(lc)) as *mut GroupingSet;

                    assert!((*gs_current).kind == GROUPING_SET_SIMPLE);

                    if (mask & i) != 0 {
                        current_result = list_concat(current_result, (*gs_current).content);
                    }

                    mask <<= 1;
                });

                result = lappend(result, current_result as *mut c_void);
                i += 1;
            }
        }
        GROUPING_SET_SETS => {
            let mut lc: *mut ListCell;

            foreach!(lc, (*gs).content, {
                let current_result: *mut List = expand_groupingset_node(lfirst(current_cell!(lc)) as *mut GroupingSet);

                result = list_concat(result, current_result);
            });
        }
        _ => {}
    }

    result
}

/* list_sort comparator to sort sub-lists by length */
unsafe fn cmp_list_len_asc(
    a: *const ListCell,
    b: *const ListCell,
) -> c_int {
    let la: c_int = list_length(lfirst(a as *mut ListCell) as *mut List);
    let lb: c_int = list_length(lfirst(b as *mut ListCell) as *mut List);
    crate::common::int::pg_cmp_s32(la, lb)
}

/* list_sort comparator to sort sub-lists by length and contents */
unsafe fn cmp_list_len_contents_asc(
    a: *const ListCell,
    b: *const ListCell,
) -> c_int {
    let res = cmp_list_len_asc(a, b);

    if res == 0 {
        let la: *mut List = lfirst(a as *mut ListCell) as *mut List;
        let lb: *mut List = lfirst(b as *mut ListCell) as *mut List;
        let mut lca = list_head(la);
        let mut lcb = list_head(lb);

        while !lca.is_null() && !lcb.is_null() {
            let va: c_int = lfirst_int(lca);
            let vb: c_int = lfirst_int(lcb);

            if va > vb { return 1; }
            if va < vb { return -1; }

            lca = lnext(la, lca);
            lcb = lnext(lb, lcb);
        }
    }

    res
}

/*
 * Expand a groupingSets clause to a flat list of grouping sets.
 * The returned list is sorted by length, shortest sets first.
 */
pub unsafe fn expand_grouping_sets(
    groupingSets: *mut List,
    groupDistinct: bool,
    limit: c_int,
) -> *mut List {
    let mut expanded_groups: *mut List = NIL;
    let mut result: *mut List = NIL;
    let mut numsets: f64 = 1.0;
    let mut lc: *mut ListCell;

    if groupingSets.is_null() {
        return NIL;
    }

    foreach!(lc, groupingSets, {
        let mut current_result: *mut List = NIL;
        let gs: *mut GroupingSet = lfirst(current_cell!(lc)) as *mut GroupingSet;

        current_result = expand_groupingset_node(gs);

        assert!(!current_result.is_null());

        numsets *= list_length(current_result) as f64;

        if limit >= 0 && numsets > limit as f64 {
            return NIL;
        }

        expanded_groups = lappend(expanded_groups, current_result as *mut c_void);
    });

    /*
     * Do cartesian product between sublists of expanded_groups.
     */

    foreach!(lc, linitial(expanded_groups) as *mut List, {
        result = lappend(result, list_union_int(NIL, lfirst(current_cell!(lc)) as *mut List) as *mut c_void);
    });

    // for_each_from(lc, expanded_groups, 1)
    {
        let mut _idx: c_int = 0;
        let mut _lc = list_head(expanded_groups);
        while !_lc.is_null() {
            if _idx >= 1 {
                let p: *mut List = lfirst(_lc) as *mut List;
                let mut new_result: *mut List = NIL;
                let mut lc2: *mut ListCell;

                foreach!(lc2, result, {
                    let q: *mut List = lfirst(current_cell!(lc2)) as *mut List;
                    let mut lc3: *mut ListCell;

                    foreach!(lc3, p, {
                        new_result = lappend(
                            new_result,
                            list_union_int(q, lfirst(current_cell!(lc3)) as *mut List) as *mut c_void,
                        );
                    });
                });
                result = new_result;
            }
            _idx += 1;
            _lc = lnext(expanded_groups, _lc);
        }
    }

    /* Now sort the lists by length and deduplicate if necessary */
    if !groupDistinct || list_length(result) < 2 {
        list_sort(result, cmp_list_len_asc);
    } else {
        let mut cell: *mut ListCell;
        let mut prev: *mut List;

        /* Sort each groupset individually */
        {
            let mut _lc = list_head(result);
            while !_lc.is_null() {
                list_sort(lfirst(_lc) as *mut List, list_int_cmp);
                _lc = lnext(result, _lc);
            }
        }

        /* Now sort the list of groupsets by length and contents */
        list_sort(result, cmp_list_len_contents_asc);

        /* Finally, remove duplicates */
        prev = linitial(result) as *mut List;
        // for_each_from(cell, result, 1)
        {
            let mut _idx: c_int = 0;
            let mut _lc = list_head(result);
            while !_lc.is_null() {
                if _idx >= 1 {
                    if equal(lfirst(_lc) as *const c_void, prev as *const c_void) {
                        let next_lc = lnext(result, _lc);
                        result = list_delete_cell(result, _lc);
                        _idx += 1;
                        _lc = next_lc;
                        continue;
                    } else {
                        prev = lfirst(_lc) as *mut List;
                    }
                }
                _idx += 1;
                _lc = lnext(result, _lc);
            }
        }
    }

    result
}

/*
 * get_aggregate_argtypes
 *	Identify the specific datatypes passed to an aggregate call.
 */
pub unsafe fn get_aggregate_argtypes(aggref: *mut Aggref, inputTypes: *mut Oid) -> c_int {
    use crate::pg_config_manual::FUNC_MAX_ARGS;
    let mut numArguments: c_int = 0;
    let mut lc: *mut ListCell;

    assert!(list_length((*aggref).aggargtypes) <= FUNC_MAX_ARGS as c_int);

    foreach!(lc, (*aggref).aggargtypes, {
        *inputTypes.add(numArguments as usize) = lfirst_oid(current_cell!(lc));
        numArguments += 1;
    });

    numArguments
}

/*
 * resolve_aggregate_transtype
 *	Identify the transition state value's datatype for an aggregate call.
 */
pub unsafe fn resolve_aggregate_transtype(
    aggfuncid: Oid,
    mut aggtranstype: Oid,
    inputTypes: *mut Oid,
    numArguments: c_int,
) -> Oid {
    /* resolve actual type of transition state, if polymorphic */
    if IsPolymorphicType(aggtranstype) {
        /* have to fetch the agg's declared input types... */
        let mut declaredArgTypes: *mut Oid = core::ptr::null_mut();
        let mut agg_nargs: c_int = 0;

        get_func_signature(aggfuncid, &mut declaredArgTypes, &mut agg_nargs);

        /*
         * VARIADIC ANY aggs could have more actual than declared args, but
         * such extra args can't affect polymorphic type resolution.
         */
        assert!(agg_nargs <= numArguments);

        aggtranstype = enforce_generic_type_consistency(
            inputTypes,
            declaredArgTypes,
            agg_nargs,
            aggtranstype,
            false,
        );
        pfree(declaredArgTypes as *mut c_void);
    }
    aggtranstype
}

/*
 * agg_args_support_sendreceive
 *		Returns true if all non-byval types of aggref's args have send and
 *		receive functions.
 */
pub unsafe fn agg_args_support_sendreceive(aggref: *mut Aggref) -> bool {
    let mut lc: *mut ListCell;

    foreach!(lc, (*aggref).args, {
        let tle: *mut TargetEntry = lfirst(current_cell!(lc)) as *mut TargetEntry;
        let r#type: Oid = exprType((*tle).expr as *mut Node);

        /*
         * RECORD is a special case: record_recv only works if passed the
         * correct typmod to identify the specific anonymous record type.
         */
        use crate::catalog::pg_type_d::RECORDOID;
        if r#type == RECORDOID {
            return false;
        }

        let typeTuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(r#type));
        if !HeapTupleIsValid(typeTuple) {
            panic!("cache lookup failed for type {}", r#type);
        }

        let pt: Form_pg_type = GETSTRUCT(typeTuple) as Form_pg_type;

        if !(*pt).typbyval
            && (!OidIsValid((*pt).typsend) || !OidIsValid((*pt).typreceive))
        {
            ReleaseSysCache(typeTuple);
            return false;
        }
        ReleaseSysCache(typeTuple);
    });
    true
}

/*
 * build_aggregate_transfn_expr
 *		Create an expression tree for the transition function of an aggregate.
 */
pub unsafe fn build_aggregate_transfn_expr(
    agg_input_types: *mut Oid,
    agg_num_inputs: c_int,
    agg_num_direct_inputs: c_int,
    agg_variadic: bool,
    agg_state_type: Oid,
    agg_input_collation: Oid,
    transfn_oid: Oid,
    invtransfn_oid: Oid,
    transfnexpr: *mut *mut Expr,
    invtransfnexpr: *mut *mut Expr,
) {
    let mut args: *mut List;
    let mut fexpr: *mut FuncExpr;
    let mut i: c_int;

    /*
     * Build arg list to use in the transfn FuncExpr node.
     */
    args = list_make1!(make_agg_arg(agg_state_type, agg_input_collation) as *mut c_void);

    i = agg_num_direct_inputs;
    while i < agg_num_inputs {
        args = lappend(
            args,
            make_agg_arg(*agg_input_types.add(i as usize), agg_input_collation) as *mut c_void,
        );
        i += 1;
    }

    fexpr = makeFuncExpr(
        transfn_oid,
        agg_state_type,
        args,
        InvalidOid,
        agg_input_collation,
        COERCE_EXPLICIT_CALL,
    );
    (*fexpr).funcvariadic = agg_variadic;
    *transfnexpr = fexpr as *mut Expr;

    /*
     * Build invtransfn expression if requested, with same args as transfn
     */
    if !invtransfnexpr.is_null() {
        if OidIsValid(invtransfn_oid) {
            fexpr = makeFuncExpr(
                invtransfn_oid,
                agg_state_type,
                args,
                InvalidOid,
                agg_input_collation,
                COERCE_EXPLICIT_CALL,
            );
            (*fexpr).funcvariadic = agg_variadic;
            *invtransfnexpr = fexpr as *mut Expr;
        } else {
            *invtransfnexpr = core::ptr::null_mut();
        }
    }
}

/*
 * build_aggregate_serialfn_expr
 *		Like build_aggregate_transfn_expr, but creates an expression tree for
 *		the serialization function of an aggregate.
 */
pub unsafe fn build_aggregate_serialfn_expr(
    serialfn_oid: Oid,
    serialfnexpr: *mut *mut Expr,
) {
    let args: *mut List;
    let fexpr: *mut FuncExpr;

    /* serialfn always takes INTERNAL and returns BYTEA */
    args = list_make1!(make_agg_arg(INTERNALOID, InvalidOid) as *mut c_void);

    fexpr = makeFuncExpr(
        serialfn_oid,
        BYTEAOID,
        args,
        InvalidOid,
        InvalidOid,
        COERCE_EXPLICIT_CALL,
    );
    *serialfnexpr = fexpr as *mut Expr;
}

/*
 * build_aggregate_deserialfn_expr
 *		Like build_aggregate_transfn_expr, but creates an expression tree for
 *		the deserialization function of an aggregate.
 */
pub unsafe fn build_aggregate_deserialfn_expr(
    deserialfn_oid: Oid,
    deserialfnexpr: *mut *mut Expr,
) {
    let args: *mut List;
    let fexpr: *mut FuncExpr;

    /* deserialfn always takes BYTEA, INTERNAL and returns INTERNAL */
    args = list_make2!(
        make_agg_arg(BYTEAOID, InvalidOid) as *mut c_void,
        make_agg_arg(INTERNALOID, InvalidOid) as *mut c_void
    );

    fexpr = makeFuncExpr(
        deserialfn_oid,
        INTERNALOID,
        args,
        InvalidOid,
        InvalidOid,
        COERCE_EXPLICIT_CALL,
    );
    *deserialfnexpr = fexpr as *mut Expr;
}

/*
 * build_aggregate_finalfn_expr
 *		Like build_aggregate_transfn_expr, but creates an expression tree for
 *		the final function of an aggregate.
 */
pub unsafe fn build_aggregate_finalfn_expr(
    agg_input_types: *mut Oid,
    num_finalfn_inputs: c_int,
    agg_state_type: Oid,
    agg_result_type: Oid,
    agg_input_collation: Oid,
    finalfn_oid: Oid,
    finalfnexpr: *mut *mut Expr,
) {
    let mut args: *mut List;
    let mut i: c_int;

    /*
     * Build expr tree for final function
     */
    args = list_make1!(make_agg_arg(agg_state_type, agg_input_collation) as *mut c_void);

    /* finalfn may take additional args, which match agg's input types */
    i = 0;
    while i < num_finalfn_inputs - 1 {
        args = lappend(
            args,
            make_agg_arg(*agg_input_types.add(i as usize), agg_input_collation) as *mut c_void,
        );
        i += 1;
    }

    *finalfnexpr = makeFuncExpr(
        finalfn_oid,
        agg_result_type,
        args,
        InvalidOid,
        agg_input_collation,
        COERCE_EXPLICIT_CALL,
    ) as *mut Expr;
    /* finalfn is currently never treated as variadic */
}

/*
 * Convenience function to build dummy argument expressions for aggregates.
 *
 * We really only care that an aggregate support function can discover its
 * actual argument types at runtime using get_fn_expr_argtype(), so it's okay
 * to use Param nodes that don't correspond to any real Param.
 */
unsafe fn make_agg_arg(argtype: Oid, argcollation: Oid) -> *mut Node {
    let argp: *mut Param = makeNode!(Param, T_Param);

    (*argp).paramkind = PARAM_EXEC;
    (*argp).paramid = -1;
    (*argp).paramtype = argtype;
    (*argp).paramtypmod = -1;
    (*argp).paramcollid = argcollation;
    (*argp).location = -1;
    argp as *mut Node
}
