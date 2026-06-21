//! parse_cte.rs
//!   handle CTEs (common table expressions) in parser
//!
//! Translated 1:1 from postgres/src/backend/parser/parse_cte.c
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!   src/backend/parser/parse_cte.c

#![allow(unused_variables)]
#![allow(dead_code)]

use crate::prelude::*;
use crate::for_each_cell;

use crate::{castNode, current_cell, foreach, lfirst_node, strVal, IsA};

use crate::catalog::pg_known_oids::DEFAULT_COLLATION_OID;
use crate::catalog::pg_type_d::{TEXTOID, UNKNOWNOID};

use crate::nodes::bitmapset::{bms_add_member, bms_del_member, bms_is_empty, Bitmapset};
use crate::nodes::nodeFuncs::{exprCollation, exprLocation, exprType, exprTypmod};
use crate::nodes::nodes::{nodeTag, Node, NodeTag};
use crate::nodes::pg_list::{
    lappend, lappend_int, lappend_oid, lcons, lfirst, lfirst_int, lfirst_mut,
    lfirst_oid, list_copy, list_delete_first, list_head, list_length, list_member, lnext, List,
    ListCell, NIL,
};
use crate::nodes::parsenodes::{
    CTECycleClause, CTESearchClause, CommonTableExpr, GetCTETargetList, Query,
    SelectStmt, SetOperation, SetOperationStmt, WithClause,
};
use crate::nodes::primnodes::{JoinExpr, RangeTblRef, RangeVar, SubLink, TargetEntry};
use crate::nodes::nodes::{CmdType, JoinType};
use crate::nodes::value::{makeString, String};
use crate::nodes::nodeFuncs::{raw_expression_tree_walker, tree_walker_callback};

use crate::parser::parse_node::{ParseExprKind, ParseLoc, ParseState};

use crate::utils::adt::format_type::{format_type_be, format_type_with_typemod};
use crate::utils::adt::selfuncs::TypeCacheEntry;
unsafe fn lookup_type_cache(_type_id: Oid, _flags: c_int) -> *mut TypeCacheEntry { crate::utils::cache::typcache::lookup_type_cache(_type_id, _flags) as _ } // TODO(pg-port): utils/cache/typcache.c
unsafe fn get_negator(_opno: Oid) -> Oid { crate::utils::cache::lsyscache::get_negator(_opno) } // TODO(pg-port): utils/cache/lsyscache.c

extern "C" {
    fn strcmp(a: *const c_char, b: *const c_char) -> c_int;
}

// ---------------------------------------------------------------------------
// Stubs for symbols not yet ported.  These functions live in other PostgreSQL
// modules that PepperDB has not translated yet; minimal local declarations keep
// this file's translation faithful and self-contained.
// ---------------------------------------------------------------------------

// TODO(pg-port): real TYPECACHE_EQ_OPR lives in utils/cache/typcache.h.
const TYPECACHE_EQ_OPR: c_int = 0x0001;

// TODO(pg-port): real transformExpr lives in parser/parse_expr.c.
unsafe fn transformExpr(
    pstate: *mut ParseState,
    expr: *mut Node,
    exprKind: ParseExprKind,
) -> *mut Node {
    let _ = (pstate, expr, exprKind);
    expr
}

// TODO(pg-port): real parse_sub_analyze lives in parser/analyze.c.
unsafe fn parse_sub_analyze(
    parseTree: *mut Node,
    parentParseState: *mut ParseState,
    parentCTE: *mut CommonTableExpr,
    locked_from_parent: bool,
    resolve_unknowns: bool,
) -> *mut Query {
    let _ = (parseTree, parentParseState, parentCTE, locked_from_parent, resolve_unknowns);
    null_mut()
}

// TODO(pg-port): real select_common_type lives in parser/parse_coerce.c.
unsafe fn select_common_type(
    pstate: *mut ParseState,
    exprs: *mut List,
    context: *const c_char,
    which_expr: *mut *mut Node,
) -> Oid {
    let _ = (pstate, exprs, context, which_expr);
    UNKNOWNOID
}

// TODO(pg-port): real coerce_to_common_type lives in parser/parse_coerce.c.
unsafe fn coerce_to_common_type(
    pstate: *mut ParseState,
    node: *mut Node,
    targetTypeId: Oid,
    context: *const c_char,
) -> *mut Node {
    let _ = (pstate, targetTypeId, context);
    node
}

// TODO(pg-port): real select_common_typmod lives in parser/parse_coerce.c.
unsafe fn select_common_typmod(pstate: *mut ParseState, exprs: *mut List, common_type: Oid) -> int32 {
    let _ = (pstate, exprs, common_type);
    -1
}

// TODO(pg-port): real select_common_collation lives in parser/parse_collate.c.
unsafe fn select_common_collation(pstate: *mut ParseState, exprs: *mut List, none_ok: bool) -> Oid {
    let _ = (pstate, exprs, none_ok);
    InvalidOid
}

// TODO(pg-port): real get_collation_name lives in utils/cache/lsyscache.c.
unsafe fn get_collation_name(colloid: Oid) -> *mut c_char {
    let _ = colloid;
    null_mut()
}

/* Enumeration of contexts in which a self-reference is disallowed */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum RecursionContext {
    RECURSION_OK,
    RECURSION_NONRECURSIVETERM, /* inside the left-hand term */
    RECURSION_SUBLINK,          /* inside a sublink */
    RECURSION_OUTERJOIN,        /* inside nullable side of an outer join */
    RECURSION_INTERSECT,        /* underneath INTERSECT (ALL) */
    RECURSION_EXCEPT,           /* underneath EXCEPT (ALL) */
}
pub use RecursionContext::*;

/* Associated error messages --- each must have one %s for CTE name */
static recursion_errormsgs: [&str; 6] = [
    /* RECURSION_OK */
    "",
    /* RECURSION_NONRECURSIVETERM */
    "recursive reference to query \"{}\" must not appear within its non-recursive term",
    /* RECURSION_SUBLINK */
    "recursive reference to query \"{}\" must not appear within a subquery",
    /* RECURSION_OUTERJOIN */
    "recursive reference to query \"{}\" must not appear within an outer join",
    /* RECURSION_INTERSECT */
    "recursive reference to query \"{}\" must not appear within INTERSECT",
    /* RECURSION_EXCEPT */
    "recursive reference to query \"{}\" must not appear within EXCEPT",
];

/*
 * For WITH RECURSIVE, we have to find an ordering of the clause members
 * with no forward references, and determine which members are recursive
 * (i.e., self-referential).  It is convenient to do this with an array
 * of CteItems instead of a list of CommonTableExprs.
 */
#[repr(C)]
pub struct CteItem {
    pub cte: *mut CommonTableExpr, /* One CTE to examine */
    pub id: c_int,                 /* Its ID number for dependencies */
    pub depends_on: *mut Bitmapset, /* CTEs depended on (not including self) */
}

/* CteState is what we need to pass around in the tree walkers */
#[repr(C)]
pub struct CteState {
    /* global state: */
    pub pstate: *mut ParseState, /* global parse state */
    pub items: *mut CteItem,     /* array of CTEs and extra data */
    pub numitems: c_int,         /* number of CTEs */
    /* working state during a tree walk: */
    pub curitem: c_int,          /* index of item currently being examined */
    pub innerwiths: *mut List,   /* list of lists of CommonTableExpr */
    /* working state for checkWellFormedRecursion walk only: */
    pub selfrefcount: c_int,     /* number of self-references detected */
    pub context: RecursionContext, /* context to allow or disallow self-ref */
}

/*
 * transformWithClause -
 *	  Transform the list of WITH clause "common table expressions" into
 *	  Query nodes.
 *
 * The result is the list of transformed CTEs to be put into the output
 * Query.  (This is in fact the same as the ending value of p_ctenamespace,
 * but it seems cleaner to not expose that in the function's API.)
 */
pub unsafe fn transformWithClause(pstate: *mut ParseState, withClause: *mut WithClause) -> *mut List {
    let mut lc: *mut ListCell;

    /* Only one WITH clause per query level */
    Assert!((*pstate).p_ctenamespace == NIL);
    Assert!((*pstate).p_future_ctes == NIL);

    /*
     * For either type of WITH, there must not be duplicate CTE names in the
     * list.  Check this right away so we needn't worry later.
     *
     * Also, tentatively mark each CTE as non-recursive, and initialize its
     * reference count to zero, and set pstate->p_hasModifyingCTE if needed.
     */
    foreach!(lc, (*withClause).ctes, {
        let cte: *mut CommonTableExpr = lfirst(current_cell!(lc)) as *mut CommonTableExpr;
        let mut rest: *mut ListCell;

        for_each_cell!(rest, (*withClause).ctes, lnext((*withClause).ctes, current_cell!(lc)), {
            let cte2: *mut CommonTableExpr = lfirst(current_cell!(rest)) as *mut CommonTableExpr;

            if strcmp((*cte).ctename, (*cte2).ctename) == 0 {
                let _ = parser_errposition(pstate, (*cte2).location);
                ereport!(
                    ERROR,
                    errmsg!(
                        "WITH query name \"{}\" specified more than once",
                        cstr((*cte2).ctename)
                    )
                );
            }
        });

        (*cte).cterecursive = false;
        (*cte).cterefcount = 0;

        if !IsA!((*cte).ctequery, T_SelectStmt) {
            /* must be a data-modifying statement */
            Assert!(
                IsA!((*cte).ctequery, T_InsertStmt)
                    || IsA!((*cte).ctequery, T_UpdateStmt)
                    || IsA!((*cte).ctequery, T_DeleteStmt)
                    || IsA!((*cte).ctequery, T_MergeStmt)
            );

            (*pstate).p_hasModifyingCTE = true;
        }
    });

    if (*withClause).recursive {
        /*
         * For WITH RECURSIVE, we rearrange the list elements if needed to
         * eliminate forward references.  First, build a work array and set up
         * the data structure needed by the tree walkers.
         */
        let mut cstate: CteState = core::mem::zeroed();
        let mut i: c_int;

        cstate.pstate = pstate;
        cstate.numitems = list_length((*withClause).ctes);
        cstate.items =
            palloc0(cstate.numitems as Size * core::mem::size_of::<CteItem>()) as *mut CteItem;
        i = 0;
        foreach!(lc, (*withClause).ctes, {
            (*cstate.items.add(i as usize)).cte =
                lfirst(current_cell!(lc)) as *mut CommonTableExpr;
            (*cstate.items.add(i as usize)).id = i;
            i += 1;
        });

        /*
         * Find all the dependencies and sort the CteItems into a safe
         * processing order.  Also, mark CTEs that contain self-references.
         */
        makeDependencyGraph(&raw mut cstate);

        /*
         * Check that recursive queries are well-formed.
         */
        checkWellFormedRecursion(&raw mut cstate);

        /*
         * Set up the ctenamespace for parse analysis.  Per spec, all the WITH
         * items are visible to all others, so stuff them all in before parse
         * analysis.  We build the list in safe processing order so that the
         * planner can process the queries in sequence.
         */
        i = 0;
        while i < cstate.numitems {
            let cte: *mut CommonTableExpr = (*cstate.items.add(i as usize)).cte;

            (*pstate).p_ctenamespace = lappend((*pstate).p_ctenamespace, cte as *mut c_void);
            i += 1;
        }

        /*
         * Do parse analysis in the order determined by the topological sort.
         */
        i = 0;
        while i < cstate.numitems {
            let cte: *mut CommonTableExpr = (*cstate.items.add(i as usize)).cte;

            analyzeCTE(pstate, cte);
            i += 1;
        }
    } else {
        /*
         * For non-recursive WITH, just analyze each CTE in sequence and then
         * add it to the ctenamespace.  This corresponds to the spec's
         * definition of the scope of each WITH name.  However, to allow error
         * reports to be aware of the possibility of an erroneous reference,
         * we maintain a list in p_future_ctes of the not-yet-visible CTEs.
         */
        (*pstate).p_future_ctes = list_copy((*withClause).ctes);

        foreach!(lc, (*withClause).ctes, {
            let cte: *mut CommonTableExpr = lfirst(current_cell!(lc)) as *mut CommonTableExpr;

            analyzeCTE(pstate, cte);
            (*pstate).p_ctenamespace = lappend((*pstate).p_ctenamespace, cte as *mut c_void);
            (*pstate).p_future_ctes = list_delete_first((*pstate).p_future_ctes);
        });
    }

    (*pstate).p_ctenamespace
}

/*
 * Perform the actual parse analysis transformation of one CTE.  All
 * CTEs it depends on have already been loaded into pstate->p_ctenamespace,
 * and have been marked with the correct output column names/types.
 */
unsafe fn analyzeCTE(pstate: *mut ParseState, cte: *mut CommonTableExpr) {
    let query: *mut Query;
    let search_clause: *mut CTESearchClause = (*cte).search_clause;
    let cycle_clause: *mut CTECycleClause = (*cte).cycle_clause;

    /* Analysis not done already */
    Assert!(!IsA!((*cte).ctequery, T_Query));

    /*
     * Before analyzing the CTE's query, we'd better identify the data type of
     * the cycle mark column if any, since the query could refer to that.
     * Other validity checks on the cycle clause will be done afterwards.
     */
    if !cycle_clause.is_null() {
        let typentry: *mut TypeCacheEntry;
        let op: Oid;

        (*cycle_clause).cycle_mark_value = transformExpr(
            pstate,
            (*cycle_clause).cycle_mark_value,
            ParseExprKind::EXPR_KIND_CYCLE_MARK,
        );
        (*cycle_clause).cycle_mark_default = transformExpr(
            pstate,
            (*cycle_clause).cycle_mark_default,
            ParseExprKind::EXPR_KIND_CYCLE_MARK,
        );

        (*cycle_clause).cycle_mark_type = select_common_type(
            pstate,
            crate::list_make2!(
                (*cycle_clause).cycle_mark_value,
                (*cycle_clause).cycle_mark_default
            ),
            c"CYCLE".as_ptr(),
            null_mut(),
        );
        (*cycle_clause).cycle_mark_value = coerce_to_common_type(
            pstate,
            (*cycle_clause).cycle_mark_value,
            (*cycle_clause).cycle_mark_type,
            c"CYCLE/SET/TO".as_ptr(),
        );
        (*cycle_clause).cycle_mark_default = coerce_to_common_type(
            pstate,
            (*cycle_clause).cycle_mark_default,
            (*cycle_clause).cycle_mark_type,
            c"CYCLE/SET/DEFAULT".as_ptr(),
        );

        (*cycle_clause).cycle_mark_typmod = select_common_typmod(
            pstate,
            crate::list_make2!(
                (*cycle_clause).cycle_mark_value,
                (*cycle_clause).cycle_mark_default
            ),
            (*cycle_clause).cycle_mark_type,
        );

        (*cycle_clause).cycle_mark_collation = select_common_collation(
            pstate,
            crate::list_make2!(
                (*cycle_clause).cycle_mark_value,
                (*cycle_clause).cycle_mark_default
            ),
            true,
        );

        /* Might as well look up the relevant <> operator while we are at it */
        typentry = lookup_type_cache((*cycle_clause).cycle_mark_type, TYPECACHE_EQ_OPR);
        if !OidIsValid((*typentry).eq_opr) {
            ereport!(
                ERROR,
                errmsg!(
                    "could not identify an equality operator for type {}",
                    cstr(format_type_be((*cycle_clause).cycle_mark_type))
                )
            );
        }
        op = get_negator((*typentry).eq_opr);
        if !OidIsValid(op) {
            ereport!(
                ERROR,
                errmsg!(
                    "could not identify an inequality operator for type {}",
                    cstr(format_type_be((*cycle_clause).cycle_mark_type))
                )
            );
        }

        (*cycle_clause).cycle_mark_neop = op;
    }

    /* Now we can get on with analyzing the CTE's query */
    query = parse_sub_analyze((*cte).ctequery, pstate, cte, false, true);
    (*cte).ctequery = query as *mut Node;

    /*
     * Check that we got something reasonable.  These first two cases should
     * be prevented by the grammar.
     */
    if !IsA!(query, T_Query) {
        elog!(ERROR, "unexpected non-Query statement in WITH");
    }
    if !(*query).utilityStmt.is_null() {
        elog!(ERROR, "unexpected utility statement in WITH");
    }

    /*
     * We disallow data-modifying WITH except at the top level of a query,
     * because it's not clear when such a modification should be executed.
     */
    if (*query).commandType != CmdType::CMD_SELECT && !(*pstate).parentParseState.is_null() {
        let _ = parser_errposition(pstate, (*cte).location);
        ereport!(
            ERROR,
            errmsg!("WITH clause containing a data-modifying statement must be at the top level")
        );
    }

    /*
     * CTE queries are always marked not canSetTag.  (Currently this only
     * matters for data-modifying statements, for which the flag will be
     * propagated to the ModifyTable plan node.)
     */
    (*query).canSetTag = false;

    if !(*cte).cterecursive {
        /* Compute the output column names/types if not done yet */
        analyzeCTETargetList(pstate, cte, GetCTETargetList(cte));
    } else {
        /*
         * Verify that the previously determined output column types and
         * collations match what the query really produced.  We have to check
         * this because the recursive term could have overridden the
         * non-recursive term, and we don't have any easy way to fix that.
         */
        let mut lctlist: *mut ListCell;
        let mut lctyp: *mut ListCell;
        let mut lctypmod: *mut ListCell;
        let mut lccoll: *mut ListCell;
        let mut varattno: c_int;

        lctyp = list_head((*cte).ctecoltypes);
        lctypmod = list_head((*cte).ctecoltypmods);
        lccoll = list_head((*cte).ctecolcollations);
        varattno = 0;
        foreach!(lctlist, GetCTETargetList(cte), {
            let te: *mut TargetEntry = lfirst(current_cell!(lctlist)) as *mut TargetEntry;
            let texpr: *mut Node;

            if (*te).resjunk {
                continue;
            }
            varattno += 1;
            Assert!(varattno == (*te).resno as c_int);
            if lctyp.is_null() || lctypmod.is_null() || lccoll.is_null() {
                /* shouldn't happen */
                elog!(ERROR, "wrong number of output columns in WITH");
            }
            texpr = (*te).expr as *mut Node;
            if exprType(texpr) != lfirst_oid(lctyp) || exprTypmod(texpr) != lfirst_int(lctypmod) {
                let _ = parser_errposition(pstate, exprLocation(texpr));
                ereport!(
                    ERROR,
                    errmsg!(
                        "recursive query \"{}\" column {} has type {} in non-recursive term but type {} overall",
                        cstr((*cte).ctename),
                        varattno,
                        cstr(format_type_with_typemod(lfirst_oid(lctyp), lfirst_int(lctypmod))),
                        cstr(format_type_with_typemod(exprType(texpr), exprTypmod(texpr)))
                    )
                );
            }
            if exprCollation(texpr) != lfirst_oid(lccoll) {
                let _ = parser_errposition(pstate, exprLocation(texpr));
                ereport!(
                    ERROR,
                    errmsg!(
                        "recursive query \"{}\" column {} has collation \"{}\" in non-recursive term but collation \"{}\" overall",
                        cstr((*cte).ctename),
                        varattno,
                        cstr(get_collation_name(lfirst_oid(lccoll))),
                        cstr(get_collation_name(exprCollation(texpr)))
                    )
                );
            }
            lctyp = lnext((*cte).ctecoltypes, lctyp);
            lctypmod = lnext((*cte).ctecoltypmods, lctypmod);
            lccoll = lnext((*cte).ctecolcollations, lccoll);
        });
        if !lctyp.is_null() || !lctypmod.is_null() || !lccoll.is_null() {
            /* shouldn't happen */
            elog!(ERROR, "wrong number of output columns in WITH");
        }
    }

    /*
     * Now make validity checks on the SEARCH and CYCLE clauses, if present.
     */
    if !search_clause.is_null() || !cycle_clause.is_null() {
        let ctequery: *mut Query;
        let sos: *mut SetOperationStmt;

        if !(*cte).cterecursive {
            let _ = parser_errposition(pstate, (*cte).location);
            ereport!(ERROR, errmsg!("WITH query is not recursive"));
        }

        /*
         * SQL requires a WITH list element (CTE) to be "expandable" in order
         * to allow a search or cycle clause.  That is a stronger requirement
         * than just being recursive.  It basically means the query expression
         * looks like
         *
         * non-recursive query UNION [ALL] recursive query
         *
         * and that the recursive query is not itself a set operation.
         *
         * As of this writing, most of these criteria are already satisfied by
         * all recursive CTEs allowed by PostgreSQL.  In the future, if
         * further variants recursive CTEs are accepted, there might be
         * further checks required here to determine what is "expandable".
         */

        ctequery = castNode!(Query, T_Query, (*cte).ctequery);
        Assert!(!(*ctequery).setOperations.is_null());
        sos = castNode!(SetOperationStmt, T_SetOperationStmt, (*ctequery).setOperations);

        /*
         * This left side check is not required for expandability, but
         * rewriteSearchAndCycle() doesn't currently have support for it, so
         * we catch it here.
         */
        if !IsA!((*sos).larg, T_RangeTblRef) {
            ereport!(
                ERROR,
                errmsg!("with a SEARCH or CYCLE clause, the left side of the UNION must be a SELECT")
            );
        }

        if !IsA!((*sos).rarg, T_RangeTblRef) {
            ereport!(
                ERROR,
                errmsg!("with a SEARCH or CYCLE clause, the right side of the UNION must be a SELECT")
            );
        }
    }

    if !search_clause.is_null() {
        let mut lc: *mut ListCell;
        let mut seen: *mut List = NIL;

        foreach!(lc, (*search_clause).search_col_list, {
            let colname: *mut String = lfirst_node!(String, T_String, current_cell!(lc));

            if !list_member((*cte).ctecolnames, colname as *const c_void) {
                let _ = parser_errposition(pstate, (*search_clause).location);
                ereport!(
                    ERROR,
                    errmsg!(
                        "search column \"{}\" not in WITH query column list",
                        cstr(strVal!(colname))
                    )
                );
            }

            if list_member(seen, colname as *const c_void) {
                let _ = parser_errposition(pstate, (*search_clause).location);
                ereport!(
                    ERROR,
                    errmsg!(
                        "search column \"{}\" specified more than once",
                        cstr(strVal!(colname))
                    )
                );
            }
            seen = lappend(seen, colname as *mut c_void);
        });

        if list_member(
            (*cte).ctecolnames,
            makeString((*search_clause).search_seq_column) as *const c_void,
        ) {
            let _ = parser_errposition(pstate, (*search_clause).location);
            ereport!(
                ERROR,
                errmsg!(
                    "search sequence column name \"{}\" already used in WITH query column list",
                    cstr((*search_clause).search_seq_column)
                )
            );
        }
    }

    if !cycle_clause.is_null() {
        let mut lc: *mut ListCell;
        let mut seen: *mut List = NIL;

        foreach!(lc, (*cycle_clause).cycle_col_list, {
            let colname: *mut String = lfirst_node!(String, T_String, current_cell!(lc));

            if !list_member((*cte).ctecolnames, colname as *const c_void) {
                let _ = parser_errposition(pstate, (*cycle_clause).location);
                ereport!(
                    ERROR,
                    errmsg!(
                        "cycle column \"{}\" not in WITH query column list",
                        cstr(strVal!(colname))
                    )
                );
            }

            if list_member(seen, colname as *const c_void) {
                let _ = parser_errposition(pstate, (*cycle_clause).location);
                ereport!(
                    ERROR,
                    errmsg!(
                        "cycle column \"{}\" specified more than once",
                        cstr(strVal!(colname))
                    )
                );
            }
            seen = lappend(seen, colname as *mut c_void);
        });

        if list_member(
            (*cte).ctecolnames,
            makeString((*cycle_clause).cycle_mark_column) as *const c_void,
        ) {
            let _ = parser_errposition(pstate, (*cycle_clause).location);
            ereport!(
                ERROR,
                errmsg!(
                    "cycle mark column name \"{}\" already used in WITH query column list",
                    cstr((*cycle_clause).cycle_mark_column)
                )
            );
        }

        if list_member(
            (*cte).ctecolnames,
            makeString((*cycle_clause).cycle_path_column) as *const c_void,
        ) {
            let _ = parser_errposition(pstate, (*cycle_clause).location);
            ereport!(
                ERROR,
                errmsg!(
                    "cycle path column name \"{}\" already used in WITH query column list",
                    cstr((*cycle_clause).cycle_path_column)
                )
            );
        }

        if strcmp(
            (*cycle_clause).cycle_mark_column,
            (*cycle_clause).cycle_path_column,
        ) == 0
        {
            let _ = parser_errposition(pstate, (*cycle_clause).location);
            ereport!(
                ERROR,
                errmsg!("cycle mark column name and cycle path column name are the same")
            );
        }
    }

    if !search_clause.is_null() && !cycle_clause.is_null() {
        if strcmp(
            (*search_clause).search_seq_column,
            (*cycle_clause).cycle_mark_column,
        ) == 0
        {
            let _ = parser_errposition(pstate, (*search_clause).location);
            ereport!(
                ERROR,
                errmsg!("search sequence column name and cycle mark column name are the same")
            );
        }

        if strcmp(
            (*search_clause).search_seq_column,
            (*cycle_clause).cycle_path_column,
        ) == 0
        {
            let _ = parser_errposition(pstate, (*search_clause).location);
            ereport!(
                ERROR,
                errmsg!("search sequence column name and cycle path column name are the same")
            );
        }
    }
}

/*
 * Compute derived fields of a CTE, given the transformed output targetlist
 *
 * For a nonrecursive CTE, this is called after transforming the CTE's query.
 * For a recursive CTE, we call it after transforming the non-recursive term,
 * and pass the targetlist emitted by the non-recursive term only.
 *
 * Note: in the recursive case, the passed pstate is actually the one being
 * used to analyze the CTE's query, so it is one level lower down than in
 * the nonrecursive case.  This doesn't matter since we only use it for
 * error message context anyway.
 */
pub unsafe fn analyzeCTETargetList(pstate: *mut ParseState, cte: *mut CommonTableExpr, tlist: *mut List) {
    let numaliases: c_int;
    let mut varattno: c_int;
    let mut tlistitem: *mut ListCell;

    /* Not done already ... */
    Assert!((*cte).ctecolnames == NIL);

    /*
     * We need to determine column names, types, and collations.  The alias
     * column names override anything coming from the query itself.  (Note:
     * the SQL spec says that the alias list must be empty or exactly as long
     * as the output column set; but we allow it to be shorter for consistency
     * with Alias handling.)
     */
    (*cte).ctecolnames = copyObject((*cte).aliascolnames);
    (*cte).ctecoltypes = NIL;
    (*cte).ctecoltypmods = NIL;
    (*cte).ctecolcollations = NIL;
    numaliases = list_length((*cte).aliascolnames);
    varattno = 0;
    foreach!(tlistitem, tlist, {
        let te: *mut TargetEntry = lfirst(current_cell!(tlistitem)) as *mut TargetEntry;
        let mut coltype: Oid;
        let mut coltypmod: int32;
        let mut colcoll: Oid;

        if (*te).resjunk {
            continue;
        }
        varattno += 1;
        Assert!(varattno == (*te).resno as c_int);
        if varattno > numaliases {
            let attrname: *mut c_char;

            attrname = pstrdup((*te).resname);
            (*cte).ctecolnames = lappend((*cte).ctecolnames, makeString(attrname) as *mut c_void);
        }
        coltype = exprType((*te).expr as *mut Node);
        coltypmod = exprTypmod((*te).expr as *mut Node);
        colcoll = exprCollation((*te).expr as *mut Node);

        /*
         * If the CTE is recursive, force the exposed column type of any
         * "unknown" column to "text".  We must deal with this here because
         * we're called on the non-recursive term before there's been any
         * attempt to force unknown output columns to some other type.  We
         * have to resolve unknowns before looking at the recursive term.
         *
         * The column might contain 'foo' COLLATE "bar", so don't override
         * collation if it's already set.
         */
        if (*cte).cterecursive && coltype == UNKNOWNOID {
            coltype = TEXTOID;
            coltypmod = -1; /* should be -1 already, but be sure */
            if !OidIsValid(colcoll) {
                colcoll = DEFAULT_COLLATION_OID;
            }
        }
        (*cte).ctecoltypes = lappend_oid((*cte).ctecoltypes, coltype);
        (*cte).ctecoltypmods = lappend_int((*cte).ctecoltypmods, coltypmod);
        (*cte).ctecolcollations = lappend_oid((*cte).ctecolcollations, colcoll);
    });
    if varattno < numaliases {
        let _ = parser_errposition(pstate, (*cte).location);
        ereport!(
            ERROR,
            errmsg!(
                "WITH query \"{}\" has {} columns available but {} columns specified",
                cstr((*cte).ctename),
                varattno,
                numaliases
            )
        );
    }
}

/*
 * Identify the cross-references of a list of WITH RECURSIVE items,
 * and sort into an order that has no forward references.
 */
unsafe fn makeDependencyGraph(cstate: *mut CteState) {
    let mut i: c_int;

    i = 0;
    while i < (*cstate).numitems {
        let cte: *mut CommonTableExpr = (*(*cstate).items.add(i as usize)).cte;

        (*cstate).curitem = i;
        (*cstate).innerwiths = NIL;
        makeDependencyGraphWalker((*cte).ctequery, cstate);
        Assert!((*cstate).innerwiths == NIL);
        i += 1;
    }

    TopologicalSort((*cstate).pstate, (*cstate).items, (*cstate).numitems);
}

/*
 * Tree walker function to detect cross-references and self-references of the
 * CTEs in a WITH RECURSIVE list.
 */
unsafe fn makeDependencyGraphWalker(node: *mut Node, cstate: *mut CteState) -> bool {
    if node.is_null() {
        return false;
    }
    if IsA!(node, T_RangeVar) {
        let rv: *mut RangeVar = node as *mut RangeVar;

        /* If unqualified name, might be a CTE reference */
        if (*rv).schemaname.is_null() {
            let mut lc: *mut ListCell;
            let mut i: c_int;

            /* ... but first see if it's captured by an inner WITH */
            foreach!(lc, (*cstate).innerwiths, {
                let withlist: *mut List = lfirst(current_cell!(lc)) as *mut List;
                let mut lc2: *mut ListCell;

                foreach!(lc2, withlist, {
                    let cte: *mut CommonTableExpr =
                        lfirst(current_cell!(lc2)) as *mut CommonTableExpr;

                    if strcmp((*rv).relname, (*cte).ctename) == 0 {
                        return false; /* yes, so bail out */
                    }
                });
            });

            /* No, could be a reference to the query level we are working on */
            i = 0;
            while i < (*cstate).numitems {
                let cte: *mut CommonTableExpr = (*(*cstate).items.add(i as usize)).cte;

                if strcmp((*rv).relname, (*cte).ctename) == 0 {
                    let myindex: c_int = (*cstate).curitem;

                    if i != myindex {
                        /* Add cross-item dependency */
                        (*(*cstate).items.add(myindex as usize)).depends_on = bms_add_member(
                            (*(*cstate).items.add(myindex as usize)).depends_on,
                            (*(*cstate).items.add(i as usize)).id,
                        );
                    } else {
                        /* Found out this one is self-referential */
                        (*cte).cterecursive = true;
                    }
                    break;
                }
                i += 1;
            }
        }
        return false;
    }
    if IsA!(node, T_SelectStmt) {
        let stmt: *mut SelectStmt = node as *mut SelectStmt;

        if !(*stmt).withClause.is_null() {
            /* Examine the WITH clause and the SelectStmt */
            WalkInnerWith(node, (*stmt).withClause, cstate);
            /* We're done examining the SelectStmt */
            return false;
        }
        /* if no WITH clause, just fall through for normal processing */
    } else if IsA!(node, T_InsertStmt) {
        let stmt: *mut crate::nodes::parsenodes::InsertStmt =
            node as *mut crate::nodes::parsenodes::InsertStmt;

        if !(*stmt).withClause.is_null() {
            /* Examine the WITH clause and the InsertStmt */
            WalkInnerWith(node, (*stmt).withClause, cstate);
            /* We're done examining the InsertStmt */
            return false;
        }
        /* if no WITH clause, just fall through for normal processing */
    } else if IsA!(node, T_DeleteStmt) {
        let stmt: *mut crate::nodes::parsenodes::DeleteStmt =
            node as *mut crate::nodes::parsenodes::DeleteStmt;

        if !(*stmt).withClause.is_null() {
            /* Examine the WITH clause and the DeleteStmt */
            WalkInnerWith(node, (*stmt).withClause, cstate);
            /* We're done examining the DeleteStmt */
            return false;
        }
        /* if no WITH clause, just fall through for normal processing */
    } else if IsA!(node, T_UpdateStmt) {
        let stmt: *mut crate::nodes::parsenodes::UpdateStmt =
            node as *mut crate::nodes::parsenodes::UpdateStmt;

        if !(*stmt).withClause.is_null() {
            /* Examine the WITH clause and the UpdateStmt */
            WalkInnerWith(node, (*stmt).withClause, cstate);
            /* We're done examining the UpdateStmt */
            return false;
        }
        /* if no WITH clause, just fall through for normal processing */
    } else if IsA!(node, T_MergeStmt) {
        let stmt: *mut crate::nodes::parsenodes::MergeStmt =
            node as *mut crate::nodes::parsenodes::MergeStmt;

        if !(*stmt).withClause.is_null() {
            /* Examine the WITH clause and the MergeStmt */
            WalkInnerWith(node, (*stmt).withClause, cstate);
            /* We're done examining the MergeStmt */
            return false;
        }
        /* if no WITH clause, just fall through for normal processing */
    } else if IsA!(node, T_WithClause) {
        /*
         * Prevent raw_expression_tree_walker from recursing directly into a
         * WITH clause.  We need that to happen only under the control of the
         * code above.
         */
        return false;
    }
    raw_expression_tree_walker(node, Some(makeDependencyGraphWalkerCb), cstate as *mut c_void)
}

/* trampoline matching tree_walker_callback for makeDependencyGraphWalker */
unsafe fn makeDependencyGraphWalkerCb(node: *mut Node, context: *mut c_void) -> bool {
    makeDependencyGraphWalker(node, context as *mut CteState)
}

/*
 * makeDependencyGraphWalker's recursion into a statement having a WITH clause.
 *
 * This subroutine is concerned with updating the innerwiths list correctly
 * based on the visibility rules for CTE names.
 */
unsafe fn WalkInnerWith(stmt: *mut Node, withClause: *mut WithClause, cstate: *mut CteState) {
    let mut lc: *mut ListCell;

    if (*withClause).recursive {
        /*
         * In the RECURSIVE case, all query names of the WITH are visible to
         * all WITH items as well as the main query.  So push them all on,
         * process, pop them all off.
         */
        (*cstate).innerwiths = lcons((*withClause).ctes as *mut c_void, (*cstate).innerwiths);
        foreach!(lc, (*withClause).ctes, {
            let cte: *mut CommonTableExpr = lfirst(current_cell!(lc)) as *mut CommonTableExpr;

            makeDependencyGraphWalker((*cte).ctequery, cstate);
        });
        raw_expression_tree_walker(stmt, Some(makeDependencyGraphWalkerCb), cstate as *mut c_void);
        (*cstate).innerwiths = list_delete_first((*cstate).innerwiths);
    } else {
        /*
         * In the non-RECURSIVE case, query names are visible to the WITH
         * items after them and to the main query.
         */
        (*cstate).innerwiths = lcons(NIL as *mut c_void, (*cstate).innerwiths);
        foreach!(lc, (*withClause).ctes, {
            let cte: *mut CommonTableExpr = lfirst(current_cell!(lc)) as *mut CommonTableExpr;
            let cell1: *mut ListCell;

            makeDependencyGraphWalker((*cte).ctequery, cstate);
            /* note that recursion could mutate innerwiths list */
            cell1 = list_head((*cstate).innerwiths);
            *lfirst_mut(cell1) = lappend(lfirst(cell1) as *mut List, cte as *mut c_void) as *mut c_void;
        });
        raw_expression_tree_walker(stmt, Some(makeDependencyGraphWalkerCb), cstate as *mut c_void);
        (*cstate).innerwiths = list_delete_first((*cstate).innerwiths);
    }
}

/*
 * Sort by dependencies, using a standard topological sort operation
 */
unsafe fn TopologicalSort(pstate: *mut ParseState, items: *mut CteItem, numitems: c_int) {
    let mut i: c_int;
    let mut j: c_int;

    /* for each position in sequence ... */
    i = 0;
    while i < numitems {
        /* ... scan the remaining items to find one that has no dependencies */
        j = i;
        while j < numitems {
            if bms_is_empty((*items.add(j as usize)).depends_on) {
                break;
            }
            j += 1;
        }

        /* if we didn't find one, the dependency graph has a cycle */
        if j >= numitems {
            let _ = parser_errposition(pstate, (*(*items.add(i as usize)).cte).location);
            ereport!(
                ERROR,
                errmsg!("mutual recursion between WITH items is not implemented")
            );
        }

        /*
         * Found one.  Move it to front and remove it from every other item's
         * dependencies.
         */
        if i != j {
            let tmp: CteItem;

            tmp = core::ptr::read(items.add(i as usize));
            core::ptr::write(items.add(i as usize), core::ptr::read(items.add(j as usize)));
            core::ptr::write(items.add(j as usize), tmp);
        }

        /*
         * Items up through i are known to have no dependencies left, so we
         * can skip them in this loop.
         */
        j = i + 1;
        while j < numitems {
            (*items.add(j as usize)).depends_on = bms_del_member(
                (*items.add(j as usize)).depends_on,
                (*items.add(i as usize)).id,
            );
            j += 1;
        }
        i += 1;
    }
}

/*
 * Check that recursive queries are well-formed.
 */
unsafe fn checkWellFormedRecursion(cstate: *mut CteState) {
    let mut i: c_int;

    i = 0;
    while i < (*cstate).numitems {
        let cte: *mut CommonTableExpr = (*(*cstate).items.add(i as usize)).cte;
        let stmt: *mut SelectStmt = (*cte).ctequery as *mut SelectStmt;

        Assert!(!IsA!(stmt, T_Query)); /* not analyzed yet */

        /* Ignore items that weren't found to be recursive */
        if !(*cte).cterecursive {
            i += 1;
            continue;
        }

        /* Must be a SELECT statement */
        if !IsA!(stmt, T_SelectStmt) {
            let _ = parser_errposition((*cstate).pstate, (*cte).location);
            ereport!(
                ERROR,
                errmsg!(
                    "recursive query \"{}\" must not contain data-modifying statements",
                    cstr((*cte).ctename)
                )
            );
        }

        /* Must have top-level UNION */
        if (*stmt).op != SetOperation::SETOP_UNION {
            let _ = parser_errposition((*cstate).pstate, (*cte).location);
            ereport!(
                ERROR,
                errmsg!(
                    "recursive query \"{}\" does not have the form non-recursive-term UNION [ALL] recursive-term",
                    cstr((*cte).ctename)
                )
            );
        }

        /*
         * Really, we should insist that there not be a top-level WITH, since
         * syntactically that would enclose the UNION.  However, we've not
         * done so in the past and it's probably too late to change.  Settle
         * for insisting that WITH not contain a self-reference.  Test this
         * before examining the UNION arms, to avoid issuing confusing errors
         * in such cases.
         */
        if !(*stmt).withClause.is_null() {
            (*cstate).curitem = i;
            (*cstate).innerwiths = NIL;
            (*cstate).selfrefcount = 0;
            (*cstate).context = RECURSION_SUBLINK;
            checkWellFormedRecursionWalker((*(*stmt).withClause).ctes as *mut Node, cstate);
            Assert!((*cstate).innerwiths == NIL);
        }

        /*
         * Disallow ORDER BY and similar decoration atop the UNION. These
         * don't make sense because it's impossible to figure out what they
         * mean when we have only part of the recursive query's results. (If
         * we did allow them, we'd have to check for recursive references
         * inside these subtrees.  As for WITH, we have to do this before
         * examining the UNION arms, to avoid issuing confusing errors if
         * there is a recursive reference here.)
         */
        if !(*stmt).sortClause.is_null() {
            let _ = parser_errposition(
                (*cstate).pstate,
                exprLocation((*stmt).sortClause as *mut Node),
            );
            ereport!(
                ERROR,
                errmsg!("ORDER BY in a recursive query is not implemented")
            );
        }
        if !(*stmt).limitOffset.is_null() {
            let _ = parser_errposition((*cstate).pstate, exprLocation((*stmt).limitOffset));
            ereport!(ERROR, errmsg!("OFFSET in a recursive query is not implemented"));
        }
        if !(*stmt).limitCount.is_null() {
            let _ = parser_errposition((*cstate).pstate, exprLocation((*stmt).limitCount));
            ereport!(ERROR, errmsg!("LIMIT in a recursive query is not implemented"));
        }
        if !(*stmt).lockingClause.is_null() {
            let _ = parser_errposition(
                (*cstate).pstate,
                exprLocation((*stmt).lockingClause as *mut Node),
            );
            ereport!(
                ERROR,
                errmsg!("FOR UPDATE/SHARE in a recursive query is not implemented")
            );
        }

        /*
         * Now we can get on with checking the UNION operands themselves.
         *
         * The left-hand operand mustn't contain a self-reference at all.
         */
        (*cstate).curitem = i;
        (*cstate).innerwiths = NIL;
        (*cstate).selfrefcount = 0;
        (*cstate).context = RECURSION_NONRECURSIVETERM;
        checkWellFormedRecursionWalker((*stmt).larg as *mut Node, cstate);
        Assert!((*cstate).innerwiths == NIL);

        /* Right-hand operand should contain one reference in a valid place */
        (*cstate).curitem = i;
        (*cstate).innerwiths = NIL;
        (*cstate).selfrefcount = 0;
        (*cstate).context = RECURSION_OK;
        checkWellFormedRecursionWalker((*stmt).rarg as *mut Node, cstate);
        Assert!((*cstate).innerwiths == NIL);
        if (*cstate).selfrefcount != 1 {
            /* shouldn't happen */
            elog!(ERROR, "missing recursive reference");
        }

        i += 1;
    }
}

/*
 * Tree walker function to detect invalid self-references in a recursive query.
 */
unsafe fn checkWellFormedRecursionWalker(node: *mut Node, cstate: *mut CteState) -> bool {
    let save_context: RecursionContext = (*cstate).context;

    if node.is_null() {
        return false;
    }
    if IsA!(node, T_RangeVar) {
        let rv: *mut RangeVar = node as *mut RangeVar;

        /* If unqualified name, might be a CTE reference */
        if (*rv).schemaname.is_null() {
            let mut lc: *mut ListCell;
            let mycte: *mut CommonTableExpr;

            /* ... but first see if it's captured by an inner WITH */
            foreach!(lc, (*cstate).innerwiths, {
                let withlist: *mut List = lfirst(current_cell!(lc)) as *mut List;
                let mut lc2: *mut ListCell;

                foreach!(lc2, withlist, {
                    let cte: *mut CommonTableExpr =
                        lfirst(current_cell!(lc2)) as *mut CommonTableExpr;

                    if strcmp((*rv).relname, (*cte).ctename) == 0 {
                        return false; /* yes, so bail out */
                    }
                });
            });

            /* No, could be a reference to the query level we are working on */
            mycte = (*(*cstate).items.add((*cstate).curitem as usize)).cte;
            if strcmp((*rv).relname, (*mycte).ctename) == 0 {
                /* Found a recursive reference to the active query */
                if (*cstate).context != RECURSION_OK {
                    let _ = parser_errposition((*cstate).pstate, (*rv).location);
                    ereport!(
                        ERROR,
                        errmsg!(
                            "{}",
                            format!(
                                "{}",
                                recursion_errormsgs[(*cstate).context as usize]
                                    .replace("{}", &cstr((*mycte).ctename))
                            )
                        )
                    );
                }
                /* Count references */
                (*cstate).selfrefcount += 1;
                if (*cstate).selfrefcount > 1 {
                    let _ = parser_errposition((*cstate).pstate, (*rv).location);
                    ereport!(
                        ERROR,
                        errmsg!(
                            "recursive reference to query \"{}\" must not appear more than once",
                            cstr((*mycte).ctename)
                        )
                    );
                }
            }
        }
        return false;
    }
    if IsA!(node, T_SelectStmt) {
        let stmt: *mut SelectStmt = node as *mut SelectStmt;
        let mut lc: *mut ListCell;

        if !(*stmt).withClause.is_null() {
            if (*(*stmt).withClause).recursive {
                /*
                 * In the RECURSIVE case, all query names of the WITH are
                 * visible to all WITH items as well as the main query. So
                 * push them all on, process, pop them all off.
                 */
                (*cstate).innerwiths =
                    lcons((*(*stmt).withClause).ctes as *mut c_void, (*cstate).innerwiths);
                foreach!(lc, (*(*stmt).withClause).ctes, {
                    let cte: *mut CommonTableExpr =
                        lfirst(current_cell!(lc)) as *mut CommonTableExpr;

                    checkWellFormedRecursionWalker((*cte).ctequery, cstate);
                });
                checkWellFormedSelectStmt(stmt, cstate);
                (*cstate).innerwiths = list_delete_first((*cstate).innerwiths);
            } else {
                /*
                 * In the non-RECURSIVE case, query names are visible to the
                 * WITH items after them and to the main query.
                 */
                (*cstate).innerwiths = lcons(NIL as *mut c_void, (*cstate).innerwiths);
                foreach!(lc, (*(*stmt).withClause).ctes, {
                    let cte: *mut CommonTableExpr =
                        lfirst(current_cell!(lc)) as *mut CommonTableExpr;
                    let cell1: *mut ListCell;

                    checkWellFormedRecursionWalker((*cte).ctequery, cstate);
                    /* note that recursion could mutate innerwiths list */
                    cell1 = list_head((*cstate).innerwiths);
                    *lfirst_mut(cell1) =
                        lappend(lfirst(cell1) as *mut List, cte as *mut c_void) as *mut c_void;
                });
                checkWellFormedSelectStmt(stmt, cstate);
                (*cstate).innerwiths = list_delete_first((*cstate).innerwiths);
            }
        } else {
            checkWellFormedSelectStmt(stmt, cstate);
        }
        /* We're done examining the SelectStmt */
        return false;
    }
    if IsA!(node, T_WithClause) {
        /*
         * Prevent raw_expression_tree_walker from recursing directly into a
         * WITH clause.  We need that to happen only under the control of the
         * code above.
         */
        return false;
    }
    if IsA!(node, T_JoinExpr) {
        let j: *mut JoinExpr = node as *mut JoinExpr;

        match (*j).jointype {
            JoinType::JOIN_INNER => {
                checkWellFormedRecursionWalker((*j).larg, cstate);
                checkWellFormedRecursionWalker((*j).rarg, cstate);
                checkWellFormedRecursionWalker((*j).quals, cstate);
            }
            JoinType::JOIN_LEFT => {
                checkWellFormedRecursionWalker((*j).larg, cstate);
                if save_context == RECURSION_OK {
                    (*cstate).context = RECURSION_OUTERJOIN;
                }
                checkWellFormedRecursionWalker((*j).rarg, cstate);
                (*cstate).context = save_context;
                checkWellFormedRecursionWalker((*j).quals, cstate);
            }
            JoinType::JOIN_FULL => {
                if save_context == RECURSION_OK {
                    (*cstate).context = RECURSION_OUTERJOIN;
                }
                checkWellFormedRecursionWalker((*j).larg, cstate);
                checkWellFormedRecursionWalker((*j).rarg, cstate);
                (*cstate).context = save_context;
                checkWellFormedRecursionWalker((*j).quals, cstate);
            }
            JoinType::JOIN_RIGHT => {
                if save_context == RECURSION_OK {
                    (*cstate).context = RECURSION_OUTERJOIN;
                }
                checkWellFormedRecursionWalker((*j).larg, cstate);
                (*cstate).context = save_context;
                checkWellFormedRecursionWalker((*j).rarg, cstate);
                checkWellFormedRecursionWalker((*j).quals, cstate);
            }
            _ => {
                elog!(ERROR, "unrecognized join type: {}", (*j).jointype as c_int);
            }
        }
        return false;
    }
    if IsA!(node, T_SubLink) {
        let sl: *mut SubLink = node as *mut SubLink;

        /*
         * we intentionally override outer context, since subquery is
         * independent
         */
        (*cstate).context = RECURSION_SUBLINK;
        checkWellFormedRecursionWalker((*sl).subselect, cstate);
        (*cstate).context = save_context;
        checkWellFormedRecursionWalker((*sl).testexpr, cstate);
        return false;
    }
    raw_expression_tree_walker(node, Some(checkWellFormedRecursionWalkerCb), cstate as *mut c_void)
}

/* trampoline matching tree_walker_callback for checkWellFormedRecursionWalker */
unsafe fn checkWellFormedRecursionWalkerCb(node: *mut Node, context: *mut c_void) -> bool {
    checkWellFormedRecursionWalker(node, context as *mut CteState)
}

/*
 * subroutine for checkWellFormedRecursionWalker: process a SelectStmt
 * without worrying about its WITH clause
 */
unsafe fn checkWellFormedSelectStmt(stmt: *mut SelectStmt, cstate: *mut CteState) {
    let save_context: RecursionContext = (*cstate).context;

    if save_context != RECURSION_OK {
        /* just recurse without changing state */
        raw_expression_tree_walker(
            stmt as *mut Node,
            Some(checkWellFormedRecursionWalkerCb),
            cstate as *mut c_void,
        );
    } else {
        match (*stmt).op {
            SetOperation::SETOP_NONE | SetOperation::SETOP_UNION => {
                raw_expression_tree_walker(
                    stmt as *mut Node,
                    Some(checkWellFormedRecursionWalkerCb),
                    cstate as *mut c_void,
                );
            }
            SetOperation::SETOP_INTERSECT => {
                if (*stmt).all {
                    (*cstate).context = RECURSION_INTERSECT;
                }
                checkWellFormedRecursionWalker((*stmt).larg as *mut Node, cstate);
                checkWellFormedRecursionWalker((*stmt).rarg as *mut Node, cstate);
                (*cstate).context = save_context;
                checkWellFormedRecursionWalker((*stmt).sortClause as *mut Node, cstate);
                checkWellFormedRecursionWalker((*stmt).limitOffset, cstate);
                checkWellFormedRecursionWalker((*stmt).limitCount, cstate);
                checkWellFormedRecursionWalker((*stmt).lockingClause as *mut Node, cstate);
                /* stmt->withClause is intentionally ignored here */
            }
            SetOperation::SETOP_EXCEPT => {
                if (*stmt).all {
                    (*cstate).context = RECURSION_EXCEPT;
                }
                checkWellFormedRecursionWalker((*stmt).larg as *mut Node, cstate);
                (*cstate).context = RECURSION_EXCEPT;
                checkWellFormedRecursionWalker((*stmt).rarg as *mut Node, cstate);
                (*cstate).context = save_context;
                checkWellFormedRecursionWalker((*stmt).sortClause as *mut Node, cstate);
                checkWellFormedRecursionWalker((*stmt).limitOffset, cstate);
                checkWellFormedRecursionWalker((*stmt).limitCount, cstate);
                checkWellFormedRecursionWalker((*stmt).lockingClause as *mut Node, cstate);
                /* stmt->withClause is intentionally ignored here */
            }
            #[allow(unreachable_patterns)]
            _ => {
                elog!(ERROR, "unrecognized set op: {}", (*stmt).op as c_int);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Local helpers used by this file's translation.
// ---------------------------------------------------------------------------

// TODO(pg-port): real parser_errposition lives in parser/parse_node.c
// (crate::parser::parse_node::parser_errposition); re-declared locally to avoid
// importing the symbol while it is still being wired up across the parser.
unsafe fn parser_errposition(pstate: *mut ParseState, location: ParseLoc) -> c_int {
    crate::parser::parse_node::parser_errposition(pstate, location)
}

// TODO(pg-port): real copyObject lives in nodes/copyfuncs.rs (deferred/unwired).
unsafe fn copyObject<T>(node: *const T) -> *mut T {
    node as *mut T
}

/// Helper to render a possibly-NULL C string for error messages.
unsafe fn cstr(s: *const c_char) -> std::string::String {
    if s.is_null() {
        std::string::String::new()
    } else {
        core::ffi::CStr::from_ptr(s).to_string_lossy().into_owned()
    }
}
