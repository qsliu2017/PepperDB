//! clausesel.rs
//!   Routines to compute clause selectivities
//!
//! Translated 1:1 from postgres/src/backend/optimizer/path/clausesel.c
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!	  src/backend/optimizer/path/clausesel.c
//!
//! #include mapping:
//!   "postgres.h"                  -> crate::prelude::*
//!   "nodes/nodeFuncs.h"           -> inline is_opclause/is_andclause/is_orclause/
//!                                    is_notclause/is_funcclause/get_leftop/get_rightop/
//!                                    get_notclausearg helpers reproduced here
//!                                    (nodeFuncs not yet ported as a unit; matches the
//!                                    private copies in restrictinfo.rs / tidpath.rs)
//!   "optimizer/clauses.h"         -> is_pseudo_constant_clause /
//!                                    is_pseudo_constant_clause_relids (clauses.c not yet
//!                                    ported -> local STUBs below)
//!   "optimizer/optimizer.h"       -> estimate_expression_value / NumRelids
//!                                    (selfuncs.c / clauses.c not yet ported -> STUBs)
//!   "optimizer/pathnode.h"        -> find_base_rel (relnode.c not yet ported -> STUB)
//!   "optimizer/plancat.h"         -> (nothing referenced directly)
//!   "statistics/statistics.h"     -> statext_clauselist_selectivity
//!                                    (statistics not yet ported -> STUB)
//!   "utils/fmgroids.h"            -> F_SCALARLTSEL/F_SCALARLESEL/F_SCALARGTSEL/
//!                                    F_SCALARGESEL (fmgroids not yet ported -> local
//!                                    consts below)
//!   "utils/lsyscache.h"           -> get_oprrest (lsyscache.c not yet ported -> STUB)
//!   "utils/selfuncs.h"            -> restriction_selectivity / join_selectivity /
//!                                    function_selectivity / boolvarsel / scalararraysel /
//!                                    rowcomparesel / nulltestsel / booltestsel /
//!                                    DEFAULT_INEQ_SEL / DEFAULT_RANGE_INEQ_SEL
//!                                    (selfuncs.c not yet ported -> STUBs / local consts)

use crate::prelude::*;
use core::ffi::c_void;

use crate::nodes::equalfuncs::equal;
use crate::nodes::nodes::{JoinType, Node, Selectivity};
use crate::nodes::nodes::JoinType::JOIN_INNER;
use crate::nodes::parsenodes::RTEKind::RTE_RELATION;
use crate::nodes::pathnodes::{PlannerInfo, RelOptInfo, Relids, RestrictInfo, SpecialJoinInfo};
use crate::nodes::pg_list::{linitial, lfirst, list_length, lsecond, List, NIL};
use crate::nodes::primnodes::{
    BoolExpr, BooleanTest, CoerceToDomain, Const, CurrentOfExpr, Expr, FuncExpr, NullTest, OpExpr,
    RelabelType, RowCompareExpr, ScalarArrayOpExpr, Var, AND_EXPR, NOT_EXPR, OR_EXPR,
};
use crate::nodes::bitmapset::{bms_get_singleton_member, bms_is_empty, bms_is_member};
use crate::postgres::DatumGetBool;
use crate::postgres_ext::Oid;
use crate::{current_cell, foreach, Assert, IsA};

/*
 * ---------------------------------------------------------------------------
 * Inline clause-shape helpers (nodes/nodeFuncs.h).
 *
 * Reproduced here because nodeFuncs is not yet translated as its own unit
 * (restrictinfo.rs / tidpath.rs keep private copies for the same reason).
 * ---------------------------------------------------------------------------
 */

/// `is_opclause(clause)` -- clause is non-NULL and an OpExpr.
#[inline]
unsafe fn is_opclause(clause: *const c_void) -> bool {
    !clause.is_null() && IsA!(clause, T_OpExpr)
}

/// `is_funcclause(clause)`.
#[inline]
unsafe fn is_funcclause(clause: *const c_void) -> bool {
    !clause.is_null() && IsA!(clause, T_FuncExpr)
}

/// `is_andclause(clause)`.
#[inline]
unsafe fn is_andclause(clause: *const c_void) -> bool {
    !clause.is_null()
        && IsA!(clause, T_BoolExpr)
        && (*(clause as *const BoolExpr)).boolop == AND_EXPR
}

/// `is_orclause(clause)`.
#[inline]
unsafe fn is_orclause(clause: *const c_void) -> bool {
    !clause.is_null()
        && IsA!(clause, T_BoolExpr)
        && (*(clause as *const BoolExpr)).boolop == OR_EXPR
}

/// `is_notclause(clause)`.
#[inline]
unsafe fn is_notclause(clause: *const c_void) -> bool {
    !clause.is_null()
        && IsA!(clause, T_BoolExpr)
        && (*(clause as *const BoolExpr)).boolop == NOT_EXPR
}

/// `get_notclausearg(notclause)` -- the lone argument of a NOT clause.
#[inline]
unsafe fn get_notclausearg(notclause: *const c_void) -> *mut Expr {
    linitial((*(notclause as *const BoolExpr)).args) as *mut Expr
}

/// `get_leftop(clause)` -- left arg of a binary opclause (or only arg of unary).
#[inline]
unsafe fn get_leftop(clause: *const c_void) -> *mut Node {
    let expr = clause as *const OpExpr;
    if !(*expr).args.is_null() {
        linitial((*expr).args) as *mut Node
    } else {
        core::ptr::null_mut()
    }
}

/// `get_rightop(clause)` -- right arg of a binary opclause (NULL if unary).
#[inline]
unsafe fn get_rightop(clause: *const c_void) -> *mut Node {
    let expr = clause as *const OpExpr;
    if list_length((*expr).args) >= 2 {
        lsecond((*expr).args) as *mut Node
    } else {
        core::ptr::null_mut()
    }
}

/*
 * ---------------------------------------------------------------------------
 * Selectivity default constants (utils/selfuncs.h).
 * ---------------------------------------------------------------------------
 */

// TODO(pg-port): real DEFAULT_INEQ_SEL lives in utils/adt/selfuncs.rs (selfuncs.h)
const DEFAULT_INEQ_SEL: Selectivity = 0.3333333333333333;
// TODO(pg-port): real DEFAULT_RANGE_INEQ_SEL lives in utils/adt/selfuncs.rs (selfuncs.h)
const DEFAULT_RANGE_INEQ_SEL: Selectivity = 0.005;

/*
 * ---------------------------------------------------------------------------
 * Restriction-selectivity estimator OIDs (utils/fmgroids.h).
 *
 * These are the get_oprrest() values for the scalar inequality estimators.
 * ---------------------------------------------------------------------------
 */

// TODO(pg-port): real F_SCALARLTSEL lives in utils/fmgroids.rs (generated fmgroids.h)
const F_SCALARLTSEL: Oid = 103;
// TODO(pg-port): real F_SCALARLESEL lives in utils/fmgroids.rs (generated fmgroids.h)
const F_SCALARLESEL: Oid = 336;
// TODO(pg-port): real F_SCALARGTSEL lives in utils/fmgroids.rs (generated fmgroids.h)
const F_SCALARGTSEL: Oid = 104;
// TODO(pg-port): real F_SCALARGESEL lives in utils/fmgroids.rs (generated fmgroids.h)
const F_SCALARGESEL: Oid = 337;

/*
 * ---------------------------------------------------------------------------
 * STUBs for not-yet-ported planner / selectivity machinery.
 * ---------------------------------------------------------------------------
 */

// TODO(pg-port): real statext_clauselist_selectivity lives in statistics/extended_stats.rs
unsafe fn statext_clauselist_selectivity(
    _root: *mut PlannerInfo,
    _clauses: *mut List,
    _varRelid: c_int,
    _jointype: JoinType,
    _sjinfo: *mut SpecialJoinInfo,
    _rel: *mut RelOptInfo,
    _estimatedclauses: *mut *mut crate::nodes::bitmapset::Bitmapset,
    _is_or: bool,
) -> Selectivity {
    unimplemented!()
}

// TODO(pg-port): real find_base_rel lives in optimizer/util/relnode.rs
unsafe fn find_base_rel(_root: *mut PlannerInfo, _relid: c_int) -> *mut RelOptInfo {
    unimplemented!()
}

// TODO(pg-port): real estimate_expression_value lives in optimizer/util/clauses.rs
unsafe fn estimate_expression_value(_root: *mut PlannerInfo, _node: *mut Node) -> *mut Node {
    unimplemented!()
}

// TODO(pg-port): real NumRelids lives in optimizer/util/clauses.rs
unsafe fn NumRelids(_root: *mut PlannerInfo, _clause: *mut Node) -> c_int {
    unimplemented!()
}

// TODO(pg-port): real is_pseudo_constant_clause lives in optimizer/util/clauses.rs
unsafe fn is_pseudo_constant_clause(_clause: *mut Node) -> bool {
    unimplemented!()
}

// TODO(pg-port): real is_pseudo_constant_clause_relids lives in optimizer/util/clauses.rs
unsafe fn is_pseudo_constant_clause_relids(_clause: *mut Node, _relids: Relids) -> bool {
    unimplemented!()
}

// TODO(pg-port): real get_oprrest lives in utils/cache/lsyscache.rs
unsafe fn get_oprrest(_opno: Oid) -> Oid {
    unimplemented!()
}

// TODO(pg-port): real restriction_selectivity lives in utils/adt/plancat.rs
unsafe fn restriction_selectivity(
    _root: *mut PlannerInfo,
    _operatorid: Oid,
    _args: *mut List,
    _inputcollid: Oid,
    _varRelid: c_int,
) -> Selectivity {
    unimplemented!()
}

// TODO(pg-port): real join_selectivity lives in utils/adt/plancat.rs
unsafe fn join_selectivity(
    _root: *mut PlannerInfo,
    _operatorid: Oid,
    _args: *mut List,
    _inputcollid: Oid,
    _jointype: JoinType,
    _sjinfo: *mut SpecialJoinInfo,
) -> Selectivity {
    unimplemented!()
}

// TODO(pg-port): real function_selectivity lives in utils/adt/plancat.rs
unsafe fn function_selectivity(
    _root: *mut PlannerInfo,
    _funcid: Oid,
    _args: *mut List,
    _inputcollid: Oid,
    _is_join: bool,
    _varRelid: c_int,
    _jointype: JoinType,
    _sjinfo: *mut SpecialJoinInfo,
) -> Selectivity {
    unimplemented!()
}

// TODO(pg-port): real boolvarsel lives in utils/adt/selfuncs.rs
unsafe fn boolvarsel(_root: *mut PlannerInfo, _arg: *mut Node, _varRelid: c_int) -> Selectivity {
    unimplemented!()
}

// TODO(pg-port): real scalararraysel lives in utils/adt/selfuncs.rs
unsafe fn scalararraysel(
    _root: *mut PlannerInfo,
    _clause: *mut ScalarArrayOpExpr,
    _is_join_clause: bool,
    _varRelid: c_int,
    _jointype: JoinType,
    _sjinfo: *mut SpecialJoinInfo,
) -> Selectivity {
    unimplemented!()
}

// TODO(pg-port): real rowcomparesel lives in utils/adt/selfuncs.rs
unsafe fn rowcomparesel(
    _root: *mut PlannerInfo,
    _clause: *mut RowCompareExpr,
    _varRelid: c_int,
    _jointype: JoinType,
    _sjinfo: *mut SpecialJoinInfo,
) -> Selectivity {
    unimplemented!()
}

// TODO(pg-port): real nulltestsel lives in utils/adt/selfuncs.rs
unsafe fn nulltestsel(
    _root: *mut PlannerInfo,
    _nulltesttype: crate::nodes::primnodes::NullTestType,
    _arg: *mut Node,
    _varRelid: c_int,
    _jointype: JoinType,
    _sjinfo: *mut SpecialJoinInfo,
) -> Selectivity {
    unimplemented!()
}

// TODO(pg-port): real booltestsel lives in utils/adt/selfuncs.rs
unsafe fn booltestsel(
    _root: *mut PlannerInfo,
    _booltesttype: crate::nodes::primnodes::BoolTestType,
    _arg: *mut Node,
    _varRelid: c_int,
    _jointype: JoinType,
    _sjinfo: *mut SpecialJoinInfo,
) -> Selectivity {
    unimplemented!()
}

/*
 * Data structure for accumulating info about possible range-query
 * clause pairs in clauselist_selectivity.
 */
#[repr(C)]
pub struct RangeQueryClause {
    pub next: *mut RangeQueryClause, /* next in linked list */
    pub var: *mut Node,              /* The common variable of the clauses */
    pub have_lobound: bool,          /* found a low-bound clause yet? */
    pub have_hibound: bool,          /* found a high-bound clause yet? */
    pub lobound: Selectivity,        /* Selectivity of a var > something clause */
    pub hibound: Selectivity,        /* Selectivity of a var < something clause */
}

/****************************************************************************
 *		ROUTINES TO COMPUTE SELECTIVITIES
 ****************************************************************************/

/*
 * clauselist_selectivity -
 *	  Compute the selectivity of an implicitly-ANDed list of boolean
 *	  expression clauses.  The list can be empty, in which case 1.0
 *	  must be returned.  List elements may be either RestrictInfos
 *	  or bare expression clauses --- the former is preferred since
 *	  it allows caching of results.
 *
 * See clause_selectivity() for the meaning of the additional parameters.
 *
 * The basic approach is to apply extended statistics first, on as many
 * clauses as possible, in order to capture cross-column dependencies etc.
 * The remaining clauses are then estimated by taking the product of their
 * selectivities, but that's only right if they have independent
 * probabilities, and in reality they are often NOT independent even if they
 * only refer to a single column.  So, we want to be smarter where we can.
 *
 * We also recognize "range queries", such as "x > 34 AND x < 42".  Clauses
 * are recognized as possible range query components if they are restriction
 * opclauses whose operators have scalarltsel or a related function as their
 * restriction selectivity estimator.  We pair up clauses of this form that
 * refer to the same variable.  An unpairable clause of this kind is simply
 * multiplied into the selectivity product in the normal way.  But when we
 * find a pair, we know that the selectivities represent the relative
 * positions of the low and high bounds within the column's range, so instead
 * of figuring the selectivity as hisel * losel, we can figure it as hisel +
 * losel - 1.  (To visualize this, see that hisel is the fraction of the range
 * below the high bound, while losel is the fraction above the low bound; so
 * hisel can be interpreted directly as a 0..1 value but we need to convert
 * losel to 1-losel before interpreting it as a value.  Then the available
 * range is 1-losel to hisel.  However, this calculation double-excludes
 * nulls, so really we need hisel + losel + null_frac - 1.)
 *
 * If either selectivity is exactly DEFAULT_INEQ_SEL, we forget this equation
 * and instead use DEFAULT_RANGE_INEQ_SEL.  The same applies if the equation
 * yields an impossible (negative) result.
 *
 * A free side-effect is that we can recognize redundant inequalities such
 * as "x < 4 AND x < 5"; only the tighter constraint will be counted.
 *
 * Of course this is all very dependent on the behavior of the inequality
 * selectivity functions; perhaps some day we can generalize the approach.
 */
pub unsafe fn clauselist_selectivity(
    root: *mut PlannerInfo,
    clauses: *mut List,
    varRelid: c_int,
    jointype: JoinType,
    sjinfo: *mut SpecialJoinInfo,
) -> Selectivity {
    clauselist_selectivity_ext(root, clauses, varRelid, jointype, sjinfo, true)
}

/*
 * clauselist_selectivity_ext -
 *	  Extended version of clauselist_selectivity().  If "use_extended_stats"
 *	  is false, all extended statistics will be ignored, and only per-column
 *	  statistics will be used.
 */
pub unsafe fn clauselist_selectivity_ext(
    root: *mut PlannerInfo,
    clauses: *mut List,
    varRelid: c_int,
    jointype: JoinType,
    sjinfo: *mut SpecialJoinInfo,
    use_extended_stats: bool,
) -> Selectivity {
    let mut s1: Selectivity = 1.0;
    let rel: *mut RelOptInfo;
    let mut estimatedclauses: *mut crate::nodes::bitmapset::Bitmapset = core::ptr::null_mut();
    let mut rqlist: *mut RangeQueryClause = core::ptr::null_mut();
    let mut listidx: c_int;

    /*
     * If there's exactly one clause, just go directly to
     * clause_selectivity_ext(). None of what we might do below is relevant.
     */
    if list_length(clauses) == 1 {
        return clause_selectivity_ext(
            root,
            linitial(clauses) as *mut Node,
            varRelid,
            jointype,
            sjinfo,
            use_extended_stats,
        );
    }

    /*
     * Determine if these clauses reference a single relation.  If so, and if
     * it has extended statistics, try to apply those.
     */
    rel = find_single_rel_for_clauses(root, clauses);
    if use_extended_stats
        && !rel.is_null()
        && (*rel).rtekind == RTE_RELATION
        && (*rel).statlist != NIL
    {
        /*
         * Estimate as many clauses as possible using extended statistics.
         *
         * 'estimatedclauses' is populated with the 0-based list position
         * index of clauses estimated here, and that should be ignored below.
         */
        s1 = statext_clauselist_selectivity(
            root,
            clauses,
            varRelid,
            jointype,
            sjinfo,
            rel,
            &raw mut estimatedclauses,
            false,
        );
    }

    /*
     * Apply normal selectivity estimates for remaining clauses. We'll be
     * careful to skip any clauses which were already estimated above.
     *
     * Anything that doesn't look like a potential rangequery clause gets
     * multiplied into s1 and forgotten. Anything that does gets inserted into
     * an rqlist entry.
     */
    listidx = -1;
    foreach!(l, clauses, {
        let mut clause: *mut Node = lfirst(current_cell!(l)) as *mut Node;
        let rinfo: *mut RestrictInfo;
        let s2: Selectivity;

        listidx += 1;

        /*
         * Skip this clause if it's already been estimated by some other
         * statistics above.
         */
        if bms_is_member(listidx, estimatedclauses) {
            continue;
        }

        /* Compute the selectivity of this clause in isolation */
        s2 = clause_selectivity_ext(root, clause, varRelid, jointype, sjinfo, use_extended_stats);

        /*
         * Check for being passed a RestrictInfo.
         *
         * If it's a pseudoconstant RestrictInfo, then s2 is either 1.0 or
         * 0.0; just use that rather than looking for range pairs.
         */
        if IsA!(clause, T_RestrictInfo) {
            rinfo = clause as *mut RestrictInfo;
            if (*rinfo).pseudoconstant {
                s1 = s1 * s2;
                continue;
            }
            clause = (*rinfo).clause as *mut Node;
        } else {
            rinfo = core::ptr::null_mut();
        }

        /*
         * See if it looks like a restriction clause with a pseudoconstant on
         * one side.  (Anything more complicated than that might not behave in
         * the simple way we are expecting.)  Most of the tests here can be
         * done more efficiently with rinfo than without.
         */
        if is_opclause(clause as *const c_void)
            && list_length((*(clause as *mut OpExpr)).args) == 2
        {
            let expr: *mut OpExpr = clause as *mut OpExpr;
            let mut varonleft: bool = true;
            let ok: bool;

            if !rinfo.is_null() {
                ok = ((*rinfo).num_base_rels == 1)
                    && (is_pseudo_constant_clause_relids(
                        lsecond((*expr).args) as *mut Node,
                        (*rinfo).right_relids,
                    ) || {
                        varonleft = false;
                        is_pseudo_constant_clause_relids(
                            linitial((*expr).args) as *mut Node,
                            (*rinfo).left_relids,
                        )
                    });
            } else {
                ok = (NumRelids(root, clause) == 1)
                    && (is_pseudo_constant_clause(lsecond((*expr).args) as *mut Node) || {
                        varonleft = false;
                        is_pseudo_constant_clause(linitial((*expr).args) as *mut Node)
                    });
            }

            if ok {
                /*
                 * If it's not a "<"/"<="/">"/">=" operator, just merge the
                 * selectivity in generically.  But if it's the right oprrest,
                 * add the clause to rqlist for later processing.
                 */
                let oprrest = get_oprrest((*expr).opno);
                if oprrest == F_SCALARLTSEL || oprrest == F_SCALARLESEL {
                    addRangeClause(&raw mut rqlist, clause, varonleft, true, s2);
                } else if oprrest == F_SCALARGTSEL || oprrest == F_SCALARGESEL {
                    addRangeClause(&raw mut rqlist, clause, varonleft, false, s2);
                } else {
                    /* Just merge the selectivity in generically */
                    s1 = s1 * s2;
                }
                continue; /* drop to loop bottom */
            }
        }

        /* Not the right form, so treat it generically. */
        s1 = s1 * s2;
    });

    /*
     * Now scan the rangequery pair list.
     */
    while !rqlist.is_null() {
        let rqnext: *mut RangeQueryClause;

        if (*rqlist).have_lobound && (*rqlist).have_hibound {
            /* Successfully matched a pair of range clauses */
            let mut s2: Selectivity;

            /*
             * Exact equality to the default value probably means the
             * selectivity function punted.  This is not airtight but should
             * be good enough.
             */
            if (*rqlist).hibound == DEFAULT_INEQ_SEL || (*rqlist).lobound == DEFAULT_INEQ_SEL {
                s2 = DEFAULT_RANGE_INEQ_SEL;
            } else {
                s2 = (*rqlist).hibound + (*rqlist).lobound - 1.0;

                /* Adjust for double-exclusion of NULLs */
                s2 += nulltestsel(
                    root,
                    crate::nodes::primnodes::IS_NULL,
                    (*rqlist).var,
                    varRelid,
                    jointype,
                    sjinfo,
                );

                /*
                 * A zero or slightly negative s2 should be converted into a
                 * small positive value; we probably are dealing with a very
                 * tight range and got a bogus result due to roundoff errors.
                 * However, if s2 is very negative, then we probably have
                 * default selectivity estimates on one or both sides of the
                 * range that we failed to recognize above for some reason.
                 */
                if s2 <= 0.0 {
                    if s2 < -0.01 {
                        /*
                         * No data available --- use a default estimate that
                         * is small, but not real small.
                         */
                        s2 = DEFAULT_RANGE_INEQ_SEL;
                    } else {
                        /*
                         * It's just roundoff error; use a small positive
                         * value
                         */
                        s2 = 1.0e-10;
                    }
                }
            }
            /* Merge in the selectivity of the pair of clauses */
            s1 *= s2;
        } else {
            /* Only found one of a pair, merge it in generically */
            if (*rqlist).have_lobound {
                s1 *= (*rqlist).lobound;
            } else {
                s1 *= (*rqlist).hibound;
            }
        }
        /* release storage and advance */
        rqnext = (*rqlist).next;
        pfree(rqlist as *mut c_void);
        rqlist = rqnext;
    }

    s1
}

/*
 * clauselist_selectivity_or -
 *	  Compute the selectivity of an implicitly-ORed list of boolean
 *	  expression clauses.  The list can be empty, in which case 0.0
 *	  must be returned.  List elements may be either RestrictInfos
 *	  or bare expression clauses --- the former is preferred since
 *	  it allows caching of results.
 *
 * See clause_selectivity() for the meaning of the additional parameters.
 *
 * The basic approach is to apply extended statistics first, on as many
 * clauses as possible, in order to capture cross-column dependencies etc.
 * The remaining clauses are then estimated as if they were independent.
 */
unsafe fn clauselist_selectivity_or(
    root: *mut PlannerInfo,
    clauses: *mut List,
    varRelid: c_int,
    jointype: JoinType,
    sjinfo: *mut SpecialJoinInfo,
    use_extended_stats: bool,
) -> Selectivity {
    let mut s1: Selectivity = 0.0;
    let rel: *mut RelOptInfo;
    let mut estimatedclauses: *mut crate::nodes::bitmapset::Bitmapset = core::ptr::null_mut();
    let mut listidx: c_int;

    /*
     * Determine if these clauses reference a single relation.  If so, and if
     * it has extended statistics, try to apply those.
     */
    rel = find_single_rel_for_clauses(root, clauses);
    if use_extended_stats
        && !rel.is_null()
        && (*rel).rtekind == RTE_RELATION
        && (*rel).statlist != NIL
    {
        /*
         * Estimate as many clauses as possible using extended statistics.
         *
         * 'estimatedclauses' is populated with the 0-based list position
         * index of clauses estimated here, and that should be ignored below.
         */
        s1 = statext_clauselist_selectivity(
            root,
            clauses,
            varRelid,
            jointype,
            sjinfo,
            rel,
            &raw mut estimatedclauses,
            true,
        );
    }

    /*
     * Estimate the remaining clauses as if they were independent.
     *
     * Selectivities for an OR clause are computed as s1+s2 - s1*s2 to account
     * for the probable overlap of selected tuple sets.
     *
     * XXX is this too conservative?
     */
    listidx = -1;
    foreach!(lc, clauses, {
        let s2: Selectivity;

        listidx += 1;

        /*
         * Skip this clause if it's already been estimated by some other
         * statistics above.
         */
        if bms_is_member(listidx, estimatedclauses) {
            continue;
        }

        s2 = clause_selectivity_ext(
            root,
            lfirst(current_cell!(lc)) as *mut Node,
            varRelid,
            jointype,
            sjinfo,
            use_extended_stats,
        );

        s1 = s1 + s2 - s1 * s2;
    });

    s1
}

/*
 * addRangeClause --- add a new range clause for clauselist_selectivity
 *
 * Here is where we try to match up pairs of range-query clauses
 */
unsafe fn addRangeClause(
    rqlist: *mut *mut RangeQueryClause,
    clause: *mut Node,
    varonleft: bool,
    isLTsel: bool,
    s2: Selectivity,
) {
    let rqelem: *mut RangeQueryClause;
    let var: *mut Node;
    let is_lobound: bool;

    if varonleft {
        var = get_leftop(clause as *const c_void);
        is_lobound = !isLTsel; /* x < something is high bound */
    } else {
        var = get_rightop(clause as *const c_void);
        is_lobound = isLTsel; /* something < x is low bound */
    }

    let mut rqe: *mut RangeQueryClause = *rqlist;
    while !rqe.is_null() {
        /*
         * We use full equal() here because the "var" might be a function of
         * one or more attributes of the same relation...
         */
        if !equal(var as *const c_void, (*rqe).var as *const c_void) {
            rqe = (*rqe).next;
            continue;
        }
        /* Found the right group to put this clause in */
        if is_lobound {
            if !(*rqe).have_lobound {
                (*rqe).have_lobound = true;
                (*rqe).lobound = s2;
            } else {
                /*------
                 * We have found two similar clauses, such as
                 * x < y AND x <= z.
                 * Keep only the more restrictive one.
                 *------
                 */
                if (*rqe).lobound > s2 {
                    (*rqe).lobound = s2;
                }
            }
        } else {
            if !(*rqe).have_hibound {
                (*rqe).have_hibound = true;
                (*rqe).hibound = s2;
            } else {
                /*------
                 * We have found two similar clauses, such as
                 * x > y AND x >= z.
                 * Keep only the more restrictive one.
                 *------
                 */
                if (*rqe).hibound > s2 {
                    (*rqe).hibound = s2;
                }
            }
        }
        return;
    }

    /* No matching var found, so make a new clause-pair data structure */
    rqelem = palloc(core::mem::size_of::<RangeQueryClause>()) as *mut RangeQueryClause;
    (*rqelem).var = var;
    if is_lobound {
        (*rqelem).have_lobound = true;
        (*rqelem).have_hibound = false;
        (*rqelem).lobound = s2;
    } else {
        (*rqelem).have_lobound = false;
        (*rqelem).have_hibound = true;
        (*rqelem).hibound = s2;
    }
    (*rqelem).next = *rqlist;
    *rqlist = rqelem;
}

/*
 * find_single_rel_for_clauses
 *		Examine each clause in 'clauses' and determine if all clauses
 *		reference only a single relation.  If so return that relation,
 *		otherwise return NULL.
 */
unsafe fn find_single_rel_for_clauses(
    root: *mut PlannerInfo,
    clauses: *mut List,
) -> *mut RelOptInfo {
    let mut lastrelid: c_int = 0;

    foreach!(l, clauses, {
        let rinfo: *mut RestrictInfo = lfirst(current_cell!(l)) as *mut RestrictInfo;
        let mut relid: c_int = 0;

        /*
         * If we have a list of bare clauses rather than RestrictInfos, we
         * could pull out their relids the hard way with pull_varnos().
         * However, currently the extended-stats machinery won't do anything
         * with non-RestrictInfo clauses anyway, so there's no point in
         * spending extra cycles; just fail if that's what we have.
         *
         * An exception to that rule is if we have a bare BoolExpr AND clause.
         * We treat this as a special case because the restrictinfo machinery
         * doesn't build RestrictInfos on top of AND clauses.
         */
        if is_andclause(rinfo as *const c_void) {
            let rel: *mut RelOptInfo;

            rel = find_single_rel_for_clauses(root, (*(rinfo as *mut BoolExpr)).args);

            if rel.is_null() {
                return core::ptr::null_mut();
            }
            if lastrelid == 0 {
                lastrelid = (*rel).relid as c_int;
            } else if (*rel).relid as c_int != lastrelid {
                return core::ptr::null_mut();
            }

            continue;
        }

        if !IsA!(rinfo, T_RestrictInfo) {
            return core::ptr::null_mut();
        }

        if bms_is_empty((*rinfo).clause_relids) {
            continue; /* we can ignore variable-free clauses */
        }
        if !bms_get_singleton_member((*rinfo).clause_relids, &raw mut relid) {
            return core::ptr::null_mut(); /* multiple relations in this clause */
        }
        if lastrelid == 0 {
            lastrelid = relid; /* first clause referencing a relation */
        } else if relid != lastrelid {
            return core::ptr::null_mut(); /* relation not same as last one */
        }
    });

    if lastrelid != 0 {
        return find_base_rel(root, lastrelid);
    }

    core::ptr::null_mut() /* no clauses */
}

/*
 * treat_as_join_clause -
 *	  Decide whether an operator clause is to be handled by the
 *	  restriction or join estimator.  Subroutine for clause_selectivity().
 */
#[inline]
unsafe fn treat_as_join_clause(
    root: *mut PlannerInfo,
    clause: *mut Node,
    rinfo: *mut RestrictInfo,
    varRelid: c_int,
    sjinfo: *mut SpecialJoinInfo,
) -> bool {
    if varRelid != 0 {
        /*
         * Caller is forcing restriction mode (eg, because we are examining an
         * inner indexscan qual).
         */
        false
    } else if sjinfo.is_null() {
        /*
         * It must be a restriction clause, since it's being evaluated at a
         * scan node.
         */
        false
    } else {
        /*
         * Otherwise, it's a join if there's more than one base relation used.
         * We can optimize this calculation if an rinfo was passed.
         *
         * XXX	Since we know the clause is being evaluated at a join, the
         * only way it could be single-relation is if it was delayed by outer
         * joins.  We intentionally count only baserels here, not OJs that
         * might be present in rinfo->clause_relids, so that we direct such
         * cases to the restriction qual estimators not join estimators.
         * Eventually some notice should be taken of the possibility of
         * injected nulls, but we'll likely want to do that in the restriction
         * estimators rather than starting to treat such cases as join quals.
         */
        if !rinfo.is_null() {
            (*rinfo).num_base_rels > 1
        } else {
            NumRelids(root, clause) > 1
        }
    }
}

/*
 * clause_selectivity -
 *	  Compute the selectivity of a general boolean expression clause.
 *
 * The clause can be either a RestrictInfo or a plain expression.  If it's
 * a RestrictInfo, we try to cache the selectivity for possible re-use,
 * so passing RestrictInfos is preferred.
 *
 * varRelid is either 0 or a rangetable index.
 *
 * When varRelid is not 0, only variables belonging to that relation are
 * considered in computing selectivity; other vars are treated as constants
 * of unknown values.  This is appropriate for estimating the selectivity of
 * a join clause that is being used as a restriction clause in a scan of a
 * nestloop join's inner relation --- varRelid should then be the ID of the
 * inner relation.
 *
 * When varRelid is 0, all variables are treated as variables.  This
 * is appropriate for ordinary join clauses and restriction clauses.
 *
 * jointype is the join type, if the clause is a join clause.  Pass JOIN_INNER
 * if the clause isn't a join clause.
 *
 * sjinfo is NULL for a non-join clause, otherwise it provides additional
 * context information about the join being performed.  There are some
 * special cases:
 *	1. For a special (not INNER) join, sjinfo is always a member of
 *	   root->join_info_list.
 *	2. For an INNER join, sjinfo is just a transient struct, and only the
 *	   relids and jointype fields in it can be trusted.
 * It is possible for jointype to be different from sjinfo->jointype.
 * This indicates we are considering a variant join: either with
 * the LHS and RHS switched, or with one input unique-ified.
 *
 * Note: when passing nonzero varRelid, it's normally appropriate to set
 * jointype == JOIN_INNER, sjinfo == NULL, even if the clause is really a
 * join clause; because we aren't treating it as a join clause.
 */
pub unsafe fn clause_selectivity(
    root: *mut PlannerInfo,
    clause: *mut Node,
    varRelid: c_int,
    jointype: JoinType,
    sjinfo: *mut SpecialJoinInfo,
) -> Selectivity {
    clause_selectivity_ext(root, clause, varRelid, jointype, sjinfo, true)
}

/*
 * clause_selectivity_ext -
 *	  Extended version of clause_selectivity().  If "use_extended_stats" is
 *	  false, all extended statistics will be ignored, and only per-column
 *	  statistics will be used.
 */
pub unsafe fn clause_selectivity_ext(
    root: *mut PlannerInfo,
    mut clause: *mut Node,
    varRelid: c_int,
    jointype: JoinType,
    sjinfo: *mut SpecialJoinInfo,
    use_extended_stats: bool,
) -> Selectivity {
    let mut s1: Selectivity = 0.5; /* default for any unhandled clause type */
    let mut rinfo: *mut RestrictInfo = core::ptr::null_mut();
    let mut cacheable: bool = false;

    if clause.is_null() {
        /* can this still happen? */
        return s1;
    }

    if IsA!(clause, T_RestrictInfo) {
        rinfo = clause as *mut RestrictInfo;

        /*
         * If the clause is marked pseudoconstant, then it will be used as a
         * gating qual and should not affect selectivity estimates; hence
         * return 1.0.  The only exception is that a constant FALSE may be
         * taken as having selectivity 0.0, since it will surely mean no rows
         * out of the plan.  This case is simple enough that we need not
         * bother caching the result.
         */
        if (*rinfo).pseudoconstant {
            if !IsA!((*rinfo).clause, T_Const) {
                return 1.0 as Selectivity;
            }
        }

        /*
         * If possible, cache the result of the selectivity calculation for
         * the clause.  We can cache if varRelid is zero or the clause
         * contains only vars of that relid --- otherwise varRelid will affect
         * the result, so mustn't cache.  Outer join quals might be examined
         * with either their join's actual jointype or JOIN_INNER, so we need
         * two cache variables to remember both cases.  Note: we assume the
         * result won't change if we are switching the input relations or
         * considering a unique-ified case, so we only need one cache variable
         * for all non-JOIN_INNER cases.
         */
        if varRelid == 0
            || (*rinfo).num_base_rels == 0
            || ((*rinfo).num_base_rels == 1
                && bms_is_member(varRelid, (*rinfo).clause_relids))
        {
            /* Cacheable --- do we already have the result? */
            if jointype == JOIN_INNER {
                if (*rinfo).norm_selec >= 0.0 {
                    return (*rinfo).norm_selec;
                }
            } else {
                if (*rinfo).outer_selec >= 0.0 {
                    return (*rinfo).outer_selec;
                }
            }
            cacheable = true;
        }

        /*
         * Proceed with examination of contained clause.  If the clause is an
         * OR-clause, we want to look at the variant with sub-RestrictInfos,
         * so that per-subclause selectivities can be cached.
         */
        if !(*rinfo).orclause.is_null() {
            clause = (*rinfo).orclause as *mut Node;
        } else {
            clause = (*rinfo).clause as *mut Node;
        }
    }

    if IsA!(clause, T_Var) {
        let var: *mut Var = clause as *mut Var;

        /*
         * We probably shouldn't ever see an uplevel Var here, but if we do,
         * return the default selectivity...
         */
        if (*var).varlevelsup == 0 && (varRelid == 0 || varRelid == (*var).varno) {
            /* Use the restriction selectivity function for a bool Var */
            s1 = boolvarsel(root, var as *mut Node, varRelid);
        }
    } else if IsA!(clause, T_Const) {
        /* bool constant is pretty easy... */
        let con: *mut Const = clause as *mut Const;

        s1 = if (*con).constisnull {
            0.0
        } else if DatumGetBool((*con).constvalue) {
            1.0
        } else {
            0.0
        };
    } else if IsA!(clause, T_Param) {
        /* see if we can replace the Param */
        let subst: *mut Node = estimate_expression_value(root, clause);

        if IsA!(subst, T_Const) {
            /* bool constant is pretty easy... */
            let con: *mut Const = subst as *mut Const;

            s1 = if (*con).constisnull {
                0.0
            } else if DatumGetBool((*con).constvalue) {
                1.0
            } else {
                0.0
            };
        } else {
            /* XXX any way to do better than default? */
        }
    } else if is_notclause(clause as *const c_void) {
        /* inverse of the selectivity of the underlying clause */
        s1 = 1.0
            - clause_selectivity_ext(
                root,
                get_notclausearg(clause as *const c_void) as *mut Node,
                varRelid,
                jointype,
                sjinfo,
                use_extended_stats,
            );
    } else if is_andclause(clause as *const c_void) {
        /* share code with clauselist_selectivity() */
        s1 = clauselist_selectivity_ext(
            root,
            (*(clause as *mut BoolExpr)).args,
            varRelid,
            jointype,
            sjinfo,
            use_extended_stats,
        );
    } else if is_orclause(clause as *const c_void) {
        /*
         * Almost the same thing as clauselist_selectivity, but with the
         * clauses connected by OR.
         */
        s1 = clauselist_selectivity_or(
            root,
            (*(clause as *mut BoolExpr)).args,
            varRelid,
            jointype,
            sjinfo,
            use_extended_stats,
        );
    } else if is_opclause(clause as *const c_void) || IsA!(clause, T_DistinctExpr) {
        let opclause: *mut OpExpr = clause as *mut OpExpr;
        let opno: Oid = (*opclause).opno;

        if treat_as_join_clause(root, clause, rinfo, varRelid, sjinfo) {
            /* Estimate selectivity for a join clause. */
            s1 = join_selectivity(
                root,
                opno,
                (*opclause).args,
                (*opclause).inputcollid,
                jointype,
                sjinfo,
            );
        } else {
            /* Estimate selectivity for a restriction clause. */
            s1 = restriction_selectivity(
                root,
                opno,
                (*opclause).args,
                (*opclause).inputcollid,
                varRelid,
            );
        }

        /*
         * DistinctExpr has the same representation as OpExpr, but the
         * contained operator is "=" not "<>", so we must negate the result.
         * This estimation method doesn't give the right behavior for nulls,
         * but it's better than doing nothing.
         */
        if IsA!(clause, T_DistinctExpr) {
            s1 = 1.0 - s1;
        }
    } else if is_funcclause(clause as *const c_void) {
        let funcclause: *mut FuncExpr = clause as *mut FuncExpr;

        /* Try to get an estimate from the support function, if any */
        s1 = function_selectivity(
            root,
            (*funcclause).funcid,
            (*funcclause).args,
            (*funcclause).inputcollid,
            treat_as_join_clause(root, clause, rinfo, varRelid, sjinfo),
            varRelid,
            jointype,
            sjinfo,
        );
    } else if IsA!(clause, T_ScalarArrayOpExpr) {
        /* Use node specific selectivity calculation function */
        s1 = scalararraysel(
            root,
            clause as *mut ScalarArrayOpExpr,
            treat_as_join_clause(root, clause, rinfo, varRelid, sjinfo),
            varRelid,
            jointype,
            sjinfo,
        );
    } else if IsA!(clause, T_RowCompareExpr) {
        /* Use node specific selectivity calculation function */
        s1 = rowcomparesel(
            root,
            clause as *mut RowCompareExpr,
            varRelid,
            jointype,
            sjinfo,
        );
    } else if IsA!(clause, T_NullTest) {
        /* Use node specific selectivity calculation function */
        s1 = nulltestsel(
            root,
            (*(clause as *mut NullTest)).nulltesttype,
            (*(clause as *mut NullTest)).arg as *mut Node,
            varRelid,
            jointype,
            sjinfo,
        );
    } else if IsA!(clause, T_BooleanTest) {
        /* Use node specific selectivity calculation function */
        s1 = booltestsel(
            root,
            (*(clause as *mut BooleanTest)).booltesttype,
            (*(clause as *mut BooleanTest)).arg as *mut Node,
            varRelid,
            jointype,
            sjinfo,
        );
    } else if IsA!(clause, T_CurrentOfExpr) {
        /* CURRENT OF selects at most one row of its table */
        let cexpr: *mut CurrentOfExpr = clause as *mut CurrentOfExpr;
        let crel: *mut RelOptInfo = find_base_rel(root, (*cexpr).cvarno as c_int);

        if (*crel).tuples > 0.0 {
            s1 = 1.0 / (*crel).tuples;
        }
    } else if IsA!(clause, T_RelabelType) {
        /* Not sure this case is needed, but it can't hurt */
        s1 = clause_selectivity_ext(
            root,
            (*(clause as *mut RelabelType)).arg as *mut Node,
            varRelid,
            jointype,
            sjinfo,
            use_extended_stats,
        );
    } else if IsA!(clause, T_CoerceToDomain) {
        /* Not sure this case is needed, but it can't hurt */
        s1 = clause_selectivity_ext(
            root,
            (*(clause as *mut CoerceToDomain)).arg as *mut Node,
            varRelid,
            jointype,
            sjinfo,
            use_extended_stats,
        );
    } else {
        /*
         * For anything else, see if we can consider it as a boolean variable.
         * This only works if it's an immutable expression in Vars of a single
         * relation; but there's no point in us checking that here because
         * boolvarsel() will do it internally, and return a suitable default
         * selectivity if not.
         */
        s1 = boolvarsel(root, clause, varRelid);
    }

    /* Cache the result if possible */
    if cacheable {
        if jointype == JOIN_INNER {
            (*rinfo).norm_selec = s1;
        } else {
            (*rinfo).outer_selec = s1;
        }
    }

    // #ifdef SELECTIVITY_DEBUG
    //     elog!(DEBUG4, "clause_selectivity: s1 {}", s1);
    // #endif /* SELECTIVITY_DEBUG */

    s1
}
