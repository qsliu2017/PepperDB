//! tidpath.rs
//!   Routines to determine which TID conditions are usable for scanning
//!   a given relation, and create TidPaths and TidRangePaths accordingly.
//!
//! Translated 1:1 from postgres/src/backend/optimizer/path/tidpath.c
//!
//! For TidPaths, we look for WHERE conditions of the form
//! "CTID = pseudoconstant", which can be implemented by just fetching
//! the tuple directly via heap_fetch().  We can also handle OR'd conditions
//! such as (CTID = const1) OR (CTID = const2), as well as ScalarArrayOpExpr
//! conditions of the form CTID = ANY(pseudoconstant_array).  In particular
//! this allows
//!		WHERE ctid IN (tid1, tid2, ...)
//!
//! As with indexscans, our definition of "pseudoconstant" is pretty liberal:
//! we allow anything that doesn't involve a volatile function or a Var of
//! the relation under consideration.  Vars belonging to other relations of
//! the query are allowed, giving rise to parameterized TID scans.
//!
//! We also support "WHERE CURRENT OF cursor" conditions (CurrentOfExpr),
//! which amount to "CTID = run-time-determined-TID".  These could in
//! theory be translated to a simple comparison of CTID to the result of
//! a function, but in practice it works better to keep the special node
//! representation all the way through to execution.
//!
//! Additionally, TidRangePaths may be created for conditions of the form
//! "CTID relop pseudoconstant", where relop is one of >,>=,<,<=, and
//! AND-clauses composed of such conditions.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!	  src/backend/optimizer/path/tidpath.c
//!
//! #include mapping:
//!   "postgres.h"                  -> crate::prelude::*
//!   "access/sysattr.h"            -> crate::access::sysattr (SelfItemPointerAttributeNumber)
//!   "catalog/pg_operator.h"       -> crate::catalog::pg_known_oids (TID*Operator OIDs)
//!   "catalog/pg_type.h"           -> crate::catalog::pg_type_d (TIDOID)
//!   "nodes/nodeFuncs.h"           -> inline is_opclause helper reproduced here
//!                                    (nodeFuncs not yet ported as a unit; matches
//!                                    restrictinfo.rs's local copy)
//!   "optimizer/cost.h"            -> crate::optimizer::cost (enable_tidscan)
//!   "optimizer/optimizer.h"       -> contain_volatile_functions
//!                                    (crate::optimizer::optimizer),
//!                                    pull_varnos (crate::optimizer::util::var)
//!   "optimizer/pathnode.h"        -> add_path / create_tidscan_path /
//!                                    create_tidrangescan_path (pathnode.c not yet
//!                                    ported -> local STUBs below)
//!   "optimizer/paths.h"           -> generate_implied_equalities_for_column,
//!                                    ec_matches_callback_type (crate::optimizer::paths)
//!   "optimizer/restrictinfo.h"    -> restriction_is_or_clause /
//!                                    restriction_is_securely_promotable /
//!                                    join_clause_is_movable_to
//!                                    (crate::optimizer::util::restrictinfo)

use crate::prelude::*;
use core::ffi::c_void;

use crate::access::sysattr::SelfItemPointerAttributeNumber;
use crate::catalog::pg_known_oids::{
    TIDEqualOperator, TIDGreaterEqOperator, TIDGreaterOperator, TIDLessEqOperator, TIDLessOperator,
};
use crate::catalog::pg_type_d::TIDOID;
use crate::nodes::bitmapset::{bms_del_member, bms_is_member, bms_union};
use crate::nodes::nodes::Node;
use crate::nodes::nodes::NodeTag::{
    T_BoolExpr, T_CurrentOfExpr, T_OpExpr, T_RestrictInfo, T_ScalarArrayOpExpr, T_Var,
};
use crate::nodes::pathnodes::{
    EquivalenceClass, EquivalenceMember, Path, PlannerInfo, RelOptInfo, Relids, RestrictInfo,
    AMFLAG_HAS_TID_RANGE,
};
use crate::nodes::pg_list::{
    lappend, lfirst, linitial, list_concat, list_length, lsecond, List, NIL,
};
use crate::nodes::primnodes::{BoolExpr, CurrentOfExpr, OpExpr, ScalarArrayOpExpr, Var, AND_EXPR};
use crate::optimizer::cost::enable_tidscan;
use crate::optimizer::optimizer::contain_volatile_functions;
use crate::optimizer::paths::generate_implied_equalities_for_column;
use crate::optimizer::util::restrictinfo::{
    join_clause_is_movable_to, restriction_is_or_clause, restriction_is_securely_promotable,
};
use crate::optimizer::util::var::pull_varnos;
use crate::postgres_ext::Oid;
use crate::{castNode, current_cell, foreach, lfirst_node, list_make1, Assert, IsA};

/*
 * ---------------------------------------------------------------------------
 * Inline clause-shape helpers (nodes/nodeFuncs.h).
 *
 * Reproduced here because nodeFuncs is not yet translated as its own unit
 * (restrictinfo.rs / orclauses.rs keep private copies for the same reason).
 * ---------------------------------------------------------------------------
 */

/// `is_opclause(clause)` -- clause is non-NULL and an OpExpr.
#[inline]
unsafe fn is_opclause(clause: *const c_void) -> bool {
    !clause.is_null() && IsA!(clause, T_OpExpr)
}

/// `is_andclause(clause)`.
#[inline]
unsafe fn is_andclause(clause: *const c_void) -> bool {
    !clause.is_null()
        && IsA!(clause, T_BoolExpr)
        && (*(clause as *const BoolExpr)).boolop == AND_EXPR
}

/*
 * ---------------------------------------------------------------------------
 * STUBs for pathnode.c (optimizer/pathnode.h).
 *
 * create_tidscan_path / create_tidrangescan_path / add_path live in
 * pathnode.c, which is not yet ported.  These mirror the local stubs already
 * used in optimizer/plan/planmain.rs and planagg.rs.
 * ---------------------------------------------------------------------------
 */

// TODO(pg-port): real create_tidscan_path lives in optimizer/util/pathnode.rs
unsafe fn create_tidscan_path(
    _root: *mut PlannerInfo,
    _rel: *mut RelOptInfo,
    _tidquals: *mut List,
    _required_outer: Relids,
) -> *mut Path {
    unimplemented!()
}

// TODO(pg-port): real create_tidrangescan_path lives in optimizer/util/pathnode.rs
unsafe fn create_tidrangescan_path(
    _root: *mut PlannerInfo,
    _rel: *mut RelOptInfo,
    _tidrangequals: *mut List,
    _required_outer: Relids,
) -> *mut Path {
    unimplemented!()
}

// TODO(pg-port): real add_path lives in optimizer/util/pathnode.rs
unsafe fn add_path(_parent_rel: *mut RelOptInfo, _new_path: *mut Path) {
    unimplemented!()
}

/*
 * Does this Var represent the CTID column of the specified baserel?
 */
#[inline]
unsafe fn IsCTIDVar(var: *mut Var, rel: *mut RelOptInfo) -> bool {
    /* The vartype check is strictly paranoia */
    if (*var).varattno == SelfItemPointerAttributeNumber
        && (*var).vartype == TIDOID
        && (*var).varno == (*rel).relid as core::ffi::c_int
        && (*var).varnullingrels.is_null()
        && (*var).varlevelsup == 0
    {
        return true;
    }
    false
}

/*
 * Check to see if a RestrictInfo is of the form
 *		CTID OP pseudoconstant
 * or
 *		pseudoconstant OP CTID
 * where OP is a binary operation, the CTID Var belongs to relation "rel",
 * and nothing on the other side of the clause does.
 */
unsafe fn IsBinaryTidClause(rinfo: *mut RestrictInfo, rel: *mut RelOptInfo) -> bool {
    let node: *mut OpExpr;
    let arg1: *mut Node;
    let arg2: *mut Node;
    let mut other: *mut Node;
    let mut other_relids: Relids;

    /* Must be an OpExpr */
    if !is_opclause((*rinfo).clause as *const c_void) {
        return false;
    }
    node = (*rinfo).clause as *mut OpExpr;

    /* OpExpr must have two arguments */
    if list_length((*node).args) != 2 {
        return false;
    }
    arg1 = linitial((*node).args) as *mut Node;
    arg2 = lsecond((*node).args) as *mut Node;

    /* Look for CTID as either argument */
    other = core::ptr::null_mut();
    other_relids = NIL as Relids;
    if !arg1.is_null() && IsA!(arg1, T_Var) && IsCTIDVar(arg1 as *mut Var, rel) {
        other = arg2;
        other_relids = (*rinfo).right_relids;
    }
    if other.is_null() && !arg2.is_null() && IsA!(arg2, T_Var) && IsCTIDVar(arg2 as *mut Var, rel) {
        other = arg1;
        other_relids = (*rinfo).left_relids;
    }
    if other.is_null() {
        return false;
    }

    /* The other argument must be a pseudoconstant */
    if bms_is_member((*rel).relid as core::ffi::c_int, other_relids)
        || contain_volatile_functions(other)
    {
        return false;
    }

    true /* success */
}

/*
 * Check to see if a RestrictInfo is of the form
 *		CTID = pseudoconstant
 * or
 *		pseudoconstant = CTID
 * where the CTID Var belongs to relation "rel", and nothing on the
 * other side of the clause does.
 */
unsafe fn IsTidEqualClause(rinfo: *mut RestrictInfo, rel: *mut RelOptInfo) -> bool {
    if !IsBinaryTidClause(rinfo, rel) {
        return false;
    }

    if (*((*rinfo).clause as *mut OpExpr)).opno == TIDEqualOperator {
        return true;
    }

    false
}

/*
 * Check to see if a RestrictInfo is of the form
 *		CTID OP pseudoconstant
 * or
 *		pseudoconstant OP CTID
 * where OP is a range operator such as <, <=, >, or >=, the CTID Var belongs
 * to relation "rel", and nothing on the other side of the clause does.
 */
unsafe fn IsTidRangeClause(rinfo: *mut RestrictInfo, rel: *mut RelOptInfo) -> bool {
    let opno: Oid;

    if !IsBinaryTidClause(rinfo, rel) {
        return false;
    }
    opno = (*((*rinfo).clause as *mut OpExpr)).opno;

    if opno == TIDLessOperator
        || opno == TIDLessEqOperator
        || opno == TIDGreaterOperator
        || opno == TIDGreaterEqOperator
    {
        return true;
    }

    false
}

/*
 * Check to see if a RestrictInfo is of the form
 *		CTID = ANY (pseudoconstant_array)
 * where the CTID Var belongs to relation "rel", and nothing on the
 * other side of the clause does.
 */
unsafe fn IsTidEqualAnyClause(
    root: *mut PlannerInfo,
    rinfo: *mut RestrictInfo,
    rel: *mut RelOptInfo,
) -> bool {
    let node: *mut ScalarArrayOpExpr;
    let arg1: *mut Node;
    let arg2: *mut Node;

    /* Must be a ScalarArrayOpExpr */
    if !(!(*rinfo).clause.is_null() && IsA!((*rinfo).clause, T_ScalarArrayOpExpr)) {
        return false;
    }
    node = (*rinfo).clause as *mut ScalarArrayOpExpr;

    /* Operator must be tideq */
    if (*node).opno != TIDEqualOperator {
        return false;
    }
    if !(*node).useOr {
        return false;
    }
    Assert!(list_length((*node).args) == 2);
    arg1 = linitial((*node).args) as *mut Node;
    arg2 = lsecond((*node).args) as *mut Node;

    /* CTID must be first argument */
    if !arg1.is_null() && IsA!(arg1, T_Var) && IsCTIDVar(arg1 as *mut Var, rel) {
        /* The other argument must be a pseudoconstant */
        if bms_is_member((*rel).relid as core::ffi::c_int, pull_varnos(root, arg2))
            || contain_volatile_functions(arg2)
        {
            return false;
        }

        return true; /* success */
    }

    false
}

/*
 * Check to see if a RestrictInfo is a CurrentOfExpr referencing "rel".
 */
unsafe fn IsCurrentOfClause(rinfo: *mut RestrictInfo, rel: *mut RelOptInfo) -> bool {
    let node: *mut CurrentOfExpr;

    /* Must be a CurrentOfExpr */
    if !(!(*rinfo).clause.is_null() && IsA!((*rinfo).clause, T_CurrentOfExpr)) {
        return false;
    }
    node = (*rinfo).clause as *mut CurrentOfExpr;

    /* If it references this rel, we're good */
    if (*node).cvarno == (*rel).relid {
        return true;
    }

    false
}

/*
 * Is the RestrictInfo usable as a CTID qual for the specified rel?
 *
 * This function considers only base cases; AND/OR combination is handled
 * below.
 */
unsafe fn RestrictInfoIsTidQual(
    root: *mut PlannerInfo,
    rinfo: *mut RestrictInfo,
    rel: *mut RelOptInfo,
) -> bool {
    /*
     * We may ignore pseudoconstant clauses (they can't contain Vars, so could
     * not match anyway).
     */
    if (*rinfo).pseudoconstant {
        return false;
    }

    /*
     * If clause must wait till after some lower-security-level restriction
     * clause, reject it.
     */
    if !restriction_is_securely_promotable(rinfo, rel) {
        return false;
    }

    /*
     * Check all base cases.
     */
    if IsTidEqualClause(rinfo, rel)
        || IsTidEqualAnyClause(root, rinfo, rel)
        || IsCurrentOfClause(rinfo, rel)
    {
        return true;
    }

    false
}

/*
 * Extract a set of CTID conditions from implicit-AND List of RestrictInfos
 *
 * Returns a List of CTID qual RestrictInfos for the specified rel (with
 * implicit OR semantics across the list), or NIL if there are no usable
 * equality conditions.
 *
 * This function is mainly concerned with handling AND/OR recursion.
 * However, we do have a special rule to enforce: if there is a CurrentOfExpr
 * qual, we *must* return that and only that, else the executor may fail.
 * Ordinarily a CurrentOfExpr would be all alone anyway because of grammar
 * restrictions, but it is possible for RLS quals to appear AND'ed with it.
 * It's even possible (if fairly useless) for the RLS quals to be CTID quals.
 * So we must scan the whole rlist to see if there's a CurrentOfExpr.  Since
 * we have to do that, we also apply some very-trivial preference rules about
 * which of the other possibilities should be chosen, in the unlikely event
 * that there's more than one choice.
 */
unsafe fn TidQualFromRestrictInfoList(
    root: *mut PlannerInfo,
    rlist: *mut List,
    rel: *mut RelOptInfo,
    isCurrentOf: *mut bool,
) -> *mut List {
    let mut tidclause: *mut RestrictInfo = core::ptr::null_mut(); /* best simple CTID qual so far */
    let mut orlist: *mut List = NIL; /* best OR'ed CTID qual so far */

    *isCurrentOf = false;

    foreach!(l, rlist, {
        let rinfo: *mut RestrictInfo = lfirst_node!(RestrictInfo, T_RestrictInfo, current_cell!(l));

        if restriction_is_or_clause(rinfo) {
            let mut rlst: *mut List = NIL;

            /*
             * We must be able to extract a CTID condition from every
             * sub-clause of an OR, or we can't use it.
             */
            foreach!(j, (*((*rinfo).orclause as *mut BoolExpr)).args, {
                let orarg: *mut Node = lfirst(current_cell!(j)) as *mut Node;
                let sublist: *mut List;

                /* OR arguments should be ANDs or sub-RestrictInfos */
                if is_andclause(orarg as *const c_void) {
                    let andargs: *mut List = (*(orarg as *mut BoolExpr)).args;
                    let mut sublistIsCurrentOf: bool = false;

                    /* Recurse in case there are sub-ORs */
                    sublist =
                        TidQualFromRestrictInfoList(root, andargs, rel, &raw mut sublistIsCurrentOf);
                    if sublistIsCurrentOf {
                        elog!(ERROR, "IS CURRENT OF within OR clause");
                    }
                } else {
                    let ri: *mut RestrictInfo = castNode!(RestrictInfo, T_RestrictInfo, orarg);

                    Assert!(!restriction_is_or_clause(ri));
                    if RestrictInfoIsTidQual(root, ri, rel) {
                        sublist = list_make1!(ri);
                    } else {
                        sublist = NIL;
                    }
                }

                /*
                 * If nothing found in this arm, we can't do anything with
                 * this OR clause.
                 */
                if sublist == NIL {
                    rlst = NIL; /* forget anything we had */
                    break; /* out of loop over OR args */
                }

                /*
                 * OK, continue constructing implicitly-OR'ed result list.
                 */
                rlst = list_concat(rlst, sublist);
            });

            if !rlst.is_null() {
                /*
                 * Accept the OR'ed list if it's the first one, or if it's
                 * shorter than the previous one.
                 */
                if orlist == NIL || list_length(rlst) < list_length(orlist) {
                    orlist = rlst;
                }
            }
        } else {
            /* Not an OR clause, so handle base cases */
            if RestrictInfoIsTidQual(root, rinfo, rel) {
                /* We can stop immediately if it's a CurrentOfExpr */
                if IsCurrentOfClause(rinfo, rel) {
                    *isCurrentOf = true;
                    return list_make1!(rinfo);
                }

                /*
                 * Otherwise, remember the first non-OR CTID qual.  We could
                 * try to apply some preference order if there's more than
                 * one, but such usage seems very unlikely, so don't bother.
                 */
                if tidclause.is_null() {
                    tidclause = rinfo;
                }
            }
        }
    });

    /*
     * Prefer any singleton CTID qual to an OR'ed list.  Again, it seems
     * unlikely to be worth thinking harder than that.
     */
    if !tidclause.is_null() {
        return list_make1!(tidclause);
    }
    orlist
}

/*
 * Extract a set of CTID range conditions from implicit-AND List of RestrictInfos
 *
 * Returns a List of CTID range qual RestrictInfos for the specified rel
 * (with implicit AND semantics across the list), or NIL if there are no
 * usable range conditions or if the rel's table AM does not support TID range
 * scans.
 */
unsafe fn TidRangeQualFromRestrictInfoList(rlist: *mut List, rel: *mut RelOptInfo) -> *mut List {
    let mut rlst: *mut List = NIL;

    if ((*rel).amflags & AMFLAG_HAS_TID_RANGE) == 0 {
        return NIL;
    }

    foreach!(l, rlist, {
        let rinfo: *mut RestrictInfo = lfirst_node!(RestrictInfo, T_RestrictInfo, current_cell!(l));

        if IsTidRangeClause(rinfo, rel) {
            rlst = lappend(rlst, rinfo as *mut c_void);
        }
    });

    rlst
}

/*
 * Given a list of join clauses involving our rel, create a parameterized
 * TidPath for each one that is a suitable TidEqual clause.
 *
 * In principle we could combine clauses that reference the same outer rels,
 * but it doesn't seem like such cases would arise often enough to be worth
 * troubling over.
 */
unsafe fn BuildParameterizedTidPaths(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    clauses: *mut List,
) {
    foreach!(l, clauses, {
        let rinfo: *mut RestrictInfo = lfirst_node!(RestrictInfo, T_RestrictInfo, current_cell!(l));
        let tidquals: *mut List;
        let mut required_outer: Relids;

        /*
         * Validate whether each clause is actually usable; we must check this
         * even when examining clauses generated from an EquivalenceClass,
         * since they might not satisfy the restriction on not having Vars of
         * our rel on the other side, or somebody might've built an operator
         * class that accepts type "tid" but has other operators in it.
         *
         * We currently consider only TidEqual join clauses.  In principle we
         * might find a suitable ScalarArrayOpExpr in the rel's joininfo list,
         * but it seems unlikely to be worth expending the cycles to check.
         * And we definitely won't find a CurrentOfExpr here.  Hence, we don't
         * use RestrictInfoIsTidQual; but this must match that function
         * otherwise.
         */
        if (*rinfo).pseudoconstant
            || !restriction_is_securely_promotable(rinfo, rel)
            || !IsTidEqualClause(rinfo, rel)
        {
            continue;
        }

        /*
         * Check if clause can be moved to this rel; this is probably
         * redundant when considering EC-derived clauses, but we must check it
         * for "loose" join clauses.
         */
        if !join_clause_is_movable_to(rinfo, rel) {
            continue;
        }

        /* OK, make list of clauses for this path */
        tidquals = list_make1!(rinfo);

        /* Compute required outer rels for this path */
        required_outer = bms_union((*rinfo).required_relids, (*rel).lateral_relids);
        required_outer = bms_del_member(required_outer, (*rel).relid as core::ffi::c_int);

        add_path(
            rel,
            create_tidscan_path(root, rel, tidquals, required_outer) as *mut Path,
        );
    });
}

/*
 * Test whether an EquivalenceClass member matches our rel's CTID Var.
 *
 * This is a callback for use by generate_implied_equalities_for_column.
 */
unsafe extern "C" fn ec_member_matches_ctid(
    _root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    _ec: *mut EquivalenceClass,
    em: *mut EquivalenceMember,
    _arg: *mut c_void,
) -> bool {
    if !(*em).em_expr.is_null()
        && IsA!((*em).em_expr, T_Var)
        && IsCTIDVar((*em).em_expr as *mut Var, rel)
    {
        return true;
    }
    false
}

/*
 * create_tidscan_paths
 *	  Create paths corresponding to direct TID scans of the given rel.
 *
 *	  Candidate paths are added to the rel's pathlist (using add_path).
 */
pub unsafe fn create_tidscan_paths(root: *mut PlannerInfo, rel: *mut RelOptInfo) -> bool {
    let tidquals: *mut List;
    let tidrangequals: *mut List;
    let mut isCurrentOf: bool = false;

    /*
     * If any suitable quals exist in the rel's baserestrict list, generate a
     * plain (unparameterized) TidPath with them.
     *
     * We skip this when enable_tidscan = false, except when the qual is
     * CurrentOfExpr. In that case, a TID scan is the only correct path.
     */
    tidquals = TidQualFromRestrictInfoList(
        root,
        (*rel).baserestrictinfo,
        rel,
        &raw mut isCurrentOf,
    );

    if tidquals != NIL && (enable_tidscan || isCurrentOf) {
        /*
         * This path uses no join clauses, but it could still have required
         * parameterization due to LATERAL refs in its tlist.
         */
        let required_outer: Relids = (*rel).lateral_relids;

        add_path(
            rel,
            create_tidscan_path(root, rel, tidquals, required_outer) as *mut Path,
        );

        /*
         * When the qual is CurrentOfExpr, the path that we just added is the
         * only one the executor can handle, so we should return before adding
         * any others. Returning true lets the caller know not to add any
         * others, either.
         */
        if isCurrentOf {
            return true;
        }
    }

    /* Skip the rest if TID scans are disabled. */
    if !enable_tidscan {
        return false;
    }

    /*
     * If there are range quals in the baserestrict list, generate a
     * TidRangePath.
     */
    tidrangequals = TidRangeQualFromRestrictInfoList((*rel).baserestrictinfo, rel);

    if tidrangequals != NIL {
        /*
         * This path uses no join clauses, but it could still have required
         * parameterization due to LATERAL refs in its tlist.
         */
        let required_outer: Relids = (*rel).lateral_relids;

        add_path(
            rel,
            create_tidrangescan_path(root, rel, tidrangequals, required_outer) as *mut Path,
        );
    }

    /*
     * Try to generate parameterized TidPaths using equality clauses extracted
     * from EquivalenceClasses.  (This is important since simple "t1.ctid =
     * t2.ctid" clauses will turn into ECs.)
     */
    if (*rel).has_eclass_joins {
        let clauses: *mut List;

        /* Generate clauses, skipping any that join to lateral_referencers */
        clauses = generate_implied_equalities_for_column(
            root,
            rel,
            Some(ec_member_matches_ctid),
            core::ptr::null_mut(),
            (*rel).lateral_referencers,
        );

        /* Generate a path for each usable join clause */
        BuildParameterizedTidPaths(root, rel, clauses);
    }

    /*
     * Also consider parameterized TidPaths using "loose" join quals.  Quals
     * of the form "t1.ctid = t2.ctid" would turn into these if they are outer
     * join quals, for example.
     */
    BuildParameterizedTidPaths(root, rel, (*rel).joininfo);

    false
}
