//! Translation of postgres/src/backend/optimizer/util/restrictinfo.c
//!
//! RestrictInfo node manipulation routines.
//!
//! #include mapping:
//!   "postgres.h"             -> crate::prelude::*
//!   "nodes/makefuncs.h"      -> crate::nodes::makefuncs (make_orclause/make_andclause)
//!   "nodes/nodeFuncs.h"      -> inline helpers is_orclause/is_andclause/is_opclause/
//!                               get_leftop/get_rightop reproduced here (nodeFuncs not
//!                               yet ported as a unit; matches makefuncs.rs's local copy)
//!   "optimizer/clauses.h"    -> contain_leaked_vars (STUB below)
//!   "optimizer/optimizer.h"  -> pull_varnos (crate::optimizer::util::var)
//!   "optimizer/restrictinfo.h" -> public fn signatures
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! Translation notes (deviations from the C source):
//!
//! * RestrictInfo and the planner node types live in crate::nodes::pathnodes /
//!   crate::nodes::primnodes; the field set is taken verbatim from pathnodes.rs.
//! * Bitmapset operations come from crate::nodes::bitmapset; NULL (null_mut) is
//!   the canonical empty Relids, matching the C `Relids == Bitmapset *` model.
//! * `contain_leaked_vars` (optimizer/clauses.c) is not yet ported; it is STUBbed
//!   with a conservative `false` ("don't know is not leakproof") and a TODO.  This
//!   only affects the `leakproof` cache field for security_level > 0 quals.
//! * `RINFO_IS_PUSHED_DOWN` (a macro in pathnodes.h) is reproduced as an inline fn.
//! * `rinfo_is_constant_true` reads Const via IsA!/DatumGetBool.
//! * commute_restrictinfo's two struct flat-copies use `core::ptr::read`/`write`
//!   to mirror C `memcpy(dst, src, sizeof(T))`.

use crate::prelude::*;
use core::ffi::{c_int, c_void};

use crate::nodes::bitmapset::{
    bms_difference, bms_free, bms_is_empty, bms_is_member, bms_is_subset, bms_num_members,
    bms_overlap, bms_union,
};
use crate::nodes::makefuncs::{make_andclause, make_orclause};
use crate::nodes::nodes::Node;
use crate::nodes::nodes::NodeTag::{T_BoolExpr, T_Const, T_OpExpr, T_RestrictInfo};
use crate::nodes::pathnodes::{
    PlannerInfo, RelOptInfo, Relids, RestrictInfo, VOLATILITY_UNKNOWN,
};
use crate::nodes::pg_list::{lappend, lfirst, linitial, list_length, lsecond, List, NIL};
use crate::nodes::primnodes::{BoolExpr, Const, Expr, OpExpr, AND_EXPR, OR_EXPR};
use crate::optimizer::util::var::pull_varnos;
use crate::postgres_ext::Oid;
use crate::{castNode, current_cell, foreach, lfirst_node, list_make2, makeNode, Assert, IsA};

/*
 * ---------------------------------------------------------------------------
 * Inline clause-shape helpers (nodes/nodeFuncs.h).
 *
 * Reproduced verbatim here because nodeFuncs is not yet translated as its own
 * unit (makefuncs.rs keeps a private copy of is_andclause for the same reason).
 * ---------------------------------------------------------------------------
 */

/// `is_opclause(clause)` -- clause is NULL or a valid node pointer.
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

/// `is_orclause(clause)`.
#[inline]
unsafe fn is_orclause(clause: *const c_void) -> bool {
    !clause.is_null()
        && IsA!(clause, T_BoolExpr)
        && (*(clause as *const BoolExpr)).boolop == OR_EXPR
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
 * RINFO_IS_PUSHED_DOWN(rinfo, joinrelids)  (macro in pathnodes.h)
 *
 * The correct way to test whether a RestrictInfo is "pushed down" to a given
 * outer join, i.e. should be treated as a filter clause rather than a join
 * clause at that outer join.
 */
#[inline]
unsafe fn rinfo_is_pushed_down(rinfo: *const RestrictInfo, joinrelids: Relids) -> bool {
    (*rinfo).is_pushed_down || !bms_is_subset((*rinfo).required_relids, joinrelids)
}

/*
 * contain_leaked_vars (optimizer/clauses.c)
 *
 * STUB: clauses.c is not yet ported.  Returns false conservatively, which means
 * `leakproof` is left false ("don't know") for security_level > 0 quals.  This
 * is safe (it only suppresses an optimization), but not exact.
 */
// TODO(pg-port): optimizer/clauses.c contain_leaked_vars
unsafe fn contain_leaked_vars(clause: *mut Node) -> bool {
    crate::optimizer::util::clauses::contain_leaked_vars(clause)
}

/*
 * make_restrictinfo
 *
 * Build a RestrictInfo node containing the given subexpression.
 *
 * The is_pushed_down, has_clone, is_clone, and pseudoconstant flags for the
 * RestrictInfo must be supplied by the caller, as well as the correct values
 * for security_level, incompatible_relids, and outer_relids.
 * required_relids can be NULL, in which case it defaults to the actual clause
 * contents (i.e., clause_relids).
 */
pub unsafe fn make_restrictinfo(
    root: *mut PlannerInfo,
    clause: *mut Expr,
    is_pushed_down: bool,
    has_clone: bool,
    is_clone: bool,
    pseudoconstant: bool,
    security_level: Index,
    required_relids: Relids,
    incompatible_relids: Relids,
    outer_relids: Relids,
) -> *mut RestrictInfo {
    /*
     * If it's an OR clause, build a modified copy with RestrictInfos inserted
     * above each subclause of the top-level AND/OR structure.
     */
    if is_orclause(clause as *const c_void) {
        return make_sub_restrictinfos(
            root,
            clause,
            is_pushed_down,
            has_clone,
            is_clone,
            pseudoconstant,
            security_level,
            required_relids,
            incompatible_relids,
            outer_relids,
        ) as *mut RestrictInfo;
    }

    /* Shouldn't be an AND clause, else AND/OR flattening messed up */
    Assert!(!is_andclause(clause as *const c_void));

    make_plain_restrictinfo(
        root,
        clause,
        core::ptr::null_mut(),
        is_pushed_down,
        has_clone,
        is_clone,
        pseudoconstant,
        security_level,
        required_relids,
        incompatible_relids,
        outer_relids,
    )
}

/*
 * make_plain_restrictinfo
 *
 * Common code for the main entry points and the recursive cases.  Also,
 * useful while constructing RestrictInfos above OR clause, which already has
 * RestrictInfos above its subclauses.
 */
pub unsafe fn make_plain_restrictinfo(
    root: *mut PlannerInfo,
    clause: *mut Expr,
    orclause: *mut Expr,
    is_pushed_down: bool,
    has_clone: bool,
    is_clone: bool,
    pseudoconstant: bool,
    security_level: Index,
    required_relids: Relids,
    incompatible_relids: Relids,
    outer_relids: Relids,
) -> *mut RestrictInfo {
    let restrictinfo: *mut RestrictInfo = makeNode!(RestrictInfo, T_RestrictInfo);
    let baserels: Relids;

    (*restrictinfo).clause = clause;
    (*restrictinfo).orclause = orclause;
    (*restrictinfo).is_pushed_down = is_pushed_down;
    (*restrictinfo).pseudoconstant = pseudoconstant;
    (*restrictinfo).has_clone = has_clone;
    (*restrictinfo).is_clone = is_clone;
    (*restrictinfo).can_join = false; /* may get set below */
    (*restrictinfo).security_level = security_level;
    (*restrictinfo).incompatible_relids = incompatible_relids;
    (*restrictinfo).outer_relids = outer_relids;

    /*
     * If it's potentially delayable by lower-level security quals, figure out
     * whether it's leakproof.  We can skip testing this for level-zero quals,
     * since they would never get delayed on security grounds anyway.
     */
    if security_level > 0 {
        (*restrictinfo).leakproof = !contain_leaked_vars(clause as *mut Node);
    } else {
        (*restrictinfo).leakproof = false; /* really, "don't know" */
    }

    /*
     * Mark volatility as unknown.  The contain_volatile_functions function
     * will determine if there are any volatile functions when called for the
     * first time with this RestrictInfo.
     */
    (*restrictinfo).has_volatile = VOLATILITY_UNKNOWN;

    /*
     * If it's a binary opclause, set up left/right relids info. In any case
     * set up the total clause relids info.
     */
    if is_opclause(clause as *const c_void)
        && list_length((*(clause as *mut OpExpr)).args) == 2
    {
        (*restrictinfo).left_relids = pull_varnos(root, get_leftop(clause as *const c_void));
        (*restrictinfo).right_relids = pull_varnos(root, get_rightop(clause as *const c_void));

        (*restrictinfo).clause_relids =
            bms_union((*restrictinfo).left_relids, (*restrictinfo).right_relids);

        /*
         * Does it look like a normal join clause, i.e., a binary operator
         * relating expressions that come from distinct relations? If so we
         * might be able to use it in a join algorithm.  Note that this is a
         * purely syntactic test that is made regardless of context.
         */
        if !bms_is_empty((*restrictinfo).left_relids)
            && !bms_is_empty((*restrictinfo).right_relids)
            && !bms_overlap((*restrictinfo).left_relids, (*restrictinfo).right_relids)
        {
            (*restrictinfo).can_join = true;
            /* pseudoconstant should certainly not be true */
            Assert!(!(*restrictinfo).pseudoconstant);
        }
    } else {
        /* Not a binary opclause, so mark left/right relid sets as empty */
        (*restrictinfo).left_relids = core::ptr::null_mut();
        (*restrictinfo).right_relids = core::ptr::null_mut();
        /* and get the total relid set the hard way */
        (*restrictinfo).clause_relids = pull_varnos(root, clause as *mut Node);
    }

    /* required_relids defaults to clause_relids */
    if !required_relids.is_null() {
        (*restrictinfo).required_relids = required_relids;
    } else {
        (*restrictinfo).required_relids = (*restrictinfo).clause_relids;
    }

    /*
     * Count the number of base rels appearing in clause_relids.  To do this,
     * we just delete rels mentioned in root->outer_join_rels and count the
     * survivors.
     */
    baserels = bms_difference((*restrictinfo).clause_relids, (*root).outer_join_rels);
    (*restrictinfo).num_base_rels = bms_num_members(baserels);
    bms_free(baserels);

    /*
     * Label this RestrictInfo with a fresh serial number.
     */
    (*root).last_rinfo_serial += 1;
    (*restrictinfo).rinfo_serial = (*root).last_rinfo_serial;

    /*
     * Fill in all the cacheable fields with "not yet set" markers. None of
     * these will be computed until/unless needed.
     */
    (*restrictinfo).parent_ec = core::ptr::null_mut();

    (*restrictinfo).eval_cost.startup = -1.0;
    (*restrictinfo).norm_selec = -1.0;
    (*restrictinfo).outer_selec = -1.0;

    (*restrictinfo).mergeopfamilies = NIL;

    (*restrictinfo).left_ec = core::ptr::null_mut();
    (*restrictinfo).right_ec = core::ptr::null_mut();
    (*restrictinfo).left_em = core::ptr::null_mut();
    (*restrictinfo).right_em = core::ptr::null_mut();
    (*restrictinfo).scansel_cache = NIL;

    (*restrictinfo).outer_is_left = false;

    (*restrictinfo).hashjoinoperator = InvalidOid;

    (*restrictinfo).left_bucketsize = -1.0;
    (*restrictinfo).right_bucketsize = -1.0;
    (*restrictinfo).left_mcvfreq = -1.0;
    (*restrictinfo).right_mcvfreq = -1.0;

    (*restrictinfo).left_hasheqoperator = InvalidOid;
    (*restrictinfo).right_hasheqoperator = InvalidOid;

    restrictinfo
}

/*
 * Recursively insert sub-RestrictInfo nodes into a boolean expression.
 *
 * We put RestrictInfos above simple (non-AND/OR) clauses and above
 * sub-OR clauses, but not above sub-AND clauses, because there's no need.
 *
 * The given required_relids are attached to our top-level output, but any
 * OR-clause constituents are allowed to default to just the contained rels.
 */
unsafe fn make_sub_restrictinfos(
    root: *mut PlannerInfo,
    clause: *mut Expr,
    is_pushed_down: bool,
    has_clone: bool,
    is_clone: bool,
    pseudoconstant: bool,
    security_level: Index,
    required_relids: Relids,
    incompatible_relids: Relids,
    outer_relids: Relids,
) -> *mut Expr {
    if is_orclause(clause as *const c_void) {
        let mut orlist: *mut List = NIL;
        foreach!(temp, (*(clause as *mut BoolExpr)).args, {
            let sub = make_sub_restrictinfos(
                root,
                lfirst(current_cell!(temp)) as *mut Expr,
                is_pushed_down,
                has_clone,
                is_clone,
                pseudoconstant,
                security_level,
                core::ptr::null_mut(),
                incompatible_relids,
                outer_relids,
            );
            orlist = lappend(orlist, sub as *mut c_void);
        });
        make_plain_restrictinfo(
            root,
            clause,
            make_orclause(orlist),
            is_pushed_down,
            has_clone,
            is_clone,
            pseudoconstant,
            security_level,
            required_relids,
            incompatible_relids,
            outer_relids,
        ) as *mut Expr
    } else if is_andclause(clause as *const c_void) {
        let mut andlist: *mut List = NIL;
        foreach!(temp, (*(clause as *mut BoolExpr)).args, {
            let sub = make_sub_restrictinfos(
                root,
                lfirst(current_cell!(temp)) as *mut Expr,
                is_pushed_down,
                has_clone,
                is_clone,
                pseudoconstant,
                security_level,
                required_relids,
                incompatible_relids,
                outer_relids,
            );
            andlist = lappend(andlist, sub as *mut c_void);
        });
        make_andclause(andlist)
    } else {
        make_plain_restrictinfo(
            root,
            clause,
            core::ptr::null_mut(),
            is_pushed_down,
            has_clone,
            is_clone,
            pseudoconstant,
            security_level,
            required_relids,
            incompatible_relids,
            outer_relids,
        ) as *mut Expr
    }
}

/*
 * commute_restrictinfo
 *
 * Given a RestrictInfo containing a binary opclause, produce a RestrictInfo
 * representing the commutation of that clause.  The caller must pass the
 * OID of the commutator operator.
 *
 * Beware that the result shares sub-structure with the given RestrictInfo.
 */
pub unsafe fn commute_restrictinfo(
    rinfo: *mut RestrictInfo,
    comm_op: Oid,
) -> *mut RestrictInfo {
    let clause: *mut OpExpr = castNode!(OpExpr, T_OpExpr, (*rinfo).clause);

    Assert!(list_length((*clause).args) == 2);

    /* flat-copy all the fields of clause ... (C: memcpy sizeof(OpExpr)) */
    let newclause: *mut OpExpr = makeNode!(OpExpr, T_OpExpr);
    core::ptr::write(newclause, core::ptr::read(clause));

    /* ... and adjust those we need to change to commute it */
    (*newclause).opno = comm_op;
    (*newclause).opfuncid = InvalidOid;
    (*newclause).args = list_make2!(lsecond((*clause).args), linitial((*clause).args));

    /* likewise, flat-copy all the fields of rinfo ... (C: memcpy sizeof(RestrictInfo)) */
    let result: *mut RestrictInfo = makeNode!(RestrictInfo, T_RestrictInfo);
    core::ptr::write(result, core::ptr::read(rinfo));

    /*
     * ... and adjust those we need to change.  Note in particular that we can
     * preserve any cached selectivity or cost estimates, and keep the same
     * rinfo_serial and parent_ec.
     */
    (*result).clause = newclause as *mut Expr;
    (*result).left_relids = (*rinfo).right_relids;
    (*result).right_relids = (*rinfo).left_relids;
    Assert!((*result).orclause.is_null());
    (*result).left_ec = (*rinfo).right_ec;
    (*result).right_ec = (*rinfo).left_ec;
    (*result).left_em = (*rinfo).right_em;
    (*result).right_em = (*rinfo).left_em;
    (*result).scansel_cache = NIL; /* not worth updating this */
    if (*rinfo).hashjoinoperator == (*clause).opno {
        (*result).hashjoinoperator = comm_op;
    } else {
        (*result).hashjoinoperator = InvalidOid;
    }
    (*result).left_bucketsize = (*rinfo).right_bucketsize;
    (*result).right_bucketsize = (*rinfo).left_bucketsize;
    (*result).left_mcvfreq = (*rinfo).right_mcvfreq;
    (*result).right_mcvfreq = (*rinfo).left_mcvfreq;
    (*result).left_hasheqoperator = InvalidOid;
    (*result).right_hasheqoperator = InvalidOid;

    result
}

/*
 * restriction_is_or_clause
 *
 * Returns true iff the restrictinfo node contains an 'or' clause.
 */
pub unsafe fn restriction_is_or_clause(restrictinfo: *mut RestrictInfo) -> bool {
    !(*restrictinfo).orclause.is_null()
}

/*
 * restriction_is_securely_promotable
 *
 * Returns true if it's okay to evaluate this clause "early", that is before
 * other restriction clauses attached to the specified relation.
 */
pub unsafe fn restriction_is_securely_promotable(
    restrictinfo: *mut RestrictInfo,
    rel: *mut RelOptInfo,
) -> bool {
    /*
     * It's okay if there are no baserestrictinfo clauses for the rel that
     * would need to go before this one, *or* if this one is leakproof.
     */
    (*restrictinfo).security_level <= (*rel).baserestrict_min_security
        || (*restrictinfo).leakproof
}

/*
 * Detect whether a RestrictInfo's clause is constant TRUE (note that it's
 * surely of type boolean).  equivclass.c may generate such RestrictInfos;
 * we drop them again when creating the finished plan.
 */
#[inline]
unsafe fn rinfo_is_constant_true(rinfo: *mut RestrictInfo) -> bool {
    IsA!((*rinfo).clause, T_Const)
        && !(*((*rinfo).clause as *mut Const)).constisnull
        && DatumGetBool((*((*rinfo).clause as *mut Const)).constvalue)
}

/*
 * get_actual_clauses
 *
 * Returns a list containing the bare clauses from 'restrictinfo_list'.
 *
 * This is only to be used in cases where none of the RestrictInfos can be
 * pseudoconstant clauses (for instance, it's OK on indexqual lists).
 */
pub unsafe fn get_actual_clauses(restrictinfo_list: *mut List) -> *mut List {
    let mut result: *mut List = NIL;

    foreach!(l, restrictinfo_list, {
        let rinfo: *mut RestrictInfo = lfirst_node!(RestrictInfo, T_RestrictInfo, current_cell!(l));

        Assert!(!(*rinfo).pseudoconstant);
        Assert!(!rinfo_is_constant_true(rinfo));

        result = lappend(result, (*rinfo).clause as *mut c_void);
    });
    result
}

/*
 * extract_actual_clauses
 *
 * Extract bare clauses from 'restrictinfo_list', returning either the regular
 * ones or the pseudoconstant ones per 'pseudoconstant'.  Constant-TRUE clauses
 * are dropped in any case.
 */
pub unsafe fn extract_actual_clauses(
    restrictinfo_list: *mut List,
    pseudoconstant: bool,
) -> *mut List {
    let mut result: *mut List = NIL;

    foreach!(l, restrictinfo_list, {
        let rinfo: *mut RestrictInfo = lfirst_node!(RestrictInfo, T_RestrictInfo, current_cell!(l));

        if (*rinfo).pseudoconstant == pseudoconstant && !rinfo_is_constant_true(rinfo) {
            result = lappend(result, (*rinfo).clause as *mut c_void);
        }
    });
    result
}

/*
 * extract_actual_join_clauses
 *
 * Extract bare clauses from 'restrictinfo_list', separating those that
 * semantically match the join level from those that were pushed down.
 * Pseudoconstant and constant-TRUE clauses are excluded from the results.
 *
 * This is only used at outer joins, since for plain joins we don't care about
 * pushed-down-ness.
 */
pub unsafe fn extract_actual_join_clauses(
    restrictinfo_list: *mut List,
    joinrelids: Relids,
    joinquals: *mut *mut List,
    otherquals: *mut *mut List,
) {
    *joinquals = NIL;
    *otherquals = NIL;

    foreach!(l, restrictinfo_list, {
        let rinfo: *mut RestrictInfo = lfirst_node!(RestrictInfo, T_RestrictInfo, current_cell!(l));

        if rinfo_is_pushed_down(rinfo, joinrelids) {
            if !(*rinfo).pseudoconstant && !rinfo_is_constant_true(rinfo) {
                *otherquals = lappend(*otherquals, (*rinfo).clause as *mut c_void);
            }
        } else {
            /* joinquals shouldn't have been marked pseudoconstant */
            Assert!(!(*rinfo).pseudoconstant);
            if !rinfo_is_constant_true(rinfo) {
                *joinquals = lappend(*joinquals, (*rinfo).clause as *mut c_void);
            }
        }
    });
}

/*
 * join_clause_is_movable_to
 *		Test whether a join clause is a safe candidate for parameterization
 *		of a scan on the specified base relation.
 */
pub unsafe fn join_clause_is_movable_to(
    rinfo: *mut RestrictInfo,
    baserel: *mut RelOptInfo,
) -> bool {
    /* Clause must physically reference target rel */
    if !bms_is_member((*baserel).relid as c_int, (*rinfo).clause_relids) {
        return false;
    }

    /* Cannot move an outer-join clause into the join's outer side */
    if bms_is_member((*baserel).relid as c_int, (*rinfo).outer_relids) {
        return false;
    }

    /*
     * Target rel's Vars must not be nulled by any outer join.  We check this
     * by seeing whether clause_relids (which includes all such Vars'
     * varnullingrels) includes any outer join that can null the target rel.
     */
    if bms_overlap((*rinfo).clause_relids, (*baserel).nulling_relids) {
        return false;
    }

    /* Clause must not use any rels with LATERAL references to this rel */
    if bms_overlap((*baserel).lateral_referencers, (*rinfo).clause_relids) {
        return false;
    }

    /* Ignore clones, too */
    if (*rinfo).is_clone {
        return false;
    }

    true
}

/*
 * join_clause_is_movable_into
 *		Test whether a join clause is movable and can be evaluated within
 *		the current join context.
 *
 * currentrelids: the relids of the proposed evaluation location
 * current_and_outer: the union of currentrelids and the required_outer relids
 *		(parameterization's outer relations)
 */
pub unsafe fn join_clause_is_movable_into(
    rinfo: *mut RestrictInfo,
    currentrelids: Relids,
    current_and_outer: Relids,
) -> bool {
    /* Clause must be evaluable given available context */
    if !bms_is_subset((*rinfo).clause_relids, current_and_outer) {
        return false;
    }

    /* Clause must physically reference at least one target rel */
    if !bms_overlap(currentrelids, (*rinfo).clause_relids) {
        return false;
    }

    /* Cannot move an outer-join clause into the join's outer side */
    if bms_overlap(currentrelids, (*rinfo).outer_relids) {
        return false;
    }

    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::list_make1;
    use crate::nodes::nodes::newNode;

    /// Build a bare BoolExpr(OR) node with empty args.
    unsafe fn make_or_boolexpr() -> *mut BoolExpr {
        let e: *mut BoolExpr =
            newNode(core::mem::size_of::<BoolExpr>(), T_BoolExpr) as *mut BoolExpr;
        (*e).boolop = OR_EXPR;
        (*e).args = NIL;
        (*e).location = -1;
        e
    }

    #[test]
    fn restriction_is_or_clause_true_for_orclause() {
        unsafe {
            let rinfo: *mut RestrictInfo = makeNode!(RestrictInfo, T_RestrictInfo);
            // A RestrictInfo carries the OR shape in its orclause field.
            (*rinfo).orclause = make_or_boolexpr() as *mut Expr;
            assert!(restriction_is_or_clause(rinfo));

            // With a NULL orclause it is not an OR-clause RestrictInfo.
            (*rinfo).orclause = core::ptr::null_mut();
            assert!(!restriction_is_or_clause(rinfo));
        }
    }

    #[test]
    fn get_actual_clauses_extracts_clause() {
        unsafe {
            // Hand-build a 1-element RestrictInfo list whose .clause is a sentinel.
            let rinfo: *mut RestrictInfo = makeNode!(RestrictInfo, T_RestrictInfo);
            (*rinfo).pseudoconstant = false;
            // A non-Const sentinel clause: reuse an OR BoolExpr so rinfo_is_constant_true
            // is false (IsA Const fails).
            let sentinel = make_or_boolexpr() as *mut Expr;
            (*rinfo).clause = sentinel;

            let list = list_make1!(rinfo as *mut c_void);
            let actual = get_actual_clauses(list);

            assert_eq!(list_length(actual), 1);
            assert_eq!(linitial(actual) as *mut Expr, sentinel);
        }
    }
}
