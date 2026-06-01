//! Translation of postgres/src/backend/optimizer/util/orclauses.c
//!
//! Routines to extract restriction OR clauses from join OR clauses (the
//! "OR-clause" planner optimization).
//!
//! #include mapping:
//!   "postgres.h"               -> crate::prelude::*
//!   "nodes/makefuncs.h"        -> crate::nodes::makefuncs
//!                                 (make_orclause / make_ands_explicit)
//!   "nodes/nodeFuncs.h"        -> inline is_orclause/is_andclause helpers
//!                                 reproduced here (nodeFuncs not yet ported as a
//!                                 unit; matches restrictinfo.rs's local copy)
//!   "optimizer/optimizer.h"    -> pull_varnos (unused here directly; see notes),
//!                                 contain_volatile_functions (STUB),
//!                                 clause_selectivity / init_dummy_sjinfo (STUB)
//!   "optimizer/orclauses.h"    -> public fn signature
//!   "optimizer/paths.h"        -> join_clause_is_movable_to
//!                                 (crate::optimizer::util::restrictinfo)
//!   "optimizer/restrictinfo.h" -> make_restrictinfo / restriction_is_or_clause
//!                                 (crate::optimizer::util::restrictinfo)
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! Translation notes (deviations from the C source):
//!
//! * The OR-clause tree walking + RestrictInfo construction
//!   (extract_restriction_or_clauses, is_safe_restriction_clause_for,
//!   extract_or_clause, consider_new_or_clause) are translated REAL.
//! * The cost/selectivity machinery is STUBbed (see TODOs):
//!     - contain_volatile_functions (optimizer/clauses.c) -- conservative `false`
//!       so the safety check never rejects on volatility grounds.  This is the
//!       only behavioral difference: a volatile single-rel qual that should be
//!       rejected will be accepted.  TODO: port clauses.c.
//!     - clause_selectivity / init_dummy_sjinfo (cost.c / costsize.c, joinrels.c)
//!       -- unimplemented!(); consider_new_or_clause stops short of the
//!       selectivity threshold / norm_selec hack and unconditionally accepts the
//!       extracted clause.  TODO: port the selectivity estimator.  The
//!       SpecialJoinInfo norm_selec compensation hack is therefore omitted.
//! * BoolExpr->args is walked via foreach!/lfirst; RestrictInfo / RelOptInfo /
//!   PlannerInfo field sets are taken verbatim from pathnodes.rs.
//! * `Index rti` loops over root->simple_rel_array, mirroring the C array model
//!   (raw pointer indexing into a *mut *mut RelOptInfo).

use crate::prelude::*;
use core::ffi::c_void;

use crate::c::{Index, Min};
use crate::nodes::bitmapset::bms_equal;
use crate::nodes::makefuncs::{make_ands_explicit, make_orclause};
use crate::nodes::nodes::Node;
use crate::nodes::nodes::NodeTag::{T_BoolExpr, T_RestrictInfo};
use crate::nodes::pathnodes::{
    PlannerInfo, RelOptInfo, Relids, RestrictInfo, RELOPT_BASEREL,
};
use crate::nodes::pg_list::{lappend, lfirst, list_concat, List, NIL};
use crate::nodes::primnodes::{BoolExpr, Expr, AND_EXPR, OR_EXPR};
use crate::optimizer::util::restrictinfo::{
    join_clause_is_movable_to, make_restrictinfo, restriction_is_or_clause,
};
use crate::{castNode, current_cell, foreach, lfirst_node, Assert, IsA};

/*
 * ---------------------------------------------------------------------------
 * Inline clause-shape helpers (nodes/nodeFuncs.h).
 *
 * Reproduced here because nodeFuncs is not yet translated as its own unit
 * (restrictinfo.rs / makefuncs.rs keep private copies for the same reason).
 * ---------------------------------------------------------------------------
 */

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

/*
 * ---------------------------------------------------------------------------
 * STUBs for cost/selectivity machinery (optimizer/clauses.c, cost.c,
 * costsize.c, analyzejoins/joinrels.c).  See module notes.
 * ---------------------------------------------------------------------------
 */

/// STUB: `contain_volatile_functions` (optimizer/clauses.c).
///
/// Conservatively reports "no volatile functions" so the safety check below
/// never rejects on volatility grounds.  TODO: port clauses.c.
#[inline]
unsafe fn contain_volatile_functions(_clause: *mut Node) -> bool {
    // TODO(stub): real volatility walk lives in optimizer/clauses.c.
    false
}

/*
 * extract_restriction_or_clauses
 *	  Examine join OR-of-AND clauses to see if any useful restriction OR
 *	  clauses can be extracted.  If so, add them to the query.
 *
 * Although a join clause must reference multiple relations overall,
 * an OR of ANDs clause might contain sub-clauses that reference just one
 * relation and can be used to build a restriction clause for that rel.
 * For example consider
 *		WHERE ((a.x = 42 AND b.y = 43) OR (a.x = 44 AND b.z = 45));
 * We can transform this into
 *		WHERE ((a.x = 42 AND b.y = 43) OR (a.x = 44 AND b.z = 45))
 *			AND (a.x = 42 OR a.x = 44)
 *			AND (b.y = 43 OR b.z = 45);
 * which allows the latter clauses to be applied during the scans of a and b,
 * perhaps as index qualifications, and in any case reducing the number of
 * rows arriving at the join.  In essence this is a partial transformation to
 * CNF (AND of ORs format).  See the C source for the full rationale (including
 * the selectivity-cache hack performed by consider_new_or_clause).
 *
 * We examine each base relation to see if join clauses associated with it
 * contain extractable restriction conditions.  If so, add those conditions
 * to the rel's baserestrictinfo and update the cached selectivities of the
 * join clauses.
 */
pub unsafe fn extract_restriction_or_clauses(root: *mut PlannerInfo) {
    /* Examine each baserel for potential join OR clauses */
    let mut rti: Index = 1;
    while rti < (*root).simple_rel_array_size as Index {
        let rel: *mut RelOptInfo = *(*root).simple_rel_array.add(rti as usize);

        /* there may be empty slots corresponding to non-baserel RTEs */
        if rel.is_null() {
            rti += 1;
            continue;
        }

        Assert!((*rel).relid == rti); /* sanity check on array */

        /* ignore RTEs that are "other rels" */
        if (*rel).reloptkind != RELOPT_BASEREL {
            rti += 1;
            continue;
        }

        /*
         * Find potentially interesting OR joinclauses.  We can use any
         * joinclause that is considered safe to move to this rel by the
         * parameterized-path machinery, even though what we are going to do
         * with it is not exactly a parameterized path.
         */
        foreach!(lc, (*rel).joininfo, {
            let rinfo: *mut RestrictInfo = lfirst(current_cell!(lc)) as *mut RestrictInfo;

            if restriction_is_or_clause(rinfo) && join_clause_is_movable_to(rinfo, rel) {
                /* Try to extract a qual for this rel only */
                let orclause: *mut Expr = extract_or_clause(rinfo, rel);

                /*
                 * If successful, decide whether we want to use the clause,
                 * and insert it into the rel's restrictinfo list if so.
                 */
                if !orclause.is_null() {
                    consider_new_or_clause(root, rel, orclause, rinfo);
                }
            }
        });

        rti += 1;
    }
}

/*
 * Is the given primitive (non-OR) RestrictInfo safe to move to the rel?
 */
unsafe fn is_safe_restriction_clause_for(rinfo: *mut RestrictInfo, rel: *mut RelOptInfo) -> bool {
    /*
     * We want clauses that mention the rel, and only the rel.  So in
     * particular pseudoconstant clauses can be rejected quickly.  Then check
     * the clause's Var membership.
     */
    if (*rinfo).pseudoconstant {
        return false;
    }
    if !bms_equal((*rinfo).clause_relids, (*rel).relids) {
        return false;
    }

    /* We don't want extra evaluations of any volatile functions */
    if contain_volatile_functions((*rinfo).clause as *mut Node) {
        return false;
    }

    true
}

/*
 * Try to extract a restriction clause mentioning only "rel" from the given
 * join OR-clause.
 *
 * We must be able to extract at least one qual for this rel from each of
 * the arms of the OR, else we can't use it.
 *
 * Returns an OR clause (not a RestrictInfo!) pertaining to rel, or NULL
 * if no OR clause could be extracted.
 */
unsafe fn extract_or_clause(or_rinfo: *mut RestrictInfo, rel: *mut RelOptInfo) -> *mut Expr {
    let mut clauselist: *mut List = NIL;

    /*
     * Scan each arm of the input OR clause.  Notice we descend into
     * or_rinfo->orclause, which has RestrictInfo nodes embedded below the
     * toplevel OR/AND structure.  This is useful because we can use the info
     * in those nodes to make is_safe_restriction_clause_for()'s checks
     * cheaper.  We'll strip those nodes from the returned tree, though,
     * meaning that fresh ones will be built if the clause is accepted as a
     * restriction clause.
     */
    Assert!(is_orclause((*or_rinfo).orclause as *const c_void));
    foreach!(lc, (*((*or_rinfo).orclause as *mut BoolExpr)).args, {
        let orarg: *mut Node = lfirst(current_cell!(lc)) as *mut Node;
        let mut subclauses: *mut List = NIL;
        let subclause: *mut Node;

        /* OR arguments should be ANDs or sub-RestrictInfos */
        if is_andclause(orarg as *const c_void) {
            let andargs: *mut List = (*(orarg as *mut BoolExpr)).args;

            foreach!(lc2, andargs, {
                let rinfo: *mut RestrictInfo =
                    lfirst_node!(RestrictInfo, T_RestrictInfo, current_cell!(lc2));

                if restriction_is_or_clause(rinfo) {
                    /*
                     * Recurse to deal with nested OR.  Note we *must* recurse
                     * here, this isn't just overly-tense optimization: we
                     * have to descend far enough to find and strip all
                     * RestrictInfos in the expression.
                     */
                    let suborclause: *mut Expr = extract_or_clause(rinfo, rel);
                    if !suborclause.is_null() {
                        subclauses = lappend(subclauses, suborclause as *mut c_void);
                    }
                } else if is_safe_restriction_clause_for(rinfo, rel) {
                    subclauses = lappend(subclauses, (*rinfo).clause as *mut c_void);
                }
            });
        } else {
            let rinfo: *mut RestrictInfo = castNode!(RestrictInfo, T_RestrictInfo, orarg);

            Assert!(!restriction_is_or_clause(rinfo));
            if is_safe_restriction_clause_for(rinfo, rel) {
                subclauses = lappend(subclauses, (*rinfo).clause as *mut c_void);
            }
        }

        /*
         * If nothing could be extracted from this arm, we can't do anything
         * with this OR clause.
         */
        if subclauses == NIL {
            return core::ptr::null_mut();
        }

        /*
         * OK, add subclause(s) to the result OR.  If we found more than one,
         * we need an AND node.  But if we found only one, and it is itself an
         * OR node, add its subclauses to the result instead; this is needed
         * to preserve AND/OR flatness (ie, no OR directly underneath OR).
         */
        subclause = make_ands_explicit(subclauses) as *mut Node;
        if is_orclause(subclause as *const c_void) {
            clauselist = list_concat(clauselist, (*(subclause as *mut BoolExpr)).args);
        } else {
            clauselist = lappend(clauselist, subclause as *mut c_void);
        }
    });

    /*
     * If we got a restriction clause from every arm, wrap them up in an OR
     * node.  (In theory the OR node might be unnecessary, if there was only
     * one arm --- but then the input OR node was also redundant.)
     */
    if clauselist != NIL {
        return make_orclause(clauselist);
    }
    core::ptr::null_mut()
}

/*
 * Consider whether a successfully-extracted restriction OR clause is
 * actually worth using.  If so, add it to the planner's data structures,
 * and adjust the original join clause (join_or_rinfo) to compensate.
 *
 * TRANSLATION NOTE: the selectivity-threshold gate and the norm_selec
 * compensation hack from the C source are omitted here because they depend on
 * clause_selectivity / init_dummy_sjinfo (cost.c / costsize.c), which are not
 * yet ported.  We unconditionally accept the extracted clause and append it to
 * the rel's baserestrictinfo, mirroring the "add it to the rel" tail of the C
 * function (sans the selectivity bookkeeping).  TODO: restore the >0.9 gate and
 * the norm_selec hack once clause_selectivity is available.
 */
unsafe fn consider_new_or_clause(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    orclause: *mut Expr,
    join_or_rinfo: *mut RestrictInfo,
) {
    /*
     * Build a RestrictInfo from the new OR clause.  We can assume it's valid
     * as a base restriction clause.
     */
    let or_rinfo: *mut RestrictInfo = make_restrictinfo(
        root,
        orclause,
        true,                            /* is_pushed_down */
        false,                           /* has_clone */
        false,                           /* is_clone */
        false,                           /* pseudoconstant */
        (*join_or_rinfo).security_level, /* security_level */
        core::ptr::null_mut(),           /* required_relids */
        core::ptr::null_mut(),           /* incompatible_relids */
        core::ptr::null_mut(),           /* outer_relids */
    );

    /*
     * TODO(stub): estimate selectivity and apply the 0.9 threshold gate.
     *
     *   or_selec = clause_selectivity(root, (Node *) or_rinfo,
     *                                 0, JOIN_INNER, NULL);
     *   if (or_selec > 0.9)
     *       return;
     *
     * clause_selectivity lives in optimizer/path/clausesel.c (cost.c), not yet
     * ported.  Until then we accept the clause unconditionally.
     */

    /*
     * OK, add it to the rel's restriction-clause list.
     */
    (*rel).baserestrictinfo = lappend((*rel).baserestrictinfo, or_rinfo as *mut c_void);
    (*rel).baserestrict_min_security = Min(
        (*rel).baserestrict_min_security,
        (*or_rinfo).security_level,
    );

    /*
     * TODO(stub): the original join OR clause's cached selectivity hack.
     *
     * Adjusts join_or_rinfo->norm_selec to compensate for the (redundant)
     * lower-level qual we just added, using a dummy JOIN_INNER SpecialJoinInfo:
     *
     *   init_dummy_sjinfo(&sjinfo,
     *                     bms_difference(join_or_rinfo->clause_relids, rel->relids),
     *                     rel->relids);
     *   orig_selec = clause_selectivity(root, (Node *) join_or_rinfo,
     *                                   0, JOIN_INNER, &sjinfo);
     *   join_or_rinfo->norm_selec = Min(orig_selec / or_selec, 1.0);
     *
     * Depends on clause_selectivity / init_dummy_sjinfo (cost.c, joinrels.c),
     * not yet ported.  Omitted; join size estimates will be unadjusted.
     */
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::list_make2;
    use crate::nodes::bitmapset::bms_make_singleton;
    use crate::nodes::makefuncs::make_orclause;
    use crate::nodes::nodes::NodeTag::{T_OpExpr, T_RelOptInfo, T_RestrictInfo};
    use crate::nodes::pathnodes::{RelOptInfo, RestrictInfo};
    use crate::nodes::pg_list::{linitial, list_length, lsecond};
    use crate::nodes::primnodes::OpExpr;

    /// Allocate a zeroed RelOptInfo with the given relids bitmap.
    unsafe fn make_rel(relids: Relids) -> *mut RelOptInfo {
        let rel = palloc0(core::mem::size_of::<RelOptInfo>()) as *mut RelOptInfo;
        (*rel).r#type = T_RelOptInfo;
        (*rel).relids = relids;
        rel
    }

    /// Build a bare OpExpr node (contents don't matter for these tests).
    unsafe fn make_opexpr() -> *mut Expr {
        let op = palloc0(core::mem::size_of::<OpExpr>()) as *mut OpExpr;
        (*op).xpr.r#type = T_OpExpr;
        op as *mut Expr
    }

    /// Build a RestrictInfo wrapping `clause` with the given clause_relids and
    /// pseudoconstant flag.
    unsafe fn make_rinfo(
        clause: *mut Expr,
        clause_relids: Relids,
        pseudoconstant: bool,
    ) -> *mut RestrictInfo {
        let r = palloc0(core::mem::size_of::<RestrictInfo>()) as *mut RestrictInfo;
        (*r).r#type = T_RestrictInfo;
        (*r).clause = clause;
        (*r).clause_relids = clause_relids;
        (*r).pseudoconstant = pseudoconstant;
        r
    }

    /// A single-rel, non-pseudoconstant OpExpr RestrictInfo is safe to move.
    #[test]
    fn safe_restriction_for_single_rel() {
        unsafe {
            let relids = bms_make_singleton(1);
            let rel = make_rel(relids);
            let rinfo = make_rinfo(make_opexpr(), bms_make_singleton(1), false);

            assert!(is_safe_restriction_clause_for(rinfo, rel));
        }
    }

    /// A pseudoconstant clause is rejected; so is one whose relids differ.
    #[test]
    fn unsafe_restriction_cases() {
        unsafe {
            let rel = make_rel(bms_make_singleton(1));

            // pseudoconstant -> rejected
            let pc = make_rinfo(make_opexpr(), bms_make_singleton(1), true);
            assert!(!is_safe_restriction_clause_for(pc, rel));

            // relids mismatch (mentions rel 2, not rel 1) -> rejected
            let other = make_rinfo(make_opexpr(), bms_make_singleton(2), false);
            assert!(!is_safe_restriction_clause_for(other, rel));
        }
    }

    /// extract_or_clause over an OR of two single-rel RestrictInfo arms yields
    /// an OR clause whose two args are the per-arm clauses.
    ///
    /// We hand-build the or_rinfo->orclause tree directly (an OR BoolExpr whose
    /// args are two RestrictInfo nodes, each a safe single-rel OpExpr), which is
    /// the shape make_sub_restrictinfos would produce.
    #[test]
    fn extract_or_clause_collects_per_arm_clauses() {
        unsafe {
            let rel = make_rel(bms_make_singleton(1));

            let cl_a = make_opexpr();
            let cl_b = make_opexpr();
            let arm_a = make_rinfo(cl_a, bms_make_singleton(1), false);
            let arm_b = make_rinfo(cl_b, bms_make_singleton(1), false);

            // Build the OR(orclause) tree: BoolExpr{OR, args=[arm_a, arm_b]}.
            let orexpr = make_orclause(list_make2!(arm_a, arm_b)) as *mut BoolExpr;
            assert!(is_orclause(orexpr as *const c_void));

            // Wrap in an or_rinfo whose orclause is that tree.
            let or_rinfo = make_rinfo(make_opexpr(), core::ptr::null_mut(), false);
            (*or_rinfo).orclause = orexpr as *mut Expr;

            let result = extract_or_clause(or_rinfo, rel);
            assert!(!result.is_null());
            // Result must be an OR clause with two args (the stripped clauses).
            assert!(is_orclause(result as *const c_void));
            let bool_res = result as *mut BoolExpr;
            assert_eq!(list_length((*bool_res).args), 2);

            // The collected args are the underlying clauses, not the RestrictInfos.
            let first = linitial((*bool_res).args) as *mut Expr;
            let second = lsecond((*bool_res).args) as *mut Expr;
            assert!(first == cl_a && second == cl_b);
        }
    }
}
