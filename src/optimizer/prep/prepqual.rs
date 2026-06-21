//! Translation of postgres/src/backend/optimizer/prep/prepqual.c
//!
//! Routines for preprocessing qualification expressions: Boolean-expression
//! canonicalization (pure tree manipulation over BoolExpr/Const).
//!
//! #include mapping:
//!   "postgres.h"             -> crate::prelude::* (Datum, Oid, DatumGetBool, BoolGetDatum)
//!   "nodes/makefuncs.h"      -> crate::nodes::makefuncs (makeBoolConst, make_andclause,
//!                               make_orclause, make_notclause; make_ands_implicit already
//!                               lives there, NOT redefined here)
//!   "nodes/nodeFuncs.h"      -> inline helpers is_andclause/is_orclause reproduced here
//!                               (nodeFuncs.h macros; same pattern as restrictinfo.rs /
//!                               orclauses.rs / makefuncs.rs)
//!   "optimizer/optimizer.h"  -> public fn signatures (negate_clause, canonicalize_qual)
//!   "utils/lsyscache.h"      -> get_negator (NOT yet ported -> STUBbed below)
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! Translation notes (deviations from the C source):
//!
//! * get_negator (utils/lsyscache.c) is NOT yet ported.  It is STUBbed to
//!   always return InvalidOid (== "no negator known"), which is conservative:
//!   negate_clause then falls through to wrapping the node in an explicit NOT,
//!   exactly as the C code does when no negator exists.  Marked TODO(pg-port).
//! * equal() (equalfuncs.c) is only a partial STUB in this tree (it panics on
//!   most node comparisons).  process_duplicate_ors / find_duplicate_ors are
//!   ported faithfully and *compile*, but the duplicate-OR detection path that
//!   reaches equal() will panic at runtime.  Per the porting plan, no runtime
//!   test exercises that path.
//! * is_andclause/is_orclause are nodeFuncs.h macros, reproduced inline here.
//! * NIL is null_mut(); List/cell handling matches pg_list.rs.

use crate::prelude::*;
use core::ffi::c_void;

use crate::nodes::equalfuncs::equal;
use crate::nodes::makefuncs::{make_andclause, make_notclause, make_orclause, makeBoolConst};
use crate::nodes::nodes::Node;
use crate::nodes::nodes::NodeTag::{
    T_BoolExpr, T_BooleanTest, T_Const, T_NullTest, T_OpExpr, T_ScalarArrayOpExpr,
};
use crate::nodes::pg_list::{
    lappend, lfirst, linitial, list_concat, list_difference, list_length, list_member,
    list_union, List, NIL,
};
use crate::nodes::primnodes::{
    BoolExpr, BooleanTest, Const, Expr, NullTest, OpExpr, ScalarArrayOpExpr, AND_EXPR, IS_FALSE,
    IS_NOT_FALSE, IS_NOT_NULL, IS_NOT_TRUE, IS_NOT_UNKNOWN, IS_NULL, IS_TRUE, IS_UNKNOWN, NOT_EXPR,
    OR_EXPR,
};
use crate::postgres_ext::{InvalidOid, Oid};
use crate::{current_cell, foreach, list_make1, makeNode, nodes::nodes::nodeTag, Assert, IsA};

/*
 * ---------------------------------------------------------------------------
 * Inline clause-shape helpers (nodes/nodeFuncs.h).
 *
 * Reproduced verbatim here because nodeFuncs is not yet translated as its own
 * unit (matches makefuncs.rs / restrictinfo.rs).
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
 * STUB: get_negator (utils/lsyscache.c, included via utils/lsyscache.h).
 *
 * Looks up the negator operator OID for a given operator.  Not yet ported.
 * Returns InvalidOid ("no negator"), which makes negate_clause fall through
 * to wrapping the node in an explicit NOT -- the same behavior the C code
 * exhibits for operators that genuinely lack a negator.
 * ---------------------------------------------------------------------------
 */
// TODO(pg-port): utils/lsyscache.c get_negator
unsafe fn get_negator(_opno: Oid) -> Oid {
    InvalidOid
}

/*
 * negate_clause
 *	  Negate a Boolean expression.
 *
 * Input is a clause to be negated (e.g., the argument of a NOT clause).
 * Returns a new clause equivalent to the negation of the given clause.
 *
 * See the C source for the full rationale; the gist is that we try to push
 * the NOT down (DeMorgan, operator negation, NullTest/BooleanTest inversion)
 * to expose top-level AND/OR structure, falling back to an explicit NOT node.
 */
pub unsafe fn negate_clause(node: *mut Node) -> *mut Node {
    if node.is_null() {
        /* should not happen */
        elog!(ERROR, "can't negate an empty subexpression");
    }
    match nodeTag(node) {
        T_Const => {
            let c = node as *mut Const;
            /* NOT NULL is still NULL */
            if (*c).constisnull {
                return makeBoolConst(false, true);
            }
            /* otherwise pretty easy */
            return makeBoolConst(!DatumGetBool((*c).constvalue), false);
        }
        T_OpExpr => {
            /*
             * Negate operator if possible: (NOT (< A B)) => (>= A B)
             */
            let opexpr = node as *mut OpExpr;
            let negator = get_negator((*opexpr).opno);

            if negator != InvalidOid {
                let newopexpr: *mut OpExpr = makeNode!(OpExpr, T_OpExpr);

                (*newopexpr).opno = negator;
                (*newopexpr).opfuncid = InvalidOid;
                (*newopexpr).opresulttype = (*opexpr).opresulttype;
                (*newopexpr).opretset = (*opexpr).opretset;
                (*newopexpr).opcollid = (*opexpr).opcollid;
                (*newopexpr).inputcollid = (*opexpr).inputcollid;
                (*newopexpr).args = (*opexpr).args;
                (*newopexpr).location = (*opexpr).location;
                return newopexpr as *mut Node;
            }
            /* else fall through */
        }
        T_ScalarArrayOpExpr => {
            /*
             * Negate a ScalarArrayOpExpr if its operator has a negator;
             * for example x = ANY (list) becomes x <> ALL (list)
             */
            let saopexpr = node as *mut ScalarArrayOpExpr;
            let negator = get_negator((*saopexpr).opno);

            if negator != InvalidOid {
                let newopexpr: *mut ScalarArrayOpExpr =
                    makeNode!(ScalarArrayOpExpr, T_ScalarArrayOpExpr);

                (*newopexpr).opno = negator;
                (*newopexpr).opfuncid = InvalidOid;
                (*newopexpr).hashfuncid = InvalidOid;
                (*newopexpr).negfuncid = InvalidOid;
                (*newopexpr).useOr = !(*saopexpr).useOr;
                (*newopexpr).inputcollid = (*saopexpr).inputcollid;
                (*newopexpr).args = (*saopexpr).args;
                (*newopexpr).location = (*saopexpr).location;
                return newopexpr as *mut Node;
            }
            /* else fall through */
        }
        T_BoolExpr => {
            let expr = node as *mut BoolExpr;

            match (*expr).boolop {
                /*--------------------
                 * Apply DeMorgan's Laws:
                 *		(NOT (AND A B)) => (OR (NOT A) (NOT B))
                 *		(NOT (OR A B))	=> (AND (NOT A) (NOT B))
                 * i.e., swap AND for OR and negate each subclause.
                 *
                 * If the input is already AND/OR flat and has no NOT directly
                 * above AND or OR, this transformation preserves those
                 * properties, so we needn't call pull_ors/pull_ands here.
                 *--------------------
                 */
                AND_EXPR => {
                    let mut nargs: *mut List = NIL;
                    foreach!(lc, (*expr).args, {
                        nargs = lappend(
                            nargs,
                            negate_clause(lfirst(current_cell!(lc)) as *mut Node) as *mut c_void,
                        );
                    });
                    return make_orclause(nargs) as *mut Node;
                }
                OR_EXPR => {
                    let mut nargs: *mut List = NIL;
                    foreach!(lc, (*expr).args, {
                        nargs = lappend(
                            nargs,
                            negate_clause(lfirst(current_cell!(lc)) as *mut Node) as *mut c_void,
                        );
                    });
                    return make_andclause(nargs) as *mut Node;
                }
                NOT_EXPR => {
                    /*
                     * NOT underneath NOT: they cancel.  We assume the input is
                     * already simplified, so no need to recurse.
                     */
                    return linitial((*expr).args) as *mut Node;
                }
            }
        }
        T_NullTest => {
            let expr = node as *mut NullTest;

            /*
             * In the rowtype case, the two flavors of NullTest are *not*
             * logical inverses, so we can't simplify.  But it does work for
             * scalar datatypes.
             */
            if !(*expr).argisrow {
                let newexpr: *mut NullTest = makeNode!(NullTest, T_NullTest);

                (*newexpr).arg = (*expr).arg;
                (*newexpr).nulltesttype = if (*expr).nulltesttype == IS_NULL {
                    IS_NOT_NULL
                } else {
                    IS_NULL
                };
                (*newexpr).argisrow = (*expr).argisrow;
                (*newexpr).location = (*expr).location;
                return newexpr as *mut Node;
            }
            /* else fall through */
        }
        T_BooleanTest => {
            let expr = node as *mut BooleanTest;
            let newexpr: *mut BooleanTest = makeNode!(BooleanTest, T_BooleanTest);

            (*newexpr).arg = (*expr).arg;
            (*newexpr).booltesttype = match (*expr).booltesttype {
                IS_TRUE => IS_NOT_TRUE,
                IS_NOT_TRUE => IS_TRUE,
                IS_FALSE => IS_NOT_FALSE,
                IS_NOT_FALSE => IS_FALSE,
                IS_UNKNOWN => IS_NOT_UNKNOWN,
                IS_NOT_UNKNOWN => IS_UNKNOWN,
            };
            (*newexpr).location = (*expr).location;
            return newexpr as *mut Node;
        }
        _ => {
            /* else fall through */
        }
    }

    /*
     * Otherwise we don't know how to simplify this, so just tack on an
     * explicit NOT node.
     */
    make_notclause(node as *mut Expr) as *mut Node
}

/*
 * canonicalize_qual
 *	  Convert a qualification expression to the most useful form.
 *
 * This is primarily intended to be used on top-level WHERE (or JOIN/ON)
 * clauses.  It can also be used on top-level CHECK constraints, for which
 * pass is_check = true.  DO NOT call it on any expression that is not known
 * to be one or the other, as it might apply inappropriate simplifications.
 *
 * NOTE: we assume the input has already been through eval_const_expressions
 * and therefore possesses AND/OR flatness.
 *
 * Returns the modified qualification.
 */
#[no_mangle]
pub unsafe fn canonicalize_qual(qual: *mut Expr, is_check: bool) -> *mut Expr {
    /* Quick exit for empty qual */
    if qual.is_null() {
        return NIL as *mut Expr;
    }

    /* This should not be invoked on quals in implicit-AND format */
    Assert!(!IsA!(qual, T_List));

    /*
     * Pull up redundant subclauses in OR-of-AND trees.  We do this only
     * within the top-level AND/OR structure; there's no point in looking
     * deeper.  Also remove any NULL constants in the top-level structure.
     */
    find_duplicate_ors(qual, is_check)
}

/*
 * pull_ands
 *	  Recursively flatten nested AND clauses into a single and-clause list.
 *
 * Input is the arglist of an AND clause.
 * Returns the rebuilt arglist (note original list structure is not touched).
 */
unsafe fn pull_ands(andlist: *mut List) -> *mut List {
    let mut out_list: *mut List = NIL;

    foreach!(arg, andlist, {
        let subexpr = lfirst(current_cell!(arg)) as *mut Node;

        if is_andclause(subexpr as *const c_void) {
            out_list = list_concat(out_list, pull_ands((*(subexpr as *mut BoolExpr)).args));
        } else {
            out_list = lappend(out_list, subexpr as *mut c_void);
        }
    });
    out_list
}

/*
 * pull_ors
 *	  Recursively flatten nested OR clauses into a single or-clause list.
 *
 * Input is the arglist of an OR clause.
 * Returns the rebuilt arglist (note original list structure is not touched).
 */
unsafe fn pull_ors(orlist: *mut List) -> *mut List {
    let mut out_list: *mut List = NIL;

    foreach!(arg, orlist, {
        let subexpr = lfirst(current_cell!(arg)) as *mut Node;

        if is_orclause(subexpr as *const c_void) {
            out_list = list_concat(out_list, pull_ors((*(subexpr as *mut BoolExpr)).args));
        } else {
            out_list = lappend(out_list, subexpr as *mut c_void);
        }
    });
    out_list
}

/*--------------------
 * The following code attempts to apply the inverse OR distributive law:
 *		((A AND B) OR (A AND C))  =>  (A AND (B OR C))
 * That is, locate OR clauses in which every subclause contains an identical
 * term, and pull out the duplicated terms.  See the C source for rationale.
 *--------------------
 */

/*
 * find_duplicate_ors
 *	  Given a qualification tree with the NOTs pushed down, search for OR
 *	  clauses to which the inverse OR distributive law might apply.  Only the
 *	  top-level AND/OR structure is searched.
 *
 * While at it, we remove any NULL constants within the top-level AND/OR
 * structure (treating NULL like FALSE in WHERE, like TRUE in CHECK).
 *
 * Returns the modified qualification.  AND/OR flatness is preserved.
 */
unsafe fn find_duplicate_ors(qual: *mut Expr, is_check: bool) -> *mut Expr {
    if is_orclause(qual as *const c_void) {
        let mut orlist: *mut List = NIL;

        /* Recurse */
        foreach!(temp, (*(qual as *mut BoolExpr)).args, {
            let mut arg = lfirst(current_cell!(temp)) as *mut Expr;

            arg = find_duplicate_ors(arg, is_check);

            /* Get rid of any constant inputs */
            if !arg.is_null() && IsA!(arg, T_Const) {
                let carg = arg as *mut Const;

                if is_check {
                    /* Within OR in CHECK, drop constant FALSE */
                    if !(*carg).constisnull && !DatumGetBool((*carg).constvalue) {
                        continue;
                    }
                    /* Constant TRUE or NULL, so OR reduces to TRUE */
                    return makeBoolConst(true, false) as *mut Expr;
                } else {
                    /* Within OR in WHERE, drop constant FALSE or NULL */
                    if (*carg).constisnull || !DatumGetBool((*carg).constvalue) {
                        continue;
                    }
                    /* Constant TRUE, so OR reduces to TRUE */
                    return arg;
                }
            }

            orlist = lappend(orlist, arg as *mut c_void);
        });

        /* Flatten any ORs pulled up to just below here */
        orlist = pull_ors(orlist);

        /* Now we can look for duplicate ORs */
        return process_duplicate_ors(orlist);
    } else if is_andclause(qual as *const c_void) {
        let mut andlist: *mut List = NIL;

        /* Recurse */
        foreach!(temp, (*(qual as *mut BoolExpr)).args, {
            let mut arg = lfirst(current_cell!(temp)) as *mut Expr;

            arg = find_duplicate_ors(arg, is_check);

            /* Get rid of any constant inputs */
            if !arg.is_null() && IsA!(arg, T_Const) {
                let carg = arg as *mut Const;

                if is_check {
                    /* Within AND in CHECK, drop constant TRUE or NULL */
                    if (*carg).constisnull || DatumGetBool((*carg).constvalue) {
                        continue;
                    }
                    /* Constant FALSE, so AND reduces to FALSE */
                    return arg;
                } else {
                    /* Within AND in WHERE, drop constant TRUE */
                    if !(*carg).constisnull && DatumGetBool((*carg).constvalue) {
                        continue;
                    }
                    /* Constant FALSE or NULL, so AND reduces to FALSE */
                    return makeBoolConst(false, false) as *mut Expr;
                }
            }

            andlist = lappend(andlist, arg as *mut c_void);
        });

        /* Flatten any ANDs introduced just below here */
        andlist = pull_ands(andlist);

        /* AND of no inputs reduces to TRUE */
        if andlist == NIL {
            return makeBoolConst(true, false) as *mut Expr;
        }

        /* Single-expression AND just reduces to that expression */
        if list_length(andlist) == 1 {
            return linitial(andlist) as *mut Expr;
        }

        /* Else we still need an AND node */
        make_andclause(andlist)
    } else {
        qual
    }
}

/*
 * process_duplicate_ors
 *	  Given a list of exprs which are ORed together, try to apply the inverse
 *	  OR distributive law.
 *
 * Returns the resulting expression (could be an AND clause, an OR clause, or
 * maybe even a single subexpression).
 */
unsafe fn process_duplicate_ors(orlist: *mut List) -> *mut Expr {
    let mut reference: *mut List = NIL;
    let mut num_subclauses: c_int = 0;
    let mut winners: *mut List;
    let mut neworlist: *mut List;

    /* OR of no inputs reduces to FALSE */
    if orlist == NIL {
        return makeBoolConst(false, false) as *mut Expr;
    }

    /* Single-expression OR just reduces to that expression */
    if list_length(orlist) == 1 {
        return linitial(orlist) as *mut Expr;
    }

    /*
     * Choose the shortest AND clause as the reference list --- obviously, any
     * subclause not in this clause isn't in all the clauses.  If we find a
     * clause that's not an AND, we can treat it as a one-element AND clause,
     * which necessarily wins as shortest.
     */
    foreach!(temp, orlist, {
        let clause = lfirst(current_cell!(temp)) as *mut Expr;

        if is_andclause(clause as *const c_void) {
            let subclauses = (*(clause as *mut BoolExpr)).args;
            let nclauses = list_length(subclauses);

            if reference == NIL || nclauses < num_subclauses {
                reference = subclauses;
                num_subclauses = nclauses;
            }
        } else {
            reference = list_make1!(clause as *mut c_void);
            break;
        }
    });

    /*
     * Just in case, eliminate any duplicates in the reference list.
     */
    reference = list_union(NIL, reference);

    /*
     * Check each element of the reference list to see if it's in all the OR
     * clauses.  Build a new list of winning clauses.
     */
    winners = NIL;
    foreach!(temp, reference, {
        let refclause = lfirst(current_cell!(temp)) as *mut Expr;
        let mut win = true;

        foreach!(temp2, orlist, {
            let clause = lfirst(current_cell!(temp2)) as *mut Expr;

            if is_andclause(clause as *const c_void) {
                if !list_member((*(clause as *mut BoolExpr)).args, refclause as *const c_void) {
                    win = false;
                    break;
                }
            } else if !equal(refclause as *const c_void, clause as *const c_void) {
                win = false;
                break;
            }
        });

        if win {
            winners = lappend(winners, refclause as *mut c_void);
        }
    });

    /*
     * If no winners, we can't transform the OR
     */
    if winners == NIL {
        return make_orclause(orlist);
    }

    /*
     * Generate new OR list consisting of the remaining sub-clauses.
     *
     * If any clause degenerates to empty, then we have a situation like (A
     * AND B) OR (A), which can be reduced to just A.
     *
     * Note that because we use list_difference, any multiple occurrences of a
     * winning clause in an AND sub-clause will be removed automatically.
     */
    neworlist = NIL;
    foreach!(temp, orlist, {
        let clause = lfirst(current_cell!(temp)) as *mut Expr;

        if is_andclause(clause as *const c_void) {
            let mut subclauses = (*(clause as *mut BoolExpr)).args;

            subclauses = list_difference(subclauses, winners);
            if subclauses != NIL {
                if list_length(subclauses) == 1 {
                    neworlist = lappend(neworlist, linitial(subclauses));
                } else {
                    neworlist = lappend(neworlist, make_andclause(subclauses) as *mut c_void);
                }
            } else {
                neworlist = NIL; /* degenerate case, see above */
                break;
            }
        } else if !list_member(winners, clause as *const c_void) {
            neworlist = lappend(neworlist, clause as *mut c_void);
        } else {
            neworlist = NIL; /* degenerate case, see above */
            break;
        }
    });

    /*
     * Append reduced OR to the winners list, if it's not degenerate, handling
     * the special case of one element correctly (can that really happen?).
     * Also be careful to maintain AND/OR flatness in case we pulled up a
     * sub-sub-OR-clause.
     */
    if neworlist != NIL {
        if list_length(neworlist) == 1 {
            winners = lappend(winners, linitial(neworlist));
        } else {
            winners = lappend(winners, make_orclause(pull_ors(neworlist)) as *mut c_void);
        }
    }

    /*
     * And return the constructed AND clause, again being wary of a single
     * element and AND/OR flatness.
     */
    if list_length(winners) == 1 {
        linitial(winners) as *mut Expr
    } else {
        make_andclause(pull_ands(winners))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nodes::makefuncs::makeBoolExpr;
    use crate::list_make2;

    /// Build a trivial non-Const leaf node we can identify by tag.  We use a
    /// NullTest (IS_NULL over a NULL arg) as a stand-in "atom" so that
    /// negate_clause wraps/inverts it without needing get_negator or equal().
    unsafe fn make_atom(ttype: crate::nodes::primnodes::NullTestType) -> *mut Node {
        let nt: *mut NullTest = makeNode!(NullTest, T_NullTest);
        (*nt).arg = core::ptr::null_mut();
        (*nt).nulltesttype = ttype;
        (*nt).argisrow = false;
        (*nt).location = -1;
        nt as *mut Node
    }

    /*
     * negate_clause of NOT(a AND b) should, via DeMorgan, produce
     * (NOT a OR NOT b): an OR_EXPR BoolExpr with exactly 2 args.  Here we
     * actually call negate_clause directly on (a AND b), which is what the
     * NOT case delegates to.  Each arg is a NullTest, so negate_clause inverts
     * it to the opposite NullTest (no get_negator/equal needed).
     */
    #[test]
    fn negate_and_demorgan() {
        unsafe {
            let a = make_atom(IS_NULL);
            let b = make_atom(IS_NULL);
            let and_expr = makeBoolExpr(AND_EXPR, list_make2!(a as *mut c_void, b as *mut c_void), -1);

            let neg = negate_clause(and_expr as *mut Node);

            // Result must be an OR BoolExpr.
            assert_eq!(nodeTag(neg), T_BoolExpr);
            let be = neg as *mut BoolExpr;
            assert_eq!((*be).boolop, OR_EXPR);
            // With exactly 2 args.
            assert_eq!(list_length((*be).args), 2);
            // Each arg is an inverted NullTest (IS_NOT_NULL), since DeMorgan
            // negates each subclause.
            let arg0 = linitial((*be).args) as *mut NullTest;
            assert_eq!(nodeTag(arg0 as *mut Node), T_NullTest);
            assert_eq!((*arg0).nulltesttype, IS_NOT_NULL);
        }
    }

    /*
     * negate_clause of OR(a, b) should, via DeMorgan, produce
     * (NOT a AND NOT b): an AND_EXPR BoolExpr with exactly 2 args.
     */
    #[test]
    fn negate_or_demorgan() {
        unsafe {
            let a = make_atom(IS_NOT_NULL);
            let b = make_atom(IS_NOT_NULL);
            let or_expr = makeBoolExpr(OR_EXPR, list_make2!(a as *mut c_void, b as *mut c_void), -1);

            let neg = negate_clause(or_expr as *mut Node);

            assert_eq!(nodeTag(neg), T_BoolExpr);
            let be = neg as *mut BoolExpr;
            assert_eq!((*be).boolop, AND_EXPR);
            assert_eq!(list_length((*be).args), 2);
            let arg0 = linitial((*be).args) as *mut NullTest;
            assert_eq!((*arg0).nulltesttype, IS_NULL);
        }
    }

    /*
     * pull_ands flattens nested ANDs into a single flat arglist.
     * Input arglist: [ atom_x, AND(atom_y, atom_z) ] -> flattened to
     * [ atom_x, atom_y, atom_z ] (length 3).
     */
    #[test]
    fn pull_ands_flattens() {
        unsafe {
            let x = make_atom(IS_NULL);
            let y = make_atom(IS_NULL);
            let z = make_atom(IS_NULL);

            let inner = makeBoolExpr(AND_EXPR, list_make2!(y as *mut c_void, z as *mut c_void), -1);
            let arglist = list_make2!(x as *mut c_void, inner as *mut c_void);

            let flat = pull_ands(arglist);
            assert_eq!(list_length(flat), 3);
        }
    }

    /*
     * pull_ors flattens nested ORs similarly.
     * [ x, OR(y, z) ] -> [ x, y, z ].
     */
    #[test]
    fn pull_ors_flattens() {
        unsafe {
            let x = make_atom(IS_NULL);
            let y = make_atom(IS_NULL);
            let z = make_atom(IS_NULL);

            let inner = makeBoolExpr(OR_EXPR, list_make2!(y as *mut c_void, z as *mut c_void), -1);
            let arglist = list_make2!(x as *mut c_void, inner as *mut c_void);

            let flat = pull_ors(arglist);
            assert_eq!(list_length(flat), 3);
        }
    }

    /*
     * negate_clause of a bare Const TRUE yields Const FALSE.
     */
    #[test]
    fn negate_const_true() {
        unsafe {
            let t = makeBoolConst(true, false);
            let neg = negate_clause(t);
            assert_eq!(nodeTag(neg), T_Const);
            let c = neg as *mut Const;
            assert!(!(*c).constisnull);
            assert_eq!(DatumGetBool((*c).constvalue), false);
        }
    }
}
