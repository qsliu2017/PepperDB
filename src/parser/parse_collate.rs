//! src/backend/parser/parse_collate.c
//!
//! parse_collate.c
//!		Routines for assigning collation information.
//!
//! We choose to handle collation analysis in a post-pass over the output
//! of expression parse analysis.  This is because we need more state to
//! perform this processing than is needed in the finished tree.  If we
//! did it on-the-fly while building the tree, all that state would have
//! to be kept in expression node trees permanently.  This way, the extra
//! storage is just local variables in this recursive routine.
//!
//! The info that is actually saved in the finished tree is:
//! 1. The output collation of each expression node, or InvalidOid if it
//! returns a noncollatable data type.  This can also be InvalidOid if the
//! result type is collatable but the collation is indeterminate.
//! 2. The collation to be used in executing each function.  InvalidOid means
//! that there are no collatable inputs or their collation is indeterminate.
//! This value is only stored in node types that might call collation-using
//! functions.
//!
//! You might think we could get away with storing only one collation per
//! node, but the two concepts really need to be kept distinct.  Otherwise
//! it's too confusing when a function produces a collatable output type but
//! has no collatable inputs or produces noncollatable output from collatable
//! inputs.
//!
//! Cases with indeterminate collation might result in an error being thrown
//! at runtime.  If we knew exactly which functions require collation
//! information, we could throw those errors at parse time instead.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!	  src/backend/parser/parse_collate.c

use crate::prelude::*;

use std::ffi::c_int;

use crate::postgres_ext::Oid;

use crate::nodes::nodes::{nodeTag, NodeTag};
use crate::nodes::pg_list::{lfirst, NIL};
use crate::{current_cell, foreach, forboth, lfirst_node, linitial_node, list_make2, IsA};

// ----------------------------------------------------------------------------
// Local type stubs for unported dependencies
// ----------------------------------------------------------------------------

// from parser/parse_node.h
pub type ParseState = crate::parser::parse_node::ParseState;

// Node types referenced via casts; faithful pointer-cast usage suffices.
type Query = crate::nodes::parsenodes::Query;
type Node = crate::nodes::nodes::Node;
type List = crate::nodes::pg_list::List;
type ListCell = crate::nodes::pg_list::ListCell;

/*
 * Collation strength (the SQL standard calls this "derivation").  Order is
 * chosen to allow comparisons to work usefully.  Note: the standard doesn't
 * seem to distinguish between NONE and CONFLICT.
 */
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(C)]
enum CollateStrength {
    COLLATE_NONE = 0,     /* expression is of a noncollatable datatype */
    COLLATE_IMPLICIT = 1, /* collation was derived implicitly */
    COLLATE_CONFLICT = 2, /* we had a conflict of implicit collations */
    COLLATE_EXPLICIT = 3, /* collation was derived explicitly */
}

use CollateStrength::*;

#[repr(C)]
struct assign_collations_context {
    pstate: *mut ParseState, /* parse state (for error reporting) */
    collation: Oid,          /* OID of current collation, if any */
    strength: CollateStrength, /* strength of current collation choice */
    location: c_int,         /* location of expr that set collation */
    /* Remaining fields are only valid when strength == COLLATE_CONFLICT */
    collation2: Oid, /* OID of conflicting collation */
    location2: c_int, /* location of expr that set collation2 */
}

/*
 * assign_query_collations()
 *		Mark all expressions in the given Query with collation information.
 *
 * This should be applied to each Query after completion of parse analysis
 * for expressions.  Note that we do not recurse into sub-Queries, since
 * those should have been processed when built.
 */
pub unsafe fn assign_query_collations(pstate: *mut ParseState, query: *mut Query) {
    /*
     * We just use query_tree_walker() to visit all the contained expressions.
     * We can skip the rangetable and CTE subqueries, though, since RTEs and
     * subqueries had better have been processed already (else Vars referring
     * to them would not get created with the right collation).
     */
    query_tree_walker(
        query,
        assign_query_collations_walker as *const (),
        pstate as *mut c_void,
        QTW_IGNORE_RANGE_TABLE | QTW_IGNORE_CTE_SUBQUERIES,
    );
}

/*
 * Walker for assign_query_collations
 *
 * Each expression found by query_tree_walker is processed independently.
 * Note that query_tree_walker may pass us a whole List, such as the
 * targetlist, in which case each subexpression must be processed
 * independently --- we don't want to bleat if two different targetentries
 * have different collations.
 */
unsafe extern "C" fn assign_query_collations_walker(
    node: *mut Node,
    pstate: *mut ParseState,
) -> bool {
    /* Need do nothing for empty subexpressions */
    if node.is_null() {
        return false;
    }

    /*
     * We don't want to recurse into a set-operations tree; it's already been
     * fully processed in transformSetOperationStmt.
     */
    if IsA!(node, T_SetOperationStmt) {
        return false;
    }

    if IsA!(node, T_List) {
        assign_list_collations(pstate, node as *mut List);
    } else {
        assign_expr_collations(pstate, node);
    }

    false
}

/*
 * assign_list_collations()
 *		Mark all nodes in the list of expressions with collation information.
 *
 * The list member expressions are processed independently; they do not have
 * to share a common collation.
 */
pub unsafe fn assign_list_collations(pstate: *mut ParseState, exprs: *mut List) {
    foreach!(lc, exprs, {
        let node = lfirst(current_cell!(lc)) as *mut Node;

        assign_expr_collations(pstate, node);
    });
}

/*
 * assign_expr_collations()
 *		Mark all nodes in the given expression tree with collation information.
 *
 * This is exported for the benefit of various utility commands that process
 * expressions without building a complete Query.  It should be applied after
 * calling transformExpr() plus any expression-modifying operations such as
 * coerce_to_boolean().
 */
pub unsafe fn assign_expr_collations(pstate: *mut ParseState, expr: *mut Node) {
    let mut context: assign_collations_context = std::mem::zeroed();

    /* initialize context for tree walk */
    context.pstate = pstate;
    context.collation = InvalidOid;
    context.strength = COLLATE_NONE;
    context.location = -1;

    /* and away we go */
    assign_collations_walker(expr, &mut context);
}

/*
 * select_common_collation()
 *		Identify a common collation for a list of expressions.
 *
 * The expressions should all return the same datatype, else this is not
 * terribly meaningful.
 *
 * none_ok means that it is permitted to return InvalidOid, indicating that
 * no common collation could be identified, even for collatable datatypes.
 * Otherwise, an error is thrown for conflict of implicit collations.
 *
 * In theory, none_ok = true reflects the rules of SQL standard clause "Result
 * of data type combinations", none_ok = false reflects the rules of clause
 * "Collation determination" (in some cases invoked via "Grouping
 * operations").
 */
pub unsafe fn select_common_collation(
    pstate: *mut ParseState,
    exprs: *mut List,
    none_ok: bool,
) -> Oid {
    let mut context: assign_collations_context = std::mem::zeroed();

    /* initialize context for tree walk */
    context.pstate = pstate;
    context.collation = InvalidOid;
    context.strength = COLLATE_NONE;
    context.location = -1;

    /* and away we go */
    assign_collations_walker(exprs as *mut Node, &mut context);

    /* deal with collation conflict */
    if context.strength == COLLATE_CONFLICT {
        if none_ok {
            return InvalidOid;
        }
        ereport!(
            ERROR,
            "collation mismatch between implicit collations"
        );
        unreachable!();
    }

    /*
     * Note: if strength is still COLLATE_NONE, we'll return InvalidOid, but
     * that's okay because it must mean none of the expressions returned
     * collatable datatypes.
     */
    context.collation
}

/*
 * assign_collations_walker()
 *		Recursive guts of collation processing.
 *
 * Nodes with no children (eg, Vars, Consts, Params) must have been marked
 * when built.  All upper-level nodes are marked here.
 *
 * Note: if this is invoked directly on a List, it will attempt to infer a
 * common collation for all the list members.  In particular, it will throw
 * error if there are conflicting explicit collations for different members.
 */
unsafe extern "C" fn assign_collations_walker(
    node: *mut Node,
    context: *mut assign_collations_context,
) -> bool {
    let context = &mut *context;
    let mut loccontext: assign_collations_context = std::mem::zeroed();
    let collation: Oid;
    let strength: CollateStrength;
    let location: c_int;

    /* Need do nothing for empty subexpressions */
    if node.is_null() {
        return false;
    }

    /*
     * Prepare for recursion.  For most node types, though not all, the first
     * thing we do is recurse to process all nodes below this one. Each level
     * of the tree has its own local context.
     */
    loccontext.pstate = context.pstate;
    loccontext.collation = InvalidOid;
    loccontext.strength = COLLATE_NONE;
    loccontext.location = -1;
    /* Set these fields just to suppress uninitialized-value warnings: */
    loccontext.collation2 = InvalidOid;
    loccontext.location2 = -1;

    /*
     * Recurse if appropriate, then determine the collation for this node.
     *
     * Note: the general cases are at the bottom of the switch, after various
     * special cases.
     */
    match nodeTag(node) {
        NodeTag::T_CollateExpr => {
            /*
             * COLLATE sets an explicitly derived collation, regardless of
             * what the child state is.  But we must recurse to set up
             * collation info below here.
             */
            let expr = node as *mut CollateExpr;

            expression_tree_walker(
                node,
                assign_collations_walker as *const (),
                &mut loccontext as *mut _ as *mut c_void,
            );

            collation = (*expr).collOid;
            Assert!(OidIsValid(collation));
            strength = COLLATE_EXPLICIT;
            location = (*expr).location;
        }
        NodeTag::T_FieldSelect => {
            /*
             * For FieldSelect, the result has the field's declared
             * collation, independently of what happened in the arguments.
             * (The immediate argument must be composite and thus not
             * collatable, anyhow.)  The field's collation was already
             * looked up and saved in the node.
             */
            let expr = node as *mut FieldSelect;

            /* ... but first, recurse */
            expression_tree_walker(
                node,
                assign_collations_walker as *const (),
                &mut loccontext as *mut _ as *mut c_void,
            );

            if OidIsValid((*expr).resultcollid) {
                /* Node's result type is collatable. */
                /* Pass up field's collation as an implicit choice. */
                collation = (*expr).resultcollid;
                strength = COLLATE_IMPLICIT;
                location = exprLocation(node);
            } else {
                /* Node's result type isn't collatable. */
                collation = InvalidOid;
                strength = COLLATE_NONE;
                location = -1; /* won't be used */
            }
        }
        NodeTag::T_RowExpr => {
            /*
             * RowExpr is a special case because the subexpressions are
             * independent: we don't want to complain if some of them have
             * incompatible explicit collations.
             */
            let expr = node as *mut RowExpr;

            assign_list_collations(context.pstate, (*expr).args);

            /*
             * Since the result is always composite and therefore never
             * has a collation, we can just stop here: this node has no
             * impact on the collation of its parent.
             */
            return false; /* done */
        }
        NodeTag::T_RowCompareExpr => {
            /*
             * For RowCompare, we have to find the common collation of
             * each pair of input columns and build a list.  If we can't
             * find a common collation, we just put InvalidOid into the
             * list, which may or may not cause an error at runtime.
             */
            let expr = node as *mut RowCompareExpr;
            let mut colls: *mut List = NIL;

            forboth!(l, (*expr).largs, r, (*expr).rargs, {
                let le = lfirst(l) as *mut Node;
                let re = lfirst(r) as *mut Node;
                let coll: Oid;

                coll = select_common_collation(context.pstate, list_make2!(le as *mut c_void, re as *mut c_void), true);
                colls = lappend_oid(colls, coll);
            });
            (*expr).inputcollids = colls;

            /*
             * Since the result is always boolean and therefore never has
             * a collation, we can just stop here: this node has no impact
             * on the collation of its parent.
             */
            return false; /* done */
        }
        NodeTag::T_CoerceToDomain => {
            /*
             * If the domain declaration included a non-default COLLATE
             * spec, then use that collation as the output collation of
             * the coercion.  Otherwise allow the input collation to
             * bubble up.  (The input should be of the domain's base type,
             * therefore we don't need to worry about it not being
             * collatable when the domain is.)
             */
            let expr = node as *mut CoerceToDomain;
            let typcollation: Oid = get_typcollation((*expr).resulttype);

            /* ... but first, recurse */
            expression_tree_walker(
                node,
                assign_collations_walker as *const (),
                &mut loccontext as *mut _ as *mut c_void,
            );

            if OidIsValid(typcollation) {
                /* Node's result type is collatable. */
                if typcollation == DEFAULT_COLLATION_OID {
                    /* Collation state bubbles up from child. */
                    collation = loccontext.collation;
                    strength = loccontext.strength;
                    location = loccontext.location;
                } else {
                    /* Use domain's collation as an implicit choice. */
                    collation = typcollation;
                    strength = COLLATE_IMPLICIT;
                    location = exprLocation(node);
                }
            } else {
                /* Node's result type isn't collatable. */
                collation = InvalidOid;
                strength = COLLATE_NONE;
                location = -1; /* won't be used */
            }

            /*
             * Save the state into the expression node.  We know it
             * doesn't care about input collation.
             */
            if strength == COLLATE_CONFLICT {
                exprSetCollation(node, InvalidOid);
            } else {
                exprSetCollation(node, collation);
            }
        }
        NodeTag::T_TargetEntry => {
            expression_tree_walker(
                node,
                assign_collations_walker as *const (),
                &mut loccontext as *mut _ as *mut c_void,
            );

            /*
             * TargetEntry can have only one child, and should bubble that
             * state up to its parent.  We can't use the general-case code
             * below because exprType and friends don't work on TargetEntry.
             */
            collation = loccontext.collation;
            strength = loccontext.strength;
            location = loccontext.location;

            /*
             * Throw error if the collation is indeterminate for a TargetEntry
             * that is a sort/group target.  We prefer to do this now, instead
             * of leaving the comparison functions to fail at runtime, because
             * we can give a syntax error pointer to help locate the problem.
             * There are some cases where there might not be a failure, for
             * example if the planner chooses to use hash aggregation instead
             * of sorting for grouping; but it seems better to predictably
             * throw an error.  (Compare transformSetOperationTree, which will
             * throw error for indeterminate collation of set-op columns, even
             * though the planner might be able to implement the set-op
             * without sorting.)
             */
            if strength == COLLATE_CONFLICT && (*(node as *mut TargetEntry)).ressortgroupref != 0 {
                ereport!(
                    ERROR,
                    "collation mismatch between implicit collations"
                );
                unreachable!();
            }
        }
        NodeTag::T_InferenceElem
        | NodeTag::T_RangeTblRef
        | NodeTag::T_JoinExpr
        | NodeTag::T_FromExpr
        | NodeTag::T_OnConflictExpr
        | NodeTag::T_SortGroupClause
        | NodeTag::T_MergeAction => {
            expression_tree_walker(
                node,
                assign_collations_walker as *const (),
                &mut loccontext as *mut _ as *mut c_void,
            );

            /*
             * When we're invoked on a query's jointree, we don't need to do
             * anything with join nodes except recurse through them to process
             * WHERE/ON expressions.  So just stop here.  Likewise, we don't
             * need to do anything when invoked on sort/group lists.
             */
            return false;
        }
        NodeTag::T_Query => {
            /*
             * We get here when we're invoked on the Query belonging to a
             * SubLink.  Act as though the Query returns its first output
             * column, which indeed is what it does for EXPR_SUBLINK and
             * ARRAY_SUBLINK cases.  In the cases where the SubLink
             * returns boolean, this info will be ignored.  Special case:
             * in EXISTS, the Query might return no columns, in which case
             * we need do nothing.
             *
             * We needn't recurse, since the Query is already processed.
             */
            let qtree = node as *mut Query;
            let tent: *mut TargetEntry;

            if (*qtree).targetList == NIL {
                return false;
            }
            tent = linitial_node!(TargetEntry, T_TargetEntry, (*qtree).targetList);
            if (*tent).resjunk {
                return false;
            }

            collation = exprCollation((*tent).expr as *mut Node);
            /* collation doesn't change if it's converted to array */
            strength = COLLATE_IMPLICIT;
            location = exprLocation((*tent).expr as *mut Node);
        }
        NodeTag::T_List => {
            expression_tree_walker(
                node,
                assign_collations_walker as *const (),
                &mut loccontext as *mut _ as *mut c_void,
            );

            /*
             * When processing a list, collation state just bubbles up from
             * the list elements.
             */
            collation = loccontext.collation;
            strength = loccontext.strength;
            location = loccontext.location;
        }

        NodeTag::T_Var
        | NodeTag::T_Const
        | NodeTag::T_Param
        | NodeTag::T_CoerceToDomainValue
        | NodeTag::T_CaseTestExpr
        | NodeTag::T_SetToDefault
        | NodeTag::T_CurrentOfExpr => {
            /*
             * General case for childless expression nodes.  These should
             * already have a collation assigned; it is not this function's
             * responsibility to look into the catalogs for base-case
             * information.
             */
            collation = exprCollation(node);

            /*
             * Note: in most cases, there will be an assigned collation
             * whenever type_is_collatable(exprType(node)); but an exception
             * occurs for a Var referencing a subquery output column for which
             * a unique collation was not determinable.  That may lead to a
             * runtime failure if a collation-sensitive function is applied to
             * the Var.
             */

            if OidIsValid(collation) {
                strength = COLLATE_IMPLICIT;
            } else {
                strength = COLLATE_NONE;
            }
            location = exprLocation(node);
        }

        _ => {
            /*
             * General case for most expression nodes with children. First
             * recurse, then figure out what to assign to this node.
             */
            let typcollation: Oid;

            /*
             * For most node types, we want to treat all the child
             * expressions alike; but there are a few exceptions, hence
             * this inner switch.
             */
            match nodeTag(node) {
                NodeTag::T_Aggref => {
                    /*
                     * Aggref is messy enough that we give it its own
                     * function, in fact three of them.  The FILTER
                     * clause is independent of the rest of the
                     * aggregate, however, so it can be processed
                     * separately.
                     */
                    let aggref = node as *mut Aggref;

                    match (*aggref).aggkind as u8 as char {
                        AGGKIND_NORMAL => {
                            assign_aggregate_collations(aggref, &mut loccontext);
                        }
                        AGGKIND_ORDERED_SET => {
                            assign_ordered_set_collations(aggref, &mut loccontext);
                        }
                        AGGKIND_HYPOTHETICAL => {
                            assign_hypothetical_collations(aggref, &mut loccontext);
                        }
                        _ => {
                            elog!(ERROR, "unrecognized aggkind: {}", (*aggref).aggkind as c_int);
                        }
                    }

                    assign_expr_collations(context.pstate, (*aggref).aggfilter as *mut Node);
                }
                NodeTag::T_WindowFunc => {
                    /*
                     * WindowFunc requires special processing only for
                     * its aggfilter clause, as for aggregates.
                     */
                    let wfunc = node as *mut WindowFunc;

                    assign_collations_walker((*wfunc).args as *mut Node, &mut loccontext);

                    assign_expr_collations(context.pstate, (*wfunc).aggfilter as *mut Node);
                }
                NodeTag::T_CaseExpr => {
                    /*
                     * CaseExpr is a special case because we do not
                     * want to recurse into the test expression (if
                     * any).  It was already marked with collations
                     * during transformCaseExpr, and furthermore its
                     * collation is not relevant to the result of the
                     * CASE --- only the output expressions are.
                     */
                    let expr = node as *mut CaseExpr;

                    foreach!(lc, (*expr).args, {
                        let when = lfirst_node!(CaseWhen, T_CaseWhen, current_cell!(lc));

                        /*
                         * The condition expressions mustn't affect
                         * the CASE's result collation either; but
                         * since they are known to yield boolean, it's
                         * safe to recurse directly on them --- they
                         * won't change loccontext.
                         */
                        assign_collations_walker((*when).expr as *mut Node, &mut loccontext);
                        assign_collations_walker((*when).result as *mut Node, &mut loccontext);
                    });
                    assign_collations_walker((*expr).defresult as *mut Node, &mut loccontext);
                }
                NodeTag::T_SubscriptingRef => {
                    /*
                     * The subscripts are treated as independent
                     * expressions not contributing to the node's
                     * collation.  Only the container, and the source
                     * expression if any, contribute.  (This models
                     * the old behavior, in which the subscripts could
                     * be counted on to be integers and thus not
                     * contribute anything.)
                     */
                    let sbsref = node as *mut SubscriptingRef;

                    assign_expr_collations(
                        context.pstate,
                        (*sbsref).refupperindexpr as *mut Node,
                    );
                    assign_expr_collations(
                        context.pstate,
                        (*sbsref).reflowerindexpr as *mut Node,
                    );
                    assign_collations_walker((*sbsref).refexpr as *mut Node, &mut loccontext);
                    assign_collations_walker(
                        (*sbsref).refassgnexpr as *mut Node,
                        &mut loccontext,
                    );
                }
                _ => {
                    /*
                     * Normal case: all child expressions contribute
                     * equally to loccontext.
                     */
                    expression_tree_walker(
                        node,
                        assign_collations_walker as *const (),
                        &mut loccontext as *mut _ as *mut c_void,
                    );
                }
            }

            /*
             * Now figure out what collation to assign to this node.
             */
            typcollation = get_typcollation(exprType(node));
            if OidIsValid(typcollation) {
                /* Node's result is collatable; what about its input? */
                if loccontext.strength > COLLATE_NONE {
                    /* Collation state bubbles up from children. */
                    collation = loccontext.collation;
                    strength = loccontext.strength;
                    location = loccontext.location;
                } else {
                    /*
                     * Collatable output produced without any collatable
                     * input.  Use the type's collation (which is usually
                     * DEFAULT_COLLATION_OID, but might be different for a
                     * domain).
                     */
                    collation = typcollation;
                    strength = COLLATE_IMPLICIT;
                    location = exprLocation(node);
                }
            } else {
                /* Node's result type isn't collatable. */
                collation = InvalidOid;
                strength = COLLATE_NONE;
                location = -1; /* won't be used */
            }

            /*
             * Save the result collation into the expression node. If the
             * state is COLLATE_CONFLICT, we'll set the collation to
             * InvalidOid, which might result in an error at runtime.
             */
            if strength == COLLATE_CONFLICT {
                exprSetCollation(node, InvalidOid);
            } else {
                exprSetCollation(node, collation);
            }

            /*
             * Likewise save the input collation, which is the one that
             * any function called by this node should use.
             */
            if loccontext.strength == COLLATE_CONFLICT {
                exprSetInputCollation(node, InvalidOid);
            } else {
                exprSetInputCollation(node, loccontext.collation);
            }
        }
    }

    /*
     * Now, merge my information into my parent's state.
     */
    merge_collation_state(
        collation,
        strength,
        location,
        loccontext.collation2,
        loccontext.location2,
        context,
    );

    false
}

/*
 * Merge collation state of a subexpression into the context for its parent.
 */
unsafe fn merge_collation_state(
    collation: Oid,
    strength: CollateStrength,
    location: c_int,
    collation2: Oid,
    location2: c_int,
    context: &mut assign_collations_context,
) {
    /*
     * If the collation strength for this node is different from what's
     * already in *context, then this node either dominates or is dominated by
     * earlier siblings.
     */
    if strength > context.strength {
        /* Override previous parent state */
        context.collation = collation;
        context.strength = strength;
        context.location = location;
        /* Bubble up error info if applicable */
        if strength == COLLATE_CONFLICT {
            context.collation2 = collation2;
            context.location2 = location2;
        }
    } else if strength == context.strength {
        /* Merge, or detect error if there's a collation conflict */
        match strength {
            COLLATE_NONE => {
                /* Nothing + nothing is still nothing */
            }
            COLLATE_IMPLICIT => {
                if collation != context.collation {
                    /*
                     * Non-default implicit collation always beats default.
                     */
                    if context.collation == DEFAULT_COLLATION_OID {
                        /* Override previous parent state */
                        context.collation = collation;
                        context.strength = strength;
                        context.location = location;
                    } else if collation != DEFAULT_COLLATION_OID {
                        /*
                         * Oops, we have a conflict.  We cannot throw error
                         * here, since the conflict could be resolved by a
                         * later sibling CollateExpr, or the parent might not
                         * care about collation anyway.  Return enough info to
                         * throw the error later, if needed.
                         */
                        context.strength = COLLATE_CONFLICT;
                        context.collation2 = collation;
                        context.location2 = location;
                    }
                }
            }
            COLLATE_CONFLICT => {
                /* We're still conflicted ... */
            }
            COLLATE_EXPLICIT => {
                if collation != context.collation {
                    /*
                     * Oops, we have a conflict of explicit COLLATE clauses.
                     * Here we choose to throw error immediately; that is what
                     * the SQL standard says to do, and there's no good reason
                     * to be less strict.
                     */
                    ereport!(
                        ERROR,
                        "collation mismatch between explicit collations"
                    );
                    unreachable!();
                }
            }
        }
    }
}

/*
 * Aggref is a special case because expressions used only for ordering
 * shouldn't be taken to conflict with each other or with regular args,
 * indeed shouldn't affect the aggregate's result collation at all.
 * We handle this by applying assign_expr_collations() to them rather than
 * passing down our loccontext.
 *
 * Note that we recurse to each TargetEntry, not directly to its contained
 * expression, so that the case above for T_TargetEntry will complain if we
 * can't resolve a collation for an ORDER BY item (whether or not it is also
 * a normal aggregate arg).
 *
 * We need not recurse into the aggorder or aggdistinct lists, because those
 * contain only SortGroupClause nodes which we need not process.
 */
unsafe fn assign_aggregate_collations(
    aggref: *mut Aggref,
    loccontext: &mut assign_collations_context,
) {
    /* Plain aggregates have no direct args */
    Assert!((*aggref).aggdirectargs == NIL);

    /* Process aggregated args, holding resjunk ones at arm's length */
    foreach!(lc, (*aggref).args, {
        let tle = lfirst_node!(TargetEntry, T_TargetEntry, current_cell!(lc));

        if (*tle).resjunk {
            assign_expr_collations(loccontext.pstate, tle as *mut Node);
        } else {
            assign_collations_walker(tle as *mut Node, loccontext);
        }
    });
}

/*
 * For ordered-set aggregates, it's somewhat unclear how best to proceed.
 * The spec-defined inverse distribution functions have only one sort column
 * and don't return collatable types, but this is clearly too restrictive in
 * the general case.  Our solution is to consider that the aggregate's direct
 * arguments contribute normally to determination of the aggregate's own
 * collation, while aggregated arguments contribute only when the aggregate
 * is designed to have exactly one aggregated argument (i.e., it has a single
 * aggregated argument and is non-variadic).  If it can have more than one
 * aggregated argument, we process the aggregated arguments as independent
 * sort columns.  This avoids throwing error for something like
 *		agg(...) within group (order by x collate "foo", y collate "bar")
 * while also guaranteeing that variadic aggregates don't change in behavior
 * depending on how many sort columns a particular call happens to have.
 *
 * Otherwise this is much like the plain-aggregate case.
 */
unsafe fn assign_ordered_set_collations(
    aggref: *mut Aggref,
    loccontext: &mut assign_collations_context,
) {
    let merge_sort_collations: bool;

    /* Merge sort collations to parent only if there can be only one */
    merge_sort_collations = list_length((*aggref).args) == 1
        && get_func_variadictype((*aggref).aggfnoid) == InvalidOid;

    /* Direct args, if any, are normal children of the Aggref node */
    assign_collations_walker((*aggref).aggdirectargs as *mut Node, loccontext);

    /* Process aggregated args appropriately */
    foreach!(lc, (*aggref).args, {
        let tle = lfirst_node!(TargetEntry, T_TargetEntry, current_cell!(lc));

        if merge_sort_collations {
            assign_collations_walker(tle as *mut Node, loccontext);
        } else {
            assign_expr_collations(loccontext.pstate, tle as *mut Node);
        }
    });
}

/*
 * Hypothetical-set aggregates are even more special: per spec, we need to
 * unify the collations of each pair of hypothetical and aggregated args.
 * And we need to force the choice of collation down into the sort column
 * to ensure that the sort happens with the chosen collation.  Other than
 * that, the behavior is like regular ordered-set aggregates.  Note that
 * hypothetical direct arguments contribute to the aggregate collation
 * only when their partner aggregated arguments do.
 */
unsafe fn assign_hypothetical_collations(
    aggref: *mut Aggref,
    loccontext: &mut assign_collations_context,
) {
    let mut h_cell: *mut ListCell = list_head((*aggref).aggdirectargs);
    let mut s_cell: *mut ListCell = list_head((*aggref).args);
    let merge_sort_collations: bool;
    let mut extra_args: c_int;

    /* Merge sort collations to parent only if there can be only one */
    merge_sort_collations = list_length((*aggref).args) == 1
        && get_func_variadictype((*aggref).aggfnoid) == InvalidOid;

    /* Process any non-hypothetical direct args */
    extra_args = list_length((*aggref).aggdirectargs) - list_length((*aggref).args);
    Assert!(extra_args >= 0);
    while extra_args > 0 {
        extra_args -= 1;
        assign_collations_walker(lfirst(h_cell) as *mut Node, loccontext);
        h_cell = lnext((*aggref).aggdirectargs, h_cell);
    }

    /* Scan hypothetical args and aggregated args in parallel */
    while !h_cell.is_null() && !s_cell.is_null() {
        let h_arg = lfirst(h_cell) as *mut Node;
        let s_tle = lfirst(s_cell) as *mut TargetEntry;
        let mut paircontext: assign_collations_context = std::mem::zeroed();

        /*
         * Assign collations internally in this pair of expressions, then
         * choose a common collation for them.  This should match
         * select_common_collation(), but we can't use that function as-is
         * because we need access to the whole collation state so we can
         * bubble it up to the aggregate function's level.
         */
        paircontext.pstate = loccontext.pstate;
        paircontext.collation = InvalidOid;
        paircontext.strength = COLLATE_NONE;
        paircontext.location = -1;
        /* Set these fields just to suppress uninitialized-value warnings: */
        paircontext.collation2 = InvalidOid;
        paircontext.location2 = -1;

        assign_collations_walker(h_arg, &mut paircontext);
        assign_collations_walker((*s_tle).expr as *mut Node, &mut paircontext);

        /* deal with collation conflict */
        if paircontext.strength == COLLATE_CONFLICT {
            ereport!(
                ERROR,
                "collation mismatch between implicit collations"
            );
            unreachable!();
        }

        /*
         * At this point paircontext.collation can be InvalidOid only if the
         * type is not collatable; no need to do anything in that case.  If we
         * do have to change the sort column's collation, do it by inserting a
         * RelabelType node into the sort column TLE.
         *
         * XXX This is pretty grotty for a couple of reasons:
         * assign_collations_walker isn't supposed to be changing the
         * expression structure like this, and a parse-time change of
         * collation ought to be signaled by a CollateExpr not a RelabelType
         * (the use of RelabelType for collation marking is supposed to be a
         * planner/executor thing only).  But we have no better alternative.
         * In particular, injecting a CollateExpr could result in the
         * expression being interpreted differently after dump/reload, since
         * we might be effectively promoting an implicit collation to
         * explicit.  This kluge is relying on ruleutils.c not printing a
         * COLLATE clause for a RelabelType, and probably on some other
         * fragile behaviors.
         */
        if OidIsValid(paircontext.collation)
            && paircontext.collation != exprCollation((*s_tle).expr as *mut Node)
        {
            (*s_tle).expr = makeRelabelType(
                (*s_tle).expr,
                exprType((*s_tle).expr as *mut Node),
                exprTypmod((*s_tle).expr as *mut Node),
                paircontext.collation,
                CoercionForm::COERCE_IMPLICIT_CAST,
            ) as *mut Expr;
        }

        /*
         * If appropriate, merge this column's collation state up to the
         * aggregate function.
         */
        if merge_sort_collations {
            merge_collation_state(
                paircontext.collation,
                paircontext.strength,
                paircontext.location,
                paircontext.collation2,
                paircontext.location2,
                loccontext,
            );
        }

        h_cell = lnext((*aggref).aggdirectargs, h_cell);
        s_cell = lnext((*aggref).args, s_cell);
    }
    Assert!(h_cell.is_null() && s_cell.is_null());
}

// ----------------------------------------------------------------------------
// Local stubs for unported helper functions and types
// ----------------------------------------------------------------------------

// Node struct stubs (from nodes/primnodes.h, nodes/parsenodes.h)
#[repr(C)]
struct CollateExpr {
    collOid: Oid,
    location: c_int,
}
#[repr(C)]
struct FieldSelect {
    resultcollid: Oid,
}
#[repr(C)]
struct RowExpr {
    args: *mut List,
}
#[repr(C)]
struct RowCompareExpr {
    largs: *mut List,
    rargs: *mut List,
    inputcollids: *mut List,
}
#[repr(C)]
struct CoerceToDomain {
    resulttype: Oid,
}
#[repr(C)]
struct TargetEntry {
    expr: *mut Expr,
    ressortgroupref: u32,
    resjunk: bool,
}
#[repr(C)]
struct Aggref {
    aggfnoid: Oid,
    aggkind: c_char,
    aggdirectargs: *mut List,
    args: *mut List,
    aggfilter: *mut Expr,
}
#[repr(C)]
struct WindowFunc {
    args: *mut List,
    aggfilter: *mut Expr,
}
#[repr(C)]
struct CaseExpr {
    args: *mut List,
    defresult: *mut Expr,
}
#[repr(C)]
struct CaseWhen {
    expr: *mut Expr,
    result: *mut Expr,
}
#[repr(C)]
struct SubscriptingRef {
    refupperindexpr: *mut List,
    reflowerindexpr: *mut List,
    refexpr: *mut Expr,
    refassgnexpr: *mut Expr,
}
#[repr(C)]
struct Expr {
    _dummy: u8,
}
#[repr(C)]
enum CoercionForm {
    COERCE_IMPLICIT_CAST,
}

// Constants
const AGGKIND_NORMAL: char = 'n';
const AGGKIND_ORDERED_SET: char = 'o';
const AGGKIND_HYPOTHETICAL: char = 'h';
const DEFAULT_COLLATION_OID: Oid = 100;
const QTW_IGNORE_RANGE_TABLE: c_int = 0x02;
const QTW_IGNORE_CTE_SUBQUERIES: c_int = 0x04;

#[allow(non_snake_case)]
const fn OidIsValid(objectId: Oid) -> bool {
    objectId != InvalidOid
}

// Function stubs
unsafe fn query_tree_walker(
    _query: *mut Query,
    _walker: *const (),
    _context: *mut c_void,
    _flags: c_int,
) -> bool {
    unimplemented!() // TODO: nodes/nodeFuncs.c
}
unsafe fn expression_tree_walker(
    _node: *mut Node,
    _walker: *const (),
    _context: *mut c_void,
) -> bool {
    unimplemented!() // TODO: nodes/nodeFuncs.c
}
unsafe fn exprLocation(_expr: *mut Node) -> c_int {
    unimplemented!() // TODO: nodes/nodeFuncs.c
}
unsafe fn exprCollation(_expr: *mut Node) -> Oid {
    unimplemented!() // TODO: nodes/nodeFuncs.c
}
unsafe fn exprType(_expr: *mut Node) -> Oid {
    unimplemented!() // TODO: nodes/nodeFuncs.c
}
unsafe fn exprTypmod(_expr: *mut Node) -> i32 {
    unimplemented!() // TODO: nodes/nodeFuncs.c
}
unsafe fn exprSetCollation(_expr: *mut Node, _collation: Oid) {
    unimplemented!() // TODO: nodes/nodeFuncs.c
}
unsafe fn exprSetInputCollation(_expr: *mut Node, _inputcollation: Oid) {
    unimplemented!() // TODO: nodes/nodeFuncs.c
}
unsafe fn get_typcollation(_typid: Oid) -> Oid {
    unimplemented!() // TODO: utils/cache/lsyscache.c
}
unsafe fn get_func_variadictype(_funcid: Oid) -> Oid {
    unimplemented!() // TODO: utils/cache/lsyscache.c
}
unsafe fn makeRelabelType(
    _arg: *mut Expr,
    _rtype: Oid,
    _rtypmod: i32,
    _rcollid: Oid,
    _rformat: CoercionForm,
) -> *mut Node {
    unimplemented!() // TODO: nodes/makefuncs.c
}
unsafe fn lappend_oid(_list: *mut List, _datum: Oid) -> *mut List {
    unimplemented!() // TODO: nodes/list.c
}
unsafe fn list_length(_l: *const List) -> c_int {
    unimplemented!() // TODO: nodes/pg_list.h
}
unsafe fn list_head(_l: *const List) -> *mut ListCell {
    unimplemented!() // TODO: nodes/pg_list.h
}
unsafe fn lnext(_l: *const List, _c: *const ListCell) -> *mut ListCell {
    unimplemented!() // TODO: nodes/pg_list.h
}
