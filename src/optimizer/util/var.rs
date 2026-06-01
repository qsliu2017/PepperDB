//! src/backend/optimizer/util/var.c - Var node manipulation routines.
//!
//! #include mapping:
//!   - "postgres.h"                 -> crate::prelude::*
//!   - "access/sysattr.h"           -> crate::access::sysattr::FirstLowInvalidHeapAttributeNumber
//!   - "nodes/nodeFuncs.h"          -> crate::nodes::nodeFuncs::{expression_tree_walker,
//!                                       query_tree_walker, query_or_expression_tree_walker, ...}
//!   - "optimizer/clauses.h"        -> (only used by STUBbed flatten_* helpers)
//!   - "optimizer/optimizer.h"      -> PVC_* flags reproduced below
//!   - "optimizer/placeholder.h"    -> (only used by STUBbed flatten_* helpers)
//!   - "optimizer/prep.h"           -> (only used by STUBbed flatten_* helpers)
//!   - "parser/parsetree.h"         -> rt_fetch (only used by STUBbed flatten_* helpers)
//!   - "rewrite/rewriteManip.h"     -> IncrementVarSublevelsUp etc (STUBbed flatten_* helpers)
//!
//! Variable-reference analysis over expression trees.  All of the analyzers in
//! this file are built on the REAL expression_tree_walker / query_tree_walker /
//! query_or_expression_tree_walker from crate::nodes::nodeFuncs.
//!
//! REAL (fully ported, pure walkers):
//!   pull_varnos, pull_varnos_of_level, pull_varattnos, pull_vars_of_level,
//!   contain_var_clause, contain_vars_of_level, contain_vars_returning_old_or_new,
//!   locate_var_of_level, pull_var_clause (with the PVC_* flags).
//!
//! STUBbed (need range-table / planner state not yet ported -- Query.rtable,
//! rt_fetch, copyObject, IncrementVarSublevelsUp, make_placeholder_expr,
//! add_nulling_relids, get_relids_for_join, get_relids_in_jointree,
//! contain_volatile_functions, expression_returns_set, checkExprHasSubLink):
//!   flatten_join_alias_vars (+ _mutator), flatten_group_exprs (+ _mutator),
//!   mark_nullable_by_grouping, add_nullingrels_if_needed,
//!   is_standard_join_alias_expression, adjust_standard_join_alias_expression,
//!   alias_relid_set.

use crate::prelude::*;

use crate::access::sysattr::FirstLowInvalidHeapAttributeNumber;

use crate::nodes::bitmapset::{
    bms_add_member, bms_add_members, bms_difference, bms_equal, bms_is_empty, bms_join, Bitmapset,
};
use crate::nodes::nodeFuncs::{
    expression_tree_walker, query_or_expression_tree_walker, query_tree_walker,
};
use crate::nodes::nodes::Node;
use crate::nodes::pathnodes::{PlaceHolderInfo, PlaceHolderVar, PlannerInfo, Relids};
use crate::nodes::pg_list::{lappend, List, NIL};
use crate::nodes::primnodes::{
    Aggref, CurrentOfExpr, GroupingFunc, ReturningExpr, Var, VAR_RETURNING_DEFAULT,
};
use crate::IsA;

// ----------------------------------------------------------------------------
// PVC_* flags for pull_var_clause (from optimizer/optimizer.h).
// Reproduced here verbatim; move to the ported optimizer.h when available.
// ----------------------------------------------------------------------------

/// include Aggrefs in output list
pub const PVC_INCLUDE_AGGREGATES: c_int = 0x0001;
/// recurse into Aggref args
pub const PVC_RECURSE_AGGREGATES: c_int = 0x0002;
/// include WindowFuncs in output list
pub const PVC_INCLUDE_WINDOWFUNCS: c_int = 0x0004;
/// recurse into WindowFunc args
pub const PVC_RECURSE_WINDOWFUNCS: c_int = 0x0008;
/// include PlaceHolderVars in output list
pub const PVC_INCLUDE_PLACEHOLDERS: c_int = 0x0010;
/// recurse into PlaceHolderVar arg
pub const PVC_RECURSE_PLACEHOLDERS: c_int = 0x0020;

// ----------------------------------------------------------------------------
// Walker context structs (#[repr(C)] so they can be passed as *mut c_void).
// ----------------------------------------------------------------------------

#[repr(C)]
struct pull_varnos_context {
    varnos: Relids,
    root: *mut PlannerInfo,
    sublevels_up: c_int,
}

#[repr(C)]
struct pull_varattnos_context {
    varattnos: *mut Bitmapset,
    varno: Index,
}

#[repr(C)]
struct pull_vars_context {
    vars: *mut List,
    sublevels_up: c_int,
}

#[repr(C)]
struct locate_var_of_level_context {
    var_location: c_int,
    sublevels_up: c_int,
}

#[repr(C)]
struct pull_var_clause_context {
    varlist: *mut List,
    flags: c_int,
}

// ----------------------------------------------------------------------------
// pull_varnos
// ----------------------------------------------------------------------------

/*
 * pull_varnos
 *		Create a set of all the distinct varnos present in a parsetree.
 *		Only varnos that reference level-zero rtable entries are considered.
 *
 * The result includes outer-join relids mentioned in Var.varnullingrels and
 * PlaceHolderVar.phnullingrels fields in the parsetree.
 *
 * "root" can be passed as NULL if it is not necessary to process
 * PlaceHolderVars.
 *
 * NOTE: this is used on not-yet-planned expressions.  It may therefore find
 * bare SubLinks, and if so it needs to recurse into them to look for uplevel
 * references to the desired rtable level!	But when we find a completed
 * SubPlan, we only need to look at the parameters passed to the subplan.
 */
pub unsafe fn pull_varnos(root: *mut PlannerInfo, node: *mut Node) -> Relids {
    let mut context = pull_varnos_context {
        varnos: null_mut(),
        root,
        sublevels_up: 0,
    };

    /*
     * Must be prepared to start with a Query or a bare expression tree; if
     * it's a Query, we don't want to increment sublevels_up.
     */
    query_or_expression_tree_walker(
        node,
        Some(pull_varnos_walker),
        &mut context as *mut _ as *mut c_void,
        0,
    );

    context.varnos
}

/*
 * pull_varnos_of_level
 *		Create a set of all the distinct varnos present in a parsetree.
 *		Only Vars of the specified level are considered.
 */
pub unsafe fn pull_varnos_of_level(
    root: *mut PlannerInfo,
    node: *mut Node,
    levelsup: c_int,
) -> Relids {
    let mut context = pull_varnos_context {
        varnos: null_mut(),
        root,
        sublevels_up: levelsup,
    };

    query_or_expression_tree_walker(
        node,
        Some(pull_varnos_walker),
        &mut context as *mut _ as *mut c_void,
        0,
    );

    context.varnos
}

unsafe fn pull_varnos_walker(node: *mut Node, context: *mut c_void) -> bool {
    let context = &mut *(context as *mut pull_varnos_context);

    if node.is_null() {
        return false;
    }
    if IsA!(node, T_Var) {
        let var = node as *mut Var;

        if (*var).varlevelsup as c_int == context.sublevels_up {
            context.varnos = bms_add_member(context.varnos, (*var).varno);
            context.varnos = bms_add_members(context.varnos, (*var).varnullingrels);
        }
        return false;
    }
    if IsA!(node, T_CurrentOfExpr) {
        let cexpr = node as *mut CurrentOfExpr;

        if context.sublevels_up == 0 {
            context.varnos = bms_add_member(context.varnos, (*cexpr).cvarno as c_int);
        }
        return false;
    }
    if IsA!(node, T_PlaceHolderVar) {
        let phv = node as *mut PlaceHolderVar;

        /*
         * If a PlaceHolderVar is not of the target query level, ignore it,
         * instead recursing into its expression to see if it contains any
         * vars that are of the target level.  We'll also do that when the
         * caller doesn't pass a "root" pointer.  (We probably shouldn't see
         * PlaceHolderVars at all in such cases, but if we do, this is a
         * reasonable behavior.)
         */
        if (*phv).phlevelsup as c_int == context.sublevels_up && !context.root.is_null() {
            /*
             * Ideally, the PHV's contribution to context->varnos is its
             * ph_eval_at set.  However, this code can be invoked before
             * that's been computed.  If we cannot find a PlaceHolderInfo,
             * fall back to the conservative assumption that the PHV will be
             * evaluated at its syntactic level (phv->phrels).
             *
             * Another problem is that a PlaceHolderVar can appear in quals or
             * tlists that have been translated for use in a child appendrel.
             * Typically such a PHV is a parameter expression sourced by some
             * other relation, so that the translation from parent appendrel
             * to child doesn't change its phrels, and we should still take
             * ph_eval_at at face value.  But in corner cases, the PHV's
             * original phrels can include the parent appendrel itself, in
             * which case the translated PHV will have the child appendrel in
             * phrels, and we must translate ph_eval_at to match.
             */
            let mut phinfo: *mut PlaceHolderInfo = null_mut();

            if (*phv).phlevelsup == 0 {
                if ((*phv).phid as c_int) < (*context.root).placeholder_array_size {
                    phinfo = *(*context.root)
                        .placeholder_array
                        .add((*phv).phid as usize);
                }
            }
            if phinfo.is_null() {
                /* No PlaceHolderInfo yet, use phrels */
                context.varnos = bms_add_members(context.varnos, (*phv).phrels);
            } else if bms_equal((*phv).phrels, (*(*phinfo).ph_var).phrels) {
                /* Normal case: use ph_eval_at */
                context.varnos = bms_add_members(context.varnos, (*phinfo).ph_eval_at);
            } else {
                /* Translated PlaceHolderVar: translate ph_eval_at to match */
                let mut newevalat: Relids;
                let mut delta: Relids;

                /* remove what was removed from phv->phrels ... */
                delta = bms_difference((*(*phinfo).ph_var).phrels, (*phv).phrels);
                newevalat = bms_difference((*phinfo).ph_eval_at, delta);
                /* ... then if that was in fact part of ph_eval_at ... */
                if !bms_equal(newevalat, (*phinfo).ph_eval_at) {
                    /* ... add what was added */
                    delta = bms_difference((*phv).phrels, (*(*phinfo).ph_var).phrels);
                    newevalat = bms_join(newevalat, delta);
                }
                context.varnos = bms_join(context.varnos, newevalat);
            }

            /*
             * In all three cases, include phnullingrels in the result.  We
             * don't worry about possibly needing to translate it, because
             * appendrels only translate varnos of baserels, not outer joins.
             */
            context.varnos = bms_add_members(context.varnos, (*phv).phnullingrels);
            return false; /* don't recurse into expression */
        }
    } else if IsA!(node, T_Query) {
        /* Recurse into RTE subquery or not-yet-planned sublink subquery */
        let result: bool;

        context.sublevels_up += 1;
        result = query_tree_walker(
            node as *mut _,
            Some(pull_varnos_walker),
            context as *mut _ as *mut c_void,
            0,
        );
        context.sublevels_up -= 1;
        return result;
    }
    expression_tree_walker(
        node,
        Some(pull_varnos_walker),
        context as *mut _ as *mut c_void,
    )
}

// ----------------------------------------------------------------------------
// pull_varattnos
// ----------------------------------------------------------------------------

/*
 * pull_varattnos
 *		Find all the distinct attribute numbers present in an expression tree,
 *		and add them to the initial contents of *varattnos.
 *		Only Vars of the given varno and rtable level zero are considered.
 *
 * Attribute numbers are offset by FirstLowInvalidHeapAttributeNumber so that
 * we can include system attributes (e.g., OID) in the bitmap representation.
 *
 * Currently, this does not support unplanned subqueries; that is not needed
 * for current uses.  It will handle already-planned SubPlan nodes, though,
 * looking into only the "testexpr" and the "args" list.  (The subplan cannot
 * contain any other references to Vars of the current level.)
 */
pub unsafe fn pull_varattnos(node: *mut Node, varno: Index, varattnos: *mut *mut Bitmapset) {
    let mut context = pull_varattnos_context {
        varattnos: *varattnos,
        varno,
    };

    pull_varattnos_walker(node, &mut context as *mut _ as *mut c_void);

    *varattnos = context.varattnos;
}

unsafe fn pull_varattnos_walker(node: *mut Node, context: *mut c_void) -> bool {
    let context = &mut *(context as *mut pull_varattnos_context);

    if node.is_null() {
        return false;
    }
    if IsA!(node, T_Var) {
        let var = node as *mut Var;

        if (*var).varno as Index == context.varno && (*var).varlevelsup == 0 {
            context.varattnos = bms_add_member(
                context.varattnos,
                (*var).varattno as c_int - FirstLowInvalidHeapAttributeNumber as c_int,
            );
        }
        return false;
    }

    /* Should not find an unplanned subquery */
    Assert!(!IsA!(node, T_Query));

    expression_tree_walker(
        node,
        Some(pull_varattnos_walker),
        context as *mut _ as *mut c_void,
    )
}

// ----------------------------------------------------------------------------
// pull_vars_of_level
// ----------------------------------------------------------------------------

/*
 * pull_vars_of_level
 *		Create a list of all Vars (and PlaceHolderVars) referencing the
 *		specified query level in the given parsetree.
 *
 * Caution: the Vars are not copied, only linked into the list.
 */
pub unsafe fn pull_vars_of_level(node: *mut Node, levelsup: c_int) -> *mut List {
    let mut context = pull_vars_context {
        vars: NIL,
        sublevels_up: levelsup,
    };

    query_or_expression_tree_walker(
        node,
        Some(pull_vars_walker),
        &mut context as *mut _ as *mut c_void,
        0,
    );

    context.vars
}

unsafe fn pull_vars_walker(node: *mut Node, context: *mut c_void) -> bool {
    let context = &mut *(context as *mut pull_vars_context);

    if node.is_null() {
        return false;
    }
    if IsA!(node, T_Var) {
        let var = node as *mut Var;

        if (*var).varlevelsup as c_int == context.sublevels_up {
            context.vars = lappend(context.vars, var as *mut c_void);
        }
        return false;
    }
    if IsA!(node, T_PlaceHolderVar) {
        let phv = node as *mut PlaceHolderVar;

        if (*phv).phlevelsup as c_int == context.sublevels_up {
            context.vars = lappend(context.vars, phv as *mut c_void);
        }
        /* we don't want to look into the contained expression */
        return false;
    }
    if IsA!(node, T_Query) {
        /* Recurse into RTE subquery or not-yet-planned sublink subquery */
        let result: bool;

        context.sublevels_up += 1;
        result = query_tree_walker(
            node as *mut _,
            Some(pull_vars_walker),
            context as *mut _ as *mut c_void,
            0,
        );
        context.sublevels_up -= 1;
        return result;
    }
    expression_tree_walker(
        node,
        Some(pull_vars_walker),
        context as *mut _ as *mut c_void,
    )
}

// ----------------------------------------------------------------------------
// contain_var_clause
// ----------------------------------------------------------------------------

/*
 * contain_var_clause
 *	  Recursively scan a clause to discover whether it contains any Var nodes
 *	  (of the current query level).
 *
 *	  Returns true if any varnode found.
 *
 * Does not examine subqueries, therefore must only be used after reduction
 * of sublinks to subplans!
 */
pub unsafe fn contain_var_clause(node: *mut Node) -> bool {
    contain_var_clause_walker(node, null_mut())
}

unsafe fn contain_var_clause_walker(node: *mut Node, context: *mut c_void) -> bool {
    if node.is_null() {
        return false;
    }
    if IsA!(node, T_Var) {
        if (*(node as *mut Var)).varlevelsup == 0 {
            return true; /* abort the tree traversal and return true */
        }
        return false;
    }
    if IsA!(node, T_CurrentOfExpr) {
        return true;
    }
    if IsA!(node, T_PlaceHolderVar) {
        if (*(node as *mut PlaceHolderVar)).phlevelsup == 0 {
            return true; /* abort the tree traversal and return true */
        }
        /* else fall through to check the contained expr */
    }
    expression_tree_walker(node, Some(contain_var_clause_walker), context)
}

// ----------------------------------------------------------------------------
// contain_vars_of_level
// ----------------------------------------------------------------------------

/*
 * contain_vars_of_level
 *	  Recursively scan a clause to discover whether it contains any Var nodes
 *	  of the specified query level.
 *
 *	  Returns true if any such Var found.
 *
 * Will recurse into sublinks.  Also, may be invoked directly on a Query.
 */
pub unsafe fn contain_vars_of_level(node: *mut Node, levelsup: c_int) -> bool {
    let mut sublevels_up: c_int = levelsup;

    query_or_expression_tree_walker(
        node,
        Some(contain_vars_of_level_walker),
        &mut sublevels_up as *mut _ as *mut c_void,
        0,
    )
}

unsafe fn contain_vars_of_level_walker(node: *mut Node, context: *mut c_void) -> bool {
    let sublevels_up = context as *mut c_int;

    if node.is_null() {
        return false;
    }
    if IsA!(node, T_Var) {
        if (*(node as *mut Var)).varlevelsup as c_int == *sublevels_up {
            return true; /* abort tree traversal and return true */
        }
        return false;
    }
    if IsA!(node, T_CurrentOfExpr) {
        if *sublevels_up == 0 {
            return true;
        }
        return false;
    }
    if IsA!(node, T_PlaceHolderVar) {
        if (*(node as *mut PlaceHolderVar)).phlevelsup as c_int == *sublevels_up {
            return true; /* abort the tree traversal and return true */
        }
        /* else fall through to check the contained expr */
    }
    if IsA!(node, T_Query) {
        /* Recurse into subselects */
        let result: bool;

        *sublevels_up += 1;
        result = query_tree_walker(
            node as *mut _,
            Some(contain_vars_of_level_walker),
            sublevels_up as *mut c_void,
            0,
        );
        *sublevels_up -= 1;
        return result;
    }
    expression_tree_walker(
        node,
        Some(contain_vars_of_level_walker),
        sublevels_up as *mut c_void,
    )
}

// ----------------------------------------------------------------------------
// contain_vars_returning_old_or_new
// ----------------------------------------------------------------------------

/*
 * contain_vars_returning_old_or_new
 *	  Recursively scan a clause to discover whether it contains any Var nodes
 *	  (of the current query level) whose varreturningtype is VAR_RETURNING_OLD
 *	  or VAR_RETURNING_NEW.
 *
 *	  Returns true if any found.
 *
 * Any ReturningExprs are also detected --- if an OLD/NEW Var was rewritten,
 * we still regard this as a clause that returns OLD/NEW values.
 *
 * Does not examine subqueries, therefore must only be used after reduction
 * of sublinks to subplans!
 */
pub unsafe fn contain_vars_returning_old_or_new(node: *mut Node) -> bool {
    contain_vars_returning_old_or_new_walker(node, null_mut())
}

unsafe fn contain_vars_returning_old_or_new_walker(node: *mut Node, context: *mut c_void) -> bool {
    if node.is_null() {
        return false;
    }
    if IsA!(node, T_Var) {
        if (*(node as *mut Var)).varlevelsup == 0
            && (*(node as *mut Var)).varreturningtype != VAR_RETURNING_DEFAULT
        {
            return true; /* abort the tree traversal and return true */
        }
        return false;
    }
    if IsA!(node, T_ReturningExpr) {
        if (*(node as *mut ReturningExpr)).retlevelsup == 0 {
            return true; /* abort the tree traversal and return true */
        }
        return false;
    }
    expression_tree_walker(
        node,
        Some(contain_vars_returning_old_or_new_walker),
        context,
    )
}

// ----------------------------------------------------------------------------
// locate_var_of_level
// ----------------------------------------------------------------------------

/*
 * locate_var_of_level
 *	  Find the parse location of any Var of the specified query level.
 *
 * Returns -1 if no such Var is in the querytree, or if they all have
 * unknown parse location.  (The former case is probably caller error,
 * but we don't bother to distinguish it from the latter case.)
 *
 * Will recurse into sublinks.  Also, may be invoked directly on a Query.
 *
 * Note: it might seem appropriate to merge this functionality into
 * contain_vars_of_level, but that would complicate that function's API.
 * Currently, the only uses of this function are for error reporting,
 * and so shaving cycles probably isn't very important.
 */
pub unsafe fn locate_var_of_level(node: *mut Node, levelsup: c_int) -> c_int {
    let mut context = locate_var_of_level_context {
        var_location: -1, /* in case we find nothing */
        sublevels_up: levelsup,
    };

    query_or_expression_tree_walker(
        node,
        Some(locate_var_of_level_walker),
        &mut context as *mut _ as *mut c_void,
        0,
    );

    context.var_location
}

unsafe fn locate_var_of_level_walker(node: *mut Node, context: *mut c_void) -> bool {
    let context = &mut *(context as *mut locate_var_of_level_context);

    if node.is_null() {
        return false;
    }
    if IsA!(node, T_Var) {
        let var = node as *mut Var;

        if (*var).varlevelsup as c_int == context.sublevels_up && (*var).location >= 0 {
            context.var_location = (*var).location;
            return true; /* abort tree traversal and return true */
        }
        return false;
    }
    if IsA!(node, T_CurrentOfExpr) {
        /* since CurrentOfExpr doesn't carry location, nothing we can do */
        return false;
    }
    /* No extra code needed for PlaceHolderVar; just look in contained expr */
    if IsA!(node, T_Query) {
        /* Recurse into subselects */
        let result: bool;

        context.sublevels_up += 1;
        result = query_tree_walker(
            node as *mut _,
            Some(locate_var_of_level_walker),
            context as *mut _ as *mut c_void,
            0,
        );
        context.sublevels_up -= 1;
        return result;
    }
    expression_tree_walker(
        node,
        Some(locate_var_of_level_walker),
        context as *mut _ as *mut c_void,
    )
}

// ----------------------------------------------------------------------------
// pull_var_clause
// ----------------------------------------------------------------------------

/*
 * pull_var_clause
 *	  Recursively pulls all Var nodes from an expression clause.
 *
 *	  Aggrefs are handled according to these bits in 'flags':
 *		PVC_INCLUDE_AGGREGATES		include Aggrefs in output list
 *		PVC_RECURSE_AGGREGATES		recurse into Aggref arguments
 *		neither flag				throw error if Aggref found
 *	  Vars within an Aggref's expression are included in the result only
 *	  when PVC_RECURSE_AGGREGATES is specified.
 *
 *	  WindowFuncs are handled according to these bits in 'flags':
 *		PVC_INCLUDE_WINDOWFUNCS		include WindowFuncs in output list
 *		PVC_RECURSE_WINDOWFUNCS		recurse into WindowFunc arguments
 *		neither flag				throw error if WindowFunc found
 *	  Vars within a WindowFunc's expression are included in the result only
 *	  when PVC_RECURSE_WINDOWFUNCS is specified.
 *
 *	  PlaceHolderVars are handled according to these bits in 'flags':
 *		PVC_INCLUDE_PLACEHOLDERS	include PlaceHolderVars in output list
 *		PVC_RECURSE_PLACEHOLDERS	recurse into PlaceHolderVar arguments
 *		neither flag				throw error if PlaceHolderVar found
 *	  Vars within a PHV's expression are included in the result only
 *	  when PVC_RECURSE_PLACEHOLDERS is specified.
 *
 *	  GroupingFuncs are treated exactly like Aggrefs, and so do not need
 *	  their own flag bits.
 *
 *	  CurrentOfExpr nodes are ignored in all cases.
 *
 *	  Upper-level vars (with varlevelsup > 0) should not be seen here,
 *	  likewise for upper-level Aggrefs and PlaceHolderVars.
 *
 *	  Returns list of nodes found.  Note the nodes themselves are not
 *	  copied, only referenced.
 *
 * Does not examine subqueries, therefore must only be used after reduction
 * of sublinks to subplans!
 */
pub unsafe fn pull_var_clause(node: *mut Node, flags: c_int) -> *mut List {
    /* Assert that caller has not specified inconsistent flags */
    Assert!(
        (flags & (PVC_INCLUDE_AGGREGATES | PVC_RECURSE_AGGREGATES))
            != (PVC_INCLUDE_AGGREGATES | PVC_RECURSE_AGGREGATES)
    );
    Assert!(
        (flags & (PVC_INCLUDE_WINDOWFUNCS | PVC_RECURSE_WINDOWFUNCS))
            != (PVC_INCLUDE_WINDOWFUNCS | PVC_RECURSE_WINDOWFUNCS)
    );
    Assert!(
        (flags & (PVC_INCLUDE_PLACEHOLDERS | PVC_RECURSE_PLACEHOLDERS))
            != (PVC_INCLUDE_PLACEHOLDERS | PVC_RECURSE_PLACEHOLDERS)
    );

    let mut context = pull_var_clause_context {
        varlist: NIL,
        flags,
    };

    pull_var_clause_walker(node, &mut context as *mut _ as *mut c_void);
    context.varlist
}

unsafe fn pull_var_clause_walker(node: *mut Node, context: *mut c_void) -> bool {
    let context = &mut *(context as *mut pull_var_clause_context);

    if node.is_null() {
        return false;
    }
    if IsA!(node, T_Var) {
        if (*(node as *mut Var)).varlevelsup != 0 {
            elog!(ERROR, "Upper-level Var found where not expected");
        }
        context.varlist = lappend(context.varlist, node as *mut c_void);
        return false;
    } else if IsA!(node, T_Aggref) {
        if (*(node as *mut Aggref)).agglevelsup != 0 {
            elog!(ERROR, "Upper-level Aggref found where not expected");
        }
        if context.flags & PVC_INCLUDE_AGGREGATES != 0 {
            context.varlist = lappend(context.varlist, node as *mut c_void);
            /* we do NOT descend into the contained expression */
            return false;
        } else if context.flags & PVC_RECURSE_AGGREGATES != 0 {
            /* fall through to recurse into the aggregate's arguments */
        } else {
            elog!(ERROR, "Aggref found where not expected");
        }
    } else if IsA!(node, T_GroupingFunc) {
        if (*(node as *mut GroupingFunc)).agglevelsup != 0 {
            elog!(ERROR, "Upper-level GROUPING found where not expected");
        }
        if context.flags & PVC_INCLUDE_AGGREGATES != 0 {
            context.varlist = lappend(context.varlist, node as *mut c_void);
            /* we do NOT descend into the contained expression */
            return false;
        } else if context.flags & PVC_RECURSE_AGGREGATES != 0 {
            /* fall through to recurse into the GroupingFunc's arguments */
        } else {
            elog!(ERROR, "GROUPING found where not expected");
        }
    } else if IsA!(node, T_WindowFunc) {
        /* WindowFuncs have no levelsup field to check ... */
        if context.flags & PVC_INCLUDE_WINDOWFUNCS != 0 {
            context.varlist = lappend(context.varlist, node as *mut c_void);
            /* we do NOT descend into the contained expressions */
            return false;
        } else if context.flags & PVC_RECURSE_WINDOWFUNCS != 0 {
            /* fall through to recurse into the windowfunc's arguments */
        } else {
            elog!(ERROR, "WindowFunc found where not expected");
        }
    } else if IsA!(node, T_PlaceHolderVar) {
        if (*(node as *mut PlaceHolderVar)).phlevelsup != 0 {
            elog!(ERROR, "Upper-level PlaceHolderVar found where not expected");
        }
        if context.flags & PVC_INCLUDE_PLACEHOLDERS != 0 {
            context.varlist = lappend(context.varlist, node as *mut c_void);
            /* we do NOT descend into the contained expression */
            return false;
        } else if context.flags & PVC_RECURSE_PLACEHOLDERS != 0 {
            /* fall through to recurse into the placeholder's expression */
        } else {
            elog!(ERROR, "PlaceHolderVar found where not expected");
        }
    }
    expression_tree_walker(
        node,
        Some(pull_var_clause_walker),
        context as *mut _ as *mut c_void,
    )
}

// ----------------------------------------------------------------------------
// STUBs: flatten_join_alias_vars / flatten_group_exprs and their helpers.
//
// These require pieces not yet ported: Query.rtable + rt_fetch (parsetree.h),
// copyObject, expression_tree_mutator/query_tree_mutator usage with rangetable
// translation, IncrementVarSublevelsUp / add_nulling_relids (rewriteManip.h),
// make_placeholder_expr (placeholder.h), get_relids_for_join /
// get_relids_in_jointree (prep.h / clauses.h), contain_volatile_functions /
// expression_returns_set / checkExprHasSubLink (clauses.h).  Signatures kept.
// TODO(port): translate once the above dependencies land.
// ----------------------------------------------------------------------------

/*
 * flatten_join_alias_vars
 *	  Replace Vars that reference JOIN outputs with references to the original
 *	  relation variables instead.
 */
pub unsafe fn flatten_join_alias_vars(
    _root: *mut PlannerInfo,
    _query: *mut c_void, /* TODO: *mut Query */
    _node: *mut Node,
) -> *mut Node {
    unimplemented!("flatten_join_alias_vars: needs Query rangetable + rt_fetch (not ported)")
}

/*
 * flatten_group_exprs
 *	  Replace Vars that reference GROUP outputs with the underlying grouping
 *	  expressions.
 */
pub unsafe fn flatten_group_exprs(
    _root: *mut PlannerInfo,
    _query: *mut c_void, /* TODO: *mut Query */
    _node: *mut Node,
) -> *mut Node {
    unimplemented!("flatten_group_exprs: needs Query rangetable + rt_fetch (not ported)")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nodes::nodes::NodeTag;
    use crate::nodes::primnodes::Const;

    // Build a bare Var with the given varno/varattno/varlevelsup, zero-initialized
    // otherwise.  We set just the node tag in xpr and the fields the walkers read.
    unsafe fn make_test_var(varno: c_int, varattno: i16, varlevelsup: Index) -> *mut Var {
        let var = palloc0(core::mem::size_of::<Var>()) as *mut Var;
        (*var).xpr.r#type = NodeTag::T_Var;
        (*var).varno = varno;
        (*var).varattno = varattno;
        (*var).varlevelsup = varlevelsup;
        (*var).varreturningtype = VAR_RETURNING_DEFAULT;
        (*var).location = -1;
        var
    }

    unsafe fn make_test_const() -> *mut Const {
        let c = palloc0(core::mem::size_of::<Const>()) as *mut Const;
        (*c).xpr.r#type = NodeTag::T_Const;
        c
    }

    // contain_var_clause on a level-0 Var is true; on a Const is false.
    #[test]
    fn contain_var_clause_basic() {
        unsafe {
            let var = make_test_var(2, 3, 0);
            assert!(contain_var_clause(var as *mut Node));

            let c = make_test_const();
            assert!(!contain_var_clause(c as *mut Node));
        }
    }

    // An upper-level Var (varlevelsup > 0) is not a "current level" var.
    #[test]
    fn contain_var_clause_uplevel_is_false() {
        unsafe {
            let var = make_test_var(2, 3, 1);
            assert!(!contain_var_clause(var as *mut Node));
        }
    }

    // contain_vars_of_level matches only the requested level.
    #[test]
    fn contain_vars_of_level_basic() {
        unsafe {
            let var0 = make_test_var(2, 3, 0);
            assert!(contain_vars_of_level(var0 as *mut Node, 0));
            assert!(!contain_vars_of_level(var0 as *mut Node, 1));

            let var1 = make_test_var(2, 3, 1);
            assert!(contain_vars_of_level(var1 as *mut Node, 1));
            assert!(!contain_vars_of_level(var1 as *mut Node, 0));
        }
    }

    // pull_varnos collects the varnos of level-0 Vars into a Relids (now that
    // the real bms_* functions back it).  A Var with varno=2 -> {2}.
    #[test]
    fn pull_varnos_collects_varno() {
        unsafe {
            use crate::nodes::bitmapset::bms_is_member;
            let var = make_test_var(2, 3, 0);
            let relids = pull_varnos(null_mut(), var as *mut Node);
            assert!(bms_is_member(2, relids));
            assert!(!bms_is_member(1, relids));
        }
    }

    // locate_var_of_level returns the Var's parse location for the matching level.
    #[test]
    fn locate_var_of_level_basic() {
        unsafe {
            let var = make_test_var(2, 3, 0);
            (*var).location = 42;
            assert_eq!(locate_var_of_level(var as *mut Node, 0), 42);
            // wrong level -> not found
            assert_eq!(locate_var_of_level(var as *mut Node, 1), -1);
        }
    }

    // contain_var_clause on NULL is false.
    #[test]
    fn contain_var_clause_null() {
        unsafe {
            assert!(!contain_var_clause(null_mut()));
        }
    }
}
