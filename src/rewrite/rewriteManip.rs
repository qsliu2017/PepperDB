//! src/backend/rewrite/rewriteManip.c - expression/query-tree manipulation.
//!
//! #include mapping:
//!   - "postgres.h"                 -> crate::prelude::*
//!   - "catalog/pg_type.h"          -> (only used by STUBbed map/replace helpers)
//!   - "nodes/makefuncs.h"          -> (only used by STUBbed AddInvertedQual etc - not ported here)
//!   - "nodes/nodeFuncs.h"          -> crate::nodes::nodeFuncs::{expression_tree_walker,
//!                                       query_tree_walker, range_table_walker,
//!                                       query_or_expression_tree_walker,
//!                                       expression_tree_mutator, query_tree_mutator,
//!                                       query_or_expression_tree_mutator,
//!                                       QTW_EXAMINE_RTES_BEFORE, tree_walker_callback,
//!                                       tree_mutator_callback}
//!   - "nodes/pathnodes.h"          -> crate::nodes::pathnodes::{PlaceHolderVar, Relids}
//!   - "nodes/plannodes.h"          -> crate::nodes::plannodes::PlanRowMark
//!   - "parser/parse_coerce.h"      -> (only used by STUBbed map/replace helpers)
//!   - "parser/parse_relation.h"    -> (only used by STUBbed map/replace helpers)
//!   - "parser/parsetree.h"         -> (only used by STUBbed map/replace helpers)
//!   - "rewrite/rewriteManip.h"     -> ChangeVarNodes_context / ChangeVarNodes_callback (below)
//!   - "utils/lsyscache.h"          -> (only used by STUBbed map/replace helpers)
//!
//! REAL (fully ported, pure walkers/mutators over Var/nodeFuncs):
//!   contain_aggs_of_level (+ _walker), locate_agg_of_level (+ _walker),
//!   contain_windowfuncs (+ _walker), locate_windowfunc (+ _walker),
//!   OffsetVarNodes (+ _walker, offset_relid_set),
//!   ChangeVarNodes / ChangeVarNodesExtended (+ _walker), ChangeVarNodesWalkExpression,
//!   adjust_relid_set,
//!   IncrementVarSublevelsUp (+ _walker, _rtable),
//!   rangeTableEntry_used (+ _walker),
//!   add_nulling_relids (+ _mutator), remove_nulling_relids (+ _mutator).
//!
//! STUBbed (need parser/parse_relation + parse_coerce + lsyscache + ROWTYPE/RowExpr
//! expansion, or rangetable helpers not yet ported):
//!   map_variable_attnos (+ _mutator), replace_rte_variables (+ _mutator),
//!   ReplaceVarsFromTargetList.
//!
//! NOTE on copyObject: PostgreSQL's generic copyObject (copyfuncs.c) is not yet
//! ported.  The two REAL mutators here (add/remove_nulling_relids) only need to
//! produce a fresh Var / PlaceHolderVar whose sole mutated pointer field is the
//! nullingrels Bitmapset (which is overwritten with a freshly-built set).  We
//! therefore use a local flat node copy (copy_flat_node), exactly the behavior
//! of the `memcpy(phv, node, sizeof(PlaceHolderVar))` path PostgreSQL already
//! uses for the PHV case.  This keeps these high-value mutators runnable.

use crate::prelude::*;

use crate::nodes::bitmapset::{
    bms_add_member, bms_copy, bms_del_member, bms_difference, bms_is_empty, bms_is_member,
    bms_next_member, bms_overlap, bms_union, Bitmapset,
};
use crate::nodes::nodeFuncs::{
    expression_tree_mutator, expression_tree_walker, query_or_expression_tree_mutator,
    query_or_expression_tree_walker, query_tree_mutator, query_tree_walker, range_table_walker,
    tree_mutator_callback, tree_walker_callback, QTW_EXAMINE_RTES_BEFORE,
};
use crate::nodes::nodes::Node;
use crate::nodes::parsenodes::{Query, RangeTblEntry, RowMarkClause, RTEKind};
use crate::nodes::pathnodes::{AppendRelInfo, PlaceHolderVar, Relids};
use crate::nodes::pg_list::{lfirst, List};
use crate::nodes::plannodes::PlanRowMark;
use crate::nodes::primnodes::{
    Aggref, CurrentOfExpr, GroupingFunc, JoinExpr, RangeTblRef, ReturningExpr, Var, WindowFunc,
};
use crate::{foreach, current_cell, Assert, IsA};

// ----------------------------------------------------------------------------
// IS_SPECIAL_VARNO (primnodes.h macro): a varno < 0 is a special varno
// (INNER_VAR / OUTER_VAR / etc), not a real range-table index.
// ----------------------------------------------------------------------------
#[inline]
fn IS_SPECIAL_VARNO(varno: c_int) -> bool {
    varno < 0
}

/// Flat (shallow) copy of a single node of type `T`.  This is the
/// `palloc(sizeof(T)); memcpy(...)` idiom PostgreSQL uses in
/// add_nulling_relids_mutator for the PlaceHolderVar case; we reuse it for the
/// Var case as well (see module note on copyObject).
///
/// # Safety
/// `node` must be a valid, non-NULL pointer to a `T`.
unsafe fn copy_flat_node<T>(node: *const T) -> *mut T {
    let dst = palloc(core::mem::size_of::<T>()) as *mut T;
    core::ptr::copy_nonoverlapping(node, dst, 1);
    dst
}

// ----------------------------------------------------------------------------
// REPLACEVARS_* constants from rewriteManip.h
// ----------------------------------------------------------------------------

/// Passed to ReplaceVarsFromTargetList when a non-matching Var is found.
pub const REPLACEVARS_CHANGE_VARNO: c_int = 1;
/// Passed to ReplaceVarsFromTargetList to substitute NULL for non-matches.
pub const REPLACEVARS_SUBSTITUTE_NULL: c_int = 2;
/// Passed to ReplaceVarsFromTargetList to throw an error on non-matches.
pub const REPLACEVARS_REPORT_ERROR: c_int = 3;

// ----------------------------------------------------------------------------
// Walker context structs (#[repr(C)] so they can be passed as *mut c_void).
// ----------------------------------------------------------------------------

#[repr(C)]
struct contain_aggs_of_level_context {
    sublevels_up: c_int,
}

#[repr(C)]
struct locate_agg_of_level_context {
    agg_location: c_int,
    sublevels_up: c_int,
}

#[repr(C)]
struct locate_windowfunc_context {
    win_location: c_int,
}

#[repr(C)]
struct add_nulling_relids_context {
    target_relids: *const Bitmapset,
    added_relids: *const Bitmapset,
    sublevels_up: c_int,
}

#[repr(C)]
struct remove_nulling_relids_context {
    removable_relids: *const Bitmapset,
    except_relids: *const Bitmapset,
    sublevels_up: c_int,
}

#[repr(C)]
struct OffsetVarNodes_context {
    offset: c_int,
    sublevels_up: c_int,
}

// ChangeVarNodes_context is declared in rewrite/rewriteManip.h (forward-declared
// to expose it to ChangeVarNodes_callback users).  Reproduced here until the
// header has its own home.
pub type ChangeVarNodes_callback =
    Option<unsafe fn(node: *mut Node, arg: *mut ChangeVarNodes_context) -> bool>;

#[repr(C)]
pub struct ChangeVarNodes_context {
    pub rt_index: c_int,
    pub new_index: c_int,
    pub sublevels_up: c_int,
    pub callback: ChangeVarNodes_callback,
}

#[repr(C)]
struct IncrementVarSublevelsUp_context {
    delta_sublevels_up: c_int,
    min_sublevels_up: c_int,
}

#[repr(C)]
struct rangeTableEntry_used_context {
    rt_index: c_int,
    sublevels_up: c_int,
}

// ----------------------------------------------------------------------------
// contain_aggs_of_level
// ----------------------------------------------------------------------------

/*
 * contain_aggs_of_level -
 *	Check if an expression contains an aggregate function call of a
 *	specified query level.
 */
pub unsafe fn contain_aggs_of_level(node: *mut Node, levelsup: c_int) -> bool {
    let mut context = contain_aggs_of_level_context {
        sublevels_up: levelsup,
    };

    /*
     * Must be prepared to start with a Query or a bare expression tree; if
     * it's a Query, we don't want to increment sublevels_up.
     */
    query_or_expression_tree_walker(
        node,
        Some(contain_aggs_of_level_walker),
        &mut context as *mut _ as *mut c_void,
        0,
    )
}

unsafe fn contain_aggs_of_level_walker(node: *mut Node, context: *mut c_void) -> bool {
    let context = &mut *(context as *mut contain_aggs_of_level_context);

    if node.is_null() {
        return false;
    }
    if IsA!(node, T_Aggref) {
        if (*(node as *mut Aggref)).agglevelsup as c_int == context.sublevels_up {
            return true; /* abort the tree traversal and return true */
        }
        /* else fall through to examine argument */
    }
    if IsA!(node, T_GroupingFunc) {
        if (*(node as *mut GroupingFunc)).agglevelsup as c_int == context.sublevels_up {
            return true;
        }
        /* else fall through to examine argument */
    }
    if IsA!(node, T_Query) {
        /* Recurse into subselects */
        context.sublevels_up += 1;
        let result = query_tree_walker(
            node as *mut Query,
            Some(contain_aggs_of_level_walker),
            context as *mut _ as *mut c_void,
            0,
        );
        context.sublevels_up -= 1;
        return result;
    }
    expression_tree_walker(
        node,
        Some(contain_aggs_of_level_walker),
        context as *mut _ as *mut c_void,
    )
}

// ----------------------------------------------------------------------------
// locate_agg_of_level
// ----------------------------------------------------------------------------

/*
 * locate_agg_of_level -
 *	  Find the parse location of any aggregate of the specified query level.
 */
pub unsafe fn locate_agg_of_level(node: *mut Node, levelsup: c_int) -> c_int {
    let mut context = locate_agg_of_level_context {
        agg_location: -1, /* in case we find nothing */
        sublevels_up: levelsup,
    };

    query_or_expression_tree_walker(
        node,
        Some(locate_agg_of_level_walker),
        &mut context as *mut _ as *mut c_void,
        0,
    );

    context.agg_location
}

unsafe fn locate_agg_of_level_walker(node: *mut Node, context: *mut c_void) -> bool {
    let context = &mut *(context as *mut locate_agg_of_level_context);

    if node.is_null() {
        return false;
    }
    if IsA!(node, T_Aggref) {
        let agg = node as *mut Aggref;
        if (*agg).agglevelsup as c_int == context.sublevels_up && (*agg).location >= 0 {
            context.agg_location = (*agg).location;
            return true; /* abort the tree traversal and return true */
        }
        /* else fall through to examine argument */
    }
    if IsA!(node, T_GroupingFunc) {
        let grp = node as *mut GroupingFunc;
        if (*grp).agglevelsup as c_int == context.sublevels_up && (*grp).location >= 0 {
            context.agg_location = (*grp).location;
            return true; /* abort the tree traversal and return true */
        }
    }
    if IsA!(node, T_Query) {
        /* Recurse into subselects */
        context.sublevels_up += 1;
        let result = query_tree_walker(
            node as *mut Query,
            Some(locate_agg_of_level_walker),
            context as *mut _ as *mut c_void,
            0,
        );
        context.sublevels_up -= 1;
        return result;
    }
    expression_tree_walker(
        node,
        Some(locate_agg_of_level_walker),
        context as *mut _ as *mut c_void,
    )
}

// ----------------------------------------------------------------------------
// contain_windowfuncs
// ----------------------------------------------------------------------------

/*
 * contain_windowfuncs -
 *	Check if an expression contains a window function call of the
 *	current query level.
 */
pub unsafe fn contain_windowfuncs(node: *mut Node) -> bool {
    query_or_expression_tree_walker(node, Some(contain_windowfuncs_walker), null_mut(), 0)
}

unsafe fn contain_windowfuncs_walker(node: *mut Node, context: *mut c_void) -> bool {
    if node.is_null() {
        return false;
    }
    if IsA!(node, T_WindowFunc) {
        return true; /* abort the tree traversal and return true */
    }
    /* Mustn't recurse into subselects */
    expression_tree_walker(node, Some(contain_windowfuncs_walker), context)
}

// ----------------------------------------------------------------------------
// locate_windowfunc
// ----------------------------------------------------------------------------

/*
 * locate_windowfunc -
 *	  Find the parse location of any windowfunc of the current query level.
 */
pub unsafe fn locate_windowfunc(node: *mut Node) -> c_int {
    let mut context = locate_windowfunc_context {
        win_location: -1, /* in case we find nothing */
    };

    query_or_expression_tree_walker(
        node,
        Some(locate_windowfunc_walker),
        &mut context as *mut _ as *mut c_void,
        0,
    );

    context.win_location
}

unsafe fn locate_windowfunc_walker(node: *mut Node, context: *mut c_void) -> bool {
    let context = &mut *(context as *mut locate_windowfunc_context);

    if node.is_null() {
        return false;
    }
    if IsA!(node, T_WindowFunc) {
        if (*(node as *mut WindowFunc)).location >= 0 {
            context.win_location = (*(node as *mut WindowFunc)).location;
            return true; /* abort the tree traversal and return true */
        }
        /* else fall through to examine argument */
    }
    /* Mustn't recurse into subselects */
    expression_tree_walker(
        node,
        Some(locate_windowfunc_walker),
        context as *mut _ as *mut c_void,
    )
}

// ----------------------------------------------------------------------------
// OffsetVarNodes
// ----------------------------------------------------------------------------

unsafe fn OffsetVarNodes_walker(node: *mut Node, context: *mut c_void) -> bool {
    let context = &mut *(context as *mut OffsetVarNodes_context);

    if node.is_null() {
        return false;
    }
    if IsA!(node, T_Var) {
        let var = node as *mut Var;

        if (*var).varlevelsup as c_int == context.sublevels_up {
            (*var).varno += context.offset;
            (*var).varnullingrels = offset_relid_set((*var).varnullingrels, context.offset);
            if (*var).varnosyn as c_int > 0 {
                (*var).varnosyn = ((*var).varnosyn as c_int + context.offset) as Index;
            }
        }
        return false;
    }
    if IsA!(node, T_CurrentOfExpr) {
        let cexpr = node as *mut CurrentOfExpr;

        if context.sublevels_up == 0 {
            (*cexpr).cvarno = ((*cexpr).cvarno as c_int + context.offset) as Index;
        }
        return false;
    }
    if IsA!(node, T_RangeTblRef) {
        let rtr = node as *mut RangeTblRef;

        if context.sublevels_up == 0 {
            (*rtr).rtindex += context.offset;
        }
        /* the subquery itself is visited separately */
        return false;
    }
    if IsA!(node, T_JoinExpr) {
        let j = node as *mut JoinExpr;

        if (*j).rtindex != 0 && context.sublevels_up == 0 {
            (*j).rtindex += context.offset;
        }
        /* fall through to examine children */
    }
    if IsA!(node, T_PlaceHolderVar) {
        let phv = node as *mut PlaceHolderVar;

        if (*phv).phlevelsup as c_int == context.sublevels_up {
            (*phv).phrels = offset_relid_set((*phv).phrels, context.offset);
            (*phv).phnullingrels = offset_relid_set((*phv).phnullingrels, context.offset);
        }
        /* fall through to examine children */
    }
    if IsA!(node, T_AppendRelInfo) {
        let appinfo = node as *mut AppendRelInfo;

        if context.sublevels_up == 0 {
            (*appinfo).parent_relid =
                ((*appinfo).parent_relid as c_int + context.offset) as Index;
            (*appinfo).child_relid = ((*appinfo).child_relid as c_int + context.offset) as Index;
        }
        /* fall through to examine children */
    }
    /* Shouldn't need to handle other planner auxiliary nodes here */
    Assert!(!IsA!(node, T_PlanRowMark));
    Assert!(!IsA!(node, T_SpecialJoinInfo));
    Assert!(!IsA!(node, T_PlaceHolderInfo));
    Assert!(!IsA!(node, T_MinMaxAggInfo));

    if IsA!(node, T_Query) {
        /* Recurse into subselects */
        context.sublevels_up += 1;
        let result = query_tree_walker(
            node as *mut Query,
            Some(OffsetVarNodes_walker),
            context as *mut _ as *mut c_void,
            0,
        );
        context.sublevels_up -= 1;
        return result;
    }
    expression_tree_walker(
        node,
        Some(OffsetVarNodes_walker),
        context as *mut _ as *mut c_void,
    )
}

/*
 * OffsetVarNodes - adjust Vars when appending one query's RT to another
 *
 * Find all Var nodes in the given tree with varlevelsup == sublevels_up,
 * and increment their varno fields (rangetable indexes) by 'offset'.
 */
pub unsafe fn OffsetVarNodes(node: *mut Node, offset: c_int, sublevels_up: c_int) {
    let mut context = OffsetVarNodes_context {
        offset,
        sublevels_up,
    };

    /*
     * Must be prepared to start with a Query or a bare expression tree; if
     * it's a Query, go straight to query_tree_walker to make sure that
     * sublevels_up doesn't get incremented prematurely.
     */
    if !node.is_null() && IsA!(node, T_Query) {
        let qry = node as *mut Query;

        /*
         * If we are starting at a Query, and sublevels_up is zero, then we
         * must also fix rangetable indexes in the Query itself --- namely
         * resultRelation, mergeTargetRelation, exclRelIndex and rowMarks
         * entries.
         */
        if sublevels_up == 0 {
            if (*qry).resultRelation != 0 {
                (*qry).resultRelation += offset;
            }

            if (*qry).mergeTargetRelation != 0 {
                (*qry).mergeTargetRelation += offset;
            }

            if !(*qry).onConflict.is_null() && (*(*qry).onConflict).exclRelIndex != 0 {
                (*(*qry).onConflict).exclRelIndex += offset;
            }

            foreach!(l, (*qry).rowMarks, {
                let rc = lfirst(current_cell!(l)) as *mut RowMarkClause;
                (*rc).rti = ((*rc).rti as c_int + offset) as Index;
            });
        }
        query_tree_walker(
            qry,
            Some(OffsetVarNodes_walker),
            &mut context as *mut _ as *mut c_void,
            0,
        );
    } else {
        OffsetVarNodes_walker(node, &mut context as *mut _ as *mut c_void);
    }
}

unsafe fn offset_relid_set(relids: Relids, offset: c_int) -> Relids {
    let mut result: Relids = null_mut();
    let mut rtindex: c_int = -1;
    loop {
        rtindex = bms_next_member(relids, rtindex);
        if rtindex < 0 {
            break;
        }
        result = bms_add_member(result, rtindex + offset);
    }
    result
}

// ----------------------------------------------------------------------------
// ChangeVarNodes
// ----------------------------------------------------------------------------

unsafe fn ChangeVarNodes_walker(node: *mut Node, context: *mut c_void) -> bool {
    let context = &mut *(context as *mut ChangeVarNodes_context);

    if node.is_null() {
        return false;
    }

    if let Some(cb) = context.callback {
        if cb(node, context as *mut ChangeVarNodes_context) {
            return false;
        }
    }

    if IsA!(node, T_Var) {
        let var = node as *mut Var;

        if (*var).varlevelsup as c_int == context.sublevels_up {
            if (*var).varno == context.rt_index {
                (*var).varno = context.new_index;
            }
            (*var).varnullingrels =
                adjust_relid_set((*var).varnullingrels, context.rt_index, context.new_index);
            if (*var).varnosyn as c_int == context.rt_index {
                (*var).varnosyn = context.new_index as Index;
            }
        }
        return false;
    }
    if IsA!(node, T_CurrentOfExpr) {
        let cexpr = node as *mut CurrentOfExpr;

        if context.sublevels_up == 0 && (*cexpr).cvarno as c_int == context.rt_index {
            (*cexpr).cvarno = context.new_index as Index;
        }
        return false;
    }
    if IsA!(node, T_RangeTblRef) {
        let rtr = node as *mut RangeTblRef;

        if context.sublevels_up == 0 && (*rtr).rtindex == context.rt_index {
            (*rtr).rtindex = context.new_index;
        }
        /* the subquery itself is visited separately */
        return false;
    }
    if IsA!(node, T_JoinExpr) {
        let j = node as *mut JoinExpr;

        if context.sublevels_up == 0 && (*j).rtindex == context.rt_index {
            (*j).rtindex = context.new_index;
        }
        /* fall through to examine children */
    }
    if IsA!(node, T_PlaceHolderVar) {
        let phv = node as *mut PlaceHolderVar;

        if (*phv).phlevelsup as c_int == context.sublevels_up {
            (*phv).phrels =
                adjust_relid_set((*phv).phrels, context.rt_index, context.new_index);
            (*phv).phnullingrels = adjust_relid_set(
                (*phv).phnullingrels,
                context.rt_index,
                context.new_index,
            );
        }
        /* fall through to examine children */
    }
    if IsA!(node, T_PlanRowMark) {
        let rowmark = node as *mut PlanRowMark;

        if context.sublevels_up == 0 {
            if (*rowmark).rti as c_int == context.rt_index {
                (*rowmark).rti = context.new_index as Index;
            }
            if (*rowmark).prti as c_int == context.rt_index {
                (*rowmark).prti = context.new_index as Index;
            }
        }
        return false;
    }
    if IsA!(node, T_AppendRelInfo) {
        let appinfo = node as *mut AppendRelInfo;

        if context.sublevels_up == 0 {
            if (*appinfo).parent_relid as c_int == context.rt_index {
                (*appinfo).parent_relid = context.new_index as Index;
            }
            if (*appinfo).child_relid as c_int == context.rt_index {
                (*appinfo).child_relid = context.new_index as Index;
            }
        }
        /* fall through to examine children */
    }
    /* Shouldn't need to handle other planner auxiliary nodes here */
    Assert!(!IsA!(node, T_SpecialJoinInfo));
    Assert!(!IsA!(node, T_PlaceHolderInfo));
    Assert!(!IsA!(node, T_MinMaxAggInfo));

    if IsA!(node, T_Query) {
        /* Recurse into subselects */
        context.sublevels_up += 1;
        let result = query_tree_walker(
            node as *mut Query,
            Some(ChangeVarNodes_walker),
            context as *mut _ as *mut c_void,
            0,
        );
        context.sublevels_up -= 1;
        return result;
    }
    expression_tree_walker(
        node,
        Some(ChangeVarNodes_walker),
        context as *mut _ as *mut c_void,
    )
}

/*
 * ChangeVarNodesExtended - similar to ChangeVarNodes, but with an additional
 *							'callback' param.
 */
pub unsafe fn ChangeVarNodesExtended(
    node: *mut Node,
    rt_index: c_int,
    new_index: c_int,
    sublevels_up: c_int,
    callback: ChangeVarNodes_callback,
) {
    let mut context = ChangeVarNodes_context {
        rt_index,
        new_index,
        sublevels_up,
        callback,
    };

    if !node.is_null() && IsA!(node, T_Query) {
        let qry = node as *mut Query;

        if sublevels_up == 0 {
            if (*qry).resultRelation == rt_index {
                (*qry).resultRelation = new_index;
            }

            if (*qry).mergeTargetRelation == rt_index {
                (*qry).mergeTargetRelation = new_index;
            }

            /* this is unlikely to ever be used, but ... */
            if !(*qry).onConflict.is_null() && (*(*qry).onConflict).exclRelIndex == rt_index {
                (*(*qry).onConflict).exclRelIndex = new_index;
            }

            foreach!(l, (*qry).rowMarks, {
                let rc = lfirst(current_cell!(l)) as *mut RowMarkClause;
                if (*rc).rti as c_int == rt_index {
                    (*rc).rti = new_index as Index;
                }
            });
        }
        query_tree_walker(
            qry,
            Some(ChangeVarNodes_walker),
            &mut context as *mut _ as *mut c_void,
            0,
        );
    } else {
        ChangeVarNodes_walker(node, &mut context as *mut _ as *mut c_void);
    }
}

pub unsafe fn ChangeVarNodes(
    node: *mut Node,
    rt_index: c_int,
    new_index: c_int,
    sublevels_up: c_int,
) {
    ChangeVarNodesExtended(node, rt_index, new_index, sublevels_up, None);
}

/*
 * ChangeVarNodesWalkExpression - process expression within the custom
 *								  callback provided to the ChangeVarNodesExtended.
 */
pub unsafe fn ChangeVarNodesWalkExpression(
    node: *mut Node,
    context: *mut ChangeVarNodes_context,
) -> bool {
    expression_tree_walker(
        node,
        Some(ChangeVarNodes_walker),
        context as *mut c_void,
    )
}

/*
 * adjust_relid_set - substitute newrelid for oldrelid in a Relid set.
 */
pub unsafe fn adjust_relid_set(relids: Relids, oldrelid: c_int, newrelid: c_int) -> Relids {
    let mut relids = relids;
    if !IS_SPECIAL_VARNO(oldrelid) && bms_is_member(oldrelid, relids) {
        /* Ensure we have a modifiable copy */
        relids = bms_copy(relids);
        /* Remove old, add new */
        relids = bms_del_member(relids, oldrelid);
        if !IS_SPECIAL_VARNO(newrelid) {
            relids = bms_add_member(relids, newrelid);
        }
    }
    relids
}

// ----------------------------------------------------------------------------
// IncrementVarSublevelsUp
// ----------------------------------------------------------------------------

unsafe fn IncrementVarSublevelsUp_walker(node: *mut Node, context: *mut c_void) -> bool {
    let context = &mut *(context as *mut IncrementVarSublevelsUp_context);

    if node.is_null() {
        return false;
    }
    if IsA!(node, T_Var) {
        let var = node as *mut Var;

        if (*var).varlevelsup as c_int >= context.min_sublevels_up {
            (*var).varlevelsup =
                ((*var).varlevelsup as c_int + context.delta_sublevels_up) as Index;
        }
        return false; /* done here */
    }
    if IsA!(node, T_CurrentOfExpr) {
        /* this should not happen */
        if context.min_sublevels_up == 0 {
            elog!(ERROR, "cannot push down CurrentOfExpr");
        }
        return false;
    }
    if IsA!(node, T_Aggref) {
        let agg = node as *mut Aggref;

        if (*agg).agglevelsup as c_int >= context.min_sublevels_up {
            (*agg).agglevelsup =
                ((*agg).agglevelsup as c_int + context.delta_sublevels_up) as Index;
        }
        /* fall through to recurse into argument */
    }
    if IsA!(node, T_GroupingFunc) {
        let grp = node as *mut GroupingFunc;

        if (*grp).agglevelsup as c_int >= context.min_sublevels_up {
            (*grp).agglevelsup =
                ((*grp).agglevelsup as c_int + context.delta_sublevels_up) as Index;
        }
        /* fall through to recurse into argument */
    }
    if IsA!(node, T_PlaceHolderVar) {
        let phv = node as *mut PlaceHolderVar;

        if (*phv).phlevelsup as c_int >= context.min_sublevels_up {
            (*phv).phlevelsup =
                ((*phv).phlevelsup as c_int + context.delta_sublevels_up) as Index;
        }
        /* fall through to recurse into argument */
    }
    if IsA!(node, T_ReturningExpr) {
        let rexpr = node as *mut ReturningExpr;

        if (*rexpr).retlevelsup >= context.min_sublevels_up {
            (*rexpr).retlevelsup += context.delta_sublevels_up;
        }
        /* fall through to recurse into argument */
    }
    if IsA!(node, T_RangeTblEntry) {
        let rte = node as *mut RangeTblEntry;

        if (*rte).rtekind == RTEKind::RTE_CTE {
            if (*rte).ctelevelsup as c_int >= context.min_sublevels_up {
                (*rte).ctelevelsup =
                    ((*rte).ctelevelsup as c_int + context.delta_sublevels_up) as Index;
            }
        }
        return false; /* allow range_table_walker to continue */
    }
    if IsA!(node, T_Query) {
        /* Recurse into subselects */
        context.min_sublevels_up += 1;
        let result = query_tree_walker(
            node as *mut Query,
            Some(IncrementVarSublevelsUp_walker),
            context as *mut _ as *mut c_void,
            QTW_EXAMINE_RTES_BEFORE,
        );
        context.min_sublevels_up -= 1;
        return result;
    }
    expression_tree_walker(
        node,
        Some(IncrementVarSublevelsUp_walker),
        context as *mut _ as *mut c_void,
    )
}

/*
 * IncrementVarSublevelsUp - adjust Var nodes when pushing them down in tree.
 *
 * Find all Var nodes in the given tree having varlevelsup >= min_sublevels_up,
 * and add delta_sublevels_up to their varlevelsup value.
 */
pub unsafe fn IncrementVarSublevelsUp(
    node: *mut Node,
    delta_sublevels_up: c_int,
    min_sublevels_up: c_int,
) {
    let mut context = IncrementVarSublevelsUp_context {
        delta_sublevels_up,
        min_sublevels_up,
    };

    query_or_expression_tree_walker(
        node,
        Some(IncrementVarSublevelsUp_walker),
        &mut context as *mut _ as *mut c_void,
        QTW_EXAMINE_RTES_BEFORE,
    );
}

/*
 * IncrementVarSublevelsUp_rtable -
 *	Same as IncrementVarSublevelsUp, but to be invoked on a range table.
 */
pub unsafe fn IncrementVarSublevelsUp_rtable(
    rtable: *mut List,
    delta_sublevels_up: c_int,
    min_sublevels_up: c_int,
) {
    let mut context = IncrementVarSublevelsUp_context {
        delta_sublevels_up,
        min_sublevels_up,
    };

    range_table_walker(
        rtable,
        Some(IncrementVarSublevelsUp_walker),
        &mut context as *mut _ as *mut c_void,
        QTW_EXAMINE_RTES_BEFORE,
    );
}

// ----------------------------------------------------------------------------
// rangeTableEntry_used
// ----------------------------------------------------------------------------

unsafe fn rangeTableEntry_used_walker(node: *mut Node, context: *mut c_void) -> bool {
    let context = &mut *(context as *mut rangeTableEntry_used_context);

    if node.is_null() {
        return false;
    }
    if IsA!(node, T_Var) {
        let var = node as *mut Var;

        if (*var).varlevelsup as c_int == context.sublevels_up
            && ((*var).varno == context.rt_index
                || bms_is_member(context.rt_index, (*var).varnullingrels))
        {
            return true;
        }
        return false;
    }
    if IsA!(node, T_CurrentOfExpr) {
        let cexpr = node as *mut CurrentOfExpr;

        if context.sublevels_up == 0 && (*cexpr).cvarno as c_int == context.rt_index {
            return true;
        }
        return false;
    }
    if IsA!(node, T_RangeTblRef) {
        let rtr = node as *mut RangeTblRef;

        if (*rtr).rtindex == context.rt_index && context.sublevels_up == 0 {
            return true;
        }
        /* the subquery itself is visited separately */
        return false;
    }
    if IsA!(node, T_JoinExpr) {
        let j = node as *mut JoinExpr;

        if (*j).rtindex == context.rt_index && context.sublevels_up == 0 {
            return true;
        }
        /* fall through to examine children */
    }
    /* Shouldn't need to handle planner auxiliary nodes here */
    Assert!(!IsA!(node, T_PlaceHolderVar));
    Assert!(!IsA!(node, T_PlanRowMark));
    Assert!(!IsA!(node, T_SpecialJoinInfo));
    Assert!(!IsA!(node, T_AppendRelInfo));
    Assert!(!IsA!(node, T_PlaceHolderInfo));
    Assert!(!IsA!(node, T_MinMaxAggInfo));

    if IsA!(node, T_Query) {
        /* Recurse into subselects */
        context.sublevels_up += 1;
        let result = query_tree_walker(
            node as *mut Query,
            Some(rangeTableEntry_used_walker),
            context as *mut _ as *mut c_void,
            0,
        );
        context.sublevels_up -= 1;
        return result;
    }
    expression_tree_walker(
        node,
        Some(rangeTableEntry_used_walker),
        context as *mut _ as *mut c_void,
    )
}

/*
 * rangeTableEntry_used - detect whether an RTE is referenced somewhere
 *	in var nodes or join or setOp trees of a query or expression.
 */
pub unsafe fn rangeTableEntry_used(node: *mut Node, rt_index: c_int, sublevels_up: c_int) -> bool {
    let mut context = rangeTableEntry_used_context {
        rt_index,
        sublevels_up,
    };

    query_or_expression_tree_walker(
        node,
        Some(rangeTableEntry_used_walker),
        &mut context as *mut _ as *mut c_void,
        0,
    )
}

// ----------------------------------------------------------------------------
// add_nulling_relids
// ----------------------------------------------------------------------------

/*
 * add_nulling_relids() finds Vars and PlaceHolderVars that belong to any
 * of the target_relids, and adds added_relids to their varnullingrels
 * and phnullingrels fields.  If target_relids is NULL, all level-zero
 * Vars and PHVs are modified.
 */
pub unsafe fn add_nulling_relids(
    node: *mut Node,
    target_relids: *const Bitmapset,
    added_relids: *const Bitmapset,
) -> *mut Node {
    let mut context = add_nulling_relids_context {
        target_relids,
        added_relids,
        sublevels_up: 0,
    };
    query_or_expression_tree_mutator(
        node,
        Some(add_nulling_relids_mutator),
        &mut context as *mut _ as *mut c_void,
        0,
    )
}

unsafe fn add_nulling_relids_mutator(node: *mut Node, context: *mut c_void) -> *mut Node {
    let context = &mut *(context as *mut add_nulling_relids_context);

    if node.is_null() {
        return null_mut();
    }
    if IsA!(node, T_Var) {
        let var = node as *mut Var;

        if (*var).varlevelsup as c_int == context.sublevels_up
            && (context.target_relids.is_null()
                || bms_is_member((*var).varno, context.target_relids))
        {
            let newnullingrels = bms_union((*var).varnullingrels, context.added_relids);

            /* Copy the Var ... */
            let var = copy_flat_node(var);
            /* ... and replace the copy's varnullingrels field */
            (*var).varnullingrels = newnullingrels;
            return var as *mut Node;
        }
        /* Otherwise fall through to copy the Var normally */
    } else if IsA!(node, T_PlaceHolderVar) {
        let phv = node as *mut PlaceHolderVar;

        if (*phv).phlevelsup as c_int == context.sublevels_up
            && (context.target_relids.is_null()
                || bms_overlap((*phv).phrels, context.target_relids))
        {
            let newnullingrels = bms_union((*phv).phnullingrels, context.added_relids);

            /*
             * We don't modify the contents of the PHV's expression, only add
             * to phnullingrels.  Hence, just flat-copy the node ...
             */
            let phv = copy_flat_node(node as *const PlaceHolderVar);
            /* ... and replace the copy's phnullingrels field */
            (*phv).phnullingrels = newnullingrels;
            return phv as *mut Node;
        }
        /* Otherwise fall through to copy the PlaceHolderVar normally */
    } else if IsA!(node, T_Query) {
        /* Recurse into RTE or sublink subquery */
        context.sublevels_up += 1;
        let newnode = query_tree_mutator(
            node as *mut Query,
            Some(add_nulling_relids_mutator),
            context as *mut _ as *mut c_void,
            0,
        );
        context.sublevels_up -= 1;
        return newnode as *mut Node;
    }
    expression_tree_mutator(
        node,
        Some(add_nulling_relids_mutator),
        context as *mut _ as *mut c_void,
    )
}

// ----------------------------------------------------------------------------
// remove_nulling_relids
// ----------------------------------------------------------------------------

/*
 * remove_nulling_relids() removes mentions of the specified RT index(es)
 * in Var.varnullingrels and PlaceHolderVar.phnullingrels fields within
 * the given expression, except in nodes belonging to rels listed in
 * except_relids.
 */
pub unsafe fn remove_nulling_relids(
    node: *mut Node,
    removable_relids: *const Bitmapset,
    except_relids: *const Bitmapset,
) -> *mut Node {
    let mut context = remove_nulling_relids_context {
        removable_relids,
        except_relids,
        sublevels_up: 0,
    };
    query_or_expression_tree_mutator(
        node,
        Some(remove_nulling_relids_mutator),
        &mut context as *mut _ as *mut c_void,
        0,
    )
}

unsafe fn remove_nulling_relids_mutator(node: *mut Node, context: *mut c_void) -> *mut Node {
    let context = &mut *(context as *mut remove_nulling_relids_context);

    if node.is_null() {
        return null_mut();
    }
    if IsA!(node, T_Var) {
        let var = node as *mut Var;

        if (*var).varlevelsup as c_int == context.sublevels_up
            && !bms_is_member((*var).varno, context.except_relids)
            && bms_overlap((*var).varnullingrels, context.removable_relids)
        {
            /* Copy the Var ... */
            let var = copy_flat_node(var);
            /* ... and replace the copy's varnullingrels field */
            (*var).varnullingrels =
                bms_difference((*var).varnullingrels, context.removable_relids);
            return var as *mut Node;
        }
        /* Otherwise fall through to copy the Var normally */
    } else if IsA!(node, T_PlaceHolderVar) {
        let phv = node as *mut PlaceHolderVar;

        if (*phv).phlevelsup as c_int == context.sublevels_up
            && !bms_overlap((*phv).phrels, context.except_relids)
        {
            /*
             * Note: it might seem desirable to remove the PHV altogether if
             * phnullingrels goes to empty.  Currently we dare not do that
             * because we use PHVs in some cases to enforce separate identity
             * of subexpressions; see wrap_option usages in prepjointree.c.
             */
            /* Copy the PlaceHolderVar and mutate what's below ... */
            let phv = expression_tree_mutator(
                node,
                Some(remove_nulling_relids_mutator),
                context as *mut _ as *mut c_void,
            ) as *mut PlaceHolderVar;
            /* ... and replace the copy's phnullingrels field */
            (*phv).phnullingrels =
                bms_difference((*phv).phnullingrels, context.removable_relids);
            /* We must also update phrels, if it contains a removable RTI */
            (*phv).phrels = bms_difference((*phv).phrels, context.removable_relids);
            Assert!(!bms_is_empty((*phv).phrels));
            return phv as *mut Node;
        }
        /* Otherwise fall through to copy the PlaceHolderVar normally */
    } else if IsA!(node, T_Query) {
        /* Recurse into RTE or sublink subquery */
        context.sublevels_up += 1;
        let newnode = query_tree_mutator(
            node as *mut Query,
            Some(remove_nulling_relids_mutator),
            context as *mut _ as *mut c_void,
            0,
        );
        context.sublevels_up -= 1;
        return newnode as *mut Node;
    }
    expression_tree_mutator(
        node,
        Some(remove_nulling_relids_mutator),
        context as *mut _ as *mut c_void,
    )
}

// ----------------------------------------------------------------------------
// STUBS - genuinely unported deps (parser/parse_relation + parse_coerce +
// lsyscache + ROWTYPE/RowExpr expansion).  Signatures preserved; pointer-only
// types are kept opaque.
// ----------------------------------------------------------------------------

/// Opaque stand-in for the `attrMap` argument of map_variable_attnos (a
/// `*AttrNumber` array; left opaque until the callers are ported).
pub type AttrNumberPtr = c_void;

/*
 * map_variable_attnos() finds all user-column Vars in an expression tree
 * that reference a particular RTE, and adjusts their varattnos to reflect
 * the given mapping.
 *
 * STUB: needs ROWTYPE/RowExpr expansion via makeWholeRowVar / coercions.
 */
// TODO(pg-port): needs nodes/makefuncs + parser/parse_coerce + utils/lsyscache.
pub unsafe fn map_variable_attnos(
    _node: *mut Node,
    _target_varno: c_int,
    _sublevels_up: c_int,
    _attno_map: *const crate::access::common::attmap::AttrMap,
    _to_rowtype: Oid,
    _found_whole_row: *mut bool,
) -> *mut Node {
    unimplemented!("map_variable_attnos: parse_coerce/makefuncs/lsyscache not yet ported")
}

/// Opaque context for the replace_rte_variables callback machinery.
pub type replace_rte_variables_context = c_void;

/// Opaque callback type for replace_rte_variables (real signature needs the
/// real context struct from rewriteManip.h, which is parser-dependent).
pub type replace_rte_variables_callback = Option<
    unsafe fn(var: *mut Var, context: *mut replace_rte_variables_context) -> *mut Node,
>;

/*
 * replace_rte_variables() finds all Vars in an expression tree that reference
 * a particular RTE, and replaces them with substitute expressions obtained
 * from a caller-supplied callback function.
 *
 * STUB: needs parser/parse_relation + IncrementVarSublevelsUp-driven sublink
 * recording plus the full replace_rte_variables_context machinery.
 */
// TODO(pg-port): needs parser/parse_relation + parser/parsetree.
pub unsafe fn replace_rte_variables(
    _node: *mut Node,
    _target_varno: c_int,
    _sublevels_up: c_int,
    _callback: replace_rte_variables_callback,
    _callback_arg: *mut c_void,
    _outer_hasSubLinks: *mut bool,
) -> *mut Node {
    unimplemented!("replace_rte_variables: parser/parse_relation not yet ported")
}

/*
 * ReplaceVarsFromTargetList - replace Vars with items from a targetlist.
 *
 * STUB: needs parser/parse_relation (expandRTE / ROWTYPE handling),
 * parse_coerce, and replace_rte_variables.
 */
// TODO(pg-port): needs parser/parse_relation + parser/parse_coerce.
pub unsafe fn ReplaceVarsFromTargetList(
    _node: *mut Node,
    _target_varno: c_int,
    _sublevels_up: c_int,
    _target_rte: *mut RangeTblEntry,
    _targetlist: *mut List,
    _result_relation: c_int,
    _nomatch_option: c_int,
    _nomatch_varno: c_int,
    _outer_hasSubLinks: *mut bool,
) -> *mut Node {
    unimplemented!("ReplaceVarsFromTargetList: parser/parse_relation not yet ported")
}

// ----------------------------------------------------------------------------
// Tests for the REAL logic.
// ----------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nodes::nodes::NodeTag;

    /// Build a bare Var by hand: palloc0 + set the node tag and the requested
    /// varno / varlevelsup, leaving every other field zeroed.
    unsafe fn make_var(varno: c_int, varlevelsup: Index) -> *mut Var {
        let var = palloc0(core::mem::size_of::<Var>()) as *mut Var;
        (*var).xpr.r#type = NodeTag::T_Var;
        (*var).varno = varno;
        (*var).varlevelsup = varlevelsup;
        var
    }

    #[test]
    fn increment_var_sublevels_up_respects_min() {
        unsafe {
            // varlevelsup (2) >= min_sublevels_up (1) => bumped by delta (3).
            let var = make_var(1, 2);
            IncrementVarSublevelsUp(var as *mut Node, 3, 1);
            assert_eq!((*var).varlevelsup, 5);

            // varlevelsup (0) < min_sublevels_up (1) => unchanged.
            let var2 = make_var(1, 0);
            IncrementVarSublevelsUp(var2 as *mut Node, 3, 1);
            assert_eq!((*var2).varlevelsup, 0);

            // min_sublevels_up == 0 affects all Vars.
            let var3 = make_var(1, 0);
            IncrementVarSublevelsUp(var3 as *mut Node, 2, 0);
            assert_eq!((*var3).varlevelsup, 2);
        }
    }

    #[test]
    fn offset_var_nodes_shifts_varno_when_level_matches() {
        unsafe {
            // sublevels_up matches (0) => varno shifted by offset.
            let var = make_var(3, 0);
            OffsetVarNodes(var as *mut Node, 10, 0);
            assert_eq!((*var).varno, 13);

            // sublevels_up does not match => varno unchanged.
            let var2 = make_var(3, 0);
            OffsetVarNodes(var2 as *mut Node, 10, 1);
            assert_eq!((*var2).varno, 3);
        }
    }

    #[test]
    fn change_var_nodes_rewrites_matching_index() {
        unsafe {
            let var = make_var(4, 0);
            ChangeVarNodes(var as *mut Node, 4, 7, 0);
            assert_eq!((*var).varno, 7);

            // Non-matching varno is left alone.
            let var2 = make_var(5, 0);
            ChangeVarNodes(var2 as *mut Node, 4, 7, 0);
            assert_eq!((*var2).varno, 5);
        }
    }
}
