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
use crate::nodes::nodes::{CmdType::*, Node};
use crate::nodes::parsenodes::{Query, RangeTblEntry, RowMarkClause, RTEKind};
use crate::nodes::pathnodes::{AppendRelInfo, PlaceHolderVar, Relids};
use crate::nodes::pg_list::{lappend, lfirst, list_concat, list_length, linitial, List};
use crate::nodes::plannodes::PlanRowMark;
use crate::nodes::primnodes::{
    Aggref, BooleanTest, BoolTestType::*, CoercionForm::*, ConvertRowtypeExpr, CurrentOfExpr,
    GroupingFunc, JoinExpr, Param, ParamKind::*, RangeTblRef, ReturningExpr, RowExpr,
    TargetEntry, Var, VarReturningType, VarReturningType::*, WindowFunc, PRS2_NEW_VARNO,
    PRS2_OLD_VARNO,
};
use crate::access::common::attmap::AttrMap;
use crate::nodes::copyfuncs::copyObjectImpl;
use crate::nodes::makefuncs::make_and_qual;
use crate::parser::parse_relation::expandRTE;
use crate::parser::parse_coerce::coerce_null_to_domain;
use crate::parser::parsetree::{get_tle_by_resno, rt_fetch};
use crate::utils::cache::lsyscache::get_typlenbyval;
use crate::{foreach, current_cell, makeNode, lfirst_node, Assert, IsA};

const RECORDOID: Oid = 2249;

#[inline]
unsafe fn copyObject<T>(obj: *const T) -> *mut T {
    copyObjectImpl(obj as *const c_void) as *mut T
}

#[inline]
fn OidIsValid(oid: Oid) -> bool {
    oid != InvalidOid
}

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

/// Passed to ReplaceVarsFromTargetList to throw an error on non-matches.
pub const REPLACEVARS_REPORT_ERROR: c_int = 0;
/// Passed to ReplaceVarsFromTargetList when a non-matching Var is found.
pub const REPLACEVARS_CHANGE_VARNO: c_int = 1;
/// Passed to ReplaceVarsFromTargetList to substitute NULL for non-matches.
pub const REPLACEVARS_SUBSTITUTE_NULL: c_int = 2;

/// ReplaceVarsNoMatchOption (rewriteManip.h): action when no targetlist match.
pub type ReplaceVarsNoMatchOption = c_int;

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
// checkExprHasSubLink
// ----------------------------------------------------------------------------

/*
 * checkExprHasSubLink -
 *	Check if an expression contains a SubLink.
 */
pub unsafe fn checkExprHasSubLink(node: *mut Node) -> bool {
    /*
     * If a Query is passed, examine it --- but we should not recurse into
     * sub-Queries that are in its rangetable or CTE list.
     */
    query_or_expression_tree_walker(
        node,
        Some(checkExprHasSubLink_walker),
        null_mut(),
        crate::nodes::nodeFuncs::QTW_IGNORE_RC_SUBQUERIES,
    )
}

unsafe fn checkExprHasSubLink_walker(node: *mut Node, context: *mut c_void) -> bool {
    if node.is_null() {
        return false;
    }
    if IsA!(node, T_SubLink) {
        return true; /* abort the tree traversal and return true */
    }
    expression_tree_walker(node, Some(checkExprHasSubLink_walker), context)
}

/*
 * Check for MULTIEXPR Param within expression tree
 *
 * We intentionally don't descend into SubLinks: only Params at the current
 * query level are of interest.
 */
unsafe fn contains_multiexpr_param(node: *mut Node, context: *mut c_void) -> bool {
    if node.is_null() {
        return false;
    }
    if IsA!(node, T_Param) {
        if (*(node as *mut Param)).paramkind == PARAM_MULTIEXPR {
            return true; /* abort the tree traversal and return true */
        }
        return false;
    }
    expression_tree_walker(node, Some(contains_multiexpr_param), context)
}

// ----------------------------------------------------------------------------
// CombineRangeTables
// ----------------------------------------------------------------------------

/*
 * CombineRangeTables
 * 		Adds the RTEs of 'src_rtable' into 'dst_rtable'
 *
 * This also adds the RTEPermissionInfos of 'src_perminfos' (belonging to the
 * RTEs in 'src_rtable') into *dst_perminfos and also updates perminfoindex of
 * the RTEs in 'src_rtable' to now point to the perminfos' indexes in
 * *dst_perminfos.
 *
 * Note that this changes both 'dst_rtable' and 'dst_perminfos' destructively,
 * so the caller should have better passed safe-to-modify copies.
 */
pub unsafe fn CombineRangeTables(
    dst_rtable: *mut *mut List,
    dst_perminfos: *mut *mut List,
    src_rtable: *mut List,
    src_perminfos: *mut List,
) {
    let offset = list_length(*dst_perminfos);

    if offset > 0 {
        foreach!(l, src_rtable, {
            let rte = lfirst_node!(RangeTblEntry, T_RangeTblEntry, current_cell!(l));

            if (*rte).perminfoindex > 0 {
                (*rte).perminfoindex += offset as Index;
            }
        });
    }

    *dst_perminfos = list_concat(*dst_perminfos, src_perminfos);
    *dst_rtable = list_concat(*dst_rtable, src_rtable);
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
// SetVarReturningType
// ----------------------------------------------------------------------------

#[repr(C)]
struct SetVarReturningType_context {
    result_relation: c_int,
    sublevels_up: c_int,
    returning_type: VarReturningType,
}

unsafe fn SetVarReturningType_walker(node: *mut Node, context: *mut c_void) -> bool {
    let context = &mut *(context as *mut SetVarReturningType_context);

    if node.is_null() {
        return false;
    }
    if IsA!(node, T_Var) {
        let var = node as *mut Var;

        if (*var).varno == context.result_relation
            && (*var).varlevelsup as c_int == context.sublevels_up
        {
            (*var).varreturningtype = context.returning_type;
        }

        return false;
    }

    if IsA!(node, T_Query) {
        /* Recurse into subselects */
        context.sublevels_up += 1;
        let result = query_tree_walker(
            node as *mut Query,
            Some(SetVarReturningType_walker),
            context as *mut _ as *mut c_void,
            0,
        );
        context.sublevels_up -= 1;
        return result;
    }
    expression_tree_walker(
        node,
        Some(SetVarReturningType_walker),
        context as *mut _ as *mut c_void,
    )
}

unsafe fn SetVarReturningType(
    node: *mut Node,
    result_relation: c_int,
    sublevels_up: c_int,
    returning_type: VarReturningType,
) {
    let mut context = SetVarReturningType_context {
        result_relation,
        sublevels_up,
        returning_type,
    };

    /* Expect to start with an expression */
    SetVarReturningType_walker(node, &mut context as *mut _ as *mut c_void);
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
// getInsertSelectQuery / AddQual / AddInvertedQual
// ----------------------------------------------------------------------------

extern "C" {
    fn strcmp(s1: *const c_char, s2: *const c_char) -> c_int;
}

/*
 * If the given Query is an INSERT ... SELECT construct, extract and
 * return the sub-Query node that represents the SELECT part.  Otherwise
 * return the given Query.
 *
 * If subquery_ptr is not NULL, then *subquery_ptr is set to the location
 * of the link to the SELECT subquery inside parsetree, or NULL if not an
 * INSERT ... SELECT.
 *
 * This is a hack needed because transformations on INSERT ... SELECTs that
 * appear in rule actions should be applied to the source SELECT, not to the
 * INSERT part.  Perhaps this can be cleaned up with redesigned querytrees.
 */
pub unsafe fn getInsertSelectQuery(
    parsetree: *mut Query,
    subquery_ptr: *mut *mut *mut Query,
) -> *mut Query {
    let selectquery: *mut Query;
    let selectrte: *mut RangeTblEntry;
    let rtr: *mut RangeTblRef;

    if !subquery_ptr.is_null() {
        *subquery_ptr = null_mut();
    }

    if parsetree.is_null() {
        return parsetree;
    }
    if (*parsetree).commandType != CMD_INSERT {
        return parsetree;
    }

    /*
     * Currently, this is ONLY applied to rule-action queries, and so we
     * expect to find the OLD and NEW placeholder entries in the given query.
     * If they're not there, it must be an INSERT/SELECT in which they've been
     * pushed down to the SELECT.
     */
    if list_length((*parsetree).rtable) >= 2
        && strcmp(
            (*(*rt_fetch(PRS2_OLD_VARNO as Index, (*parsetree).rtable)).eref).aliasname,
            c"old".as_ptr(),
        ) == 0
        && strcmp(
            (*(*rt_fetch(PRS2_NEW_VARNO as Index, (*parsetree).rtable)).eref).aliasname,
            c"new".as_ptr(),
        ) == 0
    {
        return parsetree;
    }
    Assert!(!(*parsetree).jointree.is_null() && IsA!((*parsetree).jointree, T_FromExpr));
    if list_length((*(*parsetree).jointree).fromlist) != 1 {
        elog!(ERROR, "expected to find SELECT subquery");
    }
    rtr = linitial((*(*parsetree).jointree).fromlist) as *mut RangeTblRef;
    if !IsA!(rtr, T_RangeTblRef) {
        elog!(ERROR, "expected to find SELECT subquery");
    }
    selectrte = rt_fetch((*rtr).rtindex as Index, (*parsetree).rtable);
    if !((*selectrte).rtekind == RTE_SUBQUERY
        && !(*selectrte).subquery.is_null()
        && IsA!((*selectrte).subquery, T_Query)
        && (*(*selectrte).subquery).commandType == CMD_SELECT)
    {
        elog!(ERROR, "expected to find SELECT subquery");
    }
    selectquery = (*selectrte).subquery;
    if list_length((*selectquery).rtable) >= 2
        && strcmp(
            (*(*rt_fetch(PRS2_OLD_VARNO as Index, (*selectquery).rtable)).eref).aliasname,
            c"old".as_ptr(),
        ) == 0
        && strcmp(
            (*(*rt_fetch(PRS2_NEW_VARNO as Index, (*selectquery).rtable)).eref).aliasname,
            c"new".as_ptr(),
        ) == 0
    {
        if !subquery_ptr.is_null() {
            *subquery_ptr = &raw mut (*selectrte).subquery;
        }
        return selectquery;
    }
    elog!(ERROR, "could not find rule placeholders");
    #[allow(unreachable_code)]
    null_mut() /* not reached */
}

/*
 * Add the given qualifier condition to the query's WHERE clause
 */
pub unsafe fn AddQual(parsetree: *mut Query, qual: *mut Node) {
    let copy: *mut Node;

    if qual.is_null() {
        return;
    }

    if (*parsetree).commandType == CMD_UTILITY {
        /*
         * There's noplace to put the qual on a utility statement.
         *
         * If it's a NOTIFY, silently ignore the qual; this means that the
         * NOTIFY will execute, whether or not there are any qualifying rows.
         * While clearly wrong, this is much more useful than refusing to
         * execute the rule at all, and extra NOTIFY events are harmless for
         * typical uses of NOTIFY.
         *
         * If it isn't a NOTIFY, error out, since unconditional execution of
         * other utility stmts is unlikely to be wanted.  (This case is not
         * currently allowed anyway, but keep the test for safety.)
         */
        if !(*parsetree).utilityStmt.is_null() && IsA!((*parsetree).utilityStmt, T_NotifyStmt) {
            return;
        } else {
            ereport!(
                ERROR,
                errmsg!("conditional utility statements are not implemented")
            );
            /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
        }
    }

    if !(*parsetree).setOperations.is_null() {
        /*
         * There's noplace to put the qual on a setop statement, either. (This
         * could be fixed, but right now the planner simply ignores any qual
         * condition on a setop query.)
         */
        ereport!(
            ERROR,
            errmsg!("conditional UNION/INTERSECT/EXCEPT statements are not implemented")
        );
        /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
    }

    /* INTERSECT wants the original, but we need to copy - Jan */
    copy = copyObject(qual);

    (*(*parsetree).jointree).quals = make_and_qual((*(*parsetree).jointree).quals, copy);

    /*
     * We had better not have stuck an aggregate into the WHERE clause.
     */
    Assert!(!contain_aggs_of_level(copy, 0));

    /*
     * Make sure query is marked correctly if added qual has sublinks. Need
     * not search qual when query is already marked.
     */
    if !(*parsetree).hasSubLinks {
        (*parsetree).hasSubLinks = checkExprHasSubLink(copy);
    }
}

/*
 * Invert the given clause and add it to the WHERE qualifications of the
 * given querytree.  Inversion means "x IS NOT TRUE", not just "NOT x",
 * else we will do the wrong thing when x evaluates to NULL.
 */
pub unsafe fn AddInvertedQual(parsetree: *mut Query, qual: *mut Node) {
    let invqual: *mut BooleanTest;

    if qual.is_null() {
        return;
    }

    /* Need not copy input qual, because AddQual will... */
    invqual = makeNode!(BooleanTest, T_BooleanTest);
    (*invqual).arg = qual as *mut crate::nodes::primnodes::Expr;
    (*invqual).booltesttype = IS_NOT_TRUE;
    (*invqual).location = -1;

    AddQual(parsetree, invqual as *mut Node);
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
// replace_rte_variables
// ----------------------------------------------------------------------------

/* matches struct replace_rte_variables_context in rewriteManip.h */
pub type replace_rte_variables_callback =
    Option<unsafe fn(var: *mut Var, context: *mut replace_rte_variables_context) -> *mut Node>;

#[repr(C)]
pub struct replace_rte_variables_context {
    pub callback: replace_rte_variables_callback, /* callback function */
    pub callback_arg: *mut c_void,                /* context data for callback function */
    pub target_varno: c_int,                      /* RTE index to search for */
    pub sublevels_up: c_int,                      /* (current) nesting depth */
    pub inserted_sublink: bool,                   /* have we inserted a SubLink? */
}

pub unsafe fn replace_rte_variables(
    node: *mut Node,
    target_varno: c_int,
    sublevels_up: c_int,
    callback: replace_rte_variables_callback,
    callback_arg: *mut c_void,
    outer_hasSubLinks: *mut bool,
) -> *mut Node {
    let result: *mut Node;
    let mut context = replace_rte_variables_context {
        callback,
        callback_arg,
        target_varno,
        sublevels_up,
        inserted_sublink: false,
    };

    /*
     * We try to initialize inserted_sublink to true if there is no need to
     * detect new sublinks because the query already has some.
     */
    if !node.is_null() && IsA!(node, T_Query) {
        context.inserted_sublink = (*(node as *mut Query)).hasSubLinks;
    } else if !outer_hasSubLinks.is_null() {
        context.inserted_sublink = *outer_hasSubLinks;
    } else {
        context.inserted_sublink = false;
    }

    /*
     * Must be prepared to start with a Query or a bare expression tree; if
     * it's a Query, we don't want to increment sublevels_up.
     */
    result = query_or_expression_tree_mutator(
        node,
        Some(replace_rte_variables_mutator),
        &mut context as *mut _ as *mut c_void,
        0,
    );

    if context.inserted_sublink {
        if !result.is_null() && IsA!(result, T_Query) {
            (*(result as *mut Query)).hasSubLinks = true;
        } else if !outer_hasSubLinks.is_null() {
            *outer_hasSubLinks = true;
        } else {
            elog!(
                ERROR,
                "replace_rte_variables inserted a SubLink, but has noplace to record it"
            );
        }
    }

    result
}

pub unsafe fn replace_rte_variables_mutator(node: *mut Node, context: *mut c_void) -> *mut Node {
    let context = &mut *(context as *mut replace_rte_variables_context);

    if node.is_null() {
        return null_mut();
    }
    if IsA!(node, T_Var) {
        let var = node as *mut Var;

        if (*var).varno == context.target_varno
            && (*var).varlevelsup as c_int == context.sublevels_up
        {
            /* Found a matching variable, make the substitution */
            let newnode: *mut Node;

            newnode = (context.callback.unwrap())(var, context as *mut _);
            /* Detect if we are adding a sublink to query */
            if !context.inserted_sublink {
                context.inserted_sublink = checkExprHasSubLink(newnode);
            }
            return newnode;
        }
        /* otherwise fall through to copy the var normally */
    } else if IsA!(node, T_CurrentOfExpr) {
        let cexpr = node as *mut CurrentOfExpr;

        if (*cexpr).cvarno as c_int == context.target_varno && context.sublevels_up == 0 {
            /*
             * We get here if a WHERE CURRENT OF expression turns out to apply
             * to a view.  Someday we might be able to translate the
             * expression to apply to an underlying table of the view, but
             * right now it's not implemented.
             */
            ereport!(ERROR, errmsg!("WHERE CURRENT OF on a view is not implemented"));
            /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
        }
        /* otherwise fall through to copy the expr normally */
    } else if IsA!(node, T_Query) {
        /* Recurse into RTE subquery or not-yet-planned sublink subquery */
        let newnode: *mut Query;
        let save_inserted_sublink: bool;

        context.sublevels_up += 1;
        save_inserted_sublink = context.inserted_sublink;
        context.inserted_sublink = (*(node as *mut Query)).hasSubLinks;
        newnode = query_tree_mutator(
            node as *mut Query,
            Some(replace_rte_variables_mutator),
            context as *mut _ as *mut c_void,
            0,
        );
        (*newnode).hasSubLinks |= context.inserted_sublink;
        context.inserted_sublink = save_inserted_sublink;
        context.sublevels_up -= 1;
        return newnode as *mut Node;
    }
    expression_tree_mutator(
        node,
        Some(replace_rte_variables_mutator),
        context as *mut _ as *mut c_void,
    )
}

// ----------------------------------------------------------------------------
// map_variable_attnos
// ----------------------------------------------------------------------------

#[repr(C)]
struct map_variable_attnos_context {
    target_varno: c_int,         /* RTE index to search for */
    sublevels_up: c_int,         /* (current) nesting depth */
    attno_map: *const AttrMap,   /* map array for user attnos */
    to_rowtype: Oid,             /* change whole-row Vars to this type */
    found_whole_row: *mut bool,  /* output flag */
}

unsafe fn map_variable_attnos_mutator(node: *mut Node, context: *mut c_void) -> *mut Node {
    let context = &mut *(context as *mut map_variable_attnos_context);

    if node.is_null() {
        return null_mut();
    }
    if IsA!(node, T_Var) {
        let var = node as *mut Var;

        if (*var).varno == context.target_varno
            && (*var).varlevelsup as c_int == context.sublevels_up
        {
            /* Found a matching variable, make the substitution */
            let newvar = palloc(core::mem::size_of::<Var>()) as *mut Var;
            let attno = (*var).varattno;

            *newvar = *var; /* initially copy all fields of the Var */

            if attno > 0 {
                /* user-defined column, replace attno */
                if attno as c_int > (*context.attno_map).maplen
                    || *(*context.attno_map).attnums.add((attno - 1) as usize) == 0
                {
                    elog!(
                        ERROR,
                        "unexpected varattno {} in expression to be mapped",
                        attno
                    );
                }
                (*newvar).varattno = *(*context.attno_map).attnums.add((attno - 1) as usize);
                /* If the syntactic referent is same RTE, fix it too */
                if (*newvar).varnosyn as c_int == context.target_varno {
                    (*newvar).varattnosyn = (*newvar).varattno;
                }
            } else if attno == 0 {
                /* whole-row variable, warn caller */
                *(context.found_whole_row) = true;

                /* If the caller expects us to convert the Var, do so. */
                if OidIsValid(context.to_rowtype) && context.to_rowtype != (*var).vartype {
                    let r: *mut ConvertRowtypeExpr;

                    /* This certainly won't work for a RECORD variable. */
                    Assert!((*var).vartype != RECORDOID);

                    /* Var itself is changed to the requested type. */
                    (*newvar).vartype = context.to_rowtype;

                    /*
                     * Add a conversion node on top to convert back to the
                     * original type expected by the expression.
                     */
                    r = makeNode!(ConvertRowtypeExpr, T_ConvertRowtypeExpr);
                    (*r).arg = newvar as *mut crate::nodes::primnodes::Expr;
                    (*r).resulttype = (*var).vartype;
                    (*r).convertformat = COERCE_IMPLICIT_CAST;
                    (*r).location = -1;

                    return r as *mut Node;
                }
            }
            return newvar as *mut Node;
        }
        /* otherwise fall through to copy the var normally */
    } else if IsA!(node, T_ConvertRowtypeExpr) {
        let r = node as *mut ConvertRowtypeExpr;
        let var = (*r).arg as *mut Var;

        /*
         * If this is coercing a whole-row Var that we need to convert, then
         * just convert the Var without adding an extra ConvertRowtypeExpr.
         * Effectively we're simplifying var::parenttype::grandparenttype into
         * just var::grandparenttype.  This avoids building stacks of CREs if
         * this function is applied repeatedly.
         */
        if IsA!(var, T_Var)
            && (*var).varno == context.target_varno
            && (*var).varlevelsup as c_int == context.sublevels_up
            && (*var).varattno == 0
            && OidIsValid(context.to_rowtype)
            && context.to_rowtype != (*var).vartype
        {
            let newnode: *mut ConvertRowtypeExpr;
            let newvar = palloc(core::mem::size_of::<Var>()) as *mut Var;

            /* whole-row variable, warn caller */
            *(context.found_whole_row) = true;

            *newvar = *var; /* initially copy all fields of the Var */

            /* This certainly won't work for a RECORD variable. */
            Assert!((*var).vartype != RECORDOID);

            /* Var itself is changed to the requested type. */
            (*newvar).vartype = context.to_rowtype;

            newnode = palloc(core::mem::size_of::<ConvertRowtypeExpr>()) as *mut ConvertRowtypeExpr;
            *newnode = *r; /* initially copy all fields of the CRE */
            (*newnode).arg = newvar as *mut crate::nodes::primnodes::Expr;

            return newnode as *mut Node;
        }
        /* otherwise fall through to process the expression normally */
    } else if IsA!(node, T_Query) {
        /* Recurse into RTE subquery or not-yet-planned sublink subquery */
        context.sublevels_up += 1;
        let newnode = query_tree_mutator(
            node as *mut Query,
            Some(map_variable_attnos_mutator),
            context as *mut _ as *mut c_void,
            0,
        );
        context.sublevels_up -= 1;
        return newnode as *mut Node;
    }
    expression_tree_mutator(
        node,
        Some(map_variable_attnos_mutator),
        context as *mut _ as *mut c_void,
    )
}

pub unsafe fn map_variable_attnos(
    node: *mut Node,
    target_varno: c_int,
    sublevels_up: c_int,
    attno_map: *const AttrMap,
    to_rowtype: Oid,
    found_whole_row: *mut bool,
) -> *mut Node {
    let mut context = map_variable_attnos_context {
        target_varno,
        sublevels_up,
        attno_map,
        to_rowtype,
        found_whole_row,
    };

    *found_whole_row = false;

    /*
     * Must be prepared to start with a Query or a bare expression tree; if
     * it's a Query, we don't want to increment sublevels_up.
     */
    query_or_expression_tree_mutator(
        node,
        Some(map_variable_attnos_mutator),
        &mut context as *mut _ as *mut c_void,
        0,
    )
}

// ----------------------------------------------------------------------------
// ReplaceVarsFromTargetList
// ----------------------------------------------------------------------------

#[repr(C)]
struct ReplaceVarsFromTargetList_context {
    target_rte: *mut RangeTblEntry,
    targetlist: *mut List,
    result_relation: c_int,
    nomatch_option: ReplaceVarsNoMatchOption,
    nomatch_varno: c_int,
}

unsafe fn ReplaceVarsFromTargetList_callback(
    var: *mut Var,
    context: *mut replace_rte_variables_context,
) -> *mut Node {
    let rcon = (*context).callback_arg as *mut ReplaceVarsFromTargetList_context;
    let newnode: *mut Node;

    newnode = ReplaceVarFromTargetList(
        var,
        (*rcon).target_rte,
        (*rcon).targetlist,
        (*rcon).result_relation,
        (*rcon).nomatch_option,
        (*rcon).nomatch_varno,
    );

    /* Must adjust varlevelsup if replaced Var is within a subquery */
    if (*var).varlevelsup > 0 {
        IncrementVarSublevelsUp(newnode, (*var).varlevelsup as c_int, 0);
    }

    newnode
}

pub unsafe fn ReplaceVarFromTargetList(
    var: *mut Var,
    target_rte: *mut RangeTblEntry,
    targetlist: *mut List,
    result_relation: c_int,
    nomatch_option: ReplaceVarsNoMatchOption,
    nomatch_varno: c_int,
) -> *mut Node {
    let tle: *mut TargetEntry;

    if (*var).varattno == InvalidAttrNumber {
        /* Must expand whole-tuple reference into RowExpr */
        let rowexpr: *mut RowExpr;
        let mut colnames: *mut List = null_mut();
        let mut fields: *mut List = null_mut();

        /*
         * If generating an expansion for a var of a named rowtype (ie, this
         * is a plain relation RTE), then we must include dummy items for
         * dropped columns.  If the var is RECORD (ie, this is a JOIN), then
         * omit dropped columns.  In the latter case, attach column names to
         * the RowExpr for use of the executor and ruleutils.c.
         *
         * In order to be able to cache the results, we always generate the
         * expansion with varlevelsup = 0.  The caller is responsible for
         * adjusting it if needed.
         *
         * The varreturningtype is copied onto each individual field Var, so
         * that it is handled correctly when we recurse.
         */
        expandRTE(
            target_rte,
            (*var).varno,
            0, /* not varlevelsup */
            (*var).varreturningtype,
            (*var).location,
            (*var).vartype != RECORDOID,
            &mut colnames,
            &mut fields,
        );
        rowexpr = makeNode!(RowExpr, T_RowExpr);
        /* the fields will be set below */
        (*rowexpr).args = null_mut();
        (*rowexpr).row_typeid = (*var).vartype;
        (*rowexpr).row_format = COERCE_IMPLICIT_CAST;
        (*rowexpr).colnames = if (*var).vartype == RECORDOID {
            colnames
        } else {
            null_mut()
        };
        (*rowexpr).location = (*var).location;
        /* Adjust the generated per-field Vars... */
        foreach!(lc, fields, {
            let mut field = lfirst(current_cell!(lc)) as *mut Node;

            if !field.is_null() && IsA!(field, T_Var) {
                field = ReplaceVarFromTargetList(
                    field as *mut Var,
                    target_rte,
                    targetlist,
                    result_relation,
                    nomatch_option,
                    nomatch_varno,
                );
            }
            (*rowexpr).args = lappend((*rowexpr).args, field as *mut c_void);
        });

        /* Wrap it in a ReturningExpr, if needed, per comments above */
        if (*var).varreturningtype != VAR_RETURNING_DEFAULT {
            let rexpr = makeNode!(ReturningExpr, T_ReturningExpr);

            (*rexpr).retlevelsup = 0;
            (*rexpr).retold = (*var).varreturningtype == VAR_RETURNING_OLD;
            (*rexpr).retexpr = rowexpr as *mut crate::nodes::primnodes::Expr;

            return rexpr as *mut Node;
        }

        return rowexpr as *mut Node;
    }

    /* Normal case referencing one targetlist element */
    tle = get_tle_by_resno(targetlist, (*var).varattno);

    if tle.is_null() || (*tle).resjunk {
        /* Failed to find column in targetlist */
        match nomatch_option {
            REPLACEVARS_REPORT_ERROR => {
                /* fall through, throw error below */
            }

            REPLACEVARS_CHANGE_VARNO => {
                let var = copyObject(var);
                (*var).varno = nomatch_varno;
                (*var).varlevelsup = 0;
                /* we leave the syntactic referent alone */
                return var as *mut Node;
            }

            REPLACEVARS_SUBSTITUTE_NULL => {
                /*
                 * If Var is of domain type, we must add a CoerceToDomain
                 * node, in case there is a NOT NULL domain constraint.
                 */
                let mut vartyplen: i16 = 0;
                let mut vartypbyval: bool = false;

                get_typlenbyval((*var).vartype, &mut vartyplen, &mut vartypbyval);
                return coerce_null_to_domain(
                    (*var).vartype,
                    (*var).vartypmod,
                    (*var).varcollid,
                    vartyplen as c_int,
                    vartypbyval,
                );
            }
            _ => {}
        }
        elog!(
            ERROR,
            "could not find replacement targetlist entry for attno {}",
            (*var).varattno
        );
        #[allow(unreachable_code)]
        null_mut() /* keep compiler quiet */
    } else {
        /* Make a copy of the tlist item to return */
        let mut newnode = copyObject((*tle).expr);

        /*
         * Check to see if the tlist item contains a PARAM_MULTIEXPR Param,
         * and throw error if so.  This case could only happen when expanding
         * an ON UPDATE rule's NEW variable and the referenced tlist item in
         * the original UPDATE command is part of a multiple assignment. There
         * seems no practical way to handle such cases without multiple
         * evaluation of the multiple assignment's sub-select, which would
         * create semantic oddities that users of rules would probably prefer
         * not to cope with.  So treat it as an unimplemented feature.
         */
        if contains_multiexpr_param(newnode as *mut Node, null_mut()) {
            ereport!(
                ERROR,
                errmsg!("NEW variables in ON UPDATE rules cannot reference columns that are part of a multiple assignment in the subject UPDATE command")
            );
            /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
        }

        /* Handle any OLD/NEW RETURNING list Vars */
        if (*var).varreturningtype != VAR_RETURNING_DEFAULT {
            /*
             * Copy varreturningtype onto any Vars in the tlist item that
             * refer to result_relation (which had better be non-zero).
             */
            if result_relation == 0 {
                elog!(
                    ERROR,
                    "variable returning old/new found outside RETURNING list"
                );
            }

            SetVarReturningType(
                newnode as *mut Node,
                result_relation,
                0,
                (*var).varreturningtype,
            );

            /* Wrap it in a ReturningExpr, if needed, per comments above */
            if !IsA!(newnode, T_Var)
                || (*(newnode as *mut Var)).varno != result_relation
                || (*(newnode as *mut Var)).varlevelsup != 0
            {
                let rexpr = makeNode!(ReturningExpr, T_ReturningExpr);

                (*rexpr).retlevelsup = 0;
                (*rexpr).retold = (*var).varreturningtype == VAR_RETURNING_OLD;
                (*rexpr).retexpr = newnode;

                newnode = rexpr as *mut crate::nodes::primnodes::Expr;
            }
        }

        newnode as *mut Node
    }
}

pub unsafe fn ReplaceVarsFromTargetList(
    node: *mut Node,
    target_varno: c_int,
    sublevels_up: c_int,
    target_rte: *mut RangeTblEntry,
    targetlist: *mut List,
    result_relation: c_int,
    nomatch_option: ReplaceVarsNoMatchOption,
    nomatch_varno: c_int,
    outer_hasSubLinks: *mut bool,
) -> *mut Node {
    let mut context = ReplaceVarsFromTargetList_context {
        target_rte,
        targetlist,
        result_relation,
        nomatch_option,
        nomatch_varno,
    };

    replace_rte_variables(
        node,
        target_varno,
        sublevels_up,
        Some(ReplaceVarsFromTargetList_callback),
        &mut context as *mut _ as *mut c_void,
        outer_hasSubLinks,
    )
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
