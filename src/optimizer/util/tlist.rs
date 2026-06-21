//! Translation of postgres/src/backend/optimizer/util/tlist.c
//!
//! Target list manipulation routines.
//!
//! #include mapping:
//!   "postgres.h"               -> crate::prelude::*
//!   "nodes/makefuncs.h"        -> crate::nodes::makefuncs (makeTargetEntry, makeNode!)
//!   "nodes/nodeFuncs.h"        -> crate::nodes::nodeFuncs (exprType/exprCollation/
//!                                 expression_tree_walker)
//!   "optimizer/cost.h"         -> set_pathtarget_cost_width (STUB; cost.c not ported)
//!   "optimizer/optimizer.h"    -> public fn signatures
//!   "optimizer/tlist.h"        -> public fn signatures; get_pathtarget_sortgroupref
//!                                 (crate::nodes::pathnodes)
//!   "rewrite/rewriteManip.h"   -> remove_nulling_relids (STUB; rewriteManip.c not ported)
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! Translation notes (deviations from the C source):
//!
//! * TargetEntry / Var / FuncExpr / OpExpr / PlaceHolderVar / Aggref / GroupingFunc /
//!   WindowFunc live in crate::nodes::primnodes; SortGroupClause in
//!   crate::nodes::parsenodes; PathTarget / VolatileFunctionStatus /
//!   get_pathtarget_sortgroupref in crate::nodes::pathnodes; PlannerInfo / Query in
//!   crate::nodes::pathnodes / parsenodes.  Field sets are taken verbatim.
//! * C `node->expr` etc. are raw pointer field accesses; the file is entirely
//!   `unsafe` like the rest of the port.
//! * STUBS (genuinely unported deps), used only by split_pathtarget_at_srfs paths:
//!     - `copyObject`  (nodes/copyfuncs.c) -- deep copy. Used by add_to_flat_tlist and
//!       add_sp_item_to_pathtarget.
//!     - `set_pathtarget_cost_width` (optimizer/path/costsize.c) -- cost estimation.
//!     - `remove_nulling_relids` (rewrite/rewriteManip.c) -- grouping-boundary nulling.
//!   All three are `unimplemented!()` with a TODO; everything else is REAL logic.

use crate::prelude::*;

use crate::nodes::bitmapset::{bms_make_singleton, Bitmapset};
use crate::nodes::equalfuncs::equal;
use crate::nodes::makefuncs::makeTargetEntry;
use crate::nodes::nodeFuncs::{exprCollation, exprType, expression_tree_walker};
use crate::nodes::nodes::Node;
use crate::nodes::parsenodes::SortGroupClause;
use crate::nodes::pathnodes::{
    get_pathtarget_sortgroupref, PathTarget, PlannerInfo, VolatileFunctionStatus,
};
use crate::nodes::pg_list::{
    lappend, lappend_int, lfirst, lfirst_mut, lfirst_oid, list_concat, list_copy, list_head,
    list_length, list_member, list_nth_cell, lnext, List, ListCell,
};
use crate::nodes::primnodes::{AttrNumber, Expr, FuncExpr, OpExpr, TargetEntry, Var};
use crate::{
    current_cell, forboth, foreach, for_each_cell, forthree, list_make1, list_make1_int, makeNode,
    IsA,
};

use core::ffi::c_void;
use core::ptr::null_mut;

// ----------------------------------------------------------------------------
// STUBs for genuinely-unported dependencies (used only by split_pathtarget_*).
// ----------------------------------------------------------------------------

/// `copyObject(node)` (nodes/copyfuncs.c): deep-copy an arbitrary Node.
///
/// TODO(pg-port): nodes/copyfuncs.c not yet translated.
unsafe fn copyObject<T>(_node: *const T) -> *mut T {
    unimplemented!("copyObject: copyfuncs.c not yet translated")
}

/// `set_pathtarget_cost_width(root, target)` (optimizer/path/costsize.c): fill in
/// a PathTarget's cost and width fields.
///
/// TODO(pg-port): optimizer/path/costsize.c not yet translated.
unsafe fn set_pathtarget_cost_width(root: *mut PlannerInfo, target: *mut PathTarget) {
    crate::optimizer::path::costsize::set_pathtarget_cost_width(root, target);
}

/// `remove_nulling_relids(node, removable_relids, except_relids)`
/// (rewrite/rewriteManip.c): strip the given relids from Var/PHV nullingrels.
///
/// TODO(pg-port): rewrite/rewriteManip.c not yet translated.
unsafe fn remove_nulling_relids(
    node: *mut Node,
    removable_relids: *mut Bitmapset,
    except_relids: *mut Bitmapset,
) -> *mut Node {
    crate::rewrite::rewriteManip::remove_nulling_relids(node, removable_relids as _, except_relids as _)
}

// ----------------------------------------------------------------------------
// Data structures for split_pathtarget_at_srfs().
// ----------------------------------------------------------------------------

/// `split_pathtarget_item`: a subexpression of a PathTarget plus its sortgroupref.
#[repr(C)]
struct split_pathtarget_item {
    /// some subexpression of a PathTarget
    expr: *mut Node,
    /// its sortgroupref, or 0 if none
    sortgroupref: Index,
}

/// `split_pathtarget_context`: traversal state for split_pathtarget_at_srfs().
#[repr(C)]
struct split_pathtarget_context {
    root: *mut PlannerInfo,
    /// true if processing grouping target
    is_grouping_target: bool,
    /* This is a List of bare expressions: */
    /// exprs available from input
    input_target_exprs: *mut List,
    /* These are Lists of Lists of split_pathtarget_items: */
    /// SRF exprs to evaluate at each level
    level_srfs: *mut List,
    /// input vars needed at each level
    level_input_vars: *mut List,
    /// input SRFs needed at each level
    level_input_srfs: *mut List,
    /* These are Lists of split_pathtarget_items: */
    /// vars needed in current subexpr
    current_input_vars: *mut List,
    /// SRFs needed in current subexpr
    current_input_srfs: *mut List,
    /* Auxiliary data for current split_pathtarget_walker traversal: */
    /// max SRF depth in current subexpr
    current_depth: c_int,
    /// current subexpr's sortgroupref, or 0
    current_sgref: Index,
}

/// `IS_SRF_CALL(node)` macro: test if a node represents a SRF call.
#[inline]
unsafe fn IS_SRF_CALL(node: *const Node) -> bool {
    (IsA!(node, T_FuncExpr) && (*(node as *const FuncExpr)).funcretset)
        || (IsA!(node, T_OpExpr) && (*(node as *const OpExpr)).opretset)
}

//*****************************************************************************
//      Target list creation and searching utilities
//*****************************************************************************

/// tlist_member
///   Finds the (first) member of the given tlist whose expression is equal()
///   to the given expression.  Result is NULL if no such member.
pub unsafe fn tlist_member(node: *mut Expr, targetlist: *mut List) -> *mut TargetEntry {
    foreach!(temp, targetlist, {
        let tlentry = lfirst(current_cell!(temp)) as *mut TargetEntry;

        if equal(node as *const c_void, (*tlentry).expr as *const c_void) {
            return tlentry;
        }
    });
    null_mut()
}

/// tlist_member_match_var
///   Same as above, except that we match the provided Var on the basis of
///   varno/varattno/varlevelsup/vartype only, rather than full equal().
///
/// This is needed in some cases where we can't be sure of an exact typmod
/// match.  For safety, though, we insist on vartype match.
unsafe fn tlist_member_match_var(var: *mut Var, targetlist: *mut List) -> *mut TargetEntry {
    foreach!(temp, targetlist, {
        let tlentry = lfirst(current_cell!(temp)) as *mut TargetEntry;
        let tlvar = (*tlentry).expr as *mut Var;

        if tlvar.is_null() || !IsA!(tlvar, T_Var) {
            continue;
        }
        if (*var).varno == (*tlvar).varno
            && (*var).varattno == (*tlvar).varattno
            && (*var).varlevelsup == (*tlvar).varlevelsup
            && (*var).vartype == (*tlvar).vartype
        {
            return tlentry;
        }
    });
    null_mut()
}

/// add_to_flat_tlist
///     Add more items to a flattened tlist (if they're not already in it)
///
/// 'tlist' is the flattened tlist
/// 'exprs' is a list of expressions (usually, but not necessarily, Vars)
///
/// Returns the extended tlist.
pub unsafe fn add_to_flat_tlist(mut tlist: *mut List, exprs: *mut List) -> *mut List {
    let mut next_resno: c_int = list_length(tlist) + 1;

    foreach!(lc, exprs, {
        let expr = lfirst(current_cell!(lc)) as *mut Expr;

        if tlist_member(expr, tlist).is_null() {
            let tle = makeTargetEntry(
                copyObject(expr), /* copy needed?? */
                next_resno as AttrNumber,
                null_mut(),
                false,
            );
            next_resno += 1;
            tlist = lappend(tlist, tle as *mut c_void);
        }
    });
    tlist
}

/// get_tlist_exprs
///     Get just the expression subtrees of a tlist
///
/// Resjunk columns are ignored unless includeJunk is true
pub unsafe fn get_tlist_exprs(tlist: *mut List, includeJunk: bool) -> *mut List {
    let mut result: *mut List = null_mut();

    foreach!(l, tlist, {
        let tle = lfirst(current_cell!(l)) as *mut TargetEntry;

        if (*tle).resjunk && !includeJunk {
            continue;
        }

        result = lappend(result, (*tle).expr as *mut c_void);
    });
    result
}

/// count_nonjunk_tlist_entries
///     What it says ...
pub unsafe fn count_nonjunk_tlist_entries(tlist: *mut List) -> c_int {
    let mut len: c_int = 0;

    foreach!(l, tlist, {
        let tle = lfirst(current_cell!(l)) as *mut TargetEntry;

        if !(*tle).resjunk {
            len += 1;
        }
    });
    len
}

/// tlist_same_exprs
///     Check whether two target lists contain the same expressions
///
/// See the C source for the rationale behind ignoring the labeling fields.
pub unsafe fn tlist_same_exprs(tlist1: *mut List, tlist2: *mut List) -> bool {
    if list_length(tlist1) != list_length(tlist2) {
        return false; /* not same length, so can't match */
    }

    forboth!(lc1, tlist1, lc2, tlist2, {
        let tle1 = lfirst(lc1) as *mut TargetEntry;
        let tle2 = lfirst(lc2) as *mut TargetEntry;

        if !equal((*tle1).expr as *const c_void, (*tle2).expr as *const c_void) {
            return false;
        }
    });

    true
}

/// Does tlist have same output datatypes as listed in colTypes?
///
/// Resjunk columns are ignored if junkOK is true; otherwise presence of a
/// resjunk column will always cause a 'false' result.
///
/// Note: currently no callers care about comparing typmods.
pub unsafe fn tlist_same_datatypes(
    tlist: *mut List,
    colTypes: *mut List,
    junkOK: bool,
) -> bool {
    let mut curColType: *mut ListCell = list_head(colTypes);

    foreach!(l, tlist, {
        let tle = lfirst(current_cell!(l)) as *mut TargetEntry;

        if (*tle).resjunk {
            if !junkOK {
                return false;
            }
        } else {
            if curColType.is_null() {
                return false; /* tlist longer than colTypes */
            }
            if exprType((*tle).expr as *const Node) != lfirst_oid(curColType) {
                return false;
            }
            curColType = lnext(colTypes, curColType);
        }
    });
    if !curColType.is_null() {
        return false; /* tlist shorter than colTypes */
    }
    true
}

/// Does tlist have same exposed collations as listed in colCollations?
///
/// Identical logic to the above, but for collations.
pub unsafe fn tlist_same_collations(
    tlist: *mut List,
    colCollations: *mut List,
    junkOK: bool,
) -> bool {
    let mut curColColl: *mut ListCell = list_head(colCollations);

    foreach!(l, tlist, {
        let tle = lfirst(current_cell!(l)) as *mut TargetEntry;

        if (*tle).resjunk {
            if !junkOK {
                return false;
            }
        } else {
            if curColColl.is_null() {
                return false; /* tlist longer than colCollations */
            }
            if exprCollation((*tle).expr as *const Node) != lfirst_oid(curColColl) {
                return false;
            }
            curColColl = lnext(colCollations, curColColl);
        }
    });
    if !curColColl.is_null() {
        return false; /* tlist shorter than colCollations */
    }
    true
}

/// apply_tlist_labeling
///     Apply the TargetEntry labeling attributes of src_tlist to dest_tlist
///
/// This is useful for reattaching column names etc to a plan's final output
/// targetlist.
pub unsafe fn apply_tlist_labeling(dest_tlist: *mut List, src_tlist: *mut List) {
    Assert!(list_length(dest_tlist) == list_length(src_tlist));
    forboth!(ld, dest_tlist, ls, src_tlist, {
        let dest_tle = lfirst(ld) as *mut TargetEntry;
        let src_tle = lfirst(ls) as *mut TargetEntry;

        Assert!((*dest_tle).resno == (*src_tle).resno);
        (*dest_tle).resname = (*src_tle).resname;
        (*dest_tle).ressortgroupref = (*src_tle).ressortgroupref;
        (*dest_tle).resorigtbl = (*src_tle).resorigtbl;
        (*dest_tle).resorigcol = (*src_tle).resorigcol;
        (*dest_tle).resjunk = (*src_tle).resjunk;
    });
}

/// get_sortgroupref_tle
///     Find the targetlist entry matching the given SortGroupRef index, and
///     return it.
pub unsafe fn get_sortgroupref_tle(sortref: Index, targetList: *mut List) -> *mut TargetEntry {
    foreach!(l, targetList, {
        let tle = lfirst(current_cell!(l)) as *mut TargetEntry;

        if (*tle).ressortgroupref == sortref {
            return tle;
        }
    });

    elog!(ERROR, "ORDER/GROUP BY expression not found in targetlist");
    #[allow(unreachable_code)]
    null_mut() /* keep compiler quiet */
}

/// get_sortgroupclause_tle
///     Find the targetlist entry matching the given SortGroupClause by
///     ressortgroupref, and return it.
pub unsafe fn get_sortgroupclause_tle(
    sgClause: *mut SortGroupClause,
    targetList: *mut List,
) -> *mut TargetEntry {
    get_sortgroupref_tle((*sgClause).tleSortGroupRef, targetList)
}

/// get_sortgroupclause_expr
///     Find the targetlist entry matching the given SortGroupClause by
///     ressortgroupref, and return its expression.
pub unsafe fn get_sortgroupclause_expr(
    sgClause: *mut SortGroupClause,
    targetList: *mut List,
) -> *mut Node {
    let tle = get_sortgroupclause_tle(sgClause, targetList);

    (*tle).expr as *mut Node
}

/// get_sortgrouplist_exprs
///     Given a list of SortGroupClauses, build a list of the referenced
///     targetlist expressions.
pub unsafe fn get_sortgrouplist_exprs(
    sgClauses: *mut List,
    targetList: *mut List,
) -> *mut List {
    let mut result: *mut List = null_mut();

    foreach!(l, sgClauses, {
        let sortcl = lfirst(current_cell!(l)) as *mut SortGroupClause;

        let sortexpr = get_sortgroupclause_expr(sortcl, targetList);
        result = lappend(result, sortexpr as *mut c_void);
    });
    result
}

//*****************************************************************************
//      Functions to extract data from a list of SortGroupClauses
//*****************************************************************************

/// get_sortgroupref_clause
///     Find the SortGroupClause matching the given SortGroupRef index, and
///     return it.
pub unsafe fn get_sortgroupref_clause(
    sortref: Index,
    clauses: *mut List,
) -> *mut SortGroupClause {
    foreach!(l, clauses, {
        let cl = lfirst(current_cell!(l)) as *mut SortGroupClause;

        if (*cl).tleSortGroupRef == sortref {
            return cl;
        }
    });

    elog!(ERROR, "ORDER/GROUP BY expression not found in list");
    #[allow(unreachable_code)]
    null_mut() /* keep compiler quiet */
}

/// get_sortgroupref_clause_noerr
///     As above, but return NULL rather than throwing an error if not found.
pub unsafe fn get_sortgroupref_clause_noerr(
    sortref: Index,
    clauses: *mut List,
) -> *mut SortGroupClause {
    foreach!(l, clauses, {
        let cl = lfirst(current_cell!(l)) as *mut SortGroupClause;

        if (*cl).tleSortGroupRef == sortref {
            return cl;
        }
    });

    null_mut()
}

/// extract_grouping_ops - make an array of the equality operator OIDs
///     for a SortGroupClause list
pub unsafe fn extract_grouping_ops(groupClause: *mut List) -> *mut Oid {
    let numCols = list_length(groupClause);
    let mut colno: c_int = 0;
    let groupOperators: *mut Oid;

    groupOperators = palloc(core::mem::size_of::<Oid>() * numCols as usize) as *mut Oid;

    foreach!(glitem, groupClause, {
        let groupcl = lfirst(current_cell!(glitem)) as *mut SortGroupClause;

        *groupOperators.add(colno as usize) = (*groupcl).eqop;
        Assert!(OidIsValid(*groupOperators.add(colno as usize)));
        colno += 1;
    });

    groupOperators
}

/// extract_grouping_collations - make an array of the grouping column collations
///     for a SortGroupClause list
pub unsafe fn extract_grouping_collations(groupClause: *mut List, tlist: *mut List) -> *mut Oid {
    let numCols = list_length(groupClause);
    let mut colno: c_int = 0;
    let grpCollations: *mut Oid;

    grpCollations = palloc(core::mem::size_of::<Oid>() * numCols as usize) as *mut Oid;

    foreach!(glitem, groupClause, {
        let groupcl = lfirst(current_cell!(glitem)) as *mut SortGroupClause;
        let tle = get_sortgroupclause_tle(groupcl, tlist);

        *grpCollations.add(colno as usize) = exprCollation((*tle).expr as *mut Node);
        colno += 1;
    });

    grpCollations
}

/// extract_grouping_cols - make an array of the grouping column resnos
///     for a SortGroupClause list
pub unsafe fn extract_grouping_cols(groupClause: *mut List, tlist: *mut List) -> *mut AttrNumber {
    let grpColIdx: *mut AttrNumber;
    let numCols = list_length(groupClause);
    let mut colno: c_int = 0;

    grpColIdx = palloc(core::mem::size_of::<AttrNumber>() * numCols as usize) as *mut AttrNumber;

    foreach!(glitem, groupClause, {
        let groupcl = lfirst(current_cell!(glitem)) as *mut SortGroupClause;
        let tle = get_sortgroupclause_tle(groupcl, tlist);

        *grpColIdx.add(colno as usize) = (*tle).resno;
        colno += 1;
    });

    grpColIdx
}

/// grouping_is_sortable - is it possible to implement grouping list by sorting?
///
/// This is easy since the parser will have included a sortop if one exists.
pub unsafe fn grouping_is_sortable(groupClause: *mut List) -> bool {
    foreach!(glitem, groupClause, {
        let groupcl = lfirst(current_cell!(glitem)) as *mut SortGroupClause;

        if !OidIsValid((*groupcl).sortop) {
            return false;
        }
    });
    true
}

/// grouping_is_hashable - is it possible to implement grouping list by hashing?
///
/// We rely on the parser to have set the hashable flag correctly.
pub unsafe fn grouping_is_hashable(groupClause: *mut List) -> bool {
    foreach!(glitem, groupClause, {
        let groupcl = lfirst(current_cell!(glitem)) as *mut SortGroupClause;

        if !(*groupcl).hashable {
            return false;
        }
    });
    true
}

//*****************************************************************************
//      PathTarget manipulation functions
//*****************************************************************************

/// create_pathtarget
///   Build a PathTarget from a targetlist, with cost/width computed.
pub unsafe fn create_pathtarget(root: *mut PlannerInfo, tlist: *mut List) -> *mut PathTarget {
    let target = make_pathtarget_from_tlist(tlist);
    crate::optimizer::path::costsize::set_pathtarget_cost_width(root, target);
    target
}

/// make_pathtarget_from_tlist
///   Construct a PathTarget equivalent to the given targetlist.
///
/// This leaves the cost and width fields as zeroes.  Most callers will want to
/// use create_pathtarget(), so as to get those set.
pub unsafe fn make_pathtarget_from_tlist(tlist: *mut List) -> *mut PathTarget {
    let target: *mut PathTarget = makeNode!(PathTarget, T_PathTarget);
    let mut i: c_int;

    (*target).sortgrouprefs =
        palloc(list_length(tlist) as usize * core::mem::size_of::<Index>()) as *mut Index;

    i = 0;
    foreach!(lc, tlist, {
        let tle = lfirst(current_cell!(lc)) as *mut TargetEntry;

        (*target).exprs = lappend((*target).exprs, (*tle).expr as *mut c_void);
        *(*target).sortgrouprefs.add(i as usize) = (*tle).ressortgroupref;
        i += 1;
    });

    /*
     * Mark volatility as unknown.  The contain_volatile_functions function will
     * determine if there are any volatile functions when called for the first
     * time with this PathTarget.
     */
    (*target).has_volatile_expr = VolatileFunctionStatus::VOLATILITY_UNKNOWN;

    target
}

/// make_tlist_from_pathtarget
///   Construct a targetlist from a PathTarget.
pub unsafe fn make_tlist_from_pathtarget(target: *mut PathTarget) -> *mut List {
    let mut tlist: *mut List = null_mut();
    let mut i: c_int;

    i = 0;
    foreach!(lc, (*target).exprs, {
        let expr = lfirst(current_cell!(lc)) as *mut Expr;

        let tle = makeTargetEntry(expr, (i + 1) as AttrNumber, null_mut(), false);
        if !(*target).sortgrouprefs.is_null() {
            (*tle).ressortgroupref = *(*target).sortgrouprefs.add(i as usize);
        }
        tlist = lappend(tlist, tle as *mut c_void);
        i += 1;
    });

    tlist
}

/// copy_pathtarget
///   Copy a PathTarget.
///
/// The new PathTarget has its own exprs List, but shares the underlying target
/// expression trees with the old one.
pub unsafe fn copy_pathtarget(src: *mut PathTarget) -> *mut PathTarget {
    let dst: *mut PathTarget = makeNode!(PathTarget, T_PathTarget);

    /* Copy scalar fields (C: memcpy(dst, src, sizeof(PathTarget))) */
    core::ptr::copy_nonoverlapping(src, dst, 1);
    /* Shallow-copy the expression list */
    (*dst).exprs = list_copy((*src).exprs);
    /* Duplicate sortgrouprefs if any (if not, the memcpy handled this) */
    if !(*src).sortgrouprefs.is_null() {
        let nbytes = list_length((*src).exprs) as usize * core::mem::size_of::<Index>();

        (*dst).sortgrouprefs = palloc(nbytes) as *mut Index;
        core::ptr::copy_nonoverlapping((*src).sortgrouprefs, (*dst).sortgrouprefs, list_length((*src).exprs) as usize);
    }
    dst
}

/// create_empty_pathtarget
///   Create an empty (zero columns, zero cost) PathTarget.
pub unsafe fn create_empty_pathtarget() -> *mut PathTarget {
    /* This is easy, but we don't want callers to hard-wire this ... */
    makeNode!(PathTarget, T_PathTarget)
}

/// add_column_to_pathtarget
///     Append a target column to the PathTarget.
///
/// As with make_pathtarget_from_tlist, we leave it to the caller to update the
/// cost and width fields.
pub unsafe fn add_column_to_pathtarget(
    target: *mut PathTarget,
    expr: *mut Expr,
    sortgroupref: Index,
) {
    /* Updating the exprs list is easy ... */
    (*target).exprs = lappend((*target).exprs, expr as *mut c_void);
    /* ... the sortgroupref data, a bit less so */
    if !(*target).sortgrouprefs.is_null() {
        let nexprs = list_length((*target).exprs);

        /* This might look inefficient, but actually it's usually cheap */
        (*target).sortgrouprefs = repalloc(
            (*target).sortgrouprefs as *mut c_void,
            nexprs as usize * core::mem::size_of::<Index>(),
        ) as *mut Index;
        *(*target).sortgrouprefs.add((nexprs - 1) as usize) = sortgroupref;
    } else if sortgroupref != 0 {
        /* Adding sortgroupref labeling to a previously unlabeled target */
        let nexprs = list_length((*target).exprs);

        (*target).sortgrouprefs =
            palloc0(nexprs as usize * core::mem::size_of::<Index>()) as *mut Index;
        *(*target).sortgrouprefs.add((nexprs - 1) as usize) = sortgroupref;
    }

    /*
     * Reset has_volatile_expr to UNKNOWN.  We just leave it up to
     * contain_volatile_functions to set this properly again.
     */
    if (*target).has_volatile_expr == VolatileFunctionStatus::VOLATILITY_NOVOLATILE {
        (*target).has_volatile_expr = VolatileFunctionStatus::VOLATILITY_UNKNOWN;
    }
}

/// add_new_column_to_pathtarget
///     Append a target column to the PathTarget, but only if it's not equal()
///     to any pre-existing target expression.
///
/// The caller cannot specify a sortgroupref, since it would be unclear how to
/// merge that with a pre-existing column.
pub unsafe fn add_new_column_to_pathtarget(target: *mut PathTarget, expr: *mut Expr) {
    if !list_member((*target).exprs, expr as *const c_void) {
        add_column_to_pathtarget(target, expr, 0);
    }
}

/// add_new_columns_to_pathtarget
///     Apply add_new_column_to_pathtarget() for each element of the list.
pub unsafe fn add_new_columns_to_pathtarget(target: *mut PathTarget, exprs: *mut List) {
    foreach!(lc, exprs, {
        let expr = lfirst(current_cell!(lc)) as *mut Expr;

        add_new_column_to_pathtarget(target, expr);
    });
}

/// apply_pathtarget_labeling_to_tlist
///     Apply any sortgrouprefs in the PathTarget to matching tlist entries
///
/// Here, we do not assume that the tlist entries are one-for-one with the
/// PathTarget.  The intended use is to deal with cases where createplan.c has
/// decided to use some other tlist and we have to identify what matches exist.
pub unsafe fn apply_pathtarget_labeling_to_tlist(tlist: *mut List, target: *mut PathTarget) {
    let mut i: c_int;

    /* Nothing to do if PathTarget has no sortgrouprefs data */
    if (*target).sortgrouprefs.is_null() {
        return;
    }

    i = 0;
    foreach!(lc, (*target).exprs, {
        let expr = lfirst(current_cell!(lc)) as *mut Expr;
        let tle: *mut TargetEntry;

        if *(*target).sortgrouprefs.add(i as usize) != 0 {
            /*
             * For Vars, use tlist_member_match_var's weakened matching rule;
             * this allows us to deal with some cases where a set-returning
             * function has been inlined.  Otherwise, use regular equal().
             */
            if !expr.is_null() && IsA!(expr, T_Var) {
                tle = tlist_member_match_var(expr as *mut Var, tlist);
            } else {
                tle = tlist_member(expr, tlist);
            }

            /*
             * Complain if noplace for the sortgrouprefs label, or if we'd have
             * to label a column twice.
             */
            if tle.is_null() {
                elog!(ERROR, "ORDER/GROUP BY expression not found in targetlist");
            }
            if (*tle).ressortgroupref != 0
                && (*tle).ressortgroupref != *(*target).sortgrouprefs.add(i as usize)
            {
                elog!(ERROR, "targetlist item has multiple sortgroupref labels");
            }

            (*tle).ressortgroupref = *(*target).sortgrouprefs.add(i as usize);
        }
        i += 1;
    });
}

/// split_pathtarget_at_srfs
///     Split given PathTarget into multiple levels to position SRFs safely,
///     performing exact matching against input_target.
///
/// This is a wrapper for split_pathtarget_at_srfs_extended() used when both
/// targets are on the same side of the grouping boundary.
pub unsafe fn split_pathtarget_at_srfs(
    root: *mut PlannerInfo,
    target: *mut PathTarget,
    input_target: *mut PathTarget,
    targets: *mut *mut List,
    targets_contain_srfs: *mut *mut List,
) {
    split_pathtarget_at_srfs_extended(
        root,
        target,
        input_target,
        targets,
        targets_contain_srfs,
        false,
    );
}

/// split_pathtarget_at_srfs_grouping
///     Split given PathTarget into multiple levels to position SRFs safely,
///     ignoring the grouping nulling bit when matching against input_target.
///
/// Used when the targets cross the grouping boundary.
pub unsafe fn split_pathtarget_at_srfs_grouping(
    root: *mut PlannerInfo,
    target: *mut PathTarget,
    input_target: *mut PathTarget,
    targets: *mut *mut List,
    targets_contain_srfs: *mut *mut List,
) {
    split_pathtarget_at_srfs_extended(
        root,
        target,
        input_target,
        targets,
        targets_contain_srfs,
        true,
    );
}

/// split_pathtarget_at_srfs_extended
///     Split given PathTarget into multiple levels to position SRFs safely.
///
/// See the extensive comment in the C source for semantics.  The outputs are
/// two parallel lists: a list of PathTargets and an integer list of bool flags.
unsafe fn split_pathtarget_at_srfs_extended(
    root: *mut PlannerInfo,
    target: *mut PathTarget,
    input_target: *mut PathTarget,
    targets: *mut *mut List,
    targets_contain_srfs: *mut *mut List,
    is_grouping_target: bool,
) {
    let mut context: split_pathtarget_context = core::mem::zeroed();
    let mut max_depth: c_int;
    let mut need_extra_projection: bool;
    let mut prev_level_tlist: *mut List;
    let mut lci: c_int;

    /*
     * It's not unusual for planner.c to pass us two physically identical
     * targets, in which case we can conclude all expressions are available
     * from the input.
     */
    if target == input_target {
        *targets = list_make1!(target as *mut c_void);
        *targets_contain_srfs = list_make1_int!(false as c_int);
        return;
    }

    /*
     * Pass 'root', the is_grouping_target flag, and any input_target exprs down
     * to split_pathtarget_walker().
     */
    context.root = root;
    context.is_grouping_target = is_grouping_target;
    context.input_target_exprs = if !input_target.is_null() {
        (*input_target).exprs
    } else {
        null_mut()
    };

    /*
     * Initialize with empty level-zero lists, and no levels after that.
     */
    context.level_srfs = list_make1!(null_mut::<c_void>());
    context.level_input_vars = list_make1!(null_mut::<c_void>());
    context.level_input_srfs = list_make1!(null_mut::<c_void>());

    /* Initialize data we'll accumulate across all the target expressions */
    context.current_input_vars = null_mut();
    context.current_input_srfs = null_mut();
    max_depth = 0;
    need_extra_projection = false;

    /* Scan each expression in the PathTarget looking for SRFs */
    lci = 0;
    foreach!(lc, (*target).exprs, {
        let node = lfirst(current_cell!(lc)) as *mut Node;

        /* Tell split_pathtarget_walker about this expr's sortgroupref */
        context.current_sgref = get_pathtarget_sortgroupref(target, lci);
        lci += 1;

        /*
         * Find all SRFs and Vars (and Var-like nodes) in this expression, and
         * enter them into appropriate lists within the context struct.
         */
        context.current_depth = 0;
        split_pathtarget_walker(node, &mut context);

        /* An expression containing no SRFs is of no further interest */
        if context.current_depth == 0 {
            continue;
        }

        /*
         * Track max SRF nesting depth over the whole PathTarget.
         */
        if max_depth < context.current_depth {
            max_depth = context.current_depth;
            need_extra_projection = false;
        }

        /*
         * If any maximum-depth SRF is not at the top level of its expression,
         * we'll need an extra Result node.
         */
        if max_depth == context.current_depth && !IS_SRF_CALL(node) {
            need_extra_projection = true;
        }
    });

    /*
     * If we found no SRFs needing evaluation, then no ProjectSet is needed.
     */
    if max_depth == 0 {
        *targets = list_make1!(target as *mut c_void);
        *targets_contain_srfs = list_make1_int!(false as c_int);
        return;
    }

    /*
     * The Vars and SRF outputs needed at top level can be added to the last
     * level_input lists if we don't need an extra projection step.  If we do
     * need one, add a SRF-free level to the lists.
     */
    if need_extra_projection {
        context.level_srfs = lappend(context.level_srfs, null_mut());
        context.level_input_vars =
            lappend(context.level_input_vars, context.current_input_vars as *mut c_void);
        context.level_input_srfs =
            lappend(context.level_input_srfs, context.current_input_srfs as *mut c_void);
    } else {
        let lc = list_nth_cell(context.level_input_vars, max_depth);
        *lfirst_mut(lc) =
            list_concat(lfirst(lc) as *mut List, context.current_input_vars) as *mut c_void;
        let lc = list_nth_cell(context.level_input_srfs, max_depth);
        *lfirst_mut(lc) =
            list_concat(lfirst(lc) as *mut List, context.current_input_srfs) as *mut c_void;
    }

    /*
     * Now construct the output PathTargets.  The original target can be used
     * as-is for the last one; we construct new SRF-free targets for the rest.
     */
    *targets = null_mut();
    *targets_contain_srfs = null_mut();
    prev_level_tlist = null_mut();

    forthree!(
        lc1, context.level_srfs,
        lc2, context.level_input_vars,
        lc3, context.level_input_srfs,
        {
            let level_srfs = lfirst(lc1) as *mut List;
            let ntarget: *mut PathTarget;

            if lnext(context.level_srfs, lc1).is_null() {
                ntarget = target;
            } else {
                ntarget = create_empty_pathtarget();

                /*
                 * This target should evaluate any SRFs of the current level,
                 * and propagate forward any Vars needed by later levels, as
                 * well as SRFs computed earlier and needed by later levels.
                 */
                add_sp_items_to_pathtarget(ntarget, level_srfs);
                for_each_cell!(lc, context.level_input_vars, lnext(context.level_input_vars, lc2), {
                    let input_vars = lfirst(current_cell!(lc)) as *mut List;

                    add_sp_items_to_pathtarget(ntarget, input_vars);
                });
                for_each_cell!(lc, context.level_input_srfs, lnext(context.level_input_srfs, lc3), {
                    let input_srfs = lfirst(current_cell!(lc)) as *mut List;

                    foreach!(lcx, input_srfs, {
                        let item = lfirst(current_cell!(lcx)) as *mut split_pathtarget_item;

                        if list_member(prev_level_tlist, (*item).expr as *const c_void) {
                            add_sp_item_to_pathtarget(ntarget, item);
                        }
                    });
                });
                set_pathtarget_cost_width(root, ntarget);
            }

            /*
             * Add current target and does-it-compute-SRFs flag to output lists.
             */
            *targets = lappend(*targets, ntarget as *mut c_void);
            *targets_contain_srfs =
                lappend_int(*targets_contain_srfs, (!level_srfs.is_null()) as c_int);

            /* Remember this level's output for next pass */
            prev_level_tlist = (*ntarget).exprs;
        }
    );
}

/// Recursively examine expressions for split_pathtarget_at_srfs.
///
/// Note we make no effort here to prevent duplicate entries in the output lists.
/// Duplicates will be gotten rid of later.
unsafe fn split_pathtarget_walker(
    node: *mut Node,
    context: *mut split_pathtarget_context,
) -> bool {
    let mut sanitized_node: *mut Node = node;

    if node.is_null() {
        return false;
    }

    /*
     * If we are crossing the grouping boundary, we must ignore the grouping
     * nulling bit to correctly check if the subexpression is available in
     * input_target.
     */
    if (*context).is_grouping_target
        && (*(*(*context).root).parse).hasGroupRTE
        && !(*(*(*context).root).parse).groupingSets.is_null()
    {
        sanitized_node = remove_nulling_relids(
            node,
            bms_make_singleton((*(*context).root).group_rtindex),
            null_mut(),
        );
    }

    /*
     * A subexpression that matches an expression already computed in
     * input_target can be treated like a Var, even if it's actually a SRF.
     */
    if list_member((*context).input_target_exprs, sanitized_node as *const c_void) {
        let item = palloc(core::mem::size_of::<split_pathtarget_item>())
            as *mut split_pathtarget_item;

        (*item).expr = node;
        (*item).sortgroupref = (*context).current_sgref;
        (*context).current_input_vars =
            lappend((*context).current_input_vars, item as *mut c_void);
        return false;
    }

    /*
     * Vars and Var-like constructs are expected to be gotten from the input,
     * too.  We assume these cannot contain any SRFs.
     */
    if IsA!(node, T_Var)
        || IsA!(node, T_PlaceHolderVar)
        || IsA!(node, T_Aggref)
        || IsA!(node, T_GroupingFunc)
        || IsA!(node, T_WindowFunc)
    {
        let item = palloc(core::mem::size_of::<split_pathtarget_item>())
            as *mut split_pathtarget_item;

        (*item).expr = node;
        (*item).sortgroupref = (*context).current_sgref;
        (*context).current_input_vars =
            lappend((*context).current_input_vars, item as *mut c_void);
        return false;
    }

    /*
     * If it's a SRF, recursively examine its inputs, determine its level, and
     * make appropriate entries in the output lists.
     */
    if IS_SRF_CALL(node) {
        let item = palloc(core::mem::size_of::<split_pathtarget_item>())
            as *mut split_pathtarget_item;
        let save_input_vars = (*context).current_input_vars;
        let save_input_srfs = (*context).current_input_srfs;
        let save_current_depth = (*context).current_depth;
        let srf_depth: c_int;

        (*item).expr = node;
        (*item).sortgroupref = (*context).current_sgref;

        (*context).current_input_vars = null_mut();
        (*context).current_input_srfs = null_mut();
        (*context).current_depth = 0;
        (*context).current_sgref = 0; /* subexpressions are not sortgroup items */

        expression_tree_walker(
            node,
            Some(split_pathtarget_walker_trampoline),
            context as *mut c_void,
        );

        /* Depth is one more than any SRF below it */
        srf_depth = (*context).current_depth + 1;

        /* If new record depth, initialize another level of output lists */
        if srf_depth >= list_length((*context).level_srfs) {
            (*context).level_srfs = lappend((*context).level_srfs, null_mut());
            (*context).level_input_vars = lappend((*context).level_input_vars, null_mut());
            (*context).level_input_srfs = lappend((*context).level_input_srfs, null_mut());
        }

        /* Record this SRF as needing to be evaluated at appropriate level */
        let lc = list_nth_cell((*context).level_srfs, srf_depth);
        *lfirst_mut(lc) = lappend(lfirst(lc) as *mut List, item as *mut c_void) as *mut c_void;

        /* Record its inputs as being needed at the same level */
        let lc = list_nth_cell((*context).level_input_vars, srf_depth);
        *lfirst_mut(lc) =
            list_concat(lfirst(lc) as *mut List, (*context).current_input_vars) as *mut c_void;
        let lc = list_nth_cell((*context).level_input_srfs, srf_depth);
        *lfirst_mut(lc) =
            list_concat(lfirst(lc) as *mut List, (*context).current_input_srfs) as *mut c_void;

        /*
         * Restore caller-level state and update it for presence of this SRF.
         */
        (*context).current_input_vars = save_input_vars;
        (*context).current_input_srfs = lappend(save_input_srfs, item as *mut c_void);
        (*context).current_depth = Max(save_current_depth, srf_depth);

        /* We're done here */
        return false;
    }

    /*
     * Otherwise, the node is a scalar (non-set) expression, so recurse.
     */
    (*context).current_sgref = 0; /* subexpressions are not sortgroup items */
    expression_tree_walker(
        node,
        Some(split_pathtarget_walker_trampoline),
        context as *mut c_void,
    )
}

/// Trampoline adapting `split_pathtarget_walker` to the
/// `expression_tree_walker` callback signature `fn(*mut Node, *mut c_void) -> bool`.
unsafe fn split_pathtarget_walker_trampoline(node: *mut Node, context: *mut c_void) -> bool {
    split_pathtarget_walker(node, context as *mut split_pathtarget_context)
}

/// Add a split_pathtarget_item to the PathTarget, unless a matching item is
/// already present.  Like add_new_column_to_pathtarget, but allows for
/// sortgrouprefs.  An item with zero sortgroupref can be merged with one that
/// has a sortgroupref, acquiring the latter's sortgroupref.
unsafe fn add_sp_item_to_pathtarget(target: *mut PathTarget, item: *mut split_pathtarget_item) {
    let mut lci: c_int;

    /*
     * Look for a pre-existing entry that is equal() and does not have a
     * conflicting sortgroupref already.
     */
    lci = 0;
    foreach!(lc, (*target).exprs, {
        let node = lfirst(current_cell!(lc)) as *mut Node;
        let sgref = get_pathtarget_sortgroupref(target, lci);

        if ((*item).sortgroupref == sgref || (*item).sortgroupref == 0 || sgref == 0)
            && equal((*item).expr as *const c_void, node as *const c_void)
        {
            /* Found a match.  Assign item's sortgroupref if it has one. */
            if (*item).sortgroupref != 0 {
                if (*target).sortgrouprefs.is_null() {
                    (*target).sortgrouprefs = palloc0(
                        list_length((*target).exprs) as usize * core::mem::size_of::<Index>(),
                    ) as *mut Index;
                }
                *(*target).sortgrouprefs.add(lci as usize) = (*item).sortgroupref;
            }
            return;
        }
        lci += 1;
    });

    /*
     * No match, so add item to PathTarget.  Copy the expr for safety.
     */
    add_column_to_pathtarget(
        target,
        copyObject((*item).expr) as *mut Expr,
        (*item).sortgroupref,
    );
}

/// Apply add_sp_item_to_pathtarget to each element of list.
unsafe fn add_sp_items_to_pathtarget(target: *mut PathTarget, items: *mut List) {
    foreach!(lc, items, {
        let item = lfirst(current_cell!(lc)) as *mut split_pathtarget_item;

        add_sp_item_to_pathtarget(target, item);
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nodes::nodes::NodeTag;

    // Build a bare Const node we can use as a distinct, equal()-able expr.
    // We hand-build minimal TargetEntry lists and verify the searching helpers.

    unsafe fn make_const(val: i64) -> *mut Expr {
        use crate::nodes::primnodes::Const;
        let c = palloc0(core::mem::size_of::<Const>()) as *mut Const;
        (*(c as *mut Node)).r#type = NodeTag::T_Const;
        // Use constvalue as a discriminator; equal() on Const compares all fields.
        (*c).consttype = 23; // int4
        (*c).consttypmod = -1;
        (*c).constcollid = 0;
        (*c).constlen = 8;
        (*c).constvalue = val as Datum;
        (*c).constisnull = false;
        (*c).constbyval = true;
        (*c).location = -1;
        c as *mut Expr
    }

    unsafe fn make_tle(expr: *mut Expr, resno: i16, resjunk: bool) -> *mut TargetEntry {
        let tle = makeTargetEntry(expr, resno, null_mut(), resjunk);
        tle
    }

    // tlist_member relies on equal() (equalfuncs.c), which is still an
    // unimplemented!() stub, so a runtime match test can't pass yet.  Ignored
    // until equalfuncs is ported; the structural list build is exercised by
    // test_count_nonjunk_tlist_entries below.
    #[test]
    #[ignore = "tlist_member needs equal() from equalfuncs.c (still stubbed)"]
    fn test_tlist_member_finds_match() {
        unsafe {
            let c1 = make_const(10);
            let c2 = make_const(20);
            let c3 = make_const(30);

            let mut tlist: *mut List = null_mut();
            tlist = lappend(tlist, make_tle(c1, 1, false) as *mut c_void);
            tlist = lappend(tlist, make_tle(c2, 2, false) as *mut c_void);
            tlist = lappend(tlist, make_tle(c3, 3, false) as *mut c_void);

            let probe = make_const(20);
            let found = tlist_member(probe, tlist);
            assert!(!found.is_null(), "expected to find matching entry");
            assert_eq!((*found).resno, 2);

            let miss = make_const(999);
            assert!(tlist_member(miss, tlist).is_null());
        }
    }

    #[test]
    fn test_count_nonjunk_tlist_entries() {
        unsafe {
            let mut tlist: *mut List = null_mut();
            assert_eq!(count_nonjunk_tlist_entries(tlist), 0);

            tlist = lappend(tlist, make_tle(make_const(1), 1, false) as *mut c_void);
            tlist = lappend(tlist, make_tle(make_const(2), 2, true) as *mut c_void); // junk
            tlist = lappend(tlist, make_tle(make_const(3), 3, false) as *mut c_void);
            tlist = lappend(tlist, make_tle(make_const(4), 4, true) as *mut c_void); // junk

            assert_eq!(count_nonjunk_tlist_entries(tlist), 2);
        }
    }
}
