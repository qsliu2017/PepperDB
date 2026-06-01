//! pathkeys.rs
//!   Utilities for matching and building path keys
//!
//! Translated 1:1 from postgres/src/backend/optimizer/path/pathkeys.c
//!
//! See src/backend/optimizer/README for a great deal of information about
//! the nature and use of path keys.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!   src/backend/optimizer/path/pathkeys.c
//!
//! #include mapping:
//!   "postgres.h"                  -> crate::prelude::*
//!   "access/stratnum.h"           -> COMPARE_* live in crate::access::cmptype
//!   "catalog/pg_opfamily.h"       -> IsBuiltinBooleanOpfamily (pg_opfamily.h not yet
//!                                    ported -> local STUB below)
//!   "nodes/nodeFuncs.h"           -> exprCollation (crate::nodes::nodeFuncs);
//!                                    is_notclause/get_notclausearg local helpers
//!                                    reproduced here (matches clausesel.rs)
//!   "optimizer/cost.h"            -> enable_incremental_sort (crate::optimizer::cost);
//!                                    compare_path_costs / compare_fractional_path_costs
//!                                    (costsize.c not yet ported -> local STUBs)
//!   "optimizer/optimizer.h"       -> get_sortgroupref_clause_noerr /
//!                                    get_sortgroupref_tle / get_sortgroupclause_expr
//!                                    (crate::optimizer::util::tlist)
//!   "optimizer/pathnode.h"        -> (nothing referenced directly)
//!   "optimizer/paths.h"           -> make_canonical_pathkey/get_eclass_for_sort_expr/
//!                                    canonicalize_ec_expression/eclass_useful_for_merging/
//!                                    indexcol_is_bool_constant_for_query
//!                                    (crate::optimizer::paths; equivclass.c / indxpath.c
//!                                    not yet ported -> STUBs there)
//!   "partitioning/partbounds.h"   -> partitions_are_ordered (partbounds.c not yet
//!                                    ported -> local STUB below)
//!   "rewrite/rewriteManip.h"      -> remove_nulling_relids (crate::rewrite::rewriteManip)
//!   "utils/lsyscache.h"           -> get_opfamily_member_for_cmptype /
//!                                    get_mergejoin_opfamilies / get_ordering_op_properties /
//!                                    op_input_types (lsyscache.c not yet ported -> STUBs)

use crate::prelude::*;
use core::ffi::c_void;

use crate::{foreach, current_cell, forboth, foreach_delete_current, for_each_from, foreach_node};
use crate::{makeNode, IsA, list_make1};

use crate::access::cmptype::{CompareType, COMPARE_EQ, COMPARE_GT, COMPARE_LT};
use crate::access::sdir::{ScanDirection, ScanDirectionIsBackward};

use crate::nodes::bitmapset::{bms_is_empty, bms_is_subset, bms_make_singleton, bms_overlap, Bitmapset};
use crate::nodes::equalfuncs::equal;
use crate::nodes::nodeFuncs::exprCollation;
use crate::nodes::nodes::{JoinType, Node};
use crate::nodes::nodes::JoinType::{JOIN_FULL, JOIN_RIGHT, JOIN_RIGHT_ANTI, JOIN_RIGHT_SEMI};
use crate::nodes::parsenodes::{Query, SortGroupClause};
use crate::nodes::pathnodes::{
    CostSelector, EquivalenceClass, EquivalenceMember, GroupByOrdering, IndexOptInfo,
    PartitionScheme, Path, PathKey, PlannerInfo, RelOptInfo, Relids, RestrictInfo, EC_MUST_BE_REDUNDANT,
    IS_SIMPLE_REL, PATH_REQ_OUTER,
};
use crate::nodes::pg_list::{
    lappend, lfirst, linitial, linitial_oid, list_concat, list_concat_unique_ptr, list_copy,
    list_copy_head, list_difference, list_difference_ptr, list_free, list_head, list_length,
    list_member_ptr, list_nth, lnext, List, ListCell, NIL,
};
use crate::nodes::primnodes::{BoolExpr, Expr, OpExpr, TargetEntry, Var, NOT_EXPR};

use crate::optimizer::cost::enable_incremental_sort;
use crate::optimizer::paths::{
    canonicalize_ec_expression, eclass_useful_for_merging, get_eclass_for_sort_expr,
    indexcol_is_bool_constant_for_query, PathKeysComparison, PATHKEYS_BETTER1,
    PATHKEYS_BETTER2, PATHKEYS_DIFFERENT, PATHKEYS_EQUAL,
};
use crate::optimizer::util::tlist::{
    get_sortgroupclause_expr, get_sortgroupref_clause_noerr, get_sortgroupref_tle,
};
use crate::rewrite::rewriteManip::remove_nulling_relids;

/* ----------------------------------------------------------------------------
 * Local nodeFuncs.h helpers (nodeFuncs.c not yet ported as a unit; these match
 * the private copies in clausesel.rs / restrictinfo.rs).
 * ------------------------------------------------------------------------- */

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

/// `copyObject(node)` (nodes/copyfuncs.c): deep-copy an arbitrary Node.
///
/// TODO(pg-port): replace with the real recursive copyObject once copyfuncs.c
/// is fully ported (crate::nodes::copyfuncs::copyObjectImpl).
unsafe fn copyObject<T>(node: *const T) -> *mut T {
    // TODO(pg-port): shallow stub until copyfuncs.c is wired.
    node as *mut T
}

/* ----------------------------------------------------------------------------
 * lsyscache.h STUBs (utils/cache/lsyscache.c not yet ported).
 * ------------------------------------------------------------------------- */

// TODO(pg-port): real get_opfamily_member_for_cmptype lives in utils/cache/lsyscache.rs
unsafe fn get_opfamily_member_for_cmptype(
    _opfamily: Oid,
    _lefttype: Oid,
    _righttype: Oid,
    _cmptype: CompareType,
) -> Oid {
    unimplemented!()
}

// TODO(pg-port): real get_mergejoin_opfamilies lives in utils/cache/lsyscache.rs
unsafe fn get_mergejoin_opfamilies(_opno: Oid) -> *mut List {
    unimplemented!()
}

// TODO(pg-port): real get_ordering_op_properties lives in utils/cache/lsyscache.rs
unsafe fn get_ordering_op_properties(
    _opno: Oid,
    _opfamily: *mut Oid,
    _opcintype: *mut Oid,
    _cmptype: *mut CompareType,
) -> bool {
    unimplemented!()
}

// TODO(pg-port): real op_input_types lives in utils/cache/lsyscache.rs
unsafe fn op_input_types(_opno: Oid, _lefttype: *mut Oid, _righttype: *mut Oid) {
    unimplemented!()
}

// TODO(pg-port): real IsBuiltinBooleanOpfamily lives in catalog/pg_opfamily.rs (pg_opfamily.h)
unsafe fn IsBuiltinBooleanOpfamily(_opfamily: Oid) -> bool {
    unimplemented!()
}

// TODO(pg-port): real partitions_are_ordered lives in partitioning/partbounds.rs
unsafe fn partitions_are_ordered(_boundinfo: *mut c_void, _live_parts: *mut Bitmapset) -> bool {
    unimplemented!()
}

// TODO(pg-port): real compare_path_costs lives in optimizer/path/costsize.rs (cost.h)
unsafe fn compare_path_costs(_path1: *mut Path, _path2: *mut Path, _criterion: CostSelector) -> c_int {
    unimplemented!()
}

// TODO(pg-port): real compare_fractional_path_costs lives in optimizer/path/costsize.rs (cost.h)
unsafe fn compare_fractional_path_costs(
    _path1: *mut Path,
    _path2: *mut Path,
    _fraction: f64,
) -> c_int {
    unimplemented!()
}

/* Consider reordering of GROUP BY keys? */
pub static mut enable_group_by_reordering: bool = true;

/****************************************************************************
 *		PATHKEY CONSTRUCTION AND REDUNDANCY TESTING
 ****************************************************************************/

/*
 * make_canonical_pathkey
 *	  Given the parameters for a PathKey, find any pre-existing matching
 *	  pathkey in the query's list of "canonical" pathkeys.  Make a new
 *	  entry if there's not one already.
 *
 * Note that this function must not be used until after we have completed
 * merging EquivalenceClasses.
 */
pub unsafe fn make_canonical_pathkey(
    root: *mut PlannerInfo,
    mut eclass: *mut EquivalenceClass,
    opfamily: Oid,
    cmptype: CompareType,
    nulls_first: bool,
) -> *mut PathKey {
    let mut pk: *mut PathKey;
    let oldcontext: MemoryContext;

    /* Can't make canonical pathkeys if the set of ECs might still change */
    if !(*root).ec_merging_done {
        elog!(ERROR, "too soon to build canonical pathkeys");
    }

    /* The passed eclass might be non-canonical, so chase up to the top */
    while !(*eclass).ec_merged.is_null() {
        eclass = (*eclass).ec_merged;
    }

    foreach!(lc, (*root).canon_pathkeys, {
        pk = lfirst(current_cell!(lc)) as *mut PathKey;
        if eclass == (*pk).pk_eclass
            && opfamily == (*pk).pk_opfamily
            && cmptype == (*pk).pk_cmptype
            && nulls_first == (*pk).pk_nulls_first
        {
            return pk;
        }
    });

    /*
     * Be sure canonical pathkeys are allocated in the main planning context.
     * Not an issue in normal planning, but it is for GEQO.
     */
    oldcontext = MemoryContextSwitchTo((*root).planner_cxt as crate::utils::palloc::MemoryContext);

    pk = makeNode!(PathKey, T_PathKey);
    (*pk).pk_eclass = eclass;
    (*pk).pk_opfamily = opfamily;
    (*pk).pk_cmptype = cmptype;
    (*pk).pk_nulls_first = nulls_first;

    (*root).canon_pathkeys = lappend((*root).canon_pathkeys, pk as *mut c_void);

    MemoryContextSwitchTo(oldcontext);

    pk
}

/*
 * append_pathkeys
 *		Append all non-redundant PathKeys in 'source' onto 'target' and
 *		returns the updated 'target' list.
 */
pub unsafe fn append_pathkeys(mut target: *mut List, source: *mut List) -> *mut List {
    Assert!(target != NIL);

    foreach!(lc, source, {
        let pk = crate::lfirst_node!(PathKey, T_PathKey, current_cell!(lc));

        if !pathkey_is_redundant(pk, target) {
            target = lappend(target, pk as *mut c_void);
        }
    });
    target
}

/*
 * pathkey_is_redundant
 *	   Is a pathkey redundant with one already in the given list?
 *
 * We detect two cases:
 *
 * 1. If the new pathkey's equivalence class contains a constant, and isn't
 * below an outer join, then we can disregard it as a sort key.  An example:
 *			SELECT ... WHERE x = 42 ORDER BY x, y;
 * We may as well just sort by y.  Note that because of opfamily matching,
 * this is semantically correct: we know that the equality constraint is one
 * that actually binds the variable to a single value in the terms of any
 * ordering operator that might go with the eclass.  This rule not only lets
 * us simplify (or even skip) explicit sorts, but also allows matching index
 * sort orders to a query when there are don't-care index columns.
 *
 * 2. If the new pathkey's equivalence class is the same as that of any
 * existing member of the pathkey list, then it is redundant.  Some examples:
 *			SELECT ... ORDER BY x, x;
 *			SELECT ... ORDER BY x, x DESC;
 *			SELECT ... WHERE x = y ORDER BY x, y;
 * In all these cases the second sort key cannot distinguish values that are
 * considered equal by the first, and so there's no point in using it.
 * Note in particular that we need not compare opfamily (all the opfamilies
 * of the EC have the same notion of equality) nor sort direction.
 *
 * Both the given pathkey and the list members must be canonical for this
 * to work properly, but that's okay since we no longer ever construct any
 * non-canonical pathkeys.  (Note: the notion of a pathkey *list* being
 * canonical includes the additional requirement of no redundant entries,
 * which is exactly what we are checking for here.)
 *
 * Because the equivclass.c machinery forms only one copy of any EC per query,
 * pointer comparison is enough to decide whether canonical ECs are the same.
 */
unsafe fn pathkey_is_redundant(new_pathkey: *mut PathKey, pathkeys: *mut List) -> bool {
    let new_ec: *mut EquivalenceClass = (*new_pathkey).pk_eclass;

    /* Check for EC containing a constant --- unconditionally redundant */
    if EC_MUST_BE_REDUNDANT(new_ec) {
        return true;
    }

    /* If same EC already used in list, then redundant */
    foreach!(lc, pathkeys, {
        let old_pathkey = lfirst(current_cell!(lc)) as *mut PathKey;

        if new_ec == (*old_pathkey).pk_eclass {
            return true;
        }
    });

    false
}

/*
 * make_pathkey_from_sortinfo
 *	  Given an expression and sort-order information, create a PathKey.
 *	  The result is always a "canonical" PathKey, but it might be redundant.
 *
 * If the PathKey is being generated from a SortGroupClause, sortref should be
 * the SortGroupClause's SortGroupRef; otherwise zero.
 *
 * If rel is not NULL, it identifies a specific relation we're considering
 * a path for, and indicates that child EC members for that relation can be
 * considered.  Otherwise child members are ignored.  (See the comments for
 * get_eclass_for_sort_expr.)
 *
 * create_it is true if we should create any missing EquivalenceClass
 * needed to represent the sort key.  If it's false, we return NULL if the
 * sort key isn't already present in any EquivalenceClass.
 */
unsafe fn make_pathkey_from_sortinfo(
    root: *mut PlannerInfo,
    expr: *mut Expr,
    opfamily: Oid,
    opcintype: Oid,
    collation: Oid,
    reverse_sort: bool,
    nulls_first: bool,
    sortref: Index,
    rel: Relids,
    create_it: bool,
) -> *mut PathKey {
    let cmptype: CompareType;
    let equality_op: Oid;
    let opfamilies: *mut List;
    let eclass: *mut EquivalenceClass;

    cmptype = if reverse_sort { COMPARE_GT } else { COMPARE_LT };

    /*
     * EquivalenceClasses need to contain opfamily lists based on the family
     * membership of mergejoinable equality operators, which could belong to
     * more than one opfamily.  So we have to look up the opfamily's equality
     * operator and get its membership.
     */
    equality_op = get_opfamily_member_for_cmptype(opfamily, opcintype, opcintype, COMPARE_EQ);
    if !OidIsValid(equality_op) {
        /* shouldn't happen */
        elog!(
            ERROR,
            "missing operator {}({},{}) in opfamily {}",
            COMPARE_EQ,
            opcintype,
            opcintype,
            opfamily
        );
    }
    opfamilies = get_mergejoin_opfamilies(equality_op);
    if opfamilies.is_null() {
        /* certainly should find some */
        elog!(
            ERROR,
            "could not find opfamilies for equality operator {}",
            equality_op
        );
    }

    /* Now find or (optionally) create a matching EquivalenceClass */
    eclass = get_eclass_for_sort_expr(
        root, expr, opfamilies, opcintype, collation, sortref, rel, create_it,
    );

    /* Fail if no EC and !create_it */
    if eclass.is_null() {
        return core::ptr::null_mut();
    }

    /* And finally we can find or create a PathKey node */
    make_canonical_pathkey(root, eclass, opfamily, cmptype, nulls_first)
}

/*
 * make_pathkey_from_sortop
 *	  Like make_pathkey_from_sortinfo, but work from a sort operator.
 *
 * This should eventually go away, but we need to restructure SortGroupClause
 * first.
 */
unsafe fn make_pathkey_from_sortop(
    root: *mut PlannerInfo,
    expr: *mut Expr,
    ordering_op: Oid,
    reverse_sort: bool,
    nulls_first: bool,
    sortref: Index,
    create_it: bool,
) -> *mut PathKey {
    let mut opfamily: Oid = 0;
    let mut opcintype: Oid = 0;
    let collation: Oid;
    let mut cmptype: CompareType = 0;

    /* Find the operator in pg_amop --- failure shouldn't happen */
    if !get_ordering_op_properties(
        ordering_op,
        &raw mut opfamily,
        &raw mut opcintype,
        &raw mut cmptype,
    ) {
        elog!(
            ERROR,
            "operator {} is not a valid ordering operator",
            ordering_op
        );
    }

    /* Because SortGroupClause doesn't carry collation, consult the expr */
    collation = exprCollation(expr as *const Node);

    make_pathkey_from_sortinfo(
        root,
        expr,
        opfamily,
        opcintype,
        collation,
        reverse_sort,
        nulls_first,
        sortref,
        core::ptr::null_mut(),
        create_it,
    )
}

/****************************************************************************
 *		PATHKEY COMPARISONS
 ****************************************************************************/

/*
 * compare_pathkeys
 *	  Compare two pathkeys to see if they are equivalent, and if not whether
 *	  one is "better" than the other.
 *
 *	  We assume the pathkeys are canonical, and so they can be checked for
 *	  equality by simple pointer comparison.
 */
pub unsafe fn compare_pathkeys(keys1: *mut List, keys2: *mut List) -> PathKeysComparison {
    /*
     * Fall out quickly if we are passed two identical lists.  This mostly
     * catches the case where both are NIL, but that's common enough to
     * warrant the test.
     */
    if keys1 == keys2 {
        return PATHKEYS_EQUAL;
    }

    let mut key1 = list_head(keys1);
    let mut key2 = list_head(keys2);
    while !key1.is_null() && !key2.is_null() {
        let pathkey1 = lfirst(key1) as *mut PathKey;
        let pathkey2 = lfirst(key2) as *mut PathKey;

        if pathkey1 != pathkey2 {
            return PATHKEYS_DIFFERENT; /* no need to keep looking */
        }
        key1 = lnext(keys1, key1);
        key2 = lnext(keys2, key2);
    }

    /*
     * If we reached the end of only one list, the other is longer and
     * therefore not a subset.
     */
    if !key1.is_null() {
        return PATHKEYS_BETTER1; /* key1 is longer */
    }
    if !key2.is_null() {
        return PATHKEYS_BETTER2; /* key2 is longer */
    }
    PATHKEYS_EQUAL
}

/*
 * pathkeys_contained_in
 *	  Common special case of compare_pathkeys: we just want to know
 *	  if keys2 are at least as well sorted as keys1.
 */
pub unsafe fn pathkeys_contained_in(keys1: *mut List, keys2: *mut List) -> bool {
    match compare_pathkeys(keys1, keys2) {
        PATHKEYS_EQUAL | PATHKEYS_BETTER2 => return true,
        _ => {}
    }
    false
}

/*
 * group_keys_reorder_by_pathkeys
 *		Reorder GROUP BY pathkeys and clauses to match the input pathkeys.
 *
 * 'pathkeys' is an input list of pathkeys
 * '*group_pathkeys' and '*group_clauses' are pathkeys and clauses lists to
 *		reorder.  The pointers are redirected to new lists, original lists
 *		stay untouched.
 * 'num_groupby_pathkeys' is the number of first '*group_pathkeys' items to
 *		search matching pathkeys.
 *
 * Returns the number of GROUP BY keys with a matching pathkey.
 */
unsafe fn group_keys_reorder_by_pathkeys(
    pathkeys: *mut List,
    group_pathkeys: *mut *mut List,
    group_clauses: *mut *mut List,
    num_groupby_pathkeys: c_int,
) -> c_int {
    let mut new_group_pathkeys: *mut List = NIL;
    let mut new_group_clauses: *mut List = NIL;
    let grouping_pathkeys: *mut List;
    let n: c_int;

    if pathkeys == NIL || *group_pathkeys == NIL {
        return 0;
    }

    /*
     * We're going to search within just the first num_groupby_pathkeys of
     * *group_pathkeys.  The thing is that root->group_pathkeys is passed as
     * *group_pathkeys containing grouping pathkeys altogether with aggregate
     * pathkeys.  If we process aggregate pathkeys we could get an invalid
     * result of get_sortgroupref_clause_noerr(), because their
     * pathkey->pk_eclass->ec_sortref doesn't reference query targetlist.  So,
     * we allocate a separate list of pathkeys for lookups.
     */
    grouping_pathkeys = list_copy_head(*group_pathkeys, num_groupby_pathkeys);

    /*
     * Walk the pathkeys (determining ordering of the input path) and see if
     * there's a matching GROUP BY key. If we find one, we append it to the
     * list, and do the same for the clauses.
     *
     * Once we find the first pathkey without a matching GROUP BY key, the
     * rest of the pathkeys are useless and can't be used to evaluate the
     * grouping, so we abort the loop and ignore the remaining pathkeys.
     */
    foreach!(lc, pathkeys, {
        let pathkey = lfirst(current_cell!(lc)) as *mut PathKey;
        let sgc: *mut SortGroupClause;

        /*
         * Pathkeys are built in a way that allows simply comparing pointers.
         * Give up if we can't find the matching pointer.  Also give up if
         * there is no sortclause reference for some reason.
         */
        if crate::foreach_current_index!(lc) >= num_groupby_pathkeys
            || !list_member_ptr(grouping_pathkeys, pathkey as *const c_void)
            || (*(*pathkey).pk_eclass).ec_sortref == 0
        {
            break;
        }

        /*
         * Since 1349d27 pathkey coming from underlying node can be in the
         * root->group_pathkeys but not in the processed_groupClause. So, we
         * should be careful here.
         */
        sgc = get_sortgroupref_clause_noerr((*(*pathkey).pk_eclass).ec_sortref, *group_clauses);
        if sgc.is_null() {
            /* The grouping clause does not cover this pathkey */
            break;
        }

        /*
         * Sort group clause should have an ordering operator as long as there
         * is an associated pathkey.
         */
        Assert!(OidIsValid((*sgc).sortop));

        new_group_pathkeys = lappend(new_group_pathkeys, pathkey as *mut c_void);
        new_group_clauses = lappend(new_group_clauses, sgc as *mut c_void);
    });

    /* remember the number of pathkeys with a matching GROUP BY key */
    n = list_length(new_group_pathkeys);

    /* append the remaining group pathkeys (will be treated as not sorted) */
    *group_pathkeys = list_concat_unique_ptr(new_group_pathkeys, *group_pathkeys);
    *group_clauses = list_concat_unique_ptr(new_group_clauses, *group_clauses);

    list_free(grouping_pathkeys);
    n
}

/*
 * get_useful_group_keys_orderings
 *		Determine which orderings of GROUP BY keys are potentially interesting.
 *
 * Returns a list of GroupByOrdering items, each representing an interesting
 * ordering of GROUP BY keys.  Each item stores pathkeys and clauses in the
 * matching order.
 *
 * The function considers (and keeps) following GROUP BY orderings:
 *
 * - GROUP BY keys as ordered by preprocess_groupclause() to match target
 *   ORDER BY clause (as much as possible),
 * - GROUP BY keys reordered to match 'path' ordering (as much as possible).
 */
pub unsafe fn get_useful_group_keys_orderings(root: *mut PlannerInfo, path: *mut Path) -> *mut List {
    let parse: *mut Query = (*root).parse;
    let mut infos: *mut List = NIL;
    let mut info: *mut GroupByOrdering;

    let mut pathkeys: *mut List = (*root).group_pathkeys;
    let mut clauses: *mut List = (*root).processed_groupClause;

    /* always return at least the original pathkeys/clauses */
    info = makeNode!(GroupByOrdering, T_GroupByOrdering);
    (*info).pathkeys = pathkeys;
    (*info).clauses = clauses;
    infos = lappend(infos, info as *mut c_void);

    /*
     * Should we try generating alternative orderings of the group keys? If
     * not, we produce only the order specified in the query, i.e. the
     * optimization is effectively disabled.
     */
    if !enable_group_by_reordering {
        return infos;
    }

    /*
     * Grouping sets have own and more complex logic to decide the ordering.
     */
    if !(*parse).groupingSets.is_null() {
        return infos;
    }

    /*
     * If the path is sorted in some way, try reordering the group keys to
     * match the path as much of the ordering as possible.  Then thanks to
     * incremental sort we would get this sort as cheap as possible.
     */
    if !(*path).pathkeys.is_null()
        && !pathkeys_contained_in((*path).pathkeys, (*root).group_pathkeys)
    {
        let n: c_int;

        n = group_keys_reorder_by_pathkeys(
            (*path).pathkeys,
            &raw mut pathkeys,
            &raw mut clauses,
            (*root).num_groupby_pathkeys,
        );

        if n > 0
            && (enable_incremental_sort || n == (*root).num_groupby_pathkeys)
            && compare_pathkeys(pathkeys, (*root).group_pathkeys) != PATHKEYS_EQUAL
        {
            info = makeNode!(GroupByOrdering, T_GroupByOrdering);
            (*info).pathkeys = pathkeys;
            (*info).clauses = clauses;

            infos = lappend(infos, info as *mut c_void);
        }
    }

    #[cfg(debug_assertions)]
    {
        let pinfo: *mut GroupByOrdering = crate::linitial_node!(GroupByOrdering, T_GroupByOrdering, infos);

        /* Test consistency of info structures */
        for_each_from!(lc, infos, 1, {
            info = crate::lfirst_node!(GroupByOrdering, T_GroupByOrdering, current_cell!(lc));

            Assert!(list_length((*info).clauses) == list_length((*pinfo).clauses));
            Assert!(list_length((*info).pathkeys) == list_length((*pinfo).pathkeys));
            Assert!(list_difference((*info).clauses, (*pinfo).clauses) == NIL);
            Assert!(list_difference_ptr((*info).pathkeys, (*pinfo).pathkeys) == NIL);

            forboth!(lc1, (*info).clauses, lc2, (*info).pathkeys, {
                let sgc = crate::lfirst_node!(SortGroupClause, T_SortGroupClause, lc1);
                let pk = crate::lfirst_node!(PathKey, T_PathKey, lc2);

                Assert!((*(*pk).pk_eclass).ec_sortref == (*sgc).tleSortGroupRef);
            });
        });
    }
    infos
}

/*
 * pathkeys_count_contained_in
 *    Same as pathkeys_contained_in, but also sets length of longest
 *    common prefix of keys1 and keys2.
 */
pub unsafe fn pathkeys_count_contained_in(
    keys1: *mut List,
    keys2: *mut List,
    n_common: *mut c_int,
) -> bool {
    let mut n: c_int = 0;

    /*
     * See if we can avoiding looping through both lists. This optimization
     * gains us several percent in planning time in a worst-case test.
     */
    if keys1 == keys2 {
        *n_common = list_length(keys1);
        return true;
    } else if keys1 == NIL {
        *n_common = 0;
        return true;
    } else if keys2 == NIL {
        *n_common = 0;
        return false;
    }

    /*
     * If both lists are non-empty, iterate through both to find out how many
     * items are shared.
     */
    let mut key1 = list_head(keys1);
    let mut key2 = list_head(keys2);
    while !key1.is_null() && !key2.is_null() {
        let pathkey1 = lfirst(key1) as *mut PathKey;
        let pathkey2 = lfirst(key2) as *mut PathKey;

        if pathkey1 != pathkey2 {
            *n_common = n;
            return false;
        }
        n += 1;
        key1 = lnext(keys1, key1);
        key2 = lnext(keys2, key2);
    }

    /* If we ended with a null value, then we've processed the whole list. */
    *n_common = n;
    key1.is_null()
}

/*
 * get_cheapest_path_for_pathkeys
 *	  Find the cheapest path (according to the specified criterion) that
 *	  satisfies the given pathkeys and parameterization, and is parallel-safe
 *	  if required.
 *	  Return NULL if no such path.
 *
 * 'paths' is a list of possible paths that all generate the same relation
 * 'pathkeys' represents a required ordering (in canonical form!)
 * 'required_outer' denotes allowable outer relations for parameterized paths
 * 'cost_criterion' is STARTUP_COST or TOTAL_COST
 * 'require_parallel_safe' causes us to consider only parallel-safe paths
 */
pub unsafe fn get_cheapest_path_for_pathkeys(
    paths: *mut List,
    pathkeys: *mut List,
    required_outer: Relids,
    cost_criterion: CostSelector,
    require_parallel_safe: bool,
) -> *mut Path {
    let mut matched_path: *mut Path = core::ptr::null_mut();

    foreach!(l, paths, {
        let path = lfirst(current_cell!(l)) as *mut Path;

        /* If required, reject paths that are not parallel-safe */
        if require_parallel_safe && !(*path).parallel_safe {
            continue;
        }

        /*
         * Since cost comparison is a lot cheaper than pathkey comparison, do
         * that first.  (XXX is that still true?)
         */
        if !matched_path.is_null()
            && compare_path_costs(matched_path, path, cost_criterion) <= 0
        {
            continue;
        }

        if pathkeys_contained_in(pathkeys, (*path).pathkeys)
            && bms_is_subset(PATH_REQ_OUTER(path), required_outer)
        {
            matched_path = path;
        }
    });
    matched_path
}

/*
 * get_cheapest_fractional_path_for_pathkeys
 *	  Find the cheapest path (for retrieving a specified fraction of all
 *	  the tuples) that satisfies the given pathkeys and parameterization.
 *	  Return NULL if no such path.
 *
 * See compare_fractional_path_costs() for the interpretation of the fraction
 * parameter.
 *
 * 'paths' is a list of possible paths that all generate the same relation
 * 'pathkeys' represents a required ordering (in canonical form!)
 * 'required_outer' denotes allowable outer relations for parameterized paths
 * 'fraction' is the fraction of the total tuples expected to be retrieved
 */
pub unsafe fn get_cheapest_fractional_path_for_pathkeys(
    paths: *mut List,
    pathkeys: *mut List,
    required_outer: Relids,
    fraction: f64,
) -> *mut Path {
    let mut matched_path: *mut Path = core::ptr::null_mut();

    foreach!(l, paths, {
        let path = lfirst(current_cell!(l)) as *mut Path;

        /*
         * Since cost comparison is a lot cheaper than pathkey comparison, do
         * that first.  (XXX is that still true?)
         */
        if !matched_path.is_null()
            && compare_fractional_path_costs(matched_path, path, fraction) <= 0
        {
            continue;
        }

        if pathkeys_contained_in(pathkeys, (*path).pathkeys)
            && bms_is_subset(PATH_REQ_OUTER(path), required_outer)
        {
            matched_path = path;
        }
    });
    matched_path
}

/*
 * get_cheapest_parallel_safe_total_inner
 *	  Find the unparameterized parallel-safe path with the least total cost.
 */
pub unsafe fn get_cheapest_parallel_safe_total_inner(paths: *mut List) -> *mut Path {
    foreach!(l, paths, {
        let innerpath = lfirst(current_cell!(l)) as *mut Path;

        if (*innerpath).parallel_safe && bms_is_empty(PATH_REQ_OUTER(innerpath)) {
            return innerpath;
        }
    });

    core::ptr::null_mut()
}

/****************************************************************************
 *		NEW PATHKEY FORMATION
 ****************************************************************************/

/*
 * build_index_pathkeys
 *	  Build a pathkeys list that describes the ordering induced by an index
 *	  scan using the given index.  (Note that an unordered index doesn't
 *	  induce any ordering, so we return NIL.)
 *
 * If 'scandir' is BackwardScanDirection, build pathkeys representing a
 * backwards scan of the index.
 *
 * We iterate only key columns of covering indexes, since non-key columns
 * don't influence index ordering.  The result is canonical, meaning that
 * redundant pathkeys are removed; it may therefore have fewer entries than
 * there are key columns in the index.
 *
 * Another reason for stopping early is that we may be able to tell that
 * an index column's sort order is uninteresting for this query.  However,
 * that test is just based on the existence of an EquivalenceClass and not
 * on position in pathkey lists, so it's not complete.  Caller should call
 * truncate_useless_pathkeys() to possibly remove more pathkeys.
 */
pub unsafe fn build_index_pathkeys(
    root: *mut PlannerInfo,
    index: *mut IndexOptInfo,
    scandir: ScanDirection,
) -> *mut List {
    let mut retval: *mut List = NIL;
    let mut i: c_int;

    if (*index).sortopfamily.is_null() {
        return NIL; /* non-orderable index */
    }

    i = 0;
    foreach!(lc, (*index).indextlist, {
        let indextle = lfirst(current_cell!(lc)) as *mut TargetEntry;
        let indexkey: *mut Expr;
        let reverse_sort: bool;
        let nulls_first: bool;
        let cpathkey: *mut PathKey;

        /*
         * INCLUDE columns are stored in index unordered, so they don't
         * support ordered index scan.
         */
        if i >= (*index).nkeycolumns {
            break;
        }

        /* We assume we don't need to make a copy of the tlist item */
        indexkey = (*indextle).expr;

        if ScanDirectionIsBackward(scandir) {
            reverse_sort = !*(*index).reverse_sort.add(i as usize);
            nulls_first = !*(*index).nulls_first.add(i as usize);
        } else {
            reverse_sort = *(*index).reverse_sort.add(i as usize);
            nulls_first = *(*index).nulls_first.add(i as usize);
        }

        /*
         * OK, try to make a canonical pathkey for this sort key.
         */
        cpathkey = make_pathkey_from_sortinfo(
            root,
            indexkey,
            *(*index).sortopfamily.add(i as usize),
            *(*index).opcintype.add(i as usize),
            *(*index).indexcollations.add(i as usize),
            reverse_sort,
            nulls_first,
            0,
            (*(*index).rel).relids,
            false,
        );

        if !cpathkey.is_null() {
            /*
             * We found the sort key in an EquivalenceClass, so it's relevant
             * for this query.  Add it to list, unless it's redundant.
             */
            if !pathkey_is_redundant(cpathkey, retval) {
                retval = lappend(retval, cpathkey as *mut c_void);
            }
        } else {
            /*
             * Boolean index keys might be redundant even if they do not
             * appear in an EquivalenceClass, because of our special treatment
             * of boolean equality conditions --- see the comment for
             * indexcol_is_bool_constant_for_query().  If that applies, we can
             * continue to examine lower-order index columns.  Otherwise, the
             * sort key is not an interesting sort order for this query, so we
             * should stop considering index columns; any lower-order sort
             * keys won't be useful either.
             */
            if !indexcol_is_bool_constant_for_query(root, index, i) {
                break;
            }
        }

        i += 1;
    });

    retval
}

/*
 * partkey_is_bool_constant_for_query
 *
 * If a partition key column is constrained to have a constant value by the
 * query's WHERE conditions, then it's irrelevant for sort-order
 * considerations.  Usually that means we have a restriction clause
 * WHERE partkeycol = constant, which gets turned into an EquivalenceClass
 * containing a constant, which is recognized as redundant by
 * build_partition_pathkeys().  But if the partition key column is a
 * boolean variable (or expression), then we are not going to see such a
 * WHERE clause, because expression preprocessing will have simplified it
 * to "WHERE partkeycol" or "WHERE NOT partkeycol".  So we are not going
 * to have a matching EquivalenceClass (unless the query also contains
 * "ORDER BY partkeycol").  To allow such cases to work the same as they would
 * for non-boolean values, this function is provided to detect whether the
 * specified partition key column matches a boolean restriction clause.
 */
unsafe fn partkey_is_bool_constant_for_query(partrel: *mut RelOptInfo, partkeycol: c_int) -> bool {
    let partscheme: PartitionScheme = (*partrel).part_scheme;

    /*
     * If the partkey isn't boolean, we can't possibly get a match.
     *
     * Partitioning currently can only use built-in AMs, so checking for
     * built-in boolean opfamilies is good enough.
     */
    if !IsBuiltinBooleanOpfamily(*(*partscheme).partopfamily.add(partkeycol as usize)) {
        return false;
    }

    /* Check each restriction clause for the partitioned rel */
    foreach!(lc, (*partrel).baserestrictinfo, {
        let rinfo = lfirst(current_cell!(lc)) as *mut RestrictInfo;

        /* Ignore pseudoconstant quals, they won't match */
        if (*rinfo).pseudoconstant {
            continue;
        }

        /* See if we can match the clause's expression to the partkey column */
        if matches_boolean_partition_clause(rinfo, partrel, partkeycol) {
            return true;
        }
    });

    false
}

/*
 * matches_boolean_partition_clause
 *		Determine if the boolean clause described by rinfo matches
 *		partrel's partkeycol-th partition key column.
 *
 * "Matches" can be either an exact match (equivalent to partkey = true),
 * or a NOT above an exact match (equivalent to partkey = false).
 */
unsafe fn matches_boolean_partition_clause(
    rinfo: *mut RestrictInfo,
    partrel: *mut RelOptInfo,
    partkeycol: c_int,
) -> bool {
    let clause: *mut Node = (*rinfo).clause as *mut Node;
    let partexpr: *mut Node =
        linitial(*(*partrel).partexprs.add(partkeycol as usize)) as *mut Node;

    /* Direct match? */
    if equal(partexpr as *const c_void, clause as *const c_void) {
        true
    }
    /* NOT clause? */
    else if is_notclause(clause as *const c_void) {
        let arg: *mut Node = get_notclausearg(clause as *const c_void) as *mut Node;

        equal(partexpr as *const c_void, arg as *const c_void)
    } else {
        false
    }
}

/*
 * build_partition_pathkeys
 *	  Build a pathkeys list that describes the ordering induced by the
 *	  partitions of partrel, under either forward or backward scan
 *	  as per scandir.
 *
 * Caller must have checked that the partitions are properly ordered,
 * as detected by partitions_are_ordered().
 *
 * Sets *partialkeys to true if pathkeys were only built for a prefix of the
 * partition key, or false if the pathkeys include all columns of the
 * partition key.
 */
pub unsafe fn build_partition_pathkeys(
    root: *mut PlannerInfo,
    partrel: *mut RelOptInfo,
    scandir: ScanDirection,
    partialkeys: *mut bool,
) -> *mut List {
    let mut retval: *mut List = NIL;
    let partscheme: PartitionScheme = (*partrel).part_scheme;
    let mut i: c_int;

    Assert!(!partscheme.is_null());
    Assert!(partitions_are_ordered(
        (*partrel).boundinfo as *mut c_void,
        (*partrel).live_parts
    ));
    /* For now, we can only cope with baserels */
    Assert!(IS_SIMPLE_REL(partrel));

    i = 0;
    while i < (*partscheme).partnatts as c_int {
        let cpathkey: *mut PathKey;
        let keyCol: *mut Expr = linitial(*(*partrel).partexprs.add(i as usize)) as *mut Expr;

        /*
         * Try to make a canonical pathkey for this partkey.
         *
         * We assume the PartitionDesc lists any NULL partition last, so we
         * treat the scan like a NULLS LAST index: we have nulls_first for
         * backwards scan only.
         */
        cpathkey = make_pathkey_from_sortinfo(
            root,
            keyCol,
            *(*partscheme).partopfamily.add(i as usize),
            *(*partscheme).partopcintype.add(i as usize),
            *(*partscheme).partcollation.add(i as usize),
            ScanDirectionIsBackward(scandir),
            ScanDirectionIsBackward(scandir),
            0,
            (*partrel).relids,
            false,
        );

        if !cpathkey.is_null() {
            /*
             * We found the sort key in an EquivalenceClass, so it's relevant
             * for this query.  Add it to list, unless it's redundant.
             */
            if !pathkey_is_redundant(cpathkey, retval) {
                retval = lappend(retval, cpathkey as *mut c_void);
            }
        } else {
            /*
             * Boolean partition keys might be redundant even if they do not
             * appear in an EquivalenceClass, because of our special treatment
             * of boolean equality conditions --- see the comment for
             * partkey_is_bool_constant_for_query().  If that applies, we can
             * continue to examine lower-order partition keys.  Otherwise, the
             * sort key is not an interesting sort order for this query, so we
             * should stop considering partition columns; any lower-order sort
             * keys won't be useful either.
             */
            if !partkey_is_bool_constant_for_query(partrel, i) {
                *partialkeys = true;
                return retval;
            }
        }

        i += 1;
    }

    *partialkeys = false;
    retval
}

/*
 * build_expression_pathkey
 *	  Build a pathkeys list that describes an ordering by a single expression
 *	  using the given sort operator.
 *
 * expr and rel are as for make_pathkey_from_sortinfo.
 * We induce the other arguments assuming default sort order for the operator.
 *
 * Similarly to make_pathkey_from_sortinfo, the result is NIL if create_it
 * is false and the expression isn't already in some EquivalenceClass.
 */
pub unsafe fn build_expression_pathkey(
    root: *mut PlannerInfo,
    expr: *mut Expr,
    opno: Oid,
    rel: Relids,
    create_it: bool,
) -> *mut List {
    let pathkeys: *mut List;
    let mut opfamily: Oid = 0;
    let mut opcintype: Oid = 0;
    let mut cmptype: CompareType = 0;
    let cpathkey: *mut PathKey;

    /* Find the operator in pg_amop --- failure shouldn't happen */
    if !get_ordering_op_properties(
        opno,
        &raw mut opfamily,
        &raw mut opcintype,
        &raw mut cmptype,
    ) {
        elog!(ERROR, "operator {} is not a valid ordering operator", opno);
    }

    cpathkey = make_pathkey_from_sortinfo(
        root,
        expr,
        opfamily,
        opcintype,
        exprCollation(expr as *const Node),
        cmptype == COMPARE_GT,
        cmptype == COMPARE_GT,
        0,
        rel,
        create_it,
    );

    if !cpathkey.is_null() {
        pathkeys = list_make1!(cpathkey as *mut c_void);
    } else {
        pathkeys = NIL;
    }

    pathkeys
}

/*
 * convert_subquery_pathkeys
 *	  Build a pathkeys list that describes the ordering of a subquery's
 *	  result, in the terms of the outer query.  This is essentially a
 *	  task of conversion.
 *
 * 'rel': outer query's RelOptInfo for the subquery relation.
 * 'subquery_pathkeys': the subquery's output pathkeys, in its terms.
 * 'subquery_tlist': the subquery's output targetlist, in its terms.
 *
 * We intentionally don't do truncate_useless_pathkeys() here, because there
 * are situations where seeing the raw ordering of the subquery is helpful.
 * For example, if it returns ORDER BY x DESC, that may prompt us to
 * construct a mergejoin using DESC order rather than ASC order; but the
 * right_merge_direction heuristic would have us throw the knowledge away.
 */
pub unsafe fn convert_subquery_pathkeys(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    subquery_pathkeys: *mut List,
    subquery_tlist: *mut List,
) -> *mut List {
    let mut retval: *mut List = NIL;
    let mut retvallen: c_int = 0;
    let outer_query_keys: c_int = list_length((*root).query_pathkeys);

    foreach!(i, subquery_pathkeys, {
        let sub_pathkey = lfirst(current_cell!(i)) as *mut PathKey;
        let sub_eclass: *mut EquivalenceClass = (*sub_pathkey).pk_eclass;
        let mut best_pathkey: *mut PathKey = core::ptr::null_mut();

        if (*sub_eclass).ec_has_volatile {
            /*
             * If the sub_pathkey's EquivalenceClass is volatile, then it must
             * have come from an ORDER BY clause, and we have to match it to
             * that same targetlist entry.
             */
            let tle: *mut TargetEntry;
            let outer_var: *mut Var;

            if (*sub_eclass).ec_sortref == 0 {
                /* can't happen */
                elog!(ERROR, "volatile EquivalenceClass has no sortref");
            }
            tle = get_sortgroupref_tle((*sub_eclass).ec_sortref, subquery_tlist);
            Assert!(!tle.is_null());
            /* Is TLE actually available to the outer query? */
            outer_var = find_var_for_subquery_tle(rel, tle);
            if !outer_var.is_null() {
                /* We can represent this sub_pathkey */
                let sub_member: *mut EquivalenceMember;
                let outer_ec: *mut EquivalenceClass;

                Assert!(list_length((*sub_eclass).ec_members) == 1);
                sub_member = linitial((*sub_eclass).ec_members) as *mut EquivalenceMember;

                /*
                 * Note: it might look funny to be setting sortref = 0 for a
                 * reference to a volatile sub_eclass.  However, the
                 * expression is *not* volatile in the outer query: it's just
                 * a Var referencing whatever the subquery emitted. (IOW, the
                 * outer query isn't going to re-execute the volatile
                 * expression itself.)	So this is okay.
                 */
                outer_ec = get_eclass_for_sort_expr(
                    root,
                    outer_var as *mut Expr,
                    (*sub_eclass).ec_opfamilies,
                    (*sub_member).em_datatype,
                    (*sub_eclass).ec_collation,
                    0,
                    (*rel).relids,
                    false,
                );

                /*
                 * If we don't find a matching EC, sub-pathkey isn't
                 * interesting to the outer query
                 */
                if !outer_ec.is_null() {
                    best_pathkey = make_canonical_pathkey(
                        root,
                        outer_ec,
                        (*sub_pathkey).pk_opfamily,
                        (*sub_pathkey).pk_cmptype,
                        (*sub_pathkey).pk_nulls_first,
                    );
                }
            }
        } else {
            /*
             * Otherwise, the sub_pathkey's EquivalenceClass could contain
             * multiple elements (representing knowledge that multiple items
             * are effectively equal).  Each element might match none, one, or
             * more of the output columns that are visible to the outer query.
             * This means we may have multiple possible representations of the
             * sub_pathkey in the context of the outer query.  Ideally we
             * would generate them all and put them all into an EC of the
             * outer query, thereby propagating equality knowledge up to the
             * outer query.  Right now we cannot do so, because the outer
             * query's EquivalenceClasses are already frozen when this is
             * called. Instead we prefer the one that has the highest "score"
             * (number of EC peers, plus one if it matches the outer
             * query_pathkeys). This is the most likely to be useful in the
             * outer query.
             */
            let mut best_score: c_int = -1;

            /* Ignore children here */
            foreach!(j, (*sub_eclass).ec_members, {
                let sub_member = lfirst(current_cell!(j)) as *mut EquivalenceMember;
                let sub_expr: *mut Expr = (*sub_member).em_expr;
                let sub_expr_type: Oid = (*sub_member).em_datatype;
                let sub_expr_coll: Oid = (*sub_eclass).ec_collation;

                /* Child members should not exist in ec_members */
                Assert!(!(*sub_member).em_is_child);

                foreach!(k, subquery_tlist, {
                    let tle = lfirst(current_cell!(k)) as *mut TargetEntry;
                    let outer_var: *mut Var;
                    let tle_expr: *mut Expr;
                    let outer_ec: *mut EquivalenceClass;
                    let outer_pk: *mut PathKey;
                    let mut score: c_int;

                    /* Is TLE actually available to the outer query? */
                    outer_var = find_var_for_subquery_tle(rel, tle);
                    if outer_var.is_null() {
                        continue;
                    }

                    /*
                     * The targetlist entry is considered to match if it
                     * matches after sort-key canonicalization.  That is
                     * needed since the sub_expr has been through the same
                     * process.
                     */
                    tle_expr = canonicalize_ec_expression((*tle).expr, sub_expr_type, sub_expr_coll);
                    if !equal(tle_expr as *const c_void, sub_expr as *const c_void) {
                        continue;
                    }

                    /* See if we have a matching EC for the TLE */
                    outer_ec = get_eclass_for_sort_expr(
                        root,
                        outer_var as *mut Expr,
                        (*sub_eclass).ec_opfamilies,
                        sub_expr_type,
                        sub_expr_coll,
                        0,
                        (*rel).relids,
                        false,
                    );

                    /*
                     * If we don't find a matching EC, this sub-pathkey isn't
                     * interesting to the outer query
                     */
                    if outer_ec.is_null() {
                        continue;
                    }

                    outer_pk = make_canonical_pathkey(
                        root,
                        outer_ec,
                        (*sub_pathkey).pk_opfamily,
                        (*sub_pathkey).pk_cmptype,
                        (*sub_pathkey).pk_nulls_first,
                    );
                    /* score = # of equivalence peers */
                    score = list_length((*outer_ec).ec_members) - 1;
                    /* +1 if it matches the proper query_pathkeys item */
                    if retvallen < outer_query_keys
                        && list_nth((*root).query_pathkeys, retvallen) as *mut PathKey == outer_pk
                    {
                        score += 1;
                    }
                    if score > best_score {
                        best_pathkey = outer_pk;
                        best_score = score;
                    }
                });
            });
        }

        /*
         * If we couldn't find a representation of this sub_pathkey, we're
         * done (we can't use the ones to its right, either).
         */
        if best_pathkey.is_null() {
            break;
        }

        /*
         * Eliminate redundant ordering info; could happen if outer query
         * equivalences subquery keys...
         */
        if !pathkey_is_redundant(best_pathkey, retval) {
            retval = lappend(retval, best_pathkey as *mut c_void);
            retvallen += 1;
        }
    });

    retval
}

/*
 * find_var_for_subquery_tle
 *
 * If the given subquery tlist entry is due to be emitted by the subquery's
 * scan node, return a Var for it, else return NULL.
 *
 * We need this to ensure that we don't return pathkeys describing values
 * that are unavailable above the level of the subquery scan.
 */
unsafe fn find_var_for_subquery_tle(rel: *mut RelOptInfo, tle: *mut TargetEntry) -> *mut Var {
    /* If the TLE is resjunk, it's certainly not visible to the outer query */
    if (*tle).resjunk {
        return core::ptr::null_mut();
    }

    /* Search the rel's targetlist to see what it will return */
    foreach!(lc, (*(*rel).reltarget).exprs, {
        let var = lfirst(current_cell!(lc)) as *mut Var;

        /* Ignore placeholders */
        if !IsA!(var, T_Var) {
            continue;
        }
        Assert!((*var).varno as Index == (*rel).relid);

        /* If we find a Var referencing this TLE, we're good */
        if (*var).varattno == (*tle).resno {
            return copyObject(var); /* Make a copy for safety */
        }
    });
    core::ptr::null_mut()
}

/*
 * build_join_pathkeys
 *	  Build the path keys for a join relation constructed by mergejoin or
 *	  nestloop join.  This is normally the same as the outer path's keys.
 *
 *	  EXCEPTION: in a FULL, RIGHT or RIGHT_ANTI join, we cannot treat the
 *	  result as having the outer path's path keys, because null lefthand rows
 *	  may be inserted at random points.  It must be treated as unsorted.
 *
 *	  We truncate away any pathkeys that are uninteresting for higher joins.
 *
 * 'joinrel' is the join relation that paths are being formed for
 * 'jointype' is the join type (inner, left, full, etc)
 * 'outer_pathkeys' is the list of the current outer path's path keys
 *
 * Returns the list of new path keys.
 */
pub unsafe fn build_join_pathkeys(
    root: *mut PlannerInfo,
    joinrel: *mut RelOptInfo,
    jointype: JoinType,
    outer_pathkeys: *mut List,
) -> *mut List {
    /* RIGHT_SEMI should not come here */
    Assert!(jointype != JOIN_RIGHT_SEMI);

    if jointype == JOIN_FULL || jointype == JOIN_RIGHT || jointype == JOIN_RIGHT_ANTI {
        return NIL;
    }

    /*
     * This used to be quite a complex bit of code, but now that all pathkey
     * sublists start out life canonicalized, we don't have to do a darn thing
     * here!
     *
     * We do, however, need to truncate the pathkeys list, since it may
     * contain pathkeys that were useful for forming this joinrel but are
     * uninteresting to higher levels.
     */
    truncate_useless_pathkeys(root, joinrel, outer_pathkeys)
}

/****************************************************************************
 *		PATHKEYS AND SORT CLAUSES
 ****************************************************************************/

/*
 * make_pathkeys_for_sortclauses
 *		Generate a pathkeys list that represents the sort order specified
 *		by a list of SortGroupClauses
 *
 * The resulting PathKeys are always in canonical form.  (Actually, there
 * is no longer any code anywhere that creates non-canonical PathKeys.)
 *
 * 'sortclauses' is a list of SortGroupClause nodes
 * 'tlist' is the targetlist to find the referenced tlist entries in
 */
pub unsafe fn make_pathkeys_for_sortclauses(
    root: *mut PlannerInfo,
    mut sortclauses: *mut List,
    tlist: *mut List,
) -> *mut List {
    let result: *mut List;
    let mut sortable: bool = false;

    result = make_pathkeys_for_sortclauses_extended(
        root,
        &raw mut sortclauses,
        tlist,
        false,
        false,
        &raw mut sortable,
        false,
    );
    /* It's caller error if not all clauses were sortable */
    Assert!(sortable);
    result
}

/*
 * make_pathkeys_for_sortclauses_extended
 *		Generate a pathkeys list that represents the sort order specified
 *		by a list of SortGroupClauses
 *
 * The comments for make_pathkeys_for_sortclauses apply here too. In addition:
 *
 * If remove_redundant is true, then any sort clauses that are found to
 * give rise to redundant pathkeys are removed from the sortclauses list
 * (which therefore must be pass-by-reference in this version).
 *
 * If remove_group_rtindex is true, then we need to remove the RT index of the
 * grouping step from the sort expressions before we make PathKeys for them.
 *
 * *sortable is set to true if all the sort clauses are in fact sortable.
 * If any are not, they are ignored except for setting *sortable false.
 * (In that case, the output pathkey list isn't really useful.  However,
 * we process the whole sortclauses list anyway, because it's still valid
 * to remove any clauses that can be proven redundant via the eclass logic.
 * Even though we'll have to hash in that case, we might as well not hash
 * redundant columns.)
 *
 * If set_ec_sortref is true then sets the value of the pathkey's
 * EquivalenceClass unless it's already initialized.
 */
pub unsafe fn make_pathkeys_for_sortclauses_extended(
    root: *mut PlannerInfo,
    sortclauses: *mut *mut List,
    tlist: *mut List,
    remove_redundant: bool,
    remove_group_rtindex: bool,
    sortable: *mut bool,
    set_ec_sortref: bool,
) -> *mut List {
    let mut pathkeys: *mut List = NIL;

    *sortable = true;
    foreach!(l, *sortclauses, {
        let sortcl = lfirst(current_cell!(l)) as *mut SortGroupClause;
        let mut sortkey: *mut Expr;
        let pathkey: *mut PathKey;

        sortkey = get_sortgroupclause_expr(sortcl, tlist) as *mut Expr;
        if !OidIsValid((*sortcl).sortop) {
            *sortable = false;
            continue;
        }
        if remove_group_rtindex {
            Assert!((*root).group_rtindex > 0);
            sortkey = remove_nulling_relids(
                sortkey as *mut Node,
                bms_make_singleton((*root).group_rtindex),
                core::ptr::null_mut(),
            ) as *mut Expr;
        }
        pathkey = make_pathkey_from_sortop(
            root,
            sortkey,
            (*sortcl).sortop,
            (*sortcl).reverse_sort,
            (*sortcl).nulls_first,
            (*sortcl).tleSortGroupRef,
            true,
        );
        if (*(*pathkey).pk_eclass).ec_sortref == 0 && set_ec_sortref {
            /*
             * Copy the sortref if it hasn't been set yet.  That may happen if
             * the EquivalenceClass was constructed from a WHERE clause, i.e.
             * it doesn't have a target reference at all.
             */
            (*(*pathkey).pk_eclass).ec_sortref = (*sortcl).tleSortGroupRef;
        }

        /* Canonical form eliminates redundant ordering keys */
        if !pathkey_is_redundant(pathkey, pathkeys) {
            pathkeys = lappend(pathkeys, pathkey as *mut c_void);
        } else if remove_redundant {
            *sortclauses = foreach_delete_current!(*sortclauses, l);
        }
    });
    pathkeys
}

/****************************************************************************
 *		PATHKEYS AND MERGECLAUSES
 ****************************************************************************/

/*
 * initialize_mergeclause_eclasses
 *		Set the EquivalenceClass links in a mergeclause restrictinfo.
 *
 * RestrictInfo contains fields in which we may cache pointers to
 * EquivalenceClasses for the left and right inputs of the mergeclause.
 * (If the mergeclause is a true equivalence clause these will be the
 * same EquivalenceClass, otherwise not.)  If the mergeclause is either
 * used to generate an EquivalenceClass, or derived from an EquivalenceClass,
 * then it's easy to set up the left_ec and right_ec members --- otherwise,
 * this function should be called to set them up.  We will generate new
 * EquivalenceClauses if necessary to represent the mergeclause's left and
 * right sides.
 *
 * Note this is called before EC merging is complete, so the links won't
 * necessarily point to canonical ECs.  Before they are actually used for
 * anything, update_mergeclause_eclasses must be called to ensure that
 * they've been updated to point to canonical ECs.
 */
pub unsafe fn initialize_mergeclause_eclasses(
    root: *mut PlannerInfo,
    restrictinfo: *mut RestrictInfo,
) {
    let clause: *mut Expr = (*restrictinfo).clause;
    let mut lefttype: Oid = 0;
    let mut righttype: Oid = 0;

    /* Should be a mergeclause ... */
    Assert!((*restrictinfo).mergeopfamilies != NIL);
    /* ... with links not yet set */
    Assert!((*restrictinfo).left_ec.is_null());
    Assert!((*restrictinfo).right_ec.is_null());

    /* Need the declared input types of the operator */
    op_input_types(
        (*(clause as *mut OpExpr)).opno,
        &raw mut lefttype,
        &raw mut righttype,
    );

    /* Find or create a matching EquivalenceClass for each side */
    (*restrictinfo).left_ec = get_eclass_for_sort_expr(
        root,
        get_leftop(clause) as *mut Expr,
        (*restrictinfo).mergeopfamilies,
        lefttype,
        (*(clause as *mut OpExpr)).inputcollid,
        0,
        core::ptr::null_mut(),
        true,
    );
    (*restrictinfo).right_ec = get_eclass_for_sort_expr(
        root,
        get_rightop(clause) as *mut Expr,
        (*restrictinfo).mergeopfamilies,
        righttype,
        (*(clause as *mut OpExpr)).inputcollid,
        0,
        core::ptr::null_mut(),
        true,
    );
}

/*
 * update_mergeclause_eclasses
 *		Make the cached EquivalenceClass links valid in a mergeclause
 *		restrictinfo.
 *
 * These pointers should have been set by process_equivalence or
 * initialize_mergeclause_eclasses, but they might have been set to
 * non-canonical ECs that got merged later.  Chase up to the canonical
 * merged parent if so.
 */
pub unsafe fn update_mergeclause_eclasses(
    _root: *mut PlannerInfo,
    restrictinfo: *mut RestrictInfo,
) {
    /* Should be a merge clause ... */
    Assert!((*restrictinfo).mergeopfamilies != NIL);
    /* ... with pointers already set */
    Assert!(!(*restrictinfo).left_ec.is_null());
    Assert!(!(*restrictinfo).right_ec.is_null());

    /* Chase up to the top as needed */
    while !(*(*restrictinfo).left_ec).ec_merged.is_null() {
        (*restrictinfo).left_ec = (*(*restrictinfo).left_ec).ec_merged;
    }
    while !(*(*restrictinfo).right_ec).ec_merged.is_null() {
        (*restrictinfo).right_ec = (*(*restrictinfo).right_ec).ec_merged;
    }
}

/*
 * find_mergeclauses_for_outer_pathkeys
 *	  This routine attempts to find a list of mergeclauses that can be
 *	  used with a specified ordering for the join's outer relation.
 *	  If successful, it returns a list of mergeclauses.
 *
 * 'pathkeys' is a pathkeys list showing the ordering of an outer-rel path.
 * 'restrictinfos' is a list of mergejoinable restriction clauses for the
 *			join relation being formed, in no particular order.
 *
 * The restrictinfos must be marked (via outer_is_left) to show which side
 * of each clause is associated with the current outer path.  (See
 * select_mergejoin_clauses())
 *
 * The result is NIL if no merge can be done, else a maximal list of
 * usable mergeclauses (represented as a list of their restrictinfo nodes).
 * The list is ordered to match the pathkeys, as required for execution.
 */
pub unsafe fn find_mergeclauses_for_outer_pathkeys(
    root: *mut PlannerInfo,
    pathkeys: *mut List,
    restrictinfos: *mut List,
) -> *mut List {
    let mut mergeclauses: *mut List = NIL;

    /* make sure we have eclasses cached in the clauses */
    foreach!(i, restrictinfos, {
        let rinfo = lfirst(current_cell!(i)) as *mut RestrictInfo;

        update_mergeclause_eclasses(root, rinfo);
    });

    foreach!(i, pathkeys, {
        let pathkey = lfirst(current_cell!(i)) as *mut PathKey;
        let pathkey_ec: *mut EquivalenceClass = (*pathkey).pk_eclass;
        let mut matched_restrictinfos: *mut List = NIL;

        /*----------
         * A mergejoin clause matches a pathkey if it has the same EC.
         * If there are multiple matching clauses, take them all.  In plain
         * inner-join scenarios we expect only one match, because
         * equivalence-class processing will have removed any redundant
         * mergeclauses.  However, in outer-join scenarios there might be
         * multiple matches.  An example is
         *
         *	select * from a full join b
         *		on a.v1 = b.v1 and a.v2 = b.v2 and a.v1 = b.v2;
         *
         * Given the pathkeys ({a.v1}, {a.v2}) it is okay to return all three
         * clauses (in the order a.v1=b.v1, a.v1=b.v2, a.v2=b.v2) and indeed
         * we *must* do so or we will be unable to form a valid plan.
         *
         * We expect that the given pathkeys list is canonical, which means
         * no two members have the same EC, so it's not possible for this
         * code to enter the same mergeclause into the result list twice.
         *
         * It's possible that multiple matching clauses might have different
         * ECs on the other side, in which case the order we put them into our
         * result makes a difference in the pathkeys required for the inner
         * input rel.  However this routine hasn't got any info about which
         * order would be best, so we don't worry about that.
         *
         * It's also possible that the selected mergejoin clauses produce
         * a noncanonical ordering of pathkeys for the inner side, ie, we
         * might select clauses that reference b.v1, b.v2, b.v1 in that
         * order.  This is not harmful in itself, though it suggests that
         * the clauses are partially redundant.  Since the alternative is
         * to omit mergejoin clauses and thereby possibly fail to generate a
         * plan altogether, we live with it.  make_inner_pathkeys_for_merge()
         * has to delete duplicates when it constructs the inner pathkeys
         * list, and we also have to deal with such cases specially in
         * create_mergejoin_plan().
         *----------
         */
        foreach!(j, restrictinfos, {
            let rinfo = lfirst(current_cell!(j)) as *mut RestrictInfo;
            let clause_ec: *mut EquivalenceClass;

            clause_ec = if (*rinfo).outer_is_left {
                (*rinfo).left_ec
            } else {
                (*rinfo).right_ec
            };
            if clause_ec == pathkey_ec {
                matched_restrictinfos = lappend(matched_restrictinfos, rinfo as *mut c_void);
            }
        });

        /*
         * If we didn't find a mergeclause, we're done --- any additional
         * sort-key positions in the pathkeys are useless.  (But we can still
         * mergejoin if we found at least one mergeclause.)
         */
        if matched_restrictinfos == NIL {
            break;
        }

        /*
         * If we did find usable mergeclause(s) for this sort-key position,
         * add them to result list.
         */
        mergeclauses = list_concat(mergeclauses, matched_restrictinfos);
    });

    mergeclauses
}

/*
 * select_outer_pathkeys_for_merge
 *	  Builds a pathkey list representing a possible sort ordering
 *	  that can be used with the given mergeclauses.
 *
 * 'mergeclauses' is a list of RestrictInfos for mergejoin clauses
 *			that will be used in a merge join.
 * 'joinrel' is the join relation we are trying to construct.
 *
 * The restrictinfos must be marked (via outer_is_left) to show which side
 * of each clause is associated with the current outer path.  (See
 * select_mergejoin_clauses())
 *
 * Returns a pathkeys list that can be applied to the outer relation.
 *
 * Since we assume here that a sort is required, there is no particular use
 * in matching any available ordering of the outerrel.  (joinpath.c has an
 * entirely separate code path for considering sort-free mergejoins.)  Rather,
 * it's interesting to try to match, or match a prefix of the requested
 * query_pathkeys so that a second output sort may be avoided or an
 * incremental sort may be done instead.  We can get away with just a prefix
 * of the query_pathkeys when that prefix covers the entire join condition.
 * Failing that, we try to list "more popular" keys  (those with the most
 * unmatched EquivalenceClass peers) earlier, in hopes of making the resulting
 * ordering useful for as many higher-level mergejoins as possible.
 */
pub unsafe fn select_outer_pathkeys_for_merge(
    root: *mut PlannerInfo,
    mergeclauses: *mut List,
    joinrel: *mut RelOptInfo,
) -> *mut List {
    let mut pathkeys: *mut List = NIL;
    let nClauses: c_int = list_length(mergeclauses);
    let ecs: *mut *mut EquivalenceClass;
    let scores: *mut c_int;
    let mut necs: c_int;
    let mut j: c_int;

    /* Might have no mergeclauses */
    if nClauses == 0 {
        return NIL;
    }

    /*
     * Make arrays of the ECs used by the mergeclauses (dropping any
     * duplicates) and their "popularity" scores.
     */
    ecs = palloc(nClauses as usize * core::mem::size_of::<*mut EquivalenceClass>())
        as *mut *mut EquivalenceClass;
    scores = palloc(nClauses as usize * core::mem::size_of::<c_int>()) as *mut c_int;
    necs = 0;

    foreach!(lc, mergeclauses, {
        let rinfo = lfirst(current_cell!(lc)) as *mut RestrictInfo;
        let oeclass: *mut EquivalenceClass;
        let mut score: c_int;

        /* get the outer eclass */
        update_mergeclause_eclasses(root, rinfo);

        if (*rinfo).outer_is_left {
            oeclass = (*rinfo).left_ec;
        } else {
            oeclass = (*rinfo).right_ec;
        }

        /* reject duplicates */
        j = 0;
        while j < necs {
            if *ecs.add(j as usize) == oeclass {
                break;
            }
            j += 1;
        }
        if j < necs {
            continue;
        }

        /* compute score */
        score = 0;
        foreach!(lc2, (*oeclass).ec_members, {
            let em = lfirst(current_cell!(lc2)) as *mut EquivalenceMember;

            /* Child members should not exist in ec_members */
            Assert!(!(*em).em_is_child);

            /* Potential future join partner? */
            if !(*em).em_is_const && !bms_overlap((*em).em_relids, (*joinrel).relids) {
                score += 1;
            }
        });

        *ecs.add(necs as usize) = oeclass;
        *scores.add(necs as usize) = score;
        necs += 1;
    });

    /*
     * Find out if we have all the ECs mentioned in query_pathkeys; if so we
     * can generate a sort order that's also useful for final output. If we
     * only have a prefix of the query_pathkeys, and that prefix is the entire
     * join condition, then it's useful to use the prefix as the pathkeys as
     * this increases the chances that an incremental sort will be able to be
     * used by the upper planner.
     */
    if !(*root).query_pathkeys.is_null() {
        let mut matches: c_int = 0;

        let mut lc_end: *mut ListCell = core::ptr::null_mut();
        foreach!(lc, (*root).query_pathkeys, {
            let query_pathkey = lfirst(current_cell!(lc)) as *mut PathKey;
            let query_ec: *mut EquivalenceClass = (*query_pathkey).pk_eclass;

            j = 0;
            while j < necs {
                if *ecs.add(j as usize) == query_ec {
                    break; /* found match */
                }
                j += 1;
            }
            if j >= necs {
                lc_end = current_cell!(lc);
                break; /* didn't find match */
            }

            matches += 1;
        });
        /* if we got to the end of the list, we have them all */
        if lc_end.is_null() {
            /* copy query_pathkeys as starting point for our output */
            pathkeys = list_copy((*root).query_pathkeys);
            /* mark their ECs as already-emitted */
            foreach!(lc, (*root).query_pathkeys, {
                let query_pathkey = lfirst(current_cell!(lc)) as *mut PathKey;
                let query_ec: *mut EquivalenceClass = (*query_pathkey).pk_eclass;

                j = 0;
                while j < necs {
                    if *ecs.add(j as usize) == query_ec {
                        *scores.add(j as usize) = -1;
                        break;
                    }
                    j += 1;
                }
            });
        }
        /*
         * If we didn't match to all of the query_pathkeys, but did match to
         * all of the join clauses then we'll make use of these as partially
         * sorted input is better than nothing for the upper planner as it may
         * lead to incremental sorts instead of full sorts.
         */
        else if matches == nClauses {
            pathkeys = list_copy_head((*root).query_pathkeys, matches);

            /* we have all of the join pathkeys, so nothing more to do */
            pfree(ecs as *mut c_void);
            pfree(scores as *mut c_void);

            return pathkeys;
        }
    }

    /*
     * Add remaining ECs to the list in popularity order, using a default sort
     * ordering.  (We could use qsort() here, but the list length is usually
     * so small it's not worth it.)
     */
    loop {
        let mut best_j: c_int;
        let mut best_score: c_int;
        let ec: *mut EquivalenceClass;
        let pathkey: *mut PathKey;

        best_j = 0;
        best_score = *scores.add(0);
        j = 1;
        while j < necs {
            if *scores.add(j as usize) > best_score {
                best_j = j;
                best_score = *scores.add(j as usize);
            }
            j += 1;
        }
        if best_score < 0 {
            break; /* all done */
        }
        ec = *ecs.add(best_j as usize);
        *scores.add(best_j as usize) = -1;
        pathkey = make_canonical_pathkey(
            root,
            ec,
            linitial_oid((*ec).ec_opfamilies),
            COMPARE_LT,
            false,
        );
        /* can't be redundant because no duplicate ECs */
        Assert!(!pathkey_is_redundant(pathkey, pathkeys));
        pathkeys = lappend(pathkeys, pathkey as *mut c_void);
    }

    pfree(ecs as *mut c_void);
    pfree(scores as *mut c_void);

    pathkeys
}

/*
 * make_inner_pathkeys_for_merge
 *	  Builds a pathkey list representing the explicit sort order that
 *	  must be applied to an inner path to make it usable with the
 *	  given mergeclauses.
 *
 * 'mergeclauses' is a list of RestrictInfos for the mergejoin clauses
 *			that will be used in a merge join, in order.
 * 'outer_pathkeys' are the already-known canonical pathkeys for the outer
 *			side of the join.
 *
 * The restrictinfos must be marked (via outer_is_left) to show which side
 * of each clause is associated with the current outer path.  (See
 * select_mergejoin_clauses())
 *
 * Returns a pathkeys list that can be applied to the inner relation.
 *
 * Note that it is not this routine's job to decide whether sorting is
 * actually needed for a particular input path.  Assume a sort is necessary;
 * just make the keys, eh?
 */
pub unsafe fn make_inner_pathkeys_for_merge(
    root: *mut PlannerInfo,
    mergeclauses: *mut List,
    outer_pathkeys: *mut List,
) -> *mut List {
    let mut pathkeys: *mut List = NIL;
    let mut lastoeclass: *mut EquivalenceClass;
    let mut opathkey: *mut PathKey;
    let mut lop: *mut ListCell;

    lastoeclass = core::ptr::null_mut();
    opathkey = core::ptr::null_mut();
    lop = list_head(outer_pathkeys);

    foreach!(lc, mergeclauses, {
        let rinfo = lfirst(current_cell!(lc)) as *mut RestrictInfo;
        let oeclass: *mut EquivalenceClass;
        let ieclass: *mut EquivalenceClass;
        let pathkey: *mut PathKey;

        update_mergeclause_eclasses(root, rinfo);

        if (*rinfo).outer_is_left {
            oeclass = (*rinfo).left_ec;
            ieclass = (*rinfo).right_ec;
        } else {
            oeclass = (*rinfo).right_ec;
            ieclass = (*rinfo).left_ec;
        }

        /* outer eclass should match current or next pathkeys */
        /* we check this carefully for debugging reasons */
        if oeclass != lastoeclass {
            if lop.is_null() {
                elog!(ERROR, "too few pathkeys for mergeclauses");
            }
            opathkey = lfirst(lop) as *mut PathKey;
            lop = lnext(outer_pathkeys, lop);
            lastoeclass = (*opathkey).pk_eclass;
            if oeclass != lastoeclass {
                elog!(ERROR, "outer pathkeys do not match mergeclause");
            }
        }

        /*
         * Often, we'll have same EC on both sides, in which case the outer
         * pathkey is also canonical for the inner side, and we can skip a
         * useless search.
         */
        if ieclass == oeclass {
            pathkey = opathkey;
        } else {
            pathkey = make_canonical_pathkey(
                root,
                ieclass,
                (*opathkey).pk_opfamily,
                (*opathkey).pk_cmptype,
                (*opathkey).pk_nulls_first,
            );
        }

        /*
         * Don't generate redundant pathkeys (which can happen if multiple
         * mergeclauses refer to the same EC).  Because we do this, the output
         * pathkey list isn't necessarily ordered like the mergeclauses, which
         * complicates life for create_mergejoin_plan().  But if we didn't,
         * we'd have a noncanonical sort key list, which would be bad; for one
         * reason, it certainly wouldn't match any available sort order for
         * the input relation.
         */
        if !pathkey_is_redundant(pathkey, pathkeys) {
            pathkeys = lappend(pathkeys, pathkey as *mut c_void);
        }
    });

    pathkeys
}

/*
 * trim_mergeclauses_for_inner_pathkeys
 *	  This routine trims a list of mergeclauses to include just those that
 *	  work with a specified ordering for the join's inner relation.
 *
 * 'mergeclauses' is a list of RestrictInfos for mergejoin clauses for the
 *			join relation being formed, in an order known to work for the
 *			currently-considered sort ordering of the join's outer rel.
 * 'pathkeys' is a pathkeys list showing the ordering of an inner-rel path;
 *			it should be equal to, or a truncation of, the result of
 *			make_inner_pathkeys_for_merge for these mergeclauses.
 *
 * What we return will be a prefix of the given mergeclauses list.
 *
 * We need this logic because make_inner_pathkeys_for_merge's result isn't
 * necessarily in the same order as the mergeclauses.  That means that if we
 * consider an inner-rel pathkey list that is a truncation of that result,
 * we might need to drop mergeclauses even though they match a surviving inner
 * pathkey.  This happens when they are to the right of a mergeclause that
 * matches a removed inner pathkey.
 *
 * The mergeclauses must be marked (via outer_is_left) to show which side
 * of each clause is associated with the current outer path.  (See
 * select_mergejoin_clauses())
 */
pub unsafe fn trim_mergeclauses_for_inner_pathkeys(
    _root: *mut PlannerInfo,
    mergeclauses: *mut List,
    pathkeys: *mut List,
) -> *mut List {
    let mut new_mergeclauses: *mut List = NIL;
    let mut pathkey: *mut PathKey;
    let mut pathkey_ec: *mut EquivalenceClass;
    let mut matched_pathkey: bool;
    let mut lip: *mut ListCell;

    /* No pathkeys => no mergeclauses (though we don't expect this case) */
    if pathkeys == NIL {
        return NIL;
    }
    /* Initialize to consider first pathkey */
    lip = list_head(pathkeys);
    pathkey = lfirst(lip) as *mut PathKey;
    pathkey_ec = (*pathkey).pk_eclass;
    lip = lnext(pathkeys, lip);
    matched_pathkey = false;

    /* Scan mergeclauses to see how many we can use */
    foreach!(i, mergeclauses, {
        let rinfo = lfirst(current_cell!(i)) as *mut RestrictInfo;
        let clause_ec: *mut EquivalenceClass;

        /* Assume we needn't do update_mergeclause_eclasses again here */

        /* Check clause's inner-rel EC against current pathkey */
        clause_ec = if (*rinfo).outer_is_left {
            (*rinfo).right_ec
        } else {
            (*rinfo).left_ec
        };

        /* If we don't have a match, attempt to advance to next pathkey */
        if clause_ec != pathkey_ec {
            /* If we had no clauses matching this inner pathkey, must stop */
            if !matched_pathkey {
                break;
            }

            /* Advance to next inner pathkey, if any */
            if lip.is_null() {
                break;
            }
            pathkey = lfirst(lip) as *mut PathKey;
            pathkey_ec = (*pathkey).pk_eclass;
            lip = lnext(pathkeys, lip);
            matched_pathkey = false;
        }

        /* If mergeclause matches current inner pathkey, we can use it */
        if clause_ec == pathkey_ec {
            new_mergeclauses = lappend(new_mergeclauses, rinfo as *mut c_void);
            matched_pathkey = true;
        } else {
            /* Else, no hope of adding any more mergeclauses */
            break;
        }
    });

    new_mergeclauses
}

/****************************************************************************
 *		PATHKEY USEFULNESS CHECKS
 *
 * We only want to remember as many of the pathkeys of a path as have some
 * potential use, either for subsequent mergejoins or for meeting the query's
 * requested output ordering.  This ensures that add_path() won't consider
 * a path to have a usefully different ordering unless it really is useful.
 * These routines check for usefulness of given pathkeys.
 ****************************************************************************/

/*
 * pathkeys_useful_for_merging
 *		Count the number of pathkeys that may be useful for mergejoins
 *		above the given relation.
 *
 * We consider a pathkey potentially useful if it corresponds to the merge
 * ordering of either side of any joinclause for the rel.  This might be
 * overoptimistic, since joinclauses that require different other relations
 * might never be usable at the same time, but trying to be exact is likely
 * to be more trouble than it's worth.
 *
 * To avoid doubling the number of mergejoin paths considered, we would like
 * to consider only one of the two scan directions (ASC or DESC) as useful
 * for merging for any given target column.  The choice is arbitrary unless
 * one of the directions happens to match an ORDER BY key, in which case
 * that direction should be preferred, in hopes of avoiding a final sort step.
 * right_merge_direction() implements this heuristic.
 */
unsafe fn pathkeys_useful_for_merging(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    pathkeys: *mut List,
) -> c_int {
    let mut useful: c_int = 0;

    foreach!(i, pathkeys, {
        let pathkey = lfirst(current_cell!(i)) as *mut PathKey;
        let mut matched: bool = false;

        /* If "wrong" direction, not useful for merging */
        if !right_merge_direction(root, pathkey) {
            break;
        }

        /*
         * First look into the EquivalenceClass of the pathkey, to see if
         * there are any members not yet joined to the rel.  If so, it's
         * surely possible to generate a mergejoin clause using them.
         */
        if (*rel).has_eclass_joins
            && eclass_useful_for_merging(root, (*pathkey).pk_eclass, rel)
        {
            matched = true;
        } else {
            /*
             * Otherwise search the rel's joininfo list, which contains
             * non-EquivalenceClass-derivable join clauses that might
             * nonetheless be mergejoinable.
             */
            foreach!(j, (*rel).joininfo, {
                let restrictinfo = lfirst(current_cell!(j)) as *mut RestrictInfo;

                if (*restrictinfo).mergeopfamilies == NIL {
                    continue;
                }
                update_mergeclause_eclasses(root, restrictinfo);

                if (*pathkey).pk_eclass == (*restrictinfo).left_ec
                    || (*pathkey).pk_eclass == (*restrictinfo).right_ec
                {
                    matched = true;
                    break;
                }
            });
        }

        /*
         * If we didn't find a mergeclause, we're done --- any additional
         * sort-key positions in the pathkeys are useless.  (But we can still
         * mergejoin if we found at least one mergeclause.)
         */
        if matched {
            useful += 1;
        } else {
            break;
        }
    });

    useful
}

/*
 * right_merge_direction
 *		Check whether the pathkey embodies the preferred sort direction
 *		for merging its target column.
 */
unsafe fn right_merge_direction(root: *mut PlannerInfo, pathkey: *mut PathKey) -> bool {
    foreach!(l, (*root).query_pathkeys, {
        let query_pathkey = lfirst(current_cell!(l)) as *mut PathKey;

        if (*pathkey).pk_eclass == (*query_pathkey).pk_eclass
            && (*pathkey).pk_opfamily == (*query_pathkey).pk_opfamily
        {
            /*
             * Found a matching query sort column.  Prefer this pathkey's
             * direction iff it matches.  Note that we ignore pk_nulls_first,
             * which means that a sort might be needed anyway ... but we still
             * want to prefer only one of the two possible directions, and we
             * might as well use this one.
             */
            return (*pathkey).pk_cmptype == (*query_pathkey).pk_cmptype;
        }
    });

    /* If no matching ORDER BY request, prefer the ASC direction */
    (*pathkey).pk_cmptype == COMPARE_LT
}

/*
 * pathkeys_useful_for_ordering
 *		Count the number of pathkeys that are useful for meeting the
 *		query's requested output ordering.
 *
 * Because we the have the possibility of incremental sort, a prefix list of
 * keys is potentially useful for improving the performance of the requested
 * ordering. Thus we return 0, if no valuable keys are found, or the number
 * of leading keys shared by the list and the requested ordering..
 */
unsafe fn pathkeys_useful_for_ordering(root: *mut PlannerInfo, pathkeys: *mut List) -> c_int {
    let mut n_common_pathkeys: c_int = 0;

    pathkeys_count_contained_in((*root).query_pathkeys, pathkeys, &raw mut n_common_pathkeys);

    n_common_pathkeys
}

/*
 * pathkeys_useful_for_grouping
 *		Count the number of pathkeys that are useful for grouping (instead of
 *		explicit sort)
 *
 * Group pathkeys could be reordered to benefit from the ordering. The
 * ordering may not be "complete" and may require incremental sort, but that's
 * fine. So we simply count prefix pathkeys with a matching group key, and
 * stop once we find the first pathkey without a match.
 *
 * So e.g. with pathkeys (a,b,c) and group keys (a,b,e) this determines (a,b)
 * pathkeys are useful for grouping, and we might do incremental sort to get
 * path ordered by (a,b,e).
 *
 * This logic is necessary to retain paths with ordering not matching grouping
 * keys directly, without the reordering.
 *
 * Returns the length of pathkey prefix with matching group keys.
 */
unsafe fn pathkeys_useful_for_grouping(root: *mut PlannerInfo, pathkeys: *mut List) -> c_int {
    let mut n: c_int = 0;

    /* no special ordering requested for grouping */
    if (*root).group_pathkeys == NIL {
        return 0;
    }

    /* walk the pathkeys and search for matching group key */
    foreach!(key, pathkeys, {
        let pathkey = lfirst(current_cell!(key)) as *mut PathKey;

        /* no matching group key, we're done */
        if !list_member_ptr((*root).group_pathkeys, pathkey as *const c_void) {
            break;
        }

        n += 1;
    });

    n
}

/*
 * pathkeys_useful_for_distinct
 *		Count the number of pathkeys that are useful for DISTINCT or DISTINCT
 *		ON clause.
 *
 * DISTINCT keys could be reordered to benefit from the given pathkey list.  As
 * with pathkeys_useful_for_grouping, we return the number of leading keys in
 * the list that are shared by the distinctClause pathkeys.
 */
unsafe fn pathkeys_useful_for_distinct(root: *mut PlannerInfo, pathkeys: *mut List) -> c_int {
    let mut n_common_pathkeys: c_int;

    /*
     * distinct_pathkeys may have become empty if all of the pathkeys were
     * determined to be redundant.  Return 0 in this case.
     */
    if (*root).distinct_pathkeys == NIL {
        return 0;
    }

    /* walk the pathkeys and search for matching DISTINCT key */
    n_common_pathkeys = 0;
    foreach_node!(PathKey, T_PathKey, pathkey, pathkeys, {
        /* no matching DISTINCT key, we're done */
        if !list_member_ptr((*root).distinct_pathkeys, pathkey as *const c_void) {
            break;
        }

        n_common_pathkeys += 1;
    });

    n_common_pathkeys
}

/*
 * pathkeys_useful_for_setop
 *		Count the number of leading common pathkeys root's 'setop_pathkeys' in
 *		'pathkeys'.
 */
unsafe fn pathkeys_useful_for_setop(root: *mut PlannerInfo, pathkeys: *mut List) -> c_int {
    let mut n_common_pathkeys: c_int = 0;

    pathkeys_count_contained_in((*root).setop_pathkeys, pathkeys, &raw mut n_common_pathkeys);

    n_common_pathkeys
}

/*
 * truncate_useless_pathkeys
 *		Shorten the given pathkey list to just the useful pathkeys.
 */
pub unsafe fn truncate_useless_pathkeys(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    pathkeys: *mut List,
) -> *mut List {
    let mut nuseful: c_int;
    let mut nuseful2: c_int;

    nuseful = pathkeys_useful_for_merging(root, rel, pathkeys);
    nuseful2 = pathkeys_useful_for_ordering(root, pathkeys);
    if nuseful2 > nuseful {
        nuseful = nuseful2;
    }
    nuseful2 = pathkeys_useful_for_grouping(root, pathkeys);
    if nuseful2 > nuseful {
        nuseful = nuseful2;
    }
    nuseful2 = pathkeys_useful_for_distinct(root, pathkeys);
    if nuseful2 > nuseful {
        nuseful = nuseful2;
    }
    nuseful2 = pathkeys_useful_for_setop(root, pathkeys);
    if nuseful2 > nuseful {
        nuseful = nuseful2;
    }

    /*
     * Note: not safe to modify input list destructively, but we can avoid
     * copying the list if we're not actually going to change it
     */
    if nuseful == 0 {
        NIL
    } else if nuseful == list_length(pathkeys) {
        pathkeys
    } else {
        list_copy_head(pathkeys, nuseful)
    }
}

/*
 * has_useful_pathkeys
 *		Detect whether the specified rel could have any pathkeys that are
 *		useful according to truncate_useless_pathkeys().
 *
 * This is a cheap test that lets us skip building pathkeys at all in very
 * simple queries.  It's OK to err in the direction of returning "true" when
 * there really aren't any usable pathkeys, but erring in the other direction
 * is bad --- so keep this in sync with the routines above!
 *
 * We could make the test more complex, for example checking to see if any of
 * the joinclauses are really mergejoinable, but that likely wouldn't win
 * often enough to repay the extra cycles.  Queries with neither a join nor
 * a sort are reasonably common, though, so this much work seems worthwhile.
 */
pub unsafe fn has_useful_pathkeys(root: *mut PlannerInfo, rel: *mut RelOptInfo) -> bool {
    if (*rel).joininfo != NIL || (*rel).has_eclass_joins {
        return true; /* might be able to use pathkeys for merging */
    }
    if (*root).group_pathkeys != NIL {
        return true; /* might be able to use pathkeys for grouping */
    }
    if (*root).query_pathkeys != NIL {
        return true; /* might be able to use them for ordering */
    }
    false /* definitely useless */
}

/* ----------------------------------------------------------------------------
 * Local nodeFuncs.h get_leftop/get_rightop helpers (nodeFuncs.c not yet ported
 * as a unit; match the private copies in clausesel.rs).
 * ------------------------------------------------------------------------- */

/// `get_leftop(clause)` -- left arg of a binary opclause.
#[inline]
unsafe fn get_leftop(clause: *const Expr) -> *mut Node {
    let expr = clause as *const OpExpr;
    if !(*expr).args.is_null() {
        linitial((*expr).args) as *mut Node
    } else {
        core::ptr::null_mut()
    }
}

/// `get_rightop(clause)` -- right arg of a binary opclause (NULL if unary).
#[inline]
unsafe fn get_rightop(clause: *const Expr) -> *mut Node {
    let expr = clause as *const OpExpr;
    if list_length((*expr).args) >= 2 {
        crate::nodes::pg_list::lsecond((*expr).args) as *mut Node
    } else {
        core::ptr::null_mut()
    }
}
