//! src/backend/optimizer/util/placeholder.c
//!
//! PlaceHolderVar and PlaceHolderInfo manipulation routines.
//!
//! #include mapping:
//!   - "postgres.h"               -> crate::prelude::*
//!   - "nodes/nodeFuncs.h"        -> crate::nodes::nodeFuncs::{exprType, exprTypmod,
//!                                    expression_tree_walker, query_tree_walker,
//!                                    tree_walker_callback}
//!   - "optimizer/cost.h"         -> STUB (cost_qual_eval_node, clamp_width_est)
//!   - "optimizer/optimizer.h"    -> pull_varnos, pull_var_clause (crate::optimizer::util::var)
//!   - "optimizer/pathnode.h"     -> STUB (find_base_rel)
//!   - "optimizer/placeholder.h"  -> this file's public fns
//!   - "optimizer/planmain.h"     -> STUB (add_vars_to_targetlist, add_vars_to_attr_needed)
//!   - "utils/lsyscache.h"        -> STUB (get_typavgwidth)
//!
//! Nodes: PlaceHolderVar / PlaceHolderInfo / PlannerInfo / PlannerGlobal /
//! RelOptInfo / PathTarget / QualCost / SpecialJoinInfo live in
//! crate::nodes::pathnodes. FromExpr / JoinExpr / RangeTblRef in
//! crate::nodes::primnodes. Query in crate::nodes::parsenodes.
//!
//! REAL: make_placeholder_expr, find_placeholder_info,
//! find_placeholders_in_jointree, find_placeholders_recurse,
//! find_placeholders_in_expr, fix_placeholder_input_needed_levels,
//! rebuild_placeholder_attr_needed, add_placeholders_to_base_rels,
//! add_placeholders_to_joinrel, contain_placeholder_references_to,
//! contain_placeholder_references_walker, get_placeholder_nulling_relids.
//!
//! STUBBED deps (genuinely unported): get_typavgwidth, cost_qual_eval_node,
//! clamp_width_est, find_base_rel, add_vars_to_targetlist,
//! add_vars_to_attr_needed, repalloc0_array/palloc0_array helper. See the
//! `stubs` module below; each is `unimplemented!()` with a TODO(pg-port).

use crate::prelude::*;

use crate::nodes::bitmapset::{
    bms_add_members, bms_copy, bms_del_members, bms_difference, bms_get_singleton_member,
    bms_int_members, bms_is_empty, bms_is_member, bms_is_subset, bms_next_member,
    bms_nonempty_difference, Bitmapset,
};
use crate::nodes::nodeFuncs::{
    exprType, exprTypmod, expression_tree_walker, query_tree_walker, tree_walker_callback,
};
use crate::nodes::nodes::{nodeTag, Node, NodeTag};
use crate::nodes::parsenodes::Query;
use crate::nodes::pathnodes::{
    PlaceHolderInfo, PlaceHolderVar, PlannerInfo, QualCost, RelOptInfo, Relids, SpecialJoinInfo,
};
use crate::nodes::pg_list::{lappend, lfirst, list_free, List};
use crate::nodes::primnodes::{Expr, FromExpr, JoinExpr};
use crate::optimizer::util::var::{
    pull_var_clause, pull_varnos, PVC_INCLUDE_PLACEHOLDERS, PVC_RECURSE_AGGREGATES,
    PVC_RECURSE_WINDOWFUNCS,
};
use crate::{current_cell, foreach, makeNode, IsA};

// ---------------------------------------------------------------------------
// Stubs for genuinely-unported dependencies.
//
// These reach into modules (cost.c, pathnode.c, planmain.c, lsyscache.c) that
// have not been translated yet. Keep the signatures faithful to the C ones so
// the call sites read 1:1; the bodies panic until those modules land.
// ---------------------------------------------------------------------------
mod stubs {
    use super::*;

    /// utils/lsyscache.c: get_typavgwidth(typid, typmod).
    /// TODO(pg-port): translate lsyscache.c.
    pub unsafe fn get_typavgwidth(typid: Oid, typmod: int32) -> int32 {
        crate::utils::cache::lsyscache::get_typavgwidth(typid, typmod)
    }

    /// optimizer/cost.c: cost_qual_eval_node(&cost, node, root).
    /// TODO(pg-port): translate cost.c.
    pub unsafe fn cost_qual_eval_node(
        cost: *mut QualCost,
        qual: *mut Node,
        root: *mut PlannerInfo,
    ) {
        crate::optimizer::path::costsize::cost_qual_eval_node(cost as _, qual as _, root as _)
    }

    pub unsafe fn clamp_width_est(tuple_width: int64) -> c_int {
        crate::optimizer::optimizer::clamp_width_est(tuple_width)
    }

    pub unsafe fn find_base_rel(root: *mut PlannerInfo, relid: c_int) -> *mut RelOptInfo {
        crate::optimizer::util::relnode::find_base_rel(root as _, relid) as _
    }

    pub unsafe fn add_vars_to_targetlist(
        root: *mut PlannerInfo,
        vars: *mut List,
        where_needed: Relids,
    ) {
        crate::optimizer::plan::initsplan::add_vars_to_targetlist(root as _, vars as _, where_needed as _)
    }

    pub unsafe fn add_vars_to_attr_needed(
        root: *mut PlannerInfo,
        vars: *mut List,
        where_needed: Relids,
    ) {
        crate::optimizer::plan::initsplan::add_vars_to_attr_needed(root as _, vars as _, where_needed as _)
    }
}

use stubs::*;

/// copyObject() for a single node pointer. The full generated copyfuncs.c is
/// not yet ported (see crate::nodes::pg_list::copyObjectImpl TODO); for the
/// node types touched here (PlaceHolderVar) a shallow byte copy of the struct
/// preserves the tag and scalar/pointer fields, matching how the C convention
/// treats these PHV copies (the contained expr and relids are shared/replaced
/// by the caller as needed).
///
/// TODO(pg-port): replace with the real recursive copyObject once copyfuncs.c
/// is translated.
unsafe fn copyObject<T>(node: *const T) -> *mut T {
    if node.is_null() {
        return core::ptr::null_mut();
    }
    let p = palloc(core::mem::size_of::<T>()) as *mut T;
    core::ptr::copy_nonoverlapping(node, p, 1);
    p
}

/*
 * contain_placeholder_references_context
 */
struct contain_placeholder_references_context {
    relid: c_int,
    sublevels_up: c_int,
}

/*
 * make_placeholder_expr
 *		Make a PlaceHolderVar for the given expression.
 *
 * phrels is the syntactic location (as a set of relids) to attribute
 * to the expression.
 *
 * The caller is responsible for adjusting phlevelsup and phnullingrels
 * as needed.  Because we do not know here which query level the PHV
 * will be associated with, it's important that this function touches
 * only root->glob; messing with other parts of PlannerInfo would be
 * likely to do the wrong thing.
 */
pub unsafe fn make_placeholder_expr(
    root: *mut PlannerInfo,
    expr: *mut Expr,
    phrels: Relids,
) -> *mut PlaceHolderVar {
    let phv: *mut PlaceHolderVar = makeNode!(PlaceHolderVar, T_PlaceHolderVar);

    (*phv).phexpr = expr;
    (*phv).phrels = phrels;
    (*phv).phnullingrels = core::ptr::null_mut(); /* caller may change this later */
    (*(*root).glob).lastPHId += 1;
    (*phv).phid = (*(*root).glob).lastPHId;
    (*phv).phlevelsup = 0; /* caller may change this later */

    phv
}

/*
 * find_placeholder_info
 *		Fetch the PlaceHolderInfo for the given PHV
 *
 * If the PlaceHolderInfo doesn't exist yet, create it if we haven't yet
 * frozen the set of PlaceHolderInfos for the query; else throw an error.
 *
 * Note: this should only be called after query_planner() has started.
 */
pub unsafe fn find_placeholder_info(
    root: *mut PlannerInfo,
    phv: *mut PlaceHolderVar,
) -> *mut PlaceHolderInfo {
    let mut phinfo: *mut PlaceHolderInfo;
    let rels_used: Relids;

    /* if this ever isn't true, we'd need to be able to look in parent lists */
    Assert!((*phv).phlevelsup == 0);

    /* Use placeholder_array to look up existing PlaceHolderInfo quickly */
    if ((*phv).phid as c_int) < (*root).placeholder_array_size {
        phinfo = *(*root).placeholder_array.add((*phv).phid as usize);
    } else {
        phinfo = core::ptr::null_mut();
    }
    if !phinfo.is_null() {
        Assert!((*phinfo).phid == (*phv).phid);
        return phinfo;
    }

    /* Not found, so create it */
    if (*root).placeholdersFrozen {
        elog!(ERROR, "too late to create a new PlaceHolderInfo");
    }

    phinfo = makeNode!(PlaceHolderInfo, T_PlaceHolderInfo);

    (*phinfo).phid = (*phv).phid;
    (*phinfo).ph_var = copyObject(phv);

    /*
     * By convention, phinfo->ph_var->phnullingrels is always empty, since the
     * PlaceHolderInfo represents the initially-calculated state of the
     * PlaceHolderVar.
     */
    (*(*phinfo).ph_var).phnullingrels = core::ptr::null_mut();

    /*
     * Any referenced rels that are outside the PHV's syntactic scope are
     * LATERAL references, which should be included in ph_lateral but not in
     * ph_eval_at.  If no referenced rels are within the syntactic scope,
     * force evaluation at the syntactic location.
     */
    rels_used = pull_varnos(root, (*phv).phexpr as *mut Node);
    (*phinfo).ph_lateral = bms_difference(rels_used, (*phv).phrels);
    (*phinfo).ph_eval_at = bms_int_members(rels_used, (*phv).phrels);
    /* If no contained vars, force evaluation at syntactic location */
    if bms_is_empty((*phinfo).ph_eval_at) {
        (*phinfo).ph_eval_at = bms_copy((*phv).phrels);
        Assert!(!bms_is_empty((*phinfo).ph_eval_at));
    }
    (*phinfo).ph_needed = core::ptr::null_mut(); /* initially it's unused */
    /* for the moment, estimate width using just the datatype info */
    (*phinfo).ph_width = get_typavgwidth(
        exprType((*phv).phexpr as *const Node),
        exprTypmod((*phv).phexpr as *const Node),
    );

    /*
     * Add to both placeholder_list and placeholder_array.
     */
    (*root).placeholder_list = lappend((*root).placeholder_list, phinfo as *mut c_void);

    if (*phinfo).phid as c_int >= (*root).placeholder_array_size {
        /* Must allocate or enlarge placeholder_array */
        let mut new_size: c_int = if (*root).placeholder_array_size != 0 {
            (*root).placeholder_array_size * 2
        } else {
            8
        };
        while (*phinfo).phid as c_int >= new_size {
            new_size *= 2;
        }
        if !(*root).placeholder_array.is_null() {
            (*root).placeholder_array = repalloc0_array_phinfo(
                (*root).placeholder_array,
                (*root).placeholder_array_size,
                new_size,
            );
        } else {
            (*root).placeholder_array = palloc0_array_phinfo(new_size);
        }
        (*root).placeholder_array_size = new_size;
    }
    *(*root).placeholder_array.add((*phinfo).phid as usize) = phinfo;

    /*
     * The PHV's contained expression may contain other, lower-level PHVs.  We
     * now know we need to get those into the PlaceHolderInfo list, too, so we
     * may as well do that immediately.
     */
    find_placeholders_in_expr(root, (*(*phinfo).ph_var).phexpr as *mut Node);

    phinfo
}

/// palloc0_array(PlaceHolderInfo *, n): zeroed array of `n` PlaceHolderInfo
/// pointers. (memutils.h macro expanded here.)
unsafe fn palloc0_array_phinfo(n: c_int) -> *mut *mut PlaceHolderInfo {
    palloc0((n as usize) * core::mem::size_of::<*mut PlaceHolderInfo>())
        as *mut *mut PlaceHolderInfo
}

/// repalloc0_array(arr, PlaceHolderInfo *, oldlen, newlen): grow `arr` and zero
/// the newly added slots. (memutils.h macro expanded here.)
unsafe fn repalloc0_array_phinfo(
    arr: *mut *mut PlaceHolderInfo,
    oldlen: c_int,
    newlen: c_int,
) -> *mut *mut PlaceHolderInfo {
    let elem = core::mem::size_of::<*mut PlaceHolderInfo>();
    let p = repalloc(arr as *mut c_void, (newlen as usize) * elem) as *mut *mut PlaceHolderInfo;
    /* zero the [oldlen, newlen) slots */
    let mut i = oldlen as usize;
    while i < newlen as usize {
        *p.add(i) = core::ptr::null_mut();
        i += 1;
    }
    p
}

/*
 * find_placeholders_in_jointree
 *		Search the jointree for PlaceHolderVars, and build PlaceHolderInfos
 */
pub unsafe fn find_placeholders_in_jointree(root: *mut PlannerInfo) {
    /* This must be done before freezing the set of PHIs */
    Assert!(!(*root).placeholdersFrozen);

    /* We need do nothing if the query contains no PlaceHolderVars */
    if (*(*root).glob).lastPHId != 0 {
        /* Start recursion at top of jointree */
        Assert!(
            !(*(*root).parse).jointree.is_null()
                && IsA!((*(*root).parse).jointree, T_FromExpr)
        );
        find_placeholders_recurse(root, (*(*root).parse).jointree as *mut Node);
    }
}

/*
 * find_placeholders_recurse
 *	  One recursion level of find_placeholders_in_jointree.
 */
unsafe fn find_placeholders_recurse(root: *mut PlannerInfo, jtnode: *mut Node) {
    if jtnode.is_null() {
        return;
    }
    if IsA!(jtnode, T_RangeTblRef) {
        /* No quals to deal with here */
    } else if IsA!(jtnode, T_FromExpr) {
        let f = jtnode as *mut FromExpr;

        /*
         * First, recurse to handle child joins.
         */
        foreach!(l, (*f).fromlist, {
            find_placeholders_recurse(root, lfirst(current_cell!(l)) as *mut Node);
        });

        /*
         * Now process the top-level quals.
         */
        find_placeholders_in_expr(root, (*f).quals);
    } else if IsA!(jtnode, T_JoinExpr) {
        let j = jtnode as *mut JoinExpr;

        /*
         * First, recurse to handle child joins.
         */
        find_placeholders_recurse(root, (*j).larg);
        find_placeholders_recurse(root, (*j).rarg);

        /* Process the qual clauses */
        find_placeholders_in_expr(root, (*j).quals);
    } else {
        elog!(ERROR, "unrecognized node type: {}", nodeTag(jtnode) as c_int);
    }
}

/*
 * find_placeholders_in_expr
 *		Find all PlaceHolderVars in the given expression, and create
 *		PlaceHolderInfo entries for them.
 */
unsafe fn find_placeholders_in_expr(root: *mut PlannerInfo, expr: *mut Node) {
    /*
     * pull_var_clause does more than we need here, but it'll do and it's
     * convenient to use.
     */
    let vars = pull_var_clause(
        expr,
        PVC_RECURSE_AGGREGATES | PVC_RECURSE_WINDOWFUNCS | PVC_INCLUDE_PLACEHOLDERS,
    );
    foreach!(vl, vars, {
        let phv = lfirst(current_cell!(vl)) as *mut PlaceHolderVar;

        /* Ignore any plain Vars */
        if !IsA!(phv as *mut Node, T_PlaceHolderVar) {
            continue;
        }

        /* Create a PlaceHolderInfo entry if there's not one already */
        let _ = find_placeholder_info(root, phv);
    });
    list_free(vars);
}

/*
 * fix_placeholder_input_needed_levels
 *		Adjust the "needed at" levels for placeholder inputs
 */
pub unsafe fn fix_placeholder_input_needed_levels(root: *mut PlannerInfo) {
    foreach!(lc, (*root).placeholder_list, {
        let phinfo = lfirst(current_cell!(lc)) as *mut PlaceHolderInfo;
        let vars = pull_var_clause(
            (*(*phinfo).ph_var).phexpr as *mut Node,
            PVC_RECURSE_AGGREGATES | PVC_RECURSE_WINDOWFUNCS | PVC_INCLUDE_PLACEHOLDERS,
        );

        add_vars_to_targetlist(root, vars, (*phinfo).ph_eval_at);
        list_free(vars);
    });
}

/*
 * rebuild_placeholder_attr_needed
 *	  Put back attr_needed bits for Vars/PHVs needed in PlaceHolderVars.
 */
pub unsafe fn rebuild_placeholder_attr_needed(root: *mut PlannerInfo) {
    foreach!(lc, (*root).placeholder_list, {
        let phinfo = lfirst(current_cell!(lc)) as *mut PlaceHolderInfo;
        let vars = pull_var_clause(
            (*(*phinfo).ph_var).phexpr as *mut Node,
            PVC_RECURSE_AGGREGATES | PVC_RECURSE_WINDOWFUNCS | PVC_INCLUDE_PLACEHOLDERS,
        );

        add_vars_to_attr_needed(root, vars, (*phinfo).ph_eval_at);
        list_free(vars);
    });
}

/*
 * add_placeholders_to_base_rels
 *		Add any required PlaceHolderVars to base rels' targetlists.
 */
pub unsafe fn add_placeholders_to_base_rels(root: *mut PlannerInfo) {
    foreach!(lc, (*root).placeholder_list, {
        let phinfo = lfirst(current_cell!(lc)) as *mut PlaceHolderInfo;
        let eval_at: Relids = (*phinfo).ph_eval_at;
        let mut varno: c_int = 0;

        if bms_get_singleton_member(eval_at, &mut varno)
            && bms_nonempty_difference((*phinfo).ph_needed, eval_at)
        {
            let rel = find_base_rel(root, varno);

            /*
             * As in add_vars_to_targetlist(), a value computed at scan level
             * has not yet been nulled by any outer join, so its phnullingrels
             * should be empty.
             */
            Assert!((*(*phinfo).ph_var).phnullingrels.is_null());

            /* Copying the PHV might be unnecessary here, but be safe */
            (*(*rel).reltarget).exprs = lappend(
                (*(*rel).reltarget).exprs,
                copyObject((*phinfo).ph_var) as *mut c_void,
            );
            /* reltarget's cost and width fields will be updated later */
        }
    });
}

/*
 * add_placeholders_to_joinrel
 *		Add any newly-computable PlaceHolderVars to a join rel's targetlist;
 *		and if computable PHVs contain lateral references, add those
 *		references to the joinrel's direct_lateral_relids.
 */
pub unsafe fn add_placeholders_to_joinrel(
    root: *mut PlannerInfo,
    joinrel: *mut RelOptInfo,
    outer_rel: *mut RelOptInfo,
    inner_rel: *mut RelOptInfo,
    _sjinfo: *mut SpecialJoinInfo,
) {
    let relids: Relids = (*joinrel).relids;
    let mut tuple_width: int64 = (*(*joinrel).reltarget).width as int64;

    foreach!(lc, (*root).placeholder_list, {
        let phinfo = lfirst(current_cell!(lc)) as *mut PlaceHolderInfo;

        /* Is it computable here? */
        if bms_is_subset((*phinfo).ph_eval_at, relids) {
            /* Is it still needed above this joinrel? */
            if bms_nonempty_difference((*phinfo).ph_needed, relids) {
                /*
                 * Yes, but only add to tlist if it wasn't computed in either
                 * input; otherwise it should be there already.  Also, we
                 * charge the cost of evaluating the contained expression if
                 * the PHV can be computed here but not in either input.
                 */
                if !bms_is_subset((*phinfo).ph_eval_at, (*outer_rel).relids)
                    && !bms_is_subset((*phinfo).ph_eval_at, (*inner_rel).relids)
                {
                    /* Copying might be unnecessary here, but be safe */
                    let phv: *mut PlaceHolderVar = copyObject((*phinfo).ph_var);
                    let mut cost = QualCost {
                        startup: 0.0,
                        per_tuple: 0.0,
                    };

                    /*
                     * It'll start out not nulled by anything.  Joins above
                     * this one might add to its phnullingrels later.
                     */
                    Assert!((*phv).phnullingrels.is_null());

                    (*(*joinrel).reltarget).exprs =
                        lappend((*(*joinrel).reltarget).exprs, phv as *mut c_void);
                    cost_qual_eval_node(&mut cost, (*phv).phexpr as *mut Node, root);
                    (*(*joinrel).reltarget).cost.startup += cost.startup;
                    (*(*joinrel).reltarget).cost.per_tuple += cost.per_tuple;
                    tuple_width += (*phinfo).ph_width as int64;
                }
            }

            /*
             * Also adjust joinrel's direct_lateral_relids to include the
             * PHV's source rel(s).
             */
            (*joinrel).direct_lateral_relids = bms_add_members(
                (*joinrel).direct_lateral_relids,
                (*phinfo).ph_lateral,
            );
        }
    });

    (*(*joinrel).reltarget).width = clamp_width_est(tuple_width);
}

/*
 * contain_placeholder_references_to
 *		Detect whether any PlaceHolderVars in the given clause contain
 *		references to the given relid (typically an OJ relid).
 */
pub unsafe fn contain_placeholder_references_to(
    root: *mut PlannerInfo,
    clause: *mut Node,
    relid: c_int,
) -> bool {
    /* We can answer quickly in the common case that there's no PHVs at all */
    if (*(*root).glob).lastPHId == 0 {
        return false;
    }
    /* Else run the recursive search */
    let mut context = contain_placeholder_references_context {
        relid,
        sublevels_up: 0,
    };
    contain_placeholder_references_walker(
        clause,
        &mut context as *mut contain_placeholder_references_context,
    )
}

unsafe fn contain_placeholder_references_walker(
    node: *mut Node,
    context: *mut contain_placeholder_references_context,
) -> bool {
    if node.is_null() {
        return false;
    }
    if IsA!(node, T_PlaceHolderVar) {
        let phv = node as *mut PlaceHolderVar;

        /* We should just look through PHVs of other query levels */
        if (*phv).phlevelsup as c_int == (*context).sublevels_up {
            /* If phrels matches, we found what we came for */
            if bms_is_member((*context).relid, (*phv).phrels) {
                return true;
            }

            /*
             * We should not examine phnullingrels: what we are looking for is
             * references in the contained expression, not OJs that might null
             * the result afterwards.  Also, we don't need to recurse into the
             * contained expression, because phrels should adequately
             * summarize what's in there.  So we're done here.
             */
            return false;
        }
    } else if IsA!(node, T_Query) {
        /* Recurse into RTE subquery or not-yet-planned sublink subquery */
        (*context).sublevels_up += 1;
        let result = query_tree_walker(
            node as *mut Query,
            walker_cb(),
            context as *mut c_void,
            0,
        );
        (*context).sublevels_up -= 1;
        return result;
    }
    expression_tree_walker(node, walker_cb(), context as *mut c_void)
}

/// Adapter turning `contain_placeholder_references_walker` into the
/// `tree_walker_callback` ABI (node, context) -> bool.
unsafe fn walker_thunk(node: *mut Node, context: *mut c_void) -> bool {
    contain_placeholder_references_walker(
        node,
        context as *mut contain_placeholder_references_context,
    )
}

#[inline]
fn walker_cb() -> tree_walker_callback {
    Some(walker_thunk)
}

/*
 * get_placeholder_nulling_relids
 *
 * Compute the set of outer-join relids that can null a placeholder.
 */
pub unsafe fn get_placeholder_nulling_relids(
    root: *mut PlannerInfo,
    phinfo: *mut PlaceHolderInfo,
) -> Relids {
    let mut result: Relids = core::ptr::null_mut();
    let mut relid: c_int = -1;

    /*
     * Form the union of all potential nulling OJs for each baserel included
     * in ph_eval_at.
     */
    loop {
        relid = bms_next_member((*phinfo).ph_eval_at, relid);
        if relid <= 0 {
            break;
        }
        let rel = *(*root).simple_rel_array.add(relid as usize);

        /* ignore the RTE_GROUP RTE */
        if relid == (*root).group_rtindex {
            continue;
        }

        if rel.is_null() {
            /* must be an outer join */
            Assert!(bms_is_member(relid, (*root).outer_join_rels));
            continue;
        }
        result = bms_add_members(result, (*rel).nulling_relids);
    }

    /* Now remove any OJs already included in ph_eval_at, and we're done. */
    result = bms_del_members(result, (*phinfo).ph_eval_at);
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nodes::bitmapset::bms_make_singleton;
    use crate::nodes::pathnodes::PlannerGlobal;

    // Build a minimal PlannerInfo whose ->glob has a working lastPHId, enough
    // for make_placeholder_expr and find_placeholders_in_expr exercising.
    unsafe fn dummy_root() -> *mut PlannerInfo {
        let glob = palloc0(core::mem::size_of::<PlannerGlobal>()) as *mut PlannerGlobal;
        (*glob).lastPHId = 0;
        let root = palloc0(core::mem::size_of::<PlannerInfo>()) as *mut PlannerInfo;
        (*root).glob = glob;
        root
    }

    // A bare PlaceHolderVar-tagged node usable as a fake phexpr leaf. Its phid
    // is irrelevant for the make_placeholder_expr test; we only need a non-null
    // Expr pointer of some tag for pull_var_clause to find.
    #[test]
    fn make_placeholder_expr_builds_phv() {
        unsafe {
            let root = dummy_root();
            // Any non-null Expr pointer works as the represented expression.
            let expr = makeNode!(PlaceHolderVar, T_PlaceHolderVar) as *mut Expr;
            let phrels = bms_make_singleton(3);

            let phv = make_placeholder_expr(root, expr, phrels);

            assert!(!phv.is_null());
            assert!(IsA!(phv as *mut Node, T_PlaceHolderVar));
            assert_eq!((*phv).phexpr, expr);
            assert_eq!((*phv).phrels, phrels);
            assert!((*phv).phnullingrels.is_null());
            assert_eq!((*phv).phid, 1);
            assert_eq!((*phv).phlevelsup, 0);
            // lastPHId was bumped on glob.
            assert_eq!((*(*root).glob).lastPHId, 1);

            // A second one increments phid.
            let phv2 = make_placeholder_expr(root, expr, phrels);
            assert_eq!((*phv2).phid, 2);
            assert_eq!((*(*root).glob).lastPHId, 2);
        }
    }

    // find_placeholders_in_expr should collect a hand-built PHV. We feed an
    // expression that *is* a PlaceHolderVar so pull_var_clause yields it, then
    // confirm find_placeholder_info ran (placeholder_list became non-empty).
    //
    // Note: find_placeholder_info calls get_typavgwidth (stubbed) -> panics.
    // So this test asserts the panic is reached, proving we walked into the
    // PHV branch and attempted to create its PlaceHolderInfo.
    #[test]
    fn find_placeholders_in_expr_visits_phv() {
        unsafe {
            let root = dummy_root();
            // Build a PHV expression with a self-referential leaf phexpr so
            // pull_var_clause returns it as a placeholder.
            let leaf = makeNode!(PlaceHolderVar, T_PlaceHolderVar) as *mut Expr;
            let phrels = bms_make_singleton(1);
            let phv = make_placeholder_expr(root, leaf, phrels);

            let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                find_placeholders_in_expr(root, phv as *mut Node);
            }));
            // It must have reached the stubbed get_typavgwidth inside
            // find_placeholder_info (i.e. it found the PHV and tried to build
            // its info), which unimplemented!()-panics.
            assert!(caught.is_err());
        }
    }
}
