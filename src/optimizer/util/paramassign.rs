//! src/backend/optimizer/util/paramassign.c
//!
//! Functions for assigning PARAM_EXEC slots during planning.
//!
//! #include mapping:
//!   - "postgres.h"               -> crate::prelude::*
//!   - "nodes/nodeFuncs.h"        -> crate::nodes::nodeFuncs::{exprType, exprTypmod,
//!                                    exprCollation, exprLocation}
//!   - "nodes/plannodes.h"        -> crate::nodes::plannodes::NestLoopParam
//!   - "optimizer/paramassign.h"  -> this file's public fns
//!   - "optimizer/placeholder.h"  -> crate::optimizer::util::placeholder::{
//!                                    find_placeholder_info, get_placeholder_nulling_relids}
//!   - "rewrite/rewriteManip.h"   -> STUB (IncrementVarSublevelsUp; rewriteManip.c
//!                                    not yet ported)
//!
//! Nodes: Param / Var / Aggref / GroupingFunc / MergeSupportFunc / ReturningExpr /
//! ParamKind live in crate::nodes::primnodes. PlaceHolderVar / PlannerInfo /
//! PlannerGlobal / PlannerParamItem / PlaceHolderInfo / RelOptInfo / Relids in
//! crate::nodes::pathnodes. NestLoopParam in crate::nodes::plannodes. CmdType in
//! crate::nodes::nodes.
//!
//! REAL: assign_param_for_var, replace_outer_var, assign_param_for_placeholdervar,
//! replace_outer_placeholdervar, replace_outer_agg, replace_outer_grouping,
//! replace_outer_merge_support, replace_outer_returning, replace_nestloop_param_var,
//! replace_nestloop_param_placeholdervar, process_subquery_nestloop_params,
//! identify_current_nestloop_params, generate_new_exec_param,
//! assign_special_exec_param.
//!
//! STUBBED deps (genuinely unported): IncrementVarSublevelsUp (rewriteManip.c).
//! copyObject is the same shallow placeholder used elsewhere in the optimizer
//! until copyfuncs.c lands. See the `stubs` module; each is documented with a
//! TODO(pg-port).

use crate::prelude::*;

use crate::nodes::bitmapset::{
    bms_equal, bms_intersect, bms_is_member, bms_is_subset, bms_overlap, bms_union,
};
use crate::nodes::nodeFuncs::{exprCollation, exprLocation, exprType, exprTypmod};
use crate::nodes::nodes::{nodeTag, CmdType, Node, NodeTag};
use crate::nodes::pathnodes::{
    PlaceHolderInfo, PlaceHolderVar, PlannerGlobal, PlannerInfo, PlannerParamItem, RelOptInfo,
    Relids,
};
use crate::nodes::pg_list::{lappend, lappend_oid, lfirst, list_length, List};
use crate::nodes::plannodes::NestLoopParam;
use crate::nodes::primnodes::{
    Aggref, GroupingFunc, MergeSupportFunc, Param, ReturningExpr, Var, PARAM_EXEC,
};
use crate::optimizer::util::placeholder::{find_placeholder_info, get_placeholder_nulling_relids};
use crate::{
    current_cell, equal_node, foreach, foreach_delete_current, lfirst_node, makeNode, IsA,
};

// ---------------------------------------------------------------------------
// Stubs for genuinely-unported dependencies.
// ---------------------------------------------------------------------------
// IncrementVarSublevelsUp is now ported (rewrite/rewriteManip.c) - used to drop
// an outer-level item down to level 0 after copying it.
use crate::rewrite::rewriteManip::IncrementVarSublevelsUp;

/// copyObject() for a single node pointer. The full generated copyfuncs.c is not
/// yet ported (see crate::nodes::pg_list::copyObjectImpl TODO); this matches the
/// shallow placeholder used elsewhere in the optimizer. It performs a flat byte
/// copy of `*node`, which is sufficient for the in-place field rewrites this
/// module does on the fresh copy.
///
/// TODO(pg-port): replace with the real recursive copyObject once copyfuncs.c is
/// ported.
unsafe fn copyObject<T>(node: *const T) -> *mut T {
    let dst = palloc(core::mem::size_of::<T>()) as *mut T;
    core::ptr::copy_nonoverlapping(node, dst, 1);
    dst
}

/// `equal()` over two node pointers. Wraps crate::nodes::equalfuncs::equal,
/// which is fully ported.
#[macro_export]
macro_rules! equal_node {
    ($a:expr, $b:expr) => {
        $crate::nodes::equalfuncs::equal(
            $a as *const core::ffi::c_void,
            $b as *const core::ffi::c_void,
        )
    };
}

/*
 * Select a PARAM_EXEC number to identify the given Var as a parameter for
 * the current subquery.  (It might already have one.)
 * Record the need for the Var in the proper upper-level root->plan_params.
 */
unsafe fn assign_param_for_var(mut root: *mut PlannerInfo, mut var: *mut Var) -> c_int {
    let mut pitem: *mut PlannerParamItem;
    let mut levelsup: Index;

    /* Find the query level the Var belongs to */
    levelsup = (*var).varlevelsup;
    while levelsup > 0 {
        root = (*root).parent_root;
        levelsup -= 1;
    }

    /* If there's already a matching PlannerParamItem there, just use it */
    foreach!(ppl, (*root).plan_params, {
        pitem = lfirst(current_cell!(ppl)) as *mut PlannerParamItem;
        if IsA!((*pitem).item, T_Var) {
            let pvar = (*pitem).item as *mut Var;

            /*
             * This comparison must match _equalVar(), except for ignoring
             * varlevelsup.  Note that _equalVar() ignores varnosyn,
             * varattnosyn, and location, so this does too.
             */
            if (*pvar).varno == (*var).varno
                && (*pvar).varattno == (*var).varattno
                && (*pvar).vartype == (*var).vartype
                && (*pvar).vartypmod == (*var).vartypmod
                && (*pvar).varcollid == (*var).varcollid
                && (*pvar).varreturningtype == (*var).varreturningtype
                && bms_equal((*pvar).varnullingrels, (*var).varnullingrels)
            {
                return (*pitem).paramId;
            }
        }
    });

    /* Nope, so make a new one */
    var = copyObject(var);
    (*var).varlevelsup = 0;

    pitem = makeNode!(PlannerParamItem, T_PlannerParamItem);
    (*pitem).item = var as *mut Node;
    (*pitem).paramId = list_length((*(*root).glob).paramExecTypes);
    (*(*root).glob).paramExecTypes =
        lappend_oid((*(*root).glob).paramExecTypes, (*var).vartype);

    (*root).plan_params = lappend((*root).plan_params, pitem as *mut c_void);

    (*pitem).paramId
}

/*
 * Generate a Param node to replace the given Var,
 * which is expected to have varlevelsup > 0 (ie, it is not local).
 * Record the need for the Var in the proper upper-level root->plan_params.
 */
pub unsafe fn replace_outer_var(root: *mut PlannerInfo, var: *mut Var) -> *mut Param {
    let retval: *mut Param;
    let i: c_int;

    Assert!((*var).varlevelsup > 0 && (*var).varlevelsup < (*root).query_level);

    /* Find the Var in the appropriate plan_params, or add it if not present */
    i = assign_param_for_var(root, var);

    retval = makeNode!(Param, T_Param);
    (*retval).paramkind = PARAM_EXEC;
    (*retval).paramid = i;
    (*retval).paramtype = (*var).vartype;
    (*retval).paramtypmod = (*var).vartypmod;
    (*retval).paramcollid = (*var).varcollid;
    (*retval).location = (*var).location;

    retval
}

/*
 * Select a PARAM_EXEC number to identify the given PlaceHolderVar as a
 * parameter for the current subquery.  (It might already have one.)
 * Record the need for the PHV in the proper upper-level root->plan_params.
 *
 * This is just like assign_param_for_var, except for PlaceHolderVars.
 */
unsafe fn assign_param_for_placeholdervar(
    mut root: *mut PlannerInfo,
    mut phv: *mut PlaceHolderVar,
) -> c_int {
    let mut pitem: *mut PlannerParamItem;
    let mut levelsup: Index;

    /* Find the query level the PHV belongs to */
    levelsup = (*phv).phlevelsup;
    while levelsup > 0 {
        root = (*root).parent_root;
        levelsup -= 1;
    }

    /* If there's already a matching PlannerParamItem there, just use it */
    foreach!(ppl, (*root).plan_params, {
        pitem = lfirst(current_cell!(ppl)) as *mut PlannerParamItem;
        if IsA!((*pitem).item, T_PlaceHolderVar) {
            let pphv = (*pitem).item as *mut PlaceHolderVar;

            /* We assume comparing the PHIDs is sufficient */
            if (*pphv).phid == (*phv).phid {
                return (*pitem).paramId;
            }
        }
    });

    /* Nope, so make a new one */
    phv = copyObject(phv);
    IncrementVarSublevelsUp(phv as *mut Node, -((*phv).phlevelsup as c_int), 0);
    Assert!((*phv).phlevelsup == 0);

    pitem = makeNode!(PlannerParamItem, T_PlannerParamItem);
    (*pitem).item = phv as *mut Node;
    (*pitem).paramId = list_length((*(*root).glob).paramExecTypes);
    (*(*root).glob).paramExecTypes = lappend_oid(
        (*(*root).glob).paramExecTypes,
        exprType((*phv).phexpr as *const Node),
    );

    (*root).plan_params = lappend((*root).plan_params, pitem as *mut c_void);

    (*pitem).paramId
}

/*
 * Generate a Param node to replace the given PlaceHolderVar,
 * which is expected to have phlevelsup > 0 (ie, it is not local).
 * Record the need for the PHV in the proper upper-level root->plan_params.
 *
 * This is just like replace_outer_var, except for PlaceHolderVars.
 */
pub unsafe fn replace_outer_placeholdervar(
    root: *mut PlannerInfo,
    phv: *mut PlaceHolderVar,
) -> *mut Param {
    let retval: *mut Param;
    let i: c_int;

    Assert!((*phv).phlevelsup > 0 && (*phv).phlevelsup < (*root).query_level);

    /* Find the PHV in the appropriate plan_params, or add it if not present */
    i = assign_param_for_placeholdervar(root, phv);

    retval = makeNode!(Param, T_Param);
    (*retval).paramkind = PARAM_EXEC;
    (*retval).paramid = i;
    (*retval).paramtype = exprType((*phv).phexpr as *const Node);
    (*retval).paramtypmod = exprTypmod((*phv).phexpr as *const Node);
    (*retval).paramcollid = exprCollation((*phv).phexpr as *const Node);
    (*retval).location = -1;

    retval
}

/*
 * Generate a Param node to replace the given Aggref
 * which is expected to have agglevelsup > 0 (ie, it is not local).
 * Record the need for the Aggref in the proper upper-level root->plan_params.
 */
pub unsafe fn replace_outer_agg(mut root: *mut PlannerInfo, mut agg: *mut Aggref) -> *mut Param {
    let retval: *mut Param;
    let pitem: *mut PlannerParamItem;
    let mut levelsup: Index;

    Assert!((*agg).agglevelsup > 0 && (*agg).agglevelsup < (*root).query_level);

    /* Find the query level the Aggref belongs to */
    levelsup = (*agg).agglevelsup;
    while levelsup > 0 {
        root = (*root).parent_root;
        levelsup -= 1;
    }

    /*
     * It does not seem worthwhile to try to de-duplicate references to outer
     * aggs.  Just make a new slot every time.
     */
    agg = copyObject(agg);
    IncrementVarSublevelsUp(agg as *mut Node, -((*agg).agglevelsup as c_int), 0);
    Assert!((*agg).agglevelsup == 0);

    pitem = makeNode!(PlannerParamItem, T_PlannerParamItem);
    (*pitem).item = agg as *mut Node;
    (*pitem).paramId = list_length((*(*root).glob).paramExecTypes);
    (*(*root).glob).paramExecTypes =
        lappend_oid((*(*root).glob).paramExecTypes, (*agg).aggtype);

    (*root).plan_params = lappend((*root).plan_params, pitem as *mut c_void);

    retval = makeNode!(Param, T_Param);
    (*retval).paramkind = PARAM_EXEC;
    (*retval).paramid = (*pitem).paramId;
    (*retval).paramtype = (*agg).aggtype;
    (*retval).paramtypmod = -1;
    (*retval).paramcollid = (*agg).aggcollid;
    (*retval).location = (*agg).location;

    retval
}

/*
 * Generate a Param node to replace the given GroupingFunc expression which is
 * expected to have agglevelsup > 0 (ie, it is not local).
 * Record the need for the GroupingFunc in the proper upper-level
 * root->plan_params.
 */
pub unsafe fn replace_outer_grouping(
    mut root: *mut PlannerInfo,
    mut grp: *mut GroupingFunc,
) -> *mut Param {
    let retval: *mut Param;
    let pitem: *mut PlannerParamItem;
    let mut levelsup: Index;
    let ptype: Oid = exprType(grp as *const Node);

    Assert!((*grp).agglevelsup > 0 && (*grp).agglevelsup < (*root).query_level);

    /* Find the query level the GroupingFunc belongs to */
    levelsup = (*grp).agglevelsup;
    while levelsup > 0 {
        root = (*root).parent_root;
        levelsup -= 1;
    }

    /*
     * It does not seem worthwhile to try to de-duplicate references to outer
     * aggs.  Just make a new slot every time.
     */
    grp = copyObject(grp);
    IncrementVarSublevelsUp(grp as *mut Node, -((*grp).agglevelsup as c_int), 0);
    Assert!((*grp).agglevelsup == 0);

    pitem = makeNode!(PlannerParamItem, T_PlannerParamItem);
    (*pitem).item = grp as *mut Node;
    (*pitem).paramId = list_length((*(*root).glob).paramExecTypes);
    (*(*root).glob).paramExecTypes = lappend_oid((*(*root).glob).paramExecTypes, ptype);

    (*root).plan_params = lappend((*root).plan_params, pitem as *mut c_void);

    retval = makeNode!(Param, T_Param);
    (*retval).paramkind = PARAM_EXEC;
    (*retval).paramid = (*pitem).paramId;
    (*retval).paramtype = ptype;
    (*retval).paramtypmod = -1;
    (*retval).paramcollid = InvalidOid;
    (*retval).location = (*grp).location;

    retval
}

/*
 * Generate a Param node to replace the given MergeSupportFunc expression
 * which is expected to be in the RETURNING list of an upper-level MERGE
 * query.  Record the need for the MergeSupportFunc in the proper upper-level
 * root->plan_params.
 */
pub unsafe fn replace_outer_merge_support(
    mut root: *mut PlannerInfo,
    mut msf: *mut MergeSupportFunc,
) -> *mut Param {
    let retval: *mut Param;
    let pitem: *mut PlannerParamItem;
    let ptype: Oid = exprType(msf as *const Node);

    Assert!((*(*root).parse).commandType != CmdType::CMD_MERGE);

    /*
     * The parser should have ensured that the MergeSupportFunc is in the
     * RETURNING list of an upper-level MERGE query, so find that query.
     */
    loop {
        root = (*root).parent_root;
        if root.is_null() {
            elog!(ERROR, "MergeSupportFunc found outside MERGE");
        }
        if (*(*root).parse).commandType == CmdType::CMD_MERGE {
            break;
        }
    }

    /*
     * It does not seem worthwhile to try to de-duplicate references to outer
     * MergeSupportFunc expressions.  Just make a new slot every time.
     */
    msf = copyObject(msf);

    pitem = makeNode!(PlannerParamItem, T_PlannerParamItem);
    (*pitem).item = msf as *mut Node;
    (*pitem).paramId = list_length((*(*root).glob).paramExecTypes);
    (*(*root).glob).paramExecTypes = lappend_oid((*(*root).glob).paramExecTypes, ptype);

    (*root).plan_params = lappend((*root).plan_params, pitem as *mut c_void);

    retval = makeNode!(Param, T_Param);
    (*retval).paramkind = PARAM_EXEC;
    (*retval).paramid = (*pitem).paramId;
    (*retval).paramtype = ptype;
    (*retval).paramtypmod = -1;
    (*retval).paramcollid = InvalidOid;
    (*retval).location = (*msf).location;

    retval
}

/*
 * Generate a Param node to replace the given ReturningExpr expression which
 * is expected to have retlevelsup > 0 (ie, it is not local).  Record the need
 * for the ReturningExpr in the proper upper-level root->plan_params.
 */
pub unsafe fn replace_outer_returning(
    mut root: *mut PlannerInfo,
    mut rexpr: *mut ReturningExpr,
) -> *mut Param {
    let retval: *mut Param;
    let pitem: *mut PlannerParamItem;
    let mut levelsup: c_int;
    let ptype: Oid = exprType((*rexpr).retexpr as *const Node);

    Assert!((*rexpr).retlevelsup > 0 && (*rexpr).retlevelsup < (*root).query_level as c_int);

    /* Find the query level the ReturningExpr belongs to */
    levelsup = (*rexpr).retlevelsup;
    while levelsup > 0 {
        root = (*root).parent_root;
        levelsup -= 1;
    }

    /*
     * It does not seem worthwhile to try to de-duplicate references to outer
     * ReturningExprs.  Just make a new slot every time.
     */
    rexpr = copyObject(rexpr);
    IncrementVarSublevelsUp(rexpr as *mut Node, -((*rexpr).retlevelsup), 0);
    Assert!((*rexpr).retlevelsup == 0);

    pitem = makeNode!(PlannerParamItem, T_PlannerParamItem);
    (*pitem).item = rexpr as *mut Node;
    (*pitem).paramId = list_length((*(*root).glob).paramExecTypes);
    (*(*root).glob).paramExecTypes = lappend_oid((*(*root).glob).paramExecTypes, ptype);

    (*root).plan_params = lappend((*root).plan_params, pitem as *mut c_void);

    retval = makeNode!(Param, T_Param);
    (*retval).paramkind = PARAM_EXEC;
    (*retval).paramid = (*pitem).paramId;
    (*retval).paramtype = ptype;
    (*retval).paramtypmod = exprTypmod((*rexpr).retexpr as *const Node);
    (*retval).paramcollid = exprCollation((*rexpr).retexpr as *const Node);
    (*retval).location = exprLocation((*rexpr).retexpr as *const Node);

    retval
}

/*
 * Generate a Param node to replace the given Var,
 * which is expected to come from some upper NestLoop plan node.
 * Record the need for the Var in root->curOuterParams.
 */
pub unsafe fn replace_nestloop_param_var(root: *mut PlannerInfo, var: *mut Var) -> *mut Param {
    let param: *mut Param;
    let mut nlp: *mut NestLoopParam;

    /* Is this Var already listed in root->curOuterParams? */
    foreach!(lc, (*root).curOuterParams, {
        nlp = lfirst(current_cell!(lc)) as *mut NestLoopParam;
        if equal_node!(var, (*nlp).paramval) {
            /* Yes, so just make a Param referencing this NLP's slot */
            let param = makeNode!(Param, T_Param);
            (*param).paramkind = PARAM_EXEC;
            (*param).paramid = (*nlp).paramno;
            (*param).paramtype = (*var).vartype;
            (*param).paramtypmod = (*var).vartypmod;
            (*param).paramcollid = (*var).varcollid;
            (*param).location = (*var).location;
            return param;
        }
    });

    /* No, so assign a PARAM_EXEC slot for a new NLP */
    param = generate_new_exec_param(root, (*var).vartype, (*var).vartypmod, (*var).varcollid);
    (*param).location = (*var).location;

    /* Add it to the list of required NLPs */
    nlp = makeNode!(NestLoopParam, T_NestLoopParam);
    (*nlp).paramno = (*param).paramid;
    (*nlp).paramval = copyObject(var);
    (*root).curOuterParams = lappend((*root).curOuterParams, nlp as *mut c_void);

    /* And return the replacement Param */
    param
}

/*
 * Generate a Param node to replace the given PlaceHolderVar,
 * which is expected to come from some upper NestLoop plan node.
 * Record the need for the PHV in root->curOuterParams.
 *
 * This is just like replace_nestloop_param_var, except for PlaceHolderVars.
 */
pub unsafe fn replace_nestloop_param_placeholdervar(
    root: *mut PlannerInfo,
    phv: *mut PlaceHolderVar,
) -> *mut Param {
    let param: *mut Param;
    let mut nlp: *mut NestLoopParam;

    /* Is this PHV already listed in root->curOuterParams? */
    foreach!(lc, (*root).curOuterParams, {
        nlp = lfirst(current_cell!(lc)) as *mut NestLoopParam;
        if equal_node!(phv, (*nlp).paramval) {
            /* Yes, so just make a Param referencing this NLP's slot */
            let param = makeNode!(Param, T_Param);
            (*param).paramkind = PARAM_EXEC;
            (*param).paramid = (*nlp).paramno;
            (*param).paramtype = exprType((*phv).phexpr as *const Node);
            (*param).paramtypmod = exprTypmod((*phv).phexpr as *const Node);
            (*param).paramcollid = exprCollation((*phv).phexpr as *const Node);
            (*param).location = -1;
            return param;
        }
    });

    /* No, so assign a PARAM_EXEC slot for a new NLP */
    param = generate_new_exec_param(
        root,
        exprType((*phv).phexpr as *const Node),
        exprTypmod((*phv).phexpr as *const Node),
        exprCollation((*phv).phexpr as *const Node),
    );

    /* Add it to the list of required NLPs */
    nlp = makeNode!(NestLoopParam, T_NestLoopParam);
    (*nlp).paramno = (*param).paramid;
    (*nlp).paramval = copyObject(phv) as *mut Var;
    (*root).curOuterParams = lappend((*root).curOuterParams, nlp as *mut c_void);

    /* And return the replacement Param */
    param
}

/*
 * process_subquery_nestloop_params
 *	  Handle params of a parameterized subquery that need to be fed
 *	  from an outer nestloop.
 *
 * Currently, that would be *all* params that a subquery in FROM has demanded
 * from the current query level, since they must be LATERAL references.
 *
 * subplan_params is a list of PlannerParamItems that we intend to pass to
 * a subquery-in-FROM.  (This was constructed in root->plan_params while
 * planning the subquery, but isn't there anymore when this is called.)
 *
 * The subplan's references to the outer variables are already represented
 * as PARAM_EXEC Params, since that conversion was done by the routines above
 * while planning the subquery.  So we need not modify the subplan or the
 * PlannerParamItems here.  What we do need to do is add entries to
 * root->curOuterParams to signal the parent nestloop plan node that it must
 * provide these values.  This differs from replace_nestloop_param_var in
 * that the PARAM_EXEC slots to use have already been determined.
 *
 * Note that we also use root->curOuterRels as an implicit parameter for
 * sanity checks.
 */
pub unsafe fn process_subquery_nestloop_params(
    root: *mut PlannerInfo,
    subplan_params: *mut List,
) {
    foreach!(lc, subplan_params, {
        let pitem: *mut PlannerParamItem =
            lfirst_node!(PlannerParamItem, T_PlannerParamItem, current_cell!(lc));

        if IsA!((*pitem).item, T_Var) {
            let var = (*pitem).item as *mut Var;
            let mut nlp: *mut NestLoopParam;

            /* If not from a nestloop outer rel, complain */
            if !bms_is_member((*var).varno, (*root).curOuterRels) {
                elog!(ERROR, "non-LATERAL parameter required by subquery");
            }

            /* Is this param already listed in root->curOuterParams? */
            let mut found = false;
            foreach!(lc2, (*root).curOuterParams, {
                nlp = lfirst(current_cell!(lc2)) as *mut NestLoopParam;
                if (*nlp).paramno == (*pitem).paramId {
                    Assert!(equal_node!(var, (*nlp).paramval));
                    /* Present, so nothing to do */
                    found = true;
                    break;
                }
            });
            if !found {
                /* No, so add it */
                nlp = makeNode!(NestLoopParam, T_NestLoopParam);
                (*nlp).paramno = (*pitem).paramId;
                (*nlp).paramval = copyObject(var);
                (*root).curOuterParams = lappend((*root).curOuterParams, nlp as *mut c_void);
            }
        } else if IsA!((*pitem).item, T_PlaceHolderVar) {
            let phv = (*pitem).item as *mut PlaceHolderVar;
            let mut nlp: *mut NestLoopParam;

            /* If not from a nestloop outer rel, complain */
            if !bms_is_subset(
                (*find_placeholder_info(root, phv)).ph_eval_at,
                (*root).curOuterRels,
            ) {
                elog!(ERROR, "non-LATERAL parameter required by subquery");
            }

            /* Is this param already listed in root->curOuterParams? */
            let mut found = false;
            foreach!(lc2, (*root).curOuterParams, {
                nlp = lfirst(current_cell!(lc2)) as *mut NestLoopParam;
                if (*nlp).paramno == (*pitem).paramId {
                    Assert!(equal_node!(phv, (*nlp).paramval));
                    /* Present, so nothing to do */
                    found = true;
                    break;
                }
            });
            if !found {
                /* No, so add it */
                nlp = makeNode!(NestLoopParam, T_NestLoopParam);
                (*nlp).paramno = (*pitem).paramId;
                (*nlp).paramval = copyObject(phv) as *mut Var;
                (*root).curOuterParams = lappend((*root).curOuterParams, nlp as *mut c_void);
            }
        } else {
            elog!(ERROR, "unexpected type of subquery parameter");
        }
    });
}

/*
 * Identify any NestLoopParams that should be supplied by a NestLoop
 * plan node with the specified lefthand rels and required-outer rels.
 * Remove them from the active root->curOuterParams list and return
 * them as the result list.
 *
 * Vars and PHVs appearing in the result list must have nullingrel sets
 * that could validly appear in the lefthand rel's output.  Ordinarily that
 * would be true already, but if we have applied outer join identity 3,
 * there could be more or fewer nullingrel bits in the nodes appearing in
 * curOuterParams than are in the nominal leftrelids.  We deal with that by
 * forcing their nullingrel sets to include exactly the outer-join relids
 * that appear in leftrelids and can null the respective Var or PHV.
 */
pub unsafe fn identify_current_nestloop_params(
    root: *mut PlannerInfo,
    leftrelids: Relids,
    outerrelids: Relids,
) -> *mut List {
    let mut result: *mut List;
    let allleftrelids: Relids;

    /*
     * We'll be able to evaluate a PHV in the lefthand path if it uses the
     * lefthand rels plus any available required-outer rels.  But don't do so
     * if it uses *only* required-outer rels; in that case it should be
     * evaluated higher in the tree.  For Vars, no such hair-splitting is
     * necessary since they depend on only one relid.
     */
    if !outerrelids.is_null() {
        allleftrelids = bms_union(leftrelids, outerrelids);
    } else {
        allleftrelids = leftrelids;
    }

    result = core::ptr::null_mut();
    foreach!(cell, (*root).curOuterParams, {
        let nlp = lfirst(current_cell!(cell)) as *mut NestLoopParam;

        /*
         * We are looking for Vars and PHVs that can be supplied by the
         * lefthand rels.  When we find one, it's okay to modify it in-place
         * because all the routines above make a fresh copy to put into
         * curOuterParams.
         */
        if IsA!((*nlp).paramval, T_Var)
            && bms_is_member((*(*nlp).paramval).varno, leftrelids)
        {
            let var = (*nlp).paramval as *mut Var;
            let rel: *mut RelOptInfo = *(*root).simple_rel_array.add((*var).varno as usize);

            (*root).curOuterParams = foreach_delete_current!((*root).curOuterParams, cell);
            (*var).varnullingrels = bms_intersect((*rel).nulling_relids, leftrelids);
            result = lappend(result, nlp as *mut c_void);
        } else if IsA!((*nlp).paramval, T_PlaceHolderVar) {
            let mut phv = (*nlp).paramval as *mut PlaceHolderVar;
            let phinfo: *mut PlaceHolderInfo = find_placeholder_info(root, phv);
            let eval_at: Relids = (*phinfo).ph_eval_at;

            if bms_is_subset(eval_at, allleftrelids) && bms_overlap(eval_at, leftrelids) {
                (*root).curOuterParams =
                    foreach_delete_current!((*root).curOuterParams, cell);

                /*
                 * Deal with an edge case: if the PHV was pulled up out of a
                 * subquery and it contains a subquery that was originally
                 * pushed down from this query level, then that will still be
                 * represented as a SubLink, because SS_process_sublinks won't
                 * recurse into outer PHVs, so it didn't get transformed during
                 * expression preprocessing in the subquery.  We need a version
                 * of the PHV that has a SubPlan, which we can get from the
                 * current query level's placeholder_list.
                 */
                if (*(*root).parse).hasSubLinks {
                    phv = copyObject((*phinfo).ph_var);

                    /*
                     * The ph_var will have empty nullingrels, but that doesn't
                     * matter since we're about to overwrite phv->phnullingrels.
                     * Other fields should be OK already.
                     */
                    (*nlp).paramval = phv as *mut Var;
                }

                (*phv).phnullingrels =
                    bms_intersect(get_placeholder_nulling_relids(root, phinfo), leftrelids);

                result = lappend(result, nlp as *mut c_void);
            }
        }
    });
    result
}

/*
 * Generate a new Param node that will not conflict with any other.
 *
 * This is used to create Params representing subplan outputs or
 * NestLoop parameters.
 *
 * We don't need to build a PlannerParamItem for such a Param, but we do
 * need to make sure we record the type in paramExecTypes (otherwise,
 * there won't be a slot allocated for it).
 */
pub unsafe fn generate_new_exec_param(
    root: *mut PlannerInfo,
    paramtype: Oid,
    paramtypmod: int32,
    paramcollation: Oid,
) -> *mut Param {
    let retval: *mut Param;

    retval = makeNode!(Param, T_Param);
    (*retval).paramkind = PARAM_EXEC;
    (*retval).paramid = list_length((*(*root).glob).paramExecTypes);
    (*(*root).glob).paramExecTypes = lappend_oid((*(*root).glob).paramExecTypes, paramtype);
    (*retval).paramtype = paramtype;
    (*retval).paramtypmod = paramtypmod;
    (*retval).paramcollid = paramcollation;
    (*retval).location = -1;

    retval
}

/*
 * Assign a (nonnegative) PARAM_EXEC ID for a special parameter (one that
 * is not actually used to carry a value at runtime).  Such parameters are
 * used for special runtime signaling purposes, such as connecting a
 * recursive union node to its worktable scan node or forcing plan
 * re-evaluation within the EvalPlanQual mechanism.  No actual Param node
 * exists with this ID, however.
 */
pub unsafe fn assign_special_exec_param(root: *mut PlannerInfo) -> c_int {
    let paramId: c_int = list_length((*(*root).glob).paramExecTypes);

    (*(*root).glob).paramExecTypes =
        lappend_oid((*(*root).glob).paramExecTypes, InvalidOid);
    paramId
}

// ---------------------------------------------------------------------------
// Tests for REAL logic that does not depend on stubbed callees.
// ---------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;

    /// Build a minimal PlannerInfo + PlannerGlobal by hand (palloc0) so that
    /// glob->paramExecTypes / plan_params start empty.
    unsafe fn make_minimal_root() -> *mut PlannerInfo {
        let glob = palloc0(core::mem::size_of::<PlannerGlobal>()) as *mut PlannerGlobal;
        let root = palloc0(core::mem::size_of::<PlannerInfo>()) as *mut PlannerInfo;
        (*root).glob = glob;
        (*root).query_level = 1;
        root
    }

    unsafe fn make_var(vartype: Oid) -> *mut Var {
        let var = palloc0(core::mem::size_of::<Var>()) as *mut Var;
        (*(var as *mut Node)).r#type = NodeTag::T_Var;
        (*var).vartype = vartype;
        (*var).varlevelsup = 0;
        var
    }

    /// generate_new_exec_param appends the type to glob->paramExecTypes and
    /// returns an increasing paramid starting from 0.
    #[test]
    fn generate_new_exec_param_increments() {
        unsafe {
            let root = make_minimal_root();

            let p0 = generate_new_exec_param(root, 23, -1, 0);
            assert_eq!((*p0).paramid, 0);
            assert_eq!((*p0).paramtype, 23);
            assert_eq!((*p0).paramkind, PARAM_EXEC);
            assert_eq!(list_length((*(*root).glob).paramExecTypes), 1);

            let p1 = generate_new_exec_param(root, 25, -1, 0);
            assert_eq!((*p1).paramid, 1);
            assert_eq!(list_length((*(*root).glob).paramExecTypes), 2);

            // assign_special_exec_param appends an InvalidOid slot and returns
            // the next id.
            let sid = assign_special_exec_param(root);
            assert_eq!(sid, 2);
            assert_eq!(list_length((*(*root).glob).paramExecTypes), 3);
        }
    }

    /// assign_param_for_var (via the private fn) appends a type to
    /// glob->paramExecTypes and returns an increasing paramid; calling it again
    /// for an *equal* Var reuses the same slot.
    #[test]
    fn assign_param_for_var_appends_and_dedups() {
        unsafe {
            let root = make_minimal_root();

            let var = make_var(23);
            (*var).varlevelsup = 0; // stay at this level (no parent_root walk)

            let id0 = assign_param_for_var(root, var);
            assert_eq!(id0, 0);
            assert_eq!(list_length((*(*root).glob).paramExecTypes), 1);
            assert_eq!(list_length((*root).plan_params), 1);

            // An equal Var (same varno/varattno/vartype/...) reuses slot 0.
            let var2 = make_var(23);
            let id1 = assign_param_for_var(root, var2);
            assert_eq!(id1, 0);
            assert_eq!(list_length((*(*root).glob).paramExecTypes), 1);
            assert_eq!(list_length((*root).plan_params), 1);

            // A different Var (different vartype) gets a fresh slot.
            let var3 = make_var(25);
            let id2 = assign_param_for_var(root, var3);
            assert_eq!(id2, 1);
            assert_eq!(list_length((*(*root).glob).paramExecTypes), 2);
            assert_eq!(list_length((*root).plan_params), 2);
        }
    }
}
