//! setrefs.rs
//!   Post-processing of a completed plan tree: fix references to subplan vars,
//!   compute regproc values for operators, etc
//!
//! Translated 1:1 from postgres/src/backend/optimizer/plan/setrefs.c
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//!
//! IDENTIFICATION
//!   src/backend/optimizer/plan/setrefs.c

use crate::prelude::*;
use crate::{
    foreach, forboth, forthree, foreach_current_index, current_cell,
    IsA, makeNode, lfirst_node, Assert, elog,
};

use std::ptr;

use crate::nodes::nodes::{
    CMD_UTILITY, JOIN_INNER,
    Node, NodeTag, nodeTag, AggSplit, DO_AGGSPLIT_COMBINE,
    AGGSPLIT_INITIAL_SERIAL, AGGSPLIT_FINAL_DESERIAL, Cost,
};
use crate::nodes::pg_list::{
    List, ListCell, list_length, list_nth, list_concat, lappend, lappend_oid,
    lappend_int, linitial, linitial_int, lfirst, lfirst_int, lfirst_int_mut, NIL,
};
use crate::nodes::bitmapset::{
    Bitmapset, bms_add_member, bms_next_member, bms_is_subset, bms_equal,
    bms_make_singleton, bms_intersect,
};
// TODO(pg-port): real bmsToString lives in nodes/outfuncs.rs (deferred/unwired).
unsafe fn bmsToString(_bms: *const crate::nodes::bitmapset::Bitmapset) -> *mut c_char {
    c"(bitmapset)".as_ptr() as *mut c_char
}

use crate::nodes::primnodes::{
    Var, Const, Param, Aggref, WindowFunc, FuncExpr, OpExpr, DistinctExpr,
    NullIfExpr, ScalarArrayOpExpr, GroupingFunc, CurrentOfExpr, TargetEntry,
    Expr, SubPlan, AlternativeSubPlan, TableFunc, MergeAction,
    INNER_VAR, OUTER_VAR, INDEX_VAR, ROWID_VAR, IS_SPECIAL_VARNO,
    VAR_RETURNING_DEFAULT, PARAM_MULTIEXPR,
};
use crate::nodes::pathnodes::{
    PlannerInfo, PlannerGlobal, RelOptInfo, PlaceHolderVar, AppendRelInfo,
    MinMaxAggInfo, Relids,
};
use crate::nodes::parsenodes::{
    Query, RangeTblEntry, RTEPermissionInfo, TableSampleClause,
    CallStmt, RTE_RELATION, RTE_SUBQUERY, RTE_NAMEDTUPLESTORE,
};
use crate::nodes::plannodes::{
    Plan, SeqScan, SampleScan, IndexScan, IndexOnlyScan, BitmapIndexScan,
    BitmapHeapScan, TidScan, TidRangeScan, SubqueryScan, FunctionScan,
    TableFuncScan, ValuesScan, CteScan, NamedTuplestoreScan, WorkTableScan,
    ForeignScan, CustomScan, Join, NestLoop, MergeJoin, HashJoin, NestLoopParam,
    Gather, GatherMerge, Hash, Memoize, Material, Sort, IncrementalSort, Unique,
    SetOp, LockRows, Limit, Agg, Group, WindowAgg, Result, ProjectSet,
    ModifyTable, Append, MergeAppend, RecursiveUnion, BitmapAnd, BitmapOr,
    PlanRowMark, PlanInvalItem, PartitionPruneInfo, PartitionedRelPruneInfo,
    outerPlan,
    SUBQUERY_SCAN_TRIVIAL, SUBQUERY_SCAN_NONTRIVIAL, SUBQUERY_SCAN_UNKNOWN,
};

use crate::nodes::makefuncs::{
    makeNullConst, makeVar, makeVarFromTargetEntry, makeTargetEntry,
    flatCopyTargetEntry,
};
use crate::nodes::nodeFuncs::{
    exprType, exprTypmod, exprCollation, set_opfuncid, set_sa_opfuncid,
    expression_tree_mutator, expression_tree_walker, query_tree_walker,
    QTW_EXAMINE_RTES_BEFORE,
};
use crate::nodes::equalfuncs::equal;

use crate::optimizer::util::tlist::{apply_tlist_labeling, tlist_member};
use crate::rewrite::rewriteManip::remove_nulling_relids;

use crate::catalog::pg_type_d::{REGCLASSOID, OIDOID};
use crate::catalog::catalog::FirstUnpinnedObjectId;

// ===========================================================================
// access/transam.h -- FirstUnpinnedObjectId is above; FirstNormalObjectId etc.
// ===========================================================================

// ---------------------------------------------------------------------------
// Stubs for not-yet-ported callees.  TODO(pg-port): replace with real ports.
// ---------------------------------------------------------------------------

/// `copyObject()` (nodes/copyfuncs.c): deep copy of a node tree.  Not yet
/// ported; this is a shallow-copy stub like in sibling optimizer files.
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

/// `mark_partial_aggref()` (optimizer/plan/createplan.c): convert an Aggref to
/// partial-aggregation form.  TODO(pg-port): real symbol lives in createplan.rs.
unsafe fn mark_partial_aggref(_agg: *mut Aggref, _aggsplit: AggSplit) {
    // TODO(pg-port): real mark_partial_aggref lives in optimizer/plan/createplan.c
    unimplemented!()
}

/// `fetch_upper_rel()` (optimizer/util/relnode.c): fetch the upper relation.
/// TODO(pg-port): real symbol lives in optimizer/util/relnode.rs.
unsafe fn fetch_upper_rel(
    _root: *mut PlannerInfo,
    _kind: c_int,
    _relids: Relids,
) -> *mut RelOptInfo {
    // TODO(pg-port): real fetch_upper_rel lives in optimizer/util/relnode.c
    unimplemented!()
}

/// `SS_compute_initplan_cost()` (optimizer/plan/subselect.c).
/// TODO(pg-port): real symbol lives in optimizer/plan/subselect.rs.
unsafe fn SS_compute_initplan_cost(
    _init_plans: *mut List,
    _initplan_cost_p: *mut Cost,
    _unsafe_initplans_p: *mut bool,
) {
    // TODO(pg-port): real SS_compute_initplan_cost lives in optimizer/plan/subselect.c
    unimplemented!()
}

/// `getRTEPermissionInfo()` (parser/parse_relation.c).
/// TODO(pg-port): real symbol lives in parser/parse_relation.rs.
unsafe fn getRTEPermissionInfo(
    rteperminfos: *mut List,
    rte: *mut RangeTblEntry,
) -> *mut RTEPermissionInfo {
    crate::parser::parse_relation::getRTEPermissionInfo(rteperminfos as _, rte as _) as _
}

/// `addRTEPermissionInfo()` (parser/parse_relation.c).
/// TODO(pg-port): real symbol lives in parser/parse_relation.rs.
unsafe fn addRTEPermissionInfo(
    rteperminfos: *mut *mut List,
    rte: *mut RangeTblEntry,
) -> *mut RTEPermissionInfo {
    crate::parser::parse_relation::addRTEPermissionInfo(rteperminfos as _, rte as _) as _
}

/// `GetSysCacheHashValue1()` (utils/cache/syscache.c).
/// TODO(pg-port): real symbol lives in utils/cache/syscache.rs.
unsafe fn GetSysCacheHashValue1(_cache_id: c_int, _key1: Datum) -> u32 {
    crate::utils::cache::syscache::GetSysCacheHashValue1(_cache_id as _, _key1 as _) as _
}

/// `UtilityContainsQuery()` (tcop/utility.c).
/// TODO(pg-port): real symbol lives in tcop/utility.rs.
unsafe fn UtilityContainsQuery(_parsetree: *mut Node) -> *mut Query {
    // TODO(pg-port): real UtilityContainsQuery lives in tcop/utility.c
    unimplemented!()
}

/// PROCOID syscache id (utils/cache/syscache.h).
/// TODO(pg-port): real constant lives in utils/cache/syscache.rs.
const PROCOID: c_int = 47; // TODO(pg-port): real PROCOID in syscache.h

/// TYPEOID syscache id (utils/cache/syscache.h).
/// TODO(pg-port): real constant lives in utils/cache/syscache.rs.
const TYPEOID: c_int = 82; // TODO(pg-port): real TYPEOID in syscache.h

/// `UPPERREL_FINAL` (nodes/pathnodes.h UpperRelationKind).
/// TODO(pg-port): real constant lives in nodes/pathnodes.rs.
const UPPERREL_FINAL: c_int = 0; // TODO(pg-port): real UPPERREL_FINAL in pathnodes.h

/// `IS_DUMMY_REL(r)` (optimizer/optimizer.h).
/// TODO(pg-port): real macro lives in optimizer/optimizer.rs.
unsafe fn IS_DUMMY_REL(_rel: *mut RelOptInfo) -> bool {
    // TODO(pg-port): real IS_DUMMY_REL lives in optimizer/optimizer.h
    unimplemented!()
}

/// `find_base_rel()` (optimizer/util/relnode.c): find the RelOptInfo for a base rel.
/// TODO(pg-port): real symbol lives in optimizer/util/relnode.rs (placeholder::stubs).
unsafe fn find_base_rel(_root: *mut PlannerInfo, _relid: c_int) -> *mut RelOptInfo {
    // TODO(pg-port): real find_base_rel lives in optimizer/util/relnode.c
    unimplemented!()
}

/// `trivial_subqueryscan()` forward decl is in this file (below).

// ---------------------------------------------------------------------------

type AttrNumber = int16;

#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum NullingRelsMatch {
    NRM_EQUAL,    /* expect exact match of nullingrels */
    NRM_SUBSET,   /* actual Var may have a subset of input */
    NRM_SUPERSET, /* actual Var may have a superset of input */
}
pub use NullingRelsMatch::*;

#[repr(C)]
pub struct tlist_vinfo {
    pub varno: c_int,                  /* RT index of Var */
    pub varattno: AttrNumber,          /* attr number of Var */
    pub resno: AttrNumber,             /* TLE position of Var */
    pub varnullingrels: *mut Bitmapset, /* Var's varnullingrels */
}

#[repr(C)]
pub struct indexed_tlist {
    pub tlist: *mut List,    /* underlying target list */
    pub num_vars: c_int,     /* number of plain Var tlist entries */
    pub has_ph_vars: bool,   /* are there PlaceHolderVar entries? */
    pub has_non_vars: bool,  /* are there other entries? */
    pub vars: [tlist_vinfo; 0], /* has num_vars entries (FLEXIBLE_ARRAY_MEMBER) */
}

#[repr(C)]
pub struct fix_scan_expr_context {
    pub root: *mut PlannerInfo,
    pub rtoffset: c_int,
    pub num_exec: f64,
}

#[repr(C)]
pub struct fix_join_expr_context {
    pub root: *mut PlannerInfo,
    pub outer_itlist: *mut indexed_tlist,
    pub inner_itlist: *mut indexed_tlist,
    pub acceptable_rel: Index,
    pub rtoffset: c_int,
    pub nrm_match: NullingRelsMatch,
    pub num_exec: f64,
}

#[repr(C)]
pub struct fix_upper_expr_context {
    pub root: *mut PlannerInfo,
    pub subplan_itlist: *mut indexed_tlist,
    pub newvarno: c_int,
    pub rtoffset: c_int,
    pub nrm_match: NullingRelsMatch,
    pub num_exec: f64,
}

#[repr(C)]
pub struct fix_windowagg_cond_context {
    pub root: *mut PlannerInfo,
    pub subplan_itlist: *mut indexed_tlist,
    pub newvarno: c_int,
}

/* Context info for flatten_rtes_walker() */
#[repr(C)]
pub struct flatten_rtes_walker_context {
    pub glob: *mut PlannerGlobal,
    pub query: *mut Query,
}

/*
 * Selecting the best alternative in an AlternativeSubPlan expression requires
 * estimating how many times that expression will be evaluated.  For an
 * expression in a plan node's targetlist, the plan's estimated number of
 * output rows is clearly what to use, but for an expression in a qual it's
 * far less clear.  Since AlternativeSubPlans aren't heavily used, we don't
 * want to expend a lot of cycles making such estimates.  What we use is twice
 * the number of output rows.  That's not entirely unfounded: we know that
 * clause_selectivity() would fall back to a default selectivity estimate
 * of 0.5 for any SubPlan, so if the qual containing the SubPlan is the last
 * to be applied (which it likely would be, thanks to order_qual_clauses()),
 * this matches what we could have estimated in a far more laborious fashion.
 * Obviously there are many other scenarios, but it's probably not worth the
 * trouble to try to improve on this estimate, especially not when we don't
 * have a better estimate for the selectivity of the SubPlan qual itself.
 */
#[inline]
unsafe fn NUM_EXEC_TLIST(parentplan: *mut Plan) -> f64 {
    (*parentplan).plan_rows
}
#[inline]
unsafe fn NUM_EXEC_QUAL(parentplan: *mut Plan) -> f64 {
    (*parentplan).plan_rows * 2.0
}

/*
 * Check if a Const node is a regclass value.  We accept plain OID too,
 * since a regclass Const will get folded to that type if it's an argument
 * to oideq or similar operators.  (This might result in some extraneous
 * values in a plan's list of relation dependencies, but the worst result
 * would be occasional useless replans.)
 */
#[inline]
unsafe fn ISREGCLASSCONST(con: *mut Const) -> bool {
    ((*con).consttype == REGCLASSOID || (*con).consttype == OIDOID) && !(*con).constisnull
}

#[inline]
unsafe fn fix_scan_list(
    root: *mut PlannerInfo,
    lst: *mut List,
    rtoffset: c_int,
    num_exec: f64,
) -> *mut List {
    fix_scan_expr(root, lst as *mut Node, rtoffset, num_exec) as *mut List
}

/*****************************************************************************
 *
 *		SUBPLAN REFERENCES
 *
 *****************************************************************************/

/*
 * set_plan_references
 *
 * This is the final processing pass of the planner/optimizer.  The plan
 * tree is complete; we just have to adjust some representational details
 * for the convenience of the executor:
 *
 * 1. We flatten the various subquery rangetables into a single list, and
 * zero out RangeTblEntry fields that are not useful to the executor.
 *
 * 2. We adjust Vars in scan nodes to be consistent with the flat rangetable.
 *
 * 3. We adjust Vars in upper plan nodes to refer to the outputs of their
 * subplans.
 *
 * 4. Aggrefs in Agg plan nodes need to be adjusted in some cases involving
 * partial aggregation or minmax aggregate optimization.
 *
 * 5. PARAM_MULTIEXPR Params are replaced by regular PARAM_EXEC Params,
 * now that we have finished planning all MULTIEXPR subplans.
 *
 * 6. AlternativeSubPlan expressions are replaced by just one of their
 * alternatives, using an estimate of how many times they'll be executed.
 *
 * 7. We compute regproc OIDs for operators (ie, we look up the function
 * that implements each op).
 *
 * 8. We create lists of specific objects that the plan depends on.
 * This will be used by plancache.c to drive invalidation of cached plans.
 * Relation dependencies are represented by OIDs, and everything else by
 * PlanInvalItems (this distinction is motivated by the shared-inval APIs).
 * Currently, relations, user-defined functions, and domains are the only
 * types of objects that are explicitly tracked this way.
 *
 * 9. We assign every plan node in the tree a unique ID.
 *
 * We also perform one final optimization step, which is to delete
 * SubqueryScan, Append, and MergeAppend plan nodes that aren't doing
 * anything useful.  The reason for doing this last is that
 * it can't readily be done before set_plan_references, because it would
 * break set_upper_references: the Vars in the child plan's top tlist
 * wouldn't match up with the Vars in the outer plan tree.  A SubqueryScan
 * serves a necessary function as a buffer between outer query and subquery
 * variable numbering ... but after we've flattened the rangetable this is
 * no longer a problem, since then there's only one rtindex namespace.
 * Likewise, Append and MergeAppend buffer between the parent and child vars
 * of an appendrel, but we don't need to worry about that once we've done
 * set_plan_references.
 *
 * set_plan_references recursively traverses the whole plan tree.
 *
 * The return value is normally the same Plan node passed in, but can be
 * different when the passed-in Plan is a node we decide isn't needed.
 *
 * The flattened rangetable entries are appended to root->glob->finalrtable.
 * Also, rowmarks entries are appended to root->glob->finalrowmarks, and the
 * RT indexes of ModifyTable result relations to root->glob->resultRelations,
 * and flattened AppendRelInfos are appended to root->glob->appendRelations.
 * Plan dependencies are appended to root->glob->relationOids (for relations)
 * and root->glob->invalItems (for everything else).
 *
 * Notice that we modify Plan nodes in-place, but use expression_tree_mutator
 * to process targetlist and qual expressions.  We can assume that the Plan
 * nodes were just built by the planner and are not multiply referenced, but
 * it's not so safe to assume that for expression tree nodes.
 */
pub unsafe fn set_plan_references(root: *mut PlannerInfo, plan: *mut Plan) -> *mut Plan {
    let result: *mut Plan;
    let glob: *mut PlannerGlobal = (*root).glob;
    let rtoffset: c_int = list_length((*glob).finalrtable);
    let mut lc: *mut ListCell;

    /*
     * Add all the query's RTEs to the flattened rangetable.  The live ones
     * will have their rangetable indexes increased by rtoffset.  (Additional
     * RTEs, not referenced by the Plan tree, might get added after those.)
     */
    add_rtes_to_flat_rtable(root, false);

    /*
     * Adjust RT indexes of PlanRowMarks and add to final rowmarks list
     */
    foreach!(lc, (*root).rowMarks, {
        let rc: *mut PlanRowMark = lfirst_node!(PlanRowMark, T_PlanRowMark, current_cell!(lc));
        let newrc: *mut PlanRowMark;

        /* sanity check on existing row marks */
        Assert!(!(*(*root).simple_rel_array.add((*rc).rti as usize)).is_null()
            && !(*(*root).simple_rte_array.add((*rc).rti as usize)).is_null());

        /* flat copy is enough since all fields are scalars */
        newrc = palloc(core::mem::size_of::<PlanRowMark>()) as *mut PlanRowMark;
        core::ptr::copy_nonoverlapping(rc, newrc, 1);

        /* adjust indexes ... but *not* the rowmarkId */
        (*newrc).rti += rtoffset as Index;
        (*newrc).prti += rtoffset as Index;

        (*glob).finalrowmarks = lappend((*glob).finalrowmarks, newrc as *mut c_void);
    });

    /*
     * Adjust RT indexes of AppendRelInfos and add to final appendrels list.
     * We assume the AppendRelInfos were built during planning and don't need
     * to be copied.
     */
    foreach!(lc, (*root).append_rel_list, {
        let appinfo: *mut AppendRelInfo =
            lfirst_node!(AppendRelInfo, T_AppendRelInfo, current_cell!(lc));

        /* adjust RT indexes */
        (*appinfo).parent_relid += rtoffset as Index;
        (*appinfo).child_relid += rtoffset as Index;

        /*
         * Rather than adjust the translated_vars entries, just drop 'em.
         * Neither the executor nor EXPLAIN currently need that data.
         */
        (*appinfo).translated_vars = NIL;

        (*glob).appendRelations = lappend((*glob).appendRelations, appinfo as *mut c_void);
    });

    /* If needed, create workspace for processing AlternativeSubPlans */
    if (*root).hasAlternativeSubPlans {
        (*root).isAltSubplan = palloc0(
            list_length((*glob).subplans) as usize * core::mem::size_of::<bool>(),
        ) as *mut bool;
        (*root).isUsedSubplan = palloc0(
            list_length((*glob).subplans) as usize * core::mem::size_of::<bool>(),
        ) as *mut bool;
    }

    /* Now fix the Plan tree */
    result = set_plan_refs(root, plan, rtoffset);

    /*
     * If we have AlternativeSubPlans, it is likely that we now have some
     * unreferenced subplans in glob->subplans.  To avoid expending cycles on
     * those subplans later, get rid of them by setting those list entries to
     * NULL.  (Note: we can't do this immediately upon processing an
     * AlternativeSubPlan, because there may be multiple copies of the
     * AlternativeSubPlan, and they can get resolved differently.)
     */
    if (*root).hasAlternativeSubPlans {
        foreach!(lc, (*glob).subplans, {
            let ndx: c_int = foreach_current_index!(lc);

            /*
             * If it was used by some AlternativeSubPlan in this query level,
             * but wasn't selected as best by any AlternativeSubPlan, then we
             * don't need it.  Do not touch subplans that aren't parts of
             * AlternativeSubPlans.
             */
            if *(*root).isAltSubplan.add(ndx as usize)
                && !*(*root).isUsedSubplan.add(ndx as usize)
            {
                (*current_cell!(lc)).ptr_value = ptr::null_mut(); /* lfirst(lc) = NULL */
            }
        });
    }

    result
}

/*
 * Extract RangeTblEntries from the plan's rangetable, and add to flat rtable
 *
 * This can recurse into subquery plans; "recursing" is true if so.
 *
 * This also seems like a good place to add the query's RTEPermissionInfos to
 * the flat rteperminfos.
 */
unsafe fn add_rtes_to_flat_rtable(root: *mut PlannerInfo, recursing: bool) {
    let glob: *mut PlannerGlobal = (*root).glob;
    let mut rti: Index;
    let mut lc: *mut ListCell;

    /*
     * Add the query's own RTEs to the flattened rangetable.
     *
     * At top level, we must add all RTEs so that their indexes in the
     * flattened rangetable match up with their original indexes.  When
     * recursing, we only care about extracting relation RTEs (and subquery
     * RTEs that were once relation RTEs).
     */
    foreach!(lc, (*(*root).parse).rtable, {
        let rte: *mut RangeTblEntry = lfirst(current_cell!(lc)) as *mut RangeTblEntry;

        if !recursing
            || (*rte).rtekind == RTE_RELATION
            || ((*rte).rtekind == RTE_SUBQUERY && OidIsValid((*rte).relid))
        {
            add_rte_to_flat_rtable(glob, (*(*root).parse).rteperminfos, rte);
        }
    });

    /*
     * If there are any dead subqueries, they are not referenced in the Plan
     * tree, so we must add RTEs contained in them to the flattened rtable
     * separately.  (If we failed to do this, the executor would not perform
     * expected permission checks for tables mentioned in such subqueries.)
     *
     * Note: this pass over the rangetable can't be combined with the previous
     * one, because that would mess up the numbering of the live RTEs in the
     * flattened rangetable.
     */
    rti = 1;
    foreach!(lc, (*(*root).parse).rtable, {
        let rte: *mut RangeTblEntry = lfirst(current_cell!(lc)) as *mut RangeTblEntry;

        /*
         * We should ignore inheritance-parent RTEs: their contents have been
         * pulled up into our rangetable already.  Also ignore any subquery
         * RTEs without matching RelOptInfos, as they likewise have been
         * pulled up.
         */
        if (*rte).rtekind == RTE_SUBQUERY
            && !(*rte).inh
            && (rti as c_int) < (*root).simple_rel_array_size
        {
            let rel: *mut RelOptInfo = *(*root).simple_rel_array.add(rti as usize);

            if !rel.is_null() {
                Assert!((*rel).relid == rti); /* sanity check on array */

                /*
                 * The subquery might never have been planned at all, if it
                 * was excluded on the basis of self-contradictory constraints
                 * in our query level.  In this case apply
                 * flatten_unplanned_rtes.
                 *
                 * If it was planned but the result rel is dummy, we assume
                 * that it has been omitted from our plan tree (see
                 * set_subquery_pathlist), and recurse to pull up its RTEs.
                 *
                 * Otherwise, it should be represented by a SubqueryScan node
                 * somewhere in our plan tree, and we'll pull up its RTEs when
                 * we process that plan node.
                 *
                 * However, if we're recursing, then we should pull up RTEs
                 * whether the subquery is dummy or not, because we've found
                 * that some upper query level is treating this one as dummy,
                 * and so we won't scan this level's plan tree at all.
                 */
                if (*rel).subroot.is_null() {
                    flatten_unplanned_rtes(glob, rte);
                } else if recursing
                    || IS_DUMMY_REL(fetch_upper_rel(
                        (*rel).subroot,
                        UPPERREL_FINAL,
                        ptr::null_mut(),
                    ))
                {
                    add_rtes_to_flat_rtable((*rel).subroot, true);
                }
            }
        }
        rti += 1;
    });
}

/*
 * Extract RangeTblEntries from a subquery that was never planned at all
 */

unsafe fn flatten_unplanned_rtes(glob: *mut PlannerGlobal, rte: *mut RangeTblEntry) {
    let mut cxt = flatten_rtes_walker_context {
        glob,
        query: (*rte).subquery,
    };

    /* Use query_tree_walker to find all RTEs in the parse tree */
    query_tree_walker(
        (*rte).subquery,
        Some(flatten_rtes_walker),
        &mut cxt as *mut _ as *mut c_void,
        QTW_EXAMINE_RTES_BEFORE,
    );
}

unsafe fn flatten_rtes_walker(node: *mut Node, context_ptr: *mut c_void) -> bool {
    let cxt = context_ptr as *mut flatten_rtes_walker_context;

    if node.is_null() {
        return false;
    }
    if IsA!(node, T_RangeTblEntry) {
        let rte: *mut RangeTblEntry = node as *mut RangeTblEntry;

        /* As above, we need only save relation RTEs and former relations */
        if (*rte).rtekind == RTE_RELATION
            || ((*rte).rtekind == RTE_SUBQUERY && OidIsValid((*rte).relid))
        {
            add_rte_to_flat_rtable((*cxt).glob, (*(*cxt).query).rteperminfos, rte);
        }
        return false;
    }
    if IsA!(node, T_Query) {
        /*
         * Recurse into subselects.  Must update cxt->query to this query so
         * that the rtable and rteperminfos correspond with each other.
         */
        let save_query: *mut Query = (*cxt).query;
        let result: bool;

        (*cxt).query = node as *mut Query;
        result = query_tree_walker(
            node as *mut Query,
            Some(flatten_rtes_walker),
            context_ptr,
            QTW_EXAMINE_RTES_BEFORE,
        );
        (*cxt).query = save_query;
        return result;
    }
    expression_tree_walker(node, Some(flatten_rtes_walker), context_ptr)
}

/*
 * Add (a copy of) the given RTE to the final rangetable and also the
 * corresponding RTEPermissionInfo, if any, to final rteperminfos.
 *
 * In the flat rangetable, we zero out substructure pointers that are not
 * needed by the executor; this reduces the storage space and copying cost
 * for cached plans.  We keep only the ctename, alias, eref Alias fields,
 * which are needed by EXPLAIN, and perminfoindex which is needed by the
 * executor to fetch the RTE's RTEPermissionInfo.
 */
unsafe fn add_rte_to_flat_rtable(
    glob: *mut PlannerGlobal,
    rteperminfos: *mut List,
    rte: *mut RangeTblEntry,
) {
    let newrte: *mut RangeTblEntry;

    /* flat copy to duplicate all the scalar fields */
    newrte = palloc(core::mem::size_of::<RangeTblEntry>()) as *mut RangeTblEntry;
    core::ptr::copy_nonoverlapping(rte, newrte, 1);

    /* zap unneeded sub-structure */
    (*newrte).tablesample = ptr::null_mut();
    (*newrte).subquery = ptr::null_mut();
    (*newrte).joinaliasvars = NIL;
    (*newrte).joinleftcols = NIL;
    (*newrte).joinrightcols = NIL;
    (*newrte).join_using_alias = ptr::null_mut();
    (*newrte).functions = NIL;
    (*newrte).tablefunc = ptr::null_mut();
    (*newrte).values_lists = NIL;
    (*newrte).coltypes = NIL;
    (*newrte).coltypmods = NIL;
    (*newrte).colcollations = NIL;
    (*newrte).groupexprs = NIL;
    (*newrte).securityQuals = NIL;

    (*glob).finalrtable = lappend((*glob).finalrtable, newrte as *mut c_void);

    /*
     * If it's a plain relation RTE (or a subquery that was once a view
     * reference), add the relation OID to relationOids.  Also add its new RT
     * index to the set of relations to be potentially accessed during
     * execution.
     *
     * We do this even though the RTE might be unreferenced in the plan tree;
     * this would correspond to cases such as views that were expanded, child
     * tables that were eliminated by constraint exclusion, etc. Schema
     * invalidation on such a rel must still force rebuilding of the plan.
     *
     * Note we don't bother to avoid making duplicate list entries.  We could,
     * but it would probably cost more cycles than it would save.
     */
    if (*newrte).rtekind == RTE_RELATION
        || ((*newrte).rtekind == RTE_SUBQUERY && OidIsValid((*newrte).relid))
    {
        (*glob).relationOids = lappend_oid((*glob).relationOids, (*newrte).relid);
        (*glob).allRelids =
            bms_add_member((*glob).allRelids, list_length((*glob).finalrtable));
    }

    /*
     * Add a copy of the RTEPermissionInfo, if any, corresponding to this RTE
     * to the flattened global list.
     */
    if (*rte).perminfoindex > 0 {
        let perminfo: *mut RTEPermissionInfo;
        let newperminfo: *mut RTEPermissionInfo;

        /* Get the existing one from this query's rteperminfos. */
        perminfo = getRTEPermissionInfo(rteperminfos, newrte);

        /*
         * Add a new one to finalrteperminfos and copy the contents of the
         * existing one into it.  Note that addRTEPermissionInfo() also
         * updates newrte->perminfoindex to point to newperminfo in
         * finalrteperminfos.
         */
        (*newrte).perminfoindex = 0; /* expected by addRTEPermissionInfo() */
        newperminfo = addRTEPermissionInfo(&raw mut (*glob).finalrteperminfos, newrte);
        core::ptr::copy_nonoverlapping(perminfo, newperminfo, 1);
    }
}

/*
 * set_plan_refs: recurse through the Plan nodes of a single subquery level
 */
unsafe fn set_plan_refs(root: *mut PlannerInfo, plan: *mut Plan, rtoffset: c_int) -> *mut Plan {
    let mut l: *mut ListCell;

    if plan.is_null() {
        return ptr::null_mut();
    }

    /* Assign this node a unique ID. */
    (*plan).plan_node_id = (*(*root).glob).lastPlanNodeId;
    (*(*root).glob).lastPlanNodeId += 1;

    /*
     * Plan-type-specific fixes
     */
    match nodeTag(plan) {
        NodeTag::T_SeqScan => {
            let splan: *mut SeqScan = plan as *mut SeqScan;

            (*splan).scan.scanrelid += rtoffset as Index;
            (*splan).scan.plan.targetlist = fix_scan_list(
                root,
                (*splan).scan.plan.targetlist,
                rtoffset,
                NUM_EXEC_TLIST(plan),
            );
            (*splan).scan.plan.qual = fix_scan_list(
                root,
                (*splan).scan.plan.qual,
                rtoffset,
                NUM_EXEC_QUAL(plan),
            );
        }
        NodeTag::T_SampleScan => {
            let splan: *mut SampleScan = plan as *mut SampleScan;

            (*splan).scan.scanrelid += rtoffset as Index;
            (*splan).scan.plan.targetlist = fix_scan_list(
                root,
                (*splan).scan.plan.targetlist,
                rtoffset,
                NUM_EXEC_TLIST(plan),
            );
            (*splan).scan.plan.qual = fix_scan_list(
                root,
                (*splan).scan.plan.qual,
                rtoffset,
                NUM_EXEC_QUAL(plan),
            );
            (*splan).tablesample = fix_scan_expr(
                root,
                (*splan).tablesample as *mut Node,
                rtoffset,
                1.0,
            ) as *mut TableSampleClause;
        }
        NodeTag::T_IndexScan => {
            let splan: *mut IndexScan = plan as *mut IndexScan;

            (*splan).scan.scanrelid += rtoffset as Index;
            (*splan).scan.plan.targetlist = fix_scan_list(
                root,
                (*splan).scan.plan.targetlist,
                rtoffset,
                NUM_EXEC_TLIST(plan),
            );
            (*splan).scan.plan.qual = fix_scan_list(
                root,
                (*splan).scan.plan.qual,
                rtoffset,
                NUM_EXEC_QUAL(plan),
            );
            (*splan).indexqual = fix_scan_list(root, (*splan).indexqual, rtoffset, 1.0);
            (*splan).indexqualorig = fix_scan_list(
                root,
                (*splan).indexqualorig,
                rtoffset,
                NUM_EXEC_QUAL(plan),
            );
            (*splan).indexorderby = fix_scan_list(root, (*splan).indexorderby, rtoffset, 1.0);
            (*splan).indexorderbyorig = fix_scan_list(
                root,
                (*splan).indexorderbyorig,
                rtoffset,
                NUM_EXEC_QUAL(plan),
            );
        }
        NodeTag::T_IndexOnlyScan => {
            let splan: *mut IndexOnlyScan = plan as *mut IndexOnlyScan;

            return set_indexonlyscan_references(root, splan, rtoffset);
        }
        NodeTag::T_BitmapIndexScan => {
            let splan: *mut BitmapIndexScan = plan as *mut BitmapIndexScan;

            (*splan).scan.scanrelid += rtoffset as Index;
            /* no need to fix targetlist and qual */
            Assert!((*splan).scan.plan.targetlist == NIL);
            Assert!((*splan).scan.plan.qual == NIL);
            (*splan).indexqual = fix_scan_list(root, (*splan).indexqual, rtoffset, 1.0);
            (*splan).indexqualorig = fix_scan_list(
                root,
                (*splan).indexqualorig,
                rtoffset,
                NUM_EXEC_QUAL(plan),
            );
        }
        NodeTag::T_BitmapHeapScan => {
            let splan: *mut BitmapHeapScan = plan as *mut BitmapHeapScan;

            (*splan).scan.scanrelid += rtoffset as Index;
            (*splan).scan.plan.targetlist = fix_scan_list(
                root,
                (*splan).scan.plan.targetlist,
                rtoffset,
                NUM_EXEC_TLIST(plan),
            );
            (*splan).scan.plan.qual = fix_scan_list(
                root,
                (*splan).scan.plan.qual,
                rtoffset,
                NUM_EXEC_QUAL(plan),
            );
            (*splan).bitmapqualorig = fix_scan_list(
                root,
                (*splan).bitmapqualorig,
                rtoffset,
                NUM_EXEC_QUAL(plan),
            );
        }
        NodeTag::T_TidScan => {
            let splan: *mut TidScan = plan as *mut TidScan;

            (*splan).scan.scanrelid += rtoffset as Index;
            (*splan).scan.plan.targetlist = fix_scan_list(
                root,
                (*splan).scan.plan.targetlist,
                rtoffset,
                NUM_EXEC_TLIST(plan),
            );
            (*splan).scan.plan.qual = fix_scan_list(
                root,
                (*splan).scan.plan.qual,
                rtoffset,
                NUM_EXEC_QUAL(plan),
            );
            (*splan).tidquals = fix_scan_list(root, (*splan).tidquals, rtoffset, 1.0);
        }
        NodeTag::T_TidRangeScan => {
            let splan: *mut TidRangeScan = plan as *mut TidRangeScan;

            (*splan).scan.scanrelid += rtoffset as Index;
            (*splan).scan.plan.targetlist = fix_scan_list(
                root,
                (*splan).scan.plan.targetlist,
                rtoffset,
                NUM_EXEC_TLIST(plan),
            );
            (*splan).scan.plan.qual = fix_scan_list(
                root,
                (*splan).scan.plan.qual,
                rtoffset,
                NUM_EXEC_QUAL(plan),
            );
            (*splan).tidrangequals =
                fix_scan_list(root, (*splan).tidrangequals, rtoffset, 1.0);
        }
        NodeTag::T_SubqueryScan => {
            /* Needs special treatment, see comments below */
            return set_subqueryscan_references(root, plan as *mut SubqueryScan, rtoffset);
        }
        NodeTag::T_FunctionScan => {
            let splan: *mut FunctionScan = plan as *mut FunctionScan;

            (*splan).scan.scanrelid += rtoffset as Index;
            (*splan).scan.plan.targetlist = fix_scan_list(
                root,
                (*splan).scan.plan.targetlist,
                rtoffset,
                NUM_EXEC_TLIST(plan),
            );
            (*splan).scan.plan.qual = fix_scan_list(
                root,
                (*splan).scan.plan.qual,
                rtoffset,
                NUM_EXEC_QUAL(plan),
            );
            (*splan).functions = fix_scan_list(root, (*splan).functions, rtoffset, 1.0);
        }
        NodeTag::T_TableFuncScan => {
            let splan: *mut TableFuncScan = plan as *mut TableFuncScan;

            (*splan).scan.scanrelid += rtoffset as Index;
            (*splan).scan.plan.targetlist = fix_scan_list(
                root,
                (*splan).scan.plan.targetlist,
                rtoffset,
                NUM_EXEC_TLIST(plan),
            );
            (*splan).scan.plan.qual = fix_scan_list(
                root,
                (*splan).scan.plan.qual,
                rtoffset,
                NUM_EXEC_QUAL(plan),
            );
            (*splan).tablefunc =
                fix_scan_expr(root, (*splan).tablefunc as *mut Node, rtoffset, 1.0)
                    as *mut TableFunc;
        }
        NodeTag::T_ValuesScan => {
            let splan: *mut ValuesScan = plan as *mut ValuesScan;

            (*splan).scan.scanrelid += rtoffset as Index;
            (*splan).scan.plan.targetlist = fix_scan_list(
                root,
                (*splan).scan.plan.targetlist,
                rtoffset,
                NUM_EXEC_TLIST(plan),
            );
            (*splan).scan.plan.qual = fix_scan_list(
                root,
                (*splan).scan.plan.qual,
                rtoffset,
                NUM_EXEC_QUAL(plan),
            );
            (*splan).values_lists =
                fix_scan_list(root, (*splan).values_lists, rtoffset, 1.0);
        }
        NodeTag::T_CteScan => {
            let splan: *mut CteScan = plan as *mut CteScan;

            (*splan).scan.scanrelid += rtoffset as Index;
            (*splan).scan.plan.targetlist = fix_scan_list(
                root,
                (*splan).scan.plan.targetlist,
                rtoffset,
                NUM_EXEC_TLIST(plan),
            );
            (*splan).scan.plan.qual = fix_scan_list(
                root,
                (*splan).scan.plan.qual,
                rtoffset,
                NUM_EXEC_QUAL(plan),
            );
        }
        NodeTag::T_NamedTuplestoreScan => {
            let splan: *mut NamedTuplestoreScan = plan as *mut NamedTuplestoreScan;

            (*splan).scan.scanrelid += rtoffset as Index;
            (*splan).scan.plan.targetlist = fix_scan_list(
                root,
                (*splan).scan.plan.targetlist,
                rtoffset,
                NUM_EXEC_TLIST(plan),
            );
            (*splan).scan.plan.qual = fix_scan_list(
                root,
                (*splan).scan.plan.qual,
                rtoffset,
                NUM_EXEC_QUAL(plan),
            );
        }
        NodeTag::T_WorkTableScan => {
            let splan: *mut WorkTableScan = plan as *mut WorkTableScan;

            (*splan).scan.scanrelid += rtoffset as Index;
            (*splan).scan.plan.targetlist = fix_scan_list(
                root,
                (*splan).scan.plan.targetlist,
                rtoffset,
                NUM_EXEC_TLIST(plan),
            );
            (*splan).scan.plan.qual = fix_scan_list(
                root,
                (*splan).scan.plan.qual,
                rtoffset,
                NUM_EXEC_QUAL(plan),
            );
        }
        NodeTag::T_ForeignScan => {
            set_foreignscan_references(root, plan as *mut ForeignScan, rtoffset);
        }
        NodeTag::T_CustomScan => {
            set_customscan_references(root, plan as *mut CustomScan, rtoffset);
        }

        NodeTag::T_NestLoop | NodeTag::T_MergeJoin | NodeTag::T_HashJoin => {
            set_join_references(root, plan as *mut Join, rtoffset);
        }

        NodeTag::T_Gather | NodeTag::T_GatherMerge => {
            set_upper_references(root, plan, rtoffset);
            set_param_references(root, plan);
        }

        NodeTag::T_Hash => {
            set_hash_references(root, plan, rtoffset);
        }

        NodeTag::T_Memoize => {
            let mplan: *mut Memoize = plan as *mut Memoize;

            /*
             * Memoize does not evaluate its targetlist.  It just uses the
             * same targetlist from its outer subnode.
             */
            set_dummy_tlist_references(plan, rtoffset);

            (*mplan).param_exprs = fix_scan_list(
                root,
                (*mplan).param_exprs,
                rtoffset,
                NUM_EXEC_TLIST(plan),
            );
        }

        NodeTag::T_Material
        | NodeTag::T_Sort
        | NodeTag::T_IncrementalSort
        | NodeTag::T_Unique
        | NodeTag::T_SetOp => {
            /*
             * These plan types don't actually bother to evaluate their
             * targetlists, because they just return their unmodified input
             * tuples.  Even though the targetlist won't be used by the
             * executor, we fix it up for possible use by EXPLAIN (not to
             * mention ease of debugging --- wrong varnos are very confusing).
             */
            set_dummy_tlist_references(plan, rtoffset);

            /*
             * Since these plan types don't check quals either, we should not
             * find any qual expression attached to them.
             */
            Assert!((*plan).qual == NIL);
        }
        NodeTag::T_LockRows => {
            let splan: *mut LockRows = plan as *mut LockRows;

            /*
             * Like the plan types above, LockRows doesn't evaluate its
             * tlist or quals.  But we have to fix up the RT indexes in
             * its rowmarks.
             */
            set_dummy_tlist_references(plan, rtoffset);
            Assert!((*splan).plan.qual == NIL);

            foreach!(l, (*splan).rowMarks, {
                let rc: *mut PlanRowMark = lfirst(current_cell!(l)) as *mut PlanRowMark;

                (*rc).rti += rtoffset as Index;
                (*rc).prti += rtoffset as Index;
            });
        }
        NodeTag::T_Limit => {
            let splan: *mut Limit = plan as *mut Limit;

            /*
             * Like the plan types above, Limit doesn't evaluate its tlist
             * or quals.  It does have live expressions for limit/offset,
             * however; and those cannot contain subplan variable refs, so
             * fix_scan_expr works for them.
             */
            set_dummy_tlist_references(plan, rtoffset);
            Assert!((*splan).plan.qual == NIL);

            (*splan).limitOffset =
                fix_scan_expr(root, (*splan).limitOffset, rtoffset, 1.0);
            (*splan).limitCount = fix_scan_expr(root, (*splan).limitCount, rtoffset, 1.0);
        }
        NodeTag::T_Agg => {
            let agg: *mut Agg = plan as *mut Agg;

            /*
             * If this node is combining partial-aggregation results, we
             * must convert its Aggrefs to contain references to the
             * partial-aggregate subexpressions that will be available
             * from the child plan node.
             */
            if DO_AGGSPLIT_COMBINE((*agg).aggsplit) {
                (*plan).targetlist = convert_combining_aggrefs(
                    (*plan).targetlist as *mut Node,
                    ptr::null_mut(),
                ) as *mut List;
                (*plan).qual =
                    convert_combining_aggrefs((*plan).qual as *mut Node, ptr::null_mut())
                        as *mut List;
            }

            set_upper_references(root, plan, rtoffset);
        }
        NodeTag::T_Group => {
            set_upper_references(root, plan, rtoffset);
        }
        NodeTag::T_WindowAgg => {
            let wplan: *mut WindowAgg = plan as *mut WindowAgg;

            /*
             * Adjust the WindowAgg's run conditions by swapping the
             * WindowFuncs references out to instead reference the Var in
             * the scan slot so that when the executor evaluates the
             * runCondition, it receives the WindowFunc's value from the
             * slot that the result has just been stored into rather than
             * evaluating the WindowFunc all over again.
             */
            (*wplan).runCondition = set_windowagg_runcondition_references(
                root,
                (*wplan).runCondition,
                wplan as *mut Plan,
            );

            set_upper_references(root, plan, rtoffset);

            /*
             * Like Limit node limit/offset expressions, WindowAgg has
             * frame offset expressions, which cannot contain subplan
             * variable refs, so fix_scan_expr works for them.
             */
            (*wplan).startOffset =
                fix_scan_expr(root, (*wplan).startOffset, rtoffset, 1.0);
            (*wplan).endOffset = fix_scan_expr(root, (*wplan).endOffset, rtoffset, 1.0);
            (*wplan).runCondition = fix_scan_list(
                root,
                (*wplan).runCondition,
                rtoffset,
                NUM_EXEC_TLIST(plan),
            );
            (*wplan).runConditionOrig = fix_scan_list(
                root,
                (*wplan).runConditionOrig,
                rtoffset,
                NUM_EXEC_TLIST(plan),
            );
        }
        NodeTag::T_Result => {
            let splan: *mut Result = plan as *mut Result;

            /*
             * Result may or may not have a subplan; if not, it's more
             * like a scan node than an upper node.
             */
            if !(*splan).plan.lefttree.is_null() {
                set_upper_references(root, plan, rtoffset);
            } else {
                /*
                 * The tlist of a childless Result could contain
                 * unresolved ROWID_VAR Vars, in case it's representing a
                 * target relation which is completely empty because of
                 * constraint exclusion.  Replace any such Vars by null
                 * constants, as though they'd been resolved for a leaf
                 * scan node that doesn't support them.  We could have
                 * fix_scan_expr do this, but since the case is only
                 * expected to occur here, it seems safer to special-case
                 * it here and keep the assertions that ROWID_VARs
                 * shouldn't be seen by fix_scan_expr.
                 */
                foreach!(l, (*splan).plan.targetlist, {
                    let tle: *mut TargetEntry = lfirst(current_cell!(l)) as *mut TargetEntry;
                    let var: *mut Var = (*tle).expr as *mut Var;

                    if !var.is_null() && IsA!(var, T_Var) && (*var).varno == ROWID_VAR {
                        (*tle).expr = makeNullConst(
                            (*var).vartype,
                            (*var).vartypmod,
                            (*var).varcollid,
                        ) as *mut Expr;
                    }
                });

                (*splan).plan.targetlist = fix_scan_list(
                    root,
                    (*splan).plan.targetlist,
                    rtoffset,
                    NUM_EXEC_TLIST(plan),
                );
                (*splan).plan.qual = fix_scan_list(
                    root,
                    (*splan).plan.qual,
                    rtoffset,
                    NUM_EXEC_QUAL(plan),
                );
            }
            /* resconstantqual can't contain any subplan variable refs */
            (*splan).resconstantqual =
                fix_scan_expr(root, (*splan).resconstantqual, rtoffset, 1.0);
        }
        NodeTag::T_ProjectSet => {
            set_upper_references(root, plan, rtoffset);
        }
        NodeTag::T_ModifyTable => {
            let splan: *mut ModifyTable = plan as *mut ModifyTable;
            let subplan: *mut Plan = outerPlan(splan as *mut Plan);

            Assert!((*splan).plan.targetlist == NIL);
            Assert!((*splan).plan.qual == NIL);

            (*splan).withCheckOptionLists =
                fix_scan_list(root, (*splan).withCheckOptionLists, rtoffset, 1.0);

            if !(*splan).returningLists.is_null() {
                let mut newRL: *mut List = NIL;
                let mut lcrl: *mut ListCell;
                let mut lcrr: *mut ListCell;

                /*
                 * Pass each per-resultrel returningList through
                 * set_returning_clause_references().
                 */
                Assert!(
                    list_length((*splan).returningLists)
                        == list_length((*splan).resultRelations)
                );
                forboth!(
                    lcrl,
                    (*splan).returningLists,
                    lcrr,
                    (*splan).resultRelations,
                    {
                        let mut rlist: *mut List = lfirst(lcrl) as *mut List;
                        let resultrel: Index = lfirst_int(lcrr) as Index;

                        rlist = set_returning_clause_references(
                            root, rlist, subplan, resultrel, rtoffset,
                        );
                        newRL = lappend(newRL, rlist as *mut c_void);
                    }
                );
                (*splan).returningLists = newRL;

                /*
                 * Set up the visible plan targetlist as being the same as
                 * the first RETURNING list.  This is mostly for the use
                 * of EXPLAIN; the executor won't execute that targetlist,
                 * although it does use it to prepare the node's result
                 * tuple slot.  We postpone this step until here so that
                 * we don't have to do set_returning_clause_references()
                 * twice on identical targetlists.
                 */
                (*splan).plan.targetlist =
                    copyObject(linitial(newRL) as *const List);
            }

            /*
             * We treat ModifyTable with ON CONFLICT as a form of 'pseudo
             * join', where the inner side is the EXCLUDED tuple.
             * Therefore use fix_join_expr to setup the relevant variables
             * to INNER_VAR. We explicitly don't create any OUTER_VARs as
             * those are already used by RETURNING and it seems better to
             * be non-conflicting.
             */
            if !(*splan).onConflictSet.is_null() {
                let itlist: *mut indexed_tlist;

                itlist = build_tlist_index((*splan).exclRelTlist);

                (*splan).onConflictSet = fix_join_expr(
                    root,
                    (*splan).onConflictSet,
                    ptr::null_mut(),
                    itlist,
                    linitial_int((*splan).resultRelations) as Index,
                    rtoffset,
                    NRM_EQUAL,
                    NUM_EXEC_QUAL(plan),
                );

                (*splan).onConflictWhere = fix_join_expr(
                    root,
                    (*splan).onConflictWhere as *mut List,
                    ptr::null_mut(),
                    itlist,
                    linitial_int((*splan).resultRelations) as Index,
                    rtoffset,
                    NRM_EQUAL,
                    NUM_EXEC_QUAL(plan),
                ) as *mut Node;

                pfree(itlist as *mut c_void);

                (*splan).exclRelTlist =
                    fix_scan_list(root, (*splan).exclRelTlist, rtoffset, 1.0);
            }

            /*
             * The MERGE statement produces the target rows by performing
             * a right join between the target relation and the source
             * relation (which could be a plain relation or a subquery).
             * The INSERT and UPDATE actions of the MERGE statement
             * require access to the columns from the source relation. We
             * arrange things so that the source relation attributes are
             * available as INNER_VAR and the target relation attributes
             * are available from the scan tuple.
             */
            if (*splan).mergeActionLists != NIL {
                let mut newMJC: *mut List = NIL;
                let mut lca: *mut ListCell;
                let mut lcj: *mut ListCell;
                let mut lcr: *mut ListCell;

                /*
                 * Fix the targetList of individual action nodes so that
                 * the so-called "source relation" Vars are referenced as
                 * INNER_VAR.  Note that for this to work correctly during
                 * execution, the ecxt_innertuple must be set to the tuple
                 * obtained by executing the subplan, which is what
                 * constitutes the "source relation".
                 *
                 * We leave the Vars from the result relation (i.e. the
                 * target relation) unchanged i.e. those Vars would be
                 * picked from the scan slot. So during execution, we must
                 * ensure that ecxt_scantuple is setup correctly to refer
                 * to the tuple from the target relation.
                 */
                let itlist: *mut indexed_tlist;

                itlist = build_tlist_index((*subplan).targetlist);

                forthree!(
                    lca,
                    (*splan).mergeActionLists,
                    lcj,
                    (*splan).mergeJoinConditions,
                    lcr,
                    (*splan).resultRelations,
                    {
                        let mergeActionList: *mut List = lfirst(lca) as *mut List;
                        let mut mergeJoinCondition: *mut Node = lfirst(lcj) as *mut Node;
                        let resultrel: Index = lfirst_int(lcr) as Index;

                        foreach!(l, mergeActionList, {
                            let action: *mut MergeAction =
                                lfirst(current_cell!(l)) as *mut MergeAction;

                            /* Fix targetList of each action. */
                            (*action).targetList = fix_join_expr(
                                root,
                                (*action).targetList,
                                ptr::null_mut(),
                                itlist,
                                resultrel,
                                rtoffset,
                                NRM_EQUAL,
                                NUM_EXEC_TLIST(plan),
                            );

                            /* Fix quals too. */
                            (*action).qual = fix_join_expr(
                                root,
                                (*action).qual as *mut List,
                                ptr::null_mut(),
                                itlist,
                                resultrel,
                                rtoffset,
                                NRM_EQUAL,
                                NUM_EXEC_QUAL(plan),
                            ) as *mut Node;
                        });

                        /* Fix join condition too. */
                        mergeJoinCondition = fix_join_expr(
                            root,
                            mergeJoinCondition as *mut List,
                            ptr::null_mut(),
                            itlist,
                            resultrel,
                            rtoffset,
                            NRM_EQUAL,
                            NUM_EXEC_QUAL(plan),
                        ) as *mut Node;
                        newMJC = lappend(newMJC, mergeJoinCondition as *mut c_void);
                    }
                );
                (*splan).mergeJoinConditions = newMJC;
            }

            (*splan).nominalRelation += rtoffset as Index;
            if (*splan).rootRelation != 0 {
                (*splan).rootRelation += rtoffset as Index;
            }
            (*splan).exclRelRTI += rtoffset as Index;

            foreach!(l, (*splan).resultRelations, {
                *lfirst_int_mut(current_cell!(l)) += rtoffset;
            });
            foreach!(l, (*splan).rowMarks, {
                let rc: *mut PlanRowMark = lfirst(current_cell!(l)) as *mut PlanRowMark;

                (*rc).rti += rtoffset as Index;
                (*rc).prti += rtoffset as Index;
            });

            /*
             * Append this ModifyTable node's final result relation RT
             * index(es) to the global list for the plan.
             */
            (*(*root).glob).resultRelations = list_concat(
                (*(*root).glob).resultRelations,
                (*splan).resultRelations,
            );
            if (*splan).rootRelation != 0 {
                (*(*root).glob).resultRelations = lappend_int(
                    (*(*root).glob).resultRelations,
                    (*splan).rootRelation as c_int,
                );
            }
        }
        NodeTag::T_Append => {
            /* Needs special treatment, see comments below */
            return set_append_references(root, plan as *mut Append, rtoffset);
        }
        NodeTag::T_MergeAppend => {
            /* Needs special treatment, see comments below */
            return set_mergeappend_references(root, plan as *mut MergeAppend, rtoffset);
        }
        NodeTag::T_RecursiveUnion => {
            /* This doesn't evaluate targetlist or check quals either */
            set_dummy_tlist_references(plan, rtoffset);
            Assert!((*plan).qual == NIL);
        }
        NodeTag::T_BitmapAnd => {
            let splan: *mut BitmapAnd = plan as *mut BitmapAnd;

            /* BitmapAnd works like Append, but has no tlist */
            Assert!((*splan).plan.targetlist == NIL);
            Assert!((*splan).plan.qual == NIL);
            foreach!(l, (*splan).bitmapplans, {
                (*current_cell!(l)).ptr_value = set_plan_refs(
                    root,
                    lfirst(current_cell!(l)) as *mut Plan,
                    rtoffset,
                ) as *mut c_void;
            });
        }
        NodeTag::T_BitmapOr => {
            let splan: *mut BitmapOr = plan as *mut BitmapOr;

            /* BitmapOr works like Append, but has no tlist */
            Assert!((*splan).plan.targetlist == NIL);
            Assert!((*splan).plan.qual == NIL);
            foreach!(l, (*splan).bitmapplans, {
                (*current_cell!(l)).ptr_value = set_plan_refs(
                    root,
                    lfirst(current_cell!(l)) as *mut Plan,
                    rtoffset,
                ) as *mut c_void;
            });
        }
        _ => {
            elog!(ERROR, "unrecognized node type: {}", nodeTag(plan) as c_int);
        }
    }

    /*
     * Now recurse into child plans, if any
     *
     * NOTE: it is essential that we recurse into child plans AFTER we set
     * subplan references in this plan's tlist and quals.  If we did the
     * reference-adjustments bottom-up, then we would fail to match this
     * plan's var nodes against the already-modified nodes of the children.
     */
    (*plan).lefttree = set_plan_refs(root, (*plan).lefttree, rtoffset);
    (*plan).righttree = set_plan_refs(root, (*plan).righttree, rtoffset);

    plan
}

/*
 * set_indexonlyscan_references
 *		Do set_plan_references processing on an IndexOnlyScan
 *
 * This is unlike the handling of a plain IndexScan because we have to
 * convert Vars referencing the heap into Vars referencing the index.
 * We can use the fix_upper_expr machinery for that, by working from a
 * targetlist describing the index columns.
 */
unsafe fn set_indexonlyscan_references(
    root: *mut PlannerInfo,
    plan: *mut IndexOnlyScan,
    rtoffset: c_int,
) -> *mut Plan {
    let index_itlist: *mut indexed_tlist;
    let mut stripped_indextlist: *mut List;
    let mut lc: *mut ListCell;

    /*
     * Vars in the plan node's targetlist, qual, and recheckqual must only
     * reference columns that the index AM can actually return.  To ensure
     * this, remove non-returnable columns (which are marked as resjunk) from
     * the indexed tlist.  We can just drop them because the indexed_tlist
     * machinery pays attention to TLE resnos, not physical list position.
     */
    stripped_indextlist = NIL;
    foreach!(lc, (*plan).indextlist, {
        let indextle: *mut TargetEntry = lfirst(current_cell!(lc)) as *mut TargetEntry;

        if !(*indextle).resjunk {
            stripped_indextlist = lappend(stripped_indextlist, indextle as *mut c_void);
        }
    });

    index_itlist = build_tlist_index(stripped_indextlist);

    (*plan).scan.scanrelid += rtoffset as Index;
    (*plan).scan.plan.targetlist = fix_upper_expr(
        root,
        (*plan).scan.plan.targetlist as *mut Node,
        index_itlist,
        INDEX_VAR,
        rtoffset,
        NRM_EQUAL,
        NUM_EXEC_TLIST(plan as *mut Plan),
    ) as *mut List;
    (*plan).scan.plan.qual = fix_upper_expr(
        root,
        (*plan).scan.plan.qual as *mut Node,
        index_itlist,
        INDEX_VAR,
        rtoffset,
        NRM_EQUAL,
        NUM_EXEC_QUAL(plan as *mut Plan),
    ) as *mut List;
    (*plan).recheckqual = fix_upper_expr(
        root,
        (*plan).recheckqual as *mut Node,
        index_itlist,
        INDEX_VAR,
        rtoffset,
        NRM_EQUAL,
        NUM_EXEC_QUAL(plan as *mut Plan),
    ) as *mut List;
    /* indexqual is already transformed to reference index columns */
    (*plan).indexqual = fix_scan_list(root, (*plan).indexqual, rtoffset, 1.0);
    /* indexorderby is already transformed to reference index columns */
    (*plan).indexorderby = fix_scan_list(root, (*plan).indexorderby, rtoffset, 1.0);
    /* indextlist must NOT be transformed to reference index columns */
    (*plan).indextlist = fix_scan_list(
        root,
        (*plan).indextlist,
        rtoffset,
        NUM_EXEC_TLIST(plan as *mut Plan),
    );

    pfree(index_itlist as *mut c_void);

    plan as *mut Plan
}

/*
 * set_subqueryscan_references
 *		Do set_plan_references processing on a SubqueryScan
 *
 * We try to strip out the SubqueryScan entirely; if we can't, we have
 * to do the normal processing on it.
 */
unsafe fn set_subqueryscan_references(
    root: *mut PlannerInfo,
    plan: *mut SubqueryScan,
    rtoffset: c_int,
) -> *mut Plan {
    let rel: *mut RelOptInfo;
    let result: *mut Plan;

    /* Need to look up the subquery's RelOptInfo, since we need its subroot */
    rel = find_base_rel(root, (*plan).scan.scanrelid as c_int);

    /* Recursively process the subplan */
    (*plan).subplan = set_plan_references((*rel).subroot, (*plan).subplan);

    if trivial_subqueryscan(plan) {
        /*
         * We can omit the SubqueryScan node and just pull up the subplan.
         */
        result = clean_up_removed_plan_level(plan as *mut Plan, (*plan).subplan);
    } else {
        /*
         * Keep the SubqueryScan node.  We have to do the processing that
         * set_plan_references would otherwise have done on it.  Notice we do
         * not do set_upper_references() here, because a SubqueryScan will
         * always have been created with correct references to its subplan's
         * outputs to begin with.
         */
        (*plan).scan.scanrelid += rtoffset as Index;
        (*plan).scan.plan.targetlist = fix_scan_list(
            root,
            (*plan).scan.plan.targetlist,
            rtoffset,
            NUM_EXEC_TLIST(plan as *mut Plan),
        );
        (*plan).scan.plan.qual = fix_scan_list(
            root,
            (*plan).scan.plan.qual,
            rtoffset,
            NUM_EXEC_QUAL(plan as *mut Plan),
        );

        result = plan as *mut Plan;
    }

    result
}

/*
 * trivial_subqueryscan
 *		Detect whether a SubqueryScan can be deleted from the plan tree.
 *
 * We can delete it if it has no qual to check and the targetlist just
 * regurgitates the output of the child plan.
 *
 * This can be called from mark_async_capable_plan(), a helper function for
 * create_append_plan(), before set_subqueryscan_references(), to determine
 * triviality of a SubqueryScan that is a child of an Append node.  So we
 * cache the result in the SubqueryScan node to avoid repeated computation.
 *
 * Note: when called from mark_async_capable_plan(), we determine the result
 * before running finalize_plan() on the SubqueryScan node (if needed) and
 * set_plan_references() on the subplan tree, but this would be safe, because
 * 1) finalize_plan() doesn't modify the tlist or quals for the SubqueryScan
 *	  node (or that for any plan node in the subplan tree), and
 * 2) set_plan_references() modifies the tlist for every plan node in the
 *	  subplan tree, but keeps const/resjunk columns as const/resjunk ones and
 *	  preserves the length and order of the tlist, and
 * 3) set_plan_references() might delete the topmost plan node like an Append
 *	  or MergeAppend from the subplan tree and pull up the child plan node,
 *	  but in that case, the tlist for the child plan node exactly matches the
 *	  parent.
 */
pub unsafe fn trivial_subqueryscan(plan: *mut SubqueryScan) -> bool {
    let mut attrno: c_int;
    let mut lp: *mut ListCell;
    let mut lc: *mut ListCell;

    /* We might have detected this already; in which case reuse the result */
    if (*plan).scanstatus == SUBQUERY_SCAN_TRIVIAL {
        return true;
    }
    if (*plan).scanstatus == SUBQUERY_SCAN_NONTRIVIAL {
        return false;
    }
    Assert!((*plan).scanstatus == SUBQUERY_SCAN_UNKNOWN);
    /* Initially, mark the SubqueryScan as non-deletable from the plan tree */
    (*plan).scanstatus = SUBQUERY_SCAN_NONTRIVIAL;

    if (*plan).scan.plan.qual != NIL {
        return false;
    }

    if list_length((*plan).scan.plan.targetlist) != list_length((*(*plan).subplan).targetlist) {
        return false; /* tlists not same length */
    }

    attrno = 1;
    forboth!(
        lp,
        (*plan).scan.plan.targetlist,
        lc,
        (*(*plan).subplan).targetlist,
        {
            let ptle: *mut TargetEntry = lfirst(lp) as *mut TargetEntry;
            let ctle: *mut TargetEntry = lfirst(lc) as *mut TargetEntry;

            if (*ptle).resjunk != (*ctle).resjunk {
                return false; /* tlist doesn't match junk status */
            }

            /*
             * We accept either a Var referencing the corresponding element of the
             * subplan tlist, or a Const equaling the subplan element. See
             * generate_setop_tlist() for motivation.
             */
            if !(*ptle).expr.is_null() && IsA!((*ptle).expr, T_Var) {
                let var: *mut Var = (*ptle).expr as *mut Var;

                Assert!((*var).varno == (*plan).scan.scanrelid as c_int);
                Assert!((*var).varlevelsup == 0);
                if (*var).varattno != attrno as AttrNumber {
                    return false; /* out of order */
                }
            } else if !(*ptle).expr.is_null() && IsA!((*ptle).expr, T_Const) {
                if !equal((*ptle).expr as *const c_void, (*ctle).expr as *const c_void) {
                    return false;
                }
            } else {
                return false;
            }

            attrno += 1;
        }
    );

    /* Re-mark the SubqueryScan as deletable from the plan tree */
    (*plan).scanstatus = SUBQUERY_SCAN_TRIVIAL;

    true
}

/*
 * clean_up_removed_plan_level
 *		Do necessary cleanup when we strip out a SubqueryScan, Append, etc
 *
 * We are dropping the "parent" plan in favor of returning just its "child".
 * A few small tweaks are needed.
 */
unsafe fn clean_up_removed_plan_level(parent: *mut Plan, child: *mut Plan) -> *mut Plan {
    /*
     * We have to be sure we don't lose any initplans, so move any that were
     * attached to the parent plan to the child.  If any are parallel-unsafe,
     * the child is no longer parallel-safe.  As a cosmetic matter, also add
     * the initplans' run costs to the child's costs.
     */
    if !(*parent).initPlan.is_null() {
        let mut initplan_cost: Cost = 0.0;
        let mut unsafe_initplans: bool = false;

        SS_compute_initplan_cost(
            (*parent).initPlan,
            &raw mut initplan_cost,
            &raw mut unsafe_initplans,
        );
        (*child).startup_cost += initplan_cost;
        (*child).total_cost += initplan_cost;
        if unsafe_initplans {
            (*child).parallel_safe = false;
        }

        /*
         * Attach plans this way so that parent's initplans are processed
         * before any pre-existing initplans of the child.  Probably doesn't
         * matter, but let's preserve the ordering just in case.
         */
        (*child).initPlan = list_concat((*parent).initPlan, (*child).initPlan);
    }

    /*
     * We also have to transfer the parent's column labeling info into the
     * child, else columns sent to client will be improperly labeled if this
     * is the topmost plan level.  resjunk and so on may be important too.
     */
    apply_tlist_labeling((*child).targetlist, (*parent).targetlist);

    child
}

/*
 * set_foreignscan_references
 *	   Do set_plan_references processing on a ForeignScan
 */
unsafe fn set_foreignscan_references(
    root: *mut PlannerInfo,
    fscan: *mut ForeignScan,
    rtoffset: c_int,
) {
    /* Adjust scanrelid if it's valid */
    if (*fscan).scan.scanrelid > 0 {
        (*fscan).scan.scanrelid += rtoffset as Index;
    }

    if (*fscan).fdw_scan_tlist != NIL || (*fscan).scan.scanrelid == 0 {
        /*
         * Adjust tlist, qual, fdw_exprs, fdw_recheck_quals to reference
         * foreign scan tuple
         */
        let itlist: *mut indexed_tlist = build_tlist_index((*fscan).fdw_scan_tlist);

        (*fscan).scan.plan.targetlist = fix_upper_expr(
            root,
            (*fscan).scan.plan.targetlist as *mut Node,
            itlist,
            INDEX_VAR,
            rtoffset,
            NRM_EQUAL,
            NUM_EXEC_TLIST(fscan as *mut Plan),
        ) as *mut List;
        (*fscan).scan.plan.qual = fix_upper_expr(
            root,
            (*fscan).scan.plan.qual as *mut Node,
            itlist,
            INDEX_VAR,
            rtoffset,
            NRM_EQUAL,
            NUM_EXEC_QUAL(fscan as *mut Plan),
        ) as *mut List;
        (*fscan).fdw_exprs = fix_upper_expr(
            root,
            (*fscan).fdw_exprs as *mut Node,
            itlist,
            INDEX_VAR,
            rtoffset,
            NRM_EQUAL,
            NUM_EXEC_QUAL(fscan as *mut Plan),
        ) as *mut List;
        (*fscan).fdw_recheck_quals = fix_upper_expr(
            root,
            (*fscan).fdw_recheck_quals as *mut Node,
            itlist,
            INDEX_VAR,
            rtoffset,
            NRM_EQUAL,
            NUM_EXEC_QUAL(fscan as *mut Plan),
        ) as *mut List;
        pfree(itlist as *mut c_void);
        /* fdw_scan_tlist itself just needs fix_scan_list() adjustments */
        (*fscan).fdw_scan_tlist = fix_scan_list(
            root,
            (*fscan).fdw_scan_tlist,
            rtoffset,
            NUM_EXEC_TLIST(fscan as *mut Plan),
        );
    } else {
        /*
         * Adjust tlist, qual, fdw_exprs, fdw_recheck_quals in the standard
         * way
         */
        (*fscan).scan.plan.targetlist = fix_scan_list(
            root,
            (*fscan).scan.plan.targetlist,
            rtoffset,
            NUM_EXEC_TLIST(fscan as *mut Plan),
        );
        (*fscan).scan.plan.qual = fix_scan_list(
            root,
            (*fscan).scan.plan.qual,
            rtoffset,
            NUM_EXEC_QUAL(fscan as *mut Plan),
        );
        (*fscan).fdw_exprs = fix_scan_list(
            root,
            (*fscan).fdw_exprs,
            rtoffset,
            NUM_EXEC_QUAL(fscan as *mut Plan),
        );
        (*fscan).fdw_recheck_quals = fix_scan_list(
            root,
            (*fscan).fdw_recheck_quals,
            rtoffset,
            NUM_EXEC_QUAL(fscan as *mut Plan),
        );
    }

    (*fscan).fs_relids = offset_relid_set((*fscan).fs_relids, rtoffset);
    (*fscan).fs_base_relids = offset_relid_set((*fscan).fs_base_relids, rtoffset);

    /* Adjust resultRelation if it's valid */
    if (*fscan).resultRelation > 0 {
        (*fscan).resultRelation += rtoffset as Index;
    }
}

/*
 * set_customscan_references
 *	   Do set_plan_references processing on a CustomScan
 */
unsafe fn set_customscan_references(
    root: *mut PlannerInfo,
    cscan: *mut CustomScan,
    rtoffset: c_int,
) {
    let mut lc: *mut ListCell;

    /* Adjust scanrelid if it's valid */
    if (*cscan).scan.scanrelid > 0 {
        (*cscan).scan.scanrelid += rtoffset as Index;
    }

    if (*cscan).custom_scan_tlist != NIL || (*cscan).scan.scanrelid == 0 {
        /* Adjust tlist, qual, custom_exprs to reference custom scan tuple */
        let itlist: *mut indexed_tlist = build_tlist_index((*cscan).custom_scan_tlist);

        (*cscan).scan.plan.targetlist = fix_upper_expr(
            root,
            (*cscan).scan.plan.targetlist as *mut Node,
            itlist,
            INDEX_VAR,
            rtoffset,
            NRM_EQUAL,
            NUM_EXEC_TLIST(cscan as *mut Plan),
        ) as *mut List;
        (*cscan).scan.plan.qual = fix_upper_expr(
            root,
            (*cscan).scan.plan.qual as *mut Node,
            itlist,
            INDEX_VAR,
            rtoffset,
            NRM_EQUAL,
            NUM_EXEC_QUAL(cscan as *mut Plan),
        ) as *mut List;
        (*cscan).custom_exprs = fix_upper_expr(
            root,
            (*cscan).custom_exprs as *mut Node,
            itlist,
            INDEX_VAR,
            rtoffset,
            NRM_EQUAL,
            NUM_EXEC_QUAL(cscan as *mut Plan),
        ) as *mut List;
        pfree(itlist as *mut c_void);
        /* custom_scan_tlist itself just needs fix_scan_list() adjustments */
        (*cscan).custom_scan_tlist = fix_scan_list(
            root,
            (*cscan).custom_scan_tlist,
            rtoffset,
            NUM_EXEC_TLIST(cscan as *mut Plan),
        );
    } else {
        /* Adjust tlist, qual, custom_exprs in the standard way */
        (*cscan).scan.plan.targetlist = fix_scan_list(
            root,
            (*cscan).scan.plan.targetlist,
            rtoffset,
            NUM_EXEC_TLIST(cscan as *mut Plan),
        );
        (*cscan).scan.plan.qual = fix_scan_list(
            root,
            (*cscan).scan.plan.qual,
            rtoffset,
            NUM_EXEC_QUAL(cscan as *mut Plan),
        );
        (*cscan).custom_exprs = fix_scan_list(
            root,
            (*cscan).custom_exprs,
            rtoffset,
            NUM_EXEC_QUAL(cscan as *mut Plan),
        );
    }

    /* Adjust child plan-nodes recursively, if needed */
    foreach!(lc, (*cscan).custom_plans, {
        (*current_cell!(lc)).ptr_value =
            set_plan_refs(root, lfirst(current_cell!(lc)) as *mut Plan, rtoffset) as *mut c_void;
    });

    (*cscan).custom_relids = offset_relid_set((*cscan).custom_relids, rtoffset);
}

/*
 * register_partpruneinfo
 *		Subroutine for set_append_references and set_mergeappend_references
 *
 * Add the PartitionPruneInfo from root->partPruneInfos at the given index
 * into PlannerGlobal->partPruneInfos and return its index there.
 *
 * Also update the RT indexes present in PartitionedRelPruneInfos to add the
 * offset.
 *
 * Finally, if there are initial pruning steps, add the RT indexes of the
 * leaf partitions to the set of relations that are prunable at execution
 * startup time.
 */
unsafe fn register_partpruneinfo(
    root: *mut PlannerInfo,
    part_prune_index: c_int,
    rtoffset: c_int,
) -> c_int {
    let glob: *mut PlannerGlobal = (*root).glob;
    let pinfo: *mut PartitionPruneInfo;
    let mut l: *mut ListCell;

    Assert!(part_prune_index >= 0 && part_prune_index < list_length((*root).partPruneInfos));
    pinfo = crate::list_nth_node!(
        PartitionPruneInfo,
        T_PartitionPruneInfo,
        (*root).partPruneInfos,
        part_prune_index
    );

    (*pinfo).relids = offset_relid_set((*pinfo).relids, rtoffset);
    foreach!(l, (*pinfo).prune_infos, {
        let prune_infos: *mut List = lfirst(current_cell!(l)) as *mut List;
        let mut l2: *mut ListCell;

        foreach!(l2, prune_infos, {
            let prelinfo: *mut PartitionedRelPruneInfo =
                lfirst(current_cell!(l2)) as *mut PartitionedRelPruneInfo;
            let mut i: c_int;

            (*prelinfo).rtindex += rtoffset as Index;
            (*prelinfo).initial_pruning_steps =
                fix_scan_list(root, (*prelinfo).initial_pruning_steps, rtoffset, 1.0);
            (*prelinfo).exec_pruning_steps =
                fix_scan_list(root, (*prelinfo).exec_pruning_steps, rtoffset, 1.0);

            i = 0;
            while i < (*prelinfo).nparts {
                /*
                 * Non-leaf partitions and partitions that do not have a
                 * subplan are not included in this map as mentioned in
                 * make_partitionedrel_pruneinfo().
                 */
                if *(*prelinfo).leafpart_rti_map.add(i as usize) != 0 {
                    *(*prelinfo).leafpart_rti_map.add(i as usize) += rtoffset;
                    if !(*prelinfo).initial_pruning_steps.is_null() {
                        (*glob).prunableRelids = bms_add_member(
                            (*glob).prunableRelids,
                            *(*prelinfo).leafpart_rti_map.add(i as usize),
                        );
                    }
                }
                i += 1;
            }
        });
    });

    (*glob).partPruneInfos = lappend((*glob).partPruneInfos, pinfo as *mut c_void);

    list_length((*glob).partPruneInfos) - 1
}

/*
 * set_append_references
 *		Do set_plan_references processing on an Append
 *
 * We try to strip out the Append entirely; if we can't, we have
 * to do the normal processing on it.
 */
unsafe fn set_append_references(
    root: *mut PlannerInfo,
    aplan: *mut Append,
    rtoffset: c_int,
) -> *mut Plan {
    let mut l: *mut ListCell;

    /*
     * Append, like Sort et al, doesn't actually evaluate its targetlist or
     * check quals.  If it's got exactly one child plan, then it's not doing
     * anything useful at all, and we can strip it out.
     */
    Assert!((*aplan).plan.qual == NIL);

    /* First, we gotta recurse on the children */
    foreach!(l, (*aplan).appendplans, {
        (*current_cell!(l)).ptr_value =
            set_plan_refs(root, lfirst(current_cell!(l)) as *mut Plan, rtoffset) as *mut c_void;
    });

    /*
     * See if it's safe to get rid of the Append entirely.  For this to be
     * safe, there must be only one child plan and that child plan's parallel
     * awareness must match the Append's.  The reason for the latter is that
     * if the Append is parallel aware and the child is not, then the calling
     * plan may execute the non-parallel aware child multiple times.  (If you
     * change these rules, update create_append_path to match.)
     */
    if list_length((*aplan).appendplans) == 1 {
        let p: *mut Plan = linitial((*aplan).appendplans) as *mut Plan;

        if (*p).parallel_aware == (*aplan).plan.parallel_aware {
            return clean_up_removed_plan_level(aplan as *mut Plan, p);
        }
    }

    /*
     * Otherwise, clean up the Append as needed.  It's okay to do this after
     * recursing to the children, because set_dummy_tlist_references doesn't
     * look at those.
     */
    set_dummy_tlist_references(aplan as *mut Plan, rtoffset);

    (*aplan).apprelids = offset_relid_set((*aplan).apprelids, rtoffset);

    /*
     * Add PartitionPruneInfo, if any, to PlannerGlobal and update the index.
     * Also update the RT indexes present in it to add the offset.
     */
    if (*aplan).part_prune_index >= 0 {
        (*aplan).part_prune_index =
            register_partpruneinfo(root, (*aplan).part_prune_index, rtoffset);
    }

    /* We don't need to recurse to lefttree or righttree ... */
    Assert!((*aplan).plan.lefttree.is_null());
    Assert!((*aplan).plan.righttree.is_null());

    aplan as *mut Plan
}

/*
 * set_mergeappend_references
 *		Do set_plan_references processing on a MergeAppend
 *
 * We try to strip out the MergeAppend entirely; if we can't, we have
 * to do the normal processing on it.
 */
unsafe fn set_mergeappend_references(
    root: *mut PlannerInfo,
    mplan: *mut MergeAppend,
    rtoffset: c_int,
) -> *mut Plan {
    let mut l: *mut ListCell;

    /*
     * MergeAppend, like Sort et al, doesn't actually evaluate its targetlist
     * or check quals.  If it's got exactly one child plan, then it's not
     * doing anything useful at all, and we can strip it out.
     */
    Assert!((*mplan).plan.qual == NIL);

    /* First, we gotta recurse on the children */
    foreach!(l, (*mplan).mergeplans, {
        (*current_cell!(l)).ptr_value =
            set_plan_refs(root, lfirst(current_cell!(l)) as *mut Plan, rtoffset) as *mut c_void;
    });

    /*
     * See if it's safe to get rid of the MergeAppend entirely.  For this to
     * be safe, there must be only one child plan and that child plan's
     * parallel awareness must match the MergeAppend's.  The reason for the
     * latter is that if the MergeAppend is parallel aware and the child is
     * not, then the calling plan may execute the non-parallel aware child
     * multiple times.  (If you change these rules, update
     * create_merge_append_path to match.)
     */
    if list_length((*mplan).mergeplans) == 1 {
        let p: *mut Plan = linitial((*mplan).mergeplans) as *mut Plan;

        if (*p).parallel_aware == (*mplan).plan.parallel_aware {
            return clean_up_removed_plan_level(mplan as *mut Plan, p);
        }
    }

    /*
     * Otherwise, clean up the MergeAppend as needed.  It's okay to do this
     * after recursing to the children, because set_dummy_tlist_references
     * doesn't look at those.
     */
    set_dummy_tlist_references(mplan as *mut Plan, rtoffset);

    (*mplan).apprelids = offset_relid_set((*mplan).apprelids, rtoffset);

    /*
     * Add PartitionPruneInfo, if any, to PlannerGlobal and update the index.
     * Also update the RT indexes present in it to add the offset.
     */
    if (*mplan).part_prune_index >= 0 {
        (*mplan).part_prune_index =
            register_partpruneinfo(root, (*mplan).part_prune_index, rtoffset);
    }

    /* We don't need to recurse to lefttree or righttree ... */
    Assert!((*mplan).plan.lefttree.is_null());
    Assert!((*mplan).plan.righttree.is_null());

    mplan as *mut Plan
}

/*
 * set_hash_references
 *	   Do set_plan_references processing on a Hash node
 */
unsafe fn set_hash_references(root: *mut PlannerInfo, plan: *mut Plan, rtoffset: c_int) {
    let hplan: *mut Hash = plan as *mut Hash;
    let outer_plan: *mut Plan = (*plan).lefttree;
    let outer_itlist: *mut indexed_tlist;

    /*
     * Hash's hashkeys are used when feeding tuples into the hashtable,
     * therefore have them reference Hash's outer plan (which itself is the
     * inner plan of the HashJoin).
     */
    outer_itlist = build_tlist_index((*outer_plan).targetlist);
    (*hplan).hashkeys = fix_upper_expr(
        root,
        (*hplan).hashkeys as *mut Node,
        outer_itlist,
        OUTER_VAR,
        rtoffset,
        NRM_EQUAL,
        NUM_EXEC_QUAL(plan),
    ) as *mut List;

    /* Hash doesn't project */
    set_dummy_tlist_references(plan, rtoffset);

    /* Hash nodes don't have their own quals */
    Assert!((*plan).qual == NIL);
}

/*
 * offset_relid_set
 *		Apply rtoffset to the members of a Relids set.
 */
unsafe fn offset_relid_set(relids: Relids, rtoffset: c_int) -> Relids {
    let mut result: Relids = ptr::null_mut();
    let mut rtindex: c_int;

    /* If there's no offset to apply, we needn't recompute the value */
    if rtoffset == 0 {
        return relids;
    }
    rtindex = -1;
    loop {
        rtindex = bms_next_member(relids, rtindex);
        if rtindex < 0 {
            break;
        }
        result = bms_add_member(result, rtindex + rtoffset);
    }
    result
}

/*
 * copyVar
 *		Copy a Var node.
 *
 * fix_scan_expr and friends do this enough times that it's worth having
 * a bespoke routine instead of using the generic copyObject() function.
 */
#[inline]
unsafe fn copyVar(var: *mut Var) -> *mut Var {
    let newvar: *mut Var = palloc(core::mem::size_of::<Var>()) as *mut Var;

    *newvar = core::ptr::read(var);
    newvar
}

/*
 * fix_expr_common
 *		Do generic set_plan_references processing on an expression node
 *
 * This is code that is common to all variants of expression-fixing.
 * We must look up operator opcode info for OpExpr and related nodes,
 * add OIDs from regclass Const nodes into root->glob->relationOids, and
 * add PlanInvalItems for user-defined functions into root->glob->invalItems.
 * We also fill in column index lists for GROUPING() expressions.
 *
 * We assume it's okay to update opcode info in-place.  So this could possibly
 * scribble on the planner's input data structures, but it's OK.
 */
unsafe fn fix_expr_common(root: *mut PlannerInfo, node: *mut Node) {
    /* We assume callers won't call us on a NULL pointer */
    if IsA!(node, T_Aggref) {
        record_plan_function_dependency(root, (*(node as *mut Aggref)).aggfnoid);
    } else if IsA!(node, T_WindowFunc) {
        record_plan_function_dependency(root, (*(node as *mut WindowFunc)).winfnoid);
    } else if IsA!(node, T_FuncExpr) {
        record_plan_function_dependency(root, (*(node as *mut FuncExpr)).funcid);
    } else if IsA!(node, T_OpExpr) {
        set_opfuncid(node as *mut OpExpr);
        record_plan_function_dependency(root, (*(node as *mut OpExpr)).opfuncid);
    } else if IsA!(node, T_DistinctExpr) {
        set_opfuncid(node as *mut OpExpr); /* rely on struct equivalence */
        record_plan_function_dependency(root, (*(node as *mut DistinctExpr)).opfuncid);
    } else if IsA!(node, T_NullIfExpr) {
        set_opfuncid(node as *mut OpExpr); /* rely on struct equivalence */
        record_plan_function_dependency(root, (*(node as *mut NullIfExpr)).opfuncid);
    } else if IsA!(node, T_ScalarArrayOpExpr) {
        let saop: *mut ScalarArrayOpExpr = node as *mut ScalarArrayOpExpr;

        set_sa_opfuncid(saop);
        record_plan_function_dependency(root, (*saop).opfuncid);

        if OidIsValid((*saop).hashfuncid) {
            record_plan_function_dependency(root, (*saop).hashfuncid);
        }

        if OidIsValid((*saop).negfuncid) {
            record_plan_function_dependency(root, (*saop).negfuncid);
        }
    } else if IsA!(node, T_Const) {
        let con: *mut Const = node as *mut Const;

        /* Check for regclass reference */
        if ISREGCLASSCONST(con) {
            (*(*root).glob).relationOids = lappend_oid(
                (*(*root).glob).relationOids,
                DatumGetObjectId((*con).constvalue),
            );
        }
    } else if IsA!(node, T_GroupingFunc) {
        let g: *mut GroupingFunc = node as *mut GroupingFunc;
        let grouping_map: *mut AttrNumber = (*root).grouping_map;

        /* If there are no grouping sets, we don't need this. */

        Assert!(!grouping_map.is_null() || (*g).cols == NIL);

        if !grouping_map.is_null() {
            let mut lc: *mut ListCell;
            let mut cols: *mut List = NIL;

            foreach!(lc, (*g).refs, {
                cols = lappend_int(
                    cols,
                    *grouping_map.add(lfirst_int(current_cell!(lc)) as usize) as c_int,
                );
            });

            Assert!((*g).cols.is_null() || equal(cols as *const c_void, (*g).cols as *const c_void));

            if (*g).cols.is_null() {
                (*g).cols = cols;
            }
        }
    }
}

/*
 * fix_param_node
 *		Do set_plan_references processing on a Param
 *
 * If it's a PARAM_MULTIEXPR, replace it with the appropriate Param from
 * root->multiexpr_params; otherwise no change is needed.
 * Just for paranoia's sake, we make a copy of the node in either case.
 */
unsafe fn fix_param_node(root: *mut PlannerInfo, p: *mut Param) -> *mut Node {
    if (*p).paramkind == PARAM_MULTIEXPR {
        let subqueryid: c_int = (*p).paramid >> 16;
        let colno: c_int = (*p).paramid & 0xFFFF;
        let params: *mut List;

        if subqueryid <= 0 || subqueryid > list_length((*root).multiexpr_params) {
            elog!(ERROR, "unexpected PARAM_MULTIEXPR ID: {}", (*p).paramid);
        }
        params = list_nth((*root).multiexpr_params, subqueryid - 1) as *mut List;
        if colno <= 0 || colno > list_length(params) {
            elog!(ERROR, "unexpected PARAM_MULTIEXPR ID: {}", (*p).paramid);
        }
        return copyObject(list_nth(params, colno - 1) as *const Node);
    }
    copyObject(p as *const Param) as *mut Node
}

/*
 * fix_alternative_subplan
 *		Do set_plan_references processing on an AlternativeSubPlan
 *
 * Choose one of the alternative implementations and return just that one,
 * discarding the rest of the AlternativeSubPlan structure.
 * Note: caller must still recurse into the result!
 *
 * We don't make any attempt to fix up cost estimates in the parent plan
 * node or higher-level nodes.
 */
unsafe fn fix_alternative_subplan(
    root: *mut PlannerInfo,
    asplan: *mut AlternativeSubPlan,
    num_exec: f64,
) -> *mut Node {
    let mut bestplan: *mut SubPlan = ptr::null_mut();
    let mut bestcost: Cost = 0.0;
    let mut lc: *mut ListCell;

    /*
     * Compute the estimated cost of each subplan assuming num_exec
     * executions, and keep the cheapest one.  In event of exact equality of
     * estimates, we prefer the later plan; this is a bit arbitrary, but in
     * current usage it biases us to break ties against fast-start subplans.
     */
    Assert!((*asplan).subplans != NIL);

    foreach!(lc, (*asplan).subplans, {
        let curplan: *mut SubPlan = lfirst(current_cell!(lc)) as *mut SubPlan;
        let curcost: Cost;

        curcost = (*curplan).startup_cost + num_exec * (*curplan).per_call_cost;
        if bestplan.is_null() || curcost <= bestcost {
            bestplan = curplan;
            bestcost = curcost;
        }

        /* Also mark all subplans that are in AlternativeSubPlans */
        *(*root).isAltSubplan.add(((*curplan).plan_id - 1) as usize) = true;
    });

    /* Mark the subplan we selected */
    *(*root).isUsedSubplan.add(((*bestplan).plan_id - 1) as usize) = true;

    bestplan as *mut Node
}

/*
 * fix_scan_expr
 *		Do set_plan_references processing on a scan-level expression
 *
 * This consists of incrementing all Vars' varnos by rtoffset,
 * replacing PARAM_MULTIEXPR Params, expanding PlaceHolderVars,
 * replacing Aggref nodes that should be replaced by initplan output Params,
 * choosing the best implementation for AlternativeSubPlans,
 * looking up operator opcode info for OpExpr and related nodes,
 * and adding OIDs from regclass Const nodes into root->glob->relationOids.
 *
 * 'node': the expression to be modified
 * 'rtoffset': how much to increment varnos by
 * 'num_exec': estimated number of executions of expression
 *
 * The expression tree is either copied-and-modified, or modified in-place
 * if that seems safe.
 */
unsafe fn fix_scan_expr(
    root: *mut PlannerInfo,
    node: *mut Node,
    rtoffset: c_int,
    num_exec: f64,
) -> *mut Node {
    let mut context = fix_scan_expr_context {
        root,
        rtoffset,
        num_exec,
    };

    if rtoffset != 0
        || (*root).multiexpr_params != NIL
        || (*(*root).glob).lastPHId != 0
        || (*root).minmax_aggs != NIL
        || (*root).hasAlternativeSubPlans
    {
        fix_scan_expr_mutator(node, &mut context as *mut _ as *mut c_void)
    } else {
        /*
         * If rtoffset == 0, we don't need to change any Vars, and if there
         * are no MULTIEXPR subqueries then we don't need to replace
         * PARAM_MULTIEXPR Params, and if there are no placeholders anywhere
         * we won't need to remove them, and if there are no minmax Aggrefs we
         * won't need to replace them, and if there are no AlternativeSubPlans
         * we won't need to remove them.  Then it's OK to just scribble on the
         * input node tree instead of copying (since the only change, filling
         * in any unset opfuncid fields, is harmless).  This saves just enough
         * cycles to be noticeable on trivial queries.
         */
        fix_scan_expr_walker(node, &mut context as *mut _ as *mut c_void);
        node
    }
}

unsafe fn fix_scan_expr_mutator(node: *mut Node, context_ptr: *mut c_void) -> *mut Node {
    let context = context_ptr as *mut fix_scan_expr_context;

    if node.is_null() {
        return ptr::null_mut();
    }
    if IsA!(node, T_Var) {
        let var: *mut Var = copyVar(node as *mut Var);

        Assert!((*var).varlevelsup == 0);

        /*
         * We should not see Vars marked INNER_VAR, OUTER_VAR, or ROWID_VAR.
         * But an indexqual expression could contain INDEX_VAR Vars.
         */
        Assert!((*var).varno != INNER_VAR);
        Assert!((*var).varno != OUTER_VAR);
        Assert!((*var).varno != ROWID_VAR);
        if !IS_SPECIAL_VARNO((*var).varno) {
            (*var).varno += (*context).rtoffset;
        }
        if (*var).varnosyn > 0 {
            (*var).varnosyn = ((*var).varnosyn as c_int + (*context).rtoffset) as Index;
        }
        return var as *mut Node;
    }
    if IsA!(node, T_Param) {
        return fix_param_node((*context).root, node as *mut Param);
    }
    if IsA!(node, T_Aggref) {
        let aggref: *mut Aggref = node as *mut Aggref;
        let aggparam: *mut Param;

        /* See if the Aggref should be replaced by a Param */
        aggparam = find_minmax_agg_replacement_param((*context).root, aggref);
        if !aggparam.is_null() {
            /* Make a copy of the Param for paranoia's sake */
            return copyObject(aggparam as *const Param) as *mut Node;
        }
        /* If no match, just fall through to process it normally */
    }
    if IsA!(node, T_CurrentOfExpr) {
        let cexpr: *mut CurrentOfExpr = copyObject(node as *const CurrentOfExpr);

        Assert!(!IS_SPECIAL_VARNO((*cexpr).cvarno as c_int));
        (*cexpr).cvarno += (*context).rtoffset as Index;
        return cexpr as *mut Node;
    }
    if IsA!(node, T_PlaceHolderVar) {
        /* At scan level, we should always just evaluate the contained expr */
        let phv: *mut PlaceHolderVar = node as *mut PlaceHolderVar;

        /* XXX can we assert something about phnullingrels? */
        return fix_scan_expr_mutator((*phv).phexpr as *mut Node, context_ptr);
    }
    if IsA!(node, T_AlternativeSubPlan) {
        return fix_scan_expr_mutator(
            fix_alternative_subplan(
                (*context).root,
                node as *mut AlternativeSubPlan,
                (*context).num_exec,
            ),
            context_ptr,
        );
    }
    fix_expr_common((*context).root, node);
    expression_tree_mutator(node, Some(fix_scan_expr_mutator), context_ptr)
}

unsafe fn fix_scan_expr_walker(node: *mut Node, context_ptr: *mut c_void) -> bool {
    let context = context_ptr as *mut fix_scan_expr_context;

    if node.is_null() {
        return false;
    }
    Assert!(!(IsA!(node, T_Var) && (*(node as *mut Var)).varno == ROWID_VAR));
    Assert!(!IsA!(node, T_PlaceHolderVar));
    Assert!(!IsA!(node, T_AlternativeSubPlan));
    fix_expr_common((*context).root, node);
    expression_tree_walker(node, Some(fix_scan_expr_walker), context_ptr)
}

/*
 * set_join_references
 *	  Modify the target list and quals of a join node to reference its
 *	  subplans, by setting the varnos to OUTER_VAR or INNER_VAR and setting
 *	  attno values to the result domain number of either the corresponding
 *	  outer or inner join tuple item.  Also perform opcode lookup for these
 *	  expressions, and add regclass OIDs to root->glob->relationOids.
 */
unsafe fn set_join_references(root: *mut PlannerInfo, join: *mut Join, rtoffset: c_int) {
    let outer_plan: *mut Plan = (*join).plan.lefttree;
    let inner_plan: *mut Plan = (*join).plan.righttree;
    let outer_itlist: *mut indexed_tlist;
    let inner_itlist: *mut indexed_tlist;

    outer_itlist = build_tlist_index((*outer_plan).targetlist);
    inner_itlist = build_tlist_index((*inner_plan).targetlist);

    /*
     * First process the joinquals (including merge or hash clauses).  These
     * are logically below the join so they can always use all values
     * available from the input tlists.  It's okay to also handle
     * NestLoopParams now, because those couldn't refer to nullable
     * subexpressions.
     */
    (*join).joinqual = fix_join_expr(
        root,
        (*join).joinqual,
        outer_itlist,
        inner_itlist,
        0 as Index,
        rtoffset,
        NRM_EQUAL,
        NUM_EXEC_QUAL(join as *mut Plan),
    );

    /* Now do join-type-specific stuff */
    if IsA!(join, T_NestLoop) {
        let nl: *mut NestLoop = join as *mut NestLoop;
        let mut lc: *mut ListCell;

        foreach!(lc, (*nl).nestParams, {
            let nlp: *mut NestLoopParam = lfirst(current_cell!(lc)) as *mut NestLoopParam;

            /*
             * Because we don't reparameterize parameterized paths to match
             * the outer-join level at which they are used, Vars seen in the
             * NestLoopParam expression may have nullingrels that are just a
             * subset of those in the Vars actually available from the outer
             * side.  (Lateral references can also cause this, as explained in
             * the comments for identify_current_nestloop_params.)  Not
             * checking this exactly is a bit grotty, but the work needed to
             * make things match up perfectly seems well out of proportion to
             * the value.
             */
            (*nlp).paramval = fix_upper_expr(
                root,
                (*nlp).paramval as *mut Node,
                outer_itlist,
                OUTER_VAR,
                rtoffset,
                NRM_SUBSET,
                NUM_EXEC_TLIST(outer_plan),
            ) as *mut Var;
            /* Check we replaced any PlaceHolderVar with simple Var */
            if !(IsA!((*nlp).paramval, T_Var) && (*(*nlp).paramval).varno == OUTER_VAR) {
                elog!(ERROR, "NestLoopParam was not reduced to a simple Var");
            }
        });
    } else if IsA!(join, T_MergeJoin) {
        let mj: *mut MergeJoin = join as *mut MergeJoin;

        (*mj).mergeclauses = fix_join_expr(
            root,
            (*mj).mergeclauses,
            outer_itlist,
            inner_itlist,
            0 as Index,
            rtoffset,
            NRM_EQUAL,
            NUM_EXEC_QUAL(join as *mut Plan),
        );
    } else if IsA!(join, T_HashJoin) {
        let hj: *mut HashJoin = join as *mut HashJoin;

        (*hj).hashclauses = fix_join_expr(
            root,
            (*hj).hashclauses,
            outer_itlist,
            inner_itlist,
            0 as Index,
            rtoffset,
            NRM_EQUAL,
            NUM_EXEC_QUAL(join as *mut Plan),
        );

        /*
         * HashJoin's hashkeys are used to look for matching tuples from its
         * outer plan (not the Hash node!) in the hashtable.
         */
        (*hj).hashkeys = fix_upper_expr(
            root,
            (*hj).hashkeys as *mut Node,
            outer_itlist,
            OUTER_VAR,
            rtoffset,
            NRM_EQUAL,
            NUM_EXEC_QUAL(join as *mut Plan),
        ) as *mut List;
    }

    /*
     * Now we need to fix up the targetlist and qpqual, which are logically
     * above the join.  This means that, if it's not an inner join, any Vars
     * and PHVs appearing here should have nullingrels that include the
     * effects of the outer join, ie they will have nullingrels equal to the
     * input Vars' nullingrels plus the bit added by the outer join.  We don't
     * currently have enough info available here to identify what that should
     * be, so we just tell fix_join_expr to accept superset nullingrels
     * matches instead of exact ones.
     */
    (*join).plan.targetlist = fix_join_expr(
        root,
        (*join).plan.targetlist,
        outer_itlist,
        inner_itlist,
        0 as Index,
        rtoffset,
        if (*join).jointype == JOIN_INNER {
            NRM_EQUAL
        } else {
            NRM_SUPERSET
        },
        NUM_EXEC_TLIST(join as *mut Plan),
    );
    (*join).plan.qual = fix_join_expr(
        root,
        (*join).plan.qual,
        outer_itlist,
        inner_itlist,
        0 as Index,
        rtoffset,
        if (*join).jointype == JOIN_INNER {
            NRM_EQUAL
        } else {
            NRM_SUPERSET
        },
        NUM_EXEC_QUAL(join as *mut Plan),
    );

    pfree(outer_itlist as *mut c_void);
    pfree(inner_itlist as *mut c_void);
}

/*
 * set_upper_references
 *	  Update the targetlist and quals of an upper-level plan node
 *	  to refer to the tuples returned by its lefttree subplan.
 *	  Also perform opcode lookup for these expressions, and
 *	  add regclass OIDs to root->glob->relationOids.
 *
 * This is used for single-input plan types like Agg, Group, Result.
 *
 * In most cases, we have to match up individual Vars in the tlist and
 * qual expressions with elements of the subplan's tlist (which was
 * generated by flattening these selfsame expressions, so it should have all
 * the required variables).  There is an important exception, however:
 * depending on where we are in the plan tree, sort/group columns may have
 * been pushed into the subplan tlist unflattened.  If these values are also
 * needed in the output then we want to reference the subplan tlist element
 * rather than recomputing the expression.
 */
unsafe fn set_upper_references(root: *mut PlannerInfo, plan: *mut Plan, rtoffset: c_int) {
    let subplan: *mut Plan = (*plan).lefttree;
    let subplan_itlist: *mut indexed_tlist;
    let mut output_targetlist: *mut List;
    let mut l: *mut ListCell;

    subplan_itlist = build_tlist_index((*subplan).targetlist);

    /*
     * If it's a grouping node with grouping sets, any Vars and PHVs appearing
     * in the targetlist and quals should have nullingrels that include the
     * effects of the grouping step, ie they will have nullingrels equal to
     * the input Vars/PHVs' nullingrels plus the RT index of the grouping
     * step.  In order to perform exact nullingrels matches, we remove the RT
     * index of the grouping step first.
     */
    if IsA!(plan, T_Agg)
        && (*root).group_rtindex > 0
        && !(*(plan as *mut Agg)).groupingSets.is_null()
    {
        (*plan).targetlist = remove_nulling_relids(
            (*plan).targetlist as *mut Node,
            bms_make_singleton((*root).group_rtindex),
            ptr::null_mut(),
        ) as *mut List;
        (*plan).qual = remove_nulling_relids(
            (*plan).qual as *mut Node,
            bms_make_singleton((*root).group_rtindex),
            ptr::null_mut(),
        ) as *mut List;
    }

    output_targetlist = NIL;
    foreach!(l, (*plan).targetlist, {
        let mut tle: *mut TargetEntry = lfirst(current_cell!(l)) as *mut TargetEntry;
        let newexpr: *mut Node;

        /* If it's a sort/group item, first try to match by sortref */
        if (*tle).ressortgroupref != 0 {
            let mut tmp: *mut Node = search_indexed_tlist_for_sortgroupref(
                (*tle).expr,
                (*tle).ressortgroupref,
                subplan_itlist,
                OUTER_VAR,
            ) as *mut Node;
            if tmp.is_null() {
                tmp = fix_upper_expr(
                    root,
                    (*tle).expr as *mut Node,
                    subplan_itlist,
                    OUTER_VAR,
                    rtoffset,
                    NRM_EQUAL,
                    NUM_EXEC_TLIST(plan),
                );
            }
            newexpr = tmp;
        } else {
            newexpr = fix_upper_expr(
                root,
                (*tle).expr as *mut Node,
                subplan_itlist,
                OUTER_VAR,
                rtoffset,
                NRM_EQUAL,
                NUM_EXEC_TLIST(plan),
            );
        }
        tle = flatCopyTargetEntry(tle);
        (*tle).expr = newexpr as *mut Expr;
        output_targetlist = lappend(output_targetlist, tle as *mut c_void);
    });
    (*plan).targetlist = output_targetlist;

    (*plan).qual = fix_upper_expr(
        root,
        (*plan).qual as *mut Node,
        subplan_itlist,
        OUTER_VAR,
        rtoffset,
        NRM_EQUAL,
        NUM_EXEC_QUAL(plan),
    ) as *mut List;

    pfree(subplan_itlist as *mut c_void);
}

/*
 * set_param_references
 *	  Initialize the initParam list in Gather or Gather merge node such that
 *	  it contains reference of all the params that needs to be evaluated
 *	  before execution of the node.  It contains the initplan params that are
 *	  being passed to the plan nodes below it.
 */
unsafe fn set_param_references(root: *mut PlannerInfo, plan: *mut Plan) {
    Assert!(IsA!(plan, T_Gather) || IsA!(plan, T_GatherMerge));

    if !(*(*plan).lefttree).extParam.is_null() {
        let mut proot: *mut PlannerInfo;
        let mut initSetParam: *mut Bitmapset = ptr::null_mut();
        let mut l: *mut ListCell;

        proot = root;
        while !proot.is_null() {
            foreach!(l, (*proot).init_plans, {
                let initsubplan: *mut SubPlan = lfirst(current_cell!(l)) as *mut SubPlan;
                let mut l2: *mut ListCell;

                foreach!(l2, (*initsubplan).setParam, {
                    initSetParam =
                        bms_add_member(initSetParam, lfirst_int(current_cell!(l2)));
                });
            });
            proot = (*proot).parent_root;
        }

        /*
         * Remember the list of all external initplan params that are used by
         * the children of Gather or Gather merge node.
         */
        if IsA!(plan, T_Gather) {
            (*(plan as *mut Gather)).initParam =
                bms_intersect((*(*plan).lefttree).extParam, initSetParam);
        } else {
            (*(plan as *mut GatherMerge)).initParam =
                bms_intersect((*(*plan).lefttree).extParam, initSetParam);
        }
    }
}

/*
 * Recursively scan an expression tree and convert Aggrefs to the proper
 * intermediate form for combining aggregates.  This means (1) replacing each
 * one's argument list with a single argument that is the original Aggref
 * modified to show partial aggregation and (2) changing the upper Aggref to
 * show combining aggregation.
 *
 * After this step, set_upper_references will replace the partial Aggrefs
 * with Vars referencing the lower Agg plan node's outputs, so that the final
 * form seen by the executor is a combining Aggref with a Var as input.
 *
 * It's rather messy to postpone this step until setrefs.c; ideally it'd be
 * done in createplan.c.  The difficulty is that once we modify the Aggref
 * expressions, they will no longer be equal() to their original form and
 * so cross-plan-node-level matches will fail.  So this has to happen after
 * the plan node above the Agg has resolved its subplan references.
 */
unsafe fn convert_combining_aggrefs(node: *mut Node, context: *mut c_void) -> *mut Node {
    if node.is_null() {
        return ptr::null_mut();
    }
    if IsA!(node, T_Aggref) {
        let orig_agg: *mut Aggref = node as *mut Aggref;
        let child_agg: *mut Aggref;
        let parent_agg: *mut Aggref;

        /* Assert we've not chosen to partial-ize any unsupported cases */
        Assert!((*orig_agg).aggorder == NIL);
        Assert!((*orig_agg).aggdistinct == NIL);

        /*
         * Since aggregate calls can't be nested, we needn't recurse into the
         * arguments.  But for safety, flat-copy the Aggref node itself rather
         * than modifying it in-place.
         */
        child_agg = makeNode!(Aggref, T_Aggref);
        core::ptr::copy_nonoverlapping(orig_agg, child_agg, 1);

        /*
         * For the parent Aggref, we want to copy all the fields of the
         * original aggregate *except* the args list, which we'll replace
         * below, and the aggfilter expression, which should be applied only
         * by the child not the parent.  Rather than explicitly knowing about
         * all the other fields here, we can momentarily modify child_agg to
         * provide a suitable source for copyObject.
         */
        (*child_agg).args = NIL;
        (*child_agg).aggfilter = ptr::null_mut();
        parent_agg = copyObject(child_agg as *const Aggref);
        (*child_agg).args = (*orig_agg).args;
        (*child_agg).aggfilter = (*orig_agg).aggfilter;

        /*
         * Now, set up child_agg to represent the first phase of partial
         * aggregation.  For now, assume serialization is required.
         */
        mark_partial_aggref(child_agg, AGGSPLIT_INITIAL_SERIAL);

        /*
         * And set up parent_agg to represent the second phase.
         */
        (*parent_agg).args = crate::list_make1!(makeTargetEntry(
            child_agg as *mut Expr,
            1,
            ptr::null_mut(),
            false
        ) as *mut c_void);
        mark_partial_aggref(parent_agg, AGGSPLIT_FINAL_DESERIAL);

        return parent_agg as *mut Node;
    }
    expression_tree_mutator(node, Some(convert_combining_aggrefs), context)
}

/*
 * set_dummy_tlist_references
 *	  Replace the targetlist of an upper-level plan node with a simple
 *	  list of OUTER_VAR references to its child.
 *
 * This is used for plan types like Sort and Append that don't evaluate
 * their targetlists.  Although the executor doesn't care at all what's in
 * the tlist, EXPLAIN needs it to be realistic.
 *
 * Note: we could almost use set_upper_references() here, but it fails for
 * Append for lack of a lefttree subplan.  Single-purpose code is faster
 * anyway.
 */
unsafe fn set_dummy_tlist_references(plan: *mut Plan, rtoffset: c_int) {
    let mut output_targetlist: *mut List;
    let mut l: *mut ListCell;

    output_targetlist = NIL;
    foreach!(l, (*plan).targetlist, {
        let mut tle: *mut TargetEntry = lfirst(current_cell!(l)) as *mut TargetEntry;
        let oldvar: *mut Var = (*tle).expr as *mut Var;
        let newvar: *mut Var;

        /*
         * As in search_indexed_tlist_for_non_var(), we prefer to keep Consts
         * as Consts, not Vars referencing Consts.  Here, there's no speed
         * advantage to be had, but it makes EXPLAIN output look cleaner, and
         * again it avoids confusing the executor.
         */
        if IsA!(oldvar, T_Const) {
            /* just reuse the existing TLE node */
            output_targetlist = lappend(output_targetlist, tle as *mut c_void);
            continue;
        }

        newvar = makeVar(
            OUTER_VAR,
            (*tle).resno,
            exprType(oldvar as *const Node),
            exprTypmod(oldvar as *const Node),
            exprCollation(oldvar as *const Node),
            0,
        );
        if IsA!(oldvar, T_Var) && (*oldvar).varnosyn > 0 {
            (*newvar).varnosyn = (*oldvar).varnosyn + rtoffset as Index;
            (*newvar).varattnosyn = (*oldvar).varattnosyn;
        } else {
            (*newvar).varnosyn = 0; /* wasn't ever a plain Var */
            (*newvar).varattnosyn = 0;
        }

        tle = flatCopyTargetEntry(tle);
        (*tle).expr = newvar as *mut Expr;
        output_targetlist = lappend(output_targetlist, tle as *mut c_void);
    });
    (*plan).targetlist = output_targetlist;

    /* We don't touch plan->qual here */
}

/*
 * build_tlist_index --- build an index data structure for a child tlist
 *
 * In most cases, subplan tlists will be "flat" tlists with only Vars,
 * so we try to optimize that case by extracting information about Vars
 * in advance.  Matching a parent tlist to a child is still an O(N^2)
 * operation, but at least with a much smaller constant factor than plain
 * tlist_member() searches.
 *
 * The result of this function is an indexed_tlist struct to pass to
 * search_indexed_tlist_for_var() and siblings.
 * When done, the indexed_tlist may be freed with a single pfree().
 */
unsafe fn build_tlist_index(tlist: *mut List) -> *mut indexed_tlist {
    let itlist: *mut indexed_tlist;
    let mut vinfo: *mut tlist_vinfo;
    let mut l: *mut ListCell;

    /* Create data structure with enough slots for all tlist entries */
    itlist = palloc(
        core::mem::offset_of!(indexed_tlist, vars)
            + list_length(tlist) as usize * core::mem::size_of::<tlist_vinfo>(),
    ) as *mut indexed_tlist;

    (*itlist).tlist = tlist;
    (*itlist).has_ph_vars = false;
    (*itlist).has_non_vars = false;

    /* Find the Vars and fill in the index array */
    vinfo = (*itlist).vars.as_mut_ptr();
    foreach!(l, tlist, {
        let tle: *mut TargetEntry = lfirst(current_cell!(l)) as *mut TargetEntry;

        if !(*tle).expr.is_null() && IsA!((*tle).expr, T_Var) {
            let var: *mut Var = (*tle).expr as *mut Var;

            (*vinfo).varno = (*var).varno;
            (*vinfo).varattno = (*var).varattno;
            (*vinfo).resno = (*tle).resno;
            (*vinfo).varnullingrels = (*var).varnullingrels;
            vinfo = vinfo.add(1);
        } else if !(*tle).expr.is_null() && IsA!((*tle).expr, T_PlaceHolderVar) {
            (*itlist).has_ph_vars = true;
        } else {
            (*itlist).has_non_vars = true;
        }
    });

    (*itlist).num_vars =
        vinfo.offset_from((*itlist).vars.as_ptr()) as c_int;

    itlist
}

/*
 * build_tlist_index_other_vars --- build a restricted tlist index
 *
 * This is like build_tlist_index, but we only index tlist entries that
 * are Vars belonging to some rel other than the one specified.  We will set
 * has_ph_vars (allowing PlaceHolderVars to be matched), but not has_non_vars
 * (so nothing other than Vars and PlaceHolderVars can be matched).
 */
unsafe fn build_tlist_index_other_vars(tlist: *mut List, ignore_rel: c_int) -> *mut indexed_tlist {
    let itlist: *mut indexed_tlist;
    let mut vinfo: *mut tlist_vinfo;
    let mut l: *mut ListCell;

    /* Create data structure with enough slots for all tlist entries */
    itlist = palloc(
        core::mem::offset_of!(indexed_tlist, vars)
            + list_length(tlist) as usize * core::mem::size_of::<tlist_vinfo>(),
    ) as *mut indexed_tlist;

    (*itlist).tlist = tlist;
    (*itlist).has_ph_vars = false;
    (*itlist).has_non_vars = false;

    /* Find the desired Vars and fill in the index array */
    vinfo = (*itlist).vars.as_mut_ptr();
    foreach!(l, tlist, {
        let tle: *mut TargetEntry = lfirst(current_cell!(l)) as *mut TargetEntry;

        if !(*tle).expr.is_null() && IsA!((*tle).expr, T_Var) {
            let var: *mut Var = (*tle).expr as *mut Var;

            if (*var).varno != ignore_rel {
                (*vinfo).varno = (*var).varno;
                (*vinfo).varattno = (*var).varattno;
                (*vinfo).resno = (*tle).resno;
                (*vinfo).varnullingrels = (*var).varnullingrels;
                vinfo = vinfo.add(1);
            }
        } else if !(*tle).expr.is_null() && IsA!((*tle).expr, T_PlaceHolderVar) {
            (*itlist).has_ph_vars = true;
        }
    });

    (*itlist).num_vars =
        vinfo.offset_from((*itlist).vars.as_ptr()) as c_int;

    itlist
}

/*
 * search_indexed_tlist_for_var --- find a Var in an indexed tlist
 *
 * If a match is found, return a copy of the given Var with suitably
 * modified varno/varattno (to wit, newvarno and the resno of the TLE entry).
 * Also ensure that varnosyn is incremented by rtoffset.
 * If no match, return NULL.
 *
 * We cross-check the varnullingrels of the subplan output Var based on
 * nrm_match.  Most call sites should pass NRM_EQUAL indicating we expect
 * an exact match.  However, there are places where we haven't cleaned
 * things up completely, and we have to settle for allowing subset or
 * superset matches.
 */
unsafe fn search_indexed_tlist_for_var(
    var: *mut Var,
    itlist: *mut indexed_tlist,
    newvarno: c_int,
    rtoffset: c_int,
    nrm_match: NullingRelsMatch,
) -> *mut Var {
    let varno: c_int = (*var).varno;
    let varattno: AttrNumber = (*var).varattno;
    let mut vinfo: *mut tlist_vinfo;
    let mut i: c_int;

    vinfo = (*itlist).vars.as_mut_ptr();
    i = (*itlist).num_vars;
    while i > 0 {
        i -= 1;
        if (*vinfo).varno == varno && (*vinfo).varattno == varattno {
            /* Found a match */
            let newvar: *mut Var = copyVar(var);

            /*
             * Verify that we kept all the nullingrels machinations straight.
             *
             * XXX we skip the check for system columns and whole-row Vars.
             * That's because such Vars might be row identity Vars, which are
             * generated without any varnullingrels.  It'd be hard to do
             * otherwise, since they're normally made very early in planning,
             * when we haven't looked at the jointree yet and don't know which
             * joins might null such Vars.  Doesn't seem worth the expense to
             * make them fully valid.  (While it's slightly annoying that we
             * thereby lose checking for user-written references to such
             * columns, it seems unlikely that a bug in nullingrels logic
             * would affect only system columns.)
             */
            if !(varattno <= 0
                || (if nrm_match == NRM_SUBSET {
                    bms_is_subset((*var).varnullingrels, (*vinfo).varnullingrels)
                } else if nrm_match == NRM_SUPERSET {
                    bms_is_subset((*vinfo).varnullingrels, (*var).varnullingrels)
                } else {
                    bms_equal((*vinfo).varnullingrels, (*var).varnullingrels)
                }))
            {
                elog!(
                    ERROR,
                    "wrong varnullingrels {} (expected {}) for Var {}/{}",
                    std::ffi::CStr::from_ptr(bmsToString((*var).varnullingrels)).to_string_lossy(),
                    std::ffi::CStr::from_ptr(bmsToString((*vinfo).varnullingrels)).to_string_lossy(),
                    varno,
                    varattno
                );
            }

            (*newvar).varno = newvarno;
            (*newvar).varattno = (*vinfo).resno;
            if (*newvar).varnosyn > 0 {
                (*newvar).varnosyn = ((*newvar).varnosyn as c_int + rtoffset) as Index;
            }
            return newvar;
        }
        vinfo = vinfo.add(1);
    }
    ptr::null_mut() /* no match */
}

/*
 * search_indexed_tlist_for_phv --- find a PlaceHolderVar in an indexed tlist
 *
 * If a match is found, return a Var constructed to reference the tlist item.
 * If no match, return NULL.
 *
 * Cross-check phnullingrels as in search_indexed_tlist_for_var.
 *
 * NOTE: it is a waste of time to call this unless itlist->has_ph_vars.
 */
unsafe fn search_indexed_tlist_for_phv(
    phv: *mut PlaceHolderVar,
    itlist: *mut indexed_tlist,
    newvarno: c_int,
    nrm_match: NullingRelsMatch,
) -> *mut Var {
    let mut lc: *mut ListCell;

    foreach!(lc, (*itlist).tlist, {
        let tle: *mut TargetEntry = lfirst(current_cell!(lc)) as *mut TargetEntry;

        if !(*tle).expr.is_null() && IsA!((*tle).expr, T_PlaceHolderVar) {
            let subphv: *mut PlaceHolderVar = (*tle).expr as *mut PlaceHolderVar;
            let newvar: *mut Var;

            /*
             * Analogously to search_indexed_tlist_for_var, we match on phid
             * only.  We don't use equal(), partially for speed but mostly
             * because phnullingrels might not be exactly equal.
             */
            if (*phv).phid != (*subphv).phid {
                continue;
            }

            /* Verify that we kept all the nullingrels machinations straight */
            if !(if nrm_match == NRM_SUBSET {
                bms_is_subset((*phv).phnullingrels, (*subphv).phnullingrels)
            } else if nrm_match == NRM_SUPERSET {
                bms_is_subset((*subphv).phnullingrels, (*phv).phnullingrels)
            } else {
                bms_equal((*subphv).phnullingrels, (*phv).phnullingrels)
            }) {
                elog!(
                    ERROR,
                    "wrong phnullingrels {} (expected {}) for PlaceHolderVar {}",
                    std::ffi::CStr::from_ptr(bmsToString((*phv).phnullingrels)).to_string_lossy(),
                    std::ffi::CStr::from_ptr(bmsToString((*subphv).phnullingrels)).to_string_lossy(),
                    (*phv).phid
                );
            }

            /* Found a matching subplan output expression */
            newvar = makeVarFromTargetEntry(newvarno, tle);
            (*newvar).varnosyn = 0; /* wasn't ever a plain Var */
            (*newvar).varattnosyn = 0;
            return newvar;
        }
    });
    ptr::null_mut() /* no match */
}

/*
 * search_indexed_tlist_for_non_var --- find a non-Var/PHV in an indexed tlist
 *
 * If a match is found, return a Var constructed to reference the tlist item.
 * If no match, return NULL.
 *
 * NOTE: it is a waste of time to call this unless itlist->has_non_vars.
 */
unsafe fn search_indexed_tlist_for_non_var(
    node: *mut Expr,
    itlist: *mut indexed_tlist,
    newvarno: c_int,
) -> *mut Var {
    let tle: *mut TargetEntry;

    /*
     * If it's a simple Const, replacing it with a Var is silly, even if there
     * happens to be an identical Const below; a Var is more expensive to
     * execute than a Const.  What's more, replacing it could confuse some
     * places in the executor that expect to see simple Consts for, eg,
     * dropped columns.
     */
    if IsA!(node, T_Const) {
        return ptr::null_mut();
    }

    tle = tlist_member(node, (*itlist).tlist);
    if !tle.is_null() {
        /* Found a matching subplan output expression */
        let newvar: *mut Var;

        newvar = makeVarFromTargetEntry(newvarno, tle);
        (*newvar).varnosyn = 0; /* wasn't ever a plain Var */
        (*newvar).varattnosyn = 0;
        return newvar;
    }
    ptr::null_mut() /* no match */
}

/*
 * search_indexed_tlist_for_sortgroupref --- find a sort/group expression
 *
 * If a match is found, return a Var constructed to reference the tlist item.
 * If no match, return NULL.
 *
 * This is needed to ensure that we select the right subplan TLE in cases
 * where there are multiple textually-equal()-but-volatile sort expressions.
 * And it's also faster than search_indexed_tlist_for_non_var.
 */
unsafe fn search_indexed_tlist_for_sortgroupref(
    node: *mut Expr,
    sortgroupref: Index,
    itlist: *mut indexed_tlist,
    newvarno: c_int,
) -> *mut Var {
    let mut lc: *mut ListCell;

    foreach!(lc, (*itlist).tlist, {
        let tle: *mut TargetEntry = lfirst(current_cell!(lc)) as *mut TargetEntry;

        /*
         * Usually the equal() check is redundant, but in setop plans it may
         * not be, since prepunion.c assigns ressortgroupref equal to the
         * column resno without regard to whether that matches the topmost
         * level's sortgrouprefs and without regard to whether any implicit
         * coercions are added in the setop tree.  We might have to clean that
         * up someday; but for now, just ignore any false matches.
         */
        if (*tle).ressortgroupref == sortgroupref
            && equal(node as *const c_void, (*tle).expr as *const c_void)
        {
            /* Found a matching subplan output expression */
            let newvar: *mut Var;

            newvar = makeVarFromTargetEntry(newvarno, tle);
            (*newvar).varnosyn = 0; /* wasn't ever a plain Var */
            (*newvar).varattnosyn = 0;
            return newvar;
        }
    });
    ptr::null_mut() /* no match */
}

/*
 * fix_join_expr
 *	   Create a new set of targetlist entries or join qual clauses by
 *	   changing the varno/varattno values of variables in the clauses
 *	   to reference target list values from the outer and inner join
 *	   relation target lists.  Also perform opcode lookup and add
 *	   regclass OIDs to root->glob->relationOids.
 *
 * This is used in four different scenarios:
 * 1) a normal join clause, where all the Vars in the clause *must* be
 *	  replaced by OUTER_VAR or INNER_VAR references.  In this case
 *	  acceptable_rel should be zero so that any failure to match a Var will be
 *	  reported as an error.
 * 2) RETURNING clauses, which may contain both Vars of the target relation
 *	  and Vars of other relations. In this case we want to replace the
 *	  other-relation Vars by OUTER_VAR references, while leaving target Vars
 *	  alone. Thus inner_itlist = NULL and acceptable_rel = the ID of the
 *	  target relation should be passed.
 * 3) ON CONFLICT UPDATE SET/WHERE clauses.  Here references to EXCLUDED are
 *	  to be replaced with INNER_VAR references, while leaving target Vars (the
 *	  to-be-updated relation) alone. Correspondingly inner_itlist is to be
 *	  EXCLUDED elements, outer_itlist = NULL and acceptable_rel the target
 *	  relation.
 * 4) MERGE.  In this case, references to the source relation are to be
 *    replaced with INNER_VAR references, leaving Vars of the target
 *    relation (the to-be-modified relation) alone.  So inner_itlist is to be
 *    the source relation elements, outer_itlist = NULL and acceptable_rel
 *    the target relation.
 *
 * 'clauses' is the targetlist or list of join clauses
 * 'outer_itlist' is the indexed target list of the outer join relation,
 *		or NULL
 * 'inner_itlist' is the indexed target list of the inner join relation,
 *		or NULL
 * 'acceptable_rel' is either zero or the rangetable index of a relation
 *		whose Vars may appear in the clause without provoking an error
 * 'rtoffset': how much to increment varnos by
 * 'nrm_match': as for search_indexed_tlist_for_var()
 * 'num_exec': estimated number of executions of expression
 *
 * Returns the new expression tree.  The original clause structure is
 * not modified.
 */
unsafe fn fix_join_expr(
    root: *mut PlannerInfo,
    clauses: *mut List,
    outer_itlist: *mut indexed_tlist,
    inner_itlist: *mut indexed_tlist,
    acceptable_rel: Index,
    rtoffset: c_int,
    nrm_match: NullingRelsMatch,
    num_exec: f64,
) -> *mut List {
    let mut context = fix_join_expr_context {
        root,
        outer_itlist,
        inner_itlist,
        acceptable_rel,
        rtoffset,
        nrm_match,
        num_exec,
    };
    fix_join_expr_mutator(clauses as *mut Node, &mut context as *mut _ as *mut c_void) as *mut List
}

unsafe fn fix_join_expr_mutator(node: *mut Node, context_ptr: *mut c_void) -> *mut Node {
    let context = context_ptr as *mut fix_join_expr_context;
    let mut newvar: *mut Var;

    if node.is_null() {
        return ptr::null_mut();
    }
    if IsA!(node, T_Var) {
        let mut var: *mut Var = node as *mut Var;

        /*
         * Verify that Vars with non-default varreturningtype only appear in
         * the RETURNING list, and refer to the target relation.
         */
        if (*var).varreturningtype != VAR_RETURNING_DEFAULT {
            if !(*context).inner_itlist.is_null()
                || (*context).outer_itlist.is_null()
                || (*context).acceptable_rel == 0
            {
                elog!(
                    ERROR,
                    "variable returning old/new found outside RETURNING list"
                );
            }
            if (*var).varno != (*context).acceptable_rel as c_int {
                elog!(
                    ERROR,
                    "wrong varno {} (expected {}) for variable returning old/new",
                    (*var).varno,
                    (*context).acceptable_rel
                );
            }
        }

        /* Look for the var in the input tlists, first in the outer */
        if !(*context).outer_itlist.is_null() {
            newvar = search_indexed_tlist_for_var(
                var,
                (*context).outer_itlist,
                OUTER_VAR,
                (*context).rtoffset,
                (*context).nrm_match,
            );
            if !newvar.is_null() {
                return newvar as *mut Node;
            }
        }

        /* then in the inner. */
        if !(*context).inner_itlist.is_null() {
            newvar = search_indexed_tlist_for_var(
                var,
                (*context).inner_itlist,
                INNER_VAR,
                (*context).rtoffset,
                (*context).nrm_match,
            );
            if !newvar.is_null() {
                return newvar as *mut Node;
            }
        }

        /* If it's for acceptable_rel, adjust and return it */
        if (*var).varno == (*context).acceptable_rel as c_int {
            var = copyVar(var);
            (*var).varno += (*context).rtoffset;
            if (*var).varnosyn > 0 {
                (*var).varnosyn = ((*var).varnosyn as c_int + (*context).rtoffset) as Index;
            }
            return var as *mut Node;
        }

        /* No referent found for Var */
        elog!(ERROR, "variable not found in subplan target lists");
    }
    if IsA!(node, T_PlaceHolderVar) {
        let phv: *mut PlaceHolderVar = node as *mut PlaceHolderVar;

        /* See if the PlaceHolderVar has bubbled up from a lower plan node */
        if !(*context).outer_itlist.is_null() && (*(*context).outer_itlist).has_ph_vars {
            newvar = search_indexed_tlist_for_phv(
                phv,
                (*context).outer_itlist,
                OUTER_VAR,
                (*context).nrm_match,
            );
            if !newvar.is_null() {
                return newvar as *mut Node;
            }
        }
        if !(*context).inner_itlist.is_null() && (*(*context).inner_itlist).has_ph_vars {
            newvar = search_indexed_tlist_for_phv(
                phv,
                (*context).inner_itlist,
                INNER_VAR,
                (*context).nrm_match,
            );
            if !newvar.is_null() {
                return newvar as *mut Node;
            }
        }

        /* If not supplied by input plans, evaluate the contained expr */
        /* XXX can we assert something about phnullingrels? */
        return fix_join_expr_mutator((*phv).phexpr as *mut Node, context_ptr);
    }
    /* Try matching more complex expressions too, if tlists have any */
    if !(*context).outer_itlist.is_null() && (*(*context).outer_itlist).has_non_vars {
        newvar =
            search_indexed_tlist_for_non_var(node as *mut Expr, (*context).outer_itlist, OUTER_VAR);
        if !newvar.is_null() {
            return newvar as *mut Node;
        }
    }
    if !(*context).inner_itlist.is_null() && (*(*context).inner_itlist).has_non_vars {
        newvar =
            search_indexed_tlist_for_non_var(node as *mut Expr, (*context).inner_itlist, INNER_VAR);
        if !newvar.is_null() {
            return newvar as *mut Node;
        }
    }
    /* Special cases (apply only AFTER failing to match to lower tlist) */
    if IsA!(node, T_Param) {
        return fix_param_node((*context).root, node as *mut Param);
    }
    if IsA!(node, T_AlternativeSubPlan) {
        return fix_join_expr_mutator(
            fix_alternative_subplan(
                (*context).root,
                node as *mut AlternativeSubPlan,
                (*context).num_exec,
            ),
            context_ptr,
        );
    }
    fix_expr_common((*context).root, node);
    expression_tree_mutator(node, Some(fix_join_expr_mutator), context_ptr)
}

/*
 * fix_upper_expr
 *		Modifies an expression tree so that all Var nodes reference outputs
 *		of a subplan.  Also looks for Aggref nodes that should be replaced
 *		by initplan output Params.  Also performs opcode lookup, and adds
 *		regclass OIDs to root->glob->relationOids.
 *
 * This is used to fix up target and qual expressions of non-join upper-level
 * plan nodes, as well as index-only scan nodes.
 *
 * An error is raised if no matching var can be found in the subplan tlist
 * --- so this routine should only be applied to nodes whose subplans'
 * targetlists were generated by flattening the expressions used in the
 * parent node.
 *
 * If itlist->has_non_vars is true, then we try to match whole subexpressions
 * against elements of the subplan tlist, so that we can avoid recomputing
 * expressions that were already computed by the subplan.  (This is relatively
 * expensive, so we don't want to try it in the common case where the
 * subplan tlist is just a flattened list of Vars.)
 *
 * 'node': the tree to be fixed (a target item or qual)
 * 'subplan_itlist': indexed target list for subplan (or index)
 * 'newvarno': varno to use for Vars referencing tlist elements
 * 'rtoffset': how much to increment varnos by
 * 'nrm_match': as for search_indexed_tlist_for_var()
 * 'num_exec': estimated number of executions of expression
 *
 * The resulting tree is a copy of the original in which all Var nodes have
 * varno = newvarno, varattno = resno of corresponding targetlist element.
 * The original tree is not modified.
 */
unsafe fn fix_upper_expr(
    root: *mut PlannerInfo,
    node: *mut Node,
    subplan_itlist: *mut indexed_tlist,
    newvarno: c_int,
    rtoffset: c_int,
    nrm_match: NullingRelsMatch,
    num_exec: f64,
) -> *mut Node {
    let mut context = fix_upper_expr_context {
        root,
        subplan_itlist,
        newvarno,
        rtoffset,
        nrm_match,
        num_exec,
    };
    fix_upper_expr_mutator(node, &mut context as *mut _ as *mut c_void)
}

unsafe fn fix_upper_expr_mutator(node: *mut Node, context_ptr: *mut c_void) -> *mut Node {
    let context = context_ptr as *mut fix_upper_expr_context;
    let newvar: *mut Var;

    if node.is_null() {
        return ptr::null_mut();
    }
    if IsA!(node, T_Var) {
        let var: *mut Var = node as *mut Var;

        newvar = search_indexed_tlist_for_var(
            var,
            (*context).subplan_itlist,
            (*context).newvarno,
            (*context).rtoffset,
            (*context).nrm_match,
        );
        if newvar.is_null() {
            elog!(ERROR, "variable not found in subplan target list");
        }
        return newvar as *mut Node;
    }
    if IsA!(node, T_PlaceHolderVar) {
        let phv: *mut PlaceHolderVar = node as *mut PlaceHolderVar;

        /* See if the PlaceHolderVar has bubbled up from a lower plan node */
        if (*(*context).subplan_itlist).has_ph_vars {
            newvar = search_indexed_tlist_for_phv(
                phv,
                (*context).subplan_itlist,
                (*context).newvarno,
                (*context).nrm_match,
            );
            if !newvar.is_null() {
                return newvar as *mut Node;
            }
        }
        /* If not supplied by input plan, evaluate the contained expr */
        /* XXX can we assert something about phnullingrels? */
        return fix_upper_expr_mutator((*phv).phexpr as *mut Node, context_ptr);
    }
    /* Try matching more complex expressions too, if tlist has any */
    if (*(*context).subplan_itlist).has_non_vars {
        newvar = search_indexed_tlist_for_non_var(
            node as *mut Expr,
            (*context).subplan_itlist,
            (*context).newvarno,
        );
        if !newvar.is_null() {
            return newvar as *mut Node;
        }
    }
    /* Special cases (apply only AFTER failing to match to lower tlist) */
    if IsA!(node, T_Param) {
        return fix_param_node((*context).root, node as *mut Param);
    }
    if IsA!(node, T_Aggref) {
        let aggref: *mut Aggref = node as *mut Aggref;
        let aggparam: *mut Param;

        /* See if the Aggref should be replaced by a Param */
        aggparam = find_minmax_agg_replacement_param((*context).root, aggref);
        if !aggparam.is_null() {
            /* Make a copy of the Param for paranoia's sake */
            return copyObject(aggparam as *const Param) as *mut Node;
        }
        /* If no match, just fall through to process it normally */
    }
    if IsA!(node, T_AlternativeSubPlan) {
        return fix_upper_expr_mutator(
            fix_alternative_subplan(
                (*context).root,
                node as *mut AlternativeSubPlan,
                (*context).num_exec,
            ),
            context_ptr,
        );
    }
    fix_expr_common((*context).root, node);
    expression_tree_mutator(node, Some(fix_upper_expr_mutator), context_ptr)
}

/*
 * set_returning_clause_references
 *		Perform setrefs.c's work on a RETURNING targetlist
 *
 * If the query involves more than just the result table, we have to
 * adjust any Vars that refer to other tables to reference junk tlist
 * entries in the top subplan's targetlist.  Vars referencing the result
 * table should be left alone, however (the executor will evaluate them
 * using the actual heap tuple, after firing triggers if any).  In the
 * adjusted RETURNING list, result-table Vars will have their original
 * varno (plus rtoffset), but Vars for other rels will have varno OUTER_VAR.
 *
 * We also must perform opcode lookup and add regclass OIDs to
 * root->glob->relationOids.
 *
 * 'rlist': the RETURNING targetlist to be fixed
 * 'topplan': the top subplan node that will be just below the ModifyTable
 *		node (note it's not yet passed through set_plan_refs)
 * 'resultRelation': RT index of the associated result relation
 * 'rtoffset': how much to increment varnos by
 *
 * Note: the given 'root' is for the parent query level, not the 'topplan'.
 * This does not matter currently since we only access the dependency-item
 * lists in root->glob, but it would need some hacking if we wanted a root
 * that actually matches the subplan.
 *
 * Note: resultRelation is not yet adjusted by rtoffset.
 */
unsafe fn set_returning_clause_references(
    root: *mut PlannerInfo,
    mut rlist: *mut List,
    topplan: *mut Plan,
    resultRelation: Index,
    rtoffset: c_int,
) -> *mut List {
    let itlist: *mut indexed_tlist;

    /*
     * We can perform the desired Var fixup by abusing the fix_join_expr
     * machinery that formerly handled inner indexscan fixup.  We search the
     * top plan's targetlist for Vars of non-result relations, and use
     * fix_join_expr to convert RETURNING Vars into references to those tlist
     * entries, while leaving result-rel Vars as-is.
     *
     * PlaceHolderVars will also be sought in the targetlist, but no
     * more-complex expressions will be.  Note that it is not possible for a
     * PlaceHolderVar to refer to the result relation, since the result is
     * never below an outer join.  If that case could happen, we'd have to be
     * prepared to pick apart the PlaceHolderVar and evaluate its contained
     * expression instead.
     */
    itlist = build_tlist_index_other_vars((*topplan).targetlist, resultRelation as c_int);

    rlist = fix_join_expr(
        root,
        rlist,
        itlist,
        ptr::null_mut(),
        resultRelation,
        rtoffset,
        NRM_EQUAL,
        NUM_EXEC_TLIST(topplan),
    );

    pfree(itlist as *mut c_void);

    rlist
}

/*
 * fix_windowagg_condition_expr_mutator
 *		Mutator function for replacing WindowFuncs with the corresponding Var
 *		in the targetlist which references that WindowFunc.
 */
unsafe fn fix_windowagg_condition_expr_mutator(node: *mut Node, context_ptr: *mut c_void) -> *mut Node {
    let context = context_ptr as *mut fix_windowagg_cond_context;

    if node.is_null() {
        return ptr::null_mut();
    }

    if IsA!(node, T_WindowFunc) {
        let newvar: *mut Var;

        newvar = search_indexed_tlist_for_non_var(
            node as *mut Expr,
            (*context).subplan_itlist,
            (*context).newvarno,
        );
        if !newvar.is_null() {
            return newvar as *mut Node;
        }
        elog!(ERROR, "WindowFunc not found in subplan target lists");
    }

    expression_tree_mutator(node, Some(fix_windowagg_condition_expr_mutator), context_ptr)
}

/*
 * fix_windowagg_condition_expr
 *		Converts references in 'runcondition' so that any WindowFunc
 *		references are swapped out for a Var which references the matching
 *		WindowFunc in 'subplan_itlist'.
 */
unsafe fn fix_windowagg_condition_expr(
    root: *mut PlannerInfo,
    runcondition: *mut List,
    subplan_itlist: *mut indexed_tlist,
) -> *mut List {
    let mut context = fix_windowagg_cond_context {
        root,
        subplan_itlist,
        newvarno: 0,
    };

    fix_windowagg_condition_expr_mutator(
        runcondition as *mut Node,
        &mut context as *mut _ as *mut c_void,
    ) as *mut List
}

/*
 * set_windowagg_runcondition_references
 *		Converts references in 'runcondition' so that any WindowFunc
 *		references are swapped out for a Var which references the matching
 *		WindowFunc in 'plan' targetlist.
 */
unsafe fn set_windowagg_runcondition_references(
    root: *mut PlannerInfo,
    runcondition: *mut List,
    plan: *mut Plan,
) -> *mut List {
    let newlist: *mut List;
    let itlist: *mut indexed_tlist;

    itlist = build_tlist_index((*plan).targetlist);

    newlist = fix_windowagg_condition_expr(root, runcondition, itlist);

    pfree(itlist as *mut c_void);

    newlist
}

/*
 * find_minmax_agg_replacement_param
 *		If the given Aggref is one that we are optimizing into a subquery
 *		(cf. planagg.c), then return the Param that should replace it.
 *		Else return NULL.
 *
 * This is exported so that SS_finalize_plan can use it before setrefs.c runs.
 * Note that it will not find anything until we have built a Plan from a
 * MinMaxAggPath, as root->minmax_aggs will never be filled otherwise.
 */
pub unsafe fn find_minmax_agg_replacement_param(
    root: *mut PlannerInfo,
    aggref: *mut Aggref,
) -> *mut Param {
    if (*root).minmax_aggs != NIL && list_length((*aggref).args) == 1 {
        let curTarget: *mut TargetEntry = linitial((*aggref).args) as *mut TargetEntry;
        let mut lc: *mut ListCell;

        foreach!(lc, (*root).minmax_aggs, {
            let mminfo: *mut MinMaxAggInfo = lfirst(current_cell!(lc)) as *mut MinMaxAggInfo;

            if (*mminfo).aggfnoid == (*aggref).aggfnoid
                && equal((*mminfo).target as *const c_void, (*curTarget).expr as *const c_void)
            {
                return (*mminfo).param;
            }
        });
    }
    ptr::null_mut()
}

/*****************************************************************************
 *					QUERY DEPENDENCY MANAGEMENT
 *****************************************************************************/

/*
 * record_plan_function_dependency
 *		Mark the current plan as depending on a particular function.
 *
 * This is exported so that the function-inlining code can record a
 * dependency on a function that it's removed from the plan tree.
 */
pub unsafe fn record_plan_function_dependency(root: *mut PlannerInfo, funcid: Oid) {
    /*
     * For performance reasons, we don't bother to track built-in functions;
     * we just assume they'll never change (or at least not in ways that'd
     * invalidate plans using them).  For this purpose we can consider a
     * built-in function to be one with OID less than FirstUnpinnedObjectId.
     * Note that the OID generator guarantees never to generate such an OID
     * after startup, even at OID wraparound.
     */
    if funcid >= FirstUnpinnedObjectId as Oid {
        let inval_item: *mut PlanInvalItem = makeNode!(PlanInvalItem, T_PlanInvalItem);

        /*
         * It would work to use any syscache on pg_proc, but the easiest is
         * PROCOID since we already have the function's OID at hand.  Note
         * that plancache.c knows we use PROCOID.
         */
        (*inval_item).cacheId = PROCOID;
        (*inval_item).hashValue =
            GetSysCacheHashValue1(PROCOID, ObjectIdGetDatum(funcid));

        (*(*root).glob).invalItems =
            lappend((*(*root).glob).invalItems, inval_item as *mut c_void);
    }
}

/*
 * record_plan_type_dependency
 *		Mark the current plan as depending on a particular type.
 *
 * This is exported so that eval_const_expressions can record a
 * dependency on a domain that it's removed a CoerceToDomain node for.
 *
 * We don't currently need to record dependencies on domains that the
 * plan contains CoerceToDomain nodes for, though that might change in
 * future.  Hence, this isn't actually called in this module, though
 * someday fix_expr_common might call it.
 */
pub unsafe fn record_plan_type_dependency(root: *mut PlannerInfo, typid: Oid) {
    /*
     * As in record_plan_function_dependency, ignore the possibility that
     * someone would change a built-in domain.
     */
    if typid >= FirstUnpinnedObjectId as Oid {
        let inval_item: *mut PlanInvalItem = makeNode!(PlanInvalItem, T_PlanInvalItem);

        /*
         * It would work to use any syscache on pg_type, but the easiest is
         * TYPEOID since we already have the type's OID at hand.  Note that
         * plancache.c knows we use TYPEOID.
         */
        (*inval_item).cacheId = TYPEOID;
        (*inval_item).hashValue =
            GetSysCacheHashValue1(TYPEOID, ObjectIdGetDatum(typid));

        (*(*root).glob).invalItems =
            lappend((*(*root).glob).invalItems, inval_item as *mut c_void);
    }
}

/*
 * extract_query_dependencies
 *		Given a rewritten, but not yet planned, query or queries
 *		(i.e. a Query node or list of Query nodes), extract dependencies
 *		just as set_plan_references would do.  Also detect whether any
 *		rewrite steps were affected by RLS.
 *
 * This is needed by plancache.c to handle invalidation of cached unplanned
 * queries.
 *
 * Note: this does not go through eval_const_expressions, and hence doesn't
 * reflect its additions of inlined functions and elided CoerceToDomain nodes
 * to the invalItems list.  This is obviously OK for functions, since we'll
 * see them in the original query tree anyway.  For domains, it's OK because
 * we don't care about domains unless they get elided.  That is, a plan might
 * have domain dependencies that the query tree doesn't.
 */
pub unsafe fn extract_query_dependencies(
    query: *mut Node,
    relationOids: *mut *mut List,
    invalItems: *mut *mut List,
    hasRowSecurity: *mut bool,
) {
    let mut glob: PlannerGlobal = core::mem::zeroed();
    let mut root: PlannerInfo = core::mem::zeroed();

    /* Make up dummy planner state so we can use this module's machinery */
    MemSet(
        &raw mut glob as *mut c_void,
        0,
        core::mem::size_of::<PlannerGlobal>(),
    );
    glob.r#type = NodeTag::T_PlannerGlobal;
    glob.relationOids = NIL;
    glob.invalItems = NIL;
    /* Hack: we use glob.dependsOnRole to collect hasRowSecurity flags */
    glob.dependsOnRole = false;

    MemSet(
        &raw mut root as *mut c_void,
        0,
        core::mem::size_of::<PlannerInfo>(),
    );
    root.r#type = NodeTag::T_PlannerInfo;
    root.glob = &raw mut glob;

    extract_query_dependencies_walker(query, &raw mut root as *mut c_void);

    *relationOids = glob.relationOids;
    *invalItems = glob.invalItems;
    *hasRowSecurity = glob.dependsOnRole;
}

/*
 * Tree walker for extract_query_dependencies.
 *
 * This is exported so that expression_planner_with_deps can call it on
 * simple expressions (post-planning, not before planning, in that case).
 * In that usage, glob.dependsOnRole isn't meaningful, but the relationOids
 * and invalItems lists are added to as needed.
 */
pub unsafe fn extract_query_dependencies_walker(node: *mut Node, context_ptr: *mut c_void) -> bool {
    let context = context_ptr as *mut PlannerInfo;

    if node.is_null() {
        return false;
    }
    Assert!(!IsA!(node, T_PlaceHolderVar));
    if IsA!(node, T_Query) {
        let mut query: *mut Query = node as *mut Query;
        let mut lc: *mut ListCell;

        if (*query).commandType == CMD_UTILITY {
            /*
             * This logic must handle any utility command for which parse
             * analysis was nontrivial (cf. stmt_requires_parse_analysis).
             *
             * Notably, CALL requires its own processing.
             */
            if IsA!((*query).utilityStmt, T_CallStmt) {
                let callstmt: *mut CallStmt = (*query).utilityStmt as *mut CallStmt;

                /* We need not examine funccall, just the transformed exprs */
                extract_query_dependencies_walker((*callstmt).funcexpr as *mut Node, context_ptr);
                extract_query_dependencies_walker((*callstmt).outargs as *mut Node, context_ptr);
                return false;
            }

            /*
             * Ignore other utility statements, except those (such as EXPLAIN)
             * that contain a parsed-but-not-planned query.  For those, we
             * just need to transfer our attention to the contained query.
             */
            query = UtilityContainsQuery((*query).utilityStmt);
            if query.is_null() {
                return false;
            }
        }

        /* Remember if any Query has RLS quals applied by rewriter */
        if (*query).hasRowSecurity {
            (*(*context).glob).dependsOnRole = true;
        }

        /* Collect relation OIDs in this Query's rtable */
        foreach!(lc, (*query).rtable, {
            let rte: *mut RangeTblEntry = lfirst(current_cell!(lc)) as *mut RangeTblEntry;

            if (*rte).rtekind == RTE_RELATION
                || ((*rte).rtekind == RTE_SUBQUERY && OidIsValid((*rte).relid))
                || ((*rte).rtekind == RTE_NAMEDTUPLESTORE && OidIsValid((*rte).relid))
            {
                (*(*context).glob).relationOids =
                    lappend_oid((*(*context).glob).relationOids, (*rte).relid);
            }
        });

        /* And recurse into the query's subexpressions */
        return query_tree_walker(
            query,
            Some(extract_query_dependencies_walker),
            context_ptr,
            0,
        );
    }
    /* Extract function dependencies and check for regclass Consts */
    fix_expr_common(context, node);
    expression_tree_walker(node, Some(extract_query_dependencies_walker), context_ptr)
}
