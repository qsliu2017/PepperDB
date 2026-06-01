//! Translation of postgres/src/backend/optimizer/prep/prepunion.c
//!
//! Routines to plan set-operation queries.  The filename is a leftover
//! from a time when only UNIONs were implemented.
//!
//! There are two code paths in the planner for set-operation queries.
//! If a subquery consists entirely of simple UNION ALL operations, it
//! is converted into an "append relation".  Otherwise, it is handled
//! by the general code in this module (plan_set_operations and its
//! subroutines).  There is some support code here for the append-relation
//! case, but most of the heavy lifting for that is done elsewhere,
//! notably in prepjointree.c and allpaths.c.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!   src/backend/optimizer/prep/prepunion.c

use crate::prelude::*;
use core::ffi::{c_int, c_void};
use std::ptr;

use crate::nodes::bitmapset::{Bitmapset, bms_union, bms_is_empty};
use crate::nodes::nodes::{
    Node,
    NodeTag::{T_RangeTblRef, T_SetOperationStmt},
    nodeTag,
    SetOpCmd::{self, SETOPCMD_INTERSECT, SETOPCMD_INTERSECT_ALL, SETOPCMD_EXCEPT, SETOPCMD_EXCEPT_ALL},
    SetOpStrategy::{SETOP_HASHED, SETOP_SORTED},
    AggStrategy::AGG_HASHED,
    AggSplit::AGGSPLIT_SIMPLE,
};
use crate::nodes::parsenodes::{
    Query, RangeTblEntry, RTEKind::RTE_SUBQUERY,
    SetOperationStmt,
    SetOperation::{SETOP_UNION, SETOP_INTERSECT, SETOP_EXCEPT},
    SortGroupClause,
};
use crate::nodes::pathnodes::{
    PlannerInfo, RelOptInfo, Path, PathTarget,
    UpperRelationKind::{UPPERREL_SETOP, UPPERREL_FINAL},
};
use crate::nodes::pg_list::{
    List, ListCell,
    lappend, lappend_int, lcons, linitial, lfirst, lfirst_int, lfirst_oid,
    list_head, list_length, lnext, list_delete_first,
    NIL,
};
use crate::nodes::primnodes::{
    Expr, RangeTblRef, TargetEntry, Var,
};
use crate::postgres_ext::{Oid, InvalidOid};
use crate::c::Index;
use crate::c::int32;
use crate::access::attnum::AttrNumber;
use crate::{
    IsA, foreach, current_cell, forthree, forfour, list_make1, list_make2,
};

// ---------------------------------------------------------------------------
// STUBs for unported dependencies
// ---------------------------------------------------------------------------

/// TODO(pg-port): nodes/nodeFuncs.c exprType
unsafe fn exprType(node: *const Node) -> Oid {
    InvalidOid
}

/// TODO(pg-port): nodes/nodeFuncs.c exprTypmod
unsafe fn exprTypmod(node: *const Node) -> int32 {
    -1
}

/// TODO(pg-port): nodes/nodeFuncs.c exprCollation
unsafe fn exprCollation(node: *const Node) -> Oid {
    InvalidOid
}

/// TODO(pg-port): nodes/makefuncs.c makeVar
unsafe fn makeVar(
    varno: Index,
    varattno: AttrNumber,
    vartype: Oid,
    vartypmod: int32,
    varcollid: Oid,
    varlevelsup: c_int,
) -> *mut Var {
    ptr::null_mut()
}

/// TODO(pg-port): nodes/makefuncs.c makeTargetEntry
unsafe fn makeTargetEntry(
    expr: *mut Expr,
    resno: AttrNumber,
    resname: *mut c_char,
    resjunk: bool,
) -> *mut TargetEntry {
    ptr::null_mut()
}

/// TODO(pg-port): utils/palloc.h pstrdup
unsafe fn pstrdup(s: *const c_char) -> *mut c_char {
    ptr::null_mut()
}

/// TODO(pg-port): miscadmin.h check_stack_depth
unsafe fn check_stack_depth() {}

/// TODO(pg-port): nodes/equalfuncs.c equal
unsafe fn equal(a: *const c_void, b: *const c_void) -> bool {
    false
}

/// TODO(pg-port): nodes/copyfuncs.c copyObject
unsafe fn copyObject<T>(obj: *const T) -> *mut T {
    ptr::null_mut()
}

/// TODO(pg-port): optimizer/util/relnode.c fetch_upper_rel
unsafe fn fetch_upper_rel(
    root: *mut PlannerInfo,
    kind: crate::nodes::pathnodes::UpperRelationKind,
    relids: *mut Bitmapset,
) -> *mut RelOptInfo {
    ptr::null_mut()
}

/// TODO(pg-port): optimizer/util/relnode.c build_simple_rel
unsafe fn build_simple_rel(
    root: *mut PlannerInfo,
    relid: c_int,
    parent: *mut RelOptInfo,
) -> *mut RelOptInfo {
    ptr::null_mut()
}

/// TODO(pg-port): optimizer/plan/planmain.c setup_simple_rel_arrays
unsafe fn setup_simple_rel_arrays(root: *mut PlannerInfo) {}

/// TODO(pg-port): optimizer/path/allpaths.c subquery_planner
unsafe fn subquery_planner(
    glob: *mut crate::nodes::pathnodes::PlannerGlobal,
    parse: *mut Query,
    parent_root: *mut PlannerInfo,
    hasRecursion: bool,
    tuple_fraction: f64,
    setops: *mut SetOperationStmt,
) -> *mut PlannerInfo {
    ptr::null_mut()
}

/// TODO(pg-port): optimizer/util/tlist.c create_pathtarget
unsafe fn create_pathtarget(root: *mut PlannerInfo, tlist: *mut List) -> *mut PathTarget {
    ptr::null_mut()
}

/// TODO(pg-port): optimizer/util/tlist.c make_tlist_from_pathtarget
unsafe fn make_tlist_from_pathtarget(target: *mut PathTarget) -> *mut List {
    NIL
}

/// TODO(pg-port): optimizer/util/tlist.c tlist_same_datatypes
unsafe fn tlist_same_datatypes(tlist: *mut List, colTypes: *mut List, junkOK: bool) -> bool {
    false
}

/// TODO(pg-port): optimizer/util/tlist.c tlist_same_collations
unsafe fn tlist_same_collations(tlist: *mut List, colCollations: *mut List, junkOK: bool) -> bool {
    false
}

/// TODO(pg-port): optimizer/util/tlist.c get_tlist_exprs
unsafe fn get_tlist_exprs(tlist: *mut List, includeJunk: bool) -> *mut List {
    NIL
}

/// TODO(pg-port): optimizer/util/pathnode.c add_path
unsafe fn add_path(parent_rel: *mut RelOptInfo, new_path: *mut Path) {}

/// TODO(pg-port): optimizer/util/pathnode.c add_partial_path
unsafe fn add_partial_path(parent_rel: *mut RelOptInfo, new_path: *mut Path) {}

/// TODO(pg-port): optimizer/util/pathnode.c create_append_path
unsafe fn create_append_path(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    subpaths: *mut List,
    partial_subpaths: *mut List,
    pathkeys: *mut List,
    required_outer: *mut Bitmapset,
    parallel_workers: c_int,
    parallel_aware: bool,
    rows: f64,
) -> *mut Path {
    ptr::null_mut()
}

/// TODO(pg-port): optimizer/util/pathnode.c create_merge_append_path
unsafe fn create_merge_append_path(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    subpaths: *mut List,
    pathkeys: *mut List,
    required_outer: *mut Bitmapset,
) -> *mut Path {
    ptr::null_mut()
}

/// TODO(pg-port): optimizer/util/pathnode.c create_gather_path
unsafe fn create_gather_path(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    subpath: *mut Path,
    target: *mut PathTarget,
    required_outer: *mut Bitmapset,
    rows: *mut f64,
) -> *mut Path {
    ptr::null_mut()
}

/// TODO(pg-port): optimizer/util/pathnode.c create_agg_path
unsafe fn create_agg_path(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    subpath: *mut Path,
    target: *mut PathTarget,
    aggstrategy: crate::nodes::nodes::AggStrategy,
    aggsplit: crate::nodes::nodes::AggSplit,
    groupClause: *mut List,
    qual: *mut List,
    aggcosts: *const c_void,
    numGroups: f64,
) -> *mut Path {
    ptr::null_mut()
}

/// TODO(pg-port): optimizer/util/pathnode.c create_sort_path
unsafe fn create_sort_path(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    subpath: *mut Path,
    pathkeys: *mut List,
    limit_tuples: f64,
) -> *mut Path {
    ptr::null_mut()
}

/// TODO(pg-port): optimizer/util/pathnode.c create_incremental_sort_path
unsafe fn create_incremental_sort_path(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    subpath: *mut Path,
    pathkeys: *mut List,
    presorted_keys: c_int,
    limit_tuples: f64,
) -> *mut Path {
    ptr::null_mut()
}

/// TODO(pg-port): optimizer/util/pathnode.c create_upper_unique_path
unsafe fn create_upper_unique_path(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    subpath: *mut Path,
    numCols: c_int,
    numGroups: f64,
) -> *mut Path {
    ptr::null_mut()
}

/// TODO(pg-port): optimizer/util/pathnode.c create_projection_path
unsafe fn create_projection_path(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    subpath: *mut Path,
    target: *mut PathTarget,
) -> *mut Path {
    ptr::null_mut()
}

/// TODO(pg-port): optimizer/util/pathnode.c apply_projection_to_path
unsafe fn apply_projection_to_path(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    path: *mut Path,
    target: *mut PathTarget,
) -> *mut Path {
    path
}

/// TODO(pg-port): optimizer/util/pathnode.c create_subqueryscan_path
unsafe fn create_subqueryscan_path(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    subpath: *mut Path,
    trivial_tlist: bool,
    pathkeys: *mut List,
    required_outer: *mut Bitmapset,
) -> *mut Path {
    ptr::null_mut()
}

/// TODO(pg-port): optimizer/util/pathnode.c create_setop_path
unsafe fn create_setop_path(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    leftpath: *mut Path,
    rightpath: *mut Path,
    cmd: SetOpCmd,
    strategy: crate::nodes::nodes::SetOpStrategy,
    groupList: *mut List,
    dNumGroups: f64,
    dNumOutputRows: f64,
) -> *mut Path {
    ptr::null_mut()
}

/// TODO(pg-port): optimizer/util/pathnode.c create_recursiveunion_path
unsafe fn create_recursiveunion_path(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    leftpath: *mut Path,
    rightpath: *mut Path,
    target: *mut PathTarget,
    distinctList: *mut List,
    wtParam: c_int,
    dNumGroups: f64,
) -> *mut Path {
    ptr::null_mut()
}

/// TODO(pg-port): optimizer/util/pathnode.c set_cheapest
unsafe fn set_cheapest(parent_rel: *mut RelOptInfo) {}

/// TODO(pg-port): optimizer/path/pathkeys.c pathkeys_contained_in
unsafe fn pathkeys_contained_in(keys1: *mut List, keys2: *mut List) -> bool {
    false
}

/// TODO(pg-port): optimizer/path/pathkeys.c pathkeys_count_contained_in
unsafe fn pathkeys_count_contained_in(
    keys1: *mut List,
    keys2: *mut List,
    n_common: *mut c_int,
) -> bool {
    *n_common = 0;
    false
}

/// TODO(pg-port): optimizer/path/pathkeys.c get_cheapest_path_for_pathkeys
unsafe fn get_cheapest_path_for_pathkeys(
    paths: *mut List,
    pathkeys: *mut List,
    required_outer: *mut Bitmapset,
    cost_criterion: c_int,
    require_parallel_safe: bool,
) -> *mut Path {
    ptr::null_mut()
}

/// TODO(pg-port): optimizer/path/pathkeys.c convert_subquery_pathkeys
unsafe fn convert_subquery_pathkeys(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    subquery_pathkeys: *mut List,
    subquery_tlist: *mut List,
) -> *mut List {
    NIL
}

/// TODO(pg-port): optimizer/path/pathkeys.c make_pathkeys_for_sortclauses
unsafe fn make_pathkeys_for_sortclauses(
    root: *mut PlannerInfo,
    sortclauses: *mut List,
    tlist: *mut List,
) -> *mut List {
    NIL
}

/// TODO(pg-port): optimizer/path/allpaths.c add_setop_child_rel_equivalences
unsafe fn add_setop_child_rel_equivalences(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    child_tlist: *mut List,
    interesting_pathkeys: *mut List,
) {
}

/// TODO(pg-port): optimizer/path/allpaths.c set_subquery_size_estimates
unsafe fn set_subquery_size_estimates(root: *mut PlannerInfo, rel: *mut RelOptInfo) {}

/// TODO(pg-port): selfuncs.c estimate_num_groups
unsafe fn estimate_num_groups(
    root: *mut PlannerInfo,
    groupExprs: *mut List,
    input_rows: f64,
    pgset: *mut *mut List,
    hentry: *mut c_void,
) -> f64 {
    1.0
}

/// TODO(pg-port): optimizer/plan/planner.c grouping_is_sortable
unsafe fn grouping_is_sortable(clause: *mut List) -> bool {
    true
}

/// TODO(pg-port): optimizer/plan/planner.c grouping_is_hashable
unsafe fn grouping_is_hashable(clause: *mut List) -> bool {
    true
}

/// TODO(pg-port): parser/parse_coerce.c coerce_to_common_type
unsafe fn coerce_to_common_type(
    pstate: *mut c_void,
    node: *mut Node,
    targetTypeId: Oid,
    context: *const c_char,
) -> *mut Node {
    node
}

/// TODO(pg-port): nodes/nodeFuncs.c applyRelabelType
unsafe fn applyRelabelType(
    arg: *mut Node,
    rtype: Oid,
    rtypmod: int32,
    rcollid: Oid,
    rformat: c_int,
    rlocation: c_int,
    overwrite_ok: bool,
) -> *mut Node {
    arg
}

/// GUC: enable_parallel_append
static mut enable_parallel_append: bool = true;

/// GUC: max_parallel_workers_per_gather
static mut max_parallel_workers_per_gather: c_int = 2;

/// GUC: enable_incremental_sort
static mut enable_incremental_sort: bool = true;

/// TOTAL_COST cost criterion flag used in get_cheapest_path_for_pathkeys
const TOTAL_COST: c_int = 0;

/// create_upper_paths_hook type
pub type create_upper_paths_hook_type = Option<
    unsafe fn(
        root: *mut PlannerInfo,
        stage: crate::nodes::pathnodes::UpperRelationKind,
        input_rel: *mut RelOptInfo,
        output_rel: *mut RelOptInfo,
        extra: *mut c_void,
    ),
>;

/// GUC-style hook for extensions
pub static mut create_upper_paths_hook: create_upper_paths_hook_type = None;

// ---------------------------------------------------------------------------
// Port of pg_leftmost_one_pos32 (port/pg_bitutils.h)
// ---------------------------------------------------------------------------
#[inline]
fn pg_leftmost_one_pos32(word: u32) -> c_int {
    crate::port::pg_bitutils::pg_leftmost_one_pos32(word)
}

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

/*
 * plan_set_operations
 *
 *    Plans the queries for a tree of set operations (UNION/INTERSECT/EXCEPT)
 *
 * This routine only deals with the setOperations tree of the given query.
 * Any top-level ORDER BY requested in root->parse->sortClause will be handled
 * when we return to grouping_planner; likewise for LIMIT.
 *
 * What we return is an "upperrel" RelOptInfo containing at least one Path
 * that implements the set-operation tree.  In addition, root->processed_tlist
 * receives a targetlist representing the output of the topmost setop node.
 */
pub unsafe fn plan_set_operations(root: *mut PlannerInfo) -> *mut RelOptInfo {
    let parse: *mut Query = (*root).parse;
    let topop: *mut SetOperationStmt =
        (*parse).setOperations as *mut SetOperationStmt;
    let mut node: *mut Node;
    let leftmostRTE: *mut RangeTblEntry;
    let leftmostQuery: *mut Query;
    let setop_rel: *mut RelOptInfo;
    let mut top_tlist: *mut List = core::ptr::null_mut();

    Assert!(!topop.is_null());

    /* check for unsupported stuff */
    Assert!((*(*parse).jointree).fromlist.is_null());
    Assert!((*(*parse).jointree).quals.is_null());
    Assert!((*parse).groupClause.is_null());
    Assert!((*parse).havingQual.is_null());
    Assert!((*parse).windowClause.is_null());
    Assert!((*parse).distinctClause.is_null());

    /*
     * In the outer query level, equivalence classes are limited to classes
     * which define that the top-level target entry is equivalent to the
     * corresponding child target entry.  There won't be any equivalence class
     * merging.  Mark that merging is complete to allow us to make pathkeys.
     */
    Assert!((*root).eq_classes.is_null());
    (*root).ec_merging_done = true;

    /*
     * We'll need to build RelOptInfos for each of the leaf subqueries, which
     * are RTE_SUBQUERY rangetable entries in this Query.  Prepare the index
     * arrays for those, and for AppendRelInfos in case they're needed.
     */
    setup_simple_rel_arrays(root);

    /*
     * Find the leftmost component Query.  We need to use its column names for
     * all generated tlists (else SELECT INTO won't work right).
     */
    node = (*topop).larg;
    while !node.is_null() && IsA!(node, T_SetOperationStmt) {
        node = (*(node as *mut SetOperationStmt)).larg;
    }
    Assert!(!node.is_null() && IsA!(node, T_RangeTblRef));
    leftmostRTE = *(*root).simple_rte_array.add((*(node as *mut RangeTblRef)).rtindex as usize);
    leftmostQuery = (*leftmostRTE).subquery;
    Assert!(!leftmostQuery.is_null());

    /*
     * If the topmost node is a recursive union, it needs special processing.
     */
    if (*root).hasRecursion {
        setop_rel = generate_recursion_path(
            topop,
            root,
            (*leftmostQuery).targetList,
            &mut top_tlist,
        );
    } else {
        let mut trivial_tlist: bool = false;

        /*
         * Recurse on setOperations tree to generate paths for set ops. The
         * final output paths should have just the column types shown as the
         * output from the top-level node.
         */
        setop_rel = recurse_set_operations(
            topop as *mut Node,
            root,
            ptr::null_mut(), /* no parent */
            (*topop).colTypes,
            (*topop).colCollations,
            (*leftmostQuery).targetList,
            &mut top_tlist,
            &mut trivial_tlist,
        );
    }

    /* Must return the built tlist into root->processed_tlist. */
    (*root).processed_tlist = top_tlist;

    setop_rel
}

// ---------------------------------------------------------------------------
// Static (module-private) functions
// ---------------------------------------------------------------------------

/*
 * recurse_set_operations
 *    Recursively handle one step in a tree of set operations
 *
 * setOp: current step (could be a SetOperationStmt or a leaf RangeTblRef)
 * parentOp: parent step, or NULL if none (but see below)
 * colTypes: OID list of set-op's result column datatypes
 * colCollations: OID list of set-op's result column collations
 * refnames_tlist: targetlist to take column names from
 *
 * parentOp should be passed as NULL unless that step is interested in
 * getting sorted output from this step.  ("Sorted" means "sorted according
 * to the default btree opclasses of the result column datatypes".)
 *
 * Returns a RelOptInfo for the subtree, as well as these output parameters:
 * *pTargetList: receives the fully-fledged tlist for the subtree's top plan
 * *istrivial_tlist: true if, and only if, datatypes between parent and child
 * match.
 */
unsafe fn recurse_set_operations(
    setOp: *mut Node,
    root: *mut PlannerInfo,
    parentOp: *mut SetOperationStmt,
    colTypes: *mut List,
    colCollations: *mut List,
    refnames_tlist: *mut List,
    pTargetList: *mut *mut List,
    istrivial_tlist: *mut bool,
) -> *mut RelOptInfo {
    let rel: *mut RelOptInfo;

    *istrivial_tlist = true; /* for now */

    /* Guard against stack overflow due to overly complex setop nests */
    check_stack_depth();

    if IsA!(setOp, T_RangeTblRef) {
        let rtr: *mut RangeTblRef = setOp as *mut RangeTblRef;
        let rte: *mut RangeTblEntry =
            *(*root).simple_rte_array.add((*rtr).rtindex as usize);
        let subquery: *mut Query = (*rte).subquery;
        let subroot: *mut PlannerInfo;
        let tlist: *mut List;
        let mut trivial_tlist: bool = false;

        Assert!(!subquery.is_null());

        /* Build a RelOptInfo for this leaf subquery. */
        rel = build_simple_rel(root, (*rtr).rtindex, ptr::null_mut());

        /* plan_params should not be in use in current query level */
        Assert!((*root).plan_params.is_null());

        /*
         * Generate a subroot and Paths for the subquery.  If we have a
         * parentOp, pass that down to encourage subquery_planner to consider
         * suitably-sorted Paths.
         */
        subroot = {
            let sr = subquery_planner(
                (*root).glob,
                subquery,
                root,
                false,
                (*root).tuple_fraction,
                parentOp,
            );
            (*rel).subroot = sr;
            sr
        };

        /*
         * It should not be possible for the primitive query to contain any
         * cross-references to other primitive queries in the setop tree.
         */
        if !(*root).plan_params.is_null() {
            elog!(ERROR, "unexpected outer reference in set operation subquery");
        }

        /* Figure out the appropriate target list for this subquery. */
        tlist = generate_setop_tlist(
            colTypes,
            colCollations,
            (*rtr).rtindex as Index,
            true,
            (*subroot).processed_tlist,
            refnames_tlist,
            &mut trivial_tlist,
        );
        (*rel).reltarget = create_pathtarget(root, tlist);

        /* Return the fully-fledged tlist to caller, too */
        *pTargetList = tlist;
        *istrivial_tlist = trivial_tlist;

        rel
    } else if IsA!(setOp, T_SetOperationStmt) {
        let op: *mut SetOperationStmt = setOp as *mut SetOperationStmt;

        /* UNIONs are much different from INTERSECT/EXCEPT */
        let rel = if (*op).op == SETOP_UNION {
            generate_union_paths(op, root, refnames_tlist, pTargetList)
        } else {
            generate_nonunion_paths(op, root, refnames_tlist, pTargetList)
        };

        /*
         * If necessary, add a Result node to project the caller-requested
         * output columns.
         *
         * XXX you don't really want to know about this: setrefs.c will apply
         * fix_upper_expr() to the Result node's tlist. This would fail if the
         * Vars generated by generate_setop_tlist() were not exactly equal()
         * to the corresponding tlist entries of the subplan. However, since
         * the subplan was generated by generate_union_paths() or
         * generate_nonunion_paths(), and hence its tlist was generated by
         * generate_append_tlist() or generate_setop_tlist(), this will work.
         * We just tell generate_setop_tlist() to use varno 0.
         */
        if !tlist_same_datatypes(*pTargetList, colTypes, false)
            || !tlist_same_collations(*pTargetList, colCollations, false)
        {
            let target: *mut PathTarget;
            let mut trivial_tlist: bool = false;
            let lc: *mut ListCell;

            *pTargetList = generate_setop_tlist(
                colTypes,
                colCollations,
                0,
                false,
                *pTargetList,
                refnames_tlist,
                &mut trivial_tlist,
            );
            *istrivial_tlist = trivial_tlist;
            target = create_pathtarget(root, *pTargetList);

            /* Apply projection to each path */
            foreach!(lc, (*rel).pathlist, {
                let subpath: *mut Path = lfirst(current_cell!(lc)) as *mut Path;
                let path: *mut Path;

                Assert!((*subpath).param_info.is_null());
                path = apply_projection_to_path(root, (*subpath).parent, subpath, target);
                /* If we had to add a Result, path is different from subpath */
                if path != subpath {
                    *(current_cell!(lc) as *mut *mut c_void) = path as *mut c_void;
                }
            });

            /* Apply projection to each partial path */
            foreach!(lc, (*rel).partial_pathlist, {
                let subpath: *mut Path = lfirst(current_cell!(lc)) as *mut Path;
                let path: *mut Path;

                Assert!((*subpath).param_info.is_null());

                /* avoid apply_projection_to_path, in case of multiple refs */
                path = create_projection_path(root, (*subpath).parent, subpath, target);
                *(current_cell!(lc) as *mut *mut c_void) = path as *mut c_void;
            });
        }
        postprocess_setop_rel(root, rel);
        rel
    } else {
        elog!(
            ERROR,
            "unrecognized node type: {}",
            nodeTag(setOp) as c_int
        );
        *pTargetList = NIL;
        ptr::null_mut() /* keep compiler quiet */
    }
}

/*
 * Generate paths for a recursive UNION node
 */
unsafe fn generate_recursion_path(
    setOp: *mut SetOperationStmt,
    root: *mut PlannerInfo,
    refnames_tlist: *mut List,
    pTargetList: *mut *mut List,
) -> *mut RelOptInfo {
    let result_rel: *mut RelOptInfo;
    let path: *mut Path;
    let lrel: *mut RelOptInfo;
    let rrel: *mut RelOptInfo;
    let lpath: *mut Path;
    let rpath: *mut Path;
    let mut lpath_tlist: *mut List = NIL;
    let mut lpath_trivial_tlist: bool = false;
    let mut rpath_tlist: *mut List = NIL;
    let mut rpath_trivial_tlist: bool = false;
    let tlist: *mut List;
    let groupList: *mut List;
    let dNumGroups: f64;

    /* Parser should have rejected other cases */
    if (*setOp).op != SETOP_UNION {
        elog!(ERROR, "only UNION queries can be recursive");
    }
    /* Worktable ID should be assigned */
    Assert!((*root).wt_param_id >= 0);

    /*
     * Unlike a regular UNION node, process the left and right inputs
     * separately without any intention of combining them into one Append.
     */
    lrel = recurse_set_operations(
        (*setOp).larg,
        root,
        ptr::null_mut(), /* no value in sorted results */
        (*setOp).colTypes,
        (*setOp).colCollations,
        refnames_tlist,
        &mut lpath_tlist,
        &mut lpath_trivial_tlist,
    );
    if (*lrel).rtekind == RTE_SUBQUERY {
        build_setop_child_paths(
            root,
            lrel,
            lpath_trivial_tlist,
            lpath_tlist,
            NIL,
            ptr::null_mut(),
        );
    }
    lpath = (*lrel).cheapest_total_path;
    /* The right path will want to look at the left one ... */
    (*root).non_recursive_path = lpath;
    rrel = recurse_set_operations(
        (*setOp).rarg,
        root,
        ptr::null_mut(), /* no value in sorted results */
        (*setOp).colTypes,
        (*setOp).colCollations,
        refnames_tlist,
        &mut rpath_tlist,
        &mut rpath_trivial_tlist,
    );
    if (*rrel).rtekind == RTE_SUBQUERY {
        build_setop_child_paths(
            root,
            rrel,
            rpath_trivial_tlist,
            rpath_tlist,
            NIL,
            ptr::null_mut(),
        );
    }
    rpath = (*rrel).cheapest_total_path;
    (*root).non_recursive_path = ptr::null_mut();

    /*
     * Generate tlist for RecursiveUnion path node --- same as in Append cases
     */
    tlist = generate_append_tlist(
        (*setOp).colTypes,
        (*setOp).colCollations,
        list_make2!(lpath_tlist as *mut c_void, rpath_tlist as *mut c_void),
        refnames_tlist,
    );

    *pTargetList = tlist;

    /* Build result relation. */
    result_rel = fetch_upper_rel(
        root,
        UPPERREL_SETOP,
        bms_union((*lrel).relids, (*rrel).relids),
    );
    (*result_rel).reltarget = create_pathtarget(root, tlist);

    /*
     * If UNION, identify the grouping operators
     */
    if (*setOp).all {
        groupList = NIL;
        dNumGroups = 0.0;
    } else {
        /* Identify the grouping semantics */
        groupList = generate_setop_grouplist(setOp, tlist);

        /* We only support hashing here */
        if !grouping_is_hashable(groupList) {
            ereport!(
                ERROR,
                errmsg!("could not implement recursive UNION")
                /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                           errdetail("All column datatypes must be hashable.") */
            );
        }

        /*
         * For the moment, take the number of distinct groups as equal to the
         * total input size, ie, the worst case.
         */
        dNumGroups = (*lpath).rows + (*rpath).rows * 10.0;
    }

    /*
     * And make the path node.
     */
    let path = create_recursiveunion_path(
        root,
        result_rel,
        lpath,
        rpath,
        (*result_rel).reltarget,
        groupList,
        (*root).wt_param_id,
        dNumGroups,
    );

    add_path(result_rel, path);
    postprocess_setop_rel(root, result_rel);
    result_rel
}

/*
 * build_setop_child_paths
 *        Build paths for the set op child relation denoted by 'rel'.
 *
 * 'rel' is an RTE_SUBQUERY relation.  We have already generated paths within
 * the subquery's subroot; the task here is to create SubqueryScan paths for
 * 'rel', representing scans of the useful subquery paths.
 *
 * interesting_pathkeys: if not NIL, also include paths that suit these
 * pathkeys, sorting any unsorted paths as required.
 * *pNumGroups: if not NULL, we estimate the number of distinct groups
 * in the result, and store it there.
 */
unsafe fn build_setop_child_paths(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    trivial_tlist: bool,
    child_tlist: *mut List,
    interesting_pathkeys: *mut List,
    pNumGroups: *mut f64,
) {
    let final_rel: *mut RelOptInfo;
    let setop_pathkeys: *mut List = (*(*rel).subroot).setop_pathkeys;
    let lc: *mut ListCell;

    /* it can't be a set op child rel if it's not a subquery */
    Assert!((*rel).rtekind == RTE_SUBQUERY);

    /* when sorting is needed, add child rel equivalences */
    if !interesting_pathkeys.is_null() {
        add_setop_child_rel_equivalences(root, rel, child_tlist, interesting_pathkeys);
    }

    /*
     * Mark rel with estimated output rows, width, etc.  Note that we have to
     * do this before generating outer-query paths, else cost_subqueryscan is
     * not happy.
     */
    set_subquery_size_estimates(root, rel);

    /*
     * Since we may want to add a partial path to this relation, we must set
     * its consider_parallel flag correctly.
     */
    final_rel = fetch_upper_rel((*rel).subroot, UPPERREL_FINAL, ptr::null_mut());
    (*rel).consider_parallel = (*final_rel).consider_parallel;

    /* Generate subquery scan paths for any interesting path in final_rel */
    foreach!(lc, (*final_rel).pathlist, {
        let mut subpath: *mut Path = lfirst(current_cell!(lc)) as *mut Path;
        let pathkeys: *mut List;
        let cheapest_input_path: *mut Path = (*final_rel).cheapest_total_path;
        let is_sorted: bool;
        let mut presorted_keys: c_int = 0;

        /*
         * Include the cheapest path as-is so that the set operation can be
         * cheaply implemented using a method which does not require the input
         * to be sorted.
         */
        if subpath == cheapest_input_path {
            /* Convert subpath's pathkeys to outer representation */
            let pathkeys = convert_subquery_pathkeys(
                root,
                rel,
                (*subpath).pathkeys,
                make_tlist_from_pathtarget((*subpath).pathtarget),
            );

            /* Generate outer path using this subpath */
            add_path(
                rel,
                create_subqueryscan_path(root, rel, subpath, trivial_tlist, pathkeys, ptr::null_mut()),
            );
        }

        /* skip dealing with sorted paths if the setop doesn't need them */
        if interesting_pathkeys.is_null() {
            continue;
        }

        /*
         * Create paths to suit final sort order required for setop_pathkeys.
         * Here we'll sort the cheapest input path (if not sorted already) and
         * incremental sort any paths which are partially sorted.
         */
        let is_sorted = pathkeys_count_contained_in(
            setop_pathkeys,
            (*subpath).pathkeys,
            &mut presorted_keys,
        );

        if !is_sorted {
            let limittuples: f64 = (*(*rel).subroot).limit_tuples;

            /*
             * Try at least sorting the cheapest path and also try
             * incrementally sorting any path which is partially sorted
             * already (no need to deal with paths which have presorted keys
             * when incremental sort is disabled unless it's the cheapest
             * input path).
             */
            if subpath != cheapest_input_path
                && (presorted_keys == 0 || !enable_incremental_sort)
            {
                continue;
            }

            /*
             * We've no need to consider both a sort and incremental sort.
             * We'll just do a sort if there are no presorted keys and an
             * incremental sort when there are presorted keys.
             */
            if presorted_keys == 0 || !enable_incremental_sort {
                subpath = create_sort_path(
                    (*rel).subroot,
                    final_rel,
                    subpath,
                    setop_pathkeys,
                    limittuples,
                );
            } else {
                subpath = create_incremental_sort_path(
                    (*rel).subroot,
                    final_rel,
                    subpath,
                    setop_pathkeys,
                    presorted_keys,
                    limittuples,
                );
            }
        }

        /*
         * subpath is now sorted, so add it to the pathlist.  We already added
         * the cheapest_input_path above, so don't add it again unless we just
         * sorted it.
         */
        if subpath != cheapest_input_path {
            /* Convert subpath's pathkeys to outer representation */
            let pathkeys = convert_subquery_pathkeys(
                root,
                rel,
                (*subpath).pathkeys,
                make_tlist_from_pathtarget((*subpath).pathtarget),
            );

            /* Generate outer path using this subpath */
            add_path(
                rel,
                create_subqueryscan_path(root, rel, subpath, trivial_tlist, pathkeys, ptr::null_mut()),
            );
        }
    });

    /* if consider_parallel is false, there should be no partial paths */
    Assert!(
        (*final_rel).consider_parallel || (*final_rel).partial_pathlist.is_null()
    );

    /*
     * If we have a partial path for the child relation, we can use that to
     * build a partial path for this relation.  But there's no point in
     * considering any path but the cheapest.
     */
    if (*rel).consider_parallel
        && bms_is_empty((*rel).lateral_relids)
        && !(*final_rel).partial_pathlist.is_null()
    {
        let partial_subpath: *mut Path = linitial((*final_rel).partial_pathlist) as *mut Path;
        let partial_path: *mut Path = create_subqueryscan_path(
            root,
            rel,
            partial_subpath,
            trivial_tlist,
            NIL,
            ptr::null_mut(),
        );
        add_partial_path(rel, partial_path);
    }

    postprocess_setop_rel(root, rel);

    /*
     * Estimate number of groups if caller wants it.  If the subquery used
     * grouping or aggregation, its output is probably mostly unique anyway;
     * otherwise do statistical estimation.
     *
     * XXX you don't really want to know about this: we do the estimation
     * using the subroot->parse's original targetlist expressions, not the
     * subroot->processed_tlist which might seem more appropriate.  The reason
     * is that if the subquery is itself a setop, it may return a
     * processed_tlist containing "varno 0" Vars generated by
     * generate_append_tlist, and those would confuse estimate_num_groups
     * mightily.  We ought to get rid of the "varno 0" hack, but that requires
     * a redesign of the parsetree representation of setops, so that there can
     * be an RTE corresponding to each setop's output. Note, we use this not
     * subquery's targetlist but subroot->parse's targetlist, because it was
     * revised by self-join removal.  subquery's targetlist might contain the
     * references to the removed relids.
     */
    if !pNumGroups.is_null() {
        let subroot: *mut PlannerInfo = (*rel).subroot;
        let subquery: *mut Query = (*subroot).parse;

        if !(*subquery).groupClause.is_null()
            || !(*subquery).groupingSets.is_null()
            || !(*subquery).distinctClause.is_null()
            || (*subroot).hasHavingQual
            || (*subquery).hasAggs
        {
            *pNumGroups = (*(*rel).cheapest_total_path).rows;
        } else {
            *pNumGroups = estimate_num_groups(
                subroot,
                get_tlist_exprs((*(*subroot).parse).targetList, false),
                (*(*rel).cheapest_total_path).rows,
                ptr::null_mut(),
                ptr::null_mut(),
            );
        }
    }
}

/*
 * Generate paths for a UNION or UNION ALL node
 */
unsafe fn generate_union_paths(
    op: *mut SetOperationStmt,
    root: *mut PlannerInfo,
    refnames_tlist: *mut List,
    pTargetList: *mut *mut List,
) -> *mut RelOptInfo {
    let mut relids: *mut Bitmapset = ptr::null_mut();
    let result_rel: *mut RelOptInfo;
    let lc: *mut ListCell;
    let lc2: *mut ListCell;
    let lc3: *mut ListCell;
    let mut cheapest_pathlist: *mut List = NIL;
    let mut ordered_pathlist: *mut List = NIL;
    let mut partial_pathlist: *mut List = NIL;
    let mut partial_paths_valid: bool = true;
    let mut consider_parallel: bool = true;
    let rellist: *mut List;
    let mut tlist_list: *mut List = NIL;
    let mut trivial_tlist_list: *mut List = NIL;
    let tlist: *mut List;
    let mut groupList: *mut List = NIL;
    let apath: *mut Path;
    let mut gpath: *mut Path = ptr::null_mut();
    let mut try_sorted: bool = false;
    let mut union_pathkeys: *mut List = NIL;

    /*
     * If any of my children are identical UNION nodes (same op, all-flag, and
     * colTypes/colCollations) then they can be merged into this node so that
     * we generate only one Append/MergeAppend and unique-ification for the
     * lot.  Recurse to find such nodes.
     */
    rellist = plan_union_children(
        root,
        op,
        refnames_tlist,
        &mut tlist_list,
        &mut trivial_tlist_list,
    );

    /*
     * Generate tlist for Append/MergeAppend plan node.
     *
     * The tlist for an Append plan isn't important as far as the Append is
     * concerned, but we must make it look real anyway for the benefit of the
     * next plan level up.
     */
    tlist = generate_append_tlist(
        (*op).colTypes,
        (*op).colCollations,
        tlist_list,
        refnames_tlist,
    );
    *pTargetList = tlist;

    /* For UNIONs (not UNION ALL), try sorting, if sorting is possible */
    if !(*op).all {
        /* Identify the grouping semantics */
        groupList = generate_setop_grouplist(op, tlist);

        if grouping_is_sortable((*op).groupClauses) {
            try_sorted = true;
            /* Determine the pathkeys for sorting by the whole target list */
            union_pathkeys = make_pathkeys_for_sortclauses(root, groupList, tlist);

            (*root).query_pathkeys = union_pathkeys;
        }
    }

    /*
     * Now that we've got the append target list, we can build the union child
     * paths.
     */
    forthree!(lc, rellist, lc2, trivial_tlist_list, lc3, tlist_list, {
        let rel: *mut RelOptInfo = lfirst(lc) as *mut RelOptInfo;
        let trivial_tlist: bool = lfirst_int(lc2) != 0;
        let child_tlist: *mut List = lfirst(lc3) as *mut List;

        /* only build paths for the union children */
        if (*rel).rtekind == RTE_SUBQUERY {
            build_setop_child_paths(root, rel, trivial_tlist, child_tlist, union_pathkeys, ptr::null_mut());
        }
    });

    /* Build path lists and relid set. */
    foreach!(lc, rellist, {
        let rel: *mut RelOptInfo = lfirst(current_cell!(lc)) as *mut RelOptInfo;

        cheapest_pathlist = lappend(cheapest_pathlist, (*rel).cheapest_total_path as *mut c_void);

        if try_sorted {
            let ordered_path: *mut Path = get_cheapest_path_for_pathkeys(
                (*rel).pathlist,
                union_pathkeys,
                ptr::null_mut(),
                TOTAL_COST,
                false,
            );

            if !ordered_path.is_null() {
                ordered_pathlist = lappend(ordered_pathlist, ordered_path as *mut c_void);
            } else {
                /*
                 * If we can't find a sorted path, just give up trying to
                 * generate a list of correctly sorted child paths.  This can
                 * happen when type coercion was added to the targetlist due
                 * to mismatching types from the union children.
                 */
                try_sorted = false;
            }
        }

        if consider_parallel {
            if !(*rel).consider_parallel {
                consider_parallel = false;
                partial_paths_valid = false;
            } else if (*rel).partial_pathlist.is_null() {
                partial_paths_valid = false;
            } else {
                partial_pathlist = lappend(
                    partial_pathlist,
                    linitial((*rel).partial_pathlist),
                );
            }
        }

        relids = bms_union(relids, (*rel).relids);
    });

    /* Build result relation. */
    result_rel = fetch_upper_rel(root, UPPERREL_SETOP, relids);
    (*result_rel).reltarget = create_pathtarget(root, tlist);
    (*result_rel).consider_parallel = consider_parallel;
    (*result_rel).consider_startup = (*root).tuple_fraction > 0.0;

    /*
     * Append the child results together using the cheapest paths from each
     * union child.
     */
    apath = create_append_path(
        root,
        result_rel,
        cheapest_pathlist,
        NIL,
        NIL,
        ptr::null_mut(),
        0,
        false,
        -1.0,
    );

    /*
     * Estimate number of groups.  For now we just assume the output is unique
     * --- this is certainly true for the UNION case, and we want worst-case
     * estimates anyway.
     */
    (*result_rel).rows = (*apath).rows;

    /*
     * Now consider doing the same thing using the partial paths plus Append
     * plus Gather.
     */
    if partial_paths_valid {
        let papath: *mut Path;
        let mut parallel_workers: c_int = 0;

        /* Find the highest number of workers requested for any subpath. */
        foreach!(lc, partial_pathlist, {
            let subpath: *mut Path = lfirst(current_cell!(lc)) as *mut Path;

            if (*subpath).parallel_workers > parallel_workers {
                parallel_workers = (*subpath).parallel_workers;
            }
        });
        Assert!(parallel_workers > 0);

        /*
         * If the use of parallel append is permitted, always request at least
         * log2(# of children) paths.  We assume it can be useful to have
         * extra workers in this case because they will be spread out across
         * the children.  The precise formula is just a guess; see
         * add_paths_to_append_rel.
         */
        if enable_parallel_append {
            let n = pg_leftmost_one_pos32(list_length(partial_pathlist) as u32) + 1;
            if n > parallel_workers {
                parallel_workers = n;
            }
            if parallel_workers > max_parallel_workers_per_gather {
                parallel_workers = max_parallel_workers_per_gather;
            }
        }
        Assert!(parallel_workers > 0);

        papath = create_append_path(
            root,
            result_rel,
            NIL,
            partial_pathlist,
            NIL,
            ptr::null_mut(),
            parallel_workers,
            enable_parallel_append,
            -1.0,
        );
        gpath = create_gather_path(
            root,
            result_rel,
            papath,
            (*result_rel).reltarget,
            ptr::null_mut(),
            ptr::null_mut(),
        );
    }

    if !(*op).all {
        let dNumGroups: f64;
        let can_sort: bool = grouping_is_sortable(groupList);
        let can_hash: bool = grouping_is_hashable(groupList);

        /*
         * XXX for the moment, take the number of distinct groups as equal to
         * the total input size, i.e., the worst case.  This is too
         * conservative, but it's not clear how to get a decent estimate of
         * the true size.  One should note as well the propensity of novices
         * to write UNION rather than UNION ALL even when they don't expect
         * any duplicates...
         */
        dNumGroups = (*apath).rows;

        if can_hash {
            let path: *mut Path;

            /*
             * Try a hash aggregate plan on 'apath'.  This is the cheapest
             * available path containing each append child.
             */
            path = create_agg_path(
                root,
                result_rel,
                apath,
                create_pathtarget(root, tlist),
                AGG_HASHED,
                AGGSPLIT_SIMPLE,
                groupList,
                NIL,
                ptr::null_mut(),
                dNumGroups,
            );
            add_path(result_rel, path);

            /* Try hash aggregate on the Gather path, if valid */
            if !gpath.is_null() {
                /* Hashed aggregate plan --- no sort needed */
                let path = create_agg_path(
                    root,
                    result_rel,
                    gpath,
                    create_pathtarget(root, tlist),
                    AGG_HASHED,
                    AGGSPLIT_SIMPLE,
                    groupList,
                    NIL,
                    ptr::null_mut(),
                    dNumGroups,
                );
                add_path(result_rel, path);
            }
        }

        if can_sort {
            let mut path: *mut Path = apath;

            /* Try Sort -> Unique on the Append path */
            if !groupList.is_null() {
                path = create_sort_path(
                    root,
                    result_rel,
                    path,
                    make_pathkeys_for_sortclauses(root, groupList, tlist),
                    -1.0,
                );
            }

            path = create_upper_unique_path(
                root,
                result_rel,
                path,
                list_length((*path).pathkeys),
                dNumGroups,
            );

            add_path(result_rel, path);

            /* Try Sort -> Unique on the Gather path, if set */
            if !gpath.is_null() {
                let mut path: *mut Path = gpath;

                path = create_sort_path(
                    root,
                    result_rel,
                    path,
                    make_pathkeys_for_sortclauses(root, groupList, tlist),
                    -1.0,
                );

                path = create_upper_unique_path(
                    root,
                    result_rel,
                    path,
                    list_length((*path).pathkeys),
                    dNumGroups,
                );
                add_path(result_rel, path);
            }
        }

        /*
         * Try making a MergeAppend path if we managed to find a path with the
         * correct pathkeys in each union child query.
         */
        if try_sorted && !groupList.is_null() {
            let mut path: *mut Path;

            path = create_merge_append_path(
                root,
                result_rel,
                ordered_pathlist,
                union_pathkeys,
                ptr::null_mut(),
            );

            /* and make the MergeAppend unique */
            path = create_upper_unique_path(
                root,
                result_rel,
                path,
                list_length(tlist),
                dNumGroups,
            );

            add_path(result_rel, path);
        }
    } else {
        /* UNION ALL */
        add_path(result_rel, apath);

        if !gpath.is_null() {
            add_path(result_rel, gpath);
        }
    }

    result_rel
}

/*
 * Generate paths for an INTERSECT, INTERSECT ALL, EXCEPT, or EXCEPT ALL node
 */
unsafe fn generate_nonunion_paths(
    op: *mut SetOperationStmt,
    root: *mut PlannerInfo,
    refnames_tlist: *mut List,
    pTargetList: *mut *mut List,
) -> *mut RelOptInfo {
    let result_rel: *mut RelOptInfo;
    let mut lrel: *mut RelOptInfo;
    let mut rrel: *mut RelOptInfo;
    let save_fraction: f64 = (*root).tuple_fraction;
    let lpath: *mut Path;
    let rpath: *mut Path;
    let path: *mut Path;
    let mut lpath_tlist: *mut List = NIL;
    let mut rpath_tlist: *mut List = NIL;
    let tlist: *mut List;
    let groupList: *mut List;
    let mut lpath_trivial_tlist: bool = false;
    let mut rpath_trivial_tlist: bool = false;
    let mut result_trivial_tlist: bool = false;
    let mut nonunion_pathkeys: *mut List = NIL;
    let mut dLeftGroups: f64 = 0.0;
    let mut dRightGroups: f64 = 0.0;
    let dNumGroups: f64;
    let dNumOutputRows: f64;
    let can_sort: bool;
    let can_hash: bool;
    let cmd: crate::nodes::nodes::SetOpCmd;

    /*
     * Tell children to fetch all tuples.
     */
    (*root).tuple_fraction = 0.0;

    /* Recurse on children */
    lrel = recurse_set_operations(
        (*op).larg,
        root,
        op,
        (*op).colTypes,
        (*op).colCollations,
        refnames_tlist,
        &mut lpath_tlist,
        &mut lpath_trivial_tlist,
    );

    rrel = recurse_set_operations(
        (*op).rarg,
        root,
        op,
        (*op).colTypes,
        (*op).colCollations,
        refnames_tlist,
        &mut rpath_tlist,
        &mut rpath_trivial_tlist,
    );

    /*
     * Generate tlist for SetOp plan node.
     *
     * The tlist for a SetOp plan isn't important so far as the SetOp is
     * concerned, but we must make it look real anyway for the benefit of the
     * next plan level up.
     */
    tlist = generate_setop_tlist(
        (*op).colTypes,
        (*op).colCollations,
        0,
        false,
        lpath_tlist,
        refnames_tlist,
        &mut result_trivial_tlist,
    );

    /* We should not have needed any type coercions in the tlist */
    Assert!(result_trivial_tlist);

    *pTargetList = tlist;

    /* Identify the grouping semantics */
    groupList = generate_setop_grouplist(op, tlist);

    /* Check whether the operators support sorting or hashing */
    can_sort = grouping_is_sortable(groupList);
    can_hash = grouping_is_hashable(groupList);
    if !can_sort && !can_hash {
        ereport!(
            ERROR,
            errmsg!(
                "could not implement {}",
                if (*op).op == SETOP_INTERSECT { "INTERSECT" } else { "EXCEPT" }
            )
            /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                       errdetail("Some of the datatypes only support hashing, while others only support sorting.") */
        );
    }

    if can_sort {
        /* Determine the pathkeys for sorting by the whole target list */
        nonunion_pathkeys = make_pathkeys_for_sortclauses(root, groupList, tlist);

        (*root).query_pathkeys = nonunion_pathkeys;
    }

    /*
     * Now that we've got all that info, we can build the child paths.
     */
    if (*lrel).rtekind == RTE_SUBQUERY {
        build_setop_child_paths(
            root,
            lrel,
            lpath_trivial_tlist,
            lpath_tlist,
            nonunion_pathkeys,
            &mut dLeftGroups,
        );
    } else {
        dLeftGroups = (*lrel).rows;
    }
    if (*rrel).rtekind == RTE_SUBQUERY {
        build_setop_child_paths(
            root,
            rrel,
            rpath_trivial_tlist,
            rpath_tlist,
            nonunion_pathkeys,
            &mut dRightGroups,
        );
    } else {
        dRightGroups = (*rrel).rows;
    }

    /* Undo effects of forcing tuple_fraction to 0 */
    (*root).tuple_fraction = save_fraction;

    /*
     * For EXCEPT, we must put the left input first.  For INTERSECT, either
     * order should give the same results, and we prefer to put the smaller
     * input first in order to (a) minimize the size of the hash table in the
     * hashing case, and (b) improve our chances of exploiting the executor's
     * fast path for empty left-hand input.  "Smaller" means the one with the
     * fewer groups.
     */
    if (*op).op != SETOP_EXCEPT && dLeftGroups > dRightGroups {
        /* need to swap the two inputs */
        let tmprel: *mut RelOptInfo = lrel;
        lrel = rrel;
        rrel = tmprel;
        let tmplist: *mut List = lpath_tlist;
        lpath_tlist = rpath_tlist;
        rpath_tlist = tmplist;
        let tmpd: f64 = dLeftGroups;
        dLeftGroups = dRightGroups;
        dRightGroups = tmpd;
    }

    lpath = (*lrel).cheapest_total_path;
    rpath = (*rrel).cheapest_total_path;

    /* Build result relation. */
    result_rel = fetch_upper_rel(
        root,
        UPPERREL_SETOP,
        bms_union((*lrel).relids, (*rrel).relids),
    );
    (*result_rel).reltarget = create_pathtarget(root, tlist);

    /*
     * Estimate number of distinct groups that we'll need hashtable entries
     * for; this is the size of the left-hand input for EXCEPT, or the smaller
     * input for INTERSECT.  Also estimate the number of eventual output rows.
     * In non-ALL cases, we estimate each group produces one output row; in
     * ALL cases use the relevant relation size.  These are worst-case
     * estimates, of course, but we need to be conservative.
     */
    let (dNumGroups, dNumOutputRows) = if (*op).op == SETOP_EXCEPT {
        let g = dLeftGroups;
        let r = if (*op).all { (*lpath).rows } else { g };
        (g, r)
    } else {
        let g = dLeftGroups;
        let r = if (*op).all {
            if (*lpath).rows < (*rpath).rows { (*lpath).rows } else { (*rpath).rows }
        } else {
            g
        };
        (g, r)
    };
    (*result_rel).rows = dNumOutputRows;

    /* Select the SetOpCmd type */
    let cmd: crate::nodes::nodes::SetOpCmd = match (*op).op {
        SETOP_INTERSECT => {
            if (*op).all { SETOPCMD_INTERSECT_ALL } else { SETOPCMD_INTERSECT }
        }
        SETOP_EXCEPT => {
            if (*op).all { SETOPCMD_EXCEPT_ALL } else { SETOPCMD_EXCEPT }
        }
        _ => {
            elog!(ERROR, "unrecognized set op: {}", (*op).op as c_int);
            SETOPCMD_INTERSECT /* keep compiler quiet */
        }
    };

    /*
     * If we can hash, that just requires a SetOp atop the cheapest inputs.
     */
    if can_hash {
        let path = create_setop_path(
            root,
            result_rel,
            lpath,
            rpath,
            cmd,
            SETOP_HASHED,
            groupList,
            dNumGroups,
            dNumOutputRows,
        );
        add_path(result_rel, path);
    }

    /*
     * If we can sort, generate the cheapest sorted input paths, and add a
     * SetOp atop those.
     */
    if can_sort {
        let mut slpath: *mut Path;
        let mut srpath: *mut Path;

        /* First the left input ... */
        let pathkeys_l = make_pathkeys_for_sortclauses(root, groupList, lpath_tlist);
        if pathkeys_contained_in(pathkeys_l, (*lpath).pathkeys) {
            slpath = lpath; /* cheapest path is already sorted */
        } else {
            slpath = get_cheapest_path_for_pathkeys(
                (*lrel).pathlist,
                nonunion_pathkeys,
                ptr::null_mut(),
                TOTAL_COST,
                false,
            );
            /* Subquery failed to produce any presorted paths? */
            if slpath.is_null() {
                slpath = create_sort_path(root, (*lpath).parent, lpath, pathkeys_l, -1.0);
            }
        }

        /* and now the same for the right. */
        let pathkeys_r = make_pathkeys_for_sortclauses(root, groupList, rpath_tlist);
        if pathkeys_contained_in(pathkeys_r, (*rpath).pathkeys) {
            srpath = rpath; /* cheapest path is already sorted */
        } else {
            srpath = get_cheapest_path_for_pathkeys(
                (*rrel).pathlist,
                nonunion_pathkeys,
                ptr::null_mut(),
                TOTAL_COST,
                false,
            );
            /* Subquery failed to produce any presorted paths? */
            if srpath.is_null() {
                srpath = create_sort_path(root, (*rpath).parent, rpath, pathkeys_r, -1.0);
            }
        }

        let path = create_setop_path(
            root,
            result_rel,
            slpath,
            srpath,
            cmd,
            SETOP_SORTED,
            groupList,
            dNumGroups,
            dNumOutputRows,
        );
        add_path(result_rel, path);
    }

    result_rel
}

/*
 * Pull up children of a UNION node that are identically-propertied UNIONs,
 * and perform planning of the queries underneath the N-way UNION.
 *
 * The result is a list of RelOptInfos containing Paths for sub-nodes, with
 * one entry for each descendant that is a leaf query or non-identical setop.
 * We also return parallel lists of the childrens' targetlists and
 * is-trivial-tlist flags.
 *
 * NOTE: we can also pull a UNION ALL up into a UNION, since the distinct
 * output rows will be lost anyway.
 */
unsafe fn plan_union_children(
    root: *mut PlannerInfo,
    top_union: *mut SetOperationStmt,
    refnames_tlist: *mut List,
    tlist_list: *mut *mut List,
    istrivial_tlist: *mut *mut List,
) -> *mut List {
    let mut pending_rels: *mut List = list_make1!(top_union as *mut c_void);
    let mut result: *mut List = NIL;
    let mut child_tlist: *mut List = NIL;
    let mut trivial_tlist: bool = false;

    *tlist_list = NIL;
    *istrivial_tlist = NIL;

    while !pending_rels.is_null() {
        let setOp: *mut Node = linitial(pending_rels) as *mut Node;

        pending_rels = list_delete_first(pending_rels);

        if IsA!(setOp, T_SetOperationStmt) {
            let op: *mut SetOperationStmt = setOp as *mut SetOperationStmt;

            if (*op).op == (*top_union).op
                && ((*op).all == (*top_union).all || (*op).all)
                && equal((*op).colTypes as *const c_void, (*top_union).colTypes as *const c_void)
                && equal(
                    (*op).colCollations as *const c_void,
                    (*top_union).colCollations as *const c_void,
                )
            {
                /* Same UNION, so fold children into parent */
                pending_rels = lcons((*op).rarg as *mut c_void, pending_rels);
                pending_rels = lcons((*op).larg as *mut c_void, pending_rels);
                continue;
            }
        }

        /*
         * Not same, so plan this child separately.
         *
         * If top_union isn't a UNION ALL, then we are interested in sorted
         * output from the child, so pass top_union as parentOp.  Note that
         * this isn't necessarily the child node's immediate SetOperationStmt
         * parent, but that's fine: it's the effective parent.
         */
        result = lappend(
            result,
            recurse_set_operations(
                setOp,
                root,
                if (*top_union).all { ptr::null_mut() } else { top_union },
                (*top_union).colTypes,
                (*top_union).colCollations,
                refnames_tlist,
                &mut child_tlist,
                &mut trivial_tlist,
            ) as *mut c_void,
        );
        *tlist_list = lappend(*tlist_list, child_tlist as *mut c_void);
        *istrivial_tlist = lappend_int(*istrivial_tlist, trivial_tlist as c_int);
    }

    result
}

/*
 * postprocess_setop_rel - perform steps required after adding paths
 */
unsafe fn postprocess_setop_rel(root: *mut PlannerInfo, rel: *mut RelOptInfo) {
    /*
     * We don't currently worry about allowing FDWs to contribute paths to
     * this relation, but give extensions a chance.
     */
    if let Some(hook) = create_upper_paths_hook {
        hook(root, UPPERREL_SETOP, ptr::null_mut(), rel, ptr::null_mut());
    }

    /* Select cheapest path */
    set_cheapest(rel);
}

/*
 * Generate targetlist for a set-operation plan node
 *
 * colTypes: OID list of set-op's result column datatypes
 * colCollations: OID list of set-op's result column collations
 * varno: varno to use in generated Vars
 * hack_constants: true to copy up constants (see comments in code)
 * input_tlist: targetlist of this node's input node
 * refnames_tlist: targetlist to take column names from
 * trivial_tlist: output parameter, set to true if targetlist is trivial
 */
unsafe fn generate_setop_tlist(
    colTypes: *mut List,
    colCollations: *mut List,
    varno: Index,
    hack_constants: bool,
    input_tlist: *mut List,
    refnames_tlist: *mut List,
    trivial_tlist: *mut bool,
) -> *mut List {
    let mut tlist: *mut List = NIL;
    let mut resno: c_int = 1;
    let ctlc: *mut ListCell;
    let cclc: *mut ListCell;
    let itlc: *mut ListCell;
    let rtlc: *mut ListCell;

    *trivial_tlist = true; /* until proven differently */

    forfour!(
        ctlc, colTypes,
        cclc, colCollations,
        itlc, input_tlist,
        rtlc, refnames_tlist,
        {
            let colType: Oid = lfirst_oid(ctlc);
            let colColl: Oid = lfirst_oid(cclc);
            let inputtle: *mut TargetEntry = lfirst(itlc) as *mut TargetEntry;
            let reftle: *mut TargetEntry = lfirst(rtlc) as *mut TargetEntry;

            Assert!((*inputtle).resno == resno as AttrNumber);
            Assert!((*reftle).resno == resno as AttrNumber);
            Assert!(!(*inputtle).resjunk);
            Assert!(!(*reftle).resjunk);

            /*
             * Generate columns referencing input columns and having appropriate
             * data types and column names.  Insert datatype coercions where
             * necessary.
             *
             * HACK: constants in the input's targetlist are copied up as-is
             * rather than being referenced as subquery outputs.  This is mainly
             * to ensure that when we try to coerce them to the output column's
             * datatype, the right things happen for UNKNOWN constants.  But do
             * this only at the first level of subquery-scan plans; we don't want
             * phony constants appearing in the output tlists of upper-level
             * nodes!
             *
             * Note that copying a constant doesn't in itself require us to mark
             * the tlist nontrivial; see trivial_subqueryscan() in setrefs.c.
             */
            use crate::nodes::nodes::NodeTag::T_Const;
            let mut expr: *mut Node = if hack_constants
                && !(*inputtle).expr.is_null()
                && IsA!((*inputtle).expr, T_Const)
            {
                (*inputtle).expr as *mut Node
            } else {
                makeVar(
                    varno,
                    (*inputtle).resno,
                    exprType((*inputtle).expr as *const Node),
                    exprTypmod((*inputtle).expr as *const Node),
                    exprCollation((*inputtle).expr as *const Node),
                    0,
                ) as *mut Node
            };

            if exprType(expr as *const Node) != colType {
                /*
                 * Note: it's not really cool to be applying coerce_to_common_type
                 * here; one notable point is that assign_expr_collations never
                 * gets run on any generated nodes.  For the moment that's not a
                 * problem because we force the correct exposed collation below.
                 * It would likely be best to make the parser generate the correct
                 * output tlist for every set-op to begin with, though.
                 */
                expr = coerce_to_common_type(
                    ptr::null_mut(), /* no UNKNOWNs here */
                    expr,
                    colType,
                    b"UNION/INTERSECT/EXCEPT\0".as_ptr() as *const c_char,
                );
                *trivial_tlist = false; /* the coercion makes it not trivial */
            }

            /*
             * Ensure the tlist entry's exposed collation matches the set-op. This
             * is necessary because plan_set_operations() reports the result
             * ordering as a list of SortGroupClauses, which don't carry collation
             * themselves but just refer to tlist entries.  If we don't show the
             * right collation then planner.c might do the wrong thing in
             * higher-level queries.
             *
             * Note we use RelabelType, not CollateExpr, since this expression
             * will reach the executor without any further processing.
             */
            if exprCollation(expr as *const Node) != colColl {
                expr = applyRelabelType(
                    expr,
                    exprType(expr as *const Node),
                    exprTypmod(expr as *const Node),
                    colColl,
                    4, /* COERCE_IMPLICIT_CAST */
                    -1,
                    false,
                );
                *trivial_tlist = false; /* the relabel makes it not trivial */
            }

            let tle: *mut TargetEntry = makeTargetEntry(
                expr as *mut Expr,
                resno as AttrNumber,
                pstrdup((*reftle).resname),
                false,
            );

            /*
             * By convention, all output columns in a setop tree have
             * ressortgroupref equal to their resno.  In some cases the ref isn't
             * needed, but this is a cleaner way than modifying the tlist later.
             */
            (*tle).ressortgroupref = (*tle).resno as Index;

            tlist = lappend(tlist, tle as *mut c_void);
            resno += 1;
        }
    );

    tlist
}

/*
 * Generate targetlist for a set-operation Append node
 *
 * colTypes: OID list of set-op's result column datatypes
 * colCollations: OID list of set-op's result column collations
 * input_tlists: list of tlists for sub-plans of the Append
 * refnames_tlist: targetlist to take column names from
 *
 * The entries in the Append's targetlist should always be simple Vars;
 * we just have to make sure they have the right datatypes/typmods/collations.
 * The Vars are always generated with varno 0.
 *
 * XXX a problem with the varno-zero approach is that set_pathtarget_cost_width
 * cannot figure out a realistic width for the tlist we make here.  But we
 * ought to refactor this code to produce a PathTarget directly, anyway.
 */
unsafe fn generate_append_tlist(
    colTypes: *mut List,
    colCollations: *mut List,
    input_tlists: *mut List,
    refnames_tlist: *mut List,
) -> *mut List {
    let mut tlist: *mut List = NIL;
    let mut resno: c_int = 1;
    let curColType: *mut ListCell;
    let curColCollation: *mut ListCell;
    let ref_tl_item: *mut ListCell;
    let mut colindex: c_int;
    let tlistl: *mut ListCell;

    /*
     * First extract typmods to use.
     *
     * If the inputs all agree on type and typmod of a particular column, use
     * that typmod; else use -1.
     */
    let ncols: usize = list_length(colTypes) as usize;
    let colTypmods: *mut int32 = palloc(ncols * core::mem::size_of::<int32>()) as *mut int32;

    foreach!(tlistl, input_tlists, {
        let subtlist: *mut List = lfirst(current_cell!(tlistl)) as *mut List;
        let subtlistl: *mut ListCell;
        let mut curColType_inner: *mut ListCell = list_head(colTypes);

        colindex = 0;
        foreach!(subtlistl, subtlist, {
            let subtle: *mut TargetEntry = lfirst(current_cell!(subtlistl)) as *mut TargetEntry;

            Assert!(!(*subtle).resjunk);
            Assert!(!curColType_inner.is_null());
            if exprType((*subtle).expr as *const Node) == lfirst_oid(curColType_inner) {
                /* If first subplan, copy the typmod; else compare */
                let subtypmod: int32 = exprTypmod((*subtle).expr as *const Node);

                if current_cell!(tlistl) == list_head(input_tlists) as *mut ListCell {
                    *colTypmods.add(colindex as usize) = subtypmod;
                } else if subtypmod != *colTypmods.add(colindex as usize) {
                    *colTypmods.add(colindex as usize) = -1;
                }
            } else {
                /* types disagree, so force typmod to -1 */
                *colTypmods.add(colindex as usize) = -1;
            }
            curColType_inner = lnext(colTypes, curColType_inner);
            colindex += 1;
        });
        Assert!(curColType_inner.is_null());
    });

    /*
     * Now we can build the tlist for the Append.
     */
    colindex = 0;
    forthree!(
        curColType, colTypes,
        curColCollation, colCollations,
        ref_tl_item, refnames_tlist,
        {
            let colType: Oid = lfirst_oid(curColType);
            let colTypmod: int32 = *colTypmods.add(colindex as usize);
            let colColl: Oid = lfirst_oid(curColCollation);
            let reftle: *mut TargetEntry = lfirst(ref_tl_item) as *mut TargetEntry;
            colindex += 1;

            Assert!((*reftle).resno == resno as AttrNumber);
            Assert!(!(*reftle).resjunk);
            let expr: *mut Node = makeVar(0, resno as AttrNumber, colType, colTypmod, colColl, 0) as *mut Node;
            let tle: *mut TargetEntry = makeTargetEntry(
                expr as *mut Expr,
                resno as AttrNumber,
                pstrdup((*reftle).resname),
                false,
            );

            /*
             * By convention, all output columns in a setop tree have
             * ressortgroupref equal to their resno.  In some cases the ref isn't
             * needed, but this is a cleaner way than modifying the tlist later.
             */
            (*tle).ressortgroupref = (*tle).resno as Index;

            tlist = lappend(tlist, tle as *mut c_void);
            resno += 1;
        }
    );

    pfree(colTypmods as *mut c_void);

    tlist
}

/*
 * generate_setop_grouplist
 *        Build a SortGroupClause list defining the sort/grouping properties
 *        of the setop's output columns.
 *
 * Parse analysis already determined the properties and built a suitable
 * list, except that the entries do not have sortgrouprefs set because
 * the parser output representation doesn't include a tlist for each
 * setop.  So what we need to do here is copy that list and install
 * proper sortgrouprefs into it (copying those from the targetlist).
 */
unsafe fn generate_setop_grouplist(
    op: *mut SetOperationStmt,
    targetlist: *mut List,
) -> *mut List {
    let grouplist: *mut List = copyObject((*op).groupClauses as *const List);
    let lt: *mut ListCell;
    let mut lg: *mut ListCell = list_head(grouplist);

    foreach!(lt, targetlist, {
        let tle: *mut TargetEntry = lfirst(current_cell!(lt)) as *mut TargetEntry;
        let sgc: *mut SortGroupClause;

        Assert!(!(*tle).resjunk);

        /* non-resjunk columns should have sortgroupref = resno */
        Assert!((*tle).ressortgroupref == (*tle).resno as Index);

        /* non-resjunk columns should have grouping clauses */
        Assert!(!lg.is_null());
        sgc = lfirst(lg) as *mut SortGroupClause;
        lg = lnext(grouplist, lg);
        Assert!((*sgc).tleSortGroupRef == 0);

        (*sgc).tleSortGroupRef = (*tle).ressortgroupref;
    });
    Assert!(lg.is_null());
    grouplist
}
