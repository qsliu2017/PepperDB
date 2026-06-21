//! Routines to manipulate pathlists and create path nodes.
//!
//! Translated from PostgreSQL 18.3 `src/backend/optimizer/util/pathnode.c`.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

#![allow(non_snake_case, non_upper_case_globals, unused_variables, dead_code)]

use crate::prelude::*;
use crate::{IsA, makeNode, foreach, current_cell, foreach_delete_current, foreach_current_index};
use crate::nodes::nodes::{
    Node, NodeTag,
    NodeTag::*,
    AggStrategy::{self, *},
    SetOpStrategy::*,
    JoinType::JOIN_SEMI,
    AggSplit,
    SetOpCmd,
    OnConflictAction,
    LimitOption,
    CmdType,
    Cost, Cardinality,
};
use crate::nodes::pg_list::{
    List, ListCell, NIL,
    lappend, lappend_int, lappend_oid, lcons,
    list_concat, list_insert_nth, list_delete_last, list_copy_head,
    list_free, list_sort, list_length,
    lfirst, lfirst_int, lfirst_oid, linitial,
};
use crate::nodes::pathnodes::Relids;
use crate::nodes::pathnodes::{
    PlannerInfo, RelOptInfo, Path, ParamPathInfo,
    PathTarget, PathKey, QualCost, AggClauseCosts,
    IndexOptInfo,
    // path node types
    IndexPath, BitmapHeapPath, BitmapAndPath, BitmapOrPath,
    TidPath, TidRangePath, AppendPath, MergeAppendPath,
    GroupResultPath, MaterialPath, MemoizePath, UniquePath,
    GatherPath, GatherMergePath, SubqueryScanPath, ForeignPath,
    CustomPath, ProjectionPath, ProjectSetPath, SortPath,
    IncrementalSortPath, GroupPath, UpperUniquePath, AggPath,
    GroupingSetsPath, MinMaxAggPath, WindowAggPath, SetOpPath,
    RecursiveUnionPath, LockRowsPath, ModifyTablePath, LimitPath,
    NestPath, MergePath, HashPath,
    JoinPath, JoinCostWorkspace, JoinPathExtraData,
    SpecialJoinInfo, AppendRelInfo, MinMaxAggInfo, RollupData,
    RelOptKind::RELOPT_BASEREL,
    // enums
    CostSelector::{self, *},
    UniquePathMethod::{self, *},
    IS_SIMPLE_REL, IS_OTHER_REL, PATH_REQ_OUTER,
};
use crate::nodes::primnodes::{Expr, Var, OnConflictExpr, TargetEntry};
use crate::nodes::parsenodes::{RangeTblEntry, RTEKind::*, SortGroupClause, WindowClause, TableSampleClause};
use crate::nodes::bitmapset::{
    Bitmapset,
    BMS_Comparison::{self, *},
    bms_copy, bms_equal, bms_is_empty, bms_is_subset, bms_overlap,
    bms_union, bms_add_members, bms_del_members, bms_free,
    bms_compare, bms_subset_compare, bms_is_member,
};
use crate::access::htup_details::SizeofMinimalTupleHeader;
use crate::c::{OidIsValid, MAXALIGN, Index};
use crate::utils::palloc::{GetMemoryChunkContext, MemoryContextSwitchTo, pfree};
use crate::miscadmin::{work_mem, CHECK_FOR_INTERRUPTS, get_hash_memory_limit};

// Optimizer path-related imports
use crate::optimizer::paths::{
    PathKeysComparison,
    PATHKEYS_EQUAL, PATHKEYS_BETTER1, PATHKEYS_BETTER2, PATHKEYS_DIFFERENT,
    compare_pathkeys, pathkeys_contained_in, relation_has_unique_index_for,
    make_pathkeys_for_sortclauses,
};
use crate::optimizer::optimizer::{cpu_tuple_cost, cpu_operator_cost, clamp_row_est};
use crate::optimizer::path::costsize::{
    enable_hashagg, enable_memoize,
    cost_seqscan, cost_samplescan, cost_index, cost_bitmap_heap_scan,
    cost_bitmap_and_node, cost_bitmap_or_node, cost_tidscan, cost_tidrangescan,
    cost_append, cost_merge_append, cost_material, cost_sort, cost_incremental_sort,
    cost_agg, cost_group, cost_qual_eval, cost_windowagg, cost_recursive_union,
    cost_gather, cost_gather_merge, cost_subqueryscan, cost_functionscan,
    cost_tablefuncscan, cost_valuesscan, cost_ctescan, cost_namedtuplestorescan,
    cost_resultscan, expression_returns_set_rows, estimate_num_groups,
};
use crate::optimizer::plan::createplan::is_projection_capable_path;
use crate::optimizer::path::costsize::{
    final_cost_nestloop, final_cost_mergejoin, final_cost_hashjoin,
};
use crate::optimizer::util::relnode::{
    get_baserel_parampathinfo, get_appendrel_parampathinfo,
    get_joinrel_parampathinfo, get_param_path_clause_serials,
    find_param_path_info,
};
use crate::optimizer::util::tlist::copy_pathtarget;
use crate::optimizer::util::appendinfo::{
    adjust_appendrel_attrs_multilevel, adjust_child_relids_multilevel,
};
use crate::optimizer::util::clauses::is_parallel_safe;
use crate::nodes::makefuncs::makeTargetEntry;
use crate::nodes::equalfuncs::equal;

// -----------------------------------------------------------------------
// Stubs for unported dependencies
// -----------------------------------------------------------------------

/// TODO(pg-port): parser/parsetree.h - planner_rt_fetch(rti, root)
#[inline]
pub unsafe fn planner_rt_fetch(rti: Index, root: *mut PlannerInfo) -> *mut RangeTblEntry {
    if !(*root).simple_rte_array.is_null() {
        *(*root).simple_rte_array.add(rti as usize)
    } else {
        crate::parser::parsetree::rt_fetch(rti, (*(*root).parse).rtable)
    }
}

/// TODO(pg-port): optimizer/util/clauses.c - query_supports_distinctness
pub unsafe fn query_supports_distinctness(
    query: *mut crate::nodes::parsenodes::Query,
) -> bool {
    crate::optimizer::plan::analyzejoins::query_supports_distinctness(query as _)
}

/// TODO(pg-port): optimizer/util/clauses.c - query_is_distinct_for
pub unsafe fn query_is_distinct_for(
    query: *mut crate::nodes::parsenodes::Query,
    colnos: *mut List,
    opids: *mut List,
) -> bool {
    crate::optimizer::plan::analyzejoins::query_is_distinct_for(query as _, colnos, opids)
}

/// TODO(pg-port): parser/parse_clause.c - assignSortGroupRef
pub unsafe fn assignSortGroupRef(
    tle: *mut TargetEntry,
    tlist: *mut List,
) -> Index {
    crate::parser::parse_clause::assignSortGroupRef(tle as _, tlist)
}

/// TODO(pg-port): utils/lsyscache.c - get_ordering_op_for_equality_op
pub unsafe fn get_ordering_op_for_equality_op(opno: Oid, use_lhs_type: bool) -> Oid {
    crate::utils::cache::lsyscache::get_ordering_op_for_equality_op(opno, use_lhs_type)
}

/// TODO(pg-port): utils/lsyscache.c - get_equality_op_for_ordering_op
pub unsafe fn get_equality_op_for_ordering_op(opno: Oid, reverse: *mut bool) -> Oid {
    crate::utils::cache::lsyscache::get_equality_op_for_ordering_op(opno, reverse)
}

/// TODO(pg-port): foreign/fdwapi.h - ReparameterizeForeignPathByChild_function type
pub type ReparameterizeForeignPathByChild_function =
    Option<unsafe extern "C" fn(*mut PlannerInfo, *mut List, *mut RelOptInfo) -> *mut List>;

/// copyObject stub - shallow copy (same as setrefs.rs)
/// TODO(pg-port): replace with real recursive copyObject from copyfuncs.c
unsafe fn copyObject<T>(node: *const T) -> *mut T {
    use crate::utils::palloc::palloc;
    if node.is_null() {
        return core::ptr::null_mut();
    }
    let p = palloc(core::mem::size_of::<T>()) as *mut T;
    core::ptr::copy_nonoverlapping(node, p, 1);
    p
}

// -----------------------------------------------------------------------
// Local enum (C file-scope)
// -----------------------------------------------------------------------

#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum PathCostComparison {
    COSTS_EQUAL,     /* path costs are fuzzily equal */
    COSTS_BETTER1,   /* first path is cheaper than second */
    COSTS_BETTER2,   /* second path is cheaper than first */
    COSTS_DIFFERENT, /* neither path dominates the other on cost */
}
use PathCostComparison::*;

/*
 * STD_FUZZ_FACTOR is the normal fuzz factor for compare_path_costs_fuzzily.
 * XXX is it worth making this user-controllable?  It provides a tradeoff
 * between planner runtime and the accuracy of path cost comparisons.
 */
const STD_FUZZ_FACTOR: f64 = 1.01;

/*****************************************************************************
 *		MISC. PATH UTILITIES
 *****************************************************************************/

/*
 * compare_path_costs
 *	  Return -1, 0, or +1 according as path1 is cheaper, the same cost,
 *	  or more expensive than path2 for the specified criterion.
 */
pub unsafe fn compare_path_costs(
    path1: *const Path,
    path2: *const Path,
    criterion: CostSelector,
) -> c_int {
    /* Number of disabled nodes, if different, trumps all else. */
    if unlikely((*path1).disabled_nodes != (*path2).disabled_nodes) {
        if (*path1).disabled_nodes < (*path2).disabled_nodes {
            return -1;
        } else {
            return 1;
        }
    }

    if criterion == STARTUP_COST {
        if (*path1).startup_cost < (*path2).startup_cost {
            return -1;
        }
        if (*path1).startup_cost > (*path2).startup_cost {
            return 1;
        }

        /*
         * If paths have the same startup cost (not at all unlikely), order
         * them by total cost.
         */
        if (*path1).total_cost < (*path2).total_cost {
            return -1;
        }
        if (*path1).total_cost > (*path2).total_cost {
            return 1;
        }
    } else {
        if (*path1).total_cost < (*path2).total_cost {
            return -1;
        }
        if (*path1).total_cost > (*path2).total_cost {
            return 1;
        }

        /*
         * If paths have the same total cost, order them by startup cost.
         */
        if (*path1).startup_cost < (*path2).startup_cost {
            return -1;
        }
        if (*path1).startup_cost > (*path2).startup_cost {
            return 1;
        }
    }
    0
}

/*
 * compare_fractional_path_costs
 *	  Return -1, 0, or +1 according as path1 is cheaper, the same cost,
 *	  or more expensive than path2 for fetching the specified fraction
 *	  of the total tuples.
 *
 * If fraction is <= 0 or > 1, we interpret it as 1, ie, we select the
 * path with the cheaper total_cost.
 */
pub unsafe fn compare_fractional_path_costs(
    path1: *const Path,
    path2: *const Path,
    fraction: f64,
) -> c_int {
    let cost1: Cost;
    let cost2: Cost;

    /* Number of disabled nodes, if different, trumps all else. */
    if unlikely((*path1).disabled_nodes != (*path2).disabled_nodes) {
        if (*path1).disabled_nodes < (*path2).disabled_nodes {
            return -1;
        } else {
            return 1;
        }
    }

    if fraction <= 0.0 || fraction >= 1.0 {
        return compare_path_costs(path1, path2, TOTAL_COST);
    }
    cost1 = (*path1).startup_cost + fraction * ((*path1).total_cost - (*path1).startup_cost);
    cost2 = (*path2).startup_cost + fraction * ((*path2).total_cost - (*path2).startup_cost);
    if cost1 < cost2 {
        return -1;
    }
    if cost1 > cost2 {
        return 1;
    }
    0
}

/*
 * compare_path_costs_fuzzily
 *	  Compare the costs of two paths to see if either can be said to
 *	  dominate the other.
 *
 * We use fuzzy comparisons so that add_path() can avoid keeping both of
 * a pair of paths that really have insignificantly different cost.
 *
 * The fuzz_factor argument must be 1.0 plus delta, where delta is the
 * fraction of the smaller cost that is considered to be a significant
 * difference.  For example, fuzz_factor = 1.01 makes the fuzziness limit
 * be 1% of the smaller cost.
 *
 * The two paths are said to have "equal" costs if both startup and total
 * costs are fuzzily the same.  Path1 is said to be better than path2 if
 * it has fuzzily better startup cost and fuzzily no worse total cost,
 * or if it has fuzzily better total cost and fuzzily no worse startup cost.
 * Path2 is better than path1 if the reverse holds.  Finally, if one path
 * is fuzzily better than the other on startup cost and fuzzily worse on
 * total cost, we just say that their costs are "different", since neither
 * dominates the other across the whole performance spectrum.
 *
 * This function also enforces a policy rule that paths for which the relevant
 * one of parent->consider_startup and parent->consider_param_startup is false
 * cannot survive comparisons solely on the grounds of good startup cost, so
 * we never return COSTS_DIFFERENT when that is true for the total-cost loser.
 * (But if total costs are fuzzily equal, we compare startup costs anyway,
 * in hopes of eliminating one path or the other.)
 */
unsafe fn compare_path_costs_fuzzily(
    path1: *const Path,
    path2: *const Path,
    fuzz_factor: f64,
) -> PathCostComparison {
    /* #define CONSIDER_PATH_STARTUP_COST(p)  \
     *   ((p)->param_info == NULL ? (p)->parent->consider_startup : (p)->parent->consider_param_startup)
     */
    #[inline]
    unsafe fn CONSIDER_PATH_STARTUP_COST(p: *const Path) -> bool {
        if (*p).param_info.is_null() {
            (*(*p).parent).consider_startup
        } else {
            (*(*p).parent).consider_param_startup
        }
    }

    /* Number of disabled nodes, if different, trumps all else. */
    if unlikely((*path1).disabled_nodes != (*path2).disabled_nodes) {
        if (*path1).disabled_nodes < (*path2).disabled_nodes {
            return COSTS_BETTER1;
        } else {
            return COSTS_BETTER2;
        }
    }

    /*
     * Check total cost first since it's more likely to be different; many
     * paths have zero startup cost.
     */
    if (*path1).total_cost > (*path2).total_cost * fuzz_factor {
        /* path1 fuzzily worse on total cost */
        if CONSIDER_PATH_STARTUP_COST(path1)
            && (*path2).startup_cost > (*path1).startup_cost * fuzz_factor
        {
            /* ... but path2 fuzzily worse on startup, so DIFFERENT */
            return COSTS_DIFFERENT;
        }
        /* else path2 dominates */
        return COSTS_BETTER2;
    }
    if (*path2).total_cost > (*path1).total_cost * fuzz_factor {
        /* path2 fuzzily worse on total cost */
        if CONSIDER_PATH_STARTUP_COST(path2)
            && (*path1).startup_cost > (*path2).startup_cost * fuzz_factor
        {
            /* ... but path1 fuzzily worse on startup, so DIFFERENT */
            return COSTS_DIFFERENT;
        }
        /* else path1 dominates */
        return COSTS_BETTER1;
    }
    /* fuzzily the same on total cost ... */
    if (*path1).startup_cost > (*path2).startup_cost * fuzz_factor {
        /* ... but path1 fuzzily worse on startup, so path2 wins */
        return COSTS_BETTER2;
    }
    if (*path2).startup_cost > (*path1).startup_cost * fuzz_factor {
        /* ... but path2 fuzzily worse on startup, so path1 wins */
        return COSTS_BETTER1;
    }
    /* fuzzily the same on both costs */
    COSTS_EQUAL
}

/*
 * set_cheapest
 *	  Find the minimum-cost paths from among a relation's paths,
 *	  and save them in the rel's cheapest-path fields.
 */
pub unsafe fn set_cheapest(parent_rel: *mut RelOptInfo) {
    let mut cheapest_startup_path: *mut Path = core::ptr::null_mut();
    let mut cheapest_total_path: *mut Path = core::ptr::null_mut();
    let mut best_param_path: *mut Path = core::ptr::null_mut();
    let mut parameterized_paths: *mut List = NIL;

    Assert!(IsA!(parent_rel, T_RelOptInfo));

    if (*parent_rel).pathlist == NIL {
        ereport!(
            ERROR,
            errmsg!("could not devise a query plan for the given query")
        );
    }

    cheapest_startup_path = core::ptr::null_mut();
    cheapest_total_path = core::ptr::null_mut();
    best_param_path = core::ptr::null_mut();
    parameterized_paths = NIL;

    foreach!(p, (*parent_rel).pathlist, {
        let path: *mut Path = lfirst(current_cell!(p)) as *mut Path;
        let cmp: c_int;

        if !(*path).param_info.is_null() {
            /* Parameterized path, so add it to parameterized_paths */
            parameterized_paths = lappend(parameterized_paths, path as *mut c_void);

            /*
             * If we have an unparameterized cheapest-total, we no longer care
             * about finding the best parameterized path, so move on.
             */
            if !cheapest_total_path.is_null() {
                continue;
            }

            /*
             * Otherwise, track the best parameterized path, which is the one
             * with least total cost among those of the minimum
             * parameterization.
             */
            if best_param_path.is_null() {
                best_param_path = path;
            } else {
                match bms_subset_compare(PATH_REQ_OUTER(path), PATH_REQ_OUTER(best_param_path)) {
                    BMS_EQUAL => {
                        /* keep the cheaper one */
                        if compare_path_costs(path, best_param_path, TOTAL_COST) < 0 {
                            best_param_path = path;
                        }
                    }
                    BMS_SUBSET1 => {
                        /* new path is less-parameterized */
                        best_param_path = path;
                    }
                    BMS_SUBSET2 => {
                        /* old path is less-parameterized, keep it */
                    }
                    BMS_DIFFERENT => {
                        /*
                         * This means that neither path has the least possible
                         * parameterization for the rel.  We'll sit on the old
                         * path until something better comes along.
                         */
                    }
                }
            }
        } else {
            /* Unparameterized path, so consider it for cheapest slots */
            if cheapest_total_path.is_null() {
                cheapest_startup_path = path;
                cheapest_total_path = path;
                continue;
            }

            /*
             * If we find two paths of identical costs, try to keep the
             * better-sorted one.  The paths might have unrelated sort
             * orderings, in which case we can only guess which might be
             * better to keep, but if one is superior then we definitely
             * should keep that one.
             */
            let cmp = compare_path_costs(cheapest_startup_path, path, STARTUP_COST);
            if cmp > 0
                || (cmp == 0
                    && compare_pathkeys((*cheapest_startup_path).pathkeys, (*path).pathkeys)
                        == PATHKEYS_BETTER2)
            {
                cheapest_startup_path = path;
            }

            let cmp = compare_path_costs(cheapest_total_path, path, TOTAL_COST);
            if cmp > 0
                || (cmp == 0
                    && compare_pathkeys((*cheapest_total_path).pathkeys, (*path).pathkeys)
                        == PATHKEYS_BETTER2)
            {
                cheapest_total_path = path;
            }
        }
    });

    /* Add cheapest unparameterized path, if any, to parameterized_paths */
    if !cheapest_total_path.is_null() {
        parameterized_paths = lcons(cheapest_total_path as *mut c_void, parameterized_paths);
    }

    /*
     * If there is no unparameterized path, use the best parameterized path as
     * cheapest_total_path (but not as cheapest_startup_path).
     */
    if cheapest_total_path.is_null() {
        cheapest_total_path = best_param_path;
    }
    Assert!(cheapest_total_path != core::ptr::null_mut());

    (*parent_rel).cheapest_startup_path = cheapest_startup_path;
    (*parent_rel).cheapest_total_path = cheapest_total_path;
    (*parent_rel).cheapest_unique_path = core::ptr::null_mut(); /* computed only if needed */
    (*parent_rel).cheapest_parameterized_paths = parameterized_paths;
}

/*
 * add_path
 *	  Consider a potential implementation path for the specified parent rel,
 *	  and add it to the rel's pathlist if it is worthy of consideration.
 */
pub unsafe fn add_path(parent_rel: *mut RelOptInfo, new_path: *mut Path) {
    let mut accept_new: bool = true; /* unless we find a superior old path */
    let mut insert_at: c_int = 0;   /* where to insert new item */
    let new_path_pathkeys: *mut List;

    /*
     * This is a convenient place to check for query cancel --- no part of the
     * planner goes very long without calling add_path().
     */
    CHECK_FOR_INTERRUPTS();

    /* Pretend parameterized paths have no pathkeys, per comment above */
    new_path_pathkeys = if !(*new_path).param_info.is_null() {
        NIL
    } else {
        (*new_path).pathkeys
    };

    /*
     * Loop to check proposed new path against old paths.  Note it is possible
     * for more than one old path to be tossed out because new_path dominates it.
     */
    foreach!(p1, (*parent_rel).pathlist, {
        let old_path: *mut Path = lfirst(current_cell!(p1)) as *mut Path;
        let mut remove_old: bool = false; /* unless new proves superior */
        let costcmp: PathCostComparison;
        let keyscmp: PathKeysComparison;
        let outercmp: BMS_Comparison;

        /*
         * Do a fuzzy cost comparison with standard fuzziness limit.
         */
        costcmp = compare_path_costs_fuzzily(new_path, old_path, STD_FUZZ_FACTOR);

        /*
         * If the two paths compare differently for startup and total cost,
         * then we want to keep both, and we can skip comparing pathkeys and
         * required_outer rels.  If they compare the same, proceed with the
         * other comparisons.  Row count is checked last.
         */
        if costcmp != COSTS_DIFFERENT {
            /* Similarly check to see if either dominates on pathkeys */
            let old_path_pathkeys: *mut List = if !(*old_path).param_info.is_null() {
                NIL
            } else {
                (*old_path).pathkeys
            };
            keyscmp = compare_pathkeys(new_path_pathkeys, old_path_pathkeys);
            if keyscmp != PATHKEYS_DIFFERENT {
                match costcmp {
                    COSTS_EQUAL => {
                        let outercmp = bms_subset_compare(
                            PATH_REQ_OUTER(new_path),
                            PATH_REQ_OUTER(old_path),
                        );
                        if keyscmp == PATHKEYS_BETTER1 {
                            if (outercmp == BMS_EQUAL || outercmp == BMS_SUBSET1)
                                && (*new_path).rows <= (*old_path).rows
                                && (*new_path).parallel_safe >= (*old_path).parallel_safe
                            {
                                remove_old = true; /* new dominates old */
                            }
                        } else if keyscmp == PATHKEYS_BETTER2 {
                            if (outercmp == BMS_EQUAL || outercmp == BMS_SUBSET2)
                                && (*new_path).rows >= (*old_path).rows
                                && (*new_path).parallel_safe <= (*old_path).parallel_safe
                            {
                                accept_new = false; /* old dominates new */
                            }
                        } else {
                            /* keyscmp == PATHKEYS_EQUAL */
                            if outercmp == BMS_EQUAL {
                                /*
                                 * Same pathkeys and outer rels, and fuzzily
                                 * the same cost, so keep just one; to decide
                                 * which, first check parallel-safety, then
                                 * rows, then do a fuzzy cost comparison with
                                 * very small fuzz limit.
                                 */
                                if (*new_path).parallel_safe > (*old_path).parallel_safe {
                                    remove_old = true; /* new dominates old */
                                } else if (*new_path).parallel_safe < (*old_path).parallel_safe {
                                    accept_new = false; /* old dominates new */
                                } else if (*new_path).rows < (*old_path).rows {
                                    remove_old = true; /* new dominates old */
                                } else if (*new_path).rows > (*old_path).rows {
                                    accept_new = false; /* old dominates new */
                                } else if compare_path_costs_fuzzily(
                                    new_path,
                                    old_path,
                                    1.0000000001,
                                ) == COSTS_BETTER1
                                {
                                    remove_old = true; /* new dominates old */
                                } else {
                                    accept_new = false; /* old equals or dominates new */
                                }
                            } else if outercmp == BMS_SUBSET1
                                && (*new_path).rows <= (*old_path).rows
                                && (*new_path).parallel_safe >= (*old_path).parallel_safe
                            {
                                remove_old = true; /* new dominates old */
                            } else if outercmp == BMS_SUBSET2
                                && (*new_path).rows >= (*old_path).rows
                                && (*new_path).parallel_safe <= (*old_path).parallel_safe
                            {
                                accept_new = false; /* old dominates new */
                            }
                            /* else different parameterizations, keep both */
                        }
                    }
                    COSTS_BETTER1 => {
                        if keyscmp != PATHKEYS_BETTER2 {
                            let outercmp = bms_subset_compare(
                                PATH_REQ_OUTER(new_path),
                                PATH_REQ_OUTER(old_path),
                            );
                            if (outercmp == BMS_EQUAL || outercmp == BMS_SUBSET1)
                                && (*new_path).rows <= (*old_path).rows
                                && (*new_path).parallel_safe >= (*old_path).parallel_safe
                            {
                                remove_old = true; /* new dominates old */
                            }
                        }
                    }
                    COSTS_BETTER2 => {
                        if keyscmp != PATHKEYS_BETTER1 {
                            let outercmp = bms_subset_compare(
                                PATH_REQ_OUTER(new_path),
                                PATH_REQ_OUTER(old_path),
                            );
                            if (outercmp == BMS_EQUAL || outercmp == BMS_SUBSET2)
                                && (*new_path).rows >= (*old_path).rows
                                && (*new_path).parallel_safe <= (*old_path).parallel_safe
                            {
                                accept_new = false; /* old dominates new */
                            }
                        }
                    }
                    COSTS_DIFFERENT => {
                        /*
                         * can't get here, but keep this case to keep compiler
                         * quiet
                         */
                    }
                }
            }
        }

        /*
         * Remove current element from pathlist if dominated by new.
         */
        if remove_old {
            (*parent_rel).pathlist =
                foreach_delete_current!((*parent_rel).pathlist, p1);

            /*
             * Delete the data pointed-to by the deleted cell, if possible
             */
            if !IsA!(old_path, T_IndexPath) {
                pfree(old_path as *mut c_void);
            }
        } else {
            /*
             * new belongs after this old path if it has more disabled nodes
             * or if it has the same number of nodes but a greater total cost
             */
            if (*new_path).disabled_nodes > (*old_path).disabled_nodes
                || ((*new_path).disabled_nodes == (*old_path).disabled_nodes
                    && (*new_path).total_cost >= (*old_path).total_cost)
            {
                insert_at = foreach_current_index!(p1) + 1;
            }
        }

        /*
         * If we found an old path that dominates new_path, we can quit
         * scanning the pathlist; we will not add new_path, and we assume
         * new_path cannot dominate any other elements of the pathlist.
         */
        if !accept_new {
            break;
        }
    });

    if accept_new {
        /* Accept the new path: insert it at proper place in pathlist */
        (*parent_rel).pathlist =
            list_insert_nth((*parent_rel).pathlist, insert_at, new_path as *mut c_void);
    } else {
        /* Reject and recycle the new path */
        if !IsA!(new_path, T_IndexPath) {
            pfree(new_path as *mut c_void);
        }
    }
}

/*
 * add_path_precheck
 *	  Check whether a proposed new path could possibly get accepted.
 *	  We assume we know the path's pathkeys and parameterization accurately,
 *	  and have lower bounds for its costs.
 */
pub unsafe fn add_path_precheck(
    parent_rel: *mut RelOptInfo,
    disabled_nodes: c_int,
    startup_cost: Cost,
    total_cost: Cost,
    pathkeys: *mut List,
    required_outer: Relids,
) -> bool {
    let new_path_pathkeys: *mut List;
    let consider_startup: bool;

    /* Pretend parameterized paths have no pathkeys, per add_path policy */
    new_path_pathkeys = if !required_outer.is_null() { NIL } else { pathkeys };

    /* Decide whether new path's startup cost is interesting */
    consider_startup = if !required_outer.is_null() {
        (*parent_rel).consider_param_startup
    } else {
        (*parent_rel).consider_startup
    };

    foreach!(p1, (*parent_rel).pathlist, {
        let old_path: *mut Path = lfirst(current_cell!(p1)) as *mut Path;
        let keyscmp: PathKeysComparison;

        /*
         * Since the pathlist is sorted by disabled_nodes and then by
         * total_cost, we can stop looking once we reach a path with more
         * disabled nodes, or the same number of disabled nodes plus a
         * total_cost larger than the new path's.
         */
        if unlikely((*old_path).disabled_nodes != disabled_nodes) {
            if disabled_nodes < (*old_path).disabled_nodes {
                break;
            }
        } else if total_cost <= (*old_path).total_cost * STD_FUZZ_FACTOR {
            break;
        }

        /*
         * We are looking for an old_path with the same parameterization (and
         * by assumption the same rowcount) that dominates the new path on
         * pathkeys as well as both cost metrics.  If we find one, we can
         * reject the new path.
         *
         * Cost comparisons here should match compare_path_costs_fuzzily.
         */
        /* new path can win on startup cost only if consider_startup */
        if startup_cost > (*old_path).startup_cost * STD_FUZZ_FACTOR || !consider_startup {
            /* new path loses on cost, so check pathkeys... */
            let old_path_pathkeys: *mut List = if !(*old_path).param_info.is_null() {
                NIL
            } else {
                (*old_path).pathkeys
            };
            keyscmp = compare_pathkeys(new_path_pathkeys, old_path_pathkeys);
            if keyscmp == PATHKEYS_EQUAL || keyscmp == PATHKEYS_BETTER2 {
                /* new path does not win on pathkeys... */
                if bms_equal(required_outer, PATH_REQ_OUTER(old_path)) {
                    /* Found an old path that dominates the new one */
                    return false;
                }
            }
        }
    });

    true
}

/*
 * add_partial_path
 *	  Like add_path, our goal here is to consider whether a path is worthy
 *	  of being kept around, but the considerations here are a bit different.
 *	  A partial path is one which can be executed in any number of workers in
 *	  parallel such that each worker will generate a subset of the path's
 *	  overall result.
 */
pub unsafe fn add_partial_path(parent_rel: *mut RelOptInfo, new_path: *mut Path) {
    let mut accept_new: bool = true; /* unless we find a superior old path */
    let mut insert_at: c_int = 0;   /* where to insert new item */

    /* Check for query cancel. */
    CHECK_FOR_INTERRUPTS();

    /* Path to be added must be parallel safe. */
    Assert!((*new_path).parallel_safe);

    /* Relation should be OK for parallelism, too. */
    Assert!((*parent_rel).consider_parallel);

    /*
     * As in add_path, throw out any paths which are dominated by the new
     * path, but throw out the new path if some existing path dominates it.
     */
    foreach!(p1, (*parent_rel).partial_pathlist, {
        let old_path: *mut Path = lfirst(current_cell!(p1)) as *mut Path;
        let mut remove_old: bool = false; /* unless new proves superior */
        let keyscmp: PathKeysComparison;

        /* Compare pathkeys. */
        keyscmp = compare_pathkeys((*new_path).pathkeys, (*old_path).pathkeys);

        /* Unless pathkeys are incompatible, keep just one of the two paths. */
        if keyscmp != PATHKEYS_DIFFERENT {
            if unlikely((*new_path).disabled_nodes != (*old_path).disabled_nodes) {
                if (*new_path).disabled_nodes > (*old_path).disabled_nodes {
                    accept_new = false;
                } else {
                    remove_old = true;
                }
            } else if (*new_path).total_cost > (*old_path).total_cost * STD_FUZZ_FACTOR {
                /* New path costs more; keep it only if pathkeys are better. */
                if keyscmp != PATHKEYS_BETTER1 {
                    accept_new = false;
                }
            } else if (*old_path).total_cost > (*new_path).total_cost * STD_FUZZ_FACTOR {
                /* Old path costs more; keep it only if pathkeys are better. */
                if keyscmp != PATHKEYS_BETTER2 {
                    remove_old = true;
                }
            } else if keyscmp == PATHKEYS_BETTER1 {
                /* Costs are about the same, new path has better pathkeys. */
                remove_old = true;
            } else if keyscmp == PATHKEYS_BETTER2 {
                /* Costs are about the same, old path has better pathkeys. */
                accept_new = false;
            } else if (*old_path).total_cost > (*new_path).total_cost * 1.0000000001 {
                /* Pathkeys are the same, and the old path costs more. */
                remove_old = true;
            } else {
                /*
                 * Pathkeys are the same, and new path isn't materially
                 * cheaper.
                 */
                accept_new = false;
            }
        }

        /*
         * Remove current element from partial_pathlist if dominated by new.
         */
        if remove_old {
            (*parent_rel).partial_pathlist =
                foreach_delete_current!((*parent_rel).partial_pathlist, p1);
            pfree(old_path as *mut c_void);
        } else {
            /* new belongs after this old path if it has cost >= old's */
            if (*new_path).total_cost >= (*old_path).total_cost {
                insert_at = foreach_current_index!(p1) + 1;
            }
        }

        /*
         * If we found an old path that dominates new_path, we can quit
         * scanning the partial_pathlist; we will not add new_path, and we
         * assume new_path cannot dominate any later path.
         */
        if !accept_new {
            break;
        }
    });

    if accept_new {
        /* Accept the new path: insert it at proper place */
        (*parent_rel).partial_pathlist =
            list_insert_nth((*parent_rel).partial_pathlist, insert_at, new_path as *mut c_void);
    } else {
        /* Reject and recycle the new path */
        pfree(new_path as *mut c_void);
    }
}

/*
 * add_partial_path_precheck
 *	  Check whether a proposed new partial path could possibly get accepted.
 *
 * Unlike add_path_precheck, we can ignore startup cost and parameterization,
 * since they don't matter for partial paths (see add_partial_path).  But
 * we do want to make sure we don't add a partial path if there's already
 * a complete path that dominates it, since in that case the proposed path
 * is surely a loser.
 */
pub unsafe fn add_partial_path_precheck(
    parent_rel: *mut RelOptInfo,
    disabled_nodes: c_int,
    total_cost: Cost,
    pathkeys: *mut List,
) -> bool {
    foreach!(p1, (*parent_rel).partial_pathlist, {
        let old_path: *mut Path = lfirst(current_cell!(p1)) as *mut Path;
        let keyscmp: PathKeysComparison;

        keyscmp = compare_pathkeys(pathkeys, (*old_path).pathkeys);
        if keyscmp != PATHKEYS_DIFFERENT {
            if total_cost > (*old_path).total_cost * STD_FUZZ_FACTOR
                && keyscmp != PATHKEYS_BETTER1
            {
                return false;
            }
            if (*old_path).total_cost > total_cost * STD_FUZZ_FACTOR
                && keyscmp != PATHKEYS_BETTER2
            {
                return true;
            }
        }
    });

    /*
     * This path is neither clearly inferior to an existing partial path nor
     * clearly good enough that it might replace one.  Compare it to
     * non-parallel plans.
     */
    if !add_path_precheck(
        parent_rel,
        disabled_nodes,
        total_cost,
        total_cost,
        pathkeys,
        core::ptr::null_mut(),
    ) {
        return false;
    }

    true
}


/*****************************************************************************
 *		PATH NODE CREATION ROUTINES
 *****************************************************************************/

/*
 * create_seqscan_path
 *	  Creates a path corresponding to a sequential scan, returning the
 *	  pathnode.
 */
pub unsafe fn create_seqscan_path(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    required_outer: Relids,
    parallel_workers: c_int,
) -> *mut Path {
    let pathnode: *mut Path = makeNode!(Path, T_Path);
    if std::env::var("PDB_BT").is_ok() { eprintln!("PDB_BT create_seqscan_path makeNode Path -> {:p}", pathnode); }

    (*pathnode).pathtype = T_SeqScan;
    (*pathnode).parent = rel;
    (*pathnode).pathtarget = (*rel).reltarget;
    (*pathnode).param_info = get_baserel_parampathinfo(root, rel, required_outer);
    (*pathnode).parallel_aware = parallel_workers > 0;
    (*pathnode).parallel_safe = (*rel).consider_parallel;
    (*pathnode).parallel_workers = parallel_workers;
    (*pathnode).pathkeys = NIL; /* seqscan has unordered result */

    cost_seqscan(pathnode, root, rel, (*pathnode).param_info);

    pathnode
}

/*
 * create_samplescan_path
 *	  Creates a path node for a sampled table scan.
 */
pub unsafe fn create_samplescan_path(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    required_outer: Relids,
) -> *mut Path {
    let pathnode: *mut Path = makeNode!(Path, T_Path);

    (*pathnode).pathtype = T_SampleScan;
    (*pathnode).parent = rel;
    (*pathnode).pathtarget = (*rel).reltarget;
    (*pathnode).param_info = get_baserel_parampathinfo(root, rel, required_outer);
    (*pathnode).parallel_aware = false;
    (*pathnode).parallel_safe = (*rel).consider_parallel;
    (*pathnode).parallel_workers = 0;
    (*pathnode).pathkeys = NIL; /* samplescan has unordered result */

    cost_samplescan(pathnode, root, rel, (*pathnode).param_info);

    pathnode
}

/*
 * create_index_path
 *	  Creates a path node for an index scan.
 *
 * 'index' is a usable index.
 * 'indexclauses' is a list of IndexClause nodes representing clauses
 *			to be enforced as qual conditions in the scan.
 * 'indexorderbys' is a list of bare expressions (no RestrictInfos)
 *			to be used as index ordering operators in the scan.
 * 'indexorderbycols' is an integer list of index column numbers (zero based)
 *			the ordering operators can be used with.
 * 'pathkeys' describes the ordering of the path.
 * 'indexscandir' is either ForwardScanDirection or BackwardScanDirection.
 * 'indexonly' is true if an index-only scan is wanted.
 * 'required_outer' is the set of outer relids for a parameterized path.
 * 'loop_count' is the number of repetitions of the indexscan to factor into
 *		estimates of caching behavior.
 * 'partial_path' is true if constructing a parallel index scan path.
 *
 * Returns the new path node.
 */
pub unsafe fn create_index_path(
    root: *mut PlannerInfo,
    index: *mut IndexOptInfo,
    indexclauses: *mut List,
    indexorderbys: *mut List,
    indexorderbycols: *mut List,
    pathkeys: *mut List,
    indexscandir: crate::nodes::pathnodes::ScanDirection,
    indexonly: bool,
    required_outer: Relids,
    loop_count: f64,
    partial_path: bool,
) -> *mut IndexPath {
    let pathnode: *mut IndexPath = makeNode!(IndexPath, T_IndexPath);
    let rel: *mut RelOptInfo = (*index).rel;

    (*pathnode).path.pathtype = if indexonly { T_IndexOnlyScan } else { T_IndexScan };
    (*pathnode).path.parent = rel;
    (*pathnode).path.pathtarget = (*rel).reltarget;
    (*pathnode).path.param_info = get_baserel_parampathinfo(root, rel, required_outer);
    (*pathnode).path.parallel_aware = false;
    (*pathnode).path.parallel_safe = (*rel).consider_parallel;
    (*pathnode).path.parallel_workers = 0;
    (*pathnode).path.pathkeys = pathkeys;

    (*pathnode).indexinfo = index;
    (*pathnode).indexclauses = indexclauses;
    (*pathnode).indexorderbys = indexorderbys;
    (*pathnode).indexorderbycols = indexorderbycols;
    (*pathnode).indexscandir = indexscandir;

    cost_index(pathnode, root, loop_count, partial_path);

    pathnode
}

/*
 * create_bitmap_heap_path
 *	  Creates a path node for a bitmap scan.
 *
 * 'bitmapqual' is a tree of IndexPath, BitmapAndPath, and BitmapOrPath nodes.
 * 'required_outer' is the set of outer relids for a parameterized path.
 * 'loop_count' is the number of repetitions of the indexscan to factor into
 *		estimates of caching behavior.
 *
 * loop_count should match the value used when creating the component
 * IndexPaths.
 */
pub unsafe fn create_bitmap_heap_path(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    bitmapqual: *mut Path,
    required_outer: Relids,
    loop_count: f64,
    parallel_degree: c_int,
) -> *mut BitmapHeapPath {
    let pathnode: *mut BitmapHeapPath = makeNode!(BitmapHeapPath, T_BitmapHeapPath);

    (*pathnode).path.pathtype = T_BitmapHeapScan;
    (*pathnode).path.parent = rel;
    (*pathnode).path.pathtarget = (*rel).reltarget;
    (*pathnode).path.param_info = get_baserel_parampathinfo(root, rel, required_outer);
    (*pathnode).path.parallel_aware = parallel_degree > 0;
    (*pathnode).path.parallel_safe = (*rel).consider_parallel;
    (*pathnode).path.parallel_workers = parallel_degree;
    (*pathnode).path.pathkeys = NIL; /* always unordered */

    (*pathnode).bitmapqual = bitmapqual;

    cost_bitmap_heap_scan(
        &mut (*pathnode).path,
        root,
        rel,
        (*pathnode).path.param_info,
        bitmapqual,
        loop_count,
    );

    pathnode
}

/*
 * create_bitmap_and_path
 *	  Creates a path node representing a BitmapAnd.
 */
pub unsafe fn create_bitmap_and_path(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    bitmapquals: *mut List,
) -> *mut BitmapAndPath {
    let pathnode: *mut BitmapAndPath = makeNode!(BitmapAndPath, T_BitmapAndPath);
    let mut required_outer: Relids = core::ptr::null_mut();

    (*pathnode).path.pathtype = T_BitmapAnd;
    (*pathnode).path.parent = rel;
    (*pathnode).path.pathtarget = (*rel).reltarget;

    /*
     * Identify the required outer rels as the union of what the child paths
     * depend on.  (Alternatively, we could insist that the caller pass this
     * in, but it's more convenient and reliable to compute it here.)
     */
    foreach!(lc, bitmapquals, {
        let bitmapqual: *mut Path = lfirst(current_cell!(lc)) as *mut Path;
        required_outer = bms_add_members(required_outer, PATH_REQ_OUTER(bitmapqual));
    });
    (*pathnode).path.param_info = get_baserel_parampathinfo(root, rel, required_outer);

    /*
     * Currently, a BitmapHeapPath, BitmapAndPath, or BitmapOrPath will be
     * parallel-safe if and only if rel->consider_parallel is set.  So, we can
     * set the flag for this path based only on the relation-level flag,
     * without actually iterating over the list of children.
     */
    (*pathnode).path.parallel_aware = false;
    (*pathnode).path.parallel_safe = (*rel).consider_parallel;
    (*pathnode).path.parallel_workers = 0;

    (*pathnode).path.pathkeys = NIL; /* always unordered */

    (*pathnode).bitmapquals = bitmapquals;

    /* this sets bitmapselectivity as well as the regular cost fields: */
    cost_bitmap_and_node(pathnode, root);

    pathnode
}

/*
 * create_bitmap_or_path
 *	  Creates a path node representing a BitmapOr.
 */
pub unsafe fn create_bitmap_or_path(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    bitmapquals: *mut List,
) -> *mut BitmapOrPath {
    let pathnode: *mut BitmapOrPath = makeNode!(BitmapOrPath, T_BitmapOrPath);
    let mut required_outer: Relids = core::ptr::null_mut();

    (*pathnode).path.pathtype = T_BitmapOr;
    (*pathnode).path.parent = rel;
    (*pathnode).path.pathtarget = (*rel).reltarget;

    /*
     * Identify the required outer rels as the union of what the child paths
     * depend on.  (Alternatively, we could insist that the caller pass this
     * in, but it's more convenient and reliable to compute it here.)
     */
    foreach!(lc, bitmapquals, {
        let bitmapqual: *mut Path = lfirst(current_cell!(lc)) as *mut Path;
        required_outer = bms_add_members(required_outer, PATH_REQ_OUTER(bitmapqual));
    });
    (*pathnode).path.param_info = get_baserel_parampathinfo(root, rel, required_outer);

    /*
     * Currently, a BitmapHeapPath, BitmapAndPath, or BitmapOrPath will be
     * parallel-safe if and only if rel->consider_parallel is set.
     */
    (*pathnode).path.parallel_aware = false;
    (*pathnode).path.parallel_safe = (*rel).consider_parallel;
    (*pathnode).path.parallel_workers = 0;

    (*pathnode).path.pathkeys = NIL; /* always unordered */

    (*pathnode).bitmapquals = bitmapquals;

    /* this sets bitmapselectivity as well as the regular cost fields: */
    cost_bitmap_or_node(pathnode, root);

    pathnode
}

/*
 * create_tidscan_path
 *	  Creates a path corresponding to a scan by TID, returning the pathnode.
 */
pub unsafe fn create_tidscan_path(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    tidquals: *mut List,
    required_outer: Relids,
) -> *mut TidPath {
    let pathnode: *mut TidPath = makeNode!(TidPath, T_TidPath);

    (*pathnode).path.pathtype = T_TidScan;
    (*pathnode).path.parent = rel;
    (*pathnode).path.pathtarget = (*rel).reltarget;
    (*pathnode).path.param_info = get_baserel_parampathinfo(root, rel, required_outer);
    (*pathnode).path.parallel_aware = false;
    (*pathnode).path.parallel_safe = (*rel).consider_parallel;
    (*pathnode).path.parallel_workers = 0;
    (*pathnode).path.pathkeys = NIL; /* always unordered */

    (*pathnode).tidquals = tidquals;

    cost_tidscan(
        &mut (*pathnode).path,
        root,
        rel,
        tidquals,
        (*pathnode).path.param_info,
    );

    pathnode
}

/*
 * create_tidrangescan_path
 *	  Creates a path corresponding to a scan by a range of TIDs, returning
 *	  the pathnode.
 */
pub unsafe fn create_tidrangescan_path(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    tidrangequals: *mut List,
    required_outer: Relids,
) -> *mut TidRangePath {
    let pathnode: *mut TidRangePath = makeNode!(TidRangePath, T_TidRangePath);

    (*pathnode).path.pathtype = T_TidRangeScan;
    (*pathnode).path.parent = rel;
    (*pathnode).path.pathtarget = (*rel).reltarget;
    (*pathnode).path.param_info = get_baserel_parampathinfo(root, rel, required_outer);
    (*pathnode).path.parallel_aware = false;
    (*pathnode).path.parallel_safe = (*rel).consider_parallel;
    (*pathnode).path.parallel_workers = 0;
    (*pathnode).path.pathkeys = NIL; /* always unordered */

    (*pathnode).tidrangequals = tidrangequals;

    cost_tidrangescan(
        &mut (*pathnode).path,
        root,
        rel,
        tidrangequals,
        (*pathnode).path.param_info,
    );

    pathnode
}

/*
 * create_append_path
 *	  Creates a path corresponding to an Append plan, returning the
 *	  pathnode.
 *
 * Note that we must handle subpaths = NIL, representing a dummy access path.
 * Also, there are callers that pass root = NULL.
 *
 * 'rows', when passed as a non-negative number, will be used to overwrite the
 * returned path's row estimate.  Otherwise, the row estimate is calculated
 * by totalling the row estimates from the 'subpaths' list.
 */
pub unsafe fn create_append_path(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    subpaths: *mut List,
    partial_subpaths: *mut List,
    pathkeys: *mut List,
    required_outer: Relids,
    parallel_workers: c_int,
    parallel_aware: bool,
    rows: f64,
) -> *mut AppendPath {
    let pathnode: *mut AppendPath = makeNode!(AppendPath, T_AppendPath);

    Assert!(!parallel_aware || parallel_workers > 0);

    (*pathnode).path.pathtype = T_Append;
    (*pathnode).path.parent = rel;
    (*pathnode).path.pathtarget = (*rel).reltarget;

    /*
     * If this is for a baserel (not a join or non-leaf partition), we prefer
     * to apply get_baserel_parampathinfo to construct a full ParamPathInfo
     * for the path.
     */
    if (*rel).reloptkind == RELOPT_BASEREL && !root.is_null() && subpaths != NIL {
        (*pathnode).path.param_info =
            get_baserel_parampathinfo(root, rel, required_outer);
    } else {
        (*pathnode).path.param_info =
            get_appendrel_parampathinfo(rel, required_outer);
    }

    (*pathnode).path.parallel_aware = parallel_aware;
    (*pathnode).path.parallel_safe = (*rel).consider_parallel;
    (*pathnode).path.parallel_workers = parallel_workers;
    (*pathnode).path.pathkeys = pathkeys;

    /*
     * For parallel append, non-partial paths are sorted by descending total
     * costs. That way, the total time to finish all non-partial paths is
     * minimized.  Also, the partial paths are sorted by descending startup
     * costs.
     */
    if (*pathnode).path.parallel_aware {
        /*
         * We mustn't fiddle with the order of subpaths when the Append has
         * pathkeys.  The order they're listed in is critical to keeping the
         * pathkeys valid.
         */
        Assert!(pathkeys == NIL);

        list_sort(subpaths, append_total_cost_compare);
        list_sort(partial_subpaths, append_startup_cost_compare);
    }
    (*pathnode).first_partial_path = list_length(subpaths);
    (*pathnode).subpaths = list_concat(subpaths, partial_subpaths);

    /*
     * Apply query-wide LIMIT if known and path is for sole base relation.
     * (Handling this at this low level is a bit klugy.)
     */
    if !root.is_null() && bms_equal((*rel).relids, (*root).all_query_rels) {
        (*pathnode).limit_tuples = (*root).limit_tuples;
    } else {
        (*pathnode).limit_tuples = -1.0;
    }

    foreach!(l, (*pathnode).subpaths, {
        let subpath: *mut Path = lfirst(current_cell!(l)) as *mut Path;

        (*pathnode).path.parallel_safe =
            (*pathnode).path.parallel_safe && (*subpath).parallel_safe;

        /* All child paths must have same parameterization */
        Assert!(bms_equal(PATH_REQ_OUTER(subpath), required_outer));
    });

    Assert!(!parallel_aware || (*pathnode).path.parallel_safe);

    /*
     * If there's exactly one child path then the output of the Append is
     * necessarily ordered the same as the child's, so we can inherit the
     * child's pathkeys if any.
     */
    if list_length((*pathnode).subpaths) == 1 {
        let child: *mut Path = linitial((*pathnode).subpaths) as *mut Path;

        if (*child).parallel_aware == parallel_aware {
            (*pathnode).path.rows = (*child).rows;
            (*pathnode).path.startup_cost = (*child).startup_cost;
            (*pathnode).path.total_cost = (*child).total_cost;
        } else {
            cost_append(pathnode);
        }
        /* Must do this last, else cost_append complains */
        (*pathnode).path.pathkeys = (*child).pathkeys;
    } else {
        cost_append(pathnode);
    }

    /* If the caller provided a row estimate, override the computed value. */
    if rows >= 0.0 {
        (*pathnode).path.rows = rows;
    }

    pathnode
}

/*
 * append_total_cost_compare
 *	  list_sort comparator for sorting append child paths
 *	  by total_cost descending
 *
 * For equal total costs, we fall back to comparing startup costs; if those
 * are equal too, break ties using bms_compare on the paths' relids.
 * (This is to avoid getting unpredictable results from list_sort.)
 */
unsafe fn append_total_cost_compare(a: *const ListCell, b: *const ListCell) -> c_int {
    let path1: *mut Path = lfirst(a) as *mut Path;
    let path2: *mut Path = lfirst(b) as *mut Path;
    let cmp: c_int;

    let cmp = compare_path_costs(path1, path2, TOTAL_COST);
    if cmp != 0 {
        return -cmp;
    }
    bms_compare((*(*path1).parent).relids, (*(*path2).parent).relids)
}

/*
 * append_startup_cost_compare
 *	  list_sort comparator for sorting append child paths
 *	  by startup_cost descending
 *
 * For equal startup costs, we fall back to comparing total costs; if those
 * are equal too, break ties using bms_compare on the paths' relids.
 * (This is to avoid getting unpredictable results from list_sort.)
 */
unsafe fn append_startup_cost_compare(a: *const ListCell, b: *const ListCell) -> c_int {
    let path1: *mut Path = lfirst(a) as *mut Path;
    let path2: *mut Path = lfirst(b) as *mut Path;
    let cmp: c_int;

    let cmp = compare_path_costs(path1, path2, STARTUP_COST);
    if cmp != 0 {
        return -cmp;
    }
    bms_compare((*(*path1).parent).relids, (*(*path2).parent).relids)
}

/*
 * create_merge_append_path
 *	  Creates a path corresponding to a MergeAppend plan, returning the
 *	  pathnode.
 */
pub unsafe fn create_merge_append_path(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    subpaths: *mut List,
    pathkeys: *mut List,
    required_outer: Relids,
) -> *mut MergeAppendPath {
    let pathnode: *mut MergeAppendPath = makeNode!(MergeAppendPath, T_MergeAppendPath);
    let mut input_disabled_nodes: c_int;
    let mut input_startup_cost: Cost;
    let mut input_total_cost: Cost;

    /*
     * We don't currently support parameterized MergeAppend paths, as
     * explained in the comments for generate_orderedappend_paths.
     */
    Assert!(bms_is_empty((*rel).lateral_relids) && bms_is_empty(required_outer));

    (*pathnode).path.pathtype = T_MergeAppend;
    (*pathnode).path.parent = rel;
    (*pathnode).path.pathtarget = (*rel).reltarget;
    (*pathnode).path.param_info = core::ptr::null_mut();
    (*pathnode).path.parallel_aware = false;
    (*pathnode).path.parallel_safe = (*rel).consider_parallel;
    (*pathnode).path.parallel_workers = 0;
    (*pathnode).path.pathkeys = pathkeys;
    (*pathnode).subpaths = subpaths;

    /*
     * Apply query-wide LIMIT if known and path is for sole base relation.
     * (Handling this at this low level is a bit klugy.)
     */
    if bms_equal((*rel).relids, (*root).all_query_rels) {
        (*pathnode).limit_tuples = (*root).limit_tuples;
    } else {
        (*pathnode).limit_tuples = -1.0;
    }

    /*
     * Add up the sizes and costs of the input paths.
     */
    (*pathnode).path.rows = 0.0;
    input_disabled_nodes = 0;
    input_startup_cost = 0.0;
    input_total_cost = 0.0;
    foreach!(l, subpaths, {
        let subpath: *mut Path = lfirst(current_cell!(l)) as *mut Path;

        /* All child paths should be unparameterized */
        Assert!(bms_is_empty(PATH_REQ_OUTER(subpath)));

        (*pathnode).path.rows += (*subpath).rows;
        (*pathnode).path.parallel_safe =
            (*pathnode).path.parallel_safe && (*subpath).parallel_safe;

        if pathkeys_contained_in(pathkeys, (*subpath).pathkeys) {
            /* Subpath is adequately ordered, we won't need to sort it */
            input_disabled_nodes += (*subpath).disabled_nodes;
            input_startup_cost += (*subpath).startup_cost;
            input_total_cost += (*subpath).total_cost;
        } else {
            /* We'll need to insert a Sort node, so include cost for that */
            let mut sort_path: Path = core::mem::zeroed(); /* dummy for result of cost_sort */

            cost_sort(
                &mut sort_path,
                root,
                pathkeys,
                (*subpath).disabled_nodes,
                (*subpath).total_cost,
                (*subpath).rows,
                (*(*subpath).pathtarget).width,
                0.0,
                work_mem,
                (*pathnode).limit_tuples,
            );
            input_disabled_nodes += sort_path.disabled_nodes;
            input_startup_cost += sort_path.startup_cost;
            input_total_cost += sort_path.total_cost;
        }
    });

    /*
     * Now we can compute total costs of the MergeAppend.  If there's exactly
     * one child path and its parallel awareness matches that of the
     * MergeAppend, then the MergeAppend is a no-op and will be discarded
     * later (in setrefs.c); otherwise we do the normal cost calculation.
     */
    if list_length(subpaths) == 1
        && (*(linitial(subpaths) as *mut Path)).parallel_aware == (*pathnode).path.parallel_aware
    {
        (*pathnode).path.disabled_nodes = input_disabled_nodes;
        (*pathnode).path.startup_cost = input_startup_cost;
        (*pathnode).path.total_cost = input_total_cost;
    } else {
        cost_merge_append(
            &mut (*pathnode).path,
            root,
            pathkeys,
            list_length(subpaths),
            input_disabled_nodes,
            input_startup_cost,
            input_total_cost,
            (*pathnode).path.rows,
        );
    }

    pathnode
}

/*
 * create_group_result_path
 *	  Creates a path representing a Result-and-nothing-else plan.
 *
 * This is only used for degenerate grouping cases, in which we know we
 * need to produce one result row, possibly filtered by a HAVING qual.
 */
pub unsafe fn create_group_result_path(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    target: *mut PathTarget,
    havingqual: *mut List,
) -> *mut GroupResultPath {
    let pathnode: *mut GroupResultPath = makeNode!(GroupResultPath, T_GroupResultPath);

    (*pathnode).path.pathtype = T_Result;
    (*pathnode).path.parent = rel;
    (*pathnode).path.pathtarget = target;
    (*pathnode).path.param_info = core::ptr::null_mut(); /* there are no other rels... */
    (*pathnode).path.parallel_aware = false;
    (*pathnode).path.parallel_safe = (*rel).consider_parallel;
    (*pathnode).path.parallel_workers = 0;
    (*pathnode).path.pathkeys = NIL;
    (*pathnode).quals = havingqual;

    /*
     * We can't quite use cost_resultscan() because the quals we want to
     * account for are not baserestrict quals of the rel.  Might as well just
     * hack it here.
     */
    (*pathnode).path.rows = 1.0;
    (*pathnode).path.startup_cost = (*target).cost.startup;
    (*pathnode).path.total_cost =
        (*target).cost.startup + cpu_tuple_cost + (*target).cost.per_tuple;

    /*
     * Add cost of qual, if any --- but we ignore its selectivity, since our
     * rowcount estimate should be 1 no matter what the qual is.
     */
    if !havingqual.is_null() {
        let mut qual_cost: QualCost = core::mem::zeroed();

        cost_qual_eval(&mut qual_cost, havingqual, root);
        /* havingqual is evaluated once at startup */
        (*pathnode).path.startup_cost += qual_cost.startup + qual_cost.per_tuple;
        (*pathnode).path.total_cost += qual_cost.startup + qual_cost.per_tuple;
    }

    pathnode
}

/*
 * create_material_path
 *	  Creates a path corresponding to a Material plan, returning the
 *	  pathnode.
 */
pub unsafe fn create_material_path(rel: *mut RelOptInfo, subpath: *mut Path) -> *mut MaterialPath {
    let pathnode: *mut MaterialPath = makeNode!(MaterialPath, T_MaterialPath);

    Assert!((*subpath).parent == rel);

    (*pathnode).path.pathtype = T_Material;
    (*pathnode).path.parent = rel;
    (*pathnode).path.pathtarget = (*rel).reltarget;
    (*pathnode).path.param_info = (*subpath).param_info;
    (*pathnode).path.parallel_aware = false;
    (*pathnode).path.parallel_safe =
        (*rel).consider_parallel && (*subpath).parallel_safe;
    (*pathnode).path.parallel_workers = (*subpath).parallel_workers;
    (*pathnode).path.pathkeys = (*subpath).pathkeys;

    (*pathnode).subpath = subpath;

    cost_material(
        &mut (*pathnode).path,
        (*subpath).disabled_nodes,
        (*subpath).startup_cost,
        (*subpath).total_cost,
        (*subpath).rows,
        (*(*subpath).pathtarget).width,
    );

    pathnode
}

/*
 * create_memoize_path
 *	  Creates a path corresponding to a Memoize plan, returning the pathnode.
 */
pub unsafe fn create_memoize_path(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    subpath: *mut Path,
    param_exprs: *mut List,
    hash_operators: *mut List,
    singlerow: bool,
    binary_mode: bool,
    calls: f64,
) -> *mut MemoizePath {
    let pathnode: *mut MemoizePath = makeNode!(MemoizePath, T_MemoizePath);

    Assert!((*subpath).parent == rel);

    (*pathnode).path.pathtype = T_Memoize;
    (*pathnode).path.parent = rel;
    (*pathnode).path.pathtarget = (*rel).reltarget;
    (*pathnode).path.param_info = (*subpath).param_info;
    (*pathnode).path.parallel_aware = false;
    (*pathnode).path.parallel_safe =
        (*rel).consider_parallel && (*subpath).parallel_safe;
    (*pathnode).path.parallel_workers = (*subpath).parallel_workers;
    (*pathnode).path.pathkeys = (*subpath).pathkeys;

    (*pathnode).subpath = subpath;
    (*pathnode).hash_operators = hash_operators;
    (*pathnode).param_exprs = param_exprs;
    (*pathnode).singlerow = singlerow;
    (*pathnode).binary_mode = binary_mode;
    (*pathnode).calls = clamp_row_est(calls);

    /*
     * For now we set est_entries to 0.  cost_memoize_rescan() does all the
     * hard work to determine how many cache entries there are likely to be,
     * so it seems best to leave it up to that function to fill this field in.
     * If left at 0, the executor will make a guess at a good value.
     */
    (*pathnode).est_entries = 0;

    /* we should not generate this path type when enable_memoize=false */
    Assert!(enable_memoize);
    (*pathnode).path.disabled_nodes = (*subpath).disabled_nodes;

    /*
     * Add a small additional charge for caching the first entry.  All the
     * harder calculations for rescans are performed in cost_memoize_rescan().
     */
    (*pathnode).path.startup_cost = (*subpath).startup_cost + cpu_tuple_cost;
    (*pathnode).path.total_cost = (*subpath).total_cost + cpu_tuple_cost;
    (*pathnode).path.rows = (*subpath).rows;

    pathnode
}

/*
 * create_unique_path
 *	  Creates a path representing elimination of distinct rows from the
 *	  input data.  Distinct-ness is defined according to the needs of the
 *	  semijoin represented by sjinfo.  If it is not possible to identify
 *	  how to make the data unique, NULL is returned.
 *
 * If used at all, this is likely to be called repeatedly on the same rel;
 * and the input subpath should always be the same (the cheapest_total path
 * for the rel).  So we cache the result.
 */
pub unsafe fn create_unique_path(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    subpath: *mut Path,
    sjinfo: *mut SpecialJoinInfo,
) -> *mut UniquePath {
    let mut uniq_exprs: *mut List;
    let mut in_operators: *mut List;
    let pathnode: *mut UniquePath;
    let mut sort_path: Path = core::mem::zeroed(); /* dummy for result of cost_sort */
    let mut agg_path: Path = core::mem::zeroed();  /* dummy for result of cost_agg */
    let oldcontext: crate::utils::palloc::MemoryContext;
    let numCols: c_int;

    /* Caller made a mistake if subpath isn't cheapest_total ... */
    Assert!(subpath == (*rel).cheapest_total_path);
    Assert!((*subpath).parent == rel);
    /* ... or if SpecialJoinInfo is the wrong one */
    Assert!((*sjinfo).jointype == JOIN_SEMI);
    Assert!(bms_equal((*rel).relids, (*sjinfo).syn_righthand));

    /* If result already cached, return it */
    if !(*rel).cheapest_unique_path.is_null() {
        return (*rel).cheapest_unique_path as *mut UniquePath;
    }

    /* If it's not possible to unique-ify, return NULL */
    if !((*sjinfo).semi_can_btree || (*sjinfo).semi_can_hash) {
        return core::ptr::null_mut();
    }

    /*
     * Punt if this is a child relation and we failed to build a unique-ified
     * path for its parent.
     */
    if IS_OTHER_REL(rel) && (*(*rel).top_parent).cheapest_unique_path.is_null() {
        return core::ptr::null_mut();
    }

    /*
     * When called during GEQO join planning, we are in a short-lived memory
     * context.  We must make sure that the path and any subsidiary data
     * structures created for a baserel survive the GEQO cycle.
     */
    oldcontext = MemoryContextSwitchTo(GetMemoryChunkContext(rel as *mut c_void));

    /*
     * First, identify the columns/expressions to be made unique along with
     * the associated equality operators.
     *
     * For a child rel, we can construct these lists from those of its parent.
     */
    if IS_OTHER_REL(rel) {
        let parent_path: *mut UniquePath =
            (*(*rel).top_parent).cheapest_unique_path as *mut UniquePath;

        Assert!(!parent_path.is_null() && IsA!(parent_path, T_UniquePath));
        uniq_exprs = adjust_appendrel_attrs_multilevel(
            root,
            (*parent_path).uniq_exprs as *mut Node,
            rel,
            (*rel).top_parent,
        ) as *mut List;
        in_operators = copyObject((*parent_path).in_operators as *const c_void) as *mut List;
    } else {
        let mut newtlist: *mut List = NIL;
        let mut sortList: *mut List = NIL;

        uniq_exprs = NIL;
        in_operators = NIL;

        use crate::{forboth};
        use crate::nodes::pg_list::{lfirst_oid as _lfirst_oid};

        forboth!(lc1, (*sjinfo).semi_rhs_exprs, lc2, (*sjinfo).semi_operators, {
            let uniqexpr: *mut Expr = lfirst(lc1) as *mut Expr;
            let in_oper: Oid = lfirst_oid(lc2);
            let sortop: Oid;

            /*
             * Try to build an ORDER BY list to sort the input compatibly.
             */
            let sortop = get_ordering_op_for_equality_op(in_oper, false);
            if OidIsValid(sortop) {
                let eqop: Oid;
                let tle: *mut TargetEntry;
                let sortcl: *mut SortGroupClause;
                let sortPathkeys: *mut List;

                /*
                 * The Unique node will need equality operators.  Normally
                 * these are the same as the IN clause operators, but if those
                 * are cross-type operators then the equality operators are
                 * the ones for the IN clause operators' RHS datatype.
                 */
                let eqop = get_equality_op_for_ordering_op(sortop, core::ptr::null_mut());
                if !OidIsValid(eqop) {
                    /* shouldn't happen */
                    ereport!(
                        ERROR,
                        errmsg!(
                            "could not find equality operator for ordering operator {}",
                            sortop
                        )
                    );
                }

                let tle = makeTargetEntry(
                    uniqexpr as *mut Expr,
                    (list_length(newtlist) + 1) as i16,
                    core::ptr::null_mut(),
                    false,
                );
                newtlist = lappend(newtlist, tle as *mut c_void);

                let sortcl: *mut SortGroupClause =
                    makeNode!(SortGroupClause, T_SortGroupClause);
                (*sortcl).tleSortGroupRef = assignSortGroupRef(tle, newtlist);
                (*sortcl).eqop = eqop;
                (*sortcl).sortop = sortop;
                (*sortcl).reverse_sort = false;
                (*sortcl).nulls_first = false;
                (*sortcl).hashable = false; /* no need to make this accurate */
                sortList = lappend(sortList, sortcl as *mut c_void);

                /*
                 * At each step, convert the SortGroupClause list to pathkey
                 * form.
                 */
                let sortPathkeys =
                    make_pathkeys_for_sortclauses(root, sortList, newtlist);
                if list_length(sortPathkeys) != list_length(sortList) {
                    /* Drop the redundant SortGroupClause */
                    sortList = list_delete_last(sortList);
                    Assert!(list_length(sortPathkeys) == list_length(sortList));
                    /* Undo tlist addition too */
                    newtlist = list_delete_last(newtlist);
                    /* Don't need this column */
                    continue;
                }
            } else if (*sjinfo).semi_can_btree {
                /* shouldn't happen */
                ereport!(
                    ERROR,
                    errmsg!(
                        "could not find ordering operator for equality operator {}",
                        in_oper
                    )
                );
            }

            /*
             * We need to include this column in the output list.
             *
             * Under GEQO and when planning child joins, the sjinfo might be
             * short-lived, so we'd better make copies of data structures we
             * extract from it.
             */
            uniq_exprs = lappend(
                uniq_exprs,
                copyObject(uniqexpr as *const c_void) as *mut c_void,
            );
            in_operators = lappend_oid(in_operators, in_oper);
        });

        /*
         * It can happen that all the RHS columns are equated to constants.
         * We'd have to do something special to unique-ify in that case, and
         * it's such an unlikely-in-the-real-world case that it's not worth
         * the effort.  So just punt if we found no columns to unique-ify.
         */
        if uniq_exprs == NIL {
            MemoryContextSwitchTo(oldcontext);
            return core::ptr::null_mut();
        }
    }

    /* OK, build the path node */
    let pathnode: *mut UniquePath = makeNode!(UniquePath, T_UniquePath);

    (*pathnode).path.pathtype = T_Unique;
    (*pathnode).path.parent = rel;
    (*pathnode).path.pathtarget = (*rel).reltarget;
    (*pathnode).path.param_info = (*subpath).param_info;
    (*pathnode).path.parallel_aware = false;
    (*pathnode).path.parallel_safe =
        (*rel).consider_parallel && (*subpath).parallel_safe;
    (*pathnode).path.parallel_workers = (*subpath).parallel_workers;

    /*
     * Assume the output is unsorted, since we don't necessarily have pathkeys
     * to represent it.  (This might get overridden below.)
     */
    (*pathnode).path.pathkeys = NIL;

    (*pathnode).subpath = subpath;
    (*pathnode).in_operators = in_operators;
    (*pathnode).uniq_exprs = uniq_exprs;

    /*
     * If the input is a relation and it has a unique index that proves the
     * semi_rhs_exprs are unique, then we don't need to do anything.
     */
    if (*rel).rtekind == RTE_RELATION
        && (*sjinfo).semi_can_btree
        && relation_has_unique_index_for(
            root,
            rel,
            NIL,
            uniq_exprs,
            in_operators,
        )
    {
        (*pathnode).umethod = UNIQUE_PATH_NOOP;
        (*pathnode).path.rows = (*rel).rows;
        (*pathnode).path.disabled_nodes = (*subpath).disabled_nodes;
        (*pathnode).path.startup_cost = (*subpath).startup_cost;
        (*pathnode).path.total_cost = (*subpath).total_cost;
        (*pathnode).path.pathkeys = (*subpath).pathkeys;

        (*rel).cheapest_unique_path = pathnode as *mut Path;

        MemoryContextSwitchTo(oldcontext);

        return pathnode;
    }

    /*
     * If the input is a subquery whose output must be unique already, then we
     * don't need to do anything.
     */
    if (*rel).rtekind == RTE_SUBQUERY {
        let rte: *mut RangeTblEntry = planner_rt_fetch((*rel).relid, root);

        if query_supports_distinctness((*rte).subquery) {
            let sub_tlist_colnos: *mut List;

            let sub_tlist_colnos = translate_sub_tlist(uniq_exprs, (*rel).relid as c_int);

            if sub_tlist_colnos != NIL
                && query_is_distinct_for((*rte).subquery, sub_tlist_colnos, in_operators)
            {
                (*pathnode).umethod = UNIQUE_PATH_NOOP;
                (*pathnode).path.rows = (*rel).rows;
                (*pathnode).path.disabled_nodes = (*subpath).disabled_nodes;
                (*pathnode).path.startup_cost = (*subpath).startup_cost;
                (*pathnode).path.total_cost = (*subpath).total_cost;
                (*pathnode).path.pathkeys = (*subpath).pathkeys;

                (*rel).cheapest_unique_path = pathnode as *mut Path;

                MemoryContextSwitchTo(oldcontext);

                return pathnode;
            }
        }
    }

    /* Estimate number of output rows */
    (*pathnode).path.rows =
        estimate_num_groups(root, uniq_exprs, (*rel).rows, core::ptr::null_mut(), core::ptr::null_mut());
    numCols = list_length(uniq_exprs);

    if (*sjinfo).semi_can_btree {
        /*
         * Estimate cost for sort+unique implementation
         */
        cost_sort(
            &mut sort_path,
            root,
            NIL,
            (*subpath).disabled_nodes,
            (*subpath).total_cost,
            (*rel).rows,
            (*(*subpath).pathtarget).width,
            0.0,
            work_mem,
            -1.0,
        );

        /*
         * Charge one cpu_operator_cost per comparison per input tuple. We
         * assume all columns get compared at most of the tuples.
         */
        sort_path.total_cost += cpu_operator_cost * (*rel).rows * numCols as f64;
    }

    if (*sjinfo).semi_can_hash {
        /*
         * Estimate the overhead per hashtable entry at 64 bytes (same as in
         * planner.c).
         */
        let hashentrysize: c_int = (*(*subpath).pathtarget).width + 64;

        if hashentrysize as f64 * (*pathnode).path.rows > get_hash_memory_limit() as f64 {
            /*
             * We should not try to hash.  Hack the SpecialJoinInfo to
             * remember this, in case we come through here again.
             */
            (*sjinfo).semi_can_hash = false;
        } else {
            cost_agg(
                &mut agg_path,
                root,
                AGG_HASHED,
                core::ptr::null(),
                numCols,
                (*pathnode).path.rows,
                NIL,
                (*subpath).disabled_nodes,
                (*subpath).startup_cost,
                (*subpath).total_cost,
                (*rel).rows,
                (*(*subpath).pathtarget).width as f64,
            );
        }
    }

    if (*sjinfo).semi_can_btree && (*sjinfo).semi_can_hash {
        if agg_path.disabled_nodes < sort_path.disabled_nodes
            || (agg_path.disabled_nodes == sort_path.disabled_nodes
                && agg_path.total_cost < sort_path.total_cost)
        {
            (*pathnode).umethod = UNIQUE_PATH_HASH;
        } else {
            (*pathnode).umethod = UNIQUE_PATH_SORT;
        }
    } else if (*sjinfo).semi_can_btree {
        (*pathnode).umethod = UNIQUE_PATH_SORT;
    } else if (*sjinfo).semi_can_hash {
        (*pathnode).umethod = UNIQUE_PATH_HASH;
    } else {
        /* we can get here only if we abandoned hashing above */
        MemoryContextSwitchTo(oldcontext);
        return core::ptr::null_mut();
    }

    if (*pathnode).umethod == UNIQUE_PATH_HASH {
        (*pathnode).path.disabled_nodes = agg_path.disabled_nodes;
        (*pathnode).path.startup_cost = agg_path.startup_cost;
        (*pathnode).path.total_cost = agg_path.total_cost;
    } else {
        (*pathnode).path.disabled_nodes = sort_path.disabled_nodes;
        (*pathnode).path.startup_cost = sort_path.startup_cost;
        (*pathnode).path.total_cost = sort_path.total_cost;
    }

    (*rel).cheapest_unique_path = pathnode as *mut Path;

    MemoryContextSwitchTo(oldcontext);

    pathnode
}

/*
 * translate_sub_tlist - get subquery column numbers represented by tlist
 *
 * The given targetlist usually contains only Vars referencing the given relid.
 * Extract their varattnos (ie, the column numbers of the subquery) and return
 * as an integer List.
 *
 * If any of the tlist items is not a simple Var, we cannot determine whether
 * the subquery's uniqueness condition (if any) matches ours, so punt and
 * return NIL.
 */
unsafe fn translate_sub_tlist(tlist: *mut List, relid: c_int) -> *mut List {
    let mut result: *mut List = NIL;

    foreach!(l, tlist, {
        let var: *mut Var = lfirst(current_cell!(l)) as *mut Var;

        if var.is_null() || !IsA!(var, T_Var) || (*var).varno != relid as Index as i32 {
            return NIL; /* punt */
        }

        result = lappend_int(result, (*var).varattno as c_int);
    });
    result
}

/*
 * create_gather_merge_path
 *
 *	  Creates a path corresponding to a gather merge scan, returning
 *	  the pathnode.
 */
pub unsafe fn create_gather_merge_path(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    subpath: *mut Path,
    target: *mut PathTarget,
    pathkeys: *mut List,
    required_outer: Relids,
    rows: *mut f64,
) -> *mut GatherMergePath {
    let pathnode: *mut GatherMergePath = makeNode!(GatherMergePath, T_GatherMergePath);
    let mut input_disabled_nodes: c_int = 0;
    let mut input_startup_cost: Cost = 0.0;
    let mut input_total_cost: Cost = 0.0;

    Assert!((*subpath).parallel_safe);
    Assert!(!pathkeys.is_null());

    /*
     * The subpath should guarantee that it is adequately ordered either by
     * adding an explicit sort node or by using presorted input.
     */
    if !pathkeys_contained_in(pathkeys, (*subpath).pathkeys) {
        ereport!(ERROR, errmsg!("gather merge input not sufficiently sorted"));
    }

    (*pathnode).path.pathtype = T_GatherMerge;
    (*pathnode).path.parent = rel;
    (*pathnode).path.param_info = get_baserel_parampathinfo(root, rel, required_outer);
    (*pathnode).path.parallel_aware = false;

    (*pathnode).subpath = subpath;
    (*pathnode).num_workers = (*subpath).parallel_workers;
    (*pathnode).path.pathkeys = pathkeys;
    (*pathnode).path.pathtarget = if !target.is_null() { target } else { (*rel).reltarget };

    input_disabled_nodes += (*subpath).disabled_nodes;
    input_startup_cost += (*subpath).startup_cost;
    input_total_cost += (*subpath).total_cost;

    cost_gather_merge(
        pathnode,
        root,
        rel,
        (*pathnode).path.param_info,
        input_disabled_nodes,
        input_startup_cost,
        input_total_cost,
        rows,
    );

    pathnode
}

/*
 * create_gather_path
 *	  Creates a path corresponding to a gather scan, returning the
 *	  pathnode.
 *
 * 'rows' may optionally be set to override row estimates from other sources.
 */
pub unsafe fn create_gather_path(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    subpath: *mut Path,
    target: *mut PathTarget,
    required_outer: Relids,
    rows: *mut f64,
) -> *mut GatherPath {
    let pathnode: *mut GatherPath = makeNode!(GatherPath, T_GatherPath);

    Assert!((*subpath).parallel_safe);

    (*pathnode).path.pathtype = T_Gather;
    (*pathnode).path.parent = rel;
    (*pathnode).path.pathtarget = target;
    (*pathnode).path.param_info = get_baserel_parampathinfo(root, rel, required_outer);
    (*pathnode).path.parallel_aware = false;
    (*pathnode).path.parallel_safe = false;
    (*pathnode).path.parallel_workers = 0;
    (*pathnode).path.pathkeys = NIL; /* Gather has unordered result */

    (*pathnode).subpath = subpath;
    (*pathnode).num_workers = (*subpath).parallel_workers;
    (*pathnode).single_copy = false;

    if (*pathnode).num_workers == 0 {
        (*pathnode).path.pathkeys = (*subpath).pathkeys;
        (*pathnode).num_workers = 1;
        (*pathnode).single_copy = true;
    }

    cost_gather(pathnode, root, rel, (*pathnode).path.param_info, rows);

    pathnode
}

/*
 * create_subqueryscan_path
 *	  Creates a path corresponding to a scan of a subquery,
 *	  returning the pathnode.
 *
 * Caller must pass trivial_pathtarget = true if it believes rel->reltarget to
 * be trivial, ie just a fetch of all the subquery output columns in order.
 */
pub unsafe fn create_subqueryscan_path(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    subpath: *mut Path,
    trivial_pathtarget: bool,
    pathkeys: *mut List,
    required_outer: Relids,
) -> *mut SubqueryScanPath {
    let pathnode: *mut SubqueryScanPath = makeNode!(SubqueryScanPath, T_SubqueryScanPath);

    (*pathnode).path.pathtype = T_SubqueryScan;
    (*pathnode).path.parent = rel;
    (*pathnode).path.pathtarget = (*rel).reltarget;
    (*pathnode).path.param_info = get_baserel_parampathinfo(root, rel, required_outer);
    (*pathnode).path.parallel_aware = false;
    (*pathnode).path.parallel_safe =
        (*rel).consider_parallel && (*subpath).parallel_safe;
    (*pathnode).path.parallel_workers = (*subpath).parallel_workers;
    (*pathnode).path.pathkeys = pathkeys;
    (*pathnode).subpath = subpath;

    cost_subqueryscan(
        pathnode,
        root,
        rel,
        (*pathnode).path.param_info,
        trivial_pathtarget,
    );

    pathnode
}

/*
 * create_functionscan_path
 *	  Creates a path corresponding to a sequential scan of a function,
 *	  returning the pathnode.
 */
pub unsafe fn create_functionscan_path(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    pathkeys: *mut List,
    required_outer: Relids,
) -> *mut Path {
    let pathnode: *mut Path = makeNode!(Path, T_Path);

    (*pathnode).pathtype = T_FunctionScan;
    (*pathnode).parent = rel;
    (*pathnode).pathtarget = (*rel).reltarget;
    (*pathnode).param_info = get_baserel_parampathinfo(root, rel, required_outer);
    (*pathnode).parallel_aware = false;
    (*pathnode).parallel_safe = (*rel).consider_parallel;
    (*pathnode).parallel_workers = 0;
    (*pathnode).pathkeys = pathkeys;

    cost_functionscan(pathnode, root, rel, (*pathnode).param_info);

    pathnode
}

/*
 * create_tablefuncscan_path
 *	  Creates a path corresponding to a sequential scan of a table function,
 *	  returning the pathnode.
 */
pub unsafe fn create_tablefuncscan_path(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    required_outer: Relids,
) -> *mut Path {
    let pathnode: *mut Path = makeNode!(Path, T_Path);

    (*pathnode).pathtype = T_TableFuncScan;
    (*pathnode).parent = rel;
    (*pathnode).pathtarget = (*rel).reltarget;
    (*pathnode).param_info = get_baserel_parampathinfo(root, rel, required_outer);
    (*pathnode).parallel_aware = false;
    (*pathnode).parallel_safe = (*rel).consider_parallel;
    (*pathnode).parallel_workers = 0;
    (*pathnode).pathkeys = NIL; /* result is always unordered */

    cost_tablefuncscan(pathnode, root, rel, (*pathnode).param_info);

    pathnode
}

/*
 * create_valuesscan_path
 *	  Creates a path corresponding to a scan of a VALUES list,
 *	  returning the pathnode.
 */
pub unsafe fn create_valuesscan_path(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    required_outer: Relids,
) -> *mut Path {
    let pathnode: *mut Path = makeNode!(Path, T_Path);

    (*pathnode).pathtype = T_ValuesScan;
    (*pathnode).parent = rel;
    (*pathnode).pathtarget = (*rel).reltarget;
    (*pathnode).param_info = get_baserel_parampathinfo(root, rel, required_outer);
    (*pathnode).parallel_aware = false;
    (*pathnode).parallel_safe = (*rel).consider_parallel;
    (*pathnode).parallel_workers = 0;
    (*pathnode).pathkeys = NIL; /* result is always unordered */

    cost_valuesscan(pathnode, root, rel, (*pathnode).param_info);

    pathnode
}

/*
 * create_ctescan_path
 *	  Creates a path corresponding to a scan of a non-self-reference CTE,
 *	  returning the pathnode.
 */
pub unsafe fn create_ctescan_path(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    pathkeys: *mut List,
    required_outer: Relids,
) -> *mut Path {
    let pathnode: *mut Path = makeNode!(Path, T_Path);

    (*pathnode).pathtype = T_CteScan;
    (*pathnode).parent = rel;
    (*pathnode).pathtarget = (*rel).reltarget;
    (*pathnode).param_info = get_baserel_parampathinfo(root, rel, required_outer);
    (*pathnode).parallel_aware = false;
    (*pathnode).parallel_safe = (*rel).consider_parallel;
    (*pathnode).parallel_workers = 0;
    (*pathnode).pathkeys = pathkeys;

    cost_ctescan(pathnode, root, rel, (*pathnode).param_info);

    pathnode
}

/*
 * create_namedtuplestorescan_path
 *	  Creates a path corresponding to a scan of a named tuplestore, returning
 *	  the pathnode.
 */
pub unsafe fn create_namedtuplestorescan_path(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    required_outer: Relids,
) -> *mut Path {
    let pathnode: *mut Path = makeNode!(Path, T_Path);

    (*pathnode).pathtype = T_NamedTuplestoreScan;
    (*pathnode).parent = rel;
    (*pathnode).pathtarget = (*rel).reltarget;
    (*pathnode).param_info = get_baserel_parampathinfo(root, rel, required_outer);
    (*pathnode).parallel_aware = false;
    (*pathnode).parallel_safe = (*rel).consider_parallel;
    (*pathnode).parallel_workers = 0;
    (*pathnode).pathkeys = NIL; /* result is always unordered */

    cost_namedtuplestorescan(pathnode, root, rel, (*pathnode).param_info);

    pathnode
}

/*
 * create_resultscan_path
 *	  Creates a path corresponding to a scan of an RTE_RESULT relation,
 *	  returning the pathnode.
 */
pub unsafe fn create_resultscan_path(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    required_outer: Relids,
) -> *mut Path {
    let pathnode: *mut Path = makeNode!(Path, T_Path);

    (*pathnode).pathtype = T_Result;
    (*pathnode).parent = rel;
    (*pathnode).pathtarget = (*rel).reltarget;
    (*pathnode).param_info = get_baserel_parampathinfo(root, rel, required_outer);
    (*pathnode).parallel_aware = false;
    (*pathnode).parallel_safe = (*rel).consider_parallel;
    (*pathnode).parallel_workers = 0;
    (*pathnode).pathkeys = NIL; /* result is always unordered */

    cost_resultscan(pathnode, root, rel, (*pathnode).param_info);

    pathnode
}

/*
 * create_worktablescan_path
 *	  Creates a path corresponding to a scan of a self-reference CTE,
 *	  returning the pathnode.
 */
pub unsafe fn create_worktablescan_path(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    required_outer: Relids,
) -> *mut Path {
    let pathnode: *mut Path = makeNode!(Path, T_Path);

    (*pathnode).pathtype = T_WorkTableScan;
    (*pathnode).parent = rel;
    (*pathnode).pathtarget = (*rel).reltarget;
    (*pathnode).param_info = get_baserel_parampathinfo(root, rel, required_outer);
    (*pathnode).parallel_aware = false;
    (*pathnode).parallel_safe = (*rel).consider_parallel;
    (*pathnode).parallel_workers = 0;
    (*pathnode).pathkeys = NIL; /* result is always unordered */

    /* Cost is the same as for a regular CTE scan */
    cost_ctescan(pathnode, root, rel, (*pathnode).param_info);

    pathnode
}

/*
 * create_foreignscan_path
 *	  Creates a path corresponding to a scan of a foreign base table,
 *	  returning the pathnode.
 *
 * This function is never called from core Postgres; rather, it's expected
 * to be called by the GetForeignPaths function of a foreign data wrapper.
 */
pub unsafe fn create_foreignscan_path(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    target: *mut PathTarget,
    rows: f64,
    disabled_nodes: c_int,
    startup_cost: Cost,
    total_cost: Cost,
    pathkeys: *mut List,
    required_outer: Relids,
    fdw_outerpath: *mut Path,
    fdw_restrictinfo: *mut List,
    fdw_private: *mut List,
) -> *mut ForeignPath {
    let pathnode: *mut ForeignPath = makeNode!(ForeignPath, T_ForeignPath);

    /* Historically some FDWs were confused about when to use this */
    Assert!(IS_SIMPLE_REL(rel));

    (*pathnode).path.pathtype = T_ForeignScan;
    (*pathnode).path.parent = rel;
    (*pathnode).path.pathtarget = if !target.is_null() { target } else { (*rel).reltarget };
    (*pathnode).path.param_info = get_baserel_parampathinfo(root, rel, required_outer);
    (*pathnode).path.parallel_aware = false;
    (*pathnode).path.parallel_safe = (*rel).consider_parallel;
    (*pathnode).path.parallel_workers = 0;
    (*pathnode).path.rows = rows;
    (*pathnode).path.disabled_nodes = disabled_nodes;
    (*pathnode).path.startup_cost = startup_cost;
    (*pathnode).path.total_cost = total_cost;
    (*pathnode).path.pathkeys = pathkeys;

    (*pathnode).fdw_outerpath = fdw_outerpath;
    (*pathnode).fdw_restrictinfo = fdw_restrictinfo;
    (*pathnode).fdw_private = fdw_private;

    pathnode
}

/*
 * create_foreign_join_path
 *	  Creates a path corresponding to a scan of a foreign join,
 *	  returning the pathnode.
 *
 * This function is never called from core Postgres; rather, it's expected
 * to be called by the GetForeignJoinPaths function of a foreign data wrapper.
 */
pub unsafe fn create_foreign_join_path(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    target: *mut PathTarget,
    rows: f64,
    disabled_nodes: c_int,
    startup_cost: Cost,
    total_cost: Cost,
    pathkeys: *mut List,
    required_outer: Relids,
    fdw_outerpath: *mut Path,
    fdw_restrictinfo: *mut List,
    fdw_private: *mut List,
) -> *mut ForeignPath {
    let pathnode: *mut ForeignPath = makeNode!(ForeignPath, T_ForeignPath);

    /*
     * We should use get_joinrel_parampathinfo to handle parameterized paths,
     * but the API of this function doesn't support it, and existing
     * extensions aren't yet trying to build such paths anyway.  For the
     * moment just throw an error if someone tries it; eventually we should
     * revisit this.
     */
    if !bms_is_empty(required_outer) || !bms_is_empty((*rel).lateral_relids) {
        ereport!(ERROR, errmsg!("parameterized foreign joins are not supported yet"));
    }

    (*pathnode).path.pathtype = T_ForeignScan;
    (*pathnode).path.parent = rel;
    (*pathnode).path.pathtarget = if !target.is_null() { target } else { (*rel).reltarget };
    (*pathnode).path.param_info = core::ptr::null_mut(); /* XXX see above */
    (*pathnode).path.parallel_aware = false;
    (*pathnode).path.parallel_safe = (*rel).consider_parallel;
    (*pathnode).path.parallel_workers = 0;
    (*pathnode).path.rows = rows;
    (*pathnode).path.disabled_nodes = disabled_nodes;
    (*pathnode).path.startup_cost = startup_cost;
    (*pathnode).path.total_cost = total_cost;
    (*pathnode).path.pathkeys = pathkeys;

    (*pathnode).fdw_outerpath = fdw_outerpath;
    (*pathnode).fdw_restrictinfo = fdw_restrictinfo;
    (*pathnode).fdw_private = fdw_private;

    pathnode
}

/*
 * create_foreign_upper_path
 *	  Creates a path corresponding to an upper relation that's computed
 *	  directly by an FDW, returning the pathnode.
 *
 * This function is never called from core Postgres; rather, it's expected to
 * be called by the GetForeignUpperPaths function of a foreign data wrapper.
 */
pub unsafe fn create_foreign_upper_path(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    target: *mut PathTarget,
    rows: f64,
    disabled_nodes: c_int,
    startup_cost: Cost,
    total_cost: Cost,
    pathkeys: *mut List,
    fdw_outerpath: *mut Path,
    fdw_restrictinfo: *mut List,
    fdw_private: *mut List,
) -> *mut ForeignPath {
    let pathnode: *mut ForeignPath = makeNode!(ForeignPath, T_ForeignPath);

    /*
     * Upper relations should never have any lateral references, since joining
     * is complete.
     */
    Assert!(bms_is_empty((*rel).lateral_relids));

    (*pathnode).path.pathtype = T_ForeignScan;
    (*pathnode).path.parent = rel;
    (*pathnode).path.pathtarget = if !target.is_null() { target } else { (*rel).reltarget };
    (*pathnode).path.param_info = core::ptr::null_mut();
    (*pathnode).path.parallel_aware = false;
    (*pathnode).path.parallel_safe = (*rel).consider_parallel;
    (*pathnode).path.parallel_workers = 0;
    (*pathnode).path.rows = rows;
    (*pathnode).path.disabled_nodes = disabled_nodes;
    (*pathnode).path.startup_cost = startup_cost;
    (*pathnode).path.total_cost = total_cost;
    (*pathnode).path.pathkeys = pathkeys;

    (*pathnode).fdw_outerpath = fdw_outerpath;
    (*pathnode).fdw_restrictinfo = fdw_restrictinfo;
    (*pathnode).fdw_private = fdw_private;

    pathnode
}

/*
 * calc_nestloop_required_outer
 *	  Compute the required_outer set for a nestloop join path
 *
 * Note: when considering a child join, the inputs nonetheless use top-level
 * parent relids
 *
 * Note: result must not share storage with either input
 */
pub unsafe fn calc_nestloop_required_outer(
    outerrelids: Relids,
    outer_paramrels: Relids,
    innerrelids: Relids,
    inner_paramrels: Relids,
) -> Relids {
    let mut required_outer: Relids;

    /* inner_path can require rels from outer path, but not vice versa */
    Assert!(!bms_overlap(outer_paramrels, innerrelids));
    /* easy case if inner path is not parameterized */
    if inner_paramrels.is_null() {
        return bms_copy(outer_paramrels);
    }
    /* else, form the union ... */
    required_outer = bms_union(outer_paramrels, inner_paramrels);
    /* ... and remove any mention of now-satisfied outer rels */
    required_outer = bms_del_members(required_outer, outerrelids);
    required_outer
}

/*
 * calc_non_nestloop_required_outer
 *	  Compute the required_outer set for a merge or hash join path
 *
 * Note: result must not share storage with either input
 */
pub unsafe fn calc_non_nestloop_required_outer(
    outer_path: *mut Path,
    inner_path: *mut Path,
) -> Relids {
    let outer_paramrels: Relids = PATH_REQ_OUTER(outer_path);
    let inner_paramrels: Relids = PATH_REQ_OUTER(inner_path);
    let innerrelids: Relids; /* PG_USED_FOR_ASSERTS_ONLY */
    let outerrelids: Relids; /* PG_USED_FOR_ASSERTS_ONLY */
    let required_outer: Relids;

    /*
     * Any parameterization of the input paths refers to topmost parents of
     * the relevant relations, because reparameterize_path_by_child() hasn't
     * been called yet.
     */
    let innerrelids = if !(*(*inner_path).parent).top_parent_relids.is_null() {
        (*(*inner_path).parent).top_parent_relids
    } else {
        (*(*inner_path).parent).relids
    };

    let outerrelids = if !(*(*outer_path).parent).top_parent_relids.is_null() {
        (*(*outer_path).parent).top_parent_relids
    } else {
        (*(*outer_path).parent).relids
    };

    /* neither path can require rels from the other */
    Assert!(!bms_overlap(outer_paramrels, innerrelids));
    Assert!(!bms_overlap(inner_paramrels, outerrelids));
    /* form the union ... */
    required_outer = bms_union(outer_paramrels, inner_paramrels);
    /* we do not need an explicit test for empty; bms_union gets it right */
    required_outer
}

/*
 * create_nestloop_path
 *	  Creates a pathnode corresponding to a nestloop join between two
 *	  relations.
 */
pub unsafe fn create_nestloop_path(
    root: *mut PlannerInfo,
    joinrel: *mut RelOptInfo,
    jointype: crate::nodes::nodes::JoinType,
    workspace: *mut JoinCostWorkspace,
    extra: *mut JoinPathExtraData,
    outer_path: *mut Path,
    inner_path: *mut Path,
    mut restrict_clauses: *mut List,
    pathkeys: *mut List,
    required_outer: Relids,
) -> *mut NestPath {
    let pathnode: *mut NestPath = makeNode!(NestPath, T_NestPath);
    let inner_req_outer: Relids = PATH_REQ_OUTER(inner_path);
    let outerrelids: Relids;

    /*
     * Paths are parameterized by top-level parents, so run parameterization
     * tests on the parent relids.
     */
    let outerrelids = if !(*(*outer_path).parent).top_parent_relids.is_null() {
        (*(*outer_path).parent).top_parent_relids
    } else {
        (*(*outer_path).parent).relids
    };

    /*
     * If the inner path is parameterized by the outer, we must drop any
     * restrict_clauses that are due to be moved into the inner path.
     */
    if bms_overlap(inner_req_outer, outerrelids) {
        let enforced_serials: *mut Bitmapset =
            get_param_path_clause_serials(inner_path);
        let mut jclauses: *mut List = NIL;

        foreach!(lc, restrict_clauses, {
            let rinfo: *mut crate::nodes::pathnodes::RestrictInfo =
                lfirst(current_cell!(lc)) as *mut crate::nodes::pathnodes::RestrictInfo;

            if !bms_is_member((*rinfo).rinfo_serial, enforced_serials) {
                jclauses = lappend(jclauses, rinfo as *mut c_void);
            }
        });
        restrict_clauses = jclauses;
    }

    (*pathnode).jpath.path.pathtype = T_NestLoop;
    (*pathnode).jpath.path.parent = joinrel;
    (*pathnode).jpath.path.pathtarget = (*joinrel).reltarget;
    (*pathnode).jpath.path.param_info = get_joinrel_parampathinfo(
        root,
        joinrel,
        outer_path,
        inner_path,
        (*extra).sjinfo,
        required_outer,
        &mut restrict_clauses,
    );
    (*pathnode).jpath.path.parallel_aware = false;
    (*pathnode).jpath.path.parallel_safe = (*joinrel).consider_parallel
        && (*outer_path).parallel_safe
        && (*inner_path).parallel_safe;
    /* This is a foolish way to estimate parallel_workers, but for now... */
    (*pathnode).jpath.path.parallel_workers = (*outer_path).parallel_workers;
    (*pathnode).jpath.path.pathkeys = pathkeys;
    (*pathnode).jpath.jointype = jointype;
    (*pathnode).jpath.inner_unique = (*extra).inner_unique;
    (*pathnode).jpath.outerjoinpath = outer_path;
    (*pathnode).jpath.innerjoinpath = inner_path;
    (*pathnode).jpath.joinrestrictinfo = restrict_clauses;

    final_cost_nestloop(root, pathnode, workspace, extra);

    pathnode
}

/*
 * create_mergejoin_path
 *	  Creates a pathnode corresponding to a mergejoin join between
 *	  two relations
 */
pub unsafe fn create_mergejoin_path(
    root: *mut PlannerInfo,
    joinrel: *mut RelOptInfo,
    jointype: crate::nodes::nodes::JoinType,
    workspace: *mut JoinCostWorkspace,
    extra: *mut JoinPathExtraData,
    outer_path: *mut Path,
    inner_path: *mut Path,
    mut restrict_clauses: *mut List,
    pathkeys: *mut List,
    required_outer: Relids,
    mergeclauses: *mut List,
    outersortkeys: *mut List,
    innersortkeys: *mut List,
    outer_presorted_keys: c_int,
) -> *mut MergePath {
    let pathnode: *mut MergePath = makeNode!(MergePath, T_MergePath);

    (*pathnode).jpath.path.pathtype = T_MergeJoin;
    (*pathnode).jpath.path.parent = joinrel;
    (*pathnode).jpath.path.pathtarget = (*joinrel).reltarget;
    (*pathnode).jpath.path.param_info = get_joinrel_parampathinfo(
        root,
        joinrel,
        outer_path,
        inner_path,
        (*extra).sjinfo,
        required_outer,
        &mut restrict_clauses,
    );
    (*pathnode).jpath.path.parallel_aware = false;
    (*pathnode).jpath.path.parallel_safe = (*joinrel).consider_parallel
        && (*outer_path).parallel_safe
        && (*inner_path).parallel_safe;
    /* This is a foolish way to estimate parallel_workers, but for now... */
    (*pathnode).jpath.path.parallel_workers = (*outer_path).parallel_workers;
    (*pathnode).jpath.path.pathkeys = pathkeys;
    (*pathnode).jpath.jointype = jointype;
    (*pathnode).jpath.inner_unique = (*extra).inner_unique;
    (*pathnode).jpath.outerjoinpath = outer_path;
    (*pathnode).jpath.innerjoinpath = inner_path;
    (*pathnode).jpath.joinrestrictinfo = restrict_clauses;
    (*pathnode).path_mergeclauses = mergeclauses;
    (*pathnode).outersortkeys = outersortkeys;
    (*pathnode).innersortkeys = innersortkeys;
    (*pathnode).outer_presorted_keys = outer_presorted_keys;
    /* pathnode->skip_mark_restore will be set by final_cost_mergejoin */
    /* pathnode->materialize_inner will be set by final_cost_mergejoin */

    final_cost_mergejoin(root, pathnode, workspace, extra);

    pathnode
}

/*
 * create_hashjoin_path
 *	  Creates a pathnode corresponding to a hash join between two relations.
 */
pub unsafe fn create_hashjoin_path(
    root: *mut PlannerInfo,
    joinrel: *mut RelOptInfo,
    jointype: crate::nodes::nodes::JoinType,
    workspace: *mut JoinCostWorkspace,
    extra: *mut JoinPathExtraData,
    outer_path: *mut Path,
    inner_path: *mut Path,
    parallel_hash: bool,
    mut restrict_clauses: *mut List,
    required_outer: Relids,
    hashclauses: *mut List,
) -> *mut HashPath {
    let pathnode: *mut HashPath = makeNode!(HashPath, T_HashPath);

    (*pathnode).jpath.path.pathtype = T_HashJoin;
    (*pathnode).jpath.path.parent = joinrel;
    (*pathnode).jpath.path.pathtarget = (*joinrel).reltarget;
    (*pathnode).jpath.path.param_info = get_joinrel_parampathinfo(
        root,
        joinrel,
        outer_path,
        inner_path,
        (*extra).sjinfo,
        required_outer,
        &mut restrict_clauses,
    );
    (*pathnode).jpath.path.parallel_aware = (*joinrel).consider_parallel && parallel_hash;
    (*pathnode).jpath.path.parallel_safe = (*joinrel).consider_parallel
        && (*outer_path).parallel_safe
        && (*inner_path).parallel_safe;
    /* This is a foolish way to estimate parallel_workers, but for now... */
    (*pathnode).jpath.path.parallel_workers = (*outer_path).parallel_workers;

    /*
     * A hashjoin never has pathkeys, since its output ordering is
     * unpredictable due to possible batching.
     */
    (*pathnode).jpath.path.pathkeys = NIL;
    (*pathnode).jpath.jointype = jointype;
    (*pathnode).jpath.inner_unique = (*extra).inner_unique;
    (*pathnode).jpath.outerjoinpath = outer_path;
    (*pathnode).jpath.innerjoinpath = inner_path;
    (*pathnode).jpath.joinrestrictinfo = restrict_clauses;
    (*pathnode).path_hashclauses = hashclauses;
    /* final_cost_hashjoin will fill in pathnode->num_batches */

    final_cost_hashjoin(root, pathnode, workspace, extra);

    pathnode
}

/*
 * create_projection_path
 *	  Creates a pathnode that represents performing a projection.
 *
 * 'rel' is the parent relation associated with the result
 * 'subpath' is the path representing the source of data
 * 'target' is the PathTarget to be computed
 */
pub unsafe fn create_projection_path(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    mut subpath: *mut Path,
    target: *mut PathTarget,
) -> *mut ProjectionPath {
    let pathnode: *mut ProjectionPath = makeNode!(ProjectionPath, T_ProjectionPath);

    /*
     * We mustn't put a ProjectionPath directly above another; it's useless
     * and will confuse create_projection_plan.  Rather than making sure all
     * callers handle that, let's implement it here, by stripping off any
     * ProjectionPath in what we're given.  Given this rule, there won't be
     * more than one.
     */
    if IsA!(subpath, T_ProjectionPath) {
        let subpp: *mut ProjectionPath = subpath as *mut ProjectionPath;
        Assert!((*subpp).path.parent == rel);
        subpath = (*subpp).subpath;
        Assert!(!IsA!(subpath, T_ProjectionPath));
    }

    (*pathnode).path.pathtype = T_Result;
    (*pathnode).path.parent = rel;
    (*pathnode).path.pathtarget = target;
    /* For now, assume we are above any joins, so no parameterization */
    (*pathnode).path.param_info = core::ptr::null_mut();
    (*pathnode).path.parallel_aware = false;
    (*pathnode).path.parallel_safe = (*rel).consider_parallel
        && (*subpath).parallel_safe
        && is_parallel_safe(root, (*target).exprs as *mut crate::nodes::nodes::Node);
    (*pathnode).path.parallel_workers = (*subpath).parallel_workers;
    /* Projection does not change the sort order */
    (*pathnode).path.pathkeys = (*subpath).pathkeys;

    (*pathnode).subpath = subpath;

    /*
     * We might not need a separate Result node.  If the input plan node type
     * can project, we can just tell it to project something else.  Or, if it
     * can't project but the desired target has the same expression list as
     * what the input will produce anyway, we can still give it the desired
     * tlist (possibly changing its ressortgroupref labels, but nothing else).
     */
    let oldtarget: *mut PathTarget = (*subpath).pathtarget;
    if is_projection_capable_path(subpath)
        || equal((*oldtarget).exprs as *mut c_void, (*target).exprs as *mut c_void)
    {
        /* No separate Result node needed */
        (*pathnode).dummypp = true;

        /*
         * Set cost of plan as subpath's cost, adjusted for tlist replacement.
         */
        (*pathnode).path.rows = (*subpath).rows;
        (*pathnode).path.disabled_nodes = (*subpath).disabled_nodes;
        (*pathnode).path.startup_cost = (*subpath).startup_cost
            + ((*target).cost.startup - (*oldtarget).cost.startup);
        (*pathnode).path.total_cost = (*subpath).total_cost
            + ((*target).cost.startup - (*oldtarget).cost.startup)
            + ((*target).cost.per_tuple - (*oldtarget).cost.per_tuple) * (*subpath).rows;
    } else {
        /* We really do need the Result node */
        (*pathnode).dummypp = false;

        /*
         * The Result node's cost is cpu_tuple_cost per row, plus the cost of
         * evaluating the tlist.  There is no qual to worry about.
         */
        (*pathnode).path.rows = (*subpath).rows;
        (*pathnode).path.disabled_nodes = (*subpath).disabled_nodes;
        (*pathnode).path.startup_cost = (*subpath).startup_cost + (*target).cost.startup;
        (*pathnode).path.total_cost = (*subpath).total_cost
            + (*target).cost.startup
            + (cpu_tuple_cost + (*target).cost.per_tuple) * (*subpath).rows;
    }

    pathnode
}

/*
 * apply_projection_to_path
 *	  Add a projection step, or just apply the target directly to given path.
 *
 * This has the same net effect as create_projection_path(), except that if
 * a separate Result plan node isn't needed, we just replace the given path's
 * pathtarget with the desired one.  This must be used only when the caller
 * knows that the given path isn't referenced elsewhere and so can be modified
 * in-place.
 *
 * If the input path is a GatherPath or GatherMergePath, we try to push the
 * new target down to its input as well; this is a yet more invasive
 * modification of the input path, which create_projection_path() can't do.
 */
pub unsafe fn apply_projection_to_path(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    path: *mut Path,
    target: *mut PathTarget,
) -> *mut Path {
    let oldcost: QualCost;

    /*
     * If given path can't project, we might need a Result node, so make a
     * separate ProjectionPath.
     */
    if !is_projection_capable_path(path) {
        return create_projection_path(root, rel, path, target) as *mut Path;
    }

    /*
     * We can just jam the desired tlist into the existing path, being sure to
     * update its cost estimates appropriately.
     */
    oldcost = (*(*path).pathtarget).cost;
    (*path).pathtarget = target;

    (*path).startup_cost += (*target).cost.startup - oldcost.startup;
    (*path).total_cost += (*target).cost.startup - oldcost.startup
        + ((*target).cost.per_tuple - oldcost.per_tuple) * (*path).rows;

    /*
     * If the path happens to be a Gather or GatherMerge path, we'd like to
     * arrange for the subpath to return the required target list so that
     * workers can help project.  But if there is something that is not
     * parallel-safe in the target expressions, then we can't.
     */
    if (IsA!(path, T_GatherPath) || IsA!(path, T_GatherMergePath))
        && is_parallel_safe(root, (*target).exprs as *mut crate::nodes::nodes::Node)
    {
        /*
         * We always use create_projection_path here, even if the subpath is
         * projection-capable, so as to avoid modifying the subpath in place.
         */
        if IsA!(path, T_GatherPath) {
            let gpath: *mut GatherPath = path as *mut GatherPath;
            (*gpath).subpath = create_projection_path(
                root,
                (*(*gpath).subpath).parent,
                (*gpath).subpath,
                target,
            ) as *mut Path;
        } else {
            let gmpath: *mut GatherMergePath = path as *mut GatherMergePath;
            (*gmpath).subpath = create_projection_path(
                root,
                (*(*gmpath).subpath).parent,
                (*gmpath).subpath,
                target,
            ) as *mut Path;
        }
    } else if (*path).parallel_safe
        && !is_parallel_safe(root, (*target).exprs as *mut crate::nodes::nodes::Node)
    {
        /*
         * We're inserting a parallel-restricted target list into a path
         * currently marked parallel-safe, so we have to mark it as no longer
         * safe.
         */
        (*path).parallel_safe = false;
    }

    path
}

/*
 * create_set_projection_path
 *	  Creates a pathnode that represents performing a projection that
 *	  includes set-returning functions.
 *
 * 'rel' is the parent relation associated with the result
 * 'subpath' is the path representing the source of data
 * 'target' is the PathTarget to be computed
 */
pub unsafe fn create_set_projection_path(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    subpath: *mut Path,
    target: *mut PathTarget,
) -> *mut ProjectSetPath {
    let pathnode: *mut ProjectSetPath = makeNode!(ProjectSetPath, T_ProjectSetPath);
    let mut tlist_rows: f64;

    (*pathnode).path.pathtype = T_ProjectSet;
    (*pathnode).path.parent = rel;
    (*pathnode).path.pathtarget = target;
    /* For now, assume we are above any joins, so no parameterization */
    (*pathnode).path.param_info = core::ptr::null_mut();
    (*pathnode).path.parallel_aware = false;
    (*pathnode).path.parallel_safe = (*rel).consider_parallel
        && (*subpath).parallel_safe
        && is_parallel_safe(root, (*target).exprs as *mut crate::nodes::nodes::Node);
    (*pathnode).path.parallel_workers = (*subpath).parallel_workers;
    /* Projection does not change the sort order XXX? */
    (*pathnode).path.pathkeys = (*subpath).pathkeys;

    (*pathnode).subpath = subpath;

    /*
     * Estimate number of rows produced by SRFs for each row of input; if
     * there's more than one in this node, use the maximum.
     */
    tlist_rows = 1.0;
    foreach!(lc, (*target).exprs, {
        let node: *mut crate::nodes::nodes::Node = lfirst(current_cell!(lc)) as *mut crate::nodes::nodes::Node;
        let itemrows: f64 = expression_returns_set_rows(root, node);
        if tlist_rows < itemrows {
            tlist_rows = itemrows;
        }
    });

    /*
     * In addition to the cost of evaluating the tlist, charge cpu_tuple_cost
     * per input row, and half of cpu_tuple_cost for each added output row.
     * This is slightly bizarre maybe, but it's what 9.6 did.
     */
    (*pathnode).path.disabled_nodes = (*subpath).disabled_nodes;
    (*pathnode).path.rows = (*subpath).rows * tlist_rows;
    (*pathnode).path.startup_cost = (*subpath).startup_cost + (*target).cost.startup;
    (*pathnode).path.total_cost = (*subpath).total_cost
        + (*target).cost.startup
        + (cpu_tuple_cost + (*target).cost.per_tuple) * (*subpath).rows
        + ((*pathnode).path.rows - (*subpath).rows) * cpu_tuple_cost / 2.0;

    pathnode
}

/*
 * create_incremental_sort_path
 *	  Creates a pathnode that represents performing an incremental sort.
 *
 * 'rel' is the parent relation associated with the result
 * 'subpath' is the path representing the source of data
 * 'pathkeys' represents the desired sort order
 * 'presorted_keys' is the number of keys by which the input path is
 *		already sorted
 * 'limit_tuples' is the estimated bound on the number of output tuples,
 *		or -1 if no LIMIT or couldn't estimate
 */
pub unsafe fn create_incremental_sort_path(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    subpath: *mut Path,
    pathkeys: *mut List,
    presorted_keys: c_int,
    limit_tuples: f64,
) -> *mut IncrementalSortPath {
    let sort: *mut IncrementalSortPath = makeNode!(IncrementalSortPath, T_IncrementalSortPath);
    let pathnode: *mut SortPath = &raw mut (*sort).spath;

    (*pathnode).path.pathtype = T_IncrementalSort;
    (*pathnode).path.parent = rel;
    /* Sort doesn't project, so use source path's pathtarget */
    (*pathnode).path.pathtarget = (*subpath).pathtarget;
    /* For now, assume we are above any joins, so no parameterization */
    (*pathnode).path.param_info = core::ptr::null_mut();
    (*pathnode).path.parallel_aware = false;
    (*pathnode).path.parallel_safe = (*rel).consider_parallel && (*subpath).parallel_safe;
    (*pathnode).path.parallel_workers = (*subpath).parallel_workers;
    (*pathnode).path.pathkeys = pathkeys;

    (*pathnode).subpath = subpath;

    cost_incremental_sort(
        &raw mut (*pathnode).path,
        root,
        pathkeys,
        presorted_keys,
        (*subpath).disabled_nodes,
        (*subpath).startup_cost,
        (*subpath).total_cost,
        (*subpath).rows,
        (*(*subpath).pathtarget).width,
        0.0, /* XXX comparison_cost shouldn't be 0? */
        work_mem,
        limit_tuples,
    );

    (*sort).nPresortedCols = presorted_keys;

    sort
}

/*
 * create_sort_path
 *	  Creates a pathnode that represents performing an explicit sort.
 *
 * 'rel' is the parent relation associated with the result
 * 'subpath' is the path representing the source of data
 * 'pathkeys' represents the desired sort order
 * 'limit_tuples' is the estimated bound on the number of output tuples,
 *		or -1 if no LIMIT or couldn't estimate
 */
pub unsafe fn create_sort_path(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    subpath: *mut Path,
    pathkeys: *mut List,
    limit_tuples: f64,
) -> *mut SortPath {
    let pathnode: *mut SortPath = makeNode!(SortPath, T_SortPath);

    (*pathnode).path.pathtype = T_Sort;
    (*pathnode).path.parent = rel;
    /* Sort doesn't project, so use source path's pathtarget */
    (*pathnode).path.pathtarget = (*subpath).pathtarget;
    /* For now, assume we are above any joins, so no parameterization */
    (*pathnode).path.param_info = core::ptr::null_mut();
    (*pathnode).path.parallel_aware = false;
    (*pathnode).path.parallel_safe = (*rel).consider_parallel && (*subpath).parallel_safe;
    (*pathnode).path.parallel_workers = (*subpath).parallel_workers;
    (*pathnode).path.pathkeys = pathkeys;

    (*pathnode).subpath = subpath;

    cost_sort(
        &raw mut (*pathnode).path,
        root,
        pathkeys,
        (*subpath).disabled_nodes,
        (*subpath).total_cost,
        (*subpath).rows,
        (*(*subpath).pathtarget).width,
        0.0, /* XXX comparison_cost shouldn't be 0? */
        work_mem,
        limit_tuples,
    );

    pathnode
}

/*
 * create_group_path
 *	  Creates a pathnode that represents performing grouping of presorted input
 *
 * 'rel' is the parent relation associated with the result
 * 'subpath' is the path representing the source of data
 * 'target' is the PathTarget to be computed
 * 'groupClause' is a list of SortGroupClause's representing the grouping
 * 'qual' is the HAVING quals if any
 * 'numGroups' is the estimated number of groups
 */
pub unsafe fn create_group_path(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    subpath: *mut Path,
    group_clause: *mut List,
    qual: *mut List,
    num_groups: f64,
) -> *mut GroupPath {
    let pathnode: *mut GroupPath = makeNode!(GroupPath, T_GroupPath);
    let target: *mut PathTarget = (*rel).reltarget;

    (*pathnode).path.pathtype = T_Group;
    (*pathnode).path.parent = rel;
    (*pathnode).path.pathtarget = target;
    /* For now, assume we are above any joins, so no parameterization */
    (*pathnode).path.param_info = core::ptr::null_mut();
    (*pathnode).path.parallel_aware = false;
    (*pathnode).path.parallel_safe = (*rel).consider_parallel && (*subpath).parallel_safe;
    (*pathnode).path.parallel_workers = (*subpath).parallel_workers;
    /* Group doesn't change sort ordering */
    (*pathnode).path.pathkeys = (*subpath).pathkeys;

    (*pathnode).subpath = subpath;
    (*pathnode).groupClause = group_clause;
    (*pathnode).qual = qual;

    cost_group(
        &raw mut (*pathnode).path,
        root,
        list_length(group_clause),
        num_groups,
        qual,
        (*subpath).disabled_nodes,
        (*subpath).startup_cost,
        (*subpath).total_cost,
        (*subpath).rows,
    );

    /* add tlist eval cost for each output row */
    (*pathnode).path.startup_cost += (*target).cost.startup;
    (*pathnode).path.total_cost +=
        (*target).cost.startup + (*target).cost.per_tuple * (*pathnode).path.rows;

    pathnode
}

/*
 * create_upper_unique_path
 *	  Creates a pathnode that represents performing an explicit Unique step
 *	  on presorted input.
 *
 * 'rel' is the parent relation associated with the result
 * 'subpath' is the path representing the source of data
 * 'numCols' is the number of grouping columns
 * 'numGroups' is the estimated number of groups
 */
pub unsafe fn create_upper_unique_path(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    subpath: *mut Path,
    num_cols: c_int,
    num_groups: f64,
) -> *mut UpperUniquePath {
    let pathnode: *mut UpperUniquePath = makeNode!(UpperUniquePath, T_UpperUniquePath);

    (*pathnode).path.pathtype = T_Unique;
    (*pathnode).path.parent = rel;
    /* Unique doesn't project, so use source path's pathtarget */
    (*pathnode).path.pathtarget = (*subpath).pathtarget;
    /* For now, assume we are above any joins, so no parameterization */
    (*pathnode).path.param_info = core::ptr::null_mut();
    (*pathnode).path.parallel_aware = false;
    (*pathnode).path.parallel_safe = (*rel).consider_parallel && (*subpath).parallel_safe;
    (*pathnode).path.parallel_workers = (*subpath).parallel_workers;
    /* Unique doesn't change the input ordering */
    (*pathnode).path.pathkeys = (*subpath).pathkeys;

    (*pathnode).subpath = subpath;
    (*pathnode).numkeys = num_cols;

    /*
     * Charge one cpu_operator_cost per comparison per input tuple. We assume
     * all columns get compared at most of the tuples.
     */
    (*pathnode).path.disabled_nodes = (*subpath).disabled_nodes;
    (*pathnode).path.startup_cost = (*subpath).startup_cost;
    (*pathnode).path.total_cost =
        (*subpath).total_cost + cpu_operator_cost * (*subpath).rows * num_cols as f64;
    (*pathnode).path.rows = num_groups;

    /* suppress unused variable warning for root */
    let _ = root;

    pathnode
}

/*
 * create_agg_path
 *	  Creates a pathnode that represents performing aggregation/grouping
 *
 * 'rel' is the parent relation associated with the result
 * 'subpath' is the path representing the source of data
 * 'target' is the PathTarget to be computed
 * 'aggstrategy' is the Agg node's basic implementation strategy
 * 'aggsplit' is the Agg node's aggregate-splitting mode
 * 'groupClause' is a list of SortGroupClause's representing the grouping
 * 'qual' is the HAVING quals if any
 * 'aggcosts' contains cost info about the aggregate functions to be computed
 * 'numGroups' is the estimated number of groups (1 if not grouping)
 */
pub unsafe fn create_agg_path(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    subpath: *mut Path,
    target: *mut PathTarget,
    aggstrategy: AggStrategy,
    aggsplit: AggSplit,
    group_clause: *mut List,
    qual: *mut List,
    aggcosts: *const AggClauseCosts,
    num_groups: f64,
) -> *mut AggPath {
    let pathnode: *mut AggPath = makeNode!(AggPath, T_AggPath);

    (*pathnode).path.pathtype = T_Agg;
    (*pathnode).path.parent = rel;
    (*pathnode).path.pathtarget = target;
    /* For now, assume we are above any joins, so no parameterization */
    (*pathnode).path.param_info = core::ptr::null_mut();
    (*pathnode).path.parallel_aware = false;
    (*pathnode).path.parallel_safe = (*rel).consider_parallel && (*subpath).parallel_safe;
    (*pathnode).path.parallel_workers = (*subpath).parallel_workers;

    if aggstrategy == AGG_SORTED {
        /*
         * Attempt to preserve the order of the subpath.  Additional pathkeys
         * may have been added in adjust_group_pathkeys_for_groupagg() to
         * support ORDER BY / DISTINCT aggregates.  Pathkeys added there
         * belong to columns within the aggregate function, so we must strip
         * these additional pathkeys off as those columns are unavailable
         * above the aggregate node.
         */
        if list_length((*subpath).pathkeys) > (*root).num_groupby_pathkeys {
            (*pathnode).path.pathkeys =
                list_copy_head((*subpath).pathkeys, (*root).num_groupby_pathkeys);
        } else {
            (*pathnode).path.pathkeys = (*subpath).pathkeys; /* preserves order */
        }
    } else {
        (*pathnode).path.pathkeys = NIL; /* output is unordered */
    }

    (*pathnode).subpath = subpath;
    (*pathnode).aggstrategy = aggstrategy;
    (*pathnode).aggsplit = aggsplit;
    (*pathnode).numGroups = num_groups;
    (*pathnode).transitionSpace = if !aggcosts.is_null() { (*aggcosts).transitionSpace as u64 } else { 0 };
    (*pathnode).groupClause = group_clause;
    (*pathnode).qual = qual;

    cost_agg(
        &raw mut (*pathnode).path,
        root,
        aggstrategy,
        aggcosts,
        list_length(group_clause),
        num_groups,
        qual,
        (*subpath).disabled_nodes,
        (*subpath).startup_cost,
        (*subpath).total_cost,
        (*subpath).rows,
        (*(*subpath).pathtarget).width as f64,
    );

    /* add tlist eval cost for each output row */
    (*pathnode).path.startup_cost += (*target).cost.startup;
    (*pathnode).path.total_cost +=
        (*target).cost.startup + (*target).cost.per_tuple * (*pathnode).path.rows;

    pathnode
}

/*
 * create_groupingsets_path
 *	  Creates a pathnode that represents performing GROUPING SETS aggregation
 *
 * 'rel' is the parent relation associated with the result
 * 'subpath' is the path representing the source of data
 * 'target' is the PathTarget to be computed
 * 'having_qual' is the HAVING quals if any
 * 'rollups' is a list of RollupData nodes
 * 'agg_costs' contains cost info about the aggregate functions to be computed
 */
pub unsafe fn create_groupingsets_path(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    subpath: *mut Path,
    having_qual: *mut List,
    mut aggstrategy: AggStrategy,
    rollups: *mut List,
    agg_costs: *const AggClauseCosts,
) -> *mut GroupingSetsPath {
    let pathnode: *mut GroupingSetsPath = makeNode!(GroupingSetsPath, T_GroupingSetsPath);
    let target: *mut PathTarget = (*rel).reltarget;
    let mut is_first = true;
    let mut is_first_sort = true;

    /* The topmost generated Plan node will be an Agg */
    (*pathnode).path.pathtype = T_Agg;
    (*pathnode).path.parent = rel;
    (*pathnode).path.pathtarget = target;
    (*pathnode).path.param_info = (*subpath).param_info;
    (*pathnode).path.parallel_aware = false;
    (*pathnode).path.parallel_safe = (*rel).consider_parallel && (*subpath).parallel_safe;
    (*pathnode).path.parallel_workers = (*subpath).parallel_workers;
    (*pathnode).subpath = subpath;

    /*
     * Simplify callers by downgrading AGG_SORTED to AGG_PLAIN, and AGG_MIXED
     * to AGG_HASHED, here if possible.
     */
    if aggstrategy == AGG_SORTED
        && list_length(rollups) == 1
        && (*(linitial(rollups) as *mut RollupData)).groupClause.is_null()
    {
        aggstrategy = AGG_PLAIN;
    }

    if aggstrategy == AGG_MIXED && list_length(rollups) == 1 {
        aggstrategy = AGG_HASHED;
    }

    /*
     * Output will be in sorted order by group_pathkeys if, and only if, there
     * is a single rollup operation on a non-empty list of grouping expressions.
     */
    if aggstrategy == AGG_SORTED && list_length(rollups) == 1 {
        (*pathnode).path.pathkeys = (*root).group_pathkeys;
    } else {
        (*pathnode).path.pathkeys = NIL;
    }

    (*pathnode).aggstrategy = aggstrategy;
    (*pathnode).rollups = rollups;
    (*pathnode).qual = having_qual;
    (*pathnode).transitionSpace = if !agg_costs.is_null() { (*agg_costs).transitionSpace as u64 } else { 0 };

    Assert!(!rollups.is_null());
    Assert!(aggstrategy != AGG_PLAIN || list_length(rollups) == 1);
    Assert!(aggstrategy != AGG_MIXED || list_length(rollups) > 1);

    foreach!(lc, rollups, {
        let rollup: *mut RollupData = lfirst(current_cell!(lc)) as *mut RollupData;
        let gsets: *mut List = (*rollup).gsets;
        let num_group_cols: c_int = list_length(linitial(gsets) as *mut List);

        if is_first {
            cost_agg(
                &raw mut (*pathnode).path,
                root,
                aggstrategy,
                agg_costs,
                num_group_cols,
                (*rollup).numGroups,
                having_qual,
                (*subpath).disabled_nodes,
                (*subpath).startup_cost,
                (*subpath).total_cost,
                (*subpath).rows,
                (*(*subpath).pathtarget).width as f64,
            );
            is_first = false;
            if !(*rollup).is_hashed {
                is_first_sort = false;
            }
        } else {
            /* dummy path nodes for cost calculation */
            let mut sort_path: Path = core::mem::zeroed();
            let mut agg_path: Path = core::mem::zeroed();

            if (*rollup).is_hashed || is_first_sort {
                /*
                 * Account for cost of aggregation, but don't charge input
                 * cost again
                 */
                cost_agg(
                    &raw mut agg_path,
                    root,
                    if (*rollup).is_hashed { AGG_HASHED } else { AGG_SORTED },
                    agg_costs,
                    num_group_cols,
                    (*rollup).numGroups,
                    having_qual,
                    0,
                    0.0,
                    0.0,
                    (*subpath).rows,
                    (*(*subpath).pathtarget).width as f64,
                );
                if !(*rollup).is_hashed {
                    is_first_sort = false;
                }
            } else {
                /* Account for cost of sort, but don't charge input cost again */
                cost_sort(
                    &raw mut sort_path,
                    root,
                    NIL,
                    0,
                    0.0,
                    (*subpath).rows,
                    (*(*subpath).pathtarget).width,
                    0.0,
                    work_mem,
                    -1.0,
                );

                /* Account for cost of aggregation */
                cost_agg(
                    &raw mut agg_path,
                    root,
                    AGG_SORTED,
                    agg_costs,
                    num_group_cols,
                    (*rollup).numGroups,
                    having_qual,
                    sort_path.disabled_nodes,
                    sort_path.startup_cost,
                    sort_path.total_cost,
                    sort_path.rows,
                    (*(*subpath).pathtarget).width as f64,
                );
            }

            (*pathnode).path.disabled_nodes += agg_path.disabled_nodes;
            (*pathnode).path.total_cost += agg_path.total_cost;
            (*pathnode).path.rows += agg_path.rows;
        }
    });

    /* add tlist eval cost for each output row */
    (*pathnode).path.startup_cost += (*target).cost.startup;
    (*pathnode).path.total_cost +=
        (*target).cost.startup + (*target).cost.per_tuple * (*pathnode).path.rows;

    pathnode
}

/*
 * create_minmaxagg_path
 *	  Creates a pathnode that represents computation of MIN/MAX aggregates
 *
 * 'rel' is the parent relation associated with the result
 * 'target' is the PathTarget to be computed
 * 'mmaggregates' is a list of MinMaxAggInfo structs
 * 'quals' is the HAVING quals if any
 */
pub unsafe fn create_minmaxagg_path(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    target: *mut PathTarget,
    mmaggregates: *mut List,
    quals: *mut List,
) -> *mut MinMaxAggPath {
    let pathnode: *mut MinMaxAggPath = makeNode!(MinMaxAggPath, T_MinMaxAggPath);
    let mut initplan_cost: Cost = 0.0;
    let mut initplan_disabled_nodes: c_int = 0;

    /* The topmost generated Plan node will be a Result */
    (*pathnode).path.pathtype = T_Result;
    (*pathnode).path.parent = rel;
    (*pathnode).path.pathtarget = target;
    /* For now, assume we are above any joins, so no parameterization */
    (*pathnode).path.param_info = core::ptr::null_mut();
    (*pathnode).path.parallel_aware = false;
    (*pathnode).path.parallel_safe = true; /* might change below */
    (*pathnode).path.parallel_workers = 0;
    /* Result is one unordered row */
    (*pathnode).path.rows = 1.0;
    (*pathnode).path.pathkeys = NIL;

    (*pathnode).mmaggregates = mmaggregates;
    (*pathnode).quals = quals;

    /* Calculate cost of all the initplans, and check parallel safety */
    foreach!(lc, mmaggregates, {
        let mminfo: *mut MinMaxAggInfo =
            lfirst(current_cell!(lc)) as *mut MinMaxAggInfo;
        initplan_disabled_nodes += (*(*mminfo).path).disabled_nodes;
        initplan_cost += (*mminfo).pathcost;
        if !(*(*mminfo).path).parallel_safe {
            (*pathnode).path.parallel_safe = false;
        }
    });

    /* add tlist eval cost for each output row, plus cpu_tuple_cost */
    (*pathnode).path.disabled_nodes = initplan_disabled_nodes;
    (*pathnode).path.startup_cost = initplan_cost + (*target).cost.startup;
    (*pathnode).path.total_cost =
        initplan_cost + (*target).cost.startup + (*target).cost.per_tuple + cpu_tuple_cost;

    /*
     * Add cost of qual, if any --- but we ignore its selectivity, since our
     * rowcount estimate should be 1 no matter what the qual is.
     */
    if !quals.is_null() {
        let mut qual_cost: QualCost = core::mem::zeroed();
        cost_qual_eval(&raw mut qual_cost, quals, root);
        (*pathnode).path.startup_cost += qual_cost.startup;
        (*pathnode).path.total_cost += qual_cost.startup + qual_cost.per_tuple;
    }

    /*
     * If the initplans were all parallel-safe, also check safety of the
     * target and quals.
     */
    if (*pathnode).path.parallel_safe {
        (*pathnode).path.parallel_safe =
            is_parallel_safe(root, (*target).exprs as *mut crate::nodes::nodes::Node)
                && is_parallel_safe(root, quals as *mut crate::nodes::nodes::Node);
    }

    pathnode
}

/*
 * create_windowagg_path
 *	  Creates a pathnode that represents computation of window functions
 *
 * 'rel' is the parent relation associated with the result
 * 'subpath' is the path representing the source of data
 * 'target' is the PathTarget to be computed
 * 'windowFuncs' is a list of WindowFunc structs
 * 'runCondition' is a list of OpExprs to short-circuit WindowAgg execution
 * 'winclause' is a WindowClause that is common to all the WindowFuncs
 * 'qual' WindowClause.runconditions from lower-level WindowAggPaths.
 * 'topwindow' pass as true only for the top-level WindowAgg.
 */
pub unsafe fn create_windowagg_path(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    subpath: *mut Path,
    target: *mut PathTarget,
    window_funcs: *mut List,
    run_condition: *mut List,
    winclause: *mut WindowClause,
    qual: *mut List,
    topwindow: bool,
) -> *mut WindowAggPath {
    let pathnode: *mut WindowAggPath = makeNode!(WindowAggPath, T_WindowAggPath);

    /* qual can only be set for the topwindow */
    Assert!(qual.is_null() || topwindow);

    (*pathnode).path.pathtype = T_WindowAgg;
    (*pathnode).path.parent = rel;
    (*pathnode).path.pathtarget = target;
    /* For now, assume we are above any joins, so no parameterization */
    (*pathnode).path.param_info = core::ptr::null_mut();
    (*pathnode).path.parallel_aware = false;
    (*pathnode).path.parallel_safe = (*rel).consider_parallel && (*subpath).parallel_safe;
    (*pathnode).path.parallel_workers = (*subpath).parallel_workers;
    /* WindowAgg preserves the input sort order */
    (*pathnode).path.pathkeys = (*subpath).pathkeys;

    (*pathnode).subpath = subpath;
    (*pathnode).winclause = winclause;
    (*pathnode).qual = qual;
    (*pathnode).runCondition = run_condition;
    (*pathnode).topwindow = topwindow;

    /*
     * For costing purposes, assume that there are no redundant partitioning
     * or ordering columns.
     */
    cost_windowagg(
        &raw mut (*pathnode).path,
        root,
        window_funcs,
        winclause,
        (*subpath).disabled_nodes,
        (*subpath).startup_cost,
        (*subpath).total_cost,
        (*subpath).rows,
    );

    /* add tlist eval cost for each output row */
    (*pathnode).path.startup_cost += (*target).cost.startup;
    (*pathnode).path.total_cost +=
        (*target).cost.startup + (*target).cost.per_tuple * (*pathnode).path.rows;

    pathnode
}

/*
 * create_setop_path
 *	  Creates a pathnode that represents computation of INTERSECT or EXCEPT
 *
 * 'rel' is the parent relation associated with the result
 * 'leftpath' is the path representing the left-hand source of data
 * 'rightpath' is the path representing the right-hand source of data
 * 'cmd' is the specific semantics (INTERSECT or EXCEPT, with/without ALL)
 * 'strategy' is the implementation strategy (sorted or hashed)
 * 'groupList' is a list of SortGroupClause's representing the grouping
 * 'numGroups' is the estimated number of distinct groups in left-hand input
 * 'outputRows' is the estimated number of output rows
 */
pub unsafe fn create_setop_path(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    leftpath: *mut Path,
    rightpath: *mut Path,
    cmd: SetOpCmd,
    strategy: crate::nodes::nodes::SetOpStrategy,
    group_list: *mut List,
    num_groups: f64,
    output_rows: f64,
) -> *mut SetOpPath {
    let pathnode: *mut SetOpPath = makeNode!(SetOpPath, T_SetOpPath);

    (*pathnode).path.pathtype = T_SetOp;
    (*pathnode).path.parent = rel;
    (*pathnode).path.pathtarget = (*rel).reltarget;
    /* For now, assume we are above any joins, so no parameterization */
    (*pathnode).path.param_info = core::ptr::null_mut();
    (*pathnode).path.parallel_aware = false;
    (*pathnode).path.parallel_safe = (*rel).consider_parallel
        && (*leftpath).parallel_safe
        && (*rightpath).parallel_safe;
    (*pathnode).path.parallel_workers =
        (*leftpath).parallel_workers + (*rightpath).parallel_workers;
    /* SetOp preserves the input sort order if in sort mode */
    (*pathnode).path.pathkeys = if strategy == SETOP_SORTED {
        (*leftpath).pathkeys
    } else {
        NIL
    };

    (*pathnode).leftpath = leftpath;
    (*pathnode).rightpath = rightpath;
    (*pathnode).cmd = cmd;
    (*pathnode).strategy = strategy;
    (*pathnode).groupList = group_list;
    (*pathnode).numGroups = num_groups;

    /*
     * Compute cost estimates.
     */
    (*pathnode).path.disabled_nodes =
        (*leftpath).disabled_nodes + (*rightpath).disabled_nodes;
    if strategy == SETOP_SORTED {
        /*
         * In sorted mode, we can emit output incrementally.  Charge one
         * cpu_operator_cost per comparison per input tuple.
         */
        (*pathnode).path.startup_cost =
            (*leftpath).startup_cost + (*rightpath).startup_cost;
        (*pathnode).path.total_cost = (*leftpath).total_cost
            + (*rightpath).total_cost
            + cpu_operator_cost
                * ((*leftpath).rows + (*rightpath).rows)
                * list_length(group_list) as f64;

        /*
         * Also charge a small amount per extracted tuple.
         */
        (*pathnode).path.total_cost += cpu_operator_cost * output_rows;
    } else {
        let hashentrysize: usize;

        /*
         * In hashed mode, we must read all the input before we can emit
         * anything.
         */
        (*pathnode).path.startup_cost = (*leftpath).total_cost
            + (*rightpath).total_cost
            + cpu_operator_cost
                * ((*leftpath).rows + (*rightpath).rows)
                * list_length(group_list) as f64;
        (*pathnode).path.total_cost = (*pathnode).path.startup_cost;

        (*pathnode).path.total_cost += cpu_operator_cost * output_rows;

        /*
         * Mark the path as disabled if enable_hashagg is off.
         */
        if !enable_hashagg {
            (*pathnode).path.disabled_nodes += 1;
        }

        /*
         * Also disable if it doesn't look like the hashtable will fit into
         * hash_mem.
         */
        hashentrysize = MAXALIGN((*(*leftpath).pathtarget).width as usize)
            + SizeofMinimalTupleHeader;
        if hashentrysize as f64 * num_groups > get_hash_memory_limit() as f64 {
            (*pathnode).path.disabled_nodes += 1;
        }
    }
    (*pathnode).path.rows = output_rows;

    /* suppress unused variable warning for root */
    let _ = root;

    pathnode
}

/*
 * create_recursiveunion_path
 *	  Creates a pathnode that represents a recursive UNION node
 *
 * 'rel' is the parent relation associated with the result
 * 'leftpath' is the source of data for the non-recursive term
 * 'rightpath' is the source of data for the recursive term
 * 'target' is the PathTarget to be computed
 * 'distinctList' is a list of SortGroupClause's representing the grouping
 * 'wtParam' is the ID of Param representing work table
 * 'numGroups' is the estimated number of groups
 */
pub unsafe fn create_recursiveunion_path(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    leftpath: *mut Path,
    rightpath: *mut Path,
    target: *mut PathTarget,
    distinct_list: *mut List,
    wt_param: c_int,
    num_groups: f64,
) -> *mut RecursiveUnionPath {
    let pathnode: *mut RecursiveUnionPath =
        makeNode!(RecursiveUnionPath, T_RecursiveUnionPath);

    (*pathnode).path.pathtype = T_RecursiveUnion;
    (*pathnode).path.parent = rel;
    (*pathnode).path.pathtarget = target;
    /* For now, assume we are above any joins, so no parameterization */
    (*pathnode).path.param_info = core::ptr::null_mut();
    (*pathnode).path.parallel_aware = false;
    (*pathnode).path.parallel_safe = (*rel).consider_parallel
        && (*leftpath).parallel_safe
        && (*rightpath).parallel_safe;
    /* Foolish, but we'll do it like joins for now: */
    (*pathnode).path.parallel_workers = (*leftpath).parallel_workers;
    /* RecursiveUnion result is always unsorted */
    (*pathnode).path.pathkeys = NIL;

    (*pathnode).leftpath = leftpath;
    (*pathnode).rightpath = rightpath;
    (*pathnode).distinctList = distinct_list;
    (*pathnode).wtParam = wt_param;
    (*pathnode).numGroups = num_groups;

    cost_recursive_union(&raw mut (*pathnode).path, leftpath, rightpath);

    /* suppress unused variable warning for root */
    let _ = root;

    pathnode
}

/*
 * create_lockrows_path
 *	  Creates a pathnode that represents acquiring row locks
 *
 * 'rel' is the parent relation associated with the result
 * 'subpath' is the path representing the source of data
 * 'rowMarks' is a list of PlanRowMark's
 * 'epqParam' is the ID of Param for EvalPlanQual re-eval
 */
pub unsafe fn create_lockrows_path(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    subpath: *mut Path,
    row_marks: *mut List,
    epq_param: c_int,
) -> *mut LockRowsPath {
    let pathnode: *mut LockRowsPath = makeNode!(LockRowsPath, T_LockRowsPath);

    (*pathnode).path.pathtype = T_LockRows;
    (*pathnode).path.parent = rel;
    /* LockRows doesn't project, so use source path's pathtarget */
    (*pathnode).path.pathtarget = (*subpath).pathtarget;
    /* For now, assume we are above any joins, so no parameterization */
    (*pathnode).path.param_info = core::ptr::null_mut();
    (*pathnode).path.parallel_aware = false;
    (*pathnode).path.parallel_safe = false;
    (*pathnode).path.parallel_workers = 0;
    (*pathnode).path.rows = (*subpath).rows;

    /*
     * The result cannot be assumed sorted, since locking might cause the sort
     * key columns to be replaced with new values.
     */
    (*pathnode).path.pathkeys = NIL;

    (*pathnode).subpath = subpath;
    (*pathnode).rowMarks = row_marks;
    (*pathnode).epqParam = epq_param;

    /*
     * We should charge something extra for the costs of row locking and
     * possible refetches, but it's hard to say how much.  For now, use
     * cpu_tuple_cost per row.
     */
    (*pathnode).path.disabled_nodes = (*subpath).disabled_nodes;
    (*pathnode).path.startup_cost = (*subpath).startup_cost;
    (*pathnode).path.total_cost = (*subpath).total_cost + cpu_tuple_cost * (*subpath).rows;

    /* suppress unused variable warning for root */
    let _ = root;

    pathnode
}

/*
 * create_modifytable_path
 *	  Creates a pathnode that represents performing INSERT/UPDATE/DELETE/MERGE
 *
 * 'rel' is the parent relation associated with the result
 * 'subpath' is a Path producing source data
 * 'operation' is the operation type
 * 'canSetTag' is true if we set the command tag/es_processed
 * 'nominalRelation' is the parent RT index for use of EXPLAIN
 * 'rootRelation' is the partitioned/inherited table root RTI, or 0 if none
 * 'partColsUpdated' is true if any partitioning columns are being updated
 * 'resultRelations' is an integer list of actual RT indexes of target rel(s)
 * 'updateColnosLists' is a list of UPDATE target column number lists
 * 'withCheckOptionLists' is a list of WCO lists (one per rel)
 * 'returningLists' is a list of RETURNING tlists (one per rel)
 * 'rowMarks' is a list of PlanRowMarks (non-locking only)
 * 'onconflict' is the ON CONFLICT clause, or NULL
 * 'epqParam' is the ID of Param for EvalPlanQual re-eval
 * 'mergeActionLists' is a list of lists of MERGE actions (one per rel)
 * 'mergeJoinConditions' is a list of join conditions for MERGE (one per rel)
 */
pub unsafe fn create_modifytable_path(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    subpath: *mut Path,
    operation: CmdType,
    can_set_tag: bool,
    nominal_relation: Index,
    root_relation: Index,
    part_cols_updated: bool,
    result_relations: *mut List,
    update_colnos_lists: *mut List,
    with_check_option_lists: *mut List,
    returning_lists: *mut List,
    row_marks: *mut List,
    onconflict: *mut OnConflictExpr,
    merge_action_lists: *mut List,
    merge_join_conditions: *mut List,
    epq_param: c_int,
) -> *mut ModifyTablePath {
    let pathnode: *mut ModifyTablePath = makeNode!(ModifyTablePath, T_ModifyTablePath);

    Assert!(
        operation as u32 == crate::nodes::nodes::CmdType::CMD_MERGE as u32
            || if operation as u32 == crate::nodes::nodes::CmdType::CMD_UPDATE as u32 {
                list_length(result_relations) == list_length(update_colnos_lists)
            } else {
                update_colnos_lists.is_null()
            }
    );
    Assert!(
        with_check_option_lists.is_null()
            || list_length(result_relations) == list_length(with_check_option_lists)
    );
    Assert!(
        returning_lists.is_null()
            || list_length(result_relations) == list_length(returning_lists)
    );

    (*pathnode).path.pathtype = T_ModifyTable;
    (*pathnode).path.parent = rel;
    /* pathtarget is not interesting, just make it minimally valid */
    (*pathnode).path.pathtarget = (*rel).reltarget;
    /* For now, assume we are above any joins, so no parameterization */
    (*pathnode).path.param_info = core::ptr::null_mut();
    (*pathnode).path.parallel_aware = false;
    (*pathnode).path.parallel_safe = false;
    (*pathnode).path.parallel_workers = 0;
    (*pathnode).path.pathkeys = NIL;

    (*pathnode).path.disabled_nodes = (*subpath).disabled_nodes;
    (*pathnode).path.startup_cost = (*subpath).startup_cost;
    (*pathnode).path.total_cost = (*subpath).total_cost;
    if !returning_lists.is_null() {
        (*pathnode).path.rows = (*subpath).rows;
        (*(*pathnode).path.pathtarget).width = (*(*subpath).pathtarget).width;
    } else {
        (*pathnode).path.rows = 0.0;
        (*(*pathnode).path.pathtarget).width = 0;
    }

    (*pathnode).subpath = subpath;
    (*pathnode).operation = operation;
    (*pathnode).canSetTag = can_set_tag;
    (*pathnode).nominalRelation = nominal_relation;
    (*pathnode).rootRelation = root_relation;
    (*pathnode).partColsUpdated = part_cols_updated;
    (*pathnode).resultRelations = result_relations;
    (*pathnode).updateColnosLists = update_colnos_lists;
    (*pathnode).withCheckOptionLists = with_check_option_lists;
    (*pathnode).returningLists = returning_lists;
    (*pathnode).rowMarks = row_marks;
    (*pathnode).onconflict = onconflict;
    (*pathnode).epqParam = epq_param;
    (*pathnode).mergeActionLists = merge_action_lists;
    (*pathnode).mergeJoinConditions = merge_join_conditions;

    /* suppress unused variable warning for root */
    let _ = root;

    pathnode
}

/*
 * create_limit_path
 *	  Creates a pathnode that represents performing LIMIT/OFFSET
 *
 * 'rel' is the parent relation associated with the result
 * 'subpath' is the path representing the source of data
 * 'limitOffset' is the actual OFFSET expression, or NULL
 * 'limitCount' is the actual LIMIT expression, or NULL
 * 'offset_est' is the estimated value of the OFFSET expression
 * 'count_est' is the estimated value of the LIMIT expression
 */
pub unsafe fn create_limit_path(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    subpath: *mut Path,
    limit_offset: *mut crate::nodes::nodes::Node,
    limit_count: *mut crate::nodes::nodes::Node,
    limit_option: LimitOption,
    offset_est: i64,
    count_est: i64,
) -> *mut LimitPath {
    let pathnode: *mut LimitPath = makeNode!(LimitPath, T_LimitPath);

    (*pathnode).path.pathtype = T_Limit;
    (*pathnode).path.parent = rel;
    /* Limit doesn't project, so use source path's pathtarget */
    (*pathnode).path.pathtarget = (*subpath).pathtarget;
    /* For now, assume we are above any joins, so no parameterization */
    (*pathnode).path.param_info = core::ptr::null_mut();
    (*pathnode).path.parallel_aware = false;
    (*pathnode).path.parallel_safe = (*rel).consider_parallel && (*subpath).parallel_safe;
    (*pathnode).path.parallel_workers = (*subpath).parallel_workers;
    (*pathnode).path.rows = (*subpath).rows;
    (*pathnode).path.disabled_nodes = (*subpath).disabled_nodes;
    (*pathnode).path.startup_cost = (*subpath).startup_cost;
    (*pathnode).path.total_cost = (*subpath).total_cost;
    (*pathnode).path.pathkeys = (*subpath).pathkeys;
    (*pathnode).subpath = subpath;
    (*pathnode).limitOffset = limit_offset;
    (*pathnode).limitCount = limit_count;
    (*pathnode).limitOption = limit_option;

    /*
     * Adjust the output rows count and costs according to the offset/limit.
     */
    adjust_limit_rows_costs(
        &raw mut (*pathnode).path.rows,
        &raw mut (*pathnode).path.startup_cost,
        &raw mut (*pathnode).path.total_cost,
        offset_est,
        count_est,
    );

    /* suppress unused variable warning for root */
    let _ = root;

    pathnode
}

/*
 * adjust_limit_rows_costs
 *	  Adjust the size and cost estimates for a LimitPath node according to the
 *	  offset/limit.
 */
pub unsafe fn adjust_limit_rows_costs(
    rows: *mut f64,           /* in/out parameter */
    startup_cost: *mut Cost,  /* in/out parameter */
    total_cost: *mut Cost,    /* in/out parameter */
    offset_est: i64,
    count_est: i64,
) {
    let input_rows: f64 = *rows;
    let input_startup_cost: Cost = *startup_cost;
    let input_total_cost: Cost = *total_cost;

    if offset_est != 0 {
        let offset_rows: f64;
        if offset_est > 0 {
            offset_rows = offset_est as f64;
        } else {
            offset_rows = clamp_row_est(input_rows * 0.10);
        }
        let offset_rows = if offset_rows > *rows { *rows } else { offset_rows };
        if input_rows > 0.0 {
            *startup_cost +=
                (input_total_cost - input_startup_cost) * offset_rows / input_rows;
        }
        *rows -= offset_rows;
        if *rows < 1.0 {
            *rows = 1.0;
        }
    }

    if count_est != 0 {
        let count_rows: f64;
        if count_est > 0 {
            count_rows = count_est as f64;
        } else {
            count_rows = clamp_row_est(input_rows * 0.10);
        }
        let count_rows = if count_rows > *rows { *rows } else { count_rows };
        if input_rows > 0.0 {
            *total_cost = *startup_cost
                + (input_total_cost - input_startup_cost) * count_rows / input_rows;
        }
        *rows = count_rows;
        if *rows < 1.0 {
            *rows = 1.0;
        }
    }
}

/*
 * reparameterize_path
 *		Attempt to modify a Path to have greater parameterization
 *
 * Returns NULL if we can't reparameterize the given path.
 */
pub unsafe fn reparameterize_path(
    root: *mut PlannerInfo,
    path: *mut Path,
    required_outer: Relids,
    loop_count: f64,
) -> *mut Path {
    let rel: *mut RelOptInfo = (*path).parent;

    /* Can only increase, not decrease, path's parameterization */
    if !bms_is_subset(PATH_REQ_OUTER(path), required_outer) {
        return core::ptr::null_mut();
    }
    match (*path).pathtype {
        T_SeqScan => {
            return create_seqscan_path(root, rel, required_outer, 0);
        }
        T_SampleScan => {
            return create_samplescan_path(root, rel, required_outer) as *mut Path;
        }
        T_IndexScan | T_IndexOnlyScan => {
            let ipath: *mut IndexPath = path as *mut IndexPath;
            let newpath: *mut IndexPath = makeNode!(IndexPath, T_IndexPath);
            /*
             * We can't use create_index_path directly.  Instead we hack things
             * a bit: flat-copy the path node, revise its param_info, and redo
             * the cost estimate.
             */
            core::ptr::copy_nonoverlapping(ipath, newpath, 1);
            (*newpath).path.param_info = get_baserel_parampathinfo(root, rel, required_outer);
            cost_index(newpath, root, loop_count, false);
            return newpath as *mut Path;
        }
        T_BitmapHeapScan => {
            let bpath: *mut BitmapHeapPath = path as *mut BitmapHeapPath;
            return create_bitmap_heap_path(
                root,
                rel,
                (*bpath).bitmapqual,
                required_outer,
                loop_count,
                0,
            ) as *mut Path;
        }
        T_SubqueryScan => {
            let spath: *mut SubqueryScanPath = path as *mut SubqueryScanPath;
            let subpath: *mut Path = (*spath).subpath;
            let trivial_pathtarget: bool =
                (*subpath).total_cost == (*spath).path.total_cost;
            return create_subqueryscan_path(
                root,
                rel,
                subpath,
                trivial_pathtarget,
                (*spath).path.pathkeys,
                required_outer,
            ) as *mut Path;
        }
        T_Result => {
            /* Supported only for RTE_RESULT scan paths */
            if IsA!(path, T_Path) {
                return create_resultscan_path(root, rel, required_outer);
            }
            /* else fall through */
        }
        T_Append => {
            let apath: *mut AppendPath = path as *mut AppendPath;
            let mut childpaths: *mut List = NIL;
            let mut partialpaths: *mut List = NIL;
            let mut i: c_int = 0;

            foreach!(lc, (*apath).subpaths, {
                let mut spath: *mut Path = lfirst(current_cell!(lc)) as *mut Path;
                spath = reparameterize_path(root, spath, required_outer, loop_count);
                if spath.is_null() {
                    return core::ptr::null_mut();
                }
                /* We have to re-split the regular and partial paths */
                if i < (*apath).first_partial_path {
                    childpaths = lappend(childpaths, spath as *mut c_void);
                } else {
                    partialpaths = lappend(partialpaths, spath as *mut c_void);
                }
                i += 1;
            });
            return create_append_path(
                root,
                rel,
                childpaths,
                partialpaths,
                (*apath).path.pathkeys,
                required_outer,
                (*apath).path.parallel_workers,
                (*apath).path.parallel_aware,
                -1.0,
            ) as *mut Path;
        }
        T_Material => {
            let mpath: *mut MaterialPath = path as *mut MaterialPath;
            let spath: *mut Path = reparameterize_path(
                root, (*mpath).subpath, required_outer, loop_count,
            );
            if spath.is_null() {
                return core::ptr::null_mut();
            }
            return create_material_path(rel, spath) as *mut Path;
        }
        T_Memoize => {
            let mpath: *mut MemoizePath = path as *mut MemoizePath;
            let spath: *mut Path = reparameterize_path(
                root, (*mpath).subpath, required_outer, loop_count,
            );
            if spath.is_null() {
                return core::ptr::null_mut();
            }
            return create_memoize_path(
                root,
                rel,
                spath,
                (*mpath).param_exprs,
                (*mpath).hash_operators,
                (*mpath).singlerow,
                (*mpath).binary_mode,
                (*mpath).calls,
            ) as *mut Path;
        }
        _ => {}
    }
    core::ptr::null_mut()
}

/*
 * reparameterize_path_by_child
 * 		Given a path parameterized by the parent of the given child relation,
 * 		translate the path to be parameterized by the given child relation.
 *
 * Returns NULL if we can't reparameterize the given path.
 */
pub unsafe fn reparameterize_path_by_child(
    root: *mut PlannerInfo,
    path: *mut Path,
    child_rel: *mut RelOptInfo,
) -> *mut Path {
    // Helper macro equivalents (inline closures):
    // ADJUST_CHILD_ATTRS(node) -> adjust_appendrel_attrs_multilevel(root, node as Node, child_rel, top_parent)
    // REPARAMETERIZE_CHILD_PATH(p) -> reparameterize_path_by_child(root, p, child_rel); return NULL if NULL
    // REPARAMETERIZE_CHILD_PATH_LIST(pl) -> reparameterize_pathlist_by_child(root, pl, child_rel); return NULL if NIL

    let new_path: *mut Path;
    let new_ppi: *mut ParamPathInfo;
    let old_ppi: *mut ParamPathInfo;
    let required_outer: Relids;

    /*
     * If the path is not parameterized by the parent of the given relation,
     * it doesn't need reparameterization.
     */
    if (*path).param_info.is_null()
        || !bms_overlap(PATH_REQ_OUTER(path), (*child_rel).top_parent_relids)
    {
        return path;
    }

    let top_parent: *mut RelOptInfo = (*child_rel).top_parent;

    match crate::nodes::nodes::nodeTag(path as *mut crate::nodes::nodes::Node) {
        T_Path => {
            new_path = path;
            (*(*new_path).parent).baserestrictinfo = adjust_appendrel_attrs_multilevel(
                root,
                (*(*new_path).parent).baserestrictinfo as *mut crate::nodes::nodes::Node,
                child_rel,
                top_parent,
            ) as *mut List;
            if (*path).pathtype == T_SampleScan {
                let scan_relid: Index = (*(*path).parent).relid;
                Assert!(scan_relid > 0);
                let rte: *mut RangeTblEntry = {
                    let arr = (*root).simple_rte_array;
                    *arr.add(scan_relid as usize)
                };
                Assert!((*rte).rtekind == RTE_RELATION);
                Assert!(!(*rte).tablesample.is_null());
                (*rte).tablesample = adjust_appendrel_attrs_multilevel(
                    root,
                    (*rte).tablesample as *mut crate::nodes::nodes::Node,
                    child_rel,
                    top_parent,
                ) as *mut crate::nodes::parsenodes::TableSampleClause;
            }
        }
        T_IndexPath => {
            let ipath: *mut IndexPath = path as *mut IndexPath;
            (*(*ipath).indexinfo).indrestrictinfo = adjust_appendrel_attrs_multilevel(
                root,
                (*(*ipath).indexinfo).indrestrictinfo as *mut crate::nodes::nodes::Node,
                child_rel,
                top_parent,
            ) as *mut List;
            (*ipath).indexclauses = adjust_appendrel_attrs_multilevel(
                root,
                (*ipath).indexclauses as *mut crate::nodes::nodes::Node,
                child_rel,
                top_parent,
            ) as *mut List;
            new_path = path;
        }
        T_BitmapHeapScan => {
            let bhpath: *mut BitmapHeapPath = path as *mut BitmapHeapPath;
            (*(*bhpath).path.parent).baserestrictinfo = adjust_appendrel_attrs_multilevel(
                root,
                (*(*bhpath).path.parent).baserestrictinfo as *mut crate::nodes::nodes::Node,
                child_rel,
                top_parent,
            ) as *mut List;
            (*bhpath).bitmapqual =
                reparameterize_path_by_child(root, (*bhpath).bitmapqual, child_rel);
            if (*bhpath).bitmapqual.is_null() {
                return core::ptr::null_mut();
            }
            new_path = path;
        }
        T_BitmapAndPath => {
            let bapath: *mut BitmapAndPath = path as *mut BitmapAndPath;
            if !(*bapath).bitmapquals.is_null() {
                (*bapath).bitmapquals =
                    reparameterize_pathlist_by_child(root, (*bapath).bitmapquals, child_rel);
                if (*bapath).bitmapquals.is_null() {
                    return core::ptr::null_mut();
                }
            }
            new_path = path;
        }
        T_BitmapOrPath => {
            let bopath: *mut BitmapOrPath = path as *mut BitmapOrPath;
            if !(*bopath).bitmapquals.is_null() {
                (*bopath).bitmapquals =
                    reparameterize_pathlist_by_child(root, (*bopath).bitmapquals, child_rel);
                if (*bopath).bitmapquals.is_null() {
                    return core::ptr::null_mut();
                }
            }
            new_path = path;
        }
        T_ForeignPath => {
            let fpath: *mut ForeignPath = path as *mut ForeignPath;
            (*(*fpath).path.parent).baserestrictinfo = adjust_appendrel_attrs_multilevel(
                root,
                (*(*fpath).path.parent).baserestrictinfo as *mut crate::nodes::nodes::Node,
                child_rel,
                top_parent,
            ) as *mut List;
            if !(*fpath).fdw_outerpath.is_null() {
                (*fpath).fdw_outerpath =
                    reparameterize_path_by_child(root, (*fpath).fdw_outerpath, child_rel);
                if (*fpath).fdw_outerpath.is_null() {
                    return core::ptr::null_mut();
                }
            }
            if !(*fpath).fdw_restrictinfo.is_null() {
                (*fpath).fdw_restrictinfo = adjust_appendrel_attrs_multilevel(
                    root,
                    (*fpath).fdw_restrictinfo as *mut crate::nodes::nodes::Node,
                    child_rel,
                    top_parent,
                ) as *mut List;
            }
            /* Hand over to FDW if needed */
            {
                let fdwroutine: *mut crate::foreign::fdwapi::FdwRoutine =
                    (*(*path).parent).fdwroutine as *mut crate::foreign::fdwapi::FdwRoutine;
                let rfpc_func: crate::foreign::fdwapi::ReparameterizeForeignPathByChild_function =
                    (*fdwroutine).ReparameterizeForeignPathByChild;
                if let Some(f) = rfpc_func {
                    (*fpath).fdw_private = f(root as *mut c_void, (*fpath).fdw_private, child_rel as *mut c_void);
                }
            }
            new_path = path;
        }
        T_CustomPath => {
            let cpath: *mut CustomPath = path as *mut CustomPath;
            (*(*cpath).path.parent).baserestrictinfo = adjust_appendrel_attrs_multilevel(
                root,
                (*(*cpath).path.parent).baserestrictinfo as *mut crate::nodes::nodes::Node,
                child_rel,
                top_parent,
            ) as *mut List;
            if !(*cpath).custom_paths.is_null() {
                (*cpath).custom_paths =
                    reparameterize_pathlist_by_child(root, (*cpath).custom_paths, child_rel);
                if (*cpath).custom_paths.is_null() {
                    return core::ptr::null_mut();
                }
            }
            if !(*cpath).custom_restrictinfo.is_null() {
                (*cpath).custom_restrictinfo = adjust_appendrel_attrs_multilevel(
                    root,
                    (*cpath).custom_restrictinfo as *mut crate::nodes::nodes::Node,
                    child_rel,
                    top_parent,
                ) as *mut List;
            }
            /* CustomPath methods are opaque; no ReparameterizeCustomPathByChild callback yet */
            /* TODO(pg-port): call cpath->methods->ReparameterizeCustomPathByChild if available */
            new_path = path;
        }
        T_NestPath => {
            let npath: *mut NestPath = path as *mut NestPath;
            let jpath: *mut JoinPath = npath as *mut JoinPath;
            (*jpath).outerjoinpath =
                reparameterize_path_by_child(root, (*jpath).outerjoinpath, child_rel);
            if (*jpath).outerjoinpath.is_null() {
                return core::ptr::null_mut();
            }
            (*jpath).innerjoinpath =
                reparameterize_path_by_child(root, (*jpath).innerjoinpath, child_rel);
            if (*jpath).innerjoinpath.is_null() {
                return core::ptr::null_mut();
            }
            (*jpath).joinrestrictinfo = adjust_appendrel_attrs_multilevel(
                root,
                (*jpath).joinrestrictinfo as *mut crate::nodes::nodes::Node,
                child_rel,
                top_parent,
            ) as *mut List;
            new_path = path;
        }
        T_MergePath => {
            let mpath: *mut MergePath = path as *mut MergePath;
            let jpath: *mut JoinPath = mpath as *mut JoinPath;
            (*jpath).outerjoinpath =
                reparameterize_path_by_child(root, (*jpath).outerjoinpath, child_rel);
            if (*jpath).outerjoinpath.is_null() {
                return core::ptr::null_mut();
            }
            (*jpath).innerjoinpath =
                reparameterize_path_by_child(root, (*jpath).innerjoinpath, child_rel);
            if (*jpath).innerjoinpath.is_null() {
                return core::ptr::null_mut();
            }
            (*jpath).joinrestrictinfo = adjust_appendrel_attrs_multilevel(
                root,
                (*jpath).joinrestrictinfo as *mut crate::nodes::nodes::Node,
                child_rel,
                top_parent,
            ) as *mut List;
            (*mpath).path_mergeclauses = adjust_appendrel_attrs_multilevel(
                root,
                (*mpath).path_mergeclauses as *mut crate::nodes::nodes::Node,
                child_rel,
                top_parent,
            ) as *mut List;
            new_path = path;
        }
        T_HashPath => {
            let hpath: *mut HashPath = path as *mut HashPath;
            let jpath: *mut JoinPath = hpath as *mut JoinPath;
            (*jpath).outerjoinpath =
                reparameterize_path_by_child(root, (*jpath).outerjoinpath, child_rel);
            if (*jpath).outerjoinpath.is_null() {
                return core::ptr::null_mut();
            }
            (*jpath).innerjoinpath =
                reparameterize_path_by_child(root, (*jpath).innerjoinpath, child_rel);
            if (*jpath).innerjoinpath.is_null() {
                return core::ptr::null_mut();
            }
            (*jpath).joinrestrictinfo = adjust_appendrel_attrs_multilevel(
                root,
                (*jpath).joinrestrictinfo as *mut crate::nodes::nodes::Node,
                child_rel,
                top_parent,
            ) as *mut List;
            (*hpath).path_hashclauses = adjust_appendrel_attrs_multilevel(
                root,
                (*hpath).path_hashclauses as *mut crate::nodes::nodes::Node,
                child_rel,
                top_parent,
            ) as *mut List;
            new_path = path;
        }
        T_AppendPath => {
            let apath: *mut AppendPath = path as *mut AppendPath;
            if !(*apath).subpaths.is_null() {
                (*apath).subpaths =
                    reparameterize_pathlist_by_child(root, (*apath).subpaths, child_rel);
                if (*apath).subpaths.is_null() {
                    return core::ptr::null_mut();
                }
            }
            new_path = path;
        }
        T_MaterialPath => {
            let mpath: *mut MaterialPath = path as *mut MaterialPath;
            (*mpath).subpath =
                reparameterize_path_by_child(root, (*mpath).subpath, child_rel);
            if (*mpath).subpath.is_null() {
                return core::ptr::null_mut();
            }
            new_path = path;
        }
        T_MemoizePath => {
            let mpath: *mut MemoizePath = path as *mut MemoizePath;
            (*mpath).subpath =
                reparameterize_path_by_child(root, (*mpath).subpath, child_rel);
            if (*mpath).subpath.is_null() {
                return core::ptr::null_mut();
            }
            (*mpath).param_exprs = adjust_appendrel_attrs_multilevel(
                root,
                (*mpath).param_exprs as *mut crate::nodes::nodes::Node,
                child_rel,
                top_parent,
            ) as *mut List;
            new_path = path;
        }
        T_GatherPath => {
            let gpath: *mut GatherPath = path as *mut GatherPath;
            (*gpath).subpath =
                reparameterize_path_by_child(root, (*gpath).subpath, child_rel);
            if (*gpath).subpath.is_null() {
                return core::ptr::null_mut();
            }
            new_path = path;
        }
        _ => {
            /* We don't know how to reparameterize this path. */
            return core::ptr::null_mut();
        }
    }

    /*
     * Adjust the parameterization information, which refers to the topmost
     * parent.
     */
    old_ppi = (*new_path).param_info;
    required_outer = adjust_child_relids_multilevel(
        root,
        (*old_ppi).ppi_req_outer,
        child_rel,
        (*child_rel).top_parent,
    );

    /* If we already have a PPI for this parameterization, just return it */
    new_ppi = find_param_path_info((*new_path).parent, required_outer);

    /*
     * If not, build a new one and link it to the list of PPIs.
     */
    let new_ppi = if new_ppi.is_null() {
        let oldcontext: crate::utils::palloc::MemoryContext;
        let rel2: *mut RelOptInfo = (*path).parent;
        oldcontext = MemoryContextSwitchTo(
            GetMemoryChunkContext(rel2 as *mut c_void),
        );
        let nppi: *mut ParamPathInfo = makeNode!(ParamPathInfo, T_ParamPathInfo);
        (*nppi).ppi_req_outer = bms_copy(required_outer);
        (*nppi).ppi_rows = (*old_ppi).ppi_rows;
        (*nppi).ppi_clauses = (*old_ppi).ppi_clauses;
        (*nppi).ppi_clauses = adjust_appendrel_attrs_multilevel(
            root,
            (*nppi).ppi_clauses as *mut crate::nodes::nodes::Node,
            child_rel,
            top_parent,
        ) as *mut List;
        (*nppi).ppi_serials = bms_copy((*old_ppi).ppi_serials);
        (*rel2).ppilist = lappend((*rel2).ppilist, nppi as *mut c_void);
        MemoryContextSwitchTo(oldcontext);
        nppi
    } else {
        new_ppi
    };
    bms_free(required_outer);

    (*new_path).param_info = new_ppi;

    /*
     * Adjust the path target if the parent of the outer relation is
     * referenced in the targetlist.
     */
    if bms_overlap((*(*path).parent).lateral_relids, (*child_rel).top_parent_relids) {
        (*new_path).pathtarget = copy_pathtarget((*new_path).pathtarget);
        (*(*new_path).pathtarget).exprs = adjust_appendrel_attrs_multilevel(
            root,
            (*(*new_path).pathtarget).exprs as *mut crate::nodes::nodes::Node,
            child_rel,
            top_parent,
        ) as *mut List;
    }

    new_path
}

/*
 * path_is_reparameterizable_by_child
 * 		Given a path parameterized by the parent of the given child relation,
 * 		see if it can be translated to be parameterized by the child relation.
 */
pub unsafe fn path_is_reparameterizable_by_child(
    path: *mut Path,
    child_rel: *mut RelOptInfo,
) -> bool {
    /*
     * If the path is not parameterized by the parent of the given relation,
     * it doesn't need reparameterization.
     */
    if (*path).param_info.is_null()
        || !bms_overlap(PATH_REQ_OUTER(path), (*child_rel).top_parent_relids)
    {
        return true;
    }

    match crate::nodes::nodes::nodeTag(path as *mut crate::nodes::nodes::Node) {
        T_Path | T_IndexPath => {
            /* these are always reparameterizable */
        }
        T_BitmapHeapScan => {
            let bhpath: *mut BitmapHeapPath = path as *mut BitmapHeapPath;
            if !path_is_reparameterizable_by_child((*bhpath).bitmapqual, child_rel) {
                return false;
            }
        }
        T_BitmapAndPath => {
            let bapath: *mut BitmapAndPath = path as *mut BitmapAndPath;
            if !pathlist_is_reparameterizable_by_child((*bapath).bitmapquals, child_rel) {
                return false;
            }
        }
        T_BitmapOrPath => {
            let bopath: *mut BitmapOrPath = path as *mut BitmapOrPath;
            if !pathlist_is_reparameterizable_by_child((*bopath).bitmapquals, child_rel) {
                return false;
            }
        }
        T_ForeignPath => {
            let fpath: *mut ForeignPath = path as *mut ForeignPath;
            if !(*fpath).fdw_outerpath.is_null()
                && !path_is_reparameterizable_by_child((*fpath).fdw_outerpath, child_rel)
            {
                return false;
            }
        }
        T_CustomPath => {
            let cpath: *mut CustomPath = path as *mut CustomPath;
            if !pathlist_is_reparameterizable_by_child((*cpath).custom_paths, child_rel) {
                return false;
            }
        }
        T_NestPath | T_MergePath | T_HashPath => {
            let jpath: *mut JoinPath = path as *mut JoinPath;
            if !path_is_reparameterizable_by_child((*jpath).outerjoinpath, child_rel) {
                return false;
            }
            if !path_is_reparameterizable_by_child((*jpath).innerjoinpath, child_rel) {
                return false;
            }
        }
        T_AppendPath => {
            let apath: *mut AppendPath = path as *mut AppendPath;
            if !pathlist_is_reparameterizable_by_child((*apath).subpaths, child_rel) {
                return false;
            }
        }
        T_MaterialPath => {
            let mpath: *mut MaterialPath = path as *mut MaterialPath;
            if !path_is_reparameterizable_by_child((*mpath).subpath, child_rel) {
                return false;
            }
        }
        T_MemoizePath => {
            let mpath: *mut MemoizePath = path as *mut MemoizePath;
            if !path_is_reparameterizable_by_child((*mpath).subpath, child_rel) {
                return false;
            }
        }
        T_GatherPath => {
            let gpath: *mut GatherPath = path as *mut GatherPath;
            if !path_is_reparameterizable_by_child((*gpath).subpath, child_rel) {
                return false;
            }
        }
        _ => {
            /* We don't know how to reparameterize this path. */
            return false;
        }
    }

    true
}

/*
 * reparameterize_pathlist_by_child
 * 		Helper function to reparameterize a list of paths by given child rel.
 *
 * Returns NIL to indicate failure, so pathlist had better not be NIL.
 */
unsafe fn reparameterize_pathlist_by_child(
    root: *mut PlannerInfo,
    pathlist: *mut List,
    child_rel: *mut RelOptInfo,
) -> *mut List {
    let mut result: *mut List = NIL;

    foreach!(lc, pathlist, {
        let path: *mut Path =
            reparameterize_path_by_child(root, lfirst(current_cell!(lc)) as *mut Path, child_rel);
        if path.is_null() {
            list_free(result);
            return NIL;
        }
        result = lappend(result, path as *mut c_void);
    });

    result
}

/*
 * pathlist_is_reparameterizable_by_child
 *		Helper function to check if a list of paths can be reparameterized.
 */
unsafe fn pathlist_is_reparameterizable_by_child(
    pathlist: *mut List,
    child_rel: *mut RelOptInfo,
) -> bool {
    foreach!(lc, pathlist, {
        let path: *mut Path = lfirst(current_cell!(lc)) as *mut Path;
        if !path_is_reparameterizable_by_child(path, child_rel) {
            return false;
        }
    });
    true
}
