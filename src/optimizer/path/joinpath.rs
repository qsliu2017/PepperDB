//! joinpath.rs
//!   Routines to find all possible paths for processing a set of joins
//!
//! Translated 1:1 from postgres/src/backend/optimizer/path/joinpath.c
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!   src/backend/optimizer/path/joinpath.c
//!
//! #include mapping:
//!   "postgres.h"              -> crate::prelude::*
//!   "executor/executor.h"     -> ExecMaterializesOutput (costsize.rs stub)
//!   "foreign/fdwapi.h"        -> FdwRoutine (pathnodes.rs)
//!   "nodes/nodeFuncs.h"       -> exprType (local stub)
//!   "optimizer/cost.h"        -> enable_* GUC booleans (crate::optimizer::cost)
//!   "optimizer/optimizer.h"   -> contain_volatile_functions/pull_varnos/pull_vars_of_level
//!                               (crate::optimizer::optimizer / util::var / util::clauses)
//!   "optimizer/pathnode.h"    -> add_path/create_*_path/calc_* (crate::optimizer::util::pathnode)
//!   "optimizer/paths.h"       -> pathkeys helpers, innerrel_is_unique (crate::optimizer::paths /
//!                               path::pathkeys)
//!   "optimizer/placeholder.h" -> find_placeholder_info (crate::optimizer::util::placeholder)
//!   "optimizer/planmain.h"    -> (nothing direct)
//!   "optimizer/restrictinfo.h"-> clause_sides_match_join, update_mergeclause_eclasses (stubs)
//!   "utils/lsyscache.h"       -> get_commutator (local stub)
//!   "utils/typcache.h"        -> TypeCacheEntry / lookup_type_cache / TYPECACHE_* (local stubs)

use crate::prelude::*;
use core::ffi::c_void;

use crate::{current_cell, foreach, foreach_node};

use crate::nodes::bitmapset::{
    bms_add_members, bms_difference, bms_free, bms_intersect, bms_is_empty, bms_is_member,
    bms_is_subset, bms_join, bms_membership, bms_nonempty_difference, bms_overlap,
    Bitmapset, BMS_Membership::BMS_MULTIPLE,
};
use crate::nodes::nodes::{Node, NodeTag};
use crate::nodes::nodes::JoinType::{
    self, JOIN_ANTI, JOIN_FULL, JOIN_INNER, JOIN_LEFT, JOIN_RIGHT, JOIN_RIGHT_ANTI,
    JOIN_RIGHT_SEMI, JOIN_SEMI, JOIN_UNIQUE_INNER, JOIN_UNIQUE_OUTER,
};
use crate::nodes::pathnodes::{
    CostSelector::STARTUP_COST, CostSelector::TOTAL_COST,
    JoinCostWorkspace, JoinPathExtraData, ParamPathInfo, Path, PlannerInfo,
    PlaceHolderInfo, PlaceHolderVar, RelOptInfo, Relids, RestrictInfo, SpecialJoinInfo,
    EC_MUST_BE_REDUNDANT, IS_SIMPLE_REL, PATH_REQ_OUTER, RELOPT_OTHER_JOINREL,
};
use crate::optimizer::path::costsize::RINFO_IS_PUSHED_DOWN;
use crate::nodes::pg_list::{
    lappend, lappend_oid, lfirst, linitial, list_concat, list_copy,
    list_delete_nth_cell, list_free, list_head, list_length, list_member,
    list_truncate, lcons, lsecond, List, ListCell, NIL,
};
use crate::nodes::primnodes::{Expr, OpExpr, Var};
use crate::postgres_ext::Oid;

use crate::optimizer::cost::{
    enable_hashjoin, enable_material, enable_memoize, enable_mergejoin,
    enable_parallel_hash,
};
use crate::optimizer::path::pathkeys::{
    build_join_pathkeys, find_mergeclauses_for_outer_pathkeys, get_cheapest_parallel_safe_total_inner,
    get_cheapest_path_for_pathkeys, make_inner_pathkeys_for_merge,
    pathkeys_contained_in, pathkeys_count_contained_in, select_outer_pathkeys_for_merge,
    trim_mergeclauses_for_inner_pathkeys, update_mergeclause_eclasses,
};
use crate::optimizer::path::costsize::{
    ExecMaterializesOutput, IS_OUTER_JOIN,
    initial_cost_hashjoin, initial_cost_mergejoin, initial_cost_nestloop,
};
use crate::optimizer::util::pathnode::{
    add_partial_path, add_partial_path_precheck, add_path, add_path_precheck,
    calc_nestloop_required_outer, calc_non_nestloop_required_outer,
    compare_path_costs, create_hashjoin_path, create_material_path,
    create_memoize_path, create_mergejoin_path, create_nestloop_path,
    create_unique_path, path_is_reparameterizable_by_child,
};
use crate::optimizer::util::placeholder::find_placeholder_info;
use crate::optimizer::util::var::{pull_varnos, pull_vars_of_level};
use crate::optimizer::util::clauses::contain_volatile_functions;

// ---------------------------------------------------------------------------
// Local stubs for functions not yet ported from other translation units.
// ---------------------------------------------------------------------------

/// innerrel_is_unique -- can inner rel produce at most 1 matching row?
/// TODO(pg-port): real impl in optimizer/util/plancat.c (selfuncs-adjacent)
unsafe fn innerrel_is_unique(
    _root: *mut PlannerInfo,
    _joinrelids: Relids,
    _outerrelids: Relids,
    _innerrel: *mut RelOptInfo,
    _jointype: JoinType,
    _restrictlist: *mut List,
    _force: bool,
) -> bool {
    false /* TODO(pg-port) */
}

/// compute_semi_anti_join_factors -- fill in extra.semifactors.
/// TODO(pg-port): lives in optimizer/path/costsize.c
unsafe fn compute_semi_anti_join_factors(
    _root: *mut PlannerInfo,
    _joinrel: *mut RelOptInfo,
    _outerrel: *mut RelOptInfo,
    _innerrel: *mut RelOptInfo,
    _jointype: JoinType,
    _sjinfo: *mut SpecialJoinInfo,
    _restrictlist: *mut List,
    _semifactors: *mut crate::nodes::pathnodes::SemiAntiJoinFactors,
) {
    /* TODO(pg-port) */
}

/// clause_sides_match_join -- check outer/inner rel membership for a clause.
/// TODO(pg-port): optimizer/util/restrictinfo.c
unsafe fn clause_sides_match_join(
    _rinfo: *mut RestrictInfo,
    _outerrelids: Relids,
    _innerrelids: Relids,
) -> bool {
    true /* TODO(pg-port) */
}

/// get_commutator -- fetch commutator opno from pg_operator.
/// TODO(pg-port): utils/cache/lsyscache.c
unsafe fn get_commutator(_opno: Oid) -> Oid {
    0 /* TODO(pg-port) */
}

/// OidIsValid macro equivalent.
#[inline]
fn OidIsValid(oid: Oid) -> bool {
    oid != InvalidOid
}

const InvalidOid: Oid = 0;

// TypeCacheEntry and related constants (utils/typcache.h, local stub).
const TYPECACHE_HASH_PROC: c_int = 0x0010;
const TYPECACHE_EQ_OPR: c_int = 0x0200;

#[repr(C)]
struct TypeCacheEntry {
    pub hash_proc: Oid,
    pub eq_opr: Oid,
}

/// lookup_type_cache -- fetch or build type cache entry.
/// TODO(pg-port): utils/cache/typcache.c
unsafe fn lookup_type_cache(_typeid: Oid, _flags: c_int) -> *mut TypeCacheEntry {
    unimplemented!("TODO(pg-port): lookup_type_cache")
}

/// exprType -- return type OID of given expression node.
/// TODO(pg-port): nodes/nodeFuncs.c
unsafe fn exprType(_expr: *const Node) -> Oid {
    0 /* TODO(pg-port) */
}

/// fdw_GetForeignJoinPaths -- call FDW's GetForeignJoinPaths callback.
/// TODO(pg-port): foreign/fdwapi.c; FdwRoutine is opaque until fdwapi.h is ported.
unsafe fn fdw_GetForeignJoinPaths(
    _fdwroutine: *mut crate::nodes::pathnodes::FdwRoutine,
    _root: *mut PlannerInfo,
    _joinrel: *mut RelOptInfo,
    _outerrel: *mut RelOptInfo,
    _innerrel: *mut RelOptInfo,
    _jointype: JoinType,
    _extra: *mut JoinPathExtraData,
) {
    /* TODO(pg-port) */
}

// ---------------------------------------------------------------------------
// Hook type and global for plugin extensibility.
// ---------------------------------------------------------------------------

/// Type of the set_join_pathlist hook (optimizer/paths.h).
pub type set_join_pathlist_hook_type = unsafe fn(
    root: *mut PlannerInfo,
    joinrel: *mut RelOptInfo,
    outerrel: *mut RelOptInfo,
    innerrel: *mut RelOptInfo,
    jointype: JoinType,
    extra: *mut JoinPathExtraData,
);

/* Hook for plugins to get control in add_paths_to_joinrel() */
pub static mut set_join_pathlist_hook: Option<set_join_pathlist_hook_type> = None;

// ---------------------------------------------------------------------------
// Macros translated to inline fns.
// ---------------------------------------------------------------------------

/// PATH_PARAM_BY_PARENT(path, rel) -- path parameterized by rel's parent?
#[inline]
unsafe fn PATH_PARAM_BY_PARENT(path: *const Path, rel: *mut RelOptInfo) -> bool {
    !(*path).param_info.is_null()
        && bms_overlap(PATH_REQ_OUTER(path), (*rel).top_parent_relids)
}

/// PATH_PARAM_BY_REL_SELF(path, rel)
#[inline]
unsafe fn PATH_PARAM_BY_REL_SELF(path: *const Path, rel: *mut RelOptInfo) -> bool {
    !(*path).param_info.is_null() && bms_overlap(PATH_REQ_OUTER(path), (*rel).relids)
}

/// PATH_PARAM_BY_REL(path, rel) -- either parent or self parameterization
#[inline]
unsafe fn PATH_PARAM_BY_REL(path: *const Path, rel: *mut RelOptInfo) -> bool {
    PATH_PARAM_BY_REL_SELF(path, rel) || PATH_PARAM_BY_PARENT(path, rel)
}

// ---------------------------------------------------------------------------
// add_paths_to_joinrel
// ---------------------------------------------------------------------------

/*
 * add_paths_to_joinrel
 *    Given a join relation and two component rels from which it can be made,
 *    consider all possible paths that use the two component rels as outer
 *    and inner rel respectively.  Add these paths to the join rel's pathlist
 *    if they survive comparison with other paths (and remove any existing
 *    paths that are dominated by these paths).
 *
 * Modifies the pathlist field of the joinrel node to contain the best
 * paths found so far.
 *
 * jointype is not necessarily the same as sjinfo->jointype; it might be
 * "flipped around" if we are considering joining the rels in the opposite
 * direction from what's indicated in sjinfo.
 *
 * Also, this routine and others in this module accept the special JoinTypes
 * JOIN_UNIQUE_OUTER and JOIN_UNIQUE_INNER to indicate that we should
 * unique-ify the outer or inner relation and then apply a regular inner
 * join.  These values are not allowed to propagate outside this module,
 * however.  Path cost estimation code may need to recognize that it's
 * dealing with such a case --- the combination of nominal jointype INNER
 * with sjinfo->jointype == JOIN_SEMI indicates that.
 */
pub unsafe fn add_paths_to_joinrel(
    root: *mut PlannerInfo,
    joinrel: *mut RelOptInfo,
    outerrel: *mut RelOptInfo,
    innerrel: *mut RelOptInfo,
    jointype: JoinType,
    sjinfo: *mut SpecialJoinInfo,
    restrictlist: *mut List,
) {
    let mut extra = JoinPathExtraData {
        restrictlist: core::ptr::null_mut(),
        mergeclause_list: NIL,
        inner_unique: false,
        sjinfo: core::ptr::null_mut(),
        semifactors: crate::nodes::pathnodes::SemiAntiJoinFactors {
            outer_match_frac: 0.0,
            match_count: 0.0,
        },
        param_source_rels: core::ptr::null_mut(),
    };
    let mut mergejoin_allowed = true;
    let mut lc: crate::nodes::pg_list::ForEachState;

    /*
     * PlannerInfo doesn't contain the SpecialJoinInfos created for joins
     * between child relations, even if there is a SpecialJoinInfo node for
     * the join between the topmost parents. So, while calculating Relids set
     * representing the restriction, consider relids of topmost parent of
     * partitions.
     */
    let joinrelids: Relids = if (*joinrel).reloptkind == RELOPT_OTHER_JOINREL {
        (*joinrel).top_parent_relids
    } else {
        (*joinrel).relids
    };

    extra.restrictlist = restrictlist;
    extra.mergeclause_list = NIL;
    extra.sjinfo = sjinfo;
    extra.param_source_rels = core::ptr::null_mut();

    /*
     * See if the inner relation is provably unique for this outer rel.
     *
     * We have some special cases: for JOIN_SEMI and JOIN_ANTI, it doesn't
     * matter since the executor can make the equivalent optimization anyway;
     * we need not expend planner cycles on proofs.  For JOIN_UNIQUE_INNER, we
     * must be considering a semijoin whose inner side is not provably unique
     * (else reduce_unique_semijoins would've simplified it), so there's no
     * point in calling innerrel_is_unique.  However, if the LHS covers all of
     * the semijoin's min_lefthand, then it's appropriate to set inner_unique
     * because the path produced by create_unique_path will be unique relative
     * to the LHS.  (If we have an LHS that's only part of the min_lefthand,
     * that is *not* true.)  For JOIN_UNIQUE_OUTER, pass JOIN_INNER to avoid
     * letting that value escape this module.
     */
    extra.inner_unique = match jointype {
        JOIN_SEMI | JOIN_ANTI => {
            /*
             * XXX it may be worth proving this to allow a Memoize to be
             * considered for Nested Loop Semi/Anti Joins.
             */
            false /* well, unproven */
        }
        JOIN_UNIQUE_INNER => bms_is_subset((*sjinfo).min_lefthand, (*outerrel).relids),
        JOIN_UNIQUE_OUTER => innerrel_is_unique(
            root,
            (*joinrel).relids,
            (*outerrel).relids,
            innerrel,
            JOIN_INNER,
            restrictlist,
            false,
        ),
        _ => innerrel_is_unique(
            root,
            (*joinrel).relids,
            (*outerrel).relids,
            innerrel,
            jointype,
            restrictlist,
            false,
        ),
    };

    /*
     * Find potential mergejoin clauses.  We can skip this if we are not
     * interested in doing a mergejoin.  However, mergejoin may be our only
     * way of implementing a full outer join, so override enable_mergejoin if
     * it's a full join.
     */
    if enable_mergejoin || jointype == JOIN_FULL {
        extra.mergeclause_list = select_mergejoin_clauses(
            root,
            joinrel,
            outerrel,
            innerrel,
            restrictlist,
            jointype,
            &mut mergejoin_allowed,
        );
    }

    /*
     * If it's SEMI, ANTI, or inner_unique join, compute correction factors
     * for cost estimation.  These will be the same for all paths.
     */
    if jointype == JOIN_SEMI || jointype == JOIN_ANTI || extra.inner_unique {
        compute_semi_anti_join_factors(
            root,
            joinrel,
            outerrel,
            innerrel,
            jointype,
            sjinfo,
            restrictlist,
            &mut extra.semifactors,
        );
    }

    /*
     * Decide whether it's sensible to generate parameterized paths for this
     * joinrel, and if so, which relations such paths should require.  There
     * is usually no need to create a parameterized result path unless there
     * is a join order restriction that prevents joining one of our input rels
     * directly to the parameter source rel instead of joining to the other
     * input rel.  (But see allow_star_schema_join().)	This restriction
     * reduces the number of parameterized paths we have to deal with at
     * higher join levels, without compromising the quality of the resulting
     * plan.  We express the restriction as a Relids set that must overlap the
     * parameterization of any proposed join path.  Note: param_source_rels
     * should contain only baserels, not OJ relids, so starting from
     * all_baserels not all_query_rels is correct.
     */
    foreach!(lc, (*root).join_info_list, {
        let sjinfo2 = lfirst(crate::current_cell!(lc)) as *mut SpecialJoinInfo;

        /*
         * SJ is relevant to this join if we have some part of its RHS
         * (possibly not all of it), and haven't yet joined to its LHS.  (This
         * test is pretty simplistic, but should be sufficient considering the
         * join has already been proven legal.)  If the SJ is relevant, it
         * presents constraints for joining to anything not in its RHS.
         */
        if bms_overlap(joinrelids, (*sjinfo2).min_righthand)
            && !bms_overlap(joinrelids, (*sjinfo2).min_lefthand)
        {
            extra.param_source_rels = bms_join(
                extra.param_source_rels,
                bms_difference((*root).all_baserels, (*sjinfo2).min_righthand),
            );
        }

        /* full joins constrain both sides symmetrically */
        if (*sjinfo2).jointype == JOIN_FULL
            && bms_overlap(joinrelids, (*sjinfo2).min_lefthand)
            && !bms_overlap(joinrelids, (*sjinfo2).min_righthand)
        {
            extra.param_source_rels = bms_join(
                extra.param_source_rels,
                bms_difference((*root).all_baserels, (*sjinfo2).min_lefthand),
            );
        }
    });

    /*
     * However, when a LATERAL subquery is involved, there will simply not be
     * any paths for the joinrel that aren't parameterized by whatever the
     * subquery is parameterized by, unless its parameterization is resolved
     * within the joinrel.  So we might as well allow additional dependencies
     * on whatever residual lateral dependencies the joinrel will have.
     */
    extra.param_source_rels =
        bms_add_members(extra.param_source_rels, (*joinrel).lateral_relids);

    /*
     * 1. Consider mergejoin paths where both relations must be explicitly
     * sorted.  Skip this if we can't mergejoin.
     */
    if mergejoin_allowed {
        sort_inner_and_outer(root, joinrel, outerrel, innerrel, jointype, &mut extra);
    }

    /*
     * 2. Consider paths where the outer relation need not be explicitly
     * sorted. This includes both nestloops and mergejoins where the outer
     * path is already ordered.  Again, skip this if we can't mergejoin.
     * (That's okay because we know that nestloop can't handle
     * right/right-anti/right-semi/full joins at all, so it wouldn't work in
     * the prohibited cases either.)
     */
    if mergejoin_allowed {
        match_unsorted_outer(root, joinrel, outerrel, innerrel, jointype, &mut extra);
    }

    /* 3. (diked out as redundant 2/13/2000 -- tgl) */

    /*
     * 4. Consider paths where both outer and inner relations must be hashed
     * before being joined.  As above, disregard enable_hashjoin for full
     * joins, because there may be no other alternative.
     */
    if enable_hashjoin || jointype == JOIN_FULL {
        hash_inner_and_outer(root, joinrel, outerrel, innerrel, jointype, &mut extra);
    }

    /*
     * 5. If inner and outer relations are foreign tables (or joins) belonging
     * to the same server and assigned to the same user to check access
     * permissions as, give the FDW a chance to push down joins.
     */
    if !(*joinrel).fdwroutine.is_null() {
        // FdwRoutine is opaque; real GetForeignJoinPaths dispatch done via fdwapi.h
        // TODO(pg-port): call (*joinrel).fdwroutine->GetForeignJoinPaths when fdwapi.h is ported
        fdw_GetForeignJoinPaths(
            (*joinrel).fdwroutine,
            root,
            joinrel,
            outerrel,
            innerrel,
            jointype,
            &mut extra,
        );
    }

    /*
     * 6. Finally, give extensions a chance to manipulate the path list.  They
     * could add new paths (such as CustomPaths) by calling add_path(), or
     * add_partial_path() if parallel aware.  They could also delete or modify
     * paths added by the core code.
     */
    if let Some(hook) = set_join_pathlist_hook {
        hook(root, joinrel, outerrel, innerrel, jointype, &mut extra);
    }
}

// ---------------------------------------------------------------------------
// allow_star_schema_join
// ---------------------------------------------------------------------------

/*
 * We override the param_source_rels heuristic to accept nestloop paths in
 * which the outer rel satisfies some but not all of the inner path's
 * parameterization.  This is necessary to get good plans for star-schema
 * scenarios, in which a parameterized path for a large table may require
 * parameters from multiple small tables that will not get joined directly to
 * each other.  We can handle that by stacking nestloops that have the small
 * tables on the outside; but this breaks the rule the param_source_rels
 * heuristic is based on, namely that parameters should not be passed down
 * across joins unless there's a join-order-constraint-based reason to do so.
 * So we ignore the param_source_rels restriction when this case applies.
 *
 * allow_star_schema_join() returns true if the param_source_rels restriction
 * should be overridden, ie, it's okay to perform this join.
 */
#[inline]
unsafe fn allow_star_schema_join(
    _root: *mut PlannerInfo,
    outerrelids: Relids,
    inner_paramrels: Relids,
) -> bool {
    /*
     * It's a star-schema case if the outer rel provides some but not all of
     * the inner rel's parameterization.
     */
    bms_overlap(inner_paramrels, outerrelids)
        && bms_nonempty_difference(inner_paramrels, outerrelids)
}

/*
 * If the parameterization is only partly satisfied by the outer rel,
 * the unsatisfied part can't include any outer-join relids that could
 * null rels of the satisfied part.  That would imply that we're trying
 * to use a clause involving a Var with nonempty varnullingrels at
 * a join level where that value isn't yet computable.
 *
 * In practice, this test never finds a problem because earlier join order
 * restrictions prevent us from attempting a join that would cause a problem.
 * (That's unsurprising, because the code worked before we ever added
 * outer-join relids to expression relids.)  It still seems worth checking
 * as a backstop, but we only do so in assert-enabled builds.
 */
#[cfg(debug_assertions)]
unsafe fn have_unsafe_outer_join_ref(
    root: *mut PlannerInfo,
    outerrelids: Relids,
    inner_paramrels: Relids,
) -> bool {
    let mut result = false;
    let unsatisfied = bms_difference(inner_paramrels, outerrelids);
    let satisfied = bms_intersect(inner_paramrels, outerrelids);

    if bms_overlap(unsatisfied, (*root).outer_join_rels) {
        let mut lc: crate::nodes::pg_list::ForEachState;
        foreach!(lc, (*root).join_info_list, {
            let sjinfo = lfirst(crate::current_cell!(lc)) as *mut SpecialJoinInfo;

            if !bms_is_member((*sjinfo).ojrelid as c_int, unsatisfied) {
                continue; /* not relevant */
            }
            if bms_overlap(satisfied, (*sjinfo).min_righthand)
                || ((*sjinfo).jointype == JOIN_FULL
                    && bms_overlap(satisfied, (*sjinfo).min_lefthand))
            {
                result = true; /* doesn't work */
                break;
            }
        });
    }

    /* Waste no memory when we reject a path here */
    bms_free(unsatisfied);
    bms_free(satisfied);

    result
}

// ---------------------------------------------------------------------------
// paraminfo_get_equal_hashops
// ---------------------------------------------------------------------------

/*
 * paraminfo_get_equal_hashops
 *		Determine if the clauses in param_info and innerrel's lateral vars
 *		can be hashed.
 *		Returns true if hashing is possible, otherwise false.
 *
 * Additionally, on success we collect the outer expressions and the
 * appropriate equality operators for each hashable parameter to innerrel.
 * These are returned in parallel lists in *param_exprs and *operators.
 * We also set *binary_mode to indicate whether strict binary matching is
 * required.
 */
unsafe fn paraminfo_get_equal_hashops(
    root: *mut PlannerInfo,
    param_info: *mut ParamPathInfo,
    outerrel: *mut RelOptInfo,
    innerrel: *mut RelOptInfo,
    ph_lateral_vars: *mut List,
    param_exprs: *mut *mut List,
    operators: *mut *mut List,
    binary_mode: *mut bool,
) -> bool {
    let mut lc: crate::nodes::pg_list::ForEachState;

    *param_exprs = NIL;
    *operators = NIL;
    *binary_mode = false;

    /* Add join clauses from param_info to the hash key */
    if !param_info.is_null() {
        let clauses = (*param_info).ppi_clauses;

        foreach!(lc, clauses, {
            let rinfo = lfirst(crate::current_cell!(lc)) as *mut RestrictInfo;
            let opexpr = (*rinfo).clause as *mut OpExpr;

            /*
             * Bail if the rinfo is not compatible.  We need a join OpExpr
             * with 2 args.
             */
            if !crate::IsA!(opexpr, T_OpExpr)
                || list_length((*opexpr).args) != 2
                || !clause_sides_match_join(rinfo, (*outerrel).relids, (*innerrel).relids)
            {
                list_free(*operators);
                list_free(*param_exprs);
                return false;
            }

            let (expr, hasheqoperator): (*mut Node, Oid) = if (*rinfo).outer_is_left {
                (
                    linitial((*opexpr).args) as *mut Node,
                    (*rinfo).left_hasheqoperator,
                )
            } else {
                (
                    lsecond((*opexpr).args) as *mut Node,
                    (*rinfo).right_hasheqoperator,
                )
            };

            /* can't do memoize if we can't hash the outer type */
            if !OidIsValid(hasheqoperator) {
                list_free(*operators);
                list_free(*param_exprs);
                return false;
            }

            /*
             * 'expr' may already exist as a parameter from a previous item in
             * ppi_clauses.  No need to include it again, however we'd better
             * ensure we do switch into binary mode if required.  See below.
             */
            if !list_member(*param_exprs, expr as *const c_void) {
                *operators = lappend_oid(*operators, hasheqoperator);
                *param_exprs = lappend(*param_exprs, expr as *mut c_void);
            }

            /*
             * When the join operator is not hashable then it's possible that
             * the operator will be able to distinguish something that the
             * hash equality operator could not. For example with floating
             * point types -0.0 and +0.0 are classed as equal by the hash
             * function and equality function, but some other operator may be
             * able to tell those values apart.  This means that we must put
             * memoize into binary comparison mode so that it does bit-by-bit
             * comparisons rather than a "logical" comparison as it would
             * using the hash equality operator.
             */
            if !OidIsValid((*rinfo).hashjoinoperator) {
                *binary_mode = true;
            }
        });
    }

    /* Now add any lateral vars to the cache key too */
    let lateral_vars = list_concat(ph_lateral_vars, (*innerrel).lateral_vars);
    foreach!(lc, lateral_vars, {
        let expr = lfirst(crate::current_cell!(lc)) as *mut Node;

        /* Reject if there are any volatile functions in lateral vars */
        if contain_volatile_functions(expr) {
            list_free(*operators);
            list_free(*param_exprs);
            return false;
        }

        let typentry = lookup_type_cache(
            exprType(expr),
            TYPECACHE_HASH_PROC | TYPECACHE_EQ_OPR,
        );

        /* can't use memoize without a valid hash proc and equals operator */
        if !OidIsValid((*typentry).hash_proc) || !OidIsValid((*typentry).eq_opr) {
            list_free(*operators);
            list_free(*param_exprs);
            return false;
        }

        /*
         * 'expr' may already exist as a parameter from the ppi_clauses.  No
         * need to include it again, however we'd better ensure we do switch
         * into binary mode.
         */
        if !list_member(*param_exprs, expr as *const c_void) {
            *operators = lappend_oid(*operators, (*typentry).eq_opr);
            *param_exprs = lappend(*param_exprs, expr as *mut c_void);
        }

        /*
         * We must go into binary mode as we don't have too much of an idea of
         * how these lateral Vars are being used.  See comment above when we
         * set *binary_mode for the non-lateral Var case. This could be
         * relaxed a bit if we had the RestrictInfos and knew the operators
         * being used, however for cases like Vars that are arguments to
         * functions we must operate in binary mode as we don't have
         * visibility into what the function is doing with the Vars.
         */
        *binary_mode = true;
    });

    /* We're okay to use memoize */
    true
}

// ---------------------------------------------------------------------------
// extract_lateral_vars_from_PHVs
// ---------------------------------------------------------------------------

/*
 * extract_lateral_vars_from_PHVs
 *    Extract lateral references within PlaceHolderVars that are due to be
 *    evaluated at 'innerrelids'.
 */
unsafe fn extract_lateral_vars_from_PHVs(
    root: *mut PlannerInfo,
    innerrelids: Relids,
) -> *mut List {
    let mut ph_lateral_vars: *mut List = NIL;
    let mut lc: crate::nodes::pg_list::ForEachState;

    /* Nothing would be found if the query contains no LATERAL RTEs */
    if !(*root).hasLateralRTEs {
        return NIL;
    }

    /*
     * No need to consider PHVs that are due to be evaluated at joinrels,
     * since we do not add Memoize nodes on top of joinrel paths.
     */
    if bms_membership(innerrelids) == BMS_MULTIPLE {
        return NIL;
    }

    foreach!(lc, (*root).placeholder_list, {
        let phinfo = lfirst(crate::current_cell!(lc)) as *mut PlaceHolderInfo;
        let mut cell: crate::nodes::pg_list::ForEachState;

        /* PHV is uninteresting if no lateral refs */
        if (*phinfo).ph_lateral.is_null() {
            continue;
        }

        /* PHV is uninteresting if not due to be evaluated at innerrelids */
        if !crate::nodes::bitmapset::bms_equal((*phinfo).ph_eval_at, innerrelids) {
            continue;
        }

        /*
         * If the PHV does not reference any rels in innerrelids, use its
         * contained expression as a cache key rather than extracting the
         * Vars/PHVs from it and using those.  This can be beneficial in cases
         * where the expression results in fewer distinct values to cache
         * tuples for.
         */
        if !bms_overlap(
            pull_varnos(root, (*(*phinfo).ph_var).phexpr as *mut Node),
            innerrelids,
        ) {
            ph_lateral_vars =
                lappend(ph_lateral_vars, (*(*phinfo).ph_var).phexpr as *mut c_void);
            continue;
        }

        /* Fetch Vars and PHVs of lateral references within PlaceHolderVars */
        let vars = pull_vars_of_level((*(*phinfo).ph_var).phexpr as *mut Node, 0);
        foreach!(cell, vars, {
            let node = lfirst(crate::current_cell!(cell)) as *mut Node;

            if crate::IsA!(node, T_Var) {
                let var = node as *mut Var;
                debug_assert_eq!((*var).varlevelsup, 0);
                if bms_is_member((*var).varno as c_int, (*phinfo).ph_lateral) {
                    ph_lateral_vars = lappend(ph_lateral_vars, node as *mut c_void);
                }
            } else if crate::IsA!(node, T_PlaceHolderVar) {
                let phv = node as *mut PlaceHolderVar;
                debug_assert_eq!((*phv).phlevelsup, 0);
                let sub_phinfo = find_placeholder_info(root, phv);
                if bms_is_subset((*sub_phinfo).ph_eval_at, (*phinfo).ph_lateral) {
                    ph_lateral_vars = lappend(ph_lateral_vars, node as *mut c_void);
                }
            } else {
                debug_assert!(false, "unexpected node in vars list");
            }
        });

        list_free(vars);
    });

    ph_lateral_vars
}

// ---------------------------------------------------------------------------
// get_memoize_path
// ---------------------------------------------------------------------------

/*
 * get_memoize_path
 *		If possible, make and return a Memoize path atop of 'inner_path'.
 *		Otherwise return NULL.
 *
 * Note that currently we do not add Memoize nodes on top of join relation
 * paths.  This is because the ParamPathInfos for join relation paths do not
 * maintain ppi_clauses, as the set of relevant clauses varies depending on how
 * the join is formed.  In addition, joinrels do not maintain lateral_vars.  So
 * we do not have a way to extract cache keys from joinrels.
 */
unsafe fn get_memoize_path(
    root: *mut PlannerInfo,
    innerrel: *mut RelOptInfo,
    outerrel: *mut RelOptInfo,
    inner_path: *mut Path,
    outer_path: *mut Path,
    jointype: JoinType,
    extra: *mut JoinPathExtraData,
) -> *mut Path {
    let mut param_exprs: *mut List = NIL;
    let mut hash_operators: *mut List = NIL;
    let mut lc: crate::nodes::pg_list::ForEachState;
    let mut binary_mode: bool = false;

    /* Obviously not if it's disabled */
    if !enable_memoize {
        return core::ptr::null_mut();
    }

    /*
     * We can safely not bother with all this unless we expect to perform more
     * than one inner scan.  The first scan is always going to be a cache
     * miss.  This would likely fail later anyway based on costs, so this is
     * really just to save some wasted effort.
     */
    if (*(*outer_path).parent).rows < 2.0 {
        return core::ptr::null_mut();
    }

    /*
     * Extract lateral Vars/PHVs within PlaceHolderVars that are due to be
     * evaluated at innerrel.  These lateral Vars/PHVs could be used as
     * memoize cache keys.
     */
    let ph_lateral_vars = extract_lateral_vars_from_PHVs(root, (*innerrel).relids);

    /*
     * We can only have a memoize node when there's some kind of cache key,
     * either parameterized path clauses or lateral Vars.  No cache key sounds
     * more like something a Materialize node might be more useful for.
     */
    if ((*inner_path).param_info.is_null()
        || list_length((*(*inner_path).param_info).ppi_clauses) == 0)
        && (*innerrel).lateral_vars.is_null()
        && ph_lateral_vars.is_null()
    {
        return core::ptr::null_mut();
    }

    /*
     * Currently we don't do this for SEMI and ANTI joins unless they're
     * marked as inner_unique.  This is because nested loop SEMI/ANTI joins
     * don't scan the inner node to completion, which will mean memoize cannot
     * mark the cache entry as complete.
     *
     * XXX Currently we don't attempt to mark SEMI/ANTI joins as inner_unique
     * = true.  Should we?  See add_paths_to_joinrel()
     */
    if !(*extra).inner_unique && (jointype == JOIN_SEMI || jointype == JOIN_ANTI) {
        return core::ptr::null_mut();
    }

    /*
     * Memoize normally marks cache entries as complete when it runs out of
     * tuples to read from its subplan.  However, with unique joins, Nested
     * Loop will skip to the next outer tuple after finding the first matching
     * inner tuple.  This means that we may not read the inner side of the
     * join to completion which leaves no opportunity to mark the cache entry
     * as complete.  To work around that, when the join is unique we
     * automatically mark cache entries as complete after fetching the first
     * tuple.  This works when the entire join condition is parameterized.
     * Otherwise, when the parameterization is only a subset of the join
     * condition, we can't be sure which part of it causes the join to be
     * unique.  This means there are no guarantees that only 1 tuple will be
     * read.  We cannot mark the cache entry as complete after reading the
     * first tuple without that guarantee.  This means the scope of Memoize
     * node's usefulness is limited to only outer rows that have no join
     * partner as this is the only case where Nested Loop would exhaust the
     * inner scan of a unique join.  Since the scope is limited to that, we
     * just don't bother making a memoize path in this case.
     *
     * Lateral vars needn't be considered here as they're not considered when
     * determining if the join is unique.
     */
    if (*extra).inner_unique {
        if (*inner_path).param_info.is_null() {
            return core::ptr::null_mut();
        }

        let ppi_serials = (*(*inner_path).param_info).ppi_serials;

        foreach_node!(RestrictInfo, T_RestrictInfo, rinfo, (*extra).restrictlist, {
            if !bms_is_member((*rinfo).rinfo_serial, ppi_serials) {
                return core::ptr::null_mut();
            }
        });
    }

    /*
     * We can't use a memoize node if there are volatile functions in the
     * inner rel's target list or restrict list.  A cache hit could reduce the
     * number of calls to these functions.
     */
    if contain_volatile_functions((*innerrel).reltarget as *mut Node) {
        return core::ptr::null_mut();
    }

    foreach!(lc, (*innerrel).baserestrictinfo, {
        let rinfo = lfirst(crate::current_cell!(lc)) as *mut RestrictInfo;
        if contain_volatile_functions(rinfo as *mut Node) {
            return core::ptr::null_mut();
        }
    });

    /*
     * Also check the parameterized path restrictinfos for volatile functions.
     * Indexed functions must be immutable so shouldn't have any volatile
     * functions, however, with a lateral join the inner scan may not be an
     * index scan.
     */
    if !(*inner_path).param_info.is_null() {
        foreach!(lc, (*(*inner_path).param_info).ppi_clauses, {
            let rinfo = lfirst(crate::current_cell!(lc)) as *mut RestrictInfo;
            if contain_volatile_functions(rinfo as *mut Node) {
                return core::ptr::null_mut();
            }
        });
    }

    /* Check if we have hash ops for each parameter to the path */
    let top_outerrel = if !(*outerrel).top_parent.is_null() {
        (*outerrel).top_parent
    } else {
        outerrel
    };

    if paraminfo_get_equal_hashops(
        root,
        (*inner_path).param_info,
        top_outerrel,
        innerrel,
        ph_lateral_vars,
        &mut param_exprs,
        &mut hash_operators,
        &mut binary_mode,
    ) {
        return create_memoize_path(
            root,
            innerrel,
            inner_path,
            param_exprs,
            hash_operators,
            (*extra).inner_unique,
            binary_mode,
            (*outer_path).rows,
        ) as *mut Path;
    }

    core::ptr::null_mut()
}

// ---------------------------------------------------------------------------
// try_nestloop_path
// ---------------------------------------------------------------------------

/*
 * try_nestloop_path
 *    Consider a nestloop join path; if it appears useful, push it into
 *    the joinrel's pathlist via add_path().
 */
unsafe fn try_nestloop_path(
    root: *mut PlannerInfo,
    joinrel: *mut RelOptInfo,
    outer_path: *mut Path,
    inner_path: *mut Path,
    pathkeys: *mut List,
    jointype: JoinType,
    extra: *mut JoinPathExtraData,
) {
    let required_outer: Relids;
    let mut workspace = core::mem::zeroed::<JoinCostWorkspace>();
    let innerrel = (*inner_path).parent;
    let outerrel = (*outer_path).parent;

    let innerrelids: Relids = if !(*innerrel).top_parent_relids.is_null() {
        (*innerrel).top_parent_relids
    } else {
        (*innerrel).relids
    };

    let outerrelids: Relids = if !(*outerrel).top_parent_relids.is_null() {
        (*outerrel).top_parent_relids
    } else {
        (*outerrel).relids
    };

    let inner_paramrels = PATH_REQ_OUTER(inner_path);
    let outer_paramrels = PATH_REQ_OUTER(outer_path);

    /*
     * If we are forming an outer join at this join, it's nonsensical to use
     * an input path that uses the outer join as part of its parameterization.
     * (This can happen despite our join order restrictions, since those apply
     * to what is in an input relation not what its parameters are.)
     */
    if (*(*extra).sjinfo).ojrelid != 0
        && (bms_is_member((*(*extra).sjinfo).ojrelid as c_int, inner_paramrels)
            || bms_is_member((*(*extra).sjinfo).ojrelid as c_int, outer_paramrels))
    {
        return;
    }

    /*
     * Check to see if proposed path is still parameterized, and reject if the
     * parameterization wouldn't be sensible --- unless allow_star_schema_join
     * says to allow it anyway.
     */
    required_outer = calc_nestloop_required_outer(
        outerrelids,
        outer_paramrels,
        innerrelids,
        inner_paramrels,
    );
    if !required_outer.is_null()
        && !bms_overlap(required_outer, (*extra).param_source_rels)
        && !allow_star_schema_join(root, outerrelids, inner_paramrels)
    {
        /* Waste no memory when we reject a path here */
        bms_free(required_outer);
        return;
    }

    /* If we got past that, we shouldn't have any unsafe outer-join refs */
    /* (assert only in use_assert_checking builds) */

    /*
     * If the inner path is parameterized, it is parameterized by the topmost
     * parent of the outer rel, not the outer rel itself.  We will need to
     * translate the parameterization, if this path is chosen, during
     * create_plan().  Here we just check whether we will be able to perform
     * the translation, and if not avoid creating a nestloop path.
     */
    if PATH_PARAM_BY_PARENT(inner_path, (*outer_path).parent)
        && !path_is_reparameterizable_by_child(inner_path, (*outer_path).parent)
    {
        bms_free(required_outer);
        return;
    }

    /*
     * Do a precheck to quickly eliminate obviously-inferior paths.  We
     * calculate a cheap lower bound on the path's cost and then use
     * add_path_precheck() to see if the path is clearly going to be dominated
     * by some existing path for the joinrel.  If not, do the full pushup with
     * creating a fully valid path structure and submitting it to add_path().
     * The latter two steps are expensive enough to make this two-phase
     * methodology worthwhile.
     */
    initial_cost_nestloop(root, &mut workspace, jointype, outer_path, inner_path, extra);

    if add_path_precheck(
        joinrel,
        workspace.disabled_nodes,
        workspace.startup_cost,
        workspace.total_cost,
        pathkeys,
        required_outer,
    ) {
        add_path(
            joinrel,
            create_nestloop_path(
                root,
                joinrel,
                jointype,
                &mut workspace,
                extra,
                outer_path,
                inner_path,
                (*extra).restrictlist,
                pathkeys,
                required_outer,
            ) as *mut Path,
        );
    } else {
        /* Waste no memory when we reject a path here */
        bms_free(required_outer);
    }
}

// ---------------------------------------------------------------------------
// try_partial_nestloop_path
// ---------------------------------------------------------------------------

/*
 * try_partial_nestloop_path
 *    Consider a partial nestloop join path; if it appears useful, push it into
 *    the joinrel's partial_pathlist via add_partial_path().
 */
unsafe fn try_partial_nestloop_path(
    root: *mut PlannerInfo,
    joinrel: *mut RelOptInfo,
    outer_path: *mut Path,
    inner_path: *mut Path,
    pathkeys: *mut List,
    jointype: JoinType,
    extra: *mut JoinPathExtraData,
) {
    let mut workspace = core::mem::zeroed::<JoinCostWorkspace>();

    /*
     * If the inner path is parameterized, the parameterization must be fully
     * satisfied by the proposed outer path.  Parameterized partial paths are
     * not supported.  The caller should already have verified that no lateral
     * rels are required here.
     */
    debug_assert!(bms_is_empty((*joinrel).lateral_relids));
    debug_assert!(bms_is_empty(PATH_REQ_OUTER(outer_path)));
    if !(*inner_path).param_info.is_null() {
        let inner_paramrels = (*(*inner_path).param_info).ppi_req_outer;
        let outerrel = (*outer_path).parent;
        let outerrelids: Relids = if !(*outerrel).top_parent_relids.is_null() {
            (*outerrel).top_parent_relids
        } else {
            (*outerrel).relids
        };

        if !bms_is_subset(inner_paramrels, outerrelids) {
            return;
        }
    }

    /*
     * If the inner path is parameterized, it is parameterized by the topmost
     * parent of the outer rel, not the outer rel itself.  We will need to
     * translate the parameterization, if this path is chosen, during
     * create_plan().  Here we just check whether we will be able to perform
     * the translation, and if not avoid creating a nestloop path.
     */
    if PATH_PARAM_BY_PARENT(inner_path, (*outer_path).parent)
        && !path_is_reparameterizable_by_child(inner_path, (*outer_path).parent)
    {
        return;
    }

    /*
     * Before creating a path, get a quick lower bound on what it is likely to
     * cost.  Bail out right away if it looks terrible.
     */
    initial_cost_nestloop(root, &mut workspace, jointype, outer_path, inner_path, extra);
    if !add_partial_path_precheck(
        joinrel,
        workspace.disabled_nodes,
        workspace.total_cost,
        pathkeys,
    ) {
        return;
    }

    /* Might be good enough to be worth trying, so let's try it. */
    add_partial_path(
        joinrel,
        create_nestloop_path(
            root,
            joinrel,
            jointype,
            &mut workspace,
            extra,
            outer_path,
            inner_path,
            (*extra).restrictlist,
            pathkeys,
            core::ptr::null_mut(),
        ) as *mut Path,
    );
}

// ---------------------------------------------------------------------------
// try_mergejoin_path
// ---------------------------------------------------------------------------

/*
 * try_mergejoin_path
 *    Consider a merge join path; if it appears useful, push it into
 *    the joinrel's pathlist via add_path().
 */
unsafe fn try_mergejoin_path(
    root: *mut PlannerInfo,
    joinrel: *mut RelOptInfo,
    outer_path: *mut Path,
    inner_path: *mut Path,
    pathkeys: *mut List,
    mergeclauses: *mut List,
    mut outersortkeys: *mut List,
    mut innersortkeys: *mut List,
    jointype: JoinType,
    extra: *mut JoinPathExtraData,
    is_partial: bool,
) {
    if is_partial {
        try_partial_mergejoin_path(
            root,
            joinrel,
            outer_path,
            inner_path,
            pathkeys,
            mergeclauses,
            outersortkeys,
            innersortkeys,
            jointype,
            extra,
        );
        return;
    }

    /*
     * If we are forming an outer join at this join, it's nonsensical to use
     * an input path that uses the outer join as part of its parameterization.
     * (This can happen despite our join order restrictions, since those apply
     * to what is in an input relation not what its parameters are.)
     */
    if (*(*extra).sjinfo).ojrelid != 0
        && (bms_is_member(
            (*(*extra).sjinfo).ojrelid as c_int,
            PATH_REQ_OUTER(inner_path),
        ) || bms_is_member(
            (*(*extra).sjinfo).ojrelid as c_int,
            PATH_REQ_OUTER(outer_path),
        ))
    {
        return;
    }

    /*
     * Check to see if proposed path is still parameterized, and reject if the
     * parameterization wouldn't be sensible.
     */
    let required_outer = calc_non_nestloop_required_outer(outer_path, inner_path);
    if !required_outer.is_null()
        && !bms_overlap(required_outer, (*extra).param_source_rels)
    {
        /* Waste no memory when we reject a path here */
        bms_free(required_outer);
        return;
    }

    /*
     * If the given paths are already well enough ordered, we can skip doing
     * an explicit sort.
     *
     * We need to determine the number of presorted keys of the outer path to
     * decide whether explicit incremental sort can be applied when
     * outersortkeys is not NIL.  We do not need to do the same for the inner
     * path though, as incremental sort currently does not support
     * mark/restore.
     */
    let mut outer_presorted_keys: c_int = 0;
    if !outersortkeys.is_null()
        && pathkeys_count_contained_in(outersortkeys, (*outer_path).pathkeys, &mut outer_presorted_keys)
    {
        outersortkeys = NIL;
    }
    if !innersortkeys.is_null()
        && pathkeys_contained_in(innersortkeys, (*inner_path).pathkeys)
    {
        innersortkeys = NIL;
    }

    /*
     * See comments in try_nestloop_path().
     */
    let mut workspace = core::mem::zeroed::<JoinCostWorkspace>();
    initial_cost_mergejoin(
        root,
        &mut workspace,
        jointype,
        mergeclauses,
        outer_path,
        inner_path,
        outersortkeys,
        innersortkeys,
        outer_presorted_keys,
        extra,
    );

    if add_path_precheck(
        joinrel,
        workspace.disabled_nodes,
        workspace.startup_cost,
        workspace.total_cost,
        pathkeys,
        required_outer,
    ) {
        add_path(
            joinrel,
            create_mergejoin_path(
                root,
                joinrel,
                jointype,
                &mut workspace,
                extra,
                outer_path,
                inner_path,
                (*extra).restrictlist,
                pathkeys,
                required_outer,
                mergeclauses,
                outersortkeys,
                innersortkeys,
                outer_presorted_keys,
            ) as *mut Path,
        );
    } else {
        /* Waste no memory when we reject a path here */
        bms_free(required_outer);
    }
}

// ---------------------------------------------------------------------------
// try_partial_mergejoin_path
// ---------------------------------------------------------------------------

/*
 * try_partial_mergejoin_path
 *    Consider a partial merge join path; if it appears useful, push it into
 *    the joinrel's pathlist via add_partial_path().
 */
unsafe fn try_partial_mergejoin_path(
    root: *mut PlannerInfo,
    joinrel: *mut RelOptInfo,
    outer_path: *mut Path,
    inner_path: *mut Path,
    pathkeys: *mut List,
    mergeclauses: *mut List,
    mut outersortkeys: *mut List,
    mut innersortkeys: *mut List,
    jointype: JoinType,
    extra: *mut JoinPathExtraData,
) {
    let mut outer_presorted_keys: c_int = 0;

    /*
     * See comments in try_partial_hashjoin_path().
     */
    debug_assert!(bms_is_empty((*joinrel).lateral_relids));
    debug_assert!(bms_is_empty(PATH_REQ_OUTER(outer_path)));
    if !bms_is_empty(PATH_REQ_OUTER(inner_path)) {
        return;
    }

    /*
     * If the given paths are already well enough ordered, we can skip doing
     * an explicit sort.
     *
     * We need to determine the number of presorted keys of the outer path to
     * decide whether explicit incremental sort can be applied when
     * outersortkeys is not NIL.  We do not need to do the same for the inner
     * path though, as incremental sort currently does not support
     * mark/restore.
     */
    if !outersortkeys.is_null()
        && pathkeys_count_contained_in(outersortkeys, (*outer_path).pathkeys, &mut outer_presorted_keys)
    {
        outersortkeys = NIL;
    }
    if !innersortkeys.is_null()
        && pathkeys_contained_in(innersortkeys, (*inner_path).pathkeys)
    {
        innersortkeys = NIL;
    }

    /*
     * See comments in try_partial_nestloop_path().
     */
    let mut workspace = core::mem::zeroed::<JoinCostWorkspace>();
    initial_cost_mergejoin(
        root,
        &mut workspace,
        jointype,
        mergeclauses,
        outer_path,
        inner_path,
        outersortkeys,
        innersortkeys,
        outer_presorted_keys,
        extra,
    );

    if !add_partial_path_precheck(
        joinrel,
        workspace.disabled_nodes,
        workspace.total_cost,
        pathkeys,
    ) {
        return;
    }

    /* Might be good enough to be worth trying, so let's try it. */
    add_partial_path(
        joinrel,
        create_mergejoin_path(
            root,
            joinrel,
            jointype,
            &mut workspace,
            extra,
            outer_path,
            inner_path,
            (*extra).restrictlist,
            pathkeys,
            core::ptr::null_mut(),
            mergeclauses,
            outersortkeys,
            innersortkeys,
            outer_presorted_keys,
        ) as *mut Path,
    );
}

// ---------------------------------------------------------------------------
// try_hashjoin_path
// ---------------------------------------------------------------------------

/*
 * try_hashjoin_path
 *    Consider a hash join path; if it appears useful, push it into
 *    the joinrel's pathlist via add_path().
 */
unsafe fn try_hashjoin_path(
    root: *mut PlannerInfo,
    joinrel: *mut RelOptInfo,
    outer_path: *mut Path,
    inner_path: *mut Path,
    hashclauses: *mut List,
    jointype: JoinType,
    extra: *mut JoinPathExtraData,
) {
    let mut workspace = core::mem::zeroed::<JoinCostWorkspace>();

    /*
     * If we are forming an outer join at this join, it's nonsensical to use
     * an input path that uses the outer join as part of its parameterization.
     * (This can happen despite our join order restrictions, since those apply
     * to what is in an input relation not what its parameters are.)
     */
    if (*(*extra).sjinfo).ojrelid != 0
        && (bms_is_member(
            (*(*extra).sjinfo).ojrelid as c_int,
            PATH_REQ_OUTER(inner_path),
        ) || bms_is_member(
            (*(*extra).sjinfo).ojrelid as c_int,
            PATH_REQ_OUTER(outer_path),
        ))
    {
        return;
    }

    /*
     * Check to see if proposed path is still parameterized, and reject if the
     * parameterization wouldn't be sensible.
     */
    let required_outer = calc_non_nestloop_required_outer(outer_path, inner_path);
    if !required_outer.is_null()
        && !bms_overlap(required_outer, (*extra).param_source_rels)
    {
        /* Waste no memory when we reject a path here */
        bms_free(required_outer);
        return;
    }

    /*
     * See comments in try_nestloop_path().  Also note that hashjoin paths
     * never have any output pathkeys, per comments in create_hashjoin_path.
     */
    initial_cost_hashjoin(
        root,
        &mut workspace,
        jointype,
        hashclauses,
        outer_path,
        inner_path,
        extra,
        false,
    );

    if add_path_precheck(
        joinrel,
        workspace.disabled_nodes,
        workspace.startup_cost,
        workspace.total_cost,
        NIL,
        required_outer,
    ) {
        add_path(
            joinrel,
            create_hashjoin_path(
                root,
                joinrel,
                jointype,
                &mut workspace,
                extra,
                outer_path,
                inner_path,
                false, /* parallel_hash */
                (*extra).restrictlist,
                required_outer,
                hashclauses,
            ) as *mut Path,
        );
    } else {
        /* Waste no memory when we reject a path here */
        bms_free(required_outer);
    }
}

// ---------------------------------------------------------------------------
// try_partial_hashjoin_path
// ---------------------------------------------------------------------------

/*
 * try_partial_hashjoin_path
 *    Consider a partial hashjoin join path; if it appears useful, push it into
 *    the joinrel's partial_pathlist via add_partial_path().
 *    The outer side is partial.  If parallel_hash is true, then the inner path
 *    must be partial and will be run in parallel to create one or more shared
 *    hash tables; otherwise the inner path must be complete and a copy of it
 *    is run in every process to create separate identical private hash tables.
 */
unsafe fn try_partial_hashjoin_path(
    root: *mut PlannerInfo,
    joinrel: *mut RelOptInfo,
    outer_path: *mut Path,
    inner_path: *mut Path,
    hashclauses: *mut List,
    jointype: JoinType,
    extra: *mut JoinPathExtraData,
    parallel_hash: bool,
) {
    let mut workspace = core::mem::zeroed::<JoinCostWorkspace>();

    /*
     * If the inner path is parameterized, we can't use a partial hashjoin.
     * Parameterized partial paths are not supported.  The caller should
     * already have verified that no lateral rels are required here.
     */
    debug_assert!(bms_is_empty((*joinrel).lateral_relids));
    debug_assert!(bms_is_empty(PATH_REQ_OUTER(outer_path)));
    if !bms_is_empty(PATH_REQ_OUTER(inner_path)) {
        return;
    }

    /*
     * Before creating a path, get a quick lower bound on what it is likely to
     * cost.  Bail out right away if it looks terrible.
     */
    initial_cost_hashjoin(
        root,
        &mut workspace,
        jointype,
        hashclauses,
        outer_path,
        inner_path,
        extra,
        parallel_hash,
    );
    if !add_partial_path_precheck(
        joinrel,
        workspace.disabled_nodes,
        workspace.total_cost,
        NIL,
    ) {
        return;
    }

    /* Might be good enough to be worth trying, so let's try it. */
    add_partial_path(
        joinrel,
        create_hashjoin_path(
            root,
            joinrel,
            jointype,
            &mut workspace,
            extra,
            outer_path,
            inner_path,
            parallel_hash,
            (*extra).restrictlist,
            core::ptr::null_mut(),
            hashclauses,
        ) as *mut Path,
    );
}

// ---------------------------------------------------------------------------
// sort_inner_and_outer
// ---------------------------------------------------------------------------

/*
 * sort_inner_and_outer
 *    Create mergejoin join paths by explicitly sorting both the outer and
 *    inner join relations on each available merge ordering.
 *
 * 'joinrel' is the join relation
 * 'outerrel' is the outer join relation
 * 'innerrel' is the inner join relation
 * 'jointype' is the type of join to do
 * 'extra' contains additional input values
 */
unsafe fn sort_inner_and_outer(
    root: *mut PlannerInfo,
    joinrel: *mut RelOptInfo,
    outerrel: *mut RelOptInfo,
    innerrel: *mut RelOptInfo,
    mut jointype: JoinType,
    extra: *mut JoinPathExtraData,
) {
    let save_jointype = jointype;
    let mut outer_path: *mut Path;
    let mut inner_path: *mut Path;
    let mut cheapest_partial_outer: *mut Path = core::ptr::null_mut();
    let mut cheapest_safe_inner: *mut Path = core::ptr::null_mut();
    let mut lc: crate::nodes::pg_list::ForEachState;

    /* Nothing to do if there are no available mergejoin clauses */
    if (*extra).mergeclause_list.is_null() {
        return;
    }

    /*
     * We only consider the cheapest-total-cost input paths, since we are
     * assuming here that a sort is required.  We will consider
     * cheapest-startup-cost input paths later, and only if they don't need a
     * sort.
     *
     * This function intentionally does not consider parameterized input
     * paths, except when the cheapest-total is parameterized.  If we did so,
     * we'd have a combinatorial explosion of mergejoin paths of dubious
     * value.  This interacts with decisions elsewhere that also discriminate
     * against mergejoins with parameterized inputs; see comments in
     * src/backend/optimizer/README.
     */
    outer_path = (*outerrel).cheapest_total_path;
    inner_path = (*innerrel).cheapest_total_path;

    /*
     * If either cheapest-total path is parameterized by the other rel, we
     * can't use a mergejoin.  (There's no use looking for alternative input
     * paths, since these should already be the least-parameterized available
     * paths.)
     */
    if PATH_PARAM_BY_REL(outer_path, innerrel) || PATH_PARAM_BY_REL(inner_path, outerrel) {
        return;
    }

    /*
     * If unique-ification is requested, do it and then handle as a plain
     * inner join.
     */
    if jointype == JOIN_UNIQUE_OUTER {
        outer_path =
            create_unique_path(root, outerrel, outer_path, (*extra).sjinfo) as *mut Path;
        debug_assert!(!outer_path.is_null());
        jointype = JOIN_INNER;
    } else if jointype == JOIN_UNIQUE_INNER {
        inner_path =
            create_unique_path(root, innerrel, inner_path, (*extra).sjinfo) as *mut Path;
        debug_assert!(!inner_path.is_null());
        jointype = JOIN_INNER;
    }

    /*
     * If the joinrel is parallel-safe, we may be able to consider a partial
     * merge join.  However, we can't handle JOIN_UNIQUE_OUTER, because the
     * outer path will be partial, and therefore we won't be able to properly
     * guarantee uniqueness.  Similarly, we can't handle JOIN_FULL, JOIN_RIGHT
     * and JOIN_RIGHT_ANTI, because they can produce false null extended rows.
     * Also, the resulting path must not be parameterized.
     */
    if (*joinrel).consider_parallel
        && save_jointype != JOIN_UNIQUE_OUTER
        && save_jointype != JOIN_FULL
        && save_jointype != JOIN_RIGHT
        && save_jointype != JOIN_RIGHT_ANTI
        && !(*outerrel).partial_pathlist.is_null()
        && bms_is_empty((*joinrel).lateral_relids)
    {
        cheapest_partial_outer = linitial((*outerrel).partial_pathlist) as *mut Path;

        if (*inner_path).parallel_safe {
            cheapest_safe_inner = inner_path;
        } else if save_jointype != JOIN_UNIQUE_INNER {
            cheapest_safe_inner =
                get_cheapest_parallel_safe_total_inner((*innerrel).pathlist);
        }
    }

    /*
     * Each possible ordering of the available mergejoin clauses will generate
     * a differently-sorted result path at essentially the same cost.  We have
     * no basis for choosing one over another at this level of joining, but
     * some sort orders may be more useful than others for higher-level
     * mergejoins, so it's worth considering multiple orderings.
     *
     * Actually, it's not quite true that every mergeclause ordering will
     * generate a different path order, because some of the clauses may be
     * partially redundant (refer to the same EquivalenceClasses).  Therefore,
     * what we do is convert the mergeclause list to a list of canonical
     * pathkeys, and then consider different orderings of the pathkeys.
     *
     * Generating a path for *every* permutation of the pathkeys doesn't seem
     * like a winning strategy; the cost in planning time is too high. For
     * now, we generate one path for each pathkey, listing that pathkey first
     * and the rest in random order.  This should allow at least a one-clause
     * mergejoin without re-sorting against any other possible mergejoin
     * partner path.  But if we've not guessed the right ordering of secondary
     * keys, we may end up evaluating clauses as qpquals when they could have
     * been done as mergeclauses.  (In practice, it's rare that there's more
     * than two or three mergeclauses, so expending a huge amount of thought
     * on that is probably not worth it.)
     *
     * The pathkey order returned by select_outer_pathkeys_for_merge() has
     * some heuristics behind it (see that function), so be sure to try it
     * exactly as-is as well as making variants.
     */
    let all_pathkeys =
        select_outer_pathkeys_for_merge(root, (*extra).mergeclause_list, joinrel);

    foreach!(lc, all_pathkeys, {
        let front_pathkey = lfirst(crate::current_cell!(lc)) as *mut crate::nodes::pathnodes::PathKey;

        /* Make a pathkey list with this guy first */
        let outerkeys: *mut List = if !core::ptr::eq(
            (*crate::current_cell!(lc)).ptr_value,
            all_pathkeys as *mut c_void,
        ) || (*crate::current_cell!(lc)).int_value != 0
        {
            lcons(
                front_pathkey as *mut c_void,
                list_delete_nth_cell(list_copy(all_pathkeys), crate::foreach_current_index!(lc)),
            )
        } else {
            all_pathkeys /* no work at first one... */
        };

        /* Sort the mergeclauses into the corresponding ordering */
        let cur_mergeclauses = find_mergeclauses_for_outer_pathkeys(
            root,
            outerkeys,
            (*extra).mergeclause_list,
        );

        /* Should have used them all... */
        debug_assert_eq!(
            list_length(cur_mergeclauses),
            list_length((*extra).mergeclause_list)
        );

        /* Build sort pathkeys for the inner side */
        let innerkeys =
            make_inner_pathkeys_for_merge(root, cur_mergeclauses, outerkeys);

        /* Build pathkeys representing output sort order */
        let merge_pathkeys =
            build_join_pathkeys(root, joinrel, jointype, outerkeys);

        /*
         * And now we can make the path.
         *
         * Note: it's possible that the cheapest paths will already be sorted
         * properly.  try_mergejoin_path will detect that case and suppress an
         * explicit sort step, so we needn't do so here.
         */
        try_mergejoin_path(
            root,
            joinrel,
            outer_path,
            inner_path,
            merge_pathkeys,
            cur_mergeclauses,
            outerkeys,
            innerkeys,
            jointype,
            extra,
            false,
        );

        /*
         * If we have partial outer and parallel safe inner path then try
         * partial mergejoin path.
         */
        if !cheapest_partial_outer.is_null() && !cheapest_safe_inner.is_null() {
            try_partial_mergejoin_path(
                root,
                joinrel,
                cheapest_partial_outer,
                cheapest_safe_inner,
                merge_pathkeys,
                cur_mergeclauses,
                outerkeys,
                innerkeys,
                jointype,
                extra,
            );
        }
    });
}

// ---------------------------------------------------------------------------
// generate_mergejoin_paths
// ---------------------------------------------------------------------------

/*
 * generate_mergejoin_paths
 *    Creates possible mergejoin paths for input outerpath.
 *
 * We generate mergejoins if mergejoin clauses are available.  We have
 * two ways to generate the inner path for a mergejoin: sort the cheapest
 * inner path, or use an inner path that is already suitably ordered for the
 * merge.  If we have several mergeclauses, it could be that there is no inner
 * path (or only a very expensive one) for the full list of mergeclauses, but
 * better paths exist if we truncate the mergeclause list (thereby discarding
 * some sort key requirements).  So, we consider truncations of the
 * mergeclause list as well as the full list.  (Ideally we'd consider all
 * subsets of the mergeclause list, but that seems way too expensive.)
 */
unsafe fn generate_mergejoin_paths(
    root: *mut PlannerInfo,
    joinrel: *mut RelOptInfo,
    innerrel: *mut RelOptInfo,
    outerpath: *mut Path,
    jointype: JoinType,
    extra: *mut JoinPathExtraData,
    useallclauses: bool,
    inner_cheapest_total: *mut Path,
    merge_pathkeys: *mut List,
    is_partial: bool,
) {
    let mut jointype = jointype;
    let save_jointype = jointype;

    if jointype == JOIN_UNIQUE_OUTER || jointype == JOIN_UNIQUE_INNER {
        jointype = JOIN_INNER;
    }

    /* Look for useful mergeclauses (if any) */
    let mergeclauses = find_mergeclauses_for_outer_pathkeys(
        root,
        (*outerpath).pathkeys,
        (*extra).mergeclause_list,
    );

    /*
     * Done with this outer path if no chance for a mergejoin.
     *
     * Special corner case: for "x FULL JOIN y ON true", there will be no join
     * clauses at all.  Ordinarily we'd generate a clauseless nestloop path,
     * but since mergejoin is our only join type that supports FULL JOIN
     * without any join clauses, it's necessary to generate a clauseless
     * mergejoin path instead.
     */
    if mergeclauses.is_null() {
        if jointype == JOIN_FULL {
            /* okay to try for mergejoin */
        } else {
            return;
        }
    }
    if useallclauses
        && list_length(mergeclauses) != list_length((*extra).mergeclause_list)
    {
        return;
    }

    /* Compute the required ordering of the inner path */
    let innersortkeys =
        make_inner_pathkeys_for_merge(root, mergeclauses, (*outerpath).pathkeys);

    /*
     * Generate a mergejoin on the basis of sorting the cheapest inner. Since
     * a sort will be needed, only cheapest total cost matters. (But
     * try_mergejoin_path will do the right thing if inner_cheapest_total is
     * already correctly sorted.)
     */
    try_mergejoin_path(
        root,
        joinrel,
        outerpath,
        inner_cheapest_total,
        merge_pathkeys,
        mergeclauses,
        NIL,
        innersortkeys,
        jointype,
        extra,
        is_partial,
    );

    /* Can't do anything else if inner path needs to be unique'd */
    if save_jointype == JOIN_UNIQUE_INNER {
        return;
    }

    /*
     * Look for presorted inner paths that satisfy the innersortkey list ---
     * or any truncation thereof, if we are allowed to build a mergejoin using
     * a subset of the merge clauses.  Here, we consider both cheap startup
     * cost and cheap total cost.
     *
     * Currently we do not consider parameterized inner paths here. This
     * interacts with decisions elsewhere that also discriminate against
     * mergejoins with parameterized inputs; see comments in
     * src/backend/optimizer/README.
     *
     * As we shorten the sortkey list, we should consider only paths that are
     * strictly cheaper than (in particular, not the same as) any path found
     * in an earlier iteration.  Otherwise we'd be intentionally using fewer
     * merge keys than a given path allows (treating the rest as plain
     * joinquals), which is unlikely to be a good idea.  Also, eliminating
     * paths here on the basis of compare_path_costs is a lot cheaper than
     * building the mergejoin path only to throw it away.
     *
     * If inner_cheapest_total is well enough sorted to have not required a
     * sort in the path made above, we shouldn't make a duplicate path with
     * it, either.  We handle that case with the same logic that handles the
     * previous consideration, by initializing the variables that track
     * cheapest-so-far properly.  Note that we do NOT reject
     * inner_cheapest_total if we find it matches some shorter set of
     * pathkeys.  That case corresponds to using fewer mergekeys to avoid
     * sorting inner_cheapest_total, whereas we did sort it above, so the
     * plans being considered are different.
     */
    let (mut cheapest_startup_inner, mut cheapest_total_inner): (*mut Path, *mut Path) =
        if pathkeys_contained_in(innersortkeys, (*inner_cheapest_total).pathkeys) {
            /* inner_cheapest_total didn't require a sort */
            (inner_cheapest_total, inner_cheapest_total)
        } else {
            /* it did require a sort, at least for the full set of keys */
            (core::ptr::null_mut(), core::ptr::null_mut())
        };

    let num_sortkeys = list_length(innersortkeys);
    let trialsortkeys: *mut List = if num_sortkeys > 1 && !useallclauses {
        list_copy(innersortkeys) /* need modifiable copy */
    } else {
        innersortkeys /* won't really truncate */
    };

    let mut sortkeycnt = num_sortkeys;
    while sortkeycnt > 0 {
        let mut newclauses: *mut List = NIL;

        /*
         * Look for an inner path ordered well enough for the first
         * 'sortkeycnt' innersortkeys.  NB: trialsortkeys list is modified
         * destructively, which is why we made a copy...
         */
        let trialsortkeys = list_truncate(trialsortkeys, sortkeycnt);
        let innerpath = get_cheapest_path_for_pathkeys(
            (*innerrel).pathlist,
            trialsortkeys,
            core::ptr::null_mut(),
            TOTAL_COST,
            is_partial,
        );
        if !innerpath.is_null()
            && (cheapest_total_inner.is_null()
                || compare_path_costs(innerpath, cheapest_total_inner, TOTAL_COST) < 0)
        {
            /* Found a cheap (or even-cheaper) sorted path */
            /* Select the right mergeclauses, if we didn't already */
            if sortkeycnt < num_sortkeys {
                newclauses = trim_mergeclauses_for_inner_pathkeys(
                    root,
                    mergeclauses,
                    trialsortkeys,
                );
                debug_assert!(!newclauses.is_null());
            } else {
                newclauses = mergeclauses;
            }
            try_mergejoin_path(
                root,
                joinrel,
                outerpath,
                innerpath,
                merge_pathkeys,
                newclauses,
                NIL,
                NIL,
                jointype,
                extra,
                is_partial,
            );
            cheapest_total_inner = innerpath;
        }
        /* Same on the basis of cheapest startup cost ... */
        let innerpath = get_cheapest_path_for_pathkeys(
            (*innerrel).pathlist,
            trialsortkeys,
            core::ptr::null_mut(),
            STARTUP_COST,
            is_partial,
        );
        if !innerpath.is_null()
            && (cheapest_startup_inner.is_null()
                || compare_path_costs(innerpath, cheapest_startup_inner, STARTUP_COST) < 0)
        {
            /* Found a cheap (or even-cheaper) sorted path */
            if !core::ptr::eq(innerpath, cheapest_total_inner) {
                /*
                 * Avoid rebuilding clause list if we already made one; saves
                 * memory in big join trees...
                 */
                if newclauses.is_null() {
                    if sortkeycnt < num_sortkeys {
                        newclauses = trim_mergeclauses_for_inner_pathkeys(
                            root,
                            mergeclauses,
                            trialsortkeys,
                        );
                        debug_assert!(!newclauses.is_null());
                    } else {
                        newclauses = mergeclauses;
                    }
                }
                try_mergejoin_path(
                    root,
                    joinrel,
                    outerpath,
                    innerpath,
                    merge_pathkeys,
                    newclauses,
                    NIL,
                    NIL,
                    jointype,
                    extra,
                    is_partial,
                );
            }
            cheapest_startup_inner = innerpath;
        }

        /*
         * Don't consider truncated sortkeys if we need all clauses.
         */
        if useallclauses {
            break;
        }

        sortkeycnt -= 1;
    }
}

// ---------------------------------------------------------------------------
// match_unsorted_outer
// ---------------------------------------------------------------------------

/*
 * match_unsorted_outer
 *    Creates possible join paths for processing a single join relation
 *    'joinrel' by employing either iterative substitution or
 *    mergejoining on each of its possible outer paths (considering
 *    only outer paths that are already ordered well enough for merging).
 *
 * We always generate a nestloop path for each available outer path.
 * In fact we may generate as many as five: one on the cheapest-total-cost
 * inner path, one on the same with materialization, one on the
 * cheapest-startup-cost inner path (if different), one on the
 * cheapest-total inner-indexscan path (if any), and one on the
 * cheapest-startup inner-indexscan path (if different).
 *
 * We also consider mergejoins if mergejoin clauses are available.  See
 * detailed comments in generate_mergejoin_paths.
 *
 * 'joinrel' is the join relation
 * 'outerrel' is the outer join relation
 * 'innerrel' is the inner join relation
 * 'jointype' is the type of join to do
 * 'extra' contains additional input values
 */
unsafe fn match_unsorted_outer(
    root: *mut PlannerInfo,
    joinrel: *mut RelOptInfo,
    outerrel: *mut RelOptInfo,
    innerrel: *mut RelOptInfo,
    mut jointype: JoinType,
    extra: *mut JoinPathExtraData,
) {
    let save_jointype = jointype;
    let nestjoinOK: bool;
    let useallclauses: bool;
    let mut inner_cheapest_total: *mut Path = (*innerrel).cheapest_total_path;
    let mut matpath: *mut Path = core::ptr::null_mut();
    let mut lc1: crate::nodes::pg_list::ForEachState;

    /*
     * For now we do not support RIGHT_SEMI join in mergejoin or nestloop
     * join.
     */
    if jointype == JOIN_RIGHT_SEMI {
        return;
    }

    /*
     * Nestloop only supports inner, left, semi, and anti joins.  Also, if we
     * are doing a right, right-anti or full mergejoin, we must use *all* the
     * mergeclauses as join clauses, else we will not have a valid plan.
     * (Although these two flags are currently inverses, keep them separate
     * for clarity and possible future changes.)
     */
    match jointype {
        JOIN_INNER | JOIN_LEFT | JOIN_SEMI | JOIN_ANTI => {
            nestjoinOK = true;
            useallclauses = false;
        }
        JOIN_RIGHT | JOIN_RIGHT_ANTI | JOIN_FULL => {
            nestjoinOK = false;
            useallclauses = true;
        }
        JOIN_UNIQUE_OUTER | JOIN_UNIQUE_INNER => {
            jointype = JOIN_INNER;
            nestjoinOK = true;
            useallclauses = false;
        }
        _ => {
            crate::elog!(ERROR, "unrecognized join type: {}", jointype as c_int);
            nestjoinOK = false; /* keep compiler quiet */
            useallclauses = false;
        }
    }

    /*
     * If inner_cheapest_total is parameterized by the outer rel, ignore it;
     * we will consider it below as a member of cheapest_parameterized_paths,
     * but the other possibilities considered in this routine aren't usable.
     */
    if PATH_PARAM_BY_REL(inner_cheapest_total, outerrel) {
        inner_cheapest_total = core::ptr::null_mut();
    }

    /*
     * If we need to unique-ify the inner path, we will consider only the
     * cheapest-total inner.
     */
    if save_jointype == JOIN_UNIQUE_INNER {
        /* No way to do this with an inner path parameterized by outer rel */
        if inner_cheapest_total.is_null() {
            return;
        }
        inner_cheapest_total = create_unique_path(
            root,
            innerrel,
            inner_cheapest_total,
            (*extra).sjinfo,
        ) as *mut Path;
        debug_assert!(!inner_cheapest_total.is_null());
    } else if nestjoinOK {
        /*
         * Consider materializing the cheapest inner path, unless
         * enable_material is off or the path in question materializes its
         * output anyway.
         */
        if enable_material
            && !inner_cheapest_total.is_null()
            && !ExecMaterializesOutput((*inner_cheapest_total).pathtype)
        {
            matpath =
                create_material_path(innerrel, inner_cheapest_total) as *mut Path;
        }
    }

    foreach!(lc1, (*outerrel).pathlist, {
        let outerpath = lfirst(crate::current_cell!(lc1)) as *mut Path;
        let mut outerpath = outerpath;

        /*
         * We cannot use an outer path that is parameterized by the inner rel.
         */
        if PATH_PARAM_BY_REL(outerpath, innerrel) {
            continue;
        }

        /*
         * If we need to unique-ify the outer path, it's pointless to consider
         * any but the cheapest outer.  (XXX we don't consider parameterized
         * outers, nor inners, for unique-ified cases.  Should we?)
         */
        if save_jointype == JOIN_UNIQUE_OUTER {
            if !core::ptr::eq(outerpath, (*outerrel).cheapest_total_path) {
                continue;
            }
            outerpath = create_unique_path(
                root,
                outerrel,
                outerpath,
                (*extra).sjinfo,
            ) as *mut Path;
            debug_assert!(!outerpath.is_null());
        }

        /*
         * The result will have this sort order (even if it is implemented as
         * a nestloop, and even if some of the mergeclauses are implemented by
         * qpquals rather than as true mergeclauses):
         */
        let merge_pathkeys =
            build_join_pathkeys(root, joinrel, jointype, (*outerpath).pathkeys);

        if save_jointype == JOIN_UNIQUE_INNER {
            /*
             * Consider nestloop join, but only with the unique-ified cheapest
             * inner path
             */
            try_nestloop_path(
                root,
                joinrel,
                outerpath,
                inner_cheapest_total,
                merge_pathkeys,
                jointype,
                extra,
            );
        } else if nestjoinOK {
            /*
             * Consider nestloop joins using this outer path and various
             * available paths for the inner relation.  We consider the
             * cheapest-total paths for each available parameterization of the
             * inner relation, including the unparameterized case.
             */
            let mut lc2: crate::nodes::pg_list::ForEachState;
            foreach!(lc2, (*innerrel).cheapest_parameterized_paths, {
                let innerpath = lfirst(crate::current_cell!(lc2)) as *mut Path;

                try_nestloop_path(
                    root,
                    joinrel,
                    outerpath,
                    innerpath,
                    merge_pathkeys,
                    jointype,
                    extra,
                );

                /*
                 * Try generating a memoize path and see if that makes the
                 * nested loop any cheaper.
                 */
                let mpath = get_memoize_path(
                    root, innerrel, outerrel, innerpath, outerpath, jointype, extra,
                );
                if !mpath.is_null() {
                    try_nestloop_path(
                        root,
                        joinrel,
                        outerpath,
                        mpath,
                        merge_pathkeys,
                        jointype,
                        extra,
                    );
                }
            });

            /* Also consider materialized form of the cheapest inner path */
            if !matpath.is_null() {
                try_nestloop_path(
                    root,
                    joinrel,
                    outerpath,
                    matpath,
                    merge_pathkeys,
                    jointype,
                    extra,
                );
            }
        }

        /* Can't do anything else if outer path needs to be unique'd */
        if save_jointype == JOIN_UNIQUE_OUTER {
            continue;
        }

        /* Can't do anything else if inner rel is parameterized by outer */
        if inner_cheapest_total.is_null() {
            continue;
        }

        /* Generate merge join paths */
        generate_mergejoin_paths(
            root,
            joinrel,
            innerrel,
            outerpath,
            save_jointype,
            extra,
            useallclauses,
            inner_cheapest_total,
            merge_pathkeys,
            false,
        );
    });

    /*
     * Consider partial nestloop and mergejoin plan if outerrel has any
     * partial path and the joinrel is parallel-safe.  However, we can't
     * handle JOIN_UNIQUE_OUTER, because the outer path will be partial, and
     * therefore we won't be able to properly guarantee uniqueness.  Nor can
     * we handle joins needing lateral rels, since partial paths must not be
     * parameterized. Similarly, we can't handle JOIN_FULL, JOIN_RIGHT and
     * JOIN_RIGHT_ANTI, because they can produce false null extended rows.
     */
    if (*joinrel).consider_parallel
        && save_jointype != JOIN_UNIQUE_OUTER
        && save_jointype != JOIN_FULL
        && save_jointype != JOIN_RIGHT
        && save_jointype != JOIN_RIGHT_ANTI
        && !(*outerrel).partial_pathlist.is_null()
        && bms_is_empty((*joinrel).lateral_relids)
    {
        if nestjoinOK {
            consider_parallel_nestloop(
                root, joinrel, outerrel, innerrel, save_jointype, extra,
            );
        }

        /*
         * If inner_cheapest_total is NULL or non parallel-safe then find the
         * cheapest total parallel safe path.  If doing JOIN_UNIQUE_INNER, we
         * can't use any alternative inner path.
         */
        if inner_cheapest_total.is_null() || !(*inner_cheapest_total).parallel_safe {
            if save_jointype == JOIN_UNIQUE_INNER {
                return;
            }
            inner_cheapest_total =
                get_cheapest_parallel_safe_total_inner((*innerrel).pathlist);
        }

        if !inner_cheapest_total.is_null() {
            consider_parallel_mergejoin(
                root,
                joinrel,
                outerrel,
                innerrel,
                save_jointype,
                extra,
                inner_cheapest_total,
            );
        }
    }
}

// ---------------------------------------------------------------------------
// consider_parallel_mergejoin
// ---------------------------------------------------------------------------

/*
 * consider_parallel_mergejoin
 *    Try to build partial paths for a joinrel by joining a partial path
 *    for the outer relation to a complete path for the inner relation.
 *
 * 'joinrel' is the join relation
 * 'outerrel' is the outer join relation
 * 'innerrel' is the inner join relation
 * 'jointype' is the type of join to do
 * 'extra' contains additional input values
 * 'inner_cheapest_total' cheapest total path for innerrel
 */
unsafe fn consider_parallel_mergejoin(
    root: *mut PlannerInfo,
    joinrel: *mut RelOptInfo,
    _outerrel: *mut RelOptInfo,
    innerrel: *mut RelOptInfo,
    jointype: JoinType,
    extra: *mut JoinPathExtraData,
    inner_cheapest_total: *mut Path,
) {
    let mut lc1: crate::nodes::pg_list::ForEachState;

    /* generate merge join path for each partial outer path */
    foreach!(lc1, (*_outerrel).partial_pathlist, {
        let outerpath = lfirst(crate::current_cell!(lc1)) as *mut Path;

        /*
         * Figure out what useful ordering any paths we create will have.
         */
        let merge_pathkeys =
            build_join_pathkeys(root, joinrel, jointype, (*outerpath).pathkeys);

        generate_mergejoin_paths(
            root,
            joinrel,
            innerrel,
            outerpath,
            jointype,
            extra,
            false,
            inner_cheapest_total,
            merge_pathkeys,
            true,
        );
    });
}

// ---------------------------------------------------------------------------
// consider_parallel_nestloop
// ---------------------------------------------------------------------------

/*
 * consider_parallel_nestloop
 *    Try to build partial paths for a joinrel by joining a partial path for the
 *    outer relation to a complete path for the inner relation.
 *
 * 'joinrel' is the join relation
 * 'outerrel' is the outer join relation
 * 'innerrel' is the inner join relation
 * 'jointype' is the type of join to do
 * 'extra' contains additional input values
 */
unsafe fn consider_parallel_nestloop(
    root: *mut PlannerInfo,
    joinrel: *mut RelOptInfo,
    outerrel: *mut RelOptInfo,
    innerrel: *mut RelOptInfo,
    jointype: JoinType,
    extra: *mut JoinPathExtraData,
) {
    let save_jointype = jointype;
    let mut jointype = jointype;
    let inner_cheapest_total = (*innerrel).cheapest_total_path;
    let mut matpath: *mut Path = core::ptr::null_mut();
    let mut lc1: crate::nodes::pg_list::ForEachState;

    if jointype == JOIN_UNIQUE_INNER {
        jointype = JOIN_INNER;
    }

    /*
     * Consider materializing the cheapest inner path, unless: 1) we're doing
     * JOIN_UNIQUE_INNER, because in this case we have to unique-ify the
     * cheapest inner path, 2) enable_material is off, 3) the cheapest inner
     * path is not parallel-safe, 4) the cheapest inner path is parameterized
     * by the outer rel, or 5) the cheapest inner path materializes its output
     * anyway.
     */
    if save_jointype != JOIN_UNIQUE_INNER
        && enable_material
        && (*inner_cheapest_total).parallel_safe
        && !PATH_PARAM_BY_REL(inner_cheapest_total, outerrel)
        && !ExecMaterializesOutput((*inner_cheapest_total).pathtype)
    {
        matpath = create_material_path(innerrel, inner_cheapest_total) as *mut Path;
        debug_assert!((*matpath).parallel_safe);
    }

    foreach!(lc1, (*outerrel).partial_pathlist, {
        let outerpath = lfirst(crate::current_cell!(lc1)) as *mut Path;
        let mut lc2: crate::nodes::pg_list::ForEachState;

        /* Figure out what useful ordering any paths we create will have. */
        let pathkeys =
            build_join_pathkeys(root, joinrel, jointype, (*outerpath).pathkeys);

        /*
         * Try the cheapest parameterized paths; only those which will produce
         * an unparameterized path when joined to this outerrel will survive
         * try_partial_nestloop_path.  The cheapest unparameterized path is
         * also in this list.
         */
        foreach!(lc2, (*innerrel).cheapest_parameterized_paths, {
            let mut innerpath = lfirst(crate::current_cell!(lc2)) as *mut Path;

            /* Can't join to an inner path that is not parallel-safe */
            if !(*innerpath).parallel_safe {
                continue;
            }

            /*
             * If we're doing JOIN_UNIQUE_INNER, we can only use the inner's
             * cheapest_total_path, and we have to unique-ify it.  (We might
             * be able to relax this to allow other safe, unparameterized
             * inner paths, but right now create_unique_path is not on board
             * with that.)
             */
            if save_jointype == JOIN_UNIQUE_INNER {
                if !core::ptr::eq(innerpath, (*innerrel).cheapest_total_path) {
                    continue;
                }
                innerpath = create_unique_path(
                    root,
                    innerrel,
                    innerpath,
                    (*extra).sjinfo,
                ) as *mut Path;
                debug_assert!(!innerpath.is_null());
            }

            try_partial_nestloop_path(
                root, joinrel, outerpath, innerpath, pathkeys, jointype, extra,
            );

            /*
             * Try generating a memoize path and see if that makes the nested
             * loop any cheaper.
             */
            let mpath = get_memoize_path(
                root, innerrel, outerrel, innerpath, outerpath, jointype, extra,
            );
            if !mpath.is_null() {
                try_partial_nestloop_path(
                    root, joinrel, outerpath, mpath, pathkeys, jointype, extra,
                );
            }
        });

        /* Also consider materialized form of the cheapest inner path */
        if !matpath.is_null() {
            try_partial_nestloop_path(
                root, joinrel, outerpath, matpath, pathkeys, jointype, extra,
            );
        }
    });
}

// ---------------------------------------------------------------------------
// hash_inner_and_outer
// ---------------------------------------------------------------------------

/*
 * hash_inner_and_outer
 *    Create hashjoin join paths by explicitly hashing both the outer and
 *    inner keys of each available hash clause.
 *
 * 'joinrel' is the join relation
 * 'outerrel' is the outer join relation
 * 'innerrel' is the inner join relation
 * 'jointype' is the type of join to do
 * 'extra' contains additional input values
 */
unsafe fn hash_inner_and_outer(
    root: *mut PlannerInfo,
    joinrel: *mut RelOptInfo,
    outerrel: *mut RelOptInfo,
    innerrel: *mut RelOptInfo,
    mut jointype: JoinType,
    extra: *mut JoinPathExtraData,
) {
    let save_jointype = jointype;
    let isouterjoin = IS_OUTER_JOIN(jointype);
    let mut hashclauses: *mut List = NIL;
    let mut l: crate::nodes::pg_list::ForEachState;

    /*
     * We need to build only one hashclauses list for any given pair of outer
     * and inner relations; all of the hashable clauses will be used as keys.
     *
     * Scan the join's restrictinfo list to find hashjoinable clauses that are
     * usable with this pair of sub-relations.
     */
    foreach!(l, (*extra).restrictlist, {
        let restrictinfo = lfirst(crate::current_cell!(l)) as *mut RestrictInfo;

        /*
         * If processing an outer join, only use its own join clauses for
         * hashing.  For inner joins we need not be so picky.
         */
        if isouterjoin && RINFO_IS_PUSHED_DOWN(restrictinfo, (*joinrel).relids) {
            continue;
        }

        if !(*restrictinfo).can_join || (*restrictinfo).hashjoinoperator == InvalidOid {
            continue; /* not hashjoinable */
        }

        /*
         * Check if clause has the form "outer op inner" or "inner op outer".
         */
        if !clause_sides_match_join(restrictinfo, (*outerrel).relids, (*innerrel).relids) {
            continue; /* no good for these input relations */
        }

        /*
         * If clause has the form "inner op outer", check if its operator has
         * valid commutator.  This is necessary because hashclauses in this
         * form will get commuted in createplan.c to put the outer var on the
         * left (see get_switched_clauses).  This probably shouldn't ever
         * fail, since hashable operators ought to have commutators, but be
         * paranoid.
         *
         * The clause being hashjoinable indicates that it's an OpExpr.
         */
        if !(*restrictinfo).outer_is_left
            && !OidIsValid(get_commutator(
                (*((*restrictinfo).clause as *mut OpExpr)).opno,
            ))
        {
            continue;
        }

        hashclauses = lappend(hashclauses, restrictinfo as *mut c_void);
    });

    /* If we found any usable hashclauses, make paths */
    if !hashclauses.is_null() {
        /*
         * We consider both the cheapest-total-cost and cheapest-startup-cost
         * outer paths.  There's no need to consider any but the
         * cheapest-total-cost inner path, however.
         */
        let cheapest_startup_outer = (*outerrel).cheapest_startup_path;
        let cheapest_total_outer = (*outerrel).cheapest_total_path;
        let mut cheapest_total_inner = (*innerrel).cheapest_total_path;

        /*
         * If either cheapest-total path is parameterized by the other rel, we
         * can't use a hashjoin.  (There's no use looking for alternative
         * input paths, since these should already be the least-parameterized
         * available paths.)
         */
        if PATH_PARAM_BY_REL(cheapest_total_outer, innerrel)
            || PATH_PARAM_BY_REL(cheapest_total_inner, outerrel)
        {
            return;
        }

        /* Unique-ify if need be; we ignore parameterized possibilities */
        if jointype == JOIN_UNIQUE_OUTER {
            let cheapest_total_outer = create_unique_path(
                root,
                outerrel,
                cheapest_total_outer,
                (*extra).sjinfo,
            ) as *mut Path;
            debug_assert!(!cheapest_total_outer.is_null());
            jointype = JOIN_INNER;
            try_hashjoin_path(
                root,
                joinrel,
                cheapest_total_outer,
                cheapest_total_inner,
                hashclauses,
                jointype,
                extra,
            );
            /* no possibility of cheap startup here */
        } else if jointype == JOIN_UNIQUE_INNER {
            cheapest_total_inner = create_unique_path(
                root,
                innerrel,
                cheapest_total_inner,
                (*extra).sjinfo,
            ) as *mut Path;
            debug_assert!(!cheapest_total_inner.is_null());
            jointype = JOIN_INNER;
            try_hashjoin_path(
                root,
                joinrel,
                cheapest_total_outer,
                cheapest_total_inner,
                hashclauses,
                jointype,
                extra,
            );
            if !cheapest_startup_outer.is_null()
                && !core::ptr::eq(cheapest_startup_outer, cheapest_total_outer)
            {
                try_hashjoin_path(
                    root,
                    joinrel,
                    cheapest_startup_outer,
                    cheapest_total_inner,
                    hashclauses,
                    jointype,
                    extra,
                );
            }
        } else {
            /*
             * For other jointypes, we consider the cheapest startup outer
             * together with the cheapest total inner, and then consider
             * pairings of cheapest-total paths including parameterized ones.
             * There is no use in generating parameterized paths on the basis
             * of possibly cheap startup cost, so this is sufficient.
             */
            let mut lc1: crate::nodes::pg_list::ForEachState;
            let mut lc2: crate::nodes::pg_list::ForEachState;

            if !cheapest_startup_outer.is_null() {
                try_hashjoin_path(
                    root,
                    joinrel,
                    cheapest_startup_outer,
                    cheapest_total_inner,
                    hashclauses,
                    jointype,
                    extra,
                );
            }

            foreach!(lc1, (*outerrel).cheapest_parameterized_paths, {
                let outerpath = lfirst(crate::current_cell!(lc1)) as *mut Path;

                /*
                 * We cannot use an outer path that is parameterized by the
                 * inner rel.
                 */
                if PATH_PARAM_BY_REL(outerpath, innerrel) {
                    continue;
                }

                foreach!(lc2, (*innerrel).cheapest_parameterized_paths, {
                    let innerpath = lfirst(crate::current_cell!(lc2)) as *mut Path;

                    /*
                     * We cannot use an inner path that is parameterized by
                     * the outer rel, either.
                     */
                    if PATH_PARAM_BY_REL(innerpath, outerrel) {
                        continue;
                    }

                    if core::ptr::eq(outerpath, cheapest_startup_outer)
                        && core::ptr::eq(innerpath, cheapest_total_inner)
                    {
                        continue; /* already tried it */
                    }

                    try_hashjoin_path(
                        root,
                        joinrel,
                        outerpath,
                        innerpath,
                        hashclauses,
                        jointype,
                        extra,
                    );
                });
            });
        }

        /*
         * If the joinrel is parallel-safe, we may be able to consider a
         * partial hash join.
         *
         * However, we can't handle JOIN_UNIQUE_OUTER, because the outer path
         * will be partial, and therefore we won't be able to properly
         * guarantee uniqueness.
         *
         * Similarly, we can't handle JOIN_RIGHT_SEMI, because the hash table
         * is either a shared hash table or a private hash table per backend.
         * In the shared case, there is no concurrency protection for the
         * match flags, so multiple workers could inspect and set the flags
         * concurrently, potentially producing incorrect results.  In the
         * private case, each worker has its own copy of the hash table, so no
         * single process has all the match flags.
         *
         * Also, the resulting path must not be parameterized.
         */
        if (*joinrel).consider_parallel
            && save_jointype != JOIN_UNIQUE_OUTER
            && save_jointype != JOIN_RIGHT_SEMI
            && !(*outerrel).partial_pathlist.is_null()
            && bms_is_empty((*joinrel).lateral_relids)
        {
            let cheapest_partial_outer =
                linitial((*outerrel).partial_pathlist) as *mut Path;
            let mut cheapest_partial_inner: *mut Path = core::ptr::null_mut();
            let mut cheapest_safe_inner: *mut Path = core::ptr::null_mut();

            /*
             * Can we use a partial inner plan too, so that we can build a
             * shared hash table in parallel?  We can't handle
             * JOIN_UNIQUE_INNER because we can't guarantee uniqueness.
             */
            if !(*innerrel).partial_pathlist.is_null()
                && save_jointype != JOIN_UNIQUE_INNER
                && enable_parallel_hash
            {
                cheapest_partial_inner =
                    linitial((*innerrel).partial_pathlist) as *mut Path;
                try_partial_hashjoin_path(
                    root,
                    joinrel,
                    cheapest_partial_outer,
                    cheapest_partial_inner,
                    hashclauses,
                    jointype,
                    extra,
                    true, /* parallel_hash */
                );
            }

            /*
             * Normally, given that the joinrel is parallel-safe, the cheapest
             * total inner path will also be parallel-safe, but if not, we'll
             * have to search for the cheapest safe, unparameterized inner
             * path.  If doing JOIN_UNIQUE_INNER, we can't use any alternative
             * inner path.  If full, right, or right-anti join, we can't use
             * parallelism (building the hash table in each backend) because
             * no one process has all the match bits.
             */
            if save_jointype == JOIN_FULL
                || save_jointype == JOIN_RIGHT
                || save_jointype == JOIN_RIGHT_ANTI
            {
                cheapest_safe_inner = core::ptr::null_mut();
            } else if (*cheapest_total_inner).parallel_safe {
                cheapest_safe_inner = cheapest_total_inner;
            } else if save_jointype != JOIN_UNIQUE_INNER {
                cheapest_safe_inner =
                    get_cheapest_parallel_safe_total_inner((*innerrel).pathlist);
            }

            if !cheapest_safe_inner.is_null() {
                try_partial_hashjoin_path(
                    root,
                    joinrel,
                    cheapest_partial_outer,
                    cheapest_safe_inner,
                    hashclauses,
                    jointype,
                    extra,
                    false, /* parallel_hash */
                );
            }
        }
    }
}

// ---------------------------------------------------------------------------
// select_mergejoin_clauses
// ---------------------------------------------------------------------------

/*
 * select_mergejoin_clauses
 *    Select mergejoin clauses that are usable for a particular join.
 *    Returns a list of RestrictInfo nodes for those clauses.
 *
 * *mergejoin_allowed is normally set to true, but it is set to false if
 * this is a right-semi join, or this is a right/right-anti/full join and
 * there are nonmergejoinable join clauses.  The executor's mergejoin
 * machinery cannot handle such cases, so we have to avoid generating a
 * mergejoin plan.  (Note that this flag does NOT consider whether there are
 * actually any mergejoinable clauses.  This is correct because in some
 * cases we need to build a clauseless mergejoin.  Simply returning NIL is
 * therefore not enough to distinguish safe from unsafe cases.)
 *
 * We also mark each selected RestrictInfo to show which side is currently
 * being considered as outer.  These are transient markings that are only
 * good for the duration of the current add_paths_to_joinrel() call!
 *
 * We examine each restrictinfo clause known for the join to see
 * if it is mergejoinable and involves vars from the two sub-relations
 * currently of interest.
 */
unsafe fn select_mergejoin_clauses(
    root: *mut PlannerInfo,
    joinrel: *mut RelOptInfo,
    outerrel: *mut RelOptInfo,
    innerrel: *mut RelOptInfo,
    restrictlist: *mut List,
    jointype: JoinType,
    mergejoin_allowed: *mut bool,
) -> *mut List {
    let mut result_list: *mut List = NIL;
    let isouterjoin = IS_OUTER_JOIN(jointype);
    let mut have_nonmergeable_joinclause = false;
    let mut l: crate::nodes::pg_list::ForEachState;

    /*
     * For now we do not support RIGHT_SEMI join in mergejoin: the benefit of
     * swapping inputs tends to be small here.
     */
    if jointype == JOIN_RIGHT_SEMI {
        *mergejoin_allowed = false;
        return NIL;
    }

    foreach!(l, restrictlist, {
        let restrictinfo = lfirst(crate::current_cell!(l)) as *mut RestrictInfo;

        /*
         * If processing an outer join, only use its own join clauses in the
         * merge.  For inner joins we can use pushed-down clauses too. (Note:
         * we don't set have_nonmergeable_joinclause here because pushed-down
         * clauses will become otherquals not joinquals.)
         */
        if isouterjoin && RINFO_IS_PUSHED_DOWN(restrictinfo, (*joinrel).relids) {
            continue;
        }

        /* Check that clause is a mergeable operator clause */
        if !(*restrictinfo).can_join || (*restrictinfo).mergeopfamilies.is_null() {
            /*
             * The executor can handle extra joinquals that are constants, but
             * not anything else, when doing right/right-anti/full merge join.
             * (The reason to support constants is so we can do FULL JOIN ON
             * FALSE.)
             */
            if (*restrictinfo).clause.is_null()
                || !crate::IsA!((*restrictinfo).clause, T_Const)
            {
                have_nonmergeable_joinclause = true;
            }
            continue; /* not mergejoinable */
        }

        /*
         * Check if clause has the form "outer op inner" or "inner op outer".
         */
        if !clause_sides_match_join(restrictinfo, (*outerrel).relids, (*innerrel).relids) {
            have_nonmergeable_joinclause = true;
            continue; /* no good for these input relations */
        }

        /*
         * If clause has the form "inner op outer", check if its operator has
         * valid commutator.  This is necessary because mergejoin clauses in
         * this form will get commuted in createplan.c to put the outer var on
         * the left (see get_switched_clauses).  This probably shouldn't ever
         * fail, since mergejoinable operators ought to have commutators, but
         * be paranoid.
         *
         * The clause being mergejoinable indicates that it's an OpExpr.
         */
        if !(*restrictinfo).outer_is_left
            && !OidIsValid(get_commutator(
                (*((*restrictinfo).clause as *mut OpExpr)).opno,
            ))
        {
            have_nonmergeable_joinclause = true;
            continue;
        }

        /*
         * Insist that each side have a non-redundant eclass.  This
         * restriction is needed because various bits of the planner expect
         * that each clause in a merge be associable with some pathkey in a
         * canonical pathkey list, but redundant eclasses can't appear in
         * canonical sort orderings.  (XXX it might be worth relaxing this,
         * but not enough time to address it for 8.3.)
         */
        update_mergeclause_eclasses(root, restrictinfo);

        if EC_MUST_BE_REDUNDANT((*restrictinfo).left_ec)
            || EC_MUST_BE_REDUNDANT((*restrictinfo).right_ec)
        {
            have_nonmergeable_joinclause = true;
            continue; /* can't handle redundant eclasses */
        }

        result_list = lappend(result_list, restrictinfo as *mut c_void);
    });

    /*
     * Report whether mergejoin is allowed (see comment at top of function).
     */
    match jointype {
        JOIN_RIGHT | JOIN_RIGHT_ANTI | JOIN_FULL => {
            *mergejoin_allowed = !have_nonmergeable_joinclause;
        }
        _ => {
            *mergejoin_allowed = true;
        }
    }

    result_list
}
