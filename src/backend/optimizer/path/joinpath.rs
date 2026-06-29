//! Routines to create join paths. Translated from
//! backend/optimizer/path/joinpath.c.
//!
//! Non-type-centric free functions; bodies here as snake_case `pub fn`s,
//! re-exported from `crate::optimizer::paths` under the C names.
//!
//! Disposition: `grow`. M7's live path is the inner-join driver: given the outer
//! rel, inner rel, and the restrict/join clauses, `add_paths_to_joinrel` generates
//! nestloop paths (`match_unsorted_outer`), mergejoin paths (`sort_inner_and_outer`
//! plus `match_unsorted_outer`, over the merge clauses and pathkeys), and hashjoin
//! paths (`hash_inner_and_outer`), costing each via the costsize join estimators.
//! The outer-join (SpecialJoinInfo-driven LEFT/RIGHT/FULL/SEMI/ANTI) path
//! generation, parameterized / inner-index nestloop, partial (parallel) paths,
//! Memoize, and the FDW/extension hooks are grow guards (rules.md s4).

#![allow(
    clippy::vec_box,
    reason = "1:1 PG port: List* of RestrictInfo/PathKey pointers maps to Vec<Box<_>> (matches pathnodes types)"
)]

use crate::nodes::nodes::JoinType;
use crate::nodes::pathnodes::{
    JoinPathExtraData, Path, PathKey, PlannerInfo, RelOptInfo, RestrictInfo, SpecialJoinInfo,
};
use crate::backend::optimizer::path::costsize::{
    initial_cost_hashjoin, initial_cost_mergejoin, initial_cost_nestloop,
};
use crate::backend::optimizer::path::pathkeys::{
    build_join_pathkeys, find_mergeclauses_for_outer_pathkeys, make_inner_pathkeys_for_merge,
    pathkeys_contained_in, select_outer_pathkeys_for_merge, update_mergeclause_eclasses,
};
use crate::backend::optimizer::util::pathnode::{
    add_path, create_hashjoin_path, create_mergejoin_path, create_nestloop_path,
};
use crate::optimizer::restrictinfo::clause_sides_match_join;

/// PG `add_paths_to_joinrel`: the driver that generates all join paths for a given
/// ordered (outer, inner) pair. M7 inner join: build the merge-clause list, then run
/// `sort_inner_and_outer` (sorted mergejoins), `match_unsorted_outer` (nestloops +
/// presorted mergejoins), and `hash_inner_and_outer` (hashjoins). The SEMI/ANTI
/// unique-ification, parameterized `param_source_rels`, partial paths, and the FDW/
/// extension hooks are staged.
pub fn add_paths_to_joinrel(
    root: &mut PlannerInfo,
    joinrel: &mut RelOptInfo,
    outerrel: &RelOptInfo,
    innerrel: &RelOptInfo,
    jointype: JoinType,
    sjinfo: &SpecialJoinInfo,
    restrictlist: &[Box<RestrictInfo>],
) {
    if jointype != JoinType::INNER {
        not_yet_reachable("add_paths_to_joinrel: non-inner join path generation");
    }

    let mut extra = JoinPathExtraData {
        restrictlist: restrictlist.to_vec(),
        mergeclause_list: Vec::new(),
        // innerrel_is_unique (analyzejoins) is staged; an inner join is correct with
        // inner_unique=false (no SEMI/ANTI early-stop cost discount).
        inner_unique: false,
        sjinfo: Box::new(sjinfo.clone()),
        semifactors: crate::nodes::pathnodes::SemiAntiJoinFactors {
            outer_match_frac: 0.0,
            match_count: 0.0,
        },
        param_source_rels: None,
    };

    // Find potential mergejoin clauses (enable_mergejoin defaults true).
    let mut mergejoin_allowed = true;
    extra.mergeclause_list = select_mergejoin_clauses(
        root, joinrel, outerrel, innerrel, restrictlist, jointype, &mut mergejoin_allowed,
    );

    // param_source_rels / lateral handling: none on the M7 inner join (no lateral,
    // no SpecialJoinInfo constraints) -- param_source_rels stays empty.

    // 1. Mergejoins where both inputs are explicitly sorted.
    if mergejoin_allowed {
        sort_inner_and_outer(root, joinrel, outerrel, innerrel, jointype, &extra);
    }

    // 2. Nestloops + mergejoins where the outer need not be re-sorted.
    if mergejoin_allowed {
        match_unsorted_outer(root, joinrel, outerrel, innerrel, jointype, &extra);
    }

    // 4. Hashjoins where both inputs are hashed (enable_hashjoin defaults true).
    hash_inner_and_outer(root, joinrel, outerrel, innerrel, jointype, &extra);
}

/// PG `try_nestloop_path`: cost a candidate nestloop path and, if it is not clearly
/// dominated, build it and add it to the joinrel. M7: `required_outer` empty (no
/// parameterized nestloop / inner-index nestloop), so the parameterization rejects
/// in PG don't apply; `add_path_precheck` is the always-accept of the keep-all-paths
/// `add_path` (the precheck pruning grows with the pathlist).
fn try_nestloop_path(
    root: &mut PlannerInfo,
    joinrel: &mut RelOptInfo,
    outer_path: &Path,
    inner_path: &Path,
    pathkeys: &[PathKey],
    jointype: JoinType,
    extra: &JoinPathExtraData,
) {
    let mut workspace = empty_workspace();
    initial_cost_nestloop(root, &mut workspace, jointype, outer_path, inner_path, extra);
    let path = create_nestloop_path(
        root,
        joinrel,
        jointype,
        &workspace,
        extra,
        Box::new(outer_path.clone()),
        Box::new(inner_path.clone()),
        extra.restrictlist.clone(),
        box_pathkeys(pathkeys),
        None,
    );
    add_path(joinrel, path);
}

/// PG `try_mergejoin_path`: cost + build a mergejoin path. If the input is already
/// sorted as required, the explicit sort keys are dropped (`outersortkeys`/
/// `innersortkeys` set empty). M7: non-partial only.
#[allow(clippy::too_many_arguments, reason = "1:1 PG try_mergejoin_path signature")]
fn try_mergejoin_path(
    root: &mut PlannerInfo,
    joinrel: &mut RelOptInfo,
    outer_path: &Path,
    inner_path: &Path,
    pathkeys: &[PathKey],
    mergeclauses: &[RestrictInfo],
    outersortkeys: &[PathKey],
    innersortkeys: &[PathKey],
    jointype: JoinType,
    extra: &JoinPathExtraData,
) {
    // Drop sort keys the input is already ordered by.
    let outersortkeys = if !outersortkeys.is_empty() && pathkeys_contained_in(outersortkeys, &path_pathkeys_unboxed(outer_path)) {
        Vec::new()
    } else {
        outersortkeys.to_vec()
    };
    let innersortkeys = if !innersortkeys.is_empty() && pathkeys_contained_in(innersortkeys, &path_pathkeys_unboxed(inner_path)) {
        Vec::new()
    } else {
        innersortkeys.to_vec()
    };

    let mergeclauses_boxed = box_rinfos(mergeclauses);
    let mut workspace = empty_workspace();
    initial_cost_mergejoin(
        root,
        &mut workspace,
        jointype,
        &mergeclauses_boxed,
        outer_path,
        inner_path,
        &box_pathkeys(&outersortkeys),
        &box_pathkeys(&innersortkeys),
        0,
        extra,
    );
    let path = create_mergejoin_path(
        root,
        joinrel,
        jointype,
        &workspace,
        extra,
        Box::new(outer_path.clone()),
        Box::new(inner_path.clone()),
        extra.restrictlist.clone(),
        box_pathkeys(pathkeys),
        None,
        mergeclauses_boxed,
        box_pathkeys(&outersortkeys),
        box_pathkeys(&innersortkeys),
        0,
    );
    add_path(joinrel, path);
}

/// PG `try_hashjoin_path`: cost + build a hashjoin path (no output pathkeys). M7:
/// non-partial only.
fn try_hashjoin_path(
    root: &mut PlannerInfo,
    joinrel: &mut RelOptInfo,
    outer_path: &Path,
    inner_path: &Path,
    hashclauses: &[RestrictInfo],
    jointype: JoinType,
    extra: &JoinPathExtraData,
) {
    let hashclauses_boxed = box_rinfos(hashclauses);
    let mut workspace = empty_workspace();
    initial_cost_hashjoin(root, &mut workspace, jointype, &hashclauses_boxed, outer_path, inner_path, extra, false);
    let path = create_hashjoin_path(
        root,
        joinrel,
        jointype,
        &workspace,
        extra,
        Box::new(outer_path.clone()),
        Box::new(inner_path.clone()),
        false,
        extra.restrictlist.clone(),
        None,
        hashclauses_boxed,
    );
    add_path(joinrel, path);
}

/// PG `sort_inner_and_outer`: create mergejoin paths by explicitly sorting both
/// inputs on each available merge ordering. M7 uses the cheapest-total input paths
/// (a sort is assumed required, so cheapest-startup is irrelevant here).
fn sort_inner_and_outer(
    root: &mut PlannerInfo,
    joinrel: &mut RelOptInfo,
    outerrel: &RelOptInfo,
    innerrel: &RelOptInfo,
    jointype: JoinType,
    extra: &JoinPathExtraData,
) {
    if extra.mergeclause_list.is_empty() {
        return;
    }
    let Some(outer_path) = outerrel.cheapest_total_path.as_ref().map(|p| (**p).clone()) else { return };
    let Some(inner_path) = innerrel.cheapest_total_path.as_ref().map(|p| (**p).clone()) else { return };

    let mergeclause_list = unbox_rinfos(&extra.mergeclause_list);

    // Each ordering of the merge clauses -> a differently-sorted result. PG converts
    // to canonical pathkeys and tries each as the leading key; M7 tries the single
    // ordering select_outer_pathkeys_for_merge returns (enough for one-clause merges).
    let all_pathkeys = select_outer_pathkeys_for_merge(root, &mergeclause_list, joinrel);

    let outerkeys = all_pathkeys;
    let cur_mergeclauses = find_mergeclauses_for_outer_pathkeys(root, &outerkeys, &mergeclause_list);
    let innerkeys = make_inner_pathkeys_for_merge(root, &cur_mergeclauses, &outerkeys);
    let merge_pathkeys = build_join_pathkeys(root, joinrel, jointype, &outerkeys);

    try_mergejoin_path(
        root, joinrel, &outer_path, &inner_path,
        &merge_pathkeys, &cur_mergeclauses, &outerkeys, &innerkeys, jointype, extra,
    );
}

/// PG `match_unsorted_outer`: create nestloop paths (and presorted mergejoins) using
/// each outer path without re-sorting it. M7 inner join: nestloop over every outer
/// path x the cheapest-total inner, plus a mergejoin where the inner is sorted to
/// match the outer's existing order. Memoize / materialize / parameterized inners
/// are staged.
fn match_unsorted_outer(
    root: &mut PlannerInfo,
    joinrel: &mut RelOptInfo,
    outerrel: &RelOptInfo,
    innerrel: &RelOptInfo,
    jointype: JoinType,
    extra: &JoinPathExtraData,
) {
    let Some(inner_cheapest_total) = innerrel.cheapest_total_path.as_ref().map(|p| (**p).clone()) else { return };
    let mergeclause_list = unbox_rinfos(&extra.mergeclause_list);

    // Clone the outer pathlist so we can borrow joinrel mutably in the loop.
    let outer_paths: Vec<Path> = outerrel.pathlist.iter().map(|p| (**p).clone()).collect();
    for outerpath in &outer_paths {
        // The join inherits the outer's sort order.
        let merge_pathkeys = build_join_pathkeys(root, joinrel, jointype, &path_pathkeys_unboxed(outerpath));

        // Nestloop using this outer + the (unparameterized) cheapest inner.
        try_nestloop_path(root, joinrel, outerpath, &inner_cheapest_total, &merge_pathkeys, jointype, extra);

        // Mergejoins: sort the inner to match the outer's existing pathkeys.
        generate_mergejoin_paths(
            root, joinrel, innerrel, outerpath, jointype, extra, &mergeclause_list, &inner_cheapest_total,
        );
    }
}

/// PG `generate_mergejoin_paths` (M7 subset): for an outer path with some sort order,
/// find the merge clauses usable with that order and build a mergejoin that sorts the
/// inner to match. The mergeclause-truncation search (for partially-sorted outers) is
/// the full PG behavior; M7 builds the single full-clause mergejoin.
#[allow(clippy::too_many_arguments, reason = "mirrors PG generate_mergejoin_paths inputs")]
fn generate_mergejoin_paths(
    root: &mut PlannerInfo,
    joinrel: &mut RelOptInfo,
    _innerrel: &RelOptInfo,
    outerpath: &Path,
    jointype: JoinType,
    extra: &JoinPathExtraData,
    mergeclause_list: &[RestrictInfo],
    inner_cheapest_total: &Path,
) {
    let outerpathkeys = path_pathkeys_unboxed(outerpath);
    let mergeclauses = find_mergeclauses_for_outer_pathkeys(root, &outerpathkeys, mergeclause_list);
    if mergeclauses.is_empty() {
        return; // no merge clause usable with this outer ordering
    }
    let innersortkeys = make_inner_pathkeys_for_merge(root, &mergeclauses, &outerpathkeys);
    let merge_pathkeys = build_join_pathkeys(root, joinrel, jointype, &outerpathkeys);
    // Outer is already sorted (we use its existing pathkeys), so outersortkeys empty.
    try_mergejoin_path(
        root, joinrel, outerpath, inner_cheapest_total,
        &merge_pathkeys, &mergeclauses, &[], &innersortkeys, jointype, extra,
    );
}

/// PG `hash_inner_and_outer`: scan the restrictlist for hashjoinable clauses usable
/// with this (outer, inner) pair, and if any, build a hashjoin over the cheapest
/// inputs. M7 inner join: cheapest-startup + cheapest-total outer x cheapest-total
/// inner (no parameterized / partial pairings).
fn hash_inner_and_outer(
    root: &mut PlannerInfo,
    joinrel: &mut RelOptInfo,
    outerrel: &RelOptInfo,
    innerrel: &RelOptInfo,
    jointype: JoinType,
    extra: &JoinPathExtraData,
) {
    let outer_relids = outerrel.relids.clone().unwrap_or_default();
    let inner_relids = innerrel.relids.clone().unwrap_or_default();

    let mut hashclauses: Vec<RestrictInfo> = Vec::new();
    for rinfo in &extra.restrictlist {
        let mut ri = (**rinfo).clone();
        if !ri.can_join || ri.hashjoinoperator == crate::postgres_ext::InvalidOid {
            continue; // not hashjoinable
        }
        if !clause_sides_match_join(&mut ri, &outer_relids, &inner_relids) {
            continue; // wrong rels
        }
        // "inner op outer" form needs a commutator (createplan switches sides); the
        // builtin "=" operators are their own commutator, so this always holds on M7.
        hashclauses.push(ri);
    }
    if hashclauses.is_empty() {
        return;
    }

    let cheapest_total_inner = match innerrel.cheapest_total_path.as_ref() {
        Some(p) => (**p).clone(),
        None => return,
    };
    let cheapest_total_outer = match outerrel.cheapest_total_path.as_ref() {
        Some(p) => (**p).clone(),
        None => return,
    };
    let cheapest_startup_outer = outerrel.cheapest_startup_path.as_ref().map(|p| (**p).clone());

    // cheapest-startup outer + cheapest-total inner.
    if let Some(cso) = &cheapest_startup_outer {
        try_hashjoin_path(root, joinrel, cso, &cheapest_total_inner, &hashclauses, jointype, extra);
    }
    // cheapest-total outer + cheapest-total inner (skip if identical to the above).
    let already = cheapest_startup_outer.as_ref().is_some_and(|cso| *cso == cheapest_total_outer);
    if !already {
        try_hashjoin_path(root, joinrel, &cheapest_total_outer, &cheapest_total_inner, &hashclauses, jointype, extra);
    }
}

/// PG `select_mergejoin_clauses`: pick the RestrictInfos usable as merge clauses for
/// this (outer, inner) pair (a mergejoinable operator clause whose two sides split
/// cleanly across the inputs and whose ECs are non-redundant). `mergejoin_allowed`
/// is set false only for right-semi / right/right-anti/full with a non-mergeable
/// clause (none on the M7 inner join, so it stays true).
fn select_mergejoin_clauses(
    root: &mut PlannerInfo,
    _joinrel: &RelOptInfo,
    outerrel: &RelOptInfo,
    innerrel: &RelOptInfo,
    restrictlist: &[Box<RestrictInfo>],
    _jointype: JoinType,
    mergejoin_allowed: &mut bool,
) -> Vec<Box<RestrictInfo>> {
    let outer_relids = outerrel.relids.clone().unwrap_or_default();
    let inner_relids = innerrel.relids.clone().unwrap_or_default();
    let mut result: Vec<Box<RestrictInfo>> = Vec::new();

    for rinfo in restrictlist {
        let mut ri = (**rinfo).clone();
        // Mergeable operator clause?
        if !ri.can_join || ri.mergeopfamilies.is_empty() {
            continue;
        }
        // Sides split cleanly across the inputs?
        if !clause_sides_match_join(&mut ri, &outer_relids, &inner_relids) {
            continue;
        }
        // Each side must have a non-redundant EC.
        update_mergeclause_eclasses(root, &mut ri);
        let left_redundant = ri.left_ec.as_ref().is_some_and(|ec| ec.has_const);
        let right_redundant = ri.right_ec.as_ref().is_some_and(|ec| ec.has_const);
        if left_redundant || right_redundant {
            continue;
        }
        result.push(Box::new(ri));
    }

    // Inner join: mergejoin always allowed.
    *mergejoin_allowed = true;
    result
}

// ---------------------------------------------------------------------------
// small helpers
// ---------------------------------------------------------------------------

fn empty_workspace() -> crate::nodes::pathnodes::JoinCostWorkspace {
    crate::nodes::pathnodes::JoinCostWorkspace {
        disabled_nodes: 0,
        startup_cost: 0.0,
        total_cost: 0.0,
        run_cost: 0.0,
        inner_run_cost: 0.0,
        inner_rescan_run_cost: 0.0,
        outer_rows: 0.0,
        inner_rows: 0.0,
        outer_skip_rows: 0.0,
        inner_skip_rows: 0.0,
        numbuckets: 0,
        numbatches: 0,
        inner_rows_total: 0.0,
    }
}

fn box_pathkeys(keys: &[PathKey]) -> Vec<Box<PathKey>> {
    keys.iter().map(|k| Box::new(k.clone())).collect()
}

fn box_rinfos(rinfos: &[RestrictInfo]) -> Vec<Box<RestrictInfo>> {
    rinfos.iter().map(|r| Box::new(r.clone())).collect()
}

fn unbox_rinfos(rinfos: &[Box<RestrictInfo>]) -> Vec<RestrictInfo> {
    rinfos.iter().map(|r| (**r).clone()).collect()
}

/// A path's pathkeys as an unboxed `Vec<PathKey>` (the pathkeys helpers take
/// `&[PathKey]`; the `Path` stores `Vec<Box<PathKey>>`).
fn path_pathkeys_unboxed(path: &Path) -> Vec<PathKey> {
    path.pathkeys.iter().map(|k| (**k).clone()).collect()
}

#[cold]
fn not_yet_reachable(what: &str) -> ! {
    unimplemented!("{what}: not yet translated for this milestone");
}
