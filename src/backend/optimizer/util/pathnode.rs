//! Path-node construction and the add_path machinery. Translated from
//! backend/optimizer/util/pathnode.c.
//!
//! Non-type-centric free functions; bodies here as snake_case `pub fn`s,
//! re-exported from `crate::optimizer::pathnode` under the C names.
//!
//! Disposition: `grow`. M1's live path is the trivial Result path for a
//! FROM-less SELECT: `create_group_result_path` builds the one `GroupResultPath`,
//! `add_path` records it, and `set_cheapest` selects it. `add_path`'s cost-domination
//! comparison is reduced to a minimal "append" for the single-path case (the full
//! pruning/pathkey/parameterization comparison grows in M3+ when a rel can hold
//! more than one path). The many `create_*_path` constructors for scans/joins/
//! aggregates/sorts/limits remain hollow stubs and grow per milestone.

use crate::nodes::nodes::Node;
use crate::nodes::pathnodes::{
    GroupResultPath, Path, PathTarget, PathType, PlannerInfo, RelOptInfo,
};
use crate::elog;
use crate::optimizer::cost::DEFAULT_CPU_TUPLE_COST;
use crate::utils::elog::ERROR;

/// PG `set_cheapest`: identify the cheapest paths of a relation and stash them
/// in the RelOptInfo. M1 only ever has a single, unparameterized path (the
/// Result path), so cheapest-startup, cheapest-total, and cheapest-unique all
/// coincide. The full parameterized-path / startup-vs-total comparison grows
/// when rels gain multiple paths (M3+).
pub fn set_cheapest(parent_rel: &mut RelOptInfo) {
    if parent_rel.pathlist.is_empty() {
        elog!(ERROR, "could not devise a query plan for the given query");
    }

    if parent_rel.pathlist.len() > 1 {
        not_yet_reachable("set_cheapest: multiple-path comparison");
    }

    let path = parent_rel.pathlist[0].clone();
    if path.param_info.is_some() {
        not_yet_reachable("set_cheapest: parameterized path selection");
    }

    parent_rel.cheapest_startup_path = Some(path.clone());
    parent_rel.cheapest_total_path = Some(path);
    parent_rel.cheapest_unique_path = None;
    parent_rel.cheapest_parameterized_paths = Vec::new();
}

/// PG `add_path`: consider a potential implementation path for the given
/// relation, inserting it into the rel's pathlist if it is worthwhile. M1's rel
/// holds at most one path, so this is the trivial insert; the cost-domination
/// pruning (and `Drop`-of-rejected-path bookkeeping) grows when multiple paths
/// compete (M3+).
pub fn add_path(parent_rel: &mut RelOptInfo, new_path: Box<Path>) {
    if !parent_rel.pathlist.is_empty() {
        not_yet_reachable("add_path: cost-based path domination");
    }
    parent_rel.pathlist.push(new_path);
}

/// PG `create_group_result_path`: build the trivial Result path that a FROM-less
/// SELECT (or a degenerate empty-grouping case) uses. The path emits exactly one
/// row computing the rel's targetlist, with `havingqual` as a one-time gating
/// qual. (PG calls it a "group result" because a FROM-less SELECT is a
/// degenerate grouping case; the bare quals are jammed in unprocessed.)
pub fn create_group_result_path(
    _root: &mut PlannerInfo,
    rel: &RelOptInfo,
    target: &PathTarget,
    havingqual: Vec<Box<Node>>,
) -> Box<GroupResultPath> {
    if !havingqual.is_empty() {
        // HAVING on a FROM-less SELECT (empty grouping set) is not reachable in
        // M1; cost_qual_eval over the qual grows with HAVING support.
        not_yet_reachable("create_group_result_path: havingqual cost");
    }

    let path = Path {
        pathtype: PathType::Result,
        parent: Some(Box::new(rel.clone())),
        pathtarget: Some(Box::new(target.clone())),
        param_info: None, // there are no other rels...
        parallel_aware: false,
        parallel_safe: rel.consider_parallel,
        parallel_workers: 0,
        rows: 1.0,
        disabled_nodes: 0,
        // We can't quite use cost_resultscan() because the quals we want to
        // account for are not baserestrict quals of the rel; hack it here as PG
        // does. cost.c is not yet translated, so use the documented default
        // cpu_tuple_cost; a single path's cost does not affect plan choice.
        startup_cost: target.cost.startup,
        total_cost: target.cost.startup + DEFAULT_CPU_TUPLE_COST + target.cost.per_tuple,
        pathkeys: Vec::new(),
    };

    Box::new(GroupResultPath { path, quals: havingqual })
}

/// Panic for a pathnode path not yet translated for this milestone (rules.md s4).
#[cold]
fn not_yet_reachable(what: &str) -> ! {
    unimplemented!("{what}: not yet translated for this milestone");
}
