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

#![allow(
    clippy::vec_box,
    reason = "1:1 PG port: List* of RestrictInfo/PathKey pointers maps to Vec<Box<_>> (matches pathnodes types)"
)]

use crate::access::sdir::ScanDirection;
use crate::nodes::nodes::{JoinType, Node};
use crate::nodes::pathnodes::{
    BitmapHeapPath, CostSelector, GroupResultPath, HashPath, IndexClause, IndexOptInfo, IndexPath,
    JoinCostWorkspace, JoinPath, JoinPathExtraData, MergePath, NestPath, Path, PathKey, PathTarget,
    PathType, PlannerInfo, RelOptInfo, RestrictInfo,
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

    // M6: none of the competing paths are parameterized (no lateral refs / nestloop
    // params yet), so cheapest-startup and cheapest-total are simply the minima over
    // the pathlist. The parameterized-path / cheapest-unique tracking grows when a
    // rel gains parameterized paths.
    let mut cheapest_startup = 0usize;
    let mut cheapest_total = 0usize;
    for (i, path) in parent_rel.pathlist.iter().enumerate() {
        if path.param_info.is_some() {
            not_yet_reachable("set_cheapest: parameterized path selection");
        }
        if compare_path_costs(path, &parent_rel.pathlist[cheapest_startup], CostSelector::STARTUP_COST) < 0 {
            cheapest_startup = i;
        }
        if compare_path_costs(path, &parent_rel.pathlist[cheapest_total], CostSelector::TOTAL_COST) < 0 {
            cheapest_total = i;
        }
    }

    parent_rel.cheapest_startup_path = Some(parent_rel.pathlist[cheapest_startup].clone());
    parent_rel.cheapest_total_path = Some(parent_rel.pathlist[cheapest_total].clone());
    parent_rel.cheapest_unique_path = None;
    parent_rel.cheapest_parameterized_paths = Vec::new();
}

/// PG `compare_path_costs`: order two paths by the given cost criterion. Returns -1
/// if `path1` is cheaper, +1 if dearer, 0 if equal. The disabled-node count is the
/// primary key (a disabled node always loses), as in PG's `compare_path_costs`.
#[must_use]
pub fn compare_path_costs(path1: &Path, path2: &Path, criterion: CostSelector) -> i32 {
    use std::cmp::Ordering;

    let ord_to_i32 = |o: Ordering| match o {
        Ordering::Less => -1,
        Ordering::Equal => 0,
        Ordering::Greater => 1,
    };

    // The disabled-node count is the primary key (a disabled node always loses).
    if path1.disabled_nodes != path2.disabled_nodes {
        return ord_to_i32(path1.disabled_nodes.cmp(&path2.disabled_nodes));
    }
    let (c1, c2) = match criterion {
        CostSelector::STARTUP_COST => (path1.startup_cost, path2.startup_cost),
        CostSelector::TOTAL_COST => (path1.total_cost, path2.total_cost),
    };
    match c1.partial_cmp(&c2) {
        Some(Ordering::Equal) | None if matches!(criterion, CostSelector::TOTAL_COST) => {
            // For TOTAL_COST, ties break on startup cost (PG does the same).
            ord_to_i32(path1.startup_cost.partial_cmp(&path2.startup_cost).unwrap_or(Ordering::Equal))
        }
        Some(ord) => ord_to_i32(ord),
        None => 0,
    }
}

/// PG `add_path`: consider a potential implementation path for the given
/// relation, inserting it into the rel's pathlist if it is worthwhile. M1's rel
/// holds at most one path, so this is the trivial insert; the cost-domination
/// pruning (and `Drop`-of-rejected-path bookkeeping) grows when multiple paths
/// compete (M3+).
pub fn add_path(parent_rel: &mut RelOptInfo, new_path: Box<Path>) {
    // M6 keeps every candidate path and lets `set_cheapest` pick the minimum-cost
    // one. PG prunes dominated paths here (cost + pathkeys + parameterization +
    // row-count comparison, freeing the loser); that pruning grows when the pathlist
    // gets large enough to matter. The set is small on M6 (seqscan + per-index
    // index/bitmap paths), so retaining all of them is correct, just not minimal.
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
    havingqual: Vec<Node>,
) -> Box<GroupResultPath> {
    if !havingqual.is_empty() {
        // HAVING on a FROM-less SELECT (empty grouping set) is not reachable in
        // M1; cost_qual_eval over the qual grows with HAVING support.
        not_yet_reachable("create_group_result_path: havingqual cost");
    }

    let path = Path {
        pathtype: PathType::Result,
        parent: Some(Box::new(rel.parent_snapshot())),
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
        index_detail: None,
        join_detail: None,
    };

    Box::new(GroupResultPath { path, quals: havingqual })
}

/// PG `create_seqscan_path`: build a sequential-scan `Path` over a base rel. M2
/// has no parameterization (no lateral refs) and no parallelism; the path's
/// pathtarget is the rel's reltarget, and `cost_seqscan` fills its costs.
pub fn create_seqscan_path(
    root: &mut PlannerInfo,
    rel: &RelOptInfo,
    parallel_workers: i32,
) -> Box<Path> {
    if parallel_workers > 0 {
        not_yet_reachable("create_seqscan_path: parallel workers");
    }
    let Some(target) = rel.reltarget.as_ref().map(|t| (**t).clone()) else {
        not_yet_reachable("create_seqscan_path: missing reltarget");
    };

    let mut path = Path {
        pathtype: PathType::SeqScan,
        parent: Some(Box::new(rel.parent_snapshot())),
        pathtarget: Some(Box::new(target)),
        // get_baserel_parampathinfo(root, rel, NULL) is NULL with no required_outer.
        param_info: None,
        parallel_aware: false,
        parallel_safe: rel.consider_parallel,
        parallel_workers: 0,
        rows: 0.0,
        disabled_nodes: 0,
        startup_cost: 0.0,
        total_cost: 0.0,
        // seqscan has an unordered result.
        pathkeys: Vec::new(),
        index_detail: None,
        join_detail: None,
    };

    crate::backend::optimizer::path::costsize::cost_seqscan(&mut path, root, rel, None);

    Box::new(path)
}

/// PG `create_valuesscan_path`: build the single `Path` for a VALUES-list RTE. The
/// result is always unordered; there is no parameterization on the core int/text
/// multi-row path (LATERAL refs in the VALUES exprs would set `required_outer`,
/// guarded below). `cost.c`'s `cost_valuesscan` is not translated; fill the cost
/// with the same lightweight default the other single-path builders use (a single
/// path's cost does not affect plan choice).
pub fn create_valuesscan_path(
    _root: &mut PlannerInfo,
    rel: &RelOptInfo,
    required_outer: &Option<crate::nodes::pathnodes::Relids>,
) -> Box<Path> {
    if required_outer.is_some() {
        not_yet_reachable("create_valuesscan_path: parameterized VALUES (LATERAL refs)");
    }
    let Some(target) = rel.reltarget.as_ref().map(|t| (**t).clone()) else {
        not_yet_reachable("create_valuesscan_path: missing reltarget");
    };

    // cost_valuesscan: rows = rel->rows; startup = 0; per-row = cpu_tuple_cost +
    // cpu_operator_cost (the qual/expr eval). We only need a plausible total here.
    let per_tuple = DEFAULT_CPU_TUPLE_COST + target.cost.per_tuple;
    let path = Path {
        pathtype: PathType::ValuesScan,
        parent: Some(Box::new(rel.parent_snapshot())),
        pathtarget: Some(Box::new(target)),
        param_info: None,
        parallel_aware: false,
        parallel_safe: rel.consider_parallel,
        parallel_workers: 0,
        rows: rel.rows,
        disabled_nodes: 0,
        startup_cost: 0.0,
        total_cost: rel.rows * per_tuple,
        pathkeys: Vec::new(),
        index_detail: None,
        join_detail: None,
    };

    Box::new(path)
}

/// PG `create_functionscan_path`: build the single `Path` for a function-in-FROM
/// RTE. Always unordered; no parameterization on the milestone (LATERAL grows).
/// `cost_functionscan` is not translated; the lightweight default cost suffices
/// (a single path's cost does not affect plan choice).
pub fn create_functionscan_path(
    _root: &mut PlannerInfo,
    rel: &RelOptInfo,
    required_outer: &Option<crate::nodes::pathnodes::Relids>,
) -> Box<Path> {
    if required_outer.is_some() {
        not_yet_reachable("create_functionscan_path: parameterized function scan (LATERAL refs)");
    }
    let Some(target) = rel.reltarget.as_ref().map(|t| (**t).clone()) else {
        not_yet_reachable("create_functionscan_path: missing reltarget");
    };

    let per_tuple = DEFAULT_CPU_TUPLE_COST + target.cost.per_tuple;
    let path = Path {
        pathtype: PathType::FunctionScan,
        parent: Some(Box::new(rel.parent_snapshot())),
        pathtarget: Some(Box::new(target)),
        param_info: None,
        parallel_aware: false,
        parallel_safe: rel.consider_parallel,
        parallel_workers: 0,
        rows: rel.rows,
        disabled_nodes: 0,
        startup_cost: 0.0,
        total_cost: rel.rows * per_tuple,
        pathkeys: Vec::new(),
        index_detail: None,
        join_detail: None,
    };

    Box::new(path)
}

/// PG `create_index_path`: build an `IndexPath` over `index` for the base relation
/// `rel`, with the matched `indexclauses`. The path's selectivity is the product of
/// the clause selectivities; `cost_index` fills its costs. M6 has no
/// parameterization, no index ORDER BY, and no index-only scan (the regular
/// IndexScan form). The index-only-scan decision + the pathkeys grow later.
pub fn create_index_path(
    root: &mut PlannerInfo,
    rel: &RelOptInfo,
    index: &IndexOptInfo,
    indexclauses: Vec<Box<IndexClause>>,
    indexscandir: ScanDirection,
) -> Box<IndexPath> {
    use crate::nodes::nodes::JoinType;

    let Some(target) = rel.reltarget.as_ref().map(|t| (**t).clone()) else {
        not_yet_reachable("create_index_path: missing reltarget");
    };

    // The index selectivity is the selectivity of the index quals (the clauses the
    // index checks). clauselist_selectivity over those clauses (M6: the same rough
    // default the seqscan-rel size uses).
    let qual_clauses: Vec<Node> = indexclauses
        .iter()
        .flat_map(|ic| ic.indexquals.iter().map(|ri| ri.clause.clone()))
        .collect();
    let indexselectivity = if qual_clauses.is_empty() {
        1.0
    } else {
        crate::backend::optimizer::path::clausesel::clauselist_selectivity(
            root,
            qual_clauses,
            rel.relid as i32,
            JoinType::INNER,
            None,
        )
    };

    let detail = crate::nodes::pathnodes::IndexPathDetail {
        indexinfo: Box::new(index.clone()),
        indexclauses: indexclauses.clone(),
        indexscandir,
        indextotalcost: 0.0,
        indexselectivity,
        bitmapqual: None,
    };

    let path = Path {
        pathtype: PathType::IndexScan,
        parent: Some(Box::new(rel.parent_snapshot())),
        pathtarget: Some(Box::new(target)),
        param_info: None,
        parallel_aware: false,
        parallel_safe: rel.consider_parallel,
        parallel_workers: 0,
        rows: 0.0,
        disabled_nodes: 0,
        startup_cost: 0.0,
        total_cost: 0.0,
        pathkeys: Vec::new(),
        index_detail: Some(Box::new(detail)),
        join_detail: None,
    };

    let mut ipath = IndexPath {
        path,
        indexinfo: Box::new(index.clone()),
        indexclauses,
        indexorderbys: Vec::new(),
        indexorderbycols: Vec::new(),
        indexscandir,
        indextotalcost: 0.0,
        indexselectivity,
    };

    crate::backend::optimizer::path::costsize::cost_index(&mut ipath, root, 1.0, false);

    // Record the index-access-only cost on the path detail so a BitmapHeapPath built
    // over this index path can cost its bitmap producer (the index access) without
    // the IndexScan's per-tuple heap fetch.
    if let Some(d) = ipath.path.index_detail.as_mut() {
        d.indextotalcost = ipath.indextotalcost;
    }

    Box::new(ipath)
}

/// PG `create_bitmap_heap_path`: build a `BitmapHeapPath` whose bitmap producer is
/// `bitmapqual` (an IndexPath/BitmapAnd/BitmapOr path). `cost_bitmap_heap_scan`
/// fills its costs. M6 wraps a single IndexPath's quals (the bitmap form of the same
/// index scan).
pub fn create_bitmap_heap_path(
    root: &mut PlannerInfo,
    rel: &RelOptInfo,
    bitmapqual: Box<Path>,
) -> Box<BitmapHeapPath> {
    let Some(target) = rel.reltarget.as_ref().map(|t| (**t).clone()) else {
        not_yet_reachable("create_bitmap_heap_path: missing reltarget");
    };

    // The bitmap producer (an IndexScan path) carries the index detail; the bitmap
    // heap path's detail re-homes it under `bitmapqual` for createplan, copying the
    // producer's indexinfo/indexclauses for the BitmapIndexScan child.
    let producer_detail = bitmapqual
        .index_detail
        .as_ref()
        .unwrap_or_else(|| not_yet_reachable("create_bitmap_heap_path: bitmap producer has no index detail"));
    let detail = crate::nodes::pathnodes::IndexPathDetail {
        indexinfo: producer_detail.indexinfo.clone(),
        indexclauses: producer_detail.indexclauses.clone(),
        indexscandir: producer_detail.indexscandir,
        indextotalcost: producer_detail.indextotalcost,
        indexselectivity: producer_detail.indexselectivity,
        bitmapqual: Some(bitmapqual.clone()),
    };

    let path = Path {
        pathtype: PathType::BitmapHeapScan,
        parent: Some(Box::new(rel.parent_snapshot())),
        pathtarget: Some(Box::new(target)),
        param_info: None,
        parallel_aware: false,
        parallel_safe: rel.consider_parallel,
        parallel_workers: 0,
        rows: 0.0,
        disabled_nodes: 0,
        startup_cost: 0.0,
        total_cost: 0.0,
        pathkeys: Vec::new(),
        index_detail: Some(Box::new(detail)),
        join_detail: None,
    };

    let mut bpath = BitmapHeapPath { path, bitmapqual };

    crate::backend::optimizer::path::costsize::cost_bitmap_heap_scan(
        &mut bpath, root, rel, 1.0,
    );

    Box::new(bpath)
}

/// Build the base `Path` shared by the three join-path constructors: pathtype,
/// the joinrel parent + reltarget, parallel-safety, and the join `JoinPath` fields.
/// `required_outer` is empty on M7 (no parameterized join paths), so `param_info`
/// is None; the parameterized-path machinery grows later.
fn make_join_base_path(
    pathtype: PathType,
    joinrel: &RelOptInfo,
    jointype: JoinType,
    extra: &JoinPathExtraData,
    outer_path: Box<Path>,
    inner_path: Box<Path>,
    pathkeys: Vec<Box<PathKey>>,
) -> Path {
    let parallel_safe = joinrel.consider_parallel && outer_path.parallel_safe && inner_path.parallel_safe;
    let parallel_workers = outer_path.parallel_workers;
    Path {
        pathtype,
        parent: Some(Box::new(joinrel.parent_snapshot())),
        pathtarget: joinrel.reltarget.clone(),
        param_info: None,
        parallel_aware: false,
        parallel_safe,
        parallel_workers,
        rows: 0.0,
        disabled_nodes: 0,
        startup_cost: 0.0,
        total_cost: 0.0,
        pathkeys,
        index_detail: None,
        join_detail: Some(Box::new(crate::nodes::pathnodes::JoinPathDetail {
            jointype,
            inner_unique: extra.inner_unique,
            outerjoinpath: outer_path,
            innerjoinpath: inner_path,
            joinrestrictinfo: Vec::new(),
            merge: None,
            hash: None,
        })),
    }
}

/// Reconstruct a `NestPath` (for the cost call) from the base path + the join detail.
fn join_path_from(base: &Path, restrict_clauses: Vec<Box<RestrictInfo>>) -> JoinPath {
    let d = base
        .join_detail
        .as_ref()
        .unwrap_or_else(|| not_yet_reachable("join_path_from: missing join detail"));
    JoinPath {
        path: base.clone(),
        jointype: d.jointype,
        inner_unique: d.inner_unique,
        outerjoinpath: d.outerjoinpath.clone(),
        innerjoinpath: d.innerjoinpath.clone(),
        joinrestrictinfo: restrict_clauses,
    }
}

/// PG `create_nestloop_path`: build a NestLoop join path over `outer_path`/`inner_path`.
/// `final_cost_nestloop` fills the cost. M7: `required_outer` empty (no parameterized
/// nestloop / inner-index nestloop yet).
#[allow(clippy::too_many_arguments, reason = "1:1 PG create_nestloop_path signature")]
pub fn create_nestloop_path(
    root: &mut PlannerInfo,
    joinrel: &RelOptInfo,
    jointype: JoinType,
    workspace: &JoinCostWorkspace,
    extra: &JoinPathExtraData,
    outer_path: Box<Path>,
    inner_path: Box<Path>,
    restrict_clauses: Vec<Box<RestrictInfo>>,
    pathkeys: Vec<Box<PathKey>>,
    _required_outer: Option<crate::nodes::pathnodes::Relids>,
) -> Box<Path> {
    let base = make_join_base_path(PathType::NestLoop, joinrel, jointype, extra, outer_path, inner_path, pathkeys);
    let mut pathnode = NestPath { jpath: join_path_from(&base, restrict_clauses.clone()) };
    crate::backend::optimizer::path::costsize::final_cost_nestloop(root, &mut pathnode, workspace, extra);
    Box::new(nest_to_path(pathnode, restrict_clauses))
}

/// PG `create_mergejoin_path`: build a MergeJoin path. `final_cost_mergejoin` fills
/// the cost + skip_mark_restore + materialize_inner.
#[allow(clippy::too_many_arguments, reason = "1:1 PG create_mergejoin_path signature")]
pub fn create_mergejoin_path(
    root: &mut PlannerInfo,
    joinrel: &RelOptInfo,
    jointype: JoinType,
    workspace: &JoinCostWorkspace,
    extra: &JoinPathExtraData,
    outer_path: Box<Path>,
    inner_path: Box<Path>,
    restrict_clauses: Vec<Box<RestrictInfo>>,
    pathkeys: Vec<Box<PathKey>>,
    _required_outer: Option<crate::nodes::pathnodes::Relids>,
    mergeclauses: Vec<Box<RestrictInfo>>,
    outersortkeys: Vec<Box<PathKey>>,
    innersortkeys: Vec<Box<PathKey>>,
    outer_presorted_keys: i32,
) -> Box<Path> {
    let base = make_join_base_path(PathType::MergeJoin, joinrel, jointype, extra, outer_path, inner_path, pathkeys);
    let mut pathnode = MergePath {
        jpath: join_path_from(&base, restrict_clauses.clone()),
        path_mergeclauses: mergeclauses,
        outersortkeys,
        innersortkeys,
        outer_presorted_keys,
        skip_mark_restore: false,
        materialize_inner: false,
    };
    crate::backend::optimizer::path::costsize::final_cost_mergejoin(root, &mut pathnode, workspace, extra);
    Box::new(merge_to_path(pathnode, restrict_clauses))
}

/// PG `create_hashjoin_path`: build a HashJoin path. `final_cost_hashjoin` fills the
/// cost + num_batches. A hashjoin never has output pathkeys.
#[allow(clippy::too_many_arguments, reason = "1:1 PG create_hashjoin_path signature")]
pub fn create_hashjoin_path(
    root: &mut PlannerInfo,
    joinrel: &RelOptInfo,
    jointype: JoinType,
    workspace: &JoinCostWorkspace,
    extra: &JoinPathExtraData,
    outer_path: Box<Path>,
    inner_path: Box<Path>,
    _parallel_hash: bool,
    restrict_clauses: Vec<Box<RestrictInfo>>,
    _required_outer: Option<crate::nodes::pathnodes::Relids>,
    hashclauses: Vec<Box<RestrictInfo>>,
) -> Box<Path> {
    let base = make_join_base_path(PathType::HashJoin, joinrel, jointype, extra, outer_path, inner_path, Vec::new());
    let mut pathnode = HashPath {
        jpath: join_path_from(&base, restrict_clauses.clone()),
        path_hashclauses: hashclauses,
        num_batches: 0,
        inner_rows_total: 0.0,
    };
    crate::backend::optimizer::path::costsize::final_cost_hashjoin(root, &mut pathnode, workspace, extra);
    Box::new(hash_to_path(pathnode, restrict_clauses))
}

/// Fold a costed `NestPath` back into a flat `Path` carrying its join detail.
fn nest_to_path(p: NestPath, restrict_clauses: Vec<Box<RestrictInfo>>) -> Path {
    let mut path = p.jpath.path;
    if let Some(d) = path.join_detail.as_mut() {
        d.joinrestrictinfo = restrict_clauses;
    }
    path
}

/// Fold a costed `MergePath` back into a flat `Path` carrying its join + merge detail.
fn merge_to_path(p: MergePath, restrict_clauses: Vec<Box<RestrictInfo>>) -> Path {
    let mut path = p.jpath.path;
    if let Some(d) = path.join_detail.as_mut() {
        d.joinrestrictinfo = restrict_clauses;
        d.merge = Some(crate::nodes::pathnodes::MergePathDetail {
            path_mergeclauses: p.path_mergeclauses,
            outersortkeys: p.outersortkeys,
            innersortkeys: p.innersortkeys,
            outer_presorted_keys: p.outer_presorted_keys,
            skip_mark_restore: p.skip_mark_restore,
            materialize_inner: p.materialize_inner,
        });
    }
    path
}

/// Fold a costed `HashPath` back into a flat `Path` carrying its join + hash detail.
fn hash_to_path(p: HashPath, restrict_clauses: Vec<Box<RestrictInfo>>) -> Path {
    let mut path = p.jpath.path;
    if let Some(d) = path.join_detail.as_mut() {
        d.joinrestrictinfo = restrict_clauses;
        d.hash = Some(crate::nodes::pathnodes::HashPathDetail {
            path_hashclauses: p.path_hashclauses,
            num_batches: p.num_batches,
            inner_rows_total: p.inner_rows_total,
        });
    }
    path
}

/// Panic for a pathnode path not yet translated for this milestone (rules.md s4).
#[cold]
fn not_yet_reachable(what: &str) -> ! {
    unimplemented!("{what}: not yet translated for this milestone");
}
