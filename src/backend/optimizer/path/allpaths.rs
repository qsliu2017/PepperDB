//! Routines to find possible search paths for processing a query. Translated from
//! backend/optimizer/path/allpaths.c.
//!
//! Non-type-centric free functions; bodies here as snake_case `pub fn`s,
//! re-exported from `crate::optimizer::paths` under the C names.
//!
//! Disposition: `grow`. M2's live path is a single base relation:
//! `make_one_rel` -> `set_base_rel_pathlists` -> `set_rel_pathlist` ->
//! `set_plain_rel_pathlist` -> `create_seqscan_path`, producing one SeqScan path
//! over the base rel, then returning that rel as the topmost scan/join rel. The
//! join search (`set_base_rel_sizes`, `make_rel_from_joinlist`, the DP/GEQO
//! search), partitioning, parallel paths, and the non-RELATION RTE pathlists are
//! grow guards (rules.md s4).

use crate::nodes::nodes::Node;
use crate::nodes::parsenodes::{RTEKind, RangeTblEntry};
use crate::nodes::pathnodes::{PlannerInfo, RelOptInfo, RelOptKind};
use crate::backend::optimizer::util::pathnode::{add_path, create_seqscan_path, set_cheapest};

/// PG `make_one_rel`: find all the possible paths and return the RelOptInfo for
/// the topmost scan/join relation. M2 covers a single base relation (no joins).
pub fn make_one_rel(root: &mut PlannerInfo, joinlist: &[Node]) -> RelOptInfo {
    // set_base_rel_sizes would estimate base-rel sizes; get_relation_info already
    // filled rows for the single rel (rows = tuples), so M2 sets rows directly.
    set_base_rel_sizes(root);

    // Generate access paths for the base rels.
    set_base_rel_pathlists(root);

    // make_rel_from_joinlist over the joinlist returns the final rel. M2 has a
    // single base rel: the joinlist is one RangeTblRef, so the final rel is that
    // base rel.
    make_rel_from_joinlist(root, joinlist)
}

/// PG `set_base_rel_sizes` -> `set_baserel_size_estimates`: set each base rel's
/// row estimate as `tuples * clauselist_selectivity(baserestrictinfo)`. With no
/// quals the selectivity is 1.0 (rows = tuples); a WHERE qual scales it by the
/// rough default selectivities (clausesel.rs), enough to cost a qual'd scan.
fn set_base_rel_sizes(root: &mut PlannerInfo) {
    use crate::nodes::nodes::JoinType;

    for rti in 1..root.simple_rel_array.len() {
        // Take the rel out so we can pass `root` to clauselist_selectivity, then
        // put it back (the rel's clauses are cloned for the estimate).
        let Some(mut rel) = root.simple_rel_array[rti].take() else {
            continue;
        };
        if rel.reloptkind != RelOptKind::BASEREL {
            root.simple_rel_array[rti] = Some(rel);
            continue;
        }
        let clauses: Vec<Node> = rel.baserestrictinfo.iter().map(|ri| ri.clause.clone()).collect();
        let selec = if clauses.is_empty() {
            1.0
        } else {
            crate::backend::optimizer::path::clausesel::clauselist_selectivity(
                root,
                clauses,
                0,
                JoinType::INNER,
                None,
            )
        };
        let base = if rel.tuples > 0.0 { rel.tuples } else { 1.0 };
        // clamp_row_est: at least one estimated row.
        rel.rows = (base * selec).max(1.0);
        root.simple_rel_array[rti] = Some(rel);
    }
}

/// PG `set_base_rel_pathlists`: generate access paths for each base rel.
fn set_base_rel_pathlists(root: &mut PlannerInfo) {
    for rti in 1..root.simple_rel_array.len() {
        // Take the rel + its RTE out, build paths, put the rel back (the rel is
        // mutated; the RTE is read-only). simple_rte_array[rti] mirrors the rtable.
        let Some(mut rel) = root.simple_rel_array[rti].take() else {
            continue;
        };
        if rel.reloptkind != RelOptKind::BASEREL {
            root.simple_rel_array[rti] = Some(rel);
            continue;
        }
        crate::assert!(rel.relid == rti as crate::nodes::primnodes::Index);
        let rte = root.simple_rte_array[rti]
            .clone()
            .unwrap_or_else(|| not_yet_reachable("set_base_rel_pathlists: missing RTE"));
        set_rel_pathlist(root, &mut rel, &rte);
        root.simple_rel_array[rti] = Some(rel);
    }
}

/// PG `set_rel_pathlist`: dispatch on the rel's RTE kind to build its paths. M2
/// covers a plain `RTE_RELATION` (no inheritance, no foreign/sample table).
fn set_rel_pathlist(root: &mut PlannerInfo, rel: &mut RelOptInfo, rte: &RangeTblEntry) {
    // PG branches to set_append_rel_pathlist when `rte.inh` is still set after
    // expand_inherited_rtentry (i.e. the table actually has children). M2 has no
    // inheritance: expand_inherited_rtentry would clear `inh` for a childless
    // table, so a plain RTE_RELATION is always treated as a plain relation here.
    // The append-relation path grows with the inheritance milestone.
    match rel.rtekind {
        RTEKind::RELATION => set_plain_rel_pathlist(root, rel, rte),
        other => not_yet_reachable(&format!("set_rel_pathlist: RTE kind {other:?}")),
    }
}

/// PG `set_plain_rel_pathlist`: add the sequential-scan path (and, when supported,
/// index/TID/parallel paths) for a plain relation, then select the cheapest. M2
/// adds only the seqscan path.
fn set_plain_rel_pathlist(root: &mut PlannerInfo, rel: &mut RelOptInfo, _rte: &RangeTblEntry) {
    // required_outer is empty (no lateral refs in M2).
    let seqscan = create_seqscan_path(root, rel, 0);
    add_path(rel, seqscan);

    // create_index_paths: add IndexScan / BitmapHeapScan paths that beat the seqscan
    // by cost when a WHERE clause matches an index (M6). create_tidscan_paths /
    // parallel paths grow later.
    crate::backend::optimizer::path::indxpath::create_index_paths(root, rel);

    set_cheapest(rel);
}

/// PG `make_rel_from_joinlist` (M2 subset): for a single-element joinlist (one
/// `RangeTblRef`), the result is that base rel. The join search over a multi-item
/// joinlist grows at M-join.
fn make_rel_from_joinlist(root: &mut PlannerInfo, joinlist: &[Node]) -> RelOptInfo {
    if joinlist.len() != 1 {
        not_yet_reachable("make_rel_from_joinlist: join of multiple relations");
    }
    let Node::RangeTblRef(rtr) = &joinlist[0] else {
        not_yet_reachable("make_rel_from_joinlist: non-RangeTblRef joinlist item");
    };
    let rti = rtr.rtindex as usize;
    let Some(rel) = root.simple_rel_array[rti].take() else {
        not_yet_reachable("make_rel_from_joinlist: missing base rel");
    };
    *rel
}

#[cold]
fn not_yet_reachable(what: &str) -> ! {
    unimplemented!("{what}: not yet translated for this milestone");
}
