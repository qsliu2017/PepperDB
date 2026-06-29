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

#![allow(
    clippy::vec_box,
    reason = "1:1 PG port: List* of RelOptInfo pointers maps to Vec<Box<_>> (matches pathnodes types)"
)]

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

/// PG `make_rel_from_joinlist`: build the final scan/join rel from the joinlist. A
/// single-item joinlist returns that base rel; a multi-item joinlist runs the join
/// search over the initial rels (one per item). M7 covers the flat (non-nested)
/// joinlist of base-rel `RangeTblRef`s; sub-joinlists (explicit JOIN syntax that
/// resists flattening) and GEQO grow later.
fn make_rel_from_joinlist(root: &mut PlannerInfo, joinlist: &[Node]) -> RelOptInfo {
    let levels_needed = joinlist.len();
    if levels_needed == 0 {
        not_yet_reachable("make_rel_from_joinlist: empty joinlist");
    }

    // One initial rel per joinlist item (base rels; sub-joinlists grow later).
    let mut initial_rels: Vec<Box<RelOptInfo>> = Vec::with_capacity(levels_needed);
    for jlnode in joinlist {
        let Node::RangeTblRef(rtr) = jlnode else {
            not_yet_reachable("make_rel_from_joinlist: non-RangeTblRef joinlist item (sub-joinlist)");
        };
        let rti = rtr.rtindex as usize;
        let Some(rel) = root.simple_rel_array[rti].clone() else {
            not_yet_reachable("make_rel_from_joinlist: missing base rel");
        };
        initial_rels.push(rel);
    }

    if levels_needed == 1 {
        return *initial_rels.into_iter().next().unwrap_or_else(|| {
            not_yet_reachable("make_rel_from_joinlist: missing single base rel")
        });
    }

    // The join search needs initial_rels available (has_legal_joinclause peeks at
    // it). GEQO / the join_search_hook grow later; M7 uses the exhaustive search.
    root.initial_rels.clone_from(&initial_rels);
    standard_join_search(root, levels_needed, initial_rels)
}

/// PG `standard_join_search`: the dynamic-programming join search. Level 1 is the
/// initial rels; each higher level joins lower-level rels into bigger join rels via
/// `join_search_one_level`, then `set_cheapest` is run over the new joinrels. Returns
/// the final (all-rels) joinrel. M7 inner join: the per-level set_cheapest /
/// gather / partitionwise steps reduce to set_cheapest (done inside make_join_rel),
/// so this drives the levels and returns the top joinrel.
fn standard_join_search(
    root: &mut PlannerInfo,
    levels_needed: usize,
    initial_rels: Vec<Box<RelOptInfo>>,
) -> RelOptInfo {
    use crate::nodes::bitmapset::bms_equal;

    // join_rel_level[1] = initial_rels; the rest start empty.
    root.join_rel_level = (0..=levels_needed).map(|_| Vec::new()).collect();
    root.join_rel_level[1] = initial_rels;

    for lev in 2..=levels_needed {
        crate::backend::optimizer::path::joinrels::join_search_one_level(root, lev);
        // set_cheapest over each just-built joinrel is done inside make_join_rel
        // (the gather / partitionwise-join steps grow later).
    }

    // The final joinrel is the single rel at the top level.
    let top = root.join_rel_level[levels_needed]
        .iter()
        .find(|r| {
            r.relids
                .as_ref()
                .zip(root.all_query_rels.as_ref())
                .is_some_and(|(a, b)| bms_equal(a, b))
        })
        .or_else(|| root.join_rel_level[levels_needed].first())
        .cloned()
        .unwrap_or_else(|| not_yet_reachable("standard_join_search: no final joinrel produced"));
    *top
}

#[cold]
fn not_yet_reachable(what: &str) -> ! {
    unimplemented!("{what}: not yet translated for this milestone");
}
