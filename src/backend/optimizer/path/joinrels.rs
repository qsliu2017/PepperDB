//! Routines to determine which relations to join. Translated from
//! backend/optimizer/path/joinrels.c.
//!
//! Non-type-centric free functions; bodies here as snake_case `pub fn`s,
//! re-exported from `crate::optimizer::paths` under the C names.
//!
//! Disposition: `grow`. M7's live path is the inner-join dynamic-programming
//! search: `join_search_one_level(lev)` joins each level-(lev-1) rel against the
//! initial rels it shares a join clause with (`make_rels_by_clause_joins`), or a
//! Cartesian product when clauseless (`make_rels_by_clauseless_joins`).
//! `make_join_rel` finds-or-builds the join `RelOptInfo` (`build_join_rel`), sizes
//! it (`set_joinrel_size_estimates`), and populates its pathlist
//! (`add_paths_to_joinrel`, both input orderings). Outer joins (`join_is_legal` /
//! `add_outer_joins_to_relids` / the SpecialJoinInfo-driven jointypes), dummy-rel
//! elimination, bushy-plan pairs, and GEQO are grow guards (rules.md s4).

#![allow(
    clippy::vec_box,
    reason = "1:1 PG port: List* of RelOptInfo/RestrictInfo pointers maps to Vec<Box<_>> (matches pathnodes types)"
)]

use crate::nodes::bitmapset::{bms_overlap, bms_union, Bitmapset};
use crate::nodes::nodes::{JoinType, Node};
use crate::nodes::pathnodes::{
    PlannerInfo, RelOptInfo, Relids, RestrictInfo, SpecialJoinInfo,
};
use crate::backend::optimizer::path::joinpath::add_paths_to_joinrel;
use crate::backend::optimizer::path::costsize::set_joinrel_size_estimates;
use crate::backend::optimizer::util::joininfo::have_relevant_joinclause;
use crate::backend::optimizer::util::relnode::{build_join_rel, find_base_rel, find_join_rel};
use crate::postgres_ext::InvalidOid;

/// PG `join_search_one_level`: build all join rels containing exactly `level`
/// jointree items, attaching their implementation paths. The level-(level-1) rels
/// are joined against the initial (level-1) rels. M7 covers the left/right-sided
/// search (a previous-level rel joined to an initial rel); the bushy-plan pairs
/// (k-item x (level-k)-item for k >= 2) and the clauseless last-ditch retry are
/// reachable for 4+ rels and grow there. The new rels land in
/// `root.join_rel_level[level]`.
pub fn join_search_one_level(root: &mut PlannerInfo, level: usize) {
    crate::assert!(root.join_rel_level[level].is_empty());
    root.join_cur_level = level as i32;

    // Snapshot the previous level + the initial rels (level 1) so we can mutate
    // root.join_rel_level[level] while iterating.
    let prev_level: Vec<Box<RelOptInfo>> = root.join_rel_level[level - 1].clone();
    let initial_rels: Vec<Box<RelOptInfo>> = root.join_rel_level[1].clone();

    for old_rel in &prev_level {
        let has_join_clauses = !old_rel.joininfo.is_empty()
            || old_rel.has_eclass_joins
            || has_join_restriction(root, old_rel);

        if has_join_clauses {
            // Join this rel to the initial rels it shares a join clause with. At
            // level 2 the relation is symmetric, so to avoid duplicate work PG
            // starts at the rel after `old_rel`; M7 simply skips already-overlapping
            // and clause-less pairs (make_join_rel dedups via find_join_rel).
            make_rels_by_clause_joins(root, old_rel, &initial_rels);
        } else {
            // No join clause: Cartesian product against each disjoint initial rel.
            make_rels_by_clauseless_joins(root, old_rel, &initial_rels);
        }
    }

    // Bushy plans (k x level-k for 2 <= k <= level-2) only exist for level >= 4;
    // they grow with larger joins. The clauseless last-ditch retry (when special
    // joins make the level-(level-1) search find nothing) likewise grows with outer
    // joins / IN-EXISTS sub-joinlists.
    if root.join_rel_level[level].is_empty() && level > 2 {
        not_yet_reachable("join_search_one_level: bushy / clauseless last-ditch search");
    }
}

/// PG `make_rels_by_clause_joins`: join `old_rel` to each disjoint other rel it
/// shares a join clause (or join-order restriction) with.
fn make_rels_by_clause_joins(
    root: &mut PlannerInfo,
    old_rel: &RelOptInfo,
    other_rels: &[Box<RelOptInfo>],
) {
    let old_relids = old_rel.relids.clone().unwrap_or_default();
    for other_rel in other_rels {
        let other_relids = other_rel.relids.clone().unwrap_or_default();
        if !bms_overlap(&old_relids, &other_relids)
            && (have_relevant_joinclause(root, old_rel, other_rel)
                || have_join_order_restriction(root, old_rel, other_rel))
        {
            make_join_rel(root, old_rel, other_rel);
        }
    }
}

/// PG `make_rels_by_clauseless_joins`: Cartesian-product join `old_rel` to each
/// disjoint other rel.
fn make_rels_by_clauseless_joins(
    root: &mut PlannerInfo,
    old_rel: &RelOptInfo,
    other_rels: &[Box<RelOptInfo>],
) {
    let old_relids = old_rel.relids.clone().unwrap_or_default();
    for other_rel in other_rels {
        let other_relids = other_rel.relids.clone().unwrap_or_default();
        if !bms_overlap(&other_relids, &old_relids) {
            make_join_rel(root, old_rel, other_rel);
        }
    }
}

/// PG `make_join_rel`: find or create the join `RelOptInfo` for `rel1 JOIN rel2`,
/// and add the paths built from this pair (as both outer x inner orderings). Returns
/// the joinrel. M7 inner join: a dummy SpecialJoinInfo describes the plain inner
/// join (`init_dummy_sjinfo`); `join_is_legal` / `add_outer_joins_to_relids` (outer
/// joins) and dummy-rel short-circuits are staged.
pub fn make_join_rel(
    root: &mut PlannerInfo,
    rel1: &RelOptInfo,
    rel2: &RelOptInfo,
) -> Box<RelOptInfo> {
    let r1 = rel1.relids.clone().unwrap_or_default();
    let r2 = rel2.relids.clone().unwrap_or_default();
    crate::assert!(!bms_overlap(&r1, &r2));
    let joinrelids = bms_union(&r1, &r2);

    // Inner join: no SpecialJoinInfo in join_info_list -> make a dummy one so the
    // size-estimation / path machinery knows what's joined. (join_is_legal +
    // add_outer_joins_to_relids grow with outer joins.)
    if !root.join_info_list.is_empty() {
        not_yet_reachable("make_join_rel: special join (outer/semi/anti) legality");
    }
    let mut sjinfo = SpecialJoinInfo {
        min_lefthand: None,
        min_righthand: None,
        syn_lefthand: None,
        syn_righthand: None,
        jointype: JoinType::INNER,
        ojrelid: 0,
        commute_above_l: None,
        commute_above_r: None,
        commute_below_l: None,
        commute_below_r: None,
        lhs_strict: false,
        semi_can_btree: false,
        semi_can_hash: false,
        semi_operators: Vec::new(),
        semi_rhs_exprs: Vec::new(),
    };
    init_dummy_sjinfo(&mut sjinfo, r1, r2);

    // Build (or find) the joinrel + the restriction clauses for this pair.
    let (mut joinrel, restrictlist_nodes) =
        build_join_rel(root, joinrelids.clone(), rel1, rel2, &sjinfo, Vec::new());

    // Size estimate: rows = Cartesian product x join selectivity (set_joinrel_size
    // overrides build_join_rel's pre-selectivity Cartesian rows).
    set_joinrel_size_estimates(root, &mut joinrel, rel1, rel2, &sjinfo, &restrictlist_nodes);

    // The restrictlist is a list of RestrictInfo-wrapped Nodes; extract them.
    let restrictlist = extract_restrictinfos(&restrictlist_nodes);

    // Add paths for both input orderings (rel1 outer, then rel2 outer).
    populate_joinrel_with_paths(root, rel1, rel2, &mut joinrel, &sjinfo, &restrictlist);

    // set_cheapest over the populated joinrel, then store it back into both the
    // join_rel_list (replacing build_join_rel's path-less clone) and the current
    // DP level so the next level can find it.
    crate::backend::optimizer::util::pathnode::set_cheapest(&mut joinrel);
    store_joinrel(root, joinrel.clone(), &joinrelids);

    joinrel
}

/// PG `populate_joinrel_with_paths`: add paths to the joinrel for both input
/// orderings. M7 inner join: `add_paths_to_joinrel(rel1 outer, rel2 inner)` then the
/// reverse. The dummy-rel / constant-false short-circuits (and the outer-join
/// RIGHT/LEFT/FULL orderings) grow with outer joins.
fn populate_joinrel_with_paths(
    root: &mut PlannerInfo,
    rel1: &RelOptInfo,
    rel2: &RelOptInfo,
    joinrel: &mut RelOptInfo,
    sjinfo: &SpecialJoinInfo,
    restrictlist: &[Box<RestrictInfo>],
) {
    crate::assert!(sjinfo.jointype == JoinType::INNER);
    if is_dummy_rel(rel1) || is_dummy_rel(rel2) {
        not_yet_reachable("populate_joinrel_with_paths: dummy input rel");
    }
    add_paths_to_joinrel(root, joinrel, rel1, rel2, JoinType::INNER, sjinfo, restrictlist);
    add_paths_to_joinrel(root, joinrel, rel2, rel1, JoinType::INNER, sjinfo, restrictlist);
}

/// PG `init_dummy_sjinfo`: fill a SpecialJoinInfo for a plain inner join between
/// `left_relids` and `right_relids` (the min/syn lefthand/righthand are both the
/// inputs' relids; jointype INNER; the remaining fields left invalid).
pub fn init_dummy_sjinfo(sjinfo: &mut SpecialJoinInfo, left_relids: Relids, right_relids: Relids) {
    sjinfo.min_lefthand = Some(left_relids.clone());
    sjinfo.min_righthand = Some(right_relids.clone());
    sjinfo.syn_lefthand = Some(left_relids);
    sjinfo.syn_righthand = Some(right_relids);
    sjinfo.jointype = JoinType::INNER;
    sjinfo.ojrelid = 0;
    sjinfo.commute_above_l = None;
    sjinfo.commute_above_r = None;
    sjinfo.commute_below_l = None;
    sjinfo.commute_below_r = None;
    sjinfo.lhs_strict = false;
    sjinfo.semi_can_btree = false;
    sjinfo.semi_can_hash = false;
    sjinfo.semi_operators = Vec::new();
    sjinfo.semi_rhs_exprs = Vec::new();
    let _ = InvalidOid;
}

/// PG `have_join_order_restriction`: whether a join between rel1 and rel2 must be
/// attempted even without a join clause (a direct lateral reference, a shared
/// PlaceHolderVar eval, or an outer-join ordering constraint). M7 has none of those
/// (no lateral, no PHV, no special joins), so this is false. The join_info_list scan
/// for outer-join ordering grows with outer joins.
pub fn have_join_order_restriction(
    _root: &mut PlannerInfo,
    rel1: &RelOptInfo,
    rel2: &RelOptInfo,
) -> bool {
    let r1 = rel1.relids.clone().unwrap_or_default();
    let r2 = rel2.relids.clone().unwrap_or_default();
    let dl1 = rel1.direct_lateral_relids.clone().unwrap_or_default();
    let dl2 = rel2.direct_lateral_relids.clone().unwrap_or_default();
    // Direct lateral reference either way.
    bms_overlap(&r1, &dl2) || bms_overlap(&r2, &dl1)
    // PHV shared eval + outer-join ordering: placeholder_list / join_info_list are
    // empty on M7, so they contribute nothing.
}

/// PG `has_join_restriction` (the `old_rel` form used by join_search_one_level):
/// whether `rel` participates in any lateral/PHV/outer-join ordering restriction.
/// M7: lateral_relids empty, placeholder_list empty, join_info_list empty -> false.
fn has_join_restriction(_root: &mut PlannerInfo, _rel: &RelOptInfo) -> bool {
    false
}

/// PG `is_dummy_rel`: whether the rel has been proven empty (its single path is a
/// childless Append). M7 never marks a rel dummy (no constant-false restriction, no
/// empty rels), so no dummy Append paths exist -> false.
#[must_use]
pub fn is_dummy_rel(_rel: &RelOptInfo) -> bool {
    false
}

/// PG `mark_dummy_rel`: replace a rel's pathlist with a single childless-Append dummy
/// path. Not reached on the M7 inner-join path (no dummy rels); grows with outer
/// joins / constant-false restrictions.
pub fn mark_dummy_rel(_rel: &mut RelOptInfo) {
    not_yet_reachable("mark_dummy_rel: dummy (provably-empty) rel");
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

/// Extract the `RestrictInfo`s from a `Vec<Node>` of RestrictInfo-wrapped clauses
/// (the form `build_joinrel_restrictlist` returns).
fn extract_restrictinfos(nodes: &[Node]) -> Vec<Box<RestrictInfo>> {
    nodes
        .iter()
        .filter_map(|n| match n {
            Node::RestrictInfo(ri) => Some(ri.clone()),
            _ => None,
        })
        .collect()
}

/// Store the path-populated joinrel back into the PlannerInfo: replace the path-less
/// clone in `join_rel_list` (which build_join_rel appended) and add it to the current
/// DP level `join_rel_level[join_cur_level]` (if not already present there).
fn store_joinrel(root: &mut PlannerInfo, joinrel: Box<RelOptInfo>, joinrelids: &Bitmapset) {
    use crate::nodes::bitmapset::bms_equal;
    // Replace in join_rel_list.
    if let Some(slot) = root
        .join_rel_list
        .iter_mut()
        .find(|r| r.relids.as_ref().is_some_and(|x| bms_equal(x, joinrelids)))
    {
        slot.clone_from(&joinrel);
    } else {
        root.join_rel_list.push(joinrel.clone());
    }
    // Add to the current level (dedup: build_join_rel may produce the same joinrel
    // from multiple input pairs, but only the first construction adds it here).
    let level = root.join_cur_level as usize;
    if level > 0 && level < root.join_rel_level.len() {
        let already = root.join_rel_level[level]
            .iter()
            .any(|r| r.relids.as_ref().is_some_and(|x| bms_equal(x, joinrelids)));
        if already {
            // Update the existing level entry with the latest (more-paths) joinrel.
            if let Some(slot) = root.join_rel_level[level]
                .iter_mut()
                .find(|r| r.relids.as_ref().is_some_and(|x| bms_equal(x, joinrelids)))
            {
                *slot = joinrel;
            }
        } else {
            root.join_rel_level[level].push(joinrel);
        }
    }
}

// Keep find_base_rel / find_join_rel referenced (used once make_rel_from_joinlist
// in allpaths.rs drives the search; re-exported for that wiring).
const _: fn(&mut PlannerInfo, i32) -> Box<RelOptInfo> = find_base_rel;
#[allow(clippy::type_complexity, reason = "fn-pointer type assertion to keep find_join_rel referenced")]
const _: fn(&mut PlannerInfo, Relids) -> Option<Box<RelOptInfo>> = find_join_rel;

#[cold]
fn not_yet_reachable(what: &str) -> ! {
    unimplemented!("{what}: not yet translated for this milestone");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nodes::bitmapset::{bms_equal, bms_make_singleton};
    use crate::nodes::nodes::Node;
    use crate::nodes::pathnodes::{
        PathTarget, PathType, QualCost, RelOptKind, VolatileFunctionStatus,
    };
    use crate::nodes::primnodes::{OpExpr, Var, VarReturningType};
    use crate::postgres_ext::Oid;
    use crate::backend::optimizer::path::pathkeys::initialize_mergeclause_eclasses;
    use crate::backend::optimizer::util::pathnode::{create_seqscan_path, set_cheapest};
    use crate::backend::optimizer::util::relnode::make_node_reloptinfo;
    use crate::backend::optimizer::plan::initsplan::tests::test_planner_info;

    const INT4: Oid = Oid(23);
    const INT4_EQ: Oid = Oid(96); // "=" (int4,int4): merge + hash joinable

    fn make_var(varno: i32, varattno: i16) -> Node {
        Node::Var(Box::new(Var {
            varno,
            varattno,
            vartype: INT4,
            vartypmod: -1,
            varcollid: InvalidOid,
            varnullingrels: None,
            varlevelsup: 0,
            varreturningtype: VarReturningType::DEFAULT,
            varnosyn: varno as crate::nodes::primnodes::Index,
            varattnosyn: varattno,
            location: -1,
        }))
    }

    fn pathtarget_with(exprs: Vec<Node>) -> PathTarget {
        PathTarget {
            exprs,
            sortgrouprefs: Vec::new(),
            cost: QualCost { startup: 0.0, per_tuple: 0.0 },
            width: 4,
            has_volatile_expr: VolatileFunctionStatus::UNKNOWN,
        }
    }

    /// A base RelOptInfo for `relid` with a seqscan-able shape (RELATION rtekind,
    /// pages/tuples set) and a one-Var reltarget needed above the {1,2} join.
    fn make_scan_rel(relid: i32, tuples: f64, pages: u32) -> RelOptInfo {
        let mut rel = make_node_reloptinfo(RelOptKind::BASEREL);
        rel.relids = Some(bms_make_singleton(relid));
        rel.relid = relid as usize;
        rel.rtekind = crate::nodes::parsenodes::RTEKind::RELATION;
        rel.rows = tuples;
        rel.tuples = tuples;
        rel.pages = pages;
        rel.min_attr = 1;
        rel.max_attr = 1;
        let needed = bms_union(&bms_make_singleton(relid), &bms_make_singleton(3));
        rel.attr_needed = vec![Some(needed)];
        rel.attr_widths = vec![4];
        rel.reltarget = Some(Box::new(pathtarget_with(vec![make_var(relid, 1)])));
        rel
    }

    /// The join clause `rel1.x = rel2.y` as a RestrictInfo with the merge/hash fields
    /// set (as initsplan's check_mergejoinable/check_hashjoinable would) and the
    /// left/right ECs initialized (as distribute_qual_to_rels would).
    fn make_join_clause(root: &mut PlannerInfo) -> RestrictInfo {
        let clause = Node::OpExpr(Box::new(OpExpr {
            opno: INT4_EQ,
            opfuncid: InvalidOid,
            opresulttype: Oid(16),
            opretset: false,
            opcollid: InvalidOid,
            inputcollid: InvalidOid,
            args: vec![make_var(1, 1), make_var(2, 1)],
            location: -1,
        }));
        let relids12 = bms_union(&bms_make_singleton(1), &bms_make_singleton(2));
        let mut ri = blank_restrictinfo(clause, &relids12);
        ri.can_join = true;
        ri.left_relids = Some(bms_make_singleton(1));
        ri.right_relids = Some(bms_make_singleton(2));
        // check_mergejoinable / check_hashjoinable (lsyscache builtin table).
        ri.mergeopfamilies = crate::utils::lsyscache::get_mergejoin_opfamilies(INT4_EQ);
        ri.hashjoinoperator = INT4_EQ;
        // distribute: set the left/right ECs for the merge clause.
        initialize_mergeclause_eclasses(root, &mut ri);
        ri
    }

    fn blank_restrictinfo(clause: Node, required: &Relids) -> RestrictInfo {
        RestrictInfo {
            clause,
            is_pushed_down: false,
            can_join: false,
            pseudoconstant: false,
            has_clone: false,
            is_clone: false,
            leakproof: false,
            has_volatile: VolatileFunctionStatus::NOVOLATILE,
            security_level: 0,
            num_base_rels: 2,
            clause_relids: Some(required.clone()),
            required_relids: Some(required.clone()),
            incompatible_relids: None,
            outer_relids: None,
            left_relids: None,
            right_relids: None,
            orclause: None,
            rinfo_serial: 1,
            parent_ec: None,
            eval_cost: QualCost { startup: -1.0, per_tuple: -1.0 },
            norm_selec: -1.0,
            outer_selec: -1.0,
            mergeopfamilies: Vec::new(),
            left_ec: None,
            right_ec: None,
            left_em: None,
            right_em: None,
            scansel_cache: Vec::new(),
            outer_is_left: false,
            hashjoinoperator: InvalidOid,
            left_bucketsize: -1.0,
            right_bucketsize: -1.0,
            left_mcvfreq: -1.0,
            right_mcvfreq: -1.0,
            left_hasheqoperator: InvalidOid,
            right_hasheqoperator: InvalidOid,
        }
    }

    /// Build a PlannerInfo with two scan rels (each with a seqscan path + cheapest
    /// set) and the `a.x=b.y` join clause distributed into both rels' joininfo.
    fn setup_two_rels_with_join_clause() -> (PlannerInfo, Relids) {
        let mut root = test_planner_info();
        // EC merging is complete by the time the join search runs (query_planner sets
        // this); canonical pathkeys may then be built.
        root.ec_merging_done = true;
        let joinrelids = bms_union(&bms_make_singleton(1), &bms_make_singleton(2));

        let mut rel1 = make_scan_rel(1, 10.0, 1);
        let mut rel2 = make_scan_rel(2, 20.0, 2);

        // Seqscan path on each + set_cheapest.
        let p1 = create_seqscan_path(&mut root, &rel1, 0);
        crate::backend::optimizer::util::pathnode::add_path(&mut rel1, p1);
        set_cheapest(&mut rel1);
        let p2 = create_seqscan_path(&mut root, &rel2, 0);
        crate::backend::optimizer::util::pathnode::add_path(&mut rel2, p2);
        set_cheapest(&mut rel2);

        // The join clause, distributed into both rels' joininfo.
        let ri = make_join_clause(&mut root);
        rel1.joininfo.push(Box::new(ri.clone()));
        rel2.joininfo.push(Box::new(ri));

        root.simple_rel_array = vec![None, Some(Box::new(rel1)), Some(Box::new(rel2))];
        root.simple_rte_array = vec![None, None, None];
        (root, joinrelids)
    }

    fn path_count_by_type(joinrel: &RelOptInfo, ty: PathType) -> usize {
        joinrel.pathlist.iter().filter(|p| p.pathtype == ty).count()
    }

    #[test]
    fn op_join_predicates() {
        // int4 "=" is both merge- and hash-joinable; "<" is neither.
        assert!(crate::utils::lsyscache::op_mergejoinable(INT4_EQ, INT4));
        assert!(crate::utils::lsyscache::op_hashjoinable(INT4_EQ, INT4));
        let int4_lt = Oid(97);
        assert!(!crate::utils::lsyscache::op_mergejoinable(int4_lt, INT4));
        assert!(!crate::utils::lsyscache::op_hashjoinable(int4_lt, INT4));
        assert_eq!(
            crate::utils::lsyscache::get_mergejoin_opfamilies(INT4_EQ),
            vec![Oid(1976)]
        );
    }

    #[test]
    fn make_join_rel_generates_three_methods() {
        let (mut root, joinrelids) = setup_two_rels_with_join_clause();
        let rel1 = root.simple_rel_array[1].clone().unwrap();
        let rel2 = root.simple_rel_array[2].clone().unwrap();

        let joinrel = make_join_rel(&mut root, &rel1, &rel2);

        assert_eq!(joinrel.reloptkind, RelOptKind::JOINREL);
        assert!(bms_equal(joinrel.relids.as_ref().unwrap(), &joinrelids));

        // The join clause selectivity (DEFAULT_EQ_SEL=0.005) over 10x20 -> ~1 row.
        assert!(joinrel.rows >= 1.0 && joinrel.rows <= 200.0);

        // All three join methods generated (both input orderings -> 2 nestloops, etc.).
        assert!(path_count_by_type(&joinrel, PathType::NestLoop) >= 1, "expected a nestloop path");
        assert!(path_count_by_type(&joinrel, PathType::HashJoin) >= 1, "expected a hashjoin path");
        assert!(path_count_by_type(&joinrel, PathType::MergeJoin) >= 1, "expected a mergejoin path");

        // A cheapest path was chosen and all paths have finite, positive costs.
        let cheapest = joinrel.cheapest_total_path.as_ref().expect("cheapest total path");
        assert!(cheapest.total_cost.is_finite() && cheapest.total_cost > 0.0);
        for p in &joinrel.pathlist {
            assert!(p.total_cost.is_finite() && p.total_cost > 0.0);
            assert!(p.startup_cost <= p.total_cost + 1e-9);
        }
        // cheapest is the minimum-total in the pathlist.
        let min_total = joinrel.pathlist.iter().map(|p| p.total_cost).fold(f64::INFINITY, f64::min);
        assert!((cheapest.total_cost - min_total).abs() < 1e-9);
    }

    #[test]
    fn join_search_one_level_builds_final_joinrel() {
        let (mut root, joinrelids) = setup_two_rels_with_join_clause();
        let rel1 = root.simple_rel_array[1].clone().unwrap();
        let rel2 = root.simple_rel_array[2].clone().unwrap();

        // Drive the DP search directly: level 1 = the two base rels; level 2 joins them.
        root.all_query_rels = Some(joinrelids.clone());
        root.join_rel_level = vec![Vec::new(), vec![rel1, rel2], Vec::new()];
        join_search_one_level(&mut root, 2);

        assert_eq!(root.join_rel_level[2].len(), 1, "one level-2 joinrel");
        let top = &root.join_rel_level[2][0];
        assert!(bms_equal(top.relids.as_ref().unwrap(), &joinrelids));
        assert!(top.cheapest_total_path.is_some());
        // The final joinrel carries the three join methods.
        assert!(path_count_by_type(top, PathType::NestLoop) >= 1);
        assert!(path_count_by_type(top, PathType::HashJoin) >= 1);
        assert!(path_count_by_type(top, PathType::MergeJoin) >= 1);
    }
}
