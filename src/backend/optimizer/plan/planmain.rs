//! Routines to find the optimal scan/join paths. Translated from
//! backend/optimizer/plan/planmain.c.
//!
//! `query_planner` is the planner's scan/join entry point. Non-type-centric free
//! functions; bodies here as snake_case `pub fn`s, re-exported from
//! `crate::optimizer::planmain` under the C names (`create_plan` is also declared
//! there in PG, defined in createplan.c).
//!
//! Disposition: `grow`. M1's live path is the trivial FROM-less case:
//! query_planner builds the dummy result RelOptInfo and its one Result path,
//! then `create_plan` turns the cheapest path into a Plan. The general
//! join-search (`add_base_rels_to_query` -> deconstruct_jointree -> EC merging ->
//! `make_one_rel`) is a grow guard (rules.md s4); it is reached only once the
//! query has an actual rangetable (M3+).

use crate::nodes::pathnodes::{
    AmFlags, Path, PathTarget, PlannerInfo, QualCost, RelOptInfo, RelOptKind,
};
use crate::nodes::parsenodes::RTEKind;
use crate::backend::optimizer::plan::createplan::create_plan_recurse;
use crate::backend::optimizer::util::pathnode::{add_path, create_group_result_path, set_cheapest};
use crate::backend::optimizer::util::tlist::make_pathtarget_from_tlist;
use crate::postgres_ext::InvalidOid;

/// callback to compute the query's pathkeys (PG `query_pathkeys_callback`).
pub type QueryPathkeysCallback = fn(root: &mut PlannerInfo);

/// Panic for a planmain path not yet translated for this milestone (rules.md s4).
#[cold]
fn not_yet_reachable(what: &str) -> ! {
    unimplemented!("{what}: not yet translated for this milestone");
}

/// Whether the base scan/join computes a distinct *group/agg-input* tlist (the
/// flattened Vars an Agg/WindowAgg then reprojects into the final Aggref-bearing
/// tlist) rather than projecting the final `processed_tlist` directly. True for
/// grouping/aggregation/window queries, and for a SELECT whose tlist contains
/// set-returning functions (a ProjectSet node reprojects, PG's
/// `split_pathtarget_at_srfs` split). A sort/distinct/limit-only query has no
/// reprojecting node, so the scan projects `processed_tlist`. Note this can be
/// true while `scan_input_tlist` is empty (a bare `count(*)` or a const-args SRF
/// has no input Vars).
pub(crate) fn query_computes_scan_input_tlist(root: &PlannerInfo) -> bool {
    root.parse.hasAggs
        || root.parse.hasWindowFuncs
        || !root.parse.windowClause.is_empty()
        || !root.parse.groupClause.is_empty()
        || (root.parse.commandType == crate::nodes::nodes::CmdType::SELECT
            && tlist_returns_set(&root.processed_tlist))
}

/// Does any entry of a (TargetEntry-wrapped) targetlist contain a set-returning
/// function? The tlist half of PG's `parse->hasTargetSRFs` (detected structurally
/// here; the parser flag is not populated in this port).
pub(crate) fn tlist_returns_set(tlist: &[crate::nodes::nodes::Node]) -> bool {
    tlist
        .iter()
        .any(crate::backend::nodes::nodeFuncs::expression_returns_set)
}

/// PG `query_planner`: generate paths for the scan/join portion of the query.
/// Returns the RelOptInfo for the topmost scan/join relation.
///
/// M1 lives the trivial case: a FROM-less SELECT (empty jointree). PG models
/// this by injecting an RTE_RESULT relation in `replace_empty_jointree` and
/// special-casing a single RTE_RESULT here; for M1 we keep the rangetable empty
/// (the analyze/rewrite output has no RTEs and the PlannedStmt rtable must stay
/// empty), and build the dummy result rel directly. The full join-search grows
/// in M3+ when the query has base relations.
pub fn query_planner(root: &mut PlannerInfo, qp_callback: QueryPathkeysCallback) -> RelOptInfo {
    use crate::nodes::nodes::Node;

    // The jointree always holds one (FROM-less SELECT -> RTE_RESULT) or more
    // RangeTblRef items after replace_empty_jointree. A multi-item fromlist (a join)
    // takes the full deconstruct_jointree + make_one_rel path.
    let joinlist = jointree_fromlist(root);
    if joinlist.len() >= 2 {
        return query_planner_join(root, qp_callback);
    }
    if joinlist.len() != 1 {
        not_yet_reachable("query_planner: empty jointree");
    }
    let Node::RangeTblRef(rtr) = &joinlist[0] else {
        not_yet_reachable("query_planner: non-RangeTblRef jointree item");
    };
    let rti = rtr.rtindex as usize;
    let rtekind = rte_kind(root, rti);

    // Build the dummy result rel's / base rel's reltarget from the processed tlist
    // (the const exprs or the SELECT-list Vars).
    let reltarget = make_pathtarget_from_tlist_nodes(root);

    let final_rel = match rtekind {
        crate::nodes::parsenodes::RTEKind::RESULT => {
            build_result_rel_with_path(root, reltarget)
        }
        crate::nodes::parsenodes::RTEKind::RELATION => {
            build_scan_rel_with_path(root, rti, reltarget)
        }
        crate::nodes::parsenodes::RTEKind::VALUES => {
            build_values_rel_with_path(root, rti, reltarget)
        }
        crate::nodes::parsenodes::RTEKind::FUNCTION => {
            build_function_rel_with_path(root, rti, reltarget)
        }
        other => not_yet_reachable(&format!("query_planner: FROM item RTE kind {other:?}")),
    };

    // We don't need generate_base_implied_equalities, but must pretend EC merging
    // is complete.
    root.ec_merging_done = true;

    // Still required to call qp_callback (e.g. "SELECT 2+2 ORDER BY 1").
    qp_callback(root);

    final_rel
}

/// PG `query_planner` multi-relation path: build a `RelOptInfo` per base relation,
/// mark the needed Vars (`build_base_rel_tlists`), distribute the jointree quals
/// (`deconstruct_jointree`), then run the join search (`make_one_rel`) to produce the
/// final joinrel. M7 covers an inner join over base relations (a flat FROM list with
/// a WHERE join clause); explicit JOIN syntax / outer joins grow later.
fn query_planner_join(root: &mut PlannerInfo, qp_callback: QueryPathkeysCallback) -> RelOptInfo {
    use crate::nodes::pathnodes::JoinDomain;

    // setup_simple_rel_arrays: size the per-RT-index arrays from the rtable.
    setup_simple_rel_arrays(root);

    // The top-level join domain (deconstruct_jointree expects join_domains[0]).
    root.join_domains = vec![Box::new(JoinDomain { jd_relids: None })];

    // add_base_rels_to_query: a base RelOptInfo per RangeTblRef in the jointree.
    let jointree = root
        .parse
        .jointree
        .clone()
        .unwrap_or_else(|| not_yet_reachable("query_planner: missing jointree"));
    crate::backend::optimizer::plan::initsplan::add_base_rels_to_query(root, &jointree);

    // build_base_rel_tlists: mark every Var in the final (scan-input or processed)
    // tlist as needed, so it propagates into each base rel's reltarget. The
    // scan-input tlist is populated only for grouping/window queries (an Agg node
    // reprojects); otherwise the scan projects the final `processed_tlist`.
    let final_tlist = if query_computes_scan_input_tlist(root) {
        root.scan_input_tlist.clone()
    } else {
        root.processed_tlist.clone()
    };
    crate::backend::optimizer::plan::initsplan::build_base_rel_tlists(root, &final_tlist);

    // deconstruct_jointree: distribute the jointree's quals to the rels (baserestrict
    // or joininfo), and return the joinlist for the search.
    let joinlist = crate::backend::optimizer::plan::initsplan::deconstruct_jointree(root);

    // generate_base_implied_equalities: finalize the ECs (sets ec_merging_done) and
    // stamp each base rel's `eclass_indexes`/`has_eclass_joins`, so the join search's
    // `generate_join_implied_equalities` can find the equijoin clauses absorbed into
    // ECs (e.g. `a.x=b.y`). Without this the joinrel restrictlist is empty -> cross
    // product.
    crate::backend::optimizer::path::equivclass::generate_base_implied_equalities(root);
    qp_callback(root);

    crate::backend::optimizer::path::allpaths::make_one_rel(root, &joinlist)
}

/// The FROM item RTE kind at RT index `rti`.
fn rte_kind(root: &PlannerInfo, rti: usize) -> crate::nodes::parsenodes::RTEKind {
    use crate::nodes::nodes::Node;
    let Node::RangeTblEntry(rte) = &root.parse.rtable[rti - 1] else {
        not_yet_reachable("query_planner: rangetable entry is not an RTE");
    };
    rte.rtekind
}

/// The jointree's fromlist (a clone of its RangeTblRef items).
fn jointree_fromlist(root: &PlannerInfo) -> Vec<crate::nodes::nodes::Node> {
    use crate::nodes::nodes::Node;
    match root.parse.jointree.as_ref() {
        Some(Node::FromExpr(f)) => f.fromlist.clone(),
        None => Vec::new(),
        Some(_) => not_yet_reachable("query_planner: non-FromExpr jointree"),
    }
}

/// Build the dummy RTE_RESULT rel and its single Result path (the FROM-less SELECT
/// degenerate-grouping case). The reltarget carries the const tlist.
fn build_result_rel_with_path(root: &mut PlannerInfo, reltarget: PathTarget) -> RelOptInfo {
    let mut final_rel = make_result_rel(reltarget);
    let reltarget_clone = final_rel
        .reltarget
        .clone()
        .unwrap_or_else(|| not_yet_reachable("query_planner: missing reltarget"));
    let grp = create_group_result_path(root, &final_rel, &reltarget_clone, Vec::new());
    crate::assert!(grp.quals.is_empty());
    add_path(&mut final_rel, Box::new(grp.path));
    set_cheapest(&mut final_rel);
    final_rel
}

/// Build the base RelOptInfo for the relation at RT index `rti`, fill it from the
/// relcache (`get_relation_info`), set its reltarget (the SELECT-list Vars), and
/// run `make_one_rel` to add the seqscan path and select the cheapest.
fn build_scan_rel_with_path(
    root: &mut PlannerInfo,
    rti: usize,
    reltarget: PathTarget,
) -> RelOptInfo {
    use crate::nodes::nodes::Node;

    // setup_simple_rel_arrays: size the per-RT-index arrays to match the rtable.
    setup_simple_rel_arrays(root);

    // build_simple_rel: a base RelOptInfo for this rti, then get_relation_info.
    let relid = {
        let Node::RangeTblEntry(rte) = &root.parse.rtable[rti - 1] else {
            not_yet_reachable("query_planner: rangetable entry is not an RTE");
        };
        rte.relid
    };
    let mut rel = make_base_rel(rti, reltarget);
    crate::backend::optimizer::util::plancat::get_relation_info(root, relid, false, &mut rel);

    // deconstruct_jointree (initsplan): distribute the WHERE quals to the rels
    // they reference. M3 has a single base rel, so every jointree qual is a
    // base-restriction clause on it. Wrap each AND sub-clause in a RestrictInfo
    // and append to baserestrictinfo before path generation.
    distribute_jointree_quals_to_baserel(root, &mut rel);

    // Park the rel in the simple_rel_array so make_one_rel can find it by index.
    root.simple_rel_array[rti] = Some(Box::new(rel));

    let joinlist = jointree_fromlist(root);
    crate::backend::optimizer::path::allpaths::make_one_rel(root, &joinlist)
}

/// Build the base RelOptInfo for a VALUES RTE at RT index `rti` and run
/// `make_one_rel` to add its ValuesScan path. Unlike a plain relation there is no
/// relcache lookup; PG's `set_values_size_estimates` sets `tuples = number of
/// rows`, which `set_base_rel_sizes` turns into the row estimate.
fn build_values_rel_with_path(
    root: &mut PlannerInfo,
    rti: usize,
    reltarget: PathTarget,
) -> RelOptInfo {
    use crate::nodes::nodes::Node;

    setup_simple_rel_arrays(root);

    let nrows = {
        let Node::RangeTblEntry(rte) = &root.parse.rtable[rti - 1] else {
            not_yet_reachable("query_planner: rangetable entry is not an RTE");
        };
        rte.values_lists.len()
    };

    let mut rel = make_base_rel(rti, reltarget);
    rel.rtekind = crate::nodes::parsenodes::RTEKind::VALUES;
    // set_values_size_estimates: rel->tuples = list_length(values_lists).
    rel.tuples = nrows as f64;

    distribute_jointree_quals_to_baserel(root, &mut rel);

    root.simple_rel_array[rti] = Some(Box::new(rel));

    let joinlist = jointree_fromlist(root);
    crate::backend::optimizer::path::allpaths::make_one_rel(root, &joinlist)
}

/// Build the base rel + FunctionScan path for a function-in-FROM RTE. Mirrors
/// `build_values_rel_with_path`. `set_function_size_estimates` uses PG's default
/// per-function row estimate (100) when the function has no support-node estimate.
fn build_function_rel_with_path(
    root: &mut PlannerInfo,
    rti: usize,
    reltarget: PathTarget,
) -> RelOptInfo {
    setup_simple_rel_arrays(root);

    let mut rel = make_base_rel(rti, reltarget);
    rel.rtekind = crate::nodes::parsenodes::RTEKind::FUNCTION;
    // set_function_size_estimates: PG's default per-function row estimate.
    rel.tuples = 100.0;

    distribute_jointree_quals_to_baserel(root, &mut rel);

    root.simple_rel_array[rti] = Some(Box::new(rel));

    let joinlist = jointree_fromlist(root);
    crate::backend::optimizer::path::allpaths::make_one_rel(root, &joinlist)
}

/// initsplan `deconstruct_jointree` / `distribute_restrictinfo_to_rels` (M3
/// single-rel subset): take the jointree's WHERE quals (an implicit-AND list),
/// wrap each clause in a RestrictInfo, and append to the single base rel's
/// `baserestrictinfo`. With one base relation every qual is a base restriction;
/// the join-clause distribution grows with joins.
fn distribute_jointree_quals_to_baserel(root: &mut PlannerInfo, rel: &mut RelOptInfo) {
    use crate::backend::nodes::makefuncs::make_ands_implicit;
    use crate::backend::optimizer::util::restrictinfo::make_simple_restrictinfo;
    use crate::nodes::nodes::Node;

    let quals = match root.parse.jointree.as_ref() {
        Some(Node::FromExpr(f)) => f.quals.clone(),
        _ => None,
    };
    for clause in make_ands_implicit(quals) {
        let rinfo = make_simple_restrictinfo(root, Box::new(clause));
        rel.baserestrictinfo.push(Box::new(rinfo));
    }
}

/// PG `setup_simple_rel_arrays`: size `simple_rel_array`/`simple_rte_array` to
/// `rtable.len() + 1` (1-based RT indexes), filling the RTE array from the rtable.
fn setup_simple_rel_arrays(root: &mut PlannerInfo) {
    use crate::nodes::nodes::Node;
    let n = root.parse.rtable.len() + 1;
    root.simple_rel_array = (0..n).map(|_| None).collect();
    root.simple_rte_array = (0..n).map(|_| None).collect();
    for (i, rte) in root.parse.rtable.iter().enumerate() {
        let Node::RangeTblEntry(rte) = rte else {
            not_yet_reachable("setup_simple_rel_arrays: rangetable entry is not an RTE");
        };
        root.simple_rte_array[i + 1] = Some(rte.clone());
    }
}

/// Build the base scan rel's PathTarget. When the query groups/aggregates, the scan
/// computes the `scan_input_tlist` (the flattened group/agg-input Vars) rather than
/// the final `processed_tlist` (which carries the Aggrefs, computed above by the Agg
/// node). Both hold `Node` (TargetEntry-wrapped); unwrap for make_pathtarget_from_tlist.
fn make_pathtarget_from_tlist_nodes(root: &PlannerInfo) -> PathTarget {
    use crate::nodes::nodes::Node;
    let source = if query_computes_scan_input_tlist(root) {
        &root.scan_input_tlist
    } else {
        &root.processed_tlist
    };
    let tlist: Vec<_> = source
        .iter()
        .map(|n| match n {
            Node::TargetEntry(te) => (**te).clone(),
            _ => not_yet_reachable("query_planner: tlist entry is not a TargetEntry"),
        })
        .collect();
    make_pathtarget_from_tlist(&tlist)
}

/// PG `makeNode(RelOptInfo)` for the dummy result relation, with the fields the
/// M1 path reads set, and everything else zero/empty (palloc0 semantics). The
/// reltarget carries the const tlist.
fn make_result_rel(reltarget: PathTarget) -> RelOptInfo {
    RelOptInfo {
        reloptkind: RelOptKind::BASEREL,
        relids: None,
        rows: 1.0,
        consider_startup: false,
        consider_param_startup: false,
        consider_parallel: false,
        reltarget: Some(Box::new(reltarget)),
        pathlist: Vec::new(),
        ppilist: Vec::new(),
        partial_pathlist: Vec::new(),
        cheapest_startup_path: None,
        cheapest_total_path: None,
        cheapest_unique_path: None,
        cheapest_parameterized_paths: Vec::new(),
        direct_lateral_relids: None,
        lateral_relids: None,
        relid: 0,
        reltablespace: InvalidOid,
        rtekind: RTEKind::RESULT,
        min_attr: 0,
        max_attr: 0,
        attr_needed: Vec::new(),
        attr_widths: Vec::new(),
        notnullattnums: None,
        nulling_relids: None,
        lateral_vars: Vec::new(),
        lateral_referencers: None,
        indexlist: Vec::new(),
        statlist: Vec::new(),
        pages: 0,
        tuples: 0.0,
        allvisfrac: 0.0,
        eclass_indexes: None,
        subroot: None,
        subplan_params: Vec::new(),
        rel_parallel_workers: -1,
        amflags: AmFlags::empty(),
        serverid: InvalidOid,
        userid: InvalidOid,
        useridiscurrent: false,
        unique_for_rels: Vec::new(),
        non_unique_for_rels: Vec::new(),
        baserestrictinfo: Vec::new(),
        baserestrictcost: QualCost { startup: 0.0, per_tuple: 0.0 },
        baserestrict_min_security: 0,
        joininfo: Vec::new(),
        has_eclass_joins: false,
        consider_partitionwise_join: false,
        parent: None,
        top_parent: None,
        top_parent_relids: None,
        part_scheme: None,
        nparts: -1,
        partbounds_merged: false,
        partition_qual: Vec::new(),
        part_rels: Vec::new(),
        live_parts: None,
        all_partrels: None,
        partexprs: Vec::new(),
        nullable_partexprs: Vec::new(),
    }
}

/// PG `build_simple_rel` (M2 subset): a base `RelOptInfo` for the relation at RT
/// index `rti`, with reloptkind BASEREL, rtekind RELATION, and the given reltarget.
/// `get_relation_info` fills the attribute/size fields afterward.
fn make_base_rel(rti: usize, reltarget: PathTarget) -> RelOptInfo {
    let mut rel = make_result_rel(reltarget);
    rel.relid = rti as crate::nodes::primnodes::Index;
    rel.rtekind = RTEKind::RELATION;
    rel
}

/// PG `create_plan`: top-level driver to turn the chosen Path into a Plan tree.
/// Returns the polymorphic top plan node (`Plan *` in C). For M1 the path is the
/// Result path; create_plan_recurse builds the Result node, and
/// apply_tlist_labeling stamps the original column names.
pub fn create_plan(root: &mut PlannerInfo, best_path: &Path) -> crate::nodes::nodes::Node {
    crate::assert!(root.plan_params.is_empty());

    root.cur_outer_rels = None;
    root.cur_outer_params = Vec::new();

    // CP_EXACT_TLIST: demand the exact result tlist.
    let mut plan = create_plan_recurse(root, best_path);

    // Stamp the original column names / decoration onto the top-level tlist.
    apply_top_tlist_labeling(root, &mut plan);

    // SS_attach_initplans: none on the M1/M2 path.
    crate::assert!(root.cur_outer_params.is_empty());
    root.plan_params = Vec::new();

    plan
}

/// `apply_tlist_labeling(plan->targetlist, root->processed_tlist)` over the top
/// plan node's targetlist (Result or SeqScan). Both are `Vec<Node>` of TargetEntries.
/// When the query groups, the base scan computes `scan_input_tlist` (the group/agg
/// input Vars), so the scan plan is labeled against that; the final names land on
/// the Agg's tlist (labeled when the upper plan is assembled).
fn apply_top_tlist_labeling(root: &PlannerInfo, plan: &mut crate::nodes::nodes::Node) {
    let label_src = if query_computes_scan_input_tlist(root) {
        &root.scan_input_tlist
    } else {
        &root.processed_tlist
    };
    let tlist = top_plan_tlist_mut(plan);
    let mut dest: Vec<_> = tlist
        .iter()
        .map(|n| match n {
            crate::nodes::nodes::Node::TargetEntry(te) => (**te).clone(),
            _ => not_yet_reachable("apply_tlist_labeling: plan tlist entry is not a TargetEntry"),
        })
        .collect();
    let src: Vec<_> = label_src
        .iter()
        .map(|n| match n {
            crate::nodes::nodes::Node::TargetEntry(te) => (**te).clone(),
            _ => not_yet_reachable("apply_tlist_labeling: tlist entry is not a TargetEntry"),
        })
        .collect();
    crate::backend::optimizer::util::tlist::apply_tlist_labeling(&mut dest, &src);
    *tlist = dest
        .into_iter()
        .map(|te| crate::nodes::nodes::Node::TargetEntry(Box::new(te)))
        .collect();
}

/// Borrow the top plan node's targetlist (the `Plan.targetlist` of whichever
/// concrete plan node M1/M2 produces).
fn top_plan_tlist_mut(plan: &mut crate::nodes::nodes::Node) -> &mut Vec<crate::nodes::nodes::Node> {
    use crate::nodes::nodes::Node;
    match plan {
        Node::Result(r) => &mut r.plan.targetlist,
        Node::SeqScan(s) => &mut s.scan.plan.targetlist,
        Node::IndexScan(s) => &mut s.scan.plan.targetlist,
        Node::IndexOnlyScan(s) => &mut s.scan.plan.targetlist,
        Node::BitmapHeapScan(s) => &mut s.scan.plan.targetlist,
        Node::ValuesScan(v) => &mut v.scan.plan.targetlist,
        Node::FunctionScan(f) => &mut f.scan.plan.targetlist,
        Node::NestLoop(n) => &mut n.join.plan.targetlist,
        Node::MergeJoin(m) => &mut m.join.plan.targetlist,
        Node::HashJoin(h) => &mut h.join.plan.targetlist,
        _ => not_yet_reachable("apply_tlist_labeling: unexpected top plan node"),
    }
}
