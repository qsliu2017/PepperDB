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
    let parse = &root.parse;

    // setup_simple_rel_arrays would size the per-RT-index arrays; the M1 rel is
    // not RT-indexed (no rangetable), so they stay empty.

    // The trivial case: the jointree has no base relations to scan. PG checks for
    // a single RangeTblRef to an RTE_RESULT; here the equivalent is an empty
    // jointree fromlist (no FROM clause) with an empty rangetable.
    let from_is_empty = jointree_is_empty(root);
    if !from_is_empty {
        not_yet_reachable("query_planner: scan/join search over base relations");
    }
    if !parse.rtable.is_empty() {
        not_yet_reachable("query_planner: non-empty rangetable");
    }

    // Build the RelOptInfo for the dummy result relation directly. Its reltarget
    // carries the (already const-folded) processed tlist, so build_path_tlist
    // produces the final const TargetEntry list.
    let reltarget = make_pathtarget_from_tlist_nodes(root);
    let mut final_rel = make_result_rel(reltarget);

    // The only path is a trivial Result path. We use create_group_result_path
    // (PG: a FROM-less SELECT is a degenerate-grouping case), jamming the
    // jointree quals in unprocessed. M1 has no quals.
    let reltarget_clone = final_rel
        .reltarget
        .clone()
        .unwrap_or_else(|| not_yet_reachable("query_planner: missing reltarget"));
    let grp = create_group_result_path(root, &final_rel, &reltarget_clone, Vec::new());
    // The pathlist holds base `Path`s; createplan re-discriminates the Result by
    // pathtype + pathtarget, and the GroupResultPath's `quals` are empty on the
    // const path, so the embedded Path carries everything M1 needs.
    crate::assert!(grp.quals.is_empty());
    add_path(&mut final_rel, Box::new(grp.path));

    // Select cheapest path (trivial: a single path).
    set_cheapest(&mut final_rel);

    // We don't need generate_base_implied_equalities, but must pretend EC merging
    // is complete.
    root.ec_merging_done = true;

    // Still required to call qp_callback (e.g. "SELECT 2+2 ORDER BY 1").
    qp_callback(root);

    final_rel
}

/// Is the query's FROM clause empty (no base relations)? The M1 analyze/rewrite
/// output represents a FROM-less SELECT as an empty FromExpr fromlist and an
/// empty rangetable.
fn jointree_is_empty(root: &PlannerInfo) -> bool {
    use crate::nodes::nodes::Node;
    match root.parse.jointree.as_deref() {
        Some(Node::FromExpr(f)) => f.fromlist.is_empty(),
        // No jointree at all is also "empty FROM" for our purposes.
        None => true,
        Some(_) => not_yet_reachable("query_planner: non-FromExpr jointree"),
    }
}

/// Build the dummy result rel's PathTarget from `root.processed_tlist`. The
/// processed_tlist holds `Box<Node>` (TargetEntry-wrapped); unwrap them for
/// make_pathtarget_from_tlist.
fn make_pathtarget_from_tlist_nodes(root: &PlannerInfo) -> PathTarget {
    use crate::nodes::nodes::Node;
    let tlist: Vec<_> = root
        .processed_tlist
        .iter()
        .map(|n| match &**n {
            Node::TargetEntry(te) => (**te).clone(),
            _ => not_yet_reachable("query_planner: processed_tlist entry is not a TargetEntry"),
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

/// PG `create_plan`: top-level driver to turn the chosen Path into a Plan tree.
/// Returns the polymorphic top plan node (`Plan *` in C). For M1 the path is the
/// Result path; create_plan_recurse builds the Result node, and
/// apply_tlist_labeling stamps the original column names.
pub fn create_plan(root: &mut PlannerInfo, best_path: &Path) -> Box<crate::nodes::nodes::Node> {
    use crate::nodes::nodes::Node;
    crate::assert!(root.plan_params.is_empty());

    root.cur_outer_rels = None;
    root.cur_outer_params = Vec::new();

    // CP_EXACT_TLIST: demand the exact result tlist.
    let mut plan = create_plan_recurse(root, best_path);

    // Stamp the original column names / decoration onto the top-level tlist.
    apply_top_tlist_labeling(root, &mut plan);

    // SS_attach_initplans: none on the M1 path.
    crate::assert!(root.cur_outer_params.is_empty());
    root.plan_params = Vec::new();

    Box::new(Node::Result(Box::new(plan)))
}

/// `apply_tlist_labeling(plan->targetlist, root->processed_tlist)` over the
/// Result node's plan targetlist. Both are `Vec<Box<Node>>` of TargetEntries.
fn apply_top_tlist_labeling(root: &PlannerInfo, plan: &mut crate::nodes::plannodes::Result) {
    use crate::nodes::nodes::Node;
    let mut dest: Vec<_> = plan
        .plan
        .targetlist
        .iter()
        .map(|n| match &**n {
            Node::TargetEntry(te) => (**te).clone(),
            _ => not_yet_reachable("apply_tlist_labeling: plan tlist entry is not a TargetEntry"),
        })
        .collect();
    let src: Vec<_> = root
        .processed_tlist
        .iter()
        .map(|n| match &**n {
            Node::TargetEntry(te) => (**te).clone(),
            _ => not_yet_reachable("apply_tlist_labeling: processed_tlist entry is not a TargetEntry"),
        })
        .collect();
    crate::backend::optimizer::util::tlist::apply_tlist_labeling(&mut dest, &src);
    plan.plan.targetlist = dest
        .into_iter()
        .map(|te| Box::new(Node::TargetEntry(Box::new(te))))
        .collect();
}
