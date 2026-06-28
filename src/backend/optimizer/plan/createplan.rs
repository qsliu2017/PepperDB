//! Routines to create the desired plan for processing a query. Translated from
//! backend/optimizer/plan/createplan.c.
//!
//! Non-type-centric free functions; bodies here as snake_case `pub fn`s. The
//! public entry `create_plan` lives in planmain.rs (re-exported via
//! `crate::optimizer::planmain`); the recursion driver and the Result builder
//! live here.
//!
//! Disposition: `grow`. M1's live path is the Result plan for a FROM-less
//! SELECT: `create_plan_recurse` dispatches the path's `pathtype` and the
//! `T_Result` arm builds a childless `Result` from the path's pathtarget. The
//! scan/join/append/agg/sort/limit/... arms of the nodeTag switch are grow guards
//! (rules.md s4) and grow per milestone.

use crate::nodes::makefuncs::makeTargetEntry;
use crate::nodes::nodes::Node;
use crate::nodes::pathnodes::{Path, PathType, PlannerInfo};
use crate::nodes::plannodes::{Plan, Result};

/// Panic for a createplan path not yet translated for this milestone
/// (rules.md s4).
#[cold]
fn not_yet_reachable(what: &str) -> ! {
    unimplemented!("{what}: not yet translated for this milestone");
}

/// PG `create_plan_recurse`: recursively build a Plan from a Path. Dispatches on
/// the Path's pathtype (the NodeTag of the plan it builds). M1 lives the
/// `T_Result` arm; the rest grow per milestone.
pub fn create_plan_recurse(root: &mut PlannerInfo, best_path: &Path) -> Result {
    match best_path.pathtype {
        PathType::Result => {
            // PG distinguishes ProjectionPath / MinMaxAggPath / GroupResultPath /
            // simple RTE_RESULT scan here. For M1 the only Result path is the
            // group-result path of a FROM-less SELECT.
            create_group_result_plan(root, best_path)
        }
        other => not_yet_reachable(&format!("create_plan_recurse: {other:?}")),
    }
}

/// PG `create_group_result_plan`: build a Result plan for a GroupResultPath. The
/// plan's targetlist comes from the path's pathtarget (`build_path_tlist`); the
/// quals become the one-time `resconstantqual`. M1 has no quals.
fn create_group_result_plan(root: &mut PlannerInfo, best_path: &Path) -> Result {
    let tlist = build_path_tlist(root, best_path);

    // best_path->quals are the GroupResultPath's bare clauses; M1 has none. The
    // skeleton stores the embedded Path in the rel pathlist (planmain), so the
    // quals (always empty on the const path) are not carried here.
    let quals: Option<Node> = None;

    let mut plan = make_result(tlist, quals, None);
    copy_generic_path_info(&mut plan.plan, best_path);
    plan
}

/// PG `build_path_tlist`: build a targetlist from a path's pathtarget, assigning
/// resnos 1..n. Parameterized-path lateral-ref replacement is not reachable on
/// the M1 path (no param_info).
fn build_path_tlist(_root: &mut PlannerInfo, path: &Path) -> Vec<Node> {
    if path.param_info.is_some() {
        not_yet_reachable("build_path_tlist: parameterized path lateral refs");
    }
    let pathtarget = path
        .pathtarget
        .as_ref()
        .unwrap_or_else(|| not_yet_reachable("build_path_tlist: missing pathtarget"));
    let has_sortgrouprefs = !pathtarget.sortgrouprefs.is_empty();

    pathtarget
        .exprs
        .iter()
        .enumerate()
        .map(|(i, expr)| {
            let mut tle = makeTargetEntry(Some(expr.clone()), (i + 1) as i16, None, false);
            if has_sortgrouprefs {
                tle.ressortgroupref = pathtarget.sortgrouprefs[i];
            }
            Node::TargetEntry(Box::new(tle))
        })
        .collect()
}

/// PG `make_result`: construct a Result plan node with the given tlist and
/// one-time qual (`resconstantqual`), over an optional subplan.
fn make_result(
    tlist: Vec<Node>,
    resconstantqual: Option<Node>,
    subplan: Option<Node>,
) -> Result {
    Result {
        plan: Plan {
            disabled_nodes: 0,
            startup_cost: 0.0,
            total_cost: 0.0,
            plan_rows: 0.0,
            plan_width: 0,
            parallel_aware: false,
            parallel_safe: false,
            async_capable: false,
            plan_node_id: 0,
            targetlist: tlist,
            qual: Vec::new(),
            lefttree: subplan,
            righttree: None,
            init_plan: Vec::new(),
            ext_param: None,
            all_param: None,
        },
        resconstantqual,
    }
}

/// PG `copy_generic_path_info`: copy the Path's cost/row/width/parallel info onto
/// the Plan node.
fn copy_generic_path_info(dest: &mut Plan, src: &Path) {
    dest.disabled_nodes = src.disabled_nodes;
    dest.startup_cost = src.startup_cost;
    dest.total_cost = src.total_cost;
    dest.plan_rows = src.rows;
    dest.plan_width = src
        .pathtarget
        .as_ref()
        .map_or(0, |t| t.width);
    dest.parallel_aware = src.parallel_aware;
    dest.parallel_safe = src.parallel_safe;
}
