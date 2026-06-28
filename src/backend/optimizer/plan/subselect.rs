//! Planning of sub-selects (SubLinks / SubPlans / initplans). Translated from
//! backend/optimizer/plan/subselect.c.
//!
//! Non-type-centric free functions; bodies here as snake_case `pub fn`s,
//! re-exported from `crate::optimizer::subselect` under the C names.
//!
//! Disposition: `grow`. M1 has no sublinks/subplans/initplans, so the SS_*
//! touch-points that standard_planner / subquery_planner call are near no-ops
//! over the plan tree. `ss_finalize_plan` (extParam/allParam computation) is only
//! invoked when Params exist, which never happens on the const path; it is a
//! pass-through here. The CTE/sublink-to-join conversions, correlation-var
//! replacement, and initplan machinery are grow guards (rules.md s4).

use crate::nodes::nodes::Node;
use crate::nodes::pathnodes::PlannerInfo;

/// Panic for a subselect path not yet translated for this milestone
/// (rules.md s4).
#[cold]
fn not_yet_reachable(what: &str) -> ! {
    unimplemented!("{what}: not yet translated for this milestone");
}

/// PG `SS_finalize_plan`: compute the extParam/allParam sets for every node of a
/// finished plan tree. Called by standard_planner only when Params were
/// generated; M1 generates none, so this is reached only if a Param appears
/// (then it grows). The pass-through over a const Result computes empty
/// param sets. `plan` is the polymorphic top plan node.
pub fn ss_finalize_plan(_root: &mut PlannerInfo, plan: &mut Node) {
    // finalize_plan recurses the tree accumulating Param IDs. A childless const
    // Result references no Params; its ext_param/all_param are empty.
    let Node::Result(result) = plan else {
        not_yet_reachable("SS_finalize_plan: non-Result top plan");
    };
    if result.plan.lefttree.is_some() {
        not_yet_reachable("SS_finalize_plan: subplan recursion");
    }
    if !result.plan.init_plan.is_empty() {
        not_yet_reachable("SS_finalize_plan: initplans");
    }
    result.plan.ext_param = None;
    result.plan.all_param = None;
}
