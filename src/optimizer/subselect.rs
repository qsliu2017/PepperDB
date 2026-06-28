//! Translated from PostgreSQL src/include/optimizer/subselect.h

#![allow(clippy::boxed_local, reason = "1:1 PG port: Node/Box<Path> mirrors PG pointer-passed nodes")]
#![allow(clippy::needless_pass_by_value, reason = "1:1 PG port: stubs take owned node values matching PG C signatures; consumed once implemented")]

use crate::nodes::nodes::{Cost, Node};
use crate::nodes::parsenodes::Query;
use crate::nodes::pathnodes::{PlannerInfo, RelOptInfo, Relids};
use crate::nodes::plannodes::Plan;
use crate::nodes::primnodes::{JoinExpr, Param, ScalarArrayOpExpr, SubLink};
use crate::postgres_ext::Oid;

pub fn ss_process_ctes(root: &mut PlannerInfo) {
    unimplemented!()
}

pub fn convert_values_to_any(
    root: &mut PlannerInfo,
    testexpr: &Node,
    values: &Query,
) -> ScalarArrayOpExpr {
    unimplemented!()
}

pub fn convert_any_sublink_to_join(
    root: &mut PlannerInfo,
    sublink: &SubLink,
    available_rels: Relids,
) -> Option<JoinExpr> {
    unimplemented!()
}

pub fn convert_exists_sublink_to_join(
    root: &mut PlannerInfo,
    sublink: &SubLink,
    under_not: bool,
    available_rels: Relids,
) -> Option<JoinExpr> {
    unimplemented!()
}

pub fn ss_replace_correlation_vars(root: &mut PlannerInfo, expr: Node) -> Node {
    unimplemented!()
}

pub fn ss_process_sublinks(root: &mut PlannerInfo, expr: Node, is_qual: bool) -> Node {
    unimplemented!()
}

pub fn ss_identify_outer_params(root: &mut PlannerInfo) {
    unimplemented!()
}

pub fn ss_charge_for_initplans(root: &mut PlannerInfo, final_rel: &mut RelOptInfo) {
    unimplemented!()
}

/// out-params `initplan_cost_p`/`unsafe_initplans_p` folded into the tuple.
pub fn ss_compute_initplan_cost(init_plans: &[Plan]) -> (Cost, bool) {
    unimplemented!()
}

pub fn ss_attach_initplans(root: &mut PlannerInfo, plan: &mut Plan) {
    unimplemented!()
}

/// PG `SS_finalize_plan`. See `crate::backend::optimizer::plan::subselect`.
/// (`plan` is the polymorphic top plan node -> `&mut Node`.)
pub use crate::backend::optimizer::plan::subselect::ss_finalize_plan;

pub fn ss_make_initplan_output_param(
    root: &mut PlannerInfo,
    resulttype: Oid,
    resulttypmod: i32,
    resultcollation: Oid,
) -> Param {
    unimplemented!()
}

pub fn ss_make_initplan_from_plan(
    root: &mut PlannerInfo,
    subroot: &mut PlannerInfo,
    plan: &mut Plan,
    prm: &Param,
) {
    unimplemented!()
}
