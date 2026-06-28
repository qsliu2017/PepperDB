//! Translated from PostgreSQL src/include/optimizer/planner.h
//!
//! Note: the primary planner entry points (`planner`) are declared in
//! optimizer/optimizer.h; declarations here are for other planner modules.

#![allow(clippy::boxed_local, reason = "1:1 PG port: Box<Node>/Box<Path> mirrors PG pointer-passed nodes")]
#![allow(clippy::needless_pass_by_value, reason = "1:1 PG port: stubs take owned node values matching PG C signatures; consumed once implemented")]

use crate::nodes::nodes::AggSplit;
use crate::nodes::params::ParamListInfo;
use crate::nodes::parsenodes::{Query, RangeTblEntry, SetOperationStmt};
use crate::nodes::pathnodes::{
    Path, PlannerGlobal, PlannerInfo, RelOptInfo, UpperRelationKind,
};
use crate::nodes::plannodes::{PlannedStmt, RowMarkType};
use crate::nodes::primnodes::{Aggref, Expr};
use crate::nodes::lockoptions::LockClauseStrength;

/// Hook for plugins to get control in planner().
pub type PlannerHookType =
    fn(parse: &mut Query, query_string: &str, cursor_options: i32, bound_params: ParamListInfo) -> PlannedStmt;
pub static mut PLANNER_HOOK: Option<PlannerHookType> = None;

/// Hook for plugins to get control when grouping_planner() plans upper rels.
pub type CreateUpperPathsHookType = fn(
    root: &mut PlannerInfo,
    stage: UpperRelationKind,
    input_rel: &RelOptInfo,
    output_rel: &mut RelOptInfo,
);
pub static mut CREATE_UPPER_PATHS_HOOK: Option<CreateUpperPathsHookType> = None;

/// PG `standard_planner`. See `crate::backend::optimizer::plan::planner`.
/// (`boundParams` is nullable in C -> `Option<ParamListInfo>`.)
pub use crate::backend::optimizer::plan::planner::standard_planner;

/// PG `subquery_planner`. See `crate::backend::optimizer::plan::planner`.
pub use crate::backend::optimizer::plan::planner::subquery_planner;

pub fn select_rowmark_type(rte: &RangeTblEntry, strength: LockClauseStrength) -> RowMarkType {
    unimplemented!()
}

/// PG `limit_needed`. See `crate::backend::optimizer::plan::planner`.
pub use crate::backend::optimizer::plan::planner::limit_needed;

pub fn mark_partial_aggref(agg: &mut Aggref, aggsplit: AggSplit) {
    unimplemented!()
}

pub fn get_cheapest_fractional_path(rel: &RelOptInfo, tuple_fraction: f64) -> Path {
    unimplemented!()
}

pub fn preprocess_phv_expression(root: &mut PlannerInfo, expr: Box<Expr>) -> Box<Expr> {
    unimplemented!()
}
