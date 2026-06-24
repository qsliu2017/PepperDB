//! Translated from PostgreSQL src/include/optimizer/planner.h
//!
//! Note: the primary planner entry points (`planner`) are declared in
//! optimizer/optimizer.h; declarations here are for other planner modules.

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

pub fn standard_planner(
    parse: &mut Query,
    query_string: &str,
    cursor_options: i32,
    bound_params: ParamListInfo,
) -> PlannedStmt {
    unimplemented!()
}

pub fn subquery_planner(
    glob: &mut PlannerGlobal,
    parse: &mut Query,
    parent_root: Option<&mut PlannerInfo>,
    has_recursion: bool,
    tuple_fraction: f64,
    setops: Option<&SetOperationStmt>,
) -> PlannerInfo {
    unimplemented!()
}

pub fn select_rowmark_type(rte: &RangeTblEntry, strength: LockClauseStrength) -> RowMarkType {
    unimplemented!()
}

pub fn limit_needed(parse: &Query) -> bool {
    unimplemented!()
}

pub fn mark_partial_aggref(agg: &mut Aggref, aggsplit: AggSplit) {
    unimplemented!()
}

pub fn get_cheapest_fractional_path(rel: &RelOptInfo, tuple_fraction: f64) -> Path {
    unimplemented!()
}

pub fn preprocess_phv_expression(root: &mut PlannerInfo, expr: Box<Expr>) -> Box<Expr> {
    unimplemented!()
}
