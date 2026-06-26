//! Translated from PostgreSQL src/include/optimizer/clauses.h
//! prototypes for clauses.c.

#![allow(clippy::needless_pass_by_value, reason = "1:1 PG port: stubs take owned node values matching PG C signatures; consumed once implemented")]

use crate::nodes::bitmapset::Bitmapset;
use crate::nodes::nodes::Node;
use crate::nodes::parsenodes::{Query, RangeTblEntry};
use crate::nodes::pathnodes::{PlannerInfo, Relids};
use crate::nodes::primnodes::{OpExpr, Var};

#[derive(Debug, Clone, PartialEq)]
pub struct WindowFuncLists {
    /// total number of WindowFuncs found
    pub num_window_funcs: i32,
    /// window_funcs[] is indexed 0 .. max_win_ref
    pub max_win_ref: usize,
    /// lists of WindowFuncs for each winref
    pub window_funcs: Vec<Vec<Box<Node>>>,
}

pub fn contain_agg_clause(clause: Option<&Node>) -> bool {
    unimplemented!()
}

pub fn contain_window_function(clause: Option<&Node>) -> bool {
    unimplemented!()
}

pub fn find_window_functions(clause: Option<&Node>, max_win_ref: usize) -> Box<WindowFuncLists> {
    unimplemented!()
}

pub fn expression_returns_set_rows(root: &mut PlannerInfo, clause: Option<&Node>) -> f64 {
    unimplemented!()
}

pub fn contain_subplans(clause: Option<&Node>) -> bool {
    unimplemented!()
}

/// C returns a `char` parallel-hazard code.
pub fn max_parallel_hazard(parse: &Query) -> u8 {
    unimplemented!()
}

pub fn is_parallel_safe(root: &mut PlannerInfo, node: Option<&Node>) -> bool {
    unimplemented!()
}

pub fn contain_nonstrict_functions(clause: Option<&Node>) -> bool {
    unimplemented!()
}

pub fn contain_exec_param(clause: Option<&Node>, param_ids: &[i32]) -> bool {
    unimplemented!()
}

pub fn contain_leaked_vars(clause: Option<&Node>) -> bool {
    unimplemented!()
}

pub fn find_nonnullable_rels(clause: Option<&Node>) -> Relids {
    unimplemented!()
}

pub fn find_nonnullable_vars(clause: Option<&Node>) -> Vec<Box<Node>> {
    unimplemented!()
}

pub fn find_forced_null_vars(node: Option<&Node>) -> Vec<Box<Node>> {
    unimplemented!()
}

pub fn find_forced_null_var(node: Option<&Node>) -> Option<Box<Var>> {
    unimplemented!()
}

pub fn is_pseudo_constant_clause(clause: Option<&Node>) -> bool {
    unimplemented!()
}

pub fn is_pseudo_constant_clause_relids(clause: Option<&Node>, relids: Relids) -> bool {
    unimplemented!()
}

pub fn num_relids(root: &mut PlannerInfo, clause: Option<&Node>) -> i32 {
    unimplemented!()
}

pub fn commute_op_expr(clause: &mut OpExpr) {
    unimplemented!()
}

pub fn inline_set_returning_function(
    root: &mut PlannerInfo,
    rte: &RangeTblEntry,
) -> Option<Box<Query>> {
    unimplemented!()
}

/// C: `Expr *` (subset of Node) -> `&Node`.
pub fn pull_paramids(expr: &Node) -> Bitmapset {
    unimplemented!()
}
