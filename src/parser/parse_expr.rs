//! Translated from PostgreSQL src/include/parser/parse_expr.h

use crate::nodes::nodes::Node;
use crate::parser::parse_node::{ParseExprKind, ParseState};

/// GUC parameter.
pub static mut Transform_null_equals: bool = false;

pub fn transformExpr(
    _pstate: &mut ParseState,
    _expr: Option<Box<Node>>,
    _expr_kind: ParseExprKind,
) -> Option<Box<Node>> {
    unimplemented!()
}

pub fn ParseExprKindName(_expr_kind: ParseExprKind) -> &'static str {
    unimplemented!()
}
