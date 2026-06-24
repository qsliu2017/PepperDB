//! Translated from PostgreSQL src/include/parser/parse_cte.h

use crate::nodes::nodes::Node;
use crate::nodes::parsenodes::{CommonTableExpr, WithClause};
use crate::parser::parse_node::ParseState;

pub fn transformWithClause(
    _pstate: &mut ParseState,
    _with_clause: &WithClause,
) -> Vec<Box<Node>> {
    unimplemented!()
}

pub fn analyzeCTETargetList(
    _pstate: &mut ParseState,
    _cte: &mut CommonTableExpr,
    _tlist: Vec<Box<Node>>,
) {
    unimplemented!()
}
