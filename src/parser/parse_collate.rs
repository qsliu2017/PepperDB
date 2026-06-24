//! Translated from PostgreSQL src/include/parser/parse_collate.h

use crate::nodes::nodes::Node;
use crate::nodes::parsenodes::Query;
use crate::parser::parse_node::ParseState;
use crate::postgres_ext::Oid;

pub fn assign_query_collations(_pstate: &mut ParseState, _query: &mut Query) {
    unimplemented!()
}

pub fn assign_list_collations(_pstate: &mut ParseState, _exprs: &mut [Box<Node>]) {
    unimplemented!()
}

pub fn assign_expr_collations(_pstate: &mut ParseState, _expr: &mut Node) {
    unimplemented!()
}

pub fn select_common_collation(
    _pstate: &mut ParseState,
    _exprs: &[Box<Node>],
    _none_ok: bool,
) -> Oid {
    unimplemented!()
}
