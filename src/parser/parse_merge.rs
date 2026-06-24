//! Translated from PostgreSQL src/include/parser/parse_merge.h

use crate::nodes::parsenodes::{MergeStmt, Query};
use crate::parser::parse_node::ParseState;

pub fn transformMergeStmt(_pstate: &mut ParseState, _stmt: &mut MergeStmt) -> Box<Query> {
    unimplemented!()
}
