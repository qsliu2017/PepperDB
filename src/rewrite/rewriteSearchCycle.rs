//! Translated from PostgreSQL src/include/rewrite/rewriteSearchCycle.h
//! Support for rewriting SEARCH and CYCLE clauses.

use crate::nodes::parsenodes::CommonTableExpr;

pub fn rewrite_search_and_cycle(_cte: &CommonTableExpr) -> Box<CommonTableExpr> {
    unimplemented!()
}
