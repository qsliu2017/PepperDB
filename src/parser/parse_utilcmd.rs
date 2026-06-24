//! Translated from PostgreSQL src/include/parser/parse_utilcmd.h

use crate::access::attmap::AttrMap;
use crate::nodes::nodes::Node;
use crate::nodes::parsenodes::{
    AlterTableStmt, CreateStatsStmt, CreateStmt, IndexStmt, PartitionBoundSpec, RuleStmt,
    TableLikeClause,
};
use crate::nodes::primnodes::RangeVar;
use crate::parser::parse_node::ParseState;
use crate::postgres_ext::Oid;
use crate::utils::relcache::Relation;

pub fn transformCreateStmt(_stmt: &mut CreateStmt, _query_string: &str) -> Vec<Box<Node>> {
    unimplemented!()
}

/// `beforeStmts`/`afterStmts` out-params -> returned tuple.
pub fn transformAlterTableStmt(
    _relid: Oid,
    _stmt: &mut AlterTableStmt,
    _query_string: &str,
) -> (Box<AlterTableStmt>, Vec<Box<Node>>, Vec<Box<Node>>) {
    unimplemented!()
}

pub fn transformIndexStmt(
    _relid: Oid,
    _stmt: &mut IndexStmt,
    _query_string: &str,
) -> Box<IndexStmt> {
    unimplemented!()
}

pub fn transformStatsStmt(
    _relid: Oid,
    _stmt: &mut CreateStatsStmt,
    _query_string: &str,
) -> Box<CreateStatsStmt> {
    unimplemented!()
}

/// `actions`/`whereClause` out-params -> returned tuple.
pub fn transformRuleStmt(
    _stmt: &mut RuleStmt,
    _query_string: &str,
) -> (Vec<Box<Node>>, Option<Box<Node>>) {
    unimplemented!()
}

pub fn transformCreateSchemaStmtElements(
    _schema_elts: Vec<Box<Node>>,
    _schema_name: &str,
) -> Vec<Box<Node>> {
    unimplemented!()
}

pub fn transformPartitionBound(
    _pstate: &mut ParseState,
    _parent: Relation,
    _spec: &PartitionBoundSpec,
) -> Box<PartitionBoundSpec> {
    unimplemented!()
}

pub fn expandTableLikeClause(
    _heap_rel: &RangeVar,
    _table_like_clause: &TableLikeClause,
) -> Vec<Box<Node>> {
    unimplemented!()
}

/// `constraintOid` out-param folded into the return tuple.
pub fn generateClonedIndexStmt(
    _heap_rel: &RangeVar,
    _source_idx: Relation,
    _attmap: &AttrMap,
) -> (Box<IndexStmt>, Oid) {
    unimplemented!()
}
