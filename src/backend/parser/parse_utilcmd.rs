//! Parse analysis for utility commands (CREATE TABLE, ...). Translated from the
//! M2-reachable parts of `src/backend/parser/parse_utilcmd.c`.
//!
//! `transformCreateStmt` turns a raw `CreateStmt` into the analyzed form
//! `ProcessUtility` runs: it walks the table-element list, separating column
//! definitions from constraints, and returns the (possibly rewritten) statement
//! list. Non-type-centric free functions; bodies here as snake_case `pub fn`s,
//! re-exported from `crate::parser::parse_utilcmd` under the C names.
//!
//! Disposition: `grow`. For M2 the live path is a plain `CREATE TABLE name (col
//! type, ...)`: collect the `ColumnDef`s into the output statement. Column/table
//! constraints (NOT NULL / DEFAULT / CHECK / PRIMARY KEY / ...), `LIKE`,
//! inheritance, partitioning, and `OF type` are staged guards (rules.md s4); the
//! column type itself is resolved later in `BuildDescForRelation`
//! (`transformColumnType`'s early validation pass is folded into that single
//! resolution for M2). The before/after auxiliary statement lists (`blist`/`alist`)
//! are empty until those features land.

use crate::access::attmap::AttrMap;
use crate::nodes::nodes::Node;
use crate::nodes::parsenodes::{
    AlterTableStmt, CreateStatsStmt, CreateStmt, IndexStmt, PartitionBoundSpec, RuleStmt,
    TableLikeClause,
};
use crate::nodes::primnodes::RangeVar;
use crate::parser::parse_node::ParseState;
use crate::postgres_ext::Oid;
use crate::utils::rel::RelationData;

/// Panic for a CREATE-element feature not yet translated for this milestone
/// (rules.md s4).
#[cold]
fn not_yet_reachable(what: &str) -> ! {
    unimplemented!("{what}: not yet translated for this milestone");
}

/// PG `transformCreateStmt`: process the raw `CreateStmt` into the analyzed
/// statement list that `ProcessUtility` executes. M2 collects the `ColumnDef`
/// elements (rejecting the not-yet-supported element kinds), leaves the statement
/// otherwise intact, and returns the single rewritten `CreateStmt`.
///
/// PG returns `lappend(cxt.blist, stmt)` then concatenates the index/like/after
/// lists; for M2 those auxiliary lists are empty, so the result is exactly
/// `[CreateStmt]`. The creation-namespace lookup / IF NOT EXISTS short-circuit /
/// schema-qualification fixup are staged (DefineRelation performs the namespace
/// resolution on the M2 path).
pub fn transformCreateStmt(stmt: &mut CreateStmt, _query_string: &str) -> Vec<Node> {
    if stmt.if_not_exists {
        not_yet_reachable("transformCreateStmt: IF NOT EXISTS");
    }
    if stmt.ofTypename.is_some() {
        not_yet_reachable("transformCreateStmt: CREATE TABLE OF type");
    }
    if !stmt.inhRelations.is_empty() {
        not_yet_reachable("transformCreateStmt: table inheritance");
    }
    if stmt.partspec.is_some() || stmt.partbound.is_some() {
        not_yet_reachable("transformCreateStmt: partitioning");
    }
    if !stmt.options.is_empty() {
        not_yet_reachable("transformCreateStmt: WITH storage options");
    }

    // Run through each primary element, separating column defs from constraints.
    // M2 supports only ColumnDef; Constraint and TableLikeClause are staged.
    let mut columns: Vec<Node> = Vec::with_capacity(stmt.tableElts.len());
    for element in &stmt.tableElts {
        match element {
            Node::ColumnDef(_) => columns.push(element.clone()),
            Node::Constraint(_) => not_yet_reachable("transformCreateStmt: table constraint"),
            Node::TableLikeClause(_) => not_yet_reachable("transformCreateStmt: LIKE clause"),
            other => not_yet_reachable(&format!("transformCreateStmt: table element {other:?}")),
        }
    }

    // Output results: the column list replaces tableElts; check/not-null
    // constraint outputs are empty for M2 (no constraints reached above).
    stmt.tableElts = columns;
    crate::assert!(stmt.constraints.is_empty());

    vec![Node::CreateStmt(Box::new(stmt.clone()))]
}

/// `beforeStmts`/`afterStmts` out-params -> returned tuple.
pub fn transformAlterTableStmt(
    _relid: Oid,
    _stmt: &mut AlterTableStmt,
    _query_string: &str,
) -> (Box<AlterTableStmt>, Vec<Node>, Vec<Node>) {
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
) -> (Vec<Node>, Option<Node>) {
    unimplemented!()
}

pub fn transformCreateSchemaStmtElements(
    _schema_elts: Vec<Node>,
    _schema_name: &str,
) -> Vec<Node> {
    unimplemented!()
}

pub fn transformPartitionBound(
    _pstate: &mut ParseState,
    _parent: &RelationData,
    _spec: &PartitionBoundSpec,
) -> Box<PartitionBoundSpec> {
    unimplemented!()
}

pub fn expandTableLikeClause(
    _heap_rel: &RangeVar,
    _table_like_clause: &TableLikeClause,
) -> Vec<Node> {
    unimplemented!()
}

/// `constraintOid` out-param folded into the return tuple.
pub fn generateClonedIndexStmt(
    _heap_rel: &RangeVar,
    _source_idx: &RelationData,
    _attmap: &AttrMap,
) -> (Box<IndexStmt>, Oid) {
    unimplemented!()
}
