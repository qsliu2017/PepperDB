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
    // Column-level PRIMARY KEY / UNIQUE constraints split off into IndexStmts run
    // after the CreateStmt (PG transformColumnDefinition -> transformIndexConstraints,
    // reduced to the single-column inline form); [NOT] NULL folds into
    // `is_not_null`. Table constraints and LIKE are staged.
    let mut columns: Vec<Node> = Vec::with_capacity(stmt.tableElts.len());
    let mut index_stmts: Vec<Node> = Vec::new();
    for element in &stmt.tableElts {
        match element {
            Node::ColumnDef(cd) => {
                let mut cd = (**cd).clone();
                transform_column_definition(stmt, &mut cd, &mut index_stmts);
                columns.push(Node::ColumnDef(Box::new(cd)));
            }
            Node::Constraint(_) => not_yet_reachable("transformCreateStmt: table constraint"),
            Node::TableLikeClause(_) => not_yet_reachable("transformCreateStmt: LIKE clause"),
            other => not_yet_reachable(&format!("transformCreateStmt: table element {other:?}")),
        }
    }

    // Output results: the column list replaces tableElts; the split-off
    // PRIMARY KEY / UNIQUE index statements follow the CreateStmt (PG's alist).
    stmt.tableElts = columns;
    crate::assert!(stmt.constraints.is_empty());

    let mut result = vec![Node::CreateStmt(Box::new(stmt.clone()))];
    result.extend(index_stmts);
    result
}

/// PG `transformColumnDefinition` (inline-constraint subset): fold the column's
/// raw constraints into the ColumnDef / the deferred index list. PRIMARY KEY
/// implies NOT NULL and a `<table>_pkey` unique index; UNIQUE builds a
/// `<table>_<col>_key` unique index (the ChooseIndexName defaults). The
/// column-level FOREIGN KEY passes through to DefineRelation's existing path.
/// SERIAL / DEFAULT / CHECK / IDENTITY / GENERATED are staged.
fn transform_column_definition(
    stmt: &CreateStmt,
    column: &mut crate::nodes::parsenodes::ColumnDef,
    index_stmts: &mut Vec<Node>,
) {
    use crate::backend::parser::parser::{make_index_elem, make_index_stmt};
    use crate::nodes::parsenodes::ConstrType;

    if column.constraints.is_empty() {
        return;
    }
    let colname = column.colname.clone().unwrap_or_default();
    let relname = stmt
        .relation
        .as_ref()
        .and_then(|r| r.relname.clone())
        .unwrap_or_default();

    let constraints = std::mem::take(&mut column.constraints);
    for c_node in constraints {
        let Node::Constraint(c) = &c_node else {
            not_yet_reachable("transformColumnDefinition: non-Constraint column qualifier");
        };
        match c.contype {
            ConstrType::PRIMARY | ConstrType::UNIQUE => {
                let is_pkey = c.contype == ConstrType::PRIMARY;
                if is_pkey {
                    column.is_not_null = true;
                }
                let idxname = if is_pkey {
                    format!("{relname}_pkey")
                } else {
                    format!("{relname}_{colname}_key")
                };
                let rel = stmt
                    .relation
                    .as_deref()
                    .unwrap_or_else(|| {
                        not_yet_reachable("transformColumnDefinition: CreateStmt without relation")
                    })
                    .clone();
                let mut istmt = make_index_stmt(
                    true,
                    Some(idxname),
                    rel,
                    None,
                    vec![make_index_elem(
                        colname.clone(),
                        crate::nodes::parsenodes::SortByDir::DEFAULT,
                    )],
                );
                if let Node::IndexStmt(is) = &mut istmt {
                    is.primary = is_pkey;
                    is.isconstraint = true;
                }
                index_stmts.push(istmt);
            }
            ConstrType::NOTNULL => column.is_not_null = true,
            ConstrType::NULL => column.is_not_null = false,
            // The column-level FOREIGN KEY keeps its existing DefineRelation path.
            ConstrType::FOREIGN => column.constraints.push(c_node.clone()),
            other => not_yet_reachable(&format!("transformColumnDefinition: constraint {other:?}")),
        }
    }
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
