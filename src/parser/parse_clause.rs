//! Translated from PostgreSQL src/include/parser/parse_clause.h

use crate::nodes::nodes::{LimitOption, Node};
use crate::nodes::parsenodes::{AclMode, JsonTable, OnConflictClause, SortBy};
use crate::nodes::primnodes::{RangeVar, TargetEntry};
use crate::parser::parse_node::{ParseExprKind, ParseNamespaceItem, ParseState};
use crate::postgres_ext::Oid;

pub fn transformFromClause(_pstate: &mut ParseState, _frm_list: Vec<Node>) {
    unimplemented!()
}

/// Returns the RT index assigned to the target table.
pub fn setTargetTable(
    _pstate: &mut ParseState,
    _relation: &RangeVar,
    _inh: bool,
    _also_source: bool,
    _required_perms: AclMode,
) -> i32 {
    unimplemented!()
}

/// PG `transformWhereClause`. See `crate::backend::parser::parse_clause`.
pub use crate::backend::parser::parse_clause::transform_where_clause as transformWhereClause;

/// PG `transformLimitClause`. See `crate::backend::parser::parse_clause` (M5 body):
/// transforms + coerces the LIMIT/OFFSET expression to int8.
pub use crate::backend::parser::parse_clause::transform_limit_clause as transformLimitClause;

/// PG `transformGroupClause`. See `crate::backend::parser::parse_clause` (M5 body):
/// builds the GROUP BY SortGroupClause list (in/out `grouping_sets`/`targetlist`).
pub use crate::backend::parser::parse_clause::transform_group_clause as transformGroupClause;

/// PG `transformSortClause`. See `crate::backend::parser::parse_clause` (M5 body):
/// builds the ORDER BY SortGroupClause list.
pub use crate::backend::parser::parse_clause::transform_sort_clause as transformSortClause;

pub fn transformWindowDefinitions(
    _pstate: &mut ParseState,
    _windowdefs: Vec<Node>,
    _targetlist: &mut Vec<Node>,
) -> Vec<Node> {
    unimplemented!()
}

/// PG `transformDistinctClause`. See `crate::backend::parser::parse_clause` (M5
/// body): builds the DISTINCT SortGroupClause list over the select-list columns.
pub use crate::backend::parser::parse_clause::transform_distinct_clause as transformDistinctClause;

pub fn transformDistinctOnClause(
    _pstate: &mut ParseState,
    _distinctlist: Vec<Node>,
    _targetlist: &mut Vec<Node>,
    _sort_clause: Vec<Node>,
) -> Vec<Node> {
    unimplemented!()
}

/// `arbiterExpr`, `arbiterWhere`, `constraint` are out-params -> returned tuple.
pub fn transformOnConflictArbiter(
    _pstate: &mut ParseState,
    _on_conflict_clause: &OnConflictClause,
) -> (Vec<Node>, Option<Node>, Oid) {
    unimplemented!()
}

pub fn addTargetToSortList(
    _pstate: &mut ParseState,
    _tle: &mut TargetEntry,
    _sortlist: Vec<Node>,
    _targetlist: Vec<Node>,
    _sortby: &SortBy,
) -> Vec<Node> {
    unimplemented!()
}

/// Returns the sort/group ref index.
pub fn assignSortGroupRef(_tle: &mut TargetEntry, _tlist: &[Node]) -> usize {
    unimplemented!()
}

pub fn targetIsInSortList(_tle: &TargetEntry, _sortop: Oid, _sort_list: &[Node]) -> bool {
    unimplemented!()
}

/// In parse_jsontable.c
pub fn transformJsonTable(_pstate: &mut ParseState, _jt: &mut JsonTable) -> Box<ParseNamespaceItem> {
    unimplemented!()
}
