//! Translated from PostgreSQL src/include/parser/parse_clause.h

use crate::nodes::nodes::{LimitOption, Node};
use crate::nodes::parsenodes::{AclMode, JsonTable, OnConflictClause, SortBy};
use crate::nodes::primnodes::{RangeVar, TargetEntry};
use crate::parser::parse_node::{ParseExprKind, ParseNamespaceItem, ParseState};
use crate::postgres_ext::Oid;

pub fn transformFromClause(_pstate: &mut ParseState, _frm_list: Vec<Box<Node>>) {
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

pub fn transformWhereClause(
    _pstate: &mut ParseState,
    _clause: Option<Box<Node>>,
    _expr_kind: ParseExprKind,
    _construct_name: &str,
) -> Option<Box<Node>> {
    unimplemented!()
}

pub fn transformLimitClause(
    _pstate: &mut ParseState,
    _clause: Option<Box<Node>>,
    _expr_kind: ParseExprKind,
    _construct_name: &str,
    _limit_option: LimitOption,
) -> Option<Box<Node>> {
    unimplemented!()
}

/// `groupingSets`, `targetlist` are in/out params -> threaded as `&mut`.
pub fn transformGroupClause(
    _pstate: &mut ParseState,
    _grouplist: Vec<Box<Node>>,
    _grouping_sets: &mut Vec<Box<Node>>,
    _targetlist: &mut Vec<Box<Node>>,
    _sort_clause: Vec<Box<Node>>,
    _expr_kind: ParseExprKind,
    _use_sql99: bool,
) -> Vec<Box<Node>> {
    unimplemented!()
}

pub fn transformSortClause(
    _pstate: &mut ParseState,
    _orderlist: Vec<Box<Node>>,
    _targetlist: &mut Vec<Box<Node>>,
    _expr_kind: ParseExprKind,
    _use_sql99: bool,
) -> Vec<Box<Node>> {
    unimplemented!()
}

pub fn transformWindowDefinitions(
    _pstate: &mut ParseState,
    _windowdefs: Vec<Box<Node>>,
    _targetlist: &mut Vec<Box<Node>>,
) -> Vec<Box<Node>> {
    unimplemented!()
}

pub fn transformDistinctClause(
    _pstate: &mut ParseState,
    _targetlist: &mut Vec<Box<Node>>,
    _sort_clause: Vec<Box<Node>>,
    _is_agg: bool,
) -> Vec<Box<Node>> {
    unimplemented!()
}

pub fn transformDistinctOnClause(
    _pstate: &mut ParseState,
    _distinctlist: Vec<Box<Node>>,
    _targetlist: &mut Vec<Box<Node>>,
    _sort_clause: Vec<Box<Node>>,
) -> Vec<Box<Node>> {
    unimplemented!()
}

/// `arbiterExpr`, `arbiterWhere`, `constraint` are out-params -> returned tuple.
pub fn transformOnConflictArbiter(
    _pstate: &mut ParseState,
    _on_conflict_clause: &OnConflictClause,
) -> (Vec<Box<Node>>, Option<Box<Node>>, Oid) {
    unimplemented!()
}

pub fn addTargetToSortList(
    _pstate: &mut ParseState,
    _tle: &mut TargetEntry,
    _sortlist: Vec<Box<Node>>,
    _targetlist: Vec<Box<Node>>,
    _sortby: &SortBy,
) -> Vec<Box<Node>> {
    unimplemented!()
}

/// Returns the sort/group ref index.
pub fn assignSortGroupRef(_tle: &mut TargetEntry, _tlist: &[Box<Node>]) -> usize {
    unimplemented!()
}

pub fn targetIsInSortList(_tle: &TargetEntry, _sortop: Oid, _sort_list: &[Box<Node>]) -> bool {
    unimplemented!()
}

/// In parse_jsontable.c
pub fn transformJsonTable(_pstate: &mut ParseState, _jt: &mut JsonTable) -> Box<ParseNamespaceItem> {
    unimplemented!()
}