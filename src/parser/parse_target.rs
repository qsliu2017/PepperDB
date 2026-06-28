//! Translated from PostgreSQL src/include/parser/parse_target.h

#![allow(clippy::boxed_local, reason = "TODO(stub): drop when implemented; hollow stubs mirror PG signatures 1:1; real impl consumes params")]

use crate::access::tupdesc::TupleDesc;
use crate::nodes::nodes::Node;
use crate::nodes::primnodes::{CoercionContext, TargetEntry, Var};
use crate::parser::parse_node::{ParseExprKind, ParseState};
use crate::postgres_ext::Oid;

/// PG `transformTargetList`. See `crate::backend::parser::parse_target`.
pub use crate::backend::parser::parse_target::transformTargetList;

pub fn transformExpressionList(
    _pstate: &mut ParseState,
    _exprlist: Vec<Box<Node>>,
    _expr_kind: ParseExprKind,
    _allow_default: bool,
) -> Vec<Box<Node>> {
    unimplemented!()
}

pub fn resolveTargetListUnknowns(_pstate: &mut ParseState, _targetlist: &mut [Box<Node>]) {
    unimplemented!()
}

pub fn markTargetListOrigins(_pstate: &mut ParseState, _targetlist: &mut [Box<Node>]) {
    unimplemented!()
}

/// PG `transformTargetEntry`. See `crate::backend::parser::parse_target`.
/// (`node` is `Node *` in C -- nullable -- so `Option<Box<Node>>` here.)
pub use crate::backend::parser::parse_target::transformTargetEntry;

pub fn transformAssignedExpr(
    _pstate: &mut ParseState,
    _expr: Box<Node>,
    _expr_kind: ParseExprKind,
    _colname: &str,
    _attrno: i32,
    _indirection: Vec<Box<Node>>,
    _location: i32,
) -> Box<Node> {
    unimplemented!()
}

pub fn updateTargetListEntry(
    _pstate: &mut ParseState,
    _tle: &mut TargetEntry,
    _colname: Option<String>,
    _attrno: i32,
    _indirection: Vec<Box<Node>>,
    _location: i32,
) {
    unimplemented!()
}

pub fn transformAssignmentIndirection(
    _pstate: &mut ParseState,
    _basenode: Option<Box<Node>>,
    _target_name: &str,
    _target_is_subscripting: bool,
    _target_type_id: Oid,
    _target_typmod: i32,
    _target_collation: Oid,
    _indirection: Vec<Box<Node>>,
    _indirection_cell: usize,
    _rhs: Box<Node>,
    _ccontext: CoercionContext,
    _location: i32,
) -> Box<Node> {
    unimplemented!()
}

/// `attrnos` out-param folded into the return tuple.
pub fn checkInsertTargets(
    _pstate: &mut ParseState,
    _cols: Vec<Box<Node>>,
) -> (Vec<Box<Node>>, Vec<i32>) {
    unimplemented!()
}

pub fn expandRecordVariable(_pstate: &mut ParseState, _var: &Var, _levelsup: i32) -> TupleDesc {
    unimplemented!()
}

/// PG `FigureColname`. See `crate::backend::parser::parse_target`.
pub use crate::backend::parser::parse_target::FigureColname;

pub fn FigureIndexColname(_node: &Node) -> String {
    unimplemented!()
}
