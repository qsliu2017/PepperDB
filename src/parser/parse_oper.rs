//! Translated from PostgreSQL src/include/parser/parse_oper.h

use crate::access::htup::HeapTuple;
use crate::nodes::nodes::Node;
use crate::nodes::parsenodes::ObjectWithArgs;
use crate::parser::parse_node::ParseState;
use crate::postgres_ext::Oid;

pub type Operator = HeapTuple;

/// Outputs of `get_sort_group_operators`.
pub struct SortGroupOperators {
    pub lt_opr: Oid,
    pub eq_opr: Oid,
    pub gt_opr: Oid,
    pub is_hashable: bool,
}

/// `InvalidOid` sentinel (when `noError`) -> `Option`.
pub fn LookupOperName(
    _pstate: &mut ParseState,
    _opername: Vec<Node>,
    _oprleft: Oid,
    _oprright: Oid,
    _no_error: bool,
    _location: i32,
) -> Option<Oid> {
    unimplemented!()
}

/// `InvalidOid` sentinel (when `noError`) -> `Option`.
pub fn LookupOperWithArgs(_oper: &ObjectWithArgs, _no_error: bool) -> Option<Oid> {
    unimplemented!()
}

/// Invalid-tuple sentinel (when `noError`) -> `Option`.
pub fn oper(
    _pstate: &mut ParseState,
    _opname: Vec<Node>,
    _ltype_id: Oid,
    _rtype_id: Oid,
    _no_error: bool,
    _location: i32,
) -> Option<Operator> {
    unimplemented!()
}

/// Invalid-tuple sentinel (when `noError`) -> `Option`.
pub fn left_oper(
    _pstate: &mut ParseState,
    _op: Vec<Node>,
    _arg: Oid,
    _no_error: bool,
    _location: i32,
) -> Option<Operator> {
    unimplemented!()
}

/// Invalid-tuple sentinel (when `noError`) -> `Option`.
pub fn compatible_oper(
    _pstate: &mut ParseState,
    _op: Vec<Node>,
    _arg1: Oid,
    _arg2: Oid,
    _no_error: bool,
    _location: i32,
) -> Option<Operator> {
    unimplemented!()
}

pub fn op_signature_string(_op: Vec<Node>, _arg1: Oid, _arg2: Oid) -> String {
    unimplemented!()
}

pub fn get_sort_group_operators(
    _argtype: Oid,
    _need_lt: bool,
    _need_eq: bool,
    _need_gt: bool,
) -> SortGroupOperators {
    unimplemented!()
}

/// `InvalidOid` sentinel (when `noError`) -> `Option`.
pub fn compatible_oper_opid(
    _op: Vec<Node>,
    _arg1: Oid,
    _arg2: Oid,
    _no_error: bool,
) -> Option<Oid> {
    unimplemented!()
}

/// Extract operator OID from an Operator tuple.
pub fn oprid(_op: Operator) -> Oid {
    unimplemented!()
}

/// Extract underlying-function OID from an Operator tuple.
pub fn oprfuncid(_op: Operator) -> Oid {
    unimplemented!()
}

pub fn make_op(
    _pstate: &mut ParseState,
    _opname: Vec<Node>,
    _ltree: Option<Node>,
    _rtree: Option<Node>,
    _last_srf: Option<Node>,
    _location: i32,
) -> Node {
    unimplemented!()
}

pub fn make_scalar_array_op(
    _pstate: &mut ParseState,
    _opname: Vec<Node>,
    _use_or: bool,
    _ltree: Option<Node>,
    _rtree: Option<Node>,
    _location: i32,
) -> Node {
    unimplemented!()
}
