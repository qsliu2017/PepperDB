//! Translated from PostgreSQL src/include/parser/parse_coerce.h

use crate::nodes::nodes::Node;
use crate::nodes::primnodes::{CoercionContext, CoercionForm};
use crate::parser::parse_node::ParseState;
use crate::postgres_ext::Oid;

/// Type categories (see TYPCATEGORY_xxx symbols in catalog/pg_type.h).
pub type TYPCATEGORY = u8;

/// Result codes for find_coercion_pathway.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CoercionPathType {
    /// failed to find any coercion pathway
    None,
    /// apply the specified coercion function
    Func,
    /// binary-compatible cast, no function
    RelabelType,
    /// need an ArrayCoerceExpr node
    ArrayCoerce,
    /// need a CoerceViaIO node
    CoerceViaIo,
}

pub fn IsBinaryCoercible(_srctype: Oid, _targettype: Oid) -> bool {
    unimplemented!()
}

/// `castoid` out-param folded into the return as `Option<Oid>`.
pub fn IsBinaryCoercibleWithCast(
    _srctype: Oid,
    _targettype: Oid,
    _castoid: &mut Oid,
) -> bool {
    unimplemented!()
}

pub fn IsPreferredType(_category: TYPCATEGORY, _ty: Oid) -> bool {
    unimplemented!()
}

pub fn TypeCategory(_ty: Oid) -> TYPCATEGORY {
    unimplemented!()
}

pub fn coerce_to_target_type(
    _pstate: &mut ParseState,
    _expr: Option<Box<Node>>,
    _exprtype: Oid,
    _targettype: Oid,
    _targettypmod: i32,
    _ccontext: CoercionContext,
    _cformat: CoercionForm,
    _location: i32,
) -> Option<Box<Node>> {
    unimplemented!()
}

pub fn can_coerce_type(
    _nargs: i32,
    _input_typeids: &[Oid],
    _target_typeids: &[Oid],
    _ccontext: CoercionContext,
) -> bool {
    unimplemented!()
}

pub fn coerce_type(
    _pstate: &mut ParseState,
    _node: Box<Node>,
    _input_type_id: Oid,
    _target_type_id: Oid,
    _target_type_mod: i32,
    _ccontext: CoercionContext,
    _cformat: CoercionForm,
    _location: i32,
) -> Box<Node> {
    unimplemented!()
}

pub fn coerce_to_domain(
    _arg: Box<Node>,
    _base_type_id: Oid,
    _base_type_mod: i32,
    _type_id: Oid,
    _ccontext: CoercionContext,
    _cformat: CoercionForm,
    _location: i32,
    _hide_input_coercion: bool,
) -> Box<Node> {
    unimplemented!()
}

pub fn coerce_to_boolean(
    _pstate: &mut ParseState,
    _node: Box<Node>,
    _construct_name: &str,
) -> Box<Node> {
    unimplemented!()
}

pub fn coerce_to_specific_type(
    _pstate: &mut ParseState,
    _node: Box<Node>,
    _target_type_id: Oid,
    _construct_name: &str,
) -> Box<Node> {
    unimplemented!()
}

pub fn coerce_to_specific_type_typmod(
    _pstate: &mut ParseState,
    _node: Box<Node>,
    _target_type_id: Oid,
    _target_typmod: i32,
    _construct_name: &str,
) -> Box<Node> {
    unimplemented!()
}

pub fn coerce_null_to_domain(
    _typid: Oid,
    _typmod: i32,
    _collation: Oid,
    _typlen: i32,
    _typbyval: bool,
) -> Box<Node> {
    unimplemented!()
}

pub fn parser_coercion_errposition(
    _pstate: &mut ParseState,
    _coerce_location: i32,
    _input_expr: &Node,
) -> i32 {
    unimplemented!()
}

/// `which_expr` out-param folded into the return as the second tuple element.
pub fn select_common_type(
    _pstate: &mut ParseState,
    _exprs: &[Box<Node>],
    _context: &str,
) -> (Oid, Option<Box<Node>>) {
    unimplemented!()
}

pub fn coerce_to_common_type(
    _pstate: &mut ParseState,
    _node: Box<Node>,
    _target_type_id: Oid,
    _context: &str,
) -> Box<Node> {
    unimplemented!()
}

pub fn verify_common_type(_common_type: Oid, _exprs: &[Box<Node>]) -> bool {
    unimplemented!()
}

pub fn select_common_typmod(
    _pstate: &mut ParseState,
    _exprs: &[Box<Node>],
    _common_type: Oid,
) -> i32 {
    unimplemented!()
}

pub fn check_generic_type_consistency(
    _actual_arg_types: &[Oid],
    _declared_arg_types: &[Oid],
    _nargs: i32,
) -> bool {
    unimplemented!()
}

pub fn enforce_generic_type_consistency(
    _actual_arg_types: &[Oid],
    _declared_arg_types: &mut [Oid],
    _nargs: i32,
    _rettype: Oid,
    _allow_poly: bool,
) -> Oid {
    unimplemented!()
}

/// Returns an error message on failure, `None` on success.
pub fn check_valid_polymorphic_signature(
    _ret_type: Oid,
    _declared_arg_types: &[Oid],
    _nargs: i32,
) -> Option<String> {
    unimplemented!()
}

/// Returns an error message on failure, `None` on success.
pub fn check_valid_internal_signature(
    _ret_type: Oid,
    _declared_arg_types: &[Oid],
    _nargs: i32,
) -> Option<String> {
    unimplemented!()
}

/// `funcid` out-param folded into the return.
pub fn find_coercion_pathway(
    _target_type_id: Oid,
    _source_type_id: Oid,
    _ccontext: CoercionContext,
    _funcid: &mut Oid,
) -> CoercionPathType {
    unimplemented!()
}

/// `funcid` out-param folded into the return.
pub fn find_typmod_coercion_function(_type_id: Oid, _funcid: &mut Oid) -> CoercionPathType {
    unimplemented!()
}