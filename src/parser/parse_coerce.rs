//! Translated from PostgreSQL src/include/parser/parse_coerce.h

#![allow(clippy::boxed_local, reason = "TODO(stub): drop when implemented; hollow stubs mirror PG signatures 1:1; real impl consumes params")]

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

/// PG `coerce_to_target_type`. See `crate::backend::parser::parse_coerce`.
pub use crate::backend::parser::parse_coerce::coerce_to_target_type;

/// PG `can_coerce_type`. See `crate::backend::parser::parse_coerce`.
pub use crate::backend::parser::parse_coerce::can_coerce_type;

/// PG `coerce_type`. See `crate::backend::parser::parse_coerce`.
pub use crate::backend::parser::parse_coerce::coerce_type;

pub fn coerce_to_domain(
    _arg: Node,
    _base_type_id: Oid,
    _base_type_mod: i32,
    _type_id: Oid,
    _ccontext: CoercionContext,
    _cformat: CoercionForm,
    _location: i32,
    _hide_input_coercion: bool,
) -> Node {
    unimplemented!()
}

/// PG `coerce_to_boolean`. See `crate::backend::parser::parse_coerce`.
pub use crate::backend::parser::parse_coerce::coerce_to_boolean;

pub fn coerce_to_specific_type(
    _pstate: &mut ParseState,
    _node: Node,
    _target_type_id: Oid,
    _construct_name: &str,
) -> Node {
    unimplemented!()
}

pub fn coerce_to_specific_type_typmod(
    _pstate: &mut ParseState,
    _node: Node,
    _target_type_id: Oid,
    _target_typmod: i32,
    _construct_name: &str,
) -> Node {
    unimplemented!()
}

pub fn coerce_null_to_domain(
    _typid: Oid,
    _typmod: i32,
    _collation: Oid,
    _typlen: i32,
    _typbyval: bool,
) -> Node {
    unimplemented!()
}

pub fn parser_coercion_errposition(
    _pstate: &mut ParseState,
    _coerce_location: i32,
    _input_expr: &Node,
) -> i32 {
    unimplemented!()
}

/// PG `select_common_type`. See `crate::backend::parser::parse_coerce`.
pub use crate::backend::parser::parse_coerce::select_common_type;

/// PG `coerce_to_common_type`. See `crate::backend::parser::parse_coerce`.
pub use crate::backend::parser::parse_coerce::coerce_to_common_type;

pub fn verify_common_type(_common_type: Oid, _exprs: &[Node]) -> bool {
    unimplemented!()
}

/// PG `select_common_typmod`. See `crate::backend::parser::parse_coerce`.
pub use crate::backend::parser::parse_coerce::select_common_typmod;

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

/// PG `find_coercion_pathway`. See `crate::backend::parser::parse_coerce`.
pub use crate::backend::parser::parse_coerce::find_coercion_pathway;

/// `funcid` out-param folded into the return.
pub fn find_typmod_coercion_function(_type_id: Oid, _funcid: &mut Oid) -> CoercionPathType {
    unimplemented!()
}
