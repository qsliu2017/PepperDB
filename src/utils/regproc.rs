//! Translated from PostgreSQL src/include/utils/regproc.h

use bitflags::bitflags;

use crate::nodes::nodes::Node;
use crate::postgres_ext::Oid;

bitflags! {
    /// Control flags for format_procedure_extended.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct FormatProc: u16 {
        const INVALID_AS_NULL = 0x01; // NULL if undefined
        const FORCE_QUALIFY   = 0x02; // force qualification
    }
}

bitflags! {
    /// Control flags for format_operator_extended.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct FormatOperator: u16 {
        const INVALID_AS_NULL = 0x01; // NULL if undefined
        const FORCE_QUALIFY   = 0x02; // force qualification
    }
}

pub fn format_procedure_extended(_procedure_oid: Oid, _flags: FormatProc) -> String {
    unimplemented!()
}

pub fn format_operator_extended(_operator_oid: Oid, _flags: FormatOperator) -> String {
    unimplemented!()
}

// List* -> Vec; escontext is a soft-error node. TODO(ptr) on Node ownership.
pub fn stringToQualifiedNameList(_string: &str, _escontext: Option<&mut Node>) -> Vec<String> {
    unimplemented!()
}

pub fn format_procedure(_procedure_oid: Oid) -> String {
    unimplemented!()
}

pub fn format_procedure_qualified(_procedure_oid: Oid) -> String {
    unimplemented!()
}

// Out-params objnames/objargs -> returned tuple of Vecs.
pub fn format_procedure_parts(
    _procedure_oid: Oid,
    _missing_ok: bool,
) -> (Vec<String>, Vec<String>) {
    unimplemented!()
}

pub fn format_operator(_operator_oid: Oid) -> String {
    unimplemented!()
}

pub fn format_operator_qualified(_operator_oid: Oid) -> String {
    unimplemented!()
}

pub fn format_operator_parts(
    _operator_oid: Oid,
    _missing_ok: bool,
) -> (Vec<String>, Vec<String>) {
    unimplemented!()
}
