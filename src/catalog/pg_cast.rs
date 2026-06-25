//! Translated from PostgreSQL src/include/catalog/pg_cast.h

use crate::catalog::dependency::DependencyType;
use crate::catalog::objectaddress::ObjectAddress;
use crate::postgres_ext::Oid;

pub const CastRelationId: Oid = Oid(2605);

#[repr(C)]
#[derive(pepperdb_derive::Catalog)]
pub struct FormData_pg_cast {
    pub oid: Oid,
    pub castsource: Oid, // BKI_LOOKUP(pg_type)
    pub casttarget: Oid, // BKI_LOOKUP(pg_type)
    pub castfunc: Oid,   // BKI_LOOKUP_OPT(pg_proc); 0 = binary coercible
    pub castcontext: i8, // see CoercionCodes (COERCION_CODE_*)
    pub castmethod: i8,  // see CoercionMethod (COERCION_METHOD_*)
}

pub type Form_pg_cast = *mut FormData_pg_cast; // TODO(ptr)

// DECLARE_UNIQUE_INDEX_PKEY(pg_cast_oid_index, 2660, CastOidIndexId, ...)
// DECLARE_UNIQUE_INDEX(pg_cast_source_target_index, 2661, CastSourceTargetIndexId, ...)
// MAKE_SYSCACHE(CASTSOURCETARGET, pg_cast_source_target_index, 256)

/// Allowable values for pg_cast.castcontext (stored as char; ASCII codes).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i8)]
pub enum CoercionCodes {
    COERCION_CODE_IMPLICIT = b'i' as i8,   // coercion in context of expression
    COERCION_CODE_ASSIGNMENT = b'a' as i8, // coercion in context of assignment
    COERCION_CODE_EXPLICIT = b'e' as i8,   // explicit cast operation
}

/// Allowable values for pg_cast.castmethod (stored as char; ASCII codes).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i8)]
pub enum CoercionMethod {
    COERCION_METHOD_FUNCTION = b'f' as i8, // use a function
    COERCION_METHOD_BINARY = b'b' as i8,   // types are binary-compatible
    COERCION_METHOD_INOUT = b'i' as i8,    // use input/output functions
}

pub fn CastCreate(
    _sourcetypeid: Oid,
    _targettypeid: Oid,
    _funcid: Oid,
    _incastid: Oid,
    _outcastid: Oid,
    _castcontext: i8,
    _castmethod: i8,
    _behavior: DependencyType,
) -> ObjectAddress {
    unimplemented!()
}
