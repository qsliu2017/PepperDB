//! Translated from PostgreSQL src/include/catalog/pg_cast.h

use crate::postgres_ext::Oid;

pub const CastRelationId: Oid = Oid(2605);

#[repr(C)]
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

// TODO(catalog-derive): replace hand-emitted _d.h consts with #[derive(Catalog)]
pub const Anum_pg_cast_oid: i32 = 1;
pub const Anum_pg_cast_castsource: i32 = 2;
pub const Anum_pg_cast_casttarget: i32 = 3;
pub const Anum_pg_cast_castfunc: i32 = 4;
pub const Anum_pg_cast_castcontext: i32 = 5;
pub const Anum_pg_cast_castmethod: i32 = 6;
pub const Natts_pg_cast: i32 = 6;

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

// Forward refs for the function stub; repointed in Phase 2.
#[deprecated(note = "TODO(struct-forward): repoint to crate::catalog::objectaddress::ObjectAddress in Phase 2")]
pub struct ObjectAddress; // TODO(struct-forward)
#[deprecated(note = "TODO(struct-forward): repoint to crate::catalog::dependency::DependencyType in Phase 2")]
pub struct DependencyType; // TODO(struct-forward)

#[allow(deprecated)]
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
