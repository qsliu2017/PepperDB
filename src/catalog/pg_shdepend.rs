//! Translated from PostgreSQL src/include/catalog/pg_shdepend.h

use crate::postgres_ext::Oid;

pub const SharedDependRelationId: Oid = Oid(1214); // BKI_SHARED_RELATION

#[repr(C)]
pub struct FormData_pg_shdepend {
    pub dbid: Oid,
    pub classid: Oid,
    pub objid: Oid,
    pub objsubid: i32,
    pub refclassid: Oid,
    pub refobjid: Oid,
    pub deptype: i8, // char; see SharedDependencyType in dependency.h
}

pub type Form_pg_shdepend = *mut FormData_pg_shdepend; // TODO(ptr)

// TODO(catalog-derive): replace hand-emitted _d.h consts with #[derive(Catalog)]
pub const Anum_pg_shdepend_dbid: i32 = 1;
pub const Anum_pg_shdepend_classid: i32 = 2;
pub const Anum_pg_shdepend_objid: i32 = 3;
pub const Anum_pg_shdepend_objsubid: i32 = 4;
pub const Anum_pg_shdepend_refclassid: i32 = 5;
pub const Anum_pg_shdepend_refobjid: i32 = 6;
pub const Anum_pg_shdepend_deptype: i32 = 7;
pub const Natts_pg_shdepend: i32 = 7;

// DECLARE_INDEX(pg_shdepend_depender_index, 1232, SharedDependDependerIndexId, ...)
// DECLARE_INDEX(pg_shdepend_reference_index, 1233, SharedDependReferenceIndexId, ...)
