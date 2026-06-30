//! Translated from PostgreSQL src/include/catalog/pg_shdepend.h

use crate::postgres_ext::Oid;

pub const SharedDependRelationId: Oid = Oid::new(1214); // BKI_SHARED_RELATION

#[repr(C)]
#[derive(pepperdb_derive::Catalog)]
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

// DECLARE_INDEX(pg_shdepend_depender_index, 1232, SharedDependDependerIndexId, ...)
// DECLARE_INDEX(pg_shdepend_reference_index, 1233, SharedDependReferenceIndexId, ...)
