//! Translated from PostgreSQL src/include/catalog/pg_depend.h

use crate::postgres_ext::Oid;

pub const DependRelationId: Oid = Oid(2608);

#[repr(C)]
#[derive(pepperdb_derive::Catalog)]
pub struct FormData_pg_depend {
    pub classid: Oid, // BKI_LOOKUP(pg_class)
    pub objid: Oid,
    pub objsubid: i32,
    pub refclassid: Oid, // BKI_LOOKUP(pg_class)
    pub refobjid: Oid,
    pub refobjsubid: i32,
    pub deptype: i8, // see DependencyType in dependency.h
}

pub type Form_pg_depend = *mut FormData_pg_depend; // TODO(ptr)

// DECLARE_INDEX(pg_depend_depender_index, 2673, DependDependerIndexId)
// DECLARE_INDEX(pg_depend_reference_index, 2674, DependReferenceIndexId)

