//! Translated from PostgreSQL src/include/catalog/pg_depend.h

use crate::postgres_ext::Oid;

pub const DependRelationId: Oid = Oid(2608);

#[repr(C)]
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

// TODO(catalog-derive): replace hand-emitted _d.h consts with #[derive(Catalog)]
pub const Anum_pg_depend_classid: i32 = 1;
pub const Anum_pg_depend_objid: i32 = 2;
pub const Anum_pg_depend_objsubid: i32 = 3;
pub const Anum_pg_depend_refclassid: i32 = 4;
pub const Anum_pg_depend_refobjid: i32 = 5;
pub const Anum_pg_depend_refobjsubid: i32 = 6;
pub const Anum_pg_depend_deptype: i32 = 7;
pub const Natts_pg_depend: i32 = 7;
