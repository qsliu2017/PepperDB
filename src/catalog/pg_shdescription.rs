//! Translated from PostgreSQL src/include/catalog/pg_shdescription.h

use crate::c::text;
use crate::postgres_ext::Oid;

pub const SharedDescriptionRelationId: Oid = Oid(2396); // BKI_SHARED_RELATION

#[repr(C)]
pub struct FormData_pg_shdescription {
    pub objoid: Oid,
    pub classoid: Oid,
    // CATALOG_VARLEN (not in fixed part)
    pub description: text,
}

pub type Form_pg_shdescription = *mut FormData_pg_shdescription; // TODO(ptr)

// TODO(catalog-derive): replace hand-emitted _d.h consts with #[derive(Catalog)]
pub const Anum_pg_shdescription_objoid: i32 = 1;
pub const Anum_pg_shdescription_classoid: i32 = 2;
pub const Anum_pg_shdescription_description: i32 = 3;
pub const Natts_pg_shdescription: i32 = 3;

// DECLARE_TOAST_WITH_MACRO(pg_shdescription, 2846, 2847, ...)
// DECLARE_UNIQUE_INDEX_PKEY(pg_shdescription_o_c_index, 2397, SharedDescriptionObjIndexId, ...)
// DECLARE_FOREIGN_KEY((classoid), pg_class, (oid))
