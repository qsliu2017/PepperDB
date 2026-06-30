//! Translated from PostgreSQL src/include/catalog/pg_shdescription.h

use crate::c::text;
use crate::postgres_ext::Oid;

pub const SharedDescriptionRelationId: Oid = Oid::new(2396); // BKI_SHARED_RELATION

#[repr(C)]
#[derive(pepperdb_derive::Catalog)]
pub struct FormData_pg_shdescription {
    pub objoid: Oid,
    pub classoid: Oid,
    // CATALOG_VARLEN (not in fixed part)
    pub description: text,
}

pub type Form_pg_shdescription = *mut FormData_pg_shdescription; // TODO(ptr)

// DECLARE_TOAST_WITH_MACRO(pg_shdescription, 2846, 2847, ...)
// DECLARE_UNIQUE_INDEX_PKEY(pg_shdescription_o_c_index, 2397, SharedDescriptionObjIndexId, ...)
// DECLARE_FOREIGN_KEY((classoid), pg_class, (oid))
