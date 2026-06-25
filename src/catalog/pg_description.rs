//! Translated from PostgreSQL src/include/catalog/pg_description.h

use crate::c::text;
use crate::postgres_ext::Oid;

pub const DescriptionRelationId: Oid = Oid(2609);

#[repr(C)]
#[derive(pepperdb_derive::Catalog)]
pub struct FormData_pg_description {
    pub objoid: Oid,
    pub classoid: Oid,
    pub objsubid: i32,
    // CATALOG_VARLEN (not in fixed part):
    pub description: text, // BKI_FORCE_NOT_NULL
}

pub type Form_pg_description = *mut FormData_pg_description; // TODO(ptr)

// DECLARE_TOAST(pg_description, 2834, 2835)
// DECLARE_UNIQUE_INDEX_PKEY(pg_description_o_c_o_index, 2675, DescriptionObjIndexId)
// DECLARE_FOREIGN_KEY((classoid), pg_class, (oid))

