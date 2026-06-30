//! Translated from PostgreSQL src/include/catalog/pg_seclabel.h

use crate::c::text;
use crate::postgres_ext::Oid;

pub const SecLabelRelationId: Oid = Oid::new(3596);

#[repr(C)]
#[derive(pepperdb_derive::Catalog)]
pub struct FormData_pg_seclabel {
    pub objoid: Oid,
    pub classoid: Oid,
    pub objsubid: i32,
    // CATALOG_VARLEN (not in fixed part)
    pub provider: text,
    pub label: text,
}

pub type Form_pg_seclabel = *mut FormData_pg_seclabel; // TODO(ptr)

// DECLARE_TOAST(pg_seclabel, 3598, 3599)
// DECLARE_UNIQUE_INDEX_PKEY(pg_seclabel_object_index, 3597, SecLabelObjectIndexId, ...)
