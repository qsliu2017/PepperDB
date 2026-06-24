//! Translated from PostgreSQL src/include/catalog/pg_seclabel.h

use crate::c::text;
use crate::postgres_ext::Oid;

pub const SecLabelRelationId: Oid = Oid(3596);

#[repr(C)]
pub struct FormData_pg_seclabel {
    pub objoid: Oid,
    pub classoid: Oid,
    pub objsubid: i32,
    // CATALOG_VARLEN (not in fixed part)
    pub provider: text,
    pub label: text,
}

pub type Form_pg_seclabel = *mut FormData_pg_seclabel; // TODO(ptr)

// TODO(catalog-derive): replace hand-emitted _d.h consts with #[derive(Catalog)]
pub const Anum_pg_seclabel_objoid: i32 = 1;
pub const Anum_pg_seclabel_classoid: i32 = 2;
pub const Anum_pg_seclabel_objsubid: i32 = 3;
pub const Anum_pg_seclabel_provider: i32 = 4;
pub const Anum_pg_seclabel_label: i32 = 5;
pub const Natts_pg_seclabel: i32 = 5;

// DECLARE_TOAST(pg_seclabel, 3598, 3599)
// DECLARE_UNIQUE_INDEX_PKEY(pg_seclabel_object_index, 3597, SecLabelObjectIndexId, ...)
