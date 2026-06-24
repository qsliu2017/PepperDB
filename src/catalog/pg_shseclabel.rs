//! Translated from PostgreSQL src/include/catalog/pg_shseclabel.h

use crate::c::text;
use crate::postgres_ext::Oid;

pub const SharedSecLabelRelationId: Oid = Oid(3592); // BKI_SHARED_RELATION
pub const SharedSecLabelRelation_Rowtype_Id: Oid = Oid(4066); // BKI_ROWTYPE_OID

#[repr(C)]
pub struct FormData_pg_shseclabel {
    pub objoid: Oid,
    pub classoid: Oid,
    // CATALOG_VARLEN (not in fixed part)
    pub provider: text,
    pub label: text,
}

pub type Form_pg_shseclabel = *mut FormData_pg_shseclabel; // TODO(ptr)

// TODO(catalog-derive): replace hand-emitted _d.h consts with #[derive(Catalog)]
pub const Anum_pg_shseclabel_objoid: i32 = 1;
pub const Anum_pg_shseclabel_classoid: i32 = 2;
pub const Anum_pg_shseclabel_provider: i32 = 3;
pub const Anum_pg_shseclabel_label: i32 = 4;
pub const Natts_pg_shseclabel: i32 = 4;

// DECLARE_TOAST_WITH_MACRO(pg_shseclabel, 4060, 4061, ...)
// DECLARE_UNIQUE_INDEX_PKEY(pg_shseclabel_object_index, 3593, SharedSecLabelObjectIndexId, ...)
