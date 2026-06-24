//! Translated from PostgreSQL src/include/catalog/pg_extension.h

use crate::c::{text, NameData};
use crate::postgres_ext::Oid;

pub const ExtensionRelationId: Oid = Oid(3079);

#[repr(C)]
pub struct FormData_pg_extension {
    pub oid: Oid,
    pub extname: NameData,
    pub extowner: Oid,     // BKI_LOOKUP(pg_authid)
    pub extnamespace: Oid, // BKI_LOOKUP(pg_namespace)
    pub extrelocatable: bool,
    // CATALOG_VARLEN (not in fixed part):
    pub extversion: text,     // BKI_FORCE_NOT_NULL
    pub extconfig: [Oid; 1],  // BKI_LOOKUP(pg_class)
    pub extcondition: [text; 1],
}

pub type Form_pg_extension = *mut FormData_pg_extension; // TODO(ptr)

// DECLARE_TOAST(pg_extension, 4147, 4148)
// DECLARE_UNIQUE_INDEX_PKEY(pg_extension_oid_index, 3080, ExtensionOidIndexId)
// DECLARE_UNIQUE_INDEX(pg_extension_name_index, 3081, ExtensionNameIndexId)
// MAKE_SYSCACHE(EXTENSIONOID, pg_extension_oid_index, 2)
// MAKE_SYSCACHE(EXTENSIONNAME, pg_extension_name_index, 2)

// TODO(catalog-derive): replace hand-emitted _d.h consts with #[derive(Catalog)]
pub const Anum_pg_extension_oid: i32 = 1;
pub const Anum_pg_extension_extname: i32 = 2;
pub const Anum_pg_extension_extowner: i32 = 3;
pub const Anum_pg_extension_extnamespace: i32 = 4;
pub const Anum_pg_extension_extrelocatable: i32 = 5;
pub const Anum_pg_extension_extversion: i32 = 6;
pub const Anum_pg_extension_extconfig: i32 = 7;
pub const Anum_pg_extension_extcondition: i32 = 8;
pub const Natts_pg_extension: i32 = 8;
