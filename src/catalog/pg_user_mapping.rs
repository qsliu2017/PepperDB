//! Translated from PostgreSQL src/include/catalog/pg_user_mapping.h

use crate::c::varlena;
use crate::postgres_ext::Oid;

pub const UserMappingRelationId: Oid = Oid(1418);

#[repr(C)]
pub struct FormData_pg_user_mapping {
    pub oid: Oid,
    pub umuser: Oid,
    pub umserver: Oid,
    // CATALOG_VARLEN (not in fixed part)
    pub umoptions: [varlena; 1], // text[1]
}

pub type Form_pg_user_mapping = *mut FormData_pg_user_mapping; // TODO(ptr)

// TODO(catalog-derive): replace hand-emitted _d.h consts with #[derive(Catalog)]
pub const Anum_pg_user_mapping_oid: i32 = 1;
pub const Anum_pg_user_mapping_umuser: i32 = 2;
pub const Anum_pg_user_mapping_umserver: i32 = 3;
pub const Anum_pg_user_mapping_umoptions: i32 = 4;
pub const Natts_pg_user_mapping: i32 = 4;

// DECLARE_TOAST(pg_user_mapping, 4173, 4174)
// DECLARE_UNIQUE_INDEX_PKEY(pg_user_mapping_oid_index, 174, UserMappingOidIndexId, ...)
// DECLARE_UNIQUE_INDEX(pg_user_mapping_user_server_index, 175, UserMappingUserServerIndexId, ...)
// MAKE_SYSCACHE(USERMAPPINGOID, ...); MAKE_SYSCACHE(USERMAPPINGUSERSERVER, ...)
