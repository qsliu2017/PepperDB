//! Translated from PostgreSQL src/include/catalog/pg_user_mapping.h

use crate::c::varlena;
use crate::postgres_ext::Oid;

pub const UserMappingRelationId: Oid = Oid(1418);

#[repr(C)]
#[derive(pepperdb_derive::Catalog)]
pub struct FormData_pg_user_mapping {
    pub oid: Oid,
    pub umuser: Oid,
    pub umserver: Oid,
    // CATALOG_VARLEN (not in fixed part)
    pub umoptions: [varlena; 1], // text[1]
}

pub type Form_pg_user_mapping = *mut FormData_pg_user_mapping; // TODO(ptr)

// DECLARE_TOAST(pg_user_mapping, 4173, 4174)
// DECLARE_UNIQUE_INDEX_PKEY(pg_user_mapping_oid_index, 174, UserMappingOidIndexId, ...)
// DECLARE_UNIQUE_INDEX(pg_user_mapping_user_server_index, 175, UserMappingUserServerIndexId, ...)
// MAKE_SYSCACHE(USERMAPPINGOID, ...); MAKE_SYSCACHE(USERMAPPINGUSERSERVER, ...)
