//! Translated from PostgreSQL src/include/catalog/pg_tablespace.h

use crate::c::{NameData, varlena};
use crate::postgres_ext::Oid;

pub const TableSpaceRelationId: Oid = Oid::new(1213); // BKI_SHARED_RELATION

#[repr(C)]
#[derive(pepperdb_derive::Catalog)]
pub struct FormData_pg_tablespace {
    pub oid: Oid,
    pub spcname: NameData,
    pub spcowner: Oid,
    // CATALOG_VARLEN (not in fixed part)
    pub spcacl: [varlena; 1],     // aclitem[1]
    pub spcoptions: [varlena; 1], // text[1]
}

pub type Form_pg_tablespace = *mut FormData_pg_tablespace; // TODO(ptr)

// DECLARE_TOAST_WITH_MACRO(pg_tablespace, 4185, 4186, ...)
// DECLARE_UNIQUE_INDEX_PKEY(pg_tablespace_oid_index, 2697, TablespaceOidIndexId, ...)
// DECLARE_UNIQUE_INDEX(pg_tablespace_spcname_index, 2698, TablespaceNameIndexId, ...)
// MAKE_SYSCACHE(TABLESPACEOID, ...)
