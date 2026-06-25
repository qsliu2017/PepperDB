//! Translated from PostgreSQL src/include/catalog/pg_database.h

use crate::c::{text, NameData, TransactionId};
use crate::postgres_ext::Oid;

// BKI_SHARED_RELATION BKI_ROWTYPE_OID(1248,DatabaseRelation_Rowtype_Id) BKI_SCHEMA_MACRO
pub const DatabaseRelationId: Oid = Oid(1262);
pub const DatabaseRelation_Rowtype_Id: Oid = Oid(1248);

#[repr(C)]
#[derive(pepperdb_derive::Catalog)]
pub struct FormData_pg_database {
    pub oid: Oid,
    pub datname: NameData,
    pub datdba: Oid, // BKI_LOOKUP(pg_authid)
    pub encoding: i32,
    pub datlocprovider: i8,
    pub datistemplate: bool,
    pub datallowconn: bool,
    pub dathasloginevt: bool,
    pub datconnlimit: i32,
    pub datfrozenxid: TransactionId,
    pub datminmxid: TransactionId,
    pub dattablespace: Oid, // BKI_LOOKUP(pg_tablespace)
    // CATALOG_VARLEN (not in fixed part) -- variable-length fields:
    pub datcollate: text,
    pub datctype: text,
    pub datlocale: text,
    pub daticurules: text,
    pub datcollversion: text,
    pub datacl: [Aclitem; 1], // aclitem[1]; TODO(struct-forward)
}

// aclitem placeholder; real def lives in utils/acl.h.
#[deprecated(note = "TODO(struct-forward): repoint to crate::utils::acl::AclItem in Phase 2")]
#[repr(C)]
pub struct Aclitem {
    pub ai_grantee: Oid,
    pub ai_grantor: Oid,
    pub ai_privs: u64,
}

pub type Form_pg_database = *mut FormData_pg_database; // TODO(ptr)

// DECLARE_TOAST_WITH_MACRO(pg_database, 4177, 4178, PgDatabaseToastTable, PgDatabaseToastIndex)
// DECLARE_UNIQUE_INDEX(pg_database_datname_index, 2671, DatabaseNameIndexId)
// DECLARE_UNIQUE_INDEX_PKEY(pg_database_oid_index, 2672, DatabaseOidIndexId)
// MAKE_SYSCACHE(DATABASEOID, pg_database_oid_index, 4)

// DECLARE_OID_DEFINING_MACRO(Template0DbOid, 4)
pub const Template0DbOid: Oid = Oid(4);
// DECLARE_OID_DEFINING_MACRO(PostgresDbOid, 5)
pub const PostgresDbOid: Oid = Oid(5);

pub const DATCONNLIMIT_UNLIMITED: i32 = -1;
pub const DATCONNLIMIT_INVALID_DB: i32 = -2;

pub fn database_is_invalid_form(_datform: Form_pg_database) -> bool {
    unimplemented!()
}

pub fn database_is_invalid_oid(_dboid: Oid) -> bool {
    unimplemented!()
}
