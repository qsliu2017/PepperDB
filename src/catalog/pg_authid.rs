//! Translated from PostgreSQL src/include/catalog/pg_authid.h

use crate::c::{text, NameData};
use crate::datatype::timestamp::TimestampTz;
use crate::postgres_ext::Oid;

// BKI_SHARED_RELATION BKI_ROWTYPE_OID(2842,AuthIdRelation_Rowtype_Id) BKI_SCHEMA_MACRO
pub const AuthIdRelationId: Oid = Oid(1260);
pub const AuthIdRelation_Rowtype_Id: Oid = Oid(2842);

#[repr(C)]
#[derive(pepperdb_derive::Catalog)]
pub struct FormData_pg_authid {
    pub oid: Oid,
    pub rolname: NameData,
    pub rolsuper: bool,
    pub rolinherit: bool,
    pub rolcreaterole: bool,
    pub rolcreatedb: bool,
    pub rolcanlogin: bool,
    pub rolreplication: bool,
    pub rolbypassrls: bool,
    pub rolconnlimit: i32,
    // CATALOG_VARLEN (not in fixed part) -- nullable variable-length fields:
    pub rolpassword: text,
    pub rolvaliduntil: TimestampTz,
}

pub type Form_pg_authid = *mut FormData_pg_authid; // TODO(ptr)

// DECLARE_UNIQUE_INDEX(pg_authid_rolname_index, 2676, AuthIdRolnameIndexId)
// DECLARE_UNIQUE_INDEX_PKEY(pg_authid_oid_index, 2677, AuthIdOidIndexId)
// MAKE_SYSCACHE(AUTHNAME, pg_authid_rolname_index, 8)
// MAKE_SYSCACHE(AUTHOID, pg_authid_oid_index, 8)

