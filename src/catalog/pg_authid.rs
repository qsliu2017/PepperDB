//! Translated from PostgreSQL src/include/catalog/pg_authid.h

use crate::c::{text, NameData};
use crate::postgres_ext::Oid;

// BKI_SHARED_RELATION BKI_ROWTYPE_OID(2842,AuthIdRelation_Rowtype_Id) BKI_SCHEMA_MACRO
pub const AuthIdRelationId: Oid = Oid(1260);
pub const AuthIdRelation_Rowtype_Id: Oid = Oid(2842);

// timestamptz catalog field = TimestampTz (i64 usec since 2000-01-01); real def in datatype/timestamp.
pub type TimestampTz = i64; // TODO(struct-forward): repoint to crate::datatype::timestamp::TimestampTz

#[repr(C)]
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

// TODO(catalog-derive): replace hand-emitted _d.h consts with #[derive(Catalog)]
pub const Anum_pg_authid_oid: i32 = 1;
pub const Anum_pg_authid_rolname: i32 = 2;
pub const Anum_pg_authid_rolsuper: i32 = 3;
pub const Anum_pg_authid_rolinherit: i32 = 4;
pub const Anum_pg_authid_rolcreaterole: i32 = 5;
pub const Anum_pg_authid_rolcreatedb: i32 = 6;
pub const Anum_pg_authid_rolcanlogin: i32 = 7;
pub const Anum_pg_authid_rolreplication: i32 = 8;
pub const Anum_pg_authid_rolbypassrls: i32 = 9;
pub const Anum_pg_authid_rolconnlimit: i32 = 10;
pub const Anum_pg_authid_rolpassword: i32 = 11;
pub const Anum_pg_authid_rolvaliduntil: i32 = 12;
pub const Natts_pg_authid: i32 = 12;
