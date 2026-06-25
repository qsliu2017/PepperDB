//! Translated from PostgreSQL src/include/catalog/pg_collation.h

use crate::c::{text, NameData};
use crate::postgres_ext::Oid;

pub const CollationRelationId: Oid = Oid(3456);

#[repr(C)]
#[derive(pepperdb_derive::Catalog)]
pub struct FormData_pg_collation {
    pub oid: Oid,
    pub collname: NameData,
    pub collnamespace: Oid, // BKI_LOOKUP(pg_namespace)
    pub collowner: Oid,     // BKI_LOOKUP(pg_authid)
    pub collprovider: i8,
    pub collisdeterministic: bool,
    pub collencoding: i32,
    // CATALOG_VARLEN (not in fixed part) -- variable-length fields:
    pub collcollate: text,
    pub collctype: text,
    pub colllocale: text,
    pub collicurules: text,
    pub collversion: text,
}

pub type Form_pg_collation = *mut FormData_pg_collation; // TODO(ptr)

// DECLARE_TOAST(pg_collation, 6175, 6176)
// DECLARE_UNIQUE_INDEX(pg_collation_name_enc_nsp_index, 3164, CollationNameEncNspIndexId)
// DECLARE_UNIQUE_INDEX_PKEY(pg_collation_oid_index, 3085, CollationOidIndexId)
// MAKE_SYSCACHE(COLLNAMEENCNSP, pg_collation_name_enc_nsp_index, 8)
// MAKE_SYSCACHE(COLLOID, pg_collation_oid_index, 8)

pub const COLLPROVIDER_DEFAULT: i8 = b'd' as i8;
pub const COLLPROVIDER_BUILTIN: i8 = b'b' as i8;
pub const COLLPROVIDER_ICU: i8 = b'i' as i8;
pub const COLLPROVIDER_LIBC: i8 = b'c' as i8;

pub fn collprovider_name(c: i8) -> &'static str {
    match c {
        COLLPROVIDER_BUILTIN => "builtin",
        COLLPROVIDER_ICU => "icu",
        COLLPROVIDER_LIBC => "libc",
        _ => "???",
    }
}

pub fn CollationCreate(
    _collname: &str,
    _collnamespace: Oid,
    _collowner: Oid,
    _collprovider: i8,
    _collisdeterministic: bool,
    _collencoding: i32,
    _collcollate: &str,
    _collctype: &str,
    _colllocale: &str,
    _collicurules: &str,
    _collversion: &str,
    _if_not_exists: bool,
    _quiet: bool,
) -> Oid {
    unimplemented!()
}
