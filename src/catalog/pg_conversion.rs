//! Translated from PostgreSQL src/include/catalog/pg_conversion.h

use crate::c::{regproc, NameData};
use crate::catalog::objectaddress::ObjectAddress;
use crate::postgres_ext::Oid;

pub const ConversionRelationId: Oid = Oid::new(2607);

#[repr(C)]
#[derive(pepperdb_derive::Catalog)]
pub struct FormData_pg_conversion {
    pub oid: Oid,
    pub conname: NameData,
    pub connamespace: Oid, // BKI_LOOKUP(pg_namespace)
    pub conowner: Oid,     // BKI_LOOKUP(pg_authid)
    pub conforencoding: i32, // BKI_LOOKUP(encoding)
    pub contoencoding: i32,  // BKI_LOOKUP(encoding)
    pub conproc: regproc,    // BKI_LOOKUP(pg_proc)
    pub condefault: bool,
}

pub type Form_pg_conversion = *mut FormData_pg_conversion; // TODO(ptr)

// DECLARE_UNIQUE_INDEX(pg_conversion_default_index, 2668, ConversionDefaultIndexId, ...)
// DECLARE_UNIQUE_INDEX(pg_conversion_name_nsp_index, 2669, ConversionNameNspIndexId, ...)
// DECLARE_UNIQUE_INDEX_PKEY(pg_conversion_oid_index, 2670, ConversionOidIndexId, ...)
// MAKE_SYSCACHE(CONDEFAULT, pg_conversion_default_index, 8)
// MAKE_SYSCACHE(CONNAMENSP, pg_conversion_name_nsp_index, 8)
// MAKE_SYSCACHE(CONVOID, pg_conversion_oid_index, 8)

pub fn ConversionCreate(
    _conname: &str,
    _connamespace: Oid,
    _conowner: Oid,
    _conforencoding: i32,
    _contoencoding: i32,
    _conproc: Oid,
    _def: bool,
) -> ObjectAddress {
    unimplemented!()
}

// InvalidOid sentinel -> Option
pub fn FindDefaultConversion(_name_space: Oid, _for_encoding: i32, _to_encoding: i32) -> Option<Oid> {
    unimplemented!()
}
