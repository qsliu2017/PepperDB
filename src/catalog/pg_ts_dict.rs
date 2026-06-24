//! Translated from PostgreSQL src/include/catalog/pg_ts_dict.h

use crate::c::{NameData, text};
use crate::postgres_ext::Oid;

pub const TSDictionaryRelationId: Oid = Oid(3600);

#[repr(C)]
pub struct FormData_pg_ts_dict {
    pub oid: Oid,
    pub dictname: NameData,
    pub dictnamespace: Oid,
    pub dictowner: Oid,
    pub dicttemplate: Oid,
    // CATALOG_VARLEN (not in fixed part)
    pub dictinitoption: text,
}

pub type Form_pg_ts_dict = *mut FormData_pg_ts_dict; // TODO(ptr)

// TODO(catalog-derive): replace hand-emitted _d.h consts with #[derive(Catalog)]
pub const Anum_pg_ts_dict_oid: i32 = 1;
pub const Anum_pg_ts_dict_dictname: i32 = 2;
pub const Anum_pg_ts_dict_dictnamespace: i32 = 3;
pub const Anum_pg_ts_dict_dictowner: i32 = 4;
pub const Anum_pg_ts_dict_dicttemplate: i32 = 5;
pub const Anum_pg_ts_dict_dictinitoption: i32 = 6;
pub const Natts_pg_ts_dict: i32 = 6;

// DECLARE_TOAST(pg_ts_dict, 4169, 4170)
// DECLARE_UNIQUE_INDEX(pg_ts_dict_dictname_index, 3604, TSDictionaryNameNspIndexId, ...)
// DECLARE_UNIQUE_INDEX_PKEY(pg_ts_dict_oid_index, 3605, TSDictionaryOidIndexId, ...)
// MAKE_SYSCACHE(TSDICTNAMENSP, ...); MAKE_SYSCACHE(TSDICTOID, ...)
