//! Translated from PostgreSQL src/include/catalog/pg_statistic_ext.h

use crate::c::{NameData, varlena};
use crate::postgres_ext::Oid;

pub const StatisticExtRelationId: Oid = Oid(3381);

#[repr(C)]
pub struct FormData_pg_statistic_ext {
    pub oid: Oid,
    pub stxrelid: Oid,
    pub stxname: NameData,
    pub stxnamespace: Oid,
    pub stxowner: Oid,
    pub stxkeys: varlena, // int2vector (first varlen field, direct-accessible)
    // CATALOG_VARLEN (not in fixed part)
    pub stxstattarget: i16,
    pub stxkind: [i8; 1], // char[1]
    pub stxexprs: varlena, // pg_node_tree
}

pub type Form_pg_statistic_ext = *mut FormData_pg_statistic_ext; // TODO(ptr)

// TODO(catalog-derive): replace hand-emitted _d.h consts with #[derive(Catalog)]
pub const Anum_pg_statistic_ext_oid: i32 = 1;
pub const Anum_pg_statistic_ext_stxrelid: i32 = 2;
pub const Anum_pg_statistic_ext_stxname: i32 = 3;
pub const Anum_pg_statistic_ext_stxnamespace: i32 = 4;
pub const Anum_pg_statistic_ext_stxowner: i32 = 5;
pub const Anum_pg_statistic_ext_stxkeys: i32 = 6;
pub const Anum_pg_statistic_ext_stxstattarget: i32 = 7;
pub const Anum_pg_statistic_ext_stxkind: i32 = 8;
pub const Anum_pg_statistic_ext_stxexprs: i32 = 9;
pub const Natts_pg_statistic_ext: i32 = 9;

// DECLARE_TOAST(pg_statistic_ext, 3439, 3440)
// DECLARE_UNIQUE_INDEX_PKEY(pg_statistic_ext_oid_index, 3380, StatisticExtOidIndexId, ...)
// DECLARE_UNIQUE_INDEX(pg_statistic_ext_name_index, 3997, StatisticExtNameIndexId, ...)
// DECLARE_INDEX(pg_statistic_ext_relid_index, 3379, StatisticExtRelidIndexId, ...)
// MAKE_SYSCACHE(STATEXTOID, ...); MAKE_SYSCACHE(STATEXTNAMENSP, ...)
// DECLARE_ARRAY_FOREIGN_KEY((stxrelid, stxkeys), pg_attribute, (attrelid, attnum))

// Extended statistics kinds (EXPOSE_TO_CLIENT_CODE).
pub const STATS_EXT_NDISTINCT: u8 = b'd';
pub const STATS_EXT_DEPENDENCIES: u8 = b'f';
pub const STATS_EXT_MCV: u8 = b'm';
pub const STATS_EXT_EXPRESSIONS: u8 = b'e';
