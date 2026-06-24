//! Translated from PostgreSQL src/include/catalog/pg_ts_config_map.h

use crate::postgres_ext::Oid;

pub const TSConfigMapRelationId: Oid = Oid(3603);

#[repr(C)]
pub struct FormData_pg_ts_config_map {
    pub mapcfg: Oid,
    pub maptokentype: i32,
    pub mapseqno: i32,
    pub mapdict: Oid,
}

pub type Form_pg_ts_config_map = *mut FormData_pg_ts_config_map; // TODO(ptr)

// TODO(catalog-derive): replace hand-emitted _d.h consts with #[derive(Catalog)]
pub const Anum_pg_ts_config_map_mapcfg: i32 = 1;
pub const Anum_pg_ts_config_map_maptokentype: i32 = 2;
pub const Anum_pg_ts_config_map_mapseqno: i32 = 3;
pub const Anum_pg_ts_config_map_mapdict: i32 = 4;
pub const Natts_pg_ts_config_map: i32 = 4;

// DECLARE_UNIQUE_INDEX_PKEY(pg_ts_config_map_index, 3609, TSConfigMapIndexId, ...)
// MAKE_SYSCACHE(TSCONFIGMAP, ...)
