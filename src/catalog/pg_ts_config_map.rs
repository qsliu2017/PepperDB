//! Translated from PostgreSQL src/include/catalog/pg_ts_config_map.h

use crate::postgres_ext::Oid;

pub const TSConfigMapRelationId: Oid = Oid::new(3603);

#[repr(C)]
#[derive(pepperdb_derive::Catalog)]
pub struct FormData_pg_ts_config_map {
    pub mapcfg: Oid,
    pub maptokentype: i32,
    pub mapseqno: i32,
    pub mapdict: Oid,
}

pub type Form_pg_ts_config_map = *mut FormData_pg_ts_config_map; // TODO(ptr)

// DECLARE_UNIQUE_INDEX_PKEY(pg_ts_config_map_index, 3609, TSConfigMapIndexId, ...)
// MAKE_SYSCACHE(TSCONFIGMAP, ...)
