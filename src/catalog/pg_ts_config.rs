//! Translated from PostgreSQL src/include/catalog/pg_ts_config.h

use crate::c::NameData;
use crate::postgres_ext::Oid;

pub const TSConfigRelationId: Oid = Oid(3602);

#[repr(C)]
pub struct FormData_pg_ts_config {
    pub oid: Oid,
    pub cfgname: NameData,
    pub cfgnamespace: Oid,
    pub cfgowner: Oid,
    pub cfgparser: Oid,
}

pub type Form_pg_ts_config = *mut FormData_pg_ts_config; // TODO(ptr)

// TODO(catalog-derive): replace hand-emitted _d.h consts with #[derive(Catalog)]
pub const Anum_pg_ts_config_oid: i32 = 1;
pub const Anum_pg_ts_config_cfgname: i32 = 2;
pub const Anum_pg_ts_config_cfgnamespace: i32 = 3;
pub const Anum_pg_ts_config_cfgowner: i32 = 4;
pub const Anum_pg_ts_config_cfgparser: i32 = 5;
pub const Natts_pg_ts_config: i32 = 5;

// DECLARE_UNIQUE_INDEX(pg_ts_config_cfgname_index, 3608, TSConfigNameNspIndexId, ...)
// DECLARE_UNIQUE_INDEX_PKEY(pg_ts_config_oid_index, 3712, TSConfigOidIndexId, ...)
// MAKE_SYSCACHE(TSCONFIGNAMENSP, ...); MAKE_SYSCACHE(TSCONFIGOID, ...)
