//! Translated from PostgreSQL src/include/catalog/pg_statistic_ext_data.h

use crate::c::varlena;
use crate::postgres_ext::Oid;

pub const StatisticExtDataRelationId: Oid = Oid(3429);

#[repr(C)]
pub struct FormData_pg_statistic_ext_data {
    pub stxoid: Oid,
    pub stxdinherit: bool,
    // CATALOG_VARLEN (not in fixed part)
    pub stxdndistinct: varlena,    // pg_ndistinct (serialized)
    pub stxddependencies: varlena, // pg_dependencies (serialized)
    pub stxdmcv: varlena,          // pg_mcv_list (serialized)
    pub stxdexpr: varlena,         // pg_statistic[1] (stats for expressions)
}

pub type Form_pg_statistic_ext_data = *mut FormData_pg_statistic_ext_data; // TODO(ptr)

// TODO(catalog-derive): replace hand-emitted _d.h consts with #[derive(Catalog)]
pub const Anum_pg_statistic_ext_data_stxoid: i32 = 1;
pub const Anum_pg_statistic_ext_data_stxdinherit: i32 = 2;
pub const Anum_pg_statistic_ext_data_stxdndistinct: i32 = 3;
pub const Anum_pg_statistic_ext_data_stxddependencies: i32 = 4;
pub const Anum_pg_statistic_ext_data_stxdmcv: i32 = 5;
pub const Anum_pg_statistic_ext_data_stxdexpr: i32 = 6;
pub const Natts_pg_statistic_ext_data: i32 = 6;

// DECLARE_TOAST(pg_statistic_ext_data, 3430, 3431)
// DECLARE_UNIQUE_INDEX_PKEY(pg_statistic_ext_data_stxoid_inh_index, 3433, StatisticExtDataStxoidInhIndexId, ...)
// MAKE_SYSCACHE(STATEXTDATASTXOID, ...)
