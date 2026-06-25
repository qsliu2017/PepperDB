//! Translated from PostgreSQL src/include/catalog/pg_statistic.h

use crate::c::{float4, varlena};
use crate::postgres_ext::Oid;

pub const StatisticRelationId: Oid = Oid(2619);

pub const STATISTIC_NUM_SLOTS: i32 = 5;

#[repr(C)]
#[derive(pepperdb_derive::Catalog)]
pub struct FormData_pg_statistic {
    pub starelid: Oid,
    pub staattnum: i16,
    pub stainherit: bool,
    pub stanullfrac: float4,
    pub stawidth: i32,
    pub stadistinct: float4,
    pub stakind1: i16,
    pub stakind2: i16,
    pub stakind3: i16,
    pub stakind4: i16,
    pub stakind5: i16,
    pub staop1: Oid,
    pub staop2: Oid,
    pub staop3: Oid,
    pub staop4: Oid,
    pub staop5: Oid,
    pub stacoll1: Oid,
    pub stacoll2: Oid,
    pub stacoll3: Oid,
    pub stacoll4: Oid,
    pub stacoll5: Oid,
    // CATALOG_VARLEN (not in fixed part)
    pub stanumbers1: [float4; 1],
    pub stanumbers2: [float4; 1],
    pub stanumbers3: [float4; 1],
    pub stanumbers4: [float4; 1],
    pub stanumbers5: [float4; 1],
    pub stavalues1: varlena, // anyarray
    pub stavalues2: varlena, // anyarray
    pub stavalues3: varlena, // anyarray
    pub stavalues4: varlena, // anyarray
    pub stavalues5: varlena, // anyarray
}

pub type Form_pg_statistic = *mut FormData_pg_statistic; // TODO(ptr)

// DECLARE_TOAST(pg_statistic, 2840, 2841)
// DECLARE_UNIQUE_INDEX_PKEY(pg_statistic_relid_att_inh_index, 2696, StatisticRelidAttnumInhIndexId, ...)
// MAKE_SYSCACHE(STATRELATTINH, ...)
// DECLARE_FOREIGN_KEY((starelid, staattnum), pg_attribute, (attrelid, attnum))

// Statistical slot "kind" codes (EXPOSE_TO_CLIENT_CODE).
pub const STATISTIC_KIND_MCV: i32 = 1;
pub const STATISTIC_KIND_HISTOGRAM: i32 = 2;
pub const STATISTIC_KIND_CORRELATION: i32 = 3;
pub const STATISTIC_KIND_MCELEM: i32 = 4;
pub const STATISTIC_KIND_DECHIST: i32 = 5;
pub const STATISTIC_KIND_RANGE_LENGTH_HISTOGRAM: i32 = 6;
pub const STATISTIC_KIND_BOUNDS_HISTOGRAM: i32 = 7;
