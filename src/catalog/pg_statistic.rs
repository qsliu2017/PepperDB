//! Translated from PostgreSQL src/include/catalog/pg_statistic.h

use crate::c::{float4, varlena};
use crate::postgres_ext::Oid;

pub const StatisticRelationId: Oid = Oid(2619);

pub const STATISTIC_NUM_SLOTS: i32 = 5;

#[repr(C)]
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

// TODO(catalog-derive): replace hand-emitted _d.h consts with #[derive(Catalog)]
pub const Anum_pg_statistic_starelid: i32 = 1;
pub const Anum_pg_statistic_staattnum: i32 = 2;
pub const Anum_pg_statistic_stainherit: i32 = 3;
pub const Anum_pg_statistic_stanullfrac: i32 = 4;
pub const Anum_pg_statistic_stawidth: i32 = 5;
pub const Anum_pg_statistic_stadistinct: i32 = 6;
pub const Anum_pg_statistic_stakind1: i32 = 7;
pub const Anum_pg_statistic_stakind2: i32 = 8;
pub const Anum_pg_statistic_stakind3: i32 = 9;
pub const Anum_pg_statistic_stakind4: i32 = 10;
pub const Anum_pg_statistic_stakind5: i32 = 11;
pub const Anum_pg_statistic_staop1: i32 = 12;
pub const Anum_pg_statistic_staop2: i32 = 13;
pub const Anum_pg_statistic_staop3: i32 = 14;
pub const Anum_pg_statistic_staop4: i32 = 15;
pub const Anum_pg_statistic_staop5: i32 = 16;
pub const Anum_pg_statistic_stacoll1: i32 = 17;
pub const Anum_pg_statistic_stacoll2: i32 = 18;
pub const Anum_pg_statistic_stacoll3: i32 = 19;
pub const Anum_pg_statistic_stacoll4: i32 = 20;
pub const Anum_pg_statistic_stacoll5: i32 = 21;
pub const Anum_pg_statistic_stanumbers1: i32 = 22;
pub const Anum_pg_statistic_stanumbers2: i32 = 23;
pub const Anum_pg_statistic_stanumbers3: i32 = 24;
pub const Anum_pg_statistic_stanumbers4: i32 = 25;
pub const Anum_pg_statistic_stanumbers5: i32 = 26;
pub const Anum_pg_statistic_stavalues1: i32 = 27;
pub const Anum_pg_statistic_stavalues2: i32 = 28;
pub const Anum_pg_statistic_stavalues3: i32 = 29;
pub const Anum_pg_statistic_stavalues4: i32 = 30;
pub const Anum_pg_statistic_stavalues5: i32 = 31;
pub const Natts_pg_statistic: i32 = 31;

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
