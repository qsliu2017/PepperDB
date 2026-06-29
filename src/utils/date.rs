//! Translated from PostgreSQL src/include/utils/date.h
//!
//! On-disk types: DateADT = i32 (days since 2000-01-01), TimeADT = i64 (usec).

use crate::c::PG_INT32_MAX;
use crate::c::PG_INT32_MIN;
use crate::datatype::timestamp::fsec_t;
use crate::datatype::timestamp::Timestamp;
use crate::datatype::timestamp::TimestampTz;
use crate::pgtime::pg_tm;
use crate::postgres::Datum;

pub type DateADT = i32;
pub type TimeADT = i64;

/// time-with-time-zone: in-memory broken-down value (not a varlena).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimeTzADT {
    pub time: TimeADT, // all time units other than months and years
    pub zone: i32,     // numeric time zone, in seconds
}

// Infinity and minus infinity are the max and min values of DateADT.
pub const DATEVAL_NOBEGIN: DateADT = PG_INT32_MIN;
pub const DATEVAL_NOEND: DateADT = PG_INT32_MAX;

#[inline]
pub fn DATE_NOBEGIN(j: &mut DateADT) {
    *j = DATEVAL_NOBEGIN;
}
#[inline]
pub const fn DATE_IS_NOBEGIN(j: DateADT) -> bool {
    j == DATEVAL_NOBEGIN
}
#[inline]
pub fn DATE_NOEND(j: &mut DateADT) {
    *j = DATEVAL_NOEND;
}
#[inline]
pub const fn DATE_IS_NOEND(j: DateADT) -> bool {
    j == DATEVAL_NOEND
}
#[inline]
pub const fn DATE_NOT_FINITE(j: DateADT) -> bool {
    DATE_IS_NOBEGIN(j) || DATE_IS_NOEND(j)
}

pub const MAX_TIME_PRECISION: i32 = 6;

// fmgr interface
#[inline]
pub fn DatumGetDateADT(x: Datum) -> DateADT {
    x.0 as i32
}
#[inline]
pub fn DatumGetTimeADT(x: Datum) -> TimeADT {
    x.0 as i64
}
#[inline]
pub fn DatumGetTimeTzADTP(x: Datum) -> *mut TimeTzADT {
    x.0 as *mut TimeTzADT // TODO(ptr)
}
#[inline]
pub fn DateADTGetDatum(x: DateADT) -> Datum {
    Datum(x as usize)
}
#[inline]
pub fn TimeADTGetDatum(x: TimeADT) -> Datum {
    Datum(x as usize)
}
#[inline]
pub fn TimeTzADTPGetDatum(x: &TimeTzADT) -> Datum {
    Datum(std::ptr::from_ref::<TimeTzADT>(x) as usize) // TODO(ptr)
}

// date.c bodies live in crate::backend::utils::adt::date (.c-defs invariant);
// re-exported so callers keep using `crate::utils::date::*`. AdjustTimeForTypmod
// is the deprecated C name for the backend's `adjust_time_for_typmod`.
pub use crate::backend::utils::adt::date::{
    adjust_time_for_typmod as AdjustTimeForTypmod, anytime_typmod_check,
    date2timestamp_no_overflow, date2timestamp_opt_overflow, date2timestamptz_opt_overflow,
    date_cmp_timestamp_internal, date_cmp_timestamptz_internal, float_time_overflows, time2tm,
    time_overflows, timetz2tm, tm2time, tm2timetz,
};

// STAGED: need session/transaction state or the IANA tz DB. TODO(timezone-db).
pub fn EncodeSpecialDate(dt: DateADT) -> String {
    unimplemented!()
}
pub fn GetSQLCurrentDate() -> DateADT {
    unimplemented!()
}
pub fn GetSQLCurrentTime(typmod: i32) -> *mut TimeTzADT {
    unimplemented!() // TODO(ptr)
}
pub fn GetSQLLocalTime(typmod: i32) -> TimeADT {
    unimplemented!()
}
