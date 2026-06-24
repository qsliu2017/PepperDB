//! Translated from PostgreSQL src/include/utils/numeric.h
//!
//! Numeric is a varlena (on-disk); its contents are private to numeric.c, so it
//! stays an opaque varlena-backed type here.

use crate::common::pg_prng::PgPrngState;
use crate::postgres::Datum;

// Limits on precision/scale specifiable in a NUMERIC typmod.
pub const NUMERIC_MAX_PRECISION: i32 = 1000;
pub const NUMERIC_MIN_SCALE: i32 = -1000;
pub const NUMERIC_MAX_SCALE: i32 = 1000;

// Internal limits on scales chosen for calculation results.
pub const NUMERIC_MAX_DISPLAY_SCALE: i32 = NUMERIC_MAX_PRECISION;
pub const NUMERIC_MIN_DISPLAY_SCALE: i32 = 0;
pub const NUMERIC_MAX_RESULT_SCALE: i32 = NUMERIC_MAX_PRECISION * 2;

pub const NUMERIC_MIN_SIG_DIGITS: i32 = 16;

/// On-disk NUMERIC: contents are private to numeric.c (opaque varlena). C names
/// the value `NumericData` and uses `Numeric` for the pointer.
pub struct NumericData {
    _private: [u8; 0],
}
pub type Numeric = *mut NumericData; // TODO(ptr)

// fmgr interface
#[inline]
pub fn DatumGetNumeric(x: Datum) -> Numeric {
    x.0 as Numeric // PG_DETOAST_DATUM; TODO(ptr)
}
#[inline]
pub fn DatumGetNumericCopy(x: Datum) -> Numeric {
    x.0 as Numeric // PG_DETOAST_DATUM_COPY; TODO(ptr)
}
#[inline]
pub fn NumericGetDatum(x: Numeric) -> Datum {
    Datum(x as usize)
}

// Utility functions in numeric.c
pub fn numeric_is_nan(num: Numeric) -> bool {
    unimplemented!()
}
pub fn numeric_is_inf(num: Numeric) -> bool {
    unimplemented!()
}
pub fn numeric_maximum_size(typmod: i32) -> i32 {
    unimplemented!()
}
pub fn numeric_out_sci(num: Numeric, scale: i32) -> String {
    unimplemented!()
}
pub fn numeric_normalize(num: Numeric) -> String {
    unimplemented!()
}

pub fn int64_to_numeric(val: i64) -> Numeric {
    unimplemented!()
}
pub fn int64_div_fast_to_numeric(val1: i64, log10val2: i32) -> Numeric {
    unimplemented!()
}

/// `bool *have_error` soft-error out-param -> Result.
pub fn numeric_add_opt_error(num1: Numeric, num2: Numeric) -> Result<Numeric, ()> {
    unimplemented!()
}
pub fn numeric_sub_opt_error(num1: Numeric, num2: Numeric) -> Result<Numeric, ()> {
    unimplemented!()
}
pub fn numeric_mul_opt_error(num1: Numeric, num2: Numeric) -> Result<Numeric, ()> {
    unimplemented!()
}
pub fn numeric_div_opt_error(num1: Numeric, num2: Numeric) -> Result<Numeric, ()> {
    unimplemented!()
}
pub fn numeric_mod_opt_error(num1: Numeric, num2: Numeric) -> Result<Numeric, ()> {
    unimplemented!()
}
pub fn numeric_int4_opt_error(num: Numeric) -> Result<i32, ()> {
    unimplemented!()
}
pub fn numeric_int8_opt_error(num: Numeric) -> Result<i64, ()> {
    unimplemented!()
}

pub fn random_numeric(state: &mut PgPrngState, rmin: Numeric, rmax: Numeric) -> Numeric {
    unimplemented!()
}
