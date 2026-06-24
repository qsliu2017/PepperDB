//! Translated from PostgreSQL src/include/utils/cash.h

// Money stored/handled as a 64-bit integer.

use crate::postgres::Datum;

pub type Cash = i64;

#[inline]
pub fn DatumGetCash(x: Datum) -> Cash {
    x.0 as i64
}

#[inline]
pub fn CashGetDatum(x: Cash) -> Datum {
    Datum(x as usize)
}
