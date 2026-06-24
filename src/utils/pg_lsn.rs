//! Translated from PostgreSQL src/include/utils/pg_lsn.h

use crate::access::xlogdefs::XLogRecPtr;
use crate::postgres::Datum;

#[inline]
pub fn DatumGetLSN(x: Datum) -> XLogRecPtr {
    XLogRecPtr(x.0 as u64)
}

#[inline]
pub fn LSNGetDatum(x: XLogRecPtr) -> Datum {
    Datum(x.0 as usize)
}

/// InvalidOid sentinel maps to None: parse failure surfaced via Result.
pub fn pg_lsn_in_internal(s: &str) -> Result<XLogRecPtr, ()> {
    unimplemented!()
}
