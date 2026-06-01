//! Translation of postgres/src/include/utils/xid8.h
//!
//! fmgr glue for the SQL `xid8` type (a FullTransactionId passed by value as a
//! uint64 Datum).
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//!
//! PG_GETARG_FULLTRANSACTIONID(n) is spelled inline at call sites as
//! `DatumGetFullTransactionId(PG_GETARG_DATUM!(fcinfo, n))`, and
//! PG_RETURN_FULLTRANSACTIONID(x) as `return FullTransactionIdGetDatum(x)`
//! (same convention as utils/uuid.h's UUIDPGetDatum/DatumGetUUIDP in uuid.rs).

use crate::access::transam::{FullTransactionId, FullTransactionIdFromU64, U64FromFullTransactionId};
use crate::postgres::{Datum, DatumGetUInt64, UInt64GetDatum};

/// # Safety
/// See [`crate::postgres::DatumGetUInt64`].
#[inline]
pub unsafe fn DatumGetFullTransactionId(x: Datum) -> FullTransactionId {
    FullTransactionIdFromU64(DatumGetUInt64(x))
}

#[inline]
pub fn FullTransactionIdGetDatum(x: FullTransactionId) -> Datum {
    UInt64GetDatum(U64FromFullTransactionId(x))
}
