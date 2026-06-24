//! Translated from PostgreSQL src/include/utils/xid8.h

use crate::access::transam::{
    full_transaction_id_from_u64, u64_from_full_transaction_id, FullTransactionId,
};
use crate::postgres::Datum;

pub fn DatumGetFullTransactionId(x: Datum) -> FullTransactionId {
    full_transaction_id_from_u64(x.0 as u64)
}

pub fn FullTransactionIdGetDatum(x: FullTransactionId) -> Datum {
    Datum(u64_from_full_transaction_id(x) as usize)
}

// PG_GETARG_FULLTRANSACTIONID / PG_RETURN_FULLTRANSACTIONID are fmgr arg macros;
// fold into the above via the fcinfo accessors at call sites.
