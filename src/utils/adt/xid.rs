//! Translation of postgres/src/backend/utils/adt/xid.c
//!
//! Functions for the built-in types xid (TransactionId), xid8 (FullTransactionId),
//! and cid (CommandId).
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! `#include`s mapped: common/hashfn.h -> crate::common::hashfn, common/int.h ->
//! crate::common::int (pg_cmp_u32), access/transam.h -> crate::access::transam,
//! utils/xid8.h -> crate::utils::xid8, utils/builtins.h's uint32in_subr/uint64in_subr
//! -> crate::utils::adt::numutils, libpq/pqformat -> crate::libpq::pqformat.  snprintf
//! bound via extern "C".
//!
//! STUBBED (deps not yet ported):
//!  - xid_age/mxid_age (transam.c/multixact.c transaction-state: GetStableLatestTransactionId,
//!    ReadNextMultiXactId).
//!  - xidLogicalComparator (transam.c TransactionIdPrecedes wraparound compare).
//!  - hashxid8/hashxid8extended (delegate to hashint8/hashint8extended in access/hash/hashfunc.c).
//!  - xidsend/xid8send/cidsend (pq_endtypsend -> varatt/bytea).

use crate::prelude::*;
use crate::utils::fmgr::*;
use crate::{PG_GETARG_DATUM, PG_GETARG_INT64, PG_GETARG_POINTER, PG_RETURN_BOOL, PG_RETURN_CSTRING, PG_RETURN_INT32};
use crate::c::{int32, uint64, CommandId, TransactionId};
use crate::access::transam::{
    FullTransactionId, FullTransactionIdEquals, FullTransactionIdFollows,
    FullTransactionIdFollowsOrEquals, FullTransactionIdFromU64, FullTransactionIdPrecedes,
    FullTransactionIdPrecedesOrEquals, TransactionIdEquals, U64FromFullTransactionId,
    XidFromFullTransactionId,
};
use crate::common::hashfn::{hash_uint32, hash_uint32_extended};
use crate::common::int::pg_cmp_u32;
use crate::postgres::{CommandIdGetDatum, DatumGetCommandId, DatumGetTransactionId, TransactionIdGetDatum};
use crate::utils::adt::numutils::{uint32in_subr, uint64in_subr};
use crate::utils::xid8::{DatumGetFullTransactionId, FullTransactionIdGetDatum};
use crate::libpq::pqformat::{pq_getmsgint, pq_getmsgint64};
use crate::lib::stringinfo::StringInfo;
use core::ffi::{c_char, c_int, c_uint, c_ulonglong, c_void};

extern "C" {
    fn snprintf(s: *mut c_char, n: usize, fmt: *const c_char, ...) -> c_int;
}

pub unsafe fn xidin(fcinfo: FunctionCallInfo) -> Datum {
    let str: *mut c_char = PG_GETARG_DATUM!(fcinfo, 0) as *mut c_char; // PG_GETARG_CSTRING
    let result: TransactionId;

    result = uint32in_subr(str, null_mut(), c"xid".as_ptr(), (*fcinfo).context);
    return TransactionIdGetDatum(result); // PG_RETURN_TRANSACTIONID
}

pub unsafe fn xidout(fcinfo: FunctionCallInfo) -> Datum {
    let transaction_id: TransactionId = DatumGetTransactionId(PG_GETARG_DATUM!(fcinfo, 0));
    let result: *mut c_char = palloc(16) as *mut c_char;

    snprintf(result, 16, c"%u".as_ptr(), transaction_id as c_uint);
    PG_RETURN_CSTRING!(result);
}

/*
 *		xidrecv			- converts external binary format to xid
 */
pub unsafe fn xidrecv(fcinfo: FunctionCallInfo) -> Datum {
    let buf: StringInfo = PG_GETARG_POINTER!(fcinfo, 0) as StringInfo;

    return TransactionIdGetDatum(
        pq_getmsgint(buf, core::mem::size_of::<TransactionId>() as c_int) as TransactionId,
    );
}

/*
 *		xidsend			- converts xid to binary format  [STUBBED]
 */
pub unsafe fn xidsend(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: TransactionId = DatumGetTransactionId(PG_GETARG_DATUM!(fcinfo, 0));
    // C: pq_begintypsend(&buf); pq_sendint32(&buf, arg1); PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
    // TODO(pg-port): pq_endtypsend needs varatt.h/bytea (not yet translated).
    let _ = arg1;
    unimplemented!("xidsend: pq_endtypsend (varatt/bytea) not yet translated")
}

/*
 *		xideq			- are two xids equal?
 */
pub unsafe fn xideq(fcinfo: FunctionCallInfo) -> Datum {
    let xid1: TransactionId = DatumGetTransactionId(PG_GETARG_DATUM!(fcinfo, 0));
    let xid2: TransactionId = DatumGetTransactionId(PG_GETARG_DATUM!(fcinfo, 1));

    PG_RETURN_BOOL!(TransactionIdEquals(xid1, xid2));
}

/*
 *		xidneq			- are two xids different?
 */
pub unsafe fn xidneq(fcinfo: FunctionCallInfo) -> Datum {
    let xid1: TransactionId = DatumGetTransactionId(PG_GETARG_DATUM!(fcinfo, 0));
    let xid2: TransactionId = DatumGetTransactionId(PG_GETARG_DATUM!(fcinfo, 1));

    PG_RETURN_BOOL!(!TransactionIdEquals(xid1, xid2));
}

pub unsafe fn hashxid(fcinfo: FunctionCallInfo) -> Datum {
    hash_uint32(DatumGetTransactionId(PG_GETARG_DATUM!(fcinfo, 0)))
}

pub unsafe fn hashxidextended(fcinfo: FunctionCallInfo) -> Datum {
    hash_uint32_extended(
        DatumGetTransactionId(PG_GETARG_DATUM!(fcinfo, 0)),
        PG_GETARG_INT64!(fcinfo, 1) as u64,
    )
}

/*
 *		xid_age			- compute age of an XID (relative to latest stable xid)  [STUBBED]
 */
pub unsafe fn xid_age(fcinfo: FunctionCallInfo) -> Datum {
    // C: now = GetStableLatestTransactionId(); if (!TransactionIdIsNormal(xid)) return INT_MAX;
    //    return (int32)(now - xid);
    // TODO(pg-port): GetStableLatestTransactionId (transam.c transaction state) not translated.
    let _ = fcinfo;
    unimplemented!("xid_age: GetStableLatestTransactionId (transam.c) not yet translated")
}

/*
 *		mxid_age			- compute age of a multi XID  [STUBBED]
 */
pub unsafe fn mxid_age(fcinfo: FunctionCallInfo) -> Datum {
    // C: now = ReadNextMultiXactId(); if (!MultiXactIdIsValid(xid)) return INT_MAX;
    // TODO(pg-port): ReadNextMultiXactId (access/multixact.c) not translated.
    let _ = fcinfo;
    unimplemented!("mxid_age: ReadNextMultiXactId (multixact.c) not yet translated")
}

/*
 * xidComparator - qsort comparison function for XIDs (arbitrary, total order).
 *
 * # Safety
 * `arg1`/`arg2` point to readable TransactionId values.
 */
pub unsafe fn xidComparator(arg1: *const c_void, arg2: *const c_void) -> c_int {
    let xid1: TransactionId = *(arg1 as *const TransactionId);
    let xid2: TransactionId = *(arg2 as *const TransactionId);
    pg_cmp_u32(xid1, xid2)
}

/*
 * xidLogicalComparator - qsort comparison using logical (wraparound) order.  [STUBBED]
 */
pub unsafe fn xidLogicalComparator(arg1: *const c_void, arg2: *const c_void) -> c_int {
    // C uses TransactionIdPrecedes() (wraparound-aware, only valid within an epoch).
    // TODO(pg-port): TransactionIdPrecedes (transam.c) not yet translated.
    let _ = (arg1, arg2);
    unimplemented!("xidLogicalComparator: TransactionIdPrecedes (transam.c) not yet translated")
}

pub unsafe fn xid8toxid(fcinfo: FunctionCallInfo) -> Datum {
    let fxid: FullTransactionId = DatumGetFullTransactionId(PG_GETARG_DATUM!(fcinfo, 0));

    return TransactionIdGetDatum(XidFromFullTransactionId(fxid));
}

pub unsafe fn xid8in(fcinfo: FunctionCallInfo) -> Datum {
    let str: *mut c_char = PG_GETARG_DATUM!(fcinfo, 0) as *mut c_char;
    let result: uint64;

    result = uint64in_subr(str, null_mut(), c"xid8".as_ptr(), (*fcinfo).context);
    return FullTransactionIdGetDatum(FullTransactionIdFromU64(result));
}

pub unsafe fn xid8out(fcinfo: FunctionCallInfo) -> Datum {
    let fxid: FullTransactionId = DatumGetFullTransactionId(PG_GETARG_DATUM!(fcinfo, 0));
    let result: *mut c_char = palloc(21) as *mut c_char;

    snprintf(result, 21, c"%llu".as_ptr(), U64FromFullTransactionId(fxid) as c_ulonglong);
    PG_RETURN_CSTRING!(result);
}

pub unsafe fn xid8recv(fcinfo: FunctionCallInfo) -> Datum {
    let buf: StringInfo = PG_GETARG_POINTER!(fcinfo, 0) as StringInfo;
    let value: uint64;

    value = pq_getmsgint64(buf) as uint64;
    return FullTransactionIdGetDatum(FullTransactionIdFromU64(value));
}

pub unsafe fn xid8send(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: FullTransactionId = DatumGetFullTransactionId(PG_GETARG_DATUM!(fcinfo, 0));
    // C: pq_begintypsend; pq_sendint64(&buf, U64FromFullTransactionId(arg1)); PG_RETURN_BYTEA_P(pq_endtypsend);
    // TODO(pg-port): pq_endtypsend needs varatt.h/bytea.
    let _ = arg1;
    unimplemented!("xid8send: pq_endtypsend (varatt/bytea) not yet translated")
}

pub unsafe fn xid8eq(fcinfo: FunctionCallInfo) -> Datum {
    let f1 = DatumGetFullTransactionId(PG_GETARG_DATUM!(fcinfo, 0));
    let f2 = DatumGetFullTransactionId(PG_GETARG_DATUM!(fcinfo, 1));
    PG_RETURN_BOOL!(FullTransactionIdEquals(f1, f2));
}
pub unsafe fn xid8ne(fcinfo: FunctionCallInfo) -> Datum {
    let f1 = DatumGetFullTransactionId(PG_GETARG_DATUM!(fcinfo, 0));
    let f2 = DatumGetFullTransactionId(PG_GETARG_DATUM!(fcinfo, 1));
    PG_RETURN_BOOL!(!FullTransactionIdEquals(f1, f2));
}
pub unsafe fn xid8lt(fcinfo: FunctionCallInfo) -> Datum {
    let f1 = DatumGetFullTransactionId(PG_GETARG_DATUM!(fcinfo, 0));
    let f2 = DatumGetFullTransactionId(PG_GETARG_DATUM!(fcinfo, 1));
    PG_RETURN_BOOL!(FullTransactionIdPrecedes(f1, f2));
}
pub unsafe fn xid8gt(fcinfo: FunctionCallInfo) -> Datum {
    let f1 = DatumGetFullTransactionId(PG_GETARG_DATUM!(fcinfo, 0));
    let f2 = DatumGetFullTransactionId(PG_GETARG_DATUM!(fcinfo, 1));
    PG_RETURN_BOOL!(FullTransactionIdFollows(f1, f2));
}
pub unsafe fn xid8le(fcinfo: FunctionCallInfo) -> Datum {
    let f1 = DatumGetFullTransactionId(PG_GETARG_DATUM!(fcinfo, 0));
    let f2 = DatumGetFullTransactionId(PG_GETARG_DATUM!(fcinfo, 1));
    PG_RETURN_BOOL!(FullTransactionIdPrecedesOrEquals(f1, f2));
}
pub unsafe fn xid8ge(fcinfo: FunctionCallInfo) -> Datum {
    let f1 = DatumGetFullTransactionId(PG_GETARG_DATUM!(fcinfo, 0));
    let f2 = DatumGetFullTransactionId(PG_GETARG_DATUM!(fcinfo, 1));
    PG_RETURN_BOOL!(FullTransactionIdFollowsOrEquals(f1, f2));
}

pub unsafe fn xid8cmp(fcinfo: FunctionCallInfo) -> Datum {
    let f1 = DatumGetFullTransactionId(PG_GETARG_DATUM!(fcinfo, 0));
    let f2 = DatumGetFullTransactionId(PG_GETARG_DATUM!(fcinfo, 1));

    if FullTransactionIdFollows(f1, f2) {
        PG_RETURN_INT32!(1);
    } else if FullTransactionIdEquals(f1, f2) {
        PG_RETURN_INT32!(0);
    } else {
        PG_RETURN_INT32!(-1);
    }
}

pub unsafe fn hashxid8(fcinfo: FunctionCallInfo) -> Datum {
    crate::access::hash::hashfunc::hashint8(fcinfo)
}

pub unsafe fn hashxid8extended(fcinfo: FunctionCallInfo) -> Datum {
    crate::access::hash::hashfunc::hashint8extended(fcinfo)
}

pub unsafe fn xid8_larger(fcinfo: FunctionCallInfo) -> Datum {
    let f1 = DatumGetFullTransactionId(PG_GETARG_DATUM!(fcinfo, 0));
    let f2 = DatumGetFullTransactionId(PG_GETARG_DATUM!(fcinfo, 1));

    if FullTransactionIdFollows(f1, f2) {
        return FullTransactionIdGetDatum(f1);
    } else {
        return FullTransactionIdGetDatum(f2);
    }
}

pub unsafe fn xid8_smaller(fcinfo: FunctionCallInfo) -> Datum {
    let f1 = DatumGetFullTransactionId(PG_GETARG_DATUM!(fcinfo, 0));
    let f2 = DatumGetFullTransactionId(PG_GETARG_DATUM!(fcinfo, 1));

    if FullTransactionIdPrecedes(f1, f2) {
        return FullTransactionIdGetDatum(f1);
    } else {
        return FullTransactionIdGetDatum(f2);
    }
}

/*****************************************************************************
 *	 COMMAND IDENTIFIER ROUTINES											 *
 *****************************************************************************/

pub unsafe fn cidin(fcinfo: FunctionCallInfo) -> Datum {
    let str: *mut c_char = PG_GETARG_DATUM!(fcinfo, 0) as *mut c_char;
    let result: CommandId;

    result = uint32in_subr(str, null_mut(), c"cid".as_ptr(), (*fcinfo).context);
    return CommandIdGetDatum(result); // PG_RETURN_COMMANDID
}

pub unsafe fn cidout(fcinfo: FunctionCallInfo) -> Datum {
    let c: CommandId = DatumGetCommandId(PG_GETARG_DATUM!(fcinfo, 0));
    let result: *mut c_char = palloc(16) as *mut c_char;

    snprintf(result, 16, c"%u".as_ptr(), c as c_uint);
    PG_RETURN_CSTRING!(result);
}

pub unsafe fn cidrecv(fcinfo: FunctionCallInfo) -> Datum {
    let buf: StringInfo = PG_GETARG_POINTER!(fcinfo, 0) as StringInfo;

    return CommandIdGetDatum(pq_getmsgint(buf, core::mem::size_of::<CommandId>() as c_int) as CommandId);
}

pub unsafe fn cidsend(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: CommandId = DatumGetCommandId(PG_GETARG_DATUM!(fcinfo, 0));
    // TODO(pg-port): pq_endtypsend (varatt/bytea) not yet translated.
    let _ = arg1;
    unimplemented!("cidsend: pq_endtypsend (varatt/bytea) not yet translated")
}

pub unsafe fn cideq(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: CommandId = DatumGetCommandId(PG_GETARG_DATUM!(fcinfo, 0));
    let arg2: CommandId = DatumGetCommandId(PG_GETARG_DATUM!(fcinfo, 1));

    PG_RETURN_BOOL!(arg1 == arg2);
}

pub unsafe fn hashcid(fcinfo: FunctionCallInfo) -> Datum {
    hash_uint32(DatumGetCommandId(PG_GETARG_DATUM!(fcinfo, 0)))
}

pub unsafe fn hashcidextended(fcinfo: FunctionCallInfo) -> Datum {
    hash_uint32_extended(
        DatumGetCommandId(PG_GETARG_DATUM!(fcinfo, 0)),
        PG_GETARG_INT64!(fcinfo, 1) as u64,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::postgres::{CStringGetDatum, DatumGetBool, DatumGetCString, DatumGetInt32};
    use crate::postgres_ext::InvalidOid;
    use crate::utils::fmgr::{DirectFunctionCall1Coll, DirectFunctionCall2Coll};

    unsafe fn cstr_eq(p: *mut c_char, want: &str) -> bool {
        let mut n = 0usize;
        while *p.add(n) != 0 {
            n += 1;
        }
        core::slice::from_raw_parts(p as *const u8, n) == want.as_bytes()
    }

    #[test]
    fn xid_xid8_cid_io_and_ops() {
        unsafe {
            // xid in/out + eq/neq
            let a = DirectFunctionCall1Coll(xidin, InvalidOid, CStringGetDatum(c"42".as_ptr()));
            assert_eq!(DatumGetTransactionId(a), 42);
            let s = DatumGetCString(DirectFunctionCall1Coll(xidout, InvalidOid, TransactionIdGetDatum(4294967290)));
            assert!(cstr_eq(s, "4294967290"));
            assert!(DatumGetBool(DirectFunctionCall2Coll(xideq, InvalidOid, a, a)));
            assert!(DatumGetBool(DirectFunctionCall2Coll(xidneq, InvalidOid, a, TransactionIdGetDatum(7))));

            // xid8 in/out + ordering + cmp + larger/smaller + toxid
            let big = DirectFunctionCall1Coll(xid8in, InvalidOid, CStringGetDatum(c"12884901892".as_ptr())); // (3<<32)|4
            let s8 = DatumGetCString(DirectFunctionCall1Coll(xid8out, InvalidOid, big));
            assert!(cstr_eq(s8, "12884901892"));
            let small = DirectFunctionCall1Coll(xid8in, InvalidOid, CStringGetDatum(c"5".as_ptr()));
            assert!(DatumGetBool(DirectFunctionCall2Coll(xid8lt, InvalidOid, small, big)));
            assert!(DatumGetBool(DirectFunctionCall2Coll(xid8gt, InvalidOid, big, small)));
            assert_eq!(DatumGetInt32(DirectFunctionCall2Coll(xid8cmp, InvalidOid, small, big)), -1);
            assert_eq!(DatumGetInt32(DirectFunctionCall2Coll(xid8cmp, InvalidOid, big, big)), 0);
            // xid8toxid takes the low 32 bits: (3<<32)|4 -> 4
            let lo = DirectFunctionCall1Coll(xid8toxid, InvalidOid, big);
            assert_eq!(DatumGetTransactionId(lo), 4);
            // xid8_larger / _smaller
            assert!(FullTransactionIdEquals(
                DatumGetFullTransactionId(DirectFunctionCall2Coll(xid8_larger, InvalidOid, small, big)),
                DatumGetFullTransactionId(big)
            ));

            // cid in/out + eq
            let c = DirectFunctionCall1Coll(cidin, InvalidOid, CStringGetDatum(c"99".as_ptr()));
            assert_eq!(DatumGetCommandId(c), 99);
            assert!(DatumGetBool(DirectFunctionCall2Coll(cideq, InvalidOid, c, c)));

            // xidComparator (qsort): total order by value
            let x1: TransactionId = 10;
            let x2: TransactionId = 20;
            assert!(xidComparator(&x1 as *const _ as *const c_void, &x2 as *const _ as *const c_void) < 0);
        }
    }

    #[test]
    #[should_panic]
    fn xid8in_rejects_garbage() {
        unsafe {
            DirectFunctionCall1Coll(xid8in, InvalidOid, CStringGetDatum(c"notanumber".as_ptr()));
        }
    }
}
