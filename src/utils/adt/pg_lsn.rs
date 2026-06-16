//! Translation of postgres/src/backend/utils/adt/pg_lsn.c
//!                (+ the fmgr glue from postgres/src/include/utils/pg_lsn.h)
//!
//! The `pg_lsn` SQL type: a WAL location, i.e. an XLogRecPtr (uint64) printed as
//! "%X/%X".
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! `#include`s mapped: libpq/pqformat -> crate::libpq::pqformat, common/hashfn via
//! the hashint8 delegates in crate::access::hash::hashfunc.  The arithmetic
//! operators pg_lsn_mi/pli/mii produce/consume `numeric` (utils/numeric.c, not yet
//! translated) and are STUBBED.

use crate::prelude::*;
use crate::utils::fmgr::*;
use crate::{
    PG_GETARG_DATUM, PG_GETARG_POINTER, PG_RETURN_BOOL, PG_RETURN_CSTRING, PG_RETURN_INT32,
};
use crate::c::uint32;
use crate::postgres::{DatumGetPointer, DatumGetUInt64, PointerGetDatum, UInt64GetDatum};
use crate::libpq::pqformat::{pq_begintypsend, pq_endtypsend, pq_getmsgint64, pq_sendint64};
use crate::access::hash::hashfunc::{hashint8, hashint8extended};
use crate::utils::adt::numeric::{
    numeric_add, numeric_in, numeric_is_nan, numeric_pg_lsn, numeric_sub, Numeric,
};
use crate::postgres::{CStringGetDatum, Int32GetDatum, ObjectIdGetDatum};
use crate::utils::fmgr::{DirectFunctionCall1Coll, DirectFunctionCall2Coll, DirectFunctionCall3Coll};
use crate::postgres_ext::InvalidOid;
use crate::lib::stringinfo::{StringInfo, StringInfoData};
use crate::nodes::nodes::Node;
use core::ffi::{c_char, c_int, c_uint, c_ulong, c_void};

/* access/xlogdefs.h */
pub type XLogRecPtr = u64;
pub const InvalidXLogRecPtr: XLogRecPtr = 0;

/* pg_lsn.h */
const MAXPG_LSNCOMPONENT: c_int = 8;
const MAXPG_LSNLEN: usize = 17;

/* errcodes.h (errcode() shim ignores the value). */
const ERRCODE_INVALID_TEXT_REPRESENTATION: c_int = 0;

extern "C" {
    fn strspn(s: *const c_char, accept: *const c_char) -> usize;
    fn strtoul(s: *const c_char, endptr: *mut *mut c_char, base: c_int) -> c_ulong;
    fn snprintf(s: *mut c_char, n: usize, fmt: *const c_char, ...) -> c_int;
}

// pg_lsn.h fmgr glue: pg_lsn is passed by value as a uint64 Datum.
/// # Safety
/// See [`crate::postgres::DatumGetUInt64`].
#[inline]
pub unsafe fn DatumGetLSN(x: Datum) -> XLogRecPtr {
    DatumGetUInt64(x)
}
#[inline]
pub fn LSNGetDatum(x: XLogRecPtr) -> Datum {
    UInt64GetDatum(x)
}

/*----------------------------------------------------------
 * Formatting and conversion routines.
 *---------------------------------------------------------*/

/*
 * pg_lsn_in_internal - parse "%X/%X" into an XLogRecPtr; sets *have_error on bad
 * input rather than throwing.
 *
 * # Safety
 * `str` is a valid NUL-terminated C string; `have_error` is writable.
 */
pub unsafe fn pg_lsn_in_internal(str: *const c_char, have_error: *mut bool) -> XLogRecPtr {
    let len1: c_int;
    let len2: c_int;
    let id: uint32;
    let off: uint32;
    let result: XLogRecPtr;

    Assert!(!have_error.is_null());
    *have_error = false;

    /* Sanity check input format. */
    let hexset = c"0123456789abcdefABCDEF".as_ptr();
    len1 = strspn(str, hexset) as c_int;
    if len1 < 1 || len1 > MAXPG_LSNCOMPONENT || *str.add(len1 as usize) as u8 != b'/' {
        *have_error = true;
        return InvalidXLogRecPtr;
    }
    len2 = strspn(str.add((len1 + 1) as usize), hexset) as c_int;
    if len2 < 1 || len2 > MAXPG_LSNCOMPONENT || *str.add((len1 + 1 + len2) as usize) as u8 != b'\0' {
        *have_error = true;
        return InvalidXLogRecPtr;
    }

    /* Decode result. */
    id = strtoul(str, null_mut(), 16) as uint32;
    off = strtoul(str.add((len1 + 1) as usize), null_mut(), 16) as uint32;
    result = ((id as u64) << 32) | off as u64;

    result
}

pub unsafe fn pg_lsn_in(fcinfo: FunctionCallInfo) -> Datum {
    let str: *mut c_char = PG_GETARG_DATUM!(fcinfo, 0) as *mut c_char; // PG_GETARG_CSTRING
    let result: XLogRecPtr;
    let mut have_error: bool = false;

    result = pg_lsn_in_internal(str, &mut have_error);
    if have_error {
        let _ = errcode(ERRCODE_INVALID_TEXT_REPRESENTATION);
        let _ = (*fcinfo).context as *mut Node; // ereturn(fcinfo->context, ...) -> hard ERROR for now
        ereport!(
            ERROR,
            errmsg!("invalid input syntax for type {}: \"{}\"", "pg_lsn", cstr(str))
        );
        return 0 as Datum;
    }

    return LSNGetDatum(result); // PG_RETURN_LSN
}

pub unsafe fn pg_lsn_out(fcinfo: FunctionCallInfo) -> Datum {
    let lsn: XLogRecPtr = DatumGetLSN(PG_GETARG_DATUM!(fcinfo, 0));
    let mut buf = [0i8; MAXPG_LSNLEN + 1];
    let result: *mut c_char;

    // LSN_FORMAT_ARGS(lsn) = (uint32)(lsn >> 32), (uint32) lsn
    snprintf(
        buf.as_mut_ptr(),
        MAXPG_LSNLEN + 1,
        c"%X/%X".as_ptr(),
        (lsn >> 32) as c_uint,
        lsn as c_uint,
    );
    result = pstrdup(buf.as_ptr());
    PG_RETURN_CSTRING!(result);
}

pub unsafe fn pg_lsn_recv(fcinfo: FunctionCallInfo) -> Datum {
    let buf: StringInfo = PG_GETARG_POINTER!(fcinfo, 0) as StringInfo;
    let result: XLogRecPtr = pq_getmsgint64(buf) as XLogRecPtr;
    return LSNGetDatum(result);
}

pub unsafe fn pg_lsn_send(fcinfo: FunctionCallInfo) -> Datum {
    let lsn: XLogRecPtr = DatumGetLSN(PG_GETARG_DATUM!(fcinfo, 0));
    let mut buf: StringInfoData = core::mem::zeroed();

    pq_begintypsend(&mut buf);
    pq_sendint64(&mut buf, lsn);
    return PointerGetDatum(pq_endtypsend(&mut buf) as *const c_void); // PG_RETURN_BYTEA_P
}

/*----------------------------------------------------------
 *	Operators for PostgreSQL LSNs
 *---------------------------------------------------------*/

pub unsafe fn pg_lsn_eq(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_BOOL!(DatumGetLSN(PG_GETARG_DATUM!(fcinfo, 0)) == DatumGetLSN(PG_GETARG_DATUM!(fcinfo, 1)));
}
pub unsafe fn pg_lsn_ne(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_BOOL!(DatumGetLSN(PG_GETARG_DATUM!(fcinfo, 0)) != DatumGetLSN(PG_GETARG_DATUM!(fcinfo, 1)));
}
pub unsafe fn pg_lsn_lt(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_BOOL!(DatumGetLSN(PG_GETARG_DATUM!(fcinfo, 0)) < DatumGetLSN(PG_GETARG_DATUM!(fcinfo, 1)));
}
pub unsafe fn pg_lsn_gt(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_BOOL!(DatumGetLSN(PG_GETARG_DATUM!(fcinfo, 0)) > DatumGetLSN(PG_GETARG_DATUM!(fcinfo, 1)));
}
pub unsafe fn pg_lsn_le(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_BOOL!(DatumGetLSN(PG_GETARG_DATUM!(fcinfo, 0)) <= DatumGetLSN(PG_GETARG_DATUM!(fcinfo, 1)));
}
pub unsafe fn pg_lsn_ge(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_BOOL!(DatumGetLSN(PG_GETARG_DATUM!(fcinfo, 0)) >= DatumGetLSN(PG_GETARG_DATUM!(fcinfo, 1)));
}

pub unsafe fn pg_lsn_larger(fcinfo: FunctionCallInfo) -> Datum {
    let lsn1 = DatumGetLSN(PG_GETARG_DATUM!(fcinfo, 0));
    let lsn2 = DatumGetLSN(PG_GETARG_DATUM!(fcinfo, 1));
    return LSNGetDatum(if lsn1 > lsn2 { lsn1 } else { lsn2 });
}
pub unsafe fn pg_lsn_smaller(fcinfo: FunctionCallInfo) -> Datum {
    let lsn1 = DatumGetLSN(PG_GETARG_DATUM!(fcinfo, 0));
    let lsn2 = DatumGetLSN(PG_GETARG_DATUM!(fcinfo, 1));
    return LSNGetDatum(if lsn1 < lsn2 { lsn1 } else { lsn2 });
}

/* btree index opclass support */
pub unsafe fn pg_lsn_cmp(fcinfo: FunctionCallInfo) -> Datum {
    let a = DatumGetLSN(PG_GETARG_DATUM!(fcinfo, 0));
    let b = DatumGetLSN(PG_GETARG_DATUM!(fcinfo, 1));
    if a > b {
        PG_RETURN_INT32!(1);
    } else if a == b {
        PG_RETURN_INT32!(0);
    } else {
        PG_RETURN_INT32!(-1);
    }
}

/* hash index opclass support (a pg_lsn is a uint64, so reuse hashint8) */
pub unsafe fn pg_lsn_hash(fcinfo: FunctionCallInfo) -> Datum {
    hashint8(fcinfo)
}
pub unsafe fn pg_lsn_hash_extended(fcinfo: FunctionCallInfo) -> Datum {
    hashint8extended(fcinfo)
}

/*----------------------------------------------------------
 *	Arithmetic operators on PostgreSQL LSNs.
 *---------------------------------------------------------*/

pub unsafe fn pg_lsn_mi(fcinfo: FunctionCallInfo) -> Datum {
    let lsn1: XLogRecPtr = DatumGetLSN(PG_GETARG_DATUM!(fcinfo, 0));
    let lsn2: XLogRecPtr = DatumGetLSN(PG_GETARG_DATUM!(fcinfo, 1));
    let mut buf = [0i8; 256];
    let result: Datum;

    /* Output could be as large as plus or minus 2^63 - 1. */
    if lsn1 < lsn2 {
        // "-" UINT64_FORMAT
        snprintf(buf.as_mut_ptr(), 256, c"-%llu".as_ptr(), lsn2 - lsn1);
    } else {
        // UINT64_FORMAT
        snprintf(buf.as_mut_ptr(), 256, c"%llu".as_ptr(), lsn1 - lsn2);
    }

    /* Convert to numeric. */
    result = DirectFunctionCall3Coll(
        numeric_in,
        InvalidOid,
        CStringGetDatum(buf.as_ptr()),
        ObjectIdGetDatum(0),
        Int32GetDatum(-1),
    );

    return result;
}

/*
 * Add the number of bytes to pg_lsn, giving a new pg_lsn.
 * Must handle both positive and negative numbers of bytes.
 */
pub unsafe fn pg_lsn_pli(fcinfo: FunctionCallInfo) -> Datum {
    let lsn: XLogRecPtr = DatumGetLSN(PG_GETARG_DATUM!(fcinfo, 0));
    let nbytes: Numeric = DatumGetNumeric(PG_GETARG_DATUM!(fcinfo, 1));
    let num: Datum;
    let res: Datum;
    let mut buf = [0i8; 32];

    if numeric_is_nan(nbytes) {
        ereport!(ERROR, errmsg!("cannot add NaN to pg_lsn"));
        /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
    }

    /* Convert to numeric */
    snprintf(buf.as_mut_ptr(), 32, c"%llu".as_ptr(), lsn);
    num = DirectFunctionCall3Coll(
        numeric_in,
        InvalidOid,
        CStringGetDatum(buf.as_ptr()),
        ObjectIdGetDatum(0),
        Int32GetDatum(-1),
    );

    /* Add two numerics */
    res = DirectFunctionCall2Coll(numeric_add, InvalidOid, num, NumericGetDatum(nbytes));

    /* Convert to pg_lsn */
    return DirectFunctionCall1Coll(numeric_pg_lsn, InvalidOid, res);
}

/*
 * Subtract the number of bytes from pg_lsn, giving a new pg_lsn.
 * Must handle both positive and negative numbers of bytes.
 */
pub unsafe fn pg_lsn_mii(fcinfo: FunctionCallInfo) -> Datum {
    let lsn: XLogRecPtr = DatumGetLSN(PG_GETARG_DATUM!(fcinfo, 0));
    let nbytes: Numeric = DatumGetNumeric(PG_GETARG_DATUM!(fcinfo, 1));
    let num: Datum;
    let res: Datum;
    let mut buf = [0i8; 32];

    if numeric_is_nan(nbytes) {
        ereport!(ERROR, errmsg!("cannot subtract NaN from pg_lsn"));
        /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
    }

    /* Convert to numeric */
    snprintf(buf.as_mut_ptr(), 32, c"%llu".as_ptr(), lsn);
    num = DirectFunctionCall3Coll(
        numeric_in,
        InvalidOid,
        CStringGetDatum(buf.as_ptr()),
        ObjectIdGetDatum(0),
        Int32GetDatum(-1),
    );

    /* Subtract two numerics */
    res = DirectFunctionCall2Coll(numeric_sub, InvalidOid, num, NumericGetDatum(nbytes));

    /* Convert to pg_lsn */
    return DirectFunctionCall1Coll(numeric_pg_lsn, InvalidOid, res);
}

/* numeric.h fmgr glue (DatumGetNumeric/NumericGetDatum are not pub in numeric.rs). */
#[inline]
unsafe fn DatumGetNumeric(x: Datum) -> Numeric {
    DatumGetPointer(x) as Numeric
}
#[inline]
unsafe fn NumericGetDatum(x: Numeric) -> Datum {
    PointerGetDatum(x as *const c_void)
}

/*
 * Format a C string for an error message via Rust `{}` (lossy).
 *
 * # Safety
 * `s` is a valid NUL-terminated C string.
 */
unsafe fn cstr(s: *const c_char) -> std::string::String {
    let mut n = 0usize;
    while *s.add(n) != 0 {
        n += 1;
    }
    std::string::String::from_utf8_lossy(core::slice::from_raw_parts(s as *const u8, n)).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::postgres::{CStringGetDatum, DatumGetBool, DatumGetCString, DatumGetInt32};
    use crate::postgres_ext::InvalidOid;
    use crate::utils::fmgr::{DirectFunctionCall1Coll, DirectFunctionCall2Coll};

    unsafe fn cstr_eq(p: *const c_char, want: &str) -> bool {
        let mut n = 0usize;
        while *p.add(n) != 0 {
            n += 1;
        }
        core::slice::from_raw_parts(p as *const u8, n) == want.as_bytes()
    }

    #[test]
    fn pg_lsn_io_compare() {
        unsafe {
            // in/out round trip "16/B374D848" (uppercase hex, no leading zeros)
            let d = DirectFunctionCall1Coll(pg_lsn_in, InvalidOid, CStringGetDatum(c"16/B374D848".as_ptr()));
            assert_eq!(DatumGetLSN(d), (0x16u64 << 32) | 0xB374D848);
            let s = DatumGetCString(DirectFunctionCall1Coll(pg_lsn_out, InvalidOid, d));
            assert!(cstr_eq(s, "16/B374D848"));

            // ordering
            let lo = DirectFunctionCall1Coll(pg_lsn_in, InvalidOid, CStringGetDatum(c"0/0".as_ptr()));
            let hi = DirectFunctionCall1Coll(pg_lsn_in, InvalidOid, CStringGetDatum(c"FF/FFFFFFFF".as_ptr()));
            assert!(DatumGetBool(DirectFunctionCall2Coll(pg_lsn_lt, InvalidOid, lo, hi)));
            assert_eq!(DatumGetInt32(DirectFunctionCall2Coll(pg_lsn_cmp, InvalidOid, lo, hi)), -1);
            assert!(DatumGetBool(DirectFunctionCall2Coll(pg_lsn_eq, InvalidOid, d, d)));
        }
    }

    #[test]
    #[should_panic]
    fn pg_lsn_in_rejects_garbage() {
        unsafe {
            DirectFunctionCall1Coll(pg_lsn_in, InvalidOid, CStringGetDatum(c"not-an-lsn".as_ptr()));
        }
    }
}
