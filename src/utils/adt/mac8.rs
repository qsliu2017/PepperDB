//! Translation of postgres/src/backend/utils/adt/mac8.c
//!                (+ the macaddr8 and macaddr structs from postgres/src/include/utils/inet.h merged in)
//!
//! PostgreSQL type definitions for 8 byte (EUI-64) MAC addresses.
//!
//! EUI-48 (6 byte) MAC addresses are accepted as input and are stored in
//! EUI-64 format, with the 4th and 5th bytes set to FF and FE, respectively.
//! Output is always in 8 byte (EUI-64) format.
//!
//! Portions Copyright (c) 1998-2025, PostgreSQL Global Development Group
//!
//! `#include`s mapped: common/hashfn.h -> crate::common::hashfn (hash_any/hash_any_extended);
//! utils/inet.h -> macaddr8 + macaddr structs + DatumGetMacaddr8P/Macaddr8PGetDatum /
//! DatumGetMacaddrP/MacaddrPGetDatum merged below.  <ctype.h> isspace + libc snprintf bound
//! via extern "C".  nodes/nodes.h Node used for fcinfo->context (escontext).
//!
//! STUBBED (deps not yet ported): macaddr8_recv / macaddr8_send (libpq/pqformat:
//! pq_getmsgbyte / pq_begintypsend / pq_sendbyte / pq_endtypsend, plus the StringInfo body).

use crate::prelude::*;
use crate::utils::fmgr::*;
use crate::{
    PG_GETARG_DATUM, PG_GETARG_INT64, PG_GETARG_POINTER, PG_RETURN_BOOL, PG_RETURN_CSTRING,
    PG_RETURN_INT32,
};
use crate::c::{int32, Size};
use crate::common::hashfn::{hash_any, hash_any_extended};
use crate::nodes::nodes::Node;
use crate::postgres::{DatumGetPointer, PointerGetDatum};
use core::ffi::{c_char, c_int, c_void};

// ---- utils/inet.h (merged) ----

/*
 *	This is the internal storage format for MAC8 addresses:
 */
#[repr(C)]
pub struct macaddr8 {
    pub a: u8,
    pub b: u8,
    pub c: u8,
    pub d: u8,
    pub e: u8,
    pub f: u8,
    pub g: u8,
    pub h: u8,
}

/*
 *	This is the internal storage format for MAC addresses.
 *
 * (Duplicated locally from utils/inet.h, as permitted by the porting notes:
 * macaddr8tomacaddr needs to construct a 6-byte macaddr.)
 */
#[repr(C)]
pub struct macaddr {
    pub a: u8,
    pub b: u8,
    pub c: u8,
    pub d: u8,
    pub e: u8,
    pub f: u8,
}

/* macaddr8 is a fixed-length pass-by-reference datatype (inet.h fmgr helpers) */
#[inline]
pub unsafe fn DatumGetMacaddr8P(X: Datum) -> *mut macaddr8 {
    DatumGetPointer(X) as *mut macaddr8
}
#[inline]
pub unsafe fn Macaddr8PGetDatum(X: *const macaddr8) -> Datum {
    PointerGetDatum(X as *const c_void)
}
// PG_GETARG_MACADDR8_P(n) == DatumGetMacaddr8P(PG_GETARG_DATUM!(fcinfo, n))
// PG_RETURN_MACADDR8_P(x) == return Macaddr8PGetDatum(x)

/* macaddr is a fixed-length pass-by-reference datatype (inet.h fmgr helpers) */
#[inline]
pub unsafe fn DatumGetMacaddrP(X: Datum) -> *mut macaddr {
    DatumGetPointer(X) as *mut macaddr
}
#[inline]
pub unsafe fn MacaddrPGetDatum(X: *const macaddr) -> Datum {
    PointerGetDatum(X as *const c_void)
}
// PG_GETARG_MACADDR_P(n) == DatumGetMacaddrP(PG_GETARG_DATUM!(fcinfo, n))
// PG_RETURN_MACADDR_P(x) == return MacaddrPGetDatum(x)

extern "C" {
    /* <ctype.h>: locale-aware whitespace test, called as isspace((unsigned char) *ptr). */
    fn isspace(ch: c_int) -> c_int;
    /* libc snprintf for the fixed 8-byte hex output, matching the C byte-for-byte. */
    fn snprintf(buf: *mut c_char, size: Size, fmt: *const c_char, ...) -> c_int;
}

/* errcodes.h classification (errcode() shim ignores the value). */
const ERRCODE_INVALID_TEXT_REPRESENTATION: c_int = 0;
const ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE: c_int = 0;

/*
 *	Utility macros used for sorting and comparing:
 *
 * C casts each field up to (unsigned long) before shifting; we use u32 (the
 * width the four bytes occupy) so the `<< 24` of byte `a` cannot overflow.
 */
#[inline]
unsafe fn hibits(addr: *const macaddr8) -> u32 {
    ((*addr).a as u32) << 24
        | ((*addr).b as u32) << 16
        | ((*addr).c as u32) << 8
        | ((*addr).d as u32)
}

#[inline]
unsafe fn lobits(addr: *const macaddr8) -> u32 {
    ((*addr).e as u32) << 24
        | ((*addr).f as u32) << 16
        | ((*addr).g as u32) << 8
        | ((*addr).h as u32)
}

const HEXLOOKUP: [i8; 128] = [
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, //
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, //
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, //
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, -1, -1, -1, -1, -1, -1, //
    -1, 10, 11, 12, 13, 14, 15, -1, -1, -1, -1, -1, -1, -1, -1, -1, //
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, //
    -1, 10, 11, 12, 13, 14, 15, -1, -1, -1, -1, -1, -1, -1, -1, -1, //
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, //
];

/*
 * hex2_to_uchar - convert 2 hex digits to a byte (unsigned char)
 *
 * Sets *badhex to true if the end of the string is reached ('\0' found), or if
 * either character is not a valid hex digit.
 *
 * # Safety
 * `ptr` must point to at least two readable bytes.
 */
#[inline]
unsafe fn hex2_to_uchar(ptr: *const u8, badhex: *mut bool) -> u8 {
    let ret: u8;
    let mut lookup: i8;

    /* Handle the first character */
    if *ptr > 127 {
        *badhex = true;
        return 0;
    }

    lookup = HEXLOOKUP[*ptr as usize];
    if lookup < 0 {
        *badhex = true;
        return 0;
    }

    ret = (lookup as u8) << 4;

    /* Move to the second character */
    let ptr = ptr.add(1);

    if *ptr > 127 {
        *badhex = true;
        return 0;
    }

    lookup = HEXLOOKUP[*ptr as usize];
    if lookup < 0 {
        *badhex = true;
        return 0;
    }

    ret.wrapping_add(lookup as u8)
}

/*
 * MAC address (EUI-48 and EUI-64) reader. Accepts several common notations.
 */
pub unsafe fn macaddr8_in(fcinfo: FunctionCallInfo) -> Datum {
    let str: *const u8 = PG_GETARG_DATUM!(fcinfo, 0) as *const u8; /* PG_GETARG_CSTRING(0) */
    let escontext: *mut Node = (*fcinfo).context;
    let mut ptr: *const u8 = str;
    let mut badhex: bool = false;
    let result: *mut macaddr8;
    let mut a: u8 = 0;
    let mut b: u8 = 0;
    let mut c: u8 = 0;
    let mut d: u8 = 0;
    let mut e: u8 = 0;
    let mut f: u8 = 0;
    let mut g: u8 = 0;
    let mut h: u8 = 0;
    let mut count: c_int = 0;
    let mut spacer: u8 = b'\0';

    /* skip leading spaces */
    while *ptr != 0 && isspace(*ptr as c_int) != 0 {
        ptr = ptr.add(1);
    }

    /* digits must always come in pairs */
    while *ptr != 0 && *ptr.add(1) != 0 {
        /*
         * Attempt to decode each byte, which must be 2 hex digits in a row.
         * If either digit is not hex, hex2_to_uchar will throw ereport() for
         * us.  Either 6 or 8 byte MAC addresses are supported.
         */

        /* Attempt to collect a byte */
        count += 1;

        match count {
            1 => a = hex2_to_uchar(ptr, &mut badhex),
            2 => b = hex2_to_uchar(ptr, &mut badhex),
            3 => c = hex2_to_uchar(ptr, &mut badhex),
            4 => d = hex2_to_uchar(ptr, &mut badhex),
            5 => e = hex2_to_uchar(ptr, &mut badhex),
            6 => f = hex2_to_uchar(ptr, &mut badhex),
            7 => g = hex2_to_uchar(ptr, &mut badhex),
            8 => h = hex2_to_uchar(ptr, &mut badhex),
            _ => {
                /* must be trailing garbage... */
                return macaddr8_in_fail(str, escontext);
            }
        }

        if badhex {
            return macaddr8_in_fail(str, escontext);
        }

        /* Move forward to where the next byte should be */
        ptr = ptr.add(2);

        /* Check for a spacer, these are valid, anything else is not */
        if *ptr == b':' || *ptr == b'-' || *ptr == b'.' {
            /* remember the spacer used, if it changes then it isn't valid */
            if spacer == b'\0' {
                spacer = *ptr;
            }
            /* Have to use the same spacer throughout */
            else if spacer != *ptr {
                return macaddr8_in_fail(str, escontext);
            }

            /* move past the spacer */
            ptr = ptr.add(1);
        }

        /* allow trailing whitespace after if we have 6 or 8 bytes */
        if count == 6 || count == 8 {
            if isspace(*ptr as c_int) != 0 {
                ptr = ptr.add(1);
                while *ptr != 0 && isspace(*ptr as c_int) != 0 {
                    ptr = ptr.add(1);
                }

                /* If we found a space and then non-space, it's invalid */
                if *ptr != 0 {
                    return macaddr8_in_fail(str, escontext);
                }
            }
        }
    }

    /* Convert a 6 byte MAC address to macaddr8 */
    if count == 6 {
        h = f;
        g = e;
        f = d;

        d = 0xFF;
        e = 0xFE;
    } else if count != 8 {
        return macaddr8_in_fail(str, escontext);
    }

    result = palloc0(core::mem::size_of::<macaddr8>()) as *mut macaddr8;

    (*result).a = a;
    (*result).b = b;
    (*result).c = c;
    (*result).d = d;
    (*result).e = e;
    (*result).f = f;
    (*result).g = g;
    (*result).h = h;

    return Macaddr8PGetDatum(result); /* PG_RETURN_MACADDR8_P */
}

// the `fail:` label of macaddr8_in (ereturn -> hard ERROR for now).
//
// In C this is `ereturn(escontext, (Datum) 0, ...)`; with soft-error contexts
// unsupported, this always raises a hard ERROR which diverges, so the (Datum) 0
// "return value" never materializes.
unsafe fn macaddr8_in_fail(str: *const u8, _escontext: *mut Node) -> Datum {
    let _ = errcode(ERRCODE_INVALID_TEXT_REPRESENTATION);
    ereport!(
        ERROR,
        errmsg!(
            "invalid input syntax for type {}: \"{}\"",
            "macaddr8",
            cstr(str as *const c_char)
        )
    );
    /* C: ereturn(escontext, (Datum) 0, ...). ereport! diverges (panics) above, so
     * this is dead code, but it satisfies the Datum return type. */
    #[allow(unreachable_code)]
    {
        0 as Datum
    }
}

/*
 * MAC8 address (EUI-64) output function. Fixed format.
 */
pub unsafe fn macaddr8_out(fcinfo: FunctionCallInfo) -> Datum {
    let addr: *mut macaddr8 = DatumGetMacaddr8P(PG_GETARG_DATUM!(fcinfo, 0));
    let result: *mut c_char;

    result = palloc(32) as *mut c_char;

    snprintf(
        result,
        32,
        c"%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x".as_ptr(),
        (*addr).a as c_int,
        (*addr).b as c_int,
        (*addr).c as c_int,
        (*addr).d as c_int,
        (*addr).e as c_int,
        (*addr).f as c_int,
        (*addr).g as c_int,
        (*addr).h as c_int,
    );

    PG_RETURN_CSTRING!(result);
}

/*
 * macaddr8_recv - converts external binary format(EUI-48 and EUI-64) to macaddr8
 *
 * The external representation is just the eight bytes, MSB first.
 */
pub unsafe fn macaddr8_recv(fcinfo: FunctionCallInfo) -> Datum {
    let buf = PG_GETARG_POINTER!(fcinfo, 0); /* (StringInfo) PG_GETARG_POINTER(0) */
    // C body:
    //   addr = (macaddr8 *) palloc0(sizeof(macaddr8));
    //   addr->a = pq_getmsgbyte(buf); addr->b = ...; addr->c = ...;
    //   if (buf->len == 6) { addr->d = 0xFF; addr->e = 0xFE; }
    //   else { addr->d = pq_getmsgbyte(buf); addr->e = pq_getmsgbyte(buf); }
    //   addr->f = ...; addr->g = ...; addr->h = ...;
    //   PG_RETURN_MACADDR8_P(addr);
    // TODO(pg-port): libpq/pqformat (pq_getmsgbyte) + StringInfo->len not yet translated.
    let _ = buf;
    unimplemented!("macaddr8_recv: libpq/pqformat (pq_getmsgbyte) not yet translated")
}

/*
 * macaddr8_send - converts macaddr8(EUI-64) to binary format
 */
pub unsafe fn macaddr8_send(fcinfo: FunctionCallInfo) -> Datum {
    let addr: *mut macaddr8 = DatumGetMacaddr8P(PG_GETARG_DATUM!(fcinfo, 0));
    // C body:
    //   pq_begintypsend(&buf);
    //   pq_sendbyte(&buf, addr->a .. addr->h);
    //   PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
    // TODO(pg-port): libpq/pqformat (pq_begintypsend/pq_sendbyte/pq_endtypsend) not yet translated.
    let _ = addr;
    unimplemented!("macaddr8_send: libpq/pqformat (pq_begintypsend/pq_sendbyte) not yet translated")
}

/*
 * macaddr8_cmp_internal - comparison function for sorting:
 */
unsafe fn macaddr8_cmp_internal(a1: *const macaddr8, a2: *const macaddr8) -> int32 {
    if hibits(a1) < hibits(a2) {
        -1
    } else if hibits(a1) > hibits(a2) {
        1
    } else if lobits(a1) < lobits(a2) {
        -1
    } else if lobits(a1) > lobits(a2) {
        1
    } else {
        0
    }
}

pub unsafe fn macaddr8_cmp(fcinfo: FunctionCallInfo) -> Datum {
    let a1 = DatumGetMacaddr8P(PG_GETARG_DATUM!(fcinfo, 0));
    let a2 = DatumGetMacaddr8P(PG_GETARG_DATUM!(fcinfo, 1));

    PG_RETURN_INT32!(macaddr8_cmp_internal(a1, a2));
}

/*
 * Boolean comparison functions.
 */

pub unsafe fn macaddr8_lt(fcinfo: FunctionCallInfo) -> Datum {
    let a1 = DatumGetMacaddr8P(PG_GETARG_DATUM!(fcinfo, 0));
    let a2 = DatumGetMacaddr8P(PG_GETARG_DATUM!(fcinfo, 1));

    PG_RETURN_BOOL!(macaddr8_cmp_internal(a1, a2) < 0);
}

pub unsafe fn macaddr8_le(fcinfo: FunctionCallInfo) -> Datum {
    let a1 = DatumGetMacaddr8P(PG_GETARG_DATUM!(fcinfo, 0));
    let a2 = DatumGetMacaddr8P(PG_GETARG_DATUM!(fcinfo, 1));

    PG_RETURN_BOOL!(macaddr8_cmp_internal(a1, a2) <= 0);
}

pub unsafe fn macaddr8_eq(fcinfo: FunctionCallInfo) -> Datum {
    let a1 = DatumGetMacaddr8P(PG_GETARG_DATUM!(fcinfo, 0));
    let a2 = DatumGetMacaddr8P(PG_GETARG_DATUM!(fcinfo, 1));

    PG_RETURN_BOOL!(macaddr8_cmp_internal(a1, a2) == 0);
}

pub unsafe fn macaddr8_ge(fcinfo: FunctionCallInfo) -> Datum {
    let a1 = DatumGetMacaddr8P(PG_GETARG_DATUM!(fcinfo, 0));
    let a2 = DatumGetMacaddr8P(PG_GETARG_DATUM!(fcinfo, 1));

    PG_RETURN_BOOL!(macaddr8_cmp_internal(a1, a2) >= 0);
}

pub unsafe fn macaddr8_gt(fcinfo: FunctionCallInfo) -> Datum {
    let a1 = DatumGetMacaddr8P(PG_GETARG_DATUM!(fcinfo, 0));
    let a2 = DatumGetMacaddr8P(PG_GETARG_DATUM!(fcinfo, 1));

    PG_RETURN_BOOL!(macaddr8_cmp_internal(a1, a2) > 0);
}

pub unsafe fn macaddr8_ne(fcinfo: FunctionCallInfo) -> Datum {
    let a1 = DatumGetMacaddr8P(PG_GETARG_DATUM!(fcinfo, 0));
    let a2 = DatumGetMacaddr8P(PG_GETARG_DATUM!(fcinfo, 1));

    PG_RETURN_BOOL!(macaddr8_cmp_internal(a1, a2) != 0);
}

/*
 * Support function for hash indexes on macaddr8.
 */
pub unsafe fn hashmacaddr8(fcinfo: FunctionCallInfo) -> Datum {
    let key = DatumGetMacaddr8P(PG_GETARG_DATUM!(fcinfo, 0));

    return hash_any(key as *const c_void as *const u8, core::mem::size_of::<macaddr8>() as c_int);
}

pub unsafe fn hashmacaddr8extended(fcinfo: FunctionCallInfo) -> Datum {
    let key = DatumGetMacaddr8P(PG_GETARG_DATUM!(fcinfo, 0));

    return hash_any_extended(
        key as *const c_void as *const u8,
        core::mem::size_of::<macaddr8>() as c_int,
        PG_GETARG_INT64!(fcinfo, 1) as u64,
    );
}

/*
 * Arithmetic functions: bitwise NOT, AND, OR.
 */
pub unsafe fn macaddr8_not(fcinfo: FunctionCallInfo) -> Datum {
    let addr = DatumGetMacaddr8P(PG_GETARG_DATUM!(fcinfo, 0));
    let result: *mut macaddr8;

    result = palloc0(core::mem::size_of::<macaddr8>()) as *mut macaddr8;
    (*result).a = !(*addr).a;
    (*result).b = !(*addr).b;
    (*result).c = !(*addr).c;
    (*result).d = !(*addr).d;
    (*result).e = !(*addr).e;
    (*result).f = !(*addr).f;
    (*result).g = !(*addr).g;
    (*result).h = !(*addr).h;

    return Macaddr8PGetDatum(result); /* PG_RETURN_MACADDR8_P */
}

pub unsafe fn macaddr8_and(fcinfo: FunctionCallInfo) -> Datum {
    let addr1 = DatumGetMacaddr8P(PG_GETARG_DATUM!(fcinfo, 0));
    let addr2 = DatumGetMacaddr8P(PG_GETARG_DATUM!(fcinfo, 1));
    let result: *mut macaddr8;

    result = palloc0(core::mem::size_of::<macaddr8>()) as *mut macaddr8;
    (*result).a = (*addr1).a & (*addr2).a;
    (*result).b = (*addr1).b & (*addr2).b;
    (*result).c = (*addr1).c & (*addr2).c;
    (*result).d = (*addr1).d & (*addr2).d;
    (*result).e = (*addr1).e & (*addr2).e;
    (*result).f = (*addr1).f & (*addr2).f;
    (*result).g = (*addr1).g & (*addr2).g;
    (*result).h = (*addr1).h & (*addr2).h;

    return Macaddr8PGetDatum(result); /* PG_RETURN_MACADDR8_P */
}

pub unsafe fn macaddr8_or(fcinfo: FunctionCallInfo) -> Datum {
    let addr1 = DatumGetMacaddr8P(PG_GETARG_DATUM!(fcinfo, 0));
    let addr2 = DatumGetMacaddr8P(PG_GETARG_DATUM!(fcinfo, 1));
    let result: *mut macaddr8;

    result = palloc0(core::mem::size_of::<macaddr8>()) as *mut macaddr8;
    (*result).a = (*addr1).a | (*addr2).a;
    (*result).b = (*addr1).b | (*addr2).b;
    (*result).c = (*addr1).c | (*addr2).c;
    (*result).d = (*addr1).d | (*addr2).d;
    (*result).e = (*addr1).e | (*addr2).e;
    (*result).f = (*addr1).f | (*addr2).f;
    (*result).g = (*addr1).g | (*addr2).g;
    (*result).h = (*addr1).h | (*addr2).h;

    return Macaddr8PGetDatum(result); /* PG_RETURN_MACADDR8_P */
}

/*
 * Truncation function to allow comparing macaddr8 manufacturers.
 */
pub unsafe fn macaddr8_trunc(fcinfo: FunctionCallInfo) -> Datum {
    let addr = DatumGetMacaddr8P(PG_GETARG_DATUM!(fcinfo, 0));
    let result: *mut macaddr8;

    result = palloc0(core::mem::size_of::<macaddr8>()) as *mut macaddr8;

    (*result).a = (*addr).a;
    (*result).b = (*addr).b;
    (*result).c = (*addr).c;
    (*result).d = 0;
    (*result).e = 0;
    (*result).f = 0;
    (*result).g = 0;
    (*result).h = 0;

    return Macaddr8PGetDatum(result); /* PG_RETURN_MACADDR8_P */
}

/*
 * Set 7th bit for modified EUI-64 as used in IPv6.
 */
pub unsafe fn macaddr8_set7bit(fcinfo: FunctionCallInfo) -> Datum {
    let addr = DatumGetMacaddr8P(PG_GETARG_DATUM!(fcinfo, 0));
    let result: *mut macaddr8;

    result = palloc0(core::mem::size_of::<macaddr8>()) as *mut macaddr8;

    (*result).a = (*addr).a | 0x02;
    (*result).b = (*addr).b;
    (*result).c = (*addr).c;
    (*result).d = (*addr).d;
    (*result).e = (*addr).e;
    (*result).f = (*addr).f;
    (*result).g = (*addr).g;
    (*result).h = (*addr).h;

    return Macaddr8PGetDatum(result); /* PG_RETURN_MACADDR8_P */
}

/*----------------------------------------------------------
 *	Conversion operators.
 *---------------------------------------------------------*/

pub unsafe fn macaddrtomacaddr8(fcinfo: FunctionCallInfo) -> Datum {
    let addr6 = DatumGetMacaddrP(PG_GETARG_DATUM!(fcinfo, 0)); /* PG_GETARG_MACADDR_P(0) */
    let result: *mut macaddr8;

    result = palloc0(core::mem::size_of::<macaddr8>()) as *mut macaddr8;

    (*result).a = (*addr6).a;
    (*result).b = (*addr6).b;
    (*result).c = (*addr6).c;
    (*result).d = 0xFF;
    (*result).e = 0xFE;
    (*result).f = (*addr6).d;
    (*result).g = (*addr6).e;
    (*result).h = (*addr6).f;

    return Macaddr8PGetDatum(result); /* PG_RETURN_MACADDR8_P */
}

pub unsafe fn macaddr8tomacaddr(fcinfo: FunctionCallInfo) -> Datum {
    let addr = DatumGetMacaddr8P(PG_GETARG_DATUM!(fcinfo, 0));
    let result: *mut macaddr;

    result = palloc0(core::mem::size_of::<macaddr>()) as *mut macaddr;

    if ((*addr).d != 0xFF) || ((*addr).e != 0xFE) {
        let _ = errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
        // C also attaches errhint("Only addresses that have FF and FE as values
        // in the 4th and 5th bytes from the left, for example
        // xx:xx:xx:ff:fe:xx:xx:xx, are eligible to be converted from macaddr8 to
        // macaddr."). The ereport! shim takes a single message, so the hint is
        // folded into the errmsg text.
        ereport!(
            ERROR,
            errmsg!(
                "macaddr8 data out of range to convert to macaddr. \
                 Only addresses that have FF and FE as values in the \
                 4th and 5th bytes from the left, for example \
                 xx:xx:xx:ff:fe:xx:xx:xx, are eligible to be converted \
                 from macaddr8 to macaddr."
            )
        );
    }

    (*result).a = (*addr).a;
    (*result).b = (*addr).b;
    (*result).c = (*addr).c;
    (*result).d = (*addr).f;
    (*result).e = (*addr).g;
    (*result).f = (*addr).h;

    return MacaddrPGetDatum(result); /* PG_RETURN_MACADDR_P */
}

/*
 * Format a C string for an error message via Rust `{}` (lossy).
 *
 * # Safety
 * `s` must be a valid NUL-terminated C string.
 */
unsafe fn cstr(s: *const c_char) -> std::string::String {
    let mut n = 0usize;
    while *s.add(n) != 0 {
        n += 1;
    }
    let bytes = core::slice::from_raw_parts(s as *const u8, n);
    std::string::String::from_utf8_lossy(bytes).into_owned()
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

    // round-trip a textual macaddr8 through in -> out
    unsafe fn roundtrip(input: &std::ffi::CStr) -> *mut c_char {
        let d = DirectFunctionCall1Coll(macaddr8_in, InvalidOid, CStringGetDatum(input.as_ptr()));
        DatumGetCString(DirectFunctionCall1Coll(macaddr8_out, InvalidOid, d))
    }

    #[test]
    fn macaddr8_io_eui64_and_eui48() {
        unsafe {
            // EUI-64 input, colon-separated, canonical lowercase out
            assert!(cstr_eq(
                roundtrip(c"08:00:2b:01:02:03:04:05"),
                "08:00:2b:01:02:03:04:05"
            ));

            // uppercase + dash separators accepted, normalized to lowercase colons
            assert!(cstr_eq(
                roundtrip(c"08-00-2B-01-02-03-04-05"),
                "08:00:2b:01:02:03:04:05"
            ));

            // dot separators accepted
            assert!(cstr_eq(
                roundtrip(c"08.00.2b.01.02.03.04.05"),
                "08:00:2b:01:02:03:04:05"
            ));

            // EUI-48 (6 bytes) is widened to EUI-64 with FF:FE inserted as bytes 4,5
            assert!(cstr_eq(roundtrip(c"08:00:2b:01:02:03"), "08:00:2b:ff:fe:01:02:03"));

            // leading/trailing whitespace tolerated
            assert!(cstr_eq(
                roundtrip(c"  08:00:2b:01:02:03:04:05  "),
                "08:00:2b:01:02:03:04:05"
            ));
        }
    }

    #[test]
    fn macaddr8_compare_and_hash() {
        unsafe {
            let lo = DirectFunctionCall1Coll(
                macaddr8_in,
                InvalidOid,
                CStringGetDatum(c"00:00:00:00:00:00:00:00".as_ptr()),
            );
            let hi = DirectFunctionCall1Coll(
                macaddr8_in,
                InvalidOid,
                CStringGetDatum(c"ff:ff:ff:ff:ff:ff:ff:ff".as_ptr()),
            );
            let lo2 = DirectFunctionCall1Coll(
                macaddr8_in,
                InvalidOid,
                CStringGetDatum(c"00:00:00:00:00:00:00:00".as_ptr()),
            );

            assert!(DatumGetBool(DirectFunctionCall2Coll(macaddr8_lt, InvalidOid, lo, hi)));
            assert!(DatumGetBool(DirectFunctionCall2Coll(macaddr8_le, InvalidOid, lo, hi)));
            assert!(DatumGetBool(DirectFunctionCall2Coll(macaddr8_gt, InvalidOid, hi, lo)));
            assert!(DatumGetBool(DirectFunctionCall2Coll(macaddr8_ge, InvalidOid, hi, lo)));
            assert!(DatumGetBool(DirectFunctionCall2Coll(macaddr8_ne, InvalidOid, lo, hi)));
            assert!(DatumGetBool(DirectFunctionCall2Coll(macaddr8_eq, InvalidOid, lo, lo2)));
            assert!(DatumGetInt32(DirectFunctionCall2Coll(macaddr8_cmp, InvalidOid, lo, hi)) < 0);
            assert_eq!(
                DatumGetInt32(DirectFunctionCall2Coll(macaddr8_cmp, InvalidOid, lo, lo2)),
                0
            );

            // hash of equal values must match
            let h1 = hashmacaddr8(make_fcinfo1(lo));
            let h2 = hashmacaddr8(make_fcinfo1(lo2));
            assert_eq!(h1, h2);
        }
    }

    #[test]
    fn macaddr8_bitwise_trunc_set7bit() {
        unsafe {
            let a = DirectFunctionCall1Coll(
                macaddr8_in,
                InvalidOid,
                CStringGetDatum(c"08:00:2b:01:02:03:04:05".as_ptr()),
            );

            // NOT then NOT == identity (verified via out string)
            let n = DirectFunctionCall1Coll(macaddr8_not, InvalidOid, a);
            let nn = DirectFunctionCall1Coll(macaddr8_not, InvalidOid, n);
            let s = DatumGetCString(DirectFunctionCall1Coll(macaddr8_out, InvalidOid, nn));
            assert!(cstr_eq(s, "08:00:2b:01:02:03:04:05"));

            // AND with all-ones == identity; OR with all-zeros == identity
            let ones = DirectFunctionCall1Coll(
                macaddr8_in,
                InvalidOid,
                CStringGetDatum(c"ff:ff:ff:ff:ff:ff:ff:ff".as_ptr()),
            );
            let zeros = DirectFunctionCall1Coll(
                macaddr8_in,
                InvalidOid,
                CStringGetDatum(c"00:00:00:00:00:00:00:00".as_ptr()),
            );
            let anded = DirectFunctionCall2Coll(macaddr8_and, InvalidOid, a, ones);
            assert!(cstr_eq(
                DatumGetCString(DirectFunctionCall1Coll(macaddr8_out, InvalidOid, anded)),
                "08:00:2b:01:02:03:04:05"
            ));
            let ored = DirectFunctionCall2Coll(macaddr8_or, InvalidOid, a, zeros);
            assert!(cstr_eq(
                DatumGetCString(DirectFunctionCall1Coll(macaddr8_out, InvalidOid, ored)),
                "08:00:2b:01:02:03:04:05"
            ));

            // trunc zeroes the trailing 5 bytes
            let t = DirectFunctionCall1Coll(macaddr8_trunc, InvalidOid, a);
            assert!(cstr_eq(
                DatumGetCString(DirectFunctionCall1Coll(macaddr8_out, InvalidOid, t)),
                "08:00:2b:00:00:00:00:00"
            ));

            // set7bit flips the universal/local bit (0x02 of the first byte)
            let s7 = DirectFunctionCall1Coll(macaddr8_set7bit, InvalidOid, a);
            assert!(cstr_eq(
                DatumGetCString(DirectFunctionCall1Coll(macaddr8_out, InvalidOid, s7)),
                "0a:00:2b:01:02:03:04:05"
            ));
        }
    }

    #[test]
    fn macaddr8_macaddr_casts() {
        unsafe {
            // macaddr -> macaddr8 widens with FF:FE in the middle.
            // Build a macaddr by hand (08:00:2b:01:02:03).
            let m6 = palloc0(core::mem::size_of::<macaddr>()) as *mut macaddr;
            (*m6).a = 0x08;
            (*m6).b = 0x00;
            (*m6).c = 0x2b;
            (*m6).d = 0x01;
            (*m6).e = 0x02;
            (*m6).f = 0x03;
            let d8 = DirectFunctionCall1Coll(macaddrtomacaddr8, InvalidOid, MacaddrPGetDatum(m6));
            assert!(cstr_eq(
                DatumGetCString(DirectFunctionCall1Coll(macaddr8_out, InvalidOid, d8)),
                "08:00:2b:ff:fe:01:02:03"
            ));

            // macaddr8 -> macaddr round-trips the eligible (FF:FE-derived) value.
            let back = DirectFunctionCall1Coll(macaddr8tomacaddr, InvalidOid, d8);
            let mb = DatumGetMacaddrP(back);
            assert_eq!(
                [(*mb).a, (*mb).b, (*mb).c, (*mb).d, (*mb).e, (*mb).f],
                [0x08, 0x00, 0x2b, 0x01, 0x02, 0x03]
            );
        }
    }

    #[test]
    #[should_panic]
    fn macaddr8_in_rejects_garbage() {
        unsafe {
            DirectFunctionCall1Coll(
                macaddr8_in,
                InvalidOid,
                CStringGetDatum(c"not-a-mac".as_ptr()),
            );
        }
    }

    #[test]
    #[should_panic]
    fn macaddr8tomacaddr_rejects_non_fffe() {
        unsafe {
            // EUI-64 not derived from EUI-48 (middle bytes are not FF:FE) is rejected.
            let a = DirectFunctionCall1Coll(
                macaddr8_in,
                InvalidOid,
                CStringGetDatum(c"08:00:2b:01:02:03:04:05".as_ptr()),
            );
            DirectFunctionCall1Coll(macaddr8tomacaddr, InvalidOid, a);
        }
    }

    // Build a 1-arg fcinfo for direct calls to hashmacaddr8 (which is not invoked
    // via DirectFunctionCall here because we want to compare two raw Datums).
    unsafe fn make_fcinfo1(arg: Datum) -> FunctionCallInfo {
        use crate::utils::fmgr::{FunctionCallInfoBaseData, SizeForFunctionCallInfo};
        let buf = palloc0(SizeForFunctionCallInfo(1)) as *mut FunctionCallInfoBaseData;
        (*buf).nargs = 1;
        (*(*buf).args.as_mut_ptr().add(0)).value = arg;
        (*(*buf).args.as_mut_ptr().add(0)).isnull = false;
        buf
    }
}
