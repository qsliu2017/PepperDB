//! Translation of postgres/src/backend/utils/adt/mac.c
//!                (+ the `macaddr` struct from postgres/src/include/utils/inet.h merged in)
//!
//! PostgreSQL type definitions for 6 byte, EUI-48, MAC addresses (the "macaddr"
//! type): a fixed-length, pass-by-reference 6-byte value.
//!
//! Portions Copyright (c) 1998-2025, PostgreSQL Global Development Group
//!
//! `#include`s mapped: common/hashfn.h -> crate::common::hashfn (hash_any/hash_any_extended);
//! utils/inet.h -> the `macaddr` struct + DatumGetMacaddrP/MacaddrPGetDatum helpers, merged
//! below.  <stdio.h> snprintf bound via extern "C"; the sscanf-based parser in macaddr_in is
//! replicated with a manual hex-octet parser (the 7 accepted notations are reproduced exactly).
//!
//! `#include`s further mapped: libpq/pqformat -> crate::libpq::pqformat (pq_getmsgbyte /
//! pq_begintypsend / pq_endtypsend; pq_sendbyte is a local shim over pq_sendint8);
//! port/pg_bswap -> crate::port::pg_bswap (DatumBigEndianToNative); lib/hyperloglog ->
//! crate::lib::hyperloglog; utils/sortsupport.h -> crate::utils::sort::sortsupport
//! (SortSupport) + ssup_datum_unsigned_cmp from crate::utils::sort::tuplesort.  trace_sort
//! GUC is a local stub (it lives behind an extern block in guc_tables).

use crate::prelude::*;
use crate::utils::fmgr::*;
use crate::{
    PG_GETARG_CSTRING, PG_GETARG_DATUM, PG_GETARG_INT64, PG_GETARG_POINTER, PG_RETURN_BOOL,
    PG_RETURN_CSTRING, PG_RETURN_INT32, PG_RETURN_VOID,
};
use crate::c::{uint32, uint64};
use crate::common::hashfn::{hash_any, hash_any_extended, hash_uint32};
use crate::port::pg_bswap::DatumBigEndianToNative;
use crate::postgres::{DatumGetPointer, DatumGetUInt32, PointerGetDatum};
use crate::nodes::nodes::Node;
use crate::utils::mmgr::memnodes::MemoryContext;
use crate::utils::mmgr::mcxt::MemoryContextSwitchTo;
use crate::lib::stringinfo::{StringInfo, StringInfoData};
use crate::lib::hyperloglog::{
    addHyperLogLog, estimateHyperLogLog, hyperLogLogState, initHyperLogLog,
};
use crate::libpq::pqformat::{pq_begintypsend, pq_endtypsend, pq_getmsgbyte};
use crate::utils::sort::sortsupport::{SortSupport, SortSupportData};
use crate::utils::sort::tuplesort::ssup_datum_unsigned_cmp;
use core::ffi::{c_char, c_int, c_void};

// TODO(pg-port): trace_sort GUC lives in utils/misc/guc_tables (extern block).
static mut trace_sort: bool = false;

/* sortsupport for macaddr */
struct macaddr_sortsupport_state {
    input_count: i64,        /* number of non-null values seen */
    estimating: bool,        /* true if estimating cardinality */
    abbr_card: hyperLogLogState, /* cardinality estimator */
}

// ---- utils/inet.h: the macaddr internal storage format ----
/*
 *	This is the internal storage format for MAC addresses:
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

/* fmgr interface helpers (inet.h): macaddr is a fixed-length pass-by-reference datatype */
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
    fn snprintf(s: *mut c_char, n: usize, fmt: *const c_char, ...) -> c_int;
}

/* errcodes.h classification (errcode() shim ignores the value). */
const ERRCODE_INVALID_TEXT_REPRESENTATION: c_int = 0;
const ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE: c_int = 0;

/*
 *	Utility macros used for sorting and comparing:
 *
 *	#define hibits(addr) ((unsigned long)(((addr)->a<<16)|((addr)->b<<8)|((addr)->c)))
 *	#define lobits(addr) ((unsigned long)(((addr)->d<<16)|((addr)->e<<8)|((addr)->f)))
 *
 * The C operands are `unsigned char` promoted to `int` for the shifts; the
 * result fits in 24 bits so it cannot overflow.  We compute in u32, matching
 * the `unsigned long` cast used for the comparisons in macaddr_cmp_internal.
 */
#[inline]
unsafe fn hibits(addr: *const macaddr) -> u32 {
    (((*addr).a as u32) << 16) | (((*addr).b as u32) << 8) | ((*addr).c as u32)
}
#[inline]
unsafe fn lobits(addr: *const macaddr) -> u32 {
    (((*addr).d as u32) << 16) | (((*addr).e as u32) << 8) | ((*addr).f as u32)
}

/*
 *	MAC address reader.  Accepts several common notations.
 */
pub unsafe fn macaddr_in(fcinfo: FunctionCallInfo) -> Datum {
    let str_: *const c_char = PG_GETARG_CSTRING!(fcinfo, 0);
    let escontext: *mut Node = (*fcinfo).context;
    let result: *mut macaddr;
    /*
     * The C code declares a,b,..,f as `int` and fills them via sscanf("%x"/"%2x"),
     * with a trailing "%1s" (junk) that detects trailing non-whitespace garbage.
     * We mirror that with i32 octet slots and a manual parser that reproduces
     * sscanf's behavior for the seven accepted notations.
     */
    let a: c_int;
    let b: c_int;
    let c: c_int;
    let d: c_int;
    let e: c_int;
    let f: c_int;

    /* %1s matches iff there is trailing non-whitespace garbage */

    let mut octets = [0i32; 6];
    let mut count: c_int;

    /*
     * The original code tries each format in turn via sscanf, and `count` is the
     * number of successfully-converted fields (which is forced to != 6 if any
     * trailing "%1s" garbage was consumed).  scan_macaddr returns Some(6) when a
     * format matches with no trailing garbage, else None (analogous to count != 6).
     */
    count = match scan_macaddr(str_, MacFmt::ColonWide, &mut octets) {
        Some(n) => n,                 /* "%x:%x:%x:%x:%x:%x%1s" */
        None => -1,
    };
    if count != 6 {
        count = scan_macaddr(str_, MacFmt::DashWide, &mut octets).unwrap_or(-1); /* "%x-%x-%x-%x-%x-%x%1s" */
    }
    if count != 6 {
        count = scan_macaddr(str_, MacFmt::Pair3ColonPair3, &mut octets).unwrap_or(-1); /* "%2x%2x%2x:%2x%2x%2x%1s" */
    }
    if count != 6 {
        count = scan_macaddr(str_, MacFmt::Pair3DashPair3, &mut octets).unwrap_or(-1); /* "%2x%2x%2x-%2x%2x%2x%1s" */
    }
    if count != 6 {
        count = scan_macaddr(str_, MacFmt::DotGroups, &mut octets).unwrap_or(-1); /* "%2x%2x.%2x%2x.%2x%2x%1s" */
    }
    if count != 6 {
        count = scan_macaddr(str_, MacFmt::DashGroups, &mut octets).unwrap_or(-1); /* "%2x%2x-%2x%2x-%2x%2x%1s" */
    }
    if count != 6 {
        count = scan_macaddr(str_, MacFmt::Bare, &mut octets).unwrap_or(-1); /* "%2x%2x%2x%2x%2x%2x%1s" */
    }
    if count != 6 {
        let _ = errcode(ERRCODE_INVALID_TEXT_REPRESENTATION);
        // ereturn(escontext, (Datum) 0, ...) -- soft error context not yet ported.
        let _ = escontext;
        ereport!(
            ERROR,
            errmsg!(
                "invalid input syntax for type {}: \"{}\"",
                "macaddr",
                cstr(str_)
            )
        );
    }

    a = octets[0];
    b = octets[1];
    c = octets[2];
    d = octets[3];
    e = octets[4];
    f = octets[5];

    if (a < 0) || (a > 255) || (b < 0) || (b > 255) ||
        (c < 0) || (c > 255) || (d < 0) || (d > 255) ||
        (e < 0) || (e > 255) || (f < 0) || (f > 255)
    {
        let _ = errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
        let _ = escontext;
        ereport!(
            ERROR,
            errmsg!("invalid octet value in \"macaddr\" value: \"{}\"", cstr(str_))
        );
    }

    result = palloc(core::mem::size_of::<macaddr>()) as *mut macaddr;

    (*result).a = a as u8;
    (*result).b = b as u8;
    (*result).c = c as u8;
    (*result).d = d as u8;
    (*result).e = e as u8;
    (*result).f = f as u8;

    return MacaddrPGetDatum(result); // PG_RETURN_MACADDR_P
}

/*
 * The seven sscanf format strings accepted by macaddr_in, as an enum so the
 * single manual parser below can dispatch on the separator layout.
 */
#[derive(Clone, Copy)]
enum MacFmt {
    ColonWide,      /* %x:%x:%x:%x:%x:%x   - up to int-width hex per octet, ':' separated */
    DashWide,       /* %x-%x-%x-%x-%x-%x   - up to int-width hex per octet, '-' separated */
    Pair3ColonPair3, /* %2x%2x%2x:%2x%2x%2x - two halves of 3 packed octets, ':' between */
    Pair3DashPair3, /* %2x%2x%2x-%2x%2x%2x */
    DotGroups,      /* %2x%2x.%2x%2x.%2x%2x - three groups of 2 packed octets, '.' between */
    DashGroups,     /* %2x%2x-%2x%2x-%2x%2x - three groups of 2 packed octets, '-' between */
    Bare,           /* %2x%2x%2x%2x%2x%2x   - 12 packed hex digits, no separators */
}

/*
 * Replicate `sscanf(str, <fmt>, &a..&f, junk)` for a given MacFmt, returning
 * Some(6) iff all six octets were converted and no trailing non-whitespace
 * garbage remained (i.e. the C `count == 6` outcome), else None.
 *
 * sscanf semantics reproduced here:
 *  - "%x"  consumes a maximal run of hex digits (after optional leading
 *    whitespace and an optional sign) and stores into an int.
 *  - "%2x" consumes at most two hex digits.
 *  - a literal separator in the format must match that exact character.
 *  - "%1s" at the end matches any single non-whitespace char; if it matches,
 *    the conversion count exceeds 6, so the caller treats it as a failure.
 *  - trailing whitespace in the input does not constitute garbage for "%1s".
 *
 * # Safety
 * `str_` must be a valid NUL-terminated C string; `out` receives the 6 octets.
 */
unsafe fn scan_macaddr(str_: *const c_char, fmt: MacFmt, out: &mut [i32; 6]) -> Option<c_int> {
    let mut p = str_;

    // separators: between octets 0..1, 1..2, 2..3, 3..4, 4..5.  None means "no
    // separator" (octets are packed); Some(ch) means that exact char is required.
    // `wide` selects %x (greedy, signed) vs %2x (<=2 hex digits) field reads.
    let (seps, wide): ([Option<u8>; 5], bool) = match fmt {
        MacFmt::ColonWide => ([Some(b':'); 5], true),
        MacFmt::DashWide => ([Some(b'-'); 5], true),
        MacFmt::Pair3ColonPair3 => ([None, None, Some(b':'), None, None], false),
        MacFmt::Pair3DashPair3 => ([None, None, Some(b'-'), None, None], false),
        MacFmt::DotGroups => ([None, Some(b'.'), None, Some(b'.'), None], false),
        MacFmt::DashGroups => ([None, Some(b'-'), None, Some(b'-'), None], false),
        MacFmt::Bare => ([None; 5], false),
    };

    let mut i = 0usize;
    while i < 6 {
        let v = if wide {
            scan_hex_wide(&mut p)
        } else {
            scan_hex_n(&mut p, 2)
        };
        match v {
            Some(val) => out[i] = val,
            // A field conversion failed (count < 6 in C); report failure.
            None => return None,
        }
        if i < 5 {
            if let Some(sep) = seps[i] {
                if *p as u8 != sep {
                    return None;
                }
                p = p.add(1);
            }
        }
        i += 1;
    }

    /*
     * "%1s": skip any whitespace, then if a non-NUL character remains it would be
     * consumed by %1s, making the C `count` 7 (treated as != 6 -> failure).
     */
    while is_space(*p as u8) {
        p = p.add(1);
    }
    if *p as u8 != 0 {
        return None; /* trailing garbage matched %1s */
    }

    Some(6)
}

/*
 * scan_hex_wide: emulate scanf "%x" -- skip leading whitespace, accept an
 * optional '+'/'-' sign, then consume a maximal run of hex digits, storing the
 * value into an int (wrapping like C's int conversion).  Returns None if no hex
 * digit is present (a failed conversion).
 */
unsafe fn scan_hex_wide(p: &mut *const c_char) -> Option<c_int> {
    while is_space(**p as u8) {
        *p = p.add(1);
    }
    let mut neg = false;
    let ch = **p as u8;
    if ch == b'+' || ch == b'-' {
        neg = ch == b'-';
        *p = p.add(1);
    }
    if hexval(**p as u8).is_none() {
        return None;
    }
    let mut acc: u32 = 0;
    while let Some(h) = hexval(**p as u8) {
        acc = acc.wrapping_mul(16).wrapping_add(h as u32);
        *p = p.add(1);
    }
    let val = acc as c_int;
    Some(if neg { val.wrapping_neg() } else { val })
}

/*
 * scan_hex_n: emulate scanf "%<n>x" -- skip leading whitespace and optional
 * sign, then consume at most `n` hex digits.  Returns None if no hex digit is
 * present.  (sscanf does not require the full field width to be present.)
 */
unsafe fn scan_hex_n(p: &mut *const c_char, n: usize) -> Option<c_int> {
    while is_space(**p as u8) {
        *p = p.add(1);
    }
    let mut neg = false;
    let ch = **p as u8;
    if ch == b'+' || ch == b'-' {
        neg = ch == b'-';
        *p = p.add(1);
    }
    if hexval(**p as u8).is_none() {
        return None;
    }
    let mut acc: u32 = 0;
    let mut taken = 0usize;
    while taken < n {
        match hexval(**p as u8) {
            Some(h) => {
                acc = acc.wrapping_mul(16).wrapping_add(h as u32);
                *p = p.add(1);
                taken += 1;
            }
            None => break,
        }
    }
    let val = acc as c_int;
    Some(if neg { val.wrapping_neg() } else { val })
}

/* hex digit value, mirroring isxdigit + conversion */
#[inline]
fn hexval(ch: u8) -> Option<u8> {
    match ch {
        b'0'..=b'9' => Some(ch - b'0'),
        b'a'..=b'f' => Some(ch - b'a' + 10),
        b'A'..=b'F' => Some(ch - b'A' + 10),
        _ => None,
    }
}

/* C isspace() for the "C" locale (what scanf whitespace skipping uses) */
#[inline]
fn is_space(ch: u8) -> bool {
    matches!(ch, b' ' | b'\t' | b'\n' | b'\x0b' | b'\x0c' | b'\r')
}

/*
 *	MAC address output function.  Fixed format.
 */
pub unsafe fn macaddr_out(fcinfo: FunctionCallInfo) -> Datum {
    let addr: *mut macaddr = DatumGetMacaddrP(PG_GETARG_DATUM!(fcinfo, 0));
    let result: *mut c_char;

    result = palloc(32) as *mut c_char;

    snprintf(
        result,
        32,
        c"%02x:%02x:%02x:%02x:%02x:%02x".as_ptr(),
        (*addr).a as c_int,
        (*addr).b as c_int,
        (*addr).c as c_int,
        (*addr).d as c_int,
        (*addr).e as c_int,
        (*addr).f as c_int,
    );

    PG_RETURN_CSTRING!(result);
}

/*
 *		macaddr_recv			- converts external binary format to macaddr
 *
 * The external representation is just the six bytes, MSB first.
 */
pub unsafe fn macaddr_recv(fcinfo: FunctionCallInfo) -> Datum {
    let buf: StringInfo = PG_GETARG_POINTER!(fcinfo, 0) as StringInfo;
    let addr: *mut macaddr;

    addr = palloc(core::mem::size_of::<macaddr>()) as *mut macaddr;

    (*addr).a = pq_getmsgbyte(buf) as u8;
    (*addr).b = pq_getmsgbyte(buf) as u8;
    (*addr).c = pq_getmsgbyte(buf) as u8;
    (*addr).d = pq_getmsgbyte(buf) as u8;
    (*addr).e = pq_getmsgbyte(buf) as u8;
    (*addr).f = pq_getmsgbyte(buf) as u8;

    return MacaddrPGetDatum(addr); // PG_RETURN_MACADDR_P
}

/*
 *		macaddr_send			- converts macaddr to binary format
 */
pub unsafe fn macaddr_send(fcinfo: FunctionCallInfo) -> Datum {
    let addr: *mut macaddr = DatumGetMacaddrP(PG_GETARG_DATUM!(fcinfo, 0));
    let mut buf: StringInfoData = core::mem::zeroed();

    pq_begintypsend(&mut buf);
    pq_sendbyte(&mut buf, (*addr).a);
    pq_sendbyte(&mut buf, (*addr).b);
    pq_sendbyte(&mut buf, (*addr).c);
    pq_sendbyte(&mut buf, (*addr).d);
    pq_sendbyte(&mut buf, (*addr).e);
    pq_sendbyte(&mut buf, (*addr).f);
    return PointerGetDatum(pq_endtypsend(&mut buf) as *const c_void); // PG_RETURN_BYTEA_P
}

// pq_sendbyte(buf, byt); the Rust pqformat exports pq_sendint8.
#[inline]
unsafe fn pq_sendbyte(buf: StringInfo, byt: u8) {
    crate::libpq::pqformat::pq_sendint8(buf, byt);
}

/*
 *	Comparison function for sorting:
 */
unsafe fn macaddr_cmp_internal(a1: *const macaddr, a2: *const macaddr) -> c_int {
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

pub unsafe fn macaddr_cmp(fcinfo: FunctionCallInfo) -> Datum {
    let a1 = DatumGetMacaddrP(PG_GETARG_DATUM!(fcinfo, 0));
    let a2 = DatumGetMacaddrP(PG_GETARG_DATUM!(fcinfo, 1));

    PG_RETURN_INT32!(macaddr_cmp_internal(a1, a2));
}

/*
 *	Boolean comparisons.
 */
pub unsafe fn macaddr_lt(fcinfo: FunctionCallInfo) -> Datum {
    let a1 = DatumGetMacaddrP(PG_GETARG_DATUM!(fcinfo, 0));
    let a2 = DatumGetMacaddrP(PG_GETARG_DATUM!(fcinfo, 1));
    PG_RETURN_BOOL!(macaddr_cmp_internal(a1, a2) < 0);
}

pub unsafe fn macaddr_le(fcinfo: FunctionCallInfo) -> Datum {
    let a1 = DatumGetMacaddrP(PG_GETARG_DATUM!(fcinfo, 0));
    let a2 = DatumGetMacaddrP(PG_GETARG_DATUM!(fcinfo, 1));
    PG_RETURN_BOOL!(macaddr_cmp_internal(a1, a2) <= 0);
}

pub unsafe fn macaddr_eq(fcinfo: FunctionCallInfo) -> Datum {
    let a1 = DatumGetMacaddrP(PG_GETARG_DATUM!(fcinfo, 0));
    let a2 = DatumGetMacaddrP(PG_GETARG_DATUM!(fcinfo, 1));
    PG_RETURN_BOOL!(macaddr_cmp_internal(a1, a2) == 0);
}

pub unsafe fn macaddr_ge(fcinfo: FunctionCallInfo) -> Datum {
    let a1 = DatumGetMacaddrP(PG_GETARG_DATUM!(fcinfo, 0));
    let a2 = DatumGetMacaddrP(PG_GETARG_DATUM!(fcinfo, 1));
    PG_RETURN_BOOL!(macaddr_cmp_internal(a1, a2) >= 0);
}

pub unsafe fn macaddr_gt(fcinfo: FunctionCallInfo) -> Datum {
    let a1 = DatumGetMacaddrP(PG_GETARG_DATUM!(fcinfo, 0));
    let a2 = DatumGetMacaddrP(PG_GETARG_DATUM!(fcinfo, 1));
    PG_RETURN_BOOL!(macaddr_cmp_internal(a1, a2) > 0);
}

pub unsafe fn macaddr_ne(fcinfo: FunctionCallInfo) -> Datum {
    let a1 = DatumGetMacaddrP(PG_GETARG_DATUM!(fcinfo, 0));
    let a2 = DatumGetMacaddrP(PG_GETARG_DATUM!(fcinfo, 1));
    PG_RETURN_BOOL!(macaddr_cmp_internal(a1, a2) != 0);
}

/*
 * Support function for hash indexes on macaddr.
 */
pub unsafe fn hashmacaddr(fcinfo: FunctionCallInfo) -> Datum {
    let key = DatumGetMacaddrP(PG_GETARG_DATUM!(fcinfo, 0));

    hash_any(key as *const core::ffi::c_uchar, core::mem::size_of::<macaddr>() as c_int)
}

pub unsafe fn hashmacaddrextended(fcinfo: FunctionCallInfo) -> Datum {
    let key = DatumGetMacaddrP(PG_GETARG_DATUM!(fcinfo, 0));

    hash_any_extended(
        key as *const core::ffi::c_uchar,
        core::mem::size_of::<macaddr>() as c_int,
        PG_GETARG_INT64!(fcinfo, 1) as u64,
    )
}

/*
 * Arithmetic functions: bitwise NOT, AND, OR.
 */
pub unsafe fn macaddr_not(fcinfo: FunctionCallInfo) -> Datum {
    let addr = DatumGetMacaddrP(PG_GETARG_DATUM!(fcinfo, 0));
    let result: *mut macaddr;

    result = palloc(core::mem::size_of::<macaddr>()) as *mut macaddr;
    (*result).a = !(*addr).a;
    (*result).b = !(*addr).b;
    (*result).c = !(*addr).c;
    (*result).d = !(*addr).d;
    (*result).e = !(*addr).e;
    (*result).f = !(*addr).f;
    return MacaddrPGetDatum(result); // PG_RETURN_MACADDR_P
}

pub unsafe fn macaddr_and(fcinfo: FunctionCallInfo) -> Datum {
    let addr1 = DatumGetMacaddrP(PG_GETARG_DATUM!(fcinfo, 0));
    let addr2 = DatumGetMacaddrP(PG_GETARG_DATUM!(fcinfo, 1));
    let result: *mut macaddr;

    result = palloc(core::mem::size_of::<macaddr>()) as *mut macaddr;
    (*result).a = (*addr1).a & (*addr2).a;
    (*result).b = (*addr1).b & (*addr2).b;
    (*result).c = (*addr1).c & (*addr2).c;
    (*result).d = (*addr1).d & (*addr2).d;
    (*result).e = (*addr1).e & (*addr2).e;
    (*result).f = (*addr1).f & (*addr2).f;
    return MacaddrPGetDatum(result); // PG_RETURN_MACADDR_P
}

pub unsafe fn macaddr_or(fcinfo: FunctionCallInfo) -> Datum {
    let addr1 = DatumGetMacaddrP(PG_GETARG_DATUM!(fcinfo, 0));
    let addr2 = DatumGetMacaddrP(PG_GETARG_DATUM!(fcinfo, 1));
    let result: *mut macaddr;

    result = palloc(core::mem::size_of::<macaddr>()) as *mut macaddr;
    (*result).a = (*addr1).a | (*addr2).a;
    (*result).b = (*addr1).b | (*addr2).b;
    (*result).c = (*addr1).c | (*addr2).c;
    (*result).d = (*addr1).d | (*addr2).d;
    (*result).e = (*addr1).e | (*addr2).e;
    (*result).f = (*addr1).f | (*addr2).f;
    return MacaddrPGetDatum(result); // PG_RETURN_MACADDR_P
}

/*
 *	Truncation function to allow comparing mac manufacturers.
 *	From suggestion by Alex Pilosov <alex@pilosoft.com>
 */
pub unsafe fn macaddr_trunc(fcinfo: FunctionCallInfo) -> Datum {
    let addr = DatumGetMacaddrP(PG_GETARG_DATUM!(fcinfo, 0));
    let result: *mut macaddr;

    result = palloc(core::mem::size_of::<macaddr>()) as *mut macaddr;

    (*result).a = (*addr).a;
    (*result).b = (*addr).b;
    (*result).c = (*addr).c;
    (*result).d = 0;
    (*result).e = 0;
    (*result).f = 0;

    return MacaddrPGetDatum(result); // PG_RETURN_MACADDR_P
}

/*
 * SortSupport strategy function. Populates a SortSupport struct with the
 * information necessary to use comparison by abbreviated keys.
 */
pub unsafe fn macaddr_sortsupport(fcinfo: FunctionCallInfo) -> Datum {
    let ssup = PG_GETARG_POINTER!(fcinfo, 0) as SortSupport;

    (*ssup).comparator = Some(macaddr_fast_cmp);
    (*ssup).ssup_extra = null_mut();

    if (*ssup).abbreviate {
        let uss: *mut macaddr_sortsupport_state;
        let oldcontext: MemoryContext;

        oldcontext = MemoryContextSwitchTo((*ssup).ssup_cxt);

        uss = palloc(core::mem::size_of::<macaddr_sortsupport_state>())
            as *mut macaddr_sortsupport_state;
        (*uss).input_count = 0;
        (*uss).estimating = true;
        initHyperLogLog(&mut (*uss).abbr_card, 10);

        (*ssup).ssup_extra = uss as *mut c_void;

        (*ssup).comparator = Some(ssup_datum_unsigned_cmp);
        (*ssup).abbrev_converter = Some(macaddr_abbrev_convert);
        (*ssup).abbrev_abort = Some(macaddr_abbrev_abort);
        (*ssup).abbrev_full_comparator = Some(macaddr_fast_cmp);

        MemoryContextSwitchTo(oldcontext);
    }

    PG_RETURN_VOID!();
}

/*
 * SortSupport "traditional" comparison function. Pulls two MAC addresses from
 * the heap and runs a standard comparison on them.
 */
unsafe fn macaddr_fast_cmp(x: Datum, y: Datum, _ssup: SortSupport) -> c_int {
    let arg1 = DatumGetMacaddrP(x);
    let arg2 = DatumGetMacaddrP(y);

    macaddr_cmp_internal(arg1, arg2)
}

/*
 * Callback for estimating effectiveness of abbreviated key optimization.
 *
 * We pay no attention to the cardinality of the non-abbreviated data, because
 * there is no equality fast-path within authoritative macaddr comparator.
 */
unsafe fn macaddr_abbrev_abort(memtupcount: c_int, ssup: SortSupport) -> bool {
    let uss = (*ssup).ssup_extra as *mut macaddr_sortsupport_state;
    let abbr_card: f64;

    if memtupcount < 10000 || (*uss).input_count < 10000 || !(*uss).estimating {
        return false;
    }

    abbr_card = estimateHyperLogLog(&mut (*uss).abbr_card);

    /*
     * If we have >100k distinct values, then even if we were sorting many
     * billion rows we'd likely still break even, and the penalty of undoing
     * that many rows of abbrevs would probably not be worth it. At this point
     * we stop counting because we know that we're now fully committed.
     */
    if abbr_card > 100000.0 {
        if trace_sort {
            elog!(
                LOG,
                "macaddr_abbrev: estimation ends at cardinality {} after {} values ({} rows)",
                abbr_card,
                (*uss).input_count,
                memtupcount
            );
        }
        (*uss).estimating = false;
        return false;
    }

    /*
     * Target minimum cardinality is 1 per ~2k of non-null inputs. 0.5 row
     * fudge factor allows us to abort earlier on genuinely pathological data
     * where we've had exactly one abbreviated value in the first 2k
     * (non-null) rows.
     */
    if abbr_card < (*uss).input_count as f64 / 2000.0 + 0.5 {
        if trace_sort {
            elog!(
                LOG,
                "macaddr_abbrev: aborting abbreviation at cardinality {} below threshold {} after {} values ({} rows)",
                abbr_card,
                (*uss).input_count as f64 / 2000.0 + 0.5,
                (*uss).input_count,
                memtupcount
            );
        }
        return true;
    }

    if trace_sort {
        elog!(
            LOG,
            "macaddr_abbrev: cardinality {} after {} values ({} rows)",
            abbr_card,
            (*uss).input_count,
            memtupcount
        );
    }

    false
}

/*
 * SortSupport conversion routine. Converts original macaddr representation
 * to abbreviated key representation.
 *
 * Packs the bytes of a 6-byte MAC address into a Datum and treats it as an
 * unsigned integer for purposes of comparison. On a 64-bit machine, there
 * will be two zeroed bytes of padding. The integer is converted to native
 * endianness to facilitate easy comparison.
 */
unsafe fn macaddr_abbrev_convert(original: Datum, ssup: SortSupport) -> Datum {
    let uss = (*ssup).ssup_extra as *mut macaddr_sortsupport_state;
    let authoritative = DatumGetMacaddrP(original);
    let mut res: Datum = 0;

    /*
     * On a 64-bit machine, zero out the 8-byte datum and copy the 6 bytes of
     * the MAC address in. There will be two bytes of zero padding on the end
     * of the least significant bits.
     */
    if core::mem::size_of::<Datum>() == 8 {
        res = 0;
        core::ptr::copy_nonoverlapping(
            authoritative as *const u8,
            &mut res as *mut Datum as *mut u8,
            core::mem::size_of::<macaddr>(),
        );
    } else {
        core::ptr::copy_nonoverlapping(
            authoritative as *const u8,
            &mut res as *mut Datum as *mut u8,
            core::mem::size_of::<Datum>(),
        );
    }
    (*uss).input_count += 1;

    /*
     * Cardinality estimation. The estimate uses uint32, so on a 64-bit
     * architecture, XOR the two 32-bit halves together to produce slightly
     * more entropy. The two zeroed bytes won't have any practical impact on
     * this operation.
     */
    if (*uss).estimating {
        let tmp: uint32;

        if core::mem::size_of::<Datum>() == 8 {
            tmp = (res as uint32) ^ ((res as uint64 >> 32) as uint32);
        } else {
            tmp = res as uint32;
        }

        addHyperLogLog(&mut (*uss).abbr_card, DatumGetUInt32(hash_uint32(tmp)));
    }

    /*
     * Byteswap on little-endian machines.
     *
     * This is needed so that ssup_datum_unsigned_cmp() (an unsigned integer
     * 3-way comparator) works correctly on all platforms. Without this, the
     * comparator would have to call memcmp() with a pair of pointers to the
     * first byte of each abbreviated key, which is slower.
     */
    res = DatumBigEndianToNative(res);

    res
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

    // parse a macaddr literal and return the Datum
    unsafe fn parse(s: &core::ffi::CStr) -> Datum {
        DirectFunctionCall1Coll(macaddr_in, InvalidOid, CStringGetDatum(s.as_ptr()))
    }

    #[test]
    fn macaddr_io_all_notations() {
        unsafe {
            // canonical colon-separated round trips to the fixed output format
            let d = parse(c"08:00:2b:01:02:03");
            let s = DatumGetCString(DirectFunctionCall1Coll(macaddr_out, InvalidOid, d));
            assert!(cstr_eq(s, "08:00:2b:01:02:03"));

            // all seven accepted notations parse to the same value
            let forms: &[&core::ffi::CStr] = &[
                c"08:00:2b:01:02:03",   // %x:%x:...
                c"08-00-2b-01-02-03",   // %x-%x-...
                c"08002b:010203",       // %2x%2x%2x:%2x%2x%2x
                c"08002b-010203",       // %2x%2x%2x-%2x%2x%2x
                c"0800.2b01.0203",      // %2x%2x.%2x%2x.%2x%2x
                c"0800-2b01-0203",      // %2x%2x-%2x%2x-%2x%2x
                c"08002b010203",        // %2x%2x%2x%2x%2x%2x
            ];
            for form in forms {
                let v = parse(form);
                assert!(
                    DatumGetBool(DirectFunctionCall2Coll(macaddr_eq, InvalidOid, d, v)),
                    "form {:?} did not parse to canonical value",
                    form
                );
            }

            // uppercase hex and a single-digit octet (%x is greedy/lenient)
            let up = parse(c"8:0:2B:1:2:3");
            assert!(DatumGetBool(DirectFunctionCall2Coll(macaddr_eq, InvalidOid, d, up)));
        }
    }

    #[test]
    fn macaddr_compare_and_ops() {
        unsafe {
            let lo = parse(c"00:00:00:00:00:00");
            let hi = parse(c"ff:ff:ff:ff:ff:ff");
            let mid = parse(c"08:00:2b:01:02:03");

            assert!(DatumGetBool(DirectFunctionCall2Coll(macaddr_lt, InvalidOid, lo, hi)));
            assert!(DatumGetBool(DirectFunctionCall2Coll(macaddr_le, InvalidOid, lo, lo)));
            assert!(DatumGetBool(DirectFunctionCall2Coll(macaddr_gt, InvalidOid, hi, lo)));
            assert!(DatumGetBool(DirectFunctionCall2Coll(macaddr_ge, InvalidOid, hi, hi)));
            assert!(DatumGetBool(DirectFunctionCall2Coll(macaddr_ne, InvalidOid, lo, hi)));
            assert!(DatumGetInt32(DirectFunctionCall2Coll(macaddr_cmp, InvalidOid, lo, hi)) < 0);
            assert!(DatumGetInt32(DirectFunctionCall2Coll(macaddr_cmp, InvalidOid, hi, lo)) > 0);
            assert_eq!(
                DatumGetInt32(DirectFunctionCall2Coll(macaddr_cmp, InvalidOid, mid, mid)),
                0
            );

            // NOT
            let notlo = DirectFunctionCall1Coll(macaddr_not, InvalidOid, lo);
            assert!(DatumGetBool(DirectFunctionCall2Coll(macaddr_eq, InvalidOid, notlo, hi)));

            // AND / OR with all-ones is identity / saturating
            let andmid = DirectFunctionCall2Coll(macaddr_and, InvalidOid, mid, hi);
            assert!(DatumGetBool(DirectFunctionCall2Coll(macaddr_eq, InvalidOid, andmid, mid)));
            let ormid = DirectFunctionCall2Coll(macaddr_or, InvalidOid, mid, lo);
            assert!(DatumGetBool(DirectFunctionCall2Coll(macaddr_eq, InvalidOid, ormid, mid)));

            // trunc zeroes the last three octets
            let t = DirectFunctionCall1Coll(macaddr_trunc, InvalidOid, mid);
            let ts = DatumGetCString(DirectFunctionCall1Coll(macaddr_out, InvalidOid, t));
            assert!(cstr_eq(ts, "08:00:2b:00:00:00"));

            // hash of equal values is equal; differs for different values
            let h_mid = DatumGetInt32(hashmacaddr(make_fcinfo1(mid)));
            let h_mid2 = DatumGetInt32(hashmacaddr(make_fcinfo1(parse(c"08:00:2b:01:02:03"))));
            assert_eq!(h_mid, h_mid2);
            let h_hi = DatumGetInt32(hashmacaddr(make_fcinfo1(hi)));
            assert_ne!(h_mid, h_hi);
        }
    }

    // Build a heap-allocated 1-arg fcinfo for calling hashmacaddr directly.
    // (hashmacaddr returns via plain `return hash_any(...)`, never sets isnull,
    // so we cannot route it through DirectFunctionCall1Coll's null check cleanly;
    // a populated arg slot is all it needs.)  The palloc'd buffer outlives the
    // call within the test.
    unsafe fn make_fcinfo1(arg0: Datum) -> FunctionCallInfo {
        let size = crate::utils::fmgr::SizeForFunctionCallInfo(1);
        let raw = palloc(size) as *mut FunctionCallInfoBaseData;
        (*raw).flinfo = null_mut();
        (*raw).context = null_mut();
        (*raw).resultinfo = null_mut();
        (*raw).fncollation = InvalidOid;
        (*raw).isnull = false;
        (*raw).nargs = 1;
        (*(*raw).args.as_mut_ptr().add(0)).value = arg0;
        (*(*raw).args.as_mut_ptr().add(0)).isnull = false;
        raw
    }

    #[test]
    #[should_panic]
    fn macaddr_in_rejects_garbage() {
        unsafe {
            parse(c"not-a-mac-address");
        }
    }

    #[test]
    #[should_panic]
    fn macaddr_in_rejects_trailing_junk() {
        unsafe {
            parse(c"08:00:2b:01:02:03:zz");
        }
    }
}
