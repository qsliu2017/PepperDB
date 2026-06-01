//! Translation of postgres/src/backend/utils/adt/uuid.c
//!                (+ postgres/src/include/utils/uuid.h merged in)
//!
//! The "uuid" ADT: a fixed 16-byte value (pg_uuid_t), pass-by-reference.
//!
//! Portions Copyright (c) 2007-2025, PostgreSQL Global Development Group
//!
//! `#include`s mapped: common/hashfn.h -> crate::common::hashfn (hash_any/hash_any_extended),
//! port/pg_strong_random -> crate::port::pg_strong_random.  <ctype.h> isxdigit + libc
//! strtoul/memcmp bound via extern "C".
//!
//! STUBBED (deps not yet ported): uuid_recv/uuid_send (libpq/pqformat); uuid_sortsupport +
//! uuid_fast_cmp/uuid_abbrev_abort/uuid_abbrev_convert (utils/sortsupport.h + lib/hyperloglog
//! abbreviation); uuid_skipsupport/uuid_increment/uuid_decrement (utils/skipsupport.h + Relation);
//! uuidv7/uuidv7_interval/generate_uuidv7/get_real_time_ns_ascending and uuid_extract_timestamp
//! (clock_gettime monotonic state + utils/timestamp.h TimestampTz/Interval).

use crate::prelude::*;
use crate::utils::fmgr::*;
use crate::{
    PG_GETARG_DATUM, PG_GETARG_INT64, PG_GETARG_POINTER, PG_RETURN_BOOL, PG_RETURN_CSTRING,
    PG_RETURN_INT32, PG_RETURN_NULL, PG_RETURN_POINTER, PG_RETURN_UINT16,
};
use crate::c::{int16, int32, uint16};
use crate::common::hashfn::{hash_any, hash_any_extended};
use crate::port::pg_strong_random::pg_strong_random;
use crate::postgres::{DatumGetPointer, PointerGetDatum};
use crate::nodes::nodes::Node;
use crate::lib::stringinfo::StringInfo;
use core::ffi::{c_char, c_int, c_ulong, c_void};

// ---- utils/uuid.h ----
/* uuid size in bytes */
pub const UUID_LEN: usize = 16;

#[repr(C)]
pub struct pg_uuid_t {
    pub data: [u8; UUID_LEN],
}

/* fmgr interface helpers (uuid.h) */
#[inline]
pub unsafe fn UUIDPGetDatum(x: *const pg_uuid_t) -> Datum {
    PointerGetDatum(x as *const c_void)
}
#[inline]
pub unsafe fn DatumGetUUIDP(x: Datum) -> *mut pg_uuid_t {
    DatumGetPointer(x) as *mut pg_uuid_t
}
// PG_GETARG_UUID_P(n) == DatumGetUUIDP(PG_GETARG_DATUM!(fcinfo, n))
// PG_RETURN_UUID_P(x) == return UUIDPGetDatum(x)

extern "C" {
    fn isxdigit(ch: c_int) -> c_int;
    fn strtoul(s: *const c_char, endptr: *mut *mut c_char, base: c_int) -> c_ulong;
    fn memcmp(a: *const c_void, b: *const c_void, n: usize) -> c_int;
}

/* errcodes.h classification (errcode() shim ignores the value). */
const ERRCODE_INVALID_TEXT_REPRESENTATION: c_int = 0;
const ERRCODE_INTERNAL_ERROR: c_int = 0;

pub unsafe fn uuid_in(fcinfo: FunctionCallInfo) -> Datum {
    let uuid_str: *mut c_char = PG_GETARG_DATUM!(fcinfo, 0) as *mut c_char; // PG_GETARG_CSTRING
    let uuid: *mut pg_uuid_t;

    uuid = palloc(core::mem::size_of::<pg_uuid_t>()) as *mut pg_uuid_t;
    string_to_uuid(uuid_str, uuid, (*fcinfo).context);
    return UUIDPGetDatum(uuid); // PG_RETURN_UUID_P
}

pub unsafe fn uuid_out(fcinfo: FunctionCallInfo) -> Datum {
    let uuid: *mut pg_uuid_t = DatumGetUUIDP(PG_GETARG_DATUM!(fcinfo, 0));
    const HEX_CHARS: &[u8; 16] = b"0123456789abcdef";
    let buf: *mut c_char;
    let mut p: *mut c_char;
    let mut i: usize;

    /* counts for the four hyphens and the zero-terminator */
    buf = palloc(2 * UUID_LEN + 5) as *mut c_char;
    p = buf;
    i = 0;
    while i < UUID_LEN {
        let hi: usize;
        let lo: usize;

        /* 8-4-4-4-12 grouping: add hyphens at the appropriate places */
        if i == 4 || i == 6 || i == 8 || i == 10 {
            *p = b'-' as c_char;
            p = p.add(1);
        }

        hi = ((*uuid).data[i] >> 4) as usize;
        lo = ((*uuid).data[i] & 0x0F) as usize;

        *p = HEX_CHARS[hi] as c_char;
        p = p.add(1);
        *p = HEX_CHARS[lo] as c_char;
        p = p.add(1);
        i += 1;
    }
    *p = b'\0' as c_char;

    PG_RETURN_CSTRING!(buf);
}

/*
 * We allow UUIDs as a series of 32 hexadecimal digits with an optional dash
 * after each group of 4 hexadecimal digits, and optionally surrounded by {}.
 *
 * # Safety
 * `source` is a NUL-terminated C string; `uuid` points to a writable pg_uuid_t.
 */
unsafe fn string_to_uuid(source: *const c_char, uuid: *mut pg_uuid_t, escontext: *mut Node) {
    let mut src = source;
    let mut braces = false;
    let mut i: usize;
    let _ = escontext; // TODO(pg-port): ErrorSaveContext soft errors

    if *src as u8 == b'{' {
        src = src.add(1);
        braces = true;
    }

    i = 0;
    while i < UUID_LEN {
        let mut str_buf = [0i8; 3];

        if *src as u8 == b'\0' || *src.add(1) as u8 == b'\0' {
            return string_to_uuid_syntax_error(source, escontext);
        }
        core::ptr::copy_nonoverlapping(src, str_buf.as_mut_ptr(), 2);
        if isxdigit(str_buf[0] as u8 as c_int) == 0 || isxdigit(str_buf[1] as u8 as c_int) == 0 {
            return string_to_uuid_syntax_error(source, escontext);
        }

        str_buf[2] = b'\0' as c_char;
        (*uuid).data[i] = strtoul(str_buf.as_ptr(), null_mut(), 16) as u8;
        src = src.add(2);
        if *src as u8 == b'-' && (i % 2) == 1 && i < UUID_LEN - 1 {
            src = src.add(1);
        }
        i += 1;
    }

    if braces {
        if *src as u8 != b'}' {
            return string_to_uuid_syntax_error(source, escontext);
        }
        src = src.add(1);
    }

    if *src as u8 != b'\0' {
        return string_to_uuid_syntax_error(source, escontext);
    }
}

// the `goto syntax_error` target of string_to_uuid (ereturn -> hard ERROR for now)
unsafe fn string_to_uuid_syntax_error(source: *const c_char, _escontext: *mut Node) {
    let _ = errcode(ERRCODE_INVALID_TEXT_REPRESENTATION);
    ereport!(
        ERROR,
        errmsg!(
            "invalid input syntax for type {}: \"{}\"",
            "uuid",
            cstr(source)
        )
    );
}

pub unsafe fn uuid_recv(fcinfo: FunctionCallInfo) -> Datum {
    let buffer: StringInfo = PG_GETARG_POINTER!(fcinfo, 0) as StringInfo;
    // C: memcpy(uuid->data, pq_getmsgbytes(buffer, UUID_LEN), UUID_LEN); PG_RETURN_POINTER(uuid);
    // TODO(pg-port): libpq/pqformat (pq_getmsgbytes) not yet translated.
    let _ = buffer;
    unimplemented!("uuid_recv: libpq/pqformat (pq_getmsgbytes) not yet translated")
}

pub unsafe fn uuid_send(fcinfo: FunctionCallInfo) -> Datum {
    let uuid: *mut pg_uuid_t = DatumGetUUIDP(PG_GETARG_DATUM!(fcinfo, 0));
    // C: pq_begintypsend(&buffer); pq_sendbytes(&buffer, uuid->data, UUID_LEN);
    //    PG_RETURN_BYTEA_P(pq_endtypsend(&buffer));
    // TODO(pg-port): libpq/pqformat not yet translated.
    let _ = uuid;
    unimplemented!("uuid_send: libpq/pqformat (pq_sendbytes) not yet translated")
}

/* internal uuid compare function */
unsafe fn uuid_internal_cmp(arg1: *const pg_uuid_t, arg2: *const pg_uuid_t) -> c_int {
    memcmp(
        (*arg1).data.as_ptr() as *const c_void,
        (*arg2).data.as_ptr() as *const c_void,
        UUID_LEN,
    )
}

pub unsafe fn uuid_lt(fcinfo: FunctionCallInfo) -> Datum {
    let arg1 = DatumGetUUIDP(PG_GETARG_DATUM!(fcinfo, 0));
    let arg2 = DatumGetUUIDP(PG_GETARG_DATUM!(fcinfo, 1));
    PG_RETURN_BOOL!(uuid_internal_cmp(arg1, arg2) < 0);
}
pub unsafe fn uuid_le(fcinfo: FunctionCallInfo) -> Datum {
    let arg1 = DatumGetUUIDP(PG_GETARG_DATUM!(fcinfo, 0));
    let arg2 = DatumGetUUIDP(PG_GETARG_DATUM!(fcinfo, 1));
    PG_RETURN_BOOL!(uuid_internal_cmp(arg1, arg2) <= 0);
}
pub unsafe fn uuid_eq(fcinfo: FunctionCallInfo) -> Datum {
    let arg1 = DatumGetUUIDP(PG_GETARG_DATUM!(fcinfo, 0));
    let arg2 = DatumGetUUIDP(PG_GETARG_DATUM!(fcinfo, 1));
    PG_RETURN_BOOL!(uuid_internal_cmp(arg1, arg2) == 0);
}
pub unsafe fn uuid_ge(fcinfo: FunctionCallInfo) -> Datum {
    let arg1 = DatumGetUUIDP(PG_GETARG_DATUM!(fcinfo, 0));
    let arg2 = DatumGetUUIDP(PG_GETARG_DATUM!(fcinfo, 1));
    PG_RETURN_BOOL!(uuid_internal_cmp(arg1, arg2) >= 0);
}
pub unsafe fn uuid_gt(fcinfo: FunctionCallInfo) -> Datum {
    let arg1 = DatumGetUUIDP(PG_GETARG_DATUM!(fcinfo, 0));
    let arg2 = DatumGetUUIDP(PG_GETARG_DATUM!(fcinfo, 1));
    PG_RETURN_BOOL!(uuid_internal_cmp(arg1, arg2) > 0);
}
pub unsafe fn uuid_ne(fcinfo: FunctionCallInfo) -> Datum {
    let arg1 = DatumGetUUIDP(PG_GETARG_DATUM!(fcinfo, 0));
    let arg2 = DatumGetUUIDP(PG_GETARG_DATUM!(fcinfo, 1));
    PG_RETURN_BOOL!(uuid_internal_cmp(arg1, arg2) != 0);
}

/* handler for btree index operator */
pub unsafe fn uuid_cmp(fcinfo: FunctionCallInfo) -> Datum {
    let arg1 = DatumGetUUIDP(PG_GETARG_DATUM!(fcinfo, 0));
    let arg2 = DatumGetUUIDP(PG_GETARG_DATUM!(fcinfo, 1));
    PG_RETURN_INT32!(uuid_internal_cmp(arg1, arg2));
}

/* Sort support strategy routine */
pub unsafe fn uuid_sortsupport(fcinfo: FunctionCallInfo) -> Datum {
    // TODO(pg-port): utils/sortsupport.h (SortSupport, abbreviation via lib/hyperloglog)
    // not yet translated.
    let _ = fcinfo;
    unimplemented!("uuid_sortsupport: utils/sortsupport.h not yet translated")
}

/* Skip support strategy routine */
pub unsafe fn uuid_skipsupport(fcinfo: FunctionCallInfo) -> Datum {
    // TODO(pg-port): utils/skipsupport.h + Relation not yet translated.
    let _ = fcinfo;
    unimplemented!("uuid_skipsupport: utils/skipsupport.h not yet translated")
}

pub unsafe fn uuid_hash(fcinfo: FunctionCallInfo) -> Datum {
    let key = DatumGetUUIDP(PG_GETARG_DATUM!(fcinfo, 0));
    hash_any((*key).data.as_ptr(), UUID_LEN as c_int)
}

pub unsafe fn uuid_hash_extended(fcinfo: FunctionCallInfo) -> Datum {
    let key = DatumGetUUIDP(PG_GETARG_DATUM!(fcinfo, 0));
    hash_any_extended(
        (*key).data.as_ptr(),
        UUID_LEN as c_int,
        PG_GETARG_INT64!(fcinfo, 1) as u64,
    )
}

/*
 * Set the given UUID version and the variant bits.
 *
 * # Safety
 * `uuid` points to a writable pg_uuid_t.
 */
#[inline]
unsafe fn uuid_set_version(uuid: *mut pg_uuid_t, version: u8) {
    /* set version field, top four bits */
    (*uuid).data[6] = ((*uuid).data[6] & 0x0f) | (version << 4);
    /* set variant field, top two bits are 1, 0 */
    (*uuid).data[8] = ((*uuid).data[8] & 0x3f) | 0x80;
}

/*
 * Generate UUID version 4.  All bytes are strong random except version/variant.
 */
pub unsafe fn gen_random_uuid(fcinfo: FunctionCallInfo) -> Datum {
    let _ = fcinfo;
    let uuid: *mut pg_uuid_t = palloc(UUID_LEN) as *mut pg_uuid_t;

    if !pg_strong_random(uuid as *mut c_void, UUID_LEN) {
        let _ = errcode(ERRCODE_INTERNAL_ERROR);
        ereport!(ERROR, errmsg!("could not generate random values"));
    }

    /* "version 4" (pseudorandom) UUID + variant (RFC 9562) */
    uuid_set_version(uuid, 4);

    return UUIDPGetDatum(uuid); // PG_RETURN_UUID_P
}

pub unsafe fn uuidv7(fcinfo: FunctionCallInfo) -> Datum {
    // TODO(pg-port): generate_uuidv7 needs clock_gettime monotonic state (get_real_time_ns_ascending).
    let _ = fcinfo;
    unimplemented!("uuidv7: clock_gettime monotonic UUID-v7 generation not yet translated")
}

pub unsafe fn uuidv7_interval(fcinfo: FunctionCallInfo) -> Datum {
    // TODO(pg-port): needs utils/timestamp.h Interval + generate_uuidv7.
    let _ = fcinfo;
    unimplemented!("uuidv7_interval: utils/timestamp.h Interval not yet translated")
}

pub unsafe fn uuid_extract_timestamp(fcinfo: FunctionCallInfo) -> Datum {
    // TODO(pg-port): returns TimestampTz (utils/timestamp.h) not yet translated.
    let _ = fcinfo;
    unimplemented!("uuid_extract_timestamp: utils/timestamp.h TimestampTz not yet translated")
}

/*
 * Extract UUID version.  Returns null if not an RFC 9562 variant.
 */
pub unsafe fn uuid_extract_version(fcinfo: FunctionCallInfo) -> Datum {
    let uuid = DatumGetUUIDP(PG_GETARG_DATUM!(fcinfo, 0));
    let version: uint16;

    /* check if RFC 9562 variant */
    if ((*uuid).data[8] & 0xc0) != 0x80 {
        PG_RETURN_NULL!(fcinfo);
    }

    version = ((*uuid).data[6] >> 4) as uint16;

    PG_RETURN_UINT16!(version);
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

    #[test]
    fn uuid_io_compare_hash() {
        unsafe {
            let canon = c"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11";
            // in -> out round trip (canonical lowercase 8-4-4-4-12)
            let d = DirectFunctionCall1Coll(uuid_in, InvalidOid, CStringGetDatum(canon.as_ptr()));
            let s = DatumGetCString(DirectFunctionCall1Coll(uuid_out, InvalidOid, d));
            assert!(cstr_eq(s, "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"));

            // accepts braces + no-dash forms, normalizes on output
            let d2 = DirectFunctionCall1Coll(
                uuid_in,
                InvalidOid,
                CStringGetDatum(c"{A0EEBC999C0B4EF8BB6D6BB9BD380A11}".as_ptr()),
            );
            assert!(DatumGetBool(DirectFunctionCall2Coll(uuid_eq, InvalidOid, d, d2)));

            // ordering: 0000... < ffff...
            let lo = DirectFunctionCall1Coll(uuid_in, InvalidOid, CStringGetDatum(c"00000000-0000-0000-0000-000000000000".as_ptr()));
            let hi = DirectFunctionCall1Coll(uuid_in, InvalidOid, CStringGetDatum(c"ffffffff-ffff-ffff-ffff-ffffffffffff".as_ptr()));
            assert!(DatumGetBool(DirectFunctionCall2Coll(uuid_lt, InvalidOid, lo, hi)));
            assert!(DatumGetInt32(DirectFunctionCall2Coll(uuid_cmp, InvalidOid, lo, hi)) < 0);
            assert!(DatumGetBool(DirectFunctionCall2Coll(uuid_ne, InvalidOid, lo, hi)));

            // version nibble of a v4-shaped uuid (data[6] high nibble == 4)
            let v = DirectFunctionCall1Coll(uuid_extract_version, InvalidOid, d);
            assert_eq!(crate::postgres::DatumGetUInt16(v), 4);

            // gen_random_uuid produces a valid v4 (version 4, RFC variant)
            let g = gen_random_uuid(core::ptr::null_mut());
            let gp = DatumGetUUIDP(g);
            assert_eq!((*gp).data[6] >> 4, 4);
            assert_eq!((*gp).data[8] & 0xc0, 0x80);
        }
    }

    #[test]
    #[should_panic]
    fn uuid_in_rejects_garbage() {
        unsafe {
            DirectFunctionCall1Coll(uuid_in, InvalidOid, CStringGetDatum(c"not-a-uuid".as_ptr()));
        }
    }
}
