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
    DirectFunctionCall2, PG_GETARG_DATUM, PG_GETARG_INT64, PG_GETARG_POINTER, PG_RETURN_BOOL,
    PG_RETURN_CSTRING, PG_RETURN_INT32, PG_RETURN_NULL, PG_RETURN_POINTER, PG_RETURN_UINT16,
    PG_RETURN_VOID,
};
use crate::c::{int16, int32, int64, uint16, uint32, uint64};
use crate::common::hashfn::{hash_any, hash_any_extended, hash_uint32};
use crate::port::pg_strong_random::pg_strong_random;
use crate::port::pg_bswap::DatumBigEndianToNative;
use crate::postgres::{DatumGetPointer, DatumGetUInt32, Int64GetDatum, PointerGetDatum};
use crate::nodes::nodes::Node;
use crate::utils::mmgr::memnodes::MemoryContext;
use crate::utils::mmgr::mcxt::MemoryContextSwitchTo;
use crate::lib::stringinfo::{StringInfo, StringInfoData};
use crate::lib::hyperloglog::{
    addHyperLogLog, estimateHyperLogLog, hyperLogLogState, initHyperLogLog,
};
use crate::libpq::pqformat::{
    pq_begintypsend, pq_endtypsend, pq_getmsgbytes, pq_sendbytes,
};
use crate::utils::sort::sortsupport::{SortSupport, SortSupportData};
use crate::utils::sort::tuplesort::ssup_datum_unsigned_cmp;
use crate::utils::adt::date::{Interval, TimestampTz};
use crate::utils::adt::timestamp::timestamptz_pl_interval;
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

/* struct timespec as defined by POSIX <time.h> (64-bit Unix: two longs). */
#[repr(C)]
struct timespec {
    tv_sec: i64, // time_t
    tv_nsec: c_long,
}

/* CLOCK_REALTIME on the platforms we target. */
const CLOCK_REALTIME: c_int = 0;

extern "C" {
    fn isxdigit(ch: c_int) -> c_int;
    fn strtoul(s: *const c_char, endptr: *mut *mut c_char, base: c_int) -> c_ulong;
    fn memcmp(a: *const c_void, b: *const c_void, n: usize) -> c_int;
    fn clock_gettime(clk_id: c_int, tp: *mut timespec) -> c_int;
}

/* errcodes.h classification (errcode() shim ignores the value). */
const ERRCODE_INVALID_TEXT_REPRESENTATION: c_int = 0;
const ERRCODE_INTERNAL_ERROR: c_int = 0;

/* helper macros */
const NS_PER_S: int64 = 1000000000;
const NS_PER_MS: int64 = 1000000;
const NS_PER_US: int64 = 1000;
const US_PER_MS: int64 = 1000;

/*
 * UUID version 7 uses 12 bits in "rand_a" to store 1/4096 (or 2^12) fractions of
 * sub-millisecond.  On systems with only 10 bits of sub-millisecond precision, we
 * still use 1/4096 parts of a millisecond, but fill lower 2 bits with random
 * numbers (see generate_uuidv7() for details).
 *
 * SUBMS_MINIMAL_STEP_NS defines the minimum number of nanoseconds that guarantees
 * an increase in the UUID's clock precision.
 */
#[cfg(target_os = "macos")]
const SUBMS_MINIMAL_STEP_BITS: u32 = 10;
#[cfg(not(target_os = "macos"))]
const SUBMS_MINIMAL_STEP_BITS: u32 = 12;
const SUBMS_BITS: u32 = 12;
const SUBMS_MINIMAL_STEP_NS: int64 = (NS_PER_MS / (1 << SUBMS_MINIMAL_STEP_BITS)) + 1;

/* timestamp.h constants used by uuidv7_interval / uuid_extract_timestamp */
const POSTGRES_EPOCH_JDATE: int64 = 2451545; /* == date2j(2000, 1, 1) */
const UNIX_EPOCH_JDATE: int64 = 2440588; /* == date2j(1970, 1, 1) */
const SECS_PER_DAY: int64 = 86400;
const USECS_PER_SEC: int64 = 1000000;

/* limits.h */
const UCHAR_MAX: u8 = 255;

/* utils/skipsupport.h (local mirror, matching the adt convention) */
pub type Relation = *mut c_void;
#[repr(C)]
pub struct SkipSupportData {
    pub low_elem: Datum,
    pub high_elem: Datum,
    pub decrement: Option<unsafe fn(Relation, Datum, *mut bool) -> Datum>,
    pub increment: Option<unsafe fn(Relation, Datum, *mut bool) -> Datum>,
}
pub type SkipSupport = *mut SkipSupportData;

/* timestamp.h fmgr conversions (file-local, as in timestamp.rs) */
#[allow(non_snake_case)]
fn TimestampTzGetDatum(x: TimestampTz) -> Datum {
    Int64GetDatum(x)
}
#[allow(non_snake_case)]
fn DatumGetTimestampTz(x: Datum) -> TimestampTz {
    x as TimestampTz
}
#[allow(non_snake_case)]
fn IntervalPGetDatum(x: *const Interval) -> Datum {
    PointerGetDatum(x as *const c_void)
}

// TODO(pg-port): trace_sort GUC lives in utils/sortsupport / tuplesort.
static mut trace_sort: bool = false;

/* sortsupport for uuid */
#[repr(C)]
struct uuid_sortsupport_state {
    input_count: int64,        /* number of non-null values seen */
    estimating: bool,          /* true if estimating cardinality */
    abbr_card: hyperLogLogState, /* cardinality estimator */
}

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

// the `goto syntax_error` target of string_to_uuid: route through the real errsave
// mechanism so pg_input_is_valid / pg_input_error_info see a populated
// ErrorSaveContext; for a null/non-ErrorSaveContext this raises a hard ERROR.
unsafe fn string_to_uuid_syntax_error(source: *const c_char, escontext: *mut Node) {
    const T_ErrorSaveContext: c_int = 447;
    const ERRCODE_INVALID_TEXT_REPRESENTATION_REAL: c_int = 33685634; /* 22P02 */
    /* Genuine soft-error context: record through the real errsave mechanism. */
    if !escontext.is_null() && *(escontext as *const c_int) == T_ErrorSaveContext {
        let msg = format!(
            "invalid input syntax for type uuid: \"{}\"",
            std::ffi::CStr::from_ptr(source).to_string_lossy()
        );
        if crate::utils::error::elog_impl::errsave_start(escontext, core::ptr::null()) {
            crate::utils::error::elog_impl::errcode_impl(ERRCODE_INVALID_TEXT_REPRESENTATION_REAL);
            if let Ok(c) = std::ffi::CString::new(msg) {
                crate::utils::error::elog_impl::errmsg_c(c.as_ptr());
            }
            crate::utils::error::elog_impl::errsave_finish(
                escontext, c"uuid.rs".as_ptr(), 0, c"string_to_uuid".as_ptr(),
            );
        }
        return;
    }
    /* Hard error path (normal input): raise through the backend's error path. */
    let _ = errcode(ERRCODE_INVALID_TEXT_REPRESENTATION);
    ereport!(
        ERROR,
        errmsg!("invalid input syntax for type {}: \"{}\"", "uuid", cstr(source))
    );
}

pub unsafe fn uuid_recv(fcinfo: FunctionCallInfo) -> Datum {
    let buffer: StringInfo = PG_GETARG_POINTER!(fcinfo, 0) as StringInfo;
    let uuid: *mut pg_uuid_t;

    uuid = palloc(UUID_LEN) as *mut pg_uuid_t;
    core::ptr::copy_nonoverlapping(
        pq_getmsgbytes(buffer, UUID_LEN as c_int),
        (*uuid).data.as_mut_ptr() as *mut c_char,
        UUID_LEN,
    );
    return PointerGetDatum(uuid as *const c_void); // PG_RETURN_POINTER
}

pub unsafe fn uuid_send(fcinfo: FunctionCallInfo) -> Datum {
    let uuid: *mut pg_uuid_t = DatumGetUUIDP(PG_GETARG_DATUM!(fcinfo, 0));
    let mut buffer: StringInfoData = core::mem::zeroed();

    pq_begintypsend(&mut buffer);
    pq_sendbytes(&mut buffer, (*uuid).data.as_ptr() as *const c_void, UUID_LEN as c_int);
    return PointerGetDatum(pq_endtypsend(&mut buffer) as *const c_void); // PG_RETURN_BYTEA_P
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

/*
 * Sort support strategy routine
 */
pub unsafe fn uuid_sortsupport(fcinfo: FunctionCallInfo) -> Datum {
    let ssup = PG_GETARG_POINTER!(fcinfo, 0) as SortSupport;

    (*ssup).comparator = Some(uuid_fast_cmp);
    (*ssup).ssup_extra = null_mut();

    if (*ssup).abbreviate {
        let uss: *mut uuid_sortsupport_state;
        let oldcontext: MemoryContext;

        oldcontext = MemoryContextSwitchTo((*ssup).ssup_cxt);

        uss = palloc(core::mem::size_of::<uuid_sortsupport_state>())
            as *mut uuid_sortsupport_state;
        (*uss).input_count = 0;
        (*uss).estimating = true;
        initHyperLogLog(&mut (*uss).abbr_card, 10);

        (*ssup).ssup_extra = uss as *mut c_void;

        (*ssup).comparator = Some(ssup_datum_unsigned_cmp);
        (*ssup).abbrev_converter = Some(uuid_abbrev_convert);
        (*ssup).abbrev_abort = Some(uuid_abbrev_abort);
        (*ssup).abbrev_full_comparator = Some(uuid_fast_cmp);

        MemoryContextSwitchTo(oldcontext);
    }

    PG_RETURN_VOID!();
}

/*
 * SortSupport comparison func
 */
unsafe fn uuid_fast_cmp(x: Datum, y: Datum, _ssup: SortSupport) -> c_int {
    let arg1 = DatumGetUUIDP(x);
    let arg2 = DatumGetUUIDP(y);

    uuid_internal_cmp(arg1, arg2)
}

/*
 * Callback for estimating effectiveness of abbreviated key optimization.
 *
 * We pay no attention to the cardinality of the non-abbreviated data, because
 * there is no equality fast-path within authoritative uuid comparator.
 */
unsafe fn uuid_abbrev_abort(memtupcount: c_int, ssup: SortSupport) -> bool {
    let uss = (*ssup).ssup_extra as *mut uuid_sortsupport_state;
    let abbr_card: f64;

    if memtupcount < 10000 || (*uss).input_count < 10000 || !(*uss).estimating {
        return false;
    }

    abbr_card = estimateHyperLogLog(&mut (*uss).abbr_card);

    /*
     * If we have >100k distinct values, then even if we were sorting many
     * billion rows we'd likely still break even, and the penalty of undoing
     * that many rows of abbrevs would probably not be worth it.  Stop even
     * counting at that point.
     */
    if abbr_card > 100000.0 {
        if trace_sort {
            elog!(
                LOG,
                "uuid_abbrev: estimation ends at cardinality {} after {} values ({} rows)",
                abbr_card,
                (*uss).input_count,
                memtupcount
            );
        }
        (*uss).estimating = false;
        return false;
    }

    /*
     * Target minimum cardinality is 1 per ~2k of non-null inputs.  0.5 row
     * fudge factor allows us to abort earlier on genuinely pathological data
     * where we've had exactly one abbreviated value in the first 2k
     * (non-null) rows.
     */
    if abbr_card < (*uss).input_count as f64 / 2000.0 + 0.5 {
        if trace_sort {
            elog!(
                LOG,
                "uuid_abbrev: aborting abbreviation at cardinality {} below threshold {} after {} values ({} rows)",
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
            "uuid_abbrev: cardinality {} after {} values ({} rows)",
            abbr_card,
            (*uss).input_count,
            memtupcount
        );
    }

    false
}

/*
 * Conversion routine for sortsupport.  Converts original uuid representation
 * to abbreviated key representation.  Our encoding strategy is simple -- pack
 * the first `sizeof(Datum)` bytes of uuid data into a Datum (on little-endian
 * machines, the bytes are stored in reverse order), and treat it as an
 * unsigned integer.
 */
unsafe fn uuid_abbrev_convert(original: Datum, ssup: SortSupport) -> Datum {
    let uss = (*ssup).ssup_extra as *mut uuid_sortsupport_state;
    let authoritative = DatumGetUUIDP(original);
    let mut res: Datum;

    res = 0;
    core::ptr::copy_nonoverlapping(
        (*authoritative).data.as_ptr(),
        &mut res as *mut Datum as *mut u8,
        core::mem::size_of::<Datum>(),
    );
    (*uss).input_count += 1;

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
     * 3-way comparator) works correctly on all platforms.  If we didn't do
     * this, the comparator would have to call memcmp() with a pair of
     * pointers to the first byte of each abbreviated key, which is slower.
     */
    res = DatumBigEndianToNative(res);

    res
}

unsafe fn uuid_decrement(_rel: Relation, existing: Datum, underflow: *mut bool) -> Datum {
    let uuid: *mut pg_uuid_t;

    uuid = palloc(UUID_LEN) as *mut pg_uuid_t;
    core::ptr::copy_nonoverlapping(DatumGetUUIDP(existing), uuid, 1);
    let mut i: i32 = (UUID_LEN - 1) as i32;
    while i >= 0 {
        if (*uuid).data[i as usize] > 0 {
            (*uuid).data[i as usize] -= 1;
            *underflow = false;
            return UUIDPGetDatum(uuid);
        }
        (*uuid).data[i as usize] = UCHAR_MAX;
        i -= 1;
    }

    pfree(uuid as *mut c_void); /* cannot leak memory */

    /* return value is undefined */
    *underflow = true;
    0 as Datum
}

unsafe fn uuid_increment(_rel: Relation, existing: Datum, overflow: *mut bool) -> Datum {
    let uuid: *mut pg_uuid_t;

    uuid = palloc(UUID_LEN) as *mut pg_uuid_t;
    core::ptr::copy_nonoverlapping(DatumGetUUIDP(existing), uuid, 1);
    let mut i: i32 = (UUID_LEN - 1) as i32;
    while i >= 0 {
        if (*uuid).data[i as usize] < UCHAR_MAX {
            (*uuid).data[i as usize] += 1;
            *overflow = false;
            return UUIDPGetDatum(uuid);
        }
        (*uuid).data[i as usize] = 0;
        i -= 1;
    }

    pfree(uuid as *mut c_void); /* cannot leak memory */

    /* return value is undefined */
    *overflow = true;
    0 as Datum
}

pub unsafe fn uuid_skipsupport(fcinfo: FunctionCallInfo) -> Datum {
    let sksup = PG_GETARG_POINTER!(fcinfo, 0) as SkipSupport;
    let uuid_min = palloc(UUID_LEN) as *mut pg_uuid_t;
    let uuid_max = palloc(UUID_LEN) as *mut pg_uuid_t;

    (*uuid_min).data = [0x00; UUID_LEN];
    (*uuid_max).data = [0xFF; UUID_LEN];

    (*sksup).decrement = Some(uuid_decrement);
    (*sksup).increment = Some(uuid_increment);
    (*sksup).low_elem = UUIDPGetDatum(uuid_min);
    (*sksup).high_elem = UUIDPGetDatum(uuid_max);

    PG_RETURN_VOID!();
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

/*
 * Get the current timestamp with nanosecond precision for UUID generation.
 * The returned timestamp is ensured to be at least SUBMS_MINIMAL_STEP greater
 * than the previous returned timestamp (on this backend).
 */
#[inline]
unsafe fn get_real_time_ns_ascending() -> int64 {
    static mut PREVIOUS_NS: int64 = 0;
    let mut ns: int64;

    /* Get the current real timestamp */

    /*
     * We don't use gettimeofday(), instead use clock_gettime() with
     * CLOCK_REALTIME where available in order to get a high-precision
     * (nanoseconds) real timestamp.
     */
    let mut tmp: timespec = core::mem::zeroed();
    clock_gettime(CLOCK_REALTIME, &mut tmp);
    ns = tmp.tv_sec as int64 * NS_PER_S + tmp.tv_nsec as int64;

    /* Guarantee the minimal step advancement of the timestamp */
    if PREVIOUS_NS + SUBMS_MINIMAL_STEP_NS >= ns {
        ns = PREVIOUS_NS + SUBMS_MINIMAL_STEP_NS;
    }
    PREVIOUS_NS = ns;

    ns
}

/*
 * Generate UUID version 7 per RFC 9562, with the given timestamp.
 *
 * unix_ts_ms is a number of milliseconds since start of the UNIX epoch,
 * and sub_ms is a number of nanoseconds within millisecond.  These values are
 * used for time-dependent bits of UUID.
 *
 * NB: all numbers here are unsigned, unix_ts_ms cannot be negative per RFC.
 */
unsafe fn generate_uuidv7(unix_ts_ms: uint64, sub_ms: uint32) -> *mut pg_uuid_t {
    let uuid = palloc(UUID_LEN) as *mut pg_uuid_t;
    let increased_clock_precision: uint32;

    /* Fill in time part */
    (*uuid).data[0] = (unix_ts_ms >> 40) as u8;
    (*uuid).data[1] = (unix_ts_ms >> 32) as u8;
    (*uuid).data[2] = (unix_ts_ms >> 24) as u8;
    (*uuid).data[3] = (unix_ts_ms >> 16) as u8;
    (*uuid).data[4] = (unix_ts_ms >> 8) as u8;
    (*uuid).data[5] = unix_ts_ms as u8;

    /*
     * sub-millisecond timestamp fraction (SUBMS_BITS bits, not
     * SUBMS_MINIMAL_STEP_BITS)
     */
    increased_clock_precision = (sub_ms * (1 << SUBMS_BITS)) / NS_PER_MS as uint32;

    /* Fill the increased clock precision to "rand_a" bits */
    (*uuid).data[6] = (increased_clock_precision >> 8) as u8;
    (*uuid).data[7] = increased_clock_precision as u8;

    /* fill everything after the increased clock precision with random bytes */
    if !pg_strong_random(
        (*uuid).data.as_mut_ptr().add(8) as *mut c_void,
        UUID_LEN - 8,
    ) {
        let _ = errcode(ERRCODE_INTERNAL_ERROR);
        ereport!(ERROR, errmsg!("could not generate random values"));
    }

    if SUBMS_MINIMAL_STEP_BITS == 10 {
        /*
         * On systems that have only 10 bits of sub-ms precision, 2 least
         * significant are dependent on other time-specific bits, and they do
         * not contribute to uniqueness.  To make these bit random we mix in
         * two bits from CSPRNG.  SUBMS_MINIMAL_STEP is chosen so that we still
         * guarantee monotonicity despite altering these bits.
         */
        (*uuid).data[7] = (*uuid).data[7] ^ ((*uuid).data[8] >> 6);
    }

    /*
     * Set magic numbers for a "version 7" (pseudorandom) UUID and variant,
     * see https://www.rfc-editor.org/rfc/rfc9562#name-version-field
     */
    uuid_set_version(uuid, 7);

    uuid
}

/*
 * Generate UUID version 7 with the current timestamp.
 */
pub unsafe fn uuidv7(fcinfo: FunctionCallInfo) -> Datum {
    let _ = fcinfo;
    let ns: int64 = get_real_time_ns_ascending();
    let uuid: *mut pg_uuid_t =
        generate_uuidv7((ns / NS_PER_MS) as uint64, (ns % NS_PER_MS) as uint32);

    return UUIDPGetDatum(uuid); // PG_RETURN_UUID_P
}

/*
 * Similar to uuidv7() but with the timestamp adjusted by the given interval.
 */
pub unsafe fn uuidv7_interval(fcinfo: FunctionCallInfo) -> Datum {
    let shift: *mut Interval = DatumGetPointer(PG_GETARG_DATUM!(fcinfo, 0)) as *mut Interval; // PG_GETARG_INTERVAL_P
    let mut ts: TimestampTz;
    let uuid: *mut pg_uuid_t;
    let ns: int64 = get_real_time_ns_ascending();
    let us: int64;

    /*
     * Shift the current timestamp by the given interval.  To calculate time
     * shift correctly, we convert the UNIX epoch to TimestampTz and use
     * timestamptz_pl_interval().  This calculation is done with microsecond
     * precision.
     */

    ts = (ns / NS_PER_US) as TimestampTz
        - (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECS_PER_DAY * USECS_PER_SEC;

    /* Compute time shift */
    ts = DatumGetTimestampTz(DirectFunctionCall2!(
        timestamptz_pl_interval,
        TimestampTzGetDatum(ts),
        IntervalPGetDatum(shift)
    ));

    /* Convert a TimestampTz value back to an UNIX epoch timestamp */
    us = ts + (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECS_PER_DAY * USECS_PER_SEC;

    /* Generate an UUIDv7 */
    uuid = generate_uuidv7(
        (us / US_PER_MS) as uint64,
        ((us % US_PER_MS) * NS_PER_US + ns % NS_PER_US) as uint32,
    );

    return UUIDPGetDatum(uuid); // PG_RETURN_UUID_P
}

/*
 * Start of a Gregorian epoch == date2j(1582,10,15)
 * We cast it to 64-bit because it's used in overflow-prone computations
 */
const GREGORIAN_EPOCH_JDATE: int64 = 2299161;

/*
 * Extract timestamp from UUID.
 *
 * Returns null if not RFC 9562 variant or not a version that has a timestamp.
 */
pub unsafe fn uuid_extract_timestamp(fcinfo: FunctionCallInfo) -> Datum {
    let uuid = DatumGetUUIDP(PG_GETARG_DATUM!(fcinfo, 0));
    let version: c_int;
    let tms: uint64;
    let ts: TimestampTz;

    /* check if RFC 9562 variant */
    if ((*uuid).data[8] & 0xc0) != 0x80 {
        PG_RETURN_NULL!(fcinfo);
    }

    version = ((*uuid).data[6] >> 4) as c_int;

    if version == 1 {
        tms = ((*uuid).data[0] as uint64) << 24
            | ((*uuid).data[1] as uint64) << 16
            | ((*uuid).data[2] as uint64) << 8
            | ((*uuid).data[3] as uint64)
            | ((*uuid).data[4] as uint64) << 40
            | ((*uuid).data[5] as uint64) << 32
            | (((*uuid).data[6] as uint64 & 0xf) << 56)
            | ((*uuid).data[7] as uint64) << 48;

        /* convert 100-ns intervals to us, then adjust */
        ts = (tms / 10) as TimestampTz
            - (POSTGRES_EPOCH_JDATE - GREGORIAN_EPOCH_JDATE) * SECS_PER_DAY * USECS_PER_SEC;
        return TimestampTzGetDatum(ts); // PG_RETURN_TIMESTAMPTZ
    }

    if version == 7 {
        tms = ((*uuid).data[5] as uint64)
            | (((*uuid).data[4] as uint64) << 8)
            | (((*uuid).data[3] as uint64) << 16)
            | (((*uuid).data[2] as uint64) << 24)
            | (((*uuid).data[1] as uint64) << 32)
            | (((*uuid).data[0] as uint64) << 40);

        /* convert ms to us, then adjust */
        ts = (tms as int64 * US_PER_MS) as TimestampTz
            - (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECS_PER_DAY * USECS_PER_SEC;

        return TimestampTzGetDatum(ts); // PG_RETURN_TIMESTAMPTZ
    }

    /* not a timestamp-containing UUID version */
    PG_RETURN_NULL!(fcinfo);
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
