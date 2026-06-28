//! Core text/bytea type machinery. Translated from
//! src/backend/utils/adt/varlena.c (the LARGE file; the core text type is
//! translated completely, exotic ops are staged -- see the per-fn notes).
//!
//! FULLY TRANSLATED (the `text` type works for catalog + user text):
//!   - the cstring<->text helpers: `cstring_to_text`, `cstring_to_text_with_len`,
//!     `text_to_cstring`, `text_to_cstring_buffer`;
//!   - I/O: `textin`/`textout`/`textsend` (and `unknownin`/`unknownout`);
//!   - length: `textlen`/`text_length`, `textoctetlen`, `byteaoctetlen`;
//!   - concat: `textcat`/`text_catenate`, `byteacat`/`bytea_catenate`;
//!   - substr: `text_substr`/`text_substr_no_len`/`text_substring`;
//!   - comparison: `varstr_cmp`(C/default-collation memcmp path)/`text_cmp`,
//!     `texteq`/`textne`/`text_lt`/`text_le`/`text_gt`/`text_ge`/`bttextcmp`,
//!     `text_starts_with`;
//!   - bytea I/O: `byteain`/`byteaout`, the bytea comparison suite.
//!
//! STAGED (call existing stubs / `unimplemented!()`; rules.md s4):
//!   - `textrecv`/`bytearecv`/`namerecv`-style binary input: the MsgReader is
//!     passed by pointer-through-Datum, not yet marshalled (same as int4recv).
//!   - non-C / non-default collations route `varstr_cmp` through the
//!     `utils::varlena` ICU/strcoll stub (`pg_strncoll` not translated).
//!   - regex match (`textregexeq`...), encode/decode (`encode`/`decode`),
//!     text_to_array/array_to_text, normalize, the SortSupport abbreviation,
//!     and the string_agg aggregates live in other functions not in this core
//!     set; they are not provided here and remain reachable via the header
//!     stubs when those subsystems land.
//!
//! VARLENA BYTE LAYOUT: built with the foundation's `varatt` accessors. Because
//! palloc is still a stub, a freshly-built varlena is a leaked Rust allocation
//! (a `Vec<u8>` whose buffer is `Box::leak`ed) carrying a 4-byte little-endian
//! header (`SET_VARSIZE` = total length << 2) followed by the payload, exactly
//! PG's uncompressed 4-byte-header on-disk form. Read paths use `VARSIZE_ANY` /
//! `VARSIZE_ANY_EXHDR` / `VARDATA_ANY`, so short-header (1-byte) inputs are
//! handled too; external/compressed (toasted) inputs are not produced here and
//! `pg_detoast_*` remains deferred.

#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    reason = "faithful C width arithmetic: varlena.c does explicit int32 length \
              math and casts between byte counts and i32 lengths (the value-cast \
              family is an allowed port-inherent lint per rules.md s11)"
)]

use crate::c::{text, VARHDRSZ};
use crate::ereport;
use crate::fmgr::{FunctionCallInfoBaseData, PG_GET_COLLATION};
use crate::catalog::genbki::C_COLLATION_OID;
use crate::postgres::{
    CStringGetDatum, Datum, DatumGetBool, DatumGetCString, DatumGetInt32, DatumGetPointer,
    Int32GetDatum, PointerGetDatum,
};
use crate::postgres_ext::Oid;
use crate::utils::elog::ERROR;
use crate::utils::errcodes::{ERRCODE_INVALID_TEXT_REPRESENTATION, ERRCODE_SUBSTRING_ERROR};
use crate::varatt::{SET_VARSIZE, VARDATA, VARDATA_ANY, VARSIZE_ANY_EXHDR};

// ---------------------------------------------------------------------------
// Varlena buffer construction / access.
//
// palloc is a stub, so we leak owned buffers (like int.rs leaks output cstrings)
// until a MemoryContext exists. Each builder lays out [4-byte LE header][data]
// and returns a raw `*mut varlena`-family pointer the Datum carries.
// ---------------------------------------------------------------------------

/// Allocate a leaked 4-byte-header varlena buffer of `len` payload bytes and
/// copy `src` into the data area. Returns a pointer to the header.
fn make_varlena(src: &[u8]) -> *mut u8 {
    let total = src.len() + VARHDRSZ as usize;
    let mut buf = vec![0u8; total].into_boxed_slice();
    let ptr = buf.as_mut_ptr();
    // SAFETY: `ptr` heads a freshly-allocated `total`-byte buffer; the header
    // write touches the first 4 bytes and VARDATA the following `len`.
    unsafe {
        SET_VARSIZE(ptr, total as u32);
        if !src.is_empty() {
            core::ptr::copy_nonoverlapping(src.as_ptr(), VARDATA(ptr), src.len());
        }
    }
    // Leak: no MemoryContext to own this yet (mirrors int.rs output leaking).
    Box::leak(buf).as_mut_ptr()
}

/// Borrow the payload bytes of any non-toasted varlena as a slice. Handles both
/// 4-byte and 1-byte (short) headers via the `VARSIZE_ANY_EXHDR`/`VARDATA_ANY`
/// accessors.
///
/// SAFETY: `p` must point at a valid, non-external/non-compressed varlena that
/// outlives the returned slice.
unsafe fn varlena_bytes<'a>(p: *mut u8) -> &'a [u8] {
    let len = VARSIZE_ANY_EXHDR(p);
    core::slice::from_raw_parts(VARDATA_ANY(p), len)
}

// ---------------------------------------------------------------------------
// PG_GETARG_* / PG_RETURN_* accessors.
// ---------------------------------------------------------------------------

#[inline]
fn pg_getarg_cstring(fcinfo: &FunctionCallInfoBaseData, n: usize) -> String {
    let p = DatumGetCString(fcinfo.args[n].value);
    // SAFETY: an input function's cstring argument is a NUL-terminated C string
    // that outlives the call.
    let cstr = unsafe { core::ffi::CStr::from_ptr(p) };
    cstr.to_string_lossy().into_owned()
}

#[inline]
fn pg_return_cstring(s: &str) -> Datum {
    let bytes: Vec<u8> = s.bytes().take_while(|&b| b != 0).collect();
    let c = std::ffi::CString::new(bytes).unwrap_or_default();
    CStringGetDatum(c.into_raw())
}

/// `PG_GETARG_TEXT_PP(n)` / `PG_GETARG_BYTEA_PP(n)`: the argument varlena ptr.
#[inline]
fn pg_getarg_varlena(fcinfo: &FunctionCallInfoBaseData, n: usize) -> *mut u8 {
    DatumGetPointer(fcinfo.args[n].value)
}

// ===========================================================================
//   CONVERSION ROUTINES EXPORTED FOR USE BY OTHER CODE
// ===========================================================================

/// PG `cstring_to_text`: create a text value from a NUL-terminated C string.
#[must_use]
pub fn cstring_to_text(s: &str) -> *mut text {
    cstring_to_text_with_len(s, s.len() as i32)
}

/// PG `cstring_to_text_with_len`: like [`cstring_to_text`] but caller supplies
/// the length (the string need not be NUL-terminated).
#[must_use]
pub fn cstring_to_text_with_len(s: &str, len: i32) -> *mut text {
    let n = (len.max(0) as usize).min(s.len());
    make_varlena(&s.as_bytes()[..n]).cast::<text>()
}

/// PG `text_to_cstring`: a (leaked) NUL-terminated string from a text value.
///
/// Supports short-header inputs; toasted inputs would need `pg_detoast_*`
/// (deferred). The result lifetime is the buffer's, returned as an owned
/// `String` since the Rust callers want a `String` (the header decl's shape).
#[must_use]
pub fn text_to_cstring(t: &text) -> String {
    let p = std::ptr::from_ref::<text>(t).cast::<u8>().cast_mut();
    // SAFETY: `t` is a valid non-toasted text the caller keeps alive.
    let bytes = unsafe { varlena_bytes(p) };
    String::from_utf8_lossy(bytes).into_owned()
}

/// PG `text_to_cstring_buffer`: copy a text value into `dst`, truncating to fit;
/// the result is NUL-terminated unless `dst` is empty.
pub fn text_to_cstring_buffer(src: &text, dst: &mut [u8]) {
    if dst.is_empty() {
        return;
    }
    let p = std::ptr::from_ref::<text>(src).cast::<u8>().cast_mut();
    // SAFETY: `src` is a valid non-toasted text the caller keeps alive.
    let bytes = unsafe { varlena_bytes(p) };
    let n = (dst.len() - 1).min(bytes.len());
    // Encoding-safe truncation: back off to a UTF-8 char boundary.
    let n = floor_char_boundary(bytes, n);
    dst[..n].copy_from_slice(&bytes[..n]);
    dst[n] = 0;
}

/// Largest `m <= n` that is a UTF-8 char boundary in `b` (encoding-safe clip).
fn floor_char_boundary(b: &[u8], n: usize) -> usize {
    let mut m = n.min(b.len());
    while m > 0 && (b[m] & 0xC0) == 0x80 {
        m -= 1;
    }
    m
}

// ===========================================================================
//   USER I/O ROUTINES
// ===========================================================================

/// PG `textin`: converts a cstring to text.
pub fn textin(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let input = pg_getarg_cstring(fcinfo, 0);
    PointerGetDatum(cstring_to_text(&input).cast::<u8>())
}

/// PG `textout`: converts text to a cstring.
pub fn textout(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let p = pg_getarg_varlena(fcinfo, 0);
    // SAFETY: the arg is a valid non-toasted text varlena.
    let bytes = unsafe { varlena_bytes(p) };
    let s = String::from_utf8_lossy(bytes);
    pg_return_cstring(&s)
}

/// PG `textrecv`: converts external binary format to text.
pub fn textrecv(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("textrecv needs the binary wire StringInfo (pq_getmsgtext) marshalling")
}

/// PG `textsend`: converts text to binary format. The wire form of text is just
/// its raw bytes wrapped as a bytea.
pub fn textsend(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let p = pg_getarg_varlena(fcinfo, 0);
    // SAFETY: the arg is a valid non-toasted text varlena.
    let bytes = unsafe { varlena_bytes(p) };
    PointerGetDatum(make_varlena(bytes))
}

/// PG `unknownin`: representation is the same as cstring.
pub fn unknownin(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let s = pg_getarg_cstring(fcinfo, 0);
    pg_return_cstring(&s)
}

/// PG `unknownout`: representation is the same as cstring.
pub fn unknownout(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let s = pg_getarg_cstring(fcinfo, 0);
    pg_return_cstring(&s)
}

// ===========================================================================
//   bytea I/O
// ===========================================================================

/// PG `byteain`: parse the printable representation of a byte array. Supports
/// the `\x...` hex form and the traditional backslash-escaped form.
pub fn byteain(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let input = pg_getarg_cstring(fcinfo, 0);
    let b = input.as_bytes();

    // Hex input: "\x" prefix.
    if b.len() >= 2 && b[0] == b'\\' && b[1] == b'x' {
        let out = hex_decode_str(&input[2..]);
        return PointerGetDatum(make_varlena(&out));
    }

    // Traditional escaped style.
    let mut out: Vec<u8> = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] != b'\\' {
            out.push(b[i]);
            i += 1;
        } else if i + 3 < b.len()
            && (b[i + 1] >= b'0' && b[i + 1] <= b'3')
            && (b[i + 2] >= b'0' && b[i + 2] <= b'7')
            && (b[i + 3] >= b'0' && b[i + 3] <= b'7')
        {
            let v = ((b[i + 1] - b'0') << 6) + ((b[i + 2] - b'0') << 3) + (b[i + 3] - b'0');
            out.push(v);
            i += 4;
        } else if i + 1 < b.len() && b[i + 1] == b'\\' {
            out.push(b'\\');
            i += 2;
        } else {
            ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
                e.errcode(ERRCODE_INVALID_TEXT_REPRESENTATION)
                    .errmsg("invalid input syntax for type bytea");
            });
            unreachable!()
        }
    }
    PointerGetDatum(make_varlena(&out))
}

/// PG `byteaout`: traditional escaped representation (the default when not in
/// hex output mode; this port emits the escaped form, matching `bytea_output`
/// = escape).
pub fn byteaout(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let p = pg_getarg_varlena(fcinfo, 0);
    // SAFETY: the arg is a valid non-toasted bytea varlena.
    let data = unsafe { varlena_bytes(p) };
    let mut out = String::with_capacity(data.len());
    for &c in data {
        if c == b'\\' {
            out.push_str("\\\\");
        } else if !(0x20..=0x7e).contains(&c) {
            out.push('\\');
            out.push(char::from(b'0' + ((c >> 6) & 0x03)));
            out.push(char::from(b'0' + ((c >> 3) & 0x07)));
            out.push(char::from(b'0' + (c & 0x07)));
        } else {
            out.push(char::from(c));
        }
    }
    pg_return_cstring(&out)
}

/// PG `bytearecv`: converts external binary format to bytea.
pub fn bytearecv(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("bytearecv needs the binary wire StringInfo (pq_copymsgbytes) marshalling")
}

/// PG `byteasend`: converts bytea to binary format (a copy of the input).
pub fn byteasend(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let p = pg_getarg_varlena(fcinfo, 0);
    // SAFETY: the arg is a valid non-toasted bytea varlena.
    let data = unsafe { varlena_bytes(p) };
    PointerGetDatum(make_varlena(data))
}

/// Decode the hex digits of `s` (after the `\x`) into bytes. C: `hex_decode`.
fn hex_decode_str(s: &str) -> Vec<u8> {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len() / 2);
    let mut hi: Option<u8> = None;
    for &c in b {
        if c.is_ascii_whitespace() {
            continue;
        }
        let Some(d) = hex_digit(c) else {
            ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
                e.errcode(ERRCODE_INVALID_TEXT_REPRESENTATION)
                    .errmsg("invalid hexadecimal digit");
            });
            unreachable!()
        };
        match hi.take() {
            None => hi = Some(d),
            Some(h) => out.push((h << 4) | d),
        }
    }
    if hi.is_some() {
        ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(ERRCODE_INVALID_TEXT_REPRESENTATION)
                .errmsg("invalid hexadecimal data: odd number of digits");
        });
        unreachable!()
    }
    out
}

fn hex_digit(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

// ===========================================================================
//   PUBLIC ROUTINES: length
// ===========================================================================

/// PG `textlen`: the logical (character) length of a text.
pub fn textlen(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let p = pg_getarg_varlena(fcinfo, 0);
    Int32GetDatum(text_length(p))
}

/// PG `text_length`: the real work of `textlen`. Default (UTF-8) database
/// encoding: count Unicode scalar values. Invalid bytes are counted lossily.
fn text_length(p: *mut u8) -> i32 {
    // SAFETY: `p` is a valid non-toasted text varlena.
    let bytes = unsafe { varlena_bytes(p) };
    // UTF-8 char count (mb path). For a single-byte encoding this equals the
    // byte length, which is also correct here for ASCII.
    String::from_utf8_lossy(bytes).chars().count() as i32
}

/// PG `textoctetlen`: the physical (byte) length of a text.
pub fn textoctetlen(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let p = pg_getarg_varlena(fcinfo, 0);
    // SAFETY: valid non-toasted varlena.
    let len = unsafe { VARSIZE_ANY_EXHDR(p) };
    Int32GetDatum(len as i32)
}

/// PG `byteaoctetlen`: the byte length of a bytea.
pub fn byteaoctetlen(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let p = pg_getarg_varlena(fcinfo, 0);
    // SAFETY: valid non-toasted varlena.
    let len = unsafe { VARSIZE_ANY_EXHDR(p) };
    Int32GetDatum(len as i32)
}

// ===========================================================================
//   PUBLIC ROUTINES: concatenation
// ===========================================================================

/// PG `textcat`: concatenate two text values.
pub fn textcat(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let t1 = pg_getarg_varlena(fcinfo, 0);
    let t2 = pg_getarg_varlena(fcinfo, 1);
    PointerGetDatum(text_catenate(t1, t2))
}

/// PG `text_catenate`: guts of `textcat`.
///
/// SAFETY contract folded in: `t1`/`t2` are valid non-toasted varlenas.
fn text_catenate(t1: *mut u8, t2: *mut u8) -> *mut u8 {
    // SAFETY: both are valid non-toasted varlenas.
    let (b1, b2) = unsafe { (varlena_bytes(t1), varlena_bytes(t2)) };
    let mut joined = Vec::with_capacity(b1.len() + b2.len());
    joined.extend_from_slice(b1);
    joined.extend_from_slice(b2);
    make_varlena(&joined)
}

/// PG `byteacat`: concatenate two bytea values.
pub fn byteacat(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let t1 = pg_getarg_varlena(fcinfo, 0);
    let t2 = pg_getarg_varlena(fcinfo, 1);
    // bytea_catenate is byte-identical to text_catenate.
    PointerGetDatum(text_catenate(t1, t2))
}

// ===========================================================================
//   PUBLIC ROUTINES: substring
// ===========================================================================

/// PG `text_substr`: `substring(string FROM start FOR length)`.
pub fn text_substr(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let p = pg_getarg_varlena(fcinfo, 0);
    let start = DatumGetInt32(fcinfo.args[1].value);
    let length = DatumGetInt32(fcinfo.args[2].value);
    PointerGetDatum(text_substring(p, start, length, false))
}

/// PG `text_substr_no_len`: `substring(string FROM start)`.
pub fn text_substr_no_len(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let p = pg_getarg_varlena(fcinfo, 0);
    let start = DatumGetInt32(fcinfo.args[1].value);
    PointerGetDatum(text_substring(p, start, -1, true))
}

/// PG `text_substring`: the real work of `text_substr`/`text_substr_no_len`,
/// over the default (UTF-8) encoding (character-based positions).
///
/// SAFETY contract folded in: `p` is a valid non-toasted text varlena.
fn text_substring(p: *mut u8, start: i32, length: i32, length_not_specified: bool) -> *mut u8 {
    // SAFETY: valid non-toasted varlena.
    let bytes = unsafe { varlena_bytes(p) };
    let chars: Vec<char> = String::from_utf8_lossy(bytes).chars().collect();
    let total = chars.len() as i64;

    let s = i64::from(start);
    let s1 = s.max(1); // adjusted (1-based) start

    let l1: i64 = if length_not_specified {
        -1
    } else if length < 0 {
        ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(ERRCODE_SUBSTRING_ERROR)
                .errmsg("negative substring length not allowed");
        });
        unreachable!()
    } else {
        // E = S + length (end, exclusive); guard overflow per C.
        match s.checked_add(i64::from(length)) {
            None => -1,
            Some(e) => {
                if e < 1 {
                    return cstring_to_text("").cast::<u8>();
                }
                e - s1
            }
        }
    };

    // Convert to a 0-based char window and slice.
    let begin = (s1 - 1).clamp(0, total) as usize;
    let end = if l1 < 0 {
        total as usize
    } else {
        ((s1 - 1 + l1).clamp(0, total)) as usize
    };
    let sub: String = chars[begin..end.max(begin)].iter().collect();
    cstring_to_text(&sub).cast::<u8>()
}

// ===========================================================================
//   COMPARISON ROUTINES
// ===========================================================================

/// PG `varstr_cmp`: collation-aware variable-length string comparison. The C /
/// default-collation path is a `memcmp` with a length tiebreak (works for M2);
/// other collations route through the (deferred) `utils::varlena::varstr_cmp`
/// ICU/strcoll stub.
#[must_use]
pub fn varstr_cmp(arg1: &[u8], arg2: &[u8], collid: Oid) -> i32 {
    if collid == C_COLLATION_OID || collid == crate::catalog::genbki::DEFAULT_COLLATION_OID {
        let n = arg1.len().min(arg2.len());
        let result = arg1[..n].cmp(&arg2[..n]);
        return match result {
            core::cmp::Ordering::Less => -1,
            core::cmp::Ordering::Greater => 1,
            core::cmp::Ordering::Equal => arg1.len().cmp(&arg2.len()) as i32,
        };
    }
    // Provider-specific collation: not translated yet.
    let s1 = String::from_utf8_lossy(arg1);
    let s2 = String::from_utf8_lossy(arg2);
    crate::utils::varlena::varstr_cmp(&s1, arg1.len() as i32, &s2, arg2.len() as i32, collid)
}

/// PG `text_cmp`: internal comparison of two text strings; returns -1/0/1.
///
/// SAFETY contract folded in: `arg1`/`arg2` are valid non-toasted varlenas.
fn text_cmp(arg1: *mut u8, arg2: *mut u8, collid: Oid) -> i32 {
    // SAFETY: valid non-toasted varlenas.
    let (b1, b2) = unsafe { (varlena_bytes(arg1), varlena_bytes(arg2)) };
    varstr_cmp(b1, b2, collid)
}

macro_rules! text_cmp_op {
    ($name:ident, $op:tt) => {
        #[doc = concat!("PG `", stringify!($name), "`.")]
        pub fn $name(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
            let arg1 = pg_getarg_varlena(fcinfo, 0);
            let arg2 = pg_getarg_varlena(fcinfo, 1);
            let result = text_cmp(arg1, arg2, PG_GET_COLLATION(fcinfo)) $op 0;
            crate::postgres::BoolGetDatum(result)
        }
    };
}

text_cmp_op!(text_lt, <);
text_cmp_op!(text_le, <=);
text_cmp_op!(text_gt, >);
text_cmp_op!(text_ge, >=);

/// PG `texteq`: equality (bitwise, exploiting that equal strings have equal
/// length under deterministic collations -- the default).
pub fn texteq(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let arg1 = pg_getarg_varlena(fcinfo, 0);
    let arg2 = pg_getarg_varlena(fcinfo, 1);
    // SAFETY: valid non-toasted varlenas.
    let (b1, b2) = unsafe { (varlena_bytes(arg1), varlena_bytes(arg2)) };
    crate::postgres::BoolGetDatum(b1 == b2)
}

/// PG `textne`: inequality.
pub fn textne(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let arg1 = pg_getarg_varlena(fcinfo, 0);
    let arg2 = pg_getarg_varlena(fcinfo, 1);
    // SAFETY: valid non-toasted varlenas.
    let (b1, b2) = unsafe { (varlena_bytes(arg1), varlena_bytes(arg2)) };
    crate::postgres::BoolGetDatum(b1 != b2)
}

/// PG `bttextcmp`: btree 3-way comparison support for text.
pub fn bttextcmp(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let arg1 = pg_getarg_varlena(fcinfo, 0);
    let arg2 = pg_getarg_varlena(fcinfo, 1);
    Int32GetDatum(text_cmp(arg1, arg2, PG_GET_COLLATION(fcinfo)))
}

/// PG `text_starts_with`: `starts_with(string, prefix)`.
pub fn text_starts_with(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let arg1 = pg_getarg_varlena(fcinfo, 0);
    let arg2 = pg_getarg_varlena(fcinfo, 1);
    // SAFETY: valid non-toasted varlenas.
    let (b1, b2) = unsafe { (varlena_bytes(arg1), varlena_bytes(arg2)) };
    crate::postgres::BoolGetDatum(b1.starts_with(b2))
}

macro_rules! bytea_cmp_op {
    ($name:ident, $op:tt) => {
        #[doc = concat!("PG `", stringify!($name), "`: bytewise bytea comparison.")]
        pub fn $name(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
            let a = pg_getarg_varlena(fcinfo, 0);
            let b = pg_getarg_varlena(fcinfo, 1);
            // SAFETY: valid non-toasted varlenas.
            let (ba, bb) = unsafe { (varlena_bytes(a), varlena_bytes(b)) };
            crate::postgres::BoolGetDatum(ba $op bb)
        }
    };
}

bytea_cmp_op!(byteaeq, ==);
bytea_cmp_op!(byteane, !=);
bytea_cmp_op!(bytealt, <);
bytea_cmp_op!(byteale, <=);
bytea_cmp_op!(byteagt, >);
bytea_cmp_op!(byteage, >=);

/// PG `byteacmp`: btree 3-way comparison support for bytea.
pub fn byteacmp(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let a = pg_getarg_varlena(fcinfo, 0);
    let b = pg_getarg_varlena(fcinfo, 1);
    // SAFETY: valid non-toasted varlenas.
    let (ba, bb) = unsafe { (varlena_bytes(a), varlena_bytes(b)) };
    let r = match ba.cmp(bb) {
        core::cmp::Ordering::Less => -1,
        core::cmp::Ordering::Equal => 0,
        core::cmp::Ordering::Greater => 1,
    };
    Int32GetDatum(r)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::postgres::NullableDatum;

    fn fc(args: &[Datum]) -> FunctionCallInfoBaseData {
        FunctionCallInfoBaseData {
            flinfo: None,
            context: core::ptr::null_mut(),
            resultinfo: core::ptr::null_mut(),
            fncollation: C_COLLATION_OID,
            isnull: false,
            nargs: args.len() as i16,
            args: args
                .iter()
                .map(|&value| NullableDatum { value, isnull: false })
                .collect(),
        }
    }

    fn cstr_datum(s: &str) -> Datum {
        let c = std::ffi::CString::new(s).unwrap();
        CStringGetDatum(c.into_raw())
    }

    fn text_datum(s: &str) -> Datum {
        PointerGetDatum(cstring_to_text(s).cast::<u8>())
    }

    fn out_to_string(d: Datum) -> String {
        let p = DatumGetCString(d);
        let cstr = unsafe { core::ffi::CStr::from_ptr(p) };
        cstr.to_string_lossy().into_owned()
    }

    fn run_out(d: Datum) -> String {
        let mut f = fc(&[d]);
        out_to_string(textout(&mut f))
    }

    #[test]
    fn cstring_text_roundtrip() {
        for s in ["", "a", "hello world", "snowman \u{2603} ok", "\u{1f600}"] {
            let t = cstring_to_text(s);
            // SAFETY: freshly built text we own.
            assert_eq!(text_to_cstring(unsafe { &*t }), s, "{s}");
        }
    }

    #[test]
    fn textin_textout_roundtrip() {
        for s in ["", "ascii", "with spaces", "caf\u{e9}", "\u{4f60}\u{597d}"] {
            let mut inf = fc(&[cstr_datum(s)]);
            let d = textin(&mut inf);
            assert_eq!(run_out(d), s, "{s}");
        }
    }

    #[test]
    fn texteq_textne_and_cmp() {
        let a = text_datum("abc");
        let a2 = text_datum("abc");
        let b = text_datum("abd");
        let pre = text_datum("ab");
        assert!(DatumGetBool(texteq(&mut fc(&[a, a2]))));
        assert!(!DatumGetBool(texteq(&mut fc(&[a, b]))));
        assert!(DatumGetBool(textne(&mut fc(&[a, b]))));
        // ordering: ab < abc < abd
        assert!(DatumGetBool(text_lt(&mut fc(&[pre, a]))));
        assert!(DatumGetBool(text_lt(&mut fc(&[a, b]))));
        assert!(DatumGetBool(text_le(&mut fc(&[a, a2]))));
        assert!(DatumGetBool(text_gt(&mut fc(&[b, a]))));
        assert!(DatumGetBool(text_ge(&mut fc(&[a, a2]))));
        assert!(DatumGetInt32(bttextcmp(&mut fc(&[a, b]))) < 0);
        assert_eq!(DatumGetInt32(bttextcmp(&mut fc(&[a, a2]))), 0);
        assert!(DatumGetInt32(bttextcmp(&mut fc(&[b, a]))) > 0);
        assert!(DatumGetBool(text_starts_with(&mut fc(&[a, pre]))));
    }

    #[test]
    fn textcat_concat() {
        let r = textcat(&mut fc(&[text_datum("foo"), text_datum("bar")]));
        assert_eq!(run_out(r), "foobar");
        let r = textcat(&mut fc(&[text_datum(""), text_datum("x")]));
        assert_eq!(run_out(r), "x");
    }

    #[test]
    fn text_length_char_and_octet() {
        // "caf\u{e9}" = 4 chars, 5 bytes in UTF-8.
        let mut lf = fc(&[text_datum("caf\u{e9}")]);
        assert_eq!(DatumGetInt32(textlen(&mut lf)), 4);
        let mut of = fc(&[text_datum("caf\u{e9}")]);
        assert_eq!(DatumGetInt32(textoctetlen(&mut of)), 5);
        // multibyte: 2 CJK chars = 2 chars, 6 bytes.
        let mut lf2 = fc(&[text_datum("\u{4f60}\u{597d}")]);
        assert_eq!(DatumGetInt32(textlen(&mut lf2)), 2);
        let mut of2 = fc(&[text_datum("\u{4f60}\u{597d}")]);
        assert_eq!(DatumGetInt32(textoctetlen(&mut of2)), 6);
    }

    #[test]
    fn text_substr_cases() {
        let s = "hello world";
        // substring('hello world' from 1 for 5) = 'hello'
        let r = text_substr(&mut fc(&[text_datum(s), Int32GetDatum(1), Int32GetDatum(5)]));
        assert_eq!(run_out(r), "hello");
        // from 7 (no len) = 'world'
        let r = text_substr_no_len(&mut fc(&[text_datum(s), Int32GetDatum(7)]));
        assert_eq!(run_out(r), "world");
        // start <= 0 adjusts: from -1 for 3 -> end E = -1+3 = 2 exclusive -> 'h'
        let r = text_substr(&mut fc(&[text_datum(s), Int32GetDatum(-1), Int32GetDatum(3)]));
        assert_eq!(run_out(r), "h");
        // multibyte substring
        let r = text_substr(&mut fc(&[
            text_datum("\u{4f60}\u{597d}\u{4e16}"),
            Int32GetDatum(2),
            Int32GetDatum(1),
        ]));
        assert_eq!(run_out(r), "\u{597d}");
    }

    #[test]
    fn bytea_in_out_roundtrip() {
        // hex form
        let mut f = fc(&[cstr_datum("\\xdeadbeef")]);
        let d = byteain(&mut f);
        let mut of = fc(&[d]);
        // default escape output: printable 0xde.. are non-printable -> octal
        let s = out_to_string(byteaout(&mut of));
        assert_eq!(s, "\\336\\255\\276\\357");
        // escaped roundtrip of printable + escape
        let mut f = fc(&[cstr_datum("ab\\\\c")]);
        let d = byteain(&mut f);
        let mut of = fc(&[d]);
        assert_eq!(out_to_string(byteaout(&mut of)), "ab\\\\c");
    }

    #[test]
    fn bytea_cmp_and_octetlen() {
        let a = text_datum("abc");
        let b = text_datum("abd");
        assert!(DatumGetBool(bytealt(&mut fc(&[a, b]))));
        assert!(DatumGetBool(byteaeq(&mut fc(&[a, a]))));
        assert!(DatumGetInt32(byteacmp(&mut fc(&[a, b]))) < 0);
        let mut of = fc(&[text_datum("hello")]);
        assert_eq!(DatumGetInt32(byteaoctetlen(&mut of)), 5);
    }

    #[test]
    fn fmgr_table_binds_texteq() {
        use crate::utils::fmgrtab::fmgr_builtins;
        let entry = fmgr_builtins
            .iter()
            .find(|b| b.func_name == "texteq")
            .expect("texteq present");
        let func = entry.func.expect("texteq bound");
        let mut f = fc(&[text_datum("z"), text_datum("z")]);
        assert!(DatumGetBool(func(&mut f)));
    }

    #[test]
    fn fmgr_table_binds_textout() {
        use crate::utils::fmgrtab::fmgr_builtins;
        let entry = fmgr_builtins
            .iter()
            .find(|b| b.func_name == "textout")
            .expect("textout present");
        let func = entry.func.expect("textout bound");
        let mut f = fc(&[text_datum("bound")]);
        assert_eq!(out_to_string(func(&mut f)), "bound");
    }

    #[test]
    fn text_to_cstring_buffer_truncates_safely() {
        let t = cstring_to_text("caf\u{e9}"); // 5 bytes
        let mut buf = [0u8; 4]; // room for 3 bytes + NUL -> "caf"
        // SAFETY: freshly built text we own.
        text_to_cstring_buffer(unsafe { &*t }, &mut buf);
        let s = core::ffi::CStr::from_bytes_until_nul(&buf).unwrap();
        assert_eq!(s.to_str().unwrap(), "caf");
    }
}
