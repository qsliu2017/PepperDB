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
use crate::fmgr::{FunctionCallInfoBaseData, PG_ARGISNULL, PG_GET_COLLATION, PG_NARGS};
use crate::catalog::genbki::C_COLLATION_OID;
use crate::postgres::{
    CStringGetDatum, Datum, DatumGetBool, DatumGetCString, DatumGetInt32, DatumGetInt64,
    DatumGetPointer, Int32GetDatum, PointerGetDatum,
};
use crate::postgres_ext::Oid;
use crate::utils::elog::ERROR;
use crate::utils::errcodes::{
    ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_INVALID_TEXT_REPRESENTATION,
    ERRCODE_NULL_VALUE_NOT_ALLOWED, ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
    ERRCODE_PROGRAM_LIMIT_EXCEEDED, ERRCODE_SUBSTRING_ERROR,
};
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
///
/// The result is carried in a varlena (byref) Datum that must outlive the call,
/// so the owned buffer is leaked into a raw pointer -- the same output-Datum
/// convention int.rs (`pg_return_cstring` -> `CString::into_raw`) uses.
/// TODO(memory-context): reclaim via the per-call/statement memory context when
/// that lands, replacing the leak.
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
    // Deterministic byte-order path (C / default / the regress DB collation, all of
    // which are C-order here). A truly locale-provider (ICU/libc strcoll) collation
    // is the only case that would need the deferred `utils::varlena` path; since no
    // such provider is wired, every collation we can see is memcmp-ordered. An
    // InvalidOid collation also falls here (a non-collatable-context comparison).
    if collid == C_COLLATION_OID
        || collid == crate::catalog::genbki::DEFAULT_COLLATION_OID
        || collid == crate::postgres_ext::InvalidOid
    {
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

// ===========================================================================
//   concat / concat_ws
// ===========================================================================

/// PG `concat_internal`: join argument values from `argidx`, separated by
/// `sepstr`, skipping NULLs. Each value is stringified via its type's output
/// function. Returns None when the (VARIADIC) result is NULL.
///
/// The per-argument type OID comes from `get_fn_expr_argtype`, whose dependency
/// `get_call_expr_argtype` is still a stub, so this reaches unimplemented at
/// runtime until that lands (STAGED). The join logic itself is complete.
#[allow(
    clippy::unnecessary_wraps,
    reason = "faithful to C concat_internal returning `text *` NULL: the None arm \
              is the concat(VARIADIC NULL) result, currently unreachable only \
              because the variadic branch is staged"
)]
fn concat_internal(sepstr: &str, argidx: usize, fcinfo: &mut FunctionCallInfoBaseData) -> Option<String> {
    // concat(VARIADIC array) delegates to array_to_text; that path (and the
    // variadic detection it needs) is deferred with the array subsystem.
    let variadic = fcinfo
        .flinfo
        .as_deref_mut()
        .is_some_and(crate::backend::utils::fmgr::fmgr::get_fn_expr_variadic);
    if variadic {
        unimplemented!("concat(VARIADIC ...) needs array_to_text + array deconstruct");
    }

    let nargs = PG_NARGS(fcinfo) as usize;
    let mut out = String::new();
    let mut first = true;
    for i in argidx..nargs {
        if PG_ARGISNULL(fcinfo, i) {
            continue;
        }
        if first {
            first = false;
        } else {
            out.push_str(sepstr);
        }
        let typid = format_argtype(fcinfo, i);
        let (typoutput, _varlena) = crate::utils::lsyscache::getTypeOutputInfo(typid);
        out.push_str(&crate::backend::utils::fmgr::fmgr::OidOutputFunctionCall(
            typoutput,
            fcinfo.args[i].value,
        ));
    }
    Some(out)
}

/// PG `text_concat`: `concat(...)` -- concatenate all args, ignoring NULLs.
pub fn text_concat(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    match concat_internal("", 0, fcinfo) {
        None => {
            fcinfo.isnull = true;
            Datum(0)
        }
        Some(s) => PointerGetDatum(cstring_to_text(&s).cast::<u8>()),
    }
}

/// PG `text_concat_ws`: `concat_ws(sep, ...)` -- concatenate args past the first
/// with the first as separator, ignoring NULLs. NULL separator returns NULL.
pub fn text_concat_ws(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    if PG_ARGISNULL(fcinfo, 0) {
        fcinfo.isnull = true;
        return Datum(0);
    }
    let sep_ptr = pg_getarg_varlena(fcinfo, 0);
    // SAFETY: valid non-toasted varlena.
    let sep = String::from_utf8_lossy(unsafe { varlena_bytes(sep_ptr) }).into_owned();
    match concat_internal(&sep, 1, fcinfo) {
        None => {
            fcinfo.isnull = true;
            Datum(0)
        }
        Some(s) => PointerGetDatum(cstring_to_text(&s).cast::<u8>()),
    }
}

// ===========================================================================
//   OVERLAY
// ===========================================================================

/// PG `textoverlay`: `overlay(t1 placing t2 from sp for sl)`.
pub fn textoverlay(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let t1 = pg_getarg_varlena(fcinfo, 0);
    let t2 = pg_getarg_varlena(fcinfo, 1);
    let sp = DatumGetInt32(fcinfo.args[2].value);
    let sl = DatumGetInt32(fcinfo.args[3].value);
    PointerGetDatum(text_overlay(t1, t2, sp, sl))
}

/// PG `textoverlay_no_len`: `overlay(t1 placing t2 from sp)` (length = len(t2)).
pub fn textoverlay_no_len(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let t1 = pg_getarg_varlena(fcinfo, 0);
    let t2 = pg_getarg_varlena(fcinfo, 1);
    let sp = DatumGetInt32(fcinfo.args[2].value);
    let sl = text_length(t2); // defaults to length(t2)
    PointerGetDatum(text_overlay(t1, t2, sp, sl))
}

/// PG `text_overlay`: guts of the overlay functions.
///
/// SAFETY contract folded in: `t1`/`t2` are valid non-toasted varlenas.
fn text_overlay(t1: *mut u8, t2: *mut u8, sp: i32, sl: i32) -> *mut u8 {
    if sp <= 0 {
        ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(ERRCODE_SUBSTRING_ERROR)
                .errmsg("negative substring length not allowed");
        });
        unreachable!()
    }
    let Some(sp_pl_sl) = sp.checked_add(sl) else {
        ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
                .errmsg("integer out of range");
        });
        unreachable!()
    };
    let s1 = text_substring(t1, 1, sp - 1, false);
    let s2 = text_substring(t1, sp_pl_sl, -1, true);
    let result = text_catenate(s1, t2);
    text_catenate(result, s2)
}

// ===========================================================================
//   POSITION (byte-based BMH-equivalent, UTF-8 default encoding)
// ===========================================================================

/// PG `textpos` / `strpos`: `position(substring in string)`. 1-based character
/// index of the first match, or 0 if absent.
pub fn textpos(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let str_ptr = pg_getarg_varlena(fcinfo, 0);
    let search_ptr = pg_getarg_varlena(fcinfo, 1);
    // SAFETY: valid non-toasted varlenas.
    let (hay, needle) = unsafe { (varlena_bytes(str_ptr), varlena_bytes(search_ptr)) };
    Int32GetDatum(text_position(hay, needle))
}

/// PG `text_position`: byte-search of `needle` in `haystack` (valid for UTF-8),
/// returning a 1-based *character* position, or 0 for no match. An empty needle
/// matches at position 1.
fn text_position(haystack: &[u8], needle: &[u8]) -> i32 {
    if needle.is_empty() {
        return 1;
    }
    let Some(byteoff) = byte_find(haystack, needle) else {
        return 0;
    };
    // Convert the byte offset to a 1-based character index (UTF-8).
    let chars_before = String::from_utf8_lossy(&haystack[..byteoff]).chars().count();
    (chars_before + 1) as i32
}

/// First byte offset of `needle` in `haystack`, or None. (`memmem`-equivalent.)
fn byte_find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() {
        return Some(0);
    }
    if needle.len() > haystack.len() {
        return None;
    }
    haystack
        .windows(needle.len())
        .position(|w| w == needle)
}

// ===========================================================================
//   bytea substring / position
// ===========================================================================

/// PG `bytea_substr`: `substr(bytea, start, length)`.
pub fn bytea_substr(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let p = pg_getarg_varlena(fcinfo, 0);
    let start = DatumGetInt32(fcinfo.args[1].value);
    let length = DatumGetInt32(fcinfo.args[2].value);
    PointerGetDatum(bytea_substring(p, start, length, false))
}

/// PG `bytea_substr_no_len`: `substr(bytea, start)`.
pub fn bytea_substr_no_len(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let p = pg_getarg_varlena(fcinfo, 0);
    let start = DatumGetInt32(fcinfo.args[1].value);
    PointerGetDatum(bytea_substring(p, start, -1, true))
}

/// PG `bytea_substring`: byte-window slice; logic mirrors `text_substring` but
/// on raw bytes.
///
/// SAFETY contract folded in: `p` is a valid non-toasted bytea varlena.
fn bytea_substring(p: *mut u8, s: i32, l: i32, length_not_specified: bool) -> *mut u8 {
    // SAFETY: valid non-toasted varlena.
    let bytes = unsafe { varlena_bytes(p) };
    let total = bytes.len() as i64;
    let s = i64::from(s);
    let s1 = s.max(1);

    let l1: i64 = if length_not_specified {
        -1
    } else if l < 0 {
        ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(ERRCODE_SUBSTRING_ERROR)
                .errmsg("negative substring length not allowed");
        });
        unreachable!()
    } else {
        match s.checked_add(i64::from(l)) {
            None => -1,
            Some(e) => {
                if e < 1 {
                    return make_varlena(&[]);
                }
                e - s1
            }
        }
    };

    let begin = (s1 - 1).clamp(0, total) as usize;
    let end = if l1 < 0 {
        total as usize
    } else {
        (s1 - 1 + l1).clamp(0, total) as usize
    };
    make_varlena(&bytes[begin..end.max(begin)])
}

/// PG `byteapos`: `position(t2 in t1)` for bytea; 1-based byte index or 0.
pub fn byteapos(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let t1 = pg_getarg_varlena(fcinfo, 0);
    let t2 = pg_getarg_varlena(fcinfo, 1);
    // SAFETY: valid non-toasted varlenas.
    let (b1, b2) = unsafe { (varlena_bytes(t1), varlena_bytes(t2)) };
    if b2.is_empty() {
        return Int32GetDatum(1); // result for empty pattern
    }
    let pos = byte_find(b1, b2).map_or(0, |off| (off + 1) as i32);
    Int32GetDatum(pos)
}

// ===========================================================================
//   REPLACE / SPLIT_PART (byte operations, UTF-8 default encoding)
// ===========================================================================

/// PG `replace_text`: `replace(string, from, to)`.
pub fn replace_text(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let src = pg_getarg_varlena(fcinfo, 0);
    let from = pg_getarg_varlena(fcinfo, 1);
    let to = pg_getarg_varlena(fcinfo, 2);
    // SAFETY: valid non-toasted varlenas.
    let (src_b, from_b, to_b) = unsafe {
        (varlena_bytes(src), varlena_bytes(from), varlena_bytes(to))
    };

    // Return unmodified source string if empty source or pattern.
    if src_b.is_empty() || from_b.is_empty() {
        return PointerGetDatum(make_varlena(src_b));
    }

    let mut out: Vec<u8> = Vec::with_capacity(src_b.len());
    let mut start = 0usize;
    while let Some(rel) = byte_find(&src_b[start..], from_b) {
        let at = start + rel;
        out.extend_from_slice(&src_b[start..at]);
        out.extend_from_slice(to_b);
        start = at + from_b.len();
    }
    out.extend_from_slice(&src_b[start..]);
    PointerGetDatum(make_varlena(&out))
}

/// PG `split_part`: `split_part(string, fldsep, fldnum)`.
pub fn split_part(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let input = pg_getarg_varlena(fcinfo, 0);
    let fldsep = pg_getarg_varlena(fcinfo, 1);
    let fldnum = DatumGetInt32(fcinfo.args[2].value);
    // SAFETY: valid non-toasted varlenas.
    let (inp, sep) = unsafe { (varlena_bytes(input), varlena_bytes(fldsep)) };

    if fldnum == 0 {
        ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg("field position must not be zero");
        });
        unreachable!()
    }

    // Return empty string for empty input string.
    if inp.is_empty() {
        return PointerGetDatum(make_varlena(&[]));
    }

    // Handle empty field separator.
    if sep.is_empty() {
        // If first or last field, return input string, else empty string.
        if fldnum == 1 || fldnum == -1 {
            return PointerGetDatum(make_varlena(inp));
        }
        return PointerGetDatum(make_varlena(&[]));
    }

    // Collect all field boundaries (byte-search; matches skip the pattern).
    let mut fields: Vec<&[u8]> = Vec::new();
    let mut start = 0usize;
    loop {
        let Some(rel) = byte_find(&inp[start..], sep) else {
            fields.push(&inp[start..]);
            break;
        };
        let at = start + rel;
        fields.push(&inp[start..at]);
        start = at + sep.len();
    }

    // fldnum is 1-based; negative counts from the right.
    let n = fields.len() as i32;
    let idx = if fldnum < 0 { n + fldnum } else { fldnum - 1 };
    let field: &[u8] = if idx < 0 || idx >= n {
        &[]
    } else {
        fields[idx as usize]
    };
    PointerGetDatum(make_varlena(field))
}

// ===========================================================================
//   to_hex / to_bin / to_oct
// ===========================================================================

/// PG `convert_to_base`: base-`base` (2..=16) unsigned digit string.
fn convert_to_base(mut value: u64, base: u64) -> *mut u8 {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut buf = [0u8; 64];
    let mut ptr = buf.len();
    loop {
        ptr -= 1;
        buf[ptr] = DIGITS[(value % base) as usize];
        value /= base;
        if ptr == 0 || value == 0 {
            break;
        }
    }
    make_varlena(&buf[ptr..])
}

/// PG `to_bin32`.
pub fn to_bin32(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let v = u64::from(DatumGetInt32(fcinfo.args[0].value) as u32);
    PointerGetDatum(convert_to_base(v, 2))
}
/// PG `to_bin64`.
pub fn to_bin64(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let v = DatumGetInt64(fcinfo.args[0].value) as u64;
    PointerGetDatum(convert_to_base(v, 2))
}
/// PG `to_oct32`.
pub fn to_oct32(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let v = u64::from(DatumGetInt32(fcinfo.args[0].value) as u32);
    PointerGetDatum(convert_to_base(v, 8))
}
/// PG `to_oct64`.
pub fn to_oct64(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let v = DatumGetInt64(fcinfo.args[0].value) as u64;
    PointerGetDatum(convert_to_base(v, 8))
}
/// PG `to_hex32`.
pub fn to_hex32(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let v = u64::from(DatumGetInt32(fcinfo.args[0].value) as u32);
    PointerGetDatum(convert_to_base(v, 16))
}
/// PG `to_hex64`.
pub fn to_hex64(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let v = DatumGetInt64(fcinfo.args[0].value) as u64;
    PointerGetDatum(convert_to_base(v, 16))
}

// ===========================================================================
//   left / right / reverse / repeat
// ===========================================================================

/// PG `text_left`: `left(string, n)`. First `n` chars; negative `n` returns all
/// but the last `|n|` chars.
pub fn text_left(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let p = pg_getarg_varlena(fcinfo, 0);
    let n = DatumGetInt32(fcinfo.args[1].value);
    // SAFETY: valid non-toasted varlena.
    let chars: Vec<char> = String::from_utf8_lossy(unsafe { varlena_bytes(p) })
        .chars()
        .collect();
    let total = chars.len() as i64;
    let take = if n < 0 {
        (total + i64::from(n)).max(0)
    } else {
        i64::from(n).min(total)
    } as usize;
    let s: String = chars[..take].iter().collect();
    PointerGetDatum(cstring_to_text(&s).cast::<u8>())
}

/// PG `text_right`: `right(string, n)`. Last `n` chars; negative `n` returns all
/// but the first `|n|` chars.
pub fn text_right(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let p = pg_getarg_varlena(fcinfo, 0);
    let n = DatumGetInt32(fcinfo.args[1].value);
    // SAFETY: valid non-toasted varlena.
    let chars: Vec<char> = String::from_utf8_lossy(unsafe { varlena_bytes(p) })
        .chars()
        .collect();
    let total = chars.len() as i64;
    let off = if n < 0 {
        i64::from(-n).min(total)
    } else {
        (total - i64::from(n)).max(0)
    } as usize;
    let s: String = chars[off..].iter().collect();
    PointerGetDatum(cstring_to_text(&s).cast::<u8>())
}

/// PG `text_reverse`: `reverse(string)`, character-wise (UTF-8).
pub fn text_reverse(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let p = pg_getarg_varlena(fcinfo, 0);
    // SAFETY: valid non-toasted varlena.
    let s: String = String::from_utf8_lossy(unsafe { varlena_bytes(p) })
        .chars()
        .rev()
        .collect();
    PointerGetDatum(cstring_to_text(&s).cast::<u8>())
}

/// PG `repeat`: `repeat(string, count)`.
pub fn text_repeat(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let p = pg_getarg_varlena(fcinfo, 0);
    let count = DatumGetInt32(fcinfo.args[1].value).max(0);
    // SAFETY: valid non-toasted varlena.
    let bytes = unsafe { varlena_bytes(p) };
    let Some(tlen) = (count as usize).checked_mul(bytes.len()) else {
        ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
                .errmsg("requested length too large");
        });
        unreachable!()
    };
    let mut out = Vec::with_capacity(tlen);
    for _ in 0..count {
        out.extend_from_slice(bytes);
    }
    PointerGetDatum(make_varlena(&out))
}

// ===========================================================================
//   trim family (oracle_compat.c: ltrim/rtrim/btrim + bytea variants)
// ===========================================================================

/// PG `dotrim`: character-wise trim of any char in `set` from either end.
fn dotrim(string: &[u8], set: &[u8], trim_start: bool, trim_end: bool) -> *mut u8 {
    if string.is_empty() || set.is_empty() {
        return make_varlena(string);
    }
    let chars: Vec<char> = String::from_utf8_lossy(string).chars().collect();
    let setchars: Vec<char> = String::from_utf8_lossy(set).chars().collect();
    let mut begin = 0usize;
    let mut end = chars.len();
    if trim_start {
        while begin < end && setchars.contains(&chars[begin]) {
            begin += 1;
        }
    }
    if trim_end {
        while end > begin && setchars.contains(&chars[end - 1]) {
            end -= 1;
        }
    }
    let s: String = chars[begin..end].iter().collect();
    cstring_to_text(&s).cast::<u8>()
}

macro_rules! text_trim_fn {
    ($name:ident, $trim_start:expr, $trim_end:expr, $doc:literal) => {
        #[doc = $doc]
        pub fn $name(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
            let string = pg_getarg_varlena(fcinfo, 0);
            let set = pg_getarg_varlena(fcinfo, 1);
            // SAFETY: valid non-toasted varlenas.
            let (sb, setb) = unsafe { (varlena_bytes(string), varlena_bytes(set)) };
            PointerGetDatum(dotrim(sb, setb, $trim_start, $trim_end))
        }
    };
}

text_trim_fn!(ltrim, true, false, "PG `ltrim`: trim leading chars in set.");
text_trim_fn!(rtrim, false, true, "PG `rtrim`: trim trailing chars in set.");
text_trim_fn!(btrim, true, true, "PG `btrim`: trim leading+trailing chars in set.");

macro_rules! text_trim1_fn {
    ($name:ident, $trim_start:expr, $trim_end:expr, $doc:literal) => {
        #[doc = $doc]
        pub fn $name(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
            let string = pg_getarg_varlena(fcinfo, 0);
            // SAFETY: valid non-toasted varlena.
            let sb = unsafe { varlena_bytes(string) };
            PointerGetDatum(dotrim(sb, b" ", $trim_start, $trim_end))
        }
    };
}

text_trim1_fn!(ltrim1, true, false, "PG `ltrim1`: trim leading spaces.");
text_trim1_fn!(rtrim1, false, true, "PG `rtrim1`: trim trailing spaces.");
text_trim1_fn!(btrim1, true, true, "PG `btrim1`: trim leading+trailing spaces.");

/// PG `dobyteatrim`: byte-wise trim of any byte in `set` from either end.
fn dobyteatrim(string: &[u8], set: &[u8], trim_start: bool, trim_end: bool) -> *mut u8 {
    if string.is_empty() || set.is_empty() {
        return make_varlena(string);
    }
    let mut begin = 0usize;
    let mut end = string.len();
    if trim_start {
        while begin < end && set.contains(&string[begin]) {
            begin += 1;
        }
    }
    if trim_end {
        while end > begin && set.contains(&string[end - 1]) {
            end -= 1;
        }
    }
    make_varlena(&string[begin..end])
}

macro_rules! bytea_trim_fn {
    ($name:ident, $trim_start:expr, $trim_end:expr, $doc:literal) => {
        #[doc = $doc]
        pub fn $name(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
            let string = pg_getarg_varlena(fcinfo, 0);
            let set = pg_getarg_varlena(fcinfo, 1);
            // SAFETY: valid non-toasted varlenas.
            let (sb, setb) = unsafe { (varlena_bytes(string), varlena_bytes(set)) };
            PointerGetDatum(dobyteatrim(sb, setb, $trim_start, $trim_end))
        }
    };
}

bytea_trim_fn!(byteatrim, true, true, "PG `byteatrim`: trim leading+trailing bytes in set.");
bytea_trim_fn!(bytealtrim, true, false, "PG `bytealtrim`: trim leading bytes in set.");
bytea_trim_fn!(byteartrim, false, true, "PG `byteartrim`: trim trailing bytes in set.");

// ===========================================================================
//   translate / ascii / chr
// ===========================================================================

/// PG `translate`: `translate(string, from, to)` -- replace each char present in
/// `from` with the same-position char in `to`, deleting when `to` is shorter.
pub fn translate(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let string = pg_getarg_varlena(fcinfo, 0);
    let from = pg_getarg_varlena(fcinfo, 1);
    let to = pg_getarg_varlena(fcinfo, 2);
    // SAFETY: valid non-toasted varlenas.
    let (sb, fb, tb) = unsafe {
        (varlena_bytes(string), varlena_bytes(from), varlena_bytes(to))
    };
    if sb.is_empty() {
        return PointerGetDatum(make_varlena(sb));
    }
    let from_chars: Vec<char> = String::from_utf8_lossy(fb).chars().collect();
    let to_chars: Vec<char> = String::from_utf8_lossy(tb).chars().collect();
    let mut out = String::new();
    for ch in String::from_utf8_lossy(sb).chars() {
        match from_chars.iter().position(|&f| f == ch) {
            Some(idx) => {
                if let Some(&rep) = to_chars.get(idx) {
                    out.push(rep);
                }
                // else: delete (no corresponding "to" char)
            }
            None => out.push(ch),
        }
    }
    PointerGetDatum(cstring_to_text(&out).cast::<u8>())
}

/// PG `ascii`: decimal code of the first character (UTF-8 code point), or 0 for
/// an empty string.
pub fn ascii(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let p = pg_getarg_varlena(fcinfo, 0);
    // SAFETY: valid non-toasted varlena.
    let bytes = unsafe { varlena_bytes(p) };
    if bytes.is_empty() {
        return Int32GetDatum(0);
    }
    let cp = String::from_utf8_lossy(bytes)
        .chars()
        .next()
        .map_or(0, |c| c as i32);
    Int32GetDatum(cp)
}

/// PG `chr`: the character with Unicode code point `arg` (UTF-8 default
/// encoding).
pub fn chr(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let arg = DatumGetInt32(fcinfo.args[0].value);
    if arg < 0 {
        ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg("character number must be positive");
        });
        unreachable!()
    }
    if arg == 0 {
        ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
                .errmsg("null character not permitted");
        });
        unreachable!()
    }
    let cvalue = arg as u32;
    let Some(ch) = char::from_u32(cvalue) else {
        ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
                .errmsg(format!("requested character too large for encoding: {cvalue}"));
        });
        unreachable!()
    };
    let mut buf = [0u8; 4];
    let s = ch.encode_utf8(&mut buf);
    PointerGetDatum(cstring_to_text(s).cast::<u8>())
}

// ===========================================================================
//   quoting: quote_ident / quote_literal / quote_nullable
// ===========================================================================

/// PG `quote_identifier` (ruleutils.c): quote an identifier if it isn't a safe
/// bare identifier (all lowercase/digits/underscore, not a non-unreserved
/// keyword). Returns the (possibly unchanged) quoted form.
fn quote_identifier(ident: &str) -> String {
    let bytes = ident.as_bytes();
    let mut safe = bytes
        .first()
        .is_some_and(|&c| c.is_ascii_lowercase() || c == b'_');
    let mut nquotes = 0usize;
    for &ch in bytes {
        let ok = ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == b'_';
        if !ok {
            safe = false;
            if ch == b'"' {
                nquotes += 1;
            }
        }
    }
    if safe && keyword_needs_quote(ident) {
        safe = false;
    }
    if safe {
        return ident.to_owned();
    }
    let mut result = String::with_capacity(ident.len() + nquotes + 2);
    result.push('"');
    for ch in ident.chars() {
        if ch == '"' {
            result.push('"');
        }
        result.push(ch);
    }
    result.push('"');
    result
}

/// True if `ident` (already all-lowercase) is a keyword that must be quoted --
/// PG quotes keywords except UNRESERVED ones. C: `ScanKeywordLookup` +
/// `ScanKeywordCategories[kwnum] != UNRESERVED_KEYWORD`.
fn keyword_needs_quote(ident: &str) -> bool {
    use crate::parser::kwlist::{KeywordCategory, KEYWORDS};
    KEYWORDS
        .binary_search_by(|&(name, _, _)| name.cmp(ident))
        .is_ok_and(|i| KEYWORDS[i].1 != KeywordCategory::UNRESERVED_KEYWORD)
}

/// PG `quote_literal_internal`: build a single-quoted SQL literal (doubling any
/// embedded quote, prefixing `E` when a backslash is present).
fn quote_literal_cstr(src: &str) -> String {
    let mut out = String::with_capacity(src.len() + 3);
    if src.contains('\\') {
        out.push('E');
    }
    out.push('\'');
    for ch in src.chars() {
        if ch == '\'' || ch == '\\' {
            out.push(ch);
        }
        out.push(ch);
    }
    out.push('\'');
    out
}

/// PG `quote_ident`: return a properly-quoted identifier.
pub fn quote_ident(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let p = pg_getarg_varlena(fcinfo, 0);
    // SAFETY: valid non-toasted varlena.
    let s = String::from_utf8_lossy(unsafe { varlena_bytes(p) }).into_owned();
    PointerGetDatum(cstring_to_text(&quote_identifier(&s)).cast::<u8>())
}

/// PG `quote_literal`: return a properly-quoted literal.
pub fn quote_literal(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let p = pg_getarg_varlena(fcinfo, 0);
    // SAFETY: valid non-toasted varlena.
    let s = String::from_utf8_lossy(unsafe { varlena_bytes(p) }).into_owned();
    PointerGetDatum(cstring_to_text(&quote_literal_cstr(&s)).cast::<u8>())
}

/// PG `quote_nullable`: like `quote_literal`, but NULL becomes the text `NULL`.
pub fn quote_nullable(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    if PG_ARGISNULL(fcinfo, 0) {
        return PointerGetDatum(cstring_to_text("NULL").cast::<u8>());
    }
    quote_literal(fcinfo)
}

// ===========================================================================
//   format() -- the SUS-printf-style formatter (%s %I %L %%, %n$, width, *)
// ===========================================================================

const TEXT_FORMAT_FLAG_MINUS: u32 = 0x0001;

/// One parsed argument for the format machinery: value already stringified via
/// the type's output function, plus its null flag. The type-OID resolution the
/// C does per-arg (`get_fn_expr_argtype`) is not yet available (its dependency
/// `get_call_expr_argtype` is a stub), so the executable entry point stages
/// there; the format-spec engine itself is exercised through
/// [`text_format_impl`] with pre-resolved args in tests.
struct FormatArg {
    /// The value's output-function text, or None when the arg is NULL.
    str_value: Option<String>,
}

/// PG `text_format`: `format(fmtstr, args...)`.
pub fn text_format(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    if PG_ARGISNULL(fcinfo, 0) {
        fcinfo.isnull = true;
        return Datum(0);
    }
    let fmt_ptr = pg_getarg_varlena(fcinfo, 0);
    // SAFETY: valid non-toasted varlena.
    let fmt = String::from_utf8_lossy(unsafe { varlena_bytes(fmt_ptr) }).into_owned();

    // Resolve the (already-stringified) positional arguments. The per-arg type
    // OID comes from get_fn_expr_argtype, whose dependency is still a stub, so
    // this collection reaches unimplemented until that lands (STAGED).
    let nargs = PG_NARGS(fcinfo) as usize;
    let args: Vec<FormatArg> = (1..nargs)
        .map(|i| {
            if PG_ARGISNULL(fcinfo, i) {
                FormatArg { str_value: None }
            } else {
                let typid = format_argtype(fcinfo, i);
                let (typoutput, _varlena) =
                    crate::utils::lsyscache::getTypeOutputInfo(typid);
                let value = fcinfo.args[i].value;
                FormatArg {
                    str_value: Some(crate::backend::utils::fmgr::fmgr::OidOutputFunctionCall(
                        typoutput, value,
                    )),
                }
            }
        })
        .collect();

    let result = text_format_impl(&fmt, &args);
    PointerGetDatum(cstring_to_text(&result).cast::<u8>())
}

/// PG `text_format_nv`: non-variadic wrapper (opr_sanity requires the shared C
/// function to accept the same arg count across catalog entries).
pub fn text_format_nv(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    text_format(fcinfo)
}

/// Resolve a format() argument's type OID. C: `get_fn_expr_argtype`; its
/// dependency `get_call_expr_argtype` is not translated yet, so this stages.
fn format_argtype(fcinfo: &mut FunctionCallInfoBaseData, argnum: usize) -> Oid {
    let Some(fn_info) = fcinfo.flinfo.as_deref_mut() else {
        return crate::postgres_ext::InvalidOid;
    };
    crate::backend::utils::fmgr::fmgr::get_fn_expr_argtype(fn_info, argnum as i32)
}

/// The format() spec engine, operating on already-stringified args. Faithful to
/// `text_format`: handles `%%`, `%s`/`%I`/`%L`, positional `%n$`, flags (`-`),
/// direct and indirect (`*`, `*n$`) width. `args[0]` is format() argument 1.
fn text_format_impl(fmt: &str, args: &[FormatArg]) -> String {
    let bytes = fmt.as_bytes();
    let end = bytes.len();
    let mut out = String::new();
    let mut arg = 1usize; // next argument position to print (1-based)
    let nargs = args.len() + 1; // arg 0 is the format string itself
    let mut cp = 0usize;

    while cp < end {
        let c = bytes[cp];
        if c != b'%' {
            // Copy a whole UTF-8 char through untouched.
            let ch_len = utf8_len(bytes, cp);
            out.push_str(&fmt[cp..cp + ch_len]);
            cp += ch_len;
            continue;
        }
        // At '%'; advance.
        cp = advance_parse_pointer(cp, end);

        // Easy case: %% outputs a single %.
        if bytes[cp] == b'%' {
            out.push('%');
            cp += 1;
            continue;
        }

        // Parse [argpos][flags][width].
        let parsed = text_format_parse_format(bytes, cp, end);
        cp = parsed.next;
        let (argpos, widthpos, flags, mut width) = (parsed.argpos, parsed.widthpos, parsed.flags, parsed.width);

        // Validate the conversion char before fetching arguments.
        let conv = bytes[cp];
        if conv != b's' && conv != b'I' && conv != b'L' {
            let ch_len = utf8_len(bytes, cp);
            let spec = &fmt[cp..cp + ch_len];
            ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
                e.errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                    .errmsg(format!("unrecognized format() type specifier \"{spec}\""))
                    .errhint("For a single \"%\" use \"%%\".");
            });
            unreachable!()
        }

        // Indirect width.
        if widthpos >= 0 {
            if widthpos > 0 {
                arg = widthpos as usize;
            }
            if arg >= nargs {
                too_few_arguments();
            }
            let val = format_arg_as_i32(&args[arg - 1]);
            arg += 1;
            width = val;
        }

        // Select the value argument.
        if argpos > 0 {
            arg = argpos as usize;
        }
        if arg >= nargs {
            too_few_arguments();
        }
        let value = &args[arg - 1];
        arg += 1;

        text_format_string_conversion(&mut out, conv, value, flags, width);
        // Advance past the (single-byte) conversion char; C's for-loop cp++.
        cp += 1;
    }
    out
}

/// C: interpret a width argument that came from an integer-typed value. Here
/// the value is already stringified; NULL width is treated as zero.
fn format_arg_as_i32(a: &FormatArg) -> i32 {
    a.str_value
        .as_ref()
        .map_or(0, |s| s.trim().parse::<i32>().unwrap_or(0))
}

/// C: `too few arguments for format()`.
fn too_few_arguments() -> ! {
    ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
        e.errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg("too few arguments for format()");
    });
    unreachable!()
}

/// C: `ADVANCE_PARSE_POINTER` -- step past one byte, erroring if the spec is
/// unterminated.
fn advance_parse_pointer(ptr: usize, end: usize) -> usize {
    let next = ptr + 1;
    if next >= end {
        ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg("unterminated format() type specifier")
                .errhint("For a single \"%\" use \"%%\".");
        });
        unreachable!()
    }
    next
}

/// Length in bytes of the UTF-8 char starting at `bytes[i]`.
fn utf8_len(bytes: &[u8], i: usize) -> usize {
    let b = bytes[i];
    let len = if b < 0x80 {
        1
    } else if b >> 5 == 0b110 {
        2
    } else if b >> 4 == 0b1110 {
        3
    } else if b >> 3 == 0b11110 {
        4
    } else {
        1
    };
    len.min(bytes.len() - i)
}

struct ParsedFormat {
    next: usize,
    argpos: i32,
    widthpos: i32,
    flags: u32,
    width: i32,
}

/// C: `text_format_parse_digits` -- parse contiguous decimal digits; returns the
/// value and whether any digits were parsed. Advances `*cp`.
fn text_format_parse_digits(bytes: &[u8], cp: &mut usize, end: usize) -> Option<i32> {
    let mut found = false;
    let mut val: i32 = 0;
    while bytes[*cp].is_ascii_digit() {
        let digit = i32::from(bytes[*cp] - b'0');
        let overflow = val
            .checked_mul(10)
            .and_then(|v| v.checked_add(digit));
        let Some(v) = overflow else {
            ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
                e.errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
                    .errmsg("number is out of range");
            });
            unreachable!()
        };
        val = v;
        *cp = advance_parse_pointer(*cp, end);
        found = true;
    }
    found.then_some(val)
}

/// C: `text_format_parse_format` -- parse `[argpos][flags][width]` after the
/// leading `%`. Returns parse results with the type char position in `next`.
fn text_format_parse_format(bytes: &[u8], start: usize, end: usize) -> ParsedFormat {
    let mut cp = start;
    let mut argpos = -1i32;
    let mut widthpos = -1i32;
    let mut flags = 0u32;
    let mut width = 0i32;

    // First number: either a width or an argument position (if followed by $).
    if let Some(n) = text_format_parse_digits(bytes, &mut cp, end) {
        if bytes[cp] != b'$' {
            return ParsedFormat { next: cp, argpos, widthpos: -1, flags, width: n };
        }
        argpos = n;
        if n == 0 {
            format_argument_0();
        }
        cp = advance_parse_pointer(cp, end);
    }

    // Flags (only minus is supported).
    while bytes[cp] == b'-' {
        flags |= TEXT_FORMAT_FLAG_MINUS;
        cp = advance_parse_pointer(cp, end);
    }

    if bytes[cp] == b'*' {
        // Indirect width.
        cp = advance_parse_pointer(cp, end);
        if let Some(n) = text_format_parse_digits(bytes, &mut cp, end) {
            if bytes[cp] != b'$' {
                ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
                    e.errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                        .errmsg("width argument position must be ended by \"$\"");
                });
                unreachable!()
            }
            widthpos = n;
            if n == 0 {
                format_argument_0();
            }
            cp = advance_parse_pointer(cp, end);
        } else {
            widthpos = 0; // width's argument position is unspecified
        }
    } else if let Some(n) = text_format_parse_digits(bytes, &mut cp, end) {
        width = n;
    }

    ParsedFormat { next: cp, argpos, widthpos, flags, width }
}

/// C: refuse an explicit argument index of 0.
fn format_argument_0() -> ! {
    ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
        e.errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg("format specifies argument 0, but arguments are numbered from 1");
    });
    unreachable!()
}

/// C: `text_format_string_conversion` -- format one `%s`/`%I`/`%L`, handling
/// NULLs then escaping.
fn text_format_string_conversion(
    out: &mut String,
    conversion: u8,
    value: &FormatArg,
    flags: u32,
    width: i32,
) {
    let Some(str_value) = &value.str_value else {
        // NULL argument.
        match conversion {
            b's' => text_format_append_string(out, "", flags, width),
            b'L' => text_format_append_string(out, "NULL", flags, width),
            _ => {
                // 'I'
                ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
                    e.errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED)
                        .errmsg("null values cannot be formatted as an SQL identifier");
                });
                unreachable!()
            }
        }
        return;
    };

    match conversion {
        b'I' => text_format_append_string(out, &quote_identifier(str_value), flags, width),
        b'L' => text_format_append_string(out, &quote_literal_cstr(str_value), flags, width),
        _ => text_format_append_string(out, str_value, flags, width),
    }
}

/// C: `text_format_append_string` -- append `str`, padding per flags/width.
fn text_format_append_string(out: &mut String, s: &str, flags: u32, width: i32) {
    if width == 0 {
        out.push_str(s);
        return;
    }
    let mut align_to_left = false;
    let mut width = width;
    if width < 0 {
        align_to_left = true;
        let Some(w) = width.checked_neg() else {
            ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
                e.errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
                    .errmsg("number is out of range");
            });
            unreachable!()
        };
        width = w;
    } else if flags & TEXT_FORMAT_FLAG_MINUS != 0 {
        align_to_left = true;
    }
    let len = s.chars().count() as i32;
    let pad = if len < width { (width - len) as usize } else { 0 };
    if align_to_left {
        out.push_str(s);
        out.extend(std::iter::repeat_n(' ', pad));
    } else {
        out.extend(std::iter::repeat_n(' ', pad));
        out.push_str(s);
    }
}

// ===========================================================================
//   parse_ident (misc.c) -- returns text[] (array output STAGED)
// ===========================================================================

/// PG `is_ident_start` (misc.c): valid identifier start byte.
fn is_ident_start(c: u8) -> bool {
    c == b'_' || c.is_ascii_alphabetic() || (c & 0x80) != 0
}

/// PG `is_ident_cont` (misc.c): valid identifier continuation byte.
fn is_ident_cont(c: u8) -> bool {
    c.is_ascii_digit() || c == b'$' || is_ident_start(c)
}

/// PG `parse_ident`: split a possibly-qualified identifier into its parts,
/// applying quoting/downcasing rules; returns a text[]. The identifier scanner
/// is translated fully; the array *result* construction (`accumArrayResult` /
/// `makeArrayResult`) is still stubbed, so building the return value stages.
pub fn parse_ident(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let qualname_ptr = pg_getarg_varlena(fcinfo, 0);
    let strict = DatumGetBool(fcinfo.args[1].value);
    // SAFETY: valid non-toasted varlena.
    let original = String::from_utf8_lossy(unsafe { varlena_bytes(qualname_ptr) }).into_owned();
    let parts = parse_ident_parts(&original, strict);

    // Materialize a text[] from the parsed parts. accumArrayResult /
    // makeArrayResult are still stubs (array subsystem deferred), so building
    // the return value stages here even though the scan above is complete.
    let rcontext: crate::utils::palloc::MemoryContext = core::ptr::null_mut();
    let mut astate: *mut crate::utils::array::ArrayBuildState = core::ptr::null_mut();
    for part in parts {
        let d = PointerGetDatum(cstring_to_text(&part).cast::<u8>());
        astate = crate::utils::array::accumArrayResult(
            astate,
            d,
            false,
            crate::catalog::genbki::TEXTOID,
            rcontext,
        );
    }
    // SAFETY: accumArrayResult returns a live state pointer (once implemented).
    crate::utils::array::makeArrayResult(unsafe { &mut *astate }, rcontext)
}

/// The identifier scanner behind `parse_ident`; returns the downcased/unquoted
/// parts, raising the same errors PG does for malformed input.
fn parse_ident_parts(qualname: &str, strict: bool) -> Vec<String> {
    let bytes = qualname.as_bytes();
    let mut parts: Vec<String> = Vec::new();
    let mut i = 0usize;
    let n = bytes.len();
    let is_space = |b: u8| crate::backend::parser::scansup::scanner_isspace(b);

    while i < n && is_space(bytes[i]) {
        i += 1;
    }

    let mut after_dot = false;
    loop {
        let mut missing_ident = true;

        if i < n && bytes[i] == b'"' {
            // Quoted identifier.
            let mut curname = String::new();
            i += 1;
            loop {
                let Some(rel) = bytes[i..].iter().position(|&b| b == b'"') else {
                    ident_error(qualname, Some("String has unclosed double quotes."));
                };
                let endp = i + rel;
                curname.push_str(&qualname[i..endp]);
                if endp + 1 < n && bytes[endp + 1] == b'"' {
                    // Doubled quote -> literal quote.
                    curname.push('"');
                    i = endp + 2;
                } else {
                    i = endp + 1;
                    break;
                }
            }
            if curname.is_empty() {
                ident_error(qualname, Some("Quoted identifier must not be empty."));
            }
            parts.push(curname);
            missing_ident = false;
        } else if i < n && is_ident_start(bytes[i]) {
            let start = i;
            i += 1;
            while i < n && is_ident_cont(bytes[i]) {
                i += 1;
            }
            let raw = &qualname[start..i];
            let downname =
                crate::backend::parser::scansup::downcase_identifier(raw, raw.len() as i32, false, false);
            parts.push(downname);
            missing_ident = false;
        }

        if missing_ident {
            if i < n && bytes[i] == b'.' {
                ident_error(qualname, Some("No valid identifier before \".\"."));
            } else if after_dot {
                ident_error(qualname, Some("No valid identifier after \".\"."));
            } else {
                ident_error(qualname, None);
            }
        }

        while i < n && is_space(bytes[i]) {
            i += 1;
        }

        if i < n && bytes[i] == b'.' {
            after_dot = true;
            i += 1;
            while i < n && is_space(bytes[i]) {
                i += 1;
            }
        } else if i >= n {
            break;
        } else {
            if strict {
                ident_error(qualname, None);
            }
            break;
        }
    }
    parts
}

/// C: the `parse_ident` "not a valid identifier" error, with optional detail.
fn ident_error(qualname: &str, detail: Option<&str>) -> ! {
    let msg = format!("string is not a valid identifier: \"{qualname}\"");
    match detail {
        Some(d) => {
            let d = d.to_owned();
            ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
                e.errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                    .errmsg(msg.clone())
                    .errdetail(d.clone());
            });
        }
        None => {
            ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
                e.errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                    .errmsg(msg.clone());
            });
        }
    }
    unreachable!()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::postgres::NullableDatum;

    fn fc(args: &[Datum]) -> FunctionCallInfoBaseData {
        FunctionCallInfoBaseData {
            flinfo: None,
            context: None,
            resultinfo: None,
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

    // ---- new string-breadth funcs ----

    fn i32_datum(n: i32) -> Datum {
        Int32GetDatum(n)
    }

    #[test]
    fn reverse_left_right() {
        assert_eq!(run_out(text_reverse(&mut fc(&[text_datum("abcde")]))), "edcba");
        // multibyte reverse
        assert_eq!(
            run_out(text_reverse(&mut fc(&[text_datum("\u{4f60}\u{597d}")]))),
            "\u{597d}\u{4f60}"
        );
        // left: positive + negative
        assert_eq!(run_out(text_left(&mut fc(&[text_datum("abcde"), i32_datum(2)]))), "ab");
        assert_eq!(run_out(text_left(&mut fc(&[text_datum("abcde"), i32_datum(-2)]))), "abc");
        // right: positive + negative
        assert_eq!(run_out(text_right(&mut fc(&[text_datum("abcde"), i32_datum(2)]))), "de");
        assert_eq!(run_out(text_right(&mut fc(&[text_datum("abcde"), i32_datum(-2)]))), "cde");
    }

    #[test]
    fn repeat_and_trim() {
        assert_eq!(run_out(text_repeat(&mut fc(&[text_datum("ab"), i32_datum(3)]))), "ababab");
        assert_eq!(run_out(text_repeat(&mut fc(&[text_datum("x"), i32_datum(-1)]))), "");
        assert_eq!(run_out(btrim(&mut fc(&[text_datum("xxhixx"), text_datum("x")]))), "hi");
        assert_eq!(run_out(ltrim(&mut fc(&[text_datum("xxhixx"), text_datum("x")]))), "hixx");
        assert_eq!(run_out(rtrim(&mut fc(&[text_datum("xxhixx"), text_datum("x")]))), "xxhi");
        assert_eq!(run_out(btrim1(&mut fc(&[text_datum("  hi  ")]))), "hi");
    }

    #[test]
    fn split_part_cases() {
        let s = text_datum("a,b,c");
        assert_eq!(run_out(split_part(&mut fc(&[s, text_datum(","), i32_datum(2)]))), "b");
        let s = text_datum("a,b,c");
        assert_eq!(run_out(split_part(&mut fc(&[s, text_datum(","), i32_datum(-1)]))), "c");
        let s = text_datum("a,b,c");
        assert_eq!(run_out(split_part(&mut fc(&[s, text_datum(","), i32_datum(9)]))), "");
    }

    #[test]
    fn replace_and_translate() {
        assert_eq!(
            run_out(replace_text(&mut fc(&[
                text_datum("hello world"),
                text_datum("o"),
                text_datum("0"),
            ]))),
            "hell0 w0rld"
        );
        assert_eq!(
            run_out(translate(&mut fc(&[
                text_datum("12345"),
                text_datum("143"),
                text_datum("ax"),
            ]))),
            "a2x5" // 1->a, 4->x, 3->(deleted, no "to")
        );
    }

    #[test]
    fn position_and_to_hex() {
        assert_eq!(
            DatumGetInt32(textpos(&mut fc(&[text_datum("high"), text_datum("ig")]))),
            2
        );
        assert_eq!(
            DatumGetInt32(textpos(&mut fc(&[text_datum("high"), text_datum("z")]))),
            0
        );
        assert_eq!(run_out(to_hex32(&mut fc(&[i32_datum(255)]))), "ff");
        assert_eq!(run_out(to_bin32(&mut fc(&[i32_datum(5)]))), "101");
        assert_eq!(run_out(to_oct32(&mut fc(&[i32_datum(64)]))), "100");
        // negative -> unsigned wrap
        assert_eq!(run_out(to_hex32(&mut fc(&[i32_datum(-1)]))), "ffffffff");
    }

    #[test]
    fn ascii_chr_quote() {
        assert_eq!(DatumGetInt32(ascii(&mut fc(&[text_datum("Ab")]))), 65);
        assert_eq!(run_out(chr(&mut fc(&[i32_datum(65)]))), "A");
        // quote_ident: safe vs needs-quote
        assert_eq!(run_out(quote_ident(&mut fc(&[text_datum("foo")]))), "foo");
        assert_eq!(run_out(quote_ident(&mut fc(&[text_datum("Foo")]))), "\"Foo\"");
        assert_eq!(run_out(quote_ident(&mut fc(&[text_datum("select")]))), "\"select\"");
        // quote_literal: doubling + backslash prefix
        assert_eq!(run_out(quote_literal(&mut fc(&[text_datum("it's")]))), "'it''s'");
        assert_eq!(run_out(quote_literal(&mut fc(&[text_datum("a\\b")]))), "E'a\\\\b'");
    }

    fn fa(s: Option<&str>) -> FormatArg {
        FormatArg { str_value: s.map(str::to_owned) }
    }

    #[test]
    fn format_basic() {
        assert_eq!(text_format_impl("Hello %s", &[fa(Some("World"))]), "Hello World");
        assert_eq!(text_format_impl("100%%", &[]), "100%");
        // %I identifier + %L literal
        assert_eq!(text_format_impl("%I", &[fa(Some("Foo"))]), "\"Foo\"");
        assert_eq!(text_format_impl("%L", &[fa(Some("it's"))]), "'it''s'");
        // %L on NULL -> NULL; %s on NULL -> empty
        assert_eq!(text_format_impl("%L", &[fa(None)]), "NULL");
        assert_eq!(text_format_impl("[%s]", &[fa(None)]), "[]");
    }

    #[test]
    fn format_positional_and_width() {
        // positional
        assert_eq!(
            text_format_impl("%2$s %1$s", &[fa(Some("a")), fa(Some("b"))]),
            "b a"
        );
        // right-justify width
        assert_eq!(text_format_impl("[%5s]", &[fa(Some("ab"))]), "[   ab]");
        // left-justify with minus flag
        assert_eq!(text_format_impl("[%-5s]", &[fa(Some("ab"))]), "[ab   ]");
        // indirect width via *
        assert_eq!(
            text_format_impl("[%*s]", &[fa(Some("4")), fa(Some("xy"))]),
            "[  xy]"
        );
    }

    #[test]
    fn fmgr_table_binds_split_part() {
        use crate::utils::fmgrtab::fmgr_builtins;
        let entry = fmgr_builtins
            .iter()
            .find(|b| b.func_name == "split_part")
            .expect("split_part present");
        let func = entry.func.expect("split_part bound");
        let mut f = fc(&[text_datum("x-y-z"), text_datum("-"), i32_datum(2)]);
        assert_eq!(run_out(func(&mut f)), "y");
    }

    #[test]
    fn fmgr_table_binds_to_hex() {
        use crate::utils::fmgrtab::fmgr_builtins;
        let entry = fmgr_builtins
            .iter()
            .find(|b| b.func_name == "to_hex32")
            .expect("to_hex32 present");
        let func = entry.func.expect("to_hex32 bound");
        let mut f = fc(&[i32_datum(4096)]);
        assert_eq!(run_out(func(&mut f)), "1000");
    }
}
