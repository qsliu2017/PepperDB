//! Functions for the SQL character types `bpchar` (CHARACTER(n), blank-padded)
//! and `varchar` (CHARACTER VARYING(n)). Translated from
//! src/backend/utils/adt/varchar.c.
//!
//! Both piggyback on `text`'s varlena representation. `bpchar` blank-pads (and
//! trailing blanks are ignored in comparison, per SQL); `varchar` does not pad.
//!
//! Covers the I/O routines (`bpcharin`/`bpcharout`, `varcharin`/`varcharout`),
//! the length-coercion functions (`bpchar`/`varchar`, applied on an explicit or
//! implicit typmod cast), the typmod encode/decode (`bpchartypmodin`/`out`,
//! `varchartypmodin`/`out`), the cross-type casts (char<->bpchar, name<->bpchar),
//! and the bpchar comparison suite (which strips trailing blanks first).
//!
//! `atttypmod` for these types is `VARHDRSZ + n` (the declared length `n` plus the
//! 4-byte varlena header), matching PG's historical encoding.
//!
//! recv/send and the ICU/non-default-collation paths reach subsystems staged
//! elsewhere (rules.md s4). The C/default-collation memcmp path is what runs.

#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    reason = "faithful C width arithmetic: varchar.c does explicit int32 length \
              math and casts between byte counts and i32 lengths (the value-cast \
              family is an allowed port-inherent lint per rules.md s11)"
)]

use crate::backend::utils::adt::varlena::{cstring_to_text_with_len, varstr_cmp};
use crate::c::{text, NameData, NAMEDATALEN, VARHDRSZ};
use crate::catalog::genbki::C_COLLATION_OID;
use crate::ereturn;
use crate::fmgr::{FunctionCallInfoBaseData, PG_GET_COLLATION};
use crate::nodes::miscnodes::ErrorSaveContext;
use crate::nodes::nodes::Node;
use crate::postgres::{
    BoolGetDatum, CStringGetDatum, CharGetDatum, Datum, DatumGetBool, DatumGetChar,
    DatumGetCString, DatumGetInt32, DatumGetPointer, Int32GetDatum, NameGetDatum, PointerGetDatum,
};
use crate::postgres_ext::Oid;
use crate::utils::errcodes::ERRCODE_STRING_DATA_RIGHT_TRUNCATION;
use crate::varatt::{SET_VARSIZE, VARDATA, VARDATA_ANY, VARSIZE_ANY_EXHDR};

// ---------------------------------------------------------------------------
// Shared varlena helpers (mirror varlena.rs; palloc is a stub so we leak).
// ---------------------------------------------------------------------------

/// Allocate a leaked 4-byte-header varlena of `data.len()` payload bytes and copy
/// `data` in. Returns a pointer to the header. See varlena.rs::make_varlena.
fn make_varlena(data: &[u8]) -> *mut u8 {
    let total = data.len() + VARHDRSZ as usize;
    let mut buf = vec![0u8; total].into_boxed_slice();
    let ptr = buf.as_mut_ptr();
    // SAFETY: `ptr` heads a freshly-allocated `total`-byte buffer.
    unsafe {
        SET_VARSIZE(ptr, total as u32);
        if !data.is_empty() {
            core::ptr::copy_nonoverlapping(data.as_ptr(), VARDATA(ptr), data.len());
        }
    }
    Box::leak(buf).as_mut_ptr()
}

/// Borrow the payload bytes of any non-toasted varlena (4-byte or short header).
///
/// SAFETY: `p` must point at a valid, non-external/non-compressed varlena that
/// outlives the returned slice.
unsafe fn varlena_bytes<'a>(p: *mut u8) -> &'a [u8] {
    let len = VARSIZE_ANY_EXHDR(p);
    core::slice::from_raw_parts(VARDATA_ANY(p), len)
}

#[inline]
fn pg_getarg_varlena(fcinfo: &FunctionCallInfoBaseData, n: usize) -> *mut u8 {
    DatumGetPointer(fcinfo.args[n].value)
}

#[inline]
fn pg_getarg_cstring(fcinfo: &FunctionCallInfoBaseData, n: usize) -> String {
    let p = DatumGetCString(fcinfo.args[n].value);
    // SAFETY: an input function's cstring argument is a NUL-terminated C string.
    let cstr = unsafe { core::ffi::CStr::from_ptr(p) };
    cstr.to_string_lossy().into_owned()
}

#[inline]
fn pg_return_cstring(s: &str) -> Datum {
    let bytes: Vec<u8> = s.bytes().take_while(|&b| b != 0).collect();
    let c = std::ffi::CString::new(bytes).unwrap_or_default();
    CStringGetDatum(c.into_raw())
}

/// The call's soft-error context (`fcinfo->context` as `ErrorSaveContext`), or
/// `None` for the hard-error path (mirrors bool.rs/int.rs).
#[inline]
fn fcinfo_escontext(fcinfo: &mut FunctionCallInfoBaseData) -> Option<&mut ErrorSaveContext> {
    match fcinfo.context.as_deref_mut() {
        Some(Node::ErrorSaveContext(e)) => Some(e),
        _ => None,
    }
}

/// Number of CHARACTERS in the first `len` bytes of `s` (UTF-8; the mb path).
fn mbstrlen(s: &[u8]) -> usize {
    String::from_utf8_lossy(s).chars().count()
}

/// PG `pg_mbcharcliplen`: byte length of the leading `limit` CHARACTERS of `s`
/// (UTF-8). Returns the byte offset after `limit` chars, clamped to `s.len()`.
fn mbcharcliplen(s: &[u8], limit: usize) -> usize {
    let text = String::from_utf8_lossy(s);
    text.char_indices()
        .nth(limit)
        .map_or(s.len(), |(i, _)| i.min(s.len()))
}

// ===========================================================================
//   typmod encode / decode -- anychar_typmodin / anychar_typmodout
// ===========================================================================

/// PG `anychar_typmodout`: render a `bpchar`/`varchar` typmod as `(n)` (or empty
/// for an invalid typmod). The typmod is `VARHDRSZ + n`.
fn anychar_typmodout(typmod: i32) -> String {
    if typmod > VARHDRSZ {
        format!("({})", typmod - VARHDRSZ)
    } else {
        String::new()
    }
}

/// PG `bpchartypmodout`.
pub fn bpchartypmodout(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let typmod = DatumGetInt32(fcinfo.args[0].value);
    pg_return_cstring(&anychar_typmodout(typmod))
}

/// PG `varchartypmodout`.
pub fn varchartypmodout(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let typmod = DatumGetInt32(fcinfo.args[0].value);
    pg_return_cstring(&anychar_typmodout(typmod))
}

// bpchartypmodin / varchartypmodin take an `_int4` ArrayType of one element; the
// ArrayType-through-Datum marshalling is not wired here (the grammar path resolves
// typmods via typenameTypeMod's direct integer handling). Staged (rules.md s4).

/// PG `bpchartypmodin`: needs the ArrayType wire form. Staged.
pub fn bpchartypmodin(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("bpchartypmodin needs the _int4 ArrayType-through-Datum marshalling")
}

/// PG `varchartypmodin`: needs the ArrayType wire form. Staged.
pub fn varchartypmodin(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("varchartypmodin needs the _int4 ArrayType-through-Datum marshalling")
}

// ===========================================================================
//   bpchar (CHARACTER(n)) -- blank-padded
// ===========================================================================

/// PG `bpchar_input`: common guts of `bpcharin`/`bpcharrecv`. Applies `atttypmod`
/// (measured in characters); too-long input raises unless the excess are spaces
/// (then clipped). A soft error routes through `escontext`.
fn bpchar_input(
    s: &[u8],
    atttypmod: i32,
    escontext: Option<&mut ErrorSaveContext>,
) -> Option<*mut u8> {
    let len = s.len();
    let maxlen: usize;
    let mut effective_len = len;

    if atttypmod < VARHDRSZ {
        maxlen = len;
    } else {
        let declared = (atttypmod - VARHDRSZ) as usize; // chars
        let charlen = mbstrlen(s);
        if charlen > declared {
            let mbmaxlen = mbcharcliplen(s, declared);
            // Excess bytes must all be spaces.
            for &b in &s[mbmaxlen..len] {
                if b != b' ' {
                    ereturn!(escontext, None, |e: &mut crate::utils::elog::ErrorData| {
                        e.errcode(ERRCODE_STRING_DATA_RIGHT_TRUNCATION)
                            .errmsg(format!("value too long for type character({declared})"));
                    });
                }
            }
            effective_len = mbmaxlen;
            maxlen = mbmaxlen;
        } else {
            // maxlen = byte length needed = len + (declared - charlen) pad bytes.
            maxlen = len + (declared - charlen);
        }
    }

    let total = maxlen + VARHDRSZ as usize;
    let mut buf = vec![0u8; total].into_boxed_slice();
    let ptr = buf.as_mut_ptr();
    // SAFETY: fresh `total`-byte buffer; header + copy + pad stay in bounds.
    unsafe {
        SET_VARSIZE(ptr, total as u32);
        let data = VARDATA(ptr);
        core::ptr::copy_nonoverlapping(s.as_ptr(), data, effective_len);
        if maxlen > effective_len {
            core::ptr::write_bytes(data.add(effective_len), b' ', maxlen - effective_len);
        }
    }
    Some(Box::leak(buf).as_mut_ptr())
}

/// PG `bpcharin`: cstring -> CHARACTER internal representation.
pub fn bpcharin(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let s = pg_getarg_cstring(fcinfo, 0);
    let atttypmod = DatumGetInt32(fcinfo.args[2].value);
    let esc = fcinfo_escontext(fcinfo);
    // On a soft error `bpchar_input` returns None; the value is unused (the caller
    // checks SOFT_ERROR_OCCURRED).
    // On a soft error `bpchar_input` returns None; the value is unused then.
    #[allow(clippy::option_if_let_else, reason = "PointerGetDatum's arg type makes the map_or closure noisier than the if-let")]
    if let Some(p) = bpchar_input(s.as_bytes(), atttypmod, esc) {
        PointerGetDatum(p)
    } else {
        Datum(0)
    }
}

/// PG `bpcharout`: CHARACTER value -> cstring (shares text's conversion).
pub fn bpcharout(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let p = pg_getarg_varlena(fcinfo, 0);
    // SAFETY: valid non-toasted varlena.
    let bytes = unsafe { varlena_bytes(p) };
    pg_return_cstring(&String::from_utf8_lossy(bytes))
}

/// PG `bpcharrecv`: external binary -> bpchar. Staged (binary wire).
pub fn bpcharrecv(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("bpcharrecv needs the binary wire StringInfo (pq_getmsgtext) path")
}

/// PG `bpcharsend`: bpchar -> external binary (shares textsend).
pub fn bpcharsend(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    crate::backend::utils::adt::varlena::textsend(fcinfo)
}

/// PG `bpchar`: coerce a CHARACTER value to the size in `maxlen` (= declared
/// length + VARHDRSZ). `isExplicit` truncates silently; an implicit cast raises
/// unless the excess are spaces.
pub fn bpchar(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let source = pg_getarg_varlena(fcinfo, 0);
    let mut maxlen = DatumGetInt32(fcinfo.args[1].value);
    let is_explicit = DatumGetBool(fcinfo.args[2].value);

    if maxlen < VARHDRSZ {
        return PointerGetDatum(source);
    }
    maxlen -= VARHDRSZ; // declared char length

    // SAFETY: valid non-toasted varlena.
    let s = unsafe { varlena_bytes(source) };
    let len = s.len();
    let charlen = mbstrlen(s);
    let declared = maxlen as usize;

    if charlen == declared {
        return PointerGetDatum(source);
    }

    let (bytes, pad) = if charlen > declared {
        let maxmblen = mbcharcliplen(s, declared);
        if !is_explicit {
            for &b in &s[maxmblen..len] {
                if b != b' ' {
                    crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
                        e.errcode(ERRCODE_STRING_DATA_RIGHT_TRUNCATION)
                            .errmsg(format!("value too long for type character({declared})"));
                    });
                }
            }
        }
        (&s[..maxmblen], 0usize)
    } else {
        (s, declared - charlen)
    };

    let total = bytes.len() + pad;
    let mut out = Vec::with_capacity(total);
    out.extend_from_slice(bytes);
    out.resize(total, b' ');
    PointerGetDatum(make_varlena(&out))
}

/// PG `char_bpchar`: cast "char" -> bpchar(1).
pub fn char_bpchar(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let c = DatumGetChar(fcinfo.args[0].value) as u8;
    PointerGetDatum(make_varlena(&[c]))
}

/// PG `bpchar_name`: bpchar -> name (truncate to NAMEDATALEN-1, strip trailing
/// blanks, zero-pad).
pub fn bpchar_name(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let p = pg_getarg_varlena(fcinfo, 0);
    // SAFETY: valid non-toasted varlena.
    let s = unsafe { varlena_bytes(p) };
    let mut len = s.len();
    if len >= NAMEDATALEN {
        len = mbcharcliplen(s, NAMEDATALEN - 1);
    }
    // Remove trailing blanks.
    while len > 0 && s[len - 1] == b' ' {
        len -= 1;
    }
    let mut nd = Box::new(NameData { data: [0u8; NAMEDATALEN] });
    nd.data[..len].copy_from_slice(&s[..len]);
    // SAFETY: freshly leaked NameData we own.
    NameGetDatum(unsafe { &*Box::into_raw(nd) })
}

/// PG `name_bpchar`: name -> bpchar (shares text's conversion).
pub fn name_bpchar(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    // SAFETY: arg 0 is a valid Name.
    let nd = unsafe { &*crate::postgres::DatumGetName(fcinfo.args[0].value) };
    let end = nd.data.iter().position(|&b| b == 0).unwrap_or(NAMEDATALEN);
    PointerGetDatum(make_varlena(&nd.data[..end]))
}

// ---------------------------------------------------------------------------
// bpchar comparison (trailing blanks ignored).
// ---------------------------------------------------------------------------

/// PG `bpchartruelen`/`bcTruelen`: length of `arg`'s payload with trailing blanks
/// removed.
fn bctruelen(bytes: &[u8]) -> usize {
    let mut i = bytes.len();
    while i > 0 && bytes[i - 1] == b' ' {
        i -= 1;
    }
    i
}

/// PG `bpcharcmp`: 3-way comparison of two bpchar values (blank-insensitive).
fn bpchar_cmp_bytes(a: *mut u8, b: *mut u8, collid: Oid) -> i32 {
    // SAFETY: valid non-toasted varlenas.
    let (ba, bb) = unsafe { (varlena_bytes(a), varlena_bytes(b)) };
    let la = bctruelen(ba);
    let lb = bctruelen(bb);
    varstr_cmp(&ba[..la], &bb[..lb], collid)
}

/// PG `bpchareq`: equality (blank-insensitive; deterministic C-collation fast
/// path uses a length + memcmp check).
pub fn bpchareq(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let a = pg_getarg_varlena(fcinfo, 0);
    let b = pg_getarg_varlena(fcinfo, 1);
    // SAFETY: valid non-toasted varlenas.
    let (ba, bb) = unsafe { (varlena_bytes(a), varlena_bytes(b)) };
    let la = bctruelen(ba);
    let lb = bctruelen(bb);
    // Equal iff same blank-trimmed length AND equal bytes over that length.
    #[allow(
        clippy::suspicious_operation_groupings,
        reason = "the la==lb guard makes ba[..la] and bb[..lb] the same length; comparing to lb is intended"
    )]
    BoolGetDatum(la == lb && ba[..la] == bb[..lb])
}

/// PG `bpcharne`: inequality (blank-insensitive).
pub fn bpcharne(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let a = pg_getarg_varlena(fcinfo, 0);
    let b = pg_getarg_varlena(fcinfo, 1);
    // SAFETY: valid non-toasted varlenas.
    let (ba, bb) = unsafe { (varlena_bytes(a), varlena_bytes(b)) };
    let la = bctruelen(ba);
    let lb = bctruelen(bb);
    #[allow(
        clippy::suspicious_operation_groupings,
        reason = "the la!=lb short-circuit makes ba[..la]/bb[..lb] equal length; comparing to lb is intended"
    )]
    BoolGetDatum(la != lb || ba[..la] != bb[..lb])
}

macro_rules! bpchar_cmp_op {
    ($name:ident, $op:tt) => {
        #[doc = concat!("PG `", stringify!($name), "`: blank-insensitive bpchar comparison.")]
        pub fn $name(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
            let a = pg_getarg_varlena(fcinfo, 0);
            let b = pg_getarg_varlena(fcinfo, 1);
            BoolGetDatum(bpchar_cmp_bytes(a, b, PG_GET_COLLATION(fcinfo)) $op 0)
        }
    };
}

bpchar_cmp_op!(bpcharlt, <);
bpchar_cmp_op!(bpcharle, <=);
bpchar_cmp_op!(bpchargt, >);
bpchar_cmp_op!(bpcharge, >=);

/// PG `bpcharcmp`: btree 3-way comparison support for bpchar.
pub fn bpcharcmp(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let a = pg_getarg_varlena(fcinfo, 0);
    let b = pg_getarg_varlena(fcinfo, 1);
    Int32GetDatum(bpchar_cmp_bytes(a, b, PG_GET_COLLATION(fcinfo)))
}

/// PG `bpcharlen`: character length of a bpchar (trailing blanks counted, since
/// they are part of the stored value -- PG's `bpcharlen` uses the full length).
pub fn bpcharlen(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let p = pg_getarg_varlena(fcinfo, 0);
    // SAFETY: valid non-toasted varlena.
    let bytes = unsafe { varlena_bytes(p) };
    Int32GetDatum(mbstrlen(bytes) as i32)
}

// ===========================================================================
//   varchar (CHARACTER VARYING(n)) -- not padded
// ===========================================================================

/// PG `varchar_input`: common guts of `varcharin`/`varcharrecv`. Applies typmod
/// (in characters); too-long input raises unless the excess are spaces (clipped).
fn varchar_input(
    s: &[u8],
    atttypmod: i32,
    escontext: Option<&mut ErrorSaveContext>,
) -> Option<*mut u8> {
    let mut len = s.len();
    if atttypmod >= VARHDRSZ {
        let maxlen = (atttypmod - VARHDRSZ) as usize; // chars
        if mbstrlen(s) > maxlen {
            let mbmaxlen = mbcharcliplen(s, maxlen);
            for &b in &s[mbmaxlen..len] {
                if b != b' ' {
                    ereturn!(escontext, None, |e: &mut crate::utils::elog::ErrorData| {
                        e.errcode(ERRCODE_STRING_DATA_RIGHT_TRUNCATION)
                            .errmsg(format!("value too long for type character varying({maxlen})"));
                    });
                }
            }
            len = mbmaxlen;
        }
    }
    Some(make_varlena(&s[..len]))
}

/// PG `varcharin`: cstring -> varchar internal representation.
pub fn varcharin(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let s = pg_getarg_cstring(fcinfo, 0);
    let atttypmod = DatumGetInt32(fcinfo.args[2].value);
    let esc = fcinfo_escontext(fcinfo);
    #[allow(clippy::option_if_let_else, reason = "PointerGetDatum's arg type makes the map_or closure noisier than the if-let")]
    if let Some(p) = varchar_input(s.as_bytes(), atttypmod, esc) {
        PointerGetDatum(p)
    } else {
        Datum(0)
    }
}

/// PG `varcharout`: varchar value -> cstring (shares text's conversion).
pub fn varcharout(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let p = pg_getarg_varlena(fcinfo, 0);
    // SAFETY: valid non-toasted varlena.
    let bytes = unsafe { varlena_bytes(p) };
    pg_return_cstring(&String::from_utf8_lossy(bytes))
}

/// PG `varcharrecv`: external binary -> varchar. Staged (binary wire).
pub fn varcharrecv(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("varcharrecv needs the binary wire StringInfo (pq_getmsgtext) path")
}

/// PG `varcharsend`: varchar -> external binary (shares textsend).
pub fn varcharsend(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    crate::backend::utils::adt::varlena::textsend(fcinfo)
}

/// PG `varchar`: coerce a varchar value to the size in `maxlen` (= declared length
///
/// + VARHDRSZ). `isExplicit` truncates silently; an implicit cast raises unless
///   the excess are spaces.
pub fn varchar(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let source = pg_getarg_varlena(fcinfo, 0);
    let typmod = DatumGetInt32(fcinfo.args[1].value);
    let is_explicit = DatumGetBool(fcinfo.args[2].value);

    // SAFETY: valid non-toasted varlena.
    let s = unsafe { varlena_bytes(source) };
    let len = s.len();

    // No work if typmod invalid or the value already fits.
    if typmod < VARHDRSZ {
        return PointerGetDatum(source);
    }
    let maxlen = (typmod - VARHDRSZ) as usize;
    if mbstrlen(s) <= maxlen {
        return PointerGetDatum(source);
    }
    let maxmblen = mbcharcliplen(s, maxlen);
    if !is_explicit {
        for &b in &s[maxmblen..len] {
            if b != b' ' {
                crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
                    e.errcode(ERRCODE_STRING_DATA_RIGHT_TRUNCATION)
                        .errmsg(format!("value too long for type character varying({maxlen})"));
                });
            }
        }
    }
    PointerGetDatum(make_varlena(&s[..maxmblen]))
}

const _: fn() = || {
    // `text` is the documented shared payload type of these functions.
    let _ = core::mem::size_of::<text>();
    let _ = C_COLLATION_OID;
};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::postgres::{DatumGetBool, NullableDatum};

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

    fn bpchar_datum(s: &str) -> Datum {
        PointerGetDatum(make_varlena(s.as_bytes()))
    }

    fn out_to_string(d: Datum) -> String {
        let p = DatumGetCString(d);
        let cstr = unsafe { core::ffi::CStr::from_ptr(p) };
        cstr.to_string_lossy().into_owned()
    }

    fn typmod(n: i32) -> i32 {
        VARHDRSZ + n
    }

    #[test]
    fn bpchar_blank_pads() {
        // char(4) of 'a' -> 'a   '
        let mut f = fc(&[cstr_datum("a"), Datum(0), Int32GetDatum(typmod(4))]);
        let d = bpcharin(&mut f);
        let mut of = fc(&[d]);
        assert_eq!(out_to_string(bpcharout(&mut of)), "a   ");
    }

    #[test]
    fn bpchar_clips_trailing_spaces() {
        // char(4) of 'abcd    ' (4 real + spaces) -> 'abcd'
        let mut f = fc(&[cstr_datum("abcd  "), Datum(0), Int32GetDatum(typmod(4))]);
        let d = bpcharin(&mut f);
        let mut of = fc(&[d]);
        assert_eq!(out_to_string(bpcharout(&mut of)), "abcd");
    }

    #[test]
    fn bpchar_too_long_raises() {
        use std::panic::catch_unwind;
        let r = catch_unwind(|| {
            let mut f = fc(&[cstr_datum("abcde"), Datum(0), Int32GetDatum(typmod(4))]);
            bpcharin(&mut f)
        });
        assert!(r.is_err(), "value too long must raise");
    }

    #[test]
    fn bpchar_equality_blank_insensitive() {
        // 'abc' == 'abc ' under bpchar comparison.
        let a = bpchar_datum("abc");
        let b = bpchar_datum("abc ");
        assert!(DatumGetBool(bpchareq(&mut fc(&[a, b]))));
        let c = bpchar_datum("abd");
        assert!(DatumGetBool(bpcharne(&mut fc(&[a, c]))));
        assert!(DatumGetBool(bpcharlt(&mut fc(&[a, c]))));
    }

    #[test]
    fn varchar_no_pad_truncate_spaces() {
        // varchar(4) of 'ab' -> 'ab' (no padding)
        let mut f = fc(&[cstr_datum("ab"), Datum(0), Int32GetDatum(typmod(4))]);
        let d = varcharin(&mut f);
        let mut of = fc(&[d]);
        assert_eq!(out_to_string(varcharout(&mut of)), "ab");
        // varchar(4) of 'abcd  ' -> 'abcd' (trailing spaces clipped)
        let mut f = fc(&[cstr_datum("abcd  "), Datum(0), Int32GetDatum(typmod(4))]);
        let d = varcharin(&mut f);
        let mut of = fc(&[d]);
        assert_eq!(out_to_string(varcharout(&mut of)), "abcd");
    }

    #[test]
    fn varchar_too_long_raises() {
        use std::panic::catch_unwind;
        let r = catch_unwind(|| {
            let mut f = fc(&[cstr_datum("abcde"), Datum(0), Int32GetDatum(typmod(4))]);
            varcharin(&mut f)
        });
        assert!(r.is_err(), "value too long must raise");
    }

    #[test]
    fn typmod_out_renders_paren() {
        let mut f = fc(&[Int32GetDatum(typmod(4))]);
        assert_eq!(out_to_string(bpchartypmodout(&mut f)), "(4)");
        let mut f = fc(&[Int32GetDatum(-1)]);
        assert_eq!(out_to_string(varchartypmodout(&mut f)), "");
    }

    #[test]
    fn fmgr_table_binds_bpcharin() {
        use crate::utils::fmgrtab::fmgr_builtins;
        let entry = fmgr_builtins
            .iter()
            .find(|b| b.func_name == "bpcharin")
            .expect("bpcharin present");
        let func = entry.func.expect("bpcharin bound");
        let mut f = fc(&[cstr_datum("hi"), Datum(0), Int32GetDatum(typmod(4))]);
        let d = func(&mut f);
        let mut of = fc(&[d]);
        assert_eq!(out_to_string(bpcharout(&mut of)), "hi  ");
    }
}
