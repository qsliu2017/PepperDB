//! Translation of postgres/src/backend/utils/adt/varlena.c (in progress)
//!
//! Functions for the variable-length built-in types, plus the widely-used
//! cstring<->text conversion helpers.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! varatt.h VAR* helpers -> crate::varatt.  This is a LARGE source file; only the
//! cstring_to_text family and the core `text` I/O are translated so far.  The rest
//! (byteain/byteaout escape parsing [needs the standard_conforming_strings GUC],
//! text comparison/btree/collation, substring/position/overlay, the string-agg
//! aggregates, encode/decode, split/regexp, etc.) is STUBBED with TODO(pg-port).
//! text binary recv/send need mb/mbutils + pq_endtypsend (varatt/bytea), also TODO.

use crate::prelude::*;
use crate::utils::fmgr::*;
use crate::varatt::{
    pg_detoast_datum_packed, SET_VARSIZE, VARDATA, VARDATA_ANY, VARSIZE_ANY_EXHDR,
};
use crate::{PG_GETARG_DATUM, PG_GETARG_POINTER, PG_RETURN_CSTRING};
use crate::c::text;
use crate::postgres::{DatumGetPointer, PointerGetDatum};
use crate::lib::stringinfo::StringInfo;
use core::ffi::{c_char, c_int, c_void};

// libc strlen (string.h, via postgres.h).
unsafe fn strlen(s: *const c_char) -> usize {
    let mut n = 0usize;
    while *s.add(n) != 0 {
        n += 1;
    }
    n
}

/*
 * cstring_to_text
 *
 * Create a text value from a null-terminated C string.  Freshly palloc'd with a
 * full-size (4-byte) VARHDR.
 *
 * # Safety
 * `s` is a valid NUL-terminated C string.
 */
pub unsafe fn cstring_to_text(s: *const c_char) -> *mut text {
    cstring_to_text_with_len(s, strlen(s) as c_int)
}

/*
 * cstring_to_text_with_len
 *
 * Same as cstring_to_text except the caller specifies the string length; the
 * string need not be null-terminated.
 *
 * # Safety
 * `s` is readable for `len` bytes.
 */
pub unsafe fn cstring_to_text_with_len(s: *const c_char, len: c_int) -> *mut text {
    let result: *mut text = palloc((len + VARHDRSZ) as Size) as *mut text;

    SET_VARSIZE(result as *mut c_char, len + VARHDRSZ);
    core::ptr::copy_nonoverlapping(s, VARDATA(result as *const c_char), len as usize);

    result
}

/*
 * text_to_cstring
 *
 * Create a palloc'd, null-terminated C string from a text value.  Supports a
 * compressed or toasted text value (via pg_detoast_datum_packed).
 *
 * # Safety
 * `t` points to a valid text datum.
 */
pub unsafe fn text_to_cstring(t: *const text) -> *mut c_char {
    /* must cast away the const, unfortunately */
    let tunpacked: *mut text = pg_detoast_datum_packed(t as *mut c_void) as *mut text;
    let len = VARSIZE_ANY_EXHDR(tunpacked as *const c_char) as usize;
    let result: *mut c_char;

    result = palloc(len + 1) as *mut c_char;
    core::ptr::copy_nonoverlapping(VARDATA_ANY(tunpacked as *const c_char), result, len);
    *result.add(len) = 0;

    if tunpacked != t as *mut text {
        pfree(tunpacked as *mut c_void);
    }

    result
}

/// `TextDatumGetCString(d)` (a builtins.h macro) - text_to_cstring of a text Datum.
///
/// # Safety
/// `d` is a Datum holding a text pointer.
#[inline]
pub unsafe fn TextDatumGetCString(d: Datum) -> *mut c_char {
    text_to_cstring(DatumGetPointer(d) as *const text)
}

/*****************************************************************************
 *	 USER I/O ROUTINES														 *
 *****************************************************************************/

/*
 *		byteain			- converts from printable representation of byte array  [STUBBED]
 */
pub unsafe fn byteain(fcinfo: FunctionCallInfo) -> Datum {
    // TODO(pg-port): the bytea input parser needs the '\x'/octal-escape logic + the
    // standard_conforming_strings GUC (utils/guc) - not yet translated.
    let _ = fcinfo;
    unimplemented!("byteain: escape/hex parsing + GUC not yet translated")
}

/*
 *		byteaout			- converts to printable representation of byte array  [STUBBED]
 */
pub unsafe fn byteaout(fcinfo: FunctionCallInfo) -> Datum {
    // TODO(pg-port): needs the bytea_output GUC (escape vs hex) - not yet translated.
    let _ = fcinfo;
    unimplemented!("byteaout: bytea_output GUC not yet translated")
}

/*
 *		textin			- converts cstring to internal representation
 */
pub unsafe fn textin(fcinfo: FunctionCallInfo) -> Datum {
    let input_text: *mut c_char = PG_GETARG_DATUM!(fcinfo, 0) as *mut c_char; // PG_GETARG_CSTRING

    return PointerGetDatum(cstring_to_text(input_text) as *const c_void); // PG_RETURN_TEXT_P
}

/*
 *		textout			- converts internal representation to cstring
 */
pub unsafe fn textout(fcinfo: FunctionCallInfo) -> Datum {
    let txt: Datum = PG_GETARG_DATUM!(fcinfo, 0);

    PG_RETURN_CSTRING!(TextDatumGetCString(txt));
}

/*
 *		textrecv			- converts external binary format to text  [STUBBED]
 */
pub unsafe fn textrecv(fcinfo: FunctionCallInfo) -> Datum {
    let buf: StringInfo = PG_GETARG_POINTER!(fcinfo, 0) as StringInfo;
    // C: str = pq_getmsgtext(buf, buf->len - buf->cursor, &nbytes);
    //    PG_RETURN_TEXT_P(cstring_to_text_with_len(str, nbytes));
    // TODO(pg-port): pq_getmsgtext needs mb/mbutils (pg_client_to_server).
    let _ = buf;
    unimplemented!("textrecv: pq_getmsgtext (mb/mbutils) not yet translated")
}

/*
 *		textsend			- converts text to binary format  [STUBBED]
 */
pub unsafe fn textsend(fcinfo: FunctionCallInfo) -> Datum {
    let t: Datum = PG_GETARG_DATUM!(fcinfo, 0);
    // C: pq_begintypsend; pq_sendtext(&buf, VARDATA_ANY(t), VARSIZE_ANY_EXHDR(t)); PG_RETURN_BYTEA_P(pq_endtypsend);
    // TODO(pg-port): pq_sendtext (mbutils) + pq_endtypsend (varatt/bytea).
    let _ = t;
    unimplemented!("textsend: pq_sendtext/pq_endtypsend not yet translated")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::postgres::{CStringGetDatum, DatumGetCString};
    use crate::postgres_ext::InvalidOid;
    use crate::utils::fmgr::DirectFunctionCall1Coll;

    unsafe fn cstr_eq(p: *const c_char, want: &str) -> bool {
        let n = strlen(p);
        core::slice::from_raw_parts(p as *const u8, n) == want.as_bytes()
    }

    #[test]
    fn text_io_and_cstring_roundtrip() {
        unsafe {
            // cstring_to_text -> text_to_cstring round trip
            let t = cstring_to_text(c"hello, world".as_ptr());
            let back = text_to_cstring(t);
            assert!(cstr_eq(back, "hello, world"));

            // with_len (non-NUL-terminated slice of 5)
            let t2 = cstring_to_text_with_len(c"abcdefgh".as_ptr(), 5);
            let back2 = text_to_cstring(t2);
            assert!(cstr_eq(back2, "abcde"));

            // textin -> textout through the fmgr dispatch
            let d = DirectFunctionCall1Coll(textin, InvalidOid, CStringGetDatum(c"PepperDB".as_ptr()));
            let s = DatumGetCString(DirectFunctionCall1Coll(textout, InvalidOid, d));
            assert!(cstr_eq(s, "PepperDB"));

            // empty string
            let e = text_to_cstring(cstring_to_text(c"".as_ptr()));
            assert!(cstr_eq(e, ""));
        }
    }
}
