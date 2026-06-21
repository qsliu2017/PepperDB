//! Translation of postgres/src/backend/utils/adt/quote.c
//!
//! SQL string-quoting functions: quote_ident / quote_literal / quote_nullable
//! (+ the C-callable helper quote_literal_cstr).
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! `#include`s mapped: utils/builtins.h (cstring/text helpers via crate::utils::adt::
//! varlena), varatt.h -> crate::varatt.  SQL_STR_DOUBLE / ESCAPE_STRING_SYNTAX come
//! from c.h (crate::c).
//!
//! quote_ident calls quote_identifier(), which lives in utils/adt/ruleutils.c (not
//! yet translated), so quote_ident is STUBBED.

use crate::prelude::*;
use crate::utils::fmgr::*;
use crate::varatt::{pg_detoast_datum_packed, SET_VARSIZE, VARDATA, VARDATA_ANY, VARSIZE_ANY_EXHDR};
use crate::{PG_ARGISNULL, PG_GETARG_DATUM};
use crate::c::{text, ESCAPE_STRING_SYNTAX, SQL_STR_DOUBLE};
use crate::postgres::{DatumGetPointer, PointerGetDatum};
use crate::postgres_ext::InvalidOid;
use crate::utils::adt::varlena::{cstring_to_text, text_to_cstring};
use core::ffi::{c_char, c_void};

unsafe fn quote_identifier(_ident: *const c_char) -> *const c_char {
    unimplemented!("TODO(pg-port): wire utils/adt/ruleutils")
}

/*
 * quote_ident - returns a properly quoted identifier.
 */
pub unsafe fn quote_ident(fcinfo: FunctionCallInfo) -> Datum {
    let t: *mut text =
        pg_detoast_datum_packed(DatumGetPointer(PG_GETARG_DATUM!(fcinfo, 0)) as *mut c_void) as *mut text;
    let qstr: *const c_char;
    let str: *mut c_char;

    str = text_to_cstring(t);
    qstr = quote_identifier(str);
    return PointerGetDatum(cstring_to_text(qstr) as *const c_void); // PG_RETURN_TEXT_P
}

/*
 * quote_literal_internal - helper for quote_literal and quote_literal_cstr.
 *
 * # Safety
 * `dst` must have room for up to len*2 + 3 bytes; `src` is readable for `len` bytes.
 */
unsafe fn quote_literal_internal(dst: *mut c_char, mut src: *const c_char, mut len: usize) -> usize {
    let savedst = dst;
    let mut dst = dst;

    /* If any backslash is present, prefix with the E'' escape-string syntax. */
    let mut s = src;
    while s < src.add(len) {
        if *s as u8 == b'\\' {
            *dst = ESCAPE_STRING_SYNTAX as c_char;
            dst = dst.add(1);
            break;
        }
        s = s.add(1);
    }

    *dst = b'\'' as c_char;
    dst = dst.add(1);
    while len > 0 {
        len -= 1;
        if SQL_STR_DOUBLE(*src as u8, true) {
            *dst = *src;
            dst = dst.add(1);
        }
        *dst = *src;
        dst = dst.add(1);
        src = src.add(1);
    }
    *dst = b'\'' as c_char;
    dst = dst.add(1);

    dst.offset_from(savedst) as usize
}

/*
 * quote_literal - returns a properly quoted literal.
 */
pub unsafe fn quote_literal(fcinfo: FunctionCallInfo) -> Datum {
    let t: *mut text =
        pg_detoast_datum_packed(DatumGetPointer(PG_GETARG_DATUM!(fcinfo, 0)) as *mut c_void) as *mut text;
    let result: *mut text;
    let cp1: *const c_char;
    let cp2: *mut c_char;
    let len: usize;

    len = VARSIZE_ANY_EXHDR(t as *const c_char) as usize;
    /* worst-case result area; wasting a little space is OK */
    result = palloc(len * 2 + 3 + VARHDRSZ as usize) as *mut text;

    cp1 = VARDATA_ANY(t as *const c_char);
    cp2 = VARDATA(result as *const c_char);

    SET_VARSIZE(
        result as *mut c_char,
        VARHDRSZ + quote_literal_internal(cp2, cp1, len) as i32,
    );

    return PointerGetDatum(result as *const c_void); // PG_RETURN_TEXT_P
}

/*
 * quote_literal_cstr - returns a properly quoted literal (C string in/out).
 *
 * # Safety
 * `rawstr` is a valid NUL-terminated C string.
 */
pub unsafe fn quote_literal_cstr(rawstr: *const c_char) -> *mut c_char {
    let result: *mut c_char;
    let len: usize;
    let newlen: usize;

    len = strlen(rawstr);
    /* worst-case: doubling + two quotes + 'E' + NUL */
    result = palloc(len * 2 + 3 + 1) as *mut c_char;

    newlen = quote_literal_internal(result, rawstr, len);
    *result.add(newlen) = 0;

    result
}

/*
 * quote_nullable - properly-quoted literal, or the text 'NULL' for null input.
 */
pub unsafe fn quote_nullable(fcinfo: FunctionCallInfo) -> Datum {
    if PG_ARGISNULL!(fcinfo, 0) {
        return PointerGetDatum(cstring_to_text(c"NULL".as_ptr()) as *const c_void); // PG_RETURN_TEXT_P
    } else {
        // PG_RETURN_DATUM(DirectFunctionCall1(quote_literal, PG_GETARG_DATUM(0)))
        return DirectFunctionCall1Coll(quote_literal, InvalidOid, PG_GETARG_DATUM!(fcinfo, 0));
    }
}

unsafe fn strlen(s: *const c_char) -> usize {
    let mut n = 0usize;
    while *s.add(n) != 0 {
        n += 1;
    }
    n
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::postgres::DatumGetCString;
    use crate::utils::adt::varlena::{cstring_to_text as mk_text, text_to_cstring};
    use crate::utils::fmgr::DirectFunctionCall1Coll;

    unsafe fn out(d: Datum) -> std::string::String {
        let s = text_to_cstring(DatumGetPointer(d) as *const text);
        let mut n = 0usize;
        while *s.add(n) != 0 {
            n += 1;
        }
        std::string::String::from_utf8_lossy(core::slice::from_raw_parts(s as *const u8, n)).into_owned()
    }

    #[test]
    fn quote_literal_escapes() {
        unsafe {
            // plain string -> 'abc'
            let t = mk_text(c"abc".as_ptr());
            let q = DirectFunctionCall1Coll(quote_literal, InvalidOid, PointerGetDatum(t as *const c_void));
            assert_eq!(out(q), "'abc'");

            // embedded single quote is doubled -> 'a''b'
            let t = mk_text(c"a'b".as_ptr());
            let q = DirectFunctionCall1Coll(quote_literal, InvalidOid, PointerGetDatum(t as *const c_void));
            assert_eq!(out(q), "'a''b'");

            // backslash triggers E'' prefix and is doubled -> E'a\\b'
            let t = mk_text(c"a\\b".as_ptr());
            let q = DirectFunctionCall1Coll(quote_literal, InvalidOid, PointerGetDatum(t as *const c_void));
            assert_eq!(out(q), "E'a\\\\b'");

            // quote_literal_cstr round trip
            let c = quote_literal_cstr(c"x'y".as_ptr());
            let _ = DatumGetCString; // (silence unused import on some builds)
            let mut n = 0usize;
            while *c.add(n) != 0 {
                n += 1;
            }
            assert_eq!(core::slice::from_raw_parts(c as *const u8, n), b"'x''y'");
        }
    }
}
