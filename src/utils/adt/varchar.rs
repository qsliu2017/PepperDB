//! Translation of postgres/src/backend/utils/adt/varchar.c
//!
//! Functions for the built-in types char(n) and varchar(n).  Both are varlena
//! types, binary-compatible with `text` (crate::c::{BpChar, VarChar} are aliases of
//! crate::c::varlena, like text/bytea).
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! `#include`s mapped:
//!   - common/hashfn.h          -> crate::common::hashfn (hash_any / hash_any_extended)
//!   - catalog/pg_type.h        -> crate::catalog::pg_type_d (BPCHAROID / VARCHAROID)
//!   - catalog/pg_collation.h   -> crate::catalog::pg_known_oids (C_COLLATION_OID)
//!   - utils/varlena.h          -> crate::utils::adt::varlena (cstring_to_text[_with_len],
//!                                  text_to_cstring, TextDatumGetCString)
//!   - varatt.h VAR* macros     -> crate::varatt (VARDATA / SET_VARSIZE / VARDATA_ANY /
//!                                  VARSIZE_ANY_EXHDR)
//!   - <string.h> memcpy/memset -> libc via extern "C"; <stdio.h> snprintf via extern "C".
//!
//! STUBBED (dependency not yet ported):
//!   - mb/pg_wchar.h: pg_mbstrlen_with_len / pg_mbcharcliplen / pg_mbcliplen /
//!     pg_database_encoding_max_length are NOT present in crate::mb (wchar.rs has only the
//!     per-encoding mblen converters, no mbutils.c).  Local stubs below assume a
//!     single-byte server encoding (bytes == chars), which is correct for SQL_ASCII / the
//!     C locale and keeps in/out/typmod/length-cast self-contained.  TODO(pg-port): wire to
//!     mb/mbutils.c once it lands.
//!   - libpq/pqformat.h: bpcharrecv/varcharrecv use pq_getmsgtext, *send use pq_sendtext;
//!     both need mb/mbutils + pqformat -> bpcharrecv/varcharrecv/bpcharsend/varcharsend STUBBED.
//!   - utils/array.h: ArrayGetIntegerTypmods (decoding the cstring[] typmod array) is not
//!     ported; anychar_typmodin's validation logic is translated in full but the array decode
//!     is a local TODO stub.
//!   - utils/pg_locale.h + varstr_cmp/varstr_sortsupport (collation-aware comparison) and
//!     pg_strnxfrm: the comparison operators bpchareq/ne/lt/le/gt/ge/cmp, bpchar_larger/
//!     bpchar_smaller, the hash functions' non-deterministic-collation branch,
//!     bpchar_sortsupport, btbpchar_pattern_sortsupport, and check_collation_set are STUBBED.
//!   - nodes/supportnodes.h + optimizer: varchar_support (planner length-coercion flattening)
//!     STUBBED.

use crate::prelude::*;
use crate::utils::fmgr::*;
use crate::varatt::{SET_VARSIZE, VARDATA, VARDATA_ANY, VARSIZE_ANY_EXHDR};
use crate::{
    IsA, PG_GETARG_BOOL, PG_GETARG_CHAR, PG_GETARG_DATUM, PG_GETARG_INT32, PG_GETARG_NAME,
    PG_GETARG_POINTER, PG_GET_COLLATION, PG_RETURN_CSTRING, PG_RETURN_INT32, PG_RETURN_NAME,
    PG_RETURN_POINTER,
};
use crate::c::{int32, BpChar, Name, NameData, VarChar};
use crate::pg_config::NAMEDATALEN;
use crate::catalog::pg_known_oids::C_COLLATION_OID;
use crate::catalog::pg_type_d::BPCHAROID;
use crate::common::hashfn::{hash_any, hash_any_extended};
use crate::nodes::nodes::Node;
use crate::nodes::supportnodes::SupportRequestSimplify;
use crate::nodes::primnodes::{Const, FuncExpr};
use crate::nodes::pg_list::{linitial, list_length, lsecond};
use crate::nodes::nodeFuncs::{exprTypmod, relabel_to_typmod};
use crate::postgres::{DatumGetInt32, DatumGetPointer, PointerGetDatum};
use crate::utils::adt::varlena::{cstring_to_text, cstring_to_text_with_len, TextDatumGetCString};
use crate::utils::sort::sortsupport::SortSupport;
use crate::libpq::pqformat::pq_getmsgtext;
use crate::lib::stringinfo::StringInfo;
use core::ffi::{c_char, c_int, c_void};

// <string.h> / <stdio.h> via postgres.h.
extern "C" {
    fn memcpy(dst: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
    fn memset(s: *mut c_void, c: c_int, n: usize) -> *mut c_void;
    fn memcmp(a: *const c_void, b: *const c_void, n: usize) -> c_int;
    fn strlen(s: *const c_char) -> usize;
    fn snprintf(s: *mut c_char, n: usize, fmt: *const c_char, ...) -> c_int;
}

/* errcodes.h classification (the errcode() shim ignores the value). */
const ERRCODE_INVALID_PARAMETER_VALUE: c_int = 0;
const ERRCODE_STRING_DATA_RIGHT_TRUNCATION: c_int = 0;
const ERRCODE_INDETERMINATE_COLLATION: c_int = 0;

/* access/htup_details.h: MaxAttrSize, the maximum length of a tuple attribute. */
const MaxAttrSize: int32 = 10 * 1024 * 1024;

// ----------------------------------------------------------------
//   mb/pg_wchar.h shims (single-byte / C-locale assumption).
//   TODO(pg-port): replace with mb/mbutils.c once translated.
// ----------------------------------------------------------------

/// pg_mbstrlen_with_len: number of CHARACTERS in `len` bytes starting at `mbstr`.
/// STUB: assumes a single-byte encoding, so character count == byte count.
///
/// # Safety
/// `mbstr` is readable for `len` bytes.
unsafe fn pg_mbstrlen_with_len(_mbstr: *const c_char, limit: c_int) -> c_int {
    // TODO(pg-port): real implementation must walk multibyte chars (mb/mbutils.c).
    limit
}

/// pg_mbcharcliplen: byte length of the longest prefix of `mbstr` (len bytes) that
/// contains at most `limit` CHARACTERS, never splitting a multibyte char.
/// STUB: single-byte encoding -> min(len, limit) bytes.
///
/// # Safety
/// `mbstr` is readable for `len` bytes.
unsafe fn pg_mbcharcliplen(_mbstr: *const c_char, len: c_int, limit: c_int) -> c_int {
    // TODO(pg-port): real implementation must respect multibyte boundaries (mb/mbutils.c).
    if len < limit {
        len
    } else {
        limit
    }
}

/// pg_mbcliplen: byte length of the longest prefix of `mbstr` (len bytes) that fits in
/// `limit` BYTES without splitting a multibyte char.  STUB: min(len, limit).
///
/// # Safety
/// `mbstr` is readable for `len` bytes.
unsafe fn pg_mbcliplen(_mbstr: *const c_char, len: c_int, limit: c_int) -> c_int {
    // TODO(pg-port): real implementation must respect multibyte boundaries (mb/mbutils.c).
    if len < limit {
        len
    } else {
        limit
    }
}

/// pg_database_encoding_max_length: max bytes per char in the server encoding.
/// STUB: 1 (single-byte encoding).
fn pg_database_encoding_max_length() -> c_int {
    // TODO(pg-port): real value comes from the active server encoding (mb/mbutils.c).
    1
}

// ----------------------------------------------------------------
//   utils/array.h shim
// ----------------------------------------------------------------

/// ArrayGetIntegerTypmods: decode the cstring[] passed to a typmodin function into an
/// array of int32 typmods, returning the count via `*n`.
///
/// TODO(pg-port): the array type machinery (utils/array.h) is not yet translated.
///
/// # Safety
/// `ta` points to an ArrayType datum; `n` is writable.
unsafe fn ArrayGetIntegerTypmods(ta: *mut c_void, n: *mut c_int) -> *mut int32 {
    let _ = (ta, n);
    unimplemented!("ArrayGetIntegerTypmods: utils/array.h not yet translated")
}

// ----------------------------------------------------------------
//   utils/varlena.h shims (collation-aware string comparison).
//   TODO(pg-port): varstr_cmp / varstr_sortsupport live in utils/adt/varlena.c
//   which is still mid-translation; replace these once it lands.
// ----------------------------------------------------------------

/// varstr_cmp: collation-aware comparison of two strings (utils/adt/varlena.c).
///
/// # Safety
/// `arg1`/`arg2` are readable for `len1`/`len2` bytes.
unsafe fn varstr_cmp(
    arg1: *const c_char,
    len1: c_int,
    arg2: *const c_char,
    len2: c_int,
    collid: Oid,
) -> c_int {
    let _ = (arg1, len1, arg2, len2, collid);
    unimplemented!("varstr_cmp: utils/adt/varlena.c not yet translated")
}

/// varstr_sortsupport: install generic string SortSupport (utils/adt/varlena.c).
///
/// # Safety
/// `ssup` points to a SortSupport node.
unsafe fn varstr_sortsupport(ssup: SortSupport, typid: Oid, collid: Oid) {
    let _ = (ssup, typid, collid);
    unimplemented!("varstr_sortsupport: utils/adt/varlena.c not yet translated")
}

// ----------------------------------------------------------------
//   utils/pg_locale.h shims.
//   TODO(pg-port): utils/adt/pg_locale.rs exists but is not yet wired into the
//   module tree; replace these once it lands.  Only the `deterministic` flag is
//   modelled here, which is all bpchar comparison/hashing reads.
// ----------------------------------------------------------------

#[repr(C)]
pub struct pg_locale_struct {
    pub deterministic: bool,
}
pub type pg_locale_t = *mut pg_locale_struct;

/// pg_newlocale_from_collation (utils/pg_locale.h).  Not yet wired.
unsafe fn pg_newlocale_from_collation(collid: Oid) -> pg_locale_t {
    let _ = collid;
    unimplemented!("pg_newlocale_from_collation: utils/adt/pg_locale.rs not yet wired")
}

/// pg_strnxfrm (utils/pg_locale.h).  Not yet wired.
///
/// # Safety
/// `src` is readable for `srclen` bytes; `dest` is writable for `destsize` bytes.
unsafe fn pg_strnxfrm(
    dest: *mut c_char,
    destsize: usize,
    src: *const c_char,
    srclen: isize,
    locale: pg_locale_t,
) -> usize {
    let _ = (dest, destsize, src, srclen, locale);
    unimplemented!("pg_strnxfrm: utils/adt/pg_locale.rs not yet wired")
}

/* common code for bpchartypmodin and varchartypmodin */
unsafe fn anychar_typmodin(ta: *mut c_void, typename: *const c_char) -> int32 {
    let typmod: int32;
    let tl: *mut int32;
    let mut n: c_int = 0;

    tl = ArrayGetIntegerTypmods(ta, &mut n);

    /*
     * we're not too tense about good error message here because grammar
     * shouldn't allow wrong number of modifiers for CHAR
     */
    if n != 1 {
        let _ = errcode(ERRCODE_INVALID_PARAMETER_VALUE);
        ereport!(ERROR, errmsg!("invalid type modifier"));
    }

    if *tl < 1 {
        let _ = errcode(ERRCODE_INVALID_PARAMETER_VALUE);
        ereport!(
            ERROR,
            errmsg!("length for type {} must be at least 1", cstr(typename))
        );
    }
    if *tl > MaxAttrSize {
        let _ = errcode(ERRCODE_INVALID_PARAMETER_VALUE);
        ereport!(
            ERROR,
            errmsg!(
                "length for type {} cannot exceed {}",
                cstr(typename),
                MaxAttrSize
            )
        );
    }

    /*
     * For largely historical reasons, the typmod is VARHDRSZ plus the number
     * of characters; there is enough client-side code that knows about that
     * that we'd better not change it.
     */
    typmod = VARHDRSZ + *tl;

    typmod
}

/* common code for bpchartypmodout and varchartypmodout */
unsafe fn anychar_typmodout(typmod: int32) -> *mut c_char {
    let res: *mut c_char = palloc(64) as *mut c_char;

    if typmod > VARHDRSZ {
        snprintf(
            res,
            64,
            c"(%d)".as_ptr(),
            (typmod - VARHDRSZ) as c_int,
        );
    } else {
        *res = b'\0' as c_char;
    }

    res
}

/*
 * CHAR() and VARCHAR() types are part of the SQL standard. CHAR()
 * is for blank-padded string whose length is specified in CREATE TABLE.
 * VARCHAR is for storing string whose length is at most the length specified
 * at CREATE TABLE time.
 *
 * We actually implement this as a varlena so that we don't have to pass in
 * the length for the comparison functions. (The difference between these
 * types and "text" is that we truncate and possibly blank-pad the string
 * at insertion time.)
 *
 *															  - ay 6/95
 */

/*****************************************************************************
 *	 bpchar - char()														 *
 *****************************************************************************/

/*
 * bpchar_input -- common guts of bpcharin and bpcharrecv
 *
 * s is the input text of length len (may not be null-terminated)
 * atttypmod is the typmod value to apply
 *
 * Note that atttypmod is measured in characters, which
 * is not necessarily the same as the number of bytes.
 *
 * If the input string is too long, raise an error, unless the extra
 * characters are spaces, in which case they're truncated.  (per SQL)
 *
 * # Safety
 * `s` is readable for `len` bytes.
 */
unsafe fn bpchar_input(s: *const c_char, mut len: usize, atttypmod: int32, escontext: *mut Node) -> *mut BpChar {
    let _ = escontext; // TODO(pg-port): ErrorSaveContext soft errors (ereturn -> hard ERROR).
    let result: *mut BpChar;
    let r: *mut c_char;
    let mut maxlen: usize;

    /* If typmod is -1 (or invalid), use the actual string length */
    if atttypmod < VARHDRSZ {
        maxlen = len;
    } else {
        let charlen: usize; /* number of CHARACTERS in the input */

        maxlen = (atttypmod - VARHDRSZ) as usize;
        charlen = pg_mbstrlen_with_len(s, len as c_int) as usize;
        if charlen > maxlen {
            /* Verify that extra characters are spaces, and clip them off */
            let mbmaxlen = pg_mbcharcliplen(s, len as c_int, maxlen as c_int) as usize;
            let mut j: usize;

            /*
             * at this point, len is the actual BYTE length of the input
             * string, maxlen is the max number of CHARACTERS allowed for this
             * bpchar type, mbmaxlen is the length in BYTES of those chars.
             */
            j = mbmaxlen;
            while j < len {
                if *s.add(j) != b' ' as c_char {
                    let _ = errcode(ERRCODE_STRING_DATA_RIGHT_TRUNCATION);
                    ereport!(
                        ERROR,
                        errmsg!("value too long for type character({})", maxlen as c_int)
                    );
                }
                j += 1;
            }

            /*
             * Now we set maxlen to the necessary byte length, not the number
             * of CHARACTERS!
             */
            len = mbmaxlen;
            maxlen = mbmaxlen;
        } else {
            /*
             * Now we set maxlen to the necessary byte length, not the number
             * of CHARACTERS!
             */
            maxlen = len + (maxlen - charlen);
        }
    }

    result = palloc(maxlen + VARHDRSZ as usize) as *mut BpChar;
    SET_VARSIZE(result as *mut c_char, (maxlen + VARHDRSZ as usize) as int32);
    r = VARDATA(result as *const c_char);
    memcpy(r as *mut c_void, s as *const c_void, len);

    /* blank pad the string if necessary */
    if maxlen > len {
        memset(r.add(len) as *mut c_void, b' ' as c_int, maxlen - len);
    }

    result
}

/*
 * Convert a C string to CHARACTER internal representation.  atttypmod
 * is the declared length of the type plus VARHDRSZ.
 */
pub unsafe fn bpcharin(fcinfo: FunctionCallInfo) -> Datum {
    let s: *mut c_char = PG_GETARG_DATUM!(fcinfo, 0) as *mut c_char; // PG_GETARG_CSTRING(0)
    let atttypmod: int32 = PG_GETARG_INT32!(fcinfo, 2);
    let result: *mut BpChar;

    result = bpchar_input(s, strlen(s), atttypmod, (*fcinfo).context);
    return PointerGetDatum(result as *const c_void); // PG_RETURN_BPCHAR_P
}

/*
 * Convert a CHARACTER value to a C string.
 *
 * Uses the text conversion functions, which is only appropriate if BpChar
 * and text are equivalent types.
 */
pub unsafe fn bpcharout(fcinfo: FunctionCallInfo) -> Datum {
    let txt: Datum = PG_GETARG_DATUM!(fcinfo, 0);

    PG_RETURN_CSTRING!(TextDatumGetCString(txt));
}

/*
 *		bpcharrecv			- converts external binary format to bpchar
 */
pub unsafe fn bpcharrecv(fcinfo: FunctionCallInfo) -> Datum {
    let buf: StringInfo = PG_GETARG_POINTER!(fcinfo, 0) as StringInfo;
    let atttypmod: int32 = PG_GETARG_INT32!(fcinfo, 2);
    let result: *mut BpChar;
    let str: *mut c_char;
    let mut nbytes: c_int = 0;

    str = pq_getmsgtext(buf, (*buf).len - (*buf).cursor, &mut nbytes);
    result = bpchar_input(str, nbytes as usize, atttypmod, core::ptr::null_mut());
    pfree(str as *mut c_void);
    return PointerGetDatum(result as *const c_void); // PG_RETURN_BPCHAR_P
}

/*
 *		bpcharsend			- converts bpchar to binary format
 */
pub unsafe fn bpcharsend(fcinfo: FunctionCallInfo) -> Datum {
    /* Exactly the same as textsend, so share code */
    return crate::utils::adt::varlena::textsend(fcinfo);
}

/*
 * Converts a CHARACTER type to the specified size.
 *
 * maxlen is the typmod, ie, declared length plus VARHDRSZ bytes.
 * isExplicit is true if this is for an explicit cast to char(N).
 *
 * Truncation rules: for an explicit cast, silently truncate to the given
 * length; for an implicit cast, raise error unless extra characters are
 * all spaces.
 */
pub unsafe fn bpchar(fcinfo: FunctionCallInfo) -> Datum {
    let source: *mut BpChar =
        PG_GETARG_VARLENA_PP(fcinfo, 0) as *mut BpChar; // PG_GETARG_BPCHAR_PP(0)
    let mut maxlen: int32 = PG_GETARG_INT32!(fcinfo, 1);
    let isExplicit: bool = PG_GETARG_BOOL!(fcinfo, 2);
    let result: *mut BpChar;
    let mut len: int32;
    let r: *mut c_char;
    let s: *mut c_char;
    let mut i: int32;
    let charlen: int32; /* number of characters in the input string + VARHDRSZ */

    /* No work if typmod is invalid */
    if maxlen < VARHDRSZ {
        return PointerGetDatum(source as *const c_void); // PG_RETURN_BPCHAR_P
    }

    maxlen -= VARHDRSZ;

    len = VARSIZE_ANY_EXHDR(source as *const c_char) as int32;
    s = VARDATA_ANY(source as *const c_char);

    charlen = pg_mbstrlen_with_len(s, len);

    /* No work if supplied data matches typmod already */
    if charlen == maxlen {
        return PointerGetDatum(source as *const c_void); // PG_RETURN_BPCHAR_P
    }

    if charlen > maxlen {
        /* Verify that extra characters are spaces, and clip them off */
        let maxmblen: int32 = pg_mbcharcliplen(s, len, maxlen);

        if !isExplicit {
            i = maxmblen;
            while i < len {
                if *s.add(i as usize) != b' ' as c_char {
                    let _ = errcode(ERRCODE_STRING_DATA_RIGHT_TRUNCATION);
                    ereport!(
                        ERROR,
                        errmsg!("value too long for type character({})", maxlen)
                    );
                }
                i += 1;
            }
        }

        len = maxmblen;

        /*
         * At this point, maxlen is the necessary byte length, not the number
         * of CHARACTERS!
         */
        maxlen = len;
    } else {
        /*
         * At this point, maxlen is the necessary byte length, not the number
         * of CHARACTERS!
         */
        maxlen = len + (maxlen - charlen);
    }

    Assert!(maxlen >= len);

    result = palloc(maxlen as usize + VARHDRSZ as usize) as *mut BpChar;
    SET_VARSIZE(result as *mut c_char, maxlen + VARHDRSZ);
    r = VARDATA(result as *const c_char);

    memcpy(r as *mut c_void, s as *const c_void, len as usize);

    /* blank pad the string if necessary */
    if maxlen > len {
        memset(
            r.add(len as usize) as *mut c_void,
            b' ' as c_int,
            (maxlen - len) as usize,
        );
    }

    return PointerGetDatum(result as *const c_void); // PG_RETURN_BPCHAR_P
}

/* char_bpchar()
 * Convert char to bpchar(1).
 */
pub unsafe fn char_bpchar(fcinfo: FunctionCallInfo) -> Datum {
    let c: c_char = PG_GETARG_CHAR!(fcinfo, 0);
    let result: *mut BpChar;

    result = palloc(VARHDRSZ as usize + 1) as *mut BpChar;

    SET_VARSIZE(result as *mut c_char, VARHDRSZ + 1);
    *VARDATA(result as *const c_char) = c;

    return PointerGetDatum(result as *const c_void); // PG_RETURN_BPCHAR_P
}

/* bpchar_name()
 * Converts a bpchar() type to a NameData type.
 */
pub unsafe fn bpchar_name(fcinfo: FunctionCallInfo) -> Datum {
    let s: *mut BpChar = PG_GETARG_VARLENA_PP(fcinfo, 0) as *mut BpChar; // PG_GETARG_BPCHAR_PP(0)
    let s_data: *mut c_char;
    let result: Name;
    let mut len: int32;

    len = VARSIZE_ANY_EXHDR(s as *const c_char) as int32;
    s_data = VARDATA_ANY(s as *const c_char);

    /* Truncate oversize input */
    if len >= NAMEDATALEN as int32 {
        len = pg_mbcliplen(s_data, len, NAMEDATALEN as int32 - 1);
    }

    /* Remove trailing blanks */
    while len > 0 {
        if *s_data.add((len - 1) as usize) != b' ' as c_char {
            break;
        }
        len -= 1;
    }

    /* We use palloc0 here to ensure result is zero-padded */
    result = palloc0(NAMEDATALEN) as Name;
    memcpy(
        NameStr(&*result) as *mut c_void,
        s_data as *const c_void,
        len as usize,
    );

    PG_RETURN_NAME!(result);
}

/* name_bpchar()
 * Converts a NameData type to a bpchar type.
 *
 * Uses the text conversion functions, which is only appropriate if BpChar
 * and text are equivalent types.
 */
pub unsafe fn name_bpchar(fcinfo: FunctionCallInfo) -> Datum {
    let s: Name = PG_GETARG_NAME!(fcinfo, 0);
    let result: *mut BpChar;

    result = cstring_to_text(NameStr(&*s)) as *mut BpChar;
    return PointerGetDatum(result as *const c_void); // PG_RETURN_BPCHAR_P
}

pub unsafe fn bpchartypmodin(fcinfo: FunctionCallInfo) -> Datum {
    let ta: *mut c_void = PG_GETARG_POINTER!(fcinfo, 0) as *mut c_void; // PG_GETARG_ARRAYTYPE_P(0)

    PG_RETURN_INT32!(anychar_typmodin(ta, c"char".as_ptr()));
}

pub unsafe fn bpchartypmodout(fcinfo: FunctionCallInfo) -> Datum {
    let typmod: int32 = PG_GETARG_INT32!(fcinfo, 0);

    PG_RETURN_CSTRING!(anychar_typmodout(typmod));
}

/*****************************************************************************
 *	 varchar - varchar(n)
 *
 * Note: varchar piggybacks on type text for most operations, and so has no
 * C-coded functions except for I/O and typmod checking.
 *****************************************************************************/

/*
 * varchar_input -- common guts of varcharin and varcharrecv
 *
 * s is the input text of length len (may not be null-terminated)
 * atttypmod is the typmod value to apply
 *
 * # Safety
 * `s` is readable for `len` bytes.
 */
unsafe fn varchar_input(s: *const c_char, mut len: usize, atttypmod: int32, escontext: *mut Node) -> *mut VarChar {
    let _ = escontext; // TODO(pg-port): ErrorSaveContext soft errors (ereturn -> hard ERROR).
    let result: *mut VarChar;
    let maxlen: usize;

    maxlen = (atttypmod - VARHDRSZ) as usize;

    if atttypmod >= VARHDRSZ && len > maxlen {
        /* Verify that extra characters are spaces, and clip them off */
        let mbmaxlen = pg_mbcharcliplen(s, len as c_int, maxlen as c_int) as usize;
        let mut j: usize;

        j = mbmaxlen;
        while j < len {
            if *s.add(j) != b' ' as c_char {
                let _ = errcode(ERRCODE_STRING_DATA_RIGHT_TRUNCATION);
                ereport!(
                    ERROR,
                    errmsg!("value too long for type character varying({})", maxlen as c_int)
                );
            }
            j += 1;
        }

        len = mbmaxlen;
    }

    /*
     * We can use cstring_to_text_with_len because VarChar and text are
     * binary-compatible types.
     */
    result = cstring_to_text_with_len(s, len as c_int) as *mut VarChar;
    result
}

/*
 * Convert a C string to VARCHAR internal representation.  atttypmod
 * is the declared length of the type plus VARHDRSZ.
 */
pub unsafe fn varcharin(fcinfo: FunctionCallInfo) -> Datum {
    let s: *mut c_char = PG_GETARG_DATUM!(fcinfo, 0) as *mut c_char; // PG_GETARG_CSTRING(0)
    let atttypmod: int32 = PG_GETARG_INT32!(fcinfo, 2);
    let result: *mut VarChar;

    result = varchar_input(s, strlen(s), atttypmod, (*fcinfo).context);
    return PointerGetDatum(result as *const c_void); // PG_RETURN_VARCHAR_P
}

/*
 * Convert a VARCHAR value to a C string.
 *
 * Uses the text to C string conversion function, which is only appropriate
 * if VarChar and text are equivalent types.
 */
pub unsafe fn varcharout(fcinfo: FunctionCallInfo) -> Datum {
    let txt: Datum = PG_GETARG_DATUM!(fcinfo, 0);

    PG_RETURN_CSTRING!(TextDatumGetCString(txt));
}

/*
 *		varcharrecv			- converts external binary format to varchar
 */
pub unsafe fn varcharrecv(fcinfo: FunctionCallInfo) -> Datum {
    let buf: StringInfo = PG_GETARG_POINTER!(fcinfo, 0) as StringInfo;
    let atttypmod: int32 = PG_GETARG_INT32!(fcinfo, 2);
    let result: *mut VarChar;
    let str: *mut c_char;
    let mut nbytes: c_int = 0;

    str = pq_getmsgtext(buf, (*buf).len - (*buf).cursor, &mut nbytes);
    result = varchar_input(str, nbytes as usize, atttypmod, core::ptr::null_mut());
    pfree(str as *mut c_void);
    return PointerGetDatum(result as *const c_void); // PG_RETURN_VARCHAR_P
}

/*
 *		varcharsend			- converts varchar to binary format
 */
pub unsafe fn varcharsend(fcinfo: FunctionCallInfo) -> Datum {
    /* Exactly the same as textsend, so share code */
    return crate::utils::adt::varlena::textsend(fcinfo);
}

/*
 * varchar_support()
 *
 * Planner support function for the varchar() length coercion function.
 *
 * Currently, the only interesting thing we can do is flatten calls that set
 * the new maximum length >= the previous maximum length.  We can ignore the
 * isExplicit argument, since that only affects truncation cases.
 */
pub unsafe fn varchar_support(fcinfo: FunctionCallInfo) -> Datum {
    let rawreq: *mut Node = PG_GETARG_POINTER!(fcinfo, 0) as *mut Node;
    let mut ret: *mut Node = core::ptr::null_mut();

    if IsA!(rawreq, T_SupportRequestSimplify) {
        let req: *mut SupportRequestSimplify = rawreq as *mut SupportRequestSimplify;
        let expr: *mut FuncExpr = (*req).fcall;
        let typmod: *mut Node;

        Assert!(list_length((*expr).args) >= 2);

        typmod = lsecond((*expr).args) as *mut Node;

        if IsA!(typmod, T_Const) && !(*(typmod as *mut Const)).constisnull {
            let source: *mut Node = linitial((*expr).args) as *mut Node;
            let old_typmod: int32 = exprTypmod(source);
            let new_typmod: int32 = DatumGetInt32((*(typmod as *mut Const)).constvalue);
            let old_max: int32 = old_typmod - VARHDRSZ;
            let new_max: int32 = new_typmod - VARHDRSZ;

            if new_typmod < 0 || (old_typmod >= 0 && old_max <= new_max) {
                ret = relabel_to_typmod(source, new_typmod);
            }
        }
    }

    PG_RETURN_POINTER!(ret);
}

/*
 * Converts a VARCHAR type to the specified size.
 *
 * maxlen is the typmod, ie, declared length plus VARHDRSZ bytes.
 * isExplicit is true if this is for an explicit cast to varchar(N).
 */
pub unsafe fn varchar(fcinfo: FunctionCallInfo) -> Datum {
    let source: *mut VarChar =
        PG_GETARG_VARLENA_PP(fcinfo, 0) as *mut VarChar; // PG_GETARG_VARCHAR_PP(0)
    let typmod: int32 = PG_GETARG_INT32!(fcinfo, 1);
    let isExplicit: bool = PG_GETARG_BOOL!(fcinfo, 2);
    let len: int32;
    let maxlen: int32;
    let maxmblen: int32;
    let mut i: int32;
    let s_data: *mut c_char;

    len = VARSIZE_ANY_EXHDR(source as *const c_char) as int32;
    s_data = VARDATA_ANY(source as *const c_char);
    maxlen = typmod - VARHDRSZ;

    /* No work if typmod is invalid or supplied data fits it already */
    if maxlen < 0 || len <= maxlen {
        return PointerGetDatum(source as *const c_void); // PG_RETURN_VARCHAR_P
    }

    /* only reach here if string is too long... */

    /* truncate multibyte string preserving multibyte boundary */
    maxmblen = pg_mbcharcliplen(s_data, len, maxlen);

    if !isExplicit {
        i = maxmblen;
        while i < len {
            if *s_data.add(i as usize) != b' ' as c_char {
                let _ = errcode(ERRCODE_STRING_DATA_RIGHT_TRUNCATION);
                ereport!(
                    ERROR,
                    errmsg!("value too long for type character varying({})", maxlen)
                );
            }
            i += 1;
        }
    }

    return PointerGetDatum(cstring_to_text_with_len(s_data, maxmblen) as *const c_void); // PG_RETURN_VARCHAR_P
}

pub unsafe fn varchartypmodin(fcinfo: FunctionCallInfo) -> Datum {
    let ta: *mut c_void = PG_GETARG_POINTER!(fcinfo, 0) as *mut c_void; // PG_GETARG_ARRAYTYPE_P(0)

    PG_RETURN_INT32!(anychar_typmodin(ta, c"varchar".as_ptr()));
}

pub unsafe fn varchartypmodout(fcinfo: FunctionCallInfo) -> Datum {
    let typmod: int32 = PG_GETARG_INT32!(fcinfo, 0);

    PG_RETURN_CSTRING!(anychar_typmodout(typmod));
}

/*****************************************************************************
 * Exported functions
 *****************************************************************************/

/* "True" length (not counting trailing blanks) of a BpChar */
#[inline]
unsafe fn bcTruelen(arg: *mut BpChar) -> c_int {
    bpchartruelen(
        VARDATA_ANY(arg as *const c_char),
        VARSIZE_ANY_EXHDR(arg as *const c_char) as c_int,
    )
}

/*
 * # Safety
 * `s` is readable for `len` bytes.
 */
pub unsafe fn bpchartruelen(s: *mut c_char, len: c_int) -> c_int {
    let mut i: c_int;

    /*
     * Note that we rely on the assumption that ' ' is a singleton unit on
     * every supported multibyte server encoding.
     */
    i = len - 1;
    while i >= 0 {
        if *s.add(i as usize) != b' ' as c_char {
            break;
        }
        i -= 1;
    }
    i + 1
}

pub unsafe fn bpcharlen(fcinfo: FunctionCallInfo) -> Datum {
    let arg: *mut BpChar = PG_GETARG_VARLENA_PP(fcinfo, 0) as *mut BpChar; // PG_GETARG_BPCHAR_PP(0)
    let mut len: c_int;

    /* get number of bytes, ignoring trailing spaces */
    len = bcTruelen(arg);

    /* in multibyte encoding, convert to number of characters */
    if pg_database_encoding_max_length() != 1 {
        len = pg_mbstrlen_with_len(VARDATA_ANY(arg as *const c_char), len);
    }

    PG_RETURN_INT32!(len);
}

pub unsafe fn bpcharoctetlen(fcinfo: FunctionCallInfo) -> Datum {
    let arg: Datum = PG_GETARG_DATUM!(fcinfo, 0);

    /* We need not detoast the input at all */
    PG_RETURN_INT32!(toast_raw_datum_size(arg) as int32 - VARHDRSZ);
}

/*
 * toast_raw_datum_size (access/common/detoast.c): logical size (incl. VARHDRSZ) of a
 * datum.  The fully-ported copy lives in crate::utils::adt::datum but is not pub;
 * for the plain in-line case it is just VARSIZE_ANY.
 *
 * # Safety
 * `value` is a Datum holding a varlena pointer.
 */
unsafe fn toast_raw_datum_size(value: Datum) -> Size {
    crate::varatt::VARSIZE_ANY(DatumGetPointer(value) as *const c_char) as Size
}

/*****************************************************************************
 *	Comparison Functions used for bpchar
 *****************************************************************************/

#[allow(dead_code)]
unsafe fn check_collation_set(collid: Oid) {
    if !OidIsValid(collid) {
        let _ = errcode(ERRCODE_INDETERMINATE_COLLATION);
        // TODO(pg-port): errhint() not modeled by the elog shim.
        ereport!(
            ERROR,
            errmsg!("could not determine which collation to use for string comparison")
        );
    }
}

pub unsafe fn bpchareq(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: *mut BpChar = PG_GETARG_VARLENA_PP(fcinfo, 0) as *mut BpChar; // PG_GETARG_BPCHAR_PP(0)
    let arg2: *mut BpChar = PG_GETARG_VARLENA_PP(fcinfo, 1) as *mut BpChar; // PG_GETARG_BPCHAR_PP(1)
    let len1: c_int;
    let len2: c_int;
    let result: bool;
    let collid: Oid = PG_GET_COLLATION!(fcinfo);
    let mylocale: pg_locale_t;

    check_collation_set(collid);

    len1 = bcTruelen(arg1);
    len2 = bcTruelen(arg2);

    mylocale = pg_newlocale_from_collation(collid);

    if (*mylocale).deterministic {
        /*
         * Since we only care about equality or not-equality, we can avoid all
         * the expense of strcoll() here, and just do bitwise comparison.
         */
        if len1 != len2 {
            result = false;
        } else {
            result = memcmp(
                VARDATA_ANY(arg1 as *const c_char) as *const c_void,
                VARDATA_ANY(arg2 as *const c_char) as *const c_void,
                len1 as usize,
            ) == 0;
        }
    } else {
        result = varstr_cmp(
            VARDATA_ANY(arg1 as *const c_char),
            len1,
            VARDATA_ANY(arg2 as *const c_char),
            len2,
            collid,
        ) == 0;
    }

    // PG_FREE_IF_COPY(arg1, 0);
    // PG_FREE_IF_COPY(arg2, 1);

    crate::PG_RETURN_BOOL!(result);
}

pub unsafe fn bpcharne(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: *mut BpChar = PG_GETARG_VARLENA_PP(fcinfo, 0) as *mut BpChar; // PG_GETARG_BPCHAR_PP(0)
    let arg2: *mut BpChar = PG_GETARG_VARLENA_PP(fcinfo, 1) as *mut BpChar; // PG_GETARG_BPCHAR_PP(1)
    let len1: c_int;
    let len2: c_int;
    let result: bool;
    let collid: Oid = PG_GET_COLLATION!(fcinfo);
    let mylocale: pg_locale_t;

    check_collation_set(collid);

    len1 = bcTruelen(arg1);
    len2 = bcTruelen(arg2);

    mylocale = pg_newlocale_from_collation(collid);

    if (*mylocale).deterministic {
        /*
         * Since we only care about equality or not-equality, we can avoid all
         * the expense of strcoll() here, and just do bitwise comparison.
         */
        if len1 != len2 {
            result = true;
        } else {
            result = memcmp(
                VARDATA_ANY(arg1 as *const c_char) as *const c_void,
                VARDATA_ANY(arg2 as *const c_char) as *const c_void,
                len1 as usize,
            ) != 0;
        }
    } else {
        result = varstr_cmp(
            VARDATA_ANY(arg1 as *const c_char),
            len1,
            VARDATA_ANY(arg2 as *const c_char),
            len2,
            collid,
        ) != 0;
    }

    // PG_FREE_IF_COPY(arg1, 0);
    // PG_FREE_IF_COPY(arg2, 1);

    crate::PG_RETURN_BOOL!(result);
}

pub unsafe fn bpcharlt(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: *mut BpChar = PG_GETARG_VARLENA_PP(fcinfo, 0) as *mut BpChar;
    let arg2: *mut BpChar = PG_GETARG_VARLENA_PP(fcinfo, 1) as *mut BpChar;
    let len1: c_int;
    let len2: c_int;
    let cmp: c_int;

    len1 = bcTruelen(arg1);
    len2 = bcTruelen(arg2);

    cmp = varstr_cmp(
        VARDATA_ANY(arg1 as *const c_char),
        len1,
        VARDATA_ANY(arg2 as *const c_char),
        len2,
        PG_GET_COLLATION!(fcinfo),
    );

    // PG_FREE_IF_COPY(arg1, 0);
    // PG_FREE_IF_COPY(arg2, 1);

    crate::PG_RETURN_BOOL!(cmp < 0);
}

pub unsafe fn bpcharle(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: *mut BpChar = PG_GETARG_VARLENA_PP(fcinfo, 0) as *mut BpChar;
    let arg2: *mut BpChar = PG_GETARG_VARLENA_PP(fcinfo, 1) as *mut BpChar;
    let len1: c_int;
    let len2: c_int;
    let cmp: c_int;

    len1 = bcTruelen(arg1);
    len2 = bcTruelen(arg2);

    cmp = varstr_cmp(
        VARDATA_ANY(arg1 as *const c_char),
        len1,
        VARDATA_ANY(arg2 as *const c_char),
        len2,
        PG_GET_COLLATION!(fcinfo),
    );

    // PG_FREE_IF_COPY(arg1, 0);
    // PG_FREE_IF_COPY(arg2, 1);

    crate::PG_RETURN_BOOL!(cmp <= 0);
}

pub unsafe fn bpchargt(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: *mut BpChar = PG_GETARG_VARLENA_PP(fcinfo, 0) as *mut BpChar;
    let arg2: *mut BpChar = PG_GETARG_VARLENA_PP(fcinfo, 1) as *mut BpChar;
    let len1: c_int;
    let len2: c_int;
    let cmp: c_int;

    len1 = bcTruelen(arg1);
    len2 = bcTruelen(arg2);

    cmp = varstr_cmp(
        VARDATA_ANY(arg1 as *const c_char),
        len1,
        VARDATA_ANY(arg2 as *const c_char),
        len2,
        PG_GET_COLLATION!(fcinfo),
    );

    // PG_FREE_IF_COPY(arg1, 0);
    // PG_FREE_IF_COPY(arg2, 1);

    crate::PG_RETURN_BOOL!(cmp > 0);
}

pub unsafe fn bpcharge(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: *mut BpChar = PG_GETARG_VARLENA_PP(fcinfo, 0) as *mut BpChar;
    let arg2: *mut BpChar = PG_GETARG_VARLENA_PP(fcinfo, 1) as *mut BpChar;
    let len1: c_int;
    let len2: c_int;
    let cmp: c_int;

    len1 = bcTruelen(arg1);
    len2 = bcTruelen(arg2);

    cmp = varstr_cmp(
        VARDATA_ANY(arg1 as *const c_char),
        len1,
        VARDATA_ANY(arg2 as *const c_char),
        len2,
        PG_GET_COLLATION!(fcinfo),
    );

    // PG_FREE_IF_COPY(arg1, 0);
    // PG_FREE_IF_COPY(arg2, 1);

    crate::PG_RETURN_BOOL!(cmp >= 0);
}

pub unsafe fn bpcharcmp(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: *mut BpChar = PG_GETARG_VARLENA_PP(fcinfo, 0) as *mut BpChar;
    let arg2: *mut BpChar = PG_GETARG_VARLENA_PP(fcinfo, 1) as *mut BpChar;
    let len1: c_int;
    let len2: c_int;
    let cmp: c_int;

    len1 = bcTruelen(arg1);
    len2 = bcTruelen(arg2);

    cmp = varstr_cmp(
        VARDATA_ANY(arg1 as *const c_char),
        len1,
        VARDATA_ANY(arg2 as *const c_char),
        len2,
        PG_GET_COLLATION!(fcinfo),
    );

    // PG_FREE_IF_COPY(arg1, 0);
    // PG_FREE_IF_COPY(arg2, 1);

    PG_RETURN_INT32!(cmp);
}

pub unsafe fn bpchar_sortsupport(fcinfo: FunctionCallInfo) -> Datum {
    let ssup: SortSupport = PG_GETARG_POINTER!(fcinfo, 0) as SortSupport;
    let collid: Oid = (*ssup).ssup_collation;
    let oldcontext: MemoryContext;

    oldcontext = MemoryContextSwitchTo((*ssup).ssup_cxt);

    /* Use generic string SortSupport */
    varstr_sortsupport(ssup, BPCHAROID, collid);

    MemoryContextSwitchTo(oldcontext);

    crate::PG_RETURN_VOID!()
}

pub unsafe fn bpchar_larger(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: *mut BpChar = PG_GETARG_VARLENA_PP(fcinfo, 0) as *mut BpChar;
    let arg2: *mut BpChar = PG_GETARG_VARLENA_PP(fcinfo, 1) as *mut BpChar;
    let len1: c_int;
    let len2: c_int;
    let cmp: c_int;

    len1 = bcTruelen(arg1);
    len2 = bcTruelen(arg2);

    cmp = varstr_cmp(
        VARDATA_ANY(arg1 as *const c_char),
        len1,
        VARDATA_ANY(arg2 as *const c_char),
        len2,
        PG_GET_COLLATION!(fcinfo),
    );

    return PointerGetDatum((if cmp >= 0 { arg1 } else { arg2 }) as *const c_void); // PG_RETURN_BPCHAR_P
}

pub unsafe fn bpchar_smaller(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: *mut BpChar = PG_GETARG_VARLENA_PP(fcinfo, 0) as *mut BpChar;
    let arg2: *mut BpChar = PG_GETARG_VARLENA_PP(fcinfo, 1) as *mut BpChar;
    let len1: c_int;
    let len2: c_int;
    let cmp: c_int;

    len1 = bcTruelen(arg1);
    len2 = bcTruelen(arg2);

    cmp = varstr_cmp(
        VARDATA_ANY(arg1 as *const c_char),
        len1,
        VARDATA_ANY(arg2 as *const c_char),
        len2,
        PG_GET_COLLATION!(fcinfo),
    );

    return PointerGetDatum((if cmp <= 0 { arg1 } else { arg2 }) as *const c_void); // PG_RETURN_BPCHAR_P
}

/*
 * bpchar needs a specialized hash function because we want to ignore
 * trailing blanks in comparisons.
 */
pub unsafe fn hashbpchar(fcinfo: FunctionCallInfo) -> Datum {
    let key: *mut BpChar = PG_GETARG_VARLENA_PP(fcinfo, 0) as *mut BpChar; // PG_GETARG_BPCHAR_PP(0)
    let collid: Oid = PG_GET_COLLATION!(fcinfo);
    let keydata: *mut c_char;
    let keylen: c_int;
    let mylocale: pg_locale_t;
    let result: Datum;

    if collid == 0 {
        let _ = errcode(ERRCODE_INDETERMINATE_COLLATION);
        ereport!(
            ERROR,
            errmsg!("could not determine which collation to use for string hashing")
        );
    }

    keydata = VARDATA_ANY(key as *const c_char);
    keylen = bcTruelen(key);

    mylocale = pg_newlocale_from_collation(collid);

    if (*mylocale).deterministic {
        result = hash_any(keydata as *const c_uchar, keylen);
    } else {
        let bsize: Size;
        let rsize: Size;
        let buf: *mut c_char;

        bsize = pg_strnxfrm(core::ptr::null_mut(), 0, keydata, keylen as isize, mylocale) as Size;
        buf = palloc(bsize + 1) as *mut c_char;

        rsize = pg_strnxfrm(buf, bsize + 1, keydata, keylen as isize, mylocale) as Size;

        /* the second call may return a smaller value than the first */
        if rsize > bsize {
            elog!(ERROR, "pg_strnxfrm() returned unexpected result");
        }

        /*
         * In principle, there's no reason to include the terminating NUL
         * character in the hash, but it was done before and the behavior must
         * be preserved.
         */
        result = hash_any(buf as *const u8, (bsize + 1) as c_int);

        pfree(buf as *mut c_void);
    }

    /* Avoid leaking memory for toasted inputs */
    // PG_FREE_IF_COPY(key, 0);

    result
}

pub unsafe fn hashbpcharextended(fcinfo: FunctionCallInfo) -> Datum {
    let key: *mut BpChar = PG_GETARG_VARLENA_PP(fcinfo, 0) as *mut BpChar; // PG_GETARG_BPCHAR_PP(0)
    let collid: Oid = PG_GET_COLLATION!(fcinfo);
    let keydata: *mut c_char;
    let keylen: c_int;
    let mylocale: pg_locale_t;
    let result: Datum;

    if collid == 0 {
        let _ = errcode(ERRCODE_INDETERMINATE_COLLATION);
        ereport!(
            ERROR,
            errmsg!("could not determine which collation to use for string hashing")
        );
    }

    keydata = VARDATA_ANY(key as *const c_char);
    keylen = bcTruelen(key);

    mylocale = pg_newlocale_from_collation(collid);

    if (*mylocale).deterministic {
        result = hash_any_extended(
            keydata as *const c_uchar,
            keylen,
            crate::PG_GETARG_INT64!(fcinfo, 1) as u64,
        );
    } else {
        let bsize: Size;
        let rsize: Size;
        let buf: *mut c_char;

        bsize = pg_strnxfrm(core::ptr::null_mut(), 0, keydata, keylen as isize, mylocale) as Size;
        buf = palloc(bsize + 1) as *mut c_char;

        rsize = pg_strnxfrm(buf, bsize + 1, keydata, keylen as isize, mylocale) as Size;

        /* the second call may return a smaller value than the first */
        if rsize > bsize {
            elog!(ERROR, "pg_strnxfrm() returned unexpected result");
        }

        /*
         * In principle, there's no reason to include the terminating NUL
         * character in the hash, but it was done before and the behavior must
         * be preserved.
         */
        result = hash_any_extended(
            buf as *const u8,
            (bsize + 1) as c_int,
            crate::PG_GETARG_INT64!(fcinfo, 1) as u64,
        );

        pfree(buf as *mut c_void);
    }

    // PG_FREE_IF_COPY(key, 0);

    result
}

/*
 * The following operators support character-by-character comparison
 * of bpchar datums, to allow building indexes suitable for LIKE clauses.
 */

unsafe fn internal_bpchar_pattern_compare(arg1: *mut BpChar, arg2: *mut BpChar) -> c_int {
    let result: c_int;
    let len1: c_int;
    let len2: c_int;

    len1 = bcTruelen(arg1);
    len2 = bcTruelen(arg2);

    result = memcmp(
        VARDATA_ANY(arg1 as *const c_char) as *const c_void,
        VARDATA_ANY(arg2 as *const c_char) as *const c_void,
        Min(len1, len2) as usize,
    );
    if result != 0 {
        result
    } else if len1 < len2 {
        -1
    } else if len1 > len2 {
        1
    } else {
        0
    }
}

pub unsafe fn bpchar_pattern_lt(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: *mut BpChar = PG_GETARG_VARLENA_PP(fcinfo, 0) as *mut BpChar;
    let arg2: *mut BpChar = PG_GETARG_VARLENA_PP(fcinfo, 1) as *mut BpChar;
    let result: c_int;

    result = internal_bpchar_pattern_compare(arg1, arg2);

    crate::PG_RETURN_BOOL!(result < 0);
}

pub unsafe fn bpchar_pattern_le(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: *mut BpChar = PG_GETARG_VARLENA_PP(fcinfo, 0) as *mut BpChar;
    let arg2: *mut BpChar = PG_GETARG_VARLENA_PP(fcinfo, 1) as *mut BpChar;
    let result: c_int;

    result = internal_bpchar_pattern_compare(arg1, arg2);

    crate::PG_RETURN_BOOL!(result <= 0);
}

pub unsafe fn bpchar_pattern_ge(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: *mut BpChar = PG_GETARG_VARLENA_PP(fcinfo, 0) as *mut BpChar;
    let arg2: *mut BpChar = PG_GETARG_VARLENA_PP(fcinfo, 1) as *mut BpChar;
    let result: c_int;

    result = internal_bpchar_pattern_compare(arg1, arg2);

    crate::PG_RETURN_BOOL!(result >= 0);
}

pub unsafe fn bpchar_pattern_gt(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: *mut BpChar = PG_GETARG_VARLENA_PP(fcinfo, 0) as *mut BpChar;
    let arg2: *mut BpChar = PG_GETARG_VARLENA_PP(fcinfo, 1) as *mut BpChar;
    let result: c_int;

    result = internal_bpchar_pattern_compare(arg1, arg2);

    crate::PG_RETURN_BOOL!(result > 0);
}

pub unsafe fn btbpchar_pattern_cmp(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: *mut BpChar = PG_GETARG_VARLENA_PP(fcinfo, 0) as *mut BpChar;
    let arg2: *mut BpChar = PG_GETARG_VARLENA_PP(fcinfo, 1) as *mut BpChar;
    let result: c_int;

    result = internal_bpchar_pattern_compare(arg1, arg2);

    PG_RETURN_INT32!(result);
}

pub unsafe fn btbpchar_pattern_sortsupport(fcinfo: FunctionCallInfo) -> Datum {
    let ssup: SortSupport = PG_GETARG_POINTER!(fcinfo, 0) as SortSupport;
    let oldcontext: MemoryContext;

    oldcontext = MemoryContextSwitchTo((*ssup).ssup_cxt);

    /* Use generic string SortSupport, forcing "C" collation */
    varstr_sortsupport(ssup, BPCHAROID, C_COLLATION_OID);

    MemoryContextSwitchTo(oldcontext);

    crate::PG_RETURN_VOID!()
}

/// `PG_GETARG_BPCHAR_PP(n)` / `PG_GETARG_VARCHAR_PP(n)`: detoast a packed varlena arg into a
/// `*mut varlena` we then cast to BpChar/VarChar.  (Spelled inline per project convention.)
///
/// # Safety
/// `fcinfo` holds at least `n+1` args, the n'th being a varlena Datum.
#[inline]
unsafe fn PG_GETARG_VARLENA_PP(fcinfo: FunctionCallInfo, n: usize) -> *mut crate::c::varlena {
    crate::varatt::pg_detoast_datum_packed(
        DatumGetPointer(PG_GETARG_DATUM!(fcinfo, n)) as *mut c_void,
    ) as *mut crate::c::varlena
}

// `Min(x, y)` (c.h macro) comes from crate::c via the prelude glob import.

/// Format a C string for an error message via Rust `{}` (lossy).
///
/// # Safety
/// `s` must be a valid NUL-terminated C string.
unsafe fn cstr(s: *const c_char) -> std::string::String {
    let n = strlen(s);
    let bytes = core::slice::from_raw_parts(s as *const u8, n);
    std::string::String::from_utf8_lossy(bytes).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::postgres::{
        BoolGetDatum, CStringGetDatum, DatumGetBool, DatumGetCString, DatumGetInt32, DatumGetName,
        Int32GetDatum,
    };
    use crate::postgres_ext::InvalidOid;
    use crate::utils::fmgr::{DirectFunctionCall1Coll, DirectFunctionCall2Coll, DirectFunctionCall3Coll};

    unsafe fn cstr_eq(p: *const c_char, want: &str) -> bool {
        let n = strlen(p);
        core::slice::from_raw_parts(p as *const u8, n) == want.as_bytes()
    }

    // Build a BpChar/VarChar/text datum from a Rust &str (4-byte header).
    unsafe fn mk(s: &str) -> Datum {
        let p = cstring_to_text_with_len(s.as_ptr() as *const c_char, s.len() as c_int);
        PointerGetDatum(p as *const c_void)
    }

    #[test]
    fn bpchar_io_and_padding() {
        unsafe {
            // bpcharin with typmod -1 (no padding): in -> out round trip.
            let d = DirectFunctionCall3Coll(
                bpcharin,
                InvalidOid,
                CStringGetDatum(c"abc".as_ptr()),
                InvalidOid as Datum, // typelem (ignored)
                Int32GetDatum(-1),
            );
            let s = DatumGetCString(DirectFunctionCall1Coll(bpcharout, InvalidOid, d));
            assert!(cstr_eq(s, "abc"));

            // bpcharin char(5): "ab" blank-pads to "ab   " (typmod = VARHDRSZ + 5).
            let d5 = DirectFunctionCall3Coll(
                bpcharin,
                InvalidOid,
                CStringGetDatum(c"ab".as_ptr()),
                InvalidOid as Datum,
                Int32GetDatum(VARHDRSZ + 5),
            );
            let s5 = DatumGetCString(DirectFunctionCall1Coll(bpcharout, InvalidOid, d5));
            assert!(cstr_eq(s5, "ab   "));

            // trailing-space truncation is allowed: "ab   " into char(2) -> "ab".
            let d2 = DirectFunctionCall3Coll(
                bpcharin,
                InvalidOid,
                CStringGetDatum(c"ab   ".as_ptr()),
                InvalidOid as Datum,
                Int32GetDatum(VARHDRSZ + 2),
            );
            let s2 = DatumGetCString(DirectFunctionCall1Coll(bpcharout, InvalidOid, d2));
            assert!(cstr_eq(s2, "ab"));
        }
    }

    #[test]
    #[should_panic]
    fn bpcharin_rejects_overlong_nonspace() {
        unsafe {
            // "abcd" into char(2): extra chars are not spaces -> hard ERROR.
            DirectFunctionCall3Coll(
                bpcharin,
                InvalidOid,
                CStringGetDatum(c"abcd".as_ptr()),
                InvalidOid as Datum,
                Int32GetDatum(VARHDRSZ + 2),
            );
        }
    }

    #[test]
    fn varchar_io_and_truncation() {
        unsafe {
            // varcharin with typmod -1: pass-through.
            let d = DirectFunctionCall3Coll(
                varcharin,
                InvalidOid,
                CStringGetDatum(c"hello".as_ptr()),
                InvalidOid as Datum,
                Int32GetDatum(-1),
            );
            let s = DatumGetCString(DirectFunctionCall1Coll(varcharout, InvalidOid, d));
            assert!(cstr_eq(s, "hello"));

            // varcharin varchar(3) of "abc" fits exactly.
            let d3 = DirectFunctionCall3Coll(
                varcharin,
                InvalidOid,
                CStringGetDatum(c"abc".as_ptr()),
                InvalidOid as Datum,
                Int32GetDatum(VARHDRSZ + 3),
            );
            let s3 = DatumGetCString(DirectFunctionCall1Coll(varcharout, InvalidOid, d3));
            assert!(cstr_eq(s3, "abc"));

            // varchar() length cast: explicit cast truncates silently to 2 bytes.
            let src = mk("abcdef");
            let casted = DirectFunctionCall3Coll(
                varchar,
                InvalidOid,
                src,
                Int32GetDatum(VARHDRSZ + 2),
                BoolGetDatum(true), // isExplicit
            );
            let sc = DatumGetCString(DirectFunctionCall1Coll(varcharout, InvalidOid, casted));
            assert!(cstr_eq(sc, "ab"));
        }
    }

    #[test]
    fn bpchar_cast_and_len() {
        unsafe {
            // bpchar() length cast: "ab" -> char(4) blank-pads to "ab  ".
            let src = mk("ab");
            let casted = DirectFunctionCall3Coll(
                bpchar,
                InvalidOid,
                src,
                Int32GetDatum(VARHDRSZ + 4),
                BoolGetDatum(false),
            );
            let sc = DatumGetCString(DirectFunctionCall1Coll(bpcharout, InvalidOid, casted));
            assert!(cstr_eq(sc, "ab  "));

            // bpcharlen ignores trailing blanks: char value "ab  " has length 2.
            let v = mk("ab  ");
            let len = DatumGetInt32(DirectFunctionCall1Coll(bpcharlen, InvalidOid, v));
            assert_eq!(len, 2);

            // bpcharoctetlen counts all bytes incl. blanks.
            let oct = DatumGetInt32(DirectFunctionCall1Coll(bpcharoctetlen, InvalidOid, mk("ab  ")));
            assert_eq!(oct, 4);
        }
    }

    #[test]
    fn bpchar_pattern_compare() {
        unsafe {
            // trailing blanks ignored: "ab " == "ab" under pattern compare (lt false, le true).
            let a = mk("ab ");
            let b = mk("ab");
            assert!(!DatumGetBool(DirectFunctionCall2Coll(bpchar_pattern_lt, InvalidOid, a, b)));
            assert!(DatumGetBool(DirectFunctionCall2Coll(bpchar_pattern_le, InvalidOid, a, b)));
            assert_eq!(
                DatumGetInt32(DirectFunctionCall2Coll(btbpchar_pattern_cmp, InvalidOid, a, b)),
                0
            );
            // "ab" < "ac"
            let ac = mk("ac");
            assert!(DatumGetBool(DirectFunctionCall2Coll(bpchar_pattern_lt, InvalidOid, b, ac)));
            assert!(DatumGetBool(DirectFunctionCall2Coll(bpchar_pattern_gt, InvalidOid, ac, b)));
        }
    }

    #[test]
    fn typmodout_formats() {
        unsafe {
            let s = DatumGetCString(DirectFunctionCall1Coll(
                bpchartypmodout,
                InvalidOid,
                Int32GetDatum(VARHDRSZ + 10),
            ));
            assert!(cstr_eq(s, "(10)"));
            // typmod <= VARHDRSZ -> empty string
            let e = DatumGetCString(DirectFunctionCall1Coll(
                varchartypmodout,
                InvalidOid,
                Int32GetDatum(VARHDRSZ),
            ));
            assert!(cstr_eq(e, ""));
        }
    }

    #[test]
    fn char_and_name_conversions() {
        unsafe {
            // char_bpchar: a single char becomes a char(1).
            let d = DirectFunctionCall1Coll(char_bpchar, InvalidOid, crate::postgres::CharGetDatum(b'Z' as c_char));
            let s = DatumGetCString(DirectFunctionCall1Coll(bpcharout, InvalidOid, d));
            assert!(cstr_eq(s, "Z"));

            // name_bpchar -> bpchar_name round trip through NameData.
            let nm = DirectFunctionCall1Coll(
                name_bpchar,
                InvalidOid,
                crate::postgres::PointerGetDatum({
                    // build a NameData "hi"
                    let n = palloc0(NAMEDATALEN) as Name;
                    *(*n).data.as_mut_ptr().add(0) = b'h' as c_char;
                    *(*n).data.as_mut_ptr().add(1) = b'i' as c_char;
                    n as *const c_void
                }),
            );
            let back = DirectFunctionCall1Coll(bpchar_name, InvalidOid, nm);
            let bn = DatumGetName(back);
            assert!(cstr_eq(NameStr(&*bn), "hi"));
        }
    }
}
