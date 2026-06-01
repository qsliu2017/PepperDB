//! Translation of postgres/src/backend/utils/adt/oracle_compat.c
//!
//! Oracle compatible functions.
//!
//!     Author: Edmund Mergl <E.Mergl@bawue.de>
//!     Multibyte enhancement: Tatsuo Ishii <ishii@postgresql.org>
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//!
//! `#include`s mapped:
//!   - common/int.h        -> crate::common::int (pg_mul_s32_overflow / pg_add_s32_overflow)
//!   - mb/pg_wchar.h       -> crate::mb::mbutils (pg_mbstrlen_with_len / pg_mblen_with_len /
//!                            pg_mblen_unbounded / pg_database_encoding_max_length) +
//!                            crate::mb::wchar (PG_UTF8, used only by ascii/chr - see below)
//!   - miscadmin.h         -> CHECK_FOR_INTERRUPTS() (no-op shim, see below)
//!   - utils/builtins.h    -> crate::utils::adt::varlena (cstring_to_text[_with_len])
//!   - utils/formatting.h  -> str_tolower / str_toupper / str_initcap / str_casefold
//!                            (formatting.c, pg_locale collation) NOT ported - see lower/upper.
//!   - utils/memutils.h    -> crate::utils::memutils (AllocSizeIsValid)
//!   - varatt.h VAR* macros -> crate::varatt (VARDATA / SET_VARSIZE / VARDATA_ANY /
//!                            VARSIZE_ANY_EXHDR)
//!   - <string.h> memcpy/memcmp -> libc via extern "C".
//!
//! NOTE: `chr(int4)` and `ascii(text)` physically live in this C file, but per the
//! port plan they were translated in crate::utils::adt::ascii (the "ascii family").
//! They are therefore SKIPPED here (not duplicated); see ascii.rs.
//!
//! TRANSLATED FULLY (self-contained over varlena + mbutils):
//!   lpad, rpad, btrim, btrim1, ltrim, ltrim1, rtrim, rtrim1, translate, repeat,
//!   dotrim (text trim helper), dobyteatrim (bytea trim helper) + byteatrim /
//!   bytealtrim / byteartrim.
//!
//! ASCII-FOLD APPROXIMATION (formatting.c / pg_locale not ported):
//!   lower, upper, initcap, casefold.  The C uses str_tolower / str_toupper /
//!   str_initcap / str_casefold from formatting.c, which are pg_locale collation-
//!   aware.  pg_locale is not ported, so here we implement an ASCII-only fold
//!   (a..z <-> A..Z) with a clear TODO(pg-port).  This is a useful approximation
//!   for C-locale / ASCII data, not a stub.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use crate::prelude::*;
use crate::utils::fmgr::*;
use crate::varatt::{SET_VARSIZE, VARDATA, VARDATA_ANY, VARSIZE_ANY_EXHDR};
use crate::{PG_GETARG_BYTEA_PP, PG_GETARG_DATUM, PG_GETARG_INT32, PG_GETARG_TEXT_PP};
use crate::c::{bytea, int32, text};
use crate::common::int::{pg_add_s32_overflow, pg_mul_s32_overflow};
use crate::mb::mbutils::{
    pg_database_encoding_max_length, pg_mblen_unbounded, pg_mblen_with_len, pg_mbstrlen_with_len,
};
use crate::postgres::PointerGetDatum;
use crate::utils::adt::varlena::cstring_to_text_with_len;
use crate::utils::memutils::AllocSizeIsValid;
use core::ffi::{c_char, c_int, c_void};

// <string.h> via postgres.h.
extern "C" {
    fn memcpy(dst: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
    fn memcmp(a: *const c_void, b: *const c_void, n: usize) -> c_int;
}

/* errcodes.h classification (the errcode() shim ignores the value). */
const ERRCODE_PROGRAM_LIMIT_EXCEEDED: c_int = 0;

/*
 * CHECK_FOR_INTERRUPTS (miscadmin.h): the interrupt/cancel check used inside
 * `repeat`'s copy loop.  The real implementation lives in tcop/postgres.c; here
 * it is a no-op, matching the other ported units.
 */
#[inline]
fn CHECK_FOR_INTERRUPTS() {}

/*
 * pg_mblen_range (mb/pg_wchar.h): byte length of the multibyte character at `p`,
 * bounded so it cannot run past `end`.  crate::mb::mbutils::pg_mblen_range is still
 * stubbed (report_invalid_encoding_db / VALGRIND deps); since this file's callers
 * always know the remaining byte length, we express it via the fully-translated
 * pg_mblen_with_len(p, end - p), which is the identical operation.
 *
 * # Safety
 * `p` < `end`, both into the same readable buffer.
 */
#[inline]
unsafe fn pg_mblen_range(p: *const c_char, end: *const c_char) -> c_int {
    pg_mblen_with_len(p, (end as isize - p as isize) as c_int)
}

/********************************************************************
 *
 * lower
 *
 * Syntax:
 *
 *	 text lower(text string)
 *
 * Purpose:
 *
 *	 Returns string, with all letters forced to lowercase.
 *
 ********************************************************************/
pub unsafe fn lower(fcinfo: FunctionCallInfo) -> Datum {
    let in_string: *mut text = PG_GETARG_TEXT_PP(fcinfo, 0);
    let result: *mut text;

    // C: out_string = str_tolower(VARDATA_ANY(in_string), VARSIZE_ANY_EXHDR(in_string),
    //                             PG_GET_COLLATION());
    //    result = cstring_to_text(out_string); pfree(out_string);
    // TODO(pg-port): real locale-aware case mapping needs utils/formatting.c
    //   (str_tolower) + utils/pg_locale.h; neither is ported.  We fold ASCII A-Z
    //   only, which is correct for the C locale / ASCII data.
    result = ascii_case_fold(
        VARDATA_ANY(in_string as *const c_char),
        VARSIZE_ANY_EXHDR(in_string as *const c_char) as c_int,
        FoldKind::Lower,
    );

    return PointerGetDatum(result as *const c_void); // PG_RETURN_TEXT_P
}

/********************************************************************
 *
 * upper
 *
 * Syntax:
 *
 *	 text upper(text string)
 *
 * Purpose:
 *
 *	 Returns string, with all letters forced to uppercase.
 *
 ********************************************************************/
pub unsafe fn upper(fcinfo: FunctionCallInfo) -> Datum {
    let in_string: *mut text = PG_GETARG_TEXT_PP(fcinfo, 0);
    let result: *mut text;

    // C: out_string = str_toupper(...); result = cstring_to_text(out_string); ...
    // TODO(pg-port): str_toupper (formatting.c) + pg_locale not ported; ASCII fold.
    result = ascii_case_fold(
        VARDATA_ANY(in_string as *const c_char),
        VARSIZE_ANY_EXHDR(in_string as *const c_char) as c_int,
        FoldKind::Upper,
    );

    return PointerGetDatum(result as *const c_void); // PG_RETURN_TEXT_P
}

/********************************************************************
 *
 * initcap
 *
 * Syntax:
 *
 *	 text initcap(text string)
 *
 * Purpose:
 *
 *	 Returns string, with first letter of each word in uppercase, all
 *	 other letters in lowercase. A word is defined as a sequence of
 *	 alphanumeric characters, delimited by non-alphanumeric
 *	 characters.
 *
 ********************************************************************/
pub unsafe fn initcap(fcinfo: FunctionCallInfo) -> Datum {
    let in_string: *mut text = PG_GETARG_TEXT_PP(fcinfo, 0);
    let result: *mut text;

    // C: out_string = str_initcap(...); result = cstring_to_text(out_string); ...
    // TODO(pg-port): str_initcap (formatting.c) + pg_locale not ported; ASCII fold.
    result = ascii_case_fold(
        VARDATA_ANY(in_string as *const c_char),
        VARSIZE_ANY_EXHDR(in_string as *const c_char) as c_int,
        FoldKind::Initcap,
    );

    return PointerGetDatum(result as *const c_void); // PG_RETURN_TEXT_P
}

pub unsafe fn casefold(fcinfo: FunctionCallInfo) -> Datum {
    let in_string: *mut text = PG_GETARG_TEXT_PP(fcinfo, 0);
    let result: *mut text;

    // C: out_string = str_casefold(...); result = cstring_to_text(out_string); ...
    // TODO(pg-port): str_casefold (formatting.c) + pg_locale not ported.  Unicode
    //   case folding (e.g. the German sharp-s, Greek final sigma) is not modeled;
    //   ASCII folding (to lower) is used as the approximation.
    result = ascii_case_fold(
        VARDATA_ANY(in_string as *const c_char),
        VARSIZE_ANY_EXHDR(in_string as *const c_char) as c_int,
        FoldKind::Lower,
    );

    return PointerGetDatum(result as *const c_void); // PG_RETURN_TEXT_P
}

/*
 * ASCII-only case-folding helper used by lower/upper/initcap/casefold above,
 * standing in for the not-yet-ported str_*() routines from formatting.c.
 *
 * Builds a freshly-palloc'd text of the same byte length (ASCII fold never
 * changes length).  Non-ASCII bytes are passed through unchanged.
 *
 * # Safety
 * `s` is readable for `len` bytes.
 */
enum FoldKind {
    Lower,
    Upper,
    Initcap,
}

unsafe fn ascii_case_fold(s: *const c_char, len: c_int, kind: FoldKind) -> *mut text {
    let result: *mut text = cstring_to_text_with_len(s, len);
    let out: *mut c_char = VARDATA(result as *const c_char);

    let mut in_word = false;
    let mut i: c_int = 0;
    while i < len {
        let ch = *out.add(i as usize) as u8;
        let folded = match kind {
            FoldKind::Lower => ascii_tolower(ch),
            FoldKind::Upper => ascii_toupper(ch),
            FoldKind::Initcap => {
                /* word = run of alphanumeric chars; uppercase first, rest lower */
                if ch.is_ascii_alphanumeric() {
                    let f = if in_word { ascii_tolower(ch) } else { ascii_toupper(ch) };
                    in_word = true;
                    f
                } else {
                    in_word = false;
                    ch
                }
            }
        };
        *out.add(i as usize) = folded as c_char;
        i += 1;
    }

    result
}

#[inline]
fn ascii_tolower(c: u8) -> u8 {
    if c.is_ascii_uppercase() {
        c + (b'a' - b'A')
    } else {
        c
    }
}

#[inline]
fn ascii_toupper(c: u8) -> u8 {
    if c.is_ascii_lowercase() {
        c - (b'a' - b'A')
    } else {
        c
    }
}

/********************************************************************
 *
 * lpad
 *
 * Syntax:
 *
 *	 text lpad(text string1, int4 len, text string2)
 *
 * Purpose:
 *
 *	 Returns string1, left-padded to length len with the sequence of
 *	 characters in string2.  If len is less than the length of string1,
 *	 instead truncate (on the right) to len.
 *
 ********************************************************************/
pub unsafe fn lpad(fcinfo: FunctionCallInfo) -> Datum {
    let string1: *mut text = PG_GETARG_TEXT_PP(fcinfo, 0);
    let mut len: int32 = PG_GETARG_INT32!(fcinfo, 1);
    let string2: *mut text = PG_GETARG_TEXT_PP(fcinfo, 2);
    let ret: *mut text;
    let mut ptr1: *const c_char;
    let mut ptr2: *const c_char;
    let ptr2start: *const c_char;
    let mut ptr_ret: *mut c_char;
    let ptr2end: *const c_char;
    let mut m: int32;
    let mut s1len: int32;
    let mut s2len: int32;
    let mut bytelen: int32 = 0;

    /* Negative len is silently taken as zero */
    if len < 0 {
        len = 0;
    }

    s1len = VARSIZE_ANY_EXHDR(string1 as *const c_char) as int32;
    if s1len < 0 {
        s1len = 0; /* shouldn't happen */
    }

    s2len = VARSIZE_ANY_EXHDR(string2 as *const c_char) as int32;
    if s2len < 0 {
        s2len = 0; /* shouldn't happen */
    }

    s1len = pg_mbstrlen_with_len(VARDATA_ANY(string1 as *const c_char), s1len);

    if s1len > len {
        s1len = len; /* truncate string1 to len chars */
    }

    if s2len <= 0 {
        len = s1len; /* nothing to pad with, so don't pad */
    }

    /* compute worst-case output length */
    if unlikely(pg_mul_s32_overflow(pg_database_encoding_max_length(), len, &mut bytelen))
        || unlikely(pg_add_s32_overflow(bytelen, VARHDRSZ, &mut bytelen))
        || unlikely(!AllocSizeIsValid(bytelen as Size))
    {
        let _ = errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED);
        ereport!(ERROR, errmsg!("requested length too large"));
    }

    ret = palloc(bytelen as Size) as *mut text;

    m = len - s1len;

    ptr2 = VARDATA_ANY(string2 as *const c_char);
    ptr2start = ptr2;
    ptr2end = ptr2.add(s2len as usize);
    ptr_ret = VARDATA(ret as *const c_char);

    while m > 0 {
        let mlen: c_int = pg_mblen_range(ptr2, ptr2end);

        memcpy(ptr_ret as *mut c_void, ptr2 as *const c_void, mlen as usize);
        ptr_ret = ptr_ret.add(mlen as usize);
        ptr2 = ptr2.add(mlen as usize);
        if ptr2 == ptr2end {
            /* wrap around at end of s2 */
            ptr2 = ptr2start;
        }
        m -= 1;
    }

    ptr1 = VARDATA_ANY(string1 as *const c_char);

    while s1len > 0 {
        let mlen: c_int = pg_mblen_unbounded(ptr1);

        memcpy(ptr_ret as *mut c_void, ptr1 as *const c_void, mlen as usize);
        ptr_ret = ptr_ret.add(mlen as usize);
        ptr1 = ptr1.add(mlen as usize);
        s1len -= 1;
    }

    SET_VARSIZE(ret as *mut c_char, (ptr_ret as isize - ret as isize) as int32);

    return PointerGetDatum(ret as *const c_void); // PG_RETURN_TEXT_P
}

/********************************************************************
 *
 * rpad
 *
 * Syntax:
 *
 *	 text rpad(text string1, int4 len, text string2)
 *
 * Purpose:
 *
 *	 Returns string1, right-padded to length len with the sequence of
 *	 characters in string2.  If len is less than the length of string1,
 *	 instead truncate (on the right) to len.
 *
 ********************************************************************/
pub unsafe fn rpad(fcinfo: FunctionCallInfo) -> Datum {
    let string1: *mut text = PG_GETARG_TEXT_PP(fcinfo, 0);
    let mut len: int32 = PG_GETARG_INT32!(fcinfo, 1);
    let string2: *mut text = PG_GETARG_TEXT_PP(fcinfo, 2);
    let ret: *mut text;
    let mut ptr1: *const c_char;
    let mut ptr2: *const c_char;
    let ptr2start: *const c_char;
    let mut ptr_ret: *mut c_char;
    let ptr2end: *const c_char;
    let mut m: int32;
    let mut s1len: int32;
    let mut s2len: int32;
    let mut bytelen: int32 = 0;

    /* Negative len is silently taken as zero */
    if len < 0 {
        len = 0;
    }

    s1len = VARSIZE_ANY_EXHDR(string1 as *const c_char) as int32;
    if s1len < 0 {
        s1len = 0; /* shouldn't happen */
    }

    s2len = VARSIZE_ANY_EXHDR(string2 as *const c_char) as int32;
    if s2len < 0 {
        s2len = 0; /* shouldn't happen */
    }

    s1len = pg_mbstrlen_with_len(VARDATA_ANY(string1 as *const c_char), s1len);

    if s1len > len {
        s1len = len; /* truncate string1 to len chars */
    }

    if s2len <= 0 {
        len = s1len; /* nothing to pad with, so don't pad */
    }

    /* compute worst-case output length */
    if unlikely(pg_mul_s32_overflow(pg_database_encoding_max_length(), len, &mut bytelen))
        || unlikely(pg_add_s32_overflow(bytelen, VARHDRSZ, &mut bytelen))
        || unlikely(!AllocSizeIsValid(bytelen as Size))
    {
        let _ = errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED);
        ereport!(ERROR, errmsg!("requested length too large"));
    }

    ret = palloc(bytelen as Size) as *mut text;

    m = len - s1len;

    ptr1 = VARDATA_ANY(string1 as *const c_char);

    ptr_ret = VARDATA(ret as *const c_char);

    while s1len > 0 {
        let mlen: c_int = pg_mblen_unbounded(ptr1);

        memcpy(ptr_ret as *mut c_void, ptr1 as *const c_void, mlen as usize);
        ptr_ret = ptr_ret.add(mlen as usize);
        ptr1 = ptr1.add(mlen as usize);
        s1len -= 1;
    }

    ptr2 = VARDATA_ANY(string2 as *const c_char);
    ptr2start = ptr2;
    ptr2end = ptr2.add(s2len as usize);

    while m > 0 {
        let mlen: c_int = pg_mblen_range(ptr2, ptr2end);

        memcpy(ptr_ret as *mut c_void, ptr2 as *const c_void, mlen as usize);
        ptr_ret = ptr_ret.add(mlen as usize);
        ptr2 = ptr2.add(mlen as usize);
        if ptr2 == ptr2end {
            /* wrap around at end of s2 */
            ptr2 = ptr2start;
        }
        m -= 1;
    }

    SET_VARSIZE(ret as *mut c_char, (ptr_ret as isize - ret as isize) as int32);

    return PointerGetDatum(ret as *const c_void); // PG_RETURN_TEXT_P
}

/********************************************************************
 *
 * btrim
 *
 * Syntax:
 *
 *	 text btrim(text string, text set)
 *
 * Purpose:
 *
 *	 Returns string with characters removed from the front and back
 *	 up to the first character not in set.
 *
 ********************************************************************/
pub unsafe fn btrim(fcinfo: FunctionCallInfo) -> Datum {
    let string: *mut text = PG_GETARG_TEXT_PP(fcinfo, 0);
    let set: *mut text = PG_GETARG_TEXT_PP(fcinfo, 1);
    let ret: *mut text;

    ret = dotrim(
        VARDATA_ANY(string as *const c_char),
        VARSIZE_ANY_EXHDR(string as *const c_char) as c_int,
        VARDATA_ANY(set as *const c_char),
        VARSIZE_ANY_EXHDR(set as *const c_char) as c_int,
        true,
        true,
    );

    return PointerGetDatum(ret as *const c_void); // PG_RETURN_TEXT_P
}

/********************************************************************
 *
 * btrim1 --- btrim with set fixed as ' '
 *
 ********************************************************************/
pub unsafe fn btrim1(fcinfo: FunctionCallInfo) -> Datum {
    let string: *mut text = PG_GETARG_TEXT_PP(fcinfo, 0);
    let ret: *mut text;

    ret = dotrim(
        VARDATA_ANY(string as *const c_char),
        VARSIZE_ANY_EXHDR(string as *const c_char) as c_int,
        c" ".as_ptr(),
        1,
        true,
        true,
    );

    return PointerGetDatum(ret as *const c_void); // PG_RETURN_TEXT_P
}

/*
 * Common implementation for btrim, ltrim, rtrim
 *
 * # Safety
 * `string`/`set` are readable for `stringlen`/`setlen` bytes respectively.
 */
unsafe fn dotrim(
    mut string: *const c_char,
    mut stringlen: c_int,
    set: *const c_char,
    setlen: c_int,
    doltrim: bool,
    dortrim: bool,
) -> *mut text {
    let mut i: c_int;

    /* Nothing to do if either string or set is empty */
    if stringlen > 0 && setlen > 0 {
        if pg_database_encoding_max_length() > 1 {
            /*
             * In the multibyte-encoding case, build arrays of pointers to
             * character starts, so that we can avoid inefficient checks in
             * the inner loops.
             */
            let stringchars: *mut *const c_char;
            let setchars: *mut *const c_char;
            let setend: *const c_char;
            let stringmblen: *mut c_int;
            let setmblen: *mut c_int;
            let mut stringnchars: c_int;
            let mut setnchars: c_int;
            let mut resultndx: c_int;
            let mut resultnchars: c_int;
            let mut p: *const c_char;
            let pend: *const c_char;
            let mut len: c_int;
            let mut mblen: c_int;
            let mut str_pos: *const c_char;
            let mut str_len: c_int;

            stringchars =
                palloc(stringlen as Size * core::mem::size_of::<*const c_char>()) as *mut *const c_char;
            stringmblen = palloc(stringlen as Size * core::mem::size_of::<c_int>()) as *mut c_int;
            stringnchars = 0;
            p = string;
            len = stringlen;
            pend = p.add(len as usize);
            while len > 0 {
                *stringchars.add(stringnchars as usize) = p;
                mblen = pg_mblen_range(p, pend);
                *stringmblen.add(stringnchars as usize) = mblen;
                stringnchars += 1;
                p = p.add(mblen as usize);
                len -= mblen;
            }

            setchars =
                palloc(setlen as Size * core::mem::size_of::<*const c_char>()) as *mut *const c_char;
            setmblen = palloc(setlen as Size * core::mem::size_of::<c_int>()) as *mut c_int;
            setnchars = 0;
            p = set;
            len = setlen;
            setend = set.add(setlen as usize);
            while len > 0 {
                *setchars.add(setnchars as usize) = p;
                mblen = pg_mblen_range(p, setend);
                *setmblen.add(setnchars as usize) = mblen;
                setnchars += 1;
                p = p.add(mblen as usize);
                len -= mblen;
            }

            resultndx = 0; /* index in stringchars[] */
            resultnchars = stringnchars;

            if doltrim {
                while resultnchars > 0 {
                    str_pos = *stringchars.add(resultndx as usize);
                    str_len = *stringmblen.add(resultndx as usize);
                    i = 0;
                    while i < setnchars {
                        if str_len == *setmblen.add(i as usize)
                            && memcmp(
                                str_pos as *const c_void,
                                *setchars.add(i as usize) as *const c_void,
                                str_len as usize,
                            ) == 0
                        {
                            break;
                        }
                        i += 1;
                    }
                    if i >= setnchars {
                        break; /* no match here */
                    }
                    string = string.add(str_len as usize);
                    stringlen -= str_len;
                    resultndx += 1;
                    resultnchars -= 1;
                }
            }

            if dortrim {
                while resultnchars > 0 {
                    str_pos = *stringchars.add((resultndx + resultnchars - 1) as usize);
                    str_len = *stringmblen.add((resultndx + resultnchars - 1) as usize);
                    i = 0;
                    while i < setnchars {
                        if str_len == *setmblen.add(i as usize)
                            && memcmp(
                                str_pos as *const c_void,
                                *setchars.add(i as usize) as *const c_void,
                                str_len as usize,
                            ) == 0
                        {
                            break;
                        }
                        i += 1;
                    }
                    if i >= setnchars {
                        break; /* no match here */
                    }
                    stringlen -= str_len;
                    resultnchars -= 1;
                }
            }

            pfree(stringchars as *mut c_void);
            pfree(stringmblen as *mut c_void);
            pfree(setchars as *mut c_void);
            pfree(setmblen as *mut c_void);
        } else {
            /*
             * In the single-byte-encoding case, we don't need such overhead.
             */
            if doltrim {
                while stringlen > 0 {
                    let str_ch: c_char = *string;

                    i = 0;
                    while i < setlen {
                        if str_ch == *set.add(i as usize) {
                            break;
                        }
                        i += 1;
                    }
                    if i >= setlen {
                        break; /* no match here */
                    }
                    string = string.add(1);
                    stringlen -= 1;
                }
            }

            if dortrim {
                while stringlen > 0 {
                    let str_ch: c_char = *string.add((stringlen - 1) as usize);

                    i = 0;
                    while i < setlen {
                        if str_ch == *set.add(i as usize) {
                            break;
                        }
                        i += 1;
                    }
                    if i >= setlen {
                        break; /* no match here */
                    }
                    stringlen -= 1;
                }
            }
        }
    }

    /* Return selected portion of string */
    cstring_to_text_with_len(string, stringlen)
}

/*
 * Common implementation for bytea versions of btrim, ltrim, rtrim
 *
 * # Safety
 * `string`/`set` are valid (already-detoasted) bytea varlenas.
 */
unsafe fn dobyteatrim(
    string: *mut bytea,
    set: *mut bytea,
    doltrim: bool,
    dortrim: bool,
) -> *mut bytea {
    let ret: *mut bytea;
    let mut ptr: *const c_char;
    let mut end: *const c_char;
    let mut ptr2: *const c_char;
    let ptr2start: *const c_char;
    let end2: *const c_char;
    let mut m: c_int;
    let stringlen: c_int;
    let setlen: c_int;

    stringlen = VARSIZE_ANY_EXHDR(string as *const c_char) as c_int;
    setlen = VARSIZE_ANY_EXHDR(set as *const c_char) as c_int;

    if stringlen <= 0 || setlen <= 0 {
        return string;
    }

    m = stringlen;
    ptr = VARDATA_ANY(string as *const c_char);
    end = ptr.add((stringlen - 1) as usize);
    ptr2start = VARDATA_ANY(set as *const c_char);
    end2 = ptr2start.add((setlen - 1) as usize);

    if doltrim {
        while m > 0 {
            ptr2 = ptr2start;
            while ptr2 <= end2 {
                if *ptr == *ptr2 {
                    break;
                }
                ptr2 = ptr2.add(1);
            }
            if ptr2 > end2 {
                break;
            }
            ptr = ptr.add(1);
            m -= 1;
        }
    }

    if dortrim {
        while m > 0 {
            ptr2 = ptr2start;
            while ptr2 <= end2 {
                if *end == *ptr2 {
                    break;
                }
                ptr2 = ptr2.add(1);
            }
            if ptr2 > end2 {
                break;
            }
            end = end.sub(1);
            m -= 1;
        }
    }

    ret = palloc((VARHDRSZ + m) as Size) as *mut bytea;
    SET_VARSIZE(ret as *mut c_char, VARHDRSZ + m);
    memcpy(
        VARDATA(ret as *const c_char) as *mut c_void,
        ptr as *const c_void,
        m as usize,
    );
    ret
}

/********************************************************************
 *
 * byteatrim
 *
 * Syntax:
 *
 *	 bytea byteatrim(bytea string, bytea set)
 *
 * Purpose:
 *
 *	 Returns string with characters removed from the front and back
 *	 up to the first character not in set.
 *
 * Cloned from btrim and modified as required.
 ********************************************************************/
pub unsafe fn byteatrim(fcinfo: FunctionCallInfo) -> Datum {
    let string: *mut bytea = PG_GETARG_BYTEA_PP!(fcinfo, 0);
    let set: *mut bytea = PG_GETARG_BYTEA_PP!(fcinfo, 1);
    let ret: *mut bytea;

    ret = dobyteatrim(string, set, true, true);

    return PointerGetDatum(ret as *const c_void); // PG_RETURN_BYTEA_P
}

/********************************************************************
 *
 * bytealtrim
 *
 * Syntax:
 *
 *	 bytea bytealtrim(bytea string, bytea set)
 *
 * Purpose:
 *
 *	 Returns string with initial characters removed up to the first
 *	 character not in set.
 *
 ********************************************************************/
pub unsafe fn bytealtrim(fcinfo: FunctionCallInfo) -> Datum {
    let string: *mut bytea = PG_GETARG_BYTEA_PP!(fcinfo, 0);
    let set: *mut bytea = PG_GETARG_BYTEA_PP!(fcinfo, 1);
    let ret: *mut bytea;

    ret = dobyteatrim(string, set, true, false);

    return PointerGetDatum(ret as *const c_void); // PG_RETURN_BYTEA_P
}

/********************************************************************
 *
 * byteartrim
 *
 * Syntax:
 *
 *	 bytea byteartrim(bytea string, bytea set)
 *
 * Purpose:
 *
 *	 Returns string with final characters removed after the last
 *	 character not in set.
 *
 ********************************************************************/
pub unsafe fn byteartrim(fcinfo: FunctionCallInfo) -> Datum {
    let string: *mut bytea = PG_GETARG_BYTEA_PP!(fcinfo, 0);
    let set: *mut bytea = PG_GETARG_BYTEA_PP!(fcinfo, 1);
    let ret: *mut bytea;

    ret = dobyteatrim(string, set, false, true);

    return PointerGetDatum(ret as *const c_void); // PG_RETURN_BYTEA_P
}

/********************************************************************
 *
 * ltrim
 *
 * Syntax:
 *
 *	 text ltrim(text string, text set)
 *
 * Purpose:
 *
 *	 Returns string with initial characters removed up to the first
 *	 character not in set.
 *
 ********************************************************************/
pub unsafe fn ltrim(fcinfo: FunctionCallInfo) -> Datum {
    let string: *mut text = PG_GETARG_TEXT_PP(fcinfo, 0);
    let set: *mut text = PG_GETARG_TEXT_PP(fcinfo, 1);
    let ret: *mut text;

    ret = dotrim(
        VARDATA_ANY(string as *const c_char),
        VARSIZE_ANY_EXHDR(string as *const c_char) as c_int,
        VARDATA_ANY(set as *const c_char),
        VARSIZE_ANY_EXHDR(set as *const c_char) as c_int,
        true,
        false,
    );

    return PointerGetDatum(ret as *const c_void); // PG_RETURN_TEXT_P
}

/********************************************************************
 *
 * ltrim1 --- ltrim with set fixed as ' '
 *
 ********************************************************************/
pub unsafe fn ltrim1(fcinfo: FunctionCallInfo) -> Datum {
    let string: *mut text = PG_GETARG_TEXT_PP(fcinfo, 0);
    let ret: *mut text;

    ret = dotrim(
        VARDATA_ANY(string as *const c_char),
        VARSIZE_ANY_EXHDR(string as *const c_char) as c_int,
        c" ".as_ptr(),
        1,
        true,
        false,
    );

    return PointerGetDatum(ret as *const c_void); // PG_RETURN_TEXT_P
}

/********************************************************************
 *
 * rtrim
 *
 * Syntax:
 *
 *	 text rtrim(text string, text set)
 *
 * Purpose:
 *
 *	 Returns string with final characters removed after the last
 *	 character not in set.
 *
 ********************************************************************/
pub unsafe fn rtrim(fcinfo: FunctionCallInfo) -> Datum {
    let string: *mut text = PG_GETARG_TEXT_PP(fcinfo, 0);
    let set: *mut text = PG_GETARG_TEXT_PP(fcinfo, 1);
    let ret: *mut text;

    ret = dotrim(
        VARDATA_ANY(string as *const c_char),
        VARSIZE_ANY_EXHDR(string as *const c_char) as c_int,
        VARDATA_ANY(set as *const c_char),
        VARSIZE_ANY_EXHDR(set as *const c_char) as c_int,
        false,
        true,
    );

    return PointerGetDatum(ret as *const c_void); // PG_RETURN_TEXT_P
}

/********************************************************************
 *
 * rtrim1 --- rtrim with set fixed as ' '
 *
 ********************************************************************/
pub unsafe fn rtrim1(fcinfo: FunctionCallInfo) -> Datum {
    let string: *mut text = PG_GETARG_TEXT_PP(fcinfo, 0);
    let ret: *mut text;

    ret = dotrim(
        VARDATA_ANY(string as *const c_char),
        VARSIZE_ANY_EXHDR(string as *const c_char) as c_int,
        c" ".as_ptr(),
        1,
        false,
        true,
    );

    return PointerGetDatum(ret as *const c_void); // PG_RETURN_TEXT_P
}

/********************************************************************
 *
 * translate
 *
 * Syntax:
 *
 *	 text translate(text string, text from, text to)
 *
 * Purpose:
 *
 *	 Returns string after replacing all occurrences of characters in from
 *	 with the corresponding character in to.  If from is longer than to,
 *	 occurrences of the extra characters in from are deleted.
 *	 Improved by Edwin Ramirez <ramirez@doc.mssm.edu>.
 *
 ********************************************************************/
pub unsafe fn translate(fcinfo: FunctionCallInfo) -> Datum {
    let string: *mut text = PG_GETARG_TEXT_PP(fcinfo, 0);
    let from: *mut text = PG_GETARG_TEXT_PP(fcinfo, 1);
    let to: *mut text = PG_GETARG_TEXT_PP(fcinfo, 2);
    let result: *mut text;
    let from_ptr: *const c_char;
    let to_ptr: *const c_char;
    let to_end: *const c_char;
    let mut source: *const c_char;
    let mut target: *mut c_char;
    let source_end: *const c_char;
    let from_end: *const c_char;
    let mut m: c_int;
    let fromlen: c_int;
    let tolen: c_int;
    let mut retlen: c_int;
    let mut i: c_int;
    let mut bytelen: int32 = 0;
    let mut len: c_int;
    let mut source_len: c_int;
    let mut from_index: c_int;

    m = VARSIZE_ANY_EXHDR(string as *const c_char) as c_int;
    if m <= 0 {
        return PointerGetDatum(string as *const c_void); // PG_RETURN_TEXT_P
    }
    source = VARDATA_ANY(string as *const c_char);
    source_end = source.add(m as usize);

    fromlen = VARSIZE_ANY_EXHDR(from as *const c_char) as c_int;
    from_ptr = VARDATA_ANY(from as *const c_char);
    from_end = from_ptr.add(fromlen as usize);
    tolen = VARSIZE_ANY_EXHDR(to as *const c_char) as c_int;
    to_ptr = VARDATA_ANY(to as *const c_char);
    to_end = to_ptr.add(tolen as usize);

    /*
     * The worst-case expansion is to substitute a max-length character for a
     * single-byte character at each position of the string.
     */
    if unlikely(pg_mul_s32_overflow(pg_database_encoding_max_length(), m, &mut bytelen))
        || unlikely(pg_add_s32_overflow(bytelen, VARHDRSZ, &mut bytelen))
        || unlikely(!AllocSizeIsValid(bytelen as Size))
    {
        let _ = errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED);
        ereport!(ERROR, errmsg!("requested length too large"));
    }

    result = palloc(bytelen as Size) as *mut text;

    target = VARDATA(result as *const c_char);
    retlen = 0;

    while m > 0 {
        source_len = pg_mblen_range(source, source_end);
        from_index = 0;

        i = 0;
        while i < fromlen {
            len = pg_mblen_range(from_ptr.add(i as usize), from_end);
            if len == source_len
                && memcmp(
                    source as *const c_void,
                    from_ptr.add(i as usize) as *const c_void,
                    len as usize,
                ) == 0
            {
                break;
            }

            from_index += 1;
            i += len;
        }
        if i < fromlen {
            /* substitute, or delete if no corresponding "to" character */
            let mut p: *const c_char = to_ptr;

            i = 0;
            while i < from_index {
                if p >= to_end {
                    break;
                }
                p = p.add(pg_mblen_range(p, to_end) as usize);
                i += 1;
            }
            if p < to_end {
                len = pg_mblen_range(p, to_end);
                memcpy(target as *mut c_void, p as *const c_void, len as usize);
                target = target.add(len as usize);
                retlen += len;
            }
        } else {
            /* no match, so copy */
            memcpy(target as *mut c_void, source as *const c_void, source_len as usize);
            target = target.add(source_len as usize);
            retlen += source_len;
        }

        source = source.add(source_len as usize);
        m -= source_len;
    }

    SET_VARSIZE(result as *mut c_char, retlen + VARHDRSZ);

    /*
     * The function result is probably much bigger than needed, if we're using
     * a multibyte encoding, but it's not worth reallocating it; the result
     * probably won't live long anyway.
     */

    return PointerGetDatum(result as *const c_void); // PG_RETURN_TEXT_P
}

/*
 * ascii() and chr() (which physically live in oracle_compat.c) are translated in
 * crate::utils::adt::ascii alongside the rest of the ASCII family -- they are NOT
 * duplicated here.  See src/utils/adt/ascii.rs.
 */

/********************************************************************
 *
 * repeat
 *
 * Syntax:
 *
 *	 text repeat(text string, int val)
 *
 * Purpose:
 *
 *	Repeat string by val.
 *
 ********************************************************************/
pub unsafe fn repeat(fcinfo: FunctionCallInfo) -> Datum {
    let string: *mut text = PG_GETARG_TEXT_PP(fcinfo, 0);
    let mut count: int32 = PG_GETARG_INT32!(fcinfo, 1);
    let result: *mut text;
    let slen: int32;
    let mut tlen: int32 = 0;
    let mut i: int32;
    let mut cp: *mut c_char;
    let sp: *const c_char;

    if count < 0 {
        count = 0;
    }

    slen = VARSIZE_ANY_EXHDR(string as *const c_char) as int32;

    if unlikely(pg_mul_s32_overflow(count, slen, &mut tlen))
        || unlikely(pg_add_s32_overflow(tlen, VARHDRSZ, &mut tlen))
        || unlikely(!AllocSizeIsValid(tlen as Size))
    {
        let _ = errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED);
        ereport!(ERROR, errmsg!("requested length too large"));
    }

    result = palloc(tlen as Size) as *mut text;

    SET_VARSIZE(result as *mut c_char, tlen);
    cp = VARDATA(result as *const c_char);
    sp = VARDATA_ANY(string as *const c_char);
    i = 0;
    while i < count {
        memcpy(cp as *mut c_void, sp as *const c_void, slen as usize);
        cp = cp.add(slen as usize);
        CHECK_FOR_INTERRUPTS();
        i += 1;
    }

    return PointerGetDatum(result as *const c_void); // PG_RETURN_TEXT_P
}

/// `PG_GETARG_TEXT_PP(n)`: detoast a packed text arg (identity for plain/short
/// datums).  Spelled as a helper per project convention (the fmgr macro routes
/// through the not-yet-ported toast detoaster; crate::varatt::pg_detoast_datum_packed
/// is the real identity-for-plain impl).
///
/// # Safety
/// `fcinfo` holds at least `n+1` args, the n'th being a text Datum.
#[inline]
unsafe fn PG_GETARG_TEXT_PP(fcinfo: FunctionCallInfo, n: usize) -> *mut text {
    crate::varatt::pg_detoast_datum_packed(
        crate::postgres::DatumGetPointer(PG_GETARG_DATUM!(fcinfo, n)) as *mut c_void,
    ) as *mut text
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::postgres::DatumGetPointer;
    use crate::postgres_ext::InvalidOid;
    use crate::utils::adt::varlena::{cstring_to_text, cstring_to_text_with_len, text_to_cstring};
    use crate::utils::fmgr::{DirectFunctionCall1Coll, DirectFunctionCall2Coll, DirectFunctionCall3Coll};
    use crate::postgres::Int32GetDatum;

    unsafe fn cstr_eq(p: *const c_char, want: &str) -> bool {
        let mut n = 0usize;
        while *p.add(n) != 0 {
            n += 1;
        }
        core::slice::from_raw_parts(p as *const u8, n) == want.as_bytes()
    }

    // Build a text datum from a Rust &str (4-byte header).
    unsafe fn mk(s: &str) -> Datum {
        let p = cstring_to_text_with_len(s.as_ptr() as *const c_char, s.len() as c_int);
        PointerGetDatum(p as *const c_void)
    }

    unsafe fn out(d: Datum) -> *mut c_char {
        text_to_cstring(DatumGetPointer(d) as *const text)
    }

    #[test]
    fn case_fold_ascii() {
        unsafe {
            // lower / upper ASCII fold (the default UTF8 DB still folds ASCII A-Z).
            let lo = out(DirectFunctionCall1Coll(lower, InvalidOid, mk("HeLLo Wörld")));
            // 'ö' (0xC3 0xB6) passes through; ASCII letters lowered.
            assert!(cstr_eq(lo, "hello wörld"));

            let up = out(DirectFunctionCall1Coll(upper, InvalidOid, mk("HeLLo world")));
            assert!(cstr_eq(up, "HELLO WORLD"));

            // initcap: first letter of each alnum word upper, rest lower.
            let ic = out(DirectFunctionCall1Coll(initcap, InvalidOid, mk("hi THERE, bob42x")));
            assert!(cstr_eq(ic, "Hi There, Bob42x"));

            // casefold approximates to lower for ASCII.
            let cf = out(DirectFunctionCall1Coll(casefold, InvalidOid, mk("ABC")));
            assert!(cstr_eq(cf, "abc"));
        }
    }

    #[test]
    fn lpad_rpad_pad_and_truncate() {
        unsafe {
            // left pad "hi" to width 5 with "xy" -> "xyxhi"
            let lp = out(DirectFunctionCall3Coll(
                lpad,
                InvalidOid,
                mk("hi"),
                Int32GetDatum(5),
                mk("xy"),
            ));
            assert!(cstr_eq(lp, "xyxhi"));

            // right pad "hi" to width 5 with "xy" -> "hixyx"
            let rp = out(DirectFunctionCall3Coll(
                rpad,
                InvalidOid,
                mk("hi"),
                Int32GetDatum(5),
                mk("xy"),
            ));
            assert!(cstr_eq(rp, "hixyx"));

            // len < input length truncates (lpad truncates on the right too).
            let tr = out(DirectFunctionCall3Coll(
                lpad,
                InvalidOid,
                mk("hello"),
                Int32GetDatum(3),
                mk("."),
            ));
            assert!(cstr_eq(tr, "hel"));

            // empty pad string -> no padding, just (possibly truncated) input.
            let np = out(DirectFunctionCall3Coll(
                rpad,
                InvalidOid,
                mk("ab"),
                Int32GetDatum(5),
                mk(""),
            ));
            assert!(cstr_eq(np, "ab"));

            // negative len -> empty.
            let neg = out(DirectFunctionCall3Coll(
                lpad,
                InvalidOid,
                mk("abc"),
                Int32GetDatum(-3),
                mk("."),
            ));
            assert!(cstr_eq(neg, ""));
        }
    }

    #[test]
    fn trim_family() {
        unsafe {
            // btrim removes set chars from both ends.
            let b = out(DirectFunctionCall2Coll(btrim, InvalidOid, mk("xxhelloxx"), mk("x")));
            assert!(cstr_eq(b, "hello"));

            // ltrim only from the left.
            let l = out(DirectFunctionCall2Coll(ltrim, InvalidOid, mk("xxhelloxx"), mk("x")));
            assert!(cstr_eq(l, "helloxx"));

            // rtrim only from the right.
            let r = out(DirectFunctionCall2Coll(rtrim, InvalidOid, mk("xxhelloxx"), mk("x")));
            assert!(cstr_eq(r, "xxhello"));

            // btrim1 (single-arg) trims spaces.
            let b1 = out(DirectFunctionCall1Coll(btrim1, InvalidOid, mk("   spaced   ")));
            assert!(cstr_eq(b1, "spaced"));

            // ltrim1 / rtrim1 spaces.
            let l1 = out(DirectFunctionCall1Coll(ltrim1, InvalidOid, mk("  x  ")));
            assert!(cstr_eq(l1, "x  "));
            let r1 = out(DirectFunctionCall1Coll(rtrim1, InvalidOid, mk("  x  ")));
            assert!(cstr_eq(r1, "  x"));

            // multi-char set: trim any of "ab" from both ends.
            let mc = out(DirectFunctionCall2Coll(btrim, InvalidOid, mk("ababZab"), mk("ab")));
            assert!(cstr_eq(mc, "Z"));
        }
    }

    #[test]
    fn bytea_trim_family() {
        unsafe {
            // byteatrim: bytea is byte-wise (mk builds a binary-compatible varlena).
            let bt = out(DirectFunctionCall2Coll(byteatrim, InvalidOid, mk("00data00"), mk("0")));
            assert!(cstr_eq(bt, "data"));
            let blt = out(DirectFunctionCall2Coll(bytealtrim, InvalidOid, mk("00data00"), mk("0")));
            assert!(cstr_eq(blt, "data00"));
            let brt = out(DirectFunctionCall2Coll(byteartrim, InvalidOid, mk("00data00"), mk("0")));
            assert!(cstr_eq(brt, "00data"));
        }
    }

    #[test]
    fn translate_maps_and_deletes() {
        unsafe {
            // map a->1, b->2, c->3
            let t = out(DirectFunctionCall3Coll(
                translate,
                InvalidOid,
                mk("abcabc"),
                mk("abc"),
                mk("123"),
            ));
            assert!(cstr_eq(t, "123123"));

            // from longer than to: extra "from" chars are deleted.
            let del = out(DirectFunctionCall3Coll(
                translate,
                InvalidOid,
                mk("hello"),
                mk("lo"),
                mk("L"),
            ));
            // 'l'->'L', 'o' deleted: "heLL"
            assert!(cstr_eq(del, "heLL"));
        }
    }

    #[test]
    fn repeat_repeats() {
        unsafe {
            let r = out(DirectFunctionCall2Coll(repeat, InvalidOid, mk("ab"), Int32GetDatum(3)));
            assert!(cstr_eq(r, "ababab"));
            // negative count -> empty
            let z = out(DirectFunctionCall2Coll(repeat, InvalidOid, mk("ab"), Int32GetDatum(-1)));
            assert!(cstr_eq(z, ""));
        }
    }

    #[test]
    #[should_panic]
    fn repeat_rejects_overlength() {
        unsafe {
            // count * slen overflows the AllocSize limit -> ereport(ERROR).
            let _ = DirectFunctionCall2Coll(
                repeat,
                InvalidOid,
                mk("xxxxxxxxxx"),
                Int32GetDatum(i32::MAX),
            );
        }
    }
}
