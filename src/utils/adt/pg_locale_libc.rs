//! PostgreSQL locale utilities for libc
//!
//! Portions Copyright (c) 2002-2025, PostgreSQL Global Development Group
//!
//! src/backend/utils/adt/pg_locale_libc.c

use crate::prelude::*;

use crate::utils::init::globals::MyDatabaseId;

use std::ffi::{c_char, c_int, c_void};

// libc types/functions used here.
#[allow(non_camel_case_types)]
type wchar_t = i32;
#[allow(non_camel_case_types)]
type locale_t = *mut c_void;
#[allow(non_camel_case_types)]
type ssize_t = isize;

extern "C" {
    fn strlen(s: *const c_char) -> usize;
    fn memcpy(dst: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
    fn strcmp(a: *const c_char, b: *const c_char) -> c_int;

    fn tolower_l(c: c_int, loc: locale_t) -> c_int;
    fn toupper_l(c: c_int, loc: locale_t) -> c_int;
    fn isalnum_l(c: c_int, loc: locale_t) -> c_int;
    fn towlower_l(c: wchar_t, loc: locale_t) -> wchar_t;
    fn towupper_l(c: wchar_t, loc: locale_t) -> wchar_t;
    fn iswalnum_l(c: wchar_t, loc: locale_t) -> c_int;

    fn strcoll_l(a: *const c_char, b: *const c_char, loc: locale_t) -> c_int;
    fn strxfrm_l(dest: *mut c_char, src: *const c_char, n: usize, loc: locale_t) -> usize;

    fn newlocale(category_mask: c_int, locale: *const c_char, base: locale_t) -> locale_t;
    fn freelocale(loc: locale_t);
    fn uselocale(loc: locale_t) -> locale_t;
    fn mbstowcs(dest: *mut wchar_t, src: *const c_char, n: usize) -> usize;
    fn wcstombs(dest: *mut c_char, src: *const wchar_t, n: usize) -> usize;

    // glibc version (used under __GLIBC__)
    fn gnu_get_libc_version() -> *const c_char;
}

/*
 * Size of stack buffer to use for string transformations, used to avoid heap
 * allocations in typical cases. This should be large enough that most strings
 * will fit, but small enough that we feel comfortable putting it on the
 * stack.
 */
const TEXTBUFLEN: usize = 1024;

// libc category masks (Linux/glibc values).
const LC_CTYPE_MASK: c_int = 1 << 0;
const LC_COLLATE_MASK: c_int = 1 << 3;

const INT_MAX: ssize_t = i32::MAX as ssize_t;
const ENOENT: c_int = 2;

static collate_methods_libc: collate_methods = collate_methods {
    strncoll: Some(strncoll_libc),
    strnxfrm: Some(strnxfrm_libc),
    strnxfrm_prefix: None,

    /*
     * Unfortunately, it seems that strxfrm() for non-C collations is broken
     * on many common platforms; testing of multiple versions of glibc reveals
     * that, for many locales, strcoll() and strxfrm() do not return
     * consistent results. While no other libc other than Cygwin has so far
     * been shown to have a problem, we take the conservative course of action
     * for right now and disable this categorically.  (Users who are certain
     * this isn't a problem on their system can define TRUST_STRXFRM.)
     */
    strxfrm_is_safe: false,
};

pub unsafe fn strlower_libc(
    dst: *mut c_char,
    dstsize: usize,
    src: *const c_char,
    srclen: ssize_t,
    locale: pg_locale_t,
) -> usize {
    if pg_database_encoding_max_length() > 1 {
        strlower_libc_mb(dst, dstsize, src, srclen, locale)
    } else {
        strlower_libc_sb(dst, dstsize, src, srclen, locale)
    }
}

pub unsafe fn strtitle_libc(
    dst: *mut c_char,
    dstsize: usize,
    src: *const c_char,
    srclen: ssize_t,
    locale: pg_locale_t,
) -> usize {
    if pg_database_encoding_max_length() > 1 {
        strtitle_libc_mb(dst, dstsize, src, srclen, locale)
    } else {
        strtitle_libc_sb(dst, dstsize, src, srclen, locale)
    }
}

pub unsafe fn strupper_libc(
    dst: *mut c_char,
    dstsize: usize,
    src: *const c_char,
    srclen: ssize_t,
    locale: pg_locale_t,
) -> usize {
    if pg_database_encoding_max_length() > 1 {
        strupper_libc_mb(dst, dstsize, src, srclen, locale)
    } else {
        strupper_libc_sb(dst, dstsize, src, srclen, locale)
    }
}

unsafe fn strlower_libc_sb(
    dest: *mut c_char,
    destsize: usize,
    src: *const c_char,
    mut srclen: ssize_t,
    locale: pg_locale_t,
) -> usize {
    if srclen < 0 {
        srclen = strlen(src) as ssize_t;
    }

    if (srclen + 1) as usize <= destsize {
        let loc: locale_t = (*locale).info.lt;

        if (srclen + 1) as usize > destsize {
            return srclen as usize;
        }

        memcpy(dest as *mut c_void, src as *const c_void, srclen as usize);
        *dest.add(srclen as usize) = b'\0' as c_char;

        /*
         * Note: we assume that tolower_l() will not be so broken as to need
         * an isupper_l() guard test.  When using the default collation, we
         * apply the traditional Postgres behavior that forces ASCII-style
         * treatment of I/i, but in non-default collations you get exactly
         * what the collation says.
         */
        let mut p = dest;
        while *p != 0 {
            if (*locale).is_default {
                *p = pg_tolower(*p as u8) as c_char;
            } else {
                *p = tolower_l((*p as u8) as c_int, loc) as c_char;
            }
            p = p.add(1);
        }
    }

    srclen as usize
}

unsafe fn strlower_libc_mb(
    dest: *mut c_char,
    destsize: usize,
    src: *const c_char,
    mut srclen: ssize_t,
    locale: pg_locale_t,
) -> usize {
    let loc: locale_t = (*locale).info.lt;
    let result_size: usize;
    let workspace: *mut wchar_t;
    let result: *mut c_char;
    let mut curr_char: usize;
    let max_size: usize;

    if srclen < 0 {
        srclen = strlen(src) as ssize_t;
    }

    /* Overflow paranoia */
    if (srclen + 1) > (INT_MAX / std::mem::size_of::<wchar_t>() as ssize_t) {
        ereport!(ERROR, "out of memory");
        unreachable!();
    }

    /* Output workspace cannot have more codes than input bytes */
    workspace = palloc((srclen as usize + 1) * std::mem::size_of::<wchar_t>()) as *mut wchar_t;

    char2wchar(workspace, srclen as usize + 1, src, srclen as usize, locale);

    curr_char = 0;
    while *workspace.add(curr_char) != 0 {
        *workspace.add(curr_char) = towlower_l(*workspace.add(curr_char), loc);
        curr_char += 1;
    }

    /*
     * Make result large enough; case change might change number of bytes
     */
    max_size = curr_char * pg_database_encoding_max_length() as usize;
    result = palloc(max_size + 1) as *mut c_char;

    result_size = wchar2char(result, workspace, max_size + 1, locale);

    if destsize >= result_size + 1 {
        memcpy(dest as *mut c_void, result as *const c_void, result_size);
        *dest.add(result_size) = b'\0' as c_char;
    }

    pfree(workspace as *mut c_void);
    pfree(result as *mut c_void);

    result_size
}

unsafe fn strtitle_libc_sb(
    dest: *mut c_char,
    destsize: usize,
    src: *const c_char,
    mut srclen: ssize_t,
    locale: pg_locale_t,
) -> usize {
    if srclen < 0 {
        srclen = strlen(src) as ssize_t;
    }

    if (srclen + 1) as usize <= destsize {
        let loc: locale_t = (*locale).info.lt;
        let mut wasalnum: c_int = false as c_int;

        memcpy(dest as *mut c_void, src as *const c_void, srclen as usize);
        *dest.add(srclen as usize) = b'\0' as c_char;

        /*
         * Note: we assume that toupper_l()/tolower_l() will not be so broken
         * as to need guard tests.  When using the default collation, we apply
         * the traditional Postgres behavior that forces ASCII-style treatment
         * of I/i, but in non-default collations you get exactly what the
         * collation says.
         */
        let mut p = dest;
        while *p != 0 {
            if (*locale).is_default {
                if wasalnum != 0 {
                    *p = pg_tolower(*p as u8) as c_char;
                } else {
                    *p = pg_toupper(*p as u8) as c_char;
                }
            } else {
                if wasalnum != 0 {
                    *p = tolower_l((*p as u8) as c_int, loc) as c_char;
                } else {
                    *p = toupper_l((*p as u8) as c_int, loc) as c_char;
                }
            }
            wasalnum = isalnum_l((*p as u8) as c_int, loc);
            p = p.add(1);
        }
    }

    srclen as usize
}

unsafe fn strtitle_libc_mb(
    dest: *mut c_char,
    destsize: usize,
    src: *const c_char,
    mut srclen: ssize_t,
    locale: pg_locale_t,
) -> usize {
    let loc: locale_t = (*locale).info.lt;
    let mut wasalnum: c_int = false as c_int;
    let result_size: usize;
    let workspace: *mut wchar_t;
    let result: *mut c_char;
    let mut curr_char: usize;
    let max_size: usize;

    if srclen < 0 {
        srclen = strlen(src) as ssize_t;
    }

    /* Overflow paranoia */
    if (srclen + 1) > (INT_MAX / std::mem::size_of::<wchar_t>() as ssize_t) {
        ereport!(ERROR, "out of memory");
        unreachable!();
    }

    /* Output workspace cannot have more codes than input bytes */
    workspace = palloc((srclen as usize + 1) * std::mem::size_of::<wchar_t>()) as *mut wchar_t;

    char2wchar(workspace, srclen as usize + 1, src, srclen as usize, locale);

    curr_char = 0;
    while *workspace.add(curr_char) != 0 {
        if wasalnum != 0 {
            *workspace.add(curr_char) = towlower_l(*workspace.add(curr_char), loc);
        } else {
            *workspace.add(curr_char) = towupper_l(*workspace.add(curr_char), loc);
        }
        wasalnum = iswalnum_l(*workspace.add(curr_char), loc);
        curr_char += 1;
    }

    /*
     * Make result large enough; case change might change number of bytes
     */
    max_size = curr_char * pg_database_encoding_max_length() as usize;
    result = palloc(max_size + 1) as *mut c_char;

    result_size = wchar2char(result, workspace, max_size + 1, locale);

    if destsize >= result_size + 1 {
        memcpy(dest as *mut c_void, result as *const c_void, result_size);
        *dest.add(result_size) = b'\0' as c_char;
    }

    pfree(workspace as *mut c_void);
    pfree(result as *mut c_void);

    result_size
}

unsafe fn strupper_libc_sb(
    dest: *mut c_char,
    destsize: usize,
    src: *const c_char,
    mut srclen: ssize_t,
    locale: pg_locale_t,
) -> usize {
    if srclen < 0 {
        srclen = strlen(src) as ssize_t;
    }

    if (srclen + 1) as usize <= destsize {
        let loc: locale_t = (*locale).info.lt;

        memcpy(dest as *mut c_void, src as *const c_void, srclen as usize);
        *dest.add(srclen as usize) = b'\0' as c_char;

        /*
         * Note: we assume that toupper_l() will not be so broken as to need
         * an islower_l() guard test.  When using the default collation, we
         * apply the traditional Postgres behavior that forces ASCII-style
         * treatment of I/i, but in non-default collations you get exactly
         * what the collation says.
         */
        let mut p = dest;
        while *p != 0 {
            if (*locale).is_default {
                *p = pg_toupper(*p as u8) as c_char;
            } else {
                *p = toupper_l((*p as u8) as c_int, loc) as c_char;
            }
            p = p.add(1);
        }
    }

    srclen as usize
}

unsafe fn strupper_libc_mb(
    dest: *mut c_char,
    destsize: usize,
    src: *const c_char,
    mut srclen: ssize_t,
    locale: pg_locale_t,
) -> usize {
    let loc: locale_t = (*locale).info.lt;
    let result_size: usize;
    let workspace: *mut wchar_t;
    let result: *mut c_char;
    let mut curr_char: usize;
    let max_size: usize;

    if srclen < 0 {
        srclen = strlen(src) as ssize_t;
    }

    /* Overflow paranoia */
    if (srclen + 1) > (INT_MAX / std::mem::size_of::<wchar_t>() as ssize_t) {
        ereport!(ERROR, "out of memory");
        unreachable!();
    }

    /* Output workspace cannot have more codes than input bytes */
    workspace = palloc((srclen as usize + 1) * std::mem::size_of::<wchar_t>()) as *mut wchar_t;

    char2wchar(workspace, srclen as usize + 1, src, srclen as usize, locale);

    curr_char = 0;
    while *workspace.add(curr_char) != 0 {
        *workspace.add(curr_char) = towupper_l(*workspace.add(curr_char), loc);
        curr_char += 1;
    }

    /*
     * Make result large enough; case change might change number of bytes
     */
    max_size = curr_char * pg_database_encoding_max_length() as usize;
    result = palloc(max_size + 1) as *mut c_char;

    result_size = wchar2char(result, workspace, max_size + 1, locale);

    if destsize >= result_size + 1 {
        memcpy(dest as *mut c_void, result as *const c_void, result_size);
        *dest.add(result_size) = b'\0' as c_char;
    }

    pfree(workspace as *mut c_void);
    pfree(result as *mut c_void);

    result_size
}

pub unsafe fn create_pg_locale_libc(collid: Oid, context: MemoryContext) -> pg_locale_t {
    let collate: *const c_char;
    let ctype: *const c_char;
    let loc: locale_t;
    let result: pg_locale_t;

    if collid == DEFAULT_COLLATION_OID {
        let tp: HeapTuple;
        let mut datum: Datum;

        tp = SearchSysCache1(DATABASEOID, ObjectIdGetDatum(MyDatabaseId));
        if !HeapTupleIsValid(tp) {
            elog!(ERROR, "cache lookup failed for database {}", MyDatabaseId);
        }
        datum = SysCacheGetAttrNotNull(DATABASEOID, tp, Anum_pg_database_datcollate);
        collate = TextDatumGetCString(datum);
        datum = SysCacheGetAttrNotNull(DATABASEOID, tp, Anum_pg_database_datctype);
        ctype = TextDatumGetCString(datum);

        ReleaseSysCache(tp);
    } else {
        let tp: HeapTuple;
        let mut datum: Datum;

        tp = SearchSysCache1(COLLOID, ObjectIdGetDatum(collid));
        if !HeapTupleIsValid(tp) {
            elog!(ERROR, "cache lookup failed for collation {}", collid);
        }

        datum = SysCacheGetAttrNotNull(COLLOID, tp, Anum_pg_collation_collcollate);
        collate = TextDatumGetCString(datum);
        datum = SysCacheGetAttrNotNull(COLLOID, tp, Anum_pg_collation_collctype);
        ctype = TextDatumGetCString(datum);

        ReleaseSysCache(tp);
    }

    loc = make_libc_collator(collate, ctype);

    result = MemoryContextAllocZero(context, std::mem::size_of::<pg_locale_struct>()) as pg_locale_t;
    (*result).provider = COLLPROVIDER_LIBC;
    (*result).deterministic = true;
    (*result).collate_is_c = (strcmp(collate, c"C".as_ptr()) == 0)
        || (strcmp(collate, c"POSIX".as_ptr()) == 0);
    (*result).ctype_is_c = (strcmp(ctype, c"C".as_ptr()) == 0)
        || (strcmp(ctype, c"POSIX".as_ptr()) == 0);
    (*result).info.lt = loc;
    if !(*result).collate_is_c {
        (*result).collate = &collate_methods_libc as *const collate_methods;
    }

    result
}

/*
 * Create a locale_t with the given collation and ctype.
 *
 * The "C" and "POSIX" locales are not actually handled by libc, so return
 * NULL.
 *
 * Ensure that no path leaks a locale_t.
 */
unsafe fn make_libc_collator(collate: *const c_char, ctype: *const c_char) -> locale_t {
    let mut loc: locale_t = std::ptr::null_mut();

    if strcmp(collate, ctype) == 0 {
        if strcmp(ctype, c"C".as_ptr()) != 0 && strcmp(ctype, c"POSIX".as_ptr()) != 0 {
            /* Normal case where they're the same */
            *__errno_location() = 0;
            loc = newlocale(LC_COLLATE_MASK | LC_CTYPE_MASK, collate, std::ptr::null_mut());
            if loc.is_null() {
                report_newlocale_failure(collate);
            }
        }
    } else {
        /* We need two newlocale() steps */
        let mut loc1: locale_t = std::ptr::null_mut();

        if strcmp(collate, c"C".as_ptr()) != 0 && strcmp(collate, c"POSIX".as_ptr()) != 0 {
            *__errno_location() = 0;
            loc1 = newlocale(LC_COLLATE_MASK, collate, std::ptr::null_mut());
            if loc1.is_null() {
                report_newlocale_failure(collate);
            }
        }

        if strcmp(ctype, c"C".as_ptr()) != 0 && strcmp(ctype, c"POSIX".as_ptr()) != 0 {
            *__errno_location() = 0;
            loc = newlocale(LC_CTYPE_MASK, ctype, loc1);
            if loc.is_null() {
                if !loc1.is_null() {
                    freelocale(loc1);
                }
                report_newlocale_failure(ctype);
            }
        } else {
            loc = loc1;
        }
    }

    loc
}

/*
 * strncoll_libc
 *
 * NUL-terminate arguments, if necessary, and pass to strcoll_l().
 *
 * An input string length of -1 means that it's already NUL-terminated.
 */
pub unsafe extern "C" fn strncoll_libc(
    arg1: *const c_char,
    len1: ssize_t,
    arg2: *const c_char,
    len2: ssize_t,
    locale: pg_locale_t,
) -> c_int {
    let mut sbuf: [c_char; TEXTBUFLEN] = [0; TEXTBUFLEN];
    let mut buf: *mut c_char = sbuf.as_mut_ptr();
    let bufsize1: usize = if len1 == -1 { 0 } else { (len1 + 1) as usize };
    let bufsize2: usize = if len2 == -1 { 0 } else { (len2 + 1) as usize };
    let arg1n: *const c_char;
    let arg2n: *const c_char;
    let result: c_int;

    Assert!((*locale).provider == COLLPROVIDER_LIBC);

    if bufsize1 + bufsize2 > TEXTBUFLEN {
        buf = palloc(bufsize1 + bufsize2) as *mut c_char;
    }

    /* nul-terminate arguments if necessary */
    if len1 == -1 {
        arg1n = arg1;
    } else {
        let buf1: *mut c_char = buf;

        memcpy(buf1 as *mut c_void, arg1 as *const c_void, len1 as usize);
        *buf1.add(len1 as usize) = b'\0' as c_char;
        arg1n = buf1;
    }

    if len2 == -1 {
        arg2n = arg2;
    } else {
        let buf2: *mut c_char = buf.add(bufsize1);

        memcpy(buf2 as *mut c_void, arg2 as *const c_void, len2 as usize);
        *buf2.add(len2 as usize) = b'\0' as c_char;
        arg2n = buf2;
    }

    result = strcoll_l(arg1n, arg2n, (*locale).info.lt);

    if buf != sbuf.as_mut_ptr() {
        pfree(buf as *mut c_void);
    }

    result
}

/*
 * strnxfrm_libc
 *
 * NUL-terminate src, if necessary, and pass to strxfrm_l().
 *
 * A source length of -1 means that it's already NUL-terminated.
 */
pub unsafe extern "C" fn strnxfrm_libc(
    dest: *mut c_char,
    destsize: usize,
    src: *const c_char,
    srclen: ssize_t,
    locale: pg_locale_t,
) -> usize {
    let mut sbuf: [c_char; TEXTBUFLEN] = [0; TEXTBUFLEN];
    let mut buf: *mut c_char = sbuf.as_mut_ptr();
    let bufsize: usize = (srclen + 1) as usize;
    let result: usize;

    Assert!((*locale).provider == COLLPROVIDER_LIBC);

    if srclen == -1 {
        return strxfrm_l(dest, src, destsize, (*locale).info.lt);
    }

    if bufsize > TEXTBUFLEN {
        buf = palloc(bufsize) as *mut c_char;
    }

    /* nul-terminate argument */
    memcpy(buf as *mut c_void, src as *const c_void, srclen as usize);
    *buf.add(srclen as usize) = b'\0' as c_char;

    result = strxfrm_l(dest, buf, destsize, (*locale).info.lt);

    if buf != sbuf.as_mut_ptr() {
        pfree(buf as *mut c_void);
    }

    /* if dest is defined, it should be nul-terminated */
    Assert!(result >= destsize || *dest.add(result) == b'\0' as c_char);

    result
}

pub unsafe fn get_collation_actual_version_libc(collcollate: *const c_char) -> *mut c_char {
    let mut collversion: *mut c_char = std::ptr::null_mut();

    if pg_strcasecmp(c"C".as_ptr(), collcollate) != 0
        && pg_strncasecmp(c"C.".as_ptr(), collcollate, 2) != 0
        && pg_strcasecmp(c"POSIX".as_ptr(), collcollate) != 0
    {
        /* Use the glibc version because we don't have anything better. */
        collversion = pstrdup(gnu_get_libc_version());
    }

    collversion
}

/* simple subroutine for reporting errors from newlocale() */
pub unsafe fn report_newlocale_failure(localename: *const c_char) {
    let _save_errno: c_int;

    /*
     * Windows doesn't provide any useful error indication from
     * _create_locale(), and BSD-derived platforms don't seem to feel they
     * need to set errno either (even though POSIX is pretty clear that
     * newlocale should do so).  So, if errno hasn't been set, assume ENOENT
     * is what to report.
     */
    if *__errno_location() == 0 {
        *__errno_location() = ENOENT;
    }

    /*
     * ENOENT means "no such locale", not "no such file", so clarify that
     * errno with an errdetail message.
     */
    _save_errno = *__errno_location(); /* auxiliary funcs might change errno */
    let _ = localename;
    ereport!(ERROR, "could not create locale");
    unreachable!();
}

/*
 * POSIX doesn't define _l-variants of these functions, but several systems
 * have them.  We provide our own replacements here.
 */
unsafe fn mbstowcs_l(dest: *mut wchar_t, src: *const c_char, n: usize, loc: locale_t) -> usize {
    let result: usize;
    let save_locale: locale_t = uselocale(loc);

    result = mbstowcs(dest, src, n);
    uselocale(save_locale);
    result
}

unsafe fn wcstombs_l(dest: *mut c_char, src: *const wchar_t, n: usize, loc: locale_t) -> usize {
    let result: usize;
    let save_locale: locale_t = uselocale(loc);

    result = wcstombs(dest, src, n);
    uselocale(save_locale);
    result
}

/*
 * These functions convert from/to libc's wchar_t, *not* pg_wchar_t.
 * Therefore we keep them here rather than with the mbutils code.
 */

/*
 * wchar2char --- convert wide characters to multibyte format
 *
 * This has the same API as the standard wcstombs_l() function; in particular,
 * tolen is the maximum number of bytes to store at *to, and *from must be
 * zero-terminated.  The output will be zero-terminated iff there is room.
 */
pub unsafe fn wchar2char(
    to: *mut c_char,
    from: *const wchar_t,
    tolen: usize,
    locale: pg_locale_t,
) -> usize {
    let result: usize;

    if tolen == 0 {
        return 0;
    }

    if locale == (0 as pg_locale_t) {
        /* Use wcstombs directly for the default locale */
        result = wcstombs(to, from, tolen);
    } else {
        /* Use wcstombs_l for nondefault locales */
        result = wcstombs_l(to, from, tolen, (*locale).info.lt);
    }

    result
}

/*
 * char2wchar --- convert multibyte characters to wide characters
 *
 * This has almost the API of mbstowcs_l(), except that *from need not be
 * null-terminated; instead, the number of input bytes is specified as
 * fromlen.  Also, we ereport() rather than returning -1 for invalid
 * input encoding.  tolen is the maximum number of wchar_t's to store at *to.
 * The output will be zero-terminated iff there is room.
 */
pub unsafe fn char2wchar(
    to: *mut wchar_t,
    tolen: usize,
    from: *const c_char,
    fromlen: usize,
    locale: pg_locale_t,
) -> usize {
    let result: usize;

    if tolen == 0 {
        return 0;
    }

    {
        /* mbstowcs requires ending '\0' */
        let str: *mut c_char = pnstrdup(from, fromlen);

        if locale == (0 as pg_locale_t) {
            /* Use mbstowcs directly for the default locale */
            result = mbstowcs(to, str, tolen);
        } else {
            /* Use mbstowcs_l for nondefault locales */
            result = mbstowcs_l(to, str, tolen, (*locale).info.lt);
        }

        pfree(str as *mut c_void);
    }

    if result == usize::MAX {
        /*
         * Invalid multibyte character encountered.  We try to give a useful
         * error message by letting pg_verifymbstr check the string.  But it's
         * possible that the string is OK to us, and not OK to mbstowcs ---
         * this suggests that the LC_CTYPE locale is different from the
         * database encoding.  Give a generic error message if pg_verifymbstr
         * can't find anything wrong.
         */
        pg_verifymbstr(from, fromlen as c_int, false); /* might not return */
        /* but if it does ... */
        ereport!(ERROR, "invalid multibyte character for locale");
        unreachable!();
    }

    result
}

// ---------------------------------------------------------------------------
// Local stubs for not-yet-ported dependencies.
// ---------------------------------------------------------------------------

extern "C" {
    fn __errno_location() -> *mut c_int;
}

// utils/pg_locale.h
#[repr(C)]
pub struct collate_methods {
    pub strncoll: Option<
        unsafe extern "C" fn(*const c_char, ssize_t, *const c_char, ssize_t, pg_locale_t) -> c_int,
    >,
    pub strnxfrm: Option<
        unsafe extern "C" fn(*mut c_char, usize, *const c_char, ssize_t, pg_locale_t) -> usize,
    >,
    pub strnxfrm_prefix: Option<
        unsafe extern "C" fn(*mut c_char, usize, *const c_char, ssize_t, pg_locale_t) -> usize,
    >,
    pub strxfrm_is_safe: bool,
}
unsafe impl Sync for collate_methods {}

#[repr(C)]
pub union locale_info {
    pub lt: locale_t,
    pub builtin: *mut c_void,
    pub icu: *mut c_void,
}

#[repr(C)]
pub struct pg_locale_struct {
    pub provider: c_char,
    pub deterministic: bool,
    pub collate_is_c: bool,
    pub ctype_is_c: bool,
    pub is_default: bool,
    pub collate: *const collate_methods,
    pub ctype: *const c_void,
    pub info: locale_info,
}
pub type pg_locale_t = *mut pg_locale_struct;

const COLLPROVIDER_LIBC: c_char = b'c' as c_char;

#[allow(non_upper_case_globals)]
const DEFAULT_COLLATION_OID: Oid = 100;

// catalog attribute numbers
const Anum_pg_database_datcollate: c_int = 16;
const Anum_pg_database_datctype: c_int = 17;
const Anum_pg_collation_collcollate: c_int = 8;
const Anum_pg_collation_collctype: c_int = 9;

// syscache ids
const DATABASEOID: c_int = 0;
const COLLOID: c_int = 0;

#[allow(non_camel_case_types)]
type HeapTuple = *mut c_void;
#[allow(non_camel_case_types)]
type MemoryContext = *mut c_void;

unsafe fn pg_database_encoding_max_length() -> c_int {
    unimplemented!() // TODO: mb/pg_wchar.c
}
unsafe fn SearchSysCache1(_cacheid: c_int, _key: Datum) -> HeapTuple {
    unimplemented!() // TODO: utils/cache/syscache.c
}
unsafe fn SysCacheGetAttrNotNull(_cacheid: c_int, _tup: HeapTuple, _attnum: c_int) -> Datum {
    unimplemented!() // TODO: utils/cache/syscache.c
}
unsafe fn ReleaseSysCache(_tup: HeapTuple) {
    unimplemented!() // TODO: utils/cache/catcache.c
}
unsafe fn HeapTupleIsValid(tup: HeapTuple) -> bool {
    !tup.is_null()
}
unsafe fn TextDatumGetCString(_d: Datum) -> *mut c_char {
    unimplemented!() // TODO: utils/adt/varlena.c
}
unsafe fn MemoryContextAllocZero(_context: MemoryContext, _size: usize) -> *mut c_void {
    unimplemented!() // TODO: utils/mmgr/mcxt.c
}
unsafe fn pg_tolower(c: u8) -> u8 {
    let _ = c;
    unimplemented!() // TODO: src/port/pgstrcasecmp.c
}
unsafe fn pg_toupper(c: u8) -> u8 {
    let _ = c;
    unimplemented!() // TODO: src/port/pgstrcasecmp.c
}
unsafe fn pg_strcasecmp(_a: *const c_char, _b: *const c_char) -> c_int {
    unimplemented!() // TODO: src/port/pgstrcasecmp.c
}
unsafe fn pg_strncasecmp(_a: *const c_char, _b: *const c_char, _n: usize) -> c_int {
    unimplemented!() // TODO: src/port/pgstrcasecmp.c
}
unsafe fn pstrdup(_s: *const c_char) -> *mut c_char {
    unimplemented!() // TODO: utils/mmgr/mcxt.c
}
unsafe fn pnstrdup(_s: *const c_char, _len: usize) -> *mut c_char {
    unimplemented!() // TODO: utils/mmgr/mcxt.c
}
unsafe fn pg_verifymbstr(_mbstr: *const c_char, _len: c_int, _noError: bool) -> bool {
    unimplemented!() // TODO: mb/mbutils.c
}
