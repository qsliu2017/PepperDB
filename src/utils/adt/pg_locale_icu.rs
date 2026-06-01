//! PostgreSQL locale utilities for ICU
//!
//! Portions Copyright (c) 2002-2025, PostgreSQL Global Development Group
//!
//! src/backend/utils/adt/pg_locale_icu.c
//!
//! The C file body is entirely gated behind `#ifdef USE_ICU`. PepperDB has no ICU
//! linkage, so the ICU library functions (ucol_*, ucnv_*, uloc_*, u_*) are declared
//! in `extern "C"` blocks below with faithful signatures (UChar = u16,
//! UErrorCode = i32, UCollator/UConverter opaque). The Rust translation mirrors the
//! USE_ICU path directly rather than gating it out, so the code is actually ported.

use crate::prelude::*;

use crate::mb::pg_wchar::{
    get_encoding_name_for_icu, pg_encoding_to_char, GetDatabaseEncoding, PG_UTF8,
};
use crate::utils::adt::formatting::asc_tolower;
use crate::utils::builtins::TextDatumGetCString;
use crate::utils::init::globals::MyDatabaseId;
use crate::utils::mmgr::mcxt::MemoryContextStrdup;
use crate::utils::palloc::MCXT_ALLOC_NO_OOM;

use std::ffi::{c_char, c_int, c_void};

#[allow(non_camel_case_types)]
type ssize_t = isize;

// ===========================================================================
// ICU type aliases and library declarations.
//
// The whole module is `#ifdef USE_ICU` in C. Since PepperDB does not link ICU,
// we declare the ICU C symbols here (faithful signatures) so the port type-checks.
// ===========================================================================

#[allow(non_camel_case_types)]
type UChar = u16;
#[allow(non_camel_case_types)]
type UErrorCode = i32;
#[allow(non_camel_case_types)]
type UColAttribute = i32;
#[allow(non_camel_case_types)]
type UColAttributeValue = i32;
#[allow(non_camel_case_types)]
type UVersionInfo = [u8; 4]; // U_MAX_VERSION_LENGTH == 4

/// Opaque ICU collator object (UCollator).
#[repr(C)]
pub struct UCollator {
    _private: [u8; 0],
}

/// Opaque ICU converter object (UConverter).
#[repr(C)]
pub struct UConverter {
    _private: [u8; 0],
}

/// Opaque ICU character iterator (UCharIterator). Treated as an opaque blob; ICU
/// only requires that we pass its address to the relevant routines.
#[repr(C)]
pub struct UCharIterator {
    _opaque: [u8; 256],
}

// UErrorCode values used here.
const U_ZERO_ERROR: UErrorCode = 0;
const U_STRING_NOT_TERMINATED_WARNING: UErrorCode = -124;
const U_BUFFER_OVERFLOW_ERROR: UErrorCode = 15;
const U_ILLEGAL_ARGUMENT_ERROR: UErrorCode = 1;

/// U_FAILURE(x): nonzero error codes (> U_ZERO_ERROR) are failures.
#[inline]
fn U_FAILURE(status: UErrorCode) -> bool {
    status > U_ZERO_ERROR
}

/// U_SUCCESS(x): codes <= U_ZERO_ERROR are successes.
#[inline]
fn U_SUCCESS(status: UErrorCode) -> bool {
    status <= U_ZERO_ERROR
}

// uloc capacities (unicode/uloc.h).
const ULOC_LANG_CAPACITY: c_int = 12;

// ucol defaults (unicode/ucol.h).
const UCOL_DEFAULT: i32 = -1;
const UCOL_DEFAULT_STRENGTH: i32 = UCOL_DEFAULT;

// UColAttribute values (unicode/ucol.h).
const UCOL_FRENCH_COLLATION: UColAttribute = 0;
const UCOL_ALTERNATE_HANDLING: UColAttribute = 1;
const UCOL_CASE_FIRST: UColAttribute = 2;
const UCOL_CASE_LEVEL: UColAttribute = 3;
const UCOL_NORMALIZATION_MODE: UColAttribute = 4;
const UCOL_STRENGTH: UColAttribute = 5;
const UCOL_NUMERIC_COLLATION: UColAttribute = 7;

// UColAttributeValue values (unicode/ucol.h).
const UCOL_DEFAULT_VALUE: UColAttributeValue = -1;
const UCOL_PRIMARY: UColAttributeValue = 0;
const UCOL_SECONDARY: UColAttributeValue = 1;
const UCOL_TERTIARY: UColAttributeValue = 2;
const UCOL_QUATERNARY: UColAttributeValue = 3;
const UCOL_IDENTICAL: UColAttributeValue = 15;
const UCOL_OFF: UColAttributeValue = 16;
const UCOL_ON: UColAttributeValue = 17;
const UCOL_SHIFTED: UColAttributeValue = 20;
const UCOL_NON_IGNORABLE: UColAttributeValue = 21;
const UCOL_LOWER_FIRST: UColAttributeValue = 24;
const UCOL_UPPER_FIRST: UColAttributeValue = 25;

// u_strFoldCase options (unicode/uchar.h / ustring.h).
const U_FOLD_CASE_DEFAULT: u32 = 0;
const U_FOLD_CASE_EXCLUDE_SPECIAL_I: u32 = 1;

// u_versionToString buffer length (unicode/uvernum.h).
const U_MAX_VERSION_STRING_LENGTH: usize = 20;

extern "C" {
    // unicode/ucol.h
    fn ucol_open(loc: *const c_char, status: *mut UErrorCode) -> *mut UCollator;
    fn ucol_openRules(
        rules: *const UChar,
        rulesLength: i32,
        normalizationMode: i32,
        strength: i32,
        parseError: *mut c_void,
        status: *mut UErrorCode,
    ) -> *mut UCollator;
    fn ucol_close(coll: *mut UCollator);
    fn ucol_getRules(coll: *const UCollator, length: *mut i32) -> *const UChar;
    fn ucol_strcoll(
        coll: *const UCollator,
        source: *const UChar,
        sourceLength: i32,
        target: *const UChar,
        targetLength: i32,
    ) -> c_int;
    fn ucol_strcollUTF8(
        coll: *const UCollator,
        source: *const c_char,
        sourceLength: i32,
        target: *const c_char,
        targetLength: i32,
        status: *mut UErrorCode,
    ) -> c_int;
    fn ucol_getSortKey(
        coll: *const UCollator,
        source: *const UChar,
        sourceLength: i32,
        result: *mut u8,
        resultLength: i32,
    ) -> i32;
    fn ucol_nextSortKeyPart(
        coll: *const UCollator,
        iter: *mut UCharIterator,
        state: *mut u32,
        dest: *mut u8,
        count: i32,
        status: *mut UErrorCode,
    ) -> i32;
    fn ucol_getVersion(coll: *const UCollator, info: *mut u8);
    fn ucol_setAttribute(
        coll: *mut UCollator,
        attr: UColAttribute,
        value: UColAttributeValue,
        status: *mut UErrorCode,
    );

    // unicode/ucnv.h
    fn ucnv_open(converterName: *const c_char, status: *mut UErrorCode) -> *mut UConverter;
    fn ucnv_toUChars(
        cnv: *mut UConverter,
        dest: *mut UChar,
        destCapacity: i32,
        src: *const c_char,
        srcLength: i32,
        pErrorCode: *mut UErrorCode,
    ) -> i32;
    fn ucnv_fromUChars(
        cnv: *mut UConverter,
        dest: *mut c_char,
        destCapacity: i32,
        src: *const UChar,
        srcLength: i32,
        pErrorCode: *mut UErrorCode,
    ) -> i32;

    // unicode/uloc.h
    fn uloc_getLanguage(
        localeID: *const c_char,
        language: *mut c_char,
        languageCapacity: i32,
        err: *mut UErrorCode,
    ) -> i32;
    fn uloc_canonicalize(
        localeID: *const c_char,
        name: *mut c_char,
        nameCapacity: i32,
        err: *mut UErrorCode,
    ) -> i32;

    // unicode/ustring.h and unicode/uchar.h
    fn u_strlen(s: *const UChar) -> i32;
    fn u_strcpy(dst: *mut UChar, src: *const UChar) -> *mut UChar;
    fn u_strcat(dst: *mut UChar, src: *const UChar) -> *mut UChar;
    fn u_errorName(code: UErrorCode) -> *const c_char;
    fn u_versionToString(versionArray: *const u8, versionString: *mut c_char);
    fn u_strToLower(
        dest: *mut UChar,
        destCapacity: i32,
        src: *const UChar,
        srcLength: i32,
        locale: *const c_char,
        pErrorCode: *mut UErrorCode,
    ) -> i32;
    fn u_strToUpper(
        dest: *mut UChar,
        destCapacity: i32,
        src: *const UChar,
        srcLength: i32,
        locale: *const c_char,
        pErrorCode: *mut UErrorCode,
    ) -> i32;
    fn u_strToTitle(
        dest: *mut UChar,
        destCapacity: i32,
        src: *const UChar,
        srcLength: i32,
        titleIter: *mut c_void,
        locale: *const c_char,
        pErrorCode: *mut UErrorCode,
    ) -> i32;
    fn u_strFoldCase(
        dest: *mut UChar,
        destCapacity: i32,
        src: *const UChar,
        srcLength: i32,
        options: u32,
        pErrorCode: *mut UErrorCode,
    ) -> i32;

    // unicode/uiter.h
    fn uiter_setString(iter: *mut UCharIterator, s: *const UChar, length: i32);
    fn uiter_setUTF8(iter: *mut UCharIterator, s: *const c_char, length: i32);

    // libc
    fn strcmp(a: *const c_char, b: *const c_char) -> c_int;
    fn strlen(s: *const c_char) -> usize;
    fn strcpy(dst: *mut c_char, src: *const c_char) -> *mut c_char;
    fn strcat(dst: *mut c_char, src: *const c_char) -> *mut c_char;
    fn strchr(s: *const c_char, c: c_int) -> *mut c_char;
    fn strsep(stringp: *mut *mut c_char, delim: *const c_char) -> *mut c_char;
}

/*
 * ICU's signature for case-conversion functions u_strToLower / u_strToUpper /
 * u_strToTitle_default_BI / u_strFoldCase_default. ICU_Convert_Func is a
 * function pointer type. The titlecase/foldcase wrappers below adapt the ICU
 * APIs (whose argument lists differ) to this common signature.
 */
#[allow(non_camel_case_types)]
type ICU_Convert_Func = unsafe extern "C" fn(
    dest: *mut UChar,
    destCapacity: i32,
    src: *const UChar,
    srcLength: i32,
    locale: *const c_char,
    pErrorCode: *mut UErrorCode,
) -> i32;

/*
 * Size of stack buffer to use for string transformations, used to avoid heap
 * allocations in typical cases. This should be large enough that most strings
 * will fit, but small enough that we feel comfortable putting it on the
 * stack.
 */
const TEXTBUFLEN: usize = 1024;

/*
 * ucol_strcollUTF8() was introduced in ICU 50, but it is buggy before ICU 53.
 * Assume a modern ICU (>= 53) is available, so the UTF8 fast path exists.
 */
const HAVE_UCOL_STRCOLLUTF8: bool = true;

/*
 * Converter object for converting between ICU's UChar strings and C strings
 * in database encoding.  Since the database encoding doesn't change, we only
 * need one of these per session.
 */
static mut icu_converter: *mut UConverter = null_mut();

static collate_methods_icu: collate_methods = collate_methods {
    strncoll: Some(strncoll_icu),
    strnxfrm: Some(strnxfrm_icu),
    strnxfrm_prefix: Some(strnxfrm_prefix_icu),
    strxfrm_is_safe: true,
};

static collate_methods_icu_utf8: collate_methods = collate_methods {
    // HAVE_UCOL_STRCOLLUTF8 is assumed true (modern ICU).
    strncoll: Some(strncoll_icu_utf8),
    strnxfrm: Some(strnxfrm_icu),
    strnxfrm_prefix: Some(strnxfrm_prefix_icu_utf8),
    strxfrm_is_safe: true,
};

pub unsafe fn create_pg_locale_icu(collid: Oid, context: MemoryContext) -> pg_locale_t {
    let deterministic: bool;
    let iculocstr: *const c_char;
    let mut icurules: *const c_char = null();
    let collator: *mut UCollator;
    let result: pg_locale_t;

    if collid == DEFAULT_COLLATION_OID {
        let tp: HeapTuple;
        let mut datum: Datum;
        let mut isnull: bool = false;

        tp = SearchSysCache1(DATABASEOID, ObjectIdGetDatum(MyDatabaseId));
        if !HeapTupleIsValid(tp) {
            elog!(ERROR, "cache lookup failed for database {}", MyDatabaseId);
        }

        /* default database collation is always deterministic */
        deterministic = true;
        datum = SysCacheGetAttrNotNull(DATABASEOID, tp, Anum_pg_database_datlocale);
        iculocstr = TextDatumGetCString(datum);
        datum = SysCacheGetAttr(DATABASEOID, tp, Anum_pg_database_daticurules, &mut isnull);
        if !isnull {
            icurules = TextDatumGetCString(datum);
        }

        ReleaseSysCache(tp);
    } else {
        let collform: Form_pg_collation;
        let tp: HeapTuple;
        let mut datum: Datum;
        let mut isnull: bool = false;

        tp = SearchSysCache1(COLLOID, ObjectIdGetDatum(collid));
        if !HeapTupleIsValid(tp) {
            elog!(ERROR, "cache lookup failed for collation {}", collid);
        }
        collform = GETSTRUCT(tp) as Form_pg_collation;
        deterministic = (*collform).collisdeterministic;
        datum = SysCacheGetAttrNotNull(COLLOID, tp, Anum_pg_collation_colllocale);
        iculocstr = TextDatumGetCString(datum);
        datum = SysCacheGetAttr(COLLOID, tp, Anum_pg_collation_collicurules, &mut isnull);
        if !isnull {
            icurules = TextDatumGetCString(datum);
        }

        ReleaseSysCache(tp);
    }

    collator = make_icu_collator(iculocstr, icurules);

    result = MemoryContextAllocZero(context, std::mem::size_of::<pg_locale_struct>()) as pg_locale_t;
    (*result).info.icu.locale = MemoryContextStrdup(context as crate::utils::mmgr::memnodes::MemoryContext, iculocstr);
    (*result).info.icu.ucol = collator;
    (*result).provider = COLLPROVIDER_ICU;
    (*result).deterministic = deterministic;
    (*result).collate_is_c = false;
    (*result).ctype_is_c = false;
    if GetDatabaseEncoding() == PG_UTF8 as c_int {
        (*result).collate = &collate_methods_icu_utf8 as *const collate_methods;
    } else {
        (*result).collate = &collate_methods_icu as *const collate_methods;
    }

    result
}

/*
 * Wrapper around ucol_open() to handle API differences for older ICU
 * versions.
 *
 * Ensure that no path leaks a UCollator.
 */
pub unsafe fn pg_ucol_open(loc_str: *const c_char) -> *mut UCollator {
    let collator: *mut UCollator;
    let mut status: UErrorCode;
    let orig_str: *const c_char = loc_str;
    let fixed_str: *mut c_char = null_mut();

    /*
     * Must never open default collator, because it depends on the environment
     * and may change at any time. Should not happen, but check here to catch
     * bugs that might be hard to catch otherwise.
     *
     * NB: the default collator is not the same as the collator for the root
     * locale. The root locale may be specified as the empty string, "und", or
     * "root". The default collator is opened by passing NULL to ucol_open().
     */
    if loc_str.is_null() {
        elog!(ERROR, "opening default collator is not supported");
    }

    /*
     * In ICU versions 54 and earlier, "und" is not a recognized spelling of
     * the root locale. The "und" -> "root" fixup and the
     * icu_set_collation_attributes() emulation are only required for
     * U_ICU_VERSION_MAJOR_NUM < 55 / < 54; we assume a modern ICU, so those
     * compile-time branches are not taken.
     */

    status = U_ZERO_ERROR;
    collator = ucol_open(loc_str, &mut status);
    if U_FAILURE(status) {
        /* use original string for error report */
        ereport!(
            ERROR,
            errmsg!(
                "could not open collator for locale \"{}\": {}",
                cstr(orig_str),
                cstr(u_errorName(status))
            )
        );
    }

    if !fixed_str.is_null() {
        pfree(fixed_str as *mut c_void);
    }

    collator
}

/*
 * Create a UCollator with the given locale string and rules.
 *
 * Ensure that no path leaks a UCollator.
 */
unsafe fn make_icu_collator(iculocstr: *const c_char, icurules: *const c_char) -> *mut UCollator {
    if icurules.is_null() {
        /* simple case without rules */
        pg_ucol_open(iculocstr)
    } else {
        let collator_std_rules: *mut UCollator;
        let collator_all_rules: *mut UCollator;
        let std_rules: *const UChar;
        let mut my_rules: *mut UChar = null_mut();
        let all_rules: *mut UChar;
        let mut length: i32 = 0;
        let total: i32;
        let mut status: UErrorCode;

        /*
         * If rules are specified, we extract the rules of the standard
         * collation, add our own rules, and make a new collator with the
         * combined rules.
         */
        icu_to_uchar(&mut my_rules, icurules, strlen(icurules));

        collator_std_rules = pg_ucol_open(iculocstr);

        std_rules = ucol_getRules(collator_std_rules, &mut length);

        total = u_strlen(std_rules) + u_strlen(my_rules) + 1;

        /* avoid leaking collator on OOM */
        all_rules = palloc_extended(
            std::mem::size_of::<UChar>() * total as usize,
            MCXT_ALLOC_NO_OOM,
        ) as *mut UChar;
        if all_rules.is_null() {
            ucol_close(collator_std_rules);
            ereport!(ERROR, errmsg!("out of memory"));
        }

        u_strcpy(all_rules, std_rules);
        u_strcat(all_rules, my_rules);

        ucol_close(collator_std_rules);

        status = U_ZERO_ERROR;
        collator_all_rules = ucol_openRules(
            all_rules,
            u_strlen(all_rules),
            UCOL_DEFAULT,
            UCOL_DEFAULT_STRENGTH,
            null_mut(),
            &mut status,
        );
        if U_FAILURE(status) {
            ereport!(
                ERROR,
                errmsg!(
                    "could not open collator for locale \"{}\" with rules \"{}\": {}",
                    cstr(iculocstr),
                    cstr(icurules),
                    cstr(u_errorName(status))
                )
            );
        }

        pfree(my_rules as *mut c_void);
        pfree(all_rules as *mut c_void);
        collator_all_rules
    }
}

pub unsafe fn strlower_icu(
    dest: *mut c_char,
    destsize: usize,
    src: *const c_char,
    srclen: ssize_t,
    locale: pg_locale_t,
) -> usize {
    let len_uchar: i32;
    let len_conv: i32;
    let mut buff_uchar: *mut UChar = null_mut();
    let mut buff_conv: *mut UChar = null_mut();
    let result_len: usize;

    len_uchar = icu_to_uchar(&mut buff_uchar, src, srclen as usize);
    len_conv = icu_convert_case(u_strToLower, locale, &mut buff_conv, buff_uchar, len_uchar);
    result_len = icu_from_uchar(dest, destsize, buff_conv, len_conv);
    pfree(buff_uchar as *mut c_void);
    pfree(buff_conv as *mut c_void);

    result_len
}

pub unsafe fn strtitle_icu(
    dest: *mut c_char,
    destsize: usize,
    src: *const c_char,
    srclen: ssize_t,
    locale: pg_locale_t,
) -> usize {
    let len_uchar: i32;
    let len_conv: i32;
    let mut buff_uchar: *mut UChar = null_mut();
    let mut buff_conv: *mut UChar = null_mut();
    let result_len: usize;

    len_uchar = icu_to_uchar(&mut buff_uchar, src, srclen as usize);
    len_conv = icu_convert_case(
        u_strToTitle_default_BI,
        locale,
        &mut buff_conv,
        buff_uchar,
        len_uchar,
    );
    result_len = icu_from_uchar(dest, destsize, buff_conv, len_conv);
    pfree(buff_uchar as *mut c_void);
    pfree(buff_conv as *mut c_void);

    result_len
}

pub unsafe fn strupper_icu(
    dest: *mut c_char,
    destsize: usize,
    src: *const c_char,
    srclen: ssize_t,
    locale: pg_locale_t,
) -> usize {
    let len_uchar: i32;
    let len_conv: i32;
    let mut buff_uchar: *mut UChar = null_mut();
    let mut buff_conv: *mut UChar = null_mut();
    let result_len: usize;

    len_uchar = icu_to_uchar(&mut buff_uchar, src, srclen as usize);
    len_conv = icu_convert_case(u_strToUpper, locale, &mut buff_conv, buff_uchar, len_uchar);
    result_len = icu_from_uchar(dest, destsize, buff_conv, len_conv);
    pfree(buff_uchar as *mut c_void);
    pfree(buff_conv as *mut c_void);

    result_len
}

pub unsafe fn strfold_icu(
    dest: *mut c_char,
    destsize: usize,
    src: *const c_char,
    srclen: ssize_t,
    locale: pg_locale_t,
) -> usize {
    let len_uchar: i32;
    let len_conv: i32;
    let mut buff_uchar: *mut UChar = null_mut();
    let mut buff_conv: *mut UChar = null_mut();
    let result_len: usize;

    len_uchar = icu_to_uchar(&mut buff_uchar, src, srclen as usize);
    len_conv = icu_convert_case(
        u_strFoldCase_default,
        locale,
        &mut buff_conv,
        buff_uchar,
        len_uchar,
    );
    result_len = icu_from_uchar(dest, destsize, buff_conv, len_conv);
    pfree(buff_uchar as *mut c_void);
    pfree(buff_conv as *mut c_void);

    result_len
}

/*
 * strncoll_icu_utf8
 *
 * Call ucol_strcollUTF8() or ucol_strcoll() as appropriate for the given
 * database encoding. An argument length of -1 means the string is
 * NUL-terminated.
 */
pub unsafe extern "C" fn strncoll_icu_utf8(
    arg1: *const c_char,
    len1: ssize_t,
    arg2: *const c_char,
    len2: ssize_t,
    locale: pg_locale_t,
) -> c_int {
    let result: c_int;
    let mut status: UErrorCode;

    Assert!((*locale).provider == COLLPROVIDER_ICU);

    Assert!(GetDatabaseEncoding() == PG_UTF8 as c_int);

    status = U_ZERO_ERROR;
    result = ucol_strcollUTF8(
        (*locale).info.icu.ucol,
        arg1,
        len1 as i32,
        arg2,
        len2 as i32,
        &mut status,
    );
    if U_FAILURE(status) {
        ereport!(
            ERROR,
            errmsg!("collation failed: {}", cstr(u_errorName(status)))
        );
    }

    result
}

/* 'srclen' of -1 means the strings are NUL-terminated */
pub unsafe extern "C" fn strnxfrm_icu(
    dest: *mut c_char,
    destsize: usize,
    src: *const c_char,
    srclen: ssize_t,
    locale: pg_locale_t,
) -> usize {
    let mut sbuf: [c_char; TEXTBUFLEN] = [0; TEXTBUFLEN];
    let mut buf: *mut c_char = sbuf.as_mut_ptr();
    let uchar: *mut UChar;
    let mut ulen: i32;
    let uchar_bsize: usize;
    let mut result_bsize: Size;

    Assert!((*locale).provider == COLLPROVIDER_ICU);

    init_icu_converter();

    ulen = uchar_length(icu_converter, src, srclen as i32) as i32;

    uchar_bsize = (ulen as usize + 1) * std::mem::size_of::<UChar>();

    if uchar_bsize > TEXTBUFLEN {
        buf = palloc(uchar_bsize) as *mut c_char;
    }

    uchar = buf as *mut UChar;

    ulen = uchar_convert(icu_converter, uchar, ulen + 1, src, srclen as i32);

    result_bsize = ucol_getSortKey(
        (*locale).info.icu.ucol,
        uchar,
        ulen,
        dest as *mut u8,
        destsize as i32,
    ) as Size;

    /*
     * ucol_getSortKey() counts the nul-terminator in the result length, but
     * this function should not.
     */
    Assert!(result_bsize > 0);
    result_bsize -= 1;

    if buf != sbuf.as_mut_ptr() {
        pfree(buf as *mut c_void);
    }

    /* if dest is defined, it should be nul-terminated */
    Assert!(result_bsize as usize >= destsize || *dest.add(result_bsize as usize) == b'\0' as c_char);

    result_bsize as usize
}

/* 'srclen' of -1 means the strings are NUL-terminated */
pub unsafe extern "C" fn strnxfrm_prefix_icu_utf8(
    dest: *mut c_char,
    destsize: usize,
    src: *const c_char,
    srclen: ssize_t,
    locale: pg_locale_t,
) -> usize {
    let result: usize;
    let mut iter: UCharIterator = std::mem::zeroed();
    let mut state: [u32; 2] = [0; 2];
    let mut status: UErrorCode;

    Assert!((*locale).provider == COLLPROVIDER_ICU);

    Assert!(GetDatabaseEncoding() == PG_UTF8 as c_int);

    uiter_setUTF8(&mut iter, src, srclen as i32);
    state[0] = 0;
    state[1] = 0; /* won't need that again */
    status = U_ZERO_ERROR;
    result = ucol_nextSortKeyPart(
        (*locale).info.icu.ucol,
        &mut iter,
        state.as_mut_ptr(),
        dest as *mut u8,
        destsize as i32,
        &mut status,
    ) as usize;
    if U_FAILURE(status) {
        ereport!(
            ERROR,
            errmsg!("sort key generation failed: {}", cstr(u_errorName(status)))
        );
    }

    result
}

pub unsafe fn get_collation_actual_version_icu(collcollate: *const c_char) -> *mut c_char {
    let collator: *mut UCollator;
    let mut versioninfo: UVersionInfo = [0; 4];
    let mut buf: [c_char; U_MAX_VERSION_STRING_LENGTH] = [0; U_MAX_VERSION_STRING_LENGTH];

    collator = pg_ucol_open(collcollate);

    ucol_getVersion(collator, versioninfo.as_mut_ptr());
    ucol_close(collator);

    u_versionToString(versioninfo.as_ptr(), buf.as_mut_ptr());
    pstrdup(buf.as_ptr())
}

/*
 * Convert a string in the database encoding into a string of UChars.
 *
 * The source string at buff is of length nbytes
 * (it needn't be nul-terminated)
 *
 * *buff_uchar receives a pointer to the palloc'd result string, and
 * the function's result is the number of UChars generated.
 *
 * The result string is nul-terminated, though most callers rely on the
 * result length instead.
 */
unsafe fn icu_to_uchar(buff_uchar: *mut *mut UChar, buff: *const c_char, nbytes: usize) -> i32 {
    let mut len_uchar: i32;

    init_icu_converter();

    len_uchar = uchar_length(icu_converter, buff, nbytes as i32) as i32;

    *buff_uchar = palloc((len_uchar as usize + 1) * std::mem::size_of::<UChar>()) as *mut UChar;
    len_uchar = uchar_convert(icu_converter, *buff_uchar, len_uchar + 1, buff, nbytes as i32);

    len_uchar
}

/*
 * Convert a string of UChars into the database encoding.
 *
 * The source string at buff_uchar is of length len_uchar
 * (it needn't be nul-terminated)
 *
 * *result receives a pointer to the palloc'd result string, and the
 * function's result is the number of bytes generated (not counting nul).
 *
 * The result string is nul-terminated.
 */
unsafe fn icu_from_uchar(
    dest: *mut c_char,
    destsize: usize,
    buff_uchar: *const UChar,
    len_uchar: i32,
) -> usize {
    let mut status: UErrorCode;
    let mut len_result: i32;

    init_icu_converter();

    status = U_ZERO_ERROR;
    len_result = ucnv_fromUChars(icu_converter, null_mut(), 0, buff_uchar, len_uchar, &mut status);
    if U_FAILURE(status) && status != U_BUFFER_OVERFLOW_ERROR {
        ereport!(
            ERROR,
            errmsg!(
                "{} failed: {}",
                "ucnv_fromUChars",
                cstr(u_errorName(status))
            )
        );
    }

    if (len_result + 1) as usize > destsize {
        return len_result as usize;
    }

    status = U_ZERO_ERROR;
    len_result = ucnv_fromUChars(
        icu_converter,
        dest,
        len_result + 1,
        buff_uchar,
        len_uchar,
        &mut status,
    );
    if U_FAILURE(status) || status == U_STRING_NOT_TERMINATED_WARNING {
        ereport!(
            ERROR,
            errmsg!(
                "{} failed: {}",
                "ucnv_fromUChars",
                cstr(u_errorName(status))
            )
        );
    }

    len_result as usize
}

unsafe fn icu_convert_case(
    func: ICU_Convert_Func,
    mylocale: pg_locale_t,
    buff_dest: *mut *mut UChar,
    buff_source: *mut UChar,
    len_source: i32,
) -> i32 {
    let mut status: UErrorCode;
    let mut len_dest: i32;

    len_dest = len_source; /* try first with same length */
    *buff_dest = palloc(len_dest as usize * std::mem::size_of::<UChar>()) as *mut UChar;
    status = U_ZERO_ERROR;
    len_dest = func(
        *buff_dest,
        len_dest,
        buff_source,
        len_source,
        (*mylocale).info.icu.locale,
        &mut status,
    );
    if status == U_BUFFER_OVERFLOW_ERROR {
        /* try again with adjusted length */
        pfree(*buff_dest as *mut c_void);
        *buff_dest = palloc(len_dest as usize * std::mem::size_of::<UChar>()) as *mut UChar;
        status = U_ZERO_ERROR;
        len_dest = func(
            *buff_dest,
            len_dest,
            buff_source,
            len_source,
            (*mylocale).info.icu.locale,
            &mut status,
        );
    }
    if U_FAILURE(status) {
        ereport!(
            ERROR,
            errmsg!("case conversion failed: {}", cstr(u_errorName(status)))
        );
    }
    len_dest
}

unsafe extern "C" fn u_strToTitle_default_BI(
    dest: *mut UChar,
    destCapacity: i32,
    src: *const UChar,
    srcLength: i32,
    locale: *const c_char,
    pErrorCode: *mut UErrorCode,
) -> i32 {
    u_strToTitle(
        dest,
        destCapacity,
        src,
        srcLength,
        null_mut(),
        locale,
        pErrorCode,
    )
}

unsafe extern "C" fn u_strFoldCase_default(
    dest: *mut UChar,
    destCapacity: i32,
    src: *const UChar,
    srcLength: i32,
    locale: *const c_char,
    pErrorCode: *mut UErrorCode,
) -> i32 {
    let mut options: u32 = U_FOLD_CASE_DEFAULT;
    let mut lang: [c_char; 3] = [0; 3];
    let mut status: UErrorCode;

    /*
     * Unlike the ICU APIs for lowercasing, titlecasing, and uppercasing, case
     * folding does not accept a locale. Instead it just supports a single
     * option relevant to Turkic languages 'az' and 'tr'; check for those
     * languages to enable the option.
     */
    status = U_ZERO_ERROR;
    uloc_getLanguage(locale, lang.as_mut_ptr(), 3, &mut status);
    if U_SUCCESS(status) {
        /*
         * The option name is confusing, but it causes u_strFoldCase to use
         * the 'T' mappings, which are ignored for U_FOLD_CASE_DEFAULT.
         */
        if strcmp(lang.as_ptr(), c"tr".as_ptr()) == 0 || strcmp(lang.as_ptr(), c"az".as_ptr()) == 0 {
            options = U_FOLD_CASE_EXCLUDE_SPECIAL_I;
        }
    }

    u_strFoldCase(dest, destCapacity, src, srcLength, options, pErrorCode)
}

/*
 * strncoll_icu
 *
 * Convert the arguments from the database encoding to UChar strings, then
 * call ucol_strcoll(). An argument length of -1 means that the string is
 * NUL-terminated.
 *
 * When the database encoding is UTF-8, and ICU supports ucol_strcollUTF8(),
 * caller should call that instead.
 */
pub unsafe extern "C" fn strncoll_icu(
    arg1: *const c_char,
    len1: ssize_t,
    arg2: *const c_char,
    len2: ssize_t,
    locale: pg_locale_t,
) -> c_int {
    let mut sbuf: [c_char; TEXTBUFLEN] = [0; TEXTBUFLEN];
    let mut buf: *mut c_char = sbuf.as_mut_ptr();
    let mut ulen1: i32;
    let mut ulen2: i32;
    let bufsize1: usize;
    let bufsize2: usize;
    let uchar1: *mut UChar;
    let uchar2: *mut UChar;
    let result: c_int;

    Assert!((*locale).provider == COLLPROVIDER_ICU);

    /* if encoding is UTF8, use more efficient strncoll_icu_utf8 */
    if HAVE_UCOL_STRCOLLUTF8 {
        Assert!(GetDatabaseEncoding() != PG_UTF8 as c_int);
    }

    init_icu_converter();

    ulen1 = uchar_length(icu_converter, arg1, len1 as i32) as i32;
    ulen2 = uchar_length(icu_converter, arg2, len2 as i32) as i32;

    bufsize1 = (ulen1 as usize + 1) * std::mem::size_of::<UChar>();
    bufsize2 = (ulen2 as usize + 1) * std::mem::size_of::<UChar>();

    if bufsize1 + bufsize2 > TEXTBUFLEN {
        buf = palloc(bufsize1 + bufsize2) as *mut c_char;
    }

    uchar1 = buf as *mut UChar;
    uchar2 = buf.add(bufsize1) as *mut UChar;

    ulen1 = uchar_convert(icu_converter, uchar1, ulen1 + 1, arg1, len1 as i32);
    ulen2 = uchar_convert(icu_converter, uchar2, ulen2 + 1, arg2, len2 as i32);

    result = ucol_strcoll((*locale).info.icu.ucol, uchar1, ulen1, uchar2, ulen2);

    if buf != sbuf.as_mut_ptr() {
        pfree(buf as *mut c_void);
    }

    result
}

/* 'srclen' of -1 means the strings are NUL-terminated */
pub unsafe extern "C" fn strnxfrm_prefix_icu(
    dest: *mut c_char,
    destsize: usize,
    src: *const c_char,
    srclen: ssize_t,
    locale: pg_locale_t,
) -> usize {
    let mut sbuf: [c_char; TEXTBUFLEN] = [0; TEXTBUFLEN];
    let mut buf: *mut c_char = sbuf.as_mut_ptr();
    let mut iter: UCharIterator = std::mem::zeroed();
    let mut state: [u32; 2] = [0; 2];
    let mut status: UErrorCode;
    let mut ulen: i32;
    let uchar: *mut UChar;
    let uchar_bsize: usize;
    let result_bsize: Size;

    Assert!((*locale).provider == COLLPROVIDER_ICU);

    /* if encoding is UTF8, use more efficient strnxfrm_prefix_icu_utf8 */
    Assert!(GetDatabaseEncoding() != PG_UTF8 as c_int);

    init_icu_converter();

    ulen = uchar_length(icu_converter, src, srclen as i32) as i32;

    uchar_bsize = (ulen as usize + 1) * std::mem::size_of::<UChar>();

    if uchar_bsize > TEXTBUFLEN {
        buf = palloc(uchar_bsize) as *mut c_char;
    }

    uchar = buf as *mut UChar;

    ulen = uchar_convert(icu_converter, uchar, ulen + 1, src, srclen as i32);

    uiter_setString(&mut iter, uchar, ulen);
    state[0] = 0;
    state[1] = 0; /* won't need that again */
    status = U_ZERO_ERROR;
    result_bsize = ucol_nextSortKeyPart(
        (*locale).info.icu.ucol,
        &mut iter,
        state.as_mut_ptr(),
        dest as *mut u8,
        destsize as i32,
        &mut status,
    ) as Size;
    if U_FAILURE(status) {
        ereport!(
            ERROR,
            errmsg!("sort key generation failed: {}", cstr(u_errorName(status)))
        );
    }

    if buf != sbuf.as_mut_ptr() {
        pfree(buf as *mut c_void);
    }

    result_bsize as usize
}

unsafe fn init_icu_converter() {
    let icu_encoding_name: *const c_char;
    let mut status: UErrorCode;
    let conv: *mut UConverter;

    if !icu_converter.is_null() {
        return; /* already done */
    }

    icu_encoding_name = get_encoding_name_for_icu(GetDatabaseEncoding());
    if icu_encoding_name.is_null() {
        ereport!(
            ERROR,
            errmsg!(
                "encoding \"{}\" not supported by ICU",
                cstr(pg_encoding_to_char(GetDatabaseEncoding()))
            )
        );
    }

    status = U_ZERO_ERROR;
    conv = ucnv_open(icu_encoding_name, &mut status);
    if U_FAILURE(status) {
        ereport!(
            ERROR,
            errmsg!(
                "could not open ICU converter for encoding \"{}\": {}",
                cstr(icu_encoding_name),
                cstr(u_errorName(status))
            )
        );
    }

    icu_converter = conv;
}

/*
 * Find length, in UChars, of given string if converted to UChar string.
 *
 * A length of -1 indicates that the input string is NUL-terminated.
 */
unsafe fn uchar_length(converter: *mut UConverter, str: *const c_char, len: i32) -> usize {
    let mut status: UErrorCode = U_ZERO_ERROR;
    let ulen: i32;

    ulen = ucnv_toUChars(converter, null_mut(), 0, str, len, &mut status);
    if U_FAILURE(status) && status != U_BUFFER_OVERFLOW_ERROR {
        ereport!(
            ERROR,
            errmsg!("{} failed: {}", "ucnv_toUChars", cstr(u_errorName(status)))
        );
    }
    ulen as usize
}

/*
 * Convert the given source string into a UChar string, stored in dest, and
 * return the length (in UChars).
 *
 * A srclen of -1 indicates that the input string is NUL-terminated.
 */
unsafe fn uchar_convert(
    converter: *mut UConverter,
    dest: *mut UChar,
    destlen: i32,
    src: *const c_char,
    srclen: i32,
) -> i32 {
    let mut status: UErrorCode = U_ZERO_ERROR;
    let ulen: i32;

    status = U_ZERO_ERROR;
    ulen = ucnv_toUChars(converter, dest, destlen, src, srclen, &mut status);
    if U_FAILURE(status) {
        ereport!(
            ERROR,
            errmsg!("{} failed: {}", "ucnv_toUChars", cstr(u_errorName(status)))
        );
    }
    ulen
}

/*
 * Parse collation attributes from the given locale string and apply them to
 * the open collator.
 *
 * First, the locale string is canonicalized to an ICU format locale ID such
 * as "und@colStrength=primary;colCaseLevel=yes". Then, it parses and applies
 * the key-value arguments.
 *
 * Starting with ICU version 54, the attributes are processed automatically by
 * ucol_open(), so this is only necessary for emulating this behavior on older
 * versions.
 */
#[allow(dead_code)]
unsafe fn icu_set_collation_attributes(
    collator: *mut UCollator,
    loc: *const c_char,
    status: *mut UErrorCode,
) {
    let mut len: i32;
    let icu_locale_id: *mut c_char;
    let lower_str: *mut c_char;
    let mut str: *mut c_char;
    let mut token: *mut c_char;

    /*
     * The input locale may be a BCP 47 language tag, e.g.
     * "und-u-kc-ks-level1", which expresses the same attributes in a
     * different form. It will be converted to the equivalent ICU format
     * locale ID, e.g. "und@colcaselevel=yes;colstrength=primary", by
     * uloc_canonicalize().
     */
    *status = U_ZERO_ERROR;
    len = uloc_canonicalize(loc, null_mut(), 0, status);
    icu_locale_id = palloc(len as usize + 1) as *mut c_char;
    *status = U_ZERO_ERROR;
    len = uloc_canonicalize(loc, icu_locale_id, len + 1, status);
    if U_FAILURE(*status) || *status == U_STRING_NOT_TERMINATED_WARNING {
        return;
    }

    lower_str = asc_tolower(icu_locale_id, strlen(icu_locale_id));

    pfree(icu_locale_id as *mut c_void);

    str = strchr(lower_str, b'@' as c_int);
    if str.is_null() {
        return;
    }
    str = str.add(1);

    loop {
        token = strsep(&mut str, c";".as_ptr());
        if token.is_null() {
            break;
        }

        let e: *mut c_char = strchr(token, b'=' as c_int);

        if !e.is_null() {
            let name: *const c_char;
            let value: *const c_char;
            let uattr: UColAttribute;
            let uvalue: UColAttributeValue;

            *status = U_ZERO_ERROR;

            *e = b'\0' as c_char;
            name = token;
            value = e.add(1);

            /*
             * See attribute name and value lists in ICU i18n/coll.cpp
             */
            if strcmp(name, c"colstrength".as_ptr()) == 0 {
                uattr = UCOL_STRENGTH;
            } else if strcmp(name, c"colbackwards".as_ptr()) == 0 {
                uattr = UCOL_FRENCH_COLLATION;
            } else if strcmp(name, c"colcaselevel".as_ptr()) == 0 {
                uattr = UCOL_CASE_LEVEL;
            } else if strcmp(name, c"colcasefirst".as_ptr()) == 0 {
                uattr = UCOL_CASE_FIRST;
            } else if strcmp(name, c"colalternate".as_ptr()) == 0 {
                uattr = UCOL_ALTERNATE_HANDLING;
            } else if strcmp(name, c"colnormalization".as_ptr()) == 0 {
                uattr = UCOL_NORMALIZATION_MODE;
            } else if strcmp(name, c"colnumeric".as_ptr()) == 0 {
                uattr = UCOL_NUMERIC_COLLATION;
            } else {
                /* ignore if unknown */
                continue;
            }

            if strcmp(value, c"primary".as_ptr()) == 0 {
                uvalue = UCOL_PRIMARY;
            } else if strcmp(value, c"secondary".as_ptr()) == 0 {
                uvalue = UCOL_SECONDARY;
            } else if strcmp(value, c"tertiary".as_ptr()) == 0 {
                uvalue = UCOL_TERTIARY;
            } else if strcmp(value, c"quaternary".as_ptr()) == 0 {
                uvalue = UCOL_QUATERNARY;
            } else if strcmp(value, c"identical".as_ptr()) == 0 {
                uvalue = UCOL_IDENTICAL;
            } else if strcmp(value, c"no".as_ptr()) == 0 {
                uvalue = UCOL_OFF;
            } else if strcmp(value, c"yes".as_ptr()) == 0 {
                uvalue = UCOL_ON;
            } else if strcmp(value, c"shifted".as_ptr()) == 0 {
                uvalue = UCOL_SHIFTED;
            } else if strcmp(value, c"non-ignorable".as_ptr()) == 0 {
                uvalue = UCOL_NON_IGNORABLE;
            } else if strcmp(value, c"lower".as_ptr()) == 0 {
                uvalue = UCOL_LOWER_FIRST;
            } else if strcmp(value, c"upper".as_ptr()) == 0 {
                uvalue = UCOL_UPPER_FIRST;
            } else {
                *status = U_ILLEGAL_ARGUMENT_ERROR;
                break;
            }

            ucol_setAttribute(collator, uattr, uvalue, status);
        }
    }

    pfree(lower_str as *mut c_void);
}

// ===========================================================================
// Local stubs for not-yet-ported dependencies.
// ===========================================================================

/// Render a NUL-terminated C string pointer for `{}` formatting in error messages.
/// (Helper for translating C printf "%s" of `const char *`; not in the C source.)
unsafe fn cstr<'a>(p: *const c_char) -> std::borrow::Cow<'a, str> {
    if p.is_null() {
        std::borrow::Cow::Borrowed("(null)")
    } else {
        std::ffi::CStr::from_ptr(p).to_string_lossy()
    }
}

// utils/pg_locale.h -- struct collate_methods, locale_info, pg_locale_struct.
// TODO(pg-port): real definitions live in src/include/utils/pg_locale.h (not yet
// ported). Mirror the layout used by pg_locale_libc.rs so the providers agree.
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

/// ICU-specific locale info (pg_locale.h: struct's info.icu).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct pg_locale_icu_info {
    pub locale: *const c_char,
    pub ucol: *mut UCollator,
}

#[repr(C)]
pub union locale_info {
    pub lt: *mut c_void,
    pub builtin: *mut c_void,
    pub icu: pg_locale_icu_info,
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

// TODO(pg-port): catalog/pg_collation_d.h - COLLPROVIDER_ICU.
const COLLPROVIDER_ICU: c_char = b'i' as c_char;

// TODO(pg-port): catalog/pg_collation_d.h - DEFAULT_COLLATION_OID.
#[allow(non_upper_case_globals)]
const DEFAULT_COLLATION_OID: Oid = 100;

// TODO(pg-port): catalog/pg_database.h / pg_collation.h - attribute numbers.
const Anum_pg_database_datlocale: c_int = 18;
const Anum_pg_database_daticurules: c_int = 19;
const Anum_pg_collation_colllocale: c_int = 8;
const Anum_pg_collation_collicurules: c_int = 9;

// TODO(pg-port): utils/syscache.h - syscache ids.
const DATABASEOID: c_int = 0;
const COLLOID: c_int = 0;

// TODO(pg-port): access/htup.h - opaque heap tuple pointer.
#[allow(non_camel_case_types)]
type HeapTuple = *mut c_void;

/// Form_pg_collation (catalog/pg_collation.h): pointer to the on-disk collation row.
// TODO(pg-port): real Form_pg_collation lives in catalog/pg_collation.h.
#[repr(C)]
pub struct FormData_pg_collation {
    pub collisdeterministic: bool,
}
#[allow(non_camel_case_types)]
type Form_pg_collation = *mut FormData_pg_collation;

unsafe fn HeapTupleIsValid(tuple: HeapTuple) -> bool {
    !tuple.is_null()
}

// TODO(pg-port): access/htup_details.h - GETSTRUCT.
unsafe fn GETSTRUCT(_tuple: HeapTuple) -> *mut c_void {
    unimplemented!()
}

// TODO(pg-port): utils/syscache.h not ported.
unsafe fn SearchSysCache1(_cacheId: c_int, _key1: Datum) -> HeapTuple {
    unimplemented!()
}

// TODO(pg-port): utils/syscache.h not ported.
unsafe fn SysCacheGetAttrNotNull(_cacheId: c_int, _tup: HeapTuple, _attributeNumber: c_int) -> Datum {
    unimplemented!()
}

// TODO(pg-port): utils/syscache.h not ported.
unsafe fn SysCacheGetAttr(
    _cacheId: c_int,
    _tup: HeapTuple,
    _attributeNumber: c_int,
    _isNull: *mut bool,
) -> Datum {
    unimplemented!()
}

// TODO(pg-port): utils/syscache.h not ported.
unsafe fn ReleaseSysCache(_tuple: HeapTuple) {
    unimplemented!()
}
