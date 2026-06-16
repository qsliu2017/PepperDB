//! PostgreSQL locale utilities
//!
//! Portions Copyright (c) 2002-2025, PostgreSQL Global Development Group
//!
//! src/backend/utils/adt/pg_locale.c
//!
//! Here is how the locale stuff is handled: LC_COLLATE and LC_CTYPE
//! are fixed at CREATE DATABASE time, stored in pg_database, and cannot
//! be changed. Thus, the effects of strcoll(), strxfrm(), isupper(),
//! toupper(), etc. are always in the same fixed locale.
//!
//! LC_MESSAGES is settable at run time and will take effect
//! immediately.
//!
//! The other categories, LC_MONETARY, LC_NUMERIC, and LC_TIME are
//! permanently set to "C", and then we use temporary locale_t
//! objects when we need to look up locale data based on the GUCs
//! of the same name.  Information is cached when the GUCs change.
//! The cached information is only used by the formatting functions
//! (to_char, etc.) and the money type.  For the user, this should all be
//! transparent.

#![allow(non_snake_case, non_upper_case_globals, non_camel_case_types)]
#![allow(unused_variables, unused_assignments, unused_mut, dead_code)]

use crate::prelude::*; // postgres.h: Datum, palloc/pstrdup/pfree, elog!/ereport!/errmsg!, Size, etc.
use core::ffi::{c_char, c_int, c_void, CStr};

use crate::postgres_ext::{InvalidOid, Oid};
use crate::c::{uint32, OidIsValid};

use crate::common::hashfn::murmurhash32;
use crate::common::string::pg_is_ascii;
use crate::mb::pg_wchar::{pg_encoding_to_char, PG_SQL_ASCII, PG_UTF8};
use crate::utils::init::globals::{IsBinaryUpgrade, MyDatabaseId};
use crate::utils::mb::mbutils::{
    pg_any_to_server, GetDatabaseEncoding, SetMessageEncoding,
};
use crate::port::port_api::pg_get_encoding_from_locale;
use crate::utils::misc::guc::GucSource;
use crate::utils::mmgr::mcxt::MemoryContextStrdup;
use crate::utils::memutils::ALLOCSET_DEFAULT_SIZES;

// C <locale.h> struct lconv.  port_api::lconv is an opaque c_void alias, which
// cannot be field-accessed; this file needs every field, so it declares the
// faithful layout here (matches the system header field-for-field).
#[repr(C)]
pub struct lconv {
    pub decimal_point: *mut c_char,
    pub thousands_sep: *mut c_char,
    pub grouping: *mut c_char,
    pub int_curr_symbol: *mut c_char,
    pub currency_symbol: *mut c_char,
    pub mon_decimal_point: *mut c_char,
    pub mon_thousands_sep: *mut c_char,
    pub mon_grouping: *mut c_char,
    pub positive_sign: *mut c_char,
    pub negative_sign: *mut c_char,
    pub int_frac_digits: c_char,
    pub frac_digits: c_char,
    pub p_cs_precedes: c_char,
    pub p_sep_by_space: c_char,
    pub n_cs_precedes: c_char,
    pub n_sep_by_space: c_char,
    pub p_sign_posn: c_char,
    pub n_sign_posn: c_char,
    pub int_p_cs_precedes: c_char,
    pub int_p_sep_by_space: c_char,
    pub int_n_cs_precedes: c_char,
    pub int_n_sep_by_space: c_char,
    pub int_p_sign_posn: c_char,
    pub int_n_sign_posn: c_char,
}

// Provider-specific entry points (pg_locale_builtin.c / pg_locale_libc.c).  The
// ICU provider entry points live in pg_locale_icu.c but are referenced only
// under USE_ICU, which PepperDB does not enable, so they are not imported here.
use crate::utils::adt::pg_locale_builtin::{
    create_pg_locale_builtin, get_collation_actual_version_builtin,
};
use crate::utils::adt::pg_locale_libc::{
    create_pg_locale_libc, get_collation_actual_version_libc, report_newlocale_failure,
};

// ===========================================================================
// libc / system declarations.
// ===========================================================================

#[allow(non_camel_case_types)]
type ssize_t = isize;
#[allow(non_camel_case_types)]
type locale_t = *mut c_void;
#[allow(non_camel_case_types)]
type time_t = i64;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct tm {
    pub tm_sec: c_int,
    pub tm_min: c_int,
    pub tm_hour: c_int,
    pub tm_mday: c_int,
    pub tm_mon: c_int,
    pub tm_year: c_int,
    pub tm_wday: c_int,
    pub tm_yday: c_int,
    pub tm_isdst: c_int,
    pub tm_gmtoff: i64,
    pub tm_zone: *const c_char,
}

extern "C" {
    fn setlocale(category: c_int, locale: *const c_char) -> *mut c_char;
    fn setenv(name: *const c_char, value: *const c_char, overwrite: c_int) -> c_int;
    fn strlen(s: *const c_char) -> usize;
    fn strcmp(a: *const c_char, b: *const c_char) -> c_int;
    fn strlcpy(dst: *mut c_char, src: *const c_char, siz: usize) -> usize;
    fn free(ptr: *mut c_void);
    fn strdup(s: *const c_char) -> *mut c_char;

    fn newlocale(category_mask: c_int, locale: *const c_char, base: locale_t) -> locale_t;
    fn freelocale(loc: locale_t);
    fn time(tloc: *mut time_t) -> time_t;
    fn gmtime_r(timep: *const time_t, result: *mut tm) -> *mut tm;
    fn strftime_l(
        s: *mut c_char,
        maxsize: usize,
        format: *const c_char,
        timeptr: *const tm,
        loc: locale_t,
    ) -> usize;

    // Darwin errno.
    fn __error() -> *mut c_int;
}

#[inline]
unsafe fn errno() -> c_int {
    *__error()
}
#[inline]
unsafe fn set_errno(e: c_int) {
    *__error() = e;
}

// setlocale() category numbers (macOS <locale.h> values).
const LC_ALL: c_int = 0;
const LC_COLLATE: c_int = 1;
const LC_CTYPE: c_int = 2;
const LC_MONETARY: c_int = 3;
const LC_NUMERIC: c_int = 4;
const LC_TIME: c_int = 5;
const LC_MESSAGES: c_int = 6;

// newlocale() mask (macOS <xlocale.h> value for LC_ALL_MASK).
const LC_ALL_MASK: c_int = 0x0000_7fff;

const ENOENT: c_int = 2;

// Error triggered for locale-sensitive subroutines.
//
// C: #define PGLOCALE_SUPPORT_ERROR(provider)
//        elog(ERROR, "unsupported collprovider for %s: %c", __func__, provider)
macro_rules! PGLOCALE_SUPPORT_ERROR {
    ($func:expr, $provider:expr) => {
        elog!(
            ERROR,
            "unsupported collprovider for {}: {}",
            $func,
            $provider as u8 as char
        )
    };
}

/*
 * This should be large enough that most strings will fit, but small enough
 * that we feel comfortable putting it on the stack
 */
const TEXTBUFLEN: usize = 1024;

const MAX_L10N_DATA: usize = 80;

// catalog/pg_collation.h: collation provider codes.
const COLLPROVIDER_BUILTIN: c_char = b'b' as c_char;
const COLLPROVIDER_ICU: c_char = b'i' as c_char;
const COLLPROVIDER_LIBC: c_char = b'c' as c_char;

// catalog/pg_collation_d.h / pg_collation.h fixed OIDs.
const DEFAULT_COLLATION_OID: Oid = 100;
const C_COLLATION_OID: Oid = 950;

// LOCALE_NAME_BUFLEN (utils/pg_locale.h).
const LOCALE_NAME_BUFLEN: usize = 128;

// ===========================================================================
// Win32-only API used by strftime_l_win32(), search_locale_enum(),
// get_iso_localename() and IsoLocaleName().  Compiled only on Windows; the
// originals live inside #ifdef WIN32 blocks in pg_locale.c.
#[cfg(windows)]
const LOCALE_NAME_MAX_LENGTH: usize = 85;
#[cfg(windows)]
const CP_UTF8: u32 = 65001;
#[cfg(windows)]
const CP_ACP: u32 = 0;
#[cfg(windows)]
const FALSE: i32 = 0;
#[cfg(windows)]
const TRUE: i32 = 1;
#[cfg(windows)]
const LOCALE_SENGLISHLANGUAGENAME: u32 = 0x00001001;
#[cfg(windows)]
const LOCALE_SENGLISHCOUNTRYNAME: u32 = 0x00001002;
#[cfg(windows)]
const LOCALE_SNAME: u32 = 0x0000005C;
#[cfg(windows)]
const LOCALE_WINDOWS: u32 = 0x00000001;
// L"_"
#[cfg(windows)]
static L_UNDERSCORE: [u16; 2] = [b'_' as u16, 0];

#[cfg(windows)]
extern "system" {
    fn MultiByteToWideChar(
        CodePage: u32,
        dwFlags: u32,
        lpMultiByteStr: *const c_char,
        cbMultiByte: c_int,
        lpWideCharStr: *mut u16,
        cchWideChar: c_int,
    ) -> c_int;
    fn WideCharToMultiByte(
        CodePage: u32,
        dwFlags: u32,
        lpWideCharStr: *const u16,
        cchWideChar: c_int,
        lpMultiByteStr: *mut c_char,
        cbMultiByte: c_int,
        lpDefaultChar: *const c_char,
        lpUsedDefaultChar: *mut c_int,
    ) -> c_int;
    fn GetLastError() -> u32;
    fn GetLocaleInfoEx(
        lpLocaleName: *const u16,
        LCType: u32,
        lpLCData: *mut u16,
        cchData: c_int,
    ) -> c_int;
    fn EnumSystemLocalesEx(
        lpLocaleEnumProcEx: Option<unsafe extern "system" fn(*mut u16, u32, isize) -> i32>,
        dwFlags: u32,
        lParam: isize,
        lpReserved: *mut c_void,
    ) -> i32;
}

#[cfg(windows)]
use crate::utils::adt::pg_locale_libc::wchar2char;
#[cfg(windows)]
use crate::mb::mbutils::pg_mbstrlen;

#[cfg(windows)]
extern "C" {
    fn pg_strcasecmp(s1: *const c_char, s2: *const c_char) -> c_int; // port/pgstrcasecmp.c
    fn _wcsftime_l(
        s: *mut u16,
        maxsize: usize,
        format: *const u16,
        timeptr: *const libc::tm,
        locale: locale_t,
    ) -> usize;
    fn _wcsicmp(s1: *const u16, s2: *const u16) -> c_int;
    fn wcsrchr(s: *const u16, c: u16) -> *mut u16;
    fn wcscpy(dst: *mut u16, src: *const u16) -> *mut u16;
    fn wcscat(dst: *mut u16, src: *const u16) -> *mut u16;
    fn wcslen(s: *const u16) -> usize;
}

// ===========================================================================
// Shared pg_locale types (utils/pg_locale.h).  Mirrors the canonical struct
// used by the sibling provider modules.
// ===========================================================================

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
unsafe impl Sync for pg_locale_struct {}

// ===========================================================================
// GUC settings.
// ===========================================================================

#[no_mangle]
pub static mut locale_messages: *mut c_char = core::ptr::null_mut();
#[no_mangle]
pub static mut locale_monetary: *mut c_char = core::ptr::null_mut();
#[no_mangle]
pub static mut locale_numeric: *mut c_char = core::ptr::null_mut();
#[no_mangle]
pub static mut locale_time: *mut c_char = core::ptr::null_mut();

pub static mut icu_validation_level: c_int = WARNING;

/*
 * lc_time localization cache.
 *
 * We use only the first 7 or 12 entries of these arrays.  The last array
 * element is left as NULL for the convenience of outside code that wants
 * to sequentially scan these arrays.
 */
pub static mut localized_abbrev_days: [*mut c_char; 7 + 1] = [core::ptr::null_mut(); 7 + 1];
pub static mut localized_full_days: [*mut c_char; 7 + 1] = [core::ptr::null_mut(); 7 + 1];
pub static mut localized_abbrev_months: [*mut c_char; 12 + 1] = [core::ptr::null_mut(); 12 + 1];
pub static mut localized_full_months: [*mut c_char; 12 + 1] = [core::ptr::null_mut(); 12 + 1];

/* is the databases's LC_CTYPE the C locale? */
pub static mut database_ctype_is_c: bool = false;

static mut default_locale: pg_locale_t = core::ptr::null_mut();

/* indicates whether locale information cache is valid */
static mut CurrentLocaleConvValid: bool = false;
static mut CurrentLCTimeValid: bool = false;

static mut c_locale: pg_locale_struct = pg_locale_struct {
    provider: COLLPROVIDER_LIBC,
    deterministic: true,
    collate_is_c: true,
    ctype_is_c: true,
    is_default: false,
    collate: core::ptr::null(),
    ctype: core::ptr::null(),
    info: locale_info {
        lt: core::ptr::null_mut(),
    },
};

/* Cache for collation-related knowledge */

#[repr(C)]
pub struct collation_cache_entry {
    pub collid: Oid, /* hash key: pg_collation OID */
    pub locale: pg_locale_t, /* locale_t struct, or 0 if not valid */

    /* needed for simplehash */
    pub hash: uint32,
    pub status: c_char,
}

static mut CollationCacheContext: MemoryContext = core::ptr::null_mut();
static mut CollationCache: *mut collation_cache_hash = core::ptr::null_mut();

/*
 * The collation cache is often accessed repeatedly for the same collation, so
 * remember the last one used.
 */
static mut last_collation_cache_oid: Oid = InvalidOid;
static mut last_collation_cache_locale: pg_locale_t = core::ptr::null_mut();

/*
 * pg_perm_setlocale
 *
 * This wraps the libc function setlocale(), with two additions.  First, when
 * changing LC_CTYPE, update gettext's encoding for the current message
 * domain.  GNU gettext automatically tracks LC_CTYPE on most platforms, but
 * not on Windows.  Second, if the operation is successful, the corresponding
 * LC_XXX environment variable is set to match.  By setting the environment
 * variable, we ensure that any subsequent use of setlocale(..., "") will
 * preserve the settings made through this routine.  Of course, LC_ALL must
 * also be unset to fully ensure that, but that has to be done elsewhere after
 * all the individual LC_XXX variables have been set correctly.  (Thank you
 * Perl for making this kluge necessary.)
 */
pub unsafe fn pg_perm_setlocale(category: c_int, locale: *const c_char) -> *mut c_char {
    let mut result: *mut c_char;
    let envvar: *const c_char;

    result = setlocale(category, locale);

    if result.is_null() {
        return result; /* fall out immediately on failure */
    }

    /*
     * Use the right encoding in translated messages.  Under ENABLE_NLS, let
     * pg_bind_textdomain_codeset() figure it out.  Under !ENABLE_NLS, message
     * format strings are ASCII, but database-encoding strings may enter the
     * message via %s.  This makes the overall message encoding equal to the
     * database encoding.
     */
    if category == LC_CTYPE {
        static mut save_lc_ctype: [c_char; LOCALE_NAME_BUFLEN] = [0; LOCALE_NAME_BUFLEN];

        /* copy setlocale() return value before callee invokes it again */
        strlcpy(
            save_lc_ctype.as_mut_ptr(),
            result,
            core::mem::size_of_val(&save_lc_ctype),
        );
        result = save_lc_ctype.as_mut_ptr();

        // #ifdef ENABLE_NLS
        //     SetMessageEncoding(pg_bind_textdomain_codeset(textdomain(NULL)));
        // #else
        SetMessageEncoding(GetDatabaseEncoding());
        // #endif
    }

    match category {
        x if x == LC_COLLATE => {
            envvar = b"LC_COLLATE\0".as_ptr() as *const c_char;
        }
        x if x == LC_CTYPE => {
            envvar = b"LC_CTYPE\0".as_ptr() as *const c_char;
        }
        x if x == LC_MESSAGES => {
            envvar = b"LC_MESSAGES\0".as_ptr() as *const c_char;
        }
        x if x == LC_MONETARY => {
            envvar = b"LC_MONETARY\0".as_ptr() as *const c_char;
        }
        x if x == LC_NUMERIC => {
            envvar = b"LC_NUMERIC\0".as_ptr() as *const c_char;
        }
        x if x == LC_TIME => {
            envvar = b"LC_TIME\0".as_ptr() as *const c_char;
        }
        _ => {
            elog!(FATAL, "unrecognized LC category: {}", category);
            return core::ptr::null_mut(); /* keep compiler quiet */
        }
    }

    if setenv(envvar, result, 1) != 0 {
        return core::ptr::null_mut();
    }

    result
}

/*
 * Is the locale name valid for the locale category?
 *
 * If successful, and canonname isn't NULL, a palloc'd copy of the locale's
 * canonical name is stored there.  This is especially useful for figuring out
 * what locale name "" means (ie, the server environment value).  (Actually,
 * it seems that on most implementations that's the only thing it's good for;
 * we could wish that setlocale gave back a canonically spelled version of
 * the locale name, but typically it doesn't.)
 */
pub unsafe fn check_locale(
    category: c_int,
    locale: *const c_char,
    canonname: *mut *mut c_char,
) -> bool {
    let mut save: *mut c_char;
    let res: *mut c_char;

    /* Don't let Windows' non-ASCII locale names in. */
    if !pg_is_ascii(locale) {
        ereport!(
            WARNING,
            errmsg!(
                "locale name \"{}\" contains non-ASCII characters",
                CStr::from_ptr(locale).to_string_lossy()
            )
        );
        // C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE)
        return false;
    }

    if !canonname.is_null() {
        *canonname = core::ptr::null_mut(); /* in case of failure */
    }

    save = setlocale(category, core::ptr::null());
    if save.is_null() {
        return false; /* won't happen, we hope */
    }

    /* save may be pointing at a modifiable scratch variable, see above. */
    save = pstrdup(save);

    /* set the locale with setlocale, to see if it accepts it. */
    res = setlocale(category, locale);

    /* save canonical name if requested. */
    if !res.is_null() && !canonname.is_null() {
        *canonname = pstrdup(res);
    }

    /* restore old value. */
    if setlocale(category, save).is_null() {
        elog!(
            WARNING,
            "failed to restore old locale \"{}\"",
            CStr::from_ptr(save).to_string_lossy()
        );
    }
    pfree(save as *mut c_void);

    /* Don't let Windows' non-ASCII locale names out. */
    if !canonname.is_null() && !(*canonname).is_null() && !pg_is_ascii(*canonname) {
        ereport!(
            WARNING,
            errmsg!(
                "locale name \"{}\" contains non-ASCII characters",
                CStr::from_ptr(*canonname).to_string_lossy()
            )
        );
        // C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE)
        pfree(*canonname as *mut c_void);
        *canonname = core::ptr::null_mut();
        return false;
    }

    !res.is_null()
}

/*
 * GUC check/assign hooks
 *
 * For most locale categories, the assign hook doesn't actually set the locale
 * permanently, just reset flags so that the next use will cache the
 * appropriate values.  (See explanation at the top of this file.)
 *
 * Note: we accept value = "" as selecting the postmaster's environment
 * value, whatever it was (so long as the environment setting is legal).
 * This will have been locked down by an earlier call to pg_perm_setlocale.
 */
pub unsafe fn check_locale_monetary(
    newval: *mut *mut c_char,
    extra: *mut *mut c_void,
    source: GucSource,
) -> bool {
    check_locale(LC_MONETARY, *newval, core::ptr::null_mut())
}

pub unsafe fn assign_locale_monetary(newval: *const c_char, extra: *mut c_void) {
    CurrentLocaleConvValid = false;
}

pub unsafe fn check_locale_numeric(
    newval: *mut *mut c_char,
    extra: *mut *mut c_void,
    source: GucSource,
) -> bool {
    check_locale(LC_NUMERIC, *newval, core::ptr::null_mut())
}

pub unsafe fn assign_locale_numeric(newval: *const c_char, extra: *mut c_void) {
    CurrentLocaleConvValid = false;
}

pub unsafe fn check_locale_time(
    newval: *mut *mut c_char,
    extra: *mut *mut c_void,
    source: GucSource,
) -> bool {
    check_locale(LC_TIME, *newval, core::ptr::null_mut())
}

pub unsafe fn assign_locale_time(newval: *const c_char, extra: *mut c_void) {
    CurrentLCTimeValid = false;
}

/*
 * We allow LC_MESSAGES to actually be set globally.
 *
 * Note: we normally disallow value = "" because it wouldn't have consistent
 * semantics (it'd effectively just use the previous value).  However, this
 * is the value passed for PGC_S_DEFAULT, so don't complain in that case,
 * not even if the attempted setting fails due to invalid environment value.
 * The idea there is just to accept the environment setting *if possible*
 * during startup, until we can read the proper value from postgresql.conf.
 */
pub unsafe fn check_locale_messages(
    newval: *mut *mut c_char,
    extra: *mut *mut c_void,
    source: GucSource,
) -> bool {
    if **newval == b'\0' as c_char {
        if matches!(source, GucSource::PGC_S_DEFAULT) {
            return true;
        } else {
            return false;
        }
    }

    /*
     * LC_MESSAGES category does not exist everywhere, but accept it anyway
     *
     * On Windows, we can't even check the value, so accept blindly
     */
    // #if defined(LC_MESSAGES) && !defined(WIN32)
    check_locale(LC_MESSAGES, *newval, core::ptr::null_mut())
    // #else
    //     return true;
    // #endif
}

pub unsafe fn assign_locale_messages(newval: *const c_char, extra: *mut c_void) {
    /*
     * LC_MESSAGES category does not exist everywhere, but accept it anyway.
     * We ignore failure, as per comment above.
     */
    // #ifdef LC_MESSAGES
    let _ = pg_perm_setlocale(LC_MESSAGES, newval);
    // #endif
}

/*
 * Frees the malloced content of a struct lconv.  (But not the struct
 * itself.)  It's important that this not throw elog(ERROR).
 */
unsafe fn free_struct_lconv(s: *mut lconv) {
    free((*s).decimal_point as *mut c_void);
    free((*s).thousands_sep as *mut c_void);
    free((*s).grouping as *mut c_void);
    free((*s).int_curr_symbol as *mut c_void);
    free((*s).currency_symbol as *mut c_void);
    free((*s).mon_decimal_point as *mut c_void);
    free((*s).mon_thousands_sep as *mut c_void);
    free((*s).mon_grouping as *mut c_void);
    free((*s).positive_sign as *mut c_void);
    free((*s).negative_sign as *mut c_void);
}

/*
 * Check that all fields of a struct lconv (or at least, the ones we care
 * about) are non-NULL.  The field list must match free_struct_lconv().
 */
unsafe fn struct_lconv_is_valid(s: *mut lconv) -> bool {
    if (*s).decimal_point.is_null() {
        return false;
    }
    if (*s).thousands_sep.is_null() {
        return false;
    }
    if (*s).grouping.is_null() {
        return false;
    }
    if (*s).int_curr_symbol.is_null() {
        return false;
    }
    if (*s).currency_symbol.is_null() {
        return false;
    }
    if (*s).mon_decimal_point.is_null() {
        return false;
    }
    if (*s).mon_thousands_sep.is_null() {
        return false;
    }
    if (*s).mon_grouping.is_null() {
        return false;
    }
    if (*s).positive_sign.is_null() {
        return false;
    }
    if (*s).negative_sign.is_null() {
        return false;
    }
    true
}

/*
 * Convert the strdup'd string at *str from the specified encoding to the
 * database encoding.
 */
unsafe fn db_encoding_convert(encoding: c_int, str: *mut *mut c_char) {
    let pstr: *mut c_char;
    let mstr: *mut c_char;

    /* convert the string to the database encoding */
    pstr = pg_any_to_server(*str, strlen(*str) as c_int, encoding);
    if pstr == *str {
        return; /* no conversion happened */
    }

    /* need it malloc'd not palloc'd */
    mstr = strdup(pstr);
    if mstr.is_null() {
        ereport!(ERROR, errmsg!("out of memory"));
        // C also: errcode(ERRCODE_OUT_OF_MEMORY)
    }

    /* replace old string */
    free(*str as *mut c_void);
    *str = mstr;

    pfree(pstr as *mut c_void);
}

/*
 * Return the POSIX lconv struct (contains number/money formatting
 * information) with locale information for all categories.
 */
pub unsafe fn PGLC_localeconv() -> *mut lconv {
    static mut CurrentLocaleConv: lconv = lconv {
        decimal_point: core::ptr::null_mut(),
        thousands_sep: core::ptr::null_mut(),
        grouping: core::ptr::null_mut(),
        int_curr_symbol: core::ptr::null_mut(),
        currency_symbol: core::ptr::null_mut(),
        mon_decimal_point: core::ptr::null_mut(),
        mon_thousands_sep: core::ptr::null_mut(),
        mon_grouping: core::ptr::null_mut(),
        positive_sign: core::ptr::null_mut(),
        negative_sign: core::ptr::null_mut(),
        int_frac_digits: 0,
        frac_digits: 0,
        p_cs_precedes: 0,
        p_sep_by_space: 0,
        n_cs_precedes: 0,
        n_sep_by_space: 0,
        p_sign_posn: 0,
        n_sign_posn: 0,
        int_p_cs_precedes: 0,
        int_p_sep_by_space: 0,
        int_n_cs_precedes: 0,
        int_n_sep_by_space: 0,
        int_p_sign_posn: 0,
        int_n_sign_posn: 0,
    };
    static mut CurrentLocaleConvAllocated: bool = false;
    let extlconv: *mut lconv;
    let mut tmp: lconv = core::mem::zeroed();
    let mut worklconv: lconv = core::mem::zeroed();

    /* Did we do it already? */
    if CurrentLocaleConvValid {
        return &raw mut CurrentLocaleConv;
    }

    /* Free any already-allocated storage */
    if CurrentLocaleConvAllocated {
        free_struct_lconv(&raw mut CurrentLocaleConv);
        CurrentLocaleConvAllocated = false;
    }

    /*
     * Use thread-safe method of obtaining a copy of lconv from the operating
     * system.
     */
    if pg_localeconv_r(locale_monetary, locale_numeric, &raw mut tmp) != 0 {
        elog!(
            ERROR,
            "could not get lconv for LC_MONETARY = \"{}\", LC_NUMERIC = \"{}\": (errno)",
            CStr::from_ptr(locale_monetary).to_string_lossy(),
            CStr::from_ptr(locale_numeric).to_string_lossy()
        );
    }

    /* Must copy data now so we can re-encode it. */
    extlconv = &raw mut tmp;
    worklconv.decimal_point = strdup((*extlconv).decimal_point);
    worklconv.thousands_sep = strdup((*extlconv).thousands_sep);
    worklconv.grouping = strdup((*extlconv).grouping);
    worklconv.int_curr_symbol = strdup((*extlconv).int_curr_symbol);
    worklconv.currency_symbol = strdup((*extlconv).currency_symbol);
    worklconv.mon_decimal_point = strdup((*extlconv).mon_decimal_point);
    worklconv.mon_thousands_sep = strdup((*extlconv).mon_thousands_sep);
    worklconv.mon_grouping = strdup((*extlconv).mon_grouping);
    worklconv.positive_sign = strdup((*extlconv).positive_sign);
    worklconv.negative_sign = strdup((*extlconv).negative_sign);
    /* Copy scalar fields as well */
    worklconv.int_frac_digits = (*extlconv).int_frac_digits;
    worklconv.frac_digits = (*extlconv).frac_digits;
    worklconv.p_cs_precedes = (*extlconv).p_cs_precedes;
    worklconv.p_sep_by_space = (*extlconv).p_sep_by_space;
    worklconv.n_cs_precedes = (*extlconv).n_cs_precedes;
    worklconv.n_sep_by_space = (*extlconv).n_sep_by_space;
    worklconv.p_sign_posn = (*extlconv).p_sign_posn;
    worklconv.n_sign_posn = (*extlconv).n_sign_posn;

    /* Free the contents of the object populated by pg_localeconv_r(). */
    pg_localeconv_free(&raw mut tmp);

    /* If any of the preceding strdup calls failed, complain now. */
    if !struct_lconv_is_valid(&raw mut worklconv) {
        ereport!(ERROR, errmsg!("out of memory"));
        // C also: errcode(ERRCODE_OUT_OF_MEMORY)
    }

    // PG_TRY()
    {
        let mut encoding: c_int;

        /*
         * Now we must perform encoding conversion from whatever's associated
         * with the locales into the database encoding.  If we can't identify
         * the encoding implied by LC_NUMERIC or LC_MONETARY (ie we get -1),
         * use PG_SQL_ASCII, which will result in just validating that the
         * strings are OK in the database encoding.
         */
        encoding = pg_get_encoding_from_locale(locale_numeric, true);
        if encoding < 0 {
            encoding = PG_SQL_ASCII;
        }

        db_encoding_convert(encoding, &raw mut worklconv.decimal_point);
        db_encoding_convert(encoding, &raw mut worklconv.thousands_sep);
        /* grouping is not text and does not require conversion */

        encoding = pg_get_encoding_from_locale(locale_monetary, true);
        if encoding < 0 {
            encoding = PG_SQL_ASCII;
        }

        db_encoding_convert(encoding, &raw mut worklconv.int_curr_symbol);
        db_encoding_convert(encoding, &raw mut worklconv.currency_symbol);
        db_encoding_convert(encoding, &raw mut worklconv.mon_decimal_point);
        db_encoding_convert(encoding, &raw mut worklconv.mon_thousands_sep);
        /* mon_grouping is not text and does not require conversion */
        db_encoding_convert(encoding, &raw mut worklconv.positive_sign);
        db_encoding_convert(encoding, &raw mut worklconv.negative_sign);
    }
    // PG_CATCH(): on error, free_struct_lconv(&worklconv); PG_RE_THROW();
    // PG_END_TRY()

    /*
     * Everything is good, so save the results.
     */
    CurrentLocaleConv = worklconv;
    CurrentLocaleConvAllocated = true;
    CurrentLocaleConvValid = true;
    &raw mut CurrentLocaleConv
}

/*
 * Subroutine for cache_locale_time().
 * Convert the given string from encoding "encoding" to the database
 * encoding, and store the result at *dst, replacing any previous value.
 */
unsafe fn cache_single_string(dst: *mut *mut c_char, src: *const c_char, encoding: c_int) {
    let ptr: *mut c_char;
    let olddst: *mut c_char;

    /* Convert the string to the database encoding, or validate it's OK */
    ptr = pg_any_to_server(src, strlen(src) as c_int, encoding);

    /* Store the string in long-lived storage, replacing any previous value */
    olddst = *dst;
    *dst = MemoryContextStrdup(TopMemoryContext, ptr);
    if !olddst.is_null() {
        pfree(olddst as *mut c_void);
    }

    /* Might as well clean up any palloc'd conversion result, too */
    if ptr != src as *mut c_char {
        pfree(ptr as *mut c_void);
    }
}

/*
 * Update the lc_time localization cache variables if needed.
 */
pub unsafe fn cache_locale_time() {
    let mut buf: [c_char; (2 * 7 + 2 * 12) * MAX_L10N_DATA] = [0; (2 * 7 + 2 * 12) * MAX_L10N_DATA];
    let mut bufptr: *mut c_char;
    let timenow: time_t;
    let timeinfo: *mut tm;
    let mut timeinfobuf: tm = core::mem::zeroed();
    let mut strftimefail: bool = false;
    let mut encoding: c_int;
    let mut i: c_int;
    let locale: locale_t;

    /* did we do this already? */
    if CurrentLCTimeValid {
        return;
    }

    elog!(
        DEBUG3,
        "cache_locale_time() executed; locale: \"{}\"",
        CStr::from_ptr(locale_time).to_string_lossy()
    );

    set_errno(ENOENT);
    locale = newlocale(LC_ALL_MASK, locale_time, core::ptr::null_mut());
    if locale.is_null() {
        report_newlocale_failure(locale_time);
    }

    /* We use times close to current time as data for strftime(). */
    timenow = time(core::ptr::null_mut());
    timeinfo = gmtime_r(&timenow, &raw mut timeinfobuf);

    /* Store the strftime results in MAX_L10N_DATA-sized portions of buf[] */
    bufptr = buf.as_mut_ptr();

    /*
     * MAX_L10N_DATA is sufficient buffer space for every known locale, and
     * POSIX defines no strftime() errors.  (Buffer space exhaustion is not an
     * error.)  An implementation might report errors (e.g. ENOMEM) by
     * returning 0 (or, less plausibly, a negative value) and setting errno.
     * Report errno just in case the implementation did that, but clear it in
     * advance of the calls so we don't emit a stale, unrelated errno.
     */
    set_errno(0);

    /* localized days */
    i = 0;
    while i < 7 {
        (*timeinfo).tm_wday = i;
        if strftime_l(
            bufptr,
            MAX_L10N_DATA,
            b"%a\0".as_ptr() as *const c_char,
            timeinfo,
            locale,
        ) == 0
        {
            strftimefail = true;
        }
        bufptr = bufptr.add(MAX_L10N_DATA);
        if strftime_l(
            bufptr,
            MAX_L10N_DATA,
            b"%A\0".as_ptr() as *const c_char,
            timeinfo,
            locale,
        ) == 0
        {
            strftimefail = true;
        }
        bufptr = bufptr.add(MAX_L10N_DATA);
        i += 1;
    }

    /* localized months */
    i = 0;
    while i < 12 {
        (*timeinfo).tm_mon = i;
        (*timeinfo).tm_mday = 1; /* make sure we don't have invalid date */
        if strftime_l(
            bufptr,
            MAX_L10N_DATA,
            b"%b\0".as_ptr() as *const c_char,
            timeinfo,
            locale,
        ) == 0
        {
            strftimefail = true;
        }
        bufptr = bufptr.add(MAX_L10N_DATA);
        if strftime_l(
            bufptr,
            MAX_L10N_DATA,
            b"%B\0".as_ptr() as *const c_char,
            timeinfo,
            locale,
        ) == 0
        {
            strftimefail = true;
        }
        bufptr = bufptr.add(MAX_L10N_DATA);
        i += 1;
    }

    freelocale(locale);

    /*
     * At this point we've done our best to clean up, and can throw errors, or
     * call functions that might throw errors, with a clean conscience.
     */
    if strftimefail {
        elog!(ERROR, "strftime_l() failed");
    }

    /*
     * As in PGLC_localeconv(), we must convert strftime()'s output from the
     * encoding implied by LC_TIME to the database encoding.  If we can't
     * identify the LC_TIME encoding, just perform encoding validation.
     */
    encoding = pg_get_encoding_from_locale(locale_time, true);
    if encoding < 0 {
        encoding = PG_SQL_ASCII;
    }

    bufptr = buf.as_mut_ptr();

    /* localized days */
    i = 0;
    while i < 7 {
        cache_single_string(&raw mut localized_abbrev_days[i as usize], bufptr, encoding);
        bufptr = bufptr.add(MAX_L10N_DATA);
        cache_single_string(&raw mut localized_full_days[i as usize], bufptr, encoding);
        bufptr = bufptr.add(MAX_L10N_DATA);
        i += 1;
    }
    localized_abbrev_days[7] = core::ptr::null_mut();
    localized_full_days[7] = core::ptr::null_mut();

    /* localized months */
    i = 0;
    while i < 12 {
        cache_single_string(&raw mut localized_abbrev_months[i as usize], bufptr, encoding);
        bufptr = bufptr.add(MAX_L10N_DATA);
        cache_single_string(&raw mut localized_full_months[i as usize], bufptr, encoding);
        bufptr = bufptr.add(MAX_L10N_DATA);
        i += 1;
    }
    localized_abbrev_months[12] = core::ptr::null_mut();
    localized_full_months[12] = core::ptr::null_mut();

    CurrentLCTimeValid = true;
}

// #ifdef WIN32
/*
 * On Windows, strftime_l() is not available, so we provide a workaround using
 * the wide-character API.  Installed via #define strftime_l(...) on WIN32.
 *
 * Convert to UTF-16, call _wcsftime_l, convert back to the database encoding.
 * (See the C source comment block at strftime_l_win32 for full rationale.)
 */
#[cfg(windows)]
unsafe fn strftime_l_win32(
    dst: *mut c_char,
    dstlen: usize,
    format: *const c_char,
    tm: *const libc::tm,
    locale: locale_t,
) -> usize {
    let mut len: usize;
    let mut wformat: [u16; 8] = [0; 8]; /* formats used below need 3 chars */
    let mut wbuf: [u16; MAX_L10N_DATA] = [0; MAX_L10N_DATA];

    /*
     * Get a wchar_t version of the format string.  We only actually use
     * plain-ASCII formats in this file, so we can say that they're UTF8.
     */
    len = MultiByteToWideChar(
        CP_UTF8,
        0,
        format,
        -1,
        wformat.as_mut_ptr(),
        wformat.len() as c_int,
    ) as usize;
    if len == 0 {
        elog!(
            ERROR,
            "could not convert format string from UTF-8: error code {}",
            GetLastError()
        );
    }

    len = _wcsftime_l(
        wbuf.as_mut_ptr(),
        MAX_L10N_DATA,
        wformat.as_ptr(),
        tm,
        locale,
    );
    if len == 0 {
        /*
         * wcsftime failed, possibly because the result would not fit in
         * MAX_L10N_DATA.  Return 0 with the contents of dst unspecified.
         */
        return 0;
    }

    len = WideCharToMultiByte(
        CP_UTF8,
        0,
        wbuf.as_ptr(),
        len as c_int,
        dst,
        (dstlen - 1) as c_int,
        core::ptr::null(),
        core::ptr::null_mut(),
    ) as usize;
    if len == 0 {
        elog!(
            ERROR,
            "could not convert string to UTF-8: error code {}",
            GetLastError()
        );
    }

    *dst.add(len) = b'\0' as c_char;

    len
}
// #endif /* WIN32 */

// #if defined(WIN32) && defined(LC_MESSAGES)
/*
 * Callback function for EnumSystemLocalesEx() in get_iso_localename().
 *
 * Matches a Windows locale name against the requested locale name, which has
 * an input with the format: <Language>[_<Country>], e.g.
 * English[_United States]
 *
 * The input is a three wchar_t array as an LPARAM. The first element is the
 * locale_name we want to match, the second element is an allocated buffer
 * where the Unix-style locale is copied if a match is found, and the third
 * element is the search status, 1 if a match was found, 0 otherwise.
 */
#[cfg(windows)]
unsafe extern "system" fn search_locale_enum(
    pStr: *mut u16,
    dwFlags: u32,
    lparam: isize,
) -> i32 {
    let mut test_locale: [u16; LOCALE_NAME_MAX_LENGTH] = [0; LOCALE_NAME_MAX_LENGTH];
    let argv: *mut *mut u16;

    let _ = dwFlags;

    argv = lparam as *mut *mut u16;
    *(*argv.add(2)) = 0u16;

    core::ptr::write_bytes(test_locale.as_mut_ptr(), 0, test_locale.len());

    /* Get the name of the <Language> in English */
    if GetLocaleInfoEx(
        pStr,
        LOCALE_SENGLISHLANGUAGENAME,
        test_locale.as_mut_ptr(),
        LOCALE_NAME_MAX_LENGTH as c_int,
    ) != 0
    {
        /*
         * If the enumerated locale does not have a hyphen ("en") OR the
         * locale_name input does not have an underscore ("English"), we only
         * need to compare the <Language> tags.
         */
        if wcsrchr(pStr, '-' as u16).is_null()
            || wcsrchr(*argv.add(0), '_' as u16).is_null()
        {
            if _wcsicmp(*argv.add(0), test_locale.as_ptr()) == 0 {
                wcscpy(*argv.add(1), pStr);
                *(*argv.add(2)) = 1u16;
                return FALSE;
            }
        }
        /*
         * We have to compare a full <Language>_<Country> tag, so we append
         * the underscore and name of the country/region in English, e.g.
         * "English_United States".
         */
        else {
            let len: usize;

            wcscat(test_locale.as_mut_ptr(), L_UNDERSCORE.as_ptr());
            len = wcslen(test_locale.as_ptr());
            if GetLocaleInfoEx(
                pStr,
                LOCALE_SENGLISHCOUNTRYNAME,
                test_locale.as_mut_ptr().add(len),
                (LOCALE_NAME_MAX_LENGTH - len) as c_int,
            ) != 0
            {
                if _wcsicmp(*argv.add(0), test_locale.as_ptr()) == 0 {
                    wcscpy(*argv.add(1), pStr);
                    *(*argv.add(2)) = 1u16;
                    return FALSE;
                }
            }
        }
    }

    TRUE
}

/*
 * This function converts a Windows locale name to an ISO formatted version
 * for Visual Studio 2015 or greater.
 *
 * Returns NULL, if no valid conversion was found.
 */
#[cfg(windows)]
unsafe fn get_iso_localename(winlocname: *const c_char) -> *mut c_char {
    let mut wc_locale_name: [u16; LOCALE_NAME_MAX_LENGTH] = [0; LOCALE_NAME_MAX_LENGTH];
    let mut buffer: [u16; LOCALE_NAME_MAX_LENGTH] = [0; LOCALE_NAME_MAX_LENGTH];
    static mut iso_lc_messages: [c_char; LOCALE_NAME_MAX_LENGTH] = [0; LOCALE_NAME_MAX_LENGTH];
    let period: *const c_char;
    let len: c_int;
    let mut ret_val: c_int;

    /*
     * Valid locales have the following syntax:
     * <Language>[_<Country>[.<CodePage>]]
     *
     * GetLocaleInfoEx can only take locale name without code-page and for the
     * purpose of this API the code-page doesn't matter.
     */
    period = libc::strchr(winlocname, '.' as c_int);
    if !period.is_null() {
        len = period.offset_from(winlocname) as c_int;
    } else {
        len = pg_mbstrlen(winlocname);
    }

    core::ptr::write_bytes(wc_locale_name.as_mut_ptr(), 0, wc_locale_name.len());
    core::ptr::write_bytes(buffer.as_mut_ptr(), 0, buffer.len());
    MultiByteToWideChar(
        CP_ACP,
        0,
        winlocname,
        len,
        wc_locale_name.as_mut_ptr(),
        LOCALE_NAME_MAX_LENGTH as c_int,
    );

    /*
     * If the lc_messages is already a Unix-style string, we have a direct
     * match with LOCALE_SNAME, e.g. en-US, en_US.
     */
    ret_val = GetLocaleInfoEx(
        wc_locale_name.as_ptr(),
        LOCALE_SNAME,
        buffer.as_mut_ptr(),
        LOCALE_NAME_MAX_LENGTH as c_int,
    );
    if ret_val == 0 {
        /*
         * Search for a locale in the system that matches language and country
         * name.
         */
        let mut argv: [*mut u16; 3] = [core::ptr::null_mut(); 3];

        argv[0] = wc_locale_name.as_mut_ptr();
        argv[1] = buffer.as_mut_ptr();
        argv[2] = &mut ret_val as *mut c_int as *mut u16;
        EnumSystemLocalesEx(
            Some(search_locale_enum),
            LOCALE_WINDOWS,
            argv.as_mut_ptr() as isize,
            core::ptr::null_mut(),
        );
    }

    if ret_val != 0 {
        let rc: usize;
        let hyphen: *mut c_char;

        /* Locale names use only ASCII, any conversion locale suffices. */
        rc = wchar2char(
            iso_lc_messages.as_mut_ptr(),
            buffer.as_ptr() as *const _,
            core::mem::size_of_val(&iso_lc_messages),
            core::ptr::null_mut(),
        );
        if rc == usize::MAX || rc == core::mem::size_of_val(&iso_lc_messages) {
            return core::ptr::null_mut();
        }

        /*
         * Since the message catalogs sit on a case-insensitive filesystem, we
         * need not standardize letter case here.  So long as we do not ship
         * message catalogs for which it would matter, we also need not
         * translate the script/variant portion, e.g.  uz-Cyrl-UZ to
         * uz_UZ@cyrillic.  Simply replace the hyphen with an underscore.
         */
        hyphen = libc::strchr(iso_lc_messages.as_ptr(), '-' as c_int) as *mut c_char;
        if !hyphen.is_null() {
            *hyphen = '_' as c_char;
        }
        return iso_lc_messages.as_mut_ptr();
    }

    core::ptr::null_mut()
}

#[cfg(windows)]
unsafe fn IsoLocaleName(winlocname: *const c_char) -> *mut c_char {
    static mut iso_lc_messages: [c_char; LOCALE_NAME_MAX_LENGTH] = [0; LOCALE_NAME_MAX_LENGTH];

    if pg_strcasecmp(b"c\0".as_ptr() as *const c_char, winlocname) == 0
        || pg_strcasecmp(b"posix\0".as_ptr() as *const c_char, winlocname) == 0
    {
        libc::strcpy(iso_lc_messages.as_mut_ptr(), b"C\0".as_ptr() as *const c_char);
        return iso_lc_messages.as_mut_ptr();
    } else {
        return get_iso_localename(winlocname);
    }
}
// #endif /* WIN32 && LC_MESSAGES */

/*
 * Create a new pg_locale_t struct for the given collation oid.
 */
unsafe fn create_pg_locale(collid: Oid, context: MemoryContext) -> pg_locale_t {
    let tp: HeapTuple;
    let collform: Form_pg_collation;
    let result: pg_locale_t;
    let mut datum: Datum;
    let mut isnull: bool = false;

    tp = SearchSysCache1(COLLOID, ObjectIdGetDatum(collid));
    if !HeapTupleIsValid(tp) {
        elog!(ERROR, "cache lookup failed for collation {}", collid);
    }
    collform = GETSTRUCT(tp) as Form_pg_collation;

    if (*collform).collprovider == COLLPROVIDER_BUILTIN {
        result = create_pg_locale_builtin(collid, context);
    } else if (*collform).collprovider == COLLPROVIDER_ICU {
        result = create_pg_locale_icu(collid, context);
    } else if (*collform).collprovider == COLLPROVIDER_LIBC {
        result = create_pg_locale_libc(collid, context);
    } else {
        /* shouldn't happen */
        PGLOCALE_SUPPORT_ERROR!("create_pg_locale", (*collform).collprovider);
        unreachable!();
    }

    (*result).is_default = false;

    Assert!(
        ((*result).collate_is_c && (*result).collate.is_null())
            || (!(*result).collate_is_c && !(*result).collate.is_null())
    );

    datum = SysCacheGetAttr(
        COLLOID,
        tp,
        Anum_pg_collation_collversion,
        &raw mut isnull,
    );
    if !isnull {
        let actual_versionstr: *mut c_char;
        let collversionstr: *mut c_char;

        collversionstr = TextDatumGetCString(datum);

        if (*collform).collprovider == COLLPROVIDER_LIBC {
            datum = SysCacheGetAttrNotNull(COLLOID, tp, Anum_pg_collation_collcollate);
        } else {
            datum = SysCacheGetAttrNotNull(COLLOID, tp, Anum_pg_collation_colllocale);
        }

        actual_versionstr =
            get_collation_actual_version((*collform).collprovider, TextDatumGetCString(datum));
        if actual_versionstr.is_null() {
            /*
             * This could happen when specifying a version in CREATE COLLATION
             * but the provider does not support versioning, or manually
             * creating a mess in the catalogs.
             */
            ereport!(
                ERROR,
                errmsg!(
                    "collation \"{}\" has no actual version, but a version was recorded",
                    CStr::from_ptr(NameStr(&raw mut (*collform).collname)).to_string_lossy()
                )
            );
        }

        if strcmp(actual_versionstr, collversionstr) != 0 {
            ereport!(
                WARNING,
                errmsg!(
                    "collation \"{}\" has version mismatch",
                    CStr::from_ptr(NameStr(&raw mut (*collform).collname)).to_string_lossy()
                )
            );
            // C also: errdetail("The collation in the database was created using
            //   version %s, but the operating system provides version %s.",
            //   collversionstr, actual_versionstr)
            // C also: errhint("Rebuild all objects affected by this collation and run
            //   ALTER COLLATION %s REFRESH VERSION, or build PostgreSQL with the right
            //   library version.",
            //   quote_qualified_identifier(get_namespace_name(collform->collnamespace),
            //                              NameStr(collform->collname)))
        }
    }

    ReleaseSysCache(tp);

    result
}

/*
 * Initialize default_locale with database locale settings.
 */
pub unsafe fn init_database_collation() {
    let tup: HeapTuple;
    let dbform: Form_pg_database;
    let result: pg_locale_t;

    Assert!(default_locale.is_null());

    /* Fetch our pg_database row normally, via syscache */
    tup = SearchSysCache1(DATABASEOID, ObjectIdGetDatum(MyDatabaseId));
    if !HeapTupleIsValid(tup) {
        elog!(ERROR, "cache lookup failed for database {}", MyDatabaseId);
    }
    dbform = GETSTRUCT(tup) as Form_pg_database;

    if (*dbform).datlocprovider == COLLPROVIDER_BUILTIN {
        result = create_pg_locale_builtin(DEFAULT_COLLATION_OID, TopMemoryContext);
    } else if (*dbform).datlocprovider == COLLPROVIDER_ICU {
        result = create_pg_locale_icu(DEFAULT_COLLATION_OID, TopMemoryContext);
    } else if (*dbform).datlocprovider == COLLPROVIDER_LIBC {
        result = create_pg_locale_libc(DEFAULT_COLLATION_OID, TopMemoryContext);
    } else {
        /* shouldn't happen */
        PGLOCALE_SUPPORT_ERROR!("init_database_collation", (*dbform).datlocprovider);
        unreachable!();
    }

    (*result).is_default = true;
    ReleaseSysCache(tup);

    default_locale = result;
}

/*
 * Create a pg_locale_t from a collation OID.  Results are cached for the
 * lifetime of the backend.  Thus, do not free the result with freelocale().
 *
 * For simplicity, we always generate COLLATE + CTYPE even though we
 * might only need one of them.  Since this is called only once per session,
 * it shouldn't cost much.
 */
pub unsafe fn pg_newlocale_from_collation(collid: Oid) -> pg_locale_t {
    let cache_entry: *mut collation_cache_entry;
    let mut found: bool = false;

    if collid == DEFAULT_COLLATION_OID {
        return default_locale;
    }

    /*
     * Some callers expect C_COLLATION_OID to succeed even without catalog
     * access.
     */
    if collid == C_COLLATION_OID {
        return &raw mut c_locale;
    }

    if !OidIsValid(collid) {
        elog!(ERROR, "cache lookup failed for collation {}", collid);
    }

    AssertCouldGetRelation();

    if last_collation_cache_oid == collid {
        return last_collation_cache_locale;
    }

    if CollationCache.is_null() {
        CollationCacheContext = AllocSetContextCreate!(
            TopMemoryContext,
            b"collation cache\0".as_ptr() as *const c_char,
            ALLOCSET_DEFAULT_SIZES
        );
        CollationCache = collation_cache_create(CollationCacheContext, 16, core::ptr::null_mut());
    }

    cache_entry = collation_cache_insert(CollationCache, collid, &raw mut found);
    if !found {
        /*
         * Make sure cache entry is marked invalid, in case we fail before
         * setting things.
         */
        (*cache_entry).locale = core::ptr::null_mut();
    }

    if (*cache_entry).locale.is_null() {
        (*cache_entry).locale = create_pg_locale(collid, CollationCacheContext);
    }

    last_collation_cache_oid = collid;
    last_collation_cache_locale = (*cache_entry).locale;

    (*cache_entry).locale
}

/*
 * Get provider-specific collation version string for the given collation from
 * the operating system/library.
 */
pub unsafe fn get_collation_actual_version(
    collprovider: c_char,
    collcollate: *const c_char,
) -> *mut c_char {
    let mut collversion: *mut c_char = core::ptr::null_mut();

    if collprovider == COLLPROVIDER_BUILTIN {
        collversion = get_collation_actual_version_builtin(collcollate);
    }
    // #ifdef USE_ICU
    else if collprovider == COLLPROVIDER_ICU {
        collversion = get_collation_actual_version_icu(collcollate);
    }
    // #endif
    else if collprovider == COLLPROVIDER_LIBC {
        collversion = get_collation_actual_version_libc(collcollate);
    }

    collversion
}

pub unsafe fn pg_strlower(
    dst: *mut c_char,
    dstsize: usize,
    src: *const c_char,
    srclen: ssize_t,
    locale: pg_locale_t,
) -> usize {
    if (*locale).provider == COLLPROVIDER_BUILTIN {
        return strlower_builtin(dst, dstsize, src, srclen, locale);
    }
    // #ifdef USE_ICU
    else if (*locale).provider == COLLPROVIDER_ICU {
        return strlower_icu(dst, dstsize, src, srclen, locale);
    }
    // #endif
    else if (*locale).provider == COLLPROVIDER_LIBC {
        return strlower_libc(dst, dstsize, src, srclen, locale);
    } else {
        /* shouldn't happen */
        PGLOCALE_SUPPORT_ERROR!("pg_strlower", (*locale).provider);
    }

    0 /* keep compiler quiet */
}

pub unsafe fn pg_strtitle(
    dst: *mut c_char,
    dstsize: usize,
    src: *const c_char,
    srclen: ssize_t,
    locale: pg_locale_t,
) -> usize {
    if (*locale).provider == COLLPROVIDER_BUILTIN {
        return strtitle_builtin(dst, dstsize, src, srclen, locale);
    }
    // #ifdef USE_ICU
    else if (*locale).provider == COLLPROVIDER_ICU {
        return strtitle_icu(dst, dstsize, src, srclen, locale);
    }
    // #endif
    else if (*locale).provider == COLLPROVIDER_LIBC {
        return strtitle_libc(dst, dstsize, src, srclen, locale);
    } else {
        /* shouldn't happen */
        PGLOCALE_SUPPORT_ERROR!("pg_strtitle", (*locale).provider);
    }

    0 /* keep compiler quiet */
}

pub unsafe fn pg_strupper(
    dst: *mut c_char,
    dstsize: usize,
    src: *const c_char,
    srclen: ssize_t,
    locale: pg_locale_t,
) -> usize {
    if (*locale).provider == COLLPROVIDER_BUILTIN {
        return strupper_builtin(dst, dstsize, src, srclen, locale);
    }
    // #ifdef USE_ICU
    else if (*locale).provider == COLLPROVIDER_ICU {
        return strupper_icu(dst, dstsize, src, srclen, locale);
    }
    // #endif
    else if (*locale).provider == COLLPROVIDER_LIBC {
        return strupper_libc(dst, dstsize, src, srclen, locale);
    } else {
        /* shouldn't happen */
        PGLOCALE_SUPPORT_ERROR!("pg_strupper", (*locale).provider);
    }

    0 /* keep compiler quiet */
}

pub unsafe fn pg_strfold(
    dst: *mut c_char,
    dstsize: usize,
    src: *const c_char,
    srclen: ssize_t,
    locale: pg_locale_t,
) -> usize {
    if (*locale).provider == COLLPROVIDER_BUILTIN {
        return strfold_builtin(dst, dstsize, src, srclen, locale);
    }
    // #ifdef USE_ICU
    else if (*locale).provider == COLLPROVIDER_ICU {
        return strfold_icu(dst, dstsize, src, srclen, locale);
    }
    // #endif
    /* for libc, just use strlower */
    else if (*locale).provider == COLLPROVIDER_LIBC {
        return strlower_libc(dst, dstsize, src, srclen, locale);
    } else {
        /* shouldn't happen */
        PGLOCALE_SUPPORT_ERROR!("pg_strfold", (*locale).provider);
    }

    0 /* keep compiler quiet */
}

/*
 * pg_strcoll
 *
 * Like pg_strncoll for NUL-terminated input strings.
 */
pub unsafe fn pg_strcoll(arg1: *const c_char, arg2: *const c_char, locale: pg_locale_t) -> c_int {
    ((*(*locale).collate).strncoll.unwrap())(arg1, -1, arg2, -1, locale)
}

/*
 * pg_strncoll
 *
 * Call ucol_strcollUTF8(), ucol_strcoll(), strcoll_l() or wcscoll_l() as
 * appropriate for the given locale, platform, and database encoding. If the
 * locale is not specified, use the database collation.
 *
 * The input strings must be encoded in the database encoding. If an input
 * string is NUL-terminated, its length may be specified as -1.
 *
 * The caller is responsible for breaking ties if the collation is
 * deterministic; this maintains consistency with pg_strnxfrm(), which cannot
 * easily account for deterministic collations.
 */
pub unsafe fn pg_strncoll(
    arg1: *const c_char,
    len1: ssize_t,
    arg2: *const c_char,
    len2: ssize_t,
    locale: pg_locale_t,
) -> c_int {
    ((*(*locale).collate).strncoll.unwrap())(arg1, len1, arg2, len2, locale)
}

/*
 * Return true if the collation provider supports pg_strxfrm() and
 * pg_strnxfrm(); otherwise false.
 *
 *
 * No similar problem is known for the ICU provider.
 */
pub unsafe fn pg_strxfrm_enabled(locale: pg_locale_t) -> bool {
    /*
     * locale->collate->strnxfrm is still a required method, even if it may
     * have the wrong behavior, because the planner uses it for estimates in
     * some cases.
     */
    (*(*locale).collate).strxfrm_is_safe
}

/*
 * pg_strxfrm
 *
 * Like pg_strnxfrm for a NUL-terminated input string.
 */
pub unsafe fn pg_strxfrm(
    dest: *mut c_char,
    src: *const c_char,
    destsize: usize,
    locale: pg_locale_t,
) -> usize {
    ((*(*locale).collate).strnxfrm.unwrap())(dest, destsize, src, -1, locale)
}

/*
 * pg_strnxfrm
 *
 * Transforms 'src' to a nul-terminated string stored in 'dest' such that
 * ordinary strcmp() on transformed strings is equivalent to pg_strcoll() on
 * untransformed strings.
 *
 * The input string must be encoded in the database encoding. If the input
 * string is NUL-terminated, its length may be specified as -1. If 'destsize'
 * is zero, 'dest' may be NULL.
 *
 * Not all providers support pg_strnxfrm() safely. The caller should check
 * pg_strxfrm_enabled() first, otherwise this function may return wrong
 * results or an error.
 *
 * Returns the number of bytes needed (or more) to store the transformed
 * string, excluding the terminating nul byte. If the value returned is
 * 'destsize' or greater, the resulting contents of 'dest' are undefined.
 */
pub unsafe fn pg_strnxfrm(
    dest: *mut c_char,
    destsize: usize,
    src: *const c_char,
    srclen: ssize_t,
    locale: pg_locale_t,
) -> usize {
    ((*(*locale).collate).strnxfrm.unwrap())(dest, destsize, src, srclen, locale)
}

/*
 * Return true if the collation provider supports pg_strxfrm_prefix() and
 * pg_strnxfrm_prefix(); otherwise false.
 */
pub unsafe fn pg_strxfrm_prefix_enabled(locale: pg_locale_t) -> bool {
    (*(*locale).collate).strnxfrm_prefix.is_some()
}

/*
 * pg_strxfrm_prefix
 *
 * Like pg_strnxfrm_prefix for a NUL-terminated input string.
 */
pub unsafe fn pg_strxfrm_prefix(
    dest: *mut c_char,
    src: *const c_char,
    destsize: usize,
    locale: pg_locale_t,
) -> usize {
    ((*(*locale).collate).strnxfrm_prefix.unwrap())(dest, destsize, src, -1, locale)
}

/*
 * pg_strnxfrm_prefix
 *
 * Transforms 'src' to a byte sequence stored in 'dest' such that ordinary
 * memcmp() on the byte sequence is equivalent to pg_strncoll() on
 * untransformed strings. The result is not nul-terminated.
 *
 * The input string must be encoded in the database encoding. If the input
 * string is NUL-terminated, its length may be specified as -1.
 *
 * Not all providers support pg_strnxfrm_prefix() safely. The caller should
 * check pg_strxfrm_prefix_enabled() first, otherwise this function may return
 * wrong results or an error.
 *
 * If destsize is not large enough to hold the resulting byte sequence, stores
 * only the first destsize bytes in 'dest'. Returns the number of bytes
 * actually copied to 'dest'.
 */
pub unsafe fn pg_strnxfrm_prefix(
    dest: *mut c_char,
    destsize: usize,
    src: *const c_char,
    srclen: ssize_t,
    locale: pg_locale_t,
) -> usize {
    ((*(*locale).collate).strnxfrm_prefix.unwrap())(dest, destsize, src, srclen, locale)
}

/*
 * Return required encoding ID for the given locale, or -1 if any encoding is
 * valid for the locale.
 */
pub unsafe fn builtin_locale_encoding(locale: *const c_char) -> c_int {
    if strcmp(locale, b"C\0".as_ptr() as *const c_char) == 0 {
        return -1;
    } else if strcmp(locale, b"C.UTF-8\0".as_ptr() as *const c_char) == 0 {
        return PG_UTF8;
    } else if strcmp(locale, b"PG_UNICODE_FAST\0".as_ptr() as *const c_char) == 0 {
        return PG_UTF8;
    }

    ereport!(
        ERROR,
        errmsg!(
            "invalid locale name \"{}\" for builtin provider",
            CStr::from_ptr(locale).to_string_lossy()
        )
    );
    // C also: errcode(ERRCODE_WRONG_OBJECT_TYPE)

    0 /* keep compiler quiet */
}

/*
 * Validate the locale and encoding combination, and return the canonical form
 * of the locale name.
 */
pub unsafe fn builtin_validate_locale(encoding: c_int, locale: *const c_char) -> *const c_char {
    let mut canonical_name: *const c_char = core::ptr::null();
    let required_encoding: c_int;

    if strcmp(locale, b"C\0".as_ptr() as *const c_char) == 0 {
        canonical_name = b"C\0".as_ptr() as *const c_char;
    } else if strcmp(locale, b"C.UTF-8\0".as_ptr() as *const c_char) == 0
        || strcmp(locale, b"C.UTF8\0".as_ptr() as *const c_char) == 0
    {
        canonical_name = b"C.UTF-8\0".as_ptr() as *const c_char;
    } else if strcmp(locale, b"PG_UNICODE_FAST\0".as_ptr() as *const c_char) == 0 {
        canonical_name = b"PG_UNICODE_FAST\0".as_ptr() as *const c_char;
    }

    if canonical_name.is_null() {
        ereport!(
            ERROR,
            errmsg!(
                "invalid locale name \"{}\" for builtin provider",
                CStr::from_ptr(locale).to_string_lossy()
            )
        );
        // C also: errcode(ERRCODE_WRONG_OBJECT_TYPE)
    }

    required_encoding = builtin_locale_encoding(canonical_name);
    if required_encoding >= 0 && encoding != required_encoding {
        ereport!(
            ERROR,
            errmsg!(
                "encoding \"{}\" does not match locale \"{}\"",
                CStr::from_ptr(pg_encoding_to_char(encoding)).to_string_lossy(),
                CStr::from_ptr(locale).to_string_lossy()
            )
        );
        // C also: errcode(ERRCODE_WRONG_OBJECT_TYPE)
    }

    canonical_name
}

/*
 * Return the BCP47 language tag representation of the requested locale.
 *
 * This function should be called before passing the string to ucol_open(),
 * because conversion to a language tag also performs "level 2
 * canonicalization". In addition to producing a consistent format, level 2
 * canonicalization is able to more accurately interpret different input
 * locale string formats, such as POSIX and .NET IDs.
 */
pub unsafe fn icu_language_tag(loc_str: *const c_char, elevel: c_int) -> *mut c_char {
    // #ifdef USE_ICU
    let mut status: UErrorCode;
    let mut langtag: *mut c_char;
    let mut buflen: usize = 32; /* arbitrary starting buffer size */
    let strict: bool = true;

    /*
     * A BCP47 language tag doesn't have a clearly-defined upper limit (cf.
     * RFC5646 section 4.4). Additionally, in older ICU versions,
     * uloc_toLanguageTag() doesn't always return the ultimate length on the
     * first call, necessitating a loop.
     */
    langtag = palloc(buflen) as *mut c_char;
    loop {
        status = U_ZERO_ERROR;
        uloc_toLanguageTag(loc_str, langtag, buflen as c_int, strict, &raw mut status);

        /* try again if the buffer is not large enough */
        if (status == U_BUFFER_OVERFLOW_ERROR || status == U_STRING_NOT_TERMINATED_WARNING)
            && buflen < MaxAllocSize
        {
            buflen = Min(buflen * 2, MaxAllocSize);
            langtag = repalloc(langtag as *mut c_void, buflen) as *mut c_char;
            continue;
        }

        break;
    }

    if U_FAILURE(status) {
        pfree(langtag as *mut c_void);

        if elevel > 0 {
            ereport!(
                elevel,
                errmsg!(
                    "could not convert locale name \"{}\" to language tag: {}",
                    CStr::from_ptr(loc_str).to_string_lossy(),
                    CStr::from_ptr(u_errorName(status)).to_string_lossy()
                )
            );
            // C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE)
        }
        return core::ptr::null_mut();
    }

    langtag
    // #else  not USE_ICU
    //     ereport(ERROR, errmsg("ICU is not supported in this build"));
    //     return NULL;
    // #endif
}

/*
 * Perform best-effort check that the locale is a valid one.
 */
pub unsafe fn icu_validate_locale(loc_str: *const c_char) {
    // #ifdef USE_ICU
    let collator: *mut UCollator;
    let mut status: UErrorCode;
    let mut lang: [c_char; ULOC_LANG_CAPACITY] = [0; ULOC_LANG_CAPACITY];
    let mut found: bool = false;
    let mut elevel: c_int = icu_validation_level;

    /* no validation */
    if elevel < 0 {
        return;
    }

    /* downgrade to WARNING during pg_upgrade */
    if IsBinaryUpgrade && elevel > WARNING {
        elevel = WARNING;
    }

    /* validate that we can extract the language */
    status = U_ZERO_ERROR;
    uloc_getLanguage(
        loc_str,
        lang.as_mut_ptr(),
        ULOC_LANG_CAPACITY as c_int,
        &raw mut status,
    );
    if U_FAILURE(status) || status == U_STRING_NOT_TERMINATED_WARNING {
        ereport!(
            elevel,
            errmsg!(
                "could not get language from ICU locale \"{}\": {}",
                CStr::from_ptr(loc_str).to_string_lossy(),
                CStr::from_ptr(u_errorName(status)).to_string_lossy()
            )
        );
        // C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE)
        // C also: errhint("To disable ICU locale validation, set the parameter
        //   \"%s\" to \"%s\".", "icu_validation_level", "disabled")
        return;
    }

    /* check for special language name */
    if strcmp(lang.as_ptr(), b"\0".as_ptr() as *const c_char) == 0
        || strcmp(lang.as_ptr(), b"root\0".as_ptr() as *const c_char) == 0
        || strcmp(lang.as_ptr(), b"und\0".as_ptr() as *const c_char) == 0
    {
        found = true;
    }

    /* search for matching language within ICU */
    let mut i: i32 = 0;
    while !found && i < uloc_countAvailable() {
        let otherloc: *const c_char = uloc_getAvailable(i);
        let mut otherlang: [c_char; ULOC_LANG_CAPACITY] = [0; ULOC_LANG_CAPACITY];

        status = U_ZERO_ERROR;
        uloc_getLanguage(
            otherloc,
            otherlang.as_mut_ptr(),
            ULOC_LANG_CAPACITY as c_int,
            &raw mut status,
        );
        if U_FAILURE(status) || status == U_STRING_NOT_TERMINATED_WARNING {
            i += 1;
            continue;
        }

        if strcmp(lang.as_ptr(), otherlang.as_ptr()) == 0 {
            found = true;
        }
        i += 1;
    }

    if !found {
        ereport!(
            elevel,
            errmsg!(
                "ICU locale \"{}\" has unknown language \"{}\"",
                CStr::from_ptr(loc_str).to_string_lossy(),
                CStr::from_ptr(lang.as_ptr()).to_string_lossy()
            )
        );
        // C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE)
        // C also: errhint("To disable ICU locale validation, set the parameter
        //   \"%s\" to \"%s\".", "icu_validation_level", "disabled")
    }

    /* check that it can be opened */
    collator = pg_ucol_open(loc_str);
    ucol_close(collator);
    // #else  not USE_ICU
    //     /* could get here if a collation was created by a build with ICU */
    //     ereport(ERROR, errmsg("ICU is not supported in this build"));
    // #endif
}

// ===========================================================================
// Stubs for dependencies in other .c files not yet ported.
// ===========================================================================

// --- port/pg_localeconv_r.c -------------------------------------------------
// Declared locally (not imported from port_api) because this file uses the
// faithful `lconv` layout above, whereas port_api::lconv is an opaque c_void.
unsafe fn pg_localeconv_r(
    _lc_monetary: *const c_char,
    _lc_numeric: *const c_char,
    _output: *mut lconv,
) -> c_int {
    unimplemented!() // TODO(pg-port): port/pg_localeconv_r.c
}
unsafe fn pg_localeconv_free(_output: *mut lconv) {
    unimplemented!() // TODO(pg-port): port/pg_localeconv_r.c
}

// --- lib/simplehash.h (collation_cache simplehash instantiation) ------------
// The SH_PREFIX collation_cache instantiation generates these symbols.
#[repr(C)]
pub struct collation_cache_hash {
    _private: [u8; 0],
}
unsafe fn collation_cache_create(
    _ctx: MemoryContext,
    _nelements: u32,
    _private_data: *mut c_void,
) -> *mut collation_cache_hash {
    unimplemented!() // TODO(pg-port): lib/simplehash.h SH_DEFINE
}
unsafe fn collation_cache_insert(
    _tb: *mut collation_cache_hash,
    _key: Oid,
    _found: *mut bool,
) -> *mut collation_cache_entry {
    unimplemented!() // TODO(pg-port): lib/simplehash.h SH_DEFINE
}

// --- access/htup.h / utils/syscache.h ---------------------------------------
#[allow(non_camel_case_types)]
type HeapTuple = *mut c_void;

unsafe fn HeapTupleIsValid(tuple: HeapTuple) -> bool {
    !tuple.is_null()
}

unsafe fn GETSTRUCT(_tuple: HeapTuple) -> *mut c_void {
    unimplemented!() // TODO(pg-port): access/htup_details.h
}

unsafe fn SearchSysCache1(_cacheId: c_int, _key1: Datum) -> HeapTuple {
    unimplemented!() // TODO(pg-port): utils/cache/syscache.c
}
unsafe fn SysCacheGetAttr(
    _cacheId: c_int,
    _tup: HeapTuple,
    _attributeNumber: c_int,
    _isNull: *mut bool,
) -> Datum {
    unimplemented!() // TODO(pg-port): utils/cache/syscache.c
}
unsafe fn SysCacheGetAttrNotNull(_cacheId: c_int, _tup: HeapTuple, _attributeNumber: c_int) -> Datum {
    unimplemented!() // TODO(pg-port): utils/cache/syscache.c
}
unsafe fn ReleaseSysCache(_tuple: HeapTuple) {
    unimplemented!() // TODO(pg-port): utils/cache/catcache.c
}

// utils/syscache.h cache ids.
const COLLOID: c_int = 0;
const DATABASEOID: c_int = 0;

// --- catalog attribute numbers (pg_collation.h / pg_database.h) -------------
const Anum_pg_collation_collversion: c_int = 10;
const Anum_pg_collation_collcollate: c_int = 8;
const Anum_pg_collation_colllocale: c_int = 8;

// --- catalog/pg_collation.h: Form_pg_collation ------------------------------
#[repr(C)]
pub struct FormData_pg_collation {
    pub collname: [c_char; 64], // NameData
    pub collnamespace: Oid,
    pub collprovider: c_char,
}
#[allow(non_camel_case_types)]
type Form_pg_collation = *mut FormData_pg_collation;

// --- catalog/pg_database.h: Form_pg_database --------------------------------
#[repr(C)]
pub struct FormData_pg_database {
    pub datlocprovider: c_char,
}
#[allow(non_camel_case_types)]
type Form_pg_database = *mut FormData_pg_database;

// --- postgres.h / c.h helpers -----------------------------------------------
unsafe fn ObjectIdGetDatum(oid: Oid) -> Datum {
    oid as Datum
}

unsafe fn TextDatumGetCString(_d: Datum) -> *mut c_char {
    unimplemented!() // TODO(pg-port): utils/adt/varlena.c (text_to_cstring)
}

// c.h: NameStr(name) -> &(name).data[0]; here name is the NameData array field.
unsafe fn NameStr(name: *mut [c_char; 64]) -> *const c_char {
    (*name).as_ptr()
}

// utils/rel.h: AssertCouldGetRelation() expands to an Assert; no-op here.
unsafe fn AssertCouldGetRelation() {
    // TODO(pg-port): utils/rel.h AssertCouldGetRelation()
}

// --- pg_locale_builtin.c: case-conversion entry points ----------------------
unsafe fn strlower_builtin(
    _dst: *mut c_char,
    _dstsize: usize,
    _src: *const c_char,
    _srclen: ssize_t,
    _locale: pg_locale_t,
) -> usize {
    unimplemented!() // TODO(pg-port): utils/adt/pg_locale_builtin.c
}
unsafe fn strtitle_builtin(
    _dst: *mut c_char,
    _dstsize: usize,
    _src: *const c_char,
    _srclen: ssize_t,
    _locale: pg_locale_t,
) -> usize {
    unimplemented!() // TODO(pg-port): utils/adt/pg_locale_builtin.c
}
unsafe fn strupper_builtin(
    _dst: *mut c_char,
    _dstsize: usize,
    _src: *const c_char,
    _srclen: ssize_t,
    _locale: pg_locale_t,
) -> usize {
    unimplemented!() // TODO(pg-port): utils/adt/pg_locale_builtin.c
}
unsafe fn strfold_builtin(
    _dst: *mut c_char,
    _dstsize: usize,
    _src: *const c_char,
    _srclen: ssize_t,
    _locale: pg_locale_t,
) -> usize {
    unimplemented!() // TODO(pg-port): utils/adt/pg_locale_builtin.c
}

// --- pg_locale_libc.c: case-conversion entry points -------------------------
unsafe fn strlower_libc(
    _dst: *mut c_char,
    _dstsize: usize,
    _src: *const c_char,
    _srclen: ssize_t,
    _locale: pg_locale_t,
) -> usize {
    unimplemented!() // TODO(pg-port): utils/adt/pg_locale_libc.c
}
unsafe fn strtitle_libc(
    _dst: *mut c_char,
    _dstsize: usize,
    _src: *const c_char,
    _srclen: ssize_t,
    _locale: pg_locale_t,
) -> usize {
    unimplemented!() // TODO(pg-port): utils/adt/pg_locale_libc.c
}
unsafe fn strupper_libc(
    _dst: *mut c_char,
    _dstsize: usize,
    _src: *const c_char,
    _srclen: ssize_t,
    _locale: pg_locale_t,
) -> usize {
    unimplemented!() // TODO(pg-port): utils/adt/pg_locale_libc.c
}

// --- pg_locale_icu.c: ICU provider entry points (only reached under USE_ICU)-
unsafe fn create_pg_locale_icu(_collid: Oid, _context: MemoryContext) -> pg_locale_t {
    unimplemented!() // TODO(pg-port): utils/adt/pg_locale_icu.c
}
unsafe fn get_collation_actual_version_icu(_collcollate: *const c_char) -> *mut c_char {
    unimplemented!() // TODO(pg-port): utils/adt/pg_locale_icu.c
}
unsafe fn strlower_icu(
    _dst: *mut c_char,
    _dstsize: usize,
    _src: *const c_char,
    _srclen: ssize_t,
    _locale: pg_locale_t,
) -> usize {
    unimplemented!() // TODO(pg-port): utils/adt/pg_locale_icu.c
}
unsafe fn strtitle_icu(
    _dst: *mut c_char,
    _dstsize: usize,
    _src: *const c_char,
    _srclen: ssize_t,
    _locale: pg_locale_t,
) -> usize {
    unimplemented!() // TODO(pg-port): utils/adt/pg_locale_icu.c
}
unsafe fn strupper_icu(
    _dst: *mut c_char,
    _dstsize: usize,
    _src: *const c_char,
    _srclen: ssize_t,
    _locale: pg_locale_t,
) -> usize {
    unimplemented!() // TODO(pg-port): utils/adt/pg_locale_icu.c
}
unsafe fn strfold_icu(
    _dst: *mut c_char,
    _dstsize: usize,
    _src: *const c_char,
    _srclen: ssize_t,
    _locale: pg_locale_t,
) -> usize {
    unimplemented!() // TODO(pg-port): utils/adt/pg_locale_icu.c
}
unsafe fn pg_ucol_open(_loc_str: *const c_char) -> *mut UCollator {
    unimplemented!() // TODO(pg-port): utils/adt/pg_locale_icu.c
}

// --- ICU library declarations (only reached under USE_ICU) ------------------
#[allow(non_camel_case_types)]
type UErrorCode = i32;

#[repr(C)]
pub struct UCollator {
    _private: [u8; 0],
}

const U_ZERO_ERROR: UErrorCode = 0;
const U_STRING_NOT_TERMINATED_WARNING: UErrorCode = -124;
const U_BUFFER_OVERFLOW_ERROR: UErrorCode = 15;

// ULOC_LANG_CAPACITY (unicode/uloc.h).
const ULOC_LANG_CAPACITY: usize = 12;

unsafe fn U_FAILURE(code: UErrorCode) -> bool {
    code > U_ZERO_ERROR
}

unsafe fn u_errorName(_code: UErrorCode) -> *const c_char {
    unimplemented!() // TODO(pg-port): ICU unicode/utypes.h
}
unsafe fn uloc_toLanguageTag(
    _localeID: *const c_char,
    _langtag: *mut c_char,
    _langtagCapacity: c_int,
    _strict: bool,
    _err: *mut UErrorCode,
) -> c_int {
    unimplemented!() // TODO(pg-port): ICU unicode/uloc.h
}
unsafe fn uloc_getLanguage(
    _localeID: *const c_char,
    _language: *mut c_char,
    _languageCapacity: c_int,
    _err: *mut UErrorCode,
) -> c_int {
    unimplemented!() // TODO(pg-port): ICU unicode/uloc.h
}
unsafe fn uloc_countAvailable() -> i32 {
    unimplemented!() // TODO(pg-port): ICU unicode/uloc.h
}
unsafe fn uloc_getAvailable(_n: i32) -> *const c_char {
    unimplemented!() // TODO(pg-port): ICU unicode/uloc.h
}
unsafe fn ucol_close(_coll: *mut UCollator) {
    unimplemented!() // TODO(pg-port): ICU unicode/ucol.h
}
