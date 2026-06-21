//! regex/regc_pg_locale.c
//!   ctype functions adapted to work on pg_wchar (a/k/a chr),
//!   and functions to cache the results of wholesale ctype probing.
//!
//! This file is #included by regcomp.c; it's not meant to compile standalone.
//! In the Rust port it is a standalone module exposing pub fns.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!   src/backend/regex/regc_pg_locale.c

#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]

use crate::postgres_ext::Oid;
use crate::c::OidIsValid;
use crate::catalog::pg_known_oids::C_COLLATION_OID;
use crate::catalog::pg_collation::{COLLPROVIDER_BUILTIN, COLLPROVIDER_ICU, COLLPROVIDER_LIBC};
use crate::regex::regcustom::{chr, MAX_SIMPLE_CHR};
use crate::regex::regguts::cvec;
use crate::common::unicode_case::{unicode_lowercase_simple, unicode_uppercase_simple};
use crate::common::unicode_category::{
    pg_u_isalnum, pg_u_isalpha, pg_u_isdigit, pg_u_isgraph, pg_u_islower, pg_u_isprint,
    pg_u_ispunct, pg_u_isspace, pg_u_isupper,
};
use crate::mb::wchar::pg_wchar;
use crate::Assert;
use core::ffi::{c_char, c_int, c_void};

// ---------------------------------------------------------------------------
// Local ereport/errmsg shims (errcode/errdetail/errhint folded as comments).
// ---------------------------------------------------------------------------

pub const ERROR: c_int = 21;

macro_rules! ereport {
    ($level:expr, $msg:expr) => {{
        eprintln!("[ereport level={}] {}", $level, $msg);
        if $level >= ERROR {
            panic!("ereport ERROR");
        }
    }};
}

macro_rules! errmsg {
    ($($arg:tt)*) => { format!($($arg)*) };
}

// ---------------------------------------------------------------------------
// <limits.h> UCHAR_MAX (unsigned char is 8-bit on all supported platforms).
// ---------------------------------------------------------------------------
const UCHAR_MAX: pg_wchar = 255;

// PG_UTF8 encoding id (mb/pg_wchar.h). Value matches the pg_enc ordinal.
const PG_UTF8: c_int = crate::mb::wchar::pg_enc::PG_UTF8 as c_int;

// ---------------------------------------------------------------------------
// Dependencies translated in OTHER .c files; stubbed with TODO(pg-port).
// ---------------------------------------------------------------------------

pub use crate::utils::adt::pg_locale::{pg_locale_struct, pg_locale_t};

pub use crate::utils::adt::pg_locale::pg_newlocale_from_collation;

/// mb/pg_wchar.h: GetDatabaseEncoding().
/// TODO(pg-port): real implementation lives in mb/mbutils.c.
pub unsafe fn GetDatabaseEncoding() -> c_int {
    /* TODO(pg-port) */
    PG_UTF8
}

/// utils/adt/ascii.c: pg_ascii_toupper().
/// TODO(pg-port): real implementation lives in utils/adt/ascii.c.
pub fn pg_ascii_toupper(ch: u8) -> pg_wchar {
    /* TODO(pg-port) */
    if (b'a'..=b'z').contains(&ch) {
        (ch - b'a' + b'A') as pg_wchar
    } else {
        ch as pg_wchar
    }
}

/// utils/adt/ascii.c: pg_ascii_tolower().
/// TODO(pg-port): real implementation lives in utils/adt/ascii.c.
pub fn pg_ascii_tolower(ch: u8) -> pg_wchar {
    /* TODO(pg-port) */
    if (b'A'..=b'Z').contains(&ch) {
        (ch - b'A' + b'a') as pg_wchar
    } else {
        ch as pg_wchar
    }
}

// <wctype.h> / <ctype.h> locale_t-aware ctype functions (platform libc).
unsafe extern "C" {
    fn iswdigit_l(c: c_int, l: *mut c_void) -> c_int;
    fn iswalpha_l(c: c_int, l: *mut c_void) -> c_int;
    fn iswalnum_l(c: c_int, l: *mut c_void) -> c_int;
    fn iswupper_l(c: c_int, l: *mut c_void) -> c_int;
    fn iswlower_l(c: c_int, l: *mut c_void) -> c_int;
    fn iswgraph_l(c: c_int, l: *mut c_void) -> c_int;
    fn iswprint_l(c: c_int, l: *mut c_void) -> c_int;
    fn iswpunct_l(c: c_int, l: *mut c_void) -> c_int;
    fn iswspace_l(c: c_int, l: *mut c_void) -> c_int;
    fn towupper_l(c: c_int, l: *mut c_void) -> c_int;
    fn towlower_l(c: c_int, l: *mut c_void) -> c_int;
    fn isdigit_l(c: c_int, l: *mut c_void) -> c_int;
    fn isalpha_l(c: c_int, l: *mut c_void) -> c_int;
    fn isalnum_l(c: c_int, l: *mut c_void) -> c_int;
    fn isupper_l(c: c_int, l: *mut c_void) -> c_int;
    fn islower_l(c: c_int, l: *mut c_void) -> c_int;
    fn isgraph_l(c: c_int, l: *mut c_void) -> c_int;
    fn isprint_l(c: c_int, l: *mut c_void) -> c_int;
    fn ispunct_l(c: c_int, l: *mut c_void) -> c_int;
    fn isspace_l(c: c_int, l: *mut c_void) -> c_int;
    fn toupper_l(c: c_int, l: *mut c_void) -> c_int;
    fn tolower_l(c: c_int, l: *mut c_void) -> c_int;
}

/// sizeof(wchar_t) on Darwin/Linux is 4.
const WCHAR_T_SIZE: usize = 4;

// ---------------------------------------------------------------------------
// strategy state
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(C)]
enum PG_Locale_Strategy {
    PG_REGEX_STRATEGY_C,         /* C locale (encoding independent) */
    PG_REGEX_STRATEGY_BUILTIN,   /* built-in Unicode semantics */
    PG_REGEX_STRATEGY_LIBC_WIDE, /* Use locale_t <wctype.h> functions */
    PG_REGEX_STRATEGY_LIBC_1BYTE,/* Use locale_t <ctype.h> functions */
    PG_REGEX_STRATEGY_ICU,       /* Use ICU uchar.h functions */
}

use PG_Locale_Strategy::*;

static mut pg_regex_strategy: PG_Locale_Strategy = PG_REGEX_STRATEGY_C;
static mut pg_regex_locale: pg_locale_t = core::ptr::null_mut();

/*
 * Hard-wired character properties for C locale
 */
const PG_ISDIGIT: u8 = 0x01;
const PG_ISALPHA: u8 = 0x02;
const PG_ISALNUM: u8 = PG_ISDIGIT | PG_ISALPHA;
const PG_ISUPPER: u8 = 0x04;
const PG_ISLOWER: u8 = 0x08;
const PG_ISGRAPH: u8 = 0x10;
const PG_ISPRINT: u8 = 0x20;
const PG_ISPUNCT: u8 = 0x40;
const PG_ISSPACE: u8 = 0x80;

const PG_ISGP: u8 = PG_ISGRAPH | PG_ISPRINT | PG_ISPUNCT;
const PG_ISDGP: u8 = PG_ISDIGIT | PG_ISGRAPH | PG_ISPRINT;
const PG_ISUGP: u8 = PG_ISALPHA | PG_ISUPPER | PG_ISGRAPH | PG_ISPRINT;
const PG_ISLGP: u8 = PG_ISALPHA | PG_ISLOWER | PG_ISGRAPH | PG_ISPRINT;

static pg_char_properties: [u8; 128] = [
    /* NUL */ 0, /* ^A */ 0, /* ^B */ 0, /* ^C */ 0, /* ^D */ 0, /* ^E */ 0,
    /* ^F */ 0, /* ^G */ 0, /* ^H */ 0,
    /* ^I */ PG_ISSPACE, /* ^J */ PG_ISSPACE, /* ^K */ PG_ISSPACE,
    /* ^L */ PG_ISSPACE, /* ^M */ PG_ISSPACE,
    /* ^N */ 0, /* ^O */ 0, /* ^P */ 0, /* ^Q */ 0, /* ^R */ 0, /* ^S */ 0,
    /* ^T */ 0, /* ^U */ 0, /* ^V */ 0, /* ^W */ 0, /* ^X */ 0, /* ^Y */ 0,
    /* ^Z */ 0, /* ^[ */ 0, /* ^\ */ 0, /* ^] */ 0, /* ^^ */ 0, /* ^_ */ 0,
    /*   */ PG_ISPRINT | PG_ISSPACE,
    /* ! */ PG_ISGP, /* " */ PG_ISGP, /* # */ PG_ISGP, /* $ */ PG_ISGP,
    /* % */ PG_ISGP, /* & */ PG_ISGP, /* ' */ PG_ISGP, /* ( */ PG_ISGP,
    /* ) */ PG_ISGP, /* * */ PG_ISGP, /* + */ PG_ISGP, /* , */ PG_ISGP,
    /* - */ PG_ISGP, /* . */ PG_ISGP, /* / */ PG_ISGP,
    /* 0 */ PG_ISDGP, /* 1 */ PG_ISDGP, /* 2 */ PG_ISDGP, /* 3 */ PG_ISDGP,
    /* 4 */ PG_ISDGP, /* 5 */ PG_ISDGP, /* 6 */ PG_ISDGP, /* 7 */ PG_ISDGP,
    /* 8 */ PG_ISDGP, /* 9 */ PG_ISDGP,
    /* : */ PG_ISGP, /* ; */ PG_ISGP, /* < */ PG_ISGP, /* = */ PG_ISGP,
    /* > */ PG_ISGP, /* ? */ PG_ISGP, /* @ */ PG_ISGP,
    /* A */ PG_ISUGP, /* B */ PG_ISUGP, /* C */ PG_ISUGP, /* D */ PG_ISUGP,
    /* E */ PG_ISUGP, /* F */ PG_ISUGP, /* G */ PG_ISUGP, /* H */ PG_ISUGP,
    /* I */ PG_ISUGP, /* J */ PG_ISUGP, /* K */ PG_ISUGP, /* L */ PG_ISUGP,
    /* M */ PG_ISUGP, /* N */ PG_ISUGP, /* O */ PG_ISUGP, /* P */ PG_ISUGP,
    /* Q */ PG_ISUGP, /* R */ PG_ISUGP, /* S */ PG_ISUGP, /* T */ PG_ISUGP,
    /* U */ PG_ISUGP, /* V */ PG_ISUGP, /* W */ PG_ISUGP, /* X */ PG_ISUGP,
    /* Y */ PG_ISUGP, /* Z */ PG_ISUGP,
    /* [ */ PG_ISGP, /* \ */ PG_ISGP, /* ] */ PG_ISGP, /* ^ */ PG_ISGP,
    /* _ */ PG_ISGP, /* ` */ PG_ISGP,
    /* a */ PG_ISLGP, /* b */ PG_ISLGP, /* c */ PG_ISLGP, /* d */ PG_ISLGP,
    /* e */ PG_ISLGP, /* f */ PG_ISLGP, /* g */ PG_ISLGP, /* h */ PG_ISLGP,
    /* i */ PG_ISLGP, /* j */ PG_ISLGP, /* k */ PG_ISLGP, /* l */ PG_ISLGP,
    /* m */ PG_ISLGP, /* n */ PG_ISLGP, /* o */ PG_ISLGP, /* p */ PG_ISLGP,
    /* q */ PG_ISLGP, /* r */ PG_ISLGP, /* s */ PG_ISLGP, /* t */ PG_ISLGP,
    /* u */ PG_ISLGP, /* v */ PG_ISLGP, /* w */ PG_ISLGP, /* x */ PG_ISLGP,
    /* y */ PG_ISLGP, /* z */ PG_ISLGP,
    /* { */ PG_ISGP, /* | */ PG_ISGP, /* } */ PG_ISGP, /* ~ */ PG_ISGP,
    /* DEL */ 0,
];

/*
 * pg_set_regex_collation: set collation for these functions to obey
 *
 * This is called when beginning compilation or execution of a regexp.
 * Since there's no need for reentrancy of regexp operations, it's okay
 * to store the results in static variables.
 */
pub unsafe fn pg_set_regex_collation(collation: Oid) {
    let mut locale: pg_locale_t = core::ptr::null_mut();
    let strategy: PG_Locale_Strategy;

    if !OidIsValid(collation) {
        /*
         * This typically means that the parser could not resolve a conflict
         * of implicit collations, so report it that way.
         */
        ereport!(
            ERROR,
            errmsg!("could not determine which collation to use for regular expression")
            /* C also: errcode(ERRCODE_INDETERMINATE_COLLATION),
             * errhint("Use the COLLATE clause to set the collation explicitly.") */
        );
    }

    if collation == C_COLLATION_OID {
        /*
         * Some callers expect regexes to work for C_COLLATION_OID before
         * catalog access is available, so we can't call
         * pg_newlocale_from_collation().
         */
        strategy = PG_REGEX_STRATEGY_C;
        locale = core::ptr::null_mut();
    } else {
        locale = pg_newlocale_from_collation(collation);

        if !(*locale).deterministic {
            ereport!(
                ERROR,
                errmsg!("nondeterministic collations are not supported for regular expressions")
                /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
            );
        }

        if (*locale).ctype_is_c {
            /*
             * C/POSIX collations use this path regardless of database
             * encoding
             */
            strategy = PG_REGEX_STRATEGY_C;
            locale = core::ptr::null_mut();
        } else if (*locale).provider == COLLPROVIDER_BUILTIN {
            Assert!(GetDatabaseEncoding() == PG_UTF8);
            strategy = PG_REGEX_STRATEGY_BUILTIN;
        } else if (*locale).provider == COLLPROVIDER_ICU {
            // #ifdef USE_ICU
            strategy = PG_REGEX_STRATEGY_ICU;
        } else {
            Assert!((*locale).provider == COLLPROVIDER_LIBC);
            if GetDatabaseEncoding() == PG_UTF8 {
                strategy = PG_REGEX_STRATEGY_LIBC_WIDE;
            } else {
                strategy = PG_REGEX_STRATEGY_LIBC_1BYTE;
            }
        }
    }

    pg_regex_strategy = strategy;
    pg_regex_locale = locale;
}

pub unsafe fn pg_wc_isdigit(c: pg_wchar) -> c_int {
    match pg_regex_strategy {
        PG_REGEX_STRATEGY_C => {
            (c <= 127 && (pg_char_properties[c as usize] & PG_ISDIGIT) != 0) as c_int
        }
        PG_REGEX_STRATEGY_BUILTIN => {
            pg_u_isdigit(c, !(*pg_regex_locale).info.builtin.casemap_full) as c_int
        }
        PG_REGEX_STRATEGY_LIBC_WIDE => {
            if WCHAR_T_SIZE >= 4 || c <= 0xFFFF {
                return iswdigit_l(c as c_int, (*pg_regex_locale).info.lt);
            }
            /* FALL THRU */
            (c <= UCHAR_MAX && isdigit_l(c as u8 as c_int, (*pg_regex_locale).info.lt) != 0) as c_int
        }
        PG_REGEX_STRATEGY_LIBC_1BYTE => {
            (c <= UCHAR_MAX && isdigit_l(c as u8 as c_int, (*pg_regex_locale).info.lt) != 0) as c_int
        }
        PG_REGEX_STRATEGY_ICU => {
            // #ifdef USE_ICU return u_isdigit(c);
            0
        }
    }
}

pub unsafe fn pg_wc_isalpha(c: pg_wchar) -> c_int {
    match pg_regex_strategy {
        PG_REGEX_STRATEGY_C => {
            (c <= 127 && (pg_char_properties[c as usize] & PG_ISALPHA) != 0) as c_int
        }
        PG_REGEX_STRATEGY_BUILTIN => pg_u_isalpha(c) as c_int,
        PG_REGEX_STRATEGY_LIBC_WIDE => {
            if WCHAR_T_SIZE >= 4 || c <= 0xFFFF {
                return iswalpha_l(c as c_int, (*pg_regex_locale).info.lt);
            }
            /* FALL THRU */
            (c <= UCHAR_MAX && isalpha_l(c as u8 as c_int, (*pg_regex_locale).info.lt) != 0) as c_int
        }
        PG_REGEX_STRATEGY_LIBC_1BYTE => {
            (c <= UCHAR_MAX && isalpha_l(c as u8 as c_int, (*pg_regex_locale).info.lt) != 0) as c_int
        }
        PG_REGEX_STRATEGY_ICU => {
            // #ifdef USE_ICU return u_isalpha(c);
            0
        }
    }
}

pub unsafe fn pg_wc_isalnum(c: pg_wchar) -> c_int {
    match pg_regex_strategy {
        PG_REGEX_STRATEGY_C => {
            (c <= 127 && (pg_char_properties[c as usize] & PG_ISALNUM) != 0) as c_int
        }
        PG_REGEX_STRATEGY_BUILTIN => {
            pg_u_isalnum(c, !(*pg_regex_locale).info.builtin.casemap_full) as c_int
        }
        PG_REGEX_STRATEGY_LIBC_WIDE => {
            if WCHAR_T_SIZE >= 4 || c <= 0xFFFF {
                return iswalnum_l(c as c_int, (*pg_regex_locale).info.lt);
            }
            /* FALL THRU */
            (c <= UCHAR_MAX && isalnum_l(c as u8 as c_int, (*pg_regex_locale).info.lt) != 0) as c_int
        }
        PG_REGEX_STRATEGY_LIBC_1BYTE => {
            (c <= UCHAR_MAX && isalnum_l(c as u8 as c_int, (*pg_regex_locale).info.lt) != 0) as c_int
        }
        PG_REGEX_STRATEGY_ICU => {
            // #ifdef USE_ICU return u_isalnum(c);
            0
        }
    }
}

unsafe fn pg_wc_isword(c: pg_wchar) -> c_int {
    /* We define word characters as alnum class plus underscore */
    if c == crate::regex::regcustom::CHR(b'_' as c_int) {
        return 1;
    }
    pg_wc_isalnum(c)
}

unsafe fn pg_wc_isupper(c: pg_wchar) -> c_int {
    match pg_regex_strategy {
        PG_REGEX_STRATEGY_C => {
            (c <= 127 && (pg_char_properties[c as usize] & PG_ISUPPER) != 0) as c_int
        }
        PG_REGEX_STRATEGY_BUILTIN => pg_u_isupper(c) as c_int,
        PG_REGEX_STRATEGY_LIBC_WIDE => {
            if WCHAR_T_SIZE >= 4 || c <= 0xFFFF {
                return iswupper_l(c as c_int, (*pg_regex_locale).info.lt);
            }
            /* FALL THRU */
            (c <= UCHAR_MAX && isupper_l(c as u8 as c_int, (*pg_regex_locale).info.lt) != 0) as c_int
        }
        PG_REGEX_STRATEGY_LIBC_1BYTE => {
            (c <= UCHAR_MAX && isupper_l(c as u8 as c_int, (*pg_regex_locale).info.lt) != 0) as c_int
        }
        PG_REGEX_STRATEGY_ICU => {
            // #ifdef USE_ICU return u_isupper(c);
            0
        }
    }
}

unsafe fn pg_wc_islower(c: pg_wchar) -> c_int {
    match pg_regex_strategy {
        PG_REGEX_STRATEGY_C => {
            (c <= 127 && (pg_char_properties[c as usize] & PG_ISLOWER) != 0) as c_int
        }
        PG_REGEX_STRATEGY_BUILTIN => pg_u_islower(c) as c_int,
        PG_REGEX_STRATEGY_LIBC_WIDE => {
            if WCHAR_T_SIZE >= 4 || c <= 0xFFFF {
                return iswlower_l(c as c_int, (*pg_regex_locale).info.lt);
            }
            /* FALL THRU */
            (c <= UCHAR_MAX && islower_l(c as u8 as c_int, (*pg_regex_locale).info.lt) != 0) as c_int
        }
        PG_REGEX_STRATEGY_LIBC_1BYTE => {
            (c <= UCHAR_MAX && islower_l(c as u8 as c_int, (*pg_regex_locale).info.lt) != 0) as c_int
        }
        PG_REGEX_STRATEGY_ICU => {
            // #ifdef USE_ICU return u_islower(c);
            0
        }
    }
}

unsafe fn pg_wc_isgraph(c: pg_wchar) -> c_int {
    match pg_regex_strategy {
        PG_REGEX_STRATEGY_C => {
            (c <= 127 && (pg_char_properties[c as usize] & PG_ISGRAPH) != 0) as c_int
        }
        PG_REGEX_STRATEGY_BUILTIN => pg_u_isgraph(c) as c_int,
        PG_REGEX_STRATEGY_LIBC_WIDE => {
            if WCHAR_T_SIZE >= 4 || c <= 0xFFFF {
                return iswgraph_l(c as c_int, (*pg_regex_locale).info.lt);
            }
            /* FALL THRU */
            (c <= UCHAR_MAX && isgraph_l(c as u8 as c_int, (*pg_regex_locale).info.lt) != 0) as c_int
        }
        PG_REGEX_STRATEGY_LIBC_1BYTE => {
            (c <= UCHAR_MAX && isgraph_l(c as u8 as c_int, (*pg_regex_locale).info.lt) != 0) as c_int
        }
        PG_REGEX_STRATEGY_ICU => {
            // #ifdef USE_ICU return u_isgraph(c);
            0
        }
    }
}

unsafe fn pg_wc_isprint(c: pg_wchar) -> c_int {
    match pg_regex_strategy {
        PG_REGEX_STRATEGY_C => {
            (c <= 127 && (pg_char_properties[c as usize] & PG_ISPRINT) != 0) as c_int
        }
        PG_REGEX_STRATEGY_BUILTIN => pg_u_isprint(c) as c_int,
        PG_REGEX_STRATEGY_LIBC_WIDE => {
            if WCHAR_T_SIZE >= 4 || c <= 0xFFFF {
                return iswprint_l(c as c_int, (*pg_regex_locale).info.lt);
            }
            /* FALL THRU */
            (c <= UCHAR_MAX && isprint_l(c as u8 as c_int, (*pg_regex_locale).info.lt) != 0) as c_int
        }
        PG_REGEX_STRATEGY_LIBC_1BYTE => {
            (c <= UCHAR_MAX && isprint_l(c as u8 as c_int, (*pg_regex_locale).info.lt) != 0) as c_int
        }
        PG_REGEX_STRATEGY_ICU => {
            // #ifdef USE_ICU return u_isprint(c);
            0
        }
    }
}

unsafe fn pg_wc_ispunct(c: pg_wchar) -> c_int {
    match pg_regex_strategy {
        PG_REGEX_STRATEGY_C => {
            (c <= 127 && (pg_char_properties[c as usize] & PG_ISPUNCT) != 0) as c_int
        }
        PG_REGEX_STRATEGY_BUILTIN => {
            pg_u_ispunct(c, !(*pg_regex_locale).info.builtin.casemap_full) as c_int
        }
        PG_REGEX_STRATEGY_LIBC_WIDE => {
            if WCHAR_T_SIZE >= 4 || c <= 0xFFFF {
                return iswpunct_l(c as c_int, (*pg_regex_locale).info.lt);
            }
            /* FALL THRU */
            (c <= UCHAR_MAX && ispunct_l(c as u8 as c_int, (*pg_regex_locale).info.lt) != 0) as c_int
        }
        PG_REGEX_STRATEGY_LIBC_1BYTE => {
            (c <= UCHAR_MAX && ispunct_l(c as u8 as c_int, (*pg_regex_locale).info.lt) != 0) as c_int
        }
        PG_REGEX_STRATEGY_ICU => {
            // #ifdef USE_ICU return u_ispunct(c);
            0
        }
    }
}

pub unsafe fn pg_wc_isspace(c: pg_wchar) -> c_int {
    match pg_regex_strategy {
        PG_REGEX_STRATEGY_C => {
            (c <= 127 && (pg_char_properties[c as usize] & PG_ISSPACE) != 0) as c_int
        }
        PG_REGEX_STRATEGY_BUILTIN => pg_u_isspace(c) as c_int,
        PG_REGEX_STRATEGY_LIBC_WIDE => {
            if WCHAR_T_SIZE >= 4 || c <= 0xFFFF {
                return iswspace_l(c as c_int, (*pg_regex_locale).info.lt);
            }
            /* FALL THRU */
            (c <= UCHAR_MAX && isspace_l(c as u8 as c_int, (*pg_regex_locale).info.lt) != 0) as c_int
        }
        PG_REGEX_STRATEGY_LIBC_1BYTE => {
            (c <= UCHAR_MAX && isspace_l(c as u8 as c_int, (*pg_regex_locale).info.lt) != 0) as c_int
        }
        PG_REGEX_STRATEGY_ICU => {
            // #ifdef USE_ICU return u_isspace(c);
            0
        }
    }
}

unsafe fn pg_wc_toupper(c: pg_wchar) -> pg_wchar {
    match pg_regex_strategy {
        PG_REGEX_STRATEGY_C => {
            if c <= 127 {
                return pg_ascii_toupper(c as u8);
            }
            c
        }
        PG_REGEX_STRATEGY_BUILTIN => unicode_uppercase_simple(c),
        PG_REGEX_STRATEGY_LIBC_WIDE => {
            /* force C behavior for ASCII characters, per comments above */
            if (*pg_regex_locale).is_default && c <= 127 {
                return pg_ascii_toupper(c as u8);
            }
            if WCHAR_T_SIZE >= 4 || c <= 0xFFFF {
                return towupper_l(c as c_int, (*pg_regex_locale).info.lt) as pg_wchar;
            }
            /* FALL THRU */
            if (*pg_regex_locale).is_default && c <= 127 {
                return pg_ascii_toupper(c as u8);
            }
            if c <= UCHAR_MAX {
                return toupper_l(c as u8 as c_int, (*pg_regex_locale).info.lt) as pg_wchar;
            }
            c
        }
        PG_REGEX_STRATEGY_LIBC_1BYTE => {
            /* force C behavior for ASCII characters, per comments above */
            if (*pg_regex_locale).is_default && c <= 127 {
                return pg_ascii_toupper(c as u8);
            }
            if c <= UCHAR_MAX {
                return toupper_l(c as u8 as c_int, (*pg_regex_locale).info.lt) as pg_wchar;
            }
            c
        }
        PG_REGEX_STRATEGY_ICU => {
            // #ifdef USE_ICU return u_toupper(c);
            0
        }
    }
}

unsafe fn pg_wc_tolower(c: pg_wchar) -> pg_wchar {
    match pg_regex_strategy {
        PG_REGEX_STRATEGY_C => {
            if c <= 127 {
                return pg_ascii_tolower(c as u8);
            }
            c
        }
        PG_REGEX_STRATEGY_BUILTIN => unicode_lowercase_simple(c),
        PG_REGEX_STRATEGY_LIBC_WIDE => {
            /* force C behavior for ASCII characters, per comments above */
            if (*pg_regex_locale).is_default && c <= 127 {
                return pg_ascii_tolower(c as u8);
            }
            if WCHAR_T_SIZE >= 4 || c <= 0xFFFF {
                return towlower_l(c as c_int, (*pg_regex_locale).info.lt) as pg_wchar;
            }
            /* FALL THRU */
            if (*pg_regex_locale).is_default && c <= 127 {
                return pg_ascii_tolower(c as u8);
            }
            if c <= UCHAR_MAX {
                return tolower_l(c as u8 as c_int, (*pg_regex_locale).info.lt) as pg_wchar;
            }
            c
        }
        PG_REGEX_STRATEGY_LIBC_1BYTE => {
            /* force C behavior for ASCII characters, per comments above */
            if (*pg_regex_locale).is_default && c <= 127 {
                return pg_ascii_tolower(c as u8);
            }
            if c <= UCHAR_MAX {
                return tolower_l(c as u8 as c_int, (*pg_regex_locale).info.lt) as pg_wchar;
            }
            c
        }
        PG_REGEX_STRATEGY_ICU => {
            // #ifdef USE_ICU return u_tolower(c);
            0
        }
    }
}

/*
 * These functions cache the results of probing libc's ctype behavior for
 * all character codes of interest in a given encoding/collation.  The
 * result is provided as a "struct cvec", but notice that the representation
 * is a touch different from a cvec created by regc_cvec.c: we allocate the
 * chrs[] and ranges[] arrays separately from the struct so that we can
 * realloc them larger at need.  This is okay since the cvecs made here
 * should never be freed by freecvec().
 *
 * We use malloc not palloc since we mustn't lose control on out-of-memory;
 * the main regex code expects us to return a failure indication instead.
 */

type pg_wc_probefunc = unsafe fn(c: pg_wchar) -> c_int;

#[repr(C)]
struct pg_ctype_cache {
    probefunc: pg_wc_probefunc,       /* pg_wc_isalpha or a sibling */
    locale: pg_locale_t,              /* locale this entry is for */
    cv: cvec,                         /* cache entry contents */
    next: *mut pg_ctype_cache,        /* chain link */
}

static mut pg_ctype_cache_list: *mut pg_ctype_cache = core::ptr::null_mut();

/*
 * Add a chr or range to pcc->cv; return false if run out of memory
 */
unsafe fn store_match(pcc: *mut pg_ctype_cache, chr1: pg_wchar, nchrs: c_int) -> bool {
    let newchrs: *mut chr;

    if nchrs > 1 {
        if (*pcc).cv.nranges >= (*pcc).cv.rangespace {
            (*pcc).cv.rangespace *= 2;
            newchrs = libc::realloc(
                (*pcc).cv.ranges as *mut c_void,
                ((*pcc).cv.rangespace as usize) * core::mem::size_of::<chr>() * 2,
            ) as *mut chr;
            if newchrs.is_null() {
                return false;
            }
            (*pcc).cv.ranges = newchrs;
        }
        *(*pcc).cv.ranges.add(((*pcc).cv.nranges * 2) as usize) = chr1;
        *(*pcc).cv.ranges.add(((*pcc).cv.nranges * 2 + 1) as usize) =
            chr1 + (nchrs - 1) as pg_wchar;
        (*pcc).cv.nranges += 1;
    } else {
        assert!(nchrs == 1);
        if (*pcc).cv.nchrs >= (*pcc).cv.chrspace {
            (*pcc).cv.chrspace *= 2;
            newchrs = libc::realloc(
                (*pcc).cv.chrs as *mut c_void,
                ((*pcc).cv.chrspace as usize) * core::mem::size_of::<chr>(),
            ) as *mut chr;
            if newchrs.is_null() {
                return false;
            }
            (*pcc).cv.chrs = newchrs;
        }
        *(*pcc).cv.chrs.add((*pcc).cv.nchrs as usize) = chr1;
        (*pcc).cv.nchrs += 1;
    }
    true
}

/*
 * Given a probe function (e.g., pg_wc_isalpha) get a struct cvec for all
 * chrs satisfying the probe function.  The active collation is the one
 * previously set by pg_set_regex_collation.  Return NULL if out of memory.
 *
 * Note that the result must not be freed or modified by caller.
 */
unsafe fn pg_ctype_get_cache(probefunc: pg_wc_probefunc, cclasscode: c_int) -> *mut cvec {
    let mut pcc: *mut pg_ctype_cache;
    let max_chr: pg_wchar;
    let mut cur_chr: pg_wchar;
    let mut nmatches: c_int;
    let newchrs: *mut chr;

    /*
     * Do we already have the answer cached?
     */
    pcc = pg_ctype_cache_list;
    while !pcc.is_null() {
        if (*pcc).probefunc as usize == probefunc as usize && (*pcc).locale == pg_regex_locale {
            return &mut (*pcc).cv;
        }
        pcc = (*pcc).next;
    }

    'success: {
        /*
         * Nope, so initialize some workspace ...
         */
        pcc = libc::malloc(core::mem::size_of::<pg_ctype_cache>()) as *mut pg_ctype_cache;
        if pcc.is_null() {
            return core::ptr::null_mut();
        }
        (*pcc).probefunc = probefunc;
        (*pcc).locale = pg_regex_locale;
        (*pcc).cv.nchrs = 0;
        (*pcc).cv.chrspace = 128;
        (*pcc).cv.chrs =
            libc::malloc((*pcc).cv.chrspace as usize * core::mem::size_of::<chr>()) as *mut chr;
        (*pcc).cv.nranges = 0;
        (*pcc).cv.rangespace = 64;
        (*pcc).cv.ranges =
            libc::malloc((*pcc).cv.rangespace as usize * core::mem::size_of::<chr>() * 2)
                as *mut chr;
        if (*pcc).cv.chrs.is_null() || (*pcc).cv.ranges.is_null() {
            break 'success; /* goto out_of_memory */
        }
        (*pcc).cv.cclasscode = cclasscode;

        /*
         * Decide how many character codes we ought to look through.  In general
         * we don't go past MAX_SIMPLE_CHR; chr codes above that are handled at
         * runtime using the "high colormap" mechanism.  However, in C locale
         * there's no need to go further than 127, and if we only have a 1-byte
         * <ctype.h> API there's no need to go further than that can handle.
         *
         * If it's not MAX_SIMPLE_CHR that's constraining the search, mark the
         * output cvec as not having any locale-dependent behavior, since there
         * will be no need to do any run-time locale checks.  (The #if's here
         * would always be true for production values of MAX_SIMPLE_CHR, but it's
         * useful to allow it to be small for testing purposes.)
         */
        match pg_regex_strategy {
            PG_REGEX_STRATEGY_C => {
                // #if MAX_SIMPLE_CHR >= 127
                max_chr = 127;
                (*pcc).cv.cclasscode = -1;
            }
            PG_REGEX_STRATEGY_BUILTIN => {
                max_chr = MAX_SIMPLE_CHR;
            }
            PG_REGEX_STRATEGY_LIBC_WIDE => {
                max_chr = MAX_SIMPLE_CHR;
            }
            PG_REGEX_STRATEGY_LIBC_1BYTE => {
                // #if MAX_SIMPLE_CHR >= UCHAR_MAX
                if MAX_SIMPLE_CHR >= UCHAR_MAX {
                    max_chr = UCHAR_MAX;
                    (*pcc).cv.cclasscode = -1;
                } else {
                    max_chr = MAX_SIMPLE_CHR;
                }
            }
            PG_REGEX_STRATEGY_ICU => {
                max_chr = MAX_SIMPLE_CHR;
            }
        }

        /*
         * And scan 'em ...
         */
        nmatches = 0; /* number of consecutive matches */

        cur_chr = 0;
        while cur_chr <= max_chr {
            if (probefunc)(cur_chr) != 0 {
                nmatches += 1;
            } else if nmatches > 0 {
                if !store_match(pcc, cur_chr - nmatches as pg_wchar, nmatches) {
                    break 'success; /* goto out_of_memory */
                }
                nmatches = 0;
            }
            cur_chr += 1;
        }

        if nmatches > 0 {
            if !store_match(pcc, cur_chr - nmatches as pg_wchar, nmatches) {
                break 'success; /* goto out_of_memory */
            }
        }

        /*
         * We might have allocated more memory than needed, if so free it
         */
        if (*pcc).cv.nchrs == 0 {
            libc::free((*pcc).cv.chrs as *mut c_void);
            (*pcc).cv.chrs = core::ptr::null_mut();
            (*pcc).cv.chrspace = 0;
        } else if (*pcc).cv.nchrs < (*pcc).cv.chrspace {
            let newchrs2 = libc::realloc(
                (*pcc).cv.chrs as *mut c_void,
                (*pcc).cv.nchrs as usize * core::mem::size_of::<chr>(),
            ) as *mut chr;
            if newchrs2.is_null() {
                break 'success; /* goto out_of_memory */
            }
            (*pcc).cv.chrs = newchrs2;
            (*pcc).cv.chrspace = (*pcc).cv.nchrs;
        }
        if (*pcc).cv.nranges == 0 {
            libc::free((*pcc).cv.ranges as *mut c_void);
            (*pcc).cv.ranges = core::ptr::null_mut();
            (*pcc).cv.rangespace = 0;
        } else if (*pcc).cv.nranges < (*pcc).cv.rangespace {
            let newchrs3 = libc::realloc(
                (*pcc).cv.ranges as *mut c_void,
                (*pcc).cv.nranges as usize * core::mem::size_of::<chr>() * 2,
            ) as *mut chr;
            if newchrs3.is_null() {
                break 'success; /* goto out_of_memory */
            }
            (*pcc).cv.ranges = newchrs3;
            (*pcc).cv.rangespace = (*pcc).cv.nranges;
        }
        let _ = newchrs;

        /*
         * Success, link it into cache chain
         */
        (*pcc).next = pg_ctype_cache_list;
        pg_ctype_cache_list = pcc;

        return &mut (*pcc).cv;
    }

    /*
     * Failure, clean up
     */
    // out_of_memory:
    libc::free((*pcc).cv.chrs as *mut c_void);
    libc::free((*pcc).cv.ranges as *mut c_void);
    libc::free(pcc as *mut c_void);

    core::ptr::null_mut()
}
