/*-------------------------------------------------------------------------
 *
 * wchar.c
 *	  Functions for working with multibyte characters in various encodings.
 *
 * Portions Copyright (c) 1998-2025, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/common/wchar.c
 *
 *-------------------------------------------------------------------------
 */

// Combined translation of:
//   HEADER: src/include/mb/pg_wchar.h  (multibyte-character support)
//   IMPL:   src/common/wchar.c         (per-encoding helper functions)
//
// pg_wchar.h
//	  multibyte-character support
//
// Porting notes:
//   - The C `#include "common/unicode_nonspacing_table.h"` and
//     `"common/unicode_east_asian_fw_table.h"` inside ucs_wcwidth() pull in giant
//     generated Unicode DATA tables. Per the port plan, ONLY those two data tables
//     are stubbed (empty), so ucs_wcwidth()/pg_utf_dsplen() still compile and run.
//     The mbbisearch() FUNCTION is translated faithfully. Marked TODO(pg-port).
//   - `is_valid_ascii()` (from utils/ascii.h) and the `Vector8` machinery (from
//     port/simd.h) are not yet ported as separate modules. We translate the
//     portable USE_NO_SIMD scalar path of is_valid_ascii() locally (Vector8 ==
//     uint64, chunk size 8 bytes) so pg_utf8_verifystr()'s fast path compiles and
//     behaves identically to the C scalar fallback. Marked TODO(pg-port).
//   - C arithmetic wraps; several shift/compose operations on bytes can overflow a
//     narrow type before widening, so we widen to uint32 / use wrapping_* where the
//     C relies on modular semantics.
//   - The big pg_wchar_table[] is an array of fn-pointer structs (NOT a giant data
//     table), so it is included in full. Client-only encodings whose
//     mb2wchar/wchar2mb are NULL in C become `None`.

#![allow(clippy::missing_safety_doc)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(dead_code)]
// The verifychar functions advance `s`/assign `c2` as the last statement of a
// branch (mirroring the C, which keeps a running cursor); those final writes are
// genuinely unused, exactly as in the C source.
#![allow(unused_assignments)]

use crate::prelude::*;

// ================================================================
//   pg_wchar.h : types, constants, structs, inline helpers
// ================================================================

/*
 * The pg_wchar type
 */
pub type pg_wchar = c_uint;

/*
 * Maximum byte length of multibyte characters in any backend encoding
 */
pub const MAX_MULTIBYTE_CHAR_LEN: c_int = 4;

/*
 * various definitions for EUC
 */
pub const SS2: u8 = 0x8e; /* single shift 2 (JIS0201) */
pub const SS3: u8 = 0x8f; /* single shift 3 (JIS0212) */

/*
 * SJIS validation macros
 */
#[inline(always)]
pub fn ISSJISHEAD(c: u8) -> bool {
    (c >= 0x81 && c <= 0x9f) || (c >= 0xe0 && c <= 0xfc)
}
#[inline(always)]
pub fn ISSJISTAIL(c: u8) -> bool {
    (c >= 0x40 && c <= 0x7e) || (c >= 0x80 && c <= 0xfc)
}

/*----------------------------------------------------
 * MULE Internal Encoding (MIC)
 *
 * (See the C header for the full description of the MULE charset families.)
 *----------------------------------------------------
 */

/*
 * Charset IDs for official single byte encodings (0x81-0x8e)
 */
pub const LC_ISO8859_1: u8 = 0x81; /* ISO8859 Latin 1 */
pub const LC_ISO8859_2: u8 = 0x82; /* ISO8859 Latin 2 */
pub const LC_ISO8859_3: u8 = 0x83; /* ISO8859 Latin 3 */
pub const LC_ISO8859_4: u8 = 0x84; /* ISO8859 Latin 4 */
pub const LC_TIS620: u8 = 0x85; /* Thai (not supported yet) */
pub const LC_ISO8859_7: u8 = 0x86; /* Greek (not supported yet) */
pub const LC_ISO8859_6: u8 = 0x87; /* Arabic (not supported yet) */
pub const LC_ISO8859_8: u8 = 0x88; /* Hebrew (not supported yet) */
pub const LC_JISX0201K: u8 = 0x89; /* Japanese 1 byte kana */
pub const LC_JISX0201R: u8 = 0x8a; /* Japanese 1 byte Roman */
/* Note that 0x8b seems to be unused as of Emacs 20.7.
 * However, there might be a chance that 0x8b could be used
 * in later versions of Emacs.
 */
pub const LC_KOI8_R: u8 = 0x8b; /* Cyrillic KOI8-R */
pub const LC_ISO8859_5: u8 = 0x8c; /* ISO8859 Cyrillic */
pub const LC_ISO8859_9: u8 = 0x8d; /* ISO8859 Latin 5 (not supported yet) */
pub const LC_ISO8859_15: u8 = 0x8e; /* ISO8859 Latin 15 (not supported yet) */
/* #define CONTROL_1		0x8f	control characters (unused) */

/* Is a leading byte for "official" single byte encodings? */
#[inline(always)]
pub fn IS_LC1(c: u8) -> bool {
    c >= 0x81 && c <= 0x8d
}

/*
 * Charset IDs for official multibyte encodings (0x90-0x99)
 * 0x9a-0x9d are free. 0x9e and 0x9f are reserved.
 */
pub const LC_JISX0208_1978: u8 = 0x90; /* Japanese Kanji, old JIS (not supported) */
pub const LC_GB2312_80: u8 = 0x91; /* Chinese */
pub const LC_JISX0208: u8 = 0x92; /* Japanese Kanji (JIS X 0208) */
pub const LC_KS5601: u8 = 0x93; /* Korean */
pub const LC_JISX0212: u8 = 0x94; /* Japanese Kanji (JIS X 0212) */
pub const LC_CNS11643_1: u8 = 0x95; /* CNS 11643-1992 Plane 1 */
pub const LC_CNS11643_2: u8 = 0x96; /* CNS 11643-1992 Plane 2 */
pub const LC_JISX0213_1: u8 = 0x97; /* Japanese Kanji (JIS X 0213 Plane 1) (not supported) */
pub const LC_BIG5_1: u8 = 0x98; /* Plane 1 Chinese traditional (not supported) */
pub const LC_BIG5_2: u8 = 0x99; /* Plane 1 Chinese traditional (not supported) */

/* Is a leading byte for "official" multibyte encodings? */
#[inline(always)]
pub fn IS_LC2(c: u8) -> bool {
    c >= 0x90 && c <= 0x99
}

/*
 * Postgres-specific prefix bytes for "private" single byte encodings
 * (According to the MULE docs, we should be using 0x9e for this)
 */
pub const LCPRV1_A: u8 = 0x9a;
pub const LCPRV1_B: u8 = 0x9b;
#[inline(always)]
pub fn IS_LCPRV1(c: u8) -> bool {
    c == LCPRV1_A || c == LCPRV1_B
}
#[inline(always)]
pub fn IS_LCPRV1_A_RANGE(c: u8) -> bool {
    c >= 0xa0 && c <= 0xdf
}
#[inline(always)]
pub fn IS_LCPRV1_B_RANGE(c: u8) -> bool {
    c >= 0xe0 && c <= 0xef
}

/*
 * Postgres-specific prefix bytes for "private" multibyte encodings
 * (According to the MULE docs, we should be using 0x9f for this)
 */
pub const LCPRV2_A: u8 = 0x9c;
pub const LCPRV2_B: u8 = 0x9d;
#[inline(always)]
pub fn IS_LCPRV2(c: u8) -> bool {
    c == LCPRV2_A || c == LCPRV2_B
}
#[inline(always)]
pub fn IS_LCPRV2_A_RANGE(c: u8) -> bool {
    c >= 0xf0 && c <= 0xf4
}
#[inline(always)]
pub fn IS_LCPRV2_B_RANGE(c: u8) -> bool {
    c >= 0xf5 && c <= 0xfe
}

/*
 * Charset IDs for private single byte encodings (0xa0-0xef)
 */
pub const LC_SISHENG: u8 = 0xa0; /* Chinese SiSheng characters for PinYin/ZhuYin (not supported) */
pub const LC_IPA: u8 = 0xa1; /* IPA (International Phonetic Association) (not supported) */
pub const LC_VISCII_LOWER: u8 = 0xa2; /* Vietnamese VISCII1.1 lower-case (not supported) */
pub const LC_VISCII_UPPER: u8 = 0xa3; /* Vietnamese VISCII1.1 upper-case (not supported) */
pub const LC_ARABIC_DIGIT: u8 = 0xa4; /* Arabic digit (not supported) */
pub const LC_ARABIC_1_COLUMN: u8 = 0xa5; /* Arabic 1-column (not supported) */
pub const LC_ASCII_RIGHT_TO_LEFT: u8 = 0xa6; /* ASCII (left half of ISO8859-1) with right-to-left direction (not supported) */
pub const LC_LAO: u8 = 0xa7; /* Lao characters (ISO10646 0E80..0EDF) (not supported) */
pub const LC_ARABIC_2_COLUMN: u8 = 0xa8; /* Arabic 1-column (not supported) */

/*
 * Charset IDs for private multibyte encodings (0xf0-0xff)
 */
pub const LC_INDIAN_1_COLUMN: u8 = 0xf0; /* Indian charset for 1-column width glyphs (not supported) */
pub const LC_TIBETAN_1_COLUMN: u8 = 0xf1; /* Tibetan 1-column width glyphs (not supported) */
pub const LC_UNICODE_SUBSET_2: u8 = 0xf2; /* Unicode characters of the range U+2500..U+33FF. (not supported) */
pub const LC_UNICODE_SUBSET_3: u8 = 0xf3; /* Unicode characters of the range U+E000..U+FFFF. (not supported) */
pub const LC_UNICODE_SUBSET: u8 = 0xf4; /* Unicode characters of the range U+0100..U+24FF. (not supported) */
pub const LC_ETHIOPIC: u8 = 0xf5; /* Ethiopic characters (not supported) */
pub const LC_CNS11643_3: u8 = 0xf6; /* CNS 11643-1992 Plane 3 */
pub const LC_CNS11643_4: u8 = 0xf7; /* CNS 11643-1992 Plane 4 */
pub const LC_CNS11643_5: u8 = 0xf8; /* CNS 11643-1992 Plane 5 */
pub const LC_CNS11643_6: u8 = 0xf9; /* CNS 11643-1992 Plane 6 */
pub const LC_CNS11643_7: u8 = 0xfa; /* CNS 11643-1992 Plane 7 */
pub const LC_INDIAN_2_COLUMN: u8 = 0xfb; /* Indian charset for 2-column width glyphs (not supported) */
pub const LC_TIBETAN: u8 = 0xfc; /* Tibetan (not supported) */
/* #define FREE				0xfd	free (unused) */
/* #define FREE				0xfe	free (unused) */
/* #define FREE				0xff	free (unused) */

/*----------------------------------------------------
 * end of MULE stuff
 *----------------------------------------------------
 */

/*
 * PostgreSQL encoding identifiers
 *
 * WARNING: If you add some encoding don't forget to update
 *			the pg_enc2name_tbl[] array (in src/common/encnames.c),
 *			the pg_enc2gettext_tbl[] array (in src/common/encnames.c) and
 *			the pg_wchar_table[] array (in src/common/wchar.c) and to check
 *			PG_ENCODING_BE_LAST macro.
 *
 * PG_SQL_ASCII is default encoding and must be = 0.
 *
 * XXX	We must avoid renumbering any backend encoding until libpq's major
 * version number is increased beyond 5; it turns out that the backend
 * encoding IDs are effectively part of libpq's ABI as far as 8.2 initdb and
 * psql are concerned.
 */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum pg_enc {
    PG_SQL_ASCII = 0,   /* SQL/ASCII */
    PG_EUC_JP,          /* EUC for Japanese */
    PG_EUC_CN,          /* EUC for Chinese */
    PG_EUC_KR,          /* EUC for Korean */
    PG_EUC_TW,          /* EUC for Taiwan */
    PG_EUC_JIS_2004,    /* EUC-JIS-2004 */
    PG_UTF8,            /* Unicode UTF8 */
    PG_MULE_INTERNAL,   /* Mule internal code */
    PG_LATIN1,          /* ISO-8859-1 Latin 1 */
    PG_LATIN2,          /* ISO-8859-2 Latin 2 */
    PG_LATIN3,          /* ISO-8859-3 Latin 3 */
    PG_LATIN4,          /* ISO-8859-4 Latin 4 */
    PG_LATIN5,          /* ISO-8859-9 Latin 5 */
    PG_LATIN6,          /* ISO-8859-10 Latin6 */
    PG_LATIN7,          /* ISO-8859-13 Latin7 */
    PG_LATIN8,          /* ISO-8859-14 Latin8 */
    PG_LATIN9,          /* ISO-8859-15 Latin9 */
    PG_LATIN10,         /* ISO-8859-16 Latin10 */
    PG_WIN1256,         /* windows-1256 */
    PG_WIN1258,         /* Windows-1258 */
    PG_WIN866,          /* (MS-DOS CP866) */
    PG_WIN874,          /* windows-874 */
    PG_KOI8R,           /* KOI8-R */
    PG_WIN1251,         /* windows-1251 */
    PG_WIN1252,         /* windows-1252 */
    PG_ISO_8859_5,      /* ISO-8859-5 */
    PG_ISO_8859_6,      /* ISO-8859-6 */
    PG_ISO_8859_7,      /* ISO-8859-7 */
    PG_ISO_8859_8,      /* ISO-8859-8 */
    PG_WIN1250,         /* windows-1250 */
    PG_WIN1253,         /* windows-1253 */
    PG_WIN1254,         /* windows-1254 */
    PG_WIN1255,         /* windows-1255 */
    PG_WIN1257,         /* windows-1257 */
    PG_KOI8U,           /* KOI8-U */
    /* PG_ENCODING_BE_LAST points to the above entry */

    /* followings are for client encoding only */
    PG_SJIS,          /* Shift JIS (Windows-932) */
    PG_BIG5,          /* Big5 (Windows-950) */
    PG_GBK,           /* GBK (Windows-936) */
    PG_UHC,           /* UHC (Windows-949) */
    PG_GB18030,       /* GB18030 */
    PG_JOHAB,         /* EUC for Korean JOHAB */
    PG_SHIFT_JIS_2004, /* Shift-JIS-2004 */
    _PG_LAST_ENCODING_, /* mark only */
}

// Re-export the enum variants at module scope so call sites can use the bare C
// names (e.g. `PG_UTF8`) exactly as the C source does.
pub use pg_enc::*;

/// The discriminant of the encoding that `PG_ENCODING_BE_LAST` names (PG_KOI8U).
/// In C this is `#define PG_ENCODING_BE_LAST PG_KOI8U`, an enum constant used in
/// integer comparisons; we materialize its integer value here.
pub const PG_ENCODING_BE_LAST: c_int = pg_enc::PG_KOI8U as c_int;

/*
 * Please use these tests before access to pg_enc2name_tbl[]
 * or to other places...
 */
#[inline(always)]
pub fn PG_VALID_BE_ENCODING(_enc: c_int) -> bool {
    _enc >= 0 && _enc <= PG_ENCODING_BE_LAST
}

#[inline(always)]
pub fn PG_ENCODING_IS_CLIENT_ONLY(_enc: c_int) -> bool {
    _enc > PG_ENCODING_BE_LAST && _enc < (pg_enc::_PG_LAST_ENCODING_ as c_int)
}

#[inline(always)]
pub fn PG_VALID_ENCODING(_enc: c_int) -> bool {
    _enc >= 0 && _enc < (pg_enc::_PG_LAST_ENCODING_ as c_int)
}

/* On FE are possible all encodings */
#[inline(always)]
pub fn PG_VALID_FE_ENCODING(_enc: c_int) -> bool {
    PG_VALID_ENCODING(_enc)
}

/*
 * When converting strings between different encodings, we assume that space
 * for converted result is 4-to-1 growth in the worst case. (See the C header
 * for the full rationale.)
 */
pub const MAX_CONVERSION_GROWTH: c_int = 4;

/*
 * Maximum byte length of a string that's required in any encoding to convert
 * at least one character to any other encoding. (See the C header.)
 */
pub const MAX_CONVERSION_INPUT_LENGTH: c_int = 16;

/*
 * Maximum byte length of the string equivalent to any one Unicode code point,
 * in any backend encoding. (See the C header.)
 */
pub const MAX_UNICODE_EQUIVALENT_STRING: c_int = 16;

/*
 * Table for mapping an encoding number to official encoding name and
 * possibly other subsidiary data.  Be careful to check encoding number
 * before accessing a table entry!
 *
 * if (PG_VALID_ENCODING(encoding))
 *		pg_enc2name_tbl[ encoding ];
 */
#[repr(C)]
pub struct pg_enc2name {
    pub name: *const c_char,
    pub encoding: pg_enc,
    // #ifdef WIN32: unsigned codepage; /* codepage for WIN32 */ -- omitted (non-WIN32 build)
}

// extern PGDLLIMPORT const pg_enc2name pg_enc2name_tbl[]; -- defined in encnames.c

/*
 * Encoding names for gettext
 */
// extern PGDLLIMPORT const char *pg_enc2gettext_tbl[]; -- defined in encnames.c

/*
 * pg_wchar stuff
 */
pub type mb2wchar_with_len_converter =
    Option<unsafe extern "C" fn(from: *const c_uchar, to: *mut pg_wchar, len: c_int) -> c_int>;

pub type wchar2mb_with_len_converter =
    Option<unsafe extern "C" fn(from: *const pg_wchar, to: *mut c_uchar, len: c_int) -> c_int>;

pub type mblen_converter = Option<unsafe extern "C" fn(mbstr: *const c_uchar) -> c_int>;

pub type mbdisplaylen_converter = Option<unsafe extern "C" fn(mbstr: *const c_uchar) -> c_int>;

pub type mbcharacter_incrementer =
    Option<unsafe extern "C" fn(mbstr: *mut c_uchar, len: c_int) -> bool>;

pub type mbchar_verifier =
    Option<unsafe extern "C" fn(mbstr: *const c_uchar, len: c_int) -> c_int>;

pub type mbstr_verifier =
    Option<unsafe extern "C" fn(mbstr: *const c_uchar, len: c_int) -> c_int>;

#[repr(C)]
pub struct pg_wchar_tbl {
    /// convert a multibyte string to a wchar
    pub mb2wchar_with_len: mb2wchar_with_len_converter,
    /// convert a wchar string to a multibyte
    pub wchar2mb_with_len: wchar2mb_with_len_converter,
    /// get byte length of a char
    pub mblen: mblen_converter,
    /// get display width of a char
    pub dsplen: mbdisplaylen_converter,
    /// verify multibyte character
    pub mbverifychar: mbchar_verifier,
    /// verify multibyte string
    pub mbverifystr: mbstr_verifier,
    /// max bytes for a char in this encoding
    pub maxmblen: c_int,
}

// extern PGDLLIMPORT const pg_wchar_tbl pg_wchar_table[]; -- defined below.

/*
 * Radix tree for character conversion. (See the C header for the full layout
 * description.)
 */
#[repr(C)]
pub struct pg_mb_radix_tree {
    /*
     * Array containing all the values. Only one of chars16 or chars32 is
     * used, depending on how wide the values we need to represent are.
     */
    pub chars16: *const uint16,
    pub chars32: *const uint32,

    /* Radix tree for 1-byte inputs */
    pub b1root: uint32, /* offset of table in the chars[16|32] array */
    pub b1_lower: uint8, /* min allowed value for a single byte input */
    pub b1_upper: uint8, /* max allowed value for a single byte input */

    /* Radix tree for 2-byte inputs */
    pub b2root: uint32, /* offset of 1st byte's table */
    pub b2_1_lower: uint8, /* min/max allowed value for 1st input byte */
    pub b2_1_upper: uint8,
    pub b2_2_lower: uint8, /* min/max allowed value for 2nd input byte */
    pub b2_2_upper: uint8,

    /* Radix tree for 3-byte inputs */
    pub b3root: uint32, /* offset of 1st byte's table */
    pub b3_1_lower: uint8, /* min/max allowed value for 1st input byte */
    pub b3_1_upper: uint8,
    pub b3_2_lower: uint8, /* min/max allowed value for 2nd input byte */
    pub b3_2_upper: uint8,
    pub b3_3_lower: uint8, /* min/max allowed value for 3rd input byte */
    pub b3_3_upper: uint8,

    /* Radix tree for 4-byte inputs */
    pub b4root: uint32, /* offset of 1st byte's table */
    pub b4_1_lower: uint8, /* min/max allowed value for 1st input byte */
    pub b4_1_upper: uint8,
    pub b4_2_lower: uint8, /* min/max allowed value for 2nd input byte */
    pub b4_2_upper: uint8,
    pub b4_3_lower: uint8, /* min/max allowed value for 3rd input byte */
    pub b4_3_upper: uint8,
    pub b4_4_lower: uint8, /* min/max allowed value for 4th input byte */
    pub b4_4_upper: uint8,
}

/*
 * UTF-8 to local code conversion map (for combined characters)
 */
#[repr(C)]
pub struct pg_utf_to_local_combined {
    pub utf1: uint32, /* UTF-8 code 1 */
    pub utf2: uint32, /* UTF-8 code 2 */
    pub code: uint32, /* local code */
}

/*
 * local code to UTF-8 conversion map (for combined characters)
 */
#[repr(C)]
pub struct pg_local_to_utf_combined {
    pub code: uint32, /* local code */
    pub utf1: uint32, /* UTF-8 code 1 */
    pub utf2: uint32, /* UTF-8 code 2 */
}

/*
 * callback function for algorithmic encoding conversions (in either direction)
 *
 * if function returns zero, it does not know how to convert the code
 */
pub type utf_local_conversion_func = Option<unsafe extern "C" fn(code: uint32) -> uint32>;

/*
 * Support macro CHECK_ENCODING_CONVERSION_ARGS is omitted here; it depends on
 * fmgr (PG_GETARG_INT32) which is intentionally not pulled into this header in
 * the C source either.
 */

/*
 * Some handy functions for Unicode-specific tests.
 */
#[inline(always)]
pub fn is_valid_unicode_codepoint(c: pg_wchar) -> bool {
    c > 0 && c <= 0x10FFFF
}

#[inline(always)]
pub fn is_utf16_surrogate_first(c: pg_wchar) -> bool {
    c >= 0xD800 && c <= 0xDBFF
}

#[inline(always)]
pub fn is_utf16_surrogate_second(c: pg_wchar) -> bool {
    c >= 0xDC00 && c <= 0xDFFF
}

#[inline(always)]
pub fn surrogate_pair_to_codepoint(first: pg_wchar, second: pg_wchar) -> pg_wchar {
    ((first & 0x3FF) << 10) + 0x10000 + (second & 0x3FF)
}

/*
 * Convert a UTF-8 character to a Unicode code point.
 * This is a one-character version of pg_utf2wchar_with_len.
 *
 * No error checks here, c must point to a long-enough string.
 */
#[inline]
pub unsafe fn utf8_to_unicode(c: *const c_uchar) -> pg_wchar {
    if (*c & 0x80) == 0 {
        *c.add(0) as pg_wchar
    } else if (*c & 0xe0) == 0xc0 {
        ((((*c.add(0) & 0x1f) as pg_wchar) << 6) | ((*c.add(1) & 0x3f) as pg_wchar)) as pg_wchar
    } else if (*c & 0xf0) == 0xe0 {
        (((*c.add(0) & 0x0f) as pg_wchar) << 12)
            | (((*c.add(1) & 0x3f) as pg_wchar) << 6)
            | ((*c.add(2) & 0x3f) as pg_wchar)
    } else if (*c & 0xf8) == 0xf0 {
        (((*c.add(0) & 0x07) as pg_wchar) << 18)
            | (((*c.add(1) & 0x3f) as pg_wchar) << 12)
            | (((*c.add(2) & 0x3f) as pg_wchar) << 6)
            | ((*c.add(3) & 0x3f) as pg_wchar)
    } else {
        /* that is an invalid code on purpose */
        0xffffffff
    }
}

/*
 * Map a Unicode code point to UTF-8.  utf8string must have at least
 * unicode_utf8len(c) bytes available.
 */
#[inline]
pub unsafe fn unicode_to_utf8(c: pg_wchar, utf8string: *mut c_uchar) -> *mut c_uchar {
    if c <= 0x7F {
        *utf8string.add(0) = c as c_uchar;
    } else if c <= 0x7FF {
        *utf8string.add(0) = (0xC0 | ((c >> 6) & 0x1F)) as c_uchar;
        *utf8string.add(1) = (0x80 | (c & 0x3F)) as c_uchar;
    } else if c <= 0xFFFF {
        *utf8string.add(0) = (0xE0 | ((c >> 12) & 0x0F)) as c_uchar;
        *utf8string.add(1) = (0x80 | ((c >> 6) & 0x3F)) as c_uchar;
        *utf8string.add(2) = (0x80 | (c & 0x3F)) as c_uchar;
    } else {
        *utf8string.add(0) = (0xF0 | ((c >> 18) & 0x07)) as c_uchar;
        *utf8string.add(1) = (0x80 | ((c >> 12) & 0x3F)) as c_uchar;
        *utf8string.add(2) = (0x80 | ((c >> 6) & 0x3F)) as c_uchar;
        *utf8string.add(3) = (0x80 | (c & 0x3F)) as c_uchar;
    }

    utf8string
}

/*
 * Number of bytes needed to represent the given char in UTF8.
 */
#[inline]
pub fn unicode_utf8len(c: pg_wchar) -> c_int {
    if c <= 0x7F {
        1
    } else if c <= 0x7FF {
        2
    } else if c <= 0xFFFF {
        3
    } else {
        4
    }
}

// ================================================================
//   wchar.c : implementation
// ================================================================

/*
 * In today's multibyte encodings other than UTF8, this two-byte sequence
 * ensures pg_encoding_mblen() == 2 && pg_encoding_verifymbstr() == 0.
 *
 * For historical reasons, several verifychar implementations opt to reject
 * this pair specifically. (See the C source for the full rationale.)
 *
 * PQescapeString() historically used spaces for BYTE1; many other values
 * could suffice for BYTE1.
 */
const NONUTF8_INVALID_BYTE0: c_uchar = 0x8d;
const NONUTF8_INVALID_BYTE1: c_uchar = b' ';

/*
 * Operations on multi-byte encodings are driven by a table of helper
 * functions. (See the C source for the contract these functions follow.)
 */

/* No error-reporting facility.  Ignore incomplete trailing byte sequence. */
// #define MB2CHAR_NEED_AT_LEAST(len, need) if ((len) < (need)) break
// In Rust this is expanded inline at each use site as `if len < need { break; }`.

/*
 * SQL/ASCII
 */
unsafe extern "C" fn pg_ascii2wchar_with_len(
    mut from: *const c_uchar,
    mut to: *mut pg_wchar,
    mut len: c_int,
) -> c_int {
    let mut cnt: c_int = 0;

    while len > 0 && *from != 0 {
        *to = *from as pg_wchar;
        to = to.add(1);
        from = from.add(1);
        len -= 1;
        cnt += 1;
    }
    *to = 0;
    cnt
}

unsafe extern "C" fn pg_ascii_mblen(_s: *const c_uchar) -> c_int {
    1
}

unsafe extern "C" fn pg_ascii_dsplen(s: *const c_uchar) -> c_int {
    if *s == b'\0' {
        return 0;
    }
    if *s < 0x20 || *s == 0x7f {
        return -1;
    }

    1
}

/*
 * EUC
 */
unsafe extern "C" fn pg_euc2wchar_with_len(
    mut from: *const c_uchar,
    mut to: *mut pg_wchar,
    mut len: c_int,
) -> c_int {
    let mut cnt: c_int = 0;

    while len > 0 && *from != 0 {
        if *from == SS2 {
            /* JIS X 0201 (so called "1 byte KANA") */
            if len < 2 {
                break;
            }
            from = from.add(1);
            *to = ((SS2 as pg_wchar) << 8) | (*from as pg_wchar);
            from = from.add(1);
            len -= 2;
        } else if *from == SS3 {
            /* JIS X 0212 KANJI */
            if len < 3 {
                break;
            }
            from = from.add(1);
            *to = ((SS3 as pg_wchar) << 16) | ((*from as pg_wchar) << 8);
            from = from.add(1);
            *to |= *from as pg_wchar;
            from = from.add(1);
            len -= 3;
        } else if IS_HIGHBIT_SET(*from) {
            /* JIS X 0208 KANJI */
            if len < 2 {
                break;
            }
            *to = (*from as pg_wchar) << 8;
            from = from.add(1);
            *to |= *from as pg_wchar;
            from = from.add(1);
            len -= 2;
        } else {
            /* must be ASCII */
            *to = *from as pg_wchar;
            from = from.add(1);
            len -= 1;
        }
        to = to.add(1);
        cnt += 1;
    }
    *to = 0;
    cnt
}

#[inline]
unsafe fn pg_euc_mblen(s: *const c_uchar) -> c_int {
    let len: c_int;

    if *s == SS2 {
        len = 2;
    } else if *s == SS3 {
        len = 3;
    } else if IS_HIGHBIT_SET(*s) {
        len = 2;
    } else {
        len = 1;
    }
    len
}

#[inline]
unsafe fn pg_euc_dsplen(s: *const c_uchar) -> c_int {
    let len: c_int;

    if *s == SS2 {
        len = 2;
    } else if *s == SS3 {
        len = 2;
    } else if IS_HIGHBIT_SET(*s) {
        len = 2;
    } else {
        len = pg_ascii_dsplen(s);
    }
    len
}

/*
 * EUC_JP
 */
unsafe extern "C" fn pg_eucjp2wchar_with_len(
    from: *const c_uchar,
    to: *mut pg_wchar,
    len: c_int,
) -> c_int {
    pg_euc2wchar_with_len(from, to, len)
}

unsafe extern "C" fn pg_eucjp_mblen(s: *const c_uchar) -> c_int {
    pg_euc_mblen(s)
}

unsafe extern "C" fn pg_eucjp_dsplen(s: *const c_uchar) -> c_int {
    let len: c_int;

    if *s == SS2 {
        len = 1;
    } else if *s == SS3 {
        len = 2;
    } else if IS_HIGHBIT_SET(*s) {
        len = 2;
    } else {
        len = pg_ascii_dsplen(s);
    }
    len
}

/*
 * EUC_KR
 */
unsafe extern "C" fn pg_euckr2wchar_with_len(
    from: *const c_uchar,
    to: *mut pg_wchar,
    len: c_int,
) -> c_int {
    pg_euc2wchar_with_len(from, to, len)
}

unsafe extern "C" fn pg_euckr_mblen(s: *const c_uchar) -> c_int {
    pg_euc_mblen(s)
}

unsafe extern "C" fn pg_euckr_dsplen(s: *const c_uchar) -> c_int {
    pg_euc_dsplen(s)
}

/*
 * EUC_CN
 *
 */
unsafe extern "C" fn pg_euccn2wchar_with_len(
    mut from: *const c_uchar,
    mut to: *mut pg_wchar,
    mut len: c_int,
) -> c_int {
    let mut cnt: c_int = 0;

    while len > 0 && *from != 0 {
        if *from == SS2 {
            /* code set 2 (unused?) */
            if len < 3 {
                break;
            }
            from = from.add(1);
            *to = ((SS2 as pg_wchar) << 16) | ((*from as pg_wchar) << 8);
            from = from.add(1);
            *to |= *from as pg_wchar;
            from = from.add(1);
            len -= 3;
        } else if *from == SS3 {
            /* code set 3 (unused ?) */
            if len < 3 {
                break;
            }
            from = from.add(1);
            *to = ((SS3 as pg_wchar) << 16) | ((*from as pg_wchar) << 8);
            from = from.add(1);
            *to |= *from as pg_wchar;
            from = from.add(1);
            len -= 3;
        } else if IS_HIGHBIT_SET(*from) {
            /* code set 1 */
            if len < 2 {
                break;
            }
            *to = (*from as pg_wchar) << 8;
            from = from.add(1);
            *to |= *from as pg_wchar;
            from = from.add(1);
            len -= 2;
        } else {
            *to = *from as pg_wchar;
            from = from.add(1);
            len -= 1;
        }
        to = to.add(1);
        cnt += 1;
    }
    *to = 0;
    cnt
}

/*
 * mbverifychar does not accept SS2 or SS3 (CS2 and CS3 are not defined for
 * EUC_CN), but mb2wchar_with_len does.  Tell a coherent story for code that
 * relies on agreement between mb2wchar_with_len and mblen.  Invalid text
 * datums (e.g. from shared catalogs) reach this.
 */
unsafe extern "C" fn pg_euccn_mblen(s: *const c_uchar) -> c_int {
    let len: c_int;

    if *s == SS2 {
        len = 3;
    } else if *s == SS3 {
        len = 3;
    } else if IS_HIGHBIT_SET(*s) {
        len = 2;
    } else {
        len = 1;
    }
    len
}

unsafe extern "C" fn pg_euccn_dsplen(s: *const c_uchar) -> c_int {
    let len: c_int;

    if IS_HIGHBIT_SET(*s) {
        len = 2;
    } else {
        len = pg_ascii_dsplen(s);
    }
    len
}

/*
 * EUC_TW
 *
 */
unsafe extern "C" fn pg_euctw2wchar_with_len(
    mut from: *const c_uchar,
    mut to: *mut pg_wchar,
    mut len: c_int,
) -> c_int {
    let mut cnt: c_int = 0;

    while len > 0 && *from != 0 {
        if *from == SS2 {
            /* code set 2 */
            if len < 4 {
                break;
            }
            from = from.add(1);
            *to = ((SS2 as uint32) << 24) | ((*from as pg_wchar) << 16);
            from = from.add(1);
            *to |= (*from as pg_wchar) << 8;
            from = from.add(1);
            *to |= *from as pg_wchar;
            from = from.add(1);
            len -= 4;
        } else if *from == SS3 {
            /* code set 3 (unused?) */
            if len < 3 {
                break;
            }
            from = from.add(1);
            *to = ((SS3 as pg_wchar) << 16) | ((*from as pg_wchar) << 8);
            from = from.add(1);
            *to |= *from as pg_wchar;
            from = from.add(1);
            len -= 3;
        } else if IS_HIGHBIT_SET(*from) {
            /* code set 2 */
            if len < 2 {
                break;
            }
            *to = (*from as pg_wchar) << 8;
            from = from.add(1);
            *to |= *from as pg_wchar;
            from = from.add(1);
            len -= 2;
        } else {
            *to = *from as pg_wchar;
            from = from.add(1);
            len -= 1;
        }
        to = to.add(1);
        cnt += 1;
    }
    *to = 0;
    cnt
}

unsafe extern "C" fn pg_euctw_mblen(s: *const c_uchar) -> c_int {
    let len: c_int;

    if *s == SS2 {
        len = 4;
    } else if *s == SS3 {
        len = 3;
    } else if IS_HIGHBIT_SET(*s) {
        len = 2;
    } else {
        len = 1;
    }
    len
}

unsafe extern "C" fn pg_euctw_dsplen(s: *const c_uchar) -> c_int {
    let len: c_int;

    if *s == SS2 {
        len = 2;
    } else if *s == SS3 {
        len = 2;
    } else if IS_HIGHBIT_SET(*s) {
        len = 2;
    } else {
        len = pg_ascii_dsplen(s);
    }
    len
}

/*
 * Convert pg_wchar to EUC_* encoding.
 * caller must allocate enough space for "to", including a trailing zero!
 * len: length of from.
 * "from" not necessarily null terminated.
 */
unsafe extern "C" fn pg_wchar2euc_with_len(
    mut from: *const pg_wchar,
    mut to: *mut c_uchar,
    mut len: c_int,
) -> c_int {
    let mut cnt: c_int = 0;

    while len > 0 && *from != 0 {
        // C: `unsigned char c;` then a cascade of `if ((c = (*from >> N)))` whose
        // assignment truncates to 8 bits and whose value is tested for nonzero.
        let c: c_uchar;

        c = (*from >> 24) as c_uchar;
        if c != 0 {
            *to = c;
            to = to.add(1);
            *to = ((*from >> 16) & 0xff) as c_uchar;
            to = to.add(1);
            *to = ((*from >> 8) & 0xff) as c_uchar;
            to = to.add(1);
            *to = (*from & 0xff) as c_uchar;
            to = to.add(1);
            cnt += 4;
        } else if {
            let c = (*from >> 16) as c_uchar;
            c != 0
        } {
            *to = (*from >> 16) as c_uchar;
            to = to.add(1);
            *to = ((*from >> 8) & 0xff) as c_uchar;
            to = to.add(1);
            *to = (*from & 0xff) as c_uchar;
            to = to.add(1);
            cnt += 3;
        } else if {
            let c = (*from >> 8) as c_uchar;
            c != 0
        } {
            *to = (*from >> 8) as c_uchar;
            to = to.add(1);
            *to = (*from & 0xff) as c_uchar;
            to = to.add(1);
            cnt += 2;
        } else {
            *to = *from as c_uchar;
            to = to.add(1);
            cnt += 1;
        }
        from = from.add(1);
        len -= 1;
    }
    *to = 0;
    cnt
}

/*
 * JOHAB
 */
unsafe extern "C" fn pg_johab_mblen(s: *const c_uchar) -> c_int {
    pg_euc_mblen(s)
}

unsafe extern "C" fn pg_johab_dsplen(s: *const c_uchar) -> c_int {
    pg_euc_dsplen(s)
}

/*
 * convert UTF8 string to pg_wchar (UCS-4)
 * caller must allocate enough space for "to", including a trailing zero!
 * len: length of from.
 * "from" not necessarily null terminated.
 */
unsafe extern "C" fn pg_utf2wchar_with_len(
    mut from: *const c_uchar,
    mut to: *mut pg_wchar,
    mut len: c_int,
) -> c_int {
    let mut cnt: c_int = 0;
    let mut c1: uint32;
    let mut c2: uint32;
    let mut c3: uint32;
    let mut c4: uint32;

    while len > 0 && *from != 0 {
        if (*from & 0x80) == 0 {
            *to = *from as pg_wchar;
            from = from.add(1);
            len -= 1;
        } else if (*from & 0xe0) == 0xc0 {
            if len < 2 {
                break;
            }
            c1 = (*from & 0x1f) as uint32;
            from = from.add(1);
            c2 = (*from & 0x3f) as uint32;
            from = from.add(1);
            *to = (c1 << 6) | c2;
            len -= 2;
        } else if (*from & 0xf0) == 0xe0 {
            if len < 3 {
                break;
            }
            c1 = (*from & 0x0f) as uint32;
            from = from.add(1);
            c2 = (*from & 0x3f) as uint32;
            from = from.add(1);
            c3 = (*from & 0x3f) as uint32;
            from = from.add(1);
            *to = (c1 << 12) | (c2 << 6) | c3;
            len -= 3;
        } else if (*from & 0xf8) == 0xf0 {
            if len < 4 {
                break;
            }
            c1 = (*from & 0x07) as uint32;
            from = from.add(1);
            c2 = (*from & 0x3f) as uint32;
            from = from.add(1);
            c3 = (*from & 0x3f) as uint32;
            from = from.add(1);
            c4 = (*from & 0x3f) as uint32;
            from = from.add(1);
            *to = (c1 << 18) | (c2 << 12) | (c3 << 6) | c4;
            len -= 4;
        } else {
            /* treat a bogus char as length 1; not ours to raise error */
            *to = *from as pg_wchar;
            from = from.add(1);
            len -= 1;
        }
        to = to.add(1);
        cnt += 1;
    }
    *to = 0;
    cnt
}

/*
 * Trivial conversion from pg_wchar to UTF-8.
 * caller should allocate enough space for "to"
 * len: length of from.
 * "from" not necessarily null terminated.
 */
unsafe extern "C" fn pg_wchar2utf_with_len(
    mut from: *const pg_wchar,
    mut to: *mut c_uchar,
    mut len: c_int,
) -> c_int {
    let mut cnt: c_int = 0;

    while len > 0 && *from != 0 {
        let char_len: c_int;

        unicode_to_utf8(*from, to);
        char_len = pg_utf_mblen(to);
        cnt += char_len;
        to = to.add(char_len as usize);
        from = from.add(1);
        len -= 1;
    }
    *to = 0;
    cnt
}

/*
 * Return the byte length of a UTF8 character pointed to by s
 *
 * Note: in the current implementation we do not support UTF8 sequences
 * of more than 4 bytes; hence do NOT return a value larger than 4.
 * We return "1" for any leading byte that is either flat-out illegal or
 * indicates a length larger than we support.
 *
 * pg_utf2wchar_with_len(), utf8_to_unicode(), pg_utf8_islegal(), and perhaps
 * other places would need to be fixed to change this.
 */
pub unsafe extern "C" fn pg_utf_mblen(s: *const c_uchar) -> c_int {
    let len: c_int;

    if (*s & 0x80) == 0 {
        len = 1;
    } else if (*s & 0xe0) == 0xc0 {
        len = 2;
    } else if (*s & 0xf0) == 0xe0 {
        len = 3;
    } else if (*s & 0xf8) == 0xf0 {
        len = 4;
    }
    // #ifdef NOT_USED
    //  else if ((*s & 0xfc) == 0xf8) len = 5;
    //  else if ((*s & 0xfe) == 0xfc) len = 6;
    // #endif
    else {
        len = 1;
    }
    len
}

/*
 * This is an implementation of wcwidth() and wcswidth() as defined in
 * "The Single UNIX Specification, Version 2, The Open Group, 1997"
 * <http://www.unix.org/online.html>
 *
 * Markus Kuhn -- 2001-09-08 -- public domain
 *
 * customised for PostgreSQL
 *
 * original available at : http://www.cl.cam.ac.uk/~mgk25/ucs/wcwidth.c
 */

#[repr(C)]
#[derive(Clone, Copy)]
pub struct mbinterval {
    pub first: c_uint,
    pub last: c_uint,
}

/* auxiliary function for binary search in interval table */
unsafe fn mbbisearch(ucs: pg_wchar, table: *const mbinterval, max: c_int) -> c_int {
    let mut min: c_int = 0;
    let mut max = max;
    let mut mid: c_int;

    if ucs < (*table.offset(0)).first || ucs > (*table.offset(max as isize)).last {
        return 0;
    }
    while max >= min {
        mid = (min + max) / 2;
        if ucs > (*table.offset(mid as isize)).last {
            min = mid + 1;
        } else if ucs < (*table.offset(mid as isize)).first {
            max = mid - 1;
        } else {
            return 1;
        }
    }

    0
}

/* The following functions define the column width of an ISO 10646
 * character. (See the C source for the full description of the width rules.)
 *
 * This implementation assumes that wchar_t characters are encoded
 * in ISO 10646.
 */

// TODO(pg-port): The C `ucs_wcwidth()` body `#include`s two generated Unicode DATA
// tables:
//   #include "common/unicode_nonspacing_table.h"   -> defines `nonspacing[]`
//   #include "common/unicode_east_asian_fw_table.h" -> defines `east_asian_fw[]`
// Per the port plan ONLY these two DATA tables are stubbed (left EMPTY) to avoid
// emitting thousands of constants. The mbbisearch() FUNCTION above is faithful, so
// pg_wcwidth/ucs_wcwidth still compile and run: with empty tables, the bisearch
// over them always returns 0, so combining/wide characters get the default width
// of 1 until the real tables are imported.
// Translated from common/unicode_nonspacing_table.h (334 intervals).
static nonspacing: &[mbinterval] = &[
    mbinterval { first: 0x00AD, last: 0x00AD },
    mbinterval { first: 0x0300, last: 0x036F },
    mbinterval { first: 0x0483, last: 0x0489 },
    mbinterval { first: 0x0591, last: 0x05BD },
    mbinterval { first: 0x05BF, last: 0x05BF },
    mbinterval { first: 0x05C1, last: 0x05C2 },
    mbinterval { first: 0x05C4, last: 0x05C5 },
    mbinterval { first: 0x05C7, last: 0x05C7 },
    mbinterval { first: 0x0600, last: 0x0605 },
    mbinterval { first: 0x0610, last: 0x061A },
    mbinterval { first: 0x061C, last: 0x061C },
    mbinterval { first: 0x064B, last: 0x065F },
    mbinterval { first: 0x0670, last: 0x0670 },
    mbinterval { first: 0x06D6, last: 0x06DD },
    mbinterval { first: 0x06DF, last: 0x06E4 },
    mbinterval { first: 0x06E7, last: 0x06E8 },
    mbinterval { first: 0x06EA, last: 0x06ED },
    mbinterval { first: 0x070F, last: 0x070F },
    mbinterval { first: 0x0711, last: 0x0711 },
    mbinterval { first: 0x0730, last: 0x074A },
    mbinterval { first: 0x07A6, last: 0x07B0 },
    mbinterval { first: 0x07EB, last: 0x07F3 },
    mbinterval { first: 0x07FD, last: 0x07FD },
    mbinterval { first: 0x0816, last: 0x0819 },
    mbinterval { first: 0x081B, last: 0x0823 },
    mbinterval { first: 0x0825, last: 0x0827 },
    mbinterval { first: 0x0829, last: 0x082D },
    mbinterval { first: 0x0859, last: 0x085B },
    mbinterval { first: 0x0890, last: 0x089F },
    mbinterval { first: 0x08CA, last: 0x0902 },
    mbinterval { first: 0x093A, last: 0x093A },
    mbinterval { first: 0x093C, last: 0x093C },
    mbinterval { first: 0x0941, last: 0x0948 },
    mbinterval { first: 0x094D, last: 0x094D },
    mbinterval { first: 0x0951, last: 0x0957 },
    mbinterval { first: 0x0962, last: 0x0963 },
    mbinterval { first: 0x0981, last: 0x0981 },
    mbinterval { first: 0x09BC, last: 0x09BC },
    mbinterval { first: 0x09C1, last: 0x09C4 },
    mbinterval { first: 0x09CD, last: 0x09CD },
    mbinterval { first: 0x09E2, last: 0x09E3 },
    mbinterval { first: 0x09FE, last: 0x0A02 },
    mbinterval { first: 0x0A3C, last: 0x0A3C },
    mbinterval { first: 0x0A41, last: 0x0A51 },
    mbinterval { first: 0x0A70, last: 0x0A71 },
    mbinterval { first: 0x0A75, last: 0x0A75 },
    mbinterval { first: 0x0A81, last: 0x0A82 },
    mbinterval { first: 0x0ABC, last: 0x0ABC },
    mbinterval { first: 0x0AC1, last: 0x0AC8 },
    mbinterval { first: 0x0ACD, last: 0x0ACD },
    mbinterval { first: 0x0AE2, last: 0x0AE3 },
    mbinterval { first: 0x0AFA, last: 0x0B01 },
    mbinterval { first: 0x0B3C, last: 0x0B3C },
    mbinterval { first: 0x0B3F, last: 0x0B3F },
    mbinterval { first: 0x0B41, last: 0x0B44 },
    mbinterval { first: 0x0B4D, last: 0x0B56 },
    mbinterval { first: 0x0B62, last: 0x0B63 },
    mbinterval { first: 0x0B82, last: 0x0B82 },
    mbinterval { first: 0x0BC0, last: 0x0BC0 },
    mbinterval { first: 0x0BCD, last: 0x0BCD },
    mbinterval { first: 0x0C00, last: 0x0C00 },
    mbinterval { first: 0x0C04, last: 0x0C04 },
    mbinterval { first: 0x0C3C, last: 0x0C3C },
    mbinterval { first: 0x0C3E, last: 0x0C40 },
    mbinterval { first: 0x0C46, last: 0x0C56 },
    mbinterval { first: 0x0C62, last: 0x0C63 },
    mbinterval { first: 0x0C81, last: 0x0C81 },
    mbinterval { first: 0x0CBC, last: 0x0CBC },
    mbinterval { first: 0x0CBF, last: 0x0CBF },
    mbinterval { first: 0x0CC6, last: 0x0CC6 },
    mbinterval { first: 0x0CCC, last: 0x0CCD },
    mbinterval { first: 0x0CE2, last: 0x0CE3 },
    mbinterval { first: 0x0D00, last: 0x0D01 },
    mbinterval { first: 0x0D3B, last: 0x0D3C },
    mbinterval { first: 0x0D41, last: 0x0D44 },
    mbinterval { first: 0x0D4D, last: 0x0D4D },
    mbinterval { first: 0x0D62, last: 0x0D63 },
    mbinterval { first: 0x0D81, last: 0x0D81 },
    mbinterval { first: 0x0DCA, last: 0x0DCA },
    mbinterval { first: 0x0DD2, last: 0x0DD6 },
    mbinterval { first: 0x0E31, last: 0x0E31 },
    mbinterval { first: 0x0E34, last: 0x0E3A },
    mbinterval { first: 0x0E47, last: 0x0E4E },
    mbinterval { first: 0x0EB1, last: 0x0EB1 },
    mbinterval { first: 0x0EB4, last: 0x0EBC },
    mbinterval { first: 0x0EC8, last: 0x0ECE },
    mbinterval { first: 0x0F18, last: 0x0F19 },
    mbinterval { first: 0x0F35, last: 0x0F35 },
    mbinterval { first: 0x0F37, last: 0x0F37 },
    mbinterval { first: 0x0F39, last: 0x0F39 },
    mbinterval { first: 0x0F71, last: 0x0F7E },
    mbinterval { first: 0x0F80, last: 0x0F84 },
    mbinterval { first: 0x0F86, last: 0x0F87 },
    mbinterval { first: 0x0F8D, last: 0x0FBC },
    mbinterval { first: 0x0FC6, last: 0x0FC6 },
    mbinterval { first: 0x102D, last: 0x1030 },
    mbinterval { first: 0x1032, last: 0x1037 },
    mbinterval { first: 0x1039, last: 0x103A },
    mbinterval { first: 0x103D, last: 0x103E },
    mbinterval { first: 0x1058, last: 0x1059 },
    mbinterval { first: 0x105E, last: 0x1060 },
    mbinterval { first: 0x1071, last: 0x1074 },
    mbinterval { first: 0x1082, last: 0x1082 },
    mbinterval { first: 0x1085, last: 0x1086 },
    mbinterval { first: 0x108D, last: 0x108D },
    mbinterval { first: 0x109D, last: 0x109D },
    mbinterval { first: 0x135D, last: 0x135F },
    mbinterval { first: 0x1712, last: 0x1714 },
    mbinterval { first: 0x1732, last: 0x1733 },
    mbinterval { first: 0x1752, last: 0x1753 },
    mbinterval { first: 0x1772, last: 0x1773 },
    mbinterval { first: 0x17B4, last: 0x17B5 },
    mbinterval { first: 0x17B7, last: 0x17BD },
    mbinterval { first: 0x17C6, last: 0x17C6 },
    mbinterval { first: 0x17C9, last: 0x17D3 },
    mbinterval { first: 0x17DD, last: 0x17DD },
    mbinterval { first: 0x180B, last: 0x180F },
    mbinterval { first: 0x1885, last: 0x1886 },
    mbinterval { first: 0x18A9, last: 0x18A9 },
    mbinterval { first: 0x1920, last: 0x1922 },
    mbinterval { first: 0x1927, last: 0x1928 },
    mbinterval { first: 0x1932, last: 0x1932 },
    mbinterval { first: 0x1939, last: 0x193B },
    mbinterval { first: 0x1A17, last: 0x1A18 },
    mbinterval { first: 0x1A1B, last: 0x1A1B },
    mbinterval { first: 0x1A56, last: 0x1A56 },
    mbinterval { first: 0x1A58, last: 0x1A60 },
    mbinterval { first: 0x1A62, last: 0x1A62 },
    mbinterval { first: 0x1A65, last: 0x1A6C },
    mbinterval { first: 0x1A73, last: 0x1A7F },
    mbinterval { first: 0x1AB0, last: 0x1B03 },
    mbinterval { first: 0x1B34, last: 0x1B34 },
    mbinterval { first: 0x1B36, last: 0x1B3A },
    mbinterval { first: 0x1B3C, last: 0x1B3C },
    mbinterval { first: 0x1B42, last: 0x1B42 },
    mbinterval { first: 0x1B6B, last: 0x1B73 },
    mbinterval { first: 0x1B80, last: 0x1B81 },
    mbinterval { first: 0x1BA2, last: 0x1BA5 },
    mbinterval { first: 0x1BA8, last: 0x1BA9 },
    mbinterval { first: 0x1BAB, last: 0x1BAD },
    mbinterval { first: 0x1BE6, last: 0x1BE6 },
    mbinterval { first: 0x1BE8, last: 0x1BE9 },
    mbinterval { first: 0x1BED, last: 0x1BED },
    mbinterval { first: 0x1BEF, last: 0x1BF1 },
    mbinterval { first: 0x1C2C, last: 0x1C33 },
    mbinterval { first: 0x1C36, last: 0x1C37 },
    mbinterval { first: 0x1CD0, last: 0x1CD2 },
    mbinterval { first: 0x1CD4, last: 0x1CE0 },
    mbinterval { first: 0x1CE2, last: 0x1CE8 },
    mbinterval { first: 0x1CED, last: 0x1CED },
    mbinterval { first: 0x1CF4, last: 0x1CF4 },
    mbinterval { first: 0x1CF8, last: 0x1CF9 },
    mbinterval { first: 0x1DC0, last: 0x1DFF },
    mbinterval { first: 0x200B, last: 0x200F },
    mbinterval { first: 0x202A, last: 0x202E },
    mbinterval { first: 0x2060, last: 0x206F },
    mbinterval { first: 0x20D0, last: 0x20F0 },
    mbinterval { first: 0x2CEF, last: 0x2CF1 },
    mbinterval { first: 0x2D7F, last: 0x2D7F },
    mbinterval { first: 0x2DE0, last: 0x2DFF },
    mbinterval { first: 0x302A, last: 0x302D },
    mbinterval { first: 0x3099, last: 0x309A },
    mbinterval { first: 0xA66F, last: 0xA672 },
    mbinterval { first: 0xA674, last: 0xA67D },
    mbinterval { first: 0xA69E, last: 0xA69F },
    mbinterval { first: 0xA6F0, last: 0xA6F1 },
    mbinterval { first: 0xA802, last: 0xA802 },
    mbinterval { first: 0xA806, last: 0xA806 },
    mbinterval { first: 0xA80B, last: 0xA80B },
    mbinterval { first: 0xA825, last: 0xA826 },
    mbinterval { first: 0xA82C, last: 0xA82C },
    mbinterval { first: 0xA8C4, last: 0xA8C5 },
    mbinterval { first: 0xA8E0, last: 0xA8F1 },
    mbinterval { first: 0xA8FF, last: 0xA8FF },
    mbinterval { first: 0xA926, last: 0xA92D },
    mbinterval { first: 0xA947, last: 0xA951 },
    mbinterval { first: 0xA980, last: 0xA982 },
    mbinterval { first: 0xA9B3, last: 0xA9B3 },
    mbinterval { first: 0xA9B6, last: 0xA9B9 },
    mbinterval { first: 0xA9BC, last: 0xA9BD },
    mbinterval { first: 0xA9E5, last: 0xA9E5 },
    mbinterval { first: 0xAA29, last: 0xAA2E },
    mbinterval { first: 0xAA31, last: 0xAA32 },
    mbinterval { first: 0xAA35, last: 0xAA36 },
    mbinterval { first: 0xAA43, last: 0xAA43 },
    mbinterval { first: 0xAA4C, last: 0xAA4C },
    mbinterval { first: 0xAA7C, last: 0xAA7C },
    mbinterval { first: 0xAAB0, last: 0xAAB0 },
    mbinterval { first: 0xAAB2, last: 0xAAB4 },
    mbinterval { first: 0xAAB7, last: 0xAAB8 },
    mbinterval { first: 0xAABE, last: 0xAABF },
    mbinterval { first: 0xAAC1, last: 0xAAC1 },
    mbinterval { first: 0xAAEC, last: 0xAAED },
    mbinterval { first: 0xAAF6, last: 0xAAF6 },
    mbinterval { first: 0xABE5, last: 0xABE5 },
    mbinterval { first: 0xABE8, last: 0xABE8 },
    mbinterval { first: 0xABED, last: 0xABED },
    mbinterval { first: 0xFB1E, last: 0xFB1E },
    mbinterval { first: 0xFE00, last: 0xFE0F },
    mbinterval { first: 0xFE20, last: 0xFE2F },
    mbinterval { first: 0xFEFF, last: 0xFEFF },
    mbinterval { first: 0xFFF9, last: 0xFFFB },
    mbinterval { first: 0x101FD, last: 0x101FD },
    mbinterval { first: 0x102E0, last: 0x102E0 },
    mbinterval { first: 0x10376, last: 0x1037A },
    mbinterval { first: 0x10A01, last: 0x10A0F },
    mbinterval { first: 0x10A38, last: 0x10A3F },
    mbinterval { first: 0x10AE5, last: 0x10AE6 },
    mbinterval { first: 0x10D24, last: 0x10D27 },
    mbinterval { first: 0x10D69, last: 0x10D6D },
    mbinterval { first: 0x10EAB, last: 0x10EAC },
    mbinterval { first: 0x10EFC, last: 0x10EFF },
    mbinterval { first: 0x10F46, last: 0x10F50 },
    mbinterval { first: 0x10F82, last: 0x10F85 },
    mbinterval { first: 0x11001, last: 0x11001 },
    mbinterval { first: 0x11038, last: 0x11046 },
    mbinterval { first: 0x11070, last: 0x11070 },
    mbinterval { first: 0x11073, last: 0x11074 },
    mbinterval { first: 0x1107F, last: 0x11081 },
    mbinterval { first: 0x110B3, last: 0x110B6 },
    mbinterval { first: 0x110B9, last: 0x110BA },
    mbinterval { first: 0x110BD, last: 0x110BD },
    mbinterval { first: 0x110C2, last: 0x110CD },
    mbinterval { first: 0x11100, last: 0x11102 },
    mbinterval { first: 0x11127, last: 0x1112B },
    mbinterval { first: 0x1112D, last: 0x11134 },
    mbinterval { first: 0x11173, last: 0x11173 },
    mbinterval { first: 0x11180, last: 0x11181 },
    mbinterval { first: 0x111B6, last: 0x111BE },
    mbinterval { first: 0x111C9, last: 0x111CC },
    mbinterval { first: 0x111CF, last: 0x111CF },
    mbinterval { first: 0x1122F, last: 0x11231 },
    mbinterval { first: 0x11234, last: 0x11234 },
    mbinterval { first: 0x11236, last: 0x11237 },
    mbinterval { first: 0x1123E, last: 0x1123E },
    mbinterval { first: 0x11241, last: 0x11241 },
    mbinterval { first: 0x112DF, last: 0x112DF },
    mbinterval { first: 0x112E3, last: 0x112EA },
    mbinterval { first: 0x11300, last: 0x11301 },
    mbinterval { first: 0x1133B, last: 0x1133C },
    mbinterval { first: 0x11340, last: 0x11340 },
    mbinterval { first: 0x11366, last: 0x11374 },
    mbinterval { first: 0x113BB, last: 0x113C0 },
    mbinterval { first: 0x113CE, last: 0x113CE },
    mbinterval { first: 0x113D0, last: 0x113D0 },
    mbinterval { first: 0x113D2, last: 0x113D2 },
    mbinterval { first: 0x113E1, last: 0x113E2 },
    mbinterval { first: 0x11438, last: 0x1143F },
    mbinterval { first: 0x11442, last: 0x11444 },
    mbinterval { first: 0x11446, last: 0x11446 },
    mbinterval { first: 0x1145E, last: 0x1145E },
    mbinterval { first: 0x114B3, last: 0x114B8 },
    mbinterval { first: 0x114BA, last: 0x114BA },
    mbinterval { first: 0x114BF, last: 0x114C0 },
    mbinterval { first: 0x114C2, last: 0x114C3 },
    mbinterval { first: 0x115B2, last: 0x115B5 },
    mbinterval { first: 0x115BC, last: 0x115BD },
    mbinterval { first: 0x115BF, last: 0x115C0 },
    mbinterval { first: 0x115DC, last: 0x115DD },
    mbinterval { first: 0x11633, last: 0x1163A },
    mbinterval { first: 0x1163D, last: 0x1163D },
    mbinterval { first: 0x1163F, last: 0x11640 },
    mbinterval { first: 0x116AB, last: 0x116AB },
    mbinterval { first: 0x116AD, last: 0x116AD },
    mbinterval { first: 0x116B0, last: 0x116B5 },
    mbinterval { first: 0x116B7, last: 0x116B7 },
    mbinterval { first: 0x1171D, last: 0x1171D },
    mbinterval { first: 0x1171F, last: 0x1171F },
    mbinterval { first: 0x11722, last: 0x11725 },
    mbinterval { first: 0x11727, last: 0x1172B },
    mbinterval { first: 0x1182F, last: 0x11837 },
    mbinterval { first: 0x11839, last: 0x1183A },
    mbinterval { first: 0x1193B, last: 0x1193C },
    mbinterval { first: 0x1193E, last: 0x1193E },
    mbinterval { first: 0x11943, last: 0x11943 },
    mbinterval { first: 0x119D4, last: 0x119DB },
    mbinterval { first: 0x119E0, last: 0x119E0 },
    mbinterval { first: 0x11A01, last: 0x11A0A },
    mbinterval { first: 0x11A33, last: 0x11A38 },
    mbinterval { first: 0x11A3B, last: 0x11A3E },
    mbinterval { first: 0x11A47, last: 0x11A47 },
    mbinterval { first: 0x11A51, last: 0x11A56 },
    mbinterval { first: 0x11A59, last: 0x11A5B },
    mbinterval { first: 0x11A8A, last: 0x11A96 },
    mbinterval { first: 0x11A98, last: 0x11A99 },
    mbinterval { first: 0x11C30, last: 0x11C3D },
    mbinterval { first: 0x11C3F, last: 0x11C3F },
    mbinterval { first: 0x11C92, last: 0x11CA7 },
    mbinterval { first: 0x11CAA, last: 0x11CB0 },
    mbinterval { first: 0x11CB2, last: 0x11CB3 },
    mbinterval { first: 0x11CB5, last: 0x11CB6 },
    mbinterval { first: 0x11D31, last: 0x11D45 },
    mbinterval { first: 0x11D47, last: 0x11D47 },
    mbinterval { first: 0x11D90, last: 0x11D91 },
    mbinterval { first: 0x11D95, last: 0x11D95 },
    mbinterval { first: 0x11D97, last: 0x11D97 },
    mbinterval { first: 0x11EF3, last: 0x11EF4 },
    mbinterval { first: 0x11F00, last: 0x11F01 },
    mbinterval { first: 0x11F36, last: 0x11F3A },
    mbinterval { first: 0x11F40, last: 0x11F40 },
    mbinterval { first: 0x11F42, last: 0x11F42 },
    mbinterval { first: 0x11F5A, last: 0x11F5A },
    mbinterval { first: 0x13430, last: 0x13440 },
    mbinterval { first: 0x13447, last: 0x13455 },
    mbinterval { first: 0x1611E, last: 0x16129 },
    mbinterval { first: 0x1612D, last: 0x1612F },
    mbinterval { first: 0x16AF0, last: 0x16AF4 },
    mbinterval { first: 0x16B30, last: 0x16B36 },
    mbinterval { first: 0x16F4F, last: 0x16F4F },
    mbinterval { first: 0x16F8F, last: 0x16F92 },
    mbinterval { first: 0x16FE4, last: 0x16FE4 },
    mbinterval { first: 0x1BC9D, last: 0x1BC9E },
    mbinterval { first: 0x1BCA0, last: 0x1BCA3 },
    mbinterval { first: 0x1CF00, last: 0x1CF46 },
    mbinterval { first: 0x1D167, last: 0x1D169 },
    mbinterval { first: 0x1D173, last: 0x1D182 },
    mbinterval { first: 0x1D185, last: 0x1D18B },
    mbinterval { first: 0x1D1AA, last: 0x1D1AD },
    mbinterval { first: 0x1D242, last: 0x1D244 },
    mbinterval { first: 0x1DA00, last: 0x1DA36 },
    mbinterval { first: 0x1DA3B, last: 0x1DA6C },
    mbinterval { first: 0x1DA75, last: 0x1DA75 },
    mbinterval { first: 0x1DA84, last: 0x1DA84 },
    mbinterval { first: 0x1DA9B, last: 0x1DAAF },
    mbinterval { first: 0x1E000, last: 0x1E02A },
    mbinterval { first: 0x1E08F, last: 0x1E08F },
    mbinterval { first: 0x1E130, last: 0x1E136 },
    mbinterval { first: 0x1E2AE, last: 0x1E2AE },
    mbinterval { first: 0x1E2EC, last: 0x1E2EF },
    mbinterval { first: 0x1E4EC, last: 0x1E4EF },
    mbinterval { first: 0x1E5EE, last: 0x1E5EF },
    mbinterval { first: 0x1E8D0, last: 0x1E8D6 },
    mbinterval { first: 0x1E944, last: 0x1E94A },
    mbinterval { first: 0xE0001, last: 0xE01EF },
];
// Translated from common/unicode_east_asian_fw_table.h (122 intervals).
static east_asian_fw: &[mbinterval] = &[
    mbinterval { first: 0x1100, last: 0x115F },
    mbinterval { first: 0x231A, last: 0x231B },
    mbinterval { first: 0x2329, last: 0x232A },
    mbinterval { first: 0x23E9, last: 0x23EC },
    mbinterval { first: 0x23F0, last: 0x23F0 },
    mbinterval { first: 0x23F3, last: 0x23F3 },
    mbinterval { first: 0x25FD, last: 0x25FE },
    mbinterval { first: 0x2614, last: 0x2615 },
    mbinterval { first: 0x2630, last: 0x2637 },
    mbinterval { first: 0x2648, last: 0x2653 },
    mbinterval { first: 0x267F, last: 0x267F },
    mbinterval { first: 0x268A, last: 0x268F },
    mbinterval { first: 0x2693, last: 0x2693 },
    mbinterval { first: 0x26A1, last: 0x26A1 },
    mbinterval { first: 0x26AA, last: 0x26AB },
    mbinterval { first: 0x26BD, last: 0x26BE },
    mbinterval { first: 0x26C4, last: 0x26C5 },
    mbinterval { first: 0x26CE, last: 0x26CE },
    mbinterval { first: 0x26D4, last: 0x26D4 },
    mbinterval { first: 0x26EA, last: 0x26EA },
    mbinterval { first: 0x26F2, last: 0x26F3 },
    mbinterval { first: 0x26F5, last: 0x26F5 },
    mbinterval { first: 0x26FA, last: 0x26FA },
    mbinterval { first: 0x26FD, last: 0x26FD },
    mbinterval { first: 0x2705, last: 0x2705 },
    mbinterval { first: 0x270A, last: 0x270B },
    mbinterval { first: 0x2728, last: 0x2728 },
    mbinterval { first: 0x274C, last: 0x274C },
    mbinterval { first: 0x274E, last: 0x274E },
    mbinterval { first: 0x2753, last: 0x2755 },
    mbinterval { first: 0x2757, last: 0x2757 },
    mbinterval { first: 0x2795, last: 0x2797 },
    mbinterval { first: 0x27B0, last: 0x27B0 },
    mbinterval { first: 0x27BF, last: 0x27BF },
    mbinterval { first: 0x2B1B, last: 0x2B1C },
    mbinterval { first: 0x2B50, last: 0x2B50 },
    mbinterval { first: 0x2B55, last: 0x2B55 },
    mbinterval { first: 0x2E80, last: 0x2E99 },
    mbinterval { first: 0x2E9B, last: 0x2EF3 },
    mbinterval { first: 0x2F00, last: 0x2FD5 },
    mbinterval { first: 0x2FF0, last: 0x303E },
    mbinterval { first: 0x3041, last: 0x3096 },
    mbinterval { first: 0x3099, last: 0x30FF },
    mbinterval { first: 0x3105, last: 0x312F },
    mbinterval { first: 0x3131, last: 0x318E },
    mbinterval { first: 0x3190, last: 0x31E5 },
    mbinterval { first: 0x31EF, last: 0x321E },
    mbinterval { first: 0x3220, last: 0x3247 },
    mbinterval { first: 0x3250, last: 0xA48C },
    mbinterval { first: 0xA490, last: 0xA4C6 },
    mbinterval { first: 0xA960, last: 0xA97C },
    mbinterval { first: 0xAC00, last: 0xD7A3 },
    mbinterval { first: 0xF900, last: 0xFAFF },
    mbinterval { first: 0xFE10, last: 0xFE19 },
    mbinterval { first: 0xFE30, last: 0xFE52 },
    mbinterval { first: 0xFE54, last: 0xFE66 },
    mbinterval { first: 0xFE68, last: 0xFE6B },
    mbinterval { first: 0xFF01, last: 0xFF60 },
    mbinterval { first: 0xFFE0, last: 0xFFE6 },
    mbinterval { first: 0x16FE0, last: 0x16FE4 },
    mbinterval { first: 0x16FF0, last: 0x16FF1 },
    mbinterval { first: 0x17000, last: 0x187F7 },
    mbinterval { first: 0x18800, last: 0x18CD5 },
    mbinterval { first: 0x18CFF, last: 0x18D08 },
    mbinterval { first: 0x1AFF0, last: 0x1AFF3 },
    mbinterval { first: 0x1AFF5, last: 0x1AFFB },
    mbinterval { first: 0x1AFFD, last: 0x1AFFE },
    mbinterval { first: 0x1B000, last: 0x1B122 },
    mbinterval { first: 0x1B132, last: 0x1B132 },
    mbinterval { first: 0x1B150, last: 0x1B152 },
    mbinterval { first: 0x1B155, last: 0x1B155 },
    mbinterval { first: 0x1B164, last: 0x1B167 },
    mbinterval { first: 0x1B170, last: 0x1B2FB },
    mbinterval { first: 0x1D300, last: 0x1D356 },
    mbinterval { first: 0x1D360, last: 0x1D376 },
    mbinterval { first: 0x1F004, last: 0x1F004 },
    mbinterval { first: 0x1F0CF, last: 0x1F0CF },
    mbinterval { first: 0x1F18E, last: 0x1F18E },
    mbinterval { first: 0x1F191, last: 0x1F19A },
    mbinterval { first: 0x1F200, last: 0x1F202 },
    mbinterval { first: 0x1F210, last: 0x1F23B },
    mbinterval { first: 0x1F240, last: 0x1F248 },
    mbinterval { first: 0x1F250, last: 0x1F251 },
    mbinterval { first: 0x1F260, last: 0x1F265 },
    mbinterval { first: 0x1F300, last: 0x1F320 },
    mbinterval { first: 0x1F32D, last: 0x1F335 },
    mbinterval { first: 0x1F337, last: 0x1F37C },
    mbinterval { first: 0x1F37E, last: 0x1F393 },
    mbinterval { first: 0x1F3A0, last: 0x1F3CA },
    mbinterval { first: 0x1F3CF, last: 0x1F3D3 },
    mbinterval { first: 0x1F3E0, last: 0x1F3F0 },
    mbinterval { first: 0x1F3F4, last: 0x1F3F4 },
    mbinterval { first: 0x1F3F8, last: 0x1F43E },
    mbinterval { first: 0x1F440, last: 0x1F440 },
    mbinterval { first: 0x1F442, last: 0x1F4FC },
    mbinterval { first: 0x1F4FF, last: 0x1F53D },
    mbinterval { first: 0x1F54B, last: 0x1F54E },
    mbinterval { first: 0x1F550, last: 0x1F567 },
    mbinterval { first: 0x1F57A, last: 0x1F57A },
    mbinterval { first: 0x1F595, last: 0x1F596 },
    mbinterval { first: 0x1F5A4, last: 0x1F5A4 },
    mbinterval { first: 0x1F5FB, last: 0x1F64F },
    mbinterval { first: 0x1F680, last: 0x1F6C5 },
    mbinterval { first: 0x1F6CC, last: 0x1F6CC },
    mbinterval { first: 0x1F6D0, last: 0x1F6D2 },
    mbinterval { first: 0x1F6D5, last: 0x1F6D7 },
    mbinterval { first: 0x1F6DC, last: 0x1F6DF },
    mbinterval { first: 0x1F6EB, last: 0x1F6EC },
    mbinterval { first: 0x1F6F4, last: 0x1F6FC },
    mbinterval { first: 0x1F7E0, last: 0x1F7EB },
    mbinterval { first: 0x1F7F0, last: 0x1F7F0 },
    mbinterval { first: 0x1F90C, last: 0x1F93A },
    mbinterval { first: 0x1F93C, last: 0x1F945 },
    mbinterval { first: 0x1F947, last: 0x1F9FF },
    mbinterval { first: 0x1FA70, last: 0x1FA7C },
    mbinterval { first: 0x1FA80, last: 0x1FA89 },
    mbinterval { first: 0x1FA8F, last: 0x1FAC6 },
    mbinterval { first: 0x1FACE, last: 0x1FADC },
    mbinterval { first: 0x1FADF, last: 0x1FAE9 },
    mbinterval { first: 0x1FAF0, last: 0x1FAF8 },
    mbinterval { first: 0x20000, last: 0x2FFFD },
    mbinterval { first: 0x30000, last: 0x3FFFD },
];

unsafe fn ucs_wcwidth(ucs: pg_wchar) -> c_int {
    // (See the TODO above: the two Unicode tables are stubbed empty.)

    /* test for 8-bit control characters */
    if ucs == 0 {
        return 0;
    }

    if ucs < 0x20 || (ucs >= 0x7f && ucs < 0xa0) || ucs > 0x0010ffff {
        return -1;
    }

    /*
     * binary search in table of non-spacing characters
     *
     * XXX: In the official Unicode sources, it is possible for a character to
     * be described as both non-spacing and wide at the same time. As of
     * Unicode 13.0, treating the non-spacing property as the determining
     * factor for display width leads to the correct behavior, so do that
     * search first.
     */
    // sizeof(nonspacing) / sizeof(struct mbinterval) - 1 ; with an empty stub the
    // table is empty, so guard the bisearch (which dereferences table[0]) and skip.
    if !nonspacing.is_empty()
        && mbbisearch(ucs, nonspacing.as_ptr(), (nonspacing.len() - 1) as c_int) != 0
    {
        return 0;
    }

    /* binary search in table of wide characters */
    if !east_asian_fw.is_empty()
        && mbbisearch(ucs, east_asian_fw.as_ptr(), (east_asian_fw.len() - 1) as c_int) != 0
    {
        return 2;
    }

    1
}

unsafe extern "C" fn pg_utf_dsplen(s: *const c_uchar) -> c_int {
    ucs_wcwidth(utf8_to_unicode(s))
}

/*
 * convert mule internal code to pg_wchar
 * caller should allocate enough space for "to"
 * len: length of from.
 * "from" not necessarily null terminated.
 */
unsafe extern "C" fn pg_mule2wchar_with_len(
    mut from: *const c_uchar,
    mut to: *mut pg_wchar,
    mut len: c_int,
) -> c_int {
    let mut cnt: c_int = 0;

    while len > 0 && *from != 0 {
        if IS_LC1(*from) {
            if len < 2 {
                break;
            }
            *to = (*from as pg_wchar) << 16;
            from = from.add(1);
            *to |= *from as pg_wchar;
            from = from.add(1);
            len -= 2;
        } else if IS_LCPRV1(*from) {
            if len < 3 {
                break;
            }
            from = from.add(1);
            *to = (*from as pg_wchar) << 16;
            from = from.add(1);
            *to |= *from as pg_wchar;
            from = from.add(1);
            len -= 3;
        } else if IS_LC2(*from) {
            if len < 3 {
                break;
            }
            *to = (*from as pg_wchar) << 16;
            from = from.add(1);
            *to |= (*from as pg_wchar) << 8;
            from = from.add(1);
            *to |= *from as pg_wchar;
            from = from.add(1);
            len -= 3;
        } else if IS_LCPRV2(*from) {
            if len < 4 {
                break;
            }
            from = from.add(1);
            *to = (*from as pg_wchar) << 16;
            from = from.add(1);
            *to |= (*from as pg_wchar) << 8;
            from = from.add(1);
            *to |= *from as pg_wchar;
            from = from.add(1);
            len -= 4;
        } else {
            /* assume ASCII */
            *to = *from as c_uchar as pg_wchar;
            from = from.add(1);
            len -= 1;
        }
        to = to.add(1);
        cnt += 1;
    }
    *to = 0;
    cnt
}

/*
 * convert pg_wchar to mule internal code
 * caller should allocate enough space for "to"
 * len: length of from.
 * "from" not necessarily null terminated.
 */
unsafe extern "C" fn pg_wchar2mule_with_len(
    mut from: *const pg_wchar,
    mut to: *mut c_uchar,
    mut len: c_int,
) -> c_int {
    let mut cnt: c_int = 0;

    while len > 0 && *from != 0 {
        let lb: c_uchar;

        lb = ((*from >> 16) & 0xff) as c_uchar;
        if IS_LC1(lb) {
            *to = lb;
            to = to.add(1);
            *to = (*from & 0xff) as c_uchar;
            to = to.add(1);
            cnt += 2;
        } else if IS_LC2(lb) {
            *to = lb;
            to = to.add(1);
            *to = ((*from >> 8) & 0xff) as c_uchar;
            to = to.add(1);
            *to = (*from & 0xff) as c_uchar;
            to = to.add(1);
            cnt += 3;
        } else if IS_LCPRV1_A_RANGE(lb) {
            *to = LCPRV1_A;
            to = to.add(1);
            *to = lb;
            to = to.add(1);
            *to = (*from & 0xff) as c_uchar;
            to = to.add(1);
            cnt += 3;
        } else if IS_LCPRV1_B_RANGE(lb) {
            *to = LCPRV1_B;
            to = to.add(1);
            *to = lb;
            to = to.add(1);
            *to = (*from & 0xff) as c_uchar;
            to = to.add(1);
            cnt += 3;
        } else if IS_LCPRV2_A_RANGE(lb) {
            *to = LCPRV2_A;
            to = to.add(1);
            *to = lb;
            to = to.add(1);
            *to = ((*from >> 8) & 0xff) as c_uchar;
            to = to.add(1);
            *to = (*from & 0xff) as c_uchar;
            to = to.add(1);
            cnt += 4;
        } else if IS_LCPRV2_B_RANGE(lb) {
            *to = LCPRV2_B;
            to = to.add(1);
            *to = lb;
            to = to.add(1);
            *to = ((*from >> 8) & 0xff) as c_uchar;
            to = to.add(1);
            *to = (*from & 0xff) as c_uchar;
            to = to.add(1);
            cnt += 4;
        } else {
            *to = (*from & 0xff) as c_uchar;
            to = to.add(1);
            cnt += 1;
        }
        from = from.add(1);
        len -= 1;
    }
    *to = 0;
    cnt
}

/* exported for direct use by conv.c */
pub unsafe extern "C" fn pg_mule_mblen(s: *const c_uchar) -> c_int {
    let len: c_int;

    if IS_LC1(*s) {
        len = 2;
    } else if IS_LCPRV1(*s) {
        len = 3;
    } else if IS_LC2(*s) {
        len = 3;
    } else if IS_LCPRV2(*s) {
        len = 4;
    } else {
        len = 1; /* assume ASCII */
    }
    len
}

unsafe extern "C" fn pg_mule_dsplen(s: *const c_uchar) -> c_int {
    let len: c_int;

    /*
     * Note: it's not really appropriate to assume that all multibyte charsets
     * are double-wide on screen.  But this seems an okay approximation for
     * the MULE charsets we currently support.
     */

    if IS_LC1(*s) {
        len = 1;
    } else if IS_LCPRV1(*s) {
        len = 1;
    } else if IS_LC2(*s) {
        len = 2;
    } else if IS_LCPRV2(*s) {
        len = 2;
    } else {
        len = 1; /* assume ASCII */
    }

    len
}

/*
 * ISO8859-1
 */
unsafe extern "C" fn pg_latin12wchar_with_len(
    mut from: *const c_uchar,
    mut to: *mut pg_wchar,
    mut len: c_int,
) -> c_int {
    let mut cnt: c_int = 0;

    while len > 0 && *from != 0 {
        *to = *from as pg_wchar;
        to = to.add(1);
        from = from.add(1);
        len -= 1;
        cnt += 1;
    }
    *to = 0;
    cnt
}

/*
 * Trivial conversion from pg_wchar to single byte encoding. Just ignores
 * high bits.
 * caller should allocate enough space for "to"
 * len: length of from.
 * "from" not necessarily null terminated.
 */
unsafe extern "C" fn pg_wchar2single_with_len(
    mut from: *const pg_wchar,
    mut to: *mut c_uchar,
    mut len: c_int,
) -> c_int {
    let mut cnt: c_int = 0;

    while len > 0 && *from != 0 {
        *to = *from as c_uchar;
        to = to.add(1);
        from = from.add(1);
        len -= 1;
        cnt += 1;
    }
    *to = 0;
    cnt
}

unsafe extern "C" fn pg_latin1_mblen(_s: *const c_uchar) -> c_int {
    1
}

unsafe extern "C" fn pg_latin1_dsplen(s: *const c_uchar) -> c_int {
    pg_ascii_dsplen(s)
}

/*
 * SJIS
 */
unsafe extern "C" fn pg_sjis_mblen(s: *const c_uchar) -> c_int {
    let len: c_int;

    if *s >= 0xa1 && *s <= 0xdf {
        len = 1; /* 1 byte kana? */
    } else if IS_HIGHBIT_SET(*s) {
        len = 2; /* kanji? */
    } else {
        len = 1; /* should be ASCII */
    }
    len
}

unsafe extern "C" fn pg_sjis_dsplen(s: *const c_uchar) -> c_int {
    let len: c_int;

    if *s >= 0xa1 && *s <= 0xdf {
        len = 1; /* 1 byte kana? */
    } else if IS_HIGHBIT_SET(*s) {
        len = 2; /* kanji? */
    } else {
        len = pg_ascii_dsplen(s); /* should be ASCII */
    }
    len
}

/*
 * Big5
 */
unsafe extern "C" fn pg_big5_mblen(s: *const c_uchar) -> c_int {
    let len: c_int;

    if IS_HIGHBIT_SET(*s) {
        len = 2; /* kanji? */
    } else {
        len = 1; /* should be ASCII */
    }
    len
}

unsafe extern "C" fn pg_big5_dsplen(s: *const c_uchar) -> c_int {
    let len: c_int;

    if IS_HIGHBIT_SET(*s) {
        len = 2; /* kanji? */
    } else {
        len = pg_ascii_dsplen(s); /* should be ASCII */
    }
    len
}

/*
 * GBK
 */
unsafe extern "C" fn pg_gbk_mblen(s: *const c_uchar) -> c_int {
    let len: c_int;

    if IS_HIGHBIT_SET(*s) {
        len = 2; /* kanji? */
    } else {
        len = 1; /* should be ASCII */
    }
    len
}

unsafe extern "C" fn pg_gbk_dsplen(s: *const c_uchar) -> c_int {
    let len: c_int;

    if IS_HIGHBIT_SET(*s) {
        len = 2; /* kanji? */
    } else {
        len = pg_ascii_dsplen(s); /* should be ASCII */
    }
    len
}

/*
 * UHC
 */
unsafe extern "C" fn pg_uhc_mblen(s: *const c_uchar) -> c_int {
    let len: c_int;

    if IS_HIGHBIT_SET(*s) {
        len = 2; /* 2byte? */
    } else {
        len = 1; /* should be ASCII */
    }
    len
}

unsafe extern "C" fn pg_uhc_dsplen(s: *const c_uchar) -> c_int {
    let len: c_int;

    if IS_HIGHBIT_SET(*s) {
        len = 2; /* 2byte? */
    } else {
        len = pg_ascii_dsplen(s); /* should be ASCII */
    }
    len
}

/*
 * GB18030
 *	Added by Bill Huang <bhuang@redhat.com>,<bill_huanghb@ybb.ne.jp>
 */

/*
 * Unlike all other mblen() functions, this also looks at the second byte of
 * the input.  However, if you only pass the first byte of a multi-byte
 * string, and \0 as the second byte, this still works in a predictable way:
 * a 4-byte character will be reported as two 2-byte characters.  That's
 * enough for all current uses, as a client-only encoding.  It works that
 * way, because in any valid 4-byte GB18030-encoded character, the third and
 * fourth byte look like a 2-byte encoded character, when looked at
 * separately.
 */
unsafe extern "C" fn pg_gb18030_mblen(s: *const c_uchar) -> c_int {
    let len: c_int;

    if !IS_HIGHBIT_SET(*s) {
        len = 1; /* ASCII */
    } else if *s.add(1) >= 0x30 && *s.add(1) <= 0x39 {
        len = 4;
    } else {
        len = 2;
    }
    len
}

unsafe extern "C" fn pg_gb18030_dsplen(s: *const c_uchar) -> c_int {
    let len: c_int;

    if IS_HIGHBIT_SET(*s) {
        len = 2;
    } else {
        len = pg_ascii_dsplen(s); /* ASCII */
    }
    len
}

/*
 *-------------------------------------------------------------------
 * multibyte sequence validators
 *
 * The verifychar functions accept "s", a pointer to the first byte of a
 * string, and "len", the remaining length of the string.  If there is a
 * validly encoded character beginning at *s, return its length in bytes;
 * else return -1.
 *
 * The verifystr functions also accept "s", a pointer to a string and "len",
 * the length of the string.  They verify the whole string, and return the
 * number of input bytes (<= len) that are valid.  In other words, if the
 * whole string is valid, verifystr returns "len", otherwise it returns the
 * byte offset of the first invalid character.  The verifystr functions must
 * test for and reject zeroes in the input.
 *
 * The verifychar functions can assume that len > 0 and that *s != '\0', but
 * they must test for and reject zeroes in any additional bytes of a
 * multibyte character.  Note that this definition allows the function for a
 * single-byte encoding to be just "return 1".
 *-------------------------------------------------------------------
 */
unsafe extern "C" fn pg_ascii_verifychar(_s: *const c_uchar, _len: c_int) -> c_int {
    1
}

unsafe extern "C" fn pg_ascii_verifystr(s: *const c_uchar, len: c_int) -> c_int {
    let nullpos = memchr_local(s, 0, len as usize);

    if nullpos.is_null() {
        len
    } else {
        nullpos.offset_from(s) as c_int
    }
}

#[inline(always)]
fn IS_EUC_RANGE_VALID(c: u8) -> bool {
    c >= 0xa1 && c <= 0xfe
}

unsafe extern "C" fn pg_eucjp_verifychar(mut s: *const c_uchar, len: c_int) -> c_int {
    let l: c_int;
    let c1: c_uchar;
    let mut c2: c_uchar;

    c1 = *s;
    s = s.add(1);

    match c1 {
        SS2 => {
            /* JIS X 0201 */
            l = 2;
            if l > len {
                return -1;
            }
            c2 = *s;
            s = s.add(1);
            if c2 < 0xa1 || c2 > 0xdf {
                return -1;
            }
        }
        SS3 => {
            /* JIS X 0212 */
            l = 3;
            if l > len {
                return -1;
            }
            c2 = *s;
            s = s.add(1);
            if !IS_EUC_RANGE_VALID(c2) {
                return -1;
            }
            c2 = *s;
            s = s.add(1);
            if !IS_EUC_RANGE_VALID(c2) {
                return -1;
            }
        }
        _ => {
            if IS_HIGHBIT_SET(c1) {
                /* JIS X 0208? */
                l = 2;
                if l > len {
                    return -1;
                }
                if !IS_EUC_RANGE_VALID(c1) {
                    return -1;
                }
                c2 = *s;
                s = s.add(1);
                if !IS_EUC_RANGE_VALID(c2) {
                    return -1;
                }
            } else
            /* must be ASCII */
            {
                l = 1;
            }
        }
    }

    l
}

unsafe extern "C" fn pg_eucjp_verifystr(mut s: *const c_uchar, mut len: c_int) -> c_int {
    let start = s;

    while len > 0 {
        let l: c_int;

        /* fast path for ASCII-subset characters */
        if !IS_HIGHBIT_SET(*s) {
            if *s == b'\0' {
                break;
            }
            l = 1;
        } else {
            l = pg_eucjp_verifychar(s, len);
            if l == -1 {
                break;
            }
        }
        s = s.add(l as usize);
        len -= l;
    }

    s.offset_from(start) as c_int
}

unsafe extern "C" fn pg_euckr_verifychar(mut s: *const c_uchar, len: c_int) -> c_int {
    let l: c_int;
    let c1: c_uchar;
    let c2: c_uchar;

    c1 = *s;
    s = s.add(1);

    if IS_HIGHBIT_SET(c1) {
        l = 2;
        if l > len {
            return -1;
        }
        if !IS_EUC_RANGE_VALID(c1) {
            return -1;
        }
        c2 = *s;
        s = s.add(1);
        if !IS_EUC_RANGE_VALID(c2) {
            return -1;
        }
    } else
    /* must be ASCII */
    {
        l = 1;
    }

    l
}

unsafe extern "C" fn pg_euckr_verifystr(mut s: *const c_uchar, mut len: c_int) -> c_int {
    let start = s;

    while len > 0 {
        let l: c_int;

        /* fast path for ASCII-subset characters */
        if !IS_HIGHBIT_SET(*s) {
            if *s == b'\0' {
                break;
            }
            l = 1;
        } else {
            l = pg_euckr_verifychar(s, len);
            if l == -1 {
                break;
            }
        }
        s = s.add(l as usize);
        len -= l;
    }

    s.offset_from(start) as c_int
}

/* EUC-CN byte sequences are exactly same as EUC-KR */
// #define pg_euccn_verifychar	pg_euckr_verifychar
// #define pg_euccn_verifystr	pg_euckr_verifystr
unsafe extern "C" fn pg_euccn_verifychar(s: *const c_uchar, len: c_int) -> c_int {
    pg_euckr_verifychar(s, len)
}
unsafe extern "C" fn pg_euccn_verifystr(s: *const c_uchar, len: c_int) -> c_int {
    pg_euckr_verifystr(s, len)
}

unsafe extern "C" fn pg_euctw_verifychar(mut s: *const c_uchar, len: c_int) -> c_int {
    let l: c_int;
    let c1: c_uchar;
    let mut c2: c_uchar;

    c1 = *s;
    s = s.add(1);

    match c1 {
        SS2 => {
            /* CNS 11643 Plane 1-7 */
            l = 4;
            if l > len {
                return -1;
            }
            c2 = *s;
            s = s.add(1);
            if c2 < 0xa1 || c2 > 0xa7 {
                return -1;
            }
            c2 = *s;
            s = s.add(1);
            if !IS_EUC_RANGE_VALID(c2) {
                return -1;
            }
            c2 = *s;
            s = s.add(1);
            if !IS_EUC_RANGE_VALID(c2) {
                return -1;
            }
        }
        SS3 => {
            /* unused */
            return -1;
        }
        _ => {
            if IS_HIGHBIT_SET(c1) {
                /* CNS 11643 Plane 1 */
                l = 2;
                if l > len {
                    return -1;
                }
                /* no further range check on c1? */
                c2 = *s;
                s = s.add(1);
                if !IS_EUC_RANGE_VALID(c2) {
                    return -1;
                }
            } else
            /* must be ASCII */
            {
                l = 1;
            }
        }
    }
    l
}

unsafe extern "C" fn pg_euctw_verifystr(mut s: *const c_uchar, mut len: c_int) -> c_int {
    let start = s;

    while len > 0 {
        let l: c_int;

        /* fast path for ASCII-subset characters */
        if !IS_HIGHBIT_SET(*s) {
            if *s == b'\0' {
                break;
            }
            l = 1;
        } else {
            l = pg_euctw_verifychar(s, len);
            if l == -1 {
                break;
            }
        }
        s = s.add(l as usize);
        len -= l;
    }

    s.offset_from(start) as c_int
}

unsafe extern "C" fn pg_johab_verifychar(mut s: *const c_uchar, len: c_int) -> c_int {
    let mut l: c_int;
    let mbl: c_int;

    l = pg_johab_mblen(s);
    mbl = l;

    if len < l {
        return -1;
    }

    if !IS_HIGHBIT_SET(*s) {
        return mbl;
    }

    // C: unsigned char c; while (--l > 0) { c = *++s; ... }
    l -= 1;
    while l > 0 {
        s = s.add(1);
        let c: c_uchar = *s;
        if !IS_EUC_RANGE_VALID(c) {
            return -1;
        }
        l -= 1;
    }
    mbl
}

unsafe extern "C" fn pg_johab_verifystr(mut s: *const c_uchar, mut len: c_int) -> c_int {
    let start = s;

    while len > 0 {
        let l: c_int;

        /* fast path for ASCII-subset characters */
        if !IS_HIGHBIT_SET(*s) {
            if *s == b'\0' {
                break;
            }
            l = 1;
        } else {
            l = pg_johab_verifychar(s, len);
            if l == -1 {
                break;
            }
        }
        s = s.add(l as usize);
        len -= l;
    }

    s.offset_from(start) as c_int
}

unsafe extern "C" fn pg_mule_verifychar(mut s: *const c_uchar, len: c_int) -> c_int {
    let mut l: c_int;
    let mbl: c_int;

    l = pg_mule_mblen(s);
    mbl = l;

    if len < l {
        return -1;
    }

    l -= 1;
    while l > 0 {
        s = s.add(1);
        let c = *s;
        if !IS_HIGHBIT_SET(c) {
            return -1;
        }
        l -= 1;
    }
    mbl
}

unsafe extern "C" fn pg_mule_verifystr(mut s: *const c_uchar, mut len: c_int) -> c_int {
    let start = s;

    while len > 0 {
        let l: c_int;

        /* fast path for ASCII-subset characters */
        if !IS_HIGHBIT_SET(*s) {
            if *s == b'\0' {
                break;
            }
            l = 1;
        } else {
            l = pg_mule_verifychar(s, len);
            if l == -1 {
                break;
            }
        }
        s = s.add(l as usize);
        len -= l;
    }

    s.offset_from(start) as c_int
}

unsafe extern "C" fn pg_latin1_verifychar(_s: *const c_uchar, _len: c_int) -> c_int {
    1
}

unsafe extern "C" fn pg_latin1_verifystr(s: *const c_uchar, len: c_int) -> c_int {
    let nullpos = memchr_local(s, 0, len as usize);

    if nullpos.is_null() {
        len
    } else {
        nullpos.offset_from(s) as c_int
    }
}

unsafe extern "C" fn pg_sjis_verifychar(mut s: *const c_uchar, len: c_int) -> c_int {
    let l: c_int;
    let mbl: c_int;
    let c1: c_uchar;
    let c2: c_uchar;

    l = pg_sjis_mblen(s);
    mbl = l;

    if len < l {
        return -1;
    }

    if l == 1 {
        /* pg_sjis_mblen already verified it */
        return mbl;
    }

    c1 = *s;
    s = s.add(1);
    c2 = *s;
    if !ISSJISHEAD(c1) || !ISSJISTAIL(c2) {
        return -1;
    }
    mbl
}

unsafe extern "C" fn pg_sjis_verifystr(mut s: *const c_uchar, mut len: c_int) -> c_int {
    let start = s;

    while len > 0 {
        let l: c_int;

        /* fast path for ASCII-subset characters */
        if !IS_HIGHBIT_SET(*s) {
            if *s == b'\0' {
                break;
            }
            l = 1;
        } else {
            l = pg_sjis_verifychar(s, len);
            if l == -1 {
                break;
            }
        }
        s = s.add(l as usize);
        len -= l;
    }

    s.offset_from(start) as c_int
}

unsafe extern "C" fn pg_big5_verifychar(mut s: *const c_uchar, len: c_int) -> c_int {
    let mut l: c_int;
    let mbl: c_int;

    l = pg_big5_mblen(s);
    mbl = l;

    if len < l {
        return -1;
    }

    if l == 2 && *s.add(0) == NONUTF8_INVALID_BYTE0 && *s.add(1) == NONUTF8_INVALID_BYTE1 {
        return -1;
    }

    l -= 1;
    while l > 0 {
        s = s.add(1);
        if *s == b'\0' {
            return -1;
        }
        l -= 1;
    }

    mbl
}

unsafe extern "C" fn pg_big5_verifystr(mut s: *const c_uchar, mut len: c_int) -> c_int {
    let start = s;

    while len > 0 {
        let l: c_int;

        /* fast path for ASCII-subset characters */
        if !IS_HIGHBIT_SET(*s) {
            if *s == b'\0' {
                break;
            }
            l = 1;
        } else {
            l = pg_big5_verifychar(s, len);
            if l == -1 {
                break;
            }
        }
        s = s.add(l as usize);
        len -= l;
    }

    s.offset_from(start) as c_int
}

unsafe extern "C" fn pg_gbk_verifychar(mut s: *const c_uchar, len: c_int) -> c_int {
    let mut l: c_int;
    let mbl: c_int;

    l = pg_gbk_mblen(s);
    mbl = l;

    if len < l {
        return -1;
    }

    if l == 2 && *s.add(0) == NONUTF8_INVALID_BYTE0 && *s.add(1) == NONUTF8_INVALID_BYTE1 {
        return -1;
    }

    l -= 1;
    while l > 0 {
        s = s.add(1);
        if *s == b'\0' {
            return -1;
        }
        l -= 1;
    }

    mbl
}

unsafe extern "C" fn pg_gbk_verifystr(mut s: *const c_uchar, mut len: c_int) -> c_int {
    let start = s;

    while len > 0 {
        let l: c_int;

        /* fast path for ASCII-subset characters */
        if !IS_HIGHBIT_SET(*s) {
            if *s == b'\0' {
                break;
            }
            l = 1;
        } else {
            l = pg_gbk_verifychar(s, len);
            if l == -1 {
                break;
            }
        }
        s = s.add(l as usize);
        len -= l;
    }

    s.offset_from(start) as c_int
}

unsafe extern "C" fn pg_uhc_verifychar(mut s: *const c_uchar, len: c_int) -> c_int {
    let mut l: c_int;
    let mbl: c_int;

    l = pg_uhc_mblen(s);
    mbl = l;

    if len < l {
        return -1;
    }

    if l == 2 && *s.add(0) == NONUTF8_INVALID_BYTE0 && *s.add(1) == NONUTF8_INVALID_BYTE1 {
        return -1;
    }

    l -= 1;
    while l > 0 {
        s = s.add(1);
        if *s == b'\0' {
            return -1;
        }
        l -= 1;
    }

    mbl
}

unsafe extern "C" fn pg_uhc_verifystr(mut s: *const c_uchar, mut len: c_int) -> c_int {
    let start = s;

    while len > 0 {
        let l: c_int;

        /* fast path for ASCII-subset characters */
        if !IS_HIGHBIT_SET(*s) {
            if *s == b'\0' {
                break;
            }
            l = 1;
        } else {
            l = pg_uhc_verifychar(s, len);
            if l == -1 {
                break;
            }
        }
        s = s.add(l as usize);
        len -= l;
    }

    s.offset_from(start) as c_int
}

unsafe extern "C" fn pg_gb18030_verifychar(s: *const c_uchar, len: c_int) -> c_int {
    let l: c_int;

    if !IS_HIGHBIT_SET(*s) {
        l = 1; /* ASCII */
    } else if len >= 4 && *s.add(1) >= 0x30 && *s.add(1) <= 0x39 {
        /* Should be 4-byte, validate remaining bytes */
        if *s.add(0) >= 0x81
            && *s.add(0) <= 0xfe
            && *s.add(2) >= 0x81
            && *s.add(2) <= 0xfe
            && *s.add(3) >= 0x30
            && *s.add(3) <= 0x39
        {
            l = 4;
        } else {
            l = -1;
        }
    } else if len >= 2 && *s.add(0) >= 0x81 && *s.add(0) <= 0xfe {
        /* Should be 2-byte, validate */
        if (*s.add(1) >= 0x40 && *s.add(1) <= 0x7e) || (*s.add(1) >= 0x80 && *s.add(1) <= 0xfe) {
            l = 2;
        } else {
            l = -1;
        }
    } else {
        l = -1;
    }
    l
}

unsafe extern "C" fn pg_gb18030_verifystr(mut s: *const c_uchar, mut len: c_int) -> c_int {
    let start = s;

    while len > 0 {
        let l: c_int;

        /* fast path for ASCII-subset characters */
        if !IS_HIGHBIT_SET(*s) {
            if *s == b'\0' {
                break;
            }
            l = 1;
        } else {
            l = pg_gb18030_verifychar(s, len);
            if l == -1 {
                break;
            }
        }
        s = s.add(l as usize);
        len -= l;
    }

    s.offset_from(start) as c_int
}

unsafe extern "C" fn pg_utf8_verifychar(s: *const c_uchar, len: c_int) -> c_int {
    let l: c_int;

    if (*s & 0x80) == 0 {
        if *s == b'\0' {
            return -1;
        }
        return 1;
    } else if (*s & 0xe0) == 0xc0 {
        l = 2;
    } else if (*s & 0xf0) == 0xe0 {
        l = 3;
    } else if (*s & 0xf8) == 0xf0 {
        l = 4;
    } else {
        l = 1;
    }

    if l > len {
        return -1;
    }

    if !pg_utf8_islegal(s, l) {
        return -1;
    }

    l
}

/*
 * The fast path of the UTF-8 verifier uses a deterministic finite automaton
 * (DFA) for multibyte characters. (See the C source for the full discussion of
 * the shift-based DFA and the state numbering rationale.)
 */

/* Error */
const ERR: u32 = 0;
/* Begin */
const BGN: u32 = 11;
/* Continuation states, expect 1/2/3 continuation bytes */
const CS1: u32 = 16;
const CS2: u32 = 1;
const CS3: u32 = 5;
/* Partial states, where the first continuation byte has a restricted range */
const P3A: u32 = 6; /* Lead was E0, check for 3-byte overlong */
const P3B: u32 = 20; /* Lead was ED, check for surrogate */
const P4A: u32 = 25; /* Lead was F0, check for 4-byte overlong */
const P4B: u32 = 30; /* Lead was F4, check for too-large */
/* Begin and End are the same state */
const END: u32 = BGN;

/* the encoded state transitions for the lookup table */

/* ASCII */
const ASC: u32 = END << BGN;
/* 2-byte lead */
const L2A: u32 = CS1 << BGN;
/* 3-byte lead */
const L3A: u32 = P3A << BGN;
const L3B: u32 = CS2 << BGN;
const L3C: u32 = P3B << BGN;
/* 4-byte lead */
const L4A: u32 = P4A << BGN;
const L4B: u32 = CS3 << BGN;
const L4C: u32 = P4B << BGN;
/* continuation byte */
const CR1: u32 = (END << CS1) | (CS1 << CS2) | (CS2 << CS3) | (CS1 << P3B) | (CS2 << P4B);
const CR2: u32 = (END << CS1) | (CS1 << CS2) | (CS2 << CS3) | (CS1 << P3B) | (CS2 << P4A);
const CR3: u32 = (END << CS1) | (CS1 << CS2) | (CS2 << CS3) | (CS1 << P3A) | (CS2 << P4A);
/* invalid byte */
const ILL: u32 = ERR;

static Utf8Transition: [uint32; 256] = [
    /* ASCII */
    ILL, ASC, ASC, ASC, ASC, ASC, ASC, ASC, //
    ASC, ASC, ASC, ASC, ASC, ASC, ASC, ASC, //
    ASC, ASC, ASC, ASC, ASC, ASC, ASC, ASC, //
    ASC, ASC, ASC, ASC, ASC, ASC, ASC, ASC, //
    ASC, ASC, ASC, ASC, ASC, ASC, ASC, ASC, //
    ASC, ASC, ASC, ASC, ASC, ASC, ASC, ASC, //
    ASC, ASC, ASC, ASC, ASC, ASC, ASC, ASC, //
    ASC, ASC, ASC, ASC, ASC, ASC, ASC, ASC, //
    ASC, ASC, ASC, ASC, ASC, ASC, ASC, ASC, //
    ASC, ASC, ASC, ASC, ASC, ASC, ASC, ASC, //
    ASC, ASC, ASC, ASC, ASC, ASC, ASC, ASC, //
    ASC, ASC, ASC, ASC, ASC, ASC, ASC, ASC, //
    ASC, ASC, ASC, ASC, ASC, ASC, ASC, ASC, //
    ASC, ASC, ASC, ASC, ASC, ASC, ASC, ASC, //
    ASC, ASC, ASC, ASC, ASC, ASC, ASC, ASC, //
    ASC, ASC, ASC, ASC, ASC, ASC, ASC, ASC, //
    /* continuation bytes */
    /* 80..8F */
    CR1, CR1, CR1, CR1, CR1, CR1, CR1, CR1, //
    CR1, CR1, CR1, CR1, CR1, CR1, CR1, CR1, //
    /* 90..9F */
    CR2, CR2, CR2, CR2, CR2, CR2, CR2, CR2, //
    CR2, CR2, CR2, CR2, CR2, CR2, CR2, CR2, //
    /* A0..BF */
    CR3, CR3, CR3, CR3, CR3, CR3, CR3, CR3, //
    CR3, CR3, CR3, CR3, CR3, CR3, CR3, CR3, //
    CR3, CR3, CR3, CR3, CR3, CR3, CR3, CR3, //
    CR3, CR3, CR3, CR3, CR3, CR3, CR3, CR3, //
    /* leading bytes */
    /* C0..DF */
    ILL, ILL, L2A, L2A, L2A, L2A, L2A, L2A, //
    L2A, L2A, L2A, L2A, L2A, L2A, L2A, L2A, //
    L2A, L2A, L2A, L2A, L2A, L2A, L2A, L2A, //
    L2A, L2A, L2A, L2A, L2A, L2A, L2A, L2A, //
    /* E0..EF */
    L3A, L3B, L3B, L3B, L3B, L3B, L3B, L3B, //
    L3B, L3B, L3B, L3B, L3B, L3C, L3B, L3B, //
    /* F0..FF */
    L4A, L4B, L4B, L4B, L4C, ILL, ILL, ILL, //
    ILL, ILL, ILL, ILL, ILL, ILL, ILL, ILL, //
];

unsafe fn utf8_advance(mut s: *const c_uchar, state: *mut uint32, mut len: c_int) {
    /* Note: We deliberately don't check the state's value here. */
    while len > 0 {
        /*
         * It's important that the mask value is 31: In most instruction sets,
         * a shift by a 32-bit operand is understood to be a shift by its mod
         * 32, so the compiler should elide the mask operation.
         */
        *state = Utf8Transition[*s as usize] >> (*state & 31);
        s = s.add(1);
        len -= 1;
    }

    *state &= 31;
}

unsafe extern "C" fn pg_utf8_verifystr(mut s: *const c_uchar, mut len: c_int) -> c_int {
    let start = s;
    let orig_len: c_int = len;
    let mut state: uint32 = BGN;

    /*
     * With a stride of two vector widths, gcc will unroll the loop. Even if
     * the compiler can unroll a longer loop, it's not worth it because we
     * must fall back to the byte-wise algorithm if we find any non-ASCII.
     */
    // #define STRIDE_LENGTH (2 * sizeof(Vector8))
    const STRIDE_LENGTH: c_int = 2 * VECTOR8_SIZE as c_int;

    if len >= STRIDE_LENGTH {
        while len >= STRIDE_LENGTH {
            /*
             * If the chunk is all ASCII, we can skip the full UTF-8 check,
             * but we must first check for a non-END state, which means the
             * previous chunk ended in the middle of a multibyte sequence.
             */
            if state != END || !is_valid_ascii(s, STRIDE_LENGTH) {
                utf8_advance(s, &mut state, STRIDE_LENGTH);
            }

            s = s.add(STRIDE_LENGTH as usize);
            len -= STRIDE_LENGTH;
        }

        /* The error state persists, so we only need to check for it here. */
        if state == ERR {
            /*
             * Start over from the beginning with the slow path so we can
             * count the valid bytes.
             */
            len = orig_len;
            s = start;
        } else if state != END {
            /*
             * The fast path exited in the middle of a multibyte sequence.
             * Walk backwards to find the leading byte so that the slow path
             * can resume checking from there. We must always backtrack at
             * least one byte, since the current byte could be e.g. an ASCII
             * byte after a 2-byte lead, which is invalid.
             */
            loop {
                Assert!(s > start);
                s = s.sub(1);
                len += 1;
                Assert!(IS_HIGHBIT_SET(*s));
                if pg_utf_mblen(s) > 1 {
                    break;
                }
            }
        }
    }

    /* check remaining bytes */
    while len > 0 {
        let l: c_int;

        /* fast path for ASCII-subset characters */
        if !IS_HIGHBIT_SET(*s) {
            if *s == b'\0' {
                break;
            }
            l = 1;
        } else {
            l = pg_utf8_verifychar(s, len);
            if l == -1 {
                break;
            }
        }
        s = s.add(l as usize);
        len -= l;
    }

    s.offset_from(start) as c_int
}

/*
 * Check for validity of a single UTF-8 encoded character
 *
 * This directly implements the rules in RFC3629.  The bizarre-looking
 * restrictions on the second byte are meant to ensure that there isn't
 * more than one encoding of a given Unicode character point; that is,
 * you may not use a longer-than-necessary byte sequence with high order
 * zero bits to represent a character that would fit in fewer bytes.
 * To do otherwise is to create security hazards (eg, create an apparent
 * non-ASCII character that decodes to plain ASCII).
 *
 * length is assumed to have been obtained by pg_utf_mblen(), and the
 * caller must have checked that that many bytes are present in the buffer.
 */
pub unsafe extern "C" fn pg_utf8_islegal(source: *const c_uchar, length: c_int) -> bool {
    let mut a: c_uchar;

    // C switch falls through cases 4 -> 3 -> 2 -> 1; replicate with explicit flow.
    match length {
        4 => {
            a = *source.add(3);
            if a < 0x80 || a > 0xBF {
                return false;
            }
            /* FALL THRU */
            a = *source.add(2);
            if a < 0x80 || a > 0xBF {
                return false;
            }
            /* FALL THRU (case 3) */
            a = *source.add(1);
            if !islegal_check_2nd_byte(*source, a) {
                return false;
            }
            /* FALL THRU (case 2) */
            a = *source;
            if a >= 0x80 && a < 0xC2 {
                return false;
            }
            if a > 0xF4 {
                return false;
            }
        }
        3 => {
            a = *source.add(2);
            if a < 0x80 || a > 0xBF {
                return false;
            }
            /* FALL THRU */
            a = *source.add(1);
            if !islegal_check_2nd_byte(*source, a) {
                return false;
            }
            /* FALL THRU (case 2) */
            a = *source;
            if a >= 0x80 && a < 0xC2 {
                return false;
            }
            if a > 0xF4 {
                return false;
            }
        }
        2 => {
            a = *source.add(1);
            if !islegal_check_2nd_byte(*source, a) {
                return false;
            }
            /* FALL THRU */
            a = *source;
            if a >= 0x80 && a < 0xC2 {
                return false;
            }
            if a > 0xF4 {
                return false;
            }
        }
        1 => {
            a = *source;
            if a >= 0x80 && a < 0xC2 {
                return false;
            }
            if a > 0xF4 {
                return false;
            }
        }
        _ => {
            /* default: reject lengths 5 and 6 for now */
            return false;
        }
    }
    true
}

// Helper factoring the C `case 2:` inner switch on `*source` against the second
// byte `a`. Returns true if the second byte is in range for the given lead byte.
#[inline]
fn islegal_check_2nd_byte(source0: c_uchar, a: c_uchar) -> bool {
    match source0 {
        0xE0 => !(a < 0xA0 || a > 0xBF),
        0xED => !(a < 0x80 || a > 0x9F),
        0xF0 => !(a < 0x90 || a > 0xBF),
        0xF4 => !(a < 0x80 || a > 0x8F),
        _ => !(a < 0x80 || a > 0xBF),
    }
}

/*
 * Fills the provided buffer with two bytes such that:
 *   pg_encoding_mblen(dst) == 2 && pg_encoding_verifymbstr(dst) == 0
 */
pub unsafe fn pg_encoding_set_invalid(encoding: c_int, dst: *mut c_char) {
    Assert!(pg_encoding_max_length(encoding) > 1);

    *dst.add(0) = (if encoding == (PG_UTF8 as c_int) {
        0xc0
    } else {
        NONUTF8_INVALID_BYTE0
    }) as c_char;
    *dst.add(1) = NONUTF8_INVALID_BYTE1 as c_char;
}

/*
 *-------------------------------------------------------------------
 * encoding info table
 *-------------------------------------------------------------------
 *
 * In C this is a designated-initializer array `[PG_ENC] = {...}`. We build the
 * Rust equivalent by listing the entries in encoding-enum order (the array is
 * dense over all encodings 0.._PG_LAST_ENCODING_). Client-only encodings have
 * NULL mb2wchar/wchar2mb in C, here represented by `None`.
 */
pub static pg_wchar_table: [pg_wchar_tbl; pg_enc::_PG_LAST_ENCODING_ as usize] = [
    /* [PG_SQL_ASCII] */
    pg_wchar_tbl {
        mb2wchar_with_len: Some(pg_ascii2wchar_with_len),
        wchar2mb_with_len: Some(pg_wchar2single_with_len),
        mblen: Some(pg_ascii_mblen),
        dsplen: Some(pg_ascii_dsplen),
        mbverifychar: Some(pg_ascii_verifychar),
        mbverifystr: Some(pg_ascii_verifystr),
        maxmblen: 1,
    },
    /* [PG_EUC_JP] */
    pg_wchar_tbl {
        mb2wchar_with_len: Some(pg_eucjp2wchar_with_len),
        wchar2mb_with_len: Some(pg_wchar2euc_with_len),
        mblen: Some(pg_eucjp_mblen),
        dsplen: Some(pg_eucjp_dsplen),
        mbverifychar: Some(pg_eucjp_verifychar),
        mbverifystr: Some(pg_eucjp_verifystr),
        maxmblen: 3,
    },
    /* [PG_EUC_CN] */
    pg_wchar_tbl {
        mb2wchar_with_len: Some(pg_euccn2wchar_with_len),
        wchar2mb_with_len: Some(pg_wchar2euc_with_len),
        mblen: Some(pg_euccn_mblen),
        dsplen: Some(pg_euccn_dsplen),
        mbverifychar: Some(pg_euccn_verifychar),
        mbverifystr: Some(pg_euccn_verifystr),
        maxmblen: 3,
    },
    /* [PG_EUC_KR] */
    pg_wchar_tbl {
        mb2wchar_with_len: Some(pg_euckr2wchar_with_len),
        wchar2mb_with_len: Some(pg_wchar2euc_with_len),
        mblen: Some(pg_euckr_mblen),
        dsplen: Some(pg_euckr_dsplen),
        mbverifychar: Some(pg_euckr_verifychar),
        mbverifystr: Some(pg_euckr_verifystr),
        maxmblen: 3,
    },
    /* [PG_EUC_TW] */
    pg_wchar_tbl {
        mb2wchar_with_len: Some(pg_euctw2wchar_with_len),
        wchar2mb_with_len: Some(pg_wchar2euc_with_len),
        mblen: Some(pg_euctw_mblen),
        dsplen: Some(pg_euctw_dsplen),
        mbverifychar: Some(pg_euctw_verifychar),
        mbverifystr: Some(pg_euctw_verifystr),
        maxmblen: 4,
    },
    /* [PG_EUC_JIS_2004] */
    pg_wchar_tbl {
        mb2wchar_with_len: Some(pg_eucjp2wchar_with_len),
        wchar2mb_with_len: Some(pg_wchar2euc_with_len),
        mblen: Some(pg_eucjp_mblen),
        dsplen: Some(pg_eucjp_dsplen),
        mbverifychar: Some(pg_eucjp_verifychar),
        mbverifystr: Some(pg_eucjp_verifystr),
        maxmblen: 3,
    },
    /* [PG_UTF8] */
    pg_wchar_tbl {
        mb2wchar_with_len: Some(pg_utf2wchar_with_len),
        wchar2mb_with_len: Some(pg_wchar2utf_with_len),
        mblen: Some(pg_utf_mblen),
        dsplen: Some(pg_utf_dsplen),
        mbverifychar: Some(pg_utf8_verifychar),
        mbverifystr: Some(pg_utf8_verifystr),
        maxmblen: 4,
    },
    /* [PG_MULE_INTERNAL] */
    pg_wchar_tbl {
        mb2wchar_with_len: Some(pg_mule2wchar_with_len),
        wchar2mb_with_len: Some(pg_wchar2mule_with_len),
        mblen: Some(pg_mule_mblen),
        dsplen: Some(pg_mule_dsplen),
        mbverifychar: Some(pg_mule_verifychar),
        mbverifystr: Some(pg_mule_verifystr),
        maxmblen: 4,
    },
    /* [PG_LATIN1] */
    LATIN1_TBL,
    /* [PG_LATIN2] */
    LATIN1_TBL,
    /* [PG_LATIN3] */
    LATIN1_TBL,
    /* [PG_LATIN4] */
    LATIN1_TBL,
    /* [PG_LATIN5] */
    LATIN1_TBL,
    /* [PG_LATIN6] */
    LATIN1_TBL,
    /* [PG_LATIN7] */
    LATIN1_TBL,
    /* [PG_LATIN8] */
    LATIN1_TBL,
    /* [PG_LATIN9] */
    LATIN1_TBL,
    /* [PG_LATIN10] */
    LATIN1_TBL,
    /* [PG_WIN1256] */
    LATIN1_TBL,
    /* [PG_WIN1258] */
    LATIN1_TBL,
    /* [PG_WIN866] */
    LATIN1_TBL,
    /* [PG_WIN874] */
    LATIN1_TBL,
    /* [PG_KOI8R] */
    LATIN1_TBL,
    /* [PG_WIN1251] */
    LATIN1_TBL,
    /* [PG_WIN1252] */
    LATIN1_TBL,
    /* [PG_ISO_8859_5] */
    LATIN1_TBL,
    /* [PG_ISO_8859_6] */
    LATIN1_TBL,
    /* [PG_ISO_8859_7] */
    LATIN1_TBL,
    /* [PG_ISO_8859_8] */
    LATIN1_TBL,
    /* [PG_WIN1250] */
    LATIN1_TBL,
    /* [PG_WIN1253] */
    LATIN1_TBL,
    /* [PG_WIN1254] */
    LATIN1_TBL,
    /* [PG_WIN1255] */
    LATIN1_TBL,
    /* [PG_WIN1257] */
    LATIN1_TBL,
    /* [PG_KOI8U] */
    LATIN1_TBL,
    /* [PG_SJIS] */
    pg_wchar_tbl {
        mb2wchar_with_len: None,
        wchar2mb_with_len: None,
        mblen: Some(pg_sjis_mblen),
        dsplen: Some(pg_sjis_dsplen),
        mbverifychar: Some(pg_sjis_verifychar),
        mbverifystr: Some(pg_sjis_verifystr),
        maxmblen: 2,
    },
    /* [PG_BIG5] */
    pg_wchar_tbl {
        mb2wchar_with_len: None,
        wchar2mb_with_len: None,
        mblen: Some(pg_big5_mblen),
        dsplen: Some(pg_big5_dsplen),
        mbverifychar: Some(pg_big5_verifychar),
        mbverifystr: Some(pg_big5_verifystr),
        maxmblen: 2,
    },
    /* [PG_GBK] */
    pg_wchar_tbl {
        mb2wchar_with_len: None,
        wchar2mb_with_len: None,
        mblen: Some(pg_gbk_mblen),
        dsplen: Some(pg_gbk_dsplen),
        mbverifychar: Some(pg_gbk_verifychar),
        mbverifystr: Some(pg_gbk_verifystr),
        maxmblen: 2,
    },
    /* [PG_UHC] */
    pg_wchar_tbl {
        mb2wchar_with_len: None,
        wchar2mb_with_len: None,
        mblen: Some(pg_uhc_mblen),
        dsplen: Some(pg_uhc_dsplen),
        mbverifychar: Some(pg_uhc_verifychar),
        mbverifystr: Some(pg_uhc_verifystr),
        maxmblen: 2,
    },
    /* [PG_GB18030] */
    pg_wchar_tbl {
        mb2wchar_with_len: None,
        wchar2mb_with_len: None,
        mblen: Some(pg_gb18030_mblen),
        dsplen: Some(pg_gb18030_dsplen),
        mbverifychar: Some(pg_gb18030_verifychar),
        mbverifystr: Some(pg_gb18030_verifystr),
        maxmblen: 4,
    },
    /* [PG_JOHAB] */
    pg_wchar_tbl {
        mb2wchar_with_len: None,
        wchar2mb_with_len: None,
        mblen: Some(pg_johab_mblen),
        dsplen: Some(pg_johab_dsplen),
        mbverifychar: Some(pg_johab_verifychar),
        mbverifystr: Some(pg_johab_verifystr),
        maxmblen: 3,
    },
    /* [PG_SHIFT_JIS_2004] */
    pg_wchar_tbl {
        mb2wchar_with_len: None,
        wchar2mb_with_len: None,
        mblen: Some(pg_sjis_mblen),
        dsplen: Some(pg_sjis_dsplen),
        mbverifychar: Some(pg_sjis_verifychar),
        mbverifystr: Some(pg_sjis_verifystr),
        maxmblen: 2,
    },
];

// All Latin/single-byte server encodings share this identical entry in C
// ({pg_latin12wchar_with_len, pg_wchar2single_with_len, pg_latin1_mblen,
//   pg_latin1_dsplen, pg_latin1_verifychar, pg_latin1_verifystr, 1}); we name it
// once to keep the table readable while remaining a plain fn-pointer table.
const LATIN1_TBL: pg_wchar_tbl = pg_wchar_tbl {
    mb2wchar_with_len: Some(pg_latin12wchar_with_len),
    wchar2mb_with_len: Some(pg_wchar2single_with_len),
    mblen: Some(pg_latin1_mblen),
    dsplen: Some(pg_latin1_dsplen),
    mbverifychar: Some(pg_latin1_verifychar),
    mbverifystr: Some(pg_latin1_verifystr),
    maxmblen: 1,
};

/*
 * Returns the byte length of a multibyte character.
 *
 * (See the C source for the full description of when pg_encoding_mblen() may be
 * used vs. pg_encoding_mblen_or_incomplete().)
 */
pub unsafe fn pg_encoding_mblen(encoding: c_int, mbstr: *const c_char) -> c_int {
    if PG_VALID_ENCODING(encoding) {
        (pg_wchar_table[encoding as usize].mblen.unwrap())(mbstr as *const c_uchar)
    } else {
        (pg_wchar_table[PG_SQL_ASCII as usize].mblen.unwrap())(mbstr as *const c_uchar)
    }
}

/*
 * Returns the byte length of a multibyte character (possibly not
 * zero-terminated), or INT_MAX if too few bytes remain to determine a length.
 */
pub unsafe fn pg_encoding_mblen_or_incomplete(
    encoding: c_int,
    mbstr: *const c_char,
    remaining: Size,
) -> c_int {
    /*
     * Define zero remaining as too few, even for single-byte encodings.
     * pg_gb18030_mblen() reads one or two bytes; single-byte encodings read
     * zero; others read one.
     */
    if remaining < 1
        || (encoding == (PG_GB18030 as c_int)
            && IS_HIGHBIT_SET(*(mbstr as *const c_uchar))
            && remaining < 2)
    {
        return INT_MAX;
    }
    pg_encoding_mblen(encoding, mbstr)
}

/*
 * Returns the byte length of a multibyte character; but not more than the
 * distance to the terminating zero byte.  For input that might lack a
 * terminating zero, use Min(remaining, pg_encoding_mblen_or_incomplete()).
 */
pub unsafe fn pg_encoding_mblen_bounded(encoding: c_int, mbstr: *const c_char) -> c_int {
    strnlen_local(mbstr, pg_encoding_mblen(encoding, mbstr) as usize) as c_int
}

/*
 * Returns the display length of a multibyte character.
 */
pub unsafe fn pg_encoding_dsplen(encoding: c_int, mbstr: *const c_char) -> c_int {
    if PG_VALID_ENCODING(encoding) {
        (pg_wchar_table[encoding as usize].dsplen.unwrap())(mbstr as *const c_uchar)
    } else {
        (pg_wchar_table[PG_SQL_ASCII as usize].dsplen.unwrap())(mbstr as *const c_uchar)
    }
}

/*
 * Verify the first multibyte character of the given string.
 * Return its byte length if good, -1 if bad.  (See comments above for
 * full details of the mbverifychar API.)
 */
pub unsafe fn pg_encoding_verifymbchar(encoding: c_int, mbstr: *const c_char, len: c_int) -> c_int {
    if PG_VALID_ENCODING(encoding) {
        (pg_wchar_table[encoding as usize].mbverifychar.unwrap())(mbstr as *const c_uchar, len)
    } else {
        (pg_wchar_table[PG_SQL_ASCII as usize].mbverifychar.unwrap())(mbstr as *const c_uchar, len)
    }
}

/*
 * Verify that a string is valid for the given encoding.
 * Returns the number of input bytes (<= len) that form a valid string.
 * (See comments above for full details of the mbverifystr API.)
 */
pub unsafe fn pg_encoding_verifymbstr(encoding: c_int, mbstr: *const c_char, len: c_int) -> c_int {
    if PG_VALID_ENCODING(encoding) {
        (pg_wchar_table[encoding as usize].mbverifystr.unwrap())(mbstr as *const c_uchar, len)
    } else {
        (pg_wchar_table[PG_SQL_ASCII as usize].mbverifystr.unwrap())(mbstr as *const c_uchar, len)
    }
}

/*
 * fetch maximum length of a given encoding
 */
pub fn pg_encoding_max_length(encoding: c_int) -> c_int {
    Assert!(PG_VALID_ENCODING(encoding));

    /*
     * Check for the encoding despite the assert, due to some mingw versions
     * otherwise issuing bogus warnings.
     */
    if PG_VALID_ENCODING(encoding) {
        pg_wchar_table[encoding as usize].maxmblen
    } else {
        pg_wchar_table[PG_SQL_ASCII as usize].maxmblen
    }
}

// ================================================================
//   Local C-stdlib / port shims used above.
// ================================================================

/// C `<limits.h>` INT_MAX, used by pg_encoding_mblen_or_incomplete().
const INT_MAX: c_int = c_int::MAX;

/// memchr(s, c, n): scan the first `n` bytes of `s` for byte `c`; return a
/// pointer to the first match or NULL. (C `memchr` from string.h.)
///
/// # Safety
/// `s` must be valid for reads of `n` bytes.
#[inline]
unsafe fn memchr_local(s: *const c_uchar, c: c_uchar, n: usize) -> *const c_uchar {
    let mut i = 0usize;
    while i < n {
        if *s.add(i) == c {
            return s.add(i);
        }
        i += 1;
    }
    null()
}

/// strnlen(s, maxlen): length of the C string `s`, but no more than `maxlen`.
///
/// # Safety
/// `s` must be valid for reads up to the first NUL or `maxlen` bytes.
#[inline]
unsafe fn strnlen_local(s: *const c_char, maxlen: usize) -> usize {
    let mut i = 0usize;
    while i < maxlen && *s.add(i) != 0 {
        i += 1;
    }
    i
}

// ----------------------------------------------------------------
//   Vector8 / is_valid_ascii (port/simd.h + utils/ascii.h)
//
// Not yet ported as standalone modules. We translate the portable USE_NO_SIMD
// scalar fallback (Vector8 == uint64, 8-byte chunks) so pg_utf8_verifystr()'s
// fast path compiles and matches the C scalar semantics exactly.
// TODO(pg-port): replace with the real port/simd.h + utils/ascii.h translations
// (SIMD widths 16) when those modules are ported.
// ----------------------------------------------------------------

/// `sizeof(Vector8)` for the scalar fallback (uint64) path.
const VECTOR8_SIZE: usize = core::mem::size_of::<uint64>();

/// `vector8_broadcast(c)`: a uint64 with byte `c` in every lane (USE_NO_SIMD).
#[inline(always)]
fn vector8_broadcast(c: u8) -> uint64 {
    (!0u64 / 0xFF) * (c as u64)
}

/*
 * Verify a chunk of bytes for valid ASCII.
 *
 * Returns false if the input contains any zero bytes or bytes with the
 * high-bit set. Input len must be a multiple of the chunk size (8 or 16).
 *
 * This is the USE_NO_SIMD scalar fallback from utils/ascii.h.
 */
unsafe fn is_valid_ascii(mut s: *const c_uchar, len: c_int) -> bool {
    let s_end = s.add(len as usize);
    let mut chunk: uint64;
    let mut highbit_cum: uint64 = vector8_broadcast(0);
    // #ifdef USE_NO_SIMD
    let mut zero_cum: uint64 = vector8_broadcast(0x80);

    Assert!(len as usize % VECTOR8_SIZE == 0);

    while s < s_end {
        // vector8_load(&chunk, s): read 8 bytes (native endianness; only bitwise
        // ops below depend on it, all of which are byte-lane-local).
        chunk = (s as *const uint64).read_unaligned();

        /* Capture any zero bytes in this chunk. */
        // #ifdef USE_NO_SIMD
        /*
         * First, add 0x7f to each byte. This sets the high bit in each byte,
         * unless it was a zero. (See utils/ascii.h for the full rationale.)
         * C arithmetic wraps, so use wrapping_add to match the modular add.
         */
        zero_cum &= chunk.wrapping_add(vector8_broadcast(0x7F));

        /* Capture all set bits in this chunk. */
        highbit_cum |= chunk;

        s = s.add(VECTOR8_SIZE);
    }

    /* Check if any high bits in the high bit accumulator got set. */
    if vector8_is_highbit_set(highbit_cum) {
        return false;
    }

    // #ifdef USE_NO_SIMD
    /* Check if any high bits in the zero accumulator got cleared. */
    if zero_cum != vector8_broadcast(0x80) {
        return false;
    }

    true
}

/// `vector8_is_highbit_set(v)` for the USE_NO_SIMD path: true iff any lane has
/// its high bit set, i.e. `v & 0x8080...80 != 0`.
#[inline(always)]
fn vector8_is_highbit_set(v: uint64) -> bool {
    (v & vector8_broadcast(0x80)) != 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn utf8_roundtrip_and_mblen() {
        unsafe {
            // (codepoint, expected UTF-8 byte length)
            let cases: [(pg_wchar, c_int); 4] = [
                (0x41, 1),    // 'A'
                (0xE9, 2),    // 'é'
                (0x20AC, 3),  // '€'
                (0x1F600, 4), // '😀'
            ];
            for (cp, explen) in cases {
                let mut buf = [0u8; 8];
                // unicode_to_utf8 fills `buf` and returns the original pointer (per C);
                // the byte count comes from unicode_utf8len.
                unicode_to_utf8(cp, buf.as_mut_ptr());
                assert_eq!(unicode_utf8len(cp), explen, "encode len for U+{:X}", cp);

                // pg_encoding_mblen on the encoded bytes must agree.
                let mblen = pg_encoding_mblen(PG_UTF8 as c_int, buf.as_ptr() as *const c_char);
                assert_eq!(mblen, explen, "mblen for U+{:X}", cp);

                // decode back to the original codepoint.
                let decoded = utf8_to_unicode(buf.as_ptr());
                assert_eq!(decoded, cp, "roundtrip U+{:X}", cp);
            }
        }
    }

    #[test]
    fn utf8_display_width() {
        unsafe {
            // Display width via the real Unicode width tables:
            //  ASCII 'A' -> 1, CJK U+4E00 -> 2 (east_asian_fw), combining U+0301 -> 0 (nonspacing).
            let mut buf = [0u8; 8];
            let mut width = |cp: pg_wchar| -> c_int {
                unicode_to_utf8(cp, buf.as_mut_ptr());
                pg_encoding_dsplen(PG_UTF8 as c_int, buf.as_ptr() as *const c_char)
            };
            assert_eq!(width(0x41), 1, "ASCII width");
            assert_eq!(width(0x4E00), 2, "CJK width (east_asian_fw table)");
            assert_eq!(width(0x0301), 0, "combining mark width (nonspacing table)");
        }
    }
}
