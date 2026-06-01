//! mb/pg_wchar.h - multibyte-character support.

use std::ffi::{c_char, c_int, c_uint};

use crate::c::{uint8, uint16, uint32, Size};
use crate::postgres_ext::Oid;

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
pub const SS2: c_int = 0x8e; /* single shift 2 (JIS0201) */
pub const SS3: c_int = 0x8f; /* single shift 3 (JIS0212) */

/*
 * SJIS validation macros
 */
#[inline]
pub fn ISSJISHEAD(c: c_int) -> bool {
    (c >= 0x81 && c <= 0x9f) || (c >= 0xe0 && c <= 0xfc)
}
#[inline]
pub fn ISSJISTAIL(c: c_int) -> bool {
    (c >= 0x40 && c <= 0x7e) || (c >= 0x80 && c <= 0xfc)
}

/*
 * Charset IDs for official single byte encodings (0x81-0x8e)
 */
pub const LC_ISO8859_1: c_int = 0x81; /* ISO8859 Latin 1 */
pub const LC_ISO8859_2: c_int = 0x82; /* ISO8859 Latin 2 */
pub const LC_ISO8859_3: c_int = 0x83; /* ISO8859 Latin 3 */
pub const LC_ISO8859_4: c_int = 0x84; /* ISO8859 Latin 4 */
pub const LC_TIS620: c_int = 0x85; /* Thai (not supported yet) */
pub const LC_ISO8859_7: c_int = 0x86; /* Greek (not supported yet) */
pub const LC_ISO8859_6: c_int = 0x87; /* Arabic (not supported yet) */
pub const LC_ISO8859_8: c_int = 0x88; /* Hebrew (not supported yet) */
pub const LC_JISX0201K: c_int = 0x89; /* Japanese 1 byte kana */
pub const LC_JISX0201R: c_int = 0x8a; /* Japanese 1 byte Roman */
/* Note that 0x8b seems to be unused as of Emacs 20.7. */
pub const LC_KOI8_R: c_int = 0x8b; /* Cyrillic KOI8-R */
pub const LC_ISO8859_5: c_int = 0x8c; /* ISO8859 Cyrillic */
pub const LC_ISO8859_9: c_int = 0x8d; /* ISO8859 Latin 5 (not supported yet) */
pub const LC_ISO8859_15: c_int = 0x8e; /* ISO8859 Latin 15 (not supported yet) */
/* #define CONTROL_1 0x8f control characters (unused) */

/* Is a leading byte for "official" single byte encodings? */
#[inline]
pub fn IS_LC1(c: c_int) -> bool {
    (c as u8) >= 0x81 && (c as u8) <= 0x8d
}

/*
 * Charset IDs for official multibyte encodings (0x90-0x99)
 * 0x9a-0x9d are free. 0x9e and 0x9f are reserved.
 */
pub const LC_JISX0208_1978: c_int = 0x90; /* Japanese Kanji, old JIS (not supported) */
pub const LC_GB2312_80: c_int = 0x91; /* Chinese */
pub const LC_JISX0208: c_int = 0x92; /* Japanese Kanji (JIS X 0208) */
pub const LC_KS5601: c_int = 0x93; /* Korean */
pub const LC_JISX0212: c_int = 0x94; /* Japanese Kanji (JIS X 0212) */
pub const LC_CNS11643_1: c_int = 0x95; /* CNS 11643-1992 Plane 1 */
pub const LC_CNS11643_2: c_int = 0x96; /* CNS 11643-1992 Plane 2 */
pub const LC_JISX0213_1: c_int = 0x97; /* Japanese Kanji (JIS X 0213 Plane 1) (not supported) */
pub const LC_BIG5_1: c_int = 0x98; /* Plane 1 Chinese traditional (not supported) */
pub const LC_BIG5_2: c_int = 0x99; /* Plane 1 Chinese traditional (not supported) */

/* Is a leading byte for "official" multibyte encodings? */
#[inline]
pub fn IS_LC2(c: c_int) -> bool {
    (c as u8) >= 0x90 && (c as u8) <= 0x99
}

/*
 * Postgres-specific prefix bytes for "private" single byte encodings
 */
pub const LCPRV1_A: c_int = 0x9a;
pub const LCPRV1_B: c_int = 0x9b;
#[inline]
pub fn IS_LCPRV1(c: c_int) -> bool {
    (c as u8) == LCPRV1_A as u8 || (c as u8) == LCPRV1_B as u8
}
#[inline]
pub fn IS_LCPRV1_A_RANGE(c: c_int) -> bool {
    (c as u8) >= 0xa0 && (c as u8) <= 0xdf
}
#[inline]
pub fn IS_LCPRV1_B_RANGE(c: c_int) -> bool {
    (c as u8) >= 0xe0 && (c as u8) <= 0xef
}

/*
 * Postgres-specific prefix bytes for "private" multibyte encodings
 */
pub const LCPRV2_A: c_int = 0x9c;
pub const LCPRV2_B: c_int = 0x9d;
#[inline]
pub fn IS_LCPRV2(c: c_int) -> bool {
    (c as u8) == LCPRV2_A as u8 || (c as u8) == LCPRV2_B as u8
}
#[inline]
pub fn IS_LCPRV2_A_RANGE(c: c_int) -> bool {
    (c as u8) >= 0xf0 && (c as u8) <= 0xf4
}
#[inline]
pub fn IS_LCPRV2_B_RANGE(c: c_int) -> bool {
    (c as u8) >= 0xf5 && (c as u8) <= 0xfe
}

/*
 * Charset IDs for private single byte encodings (0xa0-0xef)
 */
pub const LC_SISHENG: c_int = 0xa0; /* Chinese SiSheng (not supported) */
pub const LC_IPA: c_int = 0xa1; /* IPA (not supported) */
pub const LC_VISCII_LOWER: c_int = 0xa2; /* Vietnamese VISCII1.1 lower-case (not supported) */
pub const LC_VISCII_UPPER: c_int = 0xa3; /* Vietnamese VISCII1.1 upper-case (not supported) */
pub const LC_ARABIC_DIGIT: c_int = 0xa4; /* Arabic digit (not supported) */
pub const LC_ARABIC_1_COLUMN: c_int = 0xa5; /* Arabic 1-column (not supported) */
pub const LC_ASCII_RIGHT_TO_LEFT: c_int = 0xa6; /* ASCII right-to-left (not supported) */
pub const LC_LAO: c_int = 0xa7; /* Lao characters (not supported) */
pub const LC_ARABIC_2_COLUMN: c_int = 0xa8; /* Arabic 1-column (not supported) */

/*
 * Charset IDs for private multibyte encodings (0xf0-0xff)
 */
pub const LC_INDIAN_1_COLUMN: c_int = 0xf0; /* Indian 1-column glyphs (not supported) */
pub const LC_TIBETAN_1_COLUMN: c_int = 0xf1; /* Tibetan 1-column glyphs (not supported) */
pub const LC_UNICODE_SUBSET_2: c_int = 0xf2; /* Unicode U+2500..U+33FF (not supported) */
pub const LC_UNICODE_SUBSET_3: c_int = 0xf3; /* Unicode U+E000..U+FFFF (not supported) */
pub const LC_UNICODE_SUBSET: c_int = 0xf4; /* Unicode U+0100..U+24FF (not supported) */
pub const LC_ETHIOPIC: c_int = 0xf5; /* Ethiopic (not supported) */
pub const LC_CNS11643_3: c_int = 0xf6; /* CNS 11643-1992 Plane 3 */
pub const LC_CNS11643_4: c_int = 0xf7; /* CNS 11643-1992 Plane 4 */
pub const LC_CNS11643_5: c_int = 0xf8; /* CNS 11643-1992 Plane 5 */
pub const LC_CNS11643_6: c_int = 0xf9; /* CNS 11643-1992 Plane 6 */
pub const LC_CNS11643_7: c_int = 0xfa; /* CNS 11643-1992 Plane 7 */
pub const LC_INDIAN_2_COLUMN: c_int = 0xfb; /* Indian 2-column glyphs (not supported) */
pub const LC_TIBETAN: c_int = 0xfc; /* Tibetan (not supported) */
/* 0xfd, 0xfe, 0xff free (unused) */

/*
 * PostgreSQL encoding identifiers (enum pg_enc)
 */
pub type pg_enc = c_int;
pub const PG_SQL_ASCII: pg_enc = 0; /* SQL/ASCII */
pub const PG_EUC_JP: pg_enc = 1; /* EUC for Japanese */
pub const PG_EUC_CN: pg_enc = 2; /* EUC for Chinese */
pub const PG_EUC_KR: pg_enc = 3; /* EUC for Korean */
pub const PG_EUC_TW: pg_enc = 4; /* EUC for Taiwan */
pub const PG_EUC_JIS_2004: pg_enc = 5; /* EUC-JIS-2004 */
pub const PG_UTF8: pg_enc = 6; /* Unicode UTF8 */
pub const PG_MULE_INTERNAL: pg_enc = 7; /* Mule internal code */
pub const PG_LATIN1: pg_enc = 8; /* ISO-8859-1 Latin 1 */
pub const PG_LATIN2: pg_enc = 9; /* ISO-8859-2 Latin 2 */
pub const PG_LATIN3: pg_enc = 10; /* ISO-8859-3 Latin 3 */
pub const PG_LATIN4: pg_enc = 11; /* ISO-8859-4 Latin 4 */
pub const PG_LATIN5: pg_enc = 12; /* ISO-8859-9 Latin 5 */
pub const PG_LATIN6: pg_enc = 13; /* ISO-8859-10 Latin6 */
pub const PG_LATIN7: pg_enc = 14; /* ISO-8859-13 Latin7 */
pub const PG_LATIN8: pg_enc = 15; /* ISO-8859-14 Latin8 */
pub const PG_LATIN9: pg_enc = 16; /* ISO-8859-15 Latin9 */
pub const PG_LATIN10: pg_enc = 17; /* ISO-8859-16 Latin10 */
pub const PG_WIN1256: pg_enc = 18; /* windows-1256 */
pub const PG_WIN1258: pg_enc = 19; /* Windows-1258 */
pub const PG_WIN866: pg_enc = 20; /* (MS-DOS CP866) */
pub const PG_WIN874: pg_enc = 21; /* windows-874 */
pub const PG_KOI8R: pg_enc = 22; /* KOI8-R */
pub const PG_WIN1251: pg_enc = 23; /* windows-1251 */
pub const PG_WIN1252: pg_enc = 24; /* windows-1252 */
pub const PG_ISO_8859_5: pg_enc = 25; /* ISO-8859-5 */
pub const PG_ISO_8859_6: pg_enc = 26; /* ISO-8859-6 */
pub const PG_ISO_8859_7: pg_enc = 27; /* ISO-8859-7 */
pub const PG_ISO_8859_8: pg_enc = 28; /* ISO-8859-8 */
pub const PG_WIN1250: pg_enc = 29; /* windows-1250 */
pub const PG_WIN1253: pg_enc = 30; /* windows-1253 */
pub const PG_WIN1254: pg_enc = 31; /* windows-1254 */
pub const PG_WIN1255: pg_enc = 32; /* windows-1255 */
pub const PG_WIN1257: pg_enc = 33; /* windows-1257 */
pub const PG_KOI8U: pg_enc = 34; /* KOI8-U */
/* PG_ENCODING_BE_LAST points to the above entry */
/* followings are for client encoding only */
pub const PG_SJIS: pg_enc = 35; /* Shift JIS (Windows-932) */
pub const PG_BIG5: pg_enc = 36; /* Big5 (Windows-950) */
pub const PG_GBK: pg_enc = 37; /* GBK (Windows-936) */
pub const PG_UHC: pg_enc = 38; /* UHC (Windows-949) */
pub const PG_GB18030: pg_enc = 39; /* GB18030 */
pub const PG_JOHAB: pg_enc = 40; /* EUC for Korean JOHAB */
pub const PG_SHIFT_JIS_2004: pg_enc = 41; /* Shift-JIS-2004 */
pub const _PG_LAST_ENCODING_: pg_enc = 42; /* mark only */

pub const PG_ENCODING_BE_LAST: pg_enc = PG_KOI8U;

/*
 * Please use these tests before access to pg_enc2name_tbl[] or others.
 */
#[inline]
pub fn PG_VALID_BE_ENCODING(_enc: c_int) -> bool {
    _enc >= 0 && _enc <= PG_ENCODING_BE_LAST
}

#[inline]
pub fn PG_ENCODING_IS_CLIENT_ONLY(_enc: c_int) -> bool {
    _enc > PG_ENCODING_BE_LAST && _enc < _PG_LAST_ENCODING_
}

#[inline]
pub fn PG_VALID_ENCODING(_enc: c_int) -> bool {
    _enc >= 0 && _enc < _PG_LAST_ENCODING_
}

/* On FE are possible all encodings */
#[inline]
pub fn PG_VALID_FE_ENCODING(_enc: c_int) -> bool {
    PG_VALID_ENCODING(_enc)
}

pub const MAX_CONVERSION_GROWTH: c_int = 4;

pub const MAX_CONVERSION_INPUT_LENGTH: c_int = 16;

pub const MAX_UNICODE_EQUIVALENT_STRING: c_int = 16;

/*
 * Table for mapping an encoding number to official encoding name and
 * possibly other subsidiary data.
 *
 * Note: the WIN32-only 'codepage' field is omitted (non-WIN32 build).
 */
#[repr(C)]
pub struct pg_enc2name {
    pub name: *const c_char,
    pub encoding: pg_enc,
}

extern "C" {
    pub static pg_enc2name_tbl: [pg_enc2name; 0];
    /* Encoding names for gettext */
    pub static pg_enc2gettext_tbl: [*const c_char; 0];
}

/*
 * pg_wchar stuff
 */
pub type mb2wchar_with_len_converter =
    Option<unsafe extern "C" fn(from: *const u8, to: *mut pg_wchar, len: c_int) -> c_int>;

pub type wchar2mb_with_len_converter =
    Option<unsafe extern "C" fn(from: *const pg_wchar, to: *mut u8, len: c_int) -> c_int>;

pub type mblen_converter = Option<unsafe extern "C" fn(mbstr: *const u8) -> c_int>;

pub type mbdisplaylen_converter = Option<unsafe extern "C" fn(mbstr: *const u8) -> c_int>;

pub type mbcharacter_incrementer =
    Option<unsafe extern "C" fn(mbstr: *mut u8, len: c_int) -> bool>;

pub type mbchar_verifier = Option<unsafe extern "C" fn(mbstr: *const u8, len: c_int) -> c_int>;

pub type mbstr_verifier = Option<unsafe extern "C" fn(mbstr: *const u8, len: c_int) -> c_int>;

#[repr(C)]
pub struct pg_wchar_tbl {
    pub mb2wchar_with_len: mb2wchar_with_len_converter, /* convert a multibyte string to a wchar */
    pub wchar2mb_with_len: wchar2mb_with_len_converter, /* convert a wchar string to a multibyte */
    pub mblen: mblen_converter,                         /* get byte length of a char */
    pub dsplen: mbdisplaylen_converter,                 /* get display width of a char */
    pub mbverifychar: mbchar_verifier,                  /* verify multibyte character */
    pub mbverifystr: mbstr_verifier,                    /* verify multibyte string */
    pub maxmblen: c_int,                                /* max bytes for a char in this encoding */
}

extern "C" {
    pub static pg_wchar_table: [pg_wchar_tbl; 0];
}

/*
 * Radix tree for character conversion.
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
    pub b1root: uint32,  /* offset of table in the chars[16|32] array */
    pub b1_lower: uint8, /* min allowed value for a single byte input */
    pub b1_upper: uint8, /* max allowed value for a single byte input */

    /* Radix tree for 2-byte inputs */
    pub b2root: uint32, /* offset of 1st byte's table */
    pub b2_1_lower: uint8,
    pub b2_1_upper: uint8,
    pub b2_2_lower: uint8,
    pub b2_2_upper: uint8,

    /* Radix tree for 3-byte inputs */
    pub b3root: uint32, /* offset of 1st byte's table */
    pub b3_1_lower: uint8,
    pub b3_1_upper: uint8,
    pub b3_2_lower: uint8,
    pub b3_2_upper: uint8,
    pub b3_3_lower: uint8,
    pub b3_3_upper: uint8,

    /* Radix tree for 4-byte inputs */
    pub b4root: uint32, /* offset of 1st byte's table */
    pub b4_1_lower: uint8,
    pub b4_1_upper: uint8,
    pub b4_2_lower: uint8,
    pub b4_2_upper: uint8,
    pub b4_3_lower: uint8,
    pub b4_3_upper: uint8,
    pub b4_4_lower: uint8,
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
 * Support macro for encoding conversion functions to validate their arguments.
 */
#[macro_export]
macro_rules! CHECK_ENCODING_CONVERSION_ARGS {
    ($srcencoding:expr, $destencoding:expr) => {
        $crate::mb::pg_wchar::check_encoding_conversion_args(
            $crate::fmgr::PG_GETARG_INT32(0),
            $crate::fmgr::PG_GETARG_INT32(1),
            $crate::fmgr::PG_GETARG_INT32(4),
            $srcencoding,
            $destencoding,
        )
    };
}

/*
 * Some handy functions for Unicode-specific tests.
 */
#[inline]
pub fn is_valid_unicode_codepoint(c: pg_wchar) -> bool {
    c > 0 && c <= 0x10FFFF
}

#[inline]
pub fn is_utf16_surrogate_first(c: pg_wchar) -> bool {
    c >= 0xD800 && c <= 0xDBFF
}

#[inline]
pub fn is_utf16_surrogate_second(c: pg_wchar) -> bool {
    c >= 0xDC00 && c <= 0xDFFF
}

#[inline]
pub fn surrogate_pair_to_codepoint(first: pg_wchar, second: pg_wchar) -> pg_wchar {
    ((first & 0x3FF) << 10) + 0x10000 + (second & 0x3FF)
}

/*
 * Convert a UTF-8 character to a Unicode code point.
 *
 * No error checks here, c must point to a long-enough string.
 */
#[inline]
pub unsafe fn utf8_to_unicode(c: *const u8) -> pg_wchar {
    if (*c & 0x80) == 0 {
        *c.add(0) as pg_wchar
    } else if (*c & 0xe0) == 0xc0 {
        (((*c.add(0) as pg_wchar & 0x1f) << 6) | (*c.add(1) as pg_wchar & 0x3f)) as pg_wchar
    } else if (*c & 0xf0) == 0xe0 {
        (((*c.add(0) as pg_wchar & 0x0f) << 12)
            | ((*c.add(1) as pg_wchar & 0x3f) << 6)
            | (*c.add(2) as pg_wchar & 0x3f)) as pg_wchar
    } else if (*c & 0xf8) == 0xf0 {
        (((*c.add(0) as pg_wchar & 0x07) << 18)
            | ((*c.add(1) as pg_wchar & 0x3f) << 12)
            | ((*c.add(2) as pg_wchar & 0x3f) << 6)
            | (*c.add(3) as pg_wchar & 0x3f)) as pg_wchar
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
pub unsafe fn unicode_to_utf8(c: pg_wchar, utf8string: *mut u8) -> *mut u8 {
    if c <= 0x7F {
        *utf8string.add(0) = c as u8;
    } else if c <= 0x7FF {
        *utf8string.add(0) = (0xC0 | ((c >> 6) & 0x1F)) as u8;
        *utf8string.add(1) = (0x80 | (c & 0x3F)) as u8;
    } else if c <= 0xFFFF {
        *utf8string.add(0) = (0xE0 | ((c >> 12) & 0x0F)) as u8;
        *utf8string.add(1) = (0x80 | ((c >> 6) & 0x3F)) as u8;
        *utf8string.add(2) = (0x80 | (c & 0x3F)) as u8;
    } else {
        *utf8string.add(0) = (0xF0 | ((c >> 18) & 0x07)) as u8;
        *utf8string.add(1) = (0x80 | ((c >> 12) & 0x3F)) as u8;
        *utf8string.add(2) = (0x80 | ((c >> 6) & 0x3F)) as u8;
        *utf8string.add(3) = (0x80 | (c & 0x3F)) as u8;
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

/*
 * These functions are considered part of libpq's exported API.
 */
pub unsafe fn pg_char_to_encoding(_name: *const c_char) -> c_int {
    unimplemented!()
}
pub unsafe fn pg_encoding_to_char(_encoding: c_int) -> *const c_char {
    unimplemented!()
}
pub unsafe fn pg_valid_server_encoding_id(_encoding: c_int) -> c_int {
    unimplemented!()
}

/*
 * These functions are available to frontend code that links with libpgcommon.
 */
pub unsafe fn pg_encoding_set_invalid(_encoding: c_int, _dst: *mut c_char) {
    unimplemented!()
}
pub unsafe fn pg_encoding_mblen(_encoding: c_int, _mbstr: *const c_char) -> c_int {
    unimplemented!()
}
pub unsafe fn pg_encoding_mblen_or_incomplete(
    _encoding: c_int,
    _mbstr: *const c_char,
    _remaining: Size,
) -> c_int {
    unimplemented!()
}
pub unsafe fn pg_encoding_mblen_bounded(_encoding: c_int, _mbstr: *const c_char) -> c_int {
    unimplemented!()
}
pub unsafe fn pg_encoding_dsplen(_encoding: c_int, _mbstr: *const c_char) -> c_int {
    unimplemented!()
}
pub unsafe fn pg_encoding_verifymbchar(_encoding: c_int, _mbstr: *const c_char, _len: c_int) -> c_int {
    unimplemented!()
}
pub unsafe fn pg_encoding_verifymbstr(_encoding: c_int, _mbstr: *const c_char, _len: c_int) -> c_int {
    unimplemented!()
}
pub unsafe fn pg_encoding_max_length(_encoding: c_int) -> c_int {
    unimplemented!()
}
pub unsafe fn pg_valid_client_encoding(_name: *const c_char) -> c_int {
    unimplemented!()
}
pub unsafe fn pg_valid_server_encoding(_name: *const c_char) -> c_int {
    unimplemented!()
}
pub unsafe fn is_encoding_supported_by_icu(_encoding: c_int) -> bool {
    unimplemented!()
}
pub unsafe fn get_encoding_name_for_icu(_encoding: c_int) -> *const c_char {
    unimplemented!()
}

pub unsafe fn pg_utf8_islegal(_source: *const u8, _length: c_int) -> bool {
    unimplemented!()
}
pub unsafe fn pg_utf_mblen(_s: *const u8) -> c_int {
    unimplemented!()
}
pub unsafe fn pg_mule_mblen(_s: *const u8) -> c_int {
    unimplemented!()
}

/*
 * The remaining functions are backend-only.
 */
pub unsafe fn pg_mb2wchar(_from: *const c_char, _to: *mut pg_wchar) -> c_int {
    unimplemented!()
}
pub unsafe fn pg_mb2wchar_with_len(_from: *const c_char, _to: *mut pg_wchar, _len: c_int) -> c_int {
    unimplemented!()
}
pub unsafe fn pg_encoding_mb2wchar_with_len(
    _encoding: c_int,
    _from: *const c_char,
    _to: *mut pg_wchar,
    _len: c_int,
) -> c_int {
    unimplemented!()
}
pub unsafe fn pg_wchar2mb(_from: *const pg_wchar, _to: *mut c_char) -> c_int {
    unimplemented!()
}
pub unsafe fn pg_wchar2mb_with_len(_from: *const pg_wchar, _to: *mut c_char, _len: c_int) -> c_int {
    unimplemented!()
}
pub unsafe fn pg_encoding_wchar2mb_with_len(
    _encoding: c_int,
    _from: *const pg_wchar,
    _to: *mut c_char,
    _len: c_int,
) -> c_int {
    unimplemented!()
}
pub unsafe fn pg_char_and_wchar_strcmp(_s1: *const c_char, _s2: *const pg_wchar) -> c_int {
    unimplemented!()
}
pub unsafe fn pg_wchar_strncmp(_s1: *const pg_wchar, _s2: *const pg_wchar, _n: Size) -> c_int {
    unimplemented!()
}
pub unsafe fn pg_char_and_wchar_strncmp(
    _s1: *const c_char,
    _s2: *const pg_wchar,
    _n: Size,
) -> c_int {
    unimplemented!()
}
pub unsafe fn pg_wchar_strlen(_str: *const pg_wchar) -> Size {
    unimplemented!()
}
pub unsafe fn pg_mblen_cstr(_mbstr: *const c_char) -> c_int {
    unimplemented!()
}
pub unsafe fn pg_mblen_range(_mbstr: *const c_char, _end: *const c_char) -> c_int {
    unimplemented!()
}
pub unsafe fn pg_mblen_with_len(_mbstr: *const c_char, _limit: c_int) -> c_int {
    unimplemented!()
}
pub unsafe fn pg_mblen_unbounded(_mbstr: *const c_char) -> c_int {
    unimplemented!()
}

/* deprecated */
pub unsafe fn pg_mblen(_mbstr: *const c_char) -> c_int {
    unimplemented!()
}

pub unsafe fn pg_dsplen(_mbstr: *const c_char) -> c_int {
    unimplemented!()
}
pub unsafe fn pg_mbstrlen(_mbstr: *const c_char) -> c_int {
    unimplemented!()
}
pub unsafe fn pg_mbstrlen_with_len(_mbstr: *const c_char, _limit: c_int) -> c_int {
    unimplemented!()
}
pub unsafe fn pg_mbcliplen(_mbstr: *const c_char, _len: c_int, _limit: c_int) -> c_int {
    unimplemented!()
}
pub unsafe fn pg_encoding_mbcliplen(
    _encoding: c_int,
    _mbstr: *const c_char,
    _len: c_int,
    _limit: c_int,
) -> c_int {
    unimplemented!()
}
pub unsafe fn pg_mbcharcliplen(_mbstr: *const c_char, _len: c_int, _limit: c_int) -> c_int {
    unimplemented!()
}
pub unsafe fn pg_database_encoding_max_length() -> c_int {
    unimplemented!()
}
pub unsafe fn pg_database_encoding_character_incrementer() -> mbcharacter_incrementer {
    unimplemented!()
}

pub unsafe fn PrepareClientEncoding(_encoding: c_int) -> c_int {
    unimplemented!()
}
pub unsafe fn SetClientEncoding(_encoding: c_int) -> c_int {
    unimplemented!()
}
pub unsafe fn InitializeClientEncoding() {
    unimplemented!()
}
pub unsafe fn pg_get_client_encoding() -> c_int {
    unimplemented!()
}
pub unsafe fn pg_get_client_encoding_name() -> *const c_char {
    unimplemented!()
}

pub unsafe fn SetDatabaseEncoding(_encoding: c_int) {
    unimplemented!()
}
pub unsafe fn GetDatabaseEncoding() -> c_int {
    unimplemented!()
}
pub unsafe fn GetDatabaseEncodingName() -> *const c_char {
    unimplemented!()
}
pub unsafe fn SetMessageEncoding(_encoding: c_int) {
    unimplemented!()
}
pub unsafe fn GetMessageEncoding() -> c_int {
    unimplemented!()
}

pub unsafe fn pg_do_encoding_conversion(
    _src: *mut u8,
    _len: c_int,
    _src_encoding: c_int,
    _dest_encoding: c_int,
) -> *mut u8 {
    unimplemented!()
}
pub unsafe fn pg_do_encoding_conversion_buf(
    _proc: Oid,
    _src_encoding: c_int,
    _dest_encoding: c_int,
    _src: *mut u8,
    _srclen: c_int,
    _dest: *mut u8,
    _destlen: c_int,
    _noError: bool,
) -> c_int {
    unimplemented!()
}

pub unsafe fn pg_client_to_server(_s: *const c_char, _len: c_int) -> *mut c_char {
    unimplemented!()
}
pub unsafe fn pg_server_to_client(_s: *const c_char, _len: c_int) -> *mut c_char {
    unimplemented!()
}
pub unsafe fn pg_any_to_server(_s: *const c_char, _len: c_int, _encoding: c_int) -> *mut c_char {
    unimplemented!()
}
pub unsafe fn pg_server_to_any(_s: *const c_char, _len: c_int, _encoding: c_int) -> *mut c_char {
    unimplemented!()
}

pub unsafe fn pg_unicode_to_server(_c: pg_wchar, _s: *mut u8) {
    unimplemented!()
}
pub unsafe fn pg_unicode_to_server_noerror(_c: pg_wchar, _s: *mut u8) -> bool {
    unimplemented!()
}

pub unsafe fn BIG5toCNS(_big5: u16, _lc: *mut u8) -> u16 {
    unimplemented!()
}
pub unsafe fn CNStoBIG5(_cns: u16, _lc: u8) -> u16 {
    unimplemented!()
}

pub unsafe fn UtfToLocal(
    _utf: *const u8,
    _len: c_int,
    _iso: *mut u8,
    _map: *const pg_mb_radix_tree,
    _cmap: *const pg_utf_to_local_combined,
    _cmapsize: c_int,
    _conv_func: utf_local_conversion_func,
    _encoding: c_int,
    _noError: bool,
) -> c_int {
    unimplemented!()
}
pub unsafe fn LocalToUtf(
    _iso: *const u8,
    _len: c_int,
    _utf: *mut u8,
    _map: *const pg_mb_radix_tree,
    _cmap: *const pg_local_to_utf_combined,
    _cmapsize: c_int,
    _conv_func: utf_local_conversion_func,
    _encoding: c_int,
    _noError: bool,
) -> c_int {
    unimplemented!()
}

pub unsafe fn pg_verifymbstr(_mbstr: *const c_char, _len: c_int, _noError: bool) -> bool {
    unimplemented!()
}
pub unsafe fn pg_verify_mbstr(
    _encoding: c_int,
    _mbstr: *const c_char,
    _len: c_int,
    _noError: bool,
) -> bool {
    unimplemented!()
}
pub unsafe fn pg_verify_mbstr_len(
    _encoding: c_int,
    _mbstr: *const c_char,
    _len: c_int,
    _noError: bool,
) -> c_int {
    unimplemented!()
}

pub unsafe fn check_encoding_conversion_args(
    _src_encoding: c_int,
    _dest_encoding: c_int,
    _len: c_int,
    _expected_src_encoding: c_int,
    _expected_dest_encoding: c_int,
) {
    unimplemented!()
}

pub unsafe fn report_invalid_encoding(_encoding: c_int, _mbstr: *const c_char, _len: c_int) -> ! {
    unimplemented!()
}
pub unsafe fn report_untranslatable_char(
    _src_encoding: c_int,
    _dest_encoding: c_int,
    _mbstr: *const c_char,
    _len: c_int,
) -> ! {
    unimplemented!()
}

pub unsafe fn local2local(
    _l: *const u8,
    _p: *mut u8,
    _len: c_int,
    _src_encoding: c_int,
    _dest_encoding: c_int,
    _tab: *const u8,
    _noError: bool,
) -> c_int {
    unimplemented!()
}
pub unsafe fn latin2mic(
    _l: *const u8,
    _p: *mut u8,
    _len: c_int,
    _lc: c_int,
    _encoding: c_int,
    _noError: bool,
) -> c_int {
    unimplemented!()
}
pub unsafe fn mic2latin(
    _mic: *const u8,
    _p: *mut u8,
    _len: c_int,
    _lc: c_int,
    _encoding: c_int,
    _noError: bool,
) -> c_int {
    unimplemented!()
}
pub unsafe fn latin2mic_with_table(
    _l: *const u8,
    _p: *mut u8,
    _len: c_int,
    _lc: c_int,
    _encoding: c_int,
    _tab: *const u8,
    _noError: bool,
) -> c_int {
    unimplemented!()
}
pub unsafe fn mic2latin_with_table(
    _mic: *const u8,
    _p: *mut u8,
    _len: c_int,
    _lc: c_int,
    _encoding: c_int,
    _tab: *const u8,
    _noError: bool,
) -> c_int {
    unimplemented!()
}
