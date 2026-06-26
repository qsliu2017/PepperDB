//! Translated from PostgreSQL src/include/mb/pg_wchar.h
//!
//! Multibyte-character support: encoding identifiers, conversion tables, and the
//! wchar API. Server encoding is UTF-8 only in this port, but the full encoding
//! set and signatures are preserved. Inline Unicode helpers are translated in
//! full; bare declarations are stubbed. Conversion tables that C exports as
//! `extern const ... []` become deferred `pub fn` accessors (the data is
//! generated/loaded elsewhere).

/// The pg_wchar type.
pub type pg_wchar = u32;

/// Maximum byte length of multibyte characters in any backend encoding.
pub const MAX_MULTIBYTE_CHAR_LEN: i32 = 4;

// EUC single-shift bytes.
pub const SS2: u8 = 0x8e; // single shift 2 (JIS0201)
pub const SS3: u8 = 0x8f; // single shift 3 (JIS0212)

// SJIS validation.
#[inline]
pub const fn ISSJISHEAD(c: u8) -> bool {
    (c >= 0x81 && c <= 0x9f) || (c >= 0xe0 && c <= 0xfc)
}
#[inline]
pub const fn ISSJISTAIL(c: u8) -> bool {
    (c >= 0x40 && c <= 0x7e) || (c >= 0x80 && c <= 0xfc)
}

// --- MULE Internal Encoding (MIC) charset identifiers ----------------------

// Official single byte encodings - one contiguous block 0x81..=0x8e.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum LcOfficialSingle {
    ISO8859_1 = 0x81,
    ISO8859_2 = 0x82,
    ISO8859_3 = 0x83,
    ISO8859_4 = 0x84,
    TIS620 = 0x85,
    ISO8859_7 = 0x86,
    ISO8859_6 = 0x87,
    ISO8859_8 = 0x88,
    JISX0201K = 0x89,
    JISX0201R = 0x8a,
    KOI8_R = 0x8b,
    ISO8859_5 = 0x8c,
    ISO8859_9 = 0x8d,
    ISO8859_15 = 0x8e,
}
pub use LcOfficialSingle::*;

#[inline]
pub const fn IS_LC1(c: u8) -> bool {
    c >= 0x81 && c <= 0x8d
}

// Official multibyte encodings - one contiguous block 0x90..=0x99.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum LcOfficialMulti {
    JISX0208_1978 = 0x90,
    GB2312_80 = 0x91,
    JISX0208 = 0x92,
    KS5601 = 0x93,
    JISX0212 = 0x94,
    CNS11643_1 = 0x95,
    CNS11643_2 = 0x96,
    JISX0213_1 = 0x97,
    BIG5_1 = 0x98,
    BIG5_2 = 0x99,
}
pub use LcOfficialMulti::*;

#[inline]
pub const fn IS_LC2(c: u8) -> bool {
    c >= 0x90 && c <= 0x99
}

// Private single byte prefix bytes.
pub const LCPRV1_A: u8 = 0x9a;
pub const LCPRV1_B: u8 = 0x9b;
#[inline]
pub const fn IS_LCPRV1(c: u8) -> bool {
    c == LCPRV1_A || c == LCPRV1_B
}
#[inline]
pub const fn IS_LCPRV1_A_RANGE(c: u8) -> bool {
    c >= 0xa0 && c <= 0xdf
}
#[inline]
pub const fn IS_LCPRV1_B_RANGE(c: u8) -> bool {
    c >= 0xe0 && c <= 0xef
}

// Private multibyte prefix bytes.
pub const LCPRV2_A: u8 = 0x9c;
pub const LCPRV2_B: u8 = 0x9d;
#[inline]
pub const fn IS_LCPRV2(c: u8) -> bool {
    c == LCPRV2_A || c == LCPRV2_B
}
#[inline]
pub const fn IS_LCPRV2_A_RANGE(c: u8) -> bool {
    c >= 0xf0 && c <= 0xf4
}
#[inline]
pub const fn IS_LCPRV2_B_RANGE(c: u8) -> bool {
    c >= 0xf5 && c <= 0xfe
}

// Private single byte encodings (0xa0-0xef).
pub const LC_SISHENG: u8 = 0xa0;
pub const LC_IPA: u8 = 0xa1;
pub const LC_VISCII_LOWER: u8 = 0xa2;
pub const LC_VISCII_UPPER: u8 = 0xa3;
pub const LC_ARABIC_DIGIT: u8 = 0xa4;
pub const LC_ARABIC_1_COLUMN: u8 = 0xa5;
pub const LC_ASCII_RIGHT_TO_LEFT: u8 = 0xa6;
pub const LC_LAO: u8 = 0xa7;
pub const LC_ARABIC_2_COLUMN: u8 = 0xa8;

// Private multibyte encodings (0xf0-0xff).
pub const LC_INDIAN_1_COLUMN: u8 = 0xf0;
pub const LC_TIBETAN_1_COLUMN: u8 = 0xf1;
pub const LC_UNICODE_SUBSET_2: u8 = 0xf2;
pub const LC_UNICODE_SUBSET_3: u8 = 0xf3;
pub const LC_UNICODE_SUBSET: u8 = 0xf4;
pub const LC_ETHIOPIC: u8 = 0xf5;
pub const LC_CNS11643_3: u8 = 0xf6;
pub const LC_CNS11643_4: u8 = 0xf7;
pub const LC_CNS11643_5: u8 = 0xf8;
pub const LC_CNS11643_6: u8 = 0xf9;
pub const LC_CNS11643_7: u8 = 0xfa;
pub const LC_INDIAN_2_COLUMN: u8 = 0xfb;
pub const LC_TIBETAN: u8 = 0xfc;

// --- PostgreSQL encoding identifiers ---------------------------------------

/// PostgreSQL encoding IDs. PG_SQL_ASCII must be 0; ordering is ABI-significant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(i32)]
pub enum pg_enc {
    PG_SQL_ASCII = 0,
    PG_EUC_JP,
    PG_EUC_CN,
    PG_EUC_KR,
    PG_EUC_TW,
    PG_EUC_JIS_2004,
    PG_UTF8,
    PG_MULE_INTERNAL,
    PG_LATIN1,
    PG_LATIN2,
    PG_LATIN3,
    PG_LATIN4,
    PG_LATIN5,
    PG_LATIN6,
    PG_LATIN7,
    PG_LATIN8,
    PG_LATIN9,
    PG_LATIN10,
    PG_WIN1256,
    PG_WIN1258,
    PG_WIN866,
    PG_WIN874,
    PG_KOI8R,
    PG_WIN1251,
    PG_WIN1252,
    PG_ISO_8859_5,
    PG_ISO_8859_6,
    PG_ISO_8859_7,
    PG_ISO_8859_8,
    PG_WIN1250,
    PG_WIN1253,
    PG_WIN1254,
    PG_WIN1255,
    PG_WIN1257,
    PG_KOI8U, // PG_ENCODING_BE_LAST points here
    // client encoding only:
    PG_SJIS,
    PG_BIG5,
    PG_GBK,
    PG_UHC,
    PG_GB18030,
    PG_JOHAB,
    PG_SHIFT_JIS_2004,
    _PG_LAST_ENCODING_, // mark only
}
pub use pg_enc::*;

pub const PG_ENCODING_BE_LAST: i32 = pg_enc::PG_KOI8U as i32;

#[inline]
pub const fn PG_VALID_BE_ENCODING(enc: i32) -> bool {
    enc >= 0 && enc <= PG_ENCODING_BE_LAST
}
#[inline]
pub const fn PG_ENCODING_IS_CLIENT_ONLY(enc: i32) -> bool {
    enc > PG_ENCODING_BE_LAST && enc < pg_enc::_PG_LAST_ENCODING_ as i32
}
#[inline]
pub const fn PG_VALID_ENCODING(enc: i32) -> bool {
    enc >= 0 && enc < pg_enc::_PG_LAST_ENCODING_ as i32
}
#[inline]
pub const fn PG_VALID_FE_ENCODING(enc: i32) -> bool {
    PG_VALID_ENCODING(enc)
}

// Conversion sizing constants.
pub const MAX_CONVERSION_GROWTH: i32 = 4;
pub const MAX_CONVERSION_INPUT_LENGTH: i32 = 16;
pub const MAX_UNICODE_EQUIVALENT_STRING: i32 = 16;

/// Encoding number -> name (no WIN32 codepage field on target platforms).
#[derive(Debug, Clone, Copy)]
pub struct pg_enc2name {
    pub name: &'static str,
    pub encoding: pg_enc,
}

/// Accessor for the encoding->name table (data lives in common/encnames).
pub fn pg_enc2name_tbl() -> &'static [pg_enc2name] {
    unimplemented!()
}

/// Accessor for the gettext encoding-name table.
pub fn pg_enc2gettext_tbl() -> &'static [&'static str] {
    unimplemented!()
}

// --- pg_wchar conversion vtable --------------------------------------------

pub type mb2wchar_with_len_converter = fn(from: &[u8], to: &mut [pg_wchar]) -> i32;
pub type wchar2mb_with_len_converter = fn(from: &[pg_wchar], to: &mut [u8]) -> i32;
pub type mblen_converter = fn(mbstr: &[u8]) -> i32;
pub type mbdisplaylen_converter = fn(mbstr: &[u8]) -> i32;
pub type mbcharacter_incrementer = fn(mbstr: &mut [u8]) -> bool;
pub type mbchar_verifier = fn(mbstr: &[u8]) -> i32;
pub type mbstr_verifier = fn(mbstr: &[u8]) -> i32;

/// Per-encoding behaviour table.
pub struct pg_wchar_tbl {
    pub mb2wchar_with_len: mb2wchar_with_len_converter,
    pub wchar2mb_with_len: wchar2mb_with_len_converter,
    pub mblen: mblen_converter,
    pub dsplen: mbdisplaylen_converter,
    pub mbverifychar: mbchar_verifier,
    pub mbverifystr: mbstr_verifier,
    pub maxmblen: i32,
}

/// Accessor for the per-encoding table (data lives in common/wchar).
pub fn pg_wchar_table() -> &'static [pg_wchar_tbl] {
    unimplemented!()
}

// --- UTF-8 <-> local conversion data structures ----------------------------

/// Radix tree for character conversion (four logical trees in one array).
pub struct pg_mb_radix_tree {
    pub chars16: &'static [u16],
    pub chars32: &'static [u32],

    pub b1root: u32,
    pub b1_lower: u8,
    pub b1_upper: u8,

    pub b2root: u32,
    pub b2_1_lower: u8,
    pub b2_1_upper: u8,
    pub b2_2_lower: u8,
    pub b2_2_upper: u8,

    pub b3root: u32,
    pub b3_1_lower: u8,
    pub b3_1_upper: u8,
    pub b3_2_lower: u8,
    pub b3_2_upper: u8,
    pub b3_3_lower: u8,
    pub b3_3_upper: u8,

    pub b4root: u32,
    pub b4_1_lower: u8,
    pub b4_1_upper: u8,
    pub b4_2_lower: u8,
    pub b4_2_upper: u8,
    pub b4_3_lower: u8,
    pub b4_3_upper: u8,
    pub b4_4_lower: u8,
    pub b4_4_upper: u8,
}

/// UTF-8 -> local code map entry (for combined characters).
#[derive(Debug, Clone, Copy)]
pub struct pg_utf_to_local_combined {
    pub utf1: u32,
    pub utf2: u32,
    pub code: u32,
}

/// local code -> UTF-8 map entry (for combined characters).
#[derive(Debug, Clone, Copy)]
pub struct pg_local_to_utf_combined {
    pub code: u32,
    pub utf1: u32,
    pub utf2: u32,
}

/// Algorithmic encoding conversion callback (0 => can't convert).
pub type utf_local_conversion_func = fn(code: u32) -> u32;

// --- inline Unicode helpers (translated in full) ---------------------------

#[inline]
pub fn is_valid_unicode_codepoint(c: pg_wchar) -> bool {
    c > 0 && c <= 0x10FFFF
}

#[inline]
pub fn is_utf16_surrogate_first(c: pg_wchar) -> bool {
    (0xD800..=0xDBFF).contains(&c)
}

#[inline]
pub fn is_utf16_surrogate_second(c: pg_wchar) -> bool {
    (0xDC00..=0xDFFF).contains(&c)
}

#[inline]
pub fn surrogate_pair_to_codepoint(first: pg_wchar, second: pg_wchar) -> pg_wchar {
    ((first & 0x3FF) << 10) + 0x10000 + (second & 0x3FF)
}

/// Convert a UTF-8 character to a code point (no error checks; `c` long enough).
#[inline]
pub fn utf8_to_unicode(c: &[u8]) -> pg_wchar {
    let c0 = c[0];
    if (c0 & 0x80) == 0 {
        pg_wchar::from(c0)
    } else if (c0 & 0xe0) == 0xc0 {
        (pg_wchar::from(c0 & 0x1f) << 6) | pg_wchar::from(c[1] & 0x3f)
    } else if (c0 & 0xf0) == 0xe0 {
        (pg_wchar::from(c0 & 0x0f) << 12)
            | (pg_wchar::from(c[1] & 0x3f) << 6)
            | pg_wchar::from(c[2] & 0x3f)
    } else if (c0 & 0xf8) == 0xf0 {
        (pg_wchar::from(c0 & 0x07) << 18)
            | (pg_wchar::from(c[1] & 0x3f) << 12)
            | (pg_wchar::from(c[2] & 0x3f) << 6)
            | pg_wchar::from(c[3] & 0x3f)
    } else {
        0xffffffff
    }
}

/// Map a Unicode code point to UTF-8. `utf8string` must have at least
/// `unicode_utf8len(c)` bytes.
#[inline]
pub fn unicode_to_utf8(c: pg_wchar, utf8string: &mut [u8]) {
    if c <= 0x7F {
        utf8string[0] = c as u8;
    } else if c <= 0x7FF {
        utf8string[0] = 0xC0 | ((c >> 6) & 0x1F) as u8;
        utf8string[1] = 0x80 | (c & 0x3F) as u8;
    } else if c <= 0xFFFF {
        utf8string[0] = 0xE0 | ((c >> 12) & 0x0F) as u8;
        utf8string[1] = 0x80 | ((c >> 6) & 0x3F) as u8;
        utf8string[2] = 0x80 | (c & 0x3F) as u8;
    } else {
        utf8string[0] = 0xF0 | ((c >> 18) & 0x07) as u8;
        utf8string[1] = 0x80 | ((c >> 12) & 0x3F) as u8;
        utf8string[2] = 0x80 | ((c >> 6) & 0x3F) as u8;
        utf8string[3] = 0x80 | (c & 0x3F) as u8;
    }
}

/// Bytes needed to represent `c` in UTF-8.
#[inline]
pub fn unicode_utf8len(c: pg_wchar) -> i32 {
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

// --- libpq-exported API ----------------------------------------------------

pub fn pg_char_to_encoding(_name: &str) -> i32 {
    unimplemented!()
}
pub fn pg_encoding_to_char(_encoding: i32) -> &'static str {
    unimplemented!()
}
pub fn pg_valid_server_encoding_id(_encoding: i32) -> i32 {
    unimplemented!()
}

// --- libpgcommon frontend-available API ------------------------------------

pub fn pg_encoding_set_invalid(_encoding: i32, _dst: *mut u8) {
    unimplemented!()
}
pub fn pg_encoding_mblen(_encoding: i32, _mbstr: *const u8) -> i32 {
    unimplemented!()
}
pub fn pg_encoding_mblen_or_incomplete(_encoding: i32, _mbstr: *const u8, _remaining: usize) -> i32 {
    unimplemented!()
}
pub fn pg_encoding_mblen_bounded(_encoding: i32, _mbstr: *const u8) -> i32 {
    unimplemented!()
}
pub fn pg_encoding_dsplen(_encoding: i32, _mbstr: *const u8) -> i32 {
    unimplemented!()
}
pub fn pg_encoding_verifymbchar(_encoding: i32, _mbstr: *const u8, _len: i32) -> i32 {
    unimplemented!()
}
pub fn pg_encoding_verifymbstr(_encoding: i32, _mbstr: *const u8, _len: i32) -> i32 {
    unimplemented!()
}
pub fn pg_encoding_max_length(_encoding: i32) -> i32 {
    unimplemented!()
}
pub fn pg_valid_client_encoding(_name: &str) -> i32 {
    unimplemented!()
}
pub fn pg_valid_server_encoding(_name: &str) -> i32 {
    unimplemented!()
}
pub fn is_encoding_supported_by_icu(_encoding: i32) -> bool {
    unimplemented!()
}
pub fn get_encoding_name_for_icu(_encoding: i32) -> &'static str {
    unimplemented!()
}

pub fn pg_utf8_islegal(_source: &[u8]) -> bool {
    unimplemented!()
}
pub fn pg_utf_mblen(_s: *const u8) -> i32 {
    unimplemented!()
}
pub fn pg_mule_mblen(_s: *const u8) -> i32 {
    unimplemented!()
}

// --- backend-only API ------------------------------------------------------

pub fn pg_mb2wchar(_from: *const u8, _to: *mut pg_wchar) -> i32 {
    unimplemented!()
}
pub fn pg_mb2wchar_with_len(_from: &[u8], _to: &mut [pg_wchar]) -> i32 {
    unimplemented!()
}
pub fn pg_encoding_mb2wchar_with_len(_encoding: i32, _from: &[u8], _to: &mut [pg_wchar]) -> i32 {
    unimplemented!()
}
pub fn pg_wchar2mb(_from: *const pg_wchar, _to: *mut u8) -> i32 {
    unimplemented!()
}
pub fn pg_wchar2mb_with_len(_from: &[pg_wchar], _to: &mut [u8]) -> i32 {
    unimplemented!()
}
pub fn pg_encoding_wchar2mb_with_len(_encoding: i32, _from: &[pg_wchar], _to: &mut [u8]) -> i32 {
    unimplemented!()
}
pub fn pg_char_and_wchar_strcmp(_s1: *const u8, _s2: *const pg_wchar) -> i32 {
    unimplemented!()
}
pub fn pg_wchar_strncmp(_s1: *const pg_wchar, _s2: *const pg_wchar, _n: usize) -> i32 {
    unimplemented!()
}
pub fn pg_char_and_wchar_strncmp(_s1: *const u8, _s2: *const pg_wchar, _n: usize) -> i32 {
    unimplemented!()
}
pub fn pg_wchar_strlen(_str: *const pg_wchar) -> usize {
    unimplemented!()
}
pub fn pg_mblen_cstr(_mbstr: *const u8) -> i32 {
    unimplemented!()
}
pub fn pg_mblen_range(_mbstr: *const u8, _end: *const u8) -> i32 {
    unimplemented!()
}
pub fn pg_mblen_with_len(_mbstr: *const u8, _limit: i32) -> i32 {
    unimplemented!()
}
pub fn pg_mblen_unbounded(_mbstr: *const u8) -> i32 {
    unimplemented!()
}

/// deprecated
pub fn pg_mblen(_mbstr: *const u8) -> i32 {
    unimplemented!()
}

pub fn pg_dsplen(_mbstr: *const u8) -> i32 {
    unimplemented!()
}
pub fn pg_mbstrlen(_mbstr: *const u8) -> i32 {
    unimplemented!()
}
pub fn pg_mbstrlen_with_len(_mbstr: *const u8, _limit: i32) -> i32 {
    unimplemented!()
}
pub fn pg_mbcliplen(_mbstr: *const u8, _len: i32, _limit: i32) -> i32 {
    unimplemented!()
}
pub fn pg_encoding_mbcliplen(_encoding: i32, _mbstr: *const u8, _len: i32, _limit: i32) -> i32 {
    unimplemented!()
}
pub fn pg_mbcharcliplen(_mbstr: *const u8, _len: i32, _limit: i32) -> i32 {
    unimplemented!()
}
pub fn pg_database_encoding_max_length() -> i32 {
    unimplemented!()
}
pub fn pg_database_encoding_character_incrementer() -> mbcharacter_incrementer {
    unimplemented!()
}

pub fn PrepareClientEncoding(_encoding: i32) -> i32 {
    unimplemented!()
}
pub fn SetClientEncoding(_encoding: i32) -> i32 {
    unimplemented!()
}
pub fn InitializeClientEncoding() {
    unimplemented!()
}
pub fn pg_get_client_encoding() -> i32 {
    unimplemented!()
}
pub fn pg_get_client_encoding_name() -> &'static str {
    unimplemented!()
}

pub fn SetDatabaseEncoding(_encoding: i32) {
    unimplemented!()
}
pub fn GetDatabaseEncoding() -> i32 {
    unimplemented!()
}
pub fn GetDatabaseEncodingName() -> &'static str {
    unimplemented!()
}
pub fn SetMessageEncoding(_encoding: i32) {
    unimplemented!()
}
pub fn GetMessageEncoding() -> i32 {
    unimplemented!()
}

pub fn pg_do_encoding_conversion(_src: &[u8], _src_encoding: i32, _dest_encoding: i32) -> Vec<u8> {
    unimplemented!()
}
/// Returns the number of bytes written into `dest`, or Err on invalid input.
pub fn pg_do_encoding_conversion_buf(
    _proc: crate::postgres_ext::Oid,
    _src_encoding: i32,
    _dest_encoding: i32,
    _src: &[u8],
    _dest: &mut [u8],
) -> Result<usize, ()> {
    unimplemented!()
}

pub fn pg_client_to_server(_s: *const u8, _len: i32) -> *mut u8 {
    unimplemented!()
}
pub fn pg_server_to_client(_s: *const u8, _len: i32) -> *mut u8 {
    unimplemented!()
}
pub fn pg_any_to_server(_s: *const u8, _len: i32, _encoding: i32) -> *mut u8 {
    unimplemented!()
}
pub fn pg_server_to_any(_s: *const u8, _len: i32, _encoding: i32) -> *mut u8 {
    unimplemented!()
}

pub fn pg_unicode_to_server(_c: pg_wchar, _s: *mut u8) {
    unimplemented!()
}
pub fn pg_unicode_to_server_noerror(_c: pg_wchar, _s: *mut u8) -> bool {
    unimplemented!()
}

pub fn BIG5toCNS(_big5: u16, _lc: *mut u8) -> u16 {
    unimplemented!()
}
pub fn CNStoBIG5(_cns: u16, _lc: u8) -> u16 {
    unimplemented!()
}

pub fn UtfToLocal(
    _utf: *const u8,
    _len: i32,
    _iso: *mut u8,
    _map: &pg_mb_radix_tree,
    _cmap: *const pg_utf_to_local_combined,
    _cmapsize: i32,
    _conv_func: utf_local_conversion_func,
    _encoding: i32,
    _no_error: bool,
) -> i32 {
    unimplemented!()
}
pub fn LocalToUtf(
    _iso: *const u8,
    _len: i32,
    _utf: *mut u8,
    _map: &pg_mb_radix_tree,
    _cmap: *const pg_local_to_utf_combined,
    _cmapsize: i32,
    _conv_func: utf_local_conversion_func,
    _encoding: i32,
    _no_error: bool,
) -> i32 {
    unimplemented!()
}

pub fn pg_verifymbstr(_mbstr: *const u8, _len: i32, _no_error: bool) -> bool {
    unimplemented!()
}
/// Ok if the string is valid in `encoding`, Err otherwise.
pub fn pg_verify_mbstr(_encoding: i32, _mbstr: &[u8]) -> Result<(), ()> {
    unimplemented!()
}
/// Returns the number of valid bytes, or Err on the first invalid character.
pub fn pg_verify_mbstr_len(_encoding: i32, _mbstr: &[u8]) -> Result<usize, ()> {
    unimplemented!()
}

pub fn check_encoding_conversion_args(
    _src_encoding: i32,
    _dest_encoding: i32,
    _len: i32,
    _expected_src_encoding: i32,
    _expected_dest_encoding: i32,
) {
    unimplemented!()
}

/// pg_noreturn in C.
pub fn report_invalid_encoding(_encoding: i32, _mbstr: *const u8, _len: i32) -> ! {
    unimplemented!()
}
/// pg_noreturn in C.
pub fn report_untranslatable_char(
    _src_encoding: i32,
    _dest_encoding: i32,
    _mbstr: *const u8,
    _len: i32,
) -> ! {
    unimplemented!()
}

// Conversion fns: write into `p`, return bytes written or Err on bad input.
// `tab` is an in-memory lookup table.
pub fn local2local(
    _l: &[u8],
    _p: &mut [u8],
    _src_encoding: i32,
    _dest_encoding: i32,
    _tab: &[u8],
) -> Result<usize, ()> {
    unimplemented!()
}
pub fn latin2mic(_l: &[u8], _p: &mut [u8], _lc: i32, _encoding: i32) -> Result<usize, ()> {
    unimplemented!()
}
pub fn mic2latin(_mic: &[u8], _p: &mut [u8], _lc: i32, _encoding: i32) -> Result<usize, ()> {
    unimplemented!()
}
pub fn latin2mic_with_table(
    _l: &[u8],
    _p: &mut [u8],
    _lc: i32,
    _encoding: i32,
    _tab: &[u8],
) -> Result<usize, ()> {
    unimplemented!()
}
pub fn mic2latin_with_table(
    _mic: &[u8],
    _p: &mut [u8],
    _lc: i32,
    _encoding: i32,
    _tab: &[u8],
) -> Result<usize, ()> {
    unimplemented!()
}
