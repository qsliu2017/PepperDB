//! Translated from PostgreSQL src/include/utils/pg_locale.h
//
// Locale/collation utilities. Server encoding is UTF-8 and the default collation is
// C (byte) collation. The USE_ICU code paths are dropped (no ICU collator), but
// collation handling is NOT silently removed: index sort order is collation-defined
// and persisted, so the strncoll/strnxfrm method surface is preserved.

use crate::postgres_ext::Oid;

pub const LOCALE_NAME_BUFLEN: usize = 128;

// Max output codepoints for full case mapping is 3; buffer sized by codepoint width.
pub const UNICODE_CASEMAP_LEN: usize = 3;
// MAX_MULTIBYTE_CHAR_LEN comes from mb/pg_wchar.h (UTF-8 -> 4). Inlined here as the
// header is not in this batch's include set.
pub const UNICODE_CASEMAP_BUFSZ: usize = UNICODE_CASEMAP_LEN * 4;

// GUC settings -- process globals in C. TODO(global): move to Session/task-local.
pub static mut locale_messages: Option<String> = None;
pub static mut locale_monetary: Option<String> = None;
pub static mut locale_numeric: Option<String> = None;
pub static mut locale_time: Option<String> = None;
pub static mut icu_validation_level: i32 = 0;

// lc_time localization cache.
pub static mut database_ctype_is_c: bool = false;

/// `pg_locale_t` in C is a (nullable) pointer to pg_locale_struct; modeled as an
/// owned/borrowed reference at call sites. Kept as the value type here.
pub type pg_locale_t = PgLocale;

/// Methods that define collation behavior (a routine struct: required strncoll/
/// strnxfrm, optional strnxfrm_prefix). Modeled as a trait per routine-struct.md;
/// `strxfrm_is_safe` is a capability scalar, kept as an associated const.
pub trait CollateMethods {
    const STRXFRM_IS_SAFE: bool;

    /// required
    fn strncoll(&self, arg1: &[u8], arg2: &[u8], locale: &PgLocale) -> i32;

    /// required
    fn strnxfrm(&self, dest: &mut [u8], src: &[u8], locale: &PgLocale) -> usize;

    /// optional (default: not provided)
    fn strnxfrm_prefix(&self, _dest: &mut [u8], _src: &[u8], _locale: &PgLocale) -> Option<usize> {
        None
    }
}

/// Provider-specific locale info. ICU variant dropped (UTF-8 + C/builtin only).
pub enum LocaleInfo {
    Builtin { locale: String, casemap_full: bool },
    // locale_t lt -- libc locale handle. TODO(locale): map to a real libc handle.
    Libc,
}

/// Discriminated locale state (C `struct pg_locale_struct`). In-memory; idiomatic.
pub struct PgLocale {
    pub provider: u8,
    pub deterministic: bool,
    pub collate_is_c: bool,
    pub ctype_is_c: bool,
    pub is_default: bool,
    // `collate` is NULL when collate_is_c. Closed set of impls -> dispatch by enum;
    // kept as Option here since the concrete method table is selected per locale.
    pub collate: Option<CollateProvider>,
    pub info: LocaleInfo,
}

/// Closed set of collation providers (replaces the `const collate_methods *` vtable).
pub enum CollateProvider {
    Builtin,
    Libc,
}

// === Function prototypes ===

pub fn check_locale(_category: i32, _locale: &str) -> Option<String> {
    unimplemented!()
}
pub fn pg_perm_setlocale(_category: i32, _locale: &str) -> Option<String> {
    unimplemented!()
}

/// POSIX lconv (number/money formatting). TODO(locale): model the lconv fields.
pub fn PGLC_localeconv() -> Lconv {
    unimplemented!()
}

/// Placeholder for the libc `struct lconv`. TODO(locale): fill in real fields.
pub struct Lconv;

pub fn cache_locale_time() {
    unimplemented!()
}

pub fn init_database_collation() {
    unimplemented!()
}
pub fn pg_newlocale_from_collation(_collid: Oid) -> PgLocale {
    unimplemented!()
}

pub fn get_collation_actual_version(_collprovider: u8, _collcollate: &str) -> Option<String> {
    unimplemented!()
}

pub fn pg_strlower(_dst: &mut [u8], _src: &[u8], _locale: &PgLocale) -> usize {
    unimplemented!()
}
pub fn pg_strtitle(_dst: &mut [u8], _src: &[u8], _locale: &PgLocale) -> usize {
    unimplemented!()
}
pub fn pg_strupper(_dst: &mut [u8], _src: &[u8], _locale: &PgLocale) -> usize {
    unimplemented!()
}
pub fn pg_strfold(_dst: &mut [u8], _src: &[u8], _locale: &PgLocale) -> usize {
    unimplemented!()
}
pub fn pg_strcoll(_arg1: &str, _arg2: &str, _locale: &PgLocale) -> i32 {
    unimplemented!()
}
pub fn pg_strncoll(_arg1: &[u8], _arg2: &[u8], _locale: &PgLocale) -> i32 {
    unimplemented!()
}
pub fn pg_strxfrm_enabled(_locale: &PgLocale) -> bool {
    unimplemented!()
}
pub fn pg_strxfrm(_dest: &mut [u8], _src: &str, _locale: &PgLocale) -> usize {
    unimplemented!()
}
pub fn pg_strnxfrm(_dest: &mut [u8], _src: &[u8], _locale: &PgLocale) -> usize {
    unimplemented!()
}
pub fn pg_strxfrm_prefix_enabled(_locale: &PgLocale) -> bool {
    unimplemented!()
}
pub fn pg_strxfrm_prefix(_dest: &mut [u8], _src: &str, _locale: &PgLocale) -> usize {
    unimplemented!()
}
pub fn pg_strnxfrm_prefix(_dest: &mut [u8], _src: &[u8], _locale: &PgLocale) -> usize {
    unimplemented!()
}

pub fn builtin_locale_encoding(_locale: &str) -> i32 {
    unimplemented!()
}
pub fn builtin_validate_locale(_encoding: i32, _locale: &str) -> Option<String> {
    unimplemented!()
}
pub fn icu_validate_locale(_loc_str: &str) {
    unimplemented!()
}
pub fn icu_language_tag(_loc_str: &str, _elevel: i32) -> Option<String> {
    unimplemented!()
}
pub fn report_newlocale_failure(_localename: &str) {
    unimplemented!()
}

// wchar2char / char2wchar convert from/to libc's wchar_t (not pg_wchar_t).
pub fn wchar2char(_to: &mut [u8], _from: &[u32], _locale: &PgLocale) -> usize {
    unimplemented!()
}
pub fn char2wchar(_to: &mut [u32], _from: &[u8], _locale: &PgLocale) -> usize {
    unimplemented!()
}
