//! Translated from PostgreSQL src/include/common/unicode_norm.h
//! Routines for normalizing Unicode strings (API over unicode_norm_table).

use crate::mb::pg_wchar::pg_wchar;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum UnicodeNormalizationForm {
    UNICODE_NFC = 0,
    UNICODE_NFD = 1,
    UNICODE_NFKC = 2,
    UNICODE_NFKD = 3,
}

/// Quick-check result, see UAX #15.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum UnicodeNormalizationQC {
    UNICODE_NORM_QC_NO = 0,
    UNICODE_NORM_QC_YES = 1,
    UNICODE_NORM_QC_MAYBE = -1,
}

/// Normalize `input` (a code-point sequence) to the given form.
pub fn unicode_normalize(form: UnicodeNormalizationForm, input: &[pg_wchar]) -> Vec<pg_wchar> {
    let _ = (form, input);
    unimplemented!()
}

/// Quick-check whether `input` is already normalized to `form`.
pub fn unicode_is_normalized_quickcheck(
    form: UnicodeNormalizationForm,
    input: &[pg_wchar],
) -> UnicodeNormalizationQC {
    let _ = (form, input);
    unimplemented!()
}
