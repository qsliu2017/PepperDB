//! Translated from PostgreSQL src/include/common/unicode_category.h
//! Routines for determining the category of Unicode characters.

use crate::mb::pg_wchar::pg_wchar;

/// Unicode General Category Values. Numeric values match ICU UCharCategory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum pg_unicode_category {
    UNASSIGNED = 0,            // Cn
    UPPERCASE_LETTER = 1,      // Lu
    LOWERCASE_LETTER = 2,      // Ll
    TITLECASE_LETTER = 3,      // Lt
    MODIFIER_LETTER = 4,       // Lm
    OTHER_LETTER = 5,          // Lo
    NONSPACING_MARK = 6,       // Mn
    ENCLOSING_MARK = 7,        // Me
    SPACING_MARK = 8,          // Mc
    DECIMAL_NUMBER = 9,        // Nd
    LETTER_NUMBER = 10,        // Nl
    OTHER_NUMBER = 11,         // No
    SPACE_SEPARATOR = 12,      // Zs
    LINE_SEPARATOR = 13,       // Zl
    PARAGRAPH_SEPARATOR = 14,  // Zp
    CONTROL = 15,              // Cc
    FORMAT = 16,               // Cf
    PRIVATE_USE = 17,          // Co
    SURROGATE = 18,            // Cs
    DASH_PUNCTUATION = 19,     // Pd
    OPEN_PUNCTUATION = 20,     // Ps
    CLOSE_PUNCTUATION = 21,    // Pe
    CONNECTOR_PUNCTUATION = 22, // Pc
    OTHER_PUNCTUATION = 23,    // Po
    MATH_SYMBOL = 24,          // Sm
    CURRENCY_SYMBOL = 25,      // Sc
    MODIFIER_SYMBOL = 26,      // Sk
    OTHER_SYMBOL = 27,         // So
    INITIAL_PUNCTUATION = 28,  // Pi
    FINAL_PUNCTUATION = 29,    // Pf
}

pub fn unicode_category(code: pg_wchar) -> pg_unicode_category {
    let _ = code;
    unimplemented!()
}

pub fn unicode_category_string(category: pg_unicode_category) -> &'static str {
    let _ = category;
    unimplemented!()
}

pub fn unicode_category_abbrev(category: pg_unicode_category) -> &'static str {
    let _ = category;
    unimplemented!()
}

pub fn pg_u_prop_alphabetic(code: pg_wchar) -> bool {
    let _ = code;
    unimplemented!()
}
pub fn pg_u_prop_lowercase(code: pg_wchar) -> bool {
    let _ = code;
    unimplemented!()
}
pub fn pg_u_prop_uppercase(code: pg_wchar) -> bool {
    let _ = code;
    unimplemented!()
}
pub fn pg_u_prop_cased(code: pg_wchar) -> bool {
    let _ = code;
    unimplemented!()
}
pub fn pg_u_prop_case_ignorable(code: pg_wchar) -> bool {
    let _ = code;
    unimplemented!()
}
pub fn pg_u_prop_white_space(code: pg_wchar) -> bool {
    let _ = code;
    unimplemented!()
}
pub fn pg_u_prop_hex_digit(code: pg_wchar) -> bool {
    let _ = code;
    unimplemented!()
}
pub fn pg_u_prop_join_control(code: pg_wchar) -> bool {
    let _ = code;
    unimplemented!()
}

pub fn pg_u_isdigit(code: pg_wchar, posix: bool) -> bool {
    let _ = (code, posix);
    unimplemented!()
}
pub fn pg_u_isalpha(code: pg_wchar) -> bool {
    let _ = code;
    unimplemented!()
}
pub fn pg_u_isalnum(code: pg_wchar, posix: bool) -> bool {
    let _ = (code, posix);
    unimplemented!()
}
pub fn pg_u_isword(code: pg_wchar) -> bool {
    let _ = code;
    unimplemented!()
}
pub fn pg_u_isupper(code: pg_wchar) -> bool {
    let _ = code;
    unimplemented!()
}
pub fn pg_u_islower(code: pg_wchar) -> bool {
    let _ = code;
    unimplemented!()
}
pub fn pg_u_isblank(code: pg_wchar) -> bool {
    let _ = code;
    unimplemented!()
}
pub fn pg_u_iscntrl(code: pg_wchar) -> bool {
    let _ = code;
    unimplemented!()
}
pub fn pg_u_isgraph(code: pg_wchar) -> bool {
    let _ = code;
    unimplemented!()
}
pub fn pg_u_isprint(code: pg_wchar) -> bool {
    let _ = code;
    unimplemented!()
}
pub fn pg_u_ispunct(code: pg_wchar, posix: bool) -> bool {
    let _ = (code, posix);
    unimplemented!()
}
pub fn pg_u_isspace(code: pg_wchar) -> bool {
    let _ = code;
    unimplemented!()
}
pub fn pg_u_isxdigit(code: pg_wchar, posix: bool) -> bool {
    let _ = (code, posix);
    unimplemented!()
}
