//! Translated from PostgreSQL src/include/common/unicode_category.h
//! Routines for determining the category of Unicode characters.

use crate::mb::pg_wchar::pg_wchar;

/// Unicode General Category Values. Numeric values match ICU UCharCategory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum pg_unicode_category {
    PG_U_UNASSIGNED = 0,            // Cn
    PG_U_UPPERCASE_LETTER = 1,      // Lu
    PG_U_LOWERCASE_LETTER = 2,      // Ll
    PG_U_TITLECASE_LETTER = 3,      // Lt
    PG_U_MODIFIER_LETTER = 4,       // Lm
    PG_U_OTHER_LETTER = 5,          // Lo
    PG_U_NONSPACING_MARK = 6,       // Mn
    PG_U_ENCLOSING_MARK = 7,        // Me
    PG_U_SPACING_MARK = 8,          // Mc
    PG_U_DECIMAL_NUMBER = 9,        // Nd
    PG_U_LETTER_NUMBER = 10,        // Nl
    PG_U_OTHER_NUMBER = 11,         // No
    PG_U_SPACE_SEPARATOR = 12,      // Zs
    PG_U_LINE_SEPARATOR = 13,       // Zl
    PG_U_PARAGRAPH_SEPARATOR = 14,  // Zp
    PG_U_CONTROL = 15,              // Cc
    PG_U_FORMAT = 16,               // Cf
    PG_U_PRIVATE_USE = 17,          // Co
    PG_U_SURROGATE = 18,            // Cs
    PG_U_DASH_PUNCTUATION = 19,     // Pd
    PG_U_OPEN_PUNCTUATION = 20,     // Ps
    PG_U_CLOSE_PUNCTUATION = 21,    // Pe
    PG_U_CONNECTOR_PUNCTUATION = 22, // Pc
    PG_U_OTHER_PUNCTUATION = 23,    // Po
    PG_U_MATH_SYMBOL = 24,          // Sm
    PG_U_CURRENCY_SYMBOL = 25,      // Sc
    PG_U_MODIFIER_SYMBOL = 26,      // Sk
    PG_U_OTHER_SYMBOL = 27,         // So
    PG_U_INITIAL_PUNCTUATION = 28,  // Pi
    PG_U_FINAL_PUNCTUATION = 29,    // Pf
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
