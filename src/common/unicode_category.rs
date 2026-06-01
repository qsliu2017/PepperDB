//! Determine general category and character properties of Unicode characters.
//!
//! Translated 1:1 from:
//!   IMPL:   postgres/src/common/unicode_category.c
//!   HEADER: postgres/src/include/common/unicode_category.h
//!           (the EXPOSE_TO_CLIENT_CODE part: PG_U_*_MASK macros,
//!            PG_U_CATEGORY_MASK, PG_U_CHARACTER_TAB, and the
//!            unicode_category_string / unicode_category_abbrev mappings)
//!
//! Encoding must be UTF8, where we assume the pg_wchar representation is a code
//! point. The big lookup tables, the pg_unicode_category values (PG_U_* consts),
//! the PG_U_PROP_* bits, and the pg_category_range / pg_unicode_range /
//! pg_unicode_properties structs are imported from unicode_category_table.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use crate::common::unicode_category_table::*;
use crate::mb::wchar::pg_wchar;
use crate::prelude::*;

// pg_unicode_category is represented as u8 (matching the PG_U_* consts and the
// `category` field of the imported tables).
pub type pg_unicode_category = uint8;

/*
 * Create bitmasks from pg_unicode_category values for efficient comparison of
 * multiple categories. For instance, PG_U_MN_MASK is a bitmask representing
 * the general category Mn; and PG_U_M_MASK represents general categories Mn,
 * Me, and Mc.
 *
 * The number of Unicode General Categories should never grow, so a 32-bit
 * mask is fine.
 */
#[inline]
pub const fn PG_U_CATEGORY_MASK(cat: uint8) -> uint32 {
    1u32 << cat
}

pub const PG_U_LU_MASK: uint32 = PG_U_CATEGORY_MASK(PG_U_UPPERCASE_LETTER);
pub const PG_U_LL_MASK: uint32 = PG_U_CATEGORY_MASK(PG_U_LOWERCASE_LETTER);
pub const PG_U_LT_MASK: uint32 = PG_U_CATEGORY_MASK(PG_U_TITLECASE_LETTER);
pub const PG_U_LC_MASK: uint32 = PG_U_LU_MASK | PG_U_LL_MASK | PG_U_LT_MASK;
pub const PG_U_LM_MASK: uint32 = PG_U_CATEGORY_MASK(PG_U_MODIFIER_LETTER);
pub const PG_U_LO_MASK: uint32 = PG_U_CATEGORY_MASK(PG_U_OTHER_LETTER);
pub const PG_U_L_MASK: uint32 =
    PG_U_LU_MASK | PG_U_LL_MASK | PG_U_LT_MASK | PG_U_LM_MASK | PG_U_LO_MASK;
pub const PG_U_MN_MASK: uint32 = PG_U_CATEGORY_MASK(PG_U_NONSPACING_MARK);
pub const PG_U_ME_MASK: uint32 = PG_U_CATEGORY_MASK(PG_U_ENCLOSING_MARK);
pub const PG_U_MC_MASK: uint32 = PG_U_CATEGORY_MASK(PG_U_SPACING_MARK);
pub const PG_U_M_MASK: uint32 = PG_U_MN_MASK | PG_U_MC_MASK | PG_U_ME_MASK;
pub const PG_U_ND_MASK: uint32 = PG_U_CATEGORY_MASK(PG_U_DECIMAL_NUMBER);
pub const PG_U_NL_MASK: uint32 = PG_U_CATEGORY_MASK(PG_U_LETTER_NUMBER);
pub const PG_U_NO_MASK: uint32 = PG_U_CATEGORY_MASK(PG_U_OTHER_NUMBER);
pub const PG_U_N_MASK: uint32 = PG_U_ND_MASK | PG_U_NL_MASK | PG_U_NO_MASK;
pub const PG_U_PC_MASK: uint32 = PG_U_CATEGORY_MASK(PG_U_CONNECTOR_PUNCTUATION);
pub const PG_U_PD_MASK: uint32 = PG_U_CATEGORY_MASK(PG_U_DASH_PUNCTUATION);
pub const PG_U_PS_MASK: uint32 = PG_U_CATEGORY_MASK(PG_U_OPEN_PUNCTUATION);
pub const PG_U_PE_MASK: uint32 = PG_U_CATEGORY_MASK(PG_U_CLOSE_PUNCTUATION);
pub const PG_U_PI_MASK: uint32 = PG_U_CATEGORY_MASK(PG_U_INITIAL_PUNCTUATION);
pub const PG_U_PF_MASK: uint32 = PG_U_CATEGORY_MASK(PG_U_FINAL_PUNCTUATION);
pub const PG_U_PO_MASK: uint32 = PG_U_CATEGORY_MASK(PG_U_OTHER_PUNCTUATION);
pub const PG_U_P_MASK: uint32 = PG_U_PC_MASK
    | PG_U_PD_MASK
    | PG_U_PS_MASK
    | PG_U_PE_MASK
    | PG_U_PI_MASK
    | PG_U_PF_MASK
    | PG_U_PO_MASK;
pub const PG_U_SM_MASK: uint32 = PG_U_CATEGORY_MASK(PG_U_MATH_SYMBOL);
pub const PG_U_SC_MASK: uint32 = PG_U_CATEGORY_MASK(PG_U_CURRENCY_SYMBOL);
pub const PG_U_SK_MASK: uint32 = PG_U_CATEGORY_MASK(PG_U_MODIFIER_SYMBOL);
pub const PG_U_SO_MASK: uint32 = PG_U_CATEGORY_MASK(PG_U_OTHER_SYMBOL);
pub const PG_U_S_MASK: uint32 = PG_U_SM_MASK | PG_U_SC_MASK | PG_U_SK_MASK | PG_U_SO_MASK;
pub const PG_U_ZS_MASK: uint32 = PG_U_CATEGORY_MASK(PG_U_SPACE_SEPARATOR);
pub const PG_U_ZL_MASK: uint32 = PG_U_CATEGORY_MASK(PG_U_LINE_SEPARATOR);
pub const PG_U_ZP_MASK: uint32 = PG_U_CATEGORY_MASK(PG_U_PARAGRAPH_SEPARATOR);
pub const PG_U_Z_MASK: uint32 = PG_U_ZS_MASK | PG_U_ZL_MASK | PG_U_ZP_MASK;
pub const PG_U_CC_MASK: uint32 = PG_U_CATEGORY_MASK(PG_U_CONTROL);
pub const PG_U_CF_MASK: uint32 = PG_U_CATEGORY_MASK(PG_U_FORMAT);
pub const PG_U_CS_MASK: uint32 = PG_U_CATEGORY_MASK(PG_U_SURROGATE);
pub const PG_U_CO_MASK: uint32 = PG_U_CATEGORY_MASK(PG_U_PRIVATE_USE);
pub const PG_U_CN_MASK: uint32 = PG_U_CATEGORY_MASK(PG_U_UNASSIGNED);
pub const PG_U_C_MASK: uint32 =
    PG_U_CC_MASK | PG_U_CF_MASK | PG_U_CS_MASK | PG_U_CO_MASK | PG_U_CN_MASK;

pub const PG_U_CHARACTER_TAB: pg_wchar = 0x09;

/*
 * Unicode general category for the given codepoint.
 */
pub fn unicode_category(code: pg_wchar) -> pg_unicode_category {
    let mut min: c_int = 0;
    let mut mid: c_int;
    let mut max: c_int = (lengthof!(unicode_categories) as c_int) - 1;

    Assert!(code <= 0x10ffff);

    if code < 0x80 {
        return unicode_opt_ascii[code as usize].category;
    }

    while max >= min {
        mid = (min + max) / 2;
        if code > unicode_categories[mid as usize].last {
            min = mid + 1;
        } else if code < unicode_categories[mid as usize].first {
            max = mid - 1;
        } else {
            return unicode_categories[mid as usize].category;
        }
    }

    PG_U_UNASSIGNED
}

pub fn pg_u_prop_alphabetic(code: pg_wchar) -> bool {
    if code < 0x80 {
        return (unicode_opt_ascii[code as usize].properties & PG_U_PROP_ALPHABETIC) != 0;
    }

    range_search(&unicode_alphabetic, code)
}

pub fn pg_u_prop_lowercase(code: pg_wchar) -> bool {
    if code < 0x80 {
        return (unicode_opt_ascii[code as usize].properties & PG_U_PROP_LOWERCASE) != 0;
    }

    range_search(&unicode_lowercase, code)
}

pub fn pg_u_prop_uppercase(code: pg_wchar) -> bool {
    if code < 0x80 {
        return (unicode_opt_ascii[code as usize].properties & PG_U_PROP_UPPERCASE) != 0;
    }

    range_search(&unicode_uppercase, code)
}

pub fn pg_u_prop_cased(code: pg_wchar) -> bool {
    if code < 0x80 {
        return (unicode_opt_ascii[code as usize].properties & PG_U_PROP_CASED) != 0;
    }

    let category_mask: uint32 = PG_U_CATEGORY_MASK(unicode_category(code));

    (category_mask & PG_U_LT_MASK) != 0 || pg_u_prop_lowercase(code) || pg_u_prop_uppercase(code)
}

pub fn pg_u_prop_case_ignorable(code: pg_wchar) -> bool {
    if code < 0x80 {
        return (unicode_opt_ascii[code as usize].properties & PG_U_PROP_CASE_IGNORABLE) != 0;
    }

    range_search(&unicode_case_ignorable, code)
}

pub fn pg_u_prop_white_space(code: pg_wchar) -> bool {
    if code < 0x80 {
        return (unicode_opt_ascii[code as usize].properties & PG_U_PROP_WHITE_SPACE) != 0;
    }

    range_search(&unicode_white_space, code)
}

pub fn pg_u_prop_hex_digit(code: pg_wchar) -> bool {
    if code < 0x80 {
        return (unicode_opt_ascii[code as usize].properties & PG_U_PROP_HEX_DIGIT) != 0;
    }

    range_search(&unicode_hex_digit, code)
}

pub fn pg_u_prop_join_control(code: pg_wchar) -> bool {
    if code < 0x80 {
        return (unicode_opt_ascii[code as usize].properties & PG_U_PROP_JOIN_CONTROL) != 0;
    }

    range_search(&unicode_join_control, code)
}

/*
 * The following functions implement the Compatibility Properties described
 * at: http://www.unicode.org/reports/tr18/#Compatibility_Properties
 *
 * If 'posix' is true, implements the "POSIX Compatible" variant, otherwise
 * the "Standard" variant.
 */

pub fn pg_u_isdigit(code: pg_wchar, posix: bool) -> bool {
    if posix {
        ('0' as pg_wchar) <= code && code <= ('9' as pg_wchar)
    } else {
        unicode_category(code) == PG_U_DECIMAL_NUMBER
    }
}

pub fn pg_u_isalpha(code: pg_wchar) -> bool {
    pg_u_prop_alphabetic(code)
}

pub fn pg_u_isalnum(code: pg_wchar, posix: bool) -> bool {
    pg_u_isalpha(code) || pg_u_isdigit(code, posix)
}

pub fn pg_u_isword(code: pg_wchar) -> bool {
    let category_mask: uint32 = PG_U_CATEGORY_MASK(unicode_category(code));

    (category_mask & (PG_U_M_MASK | PG_U_ND_MASK | PG_U_PC_MASK)) != 0
        || pg_u_isalpha(code)
        || pg_u_prop_join_control(code)
}

pub fn pg_u_isupper(code: pg_wchar) -> bool {
    pg_u_prop_uppercase(code)
}

pub fn pg_u_islower(code: pg_wchar) -> bool {
    pg_u_prop_lowercase(code)
}

pub fn pg_u_isblank(code: pg_wchar) -> bool {
    code == PG_U_CHARACTER_TAB || unicode_category(code) == PG_U_SPACE_SEPARATOR
}

pub fn pg_u_iscntrl(code: pg_wchar) -> bool {
    unicode_category(code) == PG_U_CONTROL
}

pub fn pg_u_isgraph(code: pg_wchar) -> bool {
    let category_mask: uint32 = PG_U_CATEGORY_MASK(unicode_category(code));

    if (category_mask & (PG_U_CC_MASK | PG_U_CS_MASK | PG_U_CN_MASK)) != 0 || pg_u_isspace(code) {
        return false;
    }
    true
}

pub fn pg_u_isprint(code: pg_wchar) -> bool {
    let category: pg_unicode_category = unicode_category(code);

    if category == PG_U_CONTROL {
        return false;
    }

    pg_u_isgraph(code) || pg_u_isblank(code)
}

pub fn pg_u_ispunct(code: pg_wchar, posix: bool) -> bool {
    let category_mask: uint32;

    if posix {
        if pg_u_isalpha(code) {
            return false;
        }

        category_mask = PG_U_CATEGORY_MASK(unicode_category(code));
        (category_mask & (PG_U_P_MASK | PG_U_S_MASK)) != 0
    } else {
        category_mask = PG_U_CATEGORY_MASK(unicode_category(code));

        (category_mask & PG_U_P_MASK) != 0
    }
}

pub fn pg_u_isspace(code: pg_wchar) -> bool {
    pg_u_prop_white_space(code)
}

pub fn pg_u_isxdigit(code: pg_wchar, posix: bool) -> bool {
    if posix {
        (('0' as pg_wchar) <= code && code <= ('9' as pg_wchar))
            || (('A' as pg_wchar) <= code && code <= ('F' as pg_wchar))
            || (('a' as pg_wchar) <= code && code <= ('f' as pg_wchar))
    } else {
        unicode_category(code) == PG_U_DECIMAL_NUMBER || pg_u_prop_hex_digit(code)
    }
}

/*
 * Description of Unicode general category.
 */
pub fn unicode_category_string(category: pg_unicode_category) -> *const c_char {
    match category {
        PG_U_UNASSIGNED => c"Unassigned".as_ptr(),
        PG_U_UPPERCASE_LETTER => c"Uppercase_Letter".as_ptr(),
        PG_U_LOWERCASE_LETTER => c"Lowercase_Letter".as_ptr(),
        PG_U_TITLECASE_LETTER => c"Titlecase_Letter".as_ptr(),
        PG_U_MODIFIER_LETTER => c"Modifier_Letter".as_ptr(),
        PG_U_OTHER_LETTER => c"Other_Letter".as_ptr(),
        PG_U_NONSPACING_MARK => c"Nonspacing_Mark".as_ptr(),
        PG_U_ENCLOSING_MARK => c"Enclosing_Mark".as_ptr(),
        PG_U_SPACING_MARK => c"Spacing_Mark".as_ptr(),
        PG_U_DECIMAL_NUMBER => c"Decimal_Number".as_ptr(),
        PG_U_LETTER_NUMBER => c"Letter_Number".as_ptr(),
        PG_U_OTHER_NUMBER => c"Other_Number".as_ptr(),
        PG_U_SPACE_SEPARATOR => c"Space_Separator".as_ptr(),
        PG_U_LINE_SEPARATOR => c"Line_Separator".as_ptr(),
        PG_U_PARAGRAPH_SEPARATOR => c"Paragraph_Separator".as_ptr(),
        PG_U_CONTROL => c"Control".as_ptr(),
        PG_U_FORMAT => c"Format".as_ptr(),
        PG_U_PRIVATE_USE => c"Private_Use".as_ptr(),
        PG_U_SURROGATE => c"Surrogate".as_ptr(),
        PG_U_DASH_PUNCTUATION => c"Dash_Punctuation".as_ptr(),
        PG_U_OPEN_PUNCTUATION => c"Open_Punctuation".as_ptr(),
        PG_U_CLOSE_PUNCTUATION => c"Close_Punctuation".as_ptr(),
        PG_U_CONNECTOR_PUNCTUATION => c"Connector_Punctuation".as_ptr(),
        PG_U_OTHER_PUNCTUATION => c"Other_Punctuation".as_ptr(),
        PG_U_MATH_SYMBOL => c"Math_Symbol".as_ptr(),
        PG_U_CURRENCY_SYMBOL => c"Currency_Symbol".as_ptr(),
        PG_U_MODIFIER_SYMBOL => c"Modifier_Symbol".as_ptr(),
        PG_U_OTHER_SYMBOL => c"Other_Symbol".as_ptr(),
        PG_U_INITIAL_PUNCTUATION => c"Initial_Punctuation".as_ptr(),
        PG_U_FINAL_PUNCTUATION => c"Final_Punctuation".as_ptr(),
        _ => {
            Assert!(false);
            c"Unrecognized".as_ptr() /* keep compiler quiet */
        }
    }
}

/*
 * Short code for Unicode general category.
 */
pub fn unicode_category_abbrev(category: pg_unicode_category) -> *const c_char {
    match category {
        PG_U_UNASSIGNED => c"Cn".as_ptr(),
        PG_U_UPPERCASE_LETTER => c"Lu".as_ptr(),
        PG_U_LOWERCASE_LETTER => c"Ll".as_ptr(),
        PG_U_TITLECASE_LETTER => c"Lt".as_ptr(),
        PG_U_MODIFIER_LETTER => c"Lm".as_ptr(),
        PG_U_OTHER_LETTER => c"Lo".as_ptr(),
        PG_U_NONSPACING_MARK => c"Mn".as_ptr(),
        PG_U_ENCLOSING_MARK => c"Me".as_ptr(),
        PG_U_SPACING_MARK => c"Mc".as_ptr(),
        PG_U_DECIMAL_NUMBER => c"Nd".as_ptr(),
        PG_U_LETTER_NUMBER => c"Nl".as_ptr(),
        PG_U_OTHER_NUMBER => c"No".as_ptr(),
        PG_U_SPACE_SEPARATOR => c"Zs".as_ptr(),
        PG_U_LINE_SEPARATOR => c"Zl".as_ptr(),
        PG_U_PARAGRAPH_SEPARATOR => c"Zp".as_ptr(),
        PG_U_CONTROL => c"Cc".as_ptr(),
        PG_U_FORMAT => c"Cf".as_ptr(),
        PG_U_PRIVATE_USE => c"Co".as_ptr(),
        PG_U_SURROGATE => c"Cs".as_ptr(),
        PG_U_DASH_PUNCTUATION => c"Pd".as_ptr(),
        PG_U_OPEN_PUNCTUATION => c"Ps".as_ptr(),
        PG_U_CLOSE_PUNCTUATION => c"Pe".as_ptr(),
        PG_U_CONNECTOR_PUNCTUATION => c"Pc".as_ptr(),
        PG_U_OTHER_PUNCTUATION => c"Po".as_ptr(),
        PG_U_MATH_SYMBOL => c"Sm".as_ptr(),
        PG_U_CURRENCY_SYMBOL => c"Sc".as_ptr(),
        PG_U_MODIFIER_SYMBOL => c"Sk".as_ptr(),
        PG_U_OTHER_SYMBOL => c"So".as_ptr(),
        PG_U_INITIAL_PUNCTUATION => c"Pi".as_ptr(),
        PG_U_FINAL_PUNCTUATION => c"Pf".as_ptr(),
        _ => {
            Assert!(false);
            c"??".as_ptr() /* keep compiler quiet */
        }
    }
}

/*
 * Binary search to test if given codepoint exists in one of the ranges in the
 * given table.
 *
 * The C signature takes (tbl, size, code); in Rust we pass a slice and use
 * tbl.len().
 */
fn range_search(tbl: &[pg_unicode_range], code: pg_wchar) -> bool {
    let mut min: c_int = 0;
    let mut mid: c_int;
    let mut max: c_int = (tbl.len() as c_int) - 1;

    Assert!(code <= 0x10ffff);

    while max >= min {
        mid = (min + max) / 2;
        if code > tbl[mid as usize].last {
            min = mid + 1;
        } else if code < tbl[mid as usize].first {
            max = mid - 1;
        } else {
            return true;
        }
    }

    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_unicode_category() {
        assert_eq!(unicode_category(0x41), PG_U_UPPERCASE_LETTER); // 'A'
        assert_eq!(unicode_category('a' as pg_wchar), PG_U_LOWERCASE_LETTER);
        assert_eq!(unicode_category('5' as pg_wchar), PG_U_DECIMAL_NUMBER);
        assert_eq!(unicode_category(' ' as pg_wchar), PG_U_SPACE_SEPARATOR);
    }

    #[test]
    fn test_isalpha_isdigit() {
        assert!(pg_u_isalpha('A' as pg_wchar));
        assert!(!pg_u_isalpha('5' as pg_wchar));
        assert!(pg_u_isdigit('7' as pg_wchar, false));
        assert!(pg_u_isdigit('7' as pg_wchar, true));
    }

    #[test]
    fn test_non_ascii() {
        // U+00E9 LATIN SMALL LETTER E WITH ACUTE is a lowercase letter.
        assert_eq!(unicode_category(0x00E9), PG_U_LOWERCASE_LETTER);
        assert!(pg_u_islower(0x00E9));
        assert!(pg_u_isalpha(0x00E9));
    }

    #[test]
    fn test_category_abbrev() {
        let p = unicode_category_abbrev(PG_U_UPPERCASE_LETTER);
        let s = unsafe { core::ffi::CStr::from_ptr(p) };
        assert_eq!(s.to_str().unwrap(), "Lu");

        let p = unicode_category_string(PG_U_LOWERCASE_LETTER);
        let s = unsafe { core::ffi::CStr::from_ptr(p) };
        assert_eq!(s.to_str().unwrap(), "Lowercase_Letter");
    }
}
