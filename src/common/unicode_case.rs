//! Unicode case mapping and case conversion.
//!
//! Translated 1:1 from:
//!   IMPL:   postgres/src/common/unicode_case.c
//!   HEADER: postgres/src/include/common/unicode_case.h
//!           (the public WordBoundaryNext typedef + the unicode_*_simple /
//!            unicode_str{lower,title,upper,fold} prototypes)
//!
//! The lookup tables (MAX_CASE_EXPANSION, PG_U_FINAL_SIGMA, CaseLower/Title/
//! Upper/Fold/NCaseKind, pg_special_case, special_case[], case_map_{lower,
//! title,upper,fold}[], case_map_special[], case_map[]) are imported from
//! unicode_case_table. The `case_index()` helper lives in the C header
//! (unicode_case_table.h) but is NOT part of the generated table module, so it
//! is translated here.
//!
//! Strings are assumed to be UTF-8, where the pg_wchar representation is a code
//! point.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use crate::common::unicode_case_table::*;
use crate::common::unicode_category::{pg_u_prop_case_ignorable, pg_u_prop_cased};
use crate::mb::wchar::{pg_wchar, unicode_to_utf8, unicode_utf8len, utf8_to_unicode};
use crate::prelude::*;

extern "C" {
    fn memcpy(dst: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
}

/// Callback that returns the offset of the next word boundary. Mirrors the
/// C typedef `size_t (*WordBoundaryNext)(void *wbstate)`. We model it as a
/// boxed Rust closure operating over an opaque state pointer.
pub type WordBoundaryNext = fn(wbstate: *mut c_void) -> usize;

/// Result of mapping a single character (mirrors the C `enum CaseMapResult`).
#[derive(Clone, Copy, PartialEq, Eq)]
enum CaseMapResult {
    Self_,
    Simple,
    Special,
}

/*
 * Map for each case kind.
 *
 * In C this is `static const pg_wchar *const casekind_map[NCaseKind]`. In Rust
 * we resolve the per-kind table by index at the call sites via this helper,
 * which avoids storing references to statics in a static array.
 */
fn casekind_map(casekind: usize) -> &'static [u32] {
    match casekind {
        CaseLower => &case_map_lower,
        CaseTitle => &case_map_title,
        CaseUpper => &case_map_upper,
        CaseFold => &case_map_fold,
        _ => {
            Assert!(false);
            &case_map_lower
        }
    }
}

/*
 * case_index()
 *
 * Given a code point, compute the index in the case_map at which we can find
 * the offset into the mapping tables.
 *
 * Translated from the static inline in src/include/common/unicode_case_table.h.
 */
fn case_index(cp: pg_wchar) -> uint16 {
    /* Fast path for codepoints < 0x0588 */
    if cp < 0x0588 {
        return case_map[cp as usize];
    }

    if cp < 0xABC0 {
        if cp < 0x2185 {
            if cp >= 0x10A0 && cp < 0x1100 {
                return case_map[(cp - 0x10A0 + 1416) as usize];
            } else if cp >= 0x13A0 {
                if cp < 0x13FE {
                    return case_map[(cp - 0x13A0 + 1512) as usize];
                } else if cp >= 0x1C80 {
                    return case_map[(cp - 0x1C80 + 1606) as usize];
                }
            }
        } else if cp >= 0x24B6 {
            if cp < 0x2D2E {
                if cp < 0x24EA {
                    return case_map[(cp - 0x24B6 + 2891) as usize];
                } else if cp >= 0x2C00 {
                    return case_map[(cp - 0x2C00 + 2943) as usize];
                }
            } else if cp >= 0xA640 {
                if cp < 0xA7F7 {
                    return case_map[(cp - 0xA640 + 3245) as usize];
                } else if cp >= 0xAB53 {
                    return case_map[(cp - 0xAB53 + 3684) as usize];
                }
            }
        }
    } else if cp >= 0xFB00 {
        if cp < 0x10D86 {
            if cp < 0xFF5B {
                if cp < 0xFB18 {
                    return case_map[(cp - 0xFB00 + 3793) as usize];
                } else if cp >= 0xFF21 {
                    return case_map[(cp - 0xFF21 + 3817) as usize];
                }
            } else if cp >= 0x10400 {
                if cp < 0x105BD {
                    return case_map[(cp - 0x10400 + 3875) as usize];
                } else if cp >= 0x10C80 {
                    return case_map[(cp - 0x10C80 + 4320) as usize];
                }
            }
        } else if cp >= 0x118A0 {
            if cp < 0x16E80 {
                if cp < 0x118E0 {
                    return case_map[(cp - 0x118A0 + 4582) as usize];
                } else if cp >= 0x16E40 {
                    return case_map[(cp - 0x16E40 + 4646) as usize];
                }
            } else if cp >= 0x1E900 {
                if cp < 0x1E944 {
                    return case_map[(cp - 0x1E900 + 4710) as usize];
                }
            }
        }
    }

    0
}

pub fn unicode_lowercase_simple(code: pg_wchar) -> pg_wchar {
    let cp = find_case_map(code, &case_map_lower);

    if cp != 0 {
        cp
    } else {
        code
    }
}

pub fn unicode_titlecase_simple(code: pg_wchar) -> pg_wchar {
    let cp = find_case_map(code, &case_map_title);

    if cp != 0 {
        cp
    } else {
        code
    }
}

pub fn unicode_uppercase_simple(code: pg_wchar) -> pg_wchar {
    let cp = find_case_map(code, &case_map_upper);

    if cp != 0 {
        cp
    } else {
        code
    }
}

pub fn unicode_casefold_simple(code: pg_wchar) -> pg_wchar {
    let cp = find_case_map(code, &case_map_fold);

    if cp != 0 {
        cp
    } else {
        code
    }
}

/*
 * unicode_strlower()
 *
 * Convert src to lowercase, and return the result length (not including
 * terminating NUL).
 *
 * String src must be encoded in UTF-8. If srclen < 0, src must be
 * NUL-terminated.
 *
 * Result string is stored in dst, truncating if larger than dstsize. If
 * dstsize is greater than the result length, dst will be NUL-terminated;
 * otherwise not.
 *
 * If dstsize is zero, dst may be NULL. This is useful for calculating the
 * required buffer size before allocating.
 *
 * If full is true, use special case mappings if available and if the
 * conditions are satisfied.
 */
pub unsafe fn unicode_strlower(
    dst: *mut c_char,
    dstsize: usize,
    src: *const c_char,
    srclen: isize,
    full: bool,
) -> usize {
    convert_case(dst, dstsize, src, srclen, CaseLower, full, None, core::ptr::null_mut())
}

/*
 * unicode_strtitle()
 *
 * Convert src to titlecase, and return the result length (not including
 * terminating NUL).
 *
 * See the C source for the full contract; titlecasing requires word boundary
 * information provided by the callback wbnext, with state wbstate owned by the
 * caller.
 */
pub unsafe fn unicode_strtitle(
    dst: *mut c_char,
    dstsize: usize,
    src: *const c_char,
    srclen: isize,
    full: bool,
    wbnext: WordBoundaryNext,
    wbstate: *mut c_void,
) -> usize {
    convert_case(dst, dstsize, src, srclen, CaseTitle, full, Some(wbnext), wbstate)
}

/*
 * unicode_strupper()
 *
 * Convert src to uppercase, and return the result length (not including
 * terminating NUL). See unicode_strlower() for buffer semantics.
 */
pub unsafe fn unicode_strupper(
    dst: *mut c_char,
    dstsize: usize,
    src: *const c_char,
    srclen: isize,
    full: bool,
) -> usize {
    convert_case(dst, dstsize, src, srclen, CaseUpper, full, None, core::ptr::null_mut())
}

/*
 * unicode_strfold()
 *
 * Case fold src, and return the result length (not including terminating
 * NUL). See unicode_strlower() for buffer semantics.
 */
pub unsafe fn unicode_strfold(
    dst: *mut c_char,
    dstsize: usize,
    src: *const c_char,
    srclen: isize,
    full: bool,
) -> usize {
    convert_case(dst, dstsize, src, srclen, CaseFold, full, None, core::ptr::null_mut())
}

/*
 * Implement Unicode Default Case Conversion algorithm.
 *
 * If str_casekind is CaseLower or CaseUpper, map each character in the string
 * for which a mapping is available.
 *
 * If str_casekind is CaseTitle, maps characters found on a word boundary to
 * titlecase (or uppercase if full is false) and other characters to
 * lowercase.
 *
 * If full is true, use special mappings for relevant characters, which can
 * map a single codepoint to multiple codepoints, or depend on conditions.
 */
unsafe fn convert_case(
    dst: *mut c_char,
    dstsize: usize,
    src: *const c_char,
    srclen: isize,
    str_casekind: usize,
    full: bool,
    wbnext: Option<WordBoundaryNext>,
    wbstate: *mut c_void,
) -> usize {
    /* character CaseKind varies while titlecasing */
    let mut chr_casekind: usize = str_casekind;
    let mut srcoff: usize = 0;
    let mut result_len: usize = 0;
    let mut boundary: usize = 0;

    Assert!(
        (str_casekind == CaseTitle && wbnext.is_some() && !wbstate.is_null())
            || (str_casekind != CaseTitle && wbnext.is_none() && wbstate.is_null())
    );

    if str_casekind == CaseTitle {
        boundary = (wbnext.unwrap())(wbstate);
        Assert!(boundary == 0); /* start of text is always a boundary */
    }

    while (srclen < 0 || (srcoff as isize) < srclen)
        && *(src.add(srcoff)) != 0
    {
        let u1: pg_wchar = utf8_to_unicode((src as *const c_uchar).add(srcoff));
        let u1len = unicode_utf8len(u1) as usize;
        let mut simple: pg_wchar = 0;
        let mut special: *const pg_wchar = core::ptr::null();

        if str_casekind == CaseTitle {
            if srcoff == boundary {
                chr_casekind = if full { CaseTitle } else { CaseUpper };
                boundary = (wbnext.unwrap())(wbstate);
            } else {
                chr_casekind = CaseLower;
            }
        }

        let casemap_result = casemap(
            u1,
            chr_casekind,
            full,
            src,
            srclen,
            srcoff,
            &mut simple,
            &mut special,
        );

        match casemap_result {
            CaseMapResult::Self_ => {
                /* no mapping; copy bytes from src */
                Assert!(simple == 0);
                Assert!(special.is_null());
                if result_len + u1len <= dstsize {
                    memcpy(
                        dst.add(result_len) as *mut c_void,
                        src.add(srcoff) as *const c_void,
                        u1len,
                    );
                }

                result_len += u1len;
            }
            CaseMapResult::Simple => {
                /* replace with single character */
                let u2: pg_wchar = simple;
                let u2len = unicode_utf8len(u2) as usize;

                Assert!(special.is_null());
                if result_len + u2len <= dstsize {
                    unicode_to_utf8(u2, (dst as *mut c_uchar).add(result_len));
                }

                result_len += u2len;
            }
            CaseMapResult::Special => {
                /* replace with up to MAX_CASE_EXPANSION characters */
                Assert!(simple == 0);
                let mut i = 0;
                while i < MAX_CASE_EXPANSION && *special.add(i) != 0 {
                    let u2: pg_wchar = *special.add(i);
                    let u2len = unicode_utf8len(u2) as usize;

                    if result_len + u2len <= dstsize {
                        unicode_to_utf8(u2, (dst as *mut c_uchar).add(result_len));
                    }

                    result_len += u2len;
                    i += 1;
                }
            }
        }

        srcoff += u1len;
    }

    if result_len < dstsize {
        *(dst.add(result_len)) = 0;
    }

    result_len
}

/*
 * Check that the condition matches Final_Sigma, described in Unicode Table
 * 3-17. The character at the given offset must be directly preceded by a
 * Cased character, and must not be directly followed by a Cased character.
 *
 * Case_Ignorable characters are ignored. NB: some characters may be both
 * Cased and Case_Ignorable, in which case they are ignored.
 */
unsafe fn check_final_sigma(str: *const c_uchar, len: usize, offset: usize) -> bool {
    /* the start of the string is not preceded by a Cased character */
    if offset == 0 {
        return false;
    }

    /* iterate backwards, looking for Cased character */
    let mut i: isize = offset as isize - 1;
    while i >= 0 {
        let b = *str.add(i as usize);
        if (b & 0x80) == 0 || (b & 0xC0) == 0xC0 {
            let curr: pg_wchar = utf8_to_unicode(str.add(i as usize));

            if pg_u_prop_case_ignorable(curr) {
                i -= 1;
                continue;
            } else if pg_u_prop_cased(curr) {
                break;
            } else {
                return false;
            }
        } else if (b & 0xC0) == 0x80 {
            i -= 1;
            continue;
        }

        Assert!(false); /* invalid UTF-8 */
        i -= 1;
    }

    /* end of string is not followed by a Cased character */
    if offset == len {
        return true;
    }

    /* iterate forwards, looking for Cased character */
    let mut j: usize = offset + 1;
    while j < len && *str.add(j) != 0 {
        let b = *str.add(j);
        if (b & 0x80) == 0 || (b & 0xC0) == 0xC0 {
            let curr: pg_wchar = utf8_to_unicode(str.add(j));

            if pg_u_prop_case_ignorable(curr) {
                j += 1;
                continue;
            } else if pg_u_prop_cased(curr) {
                return false;
            } else {
                break;
            }
        } else if (b & 0xC0) == 0x80 {
            j += 1;
            continue;
        }

        Assert!(false); /* invalid UTF-8 */
        j += 1;
    }

    true
}

/*
 * Unicode allows for special casing to be applied only under certain
 * circumstances. The only currently-supported condition is Final_Sigma.
 */
unsafe fn check_special_conditions(
    conditions: i16,
    str: *const c_char,
    len: usize,
    offset: usize,
) -> bool {
    if conditions == 0 {
        true
    } else if conditions == PG_U_FINAL_SIGMA {
        check_final_sigma(str as *const c_uchar, len, offset)
    } else {
        /* no other conditions supported */
        Assert!(false);
        false
    }
}

/*
 * Map the given character to the requested case.
 *
 * If full is true, and a special case mapping is found and the conditions are
 * met, 'special' is set to the mapping result (which is an array of up to
 * MAX_CASE_EXPANSION characters) and CASEMAP_SPECIAL is returned.
 *
 * Otherwise, search for a simple mapping, and if found, set 'simple' to the
 * result and return CASEMAP_SIMPLE.
 *
 * If no mapping is found, return CASEMAP_SELF, and the caller should copy the
 * character without modification.
 */
unsafe fn casemap(
    u1: pg_wchar,
    casekind: usize,
    full: bool,
    src: *const c_char,
    srclen: isize,
    srcoff: usize,
    simple: *mut pg_wchar,
    special: *mut *const pg_wchar,
) -> CaseMapResult {
    let idx: uint16;

    /* Fast path for codepoints < 0x80 */
    if u1 < 0x80 {
        /*
         * The first elements in all tables are reserved as 0 (as NULL). The
         * data starts at index 1, not 0.
         */
        *simple = casekind_map(casekind)[(u1 + 1) as usize];

        return CaseMapResult::Simple;
    }

    idx = case_index(u1);

    if idx == 0 {
        return CaseMapResult::Self_;
    }

    // The C uses srclen as size_t here; the convert_case caller only reaches
    // this with srclen >= 0 in the conditions path, matching C's implicit
    // conversion semantics.
    let sclen = if srclen < 0 { 0usize } else { srclen as usize };

    if full
        && case_map_special[idx as usize] != 0
        && check_special_conditions(
            special_case[case_map_special[idx as usize] as usize].conditions,
            src,
            sclen,
            srcoff,
        )
    {
        *special = special_case[case_map_special[idx as usize] as usize].map[casekind].as_ptr();
        return CaseMapResult::Special;
    }

    *simple = casekind_map(casekind)[idx as usize];

    CaseMapResult::Simple
}

/*
 * Find entry in simple case map.
 * If the entry does not exist, 0 will be returned.
 */
fn find_case_map(ucs: pg_wchar, map: &[u32]) -> pg_wchar {
    /* Fast path for codepoints < 0x80 */
    if ucs < 0x80 {
        /* The first elements in all tables are reserved as 0 (as NULL). */
        return map[(ucs + 1) as usize];
    }
    map[case_index(ucs) as usize]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lowercase_simple_ascii() {
        assert_eq!(unicode_lowercase_simple(0x41), 0x61); // 'A' -> 'a'
    }

    #[test]
    fn test_uppercase_simple_ascii() {
        assert_eq!(unicode_uppercase_simple(0x61), 0x41); // 'a' -> 'A'
    }

    #[test]
    fn test_casefold_simple_nonascii() {
        // U+00C9 LATIN CAPITAL LETTER E WITH ACUTE -> U+00E9 small e with acute
        assert_eq!(unicode_casefold_simple(0x00C9), 0x00E9);
    }

    #[test]
    fn test_lowercase_simple_nonascii() {
        // U+00C9 -> U+00E9
        assert_eq!(unicode_lowercase_simple(0x00C9), 0x00E9);
        // U+00E9 -> U+00C9
        assert_eq!(unicode_uppercase_simple(0x00E9), 0x00C9);
    }

    #[test]
    fn test_titlecase_simple_basic() {
        // ASCII letters titlecase the same as uppercase.
        assert_eq!(unicode_titlecase_simple(0x61), 0x41); // 'a' -> 'A'
    }

    #[test]
    fn test_self_mapping() {
        // A digit has no case mapping; returns itself.
        assert_eq!(unicode_lowercase_simple(0x30), 0x30); // '0'
        assert_eq!(unicode_uppercase_simple(0x30), 0x30);
    }

    #[test]
    fn test_strlower_ascii() {
        unsafe {
            let src = b"ABC\0";
            let mut dst = [0u8; 16];
            let n = unicode_strlower(
                dst.as_mut_ptr() as *mut c_char,
                dst.len(),
                src.as_ptr() as *const c_char,
                -1,
                false,
            );
            assert_eq!(n, 3);
            assert_eq!(&dst[..3], b"abc");
            assert_eq!(dst[3], 0);
        }
    }

    #[test]
    fn test_strupper_ascii() {
        unsafe {
            let src = b"abc\0";
            let mut dst = [0u8; 16];
            let n = unicode_strupper(
                dst.as_mut_ptr() as *mut c_char,
                dst.len(),
                src.as_ptr() as *const c_char,
                -1,
                false,
            );
            assert_eq!(n, 3);
            assert_eq!(&dst[..3], b"ABC");
        }
    }

    #[test]
    fn test_strlower_nonascii_simple() {
        unsafe {
            // U+00C9 (0xC3 0x89) -> U+00E9 (0xC3 0xA9)
            let src = [0xC3u8, 0x89, 0x00];
            let mut dst = [0u8; 16];
            let n = unicode_strlower(
                dst.as_mut_ptr() as *mut c_char,
                dst.len(),
                src.as_ptr() as *const c_char,
                -1,
                false,
            );
            assert_eq!(n, 2);
            assert_eq!(&dst[..2], &[0xC3u8, 0xA9]);
        }
    }
}
