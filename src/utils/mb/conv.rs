//! Translated from postgres/src/backend/utils/mb/conv.c
//!
//! Utility functions for conversion procs - the generic character-set
//! conversion helper routines that the per-encoding conversion procs call.
//!
//! #include "postgres.h"        -> crate::prelude::*
//! #include "mb/pg_wchar.h"     -> crate::mb::wchar (pg_wchar, PG_* encoding ids,
//!                                  pg_mb_radix_tree, pg_utf_to_local_combined,
//!                                  pg_local_to_utf_combined, utf_local_conversion_func,
//!                                  pg_mule_mblen, pg_utf_mblen, pg_utf8_islegal,
//!                                  pg_encoding_verifymbchar, IS_HIGHBIT_SET, HIGHBIT,
//!                                  PG_VALID_ENCODING)
//!
//! The error reporters report_invalid_encoding / report_untranslatable_char live
//! in mbutils.c -> crate::mb::mbutils (both have type `-> !`).

use crate::prelude::*;

use crate::mb::mbutils::{report_invalid_encoding, report_untranslatable_char};
use crate::mb::wchar::{
    pg_enc, pg_encoding_verifymbchar, pg_local_to_utf_combined, pg_mb_radix_tree, pg_mule_mblen,
    pg_utf8_islegal, pg_utf_mblen, pg_utf_to_local_combined, utf_local_conversion_func,
    PG_VALID_ENCODING,
};

use crate::c::{HIGHBIT, IS_HIGHBIT_SET};

// Encoding ids used as plain ints in this file.
#[inline]
fn PG_UTF8_ID() -> c_int {
    pg_enc::PG_UTF8 as c_int
}
#[inline]
fn PG_MULE_INTERNAL_ID() -> c_int {
    pg_enc::PG_MULE_INTERNAL as c_int
}

/*
 * local2local: a generic single byte charset encoding
 * conversion between two ASCII-superset encodings.
 *
 * l points to the source string of length len
 * p is the output area (must be large enough!)
 * src_encoding is the PG identifier for the source encoding
 * dest_encoding is the PG identifier for the target encoding
 * tab holds conversion entries for the source charset
 * starting from 128 (0x80). each entry in the table holds the corresponding
 * code point for the target charset, or 0 if there is no equivalent code.
 *
 * Returns the number of input bytes consumed.  If noError is true, this can
 * be less than 'len'.
 */
pub unsafe fn local2local(
    l: *const c_uchar,
    mut p: *mut c_uchar,
    mut len: c_int,
    src_encoding: c_int,
    dest_encoding: c_int,
    tab: *const c_uchar,
    noError: bool,
) -> c_int {
    let start = l;
    let mut l = l;

    while len > 0 {
        let c1: c_uchar = *l;
        if c1 == 0 {
            if noError {
                break;
            }
            report_invalid_encoding(src_encoding, l as *const c_char, len);
        }
        if !IS_HIGHBIT_SET(c1) {
            *p = c1;
            p = p.add(1);
        } else {
            let c2: c_uchar = *tab.add((c1 - HIGHBIT) as usize);
            if c2 != 0 {
                *p = c2;
                p = p.add(1);
            } else {
                if noError {
                    break;
                }
                report_untranslatable_char(src_encoding, dest_encoding, l as *const c_char, len);
            }
        }
        l = l.add(1);
        len -= 1;
    }
    *p = b'\0';

    (l as isize - start as isize) as c_int
}

/*
 * LATINn ---> MIC when the charset's local codes map directly to MIC
 *
 * l points to the source string of length len
 * p is the output area (must be large enough!)
 * lc is the mule character set id for the local encoding
 * encoding is the PG identifier for the local encoding
 *
 * Returns the number of input bytes consumed.  If noError is true, this can
 * be less than 'len'.
 */
pub unsafe fn latin2mic(
    l: *const c_uchar,
    mut p: *mut c_uchar,
    mut len: c_int,
    lc: c_int,
    encoding: c_int,
    noError: bool,
) -> c_int {
    let start = l;
    let mut l = l;

    while len > 0 {
        let c1: c_int = *l as c_int;
        if c1 == 0 {
            if noError {
                break;
            }
            report_invalid_encoding(encoding, l as *const c_char, len);
        }
        if IS_HIGHBIT_SET(c1 as c_uchar) {
            *p = lc as c_uchar;
            p = p.add(1);
        }
        *p = c1 as c_uchar;
        p = p.add(1);
        l = l.add(1);
        len -= 1;
    }
    *p = b'\0';

    (l as isize - start as isize) as c_int
}

/*
 * MIC ---> LATINn when the charset's local codes map directly to MIC
 *
 * mic points to the source string of length len
 * p is the output area (must be large enough!)
 * lc is the mule character set id for the local encoding
 * encoding is the PG identifier for the local encoding
 *
 * Returns the number of input bytes consumed.  If noError is true, this can
 * be less than 'len'.
 */
pub unsafe fn mic2latin(
    mic: *const c_uchar,
    mut p: *mut c_uchar,
    mut len: c_int,
    lc: c_int,
    encoding: c_int,
    noError: bool,
) -> c_int {
    let start = mic;
    let mut mic = mic;

    while len > 0 {
        let c1: c_int = *mic as c_int;
        if c1 == 0 {
            if noError {
                break;
            }
            report_invalid_encoding(PG_MULE_INTERNAL_ID(), mic as *const c_char, len);
        }
        if !IS_HIGHBIT_SET(c1 as c_uchar) {
            /* easy for ASCII */
            *p = c1 as c_uchar;
            p = p.add(1);
            mic = mic.add(1);
            len -= 1;
        } else {
            let l: c_int = pg_mule_mblen(mic);

            if len < l {
                if noError {
                    break;
                }
                report_invalid_encoding(PG_MULE_INTERNAL_ID(), mic as *const c_char, len);
            }
            if l != 2 || c1 != lc || !IS_HIGHBIT_SET(*mic.add(1)) {
                if noError {
                    break;
                }
                report_untranslatable_char(
                    PG_MULE_INTERNAL_ID(),
                    encoding,
                    mic as *const c_char,
                    len,
                );
            }
            *p = *mic.add(1);
            p = p.add(1);
            mic = mic.add(2);
            len -= 2;
        }
    }
    *p = b'\0';

    (mic as isize - start as isize) as c_int
}

/*
 * latin2mic_with_table: a generic single byte charset encoding
 * conversion from a local charset to the mule internal code.
 *
 * l points to the source string of length len
 * p is the output area (must be large enough!)
 * lc is the mule character set id for the local encoding
 * encoding is the PG identifier for the local encoding
 * tab holds conversion entries for the local charset
 * starting from 128 (0x80). each entry in the table holds the corresponding
 * code point for the mule encoding, or 0 if there is no equivalent code.
 *
 * Returns the number of input bytes consumed.  If noError is true, this can
 * be less than 'len'.
 */
pub unsafe fn latin2mic_with_table(
    l: *const c_uchar,
    mut p: *mut c_uchar,
    mut len: c_int,
    lc: c_int,
    encoding: c_int,
    tab: *const c_uchar,
    noError: bool,
) -> c_int {
    let start = l;
    let mut l = l;

    while len > 0 {
        let c1: c_uchar = *l;
        if c1 == 0 {
            if noError {
                break;
            }
            report_invalid_encoding(encoding, l as *const c_char, len);
        }
        if !IS_HIGHBIT_SET(c1) {
            *p = c1;
            p = p.add(1);
        } else {
            let c2: c_uchar = *tab.add((c1 - HIGHBIT) as usize);
            if c2 != 0 {
                *p = lc as c_uchar;
                p = p.add(1);
                *p = c2;
                p = p.add(1);
            } else {
                if noError {
                    break;
                }
                report_untranslatable_char(
                    encoding,
                    PG_MULE_INTERNAL_ID(),
                    l as *const c_char,
                    len,
                );
            }
        }
        l = l.add(1);
        len -= 1;
    }
    *p = b'\0';

    (l as isize - start as isize) as c_int
}

/*
 * mic2latin_with_table: a generic single byte charset encoding
 * conversion from the mule internal code to a local charset.
 *
 * mic points to the source string of length len
 * p is the output area (must be large enough!)
 * lc is the mule character set id for the local encoding
 * encoding is the PG identifier for the local encoding
 * tab holds conversion entries for the mule internal code's second byte,
 * starting from 128 (0x80). each entry in the table holds the corresponding
 * code point for the local charset, or 0 if there is no equivalent code.
 *
 * Returns the number of input bytes consumed.  If noError is true, this can
 * be less than 'len'.
 */
pub unsafe fn mic2latin_with_table(
    mic: *const c_uchar,
    mut p: *mut c_uchar,
    mut len: c_int,
    lc: c_int,
    encoding: c_int,
    tab: *const c_uchar,
    noError: bool,
) -> c_int {
    let start = mic;
    let mut mic = mic;

    while len > 0 {
        let c1: c_uchar = *mic;
        if c1 == 0 {
            if noError {
                break;
            }
            report_invalid_encoding(PG_MULE_INTERNAL_ID(), mic as *const c_char, len);
        }
        if !IS_HIGHBIT_SET(c1) {
            /* easy for ASCII */
            *p = c1;
            p = p.add(1);
            mic = mic.add(1);
            len -= 1;
        } else {
            let l: c_int = pg_mule_mblen(mic);

            if len < l {
                if noError {
                    break;
                }
                report_invalid_encoding(PG_MULE_INTERNAL_ID(), mic as *const c_char, len);
            }
            // Note: c2 is only meaningful when the prior conditions all hold.
            let c2: c_uchar = if l == 2 && IS_HIGHBIT_SET(*mic.add(1)) {
                *tab.add((*mic.add(1) - HIGHBIT) as usize)
            } else {
                0
            };
            if l != 2 || c1 as c_int != lc || !IS_HIGHBIT_SET(*mic.add(1)) || c2 == 0 {
                if noError {
                    break;
                }
                report_untranslatable_char(
                    PG_MULE_INTERNAL_ID(),
                    encoding,
                    mic as *const c_char,
                    len,
                );
                #[allow(unreachable_code)]
                {
                    break; /* keep compiler quiet */
                }
            }
            *p = c2;
            p = p.add(1);
            mic = mic.add(2);
            len -= 2;
        }
    }
    *p = b'\0';

    (mic as isize - start as isize) as c_int
}

/*
 * comparison routine for bsearch()
 * this routine is intended for combined UTF8 -> local code
 *
 * p1 points to a uint32[2] key {s1, s2}; p2 points to a
 * pg_utf_to_local_combined.
 */
unsafe fn compare3(p1: *const c_void, p2: *const c_void) -> c_int {
    let key = p1 as *const uint32;
    let s1: uint32 = *key;
    let s2: uint32 = *key.add(1);
    let entry = p2 as *const pg_utf_to_local_combined;
    let d1: uint32 = (*entry).utf1;
    let d2: uint32 = (*entry).utf2;
    if s1 > d1 || (s1 == d1 && s2 > d2) {
        1
    } else if s1 == d1 && s2 == d2 {
        0
    } else {
        -1
    }
}

/*
 * comparison routine for bsearch()
 * this routine is intended for local code -> combined UTF8
 *
 * p1 points to a uint32 key; p2 points to a pg_local_to_utf_combined.
 */
unsafe fn compare4(p1: *const c_void, p2: *const c_void) -> c_int {
    let v1: uint32 = *(p1 as *const uint32);
    let v2: uint32 = (*(p2 as *const pg_local_to_utf_combined)).code;
    if v1 > v2 {
        1
    } else if v1 == v2 {
        0
    } else {
        -1
    }
}

/*
 * Generic bsearch over a contiguous, sorted array of `nmemb` elements each of
 * `size` bytes, using a comparator with C semantics (libc bsearch).  Returns a
 * pointer to the matching element or null.
 */
unsafe fn bsearch_raw(
    key: *const c_void,
    base: *const c_void,
    nmemb: usize,
    size: usize,
    compar: unsafe fn(*const c_void, *const c_void) -> c_int,
) -> *const c_void {
    let mut lo: usize = 0;
    let mut hi: usize = nmemb;
    let base = base as *const u8;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let elem = base.add(mid * size) as *const c_void;
        let r = compar(key, elem);
        if r < 0 {
            hi = mid;
        } else if r > 0 {
            lo = mid + 1;
        } else {
            return elem;
        }
    }
    null()
}

/*
 * store 32bit character representation into multibyte stream
 */
#[inline]
unsafe fn store_coded_char(mut dest: *mut c_uchar, code: uint32) -> *mut c_uchar {
    if code & 0xff000000 != 0 {
        *dest = (code >> 24) as c_uchar;
        dest = dest.add(1);
    }
    if code & 0x00ff0000 != 0 {
        *dest = (code >> 16) as c_uchar;
        dest = dest.add(1);
    }
    if code & 0x0000ff00 != 0 {
        *dest = (code >> 8) as c_uchar;
        dest = dest.add(1);
    }
    if code & 0x000000ff != 0 {
        *dest = code as c_uchar;
        dest = dest.add(1);
    }
    dest
}

/*
 * Convert a character using a conversion radix tree.
 *
 * 'l' is the length of the input character in bytes, and b1-b4 are
 * the input character's bytes.
 */
#[inline]
unsafe fn pg_mb_radix_conv(
    rt: *const pg_mb_radix_tree,
    l: c_int,
    b1: c_uchar,
    b2: c_uchar,
    b3: c_uchar,
    b4: c_uchar,
) -> uint32 {
    let rt = &*rt;
    if l == 4 {
        /* 4-byte code */

        /* check code validity */
        if b1 < rt.b4_1_lower
            || b1 > rt.b4_1_upper
            || b2 < rt.b4_2_lower
            || b2 > rt.b4_2_upper
            || b3 < rt.b4_3_lower
            || b3 > rt.b4_3_upper
            || b4 < rt.b4_4_lower
            || b4 > rt.b4_4_upper
        {
            return 0;
        }

        /* perform lookup */
        if !rt.chars32.is_null() {
            let mut idx: uint32 = rt.b4root;
            idx = *rt.chars32.add((b1 as u32 + idx - rt.b4_1_lower as u32) as usize);
            idx = *rt.chars32.add((b2 as u32 + idx - rt.b4_2_lower as u32) as usize);
            idx = *rt.chars32.add((b3 as u32 + idx - rt.b4_3_lower as u32) as usize);
            *rt.chars32.add((b4 as u32 + idx - rt.b4_4_lower as u32) as usize)
        } else {
            let mut idx: uint16 = rt.b4root as uint16;
            idx = *rt.chars16.add((b1 as u32 + idx as u32 - rt.b4_1_lower as u32) as usize);
            idx = *rt.chars16.add((b2 as u32 + idx as u32 - rt.b4_2_lower as u32) as usize);
            idx = *rt.chars16.add((b3 as u32 + idx as u32 - rt.b4_3_lower as u32) as usize);
            *rt.chars16.add((b4 as u32 + idx as u32 - rt.b4_4_lower as u32) as usize) as uint32
        }
    } else if l == 3 {
        /* 3-byte code */

        /* check code validity */
        if b2 < rt.b3_1_lower
            || b2 > rt.b3_1_upper
            || b3 < rt.b3_2_lower
            || b3 > rt.b3_2_upper
            || b4 < rt.b3_3_lower
            || b4 > rt.b3_3_upper
        {
            return 0;
        }

        /* perform lookup */
        if !rt.chars32.is_null() {
            let mut idx: uint32 = rt.b3root;
            idx = *rt.chars32.add((b2 as u32 + idx - rt.b3_1_lower as u32) as usize);
            idx = *rt.chars32.add((b3 as u32 + idx - rt.b3_2_lower as u32) as usize);
            *rt.chars32.add((b4 as u32 + idx - rt.b3_3_lower as u32) as usize)
        } else {
            let mut idx: uint16 = rt.b3root as uint16;
            idx = *rt.chars16.add((b2 as u32 + idx as u32 - rt.b3_1_lower as u32) as usize);
            idx = *rt.chars16.add((b3 as u32 + idx as u32 - rt.b3_2_lower as u32) as usize);
            *rt.chars16.add((b4 as u32 + idx as u32 - rt.b3_3_lower as u32) as usize) as uint32
        }
    } else if l == 2 {
        /* 2-byte code */

        /* check code validity - first byte */
        if b3 < rt.b2_1_lower || b3 > rt.b2_1_upper || b4 < rt.b2_2_lower || b4 > rt.b2_2_upper {
            return 0;
        }

        /* perform lookup */
        if !rt.chars32.is_null() {
            let mut idx: uint32 = rt.b2root;
            idx = *rt.chars32.add((b3 as u32 + idx - rt.b2_1_lower as u32) as usize);
            *rt.chars32.add((b4 as u32 + idx - rt.b2_2_lower as u32) as usize)
        } else {
            let mut idx: uint16 = rt.b2root as uint16;
            idx = *rt.chars16.add((b3 as u32 + idx as u32 - rt.b2_1_lower as u32) as usize);
            *rt.chars16.add((b4 as u32 + idx as u32 - rt.b2_2_lower as u32) as usize) as uint32
        }
    } else if l == 1 {
        /* 1-byte code */

        /* check code validity - first byte */
        if b4 < rt.b1_lower || b4 > rt.b1_upper {
            return 0;
        }

        /* perform lookup */
        if !rt.chars32.is_null() {
            *rt.chars32.add((b4 as u32 + rt.b1root - rt.b1_lower as u32) as usize)
        } else {
            *rt.chars16.add((b4 as u32 + rt.b1root - rt.b1_lower as u32) as usize) as uint32
        }
    } else {
        0 /* shouldn't happen */
    }
}

/*
 * UTF8 ---> local code
 *
 * utf: input string in UTF8 encoding (need not be null-terminated)
 * len: length of input string (in bytes)
 * iso: pointer to the output area (must be large enough!)
 *        (output string will be null-terminated)
 * map: conversion map for single characters
 * cmap: conversion map for combined characters (optional, pass null if none)
 * cmapsize: number of entries in the cmap (optional, pass 0 if none)
 * conv_func: algorithmic encoding conversion function (optional, None if none)
 * encoding: PG identifier for the local encoding
 *
 * For each character, the cmap (if provided) is consulted first; if no match,
 * the map is consulted next; if still no match, the conv_func (if provided)
 * is applied.  An error is raised if no match is found.
 *
 * Returns the number of input bytes consumed.  If noError is true, this can
 * be less than 'len'.
 */
pub unsafe fn UtfToLocal(
    utf: *const c_uchar,
    mut len: c_int,
    mut iso: *mut c_uchar,
    map: *const pg_mb_radix_tree,
    cmap: *const pg_utf_to_local_combined,
    cmapsize: c_int,
    conv_func: utf_local_conversion_func,
    encoding: c_int,
    noError: bool,
) -> c_int {
    let mut iutf: uint32;
    let mut l: c_int;
    let start = utf;
    let mut utf = utf;

    if !PG_VALID_ENCODING(encoding) {
        ereport!(ERROR, errmsg!("invalid encoding number: {}", encoding));
    }

    while len > 0 {
        let mut b1: c_uchar = 0;
        let mut b2: c_uchar = 0;
        let mut b3: c_uchar = 0;
        let mut b4: c_uchar = 0;

        /* "break" cases all represent errors */
        if *utf == b'\0' {
            break;
        }

        l = pg_utf_mblen(utf);
        if len < l {
            break;
        }

        if !pg_utf8_islegal(utf, l) {
            break;
        }

        if l == 1 {
            /* ASCII case is easy, assume it's one-to-one conversion */
            *iso = *utf;
            iso = iso.add(1);
            utf = utf.add(1);
            len -= l;
            continue;
        }

        /* collect coded char of length l */
        if l == 2 {
            b3 = *utf;
            utf = utf.add(1);
            b4 = *utf;
            utf = utf.add(1);
        } else if l == 3 {
            b2 = *utf;
            utf = utf.add(1);
            b3 = *utf;
            utf = utf.add(1);
            b4 = *utf;
            utf = utf.add(1);
        } else if l == 4 {
            b1 = *utf;
            utf = utf.add(1);
            b2 = *utf;
            utf = utf.add(1);
            b3 = *utf;
            utf = utf.add(1);
            b4 = *utf;
            utf = utf.add(1);
        } else {
            elog!(ERROR, "unsupported character length {}", l);
        }
        iutf = (b1 as u32) << 24 | (b2 as u32) << 16 | (b3 as u32) << 8 | (b4 as u32);

        /* First, try with combined map if possible */
        if !cmap.is_null() && len > l {
            let utf_save = utf;
            let len_save = len;
            let l_save = l;

            /* collect next character, same as above */
            len -= l;

            l = pg_utf_mblen(utf);
            if len < l {
                /* need more data to decide if this is a combined char */
                utf = utf.sub(l_save as usize);
                break;
            }

            if !pg_utf8_islegal(utf, l) {
                if !noError {
                    report_invalid_encoding(PG_UTF8_ID(), utf as *const c_char, len);
                }
                utf = utf.sub(l_save as usize);
                break;
            }

            /* We assume ASCII character cannot be in combined map */
            if l > 1 {
                let iutf2: uint32;

                if l == 2 {
                    let x0 = (*utf as u32) << 8;
                    utf = utf.add(1);
                    iutf2 = x0 | (*utf as u32);
                    utf = utf.add(1);
                } else if l == 3 {
                    let mut v = (*utf as u32) << 16;
                    utf = utf.add(1);
                    v |= (*utf as u32) << 8;
                    utf = utf.add(1);
                    v |= *utf as u32;
                    utf = utf.add(1);
                    iutf2 = v;
                } else if l == 4 {
                    let mut v = (*utf as u32) << 24;
                    utf = utf.add(1);
                    v |= (*utf as u32) << 16;
                    utf = utf.add(1);
                    v |= (*utf as u32) << 8;
                    utf = utf.add(1);
                    v |= *utf as u32;
                    utf = utf.add(1);
                    iutf2 = v;
                } else {
                    elog!(ERROR, "unsupported character length {}", l);
                    unreachable!()
                }

                let cutf: [uint32; 2] = [iutf, iutf2];

                let cp = bsearch_raw(
                    cutf.as_ptr() as *const c_void,
                    cmap as *const c_void,
                    cmapsize as usize,
                    core::mem::size_of::<pg_utf_to_local_combined>(),
                    compare3,
                ) as *const pg_utf_to_local_combined;

                if !cp.is_null() {
                    iso = store_coded_char(iso, (*cp).code);
                    continue;
                }
            }

            /* fail, so back up to reprocess second character next time */
            utf = utf_save;
            len = len_save;
            l = l_save;
        }

        /* Now check ordinary map */
        if !map.is_null() {
            let converted = pg_mb_radix_conv(map, l, b1, b2, b3, b4);

            if converted != 0 {
                iso = store_coded_char(iso, converted);
                continue;
            }
        }

        /* if there's a conversion function, try that */
        if let Some(f) = conv_func {
            let converted = f(iutf);

            if converted != 0 {
                iso = store_coded_char(iso, converted);
                continue;
            }
        }

        /* failed to translate this character */
        utf = utf.sub(l as usize);
        if noError {
            break;
        }
        report_untranslatable_char(PG_UTF8_ID(), encoding, utf as *const c_char, len);
    }

    /* if we broke out of loop early, must be invalid input */
    if len > 0 && !noError {
        report_invalid_encoding(PG_UTF8_ID(), utf as *const c_char, len);
    }

    *iso = b'\0';

    (utf as isize - start as isize) as c_int
}

/*
 * local code ---> UTF8
 *
 * iso: input string in local encoding (need not be null-terminated)
 * len: length of input string (in bytes)
 * utf: pointer to the output area (must be large enough!)
 *        (output string will be null-terminated)
 * map: conversion map for single characters
 * cmap: conversion map for combined characters (optional, pass null if none)
 * cmapsize: number of entries in the cmap (optional, pass 0 if none)
 * conv_func: algorithmic encoding conversion function (optional, None if none)
 * encoding: PG identifier for the local encoding
 *
 * For each character, the map is consulted first; if no match, the cmap
 * (if provided) is consulted next; if still no match, the conv_func
 * (if provided) is applied.  An error is raised if no match is found.
 *
 * Returns the number of input bytes consumed.  If noError is true, this can
 * be less than 'len'.
 */
pub unsafe fn LocalToUtf(
    iso: *const c_uchar,
    mut len: c_int,
    mut utf: *mut c_uchar,
    map: *const pg_mb_radix_tree,
    cmap: *const pg_local_to_utf_combined,
    cmapsize: c_int,
    conv_func: utf_local_conversion_func,
    encoding: c_int,
    noError: bool,
) -> c_int {
    let mut iiso: uint32;
    let mut l: c_int;
    let start = iso;
    let mut iso = iso;

    if !PG_VALID_ENCODING(encoding) {
        ereport!(ERROR, errmsg!("invalid encoding number: {}", encoding));
    }

    while len > 0 {
        let mut b1: c_uchar = 0;
        let mut b2: c_uchar = 0;
        let mut b3: c_uchar = 0;
        let mut b4: c_uchar = 0;

        /* "break" cases all represent errors */
        if *iso == b'\0' {
            break;
        }

        if !IS_HIGHBIT_SET(*iso) {
            /* ASCII case is easy, assume it's one-to-one conversion */
            *utf = *iso;
            utf = utf.add(1);
            iso = iso.add(1);
            l = 1;
            len -= l;
            continue;
        }

        l = pg_encoding_verifymbchar(encoding, iso as *const c_char, len);
        if l < 0 {
            break;
        }

        /* collect coded char of length l */
        if l == 1 {
            b4 = *iso;
            iso = iso.add(1);
        } else if l == 2 {
            b3 = *iso;
            iso = iso.add(1);
            b4 = *iso;
            iso = iso.add(1);
        } else if l == 3 {
            b2 = *iso;
            iso = iso.add(1);
            b3 = *iso;
            iso = iso.add(1);
            b4 = *iso;
            iso = iso.add(1);
        } else if l == 4 {
            b1 = *iso;
            iso = iso.add(1);
            b2 = *iso;
            iso = iso.add(1);
            b3 = *iso;
            iso = iso.add(1);
            b4 = *iso;
            iso = iso.add(1);
        } else {
            elog!(ERROR, "unsupported character length {}", l);
        }
        iiso = (b1 as u32) << 24 | (b2 as u32) << 16 | (b3 as u32) << 8 | (b4 as u32);

        if !map.is_null() {
            let converted = pg_mb_radix_conv(map, l, b1, b2, b3, b4);

            if converted != 0 {
                utf = store_coded_char(utf, converted);
                continue;
            }

            /* If there's a combined character map, try that */
            if !cmap.is_null() {
                let cp = bsearch_raw(
                    &iiso as *const uint32 as *const c_void,
                    cmap as *const c_void,
                    cmapsize as usize,
                    core::mem::size_of::<pg_local_to_utf_combined>(),
                    compare4,
                ) as *const pg_local_to_utf_combined;

                if !cp.is_null() {
                    utf = store_coded_char(utf, (*cp).utf1);
                    utf = store_coded_char(utf, (*cp).utf2);
                    continue;
                }
            }
        }

        /* if there's a conversion function, try that */
        if let Some(f) = conv_func {
            let converted = f(iiso);

            if converted != 0 {
                utf = store_coded_char(utf, converted);
                continue;
            }
        }

        /* failed to translate this character */
        iso = iso.sub(l as usize);
        if noError {
            break;
        }
        report_untranslatable_char(encoding, PG_UTF8_ID(), iso as *const c_char, len);
    }

    /* if we broke out of loop early, must be invalid input */
    if len > 0 && !noError {
        report_invalid_encoding(encoding, iso as *const c_char, len);
    }

    *utf = b'\0';

    (iso as isize - start as isize) as c_int
}

#[cfg(test)]
mod tests {
    use super::*;

    // local2local with an identity 256-entry "high half" table (entries for
    // bytes 0x80..0xFF map to themselves) round-trips any non-NUL input.
    #[test]
    fn local2local_identity_roundtrip() {
        // tab is indexed from 0x80 (HIGHBIT); 128 entries, entry i -> 0x80 + i.
        let mut tab = [0u8; 128];
        for i in 0..128usize {
            tab[i] = 0x80 + i as u8;
        }
        // input: mix of ASCII and high-bit bytes, no NUL.
        let input: [u8; 6] = [b'A', 0x80, b'z', 0xFF, b'0', 0xC3];
        let mut out = [0u8; 16];
        let consumed = unsafe {
            local2local(
                input.as_ptr(),
                out.as_mut_ptr(),
                input.len() as c_int,
                1, // src_encoding (arbitrary; no error path hit)
                1, // dest_encoding
                tab.as_ptr(),
                false,
            )
        };
        assert_eq!(consumed, input.len() as c_int);
        // output equals input (identity), NUL-terminated.
        for i in 0..input.len() {
            assert_eq!(out[i], input[i], "byte {} mismatch", i);
        }
        assert_eq!(out[input.len()], 0);
    }

    // latin2mic on a plain-ASCII buffer is identity: high-bit-clear bytes pass
    // through unchanged (no lead byte emitted), and the count consumed == len.
    #[test]
    fn latin2mic_ascii_identity() {
        let input: [u8; 5] = [b'H', b'e', b'l', b'l', b'o'];
        let mut out = [0u8; 16];
        // lc is the mule lead byte; never emitted for pure ASCII.
        let consumed = unsafe {
            latin2mic(
                input.as_ptr(),
                out.as_mut_ptr(),
                input.len() as c_int,
                0x81, // some LC_* id; unused for ASCII
                1,    // encoding
                false,
            )
        };
        assert_eq!(consumed, input.len() as c_int);
        for i in 0..input.len() {
            assert_eq!(out[i], input[i]);
        }
        assert_eq!(out[input.len()], 0);
    }

    // store_coded_char emits exactly the non-zero leading bytes, big-endian.
    #[test]
    fn store_coded_char_widths() {
        unsafe {
            let mut buf = [0u8; 8];
            // 1-byte
            let p = store_coded_char(buf.as_mut_ptr(), 0x41);
            assert_eq!(p as usize - buf.as_ptr() as usize, 1);
            assert_eq!(buf[0], 0x41);
            // 2-byte
            let mut buf = [0u8; 8];
            let p = store_coded_char(buf.as_mut_ptr(), 0x8142);
            assert_eq!(p as usize - buf.as_ptr() as usize, 2);
            assert_eq!(buf[0], 0x81);
            assert_eq!(buf[1], 0x42);
            // 3-byte
            let mut buf = [0u8; 8];
            let p = store_coded_char(buf.as_mut_ptr(), 0x92A1B2);
            assert_eq!(p as usize - buf.as_ptr() as usize, 3);
            assert_eq!(buf[0], 0x92);
            assert_eq!(buf[1], 0xA1);
            assert_eq!(buf[2], 0xB2);
        }
    }

    // compare3 / compare4 ordering semantics.
    #[test]
    fn comparators() {
        unsafe {
            let key2: [uint32; 2] = [0x100, 0x200];
            let entry = pg_utf_to_local_combined {
                utf1: 0x100,
                utf2: 0x200,
                code: 0xABCD,
            };
            assert_eq!(
                compare3(
                    key2.as_ptr() as *const c_void,
                    &entry as *const _ as *const c_void
                ),
                0
            );
            let bigger: [uint32; 2] = [0x101, 0x000];
            assert_eq!(
                compare3(
                    bigger.as_ptr() as *const c_void,
                    &entry as *const _ as *const c_void
                ),
                1
            );

            let key: uint32 = 0x55;
            let e4 = pg_local_to_utf_combined {
                code: 0x55,
                utf1: 1,
                utf2: 2,
            };
            assert_eq!(
                compare4(
                    &key as *const _ as *const c_void,
                    &e4 as *const _ as *const c_void
                ),
                0
            );
        }
    }
}
