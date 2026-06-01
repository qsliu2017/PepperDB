//! Normalize a Unicode string.
//!
//! Translated 1:1 from:
//!   IMPL:   postgres/src/common/unicode_norm.c
//!   HEADER: postgres/src/include/common/unicode_norm.h
//!           (the public UnicodeNormalizationForm / UnicodeNormalizationQC
//!            enums + the unicode_normalize /
//!            unicode_is_normalized_quickcheck prototypes)
//!
//! This implements Unicode normalization, per the documentation at
//! <https://www.unicode.org/reports/tr15/>.
//!
//! The big decomposition / recomposition / quick-check tables, the info
//! structs, and the perfect-hash functions are imported from the generated
//! modules (unicode_norm_table, unicode_normprops_table,
//! unicode_norm_hashfunc). This file is the BACKEND build only: it uses the
//! perfect-hash lookups and palloc (the C `#ifdef FRONTEND` malloc / bsearch
//! paths are not translated).
//!
//! Hash keys: the C builds them in network (big-endian) byte order via
//! pg_hton32 / pg_hton64 before handing the raw bytes to the perfect hash,
//! because the generated hash tables were built from `pack('N', ...)` /
//! `pack('Q>', ...)` keys. We therefore feed `to_be_bytes()` to the hash
//! functions to stay byte-for-byte compatible with the generated tables.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use crate::common::unicode_norm_hashfunc::{
    Decomp_hash_func, NFC_QC_hash_func, NFKC_QC_hash_func, Recomp_hash_func, RecompInverseLookup,
};
use crate::common::unicode_norm_table::{
    pg_unicode_decomposition, UnicodeDecompMain, UnicodeDecomp_codepoints, DECOMP_COMPAT,
    DECOMP_INLINE, DECOMP_NO_COMPOSE,
};
use crate::common::unicode_normprops_table::{
    pg_unicode_normprops, UnicodeNormProps_NFC_QC, UnicodeNormProps_NFKC_QC, UNICODE_NORM_QC_MAYBE,
    UNICODE_NORM_QC_NO, UNICODE_NORM_QC_YES,
};
use crate::mb::wchar::pg_wchar;
use crate::prelude::*;

/*
 * unicode_norm.h
 *
 * Public normalization-form and quick-check enums. In the C these live in the
 * header; we model them as Rust enums here. UnicodeNormalizationForm follows
 * the C discriminants exactly (NFC=0, NFD=1, NFKC=2, NFKD=3).
 */
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(C)]
pub enum UnicodeNormalizationForm {
    UNICODE_NFC = 0,
    UNICODE_NFD = 1,
    UNICODE_NFKC = 2,
    UNICODE_NFKD = 3,
}
pub use UnicodeNormalizationForm::*;

/*
 * UnicodeNormalizationQC: see UAX #15. The C is an enum with NO=0, YES=1,
 * MAYBE=-1. The generated normprops table stores the quickcheck values as
 * `i8` with matching consts (UNICODE_NORM_QC_NO/YES/MAYBE), so we reuse that
 * representation directly as the QC type.
 */
pub type UnicodeNormalizationQC = i8;

/* Constants for calculations with Hangul characters */
const SBASE: u32 = 0xAC00; /* U+AC00 */
const LBASE: u32 = 0x1100; /* U+1100 */
const VBASE: u32 = 0x1161; /* U+1161 */
const TBASE: u32 = 0x11A7; /* U+11A7 */
const LCOUNT: u32 = 19;
const VCOUNT: u32 = 21;
const TCOUNT: u32 = 28;
const NCOUNT: u32 = VCOUNT * TCOUNT;
const SCOUNT: u32 = LCOUNT * NCOUNT;

/*
 * The DECOMPOSITION_* accessor macros from unicode_norm_table.h. The size and
 * the flag bits are packed together in the `dec_size_flags` byte.
 */
#[inline]
fn decomposition_size(x: &pg_unicode_decomposition) -> i32 {
    (x.dec_size_flags & 0x1F) as i32
}

#[inline]
fn decomposition_no_compose(x: &pg_unicode_decomposition) -> bool {
    (x.dec_size_flags & (DECOMP_NO_COMPOSE | DECOMP_COMPAT)) != 0
}

#[inline]
fn decomposition_is_inline(x: &pg_unicode_decomposition) -> bool {
    (x.dec_size_flags & DECOMP_INLINE) != 0
}

#[inline]
fn decomposition_is_compat(x: &pg_unicode_decomposition) -> bool {
    (x.dec_size_flags & DECOMP_COMPAT) != 0
}

/*
 * get_code_entry
 *
 * Get the entry corresponding to code in the decomposition lookup table.
 * Uses the perfect hash function for the lookup (backend path).
 */
fn get_code_entry(code: pg_wchar) -> *const pg_unicode_decomposition {
    /*
     * Compute the hash function. The hash key is the codepoint with the bytes
     * in network order.
     */
    let h = Decomp_hash_func(&code.to_be_bytes());

    /* An out-of-range result implies no match */
    if h < 0 || (h as usize) >= UnicodeDecompMain.len() {
        return null();
    }

    /*
     * Since it's a perfect hash, we need only match to the specific codepoint
     * it identifies.
     */
    if code != UnicodeDecompMain[h as usize].codepoint {
        return null();
    }

    /* Success! */
    &UnicodeDecompMain[h as usize]
}

/*
 * Get the combining class of the given codepoint.
 */
fn get_canonical_class(code: pg_wchar) -> u8 {
    let entry = get_code_entry(code);

    /*
     * If no entries are found, the character used is either an Hangul
     * character or a character with a class of 0 and no decompositions.
     */
    if entry.is_null() {
        0
    } else {
        unsafe { (*entry).comb_class }
    }
}

/*
 * Given a decomposition entry looked up earlier, get the decomposed
 * characters.
 *
 * Note: the returned pointer can point to a statically allocated buffer, and
 * is only valid until the next call to this function!
 */
fn get_code_decomposition(
    entry: *const pg_unicode_decomposition,
    dec_size: &mut i32,
) -> *const pg_wchar {
    /* mirror of the C `static pg_wchar x;` scratch slot */
    static mut X: pg_wchar = 0;

    let e = unsafe { &*entry };

    if decomposition_is_inline(e) {
        Assert!(decomposition_size(e) == 1);
        unsafe {
            X = e.dec_index as pg_wchar;
            *dec_size = 1;
            &raw const X
        }
    } else {
        *dec_size = decomposition_size(e);
        &UnicodeDecomp_codepoints[e.dec_index as usize] as *const u32
    }
}

/*
 * Calculate how many characters a given character will decompose to.
 *
 * This needs to recurse, if the character decomposes into characters that
 * are, in turn, decomposable.
 */
fn get_decomposed_size(code: pg_wchar, compat: bool) -> i32 {
    /*
     * Fast path for Hangul characters not stored in tables to save memory as
     * decomposition is algorithmic. See
     * <https://www.unicode.org/reports/tr15/tr15-18.html>, annex 10 for
     * details on the matter.
     */
    if code >= SBASE && code < SBASE + SCOUNT {
        let sindex = code - SBASE;
        let tindex = sindex % TCOUNT;

        if tindex != 0 {
            return 3;
        }
        return 2;
    }

    let entry = get_code_entry(code);

    /*
     * Just count current code if no other decompositions. A NULL entry is
     * equivalent to a character with class 0 and no decompositions.
     */
    if entry.is_null() {
        return 1;
    }
    let e = unsafe { &*entry };
    if decomposition_size(e) == 0 || (!compat && decomposition_is_compat(e)) {
        return 1;
    }

    /*
     * If this entry has other decomposition codes look at them as well. First
     * get its decomposition in the list of tables available.
     */
    let mut dec_size: i32 = 0;
    let decomp = get_code_decomposition(entry, &mut dec_size);
    let mut size: i32 = 0;
    for i in 0..dec_size {
        let lcode = unsafe { *decomp.add(i as usize) };
        size += get_decomposed_size(lcode, compat);
    }

    size
}

/*
 * Recompose a set of characters. For hangul characters, the calculation is
 * algorithmic. For others, an inverse lookup at the decomposition table is
 * necessary. Returns true if a recomposition can be done, and false
 * otherwise.
 */
fn recompose_code(start: u32, code: u32, result: &mut u32) -> bool {
    /*
     * Handle Hangul characters algorithmically, per the Unicode spec.
     *
     * Check if two current characters are L and V.
     */
    if start >= LBASE && start < LBASE + LCOUNT && code >= VBASE && code < VBASE + VCOUNT {
        /* make syllable of form LV */
        let lindex = start - LBASE;
        let vindex = code - VBASE;

        *result = SBASE + (lindex * VCOUNT + vindex) * TCOUNT;
        true
    }
    /* Check if two current characters are LV and T */
    else if start >= SBASE
        && start < (SBASE + SCOUNT)
        && ((start - SBASE) % TCOUNT) == 0
        && code >= TBASE
        && code < (TBASE + TCOUNT)
    {
        /* make syllable of form LVT */
        let tindex = code - TBASE;

        *result = start + tindex;
        true
    } else {
        /*
         * Do an inverse lookup of the decomposition tables to see if anything
         * matches. The comparison just needs to be a perfect match on the
         * sub-table of size two, because the start character has already been
         * recomposed partially. This lookup uses a perfect hash function for
         * the backend code.
         *
         * Compute the hash function. The hash key is formed by concatenating
         * the bytes of the two codepoints in network order. The C builds it
         * as pg_hton64(((uint64) start << 32) | code); the big-endian byte
         * stream of that value is exactly start's BE bytes followed by code's
         * BE bytes.
         */
        let mut hashkey = [0u8; 8];
        hashkey[0..4].copy_from_slice(&start.to_be_bytes());
        hashkey[4..8].copy_from_slice(&code.to_be_bytes());
        let h = Recomp_hash_func(&hashkey);

        /* An out-of-range result implies no match */
        if h < 0 || (h as usize) >= RecompInverseLookup.len() {
            return false;
        }

        let inv_lookup_index = RecompInverseLookup[h as usize];
        let entry = &UnicodeDecompMain[inv_lookup_index as usize];

        if start == UnicodeDecomp_codepoints[entry.dec_index as usize]
            && code == UnicodeDecomp_codepoints[entry.dec_index as usize + 1]
        {
            *result = entry.codepoint;
            return true;
        }

        false
    }
}

/*
 * Decompose the given code into the array given by caller. The decomposition
 * begins at the position given by caller, saving one lookup on the
 * decomposition table. The current position needs to be updated here to let
 * the caller know from where to continue filling in the array result.
 */
unsafe fn decompose_code(code: pg_wchar, compat: bool, result: *mut pg_wchar, current: &mut i32) {
    /*
     * Fast path for Hangul characters not stored in tables to save memory as
     * decomposition is algorithmic. See
     * <https://www.unicode.org/reports/tr15/tr15-18.html>, annex 10 for
     * details on the matter.
     */
    if code >= SBASE && code < SBASE + SCOUNT {
        let sindex = code - SBASE;
        let l = LBASE + sindex / (VCOUNT * TCOUNT);
        let v = VBASE + (sindex % (VCOUNT * TCOUNT)) / TCOUNT;
        let tindex = sindex % TCOUNT;

        *result.add(*current as usize) = l;
        *current += 1;
        *result.add(*current as usize) = v;
        *current += 1;

        if tindex != 0 {
            *result.add(*current as usize) = TBASE + tindex;
            *current += 1;
        }

        return;
    }

    let entry = get_code_entry(code);

    /*
     * Just fill in with the current decomposition if there are no
     * decomposition codes to recurse to. A NULL entry is equivalent to a
     * character with class 0 and no decompositions, so just leave also in
     * this case.
     */
    let leave = if entry.is_null() {
        true
    } else {
        let e = &*entry;
        decomposition_size(e) == 0 || (!compat && decomposition_is_compat(e))
    };
    if leave {
        *result.add(*current as usize) = code;
        *current += 1;
        return;
    }

    /*
     * If this entry has other decomposition codes look at them as well.
     */
    let mut dec_size: i32 = 0;
    let decomp = get_code_decomposition(entry, &mut dec_size);
    for i in 0..dec_size {
        let lcode = *decomp.add(i as usize) as pg_wchar;

        /* Leave if no more decompositions */
        decompose_code(lcode, compat, result, current);
    }
}

/*
 * unicode_normalize - Normalize a Unicode string to the specified form.
 *
 * The input is a 0-terminated array of codepoints.
 *
 * The returned string is palloc'd; OOM is reported by palloc with ereport().
 */
pub unsafe fn unicode_normalize(
    form: UnicodeNormalizationForm,
    input: *const pg_wchar,
) -> *mut pg_wchar {
    let compat = form == UNICODE_NFKC || form == UNICODE_NFKD;
    let recompose = form == UNICODE_NFC || form == UNICODE_NFKC;

    /* First, do character decomposition */

    /*
     * Calculate how many characters long the decomposed version will be.
     */
    let mut decomp_size: i32 = 0;
    let mut p = input;
    while *p != 0 {
        decomp_size += get_decomposed_size(*p, compat);
        p = p.add(1);
    }

    let decomp_chars = palloc((decomp_size as usize + 1) * core::mem::size_of::<pg_wchar>())
        as *mut pg_wchar;

    /*
     * Now fill in each entry recursively. This needs a second pass on the
     * decomposition table.
     */
    let mut current_size: i32 = 0;
    p = input;
    while *p != 0 {
        decompose_code(*p, compat, decomp_chars, &mut current_size);
        p = p.add(1);
    }
    *decomp_chars.add(decomp_size as usize) = 0; /* '\0' */
    Assert!(decomp_size == current_size);

    /* Leave if there is nothing to decompose */
    if decomp_size == 0 {
        return decomp_chars;
    }

    /*
     * Now apply canonical ordering.
     */
    let mut count: i32 = 1;
    while count < decomp_size {
        let prev = *decomp_chars.add((count - 1) as usize);
        let next = *decomp_chars.add(count as usize);
        let prev_class = get_canonical_class(prev);
        let next_class = get_canonical_class(next);

        /*
         * Per Unicode (<https://www.unicode.org/reports/tr15/tr15-18.html>)
         * annex 4, a sequence of two adjacent characters in a string is an
         * exchangeable pair if the combining class (from the Unicode
         * Character Database) for the first character is greater than the
         * combining class for the second, and the second is not a starter. A
         * character is a starter if its combining class is 0.
         */
        if prev_class == 0 || next_class == 0 {
            count += 1;
            continue;
        }

        if prev_class <= next_class {
            count += 1;
            continue;
        }

        /* exchange can happen */
        let tmp = *decomp_chars.add((count - 1) as usize);
        *decomp_chars.add((count - 1) as usize) = *decomp_chars.add(count as usize);
        *decomp_chars.add(count as usize) = tmp;

        /* backtrack to check again */
        if count > 1 {
            count -= 2;
        }

        count += 1;
    }

    if !recompose {
        return decomp_chars;
    }

    /*
     * The last phase of NFC and NFKC is the recomposition of the reordered
     * Unicode string using combining classes. The recomposed string cannot be
     * longer than the decomposed one, so make the allocation of the output
     * string based on that assumption.
     */
    let recomp_chars = palloc((decomp_size as usize + 1) * core::mem::size_of::<pg_wchar>())
        as *mut pg_wchar;

    let mut last_class: i32 = -1; /* this eliminates a special check */
    let mut starter_pos: i32 = 0;
    let mut target_pos: i32 = 1;
    *recomp_chars.add(0) = *decomp_chars.add(0);
    let mut starter_ch: u32 = *recomp_chars.add(0);

    count = 1;
    while count < decomp_size {
        let ch = *decomp_chars.add(count as usize);
        let ch_class = get_canonical_class(ch) as i32;
        let mut composite: pg_wchar = 0;

        if last_class < ch_class && recompose_code(starter_ch, ch, &mut composite) {
            *recomp_chars.add(starter_pos as usize) = composite;
            starter_ch = composite;
        } else if ch_class == 0 {
            starter_pos = target_pos;
            starter_ch = ch;
            last_class = -1;
            *recomp_chars.add(target_pos as usize) = ch;
            target_pos += 1;
        } else {
            last_class = ch_class;
            *recomp_chars.add(target_pos as usize) = ch;
            target_pos += 1;
        }

        count += 1;
    }
    *recomp_chars.add(target_pos as usize) = 0; /* '\0' */

    pfree(decomp_chars as *mut c_void);

    recomp_chars
}

/*
 * Normalization "quick check" algorithm; see
 * <http://www.unicode.org/reports/tr15/#Detecting_Normalization_Forms>
 */

fn qc_hash_lookup(
    ch: pg_wchar,
    normprops: &[pg_unicode_normprops],
    hash: fn(&[u8]) -> i32,
) -> *const pg_unicode_normprops {
    /*
     * Compute the hash function. The hash key is the codepoint with the bytes
     * in network order.
     */
    let h = hash(&ch.to_be_bytes());

    /* An out-of-range result implies no match */
    if h < 0 || (h as usize) >= normprops.len() {
        return null();
    }

    /*
     * Since it's a perfect hash, we need only match to the specific codepoint
     * it identifies.
     */
    if ch != normprops[h as usize].codepoint {
        return null();
    }

    /* Success! */
    &normprops[h as usize]
}

/*
 * Look up the normalization quick check character property.
 */
fn qc_is_allowed(form: UnicodeNormalizationForm, ch: pg_wchar) -> UnicodeNormalizationQC {
    /*
     * Pick the NFC vs NFKC quick-check table + matching hash function inline.
     * (In the C, these are wrapped in the pg_unicode_norminfo structs
     * UnicodeNormInfo_NFC_QC / _NFKC_QC.)
     */
    let found = match form {
        UNICODE_NFC => qc_hash_lookup(ch, &UnicodeNormProps_NFC_QC, NFC_QC_hash_func),
        UNICODE_NFKC => qc_hash_lookup(ch, &UnicodeNormProps_NFKC_QC, NFKC_QC_hash_func),
        _ => {
            Assert!(false);
            null()
        }
    };

    if !found.is_null() {
        unsafe { (*found).quickcheck }
    } else {
        UNICODE_NORM_QC_YES
    }
}

pub unsafe fn unicode_is_normalized_quickcheck(
    form: UnicodeNormalizationForm,
    input: *const pg_wchar,
) -> UnicodeNormalizationQC {
    let mut last_canonical_class: u8 = 0;
    let mut result: UnicodeNormalizationQC = UNICODE_NORM_QC_YES;

    /*
     * For the "D" forms, we don't run the quickcheck. We don't include the
     * lookup tables for those because they are huge, checking for these
     * particular forms is less common, and running the slow path is faster
     * for the "D" forms than the "C" forms because you don't need to
     * recompose, which is slow.
     */
    if form == UNICODE_NFD || form == UNICODE_NFKD {
        return UNICODE_NORM_QC_MAYBE;
    }

    let mut p = input;
    while *p != 0 {
        let ch = *p;

        let canonical_class = get_canonical_class(ch);
        if last_canonical_class > canonical_class && canonical_class != 0 {
            return UNICODE_NORM_QC_NO;
        }

        let check = qc_is_allowed(form, ch);
        if check == UNICODE_NORM_QC_NO {
            return UNICODE_NORM_QC_NO;
        } else if check == UNICODE_NORM_QC_MAYBE {
            result = UNICODE_NORM_QC_MAYBE;
        }

        last_canonical_class = canonical_class;
        p = p.add(1);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    /* Collect a NUL-terminated pg_wchar output into a Vec (excluding NUL). */
    unsafe fn collect(out: *const pg_wchar) -> Vec<pg_wchar> {
        let mut v = Vec::new();
        let mut p = out;
        while *p != 0 {
            v.push(*p);
            p = p.add(1);
        }
        v
    }

    #[test]
    fn nfd_decomposes_a_ring() {
        /* U+00C5 (A with ring above) -> U+0041 U+030A */
        let input: [pg_wchar; 2] = [0x00C5, 0];
        unsafe {
            let out = unicode_normalize(UNICODE_NFD, input.as_ptr());
            assert_eq!(collect(out), vec![0x0041, 0x030A]);
        }
    }

    #[test]
    fn nfc_recomposes_a_ring() {
        /* U+0041 U+030A -> U+00C5 */
        let input: [pg_wchar; 3] = [0x0041, 0x030A, 0];
        unsafe {
            let out = unicode_normalize(UNICODE_NFC, input.as_ptr());
            assert_eq!(collect(out), vec![0x00C5]);
        }
    }

    #[test]
    fn nfd_decomposes_hangul() {
        /* U+AC00 (GA) -> L U+1100, V U+1161 (no trailing jamo) */
        let input: [pg_wchar; 2] = [0xAC00, 0];
        unsafe {
            let out = unicode_normalize(UNICODE_NFD, input.as_ptr());
            assert_eq!(collect(out), vec![0x1100, 0x1161]);
        }
    }

    #[test]
    fn nfc_recomposes_hangul() {
        /* L U+1100, V U+1161 -> U+AC00 */
        let input: [pg_wchar; 3] = [0x1100, 0x1161, 0];
        unsafe {
            let out = unicode_normalize(UNICODE_NFC, input.as_ptr());
            assert_eq!(collect(out), vec![0xAC00]);
        }
    }

    #[test]
    fn qc_combining_ring_is_maybe() {
        /* U+0300 (combining grave) is QC=MAYBE under NFC */
        assert_eq!(qc_is_allowed(UNICODE_NFC, 0x0300), UNICODE_NORM_QC_MAYBE);
    }

    #[test]
    fn qc_d_forms_are_maybe() {
        let input: [pg_wchar; 2] = [0x0041, 0];
        unsafe {
            assert_eq!(
                unicode_is_normalized_quickcheck(UNICODE_NFD, input.as_ptr()),
                UNICODE_NORM_QC_MAYBE
            );
        }
    }
}
