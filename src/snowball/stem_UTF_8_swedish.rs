//! Snowball Swedish (UTF-8) stemmer.
//!
//! 1:1 translation of the generated
//! `src/backend/snowball/libstemmer/stem_UTF_8_swedish.c` (Snowball 2.2.0),
//! merged with its declarations header
//! `src/include/snowball/libstemmer/stem_UTF_8_swedish.h`.
//!
//! The libstemmer runtime is the ported Rust runtime in `crate::snowball::{api,
//! utilities}`; this file only contains the language-specific generated tables
//! and the four step functions plus the three exported env/stem entry points.

use crate::prelude::*;

use crate::snowball::api::{among, symbol, SN_close_env, SN_create_env, SN_env};
use crate::snowball::utilities::{
    find_among_b, in_grouping_b_U, in_grouping_U, out_grouping_U, skip_b_utf8, skip_utf8,
    slice_del, slice_from_s,
};

// ---------------------------------------------------------------------------
// among string literals + tables
// ---------------------------------------------------------------------------

static S_0_0: [symbol; 1] = [b'a'];
static S_0_1: [symbol; 4] = [b'a', b'r', b'n', b'a'];
static S_0_2: [symbol; 4] = [b'e', b'r', b'n', b'a'];
static S_0_3: [symbol; 7] = [b'h', b'e', b't', b'e', b'r', b'n', b'a'];
static S_0_4: [symbol; 4] = [b'o', b'r', b'n', b'a'];
static S_0_5: [symbol; 2] = [b'a', b'd'];
static S_0_6: [symbol; 1] = [b'e'];
static S_0_7: [symbol; 3] = [b'a', b'd', b'e'];
static S_0_8: [symbol; 4] = [b'a', b'n', b'd', b'e'];
static S_0_9: [symbol; 4] = [b'a', b'r', b'n', b'e'];
static S_0_10: [symbol; 3] = [b'a', b'r', b'e'];
static S_0_11: [symbol; 4] = [b'a', b's', b't', b'e'];
static S_0_12: [symbol; 2] = [b'e', b'n'];
static S_0_13: [symbol; 5] = [b'a', b'n', b'd', b'e', b'n'];
static S_0_14: [symbol; 4] = [b'a', b'r', b'e', b'n'];
static S_0_15: [symbol; 5] = [b'h', b'e', b't', b'e', b'n'];
static S_0_16: [symbol; 3] = [b'e', b'r', b'n'];
static S_0_17: [symbol; 2] = [b'a', b'r'];
static S_0_18: [symbol; 2] = [b'e', b'r'];
static S_0_19: [symbol; 5] = [b'h', b'e', b't', b'e', b'r'];
static S_0_20: [symbol; 2] = [b'o', b'r'];
static S_0_21: [symbol; 1] = [b's'];
static S_0_22: [symbol; 2] = [b'a', b's'];
static S_0_23: [symbol; 5] = [b'a', b'r', b'n', b'a', b's'];
static S_0_24: [symbol; 5] = [b'e', b'r', b'n', b'a', b's'];
static S_0_25: [symbol; 5] = [b'o', b'r', b'n', b'a', b's'];
static S_0_26: [symbol; 2] = [b'e', b's'];
static S_0_27: [symbol; 4] = [b'a', b'd', b'e', b's'];
static S_0_28: [symbol; 5] = [b'a', b'n', b'd', b'e', b's'];
static S_0_29: [symbol; 3] = [b'e', b'n', b's'];
static S_0_30: [symbol; 5] = [b'a', b'r', b'e', b'n', b's'];
static S_0_31: [symbol; 6] = [b'h', b'e', b't', b'e', b'n', b's'];
static S_0_32: [symbol; 4] = [b'e', b'r', b'n', b's'];
static S_0_33: [symbol; 2] = [b'a', b't'];
static S_0_34: [symbol; 5] = [b'a', b'n', b'd', b'e', b't'];
static S_0_35: [symbol; 3] = [b'h', b'e', b't'];
static S_0_36: [symbol; 3] = [b'a', b's', b't'];

static A_0: [among; 37] = [
    among { s_size: 1, s: S_0_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_0_1.as_ptr(), substring_i: 0, result: 1, function: None },
    among { s_size: 4, s: S_0_2.as_ptr(), substring_i: 0, result: 1, function: None },
    among { s_size: 7, s: S_0_3.as_ptr(), substring_i: 2, result: 1, function: None },
    among { s_size: 4, s: S_0_4.as_ptr(), substring_i: 0, result: 1, function: None },
    among { s_size: 2, s: S_0_5.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 1, s: S_0_6.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_0_7.as_ptr(), substring_i: 6, result: 1, function: None },
    among { s_size: 4, s: S_0_8.as_ptr(), substring_i: 6, result: 1, function: None },
    among { s_size: 4, s: S_0_9.as_ptr(), substring_i: 6, result: 1, function: None },
    among { s_size: 3, s: S_0_10.as_ptr(), substring_i: 6, result: 1, function: None },
    among { s_size: 4, s: S_0_11.as_ptr(), substring_i: 6, result: 1, function: None },
    among { s_size: 2, s: S_0_12.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_0_13.as_ptr(), substring_i: 12, result: 1, function: None },
    among { s_size: 4, s: S_0_14.as_ptr(), substring_i: 12, result: 1, function: None },
    among { s_size: 5, s: S_0_15.as_ptr(), substring_i: 12, result: 1, function: None },
    among { s_size: 3, s: S_0_16.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_0_17.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_0_18.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_0_19.as_ptr(), substring_i: 18, result: 1, function: None },
    among { s_size: 2, s: S_0_20.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 1, s: S_0_21.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 2, s: S_0_22.as_ptr(), substring_i: 21, result: 1, function: None },
    among { s_size: 5, s: S_0_23.as_ptr(), substring_i: 22, result: 1, function: None },
    among { s_size: 5, s: S_0_24.as_ptr(), substring_i: 22, result: 1, function: None },
    among { s_size: 5, s: S_0_25.as_ptr(), substring_i: 22, result: 1, function: None },
    among { s_size: 2, s: S_0_26.as_ptr(), substring_i: 21, result: 1, function: None },
    among { s_size: 4, s: S_0_27.as_ptr(), substring_i: 26, result: 1, function: None },
    among { s_size: 5, s: S_0_28.as_ptr(), substring_i: 26, result: 1, function: None },
    among { s_size: 3, s: S_0_29.as_ptr(), substring_i: 21, result: 1, function: None },
    among { s_size: 5, s: S_0_30.as_ptr(), substring_i: 29, result: 1, function: None },
    among { s_size: 6, s: S_0_31.as_ptr(), substring_i: 29, result: 1, function: None },
    among { s_size: 4, s: S_0_32.as_ptr(), substring_i: 21, result: 1, function: None },
    among { s_size: 2, s: S_0_33.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_0_34.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_0_35.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_0_36.as_ptr(), substring_i: -1, result: 1, function: None },
];

static S_1_0: [symbol; 2] = [b'd', b'd'];
static S_1_1: [symbol; 2] = [b'g', b'd'];
static S_1_2: [symbol; 2] = [b'n', b'n'];
static S_1_3: [symbol; 2] = [b'd', b't'];
static S_1_4: [symbol; 2] = [b'g', b't'];
static S_1_5: [symbol; 2] = [b'k', b't'];
static S_1_6: [symbol; 2] = [b't', b't'];

static A_1: [among; 7] = [
    among { s_size: 2, s: S_1_0.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_1_1.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_1_2.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_1_3.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_1_4.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_1_5.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_1_6.as_ptr(), substring_i: -1, result: -1, function: None },
];

static S_2_0: [symbol; 2] = [b'i', b'g'];
static S_2_1: [symbol; 3] = [b'l', b'i', b'g'];
static S_2_2: [symbol; 3] = [b'e', b'l', b's'];
static S_2_3: [symbol; 5] = [b'f', b'u', b'l', b'l', b't'];
static S_2_4: [symbol; 4] = [0xC3, 0xB6, b's', b't'];

static A_2: [among; 5] = [
    among { s_size: 2, s: S_2_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_2_1.as_ptr(), substring_i: 0, result: 1, function: None },
    among { s_size: 3, s: S_2_2.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_2_3.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 4, s: S_2_4.as_ptr(), substring_i: -1, result: 2, function: None },
];

static G_V: [c_uchar; 19] =
    [17, 65, 16, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 24, 0, 32];

static G_S_ENDING: [c_uchar; 3] = [119, 127, 149];

static G_OST_ENDING: [c_uchar; 2] = [173, 58];

static S_0: [symbol; 3] = [0xC3, 0xB6, b's'];
static S_1: [symbol; 4] = [b'f', b'u', b'l', b'l'];

// ---------------------------------------------------------------------------
// step functions
// ---------------------------------------------------------------------------

unsafe fn r_mark_regions(z: *mut SN_env) -> c_int {
    *(*z).I.offset(1) = (*z).l;
    {
        let c_test1 = (*z).c;
        {
            let ret = skip_utf8((*z).p, (*z).c, (*z).l, 3);
            if ret < 0 {
                return 0;
            }
            (*z).c = ret;
        }
        *(*z).I.offset(0) = (*z).c;
        (*z).c = c_test1;
    }

    if out_grouping_U(z, G_V.as_ptr(), 97, 246, 1) < 0 {
        return 0;
    }

    {
        let ret = in_grouping_U(z, G_V.as_ptr(), 97, 246, 1);
        if ret < 0 {
            return 0;
        }
        (*z).c += ret;
    }
    *(*z).I.offset(1) = (*z).c;

    'lab0: {
        if *(*z).I.offset(1) >= *(*z).I.offset(0) {
            break 'lab0;
        }
        *(*z).I.offset(1) = *(*z).I.offset(0);
    }
    1
}

unsafe fn r_main_suffix(z: *mut SN_env) -> c_int {
    let among_var: c_int;

    {
        let mlimit1: c_int;
        if (*z).c < *(*z).I.offset(1) {
            return 0;
        }
        mlimit1 = (*z).lb;
        (*z).lb = *(*z).I.offset(1);
        (*z).ket = (*z).c;
        if (*z).c <= (*z).lb
            || *(*z).p.offset((*z).c as isize - 1) as c_int >> 5 != 3
            || (1851442 >> (*(*z).p.offset((*z).c as isize - 1) as c_int & 0x1f)) & 1 == 0
        {
            (*z).lb = mlimit1;
            return 0;
        }
        among_var = find_among_b(z, A_0.as_ptr(), 37);
        if among_var == 0 {
            (*z).lb = mlimit1;
            return 0;
        }
        (*z).bra = (*z).c;
        (*z).lb = mlimit1;
    }
    match among_var {
        1 => {
            let ret = slice_del(z);
            if ret < 0 {
                return ret;
            }
        }
        2 => {
            if in_grouping_b_U(z, G_S_ENDING.as_ptr(), 98, 121, 0) != 0 {
                return 0;
            }
            let ret = slice_del(z);
            if ret < 0 {
                return ret;
            }
        }
        _ => {}
    }
    1
}

unsafe fn r_consonant_pair(z: *mut SN_env) -> c_int {
    {
        let mlimit1: c_int;
        if (*z).c < *(*z).I.offset(1) {
            return 0;
        }
        mlimit1 = (*z).lb;
        (*z).lb = *(*z).I.offset(1);
        {
            let m2 = (*z).l - (*z).c;
            let _ = m2;
            if (*z).c - 1 <= (*z).lb
                || *(*z).p.offset((*z).c as isize - 1) as c_int >> 5 != 3
                || (1064976 >> (*(*z).p.offset((*z).c as isize - 1) as c_int & 0x1f)) & 1 == 0
            {
                (*z).lb = mlimit1;
                return 0;
            }
            if find_among_b(z, A_1.as_ptr(), 7) == 0 {
                (*z).lb = mlimit1;
                return 0;
            }
            (*z).c = (*z).l - m2;
            (*z).ket = (*z).c;
            {
                let ret = skip_b_utf8((*z).p, (*z).c, (*z).lb, 1);
                if ret < 0 {
                    (*z).lb = mlimit1;
                    return 0;
                }
                (*z).c = ret;
            }
            (*z).bra = (*z).c;
            {
                let ret = slice_del(z);
                if ret < 0 {
                    return ret;
                }
            }
        }
        (*z).lb = mlimit1;
    }
    1
}

unsafe fn r_other_suffix(z: *mut SN_env) -> c_int {
    let among_var: c_int;

    {
        let mlimit1: c_int;
        if (*z).c < *(*z).I.offset(1) {
            return 0;
        }
        mlimit1 = (*z).lb;
        (*z).lb = *(*z).I.offset(1);
        (*z).ket = (*z).c;
        if (*z).c - 1 <= (*z).lb
            || *(*z).p.offset((*z).c as isize - 1) as c_int >> 5 != 3
            || (1572992 >> (*(*z).p.offset((*z).c as isize - 1) as c_int & 0x1f)) & 1 == 0
        {
            (*z).lb = mlimit1;
            return 0;
        }
        among_var = find_among_b(z, A_2.as_ptr(), 5);
        if among_var == 0 {
            (*z).lb = mlimit1;
            return 0;
        }
        (*z).bra = (*z).c;
        (*z).lb = mlimit1;
    }
    match among_var {
        1 => {
            let ret = slice_del(z);
            if ret < 0 {
                return ret;
            }
        }
        2 => {
            if in_grouping_b_U(z, G_OST_ENDING.as_ptr(), 105, 118, 0) != 0 {
                return 0;
            }
            let ret = slice_from_s(z, 3, S_0.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        3 => {
            let ret = slice_from_s(z, 4, S_1.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        _ => {}
    }
    1
}

// ---------------------------------------------------------------------------
// exported entry points
// ---------------------------------------------------------------------------

pub unsafe extern "C" fn swedish_UTF_8_stem(z: *mut SN_env) -> c_int {
    {
        let c1 = (*z).c;
        {
            let ret = r_mark_regions(z);
            if ret < 0 {
                return ret;
            }
        }
        (*z).c = c1;
    }
    (*z).lb = (*z).c;
    (*z).c = (*z).l;

    {
        let m2 = (*z).l - (*z).c;
        {
            let ret = r_main_suffix(z);
            if ret < 0 {
                return ret;
            }
        }
        (*z).c = (*z).l - m2;
    }
    {
        let m3 = (*z).l - (*z).c;
        {
            let ret = r_consonant_pair(z);
            if ret < 0 {
                return ret;
            }
        }
        (*z).c = (*z).l - m3;
    }
    {
        let m4 = (*z).l - (*z).c;
        {
            let ret = r_other_suffix(z);
            if ret < 0 {
                return ret;
            }
        }
        (*z).c = (*z).l - m4;
    }
    (*z).c = (*z).lb;
    1
}

pub unsafe extern "C" fn swedish_UTF_8_create_env() -> *mut SN_env {
    SN_create_env(0, 2)
}

pub unsafe extern "C" fn swedish_UTF_8_close_env(z: *mut SN_env) {
    SN_close_env(z, 0)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snowball::api::{SIZE, SN_set_current};

    unsafe fn stem(word: &[u8]) -> Vec<u8> {
        let z = swedish_UTF_8_create_env();
        assert!(!z.is_null());
        let rc = SN_set_current(z, word.len() as c_int, word.as_ptr());
        assert!(rc >= 0);
        let sr = swedish_UTF_8_stem(z);
        assert!(sr >= 0);
        let out = std::slice::from_raw_parts((*z).p, SIZE((*z).p) as usize).to_vec();
        swedish_UTF_8_close_env(z);
        out
    }

    // A short root word with no strippable suffix stays unchanged.
    #[test]
    fn root_unchanged() {
        unsafe {
            // "katt" - too short to enter the R1 region with a suffix; the
            // consonant-pair "tt" sits at the very start so nothing is removed.
            let w = b"katt";
            assert_eq!(stem(w), w.to_vec());
        }
    }

    // Stemming is idempotent: stem(stem(w)) == stem(w).
    #[test]
    fn idempotent() {
        unsafe {
            // "studenterna" exercises the -erna main suffix.
            let once = stem(b"studenterna");
            assert!(!once.is_empty());
            let twice = stem(&once);
            assert_eq!(once, twice);
        }
    }

    // Result is non-empty and the algorithm did not crash / error.
    #[test]
    fn non_empty_no_crash() {
        unsafe {
            let out = stem(b"fastigheterna");
            assert!(!out.is_empty());
        }
    }
}
