//! Norwegian Snowball stemmer (ISO-8859-1, single-byte).
//!
//! 1:1 translation of the generated
//! `src/backend/snowball/libstemmer/stem_ISO_8859_1_norwegian.c` (Snowball
//! 2.2.0), merged with its header. The runtime helpers come from
//! `crate::snowball::api` and `crate::snowball::utilities`.
//!
//! Because the source charset is ISO-8859-1, every "character" is a single
//! byte: the non-`_U` grouping helpers and plain byte advances are used (no
//! UTF-8 skipping). High bytes (0x80-0xFF) are compared as raw `u8`.

use crate::prelude::*;

use crate::snowball::api::{among, symbol, SN_close_env, SN_create_env, SN_env, SIZE};
use crate::snowball::utilities::{
    find_among_b, in_grouping, in_grouping_b, out_grouping, out_grouping_b, slice_del,
    slice_from_s,
};

// ---------------------------------------------------------------------------
// among string tables
// ---------------------------------------------------------------------------

static S_0_0: [symbol; 1] = [b'a'];
static S_0_1: [symbol; 1] = [b'e'];
static S_0_2: [symbol; 3] = [b'e', b'd', b'e'];
static S_0_3: [symbol; 4] = [b'a', b'n', b'd', b'e'];
static S_0_4: [symbol; 4] = [b'e', b'n', b'd', b'e'];
static S_0_5: [symbol; 3] = [b'a', b'n', b'e'];
static S_0_6: [symbol; 3] = [b'e', b'n', b'e'];
static S_0_7: [symbol; 6] = [b'h', b'e', b't', b'e', b'n', b'e'];
static S_0_8: [symbol; 4] = [b'e', b'r', b't', b'e'];
static S_0_9: [symbol; 2] = [b'e', b'n'];
static S_0_10: [symbol; 5] = [b'h', b'e', b't', b'e', b'n'];
static S_0_11: [symbol; 2] = [b'a', b'r'];
static S_0_12: [symbol; 2] = [b'e', b'r'];
static S_0_13: [symbol; 5] = [b'h', b'e', b't', b'e', b'r'];
static S_0_14: [symbol; 1] = [b's'];
static S_0_15: [symbol; 2] = [b'a', b's'];
static S_0_16: [symbol; 2] = [b'e', b's'];
static S_0_17: [symbol; 4] = [b'e', b'd', b'e', b's'];
static S_0_18: [symbol; 5] = [b'e', b'n', b'd', b'e', b's'];
static S_0_19: [symbol; 4] = [b'e', b'n', b'e', b's'];
static S_0_20: [symbol; 7] = [b'h', b'e', b't', b'e', b'n', b'e', b's'];
static S_0_21: [symbol; 3] = [b'e', b'n', b's'];
static S_0_22: [symbol; 6] = [b'h', b'e', b't', b'e', b'n', b's'];
static S_0_23: [symbol; 3] = [b'e', b'r', b's'];
static S_0_24: [symbol; 3] = [b'e', b't', b's'];
static S_0_25: [symbol; 2] = [b'e', b't'];
static S_0_26: [symbol; 3] = [b'h', b'e', b't'];
static S_0_27: [symbol; 3] = [b'e', b'r', b't'];
static S_0_28: [symbol; 3] = [b'a', b's', b't'];

static A_0: [among; 29] = [
    among { s_size: 1, s: S_0_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 1, s: S_0_1.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_0_2.as_ptr(), substring_i: 1, result: 1, function: None },
    among { s_size: 4, s: S_0_3.as_ptr(), substring_i: 1, result: 1, function: None },
    among { s_size: 4, s: S_0_4.as_ptr(), substring_i: 1, result: 1, function: None },
    among { s_size: 3, s: S_0_5.as_ptr(), substring_i: 1, result: 1, function: None },
    among { s_size: 3, s: S_0_6.as_ptr(), substring_i: 1, result: 1, function: None },
    among { s_size: 6, s: S_0_7.as_ptr(), substring_i: 6, result: 1, function: None },
    among { s_size: 4, s: S_0_8.as_ptr(), substring_i: 1, result: 3, function: None },
    among { s_size: 2, s: S_0_9.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_0_10.as_ptr(), substring_i: 9, result: 1, function: None },
    among { s_size: 2, s: S_0_11.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_0_12.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_0_13.as_ptr(), substring_i: 12, result: 1, function: None },
    among { s_size: 1, s: S_0_14.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 2, s: S_0_15.as_ptr(), substring_i: 14, result: 1, function: None },
    among { s_size: 2, s: S_0_16.as_ptr(), substring_i: 14, result: 1, function: None },
    among { s_size: 4, s: S_0_17.as_ptr(), substring_i: 16, result: 1, function: None },
    among { s_size: 5, s: S_0_18.as_ptr(), substring_i: 16, result: 1, function: None },
    among { s_size: 4, s: S_0_19.as_ptr(), substring_i: 16, result: 1, function: None },
    among { s_size: 7, s: S_0_20.as_ptr(), substring_i: 19, result: 1, function: None },
    among { s_size: 3, s: S_0_21.as_ptr(), substring_i: 14, result: 1, function: None },
    among { s_size: 6, s: S_0_22.as_ptr(), substring_i: 21, result: 1, function: None },
    among { s_size: 3, s: S_0_23.as_ptr(), substring_i: 14, result: 1, function: None },
    among { s_size: 3, s: S_0_24.as_ptr(), substring_i: 14, result: 1, function: None },
    among { s_size: 2, s: S_0_25.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_0_26.as_ptr(), substring_i: 25, result: 1, function: None },
    among { s_size: 3, s: S_0_27.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 3, s: S_0_28.as_ptr(), substring_i: -1, result: 1, function: None },
];

static S_1_0: [symbol; 2] = [b'd', b't'];
static S_1_1: [symbol; 2] = [b'v', b't'];

static A_1: [among; 2] = [
    among { s_size: 2, s: S_1_0.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_1_1.as_ptr(), substring_i: -1, result: -1, function: None },
];

static S_2_0: [symbol; 3] = [b'l', b'e', b'g'];
static S_2_1: [symbol; 4] = [b'e', b'l', b'e', b'g'];
static S_2_2: [symbol; 2] = [b'i', b'g'];
static S_2_3: [symbol; 3] = [b'e', b'i', b'g'];
static S_2_4: [symbol; 3] = [b'l', b'i', b'g'];
static S_2_5: [symbol; 4] = [b'e', b'l', b'i', b'g'];
static S_2_6: [symbol; 3] = [b'e', b'l', b's'];
static S_2_7: [symbol; 3] = [b'l', b'o', b'v'];
static S_2_8: [symbol; 4] = [b'e', b'l', b'o', b'v'];
static S_2_9: [symbol; 4] = [b's', b'l', b'o', b'v'];
static S_2_10: [symbol; 7] = [b'h', b'e', b't', b's', b'l', b'o', b'v'];

static A_2: [among; 11] = [
    among { s_size: 3, s: S_2_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_2_1.as_ptr(), substring_i: 0, result: 1, function: None },
    among { s_size: 2, s: S_2_2.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_2_3.as_ptr(), substring_i: 2, result: 1, function: None },
    among { s_size: 3, s: S_2_4.as_ptr(), substring_i: 2, result: 1, function: None },
    among { s_size: 4, s: S_2_5.as_ptr(), substring_i: 4, result: 1, function: None },
    among { s_size: 3, s: S_2_6.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_2_7.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_2_8.as_ptr(), substring_i: 7, result: 1, function: None },
    among { s_size: 4, s: S_2_9.as_ptr(), substring_i: 7, result: 1, function: None },
    among { s_size: 7, s: S_2_10.as_ptr(), substring_i: 9, result: 1, function: None },
];

// ---------------------------------------------------------------------------
// grouping bit tables
// ---------------------------------------------------------------------------

static G_V: [c_uchar; 19] = [
    17, 65, 16, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 48, 0, 128,
];

static G_S_ENDING: [c_uchar; 4] = [119, 125, 149, 1];

// ---------------------------------------------------------------------------
// literal strings used by slice_from_s
// ---------------------------------------------------------------------------

static S_0: [symbol; 2] = [b'e', b'r'];

// ---------------------------------------------------------------------------
// rules
// ---------------------------------------------------------------------------

unsafe fn r_mark_regions(z: *mut SN_env) -> c_int {
    *(*z).I.offset(1) = (*z).l;
    {
        let c_test1 = (*z).c;
        (*z).c = (*z).c + 3;
        if (*z).c > (*z).l {
            return 0;
        }
        *(*z).I.offset(0) = (*z).c;
        (*z).c = c_test1;
    }

    if out_grouping(z, G_V.as_ptr(), 97, 248, 1) < 0 {
        return 0;
    }

    {
        let ret = in_grouping(z, G_V.as_ptr(), 97, 248, 1);
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
    let among_var;

    {
        let mlimit1;
        if (*z).c < *(*z).I.offset(1) {
            return 0;
        }
        mlimit1 = (*z).lb;
        (*z).lb = *(*z).I.offset(1);
        (*z).ket = (*z).c;
        if (*z).c <= (*z).lb
            || *(*z).p.offset(((*z).c - 1) as isize) as c_int >> 5 != 3
            || (1851426 >> (*(*z).p.offset(((*z).c - 1) as isize) as c_int & 0x1f)) & 1 == 0
        {
            (*z).lb = mlimit1;
            return 0;
        }
        among_var = find_among_b(z, A_0.as_ptr(), 29);
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
            'lab0: {
                {
                    let m2 = (*z).l - (*z).c;
                    'lab1: {
                        if in_grouping_b(z, G_S_ENDING.as_ptr(), 98, 122, 0) != 0 {
                            break 'lab1;
                        }
                        break 'lab0;
                    }
                    (*z).c = (*z).l - m2;
                    if (*z).c <= (*z).lb || *(*z).p.offset(((*z).c - 1) as isize) != b'k' {
                        return 0;
                    }
                    (*z).c -= 1;
                    if out_grouping_b(z, G_V.as_ptr(), 97, 248, 0) != 0 {
                        return 0;
                    }
                }
            }
            let ret = slice_del(z);
            if ret < 0 {
                return ret;
            }
        }
        3 => {
            let ret = slice_from_s(z, 2, S_0.as_ptr());
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
        let m_test1 = (*z).l - (*z).c;

        {
            let mlimit2;
            if (*z).c < *(*z).I.offset(1) {
                return 0;
            }
            mlimit2 = (*z).lb;
            (*z).lb = *(*z).I.offset(1);
            (*z).ket = (*z).c;
            if (*z).c - 1 <= (*z).lb || *(*z).p.offset(((*z).c - 1) as isize) != 116 {
                (*z).lb = mlimit2;
                return 0;
            }
            if find_among_b(z, A_1.as_ptr(), 2) == 0 {
                (*z).lb = mlimit2;
                return 0;
            }
            (*z).bra = (*z).c;
            (*z).lb = mlimit2;
        }
        (*z).c = (*z).l - m_test1;
    }
    if (*z).c <= (*z).lb {
        return 0;
    }
    (*z).c -= 1;
    (*z).bra = (*z).c;
    {
        let ret = slice_del(z);
        if ret < 0 {
            return ret;
        }
    }
    1
}

unsafe fn r_other_suffix(z: *mut SN_env) -> c_int {
    {
        let mlimit1;
        if (*z).c < *(*z).I.offset(1) {
            return 0;
        }
        mlimit1 = (*z).lb;
        (*z).lb = *(*z).I.offset(1);
        (*z).ket = (*z).c;
        if (*z).c - 1 <= (*z).lb
            || *(*z).p.offset(((*z).c - 1) as isize) as c_int >> 5 != 3
            || (4718720 >> (*(*z).p.offset(((*z).c - 1) as isize) as c_int & 0x1f)) & 1 == 0
        {
            (*z).lb = mlimit1;
            return 0;
        }
        if find_among_b(z, A_2.as_ptr(), 11) == 0 {
            (*z).lb = mlimit1;
            return 0;
        }
        (*z).bra = (*z).c;
        (*z).lb = mlimit1;
    }
    {
        let ret = slice_del(z);
        if ret < 0 {
            return ret;
        }
    }
    1
}

// ---------------------------------------------------------------------------
// exported entry points
// ---------------------------------------------------------------------------

#[no_mangle]
pub unsafe extern "C" fn norwegian_ISO_8859_1_stem(z: *mut SN_env) -> c_int {
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

#[no_mangle]
pub unsafe extern "C" fn norwegian_ISO_8859_1_create_env() -> *mut SN_env {
    SN_create_env(0, 2)
}

#[no_mangle]
pub unsafe extern "C" fn norwegian_ISO_8859_1_close_env(z: *mut SN_env) {
    SN_close_env(z, 0)
}

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snowball::api::SN_set_current;

    // Run the full create -> set -> stem -> read -> close cycle and return the
    // stemmed bytes.
    unsafe fn stem(word: &[u8]) -> Vec<u8> {
        let z = norwegian_ISO_8859_1_create_env();
        assert!(!z.is_null());
        let rc = SN_set_current(z, word.len() as c_int, word.as_ptr());
        assert!(rc >= 0);
        let rc = norwegian_ISO_8859_1_stem(z);
        assert!(rc >= 0, "stem returned {rc}");
        let out = std::slice::from_raw_parts((*z).p, SIZE((*z).p) as usize).to_vec();
        norwegian_ISO_8859_1_close_env(z);
        out
    }

    // A short root with no strippable suffix is returned unchanged.
    #[test]
    fn short_root_unchanged() {
        unsafe {
            assert_eq!(stem(b"bil"), b"bil".to_vec());
        }
    }

    // Idempotence: stemming a stem yields the same stem. Includes an ISO-8859-1
    // high byte (0xE5 = 'a-ring') to exercise the single-byte path.
    #[test]
    fn idempotent() {
        unsafe {
            for w in [
                &b"hetenes"[..],
                &b"erte"[..],
                &b"forskjellige"[..],
                &b"\xe5pen"[..],
            ] {
                let once = stem(w);
                let twice = stem(&once);
                assert_eq!(once, twice, "not idempotent for {:?}", w);
                assert!(!once.is_empty());
            }
        }
    }

    // A genuine suffix collapses and the result stays non-empty and no longer
    // than the input.
    #[test]
    fn suffix_stripped_nonempty() {
        unsafe {
            let r = stem(b"universitetet");
            assert!(!r.is_empty());
            assert!(r.len() <= "universitetet".len());
        }
    }
}
