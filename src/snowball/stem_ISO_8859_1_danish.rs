//! Danish Snowball stemmer (ISO-8859-1, single-byte).
//!
//! 1:1 translation of the generated
//! `src/backend/snowball/libstemmer/stem_ISO_8859_1_danish.c` (Snowball 2.2.0),
//! merged with its header. The runtime helpers come from
//! `crate::snowball::api` and `crate::snowball::utilities`. Because the input is
//! single-byte ISO-8859-1, this port uses the non-`_U` grouping helpers and a
//! plain byte advance/retreat instead of the UTF-8 skip helpers; high bytes such
//! as 0xF8 ('o' with stroke) are stored and compared as raw bytes.

use crate::prelude::*;

use crate::snowball::api::{among, symbol, SN_close_env, SN_create_env, SN_env, SIZE};
use crate::snowball::utilities::{
    eq_s_b, eq_v_b, find_among_b, in_grouping, in_grouping_b, out_grouping, slice_del,
    slice_from_s, slice_to,
};

// ---------------------------------------------------------------------------
// among string tables
// ---------------------------------------------------------------------------

static S_0_0: [symbol; 3] = [b'h', b'e', b'd'];
static S_0_1: [symbol; 5] = [b'e', b't', b'h', b'e', b'd'];
static S_0_2: [symbol; 4] = [b'e', b'r', b'e', b'd'];
static S_0_3: [symbol; 1] = [b'e'];
static S_0_4: [symbol; 5] = [b'e', b'r', b'e', b'd', b'e'];
static S_0_5: [symbol; 4] = [b'e', b'n', b'd', b'e'];
static S_0_6: [symbol; 6] = [b'e', b'r', b'e', b'n', b'd', b'e'];
static S_0_7: [symbol; 3] = [b'e', b'n', b'e'];
static S_0_8: [symbol; 4] = [b'e', b'r', b'n', b'e'];
static S_0_9: [symbol; 3] = [b'e', b'r', b'e'];
static S_0_10: [symbol; 2] = [b'e', b'n'];
static S_0_11: [symbol; 5] = [b'h', b'e', b'd', b'e', b'n'];
static S_0_12: [symbol; 4] = [b'e', b'r', b'e', b'n'];
static S_0_13: [symbol; 2] = [b'e', b'r'];
static S_0_14: [symbol; 5] = [b'h', b'e', b'd', b'e', b'r'];
static S_0_15: [symbol; 4] = [b'e', b'r', b'e', b'r'];
static S_0_16: [symbol; 1] = [b's'];
static S_0_17: [symbol; 4] = [b'h', b'e', b'd', b's'];
static S_0_18: [symbol; 2] = [b'e', b's'];
static S_0_19: [symbol; 5] = [b'e', b'n', b'd', b'e', b's'];
static S_0_20: [symbol; 7] = [b'e', b'r', b'e', b'n', b'd', b'e', b's'];
static S_0_21: [symbol; 4] = [b'e', b'n', b'e', b's'];
static S_0_22: [symbol; 5] = [b'e', b'r', b'n', b'e', b's'];
static S_0_23: [symbol; 4] = [b'e', b'r', b'e', b's'];
static S_0_24: [symbol; 3] = [b'e', b'n', b's'];
static S_0_25: [symbol; 6] = [b'h', b'e', b'd', b'e', b'n', b's'];
static S_0_26: [symbol; 5] = [b'e', b'r', b'e', b'n', b's'];
static S_0_27: [symbol; 3] = [b'e', b'r', b's'];
static S_0_28: [symbol; 3] = [b'e', b't', b's'];
static S_0_29: [symbol; 5] = [b'e', b'r', b'e', b't', b's'];
static S_0_30: [symbol; 2] = [b'e', b't'];
static S_0_31: [symbol; 4] = [b'e', b'r', b'e', b't'];

static A_0: [among; 32] = [
    among { s_size: 3, s: S_0_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_0_1.as_ptr(), substring_i: 0, result: 1, function: None },
    among { s_size: 4, s: S_0_2.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 1, s: S_0_3.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_0_4.as_ptr(), substring_i: 3, result: 1, function: None },
    among { s_size: 4, s: S_0_5.as_ptr(), substring_i: 3, result: 1, function: None },
    among { s_size: 6, s: S_0_6.as_ptr(), substring_i: 5, result: 1, function: None },
    among { s_size: 3, s: S_0_7.as_ptr(), substring_i: 3, result: 1, function: None },
    among { s_size: 4, s: S_0_8.as_ptr(), substring_i: 3, result: 1, function: None },
    among { s_size: 3, s: S_0_9.as_ptr(), substring_i: 3, result: 1, function: None },
    among { s_size: 2, s: S_0_10.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_0_11.as_ptr(), substring_i: 10, result: 1, function: None },
    among { s_size: 4, s: S_0_12.as_ptr(), substring_i: 10, result: 1, function: None },
    among { s_size: 2, s: S_0_13.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_0_14.as_ptr(), substring_i: 13, result: 1, function: None },
    among { s_size: 4, s: S_0_15.as_ptr(), substring_i: 13, result: 1, function: None },
    among { s_size: 1, s: S_0_16.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 4, s: S_0_17.as_ptr(), substring_i: 16, result: 1, function: None },
    among { s_size: 2, s: S_0_18.as_ptr(), substring_i: 16, result: 1, function: None },
    among { s_size: 5, s: S_0_19.as_ptr(), substring_i: 18, result: 1, function: None },
    among { s_size: 7, s: S_0_20.as_ptr(), substring_i: 19, result: 1, function: None },
    among { s_size: 4, s: S_0_21.as_ptr(), substring_i: 18, result: 1, function: None },
    among { s_size: 5, s: S_0_22.as_ptr(), substring_i: 18, result: 1, function: None },
    among { s_size: 4, s: S_0_23.as_ptr(), substring_i: 18, result: 1, function: None },
    among { s_size: 3, s: S_0_24.as_ptr(), substring_i: 16, result: 1, function: None },
    among { s_size: 6, s: S_0_25.as_ptr(), substring_i: 24, result: 1, function: None },
    among { s_size: 5, s: S_0_26.as_ptr(), substring_i: 24, result: 1, function: None },
    among { s_size: 3, s: S_0_27.as_ptr(), substring_i: 16, result: 1, function: None },
    among { s_size: 3, s: S_0_28.as_ptr(), substring_i: 16, result: 1, function: None },
    among { s_size: 5, s: S_0_29.as_ptr(), substring_i: 28, result: 1, function: None },
    among { s_size: 2, s: S_0_30.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_0_31.as_ptr(), substring_i: 30, result: 1, function: None },
];

static S_1_0: [symbol; 2] = [b'g', b'd'];
static S_1_1: [symbol; 2] = [b'd', b't'];
static S_1_2: [symbol; 2] = [b'g', b't'];
static S_1_3: [symbol; 2] = [b'k', b't'];

static A_1: [among; 4] = [
    among { s_size: 2, s: S_1_0.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_1_1.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_1_2.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_1_3.as_ptr(), substring_i: -1, result: -1, function: None },
];

static S_2_0: [symbol; 2] = [b'i', b'g'];
static S_2_1: [symbol; 3] = [b'l', b'i', b'g'];
static S_2_2: [symbol; 4] = [b'e', b'l', b'i', b'g'];
static S_2_3: [symbol; 3] = [b'e', b'l', b's'];
static S_2_4: [symbol; 4] = [b'l', 0xF8, b's', b't'];

static A_2: [among; 5] = [
    among { s_size: 2, s: S_2_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_2_1.as_ptr(), substring_i: 0, result: 1, function: None },
    among { s_size: 4, s: S_2_2.as_ptr(), substring_i: 1, result: 1, function: None },
    among { s_size: 3, s: S_2_3.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_2_4.as_ptr(), substring_i: -1, result: 2, function: None },
];

// ---------------------------------------------------------------------------
// grouping bit tables
// ---------------------------------------------------------------------------

static G_C: [c_uchar; 4] = [119, 223, 119, 1];

static G_V: [c_uchar; 19] = [
    17, 65, 16, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 48, 0, 128,
];

static G_S_ENDING: [c_uchar; 17] = [
    239, 254, 42, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 16,
];

// ---------------------------------------------------------------------------
// literal strings used by slice_from_s / eq_s_b
// ---------------------------------------------------------------------------

static S_0: [symbol; 2] = [b's', b't'];
static S_1: [symbol; 2] = [b'i', b'g'];
static S_2: [symbol; 3] = [b'l', 0xF8, b's'];

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
            || (1851440 >> (*(*z).p.offset(((*z).c - 1) as isize) as c_int & 0x1f)) & 1 == 0
        {
            (*z).lb = mlimit1;
            return 0;
        }
        among_var = find_among_b(z, A_0.as_ptr(), 32);
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
            if in_grouping_b(z, G_S_ENDING.as_ptr(), 97, 229, 0) != 0 {
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
        let m_test1 = (*z).l - (*z).c;

        {
            let mlimit2;
            if (*z).c < *(*z).I.offset(1) {
                return 0;
            }
            mlimit2 = (*z).lb;
            (*z).lb = *(*z).I.offset(1);
            (*z).ket = (*z).c;
            if (*z).c - 1 <= (*z).lb
                || (*(*z).p.offset(((*z).c - 1) as isize) != 100
                    && *(*z).p.offset(((*z).c - 1) as isize) != 116)
            {
                (*z).lb = mlimit2;
                return 0;
            }
            if find_among_b(z, A_1.as_ptr(), 4) == 0 {
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
    let among_var;
    {
        let m1 = (*z).l - (*z).c;
        'lab0: {
            (*z).ket = (*z).c;
            if eq_s_b(z, 2, S_0.as_ptr()) == 0 {
                break 'lab0;
            }
            (*z).bra = (*z).c;
            if eq_s_b(z, 2, S_1.as_ptr()) == 0 {
                break 'lab0;
            }
            let ret = slice_del(z);
            if ret < 0 {
                return ret;
            }
        }
        (*z).c = (*z).l - m1;
    }

    {
        let mlimit2;
        if (*z).c < *(*z).I.offset(1) {
            return 0;
        }
        mlimit2 = (*z).lb;
        (*z).lb = *(*z).I.offset(1);
        (*z).ket = (*z).c;
        if (*z).c - 1 <= (*z).lb
            || *(*z).p.offset(((*z).c - 1) as isize) as c_int >> 5 != 3
            || (1572992 >> (*(*z).p.offset(((*z).c - 1) as isize) as c_int & 0x1f)) & 1 == 0
        {
            (*z).lb = mlimit2;
            return 0;
        }
        among_var = find_among_b(z, A_2.as_ptr(), 5);
        if among_var == 0 {
            (*z).lb = mlimit2;
            return 0;
        }
        (*z).bra = (*z).c;
        (*z).lb = mlimit2;
    }
    match among_var {
        1 => {
            {
                let ret = slice_del(z);
                if ret < 0 {
                    return ret;
                }
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
        }
        2 => {
            let ret = slice_from_s(z, 3, S_2.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        _ => {}
    }
    1
}

unsafe fn r_undouble(z: *mut SN_env) -> c_int {
    {
        let mlimit1;
        if (*z).c < *(*z).I.offset(1) {
            return 0;
        }
        mlimit1 = (*z).lb;
        (*z).lb = *(*z).I.offset(1);
        (*z).ket = (*z).c;
        if in_grouping_b(z, G_C.as_ptr(), 98, 122, 0) != 0 {
            (*z).lb = mlimit1;
            return 0;
        }
        (*z).bra = (*z).c;
        *(*z).S.offset(0) = slice_to(z, *(*z).S.offset(0));
        if (*(*z).S.offset(0)).is_null() {
            return -1;
        }
        (*z).lb = mlimit1;
    }
    if eq_v_b(z, *(*z).S.offset(0)) == 0 {
        return 0;
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
pub unsafe extern "C" fn danish_ISO_8859_1_stem(z: *mut SN_env) -> c_int {
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
    {
        let m5 = (*z).l - (*z).c;
        {
            let ret = r_undouble(z);
            if ret < 0 {
                return ret;
            }
        }
        (*z).c = (*z).l - m5;
    }
    (*z).c = (*z).lb;
    1
}

#[no_mangle]
pub unsafe extern "C" fn danish_ISO_8859_1_create_env() -> *mut SN_env {
    SN_create_env(1, 2)
}

#[no_mangle]
pub unsafe extern "C" fn danish_ISO_8859_1_close_env(z: *mut SN_env) {
    SN_close_env(z, 1)
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
        let z = danish_ISO_8859_1_create_env();
        assert!(!z.is_null());
        let rc = SN_set_current(z, word.len() as c_int, word.as_ptr());
        assert!(rc >= 0);
        let rc = danish_ISO_8859_1_stem(z);
        assert!(rc >= 0, "stem returned {rc}");
        let out = std::slice::from_raw_parts((*z).p, SIZE((*z).p) as usize).to_vec();
        danish_ISO_8859_1_close_env(z);
        out
    }

    // A short root with no strippable suffix is returned unchanged.
    #[test]
    fn short_root_unchanged() {
        unsafe {
            assert_eq!(stem(b"bil"), b"bil".to_vec());
        }
    }

    // Idempotence: stemming a stem yields the same stem. High bytes are raw
    // ISO-8859-1 (single-byte), e.g. 0xE5 for 'a' with ring.
    #[test]
    fn idempotent() {
        unsafe {
            for w in [
                &b"hentede"[..],
                &b"venskab"[..],
                &b"unders\xe5gelse"[..],
                &b"forhold"[..],
                &b"sandheder"[..],
            ] {
                let once = stem(w);
                let twice = stem(&once);
                assert_eq!(once, twice, "not idempotent for {:?}", w);
                assert!(!once.is_empty());
            }
        }
    }

    // The "heds" / "hed" suffix family collapses; result must be non-empty and
    // cannot grow past the input length.
    #[test]
    fn suffix_stripped_nonempty() {
        unsafe {
            let r = stem(b"sandheder");
            assert!(!r.is_empty());
            assert!(r.len() <= "sandheder".len());
        }
    }
}
