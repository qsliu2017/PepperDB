//! Snowball Finnish (ISO_8859_1) stemmer.
//!
//! 1:1 translation of the generated
//! `src/backend/snowball/libstemmer/stem_ISO_8859_1_finnish.c` (Snowball 2.2.0),
//! merged with its declarations header
//! `src/include/snowball/libstemmer/stem_ISO_8859_1_finnish.h`.
//!
//! ISO_8859_1 is a SINGLE-BYTE encoding: this uses the non-`_U` grouping
//! helpers and plain single-byte advance (`z->c--`) instead of the UTF-8
//! `skip_*_utf8` helpers. The high characters 0xE4 ('a-umlaut') and 0xF6
//! ('o-umlaut') are single raw bytes rather than two-byte UTF-8 sequences.
//!
//! The libstemmer runtime is the ported Rust runtime in `crate::snowball::{api,
//! utilities}`; this file only contains the language-specific generated tables
//! and the step functions plus the three exported env/stem entry points.

use crate::prelude::*;

use crate::snowball::api::{among, symbol, SN_close_env, SN_create_env, SN_env};
use crate::snowball::utilities::{
    eq_s_b, eq_v_b, find_among_b, in_grouping, in_grouping_b, out_grouping, slice_del,
    slice_from_s, slice_to,
};

// ---------------------------------------------------------------------------
// among string literals + tables
// ---------------------------------------------------------------------------

static S_0_0: [symbol; 2] = [b'p', b'a'];
static S_0_1: [symbol; 3] = [b's', b't', b'i'];
static S_0_2: [symbol; 4] = [b'k', b'a', b'a', b'n'];
static S_0_3: [symbol; 3] = [b'h', b'a', b'n'];
static S_0_4: [symbol; 3] = [b'k', b'i', b'n'];
static S_0_5: [symbol; 3] = [b'h', 0xE4, b'n'];
static S_0_6: [symbol; 4] = [b'k', 0xE4, 0xE4, b'n'];
static S_0_7: [symbol; 2] = [b'k', b'o'];
static S_0_8: [symbol; 2] = [b'p', 0xE4];
static S_0_9: [symbol; 2] = [b'k', 0xF6];

static A_0: [among; 10] = [
    among { s_size: 2, s: S_0_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_0_1.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 4, s: S_0_2.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_0_3.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_0_4.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_0_5.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_0_6.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_0_7.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_0_8.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_0_9.as_ptr(), substring_i: -1, result: 1, function: None },
];

static S_1_0: [symbol; 3] = [b'l', b'l', b'a'];
static S_1_1: [symbol; 2] = [b'n', b'a'];
static S_1_2: [symbol; 3] = [b's', b's', b'a'];
static S_1_3: [symbol; 2] = [b't', b'a'];
static S_1_4: [symbol; 3] = [b'l', b't', b'a'];
static S_1_5: [symbol; 3] = [b's', b't', b'a'];

static A_1: [among; 6] = [
    among { s_size: 3, s: S_1_0.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_1_1.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_1_2.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_1_3.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_1_4.as_ptr(), substring_i: 3, result: -1, function: None },
    among { s_size: 3, s: S_1_5.as_ptr(), substring_i: 3, result: -1, function: None },
];

static S_2_0: [symbol; 3] = [b'l', b'l', 0xE4];
static S_2_1: [symbol; 2] = [b'n', 0xE4];
static S_2_2: [symbol; 3] = [b's', b's', 0xE4];
static S_2_3: [symbol; 2] = [b't', 0xE4];
static S_2_4: [symbol; 3] = [b'l', b't', 0xE4];
static S_2_5: [symbol; 3] = [b's', b't', 0xE4];

static A_2: [among; 6] = [
    among { s_size: 3, s: S_2_0.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_2_1.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_2_2.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_2_3.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_2_4.as_ptr(), substring_i: 3, result: -1, function: None },
    among { s_size: 3, s: S_2_5.as_ptr(), substring_i: 3, result: -1, function: None },
];

static S_3_0: [symbol; 3] = [b'l', b'l', b'e'];
static S_3_1: [symbol; 3] = [b'i', b'n', b'e'];

static A_3: [among; 2] = [
    among { s_size: 3, s: S_3_0.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_3_1.as_ptr(), substring_i: -1, result: -1, function: None },
];

static S_4_0: [symbol; 3] = [b'n', b's', b'a'];
static S_4_1: [symbol; 3] = [b'm', b'm', b'e'];
static S_4_2: [symbol; 3] = [b'n', b'n', b'e'];
static S_4_3: [symbol; 2] = [b'n', b'i'];
static S_4_4: [symbol; 2] = [b's', b'i'];
static S_4_5: [symbol; 2] = [b'a', b'n'];
static S_4_6: [symbol; 2] = [b'e', b'n'];
static S_4_7: [symbol; 2] = [0xE4, b'n'];
static S_4_8: [symbol; 3] = [b'n', b's', 0xE4];

static A_4: [among; 9] = [
    among { s_size: 3, s: S_4_0.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 3, s: S_4_1.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 3, s: S_4_2.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 2, s: S_4_3.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 2, s: S_4_4.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_4_5.as_ptr(), substring_i: -1, result: 4, function: None },
    among { s_size: 2, s: S_4_6.as_ptr(), substring_i: -1, result: 6, function: None },
    among { s_size: 2, s: S_4_7.as_ptr(), substring_i: -1, result: 5, function: None },
    among { s_size: 3, s: S_4_8.as_ptr(), substring_i: -1, result: 3, function: None },
];

static S_5_0: [symbol; 2] = [b'a', b'a'];
static S_5_1: [symbol; 2] = [b'e', b'e'];
static S_5_2: [symbol; 2] = [b'i', b'i'];
static S_5_3: [symbol; 2] = [b'o', b'o'];
static S_5_4: [symbol; 2] = [b'u', b'u'];
static S_5_5: [symbol; 2] = [0xE4, 0xE4];
static S_5_6: [symbol; 2] = [0xF6, 0xF6];

static A_5: [among; 7] = [
    among { s_size: 2, s: S_5_0.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_5_1.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_5_2.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_5_3.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_5_4.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_5_5.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_5_6.as_ptr(), substring_i: -1, result: -1, function: None },
];

static S_6_0: [symbol; 1] = [b'a'];
static S_6_1: [symbol; 3] = [b'l', b'l', b'a'];
static S_6_2: [symbol; 2] = [b'n', b'a'];
static S_6_3: [symbol; 3] = [b's', b's', b'a'];
static S_6_4: [symbol; 2] = [b't', b'a'];
static S_6_5: [symbol; 3] = [b'l', b't', b'a'];
static S_6_6: [symbol; 3] = [b's', b't', b'a'];
static S_6_7: [symbol; 3] = [b't', b't', b'a'];
static S_6_8: [symbol; 3] = [b'l', b'l', b'e'];
static S_6_9: [symbol; 3] = [b'i', b'n', b'e'];
static S_6_10: [symbol; 3] = [b'k', b's', b'i'];
static S_6_11: [symbol; 1] = [b'n'];
static S_6_12: [symbol; 3] = [b'h', b'a', b'n'];
static S_6_13: [symbol; 3] = [b'd', b'e', b'n'];
static S_6_14: [symbol; 4] = [b's', b'e', b'e', b'n'];
static S_6_15: [symbol; 3] = [b'h', b'e', b'n'];
static S_6_16: [symbol; 4] = [b't', b't', b'e', b'n'];
static S_6_17: [symbol; 3] = [b'h', b'i', b'n'];
static S_6_18: [symbol; 4] = [b's', b'i', b'i', b'n'];
static S_6_19: [symbol; 3] = [b'h', b'o', b'n'];
static S_6_20: [symbol; 3] = [b'h', 0xE4, b'n'];
static S_6_21: [symbol; 3] = [b'h', 0xF6, b'n'];
static S_6_22: [symbol; 1] = [0xE4];
static S_6_23: [symbol; 3] = [b'l', b'l', 0xE4];
static S_6_24: [symbol; 2] = [b'n', 0xE4];
static S_6_25: [symbol; 3] = [b's', b's', 0xE4];
static S_6_26: [symbol; 2] = [b't', 0xE4];
static S_6_27: [symbol; 3] = [b'l', b't', 0xE4];
static S_6_28: [symbol; 3] = [b's', b't', 0xE4];
static S_6_29: [symbol; 3] = [b't', b't', 0xE4];

static A_6: [among; 30] = [
    among { s_size: 1, s: S_6_0.as_ptr(), substring_i: -1, result: 8, function: None },
    among { s_size: 3, s: S_6_1.as_ptr(), substring_i: 0, result: -1, function: None },
    among { s_size: 2, s: S_6_2.as_ptr(), substring_i: 0, result: -1, function: None },
    among { s_size: 3, s: S_6_3.as_ptr(), substring_i: 0, result: -1, function: None },
    among { s_size: 2, s: S_6_4.as_ptr(), substring_i: 0, result: -1, function: None },
    among { s_size: 3, s: S_6_5.as_ptr(), substring_i: 4, result: -1, function: None },
    among { s_size: 3, s: S_6_6.as_ptr(), substring_i: 4, result: -1, function: None },
    among { s_size: 3, s: S_6_7.as_ptr(), substring_i: 4, result: 2, function: None },
    among { s_size: 3, s: S_6_8.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_6_9.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_6_10.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 1, s: S_6_11.as_ptr(), substring_i: -1, result: 7, function: None },
    among { s_size: 3, s: S_6_12.as_ptr(), substring_i: 11, result: 1, function: None },
    among { s_size: 3, s: S_6_13.as_ptr(), substring_i: 11, result: -1, function: Some(r_VI) },
    among { s_size: 4, s: S_6_14.as_ptr(), substring_i: 11, result: -1, function: Some(r_LONG) },
    among { s_size: 3, s: S_6_15.as_ptr(), substring_i: 11, result: 2, function: None },
    among { s_size: 4, s: S_6_16.as_ptr(), substring_i: 11, result: -1, function: Some(r_VI) },
    among { s_size: 3, s: S_6_17.as_ptr(), substring_i: 11, result: 3, function: None },
    among { s_size: 4, s: S_6_18.as_ptr(), substring_i: 11, result: -1, function: Some(r_VI) },
    among { s_size: 3, s: S_6_19.as_ptr(), substring_i: 11, result: 4, function: None },
    among { s_size: 3, s: S_6_20.as_ptr(), substring_i: 11, result: 5, function: None },
    among { s_size: 3, s: S_6_21.as_ptr(), substring_i: 11, result: 6, function: None },
    among { s_size: 1, s: S_6_22.as_ptr(), substring_i: -1, result: 8, function: None },
    among { s_size: 3, s: S_6_23.as_ptr(), substring_i: 22, result: -1, function: None },
    among { s_size: 2, s: S_6_24.as_ptr(), substring_i: 22, result: -1, function: None },
    among { s_size: 3, s: S_6_25.as_ptr(), substring_i: 22, result: -1, function: None },
    among { s_size: 2, s: S_6_26.as_ptr(), substring_i: 22, result: -1, function: None },
    among { s_size: 3, s: S_6_27.as_ptr(), substring_i: 26, result: -1, function: None },
    among { s_size: 3, s: S_6_28.as_ptr(), substring_i: 26, result: -1, function: None },
    among { s_size: 3, s: S_6_29.as_ptr(), substring_i: 26, result: 2, function: None },
];

static S_7_0: [symbol; 3] = [b'e', b'j', b'a'];
static S_7_1: [symbol; 3] = [b'm', b'm', b'a'];
static S_7_2: [symbol; 4] = [b'i', b'm', b'm', b'a'];
static S_7_3: [symbol; 3] = [b'm', b'p', b'a'];
static S_7_4: [symbol; 4] = [b'i', b'm', b'p', b'a'];
static S_7_5: [symbol; 3] = [b'm', b'm', b'i'];
static S_7_6: [symbol; 4] = [b'i', b'm', b'm', b'i'];
static S_7_7: [symbol; 3] = [b'm', b'p', b'i'];
static S_7_8: [symbol; 4] = [b'i', b'm', b'p', b'i'];
static S_7_9: [symbol; 3] = [b'e', b'j', 0xE4];
static S_7_10: [symbol; 3] = [b'm', b'm', 0xE4];
static S_7_11: [symbol; 4] = [b'i', b'm', b'm', 0xE4];
static S_7_12: [symbol; 3] = [b'm', b'p', 0xE4];
static S_7_13: [symbol; 4] = [b'i', b'm', b'p', 0xE4];

static A_7: [among; 14] = [
    among { s_size: 3, s: S_7_0.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_7_1.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_7_2.as_ptr(), substring_i: 1, result: -1, function: None },
    among { s_size: 3, s: S_7_3.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_7_4.as_ptr(), substring_i: 3, result: -1, function: None },
    among { s_size: 3, s: S_7_5.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_7_6.as_ptr(), substring_i: 5, result: -1, function: None },
    among { s_size: 3, s: S_7_7.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_7_8.as_ptr(), substring_i: 7, result: -1, function: None },
    among { s_size: 3, s: S_7_9.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_7_10.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_7_11.as_ptr(), substring_i: 10, result: -1, function: None },
    among { s_size: 3, s: S_7_12.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_7_13.as_ptr(), substring_i: 12, result: -1, function: None },
];

static S_8_0: [symbol; 1] = [b'i'];
static S_8_1: [symbol; 1] = [b'j'];

static A_8: [among; 2] = [
    among { s_size: 1, s: S_8_0.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 1, s: S_8_1.as_ptr(), substring_i: -1, result: -1, function: None },
];

static S_9_0: [symbol; 3] = [b'm', b'm', b'a'];
static S_9_1: [symbol; 4] = [b'i', b'm', b'm', b'a'];

static A_9: [among; 2] = [
    among { s_size: 3, s: S_9_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_9_1.as_ptr(), substring_i: 0, result: -1, function: None },
];

static G_AEI: [c_uchar; 17] = [17, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 8];

static G_C: [c_uchar; 4] = [119, 223, 119, 1];

static G_V1: [c_uchar; 19] =
    [17, 65, 16, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 8, 0, 32];

static G_V2: [c_uchar; 19] =
    [17, 65, 16, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 8, 0, 32];

static G_PARTICLE_END: [c_uchar; 19] =
    [17, 97, 24, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 8, 0, 32];

static S_0: [symbol; 3] = [b'k', b's', b'e'];
static S_1: [symbol; 3] = [b'k', b's', b'i'];
static S_2: [symbol; 2] = [b'i', b'e'];
static S_3: [symbol; 2] = [b'p', b'o'];
static S_4: [symbol; 2] = [b'p', b'o'];

// ---------------------------------------------------------------------------
// step functions
// ---------------------------------------------------------------------------

unsafe fn r_mark_regions(z: *mut SN_env) -> c_int {
    *(*z).I.offset(1) = (*z).l;
    *(*z).I.offset(0) = (*z).l;

    if out_grouping(z, G_V1.as_ptr(), 97, 246, 1) < 0 {
        return 0;
    }

    {
        let ret = in_grouping(z, G_V1.as_ptr(), 97, 246, 1);
        if ret < 0 {
            return 0;
        }
        (*z).c += ret;
    }
    *(*z).I.offset(1) = (*z).c;

    if out_grouping(z, G_V1.as_ptr(), 97, 246, 1) < 0 {
        return 0;
    }

    {
        let ret = in_grouping(z, G_V1.as_ptr(), 97, 246, 1);
        if ret < 0 {
            return 0;
        }
        (*z).c += ret;
    }
    *(*z).I.offset(0) = (*z).c;
    1
}

unsafe fn r_R2(z: *mut SN_env) -> c_int {
    (*(*z).I.offset(0) <= (*z).c) as c_int
}

unsafe fn r_particle_etc(z: *mut SN_env) -> c_int {
    let among_var: c_int;

    {
        let mlimit1: c_int;
        if (*z).c < *(*z).I.offset(1) {
            return 0;
        }
        mlimit1 = (*z).lb;
        (*z).lb = *(*z).I.offset(1);
        (*z).ket = (*z).c;
        among_var = find_among_b(z, A_0.as_ptr(), 10);
        if among_var == 0 {
            (*z).lb = mlimit1;
            return 0;
        }
        (*z).bra = (*z).c;
        (*z).lb = mlimit1;
    }
    match among_var {
        1 => {
            if in_grouping_b(z, G_PARTICLE_END.as_ptr(), 97, 246, 0) != 0 {
                return 0;
            }
        }
        2 => {
            let ret = r_R2(z);
            if ret <= 0 {
                return ret;
            }
        }
        _ => {}
    }
    {
        let ret = slice_del(z);
        if ret < 0 {
            return ret;
        }
    }
    1
}

unsafe fn r_possessive(z: *mut SN_env) -> c_int {
    let among_var: c_int;

    {
        let mlimit1: c_int;
        if (*z).c < *(*z).I.offset(1) {
            return 0;
        }
        mlimit1 = (*z).lb;
        (*z).lb = *(*z).I.offset(1);
        (*z).ket = (*z).c;
        among_var = find_among_b(z, A_4.as_ptr(), 9);
        if among_var == 0 {
            (*z).lb = mlimit1;
            return 0;
        }
        (*z).bra = (*z).c;
        (*z).lb = mlimit1;
    }
    match among_var {
        1 => {
            {
                let m2 = (*z).l - (*z).c;
                let _ = m2;
                'lab0: {
                    if (*z).c <= (*z).lb || *(*z).p.offset((*z).c as isize - 1) != b'k' {
                        break 'lab0;
                    }
                    (*z).c -= 1;
                    return 0;
                }
                (*z).c = (*z).l - m2;
            }
            {
                let ret = slice_del(z);
                if ret < 0 {
                    return ret;
                }
            }
        }
        2 => {
            {
                let ret = slice_del(z);
                if ret < 0 {
                    return ret;
                }
            }
            (*z).ket = (*z).c;
            if eq_s_b(z, 3, S_0.as_ptr()) == 0 {
                return 0;
            }
            (*z).bra = (*z).c;
            {
                let ret = slice_from_s(z, 3, S_1.as_ptr());
                if ret < 0 {
                    return ret;
                }
            }
        }
        3 => {
            let ret = slice_del(z);
            if ret < 0 {
                return ret;
            }
        }
        4 => {
            if (*z).c - 1 <= (*z).lb || *(*z).p.offset((*z).c as isize - 1) as c_int != 97 {
                return 0;
            }
            if find_among_b(z, A_1.as_ptr(), 6) == 0 {
                return 0;
            }
            let ret = slice_del(z);
            if ret < 0 {
                return ret;
            }
        }
        5 => {
            if (*z).c - 1 <= (*z).lb || *(*z).p.offset((*z).c as isize - 1) as c_int != 228 {
                return 0;
            }
            if find_among_b(z, A_2.as_ptr(), 6) == 0 {
                return 0;
            }
            let ret = slice_del(z);
            if ret < 0 {
                return ret;
            }
        }
        6 => {
            if (*z).c - 2 <= (*z).lb || *(*z).p.offset((*z).c as isize - 1) as c_int != 101 {
                return 0;
            }
            if find_among_b(z, A_3.as_ptr(), 2) == 0 {
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

unsafe extern "C" fn r_LONG(z: *mut SN_env) -> c_int {
    if find_among_b(z, A_5.as_ptr(), 7) == 0 {
        return 0;
    }
    1
}

unsafe extern "C" fn r_VI(z: *mut SN_env) -> c_int {
    if (*z).c <= (*z).lb || *(*z).p.offset((*z).c as isize - 1) != b'i' {
        return 0;
    }
    (*z).c -= 1;
    if in_grouping_b(z, G_V2.as_ptr(), 97, 246, 0) != 0 {
        return 0;
    }
    1
}

unsafe fn r_case_ending(z: *mut SN_env) -> c_int {
    let among_var: c_int;

    {
        let mlimit1: c_int;
        if (*z).c < *(*z).I.offset(1) {
            return 0;
        }
        mlimit1 = (*z).lb;
        (*z).lb = *(*z).I.offset(1);
        (*z).ket = (*z).c;
        among_var = find_among_b(z, A_6.as_ptr(), 30);
        if among_var == 0 {
            (*z).lb = mlimit1;
            return 0;
        }
        (*z).bra = (*z).c;
        (*z).lb = mlimit1;
    }
    match among_var {
        1 => {
            if (*z).c <= (*z).lb || *(*z).p.offset((*z).c as isize - 1) != b'a' {
                return 0;
            }
            (*z).c -= 1;
        }
        2 => {
            if (*z).c <= (*z).lb || *(*z).p.offset((*z).c as isize - 1) != b'e' {
                return 0;
            }
            (*z).c -= 1;
        }
        3 => {
            if (*z).c <= (*z).lb || *(*z).p.offset((*z).c as isize - 1) != b'i' {
                return 0;
            }
            (*z).c -= 1;
        }
        4 => {
            if (*z).c <= (*z).lb || *(*z).p.offset((*z).c as isize - 1) != b'o' {
                return 0;
            }
            (*z).c -= 1;
        }
        5 => {
            if (*z).c <= (*z).lb || *(*z).p.offset((*z).c as isize - 1) != 0xE4 {
                return 0;
            }
            (*z).c -= 1;
        }
        6 => {
            if (*z).c <= (*z).lb || *(*z).p.offset((*z).c as isize - 1) != 0xF6 {
                return 0;
            }
            (*z).c -= 1;
        }
        7 => {
            let m2 = (*z).l - (*z).c;
            let _ = m2;
            'lab0: {
                {
                    let m3 = (*z).l - (*z).c;
                    let _ = m3;
                    {
                        let m4 = (*z).l - (*z).c;
                        let _ = m4;
                        'lab1: {
                            'lab2: {
                                {
                                    let ret = r_LONG(z);
                                    if ret == 0 {
                                        break 'lab2;
                                    }
                                    if ret < 0 {
                                        return ret;
                                    }
                                }
                                break 'lab1;
                            }
                            // lab2:
                            (*z).c = (*z).l - m4;
                            if eq_s_b(z, 2, S_2.as_ptr()) == 0 {
                                (*z).c = (*z).l - m2;
                                break 'lab0;
                            }
                        }
                        // lab1:
                        (*z).c = (*z).l - m3;
                        if (*z).c <= (*z).lb {
                            (*z).c = (*z).l - m2;
                            break 'lab0;
                        }
                        (*z).c -= 1;
                    }
                    (*z).bra = (*z).c;
                }
            }
            // lab0:
        }
        8 => {
            if in_grouping_b(z, G_V1.as_ptr(), 97, 246, 0) != 0 {
                return 0;
            }
            if in_grouping_b(z, G_C.as_ptr(), 98, 122, 0) != 0 {
                return 0;
            }
        }
        _ => {}
    }
    {
        let ret = slice_del(z);
        if ret < 0 {
            return ret;
        }
    }
    *(*z).I.offset(2) = 1;
    1
}

unsafe fn r_other_endings(z: *mut SN_env) -> c_int {
    let among_var: c_int;

    {
        let mlimit1: c_int;
        if (*z).c < *(*z).I.offset(0) {
            return 0;
        }
        mlimit1 = (*z).lb;
        (*z).lb = *(*z).I.offset(0);
        (*z).ket = (*z).c;
        among_var = find_among_b(z, A_7.as_ptr(), 14);
        if among_var == 0 {
            (*z).lb = mlimit1;
            return 0;
        }
        (*z).bra = (*z).c;
        (*z).lb = mlimit1;
    }
    match among_var {
        1 => {
            let m2 = (*z).l - (*z).c;
            let _ = m2;
            'lab0: {
                if eq_s_b(z, 2, S_3.as_ptr()) == 0 {
                    break 'lab0;
                }
                return 0;
            }
            // lab0:
            (*z).c = (*z).l - m2;
        }
        _ => {}
    }
    {
        let ret = slice_del(z);
        if ret < 0 {
            return ret;
        }
    }
    1
}

unsafe fn r_i_plural(z: *mut SN_env) -> c_int {
    {
        let mlimit1: c_int;
        if (*z).c < *(*z).I.offset(1) {
            return 0;
        }
        mlimit1 = (*z).lb;
        (*z).lb = *(*z).I.offset(1);
        (*z).ket = (*z).c;
        if (*z).c <= (*z).lb
            || (*(*z).p.offset((*z).c as isize - 1) as c_int != 105
                && *(*z).p.offset((*z).c as isize - 1) as c_int != 106)
        {
            (*z).lb = mlimit1;
            return 0;
        }
        if find_among_b(z, A_8.as_ptr(), 2) == 0 {
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

unsafe fn r_t_plural(z: *mut SN_env) -> c_int {
    let among_var: c_int;

    {
        let mlimit1: c_int;
        if (*z).c < *(*z).I.offset(1) {
            return 0;
        }
        mlimit1 = (*z).lb;
        (*z).lb = *(*z).I.offset(1);
        (*z).ket = (*z).c;
        if (*z).c <= (*z).lb || *(*z).p.offset((*z).c as isize - 1) != b't' {
            (*z).lb = mlimit1;
            return 0;
        }
        (*z).c -= 1;
        (*z).bra = (*z).c;
        {
            let m_test2 = (*z).l - (*z).c;
            if in_grouping_b(z, G_V1.as_ptr(), 97, 246, 0) != 0 {
                (*z).lb = mlimit1;
                return 0;
            }
            (*z).c = (*z).l - m_test2;
        }
        {
            let ret = slice_del(z);
            if ret < 0 {
                return ret;
            }
        }
        (*z).lb = mlimit1;
    }

    {
        let mlimit3: c_int;
        if (*z).c < *(*z).I.offset(0) {
            return 0;
        }
        mlimit3 = (*z).lb;
        (*z).lb = *(*z).I.offset(0);
        (*z).ket = (*z).c;
        if (*z).c - 2 <= (*z).lb || *(*z).p.offset((*z).c as isize - 1) as c_int != 97 {
            (*z).lb = mlimit3;
            return 0;
        }
        among_var = find_among_b(z, A_9.as_ptr(), 2);
        if among_var == 0 {
            (*z).lb = mlimit3;
            return 0;
        }
        (*z).bra = (*z).c;
        (*z).lb = mlimit3;
    }
    match among_var {
        1 => {
            let m4 = (*z).l - (*z).c;
            let _ = m4;
            'lab0: {
                if eq_s_b(z, 2, S_4.as_ptr()) == 0 {
                    break 'lab0;
                }
                return 0;
            }
            // lab0:
            (*z).c = (*z).l - m4;
        }
        _ => {}
    }
    {
        let ret = slice_del(z);
        if ret < 0 {
            return ret;
        }
    }
    1
}

unsafe fn r_tidy(z: *mut SN_env) -> c_int {
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
            'lab0: {
                let m3 = (*z).l - (*z).c;
                let _ = m3;
                {
                    let ret = r_LONG(z);
                    if ret == 0 {
                        break 'lab0;
                    }
                    if ret < 0 {
                        return ret;
                    }
                }
                (*z).c = (*z).l - m3;
                (*z).ket = (*z).c;
                if (*z).c <= (*z).lb {
                    break 'lab0;
                }
                (*z).c -= 1;
                (*z).bra = (*z).c;
                {
                    let ret = slice_del(z);
                    if ret < 0 {
                        return ret;
                    }
                }
            }
            // lab0:
            (*z).c = (*z).l - m2;
        }
        {
            let m4 = (*z).l - (*z).c;
            let _ = m4;
            'lab1: {
                (*z).ket = (*z).c;
                if in_grouping_b(z, G_AEI.as_ptr(), 97, 228, 0) != 0 {
                    break 'lab1;
                }
                (*z).bra = (*z).c;
                if in_grouping_b(z, G_C.as_ptr(), 98, 122, 0) != 0 {
                    break 'lab1;
                }
                {
                    let ret = slice_del(z);
                    if ret < 0 {
                        return ret;
                    }
                }
            }
            // lab1:
            (*z).c = (*z).l - m4;
        }
        {
            let m5 = (*z).l - (*z).c;
            let _ = m5;
            'lab2: {
                (*z).ket = (*z).c;
                if (*z).c <= (*z).lb || *(*z).p.offset((*z).c as isize - 1) != b'j' {
                    break 'lab2;
                }
                (*z).c -= 1;
                (*z).bra = (*z).c;
                {
                    let m6 = (*z).l - (*z).c;
                    let _ = m6;
                    'lab3: {
                        'lab4: {
                            if (*z).c <= (*z).lb || *(*z).p.offset((*z).c as isize - 1) != b'o' {
                                break 'lab4;
                            }
                            (*z).c -= 1;
                            break 'lab3;
                        }
                        // lab4:
                        (*z).c = (*z).l - m6;
                        if (*z).c <= (*z).lb || *(*z).p.offset((*z).c as isize - 1) != b'u' {
                            break 'lab2;
                        }
                        (*z).c -= 1;
                    }
                    // lab3:
                }
                {
                    let ret = slice_del(z);
                    if ret < 0 {
                        return ret;
                    }
                }
            }
            // lab2:
            (*z).c = (*z).l - m5;
        }
        {
            let m7 = (*z).l - (*z).c;
            let _ = m7;
            'lab5: {
                (*z).ket = (*z).c;
                if (*z).c <= (*z).lb || *(*z).p.offset((*z).c as isize - 1) != b'o' {
                    break 'lab5;
                }
                (*z).c -= 1;
                (*z).bra = (*z).c;
                if (*z).c <= (*z).lb || *(*z).p.offset((*z).c as isize - 1) != b'j' {
                    break 'lab5;
                }
                (*z).c -= 1;
                {
                    let ret = slice_del(z);
                    if ret < 0 {
                        return ret;
                    }
                }
            }
            // lab5:
            (*z).c = (*z).l - m7;
        }
        (*z).lb = mlimit1;
    }

    if in_grouping_b(z, G_V1.as_ptr(), 97, 246, 1) < 0 {
        return 0;
    }
    (*z).ket = (*z).c;
    if in_grouping_b(z, G_C.as_ptr(), 98, 122, 0) != 0 {
        return 0;
    }
    (*z).bra = (*z).c;
    *(*z).S.offset(0) = slice_to(z, *(*z).S.offset(0));
    if (*(*z).S.offset(0)).is_null() {
        return -1;
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
pub unsafe extern "C" fn finnish_ISO_8859_1_stem(z: *mut SN_env) -> c_int {
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
    *(*z).I.offset(2) = 0;
    (*z).lb = (*z).c;
    (*z).c = (*z).l;

    {
        let m2 = (*z).l - (*z).c;
        {
            let ret = r_particle_etc(z);
            if ret < 0 {
                return ret;
            }
        }
        (*z).c = (*z).l - m2;
    }
    {
        let m3 = (*z).l - (*z).c;
        {
            let ret = r_possessive(z);
            if ret < 0 {
                return ret;
            }
        }
        (*z).c = (*z).l - m3;
    }
    {
        let m4 = (*z).l - (*z).c;
        {
            let ret = r_case_ending(z);
            if ret < 0 {
                return ret;
            }
        }
        (*z).c = (*z).l - m4;
    }
    {
        let m5 = (*z).l - (*z).c;
        {
            let ret = r_other_endings(z);
            if ret < 0 {
                return ret;
            }
        }
        (*z).c = (*z).l - m5;
    }

    'lab0: {
        'lab1: {
            if *(*z).I.offset(2) == 0 {
                break 'lab1;
            }
            {
                let m6 = (*z).l - (*z).c;
                {
                    let ret = r_i_plural(z);
                    if ret < 0 {
                        return ret;
                    }
                }
                (*z).c = (*z).l - m6;
            }
            break 'lab0;
        }
        // lab1:
        {
            let m7 = (*z).l - (*z).c;
            {
                let ret = r_t_plural(z);
                if ret < 0 {
                    return ret;
                }
            }
            (*z).c = (*z).l - m7;
        }
    }
    // lab0:
    {
        let m8 = (*z).l - (*z).c;
        {
            let ret = r_tidy(z);
            if ret < 0 {
                return ret;
            }
        }
        (*z).c = (*z).l - m8;
    }
    (*z).c = (*z).lb;
    1
}

#[no_mangle]
pub unsafe extern "C" fn finnish_ISO_8859_1_create_env() -> *mut SN_env {
    SN_create_env(1, 3)
}

#[no_mangle]
pub unsafe extern "C" fn finnish_ISO_8859_1_close_env(z: *mut SN_env) {
    SN_close_env(z, 1)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snowball::api::{SIZE, SN_set_current};

    unsafe fn stem(word: &[u8]) -> Vec<u8> {
        let z = finnish_ISO_8859_1_create_env();
        assert!(!z.is_null());
        let rc = SN_set_current(z, word.len() as c_int, word.as_ptr());
        assert!(rc >= 0);
        let sr = finnish_ISO_8859_1_stem(z);
        assert!(sr >= 0);
        let out = std::slice::from_raw_parts((*z).p, SIZE((*z).p) as usize).to_vec();
        finnish_ISO_8859_1_close_env(z);
        out
    }

    // A short root word with no strippable suffix stays unchanged.
    #[test]
    fn root_unchanged() {
        unsafe {
            // "talo" ('house') - too short to enter the regions, unchanged.
            let w = b"talo";
            assert_eq!(stem(w), w.to_vec());
        }
    }

    // Stemming is idempotent: stem(stem(w)) == stem(w).
    #[test]
    fn idempotent() {
        unsafe {
            // "talossa" exercises the -ssa case ending.
            let once = stem(b"talossa");
            assert!(!once.is_empty());
            let twice = stem(&once);
            assert_eq!(once, twice);

            // A word containing the ISO_8859_1 high byte 0xE4 ('a-umlaut').
            let once2 = stem(b"p\xe4iv\xe4n\xe4");
            assert!(!once2.is_empty());
            let twice2 = stem(&once2);
            assert_eq!(once2, twice2);

            // A word containing the ISO_8859_1 high byte 0xF6 ('o-umlaut').
            let once3 = stem(b"y\xf6ll\xe4");
            assert!(!once3.is_empty());
            let twice3 = stem(&once3);
            assert_eq!(once3, twice3);
        }
    }

    // Result is non-empty and the algorithm did not crash / error.
    #[test]
    fn non_empty_no_crash() {
        unsafe {
            let out = stem(b"kaupungissa");
            assert!(!out.is_empty());
        }
    }
}
