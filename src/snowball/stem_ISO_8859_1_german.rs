//! German Snowball stemmer (ISO-8859-1, single-byte).
//!
//! 1:1 translation of the generated
//! `src/backend/snowball/libstemmer/stem_ISO_8859_1_german.c` (Snowball 2.2.0),
//! merged with its header. The runtime helpers come from
//! `crate::snowball::api` and `crate::snowball::utilities`. Because the input is
//! single-byte ISO-8859-1, this port uses the non-`_U` grouping helpers and a
//! plain byte advance/retreat instead of the UTF-8 skip helpers; high bytes such
//! as 0xE4 ('a' with umlaut) and 0xDF (sharp s) are stored and compared as raw
//! bytes.

use crate::prelude::*;

use crate::snowball::api::{among, symbol, SN_close_env, SN_create_env, SN_env, SIZE};
use crate::snowball::utilities::{
    eq_s_b, find_among, find_among_b, in_grouping, in_grouping_b, out_grouping, slice_del,
    slice_from_s,
};

// ---------------------------------------------------------------------------
// among string tables
// ---------------------------------------------------------------------------

static S_0_1: [symbol; 2] = [b'a', b'e'];
static S_0_2: [symbol; 2] = [b'o', b'e'];
static S_0_3: [symbol; 2] = [b'q', b'u'];
static S_0_4: [symbol; 2] = [b'u', b'e'];
static S_0_5: [symbol; 1] = [0xDF];

static A_0: [among; 6] = [
    among { s_size: 0, s: core::ptr::null(), substring_i: -1, result: 5, function: None },
    among { s_size: 2, s: S_0_1.as_ptr(), substring_i: 0, result: 2, function: None },
    among { s_size: 2, s: S_0_2.as_ptr(), substring_i: 0, result: 3, function: None },
    among { s_size: 2, s: S_0_3.as_ptr(), substring_i: 0, result: -1, function: None },
    among { s_size: 2, s: S_0_4.as_ptr(), substring_i: 0, result: 4, function: None },
    among { s_size: 1, s: S_0_5.as_ptr(), substring_i: 0, result: 1, function: None },
];

static S_1_1: [symbol; 1] = [b'U'];
static S_1_2: [symbol; 1] = [b'Y'];
static S_1_3: [symbol; 1] = [0xE4];
static S_1_4: [symbol; 1] = [0xF6];
static S_1_5: [symbol; 1] = [0xFC];

static A_1: [among; 6] = [
    among { s_size: 0, s: core::ptr::null(), substring_i: -1, result: 5, function: None },
    among { s_size: 1, s: S_1_1.as_ptr(), substring_i: 0, result: 2, function: None },
    among { s_size: 1, s: S_1_2.as_ptr(), substring_i: 0, result: 1, function: None },
    among { s_size: 1, s: S_1_3.as_ptr(), substring_i: 0, result: 3, function: None },
    among { s_size: 1, s: S_1_4.as_ptr(), substring_i: 0, result: 4, function: None },
    among { s_size: 1, s: S_1_5.as_ptr(), substring_i: 0, result: 2, function: None },
];

static S_2_0: [symbol; 1] = [b'e'];
static S_2_1: [symbol; 2] = [b'e', b'm'];
static S_2_2: [symbol; 2] = [b'e', b'n'];
static S_2_3: [symbol; 7] = [b'e', b'r', b'i', b'n', b'n', b'e', b'n'];
static S_2_4: [symbol; 4] = [b'e', b'r', b'i', b'n'];
static S_2_5: [symbol; 2] = [b'l', b'n'];
static S_2_6: [symbol; 3] = [b'e', b'r', b'n'];
static S_2_7: [symbol; 2] = [b'e', b'r'];
static S_2_8: [symbol; 1] = [b's'];
static S_2_9: [symbol; 2] = [b'e', b's'];
static S_2_10: [symbol; 3] = [b'l', b'n', b's'];

static A_2: [among; 11] = [
    among { s_size: 1, s: S_2_0.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 2, s: S_2_1.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_2_2.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 7, s: S_2_3.as_ptr(), substring_i: 2, result: 2, function: None },
    among { s_size: 4, s: S_2_4.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 2, s: S_2_5.as_ptr(), substring_i: -1, result: 5, function: None },
    among { s_size: 3, s: S_2_6.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 2, s: S_2_7.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 1, s: S_2_8.as_ptr(), substring_i: -1, result: 4, function: None },
    among { s_size: 2, s: S_2_9.as_ptr(), substring_i: 8, result: 3, function: None },
    among { s_size: 3, s: S_2_10.as_ptr(), substring_i: 8, result: 5, function: None },
];

static S_3_0: [symbol; 2] = [b'e', b'n'];
static S_3_1: [symbol; 2] = [b'e', b'r'];
static S_3_2: [symbol; 2] = [b's', b't'];
static S_3_3: [symbol; 3] = [b'e', b's', b't'];

static A_3: [among; 4] = [
    among { s_size: 2, s: S_3_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_3_1.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_3_2.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 3, s: S_3_3.as_ptr(), substring_i: 2, result: 1, function: None },
];

static S_4_0: [symbol; 2] = [b'i', b'g'];
static S_4_1: [symbol; 4] = [b'l', b'i', b'c', b'h'];

static A_4: [among; 2] = [
    among { s_size: 2, s: S_4_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_4_1.as_ptr(), substring_i: -1, result: 1, function: None },
];

static S_5_0: [symbol; 3] = [b'e', b'n', b'd'];
static S_5_1: [symbol; 2] = [b'i', b'g'];
static S_5_2: [symbol; 3] = [b'u', b'n', b'g'];
static S_5_3: [symbol; 4] = [b'l', b'i', b'c', b'h'];
static S_5_4: [symbol; 4] = [b'i', b's', b'c', b'h'];
static S_5_5: [symbol; 2] = [b'i', b'k'];
static S_5_6: [symbol; 4] = [b'h', b'e', b'i', b't'];
static S_5_7: [symbol; 4] = [b'k', b'e', b'i', b't'];

static A_5: [among; 8] = [
    among { s_size: 3, s: S_5_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_5_1.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 3, s: S_5_2.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_5_3.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 4, s: S_5_4.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 2, s: S_5_5.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 4, s: S_5_6.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 4, s: S_5_7.as_ptr(), substring_i: -1, result: 4, function: None },
];

// ---------------------------------------------------------------------------
// grouping bit tables
// ---------------------------------------------------------------------------

static G_V: [c_uchar; 20] = [
    17, 65, 16, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 8, 0, 32, 8,
];

static G_S_ENDING: [c_uchar; 3] = [117, 30, 5];

static G_ST_ENDING: [c_uchar; 3] = [117, 30, 4];

// ---------------------------------------------------------------------------
// literal strings used by slice_from_s / eq_s_b
// ---------------------------------------------------------------------------

static S_0: [symbol; 1] = [b'U'];
static S_1: [symbol; 1] = [b'Y'];
static S_2: [symbol; 2] = [b's', b's'];
static S_3: [symbol; 1] = [0xE4];
static S_4: [symbol; 1] = [0xF6];
static S_5: [symbol; 1] = [0xFC];
static S_6: [symbol; 1] = [b'y'];
static S_7: [symbol; 1] = [b'u'];
static S_8: [symbol; 1] = [b'a'];
static S_9: [symbol; 1] = [b'o'];
static S_10: [symbol; 4] = [b's', b'y', b's', b't'];
static S_11: [symbol; 3] = [b'n', b'i', b's'];
static S_12: [symbol; 1] = [b'l'];
static S_13: [symbol; 2] = [b'i', b'g'];
static S_14: [symbol; 2] = [b'e', b'r'];
static S_15: [symbol; 2] = [b'e', b'n'];

// ---------------------------------------------------------------------------
// rules
// ---------------------------------------------------------------------------

unsafe fn r_prelude(z: *mut SN_env) -> c_int {
    let mut among_var;
    {
        let c_test1 = (*z).c;
        'loop0: loop {
            let c2 = (*z).c;
            'lab0: {
                'loop1: loop {
                    let c3 = (*z).c;
                    'lab1: {
                        if in_grouping(z, G_V.as_ptr(), 97, 252, 0) != 0 {
                            break 'lab1;
                        }
                        (*z).bra = (*z).c;
                        'lab3: {
                            let c4 = (*z).c;
                            'lab3_inner: {
                                if (*z).c == (*z).l || *(*z).p.offset((*z).c as isize) != b'u' {
                                    break 'lab3_inner;
                                }
                                (*z).c += 1;
                                (*z).ket = (*z).c;
                                if in_grouping(z, G_V.as_ptr(), 97, 252, 0) != 0 {
                                    break 'lab3_inner;
                                }
                                {
                                    let ret = slice_from_s(z, 1, S_0.as_ptr());
                                    if ret < 0 {
                                        return ret;
                                    }
                                }
                                break 'lab3;
                            }
                            // lab3:
                            (*z).c = c4;
                            if (*z).c == (*z).l || *(*z).p.offset((*z).c as isize) != b'y' {
                                break 'lab1;
                            }
                            (*z).c += 1;
                            (*z).ket = (*z).c;
                            if in_grouping(z, G_V.as_ptr(), 97, 252, 0) != 0 {
                                break 'lab1;
                            }
                            {
                                let ret = slice_from_s(z, 1, S_1.as_ptr());
                                if ret < 0 {
                                    return ret;
                                }
                            }
                        }
                        // lab2:
                        (*z).c = c3;
                        break 'loop1;
                    }
                    // lab1:
                    (*z).c = c3;
                    if (*z).c >= (*z).l {
                        break 'lab0;
                    }
                    (*z).c += 1;
                }
                continue 'loop0;
            }
            // lab0:
            (*z).c = c2;
            break 'loop0;
        }
        (*z).c = c_test1;
    }
    'loop2: loop {
        let c5 = (*z).c;
        'lab4: {
            (*z).bra = (*z).c;
            among_var = find_among(z, A_0.as_ptr(), 6);
            (*z).ket = (*z).c;
            match among_var {
                1 => {
                    let ret = slice_from_s(z, 2, S_2.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
                2 => {
                    let ret = slice_from_s(z, 1, S_3.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
                3 => {
                    let ret = slice_from_s(z, 1, S_4.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
                4 => {
                    let ret = slice_from_s(z, 1, S_5.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
                5 => {
                    if (*z).c >= (*z).l {
                        break 'lab4;
                    }
                    (*z).c += 1;
                }
                _ => {}
            }
            continue 'loop2;
        }
        // lab4:
        (*z).c = c5;
        break 'loop2;
    }
    1
}

unsafe fn r_mark_regions(z: *mut SN_env) -> c_int {
    *(*z).I.offset(2) = (*z).l;
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

    {
        let ret = out_grouping(z, G_V.as_ptr(), 97, 252, 1);
        if ret < 0 {
            return 0;
        }
        (*z).c += ret;
    }

    {
        let ret = in_grouping(z, G_V.as_ptr(), 97, 252, 1);
        if ret < 0 {
            return 0;
        }
        (*z).c += ret;
    }
    *(*z).I.offset(2) = (*z).c;

    'lab0: {
        if *(*z).I.offset(2) >= *(*z).I.offset(0) {
            break 'lab0;
        }
        *(*z).I.offset(2) = *(*z).I.offset(0);
    }

    {
        let ret = out_grouping(z, G_V.as_ptr(), 97, 252, 1);
        if ret < 0 {
            return 0;
        }
        (*z).c += ret;
    }

    {
        let ret = in_grouping(z, G_V.as_ptr(), 97, 252, 1);
        if ret < 0 {
            return 0;
        }
        (*z).c += ret;
    }
    *(*z).I.offset(1) = (*z).c;
    1
}

unsafe fn r_postlude(z: *mut SN_env) -> c_int {
    let mut among_var;
    'loop0: loop {
        let c1 = (*z).c;
        'lab0: {
            (*z).bra = (*z).c;
            among_var = find_among(z, A_1.as_ptr(), 6);
            (*z).ket = (*z).c;
            match among_var {
                1 => {
                    let ret = slice_from_s(z, 1, S_6.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
                2 => {
                    let ret = slice_from_s(z, 1, S_7.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
                3 => {
                    let ret = slice_from_s(z, 1, S_8.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
                4 => {
                    let ret = slice_from_s(z, 1, S_9.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
                5 => {
                    if (*z).c >= (*z).l {
                        break 'lab0;
                    }
                    (*z).c += 1;
                }
                _ => {}
            }
            continue 'loop0;
        }
        // lab0:
        (*z).c = c1;
        break 'loop0;
    }
    1
}

unsafe fn r_R1(z: *mut SN_env) -> c_int {
    (*(*z).I.offset(2) <= (*z).c) as c_int
}

unsafe fn r_R2(z: *mut SN_env) -> c_int {
    (*(*z).I.offset(1) <= (*z).c) as c_int
}

unsafe fn r_standard_suffix(z: *mut SN_env) -> c_int {
    let mut among_var;
    {
        let m1 = (*z).l - (*z).c;
        'lab0: {
            (*z).ket = (*z).c;
            if (*z).c <= (*z).lb
                || *(*z).p.offset(((*z).c - 1) as isize) as c_int >> 5 != 3
                || (811040 >> (*(*z).p.offset(((*z).c - 1) as isize) as c_int & 0x1f)) & 1 == 0
            {
                break 'lab0;
            }
            among_var = find_among_b(z, A_2.as_ptr(), 11);
            if among_var == 0 {
                break 'lab0;
            }
            (*z).bra = (*z).c;
            {
                let ret = r_R1(z);
                if ret == 0 {
                    break 'lab0;
                }
                if ret < 0 {
                    return ret;
                }
            }
            match among_var {
                1 => {
                    {
                        let m2 = (*z).l - (*z).c;
                        'lab1: {
                            if eq_s_b(z, 4, S_10.as_ptr()) != 0 {
                                break 'lab1;
                            }
                            break 'lab0;
                        }
                        // lab1:
                        (*z).c = (*z).l - m2;
                    }
                    let ret = slice_del(z);
                    if ret < 0 {
                        return ret;
                    }
                }
                2 => {
                    let ret = slice_del(z);
                    if ret < 0 {
                        return ret;
                    }
                }
                3 => {
                    {
                        let ret = slice_del(z);
                        if ret < 0 {
                            return ret;
                        }
                    }
                    {
                        let m3 = (*z).l - (*z).c;
                        'lab2: {
                            (*z).ket = (*z).c;
                            if (*z).c <= (*z).lb || *(*z).p.offset(((*z).c - 1) as isize) != b's' {
                                (*z).c = (*z).l - m3;
                                break 'lab2;
                            }
                            (*z).c -= 1;
                            (*z).bra = (*z).c;
                            if eq_s_b(z, 3, S_11.as_ptr()) == 0 {
                                (*z).c = (*z).l - m3;
                                break 'lab2;
                            }
                            let ret = slice_del(z);
                            if ret < 0 {
                                return ret;
                            }
                        }
                    }
                }
                4 => {
                    if in_grouping_b(z, G_S_ENDING.as_ptr(), 98, 116, 0) != 0 {
                        break 'lab0;
                    }
                    let ret = slice_del(z);
                    if ret < 0 {
                        return ret;
                    }
                }
                5 => {
                    let ret = slice_from_s(z, 1, S_12.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
                _ => {}
            }
        }
        // lab0:
        (*z).c = (*z).l - m1;
    }
    {
        let m4 = (*z).l - (*z).c;
        'lab3: {
            (*z).ket = (*z).c;
            if (*z).c - 1 <= (*z).lb
                || *(*z).p.offset(((*z).c - 1) as isize) as c_int >> 5 != 3
                || (1327104 >> (*(*z).p.offset(((*z).c - 1) as isize) as c_int & 0x1f)) & 1 == 0
            {
                break 'lab3;
            }
            among_var = find_among_b(z, A_3.as_ptr(), 4);
            if among_var == 0 {
                break 'lab3;
            }
            (*z).bra = (*z).c;
            {
                let ret = r_R1(z);
                if ret == 0 {
                    break 'lab3;
                }
                if ret < 0 {
                    return ret;
                }
            }
            match among_var {
                1 => {
                    let ret = slice_del(z);
                    if ret < 0 {
                        return ret;
                    }
                }
                2 => {
                    if in_grouping_b(z, G_ST_ENDING.as_ptr(), 98, 116, 0) != 0 {
                        break 'lab3;
                    }
                    (*z).c = (*z).c - 3;
                    if (*z).c < (*z).lb {
                        break 'lab3;
                    }
                    let ret = slice_del(z);
                    if ret < 0 {
                        return ret;
                    }
                }
                _ => {}
            }
        }
        // lab3:
        (*z).c = (*z).l - m4;
    }
    {
        let m5 = (*z).l - (*z).c;
        'lab4: {
            (*z).ket = (*z).c;
            if (*z).c - 1 <= (*z).lb
                || *(*z).p.offset(((*z).c - 1) as isize) as c_int >> 5 != 3
                || (1051024 >> (*(*z).p.offset(((*z).c - 1) as isize) as c_int & 0x1f)) & 1 == 0
            {
                break 'lab4;
            }
            among_var = find_among_b(z, A_5.as_ptr(), 8);
            if among_var == 0 {
                break 'lab4;
            }
            (*z).bra = (*z).c;
            {
                let ret = r_R2(z);
                if ret == 0 {
                    break 'lab4;
                }
                if ret < 0 {
                    return ret;
                }
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
                        let m6 = (*z).l - (*z).c;
                        'lab5: {
                            (*z).ket = (*z).c;
                            if eq_s_b(z, 2, S_13.as_ptr()) == 0 {
                                (*z).c = (*z).l - m6;
                                break 'lab5;
                            }
                            (*z).bra = (*z).c;
                            {
                                let m7 = (*z).l - (*z).c;
                                'lab6: {
                                    if (*z).c <= (*z).lb
                                        || *(*z).p.offset(((*z).c - 1) as isize) != b'e'
                                    {
                                        break 'lab6;
                                    }
                                    (*z).c -= 1;
                                    (*z).c = (*z).l - m6;
                                    break 'lab5;
                                }
                                // lab6:
                                (*z).c = (*z).l - m7;
                            }
                            {
                                let ret = r_R2(z);
                                if ret == 0 {
                                    (*z).c = (*z).l - m6;
                                    break 'lab5;
                                }
                                if ret < 0 {
                                    return ret;
                                }
                            }
                            let ret = slice_del(z);
                            if ret < 0 {
                                return ret;
                            }
                        }
                    }
                }
                2 => {
                    {
                        let m8 = (*z).l - (*z).c;
                        'lab7: {
                            if (*z).c <= (*z).lb
                                || *(*z).p.offset(((*z).c - 1) as isize) != b'e'
                            {
                                break 'lab7;
                            }
                            (*z).c -= 1;
                            break 'lab4;
                        }
                        // lab7:
                        (*z).c = (*z).l - m8;
                    }
                    let ret = slice_del(z);
                    if ret < 0 {
                        return ret;
                    }
                }
                3 => {
                    {
                        let ret = slice_del(z);
                        if ret < 0 {
                            return ret;
                        }
                    }
                    {
                        let m9 = (*z).l - (*z).c;
                        'lab8: {
                            (*z).ket = (*z).c;
                            'lab9: {
                                let m10 = (*z).l - (*z).c;
                                'lab10: {
                                    if eq_s_b(z, 2, S_14.as_ptr()) == 0 {
                                        break 'lab10;
                                    }
                                    break 'lab9;
                                }
                                // lab10:
                                (*z).c = (*z).l - m10;
                                if eq_s_b(z, 2, S_15.as_ptr()) == 0 {
                                    (*z).c = (*z).l - m9;
                                    break 'lab8;
                                }
                            }
                            // lab9:
                            (*z).bra = (*z).c;
                            {
                                let ret = r_R1(z);
                                if ret == 0 {
                                    (*z).c = (*z).l - m9;
                                    break 'lab8;
                                }
                                if ret < 0 {
                                    return ret;
                                }
                            }
                            let ret = slice_del(z);
                            if ret < 0 {
                                return ret;
                            }
                        }
                    }
                }
                4 => {
                    {
                        let ret = slice_del(z);
                        if ret < 0 {
                            return ret;
                        }
                    }
                    {
                        let m11 = (*z).l - (*z).c;
                        'lab11: {
                            (*z).ket = (*z).c;
                            if (*z).c - 1 <= (*z).lb
                                || (*(*z).p.offset(((*z).c - 1) as isize) != 103
                                    && *(*z).p.offset(((*z).c - 1) as isize) != 104)
                            {
                                (*z).c = (*z).l - m11;
                                break 'lab11;
                            }
                            if find_among_b(z, A_4.as_ptr(), 2) == 0 {
                                (*z).c = (*z).l - m11;
                                break 'lab11;
                            }
                            (*z).bra = (*z).c;
                            {
                                let ret = r_R2(z);
                                if ret == 0 {
                                    (*z).c = (*z).l - m11;
                                    break 'lab11;
                                }
                                if ret < 0 {
                                    return ret;
                                }
                            }
                            let ret = slice_del(z);
                            if ret < 0 {
                                return ret;
                            }
                        }
                    }
                }
                _ => {}
            }
        }
        // lab4:
        (*z).c = (*z).l - m5;
    }
    1
}

// ---------------------------------------------------------------------------
// exported entry points
// ---------------------------------------------------------------------------

#[no_mangle]
pub unsafe extern "C" fn german_ISO_8859_1_stem(z: *mut SN_env) -> c_int {
    {
        let c1 = (*z).c;
        {
            let ret = r_prelude(z);
            if ret < 0 {
                return ret;
            }
        }
        (*z).c = c1;
    }
    {
        let c2 = (*z).c;
        {
            let ret = r_mark_regions(z);
            if ret < 0 {
                return ret;
            }
        }
        (*z).c = c2;
    }
    (*z).lb = (*z).c;
    (*z).c = (*z).l;

    {
        let ret = r_standard_suffix(z);
        if ret < 0 {
            return ret;
        }
    }
    (*z).c = (*z).lb;
    {
        let c3 = (*z).c;
        {
            let ret = r_postlude(z);
            if ret < 0 {
                return ret;
            }
        }
        (*z).c = c3;
    }
    1
}

#[no_mangle]
pub unsafe extern "C" fn german_ISO_8859_1_create_env() -> *mut SN_env {
    SN_create_env(0, 3)
}

#[no_mangle]
pub unsafe extern "C" fn german_ISO_8859_1_close_env(z: *mut SN_env) {
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
        let z = german_ISO_8859_1_create_env();
        assert!(!z.is_null());
        let rc = SN_set_current(z, word.len() as c_int, word.as_ptr());
        assert!(rc >= 0);
        let rc = german_ISO_8859_1_stem(z);
        assert!(rc >= 0, "stem returned {rc}");
        let out = std::slice::from_raw_parts((*z).p, SIZE((*z).p) as usize).to_vec();
        german_ISO_8859_1_close_env(z);
        out
    }

    // A short root with no strippable suffix is returned unchanged.
    #[test]
    fn short_root_unchanged() {
        unsafe {
            assert_eq!(stem(b"tag"), b"tag".to_vec());
        }
    }

    // Idempotence: stemming a stem yields the same stem. High bytes are raw
    // ISO-8859-1 (single-byte), e.g. 0xE4 for 'a' with umlaut, 0xDF for sharp s.
    #[test]
    fn idempotent() {
        unsafe {
            for w in [
                &b"frauen"[..],
                &b"kinder"[..],
                &b"sch\xf6nheit"[..],
                &b"stra\xdfe"[..],
                &b"freundlich"[..],
                &b"b\xe4ckerei"[..],
            ] {
                let once = stem(w);
                let twice = stem(&once);
                assert_eq!(once, twice, "not idempotent for {:?}", w);
                assert!(!once.is_empty());
            }
        }
    }

    // A "heit" suffix family collapses; result must be non-empty and cannot grow
    // past the input length.
    #[test]
    fn suffix_stripped_nonempty() {
        unsafe {
            let r = stem(b"sch\xf6nheit");
            assert!(!r.is_empty());
            assert!(r.len() <= "sch\u{00f6}nheit".len());
        }
    }
}
