//! Dutch Snowball stemmer (ISO-8859-1, single-byte).
//!
//! 1:1 translation of the generated
//! `src/backend/snowball/libstemmer/stem_ISO_8859_1_dutch.c` (Snowball 2.2.0),
//! merged with its header. The runtime helpers come from
//! `crate::snowball::api` and `crate::snowball::utilities`.
//!
//! Because the source charset is ISO-8859-1, every "character" is a single
//! byte: the non-`_U` grouping helpers and plain byte advances are used (no
//! UTF-8 skipping). High bytes (0x80-0xFF) are compared as raw `u8`.

use crate::prelude::*;

use crate::snowball::api::{among, symbol, SN_close_env, SN_create_env, SN_env, SIZE};
use crate::snowball::utilities::{
    eq_s_b, find_among, find_among_b, in_grouping, out_grouping, out_grouping_b, slice_del,
    slice_from_s,
};

// ---------------------------------------------------------------------------
// among string tables
// ---------------------------------------------------------------------------

static S_0_1: [symbol; 1] = [0xE1];
static S_0_2: [symbol; 1] = [0xE4];
static S_0_3: [symbol; 1] = [0xE9];
static S_0_4: [symbol; 1] = [0xEB];
static S_0_5: [symbol; 1] = [0xED];
static S_0_6: [symbol; 1] = [0xEF];
static S_0_7: [symbol; 1] = [0xF3];
static S_0_8: [symbol; 1] = [0xF6];
static S_0_9: [symbol; 1] = [0xFA];
static S_0_10: [symbol; 1] = [0xFC];

static A_0: [among; 11] = [
    among { s_size: 0, s: core::ptr::null(), substring_i: -1, result: 6, function: None },
    among { s_size: 1, s: S_0_1.as_ptr(), substring_i: 0, result: 1, function: None },
    among { s_size: 1, s: S_0_2.as_ptr(), substring_i: 0, result: 1, function: None },
    among { s_size: 1, s: S_0_3.as_ptr(), substring_i: 0, result: 2, function: None },
    among { s_size: 1, s: S_0_4.as_ptr(), substring_i: 0, result: 2, function: None },
    among { s_size: 1, s: S_0_5.as_ptr(), substring_i: 0, result: 3, function: None },
    among { s_size: 1, s: S_0_6.as_ptr(), substring_i: 0, result: 3, function: None },
    among { s_size: 1, s: S_0_7.as_ptr(), substring_i: 0, result: 4, function: None },
    among { s_size: 1, s: S_0_8.as_ptr(), substring_i: 0, result: 4, function: None },
    among { s_size: 1, s: S_0_9.as_ptr(), substring_i: 0, result: 5, function: None },
    among { s_size: 1, s: S_0_10.as_ptr(), substring_i: 0, result: 5, function: None },
];

static S_1_1: [symbol; 1] = [b'I'];
static S_1_2: [symbol; 1] = [b'Y'];

static A_1: [among; 3] = [
    among { s_size: 0, s: core::ptr::null(), substring_i: -1, result: 3, function: None },
    among { s_size: 1, s: S_1_1.as_ptr(), substring_i: 0, result: 2, function: None },
    among { s_size: 1, s: S_1_2.as_ptr(), substring_i: 0, result: 1, function: None },
];

static S_2_0: [symbol; 2] = [b'd', b'd'];
static S_2_1: [symbol; 2] = [b'k', b'k'];
static S_2_2: [symbol; 2] = [b't', b't'];

static A_2: [among; 3] = [
    among { s_size: 2, s: S_2_0.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_2_1.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_2_2.as_ptr(), substring_i: -1, result: -1, function: None },
];

static S_3_0: [symbol; 3] = [b'e', b'n', b'e'];
static S_3_1: [symbol; 2] = [b's', b'e'];
static S_3_2: [symbol; 2] = [b'e', b'n'];
static S_3_3: [symbol; 5] = [b'h', b'e', b'd', b'e', b'n'];
static S_3_4: [symbol; 1] = [b's'];

static A_3: [among; 5] = [
    among { s_size: 3, s: S_3_0.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 2, s: S_3_1.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 2, s: S_3_2.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 5, s: S_3_3.as_ptr(), substring_i: 2, result: 1, function: None },
    among { s_size: 1, s: S_3_4.as_ptr(), substring_i: -1, result: 3, function: None },
];

static S_4_0: [symbol; 3] = [b'e', b'n', b'd'];
static S_4_1: [symbol; 2] = [b'i', b'g'];
static S_4_2: [symbol; 3] = [b'i', b'n', b'g'];
static S_4_3: [symbol; 4] = [b'l', b'i', b'j', b'k'];
static S_4_4: [symbol; 4] = [b'b', b'a', b'a', b'r'];
static S_4_5: [symbol; 3] = [b'b', b'a', b'r'];

static A_4: [among; 6] = [
    among { s_size: 3, s: S_4_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_4_1.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 3, s: S_4_2.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_4_3.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 4, s: S_4_4.as_ptr(), substring_i: -1, result: 4, function: None },
    among { s_size: 3, s: S_4_5.as_ptr(), substring_i: -1, result: 5, function: None },
];

static S_5_0: [symbol; 2] = [b'a', b'a'];
static S_5_1: [symbol; 2] = [b'e', b'e'];
static S_5_2: [symbol; 2] = [b'o', b'o'];
static S_5_3: [symbol; 2] = [b'u', b'u'];

static A_5: [among; 4] = [
    among { s_size: 2, s: S_5_0.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_5_1.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_5_2.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_5_3.as_ptr(), substring_i: -1, result: -1, function: None },
];

// ---------------------------------------------------------------------------
// grouping bit tables
// ---------------------------------------------------------------------------

static G_V: [c_uchar; 17] = [17, 65, 16, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 128];

static G_V_I: [c_uchar; 20] = [
    1, 0, 0, 17, 65, 16, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 128,
];

static G_V_J: [c_uchar; 17] = [17, 67, 16, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 128];

// ---------------------------------------------------------------------------
// literal strings used by slice_from_s
// ---------------------------------------------------------------------------

static S_0: [symbol; 1] = [b'a'];
static S_1: [symbol; 1] = [b'e'];
static S_2: [symbol; 1] = [b'i'];
static S_3: [symbol; 1] = [b'o'];
static S_4: [symbol; 1] = [b'u'];
static S_5: [symbol; 1] = [b'Y'];
static S_6: [symbol; 1] = [b'I'];
static S_7: [symbol; 1] = [b'Y'];
static S_8: [symbol; 1] = [b'y'];
static S_9: [symbol; 1] = [b'i'];
static S_10: [symbol; 3] = [b'g', b'e', b'm'];
static S_11: [symbol; 4] = [b'h', b'e', b'i', b'd'];
static S_12: [symbol; 4] = [b'h', b'e', b'i', b'd'];
static S_13: [symbol; 2] = [b'e', b'n'];
static S_14: [symbol; 2] = [b'i', b'g'];

// ---------------------------------------------------------------------------
// rules
// ---------------------------------------------------------------------------

unsafe fn r_prelude(z: *mut SN_env) -> c_int {
    let mut among_var;
    {
        let c_test1 = (*z).c;
        'loop1: loop {
            let c2 = (*z).c;
            'lab0: {
                (*z).bra = (*z).c;
                if (*z).c >= (*z).l
                    || *(*z).p.offset((*z).c as isize) as c_int >> 5 != 7
                    || (340306450 >> (*(*z).p.offset((*z).c as isize) as c_int & 0x1f)) & 1 == 0
                {
                    among_var = 6;
                } else {
                    among_var = find_among(z, A_0.as_ptr(), 11);
                }
                (*z).ket = (*z).c;
                match among_var {
                    1 => {
                        let ret = slice_from_s(z, 1, S_0.as_ptr());
                        if ret < 0 {
                            return ret;
                        }
                    }
                    2 => {
                        let ret = slice_from_s(z, 1, S_1.as_ptr());
                        if ret < 0 {
                            return ret;
                        }
                    }
                    3 => {
                        let ret = slice_from_s(z, 1, S_2.as_ptr());
                        if ret < 0 {
                            return ret;
                        }
                    }
                    4 => {
                        let ret = slice_from_s(z, 1, S_3.as_ptr());
                        if ret < 0 {
                            return ret;
                        }
                    }
                    5 => {
                        let ret = slice_from_s(z, 1, S_4.as_ptr());
                        if ret < 0 {
                            return ret;
                        }
                    }
                    6 => {
                        if (*z).c >= (*z).l {
                            break 'lab0;
                        }
                        (*z).c += 1;
                    }
                    _ => {}
                }
                continue 'loop1;
            }
            // lab0:
            (*z).c = c2;
            break;
        }
        (*z).c = c_test1;
    }
    {
        let c3 = (*z).c;
        'lab1: {
            (*z).bra = (*z).c;
            if (*z).c == (*z).l || *(*z).p.offset((*z).c as isize) != b'y' {
                (*z).c = c3;
                break 'lab1;
            }
            (*z).c += 1;
            (*z).ket = (*z).c;
            let ret = slice_from_s(z, 1, S_5.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
    }
    'loop2: loop {
        let c4 = (*z).c;
        'lab2: {
            'loop3: loop {
                let c5 = (*z).c;
                'lab3: {
                    'lab4: {
                        if in_grouping(z, G_V.as_ptr(), 97, 232, 0) != 0 {
                            break 'lab3;
                        }
                        (*z).bra = (*z).c;
                        {
                            let c6 = (*z).c;
                            'lab5: {
                                if (*z).c == (*z).l || *(*z).p.offset((*z).c as isize) != b'i' {
                                    break 'lab5;
                                }
                                (*z).c += 1;
                                (*z).ket = (*z).c;
                                if in_grouping(z, G_V.as_ptr(), 97, 232, 0) != 0 {
                                    break 'lab5;
                                }
                                let ret = slice_from_s(z, 1, S_6.as_ptr());
                                if ret < 0 {
                                    return ret;
                                }
                                break 'lab4;
                            }
                            // lab5:
                            (*z).c = c6;
                            if (*z).c == (*z).l || *(*z).p.offset((*z).c as isize) != b'y' {
                                break 'lab3;
                            }
                            (*z).c += 1;
                            (*z).ket = (*z).c;
                            let ret = slice_from_s(z, 1, S_7.as_ptr());
                            if ret < 0 {
                                return ret;
                            }
                        }
                    }
                    // lab4:
                    (*z).c = c5;
                    break 'loop3;
                }
                // lab3:
                (*z).c = c5;
                if (*z).c >= (*z).l {
                    break 'lab2;
                }
                (*z).c += 1;
            }
            continue 'loop2;
        }
        // lab2:
        (*z).c = c4;
        break;
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
        let ret = out_grouping(z, G_V.as_ptr(), 97, 232, 1);
        if ret < 0 {
            return 0;
        }
        (*z).c += ret;
    }

    {
        let ret = in_grouping(z, G_V.as_ptr(), 97, 232, 1);
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
        let ret = out_grouping(z, G_V.as_ptr(), 97, 232, 1);
        if ret < 0 {
            return 0;
        }
        (*z).c += ret;
    }

    {
        let ret = in_grouping(z, G_V.as_ptr(), 97, 232, 1);
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
    'loop1: loop {
        let c1 = (*z).c;
        'lab0: {
            (*z).bra = (*z).c;
            if (*z).c >= (*z).l
                || (*(*z).p.offset((*z).c as isize) != 73 && *(*z).p.offset((*z).c as isize) != 89)
            {
                among_var = 3;
            } else {
                among_var = find_among(z, A_1.as_ptr(), 3);
            }
            (*z).ket = (*z).c;
            match among_var {
                1 => {
                    let ret = slice_from_s(z, 1, S_8.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
                2 => {
                    let ret = slice_from_s(z, 1, S_9.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
                3 => {
                    if (*z).c >= (*z).l {
                        break 'lab0;
                    }
                    (*z).c += 1;
                }
                _ => {}
            }
            continue 'loop1;
        }
        // lab0:
        (*z).c = c1;
        break;
    }
    1
}

unsafe fn r_R1(z: *mut SN_env) -> c_int {
    (*(*z).I.offset(2) <= (*z).c) as c_int
}

unsafe fn r_R2(z: *mut SN_env) -> c_int {
    (*(*z).I.offset(1) <= (*z).c) as c_int
}

unsafe fn r_undouble(z: *mut SN_env) -> c_int {
    {
        let m_test1 = (*z).l - (*z).c;
        if (*z).c - 1 <= (*z).lb
            || *(*z).p.offset(((*z).c - 1) as isize) as c_int >> 5 != 3
            || (1050640 >> (*(*z).p.offset(((*z).c - 1) as isize) as c_int & 0x1f)) & 1 == 0
        {
            return 0;
        }
        if find_among_b(z, A_2.as_ptr(), 3) == 0 {
            return 0;
        }
        (*z).c = (*z).l - m_test1;
    }
    (*z).ket = (*z).c;
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

unsafe fn r_e_ending(z: *mut SN_env) -> c_int {
    *(*z).I.offset(3) = 0;
    (*z).ket = (*z).c;
    if (*z).c <= (*z).lb || *(*z).p.offset(((*z).c - 1) as isize) != b'e' {
        return 0;
    }
    (*z).c -= 1;
    (*z).bra = (*z).c;
    {
        let ret = r_R1(z);
        if ret <= 0 {
            return ret;
        }
    }
    {
        let m_test1 = (*z).l - (*z).c;
        if out_grouping_b(z, G_V.as_ptr(), 97, 232, 0) != 0 {
            return 0;
        }
        (*z).c = (*z).l - m_test1;
    }
    {
        let ret = slice_del(z);
        if ret < 0 {
            return ret;
        }
    }
    *(*z).I.offset(3) = 1;
    {
        let ret = r_undouble(z);
        if ret <= 0 {
            return ret;
        }
    }
    1
}

unsafe fn r_en_ending(z: *mut SN_env) -> c_int {
    {
        let ret = r_R1(z);
        if ret <= 0 {
            return ret;
        }
    }
    {
        let m1 = (*z).l - (*z).c;
        let _ = m1;
        if out_grouping_b(z, G_V.as_ptr(), 97, 232, 0) != 0 {
            return 0;
        }
        (*z).c = (*z).l - m1;
        {
            let m2 = (*z).l - (*z).c;
            let _ = m2;
            'lab0: {
                if eq_s_b(z, 3, S_10.as_ptr()) == 0 {
                    break 'lab0;
                }
                return 0;
            }
            // lab0:
            (*z).c = (*z).l - m2;
        }
    }
    {
        let ret = slice_del(z);
        if ret < 0 {
            return ret;
        }
    }
    {
        let ret = r_undouble(z);
        if ret <= 0 {
            return ret;
        }
    }
    1
}

unsafe fn r_standard_suffix(z: *mut SN_env) -> c_int {
    let mut among_var;
    {
        let m1 = (*z).l - (*z).c;
        let _ = m1;
        'lab0: {
            (*z).ket = (*z).c;
            if (*z).c <= (*z).lb
                || *(*z).p.offset(((*z).c - 1) as isize) as c_int >> 5 != 3
                || (540704 >> (*(*z).p.offset(((*z).c - 1) as isize) as c_int & 0x1f)) & 1 == 0
            {
                break 'lab0;
            }
            among_var = find_among_b(z, A_3.as_ptr(), 5);
            if among_var == 0 {
                break 'lab0;
            }
            (*z).bra = (*z).c;
            match among_var {
                1 => {
                    {
                        let ret = r_R1(z);
                        if ret == 0 {
                            break 'lab0;
                        }
                        if ret < 0 {
                            return ret;
                        }
                    }
                    {
                        let ret = slice_from_s(z, 4, S_11.as_ptr());
                        if ret < 0 {
                            return ret;
                        }
                    }
                }
                2 => {
                    let ret = r_en_ending(z);
                    if ret == 0 {
                        break 'lab0;
                    }
                    if ret < 0 {
                        return ret;
                    }
                }
                3 => {
                    {
                        let ret = r_R1(z);
                        if ret == 0 {
                            break 'lab0;
                        }
                        if ret < 0 {
                            return ret;
                        }
                    }
                    if out_grouping_b(z, G_V_J.as_ptr(), 97, 232, 0) != 0 {
                        break 'lab0;
                    }
                    {
                        let ret = slice_del(z);
                        if ret < 0 {
                            return ret;
                        }
                    }
                }
                _ => {}
            }
        }
        // lab0:
        (*z).c = (*z).l - m1;
    }
    {
        let m2 = (*z).l - (*z).c;
        let _ = m2;
        {
            let ret = r_e_ending(z);
            if ret < 0 {
                return ret;
            }
        }
        (*z).c = (*z).l - m2;
    }
    {
        let m3 = (*z).l - (*z).c;
        let _ = m3;
        'lab1: {
            (*z).ket = (*z).c;
            if eq_s_b(z, 4, S_12.as_ptr()) == 0 {
                break 'lab1;
            }
            (*z).bra = (*z).c;
            {
                let ret = r_R2(z);
                if ret == 0 {
                    break 'lab1;
                }
                if ret < 0 {
                    return ret;
                }
            }
            {
                let m4 = (*z).l - (*z).c;
                let _ = m4;
                'lab2: {
                    if (*z).c <= (*z).lb || *(*z).p.offset(((*z).c - 1) as isize) != b'c' {
                        break 'lab2;
                    }
                    (*z).c -= 1;
                    break 'lab1;
                }
                // lab2:
                (*z).c = (*z).l - m4;
            }
            {
                let ret = slice_del(z);
                if ret < 0 {
                    return ret;
                }
            }
            (*z).ket = (*z).c;
            if eq_s_b(z, 2, S_13.as_ptr()) == 0 {
                break 'lab1;
            }
            (*z).bra = (*z).c;
            {
                let ret = r_en_ending(z);
                if ret == 0 {
                    break 'lab1;
                }
                if ret < 0 {
                    return ret;
                }
            }
        }
        // lab1:
        (*z).c = (*z).l - m3;
    }
    {
        let m5 = (*z).l - (*z).c;
        let _ = m5;
        'lab3: {
            (*z).ket = (*z).c;
            if (*z).c - 1 <= (*z).lb
                || *(*z).p.offset(((*z).c - 1) as isize) as c_int >> 5 != 3
                || (264336 >> (*(*z).p.offset(((*z).c - 1) as isize) as c_int & 0x1f)) & 1 == 0
            {
                break 'lab3;
            }
            among_var = find_among_b(z, A_4.as_ptr(), 6);
            if among_var == 0 {
                break 'lab3;
            }
            (*z).bra = (*z).c;
            match among_var {
                1 => {
                    {
                        let ret = r_R2(z);
                        if ret == 0 {
                            break 'lab3;
                        }
                        if ret < 0 {
                            return ret;
                        }
                    }
                    {
                        let ret = slice_del(z);
                        if ret < 0 {
                            return ret;
                        }
                    }
                    'lab4: {
                        let m6 = (*z).l - (*z).c;
                        let _ = m6;
                        'lab5: {
                            (*z).ket = (*z).c;
                            if eq_s_b(z, 2, S_14.as_ptr()) == 0 {
                                break 'lab5;
                            }
                            (*z).bra = (*z).c;
                            {
                                let ret = r_R2(z);
                                if ret == 0 {
                                    break 'lab5;
                                }
                                if ret < 0 {
                                    return ret;
                                }
                            }
                            {
                                let m7 = (*z).l - (*z).c;
                                let _ = m7;
                                'lab6: {
                                    if (*z).c <= (*z).lb
                                        || *(*z).p.offset(((*z).c - 1) as isize) != b'e'
                                    {
                                        break 'lab6;
                                    }
                                    (*z).c -= 1;
                                    break 'lab5;
                                }
                                // lab6:
                                (*z).c = (*z).l - m7;
                            }
                            {
                                let ret = slice_del(z);
                                if ret < 0 {
                                    return ret;
                                }
                            }
                            break 'lab4;
                        }
                        // lab5:
                        (*z).c = (*z).l - m6;
                        {
                            let ret = r_undouble(z);
                            if ret == 0 {
                                break 'lab3;
                            }
                            if ret < 0 {
                                return ret;
                            }
                        }
                    }
                    // lab4:
                }
                2 => {
                    {
                        let ret = r_R2(z);
                        if ret == 0 {
                            break 'lab3;
                        }
                        if ret < 0 {
                            return ret;
                        }
                    }
                    {
                        let m8 = (*z).l - (*z).c;
                        let _ = m8;
                        'lab7: {
                            if (*z).c <= (*z).lb || *(*z).p.offset(((*z).c - 1) as isize) != b'e' {
                                break 'lab7;
                            }
                            (*z).c -= 1;
                            break 'lab3;
                        }
                        // lab7:
                        (*z).c = (*z).l - m8;
                    }
                    {
                        let ret = slice_del(z);
                        if ret < 0 {
                            return ret;
                        }
                    }
                }
                3 => {
                    {
                        let ret = r_R2(z);
                        if ret == 0 {
                            break 'lab3;
                        }
                        if ret < 0 {
                            return ret;
                        }
                    }
                    {
                        let ret = slice_del(z);
                        if ret < 0 {
                            return ret;
                        }
                    }
                    {
                        let ret = r_e_ending(z);
                        if ret == 0 {
                            break 'lab3;
                        }
                        if ret < 0 {
                            return ret;
                        }
                    }
                }
                4 => {
                    {
                        let ret = r_R2(z);
                        if ret == 0 {
                            break 'lab3;
                        }
                        if ret < 0 {
                            return ret;
                        }
                    }
                    {
                        let ret = slice_del(z);
                        if ret < 0 {
                            return ret;
                        }
                    }
                }
                5 => {
                    {
                        let ret = r_R2(z);
                        if ret == 0 {
                            break 'lab3;
                        }
                        if ret < 0 {
                            return ret;
                        }
                    }
                    if *(*z).I.offset(3) == 0 {
                        break 'lab3;
                    }
                    {
                        let ret = slice_del(z);
                        if ret < 0 {
                            return ret;
                        }
                    }
                }
                _ => {}
            }
        }
        // lab3:
        (*z).c = (*z).l - m5;
    }
    {
        let m9 = (*z).l - (*z).c;
        let _ = m9;
        'lab8: {
            if out_grouping_b(z, G_V_I.as_ptr(), 73, 232, 0) != 0 {
                break 'lab8;
            }
            {
                let m_test10 = (*z).l - (*z).c;
                if (*z).c - 1 <= (*z).lb
                    || *(*z).p.offset(((*z).c - 1) as isize) as c_int >> 5 != 3
                    || (2129954 >> (*(*z).p.offset(((*z).c - 1) as isize) as c_int & 0x1f)) & 1 == 0
                {
                    break 'lab8;
                }
                if find_among_b(z, A_5.as_ptr(), 4) == 0 {
                    break 'lab8;
                }
                if out_grouping_b(z, G_V.as_ptr(), 97, 232, 0) != 0 {
                    break 'lab8;
                }
                (*z).c = (*z).l - m_test10;
            }
            (*z).ket = (*z).c;
            if (*z).c <= (*z).lb {
                break 'lab8;
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
        // lab8:
        (*z).c = (*z).l - m9;
    }
    1
}

// ---------------------------------------------------------------------------
// exported entry points
// ---------------------------------------------------------------------------

#[no_mangle]
pub unsafe extern "C" fn dutch_ISO_8859_1_stem(z: *mut SN_env) -> c_int {
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
pub unsafe extern "C" fn dutch_ISO_8859_1_create_env() -> *mut SN_env {
    SN_create_env(0, 4)
}

#[no_mangle]
pub unsafe extern "C" fn dutch_ISO_8859_1_close_env(z: *mut SN_env) {
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
        let z = dutch_ISO_8859_1_create_env();
        assert!(!z.is_null());
        let rc = SN_set_current(z, word.len() as c_int, word.as_ptr());
        assert!(rc >= 0);
        let rc = dutch_ISO_8859_1_stem(z);
        assert!(rc >= 0, "stem returned {rc}");
        let out = std::slice::from_raw_parts((*z).p, SIZE((*z).p) as usize).to_vec();
        dutch_ISO_8859_1_close_env(z);
        out
    }

    // A short root with no strippable suffix is returned unchanged.
    #[test]
    fn short_root_unchanged() {
        unsafe {
            assert_eq!(stem(b"huis"), b"huis".to_vec());
        }
    }

    // Idempotence: stemming a stem yields the same stem. Includes an ISO-8859-1
    // high byte (0xEB = 'e-diaeresis') to exercise the single-byte path.
    #[test]
    fn idempotent() {
        unsafe {
            for w in [
                &b"lichamelijk"[..],
                &b"wandelende"[..],
                &b"verschillende"[..],
                &b"be\xebindigen"[..],
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
            let r = stem(b"lichamelijkheden");
            assert!(!r.is_empty());
            assert!(r.len() <= "lichamelijkheden".len());
        }
    }
}
