//! Porter Snowball stemmer (UTF-8).
//!
//! 1:1 translation of the generated
//! `src/backend/snowball/libstemmer/stem_UTF_8_porter.c` (Snowball 2.2.0),
//! merged with its header `stem_UTF_8_porter.h`. The runtime helpers come from
//! `crate::snowball::api` and `crate::snowball::utilities`.

use crate::prelude::*;

use crate::snowball::api::{among, symbol, SN_close_env, SN_create_env, SN_env, SIZE};
use crate::snowball::utilities::{
    find_among_b, in_grouping_b_U, in_grouping_U, insert_s, out_grouping_b_U, out_grouping_U,
    skip_b_utf8, skip_utf8, slice_del, slice_from_s,
};

// ---------------------------------------------------------------------------
// among string tables
// ---------------------------------------------------------------------------

static S_0_0: [symbol; 1] = [b's'];
static S_0_1: [symbol; 3] = [b'i', b'e', b's'];
static S_0_2: [symbol; 4] = [b's', b's', b'e', b's'];
static S_0_3: [symbol; 2] = [b's', b's'];

static A_0: [among; 4] = [
    among { s_size: 1, s: S_0_0.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 3, s: S_0_1.as_ptr(), substring_i: 0, result: 2, function: None },
    among { s_size: 4, s: S_0_2.as_ptr(), substring_i: 0, result: 1, function: None },
    among { s_size: 2, s: S_0_3.as_ptr(), substring_i: 0, result: -1, function: None },
];

static S_1_1: [symbol; 2] = [b'b', b'b'];
static S_1_2: [symbol; 2] = [b'd', b'd'];
static S_1_3: [symbol; 2] = [b'f', b'f'];
static S_1_4: [symbol; 2] = [b'g', b'g'];
static S_1_5: [symbol; 2] = [b'b', b'l'];
static S_1_6: [symbol; 2] = [b'm', b'm'];
static S_1_7: [symbol; 2] = [b'n', b'n'];
static S_1_8: [symbol; 2] = [b'p', b'p'];
static S_1_9: [symbol; 2] = [b'r', b'r'];
static S_1_10: [symbol; 2] = [b'a', b't'];
static S_1_11: [symbol; 2] = [b't', b't'];
static S_1_12: [symbol; 2] = [b'i', b'z'];

static A_1: [among; 13] = [
    among { s_size: 0, s: std::ptr::null(), substring_i: -1, result: 3, function: None },
    among { s_size: 2, s: S_1_1.as_ptr(), substring_i: 0, result: 2, function: None },
    among { s_size: 2, s: S_1_2.as_ptr(), substring_i: 0, result: 2, function: None },
    among { s_size: 2, s: S_1_3.as_ptr(), substring_i: 0, result: 2, function: None },
    among { s_size: 2, s: S_1_4.as_ptr(), substring_i: 0, result: 2, function: None },
    among { s_size: 2, s: S_1_5.as_ptr(), substring_i: 0, result: 1, function: None },
    among { s_size: 2, s: S_1_6.as_ptr(), substring_i: 0, result: 2, function: None },
    among { s_size: 2, s: S_1_7.as_ptr(), substring_i: 0, result: 2, function: None },
    among { s_size: 2, s: S_1_8.as_ptr(), substring_i: 0, result: 2, function: None },
    among { s_size: 2, s: S_1_9.as_ptr(), substring_i: 0, result: 2, function: None },
    among { s_size: 2, s: S_1_10.as_ptr(), substring_i: 0, result: 1, function: None },
    among { s_size: 2, s: S_1_11.as_ptr(), substring_i: 0, result: 2, function: None },
    among { s_size: 2, s: S_1_12.as_ptr(), substring_i: 0, result: 1, function: None },
];

static S_2_0: [symbol; 2] = [b'e', b'd'];
static S_2_1: [symbol; 3] = [b'e', b'e', b'd'];
static S_2_2: [symbol; 3] = [b'i', b'n', b'g'];

static A_2: [among; 3] = [
    among { s_size: 2, s: S_2_0.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 3, s: S_2_1.as_ptr(), substring_i: 0, result: 1, function: None },
    among { s_size: 3, s: S_2_2.as_ptr(), substring_i: -1, result: 2, function: None },
];

static S_3_0: [symbol; 4] = [b'a', b'n', b'c', b'i'];
static S_3_1: [symbol; 4] = [b'e', b'n', b'c', b'i'];
static S_3_2: [symbol; 4] = [b'a', b'b', b'l', b'i'];
static S_3_3: [symbol; 3] = [b'e', b'l', b'i'];
static S_3_4: [symbol; 4] = [b'a', b'l', b'l', b'i'];
static S_3_5: [symbol; 5] = [b'o', b'u', b's', b'l', b'i'];
static S_3_6: [symbol; 5] = [b'e', b'n', b't', b'l', b'i'];
static S_3_7: [symbol; 5] = [b'a', b'l', b'i', b't', b'i'];
static S_3_8: [symbol; 6] = [b'b', b'i', b'l', b'i', b't', b'i'];
static S_3_9: [symbol; 5] = [b'i', b'v', b'i', b't', b'i'];
static S_3_10: [symbol; 6] = [b't', b'i', b'o', b'n', b'a', b'l'];
static S_3_11: [symbol; 7] = [b'a', b't', b'i', b'o', b'n', b'a', b'l'];
static S_3_12: [symbol; 5] = [b'a', b'l', b'i', b's', b'm'];
static S_3_13: [symbol; 5] = [b'a', b't', b'i', b'o', b'n'];
static S_3_14: [symbol; 7] = [b'i', b'z', b'a', b't', b'i', b'o', b'n'];
static S_3_15: [symbol; 4] = [b'i', b'z', b'e', b'r'];
static S_3_16: [symbol; 4] = [b'a', b't', b'o', b'r'];
static S_3_17: [symbol; 7] = [b'i', b'v', b'e', b'n', b'e', b's', b's'];
static S_3_18: [symbol; 7] = [b'f', b'u', b'l', b'n', b'e', b's', b's'];
static S_3_19: [symbol; 7] = [b'o', b'u', b's', b'n', b'e', b's', b's'];

static A_3: [among; 20] = [
    among { s_size: 4, s: S_3_0.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 4, s: S_3_1.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 4, s: S_3_2.as_ptr(), substring_i: -1, result: 4, function: None },
    among { s_size: 3, s: S_3_3.as_ptr(), substring_i: -1, result: 6, function: None },
    among { s_size: 4, s: S_3_4.as_ptr(), substring_i: -1, result: 9, function: None },
    among { s_size: 5, s: S_3_5.as_ptr(), substring_i: -1, result: 11, function: None },
    among { s_size: 5, s: S_3_6.as_ptr(), substring_i: -1, result: 5, function: None },
    among { s_size: 5, s: S_3_7.as_ptr(), substring_i: -1, result: 9, function: None },
    among { s_size: 6, s: S_3_8.as_ptr(), substring_i: -1, result: 13, function: None },
    among { s_size: 5, s: S_3_9.as_ptr(), substring_i: -1, result: 12, function: None },
    among { s_size: 6, s: S_3_10.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 7, s: S_3_11.as_ptr(), substring_i: 10, result: 8, function: None },
    among { s_size: 5, s: S_3_12.as_ptr(), substring_i: -1, result: 9, function: None },
    among { s_size: 5, s: S_3_13.as_ptr(), substring_i: -1, result: 8, function: None },
    among { s_size: 7, s: S_3_14.as_ptr(), substring_i: 13, result: 7, function: None },
    among { s_size: 4, s: S_3_15.as_ptr(), substring_i: -1, result: 7, function: None },
    among { s_size: 4, s: S_3_16.as_ptr(), substring_i: -1, result: 8, function: None },
    among { s_size: 7, s: S_3_17.as_ptr(), substring_i: -1, result: 12, function: None },
    among { s_size: 7, s: S_3_18.as_ptr(), substring_i: -1, result: 10, function: None },
    among { s_size: 7, s: S_3_19.as_ptr(), substring_i: -1, result: 11, function: None },
];

static S_4_0: [symbol; 5] = [b'i', b'c', b'a', b't', b'e'];
static S_4_1: [symbol; 5] = [b'a', b't', b'i', b'v', b'e'];
static S_4_2: [symbol; 5] = [b'a', b'l', b'i', b'z', b'e'];
static S_4_3: [symbol; 5] = [b'i', b'c', b'i', b't', b'i'];
static S_4_4: [symbol; 4] = [b'i', b'c', b'a', b'l'];
static S_4_5: [symbol; 3] = [b'f', b'u', b'l'];
static S_4_6: [symbol; 4] = [b'n', b'e', b's', b's'];

static A_4: [among; 7] = [
    among { s_size: 5, s: S_4_0.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 5, s: S_4_1.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 5, s: S_4_2.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_4_3.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 4, s: S_4_4.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 3, s: S_4_5.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 4, s: S_4_6.as_ptr(), substring_i: -1, result: 3, function: None },
];

static S_5_0: [symbol; 2] = [b'i', b'c'];
static S_5_1: [symbol; 4] = [b'a', b'n', b'c', b'e'];
static S_5_2: [symbol; 4] = [b'e', b'n', b'c', b'e'];
static S_5_3: [symbol; 4] = [b'a', b'b', b'l', b'e'];
static S_5_4: [symbol; 4] = [b'i', b'b', b'l', b'e'];
static S_5_5: [symbol; 3] = [b'a', b't', b'e'];
static S_5_6: [symbol; 3] = [b'i', b'v', b'e'];
static S_5_7: [symbol; 3] = [b'i', b'z', b'e'];
static S_5_8: [symbol; 3] = [b'i', b't', b'i'];
static S_5_9: [symbol; 2] = [b'a', b'l'];
static S_5_10: [symbol; 3] = [b'i', b's', b'm'];
static S_5_11: [symbol; 3] = [b'i', b'o', b'n'];
static S_5_12: [symbol; 2] = [b'e', b'r'];
static S_5_13: [symbol; 3] = [b'o', b'u', b's'];
static S_5_14: [symbol; 3] = [b'a', b'n', b't'];
static S_5_15: [symbol; 3] = [b'e', b'n', b't'];
static S_5_16: [symbol; 4] = [b'm', b'e', b'n', b't'];
static S_5_17: [symbol; 5] = [b'e', b'm', b'e', b'n', b't'];
static S_5_18: [symbol; 2] = [b'o', b'u'];

static A_5: [among; 19] = [
    among { s_size: 2, s: S_5_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_5_1.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_5_2.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_5_3.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_5_4.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_5_5.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_5_6.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_5_7.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_5_8.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_5_9.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_5_10.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_5_11.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 2, s: S_5_12.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_5_13.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_5_14.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_5_15.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_5_16.as_ptr(), substring_i: 15, result: 1, function: None },
    among { s_size: 5, s: S_5_17.as_ptr(), substring_i: 16, result: 1, function: None },
    among { s_size: 2, s: S_5_18.as_ptr(), substring_i: -1, result: 1, function: None },
];

// ---------------------------------------------------------------------------
// grouping bit tables
// ---------------------------------------------------------------------------

static G_V: [c_uchar; 4] = [17, 65, 16, 1];

static G_V_WXY: [c_uchar; 5] = [1, 17, 65, 208, 1];

// ---------------------------------------------------------------------------
// literal strings used by slice_from_s / insert_s
// ---------------------------------------------------------------------------

static S_0: [symbol; 2] = [b's', b's'];
static S_1: [symbol; 1] = [b'i'];
static S_2: [symbol; 2] = [b'e', b'e'];
static S_3: [symbol; 1] = [b'e'];
static S_4: [symbol; 1] = [b'e'];
static S_5: [symbol; 1] = [b'i'];
static S_6: [symbol; 4] = [b't', b'i', b'o', b'n'];
static S_7: [symbol; 4] = [b'e', b'n', b'c', b'e'];
static S_8: [symbol; 4] = [b'a', b'n', b'c', b'e'];
static S_9: [symbol; 4] = [b'a', b'b', b'l', b'e'];
static S_10: [symbol; 3] = [b'e', b'n', b't'];
static S_11: [symbol; 1] = [b'e'];
static S_12: [symbol; 3] = [b'i', b'z', b'e'];
static S_13: [symbol; 3] = [b'a', b't', b'e'];
static S_14: [symbol; 2] = [b'a', b'l'];
static S_15: [symbol; 3] = [b'f', b'u', b'l'];
static S_16: [symbol; 3] = [b'o', b'u', b's'];
static S_17: [symbol; 3] = [b'i', b'v', b'e'];
static S_18: [symbol; 3] = [b'b', b'l', b'e'];
static S_19: [symbol; 2] = [b'a', b'l'];
static S_20: [symbol; 2] = [b'i', b'c'];
static S_21: [symbol; 1] = [b'Y'];
static S_22: [symbol; 1] = [b'Y'];
static S_23: [symbol; 1] = [b'y'];

// ---------------------------------------------------------------------------
// rules
// ---------------------------------------------------------------------------

unsafe extern "C" fn r_shortv(z: *mut SN_env) -> c_int {
    if out_grouping_b_U(z, G_V_WXY.as_ptr(), 89, 121, 0) != 0 {
        return 0;
    }
    if in_grouping_b_U(z, G_V.as_ptr(), 97, 121, 0) != 0 {
        return 0;
    }
    if out_grouping_b_U(z, G_V.as_ptr(), 97, 121, 0) != 0 {
        return 0;
    }
    1
}

unsafe extern "C" fn r_R1(z: *mut SN_env) -> c_int {
    (*(*z).I.offset(1) <= (*z).c) as c_int
}

unsafe extern "C" fn r_R2(z: *mut SN_env) -> c_int {
    (*(*z).I.offset(0) <= (*z).c) as c_int
}

unsafe extern "C" fn r_Step_1a(z: *mut SN_env) -> c_int {
    let among_var;
    (*z).ket = (*z).c;
    if (*z).c <= (*z).lb || *(*z).p.offset(((*z).c - 1) as isize) != 115 {
        return 0;
    }
    among_var = find_among_b(z, A_0.as_ptr(), 4);
    if among_var == 0 {
        return 0;
    }
    (*z).bra = (*z).c;
    match among_var {
        1 => {
            let ret = slice_from_s(z, 2, S_0.as_ptr());
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
            let ret = slice_del(z);
            if ret < 0 {
                return ret;
            }
        }
        _ => {}
    }
    1
}

unsafe extern "C" fn r_Step_1b(z: *mut SN_env) -> c_int {
    let mut among_var;
    (*z).ket = (*z).c;
    if (*z).c - 1 <= (*z).lb
        || (*(*z).p.offset(((*z).c - 1) as isize) != 100
            && *(*z).p.offset(((*z).c - 1) as isize) != 103)
    {
        return 0;
    }
    among_var = find_among_b(z, A_2.as_ptr(), 3);
    if among_var == 0 {
        return 0;
    }
    (*z).bra = (*z).c;
    match among_var {
        1 => {
            {
                let ret = r_R1(z);
                if ret <= 0 {
                    return ret;
                }
            }
            {
                let ret = slice_from_s(z, 2, S_2.as_ptr());
                if ret < 0 {
                    return ret;
                }
            }
        }
        2 => {
            {
                let m_test1 = (*z).l - (*z).c;

                {
                    let ret = out_grouping_b_U(z, G_V.as_ptr(), 97, 121, 1);
                    if ret < 0 {
                        return 0;
                    }
                    (*z).c -= ret;
                }
                (*z).c = (*z).l - m_test1;
            }
            {
                let ret = slice_del(z);
                if ret < 0 {
                    return ret;
                }
            }
            {
                let m_test2 = (*z).l - (*z).c;
                if (*z).c - 1 <= (*z).lb
                    || *(*z).p.offset(((*z).c - 1) as isize) as c_int >> 5 != 3
                    || (68514004 >> (*(*z).p.offset(((*z).c - 1) as isize) as c_int & 0x1f)) & 1 == 0
                {
                    among_var = 3;
                } else {
                    among_var = find_among_b(z, A_1.as_ptr(), 13);
                }
                (*z).c = (*z).l - m_test2;
            }
            match among_var {
                1 => {
                    let ret;
                    {
                        let saved_c = (*z).c;
                        ret = insert_s(z, (*z).c, (*z).c, 1, S_3.as_ptr());
                        (*z).c = saved_c;
                    }
                    if ret < 0 {
                        return ret;
                    }
                }
                2 => {
                    (*z).ket = (*z).c;
                    {
                        let ret = skip_b_utf8((*z).p, (*z).c, (*z).lb, 1);
                        if ret < 0 {
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
                3 => {
                    if (*z).c != *(*z).I.offset(1) {
                        return 0;
                    }
                    {
                        let m_test3 = (*z).l - (*z).c;
                        {
                            let ret = r_shortv(z);
                            if ret <= 0 {
                                return ret;
                            }
                        }
                        (*z).c = (*z).l - m_test3;
                    }
                    {
                        let ret;
                        {
                            let saved_c = (*z).c;
                            ret = insert_s(z, (*z).c, (*z).c, 1, S_4.as_ptr());
                            (*z).c = saved_c;
                        }
                        if ret < 0 {
                            return ret;
                        }
                    }
                }
                _ => {}
            }
        }
        _ => {}
    }
    1
}

unsafe extern "C" fn r_Step_1c(z: *mut SN_env) -> c_int {
    (*z).ket = (*z).c;
    {
        let m1 = (*z).l - (*z).c;
        'lab0: {
            'lab1: {
                if (*z).c <= (*z).lb || *(*z).p.offset(((*z).c - 1) as isize) != b'y' {
                    break 'lab1;
                }
                (*z).c -= 1;
                break 'lab0;
            }
            // lab1:
            (*z).c = (*z).l - m1;
            if (*z).c <= (*z).lb || *(*z).p.offset(((*z).c - 1) as isize) != b'Y' {
                return 0;
            }
            (*z).c -= 1;
        }
    }
    // lab0:
    (*z).bra = (*z).c;

    {
        let ret = out_grouping_b_U(z, G_V.as_ptr(), 97, 121, 1);
        if ret < 0 {
            return 0;
        }
        (*z).c -= ret;
    }
    {
        let ret = slice_from_s(z, 1, S_5.as_ptr());
        if ret < 0 {
            return ret;
        }
    }
    1
}

unsafe extern "C" fn r_Step_2(z: *mut SN_env) -> c_int {
    let among_var;
    (*z).ket = (*z).c;
    if (*z).c - 2 <= (*z).lb
        || *(*z).p.offset(((*z).c - 1) as isize) as c_int >> 5 != 3
        || (815616 >> (*(*z).p.offset(((*z).c - 1) as isize) as c_int & 0x1f)) & 1 == 0
    {
        return 0;
    }
    among_var = find_among_b(z, A_3.as_ptr(), 20);
    if among_var == 0 {
        return 0;
    }
    (*z).bra = (*z).c;
    {
        let ret = r_R1(z);
        if ret <= 0 {
            return ret;
        }
    }
    match among_var {
        1 => {
            let ret = slice_from_s(z, 4, S_6.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        2 => {
            let ret = slice_from_s(z, 4, S_7.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        3 => {
            let ret = slice_from_s(z, 4, S_8.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        4 => {
            let ret = slice_from_s(z, 4, S_9.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        5 => {
            let ret = slice_from_s(z, 3, S_10.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        6 => {
            let ret = slice_from_s(z, 1, S_11.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        7 => {
            let ret = slice_from_s(z, 3, S_12.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        8 => {
            let ret = slice_from_s(z, 3, S_13.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        9 => {
            let ret = slice_from_s(z, 2, S_14.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        10 => {
            let ret = slice_from_s(z, 3, S_15.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        11 => {
            let ret = slice_from_s(z, 3, S_16.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        12 => {
            let ret = slice_from_s(z, 3, S_17.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        13 => {
            let ret = slice_from_s(z, 3, S_18.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        _ => {}
    }
    1
}

unsafe extern "C" fn r_Step_3(z: *mut SN_env) -> c_int {
    let among_var;
    (*z).ket = (*z).c;
    if (*z).c - 2 <= (*z).lb
        || *(*z).p.offset(((*z).c - 1) as isize) as c_int >> 5 != 3
        || (528928 >> (*(*z).p.offset(((*z).c - 1) as isize) as c_int & 0x1f)) & 1 == 0
    {
        return 0;
    }
    among_var = find_among_b(z, A_4.as_ptr(), 7);
    if among_var == 0 {
        return 0;
    }
    (*z).bra = (*z).c;
    {
        let ret = r_R1(z);
        if ret <= 0 {
            return ret;
        }
    }
    match among_var {
        1 => {
            let ret = slice_from_s(z, 2, S_19.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        2 => {
            let ret = slice_from_s(z, 2, S_20.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        3 => {
            let ret = slice_del(z);
            if ret < 0 {
                return ret;
            }
        }
        _ => {}
    }
    1
}

unsafe extern "C" fn r_Step_4(z: *mut SN_env) -> c_int {
    let among_var;
    (*z).ket = (*z).c;
    if (*z).c - 1 <= (*z).lb
        || *(*z).p.offset(((*z).c - 1) as isize) as c_int >> 5 != 3
        || (3961384 >> (*(*z).p.offset(((*z).c - 1) as isize) as c_int & 0x1f)) & 1 == 0
    {
        return 0;
    }
    among_var = find_among_b(z, A_5.as_ptr(), 19);
    if among_var == 0 {
        return 0;
    }
    (*z).bra = (*z).c;
    {
        let ret = r_R2(z);
        if ret <= 0 {
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
            {
                let m1 = (*z).l - (*z).c;
                'lab0: {
                    'lab1: {
                        if (*z).c <= (*z).lb || *(*z).p.offset(((*z).c - 1) as isize) != b's' {
                            break 'lab1;
                        }
                        (*z).c -= 1;
                        break 'lab0;
                    }
                    // lab1:
                    (*z).c = (*z).l - m1;
                    if (*z).c <= (*z).lb || *(*z).p.offset(((*z).c - 1) as isize) != b't' {
                        return 0;
                    }
                    (*z).c -= 1;
                }
            }
            // lab0:
            {
                let ret = slice_del(z);
                if ret < 0 {
                    return ret;
                }
            }
        }
        _ => {}
    }
    1
}

unsafe extern "C" fn r_Step_5a(z: *mut SN_env) -> c_int {
    (*z).ket = (*z).c;
    if (*z).c <= (*z).lb || *(*z).p.offset(((*z).c - 1) as isize) != b'e' {
        return 0;
    }
    (*z).c -= 1;
    (*z).bra = (*z).c;

    'lab0: {
        'lab1: {
            {
                let ret = r_R2(z);
                if ret == 0 {
                    break 'lab1;
                }
                if ret < 0 {
                    return ret;
                }
            }
            break 'lab0;
        }
        // lab1:
        {
            let ret = r_R1(z);
            if ret <= 0 {
                return ret;
            }
        }
        {
            let m1 = (*z).l - (*z).c;
            'lab2: {
                {
                    let ret = r_shortv(z);
                    if ret == 0 {
                        break 'lab2;
                    }
                    if ret < 0 {
                        return ret;
                    }
                }
                return 0;
            }
            // lab2:
            (*z).c = (*z).l - m1;
        }
    }
    // lab0:
    {
        let ret = slice_del(z);
        if ret < 0 {
            return ret;
        }
    }
    1
}

unsafe extern "C" fn r_Step_5b(z: *mut SN_env) -> c_int {
    (*z).ket = (*z).c;
    if (*z).c <= (*z).lb || *(*z).p.offset(((*z).c - 1) as isize) != b'l' {
        return 0;
    }
    (*z).c -= 1;
    (*z).bra = (*z).c;
    {
        let ret = r_R2(z);
        if ret <= 0 {
            return ret;
        }
    }
    if (*z).c <= (*z).lb || *(*z).p.offset(((*z).c - 1) as isize) != b'l' {
        return 0;
    }
    (*z).c -= 1;
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
pub unsafe extern "C" fn porter_UTF_8_stem(z: *mut SN_env) -> c_int {
    *(*z).I.offset(2) = 0;
    {
        let c1 = (*z).c;
        'lab0: {
            (*z).bra = (*z).c;
            if (*z).c == (*z).l || *(*z).p.offset((*z).c as isize) != b'y' {
                break 'lab0;
            }
            (*z).c += 1;
            (*z).ket = (*z).c;
            {
                let ret = slice_from_s(z, 1, S_21.as_ptr());
                if ret < 0 {
                    return ret;
                }
            }
            *(*z).I.offset(2) = 1;
        }
        (*z).c = c1;
    }
    {
        let c2 = (*z).c;
        'loop1: loop {
            let c3 = (*z).c;
            'lab2: {
                'inner: loop {
                    let c4 = (*z).c;
                    'lab3: {
                        if in_grouping_U(z, G_V.as_ptr(), 97, 121, 0) != 0 {
                            break 'lab3;
                        }
                        (*z).bra = (*z).c;
                        if (*z).c == (*z).l || *(*z).p.offset((*z).c as isize) != b'y' {
                            break 'lab3;
                        }
                        (*z).c += 1;
                        (*z).ket = (*z).c;
                        (*z).c = c4;
                        break 'inner;
                    }
                    // lab3:
                    (*z).c = c4;
                    {
                        let ret = skip_utf8((*z).p, (*z).c, (*z).l, 1);
                        if ret < 0 {
                            break 'lab2;
                        }
                        (*z).c = ret;
                    }
                }
                {
                    let ret = slice_from_s(z, 1, S_22.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
                *(*z).I.offset(2) = 1;
                continue 'loop1;
            }
            // lab2:
            (*z).c = c3;
            break 'loop1;
        }
        (*z).c = c2;
    }
    *(*z).I.offset(1) = (*z).l;
    *(*z).I.offset(0) = (*z).l;
    {
        let c5 = (*z).c;

        'lab4: {
            {
                let ret = out_grouping_U(z, G_V.as_ptr(), 97, 121, 1);
                if ret < 0 {
                    break 'lab4;
                }
                (*z).c += ret;
            }

            {
                let ret = in_grouping_U(z, G_V.as_ptr(), 97, 121, 1);
                if ret < 0 {
                    break 'lab4;
                }
                (*z).c += ret;
            }
            *(*z).I.offset(1) = (*z).c;

            {
                let ret = out_grouping_U(z, G_V.as_ptr(), 97, 121, 1);
                if ret < 0 {
                    break 'lab4;
                }
                (*z).c += ret;
            }

            {
                let ret = in_grouping_U(z, G_V.as_ptr(), 97, 121, 1);
                if ret < 0 {
                    break 'lab4;
                }
                (*z).c += ret;
            }
            *(*z).I.offset(0) = (*z).c;
        }
        // lab4:
        (*z).c = c5;
    }
    (*z).lb = (*z).c;
    (*z).c = (*z).l;

    {
        let m6 = (*z).l - (*z).c;
        {
            let ret = r_Step_1a(z);
            if ret < 0 {
                return ret;
            }
        }
        (*z).c = (*z).l - m6;
    }
    {
        let m7 = (*z).l - (*z).c;
        {
            let ret = r_Step_1b(z);
            if ret < 0 {
                return ret;
            }
        }
        (*z).c = (*z).l - m7;
    }
    {
        let m8 = (*z).l - (*z).c;
        {
            let ret = r_Step_1c(z);
            if ret < 0 {
                return ret;
            }
        }
        (*z).c = (*z).l - m8;
    }
    {
        let m9 = (*z).l - (*z).c;
        {
            let ret = r_Step_2(z);
            if ret < 0 {
                return ret;
            }
        }
        (*z).c = (*z).l - m9;
    }
    {
        let m10 = (*z).l - (*z).c;
        {
            let ret = r_Step_3(z);
            if ret < 0 {
                return ret;
            }
        }
        (*z).c = (*z).l - m10;
    }
    {
        let m11 = (*z).l - (*z).c;
        {
            let ret = r_Step_4(z);
            if ret < 0 {
                return ret;
            }
        }
        (*z).c = (*z).l - m11;
    }
    {
        let m12 = (*z).l - (*z).c;
        {
            let ret = r_Step_5a(z);
            if ret < 0 {
                return ret;
            }
        }
        (*z).c = (*z).l - m12;
    }
    {
        let m13 = (*z).l - (*z).c;
        {
            let ret = r_Step_5b(z);
            if ret < 0 {
                return ret;
            }
        }
        (*z).c = (*z).l - m13;
    }
    (*z).c = (*z).lb;
    {
        let c14 = (*z).c;
        'lab5: {
            if *(*z).I.offset(2) == 0 {
                break 'lab5;
            }
            'loop6: loop {
                let c15 = (*z).c;
                'inner2: loop {
                    let c16 = (*z).c;
                    'lab7: {
                        (*z).bra = (*z).c;
                        if (*z).c == (*z).l || *(*z).p.offset((*z).c as isize) != b'Y' {
                            break 'lab7;
                        }
                        (*z).c += 1;
                        (*z).ket = (*z).c;
                        (*z).c = c16;
                        break 'inner2;
                    }
                    // lab7:
                    (*z).c = c16;
                    {
                        let ret = skip_utf8((*z).p, (*z).c, (*z).l, 1);
                        if ret < 0 {
                            (*z).c = c15;
                            break 'loop6;
                        }
                        (*z).c = ret;
                    }
                }
                {
                    let ret = slice_from_s(z, 1, S_23.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
                continue 'loop6;
            }
        }
        // lab5:
        (*z).c = c14;
    }
    1
}

#[no_mangle]
pub unsafe extern "C" fn porter_UTF_8_create_env() -> *mut SN_env {
    SN_create_env(0, 3)
}

#[no_mangle]
pub unsafe extern "C" fn porter_UTF_8_close_env(z: *mut SN_env) {
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
        let z = porter_UTF_8_create_env();
        assert!(!z.is_null());
        let rc = SN_set_current(z, word.len() as c_int, word.as_ptr());
        assert!(rc >= 0);
        let rc = porter_UTF_8_stem(z);
        assert!(rc >= 0, "stem returned {rc}");
        let out = std::slice::from_raw_parts((*z).p, SIZE((*z).p) as usize).to_vec();
        porter_UTF_8_close_env(z);
        out
    }

    // A short root with no strippable suffix is returned unchanged.
    #[test]
    fn short_root_unchanged() {
        unsafe {
            assert_eq!(stem(b"bid"), b"bid".to_vec());
        }
    }

    // Idempotence: stemming a stem yields the same stem.
    #[test]
    fn idempotent() {
        unsafe {
            for w in [
                &b"happy"[..],
                &b"national"[..],
                &b"relational"[..],
                &b"conditional"[..],
            ] {
                let once = stem(w);
                let twice = stem(&once);
                assert_eq!(once, twice, "not idempotent for {:?}", w);
                assert!(!once.is_empty());
            }
        }
    }

    // A plural-style suffix collapses; result must be non-empty and not longer.
    #[test]
    fn suffix_stripped_nonempty() {
        unsafe {
            let r = stem(b"caresses");
            assert!(!r.is_empty());
            assert!(r.len() <= "caresses".len());
        }
    }
}
