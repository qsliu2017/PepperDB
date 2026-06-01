//! Snowball Irish stemmer (UTF-8).
//!
//! 1:1 translation of the GENERATED file
//! `src/backend/snowball/libstemmer/stem_UTF_8_irish.c` merged with its header
//! `src/include/snowball/libstemmer/stem_UTF_8_irish.h`. Mechanical port of the
//! goto-heavy generated C; behaviour is exercised end-to-end against the ported
//! Snowball runtime in the tests below.

use crate::prelude::*;

use crate::snowball::api::{among, symbol, SIZE, SN_close_env, SN_create_env, SN_env, SN_set_current};
use crate::snowball::utilities::{
    find_among, find_among_b, in_grouping_U, out_grouping_U, slice_del, slice_from_s,
};

// ---------------------------------------------------------------------------
// among string tables
// ---------------------------------------------------------------------------

static S_0_0: [symbol; 2] = [b'b', b'\''];
static S_0_1: [symbol; 2] = [b'b', b'h'];
static S_0_2: [symbol; 3] = [b'b', b'h', b'f'];
static S_0_3: [symbol; 2] = [b'b', b'p'];
static S_0_4: [symbol; 2] = [b'c', b'h'];
static S_0_5: [symbol; 2] = [b'd', b'\''];
static S_0_6: [symbol; 4] = [b'd', b'\'', b'f', b'h'];
static S_0_7: [symbol; 2] = [b'd', b'h'];
static S_0_8: [symbol; 2] = [b'd', b't'];
static S_0_9: [symbol; 2] = [b'f', b'h'];
static S_0_10: [symbol; 2] = [b'g', b'c'];
static S_0_11: [symbol; 2] = [b'g', b'h'];
static S_0_12: [symbol; 2] = [b'h', b'-'];
static S_0_13: [symbol; 2] = [b'm', b'\''];
static S_0_14: [symbol; 2] = [b'm', b'b'];
static S_0_15: [symbol; 2] = [b'm', b'h'];
static S_0_16: [symbol; 2] = [b'n', b'-'];
static S_0_17: [symbol; 2] = [b'n', b'd'];
static S_0_18: [symbol; 2] = [b'n', b'g'];
static S_0_19: [symbol; 2] = [b'p', b'h'];
static S_0_20: [symbol; 2] = [b's', b'h'];
static S_0_21: [symbol; 2] = [b't', b'-'];
static S_0_22: [symbol; 2] = [b't', b'h'];
static S_0_23: [symbol; 2] = [b't', b's'];

static A_0: [among; 24] = [
    among { s_size: 2, s: S_0_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_0_1.as_ptr(), substring_i: -1, result: 4, function: None },
    among { s_size: 3, s: S_0_2.as_ptr(), substring_i: 1, result: 2, function: None },
    among { s_size: 2, s: S_0_3.as_ptr(), substring_i: -1, result: 8, function: None },
    among { s_size: 2, s: S_0_4.as_ptr(), substring_i: -1, result: 5, function: None },
    among { s_size: 2, s: S_0_5.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_0_6.as_ptr(), substring_i: 5, result: 2, function: None },
    among { s_size: 2, s: S_0_7.as_ptr(), substring_i: -1, result: 6, function: None },
    among { s_size: 2, s: S_0_8.as_ptr(), substring_i: -1, result: 9, function: None },
    among { s_size: 2, s: S_0_9.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 2, s: S_0_10.as_ptr(), substring_i: -1, result: 5, function: None },
    among { s_size: 2, s: S_0_11.as_ptr(), substring_i: -1, result: 7, function: None },
    among { s_size: 2, s: S_0_12.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_0_13.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_0_14.as_ptr(), substring_i: -1, result: 4, function: None },
    among { s_size: 2, s: S_0_15.as_ptr(), substring_i: -1, result: 10, function: None },
    among { s_size: 2, s: S_0_16.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_0_17.as_ptr(), substring_i: -1, result: 6, function: None },
    among { s_size: 2, s: S_0_18.as_ptr(), substring_i: -1, result: 7, function: None },
    among { s_size: 2, s: S_0_19.as_ptr(), substring_i: -1, result: 8, function: None },
    among { s_size: 2, s: S_0_20.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 2, s: S_0_21.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_0_22.as_ptr(), substring_i: -1, result: 9, function: None },
    among { s_size: 2, s: S_0_23.as_ptr(), substring_i: -1, result: 3, function: None },
];

static S_1_0: [symbol; 7] = [0xC3, 0xAD, b'o', b'c', b'h', b't', b'a'];
static S_1_1: [symbol; 8] = [b'a', 0xC3, 0xAD, b'o', b'c', b'h', b't', b'a'];
static S_1_2: [symbol; 3] = [b'i', b'r', b'e'];
static S_1_3: [symbol; 4] = [b'a', b'i', b'r', b'e'];
static S_1_4: [symbol; 3] = [b'a', b'b', b'h'];
static S_1_5: [symbol; 4] = [b'e', b'a', b'b', b'h'];
static S_1_6: [symbol; 3] = [b'i', b'b', b'h'];
static S_1_7: [symbol; 4] = [b'a', b'i', b'b', b'h'];
static S_1_8: [symbol; 3] = [b'a', b'm', b'h'];
static S_1_9: [symbol; 4] = [b'e', b'a', b'm', b'h'];
static S_1_10: [symbol; 3] = [b'i', b'm', b'h'];
static S_1_11: [symbol; 4] = [b'a', b'i', b'm', b'h'];
static S_1_12: [symbol; 6] = [0xC3, 0xAD, b'o', b'c', b'h', b't'];
static S_1_13: [symbol; 7] = [b'a', 0xC3, 0xAD, b'o', b'c', b'h', b't'];
static S_1_14: [symbol; 4] = [b'i', b'r', 0xC3, 0xAD];
static S_1_15: [symbol; 5] = [b'a', b'i', b'r', 0xC3, 0xAD];

static A_1: [among; 16] = [
    among { s_size: 7, s: S_1_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_1_1.as_ptr(), substring_i: 0, result: 1, function: None },
    among { s_size: 3, s: S_1_2.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 4, s: S_1_3.as_ptr(), substring_i: 2, result: 2, function: None },
    among { s_size: 3, s: S_1_4.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_1_5.as_ptr(), substring_i: 4, result: 1, function: None },
    among { s_size: 3, s: S_1_6.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_1_7.as_ptr(), substring_i: 6, result: 1, function: None },
    among { s_size: 3, s: S_1_8.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_1_9.as_ptr(), substring_i: 8, result: 1, function: None },
    among { s_size: 3, s: S_1_10.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_1_11.as_ptr(), substring_i: 10, result: 1, function: None },
    among { s_size: 6, s: S_1_12.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 7, s: S_1_13.as_ptr(), substring_i: 12, result: 1, function: None },
    among { s_size: 4, s: S_1_14.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 5, s: S_1_15.as_ptr(), substring_i: 14, result: 2, function: None },
];

static S_2_0: [symbol; 9] = [0xC3, 0xB3, b'i', b'd', b'e', b'a', b'c', b'h', b'a'];
static S_2_1: [symbol; 7] = [b'p', b'a', b't', b'a', b'c', b'h', b'a'];
static S_2_2: [symbol; 5] = [b'a', b'c', b'h', b't', b'a'];
static S_2_3: [symbol; 8] = [b'a', b'r', b'c', b'a', b'c', b'h', b't', b'a'];
static S_2_4: [symbol; 6] = [b'e', b'a', b'c', b'h', b't', b'a'];
static S_2_5: [symbol; 12] = [b'g', b'r', b'a', b'f', b'a', 0xC3, 0xAD, b'o', b'c', b'h', b't', b'a'];
static S_2_6: [symbol; 5] = [b'p', b'a', b'i', b't', b'e'];
static S_2_7: [symbol; 3] = [b'a', b'c', b'h'];
static S_2_8: [symbol; 4] = [b'e', b'a', b'c', b'h'];
static S_2_9: [symbol; 8] = [0xC3, 0xB3, b'i', b'd', b'e', b'a', b'c', b'h'];
static S_2_10: [symbol; 7] = [b'g', b'i', b'n', b'e', b'a', b'c', b'h'];
static S_2_11: [symbol; 6] = [b'p', b'a', b't', b'a', b'c', b'h'];
static S_2_12: [symbol; 10] = [b'g', b'r', b'a', b'f', b'a', 0xC3, 0xAD, b'o', b'c', b'h'];
static S_2_13: [symbol; 7] = [b'p', b'a', b't', b'a', b'i', b'g', b'h'];
static S_2_14: [symbol; 7] = [0xC3, 0xB3, b'i', b'd', b'i', b'g', b'h'];
static S_2_15: [symbol; 8] = [b'a', b'c', b'h', b't', 0xC3, 0xBA, b'i', b'l'];
static S_2_16: [symbol; 9] = [b'e', b'a', b'c', b'h', b't', 0xC3, 0xBA, b'i', b'l'];
static S_2_17: [symbol; 6] = [b'g', b'i', b'n', b'e', b'a', b's'];
static S_2_18: [symbol; 5] = [b'g', b'i', b'n', b'i', b's'];
static S_2_19: [symbol; 4] = [b'a', b'c', b'h', b't'];
static S_2_20: [symbol; 7] = [b'a', b'r', b'c', b'a', b'c', b'h', b't'];
static S_2_21: [symbol; 5] = [b'e', b'a', b'c', b'h', b't'];
static S_2_22: [symbol; 11] = [b'g', b'r', b'a', b'f', b'a', 0xC3, 0xAD, b'o', b'c', b'h', b't'];
static S_2_23: [symbol; 10] = [b'a', b'r', b'c', b'a', b'c', b'h', b't', b'a', 0xC3, 0xAD];
static S_2_24: [symbol; 14] = [b'g', b'r', b'a', b'f', b'a', 0xC3, 0xAD, b'o', b'c', b'h', b't', b'a', 0xC3, 0xAD];

static A_2: [among; 25] = [
    among { s_size: 9, s: S_2_0.as_ptr(), substring_i: -1, result: 6, function: None },
    among { s_size: 7, s: S_2_1.as_ptr(), substring_i: -1, result: 5, function: None },
    among { s_size: 5, s: S_2_2.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_2_3.as_ptr(), substring_i: 2, result: 2, function: None },
    among { s_size: 6, s: S_2_4.as_ptr(), substring_i: 2, result: 1, function: None },
    among { s_size: 12, s: S_2_5.as_ptr(), substring_i: -1, result: 4, function: None },
    among { s_size: 5, s: S_2_6.as_ptr(), substring_i: -1, result: 5, function: None },
    among { s_size: 3, s: S_2_7.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_2_8.as_ptr(), substring_i: 7, result: 1, function: None },
    among { s_size: 8, s: S_2_9.as_ptr(), substring_i: 8, result: 6, function: None },
    among { s_size: 7, s: S_2_10.as_ptr(), substring_i: 8, result: 3, function: None },
    among { s_size: 6, s: S_2_11.as_ptr(), substring_i: 7, result: 5, function: None },
    among { s_size: 10, s: S_2_12.as_ptr(), substring_i: -1, result: 4, function: None },
    among { s_size: 7, s: S_2_13.as_ptr(), substring_i: -1, result: 5, function: None },
    among { s_size: 7, s: S_2_14.as_ptr(), substring_i: -1, result: 6, function: None },
    among { s_size: 8, s: S_2_15.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 9, s: S_2_16.as_ptr(), substring_i: 15, result: 1, function: None },
    among { s_size: 6, s: S_2_17.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 5, s: S_2_18.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 4, s: S_2_19.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 7, s: S_2_20.as_ptr(), substring_i: 19, result: 2, function: None },
    among { s_size: 5, s: S_2_21.as_ptr(), substring_i: 19, result: 1, function: None },
    among { s_size: 11, s: S_2_22.as_ptr(), substring_i: -1, result: 4, function: None },
    among { s_size: 10, s: S_2_23.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 14, s: S_2_24.as_ptr(), substring_i: -1, result: 4, function: None },
];

static S_3_0: [symbol; 4] = [b'i', b'm', b'i', b'd'];
static S_3_1: [symbol; 5] = [b'a', b'i', b'm', b'i', b'd'];
static S_3_2: [symbol; 5] = [0xC3, 0xAD, b'm', b'i', b'd'];
static S_3_3: [symbol; 6] = [b'a', 0xC3, 0xAD, b'm', b'i', b'd'];
static S_3_4: [symbol; 3] = [b'a', b'd', b'h'];
static S_3_5: [symbol; 4] = [b'e', b'a', b'd', b'h'];
static S_3_6: [symbol; 5] = [b'f', b'a', b'i', b'd', b'h'];
static S_3_7: [symbol; 4] = [b'f', b'i', b'd', b'h'];
static S_3_8: [symbol; 4] = [0xC3, 0xA1, b'i', b'l'];
static S_3_9: [symbol; 3] = [b'a', b'i', b'n'];
static S_3_10: [symbol; 4] = [b't', b'e', b'a', b'r'];
static S_3_11: [symbol; 3] = [b't', b'a', b'r'];

static A_3: [among; 12] = [
    among { s_size: 4, s: S_3_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_3_1.as_ptr(), substring_i: 0, result: 1, function: None },
    among { s_size: 5, s: S_3_2.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_3_3.as_ptr(), substring_i: 2, result: 1, function: None },
    among { s_size: 3, s: S_3_4.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 4, s: S_3_5.as_ptr(), substring_i: 4, result: 2, function: None },
    among { s_size: 5, s: S_3_6.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_3_7.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_3_8.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 3, s: S_3_9.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 4, s: S_3_10.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 3, s: S_3_11.as_ptr(), substring_i: -1, result: 2, function: None },
];

static G_V: [c_uchar; 20] = [17, 65, 16, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 17, 4, 2];

static S_0: [symbol; 1] = [b'f'];
static S_1: [symbol; 1] = [b's'];
static S_2: [symbol; 1] = [b'b'];
static S_3: [symbol; 1] = [b'c'];
static S_4: [symbol; 1] = [b'd'];
static S_5: [symbol; 1] = [b'g'];
static S_6: [symbol; 1] = [b'p'];
static S_7: [symbol; 1] = [b't'];
static S_8: [symbol; 1] = [b'm'];
static S_9: [symbol; 3] = [b'a', b'r', b'c'];
static S_10: [symbol; 3] = [b'g', b'i', b'n'];
static S_11: [symbol; 4] = [b'g', b'r', b'a', b'f'];
static S_12: [symbol; 5] = [b'p', b'a', b'i', b't', b'e'];
static S_13: [symbol; 4] = [0xC3, 0xB3, b'i', b'd'];

// ---------------------------------------------------------------------------
// helper functions
// ---------------------------------------------------------------------------

unsafe fn r_mark_regions(z: *mut SN_env) -> c_int {
    *(*z).I.offset(2) = (*z).l;
    *(*z).I.offset(1) = (*z).l;
    *(*z).I.offset(0) = (*z).l;
    {
        let c1 = (*z).c;
        'lab0: {
            {
                let ret = out_grouping_U(z, G_V.as_ptr(), 97, 250, 1);
                if ret < 0 {
                    break 'lab0;
                }
                (*z).c += ret;
            }
            *(*z).I.offset(2) = (*z).c;

            {
                let ret = in_grouping_U(z, G_V.as_ptr(), 97, 250, 1);
                if ret < 0 {
                    break 'lab0;
                }
                (*z).c += ret;
            }
            *(*z).I.offset(1) = (*z).c;

            {
                let ret = out_grouping_U(z, G_V.as_ptr(), 97, 250, 1);
                if ret < 0 {
                    break 'lab0;
                }
                (*z).c += ret;
            }

            {
                let ret = in_grouping_U(z, G_V.as_ptr(), 97, 250, 1);
                if ret < 0 {
                    break 'lab0;
                }
                (*z).c += ret;
            }
            *(*z).I.offset(0) = (*z).c;
        }
        (*z).c = c1;
    }
    1
}

unsafe fn r_initial_morph(z: *mut SN_env) -> c_int {
    let among_var;
    (*z).bra = (*z).c;
    among_var = find_among(z, A_0.as_ptr(), 24);
    if among_var == 0 {
        return 0;
    }
    (*z).ket = (*z).c;
    match among_var {
        1 => {
            let ret = slice_del(z);
            if ret < 0 {
                return ret;
            }
        }
        2 => {
            let ret = slice_from_s(z, 1, S_0.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        3 => {
            let ret = slice_from_s(z, 1, S_1.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        4 => {
            let ret = slice_from_s(z, 1, S_2.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        5 => {
            let ret = slice_from_s(z, 1, S_3.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        6 => {
            let ret = slice_from_s(z, 1, S_4.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        7 => {
            let ret = slice_from_s(z, 1, S_5.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        8 => {
            let ret = slice_from_s(z, 1, S_6.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        9 => {
            let ret = slice_from_s(z, 1, S_7.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        10 => {
            let ret = slice_from_s(z, 1, S_8.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        _ => {}
    }
    1
}

unsafe fn r_RV(z: *mut SN_env) -> c_int {
    (*(*z).I.offset(2) <= (*z).c) as c_int
}

unsafe fn r_R1(z: *mut SN_env) -> c_int {
    (*(*z).I.offset(1) <= (*z).c) as c_int
}

unsafe fn r_R2(z: *mut SN_env) -> c_int {
    (*(*z).I.offset(0) <= (*z).c) as c_int
}

unsafe fn r_noun_sfx(z: *mut SN_env) -> c_int {
    let among_var;
    (*z).ket = (*z).c;
    among_var = find_among_b(z, A_1.as_ptr(), 16);
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
                let ret = slice_del(z);
                if ret < 0 {
                    return ret;
                }
            }
        }
        2 => {
            {
                let ret = r_R2(z);
                if ret <= 0 {
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
        _ => {}
    }
    1
}

unsafe fn r_deriv(z: *mut SN_env) -> c_int {
    let among_var;
    (*z).ket = (*z).c;
    among_var = find_among_b(z, A_2.as_ptr(), 25);
    if among_var == 0 {
        return 0;
    }
    (*z).bra = (*z).c;
    match among_var {
        1 => {
            {
                let ret = r_R2(z);
                if ret <= 0 {
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
        2 => {
            let ret = slice_from_s(z, 3, S_9.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        3 => {
            let ret = slice_from_s(z, 3, S_10.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        4 => {
            let ret = slice_from_s(z, 4, S_11.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        5 => {
            let ret = slice_from_s(z, 5, S_12.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        6 => {
            let ret = slice_from_s(z, 4, S_13.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        _ => {}
    }
    1
}

unsafe fn r_verb_sfx(z: *mut SN_env) -> c_int {
    let among_var;
    (*z).ket = (*z).c;
    if (*z).c - 2 <= (*z).lb
        || *(*z).p.offset(((*z).c - 1) as isize) as c_int >> 5 != 3
        || ((282896 >> (*(*z).p.offset(((*z).c - 1) as isize) as c_int & 0x1f)) & 1) == 0
    {
        return 0;
    }
    among_var = find_among_b(z, A_3.as_ptr(), 12);
    if among_var == 0 {
        return 0;
    }
    (*z).bra = (*z).c;
    match among_var {
        1 => {
            {
                let ret = r_RV(z);
                if ret <= 0 {
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
        2 => {
            {
                let ret = r_R1(z);
                if ret <= 0 {
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
        _ => {}
    }
    1
}

// ---------------------------------------------------------------------------
// exported stem entry point
// ---------------------------------------------------------------------------

#[no_mangle]
pub unsafe extern "C" fn irish_UTF_8_stem(z: *mut SN_env) -> c_int {
    {
        let c1 = (*z).c;
        {
            let ret = r_initial_morph(z);
            if ret < 0 {
                return ret;
            }
        }
        (*z).c = c1;
    }

    {
        let ret = r_mark_regions(z);
        if ret < 0 {
            return ret;
        }
    }
    (*z).lb = (*z).c;
    (*z).c = (*z).l;

    {
        let m2 = (*z).l - (*z).c;
        let _ = m2;
        {
            let ret = r_noun_sfx(z);
            if ret < 0 {
                return ret;
            }
        }
        (*z).c = (*z).l - m2;
    }
    {
        let m3 = (*z).l - (*z).c;
        let _ = m3;
        {
            let ret = r_deriv(z);
            if ret < 0 {
                return ret;
            }
        }
        (*z).c = (*z).l - m3;
    }
    {
        let m4 = (*z).l - (*z).c;
        let _ = m4;
        {
            let ret = r_verb_sfx(z);
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
pub unsafe extern "C" fn irish_UTF_8_create_env() -> *mut SN_env {
    SN_create_env(0, 3)
}

#[no_mangle]
pub unsafe extern "C" fn irish_UTF_8_close_env(z: *mut SN_env) {
    SN_close_env(z, 0)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    unsafe fn stem_word(word: &[u8]) -> Vec<symbol> {
        let z = irish_UTF_8_create_env();
        assert!(!z.is_null());
        SN_set_current(z, word.len() as c_int, word.as_ptr());
        let rc = irish_UTF_8_stem(z);
        assert!(rc >= 0);
        let out = std::slice::from_raw_parts((*z).p, SIZE((*z).p) as usize).to_vec();
        irish_UTF_8_close_env(z);
        out
    }

    #[test]
    fn short_root_unchanged() {
        // A short root word with no applicable suffix stays as-is.
        let w = b"an";
        unsafe {
            let out = stem_word(w);
            assert_eq!(out, w.to_vec());
        }
    }

    #[test]
    fn idempotent_on_longer_word() {
        // "grafaiochta" (graf + aiocht...) exercises deriv slicing.
        let w: &[u8] = &[b'g', b'r', b'a', b'f', b'a', 0xC3, 0xAD, b'o', b'c', b'h', b't', b'a'];
        unsafe {
            let once = stem_word(w);
            assert!(!once.is_empty());
            let twice = stem_word(&once);
            assert_eq!(once, twice);
        }
    }

    #[test]
    fn returns_nonempty_and_ok() {
        let w: &[u8] = &[b'p', b'a', b't', b'a', b'c', b'h', b'a'];
        unsafe {
            let z = irish_UTF_8_create_env();
            SN_set_current(z, w.len() as c_int, w.as_ptr());
            let rc = irish_UTF_8_stem(z);
            assert!(rc >= 0);
            let out = std::slice::from_raw_parts((*z).p, SIZE((*z).p) as usize).to_vec();
            assert!(!out.is_empty());
            irish_UTF_8_close_env(z);
        }
    }
}
