//! Romanian Snowball stemmer (UTF-8).
//!
//! 1:1 translation of the generated
//! `src/backend/snowball/libstemmer/stem_UTF_8_romanian.c` (Snowball 2.2.0),
//! merged with its header `stem_UTF_8_romanian.h`. The runtime helpers come from
//! `crate::snowball::api` and `crate::snowball::utilities`.

use crate::prelude::*;

use crate::snowball::api::{among, symbol, SN_close_env, SN_create_env, SN_env, SIZE};
use crate::snowball::utilities::{
    eq_s_b, find_among, find_among_b, in_grouping_U, out_grouping_U, out_grouping_b_U, skip_utf8,
    slice_del, slice_from_s,
};

// ---------------------------------------------------------------------------
// among string tables
// ---------------------------------------------------------------------------

static S_0_0: [symbol; 2] = [0xC5, 0x9F];
static S_0_1: [symbol; 2] = [0xC5, 0xA3];

static A_0: [among; 2] = [
    among { s_size: 2, s: S_0_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_0_1.as_ptr(), substring_i: -1, result: 2, function: None },
];

static S_1_1: [symbol; 1] = [b'I'];
static S_1_2: [symbol; 1] = [b'U'];

static A_1: [among; 3] = [
    among { s_size: 0, s: std::ptr::null(), substring_i: -1, result: 3, function: None },
    among { s_size: 1, s: S_1_1.as_ptr(), substring_i: 0, result: 1, function: None },
    among { s_size: 1, s: S_1_2.as_ptr(), substring_i: 0, result: 2, function: None },
];

static S_2_0: [symbol; 2] = [b'e', b'a'];
static S_2_1: [symbol; 5] = [b'a', 0xC8, 0x9B, b'i', b'a'];
static S_2_2: [symbol; 3] = [b'a', b'u', b'a'];
static S_2_3: [symbol; 3] = [b'i', b'u', b'a'];
static S_2_4: [symbol; 5] = [b'a', 0xC8, 0x9B, b'i', b'e'];
static S_2_5: [symbol; 3] = [b'e', b'l', b'e'];
static S_2_6: [symbol; 3] = [b'i', b'l', b'e'];
static S_2_7: [symbol; 4] = [b'i', b'i', b'l', b'e'];
static S_2_8: [symbol; 3] = [b'i', b'e', b'i'];
static S_2_9: [symbol; 4] = [b'a', b't', b'e', b'i'];
static S_2_10: [symbol; 2] = [b'i', b'i'];
static S_2_11: [symbol; 4] = [b'u', b'l', b'u', b'i'];
static S_2_12: [symbol; 2] = [b'u', b'l'];
static S_2_13: [symbol; 4] = [b'e', b'l', b'o', b'r'];
static S_2_14: [symbol; 4] = [b'i', b'l', b'o', b'r'];
static S_2_15: [symbol; 5] = [b'i', b'i', b'l', b'o', b'r'];

static A_2: [among; 16] = [
    among { s_size: 2, s: S_2_0.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 5, s: S_2_1.as_ptr(), substring_i: -1, result: 7, function: None },
    among { s_size: 3, s: S_2_2.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 3, s: S_2_3.as_ptr(), substring_i: -1, result: 4, function: None },
    among { s_size: 5, s: S_2_4.as_ptr(), substring_i: -1, result: 7, function: None },
    among { s_size: 3, s: S_2_5.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 3, s: S_2_6.as_ptr(), substring_i: -1, result: 5, function: None },
    among { s_size: 4, s: S_2_7.as_ptr(), substring_i: 6, result: 4, function: None },
    among { s_size: 3, s: S_2_8.as_ptr(), substring_i: -1, result: 4, function: None },
    among { s_size: 4, s: S_2_9.as_ptr(), substring_i: -1, result: 6, function: None },
    among { s_size: 2, s: S_2_10.as_ptr(), substring_i: -1, result: 4, function: None },
    among { s_size: 4, s: S_2_11.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_2_12.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_2_13.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 4, s: S_2_14.as_ptr(), substring_i: -1, result: 4, function: None },
    among { s_size: 5, s: S_2_15.as_ptr(), substring_i: 14, result: 4, function: None },
];

static S_3_0: [symbol; 5] = [b'i', b'c', b'a', b'l', b'a'];
static S_3_1: [symbol; 5] = [b'i', b'c', b'i', b'v', b'a'];
static S_3_2: [symbol; 5] = [b'a', b't', b'i', b'v', b'a'];
static S_3_3: [symbol; 5] = [b'i', b't', b'i', b'v', b'a'];
static S_3_4: [symbol; 5] = [b'i', b'c', b'a', b'l', b'e'];
static S_3_5: [symbol; 7] = [b'a', 0xC8, 0x9B, b'i', b'u', b'n', b'e'];
static S_3_6: [symbol; 7] = [b'i', 0xC8, 0x9B, b'i', b'u', b'n', b'e'];
static S_3_7: [symbol; 6] = [b'a', b't', b'o', b'a', b'r', b'e'];
static S_3_8: [symbol; 6] = [b'i', b't', b'o', b'a', b'r', b'e'];
static S_3_9: [symbol; 7] = [0xC4, 0x83, b't', b'o', b'a', b'r', b'e'];
static S_3_10: [symbol; 7] = [b'i', b'c', b'i', b't', b'a', b't', b'e'];
static S_3_11: [symbol; 9] = [b'a', b'b', b'i', b'l', b'i', b't', b'a', b't', b'e'];
static S_3_12: [symbol; 9] = [b'i', b'b', b'i', b'l', b'i', b't', b'a', b't', b'e'];
static S_3_13: [symbol; 7] = [b'i', b'v', b'i', b't', b'a', b't', b'e'];
static S_3_14: [symbol; 5] = [b'i', b'c', b'i', b'v', b'e'];
static S_3_15: [symbol; 5] = [b'a', b't', b'i', b'v', b'e'];
static S_3_16: [symbol; 5] = [b'i', b't', b'i', b'v', b'e'];
static S_3_17: [symbol; 5] = [b'i', b'c', b'a', b'l', b'i'];
static S_3_18: [symbol; 5] = [b'a', b't', b'o', b'r', b'i'];
static S_3_19: [symbol; 7] = [b'i', b'c', b'a', b't', b'o', b'r', b'i'];
static S_3_20: [symbol; 5] = [b'i', b't', b'o', b'r', b'i'];
static S_3_21: [symbol; 6] = [0xC4, 0x83, b't', b'o', b'r', b'i'];
static S_3_22: [symbol; 7] = [b'i', b'c', b'i', b't', b'a', b't', b'i'];
static S_3_23: [symbol; 9] = [b'a', b'b', b'i', b'l', b'i', b't', b'a', b't', b'i'];
static S_3_24: [symbol; 7] = [b'i', b'v', b'i', b't', b'a', b't', b'i'];
static S_3_25: [symbol; 5] = [b'i', b'c', b'i', b'v', b'i'];
static S_3_26: [symbol; 5] = [b'a', b't', b'i', b'v', b'i'];
static S_3_27: [symbol; 5] = [b'i', b't', b'i', b'v', b'i'];
static S_3_28: [symbol; 7] = [b'i', b'c', b'i', b't', 0xC4, 0x83, b'i'];
static S_3_29: [symbol; 9] = [b'a', b'b', b'i', b'l', b'i', b't', 0xC4, 0x83, b'i'];
static S_3_30: [symbol; 7] = [b'i', b'v', b'i', b't', 0xC4, 0x83, b'i'];
static S_3_31: [symbol; 9] = [b'i', b'c', b'i', b't', 0xC4, 0x83, 0xC8, 0x9B, b'i'];
static S_3_32: [symbol; 11] = [b'a', b'b', b'i', b'l', b'i', b't', 0xC4, 0x83, 0xC8, 0x9B, b'i'];
static S_3_33: [symbol; 9] = [b'i', b'v', b'i', b't', 0xC4, 0x83, 0xC8, 0x9B, b'i'];
static S_3_34: [symbol; 4] = [b'i', b'c', b'a', b'l'];
static S_3_35: [symbol; 4] = [b'a', b't', b'o', b'r'];
static S_3_36: [symbol; 6] = [b'i', b'c', b'a', b't', b'o', b'r'];
static S_3_37: [symbol; 4] = [b'i', b't', b'o', b'r'];
static S_3_38: [symbol; 5] = [0xC4, 0x83, b't', b'o', b'r'];
static S_3_39: [symbol; 4] = [b'i', b'c', b'i', b'v'];
static S_3_40: [symbol; 4] = [b'a', b't', b'i', b'v'];
static S_3_41: [symbol; 4] = [b'i', b't', b'i', b'v'];
static S_3_42: [symbol; 6] = [b'i', b'c', b'a', b'l', 0xC4, 0x83];
static S_3_43: [symbol; 6] = [b'i', b'c', b'i', b'v', 0xC4, 0x83];
static S_3_44: [symbol; 6] = [b'a', b't', b'i', b'v', 0xC4, 0x83];
static S_3_45: [symbol; 6] = [b'i', b't', b'i', b'v', 0xC4, 0x83];

static A_3: [among; 46] = [
    among { s_size: 5, s: S_3_0.as_ptr(), substring_i: -1, result: 4, function: None },
    among { s_size: 5, s: S_3_1.as_ptr(), substring_i: -1, result: 4, function: None },
    among { s_size: 5, s: S_3_2.as_ptr(), substring_i: -1, result: 5, function: None },
    among { s_size: 5, s: S_3_3.as_ptr(), substring_i: -1, result: 6, function: None },
    among { s_size: 5, s: S_3_4.as_ptr(), substring_i: -1, result: 4, function: None },
    among { s_size: 7, s: S_3_5.as_ptr(), substring_i: -1, result: 5, function: None },
    among { s_size: 7, s: S_3_6.as_ptr(), substring_i: -1, result: 6, function: None },
    among { s_size: 6, s: S_3_7.as_ptr(), substring_i: -1, result: 5, function: None },
    among { s_size: 6, s: S_3_8.as_ptr(), substring_i: -1, result: 6, function: None },
    among { s_size: 7, s: S_3_9.as_ptr(), substring_i: -1, result: 5, function: None },
    among { s_size: 7, s: S_3_10.as_ptr(), substring_i: -1, result: 4, function: None },
    among { s_size: 9, s: S_3_11.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 9, s: S_3_12.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 7, s: S_3_13.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 5, s: S_3_14.as_ptr(), substring_i: -1, result: 4, function: None },
    among { s_size: 5, s: S_3_15.as_ptr(), substring_i: -1, result: 5, function: None },
    among { s_size: 5, s: S_3_16.as_ptr(), substring_i: -1, result: 6, function: None },
    among { s_size: 5, s: S_3_17.as_ptr(), substring_i: -1, result: 4, function: None },
    among { s_size: 5, s: S_3_18.as_ptr(), substring_i: -1, result: 5, function: None },
    among { s_size: 7, s: S_3_19.as_ptr(), substring_i: 18, result: 4, function: None },
    among { s_size: 5, s: S_3_20.as_ptr(), substring_i: -1, result: 6, function: None },
    among { s_size: 6, s: S_3_21.as_ptr(), substring_i: -1, result: 5, function: None },
    among { s_size: 7, s: S_3_22.as_ptr(), substring_i: -1, result: 4, function: None },
    among { s_size: 9, s: S_3_23.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 7, s: S_3_24.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 5, s: S_3_25.as_ptr(), substring_i: -1, result: 4, function: None },
    among { s_size: 5, s: S_3_26.as_ptr(), substring_i: -1, result: 5, function: None },
    among { s_size: 5, s: S_3_27.as_ptr(), substring_i: -1, result: 6, function: None },
    among { s_size: 7, s: S_3_28.as_ptr(), substring_i: -1, result: 4, function: None },
    among { s_size: 9, s: S_3_29.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 7, s: S_3_30.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 9, s: S_3_31.as_ptr(), substring_i: -1, result: 4, function: None },
    among { s_size: 11, s: S_3_32.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 9, s: S_3_33.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 4, s: S_3_34.as_ptr(), substring_i: -1, result: 4, function: None },
    among { s_size: 4, s: S_3_35.as_ptr(), substring_i: -1, result: 5, function: None },
    among { s_size: 6, s: S_3_36.as_ptr(), substring_i: 35, result: 4, function: None },
    among { s_size: 4, s: S_3_37.as_ptr(), substring_i: -1, result: 6, function: None },
    among { s_size: 5, s: S_3_38.as_ptr(), substring_i: -1, result: 5, function: None },
    among { s_size: 4, s: S_3_39.as_ptr(), substring_i: -1, result: 4, function: None },
    among { s_size: 4, s: S_3_40.as_ptr(), substring_i: -1, result: 5, function: None },
    among { s_size: 4, s: S_3_41.as_ptr(), substring_i: -1, result: 6, function: None },
    among { s_size: 6, s: S_3_42.as_ptr(), substring_i: -1, result: 4, function: None },
    among { s_size: 6, s: S_3_43.as_ptr(), substring_i: -1, result: 4, function: None },
    among { s_size: 6, s: S_3_44.as_ptr(), substring_i: -1, result: 5, function: None },
    among { s_size: 6, s: S_3_45.as_ptr(), substring_i: -1, result: 6, function: None },
];

static S_4_0: [symbol; 3] = [b'i', b'c', b'a'];
static S_4_1: [symbol; 5] = [b'a', b'b', b'i', b'l', b'a'];
static S_4_2: [symbol; 5] = [b'i', b'b', b'i', b'l', b'a'];
static S_4_3: [symbol; 4] = [b'o', b'a', b's', b'a'];
static S_4_4: [symbol; 3] = [b'a', b't', b'a'];
static S_4_5: [symbol; 3] = [b'i', b't', b'a'];
static S_4_6: [symbol; 4] = [b'a', b'n', b't', b'a'];
static S_4_7: [symbol; 4] = [b'i', b's', b't', b'a'];
static S_4_8: [symbol; 3] = [b'u', b't', b'a'];
static S_4_9: [symbol; 3] = [b'i', b'v', b'a'];
static S_4_10: [symbol; 2] = [b'i', b'c'];
static S_4_11: [symbol; 3] = [b'i', b'c', b'e'];
static S_4_12: [symbol; 5] = [b'a', b'b', b'i', b'l', b'e'];
static S_4_13: [symbol; 5] = [b'i', b'b', b'i', b'l', b'e'];
static S_4_14: [symbol; 4] = [b'i', b's', b'm', b'e'];
static S_4_15: [symbol; 4] = [b'i', b'u', b'n', b'e'];
static S_4_16: [symbol; 4] = [b'o', b'a', b's', b'e'];
static S_4_17: [symbol; 3] = [b'a', b't', b'e'];
static S_4_18: [symbol; 5] = [b'i', b't', b'a', b't', b'e'];
static S_4_19: [symbol; 3] = [b'i', b't', b'e'];
static S_4_20: [symbol; 4] = [b'a', b'n', b't', b'e'];
static S_4_21: [symbol; 4] = [b'i', b's', b't', b'e'];
static S_4_22: [symbol; 3] = [b'u', b't', b'e'];
static S_4_23: [symbol; 3] = [b'i', b'v', b'e'];
static S_4_24: [symbol; 3] = [b'i', b'c', b'i'];
static S_4_25: [symbol; 5] = [b'a', b'b', b'i', b'l', b'i'];
static S_4_26: [symbol; 5] = [b'i', b'b', b'i', b'l', b'i'];
static S_4_27: [symbol; 4] = [b'i', b'u', b'n', b'i'];
static S_4_28: [symbol; 5] = [b'a', b't', b'o', b'r', b'i'];
static S_4_29: [symbol; 3] = [b'o', b's', b'i'];
static S_4_30: [symbol; 3] = [b'a', b't', b'i'];
static S_4_31: [symbol; 5] = [b'i', b't', b'a', b't', b'i'];
static S_4_32: [symbol; 3] = [b'i', b't', b'i'];
static S_4_33: [symbol; 4] = [b'a', b'n', b't', b'i'];
static S_4_34: [symbol; 4] = [b'i', b's', b't', b'i'];
static S_4_35: [symbol; 3] = [b'u', b't', b'i'];
static S_4_36: [symbol; 5] = [b'i', 0xC8, 0x99, b't', b'i'];
static S_4_37: [symbol; 3] = [b'i', b'v', b'i'];
static S_4_38: [symbol; 5] = [b'i', b't', 0xC4, 0x83, b'i'];
static S_4_39: [symbol; 4] = [b'o', 0xC8, 0x99, b'i'];
static S_4_40: [symbol; 7] = [b'i', b't', 0xC4, 0x83, 0xC8, 0x9B, b'i'];
static S_4_41: [symbol; 4] = [b'a', b'b', b'i', b'l'];
static S_4_42: [symbol; 4] = [b'i', b'b', b'i', b'l'];
static S_4_43: [symbol; 3] = [b'i', b's', b'm'];
static S_4_44: [symbol; 4] = [b'a', b't', b'o', b'r'];
static S_4_45: [symbol; 2] = [b'o', b's'];
static S_4_46: [symbol; 2] = [b'a', b't'];
static S_4_47: [symbol; 2] = [b'i', b't'];
static S_4_48: [symbol; 3] = [b'a', b'n', b't'];
static S_4_49: [symbol; 3] = [b'i', b's', b't'];
static S_4_50: [symbol; 2] = [b'u', b't'];
static S_4_51: [symbol; 2] = [b'i', b'v'];
static S_4_52: [symbol; 4] = [b'i', b'c', 0xC4, 0x83];
static S_4_53: [symbol; 6] = [b'a', b'b', b'i', b'l', 0xC4, 0x83];
static S_4_54: [symbol; 6] = [b'i', b'b', b'i', b'l', 0xC4, 0x83];
static S_4_55: [symbol; 5] = [b'o', b'a', b's', 0xC4, 0x83];
static S_4_56: [symbol; 4] = [b'a', b't', 0xC4, 0x83];
static S_4_57: [symbol; 4] = [b'i', b't', 0xC4, 0x83];
static S_4_58: [symbol; 5] = [b'a', b'n', b't', 0xC4, 0x83];
static S_4_59: [symbol; 5] = [b'i', b's', b't', 0xC4, 0x83];
static S_4_60: [symbol; 4] = [b'u', b't', 0xC4, 0x83];
static S_4_61: [symbol; 4] = [b'i', b'v', 0xC4, 0x83];

static A_4: [among; 62] = [
    among { s_size: 3, s: S_4_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_4_1.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_4_2.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_4_3.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_4_4.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_4_5.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_4_6.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_4_7.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 3, s: S_4_8.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_4_9.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_4_10.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_4_11.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_4_12.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_4_13.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_4_14.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 4, s: S_4_15.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 4, s: S_4_16.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_4_17.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_4_18.as_ptr(), substring_i: 17, result: 1, function: None },
    among { s_size: 3, s: S_4_19.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_4_20.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_4_21.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 3, s: S_4_22.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_4_23.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_4_24.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_4_25.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_4_26.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_4_27.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 5, s: S_4_28.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_4_29.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_4_30.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_4_31.as_ptr(), substring_i: 30, result: 1, function: None },
    among { s_size: 3, s: S_4_32.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_4_33.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_4_34.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 3, s: S_4_35.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_4_36.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 3, s: S_4_37.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_4_38.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_4_39.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 7, s: S_4_40.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_4_41.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_4_42.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_4_43.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 4, s: S_4_44.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_4_45.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_4_46.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_4_47.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_4_48.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_4_49.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 2, s: S_4_50.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_4_51.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_4_52.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_4_53.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_4_54.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_4_55.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_4_56.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_4_57.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_4_58.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_4_59.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 4, s: S_4_60.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_4_61.as_ptr(), substring_i: -1, result: 1, function: None },
];

static S_5_0: [symbol; 2] = [b'e', b'a'];
static S_5_1: [symbol; 2] = [b'i', b'a'];
static S_5_2: [symbol; 3] = [b'e', b's', b'c'];
static S_5_3: [symbol; 4] = [0xC4, 0x83, b's', b'c'];
static S_5_4: [symbol; 3] = [b'i', b'n', b'd'];
static S_5_5: [symbol; 4] = [0xC3, 0xA2, b'n', b'd'];
static S_5_6: [symbol; 3] = [b'a', b'r', b'e'];
static S_5_7: [symbol; 3] = [b'e', b'r', b'e'];
static S_5_8: [symbol; 3] = [b'i', b'r', b'e'];
static S_5_9: [symbol; 4] = [0xC3, 0xA2, b'r', b'e'];
static S_5_10: [symbol; 2] = [b's', b'e'];
static S_5_11: [symbol; 3] = [b'a', b's', b'e'];
static S_5_12: [symbol; 4] = [b's', b'e', b's', b'e'];
static S_5_13: [symbol; 3] = [b'i', b's', b'e'];
static S_5_14: [symbol; 3] = [b'u', b's', b'e'];
static S_5_15: [symbol; 4] = [0xC3, 0xA2, b's', b'e'];
static S_5_16: [symbol; 5] = [b'e', 0xC8, 0x99, b't', b'e'];
static S_5_17: [symbol; 6] = [0xC4, 0x83, 0xC8, 0x99, b't', b'e'];
static S_5_18: [symbol; 3] = [b'e', b'z', b'e'];
static S_5_19: [symbol; 2] = [b'a', b'i'];
static S_5_20: [symbol; 3] = [b'e', b'a', b'i'];
static S_5_21: [symbol; 3] = [b'i', b'a', b'i'];
static S_5_22: [symbol; 3] = [b's', b'e', b'i'];
static S_5_23: [symbol; 5] = [b'e', 0xC8, 0x99, b't', b'i'];
static S_5_24: [symbol; 6] = [0xC4, 0x83, 0xC8, 0x99, b't', b'i'];
static S_5_25: [symbol; 2] = [b'u', b'i'];
static S_5_26: [symbol; 3] = [b'e', b'z', b'i'];
static S_5_27: [symbol; 4] = [b'a', 0xC8, 0x99, b'i'];
static S_5_28: [symbol; 5] = [b's', b'e', 0xC8, 0x99, b'i'];
static S_5_29: [symbol; 6] = [b'a', b's', b'e', 0xC8, 0x99, b'i'];
static S_5_30: [symbol; 7] = [b's', b'e', b's', b'e', 0xC8, 0x99, b'i'];
static S_5_31: [symbol; 6] = [b'i', b's', b'e', 0xC8, 0x99, b'i'];
static S_5_32: [symbol; 6] = [b'u', b's', b'e', 0xC8, 0x99, b'i'];
static S_5_33: [symbol; 7] = [0xC3, 0xA2, b's', b'e', 0xC8, 0x99, b'i'];
static S_5_34: [symbol; 4] = [b'i', 0xC8, 0x99, b'i'];
static S_5_35: [symbol; 4] = [b'u', 0xC8, 0x99, b'i'];
static S_5_36: [symbol; 5] = [0xC3, 0xA2, 0xC8, 0x99, b'i'];
static S_5_37: [symbol; 4] = [b'a', 0xC8, 0x9B, b'i'];
static S_5_38: [symbol; 5] = [b'e', b'a', 0xC8, 0x9B, b'i'];
static S_5_39: [symbol; 5] = [b'i', b'a', 0xC8, 0x9B, b'i'];
static S_5_40: [symbol; 4] = [b'e', 0xC8, 0x9B, b'i'];
static S_5_41: [symbol; 4] = [b'i', 0xC8, 0x9B, b'i'];
static S_5_42: [symbol; 7] = [b'a', b'r', 0xC4, 0x83, 0xC8, 0x9B, b'i'];
static S_5_43: [symbol; 8] = [b's', b'e', b'r', 0xC4, 0x83, 0xC8, 0x9B, b'i'];
static S_5_44: [symbol; 9] = [b'a', b's', b'e', b'r', 0xC4, 0x83, 0xC8, 0x9B, b'i'];
static S_5_45: [symbol; 10] = [b's', b'e', b's', b'e', b'r', 0xC4, 0x83, 0xC8, 0x9B, b'i'];
static S_5_46: [symbol; 9] = [b'i', b's', b'e', b'r', 0xC4, 0x83, 0xC8, 0x9B, b'i'];
static S_5_47: [symbol; 9] = [b'u', b's', b'e', b'r', 0xC4, 0x83, 0xC8, 0x9B, b'i'];
static S_5_48: [symbol; 10] = [0xC3, 0xA2, b's', b'e', b'r', 0xC4, 0x83, 0xC8, 0x9B, b'i'];
static S_5_49: [symbol; 7] = [b'i', b'r', 0xC4, 0x83, 0xC8, 0x9B, b'i'];
static S_5_50: [symbol; 7] = [b'u', b'r', 0xC4, 0x83, 0xC8, 0x9B, b'i'];
static S_5_51: [symbol; 8] = [0xC3, 0xA2, b'r', 0xC4, 0x83, 0xC8, 0x9B, b'i'];
static S_5_52: [symbol; 5] = [0xC3, 0xA2, 0xC8, 0x9B, b'i'];
static S_5_53: [symbol; 3] = [0xC3, 0xA2, b'i'];
static S_5_54: [symbol; 2] = [b'a', b'm'];
static S_5_55: [symbol; 3] = [b'e', b'a', b'm'];
static S_5_56: [symbol; 3] = [b'i', b'a', b'm'];
static S_5_57: [symbol; 2] = [b'e', b'm'];
static S_5_58: [symbol; 4] = [b'a', b's', b'e', b'm'];
static S_5_59: [symbol; 5] = [b's', b'e', b's', b'e', b'm'];
static S_5_60: [symbol; 4] = [b'i', b's', b'e', b'm'];
static S_5_61: [symbol; 4] = [b'u', b's', b'e', b'm'];
static S_5_62: [symbol; 5] = [0xC3, 0xA2, b's', b'e', b'm'];
static S_5_63: [symbol; 2] = [b'i', b'm'];
static S_5_64: [symbol; 3] = [0xC4, 0x83, b'm'];
static S_5_65: [symbol; 5] = [b'a', b'r', 0xC4, 0x83, b'm'];
static S_5_66: [symbol; 6] = [b's', b'e', b'r', 0xC4, 0x83, b'm'];
static S_5_67: [symbol; 7] = [b'a', b's', b'e', b'r', 0xC4, 0x83, b'm'];
static S_5_68: [symbol; 8] = [b's', b'e', b's', b'e', b'r', 0xC4, 0x83, b'm'];
static S_5_69: [symbol; 7] = [b'i', b's', b'e', b'r', 0xC4, 0x83, b'm'];
static S_5_70: [symbol; 7] = [b'u', b's', b'e', b'r', 0xC4, 0x83, b'm'];
static S_5_71: [symbol; 8] = [0xC3, 0xA2, b's', b'e', b'r', 0xC4, 0x83, b'm'];
static S_5_72: [symbol; 5] = [b'i', b'r', 0xC4, 0x83, b'm'];
static S_5_73: [symbol; 5] = [b'u', b'r', 0xC4, 0x83, b'm'];
static S_5_74: [symbol; 6] = [0xC3, 0xA2, b'r', 0xC4, 0x83, b'm'];
static S_5_75: [symbol; 3] = [0xC3, 0xA2, b'm'];
static S_5_76: [symbol; 2] = [b'a', b'u'];
static S_5_77: [symbol; 3] = [b'e', b'a', b'u'];
static S_5_78: [symbol; 3] = [b'i', b'a', b'u'];
static S_5_79: [symbol; 4] = [b'i', b'n', b'd', b'u'];
static S_5_80: [symbol; 5] = [0xC3, 0xA2, b'n', b'd', b'u'];
static S_5_81: [symbol; 2] = [b'e', b'z'];
static S_5_82: [symbol; 6] = [b'e', b'a', b's', b'c', 0xC4, 0x83];
static S_5_83: [symbol; 4] = [b'a', b'r', 0xC4, 0x83];
static S_5_84: [symbol; 5] = [b's', b'e', b'r', 0xC4, 0x83];
static S_5_85: [symbol; 6] = [b'a', b's', b'e', b'r', 0xC4, 0x83];
static S_5_86: [symbol; 7] = [b's', b'e', b's', b'e', b'r', 0xC4, 0x83];
static S_5_87: [symbol; 6] = [b'i', b's', b'e', b'r', 0xC4, 0x83];
static S_5_88: [symbol; 6] = [b'u', b's', b'e', b'r', 0xC4, 0x83];
static S_5_89: [symbol; 7] = [0xC3, 0xA2, b's', b'e', b'r', 0xC4, 0x83];
static S_5_90: [symbol; 4] = [b'i', b'r', 0xC4, 0x83];
static S_5_91: [symbol; 4] = [b'u', b'r', 0xC4, 0x83];
static S_5_92: [symbol; 5] = [0xC3, 0xA2, b'r', 0xC4, 0x83];
static S_5_93: [symbol; 5] = [b'e', b'a', b'z', 0xC4, 0x83];

static A_5: [among; 94] = [
    among { s_size: 2, s: S_5_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_5_1.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_5_2.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_5_3.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_5_4.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_5_5.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_5_6.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_5_7.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_5_8.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_5_9.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_5_10.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 3, s: S_5_11.as_ptr(), substring_i: 10, result: 1, function: None },
    among { s_size: 4, s: S_5_12.as_ptr(), substring_i: 10, result: 2, function: None },
    among { s_size: 3, s: S_5_13.as_ptr(), substring_i: 10, result: 1, function: None },
    among { s_size: 3, s: S_5_14.as_ptr(), substring_i: 10, result: 1, function: None },
    among { s_size: 4, s: S_5_15.as_ptr(), substring_i: 10, result: 1, function: None },
    among { s_size: 5, s: S_5_16.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_5_17.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_5_18.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_5_19.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_5_20.as_ptr(), substring_i: 19, result: 1, function: None },
    among { s_size: 3, s: S_5_21.as_ptr(), substring_i: 19, result: 1, function: None },
    among { s_size: 3, s: S_5_22.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 5, s: S_5_23.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_5_24.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_5_25.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_5_26.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_5_27.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_5_28.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 6, s: S_5_29.as_ptr(), substring_i: 28, result: 1, function: None },
    among { s_size: 7, s: S_5_30.as_ptr(), substring_i: 28, result: 2, function: None },
    among { s_size: 6, s: S_5_31.as_ptr(), substring_i: 28, result: 1, function: None },
    among { s_size: 6, s: S_5_32.as_ptr(), substring_i: 28, result: 1, function: None },
    among { s_size: 7, s: S_5_33.as_ptr(), substring_i: 28, result: 1, function: None },
    among { s_size: 4, s: S_5_34.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_5_35.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_5_36.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_5_37.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 5, s: S_5_38.as_ptr(), substring_i: 37, result: 1, function: None },
    among { s_size: 5, s: S_5_39.as_ptr(), substring_i: 37, result: 1, function: None },
    among { s_size: 4, s: S_5_40.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 4, s: S_5_41.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 7, s: S_5_42.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_5_43.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 9, s: S_5_44.as_ptr(), substring_i: 43, result: 1, function: None },
    among { s_size: 10, s: S_5_45.as_ptr(), substring_i: 43, result: 2, function: None },
    among { s_size: 9, s: S_5_46.as_ptr(), substring_i: 43, result: 1, function: None },
    among { s_size: 9, s: S_5_47.as_ptr(), substring_i: 43, result: 1, function: None },
    among { s_size: 10, s: S_5_48.as_ptr(), substring_i: 43, result: 1, function: None },
    among { s_size: 7, s: S_5_49.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 7, s: S_5_50.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_5_51.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_5_52.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 3, s: S_5_53.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_5_54.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_5_55.as_ptr(), substring_i: 54, result: 1, function: None },
    among { s_size: 3, s: S_5_56.as_ptr(), substring_i: 54, result: 1, function: None },
    among { s_size: 2, s: S_5_57.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 4, s: S_5_58.as_ptr(), substring_i: 57, result: 1, function: None },
    among { s_size: 5, s: S_5_59.as_ptr(), substring_i: 57, result: 2, function: None },
    among { s_size: 4, s: S_5_60.as_ptr(), substring_i: 57, result: 1, function: None },
    among { s_size: 4, s: S_5_61.as_ptr(), substring_i: 57, result: 1, function: None },
    among { s_size: 5, s: S_5_62.as_ptr(), substring_i: 57, result: 1, function: None },
    among { s_size: 2, s: S_5_63.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 3, s: S_5_64.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 5, s: S_5_65.as_ptr(), substring_i: 64, result: 1, function: None },
    among { s_size: 6, s: S_5_66.as_ptr(), substring_i: 64, result: 2, function: None },
    among { s_size: 7, s: S_5_67.as_ptr(), substring_i: 66, result: 1, function: None },
    among { s_size: 8, s: S_5_68.as_ptr(), substring_i: 66, result: 2, function: None },
    among { s_size: 7, s: S_5_69.as_ptr(), substring_i: 66, result: 1, function: None },
    among { s_size: 7, s: S_5_70.as_ptr(), substring_i: 66, result: 1, function: None },
    among { s_size: 8, s: S_5_71.as_ptr(), substring_i: 66, result: 1, function: None },
    among { s_size: 5, s: S_5_72.as_ptr(), substring_i: 64, result: 1, function: None },
    among { s_size: 5, s: S_5_73.as_ptr(), substring_i: 64, result: 1, function: None },
    among { s_size: 6, s: S_5_74.as_ptr(), substring_i: 64, result: 1, function: None },
    among { s_size: 3, s: S_5_75.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 2, s: S_5_76.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_5_77.as_ptr(), substring_i: 76, result: 1, function: None },
    among { s_size: 3, s: S_5_78.as_ptr(), substring_i: 76, result: 1, function: None },
    among { s_size: 4, s: S_5_79.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_5_80.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_5_81.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_5_82.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_5_83.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_5_84.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 6, s: S_5_85.as_ptr(), substring_i: 84, result: 1, function: None },
    among { s_size: 7, s: S_5_86.as_ptr(), substring_i: 84, result: 2, function: None },
    among { s_size: 6, s: S_5_87.as_ptr(), substring_i: 84, result: 1, function: None },
    among { s_size: 6, s: S_5_88.as_ptr(), substring_i: 84, result: 1, function: None },
    among { s_size: 7, s: S_5_89.as_ptr(), substring_i: 84, result: 1, function: None },
    among { s_size: 4, s: S_5_90.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_5_91.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_5_92.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_5_93.as_ptr(), substring_i: -1, result: 1, function: None },
];

static S_6_0: [symbol; 1] = [b'a'];
static S_6_1: [symbol; 1] = [b'e'];
static S_6_2: [symbol; 2] = [b'i', b'e'];
static S_6_3: [symbol; 1] = [b'i'];
static S_6_4: [symbol; 2] = [0xC4, 0x83];

static A_6: [among; 5] = [
    among { s_size: 1, s: S_6_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 1, s: S_6_1.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_6_2.as_ptr(), substring_i: 1, result: 1, function: None },
    among { s_size: 1, s: S_6_3.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_6_4.as_ptr(), substring_i: -1, result: 1, function: None },
];

// ---------------------------------------------------------------------------
// grouping bit tables
// ---------------------------------------------------------------------------

static G_V: [c_uchar; 21] = [
    17, 65, 16, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 32, 0, 0, 4,
];

// ---------------------------------------------------------------------------
// literal strings used by slice_from_s / eq_s_b
// ---------------------------------------------------------------------------

static S_0: [symbol; 2] = [0xC8, 0x99];
static S_1: [symbol; 2] = [0xC8, 0x9B];
static S_2: [symbol; 1] = [b'U'];
static S_3: [symbol; 1] = [b'I'];
static S_4: [symbol; 1] = [b'i'];
static S_5: [symbol; 1] = [b'u'];
static S_6: [symbol; 1] = [b'a'];
static S_7: [symbol; 1] = [b'e'];
static S_8: [symbol; 1] = [b'i'];
static S_9: [symbol; 2] = [b'a', b'b'];
static S_10: [symbol; 1] = [b'i'];
static S_11: [symbol; 2] = [b'a', b't'];
static S_12: [symbol; 4] = [b'a', 0xC8, 0x9B, b'i'];
static S_13: [symbol; 4] = [b'a', b'b', b'i', b'l'];
static S_14: [symbol; 4] = [b'i', b'b', b'i', b'l'];
static S_15: [symbol; 2] = [b'i', b'v'];
static S_16: [symbol; 2] = [b'i', b'c'];
static S_17: [symbol; 2] = [b'a', b't'];
static S_18: [symbol; 2] = [b'i', b't'];
static S_19: [symbol; 2] = [0xC8, 0x9B];
static S_20: [symbol; 1] = [b't'];
static S_21: [symbol; 3] = [b'i', b's', b't'];

// ---------------------------------------------------------------------------
// rules
// ---------------------------------------------------------------------------

unsafe fn r_norm(z: *mut SN_env) -> c_int {
    let mut among_var;
    {
        let c1 = (*z).c;
        'rep0: loop {
            let c2 = (*z).c;
            'lab1: {
                'zrom14: loop {
                    let c3 = (*z).c;
                    'lab2: {
                        (*z).bra = (*z).c;
                        if (*z).c + 1 >= (*z).l
                            || (*(*z).p.offset(((*z).c + 1) as isize) != 159
                                && *(*z).p.offset(((*z).c + 1) as isize) != 163)
                        {
                            break 'lab2;
                        }
                        among_var = find_among(z, A_0.as_ptr(), 2);
                        if among_var == 0 {
                            break 'lab2;
                        }
                        (*z).ket = (*z).c;
                        match among_var {
                            1 => {
                                let ret = slice_from_s(z, 2, S_0.as_ptr());
                                if ret < 0 {
                                    return ret;
                                }
                            }
                            2 => {
                                let ret = slice_from_s(z, 2, S_1.as_ptr());
                                if ret < 0 {
                                    return ret;
                                }
                            }
                            _ => {}
                        }
                        (*z).c = c3;
                        break 'zrom14;
                    }
                    // lab2:
                    (*z).c = c3;
                    {
                        let ret = skip_utf8((*z).p, (*z).c, (*z).l, 1);
                        if ret < 0 {
                            break 'lab1;
                        }
                        (*z).c = ret;
                    }
                }
                continue 'rep0;
            }
            // lab1:
            (*z).c = c2;
            break 'rep0;
        }
        (*z).c = c1;
    }
    1
}

unsafe fn r_prelude(z: *mut SN_env) -> c_int {
    'rep0: loop {
        let c1 = (*z).c;
        let c2 = (*z).c;
        'lab1: {
            'lab2: {
                if in_grouping_U(z, G_V.as_ptr(), 97, 259, 0) != 0 {
                    break 'lab1;
                }
                (*z).bra = (*z).c;
                {
                    let c3 = (*z).c;
                    'lab3: {
                        if (*z).c == (*z).l || *(*z).p.offset((*z).c as isize) != b'u' {
                            break 'lab3;
                        }
                        (*z).c += 1;
                        (*z).ket = (*z).c;
                        if in_grouping_U(z, G_V.as_ptr(), 97, 259, 0) != 0 {
                            break 'lab3;
                        }
                        {
                            let ret = slice_from_s(z, 1, S_2.as_ptr());
                            if ret < 0 {
                                return ret;
                            }
                        }
                        break 'lab2;
                    }
                    // lab3:
                    (*z).c = c3;
                    if (*z).c == (*z).l || *(*z).p.offset((*z).c as isize) != b'i' {
                        break 'lab1;
                    }
                    (*z).c += 1;
                    (*z).ket = (*z).c;
                    if in_grouping_U(z, G_V.as_ptr(), 97, 259, 0) != 0 {
                        break 'lab1;
                    }
                    {
                        let ret = slice_from_s(z, 1, S_3.as_ptr());
                        if ret < 0 {
                            return ret;
                        }
                    }
                }
            }
            // lab2:
            (*z).c = c2;
            (*z).c = c1;
            break 'rep0;
        }
        // lab1:
        (*z).c = c2;
        {
            let ret = skip_utf8((*z).p, (*z).c, (*z).l, 1);
            if ret < 0 {
                (*z).c = c1;
                break 'rep0;
            }
            (*z).c = ret;
        }
    }
    1
}

unsafe fn r_mark_regions(z: *mut SN_env) -> c_int {
    *(*z).I.offset(2) = (*z).l;
    *(*z).I.offset(1) = (*z).l;
    *(*z).I.offset(0) = (*z).l;
    {
        let c1 = (*z).c;
        'lab0: {
            let c2 = (*z).c;
            'lab1: {
                'lab2: {
                    if in_grouping_U(z, G_V.as_ptr(), 97, 259, 0) != 0 {
                        break 'lab2;
                    }
                    {
                        let c3 = (*z).c;
                        'lab3: {
                            'lab4: {
                                if out_grouping_U(z, G_V.as_ptr(), 97, 259, 0) != 0 {
                                    break 'lab4;
                                }
                                {
                                    let ret = out_grouping_U(z, G_V.as_ptr(), 97, 259, 1);
                                    if ret < 0 {
                                        break 'lab4;
                                    }
                                    (*z).c += ret;
                                }
                                break 'lab3;
                            }
                            // lab4:
                            (*z).c = c3;
                            if in_grouping_U(z, G_V.as_ptr(), 97, 259, 0) != 0 {
                                break 'lab2;
                            }
                            {
                                let ret = in_grouping_U(z, G_V.as_ptr(), 97, 259, 1);
                                if ret < 0 {
                                    break 'lab2;
                                }
                                (*z).c += ret;
                            }
                        }
                    }
                    // lab3:
                    break 'lab1;
                }
                // lab2:
                (*z).c = c2;
                if out_grouping_U(z, G_V.as_ptr(), 97, 259, 0) != 0 {
                    break 'lab0;
                }
                {
                    let c4 = (*z).c;
                    'lab5: {
                        'lab6: {
                            if out_grouping_U(z, G_V.as_ptr(), 97, 259, 0) != 0 {
                                break 'lab6;
                            }
                            {
                                let ret = out_grouping_U(z, G_V.as_ptr(), 97, 259, 1);
                                if ret < 0 {
                                    break 'lab6;
                                }
                                (*z).c += ret;
                            }
                            break 'lab5;
                        }
                        // lab6:
                        (*z).c = c4;
                        if in_grouping_U(z, G_V.as_ptr(), 97, 259, 0) != 0 {
                            break 'lab0;
                        }
                        {
                            let ret = skip_utf8((*z).p, (*z).c, (*z).l, 1);
                            if ret < 0 {
                                break 'lab0;
                            }
                            (*z).c = ret;
                        }
                    }
                    // lab5:
                }
            }
            // lab1:
            *(*z).I.offset(2) = (*z).c;
        }
        // lab0:
        (*z).c = c1;
    }
    {
        let c5 = (*z).c;
        'lab7: {
            {
                let ret = out_grouping_U(z, G_V.as_ptr(), 97, 259, 1);
                if ret < 0 {
                    break 'lab7;
                }
                (*z).c += ret;
            }
            {
                let ret = in_grouping_U(z, G_V.as_ptr(), 97, 259, 1);
                if ret < 0 {
                    break 'lab7;
                }
                (*z).c += ret;
            }
            *(*z).I.offset(1) = (*z).c;
            {
                let ret = out_grouping_U(z, G_V.as_ptr(), 97, 259, 1);
                if ret < 0 {
                    break 'lab7;
                }
                (*z).c += ret;
            }
            {
                let ret = in_grouping_U(z, G_V.as_ptr(), 97, 259, 1);
                if ret < 0 {
                    break 'lab7;
                }
                (*z).c += ret;
            }
            *(*z).I.offset(0) = (*z).c;
        }
        // lab7:
        (*z).c = c5;
    }
    1
}

unsafe fn r_postlude(z: *mut SN_env) -> c_int {
    let mut among_var;
    'rep0: loop {
        let c1 = (*z).c;
        (*z).bra = (*z).c;
        if (*z).c >= (*z).l
            || (*(*z).p.offset(((*z).c + 0) as isize) != 73
                && *(*z).p.offset(((*z).c + 0) as isize) != 85)
        {
            among_var = 3;
        } else {
            among_var = find_among(z, A_1.as_ptr(), 3);
        }
        (*z).ket = (*z).c;
        'lab0: {
            match among_var {
                1 => {
                    let ret = slice_from_s(z, 1, S_4.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
                2 => {
                    let ret = slice_from_s(z, 1, S_5.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
                3 => {
                    let ret = skip_utf8((*z).p, (*z).c, (*z).l, 1);
                    if ret < 0 {
                        break 'lab0;
                    }
                    (*z).c = ret;
                }
                _ => {}
            }
            continue 'rep0;
        }
        // lab0:
        (*z).c = c1;
        break 'rep0;
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

unsafe fn r_step_0(z: *mut SN_env) -> c_int {
    let among_var;
    (*z).ket = (*z).c;
    if (*z).c - 1 <= (*z).lb
        || *(*z).p.offset(((*z).c - 1) as isize) as c_int >> 5 != 3
        || (266786 >> (*(*z).p.offset(((*z).c - 1) as isize) as c_int & 0x1f)) & 1 == 0
    {
        return 0;
    }
    among_var = find_among_b(z, A_2.as_ptr(), 16);
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
            let ret = slice_del(z);
            if ret < 0 {
                return ret;
            }
        }
        2 => {
            let ret = slice_from_s(z, 1, S_6.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        3 => {
            let ret = slice_from_s(z, 1, S_7.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        4 => {
            let ret = slice_from_s(z, 1, S_8.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        5 => {
            {
                let m1 = (*z).l - (*z).c;
                'lab0: {
                    if eq_s_b(z, 2, S_9.as_ptr()) == 0 {
                        break 'lab0;
                    }
                    return 0;
                }
                // lab0:
                (*z).c = (*z).l - m1;
            }
            {
                let ret = slice_from_s(z, 1, S_10.as_ptr());
                if ret < 0 {
                    return ret;
                }
            }
        }
        6 => {
            let ret = slice_from_s(z, 2, S_11.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        7 => {
            let ret = slice_from_s(z, 4, S_12.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        _ => {}
    }
    1
}

unsafe fn r_combo_suffix(z: *mut SN_env) -> c_int {
    let among_var;
    {
        let m_test1 = (*z).l - (*z).c;
        (*z).ket = (*z).c;
        among_var = find_among_b(z, A_3.as_ptr(), 46);
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
                let ret = slice_from_s(z, 4, S_13.as_ptr());
                if ret < 0 {
                    return ret;
                }
            }
            2 => {
                let ret = slice_from_s(z, 4, S_14.as_ptr());
                if ret < 0 {
                    return ret;
                }
            }
            3 => {
                let ret = slice_from_s(z, 2, S_15.as_ptr());
                if ret < 0 {
                    return ret;
                }
            }
            4 => {
                let ret = slice_from_s(z, 2, S_16.as_ptr());
                if ret < 0 {
                    return ret;
                }
            }
            5 => {
                let ret = slice_from_s(z, 2, S_17.as_ptr());
                if ret < 0 {
                    return ret;
                }
            }
            6 => {
                let ret = slice_from_s(z, 2, S_18.as_ptr());
                if ret < 0 {
                    return ret;
                }
            }
            _ => {}
        }
        *(*z).I.offset(3) = 1;
        (*z).c = (*z).l - m_test1;
    }
    1
}

unsafe fn r_standard_suffix(z: *mut SN_env) -> c_int {
    let among_var;
    *(*z).I.offset(3) = 0;
    'rep0: loop {
        let m1 = (*z).l - (*z).c;
        'lab0: {
            {
                let ret = r_combo_suffix(z);
                if ret == 0 {
                    break 'lab0;
                }
                if ret < 0 {
                    return ret;
                }
            }
            continue 'rep0;
        }
        // lab0:
        (*z).c = (*z).l - m1;
        break 'rep0;
    }
    (*z).ket = (*z).c;
    among_var = find_among_b(z, A_4.as_ptr(), 62);
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
            if eq_s_b(z, 2, S_19.as_ptr()) == 0 {
                return 0;
            }
            (*z).bra = (*z).c;
            {
                let ret = slice_from_s(z, 1, S_20.as_ptr());
                if ret < 0 {
                    return ret;
                }
            }
        }
        3 => {
            let ret = slice_from_s(z, 3, S_21.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        _ => {}
    }
    *(*z).I.offset(3) = 1;
    1
}

unsafe fn r_verb_suffix(z: *mut SN_env) -> c_int {
    let among_var;
    {
        let mlimit1;
        if (*z).c < *(*z).I.offset(2) {
            return 0;
        }
        mlimit1 = (*z).lb;
        (*z).lb = *(*z).I.offset(2);
        (*z).ket = (*z).c;
        among_var = find_among_b(z, A_5.as_ptr(), 94);
        if among_var == 0 {
            (*z).lb = mlimit1;
            return 0;
        }
        (*z).bra = (*z).c;
        match among_var {
            1 => {
                'lab0: {
                    let m2 = (*z).l - (*z).c;
                    'lab1: {
                        if out_grouping_b_U(z, G_V.as_ptr(), 97, 259, 0) != 0 {
                            break 'lab1;
                        }
                        break 'lab0;
                    }
                    // lab1:
                    (*z).c = (*z).l - m2;
                    if (*z).c <= (*z).lb || *(*z).p.offset(((*z).c - 1) as isize) != b'u' {
                        (*z).lb = mlimit1;
                        return 0;
                    }
                    (*z).c -= 1;
                }
                // lab0:
                {
                    let ret = slice_del(z);
                    if ret < 0 {
                        return ret;
                    }
                }
            }
            2 => {
                let ret = slice_del(z);
                if ret < 0 {
                    return ret;
                }
            }
            _ => {}
        }
        (*z).lb = mlimit1;
    }
    1
}

unsafe fn r_vowel_suffix(z: *mut SN_env) -> c_int {
    (*z).ket = (*z).c;
    if find_among_b(z, A_6.as_ptr(), 5) == 0 {
        return 0;
    }
    (*z).bra = (*z).c;
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
    1
}

// ---------------------------------------------------------------------------
// exported entry points
// ---------------------------------------------------------------------------

#[no_mangle]
pub unsafe extern "C" fn romanian_UTF_8_stem(z: *mut SN_env) -> c_int {
    {
        let ret = r_norm(z);
        if ret < 0 {
            return ret;
        }
    }
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
        let ret = r_mark_regions(z);
        if ret < 0 {
            return ret;
        }
    }
    (*z).lb = (*z).c;
    (*z).c = (*z).l;

    {
        let m2 = (*z).l - (*z).c;
        {
            let ret = r_step_0(z);
            if ret < 0 {
                return ret;
            }
        }
        (*z).c = (*z).l - m2;
    }
    {
        let m3 = (*z).l - (*z).c;
        {
            let ret = r_standard_suffix(z);
            if ret < 0 {
                return ret;
            }
        }
        (*z).c = (*z).l - m3;
    }
    {
        let m4 = (*z).l - (*z).c;
        'lab0: {
            'lab1: {
                let m5 = (*z).l - (*z).c;
                'lab2: {
                    if *(*z).I.offset(3) == 0 {
                        break 'lab2;
                    }
                    break 'lab1;
                }
                // lab2:
                (*z).c = (*z).l - m5;
                {
                    let ret = r_verb_suffix(z);
                    if ret == 0 {
                        break 'lab0;
                    }
                    if ret < 0 {
                        return ret;
                    }
                }
            }
            // lab1:
        }
        // lab0:
        (*z).c = (*z).l - m4;
    }
    {
        let m6 = (*z).l - (*z).c;
        {
            let ret = r_vowel_suffix(z);
            if ret < 0 {
                return ret;
            }
        }
        (*z).c = (*z).l - m6;
    }
    (*z).c = (*z).lb;
    {
        let c7 = (*z).c;
        {
            let ret = r_postlude(z);
            if ret < 0 {
                return ret;
            }
        }
        (*z).c = c7;
    }
    1
}

#[no_mangle]
pub unsafe extern "C" fn romanian_UTF_8_create_env() -> *mut SN_env {
    SN_create_env(0, 4)
}

#[no_mangle]
pub unsafe extern "C" fn romanian_UTF_8_close_env(z: *mut SN_env) {
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
        let z = romanian_UTF_8_create_env();
        assert!(!z.is_null());
        let rc = SN_set_current(z, word.len() as c_int, word.as_ptr());
        assert!(rc >= 0);
        let rc = romanian_UTF_8_stem(z);
        assert!(rc >= 0, "stem returned {rc}");
        let out = std::slice::from_raw_parts((*z).p, SIZE((*z).p) as usize).to_vec();
        romanian_UTF_8_close_env(z);
        out
    }

    // Convergence: repeatedly stemming reaches a fixed point that is stable and
    // non-empty (the suffix strippers are not necessarily idempotent in one pass).
    #[test]
    fn converges() {
        unsafe {
            for w in [
                &b"abilitate"[..],
                &b"icala"[..],
                &b"national"[..],
            ] {
                let mut cur = stem(w);
                for _ in 0..8 {
                    let next = stem(&cur);
                    if next == cur {
                        break;
                    }
                    cur = next;
                }
                let stable = stem(&cur);
                assert_eq!(stable, cur, "did not converge for {:?}", w);
                assert!(!cur.is_empty());
            }
        }
    }

    // A suffix collapses; result must be non-empty and not longer than input.
    #[test]
    fn suffix_stripped_nonempty() {
        unsafe {
            let r = stem(b"icala");
            assert!(!r.is_empty());
            assert!(r.len() <= "icala".len());
        }
    }
}
