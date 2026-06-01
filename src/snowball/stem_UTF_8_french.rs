//! French Snowball stemmer (UTF-8).
//!
//! 1:1 translation of the generated
//! `src/backend/snowball/libstemmer/stem_UTF_8_french.c` (Snowball 2.2.0),
//! merged with its header declarations. The runtime helpers come from
//! `crate::snowball::api` and `crate::snowball::utilities`.

use crate::prelude::*;

use crate::snowball::api::{among, symbol, SN_close_env, SN_create_env, SN_env, SIZE};
use crate::snowball::utilities::{
    eq_s, eq_s_b, find_among, find_among_b, in_grouping_U, in_grouping_b_U, out_grouping_U,
    out_grouping_b_U, skip_b_utf8, skip_utf8, slice_del, slice_from_s,
};

// ---------------------------------------------------------------------------
// among string tables
// ---------------------------------------------------------------------------

static S_0_0: [symbol; 3] = [b'c', b'o', b'l'];
static S_0_1: [symbol; 3] = [b'p', b'a', b'r'];
static S_0_2: [symbol; 3] = [b't', b'a', b'p'];

static A_0: [among; 3] = [
    among { s_size: 3, s: S_0_0.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_0_1.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_0_2.as_ptr(), substring_i: -1, result: -1, function: None },
];

static S_1_1: [symbol; 1] = [b'H'];
static S_1_2: [symbol; 2] = [b'H', b'e'];
static S_1_3: [symbol; 2] = [b'H', b'i'];
static S_1_4: [symbol; 1] = [b'I'];
static S_1_5: [symbol; 1] = [b'U'];
static S_1_6: [symbol; 1] = [b'Y'];

static A_1: [among; 7] = [
    among { s_size: 0, s: core::ptr::null(), substring_i: -1, result: 7, function: None },
    among { s_size: 1, s: S_1_1.as_ptr(), substring_i: 0, result: 6, function: None },
    among { s_size: 2, s: S_1_2.as_ptr(), substring_i: 1, result: 4, function: None },
    among { s_size: 2, s: S_1_3.as_ptr(), substring_i: 1, result: 5, function: None },
    among { s_size: 1, s: S_1_4.as_ptr(), substring_i: 0, result: 1, function: None },
    among { s_size: 1, s: S_1_5.as_ptr(), substring_i: 0, result: 2, function: None },
    among { s_size: 1, s: S_1_6.as_ptr(), substring_i: 0, result: 3, function: None },
];

static S_2_0: [symbol; 3] = [b'i', b'q', b'U'];
static S_2_1: [symbol; 3] = [b'a', b'b', b'l'];
static S_2_2: [symbol; 4] = [b'I', 0xC3, 0xA8, b'r'];
static S_2_3: [symbol; 4] = [b'i', 0xC3, 0xA8, b'r'];
static S_2_4: [symbol; 3] = [b'e', b'u', b's'];
static S_2_5: [symbol; 2] = [b'i', b'v'];

static A_2: [among; 6] = [
    among { s_size: 3, s: S_2_0.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 3, s: S_2_1.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 4, s: S_2_2.as_ptr(), substring_i: -1, result: 4, function: None },
    among { s_size: 4, s: S_2_3.as_ptr(), substring_i: -1, result: 4, function: None },
    among { s_size: 3, s: S_2_4.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 2, s: S_2_5.as_ptr(), substring_i: -1, result: 1, function: None },
];

static S_3_0: [symbol; 2] = [b'i', b'c'];
static S_3_1: [symbol; 4] = [b'a', b'b', b'i', b'l'];
static S_3_2: [symbol; 2] = [b'i', b'v'];

static A_3: [among; 3] = [
    among { s_size: 2, s: S_3_0.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 4, s: S_3_1.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_3_2.as_ptr(), substring_i: -1, result: 3, function: None },
];

static S_4_0: [symbol; 4] = [b'i', b'q', b'U', b'e'];
static S_4_1: [symbol; 6] = [b'a', b't', b'r', b'i', b'c', b'e'];
static S_4_2: [symbol; 4] = [b'a', b'n', b'c', b'e'];
static S_4_3: [symbol; 4] = [b'e', b'n', b'c', b'e'];
static S_4_4: [symbol; 5] = [b'l', b'o', b'g', b'i', b'e'];
static S_4_5: [symbol; 4] = [b'a', b'b', b'l', b'e'];
static S_4_6: [symbol; 4] = [b'i', b's', b'm', b'e'];
static S_4_7: [symbol; 4] = [b'e', b'u', b's', b'e'];
static S_4_8: [symbol; 4] = [b'i', b's', b't', b'e'];
static S_4_9: [symbol; 3] = [b'i', b'v', b'e'];
static S_4_10: [symbol; 2] = [b'i', b'f'];
static S_4_11: [symbol; 5] = [b'u', b's', b'i', b'o', b'n'];
static S_4_12: [symbol; 5] = [b'a', b't', b'i', b'o', b'n'];
static S_4_13: [symbol; 5] = [b'u', b't', b'i', b'o', b'n'];
static S_4_14: [symbol; 5] = [b'a', b't', b'e', b'u', b'r'];
static S_4_15: [symbol; 5] = [b'i', b'q', b'U', b'e', b's'];
static S_4_16: [symbol; 7] = [b'a', b't', b'r', b'i', b'c', b'e', b's'];
static S_4_17: [symbol; 5] = [b'a', b'n', b'c', b'e', b's'];
static S_4_18: [symbol; 5] = [b'e', b'n', b'c', b'e', b's'];
static S_4_19: [symbol; 6] = [b'l', b'o', b'g', b'i', b'e', b's'];
static S_4_20: [symbol; 5] = [b'a', b'b', b'l', b'e', b's'];
static S_4_21: [symbol; 5] = [b'i', b's', b'm', b'e', b's'];
static S_4_22: [symbol; 5] = [b'e', b'u', b's', b'e', b's'];
static S_4_23: [symbol; 5] = [b'i', b's', b't', b'e', b's'];
static S_4_24: [symbol; 4] = [b'i', b'v', b'e', b's'];
static S_4_25: [symbol; 3] = [b'i', b'f', b's'];
static S_4_26: [symbol; 6] = [b'u', b's', b'i', b'o', b'n', b's'];
static S_4_27: [symbol; 6] = [b'a', b't', b'i', b'o', b'n', b's'];
static S_4_28: [symbol; 6] = [b'u', b't', b'i', b'o', b'n', b's'];
static S_4_29: [symbol; 6] = [b'a', b't', b'e', b'u', b'r', b's'];
static S_4_30: [symbol; 5] = [b'm', b'e', b'n', b't', b's'];
static S_4_31: [symbol; 6] = [b'e', b'm', b'e', b'n', b't', b's'];
static S_4_32: [symbol; 9] = [b'i', b's', b's', b'e', b'm', b'e', b'n', b't', b's'];
static S_4_33: [symbol; 5] = [b'i', b't', 0xC3, 0xA9, b's'];
static S_4_34: [symbol; 4] = [b'm', b'e', b'n', b't'];
static S_4_35: [symbol; 5] = [b'e', b'm', b'e', b'n', b't'];
static S_4_36: [symbol; 8] = [b'i', b's', b's', b'e', b'm', b'e', b'n', b't'];
static S_4_37: [symbol; 6] = [b'a', b'm', b'm', b'e', b'n', b't'];
static S_4_38: [symbol; 6] = [b'e', b'm', b'm', b'e', b'n', b't'];
static S_4_39: [symbol; 3] = [b'a', b'u', b'x'];
static S_4_40: [symbol; 4] = [b'e', b'a', b'u', b'x'];
static S_4_41: [symbol; 3] = [b'e', b'u', b'x'];
static S_4_42: [symbol; 4] = [b'i', b't', 0xC3, 0xA9];

static A_4: [among; 43] = [
    among { s_size: 4, s: S_4_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_4_1.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 4, s: S_4_2.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_4_3.as_ptr(), substring_i: -1, result: 5, function: None },
    among { s_size: 5, s: S_4_4.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 4, s: S_4_5.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_4_6.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_4_7.as_ptr(), substring_i: -1, result: 11, function: None },
    among { s_size: 4, s: S_4_8.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_4_9.as_ptr(), substring_i: -1, result: 8, function: None },
    among { s_size: 2, s: S_4_10.as_ptr(), substring_i: -1, result: 8, function: None },
    among { s_size: 5, s: S_4_11.as_ptr(), substring_i: -1, result: 4, function: None },
    among { s_size: 5, s: S_4_12.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 5, s: S_4_13.as_ptr(), substring_i: -1, result: 4, function: None },
    among { s_size: 5, s: S_4_14.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 5, s: S_4_15.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 7, s: S_4_16.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 5, s: S_4_17.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_4_18.as_ptr(), substring_i: -1, result: 5, function: None },
    among { s_size: 6, s: S_4_19.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 5, s: S_4_20.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_4_21.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_4_22.as_ptr(), substring_i: -1, result: 11, function: None },
    among { s_size: 5, s: S_4_23.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_4_24.as_ptr(), substring_i: -1, result: 8, function: None },
    among { s_size: 3, s: S_4_25.as_ptr(), substring_i: -1, result: 8, function: None },
    among { s_size: 6, s: S_4_26.as_ptr(), substring_i: -1, result: 4, function: None },
    among { s_size: 6, s: S_4_27.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 6, s: S_4_28.as_ptr(), substring_i: -1, result: 4, function: None },
    among { s_size: 6, s: S_4_29.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 5, s: S_4_30.as_ptr(), substring_i: -1, result: 15, function: None },
    among { s_size: 6, s: S_4_31.as_ptr(), substring_i: 30, result: 6, function: None },
    among { s_size: 9, s: S_4_32.as_ptr(), substring_i: 31, result: 12, function: None },
    among { s_size: 5, s: S_4_33.as_ptr(), substring_i: -1, result: 7, function: None },
    among { s_size: 4, s: S_4_34.as_ptr(), substring_i: -1, result: 15, function: None },
    among { s_size: 5, s: S_4_35.as_ptr(), substring_i: 34, result: 6, function: None },
    among { s_size: 8, s: S_4_36.as_ptr(), substring_i: 35, result: 12, function: None },
    among { s_size: 6, s: S_4_37.as_ptr(), substring_i: 34, result: 13, function: None },
    among { s_size: 6, s: S_4_38.as_ptr(), substring_i: 34, result: 14, function: None },
    among { s_size: 3, s: S_4_39.as_ptr(), substring_i: -1, result: 10, function: None },
    among { s_size: 4, s: S_4_40.as_ptr(), substring_i: 39, result: 9, function: None },
    among { s_size: 3, s: S_4_41.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_4_42.as_ptr(), substring_i: -1, result: 7, function: None },
];

static S_5_0: [symbol; 3] = [b'i', b'r', b'a'];
static S_5_1: [symbol; 2] = [b'i', b'e'];
static S_5_2: [symbol; 4] = [b'i', b's', b's', b'e'];
static S_5_3: [symbol; 7] = [b'i', b's', b's', b'a', b'n', b't', b'e'];
static S_5_4: [symbol; 1] = [b'i'];
static S_5_5: [symbol; 4] = [b'i', b'r', b'a', b'i'];
static S_5_6: [symbol; 2] = [b'i', b'r'];
static S_5_7: [symbol; 4] = [b'i', b'r', b'a', b's'];
static S_5_8: [symbol; 3] = [b'i', b'e', b's'];
static S_5_9: [symbol; 5] = [0xC3, 0xAE, b'm', b'e', b's'];
static S_5_10: [symbol; 5] = [b'i', b's', b's', b'e', b's'];
static S_5_11: [symbol; 8] = [b'i', b's', b's', b'a', b'n', b't', b'e', b's'];
static S_5_12: [symbol; 5] = [0xC3, 0xAE, b't', b'e', b's'];
static S_5_13: [symbol; 2] = [b'i', b's'];
static S_5_14: [symbol; 5] = [b'i', b'r', b'a', b'i', b's'];
static S_5_15: [symbol; 6] = [b'i', b's', b's', b'a', b'i', b's'];
static S_5_16: [symbol; 6] = [b'i', b'r', b'i', b'o', b'n', b's'];
static S_5_17: [symbol; 7] = [b'i', b's', b's', b'i', b'o', b'n', b's'];
static S_5_18: [symbol; 5] = [b'i', b'r', b'o', b'n', b's'];
static S_5_19: [symbol; 6] = [b'i', b's', b's', b'o', b'n', b's'];
static S_5_20: [symbol; 7] = [b'i', b's', b's', b'a', b'n', b't', b's'];
static S_5_21: [symbol; 2] = [b'i', b't'];
static S_5_22: [symbol; 5] = [b'i', b'r', b'a', b'i', b't'];
static S_5_23: [symbol; 6] = [b'i', b's', b's', b'a', b'i', b't'];
static S_5_24: [symbol; 6] = [b'i', b's', b's', b'a', b'n', b't'];
static S_5_25: [symbol; 7] = [b'i', b'r', b'a', b'I', b'e', b'n', b't'];
static S_5_26: [symbol; 8] = [b'i', b's', b's', b'a', b'I', b'e', b'n', b't'];
static S_5_27: [symbol; 5] = [b'i', b'r', b'e', b'n', b't'];
static S_5_28: [symbol; 6] = [b'i', b's', b's', b'e', b'n', b't'];
static S_5_29: [symbol; 5] = [b'i', b'r', b'o', b'n', b't'];
static S_5_30: [symbol; 3] = [0xC3, 0xAE, b't'];
static S_5_31: [symbol; 5] = [b'i', b'r', b'i', b'e', b'z'];
static S_5_32: [symbol; 6] = [b'i', b's', b's', b'i', b'e', b'z'];
static S_5_33: [symbol; 4] = [b'i', b'r', b'e', b'z'];
static S_5_34: [symbol; 5] = [b'i', b's', b's', b'e', b'z'];

static A_5: [among; 35] = [
    among { s_size: 3, s: S_5_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_5_1.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_5_2.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 7, s: S_5_3.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 1, s: S_5_4.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_5_5.as_ptr(), substring_i: 4, result: 1, function: None },
    among { s_size: 2, s: S_5_6.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_5_7.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_5_8.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_5_9.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_5_10.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_5_11.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_5_12.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_5_13.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_5_14.as_ptr(), substring_i: 13, result: 1, function: None },
    among { s_size: 6, s: S_5_15.as_ptr(), substring_i: 13, result: 1, function: None },
    among { s_size: 6, s: S_5_16.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 7, s: S_5_17.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_5_18.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_5_19.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 7, s: S_5_20.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_5_21.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_5_22.as_ptr(), substring_i: 21, result: 1, function: None },
    among { s_size: 6, s: S_5_23.as_ptr(), substring_i: 21, result: 1, function: None },
    among { s_size: 6, s: S_5_24.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 7, s: S_5_25.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_5_26.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_5_27.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_5_28.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_5_29.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_5_30.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_5_31.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_5_32.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_5_33.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_5_34.as_ptr(), substring_i: -1, result: 1, function: None },
];

static S_6_0: [symbol; 1] = [b'a'];
static S_6_1: [symbol; 3] = [b'e', b'r', b'a'];
static S_6_2: [symbol; 4] = [b'a', b's', b's', b'e'];
static S_6_3: [symbol; 4] = [b'a', b'n', b't', b'e'];
static S_6_4: [symbol; 3] = [0xC3, 0xA9, b'e'];
static S_6_5: [symbol; 2] = [b'a', b'i'];
static S_6_6: [symbol; 4] = [b'e', b'r', b'a', b'i'];
static S_6_7: [symbol; 2] = [b'e', b'r'];
static S_6_8: [symbol; 2] = [b'a', b's'];
static S_6_9: [symbol; 4] = [b'e', b'r', b'a', b's'];
static S_6_10: [symbol; 5] = [0xC3, 0xA2, b'm', b'e', b's'];
static S_6_11: [symbol; 5] = [b'a', b's', b's', b'e', b's'];
static S_6_12: [symbol; 5] = [b'a', b'n', b't', b'e', b's'];
static S_6_13: [symbol; 5] = [0xC3, 0xA2, b't', b'e', b's'];
static S_6_14: [symbol; 4] = [0xC3, 0xA9, b'e', b's'];
static S_6_15: [symbol; 3] = [b'a', b'i', b's'];
static S_6_16: [symbol; 5] = [b'e', b'r', b'a', b'i', b's'];
static S_6_17: [symbol; 4] = [b'i', b'o', b'n', b's'];
static S_6_18: [symbol; 6] = [b'e', b'r', b'i', b'o', b'n', b's'];
static S_6_19: [symbol; 7] = [b'a', b's', b's', b'i', b'o', b'n', b's'];
static S_6_20: [symbol; 5] = [b'e', b'r', b'o', b'n', b's'];
static S_6_21: [symbol; 4] = [b'a', b'n', b't', b's'];
static S_6_22: [symbol; 3] = [0xC3, 0xA9, b's'];
static S_6_23: [symbol; 3] = [b'a', b'i', b't'];
static S_6_24: [symbol; 5] = [b'e', b'r', b'a', b'i', b't'];
static S_6_25: [symbol; 3] = [b'a', b'n', b't'];
static S_6_26: [symbol; 5] = [b'a', b'I', b'e', b'n', b't'];
static S_6_27: [symbol; 7] = [b'e', b'r', b'a', b'I', b'e', b'n', b't'];
static S_6_28: [symbol; 6] = [0xC3, 0xA8, b'r', b'e', b'n', b't'];
static S_6_29: [symbol; 6] = [b'a', b's', b's', b'e', b'n', b't'];
static S_6_30: [symbol; 5] = [b'e', b'r', b'o', b'n', b't'];
static S_6_31: [symbol; 3] = [0xC3, 0xA2, b't'];
static S_6_32: [symbol; 2] = [b'e', b'z'];
static S_6_33: [symbol; 3] = [b'i', b'e', b'z'];
static S_6_34: [symbol; 5] = [b'e', b'r', b'i', b'e', b'z'];
static S_6_35: [symbol; 6] = [b'a', b's', b's', b'i', b'e', b'z'];
static S_6_36: [symbol; 4] = [b'e', b'r', b'e', b'z'];
static S_6_37: [symbol; 2] = [0xC3, 0xA9];

static A_6: [among; 38] = [
    among { s_size: 1, s: S_6_0.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 3, s: S_6_1.as_ptr(), substring_i: 0, result: 2, function: None },
    among { s_size: 4, s: S_6_2.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 4, s: S_6_3.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 3, s: S_6_4.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 2, s: S_6_5.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 4, s: S_6_6.as_ptr(), substring_i: 5, result: 2, function: None },
    among { s_size: 2, s: S_6_7.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 2, s: S_6_8.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 4, s: S_6_9.as_ptr(), substring_i: 8, result: 2, function: None },
    among { s_size: 5, s: S_6_10.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 5, s: S_6_11.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 5, s: S_6_12.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 5, s: S_6_13.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 4, s: S_6_14.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 3, s: S_6_15.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 5, s: S_6_16.as_ptr(), substring_i: 15, result: 2, function: None },
    among { s_size: 4, s: S_6_17.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_6_18.as_ptr(), substring_i: 17, result: 2, function: None },
    among { s_size: 7, s: S_6_19.as_ptr(), substring_i: 17, result: 3, function: None },
    among { s_size: 5, s: S_6_20.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 4, s: S_6_21.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 3, s: S_6_22.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 3, s: S_6_23.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 5, s: S_6_24.as_ptr(), substring_i: 23, result: 2, function: None },
    among { s_size: 3, s: S_6_25.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 5, s: S_6_26.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 7, s: S_6_27.as_ptr(), substring_i: 26, result: 2, function: None },
    among { s_size: 6, s: S_6_28.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 6, s: S_6_29.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 5, s: S_6_30.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 3, s: S_6_31.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 2, s: S_6_32.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 3, s: S_6_33.as_ptr(), substring_i: 32, result: 2, function: None },
    among { s_size: 5, s: S_6_34.as_ptr(), substring_i: 33, result: 2, function: None },
    among { s_size: 6, s: S_6_35.as_ptr(), substring_i: 33, result: 3, function: None },
    among { s_size: 4, s: S_6_36.as_ptr(), substring_i: 32, result: 2, function: None },
    among { s_size: 2, s: S_6_37.as_ptr(), substring_i: -1, result: 2, function: None },
];

static S_7_0: [symbol; 1] = [b'e'];
static S_7_1: [symbol; 5] = [b'I', 0xC3, 0xA8, b'r', b'e'];
static S_7_2: [symbol; 5] = [b'i', 0xC3, 0xA8, b'r', b'e'];
static S_7_3: [symbol; 3] = [b'i', b'o', b'n'];
static S_7_4: [symbol; 3] = [b'I', b'e', b'r'];
static S_7_5: [symbol; 3] = [b'i', b'e', b'r'];

static A_7: [among; 6] = [
    among { s_size: 1, s: S_7_0.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 5, s: S_7_1.as_ptr(), substring_i: 0, result: 2, function: None },
    among { s_size: 5, s: S_7_2.as_ptr(), substring_i: 0, result: 2, function: None },
    among { s_size: 3, s: S_7_3.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_7_4.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 3, s: S_7_5.as_ptr(), substring_i: -1, result: 2, function: None },
];

static S_8_0: [symbol; 3] = [b'e', b'l', b'l'];
static S_8_1: [symbol; 4] = [b'e', b'i', b'l', b'l'];
static S_8_2: [symbol; 3] = [b'e', b'n', b'n'];
static S_8_3: [symbol; 3] = [b'o', b'n', b'n'];
static S_8_4: [symbol; 3] = [b'e', b't', b't'];

static A_8: [among; 5] = [
    among { s_size: 3, s: S_8_0.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 4, s: S_8_1.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_8_2.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_8_3.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_8_4.as_ptr(), substring_i: -1, result: -1, function: None },
];

// ---------------------------------------------------------------------------
// grouping bit tables
// ---------------------------------------------------------------------------

static G_V: [c_uchar; 20] = [
    17, 65, 16, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 128, 130, 103, 8, 5,
];

static G_ELISION_CHAR: [c_uchar; 3] = [131, 14, 3];

static G_KEEP_WITH_S: [c_uchar; 17] = [1, 65, 20, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 128];

// ---------------------------------------------------------------------------
// literal strings used by slice_from_s / eq_s
// ---------------------------------------------------------------------------

static S_0: [symbol; 2] = [b'q', b'u'];
static S_1: [symbol; 1] = [b'U'];
static S_2: [symbol; 1] = [b'I'];
static S_3: [symbol; 1] = [b'Y'];
static S_4: [symbol; 2] = [0xC3, 0xAB];
static S_5: [symbol; 2] = [b'H', b'e'];
static S_6: [symbol; 2] = [0xC3, 0xAF];
static S_7: [symbol; 2] = [b'H', b'i'];
static S_8: [symbol; 1] = [b'Y'];
static S_9: [symbol; 1] = [b'U'];
static S_10: [symbol; 1] = [b'i'];
static S_11: [symbol; 1] = [b'u'];
static S_12: [symbol; 1] = [b'y'];
static S_13: [symbol; 2] = [0xC3, 0xAB];
static S_14: [symbol; 2] = [0xC3, 0xAF];
static S_15: [symbol; 2] = [b'i', b'c'];
static S_16: [symbol; 3] = [b'i', b'q', b'U'];
static S_17: [symbol; 3] = [b'l', b'o', b'g'];
static S_18: [symbol; 1] = [b'u'];
static S_19: [symbol; 3] = [b'e', b'n', b't'];
static S_20: [symbol; 2] = [b'a', b't'];
static S_21: [symbol; 3] = [b'e', b'u', b'x'];
static S_22: [symbol; 1] = [b'i'];
static S_23: [symbol; 3] = [b'a', b'b', b'l'];
static S_24: [symbol; 3] = [b'i', b'q', b'U'];
static S_25: [symbol; 2] = [b'a', b't'];
static S_26: [symbol; 2] = [b'i', b'c'];
static S_27: [symbol; 3] = [b'i', b'q', b'U'];
static S_28: [symbol; 3] = [b'e', b'a', b'u'];
static S_29: [symbol; 2] = [b'a', b'l'];
static S_30: [symbol; 3] = [b'e', b'u', b'x'];
static S_31: [symbol; 3] = [b'a', b'n', b't'];
static S_32: [symbol; 3] = [b'e', b'n', b't'];
static S_33: [symbol; 2] = [b'H', b'i'];
static S_34: [symbol; 1] = [b'i'];
static S_35: [symbol; 2] = [0xC3, 0xA9];
static S_36: [symbol; 2] = [0xC3, 0xA8];
static S_37: [symbol; 1] = [b'e'];
static S_38: [symbol; 1] = [b'i'];
static S_39: [symbol; 2] = [0xC3, 0xA7];
static S_40: [symbol; 1] = [b'c'];

// ---------------------------------------------------------------------------
// rules
// ---------------------------------------------------------------------------

unsafe fn r_elisions(z: *mut SN_env) -> c_int {
    (*z).bra = (*z).c;
    'lab0: {
        let c1 = (*z).c;
        'lab1: {
            if in_grouping_U(z, G_ELISION_CHAR.as_ptr(), 99, 116, 0) != 0 {
                break 'lab1;
            }
            break 'lab0;
        }
        // lab1:
        (*z).c = c1;
        if eq_s(z, 2, S_0.as_ptr()) == 0 {
            return 0;
        }
    }
    // lab0:
    if (*z).c == (*z).l || *(*z).p.offset((*z).c as isize) != b'\'' {
        return 0;
    }
    (*z).c += 1;
    (*z).ket = (*z).c;

    'lab2: {
        if (*z).c < (*z).l {
            break 'lab2;
        }
        return 0;
    }
    // lab2:
    {
        let ret = slice_del(z);
        if ret < 0 {
            return ret;
        }
    }
    1
}

unsafe fn r_prelude(z: *mut SN_env) -> c_int {
    'frn0b: loop {
        let c1 = (*z).c;
        'lab0: {
            'frn0: loop {
                let c2 = (*z).c;
                'lab1: {
                    'lab2: {
                        let c3 = (*z).c;
                        'lab3: {
                            if in_grouping_U(z, G_V.as_ptr(), 97, 251, 0) != 0 {
                                break 'lab3;
                            }
                            (*z).bra = (*z).c;
                            'lab4: {
                                let c4 = (*z).c;
                                'lab5: {
                                    if (*z).c == (*z).l
                                        || *(*z).p.offset((*z).c as isize) != b'u'
                                    {
                                        break 'lab5;
                                    }
                                    (*z).c += 1;
                                    (*z).ket = (*z).c;
                                    if in_grouping_U(z, G_V.as_ptr(), 97, 251, 0) != 0 {
                                        break 'lab5;
                                    }
                                    {
                                        let ret = slice_from_s(z, 1, S_1.as_ptr());
                                        if ret < 0 {
                                            return ret;
                                        }
                                    }
                                    break 'lab4;
                                }
                                // lab5:
                                (*z).c = c4;
                                'lab6: {
                                    if (*z).c == (*z).l
                                        || *(*z).p.offset((*z).c as isize) != b'i'
                                    {
                                        break 'lab6;
                                    }
                                    (*z).c += 1;
                                    (*z).ket = (*z).c;
                                    if in_grouping_U(z, G_V.as_ptr(), 97, 251, 0) != 0 {
                                        break 'lab6;
                                    }
                                    {
                                        let ret = slice_from_s(z, 1, S_2.as_ptr());
                                        if ret < 0 {
                                            return ret;
                                        }
                                    }
                                    break 'lab4;
                                }
                                // lab6:
                                (*z).c = c4;
                                if (*z).c == (*z).l || *(*z).p.offset((*z).c as isize) != b'y' {
                                    break 'lab3;
                                }
                                (*z).c += 1;
                                (*z).ket = (*z).c;
                                {
                                    let ret = slice_from_s(z, 1, S_3.as_ptr());
                                    if ret < 0 {
                                        return ret;
                                    }
                                }
                            }
                            // lab4:
                            break 'lab2;
                        }
                        // lab3:
                        (*z).c = c3;
                        'lab7: {
                            (*z).bra = (*z).c;
                            if eq_s(z, 2, S_4.as_ptr()) == 0 {
                                break 'lab7;
                            }
                            (*z).ket = (*z).c;
                            {
                                let ret = slice_from_s(z, 2, S_5.as_ptr());
                                if ret < 0 {
                                    return ret;
                                }
                            }
                            break 'lab2;
                        }
                        // lab7:
                        (*z).c = c3;
                        'lab8: {
                            (*z).bra = (*z).c;
                            if eq_s(z, 2, S_6.as_ptr()) == 0 {
                                break 'lab8;
                            }
                            (*z).ket = (*z).c;
                            {
                                let ret = slice_from_s(z, 2, S_7.as_ptr());
                                if ret < 0 {
                                    return ret;
                                }
                            }
                            break 'lab2;
                        }
                        // lab8:
                        (*z).c = c3;
                        'lab9: {
                            (*z).bra = (*z).c;
                            if (*z).c == (*z).l || *(*z).p.offset((*z).c as isize) != b'y' {
                                break 'lab9;
                            }
                            (*z).c += 1;
                            (*z).ket = (*z).c;
                            if in_grouping_U(z, G_V.as_ptr(), 97, 251, 0) != 0 {
                                break 'lab9;
                            }
                            {
                                let ret = slice_from_s(z, 1, S_8.as_ptr());
                                if ret < 0 {
                                    return ret;
                                }
                            }
                            break 'lab2;
                        }
                        // lab9:
                        (*z).c = c3;
                        if (*z).c == (*z).l || *(*z).p.offset((*z).c as isize) != b'q' {
                            break 'lab1;
                        }
                        (*z).c += 1;
                        (*z).bra = (*z).c;
                        if (*z).c == (*z).l || *(*z).p.offset((*z).c as isize) != b'u' {
                            break 'lab1;
                        }
                        (*z).c += 1;
                        (*z).ket = (*z).c;
                        {
                            let ret = slice_from_s(z, 1, S_9.as_ptr());
                            if ret < 0 {
                                return ret;
                            }
                        }
                    }
                    // lab2:
                    (*z).c = c2;
                    break 'frn0;
                }
                // lab1:
                (*z).c = c2;
                {
                    let ret = skip_utf8((*z).p, (*z).c, (*z).l, 1);
                    if ret < 0 {
                        break 'lab0;
                    }
                    (*z).c = ret;
                }
            }
            continue 'frn0b;
        }
        // lab0:
        (*z).c = c1;
        break;
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
            'lab1: {
                let c2 = (*z).c;
                'lab2: {
                    if in_grouping_U(z, G_V.as_ptr(), 97, 251, 0) != 0 {
                        break 'lab2;
                    }
                    if in_grouping_U(z, G_V.as_ptr(), 97, 251, 0) != 0 {
                        break 'lab2;
                    }
                    {
                        let ret = skip_utf8((*z).p, (*z).c, (*z).l, 1);
                        if ret < 0 {
                            break 'lab2;
                        }
                        (*z).c = ret;
                    }
                    break 'lab1;
                }
                // lab2:
                (*z).c = c2;
                'lab3: {
                    if (*z).c + 2 >= (*z).l
                        || *(*z).p.offset(((*z).c + 2) as isize) as c_int >> 5 != 3
                        || (331776 >> (*(*z).p.offset(((*z).c + 2) as isize) as c_int & 0x1f)) & 1
                            == 0
                    {
                        break 'lab3;
                    }
                    if find_among(z, A_0.as_ptr(), 3) == 0 {
                        break 'lab3;
                    }
                    break 'lab1;
                }
                // lab3:
                (*z).c = c2;
                {
                    let ret = skip_utf8((*z).p, (*z).c, (*z).l, 1);
                    if ret < 0 {
                        break 'lab0;
                    }
                    (*z).c = ret;
                }

                {
                    let ret = out_grouping_U(z, G_V.as_ptr(), 97, 251, 1);
                    if ret < 0 {
                        break 'lab0;
                    }
                    (*z).c += ret;
                }
            }
            // lab1:
            *(*z).I.offset(2) = (*z).c;
        }
        // lab0:
        (*z).c = c1;
    }
    {
        let c3 = (*z).c;
        'lab4: {
            {
                let ret = out_grouping_U(z, G_V.as_ptr(), 97, 251, 1);
                if ret < 0 {
                    break 'lab4;
                }
                (*z).c += ret;
            }

            {
                let ret = in_grouping_U(z, G_V.as_ptr(), 97, 251, 1);
                if ret < 0 {
                    break 'lab4;
                }
                (*z).c += ret;
            }
            *(*z).I.offset(1) = (*z).c;

            {
                let ret = out_grouping_U(z, G_V.as_ptr(), 97, 251, 1);
                if ret < 0 {
                    break 'lab4;
                }
                (*z).c += ret;
            }

            {
                let ret = in_grouping_U(z, G_V.as_ptr(), 97, 251, 1);
                if ret < 0 {
                    break 'lab4;
                }
                (*z).c += ret;
            }
            *(*z).I.offset(0) = (*z).c;
        }
        // lab4:
        (*z).c = c3;
    }
    1
}

unsafe fn r_postlude(z: *mut SN_env) -> c_int {
    let mut among_var;
    'frn1: loop {
        let c1 = (*z).c;
        'lab0: {
            (*z).bra = (*z).c;
            if (*z).c >= (*z).l
                || *(*z).p.offset(((*z).c + 0) as isize) as c_int >> 5 != 2
                || (35652352 >> (*(*z).p.offset(((*z).c + 0) as isize) as c_int & 0x1f)) & 1 == 0
            {
                among_var = 7;
            } else {
                among_var = find_among(z, A_1.as_ptr(), 7);
            }
            (*z).ket = (*z).c;
            match among_var {
                1 => {
                    let ret = slice_from_s(z, 1, S_10.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
                2 => {
                    let ret = slice_from_s(z, 1, S_11.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
                3 => {
                    let ret = slice_from_s(z, 1, S_12.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
                4 => {
                    let ret = slice_from_s(z, 2, S_13.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
                5 => {
                    let ret = slice_from_s(z, 2, S_14.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
                6 => {
                    let ret = slice_del(z);
                    if ret < 0 {
                        return ret;
                    }
                }
                7 => {
                    let ret = skip_utf8((*z).p, (*z).c, (*z).l, 1);
                    if ret < 0 {
                        break 'lab0;
                    }
                    (*z).c = ret;
                }
                _ => {}
            }
            continue 'frn1;
        }
        // lab0:
        (*z).c = c1;
        break;
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

unsafe fn r_standard_suffix(z: *mut SN_env) -> c_int {
    let mut among_var;
    (*z).ket = (*z).c;
    among_var = find_among_b(z, A_4.as_ptr(), 43);
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
            {
                let m1 = (*z).l - (*z).c;
                'lab0: {
                    (*z).ket = (*z).c;
                    if eq_s_b(z, 2, S_15.as_ptr()) == 0 {
                        (*z).c = (*z).l - m1;
                        break 'lab0;
                    }
                    (*z).bra = (*z).c;
                    {
                        let m2 = (*z).l - (*z).c;
                        'lab1: {
                            'lab2: {
                                {
                                    let ret = r_R2(z);
                                    if ret == 0 {
                                        break 'lab2;
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
                                break 'lab1;
                            }
                            // lab2:
                            (*z).c = (*z).l - m2;
                            {
                                let ret = slice_from_s(z, 3, S_16.as_ptr());
                                if ret < 0 {
                                    return ret;
                                }
                            }
                        }
                        // lab1:
                    }
                }
            }
        }
        3 => {
            {
                let ret = r_R2(z);
                if ret <= 0 {
                    return ret;
                }
            }
            {
                let ret = slice_from_s(z, 3, S_17.as_ptr());
                if ret < 0 {
                    return ret;
                }
            }
        }
        4 => {
            {
                let ret = r_R2(z);
                if ret <= 0 {
                    return ret;
                }
            }
            {
                let ret = slice_from_s(z, 1, S_18.as_ptr());
                if ret < 0 {
                    return ret;
                }
            }
        }
        5 => {
            {
                let ret = r_R2(z);
                if ret <= 0 {
                    return ret;
                }
            }
            {
                let ret = slice_from_s(z, 3, S_19.as_ptr());
                if ret < 0 {
                    return ret;
                }
            }
        }
        6 => {
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
            {
                let m3 = (*z).l - (*z).c;
                'lab3: {
                    (*z).ket = (*z).c;
                    among_var = find_among_b(z, A_2.as_ptr(), 6);
                    if among_var == 0 {
                        (*z).c = (*z).l - m3;
                        break 'lab3;
                    }
                    (*z).bra = (*z).c;
                    match among_var {
                        1 => {
                            {
                                let ret = r_R2(z);
                                if ret == 0 {
                                    (*z).c = (*z).l - m3;
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
                            (*z).ket = (*z).c;
                            if eq_s_b(z, 2, S_20.as_ptr()) == 0 {
                                (*z).c = (*z).l - m3;
                                break 'lab3;
                            }
                            (*z).bra = (*z).c;
                            {
                                let ret = r_R2(z);
                                if ret == 0 {
                                    (*z).c = (*z).l - m3;
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
                        2 => {
                            {
                                let m4 = (*z).l - (*z).c;
                                'lab4: {
                                    'lab5: {
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
                                            let ret = slice_del(z);
                                            if ret < 0 {
                                                return ret;
                                            }
                                        }
                                        break 'lab4;
                                    }
                                    // lab5:
                                    (*z).c = (*z).l - m4;
                                    {
                                        let ret = r_R1(z);
                                        if ret == 0 {
                                            (*z).c = (*z).l - m3;
                                            break 'lab3;
                                        }
                                        if ret < 0 {
                                            return ret;
                                        }
                                    }
                                    {
                                        let ret = slice_from_s(z, 3, S_21.as_ptr());
                                        if ret < 0 {
                                            return ret;
                                        }
                                    }
                                }
                                // lab4:
                            }
                        }
                        3 => {
                            {
                                let ret = r_R2(z);
                                if ret == 0 {
                                    (*z).c = (*z).l - m3;
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
                        4 => {
                            {
                                let ret = r_RV(z);
                                if ret == 0 {
                                    (*z).c = (*z).l - m3;
                                    break 'lab3;
                                }
                                if ret < 0 {
                                    return ret;
                                }
                            }
                            {
                                let ret = slice_from_s(z, 1, S_22.as_ptr());
                                if ret < 0 {
                                    return ret;
                                }
                            }
                        }
                        _ => {}
                    }
                }
            }
        }
        7 => {
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
            {
                let m5 = (*z).l - (*z).c;
                'lab6: {
                    (*z).ket = (*z).c;
                    if (*z).c - 1 <= (*z).lb
                        || *(*z).p.offset(((*z).c - 1) as isize) as c_int >> 5 != 3
                        || (4198408 >> (*(*z).p.offset(((*z).c - 1) as isize) as c_int & 0x1f)) & 1
                            == 0
                    {
                        (*z).c = (*z).l - m5;
                        break 'lab6;
                    }
                    among_var = find_among_b(z, A_3.as_ptr(), 3);
                    if among_var == 0 {
                        (*z).c = (*z).l - m5;
                        break 'lab6;
                    }
                    (*z).bra = (*z).c;
                    match among_var {
                        1 => {
                            let m6 = (*z).l - (*z).c;
                            'lab7: {
                                'lab8: {
                                    {
                                        let ret = r_R2(z);
                                        if ret == 0 {
                                            break 'lab8;
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
                                    break 'lab7;
                                }
                                // lab8:
                                (*z).c = (*z).l - m6;
                                {
                                    let ret = slice_from_s(z, 3, S_23.as_ptr());
                                    if ret < 0 {
                                        return ret;
                                    }
                                }
                            }
                        }
                        2 => {
                            let m7 = (*z).l - (*z).c;
                            'lab9: {
                                'lab10: {
                                    {
                                        let ret = r_R2(z);
                                        if ret == 0 {
                                            break 'lab10;
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
                                    break 'lab9;
                                }
                                // lab10:
                                (*z).c = (*z).l - m7;
                                {
                                    let ret = slice_from_s(z, 3, S_24.as_ptr());
                                    if ret < 0 {
                                        return ret;
                                    }
                                }
                            }
                        }
                        3 => {
                            {
                                let ret = r_R2(z);
                                if ret == 0 {
                                    (*z).c = (*z).l - m5;
                                    break 'lab6;
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
                        _ => {}
                    }
                }
            }
        }
        8 => {
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
            {
                let m8 = (*z).l - (*z).c;
                'lab11: {
                    (*z).ket = (*z).c;
                    if eq_s_b(z, 2, S_25.as_ptr()) == 0 {
                        (*z).c = (*z).l - m8;
                        break 'lab11;
                    }
                    (*z).bra = (*z).c;
                    {
                        let ret = r_R2(z);
                        if ret == 0 {
                            (*z).c = (*z).l - m8;
                            break 'lab11;
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
                    (*z).ket = (*z).c;
                    if eq_s_b(z, 2, S_26.as_ptr()) == 0 {
                        (*z).c = (*z).l - m8;
                        break 'lab11;
                    }
                    (*z).bra = (*z).c;
                    {
                        let m9 = (*z).l - (*z).c;
                        'lab12: {
                            'lab13: {
                                {
                                    let ret = r_R2(z);
                                    if ret == 0 {
                                        break 'lab13;
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
                                break 'lab12;
                            }
                            // lab13:
                            (*z).c = (*z).l - m9;
                            {
                                let ret = slice_from_s(z, 3, S_27.as_ptr());
                                if ret < 0 {
                                    return ret;
                                }
                            }
                        }
                        // lab12:
                    }
                }
            }
        }
        9 => {
            let ret = slice_from_s(z, 3, S_28.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        10 => {
            {
                let ret = r_R1(z);
                if ret <= 0 {
                    return ret;
                }
            }
            {
                let ret = slice_from_s(z, 2, S_29.as_ptr());
                if ret < 0 {
                    return ret;
                }
            }
        }
        11 => {
            let m10 = (*z).l - (*z).c;
            'lab14: {
                'lab15: {
                    {
                        let ret = r_R2(z);
                        if ret == 0 {
                            break 'lab15;
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
                    break 'lab14;
                }
                // lab15:
                (*z).c = (*z).l - m10;
                {
                    let ret = r_R1(z);
                    if ret <= 0 {
                        return ret;
                    }
                }
                {
                    let ret = slice_from_s(z, 3, S_30.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
            }
        }
        12 => {
            {
                let ret = r_R1(z);
                if ret <= 0 {
                    return ret;
                }
            }
            if out_grouping_b_U(z, G_V.as_ptr(), 97, 251, 0) != 0 {
                return 0;
            }
            {
                let ret = slice_del(z);
                if ret < 0 {
                    return ret;
                }
            }
        }
        13 => {
            {
                let ret = r_RV(z);
                if ret <= 0 {
                    return ret;
                }
            }
            {
                let ret = slice_from_s(z, 3, S_31.as_ptr());
                if ret < 0 {
                    return ret;
                }
            }
            return 0;
        }
        14 => {
            {
                let ret = r_RV(z);
                if ret <= 0 {
                    return ret;
                }
            }
            {
                let ret = slice_from_s(z, 3, S_32.as_ptr());
                if ret < 0 {
                    return ret;
                }
            }
            return 0;
        }
        15 => {
            {
                let m_test11 = (*z).l - (*z).c;
                if in_grouping_b_U(z, G_V.as_ptr(), 97, 251, 0) != 0 {
                    return 0;
                }
                {
                    let ret = r_RV(z);
                    if ret <= 0 {
                        return ret;
                    }
                }
                (*z).c = (*z).l - m_test11;
            }
            {
                let ret = slice_del(z);
                if ret < 0 {
                    return ret;
                }
            }
            return 0;
        }
        _ => {}
    }
    1
}

unsafe fn r_i_verb_suffix(z: *mut SN_env) -> c_int {
    {
        let mlimit1;
        if (*z).c < *(*z).I.offset(2) {
            return 0;
        }
        mlimit1 = (*z).lb;
        (*z).lb = *(*z).I.offset(2);
        (*z).ket = (*z).c;
        if (*z).c <= (*z).lb
            || *(*z).p.offset(((*z).c - 1) as isize) as c_int >> 5 != 3
            || (68944418 >> (*(*z).p.offset(((*z).c - 1) as isize) as c_int & 0x1f)) & 1 == 0
        {
            (*z).lb = mlimit1;
            return 0;
        }
        if find_among_b(z, A_5.as_ptr(), 35) == 0 {
            (*z).lb = mlimit1;
            return 0;
        }
        (*z).bra = (*z).c;
        {
            let m2 = (*z).l - (*z).c;
            'lab0: {
                if (*z).c <= (*z).lb || *(*z).p.offset(((*z).c - 1) as isize) != b'H' {
                    break 'lab0;
                }
                (*z).c -= 1;
                {
                    (*z).lb = mlimit1;
                    return 0;
                }
            }
            // lab0:
            (*z).c = (*z).l - m2;
        }
        if out_grouping_b_U(z, G_V.as_ptr(), 97, 251, 0) != 0 {
            (*z).lb = mlimit1;
            return 0;
        }
        {
            let ret = slice_del(z);
            if ret < 0 {
                return ret;
            }
        }
        (*z).lb = mlimit1;
    }
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
        among_var = find_among_b(z, A_6.as_ptr(), 38);
        if among_var == 0 {
            (*z).lb = mlimit1;
            return 0;
        }
        (*z).bra = (*z).c;
        match among_var {
            1 => {
                {
                    let ret = r_R2(z);
                    if ret == 0 {
                        (*z).lb = mlimit1;
                        return 0;
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
                    let m2 = (*z).l - (*z).c;
                    'lab0: {
                        (*z).ket = (*z).c;
                        if (*z).c <= (*z).lb || *(*z).p.offset(((*z).c - 1) as isize) != b'e' {
                            (*z).c = (*z).l - m2;
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
                }
            }
            _ => {}
        }
        (*z).lb = mlimit1;
    }
    1
}

unsafe fn r_residual_suffix(z: *mut SN_env) -> c_int {
    let among_var;
    {
        let m1 = (*z).l - (*z).c;
        'lab0: {
            (*z).ket = (*z).c;
            if (*z).c <= (*z).lb || *(*z).p.offset(((*z).c - 1) as isize) != b's' {
                (*z).c = (*z).l - m1;
                break 'lab0;
            }
            (*z).c -= 1;
            (*z).bra = (*z).c;
            {
                let m_test2 = (*z).l - (*z).c;
                'lab1: {
                    let m3 = (*z).l - (*z).c;
                    'lab2: {
                        if eq_s_b(z, 2, S_33.as_ptr()) == 0 {
                            break 'lab2;
                        }
                        break 'lab1;
                    }
                    // lab2:
                    (*z).c = (*z).l - m3;
                    if out_grouping_b_U(z, G_KEEP_WITH_S.as_ptr(), 97, 232, 0) != 0 {
                        (*z).c = (*z).l - m1;
                        break 'lab0;
                    }
                }
                // lab1:
                (*z).c = (*z).l - m_test2;
            }
            {
                let ret = slice_del(z);
                if ret < 0 {
                    return ret;
                }
            }
        }
    }

    {
        let mlimit4;
        if (*z).c < *(*z).I.offset(2) {
            return 0;
        }
        mlimit4 = (*z).lb;
        (*z).lb = *(*z).I.offset(2);
        (*z).ket = (*z).c;
        if (*z).c <= (*z).lb
            || *(*z).p.offset(((*z).c - 1) as isize) as c_int >> 5 != 3
            || (278560 >> (*(*z).p.offset(((*z).c - 1) as isize) as c_int & 0x1f)) & 1 == 0
        {
            (*z).lb = mlimit4;
            return 0;
        }
        among_var = find_among_b(z, A_7.as_ptr(), 6);
        if among_var == 0 {
            (*z).lb = mlimit4;
            return 0;
        }
        (*z).bra = (*z).c;
        match among_var {
            1 => {
                {
                    let ret = r_R2(z);
                    if ret == 0 {
                        (*z).lb = mlimit4;
                        return 0;
                    }
                    if ret < 0 {
                        return ret;
                    }
                }
                {
                    let m5 = (*z).l - (*z).c;
                    'lab3: {
                        'lab4: {
                            if (*z).c <= (*z).lb
                                || *(*z).p.offset(((*z).c - 1) as isize) != b's'
                            {
                                break 'lab4;
                            }
                            (*z).c -= 1;
                            break 'lab3;
                        }
                        // lab4:
                        (*z).c = (*z).l - m5;
                        if (*z).c <= (*z).lb || *(*z).p.offset(((*z).c - 1) as isize) != b't' {
                            (*z).lb = mlimit4;
                            return 0;
                        }
                        (*z).c -= 1;
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
                let ret = slice_from_s(z, 1, S_34.as_ptr());
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
        (*z).lb = mlimit4;
    }
    1
}

unsafe fn r_un_double(z: *mut SN_env) -> c_int {
    {
        let m_test1 = (*z).l - (*z).c;
        if (*z).c - 2 <= (*z).lb
            || *(*z).p.offset(((*z).c - 1) as isize) as c_int >> 5 != 3
            || (1069056 >> (*(*z).p.offset(((*z).c - 1) as isize) as c_int & 0x1f)) & 1 == 0
        {
            return 0;
        }
        if find_among_b(z, A_8.as_ptr(), 5) == 0 {
            return 0;
        }
        (*z).c = (*z).l - m_test1;
    }
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
    1
}

unsafe fn r_un_accent(z: *mut SN_env) -> c_int {
    {
        let mut i = 1;
        'frn2: loop {
            'lab0: {
                if out_grouping_b_U(z, G_V.as_ptr(), 97, 251, 0) != 0 {
                    break 'lab0;
                }
                i -= 1;
                continue 'frn2;
            }
            // lab0:
            break;
        }
        if i > 0 {
            return 0;
        }
    }
    (*z).ket = (*z).c;
    {
        let m1 = (*z).l - (*z).c;
        'lab1: {
            'lab2: {
                if eq_s_b(z, 2, S_35.as_ptr()) == 0 {
                    break 'lab2;
                }
                break 'lab1;
            }
            // lab2:
            (*z).c = (*z).l - m1;
            if eq_s_b(z, 2, S_36.as_ptr()) == 0 {
                return 0;
            }
        }
    }
    // lab1:
    (*z).bra = (*z).c;
    {
        let ret = slice_from_s(z, 1, S_37.as_ptr());
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
pub unsafe extern "C" fn french_UTF_8_stem(z: *mut SN_env) -> c_int {
    {
        let c1 = (*z).c;
        {
            let ret = r_elisions(z);
            if ret < 0 {
                return ret;
            }
        }
        (*z).c = c1;
    }
    {
        let c2 = (*z).c;
        {
            let ret = r_prelude(z);
            if ret < 0 {
                return ret;
            }
        }
        (*z).c = c2;
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
        let m3 = (*z).l - (*z).c;
        'lab0: {
            'lab1: {
                let m4 = (*z).l - (*z).c;
                'lab2: {
                    let m5 = (*z).l - (*z).c;
                    {
                        let m6 = (*z).l - (*z).c;
                        'lab3: {
                            'lab4: {
                                {
                                    let ret = r_standard_suffix(z);
                                    if ret == 0 {
                                        break 'lab4;
                                    }
                                    if ret < 0 {
                                        return ret;
                                    }
                                }
                                break 'lab3;
                            }
                            // lab4:
                            (*z).c = (*z).l - m6;
                            'lab5: {
                                {
                                    let ret = r_i_verb_suffix(z);
                                    if ret == 0 {
                                        break 'lab5;
                                    }
                                    if ret < 0 {
                                        return ret;
                                    }
                                }
                                break 'lab3;
                            }
                            // lab5:
                            (*z).c = (*z).l - m6;
                            {
                                let ret = r_verb_suffix(z);
                                if ret == 0 {
                                    break 'lab2;
                                }
                                if ret < 0 {
                                    return ret;
                                }
                            }
                        }
                        // lab3:
                    }
                    (*z).c = (*z).l - m5;
                    {
                        let m7 = (*z).l - (*z).c;
                        'lab6: {
                            (*z).ket = (*z).c;
                            {
                                let m8 = (*z).l - (*z).c;
                                'lab7: {
                                    'lab8: {
                                        if (*z).c <= (*z).lb
                                            || *(*z).p.offset(((*z).c - 1) as isize) != b'Y'
                                        {
                                            break 'lab8;
                                        }
                                        (*z).c -= 1;
                                        (*z).bra = (*z).c;
                                        {
                                            let ret = slice_from_s(z, 1, S_38.as_ptr());
                                            if ret < 0 {
                                                return ret;
                                            }
                                        }
                                        break 'lab7;
                                    }
                                    // lab8:
                                    (*z).c = (*z).l - m8;
                                    if eq_s_b(z, 2, S_39.as_ptr()) == 0 {
                                        (*z).c = (*z).l - m7;
                                        break 'lab6;
                                    }
                                    (*z).bra = (*z).c;
                                    {
                                        let ret = slice_from_s(z, 1, S_40.as_ptr());
                                        if ret < 0 {
                                            return ret;
                                        }
                                    }
                                }
                                // lab7:
                            }
                        }
                        // lab6:
                    }
                    break 'lab1;
                }
                // lab2:
                (*z).c = (*z).l - m4;
                {
                    let ret = r_residual_suffix(z);
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
        (*z).c = (*z).l - m3;
    }
    {
        let m9 = (*z).l - (*z).c;
        {
            let ret = r_un_double(z);
            if ret < 0 {
                return ret;
            }
        }
        (*z).c = (*z).l - m9;
    }
    {
        let m10 = (*z).l - (*z).c;
        {
            let ret = r_un_accent(z);
            if ret < 0 {
                return ret;
            }
        }
        (*z).c = (*z).l - m10;
    }
    (*z).c = (*z).lb;
    {
        let c11 = (*z).c;
        {
            let ret = r_postlude(z);
            if ret < 0 {
                return ret;
            }
        }
        (*z).c = c11;
    }
    1
}

#[no_mangle]
pub unsafe extern "C" fn french_UTF_8_create_env() -> *mut SN_env {
    SN_create_env(0, 3)
}

#[no_mangle]
pub unsafe extern "C" fn french_UTF_8_close_env(z: *mut SN_env) {
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
        let z = french_UTF_8_create_env();
        assert!(!z.is_null());
        let rc = SN_set_current(z, word.len() as c_int, word.as_ptr());
        assert!(rc >= 0);
        let rc = french_UTF_8_stem(z);
        assert!(rc >= 0, "stem returned {rc}");
        let out = std::slice::from_raw_parts((*z).p, SIZE((*z).p) as usize).to_vec();
        french_UTF_8_close_env(z);
        out
    }

    // A short root with no strippable suffix is returned unchanged.
    #[test]
    fn short_root_unchanged() {
        unsafe {
            assert_eq!(stem(b"chat"), b"chat".to_vec());
        }
    }

    // Idempotence: stemming a stem yields the same stem.
    #[test]
    fn idempotent() {
        unsafe {
            for w in [
                &b"continuellement"[..],
                &b"national"[..],
                &b"importante"[..],
                &b"finissaient"[..],
            ] {
                let once = stem(w);
                let twice = stem(&once);
                assert_eq!(once, twice, "not idempotent for {:?}", w);
                assert!(!once.is_empty());
            }
        }
    }

    // A longer inflected form is reduced and cannot grow.
    #[test]
    fn suffix_stripped_nonempty() {
        unsafe {
            let r = stem(b"importante");
            assert!(!r.is_empty());
            assert!(r.len() <= "importante".len());
        }
    }
}
