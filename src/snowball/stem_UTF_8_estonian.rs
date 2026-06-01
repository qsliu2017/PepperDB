//! Estonian Snowball stemmer (UTF-8).
//!
//! 1:1 translation of the generated
//! `src/backend/snowball/libstemmer/stem_UTF_8_estonian.c` (Snowball 2.2.0),
//! merged with its header declarations. The runtime helpers come from
//! `crate::snowball::api` and `crate::snowball::utilities`.

use crate::prelude::*;

use crate::snowball::api::{among, symbol, SN_close_env, SN_create_env, SN_env, SIZE};
use crate::snowball::utilities::{
    find_among, find_among_b, in_grouping_U, in_grouping_b_U, out_grouping_U, skip_b_utf8,
    slice_del, slice_from_s,
};

// ---------------------------------------------------------------------------
// among string tables
// ---------------------------------------------------------------------------

static S_0_0: [symbol; 2] = [b'g', b'i'];
static S_0_1: [symbol; 2] = [b'k', b'i'];

static A_0: [among; 2] = [
    among { s_size: 2, s: S_0_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_0_1.as_ptr(), substring_i: -1, result: 2, function: None },
];

static S_1_0: [symbol; 2] = [b'd', b'a'];
static S_1_1: [symbol; 4] = [b'm', b'a', b't', b'a'];
static S_1_2: [symbol; 1] = [b'b'];
static S_1_3: [symbol; 4] = [b'k', b's', b'i', b'd'];
static S_1_4: [symbol; 6] = [b'n', b'u', b'k', b's', b'i', b'd'];
static S_1_5: [symbol; 2] = [b'm', b'e'];
static S_1_6: [symbol; 4] = [b's', b'i', b'm', b'e'];
static S_1_7: [symbol; 5] = [b'k', b's', b'i', b'm', b'e'];
static S_1_8: [symbol; 7] = [b'n', b'u', b'k', b's', b'i', b'm', b'e'];
static S_1_9: [symbol; 4] = [b'a', b'k', b's', b'e'];
static S_1_10: [symbol; 5] = [b'd', b'a', b'k', b's', b'e'];
static S_1_11: [symbol; 5] = [b't', b'a', b'k', b's', b'e'];
static S_1_12: [symbol; 4] = [b's', b'i', b't', b'e'];
static S_1_13: [symbol; 5] = [b'k', b's', b'i', b't', b'e'];
static S_1_14: [symbol; 7] = [b'n', b'u', b'k', b's', b'i', b't', b'e'];
static S_1_15: [symbol; 1] = [b'n'];
static S_1_16: [symbol; 3] = [b's', b'i', b'n'];
static S_1_17: [symbol; 4] = [b'k', b's', b'i', b'n'];
static S_1_18: [symbol; 6] = [b'n', b'u', b'k', b's', b'i', b'n'];
static S_1_19: [symbol; 4] = [b'd', b'a', b'k', b's'];
static S_1_20: [symbol; 4] = [b't', b'a', b'k', b's'];

static A_1: [among; 21] = [
    among { s_size: 2, s: S_1_0.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 4, s: S_1_1.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 1, s: S_1_2.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 4, s: S_1_3.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_1_4.as_ptr(), substring_i: 3, result: 1, function: None },
    among { s_size: 2, s: S_1_5.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 4, s: S_1_6.as_ptr(), substring_i: 5, result: 1, function: None },
    among { s_size: 5, s: S_1_7.as_ptr(), substring_i: 6, result: 1, function: None },
    among { s_size: 7, s: S_1_8.as_ptr(), substring_i: 7, result: 1, function: None },
    among { s_size: 4, s: S_1_9.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 5, s: S_1_10.as_ptr(), substring_i: 9, result: 1, function: None },
    among { s_size: 5, s: S_1_11.as_ptr(), substring_i: 9, result: 1, function: None },
    among { s_size: 4, s: S_1_12.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_1_13.as_ptr(), substring_i: 12, result: 1, function: None },
    among { s_size: 7, s: S_1_14.as_ptr(), substring_i: 13, result: 1, function: None },
    among { s_size: 1, s: S_1_15.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 3, s: S_1_16.as_ptr(), substring_i: 15, result: 1, function: None },
    among { s_size: 4, s: S_1_17.as_ptr(), substring_i: 16, result: 1, function: None },
    among { s_size: 6, s: S_1_18.as_ptr(), substring_i: 17, result: 1, function: None },
    among { s_size: 4, s: S_1_19.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_1_20.as_ptr(), substring_i: -1, result: 1, function: None },
];

static S_2_0: [symbol; 2] = [b'a', b'a'];
static S_2_1: [symbol; 2] = [b'e', b'e'];
static S_2_2: [symbol; 2] = [b'i', b'i'];
static S_2_3: [symbol; 2] = [b'o', b'o'];
static S_2_4: [symbol; 2] = [b'u', b'u'];
static S_2_5: [symbol; 4] = [0xC3, 0xA4, 0xC3, 0xA4];
static S_2_6: [symbol; 4] = [0xC3, 0xB5, 0xC3, 0xB5];
static S_2_7: [symbol; 4] = [0xC3, 0xB6, 0xC3, 0xB6];
static S_2_8: [symbol; 4] = [0xC3, 0xBC, 0xC3, 0xBC];

static A_2: [among; 9] = [
    among { s_size: 2, s: S_2_0.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_2_1.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_2_2.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_2_3.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_2_4.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 4, s: S_2_5.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 4, s: S_2_6.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 4, s: S_2_7.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 4, s: S_2_8.as_ptr(), substring_i: -1, result: -1, function: None },
];

static S_3_0: [symbol; 1] = [b'i'];

static A_3: [among; 1] =
    [among { s_size: 1, s: S_3_0.as_ptr(), substring_i: -1, result: 1, function: None }];

static S_4_0: [symbol; 4] = [b'l', b'a', b'n', b'e'];
static S_4_1: [symbol; 4] = [b'l', b'i', b'n', b'e'];
static S_4_2: [symbol; 4] = [b'm', b'i', b'n', b'e'];
static S_4_3: [symbol; 5] = [b'l', b'a', b's', b's', b'e'];
static S_4_4: [symbol; 5] = [b'l', b'i', b's', b's', b'e'];
static S_4_5: [symbol; 5] = [b'm', b'i', b's', b's', b'e'];
static S_4_6: [symbol; 4] = [b'l', b'a', b's', b'i'];
static S_4_7: [symbol; 4] = [b'l', b'i', b's', b'i'];
static S_4_8: [symbol; 4] = [b'm', b'i', b's', b'i'];
static S_4_9: [symbol; 4] = [b'l', b'a', b's', b't'];
static S_4_10: [symbol; 4] = [b'l', b'i', b's', b't'];
static S_4_11: [symbol; 4] = [b'm', b'i', b's', b't'];

static A_4: [among; 12] = [
    among { s_size: 4, s: S_4_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_4_1.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 4, s: S_4_2.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 5, s: S_4_3.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_4_4.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 5, s: S_4_5.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 4, s: S_4_6.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_4_7.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 4, s: S_4_8.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 4, s: S_4_9.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_4_10.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 4, s: S_4_11.as_ptr(), substring_i: -1, result: 2, function: None },
];

static S_5_0: [symbol; 2] = [b'g', b'a'];
static S_5_1: [symbol; 2] = [b't', b'a'];
static S_5_2: [symbol; 2] = [b'l', b'e'];
static S_5_3: [symbol; 3] = [b's', b's', b'e'];
static S_5_4: [symbol; 1] = [b'l'];
static S_5_5: [symbol; 1] = [b's'];
static S_5_6: [symbol; 2] = [b'k', b's'];
static S_5_7: [symbol; 1] = [b't'];
static S_5_8: [symbol; 2] = [b'l', b't'];
static S_5_9: [symbol; 2] = [b's', b't'];

static A_5: [among; 10] = [
    among { s_size: 2, s: S_5_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_5_1.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_5_2.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_5_3.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 1, s: S_5_4.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 1, s: S_5_5.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_5_6.as_ptr(), substring_i: 5, result: 1, function: None },
    among { s_size: 1, s: S_5_7.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 2, s: S_5_8.as_ptr(), substring_i: 7, result: 1, function: None },
    among { s_size: 2, s: S_5_9.as_ptr(), substring_i: 7, result: 1, function: None },
];

static S_6_1: [symbol; 3] = [b'l', b'a', b's'];
static S_6_2: [symbol; 3] = [b'l', b'i', b's'];
static S_6_3: [symbol; 3] = [b'm', b'i', b's'];
static S_6_4: [symbol; 1] = [b't'];

static A_6: [among; 5] = [
    among { s_size: 0, s: core::ptr::null(), substring_i: -1, result: 2, function: None },
    among { s_size: 3, s: S_6_1.as_ptr(), substring_i: 0, result: 1, function: None },
    among { s_size: 3, s: S_6_2.as_ptr(), substring_i: 0, result: 1, function: None },
    among { s_size: 3, s: S_6_3.as_ptr(), substring_i: 0, result: 1, function: None },
    among { s_size: 1, s: S_6_4.as_ptr(), substring_i: 0, result: -1, function: None },
];

static S_7_0: [symbol; 1] = [b'd'];
static S_7_1: [symbol; 3] = [b's', b'i', b'd'];
static S_7_2: [symbol; 2] = [b'd', b'e'];
static S_7_3: [symbol; 6] = [b'i', b'k', b'k', b'u', b'd', b'e'];
static S_7_4: [symbol; 3] = [b'i', b'k', b'e'];
static S_7_5: [symbol; 4] = [b'i', b'k', b'k', b'e'];
static S_7_6: [symbol; 2] = [b't', b'e'];

static A_7: [among; 7] = [
    among { s_size: 1, s: S_7_0.as_ptr(), substring_i: -1, result: 4, function: None },
    among { s_size: 3, s: S_7_1.as_ptr(), substring_i: 0, result: 2, function: None },
    among { s_size: 2, s: S_7_2.as_ptr(), substring_i: -1, result: 4, function: None },
    among { s_size: 6, s: S_7_3.as_ptr(), substring_i: 2, result: 1, function: None },
    among { s_size: 3, s: S_7_4.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_7_5.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_7_6.as_ptr(), substring_i: -1, result: 3, function: None },
];

static S_8_0: [symbol; 2] = [b'v', b'a'];
static S_8_1: [symbol; 2] = [b'd', b'u'];
static S_8_2: [symbol; 2] = [b'n', b'u'];
static S_8_3: [symbol; 2] = [b't', b'u'];

static A_8: [among; 4] = [
    among { s_size: 2, s: S_8_0.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_8_1.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_8_2.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_8_3.as_ptr(), substring_i: -1, result: -1, function: None },
];

static S_9_0: [symbol; 2] = [b'k', b'k'];
static S_9_1: [symbol; 2] = [b'p', b'p'];
static S_9_2: [symbol; 2] = [b't', b't'];

static A_9: [among; 3] = [
    among { s_size: 2, s: S_9_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_9_1.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 2, s: S_9_2.as_ptr(), substring_i: -1, result: 3, function: None },
];

static S_10_0: [symbol; 2] = [b'm', b'a'];
static S_10_1: [symbol; 3] = [b'm', b'a', b'i'];
static S_10_2: [symbol; 1] = [b'm'];

static A_10: [among; 3] = [
    among { s_size: 2, s: S_10_0.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 3, s: S_10_1.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 1, s: S_10_2.as_ptr(), substring_i: -1, result: 1, function: None },
];

static S_11_0: [symbol; 4] = [b'j', b'o', b'o', b'b'];
static S_11_1: [symbol; 4] = [b'j', b'o', b'o', b'd'];
static S_11_2: [symbol; 8] = [b'j', b'o', b'o', b'd', b'a', b'k', b's', b'e'];
static S_11_3: [symbol; 5] = [b'j', b'o', b'o', b'm', b'a'];
static S_11_4: [symbol; 7] = [b'j', b'o', b'o', b'm', b'a', b't', b'a'];
static S_11_5: [symbol; 5] = [b'j', b'o', b'o', b'm', b'e'];
static S_11_6: [symbol; 4] = [b'j', b'o', b'o', b'n'];
static S_11_7: [symbol; 5] = [b'j', b'o', b'o', b't', b'e'];
static S_11_8: [symbol; 6] = [b'j', b'o', b'o', b'v', b'a', b'd'];
static S_11_9: [symbol; 4] = [b'j', b'u', b'u', b'a'];
static S_11_10: [symbol; 7] = [b'j', b'u', b'u', b'a', b'k', b's', b'e'];
static S_11_11: [symbol; 4] = [b'j', 0xC3, 0xA4, b'i'];
static S_11_12: [symbol; 5] = [b'j', 0xC3, 0xA4, b'i', b'd'];
static S_11_13: [symbol; 6] = [b'j', 0xC3, 0xA4, b'i', b'm', b'e'];
static S_11_14: [symbol; 5] = [b'j', 0xC3, 0xA4, b'i', b'n'];
static S_11_15: [symbol; 6] = [b'j', 0xC3, 0xA4, b'i', b't', b'e'];
static S_11_16: [symbol; 6] = [b'j', 0xC3, 0xA4, 0xC3, 0xA4, b'b'];
static S_11_17: [symbol; 6] = [b'j', 0xC3, 0xA4, 0xC3, 0xA4, b'd'];
static S_11_18: [symbol; 7] = [b'j', 0xC3, 0xA4, 0xC3, 0xA4, b'd', b'a'];
static S_11_19: [symbol; 10] = [b'j', 0xC3, 0xA4, 0xC3, 0xA4, b'd', b'a', b'k', b's', b'e'];
static S_11_20: [symbol; 7] = [b'j', 0xC3, 0xA4, 0xC3, 0xA4, b'd', b'i'];
static S_11_21: [symbol; 7] = [b'j', 0xC3, 0xA4, 0xC3, 0xA4, b'k', b's'];
static S_11_22: [symbol; 9] = [b'j', 0xC3, 0xA4, 0xC3, 0xA4, b'k', b's', b'i', b'd'];
static S_11_23: [symbol; 10] = [b'j', 0xC3, 0xA4, 0xC3, 0xA4, b'k', b's', b'i', b'm', b'e'];
static S_11_24: [symbol; 9] = [b'j', 0xC3, 0xA4, 0xC3, 0xA4, b'k', b's', b'i', b'n'];
static S_11_25: [symbol; 10] = [b'j', 0xC3, 0xA4, 0xC3, 0xA4, b'k', b's', b'i', b't', b'e'];
static S_11_26: [symbol; 7] = [b'j', 0xC3, 0xA4, 0xC3, 0xA4, b'm', b'a'];
static S_11_27: [symbol; 9] = [b'j', 0xC3, 0xA4, 0xC3, 0xA4, b'm', b'a', b't', b'a'];
static S_11_28: [symbol; 7] = [b'j', 0xC3, 0xA4, 0xC3, 0xA4, b'm', b'e'];
static S_11_29: [symbol; 6] = [b'j', 0xC3, 0xA4, 0xC3, 0xA4, b'n'];
static S_11_30: [symbol; 7] = [b'j', 0xC3, 0xA4, 0xC3, 0xA4, b't', b'e'];
static S_11_31: [symbol; 8] = [b'j', 0xC3, 0xA4, 0xC3, 0xA4, b'v', b'a', b'd'];
static S_11_32: [symbol; 4] = [b'j', 0xC3, 0xB5, b'i'];
static S_11_33: [symbol; 5] = [b'j', 0xC3, 0xB5, b'i', b'd'];
static S_11_34: [symbol; 6] = [b'j', 0xC3, 0xB5, b'i', b'm', b'e'];
static S_11_35: [symbol; 5] = [b'j', 0xC3, 0xB5, b'i', b'n'];
static S_11_36: [symbol; 6] = [b'j', 0xC3, 0xB5, b'i', b't', b'e'];
static S_11_37: [symbol; 4] = [b'k', b'e', b'e', b'b'];
static S_11_38: [symbol; 4] = [b'k', b'e', b'e', b'd'];
static S_11_39: [symbol; 8] = [b'k', b'e', b'e', b'd', b'a', b'k', b's', b'e'];
static S_11_40: [symbol; 5] = [b'k', b'e', b'e', b'k', b's'];
static S_11_41: [symbol; 7] = [b'k', b'e', b'e', b'k', b's', b'i', b'd'];
static S_11_42: [symbol; 8] = [b'k', b'e', b'e', b'k', b's', b'i', b'm', b'e'];
static S_11_43: [symbol; 7] = [b'k', b'e', b'e', b'k', b's', b'i', b'n'];
static S_11_44: [symbol; 8] = [b'k', b'e', b'e', b'k', b's', b'i', b't', b'e'];
static S_11_45: [symbol; 5] = [b'k', b'e', b'e', b'm', b'a'];
static S_11_46: [symbol; 7] = [b'k', b'e', b'e', b'm', b'a', b't', b'a'];
static S_11_47: [symbol; 5] = [b'k', b'e', b'e', b'm', b'e'];
static S_11_48: [symbol; 4] = [b'k', b'e', b'e', b'n'];
static S_11_49: [symbol; 4] = [b'k', b'e', b'e', b's'];
static S_11_50: [symbol; 5] = [b'k', b'e', b'e', b't', b'a'];
static S_11_51: [symbol; 5] = [b'k', b'e', b'e', b't', b'e'];
static S_11_52: [symbol; 6] = [b'k', b'e', b'e', b'v', b'a', b'd'];
static S_11_53: [symbol; 5] = [b'k', 0xC3, 0xA4, b'i', b'a'];
static S_11_54: [symbol; 8] = [b'k', 0xC3, 0xA4, b'i', b'a', b'k', b's', b'e'];
static S_11_55: [symbol; 5] = [b'k', 0xC3, 0xA4, b'i', b'b'];
static S_11_56: [symbol; 5] = [b'k', 0xC3, 0xA4, b'i', b'd'];
static S_11_57: [symbol; 6] = [b'k', 0xC3, 0xA4, b'i', b'd', b'i'];
static S_11_58: [symbol; 6] = [b'k', 0xC3, 0xA4, b'i', b'k', b's'];
static S_11_59: [symbol; 8] = [b'k', 0xC3, 0xA4, b'i', b'k', b's', b'i', b'd'];
static S_11_60: [symbol; 9] = [b'k', 0xC3, 0xA4, b'i', b'k', b's', b'i', b'm', b'e'];
static S_11_61: [symbol; 8] = [b'k', 0xC3, 0xA4, b'i', b'k', b's', b'i', b'n'];
static S_11_62: [symbol; 9] = [b'k', 0xC3, 0xA4, b'i', b'k', b's', b'i', b't', b'e'];
static S_11_63: [symbol; 6] = [b'k', 0xC3, 0xA4, b'i', b'm', b'a'];
static S_11_64: [symbol; 8] = [b'k', 0xC3, 0xA4, b'i', b'm', b'a', b't', b'a'];
static S_11_65: [symbol; 6] = [b'k', 0xC3, 0xA4, b'i', b'm', b'e'];
static S_11_66: [symbol; 5] = [b'k', 0xC3, 0xA4, b'i', b'n'];
static S_11_67: [symbol; 5] = [b'k', 0xC3, 0xA4, b'i', b's'];
static S_11_68: [symbol; 6] = [b'k', 0xC3, 0xA4, b'i', b't', b'e'];
static S_11_69: [symbol; 7] = [b'k', 0xC3, 0xA4, b'i', b'v', b'a', b'd'];
static S_11_70: [symbol; 4] = [b'l', b'a', b'o', b'b'];
static S_11_71: [symbol; 4] = [b'l', b'a', b'o', b'd'];
static S_11_72: [symbol; 5] = [b'l', b'a', b'o', b'k', b's'];
static S_11_73: [symbol; 7] = [b'l', b'a', b'o', b'k', b's', b'i', b'd'];
static S_11_74: [symbol; 8] = [b'l', b'a', b'o', b'k', b's', b'i', b'm', b'e'];
static S_11_75: [symbol; 7] = [b'l', b'a', b'o', b'k', b's', b'i', b'n'];
static S_11_76: [symbol; 8] = [b'l', b'a', b'o', b'k', b's', b'i', b't', b'e'];
static S_11_77: [symbol; 5] = [b'l', b'a', b'o', b'm', b'e'];
static S_11_78: [symbol; 4] = [b'l', b'a', b'o', b'n'];
static S_11_79: [symbol; 5] = [b'l', b'a', b'o', b't', b'e'];
static S_11_80: [symbol; 6] = [b'l', b'a', b'o', b'v', b'a', b'd'];
static S_11_81: [symbol; 4] = [b'l', b'o', b'e', b'b'];
static S_11_82: [symbol; 4] = [b'l', b'o', b'e', b'd'];
static S_11_83: [symbol; 5] = [b'l', b'o', b'e', b'k', b's'];
static S_11_84: [symbol; 7] = [b'l', b'o', b'e', b'k', b's', b'i', b'd'];
static S_11_85: [symbol; 8] = [b'l', b'o', b'e', b'k', b's', b'i', b'm', b'e'];
static S_11_86: [symbol; 7] = [b'l', b'o', b'e', b'k', b's', b'i', b'n'];
static S_11_87: [symbol; 8] = [b'l', b'o', b'e', b'k', b's', b'i', b't', b'e'];
static S_11_88: [symbol; 5] = [b'l', b'o', b'e', b'm', b'e'];
static S_11_89: [symbol; 4] = [b'l', b'o', b'e', b'n'];
static S_11_90: [symbol; 5] = [b'l', b'o', b'e', b't', b'e'];
static S_11_91: [symbol; 6] = [b'l', b'o', b'e', b'v', b'a', b'd'];
static S_11_92: [symbol; 4] = [b'l', b'o', b'o', b'b'];
static S_11_93: [symbol; 4] = [b'l', b'o', b'o', b'd'];
static S_11_94: [symbol; 5] = [b'l', b'o', b'o', b'd', b'i'];
static S_11_95: [symbol; 5] = [b'l', b'o', b'o', b'k', b's'];
static S_11_96: [symbol; 7] = [b'l', b'o', b'o', b'k', b's', b'i', b'd'];
static S_11_97: [symbol; 8] = [b'l', b'o', b'o', b'k', b's', b'i', b'm', b'e'];
static S_11_98: [symbol; 7] = [b'l', b'o', b'o', b'k', b's', b'i', b'n'];
static S_11_99: [symbol; 8] = [b'l', b'o', b'o', b'k', b's', b'i', b't', b'e'];
static S_11_100: [symbol; 5] = [b'l', b'o', b'o', b'm', b'a'];
static S_11_101: [symbol; 7] = [b'l', b'o', b'o', b'm', b'a', b't', b'a'];
static S_11_102: [symbol; 5] = [b'l', b'o', b'o', b'm', b'e'];
static S_11_103: [symbol; 4] = [b'l', b'o', b'o', b'n'];
static S_11_104: [symbol; 5] = [b'l', b'o', b'o', b't', b'e'];
static S_11_105: [symbol; 6] = [b'l', b'o', b'o', b'v', b'a', b'd'];
static S_11_106: [symbol; 4] = [b'l', b'u', b'u', b'a'];
static S_11_107: [symbol; 7] = [b'l', b'u', b'u', b'a', b'k', b's', b'e'];
static S_11_108: [symbol; 4] = [b'l', 0xC3, 0xB5, b'i'];
static S_11_109: [symbol; 5] = [b'l', 0xC3, 0xB5, b'i', b'd'];
static S_11_110: [symbol; 6] = [b'l', 0xC3, 0xB5, b'i', b'm', b'e'];
static S_11_111: [symbol; 5] = [b'l', 0xC3, 0xB5, b'i', b'n'];
static S_11_112: [symbol; 6] = [b'l', 0xC3, 0xB5, b'i', b't', b'e'];
static S_11_113: [symbol; 6] = [b'l', 0xC3, 0xB6, 0xC3, 0xB6, b'b'];
static S_11_114: [symbol; 6] = [b'l', 0xC3, 0xB6, 0xC3, 0xB6, b'd'];
static S_11_115: [symbol; 10] = [b'l', 0xC3, 0xB6, 0xC3, 0xB6, b'd', b'a', b'k', b's', b'e'];
static S_11_116: [symbol; 7] = [b'l', 0xC3, 0xB6, 0xC3, 0xB6, b'd', b'i'];
static S_11_117: [symbol; 7] = [b'l', 0xC3, 0xB6, 0xC3, 0xB6, b'k', b's'];
static S_11_118: [symbol; 9] = [b'l', 0xC3, 0xB6, 0xC3, 0xB6, b'k', b's', b'i', b'd'];
static S_11_119: [symbol; 10] = [b'l', 0xC3, 0xB6, 0xC3, 0xB6, b'k', b's', b'i', b'm', b'e'];
static S_11_120: [symbol; 9] = [b'l', 0xC3, 0xB6, 0xC3, 0xB6, b'k', b's', b'i', b'n'];
static S_11_121: [symbol; 10] = [b'l', 0xC3, 0xB6, 0xC3, 0xB6, b'k', b's', b'i', b't', b'e'];
static S_11_122: [symbol; 7] = [b'l', 0xC3, 0xB6, 0xC3, 0xB6, b'm', b'a'];
static S_11_123: [symbol; 9] = [b'l', 0xC3, 0xB6, 0xC3, 0xB6, b'm', b'a', b't', b'a'];
static S_11_124: [symbol; 7] = [b'l', 0xC3, 0xB6, 0xC3, 0xB6, b'm', b'e'];
static S_11_125: [symbol; 6] = [b'l', 0xC3, 0xB6, 0xC3, 0xB6, b'n'];
static S_11_126: [symbol; 7] = [b'l', 0xC3, 0xB6, 0xC3, 0xB6, b't', b'e'];
static S_11_127: [symbol; 8] = [b'l', 0xC3, 0xB6, 0xC3, 0xB6, b'v', b'a', b'd'];
static S_11_128: [symbol; 6] = [b'l', 0xC3, 0xBC, 0xC3, 0xBC, b'a'];
static S_11_129: [symbol; 9] = [b'l', 0xC3, 0xBC, 0xC3, 0xBC, b'a', b'k', b's', b'e'];
static S_11_130: [symbol; 6] = [b'm', 0xC3, 0xBC, 0xC3, 0xBC, b'a'];
static S_11_131: [symbol; 9] = [b'm', 0xC3, 0xBC, 0xC3, 0xBC, b'a', b'k', b's', b'e'];
static S_11_132: [symbol; 6] = [b'm', 0xC3, 0xBC, 0xC3, 0xBC, b'b'];
static S_11_133: [symbol; 6] = [b'm', 0xC3, 0xBC, 0xC3, 0xBC, b'd'];
static S_11_134: [symbol; 7] = [b'm', 0xC3, 0xBC, 0xC3, 0xBC, b'd', b'i'];
static S_11_135: [symbol; 7] = [b'm', 0xC3, 0xBC, 0xC3, 0xBC, b'k', b's'];
static S_11_136: [symbol; 9] = [b'm', 0xC3, 0xBC, 0xC3, 0xBC, b'k', b's', b'i', b'd'];
static S_11_137: [symbol; 10] = [b'm', 0xC3, 0xBC, 0xC3, 0xBC, b'k', b's', b'i', b'm', b'e'];
static S_11_138: [symbol; 9] = [b'm', 0xC3, 0xBC, 0xC3, 0xBC, b'k', b's', b'i', b'n'];
static S_11_139: [symbol; 10] = [b'm', 0xC3, 0xBC, 0xC3, 0xBC, b'k', b's', b'i', b't', b'e'];
static S_11_140: [symbol; 7] = [b'm', 0xC3, 0xBC, 0xC3, 0xBC, b'm', b'a'];
static S_11_141: [symbol; 9] = [b'm', 0xC3, 0xBC, 0xC3, 0xBC, b'm', b'a', b't', b'a'];
static S_11_142: [symbol; 7] = [b'm', 0xC3, 0xBC, 0xC3, 0xBC, b'm', b'e'];
static S_11_143: [symbol; 6] = [b'm', 0xC3, 0xBC, 0xC3, 0xBC, b'n'];
static S_11_144: [symbol; 6] = [b'm', 0xC3, 0xBC, 0xC3, 0xBC, b's'];
static S_11_145: [symbol; 7] = [b'm', 0xC3, 0xBC, 0xC3, 0xBC, b't', b'e'];
static S_11_146: [symbol; 8] = [b'm', 0xC3, 0xBC, 0xC3, 0xBC, b'v', b'a', b'd'];
static S_11_147: [symbol; 5] = [b'n', 0xC3, 0xA4, b'e', b'b'];
static S_11_148: [symbol; 5] = [b'n', 0xC3, 0xA4, b'e', b'd'];
static S_11_149: [symbol; 6] = [b'n', 0xC3, 0xA4, b'e', b'k', b's'];
static S_11_150: [symbol; 8] = [b'n', 0xC3, 0xA4, b'e', b'k', b's', b'i', b'd'];
static S_11_151: [symbol; 9] = [b'n', 0xC3, 0xA4, b'e', b'k', b's', b'i', b'm', b'e'];
static S_11_152: [symbol; 8] = [b'n', 0xC3, 0xA4, b'e', b'k', b's', b'i', b'n'];
static S_11_153: [symbol; 9] = [b'n', 0xC3, 0xA4, b'e', b'k', b's', b'i', b't', b'e'];
static S_11_154: [symbol; 6] = [b'n', 0xC3, 0xA4, b'e', b'm', b'e'];
static S_11_155: [symbol; 5] = [b'n', 0xC3, 0xA4, b'e', b'n'];
static S_11_156: [symbol; 6] = [b'n', 0xC3, 0xA4, b'e', b't', b'e'];
static S_11_157: [symbol; 7] = [b'n', 0xC3, 0xA4, b'e', b'v', b'a', b'd'];
static S_11_158: [symbol; 7] = [b'n', 0xC3, 0xA4, b'g', b'e', b'm', b'a'];
static S_11_159: [symbol; 9] = [b'n', 0xC3, 0xA4, b'g', b'e', b'm', b'a', b't', b'a'];
static S_11_160: [symbol; 5] = [b'n', 0xC3, 0xA4, b'h', b'a'];
static S_11_161: [symbol; 8] = [b'n', 0xC3, 0xA4, b'h', b'a', b'k', b's', b'e'];
static S_11_162: [symbol; 6] = [b'n', 0xC3, 0xA4, b'h', b't', b'i'];
static S_11_163: [symbol; 5] = [b'p', 0xC3, 0xB5, b'e', b'b'];
static S_11_164: [symbol; 5] = [b'p', 0xC3, 0xB5, b'e', b'd'];
static S_11_165: [symbol; 6] = [b'p', 0xC3, 0xB5, b'e', b'k', b's'];
static S_11_166: [symbol; 8] = [b'p', 0xC3, 0xB5, b'e', b'k', b's', b'i', b'd'];
static S_11_167: [symbol; 9] = [b'p', 0xC3, 0xB5, b'e', b'k', b's', b'i', b'm', b'e'];
static S_11_168: [symbol; 8] = [b'p', 0xC3, 0xB5, b'e', b'k', b's', b'i', b'n'];
static S_11_169: [symbol; 9] = [b'p', 0xC3, 0xB5, b'e', b'k', b's', b'i', b't', b'e'];
static S_11_170: [symbol; 6] = [b'p', 0xC3, 0xB5, b'e', b'm', b'e'];
static S_11_171: [symbol; 5] = [b'p', 0xC3, 0xB5, b'e', b'n'];
static S_11_172: [symbol; 6] = [b'p', 0xC3, 0xB5, b'e', b't', b'e'];
static S_11_173: [symbol; 7] = [b'p', 0xC3, 0xB5, b'e', b'v', b'a', b'd'];
static S_11_174: [symbol; 4] = [b's', b'a', b'a', b'b'];
static S_11_175: [symbol; 4] = [b's', b'a', b'a', b'd'];
static S_11_176: [symbol; 5] = [b's', b'a', b'a', b'd', b'a'];
static S_11_177: [symbol; 8] = [b's', b'a', b'a', b'd', b'a', b'k', b's', b'e'];
static S_11_178: [symbol; 5] = [b's', b'a', b'a', b'd', b'i'];
static S_11_179: [symbol; 5] = [b's', b'a', b'a', b'k', b's'];
static S_11_180: [symbol; 7] = [b's', b'a', b'a', b'k', b's', b'i', b'd'];
static S_11_181: [symbol; 8] = [b's', b'a', b'a', b'k', b's', b'i', b'm', b'e'];
static S_11_182: [symbol; 7] = [b's', b'a', b'a', b'k', b's', b'i', b'n'];
static S_11_183: [symbol; 8] = [b's', b'a', b'a', b'k', b's', b'i', b't', b'e'];
static S_11_184: [symbol; 5] = [b's', b'a', b'a', b'm', b'a'];
static S_11_185: [symbol; 7] = [b's', b'a', b'a', b'm', b'a', b't', b'a'];
static S_11_186: [symbol; 5] = [b's', b'a', b'a', b'm', b'e'];
static S_11_187: [symbol; 4] = [b's', b'a', b'a', b'n'];
static S_11_188: [symbol; 5] = [b's', b'a', b'a', b't', b'e'];
static S_11_189: [symbol; 6] = [b's', b'a', b'a', b'v', b'a', b'd'];
static S_11_190: [symbol; 3] = [b's', b'a', b'i'];
static S_11_191: [symbol; 4] = [b's', b'a', b'i', b'd'];
static S_11_192: [symbol; 5] = [b's', b'a', b'i', b'm', b'e'];
static S_11_193: [symbol; 4] = [b's', b'a', b'i', b'n'];
static S_11_194: [symbol; 5] = [b's', b'a', b'i', b't', b'e'];
static S_11_195: [symbol; 4] = [b's', 0xC3, 0xB5, b'i'];
static S_11_196: [symbol; 5] = [b's', 0xC3, 0xB5, b'i', b'd'];
static S_11_197: [symbol; 6] = [b's', 0xC3, 0xB5, b'i', b'm', b'e'];
static S_11_198: [symbol; 5] = [b's', 0xC3, 0xB5, b'i', b'n'];
static S_11_199: [symbol; 6] = [b's', 0xC3, 0xB5, b'i', b't', b'e'];
static S_11_200: [symbol; 6] = [b's', 0xC3, 0xB6, 0xC3, 0xB6, b'b'];
static S_11_201: [symbol; 6] = [b's', 0xC3, 0xB6, 0xC3, 0xB6, b'd'];
static S_11_202: [symbol; 10] = [b's', 0xC3, 0xB6, 0xC3, 0xB6, b'd', b'a', b'k', b's', b'e'];
static S_11_203: [symbol; 7] = [b's', 0xC3, 0xB6, 0xC3, 0xB6, b'd', b'i'];
static S_11_204: [symbol; 7] = [b's', 0xC3, 0xB6, 0xC3, 0xB6, b'k', b's'];
static S_11_205: [symbol; 9] = [b's', 0xC3, 0xB6, 0xC3, 0xB6, b'k', b's', b'i', b'd'];
static S_11_206: [symbol; 10] = [b's', 0xC3, 0xB6, 0xC3, 0xB6, b'k', b's', b'i', b'm', b'e'];
static S_11_207: [symbol; 9] = [b's', 0xC3, 0xB6, 0xC3, 0xB6, b'k', b's', b'i', b'n'];
static S_11_208: [symbol; 10] = [b's', 0xC3, 0xB6, 0xC3, 0xB6, b'k', b's', b'i', b't', b'e'];
static S_11_209: [symbol; 7] = [b's', 0xC3, 0xB6, 0xC3, 0xB6, b'm', b'a'];
static S_11_210: [symbol; 9] = [b's', 0xC3, 0xB6, 0xC3, 0xB6, b'm', b'a', b't', b'a'];
static S_11_211: [symbol; 7] = [b's', 0xC3, 0xB6, 0xC3, 0xB6, b'm', b'e'];
static S_11_212: [symbol; 6] = [b's', 0xC3, 0xB6, 0xC3, 0xB6, b'n'];
static S_11_213: [symbol; 7] = [b's', 0xC3, 0xB6, 0xC3, 0xB6, b't', b'e'];
static S_11_214: [symbol; 8] = [b's', 0xC3, 0xB6, 0xC3, 0xB6, b'v', b'a', b'd'];
static S_11_215: [symbol; 6] = [b's', 0xC3, 0xBC, 0xC3, 0xBC, b'a'];
static S_11_216: [symbol; 9] = [b's', 0xC3, 0xBC, 0xC3, 0xBC, b'a', b'k', b's', b'e'];
static S_11_217: [symbol; 4] = [b't', b'e', b'e', b'b'];
static S_11_218: [symbol; 4] = [b't', b'e', b'e', b'd'];
static S_11_219: [symbol; 5] = [b't', b'e', b'e', b'k', b's'];
static S_11_220: [symbol; 7] = [b't', b'e', b'e', b'k', b's', b'i', b'd'];
static S_11_221: [symbol; 8] = [b't', b'e', b'e', b'k', b's', b'i', b'm', b'e'];
static S_11_222: [symbol; 7] = [b't', b'e', b'e', b'k', b's', b'i', b'n'];
static S_11_223: [symbol; 8] = [b't', b'e', b'e', b'k', b's', b'i', b't', b'e'];
static S_11_224: [symbol; 5] = [b't', b'e', b'e', b'm', b'e'];
static S_11_225: [symbol; 4] = [b't', b'e', b'e', b'n'];
static S_11_226: [symbol; 5] = [b't', b'e', b'e', b't', b'e'];
static S_11_227: [symbol; 6] = [b't', b'e', b'e', b'v', b'a', b'd'];
static S_11_228: [symbol; 6] = [b't', b'e', b'g', b'e', b'm', b'a'];
static S_11_229: [symbol; 8] = [b't', b'e', b'g', b'e', b'm', b'a', b't', b'a'];
static S_11_230: [symbol; 4] = [b't', b'e', b'h', b'a'];
static S_11_231: [symbol; 7] = [b't', b'e', b'h', b'a', b'k', b's', b'e'];
static S_11_232: [symbol; 5] = [b't', b'e', b'h', b't', b'i'];
static S_11_233: [symbol; 4] = [b't', b'o', b'o', b'b'];
static S_11_234: [symbol; 4] = [b't', b'o', b'o', b'd'];
static S_11_235: [symbol; 5] = [b't', b'o', b'o', b'd', b'i'];
static S_11_236: [symbol; 5] = [b't', b'o', b'o', b'k', b's'];
static S_11_237: [symbol; 7] = [b't', b'o', b'o', b'k', b's', b'i', b'd'];
static S_11_238: [symbol; 8] = [b't', b'o', b'o', b'k', b's', b'i', b'm', b'e'];
static S_11_239: [symbol; 7] = [b't', b'o', b'o', b'k', b's', b'i', b'n'];
static S_11_240: [symbol; 8] = [b't', b'o', b'o', b'k', b's', b'i', b't', b'e'];
static S_11_241: [symbol; 5] = [b't', b'o', b'o', b'm', b'a'];
static S_11_242: [symbol; 7] = [b't', b'o', b'o', b'm', b'a', b't', b'a'];
static S_11_243: [symbol; 5] = [b't', b'o', b'o', b'm', b'e'];
static S_11_244: [symbol; 4] = [b't', b'o', b'o', b'n'];
static S_11_245: [symbol; 5] = [b't', b'o', b'o', b't', b'e'];
static S_11_246: [symbol; 6] = [b't', b'o', b'o', b'v', b'a', b'd'];
static S_11_247: [symbol; 4] = [b't', b'u', b'u', b'a'];
static S_11_248: [symbol; 7] = [b't', b'u', b'u', b'a', b'k', b's', b'e'];
static S_11_249: [symbol; 4] = [b't', 0xC3, 0xB5, b'i'];
static S_11_250: [symbol; 5] = [b't', 0xC3, 0xB5, b'i', b'd'];
static S_11_251: [symbol; 6] = [b't', 0xC3, 0xB5, b'i', b'm', b'e'];
static S_11_252: [symbol; 5] = [b't', 0xC3, 0xB5, b'i', b'n'];
static S_11_253: [symbol; 6] = [b't', 0xC3, 0xB5, b'i', b't', b'e'];
static S_11_254: [symbol; 4] = [b'v', b'i', b'i', b'a'];
static S_11_255: [symbol; 7] = [b'v', b'i', b'i', b'a', b'k', b's', b'e'];
static S_11_256: [symbol; 4] = [b'v', b'i', b'i', b'b'];
static S_11_257: [symbol; 4] = [b'v', b'i', b'i', b'd'];
static S_11_258: [symbol; 5] = [b'v', b'i', b'i', b'd', b'i'];
static S_11_259: [symbol; 5] = [b'v', b'i', b'i', b'k', b's'];
static S_11_260: [symbol; 7] = [b'v', b'i', b'i', b'k', b's', b'i', b'd'];
static S_11_261: [symbol; 8] = [b'v', b'i', b'i', b'k', b's', b'i', b'm', b'e'];
static S_11_262: [symbol; 7] = [b'v', b'i', b'i', b'k', b's', b'i', b'n'];
static S_11_263: [symbol; 8] = [b'v', b'i', b'i', b'k', b's', b'i', b't', b'e'];
static S_11_264: [symbol; 5] = [b'v', b'i', b'i', b'm', b'a'];
static S_11_265: [symbol; 7] = [b'v', b'i', b'i', b'm', b'a', b't', b'a'];
static S_11_266: [symbol; 5] = [b'v', b'i', b'i', b'm', b'e'];
static S_11_267: [symbol; 4] = [b'v', b'i', b'i', b'n'];
static S_11_268: [symbol; 7] = [b'v', b'i', b'i', b's', b'i', b'm', b'e'];
static S_11_269: [symbol; 6] = [b'v', b'i', b'i', b's', b'i', b'n'];
static S_11_270: [symbol; 7] = [b'v', b'i', b'i', b's', b'i', b't', b'e'];
static S_11_271: [symbol; 5] = [b'v', b'i', b'i', b't', b'e'];
static S_11_272: [symbol; 6] = [b'v', b'i', b'i', b'v', b'a', b'd'];
static S_11_273: [symbol; 5] = [b'v', 0xC3, 0xB5, b'i', b'b'];
static S_11_274: [symbol; 5] = [b'v', 0xC3, 0xB5, b'i', b'd'];
static S_11_275: [symbol; 6] = [b'v', 0xC3, 0xB5, b'i', b'd', b'a'];
static S_11_276: [symbol; 9] = [b'v', 0xC3, 0xB5, b'i', b'd', b'a', b'k', b's', b'e'];
static S_11_277: [symbol; 6] = [b'v', 0xC3, 0xB5, b'i', b'd', b'i'];
static S_11_278: [symbol; 6] = [b'v', 0xC3, 0xB5, b'i', b'k', b's'];
static S_11_279: [symbol; 8] = [b'v', 0xC3, 0xB5, b'i', b'k', b's', b'i', b'd'];
static S_11_280: [symbol; 9] = [b'v', 0xC3, 0xB5, b'i', b'k', b's', b'i', b'm', b'e'];
static S_11_281: [symbol; 8] = [b'v', 0xC3, 0xB5, b'i', b'k', b's', b'i', b'n'];
static S_11_282: [symbol; 9] = [b'v', 0xC3, 0xB5, b'i', b'k', b's', b'i', b't', b'e'];
static S_11_283: [symbol; 6] = [b'v', 0xC3, 0xB5, b'i', b'm', b'a'];
static S_11_284: [symbol; 8] = [b'v', 0xC3, 0xB5, b'i', b'm', b'a', b't', b'a'];
static S_11_285: [symbol; 6] = [b'v', 0xC3, 0xB5, b'i', b'm', b'e'];
static S_11_286: [symbol; 5] = [b'v', 0xC3, 0xB5, b'i', b'n'];
static S_11_287: [symbol; 5] = [b'v', 0xC3, 0xB5, b'i', b's'];
static S_11_288: [symbol; 6] = [b'v', 0xC3, 0xB5, b'i', b't', b'e'];
static S_11_289: [symbol; 7] = [b'v', 0xC3, 0xB5, b'i', b'v', b'a', b'd'];

static A_11: [among; 290] = [
    among { s_size: 4, s: S_11_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_11_1.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_11_2.as_ptr(), substring_i: 1, result: 1, function: None },
    among { s_size: 5, s: S_11_3.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 7, s: S_11_4.as_ptr(), substring_i: 3, result: 1, function: None },
    among { s_size: 5, s: S_11_5.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_11_6.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_11_7.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_11_8.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_11_9.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 7, s: S_11_10.as_ptr(), substring_i: 9, result: 1, function: None },
    among { s_size: 4, s: S_11_11.as_ptr(), substring_i: -1, result: 12, function: None },
    among { s_size: 5, s: S_11_12.as_ptr(), substring_i: 11, result: 12, function: None },
    among { s_size: 6, s: S_11_13.as_ptr(), substring_i: 11, result: 12, function: None },
    among { s_size: 5, s: S_11_14.as_ptr(), substring_i: 11, result: 12, function: None },
    among { s_size: 6, s: S_11_15.as_ptr(), substring_i: 11, result: 12, function: None },
    among { s_size: 6, s: S_11_16.as_ptr(), substring_i: -1, result: 12, function: None },
    among { s_size: 6, s: S_11_17.as_ptr(), substring_i: -1, result: 12, function: None },
    among { s_size: 7, s: S_11_18.as_ptr(), substring_i: 17, result: 12, function: None },
    among { s_size: 10, s: S_11_19.as_ptr(), substring_i: 18, result: 12, function: None },
    among { s_size: 7, s: S_11_20.as_ptr(), substring_i: 17, result: 12, function: None },
    among { s_size: 7, s: S_11_21.as_ptr(), substring_i: -1, result: 12, function: None },
    among { s_size: 9, s: S_11_22.as_ptr(), substring_i: 21, result: 12, function: None },
    among { s_size: 10, s: S_11_23.as_ptr(), substring_i: 21, result: 12, function: None },
    among { s_size: 9, s: S_11_24.as_ptr(), substring_i: 21, result: 12, function: None },
    among { s_size: 10, s: S_11_25.as_ptr(), substring_i: 21, result: 12, function: None },
    among { s_size: 7, s: S_11_26.as_ptr(), substring_i: -1, result: 12, function: None },
    among { s_size: 9, s: S_11_27.as_ptr(), substring_i: 26, result: 12, function: None },
    among { s_size: 7, s: S_11_28.as_ptr(), substring_i: -1, result: 12, function: None },
    among { s_size: 6, s: S_11_29.as_ptr(), substring_i: -1, result: 12, function: None },
    among { s_size: 7, s: S_11_30.as_ptr(), substring_i: -1, result: 12, function: None },
    among { s_size: 8, s: S_11_31.as_ptr(), substring_i: -1, result: 12, function: None },
    among { s_size: 4, s: S_11_32.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_11_33.as_ptr(), substring_i: 32, result: 1, function: None },
    among { s_size: 6, s: S_11_34.as_ptr(), substring_i: 32, result: 1, function: None },
    among { s_size: 5, s: S_11_35.as_ptr(), substring_i: 32, result: 1, function: None },
    among { s_size: 6, s: S_11_36.as_ptr(), substring_i: 32, result: 1, function: None },
    among { s_size: 4, s: S_11_37.as_ptr(), substring_i: -1, result: 4, function: None },
    among { s_size: 4, s: S_11_38.as_ptr(), substring_i: -1, result: 4, function: None },
    among { s_size: 8, s: S_11_39.as_ptr(), substring_i: 38, result: 4, function: None },
    among { s_size: 5, s: S_11_40.as_ptr(), substring_i: -1, result: 4, function: None },
    among { s_size: 7, s: S_11_41.as_ptr(), substring_i: 40, result: 4, function: None },
    among { s_size: 8, s: S_11_42.as_ptr(), substring_i: 40, result: 4, function: None },
    among { s_size: 7, s: S_11_43.as_ptr(), substring_i: 40, result: 4, function: None },
    among { s_size: 8, s: S_11_44.as_ptr(), substring_i: 40, result: 4, function: None },
    among { s_size: 5, s: S_11_45.as_ptr(), substring_i: -1, result: 4, function: None },
    among { s_size: 7, s: S_11_46.as_ptr(), substring_i: 45, result: 4, function: None },
    among { s_size: 5, s: S_11_47.as_ptr(), substring_i: -1, result: 4, function: None },
    among { s_size: 4, s: S_11_48.as_ptr(), substring_i: -1, result: 4, function: None },
    among { s_size: 4, s: S_11_49.as_ptr(), substring_i: -1, result: 4, function: None },
    among { s_size: 5, s: S_11_50.as_ptr(), substring_i: -1, result: 4, function: None },
    among { s_size: 5, s: S_11_51.as_ptr(), substring_i: -1, result: 4, function: None },
    among { s_size: 6, s: S_11_52.as_ptr(), substring_i: -1, result: 4, function: None },
    among { s_size: 5, s: S_11_53.as_ptr(), substring_i: -1, result: 8, function: None },
    among { s_size: 8, s: S_11_54.as_ptr(), substring_i: 53, result: 8, function: None },
    among { s_size: 5, s: S_11_55.as_ptr(), substring_i: -1, result: 8, function: None },
    among { s_size: 5, s: S_11_56.as_ptr(), substring_i: -1, result: 8, function: None },
    among { s_size: 6, s: S_11_57.as_ptr(), substring_i: 56, result: 8, function: None },
    among { s_size: 6, s: S_11_58.as_ptr(), substring_i: -1, result: 8, function: None },
    among { s_size: 8, s: S_11_59.as_ptr(), substring_i: 58, result: 8, function: None },
    among { s_size: 9, s: S_11_60.as_ptr(), substring_i: 58, result: 8, function: None },
    among { s_size: 8, s: S_11_61.as_ptr(), substring_i: 58, result: 8, function: None },
    among { s_size: 9, s: S_11_62.as_ptr(), substring_i: 58, result: 8, function: None },
    among { s_size: 6, s: S_11_63.as_ptr(), substring_i: -1, result: 8, function: None },
    among { s_size: 8, s: S_11_64.as_ptr(), substring_i: 63, result: 8, function: None },
    among { s_size: 6, s: S_11_65.as_ptr(), substring_i: -1, result: 8, function: None },
    among { s_size: 5, s: S_11_66.as_ptr(), substring_i: -1, result: 8, function: None },
    among { s_size: 5, s: S_11_67.as_ptr(), substring_i: -1, result: 8, function: None },
    among { s_size: 6, s: S_11_68.as_ptr(), substring_i: -1, result: 8, function: None },
    among { s_size: 7, s: S_11_69.as_ptr(), substring_i: -1, result: 8, function: None },
    among { s_size: 4, s: S_11_70.as_ptr(), substring_i: -1, result: 16, function: None },
    among { s_size: 4, s: S_11_71.as_ptr(), substring_i: -1, result: 16, function: None },
    among { s_size: 5, s: S_11_72.as_ptr(), substring_i: -1, result: 16, function: None },
    among { s_size: 7, s: S_11_73.as_ptr(), substring_i: 72, result: 16, function: None },
    among { s_size: 8, s: S_11_74.as_ptr(), substring_i: 72, result: 16, function: None },
    among { s_size: 7, s: S_11_75.as_ptr(), substring_i: 72, result: 16, function: None },
    among { s_size: 8, s: S_11_76.as_ptr(), substring_i: 72, result: 16, function: None },
    among { s_size: 5, s: S_11_77.as_ptr(), substring_i: -1, result: 16, function: None },
    among { s_size: 4, s: S_11_78.as_ptr(), substring_i: -1, result: 16, function: None },
    among { s_size: 5, s: S_11_79.as_ptr(), substring_i: -1, result: 16, function: None },
    among { s_size: 6, s: S_11_80.as_ptr(), substring_i: -1, result: 16, function: None },
    among { s_size: 4, s: S_11_81.as_ptr(), substring_i: -1, result: 14, function: None },
    among { s_size: 4, s: S_11_82.as_ptr(), substring_i: -1, result: 14, function: None },
    among { s_size: 5, s: S_11_83.as_ptr(), substring_i: -1, result: 14, function: None },
    among { s_size: 7, s: S_11_84.as_ptr(), substring_i: 83, result: 14, function: None },
    among { s_size: 8, s: S_11_85.as_ptr(), substring_i: 83, result: 14, function: None },
    among { s_size: 7, s: S_11_86.as_ptr(), substring_i: 83, result: 14, function: None },
    among { s_size: 8, s: S_11_87.as_ptr(), substring_i: 83, result: 14, function: None },
    among { s_size: 5, s: S_11_88.as_ptr(), substring_i: -1, result: 14, function: None },
    among { s_size: 4, s: S_11_89.as_ptr(), substring_i: -1, result: 14, function: None },
    among { s_size: 5, s: S_11_90.as_ptr(), substring_i: -1, result: 14, function: None },
    among { s_size: 6, s: S_11_91.as_ptr(), substring_i: -1, result: 14, function: None },
    among { s_size: 4, s: S_11_92.as_ptr(), substring_i: -1, result: 7, function: None },
    among { s_size: 4, s: S_11_93.as_ptr(), substring_i: -1, result: 7, function: None },
    among { s_size: 5, s: S_11_94.as_ptr(), substring_i: 93, result: 7, function: None },
    among { s_size: 5, s: S_11_95.as_ptr(), substring_i: -1, result: 7, function: None },
    among { s_size: 7, s: S_11_96.as_ptr(), substring_i: 95, result: 7, function: None },
    among { s_size: 8, s: S_11_97.as_ptr(), substring_i: 95, result: 7, function: None },
    among { s_size: 7, s: S_11_98.as_ptr(), substring_i: 95, result: 7, function: None },
    among { s_size: 8, s: S_11_99.as_ptr(), substring_i: 95, result: 7, function: None },
    among { s_size: 5, s: S_11_100.as_ptr(), substring_i: -1, result: 7, function: None },
    among { s_size: 7, s: S_11_101.as_ptr(), substring_i: 100, result: 7, function: None },
    among { s_size: 5, s: S_11_102.as_ptr(), substring_i: -1, result: 7, function: None },
    among { s_size: 4, s: S_11_103.as_ptr(), substring_i: -1, result: 7, function: None },
    among { s_size: 5, s: S_11_104.as_ptr(), substring_i: -1, result: 7, function: None },
    among { s_size: 6, s: S_11_105.as_ptr(), substring_i: -1, result: 7, function: None },
    among { s_size: 4, s: S_11_106.as_ptr(), substring_i: -1, result: 7, function: None },
    among { s_size: 7, s: S_11_107.as_ptr(), substring_i: 106, result: 7, function: None },
    among { s_size: 4, s: S_11_108.as_ptr(), substring_i: -1, result: 6, function: None },
    among { s_size: 5, s: S_11_109.as_ptr(), substring_i: 108, result: 6, function: None },
    among { s_size: 6, s: S_11_110.as_ptr(), substring_i: 108, result: 6, function: None },
    among { s_size: 5, s: S_11_111.as_ptr(), substring_i: 108, result: 6, function: None },
    among { s_size: 6, s: S_11_112.as_ptr(), substring_i: 108, result: 6, function: None },
    among { s_size: 6, s: S_11_113.as_ptr(), substring_i: -1, result: 5, function: None },
    among { s_size: 6, s: S_11_114.as_ptr(), substring_i: -1, result: 5, function: None },
    among { s_size: 10, s: S_11_115.as_ptr(), substring_i: 114, result: 5, function: None },
    among { s_size: 7, s: S_11_116.as_ptr(), substring_i: 114, result: 5, function: None },
    among { s_size: 7, s: S_11_117.as_ptr(), substring_i: -1, result: 5, function: None },
    among { s_size: 9, s: S_11_118.as_ptr(), substring_i: 117, result: 5, function: None },
    among { s_size: 10, s: S_11_119.as_ptr(), substring_i: 117, result: 5, function: None },
    among { s_size: 9, s: S_11_120.as_ptr(), substring_i: 117, result: 5, function: None },
    among { s_size: 10, s: S_11_121.as_ptr(), substring_i: 117, result: 5, function: None },
    among { s_size: 7, s: S_11_122.as_ptr(), substring_i: -1, result: 5, function: None },
    among { s_size: 9, s: S_11_123.as_ptr(), substring_i: 122, result: 5, function: None },
    among { s_size: 7, s: S_11_124.as_ptr(), substring_i: -1, result: 5, function: None },
    among { s_size: 6, s: S_11_125.as_ptr(), substring_i: -1, result: 5, function: None },
    among { s_size: 7, s: S_11_126.as_ptr(), substring_i: -1, result: 5, function: None },
    among { s_size: 8, s: S_11_127.as_ptr(), substring_i: -1, result: 5, function: None },
    among { s_size: 6, s: S_11_128.as_ptr(), substring_i: -1, result: 5, function: None },
    among { s_size: 9, s: S_11_129.as_ptr(), substring_i: 128, result: 5, function: None },
    among { s_size: 6, s: S_11_130.as_ptr(), substring_i: -1, result: 13, function: None },
    among { s_size: 9, s: S_11_131.as_ptr(), substring_i: 130, result: 13, function: None },
    among { s_size: 6, s: S_11_132.as_ptr(), substring_i: -1, result: 13, function: None },
    among { s_size: 6, s: S_11_133.as_ptr(), substring_i: -1, result: 13, function: None },
    among { s_size: 7, s: S_11_134.as_ptr(), substring_i: 133, result: 13, function: None },
    among { s_size: 7, s: S_11_135.as_ptr(), substring_i: -1, result: 13, function: None },
    among { s_size: 9, s: S_11_136.as_ptr(), substring_i: 135, result: 13, function: None },
    among { s_size: 10, s: S_11_137.as_ptr(), substring_i: 135, result: 13, function: None },
    among { s_size: 9, s: S_11_138.as_ptr(), substring_i: 135, result: 13, function: None },
    among { s_size: 10, s: S_11_139.as_ptr(), substring_i: 135, result: 13, function: None },
    among { s_size: 7, s: S_11_140.as_ptr(), substring_i: -1, result: 13, function: None },
    among { s_size: 9, s: S_11_141.as_ptr(), substring_i: 140, result: 13, function: None },
    among { s_size: 7, s: S_11_142.as_ptr(), substring_i: -1, result: 13, function: None },
    among { s_size: 6, s: S_11_143.as_ptr(), substring_i: -1, result: 13, function: None },
    among { s_size: 6, s: S_11_144.as_ptr(), substring_i: -1, result: 13, function: None },
    among { s_size: 7, s: S_11_145.as_ptr(), substring_i: -1, result: 13, function: None },
    among { s_size: 8, s: S_11_146.as_ptr(), substring_i: -1, result: 13, function: None },
    among { s_size: 5, s: S_11_147.as_ptr(), substring_i: -1, result: 18, function: None },
    among { s_size: 5, s: S_11_148.as_ptr(), substring_i: -1, result: 18, function: None },
    among { s_size: 6, s: S_11_149.as_ptr(), substring_i: -1, result: 18, function: None },
    among { s_size: 8, s: S_11_150.as_ptr(), substring_i: 149, result: 18, function: None },
    among { s_size: 9, s: S_11_151.as_ptr(), substring_i: 149, result: 18, function: None },
    among { s_size: 8, s: S_11_152.as_ptr(), substring_i: 149, result: 18, function: None },
    among { s_size: 9, s: S_11_153.as_ptr(), substring_i: 149, result: 18, function: None },
    among { s_size: 6, s: S_11_154.as_ptr(), substring_i: -1, result: 18, function: None },
    among { s_size: 5, s: S_11_155.as_ptr(), substring_i: -1, result: 18, function: None },
    among { s_size: 6, s: S_11_156.as_ptr(), substring_i: -1, result: 18, function: None },
    among { s_size: 7, s: S_11_157.as_ptr(), substring_i: -1, result: 18, function: None },
    among { s_size: 7, s: S_11_158.as_ptr(), substring_i: -1, result: 18, function: None },
    among { s_size: 9, s: S_11_159.as_ptr(), substring_i: 158, result: 18, function: None },
    among { s_size: 5, s: S_11_160.as_ptr(), substring_i: -1, result: 18, function: None },
    among { s_size: 8, s: S_11_161.as_ptr(), substring_i: 160, result: 18, function: None },
    among { s_size: 6, s: S_11_162.as_ptr(), substring_i: -1, result: 18, function: None },
    among { s_size: 5, s: S_11_163.as_ptr(), substring_i: -1, result: 15, function: None },
    among { s_size: 5, s: S_11_164.as_ptr(), substring_i: -1, result: 15, function: None },
    among { s_size: 6, s: S_11_165.as_ptr(), substring_i: -1, result: 15, function: None },
    among { s_size: 8, s: S_11_166.as_ptr(), substring_i: 165, result: 15, function: None },
    among { s_size: 9, s: S_11_167.as_ptr(), substring_i: 165, result: 15, function: None },
    among { s_size: 8, s: S_11_168.as_ptr(), substring_i: 165, result: 15, function: None },
    among { s_size: 9, s: S_11_169.as_ptr(), substring_i: 165, result: 15, function: None },
    among { s_size: 6, s: S_11_170.as_ptr(), substring_i: -1, result: 15, function: None },
    among { s_size: 5, s: S_11_171.as_ptr(), substring_i: -1, result: 15, function: None },
    among { s_size: 6, s: S_11_172.as_ptr(), substring_i: -1, result: 15, function: None },
    among { s_size: 7, s: S_11_173.as_ptr(), substring_i: -1, result: 15, function: None },
    among { s_size: 4, s: S_11_174.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 4, s: S_11_175.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 5, s: S_11_176.as_ptr(), substring_i: 175, result: 2, function: None },
    among { s_size: 8, s: S_11_177.as_ptr(), substring_i: 176, result: 2, function: None },
    among { s_size: 5, s: S_11_178.as_ptr(), substring_i: 175, result: 2, function: None },
    among { s_size: 5, s: S_11_179.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 7, s: S_11_180.as_ptr(), substring_i: 179, result: 2, function: None },
    among { s_size: 8, s: S_11_181.as_ptr(), substring_i: 179, result: 2, function: None },
    among { s_size: 7, s: S_11_182.as_ptr(), substring_i: 179, result: 2, function: None },
    among { s_size: 8, s: S_11_183.as_ptr(), substring_i: 179, result: 2, function: None },
    among { s_size: 5, s: S_11_184.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 7, s: S_11_185.as_ptr(), substring_i: 184, result: 2, function: None },
    among { s_size: 5, s: S_11_186.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 4, s: S_11_187.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 5, s: S_11_188.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 6, s: S_11_189.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 3, s: S_11_190.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 4, s: S_11_191.as_ptr(), substring_i: 190, result: 2, function: None },
    among { s_size: 5, s: S_11_192.as_ptr(), substring_i: 190, result: 2, function: None },
    among { s_size: 4, s: S_11_193.as_ptr(), substring_i: 190, result: 2, function: None },
    among { s_size: 5, s: S_11_194.as_ptr(), substring_i: 190, result: 2, function: None },
    among { s_size: 4, s: S_11_195.as_ptr(), substring_i: -1, result: 9, function: None },
    among { s_size: 5, s: S_11_196.as_ptr(), substring_i: 195, result: 9, function: None },
    among { s_size: 6, s: S_11_197.as_ptr(), substring_i: 195, result: 9, function: None },
    among { s_size: 5, s: S_11_198.as_ptr(), substring_i: 195, result: 9, function: None },
    among { s_size: 6, s: S_11_199.as_ptr(), substring_i: 195, result: 9, function: None },
    among { s_size: 6, s: S_11_200.as_ptr(), substring_i: -1, result: 9, function: None },
    among { s_size: 6, s: S_11_201.as_ptr(), substring_i: -1, result: 9, function: None },
    among { s_size: 10, s: S_11_202.as_ptr(), substring_i: 201, result: 9, function: None },
    among { s_size: 7, s: S_11_203.as_ptr(), substring_i: 201, result: 9, function: None },
    among { s_size: 7, s: S_11_204.as_ptr(), substring_i: -1, result: 9, function: None },
    among { s_size: 9, s: S_11_205.as_ptr(), substring_i: 204, result: 9, function: None },
    among { s_size: 10, s: S_11_206.as_ptr(), substring_i: 204, result: 9, function: None },
    among { s_size: 9, s: S_11_207.as_ptr(), substring_i: 204, result: 9, function: None },
    among { s_size: 10, s: S_11_208.as_ptr(), substring_i: 204, result: 9, function: None },
    among { s_size: 7, s: S_11_209.as_ptr(), substring_i: -1, result: 9, function: None },
    among { s_size: 9, s: S_11_210.as_ptr(), substring_i: 209, result: 9, function: None },
    among { s_size: 7, s: S_11_211.as_ptr(), substring_i: -1, result: 9, function: None },
    among { s_size: 6, s: S_11_212.as_ptr(), substring_i: -1, result: 9, function: None },
    among { s_size: 7, s: S_11_213.as_ptr(), substring_i: -1, result: 9, function: None },
    among { s_size: 8, s: S_11_214.as_ptr(), substring_i: -1, result: 9, function: None },
    among { s_size: 6, s: S_11_215.as_ptr(), substring_i: -1, result: 9, function: None },
    among { s_size: 9, s: S_11_216.as_ptr(), substring_i: 215, result: 9, function: None },
    among { s_size: 4, s: S_11_217.as_ptr(), substring_i: -1, result: 17, function: None },
    among { s_size: 4, s: S_11_218.as_ptr(), substring_i: -1, result: 17, function: None },
    among { s_size: 5, s: S_11_219.as_ptr(), substring_i: -1, result: 17, function: None },
    among { s_size: 7, s: S_11_220.as_ptr(), substring_i: 219, result: 17, function: None },
    among { s_size: 8, s: S_11_221.as_ptr(), substring_i: 219, result: 17, function: None },
    among { s_size: 7, s: S_11_222.as_ptr(), substring_i: 219, result: 17, function: None },
    among { s_size: 8, s: S_11_223.as_ptr(), substring_i: 219, result: 17, function: None },
    among { s_size: 5, s: S_11_224.as_ptr(), substring_i: -1, result: 17, function: None },
    among { s_size: 4, s: S_11_225.as_ptr(), substring_i: -1, result: 17, function: None },
    among { s_size: 5, s: S_11_226.as_ptr(), substring_i: -1, result: 17, function: None },
    among { s_size: 6, s: S_11_227.as_ptr(), substring_i: -1, result: 17, function: None },
    among { s_size: 6, s: S_11_228.as_ptr(), substring_i: -1, result: 17, function: None },
    among { s_size: 8, s: S_11_229.as_ptr(), substring_i: 228, result: 17, function: None },
    among { s_size: 4, s: S_11_230.as_ptr(), substring_i: -1, result: 17, function: None },
    among { s_size: 7, s: S_11_231.as_ptr(), substring_i: 230, result: 17, function: None },
    among { s_size: 5, s: S_11_232.as_ptr(), substring_i: -1, result: 17, function: None },
    among { s_size: 4, s: S_11_233.as_ptr(), substring_i: -1, result: 10, function: None },
    among { s_size: 4, s: S_11_234.as_ptr(), substring_i: -1, result: 10, function: None },
    among { s_size: 5, s: S_11_235.as_ptr(), substring_i: 234, result: 10, function: None },
    among { s_size: 5, s: S_11_236.as_ptr(), substring_i: -1, result: 10, function: None },
    among { s_size: 7, s: S_11_237.as_ptr(), substring_i: 236, result: 10, function: None },
    among { s_size: 8, s: S_11_238.as_ptr(), substring_i: 236, result: 10, function: None },
    among { s_size: 7, s: S_11_239.as_ptr(), substring_i: 236, result: 10, function: None },
    among { s_size: 8, s: S_11_240.as_ptr(), substring_i: 236, result: 10, function: None },
    among { s_size: 5, s: S_11_241.as_ptr(), substring_i: -1, result: 10, function: None },
    among { s_size: 7, s: S_11_242.as_ptr(), substring_i: 241, result: 10, function: None },
    among { s_size: 5, s: S_11_243.as_ptr(), substring_i: -1, result: 10, function: None },
    among { s_size: 4, s: S_11_244.as_ptr(), substring_i: -1, result: 10, function: None },
    among { s_size: 5, s: S_11_245.as_ptr(), substring_i: -1, result: 10, function: None },
    among { s_size: 6, s: S_11_246.as_ptr(), substring_i: -1, result: 10, function: None },
    among { s_size: 4, s: S_11_247.as_ptr(), substring_i: -1, result: 10, function: None },
    among { s_size: 7, s: S_11_248.as_ptr(), substring_i: 247, result: 10, function: None },
    among { s_size: 4, s: S_11_249.as_ptr(), substring_i: -1, result: 10, function: None },
    among { s_size: 5, s: S_11_250.as_ptr(), substring_i: 249, result: 10, function: None },
    among { s_size: 6, s: S_11_251.as_ptr(), substring_i: 249, result: 10, function: None },
    among { s_size: 5, s: S_11_252.as_ptr(), substring_i: 249, result: 10, function: None },
    among { s_size: 6, s: S_11_253.as_ptr(), substring_i: 249, result: 10, function: None },
    among { s_size: 4, s: S_11_254.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 7, s: S_11_255.as_ptr(), substring_i: 254, result: 3, function: None },
    among { s_size: 4, s: S_11_256.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 4, s: S_11_257.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 5, s: S_11_258.as_ptr(), substring_i: 257, result: 3, function: None },
    among { s_size: 5, s: S_11_259.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 7, s: S_11_260.as_ptr(), substring_i: 259, result: 3, function: None },
    among { s_size: 8, s: S_11_261.as_ptr(), substring_i: 259, result: 3, function: None },
    among { s_size: 7, s: S_11_262.as_ptr(), substring_i: 259, result: 3, function: None },
    among { s_size: 8, s: S_11_263.as_ptr(), substring_i: 259, result: 3, function: None },
    among { s_size: 5, s: S_11_264.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 7, s: S_11_265.as_ptr(), substring_i: 264, result: 3, function: None },
    among { s_size: 5, s: S_11_266.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 4, s: S_11_267.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 7, s: S_11_268.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 6, s: S_11_269.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 7, s: S_11_270.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 5, s: S_11_271.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 6, s: S_11_272.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 5, s: S_11_273.as_ptr(), substring_i: -1, result: 11, function: None },
    among { s_size: 5, s: S_11_274.as_ptr(), substring_i: -1, result: 11, function: None },
    among { s_size: 6, s: S_11_275.as_ptr(), substring_i: 274, result: 11, function: None },
    among { s_size: 9, s: S_11_276.as_ptr(), substring_i: 275, result: 11, function: None },
    among { s_size: 6, s: S_11_277.as_ptr(), substring_i: 274, result: 11, function: None },
    among { s_size: 6, s: S_11_278.as_ptr(), substring_i: -1, result: 11, function: None },
    among { s_size: 8, s: S_11_279.as_ptr(), substring_i: 278, result: 11, function: None },
    among { s_size: 9, s: S_11_280.as_ptr(), substring_i: 278, result: 11, function: None },
    among { s_size: 8, s: S_11_281.as_ptr(), substring_i: 278, result: 11, function: None },
    among { s_size: 9, s: S_11_282.as_ptr(), substring_i: 278, result: 11, function: None },
    among { s_size: 6, s: S_11_283.as_ptr(), substring_i: -1, result: 11, function: None },
    among { s_size: 8, s: S_11_284.as_ptr(), substring_i: 283, result: 11, function: None },
    among { s_size: 6, s: S_11_285.as_ptr(), substring_i: -1, result: 11, function: None },
    among { s_size: 5, s: S_11_286.as_ptr(), substring_i: -1, result: 11, function: None },
    among { s_size: 5, s: S_11_287.as_ptr(), substring_i: -1, result: 11, function: None },
    among { s_size: 6, s: S_11_288.as_ptr(), substring_i: -1, result: 11, function: None },
    among { s_size: 7, s: S_11_289.as_ptr(), substring_i: -1, result: 11, function: None },
];

// ---------------------------------------------------------------------------
// grouping bit tables
// ---------------------------------------------------------------------------

static G_V1: [c_uchar; 20] = [
    17, 65, 16, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 8, 0, 48, 8,
];

static G_RV: [c_uchar; 3] = [17, 65, 16];

static G_KI: [c_uchar; 36] = [
    117, 66, 6, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    128, 0, 0, 0, 16,
];

static G_GI: [c_uchar; 20] = [
    21, 123, 243, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 8, 0, 48, 8,
];

// ---------------------------------------------------------------------------
// literal strings used by slice_from_s
// ---------------------------------------------------------------------------

static S_0: [symbol; 1] = [b'a'];
static S_1: [symbol; 4] = [b'l', b'a', b's', b'e'];
static S_2: [symbol; 4] = [b'm', b'i', b's', b'e'];
static S_3: [symbol; 4] = [b'l', b'i', b's', b'e'];
static S_4: [symbol; 3] = [b'i', b'k', b'u'];
static S_5: [symbol; 1] = [b'e'];
static S_6: [symbol; 1] = [b't'];
static S_7: [symbol; 1] = [b'k'];
static S_8: [symbol; 1] = [b'p'];
static S_9: [symbol; 1] = [b't'];
static S_10: [symbol; 3] = [b'j', b'o', b'o'];
static S_11: [symbol; 3] = [b's', b'a', b'a'];
static S_12: [symbol; 5] = [b'v', b'i', b'i', b'm', b'a'];
static S_13: [symbol; 5] = [b'k', b'e', b'e', b's', b'i'];
static S_14: [symbol; 5] = [b'l', 0xC3, 0xB6, 0xC3, 0xB6];
static S_15: [symbol; 4] = [b'l', 0xC3, 0xB5, b'i'];
static S_16: [symbol; 3] = [b'l', b'o', b'o'];
static S_17: [symbol; 6] = [b'k', 0xC3, 0xA4, b'i', b's', b'i'];
static S_18: [symbol; 5] = [b's', 0xC3, 0xB6, 0xC3, 0xB6];
static S_19: [symbol; 3] = [b't', b'o', b'o'];
static S_20: [symbol; 6] = [b'v', 0xC3, 0xB5, b'i', b's', b'i'];
static S_21: [symbol; 7] = [b'j', 0xC3, 0xA4, 0xC3, 0xA4, b'm', b'a'];
static S_22: [symbol; 7] = [b'm', 0xC3, 0xBC, 0xC3, 0xBC, b's', b'i'];
static S_23: [symbol; 4] = [b'l', b'u', b'g', b'e'];
static S_24: [symbol; 5] = [b'p', 0xC3, 0xB5, b'd', b'e'];
static S_25: [symbol; 4] = [b'l', b'a', b'd', b'u'];
static S_26: [symbol; 4] = [b't', b'e', b'g', b'i'];
static S_27: [symbol; 5] = [b'n', 0xC3, 0xA4, b'g', b'i'];

// ---------------------------------------------------------------------------
// rules
// ---------------------------------------------------------------------------

unsafe fn r_mark_regions(z: *mut SN_env) -> c_int {
    *(*z).I.offset(0) = (*z).l;

    if out_grouping_U(z, G_V1.as_ptr(), 97, 252, 1) < 0 {
        return 0;
    }

    {
        let ret = in_grouping_U(z, G_V1.as_ptr(), 97, 252, 1);
        if ret < 0 {
            return 0;
        }
        (*z).c += ret;
    }
    *(*z).I.offset(0) = (*z).c;
    1
}

unsafe fn r_emphasis(z: *mut SN_env) -> c_int {
    let among_var;

    {
        let mlimit1;
        if (*z).c < *(*z).I.offset(0) {
            return 0;
        }
        mlimit1 = (*z).lb;
        (*z).lb = *(*z).I.offset(0);
        (*z).ket = (*z).c;
        if (*z).c - 1 <= (*z).lb || *(*z).p.offset(((*z).c - 1) as isize) != 105 {
            (*z).lb = mlimit1;
            return 0;
        }
        among_var = find_among_b(z, A_0.as_ptr(), 2);
        if among_var == 0 {
            (*z).lb = mlimit1;
            return 0;
        }
        (*z).bra = (*z).c;
        (*z).lb = mlimit1;
    }
    {
        let m_test2 = (*z).l - (*z).c;
        {
            let ret = skip_b_utf8((*z).p, (*z).c, (*z).lb, 4);
            if ret < 0 {
                return 0;
            }
            (*z).c = ret;
        }
        (*z).c = (*z).l - m_test2;
    }
    match among_var {
        1 => {
            {
                let m3 = (*z).l - (*z).c;
                if in_grouping_b_U(z, G_GI.as_ptr(), 97, 252, 0) != 0 {
                    return 0;
                }
                (*z).c = (*z).l - m3;
                {
                    let m4 = (*z).l - (*z).c;
                    'lab0: {
                        {
                            let ret = r_LONGV(z);
                            if ret == 0 {
                                break 'lab0;
                            }
                            if ret < 0 {
                                return ret;
                            }
                        }
                        return 0;
                    }
                    // lab0:
                    (*z).c = (*z).l - m4;
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
            if in_grouping_b_U(z, G_KI.as_ptr(), 98, 382, 0) != 0 {
                return 0;
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

unsafe fn r_verb(z: *mut SN_env) -> c_int {
    let among_var;

    {
        let mlimit1;
        if (*z).c < *(*z).I.offset(0) {
            return 0;
        }
        mlimit1 = (*z).lb;
        (*z).lb = *(*z).I.offset(0);
        (*z).ket = (*z).c;
        if (*z).c <= (*z).lb
            || *(*z).p.offset(((*z).c - 1) as isize) as c_int >> 5 != 3
            || (540726 >> (*(*z).p.offset(((*z).c - 1) as isize) as c_int & 0x1f)) & 1 == 0
        {
            (*z).lb = mlimit1;
            return 0;
        }
        among_var = find_among_b(z, A_1.as_ptr(), 21);
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
            let ret = slice_from_s(z, 1, S_0.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        3 => {
            if in_grouping_b_U(z, G_V1.as_ptr(), 97, 252, 0) != 0 {
                return 0;
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

unsafe fn r_LONGV(z: *mut SN_env) -> c_int {
    if find_among_b(z, A_2.as_ptr(), 9) == 0 {
        return 0;
    }
    1
}

unsafe fn r_i_plural(z: *mut SN_env) -> c_int {
    {
        let mlimit1;
        if (*z).c < *(*z).I.offset(0) {
            return 0;
        }
        mlimit1 = (*z).lb;
        (*z).lb = *(*z).I.offset(0);
        (*z).ket = (*z).c;
        if (*z).c <= (*z).lb || *(*z).p.offset(((*z).c - 1) as isize) != 105 {
            (*z).lb = mlimit1;
            return 0;
        }
        if find_among_b(z, A_3.as_ptr(), 1) == 0 {
            (*z).lb = mlimit1;
            return 0;
        }
        (*z).bra = (*z).c;
        (*z).lb = mlimit1;
    }
    if in_grouping_b_U(z, G_RV.as_ptr(), 97, 117, 0) != 0 {
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

unsafe fn r_special_noun_endings(z: *mut SN_env) -> c_int {
    let among_var;

    {
        let mlimit1;
        if (*z).c < *(*z).I.offset(0) {
            return 0;
        }
        mlimit1 = (*z).lb;
        (*z).lb = *(*z).I.offset(0);
        (*z).ket = (*z).c;
        if (*z).c - 3 <= (*z).lb
            || *(*z).p.offset(((*z).c - 1) as isize) as c_int >> 5 != 3
            || (1049120 >> (*(*z).p.offset(((*z).c - 1) as isize) as c_int & 0x1f)) & 1 == 0
        {
            (*z).lb = mlimit1;
            return 0;
        }
        among_var = find_among_b(z, A_4.as_ptr(), 12);
        if among_var == 0 {
            (*z).lb = mlimit1;
            return 0;
        }
        (*z).bra = (*z).c;
        (*z).lb = mlimit1;
    }
    match among_var {
        1 => {
            let ret = slice_from_s(z, 4, S_1.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        2 => {
            let ret = slice_from_s(z, 4, S_2.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        3 => {
            let ret = slice_from_s(z, 4, S_3.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        _ => {}
    }
    1
}

unsafe fn r_case_ending(z: *mut SN_env) -> c_int {
    let among_var;

    {
        let mlimit1;
        if (*z).c < *(*z).I.offset(0) {
            return 0;
        }
        mlimit1 = (*z).lb;
        (*z).lb = *(*z).I.offset(0);
        (*z).ket = (*z).c;
        if (*z).c <= (*z).lb
            || *(*z).p.offset(((*z).c - 1) as isize) as c_int >> 5 != 3
            || (1576994 >> (*(*z).p.offset(((*z).c - 1) as isize) as c_int & 0x1f)) & 1 == 0
        {
            (*z).lb = mlimit1;
            return 0;
        }
        among_var = find_among_b(z, A_5.as_ptr(), 10);
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
            'lab0: {
                'lab1: {
                    if in_grouping_b_U(z, G_RV.as_ptr(), 97, 117, 0) != 0 {
                        break 'lab1;
                    }
                    break 'lab0;
                }
                // lab1:
                (*z).c = (*z).l - m2;
                {
                    let ret = r_LONGV(z);
                    if ret <= 0 {
                        return ret;
                    }
                }
            }
        }
        2 => {
            let m_test3 = (*z).l - (*z).c;
            {
                let ret = skip_b_utf8((*z).p, (*z).c, (*z).lb, 4);
                if ret < 0 {
                    return 0;
                }
                (*z).c = ret;
            }
            (*z).c = (*z).l - m_test3;
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

unsafe fn r_plural_three_first_cases(z: *mut SN_env) -> c_int {
    let mut among_var;

    {
        let mlimit1;
        if (*z).c < *(*z).I.offset(0) {
            return 0;
        }
        mlimit1 = (*z).lb;
        (*z).lb = *(*z).I.offset(0);
        (*z).ket = (*z).c;
        if (*z).c <= (*z).lb
            || (*(*z).p.offset(((*z).c - 1) as isize) != 100
                && *(*z).p.offset(((*z).c - 1) as isize) != 101)
        {
            (*z).lb = mlimit1;
            return 0;
        }
        among_var = find_among_b(z, A_7.as_ptr(), 7);
        if among_var == 0 {
            (*z).lb = mlimit1;
            return 0;
        }
        (*z).bra = (*z).c;
        (*z).lb = mlimit1;
    }
    match among_var {
        1 => {
            let ret = slice_from_s(z, 3, S_4.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        2 => {
            {
                let m2 = (*z).l - (*z).c;
                'lab0: {
                    {
                        let ret = r_LONGV(z);
                        if ret == 0 {
                            break 'lab0;
                        }
                        if ret < 0 {
                            return ret;
                        }
                    }
                    return 0;
                }
                // lab0:
                (*z).c = (*z).l - m2;
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
                let m3 = (*z).l - (*z).c;
                'lab1: {
                    'lab2: {
                        {
                            let m_test4 = (*z).l - (*z).c;
                            {
                                let ret = skip_b_utf8((*z).p, (*z).c, (*z).lb, 4);
                                if ret < 0 {
                                    break 'lab2;
                                }
                                (*z).c = ret;
                            }
                            (*z).c = (*z).l - m_test4;
                        }
                        if (*z).c <= (*z).lb
                            || (*(*z).p.offset(((*z).c - 1) as isize) != 115
                                && *(*z).p.offset(((*z).c - 1) as isize) != 116)
                        {
                            among_var = 2;
                        } else {
                            among_var = find_among_b(z, A_6.as_ptr(), 5);
                        }
                        match among_var {
                            1 => {
                                let ret = slice_from_s(z, 1, S_5.as_ptr());
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
                            _ => {}
                        }
                        break 'lab1;
                    }
                    // lab2:
                    (*z).c = (*z).l - m3;
                    {
                        let ret = slice_from_s(z, 1, S_6.as_ptr());
                        if ret < 0 {
                            return ret;
                        }
                    }
                }
            }
        }
        4 => {
            {
                let m5 = (*z).l - (*z).c;
                'lab3: {
                    'lab4: {
                        if in_grouping_b_U(z, G_RV.as_ptr(), 97, 117, 0) != 0 {
                            break 'lab4;
                        }
                        break 'lab3;
                    }
                    // lab4:
                    (*z).c = (*z).l - m5;
                    {
                        let ret = r_LONGV(z);
                        if ret <= 0 {
                            return ret;
                        }
                    }
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

unsafe fn r_nu(z: *mut SN_env) -> c_int {
    {
        let mlimit1;
        if (*z).c < *(*z).I.offset(0) {
            return 0;
        }
        mlimit1 = (*z).lb;
        (*z).lb = *(*z).I.offset(0);
        (*z).ket = (*z).c;
        if (*z).c - 1 <= (*z).lb
            || (*(*z).p.offset(((*z).c - 1) as isize) != 97
                && *(*z).p.offset(((*z).c - 1) as isize) != 117)
        {
            (*z).lb = mlimit1;
            return 0;
        }
        if find_among_b(z, A_8.as_ptr(), 4) == 0 {
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

unsafe fn r_undouble_kpt(z: *mut SN_env) -> c_int {
    let among_var;
    if in_grouping_b_U(z, G_V1.as_ptr(), 97, 252, 0) != 0 {
        return 0;
    }
    if *(*z).I.offset(0) > (*z).c {
        return 0;
    }
    (*z).ket = (*z).c;
    if (*z).c - 1 <= (*z).lb
        || *(*z).p.offset(((*z).c - 1) as isize) as c_int >> 5 != 3
        || (1116160 >> (*(*z).p.offset(((*z).c - 1) as isize) as c_int & 0x1f)) & 1 == 0
    {
        return 0;
    }
    among_var = find_among_b(z, A_9.as_ptr(), 3);
    if among_var == 0 {
        return 0;
    }
    (*z).bra = (*z).c;
    match among_var {
        1 => {
            let ret = slice_from_s(z, 1, S_7.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        2 => {
            let ret = slice_from_s(z, 1, S_8.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        3 => {
            let ret = slice_from_s(z, 1, S_9.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        _ => {}
    }
    1
}

unsafe fn r_degrees(z: *mut SN_env) -> c_int {
    let among_var;

    {
        let mlimit1;
        if (*z).c < *(*z).I.offset(0) {
            return 0;
        }
        mlimit1 = (*z).lb;
        (*z).lb = *(*z).I.offset(0);
        (*z).ket = (*z).c;
        if (*z).c <= (*z).lb
            || *(*z).p.offset(((*z).c - 1) as isize) as c_int >> 5 != 3
            || (8706 >> (*(*z).p.offset(((*z).c - 1) as isize) as c_int & 0x1f)) & 1 == 0
        {
            (*z).lb = mlimit1;
            return 0;
        }
        among_var = find_among_b(z, A_10.as_ptr(), 3);
        if among_var == 0 {
            (*z).lb = mlimit1;
            return 0;
        }
        (*z).bra = (*z).c;
        (*z).lb = mlimit1;
    }
    match among_var {
        1 => {
            if in_grouping_b_U(z, G_RV.as_ptr(), 97, 117, 0) != 0 {
                return 0;
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
        _ => {}
    }
    1
}

unsafe fn r_substantive(z: *mut SN_env) -> c_int {
    {
        let m1 = (*z).l - (*z).c;
        {
            let ret = r_special_noun_endings(z);
            if ret < 0 {
                return ret;
            }
        }
        (*z).c = (*z).l - m1;
    }
    {
        let m2 = (*z).l - (*z).c;
        {
            let ret = r_case_ending(z);
            if ret < 0 {
                return ret;
            }
        }
        (*z).c = (*z).l - m2;
    }
    {
        let m3 = (*z).l - (*z).c;
        {
            let ret = r_plural_three_first_cases(z);
            if ret < 0 {
                return ret;
            }
        }
        (*z).c = (*z).l - m3;
    }
    {
        let m4 = (*z).l - (*z).c;
        {
            let ret = r_degrees(z);
            if ret < 0 {
                return ret;
            }
        }
        (*z).c = (*z).l - m4;
    }
    {
        let m5 = (*z).l - (*z).c;
        {
            let ret = r_i_plural(z);
            if ret < 0 {
                return ret;
            }
        }
        (*z).c = (*z).l - m5;
    }
    {
        let m6 = (*z).l - (*z).c;
        {
            let ret = r_nu(z);
            if ret < 0 {
                return ret;
            }
        }
        (*z).c = (*z).l - m6;
    }
    1
}

unsafe fn r_verb_exceptions(z: *mut SN_env) -> c_int {
    let among_var;
    (*z).bra = (*z).c;
    among_var = find_among(z, A_11.as_ptr(), 290);
    if among_var == 0 {
        return 0;
    }
    (*z).ket = (*z).c;
    if (*z).c < (*z).l {
        return 0;
    }
    match among_var {
        1 => {
            let ret = slice_from_s(z, 3, S_10.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        2 => {
            let ret = slice_from_s(z, 3, S_11.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        3 => {
            let ret = slice_from_s(z, 5, S_12.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        4 => {
            let ret = slice_from_s(z, 5, S_13.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        5 => {
            let ret = slice_from_s(z, 5, S_14.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        6 => {
            let ret = slice_from_s(z, 4, S_15.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        7 => {
            let ret = slice_from_s(z, 3, S_16.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        8 => {
            let ret = slice_from_s(z, 6, S_17.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        9 => {
            let ret = slice_from_s(z, 5, S_18.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        10 => {
            let ret = slice_from_s(z, 3, S_19.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        11 => {
            let ret = slice_from_s(z, 6, S_20.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        12 => {
            let ret = slice_from_s(z, 7, S_21.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        13 => {
            let ret = slice_from_s(z, 7, S_22.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        14 => {
            let ret = slice_from_s(z, 4, S_23.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        15 => {
            let ret = slice_from_s(z, 5, S_24.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        16 => {
            let ret = slice_from_s(z, 4, S_25.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        17 => {
            let ret = slice_from_s(z, 4, S_26.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        18 => {
            let ret = slice_from_s(z, 5, S_27.as_ptr());
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

#[no_mangle]
pub unsafe extern "C" fn estonian_UTF_8_stem(z: *mut SN_env) -> c_int {
    {
        let c1 = (*z).c;
        'lab0: {
            {
                let ret = r_verb_exceptions(z);
                if ret == 0 {
                    break 'lab0;
                }
                if ret < 0 {
                    return ret;
                }
            }
            return 0;
        }
        // lab0:
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
        let m3 = (*z).l - (*z).c;
        {
            let ret = r_emphasis(z);
            if ret < 0 {
                return ret;
            }
        }
        (*z).c = (*z).l - m3;
    }
    {
        let m4 = (*z).l - (*z).c;
        'lab2: {
            let m5 = (*z).l - (*z).c;
            'lab3: {
                {
                    let ret = r_verb(z);
                    if ret == 0 {
                        break 'lab3;
                    }
                    if ret < 0 {
                        return ret;
                    }
                }
                break 'lab2;
            }
            // lab3:
            (*z).c = (*z).l - m5;
            {
                let ret = r_substantive(z);
                if ret < 0 {
                    return ret;
                }
            }
        }
        // lab2:
        (*z).c = (*z).l - m4;
    }
    {
        let m6 = (*z).l - (*z).c;
        {
            let ret = r_undouble_kpt(z);
            if ret < 0 {
                return ret;
            }
        }
        (*z).c = (*z).l - m6;
    }
    (*z).c = (*z).lb;
    1
}

#[no_mangle]
pub unsafe extern "C" fn estonian_UTF_8_create_env() -> *mut SN_env {
    SN_create_env(0, 1)
}

#[no_mangle]
pub unsafe extern "C" fn estonian_UTF_8_close_env(z: *mut SN_env) {
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
        let z = estonian_UTF_8_create_env();
        assert!(!z.is_null());
        let rc = SN_set_current(z, word.len() as c_int, word.as_ptr());
        assert!(rc >= 0);
        let rc = estonian_UTF_8_stem(z);
        assert!(rc >= 0, "stem returned {rc}");
        let out = std::slice::from_raw_parts((*z).p, SIZE((*z).p) as usize).to_vec();
        estonian_UTF_8_close_env(z);
        out
    }

    // A short root with no strippable suffix is returned unchanged.
    #[test]
    fn short_root_unchanged() {
        unsafe {
            assert_eq!(stem(b"abi"), b"abi".to_vec());
        }
    }

    // Idempotence: stemming a stem yields the same stem.
    #[test]
    fn idempotent() {
        unsafe {
            for w in [
                &b"raamatud"[..],
                &b"linnas"[..],
                &b"suuremad"[..],
                &b"laudadega"[..],
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
            let r = stem(b"linnas");
            assert!(!r.is_empty());
            assert!(r.len() <= "linnas".len());
        }
    }
}
