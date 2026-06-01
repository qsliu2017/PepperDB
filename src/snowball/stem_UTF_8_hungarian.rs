//! Hungarian Snowball stemmer (UTF-8).
//!
//! 1:1 translation of the generated
//! `src/backend/snowball/libstemmer/stem_UTF_8_hungarian.c` (Snowball 2.2.0),
//! merged with its header `stem_UTF_8_hungarian.h`. The runtime helpers come
//! from `crate::snowball::api` and `crate::snowball::utilities`.

use crate::prelude::*;

use crate::snowball::api::{among, symbol, SN_close_env, SN_create_env, SN_env, SIZE};
use crate::snowball::utilities::{
    find_among, find_among_b, in_grouping_U, out_grouping_U, skip_b_utf8, skip_utf8, slice_del,
    slice_from_s,
};

// ---------------------------------------------------------------------------
// among string tables
// ---------------------------------------------------------------------------

static S_0_0: [symbol; 2] = [b'c', b's'];
static S_0_1: [symbol; 3] = [b'd', b'z', b's'];
static S_0_2: [symbol; 2] = [b'g', b'y'];
static S_0_3: [symbol; 2] = [b'l', b'y'];
static S_0_4: [symbol; 2] = [b'n', b'y'];
static S_0_5: [symbol; 2] = [b's', b'z'];
static S_0_6: [symbol; 2] = [b't', b'y'];
static S_0_7: [symbol; 2] = [b'z', b's'];

static A_0: [among; 8] = [
    among { s_size: 2, s: S_0_0.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_0_1.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_0_2.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_0_3.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_0_4.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_0_5.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_0_6.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_0_7.as_ptr(), substring_i: -1, result: -1, function: None },
];

static S_1_0: [symbol; 2] = [0xC3, 0xA1];
static S_1_1: [symbol; 2] = [0xC3, 0xA9];

static A_1: [among; 2] = [
    among { s_size: 2, s: S_1_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_1_1.as_ptr(), substring_i: -1, result: 2, function: None },
];

static S_2_0: [symbol; 2] = [b'b', b'b'];
static S_2_1: [symbol; 2] = [b'c', b'c'];
static S_2_2: [symbol; 2] = [b'd', b'd'];
static S_2_3: [symbol; 2] = [b'f', b'f'];
static S_2_4: [symbol; 2] = [b'g', b'g'];
static S_2_5: [symbol; 2] = [b'j', b'j'];
static S_2_6: [symbol; 2] = [b'k', b'k'];
static S_2_7: [symbol; 2] = [b'l', b'l'];
static S_2_8: [symbol; 2] = [b'm', b'm'];
static S_2_9: [symbol; 2] = [b'n', b'n'];
static S_2_10: [symbol; 2] = [b'p', b'p'];
static S_2_11: [symbol; 2] = [b'r', b'r'];
static S_2_12: [symbol; 3] = [b'c', b'c', b's'];
static S_2_13: [symbol; 2] = [b's', b's'];
static S_2_14: [symbol; 3] = [b'z', b'z', b's'];
static S_2_15: [symbol; 2] = [b't', b't'];
static S_2_16: [symbol; 2] = [b'v', b'v'];
static S_2_17: [symbol; 3] = [b'g', b'g', b'y'];
static S_2_18: [symbol; 3] = [b'l', b'l', b'y'];
static S_2_19: [symbol; 3] = [b'n', b'n', b'y'];
static S_2_20: [symbol; 3] = [b't', b't', b'y'];
static S_2_21: [symbol; 3] = [b's', b's', b'z'];
static S_2_22: [symbol; 2] = [b'z', b'z'];

static A_2: [among; 23] = [
    among { s_size: 2, s: S_2_0.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_2_1.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_2_2.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_2_3.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_2_4.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_2_5.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_2_6.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_2_7.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_2_8.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_2_9.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_2_10.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_2_11.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_2_12.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_2_13.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_2_14.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_2_15.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_2_16.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_2_17.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_2_18.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_2_19.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_2_20.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_2_21.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_2_22.as_ptr(), substring_i: -1, result: -1, function: None },
];

static S_3_0: [symbol; 2] = [b'a', b'l'];
static S_3_1: [symbol; 2] = [b'e', b'l'];

static A_3: [among; 2] = [
    among { s_size: 2, s: S_3_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_3_1.as_ptr(), substring_i: -1, result: 1, function: None },
];

static S_4_0: [symbol; 2] = [b'b', b'a'];
static S_4_1: [symbol; 2] = [b'r', b'a'];
static S_4_2: [symbol; 2] = [b'b', b'e'];
static S_4_3: [symbol; 2] = [b'r', b'e'];
static S_4_4: [symbol; 2] = [b'i', b'g'];
static S_4_5: [symbol; 3] = [b'n', b'a', b'k'];
static S_4_6: [symbol; 3] = [b'n', b'e', b'k'];
static S_4_7: [symbol; 3] = [b'v', b'a', b'l'];
static S_4_8: [symbol; 3] = [b'v', b'e', b'l'];
static S_4_9: [symbol; 2] = [b'u', b'l'];
static S_4_10: [symbol; 4] = [b'b', 0xC5, 0x91, b'l'];
static S_4_11: [symbol; 4] = [b'r', 0xC5, 0x91, b'l'];
static S_4_12: [symbol; 4] = [b't', 0xC5, 0x91, b'l'];
static S_4_13: [symbol; 4] = [b'n', 0xC3, 0xA1, b'l'];
static S_4_14: [symbol; 4] = [b'n', 0xC3, 0xA9, b'l'];
static S_4_15: [symbol; 4] = [b'b', 0xC3, 0xB3, b'l'];
static S_4_16: [symbol; 4] = [b'r', 0xC3, 0xB3, b'l'];
static S_4_17: [symbol; 4] = [b't', 0xC3, 0xB3, b'l'];
static S_4_18: [symbol; 3] = [0xC3, 0xBC, b'l'];
static S_4_19: [symbol; 1] = [b'n'];
static S_4_20: [symbol; 2] = [b'a', b'n'];
static S_4_21: [symbol; 3] = [b'b', b'a', b'n'];
static S_4_22: [symbol; 2] = [b'e', b'n'];
static S_4_23: [symbol; 3] = [b'b', b'e', b'n'];
static S_4_24: [symbol; 7] = [b'k', 0xC3, 0xA9, b'p', b'p', b'e', b'n'];
static S_4_25: [symbol; 2] = [b'o', b'n'];
static S_4_26: [symbol; 3] = [0xC3, 0xB6, b'n'];
static S_4_27: [symbol; 5] = [b'k', 0xC3, 0xA9, b'p', b'p'];
static S_4_28: [symbol; 3] = [b'k', b'o', b'r'];
static S_4_29: [symbol; 1] = [b't'];
static S_4_30: [symbol; 2] = [b'a', b't'];
static S_4_31: [symbol; 2] = [b'e', b't'];
static S_4_32: [symbol; 5] = [b'k', 0xC3, 0xA9, b'n', b't'];
static S_4_33: [symbol; 7] = [b'a', b'n', b'k', 0xC3, 0xA9, b'n', b't'];
static S_4_34: [symbol; 7] = [b'e', b'n', b'k', 0xC3, 0xA9, b'n', b't'];
static S_4_35: [symbol; 7] = [b'o', b'n', b'k', 0xC3, 0xA9, b'n', b't'];
static S_4_36: [symbol; 2] = [b'o', b't'];
static S_4_37: [symbol; 4] = [0xC3, 0xA9, b'r', b't'];
static S_4_38: [symbol; 3] = [0xC3, 0xB6, b't'];
static S_4_39: [symbol; 3] = [b'h', b'e', b'z'];
static S_4_40: [symbol; 3] = [b'h', b'o', b'z'];
static S_4_41: [symbol; 4] = [b'h', 0xC3, 0xB6, b'z'];
static S_4_42: [symbol; 3] = [b'v', 0xC3, 0xA1];
static S_4_43: [symbol; 3] = [b'v', 0xC3, 0xA9];

static A_4: [among; 44] = [
    among { s_size: 2, s: S_4_0.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_4_1.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_4_2.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_4_3.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_4_4.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_4_5.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_4_6.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_4_7.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_4_8.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_4_9.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 4, s: S_4_10.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 4, s: S_4_11.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 4, s: S_4_12.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 4, s: S_4_13.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 4, s: S_4_14.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 4, s: S_4_15.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 4, s: S_4_16.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 4, s: S_4_17.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_4_18.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 1, s: S_4_19.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_4_20.as_ptr(), substring_i: 19, result: -1, function: None },
    among { s_size: 3, s: S_4_21.as_ptr(), substring_i: 20, result: -1, function: None },
    among { s_size: 2, s: S_4_22.as_ptr(), substring_i: 19, result: -1, function: None },
    among { s_size: 3, s: S_4_23.as_ptr(), substring_i: 22, result: -1, function: None },
    among { s_size: 7, s: S_4_24.as_ptr(), substring_i: 22, result: -1, function: None },
    among { s_size: 2, s: S_4_25.as_ptr(), substring_i: 19, result: -1, function: None },
    among { s_size: 3, s: S_4_26.as_ptr(), substring_i: 19, result: -1, function: None },
    among { s_size: 5, s: S_4_27.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_4_28.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 1, s: S_4_29.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_4_30.as_ptr(), substring_i: 29, result: -1, function: None },
    among { s_size: 2, s: S_4_31.as_ptr(), substring_i: 29, result: -1, function: None },
    among { s_size: 5, s: S_4_32.as_ptr(), substring_i: 29, result: -1, function: None },
    among { s_size: 7, s: S_4_33.as_ptr(), substring_i: 32, result: -1, function: None },
    among { s_size: 7, s: S_4_34.as_ptr(), substring_i: 32, result: -1, function: None },
    among { s_size: 7, s: S_4_35.as_ptr(), substring_i: 32, result: -1, function: None },
    among { s_size: 2, s: S_4_36.as_ptr(), substring_i: 29, result: -1, function: None },
    among { s_size: 4, s: S_4_37.as_ptr(), substring_i: 29, result: -1, function: None },
    among { s_size: 3, s: S_4_38.as_ptr(), substring_i: 29, result: -1, function: None },
    among { s_size: 3, s: S_4_39.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_4_40.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 4, s: S_4_41.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_4_42.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_4_43.as_ptr(), substring_i: -1, result: -1, function: None },
];

static S_5_0: [symbol; 3] = [0xC3, 0xA1, b'n'];
static S_5_1: [symbol; 3] = [0xC3, 0xA9, b'n'];
static S_5_2: [symbol; 8] = [0xC3, 0xA1, b'n', b'k', 0xC3, 0xA9, b'n', b't'];

static A_5: [among; 3] = [
    among { s_size: 3, s: S_5_0.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 3, s: S_5_1.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_5_2.as_ptr(), substring_i: -1, result: 2, function: None },
];

static S_6_0: [symbol; 4] = [b's', b't', b'u', b'l'];
static S_6_1: [symbol; 5] = [b'a', b's', b't', b'u', b'l'];
static S_6_2: [symbol; 6] = [0xC3, 0xA1, b's', b't', b'u', b'l'];
static S_6_3: [symbol; 5] = [b's', b't', 0xC3, 0xBC, b'l'];
static S_6_4: [symbol; 6] = [b'e', b's', b't', 0xC3, 0xBC, b'l'];
static S_6_5: [symbol; 7] = [0xC3, 0xA9, b's', b't', 0xC3, 0xBC, b'l'];

static A_6: [among; 6] = [
    among { s_size: 4, s: S_6_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_6_1.as_ptr(), substring_i: 0, result: 1, function: None },
    among { s_size: 6, s: S_6_2.as_ptr(), substring_i: 0, result: 2, function: None },
    among { s_size: 5, s: S_6_3.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_6_4.as_ptr(), substring_i: 3, result: 1, function: None },
    among { s_size: 7, s: S_6_5.as_ptr(), substring_i: 3, result: 3, function: None },
];

static S_7_0: [symbol; 2] = [0xC3, 0xA1];
static S_7_1: [symbol; 2] = [0xC3, 0xA9];

static A_7: [among; 2] = [
    among { s_size: 2, s: S_7_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_7_1.as_ptr(), substring_i: -1, result: 1, function: None },
];

static S_8_0: [symbol; 1] = [b'k'];
static S_8_1: [symbol; 2] = [b'a', b'k'];
static S_8_2: [symbol; 2] = [b'e', b'k'];
static S_8_3: [symbol; 2] = [b'o', b'k'];
static S_8_4: [symbol; 3] = [0xC3, 0xA1, b'k'];
static S_8_5: [symbol; 3] = [0xC3, 0xA9, b'k'];
static S_8_6: [symbol; 3] = [0xC3, 0xB6, b'k'];

static A_8: [among; 7] = [
    among { s_size: 1, s: S_8_0.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 2, s: S_8_1.as_ptr(), substring_i: 0, result: 3, function: None },
    among { s_size: 2, s: S_8_2.as_ptr(), substring_i: 0, result: 3, function: None },
    among { s_size: 2, s: S_8_3.as_ptr(), substring_i: 0, result: 3, function: None },
    among { s_size: 3, s: S_8_4.as_ptr(), substring_i: 0, result: 1, function: None },
    among { s_size: 3, s: S_8_5.as_ptr(), substring_i: 0, result: 2, function: None },
    among { s_size: 3, s: S_8_6.as_ptr(), substring_i: 0, result: 3, function: None },
];

static S_9_0: [symbol; 3] = [0xC3, 0xA9, b'i'];
static S_9_1: [symbol; 5] = [0xC3, 0xA1, 0xC3, 0xA9, b'i'];
static S_9_2: [symbol; 5] = [0xC3, 0xA9, 0xC3, 0xA9, b'i'];
static S_9_3: [symbol; 2] = [0xC3, 0xA9];
static S_9_4: [symbol; 3] = [b'k', 0xC3, 0xA9];
static S_9_5: [symbol; 4] = [b'a', b'k', 0xC3, 0xA9];
static S_9_6: [symbol; 4] = [b'e', b'k', 0xC3, 0xA9];
static S_9_7: [symbol; 4] = [b'o', b'k', 0xC3, 0xA9];
static S_9_8: [symbol; 5] = [0xC3, 0xA1, b'k', 0xC3, 0xA9];
static S_9_9: [symbol; 5] = [0xC3, 0xA9, b'k', 0xC3, 0xA9];
static S_9_10: [symbol; 5] = [0xC3, 0xB6, b'k', 0xC3, 0xA9];
static S_9_11: [symbol; 4] = [0xC3, 0xA9, 0xC3, 0xA9];

static A_9: [among; 12] = [
    among { s_size: 3, s: S_9_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_9_1.as_ptr(), substring_i: 0, result: 3, function: None },
    among { s_size: 5, s: S_9_2.as_ptr(), substring_i: 0, result: 2, function: None },
    among { s_size: 2, s: S_9_3.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_9_4.as_ptr(), substring_i: 3, result: 1, function: None },
    among { s_size: 4, s: S_9_5.as_ptr(), substring_i: 4, result: 1, function: None },
    among { s_size: 4, s: S_9_6.as_ptr(), substring_i: 4, result: 1, function: None },
    among { s_size: 4, s: S_9_7.as_ptr(), substring_i: 4, result: 1, function: None },
    among { s_size: 5, s: S_9_8.as_ptr(), substring_i: 4, result: 3, function: None },
    among { s_size: 5, s: S_9_9.as_ptr(), substring_i: 4, result: 2, function: None },
    among { s_size: 5, s: S_9_10.as_ptr(), substring_i: 4, result: 1, function: None },
    among { s_size: 4, s: S_9_11.as_ptr(), substring_i: 3, result: 2, function: None },
];

static S_10_0: [symbol; 1] = [b'a'];
static S_10_1: [symbol; 2] = [b'j', b'a'];
static S_10_2: [symbol; 1] = [b'd'];
static S_10_3: [symbol; 2] = [b'a', b'd'];
static S_10_4: [symbol; 2] = [b'e', b'd'];
static S_10_5: [symbol; 2] = [b'o', b'd'];
static S_10_6: [symbol; 3] = [0xC3, 0xA1, b'd'];
static S_10_7: [symbol; 3] = [0xC3, 0xA9, b'd'];
static S_10_8: [symbol; 3] = [0xC3, 0xB6, b'd'];
static S_10_9: [symbol; 1] = [b'e'];
static S_10_10: [symbol; 2] = [b'j', b'e'];
static S_10_11: [symbol; 2] = [b'n', b'k'];
static S_10_12: [symbol; 3] = [b'u', b'n', b'k'];
static S_10_13: [symbol; 4] = [0xC3, 0xA1, b'n', b'k'];
static S_10_14: [symbol; 4] = [0xC3, 0xA9, b'n', b'k'];
static S_10_15: [symbol; 4] = [0xC3, 0xBC, b'n', b'k'];
static S_10_16: [symbol; 2] = [b'u', b'k'];
static S_10_17: [symbol; 3] = [b'j', b'u', b'k'];
static S_10_18: [symbol; 5] = [0xC3, 0xA1, b'j', b'u', b'k'];
static S_10_19: [symbol; 3] = [0xC3, 0xBC, b'k'];
static S_10_20: [symbol; 4] = [b'j', 0xC3, 0xBC, b'k'];
static S_10_21: [symbol; 6] = [0xC3, 0xA9, b'j', 0xC3, 0xBC, b'k'];
static S_10_22: [symbol; 1] = [b'm'];
static S_10_23: [symbol; 2] = [b'a', b'm'];
static S_10_24: [symbol; 2] = [b'e', b'm'];
static S_10_25: [symbol; 2] = [b'o', b'm'];
static S_10_26: [symbol; 3] = [0xC3, 0xA1, b'm'];
static S_10_27: [symbol; 3] = [0xC3, 0xA9, b'm'];
static S_10_28: [symbol; 1] = [b'o'];
static S_10_29: [symbol; 2] = [0xC3, 0xA1];
static S_10_30: [symbol; 2] = [0xC3, 0xA9];

static A_10: [among; 31] = [
    among { s_size: 1, s: S_10_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_10_1.as_ptr(), substring_i: 0, result: 1, function: None },
    among { s_size: 1, s: S_10_2.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_10_3.as_ptr(), substring_i: 2, result: 1, function: None },
    among { s_size: 2, s: S_10_4.as_ptr(), substring_i: 2, result: 1, function: None },
    among { s_size: 2, s: S_10_5.as_ptr(), substring_i: 2, result: 1, function: None },
    among { s_size: 3, s: S_10_6.as_ptr(), substring_i: 2, result: 2, function: None },
    among { s_size: 3, s: S_10_7.as_ptr(), substring_i: 2, result: 3, function: None },
    among { s_size: 3, s: S_10_8.as_ptr(), substring_i: 2, result: 1, function: None },
    among { s_size: 1, s: S_10_9.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_10_10.as_ptr(), substring_i: 9, result: 1, function: None },
    among { s_size: 2, s: S_10_11.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_10_12.as_ptr(), substring_i: 11, result: 1, function: None },
    among { s_size: 4, s: S_10_13.as_ptr(), substring_i: 11, result: 2, function: None },
    among { s_size: 4, s: S_10_14.as_ptr(), substring_i: 11, result: 3, function: None },
    among { s_size: 4, s: S_10_15.as_ptr(), substring_i: 11, result: 1, function: None },
    among { s_size: 2, s: S_10_16.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_10_17.as_ptr(), substring_i: 16, result: 1, function: None },
    among { s_size: 5, s: S_10_18.as_ptr(), substring_i: 17, result: 2, function: None },
    among { s_size: 3, s: S_10_19.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_10_20.as_ptr(), substring_i: 19, result: 1, function: None },
    among { s_size: 6, s: S_10_21.as_ptr(), substring_i: 20, result: 3, function: None },
    among { s_size: 1, s: S_10_22.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_10_23.as_ptr(), substring_i: 22, result: 1, function: None },
    among { s_size: 2, s: S_10_24.as_ptr(), substring_i: 22, result: 1, function: None },
    among { s_size: 2, s: S_10_25.as_ptr(), substring_i: 22, result: 1, function: None },
    among { s_size: 3, s: S_10_26.as_ptr(), substring_i: 22, result: 2, function: None },
    among { s_size: 3, s: S_10_27.as_ptr(), substring_i: 22, result: 3, function: None },
    among { s_size: 1, s: S_10_28.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_10_29.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 2, s: S_10_30.as_ptr(), substring_i: -1, result: 3, function: None },
];

static S_11_0: [symbol; 2] = [b'i', b'd'];
static S_11_1: [symbol; 3] = [b'a', b'i', b'd'];
static S_11_2: [symbol; 4] = [b'j', b'a', b'i', b'd'];
static S_11_3: [symbol; 3] = [b'e', b'i', b'd'];
static S_11_4: [symbol; 4] = [b'j', b'e', b'i', b'd'];
static S_11_5: [symbol; 4] = [0xC3, 0xA1, b'i', b'd'];
static S_11_6: [symbol; 4] = [0xC3, 0xA9, b'i', b'd'];
static S_11_7: [symbol; 1] = [b'i'];
static S_11_8: [symbol; 2] = [b'a', b'i'];
static S_11_9: [symbol; 3] = [b'j', b'a', b'i'];
static S_11_10: [symbol; 2] = [b'e', b'i'];
static S_11_11: [symbol; 3] = [b'j', b'e', b'i'];
static S_11_12: [symbol; 3] = [0xC3, 0xA1, b'i'];
static S_11_13: [symbol; 3] = [0xC3, 0xA9, b'i'];
static S_11_14: [symbol; 4] = [b'i', b't', b'e', b'k'];
static S_11_15: [symbol; 5] = [b'e', b'i', b't', b'e', b'k'];
static S_11_16: [symbol; 6] = [b'j', b'e', b'i', b't', b'e', b'k'];
static S_11_17: [symbol; 6] = [0xC3, 0xA9, b'i', b't', b'e', b'k'];
static S_11_18: [symbol; 2] = [b'i', b'k'];
static S_11_19: [symbol; 3] = [b'a', b'i', b'k'];
static S_11_20: [symbol; 4] = [b'j', b'a', b'i', b'k'];
static S_11_21: [symbol; 3] = [b'e', b'i', b'k'];
static S_11_22: [symbol; 4] = [b'j', b'e', b'i', b'k'];
static S_11_23: [symbol; 4] = [0xC3, 0xA1, b'i', b'k'];
static S_11_24: [symbol; 4] = [0xC3, 0xA9, b'i', b'k'];
static S_11_25: [symbol; 3] = [b'i', b'n', b'k'];
static S_11_26: [symbol; 4] = [b'a', b'i', b'n', b'k'];
static S_11_27: [symbol; 5] = [b'j', b'a', b'i', b'n', b'k'];
static S_11_28: [symbol; 4] = [b'e', b'i', b'n', b'k'];
static S_11_29: [symbol; 5] = [b'j', b'e', b'i', b'n', b'k'];
static S_11_30: [symbol; 5] = [0xC3, 0xA1, b'i', b'n', b'k'];
static S_11_31: [symbol; 5] = [0xC3, 0xA9, b'i', b'n', b'k'];
static S_11_32: [symbol; 5] = [b'a', b'i', b't', b'o', b'k'];
static S_11_33: [symbol; 6] = [b'j', b'a', b'i', b't', b'o', b'k'];
static S_11_34: [symbol; 6] = [0xC3, 0xA1, b'i', b't', b'o', b'k'];
static S_11_35: [symbol; 2] = [b'i', b'm'];
static S_11_36: [symbol; 3] = [b'a', b'i', b'm'];
static S_11_37: [symbol; 4] = [b'j', b'a', b'i', b'm'];
static S_11_38: [symbol; 3] = [b'e', b'i', b'm'];
static S_11_39: [symbol; 4] = [b'j', b'e', b'i', b'm'];
static S_11_40: [symbol; 4] = [0xC3, 0xA1, b'i', b'm'];
static S_11_41: [symbol; 4] = [0xC3, 0xA9, b'i', b'm'];

static A_11: [among; 42] = [
    among { s_size: 2, s: S_11_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_11_1.as_ptr(), substring_i: 0, result: 1, function: None },
    among { s_size: 4, s: S_11_2.as_ptr(), substring_i: 1, result: 1, function: None },
    among { s_size: 3, s: S_11_3.as_ptr(), substring_i: 0, result: 1, function: None },
    among { s_size: 4, s: S_11_4.as_ptr(), substring_i: 3, result: 1, function: None },
    among { s_size: 4, s: S_11_5.as_ptr(), substring_i: 0, result: 2, function: None },
    among { s_size: 4, s: S_11_6.as_ptr(), substring_i: 0, result: 3, function: None },
    among { s_size: 1, s: S_11_7.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_11_8.as_ptr(), substring_i: 7, result: 1, function: None },
    among { s_size: 3, s: S_11_9.as_ptr(), substring_i: 8, result: 1, function: None },
    among { s_size: 2, s: S_11_10.as_ptr(), substring_i: 7, result: 1, function: None },
    among { s_size: 3, s: S_11_11.as_ptr(), substring_i: 10, result: 1, function: None },
    among { s_size: 3, s: S_11_12.as_ptr(), substring_i: 7, result: 2, function: None },
    among { s_size: 3, s: S_11_13.as_ptr(), substring_i: 7, result: 3, function: None },
    among { s_size: 4, s: S_11_14.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_11_15.as_ptr(), substring_i: 14, result: 1, function: None },
    among { s_size: 6, s: S_11_16.as_ptr(), substring_i: 15, result: 1, function: None },
    among { s_size: 6, s: S_11_17.as_ptr(), substring_i: 14, result: 3, function: None },
    among { s_size: 2, s: S_11_18.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_11_19.as_ptr(), substring_i: 18, result: 1, function: None },
    among { s_size: 4, s: S_11_20.as_ptr(), substring_i: 19, result: 1, function: None },
    among { s_size: 3, s: S_11_21.as_ptr(), substring_i: 18, result: 1, function: None },
    among { s_size: 4, s: S_11_22.as_ptr(), substring_i: 21, result: 1, function: None },
    among { s_size: 4, s: S_11_23.as_ptr(), substring_i: 18, result: 2, function: None },
    among { s_size: 4, s: S_11_24.as_ptr(), substring_i: 18, result: 3, function: None },
    among { s_size: 3, s: S_11_25.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_11_26.as_ptr(), substring_i: 25, result: 1, function: None },
    among { s_size: 5, s: S_11_27.as_ptr(), substring_i: 26, result: 1, function: None },
    among { s_size: 4, s: S_11_28.as_ptr(), substring_i: 25, result: 1, function: None },
    among { s_size: 5, s: S_11_29.as_ptr(), substring_i: 28, result: 1, function: None },
    among { s_size: 5, s: S_11_30.as_ptr(), substring_i: 25, result: 2, function: None },
    among { s_size: 5, s: S_11_31.as_ptr(), substring_i: 25, result: 3, function: None },
    among { s_size: 5, s: S_11_32.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_11_33.as_ptr(), substring_i: 32, result: 1, function: None },
    among { s_size: 6, s: S_11_34.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 2, s: S_11_35.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_11_36.as_ptr(), substring_i: 35, result: 1, function: None },
    among { s_size: 4, s: S_11_37.as_ptr(), substring_i: 36, result: 1, function: None },
    among { s_size: 3, s: S_11_38.as_ptr(), substring_i: 35, result: 1, function: None },
    among { s_size: 4, s: S_11_39.as_ptr(), substring_i: 38, result: 1, function: None },
    among { s_size: 4, s: S_11_40.as_ptr(), substring_i: 35, result: 2, function: None },
    among { s_size: 4, s: S_11_41.as_ptr(), substring_i: 35, result: 3, function: None },
];

static G_V: [c_uchar; 35] = [
    17, 65, 16, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 17, 36, 10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    1, 0, 0, 0, 1,
];

static S_0: [symbol; 1] = [b'a'];
static S_1: [symbol; 1] = [b'e'];
static S_2: [symbol; 1] = [b'e'];
static S_3: [symbol; 1] = [b'a'];
static S_4: [symbol; 1] = [b'a'];
static S_5: [symbol; 1] = [b'e'];
static S_6: [symbol; 1] = [b'a'];
static S_7: [symbol; 1] = [b'e'];
static S_8: [symbol; 1] = [b'e'];
static S_9: [symbol; 1] = [b'a'];
static S_10: [symbol; 1] = [b'a'];
static S_11: [symbol; 1] = [b'e'];
static S_12: [symbol; 1] = [b'a'];
static S_13: [symbol; 1] = [b'e'];

// ---------------------------------------------------------------------------
// rules
// ---------------------------------------------------------------------------

unsafe fn r_mark_regions(z: *mut SN_env) -> c_int {
    *(*z).I.offset(0) = (*z).l;
    'lab0: {
        let c1 = (*z).c;
        'lab1: {
            if in_grouping_U(z, G_V.as_ptr(), 97, 369, 0) != 0 {
                break 'lab1;
            }
            if in_grouping_U(z, G_V.as_ptr(), 97, 369, 1) < 0 {
                break 'lab1;
            }
            'lab2: {
                let c2 = (*z).c;
                'lab3: {
                    if (*z).c + 1 >= (*z).l
                        || *(*z).p.offset(((*z).c + 1) as isize) as c_int >> 5 != 3
                        || ((101187584 >> (*(*z).p.offset(((*z).c + 1) as isize) as c_int & 0x1f)) & 1)
                            == 0
                    {
                        break 'lab3;
                    }
                    if find_among(z, A_0.as_ptr(), 8) == 0 {
                        break 'lab3;
                    }
                    break 'lab2;
                }
                // lab3:
                (*z).c = c2;
                {
                    let ret = skip_utf8((*z).p, (*z).c, (*z).l, 1);
                    if ret < 0 {
                        break 'lab1;
                    }
                    (*z).c = ret;
                }
            }
            // lab2:
            *(*z).I.offset(0) = (*z).c;
            break 'lab0;
        }
        // lab1:
        (*z).c = c1;
        if out_grouping_U(z, G_V.as_ptr(), 97, 369, 0) != 0 {
            return 0;
        }

        {
            let ret = out_grouping_U(z, G_V.as_ptr(), 97, 369, 1);
            if ret < 0 {
                return 0;
            }
            (*z).c += ret;
        }
        *(*z).I.offset(0) = (*z).c;
    }
    // lab0:
    1
}

unsafe fn r_R1(z: *mut SN_env) -> c_int {
    (*(*z).I.offset(0) <= (*z).c) as c_int
}

unsafe fn r_v_ending(z: *mut SN_env) -> c_int {
    let among_var;
    (*z).ket = (*z).c;
    if (*z).c - 1 <= (*z).lb
        || (*(*z).p.offset(((*z).c - 1) as isize) != 161
            && *(*z).p.offset(((*z).c - 1) as isize) != 169)
    {
        return 0;
    }
    among_var = find_among_b(z, A_1.as_ptr(), 2);
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
        _ => {}
    }
    1
}

unsafe fn r_double(z: *mut SN_env) -> c_int {
    {
        let m_test1 = (*z).l - (*z).c;
        if (*z).c - 1 <= (*z).lb
            || *(*z).p.offset(((*z).c - 1) as isize) as c_int >> 5 != 3
            || ((106790108 >> (*(*z).p.offset(((*z).c - 1) as isize) as c_int & 0x1f)) & 1) == 0
        {
            return 0;
        }
        if find_among_b(z, A_2.as_ptr(), 23) == 0 {
            return 0;
        }
        (*z).c = (*z).l - m_test1;
    }
    1
}

unsafe fn r_undouble(z: *mut SN_env) -> c_int {
    {
        let ret = skip_b_utf8((*z).p, (*z).c, (*z).lb, 1);
        if ret < 0 {
            return 0;
        }
        (*z).c = ret;
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

unsafe fn r_instrum(z: *mut SN_env) -> c_int {
    (*z).ket = (*z).c;
    if (*z).c - 1 <= (*z).lb || *(*z).p.offset(((*z).c - 1) as isize) != 108 {
        return 0;
    }
    if find_among_b(z, A_3.as_ptr(), 2) == 0 {
        return 0;
    }
    (*z).bra = (*z).c;
    {
        let ret = r_R1(z);
        if ret <= 0 {
            return ret;
        }
    }
    {
        let ret = r_double(z);
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
        let ret = r_undouble(z);
        if ret <= 0 {
            return ret;
        }
    }
    1
}

unsafe fn r_case(z: *mut SN_env) -> c_int {
    (*z).ket = (*z).c;
    if find_among_b(z, A_4.as_ptr(), 44) == 0 {
        return 0;
    }
    (*z).bra = (*z).c;
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
    {
        let ret = r_v_ending(z);
        if ret <= 0 {
            return ret;
        }
    }
    1
}

unsafe fn r_case_special(z: *mut SN_env) -> c_int {
    let among_var;
    (*z).ket = (*z).c;
    if (*z).c - 2 <= (*z).lb
        || (*(*z).p.offset(((*z).c - 1) as isize) != 110
            && *(*z).p.offset(((*z).c - 1) as isize) != 116)
    {
        return 0;
    }
    among_var = find_among_b(z, A_5.as_ptr(), 3);
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
            let ret = slice_from_s(z, 1, S_2.as_ptr());
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
        _ => {}
    }
    1
}

unsafe fn r_case_other(z: *mut SN_env) -> c_int {
    let among_var;
    (*z).ket = (*z).c;
    if (*z).c - 3 <= (*z).lb || *(*z).p.offset(((*z).c - 1) as isize) != 108 {
        return 0;
    }
    among_var = find_among_b(z, A_6.as_ptr(), 6);
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
            let ret = slice_from_s(z, 1, S_4.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        3 => {
            let ret = slice_from_s(z, 1, S_5.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        _ => {}
    }
    1
}

unsafe fn r_factive(z: *mut SN_env) -> c_int {
    (*z).ket = (*z).c;
    if (*z).c - 1 <= (*z).lb
        || (*(*z).p.offset(((*z).c - 1) as isize) != 161
            && *(*z).p.offset(((*z).c - 1) as isize) != 169)
    {
        return 0;
    }
    if find_among_b(z, A_7.as_ptr(), 2) == 0 {
        return 0;
    }
    (*z).bra = (*z).c;
    {
        let ret = r_R1(z);
        if ret <= 0 {
            return ret;
        }
    }
    {
        let ret = r_double(z);
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
        let ret = r_undouble(z);
        if ret <= 0 {
            return ret;
        }
    }
    1
}

unsafe fn r_plural(z: *mut SN_env) -> c_int {
    let among_var;
    (*z).ket = (*z).c;
    if (*z).c <= (*z).lb || *(*z).p.offset(((*z).c - 1) as isize) != 107 {
        return 0;
    }
    among_var = find_among_b(z, A_8.as_ptr(), 7);
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
            let ret = slice_del(z);
            if ret < 0 {
                return ret;
            }
        }
        _ => {}
    }
    1
}

unsafe fn r_owned(z: *mut SN_env) -> c_int {
    let among_var;
    (*z).ket = (*z).c;
    if (*z).c - 1 <= (*z).lb
        || (*(*z).p.offset(((*z).c - 1) as isize) != 105
            && *(*z).p.offset(((*z).c - 1) as isize) != 169)
    {
        return 0;
    }
    among_var = find_among_b(z, A_9.as_ptr(), 12);
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

unsafe fn r_sing_owner(z: *mut SN_env) -> c_int {
    let among_var;
    (*z).ket = (*z).c;
    among_var = find_among_b(z, A_10.as_ptr(), 31);
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
            let ret = slice_from_s(z, 1, S_10.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        3 => {
            let ret = slice_from_s(z, 1, S_11.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        _ => {}
    }
    1
}

unsafe fn r_plur_owner(z: *mut SN_env) -> c_int {
    let among_var;
    (*z).ket = (*z).c;
    if (*z).c <= (*z).lb
        || *(*z).p.offset(((*z).c - 1) as isize) as c_int >> 5 != 3
        || ((10768 >> (*(*z).p.offset(((*z).c - 1) as isize) as c_int & 0x1f)) & 1) == 0
    {
        return 0;
    }
    among_var = find_among_b(z, A_11.as_ptr(), 42);
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
            let ret = slice_from_s(z, 1, S_12.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        3 => {
            let ret = slice_from_s(z, 1, S_13.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        _ => {}
    }
    1
}

#[no_mangle]
pub unsafe extern "C" fn hungarian_UTF_8_stem(z: *mut SN_env) -> c_int {
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
            let ret = r_instrum(z);
            if ret < 0 {
                return ret;
            }
        }
        (*z).c = (*z).l - m2;
    }
    {
        let m3 = (*z).l - (*z).c;
        {
            let ret = r_case(z);
            if ret < 0 {
                return ret;
            }
        }
        (*z).c = (*z).l - m3;
    }
    {
        let m4 = (*z).l - (*z).c;
        {
            let ret = r_case_special(z);
            if ret < 0 {
                return ret;
            }
        }
        (*z).c = (*z).l - m4;
    }
    {
        let m5 = (*z).l - (*z).c;
        {
            let ret = r_case_other(z);
            if ret < 0 {
                return ret;
            }
        }
        (*z).c = (*z).l - m5;
    }
    {
        let m6 = (*z).l - (*z).c;
        {
            let ret = r_factive(z);
            if ret < 0 {
                return ret;
            }
        }
        (*z).c = (*z).l - m6;
    }
    {
        let m7 = (*z).l - (*z).c;
        {
            let ret = r_owned(z);
            if ret < 0 {
                return ret;
            }
        }
        (*z).c = (*z).l - m7;
    }
    {
        let m8 = (*z).l - (*z).c;
        {
            let ret = r_sing_owner(z);
            if ret < 0 {
                return ret;
            }
        }
        (*z).c = (*z).l - m8;
    }
    {
        let m9 = (*z).l - (*z).c;
        {
            let ret = r_plur_owner(z);
            if ret < 0 {
                return ret;
            }
        }
        (*z).c = (*z).l - m9;
    }
    {
        let m10 = (*z).l - (*z).c;
        {
            let ret = r_plural(z);
            if ret < 0 {
                return ret;
            }
        }
        (*z).c = (*z).l - m10;
    }
    (*z).c = (*z).lb;
    1
}

#[no_mangle]
pub unsafe extern "C" fn hungarian_UTF_8_create_env() -> *mut SN_env {
    SN_create_env(0, 1)
}

#[no_mangle]
pub unsafe extern "C" fn hungarian_UTF_8_close_env(z: *mut SN_env) {
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
        let z = hungarian_UTF_8_create_env();
        assert!(!z.is_null());
        let rc = SN_set_current(z, word.len() as c_int, word.as_ptr());
        assert!(rc >= 0);
        let rc = hungarian_UTF_8_stem(z);
        assert!(rc >= 0, "stem returned {rc}");
        let out = std::slice::from_raw_parts((*z).p, SIZE((*z).p) as usize).to_vec();
        hungarian_UTF_8_close_env(z);
        out
    }

    // A short root with no strippable suffix is returned unchanged.
    #[test]
    fn short_root_unchanged() {
        unsafe {
            assert_eq!(stem(b"haz"), b"haz".to_vec());
        }
    }

    // Idempotence: stemming a stem yields the same stem.
    #[test]
    fn idempotent() {
        unsafe {
            // "hazaknak" (to the houses) and a few Hungarian-accented words.
            let words: [&[u8]; 4] = [
                b"hazaknak",
                b"emberekben",
                &[b'k', b'\xc3', b'\xa9', b'p', b'p', b'e', b'n'],
                &[b'v', b'\xc3', b'\xa1', b'r', b'o', b's'],
            ];
            for w in words {
                let once = stem(w);
                let twice = stem(&once);
                assert_eq!(once, twice, "not idempotent");
                assert!(!once.is_empty());
            }
        }
    }
}
