//! English Snowball stemmer (UTF-8).
//!
//! 1:1 translation of the generated
//! `src/backend/snowball/libstemmer/stem_UTF_8_english.c` (Snowball 2.2.0),
//! merged with its header `stem_UTF_8_english.h`. The runtime helpers come from
//! `crate::snowball::api` and `crate::snowball::utilities`.

use crate::prelude::*;

use crate::snowball::api::{among, symbol, SN_close_env, SN_create_env, SN_env, SIZE};
use crate::snowball::utilities::{
    find_among, find_among_b, in_grouping_b_U, in_grouping_U, out_grouping_b_U, out_grouping_U,
    skip_b_utf8, skip_utf8, slice_del, slice_from_s,
};

// ---------------------------------------------------------------------------
// among string tables
// ---------------------------------------------------------------------------

static S_0_0: [symbol; 5] = [b'a', b'r', b's', b'e', b'n'];
static S_0_1: [symbol; 6] = [b'c', b'o', b'm', b'm', b'u', b'n'];
static S_0_2: [symbol; 5] = [b'g', b'e', b'n', b'e', b'r'];

static A_0: [among; 3] = [
    among { s_size: 5, s: S_0_0.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 6, s: S_0_1.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 5, s: S_0_2.as_ptr(), substring_i: -1, result: -1, function: None },
];

static S_1_0: [symbol; 1] = [b'\''];
static S_1_1: [symbol; 3] = [b'\'', b's', b'\''];
static S_1_2: [symbol; 2] = [b'\'', b's'];

static A_1: [among; 3] = [
    among { s_size: 1, s: S_1_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_1_1.as_ptr(), substring_i: 0, result: 1, function: None },
    among { s_size: 2, s: S_1_2.as_ptr(), substring_i: -1, result: 1, function: None },
];

static S_2_0: [symbol; 3] = [b'i', b'e', b'd'];
static S_2_1: [symbol; 1] = [b's'];
static S_2_2: [symbol; 3] = [b'i', b'e', b's'];
static S_2_3: [symbol; 4] = [b's', b's', b'e', b's'];
static S_2_4: [symbol; 2] = [b's', b's'];
static S_2_5: [symbol; 2] = [b'u', b's'];

static A_2: [among; 6] = [
    among { s_size: 3, s: S_2_0.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 1, s: S_2_1.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 3, s: S_2_2.as_ptr(), substring_i: 1, result: 2, function: None },
    among { s_size: 4, s: S_2_3.as_ptr(), substring_i: 1, result: 1, function: None },
    among { s_size: 2, s: S_2_4.as_ptr(), substring_i: 1, result: -1, function: None },
    among { s_size: 2, s: S_2_5.as_ptr(), substring_i: 1, result: -1, function: None },
];

static S_3_1: [symbol; 2] = [b'b', b'b'];
static S_3_2: [symbol; 2] = [b'd', b'd'];
static S_3_3: [symbol; 2] = [b'f', b'f'];
static S_3_4: [symbol; 2] = [b'g', b'g'];
static S_3_5: [symbol; 2] = [b'b', b'l'];
static S_3_6: [symbol; 2] = [b'm', b'm'];
static S_3_7: [symbol; 2] = [b'n', b'n'];
static S_3_8: [symbol; 2] = [b'p', b'p'];
static S_3_9: [symbol; 2] = [b'r', b'r'];
static S_3_10: [symbol; 2] = [b'a', b't'];
static S_3_11: [symbol; 2] = [b't', b't'];
static S_3_12: [symbol; 2] = [b'i', b'z'];

static A_3: [among; 13] = [
    among { s_size: 0, s: core::ptr::null(), substring_i: -1, result: 3, function: None },
    among { s_size: 2, s: S_3_1.as_ptr(), substring_i: 0, result: 2, function: None },
    among { s_size: 2, s: S_3_2.as_ptr(), substring_i: 0, result: 2, function: None },
    among { s_size: 2, s: S_3_3.as_ptr(), substring_i: 0, result: 2, function: None },
    among { s_size: 2, s: S_3_4.as_ptr(), substring_i: 0, result: 2, function: None },
    among { s_size: 2, s: S_3_5.as_ptr(), substring_i: 0, result: 1, function: None },
    among { s_size: 2, s: S_3_6.as_ptr(), substring_i: 0, result: 2, function: None },
    among { s_size: 2, s: S_3_7.as_ptr(), substring_i: 0, result: 2, function: None },
    among { s_size: 2, s: S_3_8.as_ptr(), substring_i: 0, result: 2, function: None },
    among { s_size: 2, s: S_3_9.as_ptr(), substring_i: 0, result: 2, function: None },
    among { s_size: 2, s: S_3_10.as_ptr(), substring_i: 0, result: 1, function: None },
    among { s_size: 2, s: S_3_11.as_ptr(), substring_i: 0, result: 2, function: None },
    among { s_size: 2, s: S_3_12.as_ptr(), substring_i: 0, result: 1, function: None },
];

static S_4_0: [symbol; 2] = [b'e', b'd'];
static S_4_1: [symbol; 3] = [b'e', b'e', b'd'];
static S_4_2: [symbol; 3] = [b'i', b'n', b'g'];
static S_4_3: [symbol; 4] = [b'e', b'd', b'l', b'y'];
static S_4_4: [symbol; 5] = [b'e', b'e', b'd', b'l', b'y'];
static S_4_5: [symbol; 5] = [b'i', b'n', b'g', b'l', b'y'];

static A_4: [among; 6] = [
    among { s_size: 2, s: S_4_0.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 3, s: S_4_1.as_ptr(), substring_i: 0, result: 1, function: None },
    among { s_size: 3, s: S_4_2.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 4, s: S_4_3.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 5, s: S_4_4.as_ptr(), substring_i: 3, result: 1, function: None },
    among { s_size: 5, s: S_4_5.as_ptr(), substring_i: -1, result: 2, function: None },
];

static S_5_0: [symbol; 4] = [b'a', b'n', b'c', b'i'];
static S_5_1: [symbol; 4] = [b'e', b'n', b'c', b'i'];
static S_5_2: [symbol; 3] = [b'o', b'g', b'i'];
static S_5_3: [symbol; 2] = [b'l', b'i'];
static S_5_4: [symbol; 3] = [b'b', b'l', b'i'];
static S_5_5: [symbol; 4] = [b'a', b'b', b'l', b'i'];
static S_5_6: [symbol; 4] = [b'a', b'l', b'l', b'i'];
static S_5_7: [symbol; 5] = [b'f', b'u', b'l', b'l', b'i'];
static S_5_8: [symbol; 6] = [b'l', b'e', b's', b's', b'l', b'i'];
static S_5_9: [symbol; 5] = [b'o', b'u', b's', b'l', b'i'];
static S_5_10: [symbol; 5] = [b'e', b'n', b't', b'l', b'i'];
static S_5_11: [symbol; 5] = [b'a', b'l', b'i', b't', b'i'];
static S_5_12: [symbol; 6] = [b'b', b'i', b'l', b'i', b't', b'i'];
static S_5_13: [symbol; 5] = [b'i', b'v', b'i', b't', b'i'];
static S_5_14: [symbol; 6] = [b't', b'i', b'o', b'n', b'a', b'l'];
static S_5_15: [symbol; 7] = [b'a', b't', b'i', b'o', b'n', b'a', b'l'];
static S_5_16: [symbol; 5] = [b'a', b'l', b'i', b's', b'm'];
static S_5_17: [symbol; 5] = [b'a', b't', b'i', b'o', b'n'];
static S_5_18: [symbol; 7] = [b'i', b'z', b'a', b't', b'i', b'o', b'n'];
static S_5_19: [symbol; 4] = [b'i', b'z', b'e', b'r'];
static S_5_20: [symbol; 4] = [b'a', b't', b'o', b'r'];
static S_5_21: [symbol; 7] = [b'i', b'v', b'e', b'n', b'e', b's', b's'];
static S_5_22: [symbol; 7] = [b'f', b'u', b'l', b'n', b'e', b's', b's'];
static S_5_23: [symbol; 7] = [b'o', b'u', b's', b'n', b'e', b's', b's'];

static A_5: [among; 24] = [
    among { s_size: 4, s: S_5_0.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 4, s: S_5_1.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 3, s: S_5_2.as_ptr(), substring_i: -1, result: 13, function: None },
    among { s_size: 2, s: S_5_3.as_ptr(), substring_i: -1, result: 15, function: None },
    among { s_size: 3, s: S_5_4.as_ptr(), substring_i: 3, result: 12, function: None },
    among { s_size: 4, s: S_5_5.as_ptr(), substring_i: 4, result: 4, function: None },
    among { s_size: 4, s: S_5_6.as_ptr(), substring_i: 3, result: 8, function: None },
    among { s_size: 5, s: S_5_7.as_ptr(), substring_i: 3, result: 9, function: None },
    among { s_size: 6, s: S_5_8.as_ptr(), substring_i: 3, result: 14, function: None },
    among { s_size: 5, s: S_5_9.as_ptr(), substring_i: 3, result: 10, function: None },
    among { s_size: 5, s: S_5_10.as_ptr(), substring_i: 3, result: 5, function: None },
    among { s_size: 5, s: S_5_11.as_ptr(), substring_i: -1, result: 8, function: None },
    among { s_size: 6, s: S_5_12.as_ptr(), substring_i: -1, result: 12, function: None },
    among { s_size: 5, s: S_5_13.as_ptr(), substring_i: -1, result: 11, function: None },
    among { s_size: 6, s: S_5_14.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 7, s: S_5_15.as_ptr(), substring_i: 14, result: 7, function: None },
    among { s_size: 5, s: S_5_16.as_ptr(), substring_i: -1, result: 8, function: None },
    among { s_size: 5, s: S_5_17.as_ptr(), substring_i: -1, result: 7, function: None },
    among { s_size: 7, s: S_5_18.as_ptr(), substring_i: 17, result: 6, function: None },
    among { s_size: 4, s: S_5_19.as_ptr(), substring_i: -1, result: 6, function: None },
    among { s_size: 4, s: S_5_20.as_ptr(), substring_i: -1, result: 7, function: None },
    among { s_size: 7, s: S_5_21.as_ptr(), substring_i: -1, result: 11, function: None },
    among { s_size: 7, s: S_5_22.as_ptr(), substring_i: -1, result: 9, function: None },
    among { s_size: 7, s: S_5_23.as_ptr(), substring_i: -1, result: 10, function: None },
];

static S_6_0: [symbol; 5] = [b'i', b'c', b'a', b't', b'e'];
static S_6_1: [symbol; 5] = [b'a', b't', b'i', b'v', b'e'];
static S_6_2: [symbol; 5] = [b'a', b'l', b'i', b'z', b'e'];
static S_6_3: [symbol; 5] = [b'i', b'c', b'i', b't', b'i'];
static S_6_4: [symbol; 4] = [b'i', b'c', b'a', b'l'];
static S_6_5: [symbol; 6] = [b't', b'i', b'o', b'n', b'a', b'l'];
static S_6_6: [symbol; 7] = [b'a', b't', b'i', b'o', b'n', b'a', b'l'];
static S_6_7: [symbol; 3] = [b'f', b'u', b'l'];
static S_6_8: [symbol; 4] = [b'n', b'e', b's', b's'];

static A_6: [among; 9] = [
    among { s_size: 5, s: S_6_0.as_ptr(), substring_i: -1, result: 4, function: None },
    among { s_size: 5, s: S_6_1.as_ptr(), substring_i: -1, result: 6, function: None },
    among { s_size: 5, s: S_6_2.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 5, s: S_6_3.as_ptr(), substring_i: -1, result: 4, function: None },
    among { s_size: 4, s: S_6_4.as_ptr(), substring_i: -1, result: 4, function: None },
    among { s_size: 6, s: S_6_5.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 7, s: S_6_6.as_ptr(), substring_i: 5, result: 2, function: None },
    among { s_size: 3, s: S_6_7.as_ptr(), substring_i: -1, result: 5, function: None },
    among { s_size: 4, s: S_6_8.as_ptr(), substring_i: -1, result: 5, function: None },
];

static S_7_0: [symbol; 2] = [b'i', b'c'];
static S_7_1: [symbol; 4] = [b'a', b'n', b'c', b'e'];
static S_7_2: [symbol; 4] = [b'e', b'n', b'c', b'e'];
static S_7_3: [symbol; 4] = [b'a', b'b', b'l', b'e'];
static S_7_4: [symbol; 4] = [b'i', b'b', b'l', b'e'];
static S_7_5: [symbol; 3] = [b'a', b't', b'e'];
static S_7_6: [symbol; 3] = [b'i', b'v', b'e'];
static S_7_7: [symbol; 3] = [b'i', b'z', b'e'];
static S_7_8: [symbol; 3] = [b'i', b't', b'i'];
static S_7_9: [symbol; 2] = [b'a', b'l'];
static S_7_10: [symbol; 3] = [b'i', b's', b'm'];
static S_7_11: [symbol; 3] = [b'i', b'o', b'n'];
static S_7_12: [symbol; 2] = [b'e', b'r'];
static S_7_13: [symbol; 3] = [b'o', b'u', b's'];
static S_7_14: [symbol; 3] = [b'a', b'n', b't'];
static S_7_15: [symbol; 3] = [b'e', b'n', b't'];
static S_7_16: [symbol; 4] = [b'm', b'e', b'n', b't'];
static S_7_17: [symbol; 5] = [b'e', b'm', b'e', b'n', b't'];

static A_7: [among; 18] = [
    among { s_size: 2, s: S_7_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_7_1.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_7_2.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_7_3.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_7_4.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_7_5.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_7_6.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_7_7.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_7_8.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_7_9.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_7_10.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_7_11.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 2, s: S_7_12.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_7_13.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_7_14.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_7_15.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_7_16.as_ptr(), substring_i: 15, result: 1, function: None },
    among { s_size: 5, s: S_7_17.as_ptr(), substring_i: 16, result: 1, function: None },
];

static S_8_0: [symbol; 1] = [b'e'];
static S_8_1: [symbol; 1] = [b'l'];

static A_8: [among; 2] = [
    among { s_size: 1, s: S_8_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 1, s: S_8_1.as_ptr(), substring_i: -1, result: 2, function: None },
];

static S_9_0: [symbol; 7] = [b's', b'u', b'c', b'c', b'e', b'e', b'd'];
static S_9_1: [symbol; 7] = [b'p', b'r', b'o', b'c', b'e', b'e', b'd'];
static S_9_2: [symbol; 6] = [b'e', b'x', b'c', b'e', b'e', b'd'];
static S_9_3: [symbol; 7] = [b'c', b'a', b'n', b'n', b'i', b'n', b'g'];
static S_9_4: [symbol; 6] = [b'i', b'n', b'n', b'i', b'n', b'g'];
static S_9_5: [symbol; 7] = [b'e', b'a', b'r', b'r', b'i', b'n', b'g'];
static S_9_6: [symbol; 7] = [b'h', b'e', b'r', b'r', b'i', b'n', b'g'];
static S_9_7: [symbol; 6] = [b'o', b'u', b't', b'i', b'n', b'g'];

static A_9: [among; 8] = [
    among { s_size: 7, s: S_9_0.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 7, s: S_9_1.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 6, s: S_9_2.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 7, s: S_9_3.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 6, s: S_9_4.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 7, s: S_9_5.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 7, s: S_9_6.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 6, s: S_9_7.as_ptr(), substring_i: -1, result: -1, function: None },
];

static S_10_0: [symbol; 5] = [b'a', b'n', b'd', b'e', b's'];
static S_10_1: [symbol; 5] = [b'a', b't', b'l', b'a', b's'];
static S_10_2: [symbol; 4] = [b'b', b'i', b'a', b's'];
static S_10_3: [symbol; 6] = [b'c', b'o', b's', b'm', b'o', b's'];
static S_10_4: [symbol; 5] = [b'd', b'y', b'i', b'n', b'g'];
static S_10_5: [symbol; 5] = [b'e', b'a', b'r', b'l', b'y'];
static S_10_6: [symbol; 6] = [b'g', b'e', b'n', b't', b'l', b'y'];
static S_10_7: [symbol; 4] = [b'h', b'o', b'w', b'e'];
static S_10_8: [symbol; 4] = [b'i', b'd', b'l', b'y'];
static S_10_9: [symbol; 5] = [b'l', b'y', b'i', b'n', b'g'];
static S_10_10: [symbol; 4] = [b'n', b'e', b'w', b's'];
static S_10_11: [symbol; 4] = [b'o', b'n', b'l', b'y'];
static S_10_12: [symbol; 6] = [b's', b'i', b'n', b'g', b'l', b'y'];
static S_10_13: [symbol; 5] = [b's', b'k', b'i', b'e', b's'];
static S_10_14: [symbol; 4] = [b's', b'k', b'i', b's'];
static S_10_15: [symbol; 3] = [b's', b'k', b'y'];
static S_10_16: [symbol; 5] = [b't', b'y', b'i', b'n', b'g'];
static S_10_17: [symbol; 4] = [b'u', b'g', b'l', b'y'];

static A_10: [among; 18] = [
    among { s_size: 5, s: S_10_0.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 5, s: S_10_1.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 4, s: S_10_2.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 6, s: S_10_3.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 5, s: S_10_4.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 5, s: S_10_5.as_ptr(), substring_i: -1, result: 9, function: None },
    among { s_size: 6, s: S_10_6.as_ptr(), substring_i: -1, result: 7, function: None },
    among { s_size: 4, s: S_10_7.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 4, s: S_10_8.as_ptr(), substring_i: -1, result: 6, function: None },
    among { s_size: 5, s: S_10_9.as_ptr(), substring_i: -1, result: 4, function: None },
    among { s_size: 4, s: S_10_10.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 4, s: S_10_11.as_ptr(), substring_i: -1, result: 10, function: None },
    among { s_size: 6, s: S_10_12.as_ptr(), substring_i: -1, result: 11, function: None },
    among { s_size: 5, s: S_10_13.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 4, s: S_10_14.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_10_15.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 5, s: S_10_16.as_ptr(), substring_i: -1, result: 5, function: None },
    among { s_size: 4, s: S_10_17.as_ptr(), substring_i: -1, result: 8, function: None },
];

static G_AEO: [c_uchar; 2] = [17, 64];

static G_V: [c_uchar; 4] = [17, 65, 16, 1];

static G_V_WXY: [c_uchar; 5] = [1, 17, 65, 208, 1];

static G_VALID_LI: [c_uchar; 3] = [55, 141, 2];

static S_0: [symbol; 1] = [b'Y'];
static S_1: [symbol; 1] = [b'Y'];
static S_2: [symbol; 2] = [b's', b's'];
static S_3: [symbol; 1] = [b'i'];
static S_4: [symbol; 2] = [b'i', b'e'];
static S_5: [symbol; 2] = [b'e', b'e'];
static S_6: [symbol; 1] = [b'e'];
static S_7: [symbol; 1] = [b'e'];
static S_8: [symbol; 1] = [b'i'];
static S_9: [symbol; 4] = [b't', b'i', b'o', b'n'];
static S_10: [symbol; 4] = [b'e', b'n', b'c', b'e'];
static S_11: [symbol; 4] = [b'a', b'n', b'c', b'e'];
static S_12: [symbol; 4] = [b'a', b'b', b'l', b'e'];
static S_13: [symbol; 3] = [b'e', b'n', b't'];
static S_14: [symbol; 3] = [b'i', b'z', b'e'];
static S_15: [symbol; 3] = [b'a', b't', b'e'];
static S_16: [symbol; 2] = [b'a', b'l'];
static S_17: [symbol; 3] = [b'f', b'u', b'l'];
static S_18: [symbol; 3] = [b'o', b'u', b's'];
static S_19: [symbol; 3] = [b'i', b'v', b'e'];
static S_20: [symbol; 3] = [b'b', b'l', b'e'];
static S_21: [symbol; 2] = [b'o', b'g'];
static S_22: [symbol; 4] = [b'l', b'e', b's', b's'];
static S_23: [symbol; 4] = [b't', b'i', b'o', b'n'];
static S_24: [symbol; 3] = [b'a', b't', b'e'];
static S_25: [symbol; 2] = [b'a', b'l'];
static S_26: [symbol; 2] = [b'i', b'c'];
static S_27: [symbol; 3] = [b's', b'k', b'i'];
static S_28: [symbol; 3] = [b's', b'k', b'y'];
static S_29: [symbol; 3] = [b'd', b'i', b'e'];
static S_30: [symbol; 3] = [b'l', b'i', b'e'];
static S_31: [symbol; 3] = [b't', b'i', b'e'];
static S_32: [symbol; 3] = [b'i', b'd', b'l'];
static S_33: [symbol; 5] = [b'g', b'e', b'n', b't', b'l'];
static S_34: [symbol; 4] = [b'u', b'g', b'l', b'i'];
static S_35: [symbol; 5] = [b'e', b'a', b'r', b'l', b'i'];
static S_36: [symbol; 4] = [b'o', b'n', b'l', b'i'];
static S_37: [symbol; 5] = [b's', b'i', b'n', b'g', b'l'];
static S_38: [symbol; 1] = [b'y'];

// ---------------------------------------------------------------------------
// stemmer functions
// ---------------------------------------------------------------------------

unsafe extern "C" fn r_prelude(z: *mut SN_env) -> c_int {
    *(*z).I.offset(2) = 0;
    {
        let c1 = (*z).c;
        (*z).bra = (*z).c;
        'lab0: {
            if (*z).c == (*z).l || *(*z).p.offset((*z).c as isize) != b'\'' {
                break 'lab0;
            }
            (*z).c += 1;
            (*z).ket = (*z).c;
            {
                let ret = slice_del(z);
                if ret < 0 {
                    return ret;
                }
            }
        }
        (*z).c = c1;
    }
    {
        let c2 = (*z).c;
        'lab1: {
            (*z).bra = (*z).c;
            if (*z).c == (*z).l || *(*z).p.offset((*z).c as isize) != b'y' {
                break 'lab1;
            }
            (*z).c += 1;
            (*z).ket = (*z).c;
            {
                let ret = slice_from_s(z, 1, S_0.as_ptr());
                if ret < 0 {
                    return ret;
                }
            }
            *(*z).I.offset(2) = 1;
        }
        (*z).c = c2;
    }
    {
        let c3 = (*z).c;
        'loop3: loop {
            let c4 = (*z).c;
            'lab3: {
                'inner: loop {
                    let c5 = (*z).c;
                    'lab4: {
                        if in_grouping_U(z, G_V.as_ptr(), 97, 121, 0) != 0 {
                            break 'lab4;
                        }
                        (*z).bra = (*z).c;
                        if (*z).c == (*z).l || *(*z).p.offset((*z).c as isize) != b'y' {
                            break 'lab4;
                        }
                        (*z).c += 1;
                        (*z).ket = (*z).c;
                        (*z).c = c5;
                        break 'inner;
                    }
                    // lab4:
                    (*z).c = c5;
                    {
                        let ret = skip_utf8((*z).p, (*z).c, (*z).l, 1);
                        if ret < 0 {
                            break 'lab3;
                        }
                        (*z).c = ret;
                    }
                }
                {
                    let ret = slice_from_s(z, 1, S_1.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
                *(*z).I.offset(2) = 1;
                continue 'loop3;
            }
            // lab3:
            (*z).c = c4;
            break 'loop3;
        }
        (*z).c = c3;
    }
    1
}

unsafe extern "C" fn r_mark_regions(z: *mut SN_env) -> c_int {
    *(*z).I.offset(1) = (*z).l;
    *(*z).I.offset(0) = (*z).l;
    {
        let c1 = (*z).c;
        'lab0: {
            {
                let c2 = (*z).c;
                'lab1: {
                    'lab2: {
                        if (*z).c + 4 >= (*z).l
                            || (*(*z).p.offset(((*z).c + 4) as isize) as c_int >> 5) != 3
                            || ((2375680 >> (*(*z).p.offset(((*z).c + 4) as isize) as c_int & 0x1f)) & 1) == 0
                        {
                            break 'lab2;
                        }
                        if find_among(z, A_0.as_ptr(), 3) == 0 {
                            break 'lab2;
                        }
                        break 'lab1;
                    }
                    // lab2:
                    (*z).c = c2;

                    {
                        let ret = out_grouping_U(z, G_V.as_ptr(), 97, 121, 1);
                        if ret < 0 {
                            break 'lab0;
                        }
                        (*z).c += ret;
                    }

                    {
                        let ret = in_grouping_U(z, G_V.as_ptr(), 97, 121, 1);
                        if ret < 0 {
                            break 'lab0;
                        }
                        (*z).c += ret;
                    }
                }
            }
            // lab1:
            *(*z).I.offset(1) = (*z).c;

            {
                let ret = out_grouping_U(z, G_V.as_ptr(), 97, 121, 1);
                if ret < 0 {
                    break 'lab0;
                }
                (*z).c += ret;
            }

            {
                let ret = in_grouping_U(z, G_V.as_ptr(), 97, 121, 1);
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

unsafe extern "C" fn r_shortv(z: *mut SN_env) -> c_int {
    'lab0: {
        let m1 = (*z).l - (*z).c;
        let _ = m1;
        'lab1: {
            if out_grouping_b_U(z, G_V_WXY.as_ptr(), 89, 121, 0) != 0 {
                break 'lab1;
            }
            if in_grouping_b_U(z, G_V.as_ptr(), 97, 121, 0) != 0 {
                break 'lab1;
            }
            if out_grouping_b_U(z, G_V.as_ptr(), 97, 121, 0) != 0 {
                break 'lab1;
            }
            break 'lab0;
        }
        // lab1:
        (*z).c = (*z).l - m1;
        if out_grouping_b_U(z, G_V.as_ptr(), 97, 121, 0) != 0 {
            return 0;
        }
        if in_grouping_b_U(z, G_V.as_ptr(), 97, 121, 0) != 0 {
            return 0;
        }
        if (*z).c > (*z).lb {
            return 0;
        }
    }
    // lab0:
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
    {
        let m1 = (*z).l - (*z).c;
        let _ = m1;
        'lab0: {
            (*z).ket = (*z).c;
            if (*z).c <= (*z).lb
                || (*(*z).p.offset(((*z).c - 1) as isize) != 39
                    && *(*z).p.offset(((*z).c - 1) as isize) != 115)
            {
                (*z).c = (*z).l - m1;
                break 'lab0;
            }
            if find_among_b(z, A_1.as_ptr(), 3) == 0 {
                (*z).c = (*z).l - m1;
                break 'lab0;
            }
            (*z).bra = (*z).c;
            {
                let ret = slice_del(z);
                if ret < 0 {
                    return ret;
                }
            }
        }
    }
    (*z).ket = (*z).c;
    if (*z).c <= (*z).lb
        || (*(*z).p.offset(((*z).c - 1) as isize) != 100
            && *(*z).p.offset(((*z).c - 1) as isize) != 115)
    {
        return 0;
    }
    among_var = find_among_b(z, A_2.as_ptr(), 6);
    if among_var == 0 {
        return 0;
    }
    (*z).bra = (*z).c;
    match among_var {
        1 => {
            let ret = slice_from_s(z, 2, S_2.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        2 => {
            'lab1: {
                let m2 = (*z).l - (*z).c;
                let _ = m2;
                'lab2: {
                    {
                        let ret = skip_b_utf8((*z).p, (*z).c, (*z).lb, 2);
                        if ret < 0 {
                            break 'lab2;
                        }
                        (*z).c = ret;
                    }
                    {
                        let ret = slice_from_s(z, 1, S_3.as_ptr());
                        if ret < 0 {
                            return ret;
                        }
                    }
                    break 'lab1;
                }
                // lab2:
                (*z).c = (*z).l - m2;
                {
                    let ret = slice_from_s(z, 2, S_4.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
            }
        }
        3 => {
            {
                let ret = skip_b_utf8((*z).p, (*z).c, (*z).lb, 1);
                if ret < 0 {
                    return 0;
                }
                (*z).c = ret;
            }

            {
                let ret = out_grouping_b_U(z, G_V.as_ptr(), 97, 121, 1);
                if ret < 0 {
                    return 0;
                }
                (*z).c -= ret;
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

unsafe extern "C" fn r_Step_1b(z: *mut SN_env) -> c_int {
    let among_var;
    (*z).ket = (*z).c;
    if (*z).c - 1 <= (*z).lb
        || (*(*z).p.offset(((*z).c - 1) as isize) as c_int >> 5) != 3
        || ((33554576 >> (*(*z).p.offset(((*z).c - 1) as isize) as c_int & 0x1f)) & 1) == 0
    {
        return 0;
    }
    among_var = find_among_b(z, A_4.as_ptr(), 6);
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
                let ret = slice_from_s(z, 2, S_5.as_ptr());
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
            (*z).ket = (*z).c;
            (*z).bra = (*z).c;
            {
                let m_test2 = (*z).l - (*z).c;
                let mut among_var2;
                if (*z).c - 1 <= (*z).lb
                    || (*(*z).p.offset(((*z).c - 1) as isize) as c_int >> 5) != 3
                    || ((68514004 >> (*(*z).p.offset(((*z).c - 1) as isize) as c_int & 0x1f)) & 1) == 0
                {
                    among_var2 = 3;
                } else {
                    among_var2 = find_among_b(z, A_3.as_ptr(), 13);
                }
                match among_var2 {
                    1 => {
                        {
                            let ret = slice_from_s(z, 1, S_6.as_ptr());
                            if ret < 0 {
                                return ret;
                            }
                        }
                        return 0;
                    }
                    2 => {
                        let m3 = (*z).l - (*z).c;
                        let _ = m3;
                        'lab0: {
                            if in_grouping_b_U(z, G_AEO.as_ptr(), 97, 111, 0) != 0 {
                                break 'lab0;
                            }
                            if (*z).c > (*z).lb {
                                break 'lab0;
                            }
                            return 0;
                        }
                        // lab0:
                        (*z).c = (*z).l - m3;
                    }
                    3 => {
                        if (*z).c != *(*z).I.offset(1) {
                            return 0;
                        }
                        {
                            let m_test4 = (*z).l - (*z).c;
                            {
                                let ret = r_shortv(z);
                                if ret <= 0 {
                                    return ret;
                                }
                            }
                            (*z).c = (*z).l - m_test4;
                        }
                        {
                            let ret = slice_from_s(z, 1, S_7.as_ptr());
                            if ret < 0 {
                                return ret;
                            }
                        }
                        return 0;
                    }
                    _ => {}
                }
                let _ = &mut among_var2;
                (*z).c = (*z).l - m_test2;
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
        }
        _ => {}
    }
    1
}

unsafe extern "C" fn r_Step_1c(z: *mut SN_env) -> c_int {
    (*z).ket = (*z).c;
    'lab0: {
        let m1 = (*z).l - (*z).c;
        let _ = m1;
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
    // lab0:
    (*z).bra = (*z).c;
    if out_grouping_b_U(z, G_V.as_ptr(), 97, 121, 0) != 0 {
        return 0;
    }

    'lab2: {
        if (*z).c > (*z).lb {
            break 'lab2;
        }
        return 0;
    }
    // lab2:
    {
        let ret = slice_from_s(z, 1, S_8.as_ptr());
        if ret < 0 {
            return ret;
        }
    }
    1
}

unsafe extern "C" fn r_Step_2(z: *mut SN_env) -> c_int {
    let among_var;
    (*z).ket = (*z).c;
    if (*z).c - 1 <= (*z).lb
        || (*(*z).p.offset(((*z).c - 1) as isize) as c_int >> 5) != 3
        || ((815616 >> (*(*z).p.offset(((*z).c - 1) as isize) as c_int & 0x1f)) & 1) == 0
    {
        return 0;
    }
    among_var = find_among_b(z, A_5.as_ptr(), 24);
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
            let ret = slice_from_s(z, 4, S_9.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        2 => {
            let ret = slice_from_s(z, 4, S_10.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        3 => {
            let ret = slice_from_s(z, 4, S_11.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        4 => {
            let ret = slice_from_s(z, 4, S_12.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        5 => {
            let ret = slice_from_s(z, 3, S_13.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        6 => {
            let ret = slice_from_s(z, 3, S_14.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        7 => {
            let ret = slice_from_s(z, 3, S_15.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        8 => {
            let ret = slice_from_s(z, 2, S_16.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        9 => {
            let ret = slice_from_s(z, 3, S_17.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        10 => {
            let ret = slice_from_s(z, 3, S_18.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        11 => {
            let ret = slice_from_s(z, 3, S_19.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        12 => {
            let ret = slice_from_s(z, 3, S_20.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        13 => {
            if (*z).c <= (*z).lb || *(*z).p.offset(((*z).c - 1) as isize) != b'l' {
                return 0;
            }
            (*z).c -= 1;
            {
                let ret = slice_from_s(z, 2, S_21.as_ptr());
                if ret < 0 {
                    return ret;
                }
            }
        }
        14 => {
            let ret = slice_from_s(z, 4, S_22.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        15 => {
            if in_grouping_b_U(z, G_VALID_LI.as_ptr(), 99, 116, 0) != 0 {
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

unsafe extern "C" fn r_Step_3(z: *mut SN_env) -> c_int {
    let among_var;
    (*z).ket = (*z).c;
    if (*z).c - 2 <= (*z).lb
        || (*(*z).p.offset(((*z).c - 1) as isize) as c_int >> 5) != 3
        || ((528928 >> (*(*z).p.offset(((*z).c - 1) as isize) as c_int & 0x1f)) & 1) == 0
    {
        return 0;
    }
    among_var = find_among_b(z, A_6.as_ptr(), 9);
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
            let ret = slice_from_s(z, 4, S_23.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        2 => {
            let ret = slice_from_s(z, 3, S_24.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        3 => {
            let ret = slice_from_s(z, 2, S_25.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        4 => {
            let ret = slice_from_s(z, 2, S_26.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        5 => {
            let ret = slice_del(z);
            if ret < 0 {
                return ret;
            }
        }
        6 => {
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

unsafe extern "C" fn r_Step_4(z: *mut SN_env) -> c_int {
    let among_var;
    (*z).ket = (*z).c;
    if (*z).c - 1 <= (*z).lb
        || (*(*z).p.offset(((*z).c - 1) as isize) as c_int >> 5) != 3
        || ((1864232 >> (*(*z).p.offset(((*z).c - 1) as isize) as c_int & 0x1f)) & 1) == 0
    {
        return 0;
    }
    among_var = find_among_b(z, A_7.as_ptr(), 18);
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
            'lab0: {
                let m1 = (*z).l - (*z).c;
                let _ = m1;
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

unsafe extern "C" fn r_Step_5(z: *mut SN_env) -> c_int {
    let among_var;
    (*z).ket = (*z).c;
    if (*z).c <= (*z).lb
        || (*(*z).p.offset(((*z).c - 1) as isize) != 101
            && *(*z).p.offset(((*z).c - 1) as isize) != 108)
    {
        return 0;
    }
    among_var = find_among_b(z, A_8.as_ptr(), 2);
    if among_var == 0 {
        return 0;
    }
    (*z).bra = (*z).c;
    match among_var {
        1 => {
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
                    let _ = m1;
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
        }
        2 => {
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
        }
        _ => {}
    }
    1
}

unsafe extern "C" fn r_exception2(z: *mut SN_env) -> c_int {
    (*z).ket = (*z).c;
    if (*z).c - 5 <= (*z).lb
        || (*(*z).p.offset(((*z).c - 1) as isize) != 100
            && *(*z).p.offset(((*z).c - 1) as isize) != 103)
    {
        return 0;
    }
    if find_among_b(z, A_9.as_ptr(), 8) == 0 {
        return 0;
    }
    (*z).bra = (*z).c;
    if (*z).c > (*z).lb {
        return 0;
    }
    1
}

unsafe extern "C" fn r_exception1(z: *mut SN_env) -> c_int {
    let among_var;
    (*z).bra = (*z).c;
    if (*z).c + 2 >= (*z).l
        || (*(*z).p.offset(((*z).c + 2) as isize) as c_int >> 5) != 3
        || ((42750482 >> (*(*z).p.offset(((*z).c + 2) as isize) as c_int & 0x1f)) & 1) == 0
    {
        return 0;
    }
    among_var = find_among(z, A_10.as_ptr(), 18);
    if among_var == 0 {
        return 0;
    }
    (*z).ket = (*z).c;
    if (*z).c < (*z).l {
        return 0;
    }
    match among_var {
        1 => {
            let ret = slice_from_s(z, 3, S_27.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        2 => {
            let ret = slice_from_s(z, 3, S_28.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        3 => {
            let ret = slice_from_s(z, 3, S_29.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        4 => {
            let ret = slice_from_s(z, 3, S_30.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        5 => {
            let ret = slice_from_s(z, 3, S_31.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        6 => {
            let ret = slice_from_s(z, 3, S_32.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        7 => {
            let ret = slice_from_s(z, 5, S_33.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        8 => {
            let ret = slice_from_s(z, 4, S_34.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        9 => {
            let ret = slice_from_s(z, 5, S_35.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        10 => {
            let ret = slice_from_s(z, 4, S_36.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        11 => {
            let ret = slice_from_s(z, 5, S_37.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        _ => {}
    }
    1
}

unsafe extern "C" fn r_postlude(z: *mut SN_env) -> c_int {
    if *(*z).I.offset(2) == 0 {
        return 0;
    }
    'loop0: loop {
        let c1 = (*z).c;
        'lab0: {
            'inner: loop {
                let c2 = (*z).c;
                'lab1: {
                    (*z).bra = (*z).c;
                    if (*z).c == (*z).l || *(*z).p.offset((*z).c as isize) != b'Y' {
                        break 'lab1;
                    }
                    (*z).c += 1;
                    (*z).ket = (*z).c;
                    (*z).c = c2;
                    break 'inner;
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
            {
                let ret = slice_from_s(z, 1, S_38.as_ptr());
                if ret < 0 {
                    return ret;
                }
            }
            continue 'loop0;
        }
        // lab0:
        (*z).c = c1;
        break 'loop0;
    }
    1
}

#[no_mangle]
pub unsafe extern "C" fn english_UTF_8_stem(z: *mut SN_env) -> c_int {
    'lab0: {
        let c1 = (*z).c;
        'lab1: {
            {
                let ret = r_exception1(z);
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
        (*z).c = c1;
        'lab2: {
            {
                let c2 = (*z).c;
                'lab3: {
                    {
                        let ret = skip_utf8((*z).p, (*z).c, (*z).l, 3);
                        if ret < 0 {
                            break 'lab3;
                        }
                        (*z).c = ret;
                    }
                    break 'lab2;
                }
                // lab3:
                (*z).c = c2;
            }
            break 'lab0;
        }
        // lab2:
        (*z).c = c1;

        {
            let ret = r_prelude(z);
            if ret < 0 {
                return ret;
            }
        }

        {
            let ret = r_mark_regions(z);
            if ret < 0 {
                return ret;
            }
        }
        (*z).lb = (*z).c;
        (*z).c = (*z).l;

        'lab4: {
            {
                let m3 = (*z).l - (*z).c;
                let _ = m3;
                {
                    let ret = r_Step_1a(z);
                    if ret < 0 {
                        return ret;
                    }
                }
                (*z).c = (*z).l - m3;
            }
            {
                let m4 = (*z).l - (*z).c;
                let _ = m4;
                'lab5: {
                    {
                        let ret = r_exception2(z);
                        if ret == 0 {
                            break 'lab5;
                        }
                        if ret < 0 {
                            return ret;
                        }
                    }
                    break 'lab4;
                }
                // lab5:
                (*z).c = (*z).l - m4;
                {
                    let m5 = (*z).l - (*z).c;
                    let _ = m5;
                    {
                        let ret = r_Step_1b(z);
                        if ret < 0 {
                            return ret;
                        }
                    }
                    (*z).c = (*z).l - m5;
                }
                {
                    let m6 = (*z).l - (*z).c;
                    let _ = m6;
                    {
                        let ret = r_Step_1c(z);
                        if ret < 0 {
                            return ret;
                        }
                    }
                    (*z).c = (*z).l - m6;
                }
                {
                    let m7 = (*z).l - (*z).c;
                    let _ = m7;
                    {
                        let ret = r_Step_2(z);
                        if ret < 0 {
                            return ret;
                        }
                    }
                    (*z).c = (*z).l - m7;
                }
                {
                    let m8 = (*z).l - (*z).c;
                    let _ = m8;
                    {
                        let ret = r_Step_3(z);
                        if ret < 0 {
                            return ret;
                        }
                    }
                    (*z).c = (*z).l - m8;
                }
                {
                    let m9 = (*z).l - (*z).c;
                    let _ = m9;
                    {
                        let ret = r_Step_4(z);
                        if ret < 0 {
                            return ret;
                        }
                    }
                    (*z).c = (*z).l - m9;
                }
                {
                    let m10 = (*z).l - (*z).c;
                    let _ = m10;
                    {
                        let ret = r_Step_5(z);
                        if ret < 0 {
                            return ret;
                        }
                    }
                    (*z).c = (*z).l - m10;
                }
            }
        }
        // lab4:
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
    }
    // lab0:
    1
}

#[no_mangle]
pub unsafe extern "C" fn english_UTF_8_create_env() -> *mut SN_env {
    SN_create_env(0, 3)
}

#[no_mangle]
pub unsafe extern "C" fn english_UTF_8_close_env(z: *mut SN_env) {
    SN_close_env(z, 0);
}

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snowball::api::SN_set_current;

    // Run the full stemming pipeline over `word` and return the resulting
    // stemmed bytes.
    unsafe fn stem(word: &[u8]) -> Vec<u8> {
        let z = english_UTF_8_create_env();
        assert!(!z.is_null());
        let rc = SN_set_current(z, word.len() as c_int, word.as_ptr());
        assert!(rc >= 0);
        let rc = english_UTF_8_stem(z);
        assert!(rc >= 0, "stem returned {rc}");
        let out = std::slice::from_raw_parts((*z).p, SIZE((*z).p) as usize).to_vec();
        english_UTF_8_close_env(z);
        out
    }

    // A short root with no strippable suffix is returned unchanged.
    #[test]
    fn short_root_unchanged() {
        unsafe {
            assert_eq!(stem(b"bil"), b"bil".to_vec());
        }
    }

    // Idempotence: stemming a stem yields the same stem, and stems stay
    // non-empty.
    #[test]
    fn idempotent() {
        unsafe {
            for w in [
                &b"running"[..],
                &b"happily"[..],
                &b"nationalization"[..],
                &b"conditional"[..],
                &b"argument"[..],
                &b"sensational"[..],
            ] {
                let once = stem(w);
                let twice = stem(&once);
                assert_eq!(once, twice, "not idempotent for {:?}", w);
                assert!(!once.is_empty());
            }
        }
    }

    // Suffix stripping shrinks (never grows) the word and yields a non-empty
    // stem.
    #[test]
    fn suffix_stripped_nonempty() {
        unsafe {
            let r = stem(b"consignment");
            assert!(!r.is_empty());
            assert!(r.len() <= "consignment".len());
        }
    }
}
