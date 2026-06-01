//! Yiddish Snowball stemmer (UTF-8).
//!
//! 1:1 translation of the generated
//! `src/backend/snowball/libstemmer/stem_UTF_8_yiddish.c` (Snowball 2.2.0),
//! merged with its header `stem_UTF_8_yiddish.h`. The runtime helpers come from
//! `crate::snowball::api` and `crate::snowball::utilities`.

#![allow(unreachable_code)]

use crate::prelude::*;

use crate::snowball::utilities::in_grouping_b_U;
use crate::snowball::api::{among, symbol, SN_close_env, SN_create_env, SN_env, SIZE};
use crate::snowball::utilities::{
    eq_s, eq_s_b, find_among, find_among_b, in_grouping_U, out_grouping_U, skip_b_utf8, skip_utf8,
    slice_del, slice_from_s,
};

// ---------------------------------------------------------------------------
// among string tables
// ---------------------------------------------------------------------------

static S_0_0: [symbol; 4] = [0xD7, 0x95, 0xD7, 0x95];
static S_0_1: [symbol; 4] = [0xD7, 0x95, 0xD7, 0x99];
static S_0_2: [symbol; 4] = [0xD7, 0x99, 0xD7, 0x99];
static S_0_3: [symbol; 2] = [0xD7, 0x9A];
static S_0_4: [symbol; 2] = [0xD7, 0x9D];
static S_0_5: [symbol; 2] = [0xD7, 0x9F];
static S_0_6: [symbol; 2] = [0xD7, 0xA3];
static S_0_7: [symbol; 2] = [0xD7, 0xA5];

static A_0: [among; 8] = [
    among { s_size: 4, s: S_0_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_0_1.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 4, s: S_0_2.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 2, s: S_0_3.as_ptr(), substring_i: -1, result: 4, function: None },
    among { s_size: 2, s: S_0_4.as_ptr(), substring_i: -1, result: 5, function: None },
    among { s_size: 2, s: S_0_5.as_ptr(), substring_i: -1, result: 6, function: None },
    among { s_size: 2, s: S_0_6.as_ptr(), substring_i: -1, result: 7, function: None },
    among { s_size: 2, s: S_0_7.as_ptr(), substring_i: -1, result: 8, function: None },
];

static S_1_0: [symbol; 10] = [0xD7, 0x90, 0xD7, 0x93, 0xD7, 0x95, 0xD7, 0xA8, 0xD7, 0x9B];
static S_1_1: [symbol; 8] = [0xD7, 0x90, 0xD7, 0x94, 0xD7, 0x99, 0xD7, 0xA0];
static S_1_2: [symbol; 8] = [0xD7, 0x90, 0xD7, 0x94, 0xD7, 0xA2, 0xD7, 0xA8];
static S_1_3: [symbol; 8] = [0xD7, 0x90, 0xD7, 0x94, 0xD7, 0xB2, 0xD7, 0x9E];
static S_1_4: [symbol; 6] = [0xD7, 0x90, 0xD7, 0x95, 0xD7, 0x9E];
static S_1_5: [symbol; 12] = [
    0xD7, 0x90, 0xD7, 0x95, 0xD7, 0xA0, 0xD7, 0x98, 0xD7, 0xA2, 0xD7, 0xA8,
];
static S_1_6: [symbol; 10] = [0xD7, 0x90, 0xD7, 0x99, 0xD7, 0x91, 0xD7, 0xA2, 0xD7, 0xA8];
static S_1_7: [symbol; 4] = [0xD7, 0x90, 0xD7, 0xA0];
static S_1_8: [symbol; 6] = [0xD7, 0x90, 0xD7, 0xA0, 0xD7, 0x98];
static S_1_9: [symbol; 14] = [
    0xD7, 0x90, 0xD7, 0xA0, 0xD7, 0x98, 0xD7, 0xA7, 0xD7, 0xA2, 0xD7, 0x92, 0xD7, 0xA0,
];
static S_1_10: [symbol; 12] = [
    0xD7, 0x90, 0xD7, 0xA0, 0xD7, 0x99, 0xD7, 0x93, 0xD7, 0xA2, 0xD7, 0xA8,
];
static S_1_11: [symbol; 4] = [0xD7, 0x90, 0xD7, 0xA4];
static S_1_12: [symbol; 8] = [0xD7, 0x90, 0xD7, 0xA4, 0xD7, 0x99, 0xD7, 0xA8];
static S_1_13: [symbol; 10] = [0xD7, 0x90, 0xD7, 0xA7, 0xD7, 0xA2, 0xD7, 0x92, 0xD7, 0xA0];
static S_1_14: [symbol; 8] = [0xD7, 0x90, 0xD7, 0xA8, 0xD7, 0x90, 0xD7, 0xA4];
static S_1_15: [symbol; 8] = [0xD7, 0x90, 0xD7, 0xA8, 0xD7, 0x95, 0xD7, 0x9E];
static S_1_16: [symbol; 14] = [
    0xD7, 0x90, 0xD7, 0xA8, 0xD7, 0x95, 0xD7, 0xA0, 0xD7, 0x98, 0xD7, 0xA2, 0xD7, 0xA8,
];
static S_1_17: [symbol; 12] = [
    0xD7, 0x90, 0xD7, 0xA8, 0xD7, 0x99, 0xD7, 0x91, 0xD7, 0xA2, 0xD7, 0xA8,
];
static S_1_18: [symbol; 8] = [0xD7, 0x90, 0xD7, 0xA8, 0xD7, 0xB1, 0xD7, 0xA1];
static S_1_19: [symbol; 8] = [0xD7, 0x90, 0xD7, 0xA8, 0xD7, 0xB1, 0xD7, 0xA4];
static S_1_20: [symbol; 8] = [0xD7, 0x90, 0xD7, 0xA8, 0xD7, 0xB2, 0xD7, 0xA0];
static S_1_21: [symbol; 8] = [0xD7, 0x90, 0xD7, 0xB0, 0xD7, 0xA2, 0xD7, 0xA7];
static S_1_22: [symbol; 6] = [0xD7, 0x90, 0xD7, 0xB1, 0xD7, 0xA1];
static S_1_23: [symbol; 6] = [0xD7, 0x90, 0xD7, 0xB1, 0xD7, 0xA4];
static S_1_24: [symbol; 6] = [0xD7, 0x90, 0xD7, 0xB2, 0xD7, 0xA0];
static S_1_25: [symbol; 4] = [0xD7, 0x91, 0xD7, 0x90];
static S_1_26: [symbol; 4] = [0xD7, 0x91, 0xD7, 0xB2];
static S_1_27: [symbol; 8] = [0xD7, 0x93, 0xD7, 0x95, 0xD7, 0xA8, 0xD7, 0x9B];
static S_1_28: [symbol; 6] = [0xD7, 0x93, 0xD7, 0xA2, 0xD7, 0xA8];
static S_1_29: [symbol; 6] = [0xD7, 0x9E, 0xD7, 0x99, 0xD7, 0x98];
static S_1_30: [symbol; 6] = [0xD7, 0xA0, 0xD7, 0x90, 0xD7, 0x9B];
static S_1_31: [symbol; 6] = [0xD7, 0xA4, 0xD7, 0x90, 0xD7, 0xA8];
static S_1_32: [symbol; 10] = [0xD7, 0xA4, 0xD7, 0x90, 0xD7, 0xA8, 0xD7, 0x91, 0xD7, 0xB2];
static S_1_33: [symbol; 10] = [0xD7, 0xA4, 0xD7, 0x90, 0xD7, 0xA8, 0xD7, 0xB1, 0xD7, 0xA1];
static S_1_34: [symbol; 16] = [
    0xD7, 0xA4, 0xD7, 0x95, 0xD7, 0xA0, 0xD7, 0x90, 0xD7, 0xA0, 0xD7, 0x93, 0xD7, 0xA2, 0xD7,
    0xA8,
];
static S_1_35: [symbol; 4] = [0xD7, 0xA6, 0xD7, 0x95];
static S_1_36: [symbol; 14] = [
    0xD7, 0xA6, 0xD7, 0x95, 0xD7, 0x96, 0xD7, 0x90, 0xD7, 0x9E, 0xD7, 0xA2, 0xD7, 0xA0,
];
static S_1_37: [symbol; 10] = [0xD7, 0xA6, 0xD7, 0x95, 0xD7, 0xA0, 0xD7, 0xB1, 0xD7, 0xA4];
static S_1_38: [symbol; 10] = [0xD7, 0xA6, 0xD7, 0x95, 0xD7, 0xA8, 0xD7, 0x99, 0xD7, 0xA7];
static S_1_39: [symbol; 4] = [0xD7, 0xA6, 0xD7, 0xA2];

static A_1: [among; 40] = [
    among { s_size: 10, s: S_1_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_1_1.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_1_2.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_1_3.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_1_4.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 12, s: S_1_5.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 10, s: S_1_6.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_1_7.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_1_8.as_ptr(), substring_i: 7, result: 1, function: None },
    among { s_size: 14, s: S_1_9.as_ptr(), substring_i: 8, result: 1, function: None },
    among { s_size: 12, s: S_1_10.as_ptr(), substring_i: 7, result: 1, function: None },
    among { s_size: 4, s: S_1_11.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_1_12.as_ptr(), substring_i: 11, result: 1, function: None },
    among { s_size: 10, s: S_1_13.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_1_14.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_1_15.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 14, s: S_1_16.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 12, s: S_1_17.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_1_18.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_1_19.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_1_20.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_1_21.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_1_22.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_1_23.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_1_24.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_1_25.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_1_26.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_1_27.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_1_28.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_1_29.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_1_30.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_1_31.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 10, s: S_1_32.as_ptr(), substring_i: 31, result: 1, function: None },
    among { s_size: 10, s: S_1_33.as_ptr(), substring_i: 31, result: 1, function: None },
    among { s_size: 16, s: S_1_34.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_1_35.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 14, s: S_1_36.as_ptr(), substring_i: 35, result: 1, function: None },
    among { s_size: 10, s: S_1_37.as_ptr(), substring_i: 35, result: 1, function: None },
    among { s_size: 10, s: S_1_38.as_ptr(), substring_i: 35, result: 1, function: None },
    among { s_size: 4, s: S_1_39.as_ptr(), substring_i: -1, result: 1, function: None },
];

static S_2_0: [symbol; 6] = [0xD7, 0x93, 0xD7, 0x96, 0xD7, 0xA9];
static S_2_1: [symbol; 6] = [0xD7, 0xA9, 0xD7, 0x98, 0xD7, 0xA8];
static S_2_2: [symbol; 6] = [0xD7, 0xA9, 0xD7, 0x98, 0xD7, 0xA9];
static S_2_3: [symbol; 6] = [0xD7, 0xA9, 0xD7, 0xA4, 0xD7, 0xA8];

static A_2: [among; 4] = [
    among { s_size: 6, s: S_2_0.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 6, s: S_2_1.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 6, s: S_2_2.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 6, s: S_2_3.as_ptr(), substring_i: -1, result: -1, function: None },
];

static S_3_0: [symbol; 8] = [0xD7, 0xA7, 0xD7, 0x9C, 0xD7, 0x99, 0xD7, 0x91];
static S_3_1: [symbol; 6] = [0xD7, 0xA8, 0xD7, 0x99, 0xD7, 0x91];
static S_3_2: [symbol; 8] = [0xD7, 0x98, 0xD7, 0xA8, 0xD7, 0x99, 0xD7, 0x91];
static S_3_3: [symbol; 8] = [0xD7, 0xA9, 0xD7, 0xA8, 0xD7, 0x99, 0xD7, 0x91];
static S_3_4: [symbol; 6] = [0xD7, 0x94, 0xD7, 0xB1, 0xD7, 0x91];
static S_3_5: [symbol; 8] = [0xD7, 0xA9, 0xD7, 0xB0, 0xD7, 0x99, 0xD7, 0x92];
static S_3_6: [symbol; 8] = [0xD7, 0x92, 0xD7, 0x90, 0xD7, 0xA0, 0xD7, 0x92];
static S_3_7: [symbol; 8] = [0xD7, 0x96, 0xD7, 0x95, 0xD7, 0xA0, 0xD7, 0x92];
static S_3_8: [symbol; 10] = [0xD7, 0xA9, 0xD7, 0x9C, 0xD7, 0x95, 0xD7, 0xA0, 0xD7, 0x92];
static S_3_9: [symbol; 10] = [0xD7, 0xA6, 0xD7, 0xB0, 0xD7, 0x95, 0xD7, 0xA0, 0xD7, 0x92];
static S_3_10: [symbol; 6] = [0xD7, 0x91, 0xD7, 0xB1, 0xD7, 0x92];
static S_3_11: [symbol; 8] = [0xD7, 0x91, 0xD7, 0x95, 0xD7, 0xA0, 0xD7, 0x93];
static S_3_12: [symbol; 6] = [0xD7, 0xB0, 0xD7, 0x99, 0xD7, 0x96];
static S_3_13: [symbol; 6] = [0xD7, 0x91, 0xD7, 0x99, 0xD7, 0x98];
static S_3_14: [symbol; 6] = [0xD7, 0x9C, 0xD7, 0x99, 0xD7, 0x98];
static S_3_15: [symbol; 6] = [0xD7, 0x9E, 0xD7, 0x99, 0xD7, 0x98];
static S_3_16: [symbol; 8] = [0xD7, 0xA9, 0xD7, 0xA0, 0xD7, 0x99, 0xD7, 0x98];
static S_3_17: [symbol; 6] = [0xD7, 0xA0, 0xD7, 0x95, 0xD7, 0x9E];
static S_3_18: [symbol; 8] = [0xD7, 0xA9, 0xD7, 0x98, 0xD7, 0x90, 0xD7, 0xA0];
static S_3_19: [symbol; 6] = [0xD7, 0x91, 0xD7, 0x99, 0xD7, 0xA1];
static S_3_20: [symbol; 8] = [0xD7, 0xA9, 0xD7, 0x9E, 0xD7, 0x99, 0xD7, 0xA1];
static S_3_21: [symbol; 6] = [0xD7, 0xA8, 0xD7, 0x99, 0xD7, 0xA1];
static S_3_22: [symbol; 10] = [0xD7, 0x98, 0xD7, 0xA8, 0xD7, 0x95, 0xD7, 0xA0, 0xD7, 0xA7];
static S_3_23: [symbol; 12] = [
    0xD7, 0xA4, 0xD7, 0x90, 0xD7, 0xA8, 0xD7, 0x9C, 0xD7, 0xB1, 0xD7, 0xA8,
];
static S_3_24: [symbol; 8] = [0xD7, 0xA9, 0xD7, 0xB0, 0xD7, 0xB1, 0xD7, 0xA8];
static S_3_25: [symbol; 8] = [0xD7, 0xB0, 0xD7, 0x95, 0xD7, 0x98, 0xD7, 0xA9];

static A_3: [among; 26] = [
    among { s_size: 8, s: S_3_0.as_ptr(), substring_i: -1, result: 9, function: None },
    among { s_size: 6, s: S_3_1.as_ptr(), substring_i: -1, result: 10, function: None },
    among { s_size: 8, s: S_3_2.as_ptr(), substring_i: 1, result: 7, function: None },
    among { s_size: 8, s: S_3_3.as_ptr(), substring_i: 1, result: 15, function: None },
    among { s_size: 6, s: S_3_4.as_ptr(), substring_i: -1, result: 23, function: None },
    among { s_size: 8, s: S_3_5.as_ptr(), substring_i: -1, result: 12, function: None },
    among { s_size: 8, s: S_3_6.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_3_7.as_ptr(), substring_i: -1, result: 18, function: None },
    among { s_size: 10, s: S_3_8.as_ptr(), substring_i: -1, result: 21, function: None },
    among { s_size: 10, s: S_3_9.as_ptr(), substring_i: -1, result: 20, function: None },
    among { s_size: 6, s: S_3_10.as_ptr(), substring_i: -1, result: 22, function: None },
    among { s_size: 8, s: S_3_11.as_ptr(), substring_i: -1, result: 16, function: None },
    among { s_size: 6, s: S_3_12.as_ptr(), substring_i: -1, result: 6, function: None },
    among { s_size: 6, s: S_3_13.as_ptr(), substring_i: -1, result: 4, function: None },
    among { s_size: 6, s: S_3_14.as_ptr(), substring_i: -1, result: 8, function: None },
    among { s_size: 6, s: S_3_15.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 8, s: S_3_16.as_ptr(), substring_i: -1, result: 14, function: None },
    among { s_size: 6, s: S_3_17.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 8, s: S_3_18.as_ptr(), substring_i: -1, result: 25, function: None },
    among { s_size: 6, s: S_3_19.as_ptr(), substring_i: -1, result: 5, function: None },
    among { s_size: 8, s: S_3_20.as_ptr(), substring_i: -1, result: 13, function: None },
    among { s_size: 6, s: S_3_21.as_ptr(), substring_i: -1, result: 11, function: None },
    among { s_size: 10, s: S_3_22.as_ptr(), substring_i: -1, result: 19, function: None },
    among { s_size: 12, s: S_3_23.as_ptr(), substring_i: -1, result: 24, function: None },
    among { s_size: 8, s: S_3_24.as_ptr(), substring_i: -1, result: 26, function: None },
    among { s_size: 8, s: S_3_25.as_ptr(), substring_i: -1, result: 17, function: None },
];

static S_4_0: [symbol; 6] = [0xD7, 0x95, 0xD7, 0xA0, 0xD7, 0x92];
static S_4_1: [symbol; 6] = [0xD7, 0xA1, 0xD7, 0x98, 0xD7, 0x95];
static S_4_2: [symbol; 2] = [0xD7, 0x98];
static S_4_3: [symbol; 10] = [0xD7, 0x91, 0xD7, 0xA8, 0xD7, 0x90, 0xD7, 0x9B, 0xD7, 0x98];
static S_4_4: [symbol; 4] = [0xD7, 0xA1, 0xD7, 0x98];
static S_4_5: [symbol; 6] = [0xD7, 0x99, 0xD7, 0xA1, 0xD7, 0x98];
static S_4_6: [symbol; 4] = [0xD7, 0xA2, 0xD7, 0x98];
static S_4_7: [symbol; 8] = [0xD7, 0xA9, 0xD7, 0x90, 0xD7, 0xA4, 0xD7, 0x98];
static S_4_8: [symbol; 6] = [0xD7, 0x94, 0xD7, 0xB2, 0xD7, 0x98];
static S_4_9: [symbol; 6] = [0xD7, 0xA7, 0xD7, 0xB2, 0xD7, 0x98];
static S_4_10: [symbol; 8] = [0xD7, 0x99, 0xD7, 0xA7, 0xD7, 0xB2, 0xD7, 0x98];
static S_4_11: [symbol; 6] = [0xD7, 0x9C, 0xD7, 0xA2, 0xD7, 0x9B];
static S_4_12: [symbol; 8] = [0xD7, 0xA2, 0xD7, 0x9C, 0xD7, 0xA2, 0xD7, 0x9B];
static S_4_13: [symbol; 6] = [0xD7, 0x99, 0xD7, 0x96, 0xD7, 0x9E];
static S_4_14: [symbol; 4] = [0xD7, 0x99, 0xD7, 0x9E];
static S_4_15: [symbol; 4] = [0xD7, 0xA2, 0xD7, 0x9E];
static S_4_16: [symbol; 8] = [0xD7, 0xA2, 0xD7, 0xA0, 0xD7, 0xA2, 0xD7, 0x9E];
static S_4_17: [symbol; 10] = [0xD7, 0x98, 0xD7, 0xA2, 0xD7, 0xA0, 0xD7, 0xA2, 0xD7, 0x9E];
static S_4_18: [symbol; 2] = [0xD7, 0xA0];
static S_4_19: [symbol; 10] = [0xD7, 0xA7, 0xD7, 0x9C, 0xD7, 0x99, 0xD7, 0x91, 0xD7, 0xA0];
static S_4_20: [symbol; 8] = [0xD7, 0xA8, 0xD7, 0x99, 0xD7, 0x91, 0xD7, 0xA0];
static S_4_21: [symbol; 10] = [0xD7, 0x98, 0xD7, 0xA8, 0xD7, 0x99, 0xD7, 0x91, 0xD7, 0xA0];
static S_4_22: [symbol; 10] = [0xD7, 0xA9, 0xD7, 0xA8, 0xD7, 0x99, 0xD7, 0x91, 0xD7, 0xA0];
static S_4_23: [symbol; 8] = [0xD7, 0x94, 0xD7, 0xB1, 0xD7, 0x91, 0xD7, 0xA0];
static S_4_24: [symbol; 10] = [0xD7, 0xA9, 0xD7, 0xB0, 0xD7, 0x99, 0xD7, 0x92, 0xD7, 0xA0];
static S_4_25: [symbol; 10] = [0xD7, 0x96, 0xD7, 0x95, 0xD7, 0xA0, 0xD7, 0x92, 0xD7, 0xA0];
static S_4_26: [symbol; 12] = [
    0xD7, 0xA9, 0xD7, 0x9C, 0xD7, 0x95, 0xD7, 0xA0, 0xD7, 0x92, 0xD7, 0xA0,
];
static S_4_27: [symbol; 12] = [
    0xD7, 0xA6, 0xD7, 0xB0, 0xD7, 0x95, 0xD7, 0xA0, 0xD7, 0x92, 0xD7, 0xA0,
];
static S_4_28: [symbol; 8] = [0xD7, 0x91, 0xD7, 0xB1, 0xD7, 0x92, 0xD7, 0xA0];
static S_4_29: [symbol; 10] = [0xD7, 0x91, 0xD7, 0x95, 0xD7, 0xA0, 0xD7, 0x93, 0xD7, 0xA0];
static S_4_30: [symbol; 8] = [0xD7, 0xB0, 0xD7, 0x99, 0xD7, 0x96, 0xD7, 0xA0];
static S_4_31: [symbol; 4] = [0xD7, 0x98, 0xD7, 0xA0];
static S_4_32: [symbol; 10] = [b'G', b'E', 0xD7, 0x91, 0xD7, 0x99, 0xD7, 0x98, 0xD7, 0xA0];
static S_4_33: [symbol; 10] = [b'G', b'E', 0xD7, 0x9C, 0xD7, 0x99, 0xD7, 0x98, 0xD7, 0xA0];
static S_4_34: [symbol; 10] = [b'G', b'E', 0xD7, 0x9E, 0xD7, 0x99, 0xD7, 0x98, 0xD7, 0xA0];
static S_4_35: [symbol; 10] = [0xD7, 0xA9, 0xD7, 0xA0, 0xD7, 0x99, 0xD7, 0x98, 0xD7, 0xA0];
static S_4_36: [symbol; 6] = [0xD7, 0xA1, 0xD7, 0x98, 0xD7, 0xA0];
static S_4_37: [symbol; 8] = [0xD7, 0x99, 0xD7, 0xA1, 0xD7, 0x98, 0xD7, 0xA0];
static S_4_38: [symbol; 6] = [0xD7, 0xA2, 0xD7, 0x98, 0xD7, 0xA0];
static S_4_39: [symbol; 10] = [b'G', b'E', 0xD7, 0x91, 0xD7, 0x99, 0xD7, 0xA1, 0xD7, 0xA0];
static S_4_40: [symbol; 10] = [0xD7, 0xA9, 0xD7, 0x9E, 0xD7, 0x99, 0xD7, 0xA1, 0xD7, 0xA0];
static S_4_41: [symbol; 10] = [b'G', b'E', 0xD7, 0xA8, 0xD7, 0x99, 0xD7, 0xA1, 0xD7, 0xA0];
static S_4_42: [symbol; 4] = [0xD7, 0xA2, 0xD7, 0xA0];
static S_4_43: [symbol; 12] = [
    0xD7, 0x92, 0xD7, 0x90, 0xD7, 0xA0, 0xD7, 0x92, 0xD7, 0xA2, 0xD7, 0xA0,
];
static S_4_44: [symbol; 8] = [0xD7, 0xA2, 0xD7, 0x9C, 0xD7, 0xA2, 0xD7, 0xA0];
static S_4_45: [symbol; 10] = [0xD7, 0xA0, 0xD7, 0x95, 0xD7, 0x9E, 0xD7, 0xA2, 0xD7, 0xA0];
static S_4_46: [symbol; 10] = [0xD7, 0x99, 0xD7, 0x96, 0xD7, 0x9E, 0xD7, 0xA2, 0xD7, 0xA0];
static S_4_47: [symbol; 12] = [
    0xD7, 0xA9, 0xD7, 0x98, 0xD7, 0x90, 0xD7, 0xA0, 0xD7, 0xA2, 0xD7, 0xA0,
];
static S_4_48: [symbol; 12] = [
    0xD7, 0x98, 0xD7, 0xA8, 0xD7, 0x95, 0xD7, 0xA0, 0xD7, 0xA7, 0xD7, 0xA0,
];
static S_4_49: [symbol; 14] = [
    0xD7, 0xA4, 0xD7, 0x90, 0xD7, 0xA8, 0xD7, 0x9C, 0xD7, 0xB1, 0xD7, 0xA8, 0xD7, 0xA0,
];
static S_4_50: [symbol; 10] = [0xD7, 0xA9, 0xD7, 0xB0, 0xD7, 0xB1, 0xD7, 0xA8, 0xD7, 0xA0];
static S_4_51: [symbol; 10] = [0xD7, 0xB0, 0xD7, 0x95, 0xD7, 0x98, 0xD7, 0xA9, 0xD7, 0xA0];
static S_4_52: [symbol; 6] = [0xD7, 0x92, 0xD7, 0xB2, 0xD7, 0xA0];
static S_4_53: [symbol; 2] = [0xD7, 0xA1];
static S_4_54: [symbol; 4] = [0xD7, 0x98, 0xD7, 0xA1];
static S_4_55: [symbol; 6] = [0xD7, 0xA2, 0xD7, 0x98, 0xD7, 0xA1];
static S_4_56: [symbol; 4] = [0xD7, 0xA0, 0xD7, 0xA1];
static S_4_57: [symbol; 6] = [0xD7, 0x98, 0xD7, 0xA0, 0xD7, 0xA1];
static S_4_58: [symbol; 6] = [0xD7, 0xA2, 0xD7, 0xA0, 0xD7, 0xA1];
static S_4_59: [symbol; 4] = [0xD7, 0xA2, 0xD7, 0xA1];
static S_4_60: [symbol; 6] = [0xD7, 0x99, 0xD7, 0xA2, 0xD7, 0xA1];
static S_4_61: [symbol; 8] = [0xD7, 0xA2, 0xD7, 0x9C, 0xD7, 0xA2, 0xD7, 0xA1];
static S_4_62: [symbol; 6] = [0xD7, 0xA2, 0xD7, 0xA8, 0xD7, 0xA1];
static S_4_63: [symbol; 10] = [0xD7, 0xA2, 0xD7, 0xA0, 0xD7, 0xA2, 0xD7, 0xA8, 0xD7, 0xA1];
static S_4_64: [symbol; 2] = [0xD7, 0xA2];
static S_4_65: [symbol; 4] = [0xD7, 0x98, 0xD7, 0xA2];
static S_4_66: [symbol; 6] = [0xD7, 0xA1, 0xD7, 0x98, 0xD7, 0xA2];
static S_4_67: [symbol; 6] = [0xD7, 0xA2, 0xD7, 0x98, 0xD7, 0xA2];
static S_4_68: [symbol; 4] = [0xD7, 0x99, 0xD7, 0xA2];
static S_4_69: [symbol; 6] = [0xD7, 0xA2, 0xD7, 0x9C, 0xD7, 0xA2];
static S_4_70: [symbol; 6] = [0xD7, 0xA2, 0xD7, 0xA0, 0xD7, 0xA2];
static S_4_71: [symbol; 8] = [0xD7, 0x98, 0xD7, 0xA2, 0xD7, 0xA0, 0xD7, 0xA2];
static S_4_72: [symbol; 4] = [0xD7, 0xA2, 0xD7, 0xA8];
static S_4_73: [symbol; 6] = [0xD7, 0x98, 0xD7, 0xA2, 0xD7, 0xA8];
static S_4_74: [symbol; 8] = [0xD7, 0xA1, 0xD7, 0x98, 0xD7, 0xA2, 0xD7, 0xA8];
static S_4_75: [symbol; 8] = [0xD7, 0xA2, 0xD7, 0x98, 0xD7, 0xA2, 0xD7, 0xA8];
static S_4_76: [symbol; 8] = [0xD7, 0xA2, 0xD7, 0xA0, 0xD7, 0xA2, 0xD7, 0xA8];
static S_4_77: [symbol; 10] = [0xD7, 0x98, 0xD7, 0xA2, 0xD7, 0xA0, 0xD7, 0xA2, 0xD7, 0xA8];
static S_4_78: [symbol; 4] = [0xD7, 0x95, 0xD7, 0xAA];

static A_4: [among; 79] = [
    among { s_size: 6, s: S_4_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_4_1.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_4_2.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 10, s: S_4_3.as_ptr(), substring_i: 2, result: 31, function: None },
    among { s_size: 4, s: S_4_4.as_ptr(), substring_i: 2, result: 1, function: None },
    among { s_size: 6, s: S_4_5.as_ptr(), substring_i: 4, result: 33, function: None },
    among { s_size: 4, s: S_4_6.as_ptr(), substring_i: 2, result: 1, function: None },
    among { s_size: 8, s: S_4_7.as_ptr(), substring_i: 2, result: 1, function: None },
    among { s_size: 6, s: S_4_8.as_ptr(), substring_i: 2, result: 1, function: None },
    among { s_size: 6, s: S_4_9.as_ptr(), substring_i: 2, result: 1, function: None },
    among { s_size: 8, s: S_4_10.as_ptr(), substring_i: 9, result: 1, function: None },
    among { s_size: 6, s: S_4_11.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_4_12.as_ptr(), substring_i: 11, result: 1, function: None },
    among { s_size: 6, s: S_4_13.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_4_14.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_4_15.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_4_16.as_ptr(), substring_i: 15, result: 3, function: None },
    among { s_size: 10, s: S_4_17.as_ptr(), substring_i: 16, result: 4, function: None },
    among { s_size: 2, s: S_4_18.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 10, s: S_4_19.as_ptr(), substring_i: 18, result: 14, function: None },
    among { s_size: 8, s: S_4_20.as_ptr(), substring_i: 18, result: 15, function: None },
    among { s_size: 10, s: S_4_21.as_ptr(), substring_i: 20, result: 12, function: None },
    among { s_size: 10, s: S_4_22.as_ptr(), substring_i: 20, result: 7, function: None },
    among { s_size: 8, s: S_4_23.as_ptr(), substring_i: 18, result: 27, function: None },
    among { s_size: 10, s: S_4_24.as_ptr(), substring_i: 18, result: 17, function: None },
    among { s_size: 10, s: S_4_25.as_ptr(), substring_i: 18, result: 22, function: None },
    among { s_size: 12, s: S_4_26.as_ptr(), substring_i: 18, result: 25, function: None },
    among { s_size: 12, s: S_4_27.as_ptr(), substring_i: 18, result: 24, function: None },
    among { s_size: 8, s: S_4_28.as_ptr(), substring_i: 18, result: 26, function: None },
    among { s_size: 10, s: S_4_29.as_ptr(), substring_i: 18, result: 20, function: None },
    among { s_size: 8, s: S_4_30.as_ptr(), substring_i: 18, result: 11, function: None },
    among { s_size: 4, s: S_4_31.as_ptr(), substring_i: 18, result: 4, function: None },
    among { s_size: 10, s: S_4_32.as_ptr(), substring_i: 31, result: 9, function: None },
    among { s_size: 10, s: S_4_33.as_ptr(), substring_i: 31, result: 13, function: None },
    among { s_size: 10, s: S_4_34.as_ptr(), substring_i: 31, result: 8, function: None },
    among { s_size: 10, s: S_4_35.as_ptr(), substring_i: 31, result: 19, function: None },
    among { s_size: 6, s: S_4_36.as_ptr(), substring_i: 31, result: 1, function: None },
    among { s_size: 8, s: S_4_37.as_ptr(), substring_i: 36, result: 1, function: None },
    among { s_size: 6, s: S_4_38.as_ptr(), substring_i: 31, result: 1, function: None },
    among { s_size: 10, s: S_4_39.as_ptr(), substring_i: 18, result: 10, function: None },
    among { s_size: 10, s: S_4_40.as_ptr(), substring_i: 18, result: 18, function: None },
    among { s_size: 10, s: S_4_41.as_ptr(), substring_i: 18, result: 16, function: None },
    among { s_size: 4, s: S_4_42.as_ptr(), substring_i: 18, result: 1, function: None },
    among { s_size: 12, s: S_4_43.as_ptr(), substring_i: 42, result: 5, function: None },
    among { s_size: 8, s: S_4_44.as_ptr(), substring_i: 42, result: 1, function: None },
    among { s_size: 10, s: S_4_45.as_ptr(), substring_i: 42, result: 6, function: None },
    among { s_size: 10, s: S_4_46.as_ptr(), substring_i: 42, result: 1, function: None },
    among { s_size: 12, s: S_4_47.as_ptr(), substring_i: 42, result: 29, function: None },
    among { s_size: 12, s: S_4_48.as_ptr(), substring_i: 18, result: 23, function: None },
    among { s_size: 14, s: S_4_49.as_ptr(), substring_i: 18, result: 28, function: None },
    among { s_size: 10, s: S_4_50.as_ptr(), substring_i: 18, result: 30, function: None },
    among { s_size: 10, s: S_4_51.as_ptr(), substring_i: 18, result: 21, function: None },
    among { s_size: 6, s: S_4_52.as_ptr(), substring_i: 18, result: 5, function: None },
    among { s_size: 2, s: S_4_53.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_4_54.as_ptr(), substring_i: 53, result: 4, function: None },
    among { s_size: 6, s: S_4_55.as_ptr(), substring_i: 54, result: 1, function: None },
    among { s_size: 4, s: S_4_56.as_ptr(), substring_i: 53, result: 1, function: None },
    among { s_size: 6, s: S_4_57.as_ptr(), substring_i: 56, result: 4, function: None },
    among { s_size: 6, s: S_4_58.as_ptr(), substring_i: 56, result: 3, function: None },
    among { s_size: 4, s: S_4_59.as_ptr(), substring_i: 53, result: 1, function: None },
    among { s_size: 6, s: S_4_60.as_ptr(), substring_i: 59, result: 2, function: None },
    among { s_size: 8, s: S_4_61.as_ptr(), substring_i: 59, result: 1, function: None },
    among { s_size: 6, s: S_4_62.as_ptr(), substring_i: 53, result: 1, function: None },
    among { s_size: 10, s: S_4_63.as_ptr(), substring_i: 62, result: 1, function: None },
    among { s_size: 2, s: S_4_64.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_4_65.as_ptr(), substring_i: 64, result: 4, function: None },
    among { s_size: 6, s: S_4_66.as_ptr(), substring_i: 65, result: 1, function: None },
    among { s_size: 6, s: S_4_67.as_ptr(), substring_i: 65, result: 1, function: None },
    among { s_size: 4, s: S_4_68.as_ptr(), substring_i: 64, result: -1, function: None },
    among { s_size: 6, s: S_4_69.as_ptr(), substring_i: 64, result: 1, function: None },
    among { s_size: 6, s: S_4_70.as_ptr(), substring_i: 64, result: 3, function: None },
    among { s_size: 8, s: S_4_71.as_ptr(), substring_i: 70, result: 4, function: None },
    among { s_size: 4, s: S_4_72.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_4_73.as_ptr(), substring_i: 72, result: 4, function: None },
    among { s_size: 8, s: S_4_74.as_ptr(), substring_i: 73, result: 1, function: None },
    among { s_size: 8, s: S_4_75.as_ptr(), substring_i: 73, result: 1, function: None },
    among { s_size: 8, s: S_4_76.as_ptr(), substring_i: 72, result: 3, function: None },
    among { s_size: 10, s: S_4_77.as_ptr(), substring_i: 76, result: 4, function: None },
    among { s_size: 4, s: S_4_78.as_ptr(), substring_i: -1, result: 32, function: None },
];

static S_5_0: [symbol; 6] = [0xD7, 0x95, 0xD7, 0xA0, 0xD7, 0x92];
static S_5_1: [symbol; 8] = [0xD7, 0xA9, 0xD7, 0x90, 0xD7, 0xA4, 0xD7, 0x98];
static S_5_2: [symbol; 6] = [0xD7, 0x94, 0xD7, 0xB2, 0xD7, 0x98];
static S_5_3: [symbol; 6] = [0xD7, 0xA7, 0xD7, 0xB2, 0xD7, 0x98];
static S_5_4: [symbol; 8] = [0xD7, 0x99, 0xD7, 0xA7, 0xD7, 0xB2, 0xD7, 0x98];
static S_5_5: [symbol; 2] = [0xD7, 0x9C];

static A_5: [among; 6] = [
    among { s_size: 6, s: S_5_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_5_1.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_5_2.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_5_3.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_5_4.as_ptr(), substring_i: 3, result: 1, function: None },
    among { s_size: 2, s: S_5_5.as_ptr(), substring_i: -1, result: 2, function: None },
];

static S_6_0: [symbol; 4] = [0xD7, 0x99, 0xD7, 0x92];
static S_6_1: [symbol; 4] = [0xD7, 0x99, 0xD7, 0xA7];
static S_6_2: [symbol; 6] = [0xD7, 0x93, 0xD7, 0x99, 0xD7, 0xA7];
static S_6_3: [symbol; 8] = [0xD7, 0xA0, 0xD7, 0x93, 0xD7, 0x99, 0xD7, 0xA7];
static S_6_4: [symbol; 10] = [0xD7, 0xA2, 0xD7, 0xA0, 0xD7, 0x93, 0xD7, 0x99, 0xD7, 0xA7];
static S_6_5: [symbol; 8] = [0xD7, 0x91, 0xD7, 0x9C, 0xD7, 0x99, 0xD7, 0xA7];
static S_6_6: [symbol; 8] = [0xD7, 0x92, 0xD7, 0x9C, 0xD7, 0x99, 0xD7, 0xA7];
static S_6_7: [symbol; 6] = [0xD7, 0xA0, 0xD7, 0x99, 0xD7, 0xA7];
static S_6_8: [symbol; 4] = [0xD7, 0x99, 0xD7, 0xA9];

static A_6: [among; 9] = [
    among { s_size: 4, s: S_6_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_6_1.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_6_2.as_ptr(), substring_i: 1, result: 1, function: None },
    among { s_size: 8, s: S_6_3.as_ptr(), substring_i: 2, result: 1, function: None },
    among { s_size: 10, s: S_6_4.as_ptr(), substring_i: 3, result: 1, function: None },
    among { s_size: 8, s: S_6_5.as_ptr(), substring_i: 1, result: -1, function: None },
    among { s_size: 8, s: S_6_6.as_ptr(), substring_i: 1, result: -1, function: None },
    among { s_size: 6, s: S_6_7.as_ptr(), substring_i: 1, result: 1, function: None },
    among { s_size: 4, s: S_6_8.as_ptr(), substring_i: -1, result: 1, function: None },
];

// ---------------------------------------------------------------------------
// grouping bit tables
// ---------------------------------------------------------------------------

static G_NIKED: [c_uchar; 3] = [255, 155, 6];

static G_VOWEL: [c_uchar; 5] = [33, 2, 4, 0, 6];

static G_CONSONANT: [c_uchar; 4] = [239, 254, 253, 131];

// ---------------------------------------------------------------------------
// literal strings used by slice_from_s / eq_s / eq_s_b
// ---------------------------------------------------------------------------

static S_0: [symbol; 2] = [0xD6, 0xBC];
static S_1: [symbol; 2] = [0xD7, 0xB0];
static S_2: [symbol; 2] = [0xD6, 0xB4];
static S_3: [symbol; 2] = [0xD7, 0xB1];
static S_4: [symbol; 2] = [0xD6, 0xB4];
static S_5: [symbol; 2] = [0xD7, 0xB2];
static S_6: [symbol; 2] = [0xD7, 0x9B];
static S_7: [symbol; 2] = [0xD7, 0x9E];
static S_8: [symbol; 2] = [0xD7, 0xA0];
static S_9: [symbol; 2] = [0xD7, 0xA4];
static S_10: [symbol; 2] = [0xD7, 0xA6];
static S_11: [symbol; 4] = [0xD7, 0x92, 0xD7, 0xA2];
static S_12: [symbol; 4] = [0xD7, 0x9C, 0xD7, 0x98];
static S_13: [symbol; 4] = [0xD7, 0x91, 0xD7, 0xA0];
static S_14: [symbol; 2] = [b'G', b'E'];
static S_15: [symbol; 8] = [0xD7, 0xA6, 0xD7, 0x95, 0xD7, 0x92, 0xD7, 0xA0];
static S_16: [symbol; 8] = [0xD7, 0xA6, 0xD7, 0x95, 0xD7, 0xA7, 0xD7, 0x98];
static S_17: [symbol; 8] = [0xD7, 0xA6, 0xD7, 0x95, 0xD7, 0xA7, 0xD7, 0xA0];
static S_18: [symbol; 8] = [0xD7, 0x92, 0xD7, 0xA2, 0xD7, 0x91, 0xD7, 0xA0];
static S_19: [symbol; 4] = [0xD7, 0x92, 0xD7, 0xA2];
static S_20: [symbol; 2] = [b'G', b'E'];
static S_21: [symbol; 4] = [0xD7, 0xA6, 0xD7, 0x95];
static S_22: [symbol; 3] = [b'T', b'S', b'U'];
static S_23: [symbol; 4] = [0xD7, 0x99, 0xD7, 0xA2];
static S_24: [symbol; 4] = [0xD7, 0x92, 0xD7, 0xB2];
static S_25: [symbol; 6] = [0xD7, 0xA0, 0xD7, 0xA2, 0xD7, 0x9E];
static S_26: [symbol; 6] = [0xD7, 0x9E, 0xD7, 0xB2, 0xD7, 0x93];
static S_27: [symbol; 6] = [0xD7, 0x91, 0xD7, 0xB2, 0xD7, 0x98];
static S_28: [symbol; 6] = [0xD7, 0x91, 0xD7, 0xB2, 0xD7, 0xA1];
static S_29: [symbol; 6] = [0xD7, 0xB0, 0xD7, 0xB2, 0xD7, 0x96];
static S_30: [symbol; 8] = [0xD7, 0x98, 0xD7, 0xA8, 0xD7, 0xB2, 0xD7, 0x91];
static S_31: [symbol; 6] = [0xD7, 0x9C, 0xD7, 0xB2, 0xD7, 0x98];
static S_32: [symbol; 8] = [0xD7, 0xA7, 0xD7, 0x9C, 0xD7, 0xB2, 0xD7, 0x91];
static S_33: [symbol; 6] = [0xD7, 0xA8, 0xD7, 0xB2, 0xD7, 0x91];
static S_34: [symbol; 6] = [0xD7, 0xA8, 0xD7, 0xB2, 0xD7, 0xA1];
static S_35: [symbol; 8] = [0xD7, 0xA9, 0xD7, 0xB0, 0xD7, 0xB2, 0xD7, 0x92];
static S_36: [symbol; 8] = [0xD7, 0xA9, 0xD7, 0x9E, 0xD7, 0xB2, 0xD7, 0xA1];
static S_37: [symbol; 8] = [0xD7, 0xA9, 0xD7, 0xA0, 0xD7, 0xB2, 0xD7, 0x93];
static S_38: [symbol; 8] = [0xD7, 0xA9, 0xD7, 0xA8, 0xD7, 0xB2, 0xD7, 0x91];
static S_39: [symbol; 8] = [0xD7, 0x91, 0xD7, 0x99, 0xD7, 0xA0, 0xD7, 0x93];
static S_40: [symbol; 8] = [0xD7, 0xB0, 0xD7, 0x99, 0xD7, 0x98, 0xD7, 0xA9];
static S_41: [symbol; 8] = [0xD7, 0x96, 0xD7, 0x99, 0xD7, 0xA0, 0xD7, 0x92];
static S_42: [symbol; 10] = [0xD7, 0x98, 0xD7, 0xA8, 0xD7, 0x99, 0xD7, 0xA0, 0xD7, 0xA7];
static S_43: [symbol; 10] = [0xD7, 0xA6, 0xD7, 0xB0, 0xD7, 0x99, 0xD7, 0xA0, 0xD7, 0x92];
static S_44: [symbol; 10] = [0xD7, 0xA9, 0xD7, 0x9C, 0xD7, 0x99, 0xD7, 0xA0, 0xD7, 0x92];
static S_45: [symbol; 6] = [0xD7, 0x91, 0xD7, 0xB2, 0xD7, 0x92];
static S_46: [symbol; 6] = [0xD7, 0x94, 0xD7, 0xB2, 0xD7, 0x91];
static S_47: [symbol; 12] = [
    0xD7, 0xA4, 0xD7, 0x90, 0xD7, 0xA8, 0xD7, 0x9C, 0xD7, 0x99, 0xD7, 0xA8,
];
static S_48: [symbol; 6] = [0xD7, 0xA9, 0xD7, 0x98, 0xD7, 0xB2];
static S_49: [symbol; 8] = [0xD7, 0xA9, 0xD7, 0xB0, 0xD7, 0xA2, 0xD7, 0xA8];
static S_50: [symbol; 2] = [0xD7, 0x98];
static S_51: [symbol; 8] = [0xD7, 0x91, 0xD7, 0xA8, 0xD7, 0x90, 0xD7, 0x9B];
static S_52: [symbol; 4] = [0xD7, 0x92, 0xD7, 0xA2];
static S_53: [symbol; 10] = [0xD7, 0x91, 0xD7, 0xA8, 0xD7, 0xA2, 0xD7, 0xA0, 0xD7, 0x92];
static S_54: [symbol; 4] = [0xD7, 0x92, 0xD7, 0xB2];
static S_55: [symbol; 6] = [0xD7, 0xA0, 0xD7, 0xA2, 0xD7, 0x9E];
static S_56: [symbol; 8] = [0xD7, 0xA9, 0xD7, 0xA8, 0xD7, 0xB2, 0xD7, 0x91];
static S_57: [symbol; 6] = [0xD7, 0x9E, 0xD7, 0xB2, 0xD7, 0x93];
static S_58: [symbol; 6] = [0xD7, 0x91, 0xD7, 0xB2, 0xD7, 0x98];
static S_59: [symbol; 6] = [0xD7, 0x91, 0xD7, 0xB2, 0xD7, 0xA1];
static S_60: [symbol; 6] = [0xD7, 0xB0, 0xD7, 0xB2, 0xD7, 0x96];
static S_61: [symbol; 8] = [0xD7, 0x98, 0xD7, 0xA8, 0xD7, 0xB2, 0xD7, 0x91];
static S_62: [symbol; 6] = [0xD7, 0x9C, 0xD7, 0xB2, 0xD7, 0x98];
static S_63: [symbol; 8] = [0xD7, 0xA7, 0xD7, 0x9C, 0xD7, 0xB2, 0xD7, 0x91];
static S_64: [symbol; 6] = [0xD7, 0xA8, 0xD7, 0xB2, 0xD7, 0x91];
static S_65: [symbol; 6] = [0xD7, 0xA8, 0xD7, 0xB2, 0xD7, 0xA1];
static S_66: [symbol; 8] = [0xD7, 0xA9, 0xD7, 0xB0, 0xD7, 0xB2, 0xD7, 0x92];
static S_67: [symbol; 8] = [0xD7, 0xA9, 0xD7, 0x9E, 0xD7, 0xB2, 0xD7, 0xA1];
static S_68: [symbol; 8] = [0xD7, 0xA9, 0xD7, 0xA0, 0xD7, 0xB2, 0xD7, 0x93];
static S_69: [symbol; 8] = [0xD7, 0x91, 0xD7, 0x99, 0xD7, 0xA0, 0xD7, 0x93];
static S_70: [symbol; 8] = [0xD7, 0xB0, 0xD7, 0x99, 0xD7, 0x98, 0xD7, 0xA9];
static S_71: [symbol; 8] = [0xD7, 0x96, 0xD7, 0x99, 0xD7, 0xA0, 0xD7, 0x92];
static S_72: [symbol; 10] = [0xD7, 0x98, 0xD7, 0xA8, 0xD7, 0x99, 0xD7, 0xA0, 0xD7, 0xA7];
static S_73: [symbol; 10] = [0xD7, 0xA6, 0xD7, 0xB0, 0xD7, 0x99, 0xD7, 0xA0, 0xD7, 0x92];
static S_74: [symbol; 10] = [0xD7, 0xA9, 0xD7, 0x9C, 0xD7, 0x99, 0xD7, 0xA0, 0xD7, 0x92];
static S_75: [symbol; 6] = [0xD7, 0x91, 0xD7, 0xB2, 0xD7, 0x92];
static S_76: [symbol; 6] = [0xD7, 0x94, 0xD7, 0xB2, 0xD7, 0x91];
static S_77: [symbol; 12] = [
    0xD7, 0xA4, 0xD7, 0x90, 0xD7, 0xA8, 0xD7, 0x9C, 0xD7, 0x99, 0xD7, 0xA8,
];
static S_78: [symbol; 6] = [0xD7, 0xA9, 0xD7, 0x98, 0xD7, 0xB2];
static S_79: [symbol; 8] = [0xD7, 0xA9, 0xD7, 0xB0, 0xD7, 0xA2, 0xD7, 0xA8];
static S_80: [symbol; 10] = [0xD7, 0x91, 0xD7, 0xA8, 0xD7, 0xA2, 0xD7, 0xA0, 0xD7, 0x92];
static S_81: [symbol; 2] = [0xD7, 0x94];
static S_82: [symbol; 2] = [0xD7, 0x92];
static S_83: [symbol; 2] = [0xD7, 0xA9];
static S_84: [symbol; 4] = [0xD7, 0x99, 0xD7, 0xA1];
static S_85: [symbol; 2] = [b'G', b'E'];
static S_86: [symbol; 3] = [b'T', b'S', b'U'];

// ---------------------------------------------------------------------------
// rules
// ---------------------------------------------------------------------------

unsafe fn r_prelude(z: *mut SN_env) -> c_int {
    {
        let c1 = (*z).c;
        'outer1: loop {
            let c2 = (*z).c;
            'lab1: {
                'zyid7: loop {
                    let c3 = (*z).c;
                    'lab2: {
                        (*z).bra = (*z).c;
                        let av = find_among(z, A_0.as_ptr(), 8);
                        if av == 0 {
                            break 'lab2;
                        }
                        (*z).ket = (*z).c;
                        match av {
                            1 => {
                                {
                                    let c4 = (*z).c;
                                    'lab3: {
                                        if eq_s(z, 2, S_0.as_ptr()) == 0 {
                                            break 'lab3;
                                        }
                                        break 'lab2;
                                    }
                                    (*z).c = c4;
                                }
                                {
                                    let ret = slice_from_s(z, 2, S_1.as_ptr());
                                    if ret < 0 {
                                        return ret;
                                    }
                                }
                            }
                            2 => {
                                {
                                    let c5 = (*z).c;
                                    'lab4: {
                                        if eq_s(z, 2, S_2.as_ptr()) == 0 {
                                            break 'lab4;
                                        }
                                        break 'lab2;
                                    }
                                    (*z).c = c5;
                                }
                                {
                                    let ret = slice_from_s(z, 2, S_3.as_ptr());
                                    if ret < 0 {
                                        return ret;
                                    }
                                }
                            }
                            3 => {
                                {
                                    let c6 = (*z).c;
                                    'lab5: {
                                        if eq_s(z, 2, S_4.as_ptr()) == 0 {
                                            break 'lab5;
                                        }
                                        break 'lab2;
                                    }
                                    (*z).c = c6;
                                }
                                {
                                    let ret = slice_from_s(z, 2, S_5.as_ptr());
                                    if ret < 0 {
                                        return ret;
                                    }
                                }
                            }
                            4 => {
                                let ret = slice_from_s(z, 2, S_6.as_ptr());
                                if ret < 0 {
                                    return ret;
                                }
                            }
                            5 => {
                                let ret = slice_from_s(z, 2, S_7.as_ptr());
                                if ret < 0 {
                                    return ret;
                                }
                            }
                            6 => {
                                let ret = slice_from_s(z, 2, S_8.as_ptr());
                                if ret < 0 {
                                    return ret;
                                }
                            }
                            7 => {
                                let ret = slice_from_s(z, 2, S_9.as_ptr());
                                if ret < 0 {
                                    return ret;
                                }
                            }
                            8 => {
                                let ret = slice_from_s(z, 2, S_10.as_ptr());
                                if ret < 0 {
                                    return ret;
                                }
                            }
                            _ => {}
                        }
                        (*z).c = c3;
                        break 'zyid7;
                    }
                    (*z).c = c3;
                    {
                        let ret = skip_utf8((*z).p, (*z).c, (*z).l, 1);
                        if ret < 0 {
                            break 'lab1;
                        }
                        (*z).c = ret;
                    }
                }
                continue 'outer1;
            }
            (*z).c = c2;
            break;
        }
        (*z).c = c1;
    }
    {
        let c7 = (*z).c;
        'outer2: loop {
            let c8 = (*z).c;
            'lab7: {
                'zyid8: loop {
                    let c9 = (*z).c;
                    'lab8: {
                        (*z).bra = (*z).c;
                        if in_grouping_U(z, G_NIKED.as_ptr(), 1456, 1474, 0) != 0 {
                            break 'lab8;
                        }
                        (*z).ket = (*z).c;
                        {
                            let ret = slice_del(z);
                            if ret < 0 {
                                return ret;
                            }
                        }
                        (*z).c = c9;
                        break 'zyid8;
                    }
                    (*z).c = c9;
                    {
                        let ret = skip_utf8((*z).p, (*z).c, (*z).l, 1);
                        if ret < 0 {
                            break 'lab7;
                        }
                        (*z).c = ret;
                    }
                }
                continue 'outer2;
            }
            (*z).c = c8;
            break;
        }
        (*z).c = c7;
    }
    1
}

unsafe fn r_mark_regions(z: *mut SN_env) -> c_int {
    *(*z).I.offset(1) = (*z).l;
    {
        let c1 = (*z).c;
        'lab0: {
            (*z).bra = (*z).c;
            if eq_s(z, 4, S_11.as_ptr()) == 0 {
                (*z).c = c1;
                break 'lab0;
            }
            (*z).ket = (*z).c;
            {
                let c2 = (*z).c;
                'lab1: {
                    'lab2: {
                        let c3 = (*z).c;
                        'lab3: {
                            if eq_s(z, 4, S_12.as_ptr()) == 0 {
                                break 'lab3;
                            }
                            break 'lab2;
                        }
                        (*z).c = c3;
                        'lab4: {
                            if eq_s(z, 4, S_13.as_ptr()) == 0 {
                                break 'lab4;
                            }
                            break 'lab2;
                        }
                        (*z).c = c3;
                        if (*z).c < (*z).l {
                            break 'lab1;
                        }
                    }
                    (*z).c = c1;
                    break 'lab0;
                }
                (*z).c = c2;
            }
            {
                let ret = slice_from_s(z, 2, S_14.as_ptr());
                if ret < 0 {
                    return ret;
                }
            }
        }
    }
    {
        let c4 = (*z).c;
        'lab5: {
            if find_among(z, A_1.as_ptr(), 40) == 0 {
                (*z).c = c4;
                break 'lab5;
            }
            'lab6: {
                let c5 = (*z).c;
                'lab7: {
                    {
                        let c_test6 = (*z).c;
                        'lab8: {
                            let c7 = (*z).c;
                            'lab9: {
                                if eq_s(z, 8, S_15.as_ptr()) == 0 {
                                    break 'lab9;
                                }
                                break 'lab8;
                            }
                            (*z).c = c7;
                            'lab10: {
                                if eq_s(z, 8, S_16.as_ptr()) == 0 {
                                    break 'lab10;
                                }
                                break 'lab8;
                            }
                            (*z).c = c7;
                            if eq_s(z, 8, S_17.as_ptr()) == 0 {
                                break 'lab7;
                            }
                        }
                        if (*z).c < (*z).l {
                            break 'lab7;
                        }
                        (*z).c = c_test6;
                    }
                    break 'lab6;
                }
                (*z).c = c5;
                'lab11: {
                    {
                        let c_test8 = (*z).c;
                        if eq_s(z, 8, S_18.as_ptr()) == 0 {
                            break 'lab11;
                        }
                        (*z).c = c_test8;
                    }
                    break 'lab6;
                }
                (*z).c = c5;
                'lab12: {
                    (*z).bra = (*z).c;
                    if eq_s(z, 4, S_19.as_ptr()) == 0 {
                        break 'lab12;
                    }
                    (*z).ket = (*z).c;
                    {
                        let ret = slice_from_s(z, 2, S_20.as_ptr());
                        if ret < 0 {
                            return ret;
                        }
                    }
                    break 'lab6;
                }
                (*z).c = c5;
                (*z).bra = (*z).c;
                if eq_s(z, 4, S_21.as_ptr()) == 0 {
                    (*z).c = c4;
                    break 'lab5;
                }
                (*z).ket = (*z).c;
                {
                    let ret = slice_from_s(z, 3, S_22.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
            }
        }
    }
    {
        let c_test9 = (*z).c;
        {
            let ret = skip_utf8((*z).p, (*z).c, (*z).l, 3);
            if ret < 0 {
                return 0;
            }
            (*z).c = ret;
        }
        *(*z).I.offset(0) = (*z).c;
        (*z).c = c_test9;
    }
    {
        let c10 = (*z).c;
        'lab13: {
            if (*z).c + 5 >= (*z).l
                || (*(*z).p.offset(((*z).c + 5) as isize) != 169
                    && *(*z).p.offset(((*z).c + 5) as isize) != 168)
            {
                (*z).c = c10;
                break 'lab13;
            }
            if find_among(z, A_2.as_ptr(), 4) == 0 {
                (*z).c = c10;
                break 'lab13;
            }
        }
    }
    {
        let c11 = (*z).c;
        'lab14: {
            if in_grouping_U(z, G_CONSONANT.as_ptr(), 1489, 1520, 0) != 0 {
                break 'lab14;
            }
            if in_grouping_U(z, G_CONSONANT.as_ptr(), 1489, 1520, 0) != 0 {
                break 'lab14;
            }
            if in_grouping_U(z, G_CONSONANT.as_ptr(), 1489, 1520, 0) != 0 {
                break 'lab14;
            }
            *(*z).I.offset(1) = (*z).c;
            return 0;
        }
        (*z).c = c11;
    }

    if out_grouping_U(z, G_VOWEL.as_ptr(), 1488, 1522, 1) < 0 {
        return 0;
    }
    'zyid9: loop {
        'lab15: {
            if in_grouping_U(z, G_VOWEL.as_ptr(), 1488, 1522, 0) != 0 {
                break 'lab15;
            }
            continue 'zyid9;
        }
        break;
    }
    *(*z).I.offset(1) = (*z).c;

    'lab16: {
        if *(*z).I.offset(1) >= *(*z).I.offset(0) {
            break 'lab16;
        }
        *(*z).I.offset(1) = *(*z).I.offset(0);
    }
    1
}

unsafe fn r_R1(z: *mut SN_env) -> c_int {
    (*(*z).I.offset(1) <= (*z).c) as c_int
}

unsafe fn r_R1plus3(z: *mut SN_env) -> c_int {
    (*(*z).I.offset(1) <= ((*z).c + 6)) as c_int
}

unsafe fn r_standard_suffix(z: *mut SN_env) -> c_int {
    let mut among_var;
    {
        let m1 = (*z).l - (*z).c;
        'lab0: {
            (*z).ket = (*z).c;
            among_var = find_among_b(z, A_4.as_ptr(), 79);
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
                        let ret = slice_del(z);
                        if ret < 0 {
                            return ret;
                        }
                    }
                }
                2 => {
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
                        let ret = slice_from_s(z, 4, S_23.as_ptr());
                        if ret < 0 {
                            return ret;
                        }
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
                    {
                        let ret = slice_del(z);
                        if ret < 0 {
                            return ret;
                        }
                    }
                    (*z).ket = (*z).c;
                    among_var = find_among_b(z, A_3.as_ptr(), 26);
                    if among_var == 0 {
                        break 'lab0;
                    }
                    (*z).bra = (*z).c;
                    match among_var {
                        1 => {
                            let ret = slice_from_s(z, 4, S_24.as_ptr());
                            if ret < 0 {
                                return ret;
                            }
                        }
                        2 => {
                            let ret = slice_from_s(z, 6, S_25.as_ptr());
                            if ret < 0 {
                                return ret;
                            }
                        }
                        3 => {
                            let ret = slice_from_s(z, 6, S_26.as_ptr());
                            if ret < 0 {
                                return ret;
                            }
                        }
                        4 => {
                            let ret = slice_from_s(z, 6, S_27.as_ptr());
                            if ret < 0 {
                                return ret;
                            }
                        }
                        5 => {
                            let ret = slice_from_s(z, 6, S_28.as_ptr());
                            if ret < 0 {
                                return ret;
                            }
                        }
                        6 => {
                            let ret = slice_from_s(z, 6, S_29.as_ptr());
                            if ret < 0 {
                                return ret;
                            }
                        }
                        7 => {
                            let ret = slice_from_s(z, 8, S_30.as_ptr());
                            if ret < 0 {
                                return ret;
                            }
                        }
                        8 => {
                            let ret = slice_from_s(z, 6, S_31.as_ptr());
                            if ret < 0 {
                                return ret;
                            }
                        }
                        9 => {
                            let ret = slice_from_s(z, 8, S_32.as_ptr());
                            if ret < 0 {
                                return ret;
                            }
                        }
                        10 => {
                            let ret = slice_from_s(z, 6, S_33.as_ptr());
                            if ret < 0 {
                                return ret;
                            }
                        }
                        11 => {
                            let ret = slice_from_s(z, 6, S_34.as_ptr());
                            if ret < 0 {
                                return ret;
                            }
                        }
                        12 => {
                            let ret = slice_from_s(z, 8, S_35.as_ptr());
                            if ret < 0 {
                                return ret;
                            }
                        }
                        13 => {
                            let ret = slice_from_s(z, 8, S_36.as_ptr());
                            if ret < 0 {
                                return ret;
                            }
                        }
                        14 => {
                            let ret = slice_from_s(z, 8, S_37.as_ptr());
                            if ret < 0 {
                                return ret;
                            }
                        }
                        15 => {
                            let ret = slice_from_s(z, 8, S_38.as_ptr());
                            if ret < 0 {
                                return ret;
                            }
                        }
                        16 => {
                            let ret = slice_from_s(z, 8, S_39.as_ptr());
                            if ret < 0 {
                                return ret;
                            }
                        }
                        17 => {
                            let ret = slice_from_s(z, 8, S_40.as_ptr());
                            if ret < 0 {
                                return ret;
                            }
                        }
                        18 => {
                            let ret = slice_from_s(z, 8, S_41.as_ptr());
                            if ret < 0 {
                                return ret;
                            }
                        }
                        19 => {
                            let ret = slice_from_s(z, 10, S_42.as_ptr());
                            if ret < 0 {
                                return ret;
                            }
                        }
                        20 => {
                            let ret = slice_from_s(z, 10, S_43.as_ptr());
                            if ret < 0 {
                                return ret;
                            }
                        }
                        21 => {
                            let ret = slice_from_s(z, 10, S_44.as_ptr());
                            if ret < 0 {
                                return ret;
                            }
                        }
                        22 => {
                            let ret = slice_from_s(z, 6, S_45.as_ptr());
                            if ret < 0 {
                                return ret;
                            }
                        }
                        23 => {
                            let ret = slice_from_s(z, 6, S_46.as_ptr());
                            if ret < 0 {
                                return ret;
                            }
                        }
                        24 => {
                            let ret = slice_from_s(z, 12, S_47.as_ptr());
                            if ret < 0 {
                                return ret;
                            }
                        }
                        25 => {
                            let ret = slice_from_s(z, 6, S_48.as_ptr());
                            if ret < 0 {
                                return ret;
                            }
                        }
                        26 => {
                            let ret = slice_from_s(z, 8, S_49.as_ptr());
                            if ret < 0 {
                                return ret;
                            }
                        }
                        _ => {}
                    }
                }
                4 => {
                    'lab1: {
                        let m2 = (*z).l - (*z).c;
                        'lab2: {
                            {
                                let ret = r_R1(z);
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
                        (*z).c = (*z).l - m2;
                        {
                            let ret = slice_from_s(z, 2, S_50.as_ptr());
                            if ret < 0 {
                                return ret;
                            }
                        }
                    }
                    (*z).ket = (*z).c;
                    if eq_s_b(z, 8, S_51.as_ptr()) == 0 {
                        break 'lab0;
                    }
                    {
                        let m3 = (*z).l - (*z).c;
                        'lab3: {
                            if eq_s_b(z, 4, S_52.as_ptr()) == 0 {
                                (*z).c = (*z).l - m3;
                                break 'lab3;
                            }
                        }
                    }
                    (*z).bra = (*z).c;
                    {
                        let ret = slice_from_s(z, 10, S_53.as_ptr());
                        if ret < 0 {
                            return ret;
                        }
                    }
                }
                5 => {
                    let ret = slice_from_s(z, 4, S_54.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
                6 => {
                    let ret = slice_from_s(z, 6, S_55.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
                7 => {
                    let ret = slice_from_s(z, 8, S_56.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
                8 => {
                    let ret = slice_from_s(z, 6, S_57.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
                9 => {
                    let ret = slice_from_s(z, 6, S_58.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
                10 => {
                    let ret = slice_from_s(z, 6, S_59.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
                11 => {
                    let ret = slice_from_s(z, 6, S_60.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
                12 => {
                    let ret = slice_from_s(z, 8, S_61.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
                13 => {
                    let ret = slice_from_s(z, 6, S_62.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
                14 => {
                    let ret = slice_from_s(z, 8, S_63.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
                15 => {
                    let ret = slice_from_s(z, 6, S_64.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
                16 => {
                    let ret = slice_from_s(z, 6, S_65.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
                17 => {
                    let ret = slice_from_s(z, 8, S_66.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
                18 => {
                    let ret = slice_from_s(z, 8, S_67.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
                19 => {
                    let ret = slice_from_s(z, 8, S_68.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
                20 => {
                    let ret = slice_from_s(z, 8, S_69.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
                21 => {
                    let ret = slice_from_s(z, 8, S_70.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
                22 => {
                    let ret = slice_from_s(z, 8, S_71.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
                23 => {
                    let ret = slice_from_s(z, 10, S_72.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
                24 => {
                    let ret = slice_from_s(z, 10, S_73.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
                25 => {
                    let ret = slice_from_s(z, 10, S_74.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
                26 => {
                    let ret = slice_from_s(z, 6, S_75.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
                27 => {
                    let ret = slice_from_s(z, 6, S_76.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
                28 => {
                    let ret = slice_from_s(z, 12, S_77.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
                29 => {
                    let ret = slice_from_s(z, 6, S_78.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
                30 => {
                    let ret = slice_from_s(z, 8, S_79.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
                31 => {
                    let ret = slice_from_s(z, 10, S_80.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
                32 => {
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
                        let ret = slice_from_s(z, 2, S_81.as_ptr());
                        if ret < 0 {
                            return ret;
                        }
                    }
                }
                33 => {
                    'lab4: {
                        let m4 = (*z).l - (*z).c;
                        'lab5: {
                            {
                                let m5 = (*z).l - (*z).c;
                                'lab6: {
                                    if eq_s_b(z, 2, S_82.as_ptr()) != 0 {
                                        break 'lab6;
                                    }
                                    (*z).c = (*z).l - m5;
                                    if eq_s_b(z, 2, S_83.as_ptr()) == 0 {
                                        break 'lab5;
                                    }
                                }
                                {
                                    let m6 = (*z).l - (*z).c;
                                    'lab7: {
                                        {
                                            let ret = r_R1plus3(z);
                                            if ret == 0 {
                                                (*z).c = (*z).l - m6;
                                                break 'lab7;
                                            }
                                            if ret < 0 {
                                                return ret;
                                            }
                                        }
                                        {
                                            let ret = slice_from_s(z, 4, S_84.as_ptr());
                                            if ret < 0 {
                                                return ret;
                                            }
                                        }
                                    }
                                }
                                break 'lab4;
                            }
                            (*z).c = (*z).l - m4;
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
        }
        (*z).c = (*z).l - m1;
    }
    {
        let m7 = (*z).l - (*z).c;
        'lab9: {
            (*z).ket = (*z).c;
            if (*z).c - 1 <= (*z).lb
                || *(*z).p.offset(((*z).c - 1) as isize) as c_int >> 5 != 4
                || (285474816 >> (*(*z).p.offset(((*z).c - 1) as isize) as c_int & 0x1f)) & 1 == 0
            {
                break 'lab9;
            }
            among_var = find_among_b(z, A_5.as_ptr(), 6);
            if among_var == 0 {
                break 'lab9;
            }
            (*z).bra = (*z).c;
            match among_var {
                1 => {
                    {
                        let ret = r_R1(z);
                        if ret == 0 {
                            break 'lab9;
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
                        let ret = r_R1(z);
                        if ret == 0 {
                            break 'lab9;
                        }
                        if ret < 0 {
                            return ret;
                        }
                    }
                    if in_grouping_b_U(z, G_CONSONANT.as_ptr(), 1489, 1520, 0) != 0 {
                        break 'lab9;
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
        (*z).c = (*z).l - m7;
    }
    {
        let m8 = (*z).l - (*z).c;
        'lab10: {
            (*z).ket = (*z).c;
            among_var = find_among_b(z, A_6.as_ptr(), 9);
            if among_var == 0 {
                break 'lab10;
            }
            (*z).bra = (*z).c;
            if among_var == 1 {
                {
                    let ret = r_R1(z);
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
            }
        }
        (*z).c = (*z).l - m8;
    }
    {
        let m9 = (*z).l - (*z).c;
        'outer3: loop {
            let m10 = (*z).l - (*z).c;
            'lab12: {
                'zyid10: loop {
                    let m11 = (*z).l - (*z).c;
                    'lab13: {
                        (*z).ket = (*z).c;
                        'lab14: {
                            let m12 = (*z).l - (*z).c;
                            'lab15: {
                                if eq_s_b(z, 2, S_85.as_ptr()) == 0 {
                                    break 'lab15;
                                }
                                break 'lab14;
                            }
                            (*z).c = (*z).l - m12;
                            if eq_s_b(z, 3, S_86.as_ptr()) == 0 {
                                break 'lab13;
                            }
                        }
                        (*z).bra = (*z).c;
                        {
                            let ret = slice_del(z);
                            if ret < 0 {
                                return ret;
                            }
                        }
                        (*z).c = (*z).l - m11;
                        break 'zyid10;
                    }
                    (*z).c = (*z).l - m11;
                    {
                        let ret = skip_b_utf8((*z).p, (*z).c, (*z).lb, 1);
                        if ret < 0 {
                            break 'lab12;
                        }
                        (*z).c = ret;
                    }
                }
                continue 'outer3;
            }
            (*z).c = (*z).l - m10;
            break;
        }
        (*z).c = (*z).l - m9;
    }
    1
}

// ---------------------------------------------------------------------------
// exported entry points
// ---------------------------------------------------------------------------

#[no_mangle]
pub unsafe extern "C" fn yiddish_UTF_8_stem(z: *mut SN_env) -> c_int {
    {
        let ret = r_prelude(z);
        if ret < 0 {
            return ret;
        }
    }
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
        let ret = r_standard_suffix(z);
        if ret < 0 {
            return ret;
        }
    }
    (*z).c = (*z).lb;
    1
}

#[no_mangle]
pub unsafe extern "C" fn yiddish_UTF_8_create_env() -> *mut SN_env {
    SN_create_env(0, 2)
}

#[no_mangle]
pub unsafe extern "C" fn yiddish_UTF_8_close_env(z: *mut SN_env) {
    SN_close_env(z, 0)
}

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snowball::api::SN_set_current;

    unsafe fn stem(word: &[u8]) -> Vec<u8> {
        let z = yiddish_UTF_8_create_env();
        assert!(!z.is_null());
        let rc = SN_set_current(z, word.len() as c_int, word.as_ptr());
        assert!(rc >= 0);
        let rc = yiddish_UTF_8_stem(z);
        assert!(rc >= 0, "stem returned {rc}");
        let out = std::slice::from_raw_parts((*z).p, SIZE((*z).p) as usize).to_vec();
        yiddish_UTF_8_close_env(z);
        out
    }

    #[test]
    fn idempotent() {
        unsafe {
            for w in [
                "\u{5e9}\u{5e8}\u{5d9}\u{5d9}\u{5d1}\u{5df}".as_bytes(),
                "\u{5d4}\u{5d9}\u{5d9}\u{5d6}\u{5dc}".as_bytes(),
            ] {
                let once = stem(w);
                let twice = stem(&once);
                assert_eq!(once, twice, "not idempotent for {:?}", w);
            }
        }
    }
}
