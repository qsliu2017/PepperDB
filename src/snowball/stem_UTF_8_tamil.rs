//! Tamil Snowball stemmer (UTF-8).
//!
//! 1:1 translation of the generated
//! `src/backend/snowball/libstemmer/stem_UTF_8_tamil.c` (Snowball 2.2.0),
//! merged with its header `stem_UTF_8_tamil.h`. The runtime helpers come from
//! `crate::snowball::api` and `crate::snowball::utilities`.

use crate::prelude::*;

use crate::snowball::api::{among, symbol, SN_close_env, SN_create_env, SN_env, SIZE};
use crate::snowball::utilities::{
    eq_s, eq_s_b, find_among, find_among_b, len_utf8, slice_del, slice_from_s,
};

// ---------------------------------------------------------------------------
// among string tables
// ---------------------------------------------------------------------------

static S_0_0: [symbol; 6] = [0xE0, 0xAE, 0xB5, 0xE0, 0xAF, 0x81];
static S_0_1: [symbol; 6] = [0xE0, 0xAE, 0xB5, 0xE0, 0xAF, 0x82];
static S_0_2: [symbol; 6] = [0xE0, 0xAE, 0xB5, 0xE0, 0xAF, 0x8A];
static S_0_3: [symbol; 6] = [0xE0, 0xAE, 0xB5, 0xE0, 0xAF, 0x8B];

static A_0: [among; 4] = [
    among { s_size: 6, s: S_0_0.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 6, s: S_0_1.as_ptr(), substring_i: -1, result: 4, function: None },
    among { s_size: 6, s: S_0_2.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 6, s: S_0_3.as_ptr(), substring_i: -1, result: 1, function: None },
];

static S_1_0: [symbol; 3] = [0xE0, 0xAE, 0x95];
static S_1_1: [symbol; 3] = [0xE0, 0xAE, 0x99];
static S_1_2: [symbol; 3] = [0xE0, 0xAE, 0x9A];
static S_1_3: [symbol; 3] = [0xE0, 0xAE, 0x9E];
static S_1_4: [symbol; 3] = [0xE0, 0xAE, 0xA4];
static S_1_5: [symbol; 3] = [0xE0, 0xAE, 0xA8];
static S_1_6: [symbol; 3] = [0xE0, 0xAE, 0xAA];
static S_1_7: [symbol; 3] = [0xE0, 0xAE, 0xAE];
static S_1_8: [symbol; 3] = [0xE0, 0xAE, 0xAF];
static S_1_9: [symbol; 3] = [0xE0, 0xAE, 0xB5];

static A_1: [among; 10] = [
    among { s_size: 3, s: S_1_0.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_1_1.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_1_2.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_1_3.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_1_4.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_1_5.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_1_6.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_1_7.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_1_8.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_1_9.as_ptr(), substring_i: -1, result: -1, function: None },
];

static S_2_0: [symbol; 3] = [0xE0, 0xAF, 0x80];
static S_2_1: [symbol; 3] = [0xE0, 0xAF, 0x88];
static S_2_2: [symbol; 3] = [0xE0, 0xAE, 0xBF];

static A_2: [among; 3] = [
    among { s_size: 3, s: S_2_0.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_2_1.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_2_2.as_ptr(), substring_i: -1, result: -1, function: None },
];

static S_3_0: [symbol; 3] = [0xE0, 0xAF, 0x80];
static S_3_1: [symbol; 3] = [0xE0, 0xAF, 0x81];
static S_3_2: [symbol; 3] = [0xE0, 0xAF, 0x82];
static S_3_3: [symbol; 3] = [0xE0, 0xAF, 0x86];
static S_3_4: [symbol; 3] = [0xE0, 0xAF, 0x87];
static S_3_5: [symbol; 3] = [0xE0, 0xAF, 0x88];
static S_3_6: [symbol; 3] = [0xE0, 0xAE, 0xBE];
static S_3_7: [symbol; 3] = [0xE0, 0xAE, 0xBF];

static A_3: [among; 8] = [
    among { s_size: 3, s: S_3_0.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_3_1.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_3_2.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_3_3.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_3_4.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_3_5.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_3_6.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_3_7.as_ptr(), substring_i: -1, result: -1, function: None },
];

static S_4_1: [symbol; 3] = [0xE0, 0xAF, 0x88];
static S_4_2: [symbol; 3] = [0xE0, 0xAF, 0x8D];

static A_4: [among; 3] = [
    among { s_size: 0, s: core::ptr::null(), substring_i: -1, result: 2, function: None },
    among { s_size: 3, s: S_4_1.as_ptr(), substring_i: 0, result: 1, function: None },
    among { s_size: 3, s: S_4_2.as_ptr(), substring_i: 0, result: 1, function: None },
];

static S_5_0: [symbol; 6] = [0xE0, 0xAE, 0xA9, 0xE0, 0xAF, 0x81];
static S_5_1: [symbol; 9] = [0xE0, 0xAF, 0x81, 0xE0, 0xAE, 0x95, 0xE0, 0xAF, 0x8D];
static S_5_2: [symbol; 15] = [
    0xE0, 0xAF, 0x81, 0xE0, 0xAE, 0x95, 0xE0, 0xAF, 0x8D, 0xE0, 0xAE, 0x95, 0xE0, 0xAF, 0x8D,
];
static S_5_3: [symbol; 12] = [
    0xE0, 0xAE, 0x9F, 0xE0, 0xAF, 0x8D, 0xE0, 0xAE, 0x95, 0xE0, 0xAF, 0x8D,
];
static S_5_4: [symbol; 12] = [
    0xE0, 0xAE, 0xB1, 0xE0, 0xAF, 0x8D, 0xE0, 0xAE, 0x95, 0xE0, 0xAF, 0x8D,
];
static S_5_5: [symbol; 6] = [0xE0, 0xAE, 0x99, 0xE0, 0xAF, 0x8D];
static S_5_6: [symbol; 12] = [
    0xE0, 0xAE, 0x9F, 0xE0, 0xAF, 0x8D, 0xE0, 0xAE, 0x9F, 0xE0, 0xAF, 0x8D,
];
static S_5_7: [symbol; 12] = [
    0xE0, 0xAE, 0xA4, 0xE0, 0xAF, 0x8D, 0xE0, 0xAE, 0xA4, 0xE0, 0xAF, 0x8D,
];
static S_5_8: [symbol; 12] = [
    0xE0, 0xAE, 0xA8, 0xE0, 0xAF, 0x8D, 0xE0, 0xAE, 0xA4, 0xE0, 0xAF, 0x8D,
];
static S_5_9: [symbol; 6] = [0xE0, 0xAE, 0xA8, 0xE0, 0xAF, 0x8D];
static S_5_10: [symbol; 12] = [
    0xE0, 0xAE, 0x9F, 0xE0, 0xAF, 0x8D, 0xE0, 0xAE, 0xAA, 0xE0, 0xAF, 0x8D,
];
static S_5_11: [symbol; 6] = [0xE0, 0xAE, 0xAF, 0xE0, 0xAF, 0x8D];
static S_5_12: [symbol; 12] = [
    0xE0, 0xAE, 0xA9, 0xE0, 0xAF, 0x8D, 0xE0, 0xAE, 0xB1, 0xE0, 0xAF, 0x8D,
];
static S_5_13: [symbol; 6] = [0xE0, 0xAE, 0xB5, 0xE0, 0xAF, 0x8D];
static S_5_14: [symbol; 9] = [0xE0, 0xAE, 0xA8, 0xE0, 0xAF, 0x8D, 0xE0, 0xAE, 0xA4];
static S_5_15: [symbol; 3] = [0xE0, 0xAE, 0xAF];
static S_5_16: [symbol; 3] = [0xE0, 0xAE, 0xB5];

static A_5: [among; 17] = [
    among { s_size: 6, s: S_5_0.as_ptr(), substring_i: -1, result: 8, function: None },
    among { s_size: 9, s: S_5_1.as_ptr(), substring_i: -1, result: 7, function: None },
    among { s_size: 15, s: S_5_2.as_ptr(), substring_i: -1, result: 7, function: None },
    among { s_size: 12, s: S_5_3.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 12, s: S_5_4.as_ptr(), substring_i: -1, result: 4, function: None },
    among { s_size: 6, s: S_5_5.as_ptr(), substring_i: -1, result: 9, function: None },
    among { s_size: 12, s: S_5_6.as_ptr(), substring_i: -1, result: 5, function: None },
    among { s_size: 12, s: S_5_7.as_ptr(), substring_i: -1, result: 6, function: None },
    among { s_size: 12, s: S_5_8.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_5_9.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 12, s: S_5_10.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 6, s: S_5_11.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 12, s: S_5_12.as_ptr(), substring_i: -1, result: 4, function: None },
    among { s_size: 6, s: S_5_13.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 9, s: S_5_14.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_5_15.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_5_16.as_ptr(), substring_i: -1, result: 1, function: None },
];

static S_6_0: [symbol; 3] = [0xE0, 0xAE, 0x95];
static S_6_1: [symbol; 3] = [0xE0, 0xAE, 0x9A];
static S_6_2: [symbol; 3] = [0xE0, 0xAE, 0x9F];
static S_6_3: [symbol; 3] = [0xE0, 0xAE, 0xA4];
static S_6_4: [symbol; 3] = [0xE0, 0xAE, 0xAA];
static S_6_5: [symbol; 3] = [0xE0, 0xAE, 0xB1];

static A_6: [among; 6] = [
    among { s_size: 3, s: S_6_0.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_6_1.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_6_2.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_6_3.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_6_4.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_6_5.as_ptr(), substring_i: -1, result: -1, function: None },
];

static S_7_0: [symbol; 3] = [0xE0, 0xAE, 0x95];
static S_7_1: [symbol; 3] = [0xE0, 0xAE, 0x9A];
static S_7_2: [symbol; 3] = [0xE0, 0xAE, 0x9F];
static S_7_3: [symbol; 3] = [0xE0, 0xAE, 0xA4];
static S_7_4: [symbol; 3] = [0xE0, 0xAE, 0xAA];
static S_7_5: [symbol; 3] = [0xE0, 0xAE, 0xB1];

static A_7: [among; 6] = [
    among { s_size: 3, s: S_7_0.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_7_1.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_7_2.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_7_3.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_7_4.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_7_5.as_ptr(), substring_i: -1, result: -1, function: None },
];

static S_8_0: [symbol; 3] = [0xE0, 0xAE, 0x9E];
static S_8_1: [symbol; 3] = [0xE0, 0xAE, 0xA3];
static S_8_2: [symbol; 3] = [0xE0, 0xAE, 0xA8];
static S_8_3: [symbol; 3] = [0xE0, 0xAE, 0xA9];
static S_8_4: [symbol; 3] = [0xE0, 0xAE, 0xAE];
static S_8_5: [symbol; 3] = [0xE0, 0xAE, 0xAF];
static S_8_6: [symbol; 3] = [0xE0, 0xAE, 0xB0];
static S_8_7: [symbol; 3] = [0xE0, 0xAE, 0xB2];
static S_8_8: [symbol; 3] = [0xE0, 0xAE, 0xB3];
static S_8_9: [symbol; 3] = [0xE0, 0xAE, 0xB4];
static S_8_10: [symbol; 3] = [0xE0, 0xAE, 0xB5];

static A_8: [among; 11] = [
    among { s_size: 3, s: S_8_0.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_8_1.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_8_2.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_8_3.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_8_4.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_8_5.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_8_6.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_8_7.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_8_8.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_8_9.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_8_10.as_ptr(), substring_i: -1, result: -1, function: None },
];

static S_9_0: [symbol; 3] = [0xE0, 0xAF, 0x80];
static S_9_1: [symbol; 3] = [0xE0, 0xAF, 0x81];
static S_9_2: [symbol; 3] = [0xE0, 0xAF, 0x82];
static S_9_3: [symbol; 3] = [0xE0, 0xAF, 0x86];
static S_9_4: [symbol; 3] = [0xE0, 0xAF, 0x87];
static S_9_5: [symbol; 3] = [0xE0, 0xAF, 0x88];
static S_9_6: [symbol; 3] = [0xE0, 0xAF, 0x8D];
static S_9_7: [symbol; 3] = [0xE0, 0xAE, 0xBE];
static S_9_8: [symbol; 3] = [0xE0, 0xAE, 0xBF];

static A_9: [among; 9] = [
    among { s_size: 3, s: S_9_0.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_9_1.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_9_2.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_9_3.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_9_4.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_9_5.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_9_6.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_9_7.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_9_8.as_ptr(), substring_i: -1, result: -1, function: None },
];

static S_10_0: [symbol; 3] = [0xE0, 0xAE, 0x85];
static S_10_1: [symbol; 3] = [0xE0, 0xAE, 0x87];
static S_10_2: [symbol; 3] = [0xE0, 0xAE, 0x89];

static A_10: [among; 3] = [
    among { s_size: 3, s: S_10_0.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_10_1.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_10_2.as_ptr(), substring_i: -1, result: -1, function: None },
];

static S_11_0: [symbol; 3] = [0xE0, 0xAE, 0x95];
static S_11_1: [symbol; 3] = [0xE0, 0xAE, 0x99];
static S_11_2: [symbol; 3] = [0xE0, 0xAE, 0x9A];
static S_11_3: [symbol; 3] = [0xE0, 0xAE, 0x9E];
static S_11_4: [symbol; 3] = [0xE0, 0xAE, 0xA4];
static S_11_5: [symbol; 3] = [0xE0, 0xAE, 0xA8];
static S_11_6: [symbol; 3] = [0xE0, 0xAE, 0xAA];
static S_11_7: [symbol; 3] = [0xE0, 0xAE, 0xAE];
static S_11_8: [symbol; 3] = [0xE0, 0xAE, 0xAF];
static S_11_9: [symbol; 3] = [0xE0, 0xAE, 0xB5];

static A_11: [among; 10] = [
    among { s_size: 3, s: S_11_0.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_11_1.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_11_2.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_11_3.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_11_4.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_11_5.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_11_6.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_11_7.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_11_8.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_11_9.as_ptr(), substring_i: -1, result: -1, function: None },
];

static S_12_0: [symbol; 3] = [0xE0, 0xAE, 0x95];
static S_12_1: [symbol; 3] = [0xE0, 0xAE, 0x9A];
static S_12_2: [symbol; 3] = [0xE0, 0xAE, 0x9F];
static S_12_3: [symbol; 3] = [0xE0, 0xAE, 0xA4];
static S_12_4: [symbol; 3] = [0xE0, 0xAE, 0xAA];
static S_12_5: [symbol; 3] = [0xE0, 0xAE, 0xB1];

static A_12: [among; 6] = [
    among { s_size: 3, s: S_12_0.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_12_1.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_12_2.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_12_3.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_12_4.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_12_5.as_ptr(), substring_i: -1, result: -1, function: None },
];

static S_13_0: [symbol; 9] = [0xE0, 0xAE, 0x95, 0xE0, 0xAE, 0xB3, 0xE0, 0xAF, 0x8D];
static S_13_1: [symbol; 18] = [
    0xE0, 0xAF, 0x81, 0xE0, 0xAE, 0x99, 0xE0, 0xAF, 0x8D, 0xE0, 0xAE, 0x95, 0xE0, 0xAE, 0xB3,
    0xE0, 0xAF, 0x8D,
];
static S_13_2: [symbol; 15] = [
    0xE0, 0xAE, 0x9F, 0xE0, 0xAF, 0x8D, 0xE0, 0xAE, 0x95, 0xE0, 0xAE, 0xB3, 0xE0, 0xAF, 0x8D,
];
static S_13_3: [symbol; 15] = [
    0xE0, 0xAE, 0xB1, 0xE0, 0xAF, 0x8D, 0xE0, 0xAE, 0x95, 0xE0, 0xAE, 0xB3, 0xE0, 0xAF, 0x8D,
];

static A_13: [among; 4] = [
    among { s_size: 9, s: S_13_0.as_ptr(), substring_i: -1, result: 4, function: None },
    among { s_size: 18, s: S_13_1.as_ptr(), substring_i: 0, result: 1, function: None },
    among { s_size: 15, s: S_13_2.as_ptr(), substring_i: 0, result: 3, function: None },
    among { s_size: 15, s: S_13_3.as_ptr(), substring_i: 0, result: 2, function: None },
];

static S_14_0: [symbol; 3] = [0xE0, 0xAF, 0x87];
static S_14_1: [symbol; 3] = [0xE0, 0xAF, 0x8B];
static S_14_2: [symbol; 3] = [0xE0, 0xAE, 0xBE];

static A_14: [among; 3] = [
    among { s_size: 3, s: S_14_0.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_14_1.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_14_2.as_ptr(), substring_i: -1, result: -1, function: None },
];

static S_15_0: [symbol; 6] = [0xE0, 0xAE, 0xAA, 0xE0, 0xAE, 0xBF];
static S_15_1: [symbol; 6] = [0xE0, 0xAE, 0xB5, 0xE0, 0xAE, 0xBF];

static A_15: [among; 2] = [
    among { s_size: 6, s: S_15_0.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 6, s: S_15_1.as_ptr(), substring_i: -1, result: -1, function: None },
];

static S_16_0: [symbol; 3] = [0xE0, 0xAF, 0x80];
static S_16_1: [symbol; 3] = [0xE0, 0xAF, 0x81];
static S_16_2: [symbol; 3] = [0xE0, 0xAF, 0x82];
static S_16_3: [symbol; 3] = [0xE0, 0xAF, 0x86];
static S_16_4: [symbol; 3] = [0xE0, 0xAF, 0x87];
static S_16_5: [symbol; 3] = [0xE0, 0xAF, 0x88];
static S_16_6: [symbol; 3] = [0xE0, 0xAE, 0xBE];
static S_16_7: [symbol; 3] = [0xE0, 0xAE, 0xBF];

static A_16: [among; 8] = [
    among { s_size: 3, s: S_16_0.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_16_1.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_16_2.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_16_3.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_16_4.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_16_5.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_16_6.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_16_7.as_ptr(), substring_i: -1, result: -1, function: None },
];

static S_17_0: [symbol; 15] = [
    0xE0, 0xAE, 0xAA, 0xE0, 0xAE, 0x9F, 0xE0, 0xAF, 0x8D, 0xE0, 0xAE, 0x9F, 0xE0, 0xAF, 0x81,
];
static S_17_1: [symbol; 18] = [
    0xE0, 0xAE, 0xB5, 0xE0, 0xAE, 0xBF, 0xE0, 0xAE, 0x9F, 0xE0, 0xAF, 0x8D, 0xE0, 0xAE, 0x9F,
    0xE0, 0xAF, 0x81,
];
static S_17_2: [symbol; 9] = [0xE0, 0xAE, 0xAA, 0xE0, 0xAE, 0x9F, 0xE0, 0xAF, 0x81];
static S_17_3: [symbol; 12] = [
    0xE0, 0xAE, 0xB5, 0xE0, 0xAE, 0xBF, 0xE0, 0xAE, 0x9F, 0xE0, 0xAF, 0x81,
];
static S_17_4: [symbol; 18] = [
    0xE0, 0xAE, 0xAA, 0xE0, 0xAE, 0x9F, 0xE0, 0xAF, 0x8D, 0xE0, 0xAE, 0x9F, 0xE0, 0xAE, 0xA4,
    0xE0, 0xAF, 0x81,
];
static S_17_5: [symbol; 15] = [
    0xE0, 0xAF, 0x86, 0xE0, 0xAE, 0xA9, 0xE0, 0xAF, 0x8D, 0xE0, 0xAE, 0xB1, 0xE0, 0xAF, 0x81,
];
static S_17_6: [symbol; 9] = [0xE0, 0xAF, 0x81, 0xE0, 0xAE, 0x9F, 0xE0, 0xAF, 0x88];
static S_17_7: [symbol; 15] = [
    0xE0, 0xAE, 0xBF, 0xE0, 0xAE, 0xB2, 0xE0, 0xAF, 0x8D, 0xE0, 0xAE, 0xB2, 0xE0, 0xAF, 0x88,
];
static S_17_8: [symbol; 12] = [
    0xE0, 0xAF, 0x81, 0xE0, 0xAE, 0x9F, 0xE0, 0xAE, 0xA9, 0xE0, 0xAF, 0x8D,
];
static S_17_9: [symbol; 15] = [
    0xE0, 0xAF, 0x86, 0xE0, 0xAE, 0xA9, 0xE0, 0xAF, 0x81, 0xE0, 0xAE, 0xAE, 0xE0, 0xAF, 0x8D,
];
static S_17_10: [symbol; 12] = [
    0xE0, 0xAE, 0xBF, 0xE0, 0xAE, 0x9F, 0xE0, 0xAE, 0xAE, 0xE0, 0xAF, 0x8D,
];
static S_17_11: [symbol; 21] = [
    0xE0, 0xAF, 0x86, 0xE0, 0xAE, 0xB2, 0xE0, 0xAF, 0x8D, 0xE0, 0xAE, 0xB2, 0xE0, 0xAE, 0xBE,
    0xE0, 0xAE, 0xAE, 0xE0, 0xAF, 0x8D,
];
static S_17_12: [symbol; 12] = [
    0xE0, 0xAE, 0xAA, 0xE0, 0xAE, 0x9F, 0xE0, 0xAF, 0x8D, 0xE0, 0xAE, 0x9F,
];
static S_17_13: [symbol; 15] = [
    0xE0, 0xAE, 0xAA, 0xE0, 0xAE, 0x9F, 0xE0, 0xAF, 0x8D, 0xE0, 0xAE, 0x9F, 0xE0, 0xAE, 0xA3,
];
static S_17_14: [symbol; 6] = [0xE0, 0xAF, 0x86, 0xE0, 0xAE, 0xA9];
static S_17_15: [symbol; 9] = [0xE0, 0xAE, 0xA4, 0xE0, 0xAE, 0xBE, 0xE0, 0xAE, 0xA9];
static S_17_16: [symbol; 18] = [
    0xE0, 0xAE, 0xAA, 0xE0, 0xAE, 0x9F, 0xE0, 0xAE, 0xBF, 0xE0, 0xAE, 0xA4, 0xE0, 0xAE, 0xBE,
    0xE0, 0xAE, 0xA9,
];
static S_17_17: [symbol; 12] = [
    0xE0, 0xAF, 0x81, 0xE0, 0xAE, 0x9F, 0xE0, 0xAF, 0x88, 0xE0, 0xAE, 0xAF,
];
static S_17_18: [symbol; 12] = [
    0xE0, 0xAE, 0xBE, 0xE0, 0xAE, 0x95, 0xE0, 0xAE, 0xBF, 0xE0, 0xAE, 0xAF,
];
static S_17_19: [symbol; 15] = [
    0xE0, 0xAE, 0x95, 0xE0, 0xAF, 0x81, 0xE0, 0xAE, 0xB0, 0xE0, 0xAE, 0xBF, 0xE0, 0xAE, 0xAF,
];
static S_17_20: [symbol; 9] = [0xE0, 0xAE, 0xB2, 0xE0, 0xAF, 0x8D, 0xE0, 0xAE, 0xB2];
static S_17_21: [symbol; 12] = [
    0xE0, 0xAF, 0x81, 0xE0, 0xAE, 0xB3, 0xE0, 0xAF, 0x8D, 0xE0, 0xAE, 0xB3,
];
static S_17_22: [symbol; 9] = [0xE0, 0xAE, 0xBE, 0xE0, 0xAE, 0x95, 0xE0, 0xAE, 0xBF];
static S_17_23: [symbol; 9] = [0xE0, 0xAE, 0xAA, 0xE0, 0xAE, 0x9F, 0xE0, 0xAE, 0xBF];
static S_17_24: [symbol; 15] = [
    0xE0, 0xAE, 0xBF, 0xE0, 0xAE, 0xA9, 0xE0, 0xAF, 0x8D, 0xE0, 0xAE, 0xB1, 0xE0, 0xAE, 0xBF,
];
static S_17_25: [symbol; 15] = [
    0xE0, 0xAE, 0xAA, 0xE0, 0xAE, 0xB1, 0xE0, 0xAF, 0x8D, 0xE0, 0xAE, 0xB1, 0xE0, 0xAE, 0xBF,
];

static A_17: [among; 26] = [
    among { s_size: 15, s: S_17_0.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 18, s: S_17_1.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 9, s: S_17_2.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 12, s: S_17_3.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 18, s: S_17_4.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 15, s: S_17_5.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 9, s: S_17_6.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 15, s: S_17_7.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 12, s: S_17_8.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 15, s: S_17_9.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 12, s: S_17_10.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 21, s: S_17_11.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 12, s: S_17_12.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 15, s: S_17_13.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 6, s: S_17_14.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 9, s: S_17_15.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 18, s: S_17_16.as_ptr(), substring_i: 15, result: 3, function: None },
    among { s_size: 12, s: S_17_17.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 12, s: S_17_18.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 15, s: S_17_19.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 9, s: S_17_20.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 12, s: S_17_21.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 9, s: S_17_22.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 9, s: S_17_23.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 15, s: S_17_24.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 15, s: S_17_25.as_ptr(), substring_i: -1, result: 3, function: None },
];

static S_18_0: [symbol; 3] = [0xE0, 0xAF, 0x80];
static S_18_1: [symbol; 3] = [0xE0, 0xAF, 0x81];
static S_18_2: [symbol; 3] = [0xE0, 0xAF, 0x82];
static S_18_3: [symbol; 3] = [0xE0, 0xAF, 0x86];
static S_18_4: [symbol; 3] = [0xE0, 0xAF, 0x87];
static S_18_5: [symbol; 3] = [0xE0, 0xAF, 0x88];
static S_18_6: [symbol; 3] = [0xE0, 0xAE, 0xBE];
static S_18_7: [symbol; 3] = [0xE0, 0xAE, 0xBF];

static A_18: [among; 8] = [
    among { s_size: 3, s: S_18_0.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_18_1.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_18_2.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_18_3.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_18_4.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_18_5.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_18_6.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_18_7.as_ptr(), substring_i: -1, result: -1, function: None },
];

static S_19_0: [symbol; 3] = [0xE0, 0xAF, 0x80];
static S_19_1: [symbol; 3] = [0xE0, 0xAF, 0x81];
static S_19_2: [symbol; 3] = [0xE0, 0xAF, 0x82];
static S_19_3: [symbol; 3] = [0xE0, 0xAF, 0x86];
static S_19_4: [symbol; 3] = [0xE0, 0xAF, 0x87];
static S_19_5: [symbol; 3] = [0xE0, 0xAF, 0x88];
static S_19_6: [symbol; 3] = [0xE0, 0xAE, 0xBE];
static S_19_7: [symbol; 3] = [0xE0, 0xAE, 0xBF];

static A_19: [among; 8] = [
    among { s_size: 3, s: S_19_0.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_19_1.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_19_2.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_19_3.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_19_4.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_19_5.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_19_6.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_19_7.as_ptr(), substring_i: -1, result: -1, function: None },
];

static S_20_0: [symbol; 3] = [0xE0, 0xAF, 0x80];
static S_20_1: [symbol; 9] = [0xE0, 0xAF, 0x8A, 0xE0, 0xAE, 0x9F, 0xE0, 0xAF, 0x81];
static S_20_2: [symbol; 9] = [0xE0, 0xAF, 0x8B, 0xE0, 0xAE, 0x9F, 0xE0, 0xAF, 0x81];
static S_20_3: [symbol; 6] = [0xE0, 0xAE, 0xA4, 0xE0, 0xAF, 0x81];
static S_20_4: [symbol; 21] = [
    0xE0, 0xAE, 0xBF, 0xE0, 0xAE, 0xB0, 0xE0, 0xAF, 0x81, 0xE0, 0xAE, 0xA8, 0xE0, 0xAF, 0x8D,
    0xE0, 0xAE, 0xA4, 0xE0, 0xAF, 0x81,
];
static S_20_5: [symbol; 15] = [
    0xE0, 0xAE, 0xBF, 0xE0, 0xAE, 0xA9, 0xE0, 0xAF, 0x8D, 0xE0, 0xAE, 0xB1, 0xE0, 0xAF, 0x81,
];
static S_20_6: [symbol; 9] = [0xE0, 0xAF, 0x81, 0xE0, 0xAE, 0x9F, 0xE0, 0xAF, 0x88];
static S_20_7: [symbol; 6] = [0xE0, 0xAE, 0xA9, 0xE0, 0xAF, 0x88];
static S_20_8: [symbol; 9] = [0xE0, 0xAE, 0x95, 0xE0, 0xAE, 0xA3, 0xE0, 0xAF, 0x8D];
static S_20_9: [symbol; 12] = [
    0xE0, 0xAE, 0xAE, 0xE0, 0xAF, 0x81, 0xE0, 0xAE, 0xA9, 0xE0, 0xAF, 0x8D,
];
static S_20_10: [symbol; 9] = [0xE0, 0xAE, 0xBF, 0xE0, 0xAE, 0xA9, 0xE0, 0xAF, 0x8D];
static S_20_11: [symbol; 12] = [
    0xE0, 0xAE, 0xBF, 0xE0, 0xAE, 0x9F, 0xE0, 0xAE, 0xAE, 0xE0, 0xAF, 0x8D,
];
static S_20_12: [symbol; 12] = [
    0xE0, 0xAE, 0xAE, 0xE0, 0xAF, 0x87, 0xE0, 0xAE, 0xB1, 0xE0, 0xAF, 0x8D,
];
static S_20_13: [symbol; 9] = [0xE0, 0xAE, 0xBF, 0xE0, 0xAE, 0xB1, 0xE0, 0xAF, 0x8D];
static S_20_14: [symbol; 6] = [0xE0, 0xAE, 0xB2, 0xE0, 0xAF, 0x8D];
static S_20_15: [symbol; 12] = [
    0xE0, 0xAE, 0xAE, 0xE0, 0xAF, 0x87, 0xE0, 0xAE, 0xB2, 0xE0, 0xAF, 0x8D,
];
static S_20_16: [symbol; 12] = [
    0xE0, 0xAE, 0xBE, 0xE0, 0xAE, 0xAE, 0xE0, 0xAE, 0xB2, 0xE0, 0xAF, 0x8D,
];
static S_20_17: [symbol; 9] = [0xE0, 0xAE, 0xBE, 0xE0, 0xAE, 0xB2, 0xE0, 0xAF, 0x8D];
static S_20_18: [symbol; 9] = [0xE0, 0xAE, 0xBF, 0xE0, 0xAE, 0xB2, 0xE0, 0xAF, 0x8D];
static S_20_19: [symbol; 9] = [0xE0, 0xAF, 0x81, 0xE0, 0xAE, 0xB3, 0xE0, 0xAF, 0x8D];
static S_20_20: [symbol; 12] = [
    0xE0, 0xAE, 0x95, 0xE0, 0xAF, 0x80, 0xE0, 0xAE, 0xB4, 0xE0, 0xAF, 0x8D,
];
static S_20_21: [symbol; 9] = [0xE0, 0xAE, 0xB5, 0xE0, 0xAE, 0xBF, 0xE0, 0xAE, 0x9F];

static A_20: [among; 22] = [
    among { s_size: 3, s: S_20_0.as_ptr(), substring_i: -1, result: 7, function: None },
    among { s_size: 9, s: S_20_1.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 9, s: S_20_2.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 6, s: S_20_3.as_ptr(), substring_i: -1, result: 6, function: None },
    among { s_size: 21, s: S_20_4.as_ptr(), substring_i: 3, result: 2, function: None },
    among { s_size: 15, s: S_20_5.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 9, s: S_20_6.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 6, s: S_20_7.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 9, s: S_20_8.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 12, s: S_20_9.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 9, s: S_20_10.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 12, s: S_20_11.as_ptr(), substring_i: -1, result: 4, function: None },
    among { s_size: 12, s: S_20_12.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 9, s: S_20_13.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 6, s: S_20_14.as_ptr(), substring_i: -1, result: 5, function: None },
    among { s_size: 12, s: S_20_15.as_ptr(), substring_i: 14, result: 1, function: None },
    among { s_size: 12, s: S_20_16.as_ptr(), substring_i: 14, result: 2, function: None },
    among { s_size: 9, s: S_20_17.as_ptr(), substring_i: 14, result: 2, function: None },
    among { s_size: 9, s: S_20_18.as_ptr(), substring_i: 14, result: 2, function: None },
    among { s_size: 9, s: S_20_19.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 12, s: S_20_20.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 9, s: S_20_21.as_ptr(), substring_i: -1, result: 2, function: None },
];

static S_21_0: [symbol; 3] = [0xE0, 0xAE, 0x95];
static S_21_1: [symbol; 3] = [0xE0, 0xAE, 0x9A];
static S_21_2: [symbol; 3] = [0xE0, 0xAE, 0x9F];
static S_21_3: [symbol; 3] = [0xE0, 0xAE, 0xA4];
static S_21_4: [symbol; 3] = [0xE0, 0xAE, 0xAA];
static S_21_5: [symbol; 3] = [0xE0, 0xAE, 0xB1];

static A_21: [among; 6] = [
    among { s_size: 3, s: S_21_0.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_21_1.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_21_2.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_21_3.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_21_4.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_21_5.as_ptr(), substring_i: -1, result: -1, function: None },
];

static S_22_0: [symbol; 3] = [0xE0, 0xAE, 0x95];
static S_22_1: [symbol; 3] = [0xE0, 0xAE, 0x9A];
static S_22_2: [symbol; 3] = [0xE0, 0xAE, 0x9F];
static S_22_3: [symbol; 3] = [0xE0, 0xAE, 0xA4];
static S_22_4: [symbol; 3] = [0xE0, 0xAE, 0xAA];
static S_22_5: [symbol; 3] = [0xE0, 0xAE, 0xB1];

static A_22: [among; 6] = [
    among { s_size: 3, s: S_22_0.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_22_1.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_22_2.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_22_3.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_22_4.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_22_5.as_ptr(), substring_i: -1, result: -1, function: None },
];

static S_23_0: [symbol; 3] = [0xE0, 0xAE, 0x85];
static S_23_1: [symbol; 3] = [0xE0, 0xAE, 0x86];
static S_23_2: [symbol; 3] = [0xE0, 0xAE, 0x87];
static S_23_3: [symbol; 3] = [0xE0, 0xAE, 0x88];
static S_23_4: [symbol; 3] = [0xE0, 0xAE, 0x89];
static S_23_5: [symbol; 3] = [0xE0, 0xAE, 0x8A];
static S_23_6: [symbol; 3] = [0xE0, 0xAE, 0x8E];
static S_23_7: [symbol; 3] = [0xE0, 0xAE, 0x8F];
static S_23_8: [symbol; 3] = [0xE0, 0xAE, 0x90];
static S_23_9: [symbol; 3] = [0xE0, 0xAE, 0x92];
static S_23_10: [symbol; 3] = [0xE0, 0xAE, 0x93];
static S_23_11: [symbol; 3] = [0xE0, 0xAE, 0x94];

static A_23: [among; 12] = [
    among { s_size: 3, s: S_23_0.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_23_1.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_23_2.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_23_3.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_23_4.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_23_5.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_23_6.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_23_7.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_23_8.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_23_9.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_23_10.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_23_11.as_ptr(), substring_i: -1, result: -1, function: None },
];

static S_24_0: [symbol; 3] = [0xE0, 0xAF, 0x80];
static S_24_1: [symbol; 3] = [0xE0, 0xAF, 0x81];
static S_24_2: [symbol; 3] = [0xE0, 0xAF, 0x82];
static S_24_3: [symbol; 3] = [0xE0, 0xAF, 0x86];
static S_24_4: [symbol; 3] = [0xE0, 0xAF, 0x87];
static S_24_5: [symbol; 3] = [0xE0, 0xAF, 0x88];
static S_24_6: [symbol; 3] = [0xE0, 0xAE, 0xBE];
static S_24_7: [symbol; 3] = [0xE0, 0xAE, 0xBF];

static A_24: [among; 8] = [
    among { s_size: 3, s: S_24_0.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_24_1.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_24_2.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_24_3.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_24_4.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_24_5.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_24_6.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_24_7.as_ptr(), substring_i: -1, result: -1, function: None },
];

static S_25_0: [symbol; 6] = [0xE0, 0xAE, 0x95, 0xE0, 0xAF, 0x81];
static S_25_1: [symbol; 9] = [0xE0, 0xAE, 0xAA, 0xE0, 0xAE, 0x9F, 0xE0, 0xAF, 0x81];
static S_25_2: [symbol; 6] = [0xE0, 0xAE, 0xA4, 0xE0, 0xAF, 0x81];
static S_25_3: [symbol; 15] = [
    0xE0, 0xAE, 0xBF, 0xE0, 0xAE, 0xB1, 0xE0, 0xAF, 0x8D, 0xE0, 0xAE, 0xB1, 0xE0, 0xAF, 0x81,
];
static S_25_4: [symbol; 6] = [0xE0, 0xAE, 0xA9, 0xE0, 0xAF, 0x88];
static S_25_5: [symbol; 6] = [0xE0, 0xAE, 0xB5, 0xE0, 0xAF, 0x88];
static S_25_6: [symbol; 12] = [
    0xE0, 0xAE, 0xA9, 0xE0, 0xAF, 0x86, 0xE0, 0xAE, 0xA9, 0xE0, 0xAF, 0x8D,
];
static S_25_7: [symbol; 9] = [0xE0, 0xAF, 0x87, 0xE0, 0xAE, 0xA9, 0xE0, 0xAF, 0x8D];
static S_25_8: [symbol; 9] = [0xE0, 0xAE, 0xA9, 0xE0, 0xAE, 0xA9, 0xE0, 0xAF, 0x8D];
static S_25_9: [symbol; 9] = [0xE0, 0xAE, 0xAA, 0xE0, 0xAE, 0xA9, 0xE0, 0xAF, 0x8D];
static S_25_10: [symbol; 9] = [0xE0, 0xAE, 0xB5, 0xE0, 0xAE, 0xA9, 0xE0, 0xAF, 0x8D];
static S_25_11: [symbol; 9] = [0xE0, 0xAE, 0xBE, 0xE0, 0xAE, 0xA9, 0xE0, 0xAF, 0x8D];
static S_25_12: [symbol; 12] = [
    0xE0, 0xAE, 0xA9, 0xE0, 0xAE, 0xBE, 0xE0, 0xAE, 0xA9, 0xE0, 0xAF, 0x8D,
];
static S_25_13: [symbol; 12] = [
    0xE0, 0xAE, 0xAE, 0xE0, 0xAE, 0xBF, 0xE0, 0xAE, 0xA9, 0xE0, 0xAF, 0x8D,
];
static S_25_14: [symbol; 12] = [
    0xE0, 0xAE, 0x95, 0xE0, 0xAF, 0x81, 0xE0, 0xAE, 0xAE, 0xE0, 0xAF, 0x8D,
];
static S_25_15: [symbol; 12] = [
    0xE0, 0xAE, 0x9F, 0xE0, 0xAF, 0x81, 0xE0, 0xAE, 0xAE, 0xE0, 0xAF, 0x8D,
];
static S_25_16: [symbol; 12] = [
    0xE0, 0xAE, 0xA4, 0xE0, 0xAF, 0x81, 0xE0, 0xAE, 0xAE, 0xE0, 0xAF, 0x8D,
];
static S_25_17: [symbol; 12] = [
    0xE0, 0xAE, 0xB1, 0xE0, 0xAF, 0x81, 0xE0, 0xAE, 0xAE, 0xE0, 0xAF, 0x8D,
];
static S_25_18: [symbol; 9] = [0xE0, 0xAF, 0x86, 0xE0, 0xAE, 0xAE, 0xE0, 0xAF, 0x8D];
static S_25_19: [symbol; 9] = [0xE0, 0xAF, 0x87, 0xE0, 0xAE, 0xAE, 0xE0, 0xAF, 0x8D];
static S_25_20: [symbol; 9] = [0xE0, 0xAF, 0x8B, 0xE0, 0xAE, 0xAE, 0xE0, 0xAF, 0x8D];
static S_25_21: [symbol; 9] = [0xE0, 0xAE, 0xA9, 0xE0, 0xAE, 0xAE, 0xE0, 0xAF, 0x8D];
static S_25_22: [symbol; 9] = [0xE0, 0xAE, 0xAA, 0xE0, 0xAE, 0xAE, 0xE0, 0xAF, 0x8D];
static S_25_23: [symbol; 9] = [0xE0, 0xAE, 0xBE, 0xE0, 0xAE, 0xAE, 0xE0, 0xAF, 0x8D];
static S_25_24: [symbol; 9] = [0xE0, 0xAE, 0xBE, 0xE0, 0xAE, 0xAF, 0xE0, 0xAF, 0x8D];
static S_25_25: [symbol; 9] = [0xE0, 0xAF, 0x80, 0xE0, 0xAE, 0xB0, 0xE0, 0xAF, 0x8D];
static S_25_26: [symbol; 9] = [0xE0, 0xAE, 0xA9, 0xE0, 0xAE, 0xB0, 0xE0, 0xAF, 0x8D];
static S_25_27: [symbol; 9] = [0xE0, 0xAE, 0xAA, 0xE0, 0xAE, 0xB0, 0xE0, 0xAF, 0x8D];
static S_25_28: [symbol; 12] = [
    0xE0, 0xAF, 0x80, 0xE0, 0xAE, 0xAF, 0xE0, 0xAE, 0xB0, 0xE0, 0xAF, 0x8D,
];
static S_25_29: [symbol; 9] = [0xE0, 0xAE, 0xB5, 0xE0, 0xAE, 0xB0, 0xE0, 0xAF, 0x8D];
static S_25_30: [symbol; 9] = [0xE0, 0xAE, 0xBE, 0xE0, 0xAE, 0xB0, 0xE0, 0xAF, 0x8D];
static S_25_31: [symbol; 12] = [
    0xE0, 0xAE, 0xA9, 0xE0, 0xAE, 0xBE, 0xE0, 0xAE, 0xB0, 0xE0, 0xAF, 0x8D,
];
static S_25_32: [symbol; 12] = [
    0xE0, 0xAE, 0xAE, 0xE0, 0xAE, 0xBE, 0xE0, 0xAE, 0xB0, 0xE0, 0xAF, 0x8D,
];
static S_25_33: [symbol; 24] = [
    0xE0, 0xAE, 0x95, 0xE0, 0xAF, 0x8A, 0xE0, 0xAE, 0xA3, 0xE0, 0xAF, 0x8D, 0xE0, 0xAE, 0x9F,
    0xE0, 0xAE, 0xBF, 0xE0, 0xAE, 0xB0, 0xE0, 0xAF, 0x8D,
];
static S_25_34: [symbol; 12] = [
    0xE0, 0xAE, 0xA9, 0xE0, 0xAE, 0xBF, 0xE0, 0xAE, 0xB0, 0xE0, 0xAF, 0x8D,
];
static S_25_35: [symbol; 9] = [0xE0, 0xAE, 0xA9, 0xE0, 0xAE, 0xB3, 0xE0, 0xAF, 0x8D];
static S_25_36: [symbol; 9] = [0xE0, 0xAE, 0xAA, 0xE0, 0xAE, 0xB3, 0xE0, 0xAF, 0x8D];
static S_25_37: [symbol; 9] = [0xE0, 0xAE, 0xB5, 0xE0, 0xAE, 0xB3, 0xE0, 0xAF, 0x8D];
static S_25_38: [symbol; 9] = [0xE0, 0xAE, 0xBE, 0xE0, 0xAE, 0xB3, 0xE0, 0xAF, 0x8D];
static S_25_39: [symbol; 12] = [
    0xE0, 0xAE, 0xA9, 0xE0, 0xAE, 0xBE, 0xE0, 0xAE, 0xB3, 0xE0, 0xAF, 0x8D,
];
static S_25_40: [symbol; 3] = [0xE0, 0xAE, 0x95];
static S_25_41: [symbol; 3] = [0xE0, 0xAE, 0xA4];
static S_25_42: [symbol; 3] = [0xE0, 0xAE, 0xA9];
static S_25_43: [symbol; 3] = [0xE0, 0xAE, 0xAA];
static S_25_44: [symbol; 3] = [0xE0, 0xAE, 0xAF];
static S_25_45: [symbol; 3] = [0xE0, 0xAE, 0xBE];

static A_25: [among; 46] = [
    among { s_size: 6, s: S_25_0.as_ptr(), substring_i: -1, result: 6, function: None },
    among { s_size: 9, s: S_25_1.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_25_2.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 15, s: S_25_3.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_25_4.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_25_5.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 12, s: S_25_6.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 9, s: S_25_7.as_ptr(), substring_i: -1, result: 5, function: None },
    among { s_size: 9, s: S_25_8.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 9, s: S_25_9.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 9, s: S_25_10.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 9, s: S_25_11.as_ptr(), substring_i: -1, result: 4, function: None },
    among { s_size: 12, s: S_25_12.as_ptr(), substring_i: 11, result: 1, function: None },
    among { s_size: 12, s: S_25_13.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 12, s: S_25_14.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 12, s: S_25_15.as_ptr(), substring_i: -1, result: 5, function: None },
    among { s_size: 12, s: S_25_16.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 12, s: S_25_17.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 9, s: S_25_18.as_ptr(), substring_i: -1, result: 5, function: None },
    among { s_size: 9, s: S_25_19.as_ptr(), substring_i: -1, result: 5, function: None },
    among { s_size: 9, s: S_25_20.as_ptr(), substring_i: -1, result: 5, function: None },
    among { s_size: 9, s: S_25_21.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 9, s: S_25_22.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 9, s: S_25_23.as_ptr(), substring_i: -1, result: 5, function: None },
    among { s_size: 9, s: S_25_24.as_ptr(), substring_i: -1, result: 5, function: None },
    among { s_size: 9, s: S_25_25.as_ptr(), substring_i: -1, result: 5, function: None },
    among { s_size: 9, s: S_25_26.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 9, s: S_25_27.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 12, s: S_25_28.as_ptr(), substring_i: -1, result: 5, function: None },
    among { s_size: 9, s: S_25_29.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 9, s: S_25_30.as_ptr(), substring_i: -1, result: 5, function: None },
    among { s_size: 12, s: S_25_31.as_ptr(), substring_i: 30, result: 1, function: None },
    among { s_size: 12, s: S_25_32.as_ptr(), substring_i: 30, result: 1, function: None },
    among { s_size: 24, s: S_25_33.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 12, s: S_25_34.as_ptr(), substring_i: -1, result: 5, function: None },
    among { s_size: 9, s: S_25_35.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 9, s: S_25_36.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 9, s: S_25_37.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 9, s: S_25_38.as_ptr(), substring_i: -1, result: 5, function: None },
    among { s_size: 12, s: S_25_39.as_ptr(), substring_i: 38, result: 1, function: None },
    among { s_size: 3, s: S_25_40.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_25_41.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_25_42.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_25_43.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_25_44.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_25_45.as_ptr(), substring_i: -1, result: 5, function: None },
];

static S_26_0: [symbol; 18] = [
    0xE0, 0xAE, 0x95, 0xE0, 0xAE, 0xBF, 0xE0, 0xAE, 0xA9, 0xE0, 0xAF, 0x8D, 0xE0, 0xAE, 0xB1,
    0xE0, 0xAF, 0x8D,
];
static S_26_1: [symbol; 21] = [
    0xE0, 0xAE, 0xBE, 0xE0, 0xAE, 0xA8, 0xE0, 0xAE, 0xBF, 0xE0, 0xAE, 0xA9, 0xE0, 0xAF, 0x8D,
    0xE0, 0xAE, 0xB1, 0xE0, 0xAF, 0x8D,
];
static S_26_2: [symbol; 12] = [
    0xE0, 0xAE, 0x95, 0xE0, 0xAE, 0xBF, 0xE0, 0xAE, 0xB1, 0xE0, 0xAF, 0x8D,
];
static S_26_3: [symbol; 15] = [
    0xE0, 0xAE, 0x95, 0xE0, 0xAE, 0xBF, 0xE0, 0xAE, 0xA9, 0xE0, 0xAF, 0x8D, 0xE0, 0xAE, 0xB1,
];
static S_26_4: [symbol; 18] = [
    0xE0, 0xAE, 0xBE, 0xE0, 0xAE, 0xA8, 0xE0, 0xAE, 0xBF, 0xE0, 0xAE, 0xA9, 0xE0, 0xAF, 0x8D,
    0xE0, 0xAE, 0xB1,
];
static S_26_5: [symbol; 9] = [0xE0, 0xAE, 0x95, 0xE0, 0xAE, 0xBF, 0xE0, 0xAE, 0xB1];

static A_26: [among; 6] = [
    among { s_size: 18, s: S_26_0.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 21, s: S_26_1.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 12, s: S_26_2.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 15, s: S_26_3.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 18, s: S_26_4.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 9, s: S_26_5.as_ptr(), substring_i: -1, result: -1, function: None },
];

// ---------------------------------------------------------------------------
// literal strings used by slice_from_s / eq_s / eq_s_b
// ---------------------------------------------------------------------------

static S_0: [symbol; 3] = [0xE0, 0xAE, 0x93];
static S_1: [symbol; 3] = [0xE0, 0xAE, 0x92];
static S_2: [symbol; 3] = [0xE0, 0xAE, 0x89];
static S_3: [symbol; 3] = [0xE0, 0xAE, 0x8A];
static S_4: [symbol; 3] = [0xE0, 0xAE, 0x8E];
static S_5: [symbol; 3] = [0xE0, 0xAF, 0x8D];
static S_6: [symbol; 6] = [0xE0, 0xAE, 0xB3, 0xE0, 0xAF, 0x8D];
static S_7: [symbol; 6] = [0xE0, 0xAE, 0xB2, 0xE0, 0xAF, 0x8D];
static S_8: [symbol; 6] = [0xE0, 0xAE, 0x9F, 0xE0, 0xAF, 0x81];
static S_9: [symbol; 3] = [0xE0, 0xAF, 0x88];
static S_10: [symbol; 6] = [0xE0, 0xAE, 0xAE, 0xE0, 0xAF, 0x8D];
static S_11: [symbol; 3] = [0xE0, 0xAF, 0x8D];
static S_12: [symbol; 6] = [0xE0, 0xAE, 0xAE, 0xE0, 0xAF, 0x8D];
static S_13: [symbol; 3] = [0xE0, 0xAF, 0x8D];
static S_14: [symbol; 3] = [0xE0, 0xAF, 0x8D];
static S_15: [symbol; 3] = [0xE0, 0xAF, 0x8D];
static S_16: [symbol; 3] = [0xE0, 0xAF, 0x8D];
static S_17: [symbol; 9] = [0xE0, 0xAF, 0x81, 0xE0, 0xAE, 0x99, 0xE0, 0xAF, 0x8D];
static S_18: [symbol; 3] = [0xE0, 0xAF, 0x8D];
static S_19: [symbol; 6] = [0xE0, 0xAE, 0xB2, 0xE0, 0xAF, 0x8D];
static S_20: [symbol; 6] = [0xE0, 0xAE, 0xB3, 0xE0, 0xAF, 0x8D];
static S_21: [symbol; 3] = [0xE0, 0xAF, 0x8D];
static S_22: [symbol; 9] = [0xE0, 0xAF, 0x81, 0xE0, 0xAE, 0xAE, 0xE0, 0xAF, 0x8D];
static S_23: [symbol; 3] = [0xE0, 0xAF, 0x8D];
static S_24: [symbol; 3] = [0xE0, 0xAF, 0x8D];
static S_25: [symbol; 3] = [0xE0, 0xAF, 0x8D];
static S_26: [symbol; 3] = [0xE0, 0xAF, 0x8D];
static S_27: [symbol; 3] = [0xE0, 0xAE, 0xAE];
static S_28: [symbol; 3] = [0xE0, 0xAF, 0x8D];
static S_29: [symbol; 3] = [0xE0, 0xAF, 0x8D];
static S_30: [symbol; 3] = [0xE0, 0xAF, 0x8D];
static S_31: [symbol; 3] = [0xE0, 0xAE, 0xBF];
static S_32: [symbol; 3] = [0xE0, 0xAF, 0x88];
static S_33: [symbol; 3] = [0xE0, 0xAF, 0x8D];
static S_34: [symbol; 3] = [0xE0, 0xAF, 0x8D];
static S_35: [symbol; 9] = [0xE0, 0xAE, 0xBF, 0xE0, 0xAE, 0xA9, 0xE0, 0xAF, 0x8D];
static S_36: [symbol; 3] = [0xE0, 0xAF, 0x8D];
static S_37: [symbol; 3] = [0xE0, 0xAE, 0x9A];
static S_38: [symbol; 3] = [0xE0, 0xAF, 0x8D];
static S_39: [symbol; 3] = [0xE0, 0xAF, 0x8D];
static S_40: [symbol; 3] = [0xE0, 0xAF, 0x8D];

// ---------------------------------------------------------------------------
// rules
// ---------------------------------------------------------------------------

unsafe fn r_has_min_length(z: *mut SN_env) -> c_int {
    (len_utf8((*z).p) > 4) as c_int
}

unsafe fn r_fix_va_start(z: *mut SN_env) -> c_int {
    let among_var;
    (*z).bra = (*z).c;
    if (*z).c + 5 >= (*z).l
        || *(*z).p.offset(((*z).c + 5) as isize) as c_int >> 5 != 4
        || (3078 >> (*(*z).p.offset(((*z).c + 5) as isize) as c_int & 0x1f)) & 1 == 0
    {
        return 0;
    }
    among_var = find_among(z, A_0.as_ptr(), 4);
    if among_var == 0 {
        return 0;
    }
    (*z).ket = (*z).c;
    match among_var {
        1 => {
            let ret = slice_from_s(z, 3, S_0.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        2 => {
            let ret = slice_from_s(z, 3, S_1.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        3 => {
            let ret = slice_from_s(z, 3, S_2.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        4 => {
            let ret = slice_from_s(z, 3, S_3.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        _ => {}
    }
    1
}

unsafe fn r_fix_endings(z: *mut SN_env) -> c_int {
    {
        let c1 = (*z).c;
        'ztam5: loop {
            let c2 = (*z).c;
            'lab1: {
                {
                    let ret = r_fix_ending(z);
                    if ret == 0 {
                        break 'lab1;
                    }
                    if ret < 0 {
                        return ret;
                    }
                }
                continue 'ztam5;
            }
            (*z).c = c2;
            break;
        }
        (*z).c = c1;
    }
    1
}

unsafe fn r_remove_question_prefixes(z: *mut SN_env) -> c_int {
    (*z).bra = (*z).c;
    if eq_s(z, 3, S_4.as_ptr()) == 0 {
        return 0;
    }
    if find_among(z, A_1.as_ptr(), 10) == 0 {
        return 0;
    }
    if eq_s(z, 3, S_5.as_ptr()) == 0 {
        return 0;
    }
    (*z).ket = (*z).c;
    {
        let ret = slice_del(z);
        if ret < 0 {
            return ret;
        }
    }
    {
        let c1 = (*z).c;
        {
            let ret = r_fix_va_start(z);
            if ret < 0 {
                return ret;
            }
        }
        (*z).c = c1;
    }
    1
}

unsafe fn r_fix_ending(z: *mut SN_env) -> c_int {
    let mut among_var;
    if len_utf8((*z).p) <= 3 {
        return 0;
    }
    (*z).lb = (*z).c;
    (*z).c = (*z).l;

    {
        let m1 = (*z).l - (*z).c;
        'lab0: {
            'lab1: {
                (*z).ket = (*z).c;
                among_var = find_among_b(z, A_5.as_ptr(), 17);
                if among_var == 0 {
                    break 'lab1;
                }
                (*z).bra = (*z).c;
                match among_var {
                    1 => {
                        let ret = slice_del(z);
                        if ret < 0 {
                            return ret;
                        }
                    }
                    2 => {
                        {
                            let m_test2 = (*z).l - (*z).c;
                            if find_among_b(z, A_2.as_ptr(), 3) == 0 {
                                break 'lab1;
                            }
                            (*z).c = (*z).l - m_test2;
                        }
                        {
                            let ret = slice_del(z);
                            if ret < 0 {
                                return ret;
                            }
                        }
                    }
                    3 => {
                        let ret = slice_from_s(z, 6, S_6.as_ptr());
                        if ret < 0 {
                            return ret;
                        }
                    }
                    4 => {
                        let ret = slice_from_s(z, 6, S_7.as_ptr());
                        if ret < 0 {
                            return ret;
                        }
                    }
                    5 => {
                        let ret = slice_from_s(z, 6, S_8.as_ptr());
                        if ret < 0 {
                            return ret;
                        }
                    }
                    6 => {
                        if *(*z).I.offset(0) == 0 {
                            break 'lab1;
                        }
                        {
                            let m3 = (*z).l - (*z).c;
                            'lab2: {
                                if eq_s_b(z, 3, S_9.as_ptr()) == 0 {
                                    break 'lab2;
                                }
                                break 'lab1;
                            }
                            (*z).c = (*z).l - m3;
                        }
                        {
                            let ret = slice_from_s(z, 6, S_10.as_ptr());
                            if ret < 0 {
                                return ret;
                            }
                        }
                    }
                    7 => {
                        let ret = slice_from_s(z, 3, S_11.as_ptr());
                        if ret < 0 {
                            return ret;
                        }
                    }
                    8 => {
                        {
                            let m4 = (*z).l - (*z).c;
                            'lab3: {
                                if find_among_b(z, A_3.as_ptr(), 8) == 0 {
                                    break 'lab3;
                                }
                                break 'lab1;
                            }
                            (*z).c = (*z).l - m4;
                        }
                        {
                            let ret = slice_del(z);
                            if ret < 0 {
                                return ret;
                            }
                        }
                    }
                    9 => {
                        if (*z).c - 2 <= (*z).lb
                            || (*(*z).p.offset(((*z).c - 1) as isize) != 136
                                && *(*z).p.offset(((*z).c - 1) as isize) != 141)
                        {
                            among_var = 2;
                        } else {
                            among_var = find_among_b(z, A_4.as_ptr(), 3);
                        }
                        match among_var {
                            1 => {
                                let ret = slice_del(z);
                                if ret < 0 {
                                    return ret;
                                }
                            }
                            2 => {
                                let ret = slice_from_s(z, 6, S_12.as_ptr());
                                if ret < 0 {
                                    return ret;
                                }
                            }
                            _ => {}
                        }
                    }
                    _ => {}
                }
                break 'lab0;
            }
            (*z).c = (*z).l - m1;
            (*z).ket = (*z).c;
            if eq_s_b(z, 3, S_13.as_ptr()) == 0 {
                return 0;
            }
            {
                let m5 = (*z).l - (*z).c;
                'lab4: {
                    'lab5: {
                        if find_among_b(z, A_6.as_ptr(), 6) == 0 {
                            break 'lab5;
                        }
                        {
                            let m6 = (*z).l - (*z).c;
                            'lab6: {
                                if eq_s_b(z, 3, S_14.as_ptr()) == 0 {
                                    (*z).c = (*z).l - m6;
                                    break 'lab6;
                                }
                                if find_among_b(z, A_7.as_ptr(), 6) == 0 {
                                    (*z).c = (*z).l - m6;
                                    break 'lab6;
                                }
                            }
                        }
                        (*z).bra = (*z).c;
                        {
                            let ret = slice_del(z);
                            if ret < 0 {
                                return ret;
                            }
                        }
                        break 'lab4;
                    }
                    (*z).c = (*z).l - m5;
                    'lab7: {
                        if find_among_b(z, A_8.as_ptr(), 11) == 0 {
                            break 'lab7;
                        }
                        (*z).bra = (*z).c;
                        if eq_s_b(z, 3, S_15.as_ptr()) == 0 {
                            break 'lab7;
                        }
                        {
                            let ret = slice_del(z);
                            if ret < 0 {
                                return ret;
                            }
                        }
                        break 'lab4;
                    }
                    (*z).c = (*z).l - m5;
                    {
                        let m_test7 = (*z).l - (*z).c;
                        if find_among_b(z, A_9.as_ptr(), 9) == 0 {
                            return 0;
                        }
                        (*z).c = (*z).l - m_test7;
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
        }
    }
    (*z).c = (*z).lb;
    1
}

unsafe fn r_remove_pronoun_prefixes(z: *mut SN_env) -> c_int {
    (*z).bra = (*z).c;
    if (*z).c + 2 >= (*z).l
        || *(*z).p.offset(((*z).c + 2) as isize) as c_int >> 5 != 4
        || (672 >> (*(*z).p.offset(((*z).c + 2) as isize) as c_int & 0x1f)) & 1 == 0
    {
        return 0;
    }
    if find_among(z, A_10.as_ptr(), 3) == 0 {
        return 0;
    }
    if find_among(z, A_11.as_ptr(), 10) == 0 {
        return 0;
    }
    if eq_s(z, 3, S_16.as_ptr()) == 0 {
        return 0;
    }
    (*z).ket = (*z).c;
    {
        let ret = slice_del(z);
        if ret < 0 {
            return ret;
        }
    }
    {
        let c1 = (*z).c;
        {
            let ret = r_fix_va_start(z);
            if ret < 0 {
                return ret;
            }
        }
        (*z).c = c1;
    }
    1
}

unsafe fn r_remove_plural_suffix(z: *mut SN_env) -> c_int {
    let among_var;
    (*z).lb = (*z).c;
    (*z).c = (*z).l;

    (*z).ket = (*z).c;
    if (*z).c - 8 <= (*z).lb || *(*z).p.offset(((*z).c - 1) as isize) != 141 {
        return 0;
    }
    among_var = find_among_b(z, A_13.as_ptr(), 4);
    if among_var == 0 {
        return 0;
    }
    (*z).bra = (*z).c;
    match among_var {
        1 => {
            'lab0: {
                let m1 = (*z).l - (*z).c;
                'lab1: {
                    if find_among_b(z, A_12.as_ptr(), 6) == 0 {
                        break 'lab1;
                    }
                    {
                        let ret = slice_from_s(z, 9, S_17.as_ptr());
                        if ret < 0 {
                            return ret;
                        }
                    }
                    break 'lab0;
                }
                (*z).c = (*z).l - m1;
                {
                    let ret = slice_from_s(z, 3, S_18.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
            }
        }
        2 => {
            let ret = slice_from_s(z, 6, S_19.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        3 => {
            let ret = slice_from_s(z, 6, S_20.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        4 => {
            let ret = slice_del(z);
            if ret < 0 {
                return ret;
            }
        }
        _ => {}
    }
    (*z).c = (*z).lb;
    1
}

unsafe fn r_remove_question_suffixes(z: *mut SN_env) -> c_int {
    {
        let ret = r_has_min_length(z);
        if ret <= 0 {
            return ret;
        }
    }
    (*z).lb = (*z).c;
    (*z).c = (*z).l;

    {
        let m1 = (*z).l - (*z).c;
        'lab0: {
            (*z).ket = (*z).c;
            if find_among_b(z, A_14.as_ptr(), 3) == 0 {
                break 'lab0;
            }
            (*z).bra = (*z).c;
            {
                let ret = slice_from_s(z, 3, S_21.as_ptr());
                if ret < 0 {
                    return ret;
                }
            }
        }
        (*z).c = (*z).l - m1;
    }
    (*z).c = (*z).lb;

    {
        let ret = r_fix_endings(z);
        if ret < 0 {
            return ret;
        }
    }
    1
}

unsafe fn r_remove_command_suffixes(z: *mut SN_env) -> c_int {
    {
        let ret = r_has_min_length(z);
        if ret <= 0 {
            return ret;
        }
    }
    (*z).lb = (*z).c;
    (*z).c = (*z).l;

    (*z).ket = (*z).c;
    if (*z).c - 5 <= (*z).lb || *(*z).p.offset(((*z).c - 1) as isize) != 191 {
        return 0;
    }
    if find_among_b(z, A_15.as_ptr(), 2) == 0 {
        return 0;
    }
    (*z).bra = (*z).c;
    {
        let ret = slice_del(z);
        if ret < 0 {
            return ret;
        }
    }
    (*z).c = (*z).lb;
    1
}

unsafe fn r_remove_um(z: *mut SN_env) -> c_int {
    {
        let ret = r_has_min_length(z);
        if ret <= 0 {
            return ret;
        }
    }
    (*z).lb = (*z).c;
    (*z).c = (*z).l;

    (*z).ket = (*z).c;
    if eq_s_b(z, 9, S_22.as_ptr()) == 0 {
        return 0;
    }
    (*z).bra = (*z).c;
    {
        let ret = slice_from_s(z, 3, S_23.as_ptr());
        if ret < 0 {
            return ret;
        }
    }
    (*z).c = (*z).lb;
    {
        let c1 = (*z).c;
        {
            let ret = r_fix_ending(z);
            if ret < 0 {
                return ret;
            }
        }
        (*z).c = c1;
    }
    1
}

unsafe fn r_remove_common_word_endings(z: *mut SN_env) -> c_int {
    let among_var;
    {
        let ret = r_has_min_length(z);
        if ret <= 0 {
            return ret;
        }
    }
    (*z).lb = (*z).c;
    (*z).c = (*z).l;

    (*z).ket = (*z).c;
    among_var = find_among_b(z, A_17.as_ptr(), 26);
    if among_var == 0 {
        return 0;
    }
    (*z).bra = (*z).c;
    match among_var {
        1 => {
            let ret = slice_from_s(z, 3, S_24.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        2 => {
            {
                let m1 = (*z).l - (*z).c;
                'lab0: {
                    if find_among_b(z, A_16.as_ptr(), 8) == 0 {
                        break 'lab0;
                    }
                    return 0;
                }
                (*z).c = (*z).l - m1;
            }
            {
                let ret = slice_from_s(z, 3, S_25.as_ptr());
                if ret < 0 {
                    return ret;
                }
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
    (*z).c = (*z).lb;

    {
        let ret = r_fix_endings(z);
        if ret < 0 {
            return ret;
        }
    }
    1
}

unsafe fn r_remove_vetrumai_urupukal(z: *mut SN_env) -> c_int {
    let among_var;
    *(*z).I.offset(0) = 0;
    {
        let ret = r_has_min_length(z);
        if ret <= 0 {
            return ret;
        }
    }
    (*z).lb = (*z).c;
    (*z).c = (*z).l;

    {
        let m1 = (*z).l - (*z).c;
        'lab0: {
            'lab1: {
                let m_test2 = (*z).l - (*z).c;
                (*z).ket = (*z).c;
                if (*z).c - 2 <= (*z).lb
                    || *(*z).p.offset(((*z).c - 1) as isize) as c_int >> 5 != 4
                    || (-2147475197i32 >> (*(*z).p.offset(((*z).c - 1) as isize) as c_int & 0x1f)) & 1
                        == 0
                {
                    break 'lab1;
                }
                among_var = find_among_b(z, A_20.as_ptr(), 22);
                if among_var == 0 {
                    break 'lab1;
                }
                (*z).bra = (*z).c;
                match among_var {
                    1 => {
                        let ret = slice_del(z);
                        if ret < 0 {
                            return ret;
                        }
                    }
                    2 => {
                        let ret = slice_from_s(z, 3, S_26.as_ptr());
                        if ret < 0 {
                            return ret;
                        }
                    }
                    3 => {
                        {
                            let m3 = (*z).l - (*z).c;
                            'lab2: {
                                if eq_s_b(z, 3, S_27.as_ptr()) == 0 {
                                    break 'lab2;
                                }
                                break 'lab1;
                            }
                            (*z).c = (*z).l - m3;
                        }
                        {
                            let ret = slice_from_s(z, 3, S_28.as_ptr());
                            if ret < 0 {
                                return ret;
                            }
                        }
                    }
                    4 => {
                        if len_utf8((*z).p) < 7 {
                            break 'lab1;
                        }
                        {
                            let ret = slice_from_s(z, 3, S_29.as_ptr());
                            if ret < 0 {
                                return ret;
                            }
                        }
                    }
                    5 => {
                        {
                            let m4 = (*z).l - (*z).c;
                            'lab3: {
                                if find_among_b(z, A_18.as_ptr(), 8) == 0 {
                                    break 'lab3;
                                }
                                break 'lab1;
                            }
                            (*z).c = (*z).l - m4;
                        }
                        {
                            let ret = slice_from_s(z, 3, S_30.as_ptr());
                            if ret < 0 {
                                return ret;
                            }
                        }
                    }
                    6 => {
                        {
                            let m5 = (*z).l - (*z).c;
                            'lab4: {
                                if find_among_b(z, A_19.as_ptr(), 8) == 0 {
                                    break 'lab4;
                                }
                                break 'lab1;
                            }
                            (*z).c = (*z).l - m5;
                        }
                        {
                            let ret = slice_del(z);
                            if ret < 0 {
                                return ret;
                            }
                        }
                    }
                    7 => {
                        let ret = slice_from_s(z, 3, S_31.as_ptr());
                        if ret < 0 {
                            return ret;
                        }
                    }
                    _ => {}
                }
                (*z).c = (*z).l - m_test2;
                break 'lab0;
            }
            (*z).c = (*z).l - m1;
            {
                let m_test6 = (*z).l - (*z).c;
                (*z).ket = (*z).c;
                if eq_s_b(z, 3, S_32.as_ptr()) == 0 {
                    return 0;
                }
                {
                    let m7 = (*z).l - (*z).c;
                    'lab5: {
                        'lab6: {
                            {
                                let m8 = (*z).l - (*z).c;
                                'lab7: {
                                    if find_among_b(z, A_21.as_ptr(), 6) == 0 {
                                        break 'lab7;
                                    }
                                    break 'lab6;
                                }
                                (*z).c = (*z).l - m8;
                            }
                            break 'lab5;
                        }
                        (*z).c = (*z).l - m7;
                        {
                            let m_test9 = (*z).l - (*z).c;
                            if find_among_b(z, A_22.as_ptr(), 6) == 0 {
                                return 0;
                            }
                            if eq_s_b(z, 3, S_33.as_ptr()) == 0 {
                                return 0;
                            }
                            (*z).c = (*z).l - m_test9;
                        }
                    }
                }
                (*z).bra = (*z).c;
                {
                    let ret = slice_from_s(z, 3, S_34.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
                (*z).c = (*z).l - m_test6;
            }
        }
    }
    *(*z).I.offset(0) = 1;
    {
        let m10 = (*z).l - (*z).c;
        'lab8: {
            (*z).ket = (*z).c;
            if eq_s_b(z, 9, S_35.as_ptr()) == 0 {
                break 'lab8;
            }
            (*z).bra = (*z).c;
            {
                let ret = slice_from_s(z, 3, S_36.as_ptr());
                if ret < 0 {
                    return ret;
                }
            }
        }
        (*z).c = (*z).l - m10;
    }
    (*z).c = (*z).lb;

    {
        let ret = r_fix_endings(z);
        if ret < 0 {
            return ret;
        }
    }
    1
}

unsafe fn r_remove_tense_suffixes(z: *mut SN_env) -> c_int {
    *(*z).I.offset(1) = 1;
    'ztam6: loop {
        let c1 = (*z).c;
        'lab0: {
            if *(*z).I.offset(1) == 0 {
                break 'lab0;
            }
            {
                let c2 = (*z).c;
                {
                    let ret = r_remove_tense_suffix(z);
                    if ret < 0 {
                        return ret;
                    }
                }
                (*z).c = c2;
            }
            continue 'ztam6;
        }
        (*z).c = c1;
        break;
    }
    1
}

unsafe fn r_remove_tense_suffix(z: *mut SN_env) -> c_int {
    let among_var;
    *(*z).I.offset(1) = 0;
    {
        let ret = r_has_min_length(z);
        if ret <= 0 {
            return ret;
        }
    }
    (*z).lb = (*z).c;
    (*z).c = (*z).l;

    {
        let m1 = (*z).l - (*z).c;
        'lab0: {
            let m_test2 = (*z).l - (*z).c;
            (*z).ket = (*z).c;
            among_var = find_among_b(z, A_25.as_ptr(), 46);
            if among_var == 0 {
                break 'lab0;
            }
            (*z).bra = (*z).c;
            match among_var {
                1 => {
                    let ret = slice_del(z);
                    if ret < 0 {
                        return ret;
                    }
                }
                2 => {
                    {
                        let m3 = (*z).l - (*z).c;
                        'lab1: {
                            if (*z).c - 2 <= (*z).lb
                                || *(*z).p.offset(((*z).c - 1) as isize) as c_int >> 5 != 4
                                || (1951712 >> (*(*z).p.offset(((*z).c - 1) as isize) as c_int & 0x1f)) & 1
                                    == 0
                            {
                                break 'lab1;
                            }
                            if find_among_b(z, A_23.as_ptr(), 12) == 0 {
                                break 'lab1;
                            }
                            break 'lab0;
                        }
                        (*z).c = (*z).l - m3;
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
                        let m4 = (*z).l - (*z).c;
                        'lab2: {
                            if find_among_b(z, A_24.as_ptr(), 8) == 0 {
                                break 'lab2;
                            }
                            break 'lab0;
                        }
                        (*z).c = (*z).l - m4;
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
                        let m5 = (*z).l - (*z).c;
                        'lab3: {
                            if eq_s_b(z, 3, S_37.as_ptr()) == 0 {
                                break 'lab3;
                            }
                            break 'lab0;
                        }
                        (*z).c = (*z).l - m5;
                    }
                    {
                        let ret = slice_from_s(z, 3, S_38.as_ptr());
                        if ret < 0 {
                            return ret;
                        }
                    }
                }
                5 => {
                    let ret = slice_from_s(z, 3, S_39.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
                6 => {
                    {
                        let m_test6 = (*z).l - (*z).c;
                        if eq_s_b(z, 3, S_40.as_ptr()) == 0 {
                            break 'lab0;
                        }
                        (*z).c = (*z).l - m_test6;
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
            *(*z).I.offset(1) = 1;
            (*z).c = (*z).l - m_test2;
        }
        (*z).c = (*z).l - m1;
    }
    {
        let m7 = (*z).l - (*z).c;
        'lab4: {
            (*z).ket = (*z).c;
            if (*z).c - 8 <= (*z).lb
                || (*(*z).p.offset(((*z).c - 1) as isize) != 141
                    && *(*z).p.offset(((*z).c - 1) as isize) != 177)
            {
                break 'lab4;
            }
            if find_among_b(z, A_26.as_ptr(), 6) == 0 {
                break 'lab4;
            }
            (*z).bra = (*z).c;
            {
                let ret = slice_del(z);
                if ret < 0 {
                    return ret;
                }
            }
            *(*z).I.offset(1) = 1;
        }
        (*z).c = (*z).l - m7;
    }
    (*z).c = (*z).lb;

    {
        let ret = r_fix_endings(z);
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
pub unsafe extern "C" fn tamil_UTF_8_stem(z: *mut SN_env) -> c_int {
    *(*z).I.offset(0) = 0;
    {
        let c1 = (*z).c;
        {
            let ret = r_fix_ending(z);
            if ret < 0 {
                return ret;
            }
        }
        (*z).c = c1;
    }
    {
        let ret = r_has_min_length(z);
        if ret <= 0 {
            return ret;
        }
    }
    {
        let c2 = (*z).c;
        {
            let ret = r_remove_question_prefixes(z);
            if ret < 0 {
                return ret;
            }
        }
        (*z).c = c2;
    }
    {
        let c3 = (*z).c;
        {
            let ret = r_remove_pronoun_prefixes(z);
            if ret < 0 {
                return ret;
            }
        }
        (*z).c = c3;
    }
    {
        let c4 = (*z).c;
        {
            let ret = r_remove_question_suffixes(z);
            if ret < 0 {
                return ret;
            }
        }
        (*z).c = c4;
    }
    {
        let c5 = (*z).c;
        {
            let ret = r_remove_um(z);
            if ret < 0 {
                return ret;
            }
        }
        (*z).c = c5;
    }
    {
        let c6 = (*z).c;
        {
            let ret = r_remove_common_word_endings(z);
            if ret < 0 {
                return ret;
            }
        }
        (*z).c = c6;
    }
    {
        let c7 = (*z).c;
        {
            let ret = r_remove_vetrumai_urupukal(z);
            if ret < 0 {
                return ret;
            }
        }
        (*z).c = c7;
    }
    {
        let c8 = (*z).c;
        {
            let ret = r_remove_plural_suffix(z);
            if ret < 0 {
                return ret;
            }
        }
        (*z).c = c8;
    }
    {
        let c9 = (*z).c;
        {
            let ret = r_remove_command_suffixes(z);
            if ret < 0 {
                return ret;
            }
        }
        (*z).c = c9;
    }
    {
        let c10 = (*z).c;
        {
            let ret = r_remove_tense_suffixes(z);
            if ret < 0 {
                return ret;
            }
        }
        (*z).c = c10;
    }
    1
}

#[no_mangle]
pub unsafe extern "C" fn tamil_UTF_8_create_env() -> *mut SN_env {
    SN_create_env(0, 2)
}

#[no_mangle]
pub unsafe extern "C" fn tamil_UTF_8_close_env(z: *mut SN_env) {
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
        let z = tamil_UTF_8_create_env();
        assert!(!z.is_null());
        let rc = SN_set_current(z, word.len() as c_int, word.as_ptr());
        assert!(rc >= 0);
        let rc = tamil_UTF_8_stem(z);
        assert!(rc >= 0, "stem returned {rc}");
        let out = std::slice::from_raw_parts((*z).p, SIZE((*z).p) as usize).to_vec();
        tamil_UTF_8_close_env(z);
        out
    }

    #[test]
    fn idempotent() {
        unsafe {
            for w in [
                "\u{baa}\u{bc1}\u{ba4}\u{bcd}\u{ba4}\u{b95}\u{bae}\u{bcd}".as_bytes(),
                "\u{bb5}\u{bc0}\u{b9f}\u{bc1}\u{b95}\u{bb3}\u{bcd}".as_bytes(),
            ] {
                let once = stem(w);
                let twice = stem(&once);
                assert_eq!(once, twice, "not idempotent for {:?}", w);
            }
        }
    }
}
