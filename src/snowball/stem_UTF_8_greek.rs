//! Greek Snowball stemmer (UTF-8).
//!
//! 1:1 translation of the generated
//! `src/backend/snowball/libstemmer/stem_UTF_8_greek.c` (Snowball 2.2.0),
//! merged with its header declarations. The runtime helpers come from
//! `crate::snowball::api` and `crate::snowball::utilities`.

use crate::prelude::*;

use crate::snowball::api::{among, symbol, SN_close_env, SN_create_env, SN_env, SIZE};
use crate::snowball::utilities::{
    eq_s_b, find_among_b, in_grouping_b_U, insert_s, len_utf8, skip_b_utf8, slice_del, slice_from_s,
};

// ---------------------------------------------------------------------------
// among string tables, grouping bit tables, and literal strings
// ---------------------------------------------------------------------------

static S_0_1: [symbol; 2] = [0xCF, 0x82];

static S_0_2: [symbol; 2] = [0xCE, 0x86];

static S_0_3: [symbol; 2] = [0xCE, 0x88];

static S_0_4: [symbol; 2] = [0xCE, 0x89];

static S_0_5: [symbol; 2] = [0xCE, 0x8A];

static S_0_6: [symbol; 2] = [0xCF, 0x8A];

static S_0_7: [symbol; 2] = [0xCF, 0x8B];

static S_0_8: [symbol; 2] = [0xCE, 0x8C];

static S_0_9: [symbol; 2] = [0xCF, 0x8C];

static S_0_10: [symbol; 2] = [0xCF, 0x8D];

static S_0_11: [symbol; 2] = [0xCE, 0x8E];

static S_0_12: [symbol; 2] = [0xCF, 0x8E];

static S_0_13: [symbol; 2] = [0xCE, 0x8F];

static S_0_14: [symbol; 2] = [0xCE, 0x90];

static S_0_15: [symbol; 2] = [0xCE, 0x91];

static S_0_16: [symbol; 2] = [0xCE, 0x92];

static S_0_17: [symbol; 2] = [0xCE, 0x93];

static S_0_18: [symbol; 2] = [0xCE, 0x94];

static S_0_19: [symbol; 2] = [0xCE, 0x95];

static S_0_20: [symbol; 2] = [0xCE, 0x96];

static S_0_21: [symbol; 2] = [0xCE, 0x97];

static S_0_22: [symbol; 2] = [0xCE, 0x98];

static S_0_23: [symbol; 2] = [0xCE, 0x99];

static S_0_24: [symbol; 2] = [0xCE, 0x9A];

static S_0_25: [symbol; 2] = [0xCE, 0x9B];

static S_0_26: [symbol; 2] = [0xCE, 0x9C];

static S_0_27: [symbol; 2] = [0xCE, 0x9D];

static S_0_28: [symbol; 2] = [0xCE, 0x9E];

static S_0_29: [symbol; 2] = [0xCE, 0x9F];

static S_0_30: [symbol; 2] = [0xCE, 0xA0];

static S_0_31: [symbol; 2] = [0xCE, 0xA1];

static S_0_32: [symbol; 2] = [0xCE, 0xA3];

static S_0_33: [symbol; 2] = [0xCE, 0xA4];

static S_0_34: [symbol; 2] = [0xCE, 0xA5];

static S_0_35: [symbol; 2] = [0xCE, 0xA6];

static S_0_36: [symbol; 2] = [0xCE, 0xA7];

static S_0_37: [symbol; 2] = [0xCE, 0xA8];

static S_0_38: [symbol; 2] = [0xCE, 0xA9];

static S_0_39: [symbol; 2] = [0xCE, 0xAA];

static S_0_40: [symbol; 2] = [0xCE, 0xAB];

static S_0_41: [symbol; 2] = [0xCE, 0xAC];

static S_0_42: [symbol; 2] = [0xCE, 0xAD];

static S_0_43: [symbol; 2] = [0xCE, 0xAE];

static S_0_44: [symbol; 2] = [0xCE, 0xAF];

static S_0_45: [symbol; 2] = [0xCE, 0xB0];

static A_0: [among; 46] = [
    among { s_size: 0, s: core::ptr::null(), substring_i: -1, result: 25, function: None },
    among { s_size: 2, s: S_0_1.as_ptr(), substring_i: 0, result: 18, function: None },
    among { s_size: 2, s: S_0_2.as_ptr(), substring_i: 0, result: 1, function: None },
    among { s_size: 2, s: S_0_3.as_ptr(), substring_i: 0, result: 5, function: None },
    among { s_size: 2, s: S_0_4.as_ptr(), substring_i: 0, result: 7, function: None },
    among { s_size: 2, s: S_0_5.as_ptr(), substring_i: 0, result: 9, function: None },
    among { s_size: 2, s: S_0_6.as_ptr(), substring_i: 0, result: 7, function: None },
    among { s_size: 2, s: S_0_7.as_ptr(), substring_i: 0, result: 20, function: None },
    among { s_size: 2, s: S_0_8.as_ptr(), substring_i: 0, result: 15, function: None },
    among { s_size: 2, s: S_0_9.as_ptr(), substring_i: 0, result: 15, function: None },
    among { s_size: 2, s: S_0_10.as_ptr(), substring_i: 0, result: 20, function: None },
    among { s_size: 2, s: S_0_11.as_ptr(), substring_i: 0, result: 20, function: None },
    among { s_size: 2, s: S_0_12.as_ptr(), substring_i: 0, result: 24, function: None },
    among { s_size: 2, s: S_0_13.as_ptr(), substring_i: 0, result: 24, function: None },
    among { s_size: 2, s: S_0_14.as_ptr(), substring_i: 0, result: 7, function: None },
    among { s_size: 2, s: S_0_15.as_ptr(), substring_i: 0, result: 1, function: None },
    among { s_size: 2, s: S_0_16.as_ptr(), substring_i: 0, result: 2, function: None },
    among { s_size: 2, s: S_0_17.as_ptr(), substring_i: 0, result: 3, function: None },
    among { s_size: 2, s: S_0_18.as_ptr(), substring_i: 0, result: 4, function: None },
    among { s_size: 2, s: S_0_19.as_ptr(), substring_i: 0, result: 5, function: None },
    among { s_size: 2, s: S_0_20.as_ptr(), substring_i: 0, result: 6, function: None },
    among { s_size: 2, s: S_0_21.as_ptr(), substring_i: 0, result: 7, function: None },
    among { s_size: 2, s: S_0_22.as_ptr(), substring_i: 0, result: 8, function: None },
    among { s_size: 2, s: S_0_23.as_ptr(), substring_i: 0, result: 9, function: None },
    among { s_size: 2, s: S_0_24.as_ptr(), substring_i: 0, result: 10, function: None },
    among { s_size: 2, s: S_0_25.as_ptr(), substring_i: 0, result: 11, function: None },
    among { s_size: 2, s: S_0_26.as_ptr(), substring_i: 0, result: 12, function: None },
    among { s_size: 2, s: S_0_27.as_ptr(), substring_i: 0, result: 13, function: None },
    among { s_size: 2, s: S_0_28.as_ptr(), substring_i: 0, result: 14, function: None },
    among { s_size: 2, s: S_0_29.as_ptr(), substring_i: 0, result: 15, function: None },
    among { s_size: 2, s: S_0_30.as_ptr(), substring_i: 0, result: 16, function: None },
    among { s_size: 2, s: S_0_31.as_ptr(), substring_i: 0, result: 17, function: None },
    among { s_size: 2, s: S_0_32.as_ptr(), substring_i: 0, result: 18, function: None },
    among { s_size: 2, s: S_0_33.as_ptr(), substring_i: 0, result: 19, function: None },
    among { s_size: 2, s: S_0_34.as_ptr(), substring_i: 0, result: 20, function: None },
    among { s_size: 2, s: S_0_35.as_ptr(), substring_i: 0, result: 21, function: None },
    among { s_size: 2, s: S_0_36.as_ptr(), substring_i: 0, result: 22, function: None },
    among { s_size: 2, s: S_0_37.as_ptr(), substring_i: 0, result: 23, function: None },
    among { s_size: 2, s: S_0_38.as_ptr(), substring_i: 0, result: 24, function: None },
    among { s_size: 2, s: S_0_39.as_ptr(), substring_i: 0, result: 9, function: None },
    among { s_size: 2, s: S_0_40.as_ptr(), substring_i: 0, result: 20, function: None },
    among { s_size: 2, s: S_0_41.as_ptr(), substring_i: 0, result: 1, function: None },
    among { s_size: 2, s: S_0_42.as_ptr(), substring_i: 0, result: 5, function: None },
    among { s_size: 2, s: S_0_43.as_ptr(), substring_i: 0, result: 7, function: None },
    among { s_size: 2, s: S_0_44.as_ptr(), substring_i: 0, result: 9, function: None },
    among { s_size: 2, s: S_0_45.as_ptr(), substring_i: 0, result: 20, function: None },
];

static S_1_0: [symbol; 16] = [0xCE, 0xBA, 0xCE, 0xB1, 0xCE, 0xB8, 0xCE, 0xB5, 0xCF, 0x83, 0xCF, 0x84, 0xCF, 0x89, 0xCF, 0x83];

static S_1_1: [symbol; 6] = [0xCF, 0x86, 0xCF, 0x89, 0xCF, 0x83];

static S_1_2: [symbol; 10] = [0xCF, 0x80, 0xCE, 0xB5, 0xCF, 0x81, 0xCE, 0xB1, 0xCF, 0x83];

static S_1_3: [symbol; 10] = [0xCF, 0x84, 0xCE, 0xB5, 0xCF, 0x81, 0xCE, 0xB1, 0xCF, 0x83];

static S_1_4: [symbol; 10] = [0xCE, 0xBA, 0xCF, 0x81, 0xCE, 0xB5, 0xCE, 0xB1, 0xCF, 0x83];

static S_1_5: [symbol; 20] = [0xCE, 0xBA, 0xCE, 0xB1, 0xCE, 0xB8, 0xCE, 0xB5, 0xCF, 0x83, 0xCF, 0x84, 0xCF, 0x89, 0xCF, 0x84, 0xCE, 0xBF, 0xCF, 0x83];

static S_1_6: [symbol; 10] = [0xCF, 0x86, 0xCF, 0x89, 0xCF, 0x84, 0xCE, 0xBF, 0xCF, 0x83];

static S_1_7: [symbol; 14] = [0xCF, 0x80, 0xCE, 0xB5, 0xCF, 0x81, 0xCE, 0xB1, 0xCF, 0x84, 0xCE, 0xBF, 0xCF, 0x83];

static S_1_8: [symbol; 14] = [0xCF, 0x84, 0xCE, 0xB5, 0xCF, 0x81, 0xCE, 0xB1, 0xCF, 0x84, 0xCE, 0xBF, 0xCF, 0x83];

static S_1_9: [symbol; 14] = [0xCE, 0xBA, 0xCF, 0x81, 0xCE, 0xB5, 0xCE, 0xB1, 0xCF, 0x84, 0xCE, 0xBF, 0xCF, 0x83];

static S_1_10: [symbol; 18] = [0xCE, 0xB3, 0xCE, 0xB5, 0xCE, 0xB3, 0xCE, 0xBF, 0xCE, 0xBD, 0xCE, 0xBF, 0xCF, 0x84, 0xCE, 0xBF, 0xCF, 0x83];

static S_1_11: [symbol; 14] = [0xCE, 0xB3, 0xCE, 0xB5, 0xCE, 0xB3, 0xCE, 0xBF, 0xCE, 0xBD, 0xCE, 0xBF, 0xCF, 0x83];

static S_1_12: [symbol; 12] = [0xCF, 0x86, 0xCE, 0xB1, 0xCE, 0xB3, 0xCE, 0xB9, 0xCE, 0xBF, 0xCF, 0x85];

static S_1_13: [symbol; 14] = [0xCF, 0x83, 0xCE, 0xBA, 0xCE, 0xB1, 0xCE, 0xB3, 0xCE, 0xB9, 0xCE, 0xBF, 0xCF, 0x85];

static S_1_14: [symbol; 12] = [0xCF, 0x83, 0xCE, 0xBF, 0xCE, 0xB3, 0xCE, 0xB9, 0xCE, 0xBF, 0xCF, 0x85];

static S_1_15: [symbol; 16] = [0xCF, 0x84, 0xCE, 0xB1, 0xCF, 0x84, 0xCE, 0xBF, 0xCE, 0xB3, 0xCE, 0xB9, 0xCE, 0xBF, 0xCF, 0x85];

static S_1_16: [symbol; 14] = [0xCE, 0xBF, 0xCE, 0xBB, 0xCE, 0xBF, 0xCE, 0xB3, 0xCE, 0xB9, 0xCE, 0xBF, 0xCF, 0x85];

static S_1_17: [symbol; 18] = [0xCE, 0xBA, 0xCE, 0xB1, 0xCE, 0xB8, 0xCE, 0xB5, 0xCF, 0x83, 0xCF, 0x84, 0xCF, 0x89, 0xCF, 0x84, 0xCE, 0xB1];

static S_1_18: [symbol; 8] = [0xCF, 0x86, 0xCF, 0x89, 0xCF, 0x84, 0xCE, 0xB1];

static S_1_19: [symbol; 12] = [0xCF, 0x80, 0xCE, 0xB5, 0xCF, 0x81, 0xCE, 0xB1, 0xCF, 0x84, 0xCE, 0xB1];

static S_1_20: [symbol; 12] = [0xCF, 0x84, 0xCE, 0xB5, 0xCF, 0x81, 0xCE, 0xB1, 0xCF, 0x84, 0xCE, 0xB1];

static S_1_21: [symbol; 12] = [0xCE, 0xBA, 0xCF, 0x81, 0xCE, 0xB5, 0xCE, 0xB1, 0xCF, 0x84, 0xCE, 0xB1];

static S_1_22: [symbol; 16] = [0xCE, 0xB3, 0xCE, 0xB5, 0xCE, 0xB3, 0xCE, 0xBF, 0xCE, 0xBD, 0xCE, 0xBF, 0xCF, 0x84, 0xCE, 0xB1];

static S_1_23: [symbol; 10] = [0xCF, 0x86, 0xCE, 0xB1, 0xCE, 0xB3, 0xCE, 0xB9, 0xCE, 0xB1];

static S_1_24: [symbol; 12] = [0xCF, 0x83, 0xCE, 0xBA, 0xCE, 0xB1, 0xCE, 0xB3, 0xCE, 0xB9, 0xCE, 0xB1];

static S_1_25: [symbol; 10] = [0xCF, 0x83, 0xCE, 0xBF, 0xCE, 0xB3, 0xCE, 0xB9, 0xCE, 0xB1];

static S_1_26: [symbol; 14] = [0xCF, 0x84, 0xCE, 0xB1, 0xCF, 0x84, 0xCE, 0xBF, 0xCE, 0xB3, 0xCE, 0xB9, 0xCE, 0xB1];

static S_1_27: [symbol; 12] = [0xCE, 0xBF, 0xCE, 0xBB, 0xCE, 0xBF, 0xCE, 0xB3, 0xCE, 0xB9, 0xCE, 0xB1];

static S_1_28: [symbol; 12] = [0xCF, 0x80, 0xCE, 0xB5, 0xCF, 0x81, 0xCE, 0xB1, 0xCF, 0x84, 0xCE, 0xB7];

static S_1_29: [symbol; 20] = [0xCE, 0xBA, 0xCE, 0xB1, 0xCE, 0xB8, 0xCE, 0xB5, 0xCF, 0x83, 0xCF, 0x84, 0xCF, 0x89, 0xCF, 0x84, 0xCF, 0x89, 0xCE, 0xBD];

static S_1_30: [symbol; 10] = [0xCF, 0x86, 0xCF, 0x89, 0xCF, 0x84, 0xCF, 0x89, 0xCE, 0xBD];

static S_1_31: [symbol; 14] = [0xCF, 0x80, 0xCE, 0xB5, 0xCF, 0x81, 0xCE, 0xB1, 0xCF, 0x84, 0xCF, 0x89, 0xCE, 0xBD];

static S_1_32: [symbol; 14] = [0xCF, 0x84, 0xCE, 0xB5, 0xCF, 0x81, 0xCE, 0xB1, 0xCF, 0x84, 0xCF, 0x89, 0xCE, 0xBD];

static S_1_33: [symbol; 14] = [0xCE, 0xBA, 0xCF, 0x81, 0xCE, 0xB5, 0xCE, 0xB1, 0xCF, 0x84, 0xCF, 0x89, 0xCE, 0xBD];

static S_1_34: [symbol; 18] = [0xCE, 0xB3, 0xCE, 0xB5, 0xCE, 0xB3, 0xCE, 0xBF, 0xCE, 0xBD, 0xCE, 0xBF, 0xCF, 0x84, 0xCF, 0x89, 0xCE, 0xBD];

static S_1_35: [symbol; 12] = [0xCF, 0x86, 0xCE, 0xB1, 0xCE, 0xB3, 0xCE, 0xB9, 0xCF, 0x89, 0xCE, 0xBD];

static S_1_36: [symbol; 14] = [0xCF, 0x83, 0xCE, 0xBA, 0xCE, 0xB1, 0xCE, 0xB3, 0xCE, 0xB9, 0xCF, 0x89, 0xCE, 0xBD];

static S_1_37: [symbol; 12] = [0xCF, 0x83, 0xCE, 0xBF, 0xCE, 0xB3, 0xCE, 0xB9, 0xCF, 0x89, 0xCE, 0xBD];

static S_1_38: [symbol; 16] = [0xCF, 0x84, 0xCE, 0xB1, 0xCF, 0x84, 0xCE, 0xBF, 0xCE, 0xB3, 0xCE, 0xB9, 0xCF, 0x89, 0xCE, 0xBD];

static S_1_39: [symbol; 14] = [0xCE, 0xBF, 0xCE, 0xBB, 0xCE, 0xBF, 0xCE, 0xB3, 0xCE, 0xB9, 0xCF, 0x89, 0xCE, 0xBD];

static A_1: [among; 40] = [
    among { s_size: 16, s: S_1_0.as_ptr(), substring_i: -1, result: 10, function: None },
    among { s_size: 6, s: S_1_1.as_ptr(), substring_i: -1, result: 9, function: None },
    among { s_size: 10, s: S_1_2.as_ptr(), substring_i: -1, result: 7, function: None },
    among { s_size: 10, s: S_1_3.as_ptr(), substring_i: -1, result: 8, function: None },
    among { s_size: 10, s: S_1_4.as_ptr(), substring_i: -1, result: 6, function: None },
    among { s_size: 20, s: S_1_5.as_ptr(), substring_i: -1, result: 10, function: None },
    among { s_size: 10, s: S_1_6.as_ptr(), substring_i: -1, result: 9, function: None },
    among { s_size: 14, s: S_1_7.as_ptr(), substring_i: -1, result: 7, function: None },
    among { s_size: 14, s: S_1_8.as_ptr(), substring_i: -1, result: 8, function: None },
    among { s_size: 14, s: S_1_9.as_ptr(), substring_i: -1, result: 6, function: None },
    among { s_size: 18, s: S_1_10.as_ptr(), substring_i: -1, result: 11, function: None },
    among { s_size: 14, s: S_1_11.as_ptr(), substring_i: -1, result: 11, function: None },
    among { s_size: 12, s: S_1_12.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 14, s: S_1_13.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 12, s: S_1_14.as_ptr(), substring_i: -1, result: 4, function: None },
    among { s_size: 16, s: S_1_15.as_ptr(), substring_i: -1, result: 5, function: None },
    among { s_size: 14, s: S_1_16.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 18, s: S_1_17.as_ptr(), substring_i: -1, result: 10, function: None },
    among { s_size: 8, s: S_1_18.as_ptr(), substring_i: -1, result: 9, function: None },
    among { s_size: 12, s: S_1_19.as_ptr(), substring_i: -1, result: 7, function: None },
    among { s_size: 12, s: S_1_20.as_ptr(), substring_i: -1, result: 8, function: None },
    among { s_size: 12, s: S_1_21.as_ptr(), substring_i: -1, result: 6, function: None },
    among { s_size: 16, s: S_1_22.as_ptr(), substring_i: -1, result: 11, function: None },
    among { s_size: 10, s: S_1_23.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 12, s: S_1_24.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 10, s: S_1_25.as_ptr(), substring_i: -1, result: 4, function: None },
    among { s_size: 14, s: S_1_26.as_ptr(), substring_i: -1, result: 5, function: None },
    among { s_size: 12, s: S_1_27.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 12, s: S_1_28.as_ptr(), substring_i: -1, result: 7, function: None },
    among { s_size: 20, s: S_1_29.as_ptr(), substring_i: -1, result: 10, function: None },
    among { s_size: 10, s: S_1_30.as_ptr(), substring_i: -1, result: 9, function: None },
    among { s_size: 14, s: S_1_31.as_ptr(), substring_i: -1, result: 7, function: None },
    among { s_size: 14, s: S_1_32.as_ptr(), substring_i: -1, result: 8, function: None },
    among { s_size: 14, s: S_1_33.as_ptr(), substring_i: -1, result: 6, function: None },
    among { s_size: 18, s: S_1_34.as_ptr(), substring_i: -1, result: 11, function: None },
    among { s_size: 12, s: S_1_35.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 14, s: S_1_36.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 12, s: S_1_37.as_ptr(), substring_i: -1, result: 4, function: None },
    among { s_size: 16, s: S_1_38.as_ptr(), substring_i: -1, result: 5, function: None },
    among { s_size: 14, s: S_1_39.as_ptr(), substring_i: -1, result: 3, function: None },
];

static S_2_0: [symbol; 2] = [0xCF, 0x80];

static S_2_1: [symbol; 6] = [0xCE, 0xB9, 0xCE, 0xBC, 0xCF, 0x80];

static S_2_2: [symbol; 2] = [0xCF, 0x81];

static S_2_3: [symbol; 4] = [0xCF, 0x80, 0xCF, 0x81];

static S_2_4: [symbol; 6] = [0xCE, 0xBC, 0xCF, 0x80, 0xCF, 0x81];

static S_2_5: [symbol; 6] = [0xCE, 0xB1, 0xCF, 0x81, 0xCF, 0x81];

static S_2_6: [symbol; 12] = [0xCE, 0xB3, 0xCE, 0xBB, 0xCF, 0x85, 0xCE, 0xBA, 0xCF, 0x85, 0xCF, 0x81];

static S_2_7: [symbol; 10] = [0xCF, 0x80, 0xCE, 0xBF, 0xCE, 0xBB, 0xCF, 0x85, 0xCF, 0x81];

static S_2_8: [symbol; 10] = [0xCE, 0xB1, 0xCE, 0xBC, 0xCF, 0x80, 0xCE, 0xB1, 0xCF, 0x81];

static S_2_9: [symbol; 6] = [0xCE, 0xBC, 0xCE, 0xB1, 0xCF, 0x81];

static S_2_10: [symbol; 6] = [0xCE, 0xB3, 0xCE, 0xBA, 0xCF, 0x81];

static S_2_11: [symbol; 14] = [0xCF, 0x80, 0xCE, 0xB9, 0xCF, 0x80, 0xCE, 0xB5, 0xCF, 0x81, 0xCE, 0xBF, 0xCF, 0x81];

static S_2_12: [symbol; 12] = [0xCE, 0xB2, 0xCE, 0xBF, 0xCE, 0xBB, 0xCE, 0xB2, 0xCE, 0xBF, 0xCF, 0x81];

static S_2_13: [symbol; 12] = [0xCE, 0xB3, 0xCE, 0xBB, 0xCF, 0x85, 0xCE, 0xBA, 0xCE, 0xBF, 0xCF, 0x81];

static S_2_14: [symbol; 6] = [0xCE, 0xBB, 0xCE, 0xBF, 0xCF, 0x85];

static S_2_15: [symbol; 4] = [0xCF, 0x80, 0xCE, 0xB1];

static S_2_16: [symbol; 12] = [0xCE, 0xBE, 0xCE, 0xB1, 0xCE, 0xBD, 0xCE, 0xB1, 0xCF, 0x80, 0xCE, 0xB1];

static S_2_17: [symbol; 6] = [0xCE, 0xB5, 0xCF, 0x80, 0xCE, 0xB1];

static S_2_18: [symbol; 12] = [0xCF, 0x80, 0xCE, 0xB5, 0xCF, 0x81, 0xCE, 0xB9, 0xCF, 0x80, 0xCE, 0xB1];

static S_2_19: [symbol; 12] = [0xCE, 0xB1, 0xCE, 0xBD, 0xCE, 0xB1, 0xCE, 0xBC, 0xCF, 0x80, 0xCE, 0xB1];

static S_2_20: [symbol; 8] = [0xCE, 0xB5, 0xCE, 0xBC, 0xCF, 0x80, 0xCE, 0xB1];

static S_2_21: [symbol; 2] = [0xCE, 0xB2];

static S_2_22: [symbol; 8] = [0xCE, 0xB4, 0xCE, 0xB1, 0xCE, 0xBD, 0xCE, 0xB5];

static S_2_23: [symbol; 12] = [0xCE, 0xB2, 0xCE, 0xB1, 0xCE, 0xB8, 0xCF, 0x85, 0xCF, 0x81, 0xCE, 0xB9];

static S_2_24: [symbol; 8] = [0xCE, 0xB2, 0xCE, 0xB1, 0xCF, 0x81, 0xCE, 0xBA];

static S_2_25: [symbol; 8] = [0xCE, 0xBC, 0xCE, 0xB1, 0xCF, 0x81, 0xCE, 0xBA];

static S_2_26: [symbol; 2] = [0xCE, 0xBB];

static S_2_27: [symbol; 2] = [0xCE, 0xBC];

static S_2_28: [symbol; 8] = [0xCE, 0xBA, 0xCE, 0xBF, 0xCF, 0x81, 0xCE, 0xBD];

static S_2_29: [symbol; 8] = [0xCE, 0xB1, 0xCE, 0xB8, 0xCF, 0x81, 0xCE, 0xBF];

static S_2_30: [symbol; 14] = [0xCF, 0x83, 0xCF, 0x85, 0xCE, 0xBD, 0xCE, 0xB1, 0xCE, 0xB8, 0xCF, 0x81, 0xCE, 0xBF];

static A_2: [among; 31] = [
    among { s_size: 2, s: S_2_0.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 6, s: S_2_1.as_ptr(), substring_i: 0, result: 2, function: None },
    among { s_size: 2, s: S_2_2.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 4, s: S_2_3.as_ptr(), substring_i: 2, result: 2, function: None },
    among { s_size: 6, s: S_2_4.as_ptr(), substring_i: 3, result: 2, function: None },
    among { s_size: 6, s: S_2_5.as_ptr(), substring_i: 2, result: 2, function: None },
    among { s_size: 12, s: S_2_6.as_ptr(), substring_i: 2, result: 2, function: None },
    among { s_size: 10, s: S_2_7.as_ptr(), substring_i: 2, result: 2, function: None },
    among { s_size: 10, s: S_2_8.as_ptr(), substring_i: 2, result: 2, function: None },
    among { s_size: 6, s: S_2_9.as_ptr(), substring_i: 2, result: 2, function: None },
    among { s_size: 6, s: S_2_10.as_ptr(), substring_i: 2, result: 2, function: None },
    among { s_size: 14, s: S_2_11.as_ptr(), substring_i: 2, result: 2, function: None },
    among { s_size: 12, s: S_2_12.as_ptr(), substring_i: 2, result: 2, function: None },
    among { s_size: 12, s: S_2_13.as_ptr(), substring_i: 2, result: 2, function: None },
    among { s_size: 6, s: S_2_14.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 4, s: S_2_15.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 12, s: S_2_16.as_ptr(), substring_i: 15, result: 1, function: None },
    among { s_size: 6, s: S_2_17.as_ptr(), substring_i: 15, result: 1, function: None },
    among { s_size: 12, s: S_2_18.as_ptr(), substring_i: 15, result: 1, function: None },
    among { s_size: 12, s: S_2_19.as_ptr(), substring_i: 15, result: 1, function: None },
    among { s_size: 8, s: S_2_20.as_ptr(), substring_i: 15, result: 1, function: None },
    among { s_size: 2, s: S_2_21.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 8, s: S_2_22.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 12, s: S_2_23.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 8, s: S_2_24.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 8, s: S_2_25.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 2, s: S_2_26.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 2, s: S_2_27.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 8, s: S_2_28.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 8, s: S_2_29.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 14, s: S_2_30.as_ptr(), substring_i: 29, result: 1, function: None },
];

static S_3_0: [symbol; 8] = [0xCE, 0xB9, 0xCE, 0xB6, 0xCE, 0xB5, 0xCF, 0x83];

static S_3_1: [symbol; 10] = [0xCE, 0xB9, 0xCE, 0xB6, 0xCE, 0xB5, 0xCE, 0xB9, 0xCF, 0x83];

static S_3_2: [symbol; 6] = [0xCE, 0xB9, 0xCE, 0xB6, 0xCF, 0x89];

static S_3_3: [symbol; 6] = [0xCE, 0xB9, 0xCE, 0xB6, 0xCE, 0xB1];

static S_3_4: [symbol; 10] = [0xCE, 0xB9, 0xCE, 0xB6, 0xCE, 0xB1, 0xCF, 0x84, 0xCE, 0xB5];

static S_3_5: [symbol; 10] = [0xCE, 0xB9, 0xCE, 0xB6, 0xCE, 0xB5, 0xCF, 0x84, 0xCE, 0xB5];

static S_3_6: [symbol; 6] = [0xCE, 0xB9, 0xCE, 0xB6, 0xCE, 0xB5];

static S_3_7: [symbol; 12] = [0xCE, 0xB9, 0xCE, 0xB6, 0xCE, 0xBF, 0xCF, 0x85, 0xCE, 0xBC, 0xCE, 0xB5];

static S_3_8: [symbol; 10] = [0xCE, 0xB9, 0xCE, 0xB6, 0xCE, 0xB1, 0xCE, 0xBC, 0xCE, 0xB5];

static S_3_9: [symbol; 12] = [0xCE, 0xB9, 0xCE, 0xB6, 0xCE, 0xBF, 0xCF, 0x85, 0xCE, 0xBD, 0xCE, 0xB5];

static S_3_10: [symbol; 10] = [0xCE, 0xB9, 0xCE, 0xB6, 0xCE, 0xB1, 0xCE, 0xBD, 0xCE, 0xB5];

static S_3_11: [symbol; 8] = [0xCE, 0xB9, 0xCE, 0xB6, 0xCE, 0xB5, 0xCE, 0xB9];

static S_3_12: [symbol; 10] = [0xCE, 0xB9, 0xCE, 0xB6, 0xCE, 0xBF, 0xCF, 0x85, 0xCE, 0xBD];

static S_3_13: [symbol; 8] = [0xCE, 0xB9, 0xCE, 0xB6, 0xCE, 0xB1, 0xCE, 0xBD];

static A_3: [among; 14] = [
    among { s_size: 8, s: S_3_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 10, s: S_3_1.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_3_2.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_3_3.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 10, s: S_3_4.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 10, s: S_3_5.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_3_6.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 12, s: S_3_7.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 10, s: S_3_8.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 12, s: S_3_9.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 10, s: S_3_10.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_3_11.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 10, s: S_3_12.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_3_13.as_ptr(), substring_i: -1, result: 1, function: None },
];

static S_4_0: [symbol; 2] = [0xCF, 0x83];

static S_4_1: [symbol; 2] = [0xCF, 0x87];

static S_4_2: [symbol; 4] = [0xCF, 0x85, 0xCF, 0x88];

static S_4_3: [symbol; 4] = [0xCE, 0xB6, 0xCF, 0x89];

static S_4_4: [symbol; 4] = [0xCE, 0xB2, 0xCE, 0xB9];

static S_4_5: [symbol; 4] = [0xCE, 0xBB, 0xCE, 0xB9];

static S_4_6: [symbol; 4] = [0xCE, 0xB1, 0xCE, 0xBB];

static S_4_7: [symbol; 4] = [0xCE, 0xB5, 0xCE, 0xBD];

static A_4: [among; 8] = [
    among { s_size: 2, s: S_4_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_4_1.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_4_2.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_4_3.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_4_4.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_4_5.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_4_6.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_4_7.as_ptr(), substring_i: -1, result: 1, function: None },
];

static S_5_0: [symbol; 12] = [0xCF, 0x89, 0xCE, 0xB8, 0xCE, 0xB7, 0xCE, 0xBA, 0xCE, 0xB5, 0xCF, 0x83];

static S_5_1: [symbol; 10] = [0xCF, 0x89, 0xCE, 0xB8, 0xCE, 0xB7, 0xCE, 0xBA, 0xCE, 0xB1];

static S_5_2: [symbol; 14] = [0xCF, 0x89, 0xCE, 0xB8, 0xCE, 0xB7, 0xCE, 0xBA, 0xCE, 0xB1, 0xCF, 0x84, 0xCE, 0xB5];

static S_5_3: [symbol; 10] = [0xCF, 0x89, 0xCE, 0xB8, 0xCE, 0xB7, 0xCE, 0xBA, 0xCE, 0xB5];

static S_5_4: [symbol; 14] = [0xCF, 0x89, 0xCE, 0xB8, 0xCE, 0xB7, 0xCE, 0xBA, 0xCE, 0xB1, 0xCE, 0xBC, 0xCE, 0xB5];

static S_5_5: [symbol; 14] = [0xCF, 0x89, 0xCE, 0xB8, 0xCE, 0xB7, 0xCE, 0xBA, 0xCE, 0xB1, 0xCE, 0xBD, 0xCE, 0xB5];

static S_5_6: [symbol; 12] = [0xCF, 0x89, 0xCE, 0xB8, 0xCE, 0xB7, 0xCE, 0xBA, 0xCE, 0xB1, 0xCE, 0xBD];

static A_5: [among; 7] = [
    among { s_size: 12, s: S_5_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 10, s: S_5_1.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 14, s: S_5_2.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 10, s: S_5_3.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 14, s: S_5_4.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 14, s: S_5_5.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 12, s: S_5_6.as_ptr(), substring_i: -1, result: 1, function: None },
];

static S_6_0: [symbol; 2] = [0xCF, 0x80];

static S_6_1: [symbol; 6] = [0xCE, 0xBB, 0xCE, 0xB1, 0xCF, 0x81];

static S_6_2: [symbol; 16] = [0xCE, 0xB4, 0xCE, 0xB7, 0xCE, 0xBC, 0xCE, 0xBF, 0xCE, 0xBA, 0xCF, 0x81, 0xCE, 0xB1, 0xCF, 0x84];

static S_6_3: [symbol; 4] = [0xCE, 0xB1, 0xCF, 0x86];

static S_6_4: [symbol; 18] = [0xCE, 0xB3, 0xCE, 0xB9, 0xCE, 0xB3, 0xCE, 0xB1, 0xCE, 0xBD, 0xCF, 0x84, 0xCE, 0xBF, 0xCE, 0xB1, 0xCF, 0x86];

static S_6_5: [symbol; 12] = [0xCE, 0xBE, 0xCE, 0xB1, 0xCE, 0xBD, 0xCE, 0xB1, 0xCF, 0x80, 0xCE, 0xB1];

static S_6_6: [symbol; 6] = [0xCE, 0xB5, 0xCF, 0x80, 0xCE, 0xB1];

static S_6_7: [symbol; 12] = [0xCF, 0x80, 0xCE, 0xB5, 0xCF, 0x81, 0xCE, 0xB9, 0xCF, 0x80, 0xCE, 0xB1];

static S_6_8: [symbol; 12] = [0xCE, 0xB1, 0xCE, 0xBD, 0xCE, 0xB1, 0xCE, 0xBC, 0xCF, 0x80, 0xCE, 0xB1];

static S_6_9: [symbol; 8] = [0xCE, 0xB5, 0xCE, 0xBC, 0xCF, 0x80, 0xCE, 0xB1];

static S_6_10: [symbol; 14] = [0xCF, 0x87, 0xCE, 0xB1, 0xCF, 0x81, 0xCF, 0x84, 0xCE, 0xBF, 0xCF, 0x80, 0xCE, 0xB1];

static S_6_11: [symbol; 12] = [0xCE, 0xB5, 0xCE, 0xBE, 0xCE, 0xB1, 0xCF, 0x81, 0xCF, 0x87, 0xCE, 0xB1];

static S_6_12: [symbol; 4] = [0xCF, 0x80, 0xCE, 0xB5];

static S_6_13: [symbol; 6] = [0xCE, 0xB5, 0xCF, 0x80, 0xCE, 0xB5];

static S_6_14: [symbol; 12] = [0xCE, 0xBC, 0xCE, 0xB5, 0xCF, 0x84, 0xCE, 0xB5, 0xCF, 0x80, 0xCE, 0xB5];

static S_6_15: [symbol; 6] = [0xCE, 0xB5, 0xCF, 0x83, 0xCE, 0xB5];

static S_6_16: [symbol; 4] = [0xCE, 0xB3, 0xCE, 0xB5];

static S_6_17: [symbol; 6] = [0xCE, 0xB3, 0xCE, 0xBA, 0xCE, 0xB5];

static S_6_18: [symbol; 6] = [0xCE, 0xBA, 0xCE, 0xBB, 0xCE, 0xB5];

static S_6_19: [symbol; 12] = [0xCE, 0xB5, 0xCF, 0x83, 0xCF, 0x89, 0xCE, 0xBA, 0xCE, 0xBB, 0xCE, 0xB5];

static S_6_20: [symbol; 8] = [0xCE, 0xB5, 0xCE, 0xBA, 0xCE, 0xBB, 0xCE, 0xB5];

static S_6_21: [symbol; 12] = [0xCE, 0xB1, 0xCF, 0x80, 0xCE, 0xB5, 0xCE, 0xBA, 0xCE, 0xBB, 0xCE, 0xB5];

static S_6_22: [symbol; 12] = [0xCE, 0xB1, 0xCF, 0x80, 0xCE, 0xBF, 0xCE, 0xBA, 0xCE, 0xBB, 0xCE, 0xB5];

static S_6_23: [symbol; 8] = [0xCE, 0xB4, 0xCE, 0xB1, 0xCE, 0xBD, 0xCE, 0xB5];

static S_6_24: [symbol; 4] = [0xCE, 0xB3, 0xCE, 0xBA];

static S_6_25: [symbol; 2] = [0xCE, 0xBC];

static S_6_26: [symbol; 12] = [0xCF, 0x80, 0xCE, 0xBF, 0xCF, 0x85, 0xCE, 0xBA, 0xCE, 0xB1, 0xCE, 0xBC];

static S_6_27: [symbol; 6] = [0xCE, 0xBA, 0xCE, 0xBF, 0xCE, 0xBC];

static S_6_28: [symbol; 4] = [0xCE, 0xB1, 0xCE, 0xBD];

static S_6_29: [symbol; 8] = [0xCE, 0xB1, 0xCE, 0xB8, 0xCF, 0x81, 0xCE, 0xBF];

static S_6_30: [symbol; 14] = [0xCF, 0x83, 0xCF, 0x85, 0xCE, 0xBD, 0xCE, 0xB1, 0xCE, 0xB8, 0xCF, 0x81, 0xCE, 0xBF];

static S_6_31: [symbol; 6] = [0xCE, 0xBF, 0xCE, 0xBB, 0xCE, 0xBF];

static A_6: [among; 32] = [
    among { s_size: 2, s: S_6_0.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 6, s: S_6_1.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 16, s: S_6_2.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 4, s: S_6_3.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 18, s: S_6_4.as_ptr(), substring_i: 3, result: 2, function: None },
    among { s_size: 12, s: S_6_5.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_6_6.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 12, s: S_6_7.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 12, s: S_6_8.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_6_9.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 14, s: S_6_10.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 12, s: S_6_11.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_6_12.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_6_13.as_ptr(), substring_i: 12, result: 1, function: None },
    among { s_size: 12, s: S_6_14.as_ptr(), substring_i: 13, result: 1, function: None },
    among { s_size: 6, s: S_6_15.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_6_16.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 6, s: S_6_17.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 6, s: S_6_18.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 12, s: S_6_19.as_ptr(), substring_i: 18, result: 1, function: None },
    among { s_size: 8, s: S_6_20.as_ptr(), substring_i: 18, result: 1, function: None },
    among { s_size: 12, s: S_6_21.as_ptr(), substring_i: 20, result: 1, function: None },
    among { s_size: 12, s: S_6_22.as_ptr(), substring_i: 18, result: 1, function: None },
    among { s_size: 8, s: S_6_23.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_6_24.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 2, s: S_6_25.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 12, s: S_6_26.as_ptr(), substring_i: 25, result: 2, function: None },
    among { s_size: 6, s: S_6_27.as_ptr(), substring_i: 25, result: 2, function: None },
    among { s_size: 4, s: S_6_28.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 8, s: S_6_29.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 14, s: S_6_30.as_ptr(), substring_i: 29, result: 1, function: None },
    among { s_size: 6, s: S_6_31.as_ptr(), substring_i: -1, result: 2, function: None },
];

static S_7_0: [symbol; 8] = [0xCE, 0xB9, 0xCF, 0x83, 0xCE, 0xB5, 0xCF, 0x83];

static S_7_1: [symbol; 6] = [0xCE, 0xB9, 0xCF, 0x83, 0xCE, 0xB1];

static S_7_2: [symbol; 6] = [0xCE, 0xB9, 0xCF, 0x83, 0xCE, 0xB5];

static S_7_3: [symbol; 10] = [0xCE, 0xB9, 0xCF, 0x83, 0xCE, 0xB1, 0xCF, 0x84, 0xCE, 0xB5];

static S_7_4: [symbol; 10] = [0xCE, 0xB9, 0xCF, 0x83, 0xCE, 0xB1, 0xCE, 0xBC, 0xCE, 0xB5];

static S_7_5: [symbol; 10] = [0xCE, 0xB9, 0xCF, 0x83, 0xCE, 0xB1, 0xCE, 0xBD, 0xCE, 0xB5];

static S_7_6: [symbol; 8] = [0xCE, 0xB9, 0xCF, 0x83, 0xCE, 0xB1, 0xCE, 0xBD];

static A_7: [among; 7] = [
    among { s_size: 8, s: S_7_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_7_1.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_7_2.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 10, s: S_7_3.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 10, s: S_7_4.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 10, s: S_7_5.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_7_6.as_ptr(), substring_i: -1, result: 1, function: None },
];

static S_8_0: [symbol; 12] = [0xCE, 0xBE, 0xCE, 0xB1, 0xCE, 0xBD, 0xCE, 0xB1, 0xCF, 0x80, 0xCE, 0xB1];

static S_8_1: [symbol; 6] = [0xCE, 0xB5, 0xCF, 0x80, 0xCE, 0xB1];

static S_8_2: [symbol; 12] = [0xCF, 0x80, 0xCE, 0xB5, 0xCF, 0x81, 0xCE, 0xB9, 0xCF, 0x80, 0xCE, 0xB1];

static S_8_3: [symbol; 12] = [0xCE, 0xB1, 0xCE, 0xBD, 0xCE, 0xB1, 0xCE, 0xBC, 0xCF, 0x80, 0xCE, 0xB1];

static S_8_4: [symbol; 8] = [0xCE, 0xB5, 0xCE, 0xBC, 0xCF, 0x80, 0xCE, 0xB1];

static S_8_5: [symbol; 14] = [0xCF, 0x87, 0xCE, 0xB1, 0xCF, 0x81, 0xCF, 0x84, 0xCE, 0xBF, 0xCF, 0x80, 0xCE, 0xB1];

static S_8_6: [symbol; 12] = [0xCE, 0xB5, 0xCE, 0xBE, 0xCE, 0xB1, 0xCF, 0x81, 0xCF, 0x87, 0xCE, 0xB1];

static S_8_7: [symbol; 4] = [0xCF, 0x80, 0xCE, 0xB5];

static S_8_8: [symbol; 6] = [0xCE, 0xB5, 0xCF, 0x80, 0xCE, 0xB5];

static S_8_9: [symbol; 12] = [0xCE, 0xBC, 0xCE, 0xB5, 0xCF, 0x84, 0xCE, 0xB5, 0xCF, 0x80, 0xCE, 0xB5];

static S_8_10: [symbol; 6] = [0xCE, 0xB5, 0xCF, 0x83, 0xCE, 0xB5];

static S_8_11: [symbol; 6] = [0xCE, 0xBA, 0xCE, 0xBB, 0xCE, 0xB5];

static S_8_12: [symbol; 12] = [0xCE, 0xB5, 0xCF, 0x83, 0xCF, 0x89, 0xCE, 0xBA, 0xCE, 0xBB, 0xCE, 0xB5];

static S_8_13: [symbol; 8] = [0xCE, 0xB5, 0xCE, 0xBA, 0xCE, 0xBB, 0xCE, 0xB5];

static S_8_14: [symbol; 12] = [0xCE, 0xB1, 0xCF, 0x80, 0xCE, 0xB5, 0xCE, 0xBA, 0xCE, 0xBB, 0xCE, 0xB5];

static S_8_15: [symbol; 12] = [0xCE, 0xB1, 0xCF, 0x80, 0xCE, 0xBF, 0xCE, 0xBA, 0xCE, 0xBB, 0xCE, 0xB5];

static S_8_16: [symbol; 8] = [0xCE, 0xB4, 0xCE, 0xB1, 0xCE, 0xBD, 0xCE, 0xB5];

static S_8_17: [symbol; 8] = [0xCE, 0xB1, 0xCE, 0xB8, 0xCF, 0x81, 0xCE, 0xBF];

static S_8_18: [symbol; 14] = [0xCF, 0x83, 0xCF, 0x85, 0xCE, 0xBD, 0xCE, 0xB1, 0xCE, 0xB8, 0xCF, 0x81, 0xCE, 0xBF];

static A_8: [among; 19] = [
    among { s_size: 12, s: S_8_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_8_1.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 12, s: S_8_2.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 12, s: S_8_3.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_8_4.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 14, s: S_8_5.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 12, s: S_8_6.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_8_7.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_8_8.as_ptr(), substring_i: 7, result: 1, function: None },
    among { s_size: 12, s: S_8_9.as_ptr(), substring_i: 8, result: 1, function: None },
    among { s_size: 6, s: S_8_10.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_8_11.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 12, s: S_8_12.as_ptr(), substring_i: 11, result: 1, function: None },
    among { s_size: 8, s: S_8_13.as_ptr(), substring_i: 11, result: 1, function: None },
    among { s_size: 12, s: S_8_14.as_ptr(), substring_i: 13, result: 1, function: None },
    among { s_size: 12, s: S_8_15.as_ptr(), substring_i: 11, result: 1, function: None },
    among { s_size: 8, s: S_8_16.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_8_17.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 14, s: S_8_18.as_ptr(), substring_i: 17, result: 1, function: None },
];

static S_9_0: [symbol; 10] = [0xCE, 0xB9, 0xCF, 0x83, 0xCE, 0xB5, 0xCE, 0xB9, 0xCF, 0x83];

static S_9_1: [symbol; 6] = [0xCE, 0xB9, 0xCF, 0x83, 0xCF, 0x89];

static S_9_2: [symbol; 10] = [0xCE, 0xB9, 0xCF, 0x83, 0xCE, 0xB5, 0xCF, 0x84, 0xCE, 0xB5];

static S_9_3: [symbol; 12] = [0xCE, 0xB9, 0xCF, 0x83, 0xCE, 0xBF, 0xCF, 0x85, 0xCE, 0xBC, 0xCE, 0xB5];

static S_9_4: [symbol; 12] = [0xCE, 0xB9, 0xCF, 0x83, 0xCE, 0xBF, 0xCF, 0x85, 0xCE, 0xBD, 0xCE, 0xB5];

static S_9_5: [symbol; 8] = [0xCE, 0xB9, 0xCF, 0x83, 0xCE, 0xB5, 0xCE, 0xB9];

static S_9_6: [symbol; 10] = [0xCE, 0xB9, 0xCF, 0x83, 0xCE, 0xBF, 0xCF, 0x85, 0xCE, 0xBD];

static A_9: [among; 7] = [
    among { s_size: 10, s: S_9_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_9_1.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 10, s: S_9_2.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 12, s: S_9_3.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 12, s: S_9_4.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_9_5.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 10, s: S_9_6.as_ptr(), substring_i: -1, result: 1, function: None },
];

static S_10_0: [symbol; 2] = [0xCF, 0x80];

static S_10_1: [symbol; 6] = [0xCE, 0xB5, 0xCF, 0x85, 0xCF, 0x80];

static S_10_2: [symbol; 4] = [0xCE, 0xB1, 0xCF, 0x80];

static S_10_3: [symbol; 6] = [0xCE, 0xB5, 0xCE, 0xBC, 0xCF, 0x80];

static S_10_4: [symbol; 6] = [0xCE, 0xB3, 0xCF, 0x85, 0xCF, 0x81];

static S_10_5: [symbol; 4] = [0xCF, 0x87, 0xCF, 0x81];

static S_10_6: [symbol; 6] = [0xCF, 0x87, 0xCF, 0x89, 0xCF, 0x81];

static S_10_7: [symbol; 4] = [0xCE, 0xB1, 0xCF, 0x81];

static S_10_8: [symbol; 6] = [0xCE, 0xB1, 0xCE, 0xBF, 0xCF, 0x81];

static S_10_9: [symbol; 4] = [0xCF, 0x87, 0xCF, 0x84];

static S_10_10: [symbol; 6] = [0xCE, 0xB1, 0xCF, 0x87, 0xCF, 0x84];

static S_10_11: [symbol; 4] = [0xCE, 0xBA, 0xCF, 0x84];

static S_10_12: [symbol; 6] = [0xCE, 0xB1, 0xCE, 0xBA, 0xCF, 0x84];

static S_10_13: [symbol; 4] = [0xCF, 0x83, 0xCF, 0x87];

static S_10_14: [symbol; 6] = [0xCE, 0xB1, 0xCF, 0x83, 0xCF, 0x87];

static S_10_15: [symbol; 6] = [0xCF, 0x84, 0xCE, 0xB1, 0xCF, 0x87];

static S_10_16: [symbol; 4] = [0xCF, 0x85, 0xCF, 0x88];

static S_10_17: [symbol; 6] = [0xCE, 0xB1, 0xCF, 0x84, 0xCE, 0xB1];

static S_10_18: [symbol; 4] = [0xCF, 0x86, 0xCE, 0xB1];

static S_10_19: [symbol; 6] = [0xCE, 0xB7, 0xCF, 0x86, 0xCE, 0xB1];

static S_10_20: [symbol; 6] = [0xCE, 0xBB, 0xCF, 0x85, 0xCE, 0xB3];

static S_10_21: [symbol; 6] = [0xCE, 0xBC, 0xCE, 0xB5, 0xCE, 0xB3];

static S_10_22: [symbol; 4] = [0xCE, 0xB7, 0xCE, 0xB4];

static S_10_23: [symbol; 4] = [0xCF, 0x83, 0xCE, 0xB5];

static S_10_24: [symbol; 6] = [0xCE, 0xB1, 0xCF, 0x83, 0xCE, 0xB5];

static S_10_25: [symbol; 6] = [0xCF, 0x80, 0xCE, 0xBB, 0xCE, 0xB5];

static S_10_26: [symbol; 6] = [0xCE, 0xBA, 0xCE, 0xBB, 0xCE, 0xB5];

static S_10_27: [symbol; 12] = [0xCE, 0xB5, 0xCF, 0x83, 0xCF, 0x89, 0xCE, 0xBA, 0xCE, 0xBB, 0xCE, 0xB5];

static S_10_28: [symbol; 8] = [0xCE, 0xB4, 0xCE, 0xB1, 0xCE, 0xBD, 0xCE, 0xB5];

static S_10_29: [symbol; 6] = [0xCE, 0xB5, 0xCF, 0x87, 0xCE, 0xB8];

static S_10_30: [symbol; 6] = [0xCE, 0xBA, 0xCE, 0xB1, 0xCE, 0xB8];

static S_10_31: [symbol; 4] = [0xCF, 0x83, 0xCE, 0xBA];

static S_10_32: [symbol; 6] = [0xCE, 0xBA, 0xCE, 0xB1, 0xCE, 0xBA];

static S_10_33: [symbol; 6] = [0xCE, 0xBC, 0xCE, 0xB1, 0xCE, 0xBA];

static S_10_34: [symbol; 6] = [0xCE, 0xBA, 0xCF, 0x85, 0xCE, 0xBB];

static S_10_35: [symbol; 6] = [0xCF, 0x86, 0xCE, 0xB9, 0xCE, 0xBB];

static S_10_36: [symbol; 2] = [0xCE, 0xBC];

static S_10_37: [symbol; 6] = [0xCE, 0xB3, 0xCE, 0xB5, 0xCE, 0xBC];

static S_10_38: [symbol; 6] = [0xCE, 0xB1, 0xCF, 0x87, 0xCE, 0xBD];

static S_10_39: [symbol; 14] = [0xCF, 0x83, 0xCF, 0x85, 0xCE, 0xBD, 0xCE, 0xB1, 0xCE, 0xB8, 0xCF, 0x81, 0xCE, 0xBF];

static A_10: [among; 40] = [
    among { s_size: 2, s: S_10_0.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 6, s: S_10_1.as_ptr(), substring_i: 0, result: 2, function: None },
    among { s_size: 4, s: S_10_2.as_ptr(), substring_i: 0, result: 2, function: None },
    among { s_size: 6, s: S_10_3.as_ptr(), substring_i: 0, result: 2, function: None },
    among { s_size: 6, s: S_10_4.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 4, s: S_10_5.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 6, s: S_10_6.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 4, s: S_10_7.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 6, s: S_10_8.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 4, s: S_10_9.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 6, s: S_10_10.as_ptr(), substring_i: 9, result: 2, function: None },
    among { s_size: 4, s: S_10_11.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 6, s: S_10_12.as_ptr(), substring_i: 11, result: 2, function: None },
    among { s_size: 4, s: S_10_13.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 6, s: S_10_14.as_ptr(), substring_i: 13, result: 2, function: None },
    among { s_size: 6, s: S_10_15.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 4, s: S_10_16.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 6, s: S_10_17.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 4, s: S_10_18.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 6, s: S_10_19.as_ptr(), substring_i: 18, result: 2, function: None },
    among { s_size: 6, s: S_10_20.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 6, s: S_10_21.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 4, s: S_10_22.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 4, s: S_10_23.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_10_24.as_ptr(), substring_i: 23, result: 1, function: None },
    among { s_size: 6, s: S_10_25.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_10_26.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 12, s: S_10_27.as_ptr(), substring_i: 26, result: 1, function: None },
    among { s_size: 8, s: S_10_28.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_10_29.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 6, s: S_10_30.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 4, s: S_10_31.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 6, s: S_10_32.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 6, s: S_10_33.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 6, s: S_10_34.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 6, s: S_10_35.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 2, s: S_10_36.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 6, s: S_10_37.as_ptr(), substring_i: 36, result: 2, function: None },
    among { s_size: 6, s: S_10_38.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 14, s: S_10_39.as_ptr(), substring_i: -1, result: 1, function: None },
];

static S_11_0: [symbol; 12] = [0xCE, 0xB9, 0xCF, 0x83, 0xCF, 0x84, 0xCE, 0xBF, 0xCF, 0x85, 0xCF, 0x83];

static S_11_1: [symbol; 10] = [0xCE, 0xB9, 0xCF, 0x83, 0xCF, 0x84, 0xCE, 0xB5, 0xCF, 0x83];

static S_11_2: [symbol; 10] = [0xCE, 0xB9, 0xCF, 0x83, 0xCF, 0x84, 0xCE, 0xB7, 0xCF, 0x83];

static S_11_3: [symbol; 10] = [0xCE, 0xB9, 0xCF, 0x83, 0xCF, 0x84, 0xCE, 0xBF, 0xCF, 0x83];

static S_11_4: [symbol; 10] = [0xCE, 0xB9, 0xCF, 0x83, 0xCF, 0x84, 0xCE, 0xBF, 0xCF, 0x85];

static S_11_5: [symbol; 8] = [0xCE, 0xB9, 0xCF, 0x83, 0xCF, 0x84, 0xCE, 0xB1];

static S_11_6: [symbol; 8] = [0xCE, 0xB9, 0xCF, 0x83, 0xCF, 0x84, 0xCE, 0xB5];

static S_11_7: [symbol; 8] = [0xCE, 0xB9, 0xCF, 0x83, 0xCF, 0x84, 0xCE, 0xB7];

static S_11_8: [symbol; 10] = [0xCE, 0xB9, 0xCF, 0x83, 0xCF, 0x84, 0xCE, 0xBF, 0xCE, 0xB9];

static S_11_9: [symbol; 10] = [0xCE, 0xB9, 0xCF, 0x83, 0xCF, 0x84, 0xCF, 0x89, 0xCE, 0xBD];

static S_11_10: [symbol; 8] = [0xCE, 0xB9, 0xCF, 0x83, 0xCF, 0x84, 0xCE, 0xBF];

static A_11: [among; 11] = [
    among { s_size: 12, s: S_11_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 10, s: S_11_1.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 10, s: S_11_2.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 10, s: S_11_3.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 10, s: S_11_4.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_11_5.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_11_6.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_11_7.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 10, s: S_11_8.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 10, s: S_11_9.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_11_10.as_ptr(), substring_i: -1, result: 1, function: None },
];

static S_12_0: [symbol; 4] = [0xCF, 0x83, 0xCE, 0xB5];

static S_12_1: [symbol; 12] = [0xCE, 0xBC, 0xCE, 0xB5, 0xCF, 0x84, 0xCE, 0xB1, 0xCF, 0x83, 0xCE, 0xB5];

static S_12_2: [symbol; 14] = [0xCE, 0xBC, 0xCE, 0xB9, 0xCE, 0xBA, 0xCF, 0x81, 0xCE, 0xBF, 0xCF, 0x83, 0xCE, 0xB5];

static S_12_3: [symbol; 10] = [0xCE, 0xB5, 0xCE, 0xB3, 0xCE, 0xBA, 0xCE, 0xBB, 0xCE, 0xB5];

static S_12_4: [symbol; 12] = [0xCE, 0xB1, 0xCF, 0x80, 0xCE, 0xBF, 0xCE, 0xBA, 0xCE, 0xBB, 0xCE, 0xB5];

static S_12_5: [symbol; 8] = [0xCE, 0xB4, 0xCE, 0xB1, 0xCE, 0xBD, 0xCE, 0xB5];

static S_12_6: [symbol; 16] = [0xCE, 0xB1, 0xCE, 0xBD, 0xCF, 0x84, 0xCE, 0xB9, 0xCE, 0xB4, 0xCE, 0xB1, 0xCE, 0xBD, 0xCE, 0xB5];

static A_12: [among; 7] = [
    among { s_size: 4, s: S_12_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 12, s: S_12_1.as_ptr(), substring_i: 0, result: 1, function: None },
    among { s_size: 14, s: S_12_2.as_ptr(), substring_i: 0, result: 1, function: None },
    among { s_size: 10, s: S_12_3.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 12, s: S_12_4.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_12_5.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 16, s: S_12_6.as_ptr(), substring_i: 5, result: 2, function: None },
];

static S_13_0: [symbol; 10] = [0xCF, 0x84, 0xCE, 0xBF, 0xCF, 0x80, 0xCE, 0xB9, 0xCE, 0xBA];

static S_13_1: [symbol; 14] = [0xCF, 0x83, 0xCE, 0xBA, 0xCE, 0xB5, 0xCF, 0x80, 0xCF, 0x84, 0xCE, 0xB9, 0xCE, 0xBA];

static S_13_2: [symbol; 14] = [0xCE, 0xB3, 0xCE, 0xBD, 0xCF, 0x89, 0xCF, 0x83, 0xCF, 0x84, 0xCE, 0xB9, 0xCE, 0xBA];

static S_13_3: [symbol; 16] = [0xCE, 0xB1, 0xCE, 0xB3, 0xCE, 0xBD, 0xCF, 0x89, 0xCF, 0x83, 0xCF, 0x84, 0xCE, 0xB9, 0xCE, 0xBA];

static S_13_4: [symbol; 16] = [0xCE, 0xB5, 0xCE, 0xBA, 0xCE, 0xBB, 0xCE, 0xB5, 0xCE, 0xBA, 0xCF, 0x84, 0xCE, 0xB9, 0xCE, 0xBA];

static S_13_5: [symbol; 12] = [0xCE, 0xB1, 0xCF, 0x84, 0xCE, 0xBF, 0xCE, 0xBC, 0xCE, 0xB9, 0xCE, 0xBA];

static S_13_6: [symbol; 10] = [0xCE, 0xB5, 0xCE, 0xB8, 0xCE, 0xBD, 0xCE, 0xB9, 0xCE, 0xBA];

static S_13_7: [symbol; 14] = [0xCE, 0xB8, 0xCE, 0xB5, 0xCE, 0xB1, 0xCF, 0x84, 0xCF, 0x81, 0xCE, 0xB9, 0xCE, 0xBD];

static S_13_8: [symbol; 20] = [0xCE, 0xB1, 0xCE, 0xBB, 0xCE, 0xB5, 0xCE, 0xBE, 0xCE, 0xB1, 0xCE, 0xBD, 0xCE, 0xB4, 0xCF, 0x81, 0xCE, 0xB9, 0xCE, 0xBD];

static S_13_9: [symbol; 16] = [0xCE, 0xB2, 0xCF, 0x85, 0xCE, 0xB6, 0xCE, 0xB1, 0xCE, 0xBD, 0xCF, 0x84, 0xCE, 0xB9, 0xCE, 0xBD];

static A_13: [among; 10] = [
    among { s_size: 10, s: S_13_0.as_ptr(), substring_i: -1, result: 7, function: None },
    among { s_size: 14, s: S_13_1.as_ptr(), substring_i: -1, result: 6, function: None },
    among { s_size: 14, s: S_13_2.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 16, s: S_13_3.as_ptr(), substring_i: 2, result: 1, function: None },
    among { s_size: 16, s: S_13_4.as_ptr(), substring_i: -1, result: 5, function: None },
    among { s_size: 12, s: S_13_5.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 10, s: S_13_6.as_ptr(), substring_i: -1, result: 4, function: None },
    among { s_size: 14, s: S_13_7.as_ptr(), substring_i: -1, result: 10, function: None },
    among { s_size: 20, s: S_13_8.as_ptr(), substring_i: -1, result: 8, function: None },
    among { s_size: 16, s: S_13_9.as_ptr(), substring_i: -1, result: 9, function: None },
];

static S_14_0: [symbol; 12] = [0xCE, 0xB9, 0xCF, 0x83, 0xCE, 0xBC, 0xCE, 0xBF, 0xCF, 0x85, 0xCF, 0x83];

static S_14_1: [symbol; 10] = [0xCE, 0xB9, 0xCF, 0x83, 0xCE, 0xBC, 0xCE, 0xBF, 0xCF, 0x83];

static S_14_2: [symbol; 10] = [0xCE, 0xB9, 0xCF, 0x83, 0xCE, 0xBC, 0xCE, 0xBF, 0xCF, 0x85];

static S_14_3: [symbol; 10] = [0xCE, 0xB9, 0xCF, 0x83, 0xCE, 0xBC, 0xCE, 0xBF, 0xCE, 0xB9];

static S_14_4: [symbol; 10] = [0xCE, 0xB9, 0xCF, 0x83, 0xCE, 0xBC, 0xCF, 0x89, 0xCE, 0xBD];

static S_14_5: [symbol; 8] = [0xCE, 0xB9, 0xCF, 0x83, 0xCE, 0xBC, 0xCE, 0xBF];

static A_14: [among; 6] = [
    among { s_size: 12, s: S_14_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 10, s: S_14_1.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 10, s: S_14_2.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 10, s: S_14_3.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 10, s: S_14_4.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_14_5.as_ptr(), substring_i: -1, result: 1, function: None },
];

static S_15_0: [symbol; 2] = [0xCF, 0x83];

static S_15_1: [symbol; 2] = [0xCF, 0x87];

static A_15: [among; 2] = [
    among { s_size: 2, s: S_15_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_15_1.as_ptr(), substring_i: -1, result: 1, function: None },
];

static S_16_0: [symbol; 12] = [0xCE, 0xB1, 0xCF, 0x81, 0xCE, 0xB1, 0xCE, 0xBA, 0xCE, 0xB9, 0xCE, 0xB1];

static S_16_1: [symbol; 14] = [0xCE, 0xBF, 0xCF, 0x85, 0xCE, 0xB4, 0xCE, 0xB1, 0xCE, 0xBA, 0xCE, 0xB9, 0xCE, 0xB1];

static S_16_2: [symbol; 10] = [0xCE, 0xB1, 0xCF, 0x81, 0xCE, 0xB1, 0xCE, 0xBA, 0xCE, 0xB9];

static S_16_3: [symbol; 12] = [0xCE, 0xBF, 0xCF, 0x85, 0xCE, 0xB4, 0xCE, 0xB1, 0xCE, 0xBA, 0xCE, 0xB9];

static A_16: [among; 4] = [
    among { s_size: 12, s: S_16_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 14, s: S_16_1.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 10, s: S_16_2.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 12, s: S_16_3.as_ptr(), substring_i: -1, result: 1, function: None },
];

static S_17_0: [symbol; 2] = [0xCF, 0x80];

static S_17_1: [symbol; 12] = [0xCE, 0xBA, 0xCE, 0xB1, 0xCF, 0x84, 0xCF, 0x81, 0xCE, 0xB1, 0xCF, 0x80];

static S_17_2: [symbol; 2] = [0xCF, 0x81];

static S_17_3: [symbol; 4] = [0xCE, 0xB2, 0xCF, 0x81];

static S_17_4: [symbol; 8] = [0xCE, 0xBB, 0xCE, 0xB1, 0xCE, 0xB2, 0xCF, 0x81];

static S_17_5: [symbol; 8] = [0xCE, 0xB1, 0xCE, 0xBC, 0xCE, 0xB2, 0xCF, 0x81];

static S_17_6: [symbol; 10] = [0xCF, 0x80, 0xCE, 0xB1, 0xCF, 0x84, 0xCE, 0xB5, 0xCF, 0x81];

static S_17_7: [symbol; 6] = [0xCE, 0xBC, 0xCE, 0xB5, 0xCF, 0x81];

static S_17_8: [symbol; 8] = [0xCE, 0xB1, 0xCE, 0xBD, 0xCE, 0xB8, 0xCF, 0x81];

static S_17_9: [symbol; 6] = [0xCE, 0xBA, 0xCE, 0xBF, 0xCF, 0x81];

static S_17_10: [symbol; 2] = [0xCF, 0x83];

static S_17_11: [symbol; 12] = [0xCE, 0xBD, 0xCE, 0xB1, 0xCE, 0xB3, 0xCE, 0xBA, 0xCE, 0xB1, 0xCF, 0x83];

static S_17_12: [symbol; 6] = [0xCF, 0x84, 0xCE, 0xBF, 0xCF, 0x83];

static S_17_13: [symbol; 10] = [0xCE, 0xBC, 0xCE, 0xBF, 0xCF, 0x85, 0xCF, 0x83, 0xCF, 0x84];

static S_17_14: [symbol; 4] = [0xCF, 0x81, 0xCF, 0x85];

static S_17_15: [symbol; 2] = [0xCF, 0x86];

static S_17_16: [symbol; 4] = [0xCF, 0x83, 0xCF, 0x86];

static S_17_17: [symbol; 10] = [0xCE, 0xB1, 0xCE, 0xBB, 0xCE, 0xB9, 0xCF, 0x83, 0xCF, 0x86];

static S_17_18: [symbol; 6] = [0xCE, 0xBD, 0xCF, 0x85, 0xCF, 0x86];

static S_17_19: [symbol; 2] = [0xCF, 0x87];

static S_17_20: [symbol; 2] = [0xCE, 0xB2];

static S_17_21: [symbol; 8] = [0xCE, 0xB2, 0xCE, 0xB1, 0xCE, 0xBC, 0xCE, 0xB2];

static S_17_22: [symbol; 8] = [0xCF, 0x83, 0xCE, 0xBB, 0xCE, 0xBF, 0xCE, 0xB2];

static S_17_23: [symbol; 18] = [0xCF, 0x84, 0xCF, 0x83, 0xCE, 0xB5, 0xCF, 0x87, 0xCE, 0xBF, 0xCF, 0x83, 0xCE, 0xBB, 0xCE, 0xBF, 0xCE, 0xB2];

static S_17_24: [symbol; 8] = [0xCE, 0xBA, 0xCE, 0xB1, 0xCF, 0x81, 0xCE, 0xB4];

static S_17_25: [symbol; 2] = [0xCE, 0xB6];

static S_17_26: [symbol; 4] = [0xCF, 0x84, 0xCE, 0xB6];

static S_17_27: [symbol; 2] = [0xCE, 0xBA];

static S_17_28: [symbol; 4] = [0xCF, 0x83, 0xCE, 0xBA];

static S_17_29: [symbol; 10] = [0xCE, 0xBA, 0xCE, 0xB1, 0xCF, 0x80, 0xCE, 0xB1, 0xCE, 0xBA];

static S_17_30: [symbol; 6] = [0xCF, 0x83, 0xCE, 0xBF, 0xCE, 0xBA];

static S_17_31: [symbol; 4] = [0xCF, 0x80, 0xCE, 0xBB];

static S_17_32: [symbol; 6] = [0xCF, 0x86, 0xCF, 0x85, 0xCE, 0xBB];

static S_17_33: [symbol; 8] = [0xCE, 0xBB, 0xCE, 0xBF, 0xCF, 0x85, 0xCE, 0xBB];

static S_17_34: [symbol; 6] = [0xCE, 0xB2, 0xCE, 0xB1, 0xCE, 0xBB];

static S_17_35: [symbol; 6] = [0xCE, 0xBC, 0xCE, 0xB1, 0xCE, 0xBB];

static S_17_36: [symbol; 4] = [0xCE, 0xB3, 0xCE, 0xBB];

static S_17_37: [symbol; 12] = [0xCF, 0x84, 0xCF, 0x81, 0xCE, 0xB9, 0xCF, 0x80, 0xCE, 0xBF, 0xCE, 0xBB];

static S_17_38: [symbol; 8] = [0xCF, 0x86, 0xCE, 0xB1, 0xCF, 0x81, 0xCE, 0xBC];

static S_17_39: [symbol; 8] = [0xCE, 0xBA, 0xCE, 0xB1, 0xCE, 0xB9, 0xCE, 0xBC];

static S_17_40: [symbol; 8] = [0xCE, 0xBA, 0xCE, 0xBB, 0xCE, 0xB9, 0xCE, 0xBC];

static S_17_41: [symbol; 12] = [0xCE, 0xBC, 0xCE, 0xB1, 0xCE, 0xBA, 0xCF, 0x81, 0xCF, 0x85, 0xCE, 0xBD];

static S_17_42: [symbol; 8] = [0xCF, 0x83, 0xCF, 0x80, 0xCE, 0xB1, 0xCE, 0xBD];

static S_17_43: [symbol; 8] = [0xCE, 0xB3, 0xCE, 0xB9, 0xCE, 0xB1, 0xCE, 0xBD];

static S_17_44: [symbol; 14] = [0xCE, 0xB7, 0xCE, 0xB3, 0xCE, 0xBF, 0xCF, 0x85, 0xCE, 0xBC, 0xCE, 0xB5, 0xCE, 0xBD];

static S_17_45: [symbol; 6] = [0xCE, 0xBA, 0xCE, 0xBF, 0xCE, 0xBD];

static A_17: [among; 46] = [
    among { s_size: 2, s: S_17_0.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 12, s: S_17_1.as_ptr(), substring_i: 0, result: 1, function: None },
    among { s_size: 2, s: S_17_2.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_17_3.as_ptr(), substring_i: 2, result: 1, function: None },
    among { s_size: 8, s: S_17_4.as_ptr(), substring_i: 3, result: 1, function: None },
    among { s_size: 8, s: S_17_5.as_ptr(), substring_i: 3, result: 1, function: None },
    among { s_size: 10, s: S_17_6.as_ptr(), substring_i: 2, result: 2, function: None },
    among { s_size: 6, s: S_17_7.as_ptr(), substring_i: 2, result: 1, function: None },
    among { s_size: 8, s: S_17_8.as_ptr(), substring_i: 2, result: 1, function: None },
    among { s_size: 6, s: S_17_9.as_ptr(), substring_i: 2, result: 1, function: None },
    among { s_size: 2, s: S_17_10.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 12, s: S_17_11.as_ptr(), substring_i: 10, result: 1, function: None },
    among { s_size: 6, s: S_17_12.as_ptr(), substring_i: 10, result: 2, function: None },
    among { s_size: 10, s: S_17_13.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_17_14.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_17_15.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_17_16.as_ptr(), substring_i: 15, result: 1, function: None },
    among { s_size: 10, s: S_17_17.as_ptr(), substring_i: 16, result: 1, function: None },
    among { s_size: 6, s: S_17_18.as_ptr(), substring_i: 15, result: 2, function: None },
    among { s_size: 2, s: S_17_19.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_17_20.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 8, s: S_17_21.as_ptr(), substring_i: 20, result: 1, function: None },
    among { s_size: 8, s: S_17_22.as_ptr(), substring_i: 20, result: 1, function: None },
    among { s_size: 18, s: S_17_23.as_ptr(), substring_i: 22, result: 1, function: None },
    among { s_size: 8, s: S_17_24.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 2, s: S_17_25.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 4, s: S_17_26.as_ptr(), substring_i: 25, result: 1, function: None },
    among { s_size: 2, s: S_17_27.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_17_28.as_ptr(), substring_i: 27, result: 1, function: None },
    among { s_size: 10, s: S_17_29.as_ptr(), substring_i: 27, result: 1, function: None },
    among { s_size: 6, s: S_17_30.as_ptr(), substring_i: 27, result: 1, function: None },
    among { s_size: 4, s: S_17_31.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_17_32.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_17_33.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_17_34.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 6, s: S_17_35.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_17_36.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 12, s: S_17_37.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 8, s: S_17_38.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_17_39.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_17_40.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 12, s: S_17_41.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 8, s: S_17_42.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_17_43.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 14, s: S_17_44.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 6, s: S_17_45.as_ptr(), substring_i: -1, result: 1, function: None },
];

static S_18_0: [symbol; 10] = [0xCE, 0xB9, 0xCF, 0x84, 0xCF, 0x83, 0xCE, 0xB1, 0xCF, 0x83];

static S_18_1: [symbol; 10] = [0xCE, 0xB9, 0xCF, 0x84, 0xCF, 0x83, 0xCE, 0xB5, 0xCF, 0x83];

static S_18_2: [symbol; 8] = [0xCE, 0xB9, 0xCF, 0x84, 0xCF, 0x83, 0xCE, 0xB1];

static S_18_3: [symbol; 8] = [0xCE, 0xB1, 0xCE, 0xBA, 0xCE, 0xB9, 0xCE, 0xB1];

static S_18_4: [symbol; 12] = [0xCE, 0xB1, 0xCF, 0x81, 0xCE, 0xB1, 0xCE, 0xBA, 0xCE, 0xB9, 0xCE, 0xB1];

static S_18_5: [symbol; 6] = [0xCE, 0xB1, 0xCE, 0xBA, 0xCE, 0xB9];

static S_18_6: [symbol; 10] = [0xCE, 0xB1, 0xCF, 0x81, 0xCE, 0xB1, 0xCE, 0xBA, 0xCE, 0xB9];

static S_18_7: [symbol; 10] = [0xCE, 0xB9, 0xCF, 0x84, 0xCF, 0x83, 0xCF, 0x89, 0xCE, 0xBD];

static A_18: [among; 8] = [
    among { s_size: 10, s: S_18_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 10, s: S_18_1.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_18_2.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_18_3.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 12, s: S_18_4.as_ptr(), substring_i: 3, result: 1, function: None },
    among { s_size: 6, s: S_18_5.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 10, s: S_18_6.as_ptr(), substring_i: 5, result: 1, function: None },
    among { s_size: 10, s: S_18_7.as_ptr(), substring_i: -1, result: 1, function: None },
];

static S_19_0: [symbol; 4] = [0xCE, 0xB9, 0xCF, 0x81];

static S_19_1: [symbol; 6] = [0xCF, 0x88, 0xCE, 0xB1, 0xCE, 0xBB];

static S_19_2: [symbol; 8] = [0xCE, 0xB1, 0xCE, 0xB9, 0xCF, 0x86, 0xCE, 0xBD];

static S_19_3: [symbol; 6] = [0xCE, 0xBF, 0xCE, 0xBB, 0xCE, 0xBF];

static A_19: [among; 4] = [
    among { s_size: 4, s: S_19_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_19_1.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_19_2.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_19_3.as_ptr(), substring_i: -1, result: 1, function: None },
];

static S_20_0: [symbol; 2] = [0xCE, 0xB5];

static S_20_1: [symbol; 10] = [0xCF, 0x80, 0xCE, 0xB1, 0xCE, 0xB9, 0xCF, 0x87, 0xCE, 0xBD];

static A_20: [among; 2] = [
    among { s_size: 2, s: S_20_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 10, s: S_20_1.as_ptr(), substring_i: -1, result: 1, function: None },
];

static S_21_0: [symbol; 8] = [0xCE, 0xB9, 0xCE, 0xB4, 0xCE, 0xB9, 0xCE, 0xB1];

static S_21_1: [symbol; 10] = [0xCE, 0xB9, 0xCE, 0xB4, 0xCE, 0xB9, 0xCF, 0x89, 0xCE, 0xBD];

static S_21_2: [symbol; 8] = [0xCE, 0xB9, 0xCE, 0xB4, 0xCE, 0xB9, 0xCE, 0xBF];

static A_21: [among; 3] = [
    among { s_size: 8, s: S_21_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 10, s: S_21_1.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_21_2.as_ptr(), substring_i: -1, result: 1, function: None },
];

static S_22_0: [symbol; 2] = [0xCF, 0x81];

static S_22_1: [symbol; 4] = [0xCE, 0xB9, 0xCE, 0xB2];

static S_22_2: [symbol; 2] = [0xCE, 0xB4];

static S_22_3: [symbol; 6] = [0xCE, 0xBB, 0xCF, 0x85, 0xCE, 0xBA];

static S_22_4: [symbol; 10] = [0xCF, 0x86, 0xCF, 0x81, 0xCE, 0xB1, 0xCE, 0xB3, 0xCE, 0xBA];

static S_22_5: [symbol; 8] = [0xCE, 0xBF, 0xCE, 0xB2, 0xCE, 0xB5, 0xCE, 0xBB];

static S_22_6: [symbol; 6] = [0xCE, 0xBC, 0xCE, 0xB7, 0xCE, 0xBD];

static A_22: [among; 7] = [
    among { s_size: 2, s: S_22_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_22_1.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_22_2.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_22_3.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 10, s: S_22_4.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_22_5.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_22_6.as_ptr(), substring_i: -1, result: 1, function: None },
];

static S_23_0: [symbol; 10] = [0xCE, 0xB9, 0xCF, 0x83, 0xCE, 0xBA, 0xCE, 0xBF, 0xCF, 0x83];

static S_23_1: [symbol; 10] = [0xCE, 0xB9, 0xCF, 0x83, 0xCE, 0xBA, 0xCE, 0xBF, 0xCF, 0x85];

static S_23_2: [symbol; 8] = [0xCE, 0xB9, 0xCF, 0x83, 0xCE, 0xBA, 0xCE, 0xB5];

static S_23_3: [symbol; 8] = [0xCE, 0xB9, 0xCF, 0x83, 0xCE, 0xBA, 0xCE, 0xBF];

static A_23: [among; 4] = [
    among { s_size: 10, s: S_23_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 10, s: S_23_1.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_23_2.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_23_3.as_ptr(), substring_i: -1, result: 1, function: None },
];

static S_24_0: [symbol; 8] = [0xCE, 0xB1, 0xCE, 0xB4, 0xCE, 0xB5, 0xCF, 0x83];

static S_24_1: [symbol; 8] = [0xCE, 0xB1, 0xCE, 0xB4, 0xCF, 0x89, 0xCE, 0xBD];

static A_24: [among; 2] = [
    among { s_size: 8, s: S_24_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_24_1.as_ptr(), substring_i: -1, result: 1, function: None },
];

static S_25_0: [symbol; 10] = [0xCE, 0xBC, 0xCF, 0x80, 0xCE, 0xB1, 0xCE, 0xBC, 0xCF, 0x80];

static S_25_1: [symbol; 6] = [0xCE, 0xBA, 0xCF, 0x85, 0xCF, 0x81];

static S_25_2: [symbol; 10] = [0xCF, 0x80, 0xCE, 0xB1, 0xCF, 0x84, 0xCE, 0xB5, 0xCF, 0x81];

static S_25_3: [symbol; 10] = [0xCF, 0x80, 0xCE, 0xB5, 0xCE, 0xB8, 0xCE, 0xB5, 0xCF, 0x81];

static S_25_4: [symbol; 10] = [0xCE, 0xBD, 0xCF, 0x84, 0xCE, 0xB1, 0xCE, 0xBD, 0xCF, 0x84];

static S_25_5: [symbol; 10] = [0xCE, 0xB3, 0xCE, 0xB9, 0xCE, 0xB1, 0xCE, 0xB3, 0xCE, 0xB9];

static S_25_6: [symbol; 6] = [0xCE, 0xB8, 0xCE, 0xB5, 0xCE, 0xB9];

static S_25_7: [symbol; 4] = [0xCE, 0xBF, 0xCE, 0xBA];

static S_25_8: [symbol; 6] = [0xCE, 0xBC, 0xCE, 0xB1, 0xCE, 0xBC];

static S_25_9: [symbol; 6] = [0xCE, 0xBC, 0xCE, 0xB1, 0xCE, 0xBD];

static A_25: [among; 10] = [
    among { s_size: 10, s: S_25_0.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 6, s: S_25_1.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 10, s: S_25_2.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 10, s: S_25_3.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 10, s: S_25_4.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 10, s: S_25_5.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 6, s: S_25_6.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 4, s: S_25_7.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 6, s: S_25_8.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 6, s: S_25_9.as_ptr(), substring_i: -1, result: -1, function: None },
];

static S_26_0: [symbol; 8] = [0xCE, 0xB5, 0xCE, 0xB4, 0xCE, 0xB5, 0xCF, 0x83];

static S_26_1: [symbol; 8] = [0xCE, 0xB5, 0xCE, 0xB4, 0xCF, 0x89, 0xCE, 0xBD];

static A_26: [among; 2] = [
    among { s_size: 8, s: S_26_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_26_1.as_ptr(), substring_i: -1, result: 1, function: None },
];

static S_27_0: [symbol; 10] = [0xCE, 0xBA, 0xCF, 0x81, 0xCE, 0xB1, 0xCF, 0x83, 0xCF, 0x80];

static S_27_1: [symbol; 4] = [0xCF, 0x85, 0xCF, 0x80];

static S_27_2: [symbol; 6] = [0xCE, 0xB4, 0xCE, 0xB1, 0xCF, 0x80];

static S_27_3: [symbol; 6] = [0xCE, 0xB3, 0xCE, 0xB7, 0xCF, 0x80];

static S_27_4: [symbol; 4] = [0xCE, 0xB9, 0xCF, 0x80];

static S_27_5: [symbol; 6] = [0xCE, 0xB5, 0xCE, 0xBC, 0xCF, 0x80];

static S_27_6: [symbol; 4] = [0xCE, 0xBF, 0xCF, 0x80];

static S_27_7: [symbol; 6] = [0xCE, 0xBC, 0xCE, 0xB9, 0xCE, 0xBB];

static A_27: [among; 8] = [
    among { s_size: 10, s: S_27_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_27_1.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_27_2.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_27_3.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_27_4.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_27_5.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_27_6.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_27_7.as_ptr(), substring_i: -1, result: 1, function: None },
];

static S_28_0: [symbol; 10] = [0xCE, 0xBF, 0xCF, 0x85, 0xCE, 0xB4, 0xCE, 0xB5, 0xCF, 0x83];

static S_28_1: [symbol; 10] = [0xCE, 0xBF, 0xCF, 0x85, 0xCE, 0xB4, 0xCF, 0x89, 0xCE, 0xBD];

static A_28: [among; 2] = [
    among { s_size: 10, s: S_28_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 10, s: S_28_1.as_ptr(), substring_i: -1, result: 1, function: None },
];

static S_29_0: [symbol; 4] = [0xCF, 0x83, 0xCF, 0x80];

static S_29_1: [symbol; 4] = [0xCF, 0x86, 0xCF, 0x81];

static S_29_2: [symbol; 2] = [0xCF, 0x83];

static S_29_3: [symbol; 6] = [0xCE, 0xBB, 0xCE, 0xB9, 0xCF, 0x87];

static S_29_4: [symbol; 8] = [0xCF, 0x84, 0xCF, 0x81, 0xCE, 0xB1, 0xCE, 0xB3];

static S_29_5: [symbol; 4] = [0xCF, 0x86, 0xCE, 0xB5];

static S_29_6: [symbol; 6] = [0xCE, 0xB1, 0xCF, 0x81, 0xCE, 0xBA];

static S_29_7: [symbol; 4] = [0xCF, 0x83, 0xCE, 0xBA];

static S_29_8: [symbol; 12] = [0xCE, 0xBA, 0xCE, 0xB1, 0xCE, 0xBB, 0xCE, 0xB9, 0xCE, 0xB1, 0xCE, 0xBA];

static S_29_9: [symbol; 8] = [0xCE, 0xBB, 0xCE, 0xBF, 0xCF, 0x85, 0xCE, 0xBB];

static S_29_10: [symbol; 4] = [0xCF, 0x86, 0xCE, 0xBB];

static S_29_11: [symbol; 10] = [0xCF, 0x80, 0xCE, 0xB5, 0xCF, 0x84, 0xCE, 0xB1, 0xCE, 0xBB];

static S_29_12: [symbol; 6] = [0xCE, 0xB2, 0xCE, 0xB5, 0xCE, 0xBB];

static S_29_13: [symbol; 4] = [0xCF, 0x87, 0xCE, 0xBD];

static S_29_14: [symbol; 8] = [0xCF, 0x80, 0xCE, 0xBB, 0xCE, 0xB5, 0xCE, 0xBE];

static A_29: [among; 15] = [
    among { s_size: 4, s: S_29_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_29_1.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_29_2.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_29_3.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_29_4.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_29_5.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_29_6.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_29_7.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 12, s: S_29_8.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_29_9.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_29_10.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 10, s: S_29_11.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_29_12.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_29_13.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_29_14.as_ptr(), substring_i: -1, result: 1, function: None },
];

static S_30_0: [symbol; 6] = [0xCE, 0xB5, 0xCF, 0x89, 0xCF, 0x83];

static S_30_1: [symbol; 6] = [0xCE, 0xB5, 0xCF, 0x89, 0xCE, 0xBD];

static A_30: [among; 2] = [
    among { s_size: 6, s: S_30_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_30_1.as_ptr(), substring_i: -1, result: 1, function: None },
];

static S_31_0: [symbol; 2] = [0xCF, 0x80];

static S_31_1: [symbol; 6] = [0xCF, 0x80, 0xCE, 0xB1, 0xCF, 0x81];

static S_31_2: [symbol; 2] = [0xCE, 0xB4];

static S_31_3: [symbol; 4] = [0xCE, 0xB9, 0xCE, 0xB4];

static S_31_4: [symbol; 2] = [0xCE, 0xB8];

static S_31_5: [symbol; 6] = [0xCE, 0xB3, 0xCE, 0xB1, 0xCE, 0xBB];

static S_31_6: [symbol; 4] = [0xCE, 0xB5, 0xCE, 0xBB];

static S_31_7: [symbol; 2] = [0xCE, 0xBD];

static A_31: [among; 8] = [
    among { s_size: 2, s: S_31_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_31_1.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_31_2.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_31_3.as_ptr(), substring_i: 2, result: 1, function: None },
    among { s_size: 2, s: S_31_4.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_31_5.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_31_6.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_31_7.as_ptr(), substring_i: -1, result: 1, function: None },
];

static S_32_0: [symbol; 6] = [0xCE, 0xB9, 0xCE, 0xBF, 0xCF, 0x85];

static S_32_1: [symbol; 4] = [0xCE, 0xB9, 0xCE, 0xB1];

static S_32_2: [symbol; 6] = [0xCE, 0xB9, 0xCF, 0x89, 0xCE, 0xBD];

static A_32: [among; 3] = [
    among { s_size: 6, s: S_32_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_32_1.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_32_2.as_ptr(), substring_i: -1, result: 1, function: None },
];

static S_33_0: [symbol; 8] = [0xCE, 0xB9, 0xCE, 0xBA, 0xCE, 0xBF, 0xCF, 0x85];

static S_33_1: [symbol; 6] = [0xCE, 0xB9, 0xCE, 0xBA, 0xCE, 0xB1];

static S_33_2: [symbol; 8] = [0xCE, 0xB9, 0xCE, 0xBA, 0xCF, 0x89, 0xCE, 0xBD];

static S_33_3: [symbol; 6] = [0xCE, 0xB9, 0xCE, 0xBA, 0xCE, 0xBF];

static A_33: [among; 4] = [
    among { s_size: 8, s: S_33_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_33_1.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_33_2.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_33_3.as_ptr(), substring_i: -1, result: 1, function: None },
];

static S_34_0: [symbol; 8] = [0xCE, 0xBA, 0xCE, 0xB1, 0xCE, 0xBB, 0xCF, 0x80];

static S_34_1: [symbol; 6] = [0xCE, 0xB3, 0xCE, 0xB5, 0xCF, 0x81];

static S_34_2: [symbol; 12] = [0xCF, 0x80, 0xCE, 0xBB, 0xCE, 0xB9, 0xCE, 0xB1, 0xCF, 0x84, 0xCF, 0x83];

static S_34_3: [symbol; 8] = [0xCF, 0x80, 0xCE, 0xB5, 0xCF, 0x84, 0xCF, 0x83];

static S_34_4: [symbol; 8] = [0xCF, 0x80, 0xCE, 0xB9, 0xCF, 0x84, 0xCF, 0x83];

static S_34_5: [symbol; 6] = [0xCF, 0x86, 0xCF, 0x85, 0xCF, 0x83];

static S_34_6: [symbol; 6] = [0xCF, 0x87, 0xCE, 0xB1, 0xCF, 0x83];

static S_34_7: [symbol; 8] = [0xCE, 0xBC, 0xCF, 0x80, 0xCE, 0xBF, 0xCF, 0x83];

static S_34_8: [symbol; 8] = [0xCF, 0x83, 0xCE, 0xB5, 0xCF, 0x81, 0xCF, 0x84];

static S_34_9: [symbol; 14] = [0xCE, 0xBC, 0xCF, 0x80, 0xCE, 0xB1, 0xCE, 0xB3, 0xCE, 0xB9, 0xCE, 0xB1, 0xCF, 0x84];

static S_34_10: [symbol; 6] = [0xCE, 0xBD, 0xCE, 0xB9, 0xCF, 0x84];

static S_34_11: [symbol; 12] = [0xCF, 0x80, 0xCE, 0xB9, 0xCE, 0xBA, 0xCE, 0xB1, 0xCE, 0xBD, 0xCF, 0x84];

static S_34_12: [symbol; 8] = [0xCE, 0xB5, 0xCE, 0xBE, 0xCF, 0x89, 0xCE, 0xB4];

static S_34_13: [symbol; 4] = [0xCE, 0xB1, 0xCE, 0xB4];

static S_34_14: [symbol; 10] = [0xCE, 0xBA, 0xCE, 0xB1, 0xCF, 0x84, 0xCE, 0xB1, 0xCE, 0xB4];

static S_34_15: [symbol; 10] = [0xCF, 0x83, 0xCF, 0x85, 0xCE, 0xBD, 0xCE, 0xB1, 0xCE, 0xB4];

static S_34_16: [symbol; 10] = [0xCE, 0xB1, 0xCE, 0xBD, 0xCF, 0x84, 0xCE, 0xB9, 0xCE, 0xB4];

static S_34_17: [symbol; 6] = [0xCE, 0xB5, 0xCE, 0xBD, 0xCE, 0xB4];

static S_34_18: [symbol; 8] = [0xCF, 0x85, 0xCF, 0x80, 0xCE, 0xBF, 0xCE, 0xB4];

static S_34_19: [symbol; 12] = [0xCF, 0x80, 0xCF, 0x81, 0xCF, 0x89, 0xCF, 0x84, 0xCE, 0xBF, 0xCE, 0xB4];

static S_34_20: [symbol; 10] = [0xCF, 0x86, 0xCF, 0x85, 0xCE, 0xBB, 0xCE, 0xBF, 0xCE, 0xB4];

static S_34_21: [symbol; 4] = [0xCE, 0xB7, 0xCE, 0xB8];

static S_34_22: [symbol; 8] = [0xCE, 0xB1, 0xCE, 0xBD, 0xCE, 0xB7, 0xCE, 0xB8];

static S_34_23: [symbol; 6] = [0xCE, 0xBE, 0xCE, 0xB9, 0xCE, 0xBA];

static S_34_24: [symbol; 8] = [0xCE, 0xBC, 0xCE, 0xBF, 0xCF, 0x85, 0xCE, 0xBB];

static S_34_25: [symbol; 4] = [0xCE, 0xB1, 0xCE, 0xBB];

static S_34_26: [symbol; 14] = [0xCE, 0xB1, 0xCE, 0xBC, 0xCE, 0xBC, 0xCE, 0xBF, 0xCF, 0x87, 0xCE, 0xB1, 0xCE, 0xBB];

static S_34_27: [symbol; 14] = [0xCF, 0x83, 0xCF, 0x85, 0xCE, 0xBD, 0xCE, 0xBF, 0xCE, 0xBC, 0xCE, 0xB7, 0xCE, 0xBB];

static S_34_28: [symbol; 8] = [0xCE, 0xBC, 0xCF, 0x80, 0xCE, 0xBF, 0xCE, 0xBB];

static S_34_29: [symbol; 8] = [0xCE, 0xB2, 0xCF, 0x81, 0xCF, 0x89, 0xCE, 0xBC];

static S_34_30: [symbol; 8] = [0xCF, 0x84, 0xCF, 0x83, 0xCE, 0xB1, 0xCE, 0xBC];

static S_34_31: [symbol; 8] = [0xCE, 0xBC, 0xCF, 0x80, 0xCE, 0xB1, 0xCE, 0xBD];

static S_34_32: [symbol; 8] = [0xCE, 0xB1, 0xCE, 0xBC, 0xCE, 0xB1, 0xCE, 0xBD];

static S_34_33: [symbol; 12] = [0xCE, 0xBA, 0xCE, 0xB1, 0xCE, 0xBB, 0xCE, 0xBB, 0xCE, 0xB9, 0xCE, 0xBD];

static S_34_34: [symbol; 14] = [0xCF, 0x80, 0xCE, 0xBF, 0xCF, 0x83, 0xCF, 0x84, 0xCE, 0xB5, 0xCE, 0xBB, 0xCE, 0xBD];

static S_34_35: [symbol; 10] = [0xCF, 0x86, 0xCE, 0xB9, 0xCE, 0xBB, 0xCE, 0xBF, 0xCE, 0xBD];

static A_34: [among; 36] = [
    among { s_size: 8, s: S_34_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_34_1.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 12, s: S_34_2.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_34_3.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_34_4.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_34_5.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_34_6.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_34_7.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_34_8.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 14, s: S_34_9.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_34_10.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 12, s: S_34_11.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_34_12.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_34_13.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 10, s: S_34_14.as_ptr(), substring_i: 13, result: 1, function: None },
    among { s_size: 10, s: S_34_15.as_ptr(), substring_i: 13, result: 1, function: None },
    among { s_size: 10, s: S_34_16.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_34_17.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_34_18.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 12, s: S_34_19.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 10, s: S_34_20.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_34_21.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_34_22.as_ptr(), substring_i: 21, result: 1, function: None },
    among { s_size: 6, s: S_34_23.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_34_24.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_34_25.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 14, s: S_34_26.as_ptr(), substring_i: 25, result: 1, function: None },
    among { s_size: 14, s: S_34_27.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_34_28.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_34_29.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_34_30.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_34_31.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_34_32.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 12, s: S_34_33.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 14, s: S_34_34.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 10, s: S_34_35.as_ptr(), substring_i: -1, result: 1, function: None },
];

static S_35_0: [symbol; 12] = [0xCE, 0xBF, 0xCF, 0x85, 0xCF, 0x83, 0xCE, 0xB1, 0xCE, 0xBC, 0xCE, 0xB5];

static S_35_1: [symbol; 10] = [0xCE, 0xB7, 0xCF, 0x83, 0xCE, 0xB1, 0xCE, 0xBC, 0xCE, 0xB5];

static S_35_2: [symbol; 10] = [0xCE, 0xB1, 0xCE, 0xB3, 0xCE, 0xB1, 0xCE, 0xBC, 0xCE, 0xB5];

static S_35_3: [symbol; 10] = [0xCE, 0xB7, 0xCE, 0xBA, 0xCE, 0xB1, 0xCE, 0xBC, 0xCE, 0xB5];

static S_35_4: [symbol; 14] = [0xCE, 0xB7, 0xCE, 0xB8, 0xCE, 0xB7, 0xCE, 0xBA, 0xCE, 0xB1, 0xCE, 0xBC, 0xCE, 0xB5];

static A_35: [among; 5] = [
    among { s_size: 12, s: S_35_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 10, s: S_35_1.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 10, s: S_35_2.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 10, s: S_35_3.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 14, s: S_35_4.as_ptr(), substring_i: 3, result: 1, function: None },
];

static S_36_0: [symbol; 8] = [0xCE, 0xB1, 0xCE, 0xBD, 0xCE, 0xB1, 0xCF, 0x80];

static S_36_1: [symbol; 8] = [0xCF, 0x80, 0xCE, 0xB9, 0xCE, 0xBA, 0xCF, 0x81];

static S_36_2: [symbol; 10] = [0xCE, 0xB1, 0xCF, 0x80, 0xCE, 0xBF, 0xCF, 0x83, 0xCF, 0x84];

static S_36_3: [symbol; 6] = [0xCF, 0x80, 0xCE, 0xBF, 0xCF, 0x84];

static S_36_4: [symbol; 2] = [0xCF, 0x87];

static S_36_5: [symbol; 6] = [0xCF, 0x83, 0xCE, 0xB9, 0xCF, 0x87];

static S_36_6: [symbol; 8] = [0xCE, 0xB2, 0xCE, 0xBF, 0xCF, 0x85, 0xCE, 0xB2];

static S_36_7: [symbol; 6] = [0xCF, 0x80, 0xCE, 0xB5, 0xCE, 0xB8];

static S_36_8: [symbol; 6] = [0xCE, 0xBE, 0xCE, 0xB5, 0xCE, 0xB8];

static S_36_9: [symbol; 8] = [0xCE, 0xB1, 0xCF, 0x80, 0xCE, 0xBF, 0xCE, 0xB8];

static S_36_10: [symbol; 8] = [0xCE, 0xB1, 0xCF, 0x80, 0xCE, 0xBF, 0xCE, 0xBA];

static S_36_11: [symbol; 6] = [0xCE, 0xBF, 0xCF, 0x85, 0xCE, 0xBB];

static A_36: [among; 12] = [
    among { s_size: 8, s: S_36_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_36_1.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 10, s: S_36_2.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_36_3.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_36_4.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_36_5.as_ptr(), substring_i: 4, result: 1, function: None },
    among { s_size: 8, s: S_36_6.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_36_7.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_36_8.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_36_9.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_36_10.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_36_11.as_ptr(), substring_i: -1, result: 1, function: None },
];

static S_37_0: [symbol; 4] = [0xCF, 0x84, 0xCF, 0x81];

static S_37_1: [symbol; 4] = [0xCF, 0x84, 0xCF, 0x83];

static A_37: [among; 2] = [
    among { s_size: 4, s: S_37_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_37_1.as_ptr(), substring_i: -1, result: 1, function: None },
];

static S_38_0: [symbol; 12] = [0xCE, 0xBF, 0xCF, 0x85, 0xCF, 0x83, 0xCE, 0xB1, 0xCE, 0xBD, 0xCE, 0xB5];

static S_38_1: [symbol; 10] = [0xCE, 0xB7, 0xCF, 0x83, 0xCE, 0xB1, 0xCE, 0xBD, 0xCE, 0xB5];

static S_38_2: [symbol; 14] = [0xCE, 0xBF, 0xCF, 0x85, 0xCE, 0xBD, 0xCF, 0x84, 0xCE, 0xB1, 0xCE, 0xBD, 0xCE, 0xB5];

static S_38_3: [symbol; 16] = [0xCE, 0xB9, 0xCE, 0xBF, 0xCF, 0x85, 0xCE, 0xBD, 0xCF, 0x84, 0xCE, 0xB1, 0xCE, 0xBD, 0xCE, 0xB5];

static S_38_4: [symbol; 12] = [0xCE, 0xBF, 0xCE, 0xBD, 0xCF, 0x84, 0xCE, 0xB1, 0xCE, 0xBD, 0xCE, 0xB5];

static S_38_5: [symbol; 14] = [0xCE, 0xB9, 0xCE, 0xBF, 0xCE, 0xBD, 0xCF, 0x84, 0xCE, 0xB1, 0xCE, 0xBD, 0xCE, 0xB5];

static S_38_6: [symbol; 10] = [0xCE, 0xBF, 0xCF, 0x84, 0xCE, 0xB1, 0xCE, 0xBD, 0xCE, 0xB5];

static S_38_7: [symbol; 12] = [0xCE, 0xB9, 0xCE, 0xBF, 0xCF, 0x84, 0xCE, 0xB1, 0xCE, 0xBD, 0xCE, 0xB5];

static S_38_8: [symbol; 10] = [0xCE, 0xB1, 0xCE, 0xB3, 0xCE, 0xB1, 0xCE, 0xBD, 0xCE, 0xB5];

static S_38_9: [symbol; 10] = [0xCE, 0xB7, 0xCE, 0xBA, 0xCE, 0xB1, 0xCE, 0xBD, 0xCE, 0xB5];

static S_38_10: [symbol; 14] = [0xCE, 0xB7, 0xCE, 0xB8, 0xCE, 0xB7, 0xCE, 0xBA, 0xCE, 0xB1, 0xCE, 0xBD, 0xCE, 0xB5];

static A_38: [among; 11] = [
    among { s_size: 12, s: S_38_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 10, s: S_38_1.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 14, s: S_38_2.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 16, s: S_38_3.as_ptr(), substring_i: 2, result: 1, function: None },
    among { s_size: 12, s: S_38_4.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 14, s: S_38_5.as_ptr(), substring_i: 4, result: 1, function: None },
    among { s_size: 10, s: S_38_6.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 12, s: S_38_7.as_ptr(), substring_i: 6, result: 1, function: None },
    among { s_size: 10, s: S_38_8.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 10, s: S_38_9.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 14, s: S_38_10.as_ptr(), substring_i: 9, result: 1, function: None },
];

static S_39_0: [symbol; 2] = [0xCF, 0x80];

static S_39_1: [symbol; 4] = [0xCF, 0x83, 0xCF, 0x80];

static S_39_2: [symbol; 14] = [0xCF, 0x80, 0xCE, 0xBF, 0xCE, 0xBB, 0xCF, 0x85, 0xCE, 0xB4, 0xCE, 0xB1, 0xCF, 0x80];

static S_39_3: [symbol; 8] = [0xCE, 0xB1, 0xCE, 0xB4, 0xCE, 0xB1, 0xCF, 0x80];

static S_39_4: [symbol; 18] = [0xCF, 0x87, 0xCE, 0xB1, 0xCE, 0xBC, 0xCE, 0xB7, 0xCE, 0xBB, 0xCE, 0xBF, 0xCE, 0xB4, 0xCE, 0xB1, 0xCF, 0x80];

static S_39_5: [symbol; 8] = [0xCF, 0x84, 0xCF, 0x83, 0xCE, 0xBF, 0xCF, 0x80];

static S_39_6: [symbol; 6] = [0xCE, 0xBA, 0xCE, 0xBF, 0xCF, 0x80];

static S_39_7: [symbol; 12] = [0xCF, 0x85, 0xCF, 0x80, 0xCE, 0xBF, 0xCE, 0xBA, 0xCE, 0xBF, 0xCF, 0x80];

static S_39_8: [symbol; 12] = [0xCF, 0x80, 0xCE, 0xB5, 0xCF, 0x81, 0xCE, 0xB9, 0xCF, 0x84, 0xCF, 0x81];

static S_39_9: [symbol; 6] = [0xCE, 0xBF, 0xCF, 0x85, 0xCF, 0x81];

static S_39_10: [symbol; 4] = [0xCE, 0xB5, 0xCF, 0x81];

static S_39_11: [symbol; 10] = [0xCE, 0xB2, 0xCE, 0xB5, 0xCF, 0x84, 0xCE, 0xB5, 0xCF, 0x81];

static S_39_12: [symbol; 6] = [0xCE, 0xB3, 0xCE, 0xB5, 0xCF, 0x81];

static S_39_13: [symbol; 12] = [0xCE, 0xBB, 0xCE, 0xBF, 0xCF, 0x85, 0xCE, 0xB8, 0xCE, 0xB7, 0xCF, 0x81];

static S_39_14: [symbol; 12] = [0xCE, 0xBA, 0xCE, 0xBF, 0xCF, 0x81, 0xCE, 0xBC, 0xCE, 0xBF, 0xCF, 0x81];

static S_39_15: [symbol; 2] = [0xCF, 0x83];

static S_39_16: [symbol; 16] = [0xCF, 0x83, 0xCE, 0xB1, 0xCF, 0x81, 0xCE, 0xB1, 0xCE, 0xBA, 0xCE, 0xB1, 0xCF, 0x84, 0xCF, 0x83];

static S_39_17: [symbol; 6] = [0xCE, 0xB8, 0xCF, 0x85, 0xCF, 0x83];

static S_39_18: [symbol; 6] = [0xCE, 0xB2, 0xCE, 0xB1, 0xCF, 0x83];

static S_39_19: [symbol; 10] = [0xCF, 0x80, 0xCE, 0xBF, 0xCE, 0xBB, 0xCE, 0xB9, 0xCF, 0x83];

static S_39_20: [symbol; 8] = [0xCE, 0xBA, 0xCE, 0xB1, 0xCF, 0x83, 0xCF, 0x84];

static S_39_21: [symbol; 8] = [0xCE, 0xB4, 0xCE, 0xB9, 0xCE, 0xB1, 0xCF, 0x84];

static S_39_22: [symbol; 8] = [0xCF, 0x80, 0xCE, 0xBB, 0xCE, 0xB1, 0xCF, 0x84];

static S_39_23: [symbol; 14] = [0xCF, 0x84, 0xCF, 0x83, 0xCE, 0xB1, 0xCF, 0x81, 0xCE, 0xBB, 0xCE, 0xB1, 0xCF, 0x84];

static S_39_24: [symbol; 6] = [0xCF, 0x84, 0xCE, 0xB5, 0xCF, 0x84];

static S_39_25: [symbol; 12] = [0xCF, 0x80, 0xCE, 0xBF, 0xCF, 0x85, 0xCF, 0x81, 0xCE, 0xB9, 0xCF, 0x84];

static S_39_26: [symbol; 10] = [0xCF, 0x83, 0xCE, 0xBF, 0xCF, 0x85, 0xCE, 0xBB, 0xCF, 0x84];

static S_39_27: [symbol; 8] = [0xCE, 0xB6, 0xCF, 0x89, 0xCE, 0xBD, 0xCF, 0x84];

static S_39_28: [symbol; 10] = [0xCE, 0xBC, 0xCE, 0xB1, 0xCE, 0xB9, 0xCE, 0xBD, 0xCF, 0x84];

static S_39_29: [symbol; 2] = [0xCF, 0x86];

static S_39_30: [symbol; 14] = [0xCF, 0x80, 0xCE, 0xB5, 0xCE, 0xBD, 0xCF, 0x84, 0xCE, 0xB1, 0xCF, 0x81, 0xCF, 0x86];

static S_39_31: [symbol; 14] = [0xCE, 0xBA, 0xCE, 0xBF, 0xCE, 0xB9, 0xCE, 0xBB, 0xCE, 0xB1, 0xCF, 0x81, 0xCF, 0x86];

static S_39_32: [symbol; 6] = [0xCE, 0xBF, 0xCF, 0x81, 0xCF, 0x86];

static S_39_33: [symbol; 8] = [0xCE, 0xB4, 0xCE, 0xB9, 0xCE, 0xB1, 0xCF, 0x86];

static S_39_34: [symbol; 8] = [0xCF, 0x83, 0xCF, 0x84, 0xCE, 0xB5, 0xCF, 0x86];

static S_39_35: [symbol; 16] = [0xCF, 0x86, 0xCF, 0x89, 0xCF, 0x84, 0xCE, 0xBF, 0xCF, 0x83, 0xCF, 0x84, 0xCE, 0xB5, 0xCF, 0x86];

static S_39_36: [symbol; 10] = [0xCF, 0x80, 0xCE, 0xB5, 0xCF, 0x81, 0xCE, 0xB7, 0xCF, 0x86];

static S_39_37: [symbol; 12] = [0xCF, 0x85, 0xCF, 0x80, 0xCE, 0xB5, 0xCF, 0x81, 0xCE, 0xB7, 0xCF, 0x86];

static S_39_38: [symbol; 2] = [0xCF, 0x87];

static S_39_39: [symbol; 14] = [0xCF, 0x80, 0xCE, 0xBF, 0xCE, 0xBB, 0xCF, 0x85, 0xCE, 0xBC, 0xCE, 0xB7, 0xCF, 0x87];

static S_39_40: [symbol; 8] = [0xCE, 0xB1, 0xCE, 0xBC, 0xCE, 0xB7, 0xCF, 0x87];

static S_39_41: [symbol; 12] = [0xCE, 0xB2, 0xCE, 0xB9, 0xCE, 0xBF, 0xCE, 0xBC, 0xCE, 0xB7, 0xCF, 0x87];

static S_39_42: [symbol; 22] = [0xCE, 0xBC, 0xCE, 0xB9, 0xCE, 0xBA, 0xCF, 0x81, 0xCE, 0xBF, 0xCE, 0xB2, 0xCE, 0xB9, 0xCE, 0xBF, 0xCE, 0xBC, 0xCE, 0xB7, 0xCF, 0x87];

static S_39_43: [symbol; 22] = [0xCE, 0xBC, 0xCE, 0xB5, 0xCE, 0xB3, 0xCE, 0xBB, 0xCE, 0xBF, 0xCE, 0xB2, 0xCE, 0xB9, 0xCE, 0xBF, 0xCE, 0xBC, 0xCE, 0xB7, 0xCF, 0x87];

static S_39_44: [symbol; 22] = [0xCE, 0xBA, 0xCE, 0xB1, 0xCF, 0x80, 0xCE, 0xBD, 0xCE, 0xBF, 0xCE, 0xB2, 0xCE, 0xB9, 0xCE, 0xBF, 0xCE, 0xBC, 0xCE, 0xB7, 0xCF, 0x87];

static S_39_45: [symbol; 6] = [0xCE, 0xBB, 0xCE, 0xB9, 0xCF, 0x87];

static S_39_46: [symbol; 6] = [0xCF, 0x84, 0xCE, 0xB1, 0xCE, 0xB2];

static S_39_47: [symbol; 8] = [0xCE, 0xBD, 0xCF, 0x84, 0xCE, 0xB1, 0xCE, 0xB2];

static S_39_48: [symbol; 14] = [0xCF, 0x88, 0xCE, 0xB7, 0xCE, 0xBB, 0xCE, 0xBF, 0xCF, 0x84, 0xCE, 0xB1, 0xCE, 0xB2];

static S_39_49: [symbol; 6] = [0xCE, 0xBB, 0xCE, 0xB9, 0xCE, 0xB2];

static S_39_50: [symbol; 8] = [0xCE, 0xBA, 0xCE, 0xBB, 0xCE, 0xB9, 0xCE, 0xB2];

static S_39_51: [symbol; 16] = [0xCE, 0xBE, 0xCE, 0xB7, 0xCF, 0x81, 0xCE, 0xBF, 0xCE, 0xBA, 0xCE, 0xBB, 0xCE, 0xB9, 0xCE, 0xB2];

static S_39_52: [symbol; 2] = [0xCE, 0xB3];

static S_39_53: [symbol; 10] = [0xCE, 0xB1, 0xCE, 0xBD, 0xCE, 0xBF, 0xCF, 0x81, 0xCE, 0xB3];

static S_39_54: [symbol; 10] = [0xCE, 0xB5, 0xCE, 0xBD, 0xCE, 0xBF, 0xCF, 0x81, 0xCE, 0xB3];

static S_39_55: [symbol; 4] = [0xCE, 0xB1, 0xCE, 0xB3];

static S_39_56: [symbol; 8] = [0xCF, 0x84, 0xCF, 0x81, 0xCE, 0xB1, 0xCE, 0xB3];

static S_39_57: [symbol; 8] = [0xCF, 0x84, 0xCF, 0x83, 0xCE, 0xB1, 0xCE, 0xB3];

static S_39_58: [symbol; 10] = [0xCF, 0x84, 0xCF, 0x83, 0xCE, 0xB9, 0xCE, 0xB3, 0xCE, 0xB3];

static S_39_59: [symbol; 12] = [0xCE, 0xB1, 0xCF, 0x84, 0xCF, 0x83, 0xCE, 0xB9, 0xCE, 0xB3, 0xCE, 0xB3];

static S_39_60: [symbol; 10] = [0xCE, 0xB1, 0xCE, 0xB8, 0xCE, 0xB9, 0xCE, 0xB3, 0xCE, 0xB3];

static S_39_61: [symbol; 8] = [0xCF, 0x83, 0xCF, 0x84, 0xCE, 0xB5, 0xCE, 0xB3];

static S_39_62: [symbol; 8] = [0xCE, 0xB1, 0xCF, 0x80, 0xCE, 0xB7, 0xCE, 0xB3];

static S_39_63: [symbol; 6] = [0xCF, 0x83, 0xCE, 0xB9, 0xCE, 0xB3];

static S_39_64: [symbol; 14] = [0xCE, 0xBA, 0xCE, 0xB1, 0xCE, 0xBB, 0xCF, 0x80, 0xCE, 0xBF, 0xCF, 0x85, 0xCE, 0xB6];

static S_39_65: [symbol; 2] = [0xCE, 0xB8];

static S_39_66: [symbol; 12] = [0xCE, 0xBC, 0xCF, 0x89, 0xCE, 0xB1, 0xCE, 0xBC, 0xCE, 0xB5, 0xCE, 0xB8];

static S_39_67: [symbol; 6] = [0xCF, 0x80, 0xCE, 0xB9, 0xCE, 0xB8];

static S_39_68: [symbol; 8] = [0xCE, 0xB1, 0xCF, 0x80, 0xCE, 0xB9, 0xCE, 0xB8];

static S_39_69: [symbol; 8] = [0xCE, 0xB2, 0xCE, 0xB1, 0xCF, 0x83, 0xCE, 0xBA];

static S_39_70: [symbol; 12] = [0xCE, 0xB2, 0xCF, 0x81, 0xCE, 0xB1, 0xCF, 0x87, 0xCF, 0x85, 0xCE, 0xBA];

static S_39_71: [symbol; 6] = [0xCE, 0xB4, 0xCE, 0xB5, 0xCE, 0xBA];

static S_39_72: [symbol; 10] = [0xCF, 0x80, 0xCE, 0xB5, 0xCE, 0xBB, 0xCE, 0xB5, 0xCE, 0xBA];

static S_39_73: [symbol; 4] = [0xCE, 0xB9, 0xCE, 0xBA];

static S_39_74: [symbol; 8] = [0xCE, 0xB1, 0xCE, 0xBD, 0xCE, 0xB9, 0xCE, 0xBA];

static S_39_75: [symbol; 10] = [0xCE, 0xB2, 0xCE, 0xBF, 0xCF, 0x85, 0xCE, 0xBB, 0xCE, 0xBA];

static S_39_76: [symbol; 4] = [0xCF, 0x80, 0xCE, 0xBB];

static S_39_77: [symbol; 8] = [0xCE, 0xB4, 0xCE, 0xB9, 0xCF, 0x80, 0xCE, 0xBB];

static S_39_78: [symbol; 12] = [0xCF, 0x88, 0xCF, 0x85, 0xCF, 0x87, 0xCE, 0xBF, 0xCF, 0x80, 0xCE, 0xBB];

static S_39_79: [symbol; 10] = [0xCE, 0xBB, 0xCE, 0xB1, 0xCE, 0xBF, 0xCF, 0x80, 0xCE, 0xBB];

static S_39_80: [symbol; 6] = [0xCE, 0xBF, 0xCF, 0x85, 0xCE, 0xBB];

static S_39_81: [symbol; 6] = [0xCE, 0xB3, 0xCE, 0xB1, 0xCE, 0xBB];

static S_39_82: [symbol; 14] = [0xCE, 0xB2, 0xCE, 0xB1, 0xCE, 0xB8, 0xCF, 0x85, 0xCE, 0xB3, 0xCE, 0xB1, 0xCE, 0xBB];

static S_39_83: [symbol; 14] = [0xCE, 0xBA, 0xCE, 0xB1, 0xCF, 0x84, 0xCE, 0xB1, 0xCE, 0xB3, 0xCE, 0xB1, 0xCE, 0xBB];

static S_39_84: [symbol; 12] = [0xCE, 0xBF, 0xCE, 0xBB, 0xCE, 0xBF, 0xCE, 0xB3, 0xCE, 0xB1, 0xCE, 0xBB];

static S_39_85: [symbol; 12] = [0xCE, 0xBA, 0xCE, 0xB1, 0xCF, 0x83, 0xCF, 0x84, 0xCE, 0xB5, 0xCE, 0xBB];

static S_39_86: [symbol; 6] = [0xCE, 0xBC, 0xCE, 0xB5, 0xCE, 0xBB];

static S_39_87: [symbol; 12] = [0xCF, 0x80, 0xCE, 0xBF, 0xCF, 0x81, 0xCF, 0x84, 0xCE, 0xBF, 0xCE, 0xBB];

static S_39_88: [symbol; 2] = [0xCE, 0xBC];

static S_39_89: [symbol; 14] = [0xCE, 0xB4, 0xCF, 0x81, 0xCE, 0xB1, 0xCE, 0xB4, 0xCE, 0xBF, 0xCF, 0x85, 0xCE, 0xBC];

static S_39_90: [symbol; 10] = [0xCE, 0xB2, 0xCF, 0x81, 0xCE, 0xB1, 0xCF, 0x87, 0xCE, 0xBC];

static S_39_91: [symbol; 16] = [0xCE, 0xBF, 0xCE, 0xBB, 0xCE, 0xB9, 0xCE, 0xB3, 0xCE, 0xBF, 0xCE, 0xB4, 0xCE, 0xB1, 0xCE, 0xBC];

static S_39_92: [symbol; 16] = [0xCE, 0xBC, 0xCE, 0xBF, 0xCF, 0x85, 0xCF, 0x83, 0xCE, 0xBF, 0xCF, 0x85, 0xCE, 0xBB, 0xCE, 0xBC];

static S_39_93: [symbol; 2] = [0xCE, 0xBD];

static S_39_94: [symbol; 16] = [0xCE, 0xB1, 0xCE, 0xBC, 0xCE, 0xB5, 0xCF, 0x81, 0xCE, 0xB9, 0xCE, 0xBA, 0xCE, 0xB1, 0xCE, 0xBD];

static A_39: [among; 95] = [
    among { s_size: 2, s: S_39_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_39_1.as_ptr(), substring_i: 0, result: 1, function: None },
    among { s_size: 14, s: S_39_2.as_ptr(), substring_i: 0, result: 1, function: None },
    among { s_size: 8, s: S_39_3.as_ptr(), substring_i: 0, result: 1, function: None },
    among { s_size: 18, s: S_39_4.as_ptr(), substring_i: 0, result: 1, function: None },
    among { s_size: 8, s: S_39_5.as_ptr(), substring_i: 0, result: 1, function: None },
    among { s_size: 6, s: S_39_6.as_ptr(), substring_i: 0, result: 1, function: None },
    among { s_size: 12, s: S_39_7.as_ptr(), substring_i: 6, result: 1, function: None },
    among { s_size: 12, s: S_39_8.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_39_9.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_39_10.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 10, s: S_39_11.as_ptr(), substring_i: 10, result: 1, function: None },
    among { s_size: 6, s: S_39_12.as_ptr(), substring_i: 10, result: 1, function: None },
    among { s_size: 12, s: S_39_13.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 12, s: S_39_14.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_39_15.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 16, s: S_39_16.as_ptr(), substring_i: 15, result: 1, function: None },
    among { s_size: 6, s: S_39_17.as_ptr(), substring_i: 15, result: 1, function: None },
    among { s_size: 6, s: S_39_18.as_ptr(), substring_i: 15, result: 1, function: None },
    among { s_size: 10, s: S_39_19.as_ptr(), substring_i: 15, result: 1, function: None },
    among { s_size: 8, s: S_39_20.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_39_21.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_39_22.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 14, s: S_39_23.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_39_24.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 12, s: S_39_25.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 10, s: S_39_26.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_39_27.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 10, s: S_39_28.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_39_29.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 14, s: S_39_30.as_ptr(), substring_i: 29, result: 1, function: None },
    among { s_size: 14, s: S_39_31.as_ptr(), substring_i: 29, result: 1, function: None },
    among { s_size: 6, s: S_39_32.as_ptr(), substring_i: 29, result: 1, function: None },
    among { s_size: 8, s: S_39_33.as_ptr(), substring_i: 29, result: 1, function: None },
    among { s_size: 8, s: S_39_34.as_ptr(), substring_i: 29, result: 1, function: None },
    among { s_size: 16, s: S_39_35.as_ptr(), substring_i: 34, result: 1, function: None },
    among { s_size: 10, s: S_39_36.as_ptr(), substring_i: 29, result: 1, function: None },
    among { s_size: 12, s: S_39_37.as_ptr(), substring_i: 36, result: 1, function: None },
    among { s_size: 2, s: S_39_38.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 14, s: S_39_39.as_ptr(), substring_i: 38, result: 1, function: None },
    among { s_size: 8, s: S_39_40.as_ptr(), substring_i: 38, result: 1, function: None },
    among { s_size: 12, s: S_39_41.as_ptr(), substring_i: 38, result: 1, function: None },
    among { s_size: 22, s: S_39_42.as_ptr(), substring_i: 41, result: 1, function: None },
    among { s_size: 22, s: S_39_43.as_ptr(), substring_i: 41, result: 1, function: None },
    among { s_size: 22, s: S_39_44.as_ptr(), substring_i: 41, result: 1, function: None },
    among { s_size: 6, s: S_39_45.as_ptr(), substring_i: 38, result: 1, function: None },
    among { s_size: 6, s: S_39_46.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_39_47.as_ptr(), substring_i: 46, result: 1, function: None },
    among { s_size: 14, s: S_39_48.as_ptr(), substring_i: 46, result: 1, function: None },
    among { s_size: 6, s: S_39_49.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_39_50.as_ptr(), substring_i: 49, result: 1, function: None },
    among { s_size: 16, s: S_39_51.as_ptr(), substring_i: 50, result: 1, function: None },
    among { s_size: 2, s: S_39_52.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 10, s: S_39_53.as_ptr(), substring_i: 52, result: 1, function: None },
    among { s_size: 10, s: S_39_54.as_ptr(), substring_i: 52, result: 1, function: None },
    among { s_size: 4, s: S_39_55.as_ptr(), substring_i: 52, result: 1, function: None },
    among { s_size: 8, s: S_39_56.as_ptr(), substring_i: 55, result: 1, function: None },
    among { s_size: 8, s: S_39_57.as_ptr(), substring_i: 55, result: 1, function: None },
    among { s_size: 10, s: S_39_58.as_ptr(), substring_i: 52, result: 1, function: None },
    among { s_size: 12, s: S_39_59.as_ptr(), substring_i: 58, result: 1, function: None },
    among { s_size: 10, s: S_39_60.as_ptr(), substring_i: 52, result: 1, function: None },
    among { s_size: 8, s: S_39_61.as_ptr(), substring_i: 52, result: 1, function: None },
    among { s_size: 8, s: S_39_62.as_ptr(), substring_i: 52, result: 1, function: None },
    among { s_size: 6, s: S_39_63.as_ptr(), substring_i: 52, result: 1, function: None },
    among { s_size: 14, s: S_39_64.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_39_65.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 12, s: S_39_66.as_ptr(), substring_i: 65, result: 1, function: None },
    among { s_size: 6, s: S_39_67.as_ptr(), substring_i: 65, result: 1, function: None },
    among { s_size: 8, s: S_39_68.as_ptr(), substring_i: 67, result: 1, function: None },
    among { s_size: 8, s: S_39_69.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 12, s: S_39_70.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_39_71.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 10, s: S_39_72.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_39_73.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_39_74.as_ptr(), substring_i: 73, result: 1, function: None },
    among { s_size: 10, s: S_39_75.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_39_76.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_39_77.as_ptr(), substring_i: 76, result: 1, function: None },
    among { s_size: 12, s: S_39_78.as_ptr(), substring_i: 76, result: 1, function: None },
    among { s_size: 10, s: S_39_79.as_ptr(), substring_i: 76, result: 1, function: None },
    among { s_size: 6, s: S_39_80.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_39_81.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 14, s: S_39_82.as_ptr(), substring_i: 81, result: 1, function: None },
    among { s_size: 14, s: S_39_83.as_ptr(), substring_i: 81, result: 1, function: None },
    among { s_size: 12, s: S_39_84.as_ptr(), substring_i: 81, result: 1, function: None },
    among { s_size: 12, s: S_39_85.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_39_86.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 12, s: S_39_87.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_39_88.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 14, s: S_39_89.as_ptr(), substring_i: 88, result: 1, function: None },
    among { s_size: 10, s: S_39_90.as_ptr(), substring_i: 88, result: 1, function: None },
    among { s_size: 16, s: S_39_91.as_ptr(), substring_i: 88, result: 1, function: None },
    among { s_size: 16, s: S_39_92.as_ptr(), substring_i: 88, result: 1, function: None },
    among { s_size: 2, s: S_39_93.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 16, s: S_39_94.as_ptr(), substring_i: 93, result: 1, function: None },
];

static S_40_0: [symbol; 10] = [0xCE, 0xB7, 0xCF, 0x83, 0xCE, 0xB5, 0xCF, 0x84, 0xCE, 0xB5];

static A_40: [among; 1] = [
    among { s_size: 10, s: S_40_0.as_ptr(), substring_i: -1, result: 1, function: None },
];

static S_41_0: [symbol; 6] = [0xCF, 0x80, 0xCF, 0x85, 0xCF, 0x81];

static S_41_1: [symbol; 6] = [0xCE, 0xB5, 0xCF, 0x85, 0xCF, 0x81];

static S_41_2: [symbol; 6] = [0xCF, 0x87, 0xCF, 0x89, 0xCF, 0x81];

static S_41_3: [symbol; 6] = [0xCE, 0xB2, 0xCE, 0xB1, 0xCF, 0x81];

static S_41_4: [symbol; 4] = [0xCE, 0xB2, 0xCF, 0x81];

static S_41_5: [symbol; 6] = [0xCE, 0xB1, 0xCE, 0xB9, 0xCF, 0x81];

static S_41_6: [symbol; 6] = [0xCF, 0x86, 0xCE, 0xBF, 0xCF, 0x81];

static S_41_7: [symbol; 6] = [0xCE, 0xBD, 0xCE, 0xB5, 0xCF, 0x84];

static S_41_8: [symbol; 4] = [0xCF, 0x83, 0xCF, 0x87];

static S_41_9: [symbol; 8] = [0xCF, 0x83, 0xCF, 0x85, 0xCE, 0xBD, 0xCE, 0xB4];

static S_41_10: [symbol; 6] = [0xCE, 0xB5, 0xCE, 0xBD, 0xCE, 0xB4];

static S_41_11: [symbol; 4] = [0xCE, 0xBF, 0xCE, 0xB4];

static S_41_12: [symbol; 10] = [0xCF, 0x85, 0xCF, 0x80, 0xCE, 0xB5, 0xCF, 0x81, 0xCE, 0xB8];

static S_41_13: [symbol; 4] = [0xCF, 0x83, 0xCE, 0xB8];

static S_41_14: [symbol; 6] = [0xCE, 0xB5, 0xCF, 0x85, 0xCE, 0xB8];

static S_41_15: [symbol; 6] = [0xCF, 0x81, 0xCE, 0xB1, 0xCE, 0xB8];

static S_41_16: [symbol; 6] = [0xCF, 0x84, 0xCE, 0xB1, 0xCE, 0xB8];

static S_41_17: [symbol; 8] = [0xCE, 0xB4, 0xCE, 0xB9, 0xCE, 0xB1, 0xCE, 0xB8];

static S_41_18: [symbol; 6] = [0xCE, 0xBA, 0xCE, 0xB1, 0xCE, 0xB8];

static S_41_19: [symbol; 6] = [0xCF, 0x84, 0xCE, 0xB9, 0xCE, 0xB8];

static S_41_20: [symbol; 6] = [0xCE, 0xB5, 0xCE, 0xBA, 0xCE, 0xB8];

static S_41_21: [symbol; 8] = [0xCF, 0x83, 0xCF, 0x85, 0xCE, 0xBD, 0xCE, 0xB8];

static S_41_22: [symbol; 6] = [0xCE, 0xB5, 0xCE, 0xBD, 0xCE, 0xB8];

static S_41_23: [symbol; 6] = [0xCF, 0x81, 0xCE, 0xBF, 0xCE, 0xB8];

static S_41_24: [symbol; 6] = [0xCE, 0xB1, 0xCF, 0x81, 0xCE, 0xBA];

static S_41_25: [symbol; 8] = [0xCF, 0x89, 0xCF, 0x86, 0xCE, 0xB5, 0xCE, 0xBB];

static S_41_26: [symbol; 6] = [0xCE, 0xB2, 0xCE, 0xBF, 0xCE, 0xBB];

static S_41_27: [symbol; 6] = [0xCF, 0x83, 0xCF, 0x85, 0xCE, 0xBD];

static S_41_28: [symbol; 6] = [0xCE, 0xB1, 0xCE, 0xB9, 0xCE, 0xBD];

static S_41_29: [symbol; 6] = [0xCF, 0x80, 0xCE, 0xBF, 0xCE, 0xBD];

static S_41_30: [symbol; 6] = [0xCF, 0x81, 0xCE, 0xBF, 0xCE, 0xBD];

static A_41: [among; 31] = [
    among { s_size: 6, s: S_41_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_41_1.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_41_2.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_41_3.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_41_4.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_41_5.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_41_6.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_41_7.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_41_8.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_41_9.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_41_10.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_41_11.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 10, s: S_41_12.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_41_13.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_41_14.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_41_15.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_41_16.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_41_17.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_41_18.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_41_19.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_41_20.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_41_21.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_41_22.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_41_23.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_41_24.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_41_25.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_41_26.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_41_27.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_41_28.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_41_29.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_41_30.as_ptr(), substring_i: -1, result: 1, function: None },
];

static S_42_0: [symbol; 8] = [0xCF, 0x83, 0xCE, 0xB5, 0xCF, 0x81, 0xCF, 0x80];

static S_42_1: [symbol; 6] = [0xCE, 0xBA, 0xCE, 0xBF, 0xCF, 0x80];

static S_42_2: [symbol; 8] = [0xCE, 0xB8, 0xCE, 0xB1, 0xCF, 0x81, 0xCF, 0x81];

static S_42_3: [symbol; 6] = [0xCE, 0xBD, 0xCF, 0x84, 0xCF, 0x81];

static S_42_4: [symbol; 8] = [0xCE, 0xB1, 0xCE, 0xB2, 0xCE, 0xB1, 0xCF, 0x81];

static S_42_5: [symbol; 8] = [0xCE, 0xB5, 0xCE, 0xBD, 0xCE, 0xB1, 0xCF, 0x81];

static S_42_6: [symbol; 6] = [0xCE, 0xB1, 0xCE, 0xB2, 0xCF, 0x81];

static S_42_7: [symbol; 8] = [0xCE, 0xBC, 0xCF, 0x80, 0xCE, 0xBF, 0xCF, 0x81];

static S_42_8: [symbol; 2] = [0xCF, 0x85];

static S_42_9: [symbol; 8] = [0xCF, 0x83, 0xCF, 0x85, 0xCF, 0x81, 0xCF, 0x86];

static S_42_10: [symbol; 6] = [0xCE, 0xBD, 0xCE, 0xB9, 0xCF, 0x86];

static S_42_11: [symbol; 6] = [0xCF, 0x80, 0xCE, 0xB1, 0xCE, 0xB3];

static S_42_12: [symbol; 2] = [0xCE, 0xB4];

static S_42_13: [symbol; 4] = [0xCE, 0xB1, 0xCE, 0xB4];

static S_42_14: [symbol; 2] = [0xCE, 0xB8];

static S_42_15: [symbol; 4] = [0xCE, 0xB1, 0xCE, 0xB8];

static S_42_16: [symbol; 4] = [0xCF, 0x83, 0xCE, 0xBA];

static S_42_17: [symbol; 6] = [0xCF, 0x84, 0xCE, 0xBF, 0xCE, 0xBA];

static S_42_18: [symbol; 6] = [0xCE, 0xB1, 0xCF, 0x80, 0xCE, 0xBB];

static S_42_19: [symbol; 14] = [0xCF, 0x80, 0xCE, 0xB1, 0xCF, 0x81, 0xCE, 0xB1, 0xCE, 0xBA, 0xCE, 0xB1, 0xCE, 0xBB];

static S_42_20: [symbol; 8] = [0xCF, 0x83, 0xCE, 0xBA, 0xCE, 0xB5, 0xCE, 0xBB];

static S_42_21: [symbol; 4] = [0xCE, 0xB5, 0xCE, 0xBC];

static S_42_22: [symbol; 4] = [0xCE, 0xB1, 0xCE, 0xBD];

static S_42_23: [symbol; 6] = [0xCE, 0xB2, 0xCE, 0xB5, 0xCE, 0xBD];

static S_42_24: [symbol; 10] = [0xCE, 0xB2, 0xCE, 0xB1, 0xCF, 0x81, 0xCE, 0xBF, 0xCE, 0xBD];

static A_42: [among; 25] = [
    among { s_size: 8, s: S_42_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_42_1.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_42_2.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_42_3.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_42_4.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_42_5.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_42_6.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_42_7.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_42_8.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_42_9.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_42_10.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_42_11.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_42_12.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_42_13.as_ptr(), substring_i: 12, result: 1, function: None },
    among { s_size: 2, s: S_42_14.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_42_15.as_ptr(), substring_i: 14, result: 1, function: None },
    among { s_size: 4, s: S_42_16.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_42_17.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_42_18.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 14, s: S_42_19.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_42_20.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_42_21.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_42_22.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_42_23.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 10, s: S_42_24.as_ptr(), substring_i: -1, result: 1, function: None },
];

static S_43_0: [symbol; 10] = [0xCF, 0x89, 0xCE, 0xBD, 0xCF, 0x84, 0xCE, 0xB1, 0xCF, 0x83];

static S_43_1: [symbol; 10] = [0xCE, 0xBF, 0xCE, 0xBD, 0xCF, 0x84, 0xCE, 0xB1, 0xCF, 0x83];

static A_43: [among; 2] = [
    among { s_size: 10, s: S_43_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 10, s: S_43_1.as_ptr(), substring_i: -1, result: 1, function: None },
];

static S_44_0: [symbol; 12] = [0xCE, 0xBF, 0xCE, 0xBC, 0xCE, 0xB1, 0xCF, 0x83, 0xCF, 0x84, 0xCE, 0xB5];

static S_44_1: [symbol; 14] = [0xCE, 0xB9, 0xCE, 0xBF, 0xCE, 0xBC, 0xCE, 0xB1, 0xCF, 0x83, 0xCF, 0x84, 0xCE, 0xB5];

static A_44: [among; 2] = [
    among { s_size: 12, s: S_44_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 14, s: S_44_1.as_ptr(), substring_i: 0, result: 1, function: None },
];

static S_45_0: [symbol; 2] = [0xCF, 0x80];

static S_45_1: [symbol; 4] = [0xCE, 0xB1, 0xCF, 0x80];

static S_45_2: [symbol; 12] = [0xCE, 0xB1, 0xCE, 0xBA, 0xCE, 0xB1, 0xCF, 0x84, 0xCE, 0xB1, 0xCF, 0x80];

static S_45_3: [symbol; 8] = [0xCF, 0x83, 0xCF, 0x85, 0xCE, 0xBC, 0xCF, 0x80];

static S_45_4: [symbol; 10] = [0xCE, 0xB1, 0xCF, 0x83, 0xCF, 0x85, 0xCE, 0xBC, 0xCF, 0x80];

static S_45_5: [symbol; 14] = [0xCE, 0xB1, 0xCE, 0xBC, 0xCE, 0xB5, 0xCF, 0x84, 0xCE, 0xB1, 0xCE, 0xBC, 0xCF, 0x86];

static A_45: [among; 6] = [
    among { s_size: 2, s: S_45_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_45_1.as_ptr(), substring_i: 0, result: 1, function: None },
    among { s_size: 12, s: S_45_2.as_ptr(), substring_i: 1, result: 1, function: None },
    among { s_size: 8, s: S_45_3.as_ptr(), substring_i: 0, result: 1, function: None },
    among { s_size: 10, s: S_45_4.as_ptr(), substring_i: 3, result: 1, function: None },
    among { s_size: 14, s: S_45_5.as_ptr(), substring_i: -1, result: 1, function: None },
];

static S_46_0: [symbol; 4] = [0xCE, 0xB1, 0xCF, 0x81];

static S_46_1: [symbol; 6] = [0xCE, 0xBD, 0xCE, 0xB9, 0xCF, 0x83];

static S_46_2: [symbol; 2] = [0xCE, 0xB6];

static S_46_3: [symbol; 4] = [0xCE, 0xB1, 0xCE, 0xBB];

static S_46_4: [symbol; 14] = [0xCF, 0x80, 0xCE, 0xB1, 0xCF, 0x81, 0xCE, 0xB1, 0xCE, 0xBA, 0xCE, 0xB1, 0xCE, 0xBB];

static S_46_5: [symbol; 10] = [0xCE, 0xB5, 0xCE, 0xBA, 0xCF, 0x84, 0xCE, 0xB5, 0xCE, 0xBB];

static S_46_6: [symbol; 2] = [0xCE, 0xBC];

static S_46_7: [symbol; 2] = [0xCE, 0xBE];

static S_46_8: [symbol; 6] = [0xCF, 0x80, 0xCF, 0x81, 0xCE, 0xBF];

static A_46: [among; 9] = [
    among { s_size: 4, s: S_46_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_46_1.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_46_2.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_46_3.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 14, s: S_46_4.as_ptr(), substring_i: 3, result: 1, function: None },
    among { s_size: 10, s: S_46_5.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_46_6.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_46_7.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_46_8.as_ptr(), substring_i: -1, result: 1, function: None },
];

static S_47_0: [symbol; 12] = [0xCE, 0xB7, 0xCE, 0xB8, 0xCE, 0xB7, 0xCE, 0xBA, 0xCE, 0xB5, 0xCF, 0x83];

static S_47_1: [symbol; 10] = [0xCE, 0xB7, 0xCE, 0xB8, 0xCE, 0xB7, 0xCE, 0xBA, 0xCE, 0xB1];

static S_47_2: [symbol; 10] = [0xCE, 0xB7, 0xCE, 0xB8, 0xCE, 0xB7, 0xCE, 0xBA, 0xCE, 0xB5];

static A_47: [among; 3] = [
    among { s_size: 12, s: S_47_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 10, s: S_47_1.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 10, s: S_47_2.as_ptr(), substring_i: -1, result: 1, function: None },
];

static S_48_0: [symbol; 4] = [0xCF, 0x83, 0xCF, 0x86];

static S_48_1: [symbol; 8] = [0xCE, 0xBD, 0xCE, 0xB1, 0xCF, 0x81, 0xCE, 0xB8];

static S_48_2: [symbol; 6] = [0xCF, 0x80, 0xCE, 0xB9, 0xCE, 0xB8];

static S_48_3: [symbol; 4] = [0xCE, 0xBF, 0xCE, 0xB8];

static S_48_4: [symbol; 10] = [0xCF, 0x83, 0xCE, 0xBA, 0xCE, 0xBF, 0xCF, 0x85, 0xCE, 0xBB];

static S_48_5: [symbol; 8] = [0xCF, 0x83, 0xCE, 0xBA, 0xCF, 0x89, 0xCE, 0xBB];

static A_48: [among; 6] = [
    among { s_size: 4, s: S_48_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_48_1.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_48_2.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_48_3.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 10, s: S_48_4.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_48_5.as_ptr(), substring_i: -1, result: 1, function: None },
];

static S_49_0: [symbol; 2] = [0xCE, 0xB8];

static S_49_1: [symbol; 10] = [0xCF, 0x80, 0xCF, 0x81, 0xCE, 0xBF, 0xCF, 0x83, 0xCE, 0xB8];

static S_49_2: [symbol; 18] = [0xCF, 0x80, 0xCE, 0xB1, 0xCF, 0x81, 0xCE, 0xB1, 0xCE, 0xBA, 0xCE, 0xB1, 0xCF, 0x84, 0xCE, 0xB1, 0xCE, 0xB8];

static S_49_3: [symbol; 8] = [0xCE, 0xB4, 0xCE, 0xB9, 0xCE, 0xB1, 0xCE, 0xB8];

static S_49_4: [symbol; 8] = [0xCF, 0x83, 0xCF, 0x85, 0xCE, 0xBD, 0xCE, 0xB8];

static A_49: [among; 5] = [
    among { s_size: 2, s: S_49_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 10, s: S_49_1.as_ptr(), substring_i: 0, result: 1, function: None },
    among { s_size: 18, s: S_49_2.as_ptr(), substring_i: 0, result: 1, function: None },
    among { s_size: 8, s: S_49_3.as_ptr(), substring_i: 0, result: 1, function: None },
    among { s_size: 8, s: S_49_4.as_ptr(), substring_i: 0, result: 1, function: None },
];

static S_50_0: [symbol; 8] = [0xCE, 0xB7, 0xCE, 0xBA, 0xCE, 0xB5, 0xCF, 0x83];

static S_50_1: [symbol; 6] = [0xCE, 0xB7, 0xCE, 0xBA, 0xCE, 0xB1];

static S_50_2: [symbol; 6] = [0xCE, 0xB7, 0xCE, 0xBA, 0xCE, 0xB5];

static A_50: [among; 3] = [
    among { s_size: 8, s: S_50_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_50_1.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_50_2.as_ptr(), substring_i: -1, result: 1, function: None },
];

static S_51_0: [symbol; 8] = [0xCE, 0xB2, 0xCE, 0xBB, 0xCE, 0xB5, 0xCF, 0x80];

static S_51_1: [symbol; 10] = [0xCF, 0x80, 0xCE, 0xBF, 0xCE, 0xB4, 0xCE, 0xB1, 0xCF, 0x81];

static S_51_2: [symbol; 8] = [0xCF, 0x80, 0xCF, 0x81, 0xCF, 0x89, 0xCF, 0x84];

static S_51_3: [symbol; 10] = [0xCE, 0xBA, 0xCF, 0x85, 0xCE, 0xBC, 0xCE, 0xB1, 0xCF, 0x84];

static S_51_4: [symbol; 12] = [0xCF, 0x80, 0xCE, 0xB1, 0xCE, 0xBD, 0xCF, 0x84, 0xCE, 0xB1, 0xCF, 0x87];

static S_51_5: [symbol; 6] = [0xCE, 0xBB, 0xCE, 0xB1, 0xCF, 0x87];

static S_51_6: [symbol; 6] = [0xCF, 0x86, 0xCE, 0xB1, 0xCE, 0xB3];

static S_51_7: [symbol; 6] = [0xCE, 0xBB, 0xCE, 0xB7, 0xCE, 0xB3];

static S_51_8: [symbol; 8] = [0xCF, 0x86, 0xCF, 0x81, 0xCF, 0x85, 0xCE, 0xB4];

static S_51_9: [symbol; 12] = [0xCE, 0xBC, 0xCE, 0xB1, 0xCE, 0xBD, 0xCF, 0x84, 0xCE, 0xB9, 0xCE, 0xBB];

static S_51_10: [symbol; 8] = [0xCE, 0xBC, 0xCE, 0xB1, 0xCE, 0xBB, 0xCE, 0xBB];

static S_51_11: [symbol; 4] = [0xCE, 0xBF, 0xCE, 0xBC];

static A_51: [among; 12] = [
    among { s_size: 8, s: S_51_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 10, s: S_51_1.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_51_2.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 10, s: S_51_3.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 12, s: S_51_4.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_51_5.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_51_6.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_51_7.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_51_8.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 12, s: S_51_9.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_51_10.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_51_11.as_ptr(), substring_i: -1, result: 1, function: None },
];

static S_52_0: [symbol; 10] = [0xCE, 0xB5, 0xCE, 0xBA, 0xCE, 0xBB, 0xCE, 0xB9, 0xCF, 0x80];

static S_52_1: [symbol; 2] = [0xCF, 0x81];

static S_52_2: [symbol; 10] = [0xCE, 0xB1, 0xCE, 0xBD, 0xCE, 0xB1, 0xCF, 0x81, 0xCF, 0x81];

static S_52_3: [symbol; 16] = [0xCE, 0xB5, 0xCE, 0xBD, 0xCE, 0xB4, 0xCE, 0xB9, 0xCE, 0xB1, 0xCF, 0x86, 0xCE, 0xB5, 0xCF, 0x81];

static S_52_4: [symbol; 6] = [0xCF, 0x80, 0xCE, 0xB1, 0xCF, 0x84];

static S_52_5: [symbol; 14] = [0xCE, 0xBA, 0xCE, 0xB1, 0xCE, 0xB8, 0xCE, 0xB1, 0xCF, 0x81, 0xCE, 0xB5, 0xCF, 0x85];

static S_52_6: [symbol; 16] = [0xCE, 0xB4, 0xCE, 0xB5, 0xCF, 0x85, 0xCF, 0x84, 0xCE, 0xB5, 0xCF, 0x81, 0xCE, 0xB5, 0xCF, 0x85];

static S_52_7: [symbol; 6] = [0xCE, 0xBB, 0xCE, 0xB5, 0xCF, 0x87];

static S_52_8: [symbol; 6] = [0xCF, 0x84, 0xCF, 0x83, 0xCE, 0xB1];

static S_52_9: [symbol; 6] = [0xCF, 0x87, 0xCE, 0xB1, 0xCE, 0xB4];

static S_52_10: [symbol; 6] = [0xCE, 0xBC, 0xCE, 0xB5, 0xCE, 0xB4];

static S_52_11: [symbol; 12] = [0xCE, 0xBB, 0xCE, 0xB1, 0xCE, 0xBC, 0xCF, 0x80, 0xCE, 0xB9, 0xCE, 0xB4];

static S_52_12: [symbol; 4] = [0xCE, 0xB4, 0xCE, 0xB5];

static S_52_13: [symbol; 6] = [0xCF, 0x80, 0xCE, 0xBB, 0xCE, 0xB5];

static S_52_14: [symbol; 10] = [0xCE, 0xBC, 0xCE, 0xB5, 0xCF, 0x83, 0xCE, 0xB1, 0xCE, 0xB6];

static S_52_15: [symbol; 12] = [0xCE, 0xB4, 0xCE, 0xB5, 0xCF, 0x83, 0xCF, 0x80, 0xCE, 0xBF, 0xCE, 0xB6];

static S_52_16: [symbol; 6] = [0xCE, 0xB1, 0xCE, 0xB9, 0xCE, 0xB8];

static S_52_17: [symbol; 12] = [0xCF, 0x86, 0xCE, 0xB1, 0xCF, 0x81, 0xCE, 0xBC, 0xCE, 0xB1, 0xCE, 0xBA];

static S_52_18: [symbol; 6] = [0xCE, 0xB1, 0xCE, 0xB3, 0xCE, 0xBA];

static S_52_19: [symbol; 8] = [0xCE, 0xB1, 0xCE, 0xBD, 0xCE, 0xB7, 0xCE, 0xBA];

static S_52_20: [symbol; 2] = [0xCE, 0xBB];

static S_52_21: [symbol; 2] = [0xCE, 0xBC];

static S_52_22: [symbol; 4] = [0xCE, 0xB1, 0xCE, 0xBC];

static S_52_23: [symbol; 8] = [0xCE, 0xB2, 0xCF, 0x81, 0xCE, 0xBF, 0xCE, 0xBC];

static S_52_24: [symbol; 14] = [0xCF, 0x85, 0xCF, 0x80, 0xCE, 0xBF, 0xCF, 0x84, 0xCE, 0xB5, 0xCE, 0xB9, 0xCE, 0xBD];

static A_52: [among; 25] = [
    among { s_size: 10, s: S_52_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_52_1.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 10, s: S_52_2.as_ptr(), substring_i: 1, result: 1, function: None },
    among { s_size: 16, s: S_52_3.as_ptr(), substring_i: 1, result: 1, function: None },
    among { s_size: 6, s: S_52_4.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 14, s: S_52_5.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 16, s: S_52_6.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_52_7.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_52_8.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_52_9.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_52_10.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 12, s: S_52_11.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_52_12.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_52_13.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 10, s: S_52_14.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 12, s: S_52_15.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_52_16.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 12, s: S_52_17.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_52_18.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_52_19.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_52_20.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_52_21.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_52_22.as_ptr(), substring_i: 21, result: 1, function: None },
    among { s_size: 8, s: S_52_23.as_ptr(), substring_i: 21, result: 1, function: None },
    among { s_size: 14, s: S_52_24.as_ptr(), substring_i: -1, result: 1, function: None },
];

static S_53_0: [symbol; 10] = [0xCE, 0xBF, 0xCF, 0x85, 0xCF, 0x83, 0xCE, 0xB5, 0xCF, 0x83];

static S_53_1: [symbol; 8] = [0xCE, 0xBF, 0xCF, 0x85, 0xCF, 0x83, 0xCE, 0xB1];

static S_53_2: [symbol; 8] = [0xCE, 0xBF, 0xCF, 0x85, 0xCF, 0x83, 0xCE, 0xB5];

static A_53: [among; 3] = [
    among { s_size: 10, s: S_53_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_53_1.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_53_2.as_ptr(), substring_i: -1, result: 1, function: None },
];

static S_54_0: [symbol; 4] = [0xCF, 0x81, 0xCF, 0x80];

static S_54_1: [symbol; 4] = [0xCF, 0x80, 0xCF, 0x81];

static S_54_2: [symbol; 4] = [0xCF, 0x86, 0xCF, 0x81];

static S_54_3: [symbol; 8] = [0xCF, 0x87, 0xCE, 0xBF, 0xCF, 0x81, 0xCF, 0x84];

static S_54_4: [symbol; 4] = [0xCF, 0x83, 0xCF, 0x86];

static S_54_5: [symbol; 4] = [0xCE, 0xBF, 0xCF, 0x86];

static S_54_6: [symbol; 6] = [0xCF, 0x88, 0xCE, 0xBF, 0xCF, 0x86];

static S_54_7: [symbol; 6] = [0xCE, 0xBB, 0xCE, 0xBF, 0xCF, 0x87];

static S_54_8: [symbol; 12] = [0xCE, 0xBD, 0xCE, 0xB1, 0xCF, 0x85, 0xCE, 0xBB, 0xCE, 0xBF, 0xCF, 0x87];

static S_54_9: [symbol; 6] = [0xCF, 0x80, 0xCE, 0xB5, 0xCE, 0xBB];

static S_54_10: [symbol; 4] = [0xCE, 0xBB, 0xCE, 0xBB];

static S_54_11: [symbol; 8] = [0xCF, 0x83, 0xCE, 0xBC, 0xCE, 0xB7, 0xCE, 0xBD];

static A_54: [among; 12] = [
    among { s_size: 4, s: S_54_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_54_1.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_54_2.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_54_3.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_54_4.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_54_5.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_54_6.as_ptr(), substring_i: 5, result: -1, function: None },
    among { s_size: 6, s: S_54_7.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 12, s: S_54_8.as_ptr(), substring_i: 7, result: -1, function: None },
    among { s_size: 6, s: S_54_9.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_54_10.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_54_11.as_ptr(), substring_i: -1, result: 1, function: None },
];

static S_55_0: [symbol; 2] = [0xCF, 0x80];

static S_55_1: [symbol; 6] = [0xCE, 0xB1, 0xCF, 0x83, 0xCF, 0x80];

static S_55_2: [symbol; 8] = [0xCE, 0xB1, 0xCE, 0xBD, 0xCF, 0x85, 0xCF, 0x80];

static S_55_3: [symbol; 10] = [0xCE, 0xB1, 0xCF, 0x81, 0xCF, 0x84, 0xCE, 0xB9, 0xCF, 0x80];

static S_55_4: [symbol; 8] = [0xCE, 0xB1, 0xCE, 0xB5, 0xCE, 0xB9, 0xCF, 0x80];

static S_55_5: [symbol; 8] = [0xCF, 0x83, 0xCF, 0x85, 0xCE, 0xBC, 0xCF, 0x80];

static S_55_6: [symbol; 16] = [0xCF, 0x80, 0xCF, 0x81, 0xCE, 0xBF, 0xCF, 0x83, 0xCF, 0x89, 0xCF, 0x80, 0xCE, 0xBF, 0xCF, 0x80];

static S_55_7: [symbol; 14] = [0xCF, 0x83, 0xCE, 0xB9, 0xCE, 0xB4, 0xCE, 0xB7, 0xCF, 0x81, 0xCE, 0xBF, 0xCF, 0x80];

static S_55_8: [symbol; 12] = [0xCE, 0xB4, 0xCF, 0x81, 0xCE, 0xBF, 0xCF, 0x83, 0xCE, 0xBF, 0xCF, 0x80];

static S_55_9: [symbol; 8] = [0xCE, 0xBD, 0xCE, 0xB5, 0xCE, 0xBF, 0xCF, 0x80];

static S_55_10: [symbol; 16] = [0xCE, 0xBA, 0xCF, 0x81, 0xCE, 0xBF, 0xCE, 0xBA, 0xCE, 0xB1, 0xCE, 0xBB, 0xCE, 0xBF, 0xCF, 0x80];

static S_55_11: [symbol; 8] = [0xCE, 0xBF, 0xCE, 0xBB, 0xCE, 0xBF, 0xCF, 0x80];

static S_55_12: [symbol; 2] = [0xCF, 0x81];

static S_55_13: [symbol; 4] = [0xCF, 0x84, 0xCF, 0x81];

static S_55_14: [symbol; 6] = [0xCE, 0xBF, 0xCF, 0x85, 0xCF, 0x81];

static S_55_15: [symbol; 10] = [0xCE, 0xB1, 0xCF, 0x83, 0xCF, 0x80, 0xCE, 0xB1, 0xCF, 0x81];

static S_55_16: [symbol; 6] = [0xCF, 0x87, 0xCE, 0xB1, 0xCF, 0x81];

static S_55_17: [symbol; 8] = [0xCE, 0xB1, 0xCF, 0x87, 0xCE, 0xB1, 0xCF, 0x81];

static S_55_18: [symbol; 8] = [0xCE, 0xB1, 0xCF, 0x80, 0xCE, 0xB5, 0xCF, 0x81];

static S_55_19: [symbol; 2] = [0xCF, 0x84];

static S_55_20: [symbol; 10] = [0xCE, 0xB1, 0xCE, 0xBD, 0xCF, 0x85, 0xCF, 0x83, 0xCF, 0x84];

static S_55_21: [symbol; 10] = [0xCE, 0xB1, 0xCE, 0xB2, 0xCE, 0xB1, 0xCF, 0x83, 0xCF, 0x84];

static S_55_22: [symbol; 10] = [0xCF, 0x80, 0xCF, 0x81, 0xCE, 0xBF, 0xCF, 0x83, 0xCF, 0x84];

static S_55_23: [symbol; 12] = [0xCE, 0xB1, 0xCE, 0xB9, 0xCE, 0xBC, 0xCE, 0xBF, 0xCF, 0x83, 0xCF, 0x84];

static S_55_24: [symbol; 8] = [0xCE, 0xB4, 0xCE, 0xB9, 0xCE, 0xB1, 0xCF, 0x84];

static S_55_25: [symbol; 8] = [0xCE, 0xB5, 0xCF, 0x80, 0xCE, 0xB9, 0xCF, 0x84];

static S_55_26: [symbol; 8] = [0xCF, 0x83, 0xCF, 0x85, 0xCE, 0xBD, 0xCF, 0x84];

static S_55_27: [symbol; 8] = [0xCF, 0x85, 0xCF, 0x80, 0xCE, 0xBF, 0xCF, 0x84];

static S_55_28: [symbol; 8] = [0xCE, 0xB1, 0xCF, 0x80, 0xCE, 0xBF, 0xCF, 0x84];

static S_55_29: [symbol; 8] = [0xCE, 0xBF, 0xCE, 0xBC, 0xCE, 0xBF, 0xCF, 0x84];

static S_55_30: [symbol; 10] = [0xCE, 0xBD, 0xCE, 0xBF, 0xCE, 0xBC, 0xCE, 0xBF, 0xCF, 0x84];

static S_55_31: [symbol; 6] = [0xCE, 0xBD, 0xCE, 0xB1, 0xCF, 0x85];

static S_55_32: [symbol; 10] = [0xCF, 0x80, 0xCE, 0xBF, 0xCE, 0xBB, 0xCF, 0x85, 0xCF, 0x86];

static S_55_33: [symbol; 4] = [0xCE, 0xB1, 0xCF, 0x86];

static S_55_34: [symbol; 6] = [0xCE, 0xBE, 0xCE, 0xB5, 0xCF, 0x86];

static S_55_35: [symbol; 8] = [0xCE, 0xB1, 0xCE, 0xB4, 0xCE, 0xB7, 0xCF, 0x86];

static S_55_36: [symbol; 8] = [0xCF, 0x80, 0xCE, 0xB1, 0xCE, 0xBC, 0xCF, 0x86];

static S_55_37: [symbol; 12] = [0xCE, 0xB1, 0xCE, 0xBC, 0xCE, 0xB1, 0xCE, 0xBB, 0xCE, 0xBB, 0xCE, 0xB9];

static S_55_38: [symbol; 2] = [0xCE, 0xBB];

static S_55_39: [symbol; 8] = [0xCE, 0xB1, 0xCE, 0xBC, 0xCE, 0xB1, 0xCE, 0xBB];

static S_55_40: [symbol; 2] = [0xCE, 0xBC];

static S_55_41: [symbol; 10] = [0xCE, 0xBF, 0xCF, 0x85, 0xCE, 0xBB, 0xCE, 0xB1, 0xCE, 0xBC];

static S_55_42: [symbol; 4] = [0xCE, 0xB5, 0xCE, 0xBD];

static S_55_43: [symbol; 12] = [0xCE, 0xB4, 0xCE, 0xB5, 0xCF, 0x81, 0xCE, 0xB2, 0xCE, 0xB5, 0xCE, 0xBD];

static A_55: [among; 44] = [
    among { s_size: 2, s: S_55_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_55_1.as_ptr(), substring_i: 0, result: 1, function: None },
    among { s_size: 8, s: S_55_2.as_ptr(), substring_i: 0, result: 1, function: None },
    among { s_size: 10, s: S_55_3.as_ptr(), substring_i: 0, result: 1, function: None },
    among { s_size: 8, s: S_55_4.as_ptr(), substring_i: 0, result: 1, function: None },
    among { s_size: 8, s: S_55_5.as_ptr(), substring_i: 0, result: 1, function: None },
    among { s_size: 16, s: S_55_6.as_ptr(), substring_i: 0, result: 1, function: None },
    among { s_size: 14, s: S_55_7.as_ptr(), substring_i: 0, result: 1, function: None },
    among { s_size: 12, s: S_55_8.as_ptr(), substring_i: 0, result: 1, function: None },
    among { s_size: 8, s: S_55_9.as_ptr(), substring_i: 0, result: 1, function: None },
    among { s_size: 16, s: S_55_10.as_ptr(), substring_i: 0, result: 1, function: None },
    among { s_size: 8, s: S_55_11.as_ptr(), substring_i: 0, result: 1, function: None },
    among { s_size: 2, s: S_55_12.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_55_13.as_ptr(), substring_i: 12, result: 1, function: None },
    among { s_size: 6, s: S_55_14.as_ptr(), substring_i: 12, result: 1, function: None },
    among { s_size: 10, s: S_55_15.as_ptr(), substring_i: 12, result: 1, function: None },
    among { s_size: 6, s: S_55_16.as_ptr(), substring_i: 12, result: 1, function: None },
    among { s_size: 8, s: S_55_17.as_ptr(), substring_i: 16, result: 1, function: None },
    among { s_size: 8, s: S_55_18.as_ptr(), substring_i: 12, result: 1, function: None },
    among { s_size: 2, s: S_55_19.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 10, s: S_55_20.as_ptr(), substring_i: 19, result: 1, function: None },
    among { s_size: 10, s: S_55_21.as_ptr(), substring_i: 19, result: 1, function: None },
    among { s_size: 10, s: S_55_22.as_ptr(), substring_i: 19, result: 1, function: None },
    among { s_size: 12, s: S_55_23.as_ptr(), substring_i: 19, result: 1, function: None },
    among { s_size: 8, s: S_55_24.as_ptr(), substring_i: 19, result: 1, function: None },
    among { s_size: 8, s: S_55_25.as_ptr(), substring_i: 19, result: 1, function: None },
    among { s_size: 8, s: S_55_26.as_ptr(), substring_i: 19, result: 1, function: None },
    among { s_size: 8, s: S_55_27.as_ptr(), substring_i: 19, result: 1, function: None },
    among { s_size: 8, s: S_55_28.as_ptr(), substring_i: 19, result: 1, function: None },
    among { s_size: 8, s: S_55_29.as_ptr(), substring_i: 19, result: 1, function: None },
    among { s_size: 10, s: S_55_30.as_ptr(), substring_i: 29, result: 1, function: None },
    among { s_size: 6, s: S_55_31.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 10, s: S_55_32.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_55_33.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_55_34.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_55_35.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_55_36.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 12, s: S_55_37.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_55_38.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_55_39.as_ptr(), substring_i: 38, result: 1, function: None },
    among { s_size: 2, s: S_55_40.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 10, s: S_55_41.as_ptr(), substring_i: 40, result: 1, function: None },
    among { s_size: 4, s: S_55_42.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 12, s: S_55_43.as_ptr(), substring_i: 42, result: 1, function: None },
];

static S_56_0: [symbol; 8] = [0xCE, 0xB1, 0xCE, 0xB3, 0xCE, 0xB5, 0xCF, 0x83];

static S_56_1: [symbol; 6] = [0xCE, 0xB1, 0xCE, 0xB3, 0xCE, 0xB1];

static S_56_2: [symbol; 6] = [0xCE, 0xB1, 0xCE, 0xB3, 0xCE, 0xB5];

static A_56: [among; 3] = [
    among { s_size: 8, s: S_56_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_56_1.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_56_2.as_ptr(), substring_i: -1, result: 1, function: None },
];

static S_57_0: [symbol; 8] = [0xCE, 0xB7, 0xCF, 0x83, 0xCE, 0xBF, 0xCF, 0x85];

static S_57_1: [symbol; 6] = [0xCE, 0xB7, 0xCF, 0x83, 0xCE, 0xB1];

static S_57_2: [symbol; 6] = [0xCE, 0xB7, 0xCF, 0x83, 0xCE, 0xB5];

static A_57: [among; 3] = [
    among { s_size: 8, s: S_57_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_57_1.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_57_2.as_ptr(), substring_i: -1, result: 1, function: None },
];

static S_58_0: [symbol; 2] = [0xCE, 0xBD];

static S_58_1: [symbol; 10] = [0xCE, 0xB5, 0xCF, 0x80, 0xCF, 0x84, 0xCE, 0xB1, 0xCE, 0xBD];

static S_58_2: [symbol; 14] = [0xCE, 0xB4, 0xCF, 0x89, 0xCE, 0xB4, 0xCE, 0xB5, 0xCE, 0xBA, 0xCE, 0xB1, 0xCE, 0xBD];

static S_58_3: [symbol; 12] = [0xCF, 0x87, 0xCE, 0xB5, 0xCF, 0x81, 0xCF, 0x83, 0xCE, 0xBF, 0xCE, 0xBD];

static S_58_4: [symbol; 14] = [0xCE, 0xBC, 0xCE, 0xB5, 0xCE, 0xB3, 0xCE, 0xB1, 0xCE, 0xBB, 0xCE, 0xBF, 0xCE, 0xBD];

static S_58_5: [symbol; 12] = [0xCE, 0xB5, 0xCF, 0x81, 0xCE, 0xB7, 0xCE, 0xBC, 0xCE, 0xBF, 0xCE, 0xBD];

static A_58: [among; 6] = [
    among { s_size: 2, s: S_58_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 10, s: S_58_1.as_ptr(), substring_i: 0, result: 1, function: None },
    among { s_size: 14, s: S_58_2.as_ptr(), substring_i: 0, result: 1, function: None },
    among { s_size: 12, s: S_58_3.as_ptr(), substring_i: 0, result: 1, function: None },
    among { s_size: 14, s: S_58_4.as_ptr(), substring_i: 0, result: 1, function: None },
    among { s_size: 12, s: S_58_5.as_ptr(), substring_i: 0, result: 1, function: None },
];

static S_59_0: [symbol; 8] = [0xCE, 0xB7, 0xCF, 0x83, 0xCF, 0x84, 0xCE, 0xB5];

static A_59: [among; 1] = [
    among { s_size: 8, s: S_59_0.as_ptr(), substring_i: -1, result: 1, function: None },
];

static S_60_0: [symbol; 4] = [0xCF, 0x87, 0xCF, 0x81];

static S_60_1: [symbol; 10] = [0xCE, 0xB4, 0xCF, 0x85, 0xCF, 0x83, 0xCF, 0x87, 0xCF, 0x81];

static S_60_2: [symbol; 8] = [0xCE, 0xB5, 0xCF, 0x85, 0xCF, 0x87, 0xCF, 0x81];

static S_60_3: [symbol; 6] = [0xCE, 0xB1, 0xCF, 0x87, 0xCF, 0x81];

static S_60_4: [symbol; 14] = [0xCE, 0xBA, 0xCE, 0xBF, 0xCE, 0xB9, 0xCE, 0xBD, 0xCE, 0xBF, 0xCF, 0x87, 0xCF, 0x81];

static S_60_5: [symbol; 12] = [0xCF, 0x80, 0xCE, 0xB1, 0xCE, 0xBB, 0xCE, 0xB9, 0xCE, 0xBC, 0xCF, 0x88];

static S_60_6: [symbol; 4] = [0xCF, 0x83, 0xCE, 0xB2];

static S_60_7: [symbol; 6] = [0xCE, 0xB1, 0xCF, 0x83, 0xCE, 0xB2];

static S_60_8: [symbol; 6] = [0xCE, 0xB1, 0xCF, 0x80, 0xCE, 0xBB];

static S_60_9: [symbol; 10] = [0xCE, 0xB1, 0xCE, 0xB5, 0xCE, 0xB9, 0xCE, 0xBC, 0xCE, 0xBD];

static A_60: [among; 10] = [
    among { s_size: 4, s: S_60_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 10, s: S_60_1.as_ptr(), substring_i: 0, result: 1, function: None },
    among { s_size: 8, s: S_60_2.as_ptr(), substring_i: 0, result: 1, function: None },
    among { s_size: 6, s: S_60_3.as_ptr(), substring_i: 0, result: 1, function: None },
    among { s_size: 14, s: S_60_4.as_ptr(), substring_i: 0, result: 1, function: None },
    among { s_size: 12, s: S_60_5.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_60_6.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_60_7.as_ptr(), substring_i: 6, result: 1, function: None },
    among { s_size: 6, s: S_60_8.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 10, s: S_60_9.as_ptr(), substring_i: -1, result: 1, function: None },
];

static S_61_0: [symbol; 8] = [0xCE, 0xBF, 0xCF, 0x85, 0xCE, 0xBD, 0xCE, 0xB5];

static S_61_1: [symbol; 12] = [0xCE, 0xB7, 0xCF, 0x83, 0xCE, 0xBF, 0xCF, 0x85, 0xCE, 0xBD, 0xCE, 0xB5];

static S_61_2: [symbol; 12] = [0xCE, 0xB7, 0xCE, 0xB8, 0xCE, 0xBF, 0xCF, 0x85, 0xCE, 0xBD, 0xCE, 0xB5];

static A_61: [among; 3] = [
    among { s_size: 8, s: S_61_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 12, s: S_61_1.as_ptr(), substring_i: 0, result: 1, function: None },
    among { s_size: 12, s: S_61_2.as_ptr(), substring_i: 0, result: 1, function: None },
];

static S_62_0: [symbol; 2] = [0xCF, 0x81];

static S_62_1: [symbol; 22] = [0xCF, 0x83, 0xCF, 0x84, 0xCF, 0x81, 0xCE, 0xB1, 0xCE, 0xB2, 0xCE, 0xBF, 0xCE, 0xBC, 0xCE, 0xBF, 0xCF, 0x85, 0xCF, 0x84, 0xCF, 0x83];

static S_62_2: [symbol; 18] = [0xCE, 0xBA, 0xCE, 0xB1, 0xCE, 0xBA, 0xCE, 0xBF, 0xCE, 0xBC, 0xCE, 0xBF, 0xCF, 0x85, 0xCF, 0x84, 0xCF, 0x83];

static S_62_3: [symbol; 6] = [0xCF, 0x83, 0xCF, 0x80, 0xCE, 0xB9];

static S_62_4: [symbol; 2] = [0xCE, 0xBD];

static S_62_5: [symbol; 8] = [0xCE, 0xB5, 0xCE, 0xBE, 0xCF, 0x89, 0xCE, 0xBD];

static A_62: [among; 6] = [
    among { s_size: 2, s: S_62_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 22, s: S_62_1.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 18, s: S_62_2.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_62_3.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_62_4.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_62_5.as_ptr(), substring_i: 4, result: 1, function: None },
];

static S_63_0: [symbol; 8] = [0xCE, 0xBF, 0xCF, 0x85, 0xCE, 0xBC, 0xCE, 0xB5];

static S_63_1: [symbol; 12] = [0xCE, 0xB7, 0xCF, 0x83, 0xCE, 0xBF, 0xCF, 0x85, 0xCE, 0xBC, 0xCE, 0xB5];

static S_63_2: [symbol; 12] = [0xCE, 0xB7, 0xCE, 0xB8, 0xCE, 0xBF, 0xCF, 0x85, 0xCE, 0xBC, 0xCE, 0xB5];

static A_63: [among; 3] = [
    among { s_size: 8, s: S_63_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 12, s: S_63_1.as_ptr(), substring_i: 0, result: 1, function: None },
    among { s_size: 12, s: S_63_2.as_ptr(), substring_i: 0, result: 1, function: None },
];

static S_64_0: [symbol; 10] = [0xCE, 0xB1, 0xCF, 0x83, 0xCE, 0xBF, 0xCF, 0x85, 0xCF, 0x83];

static S_64_1: [symbol; 16] = [0xCF, 0x80, 0xCE, 0xB1, 0xCF, 0x81, 0xCE, 0xB1, 0xCF, 0x83, 0xCE, 0xBF, 0xCF, 0x85, 0xCF, 0x83];

static S_64_2: [symbol; 16] = [0xCE, 0xB1, 0xCE, 0xBB, 0xCE, 0xBB, 0xCE, 0xBF, 0xCF, 0x83, 0xCE, 0xBF, 0xCF, 0x85, 0xCF, 0x83];

static S_64_3: [symbol; 2] = [0xCF, 0x86];

static S_64_4: [symbol; 2] = [0xCF, 0x87];

static S_64_5: [symbol; 4] = [0xCE, 0xB1, 0xCE, 0xB6];

static S_64_6: [symbol; 12] = [0xCF, 0x89, 0xCF, 0x81, 0xCE, 0xB9, 0xCE, 0xBF, 0xCF, 0x80, 0xCE, 0xBB];

static A_64: [among; 7] = [
    among { s_size: 10, s: S_64_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 16, s: S_64_1.as_ptr(), substring_i: 0, result: 1, function: None },
    among { s_size: 16, s: S_64_2.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_64_3.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_64_4.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_64_5.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 12, s: S_64_6.as_ptr(), substring_i: -1, result: 1, function: None },
];

static S_65_0: [symbol; 10] = [0xCE, 0xBC, 0xCE, 0xB1, 0xCF, 0x84, 0xCE, 0xBF, 0xCF, 0x83];

static S_65_1: [symbol; 8] = [0xCE, 0xBC, 0xCE, 0xB1, 0xCF, 0x84, 0xCE, 0xB1];

static S_65_2: [symbol; 10] = [0xCE, 0xBC, 0xCE, 0xB1, 0xCF, 0x84, 0xCF, 0x89, 0xCE, 0xBD];

static A_65: [among; 3] = [
    among { s_size: 10, s: S_65_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_65_1.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 10, s: S_65_2.as_ptr(), substring_i: -1, result: 1, function: None },
];

static S_66_0: [symbol; 4] = [0xCF, 0x85, 0xCF, 0x83];

static S_66_1: [symbol; 6] = [0xCE, 0xBF, 0xCF, 0x85, 0xCF, 0x83];

static S_66_2: [symbol; 4] = [0xCE, 0xB1, 0xCF, 0x83];

static S_66_3: [symbol; 4] = [0xCE, 0xB5, 0xCF, 0x83];

static S_66_4: [symbol; 8] = [0xCE, 0xB7, 0xCF, 0x83, 0xCE, 0xB5, 0xCF, 0x83];

static S_66_5: [symbol; 8] = [0xCE, 0xB7, 0xCE, 0xB4, 0xCE, 0xB5, 0xCF, 0x83];

static S_66_6: [symbol; 4] = [0xCE, 0xB7, 0xCF, 0x83];

static S_66_7: [symbol; 6] = [0xCE, 0xB5, 0xCE, 0xB9, 0xCF, 0x83];

static S_66_8: [symbol; 10] = [0xCE, 0xB7, 0xCE, 0xB8, 0xCE, 0xB5, 0xCE, 0xB9, 0xCF, 0x83];

static S_66_9: [symbol; 4] = [0xCE, 0xBF, 0xCF, 0x83];

static S_66_10: [symbol; 2] = [0xCF, 0x85];

static S_66_11: [symbol; 4] = [0xCE, 0xBF, 0xCF, 0x85];

static S_66_12: [symbol; 2] = [0xCF, 0x89];

static S_66_13: [symbol; 6] = [0xCE, 0xB7, 0xCF, 0x83, 0xCF, 0x89];

static S_66_14: [symbol; 4] = [0xCE, 0xB1, 0xCF, 0x89];

static S_66_15: [symbol; 6] = [0xCE, 0xB7, 0xCE, 0xB8, 0xCF, 0x89];

static S_66_16: [symbol; 2] = [0xCE, 0xB1];

static S_66_17: [symbol; 10] = [0xCE, 0xB9, 0xCE, 0xBF, 0xCF, 0x85, 0xCE, 0xBC, 0xCE, 0xB1];

static S_66_18: [symbol; 12] = [0xCE, 0xBF, 0xCF, 0x83, 0xCE, 0xBF, 0xCF, 0x85, 0xCE, 0xBD, 0xCE, 0xB1];

static S_66_19: [symbol; 14] = [0xCE, 0xB9, 0xCE, 0xBF, 0xCF, 0x83, 0xCE, 0xBF, 0xCF, 0x85, 0xCE, 0xBD, 0xCE, 0xB1];

static S_66_20: [symbol; 12] = [0xCE, 0xBF, 0xCE, 0xBC, 0xCE, 0xBF, 0xCF, 0x85, 0xCE, 0xBD, 0xCE, 0xB1];

static S_66_21: [symbol; 14] = [0xCE, 0xB9, 0xCE, 0xBF, 0xCE, 0xBC, 0xCE, 0xBF, 0xCF, 0x85, 0xCE, 0xBD, 0xCE, 0xB1];

static S_66_22: [symbol; 2] = [0xCE, 0xB5];

static S_66_23: [symbol; 14] = [0xCE, 0xB9, 0xCE, 0xB5, 0xCF, 0x83, 0xCE, 0xB1, 0xCF, 0x83, 0xCF, 0x84, 0xCE, 0xB5];

static S_66_24: [symbol; 12] = [0xCE, 0xBF, 0xCF, 0x83, 0xCE, 0xB1, 0xCF, 0x83, 0xCF, 0x84, 0xCE, 0xB5];

static S_66_25: [symbol; 14] = [0xCE, 0xB9, 0xCE, 0xBF, 0xCF, 0x83, 0xCE, 0xB1, 0xCF, 0x83, 0xCF, 0x84, 0xCE, 0xB5];

static S_66_26: [symbol; 14] = [0xCE, 0xBF, 0xCF, 0x85, 0xCE, 0xBC, 0xCE, 0xB1, 0xCF, 0x83, 0xCF, 0x84, 0xCE, 0xB5];

static S_66_27: [symbol; 16] = [0xCE, 0xB9, 0xCE, 0xBF, 0xCF, 0x85, 0xCE, 0xBC, 0xCE, 0xB1, 0xCF, 0x83, 0xCF, 0x84, 0xCE, 0xB5];

static S_66_28: [symbol; 14] = [0xCE, 0xB9, 0xCE, 0xB5, 0xCE, 0xBC, 0xCE, 0xB1, 0xCF, 0x83, 0xCF, 0x84, 0xCE, 0xB5];

static S_66_29: [symbol; 12] = [0xCE, 0xBF, 0xCF, 0x85, 0xCF, 0x83, 0xCE, 0xB1, 0xCF, 0x84, 0xCE, 0xB5];

static S_66_30: [symbol; 10] = [0xCE, 0xB7, 0xCF, 0x83, 0xCE, 0xB1, 0xCF, 0x84, 0xCE, 0xB5];

static S_66_31: [symbol; 10] = [0xCE, 0xB1, 0xCE, 0xB3, 0xCE, 0xB1, 0xCF, 0x84, 0xCE, 0xB5];

static S_66_32: [symbol; 10] = [0xCE, 0xB7, 0xCE, 0xBA, 0xCE, 0xB1, 0xCF, 0x84, 0xCE, 0xB5];

static S_66_33: [symbol; 14] = [0xCE, 0xB7, 0xCE, 0xB8, 0xCE, 0xB7, 0xCE, 0xBA, 0xCE, 0xB1, 0xCF, 0x84, 0xCE, 0xB5];

static S_66_34: [symbol; 8] = [0xCE, 0xB5, 0xCE, 0xB9, 0xCF, 0x84, 0xCE, 0xB5];

static S_66_35: [symbol; 12] = [0xCE, 0xB7, 0xCE, 0xB8, 0xCE, 0xB5, 0xCE, 0xB9, 0xCF, 0x84, 0xCE, 0xB5];

static S_66_36: [symbol; 2] = [0xCE, 0xB7];

static S_66_37: [symbol; 2] = [0xCE, 0xB9];

static S_66_38: [symbol; 8] = [0xCE, 0xB1, 0xCF, 0x83, 0xCE, 0xB1, 0xCE, 0xB9];

static S_66_39: [symbol; 8] = [0xCE, 0xB5, 0xCF, 0x83, 0xCE, 0xB1, 0xCE, 0xB9];

static S_66_40: [symbol; 10] = [0xCE, 0xB9, 0xCE, 0xB5, 0xCF, 0x83, 0xCE, 0xB1, 0xCE, 0xB9];

static S_66_41: [symbol; 8] = [0xCE, 0xB1, 0xCF, 0x84, 0xCE, 0xB1, 0xCE, 0xB9];

static S_66_42: [symbol; 8] = [0xCE, 0xB5, 0xCF, 0x84, 0xCE, 0xB1, 0xCE, 0xB9];

static S_66_43: [symbol; 10] = [0xCE, 0xB9, 0xCE, 0xB5, 0xCF, 0x84, 0xCE, 0xB1, 0xCE, 0xB9];

static S_66_44: [symbol; 12] = [0xCE, 0xBF, 0xCF, 0x85, 0xCE, 0xBD, 0xCF, 0x84, 0xCE, 0xB1, 0xCE, 0xB9];

static S_66_45: [symbol; 14] = [0xCE, 0xB9, 0xCE, 0xBF, 0xCF, 0x85, 0xCE, 0xBD, 0xCF, 0x84, 0xCE, 0xB1, 0xCE, 0xB9];

static S_66_46: [symbol; 10] = [0xCE, 0xBF, 0xCE, 0xBD, 0xCF, 0x84, 0xCE, 0xB1, 0xCE, 0xB9];

static S_66_47: [symbol; 10] = [0xCE, 0xBF, 0xCF, 0x85, 0xCE, 0xBC, 0xCE, 0xB1, 0xCE, 0xB9];

static S_66_48: [symbol; 8] = [0xCE, 0xB1, 0xCE, 0xBC, 0xCE, 0xB1, 0xCE, 0xB9];

static S_66_49: [symbol; 10] = [0xCE, 0xB9, 0xCE, 0xB5, 0xCE, 0xBC, 0xCE, 0xB1, 0xCE, 0xB9];

static S_66_50: [symbol; 8] = [0xCE, 0xBF, 0xCE, 0xBC, 0xCE, 0xB1, 0xCE, 0xB9];

static S_66_51: [symbol; 4] = [0xCE, 0xB5, 0xCE, 0xB9];

static S_66_52: [symbol; 8] = [0xCE, 0xB7, 0xCF, 0x83, 0xCE, 0xB5, 0xCE, 0xB9];

static S_66_53: [symbol; 6] = [0xCE, 0xB1, 0xCE, 0xB5, 0xCE, 0xB9];

static S_66_54: [symbol; 8] = [0xCE, 0xB7, 0xCE, 0xB8, 0xCE, 0xB5, 0xCE, 0xB9];

static S_66_55: [symbol; 4] = [0xCE, 0xBF, 0xCE, 0xB9];

static S_66_56: [symbol; 6] = [0xCE, 0xBF, 0xCF, 0x85, 0xCE, 0xBD];

static S_66_57: [symbol; 10] = [0xCE, 0xB7, 0xCF, 0x83, 0xCE, 0xBF, 0xCF, 0x85, 0xCE, 0xBD];

static S_66_58: [symbol; 10] = [0xCE, 0xBF, 0xCF, 0x83, 0xCE, 0xBF, 0xCF, 0x85, 0xCE, 0xBD];

static S_66_59: [symbol; 12] = [0xCE, 0xB9, 0xCE, 0xBF, 0xCF, 0x83, 0xCE, 0xBF, 0xCF, 0x85, 0xCE, 0xBD];

static S_66_60: [symbol; 10] = [0xCE, 0xB7, 0xCE, 0xB8, 0xCE, 0xBF, 0xCF, 0x85, 0xCE, 0xBD];

static S_66_61: [symbol; 10] = [0xCE, 0xBF, 0xCE, 0xBC, 0xCE, 0xBF, 0xCF, 0x85, 0xCE, 0xBD];

static S_66_62: [symbol; 12] = [0xCE, 0xB9, 0xCE, 0xBF, 0xCE, 0xBC, 0xCE, 0xBF, 0xCF, 0x85, 0xCE, 0xBD];

static S_66_63: [symbol; 4] = [0xCF, 0x89, 0xCE, 0xBD];

static S_66_64: [symbol; 8] = [0xCE, 0xB7, 0xCE, 0xB4, 0xCF, 0x89, 0xCE, 0xBD];

static S_66_65: [symbol; 4] = [0xCE, 0xB1, 0xCE, 0xBD];

static S_66_66: [symbol; 10] = [0xCE, 0xBF, 0xCF, 0x85, 0xCF, 0x83, 0xCE, 0xB1, 0xCE, 0xBD];

static S_66_67: [symbol; 16] = [0xCE, 0xBF, 0xCE, 0xBD, 0xCF, 0x84, 0xCE, 0xBF, 0xCF, 0x85, 0xCF, 0x83, 0xCE, 0xB1, 0xCE, 0xBD];

static S_66_68: [symbol; 18] = [0xCE, 0xB9, 0xCE, 0xBF, 0xCE, 0xBD, 0xCF, 0x84, 0xCE, 0xBF, 0xCF, 0x85, 0xCF, 0x83, 0xCE, 0xB1, 0xCE, 0xBD];

static S_66_69: [symbol; 8] = [0xCE, 0xB7, 0xCF, 0x83, 0xCE, 0xB1, 0xCE, 0xBD];

static S_66_70: [symbol; 14] = [0xCE, 0xBF, 0xCF, 0x83, 0xCE, 0xB1, 0xCF, 0x83, 0xCF, 0x84, 0xCE, 0xB1, 0xCE, 0xBD];

static S_66_71: [symbol; 16] = [0xCE, 0xB9, 0xCE, 0xBF, 0xCF, 0x83, 0xCE, 0xB1, 0xCF, 0x83, 0xCF, 0x84, 0xCE, 0xB1, 0xCE, 0xBD];

static S_66_72: [symbol; 14] = [0xCE, 0xBF, 0xCE, 0xBC, 0xCE, 0xB1, 0xCF, 0x83, 0xCF, 0x84, 0xCE, 0xB1, 0xCE, 0xBD];

static S_66_73: [symbol; 16] = [0xCE, 0xB9, 0xCE, 0xBF, 0xCE, 0xBC, 0xCE, 0xB1, 0xCF, 0x83, 0xCF, 0x84, 0xCE, 0xB1, 0xCE, 0xBD];

static S_66_74: [symbol; 12] = [0xCE, 0xBF, 0xCF, 0x85, 0xCE, 0xBD, 0xCF, 0x84, 0xCE, 0xB1, 0xCE, 0xBD];

static S_66_75: [symbol; 14] = [0xCE, 0xB9, 0xCE, 0xBF, 0xCF, 0x85, 0xCE, 0xBD, 0xCF, 0x84, 0xCE, 0xB1, 0xCE, 0xBD];

static S_66_76: [symbol; 10] = [0xCE, 0xBF, 0xCE, 0xBD, 0xCF, 0x84, 0xCE, 0xB1, 0xCE, 0xBD];

static S_66_77: [symbol; 12] = [0xCE, 0xB9, 0xCE, 0xBF, 0xCE, 0xBD, 0xCF, 0x84, 0xCE, 0xB1, 0xCE, 0xBD];

static S_66_78: [symbol; 8] = [0xCE, 0xBF, 0xCF, 0x84, 0xCE, 0xB1, 0xCE, 0xBD];

static S_66_79: [symbol; 10] = [0xCE, 0xB9, 0xCE, 0xBF, 0xCF, 0x84, 0xCE, 0xB1, 0xCE, 0xBD];

static S_66_80: [symbol; 8] = [0xCE, 0xB1, 0xCE, 0xB3, 0xCE, 0xB1, 0xCE, 0xBD];

static S_66_81: [symbol; 8] = [0xCE, 0xB7, 0xCE, 0xBA, 0xCE, 0xB1, 0xCE, 0xBD];

static S_66_82: [symbol; 12] = [0xCE, 0xB7, 0xCE, 0xB8, 0xCE, 0xB7, 0xCE, 0xBA, 0xCE, 0xB1, 0xCE, 0xBD];

static S_66_83: [symbol; 2] = [0xCE, 0xBF];

static A_66: [among; 84] = [
    among { s_size: 4, s: S_66_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_66_1.as_ptr(), substring_i: 0, result: 1, function: None },
    among { s_size: 4, s: S_66_2.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_66_3.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_66_4.as_ptr(), substring_i: 3, result: 1, function: None },
    among { s_size: 8, s: S_66_5.as_ptr(), substring_i: 3, result: 1, function: None },
    among { s_size: 4, s: S_66_6.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_66_7.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 10, s: S_66_8.as_ptr(), substring_i: 7, result: 1, function: None },
    among { s_size: 4, s: S_66_9.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_66_10.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_66_11.as_ptr(), substring_i: 10, result: 1, function: None },
    among { s_size: 2, s: S_66_12.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_66_13.as_ptr(), substring_i: 12, result: 1, function: None },
    among { s_size: 4, s: S_66_14.as_ptr(), substring_i: 12, result: 1, function: None },
    among { s_size: 6, s: S_66_15.as_ptr(), substring_i: 12, result: 1, function: None },
    among { s_size: 2, s: S_66_16.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 10, s: S_66_17.as_ptr(), substring_i: 16, result: 1, function: None },
    among { s_size: 12, s: S_66_18.as_ptr(), substring_i: 16, result: 1, function: None },
    among { s_size: 14, s: S_66_19.as_ptr(), substring_i: 18, result: 1, function: None },
    among { s_size: 12, s: S_66_20.as_ptr(), substring_i: 16, result: 1, function: None },
    among { s_size: 14, s: S_66_21.as_ptr(), substring_i: 20, result: 1, function: None },
    among { s_size: 2, s: S_66_22.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 14, s: S_66_23.as_ptr(), substring_i: 22, result: 1, function: None },
    among { s_size: 12, s: S_66_24.as_ptr(), substring_i: 22, result: 1, function: None },
    among { s_size: 14, s: S_66_25.as_ptr(), substring_i: 24, result: 1, function: None },
    among { s_size: 14, s: S_66_26.as_ptr(), substring_i: 22, result: 1, function: None },
    among { s_size: 16, s: S_66_27.as_ptr(), substring_i: 26, result: 1, function: None },
    among { s_size: 14, s: S_66_28.as_ptr(), substring_i: 22, result: 1, function: None },
    among { s_size: 12, s: S_66_29.as_ptr(), substring_i: 22, result: 1, function: None },
    among { s_size: 10, s: S_66_30.as_ptr(), substring_i: 22, result: 1, function: None },
    among { s_size: 10, s: S_66_31.as_ptr(), substring_i: 22, result: 1, function: None },
    among { s_size: 10, s: S_66_32.as_ptr(), substring_i: 22, result: 1, function: None },
    among { s_size: 14, s: S_66_33.as_ptr(), substring_i: 32, result: 1, function: None },
    among { s_size: 8, s: S_66_34.as_ptr(), substring_i: 22, result: 1, function: None },
    among { s_size: 12, s: S_66_35.as_ptr(), substring_i: 34, result: 1, function: None },
    among { s_size: 2, s: S_66_36.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_66_37.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_66_38.as_ptr(), substring_i: 37, result: 1, function: None },
    among { s_size: 8, s: S_66_39.as_ptr(), substring_i: 37, result: 1, function: None },
    among { s_size: 10, s: S_66_40.as_ptr(), substring_i: 39, result: 1, function: None },
    among { s_size: 8, s: S_66_41.as_ptr(), substring_i: 37, result: 1, function: None },
    among { s_size: 8, s: S_66_42.as_ptr(), substring_i: 37, result: 1, function: None },
    among { s_size: 10, s: S_66_43.as_ptr(), substring_i: 42, result: 1, function: None },
    among { s_size: 12, s: S_66_44.as_ptr(), substring_i: 37, result: 1, function: None },
    among { s_size: 14, s: S_66_45.as_ptr(), substring_i: 44, result: 1, function: None },
    among { s_size: 10, s: S_66_46.as_ptr(), substring_i: 37, result: 1, function: None },
    among { s_size: 10, s: S_66_47.as_ptr(), substring_i: 37, result: 1, function: None },
    among { s_size: 8, s: S_66_48.as_ptr(), substring_i: 37, result: 1, function: None },
    among { s_size: 10, s: S_66_49.as_ptr(), substring_i: 37, result: 1, function: None },
    among { s_size: 8, s: S_66_50.as_ptr(), substring_i: 37, result: 1, function: None },
    among { s_size: 4, s: S_66_51.as_ptr(), substring_i: 37, result: 1, function: None },
    among { s_size: 8, s: S_66_52.as_ptr(), substring_i: 51, result: 1, function: None },
    among { s_size: 6, s: S_66_53.as_ptr(), substring_i: 51, result: 1, function: None },
    among { s_size: 8, s: S_66_54.as_ptr(), substring_i: 51, result: 1, function: None },
    among { s_size: 4, s: S_66_55.as_ptr(), substring_i: 37, result: 1, function: None },
    among { s_size: 6, s: S_66_56.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 10, s: S_66_57.as_ptr(), substring_i: 56, result: 1, function: None },
    among { s_size: 10, s: S_66_58.as_ptr(), substring_i: 56, result: 1, function: None },
    among { s_size: 12, s: S_66_59.as_ptr(), substring_i: 58, result: 1, function: None },
    among { s_size: 10, s: S_66_60.as_ptr(), substring_i: 56, result: 1, function: None },
    among { s_size: 10, s: S_66_61.as_ptr(), substring_i: 56, result: 1, function: None },
    among { s_size: 12, s: S_66_62.as_ptr(), substring_i: 61, result: 1, function: None },
    among { s_size: 4, s: S_66_63.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_66_64.as_ptr(), substring_i: 63, result: 1, function: None },
    among { s_size: 4, s: S_66_65.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 10, s: S_66_66.as_ptr(), substring_i: 65, result: 1, function: None },
    among { s_size: 16, s: S_66_67.as_ptr(), substring_i: 66, result: 1, function: None },
    among { s_size: 18, s: S_66_68.as_ptr(), substring_i: 67, result: 1, function: None },
    among { s_size: 8, s: S_66_69.as_ptr(), substring_i: 65, result: 1, function: None },
    among { s_size: 14, s: S_66_70.as_ptr(), substring_i: 65, result: 1, function: None },
    among { s_size: 16, s: S_66_71.as_ptr(), substring_i: 70, result: 1, function: None },
    among { s_size: 14, s: S_66_72.as_ptr(), substring_i: 65, result: 1, function: None },
    among { s_size: 16, s: S_66_73.as_ptr(), substring_i: 72, result: 1, function: None },
    among { s_size: 12, s: S_66_74.as_ptr(), substring_i: 65, result: 1, function: None },
    among { s_size: 14, s: S_66_75.as_ptr(), substring_i: 74, result: 1, function: None },
    among { s_size: 10, s: S_66_76.as_ptr(), substring_i: 65, result: 1, function: None },
    among { s_size: 12, s: S_66_77.as_ptr(), substring_i: 76, result: 1, function: None },
    among { s_size: 8, s: S_66_78.as_ptr(), substring_i: 65, result: 1, function: None },
    among { s_size: 10, s: S_66_79.as_ptr(), substring_i: 78, result: 1, function: None },
    among { s_size: 8, s: S_66_80.as_ptr(), substring_i: 65, result: 1, function: None },
    among { s_size: 8, s: S_66_81.as_ptr(), substring_i: 65, result: 1, function: None },
    among { s_size: 12, s: S_66_82.as_ptr(), substring_i: 81, result: 1, function: None },
    among { s_size: 2, s: S_66_83.as_ptr(), substring_i: -1, result: 1, function: None },
];

static S_67_0: [symbol; 10] = [0xCE, 0xB5, 0xCF, 0x83, 0xCF, 0x84, 0xCE, 0xB5, 0xCF, 0x81];

static S_67_1: [symbol; 8] = [0xCF, 0x85, 0xCF, 0x84, 0xCE, 0xB5, 0xCF, 0x81];

static S_67_2: [symbol; 8] = [0xCF, 0x89, 0xCF, 0x84, 0xCE, 0xB5, 0xCF, 0x81];

static S_67_3: [symbol; 8] = [0xCE, 0xBF, 0xCF, 0x84, 0xCE, 0xB5, 0xCF, 0x81];

static S_67_4: [symbol; 10] = [0xCE, 0xB5, 0xCF, 0x83, 0xCF, 0x84, 0xCE, 0xB1, 0xCF, 0x84];

static S_67_5: [symbol; 8] = [0xCF, 0x85, 0xCF, 0x84, 0xCE, 0xB1, 0xCF, 0x84];

static S_67_6: [symbol; 8] = [0xCF, 0x89, 0xCF, 0x84, 0xCE, 0xB1, 0xCF, 0x84];

static S_67_7: [symbol; 8] = [0xCE, 0xBF, 0xCF, 0x84, 0xCE, 0xB1, 0xCF, 0x84];

static A_67: [among; 8] = [
    among { s_size: 10, s: S_67_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_67_1.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_67_2.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_67_3.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 10, s: S_67_4.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_67_5.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_67_6.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_67_7.as_ptr(), substring_i: -1, result: 1, function: None },
];

static G_V: [c_uchar; 4] = [81, 65, 16, 1];

static G_V2: [c_uchar; 4] = [81, 65, 0, 1];

static S_0: [symbol; 2] = [0xCE, 0xB1];

static S_1: [symbol; 2] = [0xCE, 0xB2];

static S_2: [symbol; 2] = [0xCE, 0xB3];

static S_3: [symbol; 2] = [0xCE, 0xB4];

static S_4: [symbol; 2] = [0xCE, 0xB5];

static S_5: [symbol; 2] = [0xCE, 0xB6];

static S_6: [symbol; 2] = [0xCE, 0xB7];

static S_7: [symbol; 2] = [0xCE, 0xB8];

static S_8: [symbol; 2] = [0xCE, 0xB9];

static S_9: [symbol; 2] = [0xCE, 0xBA];

static S_10: [symbol; 2] = [0xCE, 0xBB];

static S_11: [symbol; 2] = [0xCE, 0xBC];

static S_12: [symbol; 2] = [0xCE, 0xBD];

static S_13: [symbol; 2] = [0xCE, 0xBE];

static S_14: [symbol; 2] = [0xCE, 0xBF];

static S_15: [symbol; 2] = [0xCF, 0x80];

static S_16: [symbol; 2] = [0xCF, 0x81];

static S_17: [symbol; 2] = [0xCF, 0x83];

static S_18: [symbol; 2] = [0xCF, 0x84];

static S_19: [symbol; 2] = [0xCF, 0x85];

static S_20: [symbol; 2] = [0xCF, 0x86];

static S_21: [symbol; 2] = [0xCF, 0x87];

static S_22: [symbol; 2] = [0xCF, 0x88];

static S_23: [symbol; 2] = [0xCF, 0x89];

static S_24: [symbol; 4] = [0xCF, 0x86, 0xCE, 0xB1];

static S_25: [symbol; 6] = [0xCF, 0x83, 0xCE, 0xBA, 0xCE, 0xB1];

static S_26: [symbol; 6] = [0xCE, 0xBF, 0xCE, 0xBB, 0xCE, 0xBF];

static S_27: [symbol; 4] = [0xCF, 0x83, 0xCE, 0xBF];

static S_28: [symbol; 8] = [0xCF, 0x84, 0xCE, 0xB1, 0xCF, 0x84, 0xCE, 0xBF];

static S_29: [symbol; 6] = [0xCE, 0xBA, 0xCF, 0x81, 0xCE, 0xB5];

static S_30: [symbol; 6] = [0xCF, 0x80, 0xCE, 0xB5, 0xCF, 0x81];

static S_31: [symbol; 6] = [0xCF, 0x84, 0xCE, 0xB5, 0xCF, 0x81];

static S_32: [symbol; 4] = [0xCF, 0x86, 0xCF, 0x89];

static S_33: [symbol; 12] = [0xCE, 0xBA, 0xCE, 0xB1, 0xCE, 0xB8, 0xCE, 0xB5, 0xCF, 0x83, 0xCF, 0x84];

static S_34: [symbol; 10] = [0xCE, 0xB3, 0xCE, 0xB5, 0xCE, 0xB3, 0xCE, 0xBF, 0xCE, 0xBD];

static S_35: [symbol; 2] = [0xCE, 0xB9];

static S_36: [symbol; 4] = [0xCE, 0xB9, 0xCE, 0xB6];

static S_37: [symbol; 4] = [0xCF, 0x89, 0xCE, 0xBD];

static S_38: [symbol; 6] = [0xCE, 0xB9, 0xCF, 0x83, 0xCE, 0xB1];

static S_39: [symbol; 4] = [0xCE, 0xB9, 0xCF, 0x83];

static S_40: [symbol; 2] = [0xCE, 0xB9];

static S_41: [symbol; 4] = [0xCE, 0xB9, 0xCF, 0x83];

static S_42: [symbol; 2] = [0xCE, 0xB9];

static S_43: [symbol; 2] = [0xCE, 0xB9];

static S_44: [symbol; 6] = [0xCE, 0xB9, 0xCF, 0x83, 0xCF, 0x84];

static S_45: [symbol; 6] = [0xCE, 0xB9, 0xCF, 0x83, 0xCE, 0xBC];

static S_46: [symbol; 2] = [0xCE, 0xB9];

static S_47: [symbol; 12] = [0xCE, 0xB1, 0xCE, 0xB3, 0xCE, 0xBD, 0xCF, 0x89, 0xCF, 0x83, 0xCF, 0x84];

static S_48: [symbol; 8] = [0xCE, 0xB1, 0xCF, 0x84, 0xCE, 0xBF, 0xCE, 0xBC];

static S_49: [symbol; 10] = [0xCE, 0xB3, 0xCE, 0xBD, 0xCF, 0x89, 0xCF, 0x83, 0xCF, 0x84];

static S_50: [symbol; 6] = [0xCE, 0xB5, 0xCE, 0xB8, 0xCE, 0xBD];

static S_51: [symbol; 12] = [0xCE, 0xB5, 0xCE, 0xBA, 0xCE, 0xBB, 0xCE, 0xB5, 0xCE, 0xBA, 0xCF, 0x84];

static S_52: [symbol; 10] = [0xCF, 0x83, 0xCE, 0xBA, 0xCE, 0xB5, 0xCF, 0x80, 0xCF, 0x84];

static S_53: [symbol; 6] = [0xCF, 0x84, 0xCE, 0xBF, 0xCF, 0x80];

static S_54: [symbol; 16] = [0xCE, 0xB1, 0xCE, 0xBB, 0xCE, 0xB5, 0xCE, 0xBE, 0xCE, 0xB1, 0xCE, 0xBD, 0xCE, 0xB4, 0xCF, 0x81];

static S_55: [symbol; 12] = [0xCE, 0xB2, 0xCF, 0x85, 0xCE, 0xB6, 0xCE, 0xB1, 0xCE, 0xBD, 0xCF, 0x84];

static S_56: [symbol; 10] = [0xCE, 0xB8, 0xCE, 0xB5, 0xCE, 0xB1, 0xCF, 0x84, 0xCF, 0x81];

static S_57: [symbol; 8] = [0xCE, 0xB1, 0xCF, 0x81, 0xCE, 0xB1, 0xCE, 0xBA];

static S_58: [symbol; 4] = [0xCE, 0xB1, 0xCE, 0xBA];

static S_59: [symbol; 6] = [0xCE, 0xB9, 0xCF, 0x84, 0xCF, 0x83];

static S_60: [symbol; 6] = [0xCE, 0xBA, 0xCE, 0xBF, 0xCF, 0x81];

static S_61: [symbol; 6] = [0xCE, 0xB9, 0xCF, 0x84, 0xCF, 0x83];

static S_62: [symbol; 4] = [0xCE, 0xB9, 0xCE, 0xB4];

static S_63: [symbol; 4] = [0xCE, 0xB9, 0xCE, 0xB4];

static S_64: [symbol; 6] = [0xCE, 0xB9, 0xCF, 0x83, 0xCE, 0xBA];

static S_65: [symbol; 4] = [0xCE, 0xB1, 0xCE, 0xB4];

static S_66: [symbol; 4] = [0xCE, 0xB5, 0xCE, 0xB4];

static S_67: [symbol; 6] = [0xCE, 0xBF, 0xCF, 0x85, 0xCE, 0xB4];

static S_68: [symbol; 2] = [0xCE, 0xB5];

static S_69: [symbol; 2] = [0xCE, 0xB9];

static S_70: [symbol; 4] = [0xCE, 0xB9, 0xCE, 0xBA];

static S_71: [symbol; 4] = [0xCE, 0xB9, 0xCE, 0xBA];

static S_72: [symbol; 10] = [0xCE, 0xB1, 0xCE, 0xB3, 0xCE, 0xB1, 0xCE, 0xBC, 0xCE, 0xB5];

static S_73: [symbol; 8] = [0xCE, 0xB1, 0xCE, 0xB3, 0xCE, 0xB1, 0xCE, 0xBC];

static S_74: [symbol; 6] = [0xCE, 0xB1, 0xCE, 0xBC, 0xCE, 0xB5];

static S_75: [symbol; 4] = [0xCE, 0xB1, 0xCE, 0xBC];

static S_76: [symbol; 8] = [0xCE, 0xB1, 0xCE, 0xB3, 0xCE, 0xB1, 0xCE, 0xBD];

static S_77: [symbol; 6] = [0xCE, 0xB1, 0xCE, 0xBD, 0xCE, 0xB5];

static S_78: [symbol; 4] = [0xCE, 0xB1, 0xCE, 0xBD];

static S_79: [symbol; 4] = [0xCE, 0xB1, 0xCE, 0xBD];

static S_80: [symbol; 6] = [0xCE, 0xB5, 0xCF, 0x84, 0xCE, 0xB5];

static S_81: [symbol; 4] = [0xCE, 0xB5, 0xCF, 0x84];

static S_82: [symbol; 4] = [0xCE, 0xB5, 0xCF, 0x84];

static S_83: [symbol; 4] = [0xCE, 0xB5, 0xCF, 0x84];

static S_84: [symbol; 6] = [0xCE, 0xB1, 0xCF, 0x81, 0xCF, 0x87];

static S_85: [symbol; 6] = [0xCE, 0xBF, 0xCE, 0xBD, 0xCF, 0x84];

static S_86: [symbol; 6] = [0xCE, 0xBA, 0xCF, 0x81, 0xCE, 0xB5];

static S_87: [symbol; 6] = [0xCF, 0x89, 0xCE, 0xBD, 0xCF, 0x84];

static S_88: [symbol; 4] = [0xCE, 0xBF, 0xCE, 0xBD];

static S_89: [symbol; 10] = [0xCE, 0xBF, 0xCE, 0xBC, 0xCE, 0xB1, 0xCF, 0x83, 0xCF, 0x84];

static S_90: [symbol; 10] = [0xCE, 0xB9, 0xCE, 0xB5, 0xCF, 0x83, 0xCF, 0x84, 0xCE, 0xB5];

static S_91: [symbol; 8] = [0xCE, 0xB9, 0xCE, 0xB5, 0xCF, 0x83, 0xCF, 0x84];

static S_92: [symbol; 8] = [0xCE, 0xB5, 0xCF, 0x83, 0xCF, 0x84, 0xCE, 0xB5];

static S_93: [symbol; 8] = [0xCE, 0xB9, 0xCE, 0xB5, 0xCF, 0x83, 0xCF, 0x84];

static S_94: [symbol; 4] = [0xCE, 0xB7, 0xCE, 0xBA];

static S_95: [symbol; 4] = [0xCE, 0xB7, 0xCE, 0xBA];

static S_96: [symbol; 6] = [0xCE, 0xBF, 0xCF, 0x85, 0xCF, 0x83];

static S_97: [symbol; 6] = [0xCE, 0xBF, 0xCF, 0x85, 0xCF, 0x83];

static S_98: [symbol; 8] = [0xCE, 0xBA, 0xCE, 0xBF, 0xCE, 0xBB, 0xCE, 0xBB];

static S_99: [symbol; 4] = [0xCE, 0xB1, 0xCE, 0xB3];

static S_100: [symbol; 4] = [0xCE, 0xB1, 0xCE, 0xB3];

static S_101: [symbol; 4] = [0xCE, 0xB1, 0xCE, 0xB3];

static S_102: [symbol; 4] = [0xCE, 0xB7, 0xCF, 0x83];

static S_103: [symbol; 6] = [0xCE, 0xB7, 0xCF, 0x83, 0xCF, 0x84];

static S_104: [symbol; 6] = [0xCE, 0xBF, 0xCF, 0x85, 0xCE, 0xBD];

static S_105: [symbol; 6] = [0xCE, 0xBF, 0xCF, 0x85, 0xCE, 0xBC];

static S_106: [symbol; 4] = [0xCE, 0xBC, 0xCE, 0xB1];

// ---------------------------------------------------------------------------
// rules
// ---------------------------------------------------------------------------


unsafe fn r_has_min_length(z: *mut SN_env) -> c_int {
    (len_utf8((*z).p) >= 3) as c_int
}

unsafe fn r_tolower(z: *mut SN_env) -> c_int {
    let mut among_var;
    'grk0: loop {
        let m1 = (*z).l - (*z).c;
        'lab0: {
            (*z).ket = (*z).c;
            among_var = find_among_b(z, A_0.as_ptr(), 46);
            (*z).bra = (*z).c;
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
                3 => {
                    let ret = slice_from_s(z, 2, S_2.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
                4 => {
                    let ret = slice_from_s(z, 2, S_3.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
                5 => {
                    let ret = slice_from_s(z, 2, S_4.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
                6 => {
                    let ret = slice_from_s(z, 2, S_5.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
                7 => {
                    let ret = slice_from_s(z, 2, S_6.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
                8 => {
                    let ret = slice_from_s(z, 2, S_7.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
                9 => {
                    let ret = slice_from_s(z, 2, S_8.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
                10 => {
                    let ret = slice_from_s(z, 2, S_9.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
                11 => {
                    let ret = slice_from_s(z, 2, S_10.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
                12 => {
                    let ret = slice_from_s(z, 2, S_11.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
                13 => {
                    let ret = slice_from_s(z, 2, S_12.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
                14 => {
                    let ret = slice_from_s(z, 2, S_13.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
                15 => {
                    let ret = slice_from_s(z, 2, S_14.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
                16 => {
                    let ret = slice_from_s(z, 2, S_15.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
                17 => {
                    let ret = slice_from_s(z, 2, S_16.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
                18 => {
                    let ret = slice_from_s(z, 2, S_17.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
                19 => {
                    let ret = slice_from_s(z, 2, S_18.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
                20 => {
                    let ret = slice_from_s(z, 2, S_19.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
                21 => {
                    let ret = slice_from_s(z, 2, S_20.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
                22 => {
                    let ret = slice_from_s(z, 2, S_21.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
                23 => {
                    let ret = slice_from_s(z, 2, S_22.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
                24 => {
                    let ret = slice_from_s(z, 2, S_23.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
                25 => {
                    let ret = skip_b_utf8((*z).p, (*z).c, (*z).lb, 1);
                    if ret < 0 {
                        break 'lab0;
                    }
                    (*z).c = ret;
                }
                _ => {}
            }
            continue 'grk0;
        }
        // lab0:
        (*z).c = (*z).l - m1;
        break;
    }
    1
}

unsafe fn r_step_1(z: *mut SN_env) -> c_int {
    let among_var;
    (*z).ket = (*z).c;
    among_var = find_among_b(z, A_1.as_ptr(), 40);
    if among_var == 0 {
        return 0;
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
            let ret = slice_from_s(z, 4, S_27.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        5 => {
            let ret = slice_from_s(z, 8, S_28.as_ptr());
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
            let ret = slice_from_s(z, 6, S_30.as_ptr());
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
            let ret = slice_from_s(z, 4, S_32.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        10 => {
            let ret = slice_from_s(z, 12, S_33.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        11 => {
            let ret = slice_from_s(z, 10, S_34.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        _ => {}
    }
    *(*z).I.offset(0) = 0;
    1
}

unsafe fn r_step_s1(z: *mut SN_env) -> c_int {
    let among_var;
    (*z).ket = (*z).c;
    if find_among_b(z, A_3.as_ptr(), 14) == 0 {
        return 0;
    }
    (*z).bra = (*z).c;
    {
        let ret = slice_del(z);
        if ret < 0 {
            return ret;
        }
    }
    *(*z).I.offset(0) = 0;
    (*z).ket = (*z).c;
    (*z).bra = (*z).c;
    among_var = find_among_b(z, A_2.as_ptr(), 31);
    if among_var == 0 {
        return 0;
    }
    if (*z).c > (*z).lb {
        return 0;
    }
    match among_var {
        1 => {
            let ret = slice_from_s(z, 2, S_35.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        2 => {
            let ret = slice_from_s(z, 4, S_36.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        _ => {}
    }
    1
}

unsafe fn r_step_s2(z: *mut SN_env) -> c_int {
    (*z).ket = (*z).c;
    if find_among_b(z, A_5.as_ptr(), 7) == 0 {
        return 0;
    }
    (*z).bra = (*z).c;
    {
        let ret = slice_del(z);
        if ret < 0 {
            return ret;
        }
    }
    *(*z).I.offset(0) = 0;
    (*z).ket = (*z).c;
    (*z).bra = (*z).c;
    if find_among_b(z, A_4.as_ptr(), 8) == 0 {
        return 0;
    }
    if (*z).c > (*z).lb {
        return 0;
    }
    {
        let ret = slice_from_s(z, 4, S_37.as_ptr());
        if ret < 0 {
            return ret;
        }
    }
    1
}

unsafe fn r_step_s3(z: *mut SN_env) -> c_int {
    {
        let m1 = (*z).l - (*z).c;
        'lab0: {
            'lab1: {
                (*z).ket = (*z).c;
                if eq_s_b(z, 6, S_38.as_ptr()) == 0 {
                    break 'lab1;
                }
                (*z).bra = (*z).c;
                if (*z).c > (*z).lb {
                    break 'lab1;
                }
                {
                    let ret = slice_from_s(z, 4, S_39.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
                break 'lab0;
            }
            // lab1:
            (*z).c = (*z).l - m1;
            (*z).ket = (*z).c;
        }
    }
    // lab0:
    if find_among_b(z, A_7.as_ptr(), 7) == 0 {
        return 0;
    }
    (*z).bra = (*z).c;
    {
        let ret = slice_del(z);
        if ret < 0 {
            return ret;
        }
    }
    *(*z).I.offset(0) = 0;
    (*z).ket = (*z).c;
    (*z).bra = (*z).c;
    let among_var = find_among_b(z, A_6.as_ptr(), 32);
    if among_var == 0 {
        return 0;
    }
    if (*z).c > (*z).lb {
        return 0;
    }
    match among_var {
        1 => {
            let ret = slice_from_s(z, 2, S_40.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        2 => {
            let ret = slice_from_s(z, 4, S_41.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        _ => {}
    }
    1
}

unsafe fn r_step_s4(z: *mut SN_env) -> c_int {
    (*z).ket = (*z).c;
    if find_among_b(z, A_9.as_ptr(), 7) == 0 {
        return 0;
    }
    (*z).bra = (*z).c;
    {
        let ret = slice_del(z);
        if ret < 0 {
            return ret;
        }
    }
    *(*z).I.offset(0) = 0;
    (*z).ket = (*z).c;
    (*z).bra = (*z).c;
    if (*z).c - 3 <= (*z).lb
        || *(*z).p.offset(((*z).c - 1) as isize) as c_int >> 5 != 5
        || (-2145255424i32 >> (*(*z).p.offset(((*z).c - 1) as isize) as c_int & 0x1f)) & 1 == 0
    {
        return 0;
    }
    if find_among_b(z, A_8.as_ptr(), 19) == 0 {
        return 0;
    }
    if (*z).c > (*z).lb {
        return 0;
    }
    {
        let ret = slice_from_s(z, 2, S_42.as_ptr());
        if ret < 0 {
            return ret;
        }
    }
    1
}

unsafe fn r_step_s5(z: *mut SN_env) -> c_int {
    let among_var;
    (*z).ket = (*z).c;
    if find_among_b(z, A_11.as_ptr(), 11) == 0 {
        return 0;
    }
    (*z).bra = (*z).c;
    {
        let ret = slice_del(z);
        if ret < 0 {
            return ret;
        }
    }
    *(*z).I.offset(0) = 0;
    (*z).ket = (*z).c;
    (*z).bra = (*z).c;
    among_var = find_among_b(z, A_10.as_ptr(), 40);
    if among_var == 0 {
        return 0;
    }
    if (*z).c > (*z).lb {
        return 0;
    }
    match among_var {
        1 => {
            let ret = slice_from_s(z, 2, S_43.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        2 => {
            let ret = slice_from_s(z, 6, S_44.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        _ => {}
    }
    1
}

unsafe fn r_step_s6(z: *mut SN_env) -> c_int {
    let mut among_var;
    (*z).ket = (*z).c;
    if find_among_b(z, A_14.as_ptr(), 6) == 0 {
        return 0;
    }
    (*z).bra = (*z).c;
    {
        let ret = slice_del(z);
        if ret < 0 {
            return ret;
        }
    }
    *(*z).I.offset(0) = 0;
    {
        let m1 = (*z).l - (*z).c;
        'lab0: {
            'lab1: {
                (*z).ket = (*z).c;
                (*z).bra = (*z).c;
                if (*z).c - 3 <= (*z).lb || *(*z).p.offset(((*z).c - 1) as isize) != 181 {
                    break 'lab1;
                }
                among_var = find_among_b(z, A_12.as_ptr(), 7);
                if among_var == 0 {
                    break 'lab1;
                }
                if (*z).c > (*z).lb {
                    break 'lab1;
                }
                match among_var {
                    1 => {
                        let ret = slice_from_s(z, 6, S_45.as_ptr());
                        if ret < 0 {
                            return ret;
                        }
                    }
                    2 => {
                        let ret = slice_from_s(z, 2, S_46.as_ptr());
                        if ret < 0 {
                            return ret;
                        }
                    }
                    _ => {}
                }
                break 'lab0;
            }
            // lab1:
            (*z).c = (*z).l - m1;
            (*z).ket = (*z).c;
            if (*z).c - 9 <= (*z).lb
                || (*(*z).p.offset(((*z).c - 1) as isize) != 186
                    && *(*z).p.offset(((*z).c - 1) as isize) != 189)
            {
                return 0;
            }
            among_var = find_among_b(z, A_13.as_ptr(), 10);
            if among_var == 0 {
                return 0;
            }
            (*z).bra = (*z).c;
            match among_var {
                1 => {
                    let ret = slice_from_s(z, 12, S_47.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
                2 => {
                    let ret = slice_from_s(z, 8, S_48.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
                3 => {
                    let ret = slice_from_s(z, 10, S_49.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
                4 => {
                    let ret = slice_from_s(z, 6, S_50.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
                5 => {
                    let ret = slice_from_s(z, 12, S_51.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
                6 => {
                    let ret = slice_from_s(z, 10, S_52.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
                7 => {
                    let ret = slice_from_s(z, 6, S_53.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
                8 => {
                    let ret = slice_from_s(z, 16, S_54.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
                9 => {
                    let ret = slice_from_s(z, 12, S_55.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
                10 => {
                    let ret = slice_from_s(z, 10, S_56.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
                _ => {}
            }
        }
    }
    // lab0:
    1
}

unsafe fn r_step_s7(z: *mut SN_env) -> c_int {
    (*z).ket = (*z).c;
    if (*z).c - 9 <= (*z).lb
        || (*(*z).p.offset(((*z).c - 1) as isize) != 177
            && *(*z).p.offset(((*z).c - 1) as isize) != 185)
    {
        return 0;
    }
    if find_among_b(z, A_16.as_ptr(), 4) == 0 {
        return 0;
    }
    (*z).bra = (*z).c;
    {
        let ret = slice_del(z);
        if ret < 0 {
            return ret;
        }
    }
    *(*z).I.offset(0) = 0;
    (*z).ket = (*z).c;
    (*z).bra = (*z).c;
    if (*z).c - 1 <= (*z).lb
        || (*(*z).p.offset(((*z).c - 1) as isize) != 131
            && *(*z).p.offset(((*z).c - 1) as isize) != 135)
    {
        return 0;
    }
    if find_among_b(z, A_15.as_ptr(), 2) == 0 {
        return 0;
    }
    if (*z).c > (*z).lb {
        return 0;
    }
    {
        let ret = slice_from_s(z, 8, S_57.as_ptr());
        if ret < 0 {
            return ret;
        }
    }
    1
}

unsafe fn r_step_s8(z: *mut SN_env) -> c_int {
    let among_var;
    (*z).ket = (*z).c;
    if find_among_b(z, A_18.as_ptr(), 8) == 0 {
        return 0;
    }
    (*z).bra = (*z).c;
    {
        let ret = slice_del(z);
        if ret < 0 {
            return ret;
        }
    }
    *(*z).I.offset(0) = 0;
    {
        let m1 = (*z).l - (*z).c;
        'lab0: {
            'lab1: {
                (*z).ket = (*z).c;
                (*z).bra = (*z).c;
                among_var = find_among_b(z, A_17.as_ptr(), 46);
                if among_var == 0 {
                    break 'lab1;
                }
                if (*z).c > (*z).lb {
                    break 'lab1;
                }
                match among_var {
                    1 => {
                        let ret = slice_from_s(z, 4, S_58.as_ptr());
                        if ret < 0 {
                            return ret;
                        }
                    }
                    2 => {
                        let ret = slice_from_s(z, 6, S_59.as_ptr());
                        if ret < 0 {
                            return ret;
                        }
                    }
                    _ => {}
                }
                break 'lab0;
            }
            // lab1:
            (*z).c = (*z).l - m1;
            (*z).ket = (*z).c;
            (*z).bra = (*z).c;
            if eq_s_b(z, 6, S_60.as_ptr()) == 0 {
                return 0;
            }
            {
                let ret = slice_from_s(z, 6, S_61.as_ptr());
                if ret < 0 {
                    return ret;
                }
            }
        }
    }
    // lab0:
    1
}

unsafe fn r_step_s9(z: *mut SN_env) -> c_int {
    (*z).ket = (*z).c;
    if (*z).c - 7 <= (*z).lb
        || *(*z).p.offset(((*z).c - 1) as isize) as c_int >> 5 != 5
        || (-1610481664i32 >> (*(*z).p.offset(((*z).c - 1) as isize) as c_int & 0x1f)) & 1 == 0
    {
        return 0;
    }
    if find_among_b(z, A_21.as_ptr(), 3) == 0 {
        return 0;
    }
    (*z).bra = (*z).c;
    {
        let ret = slice_del(z);
        if ret < 0 {
            return ret;
        }
    }
    *(*z).I.offset(0) = 0;
    {
        let m1 = (*z).l - (*z).c;
        'lab0: {
            'lab1: {
                (*z).ket = (*z).c;
                (*z).bra = (*z).c;
                if find_among_b(z, A_19.as_ptr(), 4) == 0 {
                    break 'lab1;
                }
                if (*z).c > (*z).lb {
                    break 'lab1;
                }
                {
                    let ret = slice_from_s(z, 4, S_62.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
                break 'lab0;
            }
            // lab1:
            (*z).c = (*z).l - m1;
            (*z).ket = (*z).c;
            (*z).bra = (*z).c;
            if (*z).c - 1 <= (*z).lb
                || (*(*z).p.offset(((*z).c - 1) as isize) != 181
                    && *(*z).p.offset(((*z).c - 1) as isize) != 189)
            {
                return 0;
            }
            if find_among_b(z, A_20.as_ptr(), 2) == 0 {
                return 0;
            }
            {
                let ret = slice_from_s(z, 4, S_63.as_ptr());
                if ret < 0 {
                    return ret;
                }
            }
        }
    }
    // lab0:
    1
}

unsafe fn r_step_s10(z: *mut SN_env) -> c_int {
    (*z).ket = (*z).c;
    if find_among_b(z, A_23.as_ptr(), 4) == 0 {
        return 0;
    }
    (*z).bra = (*z).c;
    {
        let ret = slice_del(z);
        if ret < 0 {
            return ret;
        }
    }
    *(*z).I.offset(0) = 0;
    (*z).ket = (*z).c;
    (*z).bra = (*z).c;
    if find_among_b(z, A_22.as_ptr(), 7) == 0 {
        return 0;
    }
    if (*z).c > (*z).lb {
        return 0;
    }
    {
        let ret = slice_from_s(z, 6, S_64.as_ptr());
        if ret < 0 {
            return ret;
        }
    }
    1
}

unsafe fn r_step_2a(z: *mut SN_env) -> c_int {
    (*z).ket = (*z).c;
    if (*z).c - 7 <= (*z).lb
        || (*(*z).p.offset(((*z).c - 1) as isize) != 131
            && *(*z).p.offset(((*z).c - 1) as isize) != 189)
    {
        return 0;
    }
    if find_among_b(z, A_24.as_ptr(), 2) == 0 {
        return 0;
    }
    (*z).bra = (*z).c;
    {
        let ret = slice_del(z);
        if ret < 0 {
            return ret;
        }
    }
    {
        let m1 = (*z).l - (*z).c;
        'lab0: {
            if find_among_b(z, A_25.as_ptr(), 10) == 0 {
                break 'lab0;
            }
            return 0;
        }
        // lab0:
        (*z).c = (*z).l - m1;
    }
    {
        let ret;
        {
            let saved_c = (*z).c;
            ret = insert_s(z, (*z).c, (*z).c, 4, S_65.as_ptr());
            (*z).c = saved_c;
        }
        if ret < 0 {
            return ret;
        }
    }
    1
}

unsafe fn r_step_2b(z: *mut SN_env) -> c_int {
    (*z).ket = (*z).c;
    if (*z).c - 7 <= (*z).lb
        || (*(*z).p.offset(((*z).c - 1) as isize) != 131
            && *(*z).p.offset(((*z).c - 1) as isize) != 189)
    {
        return 0;
    }
    if find_among_b(z, A_26.as_ptr(), 2) == 0 {
        return 0;
    }
    (*z).bra = (*z).c;
    {
        let ret = slice_del(z);
        if ret < 0 {
            return ret;
        }
    }
    (*z).ket = (*z).c;
    (*z).bra = (*z).c;
    if (*z).c - 3 <= (*z).lb
        || (*(*z).p.offset(((*z).c - 1) as isize) != 128
            && *(*z).p.offset(((*z).c - 1) as isize) != 187)
    {
        return 0;
    }
    if find_among_b(z, A_27.as_ptr(), 8) == 0 {
        return 0;
    }
    {
        let ret = slice_from_s(z, 4, S_66.as_ptr());
        if ret < 0 {
            return ret;
        }
    }
    1
}

unsafe fn r_step_2c(z: *mut SN_env) -> c_int {
    (*z).ket = (*z).c;
    if (*z).c - 9 <= (*z).lb
        || (*(*z).p.offset(((*z).c - 1) as isize) != 131
            && *(*z).p.offset(((*z).c - 1) as isize) != 189)
    {
        return 0;
    }
    if find_among_b(z, A_28.as_ptr(), 2) == 0 {
        return 0;
    }
    (*z).bra = (*z).c;
    {
        let ret = slice_del(z);
        if ret < 0 {
            return ret;
        }
    }
    (*z).ket = (*z).c;
    (*z).bra = (*z).c;
    if find_among_b(z, A_29.as_ptr(), 15) == 0 {
        return 0;
    }
    {
        let ret = slice_from_s(z, 6, S_67.as_ptr());
        if ret < 0 {
            return ret;
        }
    }
    1
}

unsafe fn r_step_2d(z: *mut SN_env) -> c_int {
    (*z).ket = (*z).c;
    if (*z).c - 5 <= (*z).lb
        || (*(*z).p.offset(((*z).c - 1) as isize) != 131
            && *(*z).p.offset(((*z).c - 1) as isize) != 189)
    {
        return 0;
    }
    if find_among_b(z, A_30.as_ptr(), 2) == 0 {
        return 0;
    }
    (*z).bra = (*z).c;
    {
        let ret = slice_del(z);
        if ret < 0 {
            return ret;
        }
    }
    *(*z).I.offset(0) = 0;
    (*z).ket = (*z).c;
    (*z).bra = (*z).c;
    if find_among_b(z, A_31.as_ptr(), 8) == 0 {
        return 0;
    }
    if (*z).c > (*z).lb {
        return 0;
    }
    {
        let ret = slice_from_s(z, 2, S_68.as_ptr());
        if ret < 0 {
            return ret;
        }
    }
    1
}

unsafe fn r_step_3(z: *mut SN_env) -> c_int {
    (*z).ket = (*z).c;
    if find_among_b(z, A_32.as_ptr(), 3) == 0 {
        return 0;
    }
    (*z).bra = (*z).c;
    {
        let ret = slice_del(z);
        if ret < 0 {
            return ret;
        }
    }
    *(*z).I.offset(0) = 0;
    (*z).ket = (*z).c;
    (*z).bra = (*z).c;
    if in_grouping_b_U(z, G_V.as_ptr(), 945, 969, 0) != 0 {
        return 0;
    }
    {
        let ret = slice_from_s(z, 2, S_69.as_ptr());
        if ret < 0 {
            return ret;
        }
    }
    1
}

unsafe fn r_step_4(z: *mut SN_env) -> c_int {
    (*z).ket = (*z).c;
    if find_among_b(z, A_33.as_ptr(), 4) == 0 {
        return 0;
    }
    (*z).bra = (*z).c;
    {
        let ret = slice_del(z);
        if ret < 0 {
            return ret;
        }
    }
    *(*z).I.offset(0) = 0;
    {
        let m1 = (*z).l - (*z).c;
        'lab0: {
            'lab1: {
                (*z).ket = (*z).c;
                (*z).bra = (*z).c;
                if in_grouping_b_U(z, G_V.as_ptr(), 945, 969, 0) != 0 {
                    break 'lab1;
                }
                {
                    let ret = slice_from_s(z, 4, S_70.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
                break 'lab0;
            }
            // lab1:
            (*z).c = (*z).l - m1;
            (*z).ket = (*z).c;
        }
    }
    // lab0:
    (*z).bra = (*z).c;
    if find_among_b(z, A_34.as_ptr(), 36) == 0 {
        return 0;
    }
    if (*z).c > (*z).lb {
        return 0;
    }
    {
        let ret = slice_from_s(z, 4, S_71.as_ptr());
        if ret < 0 {
            return ret;
        }
    }
    1
}

unsafe fn r_step_5a(z: *mut SN_env) -> c_int {
    {
        let m1 = (*z).l - (*z).c;
        'lab0: {
            (*z).ket = (*z).c;
            if eq_s_b(z, 10, S_72.as_ptr()) == 0 {
                break 'lab0;
            }
            (*z).bra = (*z).c;
            if (*z).c > (*z).lb {
                break 'lab0;
            }
            {
                let ret = slice_from_s(z, 8, S_73.as_ptr());
                if ret < 0 {
                    return ret;
                }
            }
        }
        // lab0:
        (*z).c = (*z).l - m1;
    }
    {
        let m2 = (*z).l - (*z).c;
        'lab1: {
            (*z).ket = (*z).c;
            if (*z).c - 9 <= (*z).lb || *(*z).p.offset(((*z).c - 1) as isize) != 181 {
                break 'lab1;
            }
            if find_among_b(z, A_35.as_ptr(), 5) == 0 {
                break 'lab1;
            }
            (*z).bra = (*z).c;
            {
                let ret = slice_del(z);
                if ret < 0 {
                    return ret;
                }
            }
            *(*z).I.offset(0) = 0;
        }
        // lab1:
        (*z).c = (*z).l - m2;
    }
    (*z).ket = (*z).c;
    if eq_s_b(z, 6, S_74.as_ptr()) == 0 {
        return 0;
    }
    (*z).bra = (*z).c;
    {
        let ret = slice_del(z);
        if ret < 0 {
            return ret;
        }
    }
    *(*z).I.offset(0) = 0;
    (*z).ket = (*z).c;
    (*z).bra = (*z).c;
    if find_among_b(z, A_36.as_ptr(), 12) == 0 {
        return 0;
    }
    if (*z).c > (*z).lb {
        return 0;
    }
    {
        let ret = slice_from_s(z, 4, S_75.as_ptr());
        if ret < 0 {
            return ret;
        }
    }
    1
}

unsafe fn r_step_5b(z: *mut SN_env) -> c_int {
    {
        let m1 = (*z).l - (*z).c;
        'lab0: {
            (*z).ket = (*z).c;
            if (*z).c - 9 <= (*z).lb || *(*z).p.offset(((*z).c - 1) as isize) != 181 {
                break 'lab0;
            }
            if find_among_b(z, A_38.as_ptr(), 11) == 0 {
                break 'lab0;
            }
            (*z).bra = (*z).c;
            {
                let ret = slice_del(z);
                if ret < 0 {
                    return ret;
                }
            }
            *(*z).I.offset(0) = 0;
            (*z).ket = (*z).c;
            (*z).bra = (*z).c;
            if (*z).c - 3 <= (*z).lb
                || (*(*z).p.offset(((*z).c - 1) as isize) != 129
                    && *(*z).p.offset(((*z).c - 1) as isize) != 131)
            {
                break 'lab0;
            }
            if find_among_b(z, A_37.as_ptr(), 2) == 0 {
                break 'lab0;
            }
            if (*z).c > (*z).lb {
                break 'lab0;
            }
            {
                let ret = slice_from_s(z, 8, S_76.as_ptr());
                if ret < 0 {
                    return ret;
                }
            }
        }
        // lab0:
        (*z).c = (*z).l - m1;
    }
    (*z).ket = (*z).c;
    if eq_s_b(z, 6, S_77.as_ptr()) == 0 {
        return 0;
    }
    (*z).bra = (*z).c;
    {
        let ret = slice_del(z);
        if ret < 0 {
            return ret;
        }
    }
    *(*z).I.offset(0) = 0;
    {
        let m2 = (*z).l - (*z).c;
        'lab1: {
            'lab2: {
                (*z).ket = (*z).c;
                (*z).bra = (*z).c;
                if in_grouping_b_U(z, G_V2.as_ptr(), 945, 969, 0) != 0 {
                    break 'lab2;
                }
                {
                    let ret = slice_from_s(z, 4, S_78.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
                break 'lab1;
            }
            // lab2:
            (*z).c = (*z).l - m2;
            (*z).ket = (*z).c;
        }
    }
    // lab1:
    (*z).bra = (*z).c;
    if find_among_b(z, A_39.as_ptr(), 95) == 0 {
        return 0;
    }
    if (*z).c > (*z).lb {
        return 0;
    }
    {
        let ret = slice_from_s(z, 4, S_79.as_ptr());
        if ret < 0 {
            return ret;
        }
    }
    1
}

unsafe fn r_step_5c(z: *mut SN_env) -> c_int {
    {
        let m1 = (*z).l - (*z).c;
        'lab0: {
            (*z).ket = (*z).c;
            if (*z).c - 9 <= (*z).lb || *(*z).p.offset(((*z).c - 1) as isize) != 181 {
                break 'lab0;
            }
            if find_among_b(z, A_40.as_ptr(), 1) == 0 {
                break 'lab0;
            }
            (*z).bra = (*z).c;
            {
                let ret = slice_del(z);
                if ret < 0 {
                    return ret;
                }
            }
            *(*z).I.offset(0) = 0;
        }
        // lab0:
        (*z).c = (*z).l - m1;
    }
    (*z).ket = (*z).c;
    if eq_s_b(z, 6, S_80.as_ptr()) == 0 {
        return 0;
    }
    (*z).bra = (*z).c;
    {
        let ret = slice_del(z);
        if ret < 0 {
            return ret;
        }
    }
    *(*z).I.offset(0) = 0;
    {
        let m2 = (*z).l - (*z).c;
        'lab1: {
            'lab2: {
                (*z).ket = (*z).c;
                (*z).bra = (*z).c;
                if in_grouping_b_U(z, G_V2.as_ptr(), 945, 969, 0) != 0 {
                    break 'lab2;
                }
                {
                    let ret = slice_from_s(z, 4, S_81.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
                break 'lab1;
            }
            // lab2:
            (*z).c = (*z).l - m2;
            'lab3: {
                (*z).ket = (*z).c;
                (*z).bra = (*z).c;
                if find_among_b(z, A_41.as_ptr(), 31) == 0 {
                    break 'lab3;
                }
                {
                    let ret = slice_from_s(z, 4, S_82.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
                break 'lab1;
            }
            // lab3:
            (*z).c = (*z).l - m2;
            (*z).ket = (*z).c;
        }
    }
    // lab1:
    (*z).bra = (*z).c;
    if find_among_b(z, A_42.as_ptr(), 25) == 0 {
        return 0;
    }
    if (*z).c > (*z).lb {
        return 0;
    }
    {
        let ret = slice_from_s(z, 4, S_83.as_ptr());
        if ret < 0 {
            return ret;
        }
    }
    1
}

unsafe fn r_step_5d(z: *mut SN_env) -> c_int {
    (*z).ket = (*z).c;
    if (*z).c - 9 <= (*z).lb || *(*z).p.offset(((*z).c - 1) as isize) != 131 {
        return 0;
    }
    if find_among_b(z, A_43.as_ptr(), 2) == 0 {
        return 0;
    }
    (*z).bra = (*z).c;
    {
        let ret = slice_del(z);
        if ret < 0 {
            return ret;
        }
    }
    *(*z).I.offset(0) = 0;
    {
        let m1 = (*z).l - (*z).c;
        'lab0: {
            'lab1: {
                (*z).ket = (*z).c;
                (*z).bra = (*z).c;
                if eq_s_b(z, 6, S_84.as_ptr()) == 0 {
                    break 'lab1;
                }
                if (*z).c > (*z).lb {
                    break 'lab1;
                }
                {
                    let ret = slice_from_s(z, 6, S_85.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
                break 'lab0;
            }
            // lab1:
            (*z).c = (*z).l - m1;
            (*z).ket = (*z).c;
            (*z).bra = (*z).c;
            if eq_s_b(z, 6, S_86.as_ptr()) == 0 {
                return 0;
            }
            {
                let ret = slice_from_s(z, 6, S_87.as_ptr());
                if ret < 0 {
                    return ret;
                }
            }
        }
    }
    // lab0:
    1
}

unsafe fn r_step_5e(z: *mut SN_env) -> c_int {
    (*z).ket = (*z).c;
    if (*z).c - 11 <= (*z).lb || *(*z).p.offset(((*z).c - 1) as isize) != 181 {
        return 0;
    }
    if find_among_b(z, A_44.as_ptr(), 2) == 0 {
        return 0;
    }
    (*z).bra = (*z).c;
    {
        let ret = slice_del(z);
        if ret < 0 {
            return ret;
        }
    }
    *(*z).I.offset(0) = 0;
    (*z).ket = (*z).c;
    (*z).bra = (*z).c;
    if eq_s_b(z, 4, S_88.as_ptr()) == 0 {
        return 0;
    }
    if (*z).c > (*z).lb {
        return 0;
    }
    {
        let ret = slice_from_s(z, 10, S_89.as_ptr());
        if ret < 0 {
            return ret;
        }
    }
    1
}

unsafe fn r_step_5f(z: *mut SN_env) -> c_int {
    {
        let m1 = (*z).l - (*z).c;
        'lab0: {
            (*z).ket = (*z).c;
            if eq_s_b(z, 10, S_90.as_ptr()) == 0 {
                break 'lab0;
            }
            (*z).bra = (*z).c;
            {
                let ret = slice_del(z);
                if ret < 0 {
                    return ret;
                }
            }
            *(*z).I.offset(0) = 0;
            (*z).ket = (*z).c;
            (*z).bra = (*z).c;
            if (*z).c - 1 <= (*z).lb
                || (*(*z).p.offset(((*z).c - 1) as isize) != 128
                    && *(*z).p.offset(((*z).c - 1) as isize) != 134)
            {
                break 'lab0;
            }
            if find_among_b(z, A_45.as_ptr(), 6) == 0 {
                break 'lab0;
            }
            if (*z).c > (*z).lb {
                break 'lab0;
            }
            {
                let ret = slice_from_s(z, 8, S_91.as_ptr());
                if ret < 0 {
                    return ret;
                }
            }
        }
        // lab0:
        (*z).c = (*z).l - m1;
    }
    (*z).ket = (*z).c;
    if eq_s_b(z, 8, S_92.as_ptr()) == 0 {
        return 0;
    }
    (*z).bra = (*z).c;
    {
        let ret = slice_del(z);
        if ret < 0 {
            return ret;
        }
    }
    *(*z).I.offset(0) = 0;
    (*z).ket = (*z).c;
    (*z).bra = (*z).c;
    if find_among_b(z, A_46.as_ptr(), 9) == 0 {
        return 0;
    }
    if (*z).c > (*z).lb {
        return 0;
    }
    {
        let ret = slice_from_s(z, 8, S_93.as_ptr());
        if ret < 0 {
            return ret;
        }
    }
    1
}

unsafe fn r_step_5g(z: *mut SN_env) -> c_int {
    {
        let m1 = (*z).l - (*z).c;
        'lab0: {
            (*z).ket = (*z).c;
            if find_among_b(z, A_47.as_ptr(), 3) == 0 {
                break 'lab0;
            }
            (*z).bra = (*z).c;
            {
                let ret = slice_del(z);
                if ret < 0 {
                    return ret;
                }
            }
            *(*z).I.offset(0) = 0;
        }
        // lab0:
        (*z).c = (*z).l - m1;
    }
    (*z).ket = (*z).c;
    if find_among_b(z, A_50.as_ptr(), 3) == 0 {
        return 0;
    }
    (*z).bra = (*z).c;
    {
        let ret = slice_del(z);
        if ret < 0 {
            return ret;
        }
    }
    *(*z).I.offset(0) = 0;
    {
        let m2 = (*z).l - (*z).c;
        'lab1: {
            'lab2: {
                (*z).ket = (*z).c;
                (*z).bra = (*z).c;
                if find_among_b(z, A_48.as_ptr(), 6) == 0 {
                    break 'lab2;
                }
                {
                    let ret = slice_from_s(z, 4, S_94.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
                break 'lab1;
            }
            // lab2:
            (*z).c = (*z).l - m2;
            (*z).ket = (*z).c;
            (*z).bra = (*z).c;
            if (*z).c - 1 <= (*z).lb || *(*z).p.offset(((*z).c - 1) as isize) != 184 {
                return 0;
            }
            if find_among_b(z, A_49.as_ptr(), 5) == 0 {
                return 0;
            }
            if (*z).c > (*z).lb {
                return 0;
            }
            {
                let ret = slice_from_s(z, 4, S_95.as_ptr());
                if ret < 0 {
                    return ret;
                }
            }
        }
    }
    // lab1:
    1
}

unsafe fn r_step_5h(z: *mut SN_env) -> c_int {
    (*z).ket = (*z).c;
    if find_among_b(z, A_53.as_ptr(), 3) == 0 {
        return 0;
    }
    (*z).bra = (*z).c;
    {
        let ret = slice_del(z);
        if ret < 0 {
            return ret;
        }
    }
    *(*z).I.offset(0) = 0;
    {
        let m1 = (*z).l - (*z).c;
        'lab0: {
            'lab1: {
                (*z).ket = (*z).c;
                (*z).bra = (*z).c;
                if find_among_b(z, A_51.as_ptr(), 12) == 0 {
                    break 'lab1;
                }
                {
                    let ret = slice_from_s(z, 6, S_96.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
                break 'lab0;
            }
            // lab1:
            (*z).c = (*z).l - m1;
            (*z).ket = (*z).c;
            (*z).bra = (*z).c;
            if find_among_b(z, A_52.as_ptr(), 25) == 0 {
                return 0;
            }
            if (*z).c > (*z).lb {
                return 0;
            }
            {
                let ret = slice_from_s(z, 6, S_97.as_ptr());
                if ret < 0 {
                    return ret;
                }
            }
        }
    }
    // lab0:
    1
}

unsafe fn r_step_5i(z: *mut SN_env) -> c_int {
    let among_var;
    (*z).ket = (*z).c;
    if find_among_b(z, A_56.as_ptr(), 3) == 0 {
        return 0;
    }
    (*z).bra = (*z).c;
    {
        let ret = slice_del(z);
        if ret < 0 {
            return ret;
        }
    }
    *(*z).I.offset(0) = 0;
    {
        let m1 = (*z).l - (*z).c;
        'lab0: {
            'lab1: {
                (*z).ket = (*z).c;
                (*z).bra = (*z).c;
                if eq_s_b(z, 8, S_98.as_ptr()) == 0 {
                    break 'lab1;
                }
                {
                    let ret = slice_from_s(z, 4, S_99.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
                break 'lab0;
            }
            // lab1:
            (*z).c = (*z).l - m1;
            {
                let m2 = (*z).l - (*z).c;
                'lab2: {
                    'lab3: {
                        (*z).ket = (*z).c;
                        (*z).bra = (*z).c;
                        among_var = find_among_b(z, A_54.as_ptr(), 12);
                        if among_var == 0 {
                            break 'lab3;
                        }
                        match among_var {
                            1 => {
                                let ret = slice_from_s(z, 4, S_100.as_ptr());
                                if ret < 0 {
                                    return ret;
                                }
                            }
                            _ => {}
                        }
                        break 'lab2;
                    }
                    // lab3:
                    (*z).c = (*z).l - m2;
                    (*z).ket = (*z).c;
                    (*z).bra = (*z).c;
                    if find_among_b(z, A_55.as_ptr(), 44) == 0 {
                        return 0;
                    }
                    if (*z).c > (*z).lb {
                        return 0;
                    }
                    {
                        let ret = slice_from_s(z, 4, S_101.as_ptr());
                        if ret < 0 {
                            return ret;
                        }
                    }
                }
                // lab2:
            }
        }
    }
    // lab0:
    1
}

unsafe fn r_step_5j(z: *mut SN_env) -> c_int {
    (*z).ket = (*z).c;
    if find_among_b(z, A_57.as_ptr(), 3) == 0 {
        return 0;
    }
    (*z).bra = (*z).c;
    {
        let ret = slice_del(z);
        if ret < 0 {
            return ret;
        }
    }
    *(*z).I.offset(0) = 0;
    (*z).ket = (*z).c;
    (*z).bra = (*z).c;
    if (*z).c - 1 <= (*z).lb || *(*z).p.offset(((*z).c - 1) as isize) != 189 {
        return 0;
    }
    if find_among_b(z, A_58.as_ptr(), 6) == 0 {
        return 0;
    }
    if (*z).c > (*z).lb {
        return 0;
    }
    {
        let ret = slice_from_s(z, 4, S_102.as_ptr());
        if ret < 0 {
            return ret;
        }
    }
    1
}

unsafe fn r_step_5k(z: *mut SN_env) -> c_int {
    (*z).ket = (*z).c;
    if (*z).c - 7 <= (*z).lb || *(*z).p.offset(((*z).c - 1) as isize) != 181 {
        return 0;
    }
    if find_among_b(z, A_59.as_ptr(), 1) == 0 {
        return 0;
    }
    (*z).bra = (*z).c;
    {
        let ret = slice_del(z);
        if ret < 0 {
            return ret;
        }
    }
    *(*z).I.offset(0) = 0;
    (*z).ket = (*z).c;
    (*z).bra = (*z).c;
    if find_among_b(z, A_60.as_ptr(), 10) == 0 {
        return 0;
    }
    if (*z).c > (*z).lb {
        return 0;
    }
    {
        let ret = slice_from_s(z, 6, S_103.as_ptr());
        if ret < 0 {
            return ret;
        }
    }
    1
}

unsafe fn r_step_5l(z: *mut SN_env) -> c_int {
    (*z).ket = (*z).c;
    if (*z).c - 7 <= (*z).lb || *(*z).p.offset(((*z).c - 1) as isize) != 181 {
        return 0;
    }
    if find_among_b(z, A_61.as_ptr(), 3) == 0 {
        return 0;
    }
    (*z).bra = (*z).c;
    {
        let ret = slice_del(z);
        if ret < 0 {
            return ret;
        }
    }
    *(*z).I.offset(0) = 0;
    (*z).ket = (*z).c;
    (*z).bra = (*z).c;
    if find_among_b(z, A_62.as_ptr(), 6) == 0 {
        return 0;
    }
    if (*z).c > (*z).lb {
        return 0;
    }
    {
        let ret = slice_from_s(z, 6, S_104.as_ptr());
        if ret < 0 {
            return ret;
        }
    }
    1
}

unsafe fn r_step_5m(z: *mut SN_env) -> c_int {
    (*z).ket = (*z).c;
    if (*z).c - 7 <= (*z).lb || *(*z).p.offset(((*z).c - 1) as isize) != 181 {
        return 0;
    }
    if find_among_b(z, A_63.as_ptr(), 3) == 0 {
        return 0;
    }
    (*z).bra = (*z).c;
    {
        let ret = slice_del(z);
        if ret < 0 {
            return ret;
        }
    }
    *(*z).I.offset(0) = 0;
    (*z).ket = (*z).c;
    (*z).bra = (*z).c;
    if find_among_b(z, A_64.as_ptr(), 7) == 0 {
        return 0;
    }
    if (*z).c > (*z).lb {
        return 0;
    }
    {
        let ret = slice_from_s(z, 6, S_105.as_ptr());
        if ret < 0 {
            return ret;
        }
    }
    1
}

unsafe fn r_step_6(z: *mut SN_env) -> c_int {
    {
        let m1 = (*z).l - (*z).c;
        'lab0: {
            (*z).ket = (*z).c;
            if find_among_b(z, A_65.as_ptr(), 3) == 0 {
                break 'lab0;
            }
            (*z).bra = (*z).c;
            {
                let ret = slice_from_s(z, 4, S_106.as_ptr());
                if ret < 0 {
                    return ret;
                }
            }
        }
        // lab0:
        (*z).c = (*z).l - m1;
    }
    if *(*z).I.offset(0) == 0 {
        return 0;
    }
    (*z).ket = (*z).c;
    if find_among_b(z, A_66.as_ptr(), 84) == 0 {
        return 0;
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

unsafe fn r_step_7(z: *mut SN_env) -> c_int {
    (*z).ket = (*z).c;
    if (*z).c - 7 <= (*z).lb
        || (*(*z).p.offset(((*z).c - 1) as isize) != 129
            && *(*z).p.offset(((*z).c - 1) as isize) != 132)
    {
        return 0;
    }
    if find_among_b(z, A_67.as_ptr(), 8) == 0 {
        return 0;
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

// ---------------------------------------------------------------------------
// exported entry points
// ---------------------------------------------------------------------------

#[no_mangle]
pub unsafe extern "C" fn greek_UTF_8_stem(z: *mut SN_env) -> c_int {
    (*z).lb = (*z).c;
    (*z).c = (*z).l;

    {
        let m1 = (*z).l - (*z).c;
        {
            let ret = r_tolower(z);
            if ret < 0 {
                return ret;
            }
        }
        (*z).c = (*z).l - m1;
    }
    {
        let ret = r_has_min_length(z);
        if ret <= 0 {
            return ret;
        }
    }
    *(*z).I.offset(0) = 1;
    macro_rules! step {
        ($f:ident) => {{
            let m = (*z).l - (*z).c;
            {
                let ret = $f(z);
                if ret < 0 {
                    return ret;
                }
            }
            (*z).c = (*z).l - m;
        }};
    }
    step!(r_step_1);
    step!(r_step_s1);
    step!(r_step_s2);
    step!(r_step_s3);
    step!(r_step_s4);
    step!(r_step_s5);
    step!(r_step_s6);
    step!(r_step_s7);
    step!(r_step_s8);
    step!(r_step_s9);
    step!(r_step_s10);
    step!(r_step_2a);
    step!(r_step_2b);
    step!(r_step_2c);
    step!(r_step_2d);
    step!(r_step_3);
    step!(r_step_4);
    step!(r_step_5a);
    step!(r_step_5b);
    step!(r_step_5c);
    step!(r_step_5d);
    step!(r_step_5e);
    step!(r_step_5f);
    step!(r_step_5g);
    step!(r_step_5h);
    step!(r_step_5j);
    step!(r_step_5i);
    step!(r_step_5k);
    step!(r_step_5l);
    step!(r_step_5m);
    step!(r_step_6);
    step!(r_step_7);
    (*z).c = (*z).lb;
    1
}

#[no_mangle]
pub unsafe extern "C" fn greek_UTF_8_create_env() -> *mut SN_env {
    SN_create_env(0, 1)
}

#[no_mangle]
pub unsafe extern "C" fn greek_UTF_8_close_env(z: *mut SN_env) {
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
        let z = greek_UTF_8_create_env();
        assert!(!z.is_null());
        let rc = SN_set_current(z, word.len() as c_int, word.as_ptr());
        assert!(rc >= 0);
        let rc = greek_UTF_8_stem(z);
        assert!(rc >= 0, "stem returned {rc}");
        let out = std::slice::from_raw_parts((*z).p, SIZE((*z).p) as usize).to_vec();
        greek_UTF_8_close_env(z);
        out
    }

    // A short word below the minimum length is returned unchanged.
    #[test]
    fn short_word_unchanged() {
        unsafe {
            // two Greek letters (4 bytes) - below the 3-codepoint minimum.
            let w = [0xCE, 0xB1, 0xCE, 0xB2];
            assert_eq!(stem(&w), w.to_vec());
        }
    }

    // Idempotence: stemming a stem yields the same stem.
    #[test]
    fn idempotent() {
        unsafe {
            // a few common Greek inflected forms (UTF-8 bytes).
            let words: [&[u8]; 2] = [
                &[0xCE, 0xB1, 0xCE, 0xBD, 0xCE, 0xB8, 0xCF, 0x81, 0xCF, 0x89, 0xCF, 0x80, 0xCE, 0xBF, 0xCF, 0x83],
                &[0xCF, 0x83, 0xCF, 0x80, 0xCE, 0xB9, 0xCF, 0x84, 0xCE, 0xB9, 0xCE, 0xB1],
            ];
            for w in words {
                let once = stem(w);
                let twice = stem(&once);
                assert_eq!(once, twice, "not idempotent for {:?}", w);
                assert!(!once.is_empty());
            }
        }
    }
}
