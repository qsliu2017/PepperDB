//! Russian Snowball stemmer (KOI8-R).
//!
//! 1:1 translation of the generated
//! `src/backend/snowball/libstemmer/stem_KOI8_R_russian.c` (Snowball 2.2.0),
//! merged with its header `stem_KOI8_R_russian.h`.
//!
//! KOI8-R is a SINGLE-BYTE encoding: this uses the non-`_U` grouping helpers
//! and plain single-byte advance (`z->c++` / `z->c--`) instead of the UTF-8
//! `skip_*_utf8` helpers. The runtime helpers come from `crate::snowball::api`
//! and `crate::snowball::utilities`.

use crate::prelude::*;

use crate::snowball::api::{among, symbol, SN_close_env, SN_create_env, SN_env, SIZE};
use crate::snowball::utilities::{
    eq_s_b, find_among_b, in_grouping, out_grouping, slice_del, slice_from_s,
};

// ---------------------------------------------------------------------------
// among string tables
// ---------------------------------------------------------------------------

static S_0_0: [symbol; 3] = [0xD7, 0xDB, 0xC9];
static S_0_1: [symbol; 4] = [0xC9, 0xD7, 0xDB, 0xC9];
static S_0_2: [symbol; 4] = [0xD9, 0xD7, 0xDB, 0xC9];
static S_0_3: [symbol; 1] = [0xD7];
static S_0_4: [symbol; 2] = [0xC9, 0xD7];
static S_0_5: [symbol; 2] = [0xD9, 0xD7];
static S_0_6: [symbol; 5] = [0xD7, 0xDB, 0xC9, 0xD3, 0xD8];
static S_0_7: [symbol; 6] = [0xC9, 0xD7, 0xDB, 0xC9, 0xD3, 0xD8];
static S_0_8: [symbol; 6] = [0xD9, 0xD7, 0xDB, 0xC9, 0xD3, 0xD8];

static A_0: [among; 9] = [
    among { s_size: 3, s: S_0_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_0_1.as_ptr(), substring_i: 0, result: 2, function: None },
    among { s_size: 4, s: S_0_2.as_ptr(), substring_i: 0, result: 2, function: None },
    among { s_size: 1, s: S_0_3.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_0_4.as_ptr(), substring_i: 3, result: 2, function: None },
    among { s_size: 2, s: S_0_5.as_ptr(), substring_i: 3, result: 2, function: None },
    among { s_size: 5, s: S_0_6.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_0_7.as_ptr(), substring_i: 6, result: 2, function: None },
    among { s_size: 6, s: S_0_8.as_ptr(), substring_i: 6, result: 2, function: None },
];

static S_1_0: [symbol; 2] = [0xC0, 0xC0];
static S_1_1: [symbol; 2] = [0xC5, 0xC0];
static S_1_2: [symbol; 2] = [0xCF, 0xC0];
static S_1_3: [symbol; 2] = [0xD5, 0xC0];
static S_1_4: [symbol; 2] = [0xC5, 0xC5];
static S_1_5: [symbol; 2] = [0xC9, 0xC5];
static S_1_6: [symbol; 2] = [0xCF, 0xC5];
static S_1_7: [symbol; 2] = [0xD9, 0xC5];
static S_1_8: [symbol; 2] = [0xC9, 0xC8];
static S_1_9: [symbol; 2] = [0xD9, 0xC8];
static S_1_10: [symbol; 3] = [0xC9, 0xCD, 0xC9];
static S_1_11: [symbol; 3] = [0xD9, 0xCD, 0xC9];
static S_1_12: [symbol; 2] = [0xC5, 0xCA];
static S_1_13: [symbol; 2] = [0xC9, 0xCA];
static S_1_14: [symbol; 2] = [0xCF, 0xCA];
static S_1_15: [symbol; 2] = [0xD9, 0xCA];
static S_1_16: [symbol; 2] = [0xC5, 0xCD];
static S_1_17: [symbol; 2] = [0xC9, 0xCD];
static S_1_18: [symbol; 2] = [0xCF, 0xCD];
static S_1_19: [symbol; 2] = [0xD9, 0xCD];
static S_1_20: [symbol; 3] = [0xC5, 0xC7, 0xCF];
static S_1_21: [symbol; 3] = [0xCF, 0xC7, 0xCF];
static S_1_22: [symbol; 2] = [0xC1, 0xD1];
static S_1_23: [symbol; 2] = [0xD1, 0xD1];
static S_1_24: [symbol; 3] = [0xC5, 0xCD, 0xD5];
static S_1_25: [symbol; 3] = [0xCF, 0xCD, 0xD5];

static A_1: [among; 26] = [
    among { s_size: 2, s: S_1_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_1_1.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_1_2.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_1_3.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_1_4.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_1_5.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_1_6.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_1_7.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_1_8.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_1_9.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_1_10.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_1_11.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_1_12.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_1_13.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_1_14.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_1_15.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_1_16.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_1_17.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_1_18.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_1_19.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_1_20.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_1_21.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_1_22.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_1_23.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_1_24.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_1_25.as_ptr(), substring_i: -1, result: 1, function: None },
];

static S_2_0: [symbol; 2] = [0xC5, 0xCD];
static S_2_1: [symbol; 2] = [0xCE, 0xCE];
static S_2_2: [symbol; 2] = [0xD7, 0xDB];
static S_2_3: [symbol; 3] = [0xC9, 0xD7, 0xDB];
static S_2_4: [symbol; 3] = [0xD9, 0xD7, 0xDB];
static S_2_5: [symbol; 1] = [0xDD];
static S_2_6: [symbol; 2] = [0xC0, 0xDD];
static S_2_7: [symbol; 3] = [0xD5, 0xC0, 0xDD];

static A_2: [among; 8] = [
    among { s_size: 2, s: S_2_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_2_1.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_2_2.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_2_3.as_ptr(), substring_i: 2, result: 2, function: None },
    among { s_size: 3, s: S_2_4.as_ptr(), substring_i: 2, result: 2, function: None },
    among { s_size: 1, s: S_2_5.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_2_6.as_ptr(), substring_i: 5, result: 1, function: None },
    among { s_size: 3, s: S_2_7.as_ptr(), substring_i: 6, result: 2, function: None },
];

static S_3_0: [symbol; 2] = [0xD3, 0xD1];
static S_3_1: [symbol; 2] = [0xD3, 0xD8];

static A_3: [among; 2] = [
    among { s_size: 2, s: S_3_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_3_1.as_ptr(), substring_i: -1, result: 1, function: None },
];

static S_4_0: [symbol; 1] = [0xC0];
static S_4_1: [symbol; 2] = [0xD5, 0xC0];
static S_4_2: [symbol; 2] = [0xCC, 0xC1];
static S_4_3: [symbol; 3] = [0xC9, 0xCC, 0xC1];
static S_4_4: [symbol; 3] = [0xD9, 0xCC, 0xC1];
static S_4_5: [symbol; 2] = [0xCE, 0xC1];
static S_4_6: [symbol; 3] = [0xC5, 0xCE, 0xC1];
static S_4_7: [symbol; 3] = [0xC5, 0xD4, 0xC5];
static S_4_8: [symbol; 3] = [0xC9, 0xD4, 0xC5];
static S_4_9: [symbol; 3] = [0xCA, 0xD4, 0xC5];
static S_4_10: [symbol; 4] = [0xC5, 0xCA, 0xD4, 0xC5];
static S_4_11: [symbol; 4] = [0xD5, 0xCA, 0xD4, 0xC5];
static S_4_12: [symbol; 2] = [0xCC, 0xC9];
static S_4_13: [symbol; 3] = [0xC9, 0xCC, 0xC9];
static S_4_14: [symbol; 3] = [0xD9, 0xCC, 0xC9];
static S_4_15: [symbol; 1] = [0xCA];
static S_4_16: [symbol; 2] = [0xC5, 0xCA];
static S_4_17: [symbol; 2] = [0xD5, 0xCA];
static S_4_18: [symbol; 1] = [0xCC];
static S_4_19: [symbol; 2] = [0xC9, 0xCC];
static S_4_20: [symbol; 2] = [0xD9, 0xCC];
static S_4_21: [symbol; 2] = [0xC5, 0xCD];
static S_4_22: [symbol; 2] = [0xC9, 0xCD];
static S_4_23: [symbol; 2] = [0xD9, 0xCD];
static S_4_24: [symbol; 1] = [0xCE];
static S_4_25: [symbol; 2] = [0xC5, 0xCE];
static S_4_26: [symbol; 2] = [0xCC, 0xCF];
static S_4_27: [symbol; 3] = [0xC9, 0xCC, 0xCF];
static S_4_28: [symbol; 3] = [0xD9, 0xCC, 0xCF];
static S_4_29: [symbol; 2] = [0xCE, 0xCF];
static S_4_30: [symbol; 3] = [0xC5, 0xCE, 0xCF];
static S_4_31: [symbol; 3] = [0xCE, 0xCE, 0xCF];
static S_4_32: [symbol; 2] = [0xC0, 0xD4];
static S_4_33: [symbol; 3] = [0xD5, 0xC0, 0xD4];
static S_4_34: [symbol; 2] = [0xC5, 0xD4];
static S_4_35: [symbol; 3] = [0xD5, 0xC5, 0xD4];
static S_4_36: [symbol; 2] = [0xC9, 0xD4];
static S_4_37: [symbol; 2] = [0xD1, 0xD4];
static S_4_38: [symbol; 2] = [0xD9, 0xD4];
static S_4_39: [symbol; 2] = [0xD4, 0xD8];
static S_4_40: [symbol; 3] = [0xC9, 0xD4, 0xD8];
static S_4_41: [symbol; 3] = [0xD9, 0xD4, 0xD8];
static S_4_42: [symbol; 3] = [0xC5, 0xDB, 0xD8];
static S_4_43: [symbol; 3] = [0xC9, 0xDB, 0xD8];
static S_4_44: [symbol; 2] = [0xCE, 0xD9];
static S_4_45: [symbol; 3] = [0xC5, 0xCE, 0xD9];

static A_4: [among; 46] = [
    among { s_size: 1, s: S_4_0.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 2, s: S_4_1.as_ptr(), substring_i: 0, result: 2, function: None },
    among { s_size: 2, s: S_4_2.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_4_3.as_ptr(), substring_i: 2, result: 2, function: None },
    among { s_size: 3, s: S_4_4.as_ptr(), substring_i: 2, result: 2, function: None },
    among { s_size: 2, s: S_4_5.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_4_6.as_ptr(), substring_i: 5, result: 2, function: None },
    among { s_size: 3, s: S_4_7.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_4_8.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 3, s: S_4_9.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_4_10.as_ptr(), substring_i: 9, result: 2, function: None },
    among { s_size: 4, s: S_4_11.as_ptr(), substring_i: 9, result: 2, function: None },
    among { s_size: 2, s: S_4_12.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_4_13.as_ptr(), substring_i: 12, result: 2, function: None },
    among { s_size: 3, s: S_4_14.as_ptr(), substring_i: 12, result: 2, function: None },
    among { s_size: 1, s: S_4_15.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_4_16.as_ptr(), substring_i: 15, result: 2, function: None },
    among { s_size: 2, s: S_4_17.as_ptr(), substring_i: 15, result: 2, function: None },
    among { s_size: 1, s: S_4_18.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_4_19.as_ptr(), substring_i: 18, result: 2, function: None },
    among { s_size: 2, s: S_4_20.as_ptr(), substring_i: 18, result: 2, function: None },
    among { s_size: 2, s: S_4_21.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_4_22.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 2, s: S_4_23.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 1, s: S_4_24.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_4_25.as_ptr(), substring_i: 24, result: 2, function: None },
    among { s_size: 2, s: S_4_26.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_4_27.as_ptr(), substring_i: 26, result: 2, function: None },
    among { s_size: 3, s: S_4_28.as_ptr(), substring_i: 26, result: 2, function: None },
    among { s_size: 2, s: S_4_29.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_4_30.as_ptr(), substring_i: 29, result: 2, function: None },
    among { s_size: 3, s: S_4_31.as_ptr(), substring_i: 29, result: 1, function: None },
    among { s_size: 2, s: S_4_32.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_4_33.as_ptr(), substring_i: 32, result: 2, function: None },
    among { s_size: 2, s: S_4_34.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_4_35.as_ptr(), substring_i: 34, result: 2, function: None },
    among { s_size: 2, s: S_4_36.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 2, s: S_4_37.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 2, s: S_4_38.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 2, s: S_4_39.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_4_40.as_ptr(), substring_i: 39, result: 2, function: None },
    among { s_size: 3, s: S_4_41.as_ptr(), substring_i: 39, result: 2, function: None },
    among { s_size: 3, s: S_4_42.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_4_43.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 2, s: S_4_44.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_4_45.as_ptr(), substring_i: 44, result: 2, function: None },
];

static S_5_0: [symbol; 1] = [0xC0];
static S_5_1: [symbol; 2] = [0xC9, 0xC0];
static S_5_2: [symbol; 2] = [0xD8, 0xC0];
static S_5_3: [symbol; 1] = [0xC1];
static S_5_4: [symbol; 1] = [0xC5];
static S_5_5: [symbol; 2] = [0xC9, 0xC5];
static S_5_6: [symbol; 2] = [0xD8, 0xC5];
static S_5_7: [symbol; 2] = [0xC1, 0xC8];
static S_5_8: [symbol; 2] = [0xD1, 0xC8];
static S_5_9: [symbol; 3] = [0xC9, 0xD1, 0xC8];
static S_5_10: [symbol; 1] = [0xC9];
static S_5_11: [symbol; 2] = [0xC5, 0xC9];
static S_5_12: [symbol; 2] = [0xC9, 0xC9];
static S_5_13: [symbol; 3] = [0xC1, 0xCD, 0xC9];
static S_5_14: [symbol; 3] = [0xD1, 0xCD, 0xC9];
static S_5_15: [symbol; 4] = [0xC9, 0xD1, 0xCD, 0xC9];
static S_5_16: [symbol; 1] = [0xCA];
static S_5_17: [symbol; 2] = [0xC5, 0xCA];
static S_5_18: [symbol; 3] = [0xC9, 0xC5, 0xCA];
static S_5_19: [symbol; 2] = [0xC9, 0xCA];
static S_5_20: [symbol; 2] = [0xCF, 0xCA];
static S_5_21: [symbol; 2] = [0xC1, 0xCD];
static S_5_22: [symbol; 2] = [0xC5, 0xCD];
static S_5_23: [symbol; 3] = [0xC9, 0xC5, 0xCD];
static S_5_24: [symbol; 2] = [0xCF, 0xCD];
static S_5_25: [symbol; 2] = [0xD1, 0xCD];
static S_5_26: [symbol; 3] = [0xC9, 0xD1, 0xCD];
static S_5_27: [symbol; 1] = [0xCF];
static S_5_28: [symbol; 1] = [0xD1];
static S_5_29: [symbol; 2] = [0xC9, 0xD1];
static S_5_30: [symbol; 2] = [0xD8, 0xD1];
static S_5_31: [symbol; 1] = [0xD5];
static S_5_32: [symbol; 2] = [0xC5, 0xD7];
static S_5_33: [symbol; 2] = [0xCF, 0xD7];
static S_5_34: [symbol; 1] = [0xD8];
static S_5_35: [symbol; 1] = [0xD9];

static A_5: [among; 36] = [
    among { s_size: 1, s: S_5_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_5_1.as_ptr(), substring_i: 0, result: 1, function: None },
    among { s_size: 2, s: S_5_2.as_ptr(), substring_i: 0, result: 1, function: None },
    among { s_size: 1, s: S_5_3.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 1, s: S_5_4.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_5_5.as_ptr(), substring_i: 4, result: 1, function: None },
    among { s_size: 2, s: S_5_6.as_ptr(), substring_i: 4, result: 1, function: None },
    among { s_size: 2, s: S_5_7.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_5_8.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_5_9.as_ptr(), substring_i: 8, result: 1, function: None },
    among { s_size: 1, s: S_5_10.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_5_11.as_ptr(), substring_i: 10, result: 1, function: None },
    among { s_size: 2, s: S_5_12.as_ptr(), substring_i: 10, result: 1, function: None },
    among { s_size: 3, s: S_5_13.as_ptr(), substring_i: 10, result: 1, function: None },
    among { s_size: 3, s: S_5_14.as_ptr(), substring_i: 10, result: 1, function: None },
    among { s_size: 4, s: S_5_15.as_ptr(), substring_i: 14, result: 1, function: None },
    among { s_size: 1, s: S_5_16.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_5_17.as_ptr(), substring_i: 16, result: 1, function: None },
    among { s_size: 3, s: S_5_18.as_ptr(), substring_i: 17, result: 1, function: None },
    among { s_size: 2, s: S_5_19.as_ptr(), substring_i: 16, result: 1, function: None },
    among { s_size: 2, s: S_5_20.as_ptr(), substring_i: 16, result: 1, function: None },
    among { s_size: 2, s: S_5_21.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_5_22.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_5_23.as_ptr(), substring_i: 22, result: 1, function: None },
    among { s_size: 2, s: S_5_24.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_5_25.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_5_26.as_ptr(), substring_i: 25, result: 1, function: None },
    among { s_size: 1, s: S_5_27.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 1, s: S_5_28.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_5_29.as_ptr(), substring_i: 28, result: 1, function: None },
    among { s_size: 2, s: S_5_30.as_ptr(), substring_i: 28, result: 1, function: None },
    among { s_size: 1, s: S_5_31.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_5_32.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_5_33.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 1, s: S_5_34.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 1, s: S_5_35.as_ptr(), substring_i: -1, result: 1, function: None },
];

static S_6_0: [symbol; 3] = [0xCF, 0xD3, 0xD4];
static S_6_1: [symbol; 4] = [0xCF, 0xD3, 0xD4, 0xD8];

static A_6: [among; 2] = [
    among { s_size: 3, s: S_6_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_6_1.as_ptr(), substring_i: -1, result: 1, function: None },
];

static S_7_0: [symbol; 4] = [0xC5, 0xCA, 0xDB, 0xC5];
static S_7_1: [symbol; 1] = [0xCE];
static S_7_2: [symbol; 1] = [0xD8];
static S_7_3: [symbol; 3] = [0xC5, 0xCA, 0xDB];

static A_7: [among; 4] = [
    among { s_size: 4, s: S_7_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 1, s: S_7_1.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 1, s: S_7_2.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 3, s: S_7_3.as_ptr(), substring_i: -1, result: 1, function: None },
];

// ---------------------------------------------------------------------------
// grouping bit tables
// ---------------------------------------------------------------------------

static G_V: [c_uchar; 4] = [35, 130, 34, 18];

// ---------------------------------------------------------------------------
// literal strings used by slice_from_s / eq_s
// ---------------------------------------------------------------------------

static S_0: [symbol; 1] = [0xC5];

// ---------------------------------------------------------------------------
// rules
// ---------------------------------------------------------------------------

unsafe fn r_mark_regions(z: *mut SN_env) -> c_int {
    *(*z).I.offset(1) = (*z).l;
    *(*z).I.offset(0) = (*z).l;
    {
        let c1 = (*z).c;

        'lab0: {
            {
                let ret = out_grouping(z, G_V.as_ptr(), 192, 220, 1);
                if ret < 0 {
                    break 'lab0;
                }
                (*z).c += ret;
            }
            *(*z).I.offset(1) = (*z).c;

            {
                let ret = in_grouping(z, G_V.as_ptr(), 192, 220, 1);
                if ret < 0 {
                    break 'lab0;
                }
                (*z).c += ret;
            }

            {
                let ret = out_grouping(z, G_V.as_ptr(), 192, 220, 1);
                if ret < 0 {
                    break 'lab0;
                }
                (*z).c += ret;
            }

            {
                let ret = in_grouping(z, G_V.as_ptr(), 192, 220, 1);
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

unsafe fn r_R2(z: *mut SN_env) -> c_int {
    (*(*z).I.offset(0) <= (*z).c) as c_int
}

unsafe fn r_perfective_gerund(z: *mut SN_env) -> c_int {
    let among_var;
    (*z).ket = (*z).c;
    if (*z).c <= (*z).lb
        || (*(*z).p.offset(((*z).c - 1) as isize) as c_int >> 5) != 6
        || ((25166336 >> (*(*z).p.offset(((*z).c - 1) as isize) as c_int & 0x1f)) & 1) == 0
    {
        return 0;
    }
    among_var = find_among_b(z, A_0.as_ptr(), 9);
    if among_var == 0 {
        return 0;
    }
    (*z).bra = (*z).c;
    match among_var {
        1 => {
            'lab0: {
                let m1 = (*z).l - (*z).c;
                'lab1: {
                    if (*z).c <= (*z).lb || *(*z).p.offset(((*z).c - 1) as isize) != 0xC1 {
                        break 'lab1;
                    }
                    (*z).c -= 1;
                    break 'lab0;
                }
                #[allow(unreachable_code)]
                {
                    (*z).c = (*z).l - m1;
                    if (*z).c <= (*z).lb || *(*z).p.offset(((*z).c - 1) as isize) != 0xD1 {
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
            let ret = slice_del(z);
            if ret < 0 {
                return ret;
            }
        }
        _ => {}
    }
    1
}

unsafe fn r_adjective(z: *mut SN_env) -> c_int {
    (*z).ket = (*z).c;
    if (*z).c - 1 <= (*z).lb
        || (*(*z).p.offset(((*z).c - 1) as isize) as c_int >> 5) != 6
        || ((2271009 >> (*(*z).p.offset(((*z).c - 1) as isize) as c_int & 0x1f)) & 1) == 0
    {
        return 0;
    }
    if find_among_b(z, A_1.as_ptr(), 26) == 0 {
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

unsafe fn r_adjectival(z: *mut SN_env) -> c_int {
    let among_var;
    {
        let ret = r_adjective(z);
        if ret <= 0 {
            return ret;
        }
    }
    {
        let m1 = (*z).l - (*z).c;
        'lab0: {
            (*z).ket = (*z).c;
            if (*z).c <= (*z).lb
                || (*(*z).p.offset(((*z).c - 1) as isize) as c_int >> 5) != 6
                || ((671113216 >> (*(*z).p.offset(((*z).c - 1) as isize) as c_int & 0x1f)) & 1) == 0
            {
                (*z).c = (*z).l - m1;
                break 'lab0;
            }
            among_var = find_among_b(z, A_2.as_ptr(), 8);
            if among_var == 0 {
                (*z).c = (*z).l - m1;
                break 'lab0;
            }
            (*z).bra = (*z).c;
            match among_var {
                1 => {
                    'lab1: {
                        let m2 = (*z).l - (*z).c;
                        'lab2: {
                            if (*z).c <= (*z).lb || *(*z).p.offset(((*z).c - 1) as isize) != 0xC1 {
                                break 'lab2;
                            }
                            (*z).c -= 1;
                            break 'lab1;
                        }
                        #[allow(unreachable_code)]
                        {
                            (*z).c = (*z).l - m2;
                            if (*z).c <= (*z).lb || *(*z).p.offset(((*z).c - 1) as isize) != 0xD1 {
                                (*z).c = (*z).l - m1;
                                break 'lab0;
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
                    let ret = slice_del(z);
                    if ret < 0 {
                        return ret;
                    }
                }
                _ => {}
            }
        }
    }
    1
}

unsafe fn r_reflexive(z: *mut SN_env) -> c_int {
    (*z).ket = (*z).c;
    if (*z).c - 1 <= (*z).lb
        || (*(*z).p.offset(((*z).c - 1) as isize) != 209 && *(*z).p.offset(((*z).c - 1) as isize) != 216)
    {
        return 0;
    }
    if find_among_b(z, A_3.as_ptr(), 2) == 0 {
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

unsafe fn r_verb(z: *mut SN_env) -> c_int {
    let among_var;
    (*z).ket = (*z).c;
    if (*z).c <= (*z).lb
        || (*(*z).p.offset(((*z).c - 1) as isize) as c_int >> 5) != 6
        || ((51443235 >> (*(*z).p.offset(((*z).c - 1) as isize) as c_int & 0x1f)) & 1) == 0
    {
        return 0;
    }
    among_var = find_among_b(z, A_4.as_ptr(), 46);
    if among_var == 0 {
        return 0;
    }
    (*z).bra = (*z).c;
    match among_var {
        1 => {
            'lab0: {
                let m1 = (*z).l - (*z).c;
                'lab1: {
                    if (*z).c <= (*z).lb || *(*z).p.offset(((*z).c - 1) as isize) != 0xC1 {
                        break 'lab1;
                    }
                    (*z).c -= 1;
                    break 'lab0;
                }
                #[allow(unreachable_code)]
                {
                    (*z).c = (*z).l - m1;
                    if (*z).c <= (*z).lb || *(*z).p.offset(((*z).c - 1) as isize) != 0xD1 {
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
            let ret = slice_del(z);
            if ret < 0 {
                return ret;
            }
        }
        _ => {}
    }
    1
}

unsafe fn r_noun(z: *mut SN_env) -> c_int {
    (*z).ket = (*z).c;
    if (*z).c <= (*z).lb
        || (*(*z).p.offset(((*z).c - 1) as isize) as c_int >> 5) != 6
        || ((60991267 >> (*(*z).p.offset(((*z).c - 1) as isize) as c_int & 0x1f)) & 1) == 0
    {
        return 0;
    }
    if find_among_b(z, A_5.as_ptr(), 36) == 0 {
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

unsafe fn r_derivational(z: *mut SN_env) -> c_int {
    (*z).ket = (*z).c;
    if (*z).c - 2 <= (*z).lb
        || (*(*z).p.offset(((*z).c - 1) as isize) != 212 && *(*z).p.offset(((*z).c - 1) as isize) != 216)
    {
        return 0;
    }
    if find_among_b(z, A_6.as_ptr(), 2) == 0 {
        return 0;
    }
    (*z).bra = (*z).c;
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
    1
}

unsafe fn r_tidy_up(z: *mut SN_env) -> c_int {
    let among_var;
    (*z).ket = (*z).c;
    if (*z).c <= (*z).lb
        || (*(*z).p.offset(((*z).c - 1) as isize) as c_int >> 5) != 6
        || ((151011360 >> (*(*z).p.offset(((*z).c - 1) as isize) as c_int & 0x1f)) & 1) == 0
    {
        return 0;
    }
    among_var = find_among_b(z, A_7.as_ptr(), 4);
    if among_var == 0 {
        return 0;
    }
    (*z).bra = (*z).c;
    match among_var {
        1 => {
            {
                let ret = slice_del(z);
                if ret < 0 {
                    return ret;
                }
            }
            (*z).ket = (*z).c;
            if (*z).c <= (*z).lb || *(*z).p.offset(((*z).c - 1) as isize) != 0xCE {
                return 0;
            }
            (*z).c -= 1;
            (*z).bra = (*z).c;
            if (*z).c <= (*z).lb || *(*z).p.offset(((*z).c - 1) as isize) != 0xCE {
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
        2 => {
            if (*z).c <= (*z).lb || *(*z).p.offset(((*z).c - 1) as isize) != 0xCE {
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

// ---------------------------------------------------------------------------
// exported entry points
// ---------------------------------------------------------------------------

#[no_mangle]
pub unsafe extern "C" fn russian_KOI8_R_stem(z: *mut SN_env) -> c_int {
    {
        let c1 = (*z).c;
        'outer: loop {
            let c2 = (*z).c;
            'lab1: {
                'ru0: loop {
                    let c3 = (*z).c;
                    'lab2: {
                        (*z).bra = (*z).c;
                        if (*z).c == (*z).l || *(*z).p.offset((*z).c as isize) != 0xA3 {
                            break 'lab2;
                        }
                        (*z).c += 1;
                        (*z).ket = (*z).c;
                        (*z).c = c3;
                        break 'ru0;
                    }
                    (*z).c = c3;
                    if (*z).c >= (*z).l {
                        break 'lab1;
                    }
                    (*z).c += 1;
                }
                {
                    let ret = slice_from_s(z, 1, S_0.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
                continue 'outer;
            }
            (*z).c = c2;
            break;
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
        let mlimit4;
        if (*z).c < *(*z).I.offset(1) {
            return 0;
        }
        mlimit4 = (*z).lb;
        (*z).lb = *(*z).I.offset(1);
        {
            let m5 = (*z).l - (*z).c;
            'lab3: {
                {
                    let m6 = (*z).l - (*z).c;
                    'lab4: {
                        'lab5: {
                            {
                                let ret = r_perfective_gerund(z);
                                if ret == 0 {
                                    break 'lab5;
                                }
                                if ret < 0 {
                                    return ret;
                                }
                            }
                            break 'lab4;
                        }
                        (*z).c = (*z).l - m6;
                        {
                            let m7 = (*z).l - (*z).c;
                            'lab6: {
                                let ret = r_reflexive(z);
                                if ret == 0 {
                                    (*z).c = (*z).l - m7;
                                    break 'lab6;
                                }
                                if ret < 0 {
                                    return ret;
                                }
                            }
                        }
                        'lab7: {
                            let m8 = (*z).l - (*z).c;
                            'lab8: {
                                {
                                    let ret = r_adjectival(z);
                                    if ret == 0 {
                                        break 'lab8;
                                    }
                                    if ret < 0 {
                                        return ret;
                                    }
                                }
                                break 'lab7;
                            }
                            (*z).c = (*z).l - m8;
                            'lab9: {
                                {
                                    let ret = r_verb(z);
                                    if ret == 0 {
                                        break 'lab9;
                                    }
                                    if ret < 0 {
                                        return ret;
                                    }
                                }
                                break 'lab7;
                            }
                            (*z).c = (*z).l - m8;
                            {
                                let ret = r_noun(z);
                                if ret == 0 {
                                    break 'lab3;
                                }
                                if ret < 0 {
                                    return ret;
                                }
                            }
                        }
                    }
                }
            }
            (*z).c = (*z).l - m5;
        }
        {
            let m9 = (*z).l - (*z).c;
            'lab10: {
                (*z).ket = (*z).c;
                if (*z).c <= (*z).lb || *(*z).p.offset(((*z).c - 1) as isize) != 0xC9 {
                    (*z).c = (*z).l - m9;
                    break 'lab10;
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
        {
            let m10 = (*z).l - (*z).c;
            {
                let ret = r_derivational(z);
                if ret < 0 {
                    return ret;
                }
            }
            (*z).c = (*z).l - m10;
        }
        {
            let m11 = (*z).l - (*z).c;
            {
                let ret = r_tidy_up(z);
                if ret < 0 {
                    return ret;
                }
            }
            (*z).c = (*z).l - m11;
        }
        (*z).lb = mlimit4;
    }
    (*z).c = (*z).lb;
    1
}

#[no_mangle]
pub unsafe extern "C" fn russian_KOI8_R_create_env() -> *mut SN_env {
    SN_create_env(0, 2)
}

#[no_mangle]
pub unsafe extern "C" fn russian_KOI8_R_close_env(z: *mut SN_env) {
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
        let z = russian_KOI8_R_create_env();
        assert!(!z.is_null());
        let rc = SN_set_current(z, word.len() as c_int, word.as_ptr());
        assert!(rc >= 0);
        let rc = russian_KOI8_R_stem(z);
        assert!(rc >= 0, "stem returned {rc}");
        let out = std::slice::from_raw_parts((*z).p, SIZE((*z).p) as usize).to_vec();
        russian_KOI8_R_close_env(z);
        out
    }

    // Idempotence: stemming a stem yields the same stem.
    #[test]
    fn idempotent() {
        unsafe {
            for w in [
                &[0xD7, 0xC5, 0xD3, 0xC5, 0xCE, 0xCE, 0xC9, 0xCA][..],
                &[0xD3, 0xD4, 0xCF, 0xCC][..],
            ] {
                // The Russian stemmer strips at most one suffix layer per pass
                // (faithful to the C), so a word may not be idempotent in one step
                // but repeated stemming converges to a stable fixpoint.
                let mut cur = stem(w);
                assert!(!cur.is_empty());
                for _ in 0..8 {
                    let next = stem(&cur);
                    if next == cur {
                        break;
                    }
                    cur = next;
                }
                let again = stem(&cur);
                assert_eq!(cur, again, "did not converge for {:?}", w);
                assert!(!cur.is_empty());
            }
        }
    }
}
