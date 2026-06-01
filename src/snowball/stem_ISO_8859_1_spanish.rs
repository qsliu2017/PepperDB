//! Spanish Snowball stemmer (ISO-8859-1, single-byte).
//!
//! 1:1 translation of the generated
//! `src/backend/snowball/libstemmer/stem_ISO_8859_1_spanish.c` (Snowball 2.2.0),
//! merged with its header. The runtime helpers come from
//! `crate::snowball::api` and `crate::snowball::utilities`. Because the input is
//! single-byte ISO-8859-1, this port uses the non-`_U` grouping helpers and a
//! plain byte advance/retreat instead of the UTF-8 skip helpers; high bytes such
//! as 0xE1 ('a' with acute) are stored and compared as raw bytes.

use crate::prelude::*;

use crate::snowball::api::{among, symbol, SN_close_env, SN_create_env, SN_env, SIZE};
use crate::snowball::utilities::{
    eq_s_b, find_among, find_among_b, in_grouping, out_grouping, slice_del, slice_from_s,
};

// ---------------------------------------------------------------------------
// among string tables
// ---------------------------------------------------------------------------

static S_0_1: [symbol; 1] = [0xE1];
static S_0_2: [symbol; 1] = [0xE9];
static S_0_3: [symbol; 1] = [0xED];
static S_0_4: [symbol; 1] = [0xF3];
static S_0_5: [symbol; 1] = [0xFA];

static A_0: [among; 6] = [
    among { s_size: 0, s: core::ptr::null(), substring_i: -1, result: 6, function: None },
    among { s_size: 1, s: S_0_1.as_ptr(), substring_i: 0, result: 1, function: None },
    among { s_size: 1, s: S_0_2.as_ptr(), substring_i: 0, result: 2, function: None },
    among { s_size: 1, s: S_0_3.as_ptr(), substring_i: 0, result: 3, function: None },
    among { s_size: 1, s: S_0_4.as_ptr(), substring_i: 0, result: 4, function: None },
    among { s_size: 1, s: S_0_5.as_ptr(), substring_i: 0, result: 5, function: None },
];

static S_1_0: [symbol; 2] = [b'l', b'a'];
static S_1_1: [symbol; 4] = [b's', b'e', b'l', b'a'];
static S_1_2: [symbol; 2] = [b'l', b'e'];
static S_1_3: [symbol; 2] = [b'm', b'e'];
static S_1_4: [symbol; 2] = [b's', b'e'];
static S_1_5: [symbol; 2] = [b'l', b'o'];
static S_1_6: [symbol; 4] = [b's', b'e', b'l', b'o'];
static S_1_7: [symbol; 3] = [b'l', b'a', b's'];
static S_1_8: [symbol; 5] = [b's', b'e', b'l', b'a', b's'];
static S_1_9: [symbol; 3] = [b'l', b'e', b's'];
static S_1_10: [symbol; 3] = [b'l', b'o', b's'];
static S_1_11: [symbol; 5] = [b's', b'e', b'l', b'o', b's'];
static S_1_12: [symbol; 3] = [b'n', b'o', b's'];

static A_1: [among; 13] = [
    among { s_size: 2, s: S_1_0.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 4, s: S_1_1.as_ptr(), substring_i: 0, result: -1, function: None },
    among { s_size: 2, s: S_1_2.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_1_3.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_1_4.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_1_5.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 4, s: S_1_6.as_ptr(), substring_i: 5, result: -1, function: None },
    among { s_size: 3, s: S_1_7.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 5, s: S_1_8.as_ptr(), substring_i: 7, result: -1, function: None },
    among { s_size: 3, s: S_1_9.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_1_10.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 5, s: S_1_11.as_ptr(), substring_i: 10, result: -1, function: None },
    among { s_size: 3, s: S_1_12.as_ptr(), substring_i: -1, result: -1, function: None },
];

static S_2_0: [symbol; 4] = [b'a', b'n', b'd', b'o'];
static S_2_1: [symbol; 5] = [b'i', b'e', b'n', b'd', b'o'];
static S_2_2: [symbol; 5] = [b'y', b'e', b'n', b'd', b'o'];
static S_2_3: [symbol; 4] = [0xE1, b'n', b'd', b'o'];
static S_2_4: [symbol; 5] = [b'i', 0xE9, b'n', b'd', b'o'];
static S_2_5: [symbol; 2] = [b'a', b'r'];
static S_2_6: [symbol; 2] = [b'e', b'r'];
static S_2_7: [symbol; 2] = [b'i', b'r'];
static S_2_8: [symbol; 2] = [0xE1, b'r'];
static S_2_9: [symbol; 2] = [0xE9, b'r'];
static S_2_10: [symbol; 2] = [0xED, b'r'];

static A_2: [among; 11] = [
    among { s_size: 4, s: S_2_0.as_ptr(), substring_i: -1, result: 6, function: None },
    among { s_size: 5, s: S_2_1.as_ptr(), substring_i: -1, result: 6, function: None },
    among { s_size: 5, s: S_2_2.as_ptr(), substring_i: -1, result: 7, function: None },
    among { s_size: 4, s: S_2_3.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 5, s: S_2_4.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_2_5.as_ptr(), substring_i: -1, result: 6, function: None },
    among { s_size: 2, s: S_2_6.as_ptr(), substring_i: -1, result: 6, function: None },
    among { s_size: 2, s: S_2_7.as_ptr(), substring_i: -1, result: 6, function: None },
    among { s_size: 2, s: S_2_8.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 2, s: S_2_9.as_ptr(), substring_i: -1, result: 4, function: None },
    among { s_size: 2, s: S_2_10.as_ptr(), substring_i: -1, result: 5, function: None },
];

static S_3_0: [symbol; 2] = [b'i', b'c'];
static S_3_1: [symbol; 2] = [b'a', b'd'];
static S_3_2: [symbol; 2] = [b'o', b's'];
static S_3_3: [symbol; 2] = [b'i', b'v'];

static A_3: [among; 4] = [
    among { s_size: 2, s: S_3_0.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_3_1.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_3_2.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_3_3.as_ptr(), substring_i: -1, result: 1, function: None },
];

static S_4_0: [symbol; 4] = [b'a', b'b', b'l', b'e'];
static S_4_1: [symbol; 4] = [b'i', b'b', b'l', b'e'];
static S_4_2: [symbol; 4] = [b'a', b'n', b't', b'e'];

static A_4: [among; 3] = [
    among { s_size: 4, s: S_4_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_4_1.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_4_2.as_ptr(), substring_i: -1, result: 1, function: None },
];

static S_5_0: [symbol; 2] = [b'i', b'c'];
static S_5_1: [symbol; 4] = [b'a', b'b', b'i', b'l'];
static S_5_2: [symbol; 2] = [b'i', b'v'];

static A_5: [among; 3] = [
    among { s_size: 2, s: S_5_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_5_1.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_5_2.as_ptr(), substring_i: -1, result: 1, function: None },
];

static S_6_0: [symbol; 3] = [b'i', b'c', b'a'];
static S_6_1: [symbol; 5] = [b'a', b'n', b'c', b'i', b'a'];
static S_6_2: [symbol; 5] = [b'e', b'n', b'c', b'i', b'a'];
static S_6_3: [symbol; 5] = [b'a', b'd', b'o', b'r', b'a'];
static S_6_4: [symbol; 3] = [b'o', b's', b'a'];
static S_6_5: [symbol; 4] = [b'i', b's', b't', b'a'];
static S_6_6: [symbol; 3] = [b'i', b'v', b'a'];
static S_6_7: [symbol; 4] = [b'a', b'n', b'z', b'a'];
static S_6_8: [symbol; 5] = [b'l', b'o', b'g', 0xED, b'a'];
static S_6_9: [symbol; 4] = [b'i', b'd', b'a', b'd'];
static S_6_10: [symbol; 4] = [b'a', b'b', b'l', b'e'];
static S_6_11: [symbol; 4] = [b'i', b'b', b'l', b'e'];
static S_6_12: [symbol; 4] = [b'a', b'n', b't', b'e'];
static S_6_13: [symbol; 5] = [b'm', b'e', b'n', b't', b'e'];
static S_6_14: [symbol; 6] = [b'a', b'm', b'e', b'n', b't', b'e'];
static S_6_15: [symbol; 5] = [b'a', b'c', b'i', 0xF3, b'n'];
static S_6_16: [symbol; 5] = [b'u', b'c', b'i', 0xF3, b'n'];
static S_6_17: [symbol; 3] = [b'i', b'c', b'o'];
static S_6_18: [symbol; 4] = [b'i', b's', b'm', b'o'];
static S_6_19: [symbol; 3] = [b'o', b's', b'o'];
static S_6_20: [symbol; 7] = [b'a', b'm', b'i', b'e', b'n', b't', b'o'];
static S_6_21: [symbol; 7] = [b'i', b'm', b'i', b'e', b'n', b't', b'o'];
static S_6_22: [symbol; 3] = [b'i', b'v', b'o'];
static S_6_23: [symbol; 4] = [b'a', b'd', b'o', b'r'];
static S_6_24: [symbol; 4] = [b'i', b'c', b'a', b's'];
static S_6_25: [symbol; 6] = [b'a', b'n', b'c', b'i', b'a', b's'];
static S_6_26: [symbol; 6] = [b'e', b'n', b'c', b'i', b'a', b's'];
static S_6_27: [symbol; 6] = [b'a', b'd', b'o', b'r', b'a', b's'];
static S_6_28: [symbol; 4] = [b'o', b's', b'a', b's'];
static S_6_29: [symbol; 5] = [b'i', b's', b't', b'a', b's'];
static S_6_30: [symbol; 4] = [b'i', b'v', b'a', b's'];
static S_6_31: [symbol; 5] = [b'a', b'n', b'z', b'a', b's'];
static S_6_32: [symbol; 6] = [b'l', b'o', b'g', 0xED, b'a', b's'];
static S_6_33: [symbol; 6] = [b'i', b'd', b'a', b'd', b'e', b's'];
static S_6_34: [symbol; 5] = [b'a', b'b', b'l', b'e', b's'];
static S_6_35: [symbol; 5] = [b'i', b'b', b'l', b'e', b's'];
static S_6_36: [symbol; 7] = [b'a', b'c', b'i', b'o', b'n', b'e', b's'];
static S_6_37: [symbol; 7] = [b'u', b'c', b'i', b'o', b'n', b'e', b's'];
static S_6_38: [symbol; 6] = [b'a', b'd', b'o', b'r', b'e', b's'];
static S_6_39: [symbol; 5] = [b'a', b'n', b't', b'e', b's'];
static S_6_40: [symbol; 4] = [b'i', b'c', b'o', b's'];
static S_6_41: [symbol; 5] = [b'i', b's', b'm', b'o', b's'];
static S_6_42: [symbol; 4] = [b'o', b's', b'o', b's'];
static S_6_43: [symbol; 8] = [b'a', b'm', b'i', b'e', b'n', b't', b'o', b's'];
static S_6_44: [symbol; 8] = [b'i', b'm', b'i', b'e', b'n', b't', b'o', b's'];
static S_6_45: [symbol; 4] = [b'i', b'v', b'o', b's'];

static A_6: [among; 46] = [
    among { s_size: 3, s: S_6_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_6_1.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 5, s: S_6_2.as_ptr(), substring_i: -1, result: 5, function: None },
    among { s_size: 5, s: S_6_3.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 3, s: S_6_4.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_6_5.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_6_6.as_ptr(), substring_i: -1, result: 9, function: None },
    among { s_size: 4, s: S_6_7.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_6_8.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 4, s: S_6_9.as_ptr(), substring_i: -1, result: 8, function: None },
    among { s_size: 4, s: S_6_10.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_6_11.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_6_12.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 5, s: S_6_13.as_ptr(), substring_i: -1, result: 7, function: None },
    among { s_size: 6, s: S_6_14.as_ptr(), substring_i: 13, result: 6, function: None },
    among { s_size: 5, s: S_6_15.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 5, s: S_6_16.as_ptr(), substring_i: -1, result: 4, function: None },
    among { s_size: 3, s: S_6_17.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_6_18.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_6_19.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 7, s: S_6_20.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 7, s: S_6_21.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_6_22.as_ptr(), substring_i: -1, result: 9, function: None },
    among { s_size: 4, s: S_6_23.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 4, s: S_6_24.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_6_25.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 6, s: S_6_26.as_ptr(), substring_i: -1, result: 5, function: None },
    among { s_size: 6, s: S_6_27.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 4, s: S_6_28.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_6_29.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_6_30.as_ptr(), substring_i: -1, result: 9, function: None },
    among { s_size: 5, s: S_6_31.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_6_32.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 6, s: S_6_33.as_ptr(), substring_i: -1, result: 8, function: None },
    among { s_size: 5, s: S_6_34.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_6_35.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 7, s: S_6_36.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 7, s: S_6_37.as_ptr(), substring_i: -1, result: 4, function: None },
    among { s_size: 6, s: S_6_38.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 5, s: S_6_39.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 4, s: S_6_40.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_6_41.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_6_42.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_6_43.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_6_44.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_6_45.as_ptr(), substring_i: -1, result: 9, function: None },
];

static S_7_0: [symbol; 2] = [b'y', b'a'];
static S_7_1: [symbol; 2] = [b'y', b'e'];
static S_7_2: [symbol; 3] = [b'y', b'a', b'n'];
static S_7_3: [symbol; 3] = [b'y', b'e', b'n'];
static S_7_4: [symbol; 5] = [b'y', b'e', b'r', b'o', b'n'];
static S_7_5: [symbol; 5] = [b'y', b'e', b'n', b'd', b'o'];
static S_7_6: [symbol; 2] = [b'y', b'o'];
static S_7_7: [symbol; 3] = [b'y', b'a', b's'];
static S_7_8: [symbol; 3] = [b'y', b'e', b's'];
static S_7_9: [symbol; 4] = [b'y', b'a', b'i', b's'];
static S_7_10: [symbol; 5] = [b'y', b'a', b'm', b'o', b's'];
static S_7_11: [symbol; 2] = [b'y', 0xF3];

static A_7: [among; 12] = [
    among { s_size: 2, s: S_7_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_7_1.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_7_2.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_7_3.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_7_4.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_7_5.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_7_6.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_7_7.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_7_8.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_7_9.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_7_10.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_7_11.as_ptr(), substring_i: -1, result: 1, function: None },
];

static S_8_0: [symbol; 3] = [b'a', b'b', b'a'];
static S_8_1: [symbol; 3] = [b'a', b'd', b'a'];
static S_8_2: [symbol; 3] = [b'i', b'd', b'a'];
static S_8_3: [symbol; 3] = [b'a', b'r', b'a'];
static S_8_4: [symbol; 4] = [b'i', b'e', b'r', b'a'];
static S_8_5: [symbol; 2] = [0xED, b'a'];
static S_8_6: [symbol; 4] = [b'a', b'r', 0xED, b'a'];
static S_8_7: [symbol; 4] = [b'e', b'r', 0xED, b'a'];
static S_8_8: [symbol; 4] = [b'i', b'r', 0xED, b'a'];
static S_8_9: [symbol; 2] = [b'a', b'd'];
static S_8_10: [symbol; 2] = [b'e', b'd'];
static S_8_11: [symbol; 2] = [b'i', b'd'];
static S_8_12: [symbol; 3] = [b'a', b's', b'e'];
static S_8_13: [symbol; 4] = [b'i', b'e', b's', b'e'];
static S_8_14: [symbol; 4] = [b'a', b's', b't', b'e'];
static S_8_15: [symbol; 4] = [b'i', b's', b't', b'e'];
static S_8_16: [symbol; 2] = [b'a', b'n'];
static S_8_17: [symbol; 4] = [b'a', b'b', b'a', b'n'];
static S_8_18: [symbol; 4] = [b'a', b'r', b'a', b'n'];
static S_8_19: [symbol; 5] = [b'i', b'e', b'r', b'a', b'n'];
static S_8_20: [symbol; 3] = [0xED, b'a', b'n'];
static S_8_21: [symbol; 5] = [b'a', b'r', 0xED, b'a', b'n'];
static S_8_22: [symbol; 5] = [b'e', b'r', 0xED, b'a', b'n'];
static S_8_23: [symbol; 5] = [b'i', b'r', 0xED, b'a', b'n'];
static S_8_24: [symbol; 2] = [b'e', b'n'];
static S_8_25: [symbol; 4] = [b'a', b's', b'e', b'n'];
static S_8_26: [symbol; 5] = [b'i', b'e', b's', b'e', b'n'];
static S_8_27: [symbol; 4] = [b'a', b'r', b'o', b'n'];
static S_8_28: [symbol; 5] = [b'i', b'e', b'r', b'o', b'n'];
static S_8_29: [symbol; 4] = [b'a', b'r', 0xE1, b'n'];
static S_8_30: [symbol; 4] = [b'e', b'r', 0xE1, b'n'];
static S_8_31: [symbol; 4] = [b'i', b'r', 0xE1, b'n'];
static S_8_32: [symbol; 3] = [b'a', b'd', b'o'];
static S_8_33: [symbol; 3] = [b'i', b'd', b'o'];
static S_8_34: [symbol; 4] = [b'a', b'n', b'd', b'o'];
static S_8_35: [symbol; 5] = [b'i', b'e', b'n', b'd', b'o'];
static S_8_36: [symbol; 2] = [b'a', b'r'];
static S_8_37: [symbol; 2] = [b'e', b'r'];
static S_8_38: [symbol; 2] = [b'i', b'r'];
static S_8_39: [symbol; 2] = [b'a', b's'];
static S_8_40: [symbol; 4] = [b'a', b'b', b'a', b's'];
static S_8_41: [symbol; 4] = [b'a', b'd', b'a', b's'];
static S_8_42: [symbol; 4] = [b'i', b'd', b'a', b's'];
static S_8_43: [symbol; 4] = [b'a', b'r', b'a', b's'];
static S_8_44: [symbol; 5] = [b'i', b'e', b'r', b'a', b's'];
static S_8_45: [symbol; 3] = [0xED, b'a', b's'];
static S_8_46: [symbol; 5] = [b'a', b'r', 0xED, b'a', b's'];
static S_8_47: [symbol; 5] = [b'e', b'r', 0xED, b'a', b's'];
static S_8_48: [symbol; 5] = [b'i', b'r', 0xED, b'a', b's'];
static S_8_49: [symbol; 2] = [b'e', b's'];
static S_8_50: [symbol; 4] = [b'a', b's', b'e', b's'];
static S_8_51: [symbol; 5] = [b'i', b'e', b's', b'e', b's'];
static S_8_52: [symbol; 5] = [b'a', b'b', b'a', b'i', b's'];
static S_8_53: [symbol; 5] = [b'a', b'r', b'a', b'i', b's'];
static S_8_54: [symbol; 6] = [b'i', b'e', b'r', b'a', b'i', b's'];
static S_8_55: [symbol; 4] = [0xED, b'a', b'i', b's'];
static S_8_56: [symbol; 6] = [b'a', b'r', 0xED, b'a', b'i', b's'];
static S_8_57: [symbol; 6] = [b'e', b'r', 0xED, b'a', b'i', b's'];
static S_8_58: [symbol; 6] = [b'i', b'r', 0xED, b'a', b'i', b's'];
static S_8_59: [symbol; 5] = [b'a', b's', b'e', b'i', b's'];
static S_8_60: [symbol; 6] = [b'i', b'e', b's', b'e', b'i', b's'];
static S_8_61: [symbol; 6] = [b'a', b's', b't', b'e', b'i', b's'];
static S_8_62: [symbol; 6] = [b'i', b's', b't', b'e', b'i', b's'];
static S_8_63: [symbol; 3] = [0xE1, b'i', b's'];
static S_8_64: [symbol; 3] = [0xE9, b'i', b's'];
static S_8_65: [symbol; 5] = [b'a', b'r', 0xE9, b'i', b's'];
static S_8_66: [symbol; 5] = [b'e', b'r', 0xE9, b'i', b's'];
static S_8_67: [symbol; 5] = [b'i', b'r', 0xE9, b'i', b's'];
static S_8_68: [symbol; 4] = [b'a', b'd', b'o', b's'];
static S_8_69: [symbol; 4] = [b'i', b'd', b'o', b's'];
static S_8_70: [symbol; 4] = [b'a', b'm', b'o', b's'];
static S_8_71: [symbol; 6] = [0xE1, b'b', b'a', b'm', b'o', b's'];
static S_8_72: [symbol; 6] = [0xE1, b'r', b'a', b'm', b'o', b's'];
static S_8_73: [symbol; 7] = [b'i', 0xE9, b'r', b'a', b'm', b'o', b's'];
static S_8_74: [symbol; 5] = [0xED, b'a', b'm', b'o', b's'];
static S_8_75: [symbol; 7] = [b'a', b'r', 0xED, b'a', b'm', b'o', b's'];
static S_8_76: [symbol; 7] = [b'e', b'r', 0xED, b'a', b'm', b'o', b's'];
static S_8_77: [symbol; 7] = [b'i', b'r', 0xED, b'a', b'm', b'o', b's'];
static S_8_78: [symbol; 4] = [b'e', b'm', b'o', b's'];
static S_8_79: [symbol; 6] = [b'a', b'r', b'e', b'm', b'o', b's'];
static S_8_80: [symbol; 6] = [b'e', b'r', b'e', b'm', b'o', b's'];
static S_8_81: [symbol; 6] = [b'i', b'r', b'e', b'm', b'o', b's'];
static S_8_82: [symbol; 6] = [0xE1, b's', b'e', b'm', b'o', b's'];
static S_8_83: [symbol; 7] = [b'i', 0xE9, b's', b'e', b'm', b'o', b's'];
static S_8_84: [symbol; 4] = [b'i', b'm', b'o', b's'];
static S_8_85: [symbol; 4] = [b'a', b'r', 0xE1, b's'];
static S_8_86: [symbol; 4] = [b'e', b'r', 0xE1, b's'];
static S_8_87: [symbol; 4] = [b'i', b'r', 0xE1, b's'];
static S_8_88: [symbol; 2] = [0xED, b's'];
static S_8_89: [symbol; 3] = [b'a', b'r', 0xE1];
static S_8_90: [symbol; 3] = [b'e', b'r', 0xE1];
static S_8_91: [symbol; 3] = [b'i', b'r', 0xE1];
static S_8_92: [symbol; 3] = [b'a', b'r', 0xE9];
static S_8_93: [symbol; 3] = [b'e', b'r', 0xE9];
static S_8_94: [symbol; 3] = [b'i', b'r', 0xE9];
static S_8_95: [symbol; 2] = [b'i', 0xF3];

static A_8: [among; 96] = [
    among { s_size: 3, s: S_8_0.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 3, s: S_8_1.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 3, s: S_8_2.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 3, s: S_8_3.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 4, s: S_8_4.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 2, s: S_8_5.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 4, s: S_8_6.as_ptr(), substring_i: 5, result: 2, function: None },
    among { s_size: 4, s: S_8_7.as_ptr(), substring_i: 5, result: 2, function: None },
    among { s_size: 4, s: S_8_8.as_ptr(), substring_i: 5, result: 2, function: None },
    among { s_size: 2, s: S_8_9.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 2, s: S_8_10.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 2, s: S_8_11.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 3, s: S_8_12.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 4, s: S_8_13.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 4, s: S_8_14.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 4, s: S_8_15.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 2, s: S_8_16.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 4, s: S_8_17.as_ptr(), substring_i: 16, result: 2, function: None },
    among { s_size: 4, s: S_8_18.as_ptr(), substring_i: 16, result: 2, function: None },
    among { s_size: 5, s: S_8_19.as_ptr(), substring_i: 16, result: 2, function: None },
    among { s_size: 3, s: S_8_20.as_ptr(), substring_i: 16, result: 2, function: None },
    among { s_size: 5, s: S_8_21.as_ptr(), substring_i: 20, result: 2, function: None },
    among { s_size: 5, s: S_8_22.as_ptr(), substring_i: 20, result: 2, function: None },
    among { s_size: 5, s: S_8_23.as_ptr(), substring_i: 20, result: 2, function: None },
    among { s_size: 2, s: S_8_24.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_8_25.as_ptr(), substring_i: 24, result: 2, function: None },
    among { s_size: 5, s: S_8_26.as_ptr(), substring_i: 24, result: 2, function: None },
    among { s_size: 4, s: S_8_27.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 5, s: S_8_28.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 4, s: S_8_29.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 4, s: S_8_30.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 4, s: S_8_31.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 3, s: S_8_32.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 3, s: S_8_33.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 4, s: S_8_34.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 5, s: S_8_35.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 2, s: S_8_36.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 2, s: S_8_37.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 2, s: S_8_38.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 2, s: S_8_39.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 4, s: S_8_40.as_ptr(), substring_i: 39, result: 2, function: None },
    among { s_size: 4, s: S_8_41.as_ptr(), substring_i: 39, result: 2, function: None },
    among { s_size: 4, s: S_8_42.as_ptr(), substring_i: 39, result: 2, function: None },
    among { s_size: 4, s: S_8_43.as_ptr(), substring_i: 39, result: 2, function: None },
    among { s_size: 5, s: S_8_44.as_ptr(), substring_i: 39, result: 2, function: None },
    among { s_size: 3, s: S_8_45.as_ptr(), substring_i: 39, result: 2, function: None },
    among { s_size: 5, s: S_8_46.as_ptr(), substring_i: 45, result: 2, function: None },
    among { s_size: 5, s: S_8_47.as_ptr(), substring_i: 45, result: 2, function: None },
    among { s_size: 5, s: S_8_48.as_ptr(), substring_i: 45, result: 2, function: None },
    among { s_size: 2, s: S_8_49.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_8_50.as_ptr(), substring_i: 49, result: 2, function: None },
    among { s_size: 5, s: S_8_51.as_ptr(), substring_i: 49, result: 2, function: None },
    among { s_size: 5, s: S_8_52.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 5, s: S_8_53.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 6, s: S_8_54.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 4, s: S_8_55.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 6, s: S_8_56.as_ptr(), substring_i: 55, result: 2, function: None },
    among { s_size: 6, s: S_8_57.as_ptr(), substring_i: 55, result: 2, function: None },
    among { s_size: 6, s: S_8_58.as_ptr(), substring_i: 55, result: 2, function: None },
    among { s_size: 5, s: S_8_59.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 6, s: S_8_60.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 6, s: S_8_61.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 6, s: S_8_62.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 3, s: S_8_63.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 3, s: S_8_64.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_8_65.as_ptr(), substring_i: 64, result: 2, function: None },
    among { s_size: 5, s: S_8_66.as_ptr(), substring_i: 64, result: 2, function: None },
    among { s_size: 5, s: S_8_67.as_ptr(), substring_i: 64, result: 2, function: None },
    among { s_size: 4, s: S_8_68.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 4, s: S_8_69.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 4, s: S_8_70.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 6, s: S_8_71.as_ptr(), substring_i: 70, result: 2, function: None },
    among { s_size: 6, s: S_8_72.as_ptr(), substring_i: 70, result: 2, function: None },
    among { s_size: 7, s: S_8_73.as_ptr(), substring_i: 70, result: 2, function: None },
    among { s_size: 5, s: S_8_74.as_ptr(), substring_i: 70, result: 2, function: None },
    among { s_size: 7, s: S_8_75.as_ptr(), substring_i: 74, result: 2, function: None },
    among { s_size: 7, s: S_8_76.as_ptr(), substring_i: 74, result: 2, function: None },
    among { s_size: 7, s: S_8_77.as_ptr(), substring_i: 74, result: 2, function: None },
    among { s_size: 4, s: S_8_78.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_8_79.as_ptr(), substring_i: 78, result: 2, function: None },
    among { s_size: 6, s: S_8_80.as_ptr(), substring_i: 78, result: 2, function: None },
    among { s_size: 6, s: S_8_81.as_ptr(), substring_i: 78, result: 2, function: None },
    among { s_size: 6, s: S_8_82.as_ptr(), substring_i: 78, result: 2, function: None },
    among { s_size: 7, s: S_8_83.as_ptr(), substring_i: 78, result: 2, function: None },
    among { s_size: 4, s: S_8_84.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 4, s: S_8_85.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 4, s: S_8_86.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 4, s: S_8_87.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 2, s: S_8_88.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 3, s: S_8_89.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 3, s: S_8_90.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 3, s: S_8_91.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 3, s: S_8_92.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 3, s: S_8_93.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 3, s: S_8_94.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 2, s: S_8_95.as_ptr(), substring_i: -1, result: 2, function: None },
];

static S_9_0: [symbol; 1] = [b'a'];
static S_9_1: [symbol; 1] = [b'e'];
static S_9_2: [symbol; 1] = [b'o'];
static S_9_3: [symbol; 2] = [b'o', b's'];
static S_9_4: [symbol; 1] = [0xE1];
static S_9_5: [symbol; 1] = [0xE9];
static S_9_6: [symbol; 1] = [0xED];
static S_9_7: [symbol; 1] = [0xF3];

static A_9: [among; 8] = [
    among { s_size: 1, s: S_9_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 1, s: S_9_1.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 1, s: S_9_2.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_9_3.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 1, s: S_9_4.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 1, s: S_9_5.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 1, s: S_9_6.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 1, s: S_9_7.as_ptr(), substring_i: -1, result: 1, function: None },
];

// ---------------------------------------------------------------------------
// grouping bit tables
// ---------------------------------------------------------------------------

static G_V: [c_uchar; 20] = [
    17, 65, 16, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 17, 4, 10,
];

// ---------------------------------------------------------------------------
// literal strings used by slice_from_s / eq_s_b
// ---------------------------------------------------------------------------

static S_0: [symbol; 1] = [b'a'];
static S_1: [symbol; 1] = [b'e'];
static S_2: [symbol; 1] = [b'i'];
static S_3: [symbol; 1] = [b'o'];
static S_4: [symbol; 1] = [b'u'];
static S_5: [symbol; 5] = [b'i', b'e', b'n', b'd', b'o'];
static S_6: [symbol; 4] = [b'a', b'n', b'd', b'o'];
static S_7: [symbol; 2] = [b'a', b'r'];
static S_8: [symbol; 2] = [b'e', b'r'];
static S_9: [symbol; 2] = [b'i', b'r'];
static S_10: [symbol; 2] = [b'i', b'c'];
static S_11: [symbol; 3] = [b'l', b'o', b'g'];
static S_12: [symbol; 1] = [b'u'];
static S_13: [symbol; 4] = [b'e', b'n', b't', b'e'];
static S_14: [symbol; 2] = [b'a', b't'];
static S_15: [symbol; 2] = [b'a', b't'];

// ---------------------------------------------------------------------------
// rules
// ---------------------------------------------------------------------------

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
                    if in_grouping(z, G_V.as_ptr(), 97, 252, 0) != 0 {
                        break 'lab2;
                    }
                    {
                        let c3 = (*z).c;
                        'lab3: {
                            'lab4: {
                                if out_grouping(z, G_V.as_ptr(), 97, 252, 0) != 0 {
                                    break 'lab4;
                                }
                                {
                                    let ret = out_grouping(z, G_V.as_ptr(), 97, 252, 1);
                                    if ret < 0 {
                                        break 'lab4;
                                    }
                                    (*z).c += ret;
                                }
                                break 'lab3;
                            }
                            // lab4:
                            (*z).c = c3;
                            if in_grouping(z, G_V.as_ptr(), 97, 252, 0) != 0 {
                                break 'lab2;
                            }
                            {
                                let ret = in_grouping(z, G_V.as_ptr(), 97, 252, 1);
                                if ret < 0 {
                                    break 'lab2;
                                }
                                (*z).c += ret;
                            }
                        }
                        // lab3:
                    }
                    break 'lab1;
                }
                // lab2:
                (*z).c = c2;
                if out_grouping(z, G_V.as_ptr(), 97, 252, 0) != 0 {
                    break 'lab0;
                }
                {
                    let c4 = (*z).c;
                    'lab5: {
                        'lab6: {
                            if out_grouping(z, G_V.as_ptr(), 97, 252, 0) != 0 {
                                break 'lab6;
                            }
                            {
                                let ret = out_grouping(z, G_V.as_ptr(), 97, 252, 1);
                                if ret < 0 {
                                    break 'lab6;
                                }
                                (*z).c += ret;
                            }
                            break 'lab5;
                        }
                        // lab6:
                        (*z).c = c4;
                        if in_grouping(z, G_V.as_ptr(), 97, 252, 0) != 0 {
                            break 'lab0;
                        }
                        if (*z).c >= (*z).l {
                            break 'lab0;
                        }
                        (*z).c += 1;
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
                let ret = out_grouping(z, G_V.as_ptr(), 97, 252, 1);
                if ret < 0 {
                    break 'lab7;
                }
                (*z).c += ret;
            }
            {
                let ret = in_grouping(z, G_V.as_ptr(), 97, 252, 1);
                if ret < 0 {
                    break 'lab7;
                }
                (*z).c += ret;
            }
            *(*z).I.offset(1) = (*z).c;
            {
                let ret = out_grouping(z, G_V.as_ptr(), 97, 252, 1);
                if ret < 0 {
                    break 'lab7;
                }
                (*z).c += ret;
            }
            {
                let ret = in_grouping(z, G_V.as_ptr(), 97, 252, 1);
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
    'loop0: loop {
        let c1 = (*z).c;
        'lab0: {
            (*z).bra = (*z).c;
            if (*z).c >= (*z).l
                || *(*z).p.offset((*z).c as isize) as c_int >> 5 != 7
                || (67641858 >> (*(*z).p.offset((*z).c as isize) as c_int & 0x1f)) & 1 == 0
            {
                among_var = 6;
            } else {
                among_var = find_among(z, A_0.as_ptr(), 6);
            }
            (*z).ket = (*z).c;
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
                3 => {
                    let ret = slice_from_s(z, 1, S_2.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
                4 => {
                    let ret = slice_from_s(z, 1, S_3.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
                5 => {
                    let ret = slice_from_s(z, 1, S_4.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
                6 => {
                    if (*z).c >= (*z).l {
                        break 'lab0;
                    }
                    (*z).c += 1;
                }
                _ => {}
            }
            continue 'loop0;
        }
        // lab0:
        (*z).c = c1;
        break 'loop0;
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

unsafe fn r_attached_pronoun(z: *mut SN_env) -> c_int {
    let among_var;
    (*z).ket = (*z).c;
    if (*z).c - 1 <= (*z).lb
        || *(*z).p.offset(((*z).c - 1) as isize) as c_int >> 5 != 3
        || (557090 >> (*(*z).p.offset(((*z).c - 1) as isize) as c_int & 0x1f)) & 1 == 0
    {
        return 0;
    }
    if find_among_b(z, A_1.as_ptr(), 13) == 0 {
        return 0;
    }
    (*z).bra = (*z).c;
    if (*z).c - 1 <= (*z).lb
        || (*(*z).p.offset(((*z).c - 1) as isize) != 111
            && *(*z).p.offset(((*z).c - 1) as isize) != 114)
    {
        return 0;
    }
    among_var = find_among_b(z, A_2.as_ptr(), 11);
    if among_var == 0 {
        return 0;
    }
    {
        let ret = r_RV(z);
        if ret <= 0 {
            return ret;
        }
    }
    match among_var {
        1 => {
            (*z).bra = (*z).c;
            let ret = slice_from_s(z, 5, S_5.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        2 => {
            (*z).bra = (*z).c;
            let ret = slice_from_s(z, 4, S_6.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        3 => {
            (*z).bra = (*z).c;
            let ret = slice_from_s(z, 2, S_7.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        4 => {
            (*z).bra = (*z).c;
            let ret = slice_from_s(z, 2, S_8.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        5 => {
            (*z).bra = (*z).c;
            let ret = slice_from_s(z, 2, S_9.as_ptr());
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
            if (*z).c <= (*z).lb || *(*z).p.offset(((*z).c - 1) as isize) != b'u' {
                return 0;
            }
            (*z).c -= 1;
            let ret = slice_del(z);
            if ret < 0 {
                return ret;
            }
        }
        _ => {}
    }
    1
}

unsafe fn r_standard_suffix(z: *mut SN_env) -> c_int {
    let mut among_var;
    (*z).ket = (*z).c;
    if (*z).c - 2 <= (*z).lb
        || *(*z).p.offset(((*z).c - 1) as isize) as c_int >> 5 != 3
        || (835634 >> (*(*z).p.offset(((*z).c - 1) as isize) as c_int & 0x1f)) & 1 == 0
    {
        return 0;
    }
    among_var = find_among_b(z, A_6.as_ptr(), 46);
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
                    if eq_s_b(z, 2, S_10.as_ptr()) == 0 {
                        (*z).c = (*z).l - m1;
                        break 'lab0;
                    }
                    (*z).bra = (*z).c;
                    {
                        let ret = r_R2(z);
                        if ret == 0 {
                            (*z).c = (*z).l - m1;
                            break 'lab0;
                        }
                        if ret < 0 {
                            return ret;
                        }
                    }
                    let ret = slice_del(z);
                    if ret < 0 {
                        return ret;
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
                let ret = slice_from_s(z, 3, S_11.as_ptr());
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
                let ret = slice_from_s(z, 1, S_12.as_ptr());
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
                let ret = slice_from_s(z, 4, S_13.as_ptr());
                if ret < 0 {
                    return ret;
                }
            }
        }
        6 => {
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
                let m2 = (*z).l - (*z).c;
                'lab1: {
                    (*z).ket = (*z).c;
                    if (*z).c - 1 <= (*z).lb
                        || *(*z).p.offset(((*z).c - 1) as isize) as c_int >> 5 != 3
                        || (4718616 >> (*(*z).p.offset(((*z).c - 1) as isize) as c_int & 0x1f)) & 1
                            == 0
                    {
                        (*z).c = (*z).l - m2;
                        break 'lab1;
                    }
                    among_var = find_among_b(z, A_3.as_ptr(), 4);
                    if among_var == 0 {
                        (*z).c = (*z).l - m2;
                        break 'lab1;
                    }
                    (*z).bra = (*z).c;
                    {
                        let ret = r_R2(z);
                        if ret == 0 {
                            (*z).c = (*z).l - m2;
                            break 'lab1;
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
                    match among_var {
                        1 => {
                            (*z).ket = (*z).c;
                            if eq_s_b(z, 2, S_14.as_ptr()) == 0 {
                                (*z).c = (*z).l - m2;
                                break 'lab1;
                            }
                            (*z).bra = (*z).c;
                            {
                                let ret = r_R2(z);
                                if ret == 0 {
                                    (*z).c = (*z).l - m2;
                                    break 'lab1;
                                }
                                if ret < 0 {
                                    return ret;
                                }
                            }
                            let ret = slice_del(z);
                            if ret < 0 {
                                return ret;
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
                let m3 = (*z).l - (*z).c;
                'lab2: {
                    (*z).ket = (*z).c;
                    if (*z).c - 3 <= (*z).lb || *(*z).p.offset(((*z).c - 1) as isize) != 101 {
                        (*z).c = (*z).l - m3;
                        break 'lab2;
                    }
                    if find_among_b(z, A_4.as_ptr(), 3) == 0 {
                        (*z).c = (*z).l - m3;
                        break 'lab2;
                    }
                    (*z).bra = (*z).c;
                    {
                        let ret = r_R2(z);
                        if ret == 0 {
                            (*z).c = (*z).l - m3;
                            break 'lab2;
                        }
                        if ret < 0 {
                            return ret;
                        }
                    }
                    let ret = slice_del(z);
                    if ret < 0 {
                        return ret;
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
                let m4 = (*z).l - (*z).c;
                'lab3: {
                    (*z).ket = (*z).c;
                    if (*z).c - 1 <= (*z).lb
                        || *(*z).p.offset(((*z).c - 1) as isize) as c_int >> 5 != 3
                        || (4198408 >> (*(*z).p.offset(((*z).c - 1) as isize) as c_int & 0x1f)) & 1
                            == 0
                    {
                        (*z).c = (*z).l - m4;
                        break 'lab3;
                    }
                    if find_among_b(z, A_5.as_ptr(), 3) == 0 {
                        (*z).c = (*z).l - m4;
                        break 'lab3;
                    }
                    (*z).bra = (*z).c;
                    {
                        let ret = r_R2(z);
                        if ret == 0 {
                            (*z).c = (*z).l - m4;
                            break 'lab3;
                        }
                        if ret < 0 {
                            return ret;
                        }
                    }
                    let ret = slice_del(z);
                    if ret < 0 {
                        return ret;
                    }
                }
            }
        }
        9 => {
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
                'lab4: {
                    (*z).ket = (*z).c;
                    if eq_s_b(z, 2, S_15.as_ptr()) == 0 {
                        (*z).c = (*z).l - m5;
                        break 'lab4;
                    }
                    (*z).bra = (*z).c;
                    {
                        let ret = r_R2(z);
                        if ret == 0 {
                            (*z).c = (*z).l - m5;
                            break 'lab4;
                        }
                        if ret < 0 {
                            return ret;
                        }
                    }
                    let ret = slice_del(z);
                    if ret < 0 {
                        return ret;
                    }
                }
            }
        }
        _ => {}
    }
    1
}

unsafe fn r_y_verb_suffix(z: *mut SN_env) -> c_int {
    {
        let mlimit1;
        if (*z).c < *(*z).I.offset(2) {
            return 0;
        }
        mlimit1 = (*z).lb;
        (*z).lb = *(*z).I.offset(2);
        (*z).ket = (*z).c;
        if find_among_b(z, A_7.as_ptr(), 12) == 0 {
            (*z).lb = mlimit1;
            return 0;
        }
        (*z).bra = (*z).c;
        (*z).lb = mlimit1;
    }
    if (*z).c <= (*z).lb || *(*z).p.offset(((*z).c - 1) as isize) != b'u' {
        return 0;
    }
    (*z).c -= 1;
    {
        let ret = slice_del(z);
        if ret < 0 {
            return ret;
        }
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
        among_var = find_among_b(z, A_8.as_ptr(), 96);
        if among_var == 0 {
            (*z).lb = mlimit1;
            return 0;
        }
        (*z).bra = (*z).c;
        (*z).lb = mlimit1;
    }
    match among_var {
        1 => {
            {
                let m2 = (*z).l - (*z).c;
                'lab0: {
                    if (*z).c <= (*z).lb || *(*z).p.offset(((*z).c - 1) as isize) != b'u' {
                        (*z).c = (*z).l - m2;
                        break 'lab0;
                    }
                    (*z).c -= 1;
                    {
                        let m_test3 = (*z).l - (*z).c;
                        if (*z).c <= (*z).lb || *(*z).p.offset(((*z).c - 1) as isize) != b'g' {
                            (*z).c = (*z).l - m2;
                            break 'lab0;
                        }
                        (*z).c -= 1;
                        (*z).c = (*z).l - m_test3;
                    }
                }
            }
            (*z).bra = (*z).c;
            let ret = slice_del(z);
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
    1
}

unsafe fn r_residual_suffix(z: *mut SN_env) -> c_int {
    let among_var;
    (*z).ket = (*z).c;
    among_var = find_among_b(z, A_9.as_ptr(), 8);
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
                let m1 = (*z).l - (*z).c;
                'lab0: {
                    (*z).ket = (*z).c;
                    if (*z).c <= (*z).lb || *(*z).p.offset(((*z).c - 1) as isize) != b'u' {
                        (*z).c = (*z).l - m1;
                        break 'lab0;
                    }
                    (*z).c -= 1;
                    (*z).bra = (*z).c;
                    {
                        let m_test2 = (*z).l - (*z).c;
                        if (*z).c <= (*z).lb || *(*z).p.offset(((*z).c - 1) as isize) != b'g' {
                            (*z).c = (*z).l - m1;
                            break 'lab0;
                        }
                        (*z).c -= 1;
                        (*z).c = (*z).l - m_test2;
                    }
                    {
                        let ret = r_RV(z);
                        if ret == 0 {
                            (*z).c = (*z).l - m1;
                            break 'lab0;
                        }
                        if ret < 0 {
                            return ret;
                        }
                    }
                    let ret = slice_del(z);
                    if ret < 0 {
                        return ret;
                    }
                }
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
pub unsafe extern "C" fn spanish_ISO_8859_1_stem(z: *mut SN_env) -> c_int {
    {
        let ret = r_mark_regions(z);
        if ret < 0 {
            return ret;
        }
    }
    (*z).lb = (*z).c;
    (*z).c = (*z).l;

    {
        let m1 = (*z).l - (*z).c;
        {
            let ret = r_attached_pronoun(z);
            if ret < 0 {
                return ret;
            }
        }
        (*z).c = (*z).l - m1;
    }
    {
        let m2 = (*z).l - (*z).c;
        'lab0: {
            'lab1: {
                let m3 = (*z).l - (*z).c;
                'lab2: {
                    {
                        let ret = r_standard_suffix(z);
                        if ret == 0 {
                            break 'lab2;
                        }
                        if ret < 0 {
                            return ret;
                        }
                    }
                    break 'lab1;
                }
                // lab2:
                (*z).c = (*z).l - m3;
                'lab3: {
                    {
                        let ret = r_y_verb_suffix(z);
                        if ret == 0 {
                            break 'lab3;
                        }
                        if ret < 0 {
                            return ret;
                        }
                    }
                    break 'lab1;
                }
                // lab3:
                (*z).c = (*z).l - m3;
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
        (*z).c = (*z).l - m2;
    }
    {
        let m4 = (*z).l - (*z).c;
        {
            let ret = r_residual_suffix(z);
            if ret < 0 {
                return ret;
            }
        }
        (*z).c = (*z).l - m4;
    }
    (*z).c = (*z).lb;
    {
        let c5 = (*z).c;
        {
            let ret = r_postlude(z);
            if ret < 0 {
                return ret;
            }
        }
        (*z).c = c5;
    }
    1
}

#[no_mangle]
pub unsafe extern "C" fn spanish_ISO_8859_1_create_env() -> *mut SN_env {
    SN_create_env(0, 3)
}

#[no_mangle]
pub unsafe extern "C" fn spanish_ISO_8859_1_close_env(z: *mut SN_env) {
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
        let z = spanish_ISO_8859_1_create_env();
        assert!(!z.is_null());
        let rc = SN_set_current(z, word.len() as c_int, word.as_ptr());
        assert!(rc >= 0);
        let rc = spanish_ISO_8859_1_stem(z);
        assert!(rc >= 0, "stem returned {rc}");
        let out = std::slice::from_raw_parts((*z).p, SIZE((*z).p) as usize).to_vec();
        spanish_ISO_8859_1_close_env(z);
        out
    }

    // A short root with no strippable suffix is returned unchanged.
    #[test]
    fn short_root_unchanged() {
        unsafe {
            assert_eq!(stem(b"sol"), b"sol".to_vec());
        }
    }

    // Idempotence: stemming a stem yields the same stem. High bytes are raw
    // ISO-8859-1 (single-byte), e.g. 0xF3 for 'o' with acute.
    #[test]
    fn idempotent() {
        unsafe {
            for w in [
                &b"casas"[..],
                &b"hablando"[..],
                &b"perros"[..],
                &b"naci\xf3n"[..],
            ] {
                let once = stem(w);
                let twice = stem(&once);
                assert_eq!(once, twice, "not idempotent for {:?}", w);
                assert!(!once.is_empty());
            }
        }
    }

    // A suffix family collapses; result must be non-empty and cannot grow past
    // the input length.
    #[test]
    fn suffix_stripped_nonempty() {
        unsafe {
            let r = stem(b"nacionales");
            assert!(!r.is_empty());
            assert!(r.len() <= "nacionales".len());
        }
    }
}
