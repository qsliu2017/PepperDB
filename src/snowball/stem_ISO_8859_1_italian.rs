//! Italian Snowball stemmer (ISO-8859-1, single-byte).
//!
//! 1:1 translation of the generated
//! `src/backend/snowball/libstemmer/stem_ISO_8859_1_italian.c` (Snowball 2.2.0),
//! merged with its header. The runtime helpers come from
//! `crate::snowball::api` and `crate::snowball::utilities`. Because the input is
//! single-byte ISO-8859-1, this port uses the non-`_U` grouping helpers and a
//! plain byte advance/retreat instead of the UTF-8 skip helpers; high bytes such
//! as 0xE0 ('a' with grave) and 0xF2 ('o' with grave) are stored and compared as
//! raw bytes.

use crate::prelude::*;

use crate::snowball::api::{among, symbol, SN_close_env, SN_create_env, SN_env, SIZE};
use crate::snowball::utilities::{
    eq_s, eq_s_b, find_among, find_among_b, in_grouping, in_grouping_b, out_grouping, slice_del,
    slice_from_s,
};

// ---------------------------------------------------------------------------
// among string tables
// ---------------------------------------------------------------------------

static S_0_1: [symbol; 2] = [b'q', b'u'];
static S_0_2: [symbol; 1] = [0xE1];
static S_0_3: [symbol; 1] = [0xE9];
static S_0_4: [symbol; 1] = [0xED];
static S_0_5: [symbol; 1] = [0xF3];
static S_0_6: [symbol; 1] = [0xFA];

static A_0: [among; 7] = [
    among { s_size: 0, s: core::ptr::null(), substring_i: -1, result: 7, function: None },
    among { s_size: 2, s: S_0_1.as_ptr(), substring_i: 0, result: 6, function: None },
    among { s_size: 1, s: S_0_2.as_ptr(), substring_i: 0, result: 1, function: None },
    among { s_size: 1, s: S_0_3.as_ptr(), substring_i: 0, result: 2, function: None },
    among { s_size: 1, s: S_0_4.as_ptr(), substring_i: 0, result: 3, function: None },
    among { s_size: 1, s: S_0_5.as_ptr(), substring_i: 0, result: 4, function: None },
    among { s_size: 1, s: S_0_6.as_ptr(), substring_i: 0, result: 5, function: None },
];

static S_1_1: [symbol; 1] = [b'I'];
static S_1_2: [symbol; 1] = [b'U'];

static A_1: [among; 3] = [
    among { s_size: 0, s: core::ptr::null(), substring_i: -1, result: 3, function: None },
    among { s_size: 1, s: S_1_1.as_ptr(), substring_i: 0, result: 1, function: None },
    among { s_size: 1, s: S_1_2.as_ptr(), substring_i: 0, result: 2, function: None },
];

static S_2_0: [symbol; 2] = [b'l', b'a'];
static S_2_1: [symbol; 4] = [b'c', b'e', b'l', b'a'];
static S_2_2: [symbol; 6] = [b'g', b'l', b'i', b'e', b'l', b'a'];
static S_2_3: [symbol; 4] = [b'm', b'e', b'l', b'a'];
static S_2_4: [symbol; 4] = [b't', b'e', b'l', b'a'];
static S_2_5: [symbol; 4] = [b'v', b'e', b'l', b'a'];
static S_2_6: [symbol; 2] = [b'l', b'e'];
static S_2_7: [symbol; 4] = [b'c', b'e', b'l', b'e'];
static S_2_8: [symbol; 6] = [b'g', b'l', b'i', b'e', b'l', b'e'];
static S_2_9: [symbol; 4] = [b'm', b'e', b'l', b'e'];
static S_2_10: [symbol; 4] = [b't', b'e', b'l', b'e'];
static S_2_11: [symbol; 4] = [b'v', b'e', b'l', b'e'];
static S_2_12: [symbol; 2] = [b'n', b'e'];
static S_2_13: [symbol; 4] = [b'c', b'e', b'n', b'e'];
static S_2_14: [symbol; 6] = [b'g', b'l', b'i', b'e', b'n', b'e'];
static S_2_15: [symbol; 4] = [b'm', b'e', b'n', b'e'];
static S_2_16: [symbol; 4] = [b's', b'e', b'n', b'e'];
static S_2_17: [symbol; 4] = [b't', b'e', b'n', b'e'];
static S_2_18: [symbol; 4] = [b'v', b'e', b'n', b'e'];
static S_2_19: [symbol; 2] = [b'c', b'i'];
static S_2_20: [symbol; 2] = [b'l', b'i'];
static S_2_21: [symbol; 4] = [b'c', b'e', b'l', b'i'];
static S_2_22: [symbol; 6] = [b'g', b'l', b'i', b'e', b'l', b'i'];
static S_2_23: [symbol; 4] = [b'm', b'e', b'l', b'i'];
static S_2_24: [symbol; 4] = [b't', b'e', b'l', b'i'];
static S_2_25: [symbol; 4] = [b'v', b'e', b'l', b'i'];
static S_2_26: [symbol; 3] = [b'g', b'l', b'i'];
static S_2_27: [symbol; 2] = [b'm', b'i'];
static S_2_28: [symbol; 2] = [b's', b'i'];
static S_2_29: [symbol; 2] = [b't', b'i'];
static S_2_30: [symbol; 2] = [b'v', b'i'];
static S_2_31: [symbol; 2] = [b'l', b'o'];
static S_2_32: [symbol; 4] = [b'c', b'e', b'l', b'o'];
static S_2_33: [symbol; 6] = [b'g', b'l', b'i', b'e', b'l', b'o'];
static S_2_34: [symbol; 4] = [b'm', b'e', b'l', b'o'];
static S_2_35: [symbol; 4] = [b't', b'e', b'l', b'o'];
static S_2_36: [symbol; 4] = [b'v', b'e', b'l', b'o'];

static A_2: [among; 37] = [
    among { s_size: 2, s: S_2_0.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 4, s: S_2_1.as_ptr(), substring_i: 0, result: -1, function: None },
    among { s_size: 6, s: S_2_2.as_ptr(), substring_i: 0, result: -1, function: None },
    among { s_size: 4, s: S_2_3.as_ptr(), substring_i: 0, result: -1, function: None },
    among { s_size: 4, s: S_2_4.as_ptr(), substring_i: 0, result: -1, function: None },
    among { s_size: 4, s: S_2_5.as_ptr(), substring_i: 0, result: -1, function: None },
    among { s_size: 2, s: S_2_6.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 4, s: S_2_7.as_ptr(), substring_i: 6, result: -1, function: None },
    among { s_size: 6, s: S_2_8.as_ptr(), substring_i: 6, result: -1, function: None },
    among { s_size: 4, s: S_2_9.as_ptr(), substring_i: 6, result: -1, function: None },
    among { s_size: 4, s: S_2_10.as_ptr(), substring_i: 6, result: -1, function: None },
    among { s_size: 4, s: S_2_11.as_ptr(), substring_i: 6, result: -1, function: None },
    among { s_size: 2, s: S_2_12.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 4, s: S_2_13.as_ptr(), substring_i: 12, result: -1, function: None },
    among { s_size: 6, s: S_2_14.as_ptr(), substring_i: 12, result: -1, function: None },
    among { s_size: 4, s: S_2_15.as_ptr(), substring_i: 12, result: -1, function: None },
    among { s_size: 4, s: S_2_16.as_ptr(), substring_i: 12, result: -1, function: None },
    among { s_size: 4, s: S_2_17.as_ptr(), substring_i: 12, result: -1, function: None },
    among { s_size: 4, s: S_2_18.as_ptr(), substring_i: 12, result: -1, function: None },
    among { s_size: 2, s: S_2_19.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_2_20.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 4, s: S_2_21.as_ptr(), substring_i: 20, result: -1, function: None },
    among { s_size: 6, s: S_2_22.as_ptr(), substring_i: 20, result: -1, function: None },
    among { s_size: 4, s: S_2_23.as_ptr(), substring_i: 20, result: -1, function: None },
    among { s_size: 4, s: S_2_24.as_ptr(), substring_i: 20, result: -1, function: None },
    among { s_size: 4, s: S_2_25.as_ptr(), substring_i: 20, result: -1, function: None },
    among { s_size: 3, s: S_2_26.as_ptr(), substring_i: 20, result: -1, function: None },
    among { s_size: 2, s: S_2_27.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_2_28.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_2_29.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_2_30.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_2_31.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 4, s: S_2_32.as_ptr(), substring_i: 31, result: -1, function: None },
    among { s_size: 6, s: S_2_33.as_ptr(), substring_i: 31, result: -1, function: None },
    among { s_size: 4, s: S_2_34.as_ptr(), substring_i: 31, result: -1, function: None },
    among { s_size: 4, s: S_2_35.as_ptr(), substring_i: 31, result: -1, function: None },
    among { s_size: 4, s: S_2_36.as_ptr(), substring_i: 31, result: -1, function: None },
];

static S_3_0: [symbol; 4] = [b'a', b'n', b'd', b'o'];
static S_3_1: [symbol; 4] = [b'e', b'n', b'd', b'o'];
static S_3_2: [symbol; 2] = [b'a', b'r'];
static S_3_3: [symbol; 2] = [b'e', b'r'];
static S_3_4: [symbol; 2] = [b'i', b'r'];

static A_3: [among; 5] = [
    among { s_size: 4, s: S_3_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_3_1.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_3_2.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 2, s: S_3_3.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 2, s: S_3_4.as_ptr(), substring_i: -1, result: 2, function: None },
];

static S_4_0: [symbol; 2] = [b'i', b'c'];
static S_4_1: [symbol; 4] = [b'a', b'b', b'i', b'l'];
static S_4_2: [symbol; 2] = [b'o', b's'];
static S_4_3: [symbol; 2] = [b'i', b'v'];

static A_4: [among; 4] = [
    among { s_size: 2, s: S_4_0.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 4, s: S_4_1.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_4_2.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_4_3.as_ptr(), substring_i: -1, result: 1, function: None },
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
static S_6_1: [symbol; 5] = [b'l', b'o', b'g', b'i', b'a'];
static S_6_2: [symbol; 3] = [b'o', b's', b'a'];
static S_6_3: [symbol; 4] = [b'i', b's', b't', b'a'];
static S_6_4: [symbol; 3] = [b'i', b'v', b'a'];
static S_6_5: [symbol; 4] = [b'a', b'n', b'z', b'a'];
static S_6_6: [symbol; 4] = [b'e', b'n', b'z', b'a'];
static S_6_7: [symbol; 3] = [b'i', b'c', b'e'];
static S_6_8: [symbol; 6] = [b'a', b't', b'r', b'i', b'c', b'e'];
static S_6_9: [symbol; 4] = [b'i', b'c', b'h', b'e'];
static S_6_10: [symbol; 5] = [b'l', b'o', b'g', b'i', b'e'];
static S_6_11: [symbol; 5] = [b'a', b'b', b'i', b'l', b'e'];
static S_6_12: [symbol; 5] = [b'i', b'b', b'i', b'l', b'e'];
static S_6_13: [symbol; 6] = [b'u', b's', b'i', b'o', b'n', b'e'];
static S_6_14: [symbol; 6] = [b'a', b'z', b'i', b'o', b'n', b'e'];
static S_6_15: [symbol; 6] = [b'u', b'z', b'i', b'o', b'n', b'e'];
static S_6_16: [symbol; 5] = [b'a', b't', b'o', b'r', b'e'];
static S_6_17: [symbol; 3] = [b'o', b's', b'e'];
static S_6_18: [symbol; 4] = [b'a', b'n', b't', b'e'];
static S_6_19: [symbol; 5] = [b'm', b'e', b'n', b't', b'e'];
static S_6_20: [symbol; 6] = [b'a', b'm', b'e', b'n', b't', b'e'];
static S_6_21: [symbol; 4] = [b'i', b's', b't', b'e'];
static S_6_22: [symbol; 3] = [b'i', b'v', b'e'];
static S_6_23: [symbol; 4] = [b'a', b'n', b'z', b'e'];
static S_6_24: [symbol; 4] = [b'e', b'n', b'z', b'e'];
static S_6_25: [symbol; 3] = [b'i', b'c', b'i'];
static S_6_26: [symbol; 6] = [b'a', b't', b'r', b'i', b'c', b'i'];
static S_6_27: [symbol; 4] = [b'i', b'c', b'h', b'i'];
static S_6_28: [symbol; 5] = [b'a', b'b', b'i', b'l', b'i'];
static S_6_29: [symbol; 5] = [b'i', b'b', b'i', b'l', b'i'];
static S_6_30: [symbol; 4] = [b'i', b's', b'm', b'i'];
static S_6_31: [symbol; 6] = [b'u', b's', b'i', b'o', b'n', b'i'];
static S_6_32: [symbol; 6] = [b'a', b'z', b'i', b'o', b'n', b'i'];
static S_6_33: [symbol; 6] = [b'u', b'z', b'i', b'o', b'n', b'i'];
static S_6_34: [symbol; 5] = [b'a', b't', b'o', b'r', b'i'];
static S_6_35: [symbol; 3] = [b'o', b's', b'i'];
static S_6_36: [symbol; 4] = [b'a', b'n', b't', b'i'];
static S_6_37: [symbol; 6] = [b'a', b'm', b'e', b'n', b't', b'i'];
static S_6_38: [symbol; 6] = [b'i', b'm', b'e', b'n', b't', b'i'];
static S_6_39: [symbol; 4] = [b'i', b's', b't', b'i'];
static S_6_40: [symbol; 3] = [b'i', b'v', b'i'];
static S_6_41: [symbol; 3] = [b'i', b'c', b'o'];
static S_6_42: [symbol; 4] = [b'i', b's', b'm', b'o'];
static S_6_43: [symbol; 3] = [b'o', b's', b'o'];
static S_6_44: [symbol; 6] = [b'a', b'm', b'e', b'n', b't', b'o'];
static S_6_45: [symbol; 6] = [b'i', b'm', b'e', b'n', b't', b'o'];
static S_6_46: [symbol; 3] = [b'i', b'v', b'o'];
static S_6_47: [symbol; 3] = [b'i', b't', 0xE0];
static S_6_48: [symbol; 4] = [b'i', b's', b't', 0xE0];
static S_6_49: [symbol; 4] = [b'i', b's', b't', 0xE8];
static S_6_50: [symbol; 4] = [b'i', b's', b't', 0xEC];

static A_6: [among; 51] = [
    among { s_size: 3, s: S_6_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_6_1.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 3, s: S_6_2.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_6_3.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_6_4.as_ptr(), substring_i: -1, result: 9, function: None },
    among { s_size: 4, s: S_6_5.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_6_6.as_ptr(), substring_i: -1, result: 5, function: None },
    among { s_size: 3, s: S_6_7.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_6_8.as_ptr(), substring_i: 7, result: 1, function: None },
    among { s_size: 4, s: S_6_9.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_6_10.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 5, s: S_6_11.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_6_12.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_6_13.as_ptr(), substring_i: -1, result: 4, function: None },
    among { s_size: 6, s: S_6_14.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 6, s: S_6_15.as_ptr(), substring_i: -1, result: 4, function: None },
    among { s_size: 5, s: S_6_16.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 3, s: S_6_17.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_6_18.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_6_19.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_6_20.as_ptr(), substring_i: 19, result: 7, function: None },
    among { s_size: 4, s: S_6_21.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_6_22.as_ptr(), substring_i: -1, result: 9, function: None },
    among { s_size: 4, s: S_6_23.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_6_24.as_ptr(), substring_i: -1, result: 5, function: None },
    among { s_size: 3, s: S_6_25.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_6_26.as_ptr(), substring_i: 25, result: 1, function: None },
    among { s_size: 4, s: S_6_27.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_6_28.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_6_29.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_6_30.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_6_31.as_ptr(), substring_i: -1, result: 4, function: None },
    among { s_size: 6, s: S_6_32.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 6, s: S_6_33.as_ptr(), substring_i: -1, result: 4, function: None },
    among { s_size: 5, s: S_6_34.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 3, s: S_6_35.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_6_36.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_6_37.as_ptr(), substring_i: -1, result: 6, function: None },
    among { s_size: 6, s: S_6_38.as_ptr(), substring_i: -1, result: 6, function: None },
    among { s_size: 4, s: S_6_39.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_6_40.as_ptr(), substring_i: -1, result: 9, function: None },
    among { s_size: 3, s: S_6_41.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_6_42.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_6_43.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_6_44.as_ptr(), substring_i: -1, result: 6, function: None },
    among { s_size: 6, s: S_6_45.as_ptr(), substring_i: -1, result: 6, function: None },
    among { s_size: 3, s: S_6_46.as_ptr(), substring_i: -1, result: 9, function: None },
    among { s_size: 3, s: S_6_47.as_ptr(), substring_i: -1, result: 8, function: None },
    among { s_size: 4, s: S_6_48.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_6_49.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_6_50.as_ptr(), substring_i: -1, result: 1, function: None },
];

static S_7_0: [symbol; 4] = [b'i', b's', b'c', b'a'];
static S_7_1: [symbol; 4] = [b'e', b'n', b'd', b'a'];
static S_7_2: [symbol; 3] = [b'a', b't', b'a'];
static S_7_3: [symbol; 3] = [b'i', b't', b'a'];
static S_7_4: [symbol; 3] = [b'u', b't', b'a'];
static S_7_5: [symbol; 3] = [b'a', b'v', b'a'];
static S_7_6: [symbol; 3] = [b'e', b'v', b'a'];
static S_7_7: [symbol; 3] = [b'i', b'v', b'a'];
static S_7_8: [symbol; 6] = [b'e', b'r', b'e', b'b', b'b', b'e'];
static S_7_9: [symbol; 6] = [b'i', b'r', b'e', b'b', b'b', b'e'];
static S_7_10: [symbol; 4] = [b'i', b's', b'c', b'e'];
static S_7_11: [symbol; 4] = [b'e', b'n', b'd', b'e'];
static S_7_12: [symbol; 3] = [b'a', b'r', b'e'];
static S_7_13: [symbol; 3] = [b'e', b'r', b'e'];
static S_7_14: [symbol; 3] = [b'i', b'r', b'e'];
static S_7_15: [symbol; 4] = [b'a', b's', b's', b'e'];
static S_7_16: [symbol; 3] = [b'a', b't', b'e'];
static S_7_17: [symbol; 5] = [b'a', b'v', b'a', b't', b'e'];
static S_7_18: [symbol; 5] = [b'e', b'v', b'a', b't', b'e'];
static S_7_19: [symbol; 5] = [b'i', b'v', b'a', b't', b'e'];
static S_7_20: [symbol; 3] = [b'e', b't', b'e'];
static S_7_21: [symbol; 5] = [b'e', b'r', b'e', b't', b'e'];
static S_7_22: [symbol; 5] = [b'i', b'r', b'e', b't', b'e'];
static S_7_23: [symbol; 3] = [b'i', b't', b'e'];
static S_7_24: [symbol; 6] = [b'e', b'r', b'e', b's', b't', b'e'];
static S_7_25: [symbol; 6] = [b'i', b'r', b'e', b's', b't', b'e'];
static S_7_26: [symbol; 3] = [b'u', b't', b'e'];
static S_7_27: [symbol; 4] = [b'e', b'r', b'a', b'i'];
static S_7_28: [symbol; 4] = [b'i', b'r', b'a', b'i'];
static S_7_29: [symbol; 4] = [b'i', b's', b'c', b'i'];
static S_7_30: [symbol; 4] = [b'e', b'n', b'd', b'i'];
static S_7_31: [symbol; 4] = [b'e', b'r', b'e', b'i'];
static S_7_32: [symbol; 4] = [b'i', b'r', b'e', b'i'];
static S_7_33: [symbol; 4] = [b'a', b's', b's', b'i'];
static S_7_34: [symbol; 3] = [b'a', b't', b'i'];
static S_7_35: [symbol; 3] = [b'i', b't', b'i'];
static S_7_36: [symbol; 6] = [b'e', b'r', b'e', b's', b't', b'i'];
static S_7_37: [symbol; 6] = [b'i', b'r', b'e', b's', b't', b'i'];
static S_7_38: [symbol; 3] = [b'u', b't', b'i'];
static S_7_39: [symbol; 3] = [b'a', b'v', b'i'];
static S_7_40: [symbol; 3] = [b'e', b'v', b'i'];
static S_7_41: [symbol; 3] = [b'i', b'v', b'i'];
static S_7_42: [symbol; 4] = [b'i', b's', b'c', b'o'];
static S_7_43: [symbol; 4] = [b'a', b'n', b'd', b'o'];
static S_7_44: [symbol; 4] = [b'e', b'n', b'd', b'o'];
static S_7_45: [symbol; 4] = [b'Y', b'a', b'm', b'o'];
static S_7_46: [symbol; 4] = [b'i', b'a', b'm', b'o'];
static S_7_47: [symbol; 5] = [b'a', b'v', b'a', b'm', b'o'];
static S_7_48: [symbol; 5] = [b'e', b'v', b'a', b'm', b'o'];
static S_7_49: [symbol; 5] = [b'i', b'v', b'a', b'm', b'o'];
static S_7_50: [symbol; 5] = [b'e', b'r', b'e', b'm', b'o'];
static S_7_51: [symbol; 5] = [b'i', b'r', b'e', b'm', b'o'];
static S_7_52: [symbol; 6] = [b'a', b's', b's', b'i', b'm', b'o'];
static S_7_53: [symbol; 4] = [b'a', b'm', b'm', b'o'];
static S_7_54: [symbol; 4] = [b'e', b'm', b'm', b'o'];
static S_7_55: [symbol; 6] = [b'e', b'r', b'e', b'm', b'm', b'o'];
static S_7_56: [symbol; 6] = [b'i', b'r', b'e', b'm', b'm', b'o'];
static S_7_57: [symbol; 4] = [b'i', b'm', b'm', b'o'];
static S_7_58: [symbol; 3] = [b'a', b'n', b'o'];
static S_7_59: [symbol; 6] = [b'i', b's', b'c', b'a', b'n', b'o'];
static S_7_60: [symbol; 5] = [b'a', b'v', b'a', b'n', b'o'];
static S_7_61: [symbol; 5] = [b'e', b'v', b'a', b'n', b'o'];
static S_7_62: [symbol; 5] = [b'i', b'v', b'a', b'n', b'o'];
static S_7_63: [symbol; 6] = [b'e', b'r', b'a', b'n', b'n', b'o'];
static S_7_64: [symbol; 6] = [b'i', b'r', b'a', b'n', b'n', b'o'];
static S_7_65: [symbol; 3] = [b'o', b'n', b'o'];
static S_7_66: [symbol; 6] = [b'i', b's', b'c', b'o', b'n', b'o'];
static S_7_67: [symbol; 5] = [b'a', b'r', b'o', b'n', b'o'];
static S_7_68: [symbol; 5] = [b'e', b'r', b'o', b'n', b'o'];
static S_7_69: [symbol; 5] = [b'i', b'r', b'o', b'n', b'o'];
static S_7_70: [symbol; 8] = [b'e', b'r', b'e', b'b', b'b', b'e', b'r', b'o'];
static S_7_71: [symbol; 8] = [b'i', b'r', b'e', b'b', b'b', b'e', b'r', b'o'];
static S_7_72: [symbol; 6] = [b'a', b's', b's', b'e', b'r', b'o'];
static S_7_73: [symbol; 6] = [b'e', b's', b's', b'e', b'r', b'o'];
static S_7_74: [symbol; 6] = [b'i', b's', b's', b'e', b'r', b'o'];
static S_7_75: [symbol; 3] = [b'a', b't', b'o'];
static S_7_76: [symbol; 3] = [b'i', b't', b'o'];
static S_7_77: [symbol; 3] = [b'u', b't', b'o'];
static S_7_78: [symbol; 3] = [b'a', b'v', b'o'];
static S_7_79: [symbol; 3] = [b'e', b'v', b'o'];
static S_7_80: [symbol; 3] = [b'i', b'v', b'o'];
static S_7_81: [symbol; 2] = [b'a', b'r'];
static S_7_82: [symbol; 2] = [b'i', b'r'];
static S_7_83: [symbol; 3] = [b'e', b'r', 0xE0];
static S_7_84: [symbol; 3] = [b'i', b'r', 0xE0];
static S_7_85: [symbol; 3] = [b'e', b'r', 0xF2];
static S_7_86: [symbol; 3] = [b'i', b'r', 0xF2];

static A_7: [among; 87] = [
    among { s_size: 4, s: S_7_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_7_1.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_7_2.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_7_3.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_7_4.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_7_5.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_7_6.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_7_7.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_7_8.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_7_9.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_7_10.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_7_11.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_7_12.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_7_13.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_7_14.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_7_15.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_7_16.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_7_17.as_ptr(), substring_i: 16, result: 1, function: None },
    among { s_size: 5, s: S_7_18.as_ptr(), substring_i: 16, result: 1, function: None },
    among { s_size: 5, s: S_7_19.as_ptr(), substring_i: 16, result: 1, function: None },
    among { s_size: 3, s: S_7_20.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_7_21.as_ptr(), substring_i: 20, result: 1, function: None },
    among { s_size: 5, s: S_7_22.as_ptr(), substring_i: 20, result: 1, function: None },
    among { s_size: 3, s: S_7_23.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_7_24.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_7_25.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_7_26.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_7_27.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_7_28.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_7_29.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_7_30.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_7_31.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_7_32.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_7_33.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_7_34.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_7_35.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_7_36.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_7_37.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_7_38.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_7_39.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_7_40.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_7_41.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_7_42.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_7_43.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_7_44.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_7_45.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_7_46.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_7_47.as_ptr(), substring_i: 46, result: 1, function: None },
    among { s_size: 5, s: S_7_48.as_ptr(), substring_i: 46, result: 1, function: None },
    among { s_size: 5, s: S_7_49.as_ptr(), substring_i: 46, result: 1, function: None },
    among { s_size: 5, s: S_7_50.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_7_51.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_7_52.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_7_53.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_7_54.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_7_55.as_ptr(), substring_i: 54, result: 1, function: None },
    among { s_size: 6, s: S_7_56.as_ptr(), substring_i: 54, result: 1, function: None },
    among { s_size: 4, s: S_7_57.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_7_58.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_7_59.as_ptr(), substring_i: 58, result: 1, function: None },
    among { s_size: 5, s: S_7_60.as_ptr(), substring_i: 58, result: 1, function: None },
    among { s_size: 5, s: S_7_61.as_ptr(), substring_i: 58, result: 1, function: None },
    among { s_size: 5, s: S_7_62.as_ptr(), substring_i: 58, result: 1, function: None },
    among { s_size: 6, s: S_7_63.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_7_64.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_7_65.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_7_66.as_ptr(), substring_i: 65, result: 1, function: None },
    among { s_size: 5, s: S_7_67.as_ptr(), substring_i: 65, result: 1, function: None },
    among { s_size: 5, s: S_7_68.as_ptr(), substring_i: 65, result: 1, function: None },
    among { s_size: 5, s: S_7_69.as_ptr(), substring_i: 65, result: 1, function: None },
    among { s_size: 8, s: S_7_70.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_7_71.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_7_72.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_7_73.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_7_74.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_7_75.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_7_76.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_7_77.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_7_78.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_7_79.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_7_80.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_7_81.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_7_82.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_7_83.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_7_84.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_7_85.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_7_86.as_ptr(), substring_i: -1, result: 1, function: None },
];

// ---------------------------------------------------------------------------
// grouping bit tables
// ---------------------------------------------------------------------------

static G_V: [c_uchar; 20] = [
    17, 65, 16, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 128, 128, 8, 2, 1,
];

static G_AEIO: [c_uchar; 19] = [
    17, 65, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 128, 128, 8, 2,
];

static G_CG: [c_uchar; 1] = [17];

// ---------------------------------------------------------------------------
// literal strings used by slice_from_s / eq_s / eq_s_b
// ---------------------------------------------------------------------------

static S_0: [symbol; 1] = [0xE0];
static S_1: [symbol; 1] = [0xE8];
static S_2: [symbol; 1] = [0xEC];
static S_3: [symbol; 1] = [0xF2];
static S_4: [symbol; 1] = [0xF9];
static S_5: [symbol; 2] = [b'q', b'U'];
static S_6: [symbol; 1] = [b'U'];
static S_7: [symbol; 1] = [b'I'];
static S_8: [symbol; 1] = [b'i'];
static S_9: [symbol; 1] = [b'u'];
static S_10: [symbol; 1] = [b'e'];
static S_11: [symbol; 2] = [b'i', b'c'];
static S_12: [symbol; 3] = [b'l', b'o', b'g'];
static S_13: [symbol; 1] = [b'u'];
static S_14: [symbol; 4] = [b'e', b'n', b't', b'e'];
static S_15: [symbol; 2] = [b'a', b't'];
static S_16: [symbol; 2] = [b'a', b't'];
static S_17: [symbol; 2] = [b'i', b'c'];
static S_18: [symbol; 6] = [b'd', b'i', b'v', b'a', b'n', b'o'];
static S_19: [symbol; 5] = [b'd', b'i', b'v', b'a', b'n'];

// ---------------------------------------------------------------------------
// rules
// ---------------------------------------------------------------------------

unsafe fn r_prelude(z: *mut SN_env) -> c_int {
    let mut among_var;
    {
        let c_test1 = (*z).c;
        'loop0: loop {
            let c2 = (*z).c;
            'lab0: {
                (*z).bra = (*z).c;
                among_var = find_among(z, A_0.as_ptr(), 7);
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
                        let ret = slice_from_s(z, 2, S_5.as_ptr());
                        if ret < 0 {
                            return ret;
                        }
                    }
                    7 => {
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
            (*z).c = c2;
            break 'loop0;
        }
        (*z).c = c_test1;
    }
    'loop1: loop {
        let c3 = (*z).c;
        'lab1: {
            'loop2: loop {
                let c4 = (*z).c;
                'lab2: {
                    if in_grouping(z, G_V.as_ptr(), 97, 249, 0) != 0 {
                        break 'lab2;
                    }
                    (*z).bra = (*z).c;
                    'lab3: {
                        let c5 = (*z).c;
                        'lab4: {
                            if (*z).c == (*z).l || *(*z).p.offset((*z).c as isize) != b'u' {
                                break 'lab4;
                            }
                            (*z).c += 1;
                            (*z).ket = (*z).c;
                            if in_grouping(z, G_V.as_ptr(), 97, 249, 0) != 0 {
                                break 'lab4;
                            }
                            {
                                let ret = slice_from_s(z, 1, S_6.as_ptr());
                                if ret < 0 {
                                    return ret;
                                }
                            }
                            break 'lab3;
                        }
                        // lab4:
                        (*z).c = c5;
                        if (*z).c == (*z).l || *(*z).p.offset((*z).c as isize) != b'i' {
                            break 'lab2;
                        }
                        (*z).c += 1;
                        (*z).ket = (*z).c;
                        if in_grouping(z, G_V.as_ptr(), 97, 249, 0) != 0 {
                            break 'lab2;
                        }
                        {
                            let ret = slice_from_s(z, 1, S_7.as_ptr());
                            if ret < 0 {
                                return ret;
                            }
                        }
                    }
                    // lab3:
                    (*z).c = c4;
                    break 'loop2;
                }
                // lab2:
                (*z).c = c4;
                if (*z).c >= (*z).l {
                    break 'lab1;
                }
                (*z).c += 1;
            }
            continue 'loop1;
        }
        // lab1:
        (*z).c = c3;
        break 'loop1;
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
                    if in_grouping(z, G_V.as_ptr(), 97, 249, 0) != 0 {
                        break 'lab2;
                    }
                    {
                        let c3 = (*z).c;
                        'lab3: {
                            'lab4: {
                                if out_grouping(z, G_V.as_ptr(), 97, 249, 0) != 0 {
                                    break 'lab4;
                                }
                                {
                                    let ret = out_grouping(z, G_V.as_ptr(), 97, 249, 1);
                                    if ret < 0 {
                                        break 'lab4;
                                    }
                                    (*z).c += ret;
                                }
                                break 'lab3;
                            }
                            // lab4:
                            (*z).c = c3;
                            if in_grouping(z, G_V.as_ptr(), 97, 249, 0) != 0 {
                                break 'lab2;
                            }
                            {
                                let ret = in_grouping(z, G_V.as_ptr(), 97, 249, 1);
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
                if out_grouping(z, G_V.as_ptr(), 97, 249, 0) != 0 {
                    break 'lab0;
                }
                {
                    let c4 = (*z).c;
                    'lab5: {
                        'lab6: {
                            if out_grouping(z, G_V.as_ptr(), 97, 249, 0) != 0 {
                                break 'lab6;
                            }
                            {
                                let ret = out_grouping(z, G_V.as_ptr(), 97, 249, 1);
                                if ret < 0 {
                                    break 'lab6;
                                }
                                (*z).c += ret;
                            }
                            break 'lab5;
                        }
                        // lab6:
                        (*z).c = c4;
                        if in_grouping(z, G_V.as_ptr(), 97, 249, 0) != 0 {
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
                let ret = out_grouping(z, G_V.as_ptr(), 97, 249, 1);
                if ret < 0 {
                    break 'lab7;
                }
                (*z).c += ret;
            }
            {
                let ret = in_grouping(z, G_V.as_ptr(), 97, 249, 1);
                if ret < 0 {
                    break 'lab7;
                }
                (*z).c += ret;
            }
            *(*z).I.offset(1) = (*z).c;
            {
                let ret = out_grouping(z, G_V.as_ptr(), 97, 249, 1);
                if ret < 0 {
                    break 'lab7;
                }
                (*z).c += ret;
            }
            {
                let ret = in_grouping(z, G_V.as_ptr(), 97, 249, 1);
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
                || (*(*z).p.offset((*z).c as isize) != 73 && *(*z).p.offset((*z).c as isize) != 85)
            {
                among_var = 3;
            } else {
                among_var = find_among(z, A_1.as_ptr(), 3);
            }
            (*z).ket = (*z).c;
            match among_var {
                1 => {
                    let ret = slice_from_s(z, 1, S_8.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
                2 => {
                    let ret = slice_from_s(z, 1, S_9.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
                3 => {
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
        || (33314 >> (*(*z).p.offset(((*z).c - 1) as isize) as c_int & 0x1f)) & 1 == 0
    {
        return 0;
    }
    if find_among_b(z, A_2.as_ptr(), 37) == 0 {
        return 0;
    }
    (*z).bra = (*z).c;
    if (*z).c - 1 <= (*z).lb
        || (*(*z).p.offset(((*z).c - 1) as isize) != 111
            && *(*z).p.offset(((*z).c - 1) as isize) != 114)
    {
        return 0;
    }
    among_var = find_among_b(z, A_3.as_ptr(), 5);
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
        _ => {}
    }
    1
}

unsafe fn r_standard_suffix(z: *mut SN_env) -> c_int {
    let mut among_var;
    (*z).ket = (*z).c;
    among_var = find_among_b(z, A_6.as_ptr(), 51);
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
                    if eq_s_b(z, 2, S_11.as_ptr()) == 0 {
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
                let ret = slice_from_s(z, 3, S_12.as_ptr());
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
                let ret = slice_from_s(z, 1, S_13.as_ptr());
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
                let ret = slice_from_s(z, 4, S_14.as_ptr());
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
        }
        7 => {
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
                        || (4722696 >> (*(*z).p.offset(((*z).c - 1) as isize) as c_int & 0x1f)) & 1
                            == 0
                    {
                        (*z).c = (*z).l - m2;
                        break 'lab1;
                    }
                    among_var = find_among_b(z, A_4.as_ptr(), 4);
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
                            if eq_s_b(z, 2, S_15.as_ptr()) == 0 {
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
                let m3 = (*z).l - (*z).c;
                'lab2: {
                    (*z).ket = (*z).c;
                    if (*z).c - 1 <= (*z).lb
                        || *(*z).p.offset(((*z).c - 1) as isize) as c_int >> 5 != 3
                        || (4198408 >> (*(*z).p.offset(((*z).c - 1) as isize) as c_int & 0x1f)) & 1
                            == 0
                    {
                        (*z).c = (*z).l - m3;
                        break 'lab2;
                    }
                    if find_among_b(z, A_5.as_ptr(), 3) == 0 {
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
                let m4 = (*z).l - (*z).c;
                'lab3: {
                    (*z).ket = (*z).c;
                    if eq_s_b(z, 2, S_16.as_ptr()) == 0 {
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
                    {
                        let ret = slice_del(z);
                        if ret < 0 {
                            return ret;
                        }
                    }
                    (*z).ket = (*z).c;
                    if eq_s_b(z, 2, S_17.as_ptr()) == 0 {
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
        _ => {}
    }
    1
}

unsafe fn r_verb_suffix(z: *mut SN_env) -> c_int {
    {
        let mlimit1;
        if (*z).c < *(*z).I.offset(2) {
            return 0;
        }
        mlimit1 = (*z).lb;
        (*z).lb = *(*z).I.offset(2);
        (*z).ket = (*z).c;
        if find_among_b(z, A_7.as_ptr(), 87) == 0 {
            (*z).lb = mlimit1;
            return 0;
        }
        (*z).bra = (*z).c;
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

unsafe fn r_vowel_suffix(z: *mut SN_env) -> c_int {
    {
        let m1 = (*z).l - (*z).c;
        'lab0: {
            (*z).ket = (*z).c;
            if in_grouping_b(z, G_AEIO.as_ptr(), 97, 242, 0) != 0 {
                (*z).c = (*z).l - m1;
                break 'lab0;
            }
            (*z).bra = (*z).c;
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
            {
                let ret = slice_del(z);
                if ret < 0 {
                    return ret;
                }
            }
            (*z).ket = (*z).c;
            if (*z).c <= (*z).lb || *(*z).p.offset(((*z).c - 1) as isize) != b'i' {
                (*z).c = (*z).l - m1;
                break 'lab0;
            }
            (*z).c -= 1;
            (*z).bra = (*z).c;
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
    {
        let m2 = (*z).l - (*z).c;
        'lab1: {
            (*z).ket = (*z).c;
            if (*z).c <= (*z).lb || *(*z).p.offset(((*z).c - 1) as isize) != b'h' {
                (*z).c = (*z).l - m2;
                break 'lab1;
            }
            (*z).c -= 1;
            (*z).bra = (*z).c;
            if in_grouping_b(z, G_CG.as_ptr(), 99, 103, 0) != 0 {
                (*z).c = (*z).l - m2;
                break 'lab1;
            }
            {
                let ret = r_RV(z);
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
    }
    1
}

unsafe fn r_exceptions(z: *mut SN_env) -> c_int {
    (*z).bra = (*z).c;
    if eq_s(z, 6, S_18.as_ptr()) == 0 {
        return 0;
    }
    if (*z).c < (*z).l {
        return 0;
    }
    (*z).ket = (*z).c;
    {
        let ret = slice_from_s(z, 5, S_19.as_ptr());
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
pub unsafe extern "C" fn italian_ISO_8859_1_stem(z: *mut SN_env) -> c_int {
    'lab0: {
        let c1 = (*z).c;
        'lab1: {
            {
                let ret = r_exceptions(z);
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
            {
                let ret = r_attached_pronoun(z);
                if ret < 0 {
                    return ret;
                }
            }
            (*z).c = (*z).l - m3;
        }
        {
            let m4 = (*z).l - (*z).c;
            'lab2: {
                'lab3: {
                    let m5 = (*z).l - (*z).c;
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
                    (*z).c = (*z).l - m5;
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
            // lab2:
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
    }
    // lab0:
    1
}

#[no_mangle]
pub unsafe extern "C" fn italian_ISO_8859_1_create_env() -> *mut SN_env {
    SN_create_env(0, 3)
}

#[no_mangle]
pub unsafe extern "C" fn italian_ISO_8859_1_close_env(z: *mut SN_env) {
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
        let z = italian_ISO_8859_1_create_env();
        assert!(!z.is_null());
        let rc = SN_set_current(z, word.len() as c_int, word.as_ptr());
        assert!(rc >= 0);
        let rc = italian_ISO_8859_1_stem(z);
        assert!(rc >= 0, "stem returned {rc}");
        let out = std::slice::from_raw_parts((*z).p, SIZE((*z).p) as usize).to_vec();
        italian_ISO_8859_1_close_env(z);
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
    // ISO-8859-1 (single-byte), e.g. 0xE0 for 'a' with grave.
    #[test]
    fn idempotent() {
        unsafe {
            for w in [
                &b"case"[..],
                &b"parlando"[..],
                &b"nazionale"[..],
                &b"abbandono"[..],
                &b"citt\xe0"[..],
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
            let r = stem(b"nazionale");
            assert!(!r.is_empty());
            assert!(r.len() <= "nazionale".len());
        }
    }
}
