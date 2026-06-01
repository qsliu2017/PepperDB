//! Turkish Snowball stemmer (UTF-8).
//!
//! 1:1 translation of the generated
//! `src/backend/snowball/libstemmer/stem_UTF_8_turkish.c` (Snowball 2.2.0),
//! merged with its header `stem_UTF_8_turkish.h`. The runtime helpers come from
//! `crate::snowball::api` and `crate::snowball::utilities`.

use crate::prelude::*;

use crate::snowball::api::{among, symbol, SN_close_env, SN_create_env, SN_env, SIZE};
use crate::snowball::utilities::{
    eq_s_b, find_among_b, in_grouping_b_U, out_grouping_b_U, out_grouping_U, skip_b_utf8,
    skip_utf8, slice_del, slice_from_s,
};

// ---------------------------------------------------------------------------
// among string tables
// ---------------------------------------------------------------------------

static S_0_0: [symbol; 1] = [b'm'];
static S_0_1: [symbol; 1] = [b'n'];
static S_0_2: [symbol; 3] = [b'm', b'i', b'z'];
static S_0_3: [symbol; 3] = [b'n', b'i', b'z'];
static S_0_4: [symbol; 3] = [b'm', b'u', b'z'];
static S_0_5: [symbol; 3] = [b'n', b'u', b'z'];
static S_0_6: [symbol; 4] = [b'm', 0xC4, 0xB1, b'z'];
static S_0_7: [symbol; 4] = [b'n', 0xC4, 0xB1, b'z'];
static S_0_8: [symbol; 4] = [b'm', 0xC3, 0xBC, b'z'];
static S_0_9: [symbol; 4] = [b'n', 0xC3, 0xBC, b'z'];

static A_0: [among; 10] = [
    among { s_size: 1, s: S_0_0.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 1, s: S_0_1.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_0_2.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_0_3.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_0_4.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_0_5.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 4, s: S_0_6.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 4, s: S_0_7.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 4, s: S_0_8.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 4, s: S_0_9.as_ptr(), substring_i: -1, result: -1, function: None },
];

static S_1_0: [symbol; 4] = [b'l', b'e', b'r', b'i'];
static S_1_1: [symbol; 5] = [b'l', b'a', b'r', 0xC4, 0xB1];

static A_1: [among; 2] = [
    among { s_size: 4, s: S_1_0.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 5, s: S_1_1.as_ptr(), substring_i: -1, result: -1, function: None },
];

static S_2_0: [symbol; 2] = [b'n', b'i'];
static S_2_1: [symbol; 2] = [b'n', b'u'];
static S_2_2: [symbol; 3] = [b'n', 0xC4, 0xB1];
static S_2_3: [symbol; 3] = [b'n', 0xC3, 0xBC];

static A_2: [among; 4] = [
    among { s_size: 2, s: S_2_0.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_2_1.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_2_2.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_2_3.as_ptr(), substring_i: -1, result: -1, function: None },
];

static S_3_0: [symbol; 2] = [b'i', b'n'];
static S_3_1: [symbol; 2] = [b'u', b'n'];
static S_3_2: [symbol; 3] = [0xC4, 0xB1, b'n'];
static S_3_3: [symbol; 3] = [0xC3, 0xBC, b'n'];

static A_3: [among; 4] = [
    among { s_size: 2, s: S_3_0.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_3_1.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_3_2.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_3_3.as_ptr(), substring_i: -1, result: -1, function: None },
];

static S_4_0: [symbol; 1] = [b'a'];
static S_4_1: [symbol; 1] = [b'e'];

static A_4: [among; 2] = [
    among { s_size: 1, s: S_4_0.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 1, s: S_4_1.as_ptr(), substring_i: -1, result: -1, function: None },
];

static S_5_0: [symbol; 2] = [b'n', b'a'];
static S_5_1: [symbol; 2] = [b'n', b'e'];

static A_5: [among; 2] = [
    among { s_size: 2, s: S_5_0.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_5_1.as_ptr(), substring_i: -1, result: -1, function: None },
];

static S_6_0: [symbol; 2] = [b'd', b'a'];
static S_6_1: [symbol; 2] = [b't', b'a'];
static S_6_2: [symbol; 2] = [b'd', b'e'];
static S_6_3: [symbol; 2] = [b't', b'e'];

static A_6: [among; 4] = [
    among { s_size: 2, s: S_6_0.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_6_1.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_6_2.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_6_3.as_ptr(), substring_i: -1, result: -1, function: None },
];

static S_7_0: [symbol; 3] = [b'n', b'd', b'a'];
static S_7_1: [symbol; 3] = [b'n', b'd', b'e'];

static A_7: [among; 2] = [
    among { s_size: 3, s: S_7_0.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_7_1.as_ptr(), substring_i: -1, result: -1, function: None },
];

static S_8_0: [symbol; 3] = [b'd', b'a', b'n'];
static S_8_1: [symbol; 3] = [b't', b'a', b'n'];
static S_8_2: [symbol; 3] = [b'd', b'e', b'n'];
static S_8_3: [symbol; 3] = [b't', b'e', b'n'];

static A_8: [among; 4] = [
    among { s_size: 3, s: S_8_0.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_8_1.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_8_2.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_8_3.as_ptr(), substring_i: -1, result: -1, function: None },
];

static S_9_0: [symbol; 4] = [b'n', b'd', b'a', b'n'];
static S_9_1: [symbol; 4] = [b'n', b'd', b'e', b'n'];

static A_9: [among; 2] = [
    among { s_size: 4, s: S_9_0.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 4, s: S_9_1.as_ptr(), substring_i: -1, result: -1, function: None },
];

static S_10_0: [symbol; 2] = [b'l', b'a'];
static S_10_1: [symbol; 2] = [b'l', b'e'];

static A_10: [among; 2] = [
    among { s_size: 2, s: S_10_0.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_10_1.as_ptr(), substring_i: -1, result: -1, function: None },
];

static S_11_0: [symbol; 2] = [b'c', b'a'];
static S_11_1: [symbol; 2] = [b'c', b'e'];

static A_11: [among; 2] = [
    among { s_size: 2, s: S_11_0.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_11_1.as_ptr(), substring_i: -1, result: -1, function: None },
];

static S_12_0: [symbol; 2] = [b'i', b'm'];
static S_12_1: [symbol; 2] = [b'u', b'm'];
static S_12_2: [symbol; 3] = [0xC4, 0xB1, b'm'];
static S_12_3: [symbol; 3] = [0xC3, 0xBC, b'm'];

static A_12: [among; 4] = [
    among { s_size: 2, s: S_12_0.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_12_1.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_12_2.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_12_3.as_ptr(), substring_i: -1, result: -1, function: None },
];

static S_13_0: [symbol; 3] = [b's', b'i', b'n'];
static S_13_1: [symbol; 3] = [b's', b'u', b'n'];
static S_13_2: [symbol; 4] = [b's', 0xC4, 0xB1, b'n'];
static S_13_3: [symbol; 4] = [b's', 0xC3, 0xBC, b'n'];

static A_13: [among; 4] = [
    among { s_size: 3, s: S_13_0.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_13_1.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 4, s: S_13_2.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 4, s: S_13_3.as_ptr(), substring_i: -1, result: -1, function: None },
];

static S_14_0: [symbol; 2] = [b'i', b'z'];
static S_14_1: [symbol; 2] = [b'u', b'z'];
static S_14_2: [symbol; 3] = [0xC4, 0xB1, b'z'];
static S_14_3: [symbol; 3] = [0xC3, 0xBC, b'z'];

static A_14: [among; 4] = [
    among { s_size: 2, s: S_14_0.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_14_1.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_14_2.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_14_3.as_ptr(), substring_i: -1, result: -1, function: None },
];

static S_15_0: [symbol; 5] = [b's', b'i', b'n', b'i', b'z'];
static S_15_1: [symbol; 5] = [b's', b'u', b'n', b'u', b'z'];
static S_15_2: [symbol; 7] = [b's', 0xC4, 0xB1, b'n', 0xC4, 0xB1, b'z'];
static S_15_3: [symbol; 7] = [b's', 0xC3, 0xBC, b'n', 0xC3, 0xBC, b'z'];

static A_15: [among; 4] = [
    among { s_size: 5, s: S_15_0.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 5, s: S_15_1.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 7, s: S_15_2.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 7, s: S_15_3.as_ptr(), substring_i: -1, result: -1, function: None },
];

static S_16_0: [symbol; 3] = [b'l', b'a', b'r'];
static S_16_1: [symbol; 3] = [b'l', b'e', b'r'];

static A_16: [among; 2] = [
    among { s_size: 3, s: S_16_0.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_16_1.as_ptr(), substring_i: -1, result: -1, function: None },
];

static S_17_0: [symbol; 3] = [b'n', b'i', b'z'];
static S_17_1: [symbol; 3] = [b'n', b'u', b'z'];
static S_17_2: [symbol; 4] = [b'n', 0xC4, 0xB1, b'z'];
static S_17_3: [symbol; 4] = [b'n', 0xC3, 0xBC, b'z'];

static A_17: [among; 4] = [
    among { s_size: 3, s: S_17_0.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_17_1.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 4, s: S_17_2.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 4, s: S_17_3.as_ptr(), substring_i: -1, result: -1, function: None },
];

static S_18_0: [symbol; 3] = [b'd', b'i', b'r'];
static S_18_1: [symbol; 3] = [b't', b'i', b'r'];
static S_18_2: [symbol; 3] = [b'd', b'u', b'r'];
static S_18_3: [symbol; 3] = [b't', b'u', b'r'];
static S_18_4: [symbol; 4] = [b'd', 0xC4, 0xB1, b'r'];
static S_18_5: [symbol; 4] = [b't', 0xC4, 0xB1, b'r'];
static S_18_6: [symbol; 4] = [b'd', 0xC3, 0xBC, b'r'];
static S_18_7: [symbol; 4] = [b't', 0xC3, 0xBC, b'r'];

static A_18: [among; 8] = [
    among { s_size: 3, s: S_18_0.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_18_1.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_18_2.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_18_3.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 4, s: S_18_4.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 4, s: S_18_5.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 4, s: S_18_6.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 4, s: S_18_7.as_ptr(), substring_i: -1, result: -1, function: None },
];

static S_19_0: [symbol; 7] = [b'c', b'a', b's', 0xC4, 0xB1, b'n', b'a'];
static S_19_1: [symbol; 6] = [b'c', b'e', b's', b'i', b'n', b'e'];

static A_19: [among; 2] = [
    among { s_size: 7, s: S_19_0.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 6, s: S_19_1.as_ptr(), substring_i: -1, result: -1, function: None },
];

static S_20_0: [symbol; 2] = [b'd', b'i'];
static S_20_1: [symbol; 2] = [b't', b'i'];
static S_20_2: [symbol; 3] = [b'd', b'i', b'k'];
static S_20_3: [symbol; 3] = [b't', b'i', b'k'];
static S_20_4: [symbol; 3] = [b'd', b'u', b'k'];
static S_20_5: [symbol; 3] = [b't', b'u', b'k'];
static S_20_6: [symbol; 4] = [b'd', 0xC4, 0xB1, b'k'];
static S_20_7: [symbol; 4] = [b't', 0xC4, 0xB1, b'k'];
static S_20_8: [symbol; 4] = [b'd', 0xC3, 0xBC, b'k'];
static S_20_9: [symbol; 4] = [b't', 0xC3, 0xBC, b'k'];
static S_20_10: [symbol; 3] = [b'd', b'i', b'm'];
static S_20_11: [symbol; 3] = [b't', b'i', b'm'];
static S_20_12: [symbol; 3] = [b'd', b'u', b'm'];
static S_20_13: [symbol; 3] = [b't', b'u', b'm'];
static S_20_14: [symbol; 4] = [b'd', 0xC4, 0xB1, b'm'];
static S_20_15: [symbol; 4] = [b't', 0xC4, 0xB1, b'm'];
static S_20_16: [symbol; 4] = [b'd', 0xC3, 0xBC, b'm'];
static S_20_17: [symbol; 4] = [b't', 0xC3, 0xBC, b'm'];
static S_20_18: [symbol; 3] = [b'd', b'i', b'n'];
static S_20_19: [symbol; 3] = [b't', b'i', b'n'];
static S_20_20: [symbol; 3] = [b'd', b'u', b'n'];
static S_20_21: [symbol; 3] = [b't', b'u', b'n'];
static S_20_22: [symbol; 4] = [b'd', 0xC4, 0xB1, b'n'];
static S_20_23: [symbol; 4] = [b't', 0xC4, 0xB1, b'n'];
static S_20_24: [symbol; 4] = [b'd', 0xC3, 0xBC, b'n'];
static S_20_25: [symbol; 4] = [b't', 0xC3, 0xBC, b'n'];
static S_20_26: [symbol; 2] = [b'd', b'u'];
static S_20_27: [symbol; 2] = [b't', b'u'];
static S_20_28: [symbol; 3] = [b'd', 0xC4, 0xB1];
static S_20_29: [symbol; 3] = [b't', 0xC4, 0xB1];
static S_20_30: [symbol; 3] = [b'd', 0xC3, 0xBC];
static S_20_31: [symbol; 3] = [b't', 0xC3, 0xBC];

static A_20: [among; 32] = [
    among { s_size: 2, s: S_20_0.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_20_1.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_20_2.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_20_3.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_20_4.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_20_5.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 4, s: S_20_6.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 4, s: S_20_7.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 4, s: S_20_8.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 4, s: S_20_9.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_20_10.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_20_11.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_20_12.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_20_13.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 4, s: S_20_14.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 4, s: S_20_15.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 4, s: S_20_16.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 4, s: S_20_17.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_20_18.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_20_19.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_20_20.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_20_21.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 4, s: S_20_22.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 4, s: S_20_23.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 4, s: S_20_24.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 4, s: S_20_25.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_20_26.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_20_27.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_20_28.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_20_29.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_20_30.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_20_31.as_ptr(), substring_i: -1, result: -1, function: None },
];

static S_21_0: [symbol; 2] = [b's', b'a'];
static S_21_1: [symbol; 2] = [b's', b'e'];
static S_21_2: [symbol; 3] = [b's', b'a', b'k'];
static S_21_3: [symbol; 3] = [b's', b'e', b'k'];
static S_21_4: [symbol; 3] = [b's', b'a', b'm'];
static S_21_5: [symbol; 3] = [b's', b'e', b'm'];
static S_21_6: [symbol; 3] = [b's', b'a', b'n'];
static S_21_7: [symbol; 3] = [b's', b'e', b'n'];

static A_21: [among; 8] = [
    among { s_size: 2, s: S_21_0.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_21_1.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_21_2.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_21_3.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_21_4.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_21_5.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_21_6.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_21_7.as_ptr(), substring_i: -1, result: -1, function: None },
];

static S_22_0: [symbol; 4] = [b'm', b'i', 0xC5, 0x9F];
static S_22_1: [symbol; 4] = [b'm', b'u', 0xC5, 0x9F];
static S_22_2: [symbol; 5] = [b'm', 0xC4, 0xB1, 0xC5, 0x9F];
static S_22_3: [symbol; 5] = [b'm', 0xC3, 0xBC, 0xC5, 0x9F];

static A_22: [among; 4] = [
    among { s_size: 4, s: S_22_0.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 4, s: S_22_1.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 5, s: S_22_2.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 5, s: S_22_3.as_ptr(), substring_i: -1, result: -1, function: None },
];

static S_23_0: [symbol; 1] = [b'b'];
static S_23_1: [symbol; 1] = [b'c'];
static S_23_2: [symbol; 1] = [b'd'];
static S_23_3: [symbol; 2] = [0xC4, 0x9F];

static A_23: [among; 4] = [
    among { s_size: 1, s: S_23_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 1, s: S_23_1.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 1, s: S_23_2.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 2, s: S_23_3.as_ptr(), substring_i: -1, result: 4, function: None },
];

// ---------------------------------------------------------------------------
// grouping bit tables
// ---------------------------------------------------------------------------

static G_VOWEL: [c_uchar; 27] = [
    17, 65, 16, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 32, 8, 0, 0, 0, 0, 0, 0, 1,
];

static G_U: [c_uchar; 26] = [
    1, 16, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 1,
];

static G_VOWEL1: [c_uchar; 27] = [
    1, 64, 16, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1,
];

static G_VOWEL2: [c_uchar; 19] = [
    17, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 130,
];

static G_VOWEL3: [c_uchar; 27] = [
    1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1,
];

static G_VOWEL4: [c_uchar; 1] = [17];

static G_VOWEL5: [c_uchar; 1] = [65];

static G_VOWEL6: [c_uchar; 1] = [65];

// ---------------------------------------------------------------------------
// literal strings used by slice_from_s / eq_s_b
// ---------------------------------------------------------------------------

static S_0: [symbol; 2] = [0xC4, 0xB1];
static S_1: [symbol; 2] = [0xC3, 0xB6];
static S_2: [symbol; 2] = [0xC3, 0xBC];
static S_3: [symbol; 2] = [b'k', b'i'];
static S_4: [symbol; 3] = [b'k', b'e', b'n'];
static S_5: [symbol; 1] = [b'p'];
static S_6: [symbol; 2] = [0xC3, 0xA7];
static S_7: [symbol; 1] = [b't'];
static S_8: [symbol; 1] = [b'k'];
static S_9: [symbol; 2] = [0xC4, 0xB1];
static S_10: [symbol; 2] = [0xC4, 0xB1];
static S_11: [symbol; 1] = [b'i'];
static S_12: [symbol; 1] = [b'u'];
static S_13: [symbol; 2] = [0xC3, 0xB6];
static S_14: [symbol; 2] = [0xC3, 0xBC];
static S_15: [symbol; 2] = [0xC3, 0xBC];
static S_16: [symbol; 2] = [b'a', b'd'];
static S_17: [symbol; 3] = [b's', b'o', b'y'];

// ---------------------------------------------------------------------------
// rules
// ---------------------------------------------------------------------------

unsafe fn r_check_vowel_harmony(z: *mut SN_env) -> c_int {
    {
        let m_test1 = (*z).l - (*z).c;

        if out_grouping_b_U(z, G_VOWEL.as_ptr(), 97, 305, 1) < 0 {
            return 0;
        }
        'lab0: {
            let m2 = (*z).l - (*z).c;
            'lab1: {
                if (*z).c <= (*z).lb || *(*z).p.offset(((*z).c - 1) as isize) != b'a' {
                    break 'lab1;
                }
                (*z).c -= 1;

                if out_grouping_b_U(z, G_VOWEL1.as_ptr(), 97, 305, 1) < 0 {
                    break 'lab1;
                }
                break 'lab0;
            }
            (*z).c = (*z).l - m2;
            'lab2: {
                if (*z).c <= (*z).lb || *(*z).p.offset(((*z).c - 1) as isize) != b'e' {
                    break 'lab2;
                }
                (*z).c -= 1;

                if out_grouping_b_U(z, G_VOWEL2.as_ptr(), 101, 252, 1) < 0 {
                    break 'lab2;
                }
                break 'lab0;
            }
            (*z).c = (*z).l - m2;
            'lab3: {
                if eq_s_b(z, 2, S_0.as_ptr()) == 0 {
                    break 'lab3;
                }

                if out_grouping_b_U(z, G_VOWEL3.as_ptr(), 97, 305, 1) < 0 {
                    break 'lab3;
                }
                break 'lab0;
            }
            (*z).c = (*z).l - m2;
            'lab4: {
                if (*z).c <= (*z).lb || *(*z).p.offset(((*z).c - 1) as isize) != b'i' {
                    break 'lab4;
                }
                (*z).c -= 1;

                if out_grouping_b_U(z, G_VOWEL4.as_ptr(), 101, 105, 1) < 0 {
                    break 'lab4;
                }
                break 'lab0;
            }
            (*z).c = (*z).l - m2;
            'lab5: {
                if (*z).c <= (*z).lb || *(*z).p.offset(((*z).c - 1) as isize) != b'o' {
                    break 'lab5;
                }
                (*z).c -= 1;

                if out_grouping_b_U(z, G_VOWEL5.as_ptr(), 111, 117, 1) < 0 {
                    break 'lab5;
                }
                break 'lab0;
            }
            (*z).c = (*z).l - m2;
            'lab6: {
                if eq_s_b(z, 2, S_1.as_ptr()) == 0 {
                    break 'lab6;
                }

                if out_grouping_b_U(z, G_VOWEL6.as_ptr(), 246, 252, 1) < 0 {
                    break 'lab6;
                }
                break 'lab0;
            }
            (*z).c = (*z).l - m2;
            'lab7: {
                if (*z).c <= (*z).lb || *(*z).p.offset(((*z).c - 1) as isize) != b'u' {
                    break 'lab7;
                }
                (*z).c -= 1;

                if out_grouping_b_U(z, G_VOWEL5.as_ptr(), 111, 117, 1) < 0 {
                    break 'lab7;
                }
                break 'lab0;
            }
            (*z).c = (*z).l - m2;
            if eq_s_b(z, 2, S_2.as_ptr()) == 0 {
                return 0;
            }

            if out_grouping_b_U(z, G_VOWEL6.as_ptr(), 246, 252, 1) < 0 {
                return 0;
            }
        }
        (*z).c = (*z).l - m_test1;
    }
    1
}

unsafe fn r_mark_suffix_with_optional_n_consonant(z: *mut SN_env) -> c_int {
    'lab0: {
        let m1 = (*z).l - (*z).c;
        'lab1: {
            if (*z).c <= (*z).lb || *(*z).p.offset(((*z).c - 1) as isize) != b'n' {
                break 'lab1;
            }
            (*z).c -= 1;
            {
                let m_test2 = (*z).l - (*z).c;
                if in_grouping_b_U(z, G_VOWEL.as_ptr(), 97, 305, 0) != 0 {
                    break 'lab1;
                }
                (*z).c = (*z).l - m_test2;
            }
            break 'lab0;
        }
        (*z).c = (*z).l - m1;
        {
            let m3 = (*z).l - (*z).c;
            'lab2: {
                {
                    let m_test4 = (*z).l - (*z).c;
                    if (*z).c <= (*z).lb || *(*z).p.offset(((*z).c - 1) as isize) != b'n' {
                        break 'lab2;
                    }
                    (*z).c -= 1;
                    (*z).c = (*z).l - m_test4;
                }
                return 0;
            }
            (*z).c = (*z).l - m3;
        }
        {
            let m_test5 = (*z).l - (*z).c;
            {
                let ret = skip_b_utf8((*z).p, (*z).c, (*z).lb, 1);
                if ret < 0 {
                    return 0;
                }
                (*z).c = ret;
            }
            if in_grouping_b_U(z, G_VOWEL.as_ptr(), 97, 305, 0) != 0 {
                return 0;
            }
            (*z).c = (*z).l - m_test5;
        }
    }
    1
}

unsafe fn r_mark_suffix_with_optional_s_consonant(z: *mut SN_env) -> c_int {
    'lab0: {
        let m1 = (*z).l - (*z).c;
        'lab1: {
            if (*z).c <= (*z).lb || *(*z).p.offset(((*z).c - 1) as isize) != b's' {
                break 'lab1;
            }
            (*z).c -= 1;
            {
                let m_test2 = (*z).l - (*z).c;
                if in_grouping_b_U(z, G_VOWEL.as_ptr(), 97, 305, 0) != 0 {
                    break 'lab1;
                }
                (*z).c = (*z).l - m_test2;
            }
            break 'lab0;
        }
        (*z).c = (*z).l - m1;
        {
            let m3 = (*z).l - (*z).c;
            'lab2: {
                {
                    let m_test4 = (*z).l - (*z).c;
                    if (*z).c <= (*z).lb || *(*z).p.offset(((*z).c - 1) as isize) != b's' {
                        break 'lab2;
                    }
                    (*z).c -= 1;
                    (*z).c = (*z).l - m_test4;
                }
                return 0;
            }
            (*z).c = (*z).l - m3;
        }
        {
            let m_test5 = (*z).l - (*z).c;
            {
                let ret = skip_b_utf8((*z).p, (*z).c, (*z).lb, 1);
                if ret < 0 {
                    return 0;
                }
                (*z).c = ret;
            }
            if in_grouping_b_U(z, G_VOWEL.as_ptr(), 97, 305, 0) != 0 {
                return 0;
            }
            (*z).c = (*z).l - m_test5;
        }
    }
    1
}

unsafe fn r_mark_suffix_with_optional_y_consonant(z: *mut SN_env) -> c_int {
    'lab0: {
        let m1 = (*z).l - (*z).c;
        'lab1: {
            if (*z).c <= (*z).lb || *(*z).p.offset(((*z).c - 1) as isize) != b'y' {
                break 'lab1;
            }
            (*z).c -= 1;
            {
                let m_test2 = (*z).l - (*z).c;
                if in_grouping_b_U(z, G_VOWEL.as_ptr(), 97, 305, 0) != 0 {
                    break 'lab1;
                }
                (*z).c = (*z).l - m_test2;
            }
            break 'lab0;
        }
        (*z).c = (*z).l - m1;
        {
            let m3 = (*z).l - (*z).c;
            'lab2: {
                {
                    let m_test4 = (*z).l - (*z).c;
                    if (*z).c <= (*z).lb || *(*z).p.offset(((*z).c - 1) as isize) != b'y' {
                        break 'lab2;
                    }
                    (*z).c -= 1;
                    (*z).c = (*z).l - m_test4;
                }
                return 0;
            }
            (*z).c = (*z).l - m3;
        }
        {
            let m_test5 = (*z).l - (*z).c;
            {
                let ret = skip_b_utf8((*z).p, (*z).c, (*z).lb, 1);
                if ret < 0 {
                    return 0;
                }
                (*z).c = ret;
            }
            if in_grouping_b_U(z, G_VOWEL.as_ptr(), 97, 305, 0) != 0 {
                return 0;
            }
            (*z).c = (*z).l - m_test5;
        }
    }
    1
}

unsafe fn r_mark_suffix_with_optional_U_vowel(z: *mut SN_env) -> c_int {
    'lab0: {
        let m1 = (*z).l - (*z).c;
        'lab1: {
            if in_grouping_b_U(z, G_U.as_ptr(), 105, 305, 0) != 0 {
                break 'lab1;
            }
            {
                let m_test2 = (*z).l - (*z).c;
                if out_grouping_b_U(z, G_VOWEL.as_ptr(), 97, 305, 0) != 0 {
                    break 'lab1;
                }
                (*z).c = (*z).l - m_test2;
            }
            break 'lab0;
        }
        (*z).c = (*z).l - m1;
        {
            let m3 = (*z).l - (*z).c;
            'lab2: {
                {
                    let m_test4 = (*z).l - (*z).c;
                    if in_grouping_b_U(z, G_U.as_ptr(), 105, 305, 0) != 0 {
                        break 'lab2;
                    }
                    (*z).c = (*z).l - m_test4;
                }
                return 0;
            }
            (*z).c = (*z).l - m3;
        }
        {
            let m_test5 = (*z).l - (*z).c;
            {
                let ret = skip_b_utf8((*z).p, (*z).c, (*z).lb, 1);
                if ret < 0 {
                    return 0;
                }
                (*z).c = ret;
            }
            if out_grouping_b_U(z, G_VOWEL.as_ptr(), 97, 305, 0) != 0 {
                return 0;
            }
            (*z).c = (*z).l - m_test5;
        }
    }
    1
}

unsafe fn r_mark_possessives(z: *mut SN_env) -> c_int {
    if (*z).c <= (*z).lb
        || *(*z).p.offset(((*z).c - 1) as isize) as c_int >> 5 != 3
        || (67133440 >> (*(*z).p.offset(((*z).c - 1) as isize) as c_int & 0x1f)) & 1 == 0
    {
        return 0;
    }
    if find_among_b(z, A_0.as_ptr(), 10) == 0 {
        return 0;
    }
    {
        let ret = r_mark_suffix_with_optional_U_vowel(z);
        if ret <= 0 {
            return ret;
        }
    }
    1
}

unsafe fn r_mark_sU(z: *mut SN_env) -> c_int {
    {
        let ret = r_check_vowel_harmony(z);
        if ret <= 0 {
            return ret;
        }
    }
    if in_grouping_b_U(z, G_U.as_ptr(), 105, 305, 0) != 0 {
        return 0;
    }
    {
        let ret = r_mark_suffix_with_optional_s_consonant(z);
        if ret <= 0 {
            return ret;
        }
    }
    1
}

unsafe fn r_mark_lArI(z: *mut SN_env) -> c_int {
    if (*z).c - 3 <= (*z).lb
        || (*(*z).p.offset(((*z).c - 1) as isize) != 105 && *(*z).p.offset(((*z).c - 1) as isize) != 177)
    {
        return 0;
    }
    if find_among_b(z, A_1.as_ptr(), 2) == 0 {
        return 0;
    }
    1
}

unsafe fn r_mark_yU(z: *mut SN_env) -> c_int {
    {
        let ret = r_check_vowel_harmony(z);
        if ret <= 0 {
            return ret;
        }
    }
    if in_grouping_b_U(z, G_U.as_ptr(), 105, 305, 0) != 0 {
        return 0;
    }
    {
        let ret = r_mark_suffix_with_optional_y_consonant(z);
        if ret <= 0 {
            return ret;
        }
    }
    1
}

unsafe fn r_mark_nU(z: *mut SN_env) -> c_int {
    {
        let ret = r_check_vowel_harmony(z);
        if ret <= 0 {
            return ret;
        }
    }
    if find_among_b(z, A_2.as_ptr(), 4) == 0 {
        return 0;
    }
    1
}

unsafe fn r_mark_nUn(z: *mut SN_env) -> c_int {
    {
        let ret = r_check_vowel_harmony(z);
        if ret <= 0 {
            return ret;
        }
    }
    if (*z).c - 1 <= (*z).lb || *(*z).p.offset(((*z).c - 1) as isize) != 110 {
        return 0;
    }
    if find_among_b(z, A_3.as_ptr(), 4) == 0 {
        return 0;
    }
    {
        let ret = r_mark_suffix_with_optional_n_consonant(z);
        if ret <= 0 {
            return ret;
        }
    }
    1
}

unsafe fn r_mark_yA(z: *mut SN_env) -> c_int {
    {
        let ret = r_check_vowel_harmony(z);
        if ret <= 0 {
            return ret;
        }
    }
    if (*z).c <= (*z).lb
        || (*(*z).p.offset(((*z).c - 1) as isize) != 97 && *(*z).p.offset(((*z).c - 1) as isize) != 101)
    {
        return 0;
    }
    if find_among_b(z, A_4.as_ptr(), 2) == 0 {
        return 0;
    }
    {
        let ret = r_mark_suffix_with_optional_y_consonant(z);
        if ret <= 0 {
            return ret;
        }
    }
    1
}

unsafe fn r_mark_nA(z: *mut SN_env) -> c_int {
    {
        let ret = r_check_vowel_harmony(z);
        if ret <= 0 {
            return ret;
        }
    }
    if (*z).c - 1 <= (*z).lb
        || (*(*z).p.offset(((*z).c - 1) as isize) != 97 && *(*z).p.offset(((*z).c - 1) as isize) != 101)
    {
        return 0;
    }
    if find_among_b(z, A_5.as_ptr(), 2) == 0 {
        return 0;
    }
    1
}

unsafe fn r_mark_DA(z: *mut SN_env) -> c_int {
    {
        let ret = r_check_vowel_harmony(z);
        if ret <= 0 {
            return ret;
        }
    }
    if (*z).c - 1 <= (*z).lb
        || (*(*z).p.offset(((*z).c - 1) as isize) != 97 && *(*z).p.offset(((*z).c - 1) as isize) != 101)
    {
        return 0;
    }
    if find_among_b(z, A_6.as_ptr(), 4) == 0 {
        return 0;
    }
    1
}

unsafe fn r_mark_ndA(z: *mut SN_env) -> c_int {
    {
        let ret = r_check_vowel_harmony(z);
        if ret <= 0 {
            return ret;
        }
    }
    if (*z).c - 2 <= (*z).lb
        || (*(*z).p.offset(((*z).c - 1) as isize) != 97 && *(*z).p.offset(((*z).c - 1) as isize) != 101)
    {
        return 0;
    }
    if find_among_b(z, A_7.as_ptr(), 2) == 0 {
        return 0;
    }
    1
}

unsafe fn r_mark_DAn(z: *mut SN_env) -> c_int {
    {
        let ret = r_check_vowel_harmony(z);
        if ret <= 0 {
            return ret;
        }
    }
    if (*z).c - 2 <= (*z).lb || *(*z).p.offset(((*z).c - 1) as isize) != 110 {
        return 0;
    }
    if find_among_b(z, A_8.as_ptr(), 4) == 0 {
        return 0;
    }
    1
}

unsafe fn r_mark_ndAn(z: *mut SN_env) -> c_int {
    {
        let ret = r_check_vowel_harmony(z);
        if ret <= 0 {
            return ret;
        }
    }
    if (*z).c - 3 <= (*z).lb || *(*z).p.offset(((*z).c - 1) as isize) != 110 {
        return 0;
    }
    if find_among_b(z, A_9.as_ptr(), 2) == 0 {
        return 0;
    }
    1
}

unsafe fn r_mark_ylA(z: *mut SN_env) -> c_int {
    {
        let ret = r_check_vowel_harmony(z);
        if ret <= 0 {
            return ret;
        }
    }
    if (*z).c - 1 <= (*z).lb
        || (*(*z).p.offset(((*z).c - 1) as isize) != 97 && *(*z).p.offset(((*z).c - 1) as isize) != 101)
    {
        return 0;
    }
    if find_among_b(z, A_10.as_ptr(), 2) == 0 {
        return 0;
    }
    {
        let ret = r_mark_suffix_with_optional_y_consonant(z);
        if ret <= 0 {
            return ret;
        }
    }
    1
}

unsafe fn r_mark_ki(z: *mut SN_env) -> c_int {
    if eq_s_b(z, 2, S_3.as_ptr()) == 0 {
        return 0;
    }
    1
}

unsafe fn r_mark_ncA(z: *mut SN_env) -> c_int {
    {
        let ret = r_check_vowel_harmony(z);
        if ret <= 0 {
            return ret;
        }
    }
    if (*z).c - 1 <= (*z).lb
        || (*(*z).p.offset(((*z).c - 1) as isize) != 97 && *(*z).p.offset(((*z).c - 1) as isize) != 101)
    {
        return 0;
    }
    if find_among_b(z, A_11.as_ptr(), 2) == 0 {
        return 0;
    }
    {
        let ret = r_mark_suffix_with_optional_n_consonant(z);
        if ret <= 0 {
            return ret;
        }
    }
    1
}

unsafe fn r_mark_yUm(z: *mut SN_env) -> c_int {
    {
        let ret = r_check_vowel_harmony(z);
        if ret <= 0 {
            return ret;
        }
    }
    if (*z).c - 1 <= (*z).lb || *(*z).p.offset(((*z).c - 1) as isize) != 109 {
        return 0;
    }
    if find_among_b(z, A_12.as_ptr(), 4) == 0 {
        return 0;
    }
    {
        let ret = r_mark_suffix_with_optional_y_consonant(z);
        if ret <= 0 {
            return ret;
        }
    }
    1
}

unsafe fn r_mark_sUn(z: *mut SN_env) -> c_int {
    {
        let ret = r_check_vowel_harmony(z);
        if ret <= 0 {
            return ret;
        }
    }
    if (*z).c - 2 <= (*z).lb || *(*z).p.offset(((*z).c - 1) as isize) != 110 {
        return 0;
    }
    if find_among_b(z, A_13.as_ptr(), 4) == 0 {
        return 0;
    }
    1
}

unsafe fn r_mark_yUz(z: *mut SN_env) -> c_int {
    {
        let ret = r_check_vowel_harmony(z);
        if ret <= 0 {
            return ret;
        }
    }
    if (*z).c - 1 <= (*z).lb || *(*z).p.offset(((*z).c - 1) as isize) != 122 {
        return 0;
    }
    if find_among_b(z, A_14.as_ptr(), 4) == 0 {
        return 0;
    }
    {
        let ret = r_mark_suffix_with_optional_y_consonant(z);
        if ret <= 0 {
            return ret;
        }
    }
    1
}

unsafe fn r_mark_sUnUz(z: *mut SN_env) -> c_int {
    if (*z).c - 4 <= (*z).lb || *(*z).p.offset(((*z).c - 1) as isize) != 122 {
        return 0;
    }
    if find_among_b(z, A_15.as_ptr(), 4) == 0 {
        return 0;
    }
    1
}

unsafe fn r_mark_lAr(z: *mut SN_env) -> c_int {
    {
        let ret = r_check_vowel_harmony(z);
        if ret <= 0 {
            return ret;
        }
    }
    if (*z).c - 2 <= (*z).lb || *(*z).p.offset(((*z).c - 1) as isize) != 114 {
        return 0;
    }
    if find_among_b(z, A_16.as_ptr(), 2) == 0 {
        return 0;
    }
    1
}

unsafe fn r_mark_nUz(z: *mut SN_env) -> c_int {
    {
        let ret = r_check_vowel_harmony(z);
        if ret <= 0 {
            return ret;
        }
    }
    if (*z).c - 2 <= (*z).lb || *(*z).p.offset(((*z).c - 1) as isize) != 122 {
        return 0;
    }
    if find_among_b(z, A_17.as_ptr(), 4) == 0 {
        return 0;
    }
    1
}

unsafe fn r_mark_DUr(z: *mut SN_env) -> c_int {
    {
        let ret = r_check_vowel_harmony(z);
        if ret <= 0 {
            return ret;
        }
    }
    if (*z).c - 2 <= (*z).lb || *(*z).p.offset(((*z).c - 1) as isize) != 114 {
        return 0;
    }
    if find_among_b(z, A_18.as_ptr(), 8) == 0 {
        return 0;
    }
    1
}

unsafe fn r_mark_cAsInA(z: *mut SN_env) -> c_int {
    if (*z).c - 5 <= (*z).lb
        || (*(*z).p.offset(((*z).c - 1) as isize) != 97 && *(*z).p.offset(((*z).c - 1) as isize) != 101)
    {
        return 0;
    }
    if find_among_b(z, A_19.as_ptr(), 2) == 0 {
        return 0;
    }
    1
}

unsafe fn r_mark_yDU(z: *mut SN_env) -> c_int {
    {
        let ret = r_check_vowel_harmony(z);
        if ret <= 0 {
            return ret;
        }
    }
    if find_among_b(z, A_20.as_ptr(), 32) == 0 {
        return 0;
    }
    {
        let ret = r_mark_suffix_with_optional_y_consonant(z);
        if ret <= 0 {
            return ret;
        }
    }
    1
}

unsafe fn r_mark_ysA(z: *mut SN_env) -> c_int {
    if (*z).c - 1 <= (*z).lb
        || *(*z).p.offset(((*z).c - 1) as isize) as c_int >> 5 != 3
        || (26658 >> (*(*z).p.offset(((*z).c - 1) as isize) as c_int & 0x1f)) & 1 == 0
    {
        return 0;
    }
    if find_among_b(z, A_21.as_ptr(), 8) == 0 {
        return 0;
    }
    {
        let ret = r_mark_suffix_with_optional_y_consonant(z);
        if ret <= 0 {
            return ret;
        }
    }
    1
}

unsafe fn r_mark_ymUs_(z: *mut SN_env) -> c_int {
    {
        let ret = r_check_vowel_harmony(z);
        if ret <= 0 {
            return ret;
        }
    }
    if (*z).c - 3 <= (*z).lb || *(*z).p.offset(((*z).c - 1) as isize) != 159 {
        return 0;
    }
    if find_among_b(z, A_22.as_ptr(), 4) == 0 {
        return 0;
    }
    {
        let ret = r_mark_suffix_with_optional_y_consonant(z);
        if ret <= 0 {
            return ret;
        }
    }
    1
}

unsafe fn r_mark_yken(z: *mut SN_env) -> c_int {
    if eq_s_b(z, 3, S_4.as_ptr()) == 0 {
        return 0;
    }
    {
        let ret = r_mark_suffix_with_optional_y_consonant(z);
        if ret <= 0 {
            return ret;
        }
    }
    1
}

unsafe fn r_stem_nominal_verb_suffixes(z: *mut SN_env) -> c_int {
    (*z).ket = (*z).c;
    *(*z).I.offset(0) = 1;
    'lab0: {
        let m1 = (*z).l - (*z).c;
        'lab1: {
            'lab2: {
                let m2 = (*z).l - (*z).c;
                'lab3: {
                    {
                        let ret = r_mark_ymUs_(z);
                        if ret == 0 {
                            break 'lab3;
                        }
                        if ret < 0 {
                            return ret;
                        }
                    }
                    break 'lab2;
                }
                (*z).c = (*z).l - m2;
                'lab4: {
                    {
                        let ret = r_mark_yDU(z);
                        if ret == 0 {
                            break 'lab4;
                        }
                        if ret < 0 {
                            return ret;
                        }
                    }
                    break 'lab2;
                }
                (*z).c = (*z).l - m2;
                'lab5: {
                    {
                        let ret = r_mark_ysA(z);
                        if ret == 0 {
                            break 'lab5;
                        }
                        if ret < 0 {
                            return ret;
                        }
                    }
                    break 'lab2;
                }
                (*z).c = (*z).l - m2;
                {
                    let ret = r_mark_yken(z);
                    if ret == 0 {
                        break 'lab1;
                    }
                    if ret < 0 {
                        return ret;
                    }
                }
            }
            break 'lab0;
        }
        (*z).c = (*z).l - m1;
        'lab6: {
            {
                let ret = r_mark_cAsInA(z);
                if ret == 0 {
                    break 'lab6;
                }
                if ret < 0 {
                    return ret;
                }
            }
            {
                let m3 = (*z).l - (*z).c;
                'lab7: {
                    'lab8: {
                        {
                            let ret = r_mark_sUnUz(z);
                            if ret == 0 {
                                break 'lab8;
                            }
                            if ret < 0 {
                                return ret;
                            }
                        }
                        break 'lab7;
                    }
                    (*z).c = (*z).l - m3;
                    'lab9: {
                        {
                            let ret = r_mark_lAr(z);
                            if ret == 0 {
                                break 'lab9;
                            }
                            if ret < 0 {
                                return ret;
                            }
                        }
                        break 'lab7;
                    }
                    (*z).c = (*z).l - m3;
                    'lab10: {
                        {
                            let ret = r_mark_yUm(z);
                            if ret == 0 {
                                break 'lab10;
                            }
                            if ret < 0 {
                                return ret;
                            }
                        }
                        break 'lab7;
                    }
                    (*z).c = (*z).l - m3;
                    'lab11: {
                        {
                            let ret = r_mark_sUn(z);
                            if ret == 0 {
                                break 'lab11;
                            }
                            if ret < 0 {
                                return ret;
                            }
                        }
                        break 'lab7;
                    }
                    (*z).c = (*z).l - m3;
                    'lab12: {
                        {
                            let ret = r_mark_yUz(z);
                            if ret == 0 {
                                break 'lab12;
                            }
                            if ret < 0 {
                                return ret;
                            }
                        }
                        break 'lab7;
                    }
                    (*z).c = (*z).l - m3;
                }
            }
            {
                let ret = r_mark_ymUs_(z);
                if ret == 0 {
                    break 'lab6;
                }
                if ret < 0 {
                    return ret;
                }
            }
            break 'lab0;
        }
        (*z).c = (*z).l - m1;
        'lab13: {
            {
                let ret = r_mark_lAr(z);
                if ret == 0 {
                    break 'lab13;
                }
                if ret < 0 {
                    return ret;
                }
            }
            (*z).bra = (*z).c;
            {
                let ret = slice_del(z);
                if ret < 0 {
                    return ret;
                }
            }
            {
                let m4 = (*z).l - (*z).c;
                'lab14: {
                    (*z).ket = (*z).c;
                    'lab15: {
                        let m5 = (*z).l - (*z).c;
                        'lab16: {
                            {
                                let ret = r_mark_DUr(z);
                                if ret == 0 {
                                    break 'lab16;
                                }
                                if ret < 0 {
                                    return ret;
                                }
                            }
                            break 'lab15;
                        }
                        (*z).c = (*z).l - m5;
                        'lab17: {
                            {
                                let ret = r_mark_yDU(z);
                                if ret == 0 {
                                    break 'lab17;
                                }
                                if ret < 0 {
                                    return ret;
                                }
                            }
                            break 'lab15;
                        }
                        (*z).c = (*z).l - m5;
                        'lab18: {
                            {
                                let ret = r_mark_ysA(z);
                                if ret == 0 {
                                    break 'lab18;
                                }
                                if ret < 0 {
                                    return ret;
                                }
                            }
                            break 'lab15;
                        }
                        (*z).c = (*z).l - m5;
                        {
                            let ret = r_mark_ymUs_(z);
                            if ret == 0 {
                                (*z).c = (*z).l - m4;
                                break 'lab14;
                            }
                            if ret < 0 {
                                return ret;
                            }
                        }
                    }
                }
            }
            *(*z).I.offset(0) = 0;
            break 'lab0;
        }
        (*z).c = (*z).l - m1;
        'lab19: {
            {
                let ret = r_mark_nUz(z);
                if ret == 0 {
                    break 'lab19;
                }
                if ret < 0 {
                    return ret;
                }
            }
            {
                let m6 = (*z).l - (*z).c;
                'lab20: {
                    'lab21: {
                        {
                            let ret = r_mark_yDU(z);
                            if ret == 0 {
                                break 'lab21;
                            }
                            if ret < 0 {
                                return ret;
                            }
                        }
                        break 'lab20;
                    }
                    (*z).c = (*z).l - m6;
                    {
                        let ret = r_mark_ysA(z);
                        if ret == 0 {
                            break 'lab19;
                        }
                        if ret < 0 {
                            return ret;
                        }
                    }
                }
            }
            break 'lab0;
        }
        (*z).c = (*z).l - m1;
        'lab22: {
            {
                let m7 = (*z).l - (*z).c;
                'lab23: {
                    'lab24: {
                        {
                            let ret = r_mark_sUnUz(z);
                            if ret == 0 {
                                break 'lab24;
                            }
                            if ret < 0 {
                                return ret;
                            }
                        }
                        break 'lab23;
                    }
                    (*z).c = (*z).l - m7;
                    'lab25: {
                        {
                            let ret = r_mark_yUz(z);
                            if ret == 0 {
                                break 'lab25;
                            }
                            if ret < 0 {
                                return ret;
                            }
                        }
                        break 'lab23;
                    }
                    (*z).c = (*z).l - m7;
                    'lab26: {
                        {
                            let ret = r_mark_sUn(z);
                            if ret == 0 {
                                break 'lab26;
                            }
                            if ret < 0 {
                                return ret;
                            }
                        }
                        break 'lab23;
                    }
                    (*z).c = (*z).l - m7;
                    {
                        let ret = r_mark_yUm(z);
                        if ret == 0 {
                            break 'lab22;
                        }
                        if ret < 0 {
                            return ret;
                        }
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
            {
                let m8 = (*z).l - (*z).c;
                'lab27: {
                    (*z).ket = (*z).c;
                    {
                        let ret = r_mark_ymUs_(z);
                        if ret == 0 {
                            (*z).c = (*z).l - m8;
                            break 'lab27;
                        }
                        if ret < 0 {
                            return ret;
                        }
                    }
                }
            }
            break 'lab0;
        }
        (*z).c = (*z).l - m1;
        {
            let ret = r_mark_DUr(z);
            if ret <= 0 {
                return ret;
            }
        }
        (*z).bra = (*z).c;
        {
            let ret = slice_del(z);
            if ret < 0 {
                return ret;
            }
        }
        {
            let m9 = (*z).l - (*z).c;
            'lab28: {
                (*z).ket = (*z).c;
                'lab29: {
                    let m10 = (*z).l - (*z).c;
                    'lab30: {
                        {
                            let ret = r_mark_sUnUz(z);
                            if ret == 0 {
                                break 'lab30;
                            }
                            if ret < 0 {
                                return ret;
                            }
                        }
                        break 'lab29;
                    }
                    (*z).c = (*z).l - m10;
                    'lab31: {
                        {
                            let ret = r_mark_lAr(z);
                            if ret == 0 {
                                break 'lab31;
                            }
                            if ret < 0 {
                                return ret;
                            }
                        }
                        break 'lab29;
                    }
                    (*z).c = (*z).l - m10;
                    'lab32: {
                        {
                            let ret = r_mark_yUm(z);
                            if ret == 0 {
                                break 'lab32;
                            }
                            if ret < 0 {
                                return ret;
                            }
                        }
                        break 'lab29;
                    }
                    (*z).c = (*z).l - m10;
                    'lab33: {
                        {
                            let ret = r_mark_sUn(z);
                            if ret == 0 {
                                break 'lab33;
                            }
                            if ret < 0 {
                                return ret;
                            }
                        }
                        break 'lab29;
                    }
                    (*z).c = (*z).l - m10;
                    'lab34: {
                        {
                            let ret = r_mark_yUz(z);
                            if ret == 0 {
                                break 'lab34;
                            }
                            if ret < 0 {
                                return ret;
                            }
                        }
                        break 'lab29;
                    }
                    (*z).c = (*z).l - m10;
                }
                {
                    let ret = r_mark_ymUs_(z);
                    if ret == 0 {
                        (*z).c = (*z).l - m9;
                        break 'lab28;
                    }
                    if ret < 0 {
                        return ret;
                    }
                }
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
    1
}

unsafe fn r_stem_suffix_chain_before_ki(z: *mut SN_env) -> c_int {
    (*z).ket = (*z).c;
    {
        let ret = r_mark_ki(z);
        if ret <= 0 {
            return ret;
        }
    }
    'lab0: {
        let m1 = (*z).l - (*z).c;
        'lab1: {
            {
                let ret = r_mark_DA(z);
                if ret == 0 {
                    break 'lab1;
                }
                if ret < 0 {
                    return ret;
                }
            }
            (*z).bra = (*z).c;
            {
                let ret = slice_del(z);
                if ret < 0 {
                    return ret;
                }
            }
            {
                let m2 = (*z).l - (*z).c;
                'lab2: {
                    (*z).ket = (*z).c;
                    'lab3: {
                        let m3 = (*z).l - (*z).c;
                        'lab4: {
                            {
                                let ret = r_mark_lAr(z);
                                if ret == 0 {
                                    break 'lab4;
                                }
                                if ret < 0 {
                                    return ret;
                                }
                            }
                            (*z).bra = (*z).c;
                            {
                                let ret = slice_del(z);
                                if ret < 0 {
                                    return ret;
                                }
                            }
                            {
                                let m4 = (*z).l - (*z).c;
                                'lab5: {
                                    let ret = r_stem_suffix_chain_before_ki(z);
                                    if ret == 0 {
                                        (*z).c = (*z).l - m4;
                                        break 'lab5;
                                    }
                                    if ret < 0 {
                                        return ret;
                                    }
                                }
                            }
                            break 'lab3;
                        }
                        (*z).c = (*z).l - m3;
                        {
                            let ret = r_mark_possessives(z);
                            if ret == 0 {
                                (*z).c = (*z).l - m2;
                                break 'lab2;
                            }
                            if ret < 0 {
                                return ret;
                            }
                        }
                        (*z).bra = (*z).c;
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
                                {
                                    let ret = r_mark_lAr(z);
                                    if ret == 0 {
                                        (*z).c = (*z).l - m5;
                                        break 'lab6;
                                    }
                                    if ret < 0 {
                                        return ret;
                                    }
                                }
                                (*z).bra = (*z).c;
                                {
                                    let ret = slice_del(z);
                                    if ret < 0 {
                                        return ret;
                                    }
                                }
                                {
                                    let ret = r_stem_suffix_chain_before_ki(z);
                                    if ret == 0 {
                                        (*z).c = (*z).l - m5;
                                        break 'lab6;
                                    }
                                    if ret < 0 {
                                        return ret;
                                    }
                                }
                            }
                        }
                    }
                }
            }
            break 'lab0;
        }
        (*z).c = (*z).l - m1;
        'lab7: {
            {
                let ret = r_mark_nUn(z);
                if ret == 0 {
                    break 'lab7;
                }
                if ret < 0 {
                    return ret;
                }
            }
            (*z).bra = (*z).c;
            {
                let ret = slice_del(z);
                if ret < 0 {
                    return ret;
                }
            }
            {
                let m6 = (*z).l - (*z).c;
                'lab8: {
                    (*z).ket = (*z).c;
                    'lab9: {
                        let m7 = (*z).l - (*z).c;
                        'lab10: {
                            {
                                let ret = r_mark_lArI(z);
                                if ret == 0 {
                                    break 'lab10;
                                }
                                if ret < 0 {
                                    return ret;
                                }
                            }
                            (*z).bra = (*z).c;
                            {
                                let ret = slice_del(z);
                                if ret < 0 {
                                    return ret;
                                }
                            }
                            break 'lab9;
                        }
                        (*z).c = (*z).l - m7;
                        (*z).ket = (*z).c;
                        'lab11: {
                            'lab12: {
                                let m8 = (*z).l - (*z).c;
                                'lab13: {
                                    {
                                        let ret = r_mark_possessives(z);
                                        if ret == 0 {
                                            break 'lab13;
                                        }
                                        if ret < 0 {
                                            return ret;
                                        }
                                    }
                                    break 'lab12;
                                }
                                (*z).c = (*z).l - m8;
                                {
                                    let ret = r_mark_sU(z);
                                    if ret == 0 {
                                        break 'lab11;
                                    }
                                    if ret < 0 {
                                        return ret;
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
                            {
                                let m9 = (*z).l - (*z).c;
                                'lab14: {
                                    (*z).ket = (*z).c;
                                    {
                                        let ret = r_mark_lAr(z);
                                        if ret == 0 {
                                            (*z).c = (*z).l - m9;
                                            break 'lab14;
                                        }
                                        if ret < 0 {
                                            return ret;
                                        }
                                    }
                                    (*z).bra = (*z).c;
                                    {
                                        let ret = slice_del(z);
                                        if ret < 0 {
                                            return ret;
                                        }
                                    }
                                    {
                                        let ret = r_stem_suffix_chain_before_ki(z);
                                        if ret == 0 {
                                            (*z).c = (*z).l - m9;
                                            break 'lab14;
                                        }
                                        if ret < 0 {
                                            return ret;
                                        }
                                    }
                                }
                            }
                            break 'lab9;
                        }
                        (*z).c = (*z).l - m7;
                        {
                            let ret = r_stem_suffix_chain_before_ki(z);
                            if ret == 0 {
                                (*z).c = (*z).l - m6;
                                break 'lab8;
                            }
                            if ret < 0 {
                                return ret;
                            }
                        }
                    }
                }
            }
            break 'lab0;
        }
        (*z).c = (*z).l - m1;
        {
            let ret = r_mark_ndA(z);
            if ret <= 0 {
                return ret;
            }
        }
        {
            let m10 = (*z).l - (*z).c;
            'lab15: {
                'lab16: {
                    {
                        let ret = r_mark_lArI(z);
                        if ret == 0 {
                            break 'lab16;
                        }
                        if ret < 0 {
                            return ret;
                        }
                    }
                    (*z).bra = (*z).c;
                    {
                        let ret = slice_del(z);
                        if ret < 0 {
                            return ret;
                        }
                    }
                    break 'lab15;
                }
                (*z).c = (*z).l - m10;
                'lab17: {
                    {
                        let ret = r_mark_sU(z);
                        if ret == 0 {
                            break 'lab17;
                        }
                        if ret < 0 {
                            return ret;
                        }
                    }
                    (*z).bra = (*z).c;
                    {
                        let ret = slice_del(z);
                        if ret < 0 {
                            return ret;
                        }
                    }
                    {
                        let m11 = (*z).l - (*z).c;
                        'lab18: {
                            (*z).ket = (*z).c;
                            {
                                let ret = r_mark_lAr(z);
                                if ret == 0 {
                                    (*z).c = (*z).l - m11;
                                    break 'lab18;
                                }
                                if ret < 0 {
                                    return ret;
                                }
                            }
                            (*z).bra = (*z).c;
                            {
                                let ret = slice_del(z);
                                if ret < 0 {
                                    return ret;
                                }
                            }
                            {
                                let ret = r_stem_suffix_chain_before_ki(z);
                                if ret == 0 {
                                    (*z).c = (*z).l - m11;
                                    break 'lab18;
                                }
                                if ret < 0 {
                                    return ret;
                                }
                            }
                        }
                    }
                    break 'lab15;
                }
                (*z).c = (*z).l - m10;
                {
                    let ret = r_stem_suffix_chain_before_ki(z);
                    if ret <= 0 {
                        return ret;
                    }
                }
            }
        }
    }
    1
}

unsafe fn r_stem_noun_suffixes(z: *mut SN_env) -> c_int {
    'lab0: {
        let m1 = (*z).l - (*z).c;
        'lab1: {
            (*z).ket = (*z).c;
            {
                let ret = r_mark_lAr(z);
                if ret == 0 {
                    break 'lab1;
                }
                if ret < 0 {
                    return ret;
                }
            }
            (*z).bra = (*z).c;
            {
                let ret = slice_del(z);
                if ret < 0 {
                    return ret;
                }
            }
            {
                let m2 = (*z).l - (*z).c;
                'lab2: {
                    let ret = r_stem_suffix_chain_before_ki(z);
                    if ret == 0 {
                        (*z).c = (*z).l - m2;
                        break 'lab2;
                    }
                    if ret < 0 {
                        return ret;
                    }
                }
            }
            break 'lab0;
        }
        (*z).c = (*z).l - m1;
        'lab3: {
            (*z).ket = (*z).c;
            {
                let ret = r_mark_ncA(z);
                if ret == 0 {
                    break 'lab3;
                }
                if ret < 0 {
                    return ret;
                }
            }
            (*z).bra = (*z).c;
            {
                let ret = slice_del(z);
                if ret < 0 {
                    return ret;
                }
            }
            {
                let m3 = (*z).l - (*z).c;
                'lab4: {
                    let m4 = (*z).l - (*z).c;
                    'lab5: {
                        'lab6: {
                            (*z).ket = (*z).c;
                            {
                                let ret = r_mark_lArI(z);
                                if ret == 0 {
                                    break 'lab6;
                                }
                                if ret < 0 {
                                    return ret;
                                }
                            }
                            (*z).bra = (*z).c;
                            {
                                let ret = slice_del(z);
                                if ret < 0 {
                                    return ret;
                                }
                            }
                            break 'lab5;
                        }
                        (*z).c = (*z).l - m4;
                        (*z).ket = (*z).c;
                        'lab7: {
                            'lab8: {
                                let m5 = (*z).l - (*z).c;
                                'lab9: {
                                    {
                                        let ret = r_mark_possessives(z);
                                        if ret == 0 {
                                            break 'lab9;
                                        }
                                        if ret < 0 {
                                            return ret;
                                        }
                                    }
                                    break 'lab8;
                                }
                                (*z).c = (*z).l - m5;
                                {
                                    let ret = r_mark_sU(z);
                                    if ret == 0 {
                                        break 'lab7;
                                    }
                                    if ret < 0 {
                                        return ret;
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
                            {
                                let m6 = (*z).l - (*z).c;
                                'lab10: {
                                    (*z).ket = (*z).c;
                                    {
                                        let ret = r_mark_lAr(z);
                                        if ret == 0 {
                                            (*z).c = (*z).l - m6;
                                            break 'lab10;
                                        }
                                        if ret < 0 {
                                            return ret;
                                        }
                                    }
                                    (*z).bra = (*z).c;
                                    {
                                        let ret = slice_del(z);
                                        if ret < 0 {
                                            return ret;
                                        }
                                    }
                                    {
                                        let ret = r_stem_suffix_chain_before_ki(z);
                                        if ret == 0 {
                                            (*z).c = (*z).l - m6;
                                            break 'lab10;
                                        }
                                        if ret < 0 {
                                            return ret;
                                        }
                                    }
                                }
                            }
                            break 'lab5;
                        }
                        (*z).c = (*z).l - m4;
                        (*z).ket = (*z).c;
                        {
                            let ret = r_mark_lAr(z);
                            if ret == 0 {
                                (*z).c = (*z).l - m3;
                                break 'lab4;
                            }
                            if ret < 0 {
                                return ret;
                            }
                        }
                        (*z).bra = (*z).c;
                        {
                            let ret = slice_del(z);
                            if ret < 0 {
                                return ret;
                            }
                        }
                        {
                            let ret = r_stem_suffix_chain_before_ki(z);
                            if ret == 0 {
                                (*z).c = (*z).l - m3;
                                break 'lab4;
                            }
                            if ret < 0 {
                                return ret;
                            }
                        }
                    }
                }
            }
            break 'lab0;
        }
        (*z).c = (*z).l - m1;
        (*z).ket = (*z).c;
        'lab11: {
            'lab12: {
                let m7 = (*z).l - (*z).c;
                'lab13: {
                    {
                        let ret = r_mark_ndA(z);
                        if ret == 0 {
                            break 'lab13;
                        }
                        if ret < 0 {
                            return ret;
                        }
                    }
                    break 'lab12;
                }
                (*z).c = (*z).l - m7;
                {
                    let ret = r_mark_nA(z);
                    if ret == 0 {
                        break 'lab11;
                    }
                    if ret < 0 {
                        return ret;
                    }
                }
            }
            {
                let m8 = (*z).l - (*z).c;
                'lab14: {
                    'lab15: {
                        {
                            let ret = r_mark_lArI(z);
                            if ret == 0 {
                                break 'lab15;
                            }
                            if ret < 0 {
                                return ret;
                            }
                        }
                        (*z).bra = (*z).c;
                        {
                            let ret = slice_del(z);
                            if ret < 0 {
                                return ret;
                            }
                        }
                        break 'lab14;
                    }
                    (*z).c = (*z).l - m8;
                    'lab16: {
                        {
                            let ret = r_mark_sU(z);
                            if ret == 0 {
                                break 'lab16;
                            }
                            if ret < 0 {
                                return ret;
                            }
                        }
                        (*z).bra = (*z).c;
                        {
                            let ret = slice_del(z);
                            if ret < 0 {
                                return ret;
                            }
                        }
                        {
                            let m9 = (*z).l - (*z).c;
                            'lab17: {
                                (*z).ket = (*z).c;
                                {
                                    let ret = r_mark_lAr(z);
                                    if ret == 0 {
                                        (*z).c = (*z).l - m9;
                                        break 'lab17;
                                    }
                                    if ret < 0 {
                                        return ret;
                                    }
                                }
                                (*z).bra = (*z).c;
                                {
                                    let ret = slice_del(z);
                                    if ret < 0 {
                                        return ret;
                                    }
                                }
                                {
                                    let ret = r_stem_suffix_chain_before_ki(z);
                                    if ret == 0 {
                                        (*z).c = (*z).l - m9;
                                        break 'lab17;
                                    }
                                    if ret < 0 {
                                        return ret;
                                    }
                                }
                            }
                        }
                        break 'lab14;
                    }
                    (*z).c = (*z).l - m8;
                    {
                        let ret = r_mark_lArI(z);
                        if ret == 0 {
                            break 'lab11;
                        }
                        if ret < 0 {
                            return ret;
                        }
                    }
                }
            }
            break 'lab0;
        }
        (*z).c = (*z).l - m1;
        'lab18: {
            {
                let ret = r_stem_suffix_chain_before_ki(z);
                if ret == 0 {
                    break 'lab18;
                }
                if ret < 0 {
                    return ret;
                }
            }
            break 'lab0;
        }
        (*z).c = (*z).l - m1;
        (*z).ket = (*z).c;
        'lab19: {
            'lab20: {
                let m22 = (*z).l - (*z).c;
                'lab21: {
                    {
                        let ret = r_mark_DA(z);
                        if ret == 0 {
                            break 'lab21;
                        }
                        if ret < 0 {
                            return ret;
                        }
                    }
                    break 'lab20;
                }
                (*z).c = (*z).l - m22;
                'lab22: {
                    {
                        let ret = r_mark_yU(z);
                        if ret == 0 {
                            break 'lab22;
                        }
                        if ret < 0 {
                            return ret;
                        }
                    }
                    break 'lab20;
                }
                (*z).c = (*z).l - m22;
                {
                    let ret = r_mark_yA(z);
                    if ret == 0 {
                        break 'lab19;
                    }
                    if ret < 0 {
                        return ret;
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
            {
                let m23 = (*z).l - (*z).c;
                'lab23: {
                    (*z).ket = (*z).c;
                    'lab24: {
                        let m24 = (*z).l - (*z).c;
                        'lab25: {
                            {
                                let ret = r_mark_possessives(z);
                                if ret == 0 {
                                    break 'lab25;
                                }
                                if ret < 0 {
                                    return ret;
                                }
                            }
                            (*z).bra = (*z).c;
                            {
                                let ret = slice_del(z);
                                if ret < 0 {
                                    return ret;
                                }
                            }
                            {
                                let m25 = (*z).l - (*z).c;
                                'lab26: {
                                    (*z).ket = (*z).c;
                                    {
                                        let ret = r_mark_lAr(z);
                                        if ret == 0 {
                                            (*z).c = (*z).l - m25;
                                            break 'lab26;
                                        }
                                        if ret < 0 {
                                            return ret;
                                        }
                                    }
                                }
                            }
                            break 'lab24;
                        }
                        (*z).c = (*z).l - m24;
                        {
                            let ret = r_mark_lAr(z);
                            if ret == 0 {
                                (*z).c = (*z).l - m23;
                                break 'lab23;
                            }
                            if ret < 0 {
                                return ret;
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
                    (*z).ket = (*z).c;
                    {
                        let ret = r_stem_suffix_chain_before_ki(z);
                        if ret == 0 {
                            (*z).c = (*z).l - m23;
                            break 'lab23;
                        }
                        if ret < 0 {
                            return ret;
                        }
                    }
                }
            }
            break 'lab0;
        }
        (*z).c = (*z).l - m1;
        (*z).ket = (*z).c;
        'lab28: {
            let m26 = (*z).l - (*z).c;
            'lab29: {
                {
                    let ret = r_mark_possessives(z);
                    if ret == 0 {
                        break 'lab29;
                    }
                    if ret < 0 {
                        return ret;
                    }
                }
                break 'lab28;
            }
            (*z).c = (*z).l - m26;
            {
                let ret = r_mark_sU(z);
                if ret <= 0 {
                    return ret;
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
        {
            let m27 = (*z).l - (*z).c;
            'lab30: {
                (*z).ket = (*z).c;
                {
                    let ret = r_mark_lAr(z);
                    if ret == 0 {
                        (*z).c = (*z).l - m27;
                        break 'lab30;
                    }
                    if ret < 0 {
                        return ret;
                    }
                }
                (*z).bra = (*z).c;
                {
                    let ret = slice_del(z);
                    if ret < 0 {
                        return ret;
                    }
                }
                {
                    let ret = r_stem_suffix_chain_before_ki(z);
                    if ret == 0 {
                        (*z).c = (*z).l - m27;
                        break 'lab30;
                    }
                    if ret < 0 {
                        return ret;
                    }
                }
            }
        }
    }
    1
}

unsafe fn r_post_process_last_consonants(z: *mut SN_env) -> c_int {
    let among_var;
    (*z).ket = (*z).c;
    among_var = find_among_b(z, A_23.as_ptr(), 4);
    if among_var == 0 {
        return 0;
    }
    (*z).bra = (*z).c;
    match among_var {
        1 => {
            let ret = slice_from_s(z, 1, S_5.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        2 => {
            let ret = slice_from_s(z, 2, S_6.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        3 => {
            let ret = slice_from_s(z, 1, S_7.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        4 => {
            let ret = slice_from_s(z, 1, S_8.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        _ => {}
    }
    1
}

unsafe fn r_append_U_to_stems_ending_with_d_or_g(z: *mut SN_env) -> c_int {
    (*z).ket = (*z).c;
    (*z).bra = (*z).c;
    'lab0: {
        let m1 = (*z).l - (*z).c;
        'lab1: {
            if (*z).c <= (*z).lb || *(*z).p.offset(((*z).c - 1) as isize) != b'd' {
                break 'lab1;
            }
            (*z).c -= 1;
            break 'lab0;
        }
        (*z).c = (*z).l - m1;
        if (*z).c <= (*z).lb || *(*z).p.offset(((*z).c - 1) as isize) != b'g' {
            return 0;
        }
        (*z).c -= 1;
    }

    if out_grouping_b_U(z, G_VOWEL.as_ptr(), 97, 305, 1) < 0 {
        return 0;
    }
    'lab2: {
        let m2 = (*z).l - (*z).c;
        'lab3: {
            'lab4: {
                let m3 = (*z).l - (*z).c;
                'lab5: {
                    if (*z).c <= (*z).lb || *(*z).p.offset(((*z).c - 1) as isize) != b'a' {
                        break 'lab5;
                    }
                    (*z).c -= 1;
                    break 'lab4;
                }
                (*z).c = (*z).l - m3;
                if eq_s_b(z, 2, S_9.as_ptr()) == 0 {
                    break 'lab3;
                }
            }
            {
                let ret = slice_from_s(z, 2, S_10.as_ptr());
                if ret < 0 {
                    return ret;
                }
            }
            break 'lab2;
        }
        (*z).c = (*z).l - m2;
        'lab6: {
            'lab7: {
                let m4 = (*z).l - (*z).c;
                'lab8: {
                    if (*z).c <= (*z).lb || *(*z).p.offset(((*z).c - 1) as isize) != b'e' {
                        break 'lab8;
                    }
                    (*z).c -= 1;
                    break 'lab7;
                }
                (*z).c = (*z).l - m4;
                if (*z).c <= (*z).lb || *(*z).p.offset(((*z).c - 1) as isize) != b'i' {
                    break 'lab6;
                }
                (*z).c -= 1;
            }
            {
                let ret = slice_from_s(z, 1, S_11.as_ptr());
                if ret < 0 {
                    return ret;
                }
            }
            break 'lab2;
        }
        (*z).c = (*z).l - m2;
        'lab9: {
            'lab10: {
                let m5 = (*z).l - (*z).c;
                'lab11: {
                    if (*z).c <= (*z).lb || *(*z).p.offset(((*z).c - 1) as isize) != b'o' {
                        break 'lab11;
                    }
                    (*z).c -= 1;
                    break 'lab10;
                }
                (*z).c = (*z).l - m5;
                if (*z).c <= (*z).lb || *(*z).p.offset(((*z).c - 1) as isize) != b'u' {
                    break 'lab9;
                }
                (*z).c -= 1;
            }
            {
                let ret = slice_from_s(z, 1, S_12.as_ptr());
                if ret < 0 {
                    return ret;
                }
            }
            break 'lab2;
        }
        (*z).c = (*z).l - m2;
        {
            let m6 = (*z).l - (*z).c;
            'lab12: {
                if eq_s_b(z, 2, S_13.as_ptr()) != 0 {
                    break 'lab12;
                }
                (*z).c = (*z).l - m6;
                if eq_s_b(z, 2, S_14.as_ptr()) == 0 {
                    return 0;
                }
            }
        }
        {
            let ret = slice_from_s(z, 2, S_15.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
    }
    1
}

unsafe fn r_is_reserved_word(z: *mut SN_env) -> c_int {
    if eq_s_b(z, 2, S_16.as_ptr()) == 0 {
        return 0;
    }
    {
        let m1 = (*z).l - (*z).c;
        'lab0: {
            if eq_s_b(z, 3, S_17.as_ptr()) == 0 {
                (*z).c = (*z).l - m1;
                break 'lab0;
            }
        }
    }
    if (*z).c > (*z).lb {
        return 0;
    }
    1
}

unsafe fn r_remove_proper_noun_suffix(z: *mut SN_env) -> c_int {
    {
        let c1 = (*z).c;
        'lab0: {
            'ztur11: loop {
                let c2 = (*z).c;
                'lab1: {
                    if (*z).c == (*z).l || *(*z).p.offset((*z).c as isize) != b'\'' {
                        break 'lab1;
                    }
                    (*z).c += 1;
                    (*z).c = c2;
                    break 'ztur11;
                }
                (*z).c = c2;
                {
                    let ret = skip_utf8((*z).p, (*z).c, (*z).l, 1);
                    if ret < 0 {
                        break 'lab0;
                    }
                    (*z).c = ret;
                }
            }
            (*z).bra = (*z).c;
            (*z).c = (*z).l;
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
    1
}

unsafe fn r_more_than_one_syllable_word(z: *mut SN_env) -> c_int {
    {
        let c_test1 = (*z).c;
        {
            let mut i = 2;
            while i > 0 {
                {
                    let ret = out_grouping_U(z, G_VOWEL.as_ptr(), 97, 305, 1);
                    if ret < 0 {
                        return 0;
                    }
                    (*z).c += ret;
                }
                i -= 1;
            }
        }
        (*z).c = c_test1;
    }
    1
}

unsafe fn r_postlude(z: *mut SN_env) -> c_int {
    (*z).lb = (*z).c;
    (*z).c = (*z).l;

    {
        let m1 = (*z).l - (*z).c;
        'lab0: {
            {
                let ret = r_is_reserved_word(z);
                if ret == 0 {
                    break 'lab0;
                }
                if ret < 0 {
                    return ret;
                }
            }
            return 0;
        }
        (*z).c = (*z).l - m1;
    }
    {
        let m2 = (*z).l - (*z).c;
        {
            let ret = r_append_U_to_stems_ending_with_d_or_g(z);
            if ret < 0 {
                return ret;
            }
        }
        (*z).c = (*z).l - m2;
    }
    {
        let m3 = (*z).l - (*z).c;
        {
            let ret = r_post_process_last_consonants(z);
            if ret < 0 {
                return ret;
            }
        }
        (*z).c = (*z).l - m3;
    }
    (*z).c = (*z).lb;
    1
}

// ---------------------------------------------------------------------------
// exported entry points
// ---------------------------------------------------------------------------

#[no_mangle]
pub unsafe extern "C" fn turkish_UTF_8_stem(z: *mut SN_env) -> c_int {
    {
        let ret = r_remove_proper_noun_suffix(z);
        if ret < 0 {
            return ret;
        }
    }
    {
        let ret = r_more_than_one_syllable_word(z);
        if ret <= 0 {
            return ret;
        }
    }
    (*z).lb = (*z).c;
    (*z).c = (*z).l;

    {
        let m1 = (*z).l - (*z).c;
        {
            let ret = r_stem_nominal_verb_suffixes(z);
            if ret < 0 {
                return ret;
            }
        }
        (*z).c = (*z).l - m1;
    }
    if *(*z).I.offset(0) == 0 {
        return 0;
    }
    {
        let m2 = (*z).l - (*z).c;
        {
            let ret = r_stem_noun_suffixes(z);
            if ret < 0 {
                return ret;
            }
        }
        (*z).c = (*z).l - m2;
    }
    (*z).c = (*z).lb;
    {
        let ret = r_postlude(z);
        if ret <= 0 {
            return ret;
        }
    }
    1
}

#[no_mangle]
pub unsafe extern "C" fn turkish_UTF_8_create_env() -> *mut SN_env {
    SN_create_env(0, 1)
}

#[no_mangle]
pub unsafe extern "C" fn turkish_UTF_8_close_env(z: *mut SN_env) {
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
        let z = turkish_UTF_8_create_env();
        assert!(!z.is_null());
        let rc = SN_set_current(z, word.len() as c_int, word.as_ptr());
        assert!(rc >= 0);
        let rc = turkish_UTF_8_stem(z);
        assert!(rc >= 0, "stem returned {rc}");
        let out = std::slice::from_raw_parts((*z).p, SIZE((*z).p) as usize).to_vec();
        turkish_UTF_8_close_env(z);
        out
    }

    #[test]
    fn idempotent() {
        unsafe {
            for w in [&b"evlerim"[..], &b"kitaplar"[..], &b"gelmektedir"[..]] {
                let once = stem(w);
                let twice = stem(&once);
                assert_eq!(once, twice, "not idempotent for {:?}", w);
            }
        }
    }
}
