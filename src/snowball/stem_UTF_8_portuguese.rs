//! Portuguese Snowball stemmer (UTF-8).
//!
//! 1:1 translation of the generated
//! `src/backend/snowball/libstemmer/stem_UTF_8_portuguese.c` (Snowball 2.2.0),
//! merged with its header `stem_UTF_8_portuguese.h`. The runtime helpers come
//! from `crate::snowball::api` and `crate::snowball::utilities`.

use crate::prelude::*;

use crate::snowball::api::{among, symbol, SN_close_env, SN_create_env, SN_env, SIZE};
use crate::snowball::utilities::{
    eq_s_b, find_among, find_among_b, in_grouping_U, out_grouping_U, skip_utf8, slice_del,
    slice_from_s,
};

// ---------------------------------------------------------------------------
// among string tables
// ---------------------------------------------------------------------------

static S_0_1: [symbol; 2] = [0xC3, 0xA3];
static S_0_2: [symbol; 2] = [0xC3, 0xB5];

static A_0: [among; 3] = [
    among { s_size: 0, s: core::ptr::null(), substring_i: -1, result: 3, function: None },
    among { s_size: 2, s: S_0_1.as_ptr(), substring_i: 0, result: 1, function: None },
    among { s_size: 2, s: S_0_2.as_ptr(), substring_i: 0, result: 2, function: None },
];

static S_1_1: [symbol; 2] = [b'a', b'~'];
static S_1_2: [symbol; 2] = [b'o', b'~'];

static A_1: [among; 3] = [
    among { s_size: 0, s: core::ptr::null(), substring_i: -1, result: 3, function: None },
    among { s_size: 2, s: S_1_1.as_ptr(), substring_i: 0, result: 1, function: None },
    among { s_size: 2, s: S_1_2.as_ptr(), substring_i: 0, result: 2, function: None },
];

static S_2_0: [symbol; 2] = [b'i', b'c'];
static S_2_1: [symbol; 2] = [b'a', b'd'];
static S_2_2: [symbol; 2] = [b'o', b's'];
static S_2_3: [symbol; 2] = [b'i', b'v'];

static A_2: [among; 4] = [
    among { s_size: 2, s: S_2_0.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_2_1.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_2_2.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_2_3.as_ptr(), substring_i: -1, result: 1, function: None },
];

static S_3_0: [symbol; 4] = [b'a', b'n', b't', b'e'];
static S_3_1: [symbol; 4] = [b'a', b'v', b'e', b'l'];
static S_3_2: [symbol; 5] = [0xC3, 0xAD, b'v', b'e', b'l'];

static A_3: [among; 3] = [
    among { s_size: 4, s: S_3_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_3_1.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_3_2.as_ptr(), substring_i: -1, result: 1, function: None },
];

static S_4_0: [symbol; 2] = [b'i', b'c'];
static S_4_1: [symbol; 4] = [b'a', b'b', b'i', b'l'];
static S_4_2: [symbol; 2] = [b'i', b'v'];

static A_4: [among; 3] = [
    among { s_size: 2, s: S_4_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_4_1.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_4_2.as_ptr(), substring_i: -1, result: 1, function: None },
];

static S_5_0: [symbol; 3] = [b'i', b'c', b'a'];
static S_5_1: [symbol; 6] = [0xC3, 0xA2, b'n', b'c', b'i', b'a'];
static S_5_2: [symbol; 6] = [0xC3, 0xAA, b'n', b'c', b'i', b'a'];
static S_5_3: [symbol; 5] = [b'l', b'o', b'g', b'i', b'a'];
static S_5_4: [symbol; 3] = [b'i', b'r', b'a'];
static S_5_5: [symbol; 5] = [b'a', b'd', b'o', b'r', b'a'];
static S_5_6: [symbol; 3] = [b'o', b's', b'a'];
static S_5_7: [symbol; 4] = [b'i', b's', b't', b'a'];
static S_5_8: [symbol; 3] = [b'i', b'v', b'a'];
static S_5_9: [symbol; 3] = [b'e', b'z', b'a'];
static S_5_10: [symbol; 5] = [b'i', b'd', b'a', b'd', b'e'];
static S_5_11: [symbol; 4] = [b'a', b'n', b't', b'e'];
static S_5_12: [symbol; 5] = [b'm', b'e', b'n', b't', b'e'];
static S_5_13: [symbol; 6] = [b'a', b'm', b'e', b'n', b't', b'e'];
static S_5_14: [symbol; 5] = [0xC3, 0xA1, b'v', b'e', b'l'];
static S_5_15: [symbol; 5] = [0xC3, 0xAD, b'v', b'e', b'l'];
static S_5_16: [symbol; 3] = [b'i', b'c', b'o'];
static S_5_17: [symbol; 4] = [b'i', b's', b'm', b'o'];
static S_5_18: [symbol; 3] = [b'o', b's', b'o'];
static S_5_19: [symbol; 6] = [b'a', b'm', b'e', b'n', b't', b'o'];
static S_5_20: [symbol; 6] = [b'i', b'm', b'e', b'n', b't', b'o'];
static S_5_21: [symbol; 3] = [b'i', b'v', b'o'];
static S_5_22: [symbol; 6] = [b'a', 0xC3, 0xA7, b'a', b'~', b'o'];
static S_5_23: [symbol; 6] = [b'u', 0xC3, 0xA7, b'a', b'~', b'o'];
static S_5_24: [symbol; 4] = [b'a', b'd', b'o', b'r'];
static S_5_25: [symbol; 4] = [b'i', b'c', b'a', b's'];
static S_5_26: [symbol; 7] = [0xC3, 0xAA, b'n', b'c', b'i', b'a', b's'];
static S_5_27: [symbol; 6] = [b'l', b'o', b'g', b'i', b'a', b's'];
static S_5_28: [symbol; 4] = [b'i', b'r', b'a', b's'];
static S_5_29: [symbol; 6] = [b'a', b'd', b'o', b'r', b'a', b's'];
static S_5_30: [symbol; 4] = [b'o', b's', b'a', b's'];
static S_5_31: [symbol; 5] = [b'i', b's', b't', b'a', b's'];
static S_5_32: [symbol; 4] = [b'i', b'v', b'a', b's'];
static S_5_33: [symbol; 4] = [b'e', b'z', b'a', b's'];
static S_5_34: [symbol; 6] = [b'i', b'd', b'a', b'd', b'e', b's'];
static S_5_35: [symbol; 6] = [b'a', b'd', b'o', b'r', b'e', b's'];
static S_5_36: [symbol; 5] = [b'a', b'n', b't', b'e', b's'];
static S_5_37: [symbol; 7] = [b'a', 0xC3, 0xA7, b'o', b'~', b'e', b's'];
static S_5_38: [symbol; 7] = [b'u', 0xC3, 0xA7, b'o', b'~', b'e', b's'];
static S_5_39: [symbol; 4] = [b'i', b'c', b'o', b's'];
static S_5_40: [symbol; 5] = [b'i', b's', b'm', b'o', b's'];
static S_5_41: [symbol; 4] = [b'o', b's', b'o', b's'];
static S_5_42: [symbol; 7] = [b'a', b'm', b'e', b'n', b't', b'o', b's'];
static S_5_43: [symbol; 7] = [b'i', b'm', b'e', b'n', b't', b'o', b's'];
static S_5_44: [symbol; 4] = [b'i', b'v', b'o', b's'];

static A_5: [among; 45] = [
    among { s_size: 3, s: S_5_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_5_1.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_5_2.as_ptr(), substring_i: -1, result: 4, function: None },
    among { s_size: 5, s: S_5_3.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 3, s: S_5_4.as_ptr(), substring_i: -1, result: 9, function: None },
    among { s_size: 5, s: S_5_5.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_5_6.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_5_7.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_5_8.as_ptr(), substring_i: -1, result: 8, function: None },
    among { s_size: 3, s: S_5_9.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_5_10.as_ptr(), substring_i: -1, result: 7, function: None },
    among { s_size: 4, s: S_5_11.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_5_12.as_ptr(), substring_i: -1, result: 6, function: None },
    among { s_size: 6, s: S_5_13.as_ptr(), substring_i: 12, result: 5, function: None },
    among { s_size: 5, s: S_5_14.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_5_15.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_5_16.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_5_17.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_5_18.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_5_19.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_5_20.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_5_21.as_ptr(), substring_i: -1, result: 8, function: None },
    among { s_size: 6, s: S_5_22.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_5_23.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 4, s: S_5_24.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_5_25.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 7, s: S_5_26.as_ptr(), substring_i: -1, result: 4, function: None },
    among { s_size: 6, s: S_5_27.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 4, s: S_5_28.as_ptr(), substring_i: -1, result: 9, function: None },
    among { s_size: 6, s: S_5_29.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_5_30.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_5_31.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_5_32.as_ptr(), substring_i: -1, result: 8, function: None },
    among { s_size: 4, s: S_5_33.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_5_34.as_ptr(), substring_i: -1, result: 7, function: None },
    among { s_size: 6, s: S_5_35.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_5_36.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 7, s: S_5_37.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 7, s: S_5_38.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 4, s: S_5_39.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_5_40.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_5_41.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 7, s: S_5_42.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 7, s: S_5_43.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_5_44.as_ptr(), substring_i: -1, result: 8, function: None },
];

static S_6_0: [symbol; 3] = [b'a', b'd', b'a'];
static S_6_1: [symbol; 3] = [b'i', b'd', b'a'];
static S_6_2: [symbol; 2] = [b'i', b'a'];
static S_6_3: [symbol; 4] = [b'a', b'r', b'i', b'a'];
static S_6_4: [symbol; 4] = [b'e', b'r', b'i', b'a'];
static S_6_5: [symbol; 4] = [b'i', b'r', b'i', b'a'];
static S_6_6: [symbol; 3] = [b'a', b'r', b'a'];
static S_6_7: [symbol; 3] = [b'e', b'r', b'a'];
static S_6_8: [symbol; 3] = [b'i', b'r', b'a'];
static S_6_9: [symbol; 3] = [b'a', b'v', b'a'];
static S_6_10: [symbol; 4] = [b'a', b's', b's', b'e'];
static S_6_11: [symbol; 4] = [b'e', b's', b's', b'e'];
static S_6_12: [symbol; 4] = [b'i', b's', b's', b'e'];
static S_6_13: [symbol; 4] = [b'a', b's', b't', b'e'];
static S_6_14: [symbol; 4] = [b'e', b's', b't', b'e'];
static S_6_15: [symbol; 4] = [b'i', b's', b't', b'e'];
static S_6_16: [symbol; 2] = [b'e', b'i'];
static S_6_17: [symbol; 4] = [b'a', b'r', b'e', b'i'];
static S_6_18: [symbol; 4] = [b'e', b'r', b'e', b'i'];
static S_6_19: [symbol; 4] = [b'i', b'r', b'e', b'i'];
static S_6_20: [symbol; 2] = [b'a', b'm'];
static S_6_21: [symbol; 3] = [b'i', b'a', b'm'];
static S_6_22: [symbol; 5] = [b'a', b'r', b'i', b'a', b'm'];
static S_6_23: [symbol; 5] = [b'e', b'r', b'i', b'a', b'm'];
static S_6_24: [symbol; 5] = [b'i', b'r', b'i', b'a', b'm'];
static S_6_25: [symbol; 4] = [b'a', b'r', b'a', b'm'];
static S_6_26: [symbol; 4] = [b'e', b'r', b'a', b'm'];
static S_6_27: [symbol; 4] = [b'i', b'r', b'a', b'm'];
static S_6_28: [symbol; 4] = [b'a', b'v', b'a', b'm'];
static S_6_29: [symbol; 2] = [b'e', b'm'];
static S_6_30: [symbol; 4] = [b'a', b'r', b'e', b'm'];
static S_6_31: [symbol; 4] = [b'e', b'r', b'e', b'm'];
static S_6_32: [symbol; 4] = [b'i', b'r', b'e', b'm'];
static S_6_33: [symbol; 5] = [b'a', b's', b's', b'e', b'm'];
static S_6_34: [symbol; 5] = [b'e', b's', b's', b'e', b'm'];
static S_6_35: [symbol; 5] = [b'i', b's', b's', b'e', b'm'];
static S_6_36: [symbol; 3] = [b'a', b'd', b'o'];
static S_6_37: [symbol; 3] = [b'i', b'd', b'o'];
static S_6_38: [symbol; 4] = [b'a', b'n', b'd', b'o'];
static S_6_39: [symbol; 4] = [b'e', b'n', b'd', b'o'];
static S_6_40: [symbol; 4] = [b'i', b'n', b'd', b'o'];
static S_6_41: [symbol; 5] = [b'a', b'r', b'a', b'~', b'o'];
static S_6_42: [symbol; 5] = [b'e', b'r', b'a', b'~', b'o'];
static S_6_43: [symbol; 5] = [b'i', b'r', b'a', b'~', b'o'];
static S_6_44: [symbol; 2] = [b'a', b'r'];
static S_6_45: [symbol; 2] = [b'e', b'r'];
static S_6_46: [symbol; 2] = [b'i', b'r'];
static S_6_47: [symbol; 2] = [b'a', b's'];
static S_6_48: [symbol; 4] = [b'a', b'd', b'a', b's'];
static S_6_49: [symbol; 4] = [b'i', b'd', b'a', b's'];
static S_6_50: [symbol; 3] = [b'i', b'a', b's'];
static S_6_51: [symbol; 5] = [b'a', b'r', b'i', b'a', b's'];
static S_6_52: [symbol; 5] = [b'e', b'r', b'i', b'a', b's'];
static S_6_53: [symbol; 5] = [b'i', b'r', b'i', b'a', b's'];
static S_6_54: [symbol; 4] = [b'a', b'r', b'a', b's'];
static S_6_55: [symbol; 4] = [b'e', b'r', b'a', b's'];
static S_6_56: [symbol; 4] = [b'i', b'r', b'a', b's'];
static S_6_57: [symbol; 4] = [b'a', b'v', b'a', b's'];
static S_6_58: [symbol; 2] = [b'e', b's'];
static S_6_59: [symbol; 5] = [b'a', b'r', b'd', b'e', b's'];
static S_6_60: [symbol; 5] = [b'e', b'r', b'd', b'e', b's'];
static S_6_61: [symbol; 5] = [b'i', b'r', b'd', b'e', b's'];
static S_6_62: [symbol; 4] = [b'a', b'r', b'e', b's'];
static S_6_63: [symbol; 4] = [b'e', b'r', b'e', b's'];
static S_6_64: [symbol; 4] = [b'i', b'r', b'e', b's'];
static S_6_65: [symbol; 5] = [b'a', b's', b's', b'e', b's'];
static S_6_66: [symbol; 5] = [b'e', b's', b's', b'e', b's'];
static S_6_67: [symbol; 5] = [b'i', b's', b's', b'e', b's'];
static S_6_68: [symbol; 5] = [b'a', b's', b't', b'e', b's'];
static S_6_69: [symbol; 5] = [b'e', b's', b't', b'e', b's'];
static S_6_70: [symbol; 5] = [b'i', b's', b't', b'e', b's'];
static S_6_71: [symbol; 2] = [b'i', b's'];
static S_6_72: [symbol; 3] = [b'a', b'i', b's'];
static S_6_73: [symbol; 3] = [b'e', b'i', b's'];
static S_6_74: [symbol; 5] = [b'a', b'r', b'e', b'i', b's'];
static S_6_75: [symbol; 5] = [b'e', b'r', b'e', b'i', b's'];
static S_6_76: [symbol; 5] = [b'i', b'r', b'e', b'i', b's'];
static S_6_77: [symbol; 6] = [0xC3, 0xA1, b'r', b'e', b'i', b's'];
static S_6_78: [symbol; 6] = [0xC3, 0xA9, b'r', b'e', b'i', b's'];
static S_6_79: [symbol; 6] = [0xC3, 0xAD, b'r', b'e', b'i', b's'];
static S_6_80: [symbol; 7] = [0xC3, 0xA1, b's', b's', b'e', b'i', b's'];
static S_6_81: [symbol; 7] = [0xC3, 0xA9, b's', b's', b'e', b'i', b's'];
static S_6_82: [symbol; 7] = [0xC3, 0xAD, b's', b's', b'e', b'i', b's'];
static S_6_83: [symbol; 6] = [0xC3, 0xA1, b'v', b'e', b'i', b's'];
static S_6_84: [symbol; 5] = [0xC3, 0xAD, b'e', b'i', b's'];
static S_6_85: [symbol; 7] = [b'a', b'r', 0xC3, 0xAD, b'e', b'i', b's'];
static S_6_86: [symbol; 7] = [b'e', b'r', 0xC3, 0xAD, b'e', b'i', b's'];
static S_6_87: [symbol; 7] = [b'i', b'r', 0xC3, 0xAD, b'e', b'i', b's'];
static S_6_88: [symbol; 4] = [b'a', b'd', b'o', b's'];
static S_6_89: [symbol; 4] = [b'i', b'd', b'o', b's'];
static S_6_90: [symbol; 4] = [b'a', b'm', b'o', b's'];
static S_6_91: [symbol; 7] = [0xC3, 0xA1, b'r', b'a', b'm', b'o', b's'];
static S_6_92: [symbol; 7] = [0xC3, 0xA9, b'r', b'a', b'm', b'o', b's'];
static S_6_93: [symbol; 7] = [0xC3, 0xAD, b'r', b'a', b'm', b'o', b's'];
static S_6_94: [symbol; 7] = [0xC3, 0xA1, b'v', b'a', b'm', b'o', b's'];
static S_6_95: [symbol; 6] = [0xC3, 0xAD, b'a', b'm', b'o', b's'];
static S_6_96: [symbol; 8] = [b'a', b'r', 0xC3, 0xAD, b'a', b'm', b'o', b's'];
static S_6_97: [symbol; 8] = [b'e', b'r', 0xC3, 0xAD, b'a', b'm', b'o', b's'];
static S_6_98: [symbol; 8] = [b'i', b'r', 0xC3, 0xAD, b'a', b'm', b'o', b's'];
static S_6_99: [symbol; 4] = [b'e', b'm', b'o', b's'];
static S_6_100: [symbol; 6] = [b'a', b'r', b'e', b'm', b'o', b's'];
static S_6_101: [symbol; 6] = [b'e', b'r', b'e', b'm', b'o', b's'];
static S_6_102: [symbol; 6] = [b'i', b'r', b'e', b'm', b'o', b's'];
static S_6_103: [symbol; 8] = [0xC3, 0xA1, b's', b's', b'e', b'm', b'o', b's'];
static S_6_104: [symbol; 8] = [0xC3, 0xAA, b's', b's', b'e', b'm', b'o', b's'];
static S_6_105: [symbol; 8] = [0xC3, 0xAD, b's', b's', b'e', b'm', b'o', b's'];
static S_6_106: [symbol; 4] = [b'i', b'm', b'o', b's'];
static S_6_107: [symbol; 5] = [b'a', b'r', b'm', b'o', b's'];
static S_6_108: [symbol; 5] = [b'e', b'r', b'm', b'o', b's'];
static S_6_109: [symbol; 5] = [b'i', b'r', b'm', b'o', b's'];
static S_6_110: [symbol; 5] = [0xC3, 0xA1, b'm', b'o', b's'];
static S_6_111: [symbol; 5] = [b'a', b'r', 0xC3, 0xA1, b's'];
static S_6_112: [symbol; 5] = [b'e', b'r', 0xC3, 0xA1, b's'];
static S_6_113: [symbol; 5] = [b'i', b'r', 0xC3, 0xA1, b's'];
static S_6_114: [symbol; 2] = [b'e', b'u'];
static S_6_115: [symbol; 2] = [b'i', b'u'];
static S_6_116: [symbol; 2] = [b'o', b'u'];
static S_6_117: [symbol; 4] = [b'a', b'r', 0xC3, 0xA1];
static S_6_118: [symbol; 4] = [b'e', b'r', 0xC3, 0xA1];
static S_6_119: [symbol; 4] = [b'i', b'r', 0xC3, 0xA1];

static A_6: [among; 120] = [
    among { s_size: 3, s: S_6_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_6_1.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_6_2.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_6_3.as_ptr(), substring_i: 2, result: 1, function: None },
    among { s_size: 4, s: S_6_4.as_ptr(), substring_i: 2, result: 1, function: None },
    among { s_size: 4, s: S_6_5.as_ptr(), substring_i: 2, result: 1, function: None },
    among { s_size: 3, s: S_6_6.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_6_7.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_6_8.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_6_9.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_6_10.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_6_11.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_6_12.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_6_13.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_6_14.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_6_15.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_6_16.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_6_17.as_ptr(), substring_i: 16, result: 1, function: None },
    among { s_size: 4, s: S_6_18.as_ptr(), substring_i: 16, result: 1, function: None },
    among { s_size: 4, s: S_6_19.as_ptr(), substring_i: 16, result: 1, function: None },
    among { s_size: 2, s: S_6_20.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_6_21.as_ptr(), substring_i: 20, result: 1, function: None },
    among { s_size: 5, s: S_6_22.as_ptr(), substring_i: 21, result: 1, function: None },
    among { s_size: 5, s: S_6_23.as_ptr(), substring_i: 21, result: 1, function: None },
    among { s_size: 5, s: S_6_24.as_ptr(), substring_i: 21, result: 1, function: None },
    among { s_size: 4, s: S_6_25.as_ptr(), substring_i: 20, result: 1, function: None },
    among { s_size: 4, s: S_6_26.as_ptr(), substring_i: 20, result: 1, function: None },
    among { s_size: 4, s: S_6_27.as_ptr(), substring_i: 20, result: 1, function: None },
    among { s_size: 4, s: S_6_28.as_ptr(), substring_i: 20, result: 1, function: None },
    among { s_size: 2, s: S_6_29.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_6_30.as_ptr(), substring_i: 29, result: 1, function: None },
    among { s_size: 4, s: S_6_31.as_ptr(), substring_i: 29, result: 1, function: None },
    among { s_size: 4, s: S_6_32.as_ptr(), substring_i: 29, result: 1, function: None },
    among { s_size: 5, s: S_6_33.as_ptr(), substring_i: 29, result: 1, function: None },
    among { s_size: 5, s: S_6_34.as_ptr(), substring_i: 29, result: 1, function: None },
    among { s_size: 5, s: S_6_35.as_ptr(), substring_i: 29, result: 1, function: None },
    among { s_size: 3, s: S_6_36.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_6_37.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_6_38.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_6_39.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_6_40.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_6_41.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_6_42.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_6_43.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_6_44.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_6_45.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_6_46.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_6_47.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_6_48.as_ptr(), substring_i: 47, result: 1, function: None },
    among { s_size: 4, s: S_6_49.as_ptr(), substring_i: 47, result: 1, function: None },
    among { s_size: 3, s: S_6_50.as_ptr(), substring_i: 47, result: 1, function: None },
    among { s_size: 5, s: S_6_51.as_ptr(), substring_i: 50, result: 1, function: None },
    among { s_size: 5, s: S_6_52.as_ptr(), substring_i: 50, result: 1, function: None },
    among { s_size: 5, s: S_6_53.as_ptr(), substring_i: 50, result: 1, function: None },
    among { s_size: 4, s: S_6_54.as_ptr(), substring_i: 47, result: 1, function: None },
    among { s_size: 4, s: S_6_55.as_ptr(), substring_i: 47, result: 1, function: None },
    among { s_size: 4, s: S_6_56.as_ptr(), substring_i: 47, result: 1, function: None },
    among { s_size: 4, s: S_6_57.as_ptr(), substring_i: 47, result: 1, function: None },
    among { s_size: 2, s: S_6_58.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_6_59.as_ptr(), substring_i: 58, result: 1, function: None },
    among { s_size: 5, s: S_6_60.as_ptr(), substring_i: 58, result: 1, function: None },
    among { s_size: 5, s: S_6_61.as_ptr(), substring_i: 58, result: 1, function: None },
    among { s_size: 4, s: S_6_62.as_ptr(), substring_i: 58, result: 1, function: None },
    among { s_size: 4, s: S_6_63.as_ptr(), substring_i: 58, result: 1, function: None },
    among { s_size: 4, s: S_6_64.as_ptr(), substring_i: 58, result: 1, function: None },
    among { s_size: 5, s: S_6_65.as_ptr(), substring_i: 58, result: 1, function: None },
    among { s_size: 5, s: S_6_66.as_ptr(), substring_i: 58, result: 1, function: None },
    among { s_size: 5, s: S_6_67.as_ptr(), substring_i: 58, result: 1, function: None },
    among { s_size: 5, s: S_6_68.as_ptr(), substring_i: 58, result: 1, function: None },
    among { s_size: 5, s: S_6_69.as_ptr(), substring_i: 58, result: 1, function: None },
    among { s_size: 5, s: S_6_70.as_ptr(), substring_i: 58, result: 1, function: None },
    among { s_size: 2, s: S_6_71.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_6_72.as_ptr(), substring_i: 71, result: 1, function: None },
    among { s_size: 3, s: S_6_73.as_ptr(), substring_i: 71, result: 1, function: None },
    among { s_size: 5, s: S_6_74.as_ptr(), substring_i: 73, result: 1, function: None },
    among { s_size: 5, s: S_6_75.as_ptr(), substring_i: 73, result: 1, function: None },
    among { s_size: 5, s: S_6_76.as_ptr(), substring_i: 73, result: 1, function: None },
    among { s_size: 6, s: S_6_77.as_ptr(), substring_i: 73, result: 1, function: None },
    among { s_size: 6, s: S_6_78.as_ptr(), substring_i: 73, result: 1, function: None },
    among { s_size: 6, s: S_6_79.as_ptr(), substring_i: 73, result: 1, function: None },
    among { s_size: 7, s: S_6_80.as_ptr(), substring_i: 73, result: 1, function: None },
    among { s_size: 7, s: S_6_81.as_ptr(), substring_i: 73, result: 1, function: None },
    among { s_size: 7, s: S_6_82.as_ptr(), substring_i: 73, result: 1, function: None },
    among { s_size: 6, s: S_6_83.as_ptr(), substring_i: 73, result: 1, function: None },
    among { s_size: 5, s: S_6_84.as_ptr(), substring_i: 73, result: 1, function: None },
    among { s_size: 7, s: S_6_85.as_ptr(), substring_i: 84, result: 1, function: None },
    among { s_size: 7, s: S_6_86.as_ptr(), substring_i: 84, result: 1, function: None },
    among { s_size: 7, s: S_6_87.as_ptr(), substring_i: 84, result: 1, function: None },
    among { s_size: 4, s: S_6_88.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_6_89.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_6_90.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 7, s: S_6_91.as_ptr(), substring_i: 90, result: 1, function: None },
    among { s_size: 7, s: S_6_92.as_ptr(), substring_i: 90, result: 1, function: None },
    among { s_size: 7, s: S_6_93.as_ptr(), substring_i: 90, result: 1, function: None },
    among { s_size: 7, s: S_6_94.as_ptr(), substring_i: 90, result: 1, function: None },
    among { s_size: 6, s: S_6_95.as_ptr(), substring_i: 90, result: 1, function: None },
    among { s_size: 8, s: S_6_96.as_ptr(), substring_i: 95, result: 1, function: None },
    among { s_size: 8, s: S_6_97.as_ptr(), substring_i: 95, result: 1, function: None },
    among { s_size: 8, s: S_6_98.as_ptr(), substring_i: 95, result: 1, function: None },
    among { s_size: 4, s: S_6_99.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_6_100.as_ptr(), substring_i: 99, result: 1, function: None },
    among { s_size: 6, s: S_6_101.as_ptr(), substring_i: 99, result: 1, function: None },
    among { s_size: 6, s: S_6_102.as_ptr(), substring_i: 99, result: 1, function: None },
    among { s_size: 8, s: S_6_103.as_ptr(), substring_i: 99, result: 1, function: None },
    among { s_size: 8, s: S_6_104.as_ptr(), substring_i: 99, result: 1, function: None },
    among { s_size: 8, s: S_6_105.as_ptr(), substring_i: 99, result: 1, function: None },
    among { s_size: 4, s: S_6_106.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_6_107.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_6_108.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_6_109.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_6_110.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_6_111.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_6_112.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_6_113.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_6_114.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_6_115.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_6_116.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_6_117.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_6_118.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_6_119.as_ptr(), substring_i: -1, result: 1, function: None },
];

static S_7_0: [symbol; 1] = [b'a'];
static S_7_1: [symbol; 1] = [b'i'];
static S_7_2: [symbol; 1] = [b'o'];
static S_7_3: [symbol; 2] = [b'o', b's'];
static S_7_4: [symbol; 2] = [0xC3, 0xA1];
static S_7_5: [symbol; 2] = [0xC3, 0xAD];
static S_7_6: [symbol; 2] = [0xC3, 0xB3];

static A_7: [among; 7] = [
    among { s_size: 1, s: S_7_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 1, s: S_7_1.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 1, s: S_7_2.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_7_3.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_7_4.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_7_5.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_7_6.as_ptr(), substring_i: -1, result: 1, function: None },
];

static S_8_0: [symbol; 1] = [b'e'];
static S_8_1: [symbol; 2] = [0xC3, 0xA7];
static S_8_2: [symbol; 2] = [0xC3, 0xA9];
static S_8_3: [symbol; 2] = [0xC3, 0xAA];

static A_8: [among; 4] = [
    among { s_size: 1, s: S_8_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_8_1.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 2, s: S_8_2.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_8_3.as_ptr(), substring_i: -1, result: 1, function: None },
];

// ---------------------------------------------------------------------------
// grouping bit tables
// ---------------------------------------------------------------------------

static G_V: [c_uchar; 20] = [
    17, 65, 16, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 19, 12, 2,
];

// ---------------------------------------------------------------------------
// literal strings used by slice_from_s / eq_s_b
// ---------------------------------------------------------------------------

static S_0: [symbol; 2] = [b'a', b'~'];
static S_1: [symbol; 2] = [b'o', b'~'];
static S_2: [symbol; 2] = [0xC3, 0xA3];
static S_3: [symbol; 2] = [0xC3, 0xB5];
static S_4: [symbol; 3] = [b'l', b'o', b'g'];
static S_5: [symbol; 1] = [b'u'];
static S_6: [symbol; 4] = [b'e', b'n', b't', b'e'];
static S_7: [symbol; 2] = [b'a', b't'];
static S_8: [symbol; 2] = [b'a', b't'];
static S_9: [symbol; 2] = [b'i', b'r'];
static S_10: [symbol; 1] = [b'c'];

// ---------------------------------------------------------------------------
// rules
// ---------------------------------------------------------------------------

unsafe fn r_prelude(z: *mut SN_env) -> c_int {
    let mut among_var;
    'repeat0: loop {
        let c1 = (*z).c;
        (*z).bra = (*z).c;
        if (*z).c + 1 >= (*z).l
            || (*(*z).p.offset(((*z).c + 1) as isize) != 163
                && *(*z).p.offset(((*z).c + 1) as isize) != 181)
        {
            among_var = 3;
        } else {
            among_var = find_among(z, A_0.as_ptr(), 3);
        }
        (*z).ket = (*z).c;
        'lab0: {
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
                    let ret = skip_utf8((*z).p, (*z).c, (*z).l, 1);
                    if ret < 0 {
                        break 'lab0;
                    }
                    (*z).c = ret;
                }
                _ => {}
            }
            continue 'repeat0;
        }
        (*z).c = c1;
        break;
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
            // 'lab1 block: breaking out of it (success) falls through to set
            // z->I[2]; `break 'lab0` (failure) skips that assignment.
            'lab1: {
                let c2 = (*z).c;
                'lab2: {
                    if in_grouping_U(z, G_V.as_ptr(), 97, 250, 0) != 0 {
                        break 'lab2;
                    }
                    'lab3: {
                        let c3 = (*z).c;
                        'lab4: {
                            if out_grouping_U(z, G_V.as_ptr(), 97, 250, 0) != 0 {
                                break 'lab4;
                            }
                            {
                                let ret = out_grouping_U(z, G_V.as_ptr(), 97, 250, 1);
                                if ret < 0 {
                                    break 'lab4;
                                }
                                (*z).c += ret;
                            }
                            break 'lab3;
                        }
                        (*z).c = c3;
                        if in_grouping_U(z, G_V.as_ptr(), 97, 250, 0) != 0 {
                            break 'lab2;
                        }
                        {
                            let ret = in_grouping_U(z, G_V.as_ptr(), 97, 250, 1);
                            if ret < 0 {
                                break 'lab2;
                            }
                            (*z).c += ret;
                        }
                    }
                    // lab3: -> goto lab1 (success)
                    break 'lab1;
                }
                // lab2:
                (*z).c = c2;
                if out_grouping_U(z, G_V.as_ptr(), 97, 250, 0) != 0 {
                    break 'lab0;
                }
                'lab5: {
                    let c4 = (*z).c;
                    'lab6: {
                        if out_grouping_U(z, G_V.as_ptr(), 97, 250, 0) != 0 {
                            break 'lab6;
                        }
                        {
                            let ret = out_grouping_U(z, G_V.as_ptr(), 97, 250, 1);
                            if ret < 0 {
                                break 'lab6;
                            }
                            (*z).c += ret;
                        }
                        break 'lab5;
                    }
                    (*z).c = c4;
                    if in_grouping_U(z, G_V.as_ptr(), 97, 250, 0) != 0 {
                        break 'lab0;
                    }
                    {
                        let ret = skip_utf8((*z).p, (*z).c, (*z).l, 1);
                        if ret < 0 {
                            break 'lab0;
                        }
                        (*z).c = ret;
                    }
                }
                // lab5: -> fall through to lab1 (success)
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
                let ret = out_grouping_U(z, G_V.as_ptr(), 97, 250, 1);
                if ret < 0 {
                    break 'lab7;
                }
                (*z).c += ret;
            }
            {
                let ret = in_grouping_U(z, G_V.as_ptr(), 97, 250, 1);
                if ret < 0 {
                    break 'lab7;
                }
                (*z).c += ret;
            }
            *(*z).I.offset(1) = (*z).c;
            {
                let ret = out_grouping_U(z, G_V.as_ptr(), 97, 250, 1);
                if ret < 0 {
                    break 'lab7;
                }
                (*z).c += ret;
            }
            {
                let ret = in_grouping_U(z, G_V.as_ptr(), 97, 250, 1);
                if ret < 0 {
                    break 'lab7;
                }
                (*z).c += ret;
            }
            *(*z).I.offset(0) = (*z).c;
        }
        (*z).c = c5;
    }
    1
}

unsafe fn r_postlude(z: *mut SN_env) -> c_int {
    let mut among_var;
    'repeat1: loop {
        let c1 = (*z).c;
        (*z).bra = (*z).c;
        if (*z).c + 1 >= (*z).l || *(*z).p.offset(((*z).c + 1) as isize) != 126 {
            among_var = 3;
        } else {
            among_var = find_among(z, A_1.as_ptr(), 3);
        }
        (*z).ket = (*z).c;
        'lab0: {
            match among_var {
                1 => {
                    let ret = slice_from_s(z, 2, S_2.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
                2 => {
                    let ret = slice_from_s(z, 2, S_3.as_ptr());
                    if ret < 0 {
                        return ret;
                    }
                }
                3 => {
                    let ret = skip_utf8((*z).p, (*z).c, (*z).l, 1);
                    if ret < 0 {
                        break 'lab0;
                    }
                    (*z).c = ret;
                }
                _ => {}
            }
            continue 'repeat1;
        }
        (*z).c = c1;
        break;
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

unsafe fn r_standard_suffix(z: *mut SN_env) -> c_int {
    let mut among_var;
    (*z).ket = (*z).c;
    if (*z).c - 2 <= (*z).lb
        || *(*z).p.offset(((*z).c - 1) as isize) as c_int >> 5 != 3
        || (823330 >> (*(*z).p.offset(((*z).c - 1) as isize) as c_int & 0x1f)) & 1 == 0
    {
        return 0;
    }
    among_var = find_among_b(z, A_5.as_ptr(), 45);
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
                let ret = slice_from_s(z, 3, S_4.as_ptr());
                if ret < 0 {
                    return ret;
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
                let ret = slice_from_s(z, 1, S_5.as_ptr());
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
                let ret = slice_from_s(z, 4, S_6.as_ptr());
                if ret < 0 {
                    return ret;
                }
            }
        }
        5 => {
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
                let m1 = (*z).l - (*z).c;
                'lab0: {
                    (*z).ket = (*z).c;
                    if (*z).c - 1 <= (*z).lb
                        || *(*z).p.offset(((*z).c - 1) as isize) as c_int >> 5 != 3
                        || (4718616 >> (*(*z).p.offset(((*z).c - 1) as isize) as c_int & 0x1f)) & 1
                            == 0
                    {
                        (*z).c = (*z).l - m1;
                        break 'lab0;
                    }
                    among_var = find_among_b(z, A_2.as_ptr(), 4);
                    if among_var == 0 {
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
                    {
                        let ret = slice_del(z);
                        if ret < 0 {
                            return ret;
                        }
                    }
                    if among_var == 1 {
                        (*z).ket = (*z).c;
                        if eq_s_b(z, 2, S_7.as_ptr()) == 0 {
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
            {
                let m2 = (*z).l - (*z).c;
                'lab1: {
                    (*z).ket = (*z).c;
                    if (*z).c - 3 <= (*z).lb
                        || (*(*z).p.offset(((*z).c - 1) as isize) != 101
                            && *(*z).p.offset(((*z).c - 1) as isize) != 108)
                    {
                        (*z).c = (*z).l - m2;
                        break 'lab1;
                    }
                    if find_among_b(z, A_3.as_ptr(), 3) == 0 {
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
                    if (*z).c - 1 <= (*z).lb
                        || *(*z).p.offset(((*z).c - 1) as isize) as c_int >> 5 != 3
                        || (4198408 >> (*(*z).p.offset(((*z).c - 1) as isize) as c_int & 0x1f)) & 1
                            == 0
                    {
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
                    {
                        let ret = slice_del(z);
                        if ret < 0 {
                            return ret;
                        }
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
                    if eq_s_b(z, 2, S_8.as_ptr()) == 0 {
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
                }
            }
        }
        9 => {
            {
                let ret = r_RV(z);
                if ret <= 0 {
                    return ret;
                }
            }
            if (*z).c <= (*z).lb || *(*z).p.offset(((*z).c - 1) as isize) != b'e' {
                return 0;
            }
            (*z).c -= 1;
            {
                let ret = slice_from_s(z, 2, S_9.as_ptr());
                if ret < 0 {
                    return ret;
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
        if find_among_b(z, A_6.as_ptr(), 120) == 0 {
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

unsafe fn r_residual_suffix(z: *mut SN_env) -> c_int {
    (*z).ket = (*z).c;
    if find_among_b(z, A_7.as_ptr(), 7) == 0 {
        return 0;
    }
    (*z).bra = (*z).c;
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
    1
}

unsafe fn r_residual_form(z: *mut SN_env) -> c_int {
    let among_var;
    (*z).ket = (*z).c;
    among_var = find_among_b(z, A_8.as_ptr(), 4);
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
            (*z).ket = (*z).c;
            'lab0: {
                let m1 = (*z).l - (*z).c;
                'lab1: {
                    if (*z).c <= (*z).lb || *(*z).p.offset(((*z).c - 1) as isize) != b'u' {
                        break 'lab1;
                    }
                    (*z).c -= 1;
                    (*z).bra = (*z).c;
                    {
                        let m_test2 = (*z).l - (*z).c;
                        if (*z).c <= (*z).lb || *(*z).p.offset(((*z).c - 1) as isize) != b'g' {
                            break 'lab1;
                        }
                        (*z).c -= 1;
                        (*z).c = (*z).l - m_test2;
                    }
                    break 'lab0;
                }
                // lab1:
                (*z).c = (*z).l - m1;
                if (*z).c <= (*z).lb || *(*z).p.offset(((*z).c - 1) as isize) != b'i' {
                    return 0;
                }
                (*z).c -= 1;
                (*z).bra = (*z).c;
                {
                    let m_test3 = (*z).l - (*z).c;
                    if (*z).c <= (*z).lb || *(*z).p.offset(((*z).c - 1) as isize) != b'c' {
                        return 0;
                    }
                    (*z).c -= 1;
                    (*z).c = (*z).l - m_test3;
                }
            }
            // lab0:
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
            let ret = slice_from_s(z, 1, S_10.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        _ => {}
    }
    1
}

#[no_mangle]
pub unsafe extern "C" fn portuguese_UTF_8_stem(z: *mut SN_env) -> c_int {
    {
        let c1 = (*z).c;
        {
            let ret = r_prelude(z);
            if ret < 0 {
                return ret;
            }
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
        let m2 = (*z).l - (*z).c;
        'lab0: {
            'lab1: {
                let m3 = (*z).l - (*z).c;
                'lab2: {
                    let m4 = (*z).l - (*z).c;
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
                    (*z).c = (*z).l - m4;
                    {
                        let m6 = (*z).l - (*z).c;
                        'lab5: {
                            (*z).ket = (*z).c;
                            if (*z).c <= (*z).lb || *(*z).p.offset(((*z).c - 1) as isize) != b'i' {
                                break 'lab5;
                            }
                            (*z).c -= 1;
                            (*z).bra = (*z).c;
                            {
                                let m_test7 = (*z).l - (*z).c;
                                if (*z).c <= (*z).lb
                                    || *(*z).p.offset(((*z).c - 1) as isize) != b'c'
                                {
                                    break 'lab5;
                                }
                                (*z).c -= 1;
                                (*z).c = (*z).l - m_test7;
                            }
                            {
                                let ret = r_RV(z);
                                if ret == 0 {
                                    break 'lab5;
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
                        // lab5:
                        (*z).c = (*z).l - m6;
                    }
                    // goto lab1 (standard/verb path completed successfully)
                    break 'lab1;
                }
                // lab2:
                (*z).c = (*z).l - m3;
                {
                    let ret = r_residual_suffix(z);
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
        let m8 = (*z).l - (*z).c;
        {
            let ret = r_residual_form(z);
            if ret < 0 {
                return ret;
            }
        }
        (*z).c = (*z).l - m8;
    }
    (*z).c = (*z).lb;
    {
        let c9 = (*z).c;
        {
            let ret = r_postlude(z);
            if ret < 0 {
                return ret;
            }
        }
        (*z).c = c9;
    }
    1
}

#[no_mangle]
pub unsafe extern "C" fn portuguese_UTF_8_create_env() -> *mut SN_env {
    SN_create_env(0, 3)
}

#[no_mangle]
pub unsafe extern "C" fn portuguese_UTF_8_close_env(z: *mut SN_env) {
    SN_close_env(z, 0);
}

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snowball::api::SN_set_current;

    unsafe fn stem(word: &[u8]) -> Vec<u8> {
        let z = portuguese_UTF_8_create_env();
        assert!(!z.is_null());
        let rc = SN_set_current(z, word.len() as c_int, word.as_ptr());
        assert!(rc >= 0);
        let rc = portuguese_UTF_8_stem(z);
        assert!(rc >= 0, "stem returned {rc}");
        let out = std::slice::from_raw_parts((*z).p, SIZE((*z).p) as usize).to_vec();
        portuguese_UTF_8_close_env(z);
        out
    }

    // A short root with no strippable suffix is returned unchanged.
    #[test]
    fn short_root_unchanged() {
        unsafe {
            assert_eq!(stem(b"sol"), b"sol".to_vec());
        }
    }

    // Idempotence over several accented words; stems stay non-empty.
    #[test]
    fn idempotent() {
        unsafe {
            for w in [
                &b"boa"[..],
                &b"associa\xC3\xA7\xC3\xA3o"[..],
                &b"informa\xC3\xA7\xC3\xB5es"[..],
                &b"nacionalismo"[..],
                &b"corramos"[..],
                &b"fant\xC3\xA1stico"[..],
            ] {
                let once = stem(w);
                let twice = stem(&once);
                assert_eq!(once, twice, "not idempotent for {:?}", w);
                assert!(!once.is_empty());
            }
        }
    }
}
