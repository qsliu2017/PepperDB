//! Arabic Snowball stemmer (UTF-8).
//!
//! 1:1 translation of the generated
//! `src/backend/snowball/libstemmer/stem_UTF_8_arabic.c` (Snowball 2.2.0),
//! merged with its header `stem_UTF_8_arabic.h`. The runtime helpers come from
//! `crate::snowball::api` and `crate::snowball::utilities`.

use crate::prelude::*;

use crate::snowball::api::{among, symbol, SN_close_env, SN_create_env, SN_env, SIZE};
use crate::snowball::utilities::{
    eq_s, find_among, find_among_b, len_utf8, skip_b_utf8, skip_utf8, slice_del, slice_from_s,
};

// ---------------------------------------------------------------------------
// among string tables
// ---------------------------------------------------------------------------

static S_0_0: [symbol; 2] = [0xD9, 0x80];
static S_0_1: [symbol; 2] = [0xD9, 0x8B];
static S_0_2: [symbol; 2] = [0xD9, 0x8C];
static S_0_3: [symbol; 2] = [0xD9, 0x8D];
static S_0_4: [symbol; 2] = [0xD9, 0x8E];
static S_0_5: [symbol; 2] = [0xD9, 0x8F];
static S_0_6: [symbol; 2] = [0xD9, 0x90];
static S_0_7: [symbol; 2] = [0xD9, 0x91];
static S_0_8: [symbol; 2] = [0xD9, 0x92];
static S_0_9: [symbol; 2] = [0xD9, 0xA0];
static S_0_10: [symbol; 2] = [0xD9, 0xA1];
static S_0_11: [symbol; 2] = [0xD9, 0xA2];
static S_0_12: [symbol; 2] = [0xD9, 0xA3];
static S_0_13: [symbol; 2] = [0xD9, 0xA4];
static S_0_14: [symbol; 2] = [0xD9, 0xA5];
static S_0_15: [symbol; 2] = [0xD9, 0xA6];
static S_0_16: [symbol; 2] = [0xD9, 0xA7];
static S_0_17: [symbol; 2] = [0xD9, 0xA8];
static S_0_18: [symbol; 2] = [0xD9, 0xA9];
static S_0_19: [symbol; 3] = [0xEF, 0xBA, 0x80];
static S_0_20: [symbol; 3] = [0xEF, 0xBA, 0x81];
static S_0_21: [symbol; 3] = [0xEF, 0xBA, 0x82];
static S_0_22: [symbol; 3] = [0xEF, 0xBA, 0x83];
static S_0_23: [symbol; 3] = [0xEF, 0xBA, 0x84];
static S_0_24: [symbol; 3] = [0xEF, 0xBA, 0x85];
static S_0_25: [symbol; 3] = [0xEF, 0xBA, 0x86];
static S_0_26: [symbol; 3] = [0xEF, 0xBA, 0x87];
static S_0_27: [symbol; 3] = [0xEF, 0xBA, 0x88];
static S_0_28: [symbol; 3] = [0xEF, 0xBA, 0x89];
static S_0_29: [symbol; 3] = [0xEF, 0xBA, 0x8A];
static S_0_30: [symbol; 3] = [0xEF, 0xBA, 0x8B];
static S_0_31: [symbol; 3] = [0xEF, 0xBA, 0x8C];
static S_0_32: [symbol; 3] = [0xEF, 0xBA, 0x8D];
static S_0_33: [symbol; 3] = [0xEF, 0xBA, 0x8E];
static S_0_34: [symbol; 3] = [0xEF, 0xBA, 0x8F];
static S_0_35: [symbol; 3] = [0xEF, 0xBA, 0x90];
static S_0_36: [symbol; 3] = [0xEF, 0xBA, 0x91];
static S_0_37: [symbol; 3] = [0xEF, 0xBA, 0x92];
static S_0_38: [symbol; 3] = [0xEF, 0xBA, 0x93];
static S_0_39: [symbol; 3] = [0xEF, 0xBA, 0x94];
static S_0_40: [symbol; 3] = [0xEF, 0xBA, 0x95];
static S_0_41: [symbol; 3] = [0xEF, 0xBA, 0x96];
static S_0_42: [symbol; 3] = [0xEF, 0xBA, 0x97];
static S_0_43: [symbol; 3] = [0xEF, 0xBA, 0x98];
static S_0_44: [symbol; 3] = [0xEF, 0xBA, 0x99];
static S_0_45: [symbol; 3] = [0xEF, 0xBA, 0x9A];
static S_0_46: [symbol; 3] = [0xEF, 0xBA, 0x9B];
static S_0_47: [symbol; 3] = [0xEF, 0xBA, 0x9C];
static S_0_48: [symbol; 3] = [0xEF, 0xBA, 0x9D];
static S_0_49: [symbol; 3] = [0xEF, 0xBA, 0x9E];
static S_0_50: [symbol; 3] = [0xEF, 0xBA, 0x9F];
static S_0_51: [symbol; 3] = [0xEF, 0xBA, 0xA0];
static S_0_52: [symbol; 3] = [0xEF, 0xBA, 0xA1];
static S_0_53: [symbol; 3] = [0xEF, 0xBA, 0xA2];
static S_0_54: [symbol; 3] = [0xEF, 0xBA, 0xA3];
static S_0_55: [symbol; 3] = [0xEF, 0xBA, 0xA4];
static S_0_56: [symbol; 3] = [0xEF, 0xBA, 0xA5];
static S_0_57: [symbol; 3] = [0xEF, 0xBA, 0xA6];
static S_0_58: [symbol; 3] = [0xEF, 0xBA, 0xA7];
static S_0_59: [symbol; 3] = [0xEF, 0xBA, 0xA8];
static S_0_60: [symbol; 3] = [0xEF, 0xBA, 0xA9];
static S_0_61: [symbol; 3] = [0xEF, 0xBA, 0xAA];
static S_0_62: [symbol; 3] = [0xEF, 0xBA, 0xAB];
static S_0_63: [symbol; 3] = [0xEF, 0xBA, 0xAC];
static S_0_64: [symbol; 3] = [0xEF, 0xBA, 0xAD];
static S_0_65: [symbol; 3] = [0xEF, 0xBA, 0xAE];
static S_0_66: [symbol; 3] = [0xEF, 0xBA, 0xAF];
static S_0_67: [symbol; 3] = [0xEF, 0xBA, 0xB0];
static S_0_68: [symbol; 3] = [0xEF, 0xBA, 0xB1];
static S_0_69: [symbol; 3] = [0xEF, 0xBA, 0xB2];
static S_0_70: [symbol; 3] = [0xEF, 0xBA, 0xB3];
static S_0_71: [symbol; 3] = [0xEF, 0xBA, 0xB4];
static S_0_72: [symbol; 3] = [0xEF, 0xBA, 0xB5];
static S_0_73: [symbol; 3] = [0xEF, 0xBA, 0xB6];
static S_0_74: [symbol; 3] = [0xEF, 0xBA, 0xB7];
static S_0_75: [symbol; 3] = [0xEF, 0xBA, 0xB8];
static S_0_76: [symbol; 3] = [0xEF, 0xBA, 0xB9];
static S_0_77: [symbol; 3] = [0xEF, 0xBA, 0xBA];
static S_0_78: [symbol; 3] = [0xEF, 0xBA, 0xBB];
static S_0_79: [symbol; 3] = [0xEF, 0xBA, 0xBC];
static S_0_80: [symbol; 3] = [0xEF, 0xBA, 0xBD];
static S_0_81: [symbol; 3] = [0xEF, 0xBA, 0xBE];
static S_0_82: [symbol; 3] = [0xEF, 0xBA, 0xBF];
static S_0_83: [symbol; 3] = [0xEF, 0xBB, 0x80];
static S_0_84: [symbol; 3] = [0xEF, 0xBB, 0x81];
static S_0_85: [symbol; 3] = [0xEF, 0xBB, 0x82];
static S_0_86: [symbol; 3] = [0xEF, 0xBB, 0x83];
static S_0_87: [symbol; 3] = [0xEF, 0xBB, 0x84];
static S_0_88: [symbol; 3] = [0xEF, 0xBB, 0x85];
static S_0_89: [symbol; 3] = [0xEF, 0xBB, 0x86];
static S_0_90: [symbol; 3] = [0xEF, 0xBB, 0x87];
static S_0_91: [symbol; 3] = [0xEF, 0xBB, 0x88];
static S_0_92: [symbol; 3] = [0xEF, 0xBB, 0x89];
static S_0_93: [symbol; 3] = [0xEF, 0xBB, 0x8A];
static S_0_94: [symbol; 3] = [0xEF, 0xBB, 0x8B];
static S_0_95: [symbol; 3] = [0xEF, 0xBB, 0x8C];
static S_0_96: [symbol; 3] = [0xEF, 0xBB, 0x8D];
static S_0_97: [symbol; 3] = [0xEF, 0xBB, 0x8E];
static S_0_98: [symbol; 3] = [0xEF, 0xBB, 0x8F];
static S_0_99: [symbol; 3] = [0xEF, 0xBB, 0x90];
static S_0_100: [symbol; 3] = [0xEF, 0xBB, 0x91];
static S_0_101: [symbol; 3] = [0xEF, 0xBB, 0x92];
static S_0_102: [symbol; 3] = [0xEF, 0xBB, 0x93];
static S_0_103: [symbol; 3] = [0xEF, 0xBB, 0x94];
static S_0_104: [symbol; 3] = [0xEF, 0xBB, 0x95];
static S_0_105: [symbol; 3] = [0xEF, 0xBB, 0x96];
static S_0_106: [symbol; 3] = [0xEF, 0xBB, 0x97];
static S_0_107: [symbol; 3] = [0xEF, 0xBB, 0x98];
static S_0_108: [symbol; 3] = [0xEF, 0xBB, 0x99];
static S_0_109: [symbol; 3] = [0xEF, 0xBB, 0x9A];
static S_0_110: [symbol; 3] = [0xEF, 0xBB, 0x9B];
static S_0_111: [symbol; 3] = [0xEF, 0xBB, 0x9C];
static S_0_112: [symbol; 3] = [0xEF, 0xBB, 0x9D];
static S_0_113: [symbol; 3] = [0xEF, 0xBB, 0x9E];
static S_0_114: [symbol; 3] = [0xEF, 0xBB, 0x9F];
static S_0_115: [symbol; 3] = [0xEF, 0xBB, 0xA0];
static S_0_116: [symbol; 3] = [0xEF, 0xBB, 0xA1];
static S_0_117: [symbol; 3] = [0xEF, 0xBB, 0xA2];
static S_0_118: [symbol; 3] = [0xEF, 0xBB, 0xA3];
static S_0_119: [symbol; 3] = [0xEF, 0xBB, 0xA4];
static S_0_120: [symbol; 3] = [0xEF, 0xBB, 0xA5];
static S_0_121: [symbol; 3] = [0xEF, 0xBB, 0xA6];
static S_0_122: [symbol; 3] = [0xEF, 0xBB, 0xA7];
static S_0_123: [symbol; 3] = [0xEF, 0xBB, 0xA8];
static S_0_124: [symbol; 3] = [0xEF, 0xBB, 0xA9];
static S_0_125: [symbol; 3] = [0xEF, 0xBB, 0xAA];
static S_0_126: [symbol; 3] = [0xEF, 0xBB, 0xAB];
static S_0_127: [symbol; 3] = [0xEF, 0xBB, 0xAC];
static S_0_128: [symbol; 3] = [0xEF, 0xBB, 0xAD];
static S_0_129: [symbol; 3] = [0xEF, 0xBB, 0xAE];
static S_0_130: [symbol; 3] = [0xEF, 0xBB, 0xAF];
static S_0_131: [symbol; 3] = [0xEF, 0xBB, 0xB0];
static S_0_132: [symbol; 3] = [0xEF, 0xBB, 0xB1];
static S_0_133: [symbol; 3] = [0xEF, 0xBB, 0xB2];
static S_0_134: [symbol; 3] = [0xEF, 0xBB, 0xB3];
static S_0_135: [symbol; 3] = [0xEF, 0xBB, 0xB4];
static S_0_136: [symbol; 3] = [0xEF, 0xBB, 0xB5];
static S_0_137: [symbol; 3] = [0xEF, 0xBB, 0xB6];
static S_0_138: [symbol; 3] = [0xEF, 0xBB, 0xB7];
static S_0_139: [symbol; 3] = [0xEF, 0xBB, 0xB8];
static S_0_140: [symbol; 3] = [0xEF, 0xBB, 0xB9];
static S_0_141: [symbol; 3] = [0xEF, 0xBB, 0xBA];
static S_0_142: [symbol; 3] = [0xEF, 0xBB, 0xBB];
static S_0_143: [symbol; 3] = [0xEF, 0xBB, 0xBC];

static A_0: [among; 144] = [
    among { s_size: 2, s: S_0_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_0_1.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_0_2.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_0_3.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_0_4.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_0_5.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_0_6.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_0_7.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_0_8.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_0_9.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 2, s: S_0_10.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 2, s: S_0_11.as_ptr(), substring_i: -1, result: 4, function: None },
    among { s_size: 2, s: S_0_12.as_ptr(), substring_i: -1, result: 5, function: None },
    among { s_size: 2, s: S_0_13.as_ptr(), substring_i: -1, result: 6, function: None },
    among { s_size: 2, s: S_0_14.as_ptr(), substring_i: -1, result: 7, function: None },
    among { s_size: 2, s: S_0_15.as_ptr(), substring_i: -1, result: 8, function: None },
    among { s_size: 2, s: S_0_16.as_ptr(), substring_i: -1, result: 9, function: None },
    among { s_size: 2, s: S_0_17.as_ptr(), substring_i: -1, result: 10, function: None },
    among { s_size: 2, s: S_0_18.as_ptr(), substring_i: -1, result: 11, function: None },
    among { s_size: 3, s: S_0_19.as_ptr(), substring_i: -1, result: 12, function: None },
    among { s_size: 3, s: S_0_20.as_ptr(), substring_i: -1, result: 16, function: None },
    among { s_size: 3, s: S_0_21.as_ptr(), substring_i: -1, result: 16, function: None },
    among { s_size: 3, s: S_0_22.as_ptr(), substring_i: -1, result: 13, function: None },
    among { s_size: 3, s: S_0_23.as_ptr(), substring_i: -1, result: 13, function: None },
    among { s_size: 3, s: S_0_24.as_ptr(), substring_i: -1, result: 17, function: None },
    among { s_size: 3, s: S_0_25.as_ptr(), substring_i: -1, result: 17, function: None },
    among { s_size: 3, s: S_0_26.as_ptr(), substring_i: -1, result: 14, function: None },
    among { s_size: 3, s: S_0_27.as_ptr(), substring_i: -1, result: 14, function: None },
    among { s_size: 3, s: S_0_28.as_ptr(), substring_i: -1, result: 15, function: None },
    among { s_size: 3, s: S_0_29.as_ptr(), substring_i: -1, result: 15, function: None },
    among { s_size: 3, s: S_0_30.as_ptr(), substring_i: -1, result: 15, function: None },
    among { s_size: 3, s: S_0_31.as_ptr(), substring_i: -1, result: 15, function: None },
    among { s_size: 3, s: S_0_32.as_ptr(), substring_i: -1, result: 18, function: None },
    among { s_size: 3, s: S_0_33.as_ptr(), substring_i: -1, result: 18, function: None },
    among { s_size: 3, s: S_0_34.as_ptr(), substring_i: -1, result: 19, function: None },
    among { s_size: 3, s: S_0_35.as_ptr(), substring_i: -1, result: 19, function: None },
    among { s_size: 3, s: S_0_36.as_ptr(), substring_i: -1, result: 19, function: None },
    among { s_size: 3, s: S_0_37.as_ptr(), substring_i: -1, result: 19, function: None },
    among { s_size: 3, s: S_0_38.as_ptr(), substring_i: -1, result: 20, function: None },
    among { s_size: 3, s: S_0_39.as_ptr(), substring_i: -1, result: 20, function: None },
    among { s_size: 3, s: S_0_40.as_ptr(), substring_i: -1, result: 21, function: None },
    among { s_size: 3, s: S_0_41.as_ptr(), substring_i: -1, result: 21, function: None },
    among { s_size: 3, s: S_0_42.as_ptr(), substring_i: -1, result: 21, function: None },
    among { s_size: 3, s: S_0_43.as_ptr(), substring_i: -1, result: 21, function: None },
    among { s_size: 3, s: S_0_44.as_ptr(), substring_i: -1, result: 22, function: None },
    among { s_size: 3, s: S_0_45.as_ptr(), substring_i: -1, result: 22, function: None },
    among { s_size: 3, s: S_0_46.as_ptr(), substring_i: -1, result: 22, function: None },
    among { s_size: 3, s: S_0_47.as_ptr(), substring_i: -1, result: 22, function: None },
    among { s_size: 3, s: S_0_48.as_ptr(), substring_i: -1, result: 23, function: None },
    among { s_size: 3, s: S_0_49.as_ptr(), substring_i: -1, result: 23, function: None },
    among { s_size: 3, s: S_0_50.as_ptr(), substring_i: -1, result: 23, function: None },
    among { s_size: 3, s: S_0_51.as_ptr(), substring_i: -1, result: 23, function: None },
    among { s_size: 3, s: S_0_52.as_ptr(), substring_i: -1, result: 24, function: None },
    among { s_size: 3, s: S_0_53.as_ptr(), substring_i: -1, result: 24, function: None },
    among { s_size: 3, s: S_0_54.as_ptr(), substring_i: -1, result: 24, function: None },
    among { s_size: 3, s: S_0_55.as_ptr(), substring_i: -1, result: 24, function: None },
    among { s_size: 3, s: S_0_56.as_ptr(), substring_i: -1, result: 25, function: None },
    among { s_size: 3, s: S_0_57.as_ptr(), substring_i: -1, result: 25, function: None },
    among { s_size: 3, s: S_0_58.as_ptr(), substring_i: -1, result: 25, function: None },
    among { s_size: 3, s: S_0_59.as_ptr(), substring_i: -1, result: 25, function: None },
    among { s_size: 3, s: S_0_60.as_ptr(), substring_i: -1, result: 26, function: None },
    among { s_size: 3, s: S_0_61.as_ptr(), substring_i: -1, result: 26, function: None },
    among { s_size: 3, s: S_0_62.as_ptr(), substring_i: -1, result: 27, function: None },
    among { s_size: 3, s: S_0_63.as_ptr(), substring_i: -1, result: 27, function: None },
    among { s_size: 3, s: S_0_64.as_ptr(), substring_i: -1, result: 28, function: None },
    among { s_size: 3, s: S_0_65.as_ptr(), substring_i: -1, result: 28, function: None },
    among { s_size: 3, s: S_0_66.as_ptr(), substring_i: -1, result: 29, function: None },
    among { s_size: 3, s: S_0_67.as_ptr(), substring_i: -1, result: 29, function: None },
    among { s_size: 3, s: S_0_68.as_ptr(), substring_i: -1, result: 30, function: None },
    among { s_size: 3, s: S_0_69.as_ptr(), substring_i: -1, result: 30, function: None },
    among { s_size: 3, s: S_0_70.as_ptr(), substring_i: -1, result: 30, function: None },
    among { s_size: 3, s: S_0_71.as_ptr(), substring_i: -1, result: 30, function: None },
    among { s_size: 3, s: S_0_72.as_ptr(), substring_i: -1, result: 31, function: None },
    among { s_size: 3, s: S_0_73.as_ptr(), substring_i: -1, result: 31, function: None },
    among { s_size: 3, s: S_0_74.as_ptr(), substring_i: -1, result: 31, function: None },
    among { s_size: 3, s: S_0_75.as_ptr(), substring_i: -1, result: 31, function: None },
    among { s_size: 3, s: S_0_76.as_ptr(), substring_i: -1, result: 32, function: None },
    among { s_size: 3, s: S_0_77.as_ptr(), substring_i: -1, result: 32, function: None },
    among { s_size: 3, s: S_0_78.as_ptr(), substring_i: -1, result: 32, function: None },
    among { s_size: 3, s: S_0_79.as_ptr(), substring_i: -1, result: 32, function: None },
    among { s_size: 3, s: S_0_80.as_ptr(), substring_i: -1, result: 33, function: None },
    among { s_size: 3, s: S_0_81.as_ptr(), substring_i: -1, result: 33, function: None },
    among { s_size: 3, s: S_0_82.as_ptr(), substring_i: -1, result: 33, function: None },
    among { s_size: 3, s: S_0_83.as_ptr(), substring_i: -1, result: 33, function: None },
    among { s_size: 3, s: S_0_84.as_ptr(), substring_i: -1, result: 34, function: None },
    among { s_size: 3, s: S_0_85.as_ptr(), substring_i: -1, result: 34, function: None },
    among { s_size: 3, s: S_0_86.as_ptr(), substring_i: -1, result: 34, function: None },
    among { s_size: 3, s: S_0_87.as_ptr(), substring_i: -1, result: 34, function: None },
    among { s_size: 3, s: S_0_88.as_ptr(), substring_i: -1, result: 35, function: None },
    among { s_size: 3, s: S_0_89.as_ptr(), substring_i: -1, result: 35, function: None },
    among { s_size: 3, s: S_0_90.as_ptr(), substring_i: -1, result: 35, function: None },
    among { s_size: 3, s: S_0_91.as_ptr(), substring_i: -1, result: 35, function: None },
    among { s_size: 3, s: S_0_92.as_ptr(), substring_i: -1, result: 36, function: None },
    among { s_size: 3, s: S_0_93.as_ptr(), substring_i: -1, result: 36, function: None },
    among { s_size: 3, s: S_0_94.as_ptr(), substring_i: -1, result: 36, function: None },
    among { s_size: 3, s: S_0_95.as_ptr(), substring_i: -1, result: 36, function: None },
    among { s_size: 3, s: S_0_96.as_ptr(), substring_i: -1, result: 37, function: None },
    among { s_size: 3, s: S_0_97.as_ptr(), substring_i: -1, result: 37, function: None },
    among { s_size: 3, s: S_0_98.as_ptr(), substring_i: -1, result: 37, function: None },
    among { s_size: 3, s: S_0_99.as_ptr(), substring_i: -1, result: 37, function: None },
    among { s_size: 3, s: S_0_100.as_ptr(), substring_i: -1, result: 38, function: None },
    among { s_size: 3, s: S_0_101.as_ptr(), substring_i: -1, result: 38, function: None },
    among { s_size: 3, s: S_0_102.as_ptr(), substring_i: -1, result: 38, function: None },
    among { s_size: 3, s: S_0_103.as_ptr(), substring_i: -1, result: 38, function: None },
    among { s_size: 3, s: S_0_104.as_ptr(), substring_i: -1, result: 39, function: None },
    among { s_size: 3, s: S_0_105.as_ptr(), substring_i: -1, result: 39, function: None },
    among { s_size: 3, s: S_0_106.as_ptr(), substring_i: -1, result: 39, function: None },
    among { s_size: 3, s: S_0_107.as_ptr(), substring_i: -1, result: 39, function: None },
    among { s_size: 3, s: S_0_108.as_ptr(), substring_i: -1, result: 40, function: None },
    among { s_size: 3, s: S_0_109.as_ptr(), substring_i: -1, result: 40, function: None },
    among { s_size: 3, s: S_0_110.as_ptr(), substring_i: -1, result: 40, function: None },
    among { s_size: 3, s: S_0_111.as_ptr(), substring_i: -1, result: 40, function: None },
    among { s_size: 3, s: S_0_112.as_ptr(), substring_i: -1, result: 41, function: None },
    among { s_size: 3, s: S_0_113.as_ptr(), substring_i: -1, result: 41, function: None },
    among { s_size: 3, s: S_0_114.as_ptr(), substring_i: -1, result: 41, function: None },
    among { s_size: 3, s: S_0_115.as_ptr(), substring_i: -1, result: 41, function: None },
    among { s_size: 3, s: S_0_116.as_ptr(), substring_i: -1, result: 42, function: None },
    among { s_size: 3, s: S_0_117.as_ptr(), substring_i: -1, result: 42, function: None },
    among { s_size: 3, s: S_0_118.as_ptr(), substring_i: -1, result: 42, function: None },
    among { s_size: 3, s: S_0_119.as_ptr(), substring_i: -1, result: 42, function: None },
    among { s_size: 3, s: S_0_120.as_ptr(), substring_i: -1, result: 43, function: None },
    among { s_size: 3, s: S_0_121.as_ptr(), substring_i: -1, result: 43, function: None },
    among { s_size: 3, s: S_0_122.as_ptr(), substring_i: -1, result: 43, function: None },
    among { s_size: 3, s: S_0_123.as_ptr(), substring_i: -1, result: 43, function: None },
    among { s_size: 3, s: S_0_124.as_ptr(), substring_i: -1, result: 44, function: None },
    among { s_size: 3, s: S_0_125.as_ptr(), substring_i: -1, result: 44, function: None },
    among { s_size: 3, s: S_0_126.as_ptr(), substring_i: -1, result: 44, function: None },
    among { s_size: 3, s: S_0_127.as_ptr(), substring_i: -1, result: 44, function: None },
    among { s_size: 3, s: S_0_128.as_ptr(), substring_i: -1, result: 45, function: None },
    among { s_size: 3, s: S_0_129.as_ptr(), substring_i: -1, result: 45, function: None },
    among { s_size: 3, s: S_0_130.as_ptr(), substring_i: -1, result: 46, function: None },
    among { s_size: 3, s: S_0_131.as_ptr(), substring_i: -1, result: 46, function: None },
    among { s_size: 3, s: S_0_132.as_ptr(), substring_i: -1, result: 47, function: None },
    among { s_size: 3, s: S_0_133.as_ptr(), substring_i: -1, result: 47, function: None },
    among { s_size: 3, s: S_0_134.as_ptr(), substring_i: -1, result: 47, function: None },
    among { s_size: 3, s: S_0_135.as_ptr(), substring_i: -1, result: 47, function: None },
    among { s_size: 3, s: S_0_136.as_ptr(), substring_i: -1, result: 51, function: None },
    among { s_size: 3, s: S_0_137.as_ptr(), substring_i: -1, result: 51, function: None },
    among { s_size: 3, s: S_0_138.as_ptr(), substring_i: -1, result: 49, function: None },
    among { s_size: 3, s: S_0_139.as_ptr(), substring_i: -1, result: 49, function: None },
    among { s_size: 3, s: S_0_140.as_ptr(), substring_i: -1, result: 50, function: None },
    among { s_size: 3, s: S_0_141.as_ptr(), substring_i: -1, result: 50, function: None },
    among { s_size: 3, s: S_0_142.as_ptr(), substring_i: -1, result: 48, function: None },
    among { s_size: 3, s: S_0_143.as_ptr(), substring_i: -1, result: 48, function: None },
];

static S_1_0: [symbol; 2] = [0xD8, 0xA2];
static S_1_1: [symbol; 2] = [0xD8, 0xA3];
static S_1_2: [symbol; 2] = [0xD8, 0xA4];
static S_1_3: [symbol; 2] = [0xD8, 0xA5];
static S_1_4: [symbol; 2] = [0xD8, 0xA6];

static A_1: [among; 5] = [
    among { s_size: 2, s: S_1_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_1_1.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_1_2.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_1_3.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_1_4.as_ptr(), substring_i: -1, result: 1, function: None },
];

static S_2_0: [symbol; 2] = [0xD8, 0xA2];
static S_2_1: [symbol; 2] = [0xD8, 0xA3];
static S_2_2: [symbol; 2] = [0xD8, 0xA4];
static S_2_3: [symbol; 2] = [0xD8, 0xA5];
static S_2_4: [symbol; 2] = [0xD8, 0xA6];

static A_2: [among; 5] = [
    among { s_size: 2, s: S_2_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_2_1.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_2_2.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 2, s: S_2_3.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_2_4.as_ptr(), substring_i: -1, result: 3, function: None },
];

static S_3_0: [symbol; 4] = [0xD8, 0xA7, 0xD9, 0x84];
static S_3_1: [symbol; 6] = [0xD8, 0xA8, 0xD8, 0xA7, 0xD9, 0x84];
static S_3_2: [symbol; 6] = [0xD9, 0x83, 0xD8, 0xA7, 0xD9, 0x84];
static S_3_3: [symbol; 4] = [0xD9, 0x84, 0xD9, 0x84];

static A_3: [among; 4] = [
    among { s_size: 4, s: S_3_0.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 6, s: S_3_1.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_3_2.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_3_3.as_ptr(), substring_i: -1, result: 2, function: None },
];

static S_4_0: [symbol; 4] = [0xD8, 0xA3, 0xD8, 0xA2];
static S_4_1: [symbol; 4] = [0xD8, 0xA3, 0xD8, 0xA3];
static S_4_2: [symbol; 4] = [0xD8, 0xA3, 0xD8, 0xA4];
static S_4_3: [symbol; 4] = [0xD8, 0xA3, 0xD8, 0xA5];
static S_4_4: [symbol; 4] = [0xD8, 0xA3, 0xD8, 0xA7];

static A_4: [among; 5] = [
    among { s_size: 4, s: S_4_0.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 4, s: S_4_1.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_4_2.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_4_3.as_ptr(), substring_i: -1, result: 4, function: None },
    among { s_size: 4, s: S_4_4.as_ptr(), substring_i: -1, result: 3, function: None },
];

static S_5_0: [symbol; 2] = [0xD9, 0x81];
static S_5_1: [symbol; 2] = [0xD9, 0x88];

static A_5: [among; 2] = [
    among { s_size: 2, s: S_5_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_5_1.as_ptr(), substring_i: -1, result: 1, function: None },
];

static S_6_0: [symbol; 4] = [0xD8, 0xA7, 0xD9, 0x84];
static S_6_1: [symbol; 6] = [0xD8, 0xA8, 0xD8, 0xA7, 0xD9, 0x84];
static S_6_2: [symbol; 6] = [0xD9, 0x83, 0xD8, 0xA7, 0xD9, 0x84];
static S_6_3: [symbol; 4] = [0xD9, 0x84, 0xD9, 0x84];

static A_6: [among; 4] = [
    among { s_size: 4, s: S_6_0.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 6, s: S_6_1.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_6_2.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_6_3.as_ptr(), substring_i: -1, result: 2, function: None },
];

static S_7_0: [symbol; 2] = [0xD8, 0xA8];
static S_7_1: [symbol; 4] = [0xD8, 0xA8, 0xD8, 0xA7];
static S_7_2: [symbol; 4] = [0xD8, 0xA8, 0xD8, 0xA8];
static S_7_3: [symbol; 4] = [0xD9, 0x83, 0xD9, 0x83];

static A_7: [among; 4] = [
    among { s_size: 2, s: S_7_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_7_1.as_ptr(), substring_i: 0, result: -1, function: None },
    among { s_size: 4, s: S_7_2.as_ptr(), substring_i: 0, result: 2, function: None },
    among { s_size: 4, s: S_7_3.as_ptr(), substring_i: -1, result: 3, function: None },
];

static S_8_0: [symbol; 4] = [0xD8, 0xB3, 0xD8, 0xA3];
static S_8_1: [symbol; 4] = [0xD8, 0xB3, 0xD8, 0xAA];
static S_8_2: [symbol; 4] = [0xD8, 0xB3, 0xD9, 0x86];
static S_8_3: [symbol; 4] = [0xD8, 0xB3, 0xD9, 0x8A];

static A_8: [among; 4] = [
    among { s_size: 4, s: S_8_0.as_ptr(), substring_i: -1, result: 4, function: None },
    among { s_size: 4, s: S_8_1.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 4, s: S_8_2.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 4, s: S_8_3.as_ptr(), substring_i: -1, result: 1, function: None },
];

static S_9_0: [symbol; 6] = [0xD8, 0xAA, 0xD8, 0xB3, 0xD8, 0xAA];
static S_9_1: [symbol; 6] = [0xD9, 0x86, 0xD8, 0xB3, 0xD8, 0xAA];
static S_9_2: [symbol; 6] = [0xD9, 0x8A, 0xD8, 0xB3, 0xD8, 0xAA];

static A_9: [among; 3] = [
    among { s_size: 6, s: S_9_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_9_1.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_9_2.as_ptr(), substring_i: -1, result: 1, function: None },
];

static S_10_0: [symbol; 2] = [0xD9, 0x83];
static S_10_1: [symbol; 4] = [0xD9, 0x83, 0xD9, 0x85];
static S_10_2: [symbol; 4] = [0xD9, 0x87, 0xD9, 0x85];
static S_10_3: [symbol; 4] = [0xD9, 0x87, 0xD9, 0x86];
static S_10_4: [symbol; 2] = [0xD9, 0x87];
static S_10_5: [symbol; 2] = [0xD9, 0x8A];
static S_10_6: [symbol; 6] = [0xD9, 0x83, 0xD9, 0x85, 0xD8, 0xA7];
static S_10_7: [symbol; 6] = [0xD9, 0x87, 0xD9, 0x85, 0xD8, 0xA7];
static S_10_8: [symbol; 4] = [0xD9, 0x86, 0xD8, 0xA7];
static S_10_9: [symbol; 4] = [0xD9, 0x87, 0xD8, 0xA7];

static A_10: [among; 10] = [
    among { s_size: 2, s: S_10_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_10_1.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 4, s: S_10_2.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 4, s: S_10_3.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 2, s: S_10_4.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_10_5.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_10_6.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 6, s: S_10_7.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 4, s: S_10_8.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 4, s: S_10_9.as_ptr(), substring_i: -1, result: 2, function: None },
];

static S_11_0: [symbol; 2] = [0xD9, 0x86];

static A_11: [among; 1] = [
    among { s_size: 2, s: S_11_0.as_ptr(), substring_i: -1, result: 1, function: None },
];

static S_12_0: [symbol; 2] = [0xD9, 0x88];
static S_12_1: [symbol; 2] = [0xD9, 0x8A];
static S_12_2: [symbol; 2] = [0xD8, 0xA7];

static A_12: [among; 3] = [
    among { s_size: 2, s: S_12_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_12_1.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_12_2.as_ptr(), substring_i: -1, result: 1, function: None },
];

static S_13_0: [symbol; 4] = [0xD8, 0xA7, 0xD8, 0xAA];

static A_13: [among; 1] = [
    among { s_size: 4, s: S_13_0.as_ptr(), substring_i: -1, result: 1, function: None },
];

static S_14_0: [symbol; 2] = [0xD8, 0xAA];

static A_14: [among; 1] = [
    among { s_size: 2, s: S_14_0.as_ptr(), substring_i: -1, result: 1, function: None },
];

static S_15_0: [symbol; 2] = [0xD8, 0xA9];

static A_15: [among; 1] = [
    among { s_size: 2, s: S_15_0.as_ptr(), substring_i: -1, result: 1, function: None },
];

static S_16_0: [symbol; 2] = [0xD9, 0x8A];

static A_16: [among; 1] = [
    among { s_size: 2, s: S_16_0.as_ptr(), substring_i: -1, result: 1, function: None },
];

static S_17_0: [symbol; 2] = [0xD9, 0x83];
static S_17_1: [symbol; 4] = [0xD9, 0x83, 0xD9, 0x85];
static S_17_2: [symbol; 4] = [0xD9, 0x87, 0xD9, 0x85];
static S_17_3: [symbol; 4] = [0xD9, 0x83, 0xD9, 0x86];
static S_17_4: [symbol; 4] = [0xD9, 0x87, 0xD9, 0x86];
static S_17_5: [symbol; 2] = [0xD9, 0x87];
static S_17_6: [symbol; 6] = [0xD9, 0x83, 0xD9, 0x85, 0xD9, 0x88];
static S_17_7: [symbol; 4] = [0xD9, 0x86, 0xD9, 0x8A];
static S_17_8: [symbol; 6] = [0xD9, 0x83, 0xD9, 0x85, 0xD8, 0xA7];
static S_17_9: [symbol; 6] = [0xD9, 0x87, 0xD9, 0x85, 0xD8, 0xA7];
static S_17_10: [symbol; 4] = [0xD9, 0x86, 0xD8, 0xA7];
static S_17_11: [symbol; 4] = [0xD9, 0x87, 0xD8, 0xA7];

static A_17: [among; 12] = [
    among { s_size: 2, s: S_17_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_17_1.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 4, s: S_17_2.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 4, s: S_17_3.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 4, s: S_17_4.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 2, s: S_17_5.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_17_6.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 4, s: S_17_7.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 6, s: S_17_8.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 6, s: S_17_9.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 4, s: S_17_10.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 4, s: S_17_11.as_ptr(), substring_i: -1, result: 2, function: None },
];

static S_18_0: [symbol; 2] = [0xD9, 0x86];
static S_18_1: [symbol; 4] = [0xD9, 0x88, 0xD9, 0x86];
static S_18_2: [symbol; 4] = [0xD9, 0x8A, 0xD9, 0x86];
static S_18_3: [symbol; 4] = [0xD8, 0xA7, 0xD9, 0x86];
static S_18_4: [symbol; 4] = [0xD8, 0xAA, 0xD9, 0x86];
static S_18_5: [symbol; 2] = [0xD9, 0x8A];
static S_18_6: [symbol; 2] = [0xD8, 0xA7];
static S_18_7: [symbol; 6] = [0xD8, 0xAA, 0xD9, 0x85, 0xD8, 0xA7];
static S_18_8: [symbol; 4] = [0xD9, 0x86, 0xD8, 0xA7];
static S_18_9: [symbol; 4] = [0xD8, 0xAA, 0xD8, 0xA7];
static S_18_10: [symbol; 2] = [0xD8, 0xAA];

static A_18: [among; 11] = [
    among { s_size: 2, s: S_18_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_18_1.as_ptr(), substring_i: 0, result: 3, function: None },
    among { s_size: 4, s: S_18_2.as_ptr(), substring_i: 0, result: 3, function: None },
    among { s_size: 4, s: S_18_3.as_ptr(), substring_i: 0, result: 3, function: None },
    among { s_size: 4, s: S_18_4.as_ptr(), substring_i: 0, result: 2, function: None },
    among { s_size: 2, s: S_18_5.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_18_6.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_18_7.as_ptr(), substring_i: 6, result: 4, function: None },
    among { s_size: 4, s: S_18_8.as_ptr(), substring_i: 6, result: 2, function: None },
    among { s_size: 4, s: S_18_9.as_ptr(), substring_i: 6, result: 2, function: None },
    among { s_size: 2, s: S_18_10.as_ptr(), substring_i: -1, result: 1, function: None },
];

static S_19_0: [symbol; 4] = [0xD8, 0xAA, 0xD9, 0x85];
static S_19_1: [symbol; 4] = [0xD9, 0x88, 0xD8, 0xA7];

static A_19: [among; 2] = [
    among { s_size: 4, s: S_19_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_19_1.as_ptr(), substring_i: -1, result: 1, function: None },
];

static S_20_0: [symbol; 2] = [0xD9, 0x88];
static S_20_1: [symbol; 6] = [0xD8, 0xAA, 0xD9, 0x85, 0xD9, 0x88];

static A_20: [among; 2] = [
    among { s_size: 2, s: S_20_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_20_1.as_ptr(), substring_i: 0, result: 2, function: None },
];

static S_21_0: [symbol; 2] = [0xD9, 0x89];

static A_21: [among; 1] = [
    among { s_size: 2, s: S_21_0.as_ptr(), substring_i: -1, result: 1, function: None },
];

// ---------------------------------------------------------------------------
// literal strings used by slice_from_s / eq_s
// ---------------------------------------------------------------------------

static S_0: [symbol; 1] = [b'0'];
static S_1: [symbol; 1] = [b'1'];
static S_2: [symbol; 1] = [b'2'];
static S_3: [symbol; 1] = [b'3'];
static S_4: [symbol; 1] = [b'4'];
static S_5: [symbol; 1] = [b'5'];
static S_6: [symbol; 1] = [b'6'];
static S_7: [symbol; 1] = [b'7'];
static S_8: [symbol; 1] = [b'8'];
static S_9: [symbol; 1] = [b'9'];
static S_10: [symbol; 2] = [0xD8, 0xA1];
static S_11: [symbol; 2] = [0xD8, 0xA3];
static S_12: [symbol; 2] = [0xD8, 0xA5];
static S_13: [symbol; 2] = [0xD8, 0xA6];
static S_14: [symbol; 2] = [0xD8, 0xA2];
static S_15: [symbol; 2] = [0xD8, 0xA4];
static S_16: [symbol; 2] = [0xD8, 0xA7];
static S_17: [symbol; 2] = [0xD8, 0xA8];
static S_18: [symbol; 2] = [0xD8, 0xA9];
static S_19: [symbol; 2] = [0xD8, 0xAA];
static S_20: [symbol; 2] = [0xD8, 0xAB];
static S_21: [symbol; 2] = [0xD8, 0xAC];
static S_22: [symbol; 2] = [0xD8, 0xAD];
static S_23: [symbol; 2] = [0xD8, 0xAE];
static S_24: [symbol; 2] = [0xD8, 0xAF];
static S_25: [symbol; 2] = [0xD8, 0xB0];
static S_26: [symbol; 2] = [0xD8, 0xB1];
static S_27: [symbol; 2] = [0xD8, 0xB2];
static S_28: [symbol; 2] = [0xD8, 0xB3];
static S_29: [symbol; 2] = [0xD8, 0xB4];
static S_30: [symbol; 2] = [0xD8, 0xB5];
static S_31: [symbol; 2] = [0xD8, 0xB6];
static S_32: [symbol; 2] = [0xD8, 0xB7];
static S_33: [symbol; 2] = [0xD8, 0xB8];
static S_34: [symbol; 2] = [0xD8, 0xB9];
static S_35: [symbol; 2] = [0xD8, 0xBA];
static S_36: [symbol; 2] = [0xD9, 0x81];
static S_37: [symbol; 2] = [0xD9, 0x82];
static S_38: [symbol; 2] = [0xD9, 0x83];
static S_39: [symbol; 2] = [0xD9, 0x84];
static S_40: [symbol; 2] = [0xD9, 0x85];
static S_41: [symbol; 2] = [0xD9, 0x86];
static S_42: [symbol; 2] = [0xD9, 0x87];
static S_43: [symbol; 2] = [0xD9, 0x88];
static S_44: [symbol; 2] = [0xD9, 0x89];
static S_45: [symbol; 2] = [0xD9, 0x8A];
static S_46: [symbol; 4] = [0xD9, 0x84, 0xD8, 0xA7];
static S_47: [symbol; 4] = [0xD9, 0x84, 0xD8, 0xA3];
static S_48: [symbol; 4] = [0xD9, 0x84, 0xD8, 0xA5];
static S_49: [symbol; 4] = [0xD9, 0x84, 0xD8, 0xA2];
static S_50: [symbol; 2] = [0xD8, 0xA1];
static S_51: [symbol; 2] = [0xD8, 0xA7];
static S_52: [symbol; 2] = [0xD9, 0x88];
static S_53: [symbol; 2] = [0xD9, 0x8A];
static S_54: [symbol; 2] = [0xD8, 0xA3];
static S_55: [symbol; 2] = [0xD8, 0xA2];
static S_56: [symbol; 2] = [0xD8, 0xA7];
static S_57: [symbol; 2] = [0xD8, 0xA5];
static S_58: [symbol; 2] = [0xD8, 0xA7];
static S_59: [symbol; 2] = [0xD8, 0xA8];
static S_60: [symbol; 2] = [0xD9, 0x83];
static S_61: [symbol; 2] = [0xD9, 0x8A];
static S_62: [symbol; 2] = [0xD8, 0xAA];
static S_63: [symbol; 2] = [0xD9, 0x86];
static S_64: [symbol; 2] = [0xD8, 0xA3];
static S_65: [symbol; 6] = [0xD8, 0xA7, 0xD8, 0xB3, 0xD8, 0xAA];
static S_66: [symbol; 2] = [0xD9, 0x8A];

// ---------------------------------------------------------------------------
// rules
// ---------------------------------------------------------------------------

unsafe fn r_Normalize_pre(z: *mut SN_env) -> c_int {
    let mut among_var;
    {
        let c1 = (*z).c;
        'outer: loop {
            let c2 = (*z).c;
            'lab1: {
                {
                    let c3 = (*z).c;
                    'lab3: {
                        (*z).bra = (*z).c;
                        among_var = find_among(z, A_0.as_ptr(), 144);
                        if among_var == 0 {
                            break 'lab3;
                        }
                        (*z).ket = (*z).c;
                        match among_var {
                            1 => {
                                let ret = slice_del(z);
                                if ret < 0 {
                                    return ret;
                                }
                            }
                            2 => {
                                let ret = slice_from_s(z, 1, S_0.as_ptr());
                                if ret < 0 {
                                    return ret;
                                }
                            }
                            3 => {
                                let ret = slice_from_s(z, 1, S_1.as_ptr());
                                if ret < 0 {
                                    return ret;
                                }
                            }
                            4 => {
                                let ret = slice_from_s(z, 1, S_2.as_ptr());
                                if ret < 0 {
                                    return ret;
                                }
                            }
                            5 => {
                                let ret = slice_from_s(z, 1, S_3.as_ptr());
                                if ret < 0 {
                                    return ret;
                                }
                            }
                            6 => {
                                let ret = slice_from_s(z, 1, S_4.as_ptr());
                                if ret < 0 {
                                    return ret;
                                }
                            }
                            7 => {
                                let ret = slice_from_s(z, 1, S_5.as_ptr());
                                if ret < 0 {
                                    return ret;
                                }
                            }
                            8 => {
                                let ret = slice_from_s(z, 1, S_6.as_ptr());
                                if ret < 0 {
                                    return ret;
                                }
                            }
                            9 => {
                                let ret = slice_from_s(z, 1, S_7.as_ptr());
                                if ret < 0 {
                                    return ret;
                                }
                            }
                            10 => {
                                let ret = slice_from_s(z, 1, S_8.as_ptr());
                                if ret < 0 {
                                    return ret;
                                }
                            }
                            11 => {
                                let ret = slice_from_s(z, 1, S_9.as_ptr());
                                if ret < 0 {
                                    return ret;
                                }
                            }
                            12 => {
                                let ret = slice_from_s(z, 2, S_10.as_ptr());
                                if ret < 0 {
                                    return ret;
                                }
                            }
                            13 => {
                                let ret = slice_from_s(z, 2, S_11.as_ptr());
                                if ret < 0 {
                                    return ret;
                                }
                            }
                            14 => {
                                let ret = slice_from_s(z, 2, S_12.as_ptr());
                                if ret < 0 {
                                    return ret;
                                }
                            }
                            15 => {
                                let ret = slice_from_s(z, 2, S_13.as_ptr());
                                if ret < 0 {
                                    return ret;
                                }
                            }
                            16 => {
                                let ret = slice_from_s(z, 2, S_14.as_ptr());
                                if ret < 0 {
                                    return ret;
                                }
                            }
                            17 => {
                                let ret = slice_from_s(z, 2, S_15.as_ptr());
                                if ret < 0 {
                                    return ret;
                                }
                            }
                            18 => {
                                let ret = slice_from_s(z, 2, S_16.as_ptr());
                                if ret < 0 {
                                    return ret;
                                }
                            }
                            19 => {
                                let ret = slice_from_s(z, 2, S_17.as_ptr());
                                if ret < 0 {
                                    return ret;
                                }
                            }
                            20 => {
                                let ret = slice_from_s(z, 2, S_18.as_ptr());
                                if ret < 0 {
                                    return ret;
                                }
                            }
                            21 => {
                                let ret = slice_from_s(z, 2, S_19.as_ptr());
                                if ret < 0 {
                                    return ret;
                                }
                            }
                            22 => {
                                let ret = slice_from_s(z, 2, S_20.as_ptr());
                                if ret < 0 {
                                    return ret;
                                }
                            }
                            23 => {
                                let ret = slice_from_s(z, 2, S_21.as_ptr());
                                if ret < 0 {
                                    return ret;
                                }
                            }
                            24 => {
                                let ret = slice_from_s(z, 2, S_22.as_ptr());
                                if ret < 0 {
                                    return ret;
                                }
                            }
                            25 => {
                                let ret = slice_from_s(z, 2, S_23.as_ptr());
                                if ret < 0 {
                                    return ret;
                                }
                            }
                            26 => {
                                let ret = slice_from_s(z, 2, S_24.as_ptr());
                                if ret < 0 {
                                    return ret;
                                }
                            }
                            27 => {
                                let ret = slice_from_s(z, 2, S_25.as_ptr());
                                if ret < 0 {
                                    return ret;
                                }
                            }
                            28 => {
                                let ret = slice_from_s(z, 2, S_26.as_ptr());
                                if ret < 0 {
                                    return ret;
                                }
                            }
                            29 => {
                                let ret = slice_from_s(z, 2, S_27.as_ptr());
                                if ret < 0 {
                                    return ret;
                                }
                            }
                            30 => {
                                let ret = slice_from_s(z, 2, S_28.as_ptr());
                                if ret < 0 {
                                    return ret;
                                }
                            }
                            31 => {
                                let ret = slice_from_s(z, 2, S_29.as_ptr());
                                if ret < 0 {
                                    return ret;
                                }
                            }
                            32 => {
                                let ret = slice_from_s(z, 2, S_30.as_ptr());
                                if ret < 0 {
                                    return ret;
                                }
                            }
                            33 => {
                                let ret = slice_from_s(z, 2, S_31.as_ptr());
                                if ret < 0 {
                                    return ret;
                                }
                            }
                            34 => {
                                let ret = slice_from_s(z, 2, S_32.as_ptr());
                                if ret < 0 {
                                    return ret;
                                }
                            }
                            35 => {
                                let ret = slice_from_s(z, 2, S_33.as_ptr());
                                if ret < 0 {
                                    return ret;
                                }
                            }
                            36 => {
                                let ret = slice_from_s(z, 2, S_34.as_ptr());
                                if ret < 0 {
                                    return ret;
                                }
                            }
                            37 => {
                                let ret = slice_from_s(z, 2, S_35.as_ptr());
                                if ret < 0 {
                                    return ret;
                                }
                            }
                            38 => {
                                let ret = slice_from_s(z, 2, S_36.as_ptr());
                                if ret < 0 {
                                    return ret;
                                }
                            }
                            39 => {
                                let ret = slice_from_s(z, 2, S_37.as_ptr());
                                if ret < 0 {
                                    return ret;
                                }
                            }
                            40 => {
                                let ret = slice_from_s(z, 2, S_38.as_ptr());
                                if ret < 0 {
                                    return ret;
                                }
                            }
                            41 => {
                                let ret = slice_from_s(z, 2, S_39.as_ptr());
                                if ret < 0 {
                                    return ret;
                                }
                            }
                            42 => {
                                let ret = slice_from_s(z, 2, S_40.as_ptr());
                                if ret < 0 {
                                    return ret;
                                }
                            }
                            43 => {
                                let ret = slice_from_s(z, 2, S_41.as_ptr());
                                if ret < 0 {
                                    return ret;
                                }
                            }
                            44 => {
                                let ret = slice_from_s(z, 2, S_42.as_ptr());
                                if ret < 0 {
                                    return ret;
                                }
                            }
                            45 => {
                                let ret = slice_from_s(z, 2, S_43.as_ptr());
                                if ret < 0 {
                                    return ret;
                                }
                            }
                            46 => {
                                let ret = slice_from_s(z, 2, S_44.as_ptr());
                                if ret < 0 {
                                    return ret;
                                }
                            }
                            47 => {
                                let ret = slice_from_s(z, 2, S_45.as_ptr());
                                if ret < 0 {
                                    return ret;
                                }
                            }
                            48 => {
                                let ret = slice_from_s(z, 4, S_46.as_ptr());
                                if ret < 0 {
                                    return ret;
                                }
                            }
                            49 => {
                                let ret = slice_from_s(z, 4, S_47.as_ptr());
                                if ret < 0 {
                                    return ret;
                                }
                            }
                            50 => {
                                let ret = slice_from_s(z, 4, S_48.as_ptr());
                                if ret < 0 {
                                    return ret;
                                }
                            }
                            51 => {
                                let ret = slice_from_s(z, 4, S_49.as_ptr());
                                if ret < 0 {
                                    return ret;
                                }
                            }
                            _ => {}
                        }
                        break 'lab1;
                    }
                    (*z).c = c3;
                    {
                        let ret = skip_utf8((*z).p, (*z).c, (*z).l, 1);
                        if ret < 0 {
                            (*z).c = c2;
                            break 'outer;
                        }
                        (*z).c = ret;
                    }
                }
            }
            continue;
        }
        (*z).c = c1;
    }
    1
}

unsafe fn r_Normalize_post(z: *mut SN_env) -> c_int {
    let mut among_var;
    {
        let c1 = (*z).c;
        (*z).lb = (*z).c;
        (*z).c = (*z).l;

        'lab0: {
            (*z).ket = (*z).c;
            if (*z).c - 1 <= (*z).lb
                || (*(*z).p.offset(((*z).c - 1) as isize) as c_int >> 5) != 5
                || ((124 >> (*(*z).p.offset(((*z).c - 1) as isize) as c_int & 0x1f)) & 1) == 0
            {
                break 'lab0;
            }
            if find_among_b(z, A_1.as_ptr(), 5) == 0 {
                break 'lab0;
            }
            (*z).bra = (*z).c;
            {
                let ret = slice_from_s(z, 2, S_50.as_ptr());
                if ret < 0 {
                    return ret;
                }
            }
            (*z).c = (*z).lb;
        }
        (*z).c = c1;
    }
    {
        let c2 = (*z).c;
        'outer: loop {
            let c3 = (*z).c;
            'lab3: {
                {
                    let c4 = (*z).c;
                    'lab4: {
                        (*z).bra = (*z).c;
                        if (*z).c + 1 >= (*z).l
                            || (*(*z).p.offset(((*z).c + 1) as isize) as c_int >> 5) != 5
                            || ((124 >> (*(*z).p.offset(((*z).c + 1) as isize) as c_int & 0x1f)) & 1) == 0
                        {
                            break 'lab4;
                        }
                        among_var = find_among(z, A_2.as_ptr(), 5);
                        if among_var == 0 {
                            break 'lab4;
                        }
                        (*z).ket = (*z).c;
                        match among_var {
                            1 => {
                                let ret = slice_from_s(z, 2, S_51.as_ptr());
                                if ret < 0 {
                                    return ret;
                                }
                            }
                            2 => {
                                let ret = slice_from_s(z, 2, S_52.as_ptr());
                                if ret < 0 {
                                    return ret;
                                }
                            }
                            3 => {
                                let ret = slice_from_s(z, 2, S_53.as_ptr());
                                if ret < 0 {
                                    return ret;
                                }
                            }
                            _ => {}
                        }
                        break 'lab3;
                    }
                    (*z).c = c4;
                    {
                        let ret = skip_utf8((*z).p, (*z).c, (*z).l, 1);
                        if ret < 0 {
                            (*z).c = c3;
                            break 'outer;
                        }
                        (*z).c = ret;
                    }
                }
            }
            continue;
        }
        (*z).c = c2;
    }
    1
}

unsafe fn r_Checks1(z: *mut SN_env) -> c_int {
    let among_var;
    (*z).bra = (*z).c;
    if (*z).c + 3 >= (*z).l
        || (*(*z).p.offset(((*z).c + 3) as isize) != 132 && *(*z).p.offset(((*z).c + 3) as isize) != 167)
    {
        return 0;
    }
    among_var = find_among(z, A_3.as_ptr(), 4);
    if among_var == 0 {
        return 0;
    }
    (*z).ket = (*z).c;
    match among_var {
        1 => {
            if len_utf8((*z).p) <= 4 {
                return 0;
            }
            *(*z).I.offset(2) = 1;
            *(*z).I.offset(1) = 0;
            *(*z).I.offset(0) = 1;
        }
        2 => {
            if len_utf8((*z).p) <= 3 {
                return 0;
            }
            *(*z).I.offset(2) = 1;
            *(*z).I.offset(1) = 0;
            *(*z).I.offset(0) = 1;
        }
        _ => {}
    }
    1
}

unsafe fn r_Prefix_Step1(z: *mut SN_env) -> c_int {
    let among_var;
    (*z).bra = (*z).c;
    if (*z).c + 3 >= (*z).l
        || (*(*z).p.offset(((*z).c + 3) as isize) as c_int >> 5) != 5
        || ((188 >> (*(*z).p.offset(((*z).c + 3) as isize) as c_int & 0x1f)) & 1) == 0
    {
        return 0;
    }
    among_var = find_among(z, A_4.as_ptr(), 5);
    if among_var == 0 {
        return 0;
    }
    (*z).ket = (*z).c;
    match among_var {
        1 => {
            if len_utf8((*z).p) <= 3 {
                return 0;
            }
            {
                let ret = slice_from_s(z, 2, S_54.as_ptr());
                if ret < 0 {
                    return ret;
                }
            }
        }
        2 => {
            if len_utf8((*z).p) <= 3 {
                return 0;
            }
            {
                let ret = slice_from_s(z, 2, S_55.as_ptr());
                if ret < 0 {
                    return ret;
                }
            }
        }
        3 => {
            if len_utf8((*z).p) <= 3 {
                return 0;
            }
            {
                let ret = slice_from_s(z, 2, S_56.as_ptr());
                if ret < 0 {
                    return ret;
                }
            }
        }
        4 => {
            if len_utf8((*z).p) <= 3 {
                return 0;
            }
            {
                let ret = slice_from_s(z, 2, S_57.as_ptr());
                if ret < 0 {
                    return ret;
                }
            }
        }
        _ => {}
    }
    1
}

unsafe fn r_Prefix_Step2(z: *mut SN_env) -> c_int {
    (*z).bra = (*z).c;
    if (*z).c + 1 >= (*z).l
        || (*(*z).p.offset(((*z).c + 1) as isize) != 129 && *(*z).p.offset(((*z).c + 1) as isize) != 136)
    {
        return 0;
    }
    if find_among(z, A_5.as_ptr(), 2) == 0 {
        return 0;
    }
    (*z).ket = (*z).c;
    if len_utf8((*z).p) <= 3 {
        return 0;
    }
    {
        let c1 = (*z).c;
        'lab0: {
            if eq_s(z, 2, S_58.as_ptr()) == 0 {
                break 'lab0;
            }
            return 0;
        }
        (*z).c = c1;
    }
    {
        let ret = slice_del(z);
        if ret < 0 {
            return ret;
        }
    }
    1
}

unsafe fn r_Prefix_Step3a_Noun(z: *mut SN_env) -> c_int {
    let among_var;
    (*z).bra = (*z).c;
    if (*z).c + 3 >= (*z).l
        || (*(*z).p.offset(((*z).c + 3) as isize) != 132 && *(*z).p.offset(((*z).c + 3) as isize) != 167)
    {
        return 0;
    }
    among_var = find_among(z, A_6.as_ptr(), 4);
    if among_var == 0 {
        return 0;
    }
    (*z).ket = (*z).c;
    match among_var {
        1 => {
            if len_utf8((*z).p) <= 5 {
                return 0;
            }
            {
                let ret = slice_del(z);
                if ret < 0 {
                    return ret;
                }
            }
        }
        2 => {
            if len_utf8((*z).p) <= 4 {
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

unsafe fn r_Prefix_Step3b_Noun(z: *mut SN_env) -> c_int {
    let among_var;
    (*z).bra = (*z).c;
    if (*z).c + 1 >= (*z).l
        || (*(*z).p.offset(((*z).c + 1) as isize) != 168 && *(*z).p.offset(((*z).c + 1) as isize) != 131)
    {
        return 0;
    }
    among_var = find_among(z, A_7.as_ptr(), 4);
    if among_var == 0 {
        return 0;
    }
    (*z).ket = (*z).c;
    match among_var {
        1 => {
            if len_utf8((*z).p) <= 3 {
                return 0;
            }
            {
                let ret = slice_del(z);
                if ret < 0 {
                    return ret;
                }
            }
        }
        2 => {
            if len_utf8((*z).p) <= 3 {
                return 0;
            }
            {
                let ret = slice_from_s(z, 2, S_59.as_ptr());
                if ret < 0 {
                    return ret;
                }
            }
        }
        3 => {
            if len_utf8((*z).p) <= 3 {
                return 0;
            }
            {
                let ret = slice_from_s(z, 2, S_60.as_ptr());
                if ret < 0 {
                    return ret;
                }
            }
        }
        _ => {}
    }
    1
}

unsafe fn r_Prefix_Step3_Verb(z: *mut SN_env) -> c_int {
    let among_var;
    (*z).bra = (*z).c;
    among_var = find_among(z, A_8.as_ptr(), 4);
    if among_var == 0 {
        return 0;
    }
    (*z).ket = (*z).c;
    match among_var {
        1 => {
            if len_utf8((*z).p) <= 4 {
                return 0;
            }
            {
                let ret = slice_from_s(z, 2, S_61.as_ptr());
                if ret < 0 {
                    return ret;
                }
            }
        }
        2 => {
            if len_utf8((*z).p) <= 4 {
                return 0;
            }
            {
                let ret = slice_from_s(z, 2, S_62.as_ptr());
                if ret < 0 {
                    return ret;
                }
            }
        }
        3 => {
            if len_utf8((*z).p) <= 4 {
                return 0;
            }
            {
                let ret = slice_from_s(z, 2, S_63.as_ptr());
                if ret < 0 {
                    return ret;
                }
            }
        }
        4 => {
            if len_utf8((*z).p) <= 4 {
                return 0;
            }
            {
                let ret = slice_from_s(z, 2, S_64.as_ptr());
                if ret < 0 {
                    return ret;
                }
            }
        }
        _ => {}
    }
    1
}

unsafe fn r_Prefix_Step4_Verb(z: *mut SN_env) -> c_int {
    (*z).bra = (*z).c;
    if (*z).c + 5 >= (*z).l || *(*z).p.offset(((*z).c + 5) as isize) != 170 {
        return 0;
    }
    if find_among(z, A_9.as_ptr(), 3) == 0 {
        return 0;
    }
    (*z).ket = (*z).c;
    if len_utf8((*z).p) <= 4 {
        return 0;
    }
    *(*z).I.offset(1) = 1;
    *(*z).I.offset(2) = 0;
    {
        let ret = slice_from_s(z, 6, S_65.as_ptr());
        if ret < 0 {
            return ret;
        }
    }
    1
}

unsafe fn r_Suffix_Noun_Step1a(z: *mut SN_env) -> c_int {
    let among_var;
    (*z).ket = (*z).c;
    among_var = find_among_b(z, A_10.as_ptr(), 10);
    if among_var == 0 {
        return 0;
    }
    (*z).bra = (*z).c;
    match among_var {
        1 => {
            if len_utf8((*z).p) < 4 {
                return 0;
            }
            {
                let ret = slice_del(z);
                if ret < 0 {
                    return ret;
                }
            }
        }
        2 => {
            if len_utf8((*z).p) < 5 {
                return 0;
            }
            {
                let ret = slice_del(z);
                if ret < 0 {
                    return ret;
                }
            }
        }
        3 => {
            if len_utf8((*z).p) < 6 {
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

unsafe fn r_Suffix_Noun_Step1b(z: *mut SN_env) -> c_int {
    (*z).ket = (*z).c;
    if (*z).c - 1 <= (*z).lb || *(*z).p.offset(((*z).c - 1) as isize) != 134 {
        return 0;
    }
    if find_among_b(z, A_11.as_ptr(), 1) == 0 {
        return 0;
    }
    (*z).bra = (*z).c;
    if len_utf8((*z).p) <= 5 {
        return 0;
    }
    {
        let ret = slice_del(z);
        if ret < 0 {
            return ret;
        }
    }
    1
}

unsafe fn r_Suffix_Noun_Step2a(z: *mut SN_env) -> c_int {
    (*z).ket = (*z).c;
    if find_among_b(z, A_12.as_ptr(), 3) == 0 {
        return 0;
    }
    (*z).bra = (*z).c;
    if len_utf8((*z).p) <= 4 {
        return 0;
    }
    {
        let ret = slice_del(z);
        if ret < 0 {
            return ret;
        }
    }
    1
}

unsafe fn r_Suffix_Noun_Step2b(z: *mut SN_env) -> c_int {
    (*z).ket = (*z).c;
    if (*z).c - 3 <= (*z).lb || *(*z).p.offset(((*z).c - 1) as isize) != 170 {
        return 0;
    }
    if find_among_b(z, A_13.as_ptr(), 1) == 0 {
        return 0;
    }
    (*z).bra = (*z).c;
    if len_utf8((*z).p) < 5 {
        return 0;
    }
    {
        let ret = slice_del(z);
        if ret < 0 {
            return ret;
        }
    }
    1
}

unsafe fn r_Suffix_Noun_Step2c1(z: *mut SN_env) -> c_int {
    (*z).ket = (*z).c;
    if (*z).c - 1 <= (*z).lb || *(*z).p.offset(((*z).c - 1) as isize) != 170 {
        return 0;
    }
    if find_among_b(z, A_14.as_ptr(), 1) == 0 {
        return 0;
    }
    (*z).bra = (*z).c;
    if len_utf8((*z).p) < 4 {
        return 0;
    }
    {
        let ret = slice_del(z);
        if ret < 0 {
            return ret;
        }
    }
    1
}

unsafe fn r_Suffix_Noun_Step2c2(z: *mut SN_env) -> c_int {
    (*z).ket = (*z).c;
    if (*z).c - 1 <= (*z).lb || *(*z).p.offset(((*z).c - 1) as isize) != 169 {
        return 0;
    }
    if find_among_b(z, A_15.as_ptr(), 1) == 0 {
        return 0;
    }
    (*z).bra = (*z).c;
    if len_utf8((*z).p) < 4 {
        return 0;
    }
    {
        let ret = slice_del(z);
        if ret < 0 {
            return ret;
        }
    }
    1
}

unsafe fn r_Suffix_Noun_Step3(z: *mut SN_env) -> c_int {
    (*z).ket = (*z).c;
    if (*z).c - 1 <= (*z).lb || *(*z).p.offset(((*z).c - 1) as isize) != 138 {
        return 0;
    }
    if find_among_b(z, A_16.as_ptr(), 1) == 0 {
        return 0;
    }
    (*z).bra = (*z).c;
    if len_utf8((*z).p) < 3 {
        return 0;
    }
    {
        let ret = slice_del(z);
        if ret < 0 {
            return ret;
        }
    }
    1
}

unsafe fn r_Suffix_Verb_Step1(z: *mut SN_env) -> c_int {
    let among_var;
    (*z).ket = (*z).c;
    among_var = find_among_b(z, A_17.as_ptr(), 12);
    if among_var == 0 {
        return 0;
    }
    (*z).bra = (*z).c;
    match among_var {
        1 => {
            if len_utf8((*z).p) < 4 {
                return 0;
            }
            {
                let ret = slice_del(z);
                if ret < 0 {
                    return ret;
                }
            }
        }
        2 => {
            if len_utf8((*z).p) < 5 {
                return 0;
            }
            {
                let ret = slice_del(z);
                if ret < 0 {
                    return ret;
                }
            }
        }
        3 => {
            if len_utf8((*z).p) < 6 {
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

unsafe fn r_Suffix_Verb_Step2a(z: *mut SN_env) -> c_int {
    let among_var;
    (*z).ket = (*z).c;
    among_var = find_among_b(z, A_18.as_ptr(), 11);
    if among_var == 0 {
        return 0;
    }
    (*z).bra = (*z).c;
    match among_var {
        1 => {
            if len_utf8((*z).p) < 4 {
                return 0;
            }
            {
                let ret = slice_del(z);
                if ret < 0 {
                    return ret;
                }
            }
        }
        2 => {
            if len_utf8((*z).p) < 5 {
                return 0;
            }
            {
                let ret = slice_del(z);
                if ret < 0 {
                    return ret;
                }
            }
        }
        3 => {
            if len_utf8((*z).p) <= 5 {
                return 0;
            }
            {
                let ret = slice_del(z);
                if ret < 0 {
                    return ret;
                }
            }
        }
        4 => {
            if len_utf8((*z).p) < 6 {
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

unsafe fn r_Suffix_Verb_Step2b(z: *mut SN_env) -> c_int {
    (*z).ket = (*z).c;
    if (*z).c - 3 <= (*z).lb
        || (*(*z).p.offset(((*z).c - 1) as isize) != 133 && *(*z).p.offset(((*z).c - 1) as isize) != 167)
    {
        return 0;
    }
    if find_among_b(z, A_19.as_ptr(), 2) == 0 {
        return 0;
    }
    (*z).bra = (*z).c;
    if len_utf8((*z).p) < 5 {
        return 0;
    }
    {
        let ret = slice_del(z);
        if ret < 0 {
            return ret;
        }
    }
    1
}

unsafe fn r_Suffix_Verb_Step2c(z: *mut SN_env) -> c_int {
    let among_var;
    (*z).ket = (*z).c;
    if (*z).c - 1 <= (*z).lb || *(*z).p.offset(((*z).c - 1) as isize) != 136 {
        return 0;
    }
    among_var = find_among_b(z, A_20.as_ptr(), 2);
    if among_var == 0 {
        return 0;
    }
    (*z).bra = (*z).c;
    match among_var {
        1 => {
            if len_utf8((*z).p) < 4 {
                return 0;
            }
            {
                let ret = slice_del(z);
                if ret < 0 {
                    return ret;
                }
            }
        }
        2 => {
            if len_utf8((*z).p) < 6 {
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

unsafe fn r_Suffix_All_alef_maqsura(z: *mut SN_env) -> c_int {
    (*z).ket = (*z).c;
    if (*z).c - 1 <= (*z).lb || *(*z).p.offset(((*z).c - 1) as isize) != 137 {
        return 0;
    }
    if find_among_b(z, A_21.as_ptr(), 1) == 0 {
        return 0;
    }
    (*z).bra = (*z).c;
    {
        let ret = slice_from_s(z, 2, S_66.as_ptr());
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
pub unsafe extern "C" fn arabic_UTF_8_stem(z: *mut SN_env) -> c_int {
    *(*z).I.offset(2) = 1;
    *(*z).I.offset(1) = 1;
    *(*z).I.offset(0) = 0;
    {
        let c1 = (*z).c;
        {
            let ret = r_Checks1(z);
            if ret < 0 {
                return ret;
            }
        }
        (*z).c = c1;
    }

    {
        let ret = r_Normalize_pre(z);
        if ret < 0 {
            return ret;
        }
    }
    (*z).lb = (*z).c;
    (*z).c = (*z).l;

    {
        let m2 = (*z).l - (*z).c;
        'lab0: {
            {
                let m3 = (*z).l - (*z).c;
                'lab1: {
                    'lab2: {
                        if *(*z).I.offset(1) == 0 {
                            break 'lab2;
                        }
                        {
                            let m4 = (*z).l - (*z).c;
                            'lab4: {
                                {
                                    let mut i = 1;
                                    'arc0: loop {
                                        let m5 = (*z).l - (*z).c;
                                        'lab5: {
                                            {
                                                let ret = r_Suffix_Verb_Step1(z);
                                                if ret == 0 {
                                                    break 'lab5;
                                                }
                                                if ret < 0 {
                                                    return ret;
                                                }
                                            }
                                            i -= 1;
                                            continue 'arc0;
                                        }
                                        (*z).c = (*z).l - m5;
                                        break;
                                    }
                                    if i > 0 {
                                        break 'lab4;
                                    }
                                }
                                {
                                    let m6 = (*z).l - (*z).c;
                                    'lab6: {
                                        'lab7: {
                                            {
                                                let ret = r_Suffix_Verb_Step2a(z);
                                                if ret == 0 {
                                                    break 'lab7;
                                                }
                                                if ret < 0 {
                                                    return ret;
                                                }
                                            }
                                            break 'lab6;
                                        }
                                        (*z).c = (*z).l - m6;
                                        'lab8: {
                                            {
                                                let ret = r_Suffix_Verb_Step2c(z);
                                                if ret == 0 {
                                                    break 'lab8;
                                                }
                                                if ret < 0 {
                                                    return ret;
                                                }
                                            }
                                            break 'lab6;
                                        }
                                        (*z).c = (*z).l - m6;
                                        {
                                            let ret = skip_b_utf8((*z).p, (*z).c, (*z).lb, 1);
                                            if ret < 0 {
                                                break 'lab4;
                                            }
                                            (*z).c = ret;
                                        }
                                    }
                                }
                                break 'lab1;
                            }
                            (*z).c = (*z).l - m4;
                            'lab9: {
                                {
                                    let ret = r_Suffix_Verb_Step2b(z);
                                    if ret == 0 {
                                        break 'lab9;
                                    }
                                    if ret < 0 {
                                        return ret;
                                    }
                                }
                                break 'lab1;
                            }
                            (*z).c = (*z).l - m4;
                            {
                                let ret = r_Suffix_Verb_Step2a(z);
                                if ret == 0 {
                                    break 'lab2;
                                }
                                if ret < 0 {
                                    return ret;
                                }
                            }
                        }
                        break 'lab1;
                    }
                    // lab2:
                    (*z).c = (*z).l - m3;
                    'lab10: {
                        if *(*z).I.offset(2) == 0 {
                            break 'lab10;
                        }
                        {
                            let m7 = (*z).l - (*z).c;
                            'lab11: {
                                {
                                    let m8 = (*z).l - (*z).c;
                                    'lab12: {
                                        'lab13: {
                                            {
                                                let ret = r_Suffix_Noun_Step2c2(z);
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
                                        'lab14: {
                                            'lab15: {
                                                if *(*z).I.offset(0) == 0 {
                                                    break 'lab15;
                                                }
                                                break 'lab14;
                                            }
                                            // lab15:
                                            {
                                                let ret = r_Suffix_Noun_Step1a(z);
                                                if ret == 0 {
                                                    break 'lab14;
                                                }
                                                if ret < 0 {
                                                    return ret;
                                                }
                                            }
                                            {
                                                let m9 = (*z).l - (*z).c;
                                                'lab16: {
                                                    'lab17: {
                                                        {
                                                            let ret = r_Suffix_Noun_Step2a(z);
                                                            if ret == 0 {
                                                                break 'lab17;
                                                            }
                                                            if ret < 0 {
                                                                return ret;
                                                            }
                                                        }
                                                        break 'lab16;
                                                    }
                                                    (*z).c = (*z).l - m9;
                                                    'lab18: {
                                                        {
                                                            let ret = r_Suffix_Noun_Step2b(z);
                                                            if ret == 0 {
                                                                break 'lab18;
                                                            }
                                                            if ret < 0 {
                                                                return ret;
                                                            }
                                                        }
                                                        break 'lab16;
                                                    }
                                                    (*z).c = (*z).l - m9;
                                                    'lab19: {
                                                        {
                                                            let ret = r_Suffix_Noun_Step2c1(z);
                                                            if ret == 0 {
                                                                break 'lab19;
                                                            }
                                                            if ret < 0 {
                                                                return ret;
                                                            }
                                                        }
                                                        break 'lab16;
                                                    }
                                                    (*z).c = (*z).l - m9;
                                                    {
                                                        let ret = skip_b_utf8((*z).p, (*z).c, (*z).lb, 1);
                                                        if ret < 0 {
                                                            break 'lab14;
                                                        }
                                                        (*z).c = ret;
                                                    }
                                                }
                                            }
                                            break 'lab12;
                                        }
                                        // lab14:
                                        (*z).c = (*z).l - m8;
                                        'lab20: {
                                            {
                                                let ret = r_Suffix_Noun_Step1b(z);
                                                if ret == 0 {
                                                    break 'lab20;
                                                }
                                                if ret < 0 {
                                                    return ret;
                                                }
                                            }
                                            {
                                                let m10 = (*z).l - (*z).c;
                                                'lab21: {
                                                    'lab22: {
                                                        {
                                                            let ret = r_Suffix_Noun_Step2a(z);
                                                            if ret == 0 {
                                                                break 'lab22;
                                                            }
                                                            if ret < 0 {
                                                                return ret;
                                                            }
                                                        }
                                                        break 'lab21;
                                                    }
                                                    (*z).c = (*z).l - m10;
                                                    'lab23: {
                                                        {
                                                            let ret = r_Suffix_Noun_Step2b(z);
                                                            if ret == 0 {
                                                                break 'lab23;
                                                            }
                                                            if ret < 0 {
                                                                return ret;
                                                            }
                                                        }
                                                        break 'lab21;
                                                    }
                                                    (*z).c = (*z).l - m10;
                                                    {
                                                        let ret = r_Suffix_Noun_Step2c1(z);
                                                        if ret == 0 {
                                                            break 'lab20;
                                                        }
                                                        if ret < 0 {
                                                            return ret;
                                                        }
                                                    }
                                                }
                                            }
                                            break 'lab12;
                                        }
                                        // lab20:
                                        (*z).c = (*z).l - m8;
                                        'lab24: {
                                            'lab25: {
                                                if *(*z).I.offset(0) == 0 {
                                                    break 'lab25;
                                                }
                                                break 'lab24;
                                            }
                                            // lab25:
                                            {
                                                let ret = r_Suffix_Noun_Step2a(z);
                                                if ret == 0 {
                                                    break 'lab24;
                                                }
                                                if ret < 0 {
                                                    return ret;
                                                }
                                            }
                                            break 'lab12;
                                        }
                                        // lab24:
                                        (*z).c = (*z).l - m8;
                                        {
                                            let ret = r_Suffix_Noun_Step2b(z);
                                            if ret == 0 {
                                                (*z).c = (*z).l - m7;
                                                break 'lab11;
                                            }
                                            if ret < 0 {
                                                return ret;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        {
                            let ret = r_Suffix_Noun_Step3(z);
                            if ret == 0 {
                                break 'lab10;
                            }
                            if ret < 0 {
                                return ret;
                            }
                        }
                        break 'lab1;
                    }
                    // lab10:
                    (*z).c = (*z).l - m3;
                    {
                        let ret = r_Suffix_All_alef_maqsura(z);
                        if ret == 0 {
                            break 'lab0;
                        }
                        if ret < 0 {
                            return ret;
                        }
                    }
                }
            }
        }
        // lab0:
        (*z).c = (*z).l - m2;
    }
    (*z).c = (*z).lb;
    {
        let c11 = (*z).c;
        {
            let c12 = (*z).c;
            'lab27: {
                let ret = r_Prefix_Step1(z);
                if ret == 0 {
                    (*z).c = c12;
                    break 'lab27;
                }
                if ret < 0 {
                    return ret;
                }
            }
        }
        {
            let c13 = (*z).c;
            'lab28: {
                let ret = r_Prefix_Step2(z);
                if ret == 0 {
                    (*z).c = c13;
                    break 'lab28;
                }
                if ret < 0 {
                    return ret;
                }
            }
        }
        'lab26: {
            let c14 = (*z).c;
            'lab29: {
                'lab30: {
                    {
                        let ret = r_Prefix_Step3a_Noun(z);
                        if ret == 0 {
                            break 'lab30;
                        }
                        if ret < 0 {
                            return ret;
                        }
                    }
                    break 'lab29;
                }
                (*z).c = c14;
                'lab31: {
                    if *(*z).I.offset(2) == 0 {
                        break 'lab31;
                    }
                    {
                        let ret = r_Prefix_Step3b_Noun(z);
                        if ret == 0 {
                            break 'lab31;
                        }
                        if ret < 0 {
                            return ret;
                        }
                    }
                    break 'lab29;
                }
                (*z).c = c14;
                if *(*z).I.offset(1) == 0 {
                    break 'lab26;
                }
                {
                    let c15 = (*z).c;
                    'lab32: {
                        let ret = r_Prefix_Step3_Verb(z);
                        if ret == 0 {
                            (*z).c = c15;
                            break 'lab32;
                        }
                        if ret < 0 {
                            return ret;
                        }
                    }
                }
                {
                    let ret = r_Prefix_Step4_Verb(z);
                    if ret == 0 {
                        break 'lab26;
                    }
                    if ret < 0 {
                        return ret;
                    }
                }
            }
        }
        (*z).c = c11;
    }

    {
        let ret = r_Normalize_post(z);
        if ret < 0 {
            return ret;
        }
    }
    1
}

#[no_mangle]
pub unsafe extern "C" fn arabic_UTF_8_create_env() -> *mut SN_env {
    SN_create_env(0, 3)
}

#[no_mangle]
pub unsafe extern "C" fn arabic_UTF_8_close_env(z: *mut SN_env) {
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
        let z = arabic_UTF_8_create_env();
        assert!(!z.is_null());
        let rc = SN_set_current(z, word.len() as c_int, word.as_ptr());
        assert!(rc >= 0);
        let rc = arabic_UTF_8_stem(z);
        assert!(rc >= 0, "stem returned {rc}");
        let out = std::slice::from_raw_parts((*z).p, SIZE((*z).p) as usize).to_vec();
        arabic_UTF_8_close_env(z);
        out
    }

    // Idempotence: stemming a stem yields the same stem.
    #[test]
    fn idempotent() {
        unsafe {
            for w in [
                "\u{627}\u{644}\u{643}\u{62a}\u{627}\u{628}".as_bytes(),
                "\u{645}\u{62f}\u{631}\u{633}\u{629}".as_bytes(),
            ] {
                let once = stem(w);
                let twice = stem(&once);
                assert_eq!(once, twice, "not idempotent for {:?}", w);
            }
        }
    }
}
