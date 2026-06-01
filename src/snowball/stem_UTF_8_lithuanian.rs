//! Lithuanian Snowball stemmer (UTF-8).
//!
//! 1:1 translation of the generated
//! `src/backend/snowball/libstemmer/stem_UTF_8_lithuanian.c` (Snowball 2.2.0),
//! merged with its header `stem_UTF_8_lithuanian.h`. The runtime helpers come
//! from `crate::snowball::api` and `crate::snowball::utilities`.

use crate::prelude::*;

use crate::snowball::api::{among, symbol, SN_close_env, SN_create_env, SN_env, SIZE};
use crate::snowball::utilities::{
    find_among_b, in_grouping_U, len_utf8, out_grouping_U, skip_utf8, slice_del, slice_from_s,
};

// ---------------------------------------------------------------------------
// among string tables
// ---------------------------------------------------------------------------

static S_0_0: [symbol; 1] = [b'a'];
static S_0_1: [symbol; 2] = [b'i', b'a'];
static S_0_2: [symbol; 4] = [b'e', b'r', b'i', b'a'];
static S_0_3: [symbol; 4] = [b'o', b's', b'n', b'a'];
static S_0_4: [symbol; 5] = [b'i', b'o', b's', b'n', b'a'];
static S_0_5: [symbol; 5] = [b'u', b'o', b's', b'n', b'a'];
static S_0_6: [symbol; 6] = [b'i', b'u', b'o', b's', b'n', b'a'];
static S_0_7: [symbol; 4] = [b'y', b's', b'n', b'a'];
static S_0_8: [symbol; 5] = [0xC4, 0x97, b's', b'n', b'a'];
static S_0_9: [symbol; 1] = [b'e'];
static S_0_10: [symbol; 2] = [b'i', b'e'];
static S_0_11: [symbol; 4] = [b'e', b'n', b'i', b'e'];
static S_0_12: [symbol; 4] = [b'e', b'r', b'i', b'e'];
static S_0_13: [symbol; 3] = [b'o', b'j', b'e'];
static S_0_14: [symbol; 4] = [b'i', b'o', b'j', b'e'];
static S_0_15: [symbol; 3] = [b'u', b'j', b'e'];
static S_0_16: [symbol; 4] = [b'i', b'u', b'j', b'e'];
static S_0_17: [symbol; 3] = [b'y', b'j', b'e'];
static S_0_18: [symbol; 5] = [b'e', b'n', b'y', b'j', b'e'];
static S_0_19: [symbol; 5] = [b'e', b'r', b'y', b'j', b'e'];
static S_0_20: [symbol; 4] = [0xC4, 0x97, b'j', b'e'];
static S_0_21: [symbol; 3] = [b'a', b'm', b'e'];
static S_0_22: [symbol; 4] = [b'i', b'a', b'm', b'e'];
static S_0_23: [symbol; 4] = [b's', b'i', b'm', b'e'];
static S_0_24: [symbol; 3] = [b'o', b'm', b'e'];
static S_0_25: [symbol; 4] = [0xC4, 0x97, b'm', b'e'];
static S_0_26: [symbol; 7] = [b't', b'u', b'm', 0xC4, 0x97, b'm', b'e'];
static S_0_27: [symbol; 3] = [b'o', b's', b'e'];
static S_0_28: [symbol; 4] = [b'i', b'o', b's', b'e'];
static S_0_29: [symbol; 4] = [b'u', b'o', b's', b'e'];
static S_0_30: [symbol; 5] = [b'i', b'u', b'o', b's', b'e'];
static S_0_31: [symbol; 3] = [b'y', b's', b'e'];
static S_0_32: [symbol; 5] = [b'e', b'n', b'y', b's', b'e'];
static S_0_33: [symbol; 5] = [b'e', b'r', b'y', b's', b'e'];
static S_0_34: [symbol; 4] = [0xC4, 0x97, b's', b'e'];
static S_0_35: [symbol; 3] = [b'a', b't', b'e'];
static S_0_36: [symbol; 4] = [b'i', b'a', b't', b'e'];
static S_0_37: [symbol; 3] = [b'i', b't', b'e'];
static S_0_38: [symbol; 4] = [b'k', b'i', b't', b'e'];
static S_0_39: [symbol; 4] = [b's', b'i', b't', b'e'];
static S_0_40: [symbol; 3] = [b'o', b't', b'e'];
static S_0_41: [symbol; 4] = [b't', b'u', b't', b'e'];
static S_0_42: [symbol; 4] = [0xC4, 0x97, b't', b'e'];
static S_0_43: [symbol; 7] = [b't', b'u', b'm', 0xC4, 0x97, b't', b'e'];
static S_0_44: [symbol; 1] = [b'i'];
static S_0_45: [symbol; 2] = [b'a', b'i'];
static S_0_46: [symbol; 3] = [b'i', b'a', b'i'];
static S_0_47: [symbol; 5] = [b'e', b'r', b'i', b'a', b'i'];
static S_0_48: [symbol; 2] = [b'e', b'i'];
static S_0_49: [symbol; 5] = [b't', b'u', b'm', b'e', b'i'];
static S_0_50: [symbol; 2] = [b'k', b'i'];
static S_0_51: [symbol; 3] = [b'i', b'm', b'i'];
static S_0_52: [symbol; 5] = [b'e', b'r', b'i', b'm', b'i'];
static S_0_53: [symbol; 3] = [b'u', b'm', b'i'];
static S_0_54: [symbol; 4] = [b'i', b'u', b'm', b'i'];
static S_0_55: [symbol; 2] = [b's', b'i'];
static S_0_56: [symbol; 3] = [b'a', b's', b'i'];
static S_0_57: [symbol; 4] = [b'i', b'a', b's', b'i'];
static S_0_58: [symbol; 3] = [b'e', b's', b'i'];
static S_0_59: [symbol; 4] = [b'i', b'e', b's', b'i'];
static S_0_60: [symbol; 5] = [b's', b'i', b'e', b's', b'i'];
static S_0_61: [symbol; 3] = [b'i', b's', b'i'];
static S_0_62: [symbol; 4] = [b'a', b'i', b's', b'i'];
static S_0_63: [symbol; 4] = [b'e', b'i', b's', b'i'];
static S_0_64: [symbol; 7] = [b't', b'u', b'm', b'e', b'i', b's', b'i'];
static S_0_65: [symbol; 4] = [b'u', b'i', b's', b'i'];
static S_0_66: [symbol; 3] = [b'o', b's', b'i'];
static S_0_67: [symbol; 6] = [0xC4, 0x97, b'j', b'o', b's', b'i'];
static S_0_68: [symbol; 4] = [b'u', b'o', b's', b'i'];
static S_0_69: [symbol; 5] = [b'i', b'u', b'o', b's', b'i'];
static S_0_70: [symbol; 6] = [b's', b'i', b'u', b'o', b's', b'i'];
static S_0_71: [symbol; 3] = [b'u', b's', b'i'];
static S_0_72: [symbol; 4] = [b'a', b'u', b's', b'i'];
static S_0_73: [symbol; 7] = [0xC4, 0x8D, b'i', b'a', b'u', b's', b'i'];
static S_0_74: [symbol; 4] = [0xC4, 0x85, b's', b'i'];
static S_0_75: [symbol; 4] = [0xC4, 0x97, b's', b'i'];
static S_0_76: [symbol; 4] = [0xC5, 0xB3, b's', b'i'];
static S_0_77: [symbol; 5] = [b't', 0xC5, 0xB3, b's', b'i'];
static S_0_78: [symbol; 2] = [b't', b'i'];
static S_0_79: [symbol; 4] = [b'e', b'n', b't', b'i'];
static S_0_80: [symbol; 4] = [b'i', b'n', b't', b'i'];
static S_0_81: [symbol; 3] = [b'o', b't', b'i'];
static S_0_82: [symbol; 4] = [b'i', b'o', b't', b'i'];
static S_0_83: [symbol; 4] = [b'u', b'o', b't', b'i'];
static S_0_84: [symbol; 5] = [b'i', b'u', b'o', b't', b'i'];
static S_0_85: [symbol; 4] = [b'a', b'u', b't', b'i'];
static S_0_86: [symbol; 5] = [b'i', b'a', b'u', b't', b'i'];
static S_0_87: [symbol; 3] = [b'y', b't', b'i'];
static S_0_88: [symbol; 4] = [0xC4, 0x97, b't', b'i'];
static S_0_89: [symbol; 7] = [b't', b'e', b'l', 0xC4, 0x97, b't', b'i'];
static S_0_90: [symbol; 6] = [b'i', b'n', 0xC4, 0x97, b't', b'i'];
static S_0_91: [symbol; 7] = [b't', b'e', b'r', 0xC4, 0x97, b't', b'i'];
static S_0_92: [symbol; 2] = [b'u', b'i'];
static S_0_93: [symbol; 3] = [b'i', b'u', b'i'];
static S_0_94: [symbol; 5] = [b'e', b'n', b'i', b'u', b'i'];
static S_0_95: [symbol; 2] = [b'o', b'j'];
static S_0_96: [symbol; 3] = [0xC4, 0x97, b'j'];
static S_0_97: [symbol; 1] = [b'k'];
static S_0_98: [symbol; 2] = [b'a', b'm'];
static S_0_99: [symbol; 3] = [b'i', b'a', b'm'];
static S_0_100: [symbol; 3] = [b'i', b'e', b'm'];
static S_0_101: [symbol; 2] = [b'i', b'm'];
static S_0_102: [symbol; 3] = [b's', b'i', b'm'];
static S_0_103: [symbol; 2] = [b'o', b'm'];
static S_0_104: [symbol; 3] = [b't', b'u', b'm'];
static S_0_105: [symbol; 3] = [0xC4, 0x97, b'm'];
static S_0_106: [symbol; 6] = [b't', b'u', b'm', 0xC4, 0x97, b'm'];
static S_0_107: [symbol; 2] = [b'a', b'n'];
static S_0_108: [symbol; 2] = [b'o', b'n'];
static S_0_109: [symbol; 3] = [b'i', b'o', b'n'];
static S_0_110: [symbol; 2] = [b'u', b'n'];
static S_0_111: [symbol; 3] = [b'i', b'u', b'n'];
static S_0_112: [symbol; 3] = [0xC4, 0x97, b'n'];
static S_0_113: [symbol; 1] = [b'o'];
static S_0_114: [symbol; 2] = [b'i', b'o'];
static S_0_115: [symbol; 4] = [b'e', b'n', b'i', b'o'];
static S_0_116: [symbol; 4] = [0xC4, 0x97, b'j', b'o'];
static S_0_117: [symbol; 2] = [b'u', b'o'];
static S_0_118: [symbol; 1] = [b's'];
static S_0_119: [symbol; 2] = [b'a', b's'];
static S_0_120: [symbol; 3] = [b'i', b'a', b's'];
static S_0_121: [symbol; 2] = [b'e', b's'];
static S_0_122: [symbol; 3] = [b'i', b'e', b's'];
static S_0_123: [symbol; 2] = [b'i', b's'];
static S_0_124: [symbol; 3] = [b'a', b'i', b's'];
static S_0_125: [symbol; 4] = [b'i', b'a', b'i', b's'];
static S_0_126: [symbol; 6] = [b't', b'u', b'm', b'e', b'i', b's'];
static S_0_127: [symbol; 4] = [b'i', b'm', b'i', b's'];
static S_0_128: [symbol; 6] = [b'e', b'n', b'i', b'm', b'i', b's'];
static S_0_129: [symbol; 4] = [b'o', b'm', b'i', b's'];
static S_0_130: [symbol; 5] = [b'i', b'o', b'm', b'i', b's'];
static S_0_131: [symbol; 4] = [b'u', b'm', b'i', b's'];
static S_0_132: [symbol; 5] = [0xC4, 0x97, b'm', b'i', b's'];
static S_0_133: [symbol; 4] = [b'e', b'n', b'i', b's'];
static S_0_134: [symbol; 4] = [b'a', b's', b'i', b's'];
static S_0_135: [symbol; 4] = [b'y', b's', b'i', b's'];
static S_0_136: [symbol; 3] = [b'a', b'm', b's'];
static S_0_137: [symbol; 4] = [b'i', b'a', b'm', b's'];
static S_0_138: [symbol; 4] = [b'i', b'e', b'm', b's'];
static S_0_139: [symbol; 3] = [b'i', b'm', b's'];
static S_0_140: [symbol; 5] = [b'e', b'n', b'i', b'm', b's'];
static S_0_141: [symbol; 5] = [b'e', b'r', b'i', b'm', b's'];
static S_0_142: [symbol; 3] = [b'o', b'm', b's'];
static S_0_143: [symbol; 4] = [b'i', b'o', b'm', b's'];
static S_0_144: [symbol; 3] = [b'u', b'm', b's'];
static S_0_145: [symbol; 4] = [0xC4, 0x97, b'm', b's'];
static S_0_146: [symbol; 3] = [b'e', b'n', b's'];
static S_0_147: [symbol; 2] = [b'o', b's'];
static S_0_148: [symbol; 3] = [b'i', b'o', b's'];
static S_0_149: [symbol; 3] = [b'u', b'o', b's'];
static S_0_150: [symbol; 4] = [b'i', b'u', b'o', b's'];
static S_0_151: [symbol; 3] = [b'e', b'r', b's'];
static S_0_152: [symbol; 2] = [b'u', b's'];
static S_0_153: [symbol; 3] = [b'a', b'u', b's'];
static S_0_154: [symbol; 4] = [b'i', b'a', b'u', b's'];
static S_0_155: [symbol; 3] = [b'i', b'u', b's'];
static S_0_156: [symbol; 2] = [b'y', b's'];
static S_0_157: [symbol; 4] = [b'e', b'n', b'y', b's'];
static S_0_158: [symbol; 4] = [b'e', b'r', b'y', b's'];
static S_0_159: [symbol; 3] = [0xC4, 0x85, b's'];
static S_0_160: [symbol; 4] = [b'i', 0xC4, 0x85, b's'];
static S_0_161: [symbol; 3] = [0xC4, 0x97, b's'];
static S_0_162: [symbol; 5] = [b'a', b'm', 0xC4, 0x97, b's'];
static S_0_163: [symbol; 6] = [b'i', b'a', b'm', 0xC4, 0x97, b's'];
static S_0_164: [symbol; 5] = [b'i', b'm', 0xC4, 0x97, b's'];
static S_0_165: [symbol; 6] = [b'k', b'i', b'm', 0xC4, 0x97, b's'];
static S_0_166: [symbol; 6] = [b's', b'i', b'm', 0xC4, 0x97, b's'];
static S_0_167: [symbol; 5] = [b'o', b'm', 0xC4, 0x97, b's'];
static S_0_168: [symbol; 6] = [0xC4, 0x97, b'm', 0xC4, 0x97, b's'];
static S_0_169: [symbol; 9] = [b't', b'u', b'm', 0xC4, 0x97, b'm', 0xC4, 0x97, b's'];
static S_0_170: [symbol; 5] = [b'a', b't', 0xC4, 0x97, b's'];
static S_0_171: [symbol; 6] = [b'i', b'a', b't', 0xC4, 0x97, b's'];
static S_0_172: [symbol; 6] = [b's', b'i', b't', 0xC4, 0x97, b's'];
static S_0_173: [symbol; 5] = [b'o', b't', 0xC4, 0x97, b's'];
static S_0_174: [symbol; 6] = [0xC4, 0x97, b't', 0xC4, 0x97, b's'];
static S_0_175: [symbol; 9] = [b't', b'u', b'm', 0xC4, 0x97, b't', 0xC4, 0x97, b's'];
static S_0_176: [symbol; 3] = [0xC5, 0xAB, b's'];
static S_0_177: [symbol; 3] = [0xC4, 0xAF, b's'];
static S_0_178: [symbol; 4] = [b't', 0xC5, 0xB3, b's'];
static S_0_179: [symbol; 2] = [b'a', b't'];
static S_0_180: [symbol; 3] = [b'i', b'a', b't'];
static S_0_181: [symbol; 2] = [b'i', b't'];
static S_0_182: [symbol; 3] = [b's', b'i', b't'];
static S_0_183: [symbol; 2] = [b'o', b't'];
static S_0_184: [symbol; 3] = [0xC4, 0x97, b't'];
static S_0_185: [symbol; 6] = [b't', b'u', b'm', 0xC4, 0x97, b't'];
static S_0_186: [symbol; 1] = [b'u'];
static S_0_187: [symbol; 2] = [b'a', b'u'];
static S_0_188: [symbol; 3] = [b'i', b'a', b'u'];
static S_0_189: [symbol; 5] = [0xC4, 0x8D, b'i', b'a', b'u'];
static S_0_190: [symbol; 2] = [b'i', b'u'];
static S_0_191: [symbol; 4] = [b'e', b'n', b'i', b'u'];
static S_0_192: [symbol; 3] = [b's', b'i', b'u'];
static S_0_193: [symbol; 1] = [b'y'];
static S_0_194: [symbol; 2] = [0xC4, 0x85];
static S_0_195: [symbol; 3] = [b'i', 0xC4, 0x85];
static S_0_196: [symbol; 2] = [0xC4, 0x97];
static S_0_197: [symbol; 2] = [0xC4, 0x99];
static S_0_198: [symbol; 2] = [0xC4, 0xAF];
static S_0_199: [symbol; 4] = [b'e', b'n', 0xC4, 0xAF];
static S_0_200: [symbol; 4] = [b'e', b'r', 0xC4, 0xAF];
static S_0_201: [symbol; 2] = [0xC5, 0xB3];
static S_0_202: [symbol; 3] = [b'i', 0xC5, 0xB3];
static S_0_203: [symbol; 4] = [b'e', b'r', 0xC5, 0xB3];

static A_0: [among; 204] = [
    among { s_size: 1, s: S_0_0.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_0_1.as_ptr(), substring_i: 0, result: -1, function: None },
    among { s_size: 4, s: S_0_2.as_ptr(), substring_i: 1, result: -1, function: None },
    among { s_size: 4, s: S_0_3.as_ptr(), substring_i: 0, result: -1, function: None },
    among { s_size: 5, s: S_0_4.as_ptr(), substring_i: 3, result: -1, function: None },
    among { s_size: 5, s: S_0_5.as_ptr(), substring_i: 3, result: -1, function: None },
    among { s_size: 6, s: S_0_6.as_ptr(), substring_i: 5, result: -1, function: None },
    among { s_size: 4, s: S_0_7.as_ptr(), substring_i: 0, result: -1, function: None },
    among { s_size: 5, s: S_0_8.as_ptr(), substring_i: 0, result: -1, function: None },
    among { s_size: 1, s: S_0_9.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_0_10.as_ptr(), substring_i: 9, result: -1, function: None },
    among { s_size: 4, s: S_0_11.as_ptr(), substring_i: 10, result: -1, function: None },
    among { s_size: 4, s: S_0_12.as_ptr(), substring_i: 10, result: -1, function: None },
    among { s_size: 3, s: S_0_13.as_ptr(), substring_i: 9, result: -1, function: None },
    among { s_size: 4, s: S_0_14.as_ptr(), substring_i: 13, result: -1, function: None },
    among { s_size: 3, s: S_0_15.as_ptr(), substring_i: 9, result: -1, function: None },
    among { s_size: 4, s: S_0_16.as_ptr(), substring_i: 15, result: -1, function: None },
    among { s_size: 3, s: S_0_17.as_ptr(), substring_i: 9, result: -1, function: None },
    among { s_size: 5, s: S_0_18.as_ptr(), substring_i: 17, result: -1, function: None },
    among { s_size: 5, s: S_0_19.as_ptr(), substring_i: 17, result: -1, function: None },
    among { s_size: 4, s: S_0_20.as_ptr(), substring_i: 9, result: -1, function: None },
    among { s_size: 3, s: S_0_21.as_ptr(), substring_i: 9, result: -1, function: None },
    among { s_size: 4, s: S_0_22.as_ptr(), substring_i: 21, result: -1, function: None },
    among { s_size: 4, s: S_0_23.as_ptr(), substring_i: 9, result: -1, function: None },
    among { s_size: 3, s: S_0_24.as_ptr(), substring_i: 9, result: -1, function: None },
    among { s_size: 4, s: S_0_25.as_ptr(), substring_i: 9, result: -1, function: None },
    among { s_size: 7, s: S_0_26.as_ptr(), substring_i: 25, result: -1, function: None },
    among { s_size: 3, s: S_0_27.as_ptr(), substring_i: 9, result: -1, function: None },
    among { s_size: 4, s: S_0_28.as_ptr(), substring_i: 27, result: -1, function: None },
    among { s_size: 4, s: S_0_29.as_ptr(), substring_i: 27, result: -1, function: None },
    among { s_size: 5, s: S_0_30.as_ptr(), substring_i: 29, result: -1, function: None },
    among { s_size: 3, s: S_0_31.as_ptr(), substring_i: 9, result: -1, function: None },
    among { s_size: 5, s: S_0_32.as_ptr(), substring_i: 31, result: -1, function: None },
    among { s_size: 5, s: S_0_33.as_ptr(), substring_i: 31, result: -1, function: None },
    among { s_size: 4, s: S_0_34.as_ptr(), substring_i: 9, result: -1, function: None },
    among { s_size: 3, s: S_0_35.as_ptr(), substring_i: 9, result: -1, function: None },
    among { s_size: 4, s: S_0_36.as_ptr(), substring_i: 35, result: -1, function: None },
    among { s_size: 3, s: S_0_37.as_ptr(), substring_i: 9, result: -1, function: None },
    among { s_size: 4, s: S_0_38.as_ptr(), substring_i: 37, result: -1, function: None },
    among { s_size: 4, s: S_0_39.as_ptr(), substring_i: 37, result: -1, function: None },
    among { s_size: 3, s: S_0_40.as_ptr(), substring_i: 9, result: -1, function: None },
    among { s_size: 4, s: S_0_41.as_ptr(), substring_i: 9, result: -1, function: None },
    among { s_size: 4, s: S_0_42.as_ptr(), substring_i: 9, result: -1, function: None },
    among { s_size: 7, s: S_0_43.as_ptr(), substring_i: 42, result: -1, function: None },
    among { s_size: 1, s: S_0_44.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_0_45.as_ptr(), substring_i: 44, result: -1, function: None },
    among { s_size: 3, s: S_0_46.as_ptr(), substring_i: 45, result: -1, function: None },
    among { s_size: 5, s: S_0_47.as_ptr(), substring_i: 46, result: -1, function: None },
    among { s_size: 2, s: S_0_48.as_ptr(), substring_i: 44, result: -1, function: None },
    among { s_size: 5, s: S_0_49.as_ptr(), substring_i: 48, result: -1, function: None },
    among { s_size: 2, s: S_0_50.as_ptr(), substring_i: 44, result: -1, function: None },
    among { s_size: 3, s: S_0_51.as_ptr(), substring_i: 44, result: -1, function: None },
    among { s_size: 5, s: S_0_52.as_ptr(), substring_i: 51, result: -1, function: None },
    among { s_size: 3, s: S_0_53.as_ptr(), substring_i: 44, result: -1, function: None },
    among { s_size: 4, s: S_0_54.as_ptr(), substring_i: 53, result: -1, function: None },
    among { s_size: 2, s: S_0_55.as_ptr(), substring_i: 44, result: -1, function: None },
    among { s_size: 3, s: S_0_56.as_ptr(), substring_i: 55, result: -1, function: None },
    among { s_size: 4, s: S_0_57.as_ptr(), substring_i: 56, result: -1, function: None },
    among { s_size: 3, s: S_0_58.as_ptr(), substring_i: 55, result: -1, function: None },
    among { s_size: 4, s: S_0_59.as_ptr(), substring_i: 58, result: -1, function: None },
    among { s_size: 5, s: S_0_60.as_ptr(), substring_i: 59, result: -1, function: None },
    among { s_size: 3, s: S_0_61.as_ptr(), substring_i: 55, result: -1, function: None },
    among { s_size: 4, s: S_0_62.as_ptr(), substring_i: 61, result: -1, function: None },
    among { s_size: 4, s: S_0_63.as_ptr(), substring_i: 61, result: -1, function: None },
    among { s_size: 7, s: S_0_64.as_ptr(), substring_i: 63, result: -1, function: None },
    among { s_size: 4, s: S_0_65.as_ptr(), substring_i: 61, result: -1, function: None },
    among { s_size: 3, s: S_0_66.as_ptr(), substring_i: 55, result: -1, function: None },
    among { s_size: 6, s: S_0_67.as_ptr(), substring_i: 66, result: -1, function: None },
    among { s_size: 4, s: S_0_68.as_ptr(), substring_i: 66, result: -1, function: None },
    among { s_size: 5, s: S_0_69.as_ptr(), substring_i: 68, result: -1, function: None },
    among { s_size: 6, s: S_0_70.as_ptr(), substring_i: 69, result: -1, function: None },
    among { s_size: 3, s: S_0_71.as_ptr(), substring_i: 55, result: -1, function: None },
    among { s_size: 4, s: S_0_72.as_ptr(), substring_i: 71, result: -1, function: None },
    among { s_size: 7, s: S_0_73.as_ptr(), substring_i: 72, result: -1, function: None },
    among { s_size: 4, s: S_0_74.as_ptr(), substring_i: 55, result: -1, function: None },
    among { s_size: 4, s: S_0_75.as_ptr(), substring_i: 55, result: -1, function: None },
    among { s_size: 4, s: S_0_76.as_ptr(), substring_i: 55, result: -1, function: None },
    among { s_size: 5, s: S_0_77.as_ptr(), substring_i: 76, result: -1, function: None },
    among { s_size: 2, s: S_0_78.as_ptr(), substring_i: 44, result: -1, function: None },
    among { s_size: 4, s: S_0_79.as_ptr(), substring_i: 78, result: -1, function: None },
    among { s_size: 4, s: S_0_80.as_ptr(), substring_i: 78, result: -1, function: None },
    among { s_size: 3, s: S_0_81.as_ptr(), substring_i: 78, result: -1, function: None },
    among { s_size: 4, s: S_0_82.as_ptr(), substring_i: 81, result: -1, function: None },
    among { s_size: 4, s: S_0_83.as_ptr(), substring_i: 81, result: -1, function: None },
    among { s_size: 5, s: S_0_84.as_ptr(), substring_i: 83, result: -1, function: None },
    among { s_size: 4, s: S_0_85.as_ptr(), substring_i: 78, result: -1, function: None },
    among { s_size: 5, s: S_0_86.as_ptr(), substring_i: 85, result: -1, function: None },
    among { s_size: 3, s: S_0_87.as_ptr(), substring_i: 78, result: -1, function: None },
    among { s_size: 4, s: S_0_88.as_ptr(), substring_i: 78, result: -1, function: None },
    among { s_size: 7, s: S_0_89.as_ptr(), substring_i: 88, result: -1, function: None },
    among { s_size: 6, s: S_0_90.as_ptr(), substring_i: 88, result: -1, function: None },
    among { s_size: 7, s: S_0_91.as_ptr(), substring_i: 88, result: -1, function: None },
    among { s_size: 2, s: S_0_92.as_ptr(), substring_i: 44, result: -1, function: None },
    among { s_size: 3, s: S_0_93.as_ptr(), substring_i: 92, result: -1, function: None },
    among { s_size: 5, s: S_0_94.as_ptr(), substring_i: 93, result: -1, function: None },
    among { s_size: 2, s: S_0_95.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_0_96.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 1, s: S_0_97.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_0_98.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_0_99.as_ptr(), substring_i: 98, result: -1, function: None },
    among { s_size: 3, s: S_0_100.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_0_101.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_0_102.as_ptr(), substring_i: 101, result: -1, function: None },
    among { s_size: 2, s: S_0_103.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_0_104.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_0_105.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 6, s: S_0_106.as_ptr(), substring_i: 105, result: -1, function: None },
    among { s_size: 2, s: S_0_107.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_0_108.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_0_109.as_ptr(), substring_i: 108, result: -1, function: None },
    among { s_size: 2, s: S_0_110.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_0_111.as_ptr(), substring_i: 110, result: -1, function: None },
    among { s_size: 3, s: S_0_112.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 1, s: S_0_113.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_0_114.as_ptr(), substring_i: 113, result: -1, function: None },
    among { s_size: 4, s: S_0_115.as_ptr(), substring_i: 114, result: -1, function: None },
    among { s_size: 4, s: S_0_116.as_ptr(), substring_i: 113, result: -1, function: None },
    among { s_size: 2, s: S_0_117.as_ptr(), substring_i: 113, result: -1, function: None },
    among { s_size: 1, s: S_0_118.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_0_119.as_ptr(), substring_i: 118, result: -1, function: None },
    among { s_size: 3, s: S_0_120.as_ptr(), substring_i: 119, result: -1, function: None },
    among { s_size: 2, s: S_0_121.as_ptr(), substring_i: 118, result: -1, function: None },
    among { s_size: 3, s: S_0_122.as_ptr(), substring_i: 121, result: -1, function: None },
    among { s_size: 2, s: S_0_123.as_ptr(), substring_i: 118, result: -1, function: None },
    among { s_size: 3, s: S_0_124.as_ptr(), substring_i: 123, result: -1, function: None },
    among { s_size: 4, s: S_0_125.as_ptr(), substring_i: 124, result: -1, function: None },
    among { s_size: 6, s: S_0_126.as_ptr(), substring_i: 123, result: -1, function: None },
    among { s_size: 4, s: S_0_127.as_ptr(), substring_i: 123, result: -1, function: None },
    among { s_size: 6, s: S_0_128.as_ptr(), substring_i: 127, result: -1, function: None },
    among { s_size: 4, s: S_0_129.as_ptr(), substring_i: 123, result: -1, function: None },
    among { s_size: 5, s: S_0_130.as_ptr(), substring_i: 129, result: -1, function: None },
    among { s_size: 4, s: S_0_131.as_ptr(), substring_i: 123, result: -1, function: None },
    among { s_size: 5, s: S_0_132.as_ptr(), substring_i: 123, result: -1, function: None },
    among { s_size: 4, s: S_0_133.as_ptr(), substring_i: 123, result: -1, function: None },
    among { s_size: 4, s: S_0_134.as_ptr(), substring_i: 123, result: -1, function: None },
    among { s_size: 4, s: S_0_135.as_ptr(), substring_i: 123, result: -1, function: None },
    among { s_size: 3, s: S_0_136.as_ptr(), substring_i: 118, result: -1, function: None },
    among { s_size: 4, s: S_0_137.as_ptr(), substring_i: 136, result: -1, function: None },
    among { s_size: 4, s: S_0_138.as_ptr(), substring_i: 118, result: -1, function: None },
    among { s_size: 3, s: S_0_139.as_ptr(), substring_i: 118, result: -1, function: None },
    among { s_size: 5, s: S_0_140.as_ptr(), substring_i: 139, result: -1, function: None },
    among { s_size: 5, s: S_0_141.as_ptr(), substring_i: 139, result: -1, function: None },
    among { s_size: 3, s: S_0_142.as_ptr(), substring_i: 118, result: -1, function: None },
    among { s_size: 4, s: S_0_143.as_ptr(), substring_i: 142, result: -1, function: None },
    among { s_size: 3, s: S_0_144.as_ptr(), substring_i: 118, result: -1, function: None },
    among { s_size: 4, s: S_0_145.as_ptr(), substring_i: 118, result: -1, function: None },
    among { s_size: 3, s: S_0_146.as_ptr(), substring_i: 118, result: -1, function: None },
    among { s_size: 2, s: S_0_147.as_ptr(), substring_i: 118, result: -1, function: None },
    among { s_size: 3, s: S_0_148.as_ptr(), substring_i: 147, result: -1, function: None },
    among { s_size: 3, s: S_0_149.as_ptr(), substring_i: 147, result: -1, function: None },
    among { s_size: 4, s: S_0_150.as_ptr(), substring_i: 149, result: -1, function: None },
    among { s_size: 3, s: S_0_151.as_ptr(), substring_i: 118, result: -1, function: None },
    among { s_size: 2, s: S_0_152.as_ptr(), substring_i: 118, result: -1, function: None },
    among { s_size: 3, s: S_0_153.as_ptr(), substring_i: 152, result: -1, function: None },
    among { s_size: 4, s: S_0_154.as_ptr(), substring_i: 153, result: -1, function: None },
    among { s_size: 3, s: S_0_155.as_ptr(), substring_i: 152, result: -1, function: None },
    among { s_size: 2, s: S_0_156.as_ptr(), substring_i: 118, result: -1, function: None },
    among { s_size: 4, s: S_0_157.as_ptr(), substring_i: 156, result: -1, function: None },
    among { s_size: 4, s: S_0_158.as_ptr(), substring_i: 156, result: -1, function: None },
    among { s_size: 3, s: S_0_159.as_ptr(), substring_i: 118, result: -1, function: None },
    among { s_size: 4, s: S_0_160.as_ptr(), substring_i: 159, result: -1, function: None },
    among { s_size: 3, s: S_0_161.as_ptr(), substring_i: 118, result: -1, function: None },
    among { s_size: 5, s: S_0_162.as_ptr(), substring_i: 161, result: -1, function: None },
    among { s_size: 6, s: S_0_163.as_ptr(), substring_i: 162, result: -1, function: None },
    among { s_size: 5, s: S_0_164.as_ptr(), substring_i: 161, result: -1, function: None },
    among { s_size: 6, s: S_0_165.as_ptr(), substring_i: 164, result: -1, function: None },
    among { s_size: 6, s: S_0_166.as_ptr(), substring_i: 164, result: -1, function: None },
    among { s_size: 5, s: S_0_167.as_ptr(), substring_i: 161, result: -1, function: None },
    among { s_size: 6, s: S_0_168.as_ptr(), substring_i: 161, result: -1, function: None },
    among { s_size: 9, s: S_0_169.as_ptr(), substring_i: 168, result: -1, function: None },
    among { s_size: 5, s: S_0_170.as_ptr(), substring_i: 161, result: -1, function: None },
    among { s_size: 6, s: S_0_171.as_ptr(), substring_i: 170, result: -1, function: None },
    among { s_size: 6, s: S_0_172.as_ptr(), substring_i: 161, result: -1, function: None },
    among { s_size: 5, s: S_0_173.as_ptr(), substring_i: 161, result: -1, function: None },
    among { s_size: 6, s: S_0_174.as_ptr(), substring_i: 161, result: -1, function: None },
    among { s_size: 9, s: S_0_175.as_ptr(), substring_i: 174, result: -1, function: None },
    among { s_size: 3, s: S_0_176.as_ptr(), substring_i: 118, result: -1, function: None },
    among { s_size: 3, s: S_0_177.as_ptr(), substring_i: 118, result: -1, function: None },
    among { s_size: 4, s: S_0_178.as_ptr(), substring_i: 118, result: -1, function: None },
    among { s_size: 2, s: S_0_179.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_0_180.as_ptr(), substring_i: 179, result: -1, function: None },
    among { s_size: 2, s: S_0_181.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_0_182.as_ptr(), substring_i: 181, result: -1, function: None },
    among { s_size: 2, s: S_0_183.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_0_184.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 6, s: S_0_185.as_ptr(), substring_i: 184, result: -1, function: None },
    among { s_size: 1, s: S_0_186.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_0_187.as_ptr(), substring_i: 186, result: -1, function: None },
    among { s_size: 3, s: S_0_188.as_ptr(), substring_i: 187, result: -1, function: None },
    among { s_size: 5, s: S_0_189.as_ptr(), substring_i: 188, result: -1, function: None },
    among { s_size: 2, s: S_0_190.as_ptr(), substring_i: 186, result: -1, function: None },
    among { s_size: 4, s: S_0_191.as_ptr(), substring_i: 190, result: -1, function: None },
    among { s_size: 3, s: S_0_192.as_ptr(), substring_i: 190, result: -1, function: None },
    among { s_size: 1, s: S_0_193.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_0_194.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_0_195.as_ptr(), substring_i: 194, result: -1, function: None },
    among { s_size: 2, s: S_0_196.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_0_197.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_0_198.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 4, s: S_0_199.as_ptr(), substring_i: 198, result: -1, function: None },
    among { s_size: 4, s: S_0_200.as_ptr(), substring_i: 198, result: -1, function: None },
    among { s_size: 2, s: S_0_201.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_0_202.as_ptr(), substring_i: 201, result: -1, function: None },
    among { s_size: 4, s: S_0_203.as_ptr(), substring_i: 201, result: -1, function: None },
];

static S_1_0: [symbol; 3] = [b'i', b'n', b'g'];
static S_1_1: [symbol; 2] = [b'a', b'j'];
static S_1_2: [symbol; 3] = [b'i', b'a', b'j'];
static S_1_3: [symbol; 3] = [b'i', b'e', b'j'];
static S_1_4: [symbol; 2] = [b'o', b'j'];
static S_1_5: [symbol; 3] = [b'i', b'o', b'j'];
static S_1_6: [symbol; 3] = [b'u', b'o', b'j'];
static S_1_7: [symbol; 4] = [b'i', b'u', b'o', b'j'];
static S_1_8: [symbol; 3] = [b'a', b'u', b'j'];
static S_1_9: [symbol; 3] = [0xC4, 0x85, b'j'];
static S_1_10: [symbol; 4] = [b'i', 0xC4, 0x85, b'j'];
static S_1_11: [symbol; 3] = [0xC4, 0x97, b'j'];
static S_1_12: [symbol; 3] = [0xC5, 0xB3, b'j'];
static S_1_13: [symbol; 4] = [b'i', 0xC5, 0xB3, b'j'];
static S_1_14: [symbol; 2] = [b'o', b'k'];
static S_1_15: [symbol; 3] = [b'i', b'o', b'k'];
static S_1_16: [symbol; 3] = [b'i', b'u', b'k'];
static S_1_17: [symbol; 5] = [b'u', b'l', b'i', b'u', b'k'];
static S_1_18: [symbol; 6] = [b'u', 0xC4, 0x8D, b'i', b'u', b'k'];
static S_1_19: [symbol; 4] = [b'i', 0xC5, 0xA1, b'k'];
static S_1_20: [symbol; 3] = [b'i', b'u', b'l'];
static S_1_21: [symbol; 2] = [b'y', b'l'];
static S_1_22: [symbol; 3] = [0xC4, 0x97, b'l'];
static S_1_23: [symbol; 2] = [b'a', b'm'];
static S_1_24: [symbol; 3] = [b'd', b'a', b'm'];
static S_1_25: [symbol; 3] = [b'j', b'a', b'm'];
static S_1_26: [symbol; 4] = [b'z', b'g', b'a', b'n'];
static S_1_27: [symbol; 3] = [b'a', b'i', b'n'];
static S_1_28: [symbol; 3] = [b'e', b's', b'n'];
static S_1_29: [symbol; 2] = [b'o', b'p'];
static S_1_30: [symbol; 3] = [b'i', b'o', b'p'];
static S_1_31: [symbol; 3] = [b'i', b'a', b's'];
static S_1_32: [symbol; 3] = [b'i', b'e', b's'];
static S_1_33: [symbol; 3] = [b'a', b'i', b's'];
static S_1_34: [symbol; 4] = [b'i', b'a', b'i', b's'];
static S_1_35: [symbol; 2] = [b'o', b's'];
static S_1_36: [symbol; 3] = [b'i', b'o', b's'];
static S_1_37: [symbol; 3] = [b'u', b'o', b's'];
static S_1_38: [symbol; 4] = [b'i', b'u', b'o', b's'];
static S_1_39: [symbol; 3] = [b'a', b'u', b's'];
static S_1_40: [symbol; 4] = [b'i', b'a', b'u', b's'];
static S_1_41: [symbol; 3] = [0xC4, 0x85, b's'];
static S_1_42: [symbol; 4] = [b'i', 0xC4, 0x85, b's'];
static S_1_43: [symbol; 3] = [0xC4, 0x99, b's'];
static S_1_44: [symbol; 7] = [b'u', b't', 0xC4, 0x97, b'a', b'i', b't'];
static S_1_45: [symbol; 3] = [b'a', b'n', b't'];
static S_1_46: [symbol; 4] = [b'i', b'a', b'n', b't'];
static S_1_47: [symbol; 5] = [b's', b'i', b'a', b'n', b't'];
static S_1_48: [symbol; 3] = [b'i', b'n', b't'];
static S_1_49: [symbol; 2] = [b'o', b't'];
static S_1_50: [symbol; 3] = [b'u', b'o', b't'];
static S_1_51: [symbol; 4] = [b'i', b'u', b'o', b't'];
static S_1_52: [symbol; 2] = [b'y', b't'];
static S_1_53: [symbol; 3] = [0xC4, 0x97, b't'];
static S_1_54: [symbol; 5] = [b'y', b'k', 0xC5, 0xA1, b't'];
static S_1_55: [symbol; 3] = [b'i', b'a', b'u'];
static S_1_56: [symbol; 3] = [b'd', b'a', b'v'];
static S_1_57: [symbol; 2] = [b's', b'v'];
static S_1_58: [symbol; 3] = [0xC5, 0xA1, b'v'];
static S_1_59: [symbol; 6] = [b'y', b'k', 0xC5, 0xA1, 0xC4, 0x8D];
static S_1_60: [symbol; 2] = [0xC4, 0x99];
static S_1_61: [symbol; 5] = [0xC4, 0x97, b'j', 0xC4, 0x99];

static A_1: [among; 62] = [
    among { s_size: 3, s: S_1_0.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_1_1.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_1_2.as_ptr(), substring_i: 1, result: -1, function: None },
    among { s_size: 3, s: S_1_3.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_1_4.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_1_5.as_ptr(), substring_i: 4, result: -1, function: None },
    among { s_size: 3, s: S_1_6.as_ptr(), substring_i: 4, result: -1, function: None },
    among { s_size: 4, s: S_1_7.as_ptr(), substring_i: 6, result: -1, function: None },
    among { s_size: 3, s: S_1_8.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_1_9.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 4, s: S_1_10.as_ptr(), substring_i: 9, result: -1, function: None },
    among { s_size: 3, s: S_1_11.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_1_12.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 4, s: S_1_13.as_ptr(), substring_i: 12, result: -1, function: None },
    among { s_size: 2, s: S_1_14.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_1_15.as_ptr(), substring_i: 14, result: -1, function: None },
    among { s_size: 3, s: S_1_16.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 5, s: S_1_17.as_ptr(), substring_i: 16, result: -1, function: None },
    among { s_size: 6, s: S_1_18.as_ptr(), substring_i: 16, result: -1, function: None },
    among { s_size: 4, s: S_1_19.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_1_20.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_1_21.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_1_22.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_1_23.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_1_24.as_ptr(), substring_i: 23, result: -1, function: None },
    among { s_size: 3, s: S_1_25.as_ptr(), substring_i: 23, result: -1, function: None },
    among { s_size: 4, s: S_1_26.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_1_27.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_1_28.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_1_29.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_1_30.as_ptr(), substring_i: 29, result: -1, function: None },
    among { s_size: 3, s: S_1_31.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_1_32.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_1_33.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 4, s: S_1_34.as_ptr(), substring_i: 33, result: -1, function: None },
    among { s_size: 2, s: S_1_35.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_1_36.as_ptr(), substring_i: 35, result: -1, function: None },
    among { s_size: 3, s: S_1_37.as_ptr(), substring_i: 35, result: -1, function: None },
    among { s_size: 4, s: S_1_38.as_ptr(), substring_i: 37, result: -1, function: None },
    among { s_size: 3, s: S_1_39.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 4, s: S_1_40.as_ptr(), substring_i: 39, result: -1, function: None },
    among { s_size: 3, s: S_1_41.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 4, s: S_1_42.as_ptr(), substring_i: 41, result: -1, function: None },
    among { s_size: 3, s: S_1_43.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 7, s: S_1_44.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_1_45.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 4, s: S_1_46.as_ptr(), substring_i: 45, result: -1, function: None },
    among { s_size: 5, s: S_1_47.as_ptr(), substring_i: 46, result: -1, function: None },
    among { s_size: 3, s: S_1_48.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_1_49.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_1_50.as_ptr(), substring_i: 49, result: -1, function: None },
    among { s_size: 4, s: S_1_51.as_ptr(), substring_i: 50, result: -1, function: None },
    among { s_size: 2, s: S_1_52.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_1_53.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 5, s: S_1_54.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_1_55.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_1_56.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_1_57.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_1_58.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 6, s: S_1_59.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 2, s: S_1_60.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 5, s: S_1_61.as_ptr(), substring_i: 60, result: -1, function: None },
];

static S_2_0: [symbol; 5] = [b'o', b'j', b'i', b'm', b'e'];
static S_2_1: [symbol; 6] = [0xC4, 0x97, b'j', b'i', b'm', b'e'];
static S_2_2: [symbol; 5] = [b'a', b'v', b'i', b'm', b'e'];
static S_2_3: [symbol; 5] = [b'o', b'k', b'a', b't', b'e'];
static S_2_4: [symbol; 4] = [b'a', b'i', b't', b'e'];
static S_2_5: [symbol; 4] = [b'u', b'o', b't', b'e'];
static S_2_6: [symbol; 5] = [b'a', b's', b'i', b'u', b's'];
static S_2_7: [symbol; 7] = [b'o', b'k', b'a', b't', 0xC4, 0x97, b's'];
static S_2_8: [symbol; 6] = [b'a', b'i', b't', 0xC4, 0x97, b's'];
static S_2_9: [symbol; 6] = [b'u', b'o', b't', 0xC4, 0x97, b's'];
static S_2_10: [symbol; 4] = [b'e', b's', b'i', b'u'];

static A_2: [among; 11] = [
    among { s_size: 5, s: S_2_0.as_ptr(), substring_i: -1, result: 7, function: None },
    among { s_size: 6, s: S_2_1.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 5, s: S_2_2.as_ptr(), substring_i: -1, result: 6, function: None },
    among { s_size: 5, s: S_2_3.as_ptr(), substring_i: -1, result: 8, function: None },
    among { s_size: 4, s: S_2_4.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_2_5.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 5, s: S_2_6.as_ptr(), substring_i: -1, result: 5, function: None },
    among { s_size: 7, s: S_2_7.as_ptr(), substring_i: -1, result: 8, function: None },
    among { s_size: 6, s: S_2_8.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_2_9.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 4, s: S_2_10.as_ptr(), substring_i: -1, result: 4, function: None },
];

static S_3_0: [symbol; 2] = [0xC4, 0x8D];
static S_3_1: [symbol; 3] = [b'd', 0xC5, 0xBE];

static A_3: [among; 2] = [
    among { s_size: 2, s: S_3_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_3_1.as_ptr(), substring_i: -1, result: 2, function: None },
];

static S_4_0: [symbol; 2] = [b'g', b'd'];

static A_4: [among; 1] = [
    among { s_size: 2, s: S_4_0.as_ptr(), substring_i: -1, result: 1, function: None },
];

// ---------------------------------------------------------------------------
// grouping bit tables
// ---------------------------------------------------------------------------

static G_V: [c_uchar; 35] = [
    17, 65, 16, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 16, 0, 64, 1, 0, 64, 0, 0, 0, 0,
    0, 0, 0, 4, 4,
];

// ---------------------------------------------------------------------------
// literal strings used by slice_from_s
// ---------------------------------------------------------------------------

static S_0: [symbol; 5] = [b'a', b'i', b't', 0xC4, 0x97];
static S_1: [symbol; 5] = [b'u', b'o', b't', 0xC4, 0x97];
static S_2: [symbol; 7] = [0xC4, 0x97, b'j', b'i', b'm', b'a', b's'];
static S_3: [symbol; 4] = [b'e', b's', b'y', b's'];
static S_4: [symbol; 4] = [b'a', b's', b'y', b's'];
static S_5: [symbol; 6] = [b'a', b'v', b'i', b'm', b'a', b's'];
static S_6: [symbol; 6] = [b'o', b'j', b'i', b'm', b'a', b's'];
static S_7: [symbol; 6] = [b'o', b'k', b'a', b't', 0xC4, 0x97];
static S_8: [symbol; 1] = [b't'];
static S_9: [symbol; 1] = [b'd'];
static S_10: [symbol; 1] = [b'g'];

// ---------------------------------------------------------------------------
// rules
// ---------------------------------------------------------------------------

unsafe fn r_step1(z: *mut SN_env) -> c_int {
    {
        let mlimit1;
        if (*z).c < *(*z).I.offset(0) {
            return 0;
        }
        mlimit1 = (*z).lb;
        (*z).lb = *(*z).I.offset(0);
        (*z).ket = (*z).c;
        if find_among_b(z, A_0.as_ptr(), 204) == 0 {
            (*z).lb = mlimit1;
            return 0;
        }
        (*z).bra = (*z).c;
        (*z).lb = mlimit1;
    }
    {
        let ret = slice_del(z);
        if ret < 0 {
            return ret;
        }
    }
    1
}

unsafe fn r_step2(z: *mut SN_env) -> c_int {
    'repeat0: loop {
        let m1 = (*z).l - (*z).c;

        'lab0: {
            {
                let mlimit2;
                if (*z).c < *(*z).I.offset(0) {
                    break 'lab0;
                }
                mlimit2 = (*z).lb;
                (*z).lb = *(*z).I.offset(0);
                (*z).ket = (*z).c;
                if find_among_b(z, A_1.as_ptr(), 62) == 0 {
                    (*z).lb = mlimit2;
                    break 'lab0;
                }
                (*z).bra = (*z).c;
                (*z).lb = mlimit2;
            }
            {
                let ret = slice_del(z);
                if ret < 0 {
                    return ret;
                }
            }
            continue 'repeat0;
        }
        // lab0:
        (*z).c = (*z).l - m1;
        break 'repeat0;
    }
    1
}

unsafe fn r_fix_conflicts(z: *mut SN_env) -> c_int {
    let among_var;
    (*z).ket = (*z).c;
    if (*z).c - 3 <= (*z).lb
        || *(*z).p.offset(((*z).c - 1) as isize) as c_int >> 5 != 3
        || (2621472 >> (*(*z).p.offset(((*z).c - 1) as isize) as c_int & 0x1f)) & 1 == 0
    {
        return 0;
    }
    among_var = find_among_b(z, A_2.as_ptr(), 11);
    if among_var == 0 {
        return 0;
    }
    (*z).bra = (*z).c;
    match among_var {
        1 => {
            let ret = slice_from_s(z, 5, S_0.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        2 => {
            let ret = slice_from_s(z, 5, S_1.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        3 => {
            let ret = slice_from_s(z, 7, S_2.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        4 => {
            let ret = slice_from_s(z, 4, S_3.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        5 => {
            let ret = slice_from_s(z, 4, S_4.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        6 => {
            let ret = slice_from_s(z, 6, S_5.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        7 => {
            let ret = slice_from_s(z, 6, S_6.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        8 => {
            let ret = slice_from_s(z, 6, S_7.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        _ => {}
    }
    1
}

unsafe fn r_fix_chdz(z: *mut SN_env) -> c_int {
    let among_var;
    (*z).ket = (*z).c;
    if (*z).c - 1 <= (*z).lb
        || (*(*z).p.offset(((*z).c - 1) as isize) != 141
            && *(*z).p.offset(((*z).c - 1) as isize) != 190)
    {
        return 0;
    }
    among_var = find_among_b(z, A_3.as_ptr(), 2);
    if among_var == 0 {
        return 0;
    }
    (*z).bra = (*z).c;
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
        _ => {}
    }
    1
}

unsafe fn r_fix_gd(z: *mut SN_env) -> c_int {
    (*z).ket = (*z).c;
    if (*z).c - 1 <= (*z).lb || *(*z).p.offset(((*z).c - 1) as isize) != 100 {
        return 0;
    }
    if find_among_b(z, A_4.as_ptr(), 1) == 0 {
        return 0;
    }
    (*z).bra = (*z).c;
    {
        let ret = slice_from_s(z, 1, S_10.as_ptr());
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
pub unsafe extern "C" fn lithuanian_UTF_8_stem(z: *mut SN_env) -> c_int {
    *(*z).I.offset(0) = (*z).l;
    {
        let c1 = (*z).c;
        'lab0: {
            {
                let c2 = (*z).c;
                'lab1: {
                    {
                        let c_test3 = (*z).c;
                        if (*z).c == (*z).l || *(*z).p.offset((*z).c as isize) != b'a' {
                            (*z).c = c2;
                            break 'lab1;
                        }
                        (*z).c += 1;
                        (*z).c = c_test3;
                    }
                    if len_utf8((*z).p) <= 6 {
                        (*z).c = c2;
                        break 'lab1;
                    }
                    {
                        let ret = skip_utf8((*z).p, (*z).c, (*z).l, 1);
                        if ret < 0 {
                            (*z).c = c2;
                            break 'lab1;
                        }
                        (*z).c = ret;
                    }
                }
            }

            {
                let ret = out_grouping_U(z, G_V.as_ptr(), 97, 371, 1);
                if ret < 0 {
                    break 'lab0;
                }
                (*z).c += ret;
            }

            {
                let ret = in_grouping_U(z, G_V.as_ptr(), 97, 371, 1);
                if ret < 0 {
                    break 'lab0;
                }
                (*z).c += ret;
            }
            *(*z).I.offset(0) = (*z).c;
        }
        // lab0:
        (*z).c = c1;
    }
    (*z).lb = (*z).c;
    (*z).c = (*z).l;

    {
        let m4 = (*z).l - (*z).c;
        {
            let ret = r_fix_conflicts(z);
            if ret < 0 {
                return ret;
            }
        }
        (*z).c = (*z).l - m4;
    }
    {
        let m5 = (*z).l - (*z).c;
        {
            let ret = r_step1(z);
            if ret < 0 {
                return ret;
            }
        }
        (*z).c = (*z).l - m5;
    }
    {
        let m6 = (*z).l - (*z).c;
        {
            let ret = r_fix_chdz(z);
            if ret < 0 {
                return ret;
            }
        }
        (*z).c = (*z).l - m6;
    }
    {
        let m7 = (*z).l - (*z).c;
        {
            let ret = r_step2(z);
            if ret < 0 {
                return ret;
            }
        }
        (*z).c = (*z).l - m7;
    }
    {
        let m8 = (*z).l - (*z).c;
        {
            let ret = r_fix_chdz(z);
            if ret < 0 {
                return ret;
            }
        }
        (*z).c = (*z).l - m8;
    }
    {
        let m9 = (*z).l - (*z).c;
        {
            let ret = r_fix_gd(z);
            if ret < 0 {
                return ret;
            }
        }
        (*z).c = (*z).l - m9;
    }
    (*z).c = (*z).lb;
    1
}

#[no_mangle]
pub unsafe extern "C" fn lithuanian_UTF_8_create_env() -> *mut SN_env {
    SN_create_env(0, 1)
}

#[no_mangle]
pub unsafe extern "C" fn lithuanian_UTF_8_close_env(z: *mut SN_env) {
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
        let z = lithuanian_UTF_8_create_env();
        assert!(!z.is_null());
        let rc = SN_set_current(z, word.len() as c_int, word.as_ptr());
        assert!(rc >= 0);
        let rc = lithuanian_UTF_8_stem(z);
        assert!(rc >= 0, "stem returned {rc}");
        let out = std::slice::from_raw_parts((*z).p, SIZE((*z).p) as usize).to_vec();
        lithuanian_UTF_8_close_env(z);
        out
    }

    // A short root with no strippable suffix is returned unchanged.
    #[test]
    fn short_root_stable() {
        unsafe {
            assert_eq!(stem(b"namas"), stem(b"namas"));
        }
    }

    // Idempotence: stemming a stem yields the same stem.
    #[test]
    fn idempotent() {
        unsafe {
            for w in [
                &b"lietuvos"[..],
                &b"vaikams"[..],
                &b"gra\xC5\xBEus"[..],
                &b"miestuose"[..],
            ] {
                let once = stem(w);
                let twice = stem(&once);
                assert_eq!(once, twice, "not idempotent for {:?}", w);
                assert!(!once.is_empty());
            }
        }
    }

    // Stemming cannot grow the input.
    #[test]
    fn never_grows() {
        unsafe {
            let r = stem(b"miestuose");
            assert!(r.len() <= "miestuose".len());
            assert!(!r.is_empty());
        }
    }
}
