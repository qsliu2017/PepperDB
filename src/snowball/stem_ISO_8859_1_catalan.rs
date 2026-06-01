//! Snowball Catalan (ISO_8859_1) stemmer.
//!
//! 1:1 translation of the generated
//! `src/backend/snowball/libstemmer/stem_ISO_8859_1_catalan.c` (Snowball 2.2.0),
//! merged with its declarations header
//! `src/include/snowball/libstemmer/stem_ISO_8859_1_catalan.h`.
//!
//! ISO_8859_1 is a SINGLE-BYTE encoding: this uses the non-`_U` grouping
//! helpers and plain single-byte advance (`z->c++` / `z->c--`) instead of the
//! UTF-8 `skip_*_utf8` helpers.
//!
//! The libstemmer runtime is the ported Rust runtime in `crate::snowball::{api,
//! utilities}`; this file only contains the language-specific generated tables
//! and the step functions plus the three exported env/stem entry points.

use crate::prelude::*;

use crate::snowball::api::{among, symbol, SN_close_env, SN_create_env, SN_env, SIZE};
use crate::snowball::utilities::{
    find_among, find_among_b, in_grouping, out_grouping, slice_del, slice_from_s,
};

// ---------------------------------------------------------------------------
// among string tables
// ---------------------------------------------------------------------------

static S_0_1: [symbol; 1] = [0xb7];
static S_0_2: [symbol; 1] = [0xe0];
static S_0_3: [symbol; 1] = [0xe1];
static S_0_4: [symbol; 1] = [0xe8];
static S_0_5: [symbol; 1] = [0xe9];
static S_0_6: [symbol; 1] = [0xec];
static S_0_7: [symbol; 1] = [0xed];
static S_0_8: [symbol; 1] = [0xef];
static S_0_9: [symbol; 1] = [0xf2];
static S_0_10: [symbol; 1] = [0xf3];
static S_0_11: [symbol; 1] = [0xfa];
static S_0_12: [symbol; 1] = [0xfc];
static S_1_0: [symbol; 2] = [b'l', b'a'];
static S_1_1: [symbol; 3] = [b'-', b'l', b'a'];
static S_1_2: [symbol; 4] = [b's', b'e', b'l', b'a'];
static S_1_3: [symbol; 2] = [b'l', b'e'];
static S_1_4: [symbol; 2] = [b'm', b'e'];
static S_1_5: [symbol; 3] = [b'-', b'm', b'e'];
static S_1_6: [symbol; 2] = [b's', b'e'];
static S_1_7: [symbol; 3] = [b'-', b't', b'e'];
static S_1_8: [symbol; 2] = [b'h', b'i'];
static S_1_9: [symbol; 3] = [b'\'', b'h', b'i'];
static S_1_10: [symbol; 2] = [b'l', b'i'];
static S_1_11: [symbol; 3] = [b'-', b'l', b'i'];
static S_1_12: [symbol; 2] = [b'\'', b'l'];
static S_1_13: [symbol; 2] = [b'\'', b'm'];
static S_1_14: [symbol; 2] = [b'-', b'm'];
static S_1_15: [symbol; 2] = [b'\'', b'n'];
static S_1_16: [symbol; 2] = [b'-', b'n'];
static S_1_17: [symbol; 2] = [b'h', b'o'];
static S_1_18: [symbol; 3] = [b'\'', b'h', b'o'];
static S_1_19: [symbol; 2] = [b'l', b'o'];
static S_1_20: [symbol; 4] = [b's', b'e', b'l', b'o'];
static S_1_21: [symbol; 2] = [b'\'', b's'];
static S_1_22: [symbol; 3] = [b'l', b'a', b's'];
static S_1_23: [symbol; 5] = [b's', b'e', b'l', b'a', b's'];
static S_1_24: [symbol; 3] = [b'l', b'e', b's'];
static S_1_25: [symbol; 4] = [b'-', b'l', b'e', b's'];
static S_1_26: [symbol; 3] = [b'\'', b'l', b's'];
static S_1_27: [symbol; 3] = [b'-', b'l', b's'];
static S_1_28: [symbol; 3] = [b'\'', b'n', b's'];
static S_1_29: [symbol; 3] = [b'-', b'n', b's'];
static S_1_30: [symbol; 3] = [b'e', b'n', b's'];
static S_1_31: [symbol; 3] = [b'l', b'o', b's'];
static S_1_32: [symbol; 5] = [b's', b'e', b'l', b'o', b's'];
static S_1_33: [symbol; 3] = [b'n', b'o', b's'];
static S_1_34: [symbol; 4] = [b'-', b'n', b'o', b's'];
static S_1_35: [symbol; 3] = [b'v', b'o', b's'];
static S_1_36: [symbol; 2] = [b'u', b's'];
static S_1_37: [symbol; 3] = [b'-', b'u', b's'];
static S_1_38: [symbol; 2] = [b'\'', b't'];
static S_2_0: [symbol; 3] = [b'i', b'c', b'a'];
static S_2_1: [symbol; 6] = [b'l', 0xf3, b'g', b'i', b'c', b'a'];
static S_2_2: [symbol; 4] = [b'e', b'n', b'c', b'a'];
static S_2_3: [symbol; 3] = [b'a', b'd', b'a'];
static S_2_4: [symbol; 5] = [b'a', b'n', b'c', b'i', b'a'];
static S_2_5: [symbol; 5] = [b'e', b'n', b'c', b'i', b'a'];
static S_2_6: [symbol; 5] = [0xe8, b'n', b'c', b'i', b'a'];
static S_2_7: [symbol; 4] = [0xed, b'c', b'i', b'a'];
static S_2_8: [symbol; 5] = [b'l', b'o', b'g', b'i', b'a'];
static S_2_9: [symbol; 4] = [b'i', b'n', b'i', b'a'];
static S_2_10: [symbol; 5] = [0xed, b'i', b'n', b'i', b'a'];
static S_2_11: [symbol; 4] = [b'e', b'r', b'i', b'a'];
static S_2_12: [symbol; 4] = [0xe0, b'r', b'i', b'a'];
static S_2_13: [symbol; 6] = [b'a', b't', 0xf2, b'r', b'i', b'a'];
static S_2_14: [symbol; 4] = [b'a', b'l', b'l', b'a'];
static S_2_15: [symbol; 4] = [b'e', b'l', b'l', b'a'];
static S_2_16: [symbol; 5] = [0xed, b'v', b'o', b'l', b'a'];
static S_2_17: [symbol; 3] = [b'i', b'm', b'a'];
static S_2_18: [symbol; 6] = [0xed, b's', b's', b'i', b'm', b'a'];
static S_2_19: [symbol; 8] = [b'q', b'u', 0xed, b's', b's', b'i', b'm', b'a'];
static S_2_20: [symbol; 3] = [b'a', b'n', b'a'];
static S_2_21: [symbol; 3] = [b'i', b'n', b'a'];
static S_2_22: [symbol; 3] = [b'e', b'r', b'a'];
static S_2_23: [symbol; 5] = [b's', b'f', b'e', b'r', b'a'];
static S_2_24: [symbol; 3] = [b'o', b'r', b'a'];
static S_2_25: [symbol; 4] = [b'd', b'o', b'r', b'a'];
static S_2_26: [symbol; 5] = [b'a', b'd', b'o', b'r', b'a'];
static S_2_27: [symbol; 5] = [b'a', b'd', b'u', b'r', b'a'];
static S_2_28: [symbol; 3] = [b'e', b's', b'a'];
static S_2_29: [symbol; 3] = [b'o', b's', b'a'];
static S_2_30: [symbol; 4] = [b'a', b's', b's', b'a'];
static S_2_31: [symbol; 4] = [b'e', b's', b's', b'a'];
static S_2_32: [symbol; 4] = [b'i', b's', b's', b'a'];
static S_2_33: [symbol; 3] = [b'e', b't', b'a'];
static S_2_34: [symbol; 3] = [b'i', b't', b'a'];
static S_2_35: [symbol; 3] = [b'o', b't', b'a'];
static S_2_36: [symbol; 4] = [b'i', b's', b't', b'a'];
static S_2_37: [symbol; 7] = [b'i', b'a', b'l', b'i', b's', b't', b'a'];
static S_2_38: [symbol; 7] = [b'i', b'o', b'n', b'i', b's', b't', b'a'];
static S_2_39: [symbol; 3] = [b'i', b'v', b'a'];
static S_2_40: [symbol; 5] = [b'a', b't', b'i', b'v', b'a'];
static S_2_41: [symbol; 3] = [b'n', 0xe7, b'a'];
static S_2_42: [symbol; 5] = [b'l', b'o', b'g', 0xed, b'a'];
static S_2_43: [symbol; 2] = [b'i', b'c'];
static S_2_44: [symbol; 5] = [0xed, b's', b't', b'i', b'c'];
static S_2_45: [symbol; 3] = [b'e', b'n', b'c'];
static S_2_46: [symbol; 3] = [b'e', b's', b'c'];
static S_2_47: [symbol; 2] = [b'u', b'd'];
static S_2_48: [symbol; 4] = [b'a', b't', b'g', b'e'];
static S_2_49: [symbol; 3] = [b'b', b'l', b'e'];
static S_2_50: [symbol; 4] = [b'a', b'b', b'l', b'e'];
static S_2_51: [symbol; 4] = [b'i', b'b', b'l', b'e'];
static S_2_52: [symbol; 4] = [b'i', b's', b'm', b'e'];
static S_2_53: [symbol; 7] = [b'i', b'a', b'l', b'i', b's', b'm', b'e'];
static S_2_54: [symbol; 7] = [b'i', b'o', b'n', b'i', b's', b'm', b'e'];
static S_2_55: [symbol; 6] = [b'i', b'v', b'i', b's', b'm', b'e'];
static S_2_56: [symbol; 4] = [b'a', b'i', b'r', b'e'];
static S_2_57: [symbol; 4] = [b'i', b'c', b't', b'e'];
static S_2_58: [symbol; 4] = [b'i', b's', b't', b'e'];
static S_2_59: [symbol; 3] = [b'i', b'c', b'i'];
static S_2_60: [symbol; 3] = [0xed, b'c', b'i'];
static S_2_61: [symbol; 4] = [b'l', b'o', b'g', b'i'];
static S_2_62: [symbol; 3] = [b'a', b'r', b'i'];
static S_2_63: [symbol; 4] = [b't', b'o', b'r', b'i'];
static S_2_64: [symbol; 2] = [b'a', b'l'];
static S_2_65: [symbol; 2] = [b'i', b'l'];
static S_2_66: [symbol; 3] = [b'a', b'l', b'l'];
static S_2_67: [symbol; 3] = [b'e', b'l', b'l'];
static S_2_68: [symbol; 4] = [0xed, b'v', b'o', b'l'];
static S_2_69: [symbol; 4] = [b'i', b's', b'a', b'm'];
static S_2_70: [symbol; 5] = [b'i', b's', b's', b'e', b'm'];
static S_2_71: [symbol; 5] = [0xec, b's', b's', b'e', b'm'];
static S_2_72: [symbol; 5] = [0xed, b's', b's', b'e', b'm'];
static S_2_73: [symbol; 5] = [0xed, b's', b's', b'i', b'm'];
static S_2_74: [symbol; 7] = [b'q', b'u', 0xed, b's', b's', b'i', b'm'];
static S_2_75: [symbol; 4] = [b'a', b'm', b'e', b'n'];
static S_2_76: [symbol; 5] = [0xec, b's', b's', b'i', b'n'];
static S_2_77: [symbol; 2] = [b'a', b'r'];
static S_2_78: [symbol; 6] = [b'i', b'f', b'i', b'c', b'a', b'r'];
static S_2_79: [symbol; 4] = [b'e', b'g', b'a', b'r'];
static S_2_80: [symbol; 4] = [b'e', b'j', b'a', b'r'];
static S_2_81: [symbol; 4] = [b'i', b't', b'a', b'r'];
static S_2_82: [symbol; 5] = [b'i', b't', b'z', b'a', b'r'];
static S_2_83: [symbol; 3] = [b'f', b'e', b'r'];
static S_2_84: [symbol; 2] = [b'o', b'r'];
static S_2_85: [symbol; 3] = [b'd', b'o', b'r'];
static S_2_86: [symbol; 3] = [b'd', b'u', b'r'];
static S_2_87: [symbol; 5] = [b'd', b'o', b'r', b'a', b's'];
static S_2_88: [symbol; 3] = [b'i', b'c', b's'];
static S_2_89: [symbol; 6] = [b'l', 0xf3, b'g', b'i', b'c', b's'];
static S_2_90: [symbol; 3] = [b'u', b'd', b's'];
static S_2_91: [symbol; 4] = [b'n', b'c', b'e', b's'];
static S_2_92: [symbol; 4] = [b'a', b'd', b'e', b's'];
static S_2_93: [symbol; 6] = [b'a', b'n', b'c', b'i', b'e', b's'];
static S_2_94: [symbol; 6] = [b'e', b'n', b'c', b'i', b'e', b's'];
static S_2_95: [symbol; 6] = [0xe8, b'n', b'c', b'i', b'e', b's'];
static S_2_96: [symbol; 5] = [0xed, b'c', b'i', b'e', b's'];
static S_2_97: [symbol; 6] = [b'l', b'o', b'g', b'i', b'e', b's'];
static S_2_98: [symbol; 5] = [b'i', b'n', b'i', b'e', b's'];
static S_2_99: [symbol; 5] = [0xed, b'n', b'i', b'e', b's'];
static S_2_100: [symbol; 5] = [b'e', b'r', b'i', b'e', b's'];
static S_2_101: [symbol; 5] = [0xe0, b'r', b'i', b'e', b's'];
static S_2_102: [symbol; 7] = [b'a', b't', 0xf2, b'r', b'i', b'e', b's'];
static S_2_103: [symbol; 4] = [b'b', b'l', b'e', b's'];
static S_2_104: [symbol; 5] = [b'a', b'b', b'l', b'e', b's'];
static S_2_105: [symbol; 5] = [b'i', b'b', b'l', b'e', b's'];
static S_2_106: [symbol; 4] = [b'i', b'm', b'e', b's'];
static S_2_107: [symbol; 7] = [0xed, b's', b's', b'i', b'm', b'e', b's'];
static S_2_108: [symbol; 9] = [b'q', b'u', 0xed, b's', b's', b'i', b'm', b'e', b's'];
static S_2_109: [symbol; 6] = [b'f', b'o', b'r', b'm', b'e', b's'];
static S_2_110: [symbol; 5] = [b'i', b's', b'm', b'e', b's'];
static S_2_111: [symbol; 8] = [b'i', b'a', b'l', b'i', b's', b'm', b'e', b's'];
static S_2_112: [symbol; 4] = [b'i', b'n', b'e', b's'];
static S_2_113: [symbol; 4] = [b'e', b'r', b'e', b's'];
static S_2_114: [symbol; 4] = [b'o', b'r', b'e', b's'];
static S_2_115: [symbol; 5] = [b'd', b'o', b'r', b'e', b's'];
static S_2_116: [symbol; 6] = [b'i', b'd', b'o', b'r', b'e', b's'];
static S_2_117: [symbol; 5] = [b'd', b'u', b'r', b'e', b's'];
static S_2_118: [symbol; 4] = [b'e', b's', b'e', b's'];
static S_2_119: [symbol; 4] = [b'o', b's', b'e', b's'];
static S_2_120: [symbol; 5] = [b'a', b's', b's', b'e', b's'];
static S_2_121: [symbol; 5] = [b'i', b'c', b't', b'e', b's'];
static S_2_122: [symbol; 4] = [b'i', b't', b'e', b's'];
static S_2_123: [symbol; 4] = [b'o', b't', b'e', b's'];
static S_2_124: [symbol; 5] = [b'i', b's', b't', b'e', b's'];
static S_2_125: [symbol; 8] = [b'i', b'a', b'l', b'i', b's', b't', b'e', b's'];
static S_2_126: [symbol; 8] = [b'i', b'o', b'n', b'i', b's', b't', b'e', b's'];
static S_2_127: [symbol; 5] = [b'i', b'q', b'u', b'e', b's'];
static S_2_128: [symbol; 8] = [b'l', 0xf3, b'g', b'i', b'q', b'u', b'e', b's'];
static S_2_129: [symbol; 4] = [b'i', b'v', b'e', b's'];
static S_2_130: [symbol; 6] = [b'a', b't', b'i', b'v', b'e', b's'];
static S_2_131: [symbol; 6] = [b'l', b'o', b'g', 0xed, b'e', b's'];
static S_2_132: [symbol; 9] = [b'a', b'l', b'l', b'e', b'n', b'g', 0xfc, b'e', b's'];
static S_2_133: [symbol; 4] = [b'i', b'c', b'i', b's'];
static S_2_134: [symbol; 4] = [0xed, b'c', b'i', b's'];
static S_2_135: [symbol; 5] = [b'l', b'o', b'g', b'i', b's'];
static S_2_136: [symbol; 4] = [b'a', b'r', b'i', b's'];
static S_2_137: [symbol; 5] = [b't', b'o', b'r', b'i', b's'];
static S_2_138: [symbol; 2] = [b'l', b's'];
static S_2_139: [symbol; 3] = [b'a', b'l', b's'];
static S_2_140: [symbol; 4] = [b'e', b'l', b'l', b's'];
static S_2_141: [symbol; 3] = [b'i', b'm', b's'];
static S_2_142: [symbol; 6] = [0xed, b's', b's', b'i', b'm', b's'];
static S_2_143: [symbol; 8] = [b'q', b'u', 0xed, b's', b's', b'i', b'm', b's'];
static S_2_144: [symbol; 4] = [b'i', b'o', b'n', b's'];
static S_2_145: [symbol; 5] = [b'c', b'i', b'o', b'n', b's'];
static S_2_146: [symbol; 6] = [b'a', b'c', b'i', b'o', b'n', b's'];
static S_2_147: [symbol; 4] = [b'e', b's', b'o', b's'];
static S_2_148: [symbol; 4] = [b'o', b's', b'o', b's'];
static S_2_149: [symbol; 5] = [b'a', b's', b's', b'o', b's'];
static S_2_150: [symbol; 5] = [b'i', b's', b's', b'o', b's'];
static S_2_151: [symbol; 3] = [b'e', b'r', b's'];
static S_2_152: [symbol; 3] = [b'o', b'r', b's'];
static S_2_153: [symbol; 4] = [b'd', b'o', b'r', b's'];
static S_2_154: [symbol; 5] = [b'a', b'd', b'o', b'r', b's'];
static S_2_155: [symbol; 5] = [b'i', b'd', b'o', b'r', b's'];
static S_2_156: [symbol; 3] = [b'a', b't', b's'];
static S_2_157: [symbol; 5] = [b'i', b't', b'a', b't', b's'];
static S_2_158: [symbol; 8] = [b'b', b'i', b'l', b'i', b't', b'a', b't', b's'];
static S_2_159: [symbol; 7] = [b'i', b'v', b'i', b't', b'a', b't', b's'];
static S_2_160: [symbol; 9] = [b'a', b't', b'i', b'v', b'i', b't', b'a', b't', b's'];
static S_2_161: [symbol; 5] = [0xef, b't', b'a', b't', b's'];
static S_2_162: [symbol; 3] = [b'e', b't', b's'];
static S_2_163: [symbol; 4] = [b'a', b'n', b't', b's'];
static S_2_164: [symbol; 4] = [b'e', b'n', b't', b's'];
static S_2_165: [symbol; 5] = [b'm', b'e', b'n', b't', b's'];
static S_2_166: [symbol; 6] = [b'a', b'm', b'e', b'n', b't', b's'];
static S_2_167: [symbol; 3] = [b'o', b't', b's'];
static S_2_168: [symbol; 3] = [b'u', b't', b's'];
static S_2_169: [symbol; 3] = [b'i', b'u', b's'];
static S_2_170: [symbol; 5] = [b't', b'r', b'i', b'u', b's'];
static S_2_171: [symbol; 5] = [b'a', b't', b'i', b'u', b's'];
static S_2_172: [symbol; 2] = [0xe8, b's'];
static S_2_173: [symbol; 2] = [0xe9, b's'];
static S_2_174: [symbol; 2] = [0xed, b's'];
static S_2_175: [symbol; 3] = [b'd', 0xed, b's'];
static S_2_176: [symbol; 2] = [0xf3, b's'];
static S_2_177: [symbol; 4] = [b'i', b't', b'a', b't'];
static S_2_178: [symbol; 7] = [b'b', b'i', b'l', b'i', b't', b'a', b't'];
static S_2_179: [symbol; 6] = [b'i', b'v', b'i', b't', b'a', b't'];
static S_2_180: [symbol; 8] = [b'a', b't', b'i', b'v', b'i', b't', b'a', b't'];
static S_2_181: [symbol; 4] = [0xef, b't', b'a', b't'];
static S_2_182: [symbol; 2] = [b'e', b't'];
static S_2_183: [symbol; 3] = [b'a', b'n', b't'];
static S_2_184: [symbol; 3] = [b'e', b'n', b't'];
static S_2_185: [symbol; 4] = [b'i', b'e', b'n', b't'];
static S_2_186: [symbol; 4] = [b'm', b'e', b'n', b't'];
static S_2_187: [symbol; 5] = [b'a', b'm', b'e', b'n', b't'];
static S_2_188: [symbol; 7] = [b'i', b's', b'a', b'm', b'e', b'n', b't'];
static S_2_189: [symbol; 2] = [b'o', b't'];
static S_2_190: [symbol; 5] = [b'i', b's', b's', b'e', b'u'];
static S_2_191: [symbol; 5] = [0xec, b's', b's', b'e', b'u'];
static S_2_192: [symbol; 5] = [0xed, b's', b's', b'e', b'u'];
static S_2_193: [symbol; 4] = [b't', b'r', b'i', b'u'];
static S_2_194: [symbol; 5] = [0xed, b's', b's', b'i', b'u'];
static S_2_195: [symbol; 4] = [b'a', b't', b'i', b'u'];
static S_2_196: [symbol; 1] = [0xf3];
static S_2_197: [symbol; 2] = [b'i', 0xf3];
static S_2_198: [symbol; 3] = [b'c', b'i', 0xf3];
static S_2_199: [symbol; 4] = [b'a', b'c', b'i', 0xf3];
static S_3_0: [symbol; 3] = [b'a', b'b', b'a'];
static S_3_1: [symbol; 4] = [b'e', b's', b'c', b'a'];
static S_3_2: [symbol; 4] = [b'i', b's', b'c', b'a'];
static S_3_3: [symbol; 4] = [0xef, b's', b'c', b'a'];
static S_3_4: [symbol; 3] = [b'a', b'd', b'a'];
static S_3_5: [symbol; 3] = [b'i', b'd', b'a'];
static S_3_6: [symbol; 3] = [b'u', b'd', b'a'];
static S_3_7: [symbol; 3] = [0xef, b'd', b'a'];
static S_3_8: [symbol; 2] = [b'i', b'a'];
static S_3_9: [symbol; 4] = [b'a', b'r', b'i', b'a'];
static S_3_10: [symbol; 4] = [b'i', b'r', b'i', b'a'];
static S_3_11: [symbol; 3] = [b'a', b'r', b'a'];
static S_3_12: [symbol; 4] = [b'i', b'e', b'r', b'a'];
static S_3_13: [symbol; 3] = [b'i', b'r', b'a'];
static S_3_14: [symbol; 5] = [b'a', b'd', b'o', b'r', b'a'];
static S_3_15: [symbol; 3] = [0xef, b'r', b'a'];
static S_3_16: [symbol; 3] = [b'a', b'v', b'a'];
static S_3_17: [symbol; 3] = [b'i', b'x', b'a'];
static S_3_18: [symbol; 4] = [b'i', b't', b'z', b'a'];
static S_3_19: [symbol; 2] = [0xed, b'a'];
static S_3_20: [symbol; 4] = [b'a', b'r', 0xed, b'a'];
static S_3_21: [symbol; 4] = [b'e', b'r', 0xed, b'a'];
static S_3_22: [symbol; 4] = [b'i', b'r', 0xed, b'a'];
static S_3_23: [symbol; 2] = [0xef, b'a'];
static S_3_24: [symbol; 3] = [b'i', b's', b'c'];
static S_3_25: [symbol; 3] = [0xef, b's', b'c'];
static S_3_26: [symbol; 2] = [b'a', b'd'];
static S_3_27: [symbol; 2] = [b'e', b'd'];
static S_3_28: [symbol; 2] = [b'i', b'd'];
static S_3_29: [symbol; 2] = [b'i', b'e'];
static S_3_30: [symbol; 2] = [b'r', b'e'];
static S_3_31: [symbol; 3] = [b'd', b'r', b'e'];
static S_3_32: [symbol; 3] = [b'a', b's', b'e'];
static S_3_33: [symbol; 4] = [b'i', b'e', b's', b'e'];
static S_3_34: [symbol; 4] = [b'a', b's', b't', b'e'];
static S_3_35: [symbol; 4] = [b'i', b's', b't', b'e'];
static S_3_36: [symbol; 2] = [b'i', b'i'];
static S_3_37: [symbol; 3] = [b'i', b'n', b'i'];
static S_3_38: [symbol; 5] = [b'e', b's', b'q', b'u', b'i'];
static S_3_39: [symbol; 4] = [b'e', b'i', b'x', b'i'];
static S_3_40: [symbol; 4] = [b'i', b't', b'z', b'i'];
static S_3_41: [symbol; 2] = [b'a', b'm'];
static S_3_42: [symbol; 2] = [b'e', b'm'];
static S_3_43: [symbol; 4] = [b'a', b'r', b'e', b'm'];
static S_3_44: [symbol; 4] = [b'i', b'r', b'e', b'm'];
static S_3_45: [symbol; 4] = [0xe0, b'r', b'e', b'm'];
static S_3_46: [symbol; 4] = [0xed, b'r', b'e', b'm'];
static S_3_47: [symbol; 5] = [0xe0, b's', b's', b'e', b'm'];
static S_3_48: [symbol; 5] = [0xe9, b's', b's', b'e', b'm'];
static S_3_49: [symbol; 5] = [b'i', b'g', b'u', b'e', b'm'];
static S_3_50: [symbol; 5] = [0xef, b'g', b'u', b'e', b'm'];
static S_3_51: [symbol; 4] = [b'a', b'v', b'e', b'm'];
static S_3_52: [symbol; 4] = [0xe0, b'v', b'e', b'm'];
static S_3_53: [symbol; 4] = [0xe1, b'v', b'e', b'm'];
static S_3_54: [symbol; 5] = [b'i', b'r', 0xec, b'e', b'm'];
static S_3_55: [symbol; 3] = [0xed, b'e', b'm'];
static S_3_56: [symbol; 5] = [b'a', b'r', 0xed, b'e', b'm'];
static S_3_57: [symbol; 5] = [b'i', b'r', 0xed, b'e', b'm'];
static S_3_58: [symbol; 5] = [b'a', b's', b's', b'i', b'm'];
static S_3_59: [symbol; 5] = [b'e', b's', b's', b'i', b'm'];
static S_3_60: [symbol; 5] = [b'i', b's', b's', b'i', b'm'];
static S_3_61: [symbol; 5] = [0xe0, b's', b's', b'i', b'm'];
static S_3_62: [symbol; 5] = [0xe8, b's', b's', b'i', b'm'];
static S_3_63: [symbol; 5] = [0xe9, b's', b's', b'i', b'm'];
static S_3_64: [symbol; 5] = [0xed, b's', b's', b'i', b'm'];
static S_3_65: [symbol; 2] = [0xef, b'm'];
static S_3_66: [symbol; 2] = [b'a', b'n'];
static S_3_67: [symbol; 4] = [b'a', b'b', b'a', b'n'];
static S_3_68: [symbol; 5] = [b'a', b'r', b'i', b'a', b'n'];
static S_3_69: [symbol; 4] = [b'a', b'r', b'a', b'n'];
static S_3_70: [symbol; 5] = [b'i', b'e', b'r', b'a', b'n'];
static S_3_71: [symbol; 4] = [b'i', b'r', b'a', b'n'];
static S_3_72: [symbol; 3] = [0xed, b'a', b'n'];
static S_3_73: [symbol; 5] = [b'a', b'r', 0xed, b'a', b'n'];
static S_3_74: [symbol; 5] = [b'e', b'r', 0xed, b'a', b'n'];
static S_3_75: [symbol; 5] = [b'i', b'r', 0xed, b'a', b'n'];
static S_3_76: [symbol; 2] = [b'e', b'n'];
static S_3_77: [symbol; 3] = [b'i', b'e', b'n'];
static S_3_78: [symbol; 5] = [b'a', b'r', b'i', b'e', b'n'];
static S_3_79: [symbol; 5] = [b'i', b'r', b'i', b'e', b'n'];
static S_3_80: [symbol; 4] = [b'a', b'r', b'e', b'n'];
static S_3_81: [symbol; 4] = [b'e', b'r', b'e', b'n'];
static S_3_82: [symbol; 4] = [b'i', b'r', b'e', b'n'];
static S_3_83: [symbol; 4] = [0xe0, b'r', b'e', b'n'];
static S_3_84: [symbol; 4] = [0xef, b'r', b'e', b'n'];
static S_3_85: [symbol; 4] = [b'a', b's', b'e', b'n'];
static S_3_86: [symbol; 5] = [b'i', b'e', b's', b'e', b'n'];
static S_3_87: [symbol; 5] = [b'a', b's', b's', b'e', b'n'];
static S_3_88: [symbol; 5] = [b'e', b's', b's', b'e', b'n'];
static S_3_89: [symbol; 5] = [b'i', b's', b's', b'e', b'n'];
static S_3_90: [symbol; 5] = [0xe9, b's', b's', b'e', b'n'];
static S_3_91: [symbol; 5] = [0xef, b's', b's', b'e', b'n'];
static S_3_92: [symbol; 6] = [b'e', b's', b'q', b'u', b'e', b'n'];
static S_3_93: [symbol; 6] = [b'i', b's', b'q', b'u', b'e', b'n'];
static S_3_94: [symbol; 6] = [0xef, b's', b'q', b'u', b'e', b'n'];
static S_3_95: [symbol; 4] = [b'a', b'v', b'e', b'n'];
static S_3_96: [symbol; 4] = [b'i', b'x', b'e', b'n'];
static S_3_97: [symbol; 5] = [b'e', b'i', b'x', b'e', b'n'];
static S_3_98: [symbol; 4] = [0xef, b'x', b'e', b'n'];
static S_3_99: [symbol; 3] = [0xef, b'e', b'n'];
static S_3_100: [symbol; 2] = [b'i', b'n'];
static S_3_101: [symbol; 4] = [b'i', b'n', b'i', b'n'];
static S_3_102: [symbol; 3] = [b's', b'i', b'n'];
static S_3_103: [symbol; 4] = [b'i', b's', b'i', b'n'];
static S_3_104: [symbol; 5] = [b'a', b's', b's', b'i', b'n'];
static S_3_105: [symbol; 5] = [b'e', b's', b's', b'i', b'n'];
static S_3_106: [symbol; 5] = [b'i', b's', b's', b'i', b'n'];
static S_3_107: [symbol; 5] = [0xef, b's', b's', b'i', b'n'];
static S_3_108: [symbol; 6] = [b'e', b's', b'q', b'u', b'i', b'n'];
static S_3_109: [symbol; 5] = [b'e', b'i', b'x', b'i', b'n'];
static S_3_110: [symbol; 4] = [b'a', b'r', b'o', b'n'];
static S_3_111: [symbol; 5] = [b'i', b'e', b'r', b'o', b'n'];
static S_3_112: [symbol; 4] = [b'a', b'r', 0xe1, b'n'];
static S_3_113: [symbol; 4] = [b'e', b'r', 0xe1, b'n'];
static S_3_114: [symbol; 4] = [b'i', b'r', 0xe1, b'n'];
static S_3_115: [symbol; 3] = [b'i', 0xef, b'n'];
static S_3_116: [symbol; 3] = [b'a', b'd', b'o'];
static S_3_117: [symbol; 3] = [b'i', b'd', b'o'];
static S_3_118: [symbol; 4] = [b'a', b'n', b'd', b'o'];
static S_3_119: [symbol; 5] = [b'i', b'e', b'n', b'd', b'o'];
static S_3_120: [symbol; 2] = [b'i', b'o'];
static S_3_121: [symbol; 3] = [b'i', b'x', b'o'];
static S_3_122: [symbol; 4] = [b'e', b'i', b'x', b'o'];
static S_3_123: [symbol; 3] = [0xef, b'x', b'o'];
static S_3_124: [symbol; 4] = [b'i', b't', b'z', b'o'];
static S_3_125: [symbol; 2] = [b'a', b'r'];
static S_3_126: [symbol; 4] = [b't', b'z', b'a', b'r'];
static S_3_127: [symbol; 2] = [b'e', b'r'];
static S_3_128: [symbol; 5] = [b'e', b'i', b'x', b'e', b'r'];
static S_3_129: [symbol; 2] = [b'i', b'r'];
static S_3_130: [symbol; 4] = [b'a', b'd', b'o', b'r'];
static S_3_131: [symbol; 2] = [b'a', b's'];
static S_3_132: [symbol; 4] = [b'a', b'b', b'a', b's'];
static S_3_133: [symbol; 4] = [b'a', b'd', b'a', b's'];
static S_3_134: [symbol; 4] = [b'i', b'd', b'a', b's'];
static S_3_135: [symbol; 4] = [b'a', b'r', b'a', b's'];
static S_3_136: [symbol; 5] = [b'i', b'e', b'r', b'a', b's'];
static S_3_137: [symbol; 3] = [0xed, b'a', b's'];
static S_3_138: [symbol; 5] = [b'a', b'r', 0xed, b'a', b's'];
static S_3_139: [symbol; 5] = [b'e', b'r', 0xed, b'a', b's'];
static S_3_140: [symbol; 5] = [b'i', b'r', 0xed, b'a', b's'];
static S_3_141: [symbol; 3] = [b'i', b'd', b's'];
static S_3_142: [symbol; 2] = [b'e', b's'];
static S_3_143: [symbol; 4] = [b'a', b'd', b'e', b's'];
static S_3_144: [symbol; 4] = [b'i', b'd', b'e', b's'];
static S_3_145: [symbol; 4] = [b'u', b'd', b'e', b's'];
static S_3_146: [symbol; 4] = [0xef, b'd', b'e', b's'];
static S_3_147: [symbol; 5] = [b'a', b't', b'g', b'e', b's'];
static S_3_148: [symbol; 3] = [b'i', b'e', b's'];
static S_3_149: [symbol; 5] = [b'a', b'r', b'i', b'e', b's'];
static S_3_150: [symbol; 5] = [b'i', b'r', b'i', b'e', b's'];
static S_3_151: [symbol; 4] = [b'a', b'r', b'e', b's'];
static S_3_152: [symbol; 4] = [b'i', b'r', b'e', b's'];
static S_3_153: [symbol; 6] = [b'a', b'd', b'o', b'r', b'e', b's'];
static S_3_154: [symbol; 4] = [0xef, b'r', b'e', b's'];
static S_3_155: [symbol; 4] = [b'a', b's', b'e', b's'];
static S_3_156: [symbol; 5] = [b'i', b'e', b's', b'e', b's'];
static S_3_157: [symbol; 5] = [b'a', b's', b's', b'e', b's'];
static S_3_158: [symbol; 5] = [b'e', b's', b's', b'e', b's'];
static S_3_159: [symbol; 5] = [b'i', b's', b's', b'e', b's'];
static S_3_160: [symbol; 5] = [0xef, b's', b's', b'e', b's'];
static S_3_161: [symbol; 4] = [b'q', b'u', b'e', b's'];
static S_3_162: [symbol; 6] = [b'e', b's', b'q', b'u', b'e', b's'];
static S_3_163: [symbol; 6] = [0xef, b's', b'q', b'u', b'e', b's'];
static S_3_164: [symbol; 4] = [b'a', b'v', b'e', b's'];
static S_3_165: [symbol; 4] = [b'i', b'x', b'e', b's'];
static S_3_166: [symbol; 5] = [b'e', b'i', b'x', b'e', b's'];
static S_3_167: [symbol; 4] = [0xef, b'x', b'e', b's'];
static S_3_168: [symbol; 3] = [0xef, b'e', b's'];
static S_3_169: [symbol; 5] = [b'a', b'b', b'a', b'i', b's'];
static S_3_170: [symbol; 5] = [b'a', b'r', b'a', b'i', b's'];
static S_3_171: [symbol; 6] = [b'i', b'e', b'r', b'a', b'i', b's'];
static S_3_172: [symbol; 4] = [0xed, b'a', b'i', b's'];
static S_3_173: [symbol; 6] = [b'a', b'r', 0xed, b'a', b'i', b's'];
static S_3_174: [symbol; 6] = [b'e', b'r', 0xed, b'a', b'i', b's'];
static S_3_175: [symbol; 6] = [b'i', b'r', 0xed, b'a', b'i', b's'];
static S_3_176: [symbol; 5] = [b'a', b's', b'e', b'i', b's'];
static S_3_177: [symbol; 6] = [b'i', b'e', b's', b'e', b'i', b's'];
static S_3_178: [symbol; 6] = [b'a', b's', b't', b'e', b'i', b's'];
static S_3_179: [symbol; 6] = [b'i', b's', b't', b'e', b'i', b's'];
static S_3_180: [symbol; 4] = [b'i', b'n', b'i', b's'];
static S_3_181: [symbol; 3] = [b's', b'i', b's'];
static S_3_182: [symbol; 4] = [b'i', b's', b'i', b's'];
static S_3_183: [symbol; 5] = [b'a', b's', b's', b'i', b's'];
static S_3_184: [symbol; 5] = [b'e', b's', b's', b'i', b's'];
static S_3_185: [symbol; 5] = [b'i', b's', b's', b'i', b's'];
static S_3_186: [symbol; 5] = [0xef, b's', b's', b'i', b's'];
static S_3_187: [symbol; 6] = [b'e', b's', b'q', b'u', b'i', b's'];
static S_3_188: [symbol; 5] = [b'e', b'i', b'x', b'i', b's'];
static S_3_189: [symbol; 5] = [b'i', b't', b'z', b'i', b's'];
static S_3_190: [symbol; 3] = [0xe1, b'i', b's'];
static S_3_191: [symbol; 5] = [b'a', b'r', 0xe9, b'i', b's'];
static S_3_192: [symbol; 5] = [b'e', b'r', 0xe9, b'i', b's'];
static S_3_193: [symbol; 5] = [b'i', b'r', 0xe9, b'i', b's'];
static S_3_194: [symbol; 3] = [b'a', b'm', b's'];
static S_3_195: [symbol; 4] = [b'a', b'd', b'o', b's'];
static S_3_196: [symbol; 4] = [b'i', b'd', b'o', b's'];
static S_3_197: [symbol; 4] = [b'a', b'm', b'o', b's'];
static S_3_198: [symbol; 6] = [0xe1, b'b', b'a', b'm', b'o', b's'];
static S_3_199: [symbol; 6] = [0xe1, b'r', b'a', b'm', b'o', b's'];
static S_3_200: [symbol; 7] = [b'i', 0xe9, b'r', b'a', b'm', b'o', b's'];
static S_3_201: [symbol; 5] = [0xed, b'a', b'm', b'o', b's'];
static S_3_202: [symbol; 7] = [b'a', b'r', 0xed, b'a', b'm', b'o', b's'];
static S_3_203: [symbol; 7] = [b'e', b'r', 0xed, b'a', b'm', b'o', b's'];
static S_3_204: [symbol; 7] = [b'i', b'r', 0xed, b'a', b'm', b'o', b's'];
static S_3_205: [symbol; 6] = [b'a', b'r', b'e', b'm', b'o', b's'];
static S_3_206: [symbol; 6] = [b'e', b'r', b'e', b'm', b'o', b's'];
static S_3_207: [symbol; 6] = [b'i', b'r', b'e', b'm', b'o', b's'];
static S_3_208: [symbol; 6] = [0xe1, b's', b'e', b'm', b'o', b's'];
static S_3_209: [symbol; 7] = [b'i', 0xe9, b's', b'e', b'm', b'o', b's'];
static S_3_210: [symbol; 4] = [b'i', b'm', b'o', b's'];
static S_3_211: [symbol; 5] = [b'a', b'd', b'o', b'r', b's'];
static S_3_212: [symbol; 3] = [b'a', b's', b's'];
static S_3_213: [symbol; 5] = [b'e', b'r', b'a', b's', b's'];
static S_3_214: [symbol; 3] = [b'e', b's', b's'];
static S_3_215: [symbol; 3] = [b'a', b't', b's'];
static S_3_216: [symbol; 3] = [b'i', b't', b's'];
static S_3_217: [symbol; 4] = [b'e', b'n', b't', b's'];
static S_3_218: [symbol; 2] = [0xe0, b's'];
static S_3_219: [symbol; 4] = [b'a', b'r', 0xe0, b's'];
static S_3_220: [symbol; 4] = [b'i', b'r', 0xe0, b's'];
static S_3_221: [symbol; 4] = [b'a', b'r', 0xe1, b's'];
static S_3_222: [symbol; 4] = [b'e', b'r', 0xe1, b's'];
static S_3_223: [symbol; 4] = [b'i', b'r', 0xe1, b's'];
static S_3_224: [symbol; 2] = [0xe9, b's'];
static S_3_225: [symbol; 4] = [b'a', b'r', 0xe9, b's'];
static S_3_226: [symbol; 2] = [0xed, b's'];
static S_3_227: [symbol; 3] = [b'i', 0xef, b's'];
static S_3_228: [symbol; 2] = [b'a', b't'];
static S_3_229: [symbol; 2] = [b'i', b't'];
static S_3_230: [symbol; 3] = [b'a', b'n', b't'];
static S_3_231: [symbol; 3] = [b'e', b'n', b't'];
static S_3_232: [symbol; 3] = [b'i', b'n', b't'];
static S_3_233: [symbol; 2] = [b'u', b't'];
static S_3_234: [symbol; 2] = [0xef, b't'];
static S_3_235: [symbol; 2] = [b'a', b'u'];
static S_3_236: [symbol; 4] = [b'e', b'r', b'a', b'u'];
static S_3_237: [symbol; 3] = [b'i', b'e', b'u'];
static S_3_238: [symbol; 4] = [b'i', b'n', b'e', b'u'];
static S_3_239: [symbol; 4] = [b'a', b'r', b'e', b'u'];
static S_3_240: [symbol; 4] = [b'i', b'r', b'e', b'u'];
static S_3_241: [symbol; 4] = [0xe0, b'r', b'e', b'u'];
static S_3_242: [symbol; 4] = [0xed, b'r', b'e', b'u'];
static S_3_243: [symbol; 5] = [b'a', b's', b's', b'e', b'u'];
static S_3_244: [symbol; 5] = [b'e', b's', b's', b'e', b'u'];
static S_3_245: [symbol; 7] = [b'e', b'r', b'e', b's', b's', b'e', b'u'];
static S_3_246: [symbol; 5] = [0xe0, b's', b's', b'e', b'u'];
static S_3_247: [symbol; 5] = [0xe9, b's', b's', b'e', b'u'];
static S_3_248: [symbol; 5] = [b'i', b'g', b'u', b'e', b'u'];
static S_3_249: [symbol; 5] = [0xef, b'g', b'u', b'e', b'u'];
static S_3_250: [symbol; 4] = [0xe0, b'v', b'e', b'u'];
static S_3_251: [symbol; 4] = [0xe1, b'v', b'e', b'u'];
static S_3_252: [symbol; 5] = [b'i', b't', b'z', b'e', b'u'];
static S_3_253: [symbol; 3] = [0xec, b'e', b'u'];
static S_3_254: [symbol; 5] = [b'i', b'r', 0xec, b'e', b'u'];
static S_3_255: [symbol; 3] = [0xed, b'e', b'u'];
static S_3_256: [symbol; 5] = [b'a', b'r', 0xed, b'e', b'u'];
static S_3_257: [symbol; 5] = [b'i', b'r', 0xed, b'e', b'u'];
static S_3_258: [symbol; 5] = [b'a', b's', b's', b'i', b'u'];
static S_3_259: [symbol; 5] = [b'i', b's', b's', b'i', b'u'];
static S_3_260: [symbol; 5] = [0xe0, b's', b's', b'i', b'u'];
static S_3_261: [symbol; 5] = [0xe8, b's', b's', b'i', b'u'];
static S_3_262: [symbol; 5] = [0xe9, b's', b's', b'i', b'u'];
static S_3_263: [symbol; 5] = [0xed, b's', b's', b'i', b'u'];
static S_3_264: [symbol; 2] = [0xef, b'u'];
static S_3_265: [symbol; 2] = [b'i', b'x'];
static S_3_266: [symbol; 3] = [b'e', b'i', b'x'];
static S_3_267: [symbol; 2] = [0xef, b'x'];
static S_3_268: [symbol; 3] = [b'i', b't', b'z'];
static S_3_269: [symbol; 2] = [b'i', 0xe0];
static S_3_270: [symbol; 3] = [b'a', b'r', 0xe0];
static S_3_271: [symbol; 3] = [b'i', b'r', 0xe0];
static S_3_272: [symbol; 4] = [b'i', b't', b'z', 0xe0];
static S_3_273: [symbol; 3] = [b'a', b'r', 0xe1];
static S_3_274: [symbol; 3] = [b'e', b'r', 0xe1];
static S_3_275: [symbol; 3] = [b'i', b'r', 0xe1];
static S_3_276: [symbol; 3] = [b'i', b'r', 0xe8];
static S_3_277: [symbol; 3] = [b'a', b'r', 0xe9];
static S_3_278: [symbol; 3] = [b'e', b'r', 0xe9];
static S_3_279: [symbol; 3] = [b'i', b'r', 0xe9];
static S_3_280: [symbol; 1] = [0xed];
static S_3_281: [symbol; 2] = [b'i', 0xef];
static S_3_282: [symbol; 2] = [b'i', 0xf3];
static S_4_0: [symbol; 1] = [b'a'];
static S_4_1: [symbol; 1] = [b'e'];
static S_4_2: [symbol; 1] = [b'i'];
static S_4_3: [symbol; 2] = [0xef, b'n'];
static S_4_4: [symbol; 1] = [b'o'];
static S_4_5: [symbol; 2] = [b'i', b'r'];
static S_4_6: [symbol; 1] = [b's'];
static S_4_7: [symbol; 2] = [b'i', b's'];
static S_4_8: [symbol; 2] = [b'o', b's'];
static S_4_9: [symbol; 2] = [0xef, b's'];
static S_4_10: [symbol; 2] = [b'i', b't'];
static S_4_11: [symbol; 2] = [b'e', b'u'];
static S_4_12: [symbol; 2] = [b'i', b'u'];
static S_4_13: [symbol; 3] = [b'i', b'q', b'u'];
static S_4_14: [symbol; 3] = [b'i', b't', b'z'];
static S_4_15: [symbol; 1] = [0xe0];
static S_4_16: [symbol; 1] = [0xe1];
static S_4_17: [symbol; 1] = [0xe9];
static S_4_18: [symbol; 1] = [0xec];
static S_4_19: [symbol; 1] = [0xed];
static S_4_20: [symbol; 1] = [0xef];
static S_4_21: [symbol; 1] = [0xf3];
static S_0: [symbol; 1] = [b'a'];
static S_1: [symbol; 1] = [b'e'];
static S_2: [symbol; 1] = [b'i'];
static S_3: [symbol; 1] = [b'o'];
static S_4: [symbol; 1] = [b'u'];
static S_5: [symbol; 1] = [b'.'];
static S_6: [symbol; 3] = [b'l', b'o', b'g'];
static S_7: [symbol; 2] = [b'i', b'c'];
static S_8: [symbol; 1] = [b'c'];
static S_9: [symbol; 2] = [b'i', b'c'];

// ---------------------------------------------------------------------------
// among tables
// ---------------------------------------------------------------------------

static A_0: [among; 13] = [
    among { s_size: 0, s: core::ptr::null(), substring_i: -1, result: 7, function: None },
    among { s_size: 1, s: S_0_1.as_ptr(), substring_i: 0, result: 6, function: None },
    among { s_size: 1, s: S_0_2.as_ptr(), substring_i: 0, result: 1, function: None },
    among { s_size: 1, s: S_0_3.as_ptr(), substring_i: 0, result: 1, function: None },
    among { s_size: 1, s: S_0_4.as_ptr(), substring_i: 0, result: 2, function: None },
    among { s_size: 1, s: S_0_5.as_ptr(), substring_i: 0, result: 2, function: None },
    among { s_size: 1, s: S_0_6.as_ptr(), substring_i: 0, result: 3, function: None },
    among { s_size: 1, s: S_0_7.as_ptr(), substring_i: 0, result: 3, function: None },
    among { s_size: 1, s: S_0_8.as_ptr(), substring_i: 0, result: 3, function: None },
    among { s_size: 1, s: S_0_9.as_ptr(), substring_i: 0, result: 4, function: None },
    among { s_size: 1, s: S_0_10.as_ptr(), substring_i: 0, result: 4, function: None },
    among { s_size: 1, s: S_0_11.as_ptr(), substring_i: 0, result: 5, function: None },
    among { s_size: 1, s: S_0_12.as_ptr(), substring_i: 0, result: 5, function: None },
];
static A_1: [among; 39] = [
    among { s_size: 2, s: S_1_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_1_1.as_ptr(), substring_i: 0, result: 1, function: None },
    among { s_size: 4, s: S_1_2.as_ptr(), substring_i: 0, result: 1, function: None },
    among { s_size: 2, s: S_1_3.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_1_4.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_1_5.as_ptr(), substring_i: 4, result: 1, function: None },
    among { s_size: 2, s: S_1_6.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_1_7.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_1_8.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_1_9.as_ptr(), substring_i: 8, result: 1, function: None },
    among { s_size: 2, s: S_1_10.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_1_11.as_ptr(), substring_i: 10, result: 1, function: None },
    among { s_size: 2, s: S_1_12.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_1_13.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_1_14.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_1_15.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_1_16.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_1_17.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_1_18.as_ptr(), substring_i: 17, result: 1, function: None },
    among { s_size: 2, s: S_1_19.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_1_20.as_ptr(), substring_i: 19, result: 1, function: None },
    among { s_size: 2, s: S_1_21.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_1_22.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_1_23.as_ptr(), substring_i: 22, result: 1, function: None },
    among { s_size: 3, s: S_1_24.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_1_25.as_ptr(), substring_i: 24, result: 1, function: None },
    among { s_size: 3, s: S_1_26.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_1_27.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_1_28.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_1_29.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_1_30.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_1_31.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_1_32.as_ptr(), substring_i: 31, result: 1, function: None },
    among { s_size: 3, s: S_1_33.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_1_34.as_ptr(), substring_i: 33, result: 1, function: None },
    among { s_size: 3, s: S_1_35.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_1_36.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_1_37.as_ptr(), substring_i: 36, result: 1, function: None },
    among { s_size: 2, s: S_1_38.as_ptr(), substring_i: -1, result: 1, function: None },
];
static A_2: [among; 200] = [
    among { s_size: 3, s: S_2_0.as_ptr(), substring_i: -1, result: 4, function: None },
    among { s_size: 6, s: S_2_1.as_ptr(), substring_i: 0, result: 3, function: None },
    among { s_size: 4, s: S_2_2.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_2_3.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 5, s: S_2_4.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_2_5.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_2_6.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_2_7.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_2_8.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 4, s: S_2_9.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_2_10.as_ptr(), substring_i: 9, result: 1, function: None },
    among { s_size: 4, s: S_2_11.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_2_12.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_2_13.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_2_14.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_2_15.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_2_16.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_2_17.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_2_18.as_ptr(), substring_i: 17, result: 1, function: None },
    among { s_size: 8, s: S_2_19.as_ptr(), substring_i: 18, result: 5, function: None },
    among { s_size: 3, s: S_2_20.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_2_21.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_2_22.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_2_23.as_ptr(), substring_i: 22, result: 1, function: None },
    among { s_size: 3, s: S_2_24.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_2_25.as_ptr(), substring_i: 24, result: 1, function: None },
    among { s_size: 5, s: S_2_26.as_ptr(), substring_i: 25, result: 1, function: None },
    among { s_size: 5, s: S_2_27.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_2_28.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_2_29.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_2_30.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_2_31.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_2_32.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_2_33.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_2_34.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_2_35.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_2_36.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 7, s: S_2_37.as_ptr(), substring_i: 36, result: 1, function: None },
    among { s_size: 7, s: S_2_38.as_ptr(), substring_i: 36, result: 1, function: None },
    among { s_size: 3, s: S_2_39.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_2_40.as_ptr(), substring_i: 39, result: 1, function: None },
    among { s_size: 3, s: S_2_41.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_2_42.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 2, s: S_2_43.as_ptr(), substring_i: -1, result: 4, function: None },
    among { s_size: 5, s: S_2_44.as_ptr(), substring_i: 43, result: 1, function: None },
    among { s_size: 3, s: S_2_45.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_2_46.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_2_47.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_2_48.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_2_49.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_2_50.as_ptr(), substring_i: 49, result: 1, function: None },
    among { s_size: 4, s: S_2_51.as_ptr(), substring_i: 49, result: 1, function: None },
    among { s_size: 4, s: S_2_52.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 7, s: S_2_53.as_ptr(), substring_i: 52, result: 1, function: None },
    among { s_size: 7, s: S_2_54.as_ptr(), substring_i: 52, result: 1, function: None },
    among { s_size: 6, s: S_2_55.as_ptr(), substring_i: 52, result: 1, function: None },
    among { s_size: 4, s: S_2_56.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_2_57.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_2_58.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_2_59.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_2_60.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_2_61.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 3, s: S_2_62.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_2_63.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_2_64.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_2_65.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_2_66.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_2_67.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_2_68.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_2_69.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_2_70.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_2_71.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_2_72.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_2_73.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 7, s: S_2_74.as_ptr(), substring_i: 73, result: 5, function: None },
    among { s_size: 4, s: S_2_75.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_2_76.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_2_77.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_2_78.as_ptr(), substring_i: 77, result: 1, function: None },
    among { s_size: 4, s: S_2_79.as_ptr(), substring_i: 77, result: 1, function: None },
    among { s_size: 4, s: S_2_80.as_ptr(), substring_i: 77, result: 1, function: None },
    among { s_size: 4, s: S_2_81.as_ptr(), substring_i: 77, result: 1, function: None },
    among { s_size: 5, s: S_2_82.as_ptr(), substring_i: 77, result: 1, function: None },
    among { s_size: 3, s: S_2_83.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_2_84.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_2_85.as_ptr(), substring_i: 84, result: 1, function: None },
    among { s_size: 3, s: S_2_86.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_2_87.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_2_88.as_ptr(), substring_i: -1, result: 4, function: None },
    among { s_size: 6, s: S_2_89.as_ptr(), substring_i: 88, result: 3, function: None },
    among { s_size: 3, s: S_2_90.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_2_91.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_2_92.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 6, s: S_2_93.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_2_94.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_2_95.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_2_96.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_2_97.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 5, s: S_2_98.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_2_99.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_2_100.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_2_101.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 7, s: S_2_102.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_2_103.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_2_104.as_ptr(), substring_i: 103, result: 1, function: None },
    among { s_size: 5, s: S_2_105.as_ptr(), substring_i: 103, result: 1, function: None },
    among { s_size: 4, s: S_2_106.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 7, s: S_2_107.as_ptr(), substring_i: 106, result: 1, function: None },
    among { s_size: 9, s: S_2_108.as_ptr(), substring_i: 107, result: 5, function: None },
    among { s_size: 6, s: S_2_109.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_2_110.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_2_111.as_ptr(), substring_i: 110, result: 1, function: None },
    among { s_size: 4, s: S_2_112.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_2_113.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_2_114.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_2_115.as_ptr(), substring_i: 114, result: 1, function: None },
    among { s_size: 6, s: S_2_116.as_ptr(), substring_i: 115, result: 1, function: None },
    among { s_size: 5, s: S_2_117.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_2_118.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_2_119.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_2_120.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_2_121.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_2_122.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_2_123.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_2_124.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 8, s: S_2_125.as_ptr(), substring_i: 124, result: 1, function: None },
    among { s_size: 8, s: S_2_126.as_ptr(), substring_i: 124, result: 1, function: None },
    among { s_size: 5, s: S_2_127.as_ptr(), substring_i: -1, result: 4, function: None },
    among { s_size: 8, s: S_2_128.as_ptr(), substring_i: 127, result: 3, function: None },
    among { s_size: 4, s: S_2_129.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_2_130.as_ptr(), substring_i: 129, result: 1, function: None },
    among { s_size: 6, s: S_2_131.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 9, s: S_2_132.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_2_133.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_2_134.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_2_135.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 4, s: S_2_136.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_2_137.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_2_138.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_2_139.as_ptr(), substring_i: 138, result: 1, function: None },
    among { s_size: 4, s: S_2_140.as_ptr(), substring_i: 138, result: 1, function: None },
    among { s_size: 3, s: S_2_141.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_2_142.as_ptr(), substring_i: 141, result: 1, function: None },
    among { s_size: 8, s: S_2_143.as_ptr(), substring_i: 142, result: 5, function: None },
    among { s_size: 4, s: S_2_144.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_2_145.as_ptr(), substring_i: 144, result: 1, function: None },
    among { s_size: 6, s: S_2_146.as_ptr(), substring_i: 145, result: 2, function: None },
    among { s_size: 4, s: S_2_147.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_2_148.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_2_149.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_2_150.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_2_151.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_2_152.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_2_153.as_ptr(), substring_i: 152, result: 1, function: None },
    among { s_size: 5, s: S_2_154.as_ptr(), substring_i: 153, result: 1, function: None },
    among { s_size: 5, s: S_2_155.as_ptr(), substring_i: 153, result: 1, function: None },
    among { s_size: 3, s: S_2_156.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_2_157.as_ptr(), substring_i: 156, result: 1, function: None },
    among { s_size: 8, s: S_2_158.as_ptr(), substring_i: 157, result: 1, function: None },
    among { s_size: 7, s: S_2_159.as_ptr(), substring_i: 157, result: 1, function: None },
    among { s_size: 9, s: S_2_160.as_ptr(), substring_i: 159, result: 1, function: None },
    among { s_size: 5, s: S_2_161.as_ptr(), substring_i: 156, result: 1, function: None },
    among { s_size: 3, s: S_2_162.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_2_163.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_2_164.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_2_165.as_ptr(), substring_i: 164, result: 1, function: None },
    among { s_size: 6, s: S_2_166.as_ptr(), substring_i: 165, result: 1, function: None },
    among { s_size: 3, s: S_2_167.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_2_168.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_2_169.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_2_170.as_ptr(), substring_i: 169, result: 1, function: None },
    among { s_size: 5, s: S_2_171.as_ptr(), substring_i: 169, result: 1, function: None },
    among { s_size: 2, s: S_2_172.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_2_173.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_2_174.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_2_175.as_ptr(), substring_i: 174, result: 1, function: None },
    among { s_size: 2, s: S_2_176.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_2_177.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 7, s: S_2_178.as_ptr(), substring_i: 177, result: 1, function: None },
    among { s_size: 6, s: S_2_179.as_ptr(), substring_i: 177, result: 1, function: None },
    among { s_size: 8, s: S_2_180.as_ptr(), substring_i: 179, result: 1, function: None },
    among { s_size: 4, s: S_2_181.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_2_182.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_2_183.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_2_184.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_2_185.as_ptr(), substring_i: 184, result: 1, function: None },
    among { s_size: 4, s: S_2_186.as_ptr(), substring_i: 184, result: 1, function: None },
    among { s_size: 5, s: S_2_187.as_ptr(), substring_i: 186, result: 1, function: None },
    among { s_size: 7, s: S_2_188.as_ptr(), substring_i: 187, result: 1, function: None },
    among { s_size: 2, s: S_2_189.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_2_190.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_2_191.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_2_192.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_2_193.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_2_194.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_2_195.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 1, s: S_2_196.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_2_197.as_ptr(), substring_i: 196, result: 1, function: None },
    among { s_size: 3, s: S_2_198.as_ptr(), substring_i: 197, result: 1, function: None },
    among { s_size: 4, s: S_2_199.as_ptr(), substring_i: 198, result: 1, function: None },
];
static A_3: [among; 283] = [
    among { s_size: 3, s: S_3_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_3_1.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_3_2.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_3_3.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_3_4.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_3_5.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_3_6.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_3_7.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_3_8.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_3_9.as_ptr(), substring_i: 8, result: 1, function: None },
    among { s_size: 4, s: S_3_10.as_ptr(), substring_i: 8, result: 1, function: None },
    among { s_size: 3, s: S_3_11.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_3_12.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_3_13.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_3_14.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_3_15.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_3_16.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_3_17.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_3_18.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_3_19.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_3_20.as_ptr(), substring_i: 19, result: 1, function: None },
    among { s_size: 4, s: S_3_21.as_ptr(), substring_i: 19, result: 1, function: None },
    among { s_size: 4, s: S_3_22.as_ptr(), substring_i: 19, result: 1, function: None },
    among { s_size: 2, s: S_3_23.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_3_24.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_3_25.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_3_26.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_3_27.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_3_28.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_3_29.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_3_30.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_3_31.as_ptr(), substring_i: 30, result: 1, function: None },
    among { s_size: 3, s: S_3_32.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_3_33.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_3_34.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_3_35.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_3_36.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_3_37.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_3_38.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_3_39.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_3_40.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_3_41.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_3_42.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_3_43.as_ptr(), substring_i: 42, result: 1, function: None },
    among { s_size: 4, s: S_3_44.as_ptr(), substring_i: 42, result: 1, function: None },
    among { s_size: 4, s: S_3_45.as_ptr(), substring_i: 42, result: 1, function: None },
    among { s_size: 4, s: S_3_46.as_ptr(), substring_i: 42, result: 1, function: None },
    among { s_size: 5, s: S_3_47.as_ptr(), substring_i: 42, result: 1, function: None },
    among { s_size: 5, s: S_3_48.as_ptr(), substring_i: 42, result: 1, function: None },
    among { s_size: 5, s: S_3_49.as_ptr(), substring_i: 42, result: 1, function: None },
    among { s_size: 5, s: S_3_50.as_ptr(), substring_i: 42, result: 1, function: None },
    among { s_size: 4, s: S_3_51.as_ptr(), substring_i: 42, result: 1, function: None },
    among { s_size: 4, s: S_3_52.as_ptr(), substring_i: 42, result: 1, function: None },
    among { s_size: 4, s: S_3_53.as_ptr(), substring_i: 42, result: 1, function: None },
    among { s_size: 5, s: S_3_54.as_ptr(), substring_i: 42, result: 1, function: None },
    among { s_size: 3, s: S_3_55.as_ptr(), substring_i: 42, result: 1, function: None },
    among { s_size: 5, s: S_3_56.as_ptr(), substring_i: 55, result: 1, function: None },
    among { s_size: 5, s: S_3_57.as_ptr(), substring_i: 55, result: 1, function: None },
    among { s_size: 5, s: S_3_58.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_3_59.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_3_60.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_3_61.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_3_62.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_3_63.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_3_64.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_3_65.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_3_66.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_3_67.as_ptr(), substring_i: 66, result: 1, function: None },
    among { s_size: 5, s: S_3_68.as_ptr(), substring_i: 66, result: 1, function: None },
    among { s_size: 4, s: S_3_69.as_ptr(), substring_i: 66, result: 1, function: None },
    among { s_size: 5, s: S_3_70.as_ptr(), substring_i: 66, result: 1, function: None },
    among { s_size: 4, s: S_3_71.as_ptr(), substring_i: 66, result: 1, function: None },
    among { s_size: 3, s: S_3_72.as_ptr(), substring_i: 66, result: 1, function: None },
    among { s_size: 5, s: S_3_73.as_ptr(), substring_i: 72, result: 1, function: None },
    among { s_size: 5, s: S_3_74.as_ptr(), substring_i: 72, result: 1, function: None },
    among { s_size: 5, s: S_3_75.as_ptr(), substring_i: 72, result: 1, function: None },
    among { s_size: 2, s: S_3_76.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_3_77.as_ptr(), substring_i: 76, result: 1, function: None },
    among { s_size: 5, s: S_3_78.as_ptr(), substring_i: 77, result: 1, function: None },
    among { s_size: 5, s: S_3_79.as_ptr(), substring_i: 77, result: 1, function: None },
    among { s_size: 4, s: S_3_80.as_ptr(), substring_i: 76, result: 1, function: None },
    among { s_size: 4, s: S_3_81.as_ptr(), substring_i: 76, result: 1, function: None },
    among { s_size: 4, s: S_3_82.as_ptr(), substring_i: 76, result: 1, function: None },
    among { s_size: 4, s: S_3_83.as_ptr(), substring_i: 76, result: 1, function: None },
    among { s_size: 4, s: S_3_84.as_ptr(), substring_i: 76, result: 1, function: None },
    among { s_size: 4, s: S_3_85.as_ptr(), substring_i: 76, result: 1, function: None },
    among { s_size: 5, s: S_3_86.as_ptr(), substring_i: 76, result: 1, function: None },
    among { s_size: 5, s: S_3_87.as_ptr(), substring_i: 76, result: 1, function: None },
    among { s_size: 5, s: S_3_88.as_ptr(), substring_i: 76, result: 1, function: None },
    among { s_size: 5, s: S_3_89.as_ptr(), substring_i: 76, result: 1, function: None },
    among { s_size: 5, s: S_3_90.as_ptr(), substring_i: 76, result: 1, function: None },
    among { s_size: 5, s: S_3_91.as_ptr(), substring_i: 76, result: 1, function: None },
    among { s_size: 6, s: S_3_92.as_ptr(), substring_i: 76, result: 1, function: None },
    among { s_size: 6, s: S_3_93.as_ptr(), substring_i: 76, result: 1, function: None },
    among { s_size: 6, s: S_3_94.as_ptr(), substring_i: 76, result: 1, function: None },
    among { s_size: 4, s: S_3_95.as_ptr(), substring_i: 76, result: 1, function: None },
    among { s_size: 4, s: S_3_96.as_ptr(), substring_i: 76, result: 1, function: None },
    among { s_size: 5, s: S_3_97.as_ptr(), substring_i: 96, result: 1, function: None },
    among { s_size: 4, s: S_3_98.as_ptr(), substring_i: 76, result: 1, function: None },
    among { s_size: 3, s: S_3_99.as_ptr(), substring_i: 76, result: 1, function: None },
    among { s_size: 2, s: S_3_100.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_3_101.as_ptr(), substring_i: 100, result: 1, function: None },
    among { s_size: 3, s: S_3_102.as_ptr(), substring_i: 100, result: 1, function: None },
    among { s_size: 4, s: S_3_103.as_ptr(), substring_i: 102, result: 1, function: None },
    among { s_size: 5, s: S_3_104.as_ptr(), substring_i: 102, result: 1, function: None },
    among { s_size: 5, s: S_3_105.as_ptr(), substring_i: 102, result: 1, function: None },
    among { s_size: 5, s: S_3_106.as_ptr(), substring_i: 102, result: 1, function: None },
    among { s_size: 5, s: S_3_107.as_ptr(), substring_i: 102, result: 1, function: None },
    among { s_size: 6, s: S_3_108.as_ptr(), substring_i: 100, result: 1, function: None },
    among { s_size: 5, s: S_3_109.as_ptr(), substring_i: 100, result: 1, function: None },
    among { s_size: 4, s: S_3_110.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_3_111.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_3_112.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_3_113.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_3_114.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_3_115.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_3_116.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_3_117.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_3_118.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 5, s: S_3_119.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_3_120.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_3_121.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_3_122.as_ptr(), substring_i: 121, result: 1, function: None },
    among { s_size: 3, s: S_3_123.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_3_124.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_3_125.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_3_126.as_ptr(), substring_i: 125, result: 1, function: None },
    among { s_size: 2, s: S_3_127.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_3_128.as_ptr(), substring_i: 127, result: 1, function: None },
    among { s_size: 2, s: S_3_129.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_3_130.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_3_131.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_3_132.as_ptr(), substring_i: 131, result: 1, function: None },
    among { s_size: 4, s: S_3_133.as_ptr(), substring_i: 131, result: 1, function: None },
    among { s_size: 4, s: S_3_134.as_ptr(), substring_i: 131, result: 1, function: None },
    among { s_size: 4, s: S_3_135.as_ptr(), substring_i: 131, result: 1, function: None },
    among { s_size: 5, s: S_3_136.as_ptr(), substring_i: 131, result: 1, function: None },
    among { s_size: 3, s: S_3_137.as_ptr(), substring_i: 131, result: 1, function: None },
    among { s_size: 5, s: S_3_138.as_ptr(), substring_i: 137, result: 1, function: None },
    among { s_size: 5, s: S_3_139.as_ptr(), substring_i: 137, result: 1, function: None },
    among { s_size: 5, s: S_3_140.as_ptr(), substring_i: 137, result: 1, function: None },
    among { s_size: 3, s: S_3_141.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_3_142.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_3_143.as_ptr(), substring_i: 142, result: 1, function: None },
    among { s_size: 4, s: S_3_144.as_ptr(), substring_i: 142, result: 1, function: None },
    among { s_size: 4, s: S_3_145.as_ptr(), substring_i: 142, result: 1, function: None },
    among { s_size: 4, s: S_3_146.as_ptr(), substring_i: 142, result: 1, function: None },
    among { s_size: 5, s: S_3_147.as_ptr(), substring_i: 142, result: 1, function: None },
    among { s_size: 3, s: S_3_148.as_ptr(), substring_i: 142, result: 1, function: None },
    among { s_size: 5, s: S_3_149.as_ptr(), substring_i: 148, result: 1, function: None },
    among { s_size: 5, s: S_3_150.as_ptr(), substring_i: 148, result: 1, function: None },
    among { s_size: 4, s: S_3_151.as_ptr(), substring_i: 142, result: 1, function: None },
    among { s_size: 4, s: S_3_152.as_ptr(), substring_i: 142, result: 1, function: None },
    among { s_size: 6, s: S_3_153.as_ptr(), substring_i: 142, result: 1, function: None },
    among { s_size: 4, s: S_3_154.as_ptr(), substring_i: 142, result: 1, function: None },
    among { s_size: 4, s: S_3_155.as_ptr(), substring_i: 142, result: 1, function: None },
    among { s_size: 5, s: S_3_156.as_ptr(), substring_i: 142, result: 1, function: None },
    among { s_size: 5, s: S_3_157.as_ptr(), substring_i: 142, result: 1, function: None },
    among { s_size: 5, s: S_3_158.as_ptr(), substring_i: 142, result: 1, function: None },
    among { s_size: 5, s: S_3_159.as_ptr(), substring_i: 142, result: 1, function: None },
    among { s_size: 5, s: S_3_160.as_ptr(), substring_i: 142, result: 1, function: None },
    among { s_size: 4, s: S_3_161.as_ptr(), substring_i: 142, result: 1, function: None },
    among { s_size: 6, s: S_3_162.as_ptr(), substring_i: 161, result: 1, function: None },
    among { s_size: 6, s: S_3_163.as_ptr(), substring_i: 161, result: 1, function: None },
    among { s_size: 4, s: S_3_164.as_ptr(), substring_i: 142, result: 1, function: None },
    among { s_size: 4, s: S_3_165.as_ptr(), substring_i: 142, result: 1, function: None },
    among { s_size: 5, s: S_3_166.as_ptr(), substring_i: 165, result: 1, function: None },
    among { s_size: 4, s: S_3_167.as_ptr(), substring_i: 142, result: 1, function: None },
    among { s_size: 3, s: S_3_168.as_ptr(), substring_i: 142, result: 1, function: None },
    among { s_size: 5, s: S_3_169.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_3_170.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_3_171.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_3_172.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_3_173.as_ptr(), substring_i: 172, result: 1, function: None },
    among { s_size: 6, s: S_3_174.as_ptr(), substring_i: 172, result: 1, function: None },
    among { s_size: 6, s: S_3_175.as_ptr(), substring_i: 172, result: 1, function: None },
    among { s_size: 5, s: S_3_176.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_3_177.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_3_178.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_3_179.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_3_180.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_3_181.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_3_182.as_ptr(), substring_i: 181, result: 1, function: None },
    among { s_size: 5, s: S_3_183.as_ptr(), substring_i: 181, result: 1, function: None },
    among { s_size: 5, s: S_3_184.as_ptr(), substring_i: 181, result: 1, function: None },
    among { s_size: 5, s: S_3_185.as_ptr(), substring_i: 181, result: 1, function: None },
    among { s_size: 5, s: S_3_186.as_ptr(), substring_i: 181, result: 1, function: None },
    among { s_size: 6, s: S_3_187.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_3_188.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_3_189.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_3_190.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_3_191.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_3_192.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_3_193.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_3_194.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_3_195.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_3_196.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_3_197.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_3_198.as_ptr(), substring_i: 197, result: 1, function: None },
    among { s_size: 6, s: S_3_199.as_ptr(), substring_i: 197, result: 1, function: None },
    among { s_size: 7, s: S_3_200.as_ptr(), substring_i: 197, result: 1, function: None },
    among { s_size: 5, s: S_3_201.as_ptr(), substring_i: 197, result: 1, function: None },
    among { s_size: 7, s: S_3_202.as_ptr(), substring_i: 201, result: 1, function: None },
    among { s_size: 7, s: S_3_203.as_ptr(), substring_i: 201, result: 1, function: None },
    among { s_size: 7, s: S_3_204.as_ptr(), substring_i: 201, result: 1, function: None },
    among { s_size: 6, s: S_3_205.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_3_206.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_3_207.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_3_208.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 7, s: S_3_209.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_3_210.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_3_211.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_3_212.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_3_213.as_ptr(), substring_i: 212, result: 1, function: None },
    among { s_size: 3, s: S_3_214.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_3_215.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_3_216.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_3_217.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_3_218.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_3_219.as_ptr(), substring_i: 218, result: 1, function: None },
    among { s_size: 4, s: S_3_220.as_ptr(), substring_i: 218, result: 1, function: None },
    among { s_size: 4, s: S_3_221.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_3_222.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_3_223.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_3_224.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_3_225.as_ptr(), substring_i: 224, result: 1, function: None },
    among { s_size: 2, s: S_3_226.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_3_227.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_3_228.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_3_229.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_3_230.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_3_231.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_3_232.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_3_233.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_3_234.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_3_235.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_3_236.as_ptr(), substring_i: 235, result: 1, function: None },
    among { s_size: 3, s: S_3_237.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_3_238.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_3_239.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_3_240.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_3_241.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_3_242.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_3_243.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_3_244.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 7, s: S_3_245.as_ptr(), substring_i: 244, result: 1, function: None },
    among { s_size: 5, s: S_3_246.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_3_247.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_3_248.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_3_249.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_3_250.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_3_251.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_3_252.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_3_253.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_3_254.as_ptr(), substring_i: 253, result: 1, function: None },
    among { s_size: 3, s: S_3_255.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_3_256.as_ptr(), substring_i: 255, result: 1, function: None },
    among { s_size: 5, s: S_3_257.as_ptr(), substring_i: 255, result: 1, function: None },
    among { s_size: 5, s: S_3_258.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_3_259.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_3_260.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_3_261.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_3_262.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_3_263.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_3_264.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_3_265.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_3_266.as_ptr(), substring_i: 265, result: 1, function: None },
    among { s_size: 2, s: S_3_267.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_3_268.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_3_269.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_3_270.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_3_271.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_3_272.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_3_273.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_3_274.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_3_275.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_3_276.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_3_277.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_3_278.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_3_279.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 1, s: S_3_280.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_3_281.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_3_282.as_ptr(), substring_i: -1, result: 1, function: None },
];
static A_4: [among; 22] = [
    among { s_size: 1, s: S_4_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 1, s: S_4_1.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 1, s: S_4_2.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_4_3.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 1, s: S_4_4.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_4_5.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 1, s: S_4_6.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_4_7.as_ptr(), substring_i: 6, result: 1, function: None },
    among { s_size: 2, s: S_4_8.as_ptr(), substring_i: 6, result: 1, function: None },
    among { s_size: 2, s: S_4_9.as_ptr(), substring_i: 6, result: 1, function: None },
    among { s_size: 2, s: S_4_10.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_4_11.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_4_12.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_4_13.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 3, s: S_4_14.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 1, s: S_4_15.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 1, s: S_4_16.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 1, s: S_4_17.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 1, s: S_4_18.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 1, s: S_4_19.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 1, s: S_4_20.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 1, s: S_4_21.as_ptr(), substring_i: -1, result: 1, function: None },
];

static G_V: [c_uchar; 20] = [
    17, 65, 16, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 128, 129, 81, 6, 10,
];

// ---------------------------------------------------------------------------
// stemmer functions
// ---------------------------------------------------------------------------

unsafe extern "C" fn r_mark_regions(z: *mut SN_env) -> c_int {
    *(*z).I.offset(1) = (*z).l;
    *(*z).I.offset(0) = (*z).l;
    {
        let c1 = (*z).c;
        'lab0: {
            {
                let ret = out_grouping(z, G_V.as_ptr(), 97, 252, 1);
                if ret < 0 {
                    break 'lab0;
                }
                (*z).c += ret;
            }
            {
                let ret = in_grouping(z, G_V.as_ptr(), 97, 252, 1);
                if ret < 0 {
                    break 'lab0;
                }
                (*z).c += ret;
            }
            *(*z).I.offset(1) = (*z).c;
            {
                let ret = out_grouping(z, G_V.as_ptr(), 97, 252, 1);
                if ret < 0 {
                    break 'lab0;
                }
                (*z).c += ret;
            }
            {
                let ret = in_grouping(z, G_V.as_ptr(), 97, 252, 1);
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
    1
}

unsafe extern "C" fn r_cleaning(z: *mut SN_env) -> c_int {
    let mut among_var;
    'loop0: loop {
        let c1 = (*z).c;
        'lab0: {
            (*z).bra = (*z).c;
            among_var = find_among(z, A_0.as_ptr(), 13);
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
                    let ret = slice_from_s(z, 1, S_5.as_ptr());
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
            let _ = &mut among_var;
            continue 'loop0;
        }
        // lab0:
        (*z).c = c1;
        break 'loop0;
    }
    1
}

unsafe extern "C" fn r_R1(z: *mut SN_env) -> c_int {
    (*(*z).I.offset(1) <= (*z).c) as c_int
}

unsafe extern "C" fn r_R2(z: *mut SN_env) -> c_int {
    (*(*z).I.offset(0) <= (*z).c) as c_int
}

unsafe extern "C" fn r_attached_pronoun(z: *mut SN_env) -> c_int {
    (*z).ket = (*z).c;
    if (*z).c - 1 <= (*z).lb
        || (*(*z).p.offset(((*z).c - 1) as isize) as c_int >> 5) != 3
        || ((1634850 >> (*(*z).p.offset(((*z).c - 1) as isize) as c_int & 0x1f)) & 1) == 0
    {
        return 0;
    }
    if find_among_b(z, A_1.as_ptr(), 39) == 0 {
        return 0;
    }
    (*z).bra = (*z).c;
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
    1
}

unsafe extern "C" fn r_standard_suffix(z: *mut SN_env) -> c_int {
    let among_var;
    (*z).ket = (*z).c;
    among_var = find_among_b(z, A_2.as_ptr(), 200);
    if among_var == 0 {
        return 0;
    }
    (*z).bra = (*z).c;
    match among_var {
        1 => {
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
        }
        3 => {
            {
                let ret = r_R2(z);
                if ret <= 0 {
                    return ret;
                }
            }
            {
                let ret = slice_from_s(z, 3, S_6.as_ptr());
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
                let ret = slice_from_s(z, 2, S_7.as_ptr());
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
                let ret = slice_from_s(z, 1, S_8.as_ptr());
                if ret < 0 {
                    return ret;
                }
            }
        }
        _ => {}
    }
    1
}

unsafe extern "C" fn r_verb_suffix(z: *mut SN_env) -> c_int {
    let among_var;
    (*z).ket = (*z).c;
    among_var = find_among_b(z, A_3.as_ptr(), 283);
    if among_var == 0 {
        return 0;
    }
    (*z).bra = (*z).c;
    match among_var {
        1 => {
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
        }
        _ => {}
    }
    1
}

unsafe extern "C" fn r_residual_suffix(z: *mut SN_env) -> c_int {
    let among_var;
    (*z).ket = (*z).c;
    among_var = find_among_b(z, A_4.as_ptr(), 22);
    if among_var == 0 {
        return 0;
    }
    (*z).bra = (*z).c;
    match among_var {
        1 => {
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
        }
        2 => {
            {
                let ret = r_R1(z);
                if ret <= 0 {
                    return ret;
                }
            }
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

#[no_mangle]
pub unsafe extern "C" fn catalan_ISO_8859_1_stem(z: *mut SN_env) -> c_int {
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
        let _ = m1;
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
        let _ = m2;
        'lab0: {
            'lab1: {
                let m3 = (*z).l - (*z).c;
                let _ = m3;
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
        let _ = m4;
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
            let ret = r_cleaning(z);
            if ret < 0 {
                return ret;
            }
        }
        (*z).c = c5;
    }
    1
}

#[no_mangle]
pub unsafe extern "C" fn catalan_ISO_8859_1_create_env() -> *mut SN_env {
    SN_create_env(0, 2)
}

#[no_mangle]
pub unsafe extern "C" fn catalan_ISO_8859_1_close_env(z: *mut SN_env) {
    SN_close_env(z, 0);
}

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snowball::api::SN_set_current;

    // Run the full stemming pipeline over `word` and return the resulting
    // stemmed bytes.
    unsafe fn stem(word: &[u8]) -> Vec<u8> {
        let z = catalan_ISO_8859_1_create_env();
        assert!(!z.is_null());
        let rc = SN_set_current(z, word.len() as c_int, word.as_ptr());
        assert!(rc >= 0);
        let rc = catalan_ISO_8859_1_stem(z);
        assert!(rc >= 0, "stem returned {rc}");
        let out = std::slice::from_raw_parts((*z).p, SIZE((*z).p) as usize).to_vec();
        catalan_ISO_8859_1_close_env(z);
        out
    }

    // A short root with no strippable suffix is returned unchanged.
    #[test]
    fn short_root_unchanged() {
        unsafe {
            assert_eq!(stem(b"sol"), b"sol".to_vec());
        }
    }

    // Convergence: these suffix strippers remove at most one suffix layer per
    // pass (faithful to the C), so a word need not be idempotent in one step
    // (e.g. "generalitat" -> "general" -> "gener"); but repeated stemming
    // reaches a stable fixpoint. Assert that, and that stems stay non-empty.
    #[test]
    fn idempotent() {
        unsafe {
            for w in [
                &b"generalitat"[..],
                &b"nacional"[..],
                &b"informacions"[..],
                &b"cantaria"[..],
            ] {
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

    // Suffix stripping shrinks (never grows) the word and yields a non-empty
    // stem.
    #[test]
    fn suffix_stripped_nonempty() {
        unsafe {
            let r = stem(b"nacions");
            assert!(!r.is_empty());
            assert!(r.len() <= "nacions".len());
        }
    }
}
