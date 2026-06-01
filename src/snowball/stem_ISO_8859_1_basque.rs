//! Snowball Basque (ISO_8859_1) stemmer.
//!
//! 1:1 translation of the generated
//! `src/backend/snowball/libstemmer/stem_ISO_8859_1_basque.c` (Snowball 2.2.0),
//! merged with its declarations header
//! `src/include/snowball/libstemmer/stem_ISO_8859_1_basque.h`.
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
    find_among_b, in_grouping, out_grouping, slice_del, slice_from_s,
};

// ---------------------------------------------------------------------------
// among string tables
// ---------------------------------------------------------------------------

static S_0_0: [symbol; 4] = [b'i', b'd', b'e', b'a'];
static S_0_1: [symbol; 5] = [b'b', b'i', b'd', b'e', b'a'];
static S_0_2: [symbol; 5] = [b'k', b'i', b'd', b'e', b'a'];
static S_0_3: [symbol; 5] = [b'p', b'i', b'd', b'e', b'a'];
static S_0_4: [symbol; 6] = [b'k', b'u', b'n', b'd', b'e', b'a'];
static S_0_5: [symbol; 5] = [b'g', b'a', b'l', b'e', b'a'];
static S_0_6: [symbol; 6] = [b't', b'a', b'i', b'l', b'e', b'a'];
static S_0_7: [symbol; 7] = [b't', b'z', b'a', b'i', b'l', b'e', b'a'];
static S_0_8: [symbol; 5] = [b'g', b'u', b'n', b'e', b'a'];
static S_0_9: [symbol; 5] = [b'k', b'u', b'n', b'e', b'a'];
static S_0_10: [symbol; 5] = [b't', b'z', b'a', b'g', b'a'];
static S_0_11: [symbol; 4] = [b'g', b'a', b'i', b'a'];
static S_0_12: [symbol; 5] = [b'a', b'l', b'd', b'i', b'a'];
static S_0_13: [symbol; 6] = [b't', b'a', b'l', b'd', b'i', b'a'];
static S_0_14: [symbol; 5] = [b'k', b'a', b'r', b'i', b'a'];
static S_0_15: [symbol; 6] = [b'g', b'a', b'r', b'r', b'i', b'a'];
static S_0_16: [symbol; 6] = [b'k', b'a', b'r', b'r', b'i', b'a'];
static S_0_17: [symbol; 2] = [b'k', b'a'];
static S_0_18: [symbol; 5] = [b't', b'z', b'a', b'k', b'a'];
static S_0_19: [symbol; 2] = [b'l', b'a'];
static S_0_20: [symbol; 4] = [b'm', b'e', b'n', b'a'];
static S_0_21: [symbol; 4] = [b'p', b'e', b'n', b'a'];
static S_0_22: [symbol; 4] = [b'k', b'i', b'n', b'a'];
static S_0_23: [symbol; 5] = [b'e', b'z', b'i', b'n', b'a'];
static S_0_24: [symbol; 6] = [b't', b'e', b'z', b'i', b'n', b'a'];
static S_0_25: [symbol; 4] = [b'k', b'u', b'n', b'a'];
static S_0_26: [symbol; 4] = [b't', b'u', b'n', b'a'];
static S_0_27: [symbol; 6] = [b'k', b'i', b'z', b'u', b'n', b'a'];
static S_0_28: [symbol; 3] = [b'e', b'r', b'a'];
static S_0_29: [symbol; 4] = [b'b', b'e', b'r', b'a'];
static S_0_30: [symbol; 7] = [b'a', b'r', b'a', b'b', b'e', b'r', b'a'];
static S_0_31: [symbol; 4] = [b'k', b'e', b'r', b'a'];
static S_0_32: [symbol; 4] = [b'p', b'e', b'r', b'a'];
static S_0_33: [symbol; 4] = [b'o', b'r', b'r', b'a'];
static S_0_34: [symbol; 5] = [b'k', b'o', b'r', b'r', b'a'];
static S_0_35: [symbol; 4] = [b'd', b'u', b'r', b'a'];
static S_0_36: [symbol; 4] = [b'g', b'u', b'r', b'a'];
static S_0_37: [symbol; 4] = [b'k', b'u', b'r', b'a'];
static S_0_38: [symbol; 4] = [b't', b'u', b'r', b'a'];
static S_0_39: [symbol; 3] = [b'e', b't', b'a'];
static S_0_40: [symbol; 4] = [b'k', b'e', b't', b'a'];
static S_0_41: [symbol; 6] = [b'g', b'a', b'i', b'l', b'u', b'a'];
static S_0_42: [symbol; 3] = [b'e', b'z', b'a'];
static S_0_43: [symbol; 6] = [b'e', b'r', b'r', b'e', b'z', b'a'];
static S_0_44: [symbol; 3] = [b't', b'z', b'a'];
static S_0_45: [symbol; 6] = [b'g', b'a', b'i', b't', b'z', b'a'];
static S_0_46: [symbol; 6] = [b'k', b'a', b'i', b't', b'z', b'a'];
static S_0_47: [symbol; 6] = [b'k', b'u', b'n', b't', b'z', b'a'];
static S_0_48: [symbol; 3] = [b'i', b'd', b'e'];
static S_0_49: [symbol; 4] = [b'b', b'i', b'd', b'e'];
static S_0_50: [symbol; 4] = [b'k', b'i', b'd', b'e'];
static S_0_51: [symbol; 4] = [b'p', b'i', b'd', b'e'];
static S_0_52: [symbol; 5] = [b'k', b'u', b'n', b'd', b'e'];
static S_0_53: [symbol; 5] = [b't', b'z', b'a', b'k', b'e'];
static S_0_54: [symbol; 5] = [b't', b'z', b'e', b'k', b'e'];
static S_0_55: [symbol; 2] = [b'l', b'e'];
static S_0_56: [symbol; 4] = [b'g', b'a', b'l', b'e'];
static S_0_57: [symbol; 5] = [b't', b'a', b'i', b'l', b'e'];
static S_0_58: [symbol; 6] = [b't', b'z', b'a', b'i', b'l', b'e'];
static S_0_59: [symbol; 4] = [b'g', b'u', b'n', b'e'];
static S_0_60: [symbol; 4] = [b'k', b'u', b'n', b'e'];
static S_0_61: [symbol; 3] = [b't', b'z', b'e'];
static S_0_62: [symbol; 4] = [b'a', b't', b'z', b'e'];
static S_0_63: [symbol; 3] = [b'g', b'a', b'i'];
static S_0_64: [symbol; 4] = [b'a', b'l', b'd', b'i'];
static S_0_65: [symbol; 5] = [b't', b'a', b'l', b'd', b'i'];
static S_0_66: [symbol; 2] = [b'k', b'i'];
static S_0_67: [symbol; 3] = [b'a', b'r', b'i'];
static S_0_68: [symbol; 4] = [b'k', b'a', b'r', b'i'];
static S_0_69: [symbol; 4] = [b'l', b'a', b'r', b'i'];
static S_0_70: [symbol; 4] = [b't', b'a', b'r', b'i'];
static S_0_71: [symbol; 5] = [b'e', b't', b'a', b'r', b'i'];
static S_0_72: [symbol; 5] = [b'g', b'a', b'r', b'r', b'i'];
static S_0_73: [symbol; 5] = [b'k', b'a', b'r', b'r', b'i'];
static S_0_74: [symbol; 5] = [b'a', b'r', b'a', b'z', b'i'];
static S_0_75: [symbol; 6] = [b't', b'a', b'r', b'a', b'z', b'i'];
static S_0_76: [symbol; 2] = [b'a', b'n'];
static S_0_77: [symbol; 3] = [b'e', b'a', b'n'];
static S_0_78: [symbol; 4] = [b'r', b'e', b'a', b'n'];
static S_0_79: [symbol; 3] = [b'k', b'a', b'n'];
static S_0_80: [symbol; 4] = [b'e', b't', b'a', b'n'];
static S_0_81: [symbol; 7] = [b'a', b't', b's', b'e', b'd', b'e', b'n'];
static S_0_82: [symbol; 3] = [b'm', b'e', b'n'];
static S_0_83: [symbol; 3] = [b'p', b'e', b'n'];
static S_0_84: [symbol; 3] = [b'k', b'i', b'n'];
static S_0_85: [symbol; 5] = [b'r', b'e', b'k', b'i', b'n'];
static S_0_86: [symbol; 4] = [b'e', b'z', b'i', b'n'];
static S_0_87: [symbol; 5] = [b't', b'e', b'z', b'i', b'n'];
static S_0_88: [symbol; 3] = [b't', b'u', b'n'];
static S_0_89: [symbol; 5] = [b'k', b'i', b'z', b'u', b'n'];
static S_0_90: [symbol; 2] = [b'g', b'o'];
static S_0_91: [symbol; 3] = [b'a', b'g', b'o'];
static S_0_92: [symbol; 3] = [b't', b'i', b'o'];
static S_0_93: [symbol; 4] = [b'd', b'a', b'k', b'o'];
static S_0_94: [symbol; 2] = [b'o', b'r'];
static S_0_95: [symbol; 3] = [b'k', b'o', b'r'];
static S_0_96: [symbol; 4] = [b't', b'z', b'a', b't'];
static S_0_97: [symbol; 2] = [b'd', b'u'];
static S_0_98: [symbol; 5] = [b'g', b'a', b'i', b'l', b'u'];
static S_0_99: [symbol; 2] = [b't', b'u'];
static S_0_100: [symbol; 3] = [b'a', b't', b'u'];
static S_0_101: [symbol; 6] = [b'a', b'l', b'd', b'a', b't', b'u'];
static S_0_102: [symbol; 4] = [b't', b'a', b't', b'u'];
static S_0_103: [symbol; 6] = [b'b', b'a', b'd', b'i', b't', b'u'];
static S_0_104: [symbol; 2] = [b'e', b'z'];
static S_0_105: [symbol; 5] = [b'e', b'r', b'r', b'e', b'z'];
static S_0_106: [symbol; 4] = [b't', b'z', b'e', b'z'];
static S_0_107: [symbol; 5] = [b'g', b'a', b'i', b't', b'z'];
static S_0_108: [symbol; 5] = [b'k', b'a', b'i', b't', b'z'];
static S_1_0: [symbol; 3] = [b'a', b'd', b'a'];
static S_1_1: [symbol; 4] = [b'k', b'a', b'd', b'a'];
static S_1_2: [symbol; 4] = [b'a', b'n', b'd', b'a'];
static S_1_3: [symbol; 5] = [b'd', b'e', b'n', b'd', b'a'];
static S_1_4: [symbol; 5] = [b'g', b'a', b'b', b'e', b'a'];
static S_1_5: [symbol; 5] = [b'k', b'a', b'b', b'e', b'a'];
static S_1_6: [symbol; 5] = [b'a', b'l', b'd', b'e', b'a'];
static S_1_7: [symbol; 6] = [b'k', b'a', b'l', b'd', b'e', b'a'];
static S_1_8: [symbol; 6] = [b't', b'a', b'l', b'd', b'e', b'a'];
static S_1_9: [symbol; 5] = [b'o', b'r', b'd', b'e', b'a'];
static S_1_10: [symbol; 5] = [b'z', b'a', b'l', b'e', b'a'];
static S_1_11: [symbol; 6] = [b't', b'z', b'a', b'l', b'e', b'a'];
static S_1_12: [symbol; 5] = [b'g', b'i', b'l', b'e', b'a'];
static S_1_13: [symbol; 4] = [b'e', b'm', b'e', b'a'];
static S_1_14: [symbol; 5] = [b'k', b'u', b'm', b'e', b'a'];
static S_1_15: [symbol; 3] = [b'n', b'e', b'a'];
static S_1_16: [symbol; 4] = [b'e', b'n', b'e', b'a'];
static S_1_17: [symbol; 6] = [b'z', b'i', b'o', b'n', b'e', b'a'];
static S_1_18: [symbol; 4] = [b'u', b'n', b'e', b'a'];
static S_1_19: [symbol; 5] = [b'g', b'u', b'n', b'e', b'a'];
static S_1_20: [symbol; 3] = [b'p', b'e', b'a'];
static S_1_21: [symbol; 6] = [b'a', b'u', b'r', b'r', b'e', b'a'];
static S_1_22: [symbol; 3] = [b't', b'e', b'a'];
static S_1_23: [symbol; 5] = [b'k', b'o', b't', b'e', b'a'];
static S_1_24: [symbol; 5] = [b'a', b'r', b't', b'e', b'a'];
static S_1_25: [symbol; 5] = [b'o', b's', b't', b'e', b'a'];
static S_1_26: [symbol; 5] = [b'e', b't', b'x', b'e', b'a'];
static S_1_27: [symbol; 2] = [b'g', b'a'];
static S_1_28: [symbol; 4] = [b'a', b'n', b'g', b'a'];
static S_1_29: [symbol; 4] = [b'g', b'a', b'i', b'a'];
static S_1_30: [symbol; 5] = [b'a', b'l', b'd', b'i', b'a'];
static S_1_31: [symbol; 6] = [b't', b'a', b'l', b'd', b'i', b'a'];
static S_1_32: [symbol; 6] = [b'h', b'a', b'n', b'd', b'i', b'a'];
static S_1_33: [symbol; 6] = [b'm', b'e', b'n', b'd', b'i', b'a'];
static S_1_34: [symbol; 4] = [b'g', b'e', b'i', b'a'];
static S_1_35: [symbol; 4] = [b'e', b'g', b'i', b'a'];
static S_1_36: [symbol; 5] = [b'd', b'e', b'g', b'i', b'a'];
static S_1_37: [symbol; 5] = [b't', b'e', b'g', b'i', b'a'];
static S_1_38: [symbol; 5] = [b'n', b'a', b'h', b'i', b'a'];
static S_1_39: [symbol; 4] = [b'o', b'h', b'i', b'a'];
static S_1_40: [symbol; 3] = [b'k', b'i', b'a'];
static S_1_41: [symbol; 5] = [b't', b'o', b'k', b'i', b'a'];
static S_1_42: [symbol; 3] = [b'o', b'i', b'a'];
static S_1_43: [symbol; 4] = [b'k', b'o', b'i', b'a'];
static S_1_44: [symbol; 4] = [b'a', b'r', b'i', b'a'];
static S_1_45: [symbol; 5] = [b'k', b'a', b'r', b'i', b'a'];
static S_1_46: [symbol; 5] = [b'l', b'a', b'r', b'i', b'a'];
static S_1_47: [symbol; 5] = [b't', b'a', b'r', b'i', b'a'];
static S_1_48: [symbol; 4] = [b'e', b'r', b'i', b'a'];
static S_1_49: [symbol; 5] = [b'k', b'e', b'r', b'i', b'a'];
static S_1_50: [symbol; 5] = [b't', b'e', b'r', b'i', b'a'];
static S_1_51: [symbol; 6] = [b'g', b'a', b'r', b'r', b'i', b'a'];
static S_1_52: [symbol; 6] = [b'l', b'a', b'r', b'r', b'i', b'a'];
static S_1_53: [symbol; 6] = [b'k', b'i', b'r', b'r', b'i', b'a'];
static S_1_54: [symbol; 5] = [b'd', b'u', b'r', b'i', b'a'];
static S_1_55: [symbol; 4] = [b'a', b's', b'i', b'a'];
static S_1_56: [symbol; 3] = [b't', b'i', b'a'];
static S_1_57: [symbol; 4] = [b'e', b'z', b'i', b'a'];
static S_1_58: [symbol; 5] = [b'b', b'i', b'z', b'i', b'a'];
static S_1_59: [symbol; 6] = [b'o', b'n', b't', b'z', b'i', b'a'];
static S_1_60: [symbol; 2] = [b'k', b'a'];
static S_1_61: [symbol; 4] = [b'j', b'o', b'k', b'a'];
static S_1_62: [symbol; 5] = [b'a', b'u', b'r', b'k', b'a'];
static S_1_63: [symbol; 3] = [b's', b'k', b'a'];
static S_1_64: [symbol; 3] = [b'x', b'k', b'a'];
static S_1_65: [symbol; 3] = [b'z', b'k', b'a'];
static S_1_66: [symbol; 6] = [b'g', b'i', b'b', b'e', b'l', b'a'];
static S_1_67: [symbol; 4] = [b'g', b'e', b'l', b'a'];
static S_1_68: [symbol; 5] = [b'k', b'a', b'i', b'l', b'a'];
static S_1_69: [symbol; 5] = [b's', b'k', b'i', b'l', b'a'];
static S_1_70: [symbol; 4] = [b't', b'i', b'l', b'a'];
static S_1_71: [symbol; 3] = [b'o', b'l', b'a'];
static S_1_72: [symbol; 2] = [b'n', b'a'];
static S_1_73: [symbol; 4] = [b'k', b'a', b'n', b'a'];
static S_1_74: [symbol; 3] = [b'e', b'n', b'a'];
static S_1_75: [symbol; 7] = [b'g', b'a', b'r', b'r', b'e', b'n', b'a'];
static S_1_76: [symbol; 7] = [b'g', b'e', b'r', b'r', b'e', b'n', b'a'];
static S_1_77: [symbol; 6] = [b'u', b'r', b'r', b'e', b'n', b'a'];
static S_1_78: [symbol; 5] = [b'z', b'a', b'i', b'n', b'a'];
static S_1_79: [symbol; 6] = [b't', b'z', b'a', b'i', b'n', b'a'];
static S_1_80: [symbol; 4] = [b'k', b'i', b'n', b'a'];
static S_1_81: [symbol; 4] = [b'm', b'i', b'n', b'a'];
static S_1_82: [symbol; 5] = [b'g', b'a', b'r', b'n', b'a'];
static S_1_83: [symbol; 3] = [b'u', b'n', b'a'];
static S_1_84: [symbol; 4] = [b'd', b'u', b'n', b'a'];
static S_1_85: [symbol; 5] = [b'a', b's', b'u', b'n', b'a'];
static S_1_86: [symbol; 6] = [b't', b'a', b's', b'u', b'n', b'a'];
static S_1_87: [symbol; 5] = [b'o', b'n', b'd', b'o', b'a'];
static S_1_88: [symbol; 6] = [b'k', b'o', b'n', b'd', b'o', b'a'];
static S_1_89: [symbol; 4] = [b'n', b'g', b'o', b'a'];
static S_1_90: [symbol; 4] = [b'z', b'i', b'o', b'a'];
static S_1_91: [symbol; 3] = [b'k', b'o', b'a'];
static S_1_92: [symbol; 5] = [b't', b'a', b'k', b'o', b'a'];
static S_1_93: [symbol; 4] = [b'z', b'k', b'o', b'a'];
static S_1_94: [symbol; 3] = [b'n', b'o', b'a'];
static S_1_95: [symbol; 5] = [b'z', b'i', b'n', b'o', b'a'];
static S_1_96: [symbol; 4] = [b'a', b'r', b'o', b'a'];
static S_1_97: [symbol; 5] = [b't', b'a', b'r', b'o', b'a'];
static S_1_98: [symbol; 5] = [b'z', b'a', b'r', b'o', b'a'];
static S_1_99: [symbol; 4] = [b'e', b'r', b'o', b'a'];
static S_1_100: [symbol; 4] = [b'o', b'r', b'o', b'a'];
static S_1_101: [symbol; 4] = [b'o', b's', b'o', b'a'];
static S_1_102: [symbol; 3] = [b't', b'o', b'a'];
static S_1_103: [symbol; 4] = [b't', b't', b'o', b'a'];
static S_1_104: [symbol; 4] = [b'z', b't', b'o', b'a'];
static S_1_105: [symbol; 4] = [b't', b'x', b'o', b'a'];
static S_1_106: [symbol; 4] = [b't', b'z', b'o', b'a'];
static S_1_107: [symbol; 3] = [0xf1, b'o', b'a'];
static S_1_108: [symbol; 2] = [b'r', b'a'];
static S_1_109: [symbol; 3] = [b'a', b'r', b'a'];
static S_1_110: [symbol; 4] = [b'd', b'a', b'r', b'a'];
static S_1_111: [symbol; 5] = [b'l', b'i', b'a', b'r', b'a'];
static S_1_112: [symbol; 5] = [b't', b'i', b'a', b'r', b'a'];
static S_1_113: [symbol; 4] = [b't', b'a', b'r', b'a'];
static S_1_114: [symbol; 5] = [b'e', b't', b'a', b'r', b'a'];
static S_1_115: [symbol; 5] = [b't', b'z', b'a', b'r', b'a'];
static S_1_116: [symbol; 4] = [b'b', b'e', b'r', b'a'];
static S_1_117: [symbol; 4] = [b'k', b'e', b'r', b'a'];
static S_1_118: [symbol; 4] = [b'p', b'e', b'r', b'a'];
static S_1_119: [symbol; 3] = [b'o', b'r', b'a'];
static S_1_120: [symbol; 6] = [b't', b'z', b'a', b'r', b'r', b'a'];
static S_1_121: [symbol; 5] = [b'k', b'o', b'r', b'r', b'a'];
static S_1_122: [symbol; 3] = [b't', b'r', b'a'];
static S_1_123: [symbol; 2] = [b's', b'a'];
static S_1_124: [symbol; 3] = [b'o', b's', b'a'];
static S_1_125: [symbol; 2] = [b't', b'a'];
static S_1_126: [symbol; 3] = [b'e', b't', b'a'];
static S_1_127: [symbol; 4] = [b'k', b'e', b't', b'a'];
static S_1_128: [symbol; 3] = [b's', b't', b'a'];
static S_1_129: [symbol; 3] = [b'd', b'u', b'a'];
static S_1_130: [symbol; 6] = [b'm', b'e', b'n', b'd', b'u', b'a'];
static S_1_131: [symbol; 5] = [b'o', b'r', b'd', b'u', b'a'];
static S_1_132: [symbol; 5] = [b'l', b'e', b'k', b'u', b'a'];
static S_1_133: [symbol; 5] = [b'b', b'u', b'r', b'u', b'a'];
static S_1_134: [symbol; 5] = [b'd', b'u', b'r', b'u', b'a'];
static S_1_135: [symbol; 4] = [b't', b's', b'u', b'a'];
static S_1_136: [symbol; 3] = [b't', b'u', b'a'];
static S_1_137: [symbol; 6] = [b'm', b'e', b'n', b't', b'u', b'a'];
static S_1_138: [symbol; 5] = [b'e', b's', b't', b'u', b'a'];
static S_1_139: [symbol; 4] = [b't', b'x', b'u', b'a'];
static S_1_140: [symbol; 3] = [b'z', b'u', b'a'];
static S_1_141: [symbol; 4] = [b't', b'z', b'u', b'a'];
static S_1_142: [symbol; 2] = [b'z', b'a'];
static S_1_143: [symbol; 3] = [b'e', b'z', b'a'];
static S_1_144: [symbol; 5] = [b'e', b'r', b'o', b'z', b'a'];
static S_1_145: [symbol; 3] = [b't', b'z', b'a'];
static S_1_146: [symbol; 6] = [b'k', b'o', b'i', b't', b'z', b'a'];
static S_1_147: [symbol; 5] = [b'a', b'n', b't', b'z', b'a'];
static S_1_148: [symbol; 6] = [b'g', b'i', b'n', b't', b'z', b'a'];
static S_1_149: [symbol; 6] = [b'k', b'i', b'n', b't', b'z', b'a'];
static S_1_150: [symbol; 6] = [b'k', b'u', b'n', b't', b'z', b'a'];
static S_1_151: [symbol; 4] = [b'g', b'a', b'b', b'e'];
static S_1_152: [symbol; 4] = [b'k', b'a', b'b', b'e'];
static S_1_153: [symbol; 4] = [b'k', b'i', b'd', b'e'];
static S_1_154: [symbol; 4] = [b'a', b'l', b'd', b'e'];
static S_1_155: [symbol; 5] = [b'k', b'a', b'l', b'd', b'e'];
static S_1_156: [symbol; 5] = [b't', b'a', b'l', b'd', b'e'];
static S_1_157: [symbol; 4] = [b'o', b'r', b'd', b'e'];
static S_1_158: [symbol; 2] = [b'g', b'e'];
static S_1_159: [symbol; 4] = [b'z', b'a', b'l', b'e'];
static S_1_160: [symbol; 5] = [b't', b'z', b'a', b'l', b'e'];
static S_1_161: [symbol; 4] = [b'g', b'i', b'l', b'e'];
static S_1_162: [symbol; 3] = [b'e', b'm', b'e'];
static S_1_163: [symbol; 4] = [b'k', b'u', b'm', b'e'];
static S_1_164: [symbol; 2] = [b'n', b'e'];
static S_1_165: [symbol; 5] = [b'z', b'i', b'o', b'n', b'e'];
static S_1_166: [symbol; 3] = [b'u', b'n', b'e'];
static S_1_167: [symbol; 4] = [b'g', b'u', b'n', b'e'];
static S_1_168: [symbol; 2] = [b'p', b'e'];
static S_1_169: [symbol; 5] = [b'a', b'u', b'r', b'r', b'e'];
static S_1_170: [symbol; 2] = [b't', b'e'];
static S_1_171: [symbol; 4] = [b'k', b'o', b't', b'e'];
static S_1_172: [symbol; 4] = [b'a', b'r', b't', b'e'];
static S_1_173: [symbol; 4] = [b'o', b's', b't', b'e'];
static S_1_174: [symbol; 4] = [b'e', b't', b'x', b'e'];
static S_1_175: [symbol; 3] = [b'g', b'a', b'i'];
static S_1_176: [symbol; 2] = [b'd', b'i'];
static S_1_177: [symbol; 4] = [b'a', b'l', b'd', b'i'];
static S_1_178: [symbol; 5] = [b't', b'a', b'l', b'd', b'i'];
static S_1_179: [symbol; 5] = [b'g', b'e', b'l', b'd', b'i'];
static S_1_180: [symbol; 5] = [b'h', b'a', b'n', b'd', b'i'];
static S_1_181: [symbol; 5] = [b'm', b'e', b'n', b'd', b'i'];
static S_1_182: [symbol; 3] = [b'g', b'e', b'i'];
static S_1_183: [symbol; 3] = [b'e', b'g', b'i'];
static S_1_184: [symbol; 4] = [b'd', b'e', b'g', b'i'];
static S_1_185: [symbol; 4] = [b't', b'e', b'g', b'i'];
static S_1_186: [symbol; 4] = [b'n', b'a', b'h', b'i'];
static S_1_187: [symbol; 3] = [b'o', b'h', b'i'];
static S_1_188: [symbol; 2] = [b'k', b'i'];
static S_1_189: [symbol; 4] = [b't', b'o', b'k', b'i'];
static S_1_190: [symbol; 2] = [b'o', b'i'];
static S_1_191: [symbol; 3] = [b'g', b'o', b'i'];
static S_1_192: [symbol; 3] = [b'k', b'o', b'i'];
static S_1_193: [symbol; 3] = [b'a', b'r', b'i'];
static S_1_194: [symbol; 4] = [b'k', b'a', b'r', b'i'];
static S_1_195: [symbol; 4] = [b'l', b'a', b'r', b'i'];
static S_1_196: [symbol; 4] = [b't', b'a', b'r', b'i'];
static S_1_197: [symbol; 5] = [b'g', b'a', b'r', b'r', b'i'];
static S_1_198: [symbol; 5] = [b'l', b'a', b'r', b'r', b'i'];
static S_1_199: [symbol; 5] = [b'k', b'i', b'r', b'r', b'i'];
static S_1_200: [symbol; 4] = [b'd', b'u', b'r', b'i'];
static S_1_201: [symbol; 3] = [b'a', b's', b'i'];
static S_1_202: [symbol; 2] = [b't', b'i'];
static S_1_203: [symbol; 5] = [b'o', b'n', b't', b'z', b'i'];
static S_1_204: [symbol; 2] = [0xf1, b'i'];
static S_1_205: [symbol; 2] = [b'a', b'k'];
static S_1_206: [symbol; 2] = [b'e', b'k'];
static S_1_207: [symbol; 5] = [b't', b'a', b'r', b'i', b'k'];
static S_1_208: [symbol; 5] = [b'g', b'i', b'b', b'e', b'l'];
static S_1_209: [symbol; 3] = [b'a', b'i', b'l'];
static S_1_210: [symbol; 4] = [b'k', b'a', b'i', b'l'];
static S_1_211: [symbol; 3] = [b'k', b'a', b'n'];
static S_1_212: [symbol; 3] = [b't', b'a', b'n'];
static S_1_213: [symbol; 4] = [b'e', b't', b'a', b'n'];
static S_1_214: [symbol; 2] = [b'e', b'n'];
static S_1_215: [symbol; 3] = [b'r', b'e', b'n'];
static S_1_216: [symbol; 6] = [b'g', b'a', b'r', b'r', b'e', b'n'];
static S_1_217: [symbol; 6] = [b'g', b'e', b'r', b'r', b'e', b'n'];
static S_1_218: [symbol; 5] = [b'u', b'r', b'r', b'e', b'n'];
static S_1_219: [symbol; 3] = [b't', b'e', b'n'];
static S_1_220: [symbol; 4] = [b't', b'z', b'e', b'n'];
static S_1_221: [symbol; 4] = [b'z', b'a', b'i', b'n'];
static S_1_222: [symbol; 5] = [b't', b'z', b'a', b'i', b'n'];
static S_1_223: [symbol; 3] = [b'k', b'i', b'n'];
static S_1_224: [symbol; 3] = [b'm', b'i', b'n'];
static S_1_225: [symbol; 3] = [b'd', b'u', b'n'];
static S_1_226: [symbol; 4] = [b'a', b's', b'u', b'n'];
static S_1_227: [symbol; 5] = [b't', b'a', b's', b'u', b'n'];
static S_1_228: [symbol; 5] = [b'a', b'i', b'z', b'u', b'n'];
static S_1_229: [symbol; 4] = [b'o', b'n', b'd', b'o'];
static S_1_230: [symbol; 5] = [b'k', b'o', b'n', b'd', b'o'];
static S_1_231: [symbol; 2] = [b'g', b'o'];
static S_1_232: [symbol; 3] = [b'n', b'g', b'o'];
static S_1_233: [symbol; 3] = [b'z', b'i', b'o'];
static S_1_234: [symbol; 2] = [b'k', b'o'];
static S_1_235: [symbol; 5] = [b't', b'r', b'a', b'k', b'o'];
static S_1_236: [symbol; 4] = [b't', b'a', b'k', b'o'];
static S_1_237: [symbol; 5] = [b'e', b't', b'a', b'k', b'o'];
static S_1_238: [symbol; 3] = [b'e', b'k', b'o'];
static S_1_239: [symbol; 6] = [b't', b'a', b'r', b'i', b'k', b'o'];
static S_1_240: [symbol; 3] = [b's', b'k', b'o'];
static S_1_241: [symbol; 4] = [b't', b'u', b'k', b'o'];
static S_1_242: [symbol; 8] = [b'm', b'i', b'n', b'u', b't', b'u', b'k', b'o'];
static S_1_243: [symbol; 3] = [b'z', b'k', b'o'];
static S_1_244: [symbol; 2] = [b'n', b'o'];
static S_1_245: [symbol; 4] = [b'z', b'i', b'n', b'o'];
static S_1_246: [symbol; 2] = [b'r', b'o'];
static S_1_247: [symbol; 3] = [b'a', b'r', b'o'];
static S_1_248: [symbol; 5] = [b'i', b'g', b'a', b'r', b'o'];
static S_1_249: [symbol; 4] = [b't', b'a', b'r', b'o'];
static S_1_250: [symbol; 4] = [b'z', b'a', b'r', b'o'];
static S_1_251: [symbol; 3] = [b'e', b'r', b'o'];
static S_1_252: [symbol; 4] = [b'g', b'i', b'r', b'o'];
static S_1_253: [symbol; 3] = [b'o', b'r', b'o'];
static S_1_254: [symbol; 3] = [b'o', b's', b'o'];
static S_1_255: [symbol; 2] = [b't', b'o'];
static S_1_256: [symbol; 3] = [b't', b't', b'o'];
static S_1_257: [symbol; 3] = [b'z', b't', b'o'];
static S_1_258: [symbol; 3] = [b't', b'x', b'o'];
static S_1_259: [symbol; 3] = [b't', b'z', b'o'];
static S_1_260: [symbol; 6] = [b'g', b'i', b'n', b't', b'z', b'o'];
static S_1_261: [symbol; 2] = [0xf1, b'o'];
static S_1_262: [symbol; 2] = [b'z', b'p'];
static S_1_263: [symbol; 2] = [b'a', b'r'];
static S_1_264: [symbol; 3] = [b'd', b'a', b'r'];
static S_1_265: [symbol; 5] = [b'b', b'e', b'h', b'a', b'r'];
static S_1_266: [symbol; 5] = [b'z', b'e', b'h', b'a', b'r'];
static S_1_267: [symbol; 4] = [b'l', b'i', b'a', b'r'];
static S_1_268: [symbol; 4] = [b't', b'i', b'a', b'r'];
static S_1_269: [symbol; 3] = [b't', b'a', b'r'];
static S_1_270: [symbol; 4] = [b't', b'z', b'a', b'r'];
static S_1_271: [symbol; 2] = [b'o', b'r'];
static S_1_272: [symbol; 3] = [b'k', b'o', b'r'];
static S_1_273: [symbol; 2] = [b'o', b's'];
static S_1_274: [symbol; 3] = [b'k', b'e', b't'];
static S_1_275: [symbol; 2] = [b'd', b'u'];
static S_1_276: [symbol; 5] = [b'm', b'e', b'n', b'd', b'u'];
static S_1_277: [symbol; 4] = [b'o', b'r', b'd', b'u'];
static S_1_278: [symbol; 4] = [b'l', b'e', b'k', b'u'];
static S_1_279: [symbol; 4] = [b'b', b'u', b'r', b'u'];
static S_1_280: [symbol; 4] = [b'd', b'u', b'r', b'u'];
static S_1_281: [symbol; 3] = [b't', b's', b'u'];
static S_1_282: [symbol; 2] = [b't', b'u'];
static S_1_283: [symbol; 4] = [b't', b'a', b't', b'u'];
static S_1_284: [symbol; 5] = [b'm', b'e', b'n', b't', b'u'];
static S_1_285: [symbol; 4] = [b'e', b's', b't', b'u'];
static S_1_286: [symbol; 3] = [b't', b'x', b'u'];
static S_1_287: [symbol; 2] = [b'z', b'u'];
static S_1_288: [symbol; 3] = [b't', b'z', b'u'];
static S_1_289: [symbol; 6] = [b'g', b'i', b'n', b't', b'z', b'u'];
static S_1_290: [symbol; 1] = [b'z'];
static S_1_291: [symbol; 2] = [b'e', b'z'];
static S_1_292: [symbol; 4] = [b'e', b'r', b'o', b'z'];
static S_1_293: [symbol; 2] = [b't', b'z'];
static S_1_294: [symbol; 5] = [b'k', b'o', b'i', b't', b'z'];
static S_2_0: [symbol; 4] = [b'z', b'l', b'e', b'a'];
static S_2_1: [symbol; 5] = [b'k', b'e', b'r', b'i', b'a'];
static S_2_2: [symbol; 2] = [b'l', b'a'];
static S_2_3: [symbol; 3] = [b'e', b'r', b'a'];
static S_2_4: [symbol; 4] = [b'd', b'a', b'd', b'e'];
static S_2_5: [symbol; 4] = [b't', b'a', b'd', b'e'];
static S_2_6: [symbol; 4] = [b'd', b'a', b't', b'e'];
static S_2_7: [symbol; 4] = [b't', b'a', b't', b'e'];
static S_2_8: [symbol; 2] = [b'g', b'i'];
static S_2_9: [symbol; 2] = [b'k', b'i'];
static S_2_10: [symbol; 2] = [b'i', b'k'];
static S_2_11: [symbol; 5] = [b'l', b'a', b'n', b'i', b'k'];
static S_2_12: [symbol; 3] = [b'r', b'i', b'k'];
static S_2_13: [symbol; 5] = [b'l', b'a', b'r', b'i', b'k'];
static S_2_14: [symbol; 4] = [b'z', b't', b'i', b'k'];
static S_2_15: [symbol; 2] = [b'g', b'o'];
static S_2_16: [symbol; 2] = [b'r', b'o'];
static S_2_17: [symbol; 3] = [b'e', b'r', b'o'];
static S_2_18: [symbol; 2] = [b't', b'o'];
static S_0: [symbol; 7] = [b'a', b't', b's', b'e', b'd', b'e', b'n'];
static S_1: [symbol; 7] = [b'a', b'r', b'a', b'b', b'e', b'r', b'a'];
static S_2: [symbol; 6] = [b'b', b'a', b'd', b'i', b't', b'u'];
static S_3: [symbol; 3] = [b'j', b'o', b'k'];
static S_4: [symbol; 3] = [b't', b'r', b'a'];
static S_5: [symbol; 6] = [b'm', b'i', b'n', b'u', b't', b'u'];
static S_6: [symbol; 5] = [b'z', b'e', b'h', b'a', b'r'];
static S_7: [symbol; 5] = [b'g', b'e', b'l', b'd', b'i'];
static S_8: [symbol; 5] = [b'i', b'g', b'a', b'r', b'o'];
static S_9: [symbol; 5] = [b'a', b'u', b'r', b'k', b'a'];
static S_10: [symbol; 1] = [b'z'];

// ---------------------------------------------------------------------------
// among tables
// ---------------------------------------------------------------------------

static A_0: [among; 109] = [
    among { s_size: 4, s: S_0_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_0_1.as_ptr(), substring_i: 0, result: 1, function: None },
    among { s_size: 5, s: S_0_2.as_ptr(), substring_i: 0, result: 1, function: None },
    among { s_size: 5, s: S_0_3.as_ptr(), substring_i: 0, result: 1, function: None },
    among { s_size: 6, s: S_0_4.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_0_5.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_0_6.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 7, s: S_0_7.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_0_8.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_0_9.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_0_10.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_0_11.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_0_12.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_0_13.as_ptr(), substring_i: 12, result: 1, function: None },
    among { s_size: 5, s: S_0_14.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_0_15.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 6, s: S_0_16.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_0_17.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_0_18.as_ptr(), substring_i: 17, result: 1, function: None },
    among { s_size: 2, s: S_0_19.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_0_20.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_0_21.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_0_22.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_0_23.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_0_24.as_ptr(), substring_i: 23, result: 1, function: None },
    among { s_size: 4, s: S_0_25.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_0_26.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_0_27.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_0_28.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_0_29.as_ptr(), substring_i: 28, result: 1, function: None },
    among { s_size: 7, s: S_0_30.as_ptr(), substring_i: 29, result: 4, function: None },
    among { s_size: 4, s: S_0_31.as_ptr(), substring_i: 28, result: 1, function: None },
    among { s_size: 4, s: S_0_32.as_ptr(), substring_i: 28, result: 1, function: None },
    among { s_size: 4, s: S_0_33.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_0_34.as_ptr(), substring_i: 33, result: 1, function: None },
    among { s_size: 4, s: S_0_35.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_0_36.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_0_37.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_0_38.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_0_39.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_0_40.as_ptr(), substring_i: 39, result: 1, function: None },
    among { s_size: 6, s: S_0_41.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_0_42.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_0_43.as_ptr(), substring_i: 42, result: 1, function: None },
    among { s_size: 3, s: S_0_44.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 6, s: S_0_45.as_ptr(), substring_i: 44, result: 1, function: None },
    among { s_size: 6, s: S_0_46.as_ptr(), substring_i: 44, result: 1, function: None },
    among { s_size: 6, s: S_0_47.as_ptr(), substring_i: 44, result: 1, function: None },
    among { s_size: 3, s: S_0_48.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_0_49.as_ptr(), substring_i: 48, result: 1, function: None },
    among { s_size: 4, s: S_0_50.as_ptr(), substring_i: 48, result: 1, function: None },
    among { s_size: 4, s: S_0_51.as_ptr(), substring_i: 48, result: 1, function: None },
    among { s_size: 5, s: S_0_52.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_0_53.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_0_54.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_0_55.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_0_56.as_ptr(), substring_i: 55, result: 1, function: None },
    among { s_size: 5, s: S_0_57.as_ptr(), substring_i: 55, result: 1, function: None },
    among { s_size: 6, s: S_0_58.as_ptr(), substring_i: 55, result: 1, function: None },
    among { s_size: 4, s: S_0_59.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_0_60.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_0_61.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_0_62.as_ptr(), substring_i: 61, result: 1, function: None },
    among { s_size: 3, s: S_0_63.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_0_64.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_0_65.as_ptr(), substring_i: 64, result: 1, function: None },
    among { s_size: 2, s: S_0_66.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_0_67.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_0_68.as_ptr(), substring_i: 67, result: 1, function: None },
    among { s_size: 4, s: S_0_69.as_ptr(), substring_i: 67, result: 1, function: None },
    among { s_size: 4, s: S_0_70.as_ptr(), substring_i: 67, result: 1, function: None },
    among { s_size: 5, s: S_0_71.as_ptr(), substring_i: 70, result: 1, function: None },
    among { s_size: 5, s: S_0_72.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 5, s: S_0_73.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_0_74.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_0_75.as_ptr(), substring_i: 74, result: 1, function: None },
    among { s_size: 2, s: S_0_76.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_0_77.as_ptr(), substring_i: 76, result: 1, function: None },
    among { s_size: 4, s: S_0_78.as_ptr(), substring_i: 77, result: 1, function: None },
    among { s_size: 3, s: S_0_79.as_ptr(), substring_i: 76, result: 1, function: None },
    among { s_size: 4, s: S_0_80.as_ptr(), substring_i: 76, result: 1, function: None },
    among { s_size: 7, s: S_0_81.as_ptr(), substring_i: -1, result: 3, function: None },
    among { s_size: 3, s: S_0_82.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_0_83.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_0_84.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_0_85.as_ptr(), substring_i: 84, result: 1, function: None },
    among { s_size: 4, s: S_0_86.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_0_87.as_ptr(), substring_i: 86, result: 1, function: None },
    among { s_size: 3, s: S_0_88.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_0_89.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_0_90.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_0_91.as_ptr(), substring_i: 90, result: 1, function: None },
    among { s_size: 3, s: S_0_92.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_0_93.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_0_94.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_0_95.as_ptr(), substring_i: 94, result: 1, function: None },
    among { s_size: 4, s: S_0_96.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_0_97.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_0_98.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_0_99.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_0_100.as_ptr(), substring_i: 99, result: 1, function: None },
    among { s_size: 6, s: S_0_101.as_ptr(), substring_i: 100, result: 1, function: None },
    among { s_size: 4, s: S_0_102.as_ptr(), substring_i: 100, result: 1, function: None },
    among { s_size: 6, s: S_0_103.as_ptr(), substring_i: 99, result: 5, function: None },
    among { s_size: 2, s: S_0_104.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_0_105.as_ptr(), substring_i: 104, result: 1, function: None },
    among { s_size: 4, s: S_0_106.as_ptr(), substring_i: 104, result: 1, function: None },
    among { s_size: 5, s: S_0_107.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_0_108.as_ptr(), substring_i: -1, result: 1, function: None },
];
static A_1: [among; 295] = [
    among { s_size: 3, s: S_1_0.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_1_1.as_ptr(), substring_i: 0, result: 1, function: None },
    among { s_size: 4, s: S_1_2.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_1_3.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_1_4.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_1_5.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_1_6.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_1_7.as_ptr(), substring_i: 6, result: 1, function: None },
    among { s_size: 6, s: S_1_8.as_ptr(), substring_i: 6, result: 1, function: None },
    among { s_size: 5, s: S_1_9.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_1_10.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_1_11.as_ptr(), substring_i: 10, result: 1, function: None },
    among { s_size: 5, s: S_1_12.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_1_13.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_1_14.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_1_15.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_1_16.as_ptr(), substring_i: 15, result: 1, function: None },
    among { s_size: 6, s: S_1_17.as_ptr(), substring_i: 15, result: 1, function: None },
    among { s_size: 4, s: S_1_18.as_ptr(), substring_i: 15, result: 1, function: None },
    among { s_size: 5, s: S_1_19.as_ptr(), substring_i: 18, result: 1, function: None },
    among { s_size: 3, s: S_1_20.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_1_21.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_1_22.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_1_23.as_ptr(), substring_i: 22, result: 1, function: None },
    among { s_size: 5, s: S_1_24.as_ptr(), substring_i: 22, result: 1, function: None },
    among { s_size: 5, s: S_1_25.as_ptr(), substring_i: 22, result: 1, function: None },
    among { s_size: 5, s: S_1_26.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_1_27.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_1_28.as_ptr(), substring_i: 27, result: 1, function: None },
    among { s_size: 4, s: S_1_29.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_1_30.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_1_31.as_ptr(), substring_i: 30, result: 1, function: None },
    among { s_size: 6, s: S_1_32.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_1_33.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_1_34.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_1_35.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_1_36.as_ptr(), substring_i: 35, result: 1, function: None },
    among { s_size: 5, s: S_1_37.as_ptr(), substring_i: 35, result: 1, function: None },
    among { s_size: 5, s: S_1_38.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_1_39.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_1_40.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_1_41.as_ptr(), substring_i: 40, result: 1, function: None },
    among { s_size: 3, s: S_1_42.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_1_43.as_ptr(), substring_i: 42, result: 1, function: None },
    among { s_size: 4, s: S_1_44.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_1_45.as_ptr(), substring_i: 44, result: 1, function: None },
    among { s_size: 5, s: S_1_46.as_ptr(), substring_i: 44, result: 1, function: None },
    among { s_size: 5, s: S_1_47.as_ptr(), substring_i: 44, result: 1, function: None },
    among { s_size: 4, s: S_1_48.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_1_49.as_ptr(), substring_i: 48, result: 1, function: None },
    among { s_size: 5, s: S_1_50.as_ptr(), substring_i: 48, result: 1, function: None },
    among { s_size: 6, s: S_1_51.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 6, s: S_1_52.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_1_53.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_1_54.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_1_55.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_1_56.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_1_57.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_1_58.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_1_59.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_1_60.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_1_61.as_ptr(), substring_i: 60, result: 3, function: None },
    among { s_size: 5, s: S_1_62.as_ptr(), substring_i: 60, result: 10, function: None },
    among { s_size: 3, s: S_1_63.as_ptr(), substring_i: 60, result: 1, function: None },
    among { s_size: 3, s: S_1_64.as_ptr(), substring_i: 60, result: 1, function: None },
    among { s_size: 3, s: S_1_65.as_ptr(), substring_i: 60, result: 1, function: None },
    among { s_size: 6, s: S_1_66.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_1_67.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_1_68.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_1_69.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_1_70.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_1_71.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_1_72.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_1_73.as_ptr(), substring_i: 72, result: 1, function: None },
    among { s_size: 3, s: S_1_74.as_ptr(), substring_i: 72, result: 1, function: None },
    among { s_size: 7, s: S_1_75.as_ptr(), substring_i: 74, result: 1, function: None },
    among { s_size: 7, s: S_1_76.as_ptr(), substring_i: 74, result: 1, function: None },
    among { s_size: 6, s: S_1_77.as_ptr(), substring_i: 74, result: 1, function: None },
    among { s_size: 5, s: S_1_78.as_ptr(), substring_i: 72, result: 1, function: None },
    among { s_size: 6, s: S_1_79.as_ptr(), substring_i: 78, result: 1, function: None },
    among { s_size: 4, s: S_1_80.as_ptr(), substring_i: 72, result: 1, function: None },
    among { s_size: 4, s: S_1_81.as_ptr(), substring_i: 72, result: 1, function: None },
    among { s_size: 5, s: S_1_82.as_ptr(), substring_i: 72, result: 1, function: None },
    among { s_size: 3, s: S_1_83.as_ptr(), substring_i: 72, result: 1, function: None },
    among { s_size: 4, s: S_1_84.as_ptr(), substring_i: 83, result: 1, function: None },
    among { s_size: 5, s: S_1_85.as_ptr(), substring_i: 83, result: 1, function: None },
    among { s_size: 6, s: S_1_86.as_ptr(), substring_i: 85, result: 1, function: None },
    among { s_size: 5, s: S_1_87.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_1_88.as_ptr(), substring_i: 87, result: 1, function: None },
    among { s_size: 4, s: S_1_89.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_1_90.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_1_91.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_1_92.as_ptr(), substring_i: 91, result: 1, function: None },
    among { s_size: 4, s: S_1_93.as_ptr(), substring_i: 91, result: 1, function: None },
    among { s_size: 3, s: S_1_94.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_1_95.as_ptr(), substring_i: 94, result: 1, function: None },
    among { s_size: 4, s: S_1_96.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_1_97.as_ptr(), substring_i: 96, result: 1, function: None },
    among { s_size: 5, s: S_1_98.as_ptr(), substring_i: 96, result: 1, function: None },
    among { s_size: 4, s: S_1_99.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_1_100.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_1_101.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_1_102.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_1_103.as_ptr(), substring_i: 102, result: 1, function: None },
    among { s_size: 4, s: S_1_104.as_ptr(), substring_i: 102, result: 1, function: None },
    among { s_size: 4, s: S_1_105.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_1_106.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_1_107.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_1_108.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_1_109.as_ptr(), substring_i: 108, result: 1, function: None },
    among { s_size: 4, s: S_1_110.as_ptr(), substring_i: 109, result: 1, function: None },
    among { s_size: 5, s: S_1_111.as_ptr(), substring_i: 109, result: 1, function: None },
    among { s_size: 5, s: S_1_112.as_ptr(), substring_i: 109, result: 1, function: None },
    among { s_size: 4, s: S_1_113.as_ptr(), substring_i: 109, result: 1, function: None },
    among { s_size: 5, s: S_1_114.as_ptr(), substring_i: 113, result: 1, function: None },
    among { s_size: 5, s: S_1_115.as_ptr(), substring_i: 109, result: 1, function: None },
    among { s_size: 4, s: S_1_116.as_ptr(), substring_i: 108, result: 1, function: None },
    among { s_size: 4, s: S_1_117.as_ptr(), substring_i: 108, result: 1, function: None },
    among { s_size: 4, s: S_1_118.as_ptr(), substring_i: 108, result: 1, function: None },
    among { s_size: 3, s: S_1_119.as_ptr(), substring_i: 108, result: 2, function: None },
    among { s_size: 6, s: S_1_120.as_ptr(), substring_i: 108, result: 1, function: None },
    among { s_size: 5, s: S_1_121.as_ptr(), substring_i: 108, result: 1, function: None },
    among { s_size: 3, s: S_1_122.as_ptr(), substring_i: 108, result: 1, function: None },
    among { s_size: 2, s: S_1_123.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_1_124.as_ptr(), substring_i: 123, result: 1, function: None },
    among { s_size: 2, s: S_1_125.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_1_126.as_ptr(), substring_i: 125, result: 1, function: None },
    among { s_size: 4, s: S_1_127.as_ptr(), substring_i: 126, result: 1, function: None },
    among { s_size: 3, s: S_1_128.as_ptr(), substring_i: 125, result: 1, function: None },
    among { s_size: 3, s: S_1_129.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_1_130.as_ptr(), substring_i: 129, result: 1, function: None },
    among { s_size: 5, s: S_1_131.as_ptr(), substring_i: 129, result: 1, function: None },
    among { s_size: 5, s: S_1_132.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_1_133.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_1_134.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_1_135.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_1_136.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_1_137.as_ptr(), substring_i: 136, result: 1, function: None },
    among { s_size: 5, s: S_1_138.as_ptr(), substring_i: 136, result: 1, function: None },
    among { s_size: 4, s: S_1_139.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_1_140.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_1_141.as_ptr(), substring_i: 140, result: 1, function: None },
    among { s_size: 2, s: S_1_142.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_1_143.as_ptr(), substring_i: 142, result: 1, function: None },
    among { s_size: 5, s: S_1_144.as_ptr(), substring_i: 142, result: 1, function: None },
    among { s_size: 3, s: S_1_145.as_ptr(), substring_i: 142, result: 2, function: None },
    among { s_size: 6, s: S_1_146.as_ptr(), substring_i: 145, result: 1, function: None },
    among { s_size: 5, s: S_1_147.as_ptr(), substring_i: 145, result: 1, function: None },
    among { s_size: 6, s: S_1_148.as_ptr(), substring_i: 145, result: 1, function: None },
    among { s_size: 6, s: S_1_149.as_ptr(), substring_i: 145, result: 1, function: None },
    among { s_size: 6, s: S_1_150.as_ptr(), substring_i: 145, result: 1, function: None },
    among { s_size: 4, s: S_1_151.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_1_152.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_1_153.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_1_154.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_1_155.as_ptr(), substring_i: 154, result: 1, function: None },
    among { s_size: 5, s: S_1_156.as_ptr(), substring_i: 154, result: 1, function: None },
    among { s_size: 4, s: S_1_157.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_1_158.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_1_159.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_1_160.as_ptr(), substring_i: 159, result: 1, function: None },
    among { s_size: 4, s: S_1_161.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_1_162.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_1_163.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_1_164.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_1_165.as_ptr(), substring_i: 164, result: 1, function: None },
    among { s_size: 3, s: S_1_166.as_ptr(), substring_i: 164, result: 1, function: None },
    among { s_size: 4, s: S_1_167.as_ptr(), substring_i: 166, result: 1, function: None },
    among { s_size: 2, s: S_1_168.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_1_169.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_1_170.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_1_171.as_ptr(), substring_i: 170, result: 1, function: None },
    among { s_size: 4, s: S_1_172.as_ptr(), substring_i: 170, result: 1, function: None },
    among { s_size: 4, s: S_1_173.as_ptr(), substring_i: 170, result: 1, function: None },
    among { s_size: 4, s: S_1_174.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_1_175.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_1_176.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_1_177.as_ptr(), substring_i: 176, result: 1, function: None },
    among { s_size: 5, s: S_1_178.as_ptr(), substring_i: 177, result: 1, function: None },
    among { s_size: 5, s: S_1_179.as_ptr(), substring_i: 176, result: 8, function: None },
    among { s_size: 5, s: S_1_180.as_ptr(), substring_i: 176, result: 1, function: None },
    among { s_size: 5, s: S_1_181.as_ptr(), substring_i: 176, result: 1, function: None },
    among { s_size: 3, s: S_1_182.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_1_183.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_1_184.as_ptr(), substring_i: 183, result: 1, function: None },
    among { s_size: 4, s: S_1_185.as_ptr(), substring_i: 183, result: 1, function: None },
    among { s_size: 4, s: S_1_186.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_1_187.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_1_188.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_1_189.as_ptr(), substring_i: 188, result: 1, function: None },
    among { s_size: 2, s: S_1_190.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_1_191.as_ptr(), substring_i: 190, result: 1, function: None },
    among { s_size: 3, s: S_1_192.as_ptr(), substring_i: 190, result: 1, function: None },
    among { s_size: 3, s: S_1_193.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_1_194.as_ptr(), substring_i: 193, result: 1, function: None },
    among { s_size: 4, s: S_1_195.as_ptr(), substring_i: 193, result: 1, function: None },
    among { s_size: 4, s: S_1_196.as_ptr(), substring_i: 193, result: 1, function: None },
    among { s_size: 5, s: S_1_197.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 5, s: S_1_198.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_1_199.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_1_200.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_1_201.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_1_202.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_1_203.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_1_204.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_1_205.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_1_206.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_1_207.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_1_208.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_1_209.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_1_210.as_ptr(), substring_i: 209, result: 1, function: None },
    among { s_size: 3, s: S_1_211.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_1_212.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_1_213.as_ptr(), substring_i: 212, result: 1, function: None },
    among { s_size: 2, s: S_1_214.as_ptr(), substring_i: -1, result: 4, function: None },
    among { s_size: 3, s: S_1_215.as_ptr(), substring_i: 214, result: 2, function: None },
    among { s_size: 6, s: S_1_216.as_ptr(), substring_i: 215, result: 1, function: None },
    among { s_size: 6, s: S_1_217.as_ptr(), substring_i: 215, result: 1, function: None },
    among { s_size: 5, s: S_1_218.as_ptr(), substring_i: 215, result: 1, function: None },
    among { s_size: 3, s: S_1_219.as_ptr(), substring_i: 214, result: 4, function: None },
    among { s_size: 4, s: S_1_220.as_ptr(), substring_i: 214, result: 4, function: None },
    among { s_size: 4, s: S_1_221.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_1_222.as_ptr(), substring_i: 221, result: 1, function: None },
    among { s_size: 3, s: S_1_223.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_1_224.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_1_225.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_1_226.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_1_227.as_ptr(), substring_i: 226, result: 1, function: None },
    among { s_size: 5, s: S_1_228.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_1_229.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_1_230.as_ptr(), substring_i: 229, result: 1, function: None },
    among { s_size: 2, s: S_1_231.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_1_232.as_ptr(), substring_i: 231, result: 1, function: None },
    among { s_size: 3, s: S_1_233.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_1_234.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_1_235.as_ptr(), substring_i: 234, result: 5, function: None },
    among { s_size: 4, s: S_1_236.as_ptr(), substring_i: 234, result: 1, function: None },
    among { s_size: 5, s: S_1_237.as_ptr(), substring_i: 236, result: 1, function: None },
    among { s_size: 3, s: S_1_238.as_ptr(), substring_i: 234, result: 1, function: None },
    among { s_size: 6, s: S_1_239.as_ptr(), substring_i: 234, result: 1, function: None },
    among { s_size: 3, s: S_1_240.as_ptr(), substring_i: 234, result: 1, function: None },
    among { s_size: 4, s: S_1_241.as_ptr(), substring_i: 234, result: 1, function: None },
    among { s_size: 8, s: S_1_242.as_ptr(), substring_i: 241, result: 6, function: None },
    among { s_size: 3, s: S_1_243.as_ptr(), substring_i: 234, result: 1, function: None },
    among { s_size: 2, s: S_1_244.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_1_245.as_ptr(), substring_i: 244, result: 1, function: None },
    among { s_size: 2, s: S_1_246.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_1_247.as_ptr(), substring_i: 246, result: 1, function: None },
    among { s_size: 5, s: S_1_248.as_ptr(), substring_i: 247, result: 9, function: None },
    among { s_size: 4, s: S_1_249.as_ptr(), substring_i: 247, result: 1, function: None },
    among { s_size: 4, s: S_1_250.as_ptr(), substring_i: 247, result: 1, function: None },
    among { s_size: 3, s: S_1_251.as_ptr(), substring_i: 246, result: 1, function: None },
    among { s_size: 4, s: S_1_252.as_ptr(), substring_i: 246, result: 1, function: None },
    among { s_size: 3, s: S_1_253.as_ptr(), substring_i: 246, result: 1, function: None },
    among { s_size: 3, s: S_1_254.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_1_255.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_1_256.as_ptr(), substring_i: 255, result: 1, function: None },
    among { s_size: 3, s: S_1_257.as_ptr(), substring_i: 255, result: 1, function: None },
    among { s_size: 3, s: S_1_258.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_1_259.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 6, s: S_1_260.as_ptr(), substring_i: 259, result: 1, function: None },
    among { s_size: 2, s: S_1_261.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_1_262.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_1_263.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_1_264.as_ptr(), substring_i: 263, result: 1, function: None },
    among { s_size: 5, s: S_1_265.as_ptr(), substring_i: 263, result: 1, function: None },
    among { s_size: 5, s: S_1_266.as_ptr(), substring_i: 263, result: 7, function: None },
    among { s_size: 4, s: S_1_267.as_ptr(), substring_i: 263, result: 1, function: None },
    among { s_size: 4, s: S_1_268.as_ptr(), substring_i: 263, result: 1, function: None },
    among { s_size: 3, s: S_1_269.as_ptr(), substring_i: 263, result: 1, function: None },
    among { s_size: 4, s: S_1_270.as_ptr(), substring_i: 263, result: 1, function: None },
    among { s_size: 2, s: S_1_271.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 3, s: S_1_272.as_ptr(), substring_i: 271, result: 1, function: None },
    among { s_size: 2, s: S_1_273.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_1_274.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_1_275.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_1_276.as_ptr(), substring_i: 275, result: 1, function: None },
    among { s_size: 4, s: S_1_277.as_ptr(), substring_i: 275, result: 1, function: None },
    among { s_size: 4, s: S_1_278.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_1_279.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 4, s: S_1_280.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_1_281.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_1_282.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_1_283.as_ptr(), substring_i: 282, result: 4, function: None },
    among { s_size: 5, s: S_1_284.as_ptr(), substring_i: 282, result: 1, function: None },
    among { s_size: 4, s: S_1_285.as_ptr(), substring_i: 282, result: 1, function: None },
    among { s_size: 3, s: S_1_286.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_1_287.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_1_288.as_ptr(), substring_i: 287, result: 1, function: None },
    among { s_size: 6, s: S_1_289.as_ptr(), substring_i: 288, result: 1, function: None },
    among { s_size: 1, s: S_1_290.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_1_291.as_ptr(), substring_i: 290, result: 1, function: None },
    among { s_size: 4, s: S_1_292.as_ptr(), substring_i: 290, result: 1, function: None },
    among { s_size: 2, s: S_1_293.as_ptr(), substring_i: 290, result: 1, function: None },
    among { s_size: 5, s: S_1_294.as_ptr(), substring_i: 293, result: 1, function: None },
];
static A_2: [among; 19] = [
    among { s_size: 4, s: S_2_0.as_ptr(), substring_i: -1, result: 2, function: None },
    among { s_size: 5, s: S_2_1.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_2_2.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_2_3.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_2_4.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_2_5.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_2_6.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 4, s: S_2_7.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_2_8.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_2_9.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_2_10.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 5, s: S_2_11.as_ptr(), substring_i: 10, result: 1, function: None },
    among { s_size: 3, s: S_2_12.as_ptr(), substring_i: 10, result: 1, function: None },
    among { s_size: 5, s: S_2_13.as_ptr(), substring_i: 12, result: 1, function: None },
    among { s_size: 4, s: S_2_14.as_ptr(), substring_i: 10, result: 1, function: None },
    among { s_size: 2, s: S_2_15.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 2, s: S_2_16.as_ptr(), substring_i: -1, result: 1, function: None },
    among { s_size: 3, s: S_2_17.as_ptr(), substring_i: 16, result: 1, function: None },
    among { s_size: 2, s: S_2_18.as_ptr(), substring_i: -1, result: 1, function: None },
];

static G_V: [c_uchar; 3] = [17, 65, 16];

// ---------------------------------------------------------------------------
// stemmer functions
// ---------------------------------------------------------------------------

unsafe extern "C" fn r_mark_regions(z: *mut SN_env) -> c_int {
    *(*z).I.offset(2) = (*z).l;
    *(*z).I.offset(1) = (*z).l;
    *(*z).I.offset(0) = (*z).l;
    {
        let c1 = (*z).c;
        'lab0: {
            {
                let c2 = (*z).c;
                'lab1: {
                    'lab2: {
                        if in_grouping(z, G_V.as_ptr(), 97, 117, 0) != 0 {
                            break 'lab2;
                        }
                        {
                            let c3 = (*z).c;
                            'lab3: {
                                'lab4: {
                                    if out_grouping(z, G_V.as_ptr(), 97, 117, 0) != 0 {
                                        break 'lab4;
                                    }
                                    {
                                        let ret = out_grouping(z, G_V.as_ptr(), 97, 117, 1);
                                        if ret < 0 {
                                            break 'lab4;
                                        }
                                        (*z).c += ret;
                                    }
                                    break 'lab3;
                                }
                                // lab4:
                                (*z).c = c3;
                                if in_grouping(z, G_V.as_ptr(), 97, 117, 0) != 0 {
                                    break 'lab2;
                                }
                                {
                                    let ret = in_grouping(z, G_V.as_ptr(), 97, 117, 1);
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
                    if out_grouping(z, G_V.as_ptr(), 97, 117, 0) != 0 {
                        break 'lab0;
                    }
                    {
                        let c4 = (*z).c;
                        'lab5: {
                            'lab6: {
                                if out_grouping(z, G_V.as_ptr(), 97, 117, 0) != 0 {
                                    break 'lab6;
                                }
                                {
                                    let ret = out_grouping(z, G_V.as_ptr(), 97, 117, 1);
                                    if ret < 0 {
                                        break 'lab6;
                                    }
                                    (*z).c += ret;
                                }
                                break 'lab5;
                            }
                            // lab6:
                            (*z).c = c4;
                            if in_grouping(z, G_V.as_ptr(), 97, 117, 0) != 0 {
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
        }
        // lab0:
        (*z).c = c1;
    }
    {
        let c5 = (*z).c;
        'lab7: {
            {
                let ret = out_grouping(z, G_V.as_ptr(), 97, 117, 1);
                if ret < 0 {
                    break 'lab7;
                }
                (*z).c += ret;
            }
            {
                let ret = in_grouping(z, G_V.as_ptr(), 97, 117, 1);
                if ret < 0 {
                    break 'lab7;
                }
                (*z).c += ret;
            }
            *(*z).I.offset(1) = (*z).c;
            {
                let ret = out_grouping(z, G_V.as_ptr(), 97, 117, 1);
                if ret < 0 {
                    break 'lab7;
                }
                (*z).c += ret;
            }
            {
                let ret = in_grouping(z, G_V.as_ptr(), 97, 117, 1);
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

unsafe extern "C" fn r_RV(z: *mut SN_env) -> c_int {
    (*(*z).I.offset(2) <= (*z).c) as c_int
}

unsafe extern "C" fn r_R2(z: *mut SN_env) -> c_int {
    (*(*z).I.offset(0) <= (*z).c) as c_int
}

unsafe extern "C" fn r_R1(z: *mut SN_env) -> c_int {
    (*(*z).I.offset(1) <= (*z).c) as c_int
}

unsafe extern "C" fn r_aditzak(z: *mut SN_env) -> c_int {
    let among_var;
    (*z).ket = (*z).c;
    if (*z).c - 1 <= (*z).lb
        || (*(*z).p.offset(((*z).c - 1) as isize) as c_int >> 5) != 3
        || ((70566434 >> (*(*z).p.offset(((*z).c - 1) as isize) as c_int & 0x1f)) & 1) == 0
    {
        return 0;
    }
    among_var = find_among_b(z, A_0.as_ptr(), 109);
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
            let ret = slice_from_s(z, 7, S_0.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        4 => {
            let ret = slice_from_s(z, 7, S_1.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        5 => {
            let ret = slice_from_s(z, 6, S_2.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        _ => {}
    }
    1
}

unsafe extern "C" fn r_izenak(z: *mut SN_env) -> c_int {
    let among_var;
    (*z).ket = (*z).c;
    if (*z).c <= (*z).lb
        || (*(*z).p.offset(((*z).c - 1) as isize) as c_int >> 5) != 3
        || ((71162402 >> (*(*z).p.offset(((*z).c - 1) as isize) as c_int & 0x1f)) & 1) == 0
    {
        return 0;
    }
    among_var = find_among_b(z, A_1.as_ptr(), 295);
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
            let ret = slice_from_s(z, 3, S_3.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        4 => {
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
        5 => {
            let ret = slice_from_s(z, 3, S_4.as_ptr());
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
            let ret = slice_from_s(z, 5, S_6.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        8 => {
            let ret = slice_from_s(z, 5, S_7.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        9 => {
            let ret = slice_from_s(z, 5, S_8.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        10 => {
            let ret = slice_from_s(z, 5, S_9.as_ptr());
            if ret < 0 {
                return ret;
            }
        }
        _ => {}
    }
    1
}

unsafe extern "C" fn r_adjetiboak(z: *mut SN_env) -> c_int {
    let among_var;
    (*z).ket = (*z).c;
    if (*z).c - 1 <= (*z).lb
        || (*(*z).p.offset(((*z).c - 1) as isize) as c_int >> 5) != 3
        || ((35362 >> (*(*z).p.offset(((*z).c - 1) as isize) as c_int & 0x1f)) & 1) == 0
    {
        return 0;
    }
    among_var = find_among_b(z, A_2.as_ptr(), 19);
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
pub unsafe extern "C" fn basque_ISO_8859_1_stem(z: *mut SN_env) -> c_int {
    {
        let ret = r_mark_regions(z);
        if ret < 0 {
            return ret;
        }
    }
    (*z).lb = (*z).c;
    (*z).c = (*z).l;

    'loop0: loop {
        let m1 = (*z).l - (*z).c;
        let _ = m1;
        'lab0: {
            {
                let ret = r_aditzak(z);
                if ret == 0 {
                    break 'lab0;
                }
                if ret < 0 {
                    return ret;
                }
            }
            continue 'loop0;
        }
        // lab0:
        (*z).c = (*z).l - m1;
        break 'loop0;
    }
    'loop1: loop {
        let m2 = (*z).l - (*z).c;
        let _ = m2;
        'lab1: {
            {
                let ret = r_izenak(z);
                if ret == 0 {
                    break 'lab1;
                }
                if ret < 0 {
                    return ret;
                }
            }
            continue 'loop1;
        }
        // lab1:
        (*z).c = (*z).l - m2;
        break 'loop1;
    }
    {
        let m3 = (*z).l - (*z).c;
        let _ = m3;
        {
            let ret = r_adjetiboak(z);
            if ret < 0 {
                return ret;
            }
        }
        (*z).c = (*z).l - m3;
    }
    (*z).c = (*z).lb;
    1
}

#[no_mangle]
pub unsafe extern "C" fn basque_ISO_8859_1_create_env() -> *mut SN_env {
    SN_create_env(0, 3)
}

#[no_mangle]
pub unsafe extern "C" fn basque_ISO_8859_1_close_env(z: *mut SN_env) {
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
        let z = basque_ISO_8859_1_create_env();
        assert!(!z.is_null());
        let rc = SN_set_current(z, word.len() as c_int, word.as_ptr());
        assert!(rc >= 0);
        let rc = basque_ISO_8859_1_stem(z);
        assert!(rc >= 0, "stem returned {rc}");
        let out = std::slice::from_raw_parts((*z).p, SIZE((*z).p) as usize).to_vec();
        basque_ISO_8859_1_close_env(z);
        out
    }

    // A short root with no strippable suffix is returned unchanged.
    #[test]
    fn short_root_unchanged() {
        unsafe {
            assert_eq!(stem(b"bil"), b"bil".to_vec());
        }
    }

    // Idempotence: stemming a stem yields the same stem, and stems stay
    // non-empty.
    #[test]
    fn idempotent() {
        unsafe {
            for w in [&b"etxea"[..], &b"mendia"[..], &b"liburua"[..], &b"katuari"[..]] {
                let once = stem(w);
                let twice = stem(&once);
                assert_eq!(once, twice, "not idempotent for {:?}", w);
                assert!(!once.is_empty());
            }
        }
    }

    // Suffix stripping shrinks (never grows) the word and yields a non-empty
    // stem.
    #[test]
    fn suffix_stripped_nonempty() {
        unsafe {
            let r = stem(b"etxeak");
            assert!(!r.is_empty());
            assert!(r.len() <= "etxeak".len());
        }
    }
}
