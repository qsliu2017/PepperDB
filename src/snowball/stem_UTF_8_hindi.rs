//! Hindi Snowball stemmer (UTF-8).
//!
//! 1:1 translation of the generated
//! `src/backend/snowball/libstemmer/stem_UTF_8_hindi.c` (Snowball 2.2.0),
//! merged with its header `stem_UTF_8_hindi.h`. The runtime helpers come from
//! `crate::snowball::api` and `crate::snowball::utilities`.

use crate::prelude::*;

use crate::snowball::api::{among, symbol, SN_close_env, SN_create_env, SN_env};
use crate::snowball::utilities::{find_among_b, in_grouping_b_U, skip_utf8, slice_del};

// ---------------------------------------------------------------------------
// among string tables
// ---------------------------------------------------------------------------

static S_0_0: [symbol; 3] = [0xE0, 0xA5, 0x80];
static S_0_1: [symbol; 12] = [0xE0, 0xA5, 0x82, 0xE0, 0xA4, 0x82, 0xE0, 0xA4, 0x97, 0xE0, 0xA5, 0x80];
static S_0_2: [symbol; 12] = [0xE0, 0xA5, 0x87, 0xE0, 0xA4, 0x82, 0xE0, 0xA4, 0x97, 0xE0, 0xA5, 0x80];
static S_0_3: [symbol; 12] = [0xE0, 0xA4, 0x8A, 0xE0, 0xA4, 0x82, 0xE0, 0xA4, 0x97, 0xE0, 0xA5, 0x80];
static S_0_4: [symbol; 15] = [0xE0, 0xA4, 0x86, 0xE0, 0xA4, 0x8A, 0xE0, 0xA4, 0x82, 0xE0, 0xA4, 0x97, 0xE0, 0xA5, 0x80];
static S_0_5: [symbol; 15] = [0xE0, 0xA4, 0xBE, 0xE0, 0xA4, 0x8A, 0xE0, 0xA4, 0x82, 0xE0, 0xA4, 0x97, 0xE0, 0xA5, 0x80];
static S_0_6: [symbol; 12] = [0xE0, 0xA4, 0x8F, 0xE0, 0xA4, 0x82, 0xE0, 0xA4, 0x97, 0xE0, 0xA5, 0x80];
static S_0_7: [symbol; 15] = [0xE0, 0xA4, 0x86, 0xE0, 0xA4, 0x8F, 0xE0, 0xA4, 0x82, 0xE0, 0xA4, 0x97, 0xE0, 0xA5, 0x80];
static S_0_8: [symbol; 15] = [0xE0, 0xA4, 0xBE, 0xE0, 0xA4, 0x8F, 0xE0, 0xA4, 0x82, 0xE0, 0xA4, 0x97, 0xE0, 0xA5, 0x80];
static S_0_9: [symbol; 9] = [0xE0, 0xA5, 0x87, 0xE0, 0xA4, 0x97, 0xE0, 0xA5, 0x80];
static S_0_10: [symbol; 9] = [0xE0, 0xA5, 0x8B, 0xE0, 0xA4, 0x97, 0xE0, 0xA5, 0x80];
static S_0_11: [symbol; 9] = [0xE0, 0xA4, 0x8F, 0xE0, 0xA4, 0x97, 0xE0, 0xA5, 0x80];
static S_0_12: [symbol; 12] = [0xE0, 0xA4, 0x86, 0xE0, 0xA4, 0x8F, 0xE0, 0xA4, 0x97, 0xE0, 0xA5, 0x80];
static S_0_13: [symbol; 12] = [0xE0, 0xA4, 0xBE, 0xE0, 0xA4, 0x8F, 0xE0, 0xA4, 0x97, 0xE0, 0xA5, 0x80];
static S_0_14: [symbol; 9] = [0xE0, 0xA4, 0x93, 0xE0, 0xA4, 0x97, 0xE0, 0xA5, 0x80];
static S_0_15: [symbol; 12] = [0xE0, 0xA4, 0x86, 0xE0, 0xA4, 0x93, 0xE0, 0xA4, 0x97, 0xE0, 0xA5, 0x80];
static S_0_16: [symbol; 12] = [0xE0, 0xA4, 0xBE, 0xE0, 0xA4, 0x93, 0xE0, 0xA4, 0x97, 0xE0, 0xA5, 0x80];
static S_0_17: [symbol; 6] = [0xE0, 0xA4, 0xA4, 0xE0, 0xA5, 0x80];
static S_0_18: [symbol; 9] = [0xE0, 0xA4, 0x85, 0xE0, 0xA4, 0xA4, 0xE0, 0xA5, 0x80];
static S_0_19: [symbol; 9] = [0xE0, 0xA4, 0x86, 0xE0, 0xA4, 0xA4, 0xE0, 0xA5, 0x80];
static S_0_20: [symbol; 9] = [0xE0, 0xA4, 0xBE, 0xE0, 0xA4, 0xA4, 0xE0, 0xA5, 0x80];
static S_0_21: [symbol; 6] = [0xE0, 0xA4, 0xA8, 0xE0, 0xA5, 0x80];
static S_0_22: [symbol; 9] = [0xE0, 0xA4, 0x85, 0xE0, 0xA4, 0xA8, 0xE0, 0xA5, 0x80];
static S_0_23: [symbol; 6] = [0xE0, 0xA4, 0x86, 0xE0, 0xA4, 0x81];
static S_0_24: [symbol; 6] = [0xE0, 0xA4, 0xBE, 0xE0, 0xA4, 0x81];
static S_0_25: [symbol; 12] = [0xE0, 0xA4, 0x87, 0xE0, 0xA4, 0xAF, 0xE0, 0xA4, 0xBE, 0xE0, 0xA4, 0x81];
static S_0_26: [symbol; 15] = [0xE0, 0xA4, 0x86, 0xE0, 0xA4, 0x87, 0xE0, 0xA4, 0xAF, 0xE0, 0xA4, 0xBE, 0xE0, 0xA4, 0x81];
static S_0_27: [symbol; 15] = [0xE0, 0xA4, 0xBE, 0xE0, 0xA4, 0x87, 0xE0, 0xA4, 0xAF, 0xE0, 0xA4, 0xBE, 0xE0, 0xA4, 0x81];
static S_0_28: [symbol; 12] = [0xE0, 0xA4, 0xBF, 0xE0, 0xA4, 0xAF, 0xE0, 0xA4, 0xBE, 0xE0, 0xA4, 0x81];
static S_0_29: [symbol; 3] = [0xE0, 0xA5, 0x81];
static S_0_30: [symbol; 6] = [0xE0, 0xA5, 0x80, 0xE0, 0xA4, 0x82];
static S_0_31: [symbol; 9] = [0xE0, 0xA4, 0xA4, 0xE0, 0xA5, 0x80, 0xE0, 0xA4, 0x82];
static S_0_32: [symbol; 12] = [0xE0, 0xA4, 0x85, 0xE0, 0xA4, 0xA4, 0xE0, 0xA5, 0x80, 0xE0, 0xA4, 0x82];
static S_0_33: [symbol; 12] = [0xE0, 0xA4, 0x86, 0xE0, 0xA4, 0xA4, 0xE0, 0xA5, 0x80, 0xE0, 0xA4, 0x82];
static S_0_34: [symbol; 12] = [0xE0, 0xA4, 0xBE, 0xE0, 0xA4, 0xA4, 0xE0, 0xA5, 0x80, 0xE0, 0xA4, 0x82];
static S_0_35: [symbol; 6] = [0xE0, 0xA4, 0x86, 0xE0, 0xA4, 0x82];
static S_0_36: [symbol; 9] = [0xE0, 0xA5, 0x81, 0xE0, 0xA4, 0x86, 0xE0, 0xA4, 0x82];
static S_0_37: [symbol; 9] = [0xE0, 0xA4, 0x89, 0xE0, 0xA4, 0x86, 0xE0, 0xA4, 0x82];
static S_0_38: [symbol; 6] = [0xE0, 0xA5, 0x87, 0xE0, 0xA4, 0x82];
static S_0_39: [symbol; 6] = [0xE0, 0xA4, 0x88, 0xE0, 0xA4, 0x82];
static S_0_40: [symbol; 9] = [0xE0, 0xA4, 0x86, 0xE0, 0xA4, 0x88, 0xE0, 0xA4, 0x82];
static S_0_41: [symbol; 9] = [0xE0, 0xA4, 0xBE, 0xE0, 0xA4, 0x88, 0xE0, 0xA4, 0x82];
static S_0_42: [symbol; 6] = [0xE0, 0xA5, 0x8B, 0xE0, 0xA4, 0x82];
static S_0_43: [symbol; 12] = [0xE0, 0xA4, 0x87, 0xE0, 0xA4, 0xAF, 0xE0, 0xA5, 0x8B, 0xE0, 0xA4, 0x82];
static S_0_44: [symbol; 15] = [0xE0, 0xA4, 0x86, 0xE0, 0xA4, 0x87, 0xE0, 0xA4, 0xAF, 0xE0, 0xA5, 0x8B, 0xE0, 0xA4, 0x82];
static S_0_45: [symbol; 15] = [0xE0, 0xA4, 0xBE, 0xE0, 0xA4, 0x87, 0xE0, 0xA4, 0xAF, 0xE0, 0xA5, 0x8B, 0xE0, 0xA4, 0x82];
static S_0_46: [symbol; 12] = [0xE0, 0xA4, 0xBF, 0xE0, 0xA4, 0xAF, 0xE0, 0xA5, 0x8B, 0xE0, 0xA4, 0x82];
static S_0_47: [symbol; 6] = [0xE0, 0xA4, 0x8F, 0xE0, 0xA4, 0x82];
static S_0_48: [symbol; 9] = [0xE0, 0xA5, 0x81, 0xE0, 0xA4, 0x8F, 0xE0, 0xA4, 0x82];
static S_0_49: [symbol; 9] = [0xE0, 0xA4, 0x86, 0xE0, 0xA4, 0x8F, 0xE0, 0xA4, 0x82];
static S_0_50: [symbol; 9] = [0xE0, 0xA4, 0x89, 0xE0, 0xA4, 0x8F, 0xE0, 0xA4, 0x82];
static S_0_51: [symbol; 9] = [0xE0, 0xA4, 0xBE, 0xE0, 0xA4, 0x8F, 0xE0, 0xA4, 0x82];
static S_0_52: [symbol; 12] = [0xE0, 0xA4, 0xA4, 0xE0, 0xA4, 0xBE, 0xE0, 0xA4, 0x8F, 0xE0, 0xA4, 0x82];
static S_0_53: [symbol; 15] = [0xE0, 0xA4, 0x85, 0xE0, 0xA4, 0xA4, 0xE0, 0xA4, 0xBE, 0xE0, 0xA4, 0x8F, 0xE0, 0xA4, 0x82];
static S_0_54: [symbol; 12] = [0xE0, 0xA4, 0xA8, 0xE0, 0xA4, 0xBE, 0xE0, 0xA4, 0x8F, 0xE0, 0xA4, 0x82];
static S_0_55: [symbol; 15] = [0xE0, 0xA4, 0x85, 0xE0, 0xA4, 0xA8, 0xE0, 0xA4, 0xBE, 0xE0, 0xA4, 0x8F, 0xE0, 0xA4, 0x82];
static S_0_56: [symbol; 6] = [0xE0, 0xA4, 0x93, 0xE0, 0xA4, 0x82];
static S_0_57: [symbol; 9] = [0xE0, 0xA5, 0x81, 0xE0, 0xA4, 0x93, 0xE0, 0xA4, 0x82];
static S_0_58: [symbol; 9] = [0xE0, 0xA4, 0x86, 0xE0, 0xA4, 0x93, 0xE0, 0xA4, 0x82];
static S_0_59: [symbol; 9] = [0xE0, 0xA4, 0x89, 0xE0, 0xA4, 0x93, 0xE0, 0xA4, 0x82];
static S_0_60: [symbol; 9] = [0xE0, 0xA4, 0xBE, 0xE0, 0xA4, 0x93, 0xE0, 0xA4, 0x82];
static S_0_61: [symbol; 12] = [0xE0, 0xA4, 0xA4, 0xE0, 0xA4, 0xBE, 0xE0, 0xA4, 0x93, 0xE0, 0xA4, 0x82];
static S_0_62: [symbol; 15] = [0xE0, 0xA4, 0x85, 0xE0, 0xA4, 0xA4, 0xE0, 0xA4, 0xBE, 0xE0, 0xA4, 0x93, 0xE0, 0xA4, 0x82];
static S_0_63: [symbol; 12] = [0xE0, 0xA4, 0xA8, 0xE0, 0xA4, 0xBE, 0xE0, 0xA4, 0x93, 0xE0, 0xA4, 0x82];
static S_0_64: [symbol; 15] = [0xE0, 0xA4, 0x85, 0xE0, 0xA4, 0xA8, 0xE0, 0xA4, 0xBE, 0xE0, 0xA4, 0x93, 0xE0, 0xA4, 0x82];
static S_0_65: [symbol; 6] = [0xE0, 0xA4, 0xBE, 0xE0, 0xA4, 0x82];
static S_0_66: [symbol; 12] = [0xE0, 0xA4, 0x87, 0xE0, 0xA4, 0xAF, 0xE0, 0xA4, 0xBE, 0xE0, 0xA4, 0x82];
static S_0_67: [symbol; 15] = [0xE0, 0xA4, 0x86, 0xE0, 0xA4, 0x87, 0xE0, 0xA4, 0xAF, 0xE0, 0xA4, 0xBE, 0xE0, 0xA4, 0x82];
static S_0_68: [symbol; 15] = [0xE0, 0xA4, 0xBE, 0xE0, 0xA4, 0x87, 0xE0, 0xA4, 0xAF, 0xE0, 0xA4, 0xBE, 0xE0, 0xA4, 0x82];
static S_0_69: [symbol; 12] = [0xE0, 0xA4, 0xBF, 0xE0, 0xA4, 0xAF, 0xE0, 0xA4, 0xBE, 0xE0, 0xA4, 0x82];
static S_0_70: [symbol; 3] = [0xE0, 0xA5, 0x82];
static S_0_71: [symbol; 3] = [0xE0, 0xA4, 0x85];
static S_0_72: [symbol; 3] = [0xE0, 0xA4, 0x86];
static S_0_73: [symbol; 3] = [0xE0, 0xA4, 0x87];
static S_0_74: [symbol; 3] = [0xE0, 0xA5, 0x87];
static S_0_75: [symbol; 12] = [0xE0, 0xA5, 0x87, 0xE0, 0xA4, 0x82, 0xE0, 0xA4, 0x97, 0xE0, 0xA5, 0x87];
static S_0_76: [symbol; 12] = [0xE0, 0xA4, 0x8F, 0xE0, 0xA4, 0x82, 0xE0, 0xA4, 0x97, 0xE0, 0xA5, 0x87];
static S_0_77: [symbol; 15] = [0xE0, 0xA4, 0x86, 0xE0, 0xA4, 0x8F, 0xE0, 0xA4, 0x82, 0xE0, 0xA4, 0x97, 0xE0, 0xA5, 0x87];
static S_0_78: [symbol; 15] = [0xE0, 0xA4, 0xBE, 0xE0, 0xA4, 0x8F, 0xE0, 0xA4, 0x82, 0xE0, 0xA4, 0x97, 0xE0, 0xA5, 0x87];
static S_0_79: [symbol; 9] = [0xE0, 0xA5, 0x8B, 0xE0, 0xA4, 0x97, 0xE0, 0xA5, 0x87];
static S_0_80: [symbol; 9] = [0xE0, 0xA4, 0x93, 0xE0, 0xA4, 0x97, 0xE0, 0xA5, 0x87];
static S_0_81: [symbol; 12] = [0xE0, 0xA4, 0x86, 0xE0, 0xA4, 0x93, 0xE0, 0xA4, 0x97, 0xE0, 0xA5, 0x87];
static S_0_82: [symbol; 12] = [0xE0, 0xA4, 0xBE, 0xE0, 0xA4, 0x93, 0xE0, 0xA4, 0x97, 0xE0, 0xA5, 0x87];
static S_0_83: [symbol; 6] = [0xE0, 0xA4, 0xA4, 0xE0, 0xA5, 0x87];
static S_0_84: [symbol; 9] = [0xE0, 0xA4, 0x85, 0xE0, 0xA4, 0xA4, 0xE0, 0xA5, 0x87];
static S_0_85: [symbol; 9] = [0xE0, 0xA4, 0x86, 0xE0, 0xA4, 0xA4, 0xE0, 0xA5, 0x87];
static S_0_86: [symbol; 9] = [0xE0, 0xA4, 0xBE, 0xE0, 0xA4, 0xA4, 0xE0, 0xA5, 0x87];
static S_0_87: [symbol; 6] = [0xE0, 0xA4, 0xA8, 0xE0, 0xA5, 0x87];
static S_0_88: [symbol; 9] = [0xE0, 0xA4, 0x85, 0xE0, 0xA4, 0xA8, 0xE0, 0xA5, 0x87];
static S_0_89: [symbol; 9] = [0xE0, 0xA4, 0x86, 0xE0, 0xA4, 0xA8, 0xE0, 0xA5, 0x87];
static S_0_90: [symbol; 9] = [0xE0, 0xA4, 0xBE, 0xE0, 0xA4, 0xA8, 0xE0, 0xA5, 0x87];
static S_0_91: [symbol; 3] = [0xE0, 0xA4, 0x88];
static S_0_92: [symbol; 6] = [0xE0, 0xA4, 0x86, 0xE0, 0xA4, 0x88];
static S_0_93: [symbol; 6] = [0xE0, 0xA4, 0xBE, 0xE0, 0xA4, 0x88];
static S_0_94: [symbol; 3] = [0xE0, 0xA4, 0x89];
static S_0_95: [symbol; 3] = [0xE0, 0xA4, 0x8A];
static S_0_96: [symbol; 3] = [0xE0, 0xA5, 0x8B];
static S_0_97: [symbol; 3] = [0xE0, 0xA5, 0x8D];
static S_0_98: [symbol; 3] = [0xE0, 0xA4, 0x8F];
static S_0_99: [symbol; 6] = [0xE0, 0xA4, 0x86, 0xE0, 0xA4, 0x8F];
static S_0_100: [symbol; 6] = [0xE0, 0xA4, 0x87, 0xE0, 0xA4, 0x8F];
static S_0_101: [symbol; 9] = [0xE0, 0xA4, 0x86, 0xE0, 0xA4, 0x87, 0xE0, 0xA4, 0x8F];
static S_0_102: [symbol; 9] = [0xE0, 0xA4, 0xBE, 0xE0, 0xA4, 0x87, 0xE0, 0xA4, 0x8F];
static S_0_103: [symbol; 6] = [0xE0, 0xA4, 0xBE, 0xE0, 0xA4, 0x8F];
static S_0_104: [symbol; 6] = [0xE0, 0xA4, 0xBF, 0xE0, 0xA4, 0x8F];
static S_0_105: [symbol; 3] = [0xE0, 0xA4, 0x93];
static S_0_106: [symbol; 6] = [0xE0, 0xA4, 0x86, 0xE0, 0xA4, 0x93];
static S_0_107: [symbol; 6] = [0xE0, 0xA4, 0xBE, 0xE0, 0xA4, 0x93];
static S_0_108: [symbol; 6] = [0xE0, 0xA4, 0x95, 0xE0, 0xA4, 0xB0];
static S_0_109: [symbol; 9] = [0xE0, 0xA4, 0x85, 0xE0, 0xA4, 0x95, 0xE0, 0xA4, 0xB0];
static S_0_110: [symbol; 9] = [0xE0, 0xA4, 0x86, 0xE0, 0xA4, 0x95, 0xE0, 0xA4, 0xB0];
static S_0_111: [symbol; 9] = [0xE0, 0xA4, 0xBE, 0xE0, 0xA4, 0x95, 0xE0, 0xA4, 0xB0];
static S_0_112: [symbol; 3] = [0xE0, 0xA4, 0xBE];
static S_0_113: [symbol; 12] = [0xE0, 0xA5, 0x82, 0xE0, 0xA4, 0x82, 0xE0, 0xA4, 0x97, 0xE0, 0xA4, 0xBE];
static S_0_114: [symbol; 12] = [0xE0, 0xA4, 0x8A, 0xE0, 0xA4, 0x82, 0xE0, 0xA4, 0x97, 0xE0, 0xA4, 0xBE];
static S_0_115: [symbol; 15] = [0xE0, 0xA4, 0x86, 0xE0, 0xA4, 0x8A, 0xE0, 0xA4, 0x82, 0xE0, 0xA4, 0x97, 0xE0, 0xA4, 0xBE];
static S_0_116: [symbol; 15] = [0xE0, 0xA4, 0xBE, 0xE0, 0xA4, 0x8A, 0xE0, 0xA4, 0x82, 0xE0, 0xA4, 0x97, 0xE0, 0xA4, 0xBE];
static S_0_117: [symbol; 9] = [0xE0, 0xA5, 0x87, 0xE0, 0xA4, 0x97, 0xE0, 0xA4, 0xBE];
static S_0_118: [symbol; 9] = [0xE0, 0xA4, 0x8F, 0xE0, 0xA4, 0x97, 0xE0, 0xA4, 0xBE];
static S_0_119: [symbol; 12] = [0xE0, 0xA4, 0x86, 0xE0, 0xA4, 0x8F, 0xE0, 0xA4, 0x97, 0xE0, 0xA4, 0xBE];
static S_0_120: [symbol; 12] = [0xE0, 0xA4, 0xBE, 0xE0, 0xA4, 0x8F, 0xE0, 0xA4, 0x97, 0xE0, 0xA4, 0xBE];
static S_0_121: [symbol; 6] = [0xE0, 0xA4, 0xA4, 0xE0, 0xA4, 0xBE];
static S_0_122: [symbol; 9] = [0xE0, 0xA4, 0x85, 0xE0, 0xA4, 0xA4, 0xE0, 0xA4, 0xBE];
static S_0_123: [symbol; 9] = [0xE0, 0xA4, 0x86, 0xE0, 0xA4, 0xA4, 0xE0, 0xA4, 0xBE];
static S_0_124: [symbol; 9] = [0xE0, 0xA4, 0xBE, 0xE0, 0xA4, 0xA4, 0xE0, 0xA4, 0xBE];
static S_0_125: [symbol; 6] = [0xE0, 0xA4, 0xA8, 0xE0, 0xA4, 0xBE];
static S_0_126: [symbol; 9] = [0xE0, 0xA4, 0x85, 0xE0, 0xA4, 0xA8, 0xE0, 0xA4, 0xBE];
static S_0_127: [symbol; 9] = [0xE0, 0xA4, 0x86, 0xE0, 0xA4, 0xA8, 0xE0, 0xA4, 0xBE];
static S_0_128: [symbol; 9] = [0xE0, 0xA4, 0xBE, 0xE0, 0xA4, 0xA8, 0xE0, 0xA4, 0xBE];
static S_0_129: [symbol; 9] = [0xE0, 0xA4, 0x86, 0xE0, 0xA4, 0xAF, 0xE0, 0xA4, 0xBE];
static S_0_130: [symbol; 9] = [0xE0, 0xA4, 0xBE, 0xE0, 0xA4, 0xAF, 0xE0, 0xA4, 0xBE];
static S_0_131: [symbol; 3] = [0xE0, 0xA4, 0xBF];

static A_0: [among; 132] = [
    among { s_size: 3, s: S_0_0.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 12, s: S_0_1.as_ptr(), substring_i: 0, result: -1, function: None },
    among { s_size: 12, s: S_0_2.as_ptr(), substring_i: 0, result: -1, function: None },
    among { s_size: 12, s: S_0_3.as_ptr(), substring_i: 0, result: -1, function: None },
    among { s_size: 15, s: S_0_4.as_ptr(), substring_i: 3, result: -1, function: None },
    among { s_size: 15, s: S_0_5.as_ptr(), substring_i: 3, result: -1, function: None },
    among { s_size: 12, s: S_0_6.as_ptr(), substring_i: 0, result: -1, function: None },
    among { s_size: 15, s: S_0_7.as_ptr(), substring_i: 6, result: -1, function: None },
    among { s_size: 15, s: S_0_8.as_ptr(), substring_i: 6, result: -1, function: None },
    among { s_size: 9, s: S_0_9.as_ptr(), substring_i: 0, result: -1, function: None },
    among { s_size: 9, s: S_0_10.as_ptr(), substring_i: 0, result: -1, function: None },
    among { s_size: 9, s: S_0_11.as_ptr(), substring_i: 0, result: -1, function: None },
    among { s_size: 12, s: S_0_12.as_ptr(), substring_i: 11, result: -1, function: None },
    among { s_size: 12, s: S_0_13.as_ptr(), substring_i: 11, result: -1, function: None },
    among { s_size: 9, s: S_0_14.as_ptr(), substring_i: 0, result: -1, function: None },
    among { s_size: 12, s: S_0_15.as_ptr(), substring_i: 14, result: -1, function: None },
    among { s_size: 12, s: S_0_16.as_ptr(), substring_i: 14, result: -1, function: None },
    among { s_size: 6, s: S_0_17.as_ptr(), substring_i: 0, result: -1, function: Some(r_CONSONANT) },
    among { s_size: 9, s: S_0_18.as_ptr(), substring_i: 17, result: -1, function: None },
    among { s_size: 9, s: S_0_19.as_ptr(), substring_i: 17, result: -1, function: None },
    among { s_size: 9, s: S_0_20.as_ptr(), substring_i: 17, result: -1, function: None },
    among { s_size: 6, s: S_0_21.as_ptr(), substring_i: 0, result: -1, function: Some(r_CONSONANT) },
    among { s_size: 9, s: S_0_22.as_ptr(), substring_i: 21, result: -1, function: None },
    among { s_size: 6, s: S_0_23.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 6, s: S_0_24.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 12, s: S_0_25.as_ptr(), substring_i: 24, result: -1, function: None },
    among { s_size: 15, s: S_0_26.as_ptr(), substring_i: 25, result: -1, function: None },
    among { s_size: 15, s: S_0_27.as_ptr(), substring_i: 25, result: -1, function: None },
    among { s_size: 12, s: S_0_28.as_ptr(), substring_i: 24, result: -1, function: None },
    among { s_size: 3, s: S_0_29.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 6, s: S_0_30.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 9, s: S_0_31.as_ptr(), substring_i: 30, result: -1, function: Some(r_CONSONANT) },
    among { s_size: 12, s: S_0_32.as_ptr(), substring_i: 31, result: -1, function: None },
    among { s_size: 12, s: S_0_33.as_ptr(), substring_i: 31, result: -1, function: None },
    among { s_size: 12, s: S_0_34.as_ptr(), substring_i: 31, result: -1, function: None },
    among { s_size: 6, s: S_0_35.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 9, s: S_0_36.as_ptr(), substring_i: 35, result: -1, function: None },
    among { s_size: 9, s: S_0_37.as_ptr(), substring_i: 35, result: -1, function: None },
    among { s_size: 6, s: S_0_38.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 6, s: S_0_39.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 9, s: S_0_40.as_ptr(), substring_i: 39, result: -1, function: None },
    among { s_size: 9, s: S_0_41.as_ptr(), substring_i: 39, result: -1, function: None },
    among { s_size: 6, s: S_0_42.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 12, s: S_0_43.as_ptr(), substring_i: 42, result: -1, function: None },
    among { s_size: 15, s: S_0_44.as_ptr(), substring_i: 43, result: -1, function: None },
    among { s_size: 15, s: S_0_45.as_ptr(), substring_i: 43, result: -1, function: None },
    among { s_size: 12, s: S_0_46.as_ptr(), substring_i: 42, result: -1, function: None },
    among { s_size: 6, s: S_0_47.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 9, s: S_0_48.as_ptr(), substring_i: 47, result: -1, function: None },
    among { s_size: 9, s: S_0_49.as_ptr(), substring_i: 47, result: -1, function: None },
    among { s_size: 9, s: S_0_50.as_ptr(), substring_i: 47, result: -1, function: None },
    among { s_size: 9, s: S_0_51.as_ptr(), substring_i: 47, result: -1, function: None },
    among { s_size: 12, s: S_0_52.as_ptr(), substring_i: 51, result: -1, function: Some(r_CONSONANT) },
    among { s_size: 15, s: S_0_53.as_ptr(), substring_i: 52, result: -1, function: None },
    among { s_size: 12, s: S_0_54.as_ptr(), substring_i: 51, result: -1, function: Some(r_CONSONANT) },
    among { s_size: 15, s: S_0_55.as_ptr(), substring_i: 54, result: -1, function: None },
    among { s_size: 6, s: S_0_56.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 9, s: S_0_57.as_ptr(), substring_i: 56, result: -1, function: None },
    among { s_size: 9, s: S_0_58.as_ptr(), substring_i: 56, result: -1, function: None },
    among { s_size: 9, s: S_0_59.as_ptr(), substring_i: 56, result: -1, function: None },
    among { s_size: 9, s: S_0_60.as_ptr(), substring_i: 56, result: -1, function: None },
    among { s_size: 12, s: S_0_61.as_ptr(), substring_i: 60, result: -1, function: Some(r_CONSONANT) },
    among { s_size: 15, s: S_0_62.as_ptr(), substring_i: 61, result: -1, function: None },
    among { s_size: 12, s: S_0_63.as_ptr(), substring_i: 60, result: -1, function: Some(r_CONSONANT) },
    among { s_size: 15, s: S_0_64.as_ptr(), substring_i: 63, result: -1, function: None },
    among { s_size: 6, s: S_0_65.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 12, s: S_0_66.as_ptr(), substring_i: 65, result: -1, function: None },
    among { s_size: 15, s: S_0_67.as_ptr(), substring_i: 66, result: -1, function: None },
    among { s_size: 15, s: S_0_68.as_ptr(), substring_i: 66, result: -1, function: None },
    among { s_size: 12, s: S_0_69.as_ptr(), substring_i: 65, result: -1, function: None },
    among { s_size: 3, s: S_0_70.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_0_71.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_0_72.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_0_73.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_0_74.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 12, s: S_0_75.as_ptr(), substring_i: 74, result: -1, function: None },
    among { s_size: 12, s: S_0_76.as_ptr(), substring_i: 74, result: -1, function: None },
    among { s_size: 15, s: S_0_77.as_ptr(), substring_i: 76, result: -1, function: None },
    among { s_size: 15, s: S_0_78.as_ptr(), substring_i: 76, result: -1, function: None },
    among { s_size: 9, s: S_0_79.as_ptr(), substring_i: 74, result: -1, function: None },
    among { s_size: 9, s: S_0_80.as_ptr(), substring_i: 74, result: -1, function: None },
    among { s_size: 12, s: S_0_81.as_ptr(), substring_i: 80, result: -1, function: None },
    among { s_size: 12, s: S_0_82.as_ptr(), substring_i: 80, result: -1, function: None },
    among { s_size: 6, s: S_0_83.as_ptr(), substring_i: 74, result: -1, function: Some(r_CONSONANT) },
    among { s_size: 9, s: S_0_84.as_ptr(), substring_i: 83, result: -1, function: None },
    among { s_size: 9, s: S_0_85.as_ptr(), substring_i: 83, result: -1, function: None },
    among { s_size: 9, s: S_0_86.as_ptr(), substring_i: 83, result: -1, function: None },
    among { s_size: 6, s: S_0_87.as_ptr(), substring_i: 74, result: -1, function: Some(r_CONSONANT) },
    among { s_size: 9, s: S_0_88.as_ptr(), substring_i: 87, result: -1, function: None },
    among { s_size: 9, s: S_0_89.as_ptr(), substring_i: 87, result: -1, function: None },
    among { s_size: 9, s: S_0_90.as_ptr(), substring_i: 87, result: -1, function: None },
    among { s_size: 3, s: S_0_91.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 6, s: S_0_92.as_ptr(), substring_i: 91, result: -1, function: None },
    among { s_size: 6, s: S_0_93.as_ptr(), substring_i: 91, result: -1, function: None },
    among { s_size: 3, s: S_0_94.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_0_95.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_0_96.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_0_97.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 3, s: S_0_98.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 6, s: S_0_99.as_ptr(), substring_i: 98, result: -1, function: None },
    among { s_size: 6, s: S_0_100.as_ptr(), substring_i: 98, result: -1, function: None },
    among { s_size: 9, s: S_0_101.as_ptr(), substring_i: 100, result: -1, function: None },
    among { s_size: 9, s: S_0_102.as_ptr(), substring_i: 100, result: -1, function: None },
    among { s_size: 6, s: S_0_103.as_ptr(), substring_i: 98, result: -1, function: None },
    among { s_size: 6, s: S_0_104.as_ptr(), substring_i: 98, result: -1, function: None },
    among { s_size: 3, s: S_0_105.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 6, s: S_0_106.as_ptr(), substring_i: 105, result: -1, function: None },
    among { s_size: 6, s: S_0_107.as_ptr(), substring_i: 105, result: -1, function: None },
    among { s_size: 6, s: S_0_108.as_ptr(), substring_i: -1, result: -1, function: Some(r_CONSONANT) },
    among { s_size: 9, s: S_0_109.as_ptr(), substring_i: 108, result: -1, function: None },
    among { s_size: 9, s: S_0_110.as_ptr(), substring_i: 108, result: -1, function: None },
    among { s_size: 9, s: S_0_111.as_ptr(), substring_i: 108, result: -1, function: None },
    among { s_size: 3, s: S_0_112.as_ptr(), substring_i: -1, result: -1, function: None },
    among { s_size: 12, s: S_0_113.as_ptr(), substring_i: 112, result: -1, function: None },
    among { s_size: 12, s: S_0_114.as_ptr(), substring_i: 112, result: -1, function: None },
    among { s_size: 15, s: S_0_115.as_ptr(), substring_i: 114, result: -1, function: None },
    among { s_size: 15, s: S_0_116.as_ptr(), substring_i: 114, result: -1, function: None },
    among { s_size: 9, s: S_0_117.as_ptr(), substring_i: 112, result: -1, function: None },
    among { s_size: 9, s: S_0_118.as_ptr(), substring_i: 112, result: -1, function: None },
    among { s_size: 12, s: S_0_119.as_ptr(), substring_i: 118, result: -1, function: None },
    among { s_size: 12, s: S_0_120.as_ptr(), substring_i: 118, result: -1, function: None },
    among { s_size: 6, s: S_0_121.as_ptr(), substring_i: 112, result: -1, function: Some(r_CONSONANT) },
    among { s_size: 9, s: S_0_122.as_ptr(), substring_i: 121, result: -1, function: None },
    among { s_size: 9, s: S_0_123.as_ptr(), substring_i: 121, result: -1, function: None },
    among { s_size: 9, s: S_0_124.as_ptr(), substring_i: 121, result: -1, function: None },
    among { s_size: 6, s: S_0_125.as_ptr(), substring_i: 112, result: -1, function: Some(r_CONSONANT) },
    among { s_size: 9, s: S_0_126.as_ptr(), substring_i: 125, result: -1, function: None },
    among { s_size: 9, s: S_0_127.as_ptr(), substring_i: 125, result: -1, function: None },
    among { s_size: 9, s: S_0_128.as_ptr(), substring_i: 125, result: -1, function: None },
    among { s_size: 9, s: S_0_129.as_ptr(), substring_i: 112, result: -1, function: None },
    among { s_size: 9, s: S_0_130.as_ptr(), substring_i: 112, result: -1, function: None },
    among { s_size: 3, s: S_0_131.as_ptr(), substring_i: -1, result: -1, function: None },
];

// g_consonant grouping table.
static G_consonant: [c_uchar; 10] = [255, 255, 255, 255, 159, 0, 0, 0, 248, 7];

// static int r_CONSONANT(struct SN_env * z)
unsafe extern "C" fn r_CONSONANT(z: *mut SN_env) -> c_int {
    if in_grouping_b_U(z, G_consonant.as_ptr(), 2325, 2399, 0) != 0 {
        return 0;
    }
    1
}

// extern int hindi_UTF_8_stem(struct SN_env * z)
#[no_mangle]
pub unsafe extern "C" fn hindi_UTF_8_stem(z: *mut SN_env) -> c_int {
    {
        let ret = skip_utf8((*z).p, (*z).c, (*z).l, 1);
        if ret < 0 {
            return 0;
        }
        (*z).c = ret;
    }
    (*z).lb = (*z).c;
    (*z).c = (*z).l;

    (*z).ket = (*z).c;
    if find_among_b(z, A_0.as_ptr(), 132) == 0 {
        return 0;
    }
    (*z).bra = (*z).c;
    {
        let ret = slice_del(z);
        if ret < 0 {
            return ret;
        }
    }
    (*z).c = (*z).lb;
    1
}

// extern struct SN_env * hindi_UTF_8_create_env(void)
#[no_mangle]
pub unsafe extern "C" fn hindi_UTF_8_create_env() -> *mut SN_env {
    SN_create_env(0, 0)
}

// extern void hindi_UTF_8_close_env(struct SN_env * z)
#[no_mangle]
pub unsafe extern "C" fn hindi_UTF_8_close_env(z: *mut SN_env) {
    SN_close_env(z, 0);
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snowball::api::{SN_set_current, SIZE};

    unsafe fn stem_word(word: &[u8]) -> Vec<u8> {
        let z = hindi_UTF_8_create_env();
        SN_set_current(z, word.len() as c_int, word.as_ptr());
        let rc = hindi_UTF_8_stem(z);
        assert!(rc >= 0);
        let out = core::slice::from_raw_parts((*z).p, SIZE((*z).p) as usize).to_vec();
        hindi_UTF_8_close_env(z);
        out
    }

    // A single Devanagari syllable (one char past the initial-skip) has no
    // suffix to strip, so it comes back unchanged.
    #[test]
    fn short_word_unchanged() {
        unsafe {
            // U+0915 KA (0xE0 0xA4 0x95)
            let w: &[u8] = &[0xE0, 0xA4, 0x95];
            let out = stem_word(w);
            assert_eq!(out, w.to_vec());
            assert!(!out.is_empty());
        }
    }

    // Stemming is idempotent: stem(stem(w)) == stem(w).
    #[test]
    fn idempotent() {
        unsafe {
            // "ladakiyan" -> लड़कियाँ style word with an -iyan suffix.
            // U+0932 U+0921 U+093C U+0915 U+093F U+092F U+093E U+0901
            let w: &[u8] = &[
                0xE0, 0xA4, 0xB2, 0xE0, 0xA4, 0xA1, 0xE0, 0xA4, 0xBC, 0xE0, 0xA4, 0x95,
                0xE0, 0xA4, 0xBF, 0xE0, 0xA4, 0xAF, 0xE0, 0xA4, 0xBE, 0xE0, 0xA4, 0x81,
            ];
            let once = stem_word(w);
            assert!(!once.is_empty());
            let twice = stem_word(&once);
            assert_eq!(once, twice);
        }
    }
}
