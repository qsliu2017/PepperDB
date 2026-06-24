//! Translated from PostgreSQL src/include/tsearch/dicts/spell.h
//! Declarations for ISpell dictionary.

use bitflags::bitflags;

use crate::regex::regex::pg_regex_t;
use crate::tsearch::dicts::regis::Regis;
use crate::tsearch::ts_public::TSLexeme;

// All spell structs are in-memory dictionary-build state, not on-disk.

bitflags! {
    /// `FF_*` affix flags. Names correlate with Hunspell affix-file options.
    /// Composite `FF_COMPOUNDFLAG` per the bitflags appendix.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct FfFlags: u32 {
        const COMPOUNDONLY      = 0x01;
        const COMPOUNDBEGIN     = 0x02;
        const COMPOUNDMIDDLE    = 0x04;
        const COMPOUNDLAST      = 0x08;
        const COMPOUNDFLAG      =
            Self::COMPOUNDBEGIN.bits() | Self::COMPOUNDMIDDLE.bits() | Self::COMPOUNDLAST.bits();
        const COMPOUNDFLAGMASK  = 0x0f;
        const COMPOUNDPERMITFLAG = 0x10;
        const COMPOUNDFORBIDFLAG = 0x20;
        const CROSSPRODUCT       = 0x40;
    }
}

/// `FF_SUFFIX` / `FF_PREFIX` - a 0/1 affix-type ordinal (not a flag set).
/// Don't change the order: initialization sorts by these, expecting prefixes first.
pub const FF_PREFIX: u32 = 0;
pub const FF_SUFFIX: u32 = 1;

/// `SPNodeData` - one slot of an SPNode. C bitfields `val:8, isword:1,
/// compoundflag:4, affix:19` packed into a u32.
pub struct SPNodeData {
    bits: u32, // val:8 | isword:1 | compoundflag:4 | affix:19 (low-to-high)
    pub node: Option<Box<SPNode>>,
}

impl SPNodeData {
    pub fn val(&self) -> u8 {
        (self.bits & 0xff) as u8
    }
    pub fn set_val(&mut self, v: u8) {
        self.bits = (self.bits & !0xff) | v as u32;
    }
    pub fn isword(&self) -> bool {
        (self.bits >> 8) & 0x1 != 0
    }
    pub fn set_isword(&mut self, v: bool) {
        self.bits = (self.bits & !(0x1 << 8)) | ((v as u32) << 8);
    }
    pub fn compoundflag(&self) -> u32 {
        (self.bits >> 9) & 0xf
    }
    pub fn set_compoundflag(&mut self, v: u32) {
        self.bits = (self.bits & !(0xf << 9)) | ((v & 0xf) << 9);
    }
    pub fn affix(&self) -> u32 {
        (self.bits >> 13) & 0x7ffff
    }
    pub fn set_affix(&mut self, v: u32) {
        self.bits = (self.bits & !(0x7ffff << 13)) | ((v & 0x7ffff) << 13);
    }
}

/// `SPNode` - prefix tree (Trie) node storing a words list.
pub struct SPNode {
    pub data: Vec<SPNodeData>, // length = data.len()
}

/// `SPELL` - an entry in a words list.
pub struct SPELL {
    pub p: SpellPayload,
    pub word: String,
}

/// SPELL union: a raw flag string (before sort), or {affix, len} after sort.
pub enum SpellPayload {
    Flag(String),
    Sorted { affix: i32, len: i32 },
}

/// `AFFIX` - an entry in an affix list. C bitfields `type:1, flagflags:7,
/// issimple:1, isregis:1, replen:14` packed into a u32.
pub struct AFFIX {
    pub flag: String,
    bits: u32, // type:1 | flagflags:7 | issimple:1 | isregis:1 | replen:14
    pub find: String,
    pub repl: String,
    pub reg: AffixReg,
}

/// AFFIX union: a compiled regex, or a Regis expression.
pub enum AffixReg {
    Pregex(Box<pg_regex_t>),
    Regis(Regis),
}

impl AFFIX {
    /// FF_SUFFIX or FF_PREFIX.
    pub fn r#type(&self) -> u32 {
        self.bits & 0x1
    }
    pub fn set_type(&mut self, v: u32) {
        self.bits = (self.bits & !0x1) | (v & 0x1);
    }
    pub fn flagflags(&self) -> u32 {
        (self.bits >> 1) & 0x7f
    }
    pub fn set_flagflags(&mut self, v: u32) {
        self.bits = (self.bits & !(0x7f << 1)) | ((v & 0x7f) << 1);
    }
    pub fn issimple(&self) -> bool {
        (self.bits >> 8) & 0x1 != 0
    }
    pub fn set_issimple(&mut self, v: bool) {
        self.bits = (self.bits & !(0x1 << 8)) | ((v as u32) << 8);
    }
    pub fn isregis(&self) -> bool {
        (self.bits >> 9) & 0x1 != 0
    }
    pub fn set_isregis(&mut self, v: bool) {
        self.bits = (self.bits & !(0x1 << 9)) | ((v as u32) << 9);
    }
    pub fn replen(&self) -> u32 {
        (self.bits >> 10) & 0x3fff
    }
    pub fn set_replen(&mut self, v: u32) {
        self.bits = (self.bits & !(0x3fff << 10)) | ((v & 0x3fff) << 10);
    }
}

/// `AffixNodeData` - one slot of an AffixNode. C bitfields `val:8, naff:24`.
pub struct AffixNodeData {
    bits: u32, // val:8 | naff:24
    pub aff: Vec<*mut AFFIX>, // length = naff. TODO(ptr)
    pub node: Option<Box<AffixNode>>,
}

impl AffixNodeData {
    pub fn val(&self) -> u8 {
        (self.bits & 0xff) as u8
    }
    pub fn set_val(&mut self, v: u8) {
        self.bits = (self.bits & !0xff) | v as u32;
    }
    pub fn naff(&self) -> u32 {
        self.bits >> 8
    }
    pub fn set_naff(&mut self, v: u32) {
        self.bits = (self.bits & 0xff) | (v << 8);
    }
}

/// `AffixNode` - prefix tree (Trie) node storing an affix list. C bitfields
/// `isvoid:1, length:31`.
pub struct AffixNode {
    bits: u32, // isvoid:1 | length:31
    pub data: Vec<AffixNodeData>, // length = length
}

impl AffixNode {
    pub fn isvoid(&self) -> bool {
        self.bits & 0x1 != 0
    }
    pub fn set_isvoid(&mut self, v: bool) {
        self.bits = (self.bits & !0x1) | (v as u32);
    }
    pub fn length(&self) -> u32 {
        self.bits >> 1
    }
    pub fn set_length(&mut self, v: u32) {
        self.bits = (self.bits & 0x1) | (v << 1);
    }
}

/// `CMPDAffix` - a compound-affix entry.
pub struct CMPDAffix {
    pub affix: String,
    pub len: i32,
    pub issuffix: bool,
}

/// `FlagMode` - type of encoding affix flags in Hunspell dictionaries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlagMode {
    /// One character (like ispell).
    Char,
    /// Two characters.
    Long,
    /// Number, >= 0 and < 65536.
    Num,
}

/// `CompoundAffixFlag` - a Hunspell option flag. The flag name is a string for
/// FM_CHAR/FM_LONG, or a number for FM_NUM.
pub struct CompoundAffixFlag {
    pub flag: CompoundAffixFlagName,
    pub flag_mode: FlagMode,
    pub value: u32,
}

pub enum CompoundAffixFlagName {
    Str(String),
    Num(u32),
}

/// `FLAGNUM_MAXSIZE`.
pub const FLAGNUM_MAXSIZE: u32 = 1 << 16;

/// `IspellDict` - in-memory ISpell dictionary plus its construction state.
pub struct IspellDict {
    pub maffixes: i32,
    pub naffixes: i32,
    pub affix: Vec<AFFIX>,

    pub suffix: Option<Box<AffixNode>>,
    pub prefix: Option<Box<AffixNode>>,

    pub dictionary: Option<Box<SPNode>>,
    /// Array of sets of affixes.
    pub affix_data: Vec<String>,
    pub use_flag_aliases: bool,

    pub compound_affix: Vec<CMPDAffix>,

    pub usecompound: bool,
    pub flag_mode: FlagMode,

    // Fields below are only needed for initialization.
    pub compound_affix_flags: Vec<CompoundAffixFlag>,

    // Fields below are only used during dictionary construction.
    pub spell: Vec<Box<SPELL>>,
}

pub fn ni_normalize_word(_conf: &mut IspellDict, _word: &str) -> Vec<TSLexeme> {
    unimplemented!()
}

pub fn ni_start_build(_conf: &mut IspellDict) {
    unimplemented!()
}

pub fn ni_import_affixes(_conf: &mut IspellDict, _filename: &str) {
    unimplemented!()
}

pub fn ni_import_dictionary(_conf: &mut IspellDict, _filename: &str) {
    unimplemented!()
}

pub fn ni_sort_dictionary(_conf: &mut IspellDict) {
    unimplemented!()
}

pub fn ni_sort_affixes(_conf: &mut IspellDict) {
    unimplemented!()
}

pub fn ni_finish_build(_conf: &mut IspellDict) {
    unimplemented!()
}
