//! Translated from PostgreSQL src/include/tsearch/ts_type.h
//!
//! tsvector and tsquery types. These are ON-DISK varlena formats: the fixed
//! headers are `#[repr(C)]` and the bitfield words become integer newtypes with
//! accessor methods (bitfields are not flag sets - bitflags-port.md appendix C).
//! Target: 64-bit LE. tsvector/tsquery are 4-byte aligned varlenas with trailing
//! flexible data (WordEntry array + lexeme storage / QueryItem array + operands);
//! the FAM tail lives in the buffer, the header is a view (translation-rules.md).

use crate::postgres::Datum;

/// `WordEntry` - per-lexeme entry in a tsvector. C bitfield
/// `haspos:1 | len:11 | pos:20` packed in a u32 (on-disk; sizeof must be 4).
/// Integer newtype + accessors (bitfields are not a flag set).
#[repr(transparent)]
pub struct WordEntry(pub u32);

const _: () = assert!(core::mem::size_of::<WordEntry>() == 4);

impl WordEntry {
    /// haspos:1 (bit 0).
    pub const fn haspos(self) -> bool {
        (self.0 & 0x1) != 0
    }
    /// len:11 (bits 1..12). MAX 2Kb.
    #[allow(clippy::len_without_is_empty, reason = "mirrors PG length accessor; is_empty not part of PG API")]
    pub const fn len(self) -> u32 {
        (self.0 >> 1) & MAXSTRLEN
    }
    /// pos:20 (bits 12..32). MAX 1Mb (byte offset to lexeme string).
    pub const fn pos(self) -> u32 {
        (self.0 >> 12) & MAXSTRPOS
    }
    pub const fn new(haspos: bool, len: u32, pos: u32) -> Self {
        Self((haspos as u32) | ((len & MAXSTRLEN) << 1) | ((pos & MAXSTRPOS) << 12))
    }
}

/// `MAXSTRLEN` = (1<<11) - 1.
pub const MAXSTRLEN: u32 = (1 << 11) - 1;
/// `MAXSTRPOS` = (1<<20) - 1.
pub const MAXSTRPOS: u32 = (1 << 20) - 1;

/// `compareWordEntryPos` - qsort comparator over WordEntryPos values.
pub fn compare_word_entry_pos(_a: &WordEntryPos, _b: &WordEntryPos) -> i32 {
    unimplemented!()
}

/// `WordEntryPos` - a position, C bitfield `weight:2 | pos:14` packed in a u16.
/// Integer newtype + accessors (WEP_* macros).
pub type WordEntryPos = u16;

/// WEP_GETWEIGHT(x) = x >> 14.
pub const fn wep_getweight(x: WordEntryPos) -> u16 {
    x >> 14
}
/// WEP_GETPOS(x) = x & 0x3fff.
pub const fn wep_getpos(x: WordEntryPos) -> u16 {
    x & 0x3fff
}
/// WEP_SETWEIGHT(x, v) -> new word with weight set.
pub const fn wep_setweight(x: WordEntryPos, v: u16) -> WordEntryPos {
    v.wrapping_shl(14) | (x & 0x3fff)
}
/// WEP_SETPOS(x, v) -> new word with pos set.
pub const fn wep_setpos(x: WordEntryPos, v: u16) -> WordEntryPos {
    (x & 0xc000) | (v & 0x3fff)
}

/// `MAXENTRYPOS` = 1<<14.
pub const MAXENTRYPOS: u16 = 1 << 14;
/// `MAXNUMPOS` = 256.
pub const MAXNUMPOS: u16 = 256;
/// LIMITPOS(x) clamps to MAXENTRYPOS-1.
pub const fn limitpos(x: u16) -> u16 {
    if x >= MAXENTRYPOS {
        MAXENTRYPOS - 1
    } else {
        x
    }
}

/// `WordEntryPosVector` - npos followed by a FAM of positions. On-disk fixed
/// header; positions live in the buffer after it.
#[repr(C)]
pub struct WordEntryPosVector {
    pub npos: u16,
    // pos: [WordEntryPos; FLEXIBLE_ARRAY_MEMBER] - trailing in buffer
}

/// `WordEntryPosVector1` - variant with exactly one entry.
#[repr(C)]
pub struct WordEntryPosVector1 {
    pub npos: u16,
    pub pos: [WordEntryPos; 1],
}

/// `TSVectorData` - a complete tsvector datum (ON-DISK varlena). Fixed header is
/// `vl_len_` (varlena) + `size`; the WordEntry array and lexeme storage follow
/// in the buffer. Layout note: entries[] starts at offset 8 (DATAHDRSIZE).
#[repr(C)]
pub struct TSVectorData {
    /// Varlena header (do not touch directly).
    pub vl_len_: i32,
    /// Number of lexemes (WordEntry array entries).
    pub size: i32,
    // entries: [WordEntry; FLEXIBLE_ARRAY_MEMBER] then lexeme bytes - in buffer
}

const _: () = assert!(core::mem::size_of::<TSVectorData>() == 8);

/// `TSVector` (C: pointer to TSVectorData).
pub type TSVector = *mut TSVectorData; // TODO(ptr)

/// `DATAHDRSIZE` = offsetof(TSVectorData, entries).
pub const DATAHDRSIZE: usize = 8;

/// `CALCDATASIZE(nentries, lenstr)` - total tsvector size.
pub const fn calcdatasize(nentries: usize, lenstr: usize) -> usize {
    DATAHDRSIZE + nentries * core::mem::size_of::<WordEntry>() + lenstr
}

// ----- TSQuery -----

/// `QueryItemType` (C: int8). Tag distinguishing operand vs operator.
pub type QueryItemType = i8;

/// Valid QueryItemType values.
pub const QI_VAL: i8 = 1;
pub const QI_OPR: i8 = 2;
/// Only used in an intermediate parse stack; not legal elsewhere.
pub const QI_VALSTOP: i8 = 3;

/// `QueryOperand` - a tsquery operand. ON-DISK: trailing bitfield word
/// `length:12 | distance:20` packed in a u32. Other fields are plain.
#[repr(C)]
pub struct QueryOperand {
    /// Operand or kind of operator.
    pub type_: QueryItemType,
    /// Bitmask of allowed weights (A:1<<3 B:1<<2 C:1<<1 D:1<<0; 0 = any).
    pub weight: u8,
    /// True if it's a prefix search.
    pub prefix: bool,
    /// crc of the operand value (signed for comparison reasons).
    pub valcrc: i32,
    /// Packed `length:12 | distance:20` (offset of operand text).
    pub len_dist: QueryOperandLenDist,
}

/// Packed `length:12 | distance:20` word of QueryOperand (on-disk bitfield).
#[repr(transparent)]
pub struct QueryOperandLenDist(pub u32);

impl QueryOperandLenDist {
    /// length:12 (bits 0..12).
    pub const fn length(self) -> u32 {
        self.0 & 0xfff
    }
    /// distance:20 (bits 12..32).
    pub const fn distance(self) -> u32 {
        (self.0 >> 12) & 0xfffff
    }
    pub const fn new(length: u32, distance: u32) -> Self {
        Self((length & 0xfff) | ((distance & 0xfffff) << 12))
    }
}

/// Legal values for `QueryOperator.oper`.
pub const OP_NOT: i8 = 1;
pub const OP_AND: i8 = 2;
pub const OP_OR: i8 = 3;
/// Highest code.
pub const OP_PHRASE: i8 = 4;
pub const OP_COUNT: usize = 4;

/// Operation priority table, indexed by `oper - 1`.
pub static tsearch_op_priority: [i32; OP_COUNT] = [0; OP_COUNT]; // TODO: real priorities in .c

/// OP_PRIORITY(x) - priority by op code.
pub fn op_priority(x: i8) -> i32 {
    tsearch_op_priority[(x - 1) as usize]
}

/// `QueryOperator` - a tsquery operator node. ON-DISK; all plain fields.
#[repr(C)]
pub struct QueryOperator {
    pub type_: QueryItemType,
    /// See OP_* above.
    pub oper: i8,
    /// Distance between args for OP_PHRASE.
    pub distance: i16,
    /// Offset to left operand (right operand is item+1).
    pub left: u32,
}

/// `QueryItem` - C union of {type tag, QueryOperator, QueryOperand}. The first
/// byte (type) discriminates. On disk it is a 4-byte-aligned union; in Rust we
/// model it as a tagged enum for use, with the type tag held by the variant.
/// QueryItemType enum mirrors the QI_* discriminator.
pub enum QueryItem {
    /// QI_OPR.
    Operator(QueryOperator),
    /// QI_VAL.
    Operand(QueryOperand),
}

/// `TSQueryData` - a complete tsquery datum (ON-DISK varlena). Fixed header is
/// `vl_len_` + `size` (number of QueryItems); the QueryItem array and
/// '\0'-terminated operand strings follow in the buffer.
#[repr(C)]
pub struct TSQueryData {
    /// Varlena header (do not touch directly).
    pub vl_len_: i32,
    /// Number of QueryItems.
    pub size: i32,
    // data: [u8; FLEXIBLE_ARRAY_MEMBER] - QueryItems then operand strings
}

const _: () = assert!(core::mem::size_of::<TSQueryData>() == 8);

/// `TSQuery` (C: pointer to TSQueryData).
pub type TSQuery = *mut TSQueryData; // TODO(ptr)

/// `HDRSIZETQ` = VARHDRSZ (4) + sizeof(int32).
pub const HDRSIZETQ: usize = 4 + 4;

/// `COMPUTESIZE(size, lenofoperand)` - header + all QueryItems + operands.
pub const fn computesize(size: usize, lenofoperand: usize) -> usize {
    HDRSIZETQ + size * core::mem::size_of::<QueryOperator>() + lenofoperand
}

// --- fmgr interface functions ---

/// `DatumGetTSVector` - detoast a Datum to a TSVector.
pub fn datum_get_tsvector(_x: Datum) -> TSVector {
    unimplemented!()
}

/// `DatumGetTSVectorCopy` - detoast + copy.
pub fn datum_get_tsvector_copy(_x: Datum) -> TSVector {
    unimplemented!()
}

/// `TSVectorGetDatum`.
pub fn tsvector_get_datum(_x: &TSVectorData) -> Datum {
    unimplemented!()
}

/// `DatumGetTSQuery`.
pub fn datum_get_tsquery(_x: Datum) -> TSQuery {
    unimplemented!()
}

/// `DatumGetTSQueryCopy` - detoast + copy.
pub fn datum_get_tsquery_copy(_x: Datum) -> TSQuery {
    unimplemented!()
}

/// `TSQueryGetDatum`.
pub fn tsquery_get_datum(_x: &TSQueryData) -> Datum {
    unimplemented!()
}
