//! Translated from PostgreSQL src/include/tsearch/dicts/regis.h
//
// Fast regex subset used by ISpell. The C structs pack small bitfields into a
// uint32; Rust has no bitfields, so the packed word becomes an integer field
// with accessor methods (per translation-rules.md). These are in-memory builder
// structures, not on-disk, so the rest is idiomatic.

/// Regis node type (the packed `type:2` field).
#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegisNodeType {
    OneOf = 1,
    NoneOf = 2,
}

/// A node in a Regis chain. C bitfields `type:2, len:16, unused:14` are packed
/// into `bits`; the variable-length `data[]` becomes an owned byte buffer.
pub struct RegisNode {
    bits: u32, // type:2 | len:16 | unused:14 (low-to-high)
    pub next: Option<Box<RegisNode>>,
    pub data: Vec<u8>,
}

impl RegisNode {
    pub fn r#type(&self) -> RegisNodeType {
        match self.bits & 0x3 {
            1 => RegisNodeType::OneOf,
            2 => RegisNodeType::NoneOf,
            other => panic!("invalid RegisNodeType: {other}"),
        }
    }
    pub fn set_type(&mut self, v: RegisNodeType) {
        self.bits = (self.bits & !0x3) | (v as u32 & 0x3);
    }
    pub fn len(&self) -> u32 {
        (self.bits >> 2) & 0xffff
    }
    pub fn set_len(&mut self, v: u32) {
        self.bits = (self.bits & !(0xffff << 2)) | ((v & 0xffff) << 2);
    }
}

/// A compiled Regis expression. C bitfields `issuffix:1, nchar:16, unused:15`
/// packed into `bits`.
pub struct Regis {
    pub node: Option<Box<RegisNode>>,
    bits: u32, // issuffix:1 | nchar:16 | unused:15 (low-to-high)
}

impl Regis {
    pub fn issuffix(&self) -> bool {
        (self.bits & 0x1) != 0
    }
    pub fn set_issuffix(&mut self, v: bool) {
        self.bits = (self.bits & !0x1) | (v as u32);
    }
    pub fn nchar(&self) -> u32 {
        (self.bits >> 1) & 0xffff
    }
    pub fn set_nchar(&mut self, v: u32) {
        self.bits = (self.bits & !(0xffff << 1)) | ((v & 0xffff) << 1);
    }
}

pub fn rs_is_regis(_str: &str) -> bool {
    unimplemented!()
}

pub fn rs_compile(_r: &mut Regis, _issuffix: bool, _str: &str) {
    unimplemented!()
}

pub fn rs_free(_r: &mut Regis) {
    unimplemented!()
}

/// Returns true if `str` matches.
pub fn rs_execute(_r: &Regis, _str: &str) -> bool {
    unimplemented!()
}
