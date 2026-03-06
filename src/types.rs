// Shared type definitions: OID, TypeId, Datum.

pub type OID = u32;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TypeId {
    Bool,
    Int2,
    Int4,
    Int8,
    Float4,
    Float8,
    Text,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Datum {
    Bool(bool),
    Int2(i16),
    Int4(i32),
    Int8(i64),
    Float4(f32),
    Float8(f64),
    Text(String),
    Null,
}
