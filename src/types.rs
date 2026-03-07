//! Shared type definitions: OID, TypeId, Datum.

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

impl TypeId {
    /// PostgreSQL typalign: alignment boundary for this type's on-disk storage.
    pub fn align(&self) -> usize {
        match self {
            TypeId::Bool => 1,
            TypeId::Int2 => 2,
            TypeId::Int4 | TypeId::Float4 | TypeId::Text => 4,
            TypeId::Int8 | TypeId::Float8 => 8,
        }
    }

    /// PostgreSQL typlen: fixed byte size, or -1 for variable-length (varlena).
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> i16 {
        match self {
            TypeId::Bool => 1,
            TypeId::Int2 => 2,
            TypeId::Int4 => 4,
            TypeId::Int8 => 8,
            TypeId::Float4 => 4,
            TypeId::Float8 => 8,
            TypeId::Text => -1,
        }
    }

    /// PostgreSQL pg_type OID for this type.
    pub fn pg_oid(&self) -> OID {
        match self {
            TypeId::Bool => 16,
            TypeId::Int2 => 21,
            TypeId::Int4 => 23,
            TypeId::Int8 => 20,
            TypeId::Float4 => 700,
            TypeId::Float8 => 701,
            TypeId::Text => 25,
        }
    }

    /// Reverse lookup from PostgreSQL pg_type OID.
    pub fn from_pg_oid(oid: OID) -> Option<Self> {
        match oid {
            16 => Some(TypeId::Bool),
            21 => Some(TypeId::Int2),
            23 => Some(TypeId::Int4),
            20 => Some(TypeId::Int8),
            700 => Some(TypeId::Float4),
            701 => Some(TypeId::Float8),
            25 => Some(TypeId::Text),
            _ => None,
        }
    }
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
