// Shared type definitions: OID, TypeId, Datum.

pub type OID = u32;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TypeId {
    Int4,
}

impl TypeId {
    pub fn size(&self) -> usize {
        match self {
            TypeId::Int4 => 4,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Datum {
    Int4(i32),
    Null,
}
