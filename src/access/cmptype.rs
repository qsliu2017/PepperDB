//! Translated from PostgreSQL src/include/access/cmptype.h

/// CompareType - fundamental semantics of certain operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum CompareType {
    Invalid = 0,
    Lt = 1,
    Le = 2,
    Eq = 3,
    Ge = 4,
    Gt = 5,
    Ne = 6,
    Overlap = 7,
    ContainedBy = 8,
}
