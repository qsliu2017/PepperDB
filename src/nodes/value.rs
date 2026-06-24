//! Translated from PostgreSQL src/include/nodes/value.h

// These literal nodes become variants of `crate::nodes::nodes::Node` in the node
// pass; the C `NodeTag type` header field is the enum discriminant and is dropped.

/// T_Integer literal node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Integer {
    pub ival: i32,
}

/// T_Float literal node. Stored as a string to avoid precision loss
/// (may become NUMERIC); the string looks like a valid numeric literal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Float {
    pub fval: String,
}

/// T_Boolean literal node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Boolean {
    pub boolval: bool,
}

/// T_String literal node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct String_ {
    pub sval: String,
}

/// T_BitString literal node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BitString {
    pub bsval: String,
}

/// C: `intVal(v)`.
pub fn intVal(v: &Integer) -> i32 {
    v.ival
}
/// C: `floatVal(v)` -- `atof` of the stored string.
pub fn floatVal(v: &Float) -> f64 {
    v.fval.parse().unwrap_or(0.0)
}
/// C: `boolVal(v)`.
pub fn boolVal(v: &Boolean) -> bool {
    v.boolval
}
/// C: `strVal(v)`.
pub fn strVal(v: &String_) -> &str {
    &v.sval
}

pub fn makeInteger(i: i32) -> Integer {
    Integer { ival: i }
}
pub fn makeFloat(numeric_str: String) -> Float {
    Float { fval: numeric_str }
}
pub fn makeBoolean(val: bool) -> Boolean {
    Boolean { boolval: val }
}
pub fn makeString(str: String) -> String_ {
    String_ { sval: str }
}
pub fn makeBitString(str: String) -> BitString {
    BitString { bsval: str }
}
