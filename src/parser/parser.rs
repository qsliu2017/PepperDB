//! Translated from PostgreSQL src/include/parser/parser.h
//! Definitions for the "raw" parser (flex and bison phases only).

use crate::nodes::nodes::Node;
use crate::nodes::parsenodes::TypeName;

/// RawParseMode determines the form of the string that raw_parser() accepts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RawParseMode {
    /// parse a semicolon-separated list of SQL commands -> List of RawStmt
    Default = 0,
    /// parse a type name -> one-element List containing a TypeName
    TypeName,
    /// parse a PL/pgSQL expression -> one-element List containing a RawStmt
    PlpgsqlExpr,
    /// parse a PL/pgSQL assignment with 1 dotted name in the target ColumnRef
    PlpgsqlAssign1,
    /// parse a PL/pgSQL assignment with 2 dotted names
    PlpgsqlAssign2,
    /// parse a PL/pgSQL assignment with 3 dotted names
    PlpgsqlAssign3,
}

/// Values for the backslash_quote GUC.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackslashQuoteType {
    Off,
    On,
    SafeEncoding,
}

// GUC variables in scan.l. TODO(global): migrate to session/parser state.
pub static mut BACKSLASH_QUOTE: i32 = 0;
pub static mut ESCAPE_STRING_WARNING: bool = false;
pub static mut STANDARD_CONFORMING_STRINGS: bool = false;

/// Primary entry point for the raw parsing functions.
pub fn raw_parser(_str: &str, _mode: RawParseMode) -> Vec<Box<Node>> {
    unimplemented!()
}

/// Utility function exported by gram.y.
pub fn system_func_name(_name: &str) -> Vec<Box<Node>> {
    unimplemented!()
}

/// Utility function exported by gram.y.
pub fn system_type_name(_name: &str) -> Box<TypeName> {
    unimplemented!()
}
