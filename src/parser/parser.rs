//! Translated from PostgreSQL src/include/parser/parser.h
//! Definitions for the "raw" parser (flex and bison phases only).
//!
//! The driver body (`raw_parser`) lives in the backend definition module
//! (`crate::backend::parser::parser`); this header re-exports it. The enums/GUC
//! declarations below are part of the header itself.

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

/// Primary entry point for the raw parsing functions (`crate::backend::parser::parser`).
pub use crate::backend::parser::parser::raw_parser;

/// Utility function exported by gram.y.
pub fn system_func_name(_name: &str) -> Vec<Node> {
    unimplemented!()
}

/// Utility function exported by gram.y.
pub fn system_type_name(_name: &str) -> Box<TypeName> {
    unimplemented!()
}
