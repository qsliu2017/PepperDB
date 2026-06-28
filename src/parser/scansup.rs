//! Translated from PostgreSQL src/include/parser/scansup.h
//! Scanner support routines used by the core lexer.
//!
//! The bodies live in the backend definition module
//! (`crate::backend::parser::scansup`); this header re-exports them so existing
//! `use crate::parser::scansup::*` call sites keep resolving.

pub use crate::backend::parser::scansup::{
    downcase_identifier, downcase_truncate_identifier, scanner_isspace, truncate_identifier,
};
