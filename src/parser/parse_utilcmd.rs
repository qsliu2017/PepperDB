//! Translated from PostgreSQL src/include/parser/parse_utilcmd.h
//!
//! Prototypes for parse_utilcmd.c. The bodies live in
//! `crate::backend::parser::parse_utilcmd`; this header re-exports them under the
//! C names so `use crate::parser::parse_utilcmd::<name>` keeps resolving.

pub use crate::backend::parser::parse_utilcmd::{
    expandTableLikeClause, generateClonedIndexStmt, transformAlterTableStmt, transformCreateStmt,
    transformCreateSchemaStmtElements, transformIndexStmt, transformPartitionBound,
    transformRuleStmt, transformStatsStmt,
};
