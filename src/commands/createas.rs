//! Translated from PostgreSQL src/include/commands/createas.h
//!
//! The bodies live in `crate::backend::commands::createas`; this header rewires the
//! public entry points to `pub use` so `crate::commands::createas::<Name>` call sites
//! resolve (rules.md s3).

/// PG `ExecCreateTableAs`: execute a CREATE TABLE AS / SELECT INTO command.
pub use crate::backend::commands::createas::ExecCreateTableAs;

/// PG `GetIntoRelEFlags`: executor flags needed for CREATE TABLE AS.
pub use crate::backend::commands::createas::GetIntoRelEFlags;
