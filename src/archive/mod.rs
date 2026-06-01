//! Archive/restore module interface
//! (postgres/src/backend/archive + postgres/src/include/archive).
//!
//! Header-only type/callback layer so far: the archive-module API (`archive_module`).

pub mod archive_module;
pub mod shell_archive;
