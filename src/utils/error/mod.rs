//! Backend error-reporting support (postgres/src/backend/utils/error).
//!
//! `elog`/`ereport` themselves live in `crate::utils::elog`; this submodule
//! holds the smaller pieces (the Assert failure handler).

pub mod assert;
pub mod csvlog;
pub mod jsonlog;
pub mod elog_impl;
