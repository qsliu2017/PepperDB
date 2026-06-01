//! postgres_fe.h - Primary include file for PostgreSQL client-side .c files.
//!
//! This is the frontend counterpart to `postgres.h`. In C it is purely a
//! preprocessor aggregator: it defines `FRONTEND` and re-exports `c.h` and
//! `common/fe_memutils.h` (the "IWYU pragma: begin/end_exports" block). It
//! must be the first file included by client libraries and application
//! programs - but NOT by backend modules, which include `postgres.h`.
//!
//! There is no Rust-meaningful type or value declared directly in this header
//! beyond the `FRONTEND` build macro. The actual contents come from the
//! re-exported modules, which already exist in the crate:
//!   - C `#include "c.h"`                  -> `crate::c`
//!   - C `#include "common/fe_memutils.h"` -> `crate::common::fe_memutils`
//!
//! We intentionally do NOT add glob re-exports here (project convention: no
//! glob re-exports); consumers should `use crate::c::*` /
//! `use crate::common::fe_memutils::*` directly, matching the include graph.

/// C: `#ifndef FRONTEND #define FRONTEND 1 #endif`
///
/// Marks frontend/client compilation. In the C source this gates many
/// backend-only constructs out of client builds. There are no Cargo features
/// in this project, so it is exposed as a plain constant for code that needs
/// to branch on frontend-ness.
pub const FRONTEND: i32 = 1;
