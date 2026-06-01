//! pg_trace.h - Definitions for the PostgreSQL tracing framework.
//!
//! The C header is a thin wrapper whose sole content is:
//!
//! ```c
//! #include "utils/probes.h"  /* IWYU pragma: export */
//! ```
//!
//! `utils/probes.h` is not a checked-in header: it is generated at build time
//! from `src/backend/utils/probes.d` (the DTrace provider definition) by the
//! `dtrace -h` tool. When tracing is disabled (the default), every probe macro
//! (e.g. `TRACE_POSTGRESQL_LWLOCK_ACQUIRE(...)`) expands to a no-op, and
//! `TRACE_POSTGRESQL_<probe>_ENABLED()` expands to `(0)`.
//!
//! There is therefore no Rust-meaningful declaration to translate here: this
//! header contributes only a re-export of the generated probe macros. The Rust
//! port models the probes as no-op inline helpers in the (to-be-generated)
//! `utils::probes` module; this module re-exports nothing on its own.
//!
//! See: src/include/pg_trace.h, src/backend/utils/probes.d
