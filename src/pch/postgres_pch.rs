//! pch/postgres_pch.h - precompiled-header aggregator for the backend.
//!
//! This is a PostgreSQL precompiled-header (PCH) aggregator: in C its body is
//! nothing more than a list of `#include` directives used to speed up the MSVC
//! build. It declares NO typedefs, structs, macros, or prototypes of its own.
//!
//! Aggregated headers:
//!   - postgres.h  (the core backend prelude: c.h, palloc/MemoryContext, elog,
//!                  Datum, fmgr basics, etc.)
//!
//! In the Rust port there is no precompiled-header mechanism, so this module is
//! intentionally empty. The symbols that `postgres.h` brings in live in their
//! own translated modules (e.g. crate::c and friends) and are imported
//! directly where needed.
//!
//! 0 symbols defined.
