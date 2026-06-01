//! pch/postgres_fe_pch.h - precompiled-header aggregator for frontend builds.
//!
//! This is a PostgreSQL precompiled-header (PCH) aggregator. In the original C
//! source its entire body is a list of `#include` directives used purely to
//! speed up compilation; it declares NO typedefs, structs, macros, or
//! prototypes of its own.
//!
//! Aggregated headers:
//! - postgres_fe.h  (frontend-side `postgres.h` equivalent; pulls in the common
//!   frontend prelude shared by client programs and non-backend code)
//!
//! As a PCH aggregator this module intentionally defines 0 symbols. The actual
//! declarations live in the modules corresponding to the aggregated headers.
