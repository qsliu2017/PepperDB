//! pch/c_pch.h - precompiled-header aggregator (build-speed only; defines no symbols).
//!
//! In the upstream PostgreSQL tree this header exists solely to seed an MSVC
//! precompiled header. Its entire body is a single include directive:
//!
//! ```c
//! #include "c.h"
//! ```
//!
//! It aggregates:
//! - `c.h` (crate::c) - the fundamental C compatibility / base type definitions.
//!
//! There are no typedefs, structs, macros, or prototypes here. This module is
//! intentionally empty (0 symbols); the aggregation is expressed through the
//! Rust module tree (`crate::c`) rather than textual inclusion.
