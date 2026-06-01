//! Table partitioning support
//! (postgres/src/backend/partitioning + postgres/src/include/partitioning).
//!
//! Header-only type layer so far: the shared partitioning typedefs (`partdefs`).

pub mod partdefs;
pub mod partdesc;
pub mod partbounds;
pub mod partprune;
