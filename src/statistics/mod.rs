//! Extended planner statistics
//! (postgres/src/backend/statistics + postgres/src/include/statistics).
//!
//! Header-only type/prototype layer so far.

pub mod extended_stats_internal;
pub mod relation_stats;
pub mod stat_utils;
pub mod statistics;
pub mod mvdistinct;
pub mod attribute_stats;
pub mod extended_stats;
