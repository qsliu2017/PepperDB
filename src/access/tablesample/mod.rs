//! TABLESAMPLE methods (postgres/src/backend/access/tablesample).
//!
//! Sampling methods implementing the `TsmRoutine` vtable (`crate::access::tsmapi`).
//! So far: the BERNOULLI and SYSTEM built-in methods.

pub mod bernoulli;
pub mod tablesample;
pub mod system;
