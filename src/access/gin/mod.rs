//! Generalized Inverted Index access method (postgres/src/backend/access/gin).
//!
//! Translated incrementally. So far: the tri-state consistency shim
//! (`ginlogic`) and the build-time entry accumulator (`ginbulk`).

pub mod gin;
pub mod gin_private;
pub mod gin_tuple;
pub mod ginblock;
pub mod ginarrayproc;
pub mod ginbulk;
pub mod ginlogic;
pub mod ginpostinglist;
pub mod ginvalidate;
pub mod ginentrypage;
pub mod ginxlog;
pub mod ginvacuum;
pub mod ginbtree;
pub mod ginfast;
