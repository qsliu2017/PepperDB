//! Foreign-data-wrapper support
//! (postgres/src/backend/foreign + postgres/src/include/foreign).
//!
//! Header-only type/prototype layer so far: the FDW routine API (`fdwapi`).

pub mod fdwapi;
pub mod foreign;
