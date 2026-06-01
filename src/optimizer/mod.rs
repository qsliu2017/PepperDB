//! Query optimizer / planner (postgres/src/backend/optimizer +
//! postgres/src/include/optimizer).
//!
//! So far: the `util` helper routines. The path/plan/prep phases are future work.

pub mod cost;
pub mod geqo;
pub mod optimizer;
pub mod path;
pub mod paths;
pub mod plan;
pub mod prep;
pub mod util;
