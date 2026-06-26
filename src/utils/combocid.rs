//! Translated from PostgreSQL src/include/utils/combocid.h
//!
//! Bodies live in `crate::backend::utils::time::combocid`; this header re-exports
//! them (snake_case global-state file -> `pub use`, rules s2/s3).

pub use crate::backend::utils::time::combocid::{
    at_eo_xact_combo_cid, estimate_combo_cid_state_space, restore_combo_cid_state,
    serialize_combo_cid_state,
};
