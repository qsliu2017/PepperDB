//! Translated from PostgreSQL src/include/access/rmgr.h

/// Resource manager id. Must fit in u8; widening affects the XLOG file format.
pub type RmgrId = u8;

// The concrete `RmgrId` enum (the PG_RMGR list) is translated in
// `crate::access::rmgrlist`; the bounds the C `pg_rmgr` generator derives from
// `RM_NEXT_ID` (one past the last builtin) are computed from it here.
use crate::access::rmgrlist::RmgrId as RmgrIds;

/// One past the last builtin resource manager (C `RM_NEXT_ID`).
pub const RM_NEXT_ID: u32 = RmgrIds::MAX_ID as u32 + 1;
/// Highest builtin resource manager ID.
pub const RM_MAX_BUILTIN_ID: u32 = RM_NEXT_ID - 1;
/// Number of builtin resource managers.
pub const RM_N_BUILTIN_IDS: u32 = RM_NEXT_ID;

pub const RM_MAX_ID: u32 = u8::MAX as u32;
pub const RM_MIN_CUSTOM_ID: u32 = 128;
pub const RM_MAX_CUSTOM_ID: u32 = u8::MAX as u32;
pub const RM_N_IDS: u32 = u8::MAX as u32 + 1;
pub const RM_N_CUSTOM_IDS: u32 = RM_MAX_CUSTOM_ID - RM_MIN_CUSTOM_ID + 1;

pub const fn rmgr_id_is_custom(rmid: i32) -> bool {
    rmid >= RM_MIN_CUSTOM_ID as i32 && rmid <= RM_MAX_CUSTOM_ID as i32
}

/// C `RmgrIdIsBuiltin(rmid)`: a valid builtin resource manager.
pub const fn rmgr_id_is_builtin(rmid: i32) -> bool {
    rmid >= 0 && rmid < RM_NEXT_ID as i32
}

/// C `RmgrIdIsValid(rmid)`: builtin or custom.
pub const fn rmgr_id_is_valid(rmid: i32) -> bool {
    rmgr_id_is_builtin(rmid) || rmgr_id_is_custom(rmid)
}

/// RmgrId for extensions still in development without a reserved RmgrId.
pub const RM_EXPERIMENTAL_ID: u32 = 128;

// The `Rmgr` trait + the `match`-based dispatch (`GetRmgr`/`RmgrIdExists`/
// `RmgrStartup`/`RmgrCleanup`) live in the backend module (rmgr.c body).
// Re-export so `use crate::access::rmgr::{Rmgr, GetRmgr}` resolves to the real
// implementation (header declaration / .c definition split, rules.md section 2).
pub use crate::backend::access::transam::rmgr::{
    GetRmgr, Rmgr, RmgrCleanup, RmgrIdExists, RmgrStartup,
};
