//! Translated from PostgreSQL src/include/access/rmgr.h

/// Resource manager id. Must fit in u8; widening affects the XLOG file format.
pub type RmgrId = u8;

// The concrete RmgrIds enum is generated from access/rmgrlist.h (PG_RMGR list).
// Until that build.rs generator exists, expose the count/bound constants the
// header derives from RM_NEXT_ID. RM_NEXT_ID itself is the one-past-last builtin.
// TODO(generated): RmgrIds enum + RM_NEXT_ID come from access/rmgrlist.h.

pub const RM_MAX_ID: u32 = u8::MAX as u32;
pub const RM_MIN_CUSTOM_ID: u32 = 128;
pub const RM_MAX_CUSTOM_ID: u32 = u8::MAX as u32;
pub const RM_N_IDS: u32 = u8::MAX as u32 + 1;
pub const RM_N_CUSTOM_IDS: u32 = RM_MAX_CUSTOM_ID - RM_MIN_CUSTOM_ID + 1;

// RM_MAX_BUILTIN_ID / RM_N_BUILTIN_IDS depend on RM_NEXT_ID (generated).
// TODO(generated): derive from the rmgrlist.h-generated RM_NEXT_ID.

pub const fn rmgr_id_is_custom(rmid: i32) -> bool {
    rmid >= RM_MIN_CUSTOM_ID as i32 && rmid <= RM_MAX_CUSTOM_ID as i32
}

/// RmgrId for extensions still in development without a reserved RmgrId.
pub const RM_EXPERIMENTAL_ID: u32 = 128;
