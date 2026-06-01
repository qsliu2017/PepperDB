//! queryjumble.h - Query normalization and fingerprinting.

use std::ffi::c_int;
use std::ffi::c_uint;

use crate::c::Size;
use crate::nodes::parsenodes::Query;

/*
 * Struct for tracking locations/lengths of constants during normalization
 */
#[repr(C)]
pub struct LocationLen {
    /// start offset in query text
    pub location: c_int,
    /// length in bytes, or -1 to ignore
    pub length: c_int,

    /// Does this location represent a squashed list?
    pub squashed: bool,

    /// Is this location a PARAM_EXTERN parameter?
    pub extern_param: bool,
}

/*
 * Working state for computing a query jumble and producing a normalized
 * query string
 */
#[repr(C)]
pub struct JumbleState {
    /// Jumble of current query tree
    pub jumble: *mut std::ffi::c_uchar,

    /// Number of bytes used in jumble[]
    pub jumble_len: Size,

    /// Array of locations of constants that should be removed
    pub clocations: *mut LocationLen,

    /// Allocated length of clocations array
    pub clocations_buf_size: c_int,

    /// Current number of valid entries in clocations array
    pub clocations_count: c_int,

    /*
     * ID of the highest PARAM_EXTERN parameter we've seen in the query; used
     * to start normalization correctly.  However, if there are any squashed
     * lists in the query, we disregard query-supplied parameter numbers and
     * renumber everything.  This is to avoid possible gaps caused by
     * squashing in case any params are in squashed lists.
     */
    pub highest_extern_param_id: c_int,

    /// Whether squashable lists are present
    pub has_squashed_lists: bool,

    /*
     * Count of the number of NULL nodes seen since last appending a value.
     * These are flushed out to the jumble buffer before subsequent appends
     * and before performing the final jumble hash.
     */
    pub pending_nulls: c_uint,

    // NB: the C field `total_jumble_len` is guarded by #ifdef USE_ASSERT_CHECKING.
    // Omitted here since USE_ASSERT_CHECKING is not defined in this build.
    // #[cfg(USE_ASSERT_CHECKING)]
    // /// The total number of bytes added to the jumble buffer
    // pub total_jumble_len: Size,
}

/* Values for the compute_query_id GUC */
// C enum ComputeQueryIdType -> type alias + consts (project convention).
pub type ComputeQueryIdType = c_int;
pub const COMPUTE_QUERY_ID_OFF: ComputeQueryIdType = 0;
pub const COMPUTE_QUERY_ID_ON: ComputeQueryIdType = 1;
pub const COMPUTE_QUERY_ID_AUTO: ComputeQueryIdType = 2;
pub const COMPUTE_QUERY_ID_REGRESS: ComputeQueryIdType = 3;

/* GUC parameters */
// extern PGDLLIMPORT int compute_query_id;
pub static mut compute_query_id: c_int = 0;

// extern PGDLLIMPORT bool query_id_enabled;
pub static mut query_id_enabled: bool = false;

pub unsafe fn CleanQuerytext(
    query: *const std::ffi::c_char,
    location: *mut c_int,
    len: *mut c_int,
) -> *const std::ffi::c_char {
    unimplemented!()
}

pub unsafe fn JumbleQuery(query: *mut Query) -> *mut JumbleState {
    unimplemented!()
}

pub unsafe fn EnableQueryId() {
    unimplemented!()
}

/*
 * Returns whether query identifier computation has been enabled, either
 * directly in the GUC or by a module when the setting is 'auto'.
 */
#[inline]
pub unsafe fn IsQueryIdEnabled() -> bool {
    if compute_query_id == COMPUTE_QUERY_ID_OFF {
        return false;
    }
    if compute_query_id == COMPUTE_QUERY_ID_ON {
        return true;
    }
    query_id_enabled
}
