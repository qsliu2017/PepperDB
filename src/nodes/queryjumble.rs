//! Translated from PostgreSQL src/include/nodes/queryjumble.h

use crate::nodes::parsenodes::Query;

/// Tracks location/length of constants during normalization.
#[derive(Debug, Clone, PartialEq)]
pub struct LocationLen {
    /// Start offset in query text.
    pub location: i32,
    /// Length in bytes, or -1 to ignore.
    pub length: i32,
    /// Does this location represent a squashed list?
    pub squashed: bool,
    /// Is this location a PARAM_EXTERN parameter?
    pub extern_param: bool,
}

/// Working state for computing a query jumble and normalized query string.
#[derive(Debug, Clone, PartialEq)]
pub struct JumbleState {
    /// Jumble of current query tree.
    pub jumble: Vec<u8>,
    /// Number of bytes used in `jumble`.
    pub jumble_len: usize,
    /// Locations of constants that should be removed.
    pub clocations: Vec<LocationLen>,
    /// Allocated length of `clocations` array.
    pub clocations_buf_size: i32,
    /// Current number of valid entries in `clocations` array.
    pub clocations_count: i32,
    /// Highest PARAM_EXTERN parameter id seen; starts normalization correctly.
    pub highest_extern_param_id: i32,
    /// Whether squashable lists are present.
    pub has_squashed_lists: bool,
    /// NULL nodes seen since last appended value; flushed before next append.
    pub pending_nulls: u32,
    /// Total bytes added to the jumble buffer (assert-checking builds).
    #[cfg(debug_assertions)]
    pub total_jumble_len: usize,
}

/// Values for the compute_query_id GUC.
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ComputeQueryIdType {
    COMPUTE_QUERY_ID_OFF,
    COMPUTE_QUERY_ID_ON,
    COMPUTE_QUERY_ID_AUTO,
    COMPUTE_QUERY_ID_REGRESS,
}

// GUC parameters (process-globals; become session/context state later).
// TODO(global): convert to session-threaded state.
pub static mut compute_query_id: i32 = 0;
pub static mut query_id_enabled: bool = false;

/// Returns query text with leading whitespace/comments stripped; the adjusted
/// location and length are returned alongside it.
pub fn CleanQuerytext(query: &str, location: i32, len: i32) -> (String, i32, i32) {
    unimplemented!()
}

pub fn JumbleQuery(query: &Query) -> Box<JumbleState> {
    unimplemented!()
}

pub fn EnableQueryId() {
    unimplemented!()
}

/// Whether query identifier computation has been enabled.
pub fn IsQueryIdEnabled() -> bool {
    unsafe {
        if compute_query_id == ComputeQueryIdType::COMPUTE_QUERY_ID_OFF as i32 {
            return false;
        }
        if compute_query_id == ComputeQueryIdType::COMPUTE_QUERY_ID_ON as i32 {
            return true;
        }
        query_id_enabled
    }
}
