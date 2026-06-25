//! Translated from PostgreSQL src/include/access/xlogstats.h

use crate::access::rmgr::RM_MAX_ID;
use crate::access::xlogdefs::XLogRecPtr;
use crate::access::xlogreader::XLogReaderState;

pub const MAX_XLINFO_TYPES: usize = 16;

const RM_COUNT: usize = RM_MAX_ID as usize + 1;

/// Per-record-type WAL statistics counters (in-memory analysis).
#[derive(Debug, Clone, Copy, Default)]
pub struct XLogRecStats {
    pub count: u64,
    pub rec_len: u64,
    pub fpi_len: u64,
}

/// Aggregated WAL statistics (in-memory analysis). startptr/endptr are only used
/// by frontend tools (pg_waldump); kept inline under the single-process model.
/// `record_stats` is the large per-(rmid, recid) matrix; it is boxed so the
/// struct (and `Default`) does not place ~96 KB on the stack.
pub struct XLogStats {
    pub count: u64,
    pub startptr: XLogRecPtr,
    pub endptr: XLogRecPtr,
    pub rmgr_stats: [XLogRecStats; RM_COUNT],
    pub record_stats: Box<[[XLogRecStats; MAX_XLINFO_TYPES]; RM_COUNT]>,
}

impl Default for XLogStats {
    fn default() -> Self {
        XLogStats {
            count: 0,
            startptr: XLogRecPtr(0),
            endptr: XLogRecPtr(0),
            rmgr_stats: [XLogRecStats::default(); RM_COUNT],
            record_stats: Box::new([[XLogRecStats::default(); MAX_XLINFO_TYPES]; RM_COUNT]),
        }
    }
}

// The body lives in the backend module; re-export so header call sites resolve.
pub use crate::backend::access::transam::xlogstats::{XLogRecGetLen, XLogRecStoreStats};
