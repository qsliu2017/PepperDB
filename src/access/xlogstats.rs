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
pub struct XLogStats {
    pub count: u64,
    pub startptr: XLogRecPtr,
    pub endptr: XLogRecPtr,
    pub rmgr_stats: [XLogRecStats; RM_COUNT],
    pub record_stats: [[XLogRecStats; MAX_XLINFO_TYPES]; RM_COUNT],
}

/// Returns (rec_len, fpi_len) (out-params folded into a tuple).
pub fn XLogRecGetLen(_record: &mut XLogReaderState) -> (u32, u32) {
    unimplemented!()
}
pub fn XLogRecStoreStats(_stats: &mut XLogStats, _record: &mut XLogReaderState) {
    unimplemented!()
}
