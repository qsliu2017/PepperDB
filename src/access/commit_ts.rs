//! Translated from PostgreSQL src/include/access/commit_ts.h
//! PostgreSQL commit timestamp manager.

use crate::access::xlogdefs::RepOriginId;
use crate::access::xlogreader::XLogReaderState;
use crate::c::TransactionId;
use crate::datatype::timestamp::TimestampTz;
use crate::lib::stringinfo::StringInfo;
use crate::storage::sync::FileTag;

// GUC variable (process-global in C; becomes session/global state later).
pub static mut track_commit_timestamp: bool = false;

pub fn TransactionTreeSetCommitTsData(
    _xid: TransactionId,
    _subxids: &[TransactionId],
    _timestamp: TimestampTz,
    _nodeid: RepOriginId,
) {
    unimplemented!()
}

/// C: bool + (TimestampTz*, RepOriginId*) out-params -> Option of the pair.
pub fn TransactionIdGetCommitTsData(_xid: TransactionId) -> Option<(TimestampTz, RepOriginId)> {
    unimplemented!()
}

/// C fills *ts and *nodeid out-params and returns the latest xid.
pub fn GetLatestCommitTsData() -> (TransactionId, TimestampTz, RepOriginId) {
    unimplemented!()
}

pub fn CommitTsShmemSize() -> usize {
    unimplemented!()
}
pub fn CommitTsShmemInit() {
    unimplemented!()
}
pub fn BootStrapCommitTs() {
    unimplemented!()
}
pub fn StartupCommitTs() {
    unimplemented!()
}
pub fn CommitTsParameterChange(_newvalue: bool, _oldvalue: bool) {
    unimplemented!()
}
pub fn CompleteCommitTsInitialization() {
    unimplemented!()
}
pub fn CheckPointCommitTs() {
    unimplemented!()
}
pub fn ExtendCommitTs(_newest_xact: TransactionId) {
    unimplemented!()
}
pub fn TruncateCommitTs(_oldest_xact: TransactionId) {
    unimplemented!()
}
pub fn SetCommitTsLimit(_oldest_xact: TransactionId, _newest_xact: TransactionId) {
    unimplemented!()
}
pub fn AdvanceOldestCommitTsXid(_oldest_xact: TransactionId) {
    unimplemented!()
}

/// C writes the path into a caller buffer and returns a status int.
pub fn committssyncfiletag(_ftag: &FileTag, _path: &mut String) -> i32 {
    unimplemented!()
}

// XLOG stuff: opcodes in the high nibble of xl_info (raw, not a flag set).
pub const COMMIT_TS_ZEROPAGE: u8 = 0x00;
pub const COMMIT_TS_TRUNCATE: u8 = 0x10;

/// WAL record: set commit timestamp for a transaction tree (on-disk).
/// Trailing subxact Xids follow the fixed part.
#[repr(C)]
pub struct xl_commit_ts_set {
    pub timestamp: TimestampTz,
    pub nodeid: RepOriginId,
    pub mainxid: TransactionId,
    // subxact Xids follow
}
/// offsetof(xl_commit_ts_set, mainxid) + sizeof(TransactionId).
pub const SizeOfCommitTsSet: usize =
    core::mem::offset_of!(xl_commit_ts_set, mainxid) + core::mem::size_of::<TransactionId>();

/// WAL record: truncate the commit-ts SLRU (on-disk).
#[repr(C)]
pub struct xl_commit_ts_truncate {
    pub pageno: i64,
    pub oldestXid: TransactionId,
}
/// offsetof(xl_commit_ts_truncate, oldestXid) + sizeof(TransactionId).
pub const SizeOfCommitTsTruncate: usize =
    core::mem::offset_of!(xl_commit_ts_truncate, oldestXid) + core::mem::size_of::<TransactionId>();

pub fn commit_ts_redo(_record: &mut XLogReaderState) {
    unimplemented!()
}
pub fn commit_ts_desc(_buf: &mut StringInfo, _record: &mut XLogReaderState) {
    unimplemented!()
}
pub fn commit_ts_identify(_info: u8) -> &'static str {
    unimplemented!()
}
