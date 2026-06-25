//! Translated from PostgreSQL src/include/access/clog.h

use crate::access::xlogdefs::XLogRecPtr;
use crate::access::xlogreader::XLogReaderState;
use crate::c::TransactionId;
use crate::lib::stringinfo::StringInfo;
use crate::postgres_ext::Oid;
use crate::storage::sync::FileTag;

/// Transaction status: a sequential ordinal (0-3), not a flag set. All-zeroes is
/// the initial state. A "subcommitted" txn is a committed subtransaction whose
/// parent hasn't committed or aborted yet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum XidStatus {
    InProgress = 0x00,
    Committed = 0x01,
    Aborted = 0x02,
    SubCommitted = 0x03,
}

pub const TRANSACTION_STATUS_IN_PROGRESS: i32 = 0x00;
pub const TRANSACTION_STATUS_COMMITTED: i32 = 0x01;
pub const TRANSACTION_STATUS_ABORTED: i32 = 0x02;
pub const TRANSACTION_STATUS_SUB_COMMITTED: i32 = 0x03;

#[repr(C)]
pub struct xl_clog_truncate {
    pub pageno: i64,
    pub oldestXact: TransactionId,
    pub oldestXactDb: Oid,
}

pub fn TransactionIdSetTreeStatus(
    _xid: TransactionId,
    _subxids: &[TransactionId],
    _status: XidStatus,
    _lsn: XLogRecPtr,
) {
    unimplemented!()
}
/// Returns the status; the commit LSN is folded into the return tuple (out-param).
pub fn TransactionIdGetStatus(_xid: TransactionId) -> (XidStatus, XLogRecPtr) {
    unimplemented!()
}

pub fn CLOGShmemSize() -> usize {
    unimplemented!()
}
pub fn CLOGShmemInit() {
    unimplemented!()
}
pub fn BootStrapCLOG() {
    unimplemented!()
}
pub fn StartupCLOG() {
    unimplemented!()
}
pub fn TrimCLOG() {
    unimplemented!()
}
pub fn CheckPointCLOG() {
    unimplemented!()
}
pub fn ExtendCLOG(_newestXact: TransactionId) {
    unimplemented!()
}
pub fn TruncateCLOG(_oldestXact: TransactionId, _oldestxid_datoid: Oid) {
    unimplemented!()
}

pub fn clogsyncfiletag(_ftag: &FileTag, _path: &mut [u8]) -> i32 {
    unimplemented!()
}

// XLOG opcodes (info nibble): raw consts, not a flag set.
pub const CLOG_ZEROPAGE: u8 = 0x00;
pub const CLOG_TRUNCATE: u8 = 0x10;

pub fn clog_redo(_record: &mut XLogReaderState) {
    unimplemented!()
}
pub fn clog_desc(_buf: &mut StringInfo, _record: &mut XLogReaderState) {
    unimplemented!()
}
pub fn clog_identify(_info: u8) -> Option<&'static str> {
    unimplemented!()
}
