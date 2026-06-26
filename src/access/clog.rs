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

// Definitions live in transam/clog.c; re-exported here (rules s2). The clog
// set/get + maintenance ops became inherent methods on the CLOG `SlruCtl`
// (`shared.clog().set_tree_status(...)` etc., refactor14); they cannot be
// `pub use`d, and all callers use the methods, so only the remaining free fns
// are re-exported.
pub use crate::backend::access::transam::clog::{clog_identify, clogsyncfiletag};

/// clog.c CLOGShmemSize (estimate under the Arc model).
pub fn CLOGShmemSize(nbuffers: usize) -> usize {
    crate::backend::access::transam::clog::clog_shmem_size(nbuffers)
}

// XLOG opcodes (info nibble): raw consts, not a flag set.
pub const CLOG_ZEROPAGE: u8 = 0x00;
pub const CLOG_TRUNCATE: u8 = 0x10;

/// clog.c clog_redo: deferred to recovery (out of foundation).
pub fn clog_redo(_record: &mut XLogReaderState) {
    crate::backend::access::transam::clog::clog_redo()
}

/// clog.c clog_desc: format a clog WAL record (recovery tooling, deferred).
pub fn clog_desc(_buf: &mut StringInfo, _record: &mut XLogReaderState) {
    // TODO(recovery): rmgrdesc formatting needs the decoded record.
}
