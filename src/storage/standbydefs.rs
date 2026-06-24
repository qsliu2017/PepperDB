//! Translated from PostgreSQL src/include/storage/standbydefs.h
//!
//! Frontend exposed definitions for hot standby mode.

use crate::access::xlogreader::XLogReaderState;
use crate::c::TransactionId;
use crate::postgres_ext::Oid;
use crate::storage::sinval::SharedInvalidationMessage;

// The FAM tail of `xl_standby_locks` is `[xl_standby_lock]`
// (crate::storage::lockdefs::xl_standby_lock); only the fixed header is typed here.

/* Recovery handlers for the Standby Rmgr (RM_STANDBY_ID) */
pub fn standby_redo(_record: &mut XLogReaderState) {
    unimplemented!()
}
pub fn standby_desc(_buf: &mut String, _record: &mut XLogReaderState) {
    unimplemented!()
}
pub fn standby_identify(_info: u8) -> &'static str {
    unimplemented!()
}
pub fn standby_desc_invalidations(
    _buf: &mut String,
    _msgs: &[SharedInvalidationMessage],
    _db_id: Oid,
    _ts_id: Oid,
    _relcache_init_file_inval: bool,
) {
    unimplemented!()
}

/* XLOG message types (xl_info nibble opcodes, kept raw) */
pub const XLOG_STANDBY_LOCK: u8 = 0x00;
pub const XLOG_RUNNING_XACTS: u8 = 0x10;
pub const XLOG_INVALIDATIONS: u8 = 0x20;

/// On-disk WAL record. Fixed header; `locks[nlocks]` FAM tail lives in the buffer.
#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct xl_standby_locks {
    pub nlocks: i32, // number of entries in locks array
                     // xl_standby_lock locks[FLEXIBLE_ARRAY_MEMBER]
}
const _: () = assert!(core::mem::size_of::<xl_standby_locks>() == 4);

/// On-disk WAL record written for running xact data.
/// Fixed header; `xids[xcnt + subxcnt]` FAM tail lives in the buffer.
#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct xl_running_xacts {
    pub xcnt: i32,             // # of xact ids in xids[]
    pub subxcnt: i32,          // # of subxact ids in xids[]
    pub subxid_overflow: bool, // snapshot overflowed, subxids missing
    #[allow(deprecated)]
    pub next_xid: TransactionId, // xid from TransamVariables->nextXid
    #[allow(deprecated)]
    pub oldest_running_xid: TransactionId, // *not* oldestXmin
    #[allow(deprecated)]
    pub latest_completed_xid: TransactionId, // so we can set xmax
                               // TransactionId xids[FLEXIBLE_ARRAY_MEMBER]
}
const _: () = assert!(core::mem::offset_of!(xl_running_xacts, xcnt) == 0);
const _: () = assert!(core::mem::offset_of!(xl_running_xacts, subxcnt) == 4);
const _: () = assert!(core::mem::offset_of!(xl_running_xacts, next_xid) == 12);

/// On-disk WAL record for standby invalidations.
/// Fixed header; `msgs[nmsgs]` FAM tail lives in the buffer.
#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct xl_invalidations {
    pub db_id: Oid,                    // MyDatabaseId
    pub ts_id: Oid,                    // MyDatabaseTableSpace
    pub relcache_init_file_inval: bool, // invalidate relcache init files
    pub nmsgs: i32,                    // number of shared inval msgs
                                       // SharedInvalidationMessage msgs[FLEXIBLE_ARRAY_MEMBER]
}
const _: () = assert!(core::mem::offset_of!(xl_invalidations, db_id) == 0);
const _: () = assert!(core::mem::offset_of!(xl_invalidations, nmsgs) == 12);

/// offsetof(xl_invalidations, msgs) -- size of the fixed part.
pub const MIN_SIZE_OF_INVALIDATIONS: usize = 16;
