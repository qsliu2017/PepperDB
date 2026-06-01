//! storage/standbydefs.h - Frontend exposed definitions for hot standby mode.

use std::ffi::c_int;

use crate::access::transam::xlogreader::XLogReaderState;
use crate::c::{uint8, FLEXIBLE_ARRAY_MEMBER, TransactionId};
use crate::lib::stringinfo::StringInfo;
use crate::postgres_ext::Oid;
use crate::storage::lockdefs::xl_standby_lock;

// SharedInvalidationMessage canonically lives in storage/sinval.h, which has not
// landed yet. Use a minimal stub here.
// TODO: dedup when sinval.h lands (also defined in access/rmgrdesc/standbydesc.rs).
pub type SharedInvalidationMessage = std::ffi::c_void;

/* Recovery handlers for the Standby Rmgr (RM_STANDBY_ID) */
pub unsafe fn standby_redo(record: *mut XLogReaderState) {
    unimplemented!()
}

pub unsafe fn standby_desc(buf: StringInfo, record: *mut XLogReaderState) {
    unimplemented!()
}

pub unsafe fn standby_identify(info: uint8) -> *const std::ffi::c_char {
    unimplemented!()
}

pub unsafe fn standby_desc_invalidations(
    buf: StringInfo,
    nmsgs: c_int,
    msgs: *mut SharedInvalidationMessage,
    dbId: Oid,
    tsId: Oid,
    relcacheInitFileInval: bool,
) {
    unimplemented!()
}

/*
 * XLOG message types
 */
pub const XLOG_STANDBY_LOCK: c_int = 0x00;
pub const XLOG_RUNNING_XACTS: c_int = 0x10;
pub const XLOG_INVALIDATIONS: c_int = 0x20;

#[repr(C)]
pub struct xl_standby_locks {
    pub nlocks: c_int, /* number of entries in locks array */
    pub locks: [xl_standby_lock; FLEXIBLE_ARRAY_MEMBER],
}

/*
 * When we write running xact data to WAL, we use this structure.
 */
#[repr(C)]
pub struct xl_running_xacts {
    pub xcnt: c_int,             /* # of xact ids in xids[] */
    pub subxcnt: c_int,          /* # of subxact ids in xids[] */
    pub subxid_overflow: bool,   /* snapshot overflowed, subxids missing */
    pub nextXid: TransactionId,  /* xid from TransamVariables->nextXid */
    pub oldestRunningXid: TransactionId, /* *not* oldestXmin */
    pub latestCompletedXid: TransactionId, /* so we can set xmax */

    pub xids: [TransactionId; FLEXIBLE_ARRAY_MEMBER],
}

/*
 * Invalidations for standby, currently only when transactions without an
 * assigned xid commit.
 */
#[repr(C)]
pub struct xl_invalidations {
    pub dbId: Oid,                   /* MyDatabaseId */
    pub tsId: Oid,                   /* MyDatabaseTableSpace */
    pub relcacheInitFileInval: bool, /* invalidate relcache init files */
    pub nmsgs: c_int,                /* number of shared inval msgs */
    pub msgs: [SharedInvalidationMessage; FLEXIBLE_ARRAY_MEMBER],
}

// #define MinSizeOfInvalidations offsetof(xl_invalidations, msgs)
#[inline]
pub fn MinSizeOfInvalidations() -> usize {
    // offsetof(xl_invalidations, msgs)
    let m = std::mem::MaybeUninit::<xl_invalidations>::uninit();
    let base = m.as_ptr() as usize;
    unsafe { std::ptr::addr_of!((*m.as_ptr()).msgs) as usize - base }
}
