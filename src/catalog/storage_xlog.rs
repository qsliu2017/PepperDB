//! catalog/storage_xlog.h - prototypes for XLog support for backend/catalog/storage.c

use std::ffi::c_char;
use std::ffi::c_int;

use crate::c::uint8;
use crate::lib::stringinfo::StringInfo;
use crate::storage::block::BlockNumber;
use crate::common::relpath::ForkNumber;
// RelFileLocator and XLogReaderState canonical defs currently live in xlogreader.
// TODO: dedup when storage/relfilelocator.h and access/xlogreader.h land properly.
use crate::access::transam::xlogreader::RelFileLocator;
use crate::access::transam::xlogreader::XLogReaderState;

/*
 * Declarations for smgr-related XLOG records
 *
 * Note: we log file creation and truncation here, but logging of deletion
 * actions is handled by xact.c, because it is part of transaction commit.
 */

/* XLOG gives us high 4 bits */
pub const XLOG_SMGR_CREATE: uint8 = 0x10;
pub const XLOG_SMGR_TRUNCATE: uint8 = 0x20;

#[repr(C)]
pub struct xl_smgr_create {
    pub rlocator: RelFileLocator,
    pub forkNum: ForkNumber,
}

/* flags for xl_smgr_truncate */
pub const SMGR_TRUNCATE_HEAP: c_int = 0x0001;
pub const SMGR_TRUNCATE_VM: c_int = 0x0002;
pub const SMGR_TRUNCATE_FSM: c_int = 0x0004;
pub const SMGR_TRUNCATE_ALL: c_int =
    SMGR_TRUNCATE_HEAP | SMGR_TRUNCATE_VM | SMGR_TRUNCATE_FSM;

#[repr(C)]
pub struct xl_smgr_truncate {
    pub blkno: BlockNumber,
    pub rlocator: RelFileLocator,
    pub flags: c_int,
}

pub unsafe fn log_smgrcreate(rlocator: *const RelFileLocator, forkNum: ForkNumber) {
    unimplemented!()
}

pub unsafe fn smgr_redo(record: *mut XLogReaderState) {
    unimplemented!()
}

pub unsafe fn smgr_desc(buf: StringInfo, record: *mut XLogReaderState) {
    unimplemented!()
}

pub unsafe fn smgr_identify(info: uint8) -> *const c_char {
    unimplemented!()
}
