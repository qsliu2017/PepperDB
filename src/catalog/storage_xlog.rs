//! Translated from PostgreSQL src/include/catalog/storage_xlog.h

use bitflags::bitflags;

use crate::common::relpath::ForkNumber;
use crate::storage::block::BlockNumber;
use crate::storage::relfilelocator::RelFileLocator;

// XLOG opcodes (xl_info high 4 bits) -- sequential WAL record types, raw consts.
pub const XLOG_SMGR_CREATE: u8 = 0x10;
pub const XLOG_SMGR_TRUNCATE: u8 = 0x20;

/// On-disk WAL record for smgr file creation.
#[repr(C)]
pub struct xl_smgr_create {
    pub rlocator: RelFileLocator,
    pub forkNum: ForkNumber,
}

const _: () = assert!(core::mem::offset_of!(xl_smgr_create, rlocator) == 0);
const _: () = assert!(core::mem::offset_of!(xl_smgr_create, forkNum) == 12);

bitflags! {
    /// Flags for `xl_smgr_truncate` (single-bit set with composite `ALL`).
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct SmgrTruncate: i32 {
        const HEAP = 0x0001;
        const VM   = 0x0002;
        const FSM  = 0x0004;
        const ALL  = Self::HEAP.bits() | Self::VM.bits() | Self::FSM.bits();
    }
}

/// On-disk WAL record for smgr truncation.
#[repr(C)]
pub struct xl_smgr_truncate {
    pub blkno: BlockNumber,
    pub rlocator: RelFileLocator,
    pub flags: i32,
}

const _: () = assert!(core::mem::offset_of!(xl_smgr_truncate, blkno) == 0);
const _: () = assert!(core::mem::offset_of!(xl_smgr_truncate, rlocator) == 4);
const _: () = assert!(core::mem::offset_of!(xl_smgr_truncate, flags) == 16);

pub fn log_smgrcreate(rlocator: &RelFileLocator, fork_num: ForkNumber) {
    unimplemented!()
}

// XLogReaderState is a later-level type; StringInfo -> &mut String (stringinfo tombstone).
pub fn smgr_redo(record: &mut crate::access::xlogreader::XLogReaderState) {
    unimplemented!()
}

pub fn smgr_desc(buf: &mut String, record: &mut crate::access::xlogreader::XLogReaderState) {
    unimplemented!()
}

pub fn smgr_identify(info: u8) -> Option<&'static str> {
    unimplemented!()
}
