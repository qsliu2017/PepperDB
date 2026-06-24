//! Translated from PostgreSQL src/include/access/gistxlog.h
//! GiST WAL (xlog) routines and record layouts.

use crate::access::gist::GistNSN;
use crate::access::transam::FullTransactionId;
use crate::access::xlogreader::XLogReaderState;
use crate::c::TransactionId;
use crate::lib::stringinfo::StringInfo;
use crate::storage::block::BlockNumber;
use crate::storage::off::OffsetNumber;
use crate::storage::relfilelocator::RelFileLocator;

// WAL opcodes: high nibble of xl_info. Raw consts (opcode, not a flag set).
pub const XLOG_GIST_PAGE_UPDATE: u8 = 0x00;
pub const XLOG_GIST_DELETE: u8 = 0x10; // delete leaf index tuples for a page
pub const XLOG_GIST_PAGE_REUSE: u8 = 0x20; // old page about to be reused from FSM
pub const XLOG_GIST_PAGE_SPLIT: u8 = 0x30;
// 0x40 (INSERT_COMPLETE) and 0x50 (CREATE_INDEX) not used anymore
pub const XLOG_GIST_PAGE_DELETE: u8 = 0x60;
pub const XLOG_GIST_ASSIGN_LSN: u8 = 0x70; // nop, assign new LSN

/// Backup Blk 0: updated page. Blk 1: left half of split (if completing a split).
#[repr(C)]
pub struct gistxlogPageUpdate {
    pub ntodelete: u16, // number of deleted offsets
    pub ntoinsert: u16,
    // payload of blk 0: todelete OffsetNumbers, then tuples to insert
}

/// Backup Blk 0: leaf page whose index tuples are deleted. On-disk.
#[repr(C)]
pub struct gistxlogDelete {
    pub snapshotConflictHorizon: TransactionId,
    pub ntodelete: u16,     // number of deleted offsets
    pub isCatalogRel: bool, // recovery conflict during logical decoding on standby
    // OffsetNumber offsets[FLEXIBLE_ARRAY_MEMBER]
}
/// offsetof(gistxlogDelete, offsets) -- the fixed part size.
pub const SizeOfGistxlogDelete: usize = core::mem::size_of::<gistxlogDelete>();

/// Backup Blk 0: left half of split (if completing one). Blk 1..npage: split pages.
#[repr(C)]
pub struct gistxlogPageSplit {
    pub origrlink: BlockNumber, // rightlink of the page before split
    pub orignsn: GistNSN,       // NSN of the page before split
    pub origleaf: bool,         // was split page a leaf page?
    pub npage: u16,             // # of pages in the split
    pub markfollowright: bool,  // set F_FOLLOW_RIGHT flags
    // follows: gistxlogPage and array of IndexTupleData per page
}

/// Backup Blk 0: deleted page. Blk 1: parent page with the downlink. On-disk.
#[repr(C)]
pub struct gistxlogPageDelete {
    pub deleteXid: FullTransactionId, // last Xid which could see page in scan
    pub downlinkOffset: OffsetNumber, // offset of downlink referencing this page
}
/// offsetof(gistxlogPageDelete, downlinkOffset) + sizeof(OffsetNumber).
pub const SizeOfGistxlogPageDelete: usize =
    core::mem::offset_of!(gistxlogPageDelete, downlinkOffset) + core::mem::size_of::<OffsetNumber>();

/// What we need to know about page reuse, for hot standby. On-disk.
#[repr(C)]
pub struct gistxlogPageReuse {
    pub locator: RelFileLocator,
    pub block: BlockNumber,
    pub snapshotConflictHorizon: FullTransactionId,
    pub isCatalogRel: bool, // recovery conflict during logical decoding on standby
}
/// offsetof(gistxlogPageReuse, isCatalogRel) + sizeof(bool).
pub const SizeOfGistxlogPageReuse: usize =
    core::mem::offset_of!(gistxlogPageReuse, isCatalogRel) + core::mem::size_of::<bool>();

pub fn gist_redo(_record: &mut XLogReaderState) {
    unimplemented!()
}
pub fn gist_desc(_buf: &mut StringInfo, _record: &mut XLogReaderState) {
    unimplemented!()
}
pub fn gist_identify(_info: u8) -> &'static str {
    unimplemented!()
}
pub fn gist_xlog_startup() {
    unimplemented!()
}
pub fn gist_xlog_cleanup() {
    unimplemented!()
}
pub fn gist_mask(_pagedata: &mut [u8], _blkno: BlockNumber) {
    unimplemented!()
}
