//! Translated from PostgreSQL src/include/access/brin_xlog.h

use crate::access::xlogreader::XLogReaderState;
use crate::lib::stringinfo::StringInfo;
use crate::storage::block::BlockNumber;
use crate::storage::off::OffsetNumber;

// WAL record opcodes for BRIN: stored in the high 4 bits of info. Raw consts
// (an opcode nibble, not a flag set).
pub const XLOG_BRIN_CREATE_INDEX: u8 = 0x00;
pub const XLOG_BRIN_INSERT: u8 = 0x10;
pub const XLOG_BRIN_UPDATE: u8 = 0x20;
pub const XLOG_BRIN_SAMEPAGE_UPDATE: u8 = 0x30;
pub const XLOG_BRIN_REVMAP_EXTEND: u8 = 0x40;
pub const XLOG_BRIN_DESUMMARIZE: u8 = 0x50;

pub const XLOG_BRIN_OPMASK: u8 = 0x70;
/// When inserting the first item on a new page, restore the entire page in redo.
pub const XLOG_BRIN_INIT_PAGE: u8 = 0x80;

/// BRIN index create. Backup block 0: metapage.
#[repr(C)]
pub struct xl_brin_createidx {
    pub pagesPerRange: BlockNumber,
    pub version: u16,
}
const _: () = assert!(core::mem::offset_of!(xl_brin_createidx, version) == 4);
pub const SizeOfBrinCreateIdx: usize =
    core::mem::offset_of!(xl_brin_createidx, version) + core::mem::size_of::<u16>();

/// BRIN tuple insert. Backup block 0: main page (new BrinTuple); block 1: revmap.
#[repr(C)]
pub struct xl_brin_insert {
    pub heapBlk: BlockNumber,
    /// extra information needed to update the revmap
    pub pagesPerRange: BlockNumber,
    /// offset number in the main page to insert the tuple to
    pub offnum: OffsetNumber,
}
const _: () = assert!(core::mem::offset_of!(xl_brin_insert, offnum) == 8);
pub const SizeOfBrinInsert: usize =
    core::mem::offset_of!(xl_brin_insert, offnum) + core::mem::size_of::<OffsetNumber>();

/// Cross-page update: like an insert plus the old tuple's location.
#[repr(C)]
pub struct xl_brin_update {
    /// offset number of old tuple on old page
    pub oldOffnum: OffsetNumber,
    pub insert: xl_brin_insert,
}
pub const SizeOfBrinUpdate: usize =
    core::mem::offset_of!(xl_brin_update, insert) + SizeOfBrinInsert;

/// BRIN tuple samepage update. Backup block 0: updated page (new BrinTuple).
#[repr(C)]
pub struct xl_brin_samepage_update {
    pub offnum: OffsetNumber,
}
pub const SizeOfBrinSamepageUpdate: usize = core::mem::size_of::<OffsetNumber>();

/// Revmap extension. Backup block 0: metapage; block 1: new revmap page.
#[repr(C)]
pub struct xl_brin_revmap_extend {
    /// XXX redundant - block number is stored as part of backup block 1.
    pub targetBlk: BlockNumber,
}
pub const SizeOfBrinRevmapExtend: usize =
    core::mem::offset_of!(xl_brin_revmap_extend, targetBlk) + core::mem::size_of::<BlockNumber>();

/// Range de-summarization. Backup block 0: revmap page; block 1: regular page.
#[repr(C)]
pub struct xl_brin_desummarize {
    pub pagesPerRange: BlockNumber,
    /// page number location to set to invalid
    pub heapBlk: BlockNumber,
    /// offset of item to delete in regular index page
    pub regOffset: OffsetNumber,
}
const _: () = assert!(core::mem::offset_of!(xl_brin_desummarize, regOffset) == 8);
pub const SizeOfBrinDesummarize: usize =
    core::mem::offset_of!(xl_brin_desummarize, regOffset) + core::mem::size_of::<OffsetNumber>();

pub fn brin_redo(_record: &mut XLogReaderState) {
    unimplemented!()
}
pub fn brin_desc(_buf: &mut StringInfo, _record: &mut XLogReaderState) {
    unimplemented!()
}
pub fn brin_identify(_info: u8) -> Option<&'static str> {
    unimplemented!()
}
pub fn brin_mask(_pagedata: &mut [u8], _blkno: BlockNumber) {
    unimplemented!()
}
