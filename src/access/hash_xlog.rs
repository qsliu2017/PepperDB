//! Translated from PostgreSQL src/include/access/hash_xlog.h

use bitflags::bitflags;

use crate::access::xlogreader::XLogReaderState;
use crate::c::{RegProcedure, TransactionId};
use crate::lib::stringinfo::StringInfo;
use crate::storage::block::BlockNumber;
use crate::storage::off::OffsetNumber;

/// Number of buffers required for XLOG_HASH_SQUEEZE_PAGE operation.
pub const HASH_XLOG_FREE_OVFL_BUFS: i32 = 6;

// WAL opcodes (info nibble): raw consts, not a flag set.
pub const XLOG_HASH_INIT_META_PAGE: u8 = 0x00; // initialize the meta page
pub const XLOG_HASH_INIT_BITMAP_PAGE: u8 = 0x10; // initialize the bitmap page
pub const XLOG_HASH_INSERT: u8 = 0x20; // add index tuple without split
pub const XLOG_HASH_ADD_OVFL_PAGE: u8 = 0x30; // add overflow page
pub const XLOG_HASH_SPLIT_ALLOCATE_PAGE: u8 = 0x40; // allocate new page for split
pub const XLOG_HASH_SPLIT_PAGE: u8 = 0x50; // split page
pub const XLOG_HASH_SPLIT_COMPLETE: u8 = 0x60; // completion of split operation
pub const XLOG_HASH_MOVE_PAGE_CONTENTS: u8 = 0x70; // move tuples between pages
pub const XLOG_HASH_SQUEEZE_PAGE: u8 = 0x80; // add tuples to prior page, free ovfl
pub const XLOG_HASH_DELETE: u8 = 0x90; // delete index tuples from a page
pub const XLOG_HASH_SPLIT_CLEANUP: u8 = 0xA0; // clear split-cleanup flag
pub const XLOG_HASH_UPDATE_META_PAGE: u8 = 0xB0; // update meta page after vacuum
pub const XLOG_HASH_VACUUM_ONE_PAGE: u8 = 0xC0; // remove dead tuples from index page

bitflags! {
    /// xl_hash_split_allocate_page flag values; 8 bits available (single-bit set).
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct XlhSplit: u8 {
        const META_UPDATE_MASKS      = 1 << 0;
        const META_UPDATE_SPLITPOINT = 1 << 1;
    }
}

pub const XLH_SPLIT_META_UPDATE_MASKS: u8 = 1 << 0;
pub const XLH_SPLIT_META_UPDATE_SPLITPOINT: u8 = 1 << 1;

/// Simple (without split) insert. XLOG_HASH_INSERT.
#[repr(C)]
pub struct xl_hash_insert {
    pub offnum: OffsetNumber,
}
pub const SizeOfHashInsert: usize =
    core::mem::offset_of!(xl_hash_insert, offnum) + core::mem::size_of::<OffsetNumber>();

/// Addition of overflow page. XLOG_HASH_ADD_OVFL_PAGE.
#[repr(C)]
pub struct xl_hash_add_ovfl_page {
    pub bmsize: u16,
    pub bmpage_found: bool,
}
pub const SizeOfHashAddOvflPage: usize =
    core::mem::offset_of!(xl_hash_add_ovfl_page, bmpage_found) + core::mem::size_of::<bool>();

/// Allocating a page for split. XLOG_HASH_SPLIT_ALLOCATE_PAGE.
#[repr(C)]
pub struct xl_hash_split_allocate_page {
    pub new_bucket: u32,
    pub old_bucket_flag: u16,
    pub new_bucket_flag: u16,
    pub flags: u8,
}
pub const SizeOfHashSplitAllocPage: usize =
    core::mem::offset_of!(xl_hash_split_allocate_page, flags) + core::mem::size_of::<u8>();

/// Completing the split operation. XLOG_HASH_SPLIT_COMPLETE.
#[repr(C)]
pub struct xl_hash_split_complete {
    pub old_bucket_flag: u16,
    pub new_bucket_flag: u16,
}
pub const SizeOfHashSplitComplete: usize =
    core::mem::offset_of!(xl_hash_split_complete, new_bucket_flag) + core::mem::size_of::<u16>();

/// Move page contents (squeeze). XLOG_HASH_MOVE_PAGE_CONTENTS.
#[repr(C)]
pub struct xl_hash_move_page_contents {
    pub ntups: u16,
    /// true if destination page is the primary bucket page
    pub is_prim_bucket_same_wrt: bool,
}
pub const SizeOfHashMovePageContents: usize =
    core::mem::offset_of!(xl_hash_move_page_contents, is_prim_bucket_same_wrt)
        + core::mem::size_of::<bool>();

/// Squeeze page operation. XLOG_HASH_SQUEEZE_PAGE.
#[repr(C)]
pub struct xl_hash_squeeze_page {
    pub prevblkno: BlockNumber,
    pub nextblkno: BlockNumber,
    pub ntups: u16,
    /// true if destination page is the primary bucket page
    pub is_prim_bucket_same_wrt: bool,
    /// true if destination is the page previous to the freed overflow page
    pub is_prev_bucket_same_wrt: bool,
}
pub const SizeOfHashSqueezePage: usize =
    core::mem::offset_of!(xl_hash_squeeze_page, is_prev_bucket_same_wrt)
        + core::mem::size_of::<bool>();

/// Deletion of index tuples from a page. XLOG_HASH_DELETE.
#[repr(C)]
pub struct xl_hash_delete {
    /// true if this operation clears LH_PAGE_HAS_DEAD_TUPLES flag
    pub clear_dead_marking: bool,
    /// true if the operation is for primary bucket page
    pub is_primary_bucket_page: bool,
}
pub const SizeOfHashDelete: usize =
    core::mem::offset_of!(xl_hash_delete, is_primary_bucket_page) + core::mem::size_of::<bool>();

/// Metapage update. XLOG_HASH_UPDATE_META_PAGE.
#[repr(C)]
pub struct xl_hash_update_meta_page {
    pub ntuples: f64,
}
pub const SizeOfHashUpdateMetaPage: usize =
    core::mem::offset_of!(xl_hash_update_meta_page, ntuples) + core::mem::size_of::<f64>();

/// Initialize metapage. XLOG_HASH_INIT_META_PAGE.
#[repr(C)]
pub struct xl_hash_init_meta_page {
    pub num_tuples: f64,
    pub procid: RegProcedure,
    pub ffactor: u16,
}
pub const SizeOfHashInitMetaPage: usize =
    core::mem::offset_of!(xl_hash_init_meta_page, ffactor) + core::mem::size_of::<u16>();

/// Initialize bitmap page. XLOG_HASH_INIT_BITMAP_PAGE.
#[repr(C)]
pub struct xl_hash_init_bitmap_page {
    pub bmsize: u16,
}
pub const SizeOfHashInitBitmapPage: usize =
    core::mem::offset_of!(xl_hash_init_bitmap_page, bmsize) + core::mem::size_of::<u16>();

/// Index tuple deletion + meta page update. XLOG_HASH_VACUUM_ONE_PAGE.
/// Trailing FAM `offsets: [OffsetNumber]` lives in the WAL buffer after the header.
#[repr(C)]
pub struct xl_hash_vacuum_one_page {
    pub snapshotConflictHorizon: TransactionId,
    pub ntuples: u16,
    /// to handle recovery conflict during logical decoding on standby
    pub isCatalogRel: bool,
    // FAM: offsets: [OffsetNumber] (TARGET OFFSET NUMBERS)
}
pub const SizeOfHashVacuumOnePage: usize = core::mem::size_of::<xl_hash_vacuum_one_page>();

pub fn hash_redo(_record: &mut XLogReaderState) {
    unimplemented!()
}
pub fn hash_desc(_buf: &mut StringInfo, _record: &mut XLogReaderState) {
    unimplemented!()
}
pub fn hash_identify(_info: u8) -> Option<&'static str> {
    unimplemented!()
}
pub fn hash_mask(_pagedata: &mut [u8], _blkno: BlockNumber) {
    unimplemented!()
}
