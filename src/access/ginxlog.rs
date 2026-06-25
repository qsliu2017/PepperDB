//! Translated from PostgreSQL src/include/access/ginxlog.h

use bitflags::bitflags;

use crate::access::ginblock::{GinMetaPageData, PostingItem};
use crate::access::itup::IndexTupleData;
use crate::access::xlogrecord::XLR_MAX_BLOCK_ID;
use crate::access::xlogreader::XLogReaderState;
use crate::c::TransactionId;
use crate::lib::stringinfo::StringInfo;
use crate::storage::block::BlockNumber;
use crate::storage::off::OffsetNumber;
use crate::storage::relfilelocator::RelFileLocator;

// WAL opcodes (info nibble): raw consts, not a flag set.
pub const XLOG_GIN_CREATE_PTREE: u8 = 0x10;
pub const XLOG_GIN_INSERT: u8 = 0x20;
pub const XLOG_GIN_SPLIT: u8 = 0x30;
pub const XLOG_GIN_VACUUM_PAGE: u8 = 0x40;
pub const XLOG_GIN_DELETE_PAGE: u8 = 0x50;
pub const XLOG_GIN_UPDATE_META_PAGE: u8 = 0x60;
pub const XLOG_GIN_INSERT_LISTPAGE: u8 = 0x70;
pub const XLOG_GIN_DELETE_LISTPAGE: u8 = 0x80;
pub const XLOG_GIN_VACUUM_DATA_LEAF_PAGE: u8 = 0x90;

#[repr(C)]
pub struct ginxlogCreatePostingTree {
    pub size: u32,
    // A compressed posting list follows.
}

/// Common part of every insertion record variant. A trailing FAM follows: child
/// block numbers (if not leaf) then a ginxlogInsertEntry/ginxlogRecompressDataLeaf.
#[repr(C)]
pub struct ginxlogInsert {
    pub flags: u16, // GIN_INSERT_ISLEAF and/or GIN_INSERT_ISDATA
}

#[repr(C)]
pub struct ginxlogInsertEntry {
    pub offset: OffsetNumber,
    pub isDelete: bool,
    pub tuple: IndexTupleData, // variable length
}

#[repr(C)]
pub struct ginxlogRecompressDataLeaf {
    pub nactions: u16,
    // Variable number of 'actions' follow.
}

/// Documentation-only WAL layout for a segment action (code uses raw Pointer +
/// memcpy). Action-specific data follows.
#[repr(C)]
pub struct ginxlogSegmentAction {
    pub segno: u8,
    pub r#type: i8, // action type (GIN_SEGMENT_*)
}

// Segment action types: sequential ordinals, not a flag set.
pub const GIN_SEGMENT_UNMODIFIED: i32 = 0; // no action (not used in WAL records)
pub const GIN_SEGMENT_DELETE: i32 = 1; // a whole segment is removed
pub const GIN_SEGMENT_INSERT: i32 = 2; // a whole segment is added
pub const GIN_SEGMENT_REPLACE: i32 = 3; // a segment is replaced
pub const GIN_SEGMENT_ADDITEMS: i32 = 4; // items are added to existing segment

#[repr(C)]
pub struct ginxlogInsertDataInternal {
    pub offset: OffsetNumber,
    pub newitem: PostingItem,
}

#[repr(C)]
pub struct ginxlogSplit {
    pub locator: RelFileLocator,
    pub rrlink: BlockNumber,          // right link, or root's blocknumber if root split
    pub leftChildBlkno: BlockNumber,  // valid on a non-leaf split
    pub rightChildBlkno: BlockNumber,
    pub flags: u16,
}

bitflags! {
    /// Flags used in ginxlogInsert and ginxlogSplit records (single-bit set).
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct GinXlogFlags: u16 {
        const INSERT_ISDATA = 0x01; // for both insert and split records
        const INSERT_ISLEAF = 0x02; // ditto
        const SPLIT_ROOT    = 0x04; // only for split records
    }
}

pub const GIN_INSERT_ISDATA: u16 = 0x01;
pub const GIN_INSERT_ISLEAF: u16 = 0x02;
pub const GIN_SPLIT_ROOT: u16 = 0x04;

#[repr(C)]
pub struct ginxlogVacuumDataLeafPage {
    pub data: ginxlogRecompressDataLeaf,
}

#[repr(C)]
pub struct ginxlogDeletePage {
    pub parentOffset: OffsetNumber,
    pub rightLink: BlockNumber,
    pub deleteXid: TransactionId, // last Xid which could see this page in scan
}

#[repr(C)]
pub struct ginxlogUpdateMeta {
    pub locator: RelFileLocator,
    pub metadata: GinMetaPageData,
    pub prevTail: BlockNumber,
    pub newRightlink: BlockNumber,
    pub ntuples: i32, // >0: metadata.tail updated with that many tuples; else new sublist inserted
    // array of inserted tuples follows
}

#[repr(C)]
pub struct ginxlogInsertListPage {
    pub rightlink: BlockNumber,
    pub ntuples: i32,
    // array of inserted tuples follows
}

/// Max list pages deletable in one record (each needs a block reference).
pub const GIN_NDELETE_AT_ONCE: i32 = {
    let a = 16i32;
    let b = (XLR_MAX_BLOCK_ID as i32) - 1;
    if a < b { a } else { b }
};

#[repr(C)]
pub struct ginxlogDeleteListPages {
    pub metadata: GinMetaPageData,
    pub ndeleted: i32,
}

pub fn gin_redo(_record: &mut XLogReaderState) {
    unimplemented!()
}
pub fn gin_desc(_buf: &mut StringInfo, _record: &mut XLogReaderState) {
    unimplemented!()
}
pub fn gin_identify(_info: u8) -> Option<&'static str> {
    unimplemented!()
}
pub fn gin_xlog_startup() {
    unimplemented!()
}
pub fn gin_xlog_cleanup() {
    unimplemented!()
}
pub fn gin_mask(_pagedata: &mut [u8], _blkno: BlockNumber) {
    unimplemented!()
}
