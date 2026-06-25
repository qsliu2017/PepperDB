//! Translated from PostgreSQL src/include/access/nbtxlog.h

use crate::access::transam::FullTransactionId;
use crate::access::xlogreader::XLogReaderState;
use crate::c::TransactionId;
use crate::lib::stringinfo::StringInfo;
use crate::storage::block::BlockNumber;
use crate::storage::off::OffsetNumber;
use crate::storage::relfilelocator::RelFileLocator;

// btree WAL opcodes (info high nibble): raw consts.
pub const XLOG_BTREE_INSERT_LEAF: u8 = 0x00; // add index tuple without split
pub const XLOG_BTREE_INSERT_UPPER: u8 = 0x10; // same, on a non-leaf page
pub const XLOG_BTREE_INSERT_META: u8 = 0x20; // same, plus update metapage
pub const XLOG_BTREE_SPLIT_L: u8 = 0x30; // add index tuple with split
pub const XLOG_BTREE_SPLIT_R: u8 = 0x40; // as above, new item on right
pub const XLOG_BTREE_INSERT_POST: u8 = 0x50; // add index tuple with posting split
pub const XLOG_BTREE_DEDUP: u8 = 0x60; // deduplicate tuples for a page
pub const XLOG_BTREE_DELETE: u8 = 0x70; // delete leaf index tuples for a page
pub const XLOG_BTREE_UNLINK_PAGE: u8 = 0x80; // delete a half-dead page
pub const XLOG_BTREE_UNLINK_PAGE_META: u8 = 0x90; // same, and update metapage
pub const XLOG_BTREE_NEWROOT: u8 = 0xA0; // new root page
pub const XLOG_BTREE_MARK_PAGE_HALFDEAD: u8 = 0xB0; // mark a leaf as half-dead
pub const XLOG_BTREE_VACUUM: u8 = 0xC0; // delete entries on a page during vacuum
pub const XLOG_BTREE_REUSE_PAGE: u8 = 0xD0; // old page about to be reused from FSM
pub const XLOG_BTREE_META_CLEANUP: u8 = 0xE0; // update cleanup data in metapage

/// All that we need to regenerate the meta-data page.
#[repr(C)]
pub struct xl_btree_metadata {
    pub version: u32,
    pub root: BlockNumber,
    pub level: u32,
    pub fastroot: BlockNumber,
    pub fastlevel: u32,
    pub last_cleanup_num_delpages: u32,
    pub allequalimage: bool,
}

/// Simple (without split) insert. INSERT_LEAF/UPPER/META/POST.
#[repr(C)]
pub struct xl_btree_insert {
    pub offnum: OffsetNumber,
    // POSTING SPLIT OFFSET FOLLOWS (INSERT_POST); NEW TUPLE ALWAYS FOLLOWS AT END
}
pub const SizeOfBtreeInsert: usize =
    core::mem::offset_of!(xl_btree_insert, offnum) + core::mem::size_of::<OffsetNumber>();

/// Insert with split (shared by SPLIT_L and SPLIT_R). Backup blk 0: new left page.
#[repr(C)]
pub struct xl_btree_split {
    pub level: u32,                // tree level of page being split
    pub firstrightoff: OffsetNumber, // first origpage item on rightpage
    pub newitemoff: OffsetNumber,  // new item's offset
    pub postingoff: u16,           // offset inside orig posting tuple
}
pub const SizeOfBtreeSplit: usize =
    core::mem::offset_of!(xl_btree_split, postingoff) + core::mem::size_of::<u16>();

/// Deduplication pass for a leaf page. BTDedupInterval array follows.
#[repr(C)]
pub struct xl_btree_dedup {
    pub nintervals: u16,
    // DEDUPLICATION INTERVALS FOLLOW
}
pub const SizeOfBtreeDedup: usize =
    core::mem::offset_of!(xl_btree_dedup, nintervals) + core::mem::size_of::<u16>();

/// Page reuse within btree (generates a Hot Standby conflict point).
#[repr(C)]
pub struct xl_btree_reuse_page {
    pub locator: RelFileLocator,
    pub block: BlockNumber,
    pub snapshotConflictHorizon: FullTransactionId,
    pub isCatalogRel: bool, // recovery conflict during logical decoding on standby
}
pub const SizeOfBtreeReusePage: usize =
    core::mem::offset_of!(xl_btree_reuse_page, isCatalogRel) + core::mem::size_of::<bool>();

/// VACUUM deletion of index tuples on a leaf page.
#[repr(C)]
pub struct xl_btree_vacuum {
    pub ndeleted: u16,
    pub nupdated: u16,
    // blk 0 payload: deleted offsets, updated offsets, xl_btree_update items
}
pub const SizeOfBtreeVacuum: usize =
    core::mem::offset_of!(xl_btree_vacuum, nupdated) + core::mem::size_of::<u16>();

/// Ad-hoc deletion of index tuples on a leaf page (with conflict info).
#[repr(C)]
pub struct xl_btree_delete {
    pub snapshotConflictHorizon: TransactionId,
    pub ndeleted: u16,
    pub nupdated: u16,
    pub isCatalogRel: bool, // recovery conflict during logical decoding on standby
    // blk 0 payload: deleted offsets, updated offsets, xl_btree_update items
}
pub const SizeOfBtreeDelete: usize =
    core::mem::offset_of!(xl_btree_delete, isCatalogRel) + core::mem::size_of::<bool>();

/// Metadata for an "updated" (subset-deleted) posting list tuple. The TID
/// offsets following are 0-based into the original posting list.
#[repr(C)]
pub struct xl_btree_update {
    pub ndeletedtids: u16,
    // POSTING LIST uint16 OFFSETS TO A DELETED TID FOLLOW
}
pub const SizeOfBtreeUpdate: usize =
    core::mem::offset_of!(xl_btree_update, ndeletedtids) + core::mem::size_of::<u16>();

/// Marking an empty subtree for deletion. Backup blk 0: leaf; blk 1: top parent.
#[repr(C)]
pub struct xl_btree_mark_page_halfdead {
    pub poffset: OffsetNumber,  // deleted tuple id in parent page
    pub leafblk: BlockNumber,   // leaf block ultimately being deleted
    pub leftblk: BlockNumber,   // leaf block's left sibling, if any
    pub rightblk: BlockNumber,  // leaf block's right sibling
    pub topparent: BlockNumber, // topmost internal page in the subtree
}
pub const SizeOfBtreeMarkPageHalfDead: usize =
    core::mem::offset_of!(xl_btree_mark_page_halfdead, topparent)
        + core::mem::size_of::<BlockNumber>();

/// Deletion of a btree page. xl_btree_metadata FOLLOWS if UNLINK_PAGE_META.
#[repr(C)]
pub struct xl_btree_unlink_page {
    pub leftsib: BlockNumber,        // target block's left sibling, if any
    pub rightsib: BlockNumber,       // target block's right sibling
    pub level: u32,                  // target block's level
    pub safexid: FullTransactionId,  // target block's BTPageSetDeleted() XID
    // Used only when target page is internal (recreate half-dead leaf):
    pub leafleftsib: BlockNumber,
    pub leafrightsib: BlockNumber,
    pub leaftopparent: BlockNumber, // next child down in the subtree
}
pub const SizeOfBtreeUnlinkPage: usize =
    core::mem::offset_of!(xl_btree_unlink_page, leaftopparent)
        + core::mem::size_of::<BlockNumber>();

/// New root log record. Zero or two tuples on the new root page.
#[repr(C)]
pub struct xl_btree_newroot {
    pub rootblk: BlockNumber, // location of new root (redundant with blk 0)
    pub level: u32,           // its tree level
}
pub const SizeOfBtreeNewroot: usize =
    core::mem::offset_of!(xl_btree_newroot, level) + core::mem::size_of::<u32>();

// functions in nbtxlog.c
pub fn btree_redo(_record: &mut XLogReaderState) {
    unimplemented!()
}
pub fn btree_xlog_startup() {
    unimplemented!()
}
pub fn btree_xlog_cleanup() {
    unimplemented!()
}
pub fn btree_mask(_pagedata: &mut [u8], _blkno: BlockNumber) {
    unimplemented!()
}

// functions in nbtdesc.c
pub fn btree_desc(_buf: &mut StringInfo, _record: &mut XLogReaderState) {
    unimplemented!()
}
pub fn btree_identify(_info: u8) -> Option<&'static str> {
    unimplemented!()
}
