//! Translated from PostgreSQL src/include/access/ginblock.h
//! Structures stored in GIN index blocks. On-disk page layouts.

use bitflags::bitflags;
use crate::storage::block::{BlockIdData, BlockNumber};
use crate::storage::itemptr::ItemPointerData;
use crate::storage::off::OffsetNumber;

/// Page opaque data in a GIN page. On-disk; exactly 8 bytes (no page ID word).
#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct GinPageOpaqueData {
    pub rightlink: BlockNumber, // next page if any
    pub maxoff: OffsetNumber,   // count of PostingItems / heap tuples (page-kind dependent)
    pub flags: u16,             // see GinPageFlags below
}

const _: () = assert!(core::mem::size_of::<GinPageOpaqueData>() == 8);

bitflags! {
    /// GIN page flags (clean single-bit set, on-disk but byte-identical).
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct GinPageFlags: u16 {
        const DATA            = 1 << 0;
        const LEAF            = 1 << 1;
        const DELETED         = 1 << 2;
        const META            = 1 << 3;
        const LIST            = 1 << 4;
        const LIST_FULLROW    = 1 << 5; // only meaningful on GIN_LIST page
        const INCOMPLETE_SPLIT = 1 << 6; // page split, parent not yet updated
        const COMPRESSED      = 1 << 7;
    }
}

// Page numbers of fixed-location pages.
pub const GIN_METAPAGE_BLKNO: BlockNumber = 0;
pub const GIN_ROOT_BLKNO: BlockNumber = 1;

/// GIN metapage layout. On-disk.
#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct GinMetaPageData {
    pub head: BlockNumber, // head of pending list (GIN_LIST pages)
    pub tail: BlockNumber, // tail of pending list
    pub tailFreeSize: u32, // free space in bytes in the pending list's tail page
    pub nPendingPages: BlockNumber,
    pub nPendingHeapTuples: i64,
    pub nTotalPages: BlockNumber,
    pub nEntryPages: BlockNumber,
    pub nDataPages: BlockNumber,
    pub nEntries: i64,
    pub ginVersion: i32, // do not move: must stay last for on-disk compat
}

pub const GIN_CURRENT_VERSION: i32 = 2;

/// We may reclaim a deleted page only once every transaction started before its
/// deletion is over. `page` is a raw page buffer (PG `Page`).
pub fn GinPageIsRecyclable(_page: &[u8]) -> bool {
    unimplemented!()
}

/// Special-case item pointer offset for "max" (sorts after any valid pointer).
pub const GIN_MAX_OFFSET: OffsetNumber = 0xffff;

/// Posting item in a non-leaf posting-tree page. On-disk; uses BlockIdData (not
/// BlockNumber) to avoid padding-space waste.
#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct PostingItem {
    pub child_blkno: BlockIdData,
    pub key: ItemPointerData,
}

impl PostingItem {
    pub const fn block_number(&self) -> BlockNumber {
        self.child_blkno.block_number()
    }
    pub const fn set_block_number(&mut self, block_number: BlockNumber) {
        self.child_blkno.set(block_number);
    }
}

/// Category codes distinguishing placeholder nulls from ordinary NULL keys.
/// Signed-char ordinal (POOR for bitflags); kept as a typed i8 with consts.
pub type GinNullCategory = i8;

pub const GIN_CAT_NORM_KEY: GinNullCategory = 0; // normal, non-null key value
pub const GIN_CAT_NULL_KEY: GinNullCategory = 1; // null key value
pub const GIN_CAT_EMPTY_ITEM: GinNullCategory = 2; // placeholder for zero-key item
pub const GIN_CAT_NULL_ITEM: GinNullCategory = 3; // placeholder for null item
pub const GIN_CAT_EMPTY_QUERY: GinNullCategory = -1; // placeholder for full-scan query

/// Posting-tree leaf marker stored in the entry tuple's offset field.
pub const GIN_TREE_POSTING: OffsetNumber = 0xffff;

// Packs a flag (high bit) beside an offset *number* in the entry tuple's block
// field: not a flag set (appendix C shape), kept as a raw mask.
pub const GIN_ITUP_COMPRESSED: u32 = 1 << 31;

/// A compressed posting list. On-disk; requires 2-byte alignment. `bytes` is a
/// flexible array member (varbyte-encoded items); the fixed header is modeled
/// here and the trailing data is a slice over the buffer (length = nbytes).
#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct GinPostingList {
    pub first: ItemPointerData, // first item in this posting list (unpacked)
    pub nbytes: u16,            // number of bytes that follow
    // unsigned char bytes[FLEXIBLE_ARRAY_MEMBER]
}

impl GinPostingList {
    /// offsetof(GinPostingList, bytes) + SHORTALIGN(nbytes). SHORTALIGN = round
    /// up to 2 on the targets.
    pub const fn size_of(&self) -> usize {
        let short_aligned = (self.nbytes as usize + 1) & !1;
        core::mem::size_of::<GinPostingList>() + short_aligned
    }
}
