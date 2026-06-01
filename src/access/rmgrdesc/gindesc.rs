//! gindesc.rs
//!   rmgr descriptor routines for access/transam/gin/ginxlog.c
//!
//! 1:1 translation of src/backend/access/rmgrdesc/gindesc.c, merged with the
//! `ginxlog*` WAL-record struct definitions, the `XLOG_GIN_*` opcode constants,
//! and the `GIN_INSERT_*` / `GIN_SPLIT_*` / `GIN_SEGMENT_*` flag constants from
//! src/include/access/ginxlog.h. The few supporting on-disk structs the record
//! layouts embed (PostingItem, GinMetaPageData, GinPostingList) are merged in
//! from src/include/access/ginblock.h with their real layouts.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::prelude::*;
use crate::appendStringInfo; // crate-root #[macro_export] macro
use crate::lib::stringinfo::{appendStringInfoString, StringInfo};
use crate::storage::off::OffsetNumber;
use crate::storage::block::{BlockIdData, BlockIdGetBlockNumber, BlockNumber};
use crate::storage::itemptr::{
    ItemPointerData, ItemPointerGetBlockNumber, ItemPointerGetOffsetNumber,
};
use crate::access::common::indextuple::IndexTupleData;
use crate::common::blkreftable::RelFileLocator;
use crate::access::transam::xlogreader::{
    XLogReaderState, XLogRecBlockImageApply, XLogRecGetBlockData, XLogRecGetData, XLogRecGetInfo,
    XLogRecHasBlockImage, XLR_INFO_MASK,
};

// ---------------------------------------------------------------------------
// XLOG record opcodes for GIN (ginxlog.h)
// ---------------------------------------------------------------------------

pub const XLOG_GIN_CREATE_PTREE: uint8 = 0x10;
pub const XLOG_GIN_INSERT: uint8 = 0x20;
pub const XLOG_GIN_SPLIT: uint8 = 0x30;
pub const XLOG_GIN_VACUUM_PAGE: uint8 = 0x40;
pub const XLOG_GIN_VACUUM_DATA_LEAF_PAGE: uint8 = 0x90;
pub const XLOG_GIN_DELETE_PAGE: uint8 = 0x50;
pub const XLOG_GIN_UPDATE_META_PAGE: uint8 = 0x60;
pub const XLOG_GIN_INSERT_LISTPAGE: uint8 = 0x70;
pub const XLOG_GIN_DELETE_LISTPAGE: uint8 = 0x80;

// ---------------------------------------------------------------------------
// Flags used in ginxlogInsert and ginxlogSplit records (ginxlog.h)
// ---------------------------------------------------------------------------

pub const GIN_INSERT_ISDATA: uint16 = 0x01; // for both insert and split records
pub const GIN_INSERT_ISLEAF: uint16 = 0x02; // ditto
pub const GIN_SPLIT_ROOT: uint16 = 0x04; // only for split records

// ---------------------------------------------------------------------------
// Segment action types within a ginxlogRecompressDataLeaf payload (ginxlog.h)
// ---------------------------------------------------------------------------

pub const GIN_SEGMENT_UNMODIFIED: uint8 = 0; // no action (not used in WAL records)
pub const GIN_SEGMENT_DELETE: uint8 = 1; // a whole segment is removed
pub const GIN_SEGMENT_INSERT: uint8 = 2; // a whole segment is added
pub const GIN_SEGMENT_REPLACE: uint8 = 3; // a segment is replaced
pub const GIN_SEGMENT_ADDITEMS: uint8 = 4; // items are added to existing segment

// ---------------------------------------------------------------------------
// Supporting on-disk structs merged from access/ginblock.h (real layouts).
// ---------------------------------------------------------------------------

/// Posting item in a non-leaf posting-tree page (ginblock.h).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct PostingItem {
    /// We use BlockIdData not BlockNumber to avoid padding space wastage.
    pub child_blkno: BlockIdData,
    pub key: ItemPointerData,
}

/// `PostingItemGetBlockNumber` - block number of a PostingItem's child.
#[inline]
pub unsafe fn PostingItemGetBlockNumber(pointer: *const PostingItem) -> BlockNumber {
    BlockIdGetBlockNumber(&(*pointer).child_blkno)
}

/// GIN metapage contents (ginblock.h). Embedded by ginxlogUpdateMeta and
/// ginxlogDeleteListPages.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct GinMetaPageData {
    pub head: BlockNumber,
    pub tail: BlockNumber,
    pub tailFreeSize: uint32,
    pub nPendingPages: BlockNumber,
    pub nPendingHeapTuples: int64,
    pub nTotalPages: BlockNumber,
    pub nEntryPages: BlockNumber,
    pub nDataPages: BlockNumber,
    pub nEntries: int64,
    pub ginVersion: int32,
}

/// A compressed posting list (ginblock.h). Requires 2-byte alignment. The
/// trailing `bytes[FLEXIBLE_ARRAY_MEMBER]` is omitted (accessed by offset).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct GinPostingList {
    pub first: ItemPointerData, // first item in this posting list (unpacked)
    pub nbytes: uint16,         // number of bytes that follow
    // unsigned char bytes[FLEXIBLE_ARRAY_MEMBER] varbyte-encoded items follow
}

/// `SizeOfGinPostingList` - offsetof(GinPostingList, bytes) + SHORTALIGN(nbytes).
/// offsetof(bytes) is sizeof(first) + sizeof(nbytes) with no trailing padding
/// (2-byte aligned), i.e. the size of the fixed part of the struct.
#[inline]
pub unsafe fn SizeOfGinPostingList(plist: *const GinPostingList) -> usize {
    let header = core::mem::size_of::<ItemPointerData>() + core::mem::size_of::<uint16>();
    header + SHORTALIGN((*plist).nbytes as usize)
}

/// SHORTALIGN - round up to a 2-byte (sizeof short) boundary.
#[inline]
fn SHORTALIGN(len: usize) -> usize {
    const ALIGNOF_SHORT: usize = 2;
    (len + (ALIGNOF_SHORT - 1)) & !(ALIGNOF_SHORT - 1)
}

// ---------------------------------------------------------------------------
// WAL record structs (ginxlog.h) -- real layouts. Flexible-array / trailing
// variable payloads are omitted from the Rust structs (accessed by offset).
// ---------------------------------------------------------------------------

/// XLOG_GIN_CREATE_PTREE payload header; a compressed posting list follows.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ginxlogCreatePostingTree {
    pub size: uint32,
    // A compressed posting list follows
}

/// Common part of the insertion record (varies by page type).
///
/// Backup Blk 0: target page
/// Backup Blk 1: left child, if this insertion finishes an incomplete split
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ginxlogInsert {
    pub flags: uint16, // GIN_INSERT_ISLEAF and/or GIN_INSERT_ISDATA
    // FOLLOWS: optional BlockIdData[2] (non-leaf), then a ginxlogInsertEntry
    // or ginxlogRecompressDataLeaf depending on tree type.
}

/// Entry-tree insertion payload (follows ginxlogInsert). Variable length.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ginxlogInsertEntry {
    pub offset: OffsetNumber,
    pub isDelete: bool,
    pub tuple: IndexTupleData, // variable length
}

/// Recompressed data-leaf payload header (follows ginxlogInsert for leaf data
/// pages). A variable number of segment actions follow.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ginxlogRecompressDataLeaf {
    pub nactions: uint16,
    // Variable number of 'actions' follow
}

/// Internal (non-leaf) data-tree insertion payload.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ginxlogInsertDataInternal {
    pub offset: OffsetNumber,
    pub newitem: PostingItem,
}

/// XLOG_GIN_SPLIT record.
///
/// Backup Blk 0: new left page (= original page, if not root split)
/// Backup Blk 1: new right page
/// Backup Blk 2: original page / new root page, if root split
/// Backup Blk 3: left child, if this insertion completes an earlier split
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ginxlogSplit {
    pub locator: RelFileLocator,
    pub rrlink: BlockNumber,          // right link, or root's blockno if root split
    pub leftChildBlkno: BlockNumber,  // valid on a non-leaf split
    pub rightChildBlkno: BlockNumber,
    pub flags: uint16,                // see GIN_INSERT_* / GIN_SPLIT_*
}

/// XLOG_GIN_VACUUM_DATA_LEAF_PAGE record.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ginxlogVacuumDataLeafPage {
    pub data: ginxlogRecompressDataLeaf,
}

/// XLOG_GIN_DELETE_PAGE record.
///
/// Backup Blk 0: deleted page
/// Backup Blk 1: parent
/// Backup Blk 2: left sibling
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ginxlogDeletePage {
    pub parentOffset: OffsetNumber,
    pub rightLink: BlockNumber,
    pub deleteXid: TransactionId, // last Xid which could see this page in scan
}

/// XLOG_GIN_UPDATE_META_PAGE record.
///
/// Backup Blk 0: metapage
/// Backup Blk 1: tail page
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ginxlogUpdateMeta {
    pub locator: RelFileLocator,
    pub metadata: GinMetaPageData,
    pub prevTail: BlockNumber,
    pub newRightlink: BlockNumber,
    pub ntuples: int32, // >0: metadata.tail updated with that many tuples; else new sub list inserted
    // array of inserted tuples follows
}

/// XLOG_GIN_INSERT_LISTPAGE record.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ginxlogInsertListPage {
    pub rightlink: BlockNumber,
    pub ntuples: int32,
    // array of inserted tuples follows
}

/// XLOG_GIN_DELETE_LISTPAGE record.
///
/// Backup Blk 0: metapage
/// Backup Blk 1 to (ndeleted + 1): deleted pages
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ginxlogDeleteListPages {
    pub metadata: GinMetaPageData,
    pub ndeleted: int32,
}

// ---------------------------------------------------------------------------
// desc_recompress_leaf
// ---------------------------------------------------------------------------

/// Format the segment actions that follow a `ginxlogRecompressDataLeaf` header.
///
/// # Safety
/// `insert_data` must point to a valid `ginxlogRecompressDataLeaf` followed by
/// `nactions` correctly-encoded segment actions.
unsafe fn desc_recompress_leaf(buf: StringInfo, insert_data: *mut ginxlogRecompressDataLeaf) {
    let mut walbuf = (insert_data as *mut c_char).add(core::mem::size_of::<ginxlogRecompressDataLeaf>());

    let nactions = (*insert_data).nactions;
    appendStringInfo!(buf, " {} segments:", nactions as c_int);

    for _ in 0..nactions {
        let a_segno = *(walbuf as *mut uint8);
        walbuf = walbuf.add(1);
        let a_action = *(walbuf as *mut uint8);
        walbuf = walbuf.add(1);
        let mut nitems: uint16 = 0;

        if a_action == GIN_SEGMENT_INSERT || a_action == GIN_SEGMENT_REPLACE {
            let newsegsize = SizeOfGinPostingList(walbuf as *const GinPostingList);
            walbuf = walbuf.add(SHORTALIGN(newsegsize));
        }

        if a_action == GIN_SEGMENT_ADDITEMS {
            core::ptr::copy_nonoverlapping(
                walbuf as *const u8,
                &mut nitems as *mut uint16 as *mut u8,
                core::mem::size_of::<uint16>(),
            );
            walbuf = walbuf.add(core::mem::size_of::<uint16>());
            walbuf = walbuf.add(nitems as usize * core::mem::size_of::<ItemPointerData>());
        }

        match a_action {
            GIN_SEGMENT_ADDITEMS => {
                appendStringInfo!(buf, " {} (add {} items)", a_segno, nitems);
            }
            GIN_SEGMENT_DELETE => {
                appendStringInfo!(buf, " {} (delete)", a_segno);
            }
            GIN_SEGMENT_INSERT => {
                appendStringInfo!(buf, " {} (insert)", a_segno);
            }
            GIN_SEGMENT_REPLACE => {
                appendStringInfo!(buf, " {} (replace)", a_segno);
            }
            _ => {
                appendStringInfo!(buf, " {} unknown action {} ???", a_segno, a_action);
                // cannot decode unrecognized actions further
                return;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// gin_desc
// ---------------------------------------------------------------------------

/// # Safety
/// `record` must be a valid decoded XLOG record whose rmgr is GIN and whose
/// payload matches the opcode in its info byte. `buf` must be a writable
/// StringInfo.
pub unsafe fn gin_desc(buf: StringInfo, record: *mut XLogReaderState) {
    let rec = XLogRecGetData(record);
    let info: uint8 = XLogRecGetInfo(record) & !XLR_INFO_MASK;

    match info {
        XLOG_GIN_CREATE_PTREE => {
            // no further information
        }
        XLOG_GIN_INSERT => {
            let xlrec = &*(rec as *const ginxlogInsert);

            appendStringInfo!(
                buf,
                "isdata: {} isleaf: {}",
                if xlrec.flags & GIN_INSERT_ISDATA != 0 { 'T' } else { 'F' },
                if xlrec.flags & GIN_INSERT_ISLEAF != 0 { 'T' } else { 'F' }
            );
            if xlrec.flags & GIN_INSERT_ISLEAF == 0 {
                let mut payload = rec.add(core::mem::size_of::<ginxlogInsert>());

                let leftChildBlkno = BlockIdGetBlockNumber(payload as *const BlockIdData);
                payload = payload.add(core::mem::size_of::<BlockIdData>());
                let rightChildBlkno = BlockIdGetBlockNumber(payload as *const BlockIdData);
                payload = payload.add(core::mem::size_of::<BlockNumber>());
                let _ = payload;
                appendStringInfo!(buf, " children: {}/{}", leftChildBlkno, rightChildBlkno);
            }
            if XLogRecHasBlockImage(record, 0) {
                if XLogRecBlockImageApply(record, 0) {
                    appendStringInfoString(buf, c" (full page image)".as_ptr());
                } else {
                    appendStringInfoString(
                        buf,
                        c" (full page image, for WAL verification)".as_ptr(),
                    );
                }
            } else {
                let payload = XLogRecGetBlockData(record, 0, null_mut());

                if xlrec.flags & GIN_INSERT_ISDATA == 0 {
                    appendStringInfo!(
                        buf,
                        " isdelete: {}",
                        if (*(payload as *const ginxlogInsertEntry)).isDelete { 'T' } else { 'F' }
                    );
                } else if xlrec.flags & GIN_INSERT_ISLEAF != 0 {
                    desc_recompress_leaf(buf, payload as *mut ginxlogRecompressDataLeaf);
                } else {
                    let insert_data = &*(payload as *const ginxlogInsertDataInternal);

                    appendStringInfo!(
                        buf,
                        " pitem: {}-{}/{}",
                        PostingItemGetBlockNumber(&insert_data.newitem),
                        ItemPointerGetBlockNumber(&insert_data.newitem.key),
                        ItemPointerGetOffsetNumber(&insert_data.newitem.key)
                    );
                }
            }
        }
        XLOG_GIN_SPLIT => {
            let xlrec = &*(rec as *const ginxlogSplit);

            appendStringInfo!(
                buf,
                "isrootsplit: {}",
                if xlrec.flags & GIN_SPLIT_ROOT != 0 { 'T' } else { 'F' }
            );
            appendStringInfo!(
                buf,
                " isdata: {} isleaf: {}",
                if xlrec.flags & GIN_INSERT_ISDATA != 0 { 'T' } else { 'F' },
                if xlrec.flags & GIN_INSERT_ISLEAF != 0 { 'T' } else { 'F' }
            );
        }
        XLOG_GIN_VACUUM_PAGE => {
            // no further information
        }
        XLOG_GIN_VACUUM_DATA_LEAF_PAGE => {
            if XLogRecHasBlockImage(record, 0) {
                if XLogRecBlockImageApply(record, 0) {
                    appendStringInfoString(buf, c" (full page image)".as_ptr());
                } else {
                    appendStringInfoString(
                        buf,
                        c" (full page image, for WAL verification)".as_ptr(),
                    );
                }
            } else {
                let xlrec =
                    XLogRecGetBlockData(record, 0, null_mut()) as *mut ginxlogVacuumDataLeafPage;

                desc_recompress_leaf(buf, &mut (*xlrec).data);
            }
        }
        XLOG_GIN_DELETE_PAGE => {
            // no further information
        }
        XLOG_GIN_UPDATE_META_PAGE => {
            // no further information
        }
        XLOG_GIN_INSERT_LISTPAGE => {
            // no further information
        }
        XLOG_GIN_DELETE_LISTPAGE => {
            appendStringInfo!(
                buf,
                "ndeleted: {}",
                (*(rec as *const ginxlogDeleteListPages)).ndeleted
            );
        }
        _ => {}
    }
}

// ---------------------------------------------------------------------------
// gin_identify
// ---------------------------------------------------------------------------

/// Return a static name for the GIN WAL opcode in `info`, or NULL if the opcode
/// is unrecognized (mirrors the C `const char *` contract).
pub fn gin_identify(info: uint8) -> *const c_char {
    let mut id: *const c_char = null();

    match info & !XLR_INFO_MASK {
        XLOG_GIN_CREATE_PTREE => id = c"CREATE_PTREE".as_ptr(),
        XLOG_GIN_INSERT => id = c"INSERT".as_ptr(),
        XLOG_GIN_SPLIT => id = c"SPLIT".as_ptr(),
        XLOG_GIN_VACUUM_PAGE => id = c"VACUUM_PAGE".as_ptr(),
        XLOG_GIN_VACUUM_DATA_LEAF_PAGE => id = c"VACUUM_DATA_LEAF_PAGE".as_ptr(),
        XLOG_GIN_DELETE_PAGE => id = c"DELETE_PAGE".as_ptr(),
        XLOG_GIN_UPDATE_META_PAGE => id = c"UPDATE_META_PAGE".as_ptr(),
        XLOG_GIN_INSERT_LISTPAGE => id = c"INSERT_LISTPAGE".as_ptr(),
        XLOG_GIN_DELETE_LISTPAGE => id = c"DELETE_LISTPAGE".as_ptr(),
        _ => {}
    }

    id
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::ffi::CStr;

    unsafe fn id_str(info: uint8) -> Option<&'static str> {
        let p = gin_identify(info);
        if p.is_null() {
            None
        } else {
            Some(CStr::from_ptr(p).to_str().unwrap())
        }
    }

    #[test]
    fn identify_create_ptree() {
        unsafe {
            assert_eq!(id_str(XLOG_GIN_CREATE_PTREE), Some("CREATE_PTREE"));
        }
    }

    #[test]
    fn identify_all_opcodes() {
        unsafe {
            assert_eq!(id_str(XLOG_GIN_INSERT), Some("INSERT"));
            assert_eq!(id_str(XLOG_GIN_SPLIT), Some("SPLIT"));
            assert_eq!(id_str(XLOG_GIN_VACUUM_PAGE), Some("VACUUM_PAGE"));
            assert_eq!(
                id_str(XLOG_GIN_VACUUM_DATA_LEAF_PAGE),
                Some("VACUUM_DATA_LEAF_PAGE")
            );
            assert_eq!(id_str(XLOG_GIN_DELETE_PAGE), Some("DELETE_PAGE"));
            assert_eq!(id_str(XLOG_GIN_UPDATE_META_PAGE), Some("UPDATE_META_PAGE"));
            assert_eq!(id_str(XLOG_GIN_INSERT_LISTPAGE), Some("INSERT_LISTPAGE"));
            assert_eq!(id_str(XLOG_GIN_DELETE_LISTPAGE), Some("DELETE_LISTPAGE"));
        }
    }

    #[test]
    fn identify_masks_info_low_bits() {
        // The low XLR_INFO_MASK bits must be ignored.
        unsafe {
            assert_eq!(id_str(XLOG_GIN_INSERT | XLR_INFO_MASK), Some("INSERT"));
        }
    }

    #[test]
    fn identify_unknown_returns_null() {
        unsafe {
            assert_eq!(id_str(0x00), None);
        }
    }

    #[test]
    fn struct_size_sanity() {
        // GinMetaPageData: the two int64 fields are NOT contiguous (they're
        // interspersed with BlockNumbers), so each forces 8-byte alignment with
        // internal padding -> 56 under repr(C) (same as C's natural alignment).
        assert_eq!(core::mem::size_of::<GinMetaPageData>(), 56);
        // ginxlogRecompressDataLeaf is just a uint16 header.
        assert_eq!(core::mem::size_of::<ginxlogRecompressDataLeaf>(), 2);
        // ginxlogCreatePostingTree is a single uint32.
        assert_eq!(core::mem::size_of::<ginxlogCreatePostingTree>(), 4);
    }
}
