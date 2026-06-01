//! nbtxlog.c
//!   WAL replay logic for btrees.
//!
//! Translated 1:1 from postgres/src/backend/access/nbtree/nbtxlog.c
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!   src/backend/access/nbtree/nbtxlog.c
//!
//! #include mapping:
//!   "postgres.h"             -> crate::prelude::*
//!   "access/bufmask.h"       -> crate::access::common::bufmask::*
//!   "access/nbtree.h"        -> BTPageOpaque/BTMetaPageData/BTreeTuple*/_bt_pageinit
//!                               (stubs below; real home access/nbtree.h, TODO(pg-port))
//!   "access/nbtxlog.h"       -> xl_btree_* structs + XLOG_BTREE_* opcodes
//!                               (crate::access::rmgrdesc::nbtdesc)
//!   "access/transam.h"       -> FullTransactionId (crate::access::transam)
//!   "access/xlogutils.h"     -> XLogReadBufferForRedo/XLogInitBufferForRedo
//!                               (crate::access::transam::xlogutils)
//!   "storage/standby.h"      -> InHotStandby/ResolveRecoveryConflictWith* (stubs)
//!   "utils/memutils.h"       -> AllocSetContextCreate / ALLOCSET_DEFAULT_SIZES
//!                               (crate::prelude via AllocSetContextCreate! macro)

#![allow(unused_variables)]
#![allow(unused_mut)]
#![allow(dead_code)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use crate::prelude::*;
use crate::AllocSetContextCreate;

use std::ffi::{c_char, c_int, c_void};

use crate::c::{uint8, uint16, uint32, Size};

// xl_btree_* record structs and XLOG_BTREE_* opcodes all live in nbtdesc.
use crate::access::rmgrdesc::nbtdesc::{
    xl_btree_dedup, xl_btree_delete, xl_btree_insert, xl_btree_mark_page_halfdead,
    xl_btree_metadata, xl_btree_newroot, xl_btree_reuse_page, xl_btree_split, xl_btree_unlink_page,
    xl_btree_update, xl_btree_vacuum, RelFileLocator, SizeOfBtreeUpdate,
    XLOG_BTREE_DELETE, XLOG_BTREE_DEDUP, XLOG_BTREE_INSERT_LEAF, XLOG_BTREE_INSERT_META,
    XLOG_BTREE_INSERT_POST, XLOG_BTREE_INSERT_UPPER, XLOG_BTREE_MARK_PAGE_HALFDEAD,
    XLOG_BTREE_META_CLEANUP, XLOG_BTREE_NEWROOT, XLOG_BTREE_REUSE_PAGE, XLOG_BTREE_SPLIT_L,
    XLOG_BTREE_SPLIT_R, XLOG_BTREE_UNLINK_PAGE, XLOG_BTREE_UNLINK_PAGE_META, XLOG_BTREE_VACUUM,
};

use crate::access::common::bufmask::{
    mask_lp_flags, mask_page_hint_bits, mask_page_lsn_and_checksum, mask_unused_space,
};
use crate::access::transam::xlogdefs::XLogRecPtr;
use crate::access::transam::xlogreader::{
    XLogReaderState, XLogRecGetBlockTag, XLogRecGetBlockTagExtended, XLogRecGetData,
    XLogRecGetInfo, XLogRecHasBlockRef, XLR_INFO_MASK,
};
use crate::access::transam::xlogutils::{
    XLogInitBufferForRedo, XLogReadBufferForRedo, XLogReadBufferForRedoExtended, XLogRedoAction,
    BLK_NEEDS_REDO, RBM_NORMAL,
};
use crate::access::transam::FullTransactionId;
use crate::storage::block::{BlockNumber, InvalidBlockNumber};
use crate::storage::buf::{Buffer, InvalidBuffer};
use crate::storage::bufpage::{
    Page, PageAddItem, PageGetItem, PageGetItemId, PageGetMaxOffsetNumber, PageHeader, PageSetLSN,
};
use crate::storage::item::Item;
use crate::storage::itemid::{ItemId, ItemIdGetLength};
use crate::storage::off::{InvalidOffsetNumber, OffsetNumber, OffsetNumberNext, OffsetNumberPrev};
use crate::utils::palloc::MemoryContext;

// Nbtree sibling module re-exports for dedup structs already ported in nbtdedup.rs.
use crate::access::nbtree::nbtdedup::{
    BTDedupInterval, BTDedupState, BTDedupStateData, BTPageOpaque, BTPageOpaqueData,
    BTVacuumPosting, BTVacuumPostingData, BTP_HAS_GARBAGE, BTMaxItemSize, MaxIndexTuplesPerPage,
    P_HIKEY, _bt_dedup_finish_pending, _bt_dedup_save_htid, _bt_dedup_start_pending,
    _bt_swap_posting, _bt_update_posting,
};
use crate::access::common::indextuple::{CopyIndexTuple, IndexTuple, IndexTupleData, IndexTupleSize};

// Working memory context for replay operations (equiv. of C static opCtx).
static mut opCtx: MemoryContext = std::ptr::null_mut();

// ---------------------------------------------------------------------------
// Local stubs for symbols not yet translated.
// ---------------------------------------------------------------------------

/// TODO(pg-port): BTMetaPageData lives in access/nbtree.h.
#[repr(C)]
pub struct BTMetaPageData {
    pub btm_magic: uint32,
    pub btm_version: uint32,
    pub btm_root: BlockNumber,
    pub btm_level: uint32,
    pub btm_fastroot: BlockNumber,
    pub btm_fastlevel: uint32,
    pub btm_last_cleanup_num_delpages: uint32,
    pub btm_last_cleanup_num_heap_tuples: f64,
    pub btm_allequalimage: bool,
}

// nbtree.h page-flag constants.
/// TODO(pg-port): BTP_META from access/nbtree.h.
pub const BTP_META: u16 = 1 << 1;
/// TODO(pg-port): BTP_ROOT from access/nbtree.h.
pub const BTP_ROOT: u16 = 1 << 3;
/// TODO(pg-port): BTP_LEAF from access/nbtree.h.
pub const BTP_LEAF: u16 = 1 << 0;
/// TODO(pg-port): BTP_INCOMPLETE_SPLIT from access/nbtree.h.
pub const BTP_INCOMPLETE_SPLIT: u16 = 1 << 7;
/// TODO(pg-port): BTP_HALF_DEAD from access/nbtree.h.
pub const BTP_HALF_DEAD: u16 = 1 << 4;
/// TODO(pg-port): BTP_SPLIT_END from access/nbtree.h.
pub const BTP_SPLIT_END: u16 = 1 << 5;

// nbtree.h metapage constants.
/// TODO(pg-port): BTREE_METAPAGE from access/nbtree.h.
pub const BTREE_METAPAGE: BlockNumber = 0;
/// TODO(pg-port): BTREE_MAGIC from access/nbtree.h.
pub const BTREE_MAGIC: uint32 = 0x053162;
/// TODO(pg-port): BTREE_NOVAC_VERSION from access/nbtree.h.
pub const BTREE_NOVAC_VERSION: uint32 = 4;

/// TODO(pg-port): P_NONE from access/nbtree.h.
pub const P_NONE: BlockNumber = 0;

/// TODO(pg-port): storage/standby.h -- true only during Hot Standby replay.
const InHotStandby: bool = false;

/// TODO(pg-port): BTPageGetMeta() from access/nbtree.h.
#[allow(non_snake_case)]
unsafe fn BTPageGetMeta(page: Page) -> *mut BTMetaPageData {
    unimplemented!() // TODO(pg-port): access/nbtree.h
}

/// TODO(pg-port): _bt_pageinit() from nbtpage.c (access/nbtree.h).
#[allow(non_snake_case)]
unsafe fn _bt_pageinit(page: Page, size: Size) {
    unimplemented!() // TODO(pg-port): access/nbtree.h (nbtpage.c)
}

/// TODO(pg-port): BTPageSetDeleted() from access/nbtree.h.
#[allow(non_snake_case)]
unsafe fn BTPageSetDeleted(page: Page, safexid: FullTransactionId) {
    unimplemented!() // TODO(pg-port): access/nbtree.h
}

/// TODO(pg-port): BTreeTupleSetTopParent() from access/nbtree.h.
#[allow(non_snake_case)]
unsafe fn BTreeTupleSetTopParent(itup: *mut IndexTupleData, blkno: BlockNumber) {
    unimplemented!() // TODO(pg-port): access/nbtree.h
}

/// TODO(pg-port): BTreeTupleGetDownLink() from access/nbtree.h.
#[allow(non_snake_case)]
unsafe fn BTreeTupleGetDownLink(itup: IndexTuple) -> BlockNumber {
    unimplemented!() // TODO(pg-port): access/nbtree.h
}

/// TODO(pg-port): BTreeTupleSetDownLink() from access/nbtree.h.
#[allow(non_snake_case)]
unsafe fn BTreeTupleSetDownLink(itup: IndexTuple, blkno: BlockNumber) {
    unimplemented!() // TODO(pg-port): access/nbtree.h
}

/// TODO(pg-port): P_FIRSTDATAKEY() from access/nbtree.h (also in nbtdedup.rs stub).
#[allow(non_snake_case)]
unsafe fn P_FIRSTDATAKEY(opaque: BTPageOpaque) -> OffsetNumber {
    unimplemented!() // TODO(pg-port): access/nbtree.h
}

/// TODO(pg-port): BTPageGetOpaque() from access/nbtree.h (also in nbtdedup.rs stub).
#[allow(non_snake_case)]
unsafe fn BTPageGetOpaque(page: Page) -> BTPageOpaque {
    unimplemented!() // TODO(pg-port): access/nbtree.h
}

/// TODO(pg-port): P_RIGHTMOST() from access/nbtree.h.
#[allow(non_snake_case)]
unsafe fn P_RIGHTMOST(opaque: BTPageOpaque) -> bool {
    unimplemented!() // TODO(pg-port): access/nbtree.h
}

/// TODO(pg-port): P_ISLEAF() from access/nbtree.h.
#[allow(non_snake_case)]
unsafe fn P_ISLEAF(opaque: BTPageOpaque) -> bool {
    unimplemented!() // TODO(pg-port): access/nbtree.h
}

/// TODO(pg-port): P_INCOMPLETE_SPLIT() from access/nbtree.h.
#[allow(non_snake_case)]
unsafe fn P_INCOMPLETE_SPLIT(opaque: BTPageOpaque) -> bool {
    unimplemented!() // TODO(pg-port): access/nbtree.h
}

/// TODO(pg-port): P_HAS_GARBAGE() from access/nbtree.h.
#[allow(non_snake_case)]
unsafe fn P_HAS_GARBAGE(opaque: BTPageOpaque) -> bool {
    unimplemented!() // TODO(pg-port): access/nbtree.h
}

/// TODO(pg-port): BlockNumberIsValid() from storage/block.h.
#[allow(non_snake_case)]
#[inline]
fn BlockNumberIsValid(blkno: BlockNumber) -> bool {
    blkno != InvalidBlockNumber
}

/// TODO(pg-port): PageGetTempPageCopySpecial() from storage/bufpage.h.
#[allow(non_snake_case)]
unsafe fn PageGetTempPageCopySpecial(page: Page) -> Page {
    unimplemented!() // TODO(pg-port): storage/bufpage.h
}

/// TODO(pg-port): PageRestoreTempPage() from storage/bufpage.h.
#[allow(non_snake_case)]
unsafe fn PageRestoreTempPage(temppage: Page, oldpage: Page) {
    unimplemented!() // TODO(pg-port): storage/bufpage.h
}

/// TODO(pg-port): PageIndexTupleDelete() from storage/bufpage.h.
#[allow(non_snake_case)]
unsafe fn PageIndexTupleDelete(page: Page, offnum: OffsetNumber) {
    unimplemented!() // TODO(pg-port): storage/bufpage.h
}

/// TODO(pg-port): PageIndexMultiDelete() from storage/bufpage.h.
#[allow(non_snake_case)]
unsafe fn PageIndexMultiDelete(page: Page, itemnos: *mut OffsetNumber, nitems: c_int) {
    unimplemented!() // TODO(pg-port): storage/bufpage.h
}

/// TODO(pg-port): PageIndexTupleOverwrite() from storage/bufpage.h.
#[allow(non_snake_case)]
unsafe fn PageIndexTupleOverwrite(
    page: Page,
    offnum: OffsetNumber,
    newtup: Item,
    newsize: Size,
) -> bool {
    unimplemented!() // TODO(pg-port): storage/bufpage.h
}

/// TODO(pg-port): BufferGetPage() from storage/bufmgr.h.
#[allow(non_snake_case)]
unsafe fn BufferGetPage(buffer: Buffer) -> Page {
    unimplemented!() // TODO(pg-port): storage/bufmgr.h
}

/// TODO(pg-port): BufferGetPageSize() from storage/bufmgr.h.
#[allow(non_snake_case)]
unsafe fn BufferGetPageSize(buffer: Buffer) -> Size {
    unimplemented!() // TODO(pg-port): storage/bufmgr.h
}

/// TODO(pg-port): BufferGetBlockNumber() from storage/bufmgr.h.
#[allow(non_snake_case)]
unsafe fn BufferGetBlockNumber(buffer: Buffer) -> BlockNumber {
    unimplemented!() // TODO(pg-port): storage/bufmgr.h
}

/// TODO(pg-port): BufferIsValid() from storage/bufmgr.h.
#[allow(non_snake_case)]
unsafe fn BufferIsValid(buffer: Buffer) -> bool {
    unimplemented!() // TODO(pg-port): storage/bufmgr.h
}

/// TODO(pg-port): MarkBufferDirty() from storage/bufmgr.h.
#[allow(non_snake_case)]
unsafe fn MarkBufferDirty(buffer: Buffer) {
    unimplemented!() // TODO(pg-port): storage/bufmgr.h
}

/// TODO(pg-port): UnlockReleaseBuffer() from storage/bufmgr.h.
#[allow(non_snake_case)]
unsafe fn UnlockReleaseBuffer(buffer: Buffer) {
    unimplemented!() // TODO(pg-port): storage/bufmgr.h
}

/// TODO(pg-port): XLogRecGetBlockData() from access/xlogreader.h.
#[allow(non_snake_case)]
unsafe fn XLogRecGetBlockData(
    record: *mut XLogReaderState,
    block_id: uint8,
    len: *mut Size,
) -> *mut c_char {
    unimplemented!() // TODO(pg-port): access/xlogreader.h
}

/// TODO(pg-port): ResolveRecoveryConflictWithSnapshot() from storage/standby.h.
#[allow(non_snake_case)]
unsafe fn ResolveRecoveryConflictWithSnapshot(
    _snapshotConflictHorizon: crate::c::TransactionId,
    _isCatalogRel: bool,
    _rlocator: RelFileLocator,
) {
    unimplemented!() // TODO(pg-port): storage/standby.c
}

/// TODO(pg-port): ResolveRecoveryConflictWithSnapshotFullXid() from storage/standby.h.
#[allow(non_snake_case)]
unsafe fn ResolveRecoveryConflictWithSnapshotFullXid(
    _snapshotConflictHorizon: FullTransactionId,
    _isCatalogRel: bool,
    _locator: RelFileLocator,
) {
    unimplemented!() // TODO(pg-port): storage/standby.c
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/*
 * _bt_restore_page -- re-enter all the index tuples on a page
 *
 * The page is freshly init'd, and *from (length len) is a copy of what
 * had been its upper part (pd_upper to pd_special).  We assume that the
 * tuples had been added to the page in item-number order, and therefore
 * the one with highest item number appears first (lowest on the page).
 */
unsafe fn _bt_restore_page(page: Page, from: *mut c_char, len: c_int) {
    let mut itupdata: IndexTupleData = core::mem::zeroed();
    let mut itemsz: Size;
    let end: *mut c_char = from.add(len as usize);
    let mut items: [Item; MaxIndexTuplesPerPage as usize] =
        [std::ptr::null_mut(); MaxIndexTuplesPerPage as usize];
    let mut itemsizes: [uint16; MaxIndexTuplesPerPage as usize] =
        [0u16; MaxIndexTuplesPerPage as usize];
    let mut i: c_int;
    let nitems: c_int;

    /*
     * To get the items back in the original order, we add them to the page in
     * reverse.  To figure out where one tuple ends and another begins, we
     * have to scan them in forward order first.
     */
    let mut from = from;
    i = 0;
    while from < end {
        /*
         * As we step through the items, 'from' won't always be properly
         * aligned, so we need to use memcpy().  Further, we use Item (which
         * is just a char*) here for our items array for the same reason;
         * wouldn't want the compiler or anyone thinking that an item is
         * aligned when it isn't.
         */
        std::ptr::copy_nonoverlapping(
            from,
            &mut itupdata as *mut IndexTupleData as *mut c_char,
            core::mem::size_of::<IndexTupleData>(),
        );
        itemsz = IndexTupleSize(&itupdata as *const IndexTupleData as IndexTuple);
        itemsz = MAXALIGN(itemsz);

        items[i as usize] = from as Item;
        itemsizes[i as usize] = itemsz as uint16;
        i += 1;

        from = from.add(itemsz);
    }
    nitems = i;

    i = nitems - 1;
    while i >= 0 {
        if PageAddItem(
            page,
            items[i as usize],
            itemsizes[i as usize] as Size,
            (nitems - i) as OffsetNumber,
            false,
            false,
        ) == InvalidOffsetNumber
        {
            elog!(PANIC, "_bt_restore_page: cannot add item to page");
        }
        i -= 1;
    }
}

unsafe fn _bt_restore_meta(record: *mut XLogReaderState, block_id: uint8) {
    let lsn: XLogRecPtr = (*record).EndRecPtr;
    let metabuf: Buffer;
    let metapg: Page;
    let md: *mut BTMetaPageData;
    let pageop: BTPageOpaque;
    let xlrec: *mut xl_btree_metadata;
    let ptr: *mut c_char;
    let mut len: Size = 0;

    metabuf = XLogInitBufferForRedo(record as *mut core::ffi::c_void, block_id);
    ptr = XLogRecGetBlockData(record, block_id, &mut len);

    Assert!(len == core::mem::size_of::<xl_btree_metadata>());
    Assert!(BufferGetBlockNumber(metabuf) == BTREE_METAPAGE);
    xlrec = ptr as *mut xl_btree_metadata;
    metapg = BufferGetPage(metabuf);

    _bt_pageinit(metapg, BufferGetPageSize(metabuf));

    md = BTPageGetMeta(metapg);
    (*md).btm_magic = BTREE_MAGIC;
    (*md).btm_version = (*xlrec).version;
    (*md).btm_root = (*xlrec).root;
    (*md).btm_level = (*xlrec).level;
    (*md).btm_fastroot = (*xlrec).fastroot;
    (*md).btm_fastlevel = (*xlrec).fastlevel;
    /* Cannot log BTREE_MIN_VERSION index metapage without upgrade */
    Assert!((*md).btm_version >= BTREE_NOVAC_VERSION);
    (*md).btm_last_cleanup_num_delpages = (*xlrec).last_cleanup_num_delpages;
    (*md).btm_last_cleanup_num_heap_tuples = -1.0;
    (*md).btm_allequalimage = (*xlrec).allequalimage;

    pageop = BTPageGetOpaque(metapg);
    (*pageop).btpo_flags = BTP_META;

    /*
     * Set pd_lower just past the end of the metadata.  This is essential,
     * because without doing so, metadata will be lost if xlog.c compresses
     * the page.
     */
    (*(metapg as PageHeader)).pd_lower = ((md as *mut c_char)
        .add(core::mem::size_of::<BTMetaPageData>()))
    .offset_from(metapg as *mut c_char) as i16 as u16;

    PageSetLSN(metapg, lsn);
    MarkBufferDirty(metabuf);
    UnlockReleaseBuffer(metabuf);
}

/*
 * _bt_clear_incomplete_split -- clear INCOMPLETE_SPLIT flag on a page
 *
 * This is a common subroutine of the redo functions of all the WAL record
 * types that can insert a downlink: insert, split, and newroot.
 */
unsafe fn _bt_clear_incomplete_split(record: *mut XLogReaderState, block_id: uint8) {
    let lsn: XLogRecPtr = (*record).EndRecPtr;
    let mut buf: Buffer = 0;

    if XLogReadBufferForRedo(record as *mut core::ffi::c_void, block_id, &mut buf) == BLK_NEEDS_REDO {
        let page: Page = BufferGetPage(buf);
        let pageop: BTPageOpaque = BTPageGetOpaque(page);

        Assert!(P_INCOMPLETE_SPLIT(pageop));
        (*pageop).btpo_flags &= !BTP_INCOMPLETE_SPLIT;

        PageSetLSN(page, lsn);
        MarkBufferDirty(buf);
    }
    if BufferIsValid(buf) {
        UnlockReleaseBuffer(buf);
    }
}

unsafe fn btree_xlog_insert(isleaf: bool, ismeta: bool, posting: bool, record: *mut XLogReaderState) {
    let lsn: XLogRecPtr = (*record).EndRecPtr;
    let xlrec: *mut xl_btree_insert = XLogRecGetData(record) as *mut xl_btree_insert;
    let mut buffer: Buffer = 0;
    let page: Page;

    /*
     * Insertion to an internal page finishes an incomplete split at the child
     * level.  Clear the incomplete-split flag in the child.  Note: during
     * normal operation, the child and parent pages are locked at the same
     * time (the locks are coupled), so that clearing the flag and inserting
     * the downlink appear atomic to other backends.  We don't bother with
     * that during replay, because readers don't care about the
     * incomplete-split flag and there cannot be updates happening.
     */
    if !isleaf {
        _bt_clear_incomplete_split(record, 1);
    }
    if XLogReadBufferForRedo(record as *mut core::ffi::c_void, 0, &mut buffer) == BLK_NEEDS_REDO {
        let mut datalen: Size = 0;
        let mut datapos: *mut c_char = XLogRecGetBlockData(record, 0, &mut datalen);

        page = BufferGetPage(buffer);

        if !posting {
            /* Simple retail insertion */
            if PageAddItem(page, datapos as Item, datalen, (*xlrec).offnum, false, false)
                == InvalidOffsetNumber
            {
                elog!(PANIC, "failed to add new item");
            }
        } else {
            let itemid: ItemId;
            let oposting: IndexTuple;
            let newitem: IndexTuple;
            let nposting: IndexTuple;
            let postingoff: uint16;

            /*
             * A posting list split occurred during leaf page insertion.  WAL
             * record data will start with an offset number representing the
             * point in an existing posting list that a split occurs at.
             *
             * Use _bt_swap_posting() to repeat posting list split steps from
             * primary.  Note that newitem from WAL record is 'orignewitem',
             * not the final version of newitem that is actually inserted on
             * page.
             */
            postingoff = *(datapos as *const uint16);
            datapos = datapos.add(core::mem::size_of::<uint16>());
            datalen -= core::mem::size_of::<uint16>();

            itemid = PageGetItemId(page, OffsetNumberPrev((*xlrec).offnum));
            oposting = PageGetItem(page, itemid) as IndexTuple;

            /* Use mutable, aligned newitem copy in _bt_swap_posting() */
            Assert!(isleaf && postingoff > 0);
            newitem = CopyIndexTuple(datapos as IndexTuple);
            nposting = _bt_swap_posting(newitem, oposting, postingoff as c_int);

            /* Replace existing posting list with post-split version */
            std::ptr::copy_nonoverlapping(
                nposting as *const c_char,
                oposting as *mut c_char,
                MAXALIGN(IndexTupleSize(nposting)),
            );

            /* Insert "final" new item (not orignewitem from WAL stream) */
            Assert!(IndexTupleSize(newitem) == datalen);
            if PageAddItem(page, newitem as Item, datalen, (*xlrec).offnum, false, false)
                == InvalidOffsetNumber
            {
                elog!(PANIC, "failed to add posting split new item");
            }
        }

        PageSetLSN(page, lsn);
        MarkBufferDirty(buffer);
    }
    if BufferIsValid(buffer) {
        UnlockReleaseBuffer(buffer);
    }

    /*
     * Note: in normal operation, we'd update the metapage while still holding
     * lock on the page we inserted into.  But during replay it's not
     * necessary to hold that lock, since no other index updates can be
     * happening concurrently, and readers will cope fine with following an
     * obsolete link from the metapage.
     */
    if ismeta {
        _bt_restore_meta(record, 2);
    }
}

unsafe fn btree_xlog_split(newitemonleft: bool, record: *mut XLogReaderState) {
    let lsn: XLogRecPtr = (*record).EndRecPtr;
    let xlrec: *mut xl_btree_split = XLogRecGetData(record) as *mut xl_btree_split;
    let isleaf: bool = (*xlrec).level == 0;
    let mut buf: Buffer = 0;
    let rbuf: Buffer;
    let rpage: Page;
    let ropaque: BTPageOpaque;
    let mut datapos: *mut c_char;
    let mut datalen: Size = 0;
    let mut origpagenumber: BlockNumber = 0;
    let mut rightpagenumber: BlockNumber = 0;
    let mut spagenumber: BlockNumber = 0;

    XLogRecGetBlockTag(record, 0, std::ptr::null_mut(), std::ptr::null_mut(), &mut origpagenumber);
    XLogRecGetBlockTag(record, 1, std::ptr::null_mut(), std::ptr::null_mut(), &mut rightpagenumber);
    if !XLogRecGetBlockTagExtended(
        record,
        2,
        std::ptr::null_mut(),
        std::ptr::null_mut(),
        &mut spagenumber,
        std::ptr::null_mut(),
    ) {
        spagenumber = P_NONE;
    }

    /*
     * Clear the incomplete split flag on the appropriate child page one level
     * down when origpage/buf is an internal page (there must have been
     * cascading page splits during original execution in the event of an
     * internal page split).  This is like the corresponding btree_xlog_insert
     * call for internal pages.  We're not clearing the incomplete split flag
     * for the current page split here (you can think of this as part of the
     * insert of newitem that the page split action needs to perform in
     * passing).
     *
     * Like in btree_xlog_insert, this can be done before locking other pages.
     * We never need to couple cross-level locks in REDO routines.
     */
    if !isleaf {
        _bt_clear_incomplete_split(record, 3);
    }

    /* Reconstruct right (new) sibling page from scratch */
    rbuf = XLogInitBufferForRedo(record as *mut core::ffi::c_void, 1);
    datapos = XLogRecGetBlockData(record, 1, &mut datalen);
    rpage = BufferGetPage(rbuf);

    _bt_pageinit(rpage, BufferGetPageSize(rbuf));
    ropaque = BTPageGetOpaque(rpage);

    (*ropaque).btpo_prev = origpagenumber;
    (*ropaque).btpo_next = spagenumber;
    (*ropaque).btpo_level = (*xlrec).level;
    (*ropaque).btpo_flags = if isleaf { BTP_LEAF } else { 0 };
    (*ropaque).btpo_cycleid = 0;

    _bt_restore_page(rpage, datapos, datalen as c_int);

    PageSetLSN(rpage, lsn);
    MarkBufferDirty(rbuf);

    /* Now reconstruct original page (left half of split) */
    if XLogReadBufferForRedo(record as *mut core::ffi::c_void, 0, &mut buf) == BLK_NEEDS_REDO {
        /*
         * To retain the same physical order of the tuples that they had, we
         * initialize a temporary empty page for the left page and add all the
         * items to that in item number order.  This mirrors how _bt_split()
         * works.  Retaining the same physical order makes WAL consistency
         * checking possible.  See also _bt_restore_page(), which does the
         * same for the right page.
         */
        let origpage: Page = BufferGetPage(buf);
        let oopaque: BTPageOpaque = BTPageGetOpaque(origpage);
        let mut off: OffsetNumber;
        let mut newitem: IndexTuple = std::ptr::null_mut();
        let mut left_hikey: IndexTuple = std::ptr::null_mut();
        let mut nposting: IndexTuple = std::ptr::null_mut();
        let mut newitemsz: Size = 0;
        let mut left_hikeysz: Size = 0;
        let leftpage: Page;
        let mut leftoff: OffsetNumber;
        let mut replacepostingoff: OffsetNumber = InvalidOffsetNumber;

        datapos = XLogRecGetBlockData(record, 0, &mut datalen);

        if newitemonleft || (*xlrec).postingoff != 0 {
            newitem = datapos as IndexTuple;
            newitemsz = MAXALIGN(IndexTupleSize(newitem));
            datapos = datapos.add(newitemsz);
            datalen -= newitemsz;

            if (*xlrec).postingoff != 0 {
                let itemid: ItemId;
                let oposting: IndexTuple;

                /* Posting list must be at offset number before new item's */
                replacepostingoff = OffsetNumberPrev((*xlrec).newitemoff);

                /* Use mutable, aligned newitem copy in _bt_swap_posting() */
                newitem = CopyIndexTuple(newitem);
                itemid = PageGetItemId(origpage, replacepostingoff);
                oposting = PageGetItem(origpage, itemid) as IndexTuple;
                nposting = _bt_swap_posting(newitem, oposting, (*xlrec).postingoff as c_int);
            }
        }

        /*
         * Extract left hikey and its size.  We assume that 16-bit alignment
         * is enough to apply IndexTupleSize (since it's fetching from a
         * uint16 field).
         */
        left_hikey = datapos as IndexTuple;
        left_hikeysz = MAXALIGN(IndexTupleSize(left_hikey));
        datapos = datapos.add(left_hikeysz);
        datalen -= left_hikeysz;

        Assert!(datalen == 0);

        leftpage = PageGetTempPageCopySpecial(origpage);

        /* Add high key tuple from WAL record to temp page */
        leftoff = P_HIKEY;
        if PageAddItem(leftpage, left_hikey as Item, left_hikeysz, P_HIKEY, false, false)
            == InvalidOffsetNumber
        {
            elog!(ERROR, "failed to add high key to left page after split");
        }
        leftoff = OffsetNumberNext(leftoff);

        off = P_FIRSTDATAKEY(oopaque);
        while off < (*xlrec).firstrightoff {
            let itemid: ItemId;
            let itemsz: Size;
            let item: IndexTuple;

            /* Add replacement posting list when required */
            if off == replacepostingoff {
                Assert!(
                    newitemonleft || (*xlrec).firstrightoff == (*xlrec).newitemoff
                );
                if PageAddItem(
                    leftpage,
                    nposting as Item,
                    MAXALIGN(IndexTupleSize(nposting)),
                    leftoff,
                    false,
                    false,
                ) == InvalidOffsetNumber
                {
                    elog!(
                        ERROR,
                        "failed to add new posting list item to left page after split"
                    );
                }
                leftoff = OffsetNumberNext(leftoff);
                off = OffsetNumberNext(off);
                continue; /* don't insert oposting */
            }

            /* add the new item if it was inserted on left page */
            else if newitemonleft && off == (*xlrec).newitemoff {
                if PageAddItem(leftpage, newitem as Item, newitemsz, leftoff, false, false)
                    == InvalidOffsetNumber
                {
                    elog!(ERROR, "failed to add new item to left page after split");
                }
                leftoff = OffsetNumberNext(leftoff);
            }

            itemid = PageGetItemId(origpage, off);
            itemsz = ItemIdGetLength(itemid) as Size;
            item = PageGetItem(origpage, itemid) as IndexTuple;
            if PageAddItem(leftpage, item as Item, itemsz, leftoff, false, false)
                == InvalidOffsetNumber
            {
                elog!(ERROR, "failed to add old item to left page after split");
            }
            leftoff = OffsetNumberNext(leftoff);

            off = OffsetNumberNext(off);
        }

        /* cope with possibility that newitem goes at the end */
        if newitemonleft && off == (*xlrec).newitemoff {
            if PageAddItem(leftpage, newitem as Item, newitemsz, leftoff, false, false)
                == InvalidOffsetNumber
            {
                elog!(ERROR, "failed to add new item to left page after split");
            }
            leftoff = OffsetNumberNext(leftoff);
        }

        PageRestoreTempPage(leftpage, origpage);

        /* Fix opaque fields */
        (*oopaque).btpo_flags = BTP_INCOMPLETE_SPLIT;
        if isleaf {
            (*oopaque).btpo_flags |= BTP_LEAF;
        }
        (*oopaque).btpo_next = rightpagenumber;
        (*oopaque).btpo_cycleid = 0;

        PageSetLSN(origpage, lsn);
        MarkBufferDirty(buf);
    }

    /* Fix left-link of the page to the right of the new right sibling */
    if spagenumber != P_NONE {
        let mut sbuf: Buffer = 0;

        if XLogReadBufferForRedo(record as *mut core::ffi::c_void, 2, &mut sbuf) == BLK_NEEDS_REDO {
            let spage: Page = BufferGetPage(sbuf);
            let spageop: BTPageOpaque = BTPageGetOpaque(spage);

            (*spageop).btpo_prev = rightpagenumber;

            PageSetLSN(spage, lsn);
            MarkBufferDirty(sbuf);
        }
        if BufferIsValid(sbuf) {
            UnlockReleaseBuffer(sbuf);
        }
    }

    /*
     * Finally, release the remaining buffers.  sbuf, rbuf, and buf must be
     * released together, so that readers cannot observe inconsistencies.
     */
    UnlockReleaseBuffer(rbuf);
    if BufferIsValid(buf) {
        UnlockReleaseBuffer(buf);
    }
}

unsafe fn btree_xlog_dedup(record: *mut XLogReaderState) {
    let lsn: XLogRecPtr = (*record).EndRecPtr;
    let xlrec: *mut xl_btree_dedup = XLogRecGetData(record) as *mut xl_btree_dedup;
    let mut buf: Buffer = 0;

    if XLogReadBufferForRedo(record as *mut core::ffi::c_void, 0, &mut buf) == BLK_NEEDS_REDO {
        let ptr: *mut c_char = XLogRecGetBlockData(record, 0, std::ptr::null_mut());
        let page: Page = BufferGetPage(buf);
        let opaque: BTPageOpaque = BTPageGetOpaque(page);
        let mut offnum: OffsetNumber;
        let minoff: OffsetNumber;
        let maxoff: OffsetNumber;
        let state: BTDedupState;
        let intervals: *mut BTDedupInterval;
        let newpage: Page;

        state = palloc(core::mem::size_of::<BTDedupStateData>()) as BTDedupState;
        (*state).deduplicate = true; /* unused */
        (*state).nmaxitems = 0; /* unused */
        /* Conservatively use larger maxpostingsize than primary */
        (*state).maxpostingsize = BTMaxItemSize;
        (*state).base = std::ptr::null_mut();
        (*state).baseoff = InvalidOffsetNumber;
        (*state).basetupsize = 0;
        (*state).htids = palloc((*state).maxpostingsize) as crate::storage::itemptr::ItemPointer;
        (*state).nhtids = 0;
        (*state).nitems = 0;
        (*state).phystupsize = 0;
        (*state).nintervals = 0;

        minoff = P_FIRSTDATAKEY(opaque);
        maxoff = PageGetMaxOffsetNumber(page);
        newpage = PageGetTempPageCopySpecial(page);

        if !P_RIGHTMOST(opaque) {
            let itemid: ItemId = PageGetItemId(page, P_HIKEY);
            let itemsz: Size = ItemIdGetLength(itemid) as Size;
            let item: IndexTuple = PageGetItem(page, itemid) as IndexTuple;

            if PageAddItem(newpage, item as Item, itemsz, P_HIKEY, false, false)
                == InvalidOffsetNumber
            {
                elog!(ERROR, "deduplication failed to add highkey");
            }
        }

        intervals = ptr as *mut BTDedupInterval;
        offnum = minoff;
        while offnum <= maxoff {
            let itemid: ItemId = PageGetItemId(page, offnum);
            let itup: IndexTuple = PageGetItem(page, itemid) as IndexTuple;

            if offnum == minoff {
                _bt_dedup_start_pending(state, itup, offnum);
            } else if (*state).nintervals < (*xlrec).nintervals as c_int
                && (*state).baseoff == (*intervals.add((*state).nintervals as usize)).baseoff
                && (*state).nitems < (*intervals.add((*state).nintervals as usize)).nitems as c_int
            {
                if !_bt_dedup_save_htid(state, itup) {
                    elog!(
                        ERROR,
                        "deduplication failed to add heap tid to pending posting list"
                    );
                }
            } else {
                _bt_dedup_finish_pending(newpage, state);
                _bt_dedup_start_pending(state, itup, offnum);
            }

            offnum = OffsetNumberNext(offnum);
        }

        _bt_dedup_finish_pending(newpage, state);
        Assert!((*state).nintervals == (*xlrec).nintervals as c_int);
        Assert!(
            std::ptr::eq(
                (*state).intervals.as_ptr() as *const u8,
                (*state).intervals.as_ptr() as *const u8,
            ) || std::slice::from_raw_parts(
                (*state).intervals.as_ptr() as *const u8,
                (*state).nintervals as usize * core::mem::size_of::<BTDedupInterval>(),
            ) == std::slice::from_raw_parts(
                intervals as *const u8,
                (*state).nintervals as usize * core::mem::size_of::<BTDedupInterval>(),
            )
        );

        if P_HAS_GARBAGE(opaque) {
            let nopaque: BTPageOpaque = BTPageGetOpaque(newpage);

            (*nopaque).btpo_flags &= !BTP_HAS_GARBAGE;
        }

        PageRestoreTempPage(newpage, page);
        PageSetLSN(page, lsn);
        MarkBufferDirty(buf);
    }

    if BufferIsValid(buf) {
        UnlockReleaseBuffer(buf);
    }
}

unsafe fn btree_xlog_updates(
    page: Page,
    updatedoffsets: *mut OffsetNumber,
    mut updates: *mut xl_btree_update,
    nupdated: c_int,
) {
    let mut vacposting: BTVacuumPosting;
    let origtuple: IndexTuple;
    let itemid: ItemId;
    let itemsz: Size;

    for i in 0..nupdated {
        let itemid = PageGetItemId(page, *updatedoffsets.add(i as usize));
        let origtuple = PageGetItem(page, itemid) as IndexTuple;

        vacposting = palloc(
            core::mem::offset_of!(BTVacuumPostingData, deletetids)
                + (*updates).ndeletedtids as usize * core::mem::size_of::<uint16>(),
        ) as BTVacuumPosting;
        (*vacposting).updatedoffset = *updatedoffsets.add(i as usize);
        (*vacposting).itup = origtuple;
        (*vacposting).ndeletedtids = (*updates).ndeletedtids;
        std::ptr::copy_nonoverlapping(
            (updates as *mut c_char).add(SizeOfBtreeUpdate),
            (*vacposting).deletetids.as_mut_ptr() as *mut c_char,
            (*updates).ndeletedtids as usize * core::mem::size_of::<uint16>(),
        );

        _bt_update_posting(vacposting);

        /* Overwrite updated version of tuple */
        let itemsz = MAXALIGN(IndexTupleSize((*vacposting).itup));
        if !PageIndexTupleOverwrite(
            page,
            *updatedoffsets.add(i as usize),
            (*vacposting).itup as Item,
            itemsz,
        ) {
            elog!(PANIC, "failed to update partially dead item");
        }

        pfree((*vacposting).itup as *mut c_void);
        pfree(vacposting as *mut c_void);

        /* advance to next xl_btree_update from array */
        updates = (updates as *mut c_char)
            .add(SizeOfBtreeUpdate + (*updates).ndeletedtids as usize * core::mem::size_of::<uint16>())
            as *mut xl_btree_update;
    }
}

unsafe fn btree_xlog_vacuum(record: *mut XLogReaderState) {
    let lsn: XLogRecPtr = (*record).EndRecPtr;
    let xlrec: *mut xl_btree_vacuum = XLogRecGetData(record) as *mut xl_btree_vacuum;
    let mut buffer: Buffer = 0;
    let page: Page;
    let opaque: BTPageOpaque;

    /*
     * We need to take a cleanup lock here, just like btvacuumpage(). However,
     * it isn't necessary to exhaustively get a cleanup lock on every block in
     * the index during recovery (just getting a cleanup lock on pages with
     * items to kill suffices).  See nbtree/README for details.
     */
    if XLogReadBufferForRedoExtended(record as *mut core::ffi::c_void, 0, RBM_NORMAL, true, &mut buffer) == BLK_NEEDS_REDO {
        let ptr: *mut c_char = XLogRecGetBlockData(record, 0, std::ptr::null_mut());

        page = BufferGetPage(buffer);

        if (*xlrec).nupdated > 0 {
            let updatedoffsets: *mut OffsetNumber = (ptr as *mut c_char)
                .add((*xlrec).ndeleted as usize * core::mem::size_of::<OffsetNumber>())
                as *mut OffsetNumber;
            let updates: *mut xl_btree_update = (updatedoffsets as *mut c_char)
                .add((*xlrec).nupdated as usize * core::mem::size_of::<OffsetNumber>())
                as *mut xl_btree_update;

            btree_xlog_updates(page, updatedoffsets, updates, (*xlrec).nupdated as c_int);
        }

        if (*xlrec).ndeleted > 0 {
            PageIndexMultiDelete(page, ptr as *mut OffsetNumber, (*xlrec).ndeleted as c_int);
        }

        /*
         * Clear the vacuum cycle ID, and mark the page as not containing any
         * LP_DEAD items
         */
        let opaque = BTPageGetOpaque(page);
        (*opaque).btpo_cycleid = 0;
        (*opaque).btpo_flags &= !BTP_HAS_GARBAGE;

        PageSetLSN(page, lsn);
        MarkBufferDirty(buffer);
    }
    if BufferIsValid(buffer) {
        UnlockReleaseBuffer(buffer);
    }
}

unsafe fn btree_xlog_delete(record: *mut XLogReaderState) {
    let lsn: XLogRecPtr = (*record).EndRecPtr;
    let xlrec: *mut xl_btree_delete = XLogRecGetData(record) as *mut xl_btree_delete;
    let mut buffer: Buffer = 0;
    let page: Page;
    let opaque: BTPageOpaque;

    /*
     * If we have any conflict processing to do, it must happen before we
     * update the page
     */
    if InHotStandby {
        let mut rlocator: RelFileLocator = core::mem::zeroed();

        XLogRecGetBlockTag(record, 0, &raw mut rlocator as *mut crate::access::transam::xlogreader::RelFileLocator, std::ptr::null_mut(), std::ptr::null_mut());

        ResolveRecoveryConflictWithSnapshot(
            (*xlrec).snapshotConflictHorizon,
            (*xlrec).isCatalogRel,
            rlocator,
        );
    }

    /*
     * We don't need to take a cleanup lock to apply these changes. See
     * nbtree/README for details.
     */
    if XLogReadBufferForRedo(record as *mut core::ffi::c_void, 0, &mut buffer) == BLK_NEEDS_REDO {
        let ptr: *mut c_char = XLogRecGetBlockData(record, 0, std::ptr::null_mut());

        page = BufferGetPage(buffer);

        if (*xlrec).nupdated > 0 {
            let updatedoffsets: *mut OffsetNumber = (ptr as *mut c_char)
                .add((*xlrec).ndeleted as usize * core::mem::size_of::<OffsetNumber>())
                as *mut OffsetNumber;
            let updates: *mut xl_btree_update = (updatedoffsets as *mut c_char)
                .add((*xlrec).nupdated as usize * core::mem::size_of::<OffsetNumber>())
                as *mut xl_btree_update;

            btree_xlog_updates(page, updatedoffsets, updates, (*xlrec).nupdated as c_int);
        }

        if (*xlrec).ndeleted > 0 {
            PageIndexMultiDelete(page, ptr as *mut OffsetNumber, (*xlrec).ndeleted as c_int);
        }

        /*
         * Do *not* clear the vacuum cycle ID, but do mark the page as not
         * containing any LP_DEAD items
         */
        let opaque = BTPageGetOpaque(page);
        (*opaque).btpo_flags &= !BTP_HAS_GARBAGE;

        PageSetLSN(page, lsn);
        MarkBufferDirty(buffer);
    }
    if BufferIsValid(buffer) {
        UnlockReleaseBuffer(buffer);
    }
}

unsafe fn btree_xlog_mark_page_halfdead(info: uint8, record: *mut XLogReaderState) {
    let lsn: XLogRecPtr = (*record).EndRecPtr;
    let xlrec: *mut xl_btree_mark_page_halfdead =
        XLogRecGetData(record) as *mut xl_btree_mark_page_halfdead;
    let mut buffer: Buffer = 0;
    let mut page: Page;
    let mut pageop: BTPageOpaque;
    let mut trunctuple: IndexTupleData = core::mem::zeroed();

    /*
     * In normal operation, we would lock all the pages this WAL record
     * touches before changing any of them.  In WAL replay, it should be okay
     * to lock just one page at a time, since no concurrent index updates can
     * be happening, and readers should not care whether they arrive at the
     * target page or not (since it's surely empty).
     */

    /* to-be-deleted subtree's parent page */
    if XLogReadBufferForRedo(record as *mut core::ffi::c_void, 1, &mut buffer) == BLK_NEEDS_REDO {
        let poffset: OffsetNumber;
        let itemid: ItemId;
        let itup: IndexTuple;
        let nextoffset: OffsetNumber;
        let rightsib: BlockNumber;

        page = BufferGetPage(buffer);
        pageop = BTPageGetOpaque(page);

        poffset = (*xlrec).poffset;

        let nextoffset = OffsetNumberNext(poffset);
        let itemid = PageGetItemId(page, nextoffset);
        let itup = PageGetItem(page, itemid) as IndexTuple;
        let rightsib = BTreeTupleGetDownLink(itup);

        let itemid = PageGetItemId(page, poffset);
        let itup = PageGetItem(page, itemid) as IndexTuple;
        BTreeTupleSetDownLink(itup, rightsib);
        let nextoffset = OffsetNumberNext(poffset);
        PageIndexTupleDelete(page, nextoffset);

        PageSetLSN(page, lsn);
        MarkBufferDirty(buffer);
    }

    /*
     * Don't need to couple cross-level locks in REDO routines, so release
     * lock on internal page immediately
     */
    if BufferIsValid(buffer) {
        UnlockReleaseBuffer(buffer);
    }

    /* Rewrite the leaf page as a halfdead page */
    buffer = XLogInitBufferForRedo(record as *mut core::ffi::c_void, 0);
    page = BufferGetPage(buffer);

    _bt_pageinit(page, BufferGetPageSize(buffer));
    pageop = BTPageGetOpaque(page);

    (*pageop).btpo_prev = (*xlrec).leftblk;
    (*pageop).btpo_next = (*xlrec).rightblk;
    (*pageop).btpo_level = 0;
    (*pageop).btpo_flags = BTP_HALF_DEAD | BTP_LEAF;
    (*pageop).btpo_cycleid = 0;

    /*
     * Construct a dummy high key item that points to top parent page (value
     * is InvalidBlockNumber when the top parent page is the leaf page itself)
     */
    std::ptr::write_bytes(
        &mut trunctuple as *mut IndexTupleData as *mut u8,
        0,
        core::mem::size_of::<IndexTupleData>(),
    );
    trunctuple.t_info = core::mem::size_of::<IndexTupleData>() as u16;
    BTreeTupleSetTopParent(&mut trunctuple, (*xlrec).topparent);

    if PageAddItem(
        page,
        &mut trunctuple as *mut IndexTupleData as Item,
        core::mem::size_of::<IndexTupleData>(),
        P_HIKEY,
        false,
        false,
    ) == InvalidOffsetNumber
    {
        elog!(ERROR, "could not add dummy high key to half-dead page");
    }

    PageSetLSN(page, lsn);
    MarkBufferDirty(buffer);
    UnlockReleaseBuffer(buffer);
}

unsafe fn btree_xlog_unlink_page(info: uint8, record: *mut XLogReaderState) {
    let lsn: XLogRecPtr = (*record).EndRecPtr;
    let xlrec: *mut xl_btree_unlink_page = XLogRecGetData(record) as *mut xl_btree_unlink_page;
    let leftsib: BlockNumber;
    let rightsib: BlockNumber;
    let level: uint32;
    let isleaf: bool;
    let safexid: FullTransactionId;
    let mut leftbuf: Buffer;
    let target: Buffer;
    let mut rightbuf: Buffer = 0;
    let mut page: Page;
    let mut pageop: BTPageOpaque;

    leftsib = (*xlrec).leftsib;
    rightsib = (*xlrec).rightsib;
    level = (*xlrec).level;
    isleaf = level == 0;
    safexid = (*xlrec).safexid;

    /* No leaftopparent for level 0 (leaf page) or level 1 target */
    Assert!(!BlockNumberIsValid((*xlrec).leaftopparent) || level > 1);

    /*
     * In normal operation, we would lock all the pages this WAL record
     * touches before changing any of them.  In WAL replay, we at least lock
     * the pages in the same standard left-to-right order (leftsib, target,
     * rightsib), and don't release the sibling locks until the target is
     * marked deleted.
     */

    /* Fix right-link of left sibling, if any */
    if leftsib != P_NONE {
        let mut lb: Buffer = 0;
        if XLogReadBufferForRedo(record as *mut core::ffi::c_void, 1, &mut lb) == BLK_NEEDS_REDO {
            page = BufferGetPage(lb);
            pageop = BTPageGetOpaque(page);
            (*pageop).btpo_next = rightsib;

            PageSetLSN(page, lsn);
            MarkBufferDirty(lb);
        }
        leftbuf = lb;
    } else {
        leftbuf = InvalidBuffer;
    }

    /* Rewrite target page as empty deleted page */
    let target = XLogInitBufferForRedo(record as *mut core::ffi::c_void, 0);
    page = BufferGetPage(target);

    _bt_pageinit(page, BufferGetPageSize(target));
    pageop = BTPageGetOpaque(page);

    (*pageop).btpo_prev = leftsib;
    (*pageop).btpo_next = rightsib;
    (*pageop).btpo_level = level;
    BTPageSetDeleted(page, safexid);
    if isleaf {
        (*pageop).btpo_flags |= BTP_LEAF;
    }
    (*pageop).btpo_cycleid = 0;

    PageSetLSN(page, lsn);
    MarkBufferDirty(target);

    /* Fix left-link of right sibling */
    if XLogReadBufferForRedo(record as *mut core::ffi::c_void, 2, &mut rightbuf) == BLK_NEEDS_REDO {
        page = BufferGetPage(rightbuf);
        pageop = BTPageGetOpaque(page);
        (*pageop).btpo_prev = leftsib;

        PageSetLSN(page, lsn);
        MarkBufferDirty(rightbuf);
    }

    /* Release siblings */
    if BufferIsValid(leftbuf) {
        UnlockReleaseBuffer(leftbuf);
    }
    if BufferIsValid(rightbuf) {
        UnlockReleaseBuffer(rightbuf);
    }

    /* Release target */
    UnlockReleaseBuffer(target);

    /*
     * If we deleted a parent of the targeted leaf page, instead of the leaf
     * itself, update the leaf to point to the next remaining child in the
     * to-be-deleted subtree
     */
    if XLogRecHasBlockRef(record, 3) {
        /*
         * There is no real data on the page, so we just re-create it from
         * scratch using the information from the WAL record.
         *
         * Note that we don't end up here when the target page is also the
         * leafbuf page.  There is no need to add a dummy hikey item with a
         * top parent link when deleting leafbuf because it's the last page
         * we'll delete in the subtree undergoing deletion.
         */
        let leafbuf: Buffer;
        let mut trunctuple: IndexTupleData = core::mem::zeroed();

        Assert!(!isleaf);

        let leafbuf = XLogInitBufferForRedo(record as *mut core::ffi::c_void, 3);
        page = BufferGetPage(leafbuf);

        _bt_pageinit(page, BufferGetPageSize(leafbuf));
        pageop = BTPageGetOpaque(page);

        (*pageop).btpo_flags = BTP_HALF_DEAD | BTP_LEAF;
        (*pageop).btpo_prev = (*xlrec).leafleftsib;
        (*pageop).btpo_next = (*xlrec).leafrightsib;
        (*pageop).btpo_level = 0;
        (*pageop).btpo_cycleid = 0;

        /* Add a dummy hikey item */
        std::ptr::write_bytes(
            &mut trunctuple as *mut IndexTupleData as *mut u8,
            0,
            core::mem::size_of::<IndexTupleData>(),
        );
        trunctuple.t_info = core::mem::size_of::<IndexTupleData>() as u16;
        BTreeTupleSetTopParent(&mut trunctuple, (*xlrec).leaftopparent);

        if PageAddItem(
            page,
            &mut trunctuple as *mut IndexTupleData as Item,
            core::mem::size_of::<IndexTupleData>(),
            P_HIKEY,
            false,
            false,
        ) == InvalidOffsetNumber
        {
            elog!(ERROR, "could not add dummy high key to half-dead page");
        }

        PageSetLSN(page, lsn);
        MarkBufferDirty(leafbuf);
        UnlockReleaseBuffer(leafbuf);
    }

    /* Update metapage if needed */
    if info == XLOG_BTREE_UNLINK_PAGE_META {
        _bt_restore_meta(record, 4);
    }
}

unsafe fn btree_xlog_newroot(record: *mut XLogReaderState) {
    let lsn: XLogRecPtr = (*record).EndRecPtr;
    let xlrec: *mut xl_btree_newroot = XLogRecGetData(record) as *mut xl_btree_newroot;
    let buffer: Buffer;
    let page: Page;
    let pageop: BTPageOpaque;
    let ptr: *mut c_char;
    let mut len: Size = 0;

    let buffer = XLogInitBufferForRedo(record as *mut core::ffi::c_void, 0);
    page = BufferGetPage(buffer);

    _bt_pageinit(page, BufferGetPageSize(buffer));
    pageop = BTPageGetOpaque(page);

    (*pageop).btpo_flags = BTP_ROOT;
    (*pageop).btpo_prev = P_NONE;
    (*pageop).btpo_next = P_NONE;
    (*pageop).btpo_level = (*xlrec).level;
    if (*xlrec).level == 0 {
        (*pageop).btpo_flags |= BTP_LEAF;
    }
    (*pageop).btpo_cycleid = 0;

    if (*xlrec).level > 0 {
        let ptr = XLogRecGetBlockData(record, 0, &mut len);
        _bt_restore_page(page, ptr, len as c_int);

        /* Clear the incomplete-split flag in left child */
        _bt_clear_incomplete_split(record, 1);
    }

    PageSetLSN(page, lsn);
    MarkBufferDirty(buffer);
    UnlockReleaseBuffer(buffer);

    _bt_restore_meta(record, 2);
}

/*
 * In general VACUUM must defer recycling as a way of avoiding certain race
 * conditions.  Deleted pages contain a safexid value that is used by VACUUM
 * to determine whether or not it's safe to place a page that was deleted by
 * VACUUM earlier into the FSM now.  See nbtree/README.
 *
 * As far as any backend operating during original execution is concerned, the
 * FSM is a cache of recycle-safe pages; the mere presence of the page in the
 * FSM indicates that the page must already be safe to recycle (actually,
 * _bt_allocbuf() verifies it's safe using BTPageIsRecyclable(), but that's
 * just because it would be unwise to completely trust the FSM, given its
 * current limitations).
 *
 * This isn't sufficient to prevent similar concurrent recycling race
 * conditions during Hot Standby, though.  For that we need to log a
 * xl_btree_reuse_page record at the point that a page is actually recycled
 * and reused for an entirely unrelated page inside _bt_split().  These
 * records include the same safexid value from the original deleted page,
 * stored in the record's snapshotConflictHorizon field.
 *
 * The GlobalVisCheckRemovableFullXid() test in BTPageIsRecyclable() is used
 * to determine if it's safe to recycle a page.  This mirrors our own test:
 * the PGPROC->xmin > limitXmin test inside GetConflictingVirtualXIDs().
 * Consequently, one XID value achieves the same exclusion effect on primary
 * and standby.
 */
unsafe fn btree_xlog_reuse_page(record: *mut XLogReaderState) {
    let xlrec: *mut xl_btree_reuse_page = XLogRecGetData(record) as *mut xl_btree_reuse_page;

    if InHotStandby {
        ResolveRecoveryConflictWithSnapshotFullXid(
            (*xlrec).snapshotConflictHorizon,
            (*xlrec).isCatalogRel,
            (*xlrec).locator,
        );
    }
}

pub unsafe fn btree_redo(record: *mut XLogReaderState) {
    let info: uint8 = XLogRecGetInfo(record) & !XLR_INFO_MASK;
    let oldCtx: MemoryContext;

    oldCtx = MemoryContextSwitchTo(opCtx);
    match info {
        XLOG_BTREE_INSERT_LEAF => btree_xlog_insert(true, false, false, record),
        XLOG_BTREE_INSERT_UPPER => btree_xlog_insert(false, false, false, record),
        XLOG_BTREE_INSERT_META => btree_xlog_insert(false, true, false, record),
        XLOG_BTREE_SPLIT_L => btree_xlog_split(true, record),
        XLOG_BTREE_SPLIT_R => btree_xlog_split(false, record),
        XLOG_BTREE_INSERT_POST => btree_xlog_insert(true, false, true, record),
        XLOG_BTREE_DEDUP => btree_xlog_dedup(record),
        XLOG_BTREE_VACUUM => btree_xlog_vacuum(record),
        XLOG_BTREE_DELETE => btree_xlog_delete(record),
        XLOG_BTREE_MARK_PAGE_HALFDEAD => btree_xlog_mark_page_halfdead(info, record),
        XLOG_BTREE_UNLINK_PAGE | XLOG_BTREE_UNLINK_PAGE_META => {
            btree_xlog_unlink_page(info, record)
        }
        XLOG_BTREE_NEWROOT => btree_xlog_newroot(record),
        XLOG_BTREE_REUSE_PAGE => btree_xlog_reuse_page(record),
        XLOG_BTREE_META_CLEANUP => _bt_restore_meta(record, 0),
        _ => {
            elog!(PANIC, "btree_redo: unknown op code {}", info);
        }
    }
    MemoryContextSwitchTo(oldCtx);
    MemoryContextReset(opCtx);
}

pub unsafe fn btree_xlog_startup() {
    opCtx = AllocSetContextCreate!(
        CurrentMemoryContext,
        c"Btree recovery temporary context".as_ptr(),
        ALLOCSET_DEFAULT_SIZES
    );
}

pub unsafe fn btree_xlog_cleanup() {
    MemoryContextDelete(opCtx);
    opCtx = std::ptr::null_mut();
}

/*
 * Mask a btree page before performing consistency checks on it.
 */
pub unsafe fn btree_mask(pagedata: *mut c_char, blkno: BlockNumber) {
    let page: Page = pagedata as Page;
    let maskopaq: BTPageOpaque;

    mask_page_lsn_and_checksum(page);

    mask_page_hint_bits(page);
    mask_unused_space(page);

    maskopaq = BTPageGetOpaque(page);

    if P_ISLEAF(maskopaq) {
        /*
         * In btree leaf pages, it is possible to modify the LP_FLAGS without
         * emitting any WAL record. Hence, mask the line pointer flags. See
         * _bt_killitems(), _bt_check_unique() for details.
         */
        mask_lp_flags(page);
    }

    /*
     * BTP_HAS_GARBAGE is just an un-logged hint bit. So, mask it. See
     * _bt_delete_or_dedup_one_page(), _bt_killitems(), and _bt_check_unique()
     * for details.
     */
    (*maskopaq).btpo_flags &= !BTP_HAS_GARBAGE;

    /*
     * During replay of a btree page split, we don't set the BTP_SPLIT_END
     * flag of the right sibling and initialize the cycle_id to 0 for the same
     * page. See btree_xlog_split() for details.
     */
    (*maskopaq).btpo_flags &= !BTP_SPLIT_END;
    (*maskopaq).btpo_cycleid = 0;
}
