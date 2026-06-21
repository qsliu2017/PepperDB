//! WAL replay logic for inverted index.
//!
//! src/backend/access/gin/ginxlog.c
//! Merged header: src/include/access/ginxlog.h
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::prelude::*;

use std::ffi::{c_char, c_int, c_void};

use crate::c::{int32, uint16, uint32, uint8, Pointer, Size, TransactionId};
use crate::storage::block::{BlockNumber, InvalidBlockNumber};
use crate::access::transam::xlogdefs::XLogRecPtr;
use crate::storage::off::OffsetNumber;

// ----------------------------------------------------------------------------
// From src/include/access/ginxlog.h
// ----------------------------------------------------------------------------

pub const XLOG_GIN_CREATE_PTREE: u8 = 0x10;

#[repr(C)]
pub struct ginxlogCreatePostingTree {
    pub size: uint32,
    /* A compressed posting list follows */
}

/*
 * The format of the insertion record varies depending on the page type.
 * ginxlogInsert is the common part between all variants.
 *
 * Backup Blk 0: target page
 * Backup Blk 1: left child, if this insertion finishes an incomplete split
 */

pub const XLOG_GIN_INSERT: u8 = 0x20;

#[repr(C)]
pub struct ginxlogInsert {
    pub flags: uint16, /* GIN_INSERT_ISLEAF and/or GIN_INSERT_ISDATA */

    /*
     * FOLLOWS:
     *
     * 1. if not leaf page, block numbers of the left and right child pages
     * whose split this insertion finishes, as BlockIdData[2] (beware of
     * adding fields in this struct that would make them not 16-bit aligned)
     *
     * 2. a ginxlogInsertEntry or ginxlogRecompressDataLeaf struct, depending
     * on tree type.
     *
     * NB: the below structs are only 16-bit aligned when appended to a
     * ginxlogInsert struct! Beware of adding fields to them that require
     * stricter alignment.
     */
}

#[repr(C)]
pub struct ginxlogInsertEntry {
    pub offset: OffsetNumber,
    pub isDelete: bool,
    pub tuple: IndexTupleData, /* variable length */
}

#[repr(C)]
pub struct ginxlogRecompressDataLeaf {
    pub nactions: uint16,
    /* Variable number of 'actions' follow */
}

/*
 * Note: this struct is currently not used in code, and only acts as
 * documentation. The WAL record format is as specified here, but the code
 * uses straight access through a Pointer and memcpy to read/write these.
 */
#[repr(C)]
pub struct ginxlogSegmentAction {
    pub segno: uint8,    /* segment this action applies to */
    pub r#type: c_char,  /* action type (see below) */

    /*
     * Action-specific data follows. For INSERT and REPLACE actions that is a
     * GinPostingList struct. For ADDITEMS, a uint16 for the number of items
     * added, followed by the items themselves as ItemPointers. DELETE actions
     * have no further data.
     */
}

/* Action types */
pub const GIN_SEGMENT_UNMODIFIED: u8 = 0; /* no action (not used in WAL records) */
pub const GIN_SEGMENT_DELETE: u8 = 1; /* a whole segment is removed */
pub const GIN_SEGMENT_INSERT: u8 = 2; /* a whole segment is added */
pub const GIN_SEGMENT_REPLACE: u8 = 3; /* a segment is replaced */
pub const GIN_SEGMENT_ADDITEMS: u8 = 4; /* items are added to existing segment */

#[repr(C)]
pub struct ginxlogInsertDataInternal {
    pub offset: OffsetNumber,
    pub newitem: PostingItem,
}

/*
 * Backup Blk 0: new left page (= original page, if not root split)
 * Backup Blk 1: new right page
 * Backup Blk 2: original page / new root page, if root split
 * Backup Blk 3: left child, if this insertion completes an earlier split
 */
pub const XLOG_GIN_SPLIT: u8 = 0x30;

#[repr(C)]
pub struct ginxlogSplit {
    pub locator: RelFileLocator,
    pub rrlink: BlockNumber, /* right link, or root's blocknumber if root split */
    pub leftChildBlkno: BlockNumber, /* valid on a non-leaf split */
    pub rightChildBlkno: BlockNumber,
    pub flags: uint16, /* see below */
}

/*
 * Flags used in ginxlogInsert and ginxlogSplit records
 */
pub const GIN_INSERT_ISDATA: u16 = 0x01; /* for both insert and split records */
pub const GIN_INSERT_ISLEAF: u16 = 0x02; /* ditto */
pub const GIN_SPLIT_ROOT: u16 = 0x04; /* only for split records */

/*
 * Vacuum simply WAL-logs the whole page, when anything is modified. This
 * is functionally identical to XLOG_FPI records, but is kept separate for
 * debugging purposes.
 */
pub const XLOG_GIN_VACUUM_PAGE: u8 = 0x40;

/*
 * Vacuuming posting tree leaf page is WAL-logged like recompression caused
 * by insertion.
 */
pub const XLOG_GIN_VACUUM_DATA_LEAF_PAGE: u8 = 0x90;

#[repr(C)]
pub struct ginxlogVacuumDataLeafPage {
    pub data: ginxlogRecompressDataLeaf,
}

/*
 * Backup Blk 0: deleted page
 * Backup Blk 1: parent
 * Backup Blk 2: left sibling
 */
pub const XLOG_GIN_DELETE_PAGE: u8 = 0x50;

#[repr(C)]
pub struct ginxlogDeletePage {
    pub parentOffset: OffsetNumber,
    pub rightLink: BlockNumber,
    pub deleteXid: TransactionId, /* last Xid which could see this page in scan */
}

pub const XLOG_GIN_UPDATE_META_PAGE: u8 = 0x60;

/*
 * Backup Blk 0: metapage
 * Backup Blk 1: tail page
 */
#[repr(C)]
pub struct ginxlogUpdateMeta {
    pub locator: RelFileLocator,
    pub metadata: GinMetaPageData,
    pub prevTail: BlockNumber,
    pub newRightlink: BlockNumber,
    pub ntuples: int32, /* if ntuples > 0 then metadata.tail was updated with
                         * that many tuples; else new sub list was inserted */
    /* array of inserted tuples follows */
}

pub const XLOG_GIN_INSERT_LISTPAGE: u8 = 0x70;

#[repr(C)]
pub struct ginxlogInsertListPage {
    pub rightlink: BlockNumber,
    pub ntuples: int32,
    /* array of inserted tuples follows */
}

/*
 * Backup Blk 0: metapage
 * Backup Blk 1 to (ndeleted + 1): deleted pages
 */
pub const XLOG_GIN_DELETE_LISTPAGE: u8 = 0x80;

/*
 * The WAL record for deleting list pages must contain a block reference to
 * all the deleted pages, so the number of pages that can be deleted in one
 * record is limited by XLR_MAX_BLOCK_ID. (block_id 0 is used for the
 * metapage.)
 */
pub const GIN_NDELETE_AT_ONCE: c_int = if 16 < (XLR_MAX_BLOCK_ID - 1) {
    16
} else {
    XLR_MAX_BLOCK_ID - 1
};

#[repr(C)]
pub struct ginxlogDeleteListPages {
    pub metadata: GinMetaPageData,
    pub ndeleted: int32,
}

// ----------------------------------------------------------------------------
// From src/backend/access/gin/ginxlog.c
// ----------------------------------------------------------------------------

static mut opCtx: MemoryContext = std::ptr::null_mut(); /* working memory for operations */

unsafe fn ginRedoClearIncompleteSplit(record: *mut XLogReaderState, block_id: uint8) {
    let lsn: XLogRecPtr = (*record).EndRecPtr;
    let mut buffer: Buffer = 0;
    let page: Page;

    if XLogReadBufferForRedo(record, block_id, &mut buffer) == BLK_NEEDS_REDO {
        page = BufferGetPage(buffer) as Page;
        (*GinPageGetOpaque(page)).flags &= !(GIN_INCOMPLETE_SPLIT as u16);

        PageSetLSN(page, lsn);
        MarkBufferDirty(buffer);
    }
    if BufferIsValid(buffer) {
        UnlockReleaseBuffer(buffer);
    }
}

unsafe fn ginRedoCreatePTree(record: *mut XLogReaderState) {
    let lsn: XLogRecPtr = (*record).EndRecPtr;
    let data: *mut ginxlogCreatePostingTree =
        XLogRecGetData(record) as *mut ginxlogCreatePostingTree;
    let ptr: *mut c_char;
    let buffer: Buffer;
    let page: Page;

    buffer = XLogInitBufferForRedo(record, 0);
    page = BufferGetPage(buffer) as Page;

    GinInitBuffer(buffer, GIN_DATA | GIN_LEAF | GIN_COMPRESSED);

    ptr = XLogRecGetData(record).add(std::mem::size_of::<ginxlogCreatePostingTree>());

    /* Place page data */
    memcpy(
        GinDataLeafPageGetPostingList(page) as *mut c_void,
        ptr as *const c_void,
        (*data).size as usize,
    );

    GinDataPageSetDataSize(page, (*data).size as Size);

    PageSetLSN(page, lsn);

    MarkBufferDirty(buffer);
    UnlockReleaseBuffer(buffer);
}

unsafe fn ginRedoInsertEntry(
    buffer: Buffer,
    _isLeaf: bool,
    rightblkno: BlockNumber,
    rdata: *mut c_void,
) {
    let page: Page = BufferGetPage(buffer) as Page;
    let data: *mut ginxlogInsertEntry = rdata as *mut ginxlogInsertEntry;
    let offset: OffsetNumber = (*data).offset;
    let mut itup: IndexTuple;

    if rightblkno != InvalidBlockNumber {
        /* update link to right page after split */
        Assert!(!GinPageIsLeaf(page));
        Assert!(offset >= FirstOffsetNumber && offset <= PageGetMaxOffsetNumber(page));
        itup = PageGetItem(page, PageGetItemId(page, offset)) as IndexTuple;
        GinSetDownlink(itup, rightblkno);
    }

    if (*data).isDelete {
        Assert!(GinPageIsLeaf(page));
        Assert!(offset >= FirstOffsetNumber && offset <= PageGetMaxOffsetNumber(page));
        PageIndexTupleDelete(page, offset);
    }

    itup = &mut (*data).tuple;

    if PageAddItem(
        page,
        itup as Item,
        IndexTupleSize(itup),
        offset,
        false,
        false,
    ) == InvalidOffsetNumber
    {
        let mut locator: RelFileLocator = std::mem::zeroed();
        let mut forknum: ForkNumber = 0;
        let mut blknum: BlockNumber = 0;

        BufferGetTag(buffer, &mut locator, &mut forknum, &mut blknum);
        elog!(
            ERROR,
            "failed to add item to index page in {}/{}/{}",
            locator.spcOid,
            locator.dbOid,
            locator.relNumber
        );
    }
}

/*
 * Redo recompression of posting list.  Doing all the changes in-place is not
 * always possible, because it might require more space than we've on the page.
 * Instead, once modification is required we copy unprocessed tail of the page
 * into separately allocated chunk of memory for further reading original
 * versions of segments.  Thanks to that we don't bother about moving page data
 * in-place.
 */
unsafe fn ginRedoRecompress(page: Page, data: *mut ginxlogRecompressDataLeaf) {
    let actionno: c_int;
    let mut segno: c_int;
    let mut oldseg: *mut GinPostingList;
    let mut segmentend: Pointer;
    let mut walbuf: *mut c_char;
    let mut totalsize: c_int;
    let mut tailCopy: Pointer = std::ptr::null_mut();
    let mut writePtr: Pointer;
    let mut segptr: Pointer;

    /*
     * If the page is in pre-9.4 format, convert to new format first.
     */
    if !GinPageIsCompressed(page) {
        let uncompressed: ItemPointer = GinDataPageGetData(page) as ItemPointer;
        let nuncompressed: c_int = (*GinPageGetOpaque(page)).maxoff as c_int;
        let mut npacked: c_int = 0;

        /*
         * Empty leaf pages are deleted as part of vacuum, but leftmost and
         * rightmost pages are never deleted.  So, pg_upgrade'd from pre-9.4
         * instances might contain empty leaf pages, and we need to handle
         * them correctly.
         */
        if nuncompressed > 0 {
            let plist: *mut GinPostingList;

            plist = ginCompressPostingList(uncompressed, nuncompressed, BLCKSZ as c_int, &mut npacked);
            totalsize = SizeOfGinPostingList(plist) as c_int;

            Assert!(npacked == nuncompressed);

            memcpy(
                GinDataLeafPageGetPostingList(page) as *mut c_void,
                plist as *const c_void,
                totalsize as usize,
            );
        } else {
            totalsize = 0;
        }

        GinDataPageSetDataSize(page, totalsize as Size);
        GinPageSetCompressed(page);
        (*GinPageGetOpaque(page)).maxoff = InvalidOffsetNumber;
    }

    oldseg = GinDataLeafPageGetPostingList(page);
    writePtr = oldseg as Pointer;
    segmentend = (oldseg as Pointer).add(GinDataLeafPageGetPostingListSize(page) as usize);
    segno = 0;

    walbuf = (data as *mut c_char).add(std::mem::size_of::<ginxlogRecompressDataLeaf>());
    actionno = 0;
    let mut actionno = actionno;
    while actionno < (*data).nactions as c_int {
        let a_segno: uint8 = *(walbuf as *mut uint8);
        walbuf = walbuf.add(1);
        let mut a_action: uint8 = *(walbuf as *mut uint8);
        walbuf = walbuf.add(1);
        let mut newseg: *mut GinPostingList = std::ptr::null_mut();
        let mut newsegsize: c_int = 0;
        let mut items: *mut ItemPointerData = std::ptr::null_mut();
        let mut nitems: uint16 = 0;
        let olditems: *mut ItemPointerData;
        let nolditems: c_int;
        let newitems: *mut ItemPointerData;
        let nnewitems: c_int;
        let mut segsize: c_int;
        let mut nolditems_v: c_int = 0;
        let mut nnewitems_v: c_int = 0;

        /* Extract all the information we need from the WAL record */
        if a_action == GIN_SEGMENT_INSERT || a_action == GIN_SEGMENT_REPLACE {
            newseg = walbuf as *mut GinPostingList;
            newsegsize = SizeOfGinPostingList(newseg) as c_int;
            walbuf = walbuf.add(SHORTALIGN(newsegsize as usize));
        }

        if a_action == GIN_SEGMENT_ADDITEMS {
            memcpy(
                &mut nitems as *mut uint16 as *mut c_void,
                walbuf as *const c_void,
                std::mem::size_of::<uint16>(),
            );
            walbuf = walbuf.add(std::mem::size_of::<uint16>());
            items = walbuf as *mut ItemPointerData;
            walbuf = walbuf.add(nitems as usize * std::mem::size_of::<ItemPointerData>());
        }

        /* Skip to the segment that this action concerns */
        Assert!(segno <= a_segno as c_int);
        while segno < a_segno as c_int {
            /*
             * Once modification is started and page tail is copied, we've to
             * copy unmodified segments.
             */
            segsize = SizeOfGinPostingList(oldseg) as c_int;
            if !tailCopy.is_null() {
                Assert!(writePtr.add(segsize as usize) < PageGetSpecialPointer(page) as Pointer);
                memcpy(
                    writePtr as *mut c_void,
                    oldseg as *const c_void,
                    segsize as usize,
                );
            }
            writePtr = writePtr.add(segsize as usize);
            oldseg = GinNextPostingListSegment(oldseg);
            segno += 1;
        }

        /*
         * ADDITEMS action is handled like REPLACE, but the new segment to
         * replace the old one is reconstructed using the old segment from
         * disk and the new items from the WAL record.
         */
        if a_action == GIN_SEGMENT_ADDITEMS {
            let mut npacked: c_int = 0;

            olditems = ginPostingListDecode(oldseg, &mut nolditems_v);
            nolditems = nolditems_v;

            newitems = ginMergeItemPointers(
                items,
                nitems as u32,
                olditems,
                nolditems as u32,
                &mut nnewitems_v,
            );
            nnewitems = nnewitems_v;
            Assert!(nnewitems == nolditems + nitems as c_int);

            newseg = ginCompressPostingList(newitems, nnewitems, BLCKSZ as c_int, &mut npacked);
            Assert!(npacked == nnewitems);

            newsegsize = SizeOfGinPostingList(newseg) as c_int;
            a_action = GIN_SEGMENT_REPLACE;
        }

        segptr = oldseg as Pointer;
        if segptr != segmentend {
            segsize = SizeOfGinPostingList(oldseg) as c_int;
        } else {
            /*
             * Positioned after the last existing segment. Only INSERTs
             * expected here.
             */
            Assert!(a_action == GIN_SEGMENT_INSERT);
            segsize = 0;
        }

        /*
         * We're about to start modification of the page.  So, copy tail of
         * the page if it's not done already.
         */
        if tailCopy.is_null() && segptr != segmentend {
            let tailSize: c_int = segmentend.offset_from(segptr) as c_int;

            tailCopy = palloc(tailSize as Size) as Pointer;
            memcpy(
                tailCopy as *mut c_void,
                segptr as *const c_void,
                tailSize as usize,
            );
            segptr = tailCopy;
            oldseg = segptr as *mut GinPostingList;
            segmentend = segptr.add(tailSize as usize);
        }

        match a_action {
            x if x == GIN_SEGMENT_DELETE => {
                segptr = segptr.add(segsize as usize);
                segno += 1;
            }

            x if x == GIN_SEGMENT_INSERT => {
                /* copy the new segment in place */
                Assert!(writePtr.add(newsegsize as usize) <= PageGetSpecialPointer(page) as Pointer);
                memcpy(
                    writePtr as *mut c_void,
                    newseg as *const c_void,
                    newsegsize as usize,
                );
                writePtr = writePtr.add(newsegsize as usize);
            }

            x if x == GIN_SEGMENT_REPLACE => {
                /* copy the new version of segment in place */
                Assert!(writePtr.add(newsegsize as usize) <= PageGetSpecialPointer(page) as Pointer);
                memcpy(
                    writePtr as *mut c_void,
                    newseg as *const c_void,
                    newsegsize as usize,
                );
                writePtr = writePtr.add(newsegsize as usize);
                segptr = segptr.add(segsize as usize);
                segno += 1;
            }

            _ => {
                elog!(ERROR, "unexpected GIN leaf action: {}", a_action);
            }
        }
        oldseg = segptr as *mut GinPostingList;

        actionno += 1;
    }

    /* Copy the rest of unmodified segments if any. */
    segptr = oldseg as Pointer;
    if segptr != segmentend && !tailCopy.is_null() {
        let restSize: c_int = segmentend.offset_from(segptr) as c_int;

        Assert!(writePtr.add(restSize as usize) <= PageGetSpecialPointer(page) as Pointer);
        memcpy(
            writePtr as *mut c_void,
            segptr as *const c_void,
            restSize as usize,
        );
        writePtr = writePtr.add(restSize as usize);
    }

    totalsize = writePtr.offset_from(GinDataLeafPageGetPostingList(page) as Pointer) as c_int;
    GinDataPageSetDataSize(page, totalsize as Size);
}

unsafe fn ginRedoInsertData(
    buffer: Buffer,
    isLeaf: bool,
    rightblkno: BlockNumber,
    rdata: *mut c_void,
) {
    let page: Page = BufferGetPage(buffer) as Page;

    if isLeaf {
        let data: *mut ginxlogRecompressDataLeaf = rdata as *mut ginxlogRecompressDataLeaf;

        Assert!(GinPageIsLeaf(page));

        ginRedoRecompress(page, data);
    } else {
        let data: *mut ginxlogInsertDataInternal = rdata as *mut ginxlogInsertDataInternal;
        let oldpitem: *mut PostingItem;

        Assert!(!GinPageIsLeaf(page));

        /* update link to right page after split */
        oldpitem = GinDataPageGetPostingItem(page, (*data).offset);
        PostingItemSetBlockNumber(oldpitem, rightblkno);

        GinDataPageAddPostingItem(page, &mut (*data).newitem, (*data).offset);
    }
}

unsafe fn ginRedoInsert(record: *mut XLogReaderState) {
    let lsn: XLogRecPtr = (*record).EndRecPtr;
    let data: *mut ginxlogInsert = XLogRecGetData(record) as *mut ginxlogInsert;
    let mut buffer: Buffer = 0;
    let mut rightChildBlkno: BlockNumber = InvalidBlockNumber;
    let isLeaf: bool = ((*data).flags & GIN_INSERT_ISLEAF) != 0;

    /*
     * First clear incomplete-split flag on child page if this finishes a
     * split.
     */
    if !isLeaf {
        let mut payload: *mut c_char =
            XLogRecGetData(record).add(std::mem::size_of::<ginxlogInsert>());

        payload = payload.add(std::mem::size_of::<BlockIdData>());
        rightChildBlkno = BlockIdGetBlockNumber(payload as BlockId);
        payload = payload.add(std::mem::size_of::<BlockIdData>());
        let _ = payload;

        ginRedoClearIncompleteSplit(record, 1);
    }

    if XLogReadBufferForRedo(record, 0, &mut buffer) == BLK_NEEDS_REDO {
        let page: Page = BufferGetPage(buffer) as Page;
        let mut len: Size = 0;
        let payload: *mut c_char = XLogRecGetBlockData(record, 0, &mut len);

        /* How to insert the payload is tree-type specific */
        if (*data).flags & GIN_INSERT_ISDATA != 0 {
            Assert!(GinPageIsData(page));
            ginRedoInsertData(buffer, isLeaf, rightChildBlkno, payload as *mut c_void);
        } else {
            Assert!(!GinPageIsData(page));
            ginRedoInsertEntry(buffer, isLeaf, rightChildBlkno, payload as *mut c_void);
        }

        PageSetLSN(page, lsn);
        MarkBufferDirty(buffer);
    }
    if BufferIsValid(buffer) {
        UnlockReleaseBuffer(buffer);
    }
}

unsafe fn ginRedoSplit(record: *mut XLogReaderState) {
    let data: *mut ginxlogSplit = XLogRecGetData(record) as *mut ginxlogSplit;
    let mut lbuffer: Buffer = 0;
    let mut rbuffer: Buffer = 0;
    let mut rootbuf: Buffer = 0;
    let isLeaf: bool = ((*data).flags & GIN_INSERT_ISLEAF) != 0;
    let isRoot: bool = ((*data).flags & GIN_SPLIT_ROOT) != 0;

    /*
     * First clear incomplete-split flag on child page if this finishes a
     * split
     */
    if !isLeaf {
        ginRedoClearIncompleteSplit(record, 3);
    }

    if XLogReadBufferForRedo(record, 0, &mut lbuffer) != BLK_RESTORED {
        elog!(
            ERROR,
            "GIN split record did not contain a full-page image of left page"
        );
    }

    if XLogReadBufferForRedo(record, 1, &mut rbuffer) != BLK_RESTORED {
        elog!(
            ERROR,
            "GIN split record did not contain a full-page image of right page"
        );
    }

    if isRoot {
        if XLogReadBufferForRedo(record, 2, &mut rootbuf) != BLK_RESTORED {
            elog!(
                ERROR,
                "GIN split record did not contain a full-page image of root page"
            );
        }
        UnlockReleaseBuffer(rootbuf);
    }

    UnlockReleaseBuffer(rbuffer);
    UnlockReleaseBuffer(lbuffer);
}

/*
 * VACUUM_PAGE record contains simply a full image of the page, similar to
 * an XLOG_FPI record.
 */
unsafe fn ginRedoVacuumPage(record: *mut XLogReaderState) {
    let mut buffer: Buffer = 0;

    if XLogReadBufferForRedo(record, 0, &mut buffer) != BLK_RESTORED {
        elog!(
            ERROR,
            "replay of gin entry tree page vacuum did not restore the page"
        );
    }
    UnlockReleaseBuffer(buffer);
}

unsafe fn ginRedoVacuumDataLeafPage(record: *mut XLogReaderState) {
    let lsn: XLogRecPtr = (*record).EndRecPtr;
    let mut buffer: Buffer = 0;

    if XLogReadBufferForRedo(record, 0, &mut buffer) == BLK_NEEDS_REDO {
        let page: Page = BufferGetPage(buffer) as Page;
        let mut len: Size = 0;
        let xlrec: *mut ginxlogVacuumDataLeafPage;

        xlrec = XLogRecGetBlockData(record, 0, &mut len) as *mut ginxlogVacuumDataLeafPage;

        Assert!(GinPageIsLeaf(page));
        Assert!(GinPageIsData(page));

        ginRedoRecompress(page, &mut (*xlrec).data);
        PageSetLSN(page, lsn);
        MarkBufferDirty(buffer);
    }
    if BufferIsValid(buffer) {
        UnlockReleaseBuffer(buffer);
    }
}

unsafe fn ginRedoDeletePage(record: *mut XLogReaderState) {
    let lsn: XLogRecPtr = (*record).EndRecPtr;
    let data: *mut ginxlogDeletePage = XLogRecGetData(record) as *mut ginxlogDeletePage;
    let mut dbuffer: Buffer = 0;
    let mut pbuffer: Buffer = 0;
    let mut lbuffer: Buffer = 0;
    let mut page: Page;

    /*
     * Lock left page first in order to prevent possible deadlock with
     * ginStepRight().
     */
    if XLogReadBufferForRedo(record, 2, &mut lbuffer) == BLK_NEEDS_REDO {
        page = BufferGetPage(lbuffer) as Page;
        Assert!(GinPageIsData(page));
        (*GinPageGetOpaque(page)).rightlink = (*data).rightLink;
        PageSetLSN(page, lsn);
        MarkBufferDirty(lbuffer);
    }

    if XLogReadBufferForRedo(record, 0, &mut dbuffer) == BLK_NEEDS_REDO {
        page = BufferGetPage(dbuffer) as Page;
        Assert!(GinPageIsData(page));
        GinPageSetDeleted(page);
        GinPageSetDeleteXid(page, (*data).deleteXid);
        PageSetLSN(page, lsn);
        MarkBufferDirty(dbuffer);
    }

    if XLogReadBufferForRedo(record, 1, &mut pbuffer) == BLK_NEEDS_REDO {
        page = BufferGetPage(pbuffer) as Page;
        Assert!(GinPageIsData(page));
        Assert!(!GinPageIsLeaf(page));
        GinPageDeletePostingItem(page, (*data).parentOffset);
        PageSetLSN(page, lsn);
        MarkBufferDirty(pbuffer);
    }

    if BufferIsValid(lbuffer) {
        UnlockReleaseBuffer(lbuffer);
    }
    if BufferIsValid(pbuffer) {
        UnlockReleaseBuffer(pbuffer);
    }
    if BufferIsValid(dbuffer) {
        UnlockReleaseBuffer(dbuffer);
    }
}

unsafe fn ginRedoUpdateMetapage(record: *mut XLogReaderState) {
    let lsn: XLogRecPtr = (*record).EndRecPtr;
    let data: *mut ginxlogUpdateMeta = XLogRecGetData(record) as *mut ginxlogUpdateMeta;
    let metabuffer: Buffer;
    let metapage: Page;
    let mut buffer: Buffer = 0;

    /*
     * Restore the metapage. This is essentially the same as a full-page
     * image, so restore the metapage unconditionally without looking at the
     * LSN, to avoid torn page hazards.
     */
    metabuffer = XLogInitBufferForRedo(record, 0);
    Assert!(BufferGetBlockNumber(metabuffer) == GIN_METAPAGE_BLKNO);
    metapage = BufferGetPage(metabuffer) as Page;

    GinInitMetabuffer(metabuffer);
    memcpy(
        GinPageGetMeta(metapage) as *mut c_void,
        &(*data).metadata as *const GinMetaPageData as *const c_void,
        std::mem::size_of::<GinMetaPageData>(),
    );
    PageSetLSN(metapage, lsn);
    MarkBufferDirty(metabuffer);

    if (*data).ntuples > 0 {
        /*
         * insert into tail page
         */
        if XLogReadBufferForRedo(record, 1, &mut buffer) == BLK_NEEDS_REDO {
            let page: Page = BufferGetPage(buffer) as Page;
            let mut off: OffsetNumber;
            let mut i: c_int;
            let mut tupsize: Size;
            let payload: *mut c_char;
            let mut tuples: IndexTuple;
            let mut totaltupsize: Size = 0;

            payload = XLogRecGetBlockData(record, 1, &mut totaltupsize);
            tuples = payload as IndexTuple;

            if PageIsEmpty(page) {
                off = FirstOffsetNumber;
            } else {
                off = OffsetNumberNext(PageGetMaxOffsetNumber(page));
            }

            i = 0;
            while i < (*data).ntuples {
                tupsize = IndexTupleSize(tuples);

                if PageAddItem(page, tuples as Item, tupsize, off, false, false)
                    == InvalidOffsetNumber
                {
                    elog!(ERROR, "failed to add item to index page");
                }

                tuples = (tuples as *mut c_char).add(tupsize) as IndexTuple;

                off += 1;
                i += 1;
            }
            Assert!(payload.add(totaltupsize) == tuples as *mut c_char);

            /*
             * Increase counter of heap tuples
             */
            (*GinPageGetOpaque(page)).maxoff += 1;

            PageSetLSN(page, lsn);
            MarkBufferDirty(buffer);
        }
        if BufferIsValid(buffer) {
            UnlockReleaseBuffer(buffer);
        }
    } else if (*data).prevTail != InvalidBlockNumber {
        /*
         * New tail
         */
        if XLogReadBufferForRedo(record, 1, &mut buffer) == BLK_NEEDS_REDO {
            let page: Page = BufferGetPage(buffer) as Page;

            (*GinPageGetOpaque(page)).rightlink = (*data).newRightlink;

            PageSetLSN(page, lsn);
            MarkBufferDirty(buffer);
        }
        if BufferIsValid(buffer) {
            UnlockReleaseBuffer(buffer);
        }
    }

    UnlockReleaseBuffer(metabuffer);
}

unsafe fn ginRedoInsertListPage(record: *mut XLogReaderState) {
    let lsn: XLogRecPtr = (*record).EndRecPtr;
    let data: *mut ginxlogInsertListPage = XLogRecGetData(record) as *mut ginxlogInsertListPage;
    let buffer: Buffer;
    let page: Page;
    let l: OffsetNumber;
    let mut off: OffsetNumber = FirstOffsetNumber;
    let mut i: c_int;
    let mut tupsize: c_int;
    let payload: *mut c_char;
    let mut tuples: IndexTuple;
    let mut totaltupsize: Size = 0;

    /* We always re-initialize the page. */
    buffer = XLogInitBufferForRedo(record, 0);
    page = BufferGetPage(buffer) as Page;

    GinInitBuffer(buffer, GIN_LIST);
    (*GinPageGetOpaque(page)).rightlink = (*data).rightlink;
    if (*data).rightlink == InvalidBlockNumber {
        /* tail of sublist */
        GinPageSetFullRow(page);
        (*GinPageGetOpaque(page)).maxoff = 1;
    } else {
        (*GinPageGetOpaque(page)).maxoff = 0;
    }

    payload = XLogRecGetBlockData(record, 0, &mut totaltupsize);

    tuples = payload as IndexTuple;
    i = 0;
    while i < (*data).ntuples {
        tupsize = IndexTupleSize(tuples) as c_int;

        let l = PageAddItem(page, tuples as Item, tupsize as Size, off, false, false);

        if l == InvalidOffsetNumber {
            elog!(ERROR, "failed to add item to index page");
        }

        tuples = (tuples as *mut c_char).add(tupsize as usize) as IndexTuple;
        off += 1;
        i += 1;
    }
    let _ = l;
    Assert!(tuples as *mut c_char == payload.add(totaltupsize));

    PageSetLSN(page, lsn);
    MarkBufferDirty(buffer);

    UnlockReleaseBuffer(buffer);
}

unsafe fn ginRedoDeleteListPages(record: *mut XLogReaderState) {
    let lsn: XLogRecPtr = (*record).EndRecPtr;
    let data: *mut ginxlogDeleteListPages =
        XLogRecGetData(record) as *mut ginxlogDeleteListPages;
    let metabuffer: Buffer;
    let metapage: Page;
    let mut i: c_int;

    metabuffer = XLogInitBufferForRedo(record, 0);
    Assert!(BufferGetBlockNumber(metabuffer) == GIN_METAPAGE_BLKNO);
    metapage = BufferGetPage(metabuffer) as Page;

    GinInitMetabuffer(metabuffer);

    memcpy(
        GinPageGetMeta(metapage) as *mut c_void,
        &(*data).metadata as *const GinMetaPageData as *const c_void,
        std::mem::size_of::<GinMetaPageData>(),
    );
    PageSetLSN(metapage, lsn);
    MarkBufferDirty(metabuffer);

    /*
     * In normal operation, shiftList() takes exclusive lock on all the
     * pages-to-be-deleted simultaneously.  During replay, however, it should
     * be all right to lock them one at a time.  This is dependent on the fact
     * that we are deleting pages from the head of the list, and that readers
     * share-lock the next page before releasing the one they are on. So we
     * cannot get past a reader that is on, or due to visit, any page we are
     * going to delete.  New incoming readers will block behind our metapage
     * lock and then see a fully updated page list.
     *
     * No full-page images are taken of the deleted pages. Instead, they are
     * re-initialized as empty, deleted pages. Their right-links don't need to
     * be preserved, because no new readers can see the pages, as explained
     * above.
     */
    i = 0;
    while i < (*data).ndeleted {
        let buffer: Buffer;
        let page: Page;

        buffer = XLogInitBufferForRedo(record, (i + 1) as uint8);
        page = BufferGetPage(buffer) as Page;
        GinInitBuffer(buffer, GIN_DELETED);

        PageSetLSN(page, lsn);
        MarkBufferDirty(buffer);

        UnlockReleaseBuffer(buffer);

        i += 1;
    }
    UnlockReleaseBuffer(metabuffer);
}

pub unsafe fn gin_redo(record: *mut XLogReaderState) {
    let info: uint8 = XLogRecGetInfo(record) & !XLR_INFO_MASK;
    let oldCtx: MemoryContext;

    /*
     * GIN indexes do not require any conflict processing. NB: If we ever
     * implement a similar optimization as we have in b-tree, and remove
     * killed tuples outside VACUUM, we'll need to handle that here.
     */

    oldCtx = MemoryContextSwitchTo(opCtx);
    match info {
        XLOG_GIN_CREATE_PTREE => {
            ginRedoCreatePTree(record);
        }
        XLOG_GIN_INSERT => {
            ginRedoInsert(record);
        }
        XLOG_GIN_SPLIT => {
            ginRedoSplit(record);
        }
        XLOG_GIN_VACUUM_PAGE => {
            ginRedoVacuumPage(record);
        }
        XLOG_GIN_VACUUM_DATA_LEAF_PAGE => {
            ginRedoVacuumDataLeafPage(record);
        }
        XLOG_GIN_DELETE_PAGE => {
            ginRedoDeletePage(record);
        }
        XLOG_GIN_UPDATE_META_PAGE => {
            ginRedoUpdateMetapage(record);
        }
        XLOG_GIN_INSERT_LISTPAGE => {
            ginRedoInsertListPage(record);
        }
        XLOG_GIN_DELETE_LISTPAGE => {
            ginRedoDeleteListPages(record);
        }
        _ => {
            elog!(PANIC, "gin_redo: unknown op code {}", info);
        }
    }
    MemoryContextSwitchTo(oldCtx);
    MemoryContextReset(opCtx);
}

pub unsafe fn gin_xlog_startup() {
    opCtx = AllocSetContextCreate!(
        CurrentMemoryContext,
        c"GIN recovery temporary context".as_ptr(),
        ALLOCSET_DEFAULT_SIZES,
    );
}

pub unsafe fn gin_xlog_cleanup() {
    MemoryContextDelete(opCtx);
    opCtx = std::ptr::null_mut();
}

/*
 * Mask a GIN page before running consistency checks on it.
 */
pub unsafe fn gin_mask(pagedata: *mut c_char, _blkno: BlockNumber) {
    let page: Page = pagedata as Page;
    let pagehdr: PageHeader = page as PageHeader;
    let opaque: GinPageOpaque;

    mask_page_lsn_and_checksum(page);
    opaque = GinPageGetOpaque(page);

    mask_page_hint_bits(page);

    /*
     * For a GIN_DELETED page, the page is initialized to empty.  Hence, mask
     * the whole page content.  For other pages, mask the hole if pd_lower
     * appears to have been set correctly.
     */
    if (*opaque).flags & (GIN_DELETED as u16) != 0 {
        mask_page_content(page);
    } else if (*pagehdr).pd_lower as usize > SizeOfPageHeaderData {
        mask_unused_space(page);
    }
}

// ----------------------------------------------------------------------------
// Local stubs for unported dependencies
// ----------------------------------------------------------------------------

use crate::access::transam::xlogreader::XLogReaderState;
type Buffer = c_int;
type Page = *mut c_char;
type PageHeader = *mut PageHeaderData;
type Item = *mut c_char;
type IndexTuple = *mut IndexTupleData;
type ItemPointer = *mut ItemPointerData;
type ForkNumber = c_int;
type BlockId = *mut BlockIdData;
type GinPageOpaque = *mut GinPageOpaqueData;

#[repr(C)]
struct PageHeaderData {
    _opaque: [u8; 0],
    pd_lower: u16,
}

#[repr(C)]
pub struct IndexTupleData {
    _opaque: [u8; 0],
}

#[repr(C)]
struct ItemPointerData {
    _opaque: [u8; 0],
}

#[repr(C)]
struct BlockIdData {
    _opaque: [u8; 0],
}

#[repr(C)]
struct GinPostingList {
    _opaque: [u8; 0],
}

#[repr(C)]
pub struct PostingItem {
    _opaque: [u8; 0],
}

#[repr(C)]
pub struct GinMetaPageData {
    _opaque: [u8; 0],
}

#[repr(C)]
struct GinPageOpaqueData {
    maxoff: OffsetNumber,
    rightlink: BlockNumber,
    flags: u16,
}

#[allow(non_camel_case_types)]
type RelFileLocator = RelFileLocatorStub;

#[repr(C)]
pub struct RelFileLocatorStub {
    spcOid: Oid,
    dbOid: Oid,
    relNumber: u32,
}

const BLK_NEEDS_REDO: c_int = 0;
const BLK_RESTORED: c_int = 2;
const InvalidOffsetNumber: OffsetNumber = 0;
const FirstOffsetNumber: OffsetNumber = 1;
const XLR_INFO_MASK: uint8 = 0x0F;
const XLR_MAX_BLOCK_ID: c_int = 32;
const GIN_INCOMPLETE_SPLIT: c_int = 0x0008;
const GIN_DATA: c_int = 0x0001;
const GIN_LEAF: c_int = 0x0002;
const GIN_COMPRESSED: c_int = 0x0010;
const GIN_LIST: c_int = 0x0020;
const GIN_DELETED: c_int = 0x0004;
const GIN_METAPAGE_BLKNO: BlockNumber = 0;
const BLCKSZ: usize = 8192;
const SizeOfPageHeaderData: usize = 24;

#[allow(non_upper_case_globals)]
const SizeOfGinPostingListAlias: () = ();

// SHORTALIGN comes from crate::c (via the prelude glob).

unsafe fn XLogReadBufferForRedo(
    _record: *mut XLogReaderState,
    _block_id: uint8,
    _buf: *mut Buffer,
) -> c_int {
    unimplemented!() // TODO: access/transam/xlogutils.c
}
unsafe fn XLogInitBufferForRedo(_record: *mut XLogReaderState, _block_id: uint8) -> Buffer { unimplemented!() }
unsafe fn XLogRecGetData(_record: *mut XLogReaderState) -> *mut c_char { crate::access::transam::xlogreader::XLogRecGetData(_record) }
unsafe fn XLogRecGetBlockData(
    _record: *mut XLogReaderState,
    _block_id: uint8,
    _len: *mut Size,
) -> *mut c_char { crate::access::transam::xlogreader::XLogRecGetBlockData(_record, _block_id, _len) }
unsafe fn XLogRecGetInfo(_record: *mut XLogReaderState) -> uint8 { crate::access::transam::xlogreader::XLogRecGetInfo(_record) }
unsafe fn BufferGetPage(_buffer: Buffer) -> *mut c_char {
    unimplemented!() // TODO: storage/buffer/bufmgr.c
}
unsafe fn BufferIsValid(_buffer: Buffer) -> bool { crate::access::nbtree::nbtpage::BufferIsValid(_buffer) }
unsafe fn BufferGetBlockNumber(_buffer: Buffer) -> BlockNumber {
    unimplemented!() // TODO: storage/buffer/bufmgr.c
}
unsafe fn MarkBufferDirty(_buffer: Buffer) {
    unimplemented!() // TODO: storage/buffer/bufmgr.c
}
unsafe fn UnlockReleaseBuffer(_buffer: Buffer) {
    unimplemented!() // TODO: storage/buffer/bufmgr.c
}
unsafe fn BufferGetTag(
    _buffer: Buffer,
    _locator: *mut RelFileLocator,
    _forknum: *mut ForkNumber,
    _blknum: *mut BlockNumber,
) {
    unimplemented!() // TODO: storage/buffer/bufmgr.c
}
unsafe fn PageSetLSN(_page: Page, _lsn: XLogRecPtr) { crate::storage::bufpage::PageSetLSN(_page, _lsn) }
unsafe fn PageGetItem(_page: Page, _itemid: *mut c_void) -> *mut c_char {
    unimplemented!() // TODO: storage/page/bufpage.h
}
unsafe fn PageGetItemId(_page: Page, _offset: OffsetNumber) -> *mut c_void {
    unimplemented!() // TODO: storage/page/bufpage.h
}
unsafe fn PageGetMaxOffsetNumber(_page: Page) -> OffsetNumber {
    unimplemented!() // TODO: storage/page/bufpage.h
}
unsafe fn PageGetSpecialPointer(_page: Page) -> *mut c_char { crate::storage::bufpage::PageGetSpecialPointer(_page) }
unsafe fn PageIsEmpty(_page: Page) -> bool {
    unimplemented!() // TODO: storage/page/bufpage.h
}
unsafe fn PageAddItem(
    _page: Page,
    _item: Item,
    _size: Size,
    _offset: OffsetNumber,
    _overwrite: bool,
    _is_heap: bool,
) -> OffsetNumber { crate::storage::bufpage::PageAddItem(_page, _item, _size, _offset, _overwrite, _is_heap) }
unsafe fn PageIndexTupleDelete(_page: Page, _offset: OffsetNumber) {
    unimplemented!() // TODO: storage/page/bufpage.c
}
unsafe fn IndexTupleSize(_itup: IndexTuple) -> Size {
    unimplemented!() // TODO: access/common/itup.h
}
unsafe fn OffsetNumberNext(_offset: OffsetNumber) -> OffsetNumber { crate::storage::off::OffsetNumberNext(_offset) }
unsafe fn GinPageGetOpaque(_page: Page) -> GinPageOpaque { unimplemented!() }
unsafe fn GinPageIsLeaf(_page: Page) -> bool { crate::access::gin::ginblock::GinPageIsLeaf(_page) }
unsafe fn GinPageIsData(_page: Page) -> bool { crate::access::gin::ginblock::GinPageIsData(_page) }
unsafe fn GinPageIsCompressed(_page: Page) -> bool { crate::access::gin::ginblock::GinPageIsCompressed(_page) }
unsafe fn GinPageSetCompressed(_page: Page) { crate::access::gin::ginblock::GinPageSetCompressed(_page) }
unsafe fn GinPageSetDeleted(_page: Page) { crate::access::gin::ginblock::GinPageSetDeleted(_page) }
unsafe fn GinPageSetDeleteXid(_page: Page, _xid: TransactionId) { crate::access::gin::ginblock::GinPageSetDeleteXid(_page, _xid) }
unsafe fn GinPageSetFullRow(_page: Page) { crate::access::gin::ginblock::GinPageSetFullRow(_page) }
unsafe fn GinInitBuffer(_buffer: Buffer, _flags: c_int) { unimplemented!() }
unsafe fn GinInitMetabuffer(_buffer: Buffer) { unimplemented!() }
unsafe fn GinDataLeafPageGetPostingList(_page: Page) -> *mut GinPostingList { unimplemented!() }
unsafe fn GinDataLeafPageGetPostingListSize(_page: Page) -> Size { crate::access::gin::ginblock::GinDataLeafPageGetPostingListSize(_page) as _ }
unsafe fn GinDataPageGetData(_page: Page) -> *mut c_char { crate::access::gin::ginblock::GinDataPageGetData(_page) }
unsafe fn GinDataPageSetDataSize(_page: Page, _size: Size) { crate::access::gin::ginblock::GinDataPageSetDataSize(_page, _size as _) }
unsafe fn GinDataPageGetPostingItem(_page: Page, _offset: OffsetNumber) -> *mut PostingItem { unimplemented!() }
unsafe fn GinDataPageAddPostingItem(_page: Page, _data: *mut PostingItem, _offset: OffsetNumber) { unimplemented!() }
unsafe fn GinPageDeletePostingItem(_page: Page, _offset: OffsetNumber) { unimplemented!() }
unsafe fn GinPageGetMeta(_page: Page) -> *mut GinMetaPageData { unimplemented!() }
unsafe fn GinSetDownlink(_itup: IndexTuple, _blkno: BlockNumber) {
    unimplemented!() // TODO: access/ginblock.h
}
unsafe fn GinNextPostingListSegment(_seg: *mut GinPostingList) -> *mut GinPostingList { unimplemented!() }
unsafe fn SizeOfGinPostingList(_plist: *mut GinPostingList) -> Size {
    unimplemented!() // TODO: access/ginblock.h
}
unsafe fn PostingItemSetBlockNumber(_item: *mut PostingItem, _blkno: BlockNumber) { unimplemented!() }
unsafe fn ginCompressPostingList(
    _ipd: ItemPointer,
    _nipd: c_int,
    _maxsize: c_int,
    _nwritten: *mut c_int,
) -> *mut GinPostingList { unimplemented!() }
unsafe fn ginPostingListDecode(_plist: *mut GinPostingList, _ndecoded: *mut c_int) -> *mut ItemPointerData {
    unimplemented!() // TODO: access/gin/ginpostinglist.c
}
unsafe fn ginMergeItemPointers(
    _a: *mut ItemPointerData,
    _na: u32,
    _b: *mut ItemPointerData,
    _nb: u32,
    _nmerged: *mut c_int,
) -> *mut ItemPointerData {
    unimplemented!() // TODO: access/gin/ginbtree.c
}
unsafe fn BlockIdGetBlockNumber(_blockid: BlockId) -> BlockNumber {
    unimplemented!() // TODO: storage/block.h
}
unsafe fn mask_page_lsn_and_checksum(_page: Page) { crate::access::common::bufmask::mask_page_lsn_and_checksum(_page) }
unsafe fn mask_page_hint_bits(_page: Page) { crate::access::common::bufmask::mask_page_hint_bits(_page) }
unsafe fn mask_page_content(_page: Page) { crate::access::common::bufmask::mask_page_content(_page) }
unsafe fn mask_unused_space(_page: Page) { crate::access::common::bufmask::mask_unused_space(_page) }

extern "C" {
    fn memcpy(dest: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
}
