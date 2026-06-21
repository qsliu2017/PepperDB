//! gistxlog.c
//!   WAL replay logic for GiST.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! src/backend/access/gist/gistxlog.c
//! src/include/access/gistxlog.h

use crate::prelude::*;
use crate::{list_make1, list_make2};

use std::ffi::{c_char, c_int, c_void};

use crate::c::{uint8, uint16, uint64, Size, TransactionId};

use crate::access::transam::xlogdefs::XLogRecPtr;
use crate::storage::block::BlockNumber;

// ---------------------------------------------------------------------------
// gistxlog.h
// ---------------------------------------------------------------------------

pub const XLOG_GIST_PAGE_UPDATE: uint8 = 0x00;
pub const XLOG_GIST_DELETE: uint8 = 0x10; /* delete leaf index tuples for a page */
pub const XLOG_GIST_PAGE_REUSE: uint8 = 0x20; /* old page is about to be reused from FSM */
pub const XLOG_GIST_PAGE_SPLIT: uint8 = 0x30;
/* #define XLOG_GIST_INSERT_COMPLETE  0x40 */ /* not used anymore */
/* #define XLOG_GIST_CREATE_INDEX     0x50 */ /* not used anymore */
pub const XLOG_GIST_PAGE_DELETE: uint8 = 0x60;
pub const XLOG_GIST_ASSIGN_LSN: uint8 = 0x70; /* nop, assign new LSN */

/*
 * Backup Blk 0: updated page.
 * Backup Blk 1: If this operation completes a page split, by inserting a
 *				 downlink for the split page, the left half of the split
 */
#[repr(C)]
pub struct gistxlogPageUpdate {
    /* number of deleted offsets */
    pub ntodelete: uint16,
    pub ntoinsert: uint16,
    /*
     * In payload of blk 0 : 1. todelete OffsetNumbers 2. tuples to insert
     */
}

/*
 * Backup Blk 0: Leaf page, whose index tuples are deleted.
 */
#[repr(C)]
pub struct gistxlogDelete {
    pub snapshotConflictHorizon: TransactionId,
    pub ntodelete: uint16, /* number of deleted offsets */
    pub isCatalogRel: bool, /* to handle recovery conflict during logical
                             * decoding on standby */

    /* TODELETE OFFSET NUMBERS */
    pub offsets: [OffsetNumber; FLEXIBLE_ARRAY_MEMBER],
}

pub const SizeOfGistxlogDelete: Size = core::mem::offset_of!(gistxlogDelete, offsets);

/*
 * Backup Blk 0: If this operation completes a page split, by inserting a
 *				 downlink for the split page, the left half of the split
 * Backup Blk 1 - npage: split pages (1 is the original page)
 */
#[repr(C)]
pub struct gistxlogPageSplit {
    pub origrlink: BlockNumber, /* rightlink of the page before split */
    pub orignsn: GistNSN,       /* NSN of the page before split */
    pub origleaf: bool,         /* was split page a leaf page? */

    pub npage: uint16,          /* # of pages in the split */
    pub markfollowright: bool,  /* set F_FOLLOW_RIGHT flags */

    /*
     * follow: 1. gistxlogPage and array of IndexTupleData per page
     */
}

/*
 * Backup Blk 0: page that was deleted.
 * Backup Blk 1: parent page, containing the downlink to the deleted page.
 */
#[repr(C)]
pub struct gistxlogPageDelete {
    pub deleteXid: FullTransactionId, /* last Xid which could see page in scan */
    pub downlinkOffset: OffsetNumber, /* Offset of downlink referencing this page */
}

pub const SizeOfGistxlogPageDelete: Size =
    core::mem::offset_of!(gistxlogPageDelete, downlinkOffset) + core::mem::size_of::<OffsetNumber>();

/*
 * This is what we need to know about page reuse, for hot standby.
 */
#[repr(C)]
pub struct gistxlogPageReuse {
    pub locator: RelFileLocator,
    pub block: BlockNumber,
    pub snapshotConflictHorizon: FullTransactionId,
    pub isCatalogRel: bool, /* to handle recovery conflict during logical
                             * decoding on standby */
}

pub const SizeOfGistxlogPageReuse: Size =
    core::mem::offset_of!(gistxlogPageReuse, isCatalogRel) + core::mem::size_of::<bool>();

// ---------------------------------------------------------------------------
// gistxlog.c
// ---------------------------------------------------------------------------

static mut opCtx: MemoryContext = std::ptr::null_mut(); /* working memory for operations */

/*
 * Replay the clearing of F_FOLLOW_RIGHT flag on a child page.
 *
 * Even if the WAL record includes a full-page image, we have to update the
 * follow-right flag, because that change is not included in the full-page
 * image.  To be sure that the intermediate state with the wrong flag value is
 * not visible to concurrent Hot Standby queries, this function handles
 * restoring the full-page image as well as updating the flag.  (Note that
 * we never need to do anything else to the child page in the current WAL
 * action.)
 */
unsafe fn gistRedoClearFollowRight(record: *mut XLogReaderState, block_id: uint8) {
    let lsn: XLogRecPtr = (*record).EndRecPtr;
    let mut buffer: Buffer = 0;
    let page: Page;
    let action: XLogRedoAction;

    /*
     * Note that we still update the page even if it was restored from a full
     * page image, because the updated NSN is not included in the image.
     */
    action = XLogReadBufferForRedo(record, block_id, &mut buffer);
    if action == BLK_NEEDS_REDO || action == BLK_RESTORED {
        page = BufferGetPage(buffer);

        GistPageSetNSN(page, lsn);
        GistClearFollowRight(page);

        PageSetLSN(page, lsn);
        MarkBufferDirty(buffer);
    }
    if BufferIsValid(buffer) {
        UnlockReleaseBuffer(buffer);
    }
}

/*
 * redo any page update (except page split)
 */
unsafe fn gistRedoPageUpdateRecord(record: *mut XLogReaderState) {
    let lsn: XLogRecPtr = (*record).EndRecPtr;
    let xldata: *mut gistxlogPageUpdate = XLogRecGetData(record) as *mut gistxlogPageUpdate;
    let mut buffer: Buffer = 0;
    let page: Page;

    if XLogReadBufferForRedo(record, 0, &mut buffer) == BLK_NEEDS_REDO {
        let begin: *mut c_char;
        let mut data: *mut c_char;
        let mut datalen: Size = 0;
        let mut ninserted: c_int = 0; // PG_USED_FOR_ASSERTS_ONLY
        let _ = &mut ninserted;

        data = XLogRecGetBlockData(record, 0, &mut datalen);
        begin = data;

        page = BufferGetPage(buffer) as Page;

        if (*xldata).ntodelete == 1 && (*xldata).ntoinsert == 1 {
            /*
             * When replacing one tuple with one other tuple, we must use
             * PageIndexTupleOverwrite for consistency with gistplacetopage.
             */
            let offnum: OffsetNumber = *(data as *mut OffsetNumber);
            let itup: IndexTuple;
            let itupsize: Size;

            data = data.add(core::mem::size_of::<OffsetNumber>());
            itup = data as IndexTuple;
            itupsize = IndexTupleSize(itup);
            if !PageIndexTupleOverwrite(page, offnum, itup as Item, itupsize) {
                elog!(
                    ERROR,
                    "failed to add item to GiST index page, size {} bytes",
                    itupsize as c_int
                );
            }
            data = data.add(itupsize);
            /* should be nothing left after consuming 1 tuple */
            Assert!(data.offset_from(begin) as Size == datalen);
            /* update insertion count for assert check below */
            ninserted += 1;
        } else if (*xldata).ntodelete > 0 {
            /* Otherwise, delete old tuples if any */
            let todelete: *mut OffsetNumber = data as *mut OffsetNumber;

            data = data.add(core::mem::size_of::<OffsetNumber>() * (*xldata).ntodelete as usize);

            PageIndexMultiDelete(page, todelete, (*xldata).ntodelete as c_int);
            if GistPageIsLeaf(page) {
                GistMarkTuplesDeleted(page);
            }
        }

        /* Add new tuples if any */
        if (data.offset_from(begin) as Size) < datalen {
            let mut off: OffsetNumber = if PageIsEmpty(page) {
                FirstOffsetNumber
            } else {
                OffsetNumberNext(PageGetMaxOffsetNumber(page))
            };

            while (data.offset_from(begin) as Size) < datalen {
                let itup: IndexTuple = data as IndexTuple;
                let sz: Size = IndexTupleSize(itup);
                let l: OffsetNumber;

                data = data.add(sz);

                l = PageAddItem(page, itup as Item, sz, off, false, false);
                if l == InvalidOffsetNumber {
                    elog!(
                        ERROR,
                        "failed to add item to GiST index page, size {} bytes",
                        sz as c_int
                    );
                }
                off += 1;
                ninserted += 1;
            }
        }

        /* Check that XLOG record contained expected number of tuples */
        Assert!(ninserted == (*xldata).ntoinsert as c_int);

        PageSetLSN(page, lsn);
        MarkBufferDirty(buffer);
    }

    /*
     * Fix follow-right data on left child page
     *
     * This must be done while still holding the lock on the target page. Note
     * that even if the target page no longer exists, we still attempt to
     * replay the change on the child page.
     */
    if XLogRecHasBlockRef(record, 1) {
        gistRedoClearFollowRight(record, 1);
    }

    if BufferIsValid(buffer) {
        UnlockReleaseBuffer(buffer);
    }
}

/*
 * redo delete on gist index page to remove tuples marked as DEAD during index
 * tuple insertion
 */
unsafe fn gistRedoDeleteRecord(record: *mut XLogReaderState) {
    let lsn: XLogRecPtr = (*record).EndRecPtr;
    let xldata: *mut gistxlogDelete = XLogRecGetData(record) as *mut gistxlogDelete;
    let mut buffer: Buffer = 0;
    let page: Page;
    let toDelete: *mut OffsetNumber = (*xldata).offsets.as_mut_ptr();

    /*
     * If we have any conflict processing to do, it must happen before we
     * update the page.
     *
     * GiST delete records can conflict with standby queries.  You might think
     * that vacuum records would conflict as well, but we've handled that
     * already.  XLOG_HEAP2_PRUNE_VACUUM_SCAN records provide the highest xid
     * cleaned by the vacuum of the heap and so we can resolve any conflicts
     * just once when that arrives.  After that we know that no conflicts
     * exist from individual gist vacuum records on that index.
     */
    if InHotStandby {
        let mut rlocator: RelFileLocator = std::mem::zeroed();

        XLogRecGetBlockTag(
            record,
            0,
            &mut rlocator,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
        );

        ResolveRecoveryConflictWithSnapshot(
            (*xldata).snapshotConflictHorizon,
            (*xldata).isCatalogRel,
            rlocator,
        );
    }

    if XLogReadBufferForRedo(record, 0, &mut buffer) == BLK_NEEDS_REDO {
        page = BufferGetPage(buffer) as Page;

        PageIndexMultiDelete(page, toDelete, (*xldata).ntodelete as c_int);

        GistClearPageHasGarbage(page);
        GistMarkTuplesDeleted(page);

        PageSetLSN(page, lsn);
        MarkBufferDirty(buffer);
    }

    if BufferIsValid(buffer) {
        UnlockReleaseBuffer(buffer);
    }
}

/*
 * Returns an array of index pointers.
 */
unsafe fn decodePageSplitRecord(begin: *mut c_char, len: c_int, n: *mut c_int) -> *mut IndexTuple {
    let mut ptr: *mut c_char;
    let mut i: c_int;
    let tuples: *mut IndexTuple;

    /* extract the number of tuples */
    std::ptr::copy_nonoverlapping(
        begin as *const c_void as *const u8,
        n as *mut u8,
        core::mem::size_of::<c_int>(),
    );
    ptr = begin.add(core::mem::size_of::<c_int>());

    tuples =
        palloc(*n as usize * core::mem::size_of::<IndexTuple>()) as *mut IndexTuple;

    i = 0;
    while i < *n {
        Assert!((ptr.offset_from(begin) as c_int) < len);
        *tuples.add(i as usize) = ptr as IndexTuple;
        ptr = ptr.add(IndexTupleSize(ptr as IndexTuple));
        i += 1;
    }
    Assert!(ptr.offset_from(begin) as c_int == len);

    tuples
}

unsafe fn gistRedoPageSplitRecord(record: *mut XLogReaderState) {
    let lsn: XLogRecPtr = (*record).EndRecPtr;
    let xldata: *mut gistxlogPageSplit = XLogRecGetData(record) as *mut gistxlogPageSplit;
    let mut firstbuffer: Buffer = InvalidBuffer;
    let mut buffer: Buffer;
    let mut page: Page;
    let mut i: c_int;
    let mut isrootsplit: bool = false;

    /*
     * We must hold lock on the first-listed page throughout the action,
     * including while updating the left child page (if any).  We can unlock
     * remaining pages in the list as soon as they've been written, because
     * there is no path for concurrent queries to reach those pages without
     * first visiting the first-listed page.
     */

    /* loop around all pages */
    i = 0;
    while i < (*xldata).npage as c_int {
        let flags: c_int;
        let data: *mut c_char;
        let mut datalen: Size = 0;
        let mut num: c_int = 0;
        let mut blkno: BlockNumber = 0;
        let tuples: *mut IndexTuple;

        XLogRecGetBlockTag(
            record,
            (i + 1) as uint8,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            &mut blkno,
        );
        if blkno == GIST_ROOT_BLKNO {
            Assert!(i == 0);
            isrootsplit = true;
        }

        buffer = XLogInitBufferForRedo(record, (i + 1) as uint8);
        let page_inner: Page = BufferGetPage(buffer) as Page;
        page = page_inner;
        data = XLogRecGetBlockData(record, (i + 1) as uint8, &mut datalen);

        tuples = decodePageSplitRecord(data, datalen as c_int, &mut num);

        /* ok, clear buffer */
        if (*xldata).origleaf && blkno != GIST_ROOT_BLKNO {
            flags = F_LEAF;
        } else {
            flags = 0;
        }
        GISTInitBuffer(buffer, flags);

        /* and fill it */
        gistfillbuffer(page, tuples, num, FirstOffsetNumber);

        if blkno == GIST_ROOT_BLKNO {
            (*GistPageGetOpaque(page)).rightlink = InvalidBlockNumber;
            GistPageSetNSN(page, (*xldata).orignsn);
            GistClearFollowRight(page);
        } else {
            if i < (*xldata).npage as c_int - 1 {
                let mut nextblkno: BlockNumber = 0;

                XLogRecGetBlockTag(
                    record,
                    (i + 2) as uint8,
                    std::ptr::null_mut(),
                    std::ptr::null_mut(),
                    &mut nextblkno,
                );
                (*GistPageGetOpaque(page)).rightlink = nextblkno;
            } else {
                (*GistPageGetOpaque(page)).rightlink = (*xldata).origrlink;
            }
            GistPageSetNSN(page, (*xldata).orignsn);
            if i < (*xldata).npage as c_int - 1 && !isrootsplit && (*xldata).markfollowright {
                GistMarkFollowRight(page);
            } else {
                GistClearFollowRight(page);
            }
        }

        PageSetLSN(page, lsn);
        MarkBufferDirty(buffer);

        if i == 0 {
            firstbuffer = buffer;
        } else {
            UnlockReleaseBuffer(buffer);
        }

        i += 1;
    }

    /* Fix follow-right data on left child page, if any */
    if XLogRecHasBlockRef(record, 0) {
        gistRedoClearFollowRight(record, 0);
    }

    /* Finally, release lock on the first page */
    UnlockReleaseBuffer(firstbuffer);
}

/* redo page deletion */
unsafe fn gistRedoPageDelete(record: *mut XLogReaderState) {
    let lsn: XLogRecPtr = (*record).EndRecPtr;
    let xldata: *mut gistxlogPageDelete = XLogRecGetData(record) as *mut gistxlogPageDelete;
    let mut parentBuffer: Buffer = 0;
    let mut leafBuffer: Buffer = 0;

    if XLogReadBufferForRedo(record, 0, &mut leafBuffer) == BLK_NEEDS_REDO {
        let page: Page = BufferGetPage(leafBuffer) as Page;

        GistPageSetDeleted(page, (*xldata).deleteXid);

        PageSetLSN(page, lsn);
        MarkBufferDirty(leafBuffer);
    }

    if XLogReadBufferForRedo(record, 1, &mut parentBuffer) == BLK_NEEDS_REDO {
        let page: Page = BufferGetPage(parentBuffer) as Page;

        PageIndexTupleDelete(page, (*xldata).downlinkOffset);

        PageSetLSN(page, lsn);
        MarkBufferDirty(parentBuffer);
    }

    if BufferIsValid(parentBuffer) {
        UnlockReleaseBuffer(parentBuffer);
    }
    if BufferIsValid(leafBuffer) {
        UnlockReleaseBuffer(leafBuffer);
    }
}

unsafe fn gistRedoPageReuse(record: *mut XLogReaderState) {
    let xlrec: *mut gistxlogPageReuse = XLogRecGetData(record) as *mut gistxlogPageReuse;

    /*
     * PAGE_REUSE records exist to provide a conflict point when we reuse
     * pages in the index via the FSM.  That's all they do though.
     *
     * snapshotConflictHorizon was the page's deleteXid.  The
     * GlobalVisCheckRemovableFullXid(deleteXid) test in gistPageRecyclable()
     * conceptually mirrors the PGPROC->xmin > limitXmin test in
     * GetConflictingVirtualXIDs().  Consequently, one XID value achieves the
     * same exclusion effect on primary and standby.
     */
    if InHotStandby {
        ResolveRecoveryConflictWithSnapshotFullXid(
            (*xlrec).snapshotConflictHorizon,
            (*xlrec).isCatalogRel,
            core::ptr::read(&(*xlrec).locator),
        );
    }
}

pub unsafe fn gist_redo(record: *mut XLogReaderState) {
    let info: uint8 = XLogRecGetInfo(record) & !XLR_INFO_MASK;
    let oldCxt: MemoryContext;

    /*
     * GiST indexes do not require any conflict processing. NB: If we ever
     * implement a similar optimization we have in b-tree, and remove killed
     * tuples outside VACUUM, we'll need to handle that here.
     */

    oldCxt = MemoryContextSwitchTo(opCtx);
    match info {
        XLOG_GIST_PAGE_UPDATE => {
            gistRedoPageUpdateRecord(record);
        }
        XLOG_GIST_DELETE => {
            gistRedoDeleteRecord(record);
        }
        XLOG_GIST_PAGE_REUSE => {
            gistRedoPageReuse(record);
        }
        XLOG_GIST_PAGE_SPLIT => {
            gistRedoPageSplitRecord(record);
        }
        XLOG_GIST_PAGE_DELETE => {
            gistRedoPageDelete(record);
        }
        XLOG_GIST_ASSIGN_LSN => {
            /* nop. See gistGetFakeLSN(). */
        }
        _ => {
            elog!(PANIC, "gist_redo: unknown op code {}", info);
        }
    }

    MemoryContextSwitchTo(oldCxt);
    MemoryContextReset(opCtx);
}

pub unsafe fn gist_xlog_startup() {
    opCtx = createTempGistContext();
}

pub unsafe fn gist_xlog_cleanup() {
    MemoryContextDelete(opCtx);
}

/*
 * Mask a Gist page before running consistency checks on it.
 */
pub unsafe fn gist_mask(pagedata: *mut c_char, _blkno: BlockNumber) {
    let page: Page = pagedata as Page;

    mask_page_lsn_and_checksum(page);

    mask_page_hint_bits(page);
    mask_unused_space(page);

    /*
     * NSN is nothing but a special purpose LSN. Hence, mask it for the same
     * reason as mask_page_lsn_and_checksum.
     */
    GistPageSetNSN(page, MASK_MARKER as uint64);

    /*
     * We update F_FOLLOW_RIGHT flag on the left child after writing WAL
     * record. Hence, mask this flag. See gistplacetopage() for details.
     */
    GistMarkFollowRight(page);

    if GistPageIsLeaf(page) {
        /*
         * In gist leaf pages, it is possible to modify the LP_FLAGS without
         * emitting any WAL record. Hence, mask the line pointer flags. See
         * gistkillitems() for details.
         */
        mask_lp_flags(page);
    }

    /*
     * During gist redo, we never mark a page as garbage. Hence, mask it to
     * ignore any differences.
     */
    GistClearPageHasGarbage(page);
}

/*
 * Write WAL record of a page split.
 */
pub unsafe fn gistXLogSplit(
    page_is_leaf: bool,
    dist: *mut SplitPageLayout,
    origrlink: BlockNumber,
    orignsn: GistNSN,
    leftchildbuf: Buffer,
    markfollowright: bool,
) -> XLogRecPtr {
    let mut xlrec: gistxlogPageSplit = std::mem::zeroed();
    let mut ptr: *mut SplitPageLayout;
    let mut npage: c_int = 0;
    let recptr: XLogRecPtr;
    let mut i: c_int;

    ptr = dist;
    while !ptr.is_null() {
        npage += 1;
        ptr = (*ptr).next;
    }

    xlrec.origrlink = origrlink;
    xlrec.orignsn = orignsn;
    xlrec.origleaf = page_is_leaf;
    xlrec.npage = npage as uint16;
    xlrec.markfollowright = markfollowright;

    XLogBeginInsert();

    /*
     * Include a full page image of the child buf. (only necessary if a
     * checkpoint happened since the child page was split)
     */
    if BufferIsValid(leftchildbuf) {
        XLogRegisterBuffer(0, leftchildbuf, REGBUF_STANDARD as uint8);
    }

    /*
     * NOTE: We register a lot of data. The caller must've called
     * XLogEnsureRecordSpace() to prepare for that. We cannot do it here,
     * because we're already in a critical section. If you change the number
     * of buffer or data registrations here, make sure you modify the
     * XLogEnsureRecordSpace() calls accordingly!
     */
    XLogRegisterData(
        &mut xlrec as *mut gistxlogPageSplit as *mut c_char,
        core::mem::size_of::<gistxlogPageSplit>(),
    );

    i = 1;
    ptr = dist;
    while !ptr.is_null() {
        XLogRegisterBuffer(i as uint8, (*ptr).buffer, REGBUF_WILL_INIT as uint8);
        XLogRegisterBufData(
            i as uint8,
            &mut (*ptr).block.num as *mut _ as *mut c_char,
            core::mem::size_of::<c_int>(),
        );
        XLogRegisterBufData(i as uint8, (*ptr).list as *mut c_char, (*ptr).lenlist);
        i += 1;
        ptr = (*ptr).next;
    }

    recptr = XLogInsert(RM_GIST_ID, XLOG_GIST_PAGE_SPLIT);

    recptr
}

/*
 * Write XLOG record describing a page deletion. This also includes removal of
 * downlink from the parent page.
 */
pub unsafe fn gistXLogPageDelete(
    buffer: Buffer,
    xid: FullTransactionId,
    parentBuffer: Buffer,
    downlinkOffset: OffsetNumber,
) -> XLogRecPtr {
    let mut xlrec: gistxlogPageDelete = std::mem::zeroed();
    let recptr: XLogRecPtr;

    xlrec.deleteXid = xid;
    xlrec.downlinkOffset = downlinkOffset;

    XLogBeginInsert();
    XLogRegisterData(
        &mut xlrec as *mut gistxlogPageDelete as *mut c_char,
        SizeOfGistxlogPageDelete,
    );

    XLogRegisterBuffer(0, buffer, REGBUF_STANDARD as uint8);
    XLogRegisterBuffer(1, parentBuffer, REGBUF_STANDARD as uint8);

    recptr = XLogInsert(RM_GIST_ID, XLOG_GIST_PAGE_DELETE);

    recptr
}

/*
 * Write an empty XLOG record to assign a distinct LSN.
 */
pub unsafe fn gistXLogAssignLSN() -> XLogRecPtr {
    let mut dummy: c_int = 0;

    /*
     * Records other than XLOG_SWITCH must have content. We use an integer 0
     * to follow the restriction.
     */
    XLogBeginInsert();
    XLogSetRecordFlags(XLOG_MARK_UNIMPORTANT as uint8);
    XLogRegisterData(
        &mut dummy as *mut c_int as *mut c_char,
        core::mem::size_of::<c_int>(),
    );
    XLogInsert(RM_GIST_ID, XLOG_GIST_ASSIGN_LSN)
}

/*
 * Write XLOG record about reuse of a deleted page.
 */
pub unsafe fn gistXLogPageReuse(
    rel: Relation,
    heaprel: Relation,
    blkno: BlockNumber,
    deleteXid: FullTransactionId,
) {
    let mut xlrec_reuse: gistxlogPageReuse = std::mem::zeroed();

    /*
     * Note that we don't register the buffer with the record, because this
     * operation doesn't modify the page. This record only exists to provide a
     * conflict point for Hot Standby.
     */

    /* XLOG stuff */
    xlrec_reuse.isCatalogRel = RelationIsAccessibleInLogicalDecoding(heaprel);
    xlrec_reuse.locator = core::ptr::read(&(*rel).rd_locator);
    xlrec_reuse.block = blkno;
    xlrec_reuse.snapshotConflictHorizon = deleteXid;

    XLogBeginInsert();
    XLogRegisterData(
        &mut xlrec_reuse as *mut gistxlogPageReuse as *mut c_char,
        SizeOfGistxlogPageReuse,
    );

    XLogInsert(RM_GIST_ID, XLOG_GIST_PAGE_REUSE);
}

/*
 * Write XLOG record describing a page update. The update can include any
 * number of deletions and/or insertions of tuples on a single index page.
 *
 * If this update inserts a downlink for a split page, also record that
 * the F_FOLLOW_RIGHT flag on the child page is cleared and NSN set.
 *
 * Note that both the todelete array and the tuples are marked as belonging
 * to the target buffer; they need not be stored in XLOG if XLogInsert decides
 * to log the whole buffer contents instead.
 */
pub unsafe fn gistXLogUpdate(
    buffer: Buffer,
    todelete: *mut OffsetNumber,
    ntodelete: c_int,
    itup: *mut IndexTuple,
    ituplen: c_int,
    leftchildbuf: Buffer,
) -> XLogRecPtr {
    let mut xlrec: gistxlogPageUpdate = std::mem::zeroed();
    let mut i: c_int;
    let recptr: XLogRecPtr;

    xlrec.ntodelete = ntodelete as uint16;
    xlrec.ntoinsert = ituplen as uint16;

    XLogBeginInsert();
    XLogRegisterData(
        &mut xlrec as *mut gistxlogPageUpdate as *mut c_char,
        core::mem::size_of::<gistxlogPageUpdate>(),
    );

    XLogRegisterBuffer(0, buffer, REGBUF_STANDARD as uint8);
    XLogRegisterBufData(
        0,
        todelete as *mut c_char,
        core::mem::size_of::<OffsetNumber>() * ntodelete as usize,
    );

    /* new tuples */
    i = 0;
    while i < ituplen {
        XLogRegisterBufData(
            0,
            *itup.add(i as usize) as *mut c_char,
            IndexTupleSize(*itup.add(i as usize)),
        );
        i += 1;
    }

    /*
     * Include a full page image of the child buf. (only necessary if a
     * checkpoint happened since the child page was split)
     */
    if BufferIsValid(leftchildbuf) {
        XLogRegisterBuffer(1, leftchildbuf, REGBUF_STANDARD as uint8);
    }

    recptr = XLogInsert(RM_GIST_ID, XLOG_GIST_PAGE_UPDATE);

    recptr
}

/*
 * Write XLOG record describing a delete of leaf index tuples marked as DEAD
 * during new tuple insertion.  One may think that this case is already covered
 * by gistXLogUpdate().  But deletion of index tuples might conflict with
 * standby queries and needs special handling.
 */
pub unsafe fn gistXLogDelete(
    buffer: Buffer,
    todelete: *mut OffsetNumber,
    ntodelete: c_int,
    snapshotConflictHorizon: TransactionId,
    heaprel: Relation,
) -> XLogRecPtr {
    let mut xlrec: gistxlogDelete = std::mem::zeroed();
    let recptr: XLogRecPtr;

    xlrec.isCatalogRel = RelationIsAccessibleInLogicalDecoding(heaprel);
    xlrec.snapshotConflictHorizon = snapshotConflictHorizon;
    xlrec.ntodelete = ntodelete as uint16;

    XLogBeginInsert();
    XLogRegisterData(
        &mut xlrec as *mut gistxlogDelete as *mut c_char,
        SizeOfGistxlogDelete,
    );

    /*
     * We need the target-offsets array whether or not we store the whole
     * buffer, to allow us to find the snapshotConflictHorizon on a standby
     * server.
     */
    XLogRegisterData(
        todelete as *mut c_char,
        ntodelete as usize * core::mem::size_of::<OffsetNumber>(),
    );

    XLogRegisterBuffer(0, buffer, REGBUF_STANDARD as uint8);

    recptr = XLogInsert(RM_GIST_ID, XLOG_GIST_DELETE);

    recptr
}

// ---------------------------------------------------------------------------
// Local stubs for unported dependencies.
// ---------------------------------------------------------------------------

// XLogReaderState stub needs the EndRecPtr field accessed throughout this file.
#[repr(C)]
pub struct XLogReaderState {
    EndRecPtr: XLogRecPtr,
} // TODO: access/xlogreader.rs
type XLogRedoAction = c_int; // TODO: access/xlogutils.rs
type Buffer = c_int; // TODO: storage/buf.rs
type Page = *mut c_char; // TODO: storage/bufpage.rs
type Item = *mut c_char; // TODO: storage/item.rs
type IndexTuple = *mut c_void; // TODO: access/itup.rs
type OffsetNumber = u16; // TODO: storage/off.rs
type FullTransactionId = u64; // TODO: access/transam/xlogdefs.rs
type GistNSN = XLogRecPtr; // TODO: access/gist.rs
type RelFileLocator = c_void; // TODO: storage/relfilelocator.rs

// SplitPageLayout stub needs next/buffer/block.num/list/lenlist accessed in
// gistXLogSplit.
#[repr(C)]
pub struct SplitPageLayoutBlock {
    num: c_int,
}
#[repr(C)]
pub struct SplitPageLayout {
    block: SplitPageLayoutBlock,
    list: *mut c_char,
    lenlist: Size,
    buffer: Buffer,
    next: *mut SplitPageLayout,
} // TODO: access/gist_private.rs

// Relation stub needs the rd_locator field accessed in gistXLogPageReuse.
#[repr(C)]
pub struct RelationData {
    rd_locator: RelFileLocator,
}
type Relation = *mut RelationData; // TODO: utils/rel.rs

const BLK_NEEDS_REDO: XLogRedoAction = 0; // TODO: access/xlogutils.rs
const BLK_RESTORED: XLogRedoAction = 2; // TODO: access/xlogutils.rs
const InvalidBuffer: Buffer = 0; // TODO: storage/buf.rs
const FirstOffsetNumber: OffsetNumber = 1; // TODO: storage/off.rs
const InvalidOffsetNumber: OffsetNumber = 0; // TODO: storage/off.rs
const InvalidBlockNumber: BlockNumber = 0xFFFFFFFF; // TODO: storage/block.rs
const GIST_ROOT_BLKNO: BlockNumber = 0; // TODO: access/gist_private.rs
const F_LEAF: c_int = 1 << 0; // TODO: access/gist.rs
const XLR_INFO_MASK: uint8 = 0x0F; // TODO: access/xlogrecord.rs
const REGBUF_STANDARD: c_int = 0x02; // TODO: access/xloginsert.rs
const REGBUF_WILL_INIT: c_int = 0x04 | 0x01; // TODO: access/xloginsert.rs
const RM_GIST_ID: u8 = 8; // TODO: access/rmgrlist.rs
const XLOG_MARK_UNIMPORTANT: c_int = 0x02; // TODO: access/xlog.rs
const MASK_MARKER: c_int = 0; // TODO: access/bufmask.rs
const InHotStandby: bool = false; // TODO: storage/standby.rs

#[allow(non_snake_case)]
unsafe fn XLogReadBufferForRedo(
    _record: *mut XLogReaderState,
    _block_id: uint8,
    _buf: *mut Buffer,
) -> XLogRedoAction { unimplemented!() }

#[allow(non_snake_case)]
unsafe fn XLogInitBufferForRedo(_record: *mut XLogReaderState, _block_id: uint8) -> Buffer { unimplemented!() }

#[allow(non_snake_case)]
unsafe fn XLogRecGetData(_record: *mut XLogReaderState) -> *mut c_char { unimplemented!() }

#[allow(non_snake_case)]
unsafe fn XLogRecGetBlockData(
    _record: *mut XLogReaderState,
    _block_id: uint8,
    _len: *mut Size,
) -> *mut c_char { unimplemented!() }

#[allow(non_snake_case)]
unsafe fn XLogRecGetInfo(_record: *mut XLogReaderState) -> uint8 { unimplemented!() }

#[allow(non_snake_case)]
unsafe fn XLogRecHasBlockRef(_record: *mut XLogReaderState, _block_id: uint8) -> bool { unimplemented!() }

#[allow(non_snake_case)]
unsafe fn XLogRecGetBlockTag(
    _record: *mut XLogReaderState,
    _block_id: uint8,
    _rlocator: *mut RelFileLocator,
    _forknum: *mut c_void,
    _blknum: *mut BlockNumber,
) { unimplemented!() }

#[allow(non_snake_case)]
unsafe fn BufferGetPage(_buffer: Buffer) -> Page {
    unimplemented!() // TODO: storage/bufmgr.h
}

#[allow(non_snake_case)]
unsafe fn BufferIsValid(_buffer: Buffer) -> bool { crate::access::nbtree::nbtpage::BufferIsValid(_buffer) }

#[allow(non_snake_case)]
unsafe fn MarkBufferDirty(_buffer: Buffer) {
    unimplemented!() // TODO: storage/bufmgr.c
}

#[allow(non_snake_case)]
unsafe fn UnlockReleaseBuffer(_buffer: Buffer) {
    unimplemented!() // TODO: storage/bufmgr.c
}

#[allow(non_snake_case)]
unsafe fn PageSetLSN(_page: Page, _lsn: XLogRecPtr) { crate::storage::bufpage::PageSetLSN(_page, _lsn) }

#[allow(non_snake_case)]
unsafe fn PageIsEmpty(_page: Page) -> bool {
    unimplemented!() // TODO: storage/bufpage.h
}

#[allow(non_snake_case)]
unsafe fn PageGetMaxOffsetNumber(_page: Page) -> OffsetNumber {
    unimplemented!() // TODO: storage/bufpage.h
}

#[allow(non_snake_case)]
unsafe fn PageAddItem(
    _page: Page,
    _item: Item,
    _size: Size,
    _offnum: OffsetNumber,
    _overwrite: bool,
    _is_heap: bool,
) -> OffsetNumber { crate::storage::bufpage::PageAddItem(_page, _item, _size, _offnum, _overwrite, _is_heap) }

#[allow(non_snake_case)]
unsafe fn PageIndexTupleOverwrite(
    _page: Page,
    _offnum: OffsetNumber,
    _newtup: Item,
    _newsize: Size,
) -> bool {
    unimplemented!() // TODO: storage/bufpage.c
}

#[allow(non_snake_case)]
unsafe fn PageIndexMultiDelete(_page: Page, _itemnos: *mut OffsetNumber, _nitems: c_int) {
    unimplemented!() // TODO: storage/bufpage.c
}

#[allow(non_snake_case)]
unsafe fn PageIndexTupleDelete(_page: Page, _offnum: OffsetNumber) {
    unimplemented!() // TODO: storage/bufpage.c
}

#[allow(non_snake_case)]
unsafe fn OffsetNumberNext(_offnum: OffsetNumber) -> OffsetNumber { crate::storage::off::OffsetNumberNext(_offnum) }

#[allow(non_snake_case)]
unsafe fn IndexTupleSize(_itup: IndexTuple) -> Size {
    unimplemented!() // TODO: access/itup.h
}

#[allow(non_snake_case)]
unsafe fn GistPageSetNSN(_page: Page, _val: uint64) {
    unimplemented!() // TODO: access/gist.h
}

#[allow(non_snake_case)]
unsafe fn GistClearFollowRight(_page: Page) {
    unimplemented!() // TODO: access/gist.h
}

#[allow(non_snake_case)]
unsafe fn GistMarkFollowRight(_page: Page) {
    unimplemented!() // TODO: access/gist.h
}

#[allow(non_snake_case)]
unsafe fn GistPageIsLeaf(_page: Page) -> bool {
    unimplemented!() // TODO: access/gist.h
}

#[allow(non_snake_case)]
unsafe fn GistMarkTuplesDeleted(_page: Page) {
    unimplemented!() // TODO: access/gist.h
}

#[allow(non_snake_case)]
unsafe fn GistClearPageHasGarbage(_page: Page) {
    unimplemented!() // TODO: access/gist.h
}

#[allow(non_snake_case)]
unsafe fn GistPageSetDeleted(_page: Page, _xid: FullTransactionId) {
    unimplemented!() // TODO: access/gist.h
}

#[allow(non_snake_case)]
unsafe fn GistPageGetOpaque(_page: Page) -> *mut GISTPageOpaqueData {
    unimplemented!() // TODO: access/gist.h
}

#[repr(C)]
struct GISTPageOpaqueData {
    rightlink: BlockNumber,
}

#[allow(non_snake_case)]
unsafe fn GISTInitBuffer(_buffer: Buffer, _flags: c_int) { crate::access::gist::gistutil::GISTInitBuffer(_buffer, _flags as _) }

#[allow(non_snake_case)]
unsafe fn gistfillbuffer(
    _page: Page,
    _itup: *mut IndexTuple,
    _len: c_int,
    _off: OffsetNumber,
) { unimplemented!() }

#[allow(non_snake_case)]
unsafe fn createTempGistContext() -> MemoryContext { unimplemented!() }

#[allow(non_snake_case)]
unsafe fn ResolveRecoveryConflictWithSnapshot(
    _snapshotConflictHorizon: TransactionId,
    _isCatalogRel: bool,
    _locator: RelFileLocator,
) { unimplemented!() }

#[allow(non_snake_case)]
unsafe fn ResolveRecoveryConflictWithSnapshotFullXid(
    _snapshotConflictHorizon: FullTransactionId,
    _isCatalogRel: bool,
    _locator: RelFileLocator,
) { unimplemented!() }

#[allow(non_snake_case)]
unsafe fn RelationIsAccessibleInLogicalDecoding(_rel: Relation) -> bool {
    unimplemented!() // TODO: utils/rel.h
}

#[allow(non_snake_case)]
unsafe fn XLogBeginInsert() {
    unimplemented!() // TODO: access/xloginsert.c
}

#[allow(non_snake_case)]
unsafe fn XLogRegisterData(_data: *mut c_char, _len: Size) {
    unimplemented!() // TODO: access/xloginsert.c
}

#[allow(non_snake_case)]
unsafe fn XLogRegisterBuffer(_block_id: uint8, _buffer: Buffer, _flags: uint8) {
    unimplemented!() // TODO: access/xloginsert.c
}

#[allow(non_snake_case)]
unsafe fn XLogRegisterBufData(_block_id: uint8, _data: *mut c_char, _len: Size) {
    unimplemented!() // TODO: access/xloginsert.c
}

#[allow(non_snake_case)]
unsafe fn XLogSetRecordFlags(_flags: uint8) { crate::access::transam::xloginsert::XLogSetRecordFlags(_flags as _) }

#[allow(non_snake_case)]
unsafe fn XLogInsert(_rmid: u8, _info: uint8) -> XLogRecPtr {
    unimplemented!() // TODO: access/xloginsert.c
}

#[allow(non_snake_case)]
unsafe fn mask_page_lsn_and_checksum(_page: Page) { crate::access::common::bufmask::mask_page_lsn_and_checksum(_page) }

#[allow(non_snake_case)]
unsafe fn mask_page_hint_bits(_page: Page) { crate::access::common::bufmask::mask_page_hint_bits(_page) }

#[allow(non_snake_case)]
unsafe fn mask_unused_space(_page: Page) { crate::access::common::bufmask::mask_unused_space(_page) }

#[allow(non_snake_case)]
unsafe fn mask_lp_flags(_page: Page) { crate::access::common::bufmask::mask_lp_flags(_page) }
