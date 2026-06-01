/*
 * brin_revmap.c
 *		Range map for BRIN indexes
 *
 * The range map (revmap) is a translation structure for BRIN indexes: for each
 * page range there is one summary tuple, and its location is tracked by the
 * revmap.  Whenever a new tuple is inserted into a table that violates the
 * previously recorded summary values, a new tuple is inserted into the index
 * and the revmap is updated to point to it.
 *
 * The revmap is stored in the first pages of the index, immediately following
 * the metapage.  When the revmap needs to be expanded, all tuples on the
 * regular BRIN page at that block (if any) are moved out of the way.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/brin/brin_revmap.c
 */
//! Translated from postgres/src/backend/access/brin/brin_revmap.c
//! (with brin_revmap.h merged in).

use crate::prelude::*;

use std::ffi::c_int;

use crate::storage::block::BlockNumber;
use crate::storage::itemptr::ItemPointerData;
use crate::access::common::tupdesc; // unused-ok
use crate::miscadmin::CHECK_FOR_INTERRUPTS;

// START_CRIT_SECTION / END_CRIT_SECTION are functions in miscadmin, but this
// file uses them with macro-call syntax (matching the C macros).  Provide thin
// local macros that delegate to the real functions.
macro_rules! START_CRIT_SECTION {
    () => {
        crate::miscadmin::START_CRIT_SECTION()
    };
}
macro_rules! END_CRIT_SECTION {
    () => {
        crate::miscadmin::END_CRIT_SECTION()
    };
}

/*
 * In revmap pages, each item stores an ItemPointerData.  These defines let one
 * find the logical revmap page number and index number of the revmap item for
 * the given heap block number.
 */
macro_rules! HEAPBLK_TO_REVMAP_BLK {
    ($pagesPerRange:expr, $heapBlk:expr) => {
        (($heapBlk / $pagesPerRange) / REVMAP_PAGE_MAXITEMS)
    };
}
macro_rules! HEAPBLK_TO_REVMAP_INDEX {
    ($pagesPerRange:expr, $heapBlk:expr) => {
        (($heapBlk / $pagesPerRange) % REVMAP_PAGE_MAXITEMS)
    };
}

#[repr(C)]
pub struct BrinRevmap {
    pub rm_irel: Relation,
    pub rm_pagesPerRange: BlockNumber,
    pub rm_lastRevmapPage: BlockNumber, /* cached from the metapage */
    pub rm_metaBuf: Buffer,
    pub rm_currBuf: Buffer,
}

/* typedef appears in brin_revmap.h */

/*
 * Initialize an access object for a range map.  This must be freed by
 * brinRevmapTerminate when caller is done with it.
 */
pub unsafe fn brinRevmapInitialize(
    idxrel: Relation,
    pagesPerRange: *mut BlockNumber,
) -> *mut BrinRevmap {
    let revmap: *mut BrinRevmap;
    let meta: Buffer;
    let metadata: *mut BrinMetaPageData;
    let page: Page;

    meta = ReadBuffer(idxrel, BRIN_METAPAGE_BLKNO);
    LockBuffer(meta, BUFFER_LOCK_SHARE);
    page = BufferGetPage(meta);
    metadata = PageGetContents(page) as *mut BrinMetaPageData;

    revmap = palloc(::std::mem::size_of::<BrinRevmap>()) as *mut BrinRevmap;
    (*revmap).rm_irel = idxrel;
    (*revmap).rm_pagesPerRange = (*metadata).pagesPerRange;
    (*revmap).rm_lastRevmapPage = (*metadata).lastRevmapPage;
    (*revmap).rm_metaBuf = meta;
    (*revmap).rm_currBuf = InvalidBuffer;

    *pagesPerRange = (*metadata).pagesPerRange;

    LockBuffer(meta, BUFFER_LOCK_UNLOCK);

    return revmap;
}

/*
 * Release resources associated with a revmap access object.
 */
pub unsafe fn brinRevmapTerminate(revmap: *mut BrinRevmap) {
    ReleaseBuffer((*revmap).rm_metaBuf);
    if (*revmap).rm_currBuf != InvalidBuffer {
        ReleaseBuffer((*revmap).rm_currBuf);
    }
    pfree(revmap as *mut _);
}

/*
 * Extend the revmap to cover the given heap block number.
 */
pub unsafe fn brinRevmapExtend(revmap: *mut BrinRevmap, heapBlk: BlockNumber) {
    let mapBlk: BlockNumber /* PG_USED_FOR_ASSERTS_ONLY */;

    mapBlk = revmap_extend_and_get_blkno(revmap, heapBlk);

    /* Ensure the buffer we got is in the expected range */
    Assert!(
        mapBlk != InvalidBlockNumber
            && mapBlk != BRIN_METAPAGE_BLKNO
            && mapBlk <= (*revmap).rm_lastRevmapPage
    );
    let _ = mapBlk;
}

/*
 * Prepare to insert an entry into the revmap; the revmap buffer in which the
 * entry is to reside is locked and returned.  Most callers should call
 * brinRevmapExtend beforehand, as this routine does not extend the revmap if
 * it's not long enough.
 *
 * The returned buffer is also recorded in the revmap struct; finishing that
 * releases the buffer, therefore the caller needn't do it explicitly.
 */
pub unsafe fn brinLockRevmapPageForUpdate(
    revmap: *mut BrinRevmap,
    heapBlk: BlockNumber,
) -> Buffer {
    let rmBuf: Buffer;

    rmBuf = revmap_get_buffer(revmap, heapBlk);
    LockBuffer(rmBuf, BUFFER_LOCK_EXCLUSIVE);

    return rmBuf;
}

/*
 * In the given revmap buffer (locked appropriately by caller), which is used
 * in a BRIN index of pagesPerRange pages per range, set the element
 * corresponding to heap block number heapBlk to the given TID.
 *
 * Once the operation is complete, the caller must update the LSN on the
 * returned buffer.
 *
 * This is used both in regular operation and during WAL replay.
 */
pub unsafe fn brinSetHeapBlockItemptr(
    buf: Buffer,
    pagesPerRange: BlockNumber,
    heapBlk: BlockNumber,
    tid: ItemPointerData,
) {
    let contents: *mut RevmapContents;
    let mut iptr: *mut ItemPointerData;
    let page: Page;

    /* The correct page should already be pinned and locked */
    page = BufferGetPage(buf);
    contents = PageGetContents(page) as *mut RevmapContents;
    iptr = (*contents).rm_tids.as_mut_ptr() as *mut ItemPointerData;
    iptr = iptr.add(HEAPBLK_TO_REVMAP_INDEX!(pagesPerRange, heapBlk) as usize);

    if ItemPointerIsValid(&tid) {
        ItemPointerSet(
            iptr,
            ItemPointerGetBlockNumber(&tid),
            ItemPointerGetOffsetNumber(&tid),
        );
    } else {
        ItemPointerSetInvalid(iptr);
    }
}

/*
 * Fetch the BrinTuple for a given heap block.
 *
 * The buffer containing the tuple is locked, and returned in *buf.  The
 * returned tuple points to the shared buffer and must not be freed; if caller
 * wants to use it after releasing the buffer lock, it must create its own
 * palloc'ed copy.  As an optimization, the caller can pass a pinned buffer
 * *buf on entry, which will avoid a pin-unpin cycle when the next tuple is on
 * the same page as a previous one.
 *
 * If no tuple is found for the given heap range, returns NULL. In that case,
 * *buf might still be updated (and pin must be released by caller), but it's
 * not locked.
 *
 * The output tuple offset within the buffer is returned in *off, and its size
 * is returned in *size.
 */
pub unsafe fn brinGetTupleForHeapBlock(
    revmap: *mut BrinRevmap,
    mut heapBlk: BlockNumber,
    buf: *mut Buffer,
    off: *mut OffsetNumber,
    size: *mut Size,
    mode: c_int,
) -> *mut BrinTuple {
    let idxRel: Relation = (*revmap).rm_irel;
    let mapBlk: BlockNumber;
    let mut contents: *mut RevmapContents;
    let mut iptr: *mut ItemPointerData;
    let mut blk: BlockNumber;
    let mut page: Page;
    let mut lp: ItemId;
    let mut tup: *mut BrinTuple;
    let mut previptr: ItemPointerData = ::std::mem::zeroed();

    /* normalize the heap block number to be the first page in the range */
    heapBlk = (heapBlk / (*revmap).rm_pagesPerRange) * (*revmap).rm_pagesPerRange;

    /*
     * Compute the revmap page number we need.  If Invalid is returned (i.e.,
     * the revmap page hasn't been created yet), the requested page range is
     * not summarized.
     */
    mapBlk = revmap_get_blkno(revmap, heapBlk);
    if mapBlk == InvalidBlockNumber {
        *off = InvalidOffsetNumber;
        return std::ptr::null_mut();
    }

    ItemPointerSetInvalid(&mut previptr);
    loop {
        CHECK_FOR_INTERRUPTS();

        if (*revmap).rm_currBuf == InvalidBuffer
            || BufferGetBlockNumber((*revmap).rm_currBuf) != mapBlk
        {
            if (*revmap).rm_currBuf != InvalidBuffer {
                ReleaseBuffer((*revmap).rm_currBuf);
            }

            Assert!(mapBlk != InvalidBlockNumber);
            (*revmap).rm_currBuf = ReadBuffer((*revmap).rm_irel, mapBlk);
        }

        LockBuffer((*revmap).rm_currBuf, BUFFER_LOCK_SHARE);

        contents =
            PageGetContents(BufferGetPage((*revmap).rm_currBuf)) as *mut RevmapContents;
        iptr = (*contents).rm_tids.as_mut_ptr() as *mut ItemPointerData;
        iptr = iptr.add(HEAPBLK_TO_REVMAP_INDEX!((*revmap).rm_pagesPerRange, heapBlk) as usize);

        if !ItemPointerIsValid(iptr) {
            LockBuffer((*revmap).rm_currBuf, BUFFER_LOCK_UNLOCK);
            return std::ptr::null_mut();
        }

        /*
         * Check the TID we got in a previous iteration, if any, and save the
         * current TID we got from the revmap; if we loop, we can sanity-check
         * that the next one we get is different.  Otherwise we might be stuck
         * looping forever if the revmap is somehow badly broken.
         */
        if ItemPointerIsValid(&previptr) && ItemPointerEquals(&mut previptr, iptr) {
            ereport!(
                ERROR,
                "corrupted BRIN index: inconsistent range map"
            );
        }
        previptr = *iptr;

        blk = ItemPointerGetBlockNumber(iptr);
        *off = ItemPointerGetOffsetNumber(iptr);

        LockBuffer((*revmap).rm_currBuf, BUFFER_LOCK_UNLOCK);

        /* Ok, got a pointer to where the BrinTuple should be. Fetch it. */
        if !BufferIsValid(*buf) || BufferGetBlockNumber(*buf) != blk {
            if BufferIsValid(*buf) {
                ReleaseBuffer(*buf);
            }
            *buf = ReadBuffer(idxRel, blk);
        }
        LockBuffer(*buf, mode);
        page = BufferGetPage(*buf);

        /* If we land on a revmap page, start over */
        if BRIN_IS_REGULAR_PAGE(page) {
            /*
             * If the offset number is greater than what's in the page, it's
             * possible that the range was desummarized concurrently. Just
             * return NULL to handle that case.
             */
            if *off > PageGetMaxOffsetNumber(page) {
                LockBuffer(*buf, BUFFER_LOCK_UNLOCK);
                return std::ptr::null_mut();
            }

            lp = PageGetItemId(page, *off);
            if ItemIdIsUsed(lp) {
                tup = PageGetItem(page, lp) as *mut BrinTuple;

                if (*tup).bt_blkno == heapBlk {
                    if !size.is_null() {
                        *size = ItemIdGetLength(lp) as Size;
                    }
                    /* found it! */
                    return tup;
                }
            }
        }

        /*
         * No luck. Assume that the revmap was updated concurrently.
         */
        LockBuffer(*buf, BUFFER_LOCK_UNLOCK);
    }
    /* not reached, but keep compiler quiet */
}

/*
 * Delete an index tuple, marking a page range as unsummarized.
 *
 * Index must be locked in ShareUpdateExclusiveLock mode.
 *
 * Return false if caller should retry.
 */
pub unsafe fn brinRevmapDesummarizeRange(idxrel: Relation, heapBlk: BlockNumber) -> bool {
    let revmap: *mut BrinRevmap;
    let mut pagesPerRange: BlockNumber = 0;
    let contents: *mut RevmapContents;
    let mut iptr: *mut ItemPointerData;
    let mut invalidIptr: ItemPointerData = ::std::mem::zeroed();
    let revmapBlk: BlockNumber;
    let revmapBuf: Buffer;
    let regBuf: Buffer;
    let revmapPg: Page;
    let regPg: Page;
    let revmapOffset: OffsetNumber;
    let regOffset: OffsetNumber;
    let lp: ItemId;

    revmap = brinRevmapInitialize(idxrel, &mut pagesPerRange);

    revmapBlk = revmap_get_blkno(revmap, heapBlk);
    if !BlockNumberIsValid(revmapBlk) {
        /* revmap page doesn't exist: range not summarized, we're done */
        brinRevmapTerminate(revmap);
        return true;
    }

    /* Lock the revmap page, obtain the index tuple pointer from it */
    revmapBuf = brinLockRevmapPageForUpdate(revmap, heapBlk);
    revmapPg = BufferGetPage(revmapBuf);
    revmapOffset = HEAPBLK_TO_REVMAP_INDEX!((*revmap).rm_pagesPerRange, heapBlk) as OffsetNumber;

    contents = PageGetContents(revmapPg) as *mut RevmapContents;
    iptr = (*contents).rm_tids.as_mut_ptr() as *mut ItemPointerData;
    iptr = iptr.add(revmapOffset as usize);

    if !ItemPointerIsValid(iptr) {
        /* no index tuple: range not summarized, we're done */
        LockBuffer(revmapBuf, BUFFER_LOCK_UNLOCK);
        brinRevmapTerminate(revmap);
        return true;
    }

    regBuf = ReadBuffer(idxrel, ItemPointerGetBlockNumber(iptr));
    LockBuffer(regBuf, BUFFER_LOCK_EXCLUSIVE);
    regPg = BufferGetPage(regBuf);

    /* if this is no longer a regular page, tell caller to start over */
    if !BRIN_IS_REGULAR_PAGE(regPg) {
        LockBuffer(revmapBuf, BUFFER_LOCK_UNLOCK);
        LockBuffer(regBuf, BUFFER_LOCK_UNLOCK);
        brinRevmapTerminate(revmap);
        return false;
    }

    regOffset = ItemPointerGetOffsetNumber(iptr);
    if regOffset > PageGetMaxOffsetNumber(regPg) {
        ereport!(
            ERROR,
            "corrupted BRIN index: inconsistent range map"
        );
    }

    lp = PageGetItemId(regPg, regOffset);
    if !ItemIdIsUsed(lp) {
        ereport!(
            ERROR,
            "corrupted BRIN index: inconsistent range map"
        );
    }

    /*
     * Placeholder tuples only appear during unfinished summarization, and we
     * hold ShareUpdateExclusiveLock, so this function cannot run concurrently
     * with that.  So any placeholder tuples that exist are leftovers from a
     * crashed or aborted summarization; remove them silently.
     */

    START_CRIT_SECTION!();

    ItemPointerSetInvalid(&mut invalidIptr);
    brinSetHeapBlockItemptr(revmapBuf, (*revmap).rm_pagesPerRange, heapBlk, invalidIptr);
    PageIndexTupleDeleteNoCompact(regPg, regOffset);
    /* XXX record free space in FSM? */

    MarkBufferDirty(regBuf);
    MarkBufferDirty(revmapBuf);

    if RelationNeedsWAL(idxrel) {
        let mut xlrec: xl_brin_desummarize = ::std::mem::zeroed();
        let recptr: XLogRecPtr;

        xlrec.pagesPerRange = (*revmap).rm_pagesPerRange;
        xlrec.heapBlk = heapBlk;
        xlrec.regOffset = regOffset;

        XLogBeginInsert();
        XLogRegisterData(&mut xlrec as *mut _ as *mut _, SizeOfBrinDesummarize as c_int);
        XLogRegisterBuffer(0, revmapBuf, 0);
        XLogRegisterBuffer(1, regBuf, REGBUF_STANDARD);
        recptr = XLogInsert(RM_BRIN_ID, XLOG_BRIN_DESUMMARIZE);
        PageSetLSN(revmapPg, recptr);
        PageSetLSN(regPg, recptr);
    }

    END_CRIT_SECTION!();

    UnlockReleaseBuffer(regBuf);
    LockBuffer(revmapBuf, BUFFER_LOCK_UNLOCK);
    brinRevmapTerminate(revmap);

    return true;
}

/*
 * Given a heap block number, find the corresponding physical revmap block
 * number and return it.  If the revmap page hasn't been allocated yet, return
 * InvalidBlockNumber.
 */
unsafe fn revmap_get_blkno(revmap: *mut BrinRevmap, heapBlk: BlockNumber) -> BlockNumber {
    let targetblk: BlockNumber;

    /* obtain revmap block number, skip 1 for metapage block */
    targetblk = HEAPBLK_TO_REVMAP_BLK!((*revmap).rm_pagesPerRange, heapBlk) + 1;

    /* Normal case: the revmap page is already allocated */
    if targetblk <= (*revmap).rm_lastRevmapPage {
        return targetblk;
    }

    return InvalidBlockNumber;
}

/*
 * Obtain and return a buffer containing the revmap page for the given heap
 * page.  The revmap must have been previously extended to cover that page.
 * The returned buffer is also recorded in the revmap struct; finishing that
 * releases the buffer, therefore the caller needn't do it explicitly.
 */
unsafe fn revmap_get_buffer(revmap: *mut BrinRevmap, heapBlk: BlockNumber) -> Buffer {
    let mapBlk: BlockNumber;

    /* Translate the heap block number to physical index location. */
    mapBlk = revmap_get_blkno(revmap, heapBlk);

    if mapBlk == InvalidBlockNumber {
        elog!(ERROR, "revmap does not cover heap block {}", heapBlk);
    }

    /* Ensure the buffer we got is in the expected range */
    Assert!(mapBlk != BRIN_METAPAGE_BLKNO && mapBlk <= (*revmap).rm_lastRevmapPage);

    /*
     * Obtain the buffer from which we need to read.  If we already have the
     * correct buffer in our access struct, use that; otherwise, release that,
     * (if valid) and read the one we need.
     */
    if (*revmap).rm_currBuf == InvalidBuffer
        || mapBlk != BufferGetBlockNumber((*revmap).rm_currBuf)
    {
        if (*revmap).rm_currBuf != InvalidBuffer {
            ReleaseBuffer((*revmap).rm_currBuf);
        }

        (*revmap).rm_currBuf = ReadBuffer((*revmap).rm_irel, mapBlk);
    }

    return (*revmap).rm_currBuf;
}

/*
 * Given a heap block number, find the corresponding physical revmap block
 * number and return it. If the revmap page hasn't been allocated yet, extend
 * the revmap until it is.
 */
unsafe fn revmap_extend_and_get_blkno(
    revmap: *mut BrinRevmap,
    heapBlk: BlockNumber,
) -> BlockNumber {
    let targetblk: BlockNumber;

    /* obtain revmap block number, skip 1 for metapage block */
    targetblk = HEAPBLK_TO_REVMAP_BLK!((*revmap).rm_pagesPerRange, heapBlk) + 1;

    /* Extend the revmap, if necessary */
    while targetblk > (*revmap).rm_lastRevmapPage {
        CHECK_FOR_INTERRUPTS();
        revmap_physical_extend(revmap);
    }

    return targetblk;
}

/*
 * Try to extend the revmap by one page.  This might not happen for a number of
 * reasons; caller is expected to retry until the expected outcome is obtained.
 */
unsafe fn revmap_physical_extend(revmap: *mut BrinRevmap) {
    let buf: Buffer;
    let page: Page;
    let metapage: Page;
    let metadata: *mut BrinMetaPageData;
    let mapBlk: BlockNumber;
    let nblocks: BlockNumber;
    let irel: Relation = (*revmap).rm_irel;

    /*
     * Lock the metapage. This locks out concurrent extensions of the revmap,
     * but note that we still need to grab the relation extension lock because
     * another backend can extend the index with regular BRIN pages.
     */
    LockBuffer((*revmap).rm_metaBuf, BUFFER_LOCK_EXCLUSIVE);
    metapage = BufferGetPage((*revmap).rm_metaBuf);
    metadata = PageGetContents(metapage) as *mut BrinMetaPageData;

    /*
     * Check that our cached lastRevmapPage value was up-to-date; if it
     * wasn't, update the cached copy and have caller start over.
     */
    if (*metadata).lastRevmapPage != (*revmap).rm_lastRevmapPage {
        (*revmap).rm_lastRevmapPage = (*metadata).lastRevmapPage;
        LockBuffer((*revmap).rm_metaBuf, BUFFER_LOCK_UNLOCK);
        return;
    }
    mapBlk = (*metadata).lastRevmapPage + 1;

    nblocks = RelationGetNumberOfBlocks(irel);
    if mapBlk < nblocks {
        buf = ReadBuffer(irel, mapBlk);
        LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
        page = BufferGetPage(buf);
    } else {
        buf = ExtendBufferedRel(BMR_REL(irel), MAIN_FORKNUM, std::ptr::null_mut(), EB_LOCK_FIRST);
        if BufferGetBlockNumber(buf) != mapBlk {
            /*
             * Very rare corner case: somebody extended the relation
             * concurrently after we read its length.  If this happens, give
             * up and have caller start over.  We will have to evacuate that
             * page from under whoever is using it.
             */
            LockBuffer((*revmap).rm_metaBuf, BUFFER_LOCK_UNLOCK);
            UnlockReleaseBuffer(buf);
            return;
        }
        page = BufferGetPage(buf);
    }

    /* Check that it's a regular block (or an empty page) */
    if !PageIsNew(page) && !BRIN_IS_REGULAR_PAGE(page) {
        elog!(
            ERROR,
            "unexpected page type 0x{:04X} in BRIN index \"{}\" block {}",
            BrinPageType(page),
            std::ffi::CStr::from_ptr(RelationGetRelationName(irel)).to_string_lossy(),
            BufferGetBlockNumber(buf)
        );
    }

    /* If the page is in use, evacuate it and restart */
    if brin_start_evacuating_page(irel, buf) {
        LockBuffer((*revmap).rm_metaBuf, BUFFER_LOCK_UNLOCK);
        brin_evacuate_page(irel, (*revmap).rm_pagesPerRange, revmap, buf);

        /* have caller start over */
        return;
    }

    /*
     * Ok, we have now locked the metapage and the target block. Re-initialize
     * the target block as a revmap page, and update the metapage.
     */
    START_CRIT_SECTION!();

    /* the rm_tids array is initialized to all invalid by PageInit */
    brin_page_init(page, BRIN_PAGETYPE_REVMAP);
    MarkBufferDirty(buf);

    (*metadata).lastRevmapPage = mapBlk;

    /*
     * Set pd_lower just past the end of the metadata.  This is essential,
     * because without doing so, metadata will be lost if xlog.c compresses
     * the page.  (We must do this here because pre-v11 versions of PG did not
     * set the metapage's pd_lower correctly, so a pg_upgraded index might
     * contain the wrong value.)
     */
    (*(metapage as PageHeader)).pd_lower = (((metadata as *mut c_char)
        .add(::std::mem::size_of::<BrinMetaPageData>()))
        as isize
        - metapage as isize) as u16;

    MarkBufferDirty((*revmap).rm_metaBuf);

    if RelationNeedsWAL((*revmap).rm_irel) {
        let mut xlrec: xl_brin_revmap_extend = ::std::mem::zeroed();
        let recptr: XLogRecPtr;

        xlrec.targetBlk = mapBlk;

        XLogBeginInsert();
        XLogRegisterData(&mut xlrec as *mut _ as *mut _, SizeOfBrinRevmapExtend as c_int);
        XLogRegisterBuffer(0, (*revmap).rm_metaBuf, REGBUF_STANDARD);

        XLogRegisterBuffer(1, buf, REGBUF_WILL_INIT);

        recptr = XLogInsert(RM_BRIN_ID, XLOG_BRIN_REVMAP_EXTEND);
        PageSetLSN(metapage, recptr);
        PageSetLSN(page, recptr);
    }

    END_CRIT_SECTION!();

    LockBuffer((*revmap).rm_metaBuf, BUFFER_LOCK_UNLOCK);

    UnlockReleaseBuffer(buf);
}

// ---------------------------------------------------------------------------
// Local stubs for as-yet-unported dependencies.
// ---------------------------------------------------------------------------

// Types (stubs)
pub use crate::utils::rel::Relation;
pub type Buffer = c_int;
pub type Page = *mut std::ffi::c_void;
pub type PageHeader = *mut PageHeaderData;
pub type ItemId = *mut std::ffi::c_void;
pub type OffsetNumber = u16;
pub type Size = usize;
pub type XLogRecPtr = u64;

#[repr(C)]
pub struct PageHeaderData {
    pub pd_lower: u16,
}

#[repr(C)]
pub struct BrinMetaPageData {
    pub pagesPerRange: BlockNumber,
    pub lastRevmapPage: BlockNumber,
}

#[repr(C)]
pub struct RevmapContents {
    pub rm_tids: [ItemPointerData; FLEXIBLE_ARRAY_MEMBER],
}

pub use crate::access::brin::brin_tuple::BrinTuple;

#[repr(C)]
pub struct xl_brin_desummarize {
    pub pagesPerRange: BlockNumber,
    pub heapBlk: BlockNumber,
    pub regOffset: OffsetNumber,
}

#[repr(C)]
pub struct xl_brin_revmap_extend {
    pub targetBlk: BlockNumber,
}

const FLEXIBLE_ARRAY_MEMBER: usize = 0;

// Constants (stubs)
pub const InvalidBuffer: Buffer = 0;
pub const InvalidBlockNumber: BlockNumber = 0xFFFF_FFFF;
pub const BRIN_METAPAGE_BLKNO: BlockNumber = 0;
pub const InvalidOffsetNumber: OffsetNumber = 0;
pub const BUFFER_LOCK_UNLOCK: c_int = 0;
pub const BUFFER_LOCK_SHARE: c_int = 1;
pub const BUFFER_LOCK_EXCLUSIVE: c_int = 2;
pub const REVMAP_PAGE_MAXITEMS: BlockNumber = 1;
pub const BRIN_PAGETYPE_REVMAP: u16 = 0;
pub const MAIN_FORKNUM: c_int = 0;
pub const EB_LOCK_FIRST: u32 = 0;
pub const REGBUF_STANDARD: c_int = 0;
pub const REGBUF_WILL_INIT: c_int = 0;
pub const RM_BRIN_ID: u8 = 0;
pub const XLOG_BRIN_DESUMMARIZE: u8 = 0;
pub const XLOG_BRIN_REVMAP_EXTEND: u8 = 0;
pub const SizeOfBrinDesummarize: usize = 0;
pub const SizeOfBrinRevmapExtend: usize = 0;

// Functions (stubs)
unsafe fn ReadBuffer(_rel: Relation, _blk: BlockNumber) -> Buffer { unimplemented!() /* storage/bufmgr.c */ }
unsafe fn LockBuffer(_buf: Buffer, _mode: c_int) { unimplemented!() /* storage/bufmgr.c */ }
unsafe fn ReleaseBuffer(_buf: Buffer) { unimplemented!() /* storage/bufmgr.c */ }
unsafe fn UnlockReleaseBuffer(_buf: Buffer) { unimplemented!() /* storage/bufmgr.c */ }
unsafe fn MarkBufferDirty(_buf: Buffer) { unimplemented!() /* storage/bufmgr.c */ }
unsafe fn BufferGetPage(_buf: Buffer) -> Page { unimplemented!() /* storage/bufmgr.h */ }
unsafe fn BufferGetBlockNumber(_buf: Buffer) -> BlockNumber { unimplemented!() /* storage/bufmgr.c */ }
unsafe fn BufferIsValid(_buf: Buffer) -> bool { unimplemented!() /* storage/bufmgr.h */ }
unsafe fn ExtendBufferedRel(_bmr: BufferManagerRelation, _fork: c_int, _strategy: *mut std::ffi::c_void, _flags: u32) -> Buffer { unimplemented!() /* storage/bufmgr.c */ }
unsafe fn PageGetContents(_page: Page) -> *mut c_char { unimplemented!() /* storage/bufpage.h */ }
unsafe fn PageGetItemId(_page: Page, _off: OffsetNumber) -> ItemId { unimplemented!() /* storage/bufpage.h */ }
unsafe fn PageGetItem(_page: Page, _lp: ItemId) -> *mut std::ffi::c_void { unimplemented!() /* storage/bufpage.h */ }
unsafe fn PageGetMaxOffsetNumber(_page: Page) -> OffsetNumber { unimplemented!() /* storage/bufpage.h */ }
unsafe fn PageIsNew(_page: Page) -> bool { unimplemented!() /* storage/bufpage.h */ }
unsafe fn PageSetLSN(_page: Page, _lsn: XLogRecPtr) { unimplemented!() /* storage/bufpage.h */ }
unsafe fn PageIndexTupleDeleteNoCompact(_page: Page, _off: OffsetNumber) { unimplemented!() /* storage/bufpage.c */ }
unsafe fn ItemIdIsUsed(_lp: ItemId) -> bool { unimplemented!() /* storage/itemid.h */ }
unsafe fn ItemIdGetLength(_lp: ItemId) -> u32 { unimplemented!() /* storage/itemid.h */ }
unsafe fn ItemPointerIsValid(_iptr: *const ItemPointerData) -> bool { unimplemented!() /* storage/itemptr.h */ }
unsafe fn ItemPointerSetInvalid(_iptr: *mut ItemPointerData) { unimplemented!() /* storage/itemptr.h */ }
unsafe fn ItemPointerSet(_iptr: *mut ItemPointerData, _blk: BlockNumber, _off: OffsetNumber) { unimplemented!() /* storage/itemptr.h */ }
unsafe fn ItemPointerEquals(_a: *mut ItemPointerData, _b: *mut ItemPointerData) -> bool { unimplemented!() /* storage/itemptr.c */ }
unsafe fn ItemPointerGetBlockNumber(_iptr: *const ItemPointerData) -> BlockNumber { unimplemented!() /* storage/itemptr.h */ }
unsafe fn ItemPointerGetOffsetNumber(_iptr: *const ItemPointerData) -> OffsetNumber { unimplemented!() /* storage/itemptr.h */ }
unsafe fn BRIN_IS_REGULAR_PAGE(_page: Page) -> bool { unimplemented!() /* access/brin_page.h */ }
unsafe fn BrinPageType(_page: Page) -> u16 { unimplemented!() /* access/brin_page.h */ }
unsafe fn BlockNumberIsValid(_blk: BlockNumber) -> bool { unimplemented!() /* storage/block.h */ }
unsafe fn RelationGetNumberOfBlocks(_rel: Relation) -> BlockNumber { unimplemented!() /* storage/bufmgr.h */ }
unsafe fn RelationGetRelationName(_rel: Relation) -> *const c_char { unimplemented!() /* utils/rel.h */ }
unsafe fn RelationNeedsWAL(_rel: Relation) -> bool { unimplemented!() /* utils/rel.h */ }
unsafe fn brin_page_init(_page: Page, _type: u16) { unimplemented!() /* access/brin_pageops.c */ }
unsafe fn brin_start_evacuating_page(_irel: Relation, _buf: Buffer) -> bool { unimplemented!() /* access/brin_pageops.c */ }
unsafe fn brin_evacuate_page(_irel: Relation, _pagesPerRange: BlockNumber, _revmap: *mut BrinRevmap, _buf: Buffer) { unimplemented!() /* access/brin_pageops.c */ }
unsafe fn XLogBeginInsert() { unimplemented!() /* access/xloginsert.c */ }
unsafe fn XLogRegisterData(_data: *mut c_char, _len: c_int) { unimplemented!() /* access/xloginsert.c */ }
unsafe fn XLogRegisterBuffer(_block_id: u8, _buf: Buffer, _flags: c_int) { unimplemented!() /* access/xloginsert.c */ }
unsafe fn XLogInsert(_rmid: u8, _info: u8) -> XLogRecPtr { unimplemented!() /* access/xloginsert.c */ }
unsafe fn BMR_REL(_rel: Relation) -> BufferManagerRelation { unimplemented!() /* storage/bufmgr.h */ }

#[repr(C)]
pub struct BufferManagerRelation {
    pub rel: Relation,
}
