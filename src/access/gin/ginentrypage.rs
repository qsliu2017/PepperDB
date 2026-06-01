//! src/backend/access/gin/ginentrypage.c
//!
//! routines for handling GIN entry tree pages.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::prelude::*;

use std::mem::size_of;

use crate::pg_config::BLCKSZ;

use crate::access::common::indextuple::{
    index_form_tuple, IndexTuple, IndexTupleData, IndexTupleHasNulls, IndexTupleSize,
    INDEX_SIZE_MASK,
};
use crate::access::gin::ginblock::{
    GinCategoryOffset, GinGetDownlink, GinGetNPosting, GinGetPosting, GinGetPostingOffset,
    GinIsPostingTree, GinItupIsCompressed, GinMaxItemSize, GinNullCategory,
    GinPageGetOpaque, GinPageIsData, GinPageIsLeaf, GinPageRightMost, GinPostingList,
    GinSetDownlink, GinSetNPosting, GinSetPostingOffset, GIN_CAT_NORM_KEY, GIN_ROOT_BLKNO,
};
use crate::access::gin::gin_private::{
    ginCompareAttEntries, gintuple_get_attrnum, gintuple_get_key, GinBtree, GinBtreeData,
    GinBtreeEntryInsertData, GinBtreeStack, GinInitPage, GinPlaceToPageRC, GinState, GPTP_INSERT,
    GPTP_SPLIT,
};
use crate::access::gin::ginpostinglist::ginPostingListDecode;
use crate::storage::block::{BlockNumber, InvalidBlockNumber};
use crate::storage::buf::Buffer;
use crate::storage::bufpage::{
    Item, Page, PageAddItem, PageGetFreeSpace, PageGetItem, PageGetItemId, PageGetMaxOffsetNumber,
    PageGetPageSize, PageGetTempPageCopy, PageIndexTupleDelete,
};
use crate::storage::itemid::ItemIdData;
use crate::storage::itemptr::{ItemPointer, ItemPointerData};
use crate::storage::off::{FirstOffsetNumber, InvalidOffsetNumber, OffsetNumber};
use crate::utils::rel::Relation;

use crate::Assert;

/*
 * Form a tuple for entry tree.
 *
 * If the tuple would be too big to be stored, function throws a suitable
 * error if errorTooBig is true, or returns NULL if errorTooBig is false.
 *
 * See src/backend/access/gin/README for a description of the index tuple
 * format that is being built here.  We build on the assumption that we
 * are making a leaf-level key entry containing a posting list of nipd items.
 * If the caller is actually trying to make a posting-tree entry, non-leaf
 * entry, or pending-list entry, it should pass dataSize = 0 and then overwrite
 * the t_tid fields as necessary.  In any case, 'data' can be NULL to skip
 * filling in the posting list; the caller is responsible for filling it
 * afterwards if data = NULL and nipd > 0.
 */
pub unsafe fn GinFormTuple(
    ginstate: *mut GinState,
    attnum: OffsetNumber,
    key: Datum,
    category: GinNullCategory,
    data: Pointer,
    dataSize: Size,
    nipd: c_int,
    errorTooBig: bool,
) -> IndexTuple {
    let mut datums: [Datum; 2] = [0; 2];
    let mut isnull: [bool; 2] = [false; 2];
    let mut itup: IndexTuple;
    let mut newsize: uint32;

    /* Build the basic tuple: optional column number, plus key datum */
    if (*ginstate).oneCol {
        datums[0] = key;
        isnull[0] = category != GIN_CAT_NORM_KEY;
    } else {
        datums[0] = UInt16GetDatum(attnum);
        isnull[0] = false;
        datums[1] = key;
        isnull[1] = category != GIN_CAT_NORM_KEY;
    }

    itup = index_form_tuple(
        (*ginstate).tupdesc[(attnum - 1) as usize],
        datums.as_ptr(),
        isnull.as_ptr(),
    );

    /*
     * Determine and store offset to the posting list, making sure there is
     * room for the category byte if needed.
     *
     * Note: because index_form_tuple MAXALIGNs the tuple size, there may well
     * be some wasted pad space.  Is it worth recomputing the data length to
     * prevent that?  That would also allow us to Assert that the real data
     * doesn't overlap the GinNullCategory byte, which this code currently
     * takes on faith.
     */
    newsize = IndexTupleSize(itup) as uint32;

    if IndexTupleHasNulls(itup) {
        let minsize: uint32;

        Assert!(category != GIN_CAT_NORM_KEY);
        minsize = GinCategoryOffset((*itup).t_info, (*ginstate).oneCol) as uint32
            + size_of::<GinNullCategory>() as uint32;
        newsize = Max(newsize, minsize);
    }

    newsize = SHORTALIGN(newsize as usize) as uint32;

    GinSetPostingOffset(&mut (*itup).t_tid, newsize);
    GinSetNPosting(&mut (*itup).t_tid, nipd as OffsetNumber);

    /*
     * Add space needed for posting list, if any.  Then check that the tuple
     * won't be too big to store.
     */
    newsize += dataSize as uint32;

    newsize = MAXALIGN(newsize as usize) as uint32;

    if newsize as usize > GinMaxItemSize() {
        if errorTooBig {
            ereport!(
                ERROR,
                "index row size exceeds maximum for index"
            );
            unreachable!()
        }
        pfree(itup as *mut c_void);
        return null_mut();
    }

    /*
     * Resize tuple if needed
     */
    if newsize != IndexTupleSize(itup) as uint32 {
        itup = repalloc(itup as *mut c_void, newsize as usize) as IndexTuple;

        /*
         * PostgreSQL 9.3 and earlier did not clear this new space, so we
         * might find uninitialized padding when reading tuples from disk.
         */
        std::ptr::write_bytes(
            (itup as *mut c_char).add(IndexTupleSize(itup)),
            0,
            newsize as usize - IndexTupleSize(itup),
        );
        /* set new size in tuple header */
        (*itup).t_info &= !INDEX_SIZE_MASK;
        (*itup).t_info |= newsize as u16;
    }

    /*
     * Copy in the posting list, if provided
     */
    if !data.is_null() {
        let ptr = GinGetPosting(itup as *const c_char, &(*itup).t_tid);

        std::ptr::copy_nonoverlapping(data as *const u8, ptr as *mut u8, dataSize);
    }

    /*
     * Insert category byte, if needed
     */
    if category != GIN_CAT_NORM_KEY {
        Assert!(IndexTupleHasNulls(itup));
        GinSetNullCategory(itup, ginstate, category);
    }
    itup
}

/*
 * Read item pointers from leaf entry tuple.
 *
 * Returns a palloc'd array of ItemPointers. The number of items is returned
 * in *nitems.
 */
pub unsafe fn ginReadTuple(
    _ginstate: *mut GinState,
    _attnum: OffsetNumber,
    itup: IndexTuple,
    nitems: *mut c_int,
) -> ItemPointer {
    let ptr: Pointer = GinGetPosting(itup as *const c_char, &(*itup).t_tid);
    let nipd: c_int = GinGetNPosting(&(*itup).t_tid) as c_int;
    let ipd: ItemPointer;
    let mut ndecoded: c_int = 0;

    if GinItupIsCompressed(&(*itup).t_tid) {
        if nipd > 0 {
            ipd = ginPostingListDecode(ptr as *mut _, &mut ndecoded);
            if nipd != ndecoded {
                elog!(
                    ERROR,
                    "number of items mismatch in GIN entry tuple, {} in tuple header, {} decoded",
                    nipd,
                    ndecoded
                );
            }
        } else {
            ipd = palloc(0) as ItemPointer;
        }
    } else {
        ipd = palloc(size_of::<ItemPointerData>() * nipd as usize) as ItemPointer;
        std::ptr::copy_nonoverlapping(
            ptr as *const u8,
            ipd as *mut u8,
            size_of::<ItemPointerData>() * nipd as usize,
        );
    }
    *nitems = nipd;
    ipd
}

/*
 * Form a non-leaf entry tuple by copying the key data from the given tuple,
 * which can be either a leaf or non-leaf entry tuple.
 *
 * Any posting list in the source tuple is not copied.  The specified child
 * block number is inserted into t_tid.
 */
unsafe fn GinFormInteriorTuple(itup: IndexTuple, page: Page, childblk: BlockNumber) -> IndexTuple {
    let nitup: IndexTuple;

    if GinPageIsLeaf(page) && !GinIsPostingTree(&(*itup).t_tid) {
        /* Tuple contains a posting list, just copy stuff before that */
        let mut origsize: uint32 = GinGetPostingOffset(&(*itup).t_tid);

        origsize = MAXALIGN(origsize as usize) as uint32;
        nitup = palloc(origsize as usize) as IndexTuple;
        std::ptr::copy_nonoverlapping(
            itup as *const u8,
            nitup as *mut u8,
            origsize as usize,
        );
        /* ... be sure to fix the size header field ... */
        (*nitup).t_info &= !INDEX_SIZE_MASK;
        (*nitup).t_info |= origsize as u16;
    } else {
        /* Copy the tuple as-is */
        nitup = palloc(IndexTupleSize(itup)) as IndexTuple;
        std::ptr::copy_nonoverlapping(
            itup as *const u8,
            nitup as *mut u8,
            IndexTupleSize(itup),
        );
    }

    /* Now insert the correct downlink */
    GinSetDownlink(&mut (*nitup).t_tid, childblk);

    nitup
}

/*
 * Entry tree is a "static", ie tuple never deletes from it,
 * so we don't use right bound, we use rightmost key instead.
 */
unsafe fn getRightMostTuple(page: Page) -> IndexTuple {
    let maxoff: OffsetNumber = PageGetMaxOffsetNumber(page);

    PageGetItem(page, PageGetItemId(page, maxoff)) as IndexTuple
}

unsafe extern "C" fn entryIsMoveRight(btree: GinBtree, page: Page) -> bool {
    let itup: IndexTuple;
    let attnum: OffsetNumber;
    let key: Datum;
    let mut category: GinNullCategory = 0;

    if GinPageRightMost(page) {
        return false;
    }

    itup = getRightMostTuple(page);
    attnum = gintuple_get_attrnum((*btree).ginstate, itup);
    key = gintuple_get_key((*btree).ginstate, itup, &mut category);

    if ginCompareAttEntries(
        (*btree).ginstate,
        (*btree).entryAttnum,
        (*btree).entryKey,
        (*btree).entryCategory,
        attnum,
        key,
        category,
    ) > 0
    {
        return true;
    }

    false
}

/*
 * Find correct tuple in non-leaf page. It supposed that
 * page correctly chosen and searching value SHOULD be on page
 */
unsafe extern "C" fn entryLocateEntry(btree: GinBtree, stack: *mut GinBtreeStack) -> BlockNumber {
    let mut low: OffsetNumber;
    let mut high: OffsetNumber;
    let maxoff: OffsetNumber;
    let mut itup: IndexTuple = null_mut();
    let mut result: c_int;
    let page: Page = BufferGetPage((*stack).buffer);

    Assert!(!GinPageIsLeaf(page));
    Assert!(!GinPageIsData(page));

    if (*btree).fullScan {
        (*stack).off = FirstOffsetNumber;
        (*stack).predictNumber *= PageGetMaxOffsetNumber(page) as uint32;
        return ((*btree).getLeftMostChild.unwrap())(btree, page);
    }

    low = FirstOffsetNumber;
    high = PageGetMaxOffsetNumber(page);
    maxoff = high;
    Assert!(high >= low);

    high += 1;

    while high > low {
        let mid: OffsetNumber = low + ((high - low) / 2);

        if mid == maxoff && GinPageRightMost(page) {
            /* Right infinity */
            result = -1;
        } else {
            let attnum: OffsetNumber;
            let key: Datum;
            let mut category: GinNullCategory = 0;

            itup = PageGetItem(page, PageGetItemId(page, mid)) as IndexTuple;
            attnum = gintuple_get_attrnum((*btree).ginstate, itup);
            key = gintuple_get_key((*btree).ginstate, itup, &mut category);
            result = ginCompareAttEntries(
                (*btree).ginstate,
                (*btree).entryAttnum,
                (*btree).entryKey,
                (*btree).entryCategory,
                attnum,
                key,
                category,
            );
        }

        if result == 0 {
            (*stack).off = mid;
            Assert!(GinGetDownlink(&(*itup).t_tid) != GIN_ROOT_BLKNO);
            return GinGetDownlink(&(*itup).t_tid);
        } else if result > 0 {
            low = mid + 1;
        } else {
            high = mid;
        }
    }

    Assert!(high >= FirstOffsetNumber && high <= maxoff);

    (*stack).off = high;
    itup = PageGetItem(page, PageGetItemId(page, high)) as IndexTuple;
    Assert!(GinGetDownlink(&(*itup).t_tid) != GIN_ROOT_BLKNO);
    GinGetDownlink(&(*itup).t_tid)
}

/*
 * Searches correct position for value on leaf page.
 * Page should be correctly chosen.
 * Returns true if value found on page.
 */
unsafe extern "C" fn entryLocateLeafEntry(btree: GinBtree, stack: *mut GinBtreeStack) -> bool {
    let page: Page = BufferGetPage((*stack).buffer);
    let mut low: OffsetNumber;
    let mut high: OffsetNumber;

    Assert!(GinPageIsLeaf(page));
    Assert!(!GinPageIsData(page));

    if (*btree).fullScan {
        (*stack).off = FirstOffsetNumber;
        return true;
    }

    low = FirstOffsetNumber;
    high = PageGetMaxOffsetNumber(page);

    if high < low {
        (*stack).off = FirstOffsetNumber;
        return false;
    }

    high += 1;

    while high > low {
        let mid: OffsetNumber = low + ((high - low) / 2);
        let itup: IndexTuple;
        let attnum: OffsetNumber;
        let key: Datum;
        let mut category: GinNullCategory = 0;
        let result: c_int;

        itup = PageGetItem(page, PageGetItemId(page, mid)) as IndexTuple;
        attnum = gintuple_get_attrnum((*btree).ginstate, itup);
        key = gintuple_get_key((*btree).ginstate, itup, &mut category);
        result = ginCompareAttEntries(
            (*btree).ginstate,
            (*btree).entryAttnum,
            (*btree).entryKey,
            (*btree).entryCategory,
            attnum,
            key,
            category,
        );
        if result == 0 {
            (*stack).off = mid;
            return true;
        } else if result > 0 {
            low = mid + 1;
        } else {
            high = mid;
        }
    }

    (*stack).off = high;
    false
}

unsafe extern "C" fn entryFindChildPtr(
    _btree: GinBtree,
    page: Page,
    blkno: BlockNumber,
    storedOff: OffsetNumber,
) -> OffsetNumber {
    let mut i: OffsetNumber;
    let mut maxoff: OffsetNumber = PageGetMaxOffsetNumber(page);
    let mut itup: IndexTuple;

    Assert!(!GinPageIsLeaf(page));
    Assert!(!GinPageIsData(page));

    /* if page isn't changed, we returns storedOff */
    if storedOff >= FirstOffsetNumber && storedOff <= maxoff {
        itup = PageGetItem(page, PageGetItemId(page, storedOff)) as IndexTuple;
        if GinGetDownlink(&(*itup).t_tid) == blkno {
            return storedOff;
        }

        /*
         * we hope, that needed pointer goes to right. It's true if there
         * wasn't a deletion
         */
        i = storedOff + 1;
        while i <= maxoff {
            itup = PageGetItem(page, PageGetItemId(page, i)) as IndexTuple;
            if GinGetDownlink(&(*itup).t_tid) == blkno {
                return i;
            }
            i += 1;
        }
        maxoff = storedOff - 1;
    }

    /* last chance */
    i = FirstOffsetNumber;
    while i <= maxoff {
        itup = PageGetItem(page, PageGetItemId(page, i)) as IndexTuple;
        if GinGetDownlink(&(*itup).t_tid) == blkno {
            return i;
        }
        i += 1;
    }

    InvalidOffsetNumber
}

unsafe extern "C" fn entryGetLeftMostPage(_btree: GinBtree, page: Page) -> BlockNumber {
    let itup: IndexTuple;

    Assert!(!GinPageIsLeaf(page));
    Assert!(!GinPageIsData(page));
    Assert!(PageGetMaxOffsetNumber(page) >= FirstOffsetNumber);

    itup = PageGetItem(page, PageGetItemId(page, FirstOffsetNumber)) as IndexTuple;
    GinGetDownlink(&(*itup).t_tid)
}

unsafe fn entryIsEnoughSpace(
    _btree: GinBtree,
    buf: Buffer,
    off: OffsetNumber,
    insertData: *mut GinBtreeEntryInsertData,
) -> bool {
    let mut releasedsz: Size = 0;
    let addedsz: Size;
    let page: Page = BufferGetPage(buf);

    Assert!(!(*insertData).entry.is_null());
    Assert!(!GinPageIsData(page));

    if (*insertData).isDelete {
        let itup: IndexTuple = PageGetItem(page, PageGetItemId(page, off)) as IndexTuple;

        releasedsz = MAXALIGN(IndexTupleSize(itup)) + size_of::<ItemIdData>();
    }

    addedsz = MAXALIGN(IndexTupleSize((*insertData).entry)) + size_of::<ItemIdData>();

    if PageGetFreeSpace(page) + releasedsz >= addedsz {
        return true;
    }

    false
}

/*
 * Delete tuple on leaf page if tuples existed and we
 * should update it, update old child blkno to new right page
 * if child split occurred
 */
unsafe fn entryPreparePage(
    _btree: GinBtree,
    page: Page,
    off: OffsetNumber,
    insertData: *mut GinBtreeEntryInsertData,
    updateblkno: BlockNumber,
) {
    Assert!(!(*insertData).entry.is_null());
    Assert!(!GinPageIsData(page));

    if (*insertData).isDelete {
        Assert!(GinPageIsLeaf(page));
        PageIndexTupleDelete(page, off);
    }

    if !GinPageIsLeaf(page) && updateblkno != InvalidBlockNumber {
        let itup: IndexTuple = PageGetItem(page, PageGetItemId(page, off)) as IndexTuple;

        GinSetDownlink(&mut (*itup).t_tid, updateblkno);
    }
}

/*
 * Prepare to insert data on an entry page.
 *
 * If it will fit, return GPTP_INSERT after doing whatever setup is needed
 * before we enter the insertion critical section.  *ptp_workspace can be
 * set to pass information along to the execPlaceToPage function.
 *
 * If it won't fit, perform a page split and return two temporary page
 * images into *newlpage and *newrpage, with result GPTP_SPLIT.
 *
 * In neither case should the given page buffer be modified here.
 *
 * Note: on insertion to an internal node, in addition to inserting the given
 * item, the downlink of the existing item at stack->off will be updated to
 * point to updateblkno.
 */
unsafe extern "C" fn entryBeginPlaceToPage(
    btree: GinBtree,
    buf: Buffer,
    stack: *mut GinBtreeStack,
    insertPayload: *mut c_void,
    updateblkno: BlockNumber,
    _ptp_workspace: *mut *mut c_void,
    newlpage: *mut Page,
    newrpage: *mut Page,
) -> GinPlaceToPageRC {
    let insertData: *mut GinBtreeEntryInsertData = insertPayload as *mut GinBtreeEntryInsertData;
    let off: OffsetNumber = (*stack).off;

    /* If it doesn't fit, deal with split case */
    if !entryIsEnoughSpace(btree, buf, off, insertData) {
        entrySplitPage(btree, buf, stack, insertData, updateblkno, newlpage, newrpage);
        return GPTP_SPLIT;
    }

    /* Else, we're ready to proceed with insertion */
    GPTP_INSERT
}

/*
 * Perform data insertion after beginPlaceToPage has decided it will fit.
 *
 * This is invoked within a critical section, and XLOG record creation (if
 * needed) is already started.  The target buffer is registered in slot 0.
 */
unsafe extern "C" fn entryExecPlaceToPage(
    btree: GinBtree,
    buf: Buffer,
    stack: *mut GinBtreeStack,
    insertPayload: *mut c_void,
    updateblkno: BlockNumber,
    _ptp_workspace: *mut c_void,
) {
    let insertData: *mut GinBtreeEntryInsertData = insertPayload as *mut GinBtreeEntryInsertData;
    let page: Page = BufferGetPage(buf);
    let off: OffsetNumber = (*stack).off;
    let placed: OffsetNumber;

    entryPreparePage(btree, page, off, insertData, updateblkno);

    placed = PageAddItem(
        page,
        (*insertData).entry as Item,
        IndexTupleSize((*insertData).entry),
        off,
        false,
        false,
    );
    if placed != off {
        elog!(
            ERROR,
            "failed to add item to index page in \"{:?}\"",
            RelationGetRelationName((*btree).index)
        );
    }

    MarkBufferDirty(buf);

    if RelationNeedsWAL((*btree).index) && !(*btree).isBuild {
        /*
         * This must be static, because it has to survive until XLogInsert,
         * and we can't palloc here.  Ugly, but the XLogInsert infrastructure
         * isn't reentrant anyway.
         */
        static mut DATA: ginxlogInsertEntry = ginxlogInsertEntry {
            offset: 0,
            isDelete: false,
            tuple: IndexTupleData {
                t_tid: ItemPointerData {
                    ip_blkid: crate::storage::block::BlockIdData { bi_hi: 0, bi_lo: 0 },
                    ip_posid: 0,
                },
                t_info: 0,
            },
        };

        DATA.isDelete = (*insertData).isDelete;
        DATA.offset = off;

        XLogRegisterBuffer(0, buf, REGBUF_STANDARD);
        XLogRegisterBufData(
            0,
            &raw mut DATA as *mut c_char,
            core::mem::offset_of!(ginxlogInsertEntry, tuple) as c_int,
        );
        XLogRegisterBufData(
            0,
            (*insertData).entry as *mut c_char,
            IndexTupleSize((*insertData).entry) as c_int,
        );
    }
}

/*
 * Split entry page and insert new data.
 *
 * Returns new temp pages to *newlpage and *newrpage.
 * The original buffer is left untouched.
 */
unsafe fn entrySplitPage(
    btree: GinBtree,
    origbuf: Buffer,
    stack: *mut GinBtreeStack,
    insertData: *mut GinBtreeEntryInsertData,
    updateblkno: BlockNumber,
    newlpage: *mut Page,
    newrpage: *mut Page,
) {
    let off: OffsetNumber = (*stack).off;
    let mut i: OffsetNumber;
    let mut maxoff: OffsetNumber;
    let mut separator: OffsetNumber = InvalidOffsetNumber;
    let mut totalsize: Size = 0;
    let mut lsize: Size;
    let mut size: Size;
    let mut ptr: *mut c_char;
    let mut itup: IndexTuple;
    let mut page: Page;
    let lpage: Page = PageGetTempPageCopy(BufferGetPage(origbuf));
    let rpage: Page = PageGetTempPageCopy(BufferGetPage(origbuf));
    let pageSize: Size = PageGetPageSize(lpage);
    /* could need 2 pages' worth of tuples */
    let mut tupstore: [PGAlignedBlock; 2] = [PGAlignedBlock {
        data: [0; BLCKSZ as usize],
    }; 2];

    entryPreparePage(btree, lpage, off, insertData, updateblkno);

    /*
     * First, append all the existing tuples and the new tuple we're inserting
     * one after another in a temporary workspace.
     */
    maxoff = PageGetMaxOffsetNumber(lpage);
    ptr = tupstore[0].data.as_mut_ptr() as *mut c_char;
    i = FirstOffsetNumber;
    while i <= maxoff {
        if i == off {
            size = MAXALIGN(IndexTupleSize((*insertData).entry));
            std::ptr::copy_nonoverlapping((*insertData).entry as *const u8, ptr as *mut u8, size);
            ptr = ptr.add(size);
            totalsize += size + size_of::<ItemIdData>();
        }

        itup = PageGetItem(lpage, PageGetItemId(lpage, i)) as IndexTuple;
        size = MAXALIGN(IndexTupleSize(itup));
        std::ptr::copy_nonoverlapping(itup as *const u8, ptr as *mut u8, size);
        ptr = ptr.add(size);
        totalsize += size + size_of::<ItemIdData>();

        i += 1;
    }

    if off == maxoff + 1 {
        size = MAXALIGN(IndexTupleSize((*insertData).entry));
        std::ptr::copy_nonoverlapping((*insertData).entry as *const u8, ptr as *mut u8, size);
        ptr = ptr.add(size);
        totalsize += size + size_of::<ItemIdData>();
    }

    /*
     * Initialize the left and right pages, and copy all the tuples back to
     * them.
     */
    GinInitPage(rpage, (*GinPageGetOpaque(lpage)).flags as u32, pageSize);
    GinInitPage(lpage, (*GinPageGetOpaque(rpage)).flags as u32, pageSize);

    ptr = tupstore[0].data.as_mut_ptr() as *mut c_char;
    maxoff += 1;
    lsize = 0;

    page = lpage;
    i = FirstOffsetNumber;
    while i <= maxoff {
        itup = ptr as IndexTuple;

        /*
         * Decide where to split.  We try to equalize the pages' total data
         * size, not number of tuples.
         */
        if lsize > totalsize / 2 {
            if separator == InvalidOffsetNumber {
                separator = i - 1;
            }
            page = rpage;
        } else {
            lsize += MAXALIGN(IndexTupleSize(itup)) + size_of::<ItemIdData>();
        }

        if PageAddItem(
            page,
            itup as Item,
            IndexTupleSize(itup),
            InvalidOffsetNumber,
            false,
            false,
        ) == InvalidOffsetNumber
        {
            elog!(
                ERROR,
                "failed to add item to index page in \"{:?}\"",
                RelationGetRelationName((*btree).index)
            );
        }
        ptr = ptr.add(MAXALIGN(IndexTupleSize(itup)));

        i += 1;
    }

    let _ = separator;

    /* return temp pages to caller */
    *newlpage = lpage;
    *newrpage = rpage;
}

/*
 * Construct insertion payload for inserting the downlink for given buffer.
 */
unsafe extern "C" fn entryPrepareDownlink(_btree: GinBtree, lbuf: Buffer) -> *mut c_void {
    let insertData: *mut GinBtreeEntryInsertData;
    let lpage: Page = BufferGetPage(lbuf);
    let lblkno: BlockNumber = BufferGetBlockNumber(lbuf);
    let itup: IndexTuple;

    itup = getRightMostTuple(lpage);

    insertData = palloc(size_of::<GinBtreeEntryInsertData>()) as *mut GinBtreeEntryInsertData;
    (*insertData).entry = GinFormInteriorTuple(itup, lpage, lblkno);
    (*insertData).isDelete = false;

    insertData as *mut c_void
}

/*
 * Fills new root by rightest values from child.
 * Also called from ginxlog, should not use btree
 */
pub unsafe extern "C" fn ginEntryFillRoot(
    _btree: GinBtree,
    root: Page,
    lblkno: BlockNumber,
    lpage: Page,
    rblkno: BlockNumber,
    rpage: Page,
) {
    let mut itup: IndexTuple;

    itup = GinFormInteriorTuple(getRightMostTuple(lpage), lpage, lblkno);
    if PageAddItem(
        root,
        itup as Item,
        IndexTupleSize(itup),
        InvalidOffsetNumber,
        false,
        false,
    ) == InvalidOffsetNumber
    {
        elog!(ERROR, "failed to add item to index root page");
    }
    pfree(itup as *mut c_void);

    itup = GinFormInteriorTuple(getRightMostTuple(rpage), rpage, rblkno);
    if PageAddItem(
        root,
        itup as Item,
        IndexTupleSize(itup),
        InvalidOffsetNumber,
        false,
        false,
    ) == InvalidOffsetNumber
    {
        elog!(ERROR, "failed to add item to index root page");
    }
    pfree(itup as *mut c_void);
}

/*
 * Set up GinBtree for entry page access
 *
 * Note: during WAL recovery, there may be no valid data in ginstate
 * other than a faked-up Relation pointer; the key datum is bogus too.
 */
pub unsafe fn ginPrepareEntryScan(
    btree: GinBtree,
    attnum: OffsetNumber,
    key: Datum,
    category: GinNullCategory,
    ginstate: *mut GinState,
) {
    std::ptr::write_bytes(btree as *mut u8, 0, size_of::<GinBtreeData>());

    (*btree).index = (*ginstate).index;
    (*btree).rootBlkno = GIN_ROOT_BLKNO;
    (*btree).ginstate = ginstate;

    (*btree).findChildPage = Some(entryLocateEntry);
    (*btree).getLeftMostChild = Some(entryGetLeftMostPage);
    (*btree).isMoveRight = Some(entryIsMoveRight);
    (*btree).findItem = Some(entryLocateLeafEntry);
    (*btree).findChildPtr = Some(entryFindChildPtr);
    (*btree).beginPlaceToPage = Some(entryBeginPlaceToPage);
    (*btree).execPlaceToPage = Some(entryExecPlaceToPage);
    (*btree).fillRoot = Some(ginEntryFillRoot);
    (*btree).prepareDownlink = Some(entryPrepareDownlink);

    (*btree).isData = false;
    (*btree).fullScan = false;
    (*btree).isBuild = false;

    (*btree).entryAttnum = attnum;
    (*btree).entryKey = key;
    (*btree).entryCategory = category;
}

/* ---- xlog record layout (access/ginxlog.h) ---- */

#[repr(C)]
struct ginxlogInsertEntry {
    offset: OffsetNumber,
    isDelete: bool,
    tuple: IndexTupleData, /* variable length */
}

/* PGAlignedBlock from c.h: a block-sized, MAXALIGN'd buffer. */
#[repr(C, align(8))]
#[derive(Clone, Copy)]
struct PGAlignedBlock {
    data: [c_char; BLCKSZ as usize],
}

/* ---- local stubs for unported helpers ---- */

unsafe fn GinSetNullCategory(
    _itup: IndexTuple,
    _ginstate: *mut GinState,
    _category: GinNullCategory,
) {
    unimplemented!() // TODO: src/include/access/gin_private.h
}
unsafe fn BufferGetPage(_buffer: Buffer) -> Page {
    unimplemented!() // TODO: src/include/storage/bufmgr.h
}
unsafe fn BufferGetBlockNumber(_buffer: Buffer) -> BlockNumber {
    unimplemented!() // TODO: src/backend/storage/buffer/bufmgr.c
}
unsafe fn MarkBufferDirty(_buffer: Buffer) {
    unimplemented!() // TODO: src/backend/storage/buffer/bufmgr.c
}
unsafe fn RelationNeedsWAL(_index: Relation) -> bool {
    unimplemented!() // TODO: src/include/utils/rel.h
}
unsafe fn RelationGetRelationName(_relation: Relation) -> *const c_char {
    unimplemented!() // TODO: src/include/utils/rel.h
}
unsafe fn XLogRegisterBuffer(_block_id: uint8, _buffer: Buffer, _flags: uint8) {
    unimplemented!() // TODO: src/backend/access/transam/xloginsert.c
}
unsafe fn XLogRegisterBufData(_block_id: uint8, _data: *mut c_char, _len: c_int) {
    unimplemented!() // TODO: src/backend/access/transam/xloginsert.c
}

const REGBUF_STANDARD: uint8 = 0x10;
