//! gindatapage.c
//!   routines for handling GIN posting tree pages.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!         src/backend/access/gin/gindatapage.c
//!
//! 1:1 translation. The public function signatures match the `unimplemented!()`
//! forward declarations in access/gin/gin_private.rs; this file carries the real
//! bodies, so those names are NOT re-stubbed here.

use crate::prelude::*;

use crate::access::gin::gin::GinStatsData;
use crate::access::gin::gin_private::{
    ginCompareItemPointers, ginCompressPostingList, ginFindLeafPage, ginInsertItemPointers,
    ginInsertValue, ginMergeItemPointers, ginPostingListDecode, ginPostingListDecodeAllSegments,
    ginPostingListDecodeAllSegmentsToTbm, ginVacuumItemPointers, GinBtree, GinBtreeData,
    GinBtreeDataLeafInsertData, GinBtreeStack, GinInitPage, GinNewBuffer, GinPlaceToPageRC,
    GinVacuumState, GPTP_INSERT, GPTP_NO_WORK, GPTP_SPLIT,
};
use crate::access::gin::ginblock::{
    GinDataLeafPageGetFreeSpace, GinDataLeafPageGetPostingList, GinDataLeafPageGetPostingListSize,
    GinDataPageGetData, GinDataPageGetPostingItem, GinDataPageGetRightBound, GinDataPageMaxDataSize,
    GinDataPageSetDataSize, GinNextPostingListSegment, GinNonLeafDataPageGetFreeSpace,
    GinPageGetOpaque, GinPageIsCompressed, GinPageIsData, GinPageIsDeleted, GinPageIsLeaf,
    GinPageRightMost, GinPageSetCompressed, GinPostingList, ItemPointerSetMin, PostingItem,
    PostingItemGetBlockNumber, PostingItemSetBlockNumber, SizeOfGinPostingList, GIN_COMPRESSED,
    GIN_DATA, GIN_LEAF,
};
use crate::access::gin::ginxlog::{
    ginxlogCreatePostingTree, ginxlogInsertDataInternal, ginxlogRecompressDataLeaf,
    GIN_SEGMENT_ADDITEMS, GIN_SEGMENT_DELETE, GIN_SEGMENT_INSERT, GIN_SEGMENT_REPLACE,
    GIN_SEGMENT_UNMODIFIED, XLOG_GIN_CREATE_PTREE, XLOG_GIN_VACUUM_DATA_LEAF_PAGE,
};
use crate::access::rmgrlist::RM_GIN_ID;
use crate::access::transam::xlogdefs::XLogRecPtr;
use crate::access::transam::xloginsert::{
    XLogBeginInsert, XLogInsert, XLogRegisterBufData, XLogRegisterBuffer, XLogRegisterData,
    REGBUF_STANDARD, REGBUF_WILL_INIT,
};
use crate::c::SHORTALIGN;
use crate::lib::ilist::{
    dlist_delete, dlist_has_next, dlist_has_prev, dlist_head, dlist_head_node, dlist_init,
    dlist_insert_after, dlist_is_empty, dlist_iter, dlist_next_node, dlist_node, dlist_prev_node,
    dlist_push_tail, dlist_tail_node,
};
use crate::miscadmin::{END_CRIT_SECTION, START_CRIT_SECTION};
use crate::nodes::tidbitmap::{tbm_add_tuples, TIDBitmap};
use crate::pg_config::BLCKSZ;
use crate::storage::block::{BlockNumber, InvalidBlockNumber};
use crate::storage::buffer::bufmgr::{
    BufferGetBlockNumber, BufferGetPage, MarkBufferDirty, UnlockReleaseBuffer,
};
use crate::storage::buf::Buffer;
use crate::storage::bufpage::{
    Page, PageGetPageSize, PageGetTempPage, PageRestoreTempPage, PageSetLSN,
};
use crate::storage::itemptr::{
    ItemPointer, ItemPointerCompare, ItemPointerData, ItemPointerIsValid, ItemPointerSetInvalid,
};
use crate::storage::off::{FirstOffsetNumber, InvalidOffsetNumber, OffsetNumber};
use crate::utils::rel::Relation;

use crate::{dlist_container, dlist_foreach};

// memmove/memcpy/memset are libc primitives; PepperDB forbids the `libc` crate,
// so we declare the ones we need locally (matching sibling gin files).
extern "C" {
    fn memcpy(dest: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
    fn memmove(dest: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
}

// ---------------------------------------------------------------------------
// storage/predicate.h - PredicateLockPageSplit (not yet ported).
// ---------------------------------------------------------------------------

// TODO(pg-port): storage/predicate.h - PredicateLockPageSplit.
unsafe fn PredicateLockPageSplit(rel: Relation, oldblkno: BlockNumber, newblkno: BlockNumber) {
    let _ = (rel, oldblkno, newblkno);
}

// TODO(pg-port): utils/rel.h - RelationNeedsWAL.
unsafe fn RelationNeedsWAL(relation: Relation) -> bool {
    let _ = relation;
    true
}

/*
 * Min, Max and Target size of posting lists stored on leaf pages, in bytes.
 *
 * The code can deal with any size, but random access is more efficient when
 * a number of smaller lists are stored, rather than one big list. If a
 * posting list would become larger than Max size as a result of insertions,
 * it is split into two. If a posting list would be smaller than minimum
 * size, it is merged with the next posting list.
 */
const GinPostingListSegmentMaxSize: usize = 384;
const GinPostingListSegmentTargetSize: usize = 256;
const GinPostingListSegmentMinSize: usize = 128;

/*
 * At least this many items fit in a GinPostingListSegmentMaxSize-bytes
 * long segment. This is used when estimating how much space is required
 * for N items, at minimum.
 */
const MinTuplesPerSegment: usize = (GinPostingListSegmentMaxSize - 2) / 6;

/*
 * A working struct for manipulating a posting tree leaf page.
 */
#[repr(C)]
struct disassembledLeaf {
    segments: dlist_head, /* a list of leafSegmentInfos */

    /*
     * The following fields represent how the segments are split across pages,
     * if a page split is required. Filled in by leafRepackItems.
     */
    lastleft: *mut dlist_node, /* last segment on left page */
    lsize: c_int,              /* total size on left page */
    rsize: c_int,              /* total size on right page */

    oldformat: bool, /* page is in pre-9.4 format on disk */

    /*
     * If we need WAL data representing the reconstructed leaf page, it's
     * stored here by computeLeafRecompressWALData.
     */
    walinfo: *mut c_void, /* buffer start */
    walinfolen: c_int,    /* and length */
}

#[repr(C)]
struct leafSegmentInfo {
    node: dlist_node, /* linked list pointers */

    /*-------------
     * 'action' indicates the status of this in-memory segment, compared to
     * what's on disk. It is one of the GIN_SEGMENT_* action codes:
     *
     * UNMODIFIED	no changes
     * DELETE		the segment is to be removed. 'seg' and 'items' are
     *				ignored
     * INSERT		this is a completely new segment
     * REPLACE		this replaces an existing segment with new content
     * ADDITEMS		like REPLACE, but no items have been removed, and we track
     *				in detail what items have been added to this segment, in
     *				'modifieditems'
     *-------------
     */
    action: c_char,

    modifieditems: *mut ItemPointerData,
    nmodifieditems: uint16,

    /*
     * The following fields represent the items in this segment. If 'items' is
     * not NULL, it contains a palloc'd array of the items in this segment. If
     * 'seg' is not NULL, it contains the items in an already-compressed
     * format. It can point to an on-disk page (!modified), or a palloc'd
     * segment in memory. If both are set, they must represent the same items.
     */
    seg: *mut GinPostingList,
    items: ItemPointer,
    nitems: c_int, /* # of items in 'items', if items != NULL */
}

/*
 * Read TIDs from leaf data page to single uncompressed array. The TIDs are
 * returned in ascending order.
 *
 * advancePast is a hint, indicating that the caller is only interested in
 * TIDs > advancePast. To return all items, use ItemPointerSetMin.
 *
 * Note: This function can still return items smaller than advancePast that
 * are in the same posting list as the items of interest, so the caller must
 * still check all the returned items. But passing it allows this function to
 * skip whole posting lists.
 */
pub unsafe fn GinDataLeafPageGetItems(
    page: Page,
    nitems: *mut c_int,
    mut advancePast: ItemPointerData,
) -> ItemPointer {
    let result: ItemPointer;

    if GinPageIsCompressed(page) {
        let mut seg: *mut GinPostingList = GinDataLeafPageGetPostingList(page);
        let mut len: Size = GinDataLeafPageGetPostingListSize(page);
        let endptr: Pointer = (seg as Pointer).add(len);
        let mut next: *mut GinPostingList;

        /* Skip to the segment containing advancePast+1 */
        if ItemPointerIsValid(&advancePast) {
            next = GinNextPostingListSegment(seg);
            while (next as Pointer) < endptr
                && ginCompareItemPointers(&mut (*next).first, &mut advancePast) <= 0
            {
                seg = next;
                next = GinNextPostingListSegment(seg);
            }
            len = endptr.offset_from(seg as Pointer) as Size;
        }

        if len > 0 {
            result = ginPostingListDecodeAllSegments(seg, len as c_int, nitems);
        } else {
            result = null_mut();
            *nitems = 0;
        }
    } else {
        let tmp: ItemPointer = dataLeafPageGetUncompressed(page, nitems);

        result = palloc((*nitems) as usize * size_of::<ItemPointerData>()) as ItemPointer;
        memcpy(
            result as *mut c_void,
            tmp as *const c_void,
            (*nitems) as usize * size_of::<ItemPointerData>(),
        );
    }

    result
}

/*
 * Places all TIDs from leaf data page to bitmap.
 */
pub unsafe fn GinDataLeafPageGetItemsToTbm(page: Page, tbm: *mut TIDBitmap) -> c_int {
    let uncompressed: ItemPointer;
    let mut nitems: c_int;

    if GinPageIsCompressed(page) {
        let segment: *mut GinPostingList = GinDataLeafPageGetPostingList(page);
        let len: Size = GinDataLeafPageGetPostingListSize(page);

        nitems = ginPostingListDecodeAllSegmentsToTbm(segment, len as c_int, tbm);
    } else {
        nitems = 0;
        uncompressed = dataLeafPageGetUncompressed(page, &mut nitems);

        if nitems > 0 {
            tbm_add_tuples(tbm, uncompressed, nitems, false);
        }
    }

    nitems
}

/*
 * Get pointer to the uncompressed array of items on a pre-9.4 format
 * uncompressed leaf page. The number of items in the array is returned in
 * *nitems.
 */
unsafe fn dataLeafPageGetUncompressed(page: Page, nitems: *mut c_int) -> ItemPointer {
    let items: ItemPointer;

    Assert!(!GinPageIsCompressed(page));

    /*
     * In the old pre-9.4 page format, the whole page content is used for
     * uncompressed items, and the number of items is stored in 'maxoff'
     */
    items = GinDataPageGetData(page) as ItemPointer;
    *nitems = (*GinPageGetOpaque(page)).maxoff as c_int;

    items
}

/*
 * Check if we should follow the right link to find the item we're searching
 * for.
 *
 * Compares inserting item pointer with the right bound of the current page.
 */
unsafe extern "C" fn dataIsMoveRight(btree: GinBtree, page: Page) -> bool {
    let iptr: ItemPointer = GinDataPageGetRightBound(page);

    if GinPageRightMost(page) {
        return false;
    }

    if GinPageIsDeleted(page) {
        return true;
    }

    ginCompareItemPointers(&mut (*btree).itemptr, iptr) > 0
}

/*
 * Find correct PostingItem in non-leaf page. It is assumed that this is
 * the correct page, and the searched value SHOULD be on the page.
 */
unsafe extern "C" fn dataLocateItem(btree: GinBtree, stack: *mut GinBtreeStack) -> BlockNumber {
    let mut low: OffsetNumber;
    let mut high: OffsetNumber;
    let maxoff: OffsetNumber;
    let mut pitem: *mut PostingItem = null_mut();
    let mut result: c_int;
    let page: Page = BufferGetPage((*stack).buffer);

    Assert!(!GinPageIsLeaf(page));
    Assert!(GinPageIsData(page));

    if (*btree).fullScan {
        (*stack).off = FirstOffsetNumber;
        (*stack).predictNumber *= (*GinPageGetOpaque(page)).maxoff as uint32;
        return ((*btree).getLeftMostChild.unwrap())(btree, page);
    }

    low = FirstOffsetNumber;
    high = (*GinPageGetOpaque(page)).maxoff;
    maxoff = high;
    Assert!(high >= low);

    high += 1;

    while high > low {
        let mid: OffsetNumber = low + ((high - low) / 2);

        pitem = GinDataPageGetPostingItem(page, mid as usize);

        if mid == maxoff {
            /*
             * Right infinity, page already correctly chosen with a help of
             * dataIsMoveRight
             */
            result = -1;
        } else {
            pitem = GinDataPageGetPostingItem(page, mid as usize);
            result = ginCompareItemPointers(&mut (*btree).itemptr, &mut (*pitem).key);
        }

        if result == 0 {
            (*stack).off = mid;
            return PostingItemGetBlockNumber(pitem);
        } else if result > 0 {
            low = mid + 1;
        } else {
            high = mid;
        }
    }

    Assert!(high >= FirstOffsetNumber && high <= maxoff);

    (*stack).off = high;
    pitem = GinDataPageGetPostingItem(page, high as usize);
    PostingItemGetBlockNumber(pitem)
}

/*
 * Find link to blkno on non-leaf page, returns offset of PostingItem
 */
unsafe extern "C" fn dataFindChildPtr(
    btree: GinBtree,
    page: Page,
    blkno: BlockNumber,
    storedOff: OffsetNumber,
) -> OffsetNumber {
    let mut i: OffsetNumber;
    let mut maxoff: OffsetNumber = (*GinPageGetOpaque(page)).maxoff;
    let mut pitem: *mut PostingItem;

    Assert!(!GinPageIsLeaf(page));
    Assert!(GinPageIsData(page));

    /* if page isn't changed, we return storedOff */
    if storedOff >= FirstOffsetNumber && storedOff <= maxoff {
        pitem = GinDataPageGetPostingItem(page, storedOff as usize);
        if PostingItemGetBlockNumber(pitem) == blkno {
            return storedOff;
        }

        /*
         * we hope, that needed pointer goes to right. It's true if there
         * wasn't a deletion
         */
        i = storedOff + 1;
        while i <= maxoff {
            pitem = GinDataPageGetPostingItem(page, i as usize);
            if PostingItemGetBlockNumber(pitem) == blkno {
                return i;
            }
            i += 1;
        }

        maxoff = storedOff - 1;
    }

    /* last chance */
    i = FirstOffsetNumber;
    while i <= maxoff {
        pitem = GinDataPageGetPostingItem(page, i as usize);
        if PostingItemGetBlockNumber(pitem) == blkno {
            return i;
        }
        i += 1;
    }

    InvalidOffsetNumber
}

/*
 * Return blkno of leftmost child
 */
unsafe extern "C" fn dataGetLeftMostPage(btree: GinBtree, page: Page) -> BlockNumber {
    let pitem: *mut PostingItem;

    Assert!(!GinPageIsLeaf(page));
    Assert!(GinPageIsData(page));
    Assert!((*GinPageGetOpaque(page)).maxoff >= FirstOffsetNumber);

    pitem = GinDataPageGetPostingItem(page, FirstOffsetNumber as usize);
    PostingItemGetBlockNumber(pitem)
}

/*
 * Add PostingItem to a non-leaf page.
 */
pub unsafe fn GinDataPageAddPostingItem(page: Page, data: *mut PostingItem, offset: OffsetNumber) {
    let mut maxoff: OffsetNumber = (*GinPageGetOpaque(page)).maxoff;
    let ptr: *mut c_char;

    Assert!(PostingItemGetBlockNumber(data) != InvalidBlockNumber);
    Assert!(!GinPageIsLeaf(page));

    if offset == InvalidOffsetNumber {
        ptr = GinDataPageGetPostingItem(page, (maxoff + 1) as usize) as *mut c_char;
    } else {
        ptr = GinDataPageGetPostingItem(page, offset as usize) as *mut c_char;
        if offset != maxoff + 1 {
            memmove(
                ptr.add(size_of::<PostingItem>()) as *mut c_void,
                ptr as *const c_void,
                (maxoff - offset + 1) as usize * size_of::<PostingItem>(),
            );
        }
    }
    memcpy(
        ptr as *mut c_void,
        data as *const c_void,
        size_of::<PostingItem>(),
    );

    maxoff += 1;
    (*GinPageGetOpaque(page)).maxoff = maxoff;

    /*
     * Also set pd_lower to the end of the posting items, to follow the
     * "standard" page layout, so that we can squeeze out the unused space
     * from full-page images.
     */
    GinDataPageSetDataSize(page, maxoff as usize * size_of::<PostingItem>());
}

/*
 * Delete posting item from non-leaf page
 */
pub unsafe fn GinPageDeletePostingItem(page: Page, offset: OffsetNumber) {
    let mut maxoff: OffsetNumber = (*GinPageGetOpaque(page)).maxoff;

    Assert!(!GinPageIsLeaf(page));
    Assert!(offset >= FirstOffsetNumber && offset <= maxoff);

    if offset != maxoff {
        memmove(
            GinDataPageGetPostingItem(page, offset as usize) as *mut c_void,
            GinDataPageGetPostingItem(page, (offset + 1) as usize) as *const c_void,
            size_of::<PostingItem>() * (maxoff - offset) as usize,
        );
    }

    maxoff -= 1;
    (*GinPageGetOpaque(page)).maxoff = maxoff;

    GinDataPageSetDataSize(page, maxoff as usize * size_of::<PostingItem>());
}

/*
 * Prepare to insert data on a leaf data page.
 *
 * If it will fit, return GPTP_INSERT after doing whatever setup is needed
 * before we enter the insertion critical section.  *ptp_workspace can be
 * set to pass information along to the execPlaceToPage function.
 *
 * If it won't fit, perform a page split and return two temporary page
 * images into *newlpage and *newrpage, with result GPTP_SPLIT.
 *
 * In neither case should the given page buffer be modified here.
 */
unsafe fn dataBeginPlaceToPageLeaf(
    btree: GinBtree,
    buf: Buffer,
    stack: *mut GinBtreeStack,
    insertdata: *mut c_void,
    ptp_workspace: *mut *mut c_void,
    newlpage: *mut Page,
    newrpage: *mut Page,
) -> GinPlaceToPageRC {
    let items: *mut GinBtreeDataLeafInsertData = insertdata as *mut GinBtreeDataLeafInsertData;
    let newItems: ItemPointer = (*items).items.add((*items).curitem as usize);
    let mut maxitems: c_int = ((*items).nitem - (*items).curitem) as c_int;
    let page: Page = BufferGetPage(buf);
    let mut i: c_int;
    let mut rbound: ItemPointerData;
    let lbound: ItemPointerData;
    let needsplit: bool;
    let append: bool;
    let mut segsize: c_int;
    let freespace: Size;
    let leaf: *mut disassembledLeaf;
    let mut lastleftinfo: *mut leafSegmentInfo;
    let mut maxOldItem: ItemPointerData = core::mem::zeroed();
    let mut remaining: ItemPointerData = core::mem::zeroed();

    rbound = *GinDataPageGetRightBound(page);

    /*
     * Count how many of the new items belong to this page.
     */
    if !GinPageRightMost(page) {
        i = 0;
        while i < maxitems {
            if ginCompareItemPointers(newItems.add(i as usize), &mut rbound) > 0 {
                /*
                 * This needs to go to some other location in the tree. (The
                 * caller should've chosen the insert location so that at
                 * least the first item goes here.)
                 */
                Assert!(i > 0);
                break;
            }
            i += 1;
        }
        maxitems = i;
    }

    /* Disassemble the data on the page */
    leaf = disassembleLeaf(page);

    /*
     * Are we appending to the end of the page? IOW, are all the new items
     * larger than any of the existing items.
     */
    if !dlist_is_empty(&(*leaf).segments) {
        lastleftinfo = dlist_container!(
            leafSegmentInfo,
            node,
            dlist_tail_node(&mut (*leaf).segments)
        );
        if (*lastleftinfo).items.is_null() {
            (*lastleftinfo).items =
                ginPostingListDecode((*lastleftinfo).seg, &mut (*lastleftinfo).nitems);
        }
        maxOldItem = *(*lastleftinfo).items.add(((*lastleftinfo).nitems - 1) as usize);
        if ginCompareItemPointers(newItems.add(0), &mut maxOldItem) >= 0 {
            append = true;
        } else {
            append = false;
        }
    } else {
        ItemPointerSetMin(&mut maxOldItem);
        append = true;
    }

    /*
     * If we're appending to the end of the page, we will append as many items
     * as we can fit (after splitting), and stop when the pages becomes full.
     * Otherwise we have to limit the number of new items to insert, because
     * once we start packing we can't just stop when we run out of space,
     * because we must make sure that all the old items still fit.
     */
    if GinPageIsCompressed(page) {
        freespace = GinDataLeafPageGetFreeSpace(page);
    } else {
        freespace = 0;
    }
    if append {
        /*
         * Even when appending, trying to append more items than will fit is
         * not completely free, because we will merge the new items and old
         * items into an array below. In the best case, every new item fits in
         * a single byte, and we can use all the free space on the old page as
         * well as the new page. For simplicity, ignore segment overhead etc.
         */
        maxitems = crate::c::Min(maxitems, (freespace + GinDataPageMaxDataSize()) as c_int);
    } else {
        /*
         * Calculate a conservative estimate of how many new items we can fit
         * on the two pages after splitting.
         *
         * We can use any remaining free space on the old page to store full
         * segments, as well as the new page. Each full-sized segment can hold
         * at least MinTuplesPerSegment items
         */
        let mut nnewsegments: c_int;

        nnewsegments = (freespace / GinPostingListSegmentMaxSize) as c_int;
        nnewsegments += (GinDataPageMaxDataSize() / GinPostingListSegmentMaxSize) as c_int;
        maxitems = crate::c::Min(maxitems, nnewsegments * MinTuplesPerSegment as c_int);
    }

    /* Add the new items to the segment list */
    if !addItemsToLeaf(leaf, newItems, maxitems) {
        /* all items were duplicates, we have nothing to do */
        (*items).curitem += maxitems as uint32;

        return GPTP_NO_WORK;
    }

    /*
     * Pack the items back to compressed segments, ready for writing to disk.
     */
    needsplit = leafRepackItems(leaf, &mut remaining);

    /*
     * Did all the new items fit?
     *
     * If we're appending, it's OK if they didn't. But as a sanity check,
     * verify that all the old items fit.
     */
    if ItemPointerIsValid(&remaining) {
        if !append || ItemPointerCompare(&mut maxOldItem, &mut remaining) >= 0 {
            elog!(ERROR, "could not split GIN page; all old items didn't fit");
        }

        /* Count how many of the new items did fit. */
        i = 0;
        while i < maxitems {
            if ginCompareItemPointers(newItems.add(i as usize), &mut remaining) >= 0 {
                break;
            }
            i += 1;
        }
        if i == 0 {
            elog!(ERROR, "could not split GIN page; no new items fit");
        }
        maxitems = i;
    }

    if !needsplit {
        /*
         * Great, all the items fit on a single page.  If needed, prepare data
         * for a WAL record describing the changes we'll make.
         */
        if RelationNeedsWAL((*btree).index) && !(*btree).isBuild {
            computeLeafRecompressWALData(leaf);
        }

        /*
         * We're ready to enter the critical section, but
         * dataExecPlaceToPageLeaf will need access to the "leaf" data.
         */
        *ptp_workspace = leaf as *mut c_void;

        if append {
            elog!(
                DEBUG2,
                "appended {} new items to block {}; {} bytes ({} to go)",
                maxitems,
                BufferGetBlockNumber(buf),
                (*leaf).lsize,
                (*items).nitem as c_int - (*items).curitem as c_int - maxitems
            );
        } else {
            elog!(
                DEBUG2,
                "inserted {} new items to block {}; {} bytes ({} to go)",
                maxitems,
                BufferGetBlockNumber(buf),
                (*leaf).lsize,
                (*items).nitem as c_int - (*items).curitem as c_int - maxitems
            );
        }
    } else {
        /*
         * Have to split.
         *
         * leafRepackItems already divided the segments between the left and
         * the right page. It filled the left page as full as possible, and
         * put the rest to the right page. When building a new index, that's
         * good, because the table is scanned from beginning to end and there
         * won't be any more insertions to the left page during the build.
         * This packs the index as tight as possible. But otherwise, split
         * 50/50, by moving segments from the left page to the right page
         * until they're balanced.
         *
         * As a further heuristic, when appending items to the end of the
         * page, try to make the left page 75% full, on the assumption that
         * subsequent insertions will probably also go to the end. This packs
         * the index somewhat tighter when appending to a table, which is very
         * common.
         */
        if !(*btree).isBuild {
            while dlist_has_prev(&(*leaf).segments, (*leaf).lastleft) {
                lastleftinfo = dlist_container!(leafSegmentInfo, node, (*leaf).lastleft);

                /* ignore deleted segments */
                if (*lastleftinfo).action != GIN_SEGMENT_DELETE as c_char {
                    segsize = SizeOfGinPostingList((*lastleftinfo).seg) as c_int;

                    /*
                     * Note that we check that the right page doesn't become
                     * more full than the left page even when appending. It's
                     * possible that we added enough items to make both pages
                     * more than 75% full.
                     */
                    if ((*leaf).lsize - segsize) - ((*leaf).rsize + segsize) < 0 {
                        break;
                    }
                    if append {
                        if ((*leaf).lsize - segsize) < (BLCKSZ as c_int * 3) / 4 {
                            break;
                        }
                    }

                    (*leaf).lsize -= segsize;
                    (*leaf).rsize += segsize;
                }
                (*leaf).lastleft = dlist_prev_node(&mut (*leaf).segments, (*leaf).lastleft);
            }
        }
        Assert!((*leaf).lsize <= GinDataPageMaxDataSize() as c_int);
        Assert!((*leaf).rsize <= GinDataPageMaxDataSize() as c_int);

        /*
         * Fetch the max item in the left page's last segment; it becomes the
         * right bound of the page.
         */
        lastleftinfo = dlist_container!(leafSegmentInfo, node, (*leaf).lastleft);
        if (*lastleftinfo).items.is_null() {
            (*lastleftinfo).items =
                ginPostingListDecode((*lastleftinfo).seg, &mut (*lastleftinfo).nitems);
        }
        lbound = *(*lastleftinfo).items.add(((*lastleftinfo).nitems - 1) as usize);

        /*
         * Now allocate a couple of temporary page images, and fill them.
         */
        *newlpage = palloc(BLCKSZ as usize) as Page;
        *newrpage = palloc(BLCKSZ as usize) as Page;

        dataPlaceToPageLeafSplit(leaf, lbound, rbound, *newlpage, *newrpage);

        Assert!(
            GinPageRightMost(page)
                || ginCompareItemPointers(
                    GinDataPageGetRightBound(*newlpage),
                    GinDataPageGetRightBound(*newrpage)
                ) < 0
        );

        if append {
            elog!(
                DEBUG2,
                "appended {} items to block {}; split {}/{} ({} to go)",
                maxitems,
                BufferGetBlockNumber(buf),
                (*leaf).lsize,
                (*leaf).rsize,
                (*items).nitem as c_int - (*items).curitem as c_int - maxitems
            );
        } else {
            elog!(
                DEBUG2,
                "inserted {} items to block {}; split {}/{} ({} to go)",
                maxitems,
                BufferGetBlockNumber(buf),
                (*leaf).lsize,
                (*leaf).rsize,
                (*items).nitem as c_int - (*items).curitem as c_int - maxitems
            );
        }
    }

    (*items).curitem += maxitems as uint32;

    if needsplit {
        GPTP_SPLIT
    } else {
        GPTP_INSERT
    }
}

/*
 * Perform data insertion after beginPlaceToPage has decided it will fit.
 *
 * This is invoked within a critical section, and XLOG record creation (if
 * needed) is already started.  The target buffer is registered in slot 0.
 */
unsafe fn dataExecPlaceToPageLeaf(
    btree: GinBtree,
    buf: Buffer,
    stack: *mut GinBtreeStack,
    insertdata: *mut c_void,
    ptp_workspace: *mut c_void,
) {
    let leaf: *mut disassembledLeaf = ptp_workspace as *mut disassembledLeaf;

    /* Apply changes to page */
    dataPlaceToPageLeafRecompress(buf, leaf);

    MarkBufferDirty(buf);

    /* If needed, register WAL data built by computeLeafRecompressWALData */
    if RelationNeedsWAL((*btree).index) && !(*btree).isBuild {
        XLogRegisterBuffer(0, buf, REGBUF_STANDARD);
        XLogRegisterBufData(0, (*leaf).walinfo, (*leaf).walinfolen as u32);
    }
}

/*
 * Vacuum a posting tree leaf page.
 */
pub unsafe fn ginVacuumPostingTreeLeaf(
    indexrel: Relation,
    buffer: Buffer,
    gvs: *mut GinVacuumState,
) {
    let page: Page = BufferGetPage(buffer);
    let leaf: *mut disassembledLeaf;
    let mut removedsomething: bool = false;
    let mut iter: dlist_iter = core::mem::zeroed();

    leaf = disassembleLeaf(page);

    /* Vacuum each segment. */
    dlist_foreach!(iter, &mut (*leaf).segments, {
        let seginfo: *mut leafSegmentInfo = dlist_container!(leafSegmentInfo, node, iter.cur);
        let oldsegsize: c_int;
        let cleaned: ItemPointer;
        let mut ncleaned: c_int = 0;

        if (*seginfo).items.is_null() {
            (*seginfo).items = ginPostingListDecode((*seginfo).seg, &mut (*seginfo).nitems);
        }
        if !(*seginfo).seg.is_null() {
            oldsegsize = SizeOfGinPostingList((*seginfo).seg) as c_int;
        } else {
            oldsegsize = GinDataPageMaxDataSize() as c_int;
        }

        cleaned = ginVacuumItemPointers(gvs, (*seginfo).items, (*seginfo).nitems, &mut ncleaned);
        pfree((*seginfo).items as *mut c_void);
        (*seginfo).items = null_mut();
        (*seginfo).nitems = 0;
        if !cleaned.is_null() {
            if ncleaned > 0 {
                let mut npacked: c_int = 0;

                (*seginfo).seg =
                    ginCompressPostingList(cleaned, ncleaned, oldsegsize, &mut npacked);
                /* Removing an item never increases the size of the segment */
                if npacked != ncleaned {
                    elog!(ERROR, "could not fit vacuumed posting list");
                }
                (*seginfo).action = GIN_SEGMENT_REPLACE as c_char;
            } else {
                (*seginfo).seg = null_mut();
                (*seginfo).items = null_mut();
                (*seginfo).action = GIN_SEGMENT_DELETE as c_char;
            }
            (*seginfo).nitems = ncleaned;

            removedsomething = true;
        }
    });

    /*
     * If we removed any items, reconstruct the page from the pieces.
     *
     * We don't try to re-encode the segments here, even though some of them
     * might be really small now that we've removed some items from them. It
     * seems like a waste of effort, as there isn't really any benefit from
     * larger segments per se; larger segments only help to pack more items in
     * the same space. We might as well delay doing that until the next
     * insertion, which will need to re-encode at least part of the page
     * anyway.
     *
     * Also note if the page was in uncompressed, pre-9.4 format before, it is
     * now represented as one huge segment that contains all the items. It
     * might make sense to split that, to speed up random access, but we don't
     * bother. You'll have to REINDEX anyway if you want the full gain of the
     * new tighter index format.
     */
    if removedsomething {
        let mut modified: bool;

        /*
         * Make sure we have a palloc'd copy of all segments, after the first
         * segment that is modified. (dataPlaceToPageLeafRecompress requires
         * this).
         */
        modified = false;
        dlist_foreach!(iter, &mut (*leaf).segments, {
            let seginfo: *mut leafSegmentInfo = dlist_container!(leafSegmentInfo, node, iter.cur);

            if (*seginfo).action != GIN_SEGMENT_UNMODIFIED as c_char {
                modified = true;
            }
            if modified && (*seginfo).action != GIN_SEGMENT_DELETE as c_char {
                let segsize: c_int = SizeOfGinPostingList((*seginfo).seg) as c_int;
                let tmp: *mut GinPostingList = palloc(segsize as usize) as *mut GinPostingList;

                memcpy(
                    tmp as *mut c_void,
                    (*seginfo).seg as *const c_void,
                    segsize as usize,
                );
                (*seginfo).seg = tmp;
            }
        });

        if RelationNeedsWAL(indexrel) {
            computeLeafRecompressWALData(leaf);
        }

        /* Apply changes to page */
        START_CRIT_SECTION();

        dataPlaceToPageLeafRecompress(buffer, leaf);

        MarkBufferDirty(buffer);

        if RelationNeedsWAL(indexrel) {
            let recptr: XLogRecPtr;

            XLogBeginInsert();
            XLogRegisterBuffer(0, buffer, REGBUF_STANDARD);
            XLogRegisterBufData(0, (*leaf).walinfo, (*leaf).walinfolen as u32);
            recptr = XLogInsert(RM_GIN_ID, XLOG_GIN_VACUUM_DATA_LEAF_PAGE);
            PageSetLSN(page, recptr);
        }

        END_CRIT_SECTION();
    }
}

/*
 * Construct a ginxlogRecompressDataLeaf record representing the changes
 * in *leaf.  (Because this requires a palloc, we have to do it before
 * we enter the critical section that actually updates the page.)
 */
unsafe fn computeLeafRecompressWALData(leaf: *mut disassembledLeaf) {
    let mut nmodified: c_int = 0;
    let walbufbegin: *mut c_char;
    let mut walbufend: *mut c_char;
    let mut iter: dlist_iter = core::mem::zeroed();
    let mut segno: c_int;
    let recompress_xlog: *mut ginxlogRecompressDataLeaf;

    /* Count the modified segments */
    dlist_foreach!(iter, &mut (*leaf).segments, {
        let seginfo: *mut leafSegmentInfo = dlist_container!(leafSegmentInfo, node, iter.cur);

        if (*seginfo).action != GIN_SEGMENT_UNMODIFIED as c_char {
            nmodified += 1;
        }
    });

    walbufbegin = palloc(
        size_of::<ginxlogRecompressDataLeaf>()
            + BLCKSZ as usize /* max size needed to hold the segment data */
            + nmodified as usize * 2, /* (segno + action) per action */
    ) as *mut c_char;
    walbufend = walbufbegin;

    recompress_xlog = walbufend as *mut ginxlogRecompressDataLeaf;
    walbufend = walbufend.add(size_of::<ginxlogRecompressDataLeaf>());

    (*recompress_xlog).nactions = nmodified as uint16;

    segno = 0;
    dlist_foreach!(iter, &mut (*leaf).segments, {
        let seginfo: *mut leafSegmentInfo = dlist_container!(leafSegmentInfo, node, iter.cur);
        let mut segsize: c_int = 0;
        let mut datalen: c_int;
        let mut action: uint8 = (*seginfo).action as uint8;

        if action == GIN_SEGMENT_UNMODIFIED {
            segno += 1;
            continue;
        }

        if action != GIN_SEGMENT_DELETE {
            segsize = SizeOfGinPostingList((*seginfo).seg) as c_int;
        }

        /*
         * If storing the uncompressed list of added item pointers would take
         * more space than storing the compressed segment as is, do that
         * instead.
         */
        if action == GIN_SEGMENT_ADDITEMS
            && (*seginfo).nmodifieditems as usize * size_of::<ItemPointerData>() > segsize as usize
        {
            action = GIN_SEGMENT_REPLACE;
        }

        *walbufend = segno as c_char;
        walbufend = walbufend.add(1);
        *walbufend = action as c_char;
        walbufend = walbufend.add(1);

        match action {
            x if x == GIN_SEGMENT_DELETE => {
                datalen = 0;
            }
            x if x == GIN_SEGMENT_ADDITEMS => {
                datalen = (*seginfo).nmodifieditems as c_int * size_of::<ItemPointerData>() as c_int;
                memcpy(
                    walbufend as *mut c_void,
                    &(*seginfo).nmodifieditems as *const uint16 as *const c_void,
                    size_of::<uint16>(),
                );
                memcpy(
                    walbufend.add(size_of::<uint16>()) as *mut c_void,
                    (*seginfo).modifieditems as *const c_void,
                    datalen as usize,
                );
                datalen += size_of::<uint16>() as c_int;
            }
            x if x == GIN_SEGMENT_INSERT || x == GIN_SEGMENT_REPLACE => {
                datalen = SHORTALIGN(segsize as usize) as c_int;
                memcpy(
                    walbufend as *mut c_void,
                    (*seginfo).seg as *const c_void,
                    segsize as usize,
                );
            }
            _ => {
                elog!(ERROR, "unexpected GIN leaf action {}", action);
                datalen = 0;
            }
        }
        walbufend = walbufend.add(datalen as usize);

        if action != GIN_SEGMENT_INSERT {
            segno += 1;
        }
    });

    /* Pass back the constructed info via *leaf */
    (*leaf).walinfo = walbufbegin as *mut c_void;
    (*leaf).walinfolen = walbufend.offset_from(walbufbegin) as c_int;
}

/*
 * Assemble a disassembled posting tree leaf page back to a buffer.
 *
 * This just updates the target buffer; WAL stuff is caller's responsibility.
 *
 * NOTE: The segment pointers must not point directly to the same buffer,
 * except for segments that have not been modified and whose preceding
 * segments have not been modified either.
 */
unsafe fn dataPlaceToPageLeafRecompress(buf: Buffer, leaf: *mut disassembledLeaf) {
    let page: Page = BufferGetPage(buf);
    let mut ptr: *mut c_char;
    let mut newsize: c_int;
    let mut modified: bool = false;
    let mut iter: dlist_iter = core::mem::zeroed();
    let mut segsize: c_int;

    /*
     * If the page was in pre-9.4 format before, convert the header, and force
     * all segments to be copied to the page whether they were modified or
     * not.
     */
    if !GinPageIsCompressed(page) {
        Assert!((*leaf).oldformat);
        GinPageSetCompressed(page);
        (*GinPageGetOpaque(page)).maxoff = InvalidOffsetNumber;
        modified = true;
    }

    ptr = GinDataLeafPageGetPostingList(page) as *mut c_char;
    newsize = 0;
    dlist_foreach!(iter, &mut (*leaf).segments, {
        let seginfo: *mut leafSegmentInfo = dlist_container!(leafSegmentInfo, node, iter.cur);

        if (*seginfo).action != GIN_SEGMENT_UNMODIFIED as c_char {
            modified = true;
        }

        if (*seginfo).action != GIN_SEGMENT_DELETE as c_char {
            segsize = SizeOfGinPostingList((*seginfo).seg) as c_int;

            if modified {
                memcpy(
                    ptr as *mut c_void,
                    (*seginfo).seg as *const c_void,
                    segsize as usize,
                );
            }

            ptr = ptr.add(segsize as usize);
            newsize += segsize;
        }
    });

    Assert!(newsize <= GinDataPageMaxDataSize() as c_int);
    GinDataPageSetDataSize(page, newsize as usize);
}

/*
 * Like dataPlaceToPageLeafRecompress, but writes the disassembled leaf
 * segments to two pages instead of one.
 *
 * This is different from the non-split cases in that this does not modify
 * the original page directly, but writes to temporary in-memory copies of
 * the new left and right pages.
 */
unsafe fn dataPlaceToPageLeafSplit(
    leaf: *mut disassembledLeaf,
    lbound: ItemPointerData,
    rbound: ItemPointerData,
    lpage: Page,
    rpage: Page,
) {
    let mut ptr: *mut c_char;
    let mut segsize: c_int;
    let mut lsize: c_int;
    let mut rsize: c_int;
    let mut node: *mut dlist_node;
    let firstright: *mut dlist_node;
    let mut seginfo: *mut leafSegmentInfo;

    /* Initialize temporary pages to hold the new left and right pages */
    GinInitPage(lpage, (GIN_DATA | GIN_LEAF | GIN_COMPRESSED) as uint32, BLCKSZ as Size);
    GinInitPage(rpage, (GIN_DATA | GIN_LEAF | GIN_COMPRESSED) as uint32, BLCKSZ as Size);

    /*
     * Copy the segments that go to the left page.
     *
     * XXX: We should skip copying the unmodified part of the left page, like
     * we do when recompressing.
     */
    lsize = 0;
    ptr = GinDataLeafPageGetPostingList(lpage) as *mut c_char;
    firstright = dlist_next_node(&mut (*leaf).segments, (*leaf).lastleft);
    node = dlist_head_node(&mut (*leaf).segments);
    while node != firstright {
        seginfo = dlist_container!(leafSegmentInfo, node, node);

        if (*seginfo).action != GIN_SEGMENT_DELETE as c_char {
            segsize = SizeOfGinPostingList((*seginfo).seg) as c_int;
            memcpy(
                ptr as *mut c_void,
                (*seginfo).seg as *const c_void,
                segsize as usize,
            );
            ptr = ptr.add(segsize as usize);
            lsize += segsize;
        }

        node = dlist_next_node(&mut (*leaf).segments, node);
    }
    Assert!(lsize == (*leaf).lsize);
    GinDataPageSetDataSize(lpage, lsize as usize);
    *GinDataPageGetRightBound(lpage) = lbound;

    /* Copy the segments that go to the right page */
    ptr = GinDataLeafPageGetPostingList(rpage) as *mut c_char;
    rsize = 0;
    node = firstright;
    loop {
        seginfo = dlist_container!(leafSegmentInfo, node, node);

        if (*seginfo).action != GIN_SEGMENT_DELETE as c_char {
            segsize = SizeOfGinPostingList((*seginfo).seg) as c_int;
            memcpy(
                ptr as *mut c_void,
                (*seginfo).seg as *const c_void,
                segsize as usize,
            );
            ptr = ptr.add(segsize as usize);
            rsize += segsize;
        }

        if !dlist_has_next(&(*leaf).segments, node) {
            break;
        }

        node = dlist_next_node(&mut (*leaf).segments, node);
    }
    Assert!(rsize == (*leaf).rsize);
    GinDataPageSetDataSize(rpage, rsize as usize);
    *GinDataPageGetRightBound(rpage) = rbound;
}

/*
 * Prepare to insert data on an internal data page.
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
unsafe fn dataBeginPlaceToPageInternal(
    btree: GinBtree,
    buf: Buffer,
    stack: *mut GinBtreeStack,
    insertdata: *mut c_void,
    updateblkno: BlockNumber,
    ptp_workspace: *mut *mut c_void,
    newlpage: *mut Page,
    newrpage: *mut Page,
) -> GinPlaceToPageRC {
    let page: Page = BufferGetPage(buf);

    /* If it doesn't fit, deal with split case */
    if GinNonLeafDataPageGetFreeSpace(page) < size_of::<PostingItem>() {
        dataSplitPageInternal(btree, buf, stack, insertdata, updateblkno, newlpage, newrpage);
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
unsafe fn dataExecPlaceToPageInternal(
    btree: GinBtree,
    buf: Buffer,
    stack: *mut GinBtreeStack,
    insertdata: *mut c_void,
    updateblkno: BlockNumber,
    ptp_workspace: *mut c_void,
) {
    let page: Page = BufferGetPage(buf);
    let off: OffsetNumber = (*stack).off;
    let mut pitem: *mut PostingItem;

    /* Update existing downlink to point to next page (on internal page) */
    pitem = GinDataPageGetPostingItem(page, off as usize);
    PostingItemSetBlockNumber(pitem, updateblkno);

    /* Add new item */
    pitem = insertdata as *mut PostingItem;
    GinDataPageAddPostingItem(page, pitem, off);

    MarkBufferDirty(buf);

    if RelationNeedsWAL((*btree).index) && !(*btree).isBuild {
        /*
         * This must be static, because it has to survive until XLogInsert,
         * and we can't palloc here.  Ugly, but the XLogInsert infrastructure
         * isn't reentrant anyway.
         */
        static mut DATA: ginxlogInsertDataInternal = ginxlogInsertDataInternal {
            offset: 0,
            newitem: PostingItem {
                child_blkno: crate::storage::block::BlockIdData { bi_hi: 0, bi_lo: 0 },
                key: ItemPointerData {
                    ip_blkid: crate::storage::block::BlockIdData { bi_hi: 0, bi_lo: 0 },
                    ip_posid: 0,
                },
            },
        };

        DATA.offset = off;
        DATA.newitem = *pitem;

        XLogRegisterBuffer(0, buf, REGBUF_STANDARD);
        XLogRegisterBufData(
            0,
            &raw const DATA as *const c_void,
            size_of::<ginxlogInsertDataInternal>() as u32,
        );
    }
}

/*
 * Prepare to insert data on a posting-tree data page.
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
 *
 * Calls relevant function for internal or leaf page because they are handled
 * very differently.
 */
unsafe extern "C" fn dataBeginPlaceToPage(
    btree: GinBtree,
    buf: Buffer,
    stack: *mut GinBtreeStack,
    insertdata: *mut c_void,
    updateblkno: BlockNumber,
    ptp_workspace: *mut *mut c_void,
    newlpage: *mut Page,
    newrpage: *mut Page,
) -> GinPlaceToPageRC {
    let page: Page = BufferGetPage(buf);

    Assert!(GinPageIsData(page));

    if GinPageIsLeaf(page) {
        dataBeginPlaceToPageLeaf(
            btree,
            buf,
            stack,
            insertdata,
            ptp_workspace,
            newlpage,
            newrpage,
        )
    } else {
        dataBeginPlaceToPageInternal(
            btree,
            buf,
            stack,
            insertdata,
            updateblkno,
            ptp_workspace,
            newlpage,
            newrpage,
        )
    }
}

/*
 * Perform data insertion after beginPlaceToPage has decided it will fit.
 *
 * This is invoked within a critical section, and XLOG record creation (if
 * needed) is already started.  The target buffer is registered in slot 0.
 *
 * Calls relevant function for internal or leaf page because they are handled
 * very differently.
 */
unsafe extern "C" fn dataExecPlaceToPage(
    btree: GinBtree,
    buf: Buffer,
    stack: *mut GinBtreeStack,
    insertdata: *mut c_void,
    updateblkno: BlockNumber,
    ptp_workspace: *mut c_void,
) {
    let page: Page = BufferGetPage(buf);

    if GinPageIsLeaf(page) {
        dataExecPlaceToPageLeaf(btree, buf, stack, insertdata, ptp_workspace);
    } else {
        dataExecPlaceToPageInternal(btree, buf, stack, insertdata, updateblkno, ptp_workspace);
    }
}

/*
 * Split internal page and insert new data.
 *
 * Returns new temp pages to *newlpage and *newrpage.
 * The original buffer is left untouched.
 */
unsafe fn dataSplitPageInternal(
    btree: GinBtree,
    origbuf: Buffer,
    stack: *mut GinBtreeStack,
    insertdata: *mut c_void,
    updateblkno: BlockNumber,
    newlpage: *mut Page,
    newrpage: *mut Page,
) {
    let oldpage: Page = BufferGetPage(origbuf);
    let off: OffsetNumber = (*stack).off;
    let mut nitems: c_int = (*GinPageGetOpaque(oldpage)).maxoff as c_int;
    let nleftitems: c_int;
    let nrightitems: c_int;
    let pageSize: Size = PageGetPageSize(oldpage);
    let oldbound: ItemPointerData = *GinDataPageGetRightBound(oldpage);
    let bound: ItemPointer;
    let lpage: Page;
    let rpage: Page;
    let separator: OffsetNumber;
    let mut allitems: [PostingItem; (BLCKSZ as usize / size_of::<PostingItem>()) + 1] =
        core::mem::zeroed();

    lpage = PageGetTempPage(oldpage);
    rpage = PageGetTempPage(oldpage);
    GinInitPage(lpage, (*GinPageGetOpaque(oldpage)).flags as uint32, pageSize);
    GinInitPage(rpage, (*GinPageGetOpaque(oldpage)).flags as uint32, pageSize);

    /*
     * First construct a new list of PostingItems, which includes all the old
     * items, and the new item.
     */
    memcpy(
        allitems.as_mut_ptr() as *mut c_void,
        GinDataPageGetPostingItem(oldpage, FirstOffsetNumber as usize) as *const c_void,
        (off - 1) as usize * size_of::<PostingItem>(),
    );

    allitems[(off - 1) as usize] = *(insertdata as *mut PostingItem);
    memcpy(
        &mut allitems[off as usize] as *mut PostingItem as *mut c_void,
        GinDataPageGetPostingItem(oldpage, off as usize) as *const c_void,
        (nitems - (off as c_int - 1)) as usize * size_of::<PostingItem>(),
    );
    nitems += 1;

    /* Update existing downlink to point to next page */
    PostingItemSetBlockNumber(&mut allitems[off as usize], updateblkno);

    /*
     * When creating a new index, fit as many tuples as possible on the left
     * page, on the assumption that the table is scanned from beginning to
     * end. This packs the index as tight as possible.
     */
    if (*btree).isBuild && GinPageRightMost(oldpage) {
        separator = (GinNonLeafDataPageGetFreeSpace(rpage) / size_of::<PostingItem>()) as OffsetNumber;
    } else {
        separator = (nitems / 2) as OffsetNumber;
    }
    nleftitems = separator as c_int;
    nrightitems = nitems - separator as c_int;

    memcpy(
        GinDataPageGetPostingItem(lpage, FirstOffsetNumber as usize) as *mut c_void,
        allitems.as_ptr() as *const c_void,
        nleftitems as usize * size_of::<PostingItem>(),
    );
    (*GinPageGetOpaque(lpage)).maxoff = nleftitems as OffsetNumber;
    memcpy(
        GinDataPageGetPostingItem(rpage, FirstOffsetNumber as usize) as *mut c_void,
        &allitems[separator as usize] as *const PostingItem as *const c_void,
        nrightitems as usize * size_of::<PostingItem>(),
    );
    (*GinPageGetOpaque(rpage)).maxoff = nrightitems as OffsetNumber;

    /*
     * Also set pd_lower for both pages, like GinDataPageAddPostingItem does.
     */
    GinDataPageSetDataSize(lpage, nleftitems as usize * size_of::<PostingItem>());
    GinDataPageSetDataSize(rpage, nrightitems as usize * size_of::<PostingItem>());

    /* set up right bound for left page */
    bound = GinDataPageGetRightBound(lpage);
    *bound = (*GinDataPageGetPostingItem(lpage, nleftitems as usize)).key;

    /* set up right bound for right page */
    *GinDataPageGetRightBound(rpage) = oldbound;

    /* return temp pages to caller */
    *newlpage = lpage;
    *newrpage = rpage;
}

/*
 * Construct insertion payload for inserting the downlink for given buffer.
 */
unsafe extern "C" fn dataPrepareDownlink(btree: GinBtree, lbuf: Buffer) -> *mut c_void {
    let pitem: *mut PostingItem = palloc(size_of::<PostingItem>()) as *mut PostingItem;
    let lpage: Page = BufferGetPage(lbuf);

    PostingItemSetBlockNumber(pitem, BufferGetBlockNumber(lbuf));
    (*pitem).key = *GinDataPageGetRightBound(lpage);

    pitem as *mut c_void
}

/*
 * Fills new root by right bound values from child.
 * Also called from ginxlog, should not use btree
 */
pub unsafe extern "C" fn ginDataFillRoot(
    btree: GinBtree,
    root: Page,
    lblkno: BlockNumber,
    lpage: Page,
    rblkno: BlockNumber,
    rpage: Page,
) {
    let mut li: PostingItem = core::mem::zeroed();
    let mut ri: PostingItem = core::mem::zeroed();

    li.key = *GinDataPageGetRightBound(lpage);
    PostingItemSetBlockNumber(&mut li, lblkno);
    GinDataPageAddPostingItem(root, &mut li, InvalidOffsetNumber);

    ri.key = *GinDataPageGetRightBound(rpage);
    PostingItemSetBlockNumber(&mut ri, rblkno);
    GinDataPageAddPostingItem(root, &mut ri, InvalidOffsetNumber);
}

/*** Functions to work with disassembled leaf pages ***/

/*
 * Disassemble page into a disassembledLeaf struct.
 */
unsafe fn disassembleLeaf(page: Page) -> *mut disassembledLeaf {
    let leaf: *mut disassembledLeaf;
    let mut seg: *mut GinPostingList;
    let segbegin: Pointer;
    let segend: Pointer;

    leaf = palloc0(size_of::<disassembledLeaf>()) as *mut disassembledLeaf;
    dlist_init(&mut (*leaf).segments);

    if GinPageIsCompressed(page) {
        /*
         * Create a leafSegmentInfo entry for each segment.
         */
        seg = GinDataLeafPageGetPostingList(page);
        segbegin = seg as Pointer;
        segend = segbegin.add(GinDataLeafPageGetPostingListSize(page));
        while (seg as Pointer) < segend {
            let seginfo: *mut leafSegmentInfo =
                palloc(size_of::<leafSegmentInfo>()) as *mut leafSegmentInfo;

            (*seginfo).action = GIN_SEGMENT_UNMODIFIED as c_char;
            (*seginfo).seg = seg;
            (*seginfo).items = null_mut();
            (*seginfo).nitems = 0;
            dlist_push_tail(&mut (*leaf).segments, &mut (*seginfo).node);

            seg = GinNextPostingListSegment(seg);
        }
        (*leaf).oldformat = false;
    } else {
        /*
         * A pre-9.4 format uncompressed page is represented by a single
         * segment, with an array of items.  The corner case is uncompressed
         * page containing no items, which is represented as no segments.
         */
        let uncompressed: ItemPointer;
        let mut nuncompressed: c_int = 0;
        let seginfo: *mut leafSegmentInfo;

        uncompressed = dataLeafPageGetUncompressed(page, &mut nuncompressed);

        if nuncompressed > 0 {
            seginfo = palloc(size_of::<leafSegmentInfo>()) as *mut leafSegmentInfo;

            (*seginfo).action = GIN_SEGMENT_REPLACE as c_char;
            (*seginfo).seg = null_mut();
            (*seginfo).items =
                palloc(nuncompressed as usize * size_of::<ItemPointerData>()) as ItemPointer;
            memcpy(
                (*seginfo).items as *mut c_void,
                uncompressed as *const c_void,
                nuncompressed as usize * size_of::<ItemPointerData>(),
            );
            (*seginfo).nitems = nuncompressed;

            dlist_push_tail(&mut (*leaf).segments, &mut (*seginfo).node);
        }

        (*leaf).oldformat = true;
    }

    leaf
}

/*
 * Distribute newItems to the segments.
 *
 * Any segments that acquire new items are decoded, and the new items are
 * merged with the old items.
 *
 * Returns true if any new items were added. False means they were all
 * duplicates of existing items on the page.
 */
unsafe fn addItemsToLeaf(
    leaf: *mut disassembledLeaf,
    newItems: ItemPointer,
    nNewItems: c_int,
) -> bool {
    let mut iter: dlist_iter = core::mem::zeroed();
    let mut nextnew: ItemPointer = newItems;
    let mut newleft: c_int = nNewItems;
    let mut modified: bool = false;
    let newseg: *mut leafSegmentInfo;

    /*
     * If the page is completely empty, just construct one new segment to hold
     * all the new items.
     */
    if dlist_is_empty(&(*leaf).segments) {
        newseg = palloc(size_of::<leafSegmentInfo>()) as *mut leafSegmentInfo;
        (*newseg).seg = null_mut();
        (*newseg).items = newItems;
        (*newseg).nitems = nNewItems;
        (*newseg).action = GIN_SEGMENT_INSERT as c_char;
        dlist_push_tail(&mut (*leaf).segments, &mut (*newseg).node);
        return true;
    }

    'outer: {
        dlist_foreach!(iter, &mut (*leaf).segments, {
            let cur: *mut leafSegmentInfo = dlist_container!(leafSegmentInfo, node, iter.cur);
            let nthis: c_int;
            let tmpitems: ItemPointer;
            let mut ntmpitems: c_int = 0;

            /*
             * How many of the new items fall into this segment?
             */
            if !dlist_has_next(&(*leaf).segments, iter.cur) {
                nthis = newleft;
            } else {
                let next: *mut leafSegmentInfo;
                let mut next_first: ItemPointerData;

                next = dlist_container!(
                    leafSegmentInfo,
                    node,
                    dlist_next_node(&mut (*leaf).segments, iter.cur)
                );
                if !(*next).items.is_null() {
                    next_first = *(*next).items.add(0);
                } else {
                    Assert!(!(*next).seg.is_null());
                    next_first = (*(*next).seg).first;
                }

                let mut n: c_int = 0;
                while n < newleft
                    && ginCompareItemPointers(nextnew.add(n as usize), &mut next_first) < 0
                {
                    n += 1;
                }
                nthis = n;
            }
            if nthis == 0 {
                continue;
            }

            /* Merge the new items with the existing items. */
            if (*cur).items.is_null() {
                (*cur).items = ginPostingListDecode((*cur).seg, &mut (*cur).nitems);
            }

            /*
             * Fast path for the important special case that we're appending to
             * the end of the page: don't let the last segment on the page grow
             * larger than the target, create a new segment before that happens.
             */
            if !dlist_has_next(&(*leaf).segments, iter.cur)
                && ginCompareItemPointers(
                    (*cur).items.add(((*cur).nitems - 1) as usize),
                    nextnew.add(0),
                ) < 0
                && !(*cur).seg.is_null()
                && SizeOfGinPostingList((*cur).seg) >= GinPostingListSegmentTargetSize
            {
                let newseg2: *mut leafSegmentInfo =
                    palloc(size_of::<leafSegmentInfo>()) as *mut leafSegmentInfo;
                (*newseg2).seg = null_mut();
                (*newseg2).items = nextnew;
                (*newseg2).nitems = nthis;
                (*newseg2).action = GIN_SEGMENT_INSERT as c_char;
                dlist_push_tail(&mut (*leaf).segments, &mut (*newseg2).node);
                modified = true;
                break 'outer;
            }

            tmpitems = ginMergeItemPointers(
                (*cur).items,
                (*cur).nitems as uint32,
                nextnew,
                nthis as uint32,
                &mut ntmpitems,
            );
            if ntmpitems != (*cur).nitems {
                /*
                 * If there are no duplicates, track the added items so that we
                 * can emit a compact ADDITEMS WAL record later on. (it doesn't
                 * seem worth re-checking which items were duplicates, if there
                 * were any)
                 */
                if ntmpitems == nthis + (*cur).nitems
                    && (*cur).action == GIN_SEGMENT_UNMODIFIED as c_char
                {
                    (*cur).action = GIN_SEGMENT_ADDITEMS as c_char;
                    (*cur).modifieditems = nextnew;
                    (*cur).nmodifieditems = nthis as uint16;
                } else {
                    (*cur).action = GIN_SEGMENT_REPLACE as c_char;
                }

                (*cur).items = tmpitems;
                (*cur).nitems = ntmpitems;
                (*cur).seg = null_mut();
                modified = true;
            }

            nextnew = nextnew.add(nthis as usize);
            newleft -= nthis;
            if newleft == 0 {
                break 'outer;
            }
        });
    }

    modified
}

/*
 * Recompresses all segments that have been modified.
 *
 * If not all the items fit on two pages (ie. after split), we store as
 * many items as fit, and set *remaining to the first item that didn't fit.
 * If all items fit, *remaining is set to invalid.
 *
 * Returns true if the page has to be split.
 */
unsafe fn leafRepackItems(leaf: *mut disassembledLeaf, remaining: ItemPointer) -> bool {
    let mut pgused: c_int = 0;
    let mut needsplit: bool = false;
    let mut iter: dlist_iter = core::mem::zeroed();
    let mut segsize: c_int;
    let nextseg: *mut leafSegmentInfo;
    let mut npacked: c_int;
    let mut modified: bool;
    let mut cur_node: *mut dlist_node;
    let mut next_node: *mut dlist_node;

    ItemPointerSetInvalid(remaining);

    /*
     * cannot use dlist_foreach_modify here because we insert adjacent items
     * while iterating.
     */
    cur_node = dlist_head_node(&mut (*leaf).segments);
    while !cur_node.is_null() {
        let seginfo: *mut leafSegmentInfo = dlist_container!(leafSegmentInfo, node, cur_node);

        if dlist_has_next(&(*leaf).segments, cur_node) {
            next_node = dlist_next_node(&mut (*leaf).segments, cur_node);
        } else {
            next_node = null_mut();
        }

        /* Compress the posting list, if necessary */
        if (*seginfo).action != GIN_SEGMENT_DELETE as c_char {
            if (*seginfo).seg.is_null() {
                if (*seginfo).nitems as usize > GinPostingListSegmentMaxSize {
                    npacked = 0; /* no chance that it would fit. */
                } else {
                    npacked = 0;
                    (*seginfo).seg = ginCompressPostingList(
                        (*seginfo).items,
                        (*seginfo).nitems,
                        GinPostingListSegmentMaxSize as c_int,
                        &mut npacked,
                    );
                }
                if npacked != (*seginfo).nitems {
                    /*
                     * Too large. Compress again to the target size, and
                     * create a new segment to represent the remaining items.
                     * The new segment is inserted after this one, so it will
                     * be processed in the next iteration of this loop.
                     */
                    if !(*seginfo).seg.is_null() {
                        pfree((*seginfo).seg as *mut c_void);
                    }
                    (*seginfo).seg = ginCompressPostingList(
                        (*seginfo).items,
                        (*seginfo).nitems,
                        GinPostingListSegmentTargetSize as c_int,
                        &mut npacked,
                    );
                    if (*seginfo).action != GIN_SEGMENT_INSERT as c_char {
                        (*seginfo).action = GIN_SEGMENT_REPLACE as c_char;
                    }

                    let nextseg2: *mut leafSegmentInfo =
                        palloc(size_of::<leafSegmentInfo>()) as *mut leafSegmentInfo;
                    (*nextseg2).action = GIN_SEGMENT_INSERT as c_char;
                    (*nextseg2).seg = null_mut();
                    (*nextseg2).items = (*seginfo).items.add(npacked as usize);
                    (*nextseg2).nitems = (*seginfo).nitems - npacked;
                    next_node = &mut (*nextseg2).node;
                    dlist_insert_after(cur_node, next_node);
                }
            }

            /*
             * If the segment is very small, merge it with the next segment.
             */
            if SizeOfGinPostingList((*seginfo).seg) < GinPostingListSegmentMinSize
                && !next_node.is_null()
            {
                let nmerged: c_int;

                nextseg = dlist_container!(leafSegmentInfo, node, next_node);

                if (*seginfo).items.is_null() {
                    (*seginfo).items =
                        ginPostingListDecode((*seginfo).seg, &mut (*seginfo).nitems);
                }
                if (*nextseg).items.is_null() {
                    (*nextseg).items =
                        ginPostingListDecode((*nextseg).seg, &mut (*nextseg).nitems);
                }
                let mut nmerged_v: c_int = 0;
                (*nextseg).items = ginMergeItemPointers(
                    (*seginfo).items,
                    (*seginfo).nitems as uint32,
                    (*nextseg).items,
                    (*nextseg).nitems as uint32,
                    &mut nmerged_v,
                );
                nmerged = nmerged_v;
                Assert!(nmerged == (*seginfo).nitems + (*nextseg).nitems);
                (*nextseg).nitems = nmerged;
                (*nextseg).seg = null_mut();

                (*nextseg).action = GIN_SEGMENT_REPLACE as c_char;
                (*nextseg).modifieditems = null_mut();
                (*nextseg).nmodifieditems = 0;

                if (*seginfo).action == GIN_SEGMENT_INSERT as c_char {
                    dlist_delete(cur_node);
                    cur_node = next_node;
                    continue;
                } else {
                    (*seginfo).action = GIN_SEGMENT_DELETE as c_char;
                    (*seginfo).seg = null_mut();
                }
            }

            (*seginfo).items = null_mut();
            (*seginfo).nitems = 0;
        }

        if (*seginfo).action == GIN_SEGMENT_DELETE as c_char {
            cur_node = next_node;
            continue;
        }

        /*
         * OK, we now have a compressed version of this segment ready for
         * copying to the page. Did we exceed the size that fits on one page?
         */
        segsize = SizeOfGinPostingList((*seginfo).seg) as c_int;
        if pgused + segsize > GinDataPageMaxDataSize() as c_int {
            if !needsplit {
                /* switch to right page */
                Assert!(pgused > 0);
                (*leaf).lastleft = dlist_prev_node(&mut (*leaf).segments, cur_node);
                needsplit = true;
                (*leaf).lsize = pgused;
                pgused = 0;
            } else {
                /*
                 * Filled both pages. The last segment we constructed did not
                 * fit.
                 */
                *remaining = (*(*seginfo).seg).first;

                /*
                 * remove all segments that did not fit from the list.
                 */
                while dlist_has_next(&(*leaf).segments, cur_node) {
                    dlist_delete(dlist_next_node(&mut (*leaf).segments, cur_node));
                }
                dlist_delete(cur_node);
                break;
            }
        }

        pgused += segsize;

        cur_node = next_node;
    }

    if !needsplit {
        (*leaf).lsize = pgused;
        (*leaf).rsize = 0;
    } else {
        (*leaf).rsize = pgused;
    }

    Assert!((*leaf).lsize <= GinDataPageMaxDataSize() as c_int);
    Assert!((*leaf).rsize <= GinDataPageMaxDataSize() as c_int);

    /*
     * Make a palloc'd copy of every segment after the first modified one,
     * because as we start copying items to the original page, we might
     * overwrite an existing segment.
     */
    modified = false;
    dlist_foreach!(iter, &mut (*leaf).segments, {
        let seginfo: *mut leafSegmentInfo = dlist_container!(leafSegmentInfo, node, iter.cur);

        if !modified && (*seginfo).action != GIN_SEGMENT_UNMODIFIED as c_char {
            modified = true;
        } else if modified && (*seginfo).action == GIN_SEGMENT_UNMODIFIED as c_char {
            let tmp: *mut GinPostingList;

            segsize = SizeOfGinPostingList((*seginfo).seg) as c_int;
            tmp = palloc(segsize as usize) as *mut GinPostingList;
            memcpy(
                tmp as *mut c_void,
                (*seginfo).seg as *const c_void,
                segsize as usize,
            );
            (*seginfo).seg = tmp;
        }
    });

    needsplit
}

/*** Functions that are exported to the rest of the GIN code ***/

/*
 * Creates new posting tree containing the given TIDs. Returns the page
 * number of the root of the new posting tree.
 *
 * items[] must be in sorted order with no duplicates.
 */
pub unsafe fn createPostingTree(
    index: Relation,
    items: *mut ItemPointerData,
    nitems: uint32,
    buildStats: *mut GinStatsData,
    entrybuffer: Buffer,
) -> BlockNumber {
    let blkno: BlockNumber;
    let buffer: Buffer;
    let tmppage: Page;
    let page: Page;
    let mut ptr: Pointer;
    let mut nrootitems: c_int;
    let mut rootsize: c_int;
    let is_build: bool = !buildStats.is_null();

    /* Construct the new root page in memory first. */
    tmppage = palloc(BLCKSZ as usize) as Page;
    GinInitPage(
        tmppage,
        (GIN_DATA | GIN_LEAF | GIN_COMPRESSED) as uint32,
        BLCKSZ as Size,
    );
    (*GinPageGetOpaque(tmppage)).rightlink = InvalidBlockNumber;

    /*
     * Write as many of the items to the root page as fit. In segments of max
     * GinPostingListSegmentMaxSize bytes each.
     */
    nrootitems = 0;
    rootsize = 0;
    ptr = GinDataLeafPageGetPostingList(tmppage) as Pointer;
    while (nrootitems as uint32) < nitems {
        let segment: *mut GinPostingList;
        let mut npacked: c_int = 0;
        let segsize: c_int;

        segment = ginCompressPostingList(
            items.add(nrootitems as usize),
            (nitems - nrootitems as uint32) as c_int,
            GinPostingListSegmentMaxSize as c_int,
            &mut npacked,
        );
        segsize = SizeOfGinPostingList(segment) as c_int;
        if rootsize + segsize > GinDataPageMaxDataSize() as c_int {
            pfree(segment as *mut c_void);
            break;
        }

        memcpy(
            ptr as *mut c_void,
            segment as *const c_void,
            segsize as usize,
        );
        ptr = ptr.add(segsize as usize);
        rootsize += segsize;
        nrootitems += npacked;
        pfree(segment as *mut c_void);
    }
    GinDataPageSetDataSize(tmppage, rootsize as usize);

    /*
     * All set. Get a new physical page, and copy the in-memory page to it.
     */
    buffer = GinNewBuffer(index);
    page = BufferGetPage(buffer);
    blkno = BufferGetBlockNumber(buffer);

    /*
     * Copy any predicate locks from the entry tree leaf (containing posting
     * list) to the posting tree.
     */
    PredicateLockPageSplit(index, BufferGetBlockNumber(entrybuffer), blkno);

    START_CRIT_SECTION();

    PageRestoreTempPage(tmppage, page);
    MarkBufferDirty(buffer);

    if RelationNeedsWAL(index) && !is_build {
        let recptr: XLogRecPtr;
        let mut data: ginxlogCreatePostingTree = core::mem::zeroed();

        data.size = rootsize as uint32;

        XLogBeginInsert();
        XLogRegisterData(
            &data as *const ginxlogCreatePostingTree as *const c_void,
            size_of::<ginxlogCreatePostingTree>() as u32,
        );

        XLogRegisterData(
            GinDataLeafPageGetPostingList(page) as *const c_void,
            rootsize as u32,
        );
        XLogRegisterBuffer(0, buffer, REGBUF_WILL_INIT);

        recptr = XLogInsert(RM_GIN_ID, XLOG_GIN_CREATE_PTREE);
        PageSetLSN(page, recptr);
    }

    UnlockReleaseBuffer(buffer);

    END_CRIT_SECTION();

    /* During index build, count the newly-added data page */
    if !buildStats.is_null() {
        (*buildStats).nDataPages += 1;
    }

    elog!(DEBUG2, "created GIN posting tree with {} items", nrootitems);

    /*
     * Add any remaining TIDs to the newly-created posting tree.
     */
    if nitems > nrootitems as uint32 {
        ginInsertItemPointers(
            index,
            blkno,
            items.add(nrootitems as usize),
            nitems - nrootitems as uint32,
            buildStats,
        );
    }

    blkno
}

unsafe fn ginPrepareDataScan(btree: GinBtree, index: Relation, rootBlkno: BlockNumber) {
    core::ptr::write_bytes(btree as *mut u8, 0, size_of::<GinBtreeData>());

    (*btree).index = index;
    (*btree).rootBlkno = rootBlkno;

    (*btree).findChildPage = Some(dataLocateItem);
    (*btree).getLeftMostChild = Some(dataGetLeftMostPage);
    (*btree).isMoveRight = Some(dataIsMoveRight);
    (*btree).findItem = None;
    (*btree).findChildPtr = Some(dataFindChildPtr);
    (*btree).beginPlaceToPage = Some(dataBeginPlaceToPage);
    (*btree).execPlaceToPage = Some(dataExecPlaceToPage);
    (*btree).fillRoot = Some(ginDataFillRoot);
    (*btree).prepareDownlink = Some(dataPrepareDownlink);

    (*btree).isData = true;
    (*btree).fullScan = false;
    (*btree).isBuild = false;
}

/*
 * Inserts array of item pointers, may execute several tree scan (very rare)
 */
pub unsafe fn ginInsertItemPointers(
    index: Relation,
    rootBlkno: BlockNumber,
    items: *mut ItemPointerData,
    nitem: uint32,
    buildStats: *mut GinStatsData,
) {
    let mut btree: GinBtreeData = core::mem::zeroed();
    let mut insertdata: GinBtreeDataLeafInsertData = core::mem::zeroed();
    let mut stack: *mut GinBtreeStack;

    ginPrepareDataScan(&mut btree, index, rootBlkno);
    btree.isBuild = !buildStats.is_null();
    insertdata.items = items;
    insertdata.nitem = nitem;
    insertdata.curitem = 0;

    while insertdata.curitem < insertdata.nitem {
        /* search for the leaf page where the first item should go to */
        btree.itemptr = *insertdata.items.add(insertdata.curitem as usize);
        stack = ginFindLeafPage(&mut btree, false, true);

        ginInsertValue(
            &mut btree,
            stack,
            &mut insertdata as *mut GinBtreeDataLeafInsertData as *mut c_void,
            buildStats,
        );
    }
}

/*
 * Starts a new scan on a posting tree.
 */
pub unsafe fn ginScanBeginPostingTree(
    btree: GinBtree,
    index: Relation,
    rootBlkno: BlockNumber,
) -> *mut GinBtreeStack {
    let stack: *mut GinBtreeStack;

    ginPrepareDataScan(btree, index, rootBlkno);

    (*btree).fullScan = true;

    stack = ginFindLeafPage(btree, true, false);

    stack
}
