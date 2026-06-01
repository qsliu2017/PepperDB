//! Translation of postgres/src/backend/storage/page/bufpage.c
//! POSTGRES standard buffer page code.
//!
//! The buffer-page access layer: it operates purely on an in-memory BLCKSZ page
//! buffer (no disk I/O).  A page is laid out as
//!   [ PageHeaderData ][ line-pointer array (grows up) ] ... free ... [ tuples (grow down) ][ special ]
//!
//! #include "access/htup_details.h" -> crate::access::htup_details (MaxHeapTuplesPerPage)
//! #include "access/itup.h"         -> MaxIndexTuplesPerPage (PageIndexMultiDelete)
//! #include "access/xlog.h" / pgstat.h -> not needed here
//! #include "storage/checksum.h"    -> crate::storage::checksum
//! #include "utils/memdebug.h"      -> VALGRIND_CHECK_MEM_IS_DEFINED (no-op)
//! #include "utils/memutils.h"      -> MemoryContextAllocAligned/TopMemoryContext
//!
//! The struct/macro layer (PageHeaderData, the inline page accessors, the
//! PD_* / PAI_* / PIV_* flags) lives in storage/bufpage.h, which is translated in
//! crate::storage::bufpage; we import those here.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::prelude::*;
use crate::access::htup_details::MaxHeapTuplesPerPage;
const MaxIndexTuplesPerPage: usize = 1358; /* (BLCKSZ - SizeOfPageHeaderData) / (sizeof(ItemIdData)+sizeof(IndexTupleData)); access/itup.h */
use crate::c::{int16, uint16, MAXALIGN, MemSet, Offset};
use crate::pg_config::BLCKSZ;
use crate::storage::block::BlockNumber;
use crate::storage::bufpage::{
    Item, LocationIndex, Page, PageHeader, PageHeaderData,
    PAI_IS_HEAP, PAI_OVERWRITE, PD_VALID_FLAG_BITS,
    PG_PAGE_LAYOUT_VERSION, PIV_IGNORE_CHECKSUM_FAILURE, PIV_LOG_LOG, PIV_LOG_WARNING,
    SizeOfPageHeaderData,
    PageClearHasFreeLinePointers, PageGetItem, PageGetItemId, PageGetMaxOffsetNumber,
    PageGetPageSize, PageGetSpecialPointer, PageGetSpecialSize, PageHasFreeLinePointers,
    PageIsEmpty, PageIsNew, PageSetHasFreeLinePointers, PageSetPageSizeAndVersion,
};
/* storage/bufpage.h: PageData is the page-contents byte type; Page == PageData *. */
pub type PageData = c_char;
use crate::storage::itemid::*;
use crate::storage::off::{
    FirstOffsetNumber, InvalidOffsetNumber, OffsetNumber, OffsetNumberIsValid, OffsetNumberNext,
};
use crate::utils::mmgr::mcxt::MemoryContextAllocAligned;
use core::ffi::{c_char, c_int, c_uint, c_void};
use core::mem::size_of;

extern "C" {
    fn memcpy(dst: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
    fn memmove(dst: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
}

/* errcodes.h (the errcode() shim ignores the value). */
const ERRCODE_DATA_CORRUPTED: c_int = 0;
/* pg_config_manual.h: I/O-friendly memory alignment. */
const PG_IO_ALIGN_SIZE: Size = 4096;

#[inline]
unsafe fn hdr(page: *const c_char) -> *mut PageHeaderData {
    page as *mut PageHeaderData
}
/* base of the line-pointer array. */
#[inline]
unsafe fn linp_base(page: *const c_char) -> *mut ItemIdData {
    (page as *mut u8).add(SizeOfPageHeaderData) as *mut ItemIdData
}

// ----------------------------------------------------------------------------
//   local helpers for unported deps
// ----------------------------------------------------------------------------

/* GUC: whether data-page checksums are enabled.  TODO(pg-port): real GUC. */
#[inline]
fn DataChecksumsEnabled() -> bool {
    false
}
/* GUC variable */
pub static mut ignore_checksum_failure: bool = false;
/* memdebug.h valgrind hook -- no-op. */
#[inline]
unsafe fn VALGRIND_CHECK_MEM_IS_DEFINED(_p: *const c_void, _n: usize) {}

/* Is the whole [ptr, ptr+len) region zero bytes? */
unsafe fn pg_memory_is_all_zeros(ptr: *const c_void, len: usize) -> bool {
    let bytes = core::slice::from_raw_parts(ptr as *const u8, len);
    bytes.iter().all(|&b| b == 0)
}

// ----------------------------------------------------------------------------
//						Page support functions
// ----------------------------------------------------------------------------

/*
 * PageInit
 *		Initializes the contents of a page.
 *		Note that we don't calculate an initial checksum here; that's not done
 *		until it's time to write.
 */
pub unsafe fn PageInit(page: Page, pageSize: Size, specialSize: Size) {
    let p: PageHeader = page as PageHeader;
    let specialSize = MAXALIGN(specialSize);

    Assert!(pageSize == BLCKSZ);
    Assert!(pageSize > specialSize + SizeOfPageHeaderData);

    /* Make sure all fields of page are zero, as well as unused space */
    MemSet(p as *mut c_void, 0, pageSize);

    (*p).pd_flags = 0;
    (*p).pd_lower = SizeOfPageHeaderData as LocationIndex;
    (*p).pd_upper = (pageSize - specialSize) as LocationIndex;
    (*p).pd_special = (pageSize - specialSize) as LocationIndex;
    PageSetPageSizeAndVersion(page, pageSize, PG_PAGE_LAYOUT_VERSION as u8);
    /* p->pd_prune_xid = InvalidTransactionId;		done by above MemSet */
}

/*
 * PageIsVerified
 *		Check that the page header and checksum (if any) appear valid.
 *
 * This is called when a page has just been read in from disk.  The idea is
 * to cheaply detect trashed pages before we go nuts following bogus line
 * pointers, testing invalid transaction identifiers, etc.
 *
 * It turns out to be necessary to allow zeroed pages here too.  Even though
 * this routine is *not* called when deliberately adding a page to a relation,
 * there are scenarios in which a zeroed page might be found in a table.
 *
 * If flag PIV_LOG_WARNING/PIV_LOG_LOG is set, a WARNING/LOG message is logged
 * in the event of a checksum failure.
 *
 * If flag PIV_IGNORE_CHECKSUM_FAILURE is set, checksum failures will cause a
 * message about the failure to be emitted, but will not cause
 * PageIsVerified() to return false.
 *
 * To allow the caller to report statistics about checksum failures,
 * *checksum_failure_p can be passed in.
 */
pub unsafe fn PageIsVerified(
    page: *mut PageData,
    blkno: BlockNumber,
    flags: c_int,
    checksum_failure_p: *mut bool,
) -> bool {
    let p: *const PageHeaderData = page as *const PageHeaderData;
    let mut checksum_failure = false;
    let mut header_sane = false;
    let mut checksum: uint16 = 0;

    if !checksum_failure_p.is_null() {
        *checksum_failure_p = false;
    }

    /*
     * Don't verify page data unless the page passes basic non-zero test
     */
    if !PageIsNew(page) {
        if DataChecksumsEnabled() {
            checksum = crate::storage::checksum::pg_checksum_page(page as *mut c_char, blkno);

            if checksum != (*p).pd_checksum {
                checksum_failure = true;
                if !checksum_failure_p.is_null() {
                    *checksum_failure_p = true;
                }
            }
        }

        /*
         * The following checks don't prove the header is correct, only that
         * it looks sane enough to allow into the buffer pool.
         */
        if ((*p).pd_flags & !PD_VALID_FLAG_BITS) == 0
            && (*p).pd_lower <= (*p).pd_upper
            && (*p).pd_upper <= (*p).pd_special
            && ((*p).pd_special as usize) <= BLCKSZ
            && (*p).pd_special as usize == MAXALIGN((*p).pd_special as usize)
        {
            header_sane = true;
        }

        if header_sane && !checksum_failure {
            return true;
        }
    }

    /* Check all-zeroes case */
    if pg_memory_is_all_zeros(page as *const c_void, BLCKSZ) {
        return true;
    }

    /*
     * Throw a WARNING/LOG, as instructed by PIV_LOG_*, if the checksum fails,
     * but only after we've checked for the all-zeroes case.
     */
    if checksum_failure {
        if (flags & (PIV_LOG_WARNING | PIV_LOG_LOG)) != 0 {
            // C also: errcode(ERRCODE_DATA_CORRUPTED).  C logs at WARNING or LOG.
            let _ = errcode(ERRCODE_DATA_CORRUPTED);
            elog!(
                WARNING,
                "page verification failed, calculated checksum {} but expected {}",
                checksum,
                (*p).pd_checksum
            );
        }

        if header_sane && (flags & PIV_IGNORE_CHECKSUM_FAILURE) != 0 {
            return true;
        }
    }

    false
}

/*
 *	PageAddItemExtended
 *
 *	Add an item to a page.  Return value is the offset at which it was
 *	inserted, or InvalidOffsetNumber if the item is not inserted for any
 *	reason.  A WARNING is issued indicating the reason for the refusal.
 *
 *	offsetNumber must be either InvalidOffsetNumber to specify finding a
 *	free line pointer, or a value between FirstOffsetNumber and one past
 *	the last existing item, to specify using that particular line pointer.
 *
 *	If offsetNumber is valid and flag PAI_OVERWRITE is set, we just store
 *	the item at the specified offsetNumber, which must be either a
 *	currently-unused line pointer, or one past the last existing item.
 *
 *	If offsetNumber is valid and flag PAI_OVERWRITE is not set, insert
 *	the item at the specified offsetNumber, moving existing items later
 *	in the array to make room.
 *
 *	If offsetNumber is not valid, then assign a slot by finding the first
 *	one that is both unused and deallocated.
 *
 *	If flag PAI_IS_HEAP is set, we enforce that there can't be more than
 *	MaxHeapTuplesPerPage line pointers on the page.
 *
 *	!!! EREPORT(ERROR) IS DISALLOWED HERE !!!
 */
pub unsafe fn PageAddItemExtended(
    page: Page,
    item: Item,
    size: Size,
    mut offsetNumber: OffsetNumber,
    flags: c_int,
) -> OffsetNumber {
    let phdr: PageHeader = page as PageHeader;
    let alignedSize: Size;
    let lower: c_int;
    let upper: c_int;
    let mut itemId: ItemId;
    let limit: OffsetNumber;
    let mut needshuffle = false;

    /*
     * Be wary about corrupted page pointers
     */
    if ((*phdr).pd_lower as usize) < SizeOfPageHeaderData
        || (*phdr).pd_lower > (*phdr).pd_upper
        || (*phdr).pd_upper > (*phdr).pd_special
        || ((*phdr).pd_special as usize) > BLCKSZ
    {
        // C also: errcode(ERRCODE_DATA_CORRUPTED).
        let _ = errcode(ERRCODE_DATA_CORRUPTED);
        ereport!(
            PANIC,
            errmsg!(
                "corrupted page pointers: lower = {}, upper = {}, special = {}",
                (*phdr).pd_lower,
                (*phdr).pd_upper,
                (*phdr).pd_special
            )
        );
    }

    /*
     * Select offsetNumber to place the new item at
     */
    limit = OffsetNumberNext(PageGetMaxOffsetNumber(page));

    /* was offsetNumber passed in? */
    if OffsetNumberIsValid(offsetNumber) {
        /* yes, check it */
        if (flags & PAI_OVERWRITE) != 0 {
            if offsetNumber < limit {
                itemId = PageGetItemId(page, offsetNumber);
                if ItemIdIsUsed(itemId) || ItemIdHasStorage(itemId) {
                    elog!(WARNING, "will not overwrite a used ItemId");
                    return InvalidOffsetNumber;
                }
            }
        } else if offsetNumber < limit {
            needshuffle = true; /* need to move existing linp's */
        }
    } else {
        /* offsetNumber was not passed in, so find a free slot */
        /* if no free slot, we'll put it at limit (1st open slot) */
        if PageHasFreeLinePointers(page) {
            /*
             * Scan line pointer array to locate a "recyclable" (unused)
             * ItemId.
             *
             * Always use earlier items first.  PageTruncateLinePointerArray
             * can only truncate unused items when they appear as a contiguous
             * group at the end of the line pointer array.
             */
            offsetNumber = FirstOffsetNumber;
            while offsetNumber < limit
            /* limit is maxoff+1 */
            {
                itemId = PageGetItemId(page, offsetNumber);

                /*
                 * We check for no storage as well, just to be paranoid;
                 * unused items should never have storage.  Assert() that the
                 * invariant is respected too.
                 */
                Assert!(ItemIdIsUsed(itemId) || !ItemIdHasStorage(itemId));

                if !ItemIdIsUsed(itemId) && !ItemIdHasStorage(itemId) {
                    break;
                }
                offsetNumber += 1;
            }
            if offsetNumber >= limit {
                /* the hint is wrong, so reset it */
                PageClearHasFreeLinePointers(page);
            }
        } else {
            /* don't bother searching if hint says there's no free slot */
            offsetNumber = limit;
        }
    }

    /* Reject placing items beyond the first unused line pointer */
    if offsetNumber > limit {
        elog!(WARNING, "specified item offset is too large");
        return InvalidOffsetNumber;
    }

    /* Reject placing items beyond heap boundary, if heap */
    if (flags & PAI_IS_HEAP) != 0 && offsetNumber as c_int > MaxHeapTuplesPerPage {
        elog!(WARNING, "can't put more than MaxHeapTuplesPerPage items in a heap page");
        return InvalidOffsetNumber;
    }

    /*
     * Compute new lower and upper pointers for page, see if it'll fit.
     *
     * Note: do arithmetic as signed ints, to avoid mistakes if, say,
     * alignedSize > pd_upper.
     */
    if offsetNumber == limit || needshuffle {
        lower = (*phdr).pd_lower as c_int + size_of::<ItemIdData>() as c_int;
    } else {
        lower = (*phdr).pd_lower as c_int;
    }

    alignedSize = MAXALIGN(size);

    upper = (*phdr).pd_upper as c_int - alignedSize as c_int;

    if lower > upper {
        return InvalidOffsetNumber;
    }

    /*
     * OK to insert the item.  First, shuffle the existing pointers if needed.
     */
    itemId = PageGetItemId(page, offsetNumber);

    if needshuffle {
        memmove(
            itemId.add(1) as *mut c_void,
            itemId as *const c_void,
            (limit - offsetNumber) as usize * size_of::<ItemIdData>(),
        );
    }

    /* set the line pointer */
    ItemIdSetNormal(itemId, upper as u32, size as u32);

    /*
     * Items normally contain no uninitialized bytes.  Core bufpage consumers
     * conform, but this is not a necessary coding rule; a new index AM could
     * opt to depart from it.  However, data type input functions and other
     * C-language functions that synthesize datums should initialize all
     * bytes; datumIsEqual() relies on this.  Testing here, along with the
     * similar check in printtup(), helps to catch such mistakes.
     */
    VALGRIND_CHECK_MEM_IS_DEFINED(item as *const c_void, size);

    /* copy the item's data onto the page */
    memcpy(page.add(upper as usize) as *mut c_void, item as *const c_void, size);

    /* adjust page header */
    (*phdr).pd_lower = lower as LocationIndex;
    (*phdr).pd_upper = upper as LocationIndex;

    offsetNumber
}

/*
 * PageGetTempPage
 *		Get a temporary page in local memory for special processing.
 *		The returned page is not initialized at all; caller must do that.
 */
pub unsafe fn PageGetTempPage(page: *const PageData) -> Page {
    let pageSize: Size;
    let temp: Page;

    pageSize = PageGetPageSize(page);
    temp = palloc(pageSize) as Page;

    temp
}

/*
 * PageGetTempPageCopy
 *		Get a temporary page in local memory for special processing.
 *		The page is initialized by copying the contents of the given page.
 */
pub unsafe fn PageGetTempPageCopy(page: *const PageData) -> Page {
    let pageSize: Size;
    let temp: Page;

    pageSize = PageGetPageSize(page);
    temp = palloc(pageSize) as Page;

    memcpy(temp as *mut c_void, page as *const c_void, pageSize);

    temp
}

/*
 * PageGetTempPageCopySpecial
 *		Get a temporary page in local memory for special processing.
 *		The page is PageInit'd with the same special-space size as the
 *		given page, and the special space is copied from the given page.
 */
pub unsafe fn PageGetTempPageCopySpecial(page: *const PageData) -> Page {
    let pageSize: Size;
    let temp: Page;

    pageSize = PageGetPageSize(page);
    temp = palloc(pageSize) as Page;

    PageInit(temp, pageSize, PageGetSpecialSize(page) as Size);
    memcpy(
        PageGetSpecialPointer(temp) as *mut c_void,
        PageGetSpecialPointer(page as Page) as *const c_void,
        PageGetSpecialSize(page) as usize,
    );

    temp
}

/*
 * PageRestoreTempPage
 *		Copy temporary page back to permanent page after special processing
 *		and release the temporary page.
 */
pub unsafe fn PageRestoreTempPage(tempPage: Page, oldPage: Page) {
    let pageSize: Size;

    pageSize = PageGetPageSize(tempPage);
    memcpy(oldPage as *mut c_void, tempPage as *const c_void, pageSize);

    pfree(tempPage as *mut c_void);
}

/*
 * Tuple defrag support for PageRepairFragmentation and PageIndexMultiDelete
 */
#[derive(Clone, Copy, Default)]
struct itemIdCompactData {
    offsetindex: uint16, /* linp array index */
    itemoff: int16,      /* page offset of item data */
    alignedlen: uint16,  /* MAXALIGN(item data len) */
}
type itemIdCompact = *mut itemIdCompactData;

#[repr(C, align(8))]
struct PGAlignedBlock {
    data: [c_char; BLCKSZ],
}

/*
 * After removing or marking some line pointers unused, move the tuples to
 * remove the gaps caused by the removed items and reorder them back into
 * reverse line pointer order in the page.
 *
 * This function can often be fairly hot, so it pays to take some measures to
 * make it as optimal as possible.
 *
 * Callers may pass 'presorted' as true if the 'itemidbase' array is sorted in
 * descending order of itemoff.  When this is true we can just memmove()
 * tuples towards the end of the page.  This is quite a common case as it's
 * the order that tuples are initially inserted into pages.  When we call this
 * function to defragment the tuples in the page then any new line pointers
 * added to the page will keep that presorted order, so hitting this case is
 * still very common for tables that are commonly updated.
 *
 * When the 'itemidbase' array is not presorted then we're unable to just
 * memmove() tuples around freely.  Doing so could cause us to overwrite the
 * memory belonging to a tuple we've not moved yet.  In this case, we copy all
 * the tuples that need to be moved into a temporary buffer.  We can then
 * simply memcpy() out of that temp buffer back into the page at the correct
 * location.  Tuples are copied back into the page in the same order as the
 * 'itemidbase' array, so we end up reordering the tuples back into reverse
 * line pointer order.  This will increase the chances of hitting the
 * presorted case the next time around.
 *
 * Callers must ensure that nitems is > 0
 */
unsafe fn compactify_tuples(itemidbase: itemIdCompact, nitems: c_int, page: Page, presorted: bool) {
    let phdr: PageHeader = page as PageHeader;
    let mut upper: Offset;
    let mut copy_tail: Offset;
    let mut copy_head: Offset;
    let mut itemidptr: itemIdCompact;
    let mut i: c_int;

    /* Code within will not work correctly if nitems == 0 */
    Assert!(nitems > 0);

    if presorted {
        #[cfg(debug_assertions)]
        {
            /*
             * Verify we've not gotten any new callers that are incorrectly
             * passing a true presorted value.
             */
            let mut lastoff: Offset = (*phdr).pd_special as Offset;

            i = 0;
            while i < nitems {
                itemidptr = itemidbase.add(i as usize);

                Assert!(lastoff > (*itemidptr).itemoff as Offset);

                lastoff = (*itemidptr).itemoff as Offset;
                i += 1;
            }
        }

        /*
         * 'itemidbase' is already in the optimal order, i.e, lower item
         * pointers have a higher offset.  This allows us to memmove() the
         * tuples up to the end of the page without having to worry about
         * overwriting other tuples that have not been moved yet.
         *
         * There's a good chance that there are tuples already right at the
         * end of the page that we can simply skip over because they're
         * already in the correct location within the page.  We'll do that
         * first...
         */
        upper = (*phdr).pd_special as Offset;
        i = 0;
        loop {
            itemidptr = itemidbase.add(i as usize);
            if upper != (*itemidptr).itemoff as Offset + (*itemidptr).alignedlen as Offset {
                break;
            }
            upper -= (*itemidptr).alignedlen as Offset;

            i += 1;
            if !(i < nitems) {
                break;
            }
        }

        /*
         * Now that we've found the first tuple that needs to be moved, we can
         * do the tuple compactification.  We try and make the least number of
         * memmove() calls and only call memmove() when there's a gap.  When
         * we see a gap we just move all tuples after the gap up until the
         * point of the last move operation.
         */
        copy_head = (*itemidptr).itemoff as Offset + (*itemidptr).alignedlen as Offset;
        copy_tail = copy_head;
        while i < nitems {
            let lp: ItemId;

            itemidptr = itemidbase.add(i as usize);
            lp = PageGetItemId(page, (*itemidptr).offsetindex + 1);

            if copy_head != (*itemidptr).itemoff as Offset + (*itemidptr).alignedlen as Offset {
                memmove(
                    page.add(upper as usize) as *mut c_void,
                    page.add(copy_head as usize) as *const c_void,
                    (copy_tail - copy_head) as usize,
                );

                /*
                 * We've now moved all tuples already seen, but not the
                 * current tuple, so we set the copy_tail to the end of this
                 * tuple so it can be moved in another iteration of the loop.
                 */
                copy_tail = (*itemidptr).itemoff as Offset + (*itemidptr).alignedlen as Offset;
            }
            /* shift the target offset down by the length of this tuple */
            upper -= (*itemidptr).alignedlen as Offset;
            /* point the copy_head to the start of this tuple */
            copy_head = (*itemidptr).itemoff as Offset;

            /* update the line pointer to reference the new offset */
            ItemIdSetOffset(lp, upper as u32);
            i += 1;
        }

        /* move the remaining tuples. */
        memmove(
            page.add(upper as usize) as *mut c_void,
            page.add(copy_head as usize) as *const c_void,
            (copy_tail - copy_head) as usize,
        );
    } else {
        let mut scratch = PGAlignedBlock { data: [0; BLCKSZ] };
        let scratchptr = scratch.data.as_mut_ptr();

        /*
         * Non-presorted case:  The tuples in the itemidbase array may be in
         * any order.  So, in order to move these to the end of the page we
         * must make a temp copy of each tuple that needs to be moved before
         * we copy them back into the page at the new offset.
         *
         * If a large percentage of tuples have been pruned (>75%) then we'll
         * copy these into the temp buffer tuple-by-tuple, otherwise, we'll
         * just do a single memcpy() for all tuples that need to be moved.
         * When so many tuples have been removed there's likely to be a lot of
         * gaps and it's unlikely that many non-movable tuples remain at the
         * end of the page.
         */
        if nitems < PageGetMaxOffsetNumber(page) as c_int / 4 {
            i = 0;
            loop {
                itemidptr = itemidbase.add(i as usize);
                memcpy(
                    scratchptr.add((*itemidptr).itemoff as usize) as *mut c_void,
                    page.add((*itemidptr).itemoff as usize) as *const c_void,
                    (*itemidptr).alignedlen as usize,
                );
                i += 1;
                if !(i < nitems) {
                    break;
                }
            }

            /* Set things up for the compactification code below */
            i = 0;
            itemidptr = itemidbase.add(0);
            upper = (*phdr).pd_special as Offset;
        } else {
            upper = (*phdr).pd_special as Offset;

            /*
             * Many tuples are likely to already be in the correct location.
             * There's no need to copy these into the temp buffer.  Instead
             * we'll just skip forward in the itemidbase array to the position
             * that we do need to move tuples from so that the code below just
             * leaves these ones alone.
             */
            i = 0;
            loop {
                itemidptr = itemidbase.add(i as usize);
                if upper != (*itemidptr).itemoff as Offset + (*itemidptr).alignedlen as Offset {
                    break;
                }
                upper -= (*itemidptr).alignedlen as Offset;

                i += 1;
                if !(i < nitems) {
                    break;
                }
            }

            /* Copy all tuples that need to be moved into the temp buffer */
            memcpy(
                scratchptr.add((*phdr).pd_upper as usize) as *mut c_void,
                page.add((*phdr).pd_upper as usize) as *const c_void,
                (upper - (*phdr).pd_upper as Offset) as usize,
            );
        }

        /*
         * Do the tuple compactification.  itemidptr is already pointing to
         * the first tuple that we're going to move.  Here we collapse the
         * memcpy calls for adjacent tuples into a single call.  This is done
         * by delaying the memcpy call until we find a gap that needs to be
         * closed.
         */
        copy_head = (*itemidptr).itemoff as Offset + (*itemidptr).alignedlen as Offset;
        copy_tail = copy_head;
        while i < nitems {
            let lp: ItemId;

            itemidptr = itemidbase.add(i as usize);
            lp = PageGetItemId(page, (*itemidptr).offsetindex + 1);

            /* copy pending tuples when we detect a gap */
            if copy_head != (*itemidptr).itemoff as Offset + (*itemidptr).alignedlen as Offset {
                memcpy(
                    page.add(upper as usize) as *mut c_void,
                    scratchptr.add(copy_head as usize) as *const c_void,
                    (copy_tail - copy_head) as usize,
                );

                /*
                 * We've now copied all tuples already seen, but not the
                 * current tuple, so we set the copy_tail to the end of this
                 * tuple.
                 */
                copy_tail = (*itemidptr).itemoff as Offset + (*itemidptr).alignedlen as Offset;
            }
            /* shift the target offset down by the length of this tuple */
            upper -= (*itemidptr).alignedlen as Offset;
            /* point the copy_head to the start of this tuple */
            copy_head = (*itemidptr).itemoff as Offset;

            /* update the line pointer to reference the new offset */
            ItemIdSetOffset(lp, upper as u32);
            i += 1;
        }

        /* Copy the remaining chunk */
        memcpy(
            page.add(upper as usize) as *mut c_void,
            scratchptr.add(copy_head as usize) as *const c_void,
            (copy_tail - copy_head) as usize,
        );
    }

    (*phdr).pd_upper = upper as LocationIndex;
}

/*
 * PageRepairFragmentation
 *
 * Frees fragmented space on a heap page following pruning.
 *
 * This routine is usable for heap pages only, but see PageIndexMultiDelete.
 *
 * This routine removes unused line pointers from the end of the line pointer
 * array.  This is possible when dead heap-only tuples get removed by pruning,
 * especially when there were HOT chains with several tuples each beforehand.
 *
 * Caller had better have a full cleanup lock on page's buffer.  As a side
 * effect the page's PD_HAS_FREE_LINES hint bit will be set or unset as
 * needed.  Caller might also need to account for a reduction in the length of
 * the line pointer array following array truncation.
 */
pub unsafe fn PageRepairFragmentation(page: Page) {
    let pd_lower: Offset = (*(page as PageHeader)).pd_lower as Offset;
    let pd_upper: Offset = (*(page as PageHeader)).pd_upper as Offset;
    let pd_special: Offset = (*(page as PageHeader)).pd_special as Offset;
    let mut last_offset: Offset;
    let mut itemidbase: [itemIdCompactData; MaxHeapTuplesPerPage as usize] =
        [itemIdCompactData::default(); MaxHeapTuplesPerPage as usize];
    let mut itemidptr: itemIdCompact;
    let mut lp: ItemId;
    let nline: c_int;
    let nstorage: c_int;
    let mut nunused: c_int;
    let mut finalusedlp: OffsetNumber = InvalidOffsetNumber;
    let mut i: c_int;
    let mut totallen: Size;
    let mut presorted = true; /* For now */

    /*
     * It's worth the trouble to be more paranoid here than in most places,
     * because we are about to reshuffle data in (what is usually) a shared
     * disk buffer.  If we aren't careful then corrupted pointers, lengths,
     * etc could cause us to clobber adjacent disk buffers, spreading the data
     * loss further.  So, check everything.
     */
    if pd_lower < SizeOfPageHeaderData as Offset
        || pd_lower > pd_upper
        || pd_upper > pd_special
        || pd_special > BLCKSZ as Offset
        || pd_special != MAXALIGN(pd_special as usize) as Offset
    {
        let _ = errcode(ERRCODE_DATA_CORRUPTED);
        ereport!(
            ERROR,
            errmsg!(
                "corrupted page pointers: lower = {}, upper = {}, special = {}",
                pd_lower,
                pd_upper,
                pd_special
            )
        );
    }

    /*
     * Run through the line pointer array and collect data about live items.
     */
    nline = PageGetMaxOffsetNumber(page) as c_int;
    itemidptr = itemidbase.as_mut_ptr();
    nunused = 0;
    totallen = 0;
    last_offset = pd_special;
    i = FirstOffsetNumber as c_int;
    while i <= nline {
        lp = PageGetItemId(page, i as OffsetNumber);
        if ItemIdIsUsed(lp) {
            if ItemIdHasStorage(lp) {
                (*itemidptr).offsetindex = (i - 1) as uint16;
                (*itemidptr).itemoff = ItemIdGetOffset(lp) as int16;

                if last_offset > (*itemidptr).itemoff as Offset {
                    last_offset = (*itemidptr).itemoff as Offset;
                } else {
                    presorted = false;
                }

                if ((*itemidptr).itemoff as c_int) < pd_upper as c_int
                    || ((*itemidptr).itemoff as c_int) >= pd_special as c_int
                {
                    let _ = errcode(ERRCODE_DATA_CORRUPTED);
                    ereport!(ERROR, errmsg!("corrupted line pointer: {}", (*itemidptr).itemoff));
                }
                (*itemidptr).alignedlen = MAXALIGN(ItemIdGetLength(lp) as usize) as uint16;
                totallen += (*itemidptr).alignedlen as Size;
                itemidptr = itemidptr.add(1);
            }

            finalusedlp = i as OffsetNumber; /* Could be the final non-LP_UNUSED item */
        } else {
            /* Unused entries should have lp_len = 0, but make sure */
            Assert!(!ItemIdHasStorage(lp));
            ItemIdSetUnused(lp);
            nunused += 1;
        }
        i += 1;
    }

    nstorage = itemidptr.offset_from(itemidbase.as_ptr()) as c_int;
    if nstorage == 0 {
        /* Page is completely empty, so just reset it quickly */
        (*(page as PageHeader)).pd_upper = pd_special as LocationIndex;
    } else {
        /* Need to compact the page the hard way */
        if totallen > (pd_special - pd_lower) as Size {
            let _ = errcode(ERRCODE_DATA_CORRUPTED);
            ereport!(
                ERROR,
                errmsg!(
                    "corrupted item lengths: total {}, available space {}",
                    totallen,
                    pd_special - pd_lower
                )
            );
        }

        compactify_tuples(itemidbase.as_mut_ptr(), nstorage, page, presorted);
    }

    if finalusedlp as c_int != nline {
        /* The last line pointer is not the last used line pointer */
        let nunusedend: c_int = nline - finalusedlp as c_int;

        Assert!(nunused >= nunusedend && nunusedend > 0);

        /* remove trailing unused line pointers from the count */
        nunused -= nunusedend;
        /* truncate the line pointer array */
        (*(page as PageHeader)).pd_lower -=
            (size_of::<ItemIdData>() * nunusedend as usize) as LocationIndex;
    }

    /* Set hint bit for PageAddItemExtended */
    if nunused > 0 {
        PageSetHasFreeLinePointers(page);
    } else {
        PageClearHasFreeLinePointers(page);
    }
}

/*
 * PageTruncateLinePointerArray
 *
 * Removes unused line pointers at the end of the line pointer array.
 *
 * This routine is usable for heap pages only.  It is called by VACUUM during
 * its second pass over the heap.  We expect at least one LP_UNUSED line
 * pointer on the page (if VACUUM didn't have an LP_DEAD item on the page that
 * it just set to LP_UNUSED then it should not call here).
 *
 * We avoid truncating the line pointer array to 0 items, if necessary by
 * leaving behind a single remaining LP_UNUSED item.  This is a little
 * arbitrary, but it seems like a good idea to avoid leaving a PageIsEmpty()
 * page behind.
 *
 * Caller can have either an exclusive lock or a full cleanup lock on page's
 * buffer.  The page's PD_HAS_FREE_LINES hint bit will be set or unset based
 * on whether or not we leave behind any remaining LP_UNUSED items.
 */
pub unsafe fn PageTruncateLinePointerArray(page: Page) {
    let phdr: PageHeader = page as PageHeader;
    let mut countdone = false;
    let mut sethint = false;
    let mut nunusedend: c_int = 0;

    /* Scan line pointer array back-to-front */
    let mut i: c_int = PageGetMaxOffsetNumber(page) as c_int;
    while i >= FirstOffsetNumber as c_int {
        let lp: ItemId = PageGetItemId(page, i as OffsetNumber);

        if !countdone && i > FirstOffsetNumber as c_int {
            /*
             * Still determining which line pointers from the end of the array
             * will be truncated away.  Either count another line pointer as
             * safe to truncate, or notice that it's not safe to truncate
             * additional line pointers (stop counting line pointers).
             */
            if !ItemIdIsUsed(lp) {
                nunusedend += 1;
            } else {
                countdone = true;
            }
        } else {
            /*
             * Once we've stopped counting we still need to figure out if
             * there are any remaining LP_UNUSED line pointers somewhere more
             * towards the front of the array.
             */
            if !ItemIdIsUsed(lp) {
                /*
                 * This is an unused line pointer that we won't be truncating
                 * away -- so there is at least one.  Set hint on page.
                 */
                sethint = true;
                break;
            }
        }
        i -= 1;
    }

    if nunusedend > 0 {
        (*phdr).pd_lower -= (size_of::<ItemIdData>() * nunusedend as usize) as LocationIndex;

        // C also: #ifdef CLOBBER_FREED_MEMORY memset(page+pd_lower, 0x7F, ...).
    } else {
        Assert!(sethint);
    }

    /* Set hint bit for PageAddItemExtended */
    if sethint {
        PageSetHasFreeLinePointers(page);
    } else {
        PageClearHasFreeLinePointers(page);
    }
}

/*
 * PageGetFreeSpace
 *		Returns the size of the free (allocatable) space on a page,
 *		reduced by the space needed for a new line pointer.
 *
 * Note: this should usually only be used on index pages.  Use
 * PageGetHeapFreeSpace on heap pages.
 */
pub unsafe fn PageGetFreeSpace(page: *const PageData) -> Size {
    let phdr: *const PageHeaderData = page as *const PageHeaderData;
    let mut space: c_int;

    /*
     * Use signed arithmetic here so that we behave sensibly if pd_lower >
     * pd_upper.
     */
    space = (*phdr).pd_upper as c_int - (*phdr).pd_lower as c_int;

    if space < size_of::<ItemIdData>() as c_int {
        return 0;
    }
    space -= size_of::<ItemIdData>() as c_int;

    space as Size
}

/*
 * PageGetFreeSpaceForMultipleTuples
 *		Returns the size of the free (allocatable) space on a page,
 *		reduced by the space needed for multiple new line pointers.
 *
 * Note: this should usually only be used on index pages.  Use
 * PageGetHeapFreeSpace on heap pages.
 */
pub unsafe fn PageGetFreeSpaceForMultipleTuples(page: *const PageData, ntups: c_int) -> Size {
    let phdr: *const PageHeaderData = page as *const PageHeaderData;
    let mut space: c_int;

    /*
     * Use signed arithmetic here so that we behave sensibly if pd_lower >
     * pd_upper.
     */
    space = (*phdr).pd_upper as c_int - (*phdr).pd_lower as c_int;

    if space < ntups * size_of::<ItemIdData>() as c_int {
        return 0;
    }
    space -= ntups * size_of::<ItemIdData>() as c_int;

    space as Size
}

/*
 * PageGetExactFreeSpace
 *		Returns the size of the free (allocatable) space on a page,
 *		without any consideration for adding/removing line pointers.
 */
pub unsafe fn PageGetExactFreeSpace(page: *const PageData) -> Size {
    let phdr: *const PageHeaderData = page as *const PageHeaderData;
    let space: c_int;

    /*
     * Use signed arithmetic here so that we behave sensibly if pd_lower >
     * pd_upper.
     */
    space = (*phdr).pd_upper as c_int - (*phdr).pd_lower as c_int;

    if space < 0 {
        return 0;
    }

    space as Size
}

/*
 * PageGetHeapFreeSpace
 *		Returns the size of the free (allocatable) space on a page,
 *		reduced by the space needed for a new line pointer.
 *
 * The difference between this and PageGetFreeSpace is that this will return
 * zero if there are already MaxHeapTuplesPerPage line pointers in the page
 * and none are free.  We use this to enforce that no more than
 * MaxHeapTuplesPerPage line pointers are created on a heap page.
 */
pub unsafe fn PageGetHeapFreeSpace(page: *const PageData) -> Size {
    let mut space: Size;

    space = PageGetFreeSpace(page);
    if space > 0 {
        let nline: OffsetNumber;
        let mut offnum: OffsetNumber;

        /*
         * Are there already MaxHeapTuplesPerPage line pointers in the page?
         */
        nline = PageGetMaxOffsetNumber(page);
        if nline as c_int >= MaxHeapTuplesPerPage {
            if PageHasFreeLinePointers(page) {
                /*
                 * Since this is just a hint, we must confirm that there is
                 * indeed a free line pointer
                 */
                offnum = FirstOffsetNumber;
                while offnum <= nline {
                    let lp: ItemId = PageGetItemId(page as Page, offnum);

                    if !ItemIdIsUsed(lp) {
                        break;
                    }
                    offnum = OffsetNumberNext(offnum);
                }

                if offnum > nline {
                    /*
                     * The hint is wrong, but we can't clear it here since we
                     * don't have the ability to mark the page dirty.
                     */
                    space = 0;
                }
            } else {
                /*
                 * Although the hint might be wrong, PageAddItem will believe
                 * it anyway, so we must believe it too.
                 */
                space = 0;
            }
        }
    }
    space
}

/*
 * PageIndexTupleDelete
 *
 * This routine does the work of removing a tuple from an index page.
 *
 * Unlike heap pages, we compact out the line pointer for the removed tuple.
 */
pub unsafe fn PageIndexTupleDelete(page: Page, offnum: OffsetNumber) {
    let phdr: PageHeader = page as PageHeader;
    let addr: *mut c_char;
    let tup: ItemId;
    let mut size: Size;
    let offset: c_uint;
    let nbytes: c_int;
    let offidx: c_int;
    let mut nline: c_int;

    /*
     * As with PageRepairFragmentation, paranoia seems justified.
     */
    if ((*phdr).pd_lower as usize) < SizeOfPageHeaderData
        || (*phdr).pd_lower > (*phdr).pd_upper
        || (*phdr).pd_upper > (*phdr).pd_special
        || ((*phdr).pd_special as usize) > BLCKSZ
        || (*phdr).pd_special as usize != MAXALIGN((*phdr).pd_special as usize)
    {
        let _ = errcode(ERRCODE_DATA_CORRUPTED);
        ereport!(
            ERROR,
            errmsg!(
                "corrupted page pointers: lower = {}, upper = {}, special = {}",
                (*phdr).pd_lower,
                (*phdr).pd_upper,
                (*phdr).pd_special
            )
        );
    }

    nline = PageGetMaxOffsetNumber(page) as c_int;
    if (offnum as c_int) <= 0 || offnum as c_int > nline {
        elog!(ERROR, "invalid index offnum: {}", offnum);
    }

    /* change offset number to offset index */
    offidx = offnum as c_int - 1;

    tup = PageGetItemId(page, offnum);
    Assert!(ItemIdHasStorage(tup));
    size = ItemIdGetLength(tup) as Size;
    offset = ItemIdGetOffset(tup);

    if (offset as usize) < (*phdr).pd_upper as usize
        || (offset as Size + size) > (*phdr).pd_special as Size
        || offset != MAXALIGN(offset as usize) as c_uint
    {
        let _ = errcode(ERRCODE_DATA_CORRUPTED);
        ereport!(
            ERROR,
            errmsg!("corrupted line pointer: offset = {}, size = {}", offset, size)
        );
    }

    /* Amount of space to actually be deleted */
    size = MAXALIGN(size);

    /*
     * First, we want to get rid of the pd_linp entry for the index tuple. We
     * copy all subsequent linp's back one slot in the array. We don't use
     * PageGetItemId, because we are manipulating the _array_, not individual
     * linp's.
     */
    let linp = linp_base(page);
    nbytes = (*phdr).pd_lower as c_int
        - (linp.add(offidx as usize + 1) as *const u8 as isize - phdr as *const u8 as isize) as c_int;

    if nbytes > 0 {
        memmove(
            linp.add(offidx as usize) as *mut c_void,
            linp.add(offidx as usize + 1) as *const c_void,
            nbytes as usize,
        );
    }

    /*
     * Now move everything between the old upper bound (beginning of tuple
     * space) and the beginning of the deleted tuple forward, so that space in
     * the middle of the page is left free.  If we've just deleted the tuple
     * at the beginning of tuple space, then there's no need to do the copy.
     */

    /* beginning of tuple space */
    addr = page.add((*phdr).pd_upper as usize);

    if offset as usize > (*phdr).pd_upper as usize {
        memmove(
            addr.add(size) as *mut c_void,
            addr as *const c_void,
            offset as usize - (*phdr).pd_upper as usize,
        );
    }

    /* adjust free space boundary pointers */
    (*phdr).pd_upper += size as LocationIndex;
    (*phdr).pd_lower -= size_of::<ItemIdData>() as LocationIndex;

    /*
     * Finally, we need to adjust the linp entries that remain.
     *
     * Anything that used to be before the deleted tuple's data was moved
     * forward by the size of the deleted tuple.
     */
    if !PageIsEmpty(page) {
        let mut i: c_int;

        nline -= 1; /* there's one less than when we started */
        i = 1;
        while i <= nline {
            let ii: ItemId = PageGetItemId(page, i as OffsetNumber);

            Assert!(ItemIdHasStorage(ii));
            if (ItemIdGetOffset(ii) as usize) <= offset as usize {
                ItemIdSetOffset(ii, ItemIdGetOffset(ii) + size as u32);
            }
            i += 1;
        }
    }
}

/*
 * PageIndexMultiDelete
 *
 * This routine handles the case of deleting multiple tuples from an
 * index page at once.  It is considerably faster than a loop around
 * PageIndexTupleDelete ... however, the caller *must* supply the array
 * of item numbers to be deleted in item number order!
 */
pub unsafe fn PageIndexMultiDelete(page: Page, itemnos: *mut OffsetNumber, nitems: c_int) {
    let phdr: PageHeader = page as PageHeader;
    let pd_lower: Offset = (*phdr).pd_lower as Offset;
    let pd_upper: Offset = (*phdr).pd_upper as Offset;
    let pd_special: Offset = (*phdr).pd_special as Offset;
    let mut last_offset: Offset;
    let mut itemidbase: [itemIdCompactData; MaxIndexTuplesPerPage] =
        [itemIdCompactData::default(); MaxIndexTuplesPerPage];
    let mut newitemids: [ItemIdData; MaxIndexTuplesPerPage] =
        [ItemIdData::default(); MaxIndexTuplesPerPage];
    let mut itemidptr: itemIdCompact;
    let mut lp: ItemId;
    let nline: c_int;
    let mut nused: c_int;
    let mut totallen: Size;
    let mut size: Size;
    let mut offset: c_uint;
    let mut nextitm: c_int;
    let mut offnum: OffsetNumber;
    let mut presorted = true; /* For now */
    let mut nitems = nitems;

    Assert!(nitems <= MaxIndexTuplesPerPage as c_int);

    /*
     * If there aren't very many items to delete, then retail
     * PageIndexTupleDelete is the best way.  Delete the items in reverse
     * order so we don't have to think about adjusting item numbers for
     * previous deletions.
     *
     * TODO: tune the magic number here
     */
    if nitems <= 2 {
        nitems -= 1;
        while nitems >= 0 {
            PageIndexTupleDelete(page, *itemnos.add(nitems as usize));
            nitems -= 1;
        }
        return;
    }

    /*
     * As with PageRepairFragmentation, paranoia seems justified.
     */
    if pd_lower < SizeOfPageHeaderData as Offset
        || pd_lower > pd_upper
        || pd_upper > pd_special
        || pd_special > BLCKSZ as Offset
        || pd_special != MAXALIGN(pd_special as usize) as Offset
    {
        let _ = errcode(ERRCODE_DATA_CORRUPTED);
        ereport!(
            ERROR,
            errmsg!(
                "corrupted page pointers: lower = {}, upper = {}, special = {}",
                pd_lower,
                pd_upper,
                pd_special
            )
        );
    }

    /*
     * Scan the line pointer array and build a list of just the ones we are
     * going to keep.  Notice we do not modify the page yet, since we are
     * still validity-checking.
     */
    nline = PageGetMaxOffsetNumber(page) as c_int;
    itemidptr = itemidbase.as_mut_ptr();
    totallen = 0;
    nused = 0;
    nextitm = 0;
    last_offset = pd_special;
    offnum = FirstOffsetNumber;
    while offnum as c_int <= nline {
        lp = PageGetItemId(page, offnum);
        Assert!(ItemIdHasStorage(lp));
        size = ItemIdGetLength(lp) as Size;
        offset = ItemIdGetOffset(lp);
        if (offset as Offset) < pd_upper
            || (offset as Size + size) > pd_special as Size
            || offset != MAXALIGN(offset as usize) as c_uint
        {
            let _ = errcode(ERRCODE_DATA_CORRUPTED);
            ereport!(
                ERROR,
                errmsg!("corrupted line pointer: offset = {}, size = {}", offset, size)
            );
        }

        if nextitm < nitems && offnum == *itemnos.add(nextitm as usize) {
            /* skip item to be deleted */
            nextitm += 1;
        } else {
            (*itemidptr).offsetindex = nused as uint16; /* where it will go */
            (*itemidptr).itemoff = offset as int16;

            if last_offset > (*itemidptr).itemoff as Offset {
                last_offset = (*itemidptr).itemoff as Offset;
            } else {
                presorted = false;
            }

            (*itemidptr).alignedlen = MAXALIGN(size) as uint16;
            totallen += (*itemidptr).alignedlen as Size;
            newitemids[nused as usize] = *lp;
            itemidptr = itemidptr.add(1);
            nused += 1;
        }
        offnum = OffsetNumberNext(offnum);
    }

    /* this will catch invalid or out-of-order itemnos[] */
    if nextitm != nitems {
        elog!(ERROR, "incorrect index offsets supplied");
    }

    if totallen > (pd_special - pd_lower) as Size {
        let _ = errcode(ERRCODE_DATA_CORRUPTED);
        ereport!(
            ERROR,
            errmsg!(
                "corrupted item lengths: total {}, available space {}",
                totallen,
                pd_special - pd_lower
            )
        );
    }

    /*
     * Looks good. Overwrite the line pointers with the copy, from which we've
     * removed all the unused items.
     */
    memcpy(
        linp_base(page) as *mut c_void,
        newitemids.as_ptr() as *const c_void,
        nused as usize * size_of::<ItemIdData>(),
    );
    (*phdr).pd_lower =
        (SizeOfPageHeaderData + nused as usize * size_of::<ItemIdData>()) as LocationIndex;

    /* and compactify the tuple data */
    if nused > 0 {
        compactify_tuples(itemidbase.as_mut_ptr(), nused, page, presorted);
    } else {
        (*phdr).pd_upper = pd_special as LocationIndex;
    }
}

/*
 * PageIndexTupleDeleteNoCompact
 *
 * Remove the specified tuple from an index page, but set its line pointer
 * to "unused" instead of compacting it out, except that it can be removed
 * if it's the last line pointer on the page.
 *
 * This is used for index AMs that require that existing TIDs of live tuples
 * remain unchanged, and are willing to allow unused line pointers instead.
 */
pub unsafe fn PageIndexTupleDeleteNoCompact(page: Page, offnum: OffsetNumber) {
    let phdr: PageHeader = page as PageHeader;
    let addr: *mut c_char;
    let tup: ItemId;
    let mut size: Size;
    let offset: c_uint;
    let mut nline: c_int;

    /*
     * As with PageRepairFragmentation, paranoia seems justified.
     */
    if ((*phdr).pd_lower as usize) < SizeOfPageHeaderData
        || (*phdr).pd_lower > (*phdr).pd_upper
        || (*phdr).pd_upper > (*phdr).pd_special
        || ((*phdr).pd_special as usize) > BLCKSZ
        || (*phdr).pd_special as usize != MAXALIGN((*phdr).pd_special as usize)
    {
        let _ = errcode(ERRCODE_DATA_CORRUPTED);
        ereport!(
            ERROR,
            errmsg!(
                "corrupted page pointers: lower = {}, upper = {}, special = {}",
                (*phdr).pd_lower,
                (*phdr).pd_upper,
                (*phdr).pd_special
            )
        );
    }

    nline = PageGetMaxOffsetNumber(page) as c_int;
    if (offnum as c_int) <= 0 || offnum as c_int > nline {
        elog!(ERROR, "invalid index offnum: {}", offnum);
    }

    tup = PageGetItemId(page, offnum);
    Assert!(ItemIdHasStorage(tup));
    size = ItemIdGetLength(tup) as Size;
    offset = ItemIdGetOffset(tup);

    if (offset as usize) < (*phdr).pd_upper as usize
        || (offset as Size + size) > (*phdr).pd_special as Size
        || offset != MAXALIGN(offset as usize) as c_uint
    {
        let _ = errcode(ERRCODE_DATA_CORRUPTED);
        ereport!(
            ERROR,
            errmsg!("corrupted line pointer: offset = {}, size = {}", offset, size)
        );
    }

    /* Amount of space to actually be deleted */
    size = MAXALIGN(size);

    /*
     * Either set the line pointer to "unused", or zap it if it's the last
     * one.  (Note: it's possible that the next-to-last one(s) are already
     * unused, but we do not trouble to try to compact them out if so.)
     */
    if (offnum as c_int) < nline {
        ItemIdSetUnused(tup);
    } else {
        (*phdr).pd_lower -= size_of::<ItemIdData>() as LocationIndex;
        nline -= 1; /* there's one less than when we started */
    }

    /*
     * Now move everything between the old upper bound (beginning of tuple
     * space) and the beginning of the deleted tuple forward, so that space in
     * the middle of the page is left free.  If we've just deleted the tuple
     * at the beginning of tuple space, then there's no need to do the copy.
     */

    /* beginning of tuple space */
    addr = page.add((*phdr).pd_upper as usize);

    if offset as usize > (*phdr).pd_upper as usize {
        memmove(
            addr.add(size) as *mut c_void,
            addr as *const c_void,
            offset as usize - (*phdr).pd_upper as usize,
        );
    }

    /* adjust free space boundary pointer */
    (*phdr).pd_upper += size as LocationIndex;

    /*
     * Finally, we need to adjust the linp entries that remain.
     *
     * Anything that used to be before the deleted tuple's data was moved
     * forward by the size of the deleted tuple.
     */
    if !PageIsEmpty(page) {
        let mut i: c_int;

        i = 1;
        while i <= nline {
            let ii: ItemId = PageGetItemId(page, i as OffsetNumber);

            if ItemIdHasStorage(ii) && (ItemIdGetOffset(ii) as usize) <= offset as usize {
                ItemIdSetOffset(ii, ItemIdGetOffset(ii) + size as u32);
            }
            i += 1;
        }
    }
}

/*
 * PageIndexTupleOverwrite
 *
 * Replace a specified tuple on an index page.
 *
 * The new tuple is placed exactly where the old one had been, shifting
 * other tuples' data up or down as needed to keep the page compacted.
 * This is better than deleting and reinserting the tuple, because it
 * avoids any data shifting when the tuple size doesn't change; and
 * even when it does, we avoid moving the line pointers around.
 * This could be used by an index AM that doesn't want to unset the
 * LP_DEAD bit when it happens to be set.  It could conceivably also be
 * used by an index AM that cares about the physical order of tuples as
 * well as their logical/ItemId order.
 *
 * If there's insufficient space for the new tuple, return false.  Other
 * errors represent data-corruption problems, so we just elog.
 */
pub unsafe fn PageIndexTupleOverwrite(
    page: Page,
    offnum: OffsetNumber,
    newtup: Item,
    newsize: Size,
) -> bool {
    let phdr: PageHeader = page as PageHeader;
    let tupid: ItemId;
    let mut oldsize: c_int;
    let offset: c_uint;
    let alignednewsize: Size;
    let size_diff: c_int;
    let itemcount: c_int;

    /*
     * As with PageRepairFragmentation, paranoia seems justified.
     */
    if ((*phdr).pd_lower as usize) < SizeOfPageHeaderData
        || (*phdr).pd_lower > (*phdr).pd_upper
        || (*phdr).pd_upper > (*phdr).pd_special
        || ((*phdr).pd_special as usize) > BLCKSZ
        || (*phdr).pd_special as usize != MAXALIGN((*phdr).pd_special as usize)
    {
        let _ = errcode(ERRCODE_DATA_CORRUPTED);
        ereport!(
            ERROR,
            errmsg!(
                "corrupted page pointers: lower = {}, upper = {}, special = {}",
                (*phdr).pd_lower,
                (*phdr).pd_upper,
                (*phdr).pd_special
            )
        );
    }

    itemcount = PageGetMaxOffsetNumber(page) as c_int;
    if (offnum as c_int) <= 0 || offnum as c_int > itemcount {
        elog!(ERROR, "invalid index offnum: {}", offnum);
    }

    tupid = PageGetItemId(page, offnum);
    Assert!(ItemIdHasStorage(tupid));
    oldsize = ItemIdGetLength(tupid) as c_int;
    offset = ItemIdGetOffset(tupid);

    if (offset as c_int) < (*phdr).pd_upper as c_int
        || (offset as c_int + oldsize) > (*phdr).pd_special as c_int
        || offset != MAXALIGN(offset as usize) as c_uint
    {
        let _ = errcode(ERRCODE_DATA_CORRUPTED);
        ereport!(
            ERROR,
            errmsg!("corrupted line pointer: offset = {}, size = {}", offset, oldsize)
        );
    }

    /*
     * Determine actual change in space requirement, check for page overflow.
     */
    oldsize = MAXALIGN(oldsize as usize) as c_int;
    alignednewsize = MAXALIGN(newsize);
    if alignednewsize > (oldsize + ((*phdr).pd_upper as c_int - (*phdr).pd_lower as c_int)) as Size {
        return false;
    }

    /*
     * Relocate existing data and update line pointers, unless the new tuple
     * is the same size as the old (after alignment), in which case there's
     * nothing to do.  Notice that what we have to relocate is data before the
     * target tuple, not data after, so it's convenient to express size_diff
     * as the amount by which the tuple's size is decreasing, making it the
     * delta to add to pd_upper and affected line pointers.
     */
    size_diff = oldsize - alignednewsize as c_int;
    if size_diff != 0 {
        let addr = page.add((*phdr).pd_upper as usize);
        let mut i: c_int;

        /* relocate all tuple data before the target tuple */
        memmove(
            addr.offset(size_diff as isize) as *mut c_void,
            addr as *const c_void,
            offset as usize - (*phdr).pd_upper as usize,
        );

        /* adjust free space boundary pointer */
        (*phdr).pd_upper = ((*phdr).pd_upper as c_int + size_diff) as LocationIndex;

        /* adjust affected line pointers too */
        i = FirstOffsetNumber as c_int;
        while i <= itemcount {
            let ii: ItemId = PageGetItemId(page, i as OffsetNumber);

            /* Allow items without storage; currently only BRIN needs that */
            if ItemIdHasStorage(ii) && (ItemIdGetOffset(ii) as usize) <= offset as usize {
                ItemIdSetOffset(ii, (ItemIdGetOffset(ii) as c_int + size_diff) as u32);
            }
            i += 1;
        }
    }

    /* Update the item's tuple length without changing its lp_flags field */
    ItemIdSetNormal(tupid, (offset as c_int + size_diff) as u32, newsize as u32);

    /* Copy new tuple data onto page */
    memcpy(
        PageGetItem(page, tupid) as *mut c_void,
        newtup as *const c_void,
        newsize,
    );

    true
}

/*
 * Set checksum for a page in shared buffers.
 *
 * If checksums are disabled, or if the page is not initialized, just return
 * the input.  Otherwise, we must make a copy of the page before calculating
 * the checksum, to prevent concurrent modifications (e.g. setting hint bits)
 * from making the final checksum invalid.  It doesn't matter if we include or
 * exclude hints during the copy, as long as we write a valid page and
 * associated checksum.
 *
 * Returns a pointer to the block-sized data that needs to be written. Uses
 * statically-allocated memory, so the caller must immediately write the
 * returned page and not refer to it again.
 */
pub unsafe fn PageSetChecksumCopy(page: Page, blkno: BlockNumber) -> *mut c_char {
    static mut pageCopy: *mut c_char = core::ptr::null_mut();

    /* If we don't need a checksum, just return the passed-in data */
    if PageIsNew(page) || !DataChecksumsEnabled() {
        return page;
    }

    /*
     * We allocate the copy space once and use it over on each subsequent
     * call.  The point of palloc'ing here, rather than having a static char
     * array, is first to ensure adequate alignment for the checksumming code
     * and second to avoid wasting space in processes that never call this.
     */
    if pageCopy.is_null() {
        pageCopy = MemoryContextAllocAligned(TopMemoryContext, BLCKSZ, PG_IO_ALIGN_SIZE, 0) as *mut c_char;
    }

    memcpy(pageCopy as *mut c_void, page as *const c_void, BLCKSZ);
    (*(pageCopy as PageHeader)).pd_checksum = crate::storage::checksum::pg_checksum_page(pageCopy, blkno);
    pageCopy
}

/*
 * Set checksum for a page in private memory.
 *
 * This must only be used when we know that no other process can be modifying
 * the page buffer.
 */
pub unsafe fn PageSetChecksumInplace(page: Page, blkno: BlockNumber) {
    /* If we don't need a checksum, just return */
    if PageIsNew(page) || !DataChecksumsEnabled() {
        return;
    }

    (*(page as PageHeader)).pd_checksum = crate::storage::checksum::pg_checksum_page(page, blkno);
}

#[cfg(test)]
mod tests {
    use super::*;

    /* A MAXALIGN'd BLCKSZ page buffer. */
    fn new_page() -> Box<PGAlignedBlock> {
        Box::new(PGAlignedBlock { data: [0; BLCKSZ] })
    }

    #[test]
    fn init_add_and_free_space() {
        unsafe {
            let mut buf = new_page();
            let page = buf.data.as_mut_ptr();
            PageInit(page, BLCKSZ, 0);
            assert!(PageIsEmpty(page) && !PageIsNew(page));
            assert_eq!(PageGetMaxOffsetNumber(page), 0);

            let a: [u8; 8] = [1, 2, 3, 4, 5, 6, 7, 8];
            let b: [u8; 8] = [9, 10, 11, 12, 13, 14, 15, 16];
            let o1 = PageAddItemExtended(page, a.as_ptr() as Item, 8, InvalidOffsetNumber, 0);
            let o2 = PageAddItemExtended(page, b.as_ptr() as Item, 8, InvalidOffsetNumber, 0);
            assert_eq!(o1, 1);
            assert_eq!(o2, 2);
            assert_eq!(PageGetMaxOffsetNumber(page), 2);

            let it1 = PageGetItem(page, PageGetItemId(page, 1));
            let s1 = core::slice::from_raw_parts(it1 as *const u8, 8);
            assert_eq!(s1, &a);
        }
    }

    #[test]
    fn index_tuple_delete_compacts() {
        unsafe {
            let mut buf = new_page();
            let page = buf.data.as_mut_ptr();
            PageInit(page, BLCKSZ, 0);
            let a: [u8; 8] = [1; 8];
            let b: [u8; 8] = [2; 8];
            let c: [u8; 8] = [3; 8];
            PageAddItemExtended(page, a.as_ptr() as Item, 8, InvalidOffsetNumber, 0);
            PageAddItemExtended(page, b.as_ptr() as Item, 8, InvalidOffsetNumber, 0);
            PageAddItemExtended(page, c.as_ptr() as Item, 8, InvalidOffsetNumber, 0);
            assert_eq!(PageGetMaxOffsetNumber(page), 3);

            PageIndexTupleDelete(page, 2);
            assert_eq!(PageGetMaxOffsetNumber(page), 2);
            let i1 = core::slice::from_raw_parts(PageGetItem(page, PageGetItemId(page, 1)) as *const u8, 8);
            let i2 = core::slice::from_raw_parts(PageGetItem(page, PageGetItemId(page, 2)) as *const u8, 8);
            assert_eq!(i1, &a);
            assert_eq!(i2, &c);
        }
    }
}
