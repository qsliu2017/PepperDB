//! Translation of postgres/src/backend/storage/page/bufpage.c
//! (merged with the struct/macro layer of postgres/src/include/storage/bufpage.h).
//!
//! The buffer-page access layer: it operates purely on an in-memory BLCKSZ page
//! buffer (no disk I/O).  A page is laid out as
//!   [ PageHeaderData ][ line-pointer array (grows up) ] ... free ... [ tuples (grow down) ][ special ]
//!
//! #include "access/htup_details.h" -> crate::access::htup_details (MaxHeapTuplesPerPage)
//! #include "access/itup.h"         -> (MaxIndexTuplesPerPage: used only by the stubbed
//!                                       PageIndexMultiDelete)
//! #include "storage/checksum.h"    -> crate::storage::checksum
//! #include "access/xlog.h" / pgstat.h / utils/memutils.h -> the WAL/stat bits are not needed
//!   here (PageGetLSN/SetLSN are pure pd_lsn access).
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::prelude::*;
use crate::access::htup_details::MaxHeapTuplesPerPage;
use crate::storage::itemid::ItemIdSetNormal;
const MaxIndexTuplesPerPage: usize = 1358; /* (BLCKSZ - SizeOfPageHeaderData) / (sizeof(ItemIdData)+sizeof(IndexTupleData)); access/itup.h */
use crate::access::transam::{InvalidTransactionId, TransactionIdIsValid};
use crate::c::{int16, uint16, uint32, MAXALIGN, MemSet};
use crate::pg_config::BLCKSZ;
use crate::storage::block::BlockNumber;
use crate::storage::itemid::*;
use crate::storage::off::{
    FirstOffsetNumber, InvalidOffsetNumber, OffsetNumber, OffsetNumberIsValid, OffsetNumberNext,
};
use core::ffi::{c_char, c_int, c_void};
use core::mem::{offset_of, size_of};

extern "C" {
    fn memcpy(dst: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
    fn memmove(dst: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
}

/* A 64-bit LSN; see PageXLogRecPtr below. (xlogdefs.h XLogRecPtr) */
pub type XLogRecPtr = u64;
/* A pointer to a page's item data (utils Pointer). */
pub type Item = *mut c_char;
/* A page is just a pointer to a BLCKSZ buffer. */
pub type Page = *mut c_char;
pub type LocationIndex = uint16;

/*
 * For historical reasons, the 64-bit LSN value is stored as two 32-bit values.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct PageXLogRecPtr {
    pub xlogid: uint32,  /* high bits */
    pub xrecoff: uint32, /* low bits */
}

#[inline]
pub fn PageXLogRecPtrGet(val: PageXLogRecPtr) -> XLogRecPtr {
    ((val.xlogid as u64) << 32) | val.xrecoff as u64
}
#[inline]
pub fn PageXLogRecPtrSet(ptr: &mut PageXLogRecPtr, lsn: XLogRecPtr) {
    ptr.xlogid = (lsn >> 32) as uint32;
    ptr.xrecoff = lsn as uint32;
}

/*
 * Space-management information generic to any page (the page header).
 */
#[repr(C)]
pub struct PageHeaderData {
    pub pd_lsn: PageXLogRecPtr,         /* xlog of last change to this page */
    pub pd_checksum: uint16,            /* checksum */
    pub pd_flags: uint16,               /* flag bits, see below */
    pub pd_lower: LocationIndex,        /* offset to start of free space */
    pub pd_upper: LocationIndex,        /* offset to end of free space */
    pub pd_special: LocationIndex,      /* offset to start of special space */
    pub pd_pagesize_version: uint16,    /* page size + layout version */
    pub pd_prune_xid: TransactionId,    /* oldest prunable XID, or zero */
    pub pd_linp: [ItemIdData; 0],       /* line pointer array (FLEXIBLE) */
}
pub type PageHeader = *mut PageHeaderData;

/* pd_flags bits. */
pub const PD_HAS_FREE_LINES: uint16 = 0x0001; /* any unused line pointers? */
pub const PD_PAGE_FULL: uint16 = 0x0002; /* not enough free space for new tuple? */
pub const PD_ALL_VISIBLE: uint16 = 0x0004; /* all tuples visible to everyone */
pub const PD_VALID_FLAG_BITS: uint16 = 0x0007; /* OR of all valid pd_flags bits */

pub const PG_PAGE_LAYOUT_VERSION: uint16 = 4;
pub const PG_DATA_CHECKSUM_VERSION: uint16 = 1;

/* line pointer(s) do not count as part of header */
pub const SizeOfPageHeaderData: usize = offset_of!(PageHeaderData, pd_linp);

/* PageAddItemExtended flags */
pub const PAI_OVERWRITE: c_int = 1 << 0;
pub const PAI_IS_HEAP: c_int = 1 << 1;
/* PageIsVerified flags */
pub const PIV_LOG_WARNING: c_int = 1 << 0;
pub const PIV_LOG_LOG: c_int = 1 << 1;
pub const PIV_IGNORE_CHECKSUM_FAILURE: c_int = 1 << 2;

/* errcodes.h (the errcode() shim ignores the value). */
const ERRCODE_DATA_CORRUPTED: c_int = 0;

// ----------------------------------------------------------------------------
//   inline page-accessor functions (bufpage.h)
// ----------------------------------------------------------------------------

#[inline]
unsafe fn hdr(page: *const c_char) -> *mut PageHeaderData {
    page as *mut PageHeaderData
}
/* base of the line-pointer array. */
#[inline]
unsafe fn linp_base(page: *const c_char) -> *mut ItemIdData {
    (page as *mut u8).add(offset_of!(PageHeaderData, pd_linp)) as *mut ItemIdData
}

#[inline]
pub unsafe fn PageIsEmpty(page: *const c_char) -> bool {
    (*hdr(page)).pd_lower as usize <= SizeOfPageHeaderData
}
#[inline]
pub unsafe fn PageIsNew(page: *const c_char) -> bool {
    (*hdr(page)).pd_upper == 0
}
#[inline]
pub unsafe fn PageGetItemId(page: Page, offsetNumber: OffsetNumber) -> ItemId {
    linp_base(page).add((offsetNumber - 1) as usize)
}
#[inline]
pub unsafe fn PageGetContents(page: Page) -> *mut c_char {
    page.add(MAXALIGN(SizeOfPageHeaderData))
}
#[inline]
pub unsafe fn PageGetPageSize(page: *const c_char) -> Size {
    ((*hdr(page)).pd_pagesize_version & 0xFF00u16) as Size
}
#[inline]
pub unsafe fn PageGetPageLayoutVersion(page: *const c_char) -> u8 {
    ((*hdr(page)).pd_pagesize_version & 0x00FF) as u8
}
#[inline]
pub unsafe fn PageSetPageSizeAndVersion(page: Page, size: Size, version: u8) {
    Assert!((size & 0xFF00) == size);
    (*hdr(page)).pd_pagesize_version = size as uint16 | version as uint16;
}
#[inline]
pub unsafe fn PageGetSpecialSize(page: *const c_char) -> uint16 {
    (PageGetPageSize(page) as uint16) - (*hdr(page)).pd_special
}
#[inline]
pub unsafe fn PageGetSpecialPointer(page: Page) -> *mut c_char {
    page.add((*hdr(page)).pd_special as usize)
}
#[inline]
pub unsafe fn PageGetItem(page: *const c_char, itemId: ItemId) -> Item {
    Assert!(ItemIdHasStorage(itemId));
    (page as *mut c_char).add(ItemIdGetOffset(itemId) as usize)
}
#[inline]
pub unsafe fn PageGetMaxOffsetNumber(page: *const c_char) -> OffsetNumber {
    let lower = (*hdr(page)).pd_lower as usize;
    if lower <= SizeOfPageHeaderData {
        0
    } else {
        ((lower - SizeOfPageHeaderData) / size_of::<ItemIdData>()) as OffsetNumber
    }
}
#[inline]
pub unsafe fn PageGetLSN(page: *const c_char) -> XLogRecPtr {
    PageXLogRecPtrGet((*hdr(page)).pd_lsn)
}
#[inline]
pub unsafe fn PageSetLSN(page: Page, lsn: XLogRecPtr) {
    PageXLogRecPtrSet(&mut (*hdr(page)).pd_lsn, lsn);
}
#[inline]
pub unsafe fn PageHasFreeLinePointers(page: *const c_char) -> bool {
    ((*hdr(page)).pd_flags & PD_HAS_FREE_LINES) != 0
}
#[inline]
pub unsafe fn PageSetHasFreeLinePointers(page: Page) {
    (*hdr(page)).pd_flags |= PD_HAS_FREE_LINES;
}
#[inline]
pub unsafe fn PageClearHasFreeLinePointers(page: Page) {
    (*hdr(page)).pd_flags &= !PD_HAS_FREE_LINES;
}
#[inline]
pub unsafe fn PageIsFull(page: *const c_char) -> bool {
    ((*hdr(page)).pd_flags & PD_PAGE_FULL) != 0
}
#[inline]
pub unsafe fn PageSetFull(page: Page) {
    (*hdr(page)).pd_flags |= PD_PAGE_FULL;
}
#[inline]
pub unsafe fn PageClearFull(page: Page) {
    (*hdr(page)).pd_flags &= !PD_PAGE_FULL;
}
#[inline]
pub unsafe fn PageIsAllVisible(page: *const c_char) -> bool {
    ((*hdr(page)).pd_flags & PD_ALL_VISIBLE) != 0
}
#[inline]
pub unsafe fn PageSetAllVisible(page: Page) {
    (*hdr(page)).pd_flags |= PD_ALL_VISIBLE;
}
#[inline]
pub unsafe fn PageClearAllVisible(page: Page) {
    (*hdr(page)).pd_flags &= !PD_ALL_VISIBLE;
}
#[inline]
pub unsafe fn PageClearPrunable(page: Page) {
    (*hdr(page)).pd_prune_xid = InvalidTransactionId;
}

// ----------------------------------------------------------------------------
//   local helpers for unported deps
// ----------------------------------------------------------------------------

/* GUC: whether data-page checksums are enabled.  TODO(pg-port): real GUC. */
#[inline]
fn DataChecksumsEnabled() -> bool {
    false
}
/* GUC ignore_checksum_failure. */
static ignore_checksum_failure: bool = false;
/* memdebug.h valgrind hook -- no-op. */
#[inline]
unsafe fn VALGRIND_CHECK_MEM_IS_DEFINED(_p: *const c_void, _n: usize) {}

/* Is the whole [ptr, ptr+len) region zero bytes? */
unsafe fn pg_memory_is_all_zeros(ptr: *const c_void, len: usize) -> bool {
    let bytes = core::slice::from_raw_parts(ptr as *const u8, len);
    bytes.iter().all(|&b| b == 0)
}

// ----------------------------------------------------------------------------
//   page support functions (bufpage.c)
// ----------------------------------------------------------------------------

/*
 * PageInit - initialize the contents of a page.  No initial checksum is
 * computed; that's done when the page is written.
 */
pub unsafe fn PageInit(page: Page, pageSize: Size, specialSize: Size) {
    let p = hdr(page);
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
    /* pd_prune_xid = InvalidTransactionId done by the MemSet above */
}

/*
 * PageIsVerified - check that the page header and checksum (if any) look valid.
 * Allows all-zero pages.  Logs per PIV_LOG_* on checksum failure.
 */
pub unsafe fn PageIsVerified(
    page: *mut c_char,
    blkno: BlockNumber,
    flags: c_int,
    checksum_failure_p: *mut bool,
) -> bool {
    let p = hdr(page);
    let mut checksum_failure = false;
    let mut header_sane = false;
    let mut checksum: uint16 = 0;

    if !checksum_failure_p.is_null() {
        *checksum_failure_p = false;
    }

    /* Don't verify page data unless the page passes the basic non-zero test */
    if !PageIsNew(page) {
        if DataChecksumsEnabled() {
            checksum = crate::storage::checksum::pg_checksum_page(page, blkno);
            if checksum != (*p).pd_checksum {
                checksum_failure = true;
                if !checksum_failure_p.is_null() {
                    *checksum_failure_p = true;
                }
            }
        }

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

    if checksum_failure {
        if (flags & (PIV_LOG_WARNING | PIV_LOG_LOG)) != 0 {
            let _ = errcode(ERRCODE_DATA_CORRUPTED);
            // C logs at WARNING or LOG (not ERROR) - non-fatal; render as a note.
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
 * PageAddItemExtended - add an item to a page; returns the offset it went to,
 * or InvalidOffsetNumber on refusal (with a WARNING).  EREPORT(ERROR) is
 * disallowed here (corruption uses PANIC).
 */
pub unsafe fn PageAddItemExtended(
    page: Page,
    item: Item,
    size: Size,
    mut offsetNumber: OffsetNumber,
    flags: c_int,
) -> OffsetNumber {
    let phdr = hdr(page);
    let mut needshuffle = false;

    /* Be wary about corrupted page pointers */
    if ((*phdr).pd_lower as usize) < SizeOfPageHeaderData
        || (*phdr).pd_lower > (*phdr).pd_upper
        || (*phdr).pd_upper > (*phdr).pd_special
        || ((*phdr).pd_special as usize) > BLCKSZ
    {
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

    /* Select offsetNumber to place the new item at */
    let limit = OffsetNumberNext(PageGetMaxOffsetNumber(page));

    if OffsetNumberIsValid(offsetNumber) {
        if (flags & PAI_OVERWRITE) != 0 {
            if offsetNumber < limit {
                let itemId = PageGetItemId(page, offsetNumber);
                if ItemIdIsUsed(itemId) || ItemIdHasStorage(itemId) {
                    elog!(WARNING, "will not overwrite a used ItemId");
                    return InvalidOffsetNumber;
                }
            }
        } else if offsetNumber < limit {
            needshuffle = true; /* need to move existing linp's */
        }
    } else {
        /* offsetNumber not passed in; find a free slot, else put it at limit */
        if PageHasFreeLinePointers(page) {
            offsetNumber = FirstOffsetNumber;
            while offsetNumber < limit {
                let itemId = PageGetItemId(page, offsetNumber);
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

    /* Compute new lower and upper pointers (signed, like the C). */
    let lower: c_int = if offsetNumber == limit || needshuffle {
        (*phdr).pd_lower as c_int + size_of::<ItemIdData>() as c_int
    } else {
        (*phdr).pd_lower as c_int
    };
    let alignedSize = MAXALIGN(size);
    let upper: c_int = (*phdr).pd_upper as c_int - alignedSize as c_int;

    if lower > upper {
        return InvalidOffsetNumber;
    }

    /* OK to insert.  First, shuffle the existing pointers if needed. */
    let itemId = PageGetItemId(page, offsetNumber);
    if needshuffle {
        memmove(
            itemId.add(1) as *mut c_void,
            itemId as *const c_void,
            (limit - offsetNumber) as usize * size_of::<ItemIdData>(),
        );
    }

    /* set the line pointer */
    ItemIdSetNormal(itemId, upper as u32, size as u32);

    VALGRIND_CHECK_MEM_IS_DEFINED(item as *const c_void, size);

    /* copy the item's data onto the page */
    memcpy(page.add(upper as usize) as *mut c_void, item as *const c_void, size);

    /* adjust page header */
    (*phdr).pd_lower = lower as LocationIndex;
    (*phdr).pd_upper = upper as LocationIndex;

    offsetNumber
}

/* PageAddItem - the common wrapper for PageAddItemExtended. */
#[inline]
pub unsafe fn PageAddItem(
    page: Page,
    item: Item,
    size: Size,
    offsetNumber: OffsetNumber,
    overwrite: bool,
    is_heap: bool,
) -> OffsetNumber {
    let flags = if overwrite { PAI_OVERWRITE } else { 0 } | if is_heap { PAI_IS_HEAP } else { 0 };
    PageAddItemExtended(page, item, size, offsetNumber, flags)
}

/*
 * PageGetTempPage - a fresh (uninitialized) temp page of the same size.
 */
pub unsafe fn PageGetTempPage(page: *const c_char) -> Page {
    let pageSize = PageGetPageSize(page);
    palloc(pageSize) as Page
}
/* Same, but copying the source page's contents. */
pub unsafe fn PageGetTempPageCopy(page: *const c_char) -> Page {
    let pageSize = PageGetPageSize(page);
    let temp = palloc(pageSize) as Page;
    memcpy(temp as *mut c_void, page as *const c_void, pageSize);
    temp
}
/* Same, PageInit'd with the source's special-space size + copied special space. */
pub unsafe fn PageGetTempPageCopySpecial(page: *const c_char) -> Page {
    let pageSize = PageGetPageSize(page);
    let temp = palloc(pageSize) as Page;
    PageInit(temp, pageSize, PageGetSpecialSize(page) as Size);
    memcpy(
        PageGetSpecialPointer(temp) as *mut c_void,
        PageGetSpecialPointer(page as *mut c_char) as *const c_void,
        PageGetSpecialSize(page) as usize,
    );
    temp
}
/* Copy a temp page back to the permanent page and free the temp. */
pub unsafe fn PageRestoreTempPage(tempPage: Page, oldPage: Page) {
    let pageSize = PageGetPageSize(tempPage);
    memcpy(oldPage as *mut c_void, tempPage as *const c_void, pageSize);
    pfree(tempPage as *mut c_void);
}

/* Tuple-defrag support for PageRepairFragmentation / PageIndexMultiDelete. */
#[derive(Clone, Copy, Default)]
struct itemIdCompactData {
    offsetindex: uint16, /* linp array index */
    itemoff: int16,      /* page offset of item data */
    alignedlen: uint16,  /* MAXALIGN(item data len) */
}

#[repr(C, align(8))]
struct PGAlignedBlock {
    data: [c_char; BLCKSZ],
}

/*
 * compactify_tuples - move tuples to remove gaps after some line pointers were
 * removed/marked unused, reordering them back into reverse line-pointer order.
 * `presorted` => itemidbase is sorted descending by itemoff (memmove fast path).
 */
unsafe fn compactify_tuples(
    itemidbase: *mut itemIdCompactData,
    nitems: c_int,
    page: Page,
    presorted: bool,
) {
    let phdr = hdr(page);
    let mut upper: c_int;
    let mut copy_tail: c_int;
    let mut copy_head: c_int;
    let mut itemidptr: *mut itemIdCompactData;
    let mut i: c_int;

    Assert!(nitems > 0);

    if presorted {
        /* itemidbase already optimally ordered: memmove towards page end. */
        upper = (*phdr).pd_special as c_int;
        i = 0;
        loop {
            itemidptr = itemidbase.add(i as usize);
            if upper != (*itemidptr).itemoff as c_int + (*itemidptr).alignedlen as c_int {
                break;
            }
            upper -= (*itemidptr).alignedlen as c_int;
            i += 1;
            if i >= nitems {
                break;
            }
        }

        copy_head = (*itemidptr).itemoff as c_int + (*itemidptr).alignedlen as c_int;
        copy_tail = copy_head;
        while i < nitems {
            itemidptr = itemidbase.add(i as usize);
            let lp = PageGetItemId(page, (*itemidptr).offsetindex + 1);

            if copy_head != (*itemidptr).itemoff as c_int + (*itemidptr).alignedlen as c_int {
                memmove(
                    page.add(upper as usize) as *mut c_void,
                    page.add(copy_head as usize) as *const c_void,
                    (copy_tail - copy_head) as usize,
                );
                copy_tail = (*itemidptr).itemoff as c_int + (*itemidptr).alignedlen as c_int;
            }
            upper -= (*itemidptr).alignedlen as c_int;
            copy_head = (*itemidptr).itemoff as c_int;
            ItemIdSetOffset(lp, upper as u32);
            i += 1;
        }
        memmove(
            page.add(upper as usize) as *mut c_void,
            page.add(copy_head as usize) as *const c_void,
            (copy_tail - copy_head) as usize,
        );
    } else {
        /* Non-presorted: copy movable tuples through a temp buffer. */
        let mut scratch = PGAlignedBlock { data: [0; BLCKSZ] };
        let scratchptr = scratch.data.as_mut_ptr();

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
                if i >= nitems {
                    break;
                }
            }
            i = 0;
            itemidptr = itemidbase;
            upper = (*phdr).pd_special as c_int;
        } else {
            upper = (*phdr).pd_special as c_int;
            i = 0;
            loop {
                itemidptr = itemidbase.add(i as usize);
                if upper != (*itemidptr).itemoff as c_int + (*itemidptr).alignedlen as c_int {
                    break;
                }
                upper -= (*itemidptr).alignedlen as c_int;
                i += 1;
                if i >= nitems {
                    break;
                }
            }
            memcpy(
                scratchptr.add((*phdr).pd_upper as usize) as *mut c_void,
                page.add((*phdr).pd_upper as usize) as *const c_void,
                (upper - (*phdr).pd_upper as c_int) as usize,
            );
        }

        copy_head = (*itemidptr).itemoff as c_int + (*itemidptr).alignedlen as c_int;
        copy_tail = copy_head;
        while i < nitems {
            itemidptr = itemidbase.add(i as usize);
            let lp = PageGetItemId(page, (*itemidptr).offsetindex + 1);

            if copy_head != (*itemidptr).itemoff as c_int + (*itemidptr).alignedlen as c_int {
                memcpy(
                    page.add(upper as usize) as *mut c_void,
                    scratchptr.add(copy_head as usize) as *const c_void,
                    (copy_tail - copy_head) as usize,
                );
                copy_tail = (*itemidptr).itemoff as c_int + (*itemidptr).alignedlen as c_int;
            }
            upper -= (*itemidptr).alignedlen as c_int;
            copy_head = (*itemidptr).itemoff as c_int;
            ItemIdSetOffset(lp, upper as u32);
            i += 1;
        }
        memcpy(
            page.add(upper as usize) as *mut c_void,
            scratchptr.add(copy_head as usize) as *const c_void,
            (copy_tail - copy_head) as usize,
        );
    }

    (*phdr).pd_upper = upper as LocationIndex;
}

/*
 * PageRepairFragmentation - free fragmented space on a heap page after pruning,
 * and truncate trailing unused line pointers.
 */
pub unsafe fn PageRepairFragmentation(page: Page) {
    let pd_lower = (*hdr(page)).pd_lower as c_int;
    let pd_upper = (*hdr(page)).pd_upper as c_int;
    let pd_special = (*hdr(page)).pd_special as c_int;
    let mut last_offset: c_int;
    let mut itemidbase: [itemIdCompactData; MaxHeapTuplesPerPage as usize] =
        [itemIdCompactData::default(); MaxHeapTuplesPerPage as usize];
    let mut nidx: usize = 0;
    let nline: c_int;
    let nstorage: c_int;
    let mut nunused: c_int;
    let mut finalusedlp: OffsetNumber = InvalidOffsetNumber;
    let mut totallen: Size;
    let mut presorted = true;

    if pd_lower < SizeOfPageHeaderData as c_int
        || pd_lower > pd_upper
        || pd_upper > pd_special
        || pd_special > BLCKSZ as c_int
        || pd_special != MAXALIGN(pd_special as usize) as c_int
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

    nline = PageGetMaxOffsetNumber(page) as c_int;
    nunused = 0;
    totallen = 0;
    last_offset = pd_special;
    let mut i: c_int = FirstOffsetNumber as c_int;
    while i <= nline {
        let lp = PageGetItemId(page, i as OffsetNumber);
        if ItemIdIsUsed(lp) {
            if ItemIdHasStorage(lp) {
                let e = &mut itemidbase[nidx];
                e.offsetindex = (i - 1) as uint16;
                e.itemoff = ItemIdGetOffset(lp) as int16;

                if last_offset > e.itemoff as c_int {
                    last_offset = e.itemoff as c_int;
                } else {
                    presorted = false;
                }

                if (e.itemoff as c_int) < pd_upper || (e.itemoff as c_int) >= pd_special {
                    let _ = errcode(ERRCODE_DATA_CORRUPTED);
                    ereport!(ERROR, errmsg!("corrupted line pointer: {}", e.itemoff));
                }
                e.alignedlen = MAXALIGN(ItemIdGetLength(lp) as usize) as uint16;
                totallen += e.alignedlen as Size;
                nidx += 1;
            }
            finalusedlp = i as OffsetNumber; /* could be the final used item */
        } else {
            Assert!(!ItemIdHasStorage(lp));
            ItemIdSetUnused(lp);
            nunused += 1;
        }
        i += 1;
    }

    nstorage = nidx as c_int;
    if nstorage == 0 {
        /* Page is completely empty, so just reset it quickly */
        (*hdr(page)).pd_upper = pd_special as LocationIndex;
    } else {
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
        let nunusedend = nline - finalusedlp as c_int;
        nunused -= nunusedend;
        (*hdr(page)).pd_lower -= (size_of::<ItemIdData>() * nunusedend as usize) as LocationIndex;
    }

    if nunused > 0 {
        PageSetHasFreeLinePointers(page);
    } else {
        PageClearHasFreeLinePointers(page);
    }
}

/*
 * PageTruncateLinePointerArray - remove unused line pointers from the end of the
 * line-pointer array (heap pages only), keeping at least one if necessary.
 */
pub unsafe fn PageTruncateLinePointerArray(page: Page) {
    let phdr = hdr(page);
    let mut countdone = false;
    let mut sethint = false;
    let mut nunusedend: c_int = 0;

    let mut i: c_int = PageGetMaxOffsetNumber(page) as c_int;
    while i >= FirstOffsetNumber as c_int {
        let lp = PageGetItemId(page, i as OffsetNumber);
        if !countdone && i > FirstOffsetNumber as c_int {
            if !ItemIdIsUsed(lp) {
                nunusedend += 1;
            } else {
                countdone = true;
            }
        } else if !ItemIdIsUsed(lp) {
            sethint = true;
            break;
        }
        i -= 1;
    }

    if nunusedend > 0 {
        (*phdr).pd_lower -= (size_of::<ItemIdData>() * nunusedend as usize) as LocationIndex;
    } else {
        Assert!(sethint);
    }

    if sethint {
        PageSetHasFreeLinePointers(page);
    } else {
        PageClearHasFreeLinePointers(page);
    }
}

/*
 * PageGetFreeSpace - free allocatable space, reduced by one new line pointer.
 * (Index pages; use PageGetHeapFreeSpace for heap pages.)
 */
pub unsafe fn PageGetFreeSpace(page: *const c_char) -> Size {
    let mut space = (*hdr(page)).pd_upper as c_int - (*hdr(page)).pd_lower as c_int;
    if space < size_of::<ItemIdData>() as c_int {
        return 0;
    }
    space -= size_of::<ItemIdData>() as c_int;
    space as Size
}

/* Free space reduced by ntups new line pointers. */
pub unsafe fn PageGetFreeSpaceForMultipleTuples(page: *const c_char, ntups: c_int) -> Size {
    let mut space = (*hdr(page)).pd_upper as c_int - (*hdr(page)).pd_lower as c_int;
    if space < ntups * size_of::<ItemIdData>() as c_int {
        return 0;
    }
    space -= ntups * size_of::<ItemIdData>() as c_int;
    space as Size
}

/* Free space with no consideration for line pointers. */
pub unsafe fn PageGetExactFreeSpace(page: *const c_char) -> Size {
    let space = (*hdr(page)).pd_upper as c_int - (*hdr(page)).pd_lower as c_int;
    if space < 0 {
        return 0;
    }
    space as Size
}

/*
 * PageGetHeapFreeSpace - like PageGetFreeSpace, but returns 0 if the page
 * already has MaxHeapTuplesPerPage line pointers and none are free.
 */
pub unsafe fn PageGetHeapFreeSpace(page: *const c_char) -> Size {
    let mut space = PageGetFreeSpace(page);
    if space > 0 {
        let nline = PageGetMaxOffsetNumber(page);
        if nline as c_int >= MaxHeapTuplesPerPage {
            if PageHasFreeLinePointers(page) {
                let mut offnum = FirstOffsetNumber;
                while offnum <= nline {
                    let lp = PageGetItemId(page as *mut c_char, offnum);
                    if !ItemIdIsUsed(lp) {
                        break;
                    }
                    offnum = OffsetNumberNext(offnum);
                }
                if offnum > nline {
                    /* hint wrong, but we can't clear it here */
                    space = 0;
                }
            } else {
                space = 0;
            }
        }
    }
    space
}

/*
 * PageIndexTupleDelete - remove a tuple from an index page, compacting out its
 * line pointer as well (unlike heap pages).
 */
pub unsafe fn PageIndexTupleDelete(page: Page, offnum: OffsetNumber) {
    let phdr = hdr(page);

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

    let mut nline = PageGetMaxOffsetNumber(page) as c_int;
    if (offnum as c_int) <= 0 || offnum as c_int > nline {
        elog!(ERROR, "invalid index offnum: {}", offnum);
    }

    let offidx = (offnum - 1) as usize;
    let tup = PageGetItemId(page, offnum);
    Assert!(ItemIdHasStorage(tup));
    let mut size = ItemIdGetLength(tup) as usize;
    let offset = ItemIdGetOffset(tup) as usize;

    if offset < (*phdr).pd_upper as usize
        || (offset + size) > (*phdr).pd_special as usize
        || offset != MAXALIGN(offset)
    {
        let _ = errcode(ERRCODE_DATA_CORRUPTED);
        ereport!(
            ERROR,
            errmsg!("corrupted line pointer: offset = {}, size = {}", offset, size)
        );
    }

    size = MAXALIGN(size);

    /* Remove the pd_linp entry by shifting subsequent linp's back one slot. */
    let linp = linp_base(page);
    let nbytes = (*phdr).pd_lower as isize
        - (linp.add(offidx + 1) as *const u8 as isize - page as *const u8 as isize);
    if nbytes > 0 {
        memmove(
            linp.add(offidx) as *mut c_void,
            linp.add(offidx + 1) as *const c_void,
            nbytes as usize,
        );
    }

    /* Move tuple data between pd_upper and the deleted tuple forward. */
    let addr = page.add((*phdr).pd_upper as usize);
    if offset > (*phdr).pd_upper as usize {
        memmove(
            addr.add(size) as *mut c_void,
            addr as *const c_void,
            offset - (*phdr).pd_upper as usize,
        );
    }

    (*phdr).pd_upper += size as LocationIndex;
    (*phdr).pd_lower -= size_of::<ItemIdData>() as LocationIndex;

    /* Adjust remaining linp entries that were before the deleted tuple's data. */
    if !PageIsEmpty(page) {
        nline -= 1;
        let mut i: c_int = 1;
        while i <= nline {
            let ii = PageGetItemId(page, i as OffsetNumber);
            Assert!(ItemIdHasStorage(ii));
            if (ItemIdGetOffset(ii) as usize) <= offset {
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
pub unsafe fn PageIndexMultiDelete(page: Page, itemnos: *mut OffsetNumber, mut nitems: c_int) {
    let phdr = hdr(page);
    let pd_lower = (*phdr).pd_lower as c_int;
    let pd_upper = (*phdr).pd_upper as c_int;
    let pd_special = (*phdr).pd_special as c_int;
    let mut last_offset: c_int;
    let mut itemidbase: [itemIdCompactData; MaxIndexTuplesPerPage] =
        [itemIdCompactData::default(); MaxIndexTuplesPerPage];
    let mut newitemids: [ItemIdData; MaxIndexTuplesPerPage] =
        [ItemIdData::default(); MaxIndexTuplesPerPage];
    let mut nidx: usize;
    let lp: ItemId;
    let nline: c_int;
    let mut nused: c_int;
    let mut totallen: Size;
    let mut size: Size;
    let mut offset: c_uint;
    let mut nextitm: c_int;
    let mut offnum: OffsetNumber;
    let mut presorted = true; /* For now */

    Assert!(nitems <= MaxIndexTuplesPerPage as c_int);

    /*
     * If there aren't very many items to delete, then retail
     * PageIndexTupleDelete is the best way.  Delete the items in reverse order
     * so we don't have to think about adjusting item numbers for previous
     * deletions.
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
    if pd_lower < SizeOfPageHeaderData as c_int
        || pd_lower > pd_upper
        || pd_upper > pd_special
        || pd_special > BLCKSZ as c_int
        || pd_special != MAXALIGN(pd_special as usize) as c_int
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
     * going to keep.  Notice we do not modify the page yet, since we are still
     * validity-checking.
     */
    nline = PageGetMaxOffsetNumber(page) as c_int;
    nidx = 0;
    totallen = 0;
    nused = 0;
    nextitm = 0;
    last_offset = pd_special;
    offnum = FirstOffsetNumber;
    while offnum as c_int <= nline {
        let lp = PageGetItemId(page, offnum);
        Assert!(ItemIdHasStorage(lp));
        size = ItemIdGetLength(lp) as Size;
        offset = ItemIdGetOffset(lp);
        if (offset as c_int) < pd_upper
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
            let e = &mut itemidbase[nidx];
            e.offsetindex = nused as uint16; /* where it will go */
            e.itemoff = offset as int16;

            if last_offset > e.itemoff as c_int {
                last_offset = e.itemoff as c_int;
            } else {
                presorted = false;
            }

            e.alignedlen = MAXALIGN(size) as uint16;
            totallen += e.alignedlen as Size;
            newitemids[nused as usize] = *lp;
            nidx += 1;
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
    (*phdr).pd_lower = (SizeOfPageHeaderData + nused as usize * size_of::<ItemIdData>()) as LocationIndex;

    /* and compactify the tuple data */
    if nused > 0 {
        compactify_tuples(itemidbase.as_mut_ptr(), nused, page, presorted);
    } else {
        (*phdr).pd_upper = pd_special as LocationIndex;
    }

    let _ = lp;
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
    let phdr = hdr(page);
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

    if (offset as c_int) < (*phdr).pd_upper as c_int
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
     * Either set the line pointer to "unused", or zap it if it's the last one.
     * (Note: it's possible that the next-to-last one(s) are already unused, but
     * we do not trouble to try to compact them out if so.)
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
     * the middle of the page is left free.  If we've just deleted the tuple at
     * the beginning of tuple space, then there's no need to do the copy.
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
        let mut i: c_int = 1;
        while i <= nline {
            let ii = PageGetItemId(page, i as OffsetNumber);
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
    let phdr = hdr(page);
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
     * Relocate existing data and update line pointers, unless the new tuple is
     * the same size as the old (after alignment), in which case there's nothing
     * to do.  Notice that what we have to relocate is data before the target
     * tuple, not data after, so it's convenient to express size_diff as the
     * amount by which the tuple's size is decreasing, making it the delta to add
     * to pd_upper and affected line pointers.
     */
    size_diff = oldsize - alignednewsize as c_int;
    if size_diff != 0 {
        let addr = page.add((*phdr).pd_upper as usize);

        /* relocate all tuple data before the target tuple */
        memmove(
            addr.offset(size_diff as isize) as *mut c_void,
            addr as *const c_void,
            offset as usize - (*phdr).pd_upper as usize,
        );

        /* adjust free space boundary pointer */
        (*phdr).pd_upper = ((*phdr).pd_upper as c_int + size_diff) as LocationIndex;

        /* adjust affected line pointers too */
        let mut i: c_int = FirstOffsetNumber as c_int;
        while i <= itemcount {
            let ii = PageGetItemId(page, i as OffsetNumber);

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
 * PageSetChecksumCopy / PageSetChecksumInplace
 * STUB: compute+store the page checksum.  Deferred (DataChecksumsEnabled() is
 * currently a false stub, so these would be no-ops anyway).
 * TODO(pg-port): wire to crate::storage::checksum::pg_checksum_page once the
 * checksum GUC + a temp-buffer policy are in place.
 */
pub unsafe fn PageSetChecksumInplace(page: Page, blkno: BlockNumber) {
    if PageIsNew(page) {
        return;
    }
    if DataChecksumsEnabled() {
        (*hdr(page)).pd_checksum = crate::storage::checksum::pg_checksum_page(page, blkno);
    }
}
pub unsafe fn PageSetChecksumCopy(page: Page, _blkno: BlockNumber) -> *mut c_char {
    // TODO(pg-port): the real version copies the page into a static buffer and
    // checksums the copy (so the shared buffer isn't modified).  With checksums
    // disabled (stub), it returns the page unchanged.
    page
}

#[cfg(test)]
mod tests {
    use super::*;

    // A MAXALIGN'd BLCKSZ page buffer.
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
            let free0 = PageGetExactFreeSpace(page);
            assert_eq!(free0, BLCKSZ - MAXALIGN(SizeOfPageHeaderData));

            // add two 8-byte items.
            let a: [u8; 8] = [1, 2, 3, 4, 5, 6, 7, 8];
            let b: [u8; 8] = [9, 10, 11, 12, 13, 14, 15, 16];
            let o1 = PageAddItem(page, a.as_ptr() as Item, 8, InvalidOffsetNumber, false, false);
            let o2 = PageAddItem(page, b.as_ptr() as Item, 8, InvalidOffsetNumber, false, false);
            assert_eq!(o1, 1);
            assert_eq!(o2, 2);
            assert_eq!(PageGetMaxOffsetNumber(page), 2);

            // read back item 1's bytes.
            let it1 = PageGetItem(page, PageGetItemId(page, 1));
            let s1 = core::slice::from_raw_parts(it1 as *const u8, 8);
            assert_eq!(s1, &a);
            let it2 = PageGetItem(page, PageGetItemId(page, 2));
            let s2 = core::slice::from_raw_parts(it2 as *const u8, 8);
            assert_eq!(s2, &b);

            // free space dropped by 2*(linp + MAXALIGN(8)).
            assert!(PageGetExactFreeSpace(page) < free0);
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
            PageAddItem(page, a.as_ptr() as Item, 8, InvalidOffsetNumber, false, false);
            PageAddItem(page, b.as_ptr() as Item, 8, InvalidOffsetNumber, false, false);
            PageAddItem(page, c.as_ptr() as Item, 8, InvalidOffsetNumber, false, false);
            assert_eq!(PageGetMaxOffsetNumber(page), 3);

            // delete the middle one; the line pointer is compacted out.
            PageIndexTupleDelete(page, 2);
            assert_eq!(PageGetMaxOffsetNumber(page), 2);
            // remaining items are the old 1st (a) and 3rd (c).
            let i1 = core::slice::from_raw_parts(PageGetItem(page, PageGetItemId(page, 1)) as *const u8, 8);
            let i2 = core::slice::from_raw_parts(PageGetItem(page, PageGetItemId(page, 2)) as *const u8, 8);
            assert_eq!(i1, &a);
            assert_eq!(i2, &c);
        }
    }
}
