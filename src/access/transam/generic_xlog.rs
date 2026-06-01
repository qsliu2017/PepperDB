//! Implementation of generic xlog records.
//!
//! src/backend/access/transam/generic_xlog.c
//! (companion header: src/include/access/generic_xlog.h)
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::prelude::*;

use std::ffi::{c_char, c_int, c_void};

use crate::access::rmgrdesc::genericdesc::XLR_NORMAL_MAX_BLOCK_ID;
use crate::access::transam::xlogdefs::XLogRecPtr;
use crate::c::Size;
use crate::c::{Max, Min};
use crate::pg_config::BLCKSZ;
use crate::storage::block::BlockNumber;
use crate::storage::off::OffsetNumber;
use crate::utils::elog::ERROR;
use crate::utils::mmgr::mcxt::palloc_aligned;

// Assert and elog are #[macro_export] at the crate root.
use crate::{elog, Assert};

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

// ---------------------------------------------------------------------------
// From generic_xlog.h
// ---------------------------------------------------------------------------

pub const MAX_GENERIC_XLOG_PAGES: usize = XLR_NORMAL_MAX_BLOCK_ID as usize;

/* Flag bits for GenericXLogRegisterBuffer */
pub const GENERIC_XLOG_FULL_IMAGE: c_int = 0x0001; /* write full-page image */

// ---------------------------------------------------------------------------

/*-------------------------------------------------------------------------
 * Internally, a delta between pages consists of a set of fragments.  Each
 * fragment represents changes made in a given region of a page.  A fragment
 * is made up as follows:
 *
 * - offset of page region (OffsetNumber)
 * - length of page region (OffsetNumber)
 * - data - the data to place into the region ('length' number of bytes)
 *
 * Unchanged regions of a page are not represented in its delta.  As a result,
 * a delta can be more compact than the full page image.  But having an
 * unchanged region between two fragments that is smaller than the fragment
 * header (offset+length) does not pay off in terms of the overall size of
 * the delta.  For this reason, we merge adjacent fragments if the unchanged
 * region between them is <= MATCH_THRESHOLD bytes.
 *
 * We do not bother to merge fragments across the "lower" and "upper" parts
 * of a page; it's very seldom the case that pd_lower and pd_upper are within
 * MATCH_THRESHOLD bytes of each other, and handling that infrequent case
 * would complicate and slow down the delta-computation code unduly.
 * Therefore, the worst-case delta size includes two fragment headers plus
 * a full page's worth of data.
 *-------------------------------------------------------------------------
 */
const FRAGMENT_HEADER_SIZE: usize = 2 * std::mem::size_of::<OffsetNumber>();
const MATCH_THRESHOLD: c_int = FRAGMENT_HEADER_SIZE as c_int;
const MAX_DELTA_SIZE: usize = BLCKSZ as usize + 2 * FRAGMENT_HEADER_SIZE;

/* Struct of generic xlog data for single page */
#[repr(C)]
pub struct GenericXLogPageData {
    pub buffer: Buffer,        /* registered buffer */
    pub flags: c_int,          /* flags for this buffer */
    pub deltaLen: c_int,       /* space consumed in delta field */
    pub image: *mut c_char,    /* copy of page image for modification, do not
                                * do it in-place to have aligned memory chunk */
    pub delta: [c_char; MAX_DELTA_SIZE], /* delta between page images */
}

/*
 * State of generic xlog record construction.  Must be allocated at an I/O
 * aligned address.
 */
#[repr(C)]
pub struct GenericXLogState {
    /* Page images (properly aligned, must be first) */
    pub images: [PGIOAlignedBlock; MAX_GENERIC_XLOG_PAGES],
    /* Info about each page, see above */
    pub pages: [GenericXLogPageData; MAX_GENERIC_XLOG_PAGES],
    pub isLogged: bool,
}

/*
 * Write next fragment into pageData's delta.
 *
 * The fragment has the given offset and length, and data points to the
 * actual data (of length length).
 */
unsafe fn writeFragment(
    pageData: *mut GenericXLogPageData,
    offset: OffsetNumber,
    length: OffsetNumber,
    data: *const c_char,
) {
    let mut ptr = (*pageData).delta.as_mut_ptr().add((*pageData).deltaLen as usize);

    /* Verify we have enough space */
    Assert!(
        (*pageData).deltaLen as usize
            + std::mem::size_of::<OffsetNumber>()
            + std::mem::size_of::<OffsetNumber>()
            + length as usize
            <= std::mem::size_of_val(&(*pageData).delta)
    );

    /* Write fragment data */
    memcpy(
        ptr as *mut c_void,
        &offset as *const OffsetNumber as *const c_void,
        std::mem::size_of::<OffsetNumber>(),
    );
    ptr = ptr.add(std::mem::size_of::<OffsetNumber>());
    memcpy(
        ptr as *mut c_void,
        &length as *const OffsetNumber as *const c_void,
        std::mem::size_of::<OffsetNumber>(),
    );
    ptr = ptr.add(std::mem::size_of::<OffsetNumber>());
    memcpy(ptr as *mut c_void, data as *const c_void, length as usize);
    ptr = ptr.add(length as usize);

    (*pageData).deltaLen = ptr.offset_from((*pageData).delta.as_ptr()) as c_int;
}

/*
 * Compute the XLOG fragments needed to transform a region of curpage into the
 * corresponding region of targetpage, and append them to pageData's delta
 * field.  The region to transform runs from targetStart to targetEnd-1.
 * Bytes in curpage outside the range validStart to validEnd-1 should be
 * considered invalid, and always overwritten with target data.
 *
 * This function is a hot spot, so it's worth being as tense as possible
 * about the data-matching loops.
 */
unsafe fn computeRegionDelta(
    pageData: *mut GenericXLogPageData,
    curpage: *const c_char,
    targetpage: *const c_char,
    mut targetStart: c_int,
    targetEnd: c_int,
    validStart: c_int,
    validEnd: c_int,
) {
    let mut i: c_int;
    let loopEnd: c_int;
    let mut fragmentBegin: c_int = -1;
    let mut fragmentEnd: c_int = -1;

    /* Deal with any invalid start region by including it in first fragment */
    if validStart > targetStart {
        fragmentBegin = targetStart;
        targetStart = validStart;
    }

    /* We'll deal with any invalid end region after the main loop */
    loopEnd = Min(targetEnd, validEnd);

    /* Examine all the potentially matchable bytes */
    i = targetStart;
    while i < loopEnd {
        if *curpage.offset(i as isize) != *targetpage.offset(i as isize) {
            /* On unmatched byte, start new fragment if not already in one */
            if fragmentBegin < 0 {
                fragmentBegin = i;
            }
            /* Mark unmatched-data endpoint as uncertain */
            fragmentEnd = -1;
            /* Extend the fragment as far as possible in a tight loop */
            i += 1;
            while i < loopEnd && *curpage.offset(i as isize) != *targetpage.offset(i as isize) {
                i += 1;
            }
            if i >= loopEnd {
                break;
            }
        }

        /* Found a matched byte, so remember end of unmatched fragment */
        fragmentEnd = i;

        /*
         * Extend the match as far as possible in a tight loop.  (On typical
         * workloads, this inner loop is the bulk of this function's runtime.)
         */
        i += 1;
        while i < loopEnd && *curpage.offset(i as isize) == *targetpage.offset(i as isize) {
            i += 1;
        }

        /*
         * There are several possible cases at this point:
         *
         * 1. We have no unwritten fragment (fragmentBegin < 0).  There's
         * nothing to write; and it doesn't matter what fragmentEnd is.
         *
         * 2. We found more than MATCH_THRESHOLD consecutive matching bytes.
         * Dump out the unwritten fragment, stopping at fragmentEnd.
         *
         * 3. The match extends to loopEnd.  We'll do nothing here, exit the
         * loop, and then dump the unwritten fragment, after merging it with
         * the invalid end region if any.  If we don't so merge, fragmentEnd
         * establishes how much the final writeFragment call needs to write.
         *
         * 4. We found an unmatched byte before loopEnd.  The loop will repeat
         * and will enter the unmatched-byte stanza above.  So in this case
         * also, it doesn't matter what fragmentEnd is.  The matched bytes
         * will get merged into the continuing unmatched fragment.
         *
         * Only in case 3 do we reach the bottom of the loop with a meaningful
         * fragmentEnd value, which is why it's OK that we unconditionally
         * assign "fragmentEnd = i" above.
         */
        if fragmentBegin >= 0 && i - fragmentEnd > MATCH_THRESHOLD {
            writeFragment(
                pageData,
                fragmentBegin as OffsetNumber,
                (fragmentEnd - fragmentBegin) as OffsetNumber,
                targetpage.offset(fragmentBegin as isize),
            );
            fragmentBegin = -1;
            fragmentEnd = -1; /* not really necessary */
        }
    }

    /* Deal with any invalid end region by including it in final fragment */
    if loopEnd < targetEnd {
        if fragmentBegin < 0 {
            fragmentBegin = loopEnd;
        }
        fragmentEnd = targetEnd;
    }

    /* Write final fragment if any */
    if fragmentBegin >= 0 {
        if fragmentEnd < 0 {
            fragmentEnd = targetEnd;
        }
        writeFragment(
            pageData,
            fragmentBegin as OffsetNumber,
            (fragmentEnd - fragmentBegin) as OffsetNumber,
            targetpage.offset(fragmentBegin as isize),
        );
    }
}

/*
 * Compute the XLOG delta record needed to transform curpage into targetpage,
 * and store it in pageData's delta field.
 */
unsafe fn computeDelta(pageData: *mut GenericXLogPageData, curpage: Page, targetpage: Page) {
    let targetLower: c_int = (*(targetpage as PageHeader)).pd_lower as c_int;
    let targetUpper: c_int = (*(targetpage as PageHeader)).pd_upper as c_int;
    let curLower: c_int = (*(curpage as PageHeader)).pd_lower as c_int;
    let curUpper: c_int = (*(curpage as PageHeader)).pd_upper as c_int;

    (*pageData).deltaLen = 0;

    /* Compute delta records for lower part of page ... */
    computeRegionDelta(
        pageData,
        curpage as *const c_char,
        targetpage as *const c_char,
        0,
        targetLower,
        0,
        curLower,
    );
    /* ... and for upper part, ignoring what's between */
    computeRegionDelta(
        pageData,
        curpage as *const c_char,
        targetpage as *const c_char,
        targetUpper,
        BLCKSZ as c_int,
        curUpper,
        BLCKSZ as c_int,
    );

    /*
     * If xlog debug is enabled, then check produced delta.  Result of delta
     * application to curpage should be equivalent to targetpage.
     */
    // #ifdef WAL_DEBUG: not compiled in this build.
    let _ = (targetLower, targetUpper);
}

/*
 * Start new generic xlog record for modifications to specified relation.
 */
pub unsafe fn GenericXLogStart(relation: Relation) -> *mut GenericXLogState {
    let state: *mut GenericXLogState = palloc_aligned(
        std::mem::size_of::<GenericXLogState>(),
        PG_IO_ALIGN_SIZE,
        0,
    ) as *mut GenericXLogState;
    (*state).isLogged = RelationNeedsWAL(relation);

    for i in 0..MAX_GENERIC_XLOG_PAGES {
        (*state).pages[i].image = (*state).images[i].data.as_mut_ptr();
        (*state).pages[i].buffer = InvalidBuffer;
    }

    state
}

/*
 * Register new buffer for generic xlog record.
 *
 * Returns pointer to the page's image in the GenericXLogState, which
 * is what the caller should modify.
 *
 * If the buffer is already registered, just return its existing entry.
 * (It's not very clear what to do with the flags in such a case, but
 * for now we stay with the original flags.)
 */
pub unsafe fn GenericXLogRegisterBuffer(
    state: *mut GenericXLogState,
    buffer: Buffer,
    flags: c_int,
) -> Page {
    /* Search array for existing entry or first unused slot */
    for block_id in 0..MAX_GENERIC_XLOG_PAGES {
        let page: *mut GenericXLogPageData = &mut (*state).pages[block_id];

        if BufferIsInvalid((*page).buffer) {
            /* Empty slot, so use it (there cannot be a match later) */
            (*page).buffer = buffer;
            (*page).flags = flags;
            memcpy(
                (*page).image as *mut c_void,
                BufferGetPage(buffer) as *const c_void,
                BLCKSZ as usize,
            );
            return (*page).image as Page;
        } else if (*page).buffer == buffer {
            /*
             * Buffer is already registered.  Just return the image, which is
             * already prepared.
             */
            return (*page).image as Page;
        }
    }

    elog!(
        ERROR,
        "maximum number {} of generic xlog buffers is exceeded",
        MAX_GENERIC_XLOG_PAGES
    );
    /* keep compiler quiet */
    #[allow(unreachable_code)]
    {
        std::ptr::null_mut()
    }
}

/*
 * Apply changes represented by GenericXLogState to the actual buffers,
 * and emit a generic xlog record.
 */
pub unsafe fn GenericXLogFinish(state: *mut GenericXLogState) -> XLogRecPtr {
    let lsn: XLogRecPtr;

    if (*state).isLogged {
        /* Logged relation: make xlog record in critical section. */
        XLogBeginInsert();

        START_CRIT_SECTION!();

        /*
         * Compute deltas if necessary, write changes to buffers, mark buffers
         * dirty, and register changes.
         */
        for i in 0..MAX_GENERIC_XLOG_PAGES {
            let pageData: *mut GenericXLogPageData = &mut (*state).pages[i];
            let page: Page;
            let pageHeader: PageHeader;

            if BufferIsInvalid((*pageData).buffer) {
                continue;
            }

            page = BufferGetPage((*pageData).buffer);
            pageHeader = (*pageData).image as PageHeader;

            /*
             * Compute delta while we still have both the unmodified page and
             * the new image. Not needed if we are logging the full image.
             */
            if ((*pageData).flags & GENERIC_XLOG_FULL_IMAGE) == 0 {
                computeDelta(pageData, page, (*pageData).image as Page);
            }

            /*
             * Apply the image, being careful to zero the "hole" between
             * pd_lower and pd_upper in order to avoid divergence between
             * actual page state and what replay would produce.
             */
            memcpy(
                page as *mut c_void,
                (*pageData).image as *const c_void,
                (*pageHeader).pd_lower as usize,
            );
            memset(
                (page as *mut c_char).add((*pageHeader).pd_lower as usize) as *mut c_void,
                0,
                ((*pageHeader).pd_upper - (*pageHeader).pd_lower) as usize,
            );
            memcpy(
                (page as *mut c_char).add((*pageHeader).pd_upper as usize) as *mut c_void,
                (*pageData).image.add((*pageHeader).pd_upper as usize) as *const c_void,
                (BLCKSZ as usize) - (*pageHeader).pd_upper as usize,
            );

            MarkBufferDirty((*pageData).buffer);

            if ((*pageData).flags & GENERIC_XLOG_FULL_IMAGE) != 0 {
                XLogRegisterBuffer(
                    i as u8,
                    (*pageData).buffer,
                    (REGBUF_FORCE_IMAGE | REGBUF_STANDARD) as u8,
                );
            } else {
                XLogRegisterBuffer(i as u8, (*pageData).buffer, REGBUF_STANDARD as u8);
                XLogRegisterBufData(
                    i as u8,
                    (*pageData).delta.as_ptr(),
                    (*pageData).deltaLen as usize,
                );
            }
        }

        /* Insert xlog record */
        lsn = XLogInsert(RM_GENERIC_ID, 0);

        /* Set LSN */
        for i in 0..MAX_GENERIC_XLOG_PAGES {
            let pageData: *mut GenericXLogPageData = &mut (*state).pages[i];

            if BufferIsInvalid((*pageData).buffer) {
                continue;
            }
            PageSetLSN(BufferGetPage((*pageData).buffer), lsn);
        }
        END_CRIT_SECTION!();
    } else {
        /* Unlogged relation: skip xlog-related stuff */
        START_CRIT_SECTION!();
        for i in 0..MAX_GENERIC_XLOG_PAGES {
            let pageData: *mut GenericXLogPageData = &mut (*state).pages[i];

            if BufferIsInvalid((*pageData).buffer) {
                continue;
            }
            memcpy(
                BufferGetPage((*pageData).buffer) as *mut c_void,
                (*pageData).image as *const c_void,
                BLCKSZ as usize,
            );
            /* We don't worry about zeroing the "hole" in this case */
            MarkBufferDirty((*pageData).buffer);
        }
        END_CRIT_SECTION!();
        /* We don't have a LSN to return, in this case */
        lsn = InvalidXLogRecPtr;
    }

    pfree(state as *mut c_void);

    lsn
}

/*
 * Abort generic xlog record construction.  No changes are applied to buffers.
 *
 * Note: caller is responsible for releasing locks/pins on buffers, if needed.
 */
pub unsafe fn GenericXLogAbort(state: *mut GenericXLogState) {
    pfree(state as *mut c_void);
}

/*
 * Apply delta to given page image.
 */
unsafe fn applyPageRedo(page: Page, delta: *const c_char, deltaSize: Size) {
    let mut ptr: *const c_char = delta;
    let end: *const c_char = delta.add(deltaSize);

    while ptr < end {
        let mut offset: OffsetNumber = 0;
        let mut length: OffsetNumber = 0;

        memcpy(
            &mut offset as *mut OffsetNumber as *mut c_void,
            ptr as *const c_void,
            std::mem::size_of::<OffsetNumber>(),
        );
        ptr = ptr.add(std::mem::size_of::<OffsetNumber>());
        memcpy(
            &mut length as *mut OffsetNumber as *mut c_void,
            ptr as *const c_void,
            std::mem::size_of::<OffsetNumber>(),
        );
        ptr = ptr.add(std::mem::size_of::<OffsetNumber>());

        memcpy(
            (page as *mut c_char).add(offset as usize) as *mut c_void,
            ptr as *const c_void,
            length as usize,
        );

        ptr = ptr.add(length as usize);
    }
}

/*
 * Redo function for generic xlog record.
 */
pub unsafe fn generic_redo(record: *mut XLogReaderState) {
    let lsn: XLogRecPtr = (*record).EndRecPtr;
    let mut buffers: [Buffer; MAX_GENERIC_XLOG_PAGES] = [0; MAX_GENERIC_XLOG_PAGES];
    let mut block_id: u8;

    /* Protect limited size of buffers[] array */
    Assert!((XLogRecMaxBlockId(record) as usize) < MAX_GENERIC_XLOG_PAGES);

    /* Iterate over blocks */
    block_id = 0;
    while block_id <= XLogRecMaxBlockId(record) {
        let action: XLogRedoAction;

        if !XLogRecHasBlockRef(record, block_id) {
            buffers[block_id as usize] = InvalidBuffer;
            block_id += 1;
            continue;
        }

        action = XLogReadBufferForRedo(record, block_id, &mut buffers[block_id as usize]);

        /* Apply redo to given block if needed */
        if action == BLK_NEEDS_REDO {
            let page: Page;
            let pageHeader: PageHeader;
            let blockDelta: *mut c_char;
            let mut blockDeltaSize: Size = 0;

            page = BufferGetPage(buffers[block_id as usize]);
            blockDelta = XLogRecGetBlockData(record, block_id, &mut blockDeltaSize);
            applyPageRedo(page, blockDelta, blockDeltaSize);

            /*
             * Since the delta contains no information about what's in the
             * "hole" between pd_lower and pd_upper, set that to zero to
             * ensure we produce the same page state that application of the
             * logged action by GenericXLogFinish did.
             */
            pageHeader = page as PageHeader;
            memset(
                (page as *mut c_char).add((*pageHeader).pd_lower as usize) as *mut c_void,
                0,
                ((*pageHeader).pd_upper - (*pageHeader).pd_lower) as usize,
            );

            PageSetLSN(page, lsn);
            MarkBufferDirty(buffers[block_id as usize]);
        }
        block_id += 1;
    }

    /* Changes are done: unlock and release all buffers */
    block_id = 0;
    while block_id <= XLogRecMaxBlockId(record) {
        if BufferIsValid(buffers[block_id as usize]) {
            UnlockReleaseBuffer(buffers[block_id as usize]);
        }
        block_id += 1;
    }
}

/*
 * Mask a generic page before performing consistency checks on it.
 */
pub unsafe fn generic_mask(page: *mut c_char, blkno: BlockNumber) {
    let _ = blkno;
    mask_page_lsn_and_checksum(page);

    mask_unused_space(page);
}

// ---------------------------------------------------------------------------
// Local stubs for unported dependencies.
// ---------------------------------------------------------------------------

extern "C" {
    fn memcpy(dest: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
    fn memset(s: *mut c_void, c: c_int, n: usize) -> *mut c_void;
}

unsafe fn RelationNeedsWAL(_relation: Relation) -> bool {
    unimplemented!() // TODO: utils/rel.h
}
unsafe fn BufferGetPage(_buffer: Buffer) -> Page {
    unimplemented!() // TODO: storage/bufmgr.h
}
unsafe fn BufferIsInvalid(_buffer: Buffer) -> bool {
    unimplemented!() // TODO: storage/buf.h
}
unsafe fn BufferIsValid(_buffer: Buffer) -> bool {
    unimplemented!() // TODO: storage/bufmgr.h
}
unsafe fn MarkBufferDirty(_buffer: Buffer) {
    unimplemented!() // TODO: storage/bufmgr.h
}
unsafe fn UnlockReleaseBuffer(_buffer: Buffer) {
    unimplemented!() // TODO: storage/bufmgr.h
}
unsafe fn PageSetLSN(_page: Page, _lsn: XLogRecPtr) {
    unimplemented!() // TODO: storage/bufpage.h
}
unsafe fn XLogBeginInsert() {
    unimplemented!() // TODO: access/xloginsert.h
}
unsafe fn XLogRegisterBuffer(_block_id: u8, _buffer: Buffer, _flags: u8) {
    unimplemented!() // TODO: access/xloginsert.h
}
unsafe fn XLogRegisterBufData(_block_id: u8, _data: *const c_char, _len: usize) {
    unimplemented!() // TODO: access/xloginsert.h
}
unsafe fn XLogInsert(_rmid: RmgrId, _info: u8) -> XLogRecPtr {
    unimplemented!() // TODO: access/xloginsert.h
}
unsafe fn XLogRecMaxBlockId(_record: *mut XLogReaderState) -> u8 {
    unimplemented!() // TODO: access/xlogreader.h
}
unsafe fn XLogRecHasBlockRef(_record: *mut XLogReaderState, _block_id: u8) -> bool {
    unimplemented!() // TODO: access/xlogreader.h
}
unsafe fn XLogReadBufferForRedo(
    _record: *mut XLogReaderState,
    _block_id: u8,
    _buf: *mut Buffer,
) -> XLogRedoAction {
    unimplemented!() // TODO: access/xlogutils.h
}
unsafe fn XLogRecGetBlockData(
    _record: *mut XLogReaderState,
    _block_id: u8,
    _len: *mut Size,
) -> *mut c_char {
    unimplemented!() // TODO: access/xlogreader.h
}
unsafe fn mask_page_lsn_and_checksum(_page: *mut c_char) {
    unimplemented!() // TODO: access/bufmask.h
}
unsafe fn mask_unused_space(_page: *mut c_char) {
    unimplemented!() // TODO: access/bufmask.h
}

// Stub types for unported dependencies.
pub type Relation = *mut c_void; // TODO: utils/rel.h
pub type Buffer = c_int; // TODO: storage/buf.h
pub type Page = *mut c_char; // TODO: storage/bufpage.h
pub type PageHeader = *mut PageHeaderData; // TODO: storage/bufpage.h
pub type RmgrId = u8; // TODO: access/rmgr.h
pub type XLogRedoAction = c_int; // TODO: access/xlogutils.h

#[repr(C)]
pub struct PageHeaderData {
    // TODO: storage/bufpage.h (only fields used here)
    pub _pad: [c_char; 12],
    pub pd_lower: u16,
    pub pd_upper: u16,
}

#[repr(C)]
pub struct XLogReaderState {
    // TODO: access/xlogreader.h (only field used here)
    pub EndRecPtr: XLogRecPtr,
}

#[repr(C, align(8192))]
pub struct PGIOAlignedBlock {
    // TODO: c.h
    pub data: [c_char; BLCKSZ as usize],
}

pub const InvalidBuffer: Buffer = 0; // TODO: storage/buf.h
pub const InvalidXLogRecPtr: XLogRecPtr = 0; // TODO: access/transam/xlogdefs.h
pub const BLK_NEEDS_REDO: XLogRedoAction = 1; // TODO: access/xlogutils.h
pub const RM_GENERIC_ID: RmgrId = 0; // TODO: access/rmgrlist.h
pub const REGBUF_STANDARD: c_int = 0x04; // TODO: access/xloginsert.h
pub const REGBUF_FORCE_IMAGE: c_int = 0x02; // TODO: access/xloginsert.h
pub const PG_IO_ALIGN_SIZE: usize = 4096; // TODO: c.h
