//! visibilitymap.rs
//!   bitmap for tracking visibility of heap tuples
//!
//! Translated 1:1 from postgres/src/backend/access/heap/visibilitymap.c
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!	  src/backend/access/heap/visibilitymap.c
//!
//! INTERFACE ROUTINES
//!		visibilitymap_clear  - clear bits for one page in the visibility map
//!		visibilitymap_pin	 - pin a map page for setting a bit
//!		visibilitymap_pin_ok - check whether correct map page is already pinned
//!		visibilitymap_set	 - set a bit in a previously pinned page
//!		visibilitymap_get_status - get status of bits
//!		visibilitymap_count  - count number of bits set in visibility map
//!		visibilitymap_prepare_truncate -
//!			prepare for truncation of the visibility map
//!
//! NOTES
//!
//! The visibility map is a bitmap with two bits (all-visible and all-frozen)
//! per heap page. A set all-visible bit means that all tuples on the page are
//! known visible to all transactions, and therefore the page doesn't need to
//! be vacuumed. A set all-frozen bit means that all tuples on the page are
//! completely frozen, and therefore the page doesn't need to be vacuumed even
//! if whole table scanning vacuum is required (e.g. anti-wraparound vacuum).
//! The all-frozen bit must be set only when the page is already all-visible.
//!
//! The map is conservative in the sense that we make sure that whenever a bit
//! is set, we know the condition is true, but if a bit is not set, it might or
//! might not be true.
//!
//! Clearing visibility map bits is not separately WAL-logged.  The callers
//! must make sure that whenever a bit is cleared, the bit is cleared on WAL
//! replay of the updating operation as well.
//!
//! When we *set* a visibility map during VACUUM, we must write WAL.  This may
//! seem counterintuitive, since the bit is basically a hint: if it is clear,
//! it may still be the case that every tuple on the page is visible to all
//! transactions; we just don't know that for certain.  The difficulty is that
//! there are two bits which are typically set together: the PD_ALL_VISIBLE bit
//! on the page itself, and the visibility map bit.  If a crash occurs after the
//! visibility map page makes it to disk and before the updated heap page makes
//! it to disk, redo must set the bit on the heap page.  Otherwise, the next
//! insert, update, or delete on the heap page will fail to realize that the
//! visibility map bit must be cleared, possibly causing index-only scans to
//! return wrong answers.
//!
//! VACUUM will normally skip pages for which the visibility map bit is set;
//! such pages can't contain any dead tuples and therefore don't need vacuuming.
//!
//! LOCKING
//!
//! In heapam.c, whenever a page is modified so that not all tuples on the
//! page are visible to everyone anymore, the corresponding bit in the
//! visibility map is cleared. In order to be crash-safe, we need to do this
//! while still holding a lock on the heap page and in the same critical
//! section that logs the page modification. However, we don't want to hold
//! the buffer lock over any I/O that may be required to read in the visibility
//! map page.  To avoid this, we examine the heap page before locking it;
//! if the page-level PD_ALL_VISIBLE bit is set, we pin the visibility map
//! bit.  Then, we lock the buffer.  But this creates a race condition: there
//! is a possibility that in the time it takes to lock the buffer, the
//! PD_ALL_VISIBLE bit gets set.  If that happens, we have to unlock the
//! buffer, pin the visibility map page, and relock the buffer.  This shouldn't
//! happen often, because only VACUUM currently sets visibility map bits,
//! and the race will only occur if VACUUM processes a given page at almost
//! exactly the same time that someone tries to further modify it.
//!
//! To set a bit, you need to hold a lock on the heap page. That prevents
//! the race condition where VACUUM sees that all tuples on the page are
//! visible to everyone, but another backend modifies the page before VACUUM
//! sets the bit in the visibility map.
//!
//! When a bit is set, the LSN of the visibility map page is updated to make
//! sure that the visibility map update doesn't get written to disk before the
//! WAL record of the changes that made it possible to set the bit is flushed.
//! But when a bit is cleared, we don't have to do that because it's always
//! safe to clear a bit in the map from correctness point of view.

use crate::prelude::*;

use std::ffi::c_int;
use std::ffi::c_void;

use crate::c::bits8;
use crate::c::uint32;
use crate::c::uint64;
use crate::c::uint8;
use crate::c::MAXALIGN;
use crate::c::TransactionId;
use crate::pg_config::BLCKSZ;
use crate::port::pg_bitutils::pg_popcount_masked;
use crate::storage::block::BlockNumber;
use crate::storage::block::InvalidBlockNumber;
use crate::storage::bufpage::Page;
use crate::storage::bufpage::SizeOfPageHeaderData;

// from access/visibilitymapdefs.h
use crate::access::visibilitymapdefs::VISIBILITYMAP_ALL_FROZEN;
use crate::access::visibilitymapdefs::VISIBILITYMAP_ALL_VISIBLE;
use crate::access::visibilitymapdefs::BITS_PER_HEAPBLOCK;
use crate::access::visibilitymapdefs::VISIBILITYMAP_VALID_BITS;

// from pg_config_manual.h
use crate::pg_config_manual::BITS_PER_BYTE;

// from access/transam/xlogdefs.h
use crate::access::transam::xlogdefs::XLogRecPtr;
use crate::access::transam::xlogdefs::XLogRecPtrIsInvalid;

// from common/relpath.h
use crate::common::relpath::VISIBILITYMAP_FORKNUM;

// from storage/smgr/smgr.h
use crate::storage::smgr::smgr::SMgrRelation;

// from utils/relcache.h
type Relation = *mut crate::utils::rel::RelationData;

// from storage/buf.h / storage/bufmgr.h
type Buffer = c_int;

/*#define TRACE_VISIBILITYMAP */

/*
 * Size of the bitmap on each visibility map page, in bytes. There's no
 * extra headers, so the whole page minus the standard page header is
 * used for the bitmap.
 */
const MAPSIZE: usize = BLCKSZ - MAXALIGN(SizeOfPageHeaderData);

/* Number of heap blocks we can represent in one byte */
const HEAPBLOCKS_PER_BYTE: BlockNumber = (BITS_PER_BYTE as BlockNumber) / (BITS_PER_HEAPBLOCK as BlockNumber);

/* Number of heap blocks we can represent in one visibility map page. */
const HEAPBLOCKS_PER_PAGE: BlockNumber = (MAPSIZE as BlockNumber) * HEAPBLOCKS_PER_BYTE;

/* Mapping from heap block number to the right bit in the visibility map */
#[inline(always)]
fn HEAPBLK_TO_MAPBLOCK(x: BlockNumber) -> BlockNumber {
    x / HEAPBLOCKS_PER_PAGE
}
#[inline(always)]
fn HEAPBLK_TO_MAPBYTE(x: BlockNumber) -> BlockNumber {
    (x % HEAPBLOCKS_PER_PAGE) / HEAPBLOCKS_PER_BYTE
}
#[inline(always)]
fn HEAPBLK_TO_OFFSET(x: BlockNumber) -> BlockNumber {
    (x % HEAPBLOCKS_PER_BYTE) * (BITS_PER_HEAPBLOCK as BlockNumber)
}

/* Masks for counting subsets of bits in the visibility map. */
const VISIBLE_MASK8: bits8 = 0x55; /* The lower bit of each bit pair */
const FROZEN_MASK8: bits8 = 0xaa; /* The upper bit of each bit pair */

/*
 *	visibilitymap_clear - clear specified bits for one page in visibility map
 *
 * You must pass a buffer containing the correct map page to this function.
 * Call visibilitymap_pin first to pin the right one. This function doesn't do
 * any I/O.  Returns true if any bits have been cleared and false otherwise.
 */
pub unsafe fn visibilitymap_clear(
    rel: Relation,
    heapBlk: BlockNumber,
    vmbuf: Buffer,
    flags: uint8,
) -> bool {
    let mapBlock: BlockNumber = HEAPBLK_TO_MAPBLOCK(heapBlk);
    let mapByte: c_int = HEAPBLK_TO_MAPBYTE(heapBlk) as c_int;
    let mapOffset: c_int = HEAPBLK_TO_OFFSET(heapBlk) as c_int;
    let mask: uint8 = flags << mapOffset;
    let map: *mut c_char;
    let mut cleared: bool = false;

    /* Must never clear all_visible bit while leaving all_frozen bit set */
    Assert!(flags & VISIBILITYMAP_VALID_BITS != 0);
    Assert!(flags != VISIBILITYMAP_ALL_VISIBLE);

    #[cfg(any())]
    elog!(DEBUG1, "vm_clear {} {}", RelationGetRelationName(rel), heapBlk);

    if !BufferIsValid(vmbuf) || BufferGetBlockNumber(vmbuf) != mapBlock {
        elog!(ERROR, "wrong buffer passed to visibilitymap_clear");
    }

    LockBuffer(vmbuf, BUFFER_LOCK_EXCLUSIVE);
    map = PageGetContents(BufferGetPage(vmbuf)) as *mut c_char;

    if *map.add(mapByte as usize) & (mask as c_char) != 0 {
        *map.add(mapByte as usize) &= !(mask as c_char);

        MarkBufferDirty(vmbuf);
        cleared = true;
    }

    LockBuffer(vmbuf, BUFFER_LOCK_UNLOCK);

    cleared
}

/*
 *	visibilitymap_pin - pin a map page for setting a bit
 *
 * Setting a bit in the visibility map is a two-phase operation. First, call
 * visibilitymap_pin, to pin the visibility map page containing the bit for
 * the heap page. Because that can require I/O to read the map page, you
 * shouldn't hold a lock on the heap page while doing that. Then, call
 * visibilitymap_set to actually set the bit.
 *
 * On entry, *vmbuf should be InvalidBuffer or a valid buffer returned by
 * an earlier call to visibilitymap_pin or visibilitymap_get_status on the same
 * relation. On return, *vmbuf is a valid buffer with the map page containing
 * the bit for heapBlk.
 *
 * If the page doesn't exist in the map file yet, it is extended.
 */
pub unsafe fn visibilitymap_pin(rel: Relation, heapBlk: BlockNumber, vmbuf: *mut Buffer) {
    let mapBlock: BlockNumber = HEAPBLK_TO_MAPBLOCK(heapBlk);

    /* Reuse the old pinned buffer if possible */
    if BufferIsValid(*vmbuf) {
        if BufferGetBlockNumber(*vmbuf) == mapBlock {
            return;
        }

        ReleaseBuffer(*vmbuf);
    }
    *vmbuf = vm_readbuf(rel, mapBlock, true);
}

/*
 *	visibilitymap_pin_ok - do we already have the correct page pinned?
 *
 * On entry, vmbuf should be InvalidBuffer or a valid buffer returned by
 * an earlier call to visibilitymap_pin or visibilitymap_get_status on the same
 * relation.  The return value indicates whether the buffer covers the
 * given heapBlk.
 */
pub unsafe fn visibilitymap_pin_ok(heapBlk: BlockNumber, vmbuf: Buffer) -> bool {
    let mapBlock: BlockNumber = HEAPBLK_TO_MAPBLOCK(heapBlk);

    BufferIsValid(vmbuf) && BufferGetBlockNumber(vmbuf) == mapBlock
}

/*
 *	visibilitymap_set - set bit(s) on a previously pinned page
 *
 * recptr is the LSN of the XLOG record we're replaying, if we're in recovery,
 * or InvalidXLogRecPtr in normal running.  The VM page LSN is advanced to the
 * one provided; in normal running, we generate a new XLOG record and set the
 * page LSN to that value (though the heap page's LSN may *not* be updated;
 * see below).  cutoff_xid is the largest xmin on the page being marked
 * all-visible; it is needed for Hot Standby, and can be InvalidTransactionId
 * if the page contains no tuples.  It can also be set to InvalidTransactionId
 * when a page that is already all-visible is being marked all-frozen.
 *
 * Caller is expected to set the heap page's PD_ALL_VISIBLE bit before calling
 * this function. Except in recovery, caller should also pass the heap
 * buffer. When checksums are enabled and we're not in recovery, we must add
 * the heap buffer to the WAL chain to protect it from being torn.
 *
 * You must pass a buffer containing the correct map page to this function.
 * Call visibilitymap_pin first to pin the right one. This function doesn't do
 * any I/O.
 *
 * Returns the state of the page's VM bits before setting flags.
 */
pub unsafe fn visibilitymap_set(
    rel: Relation,
    heapBlk: BlockNumber,
    heapBuf: Buffer,
    mut recptr: XLogRecPtr,
    vmBuf: Buffer,
    cutoff_xid: TransactionId,
    flags: uint8,
) -> uint8 {
    let mapBlock: BlockNumber = HEAPBLK_TO_MAPBLOCK(heapBlk);
    let mapByte: uint32 = HEAPBLK_TO_MAPBYTE(heapBlk);
    let mapOffset: uint8 = HEAPBLK_TO_OFFSET(heapBlk) as uint8;
    let page: Page;
    let map: *mut uint8;
    let status: uint8;

    #[cfg(any())]
    elog!(DEBUG1, "vm_set {} {}", RelationGetRelationName(rel), heapBlk);

    Assert!(InRecovery || XLogRecPtrIsInvalid(recptr));
    Assert!(InRecovery || PageIsAllVisible(BufferGetPage(heapBuf) as Page));
    Assert!((flags & VISIBILITYMAP_VALID_BITS) == flags);

    /* Must never set all_frozen bit without also setting all_visible bit */
    Assert!(flags != VISIBILITYMAP_ALL_FROZEN);

    /* Check that we have the right heap page pinned, if present */
    if BufferIsValid(heapBuf) && BufferGetBlockNumber(heapBuf) != heapBlk {
        elog!(ERROR, "wrong heap buffer passed to visibilitymap_set");
    }

    /* Check that we have the right VM page pinned */
    if !BufferIsValid(vmBuf) || BufferGetBlockNumber(vmBuf) != mapBlock {
        elog!(ERROR, "wrong VM buffer passed to visibilitymap_set");
    }

    page = BufferGetPage(vmBuf);
    map = PageGetContents(page) as *mut uint8;
    LockBuffer(vmBuf, BUFFER_LOCK_EXCLUSIVE);

    status = (*map.add(mapByte as usize) >> mapOffset) & VISIBILITYMAP_VALID_BITS;
    if flags != status {
        START_CRIT_SECTION();

        *map.add(mapByte as usize) |= flags << mapOffset;
        MarkBufferDirty(vmBuf);

        if RelationNeedsWAL(rel) {
            if XLogRecPtrIsInvalid(recptr) {
                Assert!(!InRecovery);
                recptr = log_heap_visible(rel, heapBuf, vmBuf, cutoff_xid, flags);

                /*
                 * If data checksums are enabled (or wal_log_hints=on), we
                 * need to protect the heap page from being torn.
                 *
                 * If not, then we must *not* update the heap page's LSN. In
                 * this case, the FPI for the heap page was omitted from the
                 * WAL record inserted above, so it would be incorrect to
                 * update the heap page's LSN.
                 */
                if XLogHintBitIsNeeded() {
                    let heapPage: Page = BufferGetPage(heapBuf);

                    PageSetLSN(heapPage, recptr);
                }
            }
            PageSetLSN(page, recptr);
        }

        END_CRIT_SECTION();
    }

    LockBuffer(vmBuf, BUFFER_LOCK_UNLOCK);
    status
}

/*
 *	visibilitymap_get_status - get status of bits
 *
 * Are all tuples on heapBlk visible to all or are marked frozen, according
 * to the visibility map?
 *
 * On entry, *vmbuf should be InvalidBuffer or a valid buffer returned by an
 * earlier call to visibilitymap_pin or visibilitymap_get_status on the same
 * relation. On return, *vmbuf is a valid buffer with the map page containing
 * the bit for heapBlk, or InvalidBuffer. The caller is responsible for
 * releasing *vmbuf after it's done testing and setting bits.
 *
 * NOTE: This function is typically called without a lock on the heap page,
 * so somebody else could change the bit just after we look at it.  In fact,
 * since we don't lock the visibility map page either, it's even possible that
 * someone else could have changed the bit just before we look at it, but yet
 * we might see the old value.  It is the caller's responsibility to deal with
 * all concurrency issues!
 */
pub unsafe fn visibilitymap_get_status(
    rel: Relation,
    heapBlk: BlockNumber,
    vmbuf: *mut Buffer,
) -> uint8 {
    let mapBlock: BlockNumber = HEAPBLK_TO_MAPBLOCK(heapBlk);
    let mapByte: uint32 = HEAPBLK_TO_MAPBYTE(heapBlk);
    let mapOffset: uint8 = HEAPBLK_TO_OFFSET(heapBlk) as uint8;
    let map: *mut c_char;
    let result: uint8;

    #[cfg(any())]
    elog!(DEBUG1, "vm_get_status {} {}", RelationGetRelationName(rel), heapBlk);

    /* Reuse the old pinned buffer if possible */
    if BufferIsValid(*vmbuf) {
        if BufferGetBlockNumber(*vmbuf) != mapBlock {
            ReleaseBuffer(*vmbuf);
            *vmbuf = InvalidBuffer;
        }
    }

    if !BufferIsValid(*vmbuf) {
        *vmbuf = vm_readbuf(rel, mapBlock, false);
        if !BufferIsValid(*vmbuf) {
            return false as uint8;
        }
    }

    map = PageGetContents(BufferGetPage(*vmbuf)) as *mut c_char;

    /*
     * A single byte read is atomic.  There could be memory-ordering effects
     * here, but for performance reasons we make it the caller's job to worry
     * about that.
     */
    result = ((*map.add(mapByte as usize) as uint8) >> mapOffset) & VISIBILITYMAP_VALID_BITS;
    result
}

/*
 *	visibilitymap_count  - count number of bits set in visibility map
 *
 * Note: we ignore the possibility of race conditions when the table is being
 * extended concurrently with the call.  New pages added to the table aren't
 * going to be marked all-visible or all-frozen, so they won't affect the result.
 */
pub unsafe fn visibilitymap_count(
    rel: Relation,
    all_visible: *mut BlockNumber,
    all_frozen: *mut BlockNumber,
) {
    let mut mapBlock: BlockNumber;
    let mut nvisible: BlockNumber = 0;
    let mut nfrozen: BlockNumber = 0;

    /* all_visible must be specified */
    Assert!(!all_visible.is_null());

    mapBlock = 0;
    loop {
        let mapBuffer: Buffer;
        let map: *mut uint64;

        /*
         * Read till we fall off the end of the map.  We assume that any extra
         * bytes in the last page are zeroed, so we don't bother excluding
         * them from the count.
         */
        mapBuffer = vm_readbuf(rel, mapBlock, false);
        if !BufferIsValid(mapBuffer) {
            break;
        }

        /*
         * We choose not to lock the page, since the result is going to be
         * immediately stale anyway if anyone is concurrently setting or
         * clearing bits, and we only really need an approximate value.
         */
        map = PageGetContents(BufferGetPage(mapBuffer)) as *mut uint64;

        nvisible += pg_popcount_masked(map as *const c_char, MAPSIZE as c_int, VISIBLE_MASK8) as BlockNumber;
        if !all_frozen.is_null() {
            nfrozen += pg_popcount_masked(map as *const c_char, MAPSIZE as c_int, FROZEN_MASK8) as BlockNumber;
        }

        ReleaseBuffer(mapBuffer);

        mapBlock += 1;
    }

    *all_visible = nvisible;
    if !all_frozen.is_null() {
        *all_frozen = nfrozen;
    }
}

/*
 *	visibilitymap_prepare_truncate -
 *			prepare for truncation of the visibility map
 *
 * nheapblocks is the new size of the heap.
 *
 * Return the number of blocks of new visibility map.
 * If it's InvalidBlockNumber, there is nothing to truncate;
 * otherwise the caller is responsible for calling smgrtruncate()
 * to truncate the visibility map pages.
 */
pub unsafe fn visibilitymap_prepare_truncate(
    rel: Relation,
    nheapblocks: BlockNumber,
) -> BlockNumber {
    let newnblocks: BlockNumber;

    /* last remaining block, byte, and bit */
    let truncBlock: BlockNumber = HEAPBLK_TO_MAPBLOCK(nheapblocks);
    let truncByte: uint32 = HEAPBLK_TO_MAPBYTE(nheapblocks);
    let truncOffset: uint8 = HEAPBLK_TO_OFFSET(nheapblocks) as uint8;

    #[cfg(any())]
    elog!(DEBUG1, "vm_truncate {} {}", RelationGetRelationName(rel), nheapblocks);

    /*
     * If no visibility map has been created yet for this relation, there's
     * nothing to truncate.
     */
    if !smgrexists(RelationGetSmgr(rel), VISIBILITYMAP_FORKNUM) {
        return InvalidBlockNumber;
    }

    /*
     * Unless the new size is exactly at a visibility map page boundary, the
     * tail bits in the last remaining map page, representing truncated heap
     * blocks, need to be cleared. This is not only tidy, but also necessary
     * because we don't get a chance to clear the bits if the heap is extended
     * again.
     */
    if truncByte != 0 || truncOffset != 0 {
        let mapBuffer: Buffer;
        let page: Page;
        let map: *mut c_char;

        newnblocks = truncBlock + 1;

        mapBuffer = vm_readbuf(rel, truncBlock, false);
        if !BufferIsValid(mapBuffer) {
            /* nothing to do, the file was already smaller */
            return InvalidBlockNumber;
        }

        page = BufferGetPage(mapBuffer);
        map = PageGetContents(page) as *mut c_char;

        LockBuffer(mapBuffer, BUFFER_LOCK_EXCLUSIVE);

        /* NO EREPORT(ERROR) from here till changes are logged */
        START_CRIT_SECTION();

        /* Clear out the unwanted bytes. */
        MemSet(
            &raw mut *map.add((truncByte + 1) as usize) as *mut c_void,
            0,
            MAPSIZE - (truncByte + 1) as usize,
        );

        /*----
         * Mask out the unwanted bits of the last remaining byte.
         *
         * ((1 << 0) - 1) = 00000000
         * ((1 << 1) - 1) = 00000001
         * ...
         * ((1 << 6) - 1) = 00111111
         * ((1 << 7) - 1) = 01111111
         *----
         */
        *map.add(truncByte as usize) &= ((1u32 << truncOffset) - 1) as c_char;

        /*
         * Truncation of a relation is WAL-logged at a higher-level, and we
         * will be called at WAL replay. But if checksums are enabled, we need
         * to still write a WAL record to protect against a torn page, if the
         * page is flushed to disk before the truncation WAL record. We cannot
         * use MarkBufferDirtyHint here, because that will not dirty the page
         * during recovery.
         */
        MarkBufferDirty(mapBuffer);
        if !InRecovery && RelationNeedsWAL(rel) && XLogHintBitIsNeeded() {
            log_newpage_buffer(mapBuffer, false);
        }

        END_CRIT_SECTION();

        UnlockReleaseBuffer(mapBuffer);
    } else {
        newnblocks = truncBlock;
    }

    if smgrnblocks(RelationGetSmgr(rel), VISIBILITYMAP_FORKNUM) <= newnblocks {
        /* nothing to do, the file was already smaller than requested size */
        return InvalidBlockNumber;
    }

    newnblocks
}

/*
 * Read a visibility map page.
 *
 * If the page doesn't exist, InvalidBuffer is returned, or if 'extend' is
 * true, the visibility map file is extended.
 */
unsafe fn vm_readbuf(rel: Relation, blkno: BlockNumber, extend: bool) -> Buffer {
    let buf: Buffer;
    let reln: SMgrRelation;

    /*
     * Caution: re-using this smgr pointer could fail if the relcache entry
     * gets closed.  It's safe as long as we only do smgr-level operations
     * between here and the last use of the pointer.
     */
    reln = RelationGetSmgr(rel);

    /*
     * If we haven't cached the size of the visibility map fork yet, check it
     * first.
     */
    if (*reln).smgr_cached_nblocks[VISIBILITYMAP_FORKNUM as usize] == InvalidBlockNumber {
        if smgrexists(reln, VISIBILITYMAP_FORKNUM) {
            smgrnblocks(reln, VISIBILITYMAP_FORKNUM);
        } else {
            (*reln).smgr_cached_nblocks[VISIBILITYMAP_FORKNUM as usize] = 0;
        }
    }

    /*
     * For reading we use ZERO_ON_ERROR mode, and initialize the page if
     * necessary. It's always safe to clear bits, so it's better to clear
     * corrupt pages than error out.
     *
     * We use the same path below to initialize pages when extending the
     * relation, as a concurrent extension can end up with vm_extend()
     * returning an already-initialized page.
     */
    if blkno >= (*reln).smgr_cached_nblocks[VISIBILITYMAP_FORKNUM as usize] {
        if extend {
            buf = vm_extend(rel, blkno + 1);
        } else {
            return InvalidBuffer;
        }
    } else {
        buf = ReadBufferExtended(
            rel,
            VISIBILITYMAP_FORKNUM,
            blkno,
            RBM_ZERO_ON_ERROR,
            std::ptr::null_mut(),
        );
    }

    /*
     * Initializing the page when needed is trickier than it looks, because of
     * the possibility of multiple backends doing this concurrently, and our
     * desire to not uselessly take the buffer lock in the normal path where
     * the page is OK.  We must take the lock to initialize the page, so
     * recheck page newness after we have the lock, in case someone else
     * already did it.  Also, because we initially check PageIsNew with no
     * lock, it's possible to fall through and return the buffer while someone
     * else is still initializing the page (i.e., we might see pd_upper as set
     * but other page header fields are still zeroes).  This is harmless for
     * callers that will take a buffer lock themselves, but some callers
     * inspect the page without any lock at all.  The latter is OK only so
     * long as it doesn't depend on the page header having correct contents.
     * Current usage is safe because PageGetContents() does not require that.
     */
    if PageIsNew(BufferGetPage(buf)) {
        LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
        if PageIsNew(BufferGetPage(buf)) {
            PageInit(BufferGetPage(buf), BLCKSZ, 0);
        }
        LockBuffer(buf, BUFFER_LOCK_UNLOCK);
    }
    buf
}

/*
 * Ensure that the visibility map fork is at least vm_nblocks long, extending
 * it if necessary with zeroed pages.
 */
unsafe fn vm_extend(rel: Relation, vm_nblocks: BlockNumber) -> Buffer {
    let buf: Buffer;

    buf = ExtendBufferedRelTo(
        BMR_REL(rel),
        VISIBILITYMAP_FORKNUM,
        std::ptr::null_mut(),
        EB_CREATE_FORK_IF_NEEDED | EB_CLEAR_SIZE_CACHE,
        vm_nblocks,
        RBM_ZERO_ON_ERROR,
    );

    /*
     * Send a shared-inval message to force other backends to close any smgr
     * references they may have for this rel, which we are about to change.
     * This is a useful optimization because it means that backends don't have
     * to keep checking for creation or extension of the file, which happens
     * infrequently.
     */
    CacheInvalidateSmgr(smgr_rlocator(RelationGetSmgr(rel)));

    buf
}

// ------------------------------------------------------------------
// Types, constants, and functions pulled in from headers / other
// translation units that are not yet ported. These are minimal stubs.
// ------------------------------------------------------------------

// storage/buf.h
const InvalidBuffer: Buffer = 0; // TODO(pg-port): real InvalidBuffer lives in storage/buf.rs

// storage/bufmgr.h - buffer access modes
const BUFFER_LOCK_UNLOCK: c_int = 0; // TODO(pg-port): real BUFFER_LOCK_UNLOCK lives in storage/bufmgr.rs
const BUFFER_LOCK_EXCLUSIVE: c_int = 2; // TODO(pg-port): real BUFFER_LOCK_EXCLUSIVE lives in storage/bufmgr.rs

// storage/bufmgr.h - ReadBufferMode
const RBM_ZERO_ON_ERROR: c_int = 3;

// storage/bufmgr.h - ExtendBufferedFlags
const EB_CREATE_FORK_IF_NEEDED: c_int = 1 << 4;
const EB_CLEAR_SIZE_CACHE: c_int = 1 << 3;

// miscadmin.h
static mut InRecovery: bool = false; // TODO(pg-port): real InRecovery lives in access/xlog.rs

unsafe fn RelationGetSmgr(rel: Relation) -> SMgrRelation {
    crate::storage::buffer::bufmgr::RelationGetSmgr(rel as _) as _
}
unsafe fn RelationNeedsWAL(rel: Relation) -> bool {
    crate::access::nbtree::nbtdedup::RelationNeedsWAL(rel as _)
}
#[allow(dead_code)]
unsafe fn RelationGetRelationName(rel: Relation) -> *const c_char {
    crate::utils::rel::RelationGetRelationName(rel as _) as _
}

unsafe fn smgrexists(reln: SMgrRelation, forknum: c_int) -> bool {
    crate::storage::smgr::smgr::smgrexists(reln as _, forknum)
}
unsafe fn smgrnblocks(reln: SMgrRelation, forknum: c_int) -> BlockNumber {
    crate::storage::smgr::smgr::smgrnblocks(reln as _, forknum)
}
unsafe fn smgr_rlocator(reln: SMgrRelation) -> crate::storage::relfilelocator::RelFileLocatorBackend {
    core::mem::transmute((*(reln as *mut crate::storage::smgr::smgr::SMgrRelationData)).smgr_rlocator)
}

unsafe fn ReadBufferExtended(
    rel: Relation,
    forknum: c_int,
    blkno: BlockNumber,
    mode: c_int,
    strategy: *mut c_void,
) -> Buffer {
    crate::storage::buffer::bufmgr::ReadBufferExtended(rel as _, forknum, blkno, mode as _, strategy as _) as _
}
unsafe fn ExtendBufferedRelTo(
    bmr: *mut c_void,
    forknum: c_int,
    _strategy: *mut c_void,
    flags: c_int,
    extend_to: BlockNumber,
    _mode: c_int,
) -> Buffer {
    let bmr = *Box::from_raw(bmr as *mut crate::storage::buffer::bufmgr::BufferManagerRelation);
    crate::storage::buffer::bufmgr::ExtendBufferedRelTo(
        bmr, forknum, core::ptr::null_mut(), flags as _, extend_to, core::ptr::null_mut(),
    )
}
unsafe fn BMR_REL(rel: Relation) -> *mut c_void {
    Box::into_raw(Box::new(crate::storage::buffer::bufmgr::BMR_REL(rel as _))) as *mut c_void
}
unsafe fn ReleaseBuffer(buf: Buffer) {
    crate::storage::buffer::bufmgr::ReleaseBuffer(buf as _)
}
unsafe fn UnlockReleaseBuffer(buf: Buffer) {
    crate::storage::buffer::bufmgr::UnlockReleaseBuffer(buf as _)
}
unsafe fn LockBuffer(buf: Buffer, mode: c_int) {
    crate::storage::buffer::bufmgr::LockBuffer(buf as _, mode)
}
unsafe fn MarkBufferDirty(buf: Buffer) {
    crate::storage::buffer::bufmgr::MarkBufferDirty(buf as _)
}
unsafe fn BufferIsValid(buf: Buffer) -> bool {
    buf != InvalidBuffer
}
unsafe fn BufferGetBlockNumber(buf: Buffer) -> BlockNumber {
    crate::storage::buffer::bufmgr::BufferGetBlockNumber(buf as _)
}
unsafe fn BufferGetPage(buf: Buffer) -> Page {
    crate::storage::buffer::bufmgr::BufferGetPage(buf as _) as _
}

unsafe fn PageGetContents(page: Page) -> *mut c_void {
    crate::storage::bufpage::PageGetContents(page as _) as _
}
unsafe fn PageIsNew(page: Page) -> bool {
    crate::storage::bufpage::PageIsNew(page as _)
}
unsafe fn PageInit(page: Page, pageSize: usize, specialSize: usize) {
    crate::storage::bufpage::PageInit(page as _, pageSize, specialSize)
}
#[allow(dead_code)]
unsafe fn PageIsAllVisible(page: Page) -> bool {
    crate::storage::bufpage::PageIsAllVisible(page as _)
}
unsafe fn PageSetLSN(page: Page, lsn: XLogRecPtr) {
    crate::storage::bufpage::PageSetLSN(page as _, lsn)
}

unsafe fn CacheInvalidateSmgr(rlocator: crate::storage::relfilelocator::RelFileLocatorBackend) {
    crate::utils::cache::inval::CacheInvalidateSmgr(core::mem::transmute(rlocator))
}

unsafe fn log_heap_visible(
    rel: Relation,
    heap_buffer: Buffer,
    vm_buffer: Buffer,
    snapshotConflictHorizon: TransactionId,
    vmflags: uint8,
) -> XLogRecPtr {
    crate::access::heap::heapam::log_heap_visible(rel as _, heap_buffer as _, vm_buffer as _, snapshotConflictHorizon, vmflags)
}
unsafe fn log_newpage_buffer(buf: Buffer, page_std: bool) -> XLogRecPtr {
    crate::access::transam::xloginsert::log_newpage_buffer(buf as _, page_std)
}

unsafe fn XLogHintBitIsNeeded() -> bool {
    false // access/xlog.h unwired; gates hint-bit WAL, false is safe
}

fn START_CRIT_SECTION() {
    // TODO(pg-port): real START_CRIT_SECTION lives in miscadmin.h
}
fn END_CRIT_SECTION() {
    // TODO(pg-port): real END_CRIT_SECTION lives in miscadmin.h
}

#[inline(always)]
unsafe fn MemSet(start: *mut c_void, val: c_int, len: usize) {
    // TODO(pg-port): real MemSet lives in c.h
    std::ptr::write_bytes(start as *mut u8, val as u8, len);
}
