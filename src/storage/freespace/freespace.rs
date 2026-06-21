//! src/backend/storage/freespace/freespace.c
//!
//! POSTGRES free space map for quickly finding free space in relations
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! NOTES:
//!
//!	Free Space Map keeps track of the amount of free space on pages, and
//!	allows quickly searching for a page with enough free space. The FSM is
//!	stored in a dedicated relation fork of all heap relations, and those
//!	index access methods that need it (see also indexfsm.c). See README for
//!	more information.

use crate::prelude::*;

use std::ffi::c_int;

use crate::c::uint16;
use crate::c::uint8;
use crate::miscadmin::CHECK_FOR_INTERRUPTS;
use crate::pg_config::BLCKSZ;
use crate::storage::block::BlockNumber;
use crate::storage::block::BlockNumberIsValid;
use crate::storage::block::InvalidBlockNumber;

// ------------------------------------------------------------------
// Types and constants pulled in from headers that are not yet ported.
// ------------------------------------------------------------------

// from storage/relfilelocator.h
type RelFileLocator = crate::storage::relfilelocator::RelFileLocator;

// from utils/relcache.h
type Relation = *mut crate::utils::rel::RelationData;

// from storage/buf.h / storage/bufmgr.h
type Buffer = c_int;
type Page = crate::storage::bufpage::Page;

// from storage/smgr.h
// Alias the canonical SMgrRelationData so that smgr_cached_nblocks[] field
// accesses land at the correct offsets for the pointer RelationGetSmgr returns.
type SMgrRelation = *mut SMgrRelationData;
type SMgrRelationData = crate::storage::smgr::smgr::SMgrRelationData;

// from storage/fsm_internals.h
type FSMPage = *mut crate::storage::freespace::fsmpage::FSMPageData;

/*
 * We use just one byte to store the amount of free space on a page, so we
 * divide the amount of free space a page can have into 256 different
 * categories. The highest category, 255, represents a page with at least
 * MaxFSMRequestSize bytes of free space, and the second highest category
 * represents the range from 254 * FSM_CAT_STEP, inclusive, to
 * MaxFSMRequestSize, exclusive.
 *
 * MaxFSMRequestSize depends on the architecture and BLCKSZ, but assuming
 * default 8k BLCKSZ, and that MaxFSMRequestSize is 8164 bytes, the
 * categories look like this:
 *
 *
 * Range	 Category
 * 0	- 31   0
 * 32	- 63   1
 * ...    ...  ...
 * 8096 - 8127 253
 * 8128 - 8163 254
 * 8164 - 8192 255
 *
 * The reason that MaxFSMRequestSize is special is that if MaxFSMRequestSize
 * isn't equal to a range boundary, a page with exactly MaxFSMRequestSize
 * bytes of free space wouldn't satisfy a request for MaxFSMRequestSize
 * bytes. If there isn't more than MaxFSMRequestSize bytes of free space on a
 * completely empty page, that would mean that we could never satisfy a
 * request of exactly MaxFSMRequestSize bytes.
 */
const FSM_CATEGORIES: usize = 256;
const FSM_CAT_STEP: Size = (BLCKSZ / FSM_CATEGORIES) as Size;
const MaxFSMRequestSize: Size = MaxHeapTupleSize as Size;

/*
 * Depth of the on-disk tree. We need to be able to address 2^32-1 blocks,
 * and 1626 is the smallest number that satisfies X^3 >= 2^32-1. Likewise,
 * 256 is the smallest number that satisfies X^4 >= 2^32-1. In practice,
 * this means that 4096 bytes is the smallest BLCKSZ that we can get away
 * with a 3-level tree, and 512 is the smallest we support.
 */
const FSM_TREE_DEPTH: c_int = if SlotsPerFSMPage >= 1626 { 3 } else { 4 };

const FSM_ROOT_LEVEL: c_int = FSM_TREE_DEPTH - 1;
const FSM_BOTTOM_LEVEL: c_int = 0;

/*
 * The internal FSM routines work on a logical addressing scheme. Each
 * level of the tree can be thought of as a separately addressable file.
 */
#[derive(Clone, Copy)]
struct FSMAddress {
    level: c_int,      /* level */
    logpageno: c_int,  /* page number within the level */
}

/* Address of the root page. */
const FSM_ROOT_ADDRESS: FSMAddress = FSMAddress {
    level: FSM_ROOT_LEVEL,
    logpageno: 0,
};

/******** Public API ********/

/*
 * GetPageWithFreeSpace - try to find a page in the given relation with
 *		at least the specified amount of free space.
 *
 * If successful, return the block number; if not, return InvalidBlockNumber.
 *
 * The caller must be prepared for the possibility that the returned page
 * will turn out to have too little space available by the time the caller
 * gets a lock on it.  In that case, the caller should report the actual
 * amount of free space available on that page and then try again (see
 * RecordAndGetPageWithFreeSpace).  If InvalidBlockNumber is returned,
 * extend the relation.
 *
 * This can trigger FSM updates if any FSM entry is found to point to a block
 * past the end of the relation.
 */
pub unsafe fn GetPageWithFreeSpace(rel: Relation, spaceNeeded: Size) -> BlockNumber {
    let min_cat: uint8 = fsm_space_needed_to_cat(spaceNeeded);

    fsm_search(rel, min_cat)
}

/*
 * RecordAndGetPageWithFreeSpace - update info about a page and try again.
 *
 * We provide this combo form to save some locking overhead, compared to
 * separate RecordPageWithFreeSpace + GetPageWithFreeSpace calls. There's
 * also some effort to return a page close to the old page; if there's a
 * page with enough free space on the same FSM page where the old one page
 * is located, it is preferred.
 */
pub unsafe fn RecordAndGetPageWithFreeSpace(
    rel: Relation,
    oldPage: BlockNumber,
    oldSpaceAvail: Size,
    spaceNeeded: Size,
) -> BlockNumber {
    let old_cat: c_int = fsm_space_avail_to_cat(oldSpaceAvail) as c_int;
    let search_cat: c_int = fsm_space_needed_to_cat(spaceNeeded) as c_int;
    let addr: FSMAddress;
    let mut slot: uint16 = 0;
    let search_slot: c_int;

    /* Get the location of the FSM byte representing the heap block */
    addr = fsm_get_location(oldPage, &mut slot);

    search_slot = fsm_set_and_search(rel, addr, slot, old_cat as uint8, search_cat as uint8);

    /*
     * If fsm_set_and_search found a suitable new block, return that.
     * Otherwise, search as usual.
     */
    if search_slot != -1 {
        let blknum: BlockNumber = fsm_get_heap_blk(addr, search_slot as uint16);

        /*
         * Check that the blknum is actually in the relation. Don't try to
         * update the FSM in that case, just fall back to the other case
         */
        if fsm_does_block_exist(rel, blknum) {
            return blknum;
        }
    }
    fsm_search(rel, search_cat as uint8)
}

/*
 * RecordPageWithFreeSpace - update info about a page.
 *
 * Note that if the new spaceAvail value is higher than the old value stored
 * in the FSM, the space might not become visible to searchers until the next
 * FreeSpaceMapVacuum call, which updates the upper level pages.
 */
pub unsafe fn RecordPageWithFreeSpace(rel: Relation, heapBlk: BlockNumber, spaceAvail: Size) {
    let new_cat: c_int = fsm_space_avail_to_cat(spaceAvail) as c_int;
    let addr: FSMAddress;
    let mut slot: uint16 = 0;

    /* Get the location of the FSM byte representing the heap block */
    addr = fsm_get_location(heapBlk, &mut slot);

    fsm_set_and_search(rel, addr, slot, new_cat as uint8, 0);
}

/*
 * XLogRecordPageWithFreeSpace - like RecordPageWithFreeSpace, for use in
 *		WAL replay
 */
pub unsafe fn XLogRecordPageWithFreeSpace(
    rlocator: RelFileLocator,
    heapBlk: BlockNumber,
    spaceAvail: Size,
) {
    let new_cat: c_int = fsm_space_avail_to_cat(spaceAvail) as c_int;
    let addr: FSMAddress;
    let mut slot: uint16 = 0;
    let blkno: BlockNumber;
    let buf: Buffer;
    let page: Page;

    /* Get the location of the FSM byte representing the heap block */
    addr = fsm_get_location(heapBlk, &mut slot);
    blkno = fsm_logical_to_physical(addr);

    /* If the page doesn't exist already, extend */
    buf = XLogReadBufferExtended(
        rlocator,
        FSM_FORKNUM,
        blkno,
        RBM_ZERO_ON_ERROR,
        InvalidBuffer,
    );
    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

    page = BufferGetPage(buf);
    if PageIsNew(page) {
        PageInit(page, BLCKSZ, 0);
    }

    if fsm_set_avail(page, slot as c_int, new_cat as uint8) {
        MarkBufferDirtyHint(buf, false);
    }
    UnlockReleaseBuffer(buf);
}

/*
 * GetRecordedFreeSpace - return the amount of free space on a particular page,
 *		according to the FSM.
 */
pub unsafe fn GetRecordedFreeSpace(rel: Relation, heapBlk: BlockNumber) -> Size {
    let addr: FSMAddress;
    let mut slot: uint16 = 0;
    let buf: Buffer;
    let cat: uint8;

    /* Get the location of the FSM byte representing the heap block */
    addr = fsm_get_location(heapBlk, &mut slot);

    buf = fsm_readbuf(rel, addr, false);
    if !BufferIsValid(buf) {
        return 0;
    }
    cat = fsm_get_avail(BufferGetPage(buf), slot as c_int);
    ReleaseBuffer(buf);

    fsm_space_cat_to_avail(cat)
}

/*
 * FreeSpaceMapPrepareTruncateRel - prepare for truncation of a relation.
 *
 * nblocks is the new size of the heap.
 *
 * Return the number of blocks of new FSM.
 * If it's InvalidBlockNumber, there is nothing to truncate;
 * otherwise the caller is responsible for calling smgrtruncate()
 * to truncate the FSM pages, and FreeSpaceMapVacuumRange()
 * to update upper-level pages in the FSM.
 */
pub unsafe fn FreeSpaceMapPrepareTruncateRel(rel: Relation, nblocks: BlockNumber) -> BlockNumber {
    let new_nfsmblocks: BlockNumber;
    let first_removed_address: FSMAddress;
    let mut first_removed_slot: uint16 = 0;
    let buf: Buffer;

    /*
     * If no FSM has been created yet for this relation, there's nothing to
     * truncate.
     */
    if !smgrexists(RelationGetSmgr(rel), FSM_FORKNUM) {
        return InvalidBlockNumber;
    }

    /* Get the location in the FSM of the first removed heap block */
    first_removed_address = fsm_get_location(nblocks, &mut first_removed_slot);

    /*
     * Zero out the tail of the last remaining FSM page. If the slot
     * representing the first removed heap block is at a page boundary, as the
     * first slot on the FSM page that first_removed_address points to, we can
     * just truncate that page altogether.
     */
    if first_removed_slot > 0 {
        buf = fsm_readbuf(rel, first_removed_address, false);
        if !BufferIsValid(buf) {
            return InvalidBlockNumber; /* nothing to do; the FSM was already
                                        * smaller */
        }
        LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

        /* NO EREPORT(ERROR) from here till changes are logged */
        START_CRIT_SECTION();

        fsm_truncate_avail(BufferGetPage(buf), first_removed_slot as c_int);

        /*
         * This change is non-critical, because fsm_does_block_exist() would
         * stop us from returning a truncated-away block.  However, since this
         * may remove up to SlotsPerFSMPage slots, it's nice to avoid the cost
         * of that many fsm_does_block_exist() rejections.  Use a full
         * MarkBufferDirty(), not MarkBufferDirtyHint().
         */
        MarkBufferDirty(buf);

        /*
         * WAL-log like MarkBufferDirtyHint() might have done, just to avoid
         * differing from the rest of the file in this respect.  This is
         * optional; see README mention of full page images.  XXX consider
         * XLogSaveBufferForHint() for even closer similarity.
         *
         * A higher-level operation calls us at WAL replay.  If we crash
         * before the XLOG_SMGR_TRUNCATE flushes to disk, main fork length has
         * not changed, and our fork remains valid.  If we crash after that
         * flush, redo will return here.
         */
        if !InRecovery && RelationNeedsWAL(rel) && XLogHintBitIsNeeded() {
            log_newpage_buffer(buf, false);
        }

        END_CRIT_SECTION();

        UnlockReleaseBuffer(buf);

        new_nfsmblocks = fsm_logical_to_physical(first_removed_address) + 1;
    } else {
        new_nfsmblocks = fsm_logical_to_physical(first_removed_address);
        if smgrnblocks(RelationGetSmgr(rel), FSM_FORKNUM) <= new_nfsmblocks {
            return InvalidBlockNumber; /* nothing to do; the FSM was already
                                        * smaller */
        }
    }

    new_nfsmblocks
}

/*
 * FreeSpaceMapVacuum - update upper-level pages in the rel's FSM
 *
 * We assume that the bottom-level pages have already been updated with
 * new free-space information.
 */
pub unsafe fn FreeSpaceMapVacuum(rel: Relation) {
    let mut dummy: bool = false;

    /* Recursively scan the tree, starting at the root */
    fsm_vacuum_page(
        rel,
        FSM_ROOT_ADDRESS,
        0 as BlockNumber,
        InvalidBlockNumber,
        &mut dummy,
    );
}

/*
 * FreeSpaceMapVacuumRange - update upper-level pages in the rel's FSM
 *
 * As above, but assume that only heap pages between start and end-1 inclusive
 * have new free-space information, so update only the upper-level slots
 * covering that block range.  end == InvalidBlockNumber is equivalent to
 * "all the rest of the relation".
 */
pub unsafe fn FreeSpaceMapVacuumRange(rel: Relation, start: BlockNumber, end: BlockNumber) {
    let mut dummy: bool = false;

    /* Recursively scan the tree, starting at the root */
    if end > start {
        fsm_vacuum_page(rel, FSM_ROOT_ADDRESS, start, end, &mut dummy);
    }
}

/******** Internal routines ********/

/*
 * Return category corresponding x bytes of free space
 */
unsafe fn fsm_space_avail_to_cat(avail: Size) -> uint8 {
    let mut cat: c_int;

    Assert!(avail < BLCKSZ as Size);

    if avail >= MaxFSMRequestSize {
        return 255;
    }

    cat = (avail / FSM_CAT_STEP) as c_int;

    /*
     * The highest category, 255, is reserved for MaxFSMRequestSize bytes or
     * more.
     */
    if cat > 254 {
        cat = 254;
    }

    cat as uint8
}

/*
 * Return the lower bound of the range of free space represented by given
 * category.
 */
unsafe fn fsm_space_cat_to_avail(cat: uint8) -> Size {
    /* The highest category represents exactly MaxFSMRequestSize bytes. */
    if cat == 255 {
        MaxFSMRequestSize
    } else {
        cat as Size * FSM_CAT_STEP
    }
}

/*
 * Which category does a page need to have, to accommodate x bytes of data?
 * While fsm_space_avail_to_cat() rounds down, this needs to round up.
 */
unsafe fn fsm_space_needed_to_cat(needed: Size) -> uint8 {
    let mut cat: c_int;

    /* Can't ask for more space than the highest category represents */
    if needed > MaxFSMRequestSize {
        elog!(ERROR, "invalid FSM request size {}", needed);
    }

    if needed == 0 {
        return 1;
    }

    cat = ((needed + FSM_CAT_STEP - 1) / FSM_CAT_STEP) as c_int;

    if cat > 255 {
        cat = 255;
    }

    cat as uint8
}

/*
 * Returns the physical block number of a FSM page
 */
unsafe fn fsm_logical_to_physical(addr: FSMAddress) -> BlockNumber {
    let mut pages: BlockNumber;
    let mut leafno: c_int;
    let mut l: c_int;

    /*
     * Calculate the logical page number of the first leaf page below the
     * given page.
     */
    leafno = addr.logpageno;
    l = 0;
    while l < addr.level {
        leafno *= SlotsPerFSMPage as c_int;
        l += 1;
    }

    /* Count upper level nodes required to address the leaf page */
    pages = 0;
    l = 0;
    while l < FSM_TREE_DEPTH {
        pages += (leafno + 1) as BlockNumber;
        leafno /= SlotsPerFSMPage as c_int;
        l += 1;
    }

    /*
     * If the page we were asked for wasn't at the bottom level, subtract the
     * additional lower level pages we counted above.
     */
    pages -= addr.level as BlockNumber;

    /* Turn the page count into 0-based block number */
    pages - 1
}

/*
 * Return the FSM location corresponding to given heap block.
 */
unsafe fn fsm_get_location(heapblk: BlockNumber, slot: *mut uint16) -> FSMAddress {
    let mut addr: FSMAddress = FSMAddress { level: 0, logpageno: 0 };

    addr.level = FSM_BOTTOM_LEVEL;
    addr.logpageno = (heapblk / SlotsPerFSMPage as BlockNumber) as c_int;
    *slot = (heapblk % SlotsPerFSMPage as BlockNumber) as uint16;

    addr
}

/*
 * Return the heap block number corresponding to given location in the FSM.
 */
unsafe fn fsm_get_heap_blk(addr: FSMAddress, slot: uint16) -> BlockNumber {
    Assert!(addr.level == FSM_BOTTOM_LEVEL);
    (addr.logpageno as u32) * SlotsPerFSMPage as BlockNumber + slot as BlockNumber
}

/*
 * Given a logical address of a child page, get the logical page number of
 * the parent, and the slot within the parent corresponding to the child.
 */
unsafe fn fsm_get_parent(child: FSMAddress, slot: *mut uint16) -> FSMAddress {
    let mut parent: FSMAddress = FSMAddress { level: 0, logpageno: 0 };

    Assert!(child.level < FSM_ROOT_LEVEL);

    parent.level = child.level + 1;
    parent.logpageno = child.logpageno / SlotsPerFSMPage as c_int;
    *slot = (child.logpageno % SlotsPerFSMPage as c_int) as uint16;

    parent
}

/*
 * Given a logical address of a parent page and a slot number, get the
 * logical address of the corresponding child page.
 */
unsafe fn fsm_get_child(parent: FSMAddress, slot: uint16) -> FSMAddress {
    let mut child: FSMAddress = FSMAddress { level: 0, logpageno: 0 };

    Assert!(parent.level > FSM_BOTTOM_LEVEL);

    child.level = parent.level - 1;
    child.logpageno = parent.logpageno * SlotsPerFSMPage as c_int + slot as c_int;

    child
}

/*
 * Read a FSM page.
 *
 * If the page doesn't exist, InvalidBuffer is returned, or if 'extend' is
 * true, the FSM file is extended.
 */
unsafe fn fsm_readbuf(rel: Relation, addr: FSMAddress, extend: bool) -> Buffer {
    let blkno: BlockNumber = fsm_logical_to_physical(addr);
    let buf: Buffer;
    let reln: SMgrRelation = RelationGetSmgr(rel);

    /*
     * If we haven't cached the size of the FSM yet, check it first.  Also
     * recheck if the requested block seems to be past end, since our cached
     * value might be stale.  (We send smgr inval messages on truncation, but
     * not on extension.)
     */
    if (*reln).smgr_cached_nblocks[FSM_FORKNUM as usize] == InvalidBlockNumber
        || blkno >= (*reln).smgr_cached_nblocks[FSM_FORKNUM as usize]
    {
        /* Invalidate the cache so smgrnblocks asks the kernel. */
        (*reln).smgr_cached_nblocks[FSM_FORKNUM as usize] = InvalidBlockNumber;
        if smgrexists(reln, FSM_FORKNUM) {
            smgrnblocks(reln, FSM_FORKNUM);
        } else {
            (*reln).smgr_cached_nblocks[FSM_FORKNUM as usize] = 0;
        }
    }

    /*
     * For reading we use ZERO_ON_ERROR mode, and initialize the page if
     * necessary.  The FSM information is not accurate anyway, so it's better
     * to clear corrupt pages than error out. Since the FSM changes are not
     * WAL-logged, the so-called torn page problem on crash can lead to pages
     * with corrupt headers, for example.
     *
     * We use the same path below to initialize pages when extending the
     * relation, as a concurrent extension can end up with vm_extend()
     * returning an already-initialized page.
     */
    if blkno >= (*reln).smgr_cached_nblocks[FSM_FORKNUM as usize] {
        if extend {
            buf = fsm_extend(rel, blkno + 1);
        } else {
            return InvalidBuffer;
        }
    } else {
        buf = ReadBufferExtended(
            rel,
            FSM_FORKNUM,
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
 * Ensure that the FSM fork is at least fsm_nblocks long, extending
 * it if necessary with empty pages. And by empty, I mean pages filled
 * with zeros, meaning there's no free space.
 */
unsafe fn fsm_extend(rel: Relation, fsm_nblocks: BlockNumber) -> Buffer {
    ExtendBufferedRelTo(
        BMR_REL(rel),
        FSM_FORKNUM,
        std::ptr::null_mut(),
        EB_CREATE_FORK_IF_NEEDED | EB_CLEAR_SIZE_CACHE,
        fsm_nblocks,
        RBM_ZERO_ON_ERROR,
    )
}

/*
 * Set value in given FSM page and slot.
 *
 * If minValue > 0, the updated page is also searched for a page with at
 * least minValue of free space. If one is found, its slot number is
 * returned, -1 otherwise.
 */
unsafe fn fsm_set_and_search(
    rel: Relation,
    addr: FSMAddress,
    slot: uint16,
    newValue: uint8,
    minValue: uint8,
) -> c_int {
    let buf: Buffer;
    let page: Page;
    let mut newslot: c_int = -1;

    buf = fsm_readbuf(rel, addr, true);
    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

    page = BufferGetPage(buf);

    if fsm_set_avail(page, slot as c_int, newValue) {
        MarkBufferDirtyHint(buf, false);
    }

    if minValue != 0 {
        /* Search while we still hold the lock */
        newslot = fsm_search_avail(buf, minValue, addr.level == FSM_BOTTOM_LEVEL, true);
    }

    UnlockReleaseBuffer(buf);

    newslot
}

/*
 * Search the tree for a heap page with at least min_cat of free space
 */
#[allow(unused_mut)]
unsafe fn fsm_search(rel: Relation, min_cat: uint8) -> BlockNumber {
    let mut restarts: c_int = 0;
    let mut addr: FSMAddress = FSM_ROOT_ADDRESS;

    loop {
        let mut slot: c_int;
        let buf: Buffer;
        let mut max_avail: uint8 = 0;

        /* Read the FSM page. */
        buf = fsm_readbuf(rel, addr, false);

        /* Search within the page */
        if BufferIsValid(buf) {
            LockBuffer(buf, BUFFER_LOCK_SHARE);
            slot = fsm_search_avail(buf, min_cat, addr.level == FSM_BOTTOM_LEVEL, false);
            if slot == -1 {
                max_avail = fsm_get_max_avail(BufferGetPage(buf));
                UnlockReleaseBuffer(buf);
            } else {
                /* Keep the pin for possible update below */
                LockBuffer(buf, BUFFER_LOCK_UNLOCK);
            }
        } else {
            slot = -1;
        }

        if slot != -1 {
            /*
             * Descend the tree, or return the found block if we're at the
             * bottom.
             */
            if addr.level == FSM_BOTTOM_LEVEL {
                let blkno: BlockNumber = fsm_get_heap_blk(addr, slot as uint16);
                let page: Page;

                if fsm_does_block_exist(rel, blkno) {
                    ReleaseBuffer(buf);
                    return blkno;
                }

                /*
                 * Block is past the end of the relation.  Update FSM, and
                 * restart from root.  The usual "advancenext" behavior is
                 * pessimal for this rare scenario, since every later slot is
                 * unusable in the same way.  We could zero all affected slots
                 * on the same FSM page, but don't bet on the benefits of that
                 * optimization justifying its compiled code bulk.
                 */
                page = BufferGetPage(buf);
                LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
                fsm_set_avail(page, slot, 0);
                MarkBufferDirtyHint(buf, false);
                UnlockReleaseBuffer(buf);
                restarts += 1;
                if restarts > 10000 {
                    /* same rationale as below */
                    return InvalidBlockNumber;
                }
                addr = FSM_ROOT_ADDRESS;
            } else {
                ReleaseBuffer(buf);
            }
            addr = fsm_get_child(addr, slot as uint16);
        } else if addr.level == FSM_ROOT_LEVEL {
            /*
             * At the root, failure means there's no page with enough free
             * space in the FSM. Give up.
             */
            return InvalidBlockNumber;
        } else {
            let mut parentslot: uint16 = 0;
            let parent: FSMAddress;

            /*
             * At lower level, failure can happen if the value in the upper-
             * level node didn't reflect the value on the lower page. Update
             * the upper node, to avoid falling into the same trap again, and
             * start over.
             *
             * There's a race condition here, if another backend updates this
             * page right after we release it, and gets the lock on the parent
             * page before us. We'll then update the parent page with the now
             * stale information we had. It's OK, because it should happen
             * rarely, and will be fixed by the next vacuum.
             */
            parent = fsm_get_parent(addr, &mut parentslot);
            fsm_set_and_search(rel, parent, parentslot, max_avail, 0);

            /*
             * If the upper pages are badly out of date, we might need to loop
             * quite a few times, updating them as we go. Any inconsistencies
             * should eventually be corrected and the loop should end. Looping
             * indefinitely is nevertheless scary, so provide an emergency
             * valve.
             */
            restarts += 1;
            if restarts > 10000 {
                return InvalidBlockNumber;
            }

            /* Start search all over from the root */
            addr = FSM_ROOT_ADDRESS;
        }
    }
}

/*
 * Recursive guts of FreeSpaceMapVacuum
 *
 * Examine the FSM page indicated by addr, as well as its children, updating
 * upper-level nodes that cover the heap block range from start to end-1.
 * (It's okay if end is beyond the actual end of the map.)
 * Return the maximum freespace value on this page.
 *
 * If addr is past the end of the FSM, set *eof_p to true and return 0.
 *
 * This traverses the tree in depth-first order.  The tree is stored
 * physically in depth-first order, so this should be pretty I/O efficient.
 */
unsafe fn fsm_vacuum_page(
    rel: Relation,
    addr: FSMAddress,
    start: BlockNumber,
    end: BlockNumber,
    eof_p: *mut bool,
) -> uint8 {
    let buf: Buffer;
    let page: Page;
    let max_avail: uint8;

    /* Read the page if it exists, or return EOF */
    buf = fsm_readbuf(rel, addr, false);
    if !BufferIsValid(buf) {
        *eof_p = true;
        return 0;
    } else {
        *eof_p = false;
    }

    page = BufferGetPage(buf);

    /*
     * If we're above the bottom level, recurse into children, and fix the
     * information stored about them at this level.
     */
    if addr.level > FSM_BOTTOM_LEVEL {
        let mut fsm_start: FSMAddress;
        let mut fsm_end: FSMAddress;
        let mut fsm_start_slot: uint16 = 0;
        let mut fsm_end_slot: uint16 = 0;
        let mut slot: c_int;
        let start_slot: c_int;
        let end_slot: c_int;
        let mut eof: bool = false;

        /*
         * Compute the range of slots we need to update on this page, given
         * the requested range of heap blocks to consider.  The first slot to
         * update is the one covering the "start" block, and the last slot is
         * the one covering "end - 1".  (Some of this work will be duplicated
         * in each recursive call, but it's cheap enough to not worry about.)
         */
        fsm_start = fsm_get_location(start, &mut fsm_start_slot);
        fsm_end = fsm_get_location(end - 1, &mut fsm_end_slot);

        while fsm_start.level < addr.level {
            fsm_start = fsm_get_parent(fsm_start, &mut fsm_start_slot);
            fsm_end = fsm_get_parent(fsm_end, &mut fsm_end_slot);
        }
        Assert!(fsm_start.level == addr.level);

        if fsm_start.logpageno == addr.logpageno {
            start_slot = fsm_start_slot as c_int;
        } else if fsm_start.logpageno > addr.logpageno {
            start_slot = SlotsPerFSMPage as c_int; /* shouldn't get here... */
        } else {
            start_slot = 0;
        }

        if fsm_end.logpageno == addr.logpageno {
            end_slot = fsm_end_slot as c_int;
        } else if fsm_end.logpageno > addr.logpageno {
            end_slot = SlotsPerFSMPage as c_int - 1;
        } else {
            end_slot = -1; /* shouldn't get here... */
        }

        slot = start_slot;
        while slot <= end_slot {
            let child_avail: c_int;

            CHECK_FOR_INTERRUPTS();

            /* After we hit end-of-file, just clear the rest of the slots */
            if !eof {
                child_avail =
                    fsm_vacuum_page(rel, fsm_get_child(addr, slot as uint16), start, end, &mut eof)
                        as c_int;
            } else {
                child_avail = 0;
            }

            /* Update information about the child */
            if fsm_get_avail(page, slot) as c_int != child_avail {
                LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
                fsm_set_avail(page, slot, child_avail as uint8);
                MarkBufferDirtyHint(buf, false);
                LockBuffer(buf, BUFFER_LOCK_UNLOCK);
            }

            slot += 1;
        }
    }

    /* Now get the maximum value on the page, to return to caller */
    max_avail = fsm_get_max_avail(page);

    /*
     * Reset the next slot pointer. This encourages the use of low-numbered
     * pages, increasing the chances that a later vacuum can truncate the
     * relation.  We don't bother with a lock here, nor with marking the page
     * dirty if it wasn't already, since this is just a hint.
     */
    (*(PageGetContents(page) as FSMPage)).fp_next_slot = 0;

    ReleaseBuffer(buf);

    max_avail
}

/*
 * Check whether a block number is past the end of the relation.  This can
 * happen after WAL replay, if the FSM reached disk but newly-extended pages
 * it refers to did not.
 */
unsafe fn fsm_does_block_exist(rel: Relation, blknumber: BlockNumber) -> bool {
    let smgr: SMgrRelation = RelationGetSmgr(rel);

    /*
     * If below the cached nblocks, the block surely exists.  Otherwise, we
     * face a trade-off.  We opt to compare to a fresh nblocks, incurring
     * lseek() overhead.  The alternative would be to assume the block does
     * not exist, but that would cause FSM to set zero space available for
     * blocks that main fork extension just recorded.
     */
    (BlockNumberIsValid((*smgr).smgr_cached_nblocks[MAIN_FORKNUM as usize])
        && blknumber < (*smgr).smgr_cached_nblocks[MAIN_FORKNUM as usize])
        || blknumber < RelationGetNumberOfBlocks(rel)
}

// ------------------------------------------------------------------
// Local stubs for helpers not yet ported.
// ------------------------------------------------------------------

const InvalidBuffer: Buffer = 0;
const BUFFER_LOCK_UNLOCK: c_int = 0;
const BUFFER_LOCK_SHARE: c_int = 1;
const BUFFER_LOCK_EXCLUSIVE: c_int = 2;

// storage/relfilelocator.h / storage/bufmgr.h fork numbers and ReadBufferMode
const FSM_FORKNUM: c_int = 1;
const MAIN_FORKNUM: c_int = 0;
const RBM_ZERO_ON_ERROR: c_int = 3;

// storage/bufmgr.h ExtendBufferedFlags
const EB_CREATE_FORK_IF_NEEDED: c_int = 1 << 2;
const EB_CLEAR_SIZE_CACHE: c_int = 1 << 4;

// access/htup_details.h
const MaxHeapTupleSize: usize = BLCKSZ - 24; // MAXALIGN(SizeOfPageHeaderData)

// storage/fsm_internals.h
const SlotsPerFSMPage: usize = 4096; // (BLCKSZ - MAXALIGN(SizeOfPageHeaderData) - offsetof) leaves; placeholder

unsafe fn RelationGetSmgr(rel: Relation) -> SMgrRelation {
    crate::storage::buffer::bufmgr::RelationGetSmgr(rel as _) as _
}
unsafe fn RelationGetNumberOfBlocks(rel: Relation) -> BlockNumber {
    crate::storage::buffer::bufmgr::RelationGetNumberOfBlocksInFork(rel as _, MAIN_FORKNUM)
}
unsafe fn RelationNeedsWAL(rel: Relation) -> bool {
    crate::access::nbtree::nbtdedup::RelationNeedsWAL(rel as _)
}

unsafe fn smgrexists(reln: SMgrRelation, forknum: c_int) -> bool {
    crate::storage::smgr::smgr::smgrexists(reln as _, forknum)
}
unsafe fn smgrnblocks(reln: SMgrRelation, forknum: c_int) -> BlockNumber {
    crate::storage::smgr::smgr::smgrnblocks(reln as _, forknum)
}

unsafe fn ReadBufferExtended(
    rel: Relation,
    forknum: c_int,
    blkno: BlockNumber,
    mode: c_int,
    strategy: *mut std::ffi::c_void,
) -> Buffer {
    crate::storage::buffer::bufmgr::ReadBufferExtended(rel as _, forknum, blkno, mode as _, strategy as _)
}
unsafe fn ExtendBufferedRelTo(
    bmr: *mut std::ffi::c_void,
    forknum: c_int,
    _strategy: *mut std::ffi::c_void,
    flags: c_int,
    extend_to: BlockNumber,
    _mode: c_int,
) -> Buffer {
    // bmr is a heap-boxed BufferManagerRelation produced by BMR_REL below.
    let bmr = *Box::from_raw(bmr as *mut crate::storage::buffer::bufmgr::BufferManagerRelation);
    crate::storage::buffer::bufmgr::ExtendBufferedRelTo(
        bmr,
        forknum,
        std::ptr::null_mut(),
        flags as _,
        extend_to,
        std::ptr::null_mut(),
    )
}
unsafe fn BMR_REL(rel: Relation) -> *mut std::ffi::c_void {
    let bmr = crate::storage::buffer::bufmgr::BMR_REL(rel as _);
    Box::into_raw(Box::new(bmr)) as *mut std::ffi::c_void
}
unsafe fn XLogReadBufferExtended(
    rlocator: RelFileLocator,
    forknum: c_int,
    blkno: BlockNumber,
    mode: c_int,
    recent_buffer: Buffer,
) -> Buffer {
    crate::access::transam::xlogutils::XLogReadBufferExtended(core::mem::transmute(rlocator), forknum, blkno, mode as _, recent_buffer)
}
unsafe fn ReleaseBuffer(buf: Buffer) {
    crate::storage::buffer::bufmgr::ReleaseBuffer(buf)
}
unsafe fn UnlockReleaseBuffer(buf: Buffer) {
    crate::storage::buffer::bufmgr::UnlockReleaseBuffer(buf)
}
unsafe fn LockBuffer(buf: Buffer, mode: c_int) {
    crate::storage::buffer::bufmgr::LockBuffer(buf, mode)
}
unsafe fn MarkBufferDirty(buf: Buffer) {
    crate::storage::buffer::bufmgr::MarkBufferDirty(buf)
}
unsafe fn MarkBufferDirtyHint(buf: Buffer, buffer_std: bool) {
    crate::storage::buffer::bufmgr::MarkBufferDirtyHint(buf, buffer_std)
}
unsafe fn BufferIsValid(buf: Buffer) -> bool {
    buf != 0 /* BufferIsValid */
}
unsafe fn BufferGetPage(buf: Buffer) -> Page {
    crate::storage::buffer::bufmgr::BufferGetPage(buf) as _
}

unsafe fn PageIsNew(page: Page) -> bool {
    crate::storage::bufpage::PageIsNew(page as _)
}
unsafe fn PageInit(page: Page, pageSize: usize, specialSize: usize) {
    crate::storage::bufpage::PageInit(page as _, pageSize as _, specialSize as _)
}
unsafe fn PageGetContents(page: Page) -> *mut std::ffi::c_void {
    crate::storage::bufpage::PageGetContents(page as _) as _
}

unsafe fn fsm_set_avail(page: Page, slot: c_int, value: uint8) -> bool {
    crate::storage::freespace::fsmpage::fsm_set_avail(page as _, slot, value)
}
unsafe fn fsm_get_avail(page: Page, slot: c_int) -> uint8 {
    crate::storage::freespace::fsmpage::fsm_get_avail(page as _, slot)
}
unsafe fn fsm_get_max_avail(page: Page) -> uint8 {
    crate::storage::freespace::fsmpage::fsm_get_max_avail(page as _)
}
unsafe fn fsm_search_avail(buf: Buffer, minvalue: uint8, advancenext: bool, exclusive_lock_held: bool) -> c_int {
    crate::storage::freespace::fsmpage::fsm_search_avail(buf, minvalue, advancenext, exclusive_lock_held)
}
unsafe fn fsm_truncate_avail(page: Page, nslots: c_int) {
    crate::storage::freespace::fsmpage::fsm_truncate_avail(page as _, nslots);
}

unsafe fn log_newpage_buffer(buf: Buffer, page_std: bool) -> crate::access::transam::xlogdefs::XLogRecPtr {
    crate::access::transam::xloginsert::log_newpage_buffer(buf, page_std)
}

fn START_CRIT_SECTION() {
    // TODO: miscadmin.h
}
fn END_CRIT_SECTION() {
    // TODO: miscadmin.h
}

// miscadmin.h globals
static mut InRecovery: bool = false;

unsafe fn XLogHintBitIsNeeded() -> bool {
    false // TODO: access/xlog.h unwired; gates hint-bit WAL, false is safe
}
