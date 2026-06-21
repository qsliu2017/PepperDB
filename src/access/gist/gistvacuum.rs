/*-------------------------------------------------------------------------
 *
 * gistvacuum.c
 *	  vacuuming routines for the postgres GiST index access method.
 *
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/gist/gistvacuum.c
 *
 *-------------------------------------------------------------------------
 */
use crate::prelude::*;

use crate::access::transam::xlogdefs::XLogRecPtr;
use crate::storage::block::BlockNumber;

/* Working state needed by gistbulkdelete */
#[repr(C)]
pub struct GistVacState {
    pub info: *mut IndexVacuumInfo,
    pub stats: *mut IndexBulkDeleteResult,
    pub callback: IndexBulkDeleteCallback,
    pub callback_state: *mut c_void,
    pub startNSN: GistNSN,

    /*
     * These are used to memorize all internal and empty leaf pages.  They are
     * used for deleting all the empty pages.
     */
    pub internal_page_set: *mut IntegerSet,
    pub empty_leaf_set: *mut IntegerSet,
    pub page_set_context: MemoryContext,
}

/*
 * VACUUM bulkdelete stage: remove index entries.
 */
pub unsafe fn gistbulkdelete(
    info: *mut IndexVacuumInfo,
    mut stats: *mut IndexBulkDeleteResult,
    callback: IndexBulkDeleteCallback,
    callback_state: *mut c_void,
) -> *mut IndexBulkDeleteResult {
    /* allocate stats if first time through, else re-use existing struct */
    if stats.is_null() {
        stats = palloc0(size_of::<IndexBulkDeleteResult>()) as *mut IndexBulkDeleteResult;
    }

    gistvacuumscan(info, stats, callback, callback_state);

    stats
}

/*
 * VACUUM cleanup stage: delete empty pages, and update index statistics.
 */
pub unsafe fn gistvacuumcleanup(
    info: *mut IndexVacuumInfo,
    mut stats: *mut IndexBulkDeleteResult,
) -> *mut IndexBulkDeleteResult {
    /* No-op in ANALYZE ONLY mode */
    if (*info).analyze_only {
        return stats;
    }

    /*
     * If gistbulkdelete was called, we need not do anything, just return the
     * stats from the latest gistbulkdelete call.  If it wasn't called, we
     * still need to do a pass over the index, to obtain index statistics.
     */
    if stats.is_null() {
        stats = palloc0(size_of::<IndexBulkDeleteResult>()) as *mut IndexBulkDeleteResult;
        gistvacuumscan(info, stats, None, std::ptr::null_mut());
    }

    /*
     * It's quite possible for us to be fooled by concurrent page splits into
     * double-counting some index tuples, so disbelieve any total that exceeds
     * the underlying heap's count ... if we know that accurately.  Otherwise
     * this might just make matters worse.
     */
    if !(*info).estimated_count {
        if (*stats).num_index_tuples > (*info).num_heap_tuples {
            (*stats).num_index_tuples = (*info).num_heap_tuples;
        }
    }

    stats
}

/*
 * gistvacuumscan --- scan the index for VACUUMing purposes
 *
 * This scans the index for leaf tuples that are deletable according to the
 * vacuum callback, and updates the stats.  Both btbulkdelete and
 * btvacuumcleanup invoke this (the latter only if no btbulkdelete call
 * occurred).
 *
 * This also makes note of any empty leaf pages, as well as all internal
 * pages while looping over all index pages.  After scanning all the pages, we
 * remove the empty pages so that they can be reused.  Any deleted pages are
 * added directly to the free space map.  (They should've been added there
 * when they were originally deleted, already, but it's possible that the FSM
 * was lost at a crash, for example.)
 *
 * The caller is responsible for initially allocating/zeroing a stats struct.
 */
unsafe fn gistvacuumscan(
    info: *mut IndexVacuumInfo,
    stats: *mut IndexBulkDeleteResult,
    callback: IndexBulkDeleteCallback,
    callback_state: *mut c_void,
) {
    let rel: Relation = (*info).index;
    let mut vstate: GistVacState = std::mem::zeroed();
    let mut num_pages: BlockNumber = 0;
    let needLock: bool;
    let oldctx: MemoryContext;
    let mut p: BlockRangeReadStreamPrivate = std::mem::zeroed();
    let stream: *mut ReadStream;

    /*
     * Reset fields that track information about the entire index now.  This
     * avoids double-counting in the case where a single VACUUM command
     * requires multiple scans of the index.
     *
     * Avoid resetting the tuples_removed and pages_newly_deleted fields here,
     * since they track information about the VACUUM command, and so must last
     * across each call to gistvacuumscan().
     *
     * (Note that pages_free is treated as state about the whole index, not
     * the current VACUUM.  This is appropriate because RecordFreeIndexPage()
     * calls are idempotent, and get repeated for the same deleted pages in
     * some scenarios.  The point for us is to track the number of recyclable
     * pages in the index at the end of the VACUUM command.)
     */
    (*stats).num_pages = 0;
    (*stats).estimated_count = false;
    (*stats).num_index_tuples = 0.0;
    (*stats).pages_deleted = 0;
    (*stats).pages_free = 0;

    /*
     * Create the integer sets to remember all the internal and the empty leaf
     * pages in page_set_context.  Internally, the integer set will remember
     * this context so that the subsequent allocations for these integer sets
     * will be done from the same context.
     *
     * XXX the allocation sizes used below pre-date generation context's block
     * growing code.  These values should likely be benchmarked and set to
     * more suitable values.
     */
    vstate.page_set_context = GenerationContextCreate(
        CurrentMemoryContext,
        c"GiST VACUUM page set context".as_ptr(),
        16 * 1024,
        16 * 1024,
        16 * 1024,
    );
    oldctx = MemoryContextSwitchTo(vstate.page_set_context);
    vstate.internal_page_set = intset_create();
    vstate.empty_leaf_set = intset_create();
    MemoryContextSwitchTo(oldctx);

    /* Set up info to pass down to gistvacuumpage */
    vstate.info = info;
    vstate.stats = stats;
    vstate.callback = callback;
    vstate.callback_state = callback_state;
    if RelationNeedsWAL(rel) {
        vstate.startNSN = GetInsertRecPtr();
    } else {
        vstate.startNSN = gistGetFakeLSN(rel);
    }

    /*
     * The outer loop iterates over all index pages, in physical order (we
     * hope the kernel will cooperate in providing read-ahead for speed).  It
     * is critical that we visit all leaf pages, including ones added after we
     * start the scan, else we might fail to delete some deletable tuples.
     * Hence, we must repeatedly check the relation length.  We must acquire
     * the relation-extension lock while doing so to avoid a race condition:
     * if someone else is extending the relation, there is a window where
     * bufmgr/smgr have created a new all-zero page but it hasn't yet been
     * write-locked by gistNewBuffer().  If we manage to scan such a page
     * here, we'll improperly assume it can be recycled.  Taking the lock
     * synchronizes things enough to prevent a problem: either num_pages won't
     * include the new page, or gistNewBuffer already has write lock on the
     * buffer and it will be fully initialized before we can examine it.  (See
     * also vacuumlazy.c, which has the same issue.)  Also, we need not worry
     * if a page is added immediately after we look; the page splitting code
     * already has write-lock on the left page before it adds a right page, so
     * we must already have processed any tuples due to be moved into such a
     * page.
     *
     * We can skip locking for new or temp relations, however, since no one
     * else could be accessing them.
     */
    needLock = !RELATION_IS_LOCAL(rel);

    p.current_blocknum = GIST_ROOT_BLKNO;

    /*
     * It is safe to use batchmode as block_range_read_stream_cb takes no
     * locks.
     */
    stream = read_stream_begin_relation(
        (READ_STREAM_MAINTENANCE | READ_STREAM_FULL | READ_STREAM_USE_BATCHING) as c_int,
        (*info).strategy,
        rel,
        MAIN_FORKNUM,
        Some(block_range_read_stream_cb),
        &mut p as *mut _ as *mut c_void,
        0,
    );
    loop {
        /* Get the current relation length */
        if needLock {
            LockRelationForExtension(rel, ExclusiveLock);
        }
        num_pages = RelationGetNumberOfBlocks(rel);
        if needLock {
            UnlockRelationForExtension(rel, ExclusiveLock);
        }

        /* Quit if we've scanned the whole relation */
        if p.current_blocknum >= num_pages {
            break;
        }

        p.last_exclusive = num_pages;

        /* Iterate over pages, then loop back to recheck relation length */
        loop {
            /* call vacuum_delay_point while not holding any buffer lock */
            vacuum_delay_point(false);

            let buf: Buffer = read_stream_next_buffer(stream, std::ptr::null_mut());

            if !BufferIsValid(buf) {
                break;
            }

            gistvacuumpage(&mut vstate, buf);
        }

        /*
         * We have to reset the read stream to use it again. After returning
         * InvalidBuffer, the read stream API won't invoke our callback again
         * until the stream has been reset.
         */
        read_stream_reset(stream);
    }

    read_stream_end(stream);

    /*
     * If we found any recyclable pages (and recorded them in the FSM), then
     * forcibly update the upper-level FSM pages to ensure that searchers can
     * find them.  It's possible that the pages were also found during
     * previous scans and so this is a waste of time, but it's cheap enough
     * relative to scanning the index that it shouldn't matter much, and
     * making sure that free pages are available sooner not later seems
     * worthwhile.
     *
     * Note that if no recyclable pages exist, we don't bother vacuuming the
     * FSM at all.
     */
    if (*stats).pages_free > 0 {
        IndexFreeSpaceMapVacuum(rel);
    }

    /* update statistics */
    (*stats).num_pages = num_pages;

    /*
     * If we saw any empty pages, try to unlink them from the tree so that
     * they can be reused.
     */
    gistvacuum_delete_empty_pages(info, &mut vstate);

    /* we don't need the internal and empty page sets anymore */
    MemoryContextDelete(vstate.page_set_context);
    vstate.page_set_context = std::ptr::null_mut();
    vstate.internal_page_set = std::ptr::null_mut();
    vstate.empty_leaf_set = std::ptr::null_mut();
}

/*
 * gistvacuumpage --- VACUUM one page
 *
 * This processes a single page for gistbulkdelete(). `buffer` contains the
 * page to process. In some cases we must go back and reexamine
 * previously-scanned pages; this routine recurses when necessary to handle
 * that case.
 */
unsafe fn gistvacuumpage(vstate: *mut GistVacState, mut buffer: Buffer) {
    let info: *mut IndexVacuumInfo = (*vstate).info;
    let callback: IndexBulkDeleteCallback = (*vstate).callback;
    let callback_state: *mut c_void = (*vstate).callback_state;
    let rel: Relation = (*info).index;
    let orig_blkno: BlockNumber = BufferGetBlockNumber(buffer);
    let mut page: Page;
    let mut recurse_to: BlockNumber;

    /*
     * orig_blkno is the highest block number reached by the outer
     * gistvacuumscan() loop. This will be the same as blkno unless we are
     * recursing to reexamine a previous page.
     */
    let mut blkno: BlockNumber = orig_blkno;

    'restart: loop {
        recurse_to = InvalidBlockNumber;

        /*
         * We are not going to stay here for a long time, aggressively grab an
         * exclusive lock.
         */
        LockBuffer(buffer, GIST_EXCLUSIVE as c_int);
        page = BufferGetPage(buffer) as Page;

        if gistPageRecyclable(page) {
            /* Okay to recycle this page */
            RecordFreeIndexPage(rel, blkno);
            (*(*vstate).stats).pages_deleted += 1;
            (*(*vstate).stats).pages_free += 1;
        } else if GistPageIsDeleted(page) {
            /* Already deleted, but can't recycle yet */
            (*(*vstate).stats).pages_deleted += 1;
        } else if GistPageIsLeaf(page) {
            let mut todelete: [OffsetNumber; MaxOffsetNumber as usize] =
                [0; MaxOffsetNumber as usize];
            let mut ntodelete: c_int = 0;
            let nremain: c_int;
            let opaque: GISTPageOpaque = GistPageGetOpaque(page);
            let mut maxoff: OffsetNumber = PageGetMaxOffsetNumber(page);

            /*
             * Check whether we need to recurse back to earlier pages.  What we
             * are concerned about is a page split that happened since we started
             * the vacuum scan.  If the split moved some tuples to a lower page
             * then we might have missed 'em.  If so, set up for tail recursion.
             *
             * This is similar to the checks we do during searches, when following
             * a downlink, but we don't need to jump to higher-numbered pages,
             * because we will process them later, anyway.
             */
            if (GistFollowRight(page) || (*vstate).startNSN < GistPageGetNSN(page))
                && ((*opaque).rightlink != InvalidBlockNumber)
                && ((*opaque).rightlink < orig_blkno)
            {
                recurse_to = (*opaque).rightlink;
            }

            /*
             * Scan over all items to see which ones need to be deleted according
             * to the callback function.
             */
            if let Some(cb) = callback {
                let mut off: OffsetNumber = FirstOffsetNumber;
                while off <= maxoff {
                    let iid: ItemId = PageGetItemId(page, off);
                    let idxtuple: IndexTuple = PageGetItem(page, iid) as IndexTuple;

                    if cb(&mut (*idxtuple).t_tid, callback_state) {
                        todelete[ntodelete as usize] = off;
                        ntodelete += 1;
                    }
                    off = OffsetNumberNext(off);
                }
            }

            /*
             * Apply any needed deletes.  We issue just one WAL record per page,
             * so as to minimize WAL traffic.
             */
            if ntodelete > 0 {
                START_CRIT_SECTION();

                MarkBufferDirty(buffer);

                PageIndexMultiDelete(page, todelete.as_mut_ptr(), ntodelete);
                GistMarkTuplesDeleted(page);

                if RelationNeedsWAL(rel) {
                    let recptr: XLogRecPtr = gistXLogUpdate(
                        buffer,
                        todelete.as_mut_ptr(),
                        ntodelete,
                        std::ptr::null_mut(),
                        0,
                        InvalidBuffer,
                    );
                    PageSetLSN(page, recptr);
                } else {
                    PageSetLSN(page, gistGetFakeLSN(rel));
                }

                END_CRIT_SECTION();

                (*(*vstate).stats).tuples_removed += ntodelete as f64;
                /* must recompute maxoff */
                maxoff = PageGetMaxOffsetNumber(page);
            }

            nremain = maxoff as c_int - FirstOffsetNumber as c_int + 1;
            if nremain == 0 {
                /*
                 * The page is now completely empty.  Remember its block number,
                 * so that we will try to delete the page in the second stage.
                 *
                 * Skip this when recursing, because IntegerSet requires that the
                 * values are added in ascending order.  The next VACUUM will pick
                 * it up.
                 */
                if blkno == orig_blkno {
                    intset_add_member((*vstate).empty_leaf_set, blkno as u64);
                }
            } else {
                (*(*vstate).stats).num_index_tuples += nremain as f64;
            }
        } else {
            /*
             * On an internal page, check for "invalid tuples", left behind by an
             * incomplete page split on PostgreSQL 9.0 or below.  These are not
             * created by newer PostgreSQL versions, but unfortunately, there is
             * no version number anywhere in a GiST index, so we don't know
             * whether this index might still contain invalid tuples or not.
             */
            let maxoff: OffsetNumber = PageGetMaxOffsetNumber(page);
            let mut off: OffsetNumber = FirstOffsetNumber;

            while off <= maxoff {
                let iid: ItemId = PageGetItemId(page, off);
                let idxtuple: IndexTuple = PageGetItem(page, iid) as IndexTuple;

                if GistTupleIsInvalid(idxtuple) {
                    ereport!(
                        LOG,
                        "index contains an inner tuple marked as invalid"
                    );
                }
                off = OffsetNumberNext(off);
            }

            /*
             * Remember the block number of this page, so that we can revisit it
             * later in gistvacuum_delete_empty_pages(), when we search for
             * parents of empty leaf pages.
             */
            if blkno == orig_blkno {
                intset_add_member((*vstate).internal_page_set, blkno as u64);
            }
        }

        UnlockReleaseBuffer(buffer);

        /*
         * This is really tail recursion, but if the compiler is too stupid to
         * optimize it as such, we'd eat an uncomfortably large amount of stack
         * space per recursion level (due to the deletable[] array).  A failure is
         * improbable since the number of levels isn't likely to be large ... but
         * just in case, let's hand-optimize into a loop.
         */
        if recurse_to != InvalidBlockNumber {
            blkno = recurse_to;

            /* check for vacuum delay while not holding any buffer lock */
            vacuum_delay_point(false);

            buffer = ReadBufferExtended(rel, MAIN_FORKNUM, blkno, RBM_NORMAL, (*info).strategy);
            continue 'restart;
        }

        break;
    }
}

/*
 * Scan all internal pages, and try to delete their empty child pages.
 */
unsafe fn gistvacuum_delete_empty_pages(info: *mut IndexVacuumInfo, vstate: *mut GistVacState) {
    let rel: Relation = (*info).index;
    let mut empty_pages_remaining: BlockNumber;
    let mut blkno: u64 = 0;

    /*
     * Rescan all inner pages to find those that have empty child pages.
     */
    empty_pages_remaining = intset_num_entries((*vstate).empty_leaf_set) as BlockNumber;
    intset_begin_iterate((*vstate).internal_page_set);
    while empty_pages_remaining > 0
        && intset_iterate_next((*vstate).internal_page_set, &mut blkno)
    {
        let buffer: Buffer;
        let page: Page;
        let mut off: OffsetNumber;
        let maxoff: OffsetNumber;
        let mut todelete: [OffsetNumber; MaxOffsetNumber as usize] = [0; MaxOffsetNumber as usize];
        let mut leafs_to_delete: [BlockNumber; MaxOffsetNumber as usize] =
            [0; MaxOffsetNumber as usize];
        let mut ntodelete: c_int;
        let mut deleted: c_int;

        buffer = ReadBufferExtended(
            rel,
            MAIN_FORKNUM,
            blkno as BlockNumber,
            RBM_NORMAL,
            (*info).strategy,
        );

        LockBuffer(buffer, GIST_SHARE as c_int);
        page = BufferGetPage(buffer) as Page;

        if PageIsNew(page) || GistPageIsDeleted(page) || GistPageIsLeaf(page) {
            /*
             * This page was an internal page earlier, but now it's something
             * else. Shouldn't happen...
             */
            Assert!(false);
            UnlockReleaseBuffer(buffer);
            continue;
        }

        /*
         * Scan all the downlinks, and see if any of them point to empty leaf
         * pages.
         */
        maxoff = PageGetMaxOffsetNumber(page);
        ntodelete = 0;
        off = FirstOffsetNumber;
        while off <= maxoff && ntodelete < maxoff as c_int - 1 {
            let iid: ItemId = PageGetItemId(page, off);
            let idxtuple: IndexTuple = PageGetItem(page, iid) as IndexTuple;

            let leafblk: BlockNumber = ItemPointerGetBlockNumber(&mut (*idxtuple).t_tid);
            if intset_is_member((*vstate).empty_leaf_set, leafblk as u64) {
                leafs_to_delete[ntodelete as usize] = leafblk;
                todelete[ntodelete as usize] = off;
                ntodelete += 1;
            }
            off = OffsetNumberNext(off);
        }

        /*
         * In order to avoid deadlock, child page must be locked before
         * parent, so we must release the lock on the parent, lock the child,
         * and then re-acquire the lock the parent.  (And we wouldn't want to
         * do I/O, while holding a lock, anyway.)
         *
         * At the instant that we're not holding a lock on the parent, the
         * downlink might get moved by a concurrent insert, so we must
         * re-check that it still points to the same child page after we have
         * acquired both locks.  Also, another backend might have inserted a
         * tuple to the page, so that it is no longer empty.  gistdeletepage()
         * re-checks all these conditions.
         */
        LockBuffer(buffer, GIST_UNLOCK as c_int);

        deleted = 0;
        for i in 0..ntodelete {
            let leafbuf: Buffer;

            /*
             * Don't remove the last downlink from the parent.  That would
             * confuse the insertion code.
             */
            if PageGetMaxOffsetNumber(page) == FirstOffsetNumber {
                break;
            }

            leafbuf = ReadBufferExtended(
                rel,
                MAIN_FORKNUM,
                leafs_to_delete[i as usize],
                RBM_NORMAL,
                (*info).strategy,
            );
            LockBuffer(leafbuf, GIST_EXCLUSIVE as c_int);
            gistcheckpage(rel, leafbuf);

            LockBuffer(buffer, GIST_EXCLUSIVE as c_int);
            if gistdeletepage(
                info,
                (*vstate).stats,
                buffer,
                todelete[i as usize] - deleted as OffsetNumber,
                leafbuf,
            ) {
                deleted += 1;
            }
            LockBuffer(buffer, GIST_UNLOCK as c_int);

            UnlockReleaseBuffer(leafbuf);
        }

        ReleaseBuffer(buffer);

        /*
         * We can stop the scan as soon as we have seen the downlinks, even if
         * we were not able to remove them all.
         */
        empty_pages_remaining -= ntodelete as BlockNumber;
    }
}

/*
 * gistdeletepage takes a leaf page, and its parent, and tries to delete the
 * leaf.  Both pages must be locked.
 *
 * Even if the page was empty when we first saw it, a concurrent inserter might
 * have added a tuple to it since.  Similarly, the downlink might have moved.
 * We re-check all the conditions, to make sure the page is still deletable,
 * before modifying anything.
 *
 * Returns true, if the page was deleted, and false if a concurrent update
 * prevented it.
 */
unsafe fn gistdeletepage(
    info: *mut IndexVacuumInfo,
    stats: *mut IndexBulkDeleteResult,
    parentBuffer: Buffer,
    downlink: OffsetNumber,
    leafBuffer: Buffer,
) -> bool {
    let parentPage: Page = BufferGetPage(parentBuffer) as Page;
    let leafPage: Page = BufferGetPage(leafBuffer) as Page;
    let iid: ItemId;
    let idxtuple: IndexTuple;
    let recptr: XLogRecPtr;
    let txid: FullTransactionId;

    /*
     * Check that the leaf is still empty and deletable.
     */
    if !GistPageIsLeaf(leafPage) {
        /* a leaf page should never become a non-leaf page */
        Assert!(false);
        return false;
    }

    if GistFollowRight(leafPage) {
        return false; /* don't mess with a concurrent page split */
    }

    if PageGetMaxOffsetNumber(leafPage) != InvalidOffsetNumber {
        return false; /* not empty anymore */
    }

    /*
     * Ok, the leaf is deletable.  Is the downlink in the parent page still
     * valid?  It might have been moved by a concurrent insert.  We could try
     * to re-find it by scanning the page again, possibly moving right if the
     * was split.  But for now, let's keep it simple and just give up.  The
     * next VACUUM will pick it up.
     */
    if PageIsNew(parentPage) || GistPageIsDeleted(parentPage) || GistPageIsLeaf(parentPage) {
        /* shouldn't happen, internal pages are never deleted */
        Assert!(false);
        return false;
    }

    if PageGetMaxOffsetNumber(parentPage) < downlink
        || PageGetMaxOffsetNumber(parentPage) <= FirstOffsetNumber
    {
        return false;
    }

    iid = PageGetItemId(parentPage, downlink);
    idxtuple = PageGetItem(parentPage, iid) as IndexTuple;
    if BufferGetBlockNumber(leafBuffer) != ItemPointerGetBlockNumber(&mut (*idxtuple).t_tid) {
        return false;
    }

    /*
     * All good, proceed with the deletion.
     *
     * The page cannot be immediately recycled, because in-progress scans that
     * saw the downlink might still visit it.  Mark the page with the current
     * next-XID counter, so that we know when it can be recycled.  Once that
     * XID becomes older than GlobalXmin, we know that all scans that are
     * currently in progress must have ended.  (That's much more conservative
     * than needed, but let's keep it safe and simple.)
     */
    txid = ReadNextFullTransactionId();

    START_CRIT_SECTION();

    /* mark the page as deleted */
    MarkBufferDirty(leafBuffer);
    GistPageSetDeleted(leafPage, txid);
    (*stats).pages_newly_deleted += 1;
    (*stats).pages_deleted += 1;

    /* remove the downlink from the parent */
    MarkBufferDirty(parentBuffer);
    PageIndexTupleDelete(parentPage, downlink);

    if RelationNeedsWAL((*info).index) {
        recptr = gistXLogPageDelete(leafBuffer, txid, parentBuffer, downlink);
    } else {
        recptr = gistGetFakeLSN((*info).index);
    }
    PageSetLSN(parentPage, recptr);
    PageSetLSN(leafPage, recptr);

    END_CRIT_SECTION();

    true
}

// ---------------------------------------------------------------------------
// Local stubs for unported dependencies.
// ---------------------------------------------------------------------------

pub type Buffer = c_int;
pub type Page = *mut c_char;
pub type ItemId = *mut c_void;
pub type IndexTuple = *mut IndexTupleData;
pub type GistNSN = XLogRecPtr;
pub type GISTPageOpaque = *mut GISTPageOpaqueData;
pub type IndexBulkDeleteCallback =
    Option<unsafe extern "C" fn(itemptr: *mut ItemPointerData, state: *mut c_void) -> bool>;

#[repr(C)]
pub struct ItemPointerData {
    pub dummy: u8,
}

#[repr(C)]
pub struct IndexTupleData {
    pub t_tid: ItemPointerData,
}

#[repr(C)]
pub struct GISTPageOpaqueData {
    pub rightlink: BlockNumber,
}

#[repr(C)]
pub struct IndexVacuumInfo {
    pub index: Relation,
    pub analyze_only: bool,
    pub estimated_count: bool,
    pub num_heap_tuples: f64,
    pub strategy: *mut c_void,
}

#[repr(C)]
pub struct IndexBulkDeleteResult {
    pub num_pages: BlockNumber,
    pub estimated_count: bool,
    pub num_index_tuples: f64,
    pub tuples_removed: f64,
    pub pages_deleted: BlockNumber,
    pub pages_newly_deleted: BlockNumber,
    pub pages_free: BlockNumber,
}

#[repr(C)]
pub struct BlockRangeReadStreamPrivate {
    pub current_blocknum: BlockNumber,
    pub last_exclusive: BlockNumber,
}

pub enum ReadStream {}
pub enum IntegerSet {}

pub const GIST_ROOT_BLKNO: BlockNumber = 0;
pub const MAIN_FORKNUM: c_int = 0;
pub const ExclusiveLock: c_int = 7;
pub const GIST_EXCLUSIVE: c_uint = 2;
pub const GIST_SHARE: c_uint = 1;
pub const GIST_UNLOCK: c_uint = 0;
pub const RBM_NORMAL: c_int = 0;
pub const InvalidBuffer: Buffer = 0;
pub const InvalidBlockNumber: BlockNumber = 0xFFFF_FFFF;
pub const MaxOffsetNumber: u16 = 2048;
pub const FirstOffsetNumber: OffsetNumber = 1;
pub const InvalidOffsetNumber: OffsetNumber = 0;
pub const LOG: c_int = 15;
pub const READ_STREAM_MAINTENANCE: c_uint = 0x01;
pub const READ_STREAM_FULL: c_uint = 0x04;
pub const READ_STREAM_USE_BATCHING: c_uint = 0x10;

pub type OffsetNumber = u16;
pub type Relation = *mut c_void;
pub type FullTransactionId = u64;

unsafe fn RelationNeedsWAL(_rel: Relation) -> bool { unimplemented!() }
unsafe fn RELATION_IS_LOCAL(_rel: Relation) -> bool {
    unimplemented!() // TODO: access/gist_private.h
}
unsafe fn GetInsertRecPtr() -> XLogRecPtr { crate::access::transam::xlog::GetInsertRecPtr() }
unsafe fn gistGetFakeLSN(_rel: Relation) -> XLogRecPtr { unimplemented!() }
unsafe fn intset_create() -> *mut IntegerSet { unimplemented!() }
unsafe fn intset_add_member(_s: *mut IntegerSet, _x: u64) { unimplemented!() }
unsafe fn intset_num_entries(_s: *mut IntegerSet) -> u64 { unimplemented!() }
unsafe fn intset_begin_iterate(_s: *mut IntegerSet) { unimplemented!() }
unsafe fn intset_iterate_next(_s: *mut IntegerSet, _next: *mut u64) -> bool { unimplemented!() }
unsafe fn intset_is_member(_s: *mut IntegerSet, _x: u64) -> bool { unimplemented!() }
unsafe fn GenerationContextCreate(
    _parent: MemoryContext,
    _name: *const c_char,
    _min: Size,
    _init: Size,
    _max: Size,
) -> MemoryContext {
    unimplemented!() // TODO: utils/memutils.h
}
unsafe fn read_stream_begin_relation(
    _flags: c_int,
    _strategy: *mut c_void,
    _rel: Relation,
    _forknum: c_int,
    _cb: ReadStreamBlockNumberCB,
    _cb_private: *mut c_void,
    _per_buffer_data_size: Size,
) -> *mut ReadStream {
    unimplemented!() // TODO: storage/read_stream.h
}
unsafe fn read_stream_next_buffer(_stream: *mut ReadStream, _per_buffer_data: *mut *mut c_void) -> Buffer { unimplemented!() }
unsafe fn read_stream_reset(_stream: *mut ReadStream) { unimplemented!() }
unsafe fn read_stream_end(_stream: *mut ReadStream) { unimplemented!() }
type ReadStreamBlockNumberCB = Option<
    unsafe extern "C" fn(
        stream: *mut ReadStream,
        callback_private_data: *mut c_void,
        per_buffer_data: *mut c_void,
    ) -> BlockNumber,
>;
unsafe extern "C" fn block_range_read_stream_cb(
    _stream: *mut ReadStream,
    _callback_private_data: *mut c_void,
    _per_buffer_data: *mut c_void,
) -> BlockNumber { unimplemented!() }
unsafe fn LockRelationForExtension(_rel: Relation, _lockmode: c_int) {
    unimplemented!() // TODO: storage/lmgr.h
}
unsafe fn UnlockRelationForExtension(_rel: Relation, _lockmode: c_int) {
    unimplemented!() // TODO: storage/lmgr.h
}
unsafe fn RelationGetNumberOfBlocks(_rel: Relation) -> BlockNumber { unimplemented!() }
unsafe fn vacuum_delay_point(_is_analyze: bool) { crate::commands::vacuum::vacuum_delay_point(_is_analyze) }
unsafe fn BufferIsValid(_buf: Buffer) -> bool { crate::access::nbtree::nbtpage::BufferIsValid(_buf) }
unsafe fn IndexFreeSpaceMapVacuum(_rel: Relation) { unimplemented!() }
unsafe fn BufferGetBlockNumber(_buf: Buffer) -> BlockNumber {
    unimplemented!() // TODO: storage/bufmgr.h
}
unsafe fn LockBuffer(_buf: Buffer, _mode: c_int) {
    unimplemented!() // TODO: storage/bufmgr.h
}
unsafe fn BufferGetPage(_buf: Buffer) -> *mut c_void {
    unimplemented!() // TODO: storage/bufmgr.h
}
unsafe fn gistPageRecyclable(_page: Page) -> bool { crate::access::gist::gist_private::gistPageRecyclable(_page) }
unsafe fn RecordFreeIndexPage(_rel: Relation, _blkno: BlockNumber) { unimplemented!() }
unsafe fn GistPageIsDeleted(_page: Page) -> bool {
    unimplemented!() // TODO: access/gist.h
}
unsafe fn GistPageIsLeaf(_page: Page) -> bool {
    unimplemented!() // TODO: access/gist.h
}
unsafe fn GistPageGetOpaque(_page: Page) -> GISTPageOpaque {
    unimplemented!() // TODO: access/gist.h
}
unsafe fn PageGetMaxOffsetNumber(_page: Page) -> OffsetNumber {
    unimplemented!() // TODO: storage/bufpage.h
}
unsafe fn GistFollowRight(_page: Page) -> bool {
    unimplemented!() // TODO: access/gist.h
}
unsafe fn GistPageGetNSN(_page: Page) -> GistNSN {
    unimplemented!() // TODO: access/gist.h
}
unsafe fn OffsetNumberNext(off: OffsetNumber) -> OffsetNumber {
    off + 1 // OffsetNumberNext
}
unsafe fn PageGetItemId(_page: Page, _off: OffsetNumber) -> ItemId {
    unimplemented!() // TODO: storage/bufpage.h
}
unsafe fn PageGetItem(_page: Page, _iid: ItemId) -> *mut c_void {
    unimplemented!() // TODO: storage/bufpage.h
}
unsafe fn MarkBufferDirty(_buf: Buffer) {
    unimplemented!() // TODO: storage/bufmgr.h
}
unsafe fn PageIndexMultiDelete(_page: Page, _items: *mut OffsetNumber, _nitems: c_int) {
    unimplemented!() // TODO: storage/bufpage.h
}
unsafe fn GistMarkTuplesDeleted(_page: Page) {
    unimplemented!() // TODO: access/gist.h
}
unsafe fn gistXLogUpdate(
    _buffer: Buffer,
    _todelete: *mut OffsetNumber,
    _ntodelete: c_int,
    _itup: *mut IndexTuple,
    _ituplen: c_int,
    _leftchildbuf: Buffer,
) -> XLogRecPtr { unimplemented!() }
unsafe fn PageSetLSN(_page: Page, _lsn: XLogRecPtr) { crate::storage::bufpage::PageSetLSN(_page, _lsn) }
unsafe fn GistTupleIsInvalid(_itup: IndexTuple) -> bool { unimplemented!() }
unsafe fn UnlockReleaseBuffer(_buf: Buffer) {
    unimplemented!() // TODO: storage/bufmgr.h
}
unsafe fn ReadBufferExtended(
    _rel: Relation,
    _forknum: c_int,
    _blkno: BlockNumber,
    _mode: c_int,
    _strategy: *mut c_void,
) -> Buffer {
    unimplemented!() // TODO: storage/bufmgr.h
}
unsafe fn PageIsNew(_page: Page) -> bool {
    unimplemented!() // TODO: storage/bufpage.h
}
unsafe fn ItemPointerGetBlockNumber(_pointer: *mut ItemPointerData) -> BlockNumber {
    unimplemented!() // TODO: storage/itemptr.h
}
unsafe fn gistcheckpage(_rel: Relation, _buf: Buffer) { unimplemented!() }
unsafe fn ReleaseBuffer(_buf: Buffer) {
    unimplemented!() // TODO: storage/bufmgr.h
}
unsafe fn GistPageSetDeleted(_page: Page, _txid: FullTransactionId) {
    unimplemented!() // TODO: access/gist.h
}
unsafe fn PageIndexTupleDelete(_page: Page, _offnum: OffsetNumber) {
    unimplemented!() // TODO: storage/bufpage.h
}
unsafe fn gistXLogPageDelete(
    _buffer: Buffer,
    _xid: FullTransactionId,
    _parentBuffer: Buffer,
    _downlinkOffset: OffsetNumber,
) -> XLogRecPtr { crate::access::gist::gistxlog::gistXLogPageDelete(_buffer, _xid, _parentBuffer, _downlinkOffset) }
unsafe fn ReadNextFullTransactionId() -> FullTransactionId { unimplemented!() }
unsafe fn START_CRIT_SECTION() {
    unimplemented!() // TODO: miscadmin.h
}
unsafe fn END_CRIT_SECTION() {
    unimplemented!() // TODO: miscadmin.h
}
