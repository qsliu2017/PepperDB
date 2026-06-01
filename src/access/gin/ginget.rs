//! src/backend/access/gin/ginget.c
//!   fetch tuples from a GIN scan.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::prelude::*;

use core::ffi::CStr;

use crate::pg_config::BLCKSZ;

use crate::access::common::indextuple::{IndexTuple, IndexTupleData};
use crate::access::common::tupdesc::{CompactAttribute, TupleDescCompactAttr};
use crate::access::relscan::IndexScanDescData;
use crate::common::pg_prng::{pg_global_prng_state, pg_prng_double};
use crate::miscadmin::{work_mem, CHECK_FOR_INTERRUPTS};
use crate::nodes::tidbitmap::{
    tbm_add_page, tbm_add_tuples, tbm_begin_private_iterate, tbm_create,
    tbm_end_private_iterate, tbm_extract_page_tuple, tbm_free, tbm_is_empty,
    tbm_private_iterate, TIDBitmap, TBM_MAX_TUPLES_PER_PAGE,
};
use crate::storage::block::{BlockNumber, BlockNumberIsValid, InvalidBlockNumber};
use crate::storage::buf::{Buffer, InvalidBuffer};
use crate::storage::buffer::bufmgr::{
    BufferGetBlockNumber, BufferGetPage, BufferIsValid, IncrBufferRefCount, LockBuffer, ReadBuffer,
    ReleaseBuffer, UnlockReleaseBuffer,
};
use crate::storage::bufpage::{
    Page, PageGetItem, PageGetItemId, PageGetMaxOffsetNumber,
};
use crate::storage::itemptr::{
    ItemPointer, ItemPointerData, ItemPointerEquals, ItemPointerGetBlockNumber, ItemPointerIsValid,
    ItemPointerSet, ItemPointerSetInvalid,
};
use crate::storage::lmgr::predicate::PredicateLockPage;
use crate::storage::off::{
    FirstOffsetNumber, InvalidOffsetNumber, OffsetNumber, OffsetNumberNext, OffsetNumberPrev,
};
use crate::port::port_api::qsort_arg;
use crate::utils::adt::datum::datumCopy;
use crate::utils::fmgr::FunctionCall4Coll;
use crate::utils::rel::{Relation, RelationGetRelationName};
use crate::utils::snapshot::SnapshotData;

use crate::access::gin::gin::{
    GinTernaryValue, GIN_FALSE, GIN_MAYBE, GIN_SEARCH_MODE_ALL, GIN_TRUE,
};
use crate::access::gin::ginblock::{
    GinDataPageGetRightBound, GinGetNPosting, GinGetPostingTree, GinItemPointerGetBlockNumber,
    GinItemPointerGetOffsetNumber, GinIsPostingTree, GinNullCategory, GinPageGetMeta,
    GinPageGetOpaque, GinPageHasFullRow, GinPageRightMost, ItemPointerIsLossyPage,
    ItemPointerIsMin, ItemPointerSetLossyPage, ItemPointerSetMax, ItemPointerSetMin, GIN_DELETED,
    GIN_CAT_EMPTY_QUERY, GIN_CAT_NORM_KEY, GIN_CAT_NULL_ITEM, GIN_METAPAGE_BLKNO,
};
use crate::access::gin::gin_private::{
    freeGinBtreeStack, ginCompareEntries, ginCompareItemPointers, ginFindLeafPage,
    ginFreeScanKeys, ginNewScanKey, ginPrepareEntryScan, ginReadTuple, ginScanBeginPostingTree,
    ginStepRight, gintuple_get_attrnum, gintuple_get_key, GinBtree, GinBtreeData, GinBtreeStack,
    GinDataLeafPageGetItems, GinDataLeafPageGetItemsToTbm, GinScanEntry, GinScanKey, GinScanOpaque,
    GinState, GIN_SHARE, GIN_UNLOCK,
};

// `IndexScanDesc` pointer alias over the real relscan struct (the canonical
// alias in access/index/amapi.rs is an opaque `*mut c_void`).
pub type IndexScanDesc = *mut IndexScanDescData;

// Snapshot: matches IndexScanDescData.xs_snapshot.  PredicateLockPage() takes
// its own opaque `Snapshot` (*mut c_void), so call sites cast with `as _`.
pub type Snapshot = *mut SnapshotData;

// GUC parameter
#[no_mangle]
pub static mut GinFuzzySearchLimit: c_int = 0;

#[repr(C)]
pub struct pendingPosition {
    pub pendingBuffer: Buffer,
    pub firstOffset: OffsetNumber,
    pub lastOffset: OffsetNumber,
    pub item: ItemPointerData,
    pub hasMatchKey: *mut bool,
}

// ---- Local stubs for unported dependencies ----

extern "C" {
    fn memset(s: *mut c_void, c: c_int, n: usize) -> *mut c_void;
}

// #define gin_rand() pg_prng_double(&pg_global_prng_state)
unsafe fn gin_rand() -> f64 {
    pg_prng_double(&raw mut pg_global_prng_state)
}

// #define dropItem(e) ( gin_rand() > ((double)GinFuzzySearchLimit)/((double)((e)->predictNumberResult)) )
unsafe fn dropItem(e: GinScanEntry) -> bool {
    gin_rand() > (GinFuzzySearchLimit as f64) / ((*e).predictNumberResult as f64)
}

// ---- Translated functions ----

/*
 * Goes to the next page if current offset is outside of bounds
 */
unsafe fn moveRightIfItNeeded(
    btree: *mut GinBtreeData,
    stack: *mut GinBtreeStack,
    snapshot: Snapshot,
) -> bool {
    let page: Page = BufferGetPage((*stack).buffer);

    if (*stack).off > PageGetMaxOffsetNumber(page) {
        /*
         * We scanned the whole page, so we should take right page
         */
        if GinPageRightMost(page) {
            return false; /* no more pages */
        }

        (*stack).buffer = ginStepRight((*stack).buffer, (*btree).index, GIN_SHARE);
        (*stack).blkno = BufferGetBlockNumber((*stack).buffer);
        (*stack).off = FirstOffsetNumber;
        PredicateLockPage((*btree).index, (*stack).blkno, snapshot as _);
    }

    true
}

/*
 * Scan all pages of a posting tree and save all its heap ItemPointers
 * in scanEntry->matchBitmap
 */
unsafe fn scanPostingTree(
    index: Relation,
    scanEntry: GinScanEntry,
    rootPostingTree: BlockNumber,
) {
    let mut btree: GinBtreeData = std::mem::zeroed();
    let stack: *mut GinBtreeStack;
    let mut buffer: Buffer;
    let mut page: Page;

    /* Descend to the leftmost leaf page */
    stack = ginScanBeginPostingTree(&mut btree, index, rootPostingTree);
    buffer = (*stack).buffer;

    IncrBufferRefCount(buffer); /* prevent unpin in freeGinBtreeStack */

    freeGinBtreeStack(stack);

    /*
     * Loop iterates through all leaf pages of posting tree
     */
    loop {
        page = BufferGetPage(buffer);
        if ((*GinPageGetOpaque(page)).flags & GIN_DELETED) == 0 {
            let n: c_int = GinDataLeafPageGetItemsToTbm(page, (*scanEntry).matchBitmap);

            (*scanEntry).predictNumberResult += n as uint32;
        }

        if GinPageRightMost(page) {
            break; /* no more pages */
        }

        buffer = ginStepRight(buffer, index, GIN_SHARE);
    }

    UnlockReleaseBuffer(buffer);
}

/*
 * Collects TIDs into scanEntry->matchBitmap for all heap tuples that
 * match the search entry.  This supports three different match modes:
 *
 * 1. Partial-match support: scan from current point until the
 *	  comparePartialFn says we're done.
 * 2. SEARCH_MODE_ALL: scan from current point (which should be first
 *	  key for the current attnum) until we hit null items or end of attnum
 * 3. SEARCH_MODE_EVERYTHING: scan from current point (which should be first
 *	  key for the current attnum) until we hit end of attnum
 *
 * Returns true if done, false if it's necessary to restart scan from scratch
 */
unsafe fn collectMatchBitmap(
    btree: *mut GinBtreeData,
    stack: *mut GinBtreeStack,
    scanEntry: GinScanEntry,
    snapshot: Snapshot,
) -> bool {
    let attnum: OffsetNumber;
    let attr: *mut CompactAttribute;

    /* Initialize empty bitmap result */
    (*scanEntry).matchBitmap = tbm_create(work_mem as Size * 1024 as Size, std::ptr::null_mut());

    /* Null query cannot partial-match anything */
    if (*scanEntry).isPartialMatch && (*scanEntry).queryCategory != GIN_CAT_NORM_KEY {
        return true;
    }

    /* Locate tupdesc entry for key column (for attbyval/attlen data) */
    attnum = (*scanEntry).attnum;
    attr = TupleDescCompactAttr((*(*btree).ginstate).origTupdesc, (attnum - 1) as c_int);

    /*
     * Predicate lock entry leaf page, following pages will be locked by
     * moveRightIfItNeeded()
     */
    PredicateLockPage(
        (*btree).index,
        BufferGetBlockNumber((*stack).buffer),
        snapshot as _,
    );

    loop {
        let mut page: Page;
        let mut itup: IndexTuple;
        let mut idatum: Datum;
        let mut icategory: GinNullCategory = 0;

        /*
         * stack->off points to the interested entry, buffer is already locked
         */
        if moveRightIfItNeeded(btree, stack, snapshot) == false {
            return true;
        }

        page = BufferGetPage((*stack).buffer);
        itup = PageGetItem(page, PageGetItemId(page, (*stack).off)) as IndexTuple;

        /*
         * If tuple stores another attribute then stop scan
         */
        if gintuple_get_attrnum((*btree).ginstate, itup) != attnum {
            return true;
        }

        /* Safe to fetch attribute value */
        idatum = gintuple_get_key((*btree).ginstate, itup, &mut icategory);

        /*
         * Check for appropriate scan stop conditions
         */
        if (*scanEntry).isPartialMatch {
            let cmp: int32;

            /*
             * In partial match, stop scan at any null (including
             * placeholders); partial matches never match nulls
             */
            if icategory != GIN_CAT_NORM_KEY {
                return true;
            }

            /*----------
             * Check of partial match.
             * case cmp == 0 => match
             * case cmp > 0 => not match and finish scan
             * case cmp < 0 => not match and continue scan
             *----------
             */
            cmp = DatumGetInt32(FunctionCall4Coll(
                &mut (*(*btree).ginstate).comparePartialFn[(attnum - 1) as usize],
                (*(*btree).ginstate).supportCollation[(attnum - 1) as usize],
                (*scanEntry).queryKey,
                idatum,
                UInt16GetDatum((*scanEntry).strategy),
                PointerGetDatum((*scanEntry).extra_data as *const c_void),
            ));

            if cmp > 0 {
                return true;
            } else if cmp < 0 {
                (*stack).off += 1;
                continue;
            }
        } else if (*scanEntry).searchMode == GIN_SEARCH_MODE_ALL {
            /*
             * In ALL mode, we are not interested in null items, so we can
             * stop if we get to a null-item placeholder (which will be the
             * last entry for a given attnum).  We do want to include NULL_KEY
             * and EMPTY_ITEM entries, though.
             */
            if icategory == GIN_CAT_NULL_ITEM {
                return true;
            }
        }

        /*
         * OK, we want to return the TIDs listed in this entry.
         */
        if GinIsPostingTree(itup as *const ItemPointerData) {
            let rootPostingTree: BlockNumber = GinGetPostingTree(&(*itup).t_tid);

            /*
             * We should unlock current page (but not unpin) during tree scan
             * to prevent deadlock with vacuum processes.
             *
             * We save current entry value (idatum) to be able to re-find our
             * tuple after re-locking
             */
            if icategory == GIN_CAT_NORM_KEY {
                idatum = datumCopy(idatum, (*attr).attbyval, (*attr).attlen as c_int);
            }

            LockBuffer((*stack).buffer, GIN_UNLOCK);

            /*
             * Acquire predicate lock on the posting tree.  We already hold a
             * lock on the entry page, but insertions to the posting tree
             * don't check for conflicts on that level.
             */
            PredicateLockPage((*btree).index, rootPostingTree, snapshot as _);

            /* Collect all the TIDs in this entry's posting tree */
            scanPostingTree((*btree).index, scanEntry, rootPostingTree);

            /*
             * We lock again the entry page and while it was unlocked insert
             * might have occurred, so we need to re-find our position.
             */
            LockBuffer((*stack).buffer, GIN_SHARE);
            page = BufferGetPage((*stack).buffer);
            if !crate::access::gin::ginblock::GinPageIsLeaf(page) {
                /*
                 * Root page becomes non-leaf while we unlock it. We will
                 * start again, this situation doesn't occur often - root can
                 * became a non-leaf only once per life of index.
                 */
                return false;
            }

            /* Search forward to re-find idatum */
            loop {
                if moveRightIfItNeeded(btree, stack, snapshot) == false {
                    ereport!(
                        ERROR,
                        errmsg!(
                            "failed to re-find tuple within index \"{}\"",
                            CStr::from_ptr(RelationGetRelationName((*btree).index)).to_string_lossy()
                        )
                    );
                    // C also: errcode(ERRCODE_INTERNAL_ERROR)
                }

                page = BufferGetPage((*stack).buffer);
                itup = PageGetItem(page, PageGetItemId(page, (*stack).off)) as IndexTuple;

                if gintuple_get_attrnum((*btree).ginstate, itup) == attnum {
                    let newDatum: Datum;
                    let mut newCategory: GinNullCategory = 0;

                    newDatum = gintuple_get_key((*btree).ginstate, itup, &mut newCategory);

                    if ginCompareEntries(
                        (*btree).ginstate,
                        attnum,
                        newDatum,
                        newCategory,
                        idatum,
                        icategory,
                    ) == 0
                    {
                        break; /* Found! */
                    }
                }

                (*stack).off += 1;
            }

            if icategory == GIN_CAT_NORM_KEY && !(*attr).attbyval {
                pfree(DatumGetPointer(idatum) as *mut c_void);
            }
        } else {
            let ipd: ItemPointer;
            let mut nipd: c_int = 0;

            ipd = ginReadTuple((*btree).ginstate, (*scanEntry).attnum, itup, &mut nipd);
            tbm_add_tuples((*scanEntry).matchBitmap, ipd, nipd, false);
            (*scanEntry).predictNumberResult +=
                GinGetNPosting(&(*itup).t_tid) as uint32;
            pfree(ipd as *mut c_void);
        }

        /*
         * Done with this entry, go to the next
         */
        (*stack).off += 1;
    }
}

/*
 * Start* functions setup beginning state of searches: finds correct buffer and pins it.
 */
unsafe fn startScanEntry(ginstate: *mut GinState, entry: GinScanEntry, snapshot: Snapshot) {
    let mut btreeEntry: GinBtreeData = std::mem::zeroed();
    let stackEntry: *mut GinBtreeStack;
    let page: Page;
    let mut needUnlock: bool;

    'restartScanEntry: loop {
        (*entry).buffer = InvalidBuffer;
        ItemPointerSetMin(&mut (*entry).curItem);
        (*entry).offset = InvalidOffsetNumber;
        if !(*entry).list.is_null() {
            pfree((*entry).list as *mut c_void);
        }
        (*entry).list = std::ptr::null_mut();
        (*entry).nlist = 0;
        (*entry).matchBitmap = std::ptr::null_mut();
        (*entry).matchNtuples = -1;
        (*entry).matchResult.blockno = InvalidBlockNumber;
        (*entry).reduceResult = false;
        (*entry).predictNumberResult = 0;

        /*
         * we should find entry, and begin scan of posting tree or just store
         * posting list in memory
         */
        ginPrepareEntryScan(
            &mut btreeEntry,
            (*entry).attnum,
            (*entry).queryKey,
            (*entry).queryCategory,
            ginstate,
        );
        stackEntry = ginFindLeafPage(&mut btreeEntry, true, false);
        page = BufferGetPage((*stackEntry).buffer);

        /* ginFindLeafPage() will have already checked snapshot age. */
        needUnlock = true;

        (*entry).isFinished = true;

        if (*entry).isPartialMatch || (*entry).queryCategory == GIN_CAT_EMPTY_QUERY {
            /*
             * btreeEntry.findItem locates the first item >= given search key.
             * (For GIN_CAT_EMPTY_QUERY, it will find the leftmost index item
             * because of the way the GIN_CAT_EMPTY_QUERY category code is
             * assigned.)  We scan forward from there and collect all TIDs needed
             * for the entry type.
             */
            (btreeEntry.findItem.unwrap())(&mut btreeEntry, stackEntry);
            if collectMatchBitmap(&mut btreeEntry, stackEntry, entry, snapshot) == false {
                /*
                 * GIN tree was seriously restructured, so we will cleanup all
                 * found data and rescan. See comments near 'return false' in
                 * collectMatchBitmap()
                 */
                if !(*entry).matchBitmap.is_null() {
                    if !(*entry).matchIterator.is_null() {
                        tbm_end_private_iterate((*entry).matchIterator);
                    }
                    (*entry).matchIterator = std::ptr::null_mut();
                    tbm_free((*entry).matchBitmap);
                    (*entry).matchBitmap = std::ptr::null_mut();
                }
                LockBuffer((*stackEntry).buffer, GIN_UNLOCK);
                freeGinBtreeStack(stackEntry);
                continue 'restartScanEntry;
            }

            if !(*entry).matchBitmap.is_null() && !tbm_is_empty((*entry).matchBitmap) {
                (*entry).matchIterator = tbm_begin_private_iterate((*entry).matchBitmap);
                (*entry).isFinished = false;
            }
        } else if (btreeEntry.findItem.unwrap())(&mut btreeEntry, stackEntry) {
            let itup: IndexTuple =
                PageGetItem(page, PageGetItemId(page, (*stackEntry).off)) as IndexTuple;

            if GinIsPostingTree(itup as *const ItemPointerData) {
                let rootPostingTree: BlockNumber = GinGetPostingTree(&(*itup).t_tid);
                let stack: *mut GinBtreeStack;
                let entrypage: Page;
                let mut minItem: ItemPointerData = std::mem::zeroed();

                /*
                 * This is an equality scan, so lock the root of the posting tree.
                 * It represents a lock on the exact key value, and covers all the
                 * items in the posting tree.
                 */
                PredicateLockPage((*ginstate).index, rootPostingTree, snapshot as _);

                /*
                 * We should unlock entry page before touching posting tree to
                 * prevent deadlocks with vacuum processes. Because entry is never
                 * deleted from page and posting tree is never reduced to the
                 * posting list, we can unlock page after getting BlockNumber of
                 * root of posting tree.
                 */
                LockBuffer((*stackEntry).buffer, GIN_UNLOCK);
                needUnlock = false;

                stack = ginScanBeginPostingTree(&mut (*entry).btree, (*ginstate).index, rootPostingTree);
                (*entry).buffer = (*stack).buffer;

                /*
                 * We keep buffer pinned because we need to prevent deletion of
                 * page during scan. See GIN's vacuum implementation. RefCount is
                 * increased to keep buffer pinned after freeGinBtreeStack() call.
                 */
                IncrBufferRefCount((*entry).buffer);

                entrypage = BufferGetPage((*entry).buffer);

                /*
                 * Load the first page into memory.
                 */
                ItemPointerSetMin(&mut minItem);
                (*entry).list = GinDataLeafPageGetItems(entrypage, &mut (*entry).nlist, minItem);

                (*entry).predictNumberResult = (*stack).predictNumber * (*entry).nlist as uint32;

                LockBuffer((*entry).buffer, GIN_UNLOCK);
                freeGinBtreeStack(stack);
                (*entry).isFinished = false;
            } else {
                /*
                 * Lock the entry leaf page.  This is more coarse-grained than
                 * necessary, because it will conflict with any insertions that
                 * land on the same leaf page, not only the exact key we searched
                 * for.  But locking an individual tuple would require updating
                 * that lock whenever it moves because of insertions or vacuums,
                 * which seems too complicated.
                 */
                PredicateLockPage(
                    (*ginstate).index,
                    BufferGetBlockNumber((*stackEntry).buffer),
                    snapshot as _,
                );
                if GinGetNPosting(&(*itup).t_tid) > 0 {
                    (*entry).list =
                        ginReadTuple(ginstate, (*entry).attnum, itup, &mut (*entry).nlist);
                    (*entry).predictNumberResult = (*entry).nlist as uint32;

                    (*entry).isFinished = false;
                }
            }
        } else {
            /*
             * No entry found.  Predicate lock the leaf page, to lock the place
             * where the entry would've been, had there been one.
             */
            PredicateLockPage(
                (*ginstate).index,
                BufferGetBlockNumber((*stackEntry).buffer),
                snapshot as _,
            );
        }

        if needUnlock {
            LockBuffer((*stackEntry).buffer, GIN_UNLOCK);
        }
        freeGinBtreeStack(stackEntry);
        break;
    }
}

/*
 * Comparison function for scan entry indexes. Sorts by predictNumberResult,
 * least frequent items first.
 */
unsafe fn entryIndexByFrequencyCmp(a1: *const c_void, a2: *const c_void, arg: *mut c_void) -> c_int {
    let key: GinScanKey = arg as GinScanKey;
    let i1: c_int = *(a1 as *const c_int);
    let i2: c_int = *(a2 as *const c_int);
    let n1: uint32 = (*(*(*key).scanEntry.add(i1 as usize))).predictNumberResult;
    let n2: uint32 = (*(*(*key).scanEntry.add(i2 as usize))).predictNumberResult;

    if n1 < n2 {
        -1
    } else if n1 == n2 {
        0
    } else {
        1
    }
}

unsafe fn startScanKey(ginstate: *mut GinState, so: GinScanOpaque, key: GinScanKey) {
    let oldCtx: MemoryContext = CurrentMemoryContext;
    let mut i: c_int;
    let mut j: c_int;
    let entryIndexes: *mut c_int;

    ItemPointerSetMin(&mut (*key).curItem);
    (*key).curItemMatches = false;
    (*key).recheckCurItem = false;
    (*key).isFinished = false;

    /*
     * Divide the entries into two distinct sets: required and additional.
     * Additional entries are not enough for a match alone, without any items
     * from the required set, but are needed by the consistent function to
     * decide if an item matches. When scanning, we can skip over items from
     * additional entries that have no corresponding matches in any of the
     * required entries. That speeds up queries like "frequent & rare"
     * considerably, if the frequent term can be put in the additional set.
     *
     * There can be many legal ways to divide them entries into these two
     * sets. A conservative division is to just put everything in the required
     * set, but the more you can put in the additional set, the more you can
     * skip during the scan. To maximize skipping, we try to put as many
     * frequent items as possible into additional, and less frequent ones into
     * required. To do that, sort the entries by frequency
     * (predictNumberResult), and put entries into the required set in that
     * order, until the consistent function says that none of the remaining
     * entries can form a match, without any items from the required set. The
     * rest go to the additional set.
     *
     * Exclude-only scan keys are known to have no required entries.
     */
    if (*key).excludeOnly {
        MemoryContextSwitchTo((*so).keyCtx);

        (*key).nrequired = 0;
        (*key).nadditional = (*key).nentries as c_int;
        (*key).additionalEntries =
            palloc((*key).nadditional as usize * size_of::<GinScanEntry>()) as *mut GinScanEntry;
        i = 0;
        while i < (*key).nadditional {
            *(*key).additionalEntries.add(i as usize) = *(*key).scanEntry.add(i as usize);
            i += 1;
        }
    } else if (*key).nentries > 1 {
        MemoryContextSwitchTo((*so).tempCtx);

        entryIndexes = palloc(size_of::<c_int>() * (*key).nentries as usize) as *mut c_int;
        i = 0;
        while i < (*key).nentries as c_int {
            *entryIndexes.add(i as usize) = i;
            i += 1;
        }
        qsort_arg(
            entryIndexes as *mut c_void,
            (*key).nentries as Size,
            size_of::<c_int>() as Size,
            entryIndexByFrequencyCmp,
            key as *mut c_void,
        );

        i = 1;
        while i < (*key).nentries as c_int {
            *(*key).entryRes.add(*entryIndexes.add(i as usize) as usize) = GIN_MAYBE;
            i += 1;
        }
        i = 0;
        while i < (*key).nentries as c_int - 1 {
            /* Pass all entries <= i as FALSE, and the rest as MAYBE */
            *(*key).entryRes.add(*entryIndexes.add(i as usize) as usize) = GIN_FALSE;

            if ((*key).triConsistentFn.unwrap())(key) == GIN_FALSE {
                break;
            }

            /* Make this loop interruptible in case there are many keys */
            CHECK_FOR_INTERRUPTS();
            i += 1;
        }
        /* i is now the last required entry. */

        MemoryContextSwitchTo((*so).keyCtx);

        (*key).nrequired = i + 1;
        (*key).nadditional = (*key).nentries as c_int - (*key).nrequired;
        (*key).requiredEntries =
            palloc((*key).nrequired as usize * size_of::<GinScanEntry>()) as *mut GinScanEntry;
        (*key).additionalEntries =
            palloc((*key).nadditional as usize * size_of::<GinScanEntry>()) as *mut GinScanEntry;

        j = 0;
        i = 0;
        while i < (*key).nrequired {
            *(*key).requiredEntries.add(i as usize) =
                *(*key).scanEntry.add(*entryIndexes.add(j as usize) as usize);
            j += 1;
            i += 1;
        }
        i = 0;
        while i < (*key).nadditional {
            *(*key).additionalEntries.add(i as usize) =
                *(*key).scanEntry.add(*entryIndexes.add(j as usize) as usize);
            j += 1;
            i += 1;
        }

        /* clean up after consistentFn calls (also frees entryIndexes) */
        MemoryContextReset((*so).tempCtx);
    } else {
        MemoryContextSwitchTo((*so).keyCtx);

        (*key).nrequired = 1;
        (*key).nadditional = 0;
        (*key).requiredEntries = palloc(1 * size_of::<GinScanEntry>()) as *mut GinScanEntry;
        *(*key).requiredEntries.add(0) = *(*key).scanEntry.add(0);
    }
    MemoryContextSwitchTo(oldCtx);
}

unsafe fn startScan(scan: IndexScanDesc) {
    let so: GinScanOpaque = (*scan).opaque as GinScanOpaque;
    let ginstate: *mut GinState = &mut (*so).ginstate;
    let mut i: uint32;

    i = 0;
    while i < (*so).totalentries {
        startScanEntry(ginstate, *(*so).entries.add(i as usize), (*scan).xs_snapshot);
        i += 1;
    }

    if GinFuzzySearchLimit > 0 {
        /*
         * If all of keys more than threshold we will try to reduce result, we
         * hope (and only hope, for intersection operation of array our
         * supposition isn't true), that total result will not more than
         * minimal predictNumberResult.
         */
        let mut reduce: bool = true;

        i = 0;
        while i < (*so).totalentries {
            if (*(*(*so).entries.add(i as usize))).predictNumberResult
                <= (*so).totalentries * GinFuzzySearchLimit as uint32
            {
                reduce = false;
                break;
            }
            i += 1;
        }
        if reduce {
            i = 0;
            while i < (*so).totalentries {
                (*(*(*so).entries.add(i as usize))).predictNumberResult /= (*so).totalentries;
                (*(*(*so).entries.add(i as usize))).reduceResult = true;
                i += 1;
            }
        }
    }

    /*
     * Now that we have the estimates for the entry frequencies, finish
     * initializing the scan keys.
     */
    i = 0;
    while i < (*so).nkeys {
        startScanKey(ginstate, so, (*so).keys.add(i as usize));
        i += 1;
    }
}

/*
 * Load the next batch of item pointers from a posting tree.
 *
 * Note that we copy the page into GinScanEntry->list array and unlock it, but
 * keep it pinned to prevent interference with vacuum.
 */
unsafe fn entryLoadMoreItems(
    ginstate: *mut GinState,
    entry: GinScanEntry,
    advancePast: ItemPointerData,
) {
    let mut page: Page;
    let mut i: c_int;
    let mut stepright: bool;

    if !BufferIsValid((*entry).buffer) {
        (*entry).isFinished = true;
        return;
    }

    /*
     * We have two strategies for finding the correct page: step right from
     * the current page, or descend the tree again from the root. If
     * advancePast equals the current item, the next matching item should be
     * on the next page, so we step right. Otherwise, descend from root.
     */
    if ginCompareItemPointers(&mut (*entry).curItem, &advancePast as *const _ as *mut _) == 0 {
        stepright = true;
        LockBuffer((*entry).buffer, GIN_SHARE);
    } else {
        let stack: *mut GinBtreeStack;

        ReleaseBuffer((*entry).buffer);

        /*
         * Set the search key, and find the correct leaf page.
         */
        if ItemPointerIsLossyPage(&advancePast) {
            ItemPointerSet(
                &mut (*entry).btree.itemptr,
                GinItemPointerGetBlockNumber(&advancePast) + 1,
                FirstOffsetNumber,
            );
        } else {
            ItemPointerSet(
                &mut (*entry).btree.itemptr,
                GinItemPointerGetBlockNumber(&advancePast),
                OffsetNumberNext(GinItemPointerGetOffsetNumber(&advancePast)),
            );
        }
        (*entry).btree.fullScan = false;
        stack = ginFindLeafPage(&mut (*entry).btree, true, false);

        /* we don't need the stack, just the buffer. */
        (*entry).buffer = (*stack).buffer;
        IncrBufferRefCount((*entry).buffer);
        freeGinBtreeStack(stack);
        stepright = false;
    }

    elog!(
        DEBUG2,
        "entryLoadMoreItems, {}/{}, skip: {}",
        GinItemPointerGetBlockNumber(&advancePast),
        GinItemPointerGetOffsetNumber(&advancePast),
        !stepright as c_int
    );

    page = BufferGetPage((*entry).buffer);
    loop {
        (*entry).offset = InvalidOffsetNumber;
        if !(*entry).list.is_null() {
            pfree((*entry).list as *mut c_void);
            (*entry).list = std::ptr::null_mut();
            (*entry).nlist = 0;
        }

        if stepright {
            /*
             * We've processed all the entries on this page. If it was the
             * last page in the tree, we're done.
             */
            if GinPageRightMost(page) {
                UnlockReleaseBuffer((*entry).buffer);
                (*entry).buffer = InvalidBuffer;
                (*entry).isFinished = true;
                return;
            }

            /*
             * Step to next page, following the right link. then find the
             * first ItemPointer greater than advancePast.
             */
            (*entry).buffer = ginStepRight((*entry).buffer, (*ginstate).index, GIN_SHARE);
            page = BufferGetPage((*entry).buffer);
        }
        stepright = true;

        if (*GinPageGetOpaque(page)).flags & GIN_DELETED != 0 {
            continue; /* page was deleted by concurrent vacuum */
        }

        /*
         * The first item > advancePast might not be on this page, but
         * somewhere to the right, if the page was split, or a non-match from
         * another key in the query allowed us to skip some items from this
         * entry. Keep following the right-links until we re-find the correct
         * page.
         */
        if !GinPageRightMost(page)
            && ginCompareItemPointers(
                &advancePast as *const _ as *mut _,
                GinDataPageGetRightBound(page),
            ) >= 0
        {
            /*
             * the item we're looking is > the right bound of the page, so it
             * can't be on this page.
             */
            continue;
        }

        (*entry).list = GinDataLeafPageGetItems(page, &mut (*entry).nlist, advancePast);

        i = 0;
        while i < (*entry).nlist {
            if ginCompareItemPointers(
                &advancePast as *const _ as *mut _,
                (*entry).list.add(i as usize),
            ) < 0
            {
                (*entry).offset = i as OffsetNumber;

                if GinPageRightMost(page) {
                    /* after processing the copied items, we're done. */
                    UnlockReleaseBuffer((*entry).buffer);
                    (*entry).buffer = InvalidBuffer;
                } else {
                    LockBuffer((*entry).buffer, GIN_UNLOCK);
                }
                return;
            }
            i += 1;
        }
    }
}

/*
 * Sets entry->curItem to next heap item pointer > advancePast, for one entry
 * of one scan key, or sets entry->isFinished to true if there are no more.
 *
 * Item pointers are returned in ascending order.
 *
 * Note: this can return a "lossy page" item pointer, indicating that the
 * entry potentially matches all items on that heap page.  However, it is
 * not allowed to return both a lossy page pointer and exact (regular)
 * item pointers for the same page.  (Doing so would break the key-combination
 * logic in keyGetItem and scanGetItem; see comment in scanGetItem.)  In the
 * current implementation this is guaranteed by the behavior of tidbitmaps.
 */
unsafe fn entryGetItem(
    ginstate: *mut GinState,
    entry: GinScanEntry,
    mut advancePast: ItemPointerData,
) {
    Assert!(!(*entry).isFinished);

    Assert!(
        !ItemPointerIsValid(&(*entry).curItem)
            || ginCompareItemPointers(&mut (*entry).curItem, &mut advancePast) <= 0
    );

    if !(*entry).matchBitmap.is_null() {
        /* A bitmap result */
        let advancePastBlk: BlockNumber = GinItemPointerGetBlockNumber(&advancePast);
        let advancePastOff: OffsetNumber = GinItemPointerGetOffsetNumber(&advancePast);

        loop {
            /*
             * If we've exhausted all items on this block, move to next block
             * in the bitmap. tbm_private_iterate() sets matchResult.blockno
             * to InvalidBlockNumber when the bitmap is exhausted.
             */
            while (!BlockNumberIsValid((*entry).matchResult.blockno))
                || (!(*entry).matchResult.lossy
                    && (*entry).offset as c_int >= (*entry).matchNtuples)
                || (*entry).matchResult.blockno < advancePastBlk
                || (ItemPointerIsLossyPage(&advancePast)
                    && (*entry).matchResult.blockno == advancePastBlk)
            {
                if !tbm_private_iterate((*entry).matchIterator, &mut (*entry).matchResult) {
                    Assert!(!BlockNumberIsValid((*entry).matchResult.blockno));
                    ItemPointerSetInvalid(&mut (*entry).curItem);
                    tbm_end_private_iterate((*entry).matchIterator);
                    (*entry).matchIterator = std::ptr::null_mut();
                    (*entry).isFinished = true;
                    break;
                }

                /* Exact pages need their tuple offsets extracted. */
                if !(*entry).matchResult.lossy {
                    (*entry).matchNtuples = tbm_extract_page_tuple(
                        &mut (*entry).matchResult,
                        (*entry).matchOffsets.as_mut_ptr(),
                        TBM_MAX_TUPLES_PER_PAGE as uint32,
                    );
                }

                /*
                 * Reset counter to the beginning of entry->matchResult. Note:
                 * entry->offset is still greater than matchResult.ntuples if
                 * matchResult is lossy.  So, on next call we will get next
                 * result from TIDBitmap.
                 */
                (*entry).offset = 0;
            }
            if (*entry).isFinished {
                break;
            }

            /*
             * We're now on the first page after advancePast which has any
             * items on it. If it's a lossy result, return that.
             */
            if (*entry).matchResult.lossy {
                ItemPointerSetLossyPage(&mut (*entry).curItem, (*entry).matchResult.blockno);

                /*
                 * We might as well fall out of the loop; we could not
                 * estimate number of results on this page to support correct
                 * reducing of result even if it's enabled.
                 */
                break;
            }

            /*
             * Not a lossy page. If tuple offsets were extracted,
             * entry->matchNtuples must be > -1
             */
            Assert!((*entry).matchNtuples > -1);

            /* Skip over any offsets <= advancePast, and return that. */
            if (*entry).matchResult.blockno == advancePastBlk {
                Assert!((*entry).matchNtuples > 0);

                /*
                 * First, do a quick check against the last offset on the
                 * page. If that's > advancePast, so are all the other
                 * offsets, so just go back to the top to get the next page.
                 */
                if *(*entry).matchOffsets.as_ptr().add(((*entry).matchNtuples - 1) as usize)
                    <= advancePastOff
                {
                    (*entry).offset = (*entry).matchNtuples as OffsetNumber;
                    continue;
                }

                /* Otherwise scan to find the first item > advancePast */
                while *(*entry).matchOffsets.as_ptr().add((*entry).offset as usize) <= advancePastOff
                {
                    (*entry).offset += 1;
                }
            }

            ItemPointerSet(
                &mut (*entry).curItem,
                (*entry).matchResult.blockno,
                *(*entry).matchOffsets.as_ptr().add((*entry).offset as usize),
            );
            (*entry).offset += 1;

            /* Done unless we need to reduce the result */
            if !(*entry).reduceResult || !dropItem(entry) {
                break;
            }
        }
    } else if !BufferIsValid((*entry).buffer) {
        /*
         * A posting list from an entry tuple, or the last page of a posting
         * tree.
         */
        loop {
            if (*entry).offset as c_int >= (*entry).nlist {
                ItemPointerSetInvalid(&mut (*entry).curItem);
                (*entry).isFinished = true;
                break;
            }

            (*entry).curItem = *(*entry).list.add((*entry).offset as usize);
            (*entry).offset += 1;

            /* If we're not past advancePast, keep scanning */
            if ginCompareItemPointers(&mut (*entry).curItem, &mut advancePast) <= 0 {
                continue;
            }

            /* Done unless we need to reduce the result */
            if !(*entry).reduceResult || !dropItem(entry) {
                break;
            }
        }
    } else {
        /* A posting tree */
        loop {
            /* If we've processed the current batch, load more items */
            while (*entry).offset as c_int >= (*entry).nlist {
                entryLoadMoreItems(ginstate, entry, advancePast);

                if (*entry).isFinished {
                    ItemPointerSetInvalid(&mut (*entry).curItem);
                    return;
                }
            }

            (*entry).curItem = *(*entry).list.add((*entry).offset as usize);
            (*entry).offset += 1;

            /* If we're not past advancePast, keep scanning */
            if ginCompareItemPointers(&mut (*entry).curItem, &mut advancePast) <= 0 {
                continue;
            }

            /* Done unless we need to reduce the result */
            if !(*entry).reduceResult || !dropItem(entry) {
                break;
            }

            /*
             * Advance advancePast (so that entryLoadMoreItems will load the
             * right data), and keep scanning
             */
            advancePast = (*entry).curItem;
        }
    }
}

/*
 * Identify the "current" item among the input entry streams for this scan key
 * that is greater than advancePast, and test whether it passes the scan key
 * qual condition.
 *
 * The current item is the smallest curItem among the inputs.  key->curItem
 * is set to that value.  key->curItemMatches is set to indicate whether that
 * TID passes the consistentFn test.  If so, key->recheckCurItem is set true
 * iff recheck is needed for this item pointer (including the case where the
 * item pointer is a lossy page pointer).
 *
 * If all entry streams are exhausted, sets key->isFinished to true.
 *
 * Item pointers must be returned in ascending order.
 *
 * Note: this can return a "lossy page" item pointer, indicating that the
 * key potentially matches all items on that heap page.  However, it is
 * not allowed to return both a lossy page pointer and exact (regular)
 * item pointers for the same page.  (Doing so would break the key-combination
 * logic in scanGetItem.)
 */
unsafe fn keyGetItem(
    ginstate: *mut GinState,
    tempCtx: MemoryContext,
    key: GinScanKey,
    mut advancePast: ItemPointerData,
) {
    let mut minItem: ItemPointerData = std::mem::zeroed();
    let mut curPageLossy: ItemPointerData = std::mem::zeroed();
    let mut i: uint32;
    let mut haveLossyEntry: bool;
    let mut entry: GinScanEntry;
    let res: GinTernaryValue;
    let oldCtx: MemoryContext;
    let mut allFinished: bool;

    Assert!(!(*key).isFinished);

    /*
     * We might have already tested this item; if so, no need to repeat work.
     * (Note: the ">" case can happen, if advancePast is exact but we
     * previously had to set curItem to a lossy-page pointer.)
     */
    if ginCompareItemPointers(&mut (*key).curItem, &mut advancePast) > 0 {
        return;
    }

    /*
     * Find the minimum item > advancePast among the active entry streams.
     *
     * Note: a lossy-page entry is encoded by a ItemPointer with max value for
     * offset (0xffff), so that it will sort after any exact entries for the
     * same page.  So we'll prefer to return exact pointers not lossy
     * pointers, which is good.
     */
    ItemPointerSetMax(&mut minItem);
    allFinished = true;
    i = 0;
    while i < (*key).nrequired as uint32 {
        entry = *(*key).requiredEntries.add(i as usize);

        if (*entry).isFinished {
            i += 1;
            continue;
        }

        /*
         * Advance this stream if necessary.
         *
         * In particular, since entry->curItem was initialized with
         * ItemPointerSetMin, this ensures we fetch the first item for each
         * entry on the first call.
         */
        if ginCompareItemPointers(&mut (*entry).curItem, &mut advancePast) <= 0 {
            entryGetItem(ginstate, entry, advancePast);
            if (*entry).isFinished {
                i += 1;
                continue;
            }
        }

        allFinished = false;
        if ginCompareItemPointers(&mut (*entry).curItem, &mut minItem) < 0 {
            minItem = (*entry).curItem;
        }
        i += 1;
    }

    if allFinished && !(*key).excludeOnly {
        /* all entries are finished */
        (*key).isFinished = true;
        return;
    }

    if !(*key).excludeOnly {
        /*
         * For a normal scan key, we now know there are no matches < minItem.
         *
         * If minItem is lossy, it means that there were no exact items on the
         * page among requiredEntries, because lossy pointers sort after exact
         * items. However, there might be exact items for the same page among
         * additionalEntries, so we mustn't advance past them.
         */
        if ItemPointerIsLossyPage(&minItem) {
            if GinItemPointerGetBlockNumber(&advancePast) < GinItemPointerGetBlockNumber(&minItem) {
                ItemPointerSet(
                    &mut advancePast,
                    GinItemPointerGetBlockNumber(&minItem),
                    InvalidOffsetNumber,
                );
            }
        } else {
            Assert!(GinItemPointerGetOffsetNumber(&minItem) > 0);
            ItemPointerSet(
                &mut advancePast,
                GinItemPointerGetBlockNumber(&minItem),
                OffsetNumberPrev(GinItemPointerGetOffsetNumber(&minItem)),
            );
        }
    } else {
        /*
         * excludeOnly scan keys don't have any entries that are necessarily
         * present in matching items.  So, we consider the item just after
         * advancePast.
         */
        Assert!((*key).nrequired == 0);
        ItemPointerSet(
            &mut minItem,
            GinItemPointerGetBlockNumber(&advancePast),
            OffsetNumberNext(GinItemPointerGetOffsetNumber(&advancePast)),
        );
    }

    /*
     * We might not have loaded all the entry streams for this TID yet. We
     * could call the consistent function, passing MAYBE for those entries, to
     * see if it can decide if this TID matches based on the information we
     * have. But if the consistent-function is expensive, and cannot in fact
     * decide with partial information, that could be a big loss. So, load all
     * the additional entries, before calling the consistent function.
     */
    i = 0;
    while i < (*key).nadditional as uint32 {
        entry = *(*key).additionalEntries.add(i as usize);

        if (*entry).isFinished {
            i += 1;
            continue;
        }

        if ginCompareItemPointers(&mut (*entry).curItem, &mut advancePast) <= 0 {
            entryGetItem(ginstate, entry, advancePast);
            if (*entry).isFinished {
                i += 1;
                continue;
            }
        }

        /*
         * Normally, none of the items in additionalEntries can have a curItem
         * larger than minItem. But if minItem is a lossy page, then there
         * might be exact items on the same page among additionalEntries.
         */
        if ginCompareItemPointers(&mut (*entry).curItem, &mut minItem) < 0 {
            Assert!(ItemPointerIsLossyPage(&minItem));
            minItem = (*entry).curItem;
        }
        i += 1;
    }

    /*
     * Ok, we've advanced all the entries up to minItem now. Set key->curItem,
     * and perform consistentFn test.
     *
     * Lossy-page entries pose a problem, since we don't know the correct
     * entryRes state to pass to the consistentFn, and we also don't know what
     * its combining logic will be (could be AND, OR, or even NOT). If the
     * logic is OR then the consistentFn might succeed for all items in the
     * lossy page even when none of the other entries match.
     *
     * Our strategy is to call the tri-state consistent function, with the
     * lossy-page entries set to MAYBE, and all the other entries FALSE. If it
     * returns FALSE, none of the lossy items alone are enough for a match, so
     * we don't need to return a lossy-page pointer. Otherwise, return a
     * lossy-page pointer to indicate that the whole heap page must be
     * checked.  (On subsequent calls, we'll do nothing until minItem is past
     * the page altogether, thus ensuring that we never return both regular
     * and lossy pointers for the same page.)
     *
     * An exception is that it doesn't matter what we pass for lossy pointers
     * in "hidden" entries, because the consistentFn's result can't depend on
     * them. We could pass them as MAYBE as well, but if we're using the
     * "shim" implementation of a tri-state consistent function (see
     * ginlogic.c), it's better to pass as few MAYBEs as possible. So pass
     * them as true.
     *
     * Note that only lossy-page entries pointing to the current item's page
     * should trigger this processing; we might have future lossy pages in the
     * entry array, but they aren't relevant yet.
     */
    (*key).curItem = minItem;
    ItemPointerSetLossyPage(&mut curPageLossy, GinItemPointerGetBlockNumber(&(*key).curItem));
    haveLossyEntry = false;
    i = 0;
    while i < (*key).nentries {
        entry = *(*key).scanEntry.add(i as usize);
        if (*entry).isFinished == false
            && ginCompareItemPointers(&mut (*entry).curItem, &mut curPageLossy) == 0
        {
            if i < (*key).nuserentries {
                *(*key).entryRes.add(i as usize) = GIN_MAYBE;
            } else {
                *(*key).entryRes.add(i as usize) = GIN_TRUE;
            }
            haveLossyEntry = true;
        } else {
            *(*key).entryRes.add(i as usize) = GIN_FALSE;
        }
        i += 1;
    }

    /* prepare for calling consistentFn in temp context */
    oldCtx = MemoryContextSwitchTo(tempCtx);

    if haveLossyEntry {
        /* Have lossy-page entries, so see if whole page matches */
        let res2: GinTernaryValue = ((*key).triConsistentFn.unwrap())(key);

        if res2 == GIN_TRUE || res2 == GIN_MAYBE {
            /* Yes, so clean up ... */
            MemoryContextSwitchTo(oldCtx);
            MemoryContextReset(tempCtx);

            /* and return lossy pointer for whole page */
            (*key).curItem = curPageLossy;
            (*key).curItemMatches = true;
            (*key).recheckCurItem = true;
            return;
        }
    }

    /*
     * At this point we know that we don't need to return a lossy whole-page
     * pointer, but we might have matches for individual exact item pointers,
     * possibly in combination with a lossy pointer. Pass lossy pointers as
     * MAYBE to the ternary consistent function, to let it decide if this
     * tuple satisfies the overall key, even though we don't know if the lossy
     * entries match.
     *
     * Prepare entryRes array to be passed to consistentFn.
     */
    i = 0;
    while i < (*key).nentries {
        entry = *(*key).scanEntry.add(i as usize);
        if (*entry).isFinished {
            *(*key).entryRes.add(i as usize) = GIN_FALSE;
        }
        /*
         * #if 0
         * This case can't currently happen, because we loaded all the entries
         * for this item earlier.
         * else if (ginCompareItemPointers(&entry->curItem, &advancePast) <= 0)
         *     key->entryRes[i] = GIN_MAYBE;
         * #endif
         */
        else if ginCompareItemPointers(&mut (*entry).curItem, &mut curPageLossy) == 0 {
            *(*key).entryRes.add(i as usize) = GIN_MAYBE;
        } else if ginCompareItemPointers(&mut (*entry).curItem, &mut minItem) == 0 {
            *(*key).entryRes.add(i as usize) = GIN_TRUE;
        } else {
            *(*key).entryRes.add(i as usize) = GIN_FALSE;
        }
        i += 1;
    }

    res = ((*key).triConsistentFn.unwrap())(key);

    match res {
        GIN_TRUE => {
            (*key).curItemMatches = true;
            /* triConsistentFn set recheckCurItem */
        }

        GIN_FALSE => {
            (*key).curItemMatches = false;
        }

        GIN_MAYBE => {
            (*key).curItemMatches = true;
            (*key).recheckCurItem = true;
        }

        _ => {
            /*
             * the 'default' case shouldn't happen, but if the consistent
             * function returns something bogus, this is the safe result
             */
            (*key).curItemMatches = true;
            (*key).recheckCurItem = true;
        }
    }

    /*
     * We have a tuple, and we know if it matches or not. If it's a non-match,
     * we could continue to find the next matching tuple, but let's break out
     * and give scanGetItem a chance to advance the other keys. They might be
     * able to skip past to a much higher TID, allowing us to save work.
     */

    /* clean up after consistentFn calls */
    MemoryContextSwitchTo(oldCtx);
    MemoryContextReset(tempCtx);
}

/*
 * Get next heap item pointer (after advancePast) from scan.
 * Returns true if anything found.
 * On success, *item and *recheck are set.
 *
 * Note: this is very nearly the same logic as in keyGetItem(), except
 * that we know the keys are to be combined with AND logic, whereas in
 * keyGetItem() the combination logic is known only to the consistentFn.
 */
unsafe fn scanGetItem(
    scan: IndexScanDesc,
    mut advancePast: ItemPointerData,
    item: *mut ItemPointerData,
    recheck: *mut bool,
) -> bool {
    let so: GinScanOpaque = (*scan).opaque as GinScanOpaque;
    let mut i: uint32;
    let mut match_: bool;

    /*----------
     * Advance the scan keys in lock-step, until we find an item that matches
     * all the keys. If any key reports isFinished, meaning its subset of the
     * entries is exhausted, we can stop.  Otherwise, set *item to the next
     * matching item.
     *
     * This logic works only if a keyGetItem stream can never contain both
     * exact and lossy pointers for the same page.  Else we could have a
     * case like
     *
     *		stream 1		stream 2
     *		...             ...
     *		42/6			42/7
     *		50/1			42/0xffff
     *		...             ...
     *
     * We would conclude that 42/6 is not a match and advance stream 1,
     * thus never detecting the match to the lossy pointer in stream 2.
     * (keyGetItem has a similar problem versus entryGetItem.)
     *----------
     */
    loop {
        CHECK_FOR_INTERRUPTS();

        ItemPointerSetMin(item);
        match_ = true;
        i = 0;
        while i < (*so).nkeys && match_ {
            let key: GinScanKey = (*so).keys.add(i as usize);

            /*
             * If we're considering a lossy page, skip excludeOnly keys. They
             * can't exclude the whole page anyway.
             */
            if ItemPointerIsLossyPage(item) && (*key).excludeOnly {
                /*
                 * ginNewScanKey() should never mark the first key as
                 * excludeOnly.
                 */
                Assert!(i > 0);
                i += 1;
                continue;
            }

            /* Fetch the next item for this key that is > advancePast. */
            keyGetItem(&mut (*so).ginstate, (*so).tempCtx, key, advancePast);

            if (*key).isFinished {
                return false;
            }

            /*
             * If it's not a match, we can immediately conclude that nothing
             * <= this item matches, without checking the rest of the keys.
             */
            if !(*key).curItemMatches {
                advancePast = (*key).curItem;
                match_ = false;
                break;
            }

            /*
             * It's a match. We can conclude that nothing < matches, so the
             * other key streams can skip to this item.
             *
             * Beware of lossy pointers, though; from a lossy pointer, we can
             * only conclude that nothing smaller than this *block* matches.
             */
            if ItemPointerIsLossyPage(&(*key).curItem) {
                if GinItemPointerGetBlockNumber(&advancePast)
                    < GinItemPointerGetBlockNumber(&(*key).curItem)
                {
                    ItemPointerSet(
                        &mut advancePast,
                        GinItemPointerGetBlockNumber(&(*key).curItem),
                        InvalidOffsetNumber,
                    );
                }
            } else {
                Assert!(GinItemPointerGetOffsetNumber(&(*key).curItem) > 0);
                ItemPointerSet(
                    &mut advancePast,
                    GinItemPointerGetBlockNumber(&(*key).curItem),
                    OffsetNumberPrev(GinItemPointerGetOffsetNumber(&(*key).curItem)),
                );
            }

            /*
             * If this is the first key, remember this location as a potential
             * match, and proceed to check the rest of the keys.
             *
             * Otherwise, check if this is the same item that we checked the
             * previous keys for (or a lossy pointer for the same page). If
             * not, loop back to check the previous keys for this item (we
             * will check this key again too, but keyGetItem returns quickly
             * for that)
             */
            if i == 0 {
                *item = (*key).curItem;
            } else {
                if ItemPointerIsLossyPage(&(*key).curItem) || ItemPointerIsLossyPage(item) {
                    Assert!(
                        GinItemPointerGetBlockNumber(&(*key).curItem)
                            >= GinItemPointerGetBlockNumber(item)
                    );
                    match_ = GinItemPointerGetBlockNumber(&(*key).curItem)
                        == GinItemPointerGetBlockNumber(item);
                } else {
                    Assert!(ginCompareItemPointers(&mut (*key).curItem, item) >= 0);
                    match_ = ginCompareItemPointers(&mut (*key).curItem, item) == 0;
                }
            }
            i += 1;
        }

        if match_ {
            break;
        }
    }

    Assert!(!ItemPointerIsMin(item));

    /*
     * Now *item contains the first ItemPointer after previous result that
     * satisfied all the keys for that exact TID, or a lossy reference to the
     * same page.
     *
     * We must return recheck = true if any of the keys are marked recheck.
     */
    *recheck = false;
    i = 0;
    while i < (*so).nkeys {
        let key: GinScanKey = (*so).keys.add(i as usize);

        if (*key).recheckCurItem {
            *recheck = true;
            break;
        }
        i += 1;
    }

    true
}

/*
 * Functions for scanning the pending list
 */


/*
 * Get ItemPointer of next heap row to be checked from pending list.
 * Returns false if there are no more. On pages with several heap rows
 * it returns each row separately, on page with part of heap row returns
 * per page data.  pos->firstOffset and pos->lastOffset are set to identify
 * the range of pending-list tuples belonging to this heap row.
 *
 * The pendingBuffer is presumed pinned and share-locked on entry, and is
 * pinned and share-locked on success exit.  On failure exit it's released.
 */
unsafe fn scanGetCandidate(scan: IndexScanDesc, pos: *mut pendingPosition) -> bool {
    let mut maxoff: OffsetNumber;
    let mut page: Page;
    let mut itup: IndexTuple;

    ItemPointerSetInvalid(&mut (*pos).item);
    loop {
        page = BufferGetPage((*pos).pendingBuffer);

        maxoff = PageGetMaxOffsetNumber(page);
        if (*pos).firstOffset > maxoff {
            let blkno: BlockNumber = (*GinPageGetOpaque(page)).rightlink;

            if blkno == InvalidBlockNumber {
                UnlockReleaseBuffer((*pos).pendingBuffer);
                (*pos).pendingBuffer = InvalidBuffer;

                return false;
            } else {
                /*
                 * Here we must prevent deletion of next page by insertcleanup
                 * process, which may be trying to obtain exclusive lock on
                 * current page.  So, we lock next page before releasing the
                 * current one
                 */
                let tmpbuf: Buffer = ReadBuffer((*scan).indexRelation, blkno);

                LockBuffer(tmpbuf, GIN_SHARE);
                UnlockReleaseBuffer((*pos).pendingBuffer);

                (*pos).pendingBuffer = tmpbuf;
                (*pos).firstOffset = FirstOffsetNumber;
            }
        } else {
            itup = PageGetItem(page, PageGetItemId(page, (*pos).firstOffset)) as IndexTuple;
            (*pos).item = (*itup).t_tid;
            if GinPageHasFullRow(page) {
                /*
                 * find itempointer to the next row
                 */
                (*pos).lastOffset = (*pos).firstOffset + 1;
                while (*pos).lastOffset <= maxoff {
                    itup = PageGetItem(page, PageGetItemId(page, (*pos).lastOffset)) as IndexTuple;
                    if !ItemPointerEquals(&mut (*pos).item, &mut (*itup).t_tid) {
                        break;
                    }
                    (*pos).lastOffset += 1;
                }
            } else {
                /*
                 * All itempointers are the same on this page
                 */
                (*pos).lastOffset = maxoff + 1;
            }

            /*
             * Now pos->firstOffset points to the first tuple of current heap
             * row, pos->lastOffset points to the first tuple of next heap row
             * (or to the end of page)
             */
            break;
        }
    }

    true
}

/*
 * Scan pending-list page from current tuple (off) up till the first of:
 * - match is found (then returns true)
 * - no later match is possible
 * - tuple's attribute number is not equal to entry's attrnum
 * - reach end of page
 *
 * datum[]/category[]/datumExtracted[] arrays are used to cache the results
 * of gintuple_get_key() on the current page.
 */
unsafe fn matchPartialInPendingList(
    ginstate: *mut GinState,
    page: Page,
    mut off: OffsetNumber,
    maxoff: OffsetNumber,
    entry: GinScanEntry,
    datum: *mut Datum,
    category: *mut GinNullCategory,
    datumExtracted: *mut bool,
) -> bool {
    let mut itup: IndexTuple;
    let cmp: int32;

    /* Partial match to a null is not possible */
    if (*entry).queryCategory != GIN_CAT_NORM_KEY {
        return false;
    }

    while off < maxoff {
        itup = PageGetItem(page, PageGetItemId(page, off)) as IndexTuple;

        if gintuple_get_attrnum(ginstate, itup) != (*entry).attnum {
            return false;
        }

        if *datumExtracted.add((off - 1) as usize) == false {
            *datum.add((off - 1) as usize) =
                gintuple_get_key(ginstate, itup, &mut *category.add((off - 1) as usize));
            *datumExtracted.add((off - 1) as usize) = true;
        }

        /* Once we hit nulls, no further match is possible */
        if *category.add((off - 1) as usize) != GIN_CAT_NORM_KEY {
            return false;
        }

        /*----------
         * Check partial match.
         * case cmp == 0 => match
         * case cmp > 0 => not match and end scan (no later match possible)
         * case cmp < 0 => not match and continue scan
         *----------
         */
        let cmp_inner: int32 = DatumGetInt32(FunctionCall4Coll(
            &mut (*ginstate).comparePartialFn[((*entry).attnum - 1) as usize],
            (*ginstate).supportCollation[((*entry).attnum - 1) as usize],
            (*entry).queryKey,
            *datum.add((off - 1) as usize),
            UInt16GetDatum((*entry).strategy),
            PointerGetDatum((*entry).extra_data as *const c_void),
        ));
        cmp = cmp_inner;
        if cmp == 0 {
            return true;
        } else if cmp > 0 {
            return false;
        }

        off += 1;
    }

    false
}

/*
 * Set up the entryRes array for each key by looking at
 * every entry for current heap row in pending list.
 *
 * Returns true if each scan key has at least one entryRes match.
 * This corresponds to the situations where the normal index search will
 * try to apply the key's consistentFn.  (A tuple not meeting that requirement
 * cannot be returned by the normal search since no entry stream will
 * source its TID.)
 *
 * The pendingBuffer is presumed pinned and share-locked on entry.
 */
unsafe fn collectMatchesForHeapRow(scan: IndexScanDesc, pos: *mut pendingPosition) -> bool {
    let so: GinScanOpaque = (*scan).opaque as GinScanOpaque;
    let mut attrnum: OffsetNumber;
    let mut page: Page;
    let mut itup: IndexTuple;
    let mut i: c_int;
    let mut j: c_int;

    /*
     * Reset all entryRes and hasMatchKey flags
     */
    i = 0;
    while i < (*so).nkeys as c_int {
        let key: GinScanKey = (*so).keys.add(i as usize);

        memset((*key).entryRes as *mut c_void, GIN_FALSE as c_int, (*key).nentries as usize);
        i += 1;
    }
    memset((*pos).hasMatchKey as *mut c_void, false as c_int, (*so).nkeys as usize);

    /*
     * Outer loop iterates over multiple pending-list pages when a single heap
     * row has entries spanning those pages.
     */
    loop {
        // Datum datum[BLCKSZ / sizeof(IndexTupleData)];
        let mut datum: [Datum; (BLCKSZ as usize) / size_of::<IndexTupleData>()] =
            [0 as Datum; (BLCKSZ as usize) / size_of::<IndexTupleData>()];
        let mut category: [GinNullCategory; (BLCKSZ as usize) / size_of::<IndexTupleData>()] =
            [0; (BLCKSZ as usize) / size_of::<IndexTupleData>()];
        let mut datumExtracted: [bool; (BLCKSZ as usize) / size_of::<IndexTupleData>()] =
            [false; (BLCKSZ as usize) / size_of::<IndexTupleData>()];

        Assert!((*pos).lastOffset > (*pos).firstOffset);
        memset(
            datumExtracted.as_mut_ptr().add(((*pos).firstOffset - 1) as usize) as *mut c_void,
            0,
            size_of::<bool>() * ((*pos).lastOffset - (*pos).firstOffset) as usize,
        );

        page = BufferGetPage((*pos).pendingBuffer);

        i = 0;
        while i < (*so).nkeys as c_int {
            let key: GinScanKey = (*so).keys.add(i as usize);

            j = 0;
            while j < (*key).nentries as c_int {
                let entry: GinScanEntry = *(*key).scanEntry.add(j as usize);
                let mut StopLow: OffsetNumber = (*pos).firstOffset;
                let mut StopHigh: OffsetNumber = (*pos).lastOffset;
                let mut StopMiddle: OffsetNumber;

                /* If already matched on earlier page, do no extra work */
                if *(*key).entryRes.add(j as usize) != 0 {
                    j += 1;
                    continue;
                }

                /*
                 * Interesting tuples are from pos->firstOffset to
                 * pos->lastOffset and they are ordered by (attnum, Datum) as
                 * it's done in entry tree.  So we can use binary search to
                 * avoid linear scanning.
                 */
                while StopLow < StopHigh {
                    let res: int32;

                    StopMiddle = StopLow + ((StopHigh - StopLow) >> 1);

                    itup = PageGetItem(page, PageGetItemId(page, StopMiddle)) as IndexTuple;

                    attrnum = gintuple_get_attrnum(&mut (*so).ginstate, itup);

                    if (*key).attnum < attrnum {
                        StopHigh = StopMiddle;
                        continue;
                    }
                    if (*key).attnum > attrnum {
                        StopLow = StopMiddle + 1;
                        continue;
                    }

                    if *datumExtracted.as_ptr().add((StopMiddle - 1) as usize) == false {
                        datum[(StopMiddle - 1) as usize] = gintuple_get_key(
                            &mut (*so).ginstate,
                            itup,
                            &mut category[(StopMiddle - 1) as usize],
                        );
                        datumExtracted[(StopMiddle - 1) as usize] = true;
                    }

                    if (*entry).queryCategory == GIN_CAT_EMPTY_QUERY {
                        /* special behavior depending on searchMode */
                        if (*entry).searchMode == GIN_SEARCH_MODE_ALL {
                            /* match anything except NULL_ITEM */
                            if category[(StopMiddle - 1) as usize] == GIN_CAT_NULL_ITEM {
                                res = -1;
                            } else {
                                res = 0;
                            }
                        } else {
                            /* match everything */
                            res = 0;
                        }
                    } else {
                        res = ginCompareEntries(
                            &mut (*so).ginstate,
                            (*entry).attnum,
                            (*entry).queryKey,
                            (*entry).queryCategory,
                            datum[(StopMiddle - 1) as usize],
                            category[(StopMiddle - 1) as usize],
                        );
                    }

                    if res == 0 {
                        /*
                         * Found exact match (there can be only one, except in
                         * EMPTY_QUERY mode).
                         *
                         * If doing partial match, scan forward from here to
                         * end of page to check for matches.
                         *
                         * See comment above about tuple's ordering.
                         */
                        if (*entry).isPartialMatch {
                            *(*key).entryRes.add(j as usize) = matchPartialInPendingList(
                                &mut (*so).ginstate,
                                page,
                                StopMiddle,
                                (*pos).lastOffset,
                                entry,
                                datum.as_mut_ptr(),
                                category.as_mut_ptr(),
                                datumExtracted.as_mut_ptr(),
                            ) as GinTernaryValue;
                        } else {
                            *(*key).entryRes.add(j as usize) = true as GinTernaryValue;
                        }

                        /* done with binary search */
                        break;
                    } else if res < 0 {
                        StopHigh = StopMiddle;
                    } else {
                        StopLow = StopMiddle + 1;
                    }
                }

                if StopLow >= StopHigh && (*entry).isPartialMatch {
                    /*
                     * No exact match on this page.  If doing partial match,
                     * scan from the first tuple greater than target value to
                     * end of page.  Note that since we don't remember whether
                     * the comparePartialFn told us to stop early on a
                     * previous page, we will uselessly apply comparePartialFn
                     * to the first tuple on each subsequent page.
                     */
                    *(*key).entryRes.add(j as usize) = matchPartialInPendingList(
                        &mut (*so).ginstate,
                        page,
                        StopHigh,
                        (*pos).lastOffset,
                        entry,
                        datum.as_mut_ptr(),
                        category.as_mut_ptr(),
                        datumExtracted.as_mut_ptr(),
                    ) as GinTernaryValue;
                }

                *(*pos).hasMatchKey.add(i as usize) |= *(*key).entryRes.add(j as usize) != 0;
                j += 1;
            }
            i += 1;
        }

        /* Advance firstOffset over the scanned tuples */
        (*pos).firstOffset = (*pos).lastOffset;

        if GinPageHasFullRow(page) {
            /*
             * We have examined all pending entries for the current heap row.
             * Break out of loop over pages.
             */
            break;
        } else {
            /*
             * Advance to next page of pending entries for the current heap
             * row.  Complain if there isn't one.
             */
            let mut item: ItemPointerData = (*pos).item;

            if scanGetCandidate(scan, pos) == false
                || !ItemPointerEquals(&mut (*pos).item, &mut item)
            {
                elog!(
                    ERROR,
                    "could not find additional pending pages for same heap tuple"
                );
            }
        }
    }

    /*
     * All scan keys except excludeOnly require at least one entry to match.
     * excludeOnly keys are an exception, because their implied
     * GIN_CAT_EMPTY_QUERY scanEntry always matches.  So return "true" if all
     * non-excludeOnly scan keys have at least one match.
     */
    i = 0;
    while i < (*so).nkeys as c_int {
        if *(*pos).hasMatchKey.add(i as usize) == false && !(*(*so).keys.add(i as usize)).excludeOnly
        {
            return false;
        }
        i += 1;
    }

    true
}

/*
 * Collect all matched rows from pending list into bitmap.
 */
unsafe fn scanPendingInsert(scan: IndexScanDesc, tbm: *mut TIDBitmap, ntids: *mut int64) {
    let so: GinScanOpaque = (*scan).opaque as GinScanOpaque;
    let mut oldCtx: MemoryContext;
    let mut recheck: bool;
    let mut match_: bool;
    let mut i: c_int;
    let mut pos: pendingPosition = std::mem::zeroed();
    let metabuffer: Buffer = ReadBuffer((*scan).indexRelation, GIN_METAPAGE_BLKNO);
    let page: Page;
    let blkno: BlockNumber;

    *ntids = 0;

    /*
     * Acquire predicate lock on the metapage, to conflict with any fastupdate
     * insertions.
     */
    PredicateLockPage(
        (*scan).indexRelation,
        GIN_METAPAGE_BLKNO,
        (*scan).xs_snapshot as _,
    );

    LockBuffer(metabuffer, GIN_SHARE);
    page = BufferGetPage(metabuffer);
    blkno = (*GinPageGetMeta(page)).head;

    /*
     * fetch head of list before unlocking metapage. head page must be pinned
     * to prevent deletion by vacuum process
     */
    if blkno == InvalidBlockNumber {
        /* No pending list, so proceed with normal scan */
        UnlockReleaseBuffer(metabuffer);
        return;
    }

    pos.pendingBuffer = ReadBuffer((*scan).indexRelation, blkno);
    LockBuffer(pos.pendingBuffer, GIN_SHARE);
    pos.firstOffset = FirstOffsetNumber;
    UnlockReleaseBuffer(metabuffer);
    pos.hasMatchKey = palloc(size_of::<bool>() * (*so).nkeys as usize) as *mut bool;

    /*
     * loop for each heap row. scanGetCandidate returns full row or row's
     * tuples from first page.
     */
    while scanGetCandidate(scan, &mut pos) {
        /*
         * Check entries in tuple and set up entryRes array.
         *
         * If pending tuples belonging to the current heap row are spread
         * across several pages, collectMatchesForHeapRow will read all of
         * those pages.
         */
        if !collectMatchesForHeapRow(scan, &mut pos) {
            continue;
        }

        /*
         * Matching of entries of one row is finished, so check row using
         * consistent functions.
         */
        oldCtx = MemoryContextSwitchTo((*so).tempCtx);
        recheck = false;
        match_ = true;

        i = 0;
        while i < (*so).nkeys as c_int {
            let key: GinScanKey = (*so).keys.add(i as usize);

            if !((*key).boolConsistentFn.unwrap())(key) {
                match_ = false;
                break;
            }
            recheck |= (*key).recheckCurItem;
            i += 1;
        }

        MemoryContextSwitchTo(oldCtx);
        MemoryContextReset((*so).tempCtx);

        if match_ {
            tbm_add_tuples(tbm, &mut pos.item, 1, recheck);
            *ntids += 1;
        }
    }

    pfree(pos.hasMatchKey as *mut c_void);
}


// #define GinIsVoidRes(s)		( ((GinScanOpaque) scan->opaque)->isVoidRes )

#[no_mangle]
pub unsafe extern "C" fn gingetbitmap(scan: IndexScanDesc, tbm: *mut TIDBitmap) -> int64 {
    let so: GinScanOpaque = (*scan).opaque as GinScanOpaque;
    let mut ntids: int64;
    let mut iptr: ItemPointerData = std::mem::zeroed();
    let mut recheck: bool = false;

    /*
     * Set up the scan keys, and check for unsatisfiable query.
     */
    ginFreeScanKeys(so); /* there should be no keys yet, but just to be
                          * sure */
    ginNewScanKey(scan);

    if (*((*scan).opaque as GinScanOpaque)).isVoidRes {
        return 0;
    }

    ntids = 0;

    /*
     * First, scan the pending list and collect any matching entries into the
     * bitmap.  After we scan a pending item, some other backend could post it
     * into the main index, and so we might visit it a second time during the
     * main scan.  This is okay because we'll just re-set the same bit in the
     * bitmap.  (The possibility of duplicate visits is a major reason why GIN
     * can't support the amgettuple API, however.) Note that it would not do
     * to scan the main index before the pending list, since concurrent
     * cleanup could then make us miss entries entirely.
     */
    scanPendingInsert(scan, tbm, &mut ntids);

    /*
     * Now scan the main index.
     */
    startScan(scan);

    ItemPointerSetMin(&mut iptr);

    loop {
        if !scanGetItem(scan, iptr, &mut iptr, &mut recheck) {
            break;
        }

        if ItemPointerIsLossyPage(&iptr) {
            tbm_add_page(tbm, ItemPointerGetBlockNumber(&iptr));
        } else {
            tbm_add_tuples(tbm, &mut iptr, 1, recheck);
        }
        ntids += 1;
    }

    ntids
}
