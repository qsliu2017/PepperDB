//! src/backend/access/gin/ginvacuum.c
//!
//! delete & vacuum routines for the postgres GIN
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::prelude::*;

use std::ffi::c_void;

use crate::access::transam::xlogdefs::XLogRecPtr;
use crate::c::uint32;
use crate::postgres_ext::Oid;
use crate::storage::block::BlockNumber;

// ---------------------------------------------------------------------------
// Type aliases / stub types for unported dependencies
// ---------------------------------------------------------------------------

type Relation = *mut c_void;
type Buffer = c_int;
type Page = *mut c_void;
type Item = *mut c_void;
type ItemId = *mut c_void;
type ItemPointer = *mut ItemPointerData;
type IndexTuple = *mut IndexTupleData;
type OffsetNumber = u16;
type BufferAccessStrategy = *mut c_void;
type TransactionId = crate::c::TransactionId;
type GinNullCategory = i8;
type GinPostingList = c_void;
type PostingItem = c_void;
type LOCKMODE = c_int;

#[repr(C)]
pub struct ItemPointerData {
    _opaque: [u8; 6],
}

#[repr(C)]
pub struct IndexTupleData {
    _opaque: [u8; 8],
}

#[repr(C)]
pub struct GinState {
    _opaque: [u8; 0],
}

#[repr(C)]
pub struct IndexBulkDeleteResult {
    pub num_pages: BlockNumber,
    pub estimated_count: bool,
    pub num_index_tuples: f64,
    pub tuples_removed: f64,
    pub pages_newly_deleted: BlockNumber,
    pub pages_deleted: BlockNumber,
    pub pages_free: BlockNumber,
}

#[repr(C)]
pub struct IndexVacuumInfo {
    pub index: Relation,
    pub heaprel: Relation,
    pub analyze_only: bool,
    pub report_progress: bool,
    pub estimated_count: bool,
    pub message_level: c_int,
    pub num_heap_tuples: f64,
    pub strategy: BufferAccessStrategy,
}

#[repr(C)]
pub struct GinStatsData {
    pub nPendingPages: BlockNumber,
    pub nTotalPages: BlockNumber,
    pub nEntryPages: BlockNumber,
    pub nDataPages: BlockNumber,
    pub nEntries: i64,
    pub ginVersion: i32,
}

#[repr(C)]
pub struct ginxlogDeletePage {
    pub parentOffset: OffsetNumber,
    pub rightLink: BlockNumber,
    pub deleteXid: TransactionId,
}

/// Callback invoked per item pointer during bulk delete.
pub type IndexBulkDeleteCallback =
    Option<unsafe extern "C" fn(itemptr: ItemPointer, state: *mut c_void) -> bool>;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const InvalidBuffer: Buffer = 0;
const InvalidBlockNumber: BlockNumber = 0xFFFFFFFF;
const FirstOffsetNumber: OffsetNumber = 1;
const InvalidOffsetNumber: OffsetNumber = 0;
const GIN_ROOT_BLKNO: BlockNumber = 1;

const MAIN_FORKNUM: c_int = 0;
const RBM_NORMAL: c_int = 0;

const GIN_UNLOCK: c_int = 0;
const GIN_SHARE: c_int = 1;
const GIN_EXCLUSIVE: c_int = 2;

const REGBUF_STANDARD: c_int = 0x04;
const REGBUF_FORCE_IMAGE: c_int = 0x02;

const RM_GIN_ID: u8 = 0;
const XLOG_GIN_VACUUM_PAGE: u8 = 0x40;
const XLOG_GIN_DELETE_PAGE: u8 = 0x30;

const ExclusiveLock: LOCKMODE = 7;

const GinMaxItemSize: usize = 0; // placeholder; defined in gin_private.h

const ALLOCSET_DEFAULT_MINSIZE: usize = 0;
const ALLOCSET_DEFAULT_INITSIZE: usize = 8 * 1024;
const ALLOCSET_DEFAULT_MAXSIZE: usize = 8 * 1024 * 1024;

// ---------------------------------------------------------------------------
// GinVacuumState
// ---------------------------------------------------------------------------

#[repr(C)]
pub struct GinVacuumState {
    pub index: Relation,
    pub result: *mut IndexBulkDeleteResult,
    pub callback: IndexBulkDeleteCallback,
    pub callback_state: *mut c_void,
    pub ginstate: GinState,
    pub strategy: BufferAccessStrategy,
    pub tmpCxt: MemoryContext,
}

// ---------------------------------------------------------------------------
// ginVacuumItemPointers
// ---------------------------------------------------------------------------

/*
 * Vacuums an uncompressed posting list. The size of the must can be specified
 * in number of items (nitems).
 *
 * If none of the items need to be removed, returns NULL. Otherwise returns
 * a new palloc'd array with the remaining items. The number of remaining
 * items is returned in *nremaining.
 */
pub unsafe fn ginVacuumItemPointers(
    gvs: *mut GinVacuumState,
    items: *mut ItemPointerData,
    nitem: c_int,
    nremaining: *mut c_int,
) -> ItemPointer {
    let mut remaining: c_int = 0;
    let mut tmpitems: ItemPointer = std::ptr::null_mut();

    /*
     * Iterate over TIDs array
     */
    let mut i: c_int = 0;
    while i < nitem {
        if ((*gvs).callback.unwrap())(items.add(i as usize), (*gvs).callback_state) {
            (*(*gvs).result).tuples_removed += 1.0;
            if tmpitems.is_null() {
                /*
                 * First TID to be deleted: allocate memory to hold the
                 * remaining items.
                 */
                tmpitems = palloc(
                    std::mem::size_of::<ItemPointerData>() * nitem as usize,
                ) as ItemPointer;
                memcpy(
                    tmpitems as *mut c_void,
                    items as *const c_void,
                    std::mem::size_of::<ItemPointerData>() * i as usize,
                );
            }
        } else {
            (*(*gvs).result).num_index_tuples += 1.0;
            if !tmpitems.is_null() {
                *tmpitems.add(remaining as usize) = std::ptr::read(items.add(i as usize));
            }
            remaining += 1;
        }
        i += 1;
    }

    *nremaining = remaining;
    tmpitems
}

// ---------------------------------------------------------------------------
// xlogVacuumPage
// ---------------------------------------------------------------------------

/*
 * Create a WAL record for vacuuming entry tree leaf page.
 */
unsafe fn xlogVacuumPage(index: Relation, buffer: Buffer) {
    let page: Page = BufferGetPage(buffer);

    /* This is only used for entry tree leaf pages. */
    debug_assert!(!GinPageIsData(page));
    debug_assert!(GinPageIsLeaf(page));

    if !RelationNeedsWAL(index) {
        return;
    }

    /*
     * Always create a full image, we don't track the changes on the page at
     * any more fine-grained level. This could obviously be improved...
     */
    XLogBeginInsert();
    XLogRegisterBuffer(0, buffer, REGBUF_FORCE_IMAGE | REGBUF_STANDARD);

    let recptr = XLogInsert(RM_GIN_ID, XLOG_GIN_VACUUM_PAGE);
    PageSetLSN(page, recptr);
}

// ---------------------------------------------------------------------------
// DataPageDeleteStack
// ---------------------------------------------------------------------------

#[repr(C)]
pub struct DataPageDeleteStack {
    pub child: *mut DataPageDeleteStack,
    pub parent: *mut DataPageDeleteStack,

    pub blkno: BlockNumber, /* current block number */
    pub leftBuffer: Buffer, /* pinned and locked rightest non-deleted page
                             * on left */
    pub isRoot: bool,
}

// ---------------------------------------------------------------------------
// ginDeletePage
// ---------------------------------------------------------------------------

/*
 * Delete a posting tree page.
 */
unsafe fn ginDeletePage(
    gvs: *mut GinVacuumState,
    deleteBlkno: BlockNumber,
    leftBlkno: BlockNumber,
    parentBlkno: BlockNumber,
    myoff: OffsetNumber,
    _isParentRoot: bool,
) {
    let dBuffer: Buffer;
    let lBuffer: Buffer;
    let pBuffer: Buffer;
    let mut page: Page;
    let parentPage: Page;
    let rightlink: BlockNumber;

    /*
     * This function MUST be called only if someone of parent pages hold
     * exclusive cleanup lock. This guarantees that no insertions currently
     * happen in this subtree. Caller also acquires Exclusive locks on
     * deletable, parent and left pages.
     */
    lBuffer = ReadBufferExtended(
        (*gvs).index,
        MAIN_FORKNUM,
        leftBlkno,
        RBM_NORMAL,
        (*gvs).strategy,
    );
    dBuffer = ReadBufferExtended(
        (*gvs).index,
        MAIN_FORKNUM,
        deleteBlkno,
        RBM_NORMAL,
        (*gvs).strategy,
    );
    pBuffer = ReadBufferExtended(
        (*gvs).index,
        MAIN_FORKNUM,
        parentBlkno,
        RBM_NORMAL,
        (*gvs).strategy,
    );

    page = BufferGetPage(dBuffer);
    rightlink = (*GinPageGetOpaque(page)).rightlink;

    /*
     * Any insert which would have gone on the leaf block will now go to its
     * right sibling.
     */
    PredicateLockPageCombine((*gvs).index, deleteBlkno, rightlink);

    START_CRIT_SECTION();

    /* Unlink the page by changing left sibling's rightlink */
    page = BufferGetPage(lBuffer);
    (*GinPageGetOpaque(page)).rightlink = rightlink;

    /* Delete downlink from parent */
    parentPage = BufferGetPage(pBuffer);
    // USE_ASSERT_CHECKING block
    {
        let tod: *mut PostingItem = GinDataPageGetPostingItem(parentPage, myoff);
        debug_assert!(PostingItemGetBlockNumber(tod) == deleteBlkno);
    }
    GinPageDeletePostingItem(parentPage, myoff);

    page = BufferGetPage(dBuffer);

    /*
     * we shouldn't change rightlink field to save workability of running
     * search scan
     */

    /*
     * Mark page as deleted, and remember last xid which could know its
     * address.
     */
    GinPageSetDeleted(page);
    GinPageSetDeleteXid(page, ReadNextTransactionId());

    MarkBufferDirty(pBuffer);
    MarkBufferDirty(lBuffer);
    MarkBufferDirty(dBuffer);

    if RelationNeedsWAL((*gvs).index) {
        let recptr: XLogRecPtr;
        let mut data: ginxlogDeletePage = std::mem::zeroed();

        /*
         * We can't pass REGBUF_STANDARD for the deleted page, because we
         * didn't set pd_lower on pre-9.4 versions. The page might've been
         * binary-upgraded from an older version, and hence not have pd_lower
         * set correctly. Ditto for the left page, but removing the item from
         * the parent updated its pd_lower, so we know that's OK at this
         * point.
         */
        XLogBeginInsert();
        XLogRegisterBuffer(0, dBuffer, 0);
        XLogRegisterBuffer(1, pBuffer, REGBUF_STANDARD);
        XLogRegisterBuffer(2, lBuffer, 0);

        data.parentOffset = myoff;
        data.rightLink = (*GinPageGetOpaque(page)).rightlink;
        data.deleteXid = GinPageGetDeleteXid(page);

        XLogRegisterData(
            &mut data as *mut ginxlogDeletePage as *mut c_void,
            std::mem::size_of::<ginxlogDeletePage>() as c_int,
        );

        recptr = XLogInsert(RM_GIN_ID, XLOG_GIN_DELETE_PAGE);
        PageSetLSN(page, recptr);
        PageSetLSN(parentPage, recptr);
        PageSetLSN(BufferGetPage(lBuffer), recptr);
    }

    ReleaseBuffer(pBuffer);
    ReleaseBuffer(lBuffer);
    ReleaseBuffer(dBuffer);

    END_CRIT_SECTION();

    (*(*gvs).result).pages_newly_deleted += 1;
    (*(*gvs).result).pages_deleted += 1;
}

// ---------------------------------------------------------------------------
// ginScanToDelete
// ---------------------------------------------------------------------------

/*
 * Scans posting tree and deletes empty pages.  Caller must lock root page for
 * cleanup.  During scan path from root to current page is kept exclusively
 * locked.  Also keep left page exclusively locked, because ginDeletePage()
 * needs it.  If we try to relock left page later, it could deadlock with
 * ginStepRight().
 */
unsafe fn ginScanToDelete(
    gvs: *mut GinVacuumState,
    blkno: BlockNumber,
    isRoot: bool,
    parent: *mut DataPageDeleteStack,
    myoff: OffsetNumber,
) -> bool {
    let me: *mut DataPageDeleteStack;
    let buffer: Buffer;
    let page: Page;
    let mut meDelete: bool = false;
    let isempty: bool;

    if isRoot {
        me = parent;
    } else {
        if (*parent).child.is_null() {
            me = palloc0(std::mem::size_of::<DataPageDeleteStack>())
                as *mut DataPageDeleteStack;
            (*me).parent = parent;
            (*parent).child = me;
            (*me).leftBuffer = InvalidBuffer;
        } else {
            me = (*parent).child;
        }
    }

    buffer = ReadBufferExtended(
        (*gvs).index,
        MAIN_FORKNUM,
        blkno,
        RBM_NORMAL,
        (*gvs).strategy,
    );

    if !isRoot {
        LockBuffer(buffer, GIN_EXCLUSIVE);
    }

    page = BufferGetPage(buffer);

    debug_assert!(GinPageIsData(page));

    if !GinPageIsLeaf(page) {
        (*me).blkno = blkno;
        let mut i: OffsetNumber = FirstOffsetNumber;
        while i <= (*GinPageGetOpaque(page)).maxoff {
            let pitem: *mut PostingItem = GinDataPageGetPostingItem(page, i);

            if ginScanToDelete(gvs, PostingItemGetBlockNumber(pitem), false, me, i) {
                i -= 1;
            }
            i += 1;
        }

        if GinPageRightMost(page) && BufferIsValid((*(*me).child).leftBuffer) {
            UnlockReleaseBuffer((*(*me).child).leftBuffer);
            (*(*me).child).leftBuffer = InvalidBuffer;
        }
    }

    if GinPageIsLeaf(page) {
        isempty = GinDataLeafPageIsEmpty(page);
    } else {
        isempty = (*GinPageGetOpaque(page)).maxoff < FirstOffsetNumber;
    }

    if isempty {
        /* we never delete the left- or rightmost branch */
        if BufferIsValid((*me).leftBuffer) && !GinPageRightMost(page) {
            debug_assert!(!isRoot);
            ginDeletePage(
                gvs,
                blkno,
                BufferGetBlockNumber((*me).leftBuffer),
                (*(*me).parent).blkno,
                myoff,
                (*(*me).parent).isRoot,
            );
            meDelete = true;
        }
    }

    if !meDelete {
        if BufferIsValid((*me).leftBuffer) {
            UnlockReleaseBuffer((*me).leftBuffer);
        }
        (*me).leftBuffer = buffer;
    } else {
        if !isRoot {
            LockBuffer(buffer, GIN_UNLOCK);
        }

        ReleaseBuffer(buffer);
    }

    if isRoot {
        ReleaseBuffer(buffer);
    }

    meDelete
}

// ---------------------------------------------------------------------------
// ginVacuumPostingTreeLeaves
// ---------------------------------------------------------------------------

/*
 * Scan through posting tree leafs, delete empty tuples.  Returns true if there
 * is at least one empty page.
 */
unsafe fn ginVacuumPostingTreeLeaves(gvs: *mut GinVacuumState, mut blkno: BlockNumber) -> bool {
    let mut buffer: Buffer;
    let mut page: Page;
    let mut hasVoidPage: bool = false;
    let mut oldCxt: MemoryContext;

    /* Find leftmost leaf page of posting tree and lock it in exclusive mode */
    loop {
        let pitem: *mut PostingItem;

        buffer = ReadBufferExtended(
            (*gvs).index,
            MAIN_FORKNUM,
            blkno,
            RBM_NORMAL,
            (*gvs).strategy,
        );
        LockBuffer(buffer, GIN_SHARE);
        page = BufferGetPage(buffer);

        debug_assert!(GinPageIsData(page));

        if GinPageIsLeaf(page) {
            LockBuffer(buffer, GIN_UNLOCK);
            LockBuffer(buffer, GIN_EXCLUSIVE);
            break;
        }

        debug_assert!(PageGetMaxOffsetNumber(page) >= FirstOffsetNumber);

        pitem = GinDataPageGetPostingItem(page, FirstOffsetNumber);
        blkno = PostingItemGetBlockNumber(pitem);
        debug_assert!(blkno != InvalidBlockNumber);

        UnlockReleaseBuffer(buffer);
    }

    /* Iterate all posting tree leaves using rightlinks and vacuum them */
    loop {
        oldCxt = MemoryContextSwitchTo((*gvs).tmpCxt);
        ginVacuumPostingTreeLeaf((*gvs).index, buffer, gvs);
        MemoryContextSwitchTo(oldCxt);
        MemoryContextReset((*gvs).tmpCxt);

        if GinDataLeafPageIsEmpty(page) {
            hasVoidPage = true;
        }

        blkno = (*GinPageGetOpaque(page)).rightlink;

        UnlockReleaseBuffer(buffer);

        if blkno == InvalidBlockNumber {
            break;
        }

        buffer = ReadBufferExtended(
            (*gvs).index,
            MAIN_FORKNUM,
            blkno,
            RBM_NORMAL,
            (*gvs).strategy,
        );
        LockBuffer(buffer, GIN_EXCLUSIVE);
        page = BufferGetPage(buffer);
    }

    hasVoidPage
}

// ---------------------------------------------------------------------------
// ginVacuumPostingTree
// ---------------------------------------------------------------------------

unsafe fn ginVacuumPostingTree(gvs: *mut GinVacuumState, rootBlkno: BlockNumber) {
    if ginVacuumPostingTreeLeaves(gvs, rootBlkno) {
        /*
         * There is at least one empty page.  So we have to rescan the tree
         * deleting empty pages.
         */
        let buffer: Buffer;
        let mut root: DataPageDeleteStack;
        let mut ptr: *mut DataPageDeleteStack;
        let mut tmp: *mut DataPageDeleteStack;

        buffer = ReadBufferExtended(
            (*gvs).index,
            MAIN_FORKNUM,
            rootBlkno,
            RBM_NORMAL,
            (*gvs).strategy,
        );

        /*
         * Lock posting tree root for cleanup to ensure there are no
         * concurrent inserts.
         */
        LockBufferForCleanup(buffer);

        root = std::mem::zeroed();
        root.leftBuffer = InvalidBuffer;
        root.isRoot = true;

        ginScanToDelete(gvs, rootBlkno, true, &mut root, InvalidOffsetNumber);

        ptr = root.child;

        while !ptr.is_null() {
            tmp = (*ptr).child;
            pfree(ptr as *mut c_void);
            ptr = tmp;
        }

        UnlockReleaseBuffer(buffer);
    }
}

// ---------------------------------------------------------------------------
// ginVacuumEntryPage
// ---------------------------------------------------------------------------

/*
 * returns modified page or NULL if page isn't modified.
 * Function works with original page until first change is occurred,
 * then page is copied into temporary one.
 */
unsafe fn ginVacuumEntryPage(
    gvs: *mut GinVacuumState,
    buffer: Buffer,
    roots: *mut BlockNumber,
    nroot: *mut uint32,
) -> Page {
    let origpage: Page = BufferGetPage(buffer);
    let mut tmppage: Page;
    let maxoff: OffsetNumber = PageGetMaxOffsetNumber(origpage);

    tmppage = origpage;

    *nroot = 0;

    let mut i: OffsetNumber = FirstOffsetNumber;
    while i <= maxoff {
        let mut itup: IndexTuple =
            PageGetItem(tmppage, PageGetItemId(tmppage, i)) as IndexTuple;

        if GinIsPostingTree(itup) {
            /*
             * store posting tree's roots for further processing, we can't
             * vacuum it just now due to risk of deadlocks with scans/inserts
             */
            *roots.add(*nroot as usize) = GinGetDownlink(itup);
            *nroot += 1;
        } else if GinGetNPosting(itup) > 0 {
            let mut nitems: c_int = 0;
            let items_orig: ItemPointer;
            let free_items_orig: bool;
            let items: ItemPointer;

            /* Get list of item pointers from the tuple. */
            if GinItupIsCompressed(itup) {
                items_orig = ginPostingListDecode(
                    GinGetPosting(itup) as *mut GinPostingList,
                    &mut nitems,
                );
                free_items_orig = true;
            } else {
                items_orig = GinGetPosting(itup) as ItemPointer;
                nitems = GinGetNPosting(itup) as c_int;
                free_items_orig = false;
            }

            /* Remove any items from the list that need to be vacuumed. */
            items = ginVacuumItemPointers(gvs, items_orig, nitems, &mut nitems);

            if free_items_orig {
                pfree(items_orig as *mut c_void);
            }

            /* If any item pointers were removed, recreate the tuple. */
            if !items.is_null() {
                let attnum: OffsetNumber;
                let key: Datum;
                let mut category: GinNullCategory = 0;
                let plist: *mut GinPostingList;
                let plistsize: c_int;

                if nitems > 0 {
                    plist = ginCompressPostingList(
                        items,
                        nitems,
                        GinMaxItemSize,
                        std::ptr::null_mut(),
                    );
                    plistsize = SizeOfGinPostingList(plist);
                } else {
                    plist = std::ptr::null_mut();
                    plistsize = 0;
                }

                /*
                 * if we already created a temporary page, make changes in
                 * place
                 */
                if tmppage == origpage {
                    /*
                     * On first difference, create a temporary copy of the
                     * page and copy the tuple's posting list to it.
                     */
                    tmppage = PageGetTempPageCopy(origpage);

                    /* set itup pointer to new page */
                    itup = PageGetItem(tmppage, PageGetItemId(tmppage, i)) as IndexTuple;
                }

                attnum = gintuple_get_attrnum(&mut (*gvs).ginstate, itup);
                key = gintuple_get_key(&mut (*gvs).ginstate, itup, &mut category);
                itup = GinFormTuple(
                    &mut (*gvs).ginstate,
                    attnum,
                    key,
                    category,
                    plist as *mut c_char,
                    plistsize,
                    nitems,
                    true,
                );
                if !plist.is_null() {
                    pfree(plist as *mut c_void);
                }
                PageIndexTupleDelete(tmppage, i);

                if PageAddItem(
                    tmppage,
                    itup as Item,
                    IndexTupleSize(itup),
                    i,
                    false,
                    false,
                ) != i
                {
                    elog!(
                        ERROR,
                        "failed to add item to index page in \"{:p}\"",
                        RelationGetRelationName((*gvs).index)
                    );
                }

                pfree(itup as *mut c_void);
                pfree(items as *mut c_void);
            }
        }
        i += 1;
    }

    if tmppage == origpage {
        std::ptr::null_mut()
    } else {
        tmppage
    }
}

// ---------------------------------------------------------------------------
// ginbulkdelete
// ---------------------------------------------------------------------------

pub unsafe fn ginbulkdelete(
    info: *mut IndexVacuumInfo,
    mut stats: *mut IndexBulkDeleteResult,
    callback: IndexBulkDeleteCallback,
    callback_state: *mut c_void,
) -> *mut IndexBulkDeleteResult {
    let index: Relation = (*info).index;
    let mut blkno: BlockNumber = GIN_ROOT_BLKNO;
    let mut gvs: GinVacuumState = std::mem::zeroed();
    let mut buffer: Buffer;
    // BlockNumber rootOfPostingTree[BLCKSZ / (sizeof(IndexTupleData) + sizeof(ItemId))];
    const ROOT_ARR_LEN: usize =
        BLCKSZ / (std::mem::size_of::<IndexTupleData>() + std::mem::size_of::<ItemId>());
    let mut rootOfPostingTree: [BlockNumber; ROOT_ARR_LEN] = [0; ROOT_ARR_LEN];
    let mut nRoot: uint32 = 0;

    gvs.tmpCxt = AllocSetContextCreate!(
        CurrentMemoryContext,
        c"Gin vacuum temporary context".as_ptr(),
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE,
    );
    gvs.index = index;
    gvs.callback = callback;
    gvs.callback_state = callback_state;
    gvs.strategy = (*info).strategy;
    initGinState(&mut gvs.ginstate, index);

    /* first time through? */
    if stats.is_null() {
        /* Yes, so initialize stats to zeroes */
        stats =
            palloc0(std::mem::size_of::<IndexBulkDeleteResult>()) as *mut IndexBulkDeleteResult;

        /*
         * and cleanup any pending inserts
         */
        ginInsertCleanup(
            &mut gvs.ginstate,
            !AmAutoVacuumWorkerProcess(),
            false,
            true,
            stats,
        );
    }

    /* we'll re-count the tuples each time */
    (*stats).num_index_tuples = 0.0;
    gvs.result = stats;

    buffer = ReadBufferExtended(index, MAIN_FORKNUM, blkno, RBM_NORMAL, (*info).strategy);

    /* find leaf page */
    loop {
        let page: Page = BufferGetPage(buffer);
        let itup: IndexTuple;

        LockBuffer(buffer, GIN_SHARE);

        debug_assert!(!GinPageIsData(page));

        if GinPageIsLeaf(page) {
            LockBuffer(buffer, GIN_UNLOCK);
            LockBuffer(buffer, GIN_EXCLUSIVE);

            if blkno == GIN_ROOT_BLKNO && !GinPageIsLeaf(page) {
                LockBuffer(buffer, GIN_UNLOCK);
                continue; /* check it one more */
            }
            break;
        }

        debug_assert!(PageGetMaxOffsetNumber(page) >= FirstOffsetNumber);

        itup = PageGetItem(page, PageGetItemId(page, FirstOffsetNumber)) as IndexTuple;
        blkno = GinGetDownlink(itup);
        debug_assert!(blkno != InvalidBlockNumber);

        UnlockReleaseBuffer(buffer);
        buffer = ReadBufferExtended(index, MAIN_FORKNUM, blkno, RBM_NORMAL, (*info).strategy);
    }

    /* right now we found leftmost page in entry's BTree */

    loop {
        let page: Page = BufferGetPage(buffer);
        let resPage: Page;

        debug_assert!(!GinPageIsData(page));

        resPage = ginVacuumEntryPage(&mut gvs, buffer, rootOfPostingTree.as_mut_ptr(), &mut nRoot);

        blkno = (*GinPageGetOpaque(page)).rightlink;

        if !resPage.is_null() {
            START_CRIT_SECTION();
            PageRestoreTempPage(resPage, page);
            MarkBufferDirty(buffer);
            xlogVacuumPage(gvs.index, buffer);
            UnlockReleaseBuffer(buffer);
            END_CRIT_SECTION();
        } else {
            UnlockReleaseBuffer(buffer);
        }

        vacuum_delay_point(false);

        let mut i: uint32 = 0;
        while i < nRoot {
            ginVacuumPostingTree(&mut gvs, rootOfPostingTree[i as usize]);
            vacuum_delay_point(false);
            i += 1;
        }

        if blkno == InvalidBlockNumber {
            /* rightmost page */
            break;
        }

        buffer = ReadBufferExtended(index, MAIN_FORKNUM, blkno, RBM_NORMAL, (*info).strategy);
        LockBuffer(buffer, GIN_EXCLUSIVE);
    }

    MemoryContextDelete(gvs.tmpCxt);

    gvs.result
}

// ---------------------------------------------------------------------------
// ginvacuumcleanup
// ---------------------------------------------------------------------------

pub unsafe fn ginvacuumcleanup(
    info: *mut IndexVacuumInfo,
    mut stats: *mut IndexBulkDeleteResult,
) -> *mut IndexBulkDeleteResult {
    let index: Relation = (*info).index;
    let needLock: bool;
    let npages: BlockNumber;
    let mut blkno: BlockNumber;
    let mut totFreePages: BlockNumber;
    let mut ginstate: GinState = std::mem::zeroed();
    let mut idxStat: GinStatsData;

    /*
     * In an autovacuum analyze, we want to clean up pending insertions.
     * Otherwise, an ANALYZE-only call is a no-op.
     */
    if (*info).analyze_only {
        if AmAutoVacuumWorkerProcess() {
            initGinState(&mut ginstate, index);
            ginInsertCleanup(&mut ginstate, false, true, true, stats);
        }
        return stats;
    }

    /*
     * Set up all-zero stats and cleanup pending inserts if ginbulkdelete
     * wasn't called
     */
    if stats.is_null() {
        stats =
            palloc0(std::mem::size_of::<IndexBulkDeleteResult>()) as *mut IndexBulkDeleteResult;
        initGinState(&mut ginstate, index);
        ginInsertCleanup(
            &mut ginstate,
            !AmAutoVacuumWorkerProcess(),
            false,
            true,
            stats,
        );
    }

    idxStat = std::mem::zeroed();

    /*
     * XXX we always report the heap tuple count as the number of index
     * entries.  This is bogus if the index is partial, but it's real hard to
     * tell how many distinct heap entries are referenced by a GIN index.
     */
    (*stats).num_index_tuples = Max((*info).num_heap_tuples, 0.0);
    (*stats).estimated_count = (*info).estimated_count;

    /*
     * Need lock unless it's local to this backend.
     */
    needLock = !RELATION_IS_LOCAL(index);

    if needLock {
        LockRelationForExtension(index, ExclusiveLock);
    }
    npages = RelationGetNumberOfBlocks(index);
    if needLock {
        UnlockRelationForExtension(index, ExclusiveLock);
    }

    totFreePages = 0;

    blkno = GIN_ROOT_BLKNO;
    while blkno < npages {
        let buffer: Buffer;
        let page: Page;

        vacuum_delay_point(false);

        buffer = ReadBufferExtended(index, MAIN_FORKNUM, blkno, RBM_NORMAL, (*info).strategy);
        LockBuffer(buffer, GIN_SHARE);
        page = BufferGetPage(buffer);

        if GinPageIsRecyclable(page) {
            debug_assert!(blkno != GIN_ROOT_BLKNO);
            RecordFreeIndexPage(index, blkno);
            totFreePages += 1;
        } else if GinPageIsData(page) {
            idxStat.nDataPages += 1;
        } else if !GinPageIsList(page) {
            idxStat.nEntryPages += 1;

            if GinPageIsLeaf(page) {
                idxStat.nEntries += PageGetMaxOffsetNumber(page) as i64;
            }
        }

        UnlockReleaseBuffer(buffer);

        blkno += 1;
    }

    /* Update the metapage with accurate page and entry counts */
    idxStat.nTotalPages = npages;
    ginUpdateStats((*info).index, &idxStat, false);

    /* Finally, vacuum the FSM */
    IndexFreeSpaceMapVacuum((*info).index);

    (*stats).pages_free = totFreePages;

    if needLock {
        LockRelationForExtension(index, ExclusiveLock);
    }
    (*stats).num_pages = RelationGetNumberOfBlocks(index);
    if needLock {
        UnlockRelationForExtension(index, ExclusiveLock);
    }

    stats
}

// ---------------------------------------------------------------------------
// GinPageIsRecyclable
// ---------------------------------------------------------------------------

/*
 * Return whether Page can safely be recycled.
 */
pub unsafe fn GinPageIsRecyclable(page: Page) -> bool {
    let delete_xid: TransactionId;

    if PageIsNew(page) {
        return true;
    }

    if !GinPageIsDeleted(page) {
        return false;
    }

    delete_xid = GinPageGetDeleteXid(page);

    if !TransactionIdIsValid(delete_xid) {
        return true;
    }

    /*
     * If no backend still could view delete_xid as in running, all scans
     * concurrent with ginDeletePage() must have finished.
     */
    GlobalVisCheckRemovableXid(std::ptr::null_mut(), delete_xid)
}

// ---------------------------------------------------------------------------
// Local stubs for unported helpers
// ---------------------------------------------------------------------------

const BLCKSZ: usize = 8192;

#[repr(C)]
struct GinPageOpaqueData {
    rightlink: BlockNumber,
    maxoff: OffsetNumber,
    flags: u16,
}

extern "C" {
    fn memcpy(dest: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
}

// `Max` is provided by the prelude (crate::c::Max, a generic helper); no local copy.

unsafe fn BufferGetPage(_buffer: Buffer) -> Page {
    unimplemented!() // TODO: storage/bufmgr.h
}
unsafe fn BufferGetBlockNumber(_buffer: Buffer) -> BlockNumber {
    unimplemented!() // TODO: storage/bufmgr.h
}
unsafe fn BufferIsValid(_buffer: Buffer) -> bool {
    unimplemented!() // TODO: storage/bufmgr.h
}
unsafe fn ReadBufferExtended(
    _reln: Relation,
    _forkNum: c_int,
    _blockNum: BlockNumber,
    _mode: c_int,
    _strategy: BufferAccessStrategy,
) -> Buffer {
    unimplemented!() // TODO: storage/bufmgr.h
}
unsafe fn ReleaseBuffer(_buffer: Buffer) {
    unimplemented!() // TODO: storage/bufmgr.h
}
unsafe fn UnlockReleaseBuffer(_buffer: Buffer) {
    unimplemented!() // TODO: storage/bufmgr.h
}
unsafe fn MarkBufferDirty(_buffer: Buffer) {
    unimplemented!() // TODO: storage/bufmgr.h
}
unsafe fn LockBuffer(_buffer: Buffer, _mode: c_int) {
    unimplemented!() // TODO: storage/bufmgr.h
}
unsafe fn LockBufferForCleanup(_buffer: Buffer) {
    unimplemented!() // TODO: storage/bufmgr.h
}

unsafe fn GinPageGetOpaque(_page: Page) -> *mut GinPageOpaqueData {
    unimplemented!() // TODO: access/gin_private.h
}
unsafe fn GinPageIsData(_page: Page) -> bool {
    unimplemented!() // TODO: access/gin_private.h
}
unsafe fn GinPageIsLeaf(_page: Page) -> bool {
    unimplemented!() // TODO: access/gin_private.h
}
unsafe fn GinPageIsList(_page: Page) -> bool {
    unimplemented!() // TODO: access/gin_private.h
}
unsafe fn GinPageIsDeleted(_page: Page) -> bool {
    unimplemented!() // TODO: access/gin_private.h
}
unsafe fn GinPageRightMost(_page: Page) -> bool {
    unimplemented!() // TODO: access/gin_private.h
}
unsafe fn GinPageSetDeleted(_page: Page) {
    unimplemented!() // TODO: access/gin_private.h
}
unsafe fn GinPageSetDeleteXid(_page: Page, _xid: TransactionId) {
    unimplemented!() // TODO: access/gin_private.h
}
unsafe fn GinPageGetDeleteXid(_page: Page) -> TransactionId {
    unimplemented!() // TODO: access/gin_private.h
}
unsafe fn GinDataLeafPageIsEmpty(_page: Page) -> bool {
    unimplemented!() // TODO: access/gin_private.h
}
unsafe fn GinDataPageGetPostingItem(_page: Page, _i: OffsetNumber) -> *mut PostingItem {
    unimplemented!() // TODO: access/gin_private.h
}
unsafe fn GinPageDeletePostingItem(_page: Page, _off: OffsetNumber) {
    unimplemented!() // TODO: access/gin_private.h
}
unsafe fn PostingItemGetBlockNumber(_pitem: *mut PostingItem) -> BlockNumber {
    unimplemented!() // TODO: access/gin_private.h
}
unsafe fn GinIsPostingTree(_itup: IndexTuple) -> bool {
    unimplemented!() // TODO: access/gin_private.h
}
unsafe fn GinGetDownlink(_itup: IndexTuple) -> BlockNumber {
    unimplemented!() // TODO: access/gin_private.h
}
unsafe fn GinGetNPosting(_itup: IndexTuple) -> i32 {
    unimplemented!() // TODO: access/gin_private.h
}
unsafe fn GinItupIsCompressed(_itup: IndexTuple) -> bool {
    unimplemented!() // TODO: access/gin_private.h
}
unsafe fn GinGetPosting(_itup: IndexTuple) -> *mut c_void {
    unimplemented!() // TODO: access/gin_private.h
}
unsafe fn SizeOfGinPostingList(_plist: *mut GinPostingList) -> c_int {
    unimplemented!() // TODO: access/gin_private.h
}
unsafe fn ginPostingListDecode(_plist: *mut GinPostingList, _ndecoded: *mut c_int) -> ItemPointer {
    unimplemented!() // TODO: access/gin_private.h
}
unsafe fn ginCompressPostingList(
    _ipd: ItemPointer,
    _nipd: c_int,
    _maxsize: usize,
    _nwritten: *mut c_int,
) -> *mut GinPostingList {
    unimplemented!() // TODO: access/gin_private.h
}
unsafe fn GinFormTuple(
    _ginstate: *mut GinState,
    _attnum: OffsetNumber,
    _key: Datum,
    _category: GinNullCategory,
    _data: *mut c_char,
    _dataSize: c_int,
    _nipd: c_int,
    _errorTooBig: bool,
) -> IndexTuple {
    unimplemented!() // TODO: access/gin_private.h
}
unsafe fn gintuple_get_attrnum(_ginstate: *mut GinState, _tuple: IndexTuple) -> OffsetNumber {
    unimplemented!() // TODO: access/gin_private.h
}
unsafe fn gintuple_get_key(
    _ginstate: *mut GinState,
    _tuple: IndexTuple,
    _category: *mut GinNullCategory,
) -> Datum {
    unimplemented!() // TODO: access/gin_private.h
}
unsafe fn ginVacuumPostingTreeLeaf(_indexrel: Relation, _buffer: Buffer, _gvs: *mut GinVacuumState) {
    unimplemented!() // TODO: access/gin_private.h
}
unsafe fn initGinState(_state: *mut GinState, _index: Relation) {
    unimplemented!() // TODO: access/gin_private.h
}
unsafe fn ginInsertCleanup(
    _ginstate: *mut GinState,
    _full_clean: bool,
    _fill_fsm: bool,
    _forceCleanup: bool,
    _stats: *mut IndexBulkDeleteResult,
) {
    unimplemented!() // TODO: access/gin_private.h
}
unsafe fn ginUpdateStats(_index: Relation, _stats: *const GinStatsData, _is_build: bool) {
    unimplemented!() // TODO: access/gin_private.h
}

unsafe fn PageGetMaxOffsetNumber(_page: Page) -> OffsetNumber {
    unimplemented!() // TODO: storage/bufpage.h
}
unsafe fn PageGetItem(_page: Page, _itemId: ItemId) -> *mut c_void {
    unimplemented!() // TODO: storage/bufpage.h
}
unsafe fn PageGetItemId(_page: Page, _offsetNumber: OffsetNumber) -> ItemId {
    unimplemented!() // TODO: storage/bufpage.h
}
unsafe fn PageGetTempPageCopy(_page: Page) -> Page {
    unimplemented!() // TODO: storage/bufpage.h
}
unsafe fn PageRestoreTempPage(_tempPage: Page, _oldPage: Page) {
    unimplemented!() // TODO: storage/bufpage.h
}
unsafe fn PageIndexTupleDelete(_page: Page, _offnum: OffsetNumber) {
    unimplemented!() // TODO: storage/bufpage.h
}
unsafe fn PageAddItem(
    _page: Page,
    _item: Item,
    _size: usize,
    _offsetNumber: OffsetNumber,
    _overwrite: bool,
    _is_heap: bool,
) -> OffsetNumber {
    unimplemented!() // TODO: storage/bufpage.h
}
unsafe fn PageSetLSN(_page: Page, _lsn: XLogRecPtr) {
    unimplemented!() // TODO: storage/bufpage.h
}
unsafe fn PageIsNew(_page: Page) -> bool {
    unimplemented!() // TODO: storage/bufpage.h
}
unsafe fn IndexTupleSize(_itup: IndexTuple) -> usize {
    unimplemented!() // TODO: access/itup.h
}

unsafe fn RelationNeedsWAL(_relation: Relation) -> bool {
    unimplemented!() // TODO: utils/rel.h
}
unsafe fn RelationGetRelationName(_relation: Relation) -> *const c_char {
    unimplemented!() // TODO: utils/rel.h
}
unsafe fn RelationGetNumberOfBlocks(_relation: Relation) -> BlockNumber {
    unimplemented!() // TODO: storage/bufmgr.h
}
unsafe fn RELATION_IS_LOCAL(_relation: Relation) -> bool {
    unimplemented!() // TODO: utils/rel.h
}

unsafe fn XLogBeginInsert() {
    unimplemented!() // TODO: access/xloginsert.h
}
unsafe fn XLogRegisterBuffer(_block_id: c_int, _buffer: Buffer, _flags: c_int) {
    unimplemented!() // TODO: access/xloginsert.h
}
unsafe fn XLogRegisterData(_data: *mut c_void, _len: c_int) {
    unimplemented!() // TODO: access/xloginsert.h
}
unsafe fn XLogInsert(_rmid: u8, _info: u8) -> XLogRecPtr {
    unimplemented!() // TODO: access/xlog.h
}

unsafe fn START_CRIT_SECTION() {
    unimplemented!() // TODO: miscadmin.h
}
unsafe fn END_CRIT_SECTION() {
    unimplemented!() // TODO: miscadmin.h
}
unsafe fn AmAutoVacuumWorkerProcess() -> bool {
    unimplemented!() // TODO: miscadmin.h
}

unsafe fn ReadNextTransactionId() -> TransactionId {
    unimplemented!() // TODO: access/transam.h
}
unsafe fn TransactionIdIsValid(_xid: TransactionId) -> bool {
    unimplemented!() // TODO: access/transam.h
}
unsafe fn GlobalVisCheckRemovableXid(_rel: Relation, _xid: TransactionId) -> bool {
    unimplemented!() // TODO: utils/snapmgr.h
}

unsafe fn PredicateLockPageCombine(_relation: Relation, _oldblkno: BlockNumber, _newblkno: BlockNumber) {
    unimplemented!() // TODO: storage/predicate.h
}
unsafe fn LockRelationForExtension(_relation: Relation, _lockmode: LOCKMODE) {
    unimplemented!() // TODO: storage/lmgr.h
}
unsafe fn UnlockRelationForExtension(_relation: Relation, _lockmode: LOCKMODE) {
    unimplemented!() // TODO: storage/lmgr.h
}
unsafe fn RecordFreeIndexPage(_rel: Relation, _freePage: BlockNumber) {
    unimplemented!() // TODO: storage/indexfsm.h
}
unsafe fn IndexFreeSpaceMapVacuum(_rel: Relation) {
    unimplemented!() // TODO: storage/indexfsm.h
}
unsafe fn vacuum_delay_point(_is_analyze: bool) {
    unimplemented!() // TODO: commands/vacuum.h
}

