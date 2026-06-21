//! src/backend/access/gin/ginbtree.c
//!
//! page utilities routines for the postgres inverted index access method.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::prelude::*;

use crate::access::transam::xlogdefs::XLogRecPtr;
use crate::storage::block::BlockNumber;

// ---------------------------------------------------------------------------
// Local type aliases / stubs for unported dependencies
// ---------------------------------------------------------------------------

type Buffer = c_int;
type Page = Pointer;
type Relation = *mut c_void;
type OffsetNumber = uint16;

type GinBtree = *mut GinBtreeData;
type GinStatsData = c_void;

#[repr(C)]
pub struct GinBtreeStack {
    pub blkno: BlockNumber,
    pub buffer: Buffer,
    pub off: OffsetNumber,
    pub iptr: c_void, // ItemPointerData placeholder
    pub predictNumber: uint32,
    pub parent: *mut GinBtreeStack,
}

#[repr(C)]
pub struct GinBtreeData {
    // method callbacks
    pub isMoveRight: unsafe extern "C" fn(GinBtree, Page) -> bool,
    pub findChildPage: unsafe extern "C" fn(GinBtree, *mut GinBtreeStack) -> BlockNumber,
    pub getLeftMostChild: unsafe extern "C" fn(GinBtree, Page) -> BlockNumber,
    pub findChildPtr:
        unsafe extern "C" fn(GinBtree, Page, BlockNumber, OffsetNumber) -> OffsetNumber,
    pub beginPlaceToPage: unsafe extern "C" fn(
        GinBtree,
        Buffer,
        *mut GinBtreeStack,
        *mut c_void,
        BlockNumber,
        *mut *mut c_void,
        *mut Page,
        *mut Page,
    ) -> GinPlaceToPageRC,
    pub execPlaceToPage: unsafe extern "C" fn(
        GinBtree,
        Buffer,
        *mut GinBtreeStack,
        *mut c_void,
        BlockNumber,
        *mut c_void,
    ),
    pub prepareDownlink: unsafe extern "C" fn(GinBtree, Buffer) -> *mut c_void,
    pub fillRoot: unsafe extern "C" fn(GinBtree, Page, BlockNumber, Page, BlockNumber, Page),

    pub fullScan: bool,
    pub isBuild: bool,
    pub isData: bool,

    pub index: Relation,
    pub rootBlkno: BlockNumber,
}

#[repr(C)]
#[derive(PartialEq, Eq, Clone, Copy)]
pub enum GinPlaceToPageRC {
    GPTP_NO_WORK,
    GPTP_INSERT,
    GPTP_SPLIT,
}
use GinPlaceToPageRC::*;

// GIN locking modes
const GIN_UNLOCK: c_int = 0;
const GIN_SHARE: c_int = 1; // BUFFER_LOCK_SHARE
const GIN_EXCLUSIVE: c_int = 2; // BUFFER_LOCK_EXCLUSIVE

// GIN page flags
const GIN_LEAF: uint16 = 1 << 15;
const GIN_COMPRESSED: uint16 = 1 << 12;
const GIN_INCOMPLETE_SPLIT: uint16 = 1 << 11;

// GIN insert/split xlog flags
const GIN_INSERT_ISDATA: uint16 = 0x02;
const GIN_INSERT_ISLEAF: uint16 = 0x01;
const GIN_SPLIT_ROOT: uint16 = 0x04;

const InvalidBlockNumber: BlockNumber = 0xFFFFFFFF;
const InvalidOffsetNumber: OffsetNumber = 0;
const InvalidBuffer: Buffer = 0;

const REGBUF_STANDARD: c_int = 0x04;
const REGBUF_FORCE_IMAGE: c_int = 0x01;

const RM_GIN_ID: u8 = 13;
const XLOG_GIN_INSERT: u8 = 0x10;
const XLOG_GIN_SPLIT: u8 = 0x20;

#[repr(C)]
struct BlockIdData {
    bi_hi: uint16,
    bi_lo: uint16,
}

#[repr(C)]
struct ginxlogInsert {
    node: c_void, // RelFileLocator placeholder
    flags: uint16,
}

#[repr(C)]
struct ginxlogSplit {
    locator: c_void, // RelFileLocator
    flags: uint16,
    leftChildBlkno: BlockNumber,
    rightChildBlkno: BlockNumber,
    rrlink: BlockNumber,
}

// ---------------------------------------------------------------------------
// Unported helper stubs
// ---------------------------------------------------------------------------

unsafe fn LockBuffer(_buffer: Buffer, _mode: c_int) {
    unimplemented!() // TODO: storage/buf/bufmgr.c
}
unsafe fn BufferGetPage(_buffer: Buffer) -> Page {
    unimplemented!() // TODO: storage/buf/bufmgr.c
}
unsafe fn ReadBuffer(_reln: Relation, _blockNum: BlockNumber) -> Buffer {
    unimplemented!() // TODO: storage/buf/bufmgr.c
}
unsafe fn ReleaseBuffer(_buffer: Buffer) {
    unimplemented!() // TODO: storage/buf/bufmgr.c
}
unsafe fn UnlockReleaseBuffer(_buffer: Buffer) {
    unimplemented!() // TODO: storage/buf/bufmgr.c
}
unsafe fn ReleaseAndReadBuffer(_buffer: Buffer, _reln: Relation, _blockNum: BlockNumber) -> Buffer {
    unimplemented!() // TODO: storage/buf/bufmgr.c
}
unsafe fn BufferGetBlockNumber(_buffer: Buffer) -> BlockNumber {
    unimplemented!() // TODO: storage/buf/bufmgr.c
}
unsafe fn BufferIsValid(_buffer: Buffer) -> bool { crate::access::nbtree::nbtpage::BufferIsValid(_buffer) }
unsafe fn MarkBufferDirty(_buffer: Buffer) {
    unimplemented!() // TODO: storage/buf/bufmgr.c
}
unsafe fn GinPageIsLeaf(_page: Page) -> bool { crate::access::gin::ginblock::GinPageIsLeaf(_page) }
unsafe fn GinPageIsData(_page: Page) -> bool { crate::access::gin::ginblock::GinPageIsData(_page) }
unsafe fn GinPageIsIncompleteSplit(_page: Page) -> bool { crate::access::gin::ginblock::GinPageIsIncompleteSplit(_page) }
unsafe fn GinPageRightMost(_page: Page) -> bool { crate::access::gin::ginblock::GinPageRightMost(_page) }
unsafe fn GinPageGetOpaque(_page: Page) -> *mut GinPageOpaqueData {
    unimplemented!() // TODO: access/gin_private.h
}
unsafe fn GinNewBuffer(_index: Relation) -> Buffer { unimplemented!() }
unsafe fn GinInitPage(_page: Page, _flags: uint16, _size: Size) { unimplemented!() }
unsafe fn PageGetTempPage(_page: Page) -> Page {
    unimplemented!() // TODO: storage/page/bufpage.c
}
unsafe fn PageSetLSN(_page: Page, _lsn: XLogRecPtr) { crate::storage::bufpage::PageSetLSN(_page, _lsn) }
unsafe fn RelationNeedsWAL(_index: Relation) -> bool { unimplemented!() }
unsafe fn RelationGetRelationName(_index: Relation) -> *const c_char {
    unimplemented!() // TODO: utils/rel.h
}
unsafe fn CheckForSerializableConflictIn(
    _relation: Relation,
    _tid: *mut c_void,
    _blkno: BlockNumber,
) {
    unimplemented!() // TODO: storage/lmgr/predicate.c
}
unsafe fn PredicateLockPageSplit(_relation: Relation, _oldblkno: BlockNumber, _newblkno: BlockNumber) { crate::storage::lmgr::predicate::PredicateLockPageSplit(_relation, _oldblkno, _newblkno) }
unsafe fn AllocSetContextCreateInternal(
    _parent: MemoryContext,
    _name: *const c_char,
    _minContextSize: Size,
    _initBlockSize: Size,
    _maxBlockSize: Size,
) -> MemoryContext { crate::utils::mmgr::aset::AllocSetContextCreateInternal(_parent, _name, _minContextSize, _initBlockSize, _maxBlockSize) }
unsafe fn XLogBeginInsert() {
    unimplemented!() // TODO: access/transam/xloginsert.c
}
unsafe fn XLogRegisterData(_data: *const c_void, _len: c_int) {
    unimplemented!() // TODO: access/transam/xloginsert.c
}
unsafe fn XLogRegisterBuffer(_block_id: u8, _buffer: Buffer, _flags: u8) {
    unimplemented!() // TODO: access/transam/xloginsert.c
}
unsafe fn XLogInsert(_rmid: u8, _info: u8) -> XLogRecPtr {
    unimplemented!() // TODO: access/transam/xloginsert.c
}
unsafe fn BlockIdSet(_blockId: *mut BlockIdData, _blockNumber: BlockNumber) { unimplemented!() }

#[repr(C)]
struct GinPageOpaqueData {
    rightlink: BlockNumber,
    maxoff: OffsetNumber,
    flags: uint16,
}

// ALLOCSET_DEFAULT_SIZES expansion
const ALLOCSET_DEFAULT_MINSIZE: Size = 0;
const ALLOCSET_DEFAULT_INITSIZE: Size = 8 * 1024;
const ALLOCSET_DEFAULT_MAXSIZE: Size = 8 * 1024 * 1024;

const BLCKSZ: Size = 8192;

// ---------------------------------------------------------------------------
// Functions
// ---------------------------------------------------------------------------

/*
 * Lock buffer by needed method for search.
 */
pub unsafe fn ginTraverseLock(buffer: Buffer, searchMode: bool) -> c_int {
    let page: Page;
    let mut access: c_int = GIN_SHARE;

    LockBuffer(buffer, GIN_SHARE);
    page = BufferGetPage(buffer);
    if GinPageIsLeaf(page) {
        if searchMode == false {
            /* we should relock our page */
            LockBuffer(buffer, GIN_UNLOCK);
            LockBuffer(buffer, GIN_EXCLUSIVE);

            /* But root can become non-leaf during relock */
            if !GinPageIsLeaf(page) {
                /* restore old lock type (very rare) */
                LockBuffer(buffer, GIN_UNLOCK);
                LockBuffer(buffer, GIN_SHARE);
            } else {
                access = GIN_EXCLUSIVE;
            }
        }
    }

    access
}

/*
 * Descend the tree to the leaf page that contains or would contain the key
 * we're searching for. The key should already be filled in 'btree', in
 * tree-type specific manner. If btree->fullScan is true, descends to the
 * leftmost leaf page.
 *
 * If 'searchmode' is false, on return stack->buffer is exclusively locked,
 * and the stack represents the full path to the root. Otherwise stack->buffer
 * is share-locked, and stack->parent is NULL.
 *
 * If 'rootConflictCheck' is true, tree root is checked for serialization
 * conflict.
 */
pub unsafe fn ginFindLeafPage(
    btree: GinBtree,
    searchMode: bool,
    rootConflictCheck: bool,
) -> *mut GinBtreeStack {
    let mut stack: *mut GinBtreeStack;

    stack = palloc(std::mem::size_of::<GinBtreeStack>()) as *mut GinBtreeStack;
    (*stack).blkno = (*btree).rootBlkno;
    (*stack).buffer = ReadBuffer((*btree).index, (*btree).rootBlkno);
    (*stack).parent = std::ptr::null_mut();
    (*stack).predictNumber = 1;

    if rootConflictCheck {
        CheckForSerializableConflictIn((*btree).index, std::ptr::null_mut(), (*btree).rootBlkno);
    }

    loop {
        let mut page: Page;
        let child: BlockNumber;
        let access: c_int;

        (*stack).off = InvalidOffsetNumber;

        page = BufferGetPage((*stack).buffer);

        access = ginTraverseLock((*stack).buffer, searchMode);

        /*
         * If we're going to modify the tree, finish any incomplete splits we
         * encounter on the way.
         */
        if !searchMode && GinPageIsIncompleteSplit(page) {
            ginFinishOldSplit(btree, stack, std::ptr::null_mut(), access);
        }

        /*
         * ok, page is correctly locked, we should check to move right ..,
         * root never has a right link, so small optimization
         */
        while (*btree).fullScan == false
            && (*stack).blkno != (*btree).rootBlkno
            && ((*btree).isMoveRight)(btree, page)
        {
            let rightlink: BlockNumber = (*GinPageGetOpaque(page)).rightlink;

            if rightlink == InvalidBlockNumber {
                /* rightmost page */
                break;
            }

            (*stack).buffer = ginStepRight((*stack).buffer, (*btree).index, access);
            (*stack).blkno = rightlink;
            page = BufferGetPage((*stack).buffer);

            if !searchMode && GinPageIsIncompleteSplit(page) {
                ginFinishOldSplit(btree, stack, std::ptr::null_mut(), access);
            }
        }

        if GinPageIsLeaf(page) {
            /* we found, return locked page */
            return stack;
        }

        /* now we have correct buffer, try to find child */
        child = ((*btree).findChildPage)(btree, stack);

        LockBuffer((*stack).buffer, GIN_UNLOCK);
        Assert!(child != InvalidBlockNumber);
        Assert!((*stack).blkno != child);

        if searchMode {
            /* in search mode we may forget path to leaf */
            (*stack).blkno = child;
            (*stack).buffer =
                ReleaseAndReadBuffer((*stack).buffer, (*btree).index, (*stack).blkno);
        } else {
            let ptr: *mut GinBtreeStack =
                palloc(std::mem::size_of::<GinBtreeStack>()) as *mut GinBtreeStack;

            (*ptr).parent = stack;
            stack = ptr;
            (*stack).blkno = child;
            (*stack).buffer = ReadBuffer((*btree).index, (*stack).blkno);
            (*stack).predictNumber = 1;
        }
    }
}

/*
 * Step right from current page.
 *
 * The next page is locked first, before releasing the current page. This is
 * crucial to prevent concurrent VACUUM from deleting a page that we are about
 * to step to. (The lock-coupling isn't strictly necessary when we are
 * traversing the tree to find an insert location, because page deletion grabs
 * a cleanup lock on the root to prevent any concurrent inserts. See Page
 * deletion section in the README. But there's no harm in doing it always.)
 */
pub unsafe fn ginStepRight(buffer: Buffer, index: Relation, lockmode: c_int) -> Buffer {
    let nextbuffer: Buffer;
    let mut page: Page = BufferGetPage(buffer);
    let isLeaf: bool = GinPageIsLeaf(page);
    let isData: bool = GinPageIsData(page);
    let blkno: BlockNumber = (*GinPageGetOpaque(page)).rightlink;

    nextbuffer = ReadBuffer(index, blkno);
    LockBuffer(nextbuffer, lockmode);
    UnlockReleaseBuffer(buffer);

    /* Sanity check that the page we stepped to is of similar kind. */
    page = BufferGetPage(nextbuffer);
    if isLeaf != GinPageIsLeaf(page) || isData != GinPageIsData(page) {
        elog!(ERROR, "right sibling of GIN page is of different type");
    }

    nextbuffer
}

pub unsafe fn freeGinBtreeStack(mut stack: *mut GinBtreeStack) {
    while !stack.is_null() {
        let tmp: *mut GinBtreeStack = (*stack).parent;

        if (*stack).buffer != InvalidBuffer {
            ReleaseBuffer((*stack).buffer);
        }

        pfree(stack as *mut c_void);
        stack = tmp;
    }
}

/*
 * Try to find parent for current stack position. Returns correct parent and
 * child's offset in stack->parent. The root page is never released, to
 * prevent conflict with vacuum process.
 */
unsafe fn ginFindParents(btree: GinBtree, stack: *mut GinBtreeStack) {
    let mut page: Page;
    let mut buffer: Buffer;
    let mut blkno: BlockNumber;
    let mut leftmostBlkno: BlockNumber;
    let mut offset: OffsetNumber;
    let mut root: *mut GinBtreeStack;
    let ptr: *mut GinBtreeStack;

    /*
     * Unwind the stack all the way up to the root, leaving only the root
     * item.
     *
     * Be careful not to release the pin on the root page! The pin on root
     * page is required to lock out concurrent vacuums on the tree.
     */
    root = (*stack).parent;
    while !(*root).parent.is_null() {
        ReleaseBuffer((*root).buffer);
        root = (*root).parent;
    }

    Assert!((*root).blkno == (*btree).rootBlkno);
    Assert!(BufferGetBlockNumber((*root).buffer) == (*btree).rootBlkno);
    (*root).off = InvalidOffsetNumber;

    blkno = (*root).blkno;
    buffer = (*root).buffer;

    ptr = palloc(std::mem::size_of::<GinBtreeStack>()) as *mut GinBtreeStack;

    loop {
        LockBuffer(buffer, GIN_EXCLUSIVE);
        page = BufferGetPage(buffer);
        if GinPageIsLeaf(page) {
            elog!(ERROR, "Lost path");
        }

        if GinPageIsIncompleteSplit(page) {
            Assert!(blkno != (*btree).rootBlkno);
            (*ptr).blkno = blkno;
            (*ptr).buffer = buffer;

            /*
             * parent may be wrong, but if so, the ginFinishSplit call will
             * recurse to call ginFindParents again to fix it.
             */
            (*ptr).parent = root;
            (*ptr).off = InvalidOffsetNumber;

            ginFinishOldSplit(btree, ptr, std::ptr::null_mut(), GIN_EXCLUSIVE);
        }

        leftmostBlkno = ((*btree).getLeftMostChild)(btree, page);

        loop {
            offset = ((*btree).findChildPtr)(btree, page, (*stack).blkno, InvalidOffsetNumber);
            if offset != InvalidOffsetNumber {
                break;
            }
            blkno = (*GinPageGetOpaque(page)).rightlink;
            if blkno == InvalidBlockNumber {
                /* Link not present in this level */
                LockBuffer(buffer, GIN_UNLOCK);
                /* Do not release pin on the root buffer */
                if buffer != (*root).buffer {
                    ReleaseBuffer(buffer);
                }
                break;
            }
            buffer = ginStepRight(buffer, (*btree).index, GIN_EXCLUSIVE);
            page = BufferGetPage(buffer);

            /* finish any incomplete splits, as above */
            if GinPageIsIncompleteSplit(page) {
                Assert!(blkno != (*btree).rootBlkno);
                (*ptr).blkno = blkno;
                (*ptr).buffer = buffer;
                (*ptr).parent = root;
                (*ptr).off = InvalidOffsetNumber;

                ginFinishOldSplit(btree, ptr, std::ptr::null_mut(), GIN_EXCLUSIVE);
            }
        }

        if blkno != InvalidBlockNumber {
            (*ptr).blkno = blkno;
            (*ptr).buffer = buffer;
            (*ptr).parent = root; /* it may be wrong, but in next call we will
                                   * correct */
            (*ptr).off = offset;
            (*stack).parent = ptr;
            return;
        }

        /* Descend down to next level */
        blkno = leftmostBlkno;
        buffer = ReadBuffer((*btree).index, blkno);
    }
}

/*
 * Insert a new item to a page.
 *
 * Returns true if the insertion was finished. On false, the page was split and
 * the parent needs to be updated. (A root split returns true as it doesn't
 * need any further action by the caller to complete.)
 *
 * When inserting a downlink to an internal page, 'childbuf' contains the
 * child page that was split. Its GIN_INCOMPLETE_SPLIT flag will be cleared
 * atomically with the insert. Also, the existing item at offset stack->off
 * in the target page is updated to point to updateblkno.
 *
 * stack->buffer is locked on entry, and is kept locked.
 * Likewise for childbuf, if given.
 */
unsafe fn ginPlaceToPage(
    btree: GinBtree,
    stack: *mut GinBtreeStack,
    insertdata: *mut c_void,
    updateblkno: BlockNumber,
    childbuf: Buffer,
    buildStats: *mut GinStatsData,
) -> bool {
    let page: Page = BufferGetPage((*stack).buffer);
    let result: bool;
    let rc: GinPlaceToPageRC;
    let mut xlflags: uint16 = 0;
    let mut childpage: Page = std::ptr::null_mut();
    let mut newlpage: Page = std::ptr::null_mut();
    let mut newrpage: Page = std::ptr::null_mut();
    let mut ptp_workspace: *mut c_void = std::ptr::null_mut();
    let tmpCxt: MemoryContext;
    let oldCxt: MemoryContext;

    /*
     * We do all the work of this function and its subfunctions in a temporary
     * memory context.  This avoids leakages and simplifies APIs, since some
     * subfunctions allocate storage that has to survive until we've finished
     * the WAL insertion.
     */
    tmpCxt = AllocSetContextCreateInternal(
        CurrentMemoryContext as MemoryContext,
        c"ginPlaceToPage temporary context".as_ptr(),
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE,
    );
    oldCxt = MemoryContextSwitchTo(tmpCxt as _) as MemoryContext;

    if GinPageIsData(page) {
        xlflags |= GIN_INSERT_ISDATA;
    }
    if GinPageIsLeaf(page) {
        xlflags |= GIN_INSERT_ISLEAF;
        Assert!(!BufferIsValid(childbuf));
        Assert!(updateblkno == InvalidBlockNumber);
    } else {
        Assert!(BufferIsValid(childbuf));
        Assert!(updateblkno != InvalidBlockNumber);
        childpage = BufferGetPage(childbuf);
    }

    /*
     * See if the incoming tuple will fit on the page.  beginPlaceToPage will
     * decide if the page needs to be split, and will compute the split
     * contents if so.  See comments for beginPlaceToPage and execPlaceToPage
     * functions for more details of the API here.
     */
    rc = ((*btree).beginPlaceToPage)(
        btree,
        (*stack).buffer,
        stack,
        insertdata,
        updateblkno,
        &mut ptp_workspace,
        &mut newlpage,
        &mut newrpage,
    );

    if rc == GPTP_NO_WORK {
        /* Nothing to do */
        result = true;
    } else if rc == GPTP_INSERT {
        /* It will fit, perform the insertion */
        START_CRIT_SECTION();

        if RelationNeedsWAL((*btree).index) && !(*btree).isBuild {
            XLogBeginInsert();
        }

        /*
         * Perform the page update, dirty and register stack->buffer, and
         * register any extra WAL data.
         */
        ((*btree).execPlaceToPage)(
            btree,
            (*stack).buffer,
            stack,
            insertdata,
            updateblkno,
            ptp_workspace,
        );

        /* An insert to an internal page finishes the split of the child. */
        if BufferIsValid(childbuf) {
            (*GinPageGetOpaque(childpage)).flags &= !GIN_INCOMPLETE_SPLIT;
            MarkBufferDirty(childbuf);
            if RelationNeedsWAL((*btree).index) && !(*btree).isBuild {
                XLogRegisterBuffer(1, childbuf, REGBUF_STANDARD as u8);
            }
        }

        if RelationNeedsWAL((*btree).index) && !(*btree).isBuild {
            let recptr: XLogRecPtr;
            let mut xlrec: ginxlogInsert = std::mem::zeroed();
            let mut childblknos: [BlockIdData; 2] = std::mem::zeroed();

            xlrec.flags = xlflags;

            XLogRegisterData(
                &xlrec as *const _ as *const c_void,
                std::mem::size_of::<ginxlogInsert>() as c_int,
            );

            /*
             * Log information about child if this was an insertion of a
             * downlink.
             */
            if BufferIsValid(childbuf) {
                BlockIdSet(&mut childblknos[0], BufferGetBlockNumber(childbuf));
                BlockIdSet(&mut childblknos[1], (*GinPageGetOpaque(childpage)).rightlink);
                XLogRegisterData(
                    childblknos.as_ptr() as *const c_void,
                    (std::mem::size_of::<BlockIdData>() * 2) as c_int,
                );
            }

            recptr = XLogInsert(RM_GIN_ID, XLOG_GIN_INSERT);
            PageSetLSN(page, recptr);
            if BufferIsValid(childbuf) {
                PageSetLSN(childpage, recptr);
            }
        }

        END_CRIT_SECTION();

        /* Insertion is complete. */
        result = true;
    } else if rc == GPTP_SPLIT {
        /*
         * Didn't fit, need to split.  The split has been computed in newlpage
         * and newrpage, which are pointers to palloc'd pages, not associated
         * with buffers.  stack->buffer is not touched yet.
         */
        let rbuffer: Buffer;
        let savedRightLink: BlockNumber;
        let mut data: ginxlogSplit = std::mem::zeroed();
        let mut lbuffer: Buffer = InvalidBuffer;
        let mut newrootpg: Page = std::ptr::null_mut();

        /* Get a new index page to become the right page */
        rbuffer = GinNewBuffer((*btree).index);

        /* During index build, count the new page */
        if !buildStats.is_null() {
            if (*btree).isData {
                (*(buildStats as *mut GinStatsDataReal)).nDataPages += 1;
            } else {
                (*(buildStats as *mut GinStatsDataReal)).nEntryPages += 1;
            }
        }

        savedRightLink = (*GinPageGetOpaque(page)).rightlink;

        /* Begin setting up WAL record */
        // data.locator = btree->index->rd_locator;
        copy_rd_locator((*btree).index, &mut data.locator);
        data.flags = xlflags;
        if BufferIsValid(childbuf) {
            data.leftChildBlkno = BufferGetBlockNumber(childbuf);
            data.rightChildBlkno = (*GinPageGetOpaque(childpage)).rightlink;
        } else {
            data.leftChildBlkno = InvalidBlockNumber;
            data.rightChildBlkno = InvalidBlockNumber;
        }

        if (*stack).parent.is_null() {
            /*
             * splitting the root, so we need to allocate new left page and
             * place pointers to left and right page on root page.
             */
            lbuffer = GinNewBuffer((*btree).index);

            /* During index build, count the new left page */
            if !buildStats.is_null() {
                if (*btree).isData {
                    (*(buildStats as *mut GinStatsDataReal)).nDataPages += 1;
                } else {
                    (*(buildStats as *mut GinStatsDataReal)).nEntryPages += 1;
                }
            }

            data.rrlink = InvalidBlockNumber;
            data.flags |= GIN_SPLIT_ROOT;

            (*GinPageGetOpaque(newrpage)).rightlink = InvalidBlockNumber;
            (*GinPageGetOpaque(newlpage)).rightlink = BufferGetBlockNumber(rbuffer);

            /*
             * Construct a new root page containing downlinks to the new left
             * and right pages.  (Do this in a temporary copy rather than
             * overwriting the original page directly, since we're not in the
             * critical section yet.)
             */
            newrootpg = PageGetTempPage(newrpage);
            GinInitPage(
                newrootpg,
                (*GinPageGetOpaque(newlpage)).flags & !(GIN_LEAF | GIN_COMPRESSED),
                BLCKSZ,
            );

            ((*btree).fillRoot)(
                btree,
                newrootpg,
                BufferGetBlockNumber(lbuffer),
                newlpage,
                BufferGetBlockNumber(rbuffer),
                newrpage,
            );

            if GinPageIsLeaf(BufferGetPage((*stack).buffer)) {
                PredicateLockPageSplit(
                    (*btree).index,
                    BufferGetBlockNumber((*stack).buffer),
                    BufferGetBlockNumber(lbuffer),
                );

                PredicateLockPageSplit(
                    (*btree).index,
                    BufferGetBlockNumber((*stack).buffer),
                    BufferGetBlockNumber(rbuffer),
                );
            }
        } else {
            /* splitting a non-root page */
            data.rrlink = savedRightLink;

            (*GinPageGetOpaque(newrpage)).rightlink = savedRightLink;
            (*GinPageGetOpaque(newlpage)).flags |= GIN_INCOMPLETE_SPLIT;
            (*GinPageGetOpaque(newlpage)).rightlink = BufferGetBlockNumber(rbuffer);

            if GinPageIsLeaf(BufferGetPage((*stack).buffer)) {
                PredicateLockPageSplit(
                    (*btree).index,
                    BufferGetBlockNumber((*stack).buffer),
                    BufferGetBlockNumber(rbuffer),
                );
            }
        }

        /*
         * OK, we have the new contents of the left page in a temporary copy
         * now (newlpage), and likewise for the new contents of the
         * newly-allocated right block. The original page is still unchanged.
         *
         * If this is a root split, we also have a temporary page containing
         * the new contents of the root.
         */

        START_CRIT_SECTION();

        MarkBufferDirty(rbuffer);
        MarkBufferDirty((*stack).buffer);

        /*
         * Restore the temporary copies over the real buffers.
         */
        if (*stack).parent.is_null() {
            /* Splitting the root, three pages to update */
            MarkBufferDirty(lbuffer);
            memcpy(page as *mut c_void, newrootpg as *const c_void, BLCKSZ);
            memcpy(
                BufferGetPage(lbuffer) as *mut c_void,
                newlpage as *const c_void,
                BLCKSZ,
            );
            memcpy(
                BufferGetPage(rbuffer) as *mut c_void,
                newrpage as *const c_void,
                BLCKSZ,
            );
        } else {
            /* Normal split, only two pages to update */
            memcpy(page as *mut c_void, newlpage as *const c_void, BLCKSZ);
            memcpy(
                BufferGetPage(rbuffer) as *mut c_void,
                newrpage as *const c_void,
                BLCKSZ,
            );
        }

        /* We also clear childbuf's INCOMPLETE_SPLIT flag, if passed */
        if BufferIsValid(childbuf) {
            (*GinPageGetOpaque(childpage)).flags &= !GIN_INCOMPLETE_SPLIT;
            MarkBufferDirty(childbuf);
        }

        /* write WAL record */
        if RelationNeedsWAL((*btree).index) && !(*btree).isBuild {
            let recptr: XLogRecPtr;

            XLogBeginInsert();

            /*
             * We just take full page images of all the split pages. Splits
             * are uncommon enough that it's not worth complicating the code
             * to be more efficient.
             */
            if (*stack).parent.is_null() {
                XLogRegisterBuffer(0, lbuffer, (REGBUF_FORCE_IMAGE | REGBUF_STANDARD) as u8);
                XLogRegisterBuffer(1, rbuffer, (REGBUF_FORCE_IMAGE | REGBUF_STANDARD) as u8);
                XLogRegisterBuffer(
                    2,
                    (*stack).buffer,
                    (REGBUF_FORCE_IMAGE | REGBUF_STANDARD) as u8,
                );
            } else {
                XLogRegisterBuffer(
                    0,
                    (*stack).buffer,
                    (REGBUF_FORCE_IMAGE | REGBUF_STANDARD) as u8,
                );
                XLogRegisterBuffer(1, rbuffer, (REGBUF_FORCE_IMAGE | REGBUF_STANDARD) as u8);
            }
            if BufferIsValid(childbuf) {
                XLogRegisterBuffer(3, childbuf, REGBUF_STANDARD as u8);
            }

            XLogRegisterData(
                &data as *const _ as *const c_void,
                std::mem::size_of::<ginxlogSplit>() as c_int,
            );

            recptr = XLogInsert(RM_GIN_ID, XLOG_GIN_SPLIT);

            PageSetLSN(page, recptr);
            PageSetLSN(BufferGetPage(rbuffer), recptr);
            if (*stack).parent.is_null() {
                PageSetLSN(BufferGetPage(lbuffer), recptr);
            }
            if BufferIsValid(childbuf) {
                PageSetLSN(childpage, recptr);
            }
        }
        END_CRIT_SECTION();

        /*
         * We can release the locks/pins on the new pages now, but keep
         * stack->buffer locked.  childbuf doesn't get unlocked either.
         */
        UnlockReleaseBuffer(rbuffer);
        if (*stack).parent.is_null() {
            UnlockReleaseBuffer(lbuffer);
        }

        /*
         * If we split the root, we're done. Otherwise the split is not
         * complete until the downlink for the new page has been inserted to
         * the parent.
         */
        result = (*stack).parent.is_null();
    } else {
        elog!(
            ERROR,
            "invalid return code from GIN beginPlaceToPage method: {}",
            rc as c_int
        );
        result = false; /* keep compiler quiet */
    }

    /* Clean up temp context */
    MemoryContextSwitchTo(oldCxt as _);
    MemoryContextDelete(tmpCxt);

    result
}

/*
 * Finish a split by inserting the downlink for the new page to parent.
 *
 * On entry, stack->buffer is exclusively locked.
 *
 * If freestack is true, all the buffers are released and unlocked as we
 * crawl up the tree, and 'stack' is freed. Otherwise stack->buffer is kept
 * locked, and stack is unmodified, except for possibly moving right to find
 * the correct parent of page.
 */
unsafe fn ginFinishSplit(
    btree: GinBtree,
    mut stack: *mut GinBtreeStack,
    freestack: bool,
    buildStats: *mut GinStatsData,
) {
    let mut page: Page;
    let mut done: bool;
    let mut first: bool = true;

    /* this loop crawls up the stack until the insertion is complete */
    loop {
        let mut parent: *mut GinBtreeStack = (*stack).parent;
        let insertdata: *mut c_void;
        let updateblkno: BlockNumber;

        // USE_INJECTION_POINTS
        if GinPageIsLeaf(BufferGetPage((*stack).buffer)) {
            INJECTION_POINT(c"gin-leave-leaf-split-incomplete".as_ptr(), std::ptr::null_mut());
        } else {
            INJECTION_POINT(
                c"gin-leave-internal-split-incomplete".as_ptr(),
                std::ptr::null_mut(),
            );
        }

        /* search parent to lock */
        LockBuffer((*parent).buffer, GIN_EXCLUSIVE);

        /*
         * If the parent page was incompletely split, finish that split first,
         * then continue with the current one.
         *
         * Note: we have to finish *all* incomplete splits we encounter, even
         * if we have to move right. Otherwise we might choose as the target a
         * page that has no downlink in the parent, and splitting it further
         * would fail.
         */
        if GinPageIsIncompleteSplit(BufferGetPage((*parent).buffer)) {
            ginFinishOldSplit(btree, parent, buildStats, GIN_EXCLUSIVE);
        }

        /* move right if it's needed */
        page = BufferGetPage((*parent).buffer);
        loop {
            (*parent).off =
                ((*btree).findChildPtr)(btree, page, (*stack).blkno, (*parent).off);
            if (*parent).off != InvalidOffsetNumber {
                break;
            }
            if GinPageRightMost(page) {
                /*
                 * rightmost page, but we don't find parent, we should use
                 * plain search...
                 */
                LockBuffer((*parent).buffer, GIN_UNLOCK);
                ginFindParents(btree, stack);
                parent = (*stack).parent;
                Assert!(!parent.is_null());
                break;
            }

            (*parent).buffer = ginStepRight((*parent).buffer, (*btree).index, GIN_EXCLUSIVE);
            (*parent).blkno = BufferGetBlockNumber((*parent).buffer);
            page = BufferGetPage((*parent).buffer);

            if GinPageIsIncompleteSplit(BufferGetPage((*parent).buffer)) {
                ginFinishOldSplit(btree, parent, buildStats, GIN_EXCLUSIVE);
            }
        }

        /* insert the downlink */
        insertdata = ((*btree).prepareDownlink)(btree, (*stack).buffer);
        updateblkno = (*GinPageGetOpaque(BufferGetPage((*stack).buffer))).rightlink;
        done = ginPlaceToPage(
            btree,
            parent,
            insertdata,
            updateblkno,
            (*stack).buffer,
            buildStats,
        );
        pfree(insertdata);

        /*
         * If the caller requested to free the stack, unlock and release the
         * child buffer now. Otherwise keep it pinned and locked, but if we
         * have to recurse up the tree, we can unlock the upper pages, only
         * keeping the page at the bottom of the stack locked.
         */
        if !first || freestack {
            LockBuffer((*stack).buffer, GIN_UNLOCK);
        }
        if freestack {
            ReleaseBuffer((*stack).buffer);
            pfree(stack as *mut c_void);
        }
        stack = parent;

        first = false;

        if done {
            break;
        }
    }

    /* unlock the parent */
    LockBuffer((*stack).buffer, GIN_UNLOCK);

    if freestack {
        freeGinBtreeStack(stack);
    }
}

/*
 * An entry point to ginFinishSplit() that is used when we stumble upon an
 * existing incompletely split page in the tree, as opposed to completing a
 * split that we just made ourselves. The difference is that stack->buffer may
 * be merely share-locked on entry, and will be upgraded to exclusive mode.
 *
 * Note: Upgrading the lock momentarily releases it. Doing that in a scan
 * would not be OK, because a concurrent VACUUM might delete the page while
 * we're not holding the lock. It's OK in an insert, though, because VACUUM
 * has a different mechanism that prevents it from running concurrently with
 * inserts. (Namely, it holds a cleanup lock on the root.)
 */
unsafe fn ginFinishOldSplit(
    btree: GinBtree,
    stack: *mut GinBtreeStack,
    buildStats: *mut GinStatsData,
    access: c_int,
) {
    INJECTION_POINT(c"gin-finish-incomplete-split".as_ptr(), std::ptr::null_mut());
    elog!(
        DEBUG1,
        "finishing incomplete split of block {} in gin index \"{}\"",
        (*stack).blkno,
        cstr_to_display(RelationGetRelationName((*btree).index))
    );

    if access == GIN_SHARE {
        LockBuffer((*stack).buffer, GIN_UNLOCK);
        LockBuffer((*stack).buffer, GIN_EXCLUSIVE);

        if !GinPageIsIncompleteSplit(BufferGetPage((*stack).buffer)) {
            /*
             * Someone else already completed the split while we were not
             * holding the lock.
             */
            return;
        }
    }

    ginFinishSplit(btree, stack, false, buildStats);
}

/*
 * Insert a value to tree described by stack.
 *
 * The value to be inserted is given in 'insertdata'. Its format depends
 * on whether this is an entry or data tree, ginInsertValue just passes it
 * through to the tree-specific callback function.
 *
 * During an index build, buildStats is non-null and the counters it contains
 * are incremented as needed.
 *
 * NB: the passed-in stack is freed, as though by freeGinBtreeStack.
 */
pub unsafe fn ginInsertValue(
    btree: GinBtree,
    stack: *mut GinBtreeStack,
    insertdata: *mut c_void,
    buildStats: *mut GinStatsData,
) {
    let done: bool;

    /* If the leaf page was incompletely split, finish the split first */
    if GinPageIsIncompleteSplit(BufferGetPage((*stack).buffer)) {
        ginFinishOldSplit(btree, stack, buildStats, GIN_EXCLUSIVE);
    }

    done = ginPlaceToPage(
        btree,
        stack,
        insertdata,
        InvalidBlockNumber,
        InvalidBuffer,
        buildStats,
    );
    if done {
        LockBuffer((*stack).buffer, GIN_UNLOCK);
        freeGinBtreeStack(stack);
    } else {
        ginFinishSplit(btree, stack, true, buildStats);
    }
}

// ---------------------------------------------------------------------------
// Additional local stubs / helpers used above
// ---------------------------------------------------------------------------

#[repr(C)]
struct GinStatsDataReal {
    nDataPages: BlockNumber,
    nEntryPages: BlockNumber,
    nEntries: i64,
    nTotalPages: BlockNumber,
}

unsafe fn START_CRIT_SECTION() {
    unimplemented!() // TODO: storage/ipc/standby.c / miscadmin.h
}
unsafe fn END_CRIT_SECTION() {
    unimplemented!() // TODO: miscadmin.h
}
unsafe fn INJECTION_POINT(_name: *const c_char, _arg: *mut c_void) {
    unimplemented!() // TODO: utils/injection_point.c
}
unsafe fn copy_rd_locator(_index: Relation, _dst: *mut c_void) {
    unimplemented!() // TODO: data.locator = btree->index->rd_locator (utils/rel.h)
}
unsafe fn cstr_to_display(_s: *const c_char) -> &'static str {
    unimplemented!() // TODO: render relation name for elog
}
extern "C" {
    fn memcpy(dest: *mut c_void, src: *const c_void, n: Size) -> *mut c_void;
}
