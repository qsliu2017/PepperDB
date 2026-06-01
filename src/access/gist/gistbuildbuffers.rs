//! src/backend/access/gist/gistbuildbuffers.c
//!
//! node buffer management functions for GiST buffering build algorithm.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::prelude::*;

use std::ffi::{c_char, c_int, c_void};

use crate::access::common::tupdesc::TupleDesc;
use crate::nodes::pg_list::{lcons, lfirst, list_length, List, ListCell, NIL};
use crate::storage::block::{BlockNumber, InvalidBlockNumber};

// ----------------------------------------------------------------
// Constants and dependent types (from access/gist_private.h, gist.h,
// access/itup.h, storage/buffile.h, storage/bufmgr.h, utils/rel.h).
// ----------------------------------------------------------------

const BLCKSZ: usize = 8192;
const INDEX_MAX_KEYS: usize = 32;

pub type OffsetNumber = u16;
pub type Buffer = c_int;

// IndexTuple from access/itup.h
#[repr(C)]
pub struct IndexTupleData {
    pub t_tid: ItemPointerData,
    pub t_info: u16,
}
pub type IndexTuple = *mut IndexTupleData;

#[repr(C)]
pub struct ItemPointerData {
    pub ip_blkid: [u16; 2],
    pub ip_posid: u16,
}

// GISTENTRY from gist.h
#[repr(C)]
pub struct GISTENTRY {
    pub key: Datum,
    pub rel: Relation,
    pub page: Page,
    pub offset: OffsetNumber,
    pub leafkey: bool,
}

pub type Page = *mut c_char;
pub type Relation = *mut RelationData;
pub struct RelationData;

pub struct GISTSTATE;

// GISTPageSplitInfo from gist_private.h
#[repr(C)]
pub struct GISTPageSplitInfo {
    pub buf: Buffer,
    pub downlink: IndexTuple,
}

// HASHCTL / hash table support from utils/hsearch.h
#[repr(C)]
pub struct HASHCTL {
    pub keysize: Size,
    pub entrysize: Size,
    pub hcxt: MemoryContext,
    // (other fields omitted; only those used here)
}

pub const HASH_ELEM: c_int = 0x0008;
pub const HASH_BLOBS: c_int = 0x0010;
pub const HASH_CONTEXT: c_int = 0x0040;

#[repr(C)]
#[derive(PartialEq, Eq)]
pub enum HASHACTION {
    HASH_FIND,
    HASH_ENTER,
    HASH_REMOVE,
    HASH_ENTER_NULL,
}
pub use HASHACTION::*;

pub type HTAB = c_void;

// BufFile from storage/buffile.h
pub type BufFile = c_void;

/*
 * GISTNodeBufferPage - a buffer page is a fixed-size chunk of BLCKSZ bytes.
 * The first part of the page is GISTNodeBufferPage struct, and the rest of
 * the page holds the index tuples.
 */
#[repr(C)]
pub struct GISTNodeBufferPage {
    pub prev: BlockNumber, /* prev page in chain, or InvalidBlockNumber */
    /* index tuples start here, on a MAXALIGN'd boundary */
    pub tupledata: [c_char; 0], /* FLEXIBLE_ARRAY_MEMBER */
}

/*
 * Each buffer in memory keeps the amount of free space in the first int of
 * tupledata. PAGE_FREE_SPACE / PAGE_IS_EMPTY / PAGE_NO_SPACE macros below
 * operate on this.
 */

#[inline]
unsafe fn PAGE_FREE_SPACE(ptr: *mut GISTNodeBufferPage) -> *mut c_int {
    (*ptr).tupledata.as_mut_ptr() as *mut c_int
}

#[inline]
unsafe fn PAGE_IS_EMPTY(nbp: *mut GISTNodeBufferPage) -> bool {
    *PAGE_FREE_SPACE(nbp) == (BLCKSZ - BUFFER_PAGE_DATA_OFFSET_VAL()) as c_int
}

#[inline]
unsafe fn PAGE_NO_SPACE(nbp: *mut GISTNodeBufferPage, itup: IndexTuple) -> bool {
    (*PAGE_FREE_SPACE(nbp) as usize) < MAXALIGN(IndexTupleSize(itup))
}

// BUFFER_PAGE_DATA_OFFSET = MAXALIGN(offsetof(GISTNodeBufferPage, tupledata))
#[inline]
const fn BUFFER_PAGE_DATA_OFFSET_VAL() -> usize {
    // offsetof(GISTNodeBufferPage, tupledata) == sizeof(BlockNumber) == 4,
    // MAXALIGN'd.
    MAXALIGN_CONST(core::mem::offset_of!(GISTNodeBufferPage, tupledata))
}

const fn MAXALIGN_CONST(len: usize) -> usize {
    const MAXIMUM_ALIGNOF: usize = 8;
    (len + (MAXIMUM_ALIGNOF - 1)) & !(MAXIMUM_ALIGNOF - 1)
}

/*
 * GISTNodeBuffer - a node buffer in the buffering GiST build.
 */
#[repr(C)]
pub struct GISTNodeBuffer {
    /* number of page blocks in the buffer */
    pub blocksCount: BlockNumber,

    /* block number of the last page in the buffer, on disk */
    pub pageBlocknum: BlockNumber,

    /* the last page in the buffer, in memory; NULL if not loaded */
    pub pageBuffer: *mut GISTNodeBufferPage,

    /* is this buffer queued for emptying? */
    pub queuedForEmptying: bool,

    /* is this a temporary copy, not in the hash table? */
    pub isTemp: bool,

    /* level of this node buffer */
    pub level: c_int,

    /* block number of the node this buffer is for (the hash key) */
    pub nodeBlocknum: BlockNumber,
}

/*
 * GISTBuildBuffers - data structure for the GiST buffering build algorithm.
 */
#[repr(C)]
pub struct GISTBuildBuffers {
    /* Persistent memory context for the data structures of the buffers. */
    pub context: MemoryContext,

    pub pfile: *mut BufFile, /* underlying temporary file */
    pub nFileBlocks: c_long, /* number of blocks used in the temp file */

    /* free blocks management */
    pub freeBlocks: *mut c_long,
    pub nFreeBlocks: c_int,   /* # of currently free blocks */
    pub freeBlocksLen: c_int, /* current allocated length of freeBlocks[] */

    /* hash table of node buffers, by block number */
    pub nodeBuffersTab: *mut HTAB,

    /* List of node buffers to be emptied */
    pub bufferEmptyingQueue: *mut List,

    pub pagesPerBuffer: c_int, /* approx. number of pages in each buffer */
    pub levelStep: c_int,      /* number of levels in each buffer step */

    /* array of lists of node buffers, one for each level */
    pub buffersOnLevels: *mut *mut List,
    pub buffersOnLevelsLen: c_int,

    /* array of node buffers with last page loaded in memory */
    pub loadedBuffers: *mut *mut GISTNodeBuffer,
    pub loadedBuffersCount: c_int, /* # of entries in loadedBuffers */
    pub loadedBuffersLen: c_int,   /* allocated size of loadedBuffers */
    pub rootlevel: c_int,
}

/*
 * LEVEL_HAS_BUFFERS: does the given level have buffers? Buffers exist on
 * levels that are a multiple of levelStep below the root level.
 */
#[inline]
unsafe fn LEVEL_HAS_BUFFERS(nlevel: c_int, gfbb: *mut GISTBuildBuffers) -> bool {
    nlevel != 0
        && (nlevel % (*gfbb).levelStep) == 0
        && nlevel != (*gfbb).rootlevel
}

/*
 * BUFFER_HALF_FILLED: is the buffer at least half full? Used to decide when
 * to queue a buffer for emptying.
 */
#[inline]
unsafe fn BUFFER_HALF_FILLED(nodeBuffer: *mut GISTNodeBuffer, gfbb: *mut GISTBuildBuffers) -> bool {
    (*nodeBuffer).blocksCount > (*gfbb).pagesPerBuffer as BlockNumber / 2
}

const GIST_ROOT_BLKNO: BlockNumber = 0;

/*
 * Data structure representing information about node buffer for index tuples
 * relocation from split node buffer.
 */
#[repr(C)]
struct RelocationBufferInfo {
    entry: [GISTENTRY; INDEX_MAX_KEYS],
    isnull: [bool; INDEX_MAX_KEYS],
    splitinfo: *mut GISTPageSplitInfo,
    nodeBuffer: *mut GISTNodeBuffer,
}

// ----------------------------------------------------------------
// Stubs for unported helper functions.
// ----------------------------------------------------------------

#[inline]
unsafe fn IndexTupleSize(itup: IndexTuple) -> Size {
    // IndexTupleSize(itup) = ((itup)->t_info & INDEX_SIZE_MASK)
    const INDEX_SIZE_MASK: u16 = 0x1FFF;
    ((*itup).t_info & INDEX_SIZE_MASK) as Size
}

unsafe fn hash_create(
    _tabname: *const c_char,
    _nelem: c_long,
    _info: *mut HASHCTL,
    _flags: c_int,
) -> *mut HTAB {
    unimplemented!() // TODO: utils/hash/dynahash.c
}

unsafe fn hash_search(
    _hashp: *mut HTAB,
    _keyPtr: *const c_void,
    _action: HASHACTION,
    _foundPtr: *mut bool,
) -> *mut c_void {
    unimplemented!() // TODO: utils/hash/dynahash.c
}

unsafe fn BufFileCreateTemp(_interXact: bool) -> *mut BufFile {
    unimplemented!() // TODO: storage/file/buffile.c
}

unsafe fn BufFileClose(_file: *mut BufFile) {
    unimplemented!() // TODO: storage/file/buffile.c
}

unsafe fn BufFileSeekBlock(_file: *mut BufFile, _blknum: c_long) -> c_int {
    unimplemented!() // TODO: storage/file/buffile.c
}

unsafe fn BufFileReadExact(_file: *mut BufFile, _ptr: *mut c_void, _size: Size) {
    unimplemented!() // TODO: storage/file/buffile.c
}

unsafe fn BufFileWrite(_file: *mut BufFile, _ptr: *const c_void, _size: Size) {
    unimplemented!() // TODO: storage/file/buffile.c
}

unsafe fn BufferGetBlockNumber(_buffer: Buffer) -> BlockNumber {
    unimplemented!() // TODO: storage/buffer/bufmgr.c
}

unsafe fn IndexRelationGetNumberOfKeyAttributes(_r: Relation) -> c_int {
    unimplemented!() // TODO: utils/rel.h
}

unsafe fn gistDeCompressAtt(
    _giststate: *mut GISTSTATE,
    _r: Relation,
    _tuple: IndexTuple,
    _p: Page,
    _o: OffsetNumber,
    _attdata: *mut GISTENTRY,
    _isnull: *mut bool,
) {
    unimplemented!() // TODO: access/gist/gistutil.c
}

unsafe fn gistpenalty(
    _giststate: *mut GISTSTATE,
    _attno: c_int,
    _orig: *mut GISTENTRY,
    _isNullOrig: bool,
    _add: *mut GISTENTRY,
    _isNullAdd: bool,
) -> f32 {
    unimplemented!() // TODO: access/gist/gistutil.c
}

unsafe fn gistgetadjusted(
    _r: Relation,
    _oldtup: IndexTuple,
    _addtup: IndexTuple,
    _giststate: *mut GISTSTATE,
) -> IndexTuple {
    unimplemented!() // TODO: access/gist/gistutil.c
}

// snprintf is not used here; elog is provided by prelude.

// ----------------------------------------------------------------
// Translated functions.
// ----------------------------------------------------------------

/*
 * Initialize GiST build buffers.
 */
pub unsafe fn gistInitBuildBuffers(
    pagesPerBuffer: c_int,
    levelStep: c_int,
    maxLevel: c_int,
) -> *mut GISTBuildBuffers {
    let gfbb: *mut GISTBuildBuffers;
    let mut hashCtl: HASHCTL = core::mem::zeroed();

    gfbb = palloc(core::mem::size_of::<GISTBuildBuffers>()) as *mut GISTBuildBuffers;
    (*gfbb).pagesPerBuffer = pagesPerBuffer;
    (*gfbb).levelStep = levelStep;

    /*
     * Create a temporary file to hold buffer pages that are swapped out of
     * memory.
     */
    (*gfbb).pfile = BufFileCreateTemp(false);
    (*gfbb).nFileBlocks = 0;

    /* Initialize free page management. */
    (*gfbb).nFreeBlocks = 0;
    (*gfbb).freeBlocksLen = 32;
    (*gfbb).freeBlocks =
        palloc((*gfbb).freeBlocksLen as usize * core::mem::size_of::<c_long>()) as *mut c_long;

    /*
     * Current memory context will be used for all in-memory data structures
     * of buffers which are persistent during buffering build.
     */
    (*gfbb).context = CurrentMemoryContext;

    /*
     * nodeBuffersTab hash is association between index blocks and it's
     * buffers.
     */
    hashCtl.keysize = core::mem::size_of::<BlockNumber>();
    hashCtl.entrysize = core::mem::size_of::<GISTNodeBuffer>();
    hashCtl.hcxt = CurrentMemoryContext;
    (*gfbb).nodeBuffersTab = hash_create(
        c"gistbuildbuffers".as_ptr(),
        1024,
        &mut hashCtl,
        HASH_ELEM | HASH_BLOBS | HASH_CONTEXT,
    );

    (*gfbb).bufferEmptyingQueue = NIL;

    /*
     * Per-level node buffers lists for final buffers emptying process. Node
     * buffers are inserted here when they are created.
     */
    (*gfbb).buffersOnLevelsLen = 1;
    (*gfbb).buffersOnLevels =
        palloc(core::mem::size_of::<*mut List>() * (*gfbb).buffersOnLevelsLen as usize)
            as *mut *mut List;
    *(*gfbb).buffersOnLevels.offset(0) = NIL;

    /*
     * Block numbers of node buffers which last pages are currently loaded
     * into main memory.
     */
    (*gfbb).loadedBuffersLen = 32;
    (*gfbb).loadedBuffers = palloc(
        (*gfbb).loadedBuffersLen as usize * core::mem::size_of::<*mut GISTNodeBuffer>(),
    ) as *mut *mut GISTNodeBuffer;
    (*gfbb).loadedBuffersCount = 0;

    (*gfbb).rootlevel = maxLevel;

    gfbb
}

/*
 * Returns a node buffer for given block. The buffer is created if it
 * doesn't exist yet.
 */
pub unsafe fn gistGetNodeBuffer(
    gfbb: *mut GISTBuildBuffers,
    _giststate: *mut GISTSTATE,
    nodeBlocknum: BlockNumber,
    level: c_int,
) -> *mut GISTNodeBuffer {
    let nodeBuffer: *mut GISTNodeBuffer;
    let mut found: bool = false;

    /* Find node buffer in hash table */
    nodeBuffer = hash_search(
        (*gfbb).nodeBuffersTab,
        &nodeBlocknum as *const BlockNumber as *const c_void,
        HASH_ENTER,
        &mut found,
    ) as *mut GISTNodeBuffer;
    if !found {
        /*
         * Node buffer wasn't found. Initialize the new buffer as empty.
         */
        let oldcxt: MemoryContext = MemoryContextSwitchTo((*gfbb).context);

        /* nodeBuffer->nodeBlocknum is the hash key and was filled in already */
        (*nodeBuffer).blocksCount = 0;
        (*nodeBuffer).pageBlocknum = InvalidBlockNumber;
        (*nodeBuffer).pageBuffer = core::ptr::null_mut();
        (*nodeBuffer).queuedForEmptying = false;
        (*nodeBuffer).isTemp = false;
        (*nodeBuffer).level = level;

        /*
         * Add this buffer to the list of buffers on this level. Enlarge
         * buffersOnLevels array if needed.
         */
        if level >= (*gfbb).buffersOnLevelsLen {
            (*gfbb).buffersOnLevels = repalloc(
                (*gfbb).buffersOnLevels as *mut c_void,
                (level + 1) as usize * core::mem::size_of::<*mut List>(),
            ) as *mut *mut List;

            /* initialize the enlarged portion */
            let mut i = (*gfbb).buffersOnLevelsLen;
            while i <= level {
                *(*gfbb).buffersOnLevels.offset(i as isize) = NIL;
                i += 1;
            }
            (*gfbb).buffersOnLevelsLen = level + 1;
        }

        /*
         * Prepend the new buffer to the list of buffers on this level. It's
         * not arbitrary that the new buffer is put to the beginning of the
         * list: in the final emptying phase we loop through all buffers at
         * each level, and flush them. If a page is split during the emptying,
         * it's more efficient to flush the new split pages first, before
         * moving on to pre-existing pages on the level. The buffers just
         * created during the page split are likely still in cache, so
         * flushing them immediately is more efficient than putting them to
         * the end of the queue.
         */
        *(*gfbb).buffersOnLevels.offset(level as isize) = lcons(
            nodeBuffer as *mut c_void,
            *(*gfbb).buffersOnLevels.offset(level as isize),
        );

        MemoryContextSwitchTo(oldcxt);
    }

    nodeBuffer
}

/*
 * Allocate memory for a buffer page.
 */
unsafe fn gistAllocateNewPageBuffer(gfbb: *mut GISTBuildBuffers) -> *mut GISTNodeBufferPage {
    let pageBuffer: *mut GISTNodeBufferPage;

    pageBuffer = MemoryContextAllocZero((*gfbb).context, BLCKSZ) as *mut GISTNodeBufferPage;
    (*pageBuffer).prev = InvalidBlockNumber;

    /* Set page free space */
    *PAGE_FREE_SPACE(pageBuffer) = (BLCKSZ - BUFFER_PAGE_DATA_OFFSET_VAL()) as c_int;
    pageBuffer
}

/*
 * Add specified buffer into loadedBuffers array.
 */
unsafe fn gistAddLoadedBuffer(gfbb: *mut GISTBuildBuffers, nodeBuffer: *mut GISTNodeBuffer) {
    /* Never add a temporary buffer to the array */
    if (*nodeBuffer).isTemp {
        return;
    }

    /* Enlarge the array if needed */
    if (*gfbb).loadedBuffersCount >= (*gfbb).loadedBuffersLen {
        (*gfbb).loadedBuffersLen *= 2;
        (*gfbb).loadedBuffers = repalloc(
            (*gfbb).loadedBuffers as *mut c_void,
            (*gfbb).loadedBuffersLen as usize * core::mem::size_of::<*mut GISTNodeBuffer>(),
        ) as *mut *mut GISTNodeBuffer;
    }

    *(*gfbb)
        .loadedBuffers
        .offset((*gfbb).loadedBuffersCount as isize) = nodeBuffer;
    (*gfbb).loadedBuffersCount += 1;
}

/*
 * Load last page of node buffer into main memory.
 */
unsafe fn gistLoadNodeBuffer(gfbb: *mut GISTBuildBuffers, nodeBuffer: *mut GISTNodeBuffer) {
    /* Check if we really should load something */
    if (*nodeBuffer).pageBuffer.is_null() && (*nodeBuffer).blocksCount > 0 {
        /* Allocate memory for page */
        (*nodeBuffer).pageBuffer = gistAllocateNewPageBuffer(gfbb);

        /* Read block from temporary file */
        ReadTempFileBlock(
            (*gfbb).pfile,
            (*nodeBuffer).pageBlocknum as c_long,
            (*nodeBuffer).pageBuffer as *mut c_void,
        );

        /* Mark file block as free */
        gistBuffersReleaseBlock(gfbb, (*nodeBuffer).pageBlocknum as c_long);

        /* Mark node buffer as loaded */
        gistAddLoadedBuffer(gfbb, nodeBuffer);
        (*nodeBuffer).pageBlocknum = InvalidBlockNumber;
    }
}

/*
 * Write last page of node buffer to the disk.
 */
unsafe fn gistUnloadNodeBuffer(gfbb: *mut GISTBuildBuffers, nodeBuffer: *mut GISTNodeBuffer) {
    /* Check if we have something to write */
    if !(*nodeBuffer).pageBuffer.is_null() {
        let blkno: BlockNumber;

        /* Get free file block */
        blkno = gistBuffersGetFreeBlock(gfbb) as BlockNumber;

        /* Write block to the temporary file */
        WriteTempFileBlock(
            (*gfbb).pfile,
            blkno as c_long,
            (*nodeBuffer).pageBuffer as *const c_void,
        );

        /* Free memory of that page */
        pfree((*nodeBuffer).pageBuffer as *mut c_void);
        (*nodeBuffer).pageBuffer = core::ptr::null_mut();

        /* Save block number */
        (*nodeBuffer).pageBlocknum = blkno;
    }
}

/*
 * Write last pages of all node buffers to the disk.
 */
pub unsafe fn gistUnloadNodeBuffers(gfbb: *mut GISTBuildBuffers) {
    /* Unload all the buffers that have a page loaded in memory. */
    let mut i = 0;
    while i < (*gfbb).loadedBuffersCount {
        gistUnloadNodeBuffer(gfbb, *(*gfbb).loadedBuffers.offset(i as isize));
        i += 1;
    }

    /* Now there are no node buffers with loaded last page */
    (*gfbb).loadedBuffersCount = 0;
}

/*
 * Add index tuple to buffer page.
 */
unsafe fn gistPlaceItupToPage(pageBuffer: *mut GISTNodeBufferPage, itup: IndexTuple) {
    let itupsz: Size = IndexTupleSize(itup);
    let ptr: *mut c_char;

    /* There should be enough of space. */
    Assert!(*PAGE_FREE_SPACE(pageBuffer) as usize >= MAXALIGN(itupsz));

    /* Reduce free space value of page to reserve a spot for the tuple. */
    *PAGE_FREE_SPACE(pageBuffer) -= MAXALIGN(itupsz) as c_int;

    /* Get pointer to the spot we reserved (ie. end of free space). */
    ptr = (pageBuffer as *mut c_char)
        .add(BUFFER_PAGE_DATA_OFFSET_VAL() + *PAGE_FREE_SPACE(pageBuffer) as usize);

    /* Copy the index tuple there. */
    memcpy(ptr as *mut c_void, itup as *const c_void, itupsz);
}

/*
 * Get last item from buffer page and remove it from page.
 */
unsafe fn gistGetItupFromPage(pageBuffer: *mut GISTNodeBufferPage, itup: *mut IndexTuple) {
    let ptr: IndexTuple;
    let itupsz: Size;

    Assert!(!PAGE_IS_EMPTY(pageBuffer)); /* Page shouldn't be empty */

    /* Get pointer to last index tuple */
    ptr = (pageBuffer as *mut c_char)
        .add(BUFFER_PAGE_DATA_OFFSET_VAL() + *PAGE_FREE_SPACE(pageBuffer) as usize)
        as IndexTuple;
    itupsz = IndexTupleSize(ptr);

    /* Make a copy of the tuple */
    *itup = palloc(itupsz) as IndexTuple;
    memcpy(*itup as *mut c_void, ptr as *const c_void, itupsz);

    /* Mark the space used by the tuple as free */
    *PAGE_FREE_SPACE(pageBuffer) += MAXALIGN(itupsz) as c_int;
}

/*
 * Push an index tuple to node buffer.
 */
pub unsafe fn gistPushItupToNodeBuffer(
    gfbb: *mut GISTBuildBuffers,
    nodeBuffer: *mut GISTNodeBuffer,
    itup: IndexTuple,
) {
    /*
     * Most part of memory operations will be in buffering build persistent
     * context. So, let's switch to it.
     */
    let oldcxt: MemoryContext = MemoryContextSwitchTo((*gfbb).context);

    /*
     * If the buffer is currently empty, create the first page.
     */
    if (*nodeBuffer).blocksCount == 0 {
        (*nodeBuffer).pageBuffer = gistAllocateNewPageBuffer(gfbb);
        (*nodeBuffer).blocksCount = 1;
        gistAddLoadedBuffer(gfbb, nodeBuffer);
    }

    /* Load last page of node buffer if it wasn't in memory already */
    if (*nodeBuffer).pageBuffer.is_null() {
        gistLoadNodeBuffer(gfbb, nodeBuffer);
    }

    /*
     * Check if there is enough space on the last page for the tuple.
     */
    if PAGE_NO_SPACE((*nodeBuffer).pageBuffer, itup) {
        /*
         * Nope. Swap previous block to disk and allocate a new one.
         */
        let blkno: BlockNumber;

        /* Write filled page to the disk */
        blkno = gistBuffersGetFreeBlock(gfbb) as BlockNumber;
        WriteTempFileBlock(
            (*gfbb).pfile,
            blkno as c_long,
            (*nodeBuffer).pageBuffer as *const c_void,
        );

        /*
         * Reset the in-memory page as empty, and link the previous block to
         * the new page by storing its block number in the prev-link.
         */
        *PAGE_FREE_SPACE((*nodeBuffer).pageBuffer) = (BLCKSZ
            - MAXALIGN(core::mem::offset_of!(GISTNodeBufferPage, tupledata)))
            as c_int;
        (*(*nodeBuffer).pageBuffer).prev = blkno;

        /* We've just added one more page */
        (*nodeBuffer).blocksCount += 1;
    }

    gistPlaceItupToPage((*nodeBuffer).pageBuffer, itup);

    /*
     * If the buffer just overflowed, add it to the emptying queue.
     */
    if BUFFER_HALF_FILLED(nodeBuffer, gfbb) && !(*nodeBuffer).queuedForEmptying {
        (*gfbb).bufferEmptyingQueue =
            lcons(nodeBuffer as *mut c_void, (*gfbb).bufferEmptyingQueue);
        (*nodeBuffer).queuedForEmptying = true;
    }

    /* Restore memory context */
    MemoryContextSwitchTo(oldcxt);
}

/*
 * Removes one index tuple from node buffer. Returns true if success and false
 * if node buffer is empty.
 */
pub unsafe fn gistPopItupFromNodeBuffer(
    gfbb: *mut GISTBuildBuffers,
    nodeBuffer: *mut GISTNodeBuffer,
    itup: *mut IndexTuple,
) -> bool {
    /*
     * If node buffer is empty then return false.
     */
    if (*nodeBuffer).blocksCount <= 0 {
        return false;
    }

    /* Load last page of node buffer if needed */
    if (*nodeBuffer).pageBuffer.is_null() {
        gistLoadNodeBuffer(gfbb, nodeBuffer);
    }

    /*
     * Get index tuple from last non-empty page.
     */
    gistGetItupFromPage((*nodeBuffer).pageBuffer, itup);

    /*
     * If we just removed the last tuple from the page, fetch previous page on
     * this node buffer (if any).
     */
    if PAGE_IS_EMPTY((*nodeBuffer).pageBuffer) {
        let prevblkno: BlockNumber;

        /*
         * blocksCount includes the page in pageBuffer, so decrease it now.
         */
        (*nodeBuffer).blocksCount -= 1;

        /*
         * If there's more pages, fetch previous one.
         */
        prevblkno = (*(*nodeBuffer).pageBuffer).prev;
        if prevblkno != InvalidBlockNumber {
            /* There is a previous page. Fetch it. */
            Assert!((*nodeBuffer).blocksCount > 0);
            ReadTempFileBlock(
                (*gfbb).pfile,
                prevblkno as c_long,
                (*nodeBuffer).pageBuffer as *mut c_void,
            );

            /*
             * Now that we've read the block in memory, we can release its
             * on-disk block for reuse.
             */
            gistBuffersReleaseBlock(gfbb, prevblkno as c_long);
        } else {
            /* No more pages. Free memory. */
            Assert!((*nodeBuffer).blocksCount == 0);
            pfree((*nodeBuffer).pageBuffer as *mut c_void);
            (*nodeBuffer).pageBuffer = core::ptr::null_mut();
        }
    }
    true
}

/*
 * Select a currently unused block for writing to.
 */
unsafe fn gistBuffersGetFreeBlock(gfbb: *mut GISTBuildBuffers) -> c_long {
    /*
     * If there are multiple free blocks, we select the one appearing last in
     * freeBlocks[].  If there are none, assign the next block at the end of
     * the file (causing the file to be extended).
     */
    if (*gfbb).nFreeBlocks > 0 {
        (*gfbb).nFreeBlocks -= 1;
        *(*gfbb).freeBlocks.offset((*gfbb).nFreeBlocks as isize)
    } else {
        let r = (*gfbb).nFileBlocks;
        (*gfbb).nFileBlocks += 1;
        r
    }
}

/*
 * Return a block# to the freelist.
 */
unsafe fn gistBuffersReleaseBlock(gfbb: *mut GISTBuildBuffers, blocknum: c_long) {
    let ndx: c_int;

    /* Enlarge freeBlocks array if full. */
    if (*gfbb).nFreeBlocks >= (*gfbb).freeBlocksLen {
        (*gfbb).freeBlocksLen *= 2;
        (*gfbb).freeBlocks = repalloc(
            (*gfbb).freeBlocks as *mut c_void,
            (*gfbb).freeBlocksLen as usize * core::mem::size_of::<c_long>(),
        ) as *mut c_long;
    }

    /* Add blocknum to array */
    ndx = (*gfbb).nFreeBlocks;
    (*gfbb).nFreeBlocks += 1;
    *(*gfbb).freeBlocks.offset(ndx as isize) = blocknum;
}

/*
 * Free buffering build data structure.
 */
pub unsafe fn gistFreeBuildBuffers(gfbb: *mut GISTBuildBuffers) {
    /* Close buffers file. */
    BufFileClose((*gfbb).pfile);

    /* All other things will be freed on memory context release */
}

/*
 * At page split, distribute tuples from the buffer of the split page to
 * new buffers for the created page halves. This also adjusts the downlinks
 * in 'splitinfo' to include the tuples in the buffers.
 */
pub unsafe fn gistRelocateBuildBuffersOnSplit(
    gfbb: *mut GISTBuildBuffers,
    giststate: *mut GISTSTATE,
    r: Relation,
    level: c_int,
    buffer: Buffer,
    splitinfo: *mut List,
) {
    let relocationBuffersInfos: *mut RelocationBufferInfo;
    let mut found: bool = false;
    let nodeBuffer: *mut GISTNodeBuffer;
    let blocknum: BlockNumber;
    let mut itup: IndexTuple = core::ptr::null_mut();
    let splitPagesCount: c_int;
    let mut entry: [GISTENTRY; INDEX_MAX_KEYS] = core::mem::zeroed();
    let mut isnull: [bool; INDEX_MAX_KEYS] = [false; INDEX_MAX_KEYS];
    let mut oldBuf: GISTNodeBuffer = core::mem::zeroed();
    // ListCell *lc is declared inside foreach! below

    /* If the split page doesn't have buffers, we have nothing to do. */
    if !LEVEL_HAS_BUFFERS(level, gfbb) {
        return;
    }

    /*
     * Get the node buffer of the split page.
     */
    blocknum = BufferGetBlockNumber(buffer);
    nodeBuffer = hash_search(
        (*gfbb).nodeBuffersTab,
        &blocknum as *const BlockNumber as *const c_void,
        HASH_FIND,
        &mut found,
    ) as *mut GISTNodeBuffer;
    if !found {
        /* The page has no buffer, so we have nothing to do. */
        return;
    }

    /*
     * Make a copy of the old buffer, as we're going reuse it as the buffer
     * for the new left page, which is on the same block as the old page.
     * That's not true for the root page, but that's fine because we never
     * have a buffer on the root page anyway. The original algorithm as
     * described by Arge et al did, but it's of no use, as you might as well
     * read the tuples straight from the heap instead of the root buffer.
     */
    Assert!(blocknum != GIST_ROOT_BLKNO);
    memcpy(
        &mut oldBuf as *mut GISTNodeBuffer as *mut c_void,
        nodeBuffer as *const c_void,
        core::mem::size_of::<GISTNodeBuffer>(),
    );
    oldBuf.isTemp = true;

    /* Reset the old buffer, used for the new left page from now on */
    (*nodeBuffer).blocksCount = 0;
    (*nodeBuffer).pageBuffer = core::ptr::null_mut();
    (*nodeBuffer).pageBlocknum = InvalidBlockNumber;

    /*
     * Allocate memory for information about relocation buffers.
     */
    splitPagesCount = list_length(splitinfo);
    relocationBuffersInfos = palloc(
        core::mem::size_of::<RelocationBufferInfo>() * splitPagesCount as usize,
    ) as *mut RelocationBufferInfo;

    /*
     * Fill relocation buffers information for node buffers of pages produced
     * by split.
     */
    foreach!(lc, splitinfo, {
        let si: *mut GISTPageSplitInfo = lfirst(current_cell!(lc)) as *mut GISTPageSplitInfo;
        let newNodeBuffer: *mut GISTNodeBuffer;
        let i: c_int = foreach_current_index(current_cell!(lc));

        /* Decompress parent index tuple of node buffer page. */
        gistDeCompressAtt(
            giststate,
            r,
            (*si).downlink,
            core::ptr::null_mut(),
            0 as OffsetNumber,
            (*relocationBuffersInfos.offset(i as isize)).entry.as_mut_ptr(),
            (*relocationBuffersInfos.offset(i as isize)).isnull.as_mut_ptr(),
        );

        /*
         * Create a node buffer for the page. The leftmost half is on the same
         * block as the old page before split, so for the leftmost half this
         * will return the original buffer. The tuples on the original buffer
         * were relinked to the temporary buffer, so the original one is now
         * empty.
         */
        newNodeBuffer =
            gistGetNodeBuffer(gfbb, giststate, BufferGetBlockNumber((*si).buf), level);

        (*relocationBuffersInfos.offset(i as isize)).nodeBuffer = newNodeBuffer;
        (*relocationBuffersInfos.offset(i as isize)).splitinfo = si;
    });

    /*
     * Loop through all index tuples in the buffer of the page being split,
     * moving them to buffers for the new pages.  We try to move each tuple to
     * the page that will result in the lowest penalty for the leading column
     * or, in the case of a tie, the lowest penalty for the earliest column
     * that is not tied.
     *
     * The page searching logic is very similar to gistchoose().
     */
    while gistPopItupFromNodeBuffer(gfbb, &mut oldBuf, &mut itup) {
        let mut best_penalty: [f32; INDEX_MAX_KEYS] = [0.0; INDEX_MAX_KEYS];
        let mut i: c_int;
        let mut which: c_int;
        let newtup: IndexTuple;
        let targetBufferInfo: *mut RelocationBufferInfo;

        gistDeCompressAtt(
            giststate,
            r,
            itup,
            core::ptr::null_mut(),
            0 as OffsetNumber,
            entry.as_mut_ptr(),
            isnull.as_mut_ptr(),
        );

        /* default to using first page (shouldn't matter) */
        which = 0;

        /*
         * best_penalty[j] is the best penalty we have seen so far for column
         * j, or -1 when we haven't yet examined column j.  Array entries to
         * the right of the first -1 are undefined.
         */
        best_penalty[0] = -1.0;

        /*
         * Loop over possible target pages, looking for one to move this tuple
         * to.
         */
        i = 0;
        while i < splitPagesCount {
            let splitPageInfo: *mut RelocationBufferInfo =
                relocationBuffersInfos.offset(i as isize);
            let mut zero_penalty: bool;
            let mut j: c_int;

            zero_penalty = true;

            /* Loop over index attributes. */
            j = 0;
            while j < IndexRelationGetNumberOfKeyAttributes(r) {
                let usize_: f32;

                /* Compute penalty for this column. */
                usize_ = gistpenalty(
                    giststate,
                    j,
                    &mut (*splitPageInfo).entry[j as usize],
                    (*splitPageInfo).isnull[j as usize],
                    &mut entry[j as usize],
                    isnull[j as usize],
                );
                if usize_ > 0.0 {
                    zero_penalty = false;
                }

                if best_penalty[j as usize] < 0.0 || usize_ < best_penalty[j as usize] {
                    /*
                     * New best penalty for column.  Tentatively select this
                     * page as the target, and record the best penalty.  Then
                     * reset the next column's penalty to "unknown" (and
                     * indirectly, the same for all the ones to its right).
                     * This will force us to adopt this page's penalty values
                     * as the best for all the remaining columns during
                     * subsequent loop iterations.
                     */
                    which = i;
                    best_penalty[j as usize] = usize_;

                    if j < IndexRelationGetNumberOfKeyAttributes(r) - 1 {
                        best_penalty[(j + 1) as usize] = -1.0;
                    }
                } else if best_penalty[j as usize] == usize_ {
                    /*
                     * The current page is exactly as good for this column as
                     * the best page seen so far.  The next iteration of this
                     * loop will compare the next column.
                     */
                } else {
                    /*
                     * The current page is worse for this column than the best
                     * page seen so far.  Skip the remaining columns and move
                     * on to the next page, if any.
                     */
                    zero_penalty = false; /* so outer loop won't exit */
                    break;
                }

                j += 1;
            }

            /*
             * If we find a page with zero penalty for all columns, there's no
             * need to examine remaining pages; just break out of the loop and
             * return it.
             */
            if zero_penalty {
                break;
            }

            i += 1;
        }

        /* OK, "which" is the page index to push the tuple to */
        targetBufferInfo = relocationBuffersInfos.offset(which as isize);

        /* Push item to selected node buffer */
        gistPushItupToNodeBuffer(gfbb, (*targetBufferInfo).nodeBuffer, itup);

        /* Adjust the downlink for this page, if needed. */
        newtup = gistgetadjusted(r, (*(*targetBufferInfo).splitinfo).downlink, itup, giststate);
        if !newtup.is_null() {
            gistDeCompressAtt(
                giststate,
                r,
                newtup,
                core::ptr::null_mut(),
                0 as OffsetNumber,
                (*targetBufferInfo).entry.as_mut_ptr(),
                (*targetBufferInfo).isnull.as_mut_ptr(),
            );

            (*(*targetBufferInfo).splitinfo).downlink = newtup;
        }
    }

    pfree(relocationBuffersInfos as *mut c_void);
}

/*
 * Wrappers around BufFile operations. The main difference is that these
 * wrappers report errors with ereport(), so that the callers don't need
 * to check the return code.
 */

unsafe fn ReadTempFileBlock(file: *mut BufFile, blknum: c_long, ptr: *mut c_void) {
    if BufFileSeekBlock(file, blknum) != 0 {
        elog!(
            ERROR,
            "could not seek to block {} in temporary file",
            blknum
        );
    }
    BufFileReadExact(file, ptr, BLCKSZ);
}

unsafe fn WriteTempFileBlock(file: *mut BufFile, blknum: c_long, ptr: *const c_void) {
    if BufFileSeekBlock(file, blknum) != 0 {
        elog!(
            ERROR,
            "could not seek to block {} in temporary file",
            blknum
        );
    }
    BufFileWrite(file, ptr, BLCKSZ);
}

// foreach_current_index(cell) - index of the current cell within its list.
// Provided locally as the macro isn't a translated helper.
unsafe fn foreach_current_index(_lc: *mut ListCell) -> c_int {
    unimplemented!() // TODO: nodes/pg_list.h
}

// memcpy / strlen via libc.
extern "C" {
    fn memcpy(dest: *mut c_void, src: *const c_void, n: Size) -> *mut c_void;
}

use crate::{current_cell, foreach};
