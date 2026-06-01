//! logtape.rs
//!   Management of "logical tapes" within temporary files.
//!
//! Translated 1:1 from postgres/src/backend/utils/sort/logtape.c
//!
//! This module exists to support sorting via multiple merge passes (see
//! tuplesort.c).  Merging is an ideal algorithm for tape devices, but if
//! we implement it on disk by creating a separate file for each "tape",
//! there is an annoying problem: the peak space usage is at least twice
//! the volume of actual data to be sorted.  (This must be so because each
//! datum will appear in both the input and output tapes of the final
//! merge pass.)
//!
//! We can work around this problem by recognizing that any one tape
//! dataset (with the possible exception of the final output) is written
//! and read exactly once in a perfectly sequential manner.  Therefore,
//! a datum once read will not be required again, and we can recycle its
//! space for use by the new tape dataset(s) being generated.  In this way,
//! the total space usage is essentially just the actual data volume, plus
//! insignificant bookkeeping and start/stop overhead.
//!
//! Few OSes allow arbitrary parts of a file to be released back to the OS,
//! so we have to implement this space-recycling ourselves within a single
//! logical file.  logtape.c exists to perform this bookkeeping and provide
//! the illusion of N independent tape devices to tuplesort.c.  Note that
//! logtape.c itself depends on buffile.c to provide a "logical file" of
//! larger size than the underlying OS may support.
//!
//! For simplicity, we allocate and release space in the underlying file
//! in BLCKSZ-size blocks.  Space allocation boils down to keeping track
//! of which blocks in the underlying file belong to which logical tape,
//! plus any blocks that are free (recycled and not yet reused).
//! The blocks in each logical tape form a chain, with a prev- and next-
//! pointer in each block.
//!
//! The initial write pass is guaranteed to fill the underlying file
//! perfectly sequentially, no matter how data is divided into logical tapes.
//! Once we begin merge passes, the access pattern becomes considerably
//! less predictable --- but the seeking involved should be comparable to
//! what would happen if we kept each logical tape in a separate file,
//! so there's no serious performance penalty paid to obtain the space
//! savings of recycling.  We try to localize the write accesses by always
//! writing to the lowest-numbered free block when we have a choice; it's
//! not clear this helps much, but it can't hurt.  (XXX perhaps a LIFO
//! policy for free blocks would be better?)
//!
//! To further make the I/Os more sequential, we can use a larger buffer
//! when reading, and read multiple blocks from the same tape in one go,
//! whenever the buffer becomes empty.
//!
//! To support the above policy of writing to the lowest free block, the
//! freelist is a min heap.
//!
//! Since all the bookkeeping and buffer memory is allocated with palloc(),
//! and the underlying file(s) are made with OpenTemporaryFile, all resources
//! for a logical tape set are certain to be cleaned up even if processing
//! is aborted by ereport(ERROR).  To avoid confusion, the caller should take
//! care that all calls for a single LogicalTapeSet are made in the same
//! palloc context.
//!
//! To support parallel sort operations involving coordinated callers to
//! tuplesort.c routines across multiple workers, it is necessary to
//! concatenate each worker BufFile/tapeset into one single logical tapeset
//! managed by the leader.  Workers should have produced one final
//! materialized tape (their entire output) when this happens in leader.
//! There will always be the same number of runs as input tapes, and the same
//! number of input tapes as participants (worker Tuplesortstates).
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!   src/backend/utils/sort/logtape.c

#![allow(unused_variables)]
#![allow(dead_code)]

use crate::prelude::*;

use crate::c::{int64, uint64, Min, MemSet, Size};
use crate::pg_config::BLCKSZ;
use crate::pg_config_manual::MAXPGPATH;
use crate::storage::file::buffile::{
    BufFile, BufFileAppend, BufFileClose, BufFileCreateFileSet, BufFileCreateTemp,
    BufFileExportFileSet, BufFileOpenFileSet, BufFileReadExact, BufFileSeekBlock, BufFileSize,
    BufFileWrite,
};
use crate::storage::file::sharedfileset::SharedFileSet;
use crate::utils::adt::numutils::pg_itoa;
use std::ffi::{c_char, c_int, c_void};

// errcode_for_file_access(): translate errno to a SQLSTATE error code.
// TODO(pg-port): real errcode_for_file_access lives in utils/error/elog.c.
unsafe fn errcode_for_file_access() -> c_int {
    0
}

// O_RDONLY open flag used by BufFileOpenFileSet().
// TODO(pg-port): real O_RDONLY comes from <fcntl.h>.
const O_RDONLY: c_int = 0;

// VALGRIND_MAKE_MEM_DEFINED: no-op outside of valgrind builds.
// TODO(pg-port): real VALGRIND_MAKE_MEM_DEFINED lives in utils/memdebug.h.
unsafe fn VALGRIND_MAKE_MEM_DEFINED(_addr: *mut c_void, _size: usize) {}

// PGIOAlignedBlock: a BLCKSZ-sized buffer aligned for direct I/O.
// TODO(pg-port): real PGIOAlignedBlock lives in storage/bufpage.h.
#[repr(C, align(4096))]
pub struct PGIOAlignedBlock {
    pub data: [c_char; BLCKSZ],
}

// ----- header file: src/include/utils/logtape.h -----

/*
 * LogicalTapeSet and LogicalTape are opaque types whose details are not
 * known outside logtape.c.
 */
// (defined below as LogicalTapeSet / LogicalTape)

/*
 * The approach tuplesort.c takes to parallel external sorts is that workers,
 * whose state is almost the same as serial sorts, are made to produce a final
 * materialized tape of sorted output in all cases.  This is frozen, just like
 * any case requiring a final materialized tape.  However, there is one
 * difference, which is that freezing will also export an underlying shared
 * fileset BufFile for sharing.  Freezing produces TapeShare metadata that the
 * leader is passed for each worker tape that it must consume, which only
 * happens after workers have actually finished spilling, and made this data
 * available.
 *
 * The data in datum is used to indicate the location of the worker tape's
 * first block (as relevant to the leader, which performs leader-managed
 * spilling).
 */
#[repr(C)]
pub struct TapeShare {
    /* Currently, all the leader process needs is the location of the
     * materialized tape's first block.
     */
    pub firstblocknumber: int64,
}

// ----- end header -----

/*
 * A TapeBlockTrailer is stored at the end of each BLCKSZ block.
 *
 * The first block of a tape has prev == -1.  The last block of a tape
 * stores the number of valid bytes on the block, inverted, in 'next'
 * Therefore next < 0 indicates the last block.
 */
#[repr(C)]
pub struct TapeBlockTrailer {
    pub prev: int64, /* previous block on this tape, or -1 on first
                      * block */
    pub next: int64, /* next block on this tape, or # of valid
                      * bytes on last block (if < 0) */
}

const TapeBlockPayloadSize: usize = BLCKSZ - std::mem::size_of::<TapeBlockTrailer>();

#[inline]
unsafe fn TapeBlockGetTrailer(buf: *mut c_void) -> *mut TapeBlockTrailer {
    ((buf as *mut c_char).add(TapeBlockPayloadSize)) as *mut TapeBlockTrailer
}

#[inline]
unsafe fn TapeBlockIsLast(buf: *mut c_void) -> bool {
    (*TapeBlockGetTrailer(buf)).next < 0
}

#[inline]
unsafe fn TapeBlockGetNBytes(buf: *mut c_void) -> int64 {
    if TapeBlockIsLast(buf) {
        -(*TapeBlockGetTrailer(buf)).next
    } else {
        TapeBlockPayloadSize as int64
    }
}

#[inline]
unsafe fn TapeBlockSetNBytes(buf: *mut c_void, nbytes: int64) {
    (*TapeBlockGetTrailer(buf)).next = -(nbytes);
}

/*
 * When multiple tapes are being written to concurrently (as in HashAgg),
 * avoid excessive fragmentation by preallocating block numbers to individual
 * tapes. Each preallocation doubles in size starting at
 * TAPE_WRITE_PREALLOC_MIN blocks up to TAPE_WRITE_PREALLOC_MAX blocks.
 *
 * No filesystem operations are performed for preallocation; only the block
 * numbers are reserved. This may lead to sparse writes, which will cause
 * ltsWriteBlock() to fill in holes with zeros.
 */
const TAPE_WRITE_PREALLOC_MIN: c_int = 8;
const TAPE_WRITE_PREALLOC_MAX: c_int = 128;

/*
 * This data structure represents a single "logical tape" within the set
 * of logical tapes stored in the same file.
 *
 * While writing, we hold the current partially-written data block in the
 * buffer.  While reading, we can hold multiple blocks in the buffer.  Note
 * that we don't retain the trailers of a block when it's read into the
 * buffer.  The buffer therefore contains one large contiguous chunk of data
 * from the tape.
 */
#[repr(C)]
pub struct LogicalTape {
    pub tapeSet: *mut LogicalTapeSet, /* tape set this tape is part of */

    pub writing: bool, /* T while in write phase */
    pub frozen: bool,  /* T if blocks should not be freed when read */
    pub dirty: bool,   /* does buffer need to be written? */

    /*
     * Block numbers of the first, current, and next block of the tape.
     *
     * The "current" block number is only valid when writing, or reading from
     * a frozen tape.  (When reading from an unfrozen tape, we use a larger
     * read buffer that holds multiple blocks, so the "current" block is
     * ambiguous.)
     *
     * When concatenation of worker tape BufFiles is performed, an offset to
     * the first block in the unified BufFile space is applied during reads.
     */
    pub firstBlockNumber: int64,
    pub curBlockNumber: int64,
    pub nextBlockNumber: int64,
    pub offsetBlockNumber: int64,

    /*
     * Buffer for current data block(s).
     */
    pub buffer: *mut c_char, /* physical buffer (separately palloc'd) */
    pub buffer_size: c_int,  /* allocated size of the buffer */
    pub max_size: c_int,     /* highest useful, safe buffer_size */
    pub pos: c_int,          /* next read/write position in buffer */
    pub nbytes: c_int,       /* total # of valid bytes in buffer */

    /*
     * Preallocated block numbers are held in an array sorted in descending
     * order; blocks are consumed from the end of the array (lowest block
     * numbers first).
     */
    pub prealloc: *mut int64,
    pub nprealloc: c_int,      /* number of elements in list */
    pub prealloc_size: c_int,  /* number of elements list can hold */
}

/*
 * This data structure represents a set of related "logical tapes" sharing
 * space in a single underlying file.  (But that "file" may be multiple files
 * if needed to escape OS limits on file size; buffile.c handles that for us.)
 * Tapes belonging to a tape set can be created and destroyed on-the-fly, on
 * demand.
 */
#[repr(C)]
pub struct LogicalTapeSet {
    pub pfile: *mut BufFile, /* underlying file for whole tape set */
    pub fileset: *mut SharedFileSet,
    pub worker: c_int, /* worker # if shared, -1 for leader/serial */

    /*
     * File size tracking.  nBlocksWritten is the size of the underlying file,
     * in BLCKSZ blocks.  nBlocksAllocated is the number of blocks allocated
     * by ltsReleaseBlock(), and it is always greater than or equal to
     * nBlocksWritten.  Blocks between nBlocksAllocated and nBlocksWritten are
     * blocks that have been allocated for a tape, but have not been written
     * to the underlying file yet.  nHoleBlocks tracks the total number of
     * blocks that are in unused holes between worker spaces following BufFile
     * concatenation.
     */
    pub nBlocksAllocated: int64, /* # of blocks allocated */
    pub nBlocksWritten: int64,   /* # of blocks used in underlying file */
    pub nHoleBlocks: int64,      /* # of "hole" blocks left */

    /*
     * We store the numbers of recycled-and-available blocks in freeBlocks[].
     * When there are no such blocks, we extend the underlying file.
     *
     * If forgetFreeSpace is true then any freed blocks are simply forgotten
     * rather than being remembered in freeBlocks[].  See notes for
     * LogicalTapeSetForgetFreeSpace().
     */
    pub forgetFreeSpace: bool, /* are we remembering free blocks? */
    pub freeBlocks: *mut int64, /* resizable array holding minheap */
    pub nFreeBlocks: int64,    /* # of currently free blocks */
    pub freeBlocksLen: Size,   /* current allocated length of freeBlocks[] */
    pub enable_prealloc: bool, /* preallocate write blocks? */
}

/*
 * Write a block-sized buffer to the specified block of the underlying file.
 *
 * No need for an error return convention; we ereport() on any error.
 */
unsafe fn ltsWriteBlock(lts: *mut LogicalTapeSet, blocknum: int64, buffer: *const c_void) {
    /*
     * BufFile does not support "holes", so if we're about to write a block
     * that's past the current end of file, fill the space between the current
     * end of file and the target block with zeros.
     *
     * This can happen either when tapes preallocate blocks; or for the last
     * block of a tape which might not have been flushed.
     *
     * Note that BufFile concatenation can leave "holes" in BufFile between
     * worker-owned block ranges.  These are tracked for reporting purposes
     * only.  We never read from nor write to these hole blocks, and so they
     * are not considered here.
     */
    while blocknum > (*lts).nBlocksWritten {
        let mut zerobuf: PGIOAlignedBlock = PGIOAlignedBlock { data: [0; BLCKSZ] };

        MemSet(
            zerobuf.data.as_mut_ptr() as *mut c_void,
            0,
            std::mem::size_of::<PGIOAlignedBlock>(),
        );

        ltsWriteBlock(
            lts,
            (*lts).nBlocksWritten,
            zerobuf.data.as_ptr() as *const c_void,
        );
    }

    /* Write the requested block */
    if BufFileSeekBlock((*lts).pfile, blocknum) != 0 {
        ereport!(
            ERROR,
            errmsg!(
                "could not seek to block {} of temporary file",
                blocknum
            )
        );
    }
    BufFileWrite((*lts).pfile, buffer, BLCKSZ);

    /* Update nBlocksWritten, if we extended the file */
    if blocknum == (*lts).nBlocksWritten {
        (*lts).nBlocksWritten += 1;
    }
}

/*
 * Read a block-sized buffer from the specified block of the underlying file.
 *
 * No need for an error return convention; we ereport() on any error.   This
 * module should never attempt to read a block it doesn't know is there.
 */
unsafe fn ltsReadBlock(lts: *mut LogicalTapeSet, blocknum: int64, buffer: *mut c_void) {
    if BufFileSeekBlock((*lts).pfile, blocknum) != 0 {
        ereport!(
            ERROR,
            errmsg!(
                "could not seek to block {} of temporary file",
                blocknum
            )
        );
    }
    BufFileReadExact((*lts).pfile, buffer, BLCKSZ);
}

/*
 * Read as many blocks as we can into the per-tape buffer.
 *
 * Returns true if anything was read, 'false' on EOF.
 */
unsafe fn ltsReadFillBuffer(lt: *mut LogicalTape) -> bool {
    (*lt).pos = 0;
    (*lt).nbytes = 0;

    loop {
        let thisbuf: *mut c_char = (*lt).buffer.add((*lt).nbytes as usize);
        let mut datablocknum: int64 = (*lt).nextBlockNumber;

        /* Fetch next block number */
        if datablocknum == -1 {
            break; /* EOF */
        }
        /* Apply worker offset, needed for leader tapesets */
        datablocknum += (*lt).offsetBlockNumber;

        /* Read the block */
        ltsReadBlock((*lt).tapeSet, datablocknum, thisbuf as *mut c_void);
        if !(*lt).frozen {
            ltsReleaseBlock((*lt).tapeSet, datablocknum);
        }
        (*lt).curBlockNumber = (*lt).nextBlockNumber;

        (*lt).nbytes += TapeBlockGetNBytes(thisbuf as *mut c_void) as c_int;
        if TapeBlockIsLast(thisbuf as *mut c_void) {
            (*lt).nextBlockNumber = -1;
            /* EOF */
            break;
        } else {
            (*lt).nextBlockNumber = (*TapeBlockGetTrailer(thisbuf as *mut c_void)).next;
        }

        /* Advance to next block, if we have buffer space left */
        if !((*lt).buffer_size - (*lt).nbytes > BLCKSZ as c_int) {
            break;
        }
    }

    (*lt).nbytes > 0
}

#[inline]
unsafe fn left_offset(i: uint64) -> uint64 {
    2 * i + 1
}

#[inline]
unsafe fn right_offset(i: uint64) -> uint64 {
    2 * i + 2
}

#[inline]
unsafe fn parent_offset(i: uint64) -> uint64 {
    (i - 1) / 2
}

/*
 * Get the next block for writing.
 */
unsafe fn ltsGetBlock(lts: *mut LogicalTapeSet, lt: *mut LogicalTape) -> int64 {
    if (*lts).enable_prealloc {
        ltsGetPreallocBlock(lts, lt)
    } else {
        ltsGetFreeBlock(lts)
    }
}

/*
 * Select the lowest currently unused block from the tape set's global free
 * list min heap.
 */
unsafe fn ltsGetFreeBlock(lts: *mut LogicalTapeSet) -> int64 {
    let heap: *mut int64 = (*lts).freeBlocks;
    let blocknum: int64;
    let heapsize: int64;
    let holeval: int64;
    let mut holepos: uint64;

    /* freelist empty; allocate a new block */
    if (*lts).nFreeBlocks == 0 {
        let ret = (*lts).nBlocksAllocated;
        (*lts).nBlocksAllocated += 1;
        return ret;
    }

    /* easy if heap contains one element */
    if (*lts).nFreeBlocks == 1 {
        (*lts).nFreeBlocks -= 1;
        return *(*lts).freeBlocks.add(0);
    }

    /* remove top of minheap */
    blocknum = *heap.add(0);

    /* we'll replace it with end of minheap array */
    (*lts).nFreeBlocks -= 1;
    holeval = *heap.add((*lts).nFreeBlocks as usize);

    /* sift down */
    holepos = 0; /* holepos is where the "hole" is */
    heapsize = (*lts).nFreeBlocks;
    loop {
        let left: uint64 = left_offset(holepos);
        let right: uint64 = right_offset(holepos);
        let min_child: uint64;

        if (left as int64) < heapsize && (right as int64) < heapsize {
            min_child = if *heap.add(left as usize) < *heap.add(right as usize) {
                left
            } else {
                right
            };
        } else if (left as int64) < heapsize {
            min_child = left;
        } else if (right as int64) < heapsize {
            min_child = right;
        } else {
            break;
        }

        if *heap.add(min_child as usize) >= holeval {
            break;
        }

        *heap.add(holepos as usize) = *heap.add(min_child as usize);
        holepos = min_child;
    }
    *heap.add(holepos as usize) = holeval;

    blocknum
}

/*
 * Return the lowest free block number from the tape's preallocation list.
 * Refill the preallocation list with blocks from the tape set's free list if
 * necessary.
 */
unsafe fn ltsGetPreallocBlock(lts: *mut LogicalTapeSet, lt: *mut LogicalTape) -> int64 {
    /* sorted in descending order, so return the last element */
    if (*lt).nprealloc > 0 {
        (*lt).nprealloc -= 1;
        return *(*lt).prealloc.add((*lt).nprealloc as usize);
    }

    if (*lt).prealloc.is_null() {
        (*lt).prealloc_size = TAPE_WRITE_PREALLOC_MIN;
        (*lt).prealloc =
            palloc(std::mem::size_of::<int64>() * (*lt).prealloc_size as usize) as *mut int64;
    } else if (*lt).prealloc_size < TAPE_WRITE_PREALLOC_MAX {
        /* when the preallocation list runs out, double the size */
        (*lt).prealloc_size *= 2;
        if (*lt).prealloc_size > TAPE_WRITE_PREALLOC_MAX {
            (*lt).prealloc_size = TAPE_WRITE_PREALLOC_MAX;
        }
        (*lt).prealloc = repalloc(
            (*lt).prealloc as *mut c_void,
            std::mem::size_of::<int64>() * (*lt).prealloc_size as usize,
        ) as *mut int64;
    }

    /* refill preallocation list */
    (*lt).nprealloc = (*lt).prealloc_size;
    let mut i = (*lt).nprealloc;
    while i > 0 {
        *(*lt).prealloc.add((i - 1) as usize) = ltsGetFreeBlock(lts);

        /* verify descending order */
        Assert!(
            i == (*lt).nprealloc
                || *(*lt).prealloc.add((i - 1) as usize) > *(*lt).prealloc.add(i as usize)
        );
        i -= 1;
    }

    (*lt).nprealloc -= 1;
    *(*lt).prealloc.add((*lt).nprealloc as usize)
}

/*
 * Return a block# to the freelist.
 */
unsafe fn ltsReleaseBlock(lts: *mut LogicalTapeSet, blocknum: int64) {
    let heap: *mut int64;
    let mut holepos: uint64;

    /*
     * Do nothing if we're no longer interested in remembering free space.
     */
    if (*lts).forgetFreeSpace {
        return;
    }

    /*
     * Enlarge freeBlocks array if full.
     */
    if (*lts).nFreeBlocks >= (*lts).freeBlocksLen as int64 {
        /*
         * If the freelist becomes very large, just return and leak this free
         * block.
         */
        if (*lts).freeBlocksLen * 2 * std::mem::size_of::<int64>() > MaxAllocSize {
            return;
        }

        (*lts).freeBlocksLen *= 2;
        (*lts).freeBlocks = repalloc(
            (*lts).freeBlocks as *mut c_void,
            (*lts).freeBlocksLen * std::mem::size_of::<int64>(),
        ) as *mut int64;
    }

    /* create a "hole" at end of minheap array */
    heap = (*lts).freeBlocks;
    holepos = (*lts).nFreeBlocks as uint64;
    (*lts).nFreeBlocks += 1;

    /* sift up to insert blocknum */
    while holepos != 0 {
        let parent: uint64 = parent_offset(holepos);

        if *heap.add(parent as usize) < blocknum {
            break;
        }

        *heap.add(holepos as usize) = *heap.add(parent as usize);
        holepos = parent;
    }
    *heap.add(holepos as usize) = blocknum;
}

/*
 * Lazily allocate and initialize the read buffer. This avoids waste when many
 * tapes are open at once, but not all are active between rewinding and
 * reading.
 */
unsafe fn ltsInitReadBuffer(lt: *mut LogicalTape) {
    Assert!((*lt).buffer_size > 0);
    (*lt).buffer = palloc((*lt).buffer_size as usize) as *mut c_char;

    /* Read the first block, or reset if tape is empty */
    (*lt).nextBlockNumber = (*lt).firstBlockNumber;
    (*lt).pos = 0;
    (*lt).nbytes = 0;
    ltsReadFillBuffer(lt);
}

/*
 * Create a tape set, backed by a temporary underlying file.
 *
 * The tape set is initially empty. Use LogicalTapeCreate() to create
 * tapes in it.
 *
 * In a single-process sort, pass NULL argument for fileset, and -1 for
 * worker.
 *
 * In a parallel sort, parallel workers pass the shared fileset handle and
 * their own worker number.  After the workers have finished, create the
 * tape set in the leader, passing the shared fileset handle and -1 for
 * worker, and use LogicalTapeImport() to import the worker tapes into it.
 *
 * Currently, the leader will only import worker tapes into the set, it does
 * not create tapes of its own, although in principle that should work.
 *
 * If preallocate is true, blocks for each individual tape are allocated in
 * batches.  This avoids fragmentation when writing multiple tapes at the
 * same time.
 */
pub unsafe fn LogicalTapeSetCreate(
    preallocate: bool,
    fileset: *mut SharedFileSet,
    worker: c_int,
) -> *mut LogicalTapeSet {
    let lts: *mut LogicalTapeSet;

    /*
     * Create top-level struct including per-tape LogicalTape structs.
     */
    lts = palloc(std::mem::size_of::<LogicalTapeSet>()) as *mut LogicalTapeSet;
    (*lts).nBlocksAllocated = 0;
    (*lts).nBlocksWritten = 0;
    (*lts).nHoleBlocks = 0;
    (*lts).forgetFreeSpace = false;
    (*lts).freeBlocksLen = 32; /* reasonable initial guess */
    (*lts).freeBlocks = palloc((*lts).freeBlocksLen * std::mem::size_of::<int64>()) as *mut int64;
    (*lts).nFreeBlocks = 0;
    (*lts).enable_prealloc = preallocate;

    (*lts).fileset = fileset;
    (*lts).worker = worker;

    /*
     * Create temp BufFile storage as required.
     *
     * In leader, we hijack the BufFile of the first tape that's imported, and
     * concatenate the BufFiles of any subsequent tapes to that. Hence don't
     * create a BufFile here. Things are simpler for the worker case and the
     * serial case, though.  They are generally very similar -- workers use a
     * shared fileset, whereas serial sorts use a conventional serial BufFile.
     */
    if !fileset.is_null() && worker == -1 {
        (*lts).pfile = std::ptr::null_mut();
    } else if !fileset.is_null() {
        let mut filename: [c_char; MAXPGPATH] = [0; MAXPGPATH];

        pg_itoa(worker as crate::c::int16, filename.as_mut_ptr());
        (*lts).pfile = BufFileCreateFileSet(&raw mut (*fileset).fs, filename.as_ptr());
    } else {
        (*lts).pfile = BufFileCreateTemp(false);
    }

    lts
}

/*
 * Claim ownership of a logical tape from an existing shared BufFile.
 *
 * Caller should be leader process.  Though tapes are marked as frozen in
 * workers, they are not frozen when opened within leader, since unfrozen tapes
 * use a larger read buffer. (Frozen tapes have smaller read buffer, optimized
 * for random access.)
 */
pub unsafe fn LogicalTapeImport(
    lts: *mut LogicalTapeSet,
    worker: c_int,
    shared: *mut TapeShare,
) -> *mut LogicalTape {
    let lt: *mut LogicalTape;
    let tapeblocks: int64;
    let mut filename: [c_char; MAXPGPATH] = [0; MAXPGPATH];
    let file: *mut BufFile;
    let filesize: int64;

    lt = ltsCreateTape(lts);

    /*
     * build concatenated view of all buffiles, remembering the block number
     * where each source file begins.
     */
    pg_itoa(worker as crate::c::int16, filename.as_mut_ptr());
    file = BufFileOpenFileSet(
        &raw mut (*(*lts).fileset).fs,
        filename.as_ptr(),
        O_RDONLY,
        false,
    );
    filesize = BufFileSize(file);

    /*
     * Stash first BufFile, and concatenate subsequent BufFiles to that. Store
     * block offset into each tape as we go.
     */
    (*lt).firstBlockNumber = (*shared).firstblocknumber;
    if (*lts).pfile.is_null() {
        (*lts).pfile = file;
        (*lt).offsetBlockNumber = 0;
    } else {
        (*lt).offsetBlockNumber = BufFileAppend((*lts).pfile, file);
    }
    /* Don't allocate more for read buffer than could possibly help */
    (*lt).max_size = Min(MaxAllocSize as int64, filesize) as c_int;
    tapeblocks = filesize / BLCKSZ as int64;

    /*
     * Update # of allocated blocks and # blocks written to reflect the
     * imported BufFile.  Allocated/written blocks include space used by holes
     * left between concatenated BufFiles.  Also track the number of hole
     * blocks so that we can later work backwards to calculate the number of
     * physical blocks for instrumentation.
     */
    (*lts).nHoleBlocks += (*lt).offsetBlockNumber - (*lts).nBlocksAllocated;

    (*lts).nBlocksAllocated = (*lt).offsetBlockNumber + tapeblocks;
    (*lts).nBlocksWritten = (*lts).nBlocksAllocated;

    lt
}

/*
 * Close a logical tape set and release all resources.
 *
 * NOTE: This doesn't close any of the tapes!  You must close them
 * first, or you can let them be destroyed along with the memory context.
 */
pub unsafe fn LogicalTapeSetClose(lts: *mut LogicalTapeSet) {
    BufFileClose((*lts).pfile);
    pfree((*lts).freeBlocks as *mut c_void);
    pfree(lts as *mut c_void);
}

/*
 * Create a logical tape in the given tapeset.
 *
 * The tape is initialized in write state.
 */
pub unsafe fn LogicalTapeCreate(lts: *mut LogicalTapeSet) -> *mut LogicalTape {
    /*
     * The only thing that currently prevents creating new tapes in leader is
     * the fact that BufFiles opened using BufFileOpenShared() are read-only
     * by definition, but that could be changed if it seemed worthwhile.  For
     * now, writing to the leader tape will raise a "Bad file descriptor"
     * error, so tuplesort must avoid writing to the leader tape altogether.
     */
    if !(*lts).fileset.is_null() && (*lts).worker == -1 {
        elog!(ERROR, "cannot create new tapes in leader process");
    }

    ltsCreateTape(lts)
}

unsafe fn ltsCreateTape(lts: *mut LogicalTapeSet) -> *mut LogicalTape {
    let lt: *mut LogicalTape;

    /*
     * Create per-tape struct.  Note we allocate the I/O buffer lazily.
     */
    lt = palloc(std::mem::size_of::<LogicalTape>()) as *mut LogicalTape;
    (*lt).tapeSet = lts;
    (*lt).writing = true;
    (*lt).frozen = false;
    (*lt).dirty = false;
    (*lt).firstBlockNumber = -1;
    (*lt).curBlockNumber = -1;
    (*lt).nextBlockNumber = -1;
    (*lt).offsetBlockNumber = 0;
    (*lt).buffer = std::ptr::null_mut();
    (*lt).buffer_size = 0;
    /* palloc() larger than MaxAllocSize would fail */
    (*lt).max_size = MaxAllocSize as c_int;
    (*lt).pos = 0;
    (*lt).nbytes = 0;
    (*lt).prealloc = std::ptr::null_mut();
    (*lt).nprealloc = 0;
    (*lt).prealloc_size = 0;

    lt
}

/*
 * Close a logical tape.
 *
 * Note: This doesn't return any blocks to the free list!  You must read
 * the tape to the end first, to reuse the space.  In current use, though,
 * we only close tapes after fully reading them.
 */
pub unsafe fn LogicalTapeClose(lt: *mut LogicalTape) {
    if !(*lt).buffer.is_null() {
        pfree((*lt).buffer as *mut c_void);
    }
    pfree(lt as *mut c_void);
}

/*
 * Mark a logical tape set as not needing management of free space anymore.
 *
 * This should be called if the caller does not intend to write any more data
 * into the tape set, but is reading from un-frozen tapes.  Since no more
 * writes are planned, remembering free blocks is no longer useful.  Setting
 * this flag lets us avoid wasting time and space in ltsReleaseBlock(), which
 * is not designed to handle large numbers of free blocks.
 */
pub unsafe fn LogicalTapeSetForgetFreeSpace(lts: *mut LogicalTapeSet) {
    (*lts).forgetFreeSpace = true;
}

/*
 * Write to a logical tape.
 *
 * There are no error returns; we ereport() on failure.
 */
pub unsafe fn LogicalTapeWrite(lt: *mut LogicalTape, mut ptr: *const c_void, mut size: usize) {
    let lts: *mut LogicalTapeSet = (*lt).tapeSet;
    let mut nthistime: usize;

    Assert!((*lt).writing);
    Assert!((*lt).offsetBlockNumber == 0);

    /* Allocate data buffer and first block on first write */
    if (*lt).buffer.is_null() {
        (*lt).buffer = palloc(BLCKSZ) as *mut c_char;
        (*lt).buffer_size = BLCKSZ as c_int;
    }
    if (*lt).curBlockNumber == -1 {
        Assert!((*lt).firstBlockNumber == -1);
        Assert!((*lt).pos == 0);

        (*lt).curBlockNumber = ltsGetBlock(lts, lt);
        (*lt).firstBlockNumber = (*lt).curBlockNumber;

        (*TapeBlockGetTrailer((*lt).buffer as *mut c_void)).prev = -1;
    }

    Assert!((*lt).buffer_size == BLCKSZ as c_int);
    while size > 0 {
        if (*lt).pos >= TapeBlockPayloadSize as c_int {
            /* Buffer full, dump it out */
            let nextBlockNumber: int64;

            if !(*lt).dirty {
                /* Hmm, went directly from reading to writing? */
                elog!(ERROR, "invalid logtape state: should be dirty");
            }

            /*
             * First allocate the next block, so that we can store it in the
             * 'next' pointer of this block.
             */
            nextBlockNumber = ltsGetBlock((*lt).tapeSet, lt);

            /* set the next-pointer and dump the current block. */
            (*TapeBlockGetTrailer((*lt).buffer as *mut c_void)).next = nextBlockNumber;
            ltsWriteBlock((*lt).tapeSet, (*lt).curBlockNumber, (*lt).buffer as *const c_void);

            /* initialize the prev-pointer of the next block */
            (*TapeBlockGetTrailer((*lt).buffer as *mut c_void)).prev = (*lt).curBlockNumber;
            (*lt).curBlockNumber = nextBlockNumber;
            (*lt).pos = 0;
            (*lt).nbytes = 0;
        }

        nthistime = TapeBlockPayloadSize - (*lt).pos as usize;
        if nthistime > size {
            nthistime = size;
        }
        Assert!(nthistime > 0);

        std::ptr::copy_nonoverlapping(
            ptr as *const c_char,
            (*lt).buffer.add((*lt).pos as usize),
            nthistime,
        );

        (*lt).dirty = true;
        (*lt).pos += nthistime as c_int;
        if (*lt).nbytes < (*lt).pos {
            (*lt).nbytes = (*lt).pos;
        }
        ptr = (ptr as *const c_char).add(nthistime) as *const c_void;
        size -= nthistime;
    }
}

/*
 * Rewind logical tape and switch from writing to reading.
 *
 * The tape must currently be in writing state, or "frozen" in read state.
 *
 * 'buffer_size' specifies how much memory to use for the read buffer.
 * Regardless of the argument, the actual amount of memory used is between
 * BLCKSZ and MaxAllocSize, and is a multiple of BLCKSZ.  The given value is
 * rounded down and truncated to fit those constraints, if necessary.  If the
 * tape is frozen, the 'buffer_size' argument is ignored, and a small BLCKSZ
 * byte buffer is used.
 */
pub unsafe fn LogicalTapeRewindForRead(lt: *mut LogicalTape, mut buffer_size: usize) {
    let lts: *mut LogicalTapeSet = (*lt).tapeSet;

    /*
     * Round and cap buffer_size if needed.
     */
    if (*lt).frozen {
        buffer_size = BLCKSZ;
    } else {
        /* need at least one block */
        if buffer_size < BLCKSZ {
            buffer_size = BLCKSZ;
        }

        /* palloc() larger than max_size is unlikely to be helpful */
        if buffer_size > (*lt).max_size as usize {
            buffer_size = (*lt).max_size as usize;
        }

        /* round down to BLCKSZ boundary */
        buffer_size -= buffer_size % BLCKSZ;
    }

    if (*lt).writing {
        /*
         * Completion of a write phase.  Flush last partial data block, and
         * rewind for normal (destructive) read.
         */
        if (*lt).dirty {
            /*
             * As long as we've filled the buffer at least once, its contents
             * are entirely defined from valgrind's point of view, even though
             * contents beyond the current end point may be stale.  But it's
             * possible - at least in the case of a parallel sort - to sort
             * such small amount of data that we do not fill the buffer even
             * once.  Tell valgrind that its contents are defined, so it
             * doesn't bleat.
             */
            VALGRIND_MAKE_MEM_DEFINED(
                (*lt).buffer.add((*lt).nbytes as usize) as *mut c_void,
                ((*lt).buffer_size - (*lt).nbytes) as usize,
            );

            TapeBlockSetNBytes((*lt).buffer as *mut c_void, (*lt).nbytes as int64);
            ltsWriteBlock((*lt).tapeSet, (*lt).curBlockNumber, (*lt).buffer as *const c_void);
        }
        (*lt).writing = false;
    } else {
        /*
         * This is only OK if tape is frozen; we rewind for (another) read
         * pass.
         */
        Assert!((*lt).frozen);
    }

    if !(*lt).buffer.is_null() {
        pfree((*lt).buffer as *mut c_void);
    }

    /* the buffer is lazily allocated, but set the size here */
    (*lt).buffer = std::ptr::null_mut();
    (*lt).buffer_size = buffer_size as c_int;

    /* free the preallocation list, and return unused block numbers */
    if !(*lt).prealloc.is_null() {
        let mut i = (*lt).nprealloc;
        while i > 0 {
            ltsReleaseBlock(lts, *(*lt).prealloc.add((i - 1) as usize));
            i -= 1;
        }
        pfree((*lt).prealloc as *mut c_void);
        (*lt).prealloc = std::ptr::null_mut();
        (*lt).nprealloc = 0;
        (*lt).prealloc_size = 0;
    }
}

/*
 * Read from a logical tape.
 *
 * Early EOF is indicated by return value less than #bytes requested.
 */
pub unsafe fn LogicalTapeRead(lt: *mut LogicalTape, mut ptr: *mut c_void, mut size: usize) -> usize {
    let mut nread: usize = 0;
    let mut nthistime: usize;

    Assert!(!(*lt).writing);

    if (*lt).buffer.is_null() {
        ltsInitReadBuffer(lt);
    }

    while size > 0 {
        if (*lt).pos >= (*lt).nbytes {
            /* Try to load more data into buffer. */
            if !ltsReadFillBuffer(lt) {
                break; /* EOF */
            }
        }

        nthistime = ((*lt).nbytes - (*lt).pos) as usize;
        if nthistime > size {
            nthistime = size;
        }
        Assert!(nthistime > 0);

        std::ptr::copy_nonoverlapping(
            (*lt).buffer.add((*lt).pos as usize),
            ptr as *mut c_char,
            nthistime,
        );

        (*lt).pos += nthistime as c_int;
        ptr = (ptr as *mut c_char).add(nthistime) as *mut c_void;
        size -= nthistime;
        nread += nthistime;
    }

    nread
}

/*
 * "Freeze" the contents of a tape so that it can be read multiple times
 * and/or read backwards.  Once a tape is frozen, its contents will not
 * be released until the LogicalTapeSet is destroyed.  This is expected
 * to be used only for the final output pass of a merge.
 *
 * This *must* be called just at the end of a write pass, before the
 * tape is rewound (after rewind is too late!).  It performs a rewind
 * and switch to read mode "for free".  An immediately following rewind-
 * for-read call is OK but not necessary.
 *
 * share output argument is set with details of storage used for tape after
 * freezing, which may be passed to LogicalTapeSetCreate within leader
 * process later.  This metadata is only of interest to worker callers
 * freezing their final output for leader (single materialized tape).
 * Serial sorts should set share to NULL.
 */
pub unsafe fn LogicalTapeFreeze(lt: *mut LogicalTape, share: *mut TapeShare) {
    let lts: *mut LogicalTapeSet = (*lt).tapeSet;

    Assert!((*lt).writing);
    Assert!((*lt).offsetBlockNumber == 0);

    /*
     * Completion of a write phase.  Flush last partial data block, and rewind
     * for nondestructive read.
     */
    if (*lt).dirty {
        /*
         * As long as we've filled the buffer at least once, its contents are
         * entirely defined from valgrind's point of view, even though
         * contents beyond the current end point may be stale.  But it's
         * possible - at least in the case of a parallel sort - to sort such
         * small amount of data that we do not fill the buffer even once. Tell
         * valgrind that its contents are defined, so it doesn't bleat.
         */
        VALGRIND_MAKE_MEM_DEFINED(
            (*lt).buffer.add((*lt).nbytes as usize) as *mut c_void,
            ((*lt).buffer_size - (*lt).nbytes) as usize,
        );

        TapeBlockSetNBytes((*lt).buffer as *mut c_void, (*lt).nbytes as int64);
        ltsWriteBlock((*lt).tapeSet, (*lt).curBlockNumber, (*lt).buffer as *const c_void);
    }
    (*lt).writing = false;
    (*lt).frozen = true;

    /*
     * The seek and backspace functions assume a single block read buffer.
     * That's OK with current usage.  A larger buffer is helpful to make the
     * read pattern of the backing file look more sequential to the OS, when
     * we're reading from multiple tapes.  But at the end of a sort, when a
     * tape is frozen, we only read from a single tape anyway.
     */
    if (*lt).buffer.is_null() || (*lt).buffer_size != BLCKSZ as c_int {
        if !(*lt).buffer.is_null() {
            pfree((*lt).buffer as *mut c_void);
        }
        (*lt).buffer = palloc(BLCKSZ) as *mut c_char;
        (*lt).buffer_size = BLCKSZ as c_int;
    }

    /* Read the first block, or reset if tape is empty */
    (*lt).curBlockNumber = (*lt).firstBlockNumber;
    (*lt).pos = 0;
    (*lt).nbytes = 0;

    if (*lt).firstBlockNumber == -1 {
        (*lt).nextBlockNumber = -1;
    }
    ltsReadBlock((*lt).tapeSet, (*lt).curBlockNumber, (*lt).buffer as *mut c_void);
    if TapeBlockIsLast((*lt).buffer as *mut c_void) {
        (*lt).nextBlockNumber = -1;
    } else {
        (*lt).nextBlockNumber = (*TapeBlockGetTrailer((*lt).buffer as *mut c_void)).next;
    }
    (*lt).nbytes = TapeBlockGetNBytes((*lt).buffer as *mut c_void) as c_int;

    /* Handle extra steps when caller is to share its tapeset */
    if !share.is_null() {
        BufFileExportFileSet((*lts).pfile);
        (*share).firstblocknumber = (*lt).firstBlockNumber;
    }
}

/*
 * Backspace the tape a given number of bytes.  (We also support a more
 * general seek interface, see below.)
 *
 * *Only* a frozen-for-read tape can be backed up; we don't support
 * random access during write, and an unfrozen read tape may have
 * already discarded the desired data!
 *
 * Returns the number of bytes backed up.  It can be less than the
 * requested amount, if there isn't that much data before the current
 * position.  The tape is positioned to the beginning of the tape in
 * that case.
 */
pub unsafe fn LogicalTapeBackspace(lt: *mut LogicalTape, size: usize) -> usize {
    let mut seekpos: usize = 0;

    Assert!((*lt).frozen);
    Assert!((*lt).buffer_size == BLCKSZ as c_int);

    if (*lt).buffer.is_null() {
        ltsInitReadBuffer(lt);
    }

    /*
     * Easy case for seek within current block.
     */
    if size <= (*lt).pos as usize {
        (*lt).pos -= size as c_int;
        return size;
    }

    /*
     * Not-so-easy case, have to walk back the chain of blocks.  This
     * implementation would be pretty inefficient for long seeks, but we
     * really aren't doing that (a seek over one tuple is typical).
     */
    seekpos = (*lt).pos as usize; /* part within this block */
    while size > seekpos {
        let prev: int64 = (*TapeBlockGetTrailer((*lt).buffer as *mut c_void)).prev;

        if prev == -1 {
            /* Tried to back up beyond the beginning of tape. */
            if (*lt).curBlockNumber != (*lt).firstBlockNumber {
                elog!(ERROR, "unexpected end of tape");
            }
            (*lt).pos = 0;
            return seekpos;
        }

        ltsReadBlock((*lt).tapeSet, prev, (*lt).buffer as *mut c_void);

        if (*TapeBlockGetTrailer((*lt).buffer as *mut c_void)).next != (*lt).curBlockNumber {
            elog!(
                ERROR,
                "broken tape, next of block {} is {}, expected {}",
                prev,
                (*TapeBlockGetTrailer((*lt).buffer as *mut c_void)).next,
                (*lt).curBlockNumber
            );
        }

        (*lt).nbytes = TapeBlockPayloadSize as c_int;
        (*lt).curBlockNumber = prev;
        (*lt).nextBlockNumber = (*TapeBlockGetTrailer((*lt).buffer as *mut c_void)).next;

        seekpos += TapeBlockPayloadSize;
    }

    /*
     * 'seekpos' can now be greater than 'size', because it points to the
     * beginning the target block.  The difference is the position within the
     * page.
     */
    (*lt).pos = (seekpos - size) as c_int;
    size
}

/*
 * Seek to an arbitrary position in a logical tape.
 *
 * *Only* a frozen-for-read tape can be seeked.
 *
 * Must be called with a block/offset previously returned by
 * LogicalTapeTell().
 */
pub unsafe fn LogicalTapeSeek(lt: *mut LogicalTape, blocknum: int64, offset: c_int) {
    Assert!((*lt).frozen);
    Assert!(offset >= 0 && offset <= TapeBlockPayloadSize as c_int);
    Assert!((*lt).buffer_size == BLCKSZ as c_int);

    if (*lt).buffer.is_null() {
        ltsInitReadBuffer(lt);
    }

    if blocknum != (*lt).curBlockNumber {
        ltsReadBlock((*lt).tapeSet, blocknum, (*lt).buffer as *mut c_void);
        (*lt).curBlockNumber = blocknum;
        (*lt).nbytes = TapeBlockPayloadSize as c_int;
        (*lt).nextBlockNumber = (*TapeBlockGetTrailer((*lt).buffer as *mut c_void)).next;
    }

    if offset > (*lt).nbytes {
        elog!(ERROR, "invalid tape seek position");
    }
    (*lt).pos = offset;
}

/*
 * Obtain current position in a form suitable for a later LogicalTapeSeek.
 *
 * NOTE: it'd be OK to do this during write phase with intention of using
 * the position for a seek after freezing.  Not clear if anyone needs that.
 */
pub unsafe fn LogicalTapeTell(lt: *mut LogicalTape, blocknum: *mut int64, offset: *mut c_int) {
    if (*lt).buffer.is_null() {
        ltsInitReadBuffer(lt);
    }

    Assert!((*lt).offsetBlockNumber == 0);

    /* With a larger buffer, 'pos' wouldn't be the same as offset within page */
    Assert!((*lt).buffer_size == BLCKSZ as c_int);

    *blocknum = (*lt).curBlockNumber;
    *offset = (*lt).pos;
}

/*
 * Obtain total disk space currently used by a LogicalTapeSet, in blocks. Does
 * not account for open write buffer, if any.
 */
pub unsafe fn LogicalTapeSetBlocks(lts: *mut LogicalTapeSet) -> int64 {
    (*lts).nBlocksWritten - (*lts).nHoleBlocks
}
