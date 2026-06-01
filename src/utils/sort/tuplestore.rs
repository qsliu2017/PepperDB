//! tuplestore.rs
//!   Generalized routines for temporary tuple storage.
//!
//! Translated 1:1 from postgres/src/backend/utils/sort/tuplestore.c
//!
//! This module handles temporary storage of tuples for purposes such
//! as Materialize nodes, hashjoin batch files, etc.  It is essentially
//! a dumbed-down version of tuplesort.c; it does no sorting of tuples
//! but can only store and regurgitate a sequence of tuples.  However,
//! because no sort is required, it is allowed to start reading the sequence
//! before it has all been written.  This is particularly useful for cursors,
//! because it allows random access within the already-scanned portion of
//! a query without having to process the underlying scan to completion.
//! Also, it is possible to support multiple independent read pointers.
//!
//! A temporary file is used to handle the data if it exceeds the
//! space limit specified by the caller.
//!
//! The (approximate) amount of memory allowed to the tuplestore is specified
//! in kilobytes by the caller.  We absorb tuples and simply store them in an
//! in-memory array as long as we haven't exceeded maxKBytes.  If we do exceed
//! maxKBytes, we dump all the tuples into a temp file and then read from that
//! when needed.
//!
//! Upon creation, a tuplestore supports a single read pointer, numbered 0.
//! Additional read pointers can be created using tuplestore_alloc_read_pointer.
//! Mark/restore behavior is supported by copying read pointers.
//!
//! When the caller requests backward-scan capability, we write the temp file
//! in a format that allows either forward or backward scan.  Otherwise, only
//! forward scan is allowed.  A request for backward scan must be made before
//! putting any tuples into the tuplestore.  Rewind is normally allowed but
//! can be turned off via tuplestore_set_eflags; turning off rewind for all
//! read pointers enables truncation of the tuplestore at the oldest read point
//! for minimal memory usage.  (The caller must explicitly call tuplestore_trim
//! at appropriate times for truncation to actually happen.)
//!
//! Note: in TSS_WRITEFILE state, the temp file's seek position is the
//! current write position, and the write-position variables in the tuplestore
//! aren't kept up to date.  Similarly, in TSS_READFILE state the temp file's
//! seek position is the active read pointer's position, and that read pointer
//! isn't kept up to date.  We update the appropriate variables using ftell()
//! before switching to the other state or activating a different read pointer.
//!
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!   src/backend/utils/sort/tuplestore.c

#![allow(unused_variables)]
#![allow(dead_code)]
#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]

use crate::prelude::*;

use crate::access::common::heaptuple::{
    heap_copy_minimal_tuple, heap_form_minimal_tuple, heap_free_minimal_tuple,
    minimal_tuple_from_heap_tuple,
};
use crate::access::htup_details::{HeapTuple, MinimalTuple, MINIMAL_TUPLE_DATA_OFFSET};
use crate::c::{int64, Max, Min, Size};
use crate::commands::tablespace::PrepareTempTablespaces;
use crate::executor::execTuples::ExecStoreMinimalTuple;
use crate::executor::executor::{EXEC_FLAG_BACKWARD, EXEC_FLAG_REWIND};
use crate::executor::tuptable::{ExecClearTuple, ExecCopySlotMinimalTuple};
use crate::miscadmin::CHECK_FOR_INTERRUPTS;
use crate::nodes::execnodes::TupleTableSlot;
use crate::storage::file::buffile::{
    BufFile, BufFileClose, BufFileCreateTemp, BufFileReadExact, BufFileReadMaybeEOF, BufFileSeek,
    BufFileSize, BufFileTell, BufFileWrite,
};
use crate::access::common::tupdesc::TupleDesc;
use crate::storage::file::fd::off_t;
use crate::utils::mmgr::generation::GenerationContextCreate;
use crate::utils::mmgr::mcxt::{GetMemoryChunkSpace, repalloc_huge};
use crate::utils::resowner::resowner::{CurrentResourceOwner, ResourceOwner};
use core::ffi::{c_char, c_int, c_void};

// SEEK constants (stdio.h / unistd.h)
const SEEK_SET: c_int = 0;
const SEEK_CUR: c_int = 1;

// `ALLOCSET_SEPARATE_THRESHOLD` (memutils.h): the threshold above which an
// allocation is treated as a separate (dedicated) chunk by aset.c.
const ALLOCSET_SEPARATE_THRESHOLD: Size = 8192;

/// `MaxAllocHugeSize` (memutils.h). STUB.
// TODO(pg-port): port utils/memutils.h MaxAllocHugeSize properly; it is
// SIZE_MAX / 2 in the C source.
const MaxAllocHugeSize: Size = (Size::MAX) / 2;

/*
 * Possible states of a Tuplestore object.  These denote the states that
 * persist between calls of Tuplestore routines.
 */
#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(C)]
enum TupStoreStatus {
    TSS_INMEM,     /* Tuples still fit in memory */
    TSS_WRITEFILE, /* Writing to temp file */
    TSS_READFILE,  /* Reading from temp file */
}
use TupStoreStatus::*;

/*
 * State for a single read pointer.  If we are in state INMEM then all the
 * read pointers' "current" fields denote the read positions.  In state
 * WRITEFILE, the file/offset fields denote the read positions.  In state
 * READFILE, inactive read pointers have valid file/offset, but the active
 * read pointer implicitly has position equal to the temp file's seek position.
 *
 * Special case: if eof_reached is true, then the pointer's read position is
 * implicitly equal to the write position, and current/file/offset aren't
 * maintained.  This way we need not update all the read pointers each time
 * we write.
 */
#[derive(Clone, Copy)]
#[repr(C)]
struct TSReadPointer {
    eflags: c_int,      /* capability flags */
    eof_reached: bool,  /* read has reached EOF */
    current: c_int,     /* next array index to read */
    file: c_int,        /* temp file# */
    offset: off_t,      /* byte offset in file */
}

/*
 * Private state of a Tuplestore operation.
 */
#[repr(C)]
pub struct Tuplestorestate {
    status: TupStoreStatus, /* enumerated value as shown above */
    eflags: c_int,          /* capability flags (OR of pointers' flags) */
    backward: bool,         /* store extra length words in file? */
    interXact: bool,        /* keep open through transactions? */
    truncated: bool,        /* tuplestore_trim has removed tuples? */
    usedDisk: bool,         /* used by tuplestore_get_stats() */
    maxSpace: int64,        /* used by tuplestore_get_stats() */
    availMem: int64,        /* remaining memory available, in bytes */
    allowedMem: int64,      /* total memory allowed, in bytes */
    tuples: int64,          /* number of tuples added */
    myfile: *mut BufFile,   /* underlying file, or NULL if none */
    context: MemoryContext, /* memory context for holding tuples */
    resowner: ResourceOwner, /* resowner for holding temp files */

    /*
     * These function pointers decouple the routines that must know what kind
     * of tuple we are handling from the routines that don't need to know it.
     * They are set up by the tuplestore_begin_xxx routines.
     *
     * (Although tuplestore.c currently only supports heap tuples, I've copied
     * this part of tuplesort.c so that extension to other kinds of objects
     * will be easy if it's ever needed.)
     *
     * Function to copy a supplied input tuple into palloc'd space. (NB: we
     * assume that a single pfree() is enough to release the tuple later, so
     * the representation must be "flat" in one palloc chunk.) state->availMem
     * must be decreased by the amount of space used.
     */
    copytup: Option<unsafe fn(state: *mut Tuplestorestate, tup: *mut c_void) -> *mut c_void>,

    /*
     * Function to write a stored tuple onto tape.  The representation of the
     * tuple on tape need not be the same as it is in memory; requirements on
     * the tape representation are given below.  After writing the tuple,
     * pfree() it, and increase state->availMem by the amount of memory space
     * thereby released.
     */
    writetup: Option<unsafe fn(state: *mut Tuplestorestate, tup: *mut c_void)>,

    /*
     * Function to read a stored tuple from tape back into memory. 'len' is
     * the already-read length of the stored tuple.  Create and return a
     * palloc'd copy, and decrease state->availMem by the amount of memory
     * space consumed.
     */
    readtup: Option<unsafe fn(state: *mut Tuplestorestate, len: c_uint) -> *mut c_void>,

    /*
     * This array holds pointers to tuples in memory if we are in state INMEM.
     * In states WRITEFILE and READFILE it's not used.
     *
     * When memtupdeleted > 0, the first memtupdeleted pointers are already
     * released due to a tuplestore_trim() operation, but we haven't expended
     * the effort to slide the remaining pointers down.  These unused pointers
     * are set to NULL to catch any invalid accesses.  Note that memtupcount
     * includes the deleted pointers.
     */
    memtuples: *mut *mut c_void, /* array of pointers to palloc'd tuples */
    memtupdeleted: c_int,        /* the first N slots are currently unused */
    memtupcount: c_int,          /* number of tuples currently present */
    memtupsize: c_int,           /* allocated length of memtuples array */
    growmemtuples: bool,         /* memtuples' growth still underway? */

    /*
     * These variables are used to keep track of the current positions.
     *
     * In state WRITEFILE, the current file seek position is the write point;
     * in state READFILE, the write position is remembered in writepos_xxx.
     * (The write position is the same as EOF, but since BufFileSeek doesn't
     * currently implement SEEK_END, we have to remember it explicitly.)
     */
    readptrs: *mut TSReadPointer, /* array of read pointers */
    activeptr: c_int,             /* index of the active read pointer */
    readptrcount: c_int,          /* number of pointers currently valid */
    readptrsize: c_int,           /* allocated length of readptrs array */

    writepos_file: c_int,     /* file# (valid if READFILE state) */
    writepos_offset: off_t,   /* offset (valid if READFILE state) */
}

// #define COPYTUP(state,tup)	((*(state)->copytup) (state, tup))
unsafe fn COPYTUP(state: *mut Tuplestorestate, tup: *mut c_void) -> *mut c_void {
    ((*state).copytup.unwrap())(state, tup)
}
// #define WRITETUP(state,tup) ((*(state)->writetup) (state, tup))
unsafe fn WRITETUP(state: *mut Tuplestorestate, tup: *mut c_void) {
    ((*state).writetup.unwrap())(state, tup)
}
// #define READTUP(state,len)	((*(state)->readtup) (state, len))
unsafe fn READTUP(state: *mut Tuplestorestate, len: c_uint) -> *mut c_void {
    ((*state).readtup.unwrap())(state, len)
}
// #define LACKMEM(state)		((state)->availMem < 0)
unsafe fn LACKMEM(state: *mut Tuplestorestate) -> bool {
    (*state).availMem < 0
}
// #define USEMEM(state,amt)	((state)->availMem -= (amt))
unsafe fn USEMEM(state: *mut Tuplestorestate, amt: int64) {
    (*state).availMem -= amt;
}
// #define FREEMEM(state,amt)	((state)->availMem += (amt))
unsafe fn FREEMEM(state: *mut Tuplestorestate, amt: int64) {
    (*state).availMem += amt;
}

/*--------------------
 *
 * NOTES about on-tape representation of tuples:
 *
 * We require the first "unsigned int" of a stored tuple to be the total size
 * on-tape of the tuple, including itself (so it is never zero).
 * The remainder of the stored tuple
 * may or may not match the in-memory representation of the tuple ---
 * any conversion needed is the job of the writetup and readtup routines.
 *
 * If state->backward is true, then the stored representation of
 * the tuple must be followed by another "unsigned int" that is a copy of the
 * length --- so the total tape space used is actually sizeof(unsigned int)
 * more than the stored length value.  This allows read-backwards.  When
 * state->backward is not set, the write/read routines may omit the extra
 * length word.
 *
 * writetup is expected to write both length words as well as the tuple
 * data.  When readtup is called, the tape is positioned just after the
 * front length word; readtup must read the tuple data and advance past
 * the back length word (if present).
 *
 * The write/read routines can make use of the tuple description data
 * stored in the Tuplestorestate record, if needed. They are also expected
 * to adjust state->availMem by the amount of memory space (not tape space!)
 * released or consumed.  There is no error return from either writetup
 * or readtup; they should ereport() on failure.
 *
 *
 * NOTES about memory consumption calculations:
 *
 * We count space allocated for tuples against the maxKBytes limit,
 * plus the space used by the variable-size array memtuples.
 * Fixed-size space (primarily the BufFile I/O buffer) is not counted.
 * We don't worry about the size of the read pointer array, either.
 *
 * Note that we count actual space used (as shown by GetMemoryChunkSpace)
 * rather than the originally-requested size.  This is important since
 * palloc can add substantial overhead.  It's not a complete answer since
 * we won't count any wasted space in palloc allocation blocks, but it's
 * a lot better than what we were doing before 7.3.
 *
 *--------------------
 */

/*
 *		tuplestore_begin_xxx
 *
 * Initialize for a tuple store operation.
 */
unsafe fn tuplestore_begin_common(
    eflags: c_int,
    interXact: bool,
    maxKBytes: c_int,
) -> *mut Tuplestorestate {
    let state: *mut Tuplestorestate;

    state = palloc0(core::mem::size_of::<Tuplestorestate>()) as *mut Tuplestorestate;

    (*state).status = TSS_INMEM;
    (*state).eflags = eflags;
    (*state).interXact = interXact;
    (*state).truncated = false;
    (*state).usedDisk = false;
    (*state).maxSpace = 0;
    (*state).allowedMem = maxKBytes as int64 * 1024 as int64;
    (*state).availMem = (*state).allowedMem;
    (*state).myfile = null_mut();

    /*
     * The palloc/pfree pattern for tuple memory is in a FIFO pattern.  A
     * generation context is perfectly suited for this.
     */
    let (min_ctx_size, init_blk_size, max_blk_size) = ALLOCSET_DEFAULT_SIZES;
    (*state).context = GenerationContextCreate(
        CurrentMemoryContext,
        c"tuplestore tuples".as_ptr(),
        min_ctx_size,
        init_blk_size,
        max_blk_size,
    );
    (*state).resowner = CurrentResourceOwner;

    (*state).memtupdeleted = 0;
    (*state).memtupcount = 0;
    (*state).tuples = 0;

    /*
     * Initial size of array must be more than ALLOCSET_SEPARATE_THRESHOLD;
     * see comments in grow_memtuples().
     */
    (*state).memtupsize = Max(
        16384 / core::mem::size_of::<*mut c_void>() as c_int,
        ALLOCSET_SEPARATE_THRESHOLD as c_int / core::mem::size_of::<*mut c_void>() as c_int + 1,
    );

    (*state).growmemtuples = true;
    (*state).memtuples =
        palloc((*state).memtupsize as Size * core::mem::size_of::<*mut c_void>())
            as *mut *mut c_void;

    USEMEM(state, GetMemoryChunkSpace((*state).memtuples as *mut c_void) as int64);

    (*state).activeptr = 0;
    (*state).readptrcount = 1;
    (*state).readptrsize = 8; /* arbitrary */
    (*state).readptrs = palloc(
        (*state).readptrsize as Size * core::mem::size_of::<TSReadPointer>(),
    ) as *mut TSReadPointer;

    (*(*state).readptrs.add(0)).eflags = eflags;
    (*(*state).readptrs.add(0)).eof_reached = false;
    (*(*state).readptrs.add(0)).current = 0;

    state
}

/*
 * tuplestore_begin_heap
 *
 * Create a new tuplestore; other types of tuple stores (other than
 * "heap" tuple stores, for heap tuples) are possible, but not presently
 * implemented.
 *
 * randomAccess: if true, both forward and backward accesses to the
 * tuple store are allowed.
 *
 * interXact: if true, the files used for on-disk storage persist beyond the
 * end of the current transaction.  NOTE: It's the caller's responsibility to
 * create such a tuplestore in a memory context and resource owner that will
 * also survive transaction boundaries, and to ensure the tuplestore is closed
 * when it's no longer wanted.
 *
 * maxKBytes: how much data to store in memory (any data beyond this
 * amount is paged to disk).  When in doubt, use work_mem.
 */
pub unsafe fn tuplestore_begin_heap(
    randomAccess: bool,
    interXact: bool,
    maxKBytes: c_int,
) -> *mut Tuplestorestate {
    let state: *mut Tuplestorestate;
    let eflags: c_int;

    /*
     * This interpretation of the meaning of randomAccess is compatible with
     * the pre-8.3 behavior of tuplestores.
     */
    eflags = if randomAccess {
        EXEC_FLAG_BACKWARD | EXEC_FLAG_REWIND
    } else {
        EXEC_FLAG_REWIND
    };

    state = tuplestore_begin_common(eflags, interXact, maxKBytes);

    (*state).copytup = Some(copytup_heap);
    (*state).writetup = Some(writetup_heap);
    (*state).readtup = Some(readtup_heap);

    state
}

/*
 * tuplestore_set_eflags
 *
 * Set the capability flags for read pointer 0 at a finer grain than is
 * allowed by tuplestore_begin_xxx.  This must be called before inserting
 * any data into the tuplestore.
 *
 * eflags is a bitmask following the meanings used for executor node
 * startup flags (see executor.h).  tuplestore pays attention to these bits:
 *		EXEC_FLAG_REWIND		need rewind to start
 *		EXEC_FLAG_BACKWARD		need backward fetch
 * If tuplestore_set_eflags is not called, REWIND is allowed, and BACKWARD
 * is set per "randomAccess" in the tuplestore_begin_xxx call.
 *
 * NOTE: setting BACKWARD without REWIND means the pointer can read backwards,
 * but not further than the truncation point (the furthest-back read pointer
 * position at the time of the last tuplestore_trim call).
 */
pub unsafe fn tuplestore_set_eflags(state: *mut Tuplestorestate, eflags: c_int) {
    let mut eflags = eflags;
    let mut i: c_int;

    if (*state).status != TSS_INMEM || (*state).memtupcount != 0 {
        elog!(ERROR, "too late to call tuplestore_set_eflags");
    }

    (*(*state).readptrs.add(0)).eflags = eflags;
    i = 1;
    while i < (*state).readptrcount {
        eflags |= (*(*state).readptrs.add(i as usize)).eflags;
        i += 1;
    }
    (*state).eflags = eflags;
}

/*
 * tuplestore_alloc_read_pointer - allocate another read pointer.
 *
 * Returns the pointer's index.
 *
 * The new pointer initially copies the position of read pointer 0.
 * It can have its own eflags, but if any data has been inserted into
 * the tuplestore, these eflags must not represent an increase in
 * requirements.
 */
pub unsafe fn tuplestore_alloc_read_pointer(state: *mut Tuplestorestate, eflags: c_int) -> c_int {
    /* Check for possible increase of requirements */
    if (*state).status != TSS_INMEM || (*state).memtupcount != 0 {
        if ((*state).eflags | eflags) != (*state).eflags {
            elog!(ERROR, "too late to require new tuplestore eflags");
        }
    }

    /* Make room for another read pointer if needed */
    if (*state).readptrcount >= (*state).readptrsize {
        let newcnt: c_int = (*state).readptrsize * 2;

        (*state).readptrs = repalloc(
            (*state).readptrs as *mut c_void,
            newcnt as Size * core::mem::size_of::<TSReadPointer>(),
        ) as *mut TSReadPointer;
        (*state).readptrsize = newcnt;
    }

    /* And set it up */
    *(*state).readptrs.add((*state).readptrcount as usize) = *(*state).readptrs.add(0);
    (*(*state).readptrs.add((*state).readptrcount as usize)).eflags = eflags;

    (*state).eflags |= eflags;

    let ret = (*state).readptrcount;
    (*state).readptrcount += 1;
    ret
}

/*
 * tuplestore_clear
 *
 *	Delete all the contents of a tuplestore, and reset its read pointers
 *	to the start.
 */
pub unsafe fn tuplestore_clear(state: *mut Tuplestorestate) {
    let mut i: c_int;
    let mut readptr: *mut TSReadPointer;

    /* update the maxSpace before doing any USEMEM/FREEMEM adjustments */
    tuplestore_updatemax(state);

    if !(*state).myfile.is_null() {
        BufFileClose((*state).myfile);
    }
    (*state).myfile = null_mut();

    // #ifdef USE_ASSERT_CHECKING
    #[cfg(debug_assertions)]
    {
        let mut availMem: int64 = (*state).availMem;

        /*
         * Below, we reset the memory context for storing tuples.  To save
         * from having to always call GetMemoryChunkSpace() on all stored
         * tuples, we adjust the availMem to forget all the tuples and just
         * recall USEMEM for the space used by the memtuples array.  Here we
         * just Assert that's correct and the memory tracking hasn't gone
         * wrong anywhere.
         */
        i = (*state).memtupdeleted;
        while i < (*state).memtupcount {
            availMem += GetMemoryChunkSpace(*(*state).memtuples.add(i as usize)) as int64;
            i += 1;
        }

        availMem += GetMemoryChunkSpace((*state).memtuples as *mut c_void) as int64;

        Assert!(availMem == (*state).allowedMem);
    }

    /* clear the memory consumed by the memory tuples */
    MemoryContextReset((*state).context);

    /*
     * Zero the used memory and re-consume the space for the memtuples array.
     * This saves having to FREEMEM for each stored tuple.
     */
    (*state).availMem = (*state).allowedMem;
    USEMEM(state, GetMemoryChunkSpace((*state).memtuples as *mut c_void) as int64);

    (*state).status = TSS_INMEM;
    (*state).truncated = false;
    (*state).memtupdeleted = 0;
    (*state).memtupcount = 0;
    (*state).tuples = 0;
    readptr = (*state).readptrs;
    i = 0;
    while i < (*state).readptrcount {
        (*readptr).eof_reached = false;
        (*readptr).current = 0;
        readptr = readptr.add(1);
        i += 1;
    }
}

/*
 * tuplestore_end
 *
 *	Release resources and clean up.
 */
pub unsafe fn tuplestore_end(state: *mut Tuplestorestate) {
    if !(*state).myfile.is_null() {
        BufFileClose((*state).myfile);
    }

    MemoryContextDelete((*state).context);
    pfree((*state).memtuples as *mut c_void);
    pfree((*state).readptrs as *mut c_void);
    pfree(state as *mut c_void);
}

/*
 * tuplestore_select_read_pointer - make the specified read pointer active
 */
pub unsafe fn tuplestore_select_read_pointer(state: *mut Tuplestorestate, ptr: c_int) {
    let readptr: *mut TSReadPointer;
    let oldptr: *mut TSReadPointer;

    Assert!(ptr >= 0 && ptr < (*state).readptrcount);

    /* No work if already active */
    if ptr == (*state).activeptr {
        return;
    }

    readptr = &mut *(*state).readptrs.add(ptr as usize);
    oldptr = &mut *(*state).readptrs.add((*state).activeptr as usize);

    match (*state).status {
        TSS_INMEM | TSS_WRITEFILE => {
            /* no work */
        }
        TSS_READFILE => {
            /*
             * First, save the current read position in the pointer about to
             * become inactive.
             */
            if !(*oldptr).eof_reached {
                BufFileTell((*state).myfile, &mut (*oldptr).file, &mut (*oldptr).offset);
            }

            /*
             * We have to make the temp file's seek position equal to the
             * logical position of the new read pointer.  In eof_reached
             * state, that's the EOF, which we have available from the saved
             * write position.
             */
            if (*readptr).eof_reached {
                if BufFileSeek(
                    (*state).myfile,
                    (*state).writepos_file,
                    (*state).writepos_offset,
                    SEEK_SET,
                ) != 0
                {
                    ereport!(
                        ERROR,
                        errmsg!("could not seek in tuplestore temporary file")
                    );
                    // C also: errcode_for_file_access()
                }
            } else {
                if BufFileSeek(
                    (*state).myfile,
                    (*readptr).file,
                    (*readptr).offset,
                    SEEK_SET,
                ) != 0
                {
                    ereport!(
                        ERROR,
                        errmsg!("could not seek in tuplestore temporary file")
                    );
                    // C also: errcode_for_file_access()
                }
            }
        }
    }

    (*state).activeptr = ptr;
}

/*
 * tuplestore_tuple_count
 *
 * Returns the number of tuples added since creation or the last
 * tuplestore_clear().
 */
pub unsafe fn tuplestore_tuple_count(state: *mut Tuplestorestate) -> int64 {
    (*state).tuples
}

/*
 * tuplestore_ateof
 *
 * Returns the active read pointer's eof_reached state.
 */
pub unsafe fn tuplestore_ateof(state: *mut Tuplestorestate) -> bool {
    (*(*state).readptrs.add((*state).activeptr as usize)).eof_reached
}

/*
 * Grow the memtuples[] array, if possible within our memory constraint.  We
 * must not exceed INT_MAX tuples in memory or the caller-provided memory
 * limit.  Return true if we were able to enlarge the array, false if not.
 *
 * Normally, at each increment we double the size of the array.  When doing
 * that would exceed a limit, we attempt one last, smaller increase (and then
 * clear the growmemtuples flag so we don't try any more).  That allows us to
 * use memory as fully as permitted; sticking to the pure doubling rule could
 * result in almost half going unused.  Because availMem moves around with
 * tuple addition/removal, we need some rule to prevent making repeated small
 * increases in memtupsize, which would just be useless thrashing.  The
 * growmemtuples flag accomplishes that and also prevents useless
 * recalculations in this function.
 */
unsafe fn grow_memtuples(state: *mut Tuplestorestate) -> bool {
    let mut newmemtupsize: c_int;
    let memtupsize: c_int = (*state).memtupsize;
    let memNowUsed: int64 = (*state).allowedMem - (*state).availMem;

    'noalloc: {
        /* Forget it if we've already maxed out memtuples, per comment above */
        if !(*state).growmemtuples {
            return false;
        }

        /* Select new value of memtupsize */
        if memNowUsed <= (*state).availMem {
            /*
             * We've used no more than half of allowedMem; double our usage,
             * clamping at INT_MAX tuples.
             */
            if memtupsize < c_int::MAX / 2 {
                newmemtupsize = memtupsize * 2;
            } else {
                newmemtupsize = c_int::MAX;
                (*state).growmemtuples = false;
            }
        } else {
            /*
             * This will be the last increment of memtupsize.  Abandon doubling
             * strategy and instead increase as much as we safely can.
             *
             * To stay within allowedMem, we can't increase memtupsize by more
             * than availMem / sizeof(void *) elements. In practice, we want to
             * increase it by considerably less, because we need to leave some
             * space for the tuples to which the new array slots will refer.  We
             * assume the new tuples will be about the same size as the tuples
             * we've already seen, and thus we can extrapolate from the space
             * consumption so far to estimate an appropriate new size for the
             * memtuples array.  The optimal value might be higher or lower than
             * this estimate, but it's hard to know that in advance.  We again
             * clamp at INT_MAX tuples.
             *
             * This calculation is safe against enlarging the array so much that
             * LACKMEM becomes true, because the memory currently used includes
             * the present array; thus, there would be enough allowedMem for the
             * new array elements even if no other memory were currently used.
             *
             * We do the arithmetic in float8, because otherwise the product of
             * memtupsize and allowedMem could overflow.  Any inaccuracy in the
             * result should be insignificant; but even if we computed a
             * completely insane result, the checks below will prevent anything
             * really bad from happening.
             */
            let grow_ratio: f64;

            grow_ratio = (*state).allowedMem as f64 / memNowUsed as f64;
            if (memtupsize as f64 * grow_ratio) < c_int::MAX as f64 {
                newmemtupsize = (memtupsize as f64 * grow_ratio) as c_int;
            } else {
                newmemtupsize = c_int::MAX;
            }

            /* We won't make any further enlargement attempts */
            (*state).growmemtuples = false;
        }

        /* Must enlarge array by at least one element, else report failure */
        if newmemtupsize <= memtupsize {
            break 'noalloc;
        }

        /*
         * On a 32-bit machine, allowedMem could exceed MaxAllocHugeSize.  Clamp
         * to ensure our request won't be rejected.  Note that we can easily
         * exhaust address space before facing this outcome.  (This is presently
         * impossible due to guc.c's MAX_KILOBYTES limitation on work_mem, but
         * don't rely on that at this distance.)
         */
        if (newmemtupsize as Size) >= MaxAllocHugeSize / core::mem::size_of::<*mut c_void>() {
            newmemtupsize = (MaxAllocHugeSize / core::mem::size_of::<*mut c_void>()) as c_int;
            (*state).growmemtuples = false; /* can't grow any more */
        }

        /*
         * We need to be sure that we do not cause LACKMEM to become true, else
         * the space management algorithm will go nuts.  The code above should
         * never generate a dangerous request, but to be safe, check explicitly
         * that the array growth fits within availMem.  (We could still cause
         * LACKMEM if the memory chunk overhead associated with the memtuples
         * array were to increase.  That shouldn't happen because we chose the
         * initial array size large enough to ensure that palloc will be treating
         * both old and new arrays as separate chunks.  But we'll check LACKMEM
         * explicitly below just in case.)
         */
        if (*state).availMem
            < ((newmemtupsize - memtupsize) as int64
                * core::mem::size_of::<*mut c_void>() as int64)
        {
            break 'noalloc;
        }

        /* OK, do it */
        FREEMEM(state, GetMemoryChunkSpace((*state).memtuples as *mut c_void) as int64);
        (*state).memtupsize = newmemtupsize;
        (*state).memtuples = repalloc_huge(
            (*state).memtuples as *mut c_void,
            (*state).memtupsize as Size * core::mem::size_of::<*mut c_void>(),
        ) as *mut *mut c_void;
        USEMEM(state, GetMemoryChunkSpace((*state).memtuples as *mut c_void) as int64);
        if LACKMEM(state) {
            elog!(ERROR, "unexpected out-of-memory situation in tuplestore");
        }
        return true;
    }

    // noalloc:
    /* If for any reason we didn't realloc, shut off future attempts */
    (*state).growmemtuples = false;
    false
}

/*
 * Accept one tuple and append it to the tuplestore.
 *
 * Note that the input tuple is always copied; the caller need not save it.
 *
 * If the active read pointer is currently "at EOF", it remains so (the read
 * pointer implicitly advances along with the write pointer); otherwise the
 * read pointer is unchanged.  Non-active read pointers do not move, which
 * means they are certain to not be "at EOF" immediately after puttuple.
 * This curious-seeming behavior is for the convenience of nodeMaterial.c and
 * nodeCtescan.c, which would otherwise need to do extra pointer repositioning
 * steps.
 *
 * tuplestore_puttupleslot() is a convenience routine to collect data from
 * a TupleTableSlot without an extra copy operation.
 */
pub unsafe fn tuplestore_puttupleslot(state: *mut Tuplestorestate, slot: *mut TupleTableSlot) {
    let tuple: MinimalTuple;
    let oldcxt: MemoryContext = MemoryContextSwitchTo((*state).context);

    /*
     * Form a MinimalTuple in working memory
     */
    tuple = ExecCopySlotMinimalTuple(slot);
    USEMEM(state, GetMemoryChunkSpace(tuple as *mut c_void) as int64);

    tuplestore_puttuple_common(state, tuple as *mut c_void);

    MemoryContextSwitchTo(oldcxt);
}

/*
 * "Standard" case to copy from a HeapTuple.  This is actually now somewhat
 * deprecated, but not worth getting rid of in view of the number of callers.
 */
pub unsafe fn tuplestore_puttuple(state: *mut Tuplestorestate, tuple: HeapTuple) {
    let mut tuple = tuple;
    let oldcxt: MemoryContext = MemoryContextSwitchTo((*state).context);

    /*
     * Copy the tuple.  (Must do this even in WRITEFILE case.  Note that
     * COPYTUP includes USEMEM, so we needn't do that here.)
     */
    tuple = COPYTUP(state, tuple as *mut c_void) as HeapTuple;

    tuplestore_puttuple_common(state, tuple as *mut c_void);

    MemoryContextSwitchTo(oldcxt);
}

/*
 * Similar to tuplestore_puttuple(), but work from values + nulls arrays.
 * This avoids an extra tuple-construction operation.
 */
pub unsafe fn tuplestore_putvalues(
    state: *mut Tuplestorestate,
    tdesc: TupleDesc,
    values: *const Datum,
    isnull: *const bool,
) {
    let tuple: MinimalTuple;
    let oldcxt: MemoryContext = MemoryContextSwitchTo((*state).context);

    tuple = heap_form_minimal_tuple(tdesc, values, isnull, 0);
    USEMEM(state, GetMemoryChunkSpace(tuple as *mut c_void) as int64);

    tuplestore_puttuple_common(state, tuple as *mut c_void);

    MemoryContextSwitchTo(oldcxt);
}

unsafe fn tuplestore_puttuple_common(state: *mut Tuplestorestate, tuple: *mut c_void) {
    let mut readptr: *mut TSReadPointer;
    let mut i: c_int;
    let oldowner: ResourceOwner;
    let oldcxt: MemoryContext;

    (*state).tuples += 1;

    match (*state).status {
        TSS_INMEM => {
            /*
             * Update read pointers as needed; see API spec above.
             */
            readptr = (*state).readptrs;
            i = 0;
            while i < (*state).readptrcount {
                if (*readptr).eof_reached && i != (*state).activeptr {
                    (*readptr).eof_reached = false;
                    (*readptr).current = (*state).memtupcount;
                }
                readptr = readptr.add(1);
                i += 1;
            }

            /*
             * Grow the array as needed.  Note that we try to grow the array
             * when there is still one free slot remaining --- if we fail,
             * there'll still be room to store the incoming tuple, and then
             * we'll switch to tape-based operation.
             */
            if (*state).memtupcount >= (*state).memtupsize - 1 {
                grow_memtuples(state);
                Assert!((*state).memtupcount < (*state).memtupsize);
            }

            /* Stash the tuple in the in-memory array */
            *(*state).memtuples.add((*state).memtupcount as usize) = tuple;
            (*state).memtupcount += 1;

            /*
             * Done if we still fit in available memory and have array slots.
             */
            if (*state).memtupcount < (*state).memtupsize && !LACKMEM(state) {
                return;
            }

            /*
             * Nope; time to switch to tape-based operation.  Make sure that
             * the temp file(s) are created in suitable temp tablespaces.
             */
            PrepareTempTablespaces();

            /* associate the file with the store's resource owner */
            oldowner = CurrentResourceOwner;
            CurrentResourceOwner = (*state).resowner;

            /*
             * We switch out of the state->context as this is a generation
             * context, which isn't ideal for allocations relating to the
             * BufFile.
             */
            oldcxt = MemoryContextSwitchTo((*(*state).context).parent);

            (*state).myfile = BufFileCreateTemp((*state).interXact);

            MemoryContextSwitchTo(oldcxt);

            CurrentResourceOwner = oldowner;

            /*
             * Freeze the decision about whether trailing length words will be
             * used.  We can't change this choice once data is on tape, even
             * though callers might drop the requirement.
             */
            (*state).backward = ((*state).eflags & EXEC_FLAG_BACKWARD) != 0;

            /*
             * Update the maximum space used before dumping the tuples.  It's
             * possible that more space will be used by the tuples in memory
             * than the space that will be used on disk.
             */
            tuplestore_updatemax(state);

            (*state).status = TSS_WRITEFILE;
            dumptuples(state);
        }
        TSS_WRITEFILE => {
            /*
             * Update read pointers as needed; see API spec above. Note:
             * BufFileTell is quite cheap, so not worth trying to avoid
             * multiple calls.
             */
            readptr = (*state).readptrs;
            i = 0;
            while i < (*state).readptrcount {
                if (*readptr).eof_reached && i != (*state).activeptr {
                    (*readptr).eof_reached = false;
                    BufFileTell((*state).myfile, &mut (*readptr).file, &mut (*readptr).offset);
                }
                readptr = readptr.add(1);
                i += 1;
            }

            WRITETUP(state, tuple);
        }
        TSS_READFILE => {
            /*
             * Switch from reading to writing.
             */
            if !(*(*state).readptrs.add((*state).activeptr as usize)).eof_reached {
                BufFileTell(
                    (*state).myfile,
                    &mut (*(*state).readptrs.add((*state).activeptr as usize)).file,
                    &mut (*(*state).readptrs.add((*state).activeptr as usize)).offset,
                );
            }
            if BufFileSeek(
                (*state).myfile,
                (*state).writepos_file,
                (*state).writepos_offset,
                SEEK_SET,
            ) != 0
            {
                ereport!(
                    ERROR,
                    errmsg!("could not seek in tuplestore temporary file")
                );
                // C also: errcode_for_file_access()
            }
            (*state).status = TSS_WRITEFILE;

            /*
             * Update read pointers as needed; see API spec above.
             */
            readptr = (*state).readptrs;
            i = 0;
            while i < (*state).readptrcount {
                if (*readptr).eof_reached && i != (*state).activeptr {
                    (*readptr).eof_reached = false;
                    (*readptr).file = (*state).writepos_file;
                    (*readptr).offset = (*state).writepos_offset;
                }
                readptr = readptr.add(1);
                i += 1;
            }

            WRITETUP(state, tuple);
        }
    }
}

/*
 * Fetch the next tuple in either forward or back direction.
 * Returns NULL if no more tuples.  If should_free is set, the
 * caller must pfree the returned tuple when done with it.
 *
 * Backward scan is only allowed if randomAccess was set true or
 * EXEC_FLAG_BACKWARD was specified to tuplestore_set_eflags().
 */
unsafe fn tuplestore_gettuple(
    state: *mut Tuplestorestate,
    forward: bool,
    should_free: *mut bool,
) -> *mut c_void {
    let readptr: *mut TSReadPointer = &mut *(*state).readptrs.add((*state).activeptr as usize);
    let mut tuplen: c_uint;
    let tup: *mut c_void;

    Assert!(forward || ((*readptr).eflags & EXEC_FLAG_BACKWARD) != 0);

    match (*state).status {
        TSS_INMEM => {
            *should_free = false;
            if forward {
                if (*readptr).eof_reached {
                    return null_mut();
                }
                if (*readptr).current < (*state).memtupcount {
                    /* We have another tuple, so return it */
                    let r = *(*state).memtuples.add((*readptr).current as usize);
                    (*readptr).current += 1;
                    return r;
                }
                (*readptr).eof_reached = true;
                null_mut()
            } else {
                /*
                 * if all tuples are fetched already then we return last
                 * tuple, else tuple before last returned.
                 */
                if (*readptr).eof_reached {
                    (*readptr).current = (*state).memtupcount;
                    (*readptr).eof_reached = false;
                } else {
                    if (*readptr).current <= (*state).memtupdeleted {
                        Assert!(!(*state).truncated);
                        return null_mut();
                    }
                    (*readptr).current -= 1; /* last returned tuple */
                }
                if (*readptr).current <= (*state).memtupdeleted {
                    Assert!(!(*state).truncated);
                    return null_mut();
                }
                *(*state).memtuples.add(((*readptr).current - 1) as usize)
            }
        }

        TSS_WRITEFILE | TSS_READFILE => {
            // The C code falls through from TSS_WRITEFILE to TSS_READFILE.
            if (*state).status == TSS_WRITEFILE {
                /* Skip state change if we'll just return NULL */
                if (*readptr).eof_reached && forward {
                    return null_mut();
                }

                /*
                 * Switch from writing to reading.
                 */
                BufFileTell(
                    (*state).myfile,
                    &mut (*state).writepos_file,
                    &mut (*state).writepos_offset,
                );
                if !(*readptr).eof_reached {
                    if BufFileSeek(
                        (*state).myfile,
                        (*readptr).file,
                        (*readptr).offset,
                        SEEK_SET,
                    ) != 0
                    {
                        ereport!(
                            ERROR,
                            errmsg!("could not seek in tuplestore temporary file")
                        );
                        // C also: errcode_for_file_access()
                    }
                }
                (*state).status = TSS_READFILE;
                /* FALLTHROUGH */
            }

            // case TSS_READFILE:
            *should_free = true;
            if forward {
                tuplen = getlen(state, true);
                if tuplen != 0 {
                    tup = READTUP(state, tuplen);
                    return tup;
                } else {
                    (*readptr).eof_reached = true;
                    return null_mut();
                }
            }

            /*
             * Backward.
             *
             * if all tuples are fetched already then we return last tuple,
             * else tuple before last returned.
             *
             * Back up to fetch previously-returned tuple's ending length
             * word. If seek fails, assume we are at start of file.
             */
            if BufFileSeek(
                (*state).myfile,
                0,
                -(core::mem::size_of::<c_uint>() as i64),
                SEEK_CUR,
            ) != 0
            {
                /* even a failed backwards fetch gets you out of eof state */
                (*readptr).eof_reached = false;
                Assert!(!(*state).truncated);
                return null_mut();
            }
            tuplen = getlen(state, false);

            if (*readptr).eof_reached {
                (*readptr).eof_reached = false;
                /* We will return the tuple returned before returning NULL */
            } else {
                /*
                 * Back up to get ending length word of tuple before it.
                 */
                if BufFileSeek(
                    (*state).myfile,
                    0,
                    -((tuplen as i64) + 2 * core::mem::size_of::<c_uint>() as i64),
                    SEEK_CUR,
                ) != 0
                {
                    /*
                     * If that fails, presumably the prev tuple is the first
                     * in the file.  Back up so that it becomes next to read
                     * in forward direction (not obviously right, but that is
                     * what in-memory case does).
                     */
                    if BufFileSeek(
                        (*state).myfile,
                        0,
                        -((tuplen as i64) + core::mem::size_of::<c_uint>() as i64),
                        SEEK_CUR,
                    ) != 0
                    {
                        ereport!(
                            ERROR,
                            errmsg!("could not seek in tuplestore temporary file")
                        );
                        // C also: errcode_for_file_access()
                    }
                    Assert!(!(*state).truncated);
                    return null_mut();
                }
                tuplen = getlen(state, false);
            }

            /*
             * Now we have the length of the prior tuple, back up and read it.
             * Note: READTUP expects we are positioned after the initial
             * length word of the tuple, so back up to that point.
             */
            if BufFileSeek((*state).myfile, 0, -(tuplen as i64), SEEK_CUR) != 0 {
                ereport!(
                    ERROR,
                    errmsg!("could not seek in tuplestore temporary file")
                );
                // C also: errcode_for_file_access()
            }
            tup = READTUP(state, tuplen);
            tup
        }
    }
}

/*
 * tuplestore_gettupleslot - exported function to fetch a MinimalTuple
 *
 * If successful, put tuple in slot and return true; else, clear the slot
 * and return false.
 *
 * If copy is true, the slot receives a copied tuple (allocated in current
 * memory context) that will stay valid regardless of future manipulations of
 * the tuplestore's state.  If copy is false, the slot may just receive a
 * pointer to a tuple held within the tuplestore.  The latter is more
 * efficient but the slot contents may be corrupted if additional writes to
 * the tuplestore occur.  (If using tuplestore_trim, see comments therein.)
 */
pub unsafe fn tuplestore_gettupleslot(
    state: *mut Tuplestorestate,
    forward: bool,
    copy: bool,
    slot: *mut TupleTableSlot,
) -> bool {
    let mut tuple: MinimalTuple;
    let mut should_free: bool = false;

    tuple = tuplestore_gettuple(state, forward, &mut should_free) as MinimalTuple;

    if !tuple.is_null() {
        if copy && !should_free {
            tuple = heap_copy_minimal_tuple(tuple, 0);
            should_free = true;
        }
        ExecStoreMinimalTuple(tuple, slot, should_free);
        true
    } else {
        ExecClearTuple(slot);
        false
    }
}

/*
 * tuplestore_advance - exported function to adjust position without fetching
 *
 * We could optimize this case to avoid palloc/pfree overhead, but for the
 * moment it doesn't seem worthwhile.
 */
pub unsafe fn tuplestore_advance(state: *mut Tuplestorestate, forward: bool) -> bool {
    let tuple: *mut c_void;
    let mut should_free: bool = false;

    tuple = tuplestore_gettuple(state, forward, &mut should_free);

    if !tuple.is_null() {
        if should_free {
            pfree(tuple);
        }
        true
    } else {
        false
    }
}

/*
 * Advance over N tuples in either forward or back direction,
 * without returning any data.  N<=0 is a no-op.
 * Returns true if successful, false if ran out of tuples.
 */
pub unsafe fn tuplestore_skiptuples(
    state: *mut Tuplestorestate,
    ntuples: int64,
    forward: bool,
) -> bool {
    let mut ntuples = ntuples;
    let readptr: *mut TSReadPointer = &mut *(*state).readptrs.add((*state).activeptr as usize);

    Assert!(forward || ((*readptr).eflags & EXEC_FLAG_BACKWARD) != 0);

    if ntuples <= 0 {
        return true;
    }

    match (*state).status {
        TSS_INMEM => {
            if forward {
                if (*readptr).eof_reached {
                    return false;
                }
                if ((*state).memtupcount - (*readptr).current) as int64 >= ntuples {
                    (*readptr).current += ntuples as c_int;
                    return true;
                }
                (*readptr).current = (*state).memtupcount;
                (*readptr).eof_reached = true;
                false
            } else {
                if (*readptr).eof_reached {
                    (*readptr).current = (*state).memtupcount;
                    (*readptr).eof_reached = false;
                    ntuples -= 1;
                }
                if ((*readptr).current - (*state).memtupdeleted) as int64 > ntuples {
                    (*readptr).current -= ntuples as c_int;
                    return true;
                }
                Assert!(!(*state).truncated);
                (*readptr).current = (*state).memtupdeleted;
                false
            }
        }

        _ => {
            /* We don't currently try hard to optimize other cases */
            while {
                let t = ntuples;
                ntuples -= 1;
                t > 0
            } {
                let tuple: *mut c_void;
                let mut should_free: bool = false;

                tuple = tuplestore_gettuple(state, forward, &mut should_free);

                if tuple.is_null() {
                    return false;
                }
                if should_free {
                    pfree(tuple);
                }
                CHECK_FOR_INTERRUPTS();
            }
            true
        }
    }
}

/*
 * dumptuples - remove tuples from memory and write to tape
 *
 * As a side effect, we must convert each read pointer's position from
 * "current" to file/offset format.  But eof_reached pointers don't
 * need to change state.
 */
unsafe fn dumptuples(state: *mut Tuplestorestate) {
    let mut i: c_int;

    i = (*state).memtupdeleted;
    loop {
        let mut readptr: *mut TSReadPointer = (*state).readptrs;
        let mut j: c_int;

        j = 0;
        while j < (*state).readptrcount {
            if i == (*readptr).current && !(*readptr).eof_reached {
                BufFileTell((*state).myfile, &mut (*readptr).file, &mut (*readptr).offset);
            }
            readptr = readptr.add(1);
            j += 1;
        }
        if i >= (*state).memtupcount {
            break;
        }
        WRITETUP(state, *(*state).memtuples.add(i as usize));

        i += 1;
    }
    (*state).memtupdeleted = 0;
    (*state).memtupcount = 0;
}

/*
 * tuplestore_rescan		- rewind the active read pointer to start
 */
pub unsafe fn tuplestore_rescan(state: *mut Tuplestorestate) {
    let readptr: *mut TSReadPointer = &mut *(*state).readptrs.add((*state).activeptr as usize);

    Assert!(((*readptr).eflags & EXEC_FLAG_REWIND) != 0);
    Assert!(!(*state).truncated);

    match (*state).status {
        TSS_INMEM => {
            (*readptr).eof_reached = false;
            (*readptr).current = 0;
        }
        TSS_WRITEFILE => {
            (*readptr).eof_reached = false;
            (*readptr).file = 0;
            (*readptr).offset = 0;
        }
        TSS_READFILE => {
            (*readptr).eof_reached = false;
            if BufFileSeek((*state).myfile, 0, 0, SEEK_SET) != 0 {
                ereport!(
                    ERROR,
                    errmsg!("could not seek in tuplestore temporary file")
                );
                // C also: errcode_for_file_access()
            }
        }
    }
}

/*
 * tuplestore_copy_read_pointer - copy a read pointer's state to another
 */
pub unsafe fn tuplestore_copy_read_pointer(
    state: *mut Tuplestorestate,
    srcptr: c_int,
    destptr: c_int,
) {
    let sptr: *mut TSReadPointer = &mut *(*state).readptrs.add(srcptr as usize);
    let dptr: *mut TSReadPointer = &mut *(*state).readptrs.add(destptr as usize);

    Assert!(srcptr >= 0 && srcptr < (*state).readptrcount);
    Assert!(destptr >= 0 && destptr < (*state).readptrcount);

    /* Assigning to self is a no-op */
    if srcptr == destptr {
        return;
    }

    if (*dptr).eflags != (*sptr).eflags {
        /* Possible change of overall eflags, so copy and then recompute */
        let mut eflags: c_int;
        let mut i: c_int;

        *dptr = *sptr;
        eflags = (*(*state).readptrs.add(0)).eflags;
        i = 1;
        while i < (*state).readptrcount {
            eflags |= (*(*state).readptrs.add(i as usize)).eflags;
            i += 1;
        }
        (*state).eflags = eflags;
    } else {
        *dptr = *sptr;
    }

    match (*state).status {
        TSS_INMEM | TSS_WRITEFILE => {
            /* no work */
        }
        TSS_READFILE => {
            /*
             * This case is a bit tricky since the active read pointer's
             * position corresponds to the seek point, not what is in its
             * variables.  Assigning to the active requires a seek, and
             * assigning from the active requires a tell, except when
             * eof_reached.
             */
            if destptr == (*state).activeptr {
                if (*dptr).eof_reached {
                    if BufFileSeek(
                        (*state).myfile,
                        (*state).writepos_file,
                        (*state).writepos_offset,
                        SEEK_SET,
                    ) != 0
                    {
                        ereport!(
                            ERROR,
                            errmsg!("could not seek in tuplestore temporary file")
                        );
                        // C also: errcode_for_file_access()
                    }
                } else {
                    if BufFileSeek(
                        (*state).myfile,
                        (*dptr).file,
                        (*dptr).offset,
                        SEEK_SET,
                    ) != 0
                    {
                        ereport!(
                            ERROR,
                            errmsg!("could not seek in tuplestore temporary file")
                        );
                        // C also: errcode_for_file_access()
                    }
                }
            } else if srcptr == (*state).activeptr {
                if !(*dptr).eof_reached {
                    BufFileTell((*state).myfile, &mut (*dptr).file, &mut (*dptr).offset);
                }
            }
        }
    }
}

/*
 * tuplestore_trim	- remove all no-longer-needed tuples
 *
 * Calling this function authorizes the tuplestore to delete all tuples
 * before the oldest read pointer, if no read pointer is marked as requiring
 * REWIND capability.
 *
 * Note: this is obviously safe if no pointer has BACKWARD capability either.
 * If a pointer is marked as BACKWARD but not REWIND capable, it means that
 * the pointer can be moved backward but not before the oldest other read
 * pointer.
 */
pub unsafe fn tuplestore_trim(state: *mut Tuplestorestate) {
    let mut oldest: c_int;
    let nremove: c_int;
    let mut i: c_int;

    /*
     * Truncation is disallowed if any read pointer requires rewind
     * capability.
     */
    if ((*state).eflags & EXEC_FLAG_REWIND) != 0 {
        return;
    }

    /*
     * We don't bother trimming temp files since it usually would mean more
     * work than just letting them sit in kernel buffers until they age out.
     */
    if (*state).status != TSS_INMEM {
        return;
    }

    /* Find the oldest read pointer */
    oldest = (*state).memtupcount;
    i = 0;
    while i < (*state).readptrcount {
        if !(*(*state).readptrs.add(i as usize)).eof_reached {
            oldest = Min(oldest, (*(*state).readptrs.add(i as usize)).current);
        }
        i += 1;
    }

    /*
     * Note: you might think we could remove all the tuples before the oldest
     * "current", since that one is the next to be returned.  However, since
     * tuplestore_gettuple returns a direct pointer to our internal copy of
     * the tuple, it's likely that the caller has still got the tuple just
     * before "current" referenced in a slot. So we keep one extra tuple
     * before the oldest "current".  (Strictly speaking, we could require such
     * callers to use the "copy" flag to tuplestore_gettupleslot, but for
     * efficiency we allow this one case to not use "copy".)
     */
    nremove = oldest - 1;
    if nremove <= 0 {
        return; /* nothing to do */
    }

    Assert!(nremove >= (*state).memtupdeleted);
    Assert!(nremove <= (*state).memtupcount);

    /* before freeing any memory, update the statistics */
    tuplestore_updatemax(state);

    /* Release no-longer-needed tuples */
    i = (*state).memtupdeleted;
    while i < nremove {
        FREEMEM(
            state,
            GetMemoryChunkSpace(*(*state).memtuples.add(i as usize)) as int64,
        );
        pfree(*(*state).memtuples.add(i as usize));
        *(*state).memtuples.add(i as usize) = null_mut();
        i += 1;
    }
    (*state).memtupdeleted = nremove;

    /* mark tuplestore as truncated (used for Assert crosschecks only) */
    (*state).truncated = true;

    /*
     * If nremove is less than 1/8th memtupcount, just stop here, leaving the
     * "deleted" slots as NULL.  This prevents us from expending O(N^2) time
     * repeatedly memmove-ing a large pointer array.  The worst case space
     * wastage is pretty small, since it's just pointers and not whole tuples.
     */
    if nremove < (*state).memtupcount / 8 {
        return;
    }

    /*
     * Slide the array down and readjust pointers.
     *
     * In mergejoin's current usage, it's demonstrable that there will always
     * be exactly one non-removed tuple; so optimize that case.
     */
    if nremove + 1 == (*state).memtupcount {
        *(*state).memtuples.add(0) = *(*state).memtuples.add(nremove as usize);
    } else {
        core::ptr::copy(
            (*state).memtuples.add(nremove as usize),
            (*state).memtuples,
            ((*state).memtupcount - nremove) as usize,
        );
    }

    (*state).memtupdeleted = 0;
    (*state).memtupcount -= nremove;
    i = 0;
    while i < (*state).readptrcount {
        if !(*(*state).readptrs.add(i as usize)).eof_reached {
            (*(*state).readptrs.add(i as usize)).current -= nremove;
        }
        i += 1;
    }
}

/*
 * tuplestore_updatemax
 *		Update the maximum space used by this tuplestore and the method used
 *		for storage.
 */
unsafe fn tuplestore_updatemax(state: *mut Tuplestorestate) {
    if (*state).status == TSS_INMEM {
        (*state).maxSpace = Max(
            (*state).maxSpace,
            (*state).allowedMem - (*state).availMem,
        );
    } else {
        (*state).maxSpace = Max((*state).maxSpace, BufFileSize((*state).myfile));

        /*
         * usedDisk never gets set to false again after spilling to disk, even
         * if tuplestore_clear() is called and new tuples go to memory again.
         */
        (*state).usedDisk = true;
    }
}

/*
 * tuplestore_get_stats
 *		Obtain statistics about the maximum space used by the tuplestore.
 *		These statistics are the maximums and are not reset by calls to
 *		tuplestore_trim() or tuplestore_clear().
 */
pub unsafe fn tuplestore_get_stats(
    state: *mut Tuplestorestate,
    max_storage_type: *mut *const c_char,
    max_space: *mut int64,
) {
    tuplestore_updatemax(state);

    if (*state).usedDisk {
        *max_storage_type = c"Disk".as_ptr();
    } else {
        *max_storage_type = c"Memory".as_ptr();
    }

    *max_space = (*state).maxSpace;
}

/*
 * tuplestore_in_memory
 *
 * Returns true if the tuplestore has not spilled to disk.
 *
 * XXX exposing this is a violation of modularity ... should get rid of it.
 */
pub unsafe fn tuplestore_in_memory(state: *mut Tuplestorestate) -> bool {
    (*state).status == TSS_INMEM
}


/*
 * Tape interface routines
 */

unsafe fn getlen(state: *mut Tuplestorestate, eofOK: bool) -> c_uint {
    let mut len: c_uint = 0;
    let nbytes: usize;

    nbytes = BufFileReadMaybeEOF(
        (*state).myfile,
        &mut len as *mut c_uint as *mut c_void,
        core::mem::size_of::<c_uint>(),
        eofOK,
    );
    if nbytes == 0 {
        0
    } else {
        len
    }
}


/*
 * Routines specialized for HeapTuple case
 *
 * The stored form is actually a MinimalTuple, but for largely historical
 * reasons we allow COPYTUP to work from a HeapTuple.
 *
 * Since MinimalTuple already has length in its first word, we don't need
 * to write that separately.
 */

unsafe fn copytup_heap(state: *mut Tuplestorestate, tup: *mut c_void) -> *mut c_void {
    let tuple: MinimalTuple;

    tuple = minimal_tuple_from_heap_tuple(tup as HeapTuple, 0);
    USEMEM(state, GetMemoryChunkSpace(tuple as *mut c_void) as int64);
    tuple as *mut c_void
}

unsafe fn writetup_heap(state: *mut Tuplestorestate, tup: *mut c_void) {
    let tuple: MinimalTuple = tup as MinimalTuple;

    /* the part of the MinimalTuple we'll write: */
    let tupbody: *mut c_char = (tuple as *mut c_char).add(MINIMAL_TUPLE_DATA_OFFSET);
    let tupbodylen: c_uint = (*tuple).t_len - MINIMAL_TUPLE_DATA_OFFSET as c_uint;

    /* total on-disk footprint: */
    let tuplen: c_uint = tupbodylen + core::mem::size_of::<c_int>() as c_uint;

    BufFileWrite(
        (*state).myfile,
        &tuplen as *const c_uint as *const c_void,
        core::mem::size_of::<c_uint>(),
    );
    BufFileWrite((*state).myfile, tupbody as *const c_void, tupbodylen as usize);
    if (*state).backward {
        /* need trailing length word? */
        BufFileWrite(
            (*state).myfile,
            &tuplen as *const c_uint as *const c_void,
            core::mem::size_of::<c_uint>(),
        );
    }

    FREEMEM(state, GetMemoryChunkSpace(tuple as *mut c_void) as int64);
    heap_free_minimal_tuple(tuple);
}

unsafe fn readtup_heap(state: *mut Tuplestorestate, len: c_uint) -> *mut c_void {
    let tupbodylen: c_uint = len - core::mem::size_of::<c_int>() as c_uint;
    let tuplen: c_uint = tupbodylen + MINIMAL_TUPLE_DATA_OFFSET as c_uint;
    let tuple: MinimalTuple = palloc(tuplen as Size) as MinimalTuple;
    let tupbody: *mut c_char = (tuple as *mut c_char).add(MINIMAL_TUPLE_DATA_OFFSET);
    let mut tuplen_trailer: c_uint = 0;

    /* read in the tuple proper */
    (*tuple).t_len = tuplen;
    BufFileReadExact((*state).myfile, tupbody as *mut c_void, tupbodylen as usize);
    if (*state).backward {
        /* need trailing length word? */
        BufFileReadExact(
            (*state).myfile,
            &mut tuplen_trailer as *mut c_uint as *mut c_void,
            core::mem::size_of::<c_uint>(),
        );
    }
    tuple as *mut c_void
}
