//! tuplesort.rs
//!   Generalized tuple sorting routines.
//!
//! Translated 1:1 from postgres/src/backend/utils/sort/tuplesort.c
//! (PostgreSQL 18.3).  The header src/include/utils/tuplesort.h is merged in
//! at the top (SortTuple, TuplesortPublic, TuplesortInstrumentation, the public
//! prototypes, etc.).
//!
//! This module provides a generalized facility for tuple sorting, which can be
//! applied to different kinds of sortable objects.  Implementation of the
//! particular sorting variants is given in tuplesortvariants.c.  This module
//! works efficiently for both small and large amounts of data.  Small amounts
//! are sorted in-memory using qsort().  Large amounts are sorted using
//! temporary files and a standard external sort algorithm.
//!
//! See the long comment at the top of the C file for the algorithm.
//!
//! #include mapping:
//!   "postgres.h"               -> crate::prelude::*
//!   <limits.h>                 -> INT_MAX (local const)
//!   "commands/tablespace.h"    -> PrepareTempTablespaces (STUB below)
//!   "miscadmin.h"              -> CHECK_FOR_INTERRUPTS! (STUB macro below)
//!   "pg_trace.h"               -> TRACE_POSTGRESQL_SORT_* (STUB no-ops below)
//!   "storage/shmem.h"          -> add_size / mul_size
//!   "utils/guc.h"              -> trace_sort GUC (defined here)
//!   "utils/memutils.h"         -> GetMemoryChunkSpace / context helpers
//!   "utils/pg_rusage.h"        -> PGRUsage / pg_rusage_init / pg_rusage_show
//!   "utils/tuplesort.h"        -> merged here
//!   "lib/sort_template.h"      -> specialized qsort wrappers (built on
//!                                 qsort_interruptible) defined below
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!   src/backend/utils/sort/tuplesort.c

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(unused_variables)]
#![allow(dead_code)]

use crate::prelude::*;

use crate::c::{int32, int64, uint32, Max, Min, Size, MAXALIGN};
use crate::pg_config::BLCKSZ;
use crate::port::qsort::qsort_arg_comparator;
use crate::storage::file::sharedfileset::{
    dsm_segment, SharedFileSet, SharedFileSetAttach, SharedFileSetInit,
};
use crate::storage::ipc::shmem::{add_size, mul_size};
use crate::storage::lmgr::s_lock::slock_t;
use crate::storage::spin::{SpinLockAcquire, SpinLockInit, SpinLockRelease};
use crate::utils::memutils::MaxAllocHugeSize;
use crate::utils::misc::pg_rusage::{pg_rusage_init, pg_rusage_show, PGRUsage};
use crate::utils::mmgr::bump::BumpContextCreate;
use crate::utils::mmgr::mcxt::{
    repalloc_huge, GetMemoryChunkSpace, MemoryContextResetOnly,
};
use crate::utils::sort::logtape::{
    LogicalTape, LogicalTapeBackspace, LogicalTapeClose, LogicalTapeCreate, LogicalTapeFreeze,
    LogicalTapeImport, LogicalTapeRead, LogicalTapeRewindForRead, LogicalTapeSeek,
    LogicalTapeSet, LogicalTapeSetBlocks, LogicalTapeSetClose, LogicalTapeSetCreate,
    LogicalTapeSetForgetFreeSpace, LogicalTapeTell, LogicalTapeWrite, TapeShare,
};
use crate::utils::sort::sortsupport::{
    ApplySortComparator, SortSupport, SortSupportData,
};
use core::ffi::{c_int, c_void, CStr};

// ---------------------------------------------------------------------------
// Stubbed external dependencies (functions defined in other .c files not yet
// ported).
// ---------------------------------------------------------------------------

/// `CHECK_FOR_INTERRUPTS` (miscadmin.h).
// TODO(pg-port): wire to the real interrupt machinery once miscadmin is ported.
macro_rules! CHECK_FOR_INTERRUPTS {
    () => {{
        // TODO(pg-port): miscadmin.h CHECK_FOR_INTERRUPTS
    }};
}

/// `PrepareTempTablespaces` (commands/tablespace.c). STUB.
// TODO(pg-port): port tablespace.c; arranges that temp files are created in a
// suitable temp tablespace.
unsafe fn PrepareTempTablespaces() {
    // TODO(pg-port): commands/tablespace.c
}

/// `ApplyUnsignedSortComparator` (utils/sortsupport.h, inline). STUB.
// TODO(pg-port): port the inline Apply{Unsigned,Signed,Int32}SortComparator
// helpers in sortsupport.h.  They handle reverse-sort/NULLs ordering against a
// pass-by-value leading datum.
#[inline]
unsafe fn ApplyUnsignedSortComparator(
    datum1: Datum,
    isNull1: bool,
    datum2: Datum,
    isNull2: bool,
    ssup: SortSupport,
) -> c_int {
    // TODO(pg-port): utils/sortsupport.h ApplyUnsignedSortComparator
    ApplySortComparator(datum1, isNull1, datum2, isNull2, ssup)
}

/// `ApplySignedSortComparator` (utils/sortsupport.h, inline). STUB.
// TODO(pg-port): see ApplyUnsignedSortComparator.
#[inline]
unsafe fn ApplySignedSortComparator(
    datum1: Datum,
    isNull1: bool,
    datum2: Datum,
    isNull2: bool,
    ssup: SortSupport,
) -> c_int {
    // TODO(pg-port): utils/sortsupport.h ApplySignedSortComparator
    ApplySortComparator(datum1, isNull1, datum2, isNull2, ssup)
}

/// `ApplyInt32SortComparator` (utils/sortsupport.h, inline). STUB.
// TODO(pg-port): see ApplyUnsignedSortComparator.
#[inline]
unsafe fn ApplyInt32SortComparator(
    datum1: Datum,
    isNull1: bool,
    datum2: Datum,
    isNull2: bool,
    ssup: SortSupport,
) -> c_int {
    // TODO(pg-port): utils/sortsupport.h ApplyInt32SortComparator
    ApplySortComparator(datum1, isNull1, datum2, isNull2, ssup)
}

// pg_trace.h probe macros -- no-ops outside dtrace builds.
// TODO(pg-port): real probes come from pg_trace.h / probes.d.
unsafe fn TRACE_POSTGRESQL_SORT_DONE(_disk: bool, _spaceUsed: int64) {}
unsafe fn TRACE_POSTGRESQL_SORT_START(
    _key: c_int,
    _random: bool,
    _nKeys: c_int,
    _workMem: c_int,
    _randomAccess: bool,
    _parallel: c_int,
) {
}

// <limits.h>
const INT_MAX: c_int = c_int::MAX;

// ---------------------------------------------------------------------------
// tuplesort.h -- public types
// ---------------------------------------------------------------------------

/// `Tuplesortstate` / `Sharedsort` are opaque outside tuplesort.c.

/// `SortCoordinateData` (utils/tuplesort.h).
#[repr(C)]
pub struct SortCoordinateData {
    /// Worker process?  If not, must be leader.
    pub isWorker: bool,
    /// Leader-process-passed number of participants known launched (workers
    /// set this to -1).
    pub nParticipants: c_int,
    /// Private opaque state (points to shared memory).
    pub sharedsort: *mut Sharedsort,
}

pub type SortCoordinate = *mut SortCoordinateData;

/// `TuplesortMethod` (utils/tuplesort.h).
pub type TuplesortMethod = c_int;
pub const SORT_TYPE_STILL_IN_PROGRESS: TuplesortMethod = 0;
pub const SORT_TYPE_TOP_N_HEAPSORT: TuplesortMethod = 1 << 0;
pub const SORT_TYPE_QUICKSORT: TuplesortMethod = 1 << 1;
pub const SORT_TYPE_EXTERNAL_SORT: TuplesortMethod = 1 << 2;
pub const SORT_TYPE_EXTERNAL_MERGE: TuplesortMethod = 1 << 3;

pub const NUM_TUPLESORTMETHODS: c_int = 4;

/// `TuplesortSpaceType` (utils/tuplesort.h).
pub type TuplesortSpaceType = c_int;
pub const SORT_SPACE_TYPE_DISK: TuplesortSpaceType = 0;
pub const SORT_SPACE_TYPE_MEMORY: TuplesortSpaceType = 1;

/* Bitwise option flags for tuple sorts */
pub const TUPLESORT_NONE: c_int = 0;
/* specifies whether non-sequential access to the sort result is required */
pub const TUPLESORT_RANDOMACCESS: c_int = 1 << 0;
/* specifies if the tuplesort is able to support bounded sorts */
pub const TUPLESORT_ALLOWBOUNDED: c_int = 1 << 1;

/// `TupleSortUseBumpTupleCxt(opt)` (utils/tuplesort.h).
#[inline]
pub fn TupleSortUseBumpTupleCxt(opt: c_int) -> bool {
    (opt & TUPLESORT_ALLOWBOUNDED) == 0
}

/// `TuplesortInstrumentation` (utils/tuplesort.h).
#[repr(C)]
pub struct TuplesortInstrumentation {
    pub sortMethod: TuplesortMethod,    /* sort algorithm used */
    pub spaceType: TuplesortSpaceType,  /* type of space spaceUsed represents */
    pub spaceUsed: int64,               /* space consumption, in kB */
}

/// `SortTuple` (utils/tuplesort.h).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct SortTuple {
    pub tuple: *mut c_void, /* the tuple itself */
    pub datum1: Datum,      /* value of first key column */
    pub isnull1: bool,      /* is first key column NULL? */
    pub srctape: c_int,     /* source tape number */
}

/// `SortTupleComparator` (utils/tuplesort.h).
pub type SortTupleComparator =
    unsafe fn(a: *const SortTuple, b: *const SortTuple, state: *mut Tuplesortstate) -> c_int;

/// `TuplesortPublic` (utils/tuplesort.h).
#[repr(C)]
pub struct TuplesortPublic {
    pub comparetup: SortTupleComparator,
    pub comparetup_tiebreak: SortTupleComparator,
    pub removeabbrev:
        Option<unsafe fn(state: *mut Tuplesortstate, stups: *mut SortTuple, count: c_int)>,
    pub writetup:
        Option<unsafe fn(state: *mut Tuplesortstate, tape: *mut LogicalTape, stup: *mut SortTuple)>,
    pub readtup: Option<
        unsafe fn(
            state: *mut Tuplesortstate,
            stup: *mut SortTuple,
            tape: *mut LogicalTape,
            len: c_uint,
        ),
    >,
    pub freestate: Option<unsafe fn(state: *mut Tuplesortstate)>,

    pub maincontext: MemoryContext,
    pub sortcontext: MemoryContext,
    pub tuplecontext: MemoryContext,

    pub haveDatum1: bool,

    pub nKeys: c_int,
    pub sortKeys: SortSupport,

    pub onlyKey: SortSupport,

    pub sortopt: c_int,

    pub tuples: bool,

    pub arg: *mut c_void,
}

/// `TuplesortstateGetPublic(state)` (utils/tuplesort.h).
#[inline]
pub unsafe fn TuplesortstateGetPublic(state: *mut Tuplesortstate) -> *mut TuplesortPublic {
    state as *mut TuplesortPublic
}

/// `LogicalTapeReadExact(tape, ptr, len)` (utils/tuplesort.h).
macro_rules! LogicalTapeReadExact {
    ($tape:expr, $ptr:expr, $len:expr) => {{
        if LogicalTapeRead($tape, $ptr, $len) != ($len as usize) {
            elog!(ERROR, "unexpected end of data");
        }
    }};
}

// ---------------------------------------------------------------------------
// tuplesort.c
// ---------------------------------------------------------------------------

/*
 * Initial size of memtuples array.  We're trying to select this size so that
 * array doesn't exceed ALLOCSET_SEPARATE_THRESHOLD and so that the overhead of
 * allocation might possibly be lowered.  However, we don't consider array sizes
 * less than 1024.
 */
// #define INITIAL_MEMTUPSIZE Max(1024,
//     ALLOCSET_SEPARATE_THRESHOLD / sizeof(SortTuple) + 1)
#[inline]
fn INITIAL_MEMTUPSIZE() -> c_int {
    Max(
        1024,
        (ALLOCSET_SEPARATE_THRESHOLD / core::mem::size_of::<SortTuple>() + 1) as c_int,
    )
}

/// `ALLOCSET_SEPARATE_THRESHOLD` (utils/memutils.h).
// TODO(pg-port): pull from utils/memutils.h once exported there; it is 8192.
const ALLOCSET_SEPARATE_THRESHOLD: usize = 8192;

/* GUC variables */
pub static mut trace_sort: bool = false;

// #ifdef DEBUG_BOUNDED_SORT -- not built; optimize_bounded_sort omitted.

/*
 * During merge, we use a pre-allocated set of fixed-size slots to hold
 * tuples.  To avoid palloc/pfree overhead.
 */
const SLAB_SLOT_SIZE: usize = 1024;

/// `union SlabSlot` (tuplesort.c).
#[repr(C)]
pub union SlabSlot {
    pub nextfree: *mut SlabSlot,
    pub buffer: [c_char; SLAB_SLOT_SIZE],
}

/*
 * Possible states of a Tuplesort object.  These denote the states that
 * persist between calls of Tuplesort routines.
 */
pub type TupSortStatus = c_int;
const TSS_INITIAL: TupSortStatus = 0; /* Loading tuples; still within memory limit */
const TSS_BOUNDED: TupSortStatus = 1; /* Loading tuples into bounded-size heap */
const TSS_BUILDRUNS: TupSortStatus = 2; /* Loading tuples; writing to tape */
const TSS_SORTEDINMEM: TupSortStatus = 3; /* Sort completed entirely in memory */
const TSS_SORTEDONTAPE: TupSortStatus = 4; /* Sort completed, final run is on tape */
const TSS_FINALMERGE: TupSortStatus = 5; /* Performing final merge on-the-fly */

/*
 * Parameters for calculation of number of tapes to use --- see inittapes()
 * and tuplesort_merge_order().
 */
const MINORDER: c_int = 6; /* minimum merge order */
const MAXORDER: c_int = 500; /* maximum merge order */
const TAPE_BUFFER_OVERHEAD: int64 = BLCKSZ as int64;
const MERGE_BUFFER_SIZE: int64 = (BLCKSZ * 32) as int64;

/*
 * Private state of a Tuplesort operation.
 */
#[repr(C)]
pub struct Tuplesortstate {
    pub base: TuplesortPublic,
    pub status: TupSortStatus, /* enumerated value as shown above */
    pub bounded: bool,         /* did caller specify a maximum number of tuples to return? */
    pub boundUsed: bool,       /* true if we made use of a bounded heap */
    pub bound: c_int,          /* if bounded, the maximum number of tuples */
    pub tupleMem: int64,       /* memory consumed by individual tuples. */
    pub availMem: int64,       /* remaining memory available, in bytes */
    pub allowedMem: int64,     /* total memory allowed, in bytes */
    pub maxTapes: c_int,       /* max number of input tapes to merge in each pass */
    pub maxSpace: int64,       /* maximum amount of space occupied among sort of groups */
    pub isMaxSpaceDisk: bool,  /* true when maxSpace is value for on-disk space */
    pub maxSpaceStatus: TupSortStatus, /* sort status when maxSpace was reached */
    pub tapeset: *mut LogicalTapeSet, /* logtape.c object for tapes in a temp file */

    pub memtuples: *mut SortTuple, /* array of SortTuple structs */
    pub memtupcount: c_int,        /* number of tuples currently present */
    pub memtupsize: c_int,         /* allocated length of memtuples array */
    pub growmemtuples: bool,       /* memtuples' growth still underway? */

    pub slabAllocatorUsed: bool,

    pub slabMemoryBegin: *mut c_char, /* beginning of slab memory arena */
    pub slabMemoryEnd: *mut c_char,   /* end of slab memory arena */
    pub slabFreeHead: *mut SlabSlot,  /* head of free list */

    /* Memory used for input and output tape buffers. */
    pub tape_buffer_mem: usize,

    pub lastReturnedTuple: *mut c_void,

    pub currentRun: c_int,

    pub inputTapes: *mut *mut LogicalTape,
    pub nInputTapes: c_int,
    pub nInputRuns: c_int,

    pub outputTapes: *mut *mut LogicalTape,
    pub nOutputTapes: c_int,
    pub nOutputRuns: c_int,

    pub destTape: *mut LogicalTape, /* current output tape */

    pub result_tape: *mut LogicalTape, /* actual tape of finished output */
    pub current: c_int,                /* array index (only used if SORTEDINMEM) */
    pub eof_reached: bool,             /* reached EOF (needed for cursors) */

    /* markpos_xxx holds marked position for mark and restore */
    pub markpos_block: int64,    /* tape block# (only used if SORTEDONTAPE) */
    pub markpos_offset: c_int,   /* saved "current", or offset in tape block */
    pub markpos_eof: bool,       /* saved "eof_reached" */

    pub worker: c_int,
    pub shared: *mut Sharedsort,
    pub nParticipants: c_int,

    pub abbrevNext: int64, /* Tuple # at which to next check applicability */

    pub ru_start: PGRUsage,
}

/*
 * Private mutable state of tuplesort-parallel-operation.  This is allocated
 * in shared memory.
 */
#[repr(C)]
pub struct Sharedsort {
    /* mutex protects all fields prior to tapes */
    pub mutex: slock_t,

    pub currentWorker: c_int,
    pub workersFinished: c_int,

    /* Temporary file space */
    pub fileset: SharedFileSet,

    /* Size of tapes flexible array */
    pub nTapes: c_int,

    /* Tapes array (flexible array member) */
    pub tapes: [TapeShare; crate::c::FLEXIBLE_ARRAY_MEMBER],
}

/*
 * Is the given tuple allocated from the slab memory arena?
 */
#[inline]
unsafe fn IS_SLAB_SLOT(state: *mut Tuplesortstate, tuple: *mut c_void) -> bool {
    (tuple as *mut c_char) >= (*state).slabMemoryBegin
        && (tuple as *mut c_char) < (*state).slabMemoryEnd
}

/*
 * Return the given tuple to the slab memory free list, or free it
 * if it was palloc'd.
 */
#[inline]
unsafe fn RELEASE_SLAB_SLOT(state: *mut Tuplesortstate, tuple: *mut c_void) {
    let buf = tuple as *mut SlabSlot;

    if IS_SLAB_SLOT(state, buf as *mut c_void) {
        (*buf).nextfree = (*state).slabFreeHead;
        (*state).slabFreeHead = buf;
    } else {
        pfree(buf as *mut c_void);
    }
}

#[inline]
unsafe fn REMOVEABBREV(state: *mut Tuplesortstate, stup: *mut SortTuple, count: c_int) {
    ((*state).base.removeabbrev.unwrap())(state, stup, count)
}
#[inline]
unsafe fn COMPARETUP(state: *mut Tuplesortstate, a: *const SortTuple, b: *const SortTuple) -> c_int {
    ((*state).base.comparetup)(a, b, state)
}
#[inline]
unsafe fn WRITETUP(state: *mut Tuplesortstate, tape: *mut LogicalTape, stup: *mut SortTuple) {
    ((*state).base.writetup.unwrap())(state, tape, stup)
}
#[inline]
unsafe fn READTUP(
    state: *mut Tuplesortstate,
    stup: *mut SortTuple,
    tape: *mut LogicalTape,
    len: c_uint,
) {
    ((*state).base.readtup.unwrap())(state, stup, tape, len)
}
#[inline]
unsafe fn FREESTATE(state: *mut Tuplesortstate) {
    if let Some(f) = (*state).base.freestate {
        f(state)
    }
}
#[inline]
unsafe fn LACKMEM(state: *mut Tuplesortstate) -> bool {
    (*state).availMem < 0 && !(*state).slabAllocatorUsed
}
#[inline]
unsafe fn USEMEM(state: *mut Tuplesortstate, amt: int64) {
    (*state).availMem -= amt;
}
#[inline]
unsafe fn FREEMEM(state: *mut Tuplesortstate, amt: int64) {
    (*state).availMem += amt;
}
#[inline]
unsafe fn SERIAL(state: *mut Tuplesortstate) -> bool {
    (*state).shared.is_null()
}
#[inline]
unsafe fn WORKER(state: *mut Tuplesortstate) -> bool {
    !(*state).shared.is_null() && (*state).worker != -1
}
#[inline]
unsafe fn LEADER(state: *mut Tuplesortstate) -> bool {
    !(*state).shared.is_null() && (*state).worker == -1
}

/*
 * Specialized comparators that we can inline into specialized sorts.
 */

/* Used if first key's comparator is ssup_datum_unsigned_cmp */
#[inline(always)]
unsafe fn qsort_tuple_unsigned_compare(
    a: *mut SortTuple,
    b: *mut SortTuple,
    state: *mut Tuplesortstate,
) -> c_int {
    let compare: c_int;

    compare = ApplyUnsignedSortComparator(
        (*a).datum1,
        (*a).isnull1,
        (*b).datum1,
        (*b).isnull1,
        &mut (*(*state).base.sortKeys.add(0)),
    );
    if compare != 0 {
        return compare;
    }

    /*
     * No need to waste effort calling the tiebreak function when there are no
     * other keys to sort on.
     */
    if !(*state).base.onlyKey.is_null() {
        return 0;
    }

    ((*state).base.comparetup_tiebreak)(a, b, state)
}

/* Used if first key's comparator is ssup_datum_signed_cmp */
// #if SIZEOF_DATUM >= 8
#[inline(always)]
unsafe fn qsort_tuple_signed_compare(
    a: *mut SortTuple,
    b: *mut SortTuple,
    state: *mut Tuplesortstate,
) -> c_int {
    let compare: c_int;

    compare = ApplySignedSortComparator(
        (*a).datum1,
        (*a).isnull1,
        (*b).datum1,
        (*b).isnull1,
        &mut (*(*state).base.sortKeys.add(0)),
    );

    if compare != 0 {
        return compare;
    }

    if !(*state).base.onlyKey.is_null() {
        return 0;
    }

    ((*state).base.comparetup_tiebreak)(a, b, state)
}

/* Used if first key's comparator is ssup_datum_int32_cmp */
#[inline(always)]
unsafe fn qsort_tuple_int32_compare(
    a: *mut SortTuple,
    b: *mut SortTuple,
    state: *mut Tuplesortstate,
) -> c_int {
    let compare: c_int;

    compare = ApplyInt32SortComparator(
        (*a).datum1,
        (*a).isnull1,
        (*b).datum1,
        (*b).isnull1,
        &mut (*(*state).base.sortKeys.add(0)),
    );

    if compare != 0 {
        return compare;
    }

    if !(*state).base.onlyKey.is_null() {
        return 0;
    }

    ((*state).base.comparetup_tiebreak)(a, b, state)
}

/*
 * Special versions of qsort just for SortTuple objects.  These were generated
 * by lib/sort_template.h in C.  Here we build them on top of
 * qsort_interruptible() with element-size SortTuple.
 */

// ST_SORT qsort_tuple_unsigned, ST_COMPARE qsort_tuple_unsigned_compare
unsafe fn qsort_tuple_unsigned_cmpwrap(
    a: *const c_void,
    b: *const c_void,
    arg: *mut c_void,
) -> c_int {
    qsort_tuple_unsigned_compare(a as *mut SortTuple, b as *mut SortTuple, arg as *mut Tuplesortstate)
}
unsafe fn qsort_tuple_unsigned(data: *mut SortTuple, n: usize, arg: *mut Tuplesortstate) {
    crate::utils::sort::qsort_interruptible::qsort_interruptible(
        data as *mut c_void,
        n,
        core::mem::size_of::<SortTuple>(),
        qsort_tuple_unsigned_cmpwrap,
        arg as *mut c_void,
    );
}

// #if SIZEOF_DATUM >= 8 -- ST_SORT qsort_tuple_signed
unsafe fn qsort_tuple_signed_cmpwrap(
    a: *const c_void,
    b: *const c_void,
    arg: *mut c_void,
) -> c_int {
    qsort_tuple_signed_compare(a as *mut SortTuple, b as *mut SortTuple, arg as *mut Tuplesortstate)
}
unsafe fn qsort_tuple_signed(data: *mut SortTuple, n: usize, arg: *mut Tuplesortstate) {
    crate::utils::sort::qsort_interruptible::qsort_interruptible(
        data as *mut c_void,
        n,
        core::mem::size_of::<SortTuple>(),
        qsort_tuple_signed_cmpwrap,
        arg as *mut c_void,
    );
}

// ST_SORT qsort_tuple_int32
unsafe fn qsort_tuple_int32_cmpwrap(
    a: *const c_void,
    b: *const c_void,
    arg: *mut c_void,
) -> c_int {
    qsort_tuple_int32_compare(a as *mut SortTuple, b as *mut SortTuple, arg as *mut Tuplesortstate)
}
unsafe fn qsort_tuple_int32(data: *mut SortTuple, n: usize, arg: *mut Tuplesortstate) {
    crate::utils::sort::qsort_interruptible::qsort_interruptible(
        data as *mut c_void,
        n,
        core::mem::size_of::<SortTuple>(),
        qsort_tuple_int32_cmpwrap,
        arg as *mut c_void,
    );
}

// ST_SORT qsort_tuple (ST_COMPARE_RUNTIME_POINTER): comparator passed at call.
unsafe fn qsort_tuple(
    data: *mut SortTuple,
    n: usize,
    cmp: SortTupleComparator,
    arg: *mut Tuplesortstate,
) {
    // The runtime comparator is reached through a thread-local-free shim by
    // smuggling the (cmp, state) pair via a small stack struct pointed to by
    // arg.  Matches sort_template.h's ST_COMPARE_RUNTIME_POINTER expansion.
    struct CmpArg {
        cmp: SortTupleComparator,
        state: *mut Tuplesortstate,
    }
    unsafe fn shim(a: *const c_void, b: *const c_void, arg: *mut c_void) -> c_int {
        let ca = &*(arg as *const CmpArg);
        (ca.cmp)(a as *const SortTuple, b as *const SortTuple, ca.state)
    }
    let mut carg = CmpArg { cmp, state: arg };
    crate::utils::sort::qsort_interruptible::qsort_interruptible(
        data as *mut c_void,
        n,
        core::mem::size_of::<SortTuple>(),
        shim,
        &mut carg as *mut CmpArg as *mut c_void,
    );
}

// ST_SORT qsort_ssup: ST_COMPARE ApplySortComparator on datum1, arg is SortSupportData.
unsafe fn qsort_ssup_cmpwrap(a: *const c_void, b: *const c_void, arg: *mut c_void) -> c_int {
    let a = a as *const SortTuple;
    let b = b as *const SortTuple;
    ApplySortComparator(
        (*a).datum1,
        (*a).isnull1,
        (*b).datum1,
        (*b).isnull1,
        arg as *mut SortSupportData,
    )
}
unsafe fn qsort_ssup(data: *mut SortTuple, n: usize, ssup: *mut SortSupportData) {
    crate::utils::sort::qsort_interruptible::qsort_interruptible(
        data as *mut c_void,
        n,
        core::mem::size_of::<SortTuple>(),
        qsort_ssup_cmpwrap,
        ssup as *mut c_void,
    );
}

// lib/sort_template.h expansion for qsort_ssup completed above.

/*
 *		tuplesort_begin_xxx
 *
 * Initialize for a tuple sort operation.
 *
 * After calling tuplesort_begin, the caller should call tuplesort_putXXX
 * zero or more times, then call tuplesort_performsort when all the tuples
 * have been supplied.  After performsort, retrieve the tuples in sorted
 * order by calling tuplesort_getXXX until it returns false/NULL.  (If random
 * access was requested, rescan, markpos, and restorepos can also be called.)
 * Call tuplesort_end to terminate the operation and release memory/disk space.
 *
 * Each variant of tuplesort_begin has a workMem parameter specifying the
 * maximum number of kilobytes of RAM to use before spilling data to disk.
 * (The normal value of this parameter is work_mem, but some callers use
 * other values.)  Each variant also has a sortopt which is a bitmask of
 * sort options.  See TUPLESORT_* definitions in tuplesort.h
 */

pub unsafe fn tuplesort_begin_common(
    workMem: c_int,
    coordinate: SortCoordinate,
    sortopt: c_int,
) -> *mut Tuplesortstate {
    let state: *mut Tuplesortstate;
    let maincontext: MemoryContext;
    let sortcontext: MemoryContext;
    let oldcontext: MemoryContext;

    /* See leader_takeover_tapes() remarks on random access support */
    if !coordinate.is_null() && (sortopt & TUPLESORT_RANDOMACCESS) != 0 {
        elog!(ERROR, "random access disallowed under parallel sort");
    }

    /*
     * Memory context surviving tuplesort_reset.  This memory context holds
     * data which is useful to keep while sorting multiple similar batches.
     */
    maincontext = AllocSetContextCreate!(
        CurrentMemoryContext,
        c"TupleSort main".as_ptr(),
        ALLOCSET_DEFAULT_SIZES
    );

    /*
     * Create a working memory context for one sort operation.  The content of
     * this context is deleted by tuplesort_reset.
     */
    sortcontext = AllocSetContextCreate!(
        maincontext,
        c"TupleSort sort".as_ptr(),
        ALLOCSET_DEFAULT_SIZES
    );

    /*
     * Additionally a working memory context for tuples is setup in
     * tuplesort_begin_batch.
     */

    /*
     * Make the Tuplesortstate within the per-sortstate context.  This way, we
     * don't need a separate pfree() operation for it at shutdown.
     */
    oldcontext = MemoryContextSwitchTo(maincontext);

    state = palloc0(core::mem::size_of::<Tuplesortstate>()) as *mut Tuplesortstate;

    if trace_sort {
        pg_rusage_init(&mut (*state).ru_start);
    }

    (*state).base.sortopt = sortopt;
    (*state).base.tuples = true;
    (*state).abbrevNext = 10;

    /*
     * workMem is forced to be at least 64KB, the current minimum valid value
     * for the work_mem GUC.  This is a defense against parallel sort callers
     * that divide out memory among many workers in a way that leaves each
     * with very little memory.
     */
    (*state).allowedMem = Max(workMem, 64) as int64 * 1024 as int64;
    (*state).base.sortcontext = sortcontext;
    (*state).base.maincontext = maincontext;

    /*
     * Initial size of array must be more than ALLOCSET_SEPARATE_THRESHOLD;
     * see comments in grow_memtuples().
     */
    (*state).memtupsize = INITIAL_MEMTUPSIZE();
    (*state).memtuples = core::ptr::null_mut();

    /*
     * After all of the other non-parallel-related state, we setup all of the
     * state needed for each batch.
     */
    tuplesort_begin_batch(state);

    /*
     * Initialize parallel-related state based on coordination information
     * from caller
     */
    if coordinate.is_null() {
        /* Serial sort */
        (*state).shared = core::ptr::null_mut();
        (*state).worker = -1;
        (*state).nParticipants = -1;
    } else if (*coordinate).isWorker {
        /* Parallel worker produces exactly one final run from all input */
        (*state).shared = (*coordinate).sharedsort;
        (*state).worker = worker_get_identifier(state);
        (*state).nParticipants = -1;
    } else {
        /* Parallel leader state only used for final merge */
        (*state).shared = (*coordinate).sharedsort;
        (*state).worker = -1;
        (*state).nParticipants = (*coordinate).nParticipants;
        Assert!((*state).nParticipants >= 1);
    }

    MemoryContextSwitchTo(oldcontext);

    state
}

/*
 *		tuplesort_begin_batch
 *
 * Setup, or reset, all state need for processing a new set of tuples with this
 * sort state. Called both from tuplesort_begin_common (the first time sorting
 * with this sort state) and tuplesort_reset (for subsequent usages).
 */
unsafe fn tuplesort_begin_batch(state: *mut Tuplesortstate) {
    let oldcontext: MemoryContext;

    oldcontext = MemoryContextSwitchTo((*state).base.maincontext);

    /*
     * Caller tuple (e.g. IndexTuple) memory context.
     *
     * A dedicated child context used exclusively for caller passed tuples
     * eases memory management.  Resetting at key points reduces
     * fragmentation. Note that the memtuples array of SortTuples is allocated
     * in the parent context, not this context, because there is no need to
     * free memtuples early.  For bounded sorts, tuples may be pfreed in any
     * order, so we use a regular aset.c context so that it can make use of
     * free'd memory.  When the sort is not bounded, we make use of a bump.c
     * context as this keeps allocations more compact with less wastage.
     * Allocations are also slightly more CPU efficient.
     */
    if TupleSortUseBumpTupleCxt((*state).base.sortopt) {
        let (minsz, initsz, maxsz) = ALLOCSET_DEFAULT_SIZES;
        (*state).base.tuplecontext = BumpContextCreate(
            (*state).base.sortcontext,
            c"Caller tuples".as_ptr(),
            minsz,
            initsz,
            maxsz,
        );
    } else {
        (*state).base.tuplecontext = AllocSetContextCreate!(
            (*state).base.sortcontext,
            c"Caller tuples".as_ptr(),
            ALLOCSET_DEFAULT_SIZES
        );
    }

    (*state).status = TSS_INITIAL;
    (*state).bounded = false;
    (*state).boundUsed = false;

    (*state).availMem = (*state).allowedMem;

    (*state).tapeset = core::ptr::null_mut();

    (*state).memtupcount = 0;

    /*
     * Initial size of array must be more than ALLOCSET_SEPARATE_THRESHOLD;
     * see comments in grow_memtuples().
     */
    (*state).growmemtuples = true;
    (*state).slabAllocatorUsed = false;
    if !(*state).memtuples.is_null() && (*state).memtupsize != INITIAL_MEMTUPSIZE() {
        pfree((*state).memtuples as *mut c_void);
        (*state).memtuples = core::ptr::null_mut();
        (*state).memtupsize = INITIAL_MEMTUPSIZE();
    }
    if (*state).memtuples.is_null() {
        (*state).memtuples =
            palloc((*state).memtupsize as usize * core::mem::size_of::<SortTuple>())
                as *mut SortTuple;
        USEMEM(state, GetMemoryChunkSpace((*state).memtuples as *mut c_void) as int64);
    }

    /* workMem must be large enough for the minimal memtuples array */
    if LACKMEM(state) {
        elog!(ERROR, "insufficient memory allowed for sort");
    }

    (*state).currentRun = 0;

    /*
     * Tape variables (inputTapes, outputTapes, etc.) will be initialized by
     * inittapes(), if needed.
     */

    (*state).result_tape = core::ptr::null_mut(); /* flag that result tape has not been formed */

    MemoryContextSwitchTo(oldcontext);
}

/*
 * tuplesort_set_bound
 *
 *	Advise tuplesort that at most the first N result tuples are required.
 *
 * Must be called before inserting any tuples.  (Actually, we could allow it
 * as long as the sort hasn't spilled to disk, but there seems no need for
 * delayed calls at the moment.)
 *
 * This is a hint only. The tuplesort may still return more tuples than
 * requested.  Parallel leader tuplesorts will always ignore the hint.
 */
pub unsafe fn tuplesort_set_bound(state: *mut Tuplesortstate, bound: int64) {
    /* Assert we're called before loading any tuples */
    Assert!((*state).status == TSS_INITIAL && (*state).memtupcount == 0);
    /* Assert we allow bounded sorts */
    Assert!((*state).base.sortopt & TUPLESORT_ALLOWBOUNDED != 0);
    /* Can't set the bound twice, either */
    Assert!(!(*state).bounded);
    /* Also, this shouldn't be called in a parallel worker */
    Assert!(!WORKER(state));

    /* Parallel leader allows but ignores hint */
    if LEADER(state) {
        return;
    }

    // #ifdef DEBUG_BOUNDED_SORT -- not built; optimize_bounded_sort check omitted.

    /* We want to be able to compute bound * 2, so limit the setting */
    if bound > (INT_MAX / 2) as int64 {
        return;
    }

    (*state).bounded = true;
    (*state).bound = bound as c_int;

    /*
     * Bounded sorts are not an effective target for abbreviated key
     * optimization.  Disable by setting state to be consistent with no
     * abbreviation support.
     */
    (*(*state).base.sortKeys).abbrev_converter = None;
    if (*(*state).base.sortKeys).abbrev_full_comparator.is_some() {
        (*(*state).base.sortKeys).comparator = (*(*state).base.sortKeys).abbrev_full_comparator;
    }

    /* Not strictly necessary, but be tidy */
    (*(*state).base.sortKeys).abbrev_abort = None;
    (*(*state).base.sortKeys).abbrev_full_comparator = None;
}

/*
 * tuplesort_used_bound
 *
 * Allow callers to find out if the sort state was able to use a bound.
 */
pub unsafe fn tuplesort_used_bound(state: *mut Tuplesortstate) -> bool {
    (*state).boundUsed
}

/*
 * tuplesort_free
 *
 *	Internal routine for freeing resources of tuplesort.
 */
unsafe fn tuplesort_free(state: *mut Tuplesortstate) {
    /* context swap probably not needed, but let's be safe */
    let oldcontext: MemoryContext = MemoryContextSwitchTo((*state).base.sortcontext);
    let spaceUsed: int64;

    if !(*state).tapeset.is_null() {
        spaceUsed = LogicalTapeSetBlocks((*state).tapeset);
    } else {
        spaceUsed = ((*state).allowedMem - (*state).availMem + 1023) / 1024;
    }

    /*
     * Delete temporary "tape" files, if any.
     *
     * We don't bother to destroy the individual tapes here. They will go away
     * with the sortcontext.  (In TSS_FINALMERGE state, we have closed
     * finished tapes already.)
     */
    if !(*state).tapeset.is_null() {
        LogicalTapeSetClose((*state).tapeset);
    }

    if trace_sort {
        if !(*state).tapeset.is_null() {
            // C also: %s/%d/%PRId64 substitutions.
            elog!(
                LOG,
                "{} of worker {} ended, {} disk blocks used: {}",
                if SERIAL(state) {
                    "external sort"
                } else {
                    "parallel external sort"
                },
                (*state).worker,
                spaceUsed,
                CStr::from_ptr(pg_rusage_show(&mut (*state).ru_start)).to_string_lossy()
            );
        } else {
            elog!(
                LOG,
                "{} of worker {} ended, {} KB used: {}",
                if SERIAL(state) {
                    "internal sort"
                } else {
                    "unperformed parallel sort"
                },
                (*state).worker,
                spaceUsed,
                CStr::from_ptr(pg_rusage_show(&mut (*state).ru_start)).to_string_lossy()
            );
        }
    }

    TRACE_POSTGRESQL_SORT_DONE(!(*state).tapeset.is_null(), spaceUsed);

    FREESTATE(state);
    MemoryContextSwitchTo(oldcontext);

    /*
     * Free the per-sort memory context, thereby releasing all working memory.
     */
    MemoryContextReset((*state).base.sortcontext);
}

/*
 * tuplesort_end
 *
 *	Release resources and clean up.
 *
 * NOTE: after calling this, any pointers returned by tuplesort_getXXX are
 * pointing to garbage.  Be careful not to attempt to use or free such
 * pointers afterwards!
 */
pub unsafe fn tuplesort_end(state: *mut Tuplesortstate) {
    tuplesort_free(state);

    /*
     * Free the main memory context, including the Tuplesortstate struct
     * itself.
     */
    MemoryContextDelete((*state).base.maincontext);
}

/*
 * tuplesort_updatemax
 *
 *	Update maximum resource usage statistics.
 */
unsafe fn tuplesort_updatemax(state: *mut Tuplesortstate) {
    let spaceUsed: int64;
    let isSpaceDisk: bool;

    /*
     * Note: it might seem we should provide both memory and disk usage for a
     * disk-based sort.  However, the current code doesn't track memory space
     * accurately once we have begun to return tuples to the caller (since we
     * don't account for pfree's the caller is expected to do), so we cannot
     * rely on availMem in a disk sort.  This does not seem worth the overhead
     * to fix.  Is it worth creating an API for the memory context code to
     * tell us how much is actually used in sortcontext?
     */
    if !(*state).tapeset.is_null() {
        isSpaceDisk = true;
        spaceUsed = LogicalTapeSetBlocks((*state).tapeset) * BLCKSZ as int64;
    } else {
        isSpaceDisk = false;
        spaceUsed = (*state).allowedMem - (*state).availMem;
    }

    /*
     * Sort evicts data to the disk when it wasn't able to fit that data into
     * main memory.  This is why we assume space used on the disk to be more
     * important for tracking resource usage than space used in memory. Note
     * that the amount of space occupied by some tupleset on the disk might be
     * less than amount of space occupied by the same tupleset in memory due
     * to more compact representation.
     */
    if (isSpaceDisk && !(*state).isMaxSpaceDisk)
        || (isSpaceDisk == (*state).isMaxSpaceDisk && spaceUsed > (*state).maxSpace)
    {
        (*state).maxSpace = spaceUsed;
        (*state).isMaxSpaceDisk = isSpaceDisk;
        (*state).maxSpaceStatus = (*state).status;
    }
}

/*
 * tuplesort_reset
 *
 *	Reset the tuplesort.  Reset all the data in the tuplesort, but leave the
 *	meta-information in.  After tuplesort_reset, tuplesort is ready to start
 *	a new sort.  This allows avoiding recreation of tuple sort states (and
 *	save resources) when sorting multiple small batches.
 */
pub unsafe fn tuplesort_reset(state: *mut Tuplesortstate) {
    tuplesort_updatemax(state);
    tuplesort_free(state);

    /*
     * After we've freed up per-batch memory, re-setup all of the state common
     * to both the first batch and any subsequent batch.
     */
    tuplesort_begin_batch(state);

    (*state).lastReturnedTuple = core::ptr::null_mut();
    (*state).slabMemoryBegin = core::ptr::null_mut();
    (*state).slabMemoryEnd = core::ptr::null_mut();
    (*state).slabFreeHead = core::ptr::null_mut();
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
unsafe fn grow_memtuples(state: *mut Tuplesortstate) -> bool {
    let mut newmemtupsize: c_int;
    let memtupsize: c_int = (*state).memtupsize;
    let memNowUsed: int64 = (*state).allowedMem - (*state).availMem;

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
        if memtupsize < INT_MAX / 2 {
            newmemtupsize = memtupsize * 2;
        } else {
            newmemtupsize = INT_MAX;
            (*state).growmemtuples = false;
        }
    } else {
        /*
         * This will be the last increment of memtupsize.  Abandon doubling
         * strategy and instead increase as much as we safely can.
         *
         * To stay within allowedMem, we can't increase memtupsize by more
         * than availMem / sizeof(SortTuple) elements.  In practice, we want
         * to increase it by considerably less, because we need to leave some
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
        if (memtupsize as f64 * grow_ratio) < INT_MAX as f64 {
            newmemtupsize = (memtupsize as f64 * grow_ratio) as c_int;
        } else {
            newmemtupsize = INT_MAX;
        }

        /* We won't make any further enlargement attempts */
        (*state).growmemtuples = false;
    }

    /* Must enlarge array by at least one element, else report failure */
    if newmemtupsize <= memtupsize {
        // goto noalloc
        (*state).growmemtuples = false;
        return false;
    }

    /*
     * On a 32-bit machine, allowedMem could exceed MaxAllocHugeSize.  Clamp
     * to ensure our request won't be rejected.  Note that we can easily
     * exhaust address space before facing this outcome.  (This is presently
     * impossible due to guc.c's MAX_KILOBYTES limitation on work_mem, but
     * don't rely on that at this distance.)
     */
    if (newmemtupsize as Size) >= MaxAllocHugeSize / core::mem::size_of::<SortTuple>() {
        newmemtupsize = (MaxAllocHugeSize / core::mem::size_of::<SortTuple>()) as c_int;
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
        < ((newmemtupsize - memtupsize) as i64 * core::mem::size_of::<SortTuple>() as i64)
    {
        // goto noalloc
        (*state).growmemtuples = false;
        return false;
    }

    /* OK, do it */
    FREEMEM(state, GetMemoryChunkSpace((*state).memtuples as *mut c_void) as int64);
    (*state).memtupsize = newmemtupsize;
    (*state).memtuples = repalloc_huge(
        (*state).memtuples as *mut c_void,
        (*state).memtupsize as usize * core::mem::size_of::<SortTuple>(),
    ) as *mut SortTuple;
    USEMEM(state, GetMemoryChunkSpace((*state).memtuples as *mut c_void) as int64);
    if LACKMEM(state) {
        elog!(ERROR, "unexpected out-of-memory situation in tuplesort");
    }
    true
}

/*
 * Shared code for tuple and datum cases.
 */
pub unsafe fn tuplesort_puttuple_common(
    state: *mut Tuplesortstate,
    tuple: *mut SortTuple,
    useAbbrev: bool,
    tuplen: Size,
) {
    let oldcontext: MemoryContext = MemoryContextSwitchTo((*state).base.sortcontext);

    Assert!(!LEADER(state));

    /* account for the memory used for this tuple */
    USEMEM(state, tuplen as int64);
    (*state).tupleMem += tuplen as int64;

    if !useAbbrev {
        /*
         * Leave ordinary Datum representation, or NULL value.  If there is a
         * converter it won't expect NULL values, and cost model is not
         * required to account for NULL, so in that case we avoid calling
         * converter and just set datum1 to zeroed representation (to be
         * consistent, and to support cheap inequality tests for NULL
         * abbreviated keys).
         */
    } else if !consider_abort_common(state) {
        /* Store abbreviated key representation */
        (*tuple).datum1 = ((*(*state).base.sortKeys).abbrev_converter.unwrap())(
            (*tuple).datum1,
            (*state).base.sortKeys,
        );
    } else {
        /*
         * Set state to be consistent with never trying abbreviation.
         *
         * Alter datum1 representation in already-copied tuples, so as to
         * ensure a consistent representation (current tuple was just
         * handled).  It does not matter if some dumped tuples are already
         * sorted on tape, since serialized tuples lack abbreviated keys
         * (TSS_BUILDRUNS state prevents control reaching here in any case).
         */
        REMOVEABBREV(state, (*state).memtuples, (*state).memtupcount);
    }

    match (*state).status {
        TSS_INITIAL => {
            /*
             * Save the tuple into the unsorted array.  First, grow the array
             * as needed.  Note that we try to grow the array when there is
             * still one free slot remaining --- if we fail, there'll still be
             * room to store the incoming tuple, and then we'll switch to
             * tape-based operation.
             */
            if (*state).memtupcount >= (*state).memtupsize - 1 {
                let _ = grow_memtuples(state);
                Assert!((*state).memtupcount < (*state).memtupsize);
            }
            *(*state).memtuples.add((*state).memtupcount as usize) = *tuple;
            (*state).memtupcount += 1;

            /*
             * Check if it's time to switch over to a bounded heapsort. We do
             * so if the input tuple count exceeds twice the desired tuple
             * count (this is a heuristic for where heapsort becomes cheaper
             * than a quicksort), or if we've just filled workMem and have
             * enough tuples to meet the bound.
             *
             * Note that once we enter TSS_BOUNDED state we will always try to
             * complete the sort that way.  In the worst case, if later input
             * tuples are larger than earlier ones, this might cause us to
             * exceed workMem significantly.
             */
            if (*state).bounded
                && ((*state).memtupcount > (*state).bound * 2
                    || ((*state).memtupcount > (*state).bound && LACKMEM(state)))
            {
                if trace_sort {
                    elog!(
                        LOG,
                        "switching to bounded heapsort at {} tuples: {}",
                        (*state).memtupcount,
                        CStr::from_ptr(pg_rusage_show(&mut (*state).ru_start)).to_string_lossy()
                    );
                }
                make_bounded_heap(state);
                MemoryContextSwitchTo(oldcontext);
                return;
            }

            /*
             * Done if we still fit in available memory and have array slots.
             */
            if (*state).memtupcount < (*state).memtupsize && !LACKMEM(state) {
                MemoryContextSwitchTo(oldcontext);
                return;
            }

            /*
             * Nope; time to switch to tape-based operation.
             */
            inittapes(state, true);

            /*
             * Dump all tuples.
             */
            dumptuples(state, false);
        }

        TSS_BOUNDED => {
            /*
             * We don't want to grow the array here, so check whether the new
             * tuple can be discarded before putting it in.  This should be a
             * good speed optimization, too, since when there are many more
             * input tuples than the bound, most input tuples can be discarded
             * with just this one comparison.  Note that because we currently
             * have the sort direction reversed, we must check for <= not >=.
             */
            if COMPARETUP(state, tuple, (*state).memtuples.add(0)) <= 0 {
                /* new tuple <= top of the heap, so we can discard it */
                free_sort_tuple(state, tuple);
                CHECK_FOR_INTERRUPTS!();
            } else {
                /* discard top of heap, replacing it with the new tuple */
                free_sort_tuple(state, (*state).memtuples.add(0));
                tuplesort_heap_replace_top(state, tuple);
            }
        }

        TSS_BUILDRUNS => {
            /*
             * Save the tuple into the unsorted array (there must be space)
             */
            *(*state).memtuples.add((*state).memtupcount as usize) = *tuple;
            (*state).memtupcount += 1;

            /*
             * If we are over the memory limit, dump all tuples.
             */
            dumptuples(state, false);
        }

        _ => {
            elog!(ERROR, "invalid tuplesort state");
        }
    }
    MemoryContextSwitchTo(oldcontext);
}

unsafe fn consider_abort_common(state: *mut Tuplesortstate) -> bool {
    Assert!((*(*state).base.sortKeys.add(0)).abbrev_converter.is_some());
    Assert!((*(*state).base.sortKeys.add(0)).abbrev_abort.is_some());
    Assert!((*(*state).base.sortKeys.add(0)).abbrev_full_comparator.is_some());

    /*
     * Check effectiveness of abbreviation optimization.  Consider aborting
     * when still within memory limit.
     */
    if (*state).status == TSS_INITIAL && (*state).memtupcount as int64 >= (*state).abbrevNext {
        (*state).abbrevNext *= 2;

        /*
         * Check opclass-supplied abbreviation abort routine.  It may indicate
         * that abbreviation should not proceed.
         */
        if !((*(*state).base.sortKeys).abbrev_abort.unwrap())(
            (*state).memtupcount,
            (*state).base.sortKeys,
        ) {
            return false;
        }

        /*
         * Finally, restore authoritative comparator, and indicate that
         * abbreviation is not in play by setting abbrev_converter to NULL
         */
        (*(*state).base.sortKeys.add(0)).comparator =
            (*(*state).base.sortKeys.add(0)).abbrev_full_comparator;
        (*(*state).base.sortKeys.add(0)).abbrev_converter = None;
        /* Not strictly necessary, but be tidy */
        (*(*state).base.sortKeys.add(0)).abbrev_abort = None;
        (*(*state).base.sortKeys.add(0)).abbrev_full_comparator = None;

        /* Give up - expect original pass-by-value representation */
        return true;
    }

    false
}

/*
 * All tuples have been provided; finish the sort.
 */
pub unsafe fn tuplesort_performsort(state: *mut Tuplesortstate) {
    let oldcontext: MemoryContext = MemoryContextSwitchTo((*state).base.sortcontext);

    if trace_sort {
        elog!(
            LOG,
            "performsort of worker {} starting: {}",
            (*state).worker,
            CStr::from_ptr(pg_rusage_show(&mut (*state).ru_start)).to_string_lossy()
        );
    }

    match (*state).status {
        TSS_INITIAL => {
            /*
             * We were able to accumulate all the tuples within the allowed
             * amount of memory, or leader to take over worker tapes
             */
            if SERIAL(state) {
                /* Just qsort 'em and we're done */
                tuplesort_sort_memtuples(state);
                (*state).status = TSS_SORTEDINMEM;
            } else if WORKER(state) {
                /*
                 * Parallel workers must still dump out tuples to tape.  No
                 * merge is required to produce single output run, though.
                 */
                inittapes(state, false);
                dumptuples(state, true);
                worker_nomergeruns(state);
                (*state).status = TSS_SORTEDONTAPE;
            } else {
                /*
                 * Leader will take over worker tapes and merge worker runs.
                 * Note that mergeruns sets the correct state->status.
                 */
                leader_takeover_tapes(state);
                mergeruns(state);
            }
            (*state).current = 0;
            (*state).eof_reached = false;
            (*state).markpos_block = 0;
            (*state).markpos_offset = 0;
            (*state).markpos_eof = false;
        }

        TSS_BOUNDED => {
            /*
             * We were able to accumulate all the tuples required for output
             * in memory, using a heap to eliminate excess tuples.  Now we
             * have to transform the heap to a properly-sorted array. Note
             * that sort_bounded_heap sets the correct state->status.
             */
            sort_bounded_heap(state);
            (*state).current = 0;
            (*state).eof_reached = false;
            (*state).markpos_offset = 0;
            (*state).markpos_eof = false;
        }

        TSS_BUILDRUNS => {
            /*
             * Finish tape-based sort.  First, flush all tuples remaining in
             * memory out to tape; then merge until we have a single remaining
             * run (or, if !randomAccess and !WORKER(), one run per tape).
             * Note that mergeruns sets the correct state->status.
             */
            dumptuples(state, true);
            mergeruns(state);
            (*state).eof_reached = false;
            (*state).markpos_block = 0;
            (*state).markpos_offset = 0;
            (*state).markpos_eof = false;
        }

        _ => {
            elog!(ERROR, "invalid tuplesort state");
        }
    }

    if trace_sort {
        if (*state).status == TSS_FINALMERGE {
            elog!(
                LOG,
                "performsort of worker {} done (except {}-way final merge): {}",
                (*state).worker,
                (*state).nInputTapes,
                CStr::from_ptr(pg_rusage_show(&mut (*state).ru_start)).to_string_lossy()
            );
        } else {
            elog!(
                LOG,
                "performsort of worker {} done: {}",
                (*state).worker,
                CStr::from_ptr(pg_rusage_show(&mut (*state).ru_start)).to_string_lossy()
            );
        }
    }

    MemoryContextSwitchTo(oldcontext);
}

/*
 * Internal routine to fetch the next tuple in either forward or back
 * direction into *stup.  Returns false if no more tuples.
 * Returned tuple belongs to tuplesort memory context, and must not be freed
 * by caller.  Note that fetched tuple is stored in memory that may be
 * recycled by any future fetch.
 */
pub unsafe fn tuplesort_gettuple_common(
    state: *mut Tuplesortstate,
    forward: bool,
    stup: *mut SortTuple,
) -> bool {
    let mut tuplen: c_uint;
    let mut nmoved: usize;

    Assert!(!WORKER(state));

    match (*state).status {
        TSS_SORTEDINMEM => {
            Assert!(forward || (*state).base.sortopt & TUPLESORT_RANDOMACCESS != 0);
            Assert!(!(*state).slabAllocatorUsed);
            if forward {
                if (*state).current < (*state).memtupcount {
                    *stup = *(*state).memtuples.add((*state).current as usize);
                    (*state).current += 1;
                    return true;
                }
                (*state).eof_reached = true;

                /*
                 * Complain if caller tries to retrieve more tuples than
                 * originally asked for in a bounded sort.  This is because
                 * returning EOF here might be the wrong thing.
                 */
                if (*state).bounded && (*state).current >= (*state).bound {
                    elog!(ERROR, "retrieved too many tuples in a bounded sort");
                }

                false
            } else {
                if (*state).current <= 0 {
                    return false;
                }

                /*
                 * if all tuples are fetched already then we return last
                 * tuple, else - tuple before last returned.
                 */
                if (*state).eof_reached {
                    (*state).eof_reached = false;
                } else {
                    (*state).current -= 1; /* last returned tuple */
                    if (*state).current <= 0 {
                        return false;
                    }
                }
                *stup = *(*state).memtuples.add(((*state).current - 1) as usize);
                true
            }
        }

        TSS_SORTEDONTAPE => {
            Assert!(forward || (*state).base.sortopt & TUPLESORT_RANDOMACCESS != 0);
            Assert!((*state).slabAllocatorUsed);

            /*
             * The slot that held the tuple that we returned in previous
             * gettuple call can now be reused.
             */
            if !(*state).lastReturnedTuple.is_null() {
                RELEASE_SLAB_SLOT(state, (*state).lastReturnedTuple);
                (*state).lastReturnedTuple = core::ptr::null_mut();
            }

            if forward {
                if (*state).eof_reached {
                    return false;
                }

                tuplen = getlen((*state).result_tape, true);
                if tuplen != 0 {
                    READTUP(state, stup, (*state).result_tape, tuplen);

                    /*
                     * Remember the tuple we return, so that we can recycle
                     * its memory on next call.  (This can be NULL, in the
                     * !state->tuples case).
                     */
                    (*state).lastReturnedTuple = (*stup).tuple;

                    return true;
                } else {
                    (*state).eof_reached = true;
                    return false;
                }
            }

            /*
             * Backward.
             *
             * if all tuples are fetched already then we return last tuple,
             * else - tuple before last returned.
             */
            if (*state).eof_reached {
                /*
                 * Seek position is pointing just past the zero tuplen at the
                 * end of file; back up to fetch last tuple's ending length
                 * word.  If seek fails we must have a completely empty file.
                 */
                nmoved = LogicalTapeBackspace(
                    (*state).result_tape,
                    2 * core::mem::size_of::<c_uint>(),
                );
                if nmoved == 0 {
                    return false;
                } else if nmoved != 2 * core::mem::size_of::<c_uint>() {
                    elog!(ERROR, "unexpected tape position");
                }
                (*state).eof_reached = false;
            } else {
                /*
                 * Back up and fetch previously-returned tuple's ending length
                 * word.  If seek fails, assume we are at start of file.
                 */
                nmoved =
                    LogicalTapeBackspace((*state).result_tape, core::mem::size_of::<c_uint>());
                if nmoved == 0 {
                    return false;
                } else if nmoved != core::mem::size_of::<c_uint>() {
                    elog!(ERROR, "unexpected tape position");
                }
                tuplen = getlen((*state).result_tape, false);

                /*
                 * Back up to get ending length word of tuple before it.
                 */
                nmoved = LogicalTapeBackspace(
                    (*state).result_tape,
                    tuplen as usize + 2 * core::mem::size_of::<c_uint>(),
                );
                if nmoved == tuplen as usize + core::mem::size_of::<c_uint>() {
                    /*
                     * We backed up over the previous tuple, but there was no
                     * ending length word before it.  That means that the prev
                     * tuple is the first tuple in the file.  It is now the
                     * next to read in forward direction (not obviously right,
                     * but that is what in-memory case does).
                     */
                    return false;
                } else if nmoved != tuplen as usize + 2 * core::mem::size_of::<c_uint>() {
                    elog!(ERROR, "bogus tuple length in backward scan");
                }
            }

            tuplen = getlen((*state).result_tape, false);

            /*
             * Now we have the length of the prior tuple, back up and read it.
             * Note: READTUP expects we are positioned after the initial
             * length word of the tuple, so back up to that point.
             */
            nmoved = LogicalTapeBackspace((*state).result_tape, tuplen as usize);
            if nmoved != tuplen as usize {
                elog!(ERROR, "bogus tuple length in backward scan");
            }
            READTUP(state, stup, (*state).result_tape, tuplen);

            /*
             * Remember the tuple we return, so that we can recycle its memory
             * on next call. (This can be NULL, in the Datum case).
             */
            (*state).lastReturnedTuple = (*stup).tuple;

            true
        }

        TSS_FINALMERGE => {
            Assert!(forward);
            /* We are managing memory ourselves, with the slab allocator. */
            Assert!((*state).slabAllocatorUsed);

            /*
             * The slab slot holding the tuple that we returned in previous
             * gettuple call can now be reused.
             */
            if !(*state).lastReturnedTuple.is_null() {
                RELEASE_SLAB_SLOT(state, (*state).lastReturnedTuple);
                (*state).lastReturnedTuple = core::ptr::null_mut();
            }

            /*
             * This code should match the inner loop of mergeonerun().
             */
            if (*state).memtupcount > 0 {
                let srcTapeIndex: c_int = (*(*state).memtuples.add(0)).srctape;
                let srcTape: *mut LogicalTape = *(*state).inputTapes.add(srcTapeIndex as usize);
                let mut newtup: SortTuple = core::mem::zeroed();

                *stup = *(*state).memtuples.add(0);

                /*
                 * Remember the tuple we return, so that we can recycle its
                 * memory on next call. (This can be NULL, in the Datum case).
                 */
                (*state).lastReturnedTuple = (*stup).tuple;

                /*
                 * Pull next tuple from tape, and replace the returned tuple
                 * at top of the heap with it.
                 */
                if !mergereadnext(state, srcTape, &mut newtup) {
                    /*
                     * If no more data, we've reached end of run on this tape.
                     * Remove the top node from the heap.
                     */
                    tuplesort_heap_delete_top(state);
                    (*state).nInputRuns -= 1;

                    /*
                     * Close the tape.  It'd go away at the end of the sort
                     * anyway, but better to release the memory early.
                     */
                    LogicalTapeClose(srcTape);
                    return true;
                }
                newtup.srctape = srcTapeIndex;
                tuplesort_heap_replace_top(state, &mut newtup);
                return true;
            }
            false
        }

        _ => {
            elog!(ERROR, "invalid tuplesort state");
            #[allow(unreachable_code)]
            false /* keep compiler quiet */
        }
    }
}

/*
 * Advance over N tuples in either forward or back direction,
 * without returning any data.  N==0 is a no-op.
 * Returns true if successful, false if ran out of tuples.
 */
pub unsafe fn tuplesort_skiptuples(
    state: *mut Tuplesortstate,
    mut ntuples: int64,
    forward: bool,
) -> bool {
    let oldcontext: MemoryContext;

    /*
     * We don't actually support backwards skip yet, because no callers need
     * it.  The API is designed to allow for that later, though.
     */
    Assert!(forward);
    Assert!(ntuples >= 0);
    Assert!(!WORKER(state));

    match (*state).status {
        TSS_SORTEDINMEM => {
            if ((*state).memtupcount - (*state).current) as int64 >= ntuples {
                (*state).current += ntuples as c_int;
                return true;
            }
            (*state).current = (*state).memtupcount;
            (*state).eof_reached = true;

            /*
             * Complain if caller tries to retrieve more tuples than
             * originally asked for in a bounded sort.  This is because
             * returning EOF here might be the wrong thing.
             */
            if (*state).bounded && (*state).current >= (*state).bound {
                elog!(ERROR, "retrieved too many tuples in a bounded sort");
            }

            false
        }

        TSS_SORTEDONTAPE | TSS_FINALMERGE => {
            /*
             * We could probably optimize these cases better, but for now it's
             * not worth the trouble.
             */
            oldcontext = MemoryContextSwitchTo((*state).base.sortcontext);
            while {
                let old = ntuples;
                ntuples -= 1;
                old > 0
            } {
                let mut stup: SortTuple = core::mem::zeroed();

                if !tuplesort_gettuple_common(state, forward, &mut stup) {
                    MemoryContextSwitchTo(oldcontext);
                    return false;
                }
                CHECK_FOR_INTERRUPTS!();
            }
            MemoryContextSwitchTo(oldcontext);
            true
        }

        _ => {
            elog!(ERROR, "invalid tuplesort state");
            #[allow(unreachable_code)]
            false /* keep compiler quiet */
        }
    }
}

/*
 * tuplesort_merge_order - report merge order we'll use for given memory
 * (note: "merge order" just means the number of input tapes in the merge).
 *
 * This is exported for use by the planner.  allowedMem is in bytes.
 */
pub fn tuplesort_merge_order(allowedMem: int64) -> c_int {
    let mut mOrder: c_int;

    /*----------
     * In the merge phase, we need buffer space for each input and output tape.
     * Each pass in the balanced merge algorithm reads from M input tapes, and
     * writes to N output tapes.  Each tape consumes TAPE_BUFFER_OVERHEAD bytes
     * of memory.  In addition to that, we want MERGE_BUFFER_SIZE workspace per
     * input tape.
     *
     * totalMem = M * (TAPE_BUFFER_OVERHEAD + MERGE_BUFFER_SIZE) +
     *            N * TAPE_BUFFER_OVERHEAD
     *
     * Except for the last and next-to-last merge passes, where there can be
     * fewer tapes left to process, M = N.  We choose M so that we have the
     * desired amount of memory available for the input buffers
     * (TAPE_BUFFER_OVERHEAD + MERGE_BUFFER_SIZE), given the total memory
     * available for the tape buffers (allowedMem).
     *
     * Note: you might be thinking we need to account for the memtuples[]
     * array in this calculation, but we effectively treat that as part of the
     * MERGE_BUFFER_SIZE workspace.
     *----------
     */
    mOrder = (allowedMem / (2 * TAPE_BUFFER_OVERHEAD + MERGE_BUFFER_SIZE)) as c_int;

    /*
     * Even in minimum memory, use at least a MINORDER merge.  On the other
     * hand, even when we have lots of memory, do not use more than a MAXORDER
     * merge.  Tapes are pretty cheap, but they're not entirely free.  Each
     * additional tape reduces the amount of memory available to build runs,
     * which in turn can cause the same sort to need more runs, which makes
     * merging slower even if it can still be done in a single pass.  Also,
     * high order merges are quite slow due to CPU cache effects; it can be
     * faster to pay the I/O cost of a multi-pass merge than to perform a
     * single merge pass across many hundreds of tapes.
     */
    mOrder = Max(mOrder, MINORDER);
    mOrder = Min(mOrder, MAXORDER);

    mOrder
}

/*
 * Helper function to calculate how much memory to allocate for the read buffer
 * of each input tape in a merge pass.
 *
 * 'avail_mem' is the amount of memory available for the buffers of all the
 *		tapes, both input and output.
 * 'nInputTapes' and 'nInputRuns' are the number of input tapes and runs.
 * 'maxOutputTapes' is the max. number of output tapes we should produce.
 */
fn merge_read_buffer_size(
    avail_mem: int64,
    nInputTapes: c_int,
    nInputRuns: c_int,
    maxOutputTapes: c_int,
) -> int64 {
    let nOutputRuns: c_int;
    let nOutputTapes: c_int;

    /*
     * How many output tapes will we produce in this pass?
     *
     * This is nInputRuns / nInputTapes, rounded up.
     */
    nOutputRuns = (nInputRuns + nInputTapes - 1) / nInputTapes;

    nOutputTapes = Min(nOutputRuns, maxOutputTapes);

    /*
     * Each output tape consumes TAPE_BUFFER_OVERHEAD bytes of memory.  All
     * remaining memory is divided evenly between the input tapes.
     *
     * This also follows from the formula in tuplesort_merge_order, but here
     * we derive the input buffer size from the amount of memory available,
     * and M and N.
     */
    Max(
        (avail_mem - TAPE_BUFFER_OVERHEAD * nOutputTapes as int64) / nInputTapes as int64,
        0,
    )
}

/*
 * inittapes - initialize for tape sorting.
 *
 * This is called only if we have found we won't sort in memory.
 */
unsafe fn inittapes(state: *mut Tuplesortstate, mergeruns: bool) {
    Assert!(!LEADER(state));

    if mergeruns {
        /* Compute number of input tapes to use when merging */
        (*state).maxTapes = tuplesort_merge_order((*state).allowedMem);
    } else {
        /* Workers can sometimes produce single run, output without merge */
        Assert!(WORKER(state));
        (*state).maxTapes = MINORDER;
    }

    if trace_sort {
        elog!(
            LOG,
            "worker {} switching to external sort with {} tapes: {}",
            (*state).worker,
            (*state).maxTapes,
            CStr::from_ptr(pg_rusage_show(&mut (*state).ru_start)).to_string_lossy()
        );
    }

    /* Create the tape set */
    inittapestate(state, (*state).maxTapes);
    (*state).tapeset = LogicalTapeSetCreate(
        false,
        if !(*state).shared.is_null() {
            &mut (*(*state).shared).fileset
        } else {
            core::ptr::null_mut()
        },
        (*state).worker,
    );

    (*state).currentRun = 0;

    /*
     * Initialize logical tape arrays.
     */
    (*state).inputTapes = core::ptr::null_mut();
    (*state).nInputTapes = 0;
    (*state).nInputRuns = 0;

    (*state).outputTapes =
        palloc0((*state).maxTapes as usize * core::mem::size_of::<*mut LogicalTape>())
            as *mut *mut LogicalTape;
    (*state).nOutputTapes = 0;
    (*state).nOutputRuns = 0;

    (*state).status = TSS_BUILDRUNS;

    selectnewtape(state);
}

/*
 * inittapestate - initialize generic tape management state
 */
unsafe fn inittapestate(state: *mut Tuplesortstate, maxTapes: c_int) {
    let tapeSpace: int64;

    /*
     * Decrease availMem to reflect the space needed for tape buffers; but
     * don't decrease it to the point that we have no room for tuples. (That
     * case is only likely to occur if sorting pass-by-value Datums; in all
     * other scenarios the memtuples[] array is unlikely to occupy more than
     * half of allowedMem.  In the pass-by-value case it's not important to
     * account for tuple space, so we don't care if LACKMEM becomes
     * inaccurate.)
     */
    tapeSpace = maxTapes as int64 * TAPE_BUFFER_OVERHEAD;

    if tapeSpace + (GetMemoryChunkSpace((*state).memtuples as *mut c_void) as int64) < (*state).allowedMem {
        USEMEM(state, tapeSpace);
    }

    /*
     * Make sure that the temp file(s) underlying the tape set are created in
     * suitable temp tablespaces.  For parallel sorts, this should have been
     * called already, but it doesn't matter if it is called a second time.
     */
    PrepareTempTablespaces();
}

/*
 * selectnewtape -- select next tape to output to.
 *
 * This is called after finishing a run when we know another run
 * must be started.  This is used both when building the initial
 * runs, and during merge passes.
 */
unsafe fn selectnewtape(state: *mut Tuplesortstate) {
    /*
     * At the beginning of each merge pass, nOutputTapes and nOutputRuns are
     * both zero.  On each call, we create a new output tape to hold the next
     * run, until maxTapes is reached.  After that, we assign new runs to the
     * existing tapes in a round robin fashion.
     */
    if (*state).nOutputTapes < (*state).maxTapes {
        /* Create a new tape to hold the next run */
        Assert!((*(*state).outputTapes.add((*state).nOutputRuns as usize)).is_null());
        Assert!((*state).nOutputRuns == (*state).nOutputTapes);
        (*state).destTape = LogicalTapeCreate((*state).tapeset);
        *(*state).outputTapes.add((*state).nOutputTapes as usize) = (*state).destTape;
        (*state).nOutputTapes += 1;
        (*state).nOutputRuns += 1;
    } else {
        /*
         * We have reached the max number of tapes.  Append to an existing
         * tape.
         */
        (*state).destTape =
            *(*state).outputTapes.add(((*state).nOutputRuns % (*state).nOutputTapes) as usize);
        (*state).nOutputRuns += 1;
    }
}

/*
 * Initialize the slab allocation arena, for the given number of slots.
 */
unsafe fn init_slab_allocator(state: *mut Tuplesortstate, numSlots: c_int) {
    if numSlots > 0 {
        let mut p: *mut c_char;
        let mut i: c_int;

        (*state).slabMemoryBegin = palloc(numSlots as usize * SLAB_SLOT_SIZE) as *mut c_char;
        (*state).slabMemoryEnd =
            (*state).slabMemoryBegin.add(numSlots as usize * SLAB_SLOT_SIZE);
        (*state).slabFreeHead = (*state).slabMemoryBegin as *mut SlabSlot;
        USEMEM(state, (numSlots as int64) * SLAB_SLOT_SIZE as int64);

        p = (*state).slabMemoryBegin;
        i = 0;
        while i < numSlots - 1 {
            (*(p as *mut SlabSlot)).nextfree = p.add(SLAB_SLOT_SIZE) as *mut SlabSlot;
            p = p.add(SLAB_SLOT_SIZE);
            i += 1;
        }
        (*(p as *mut SlabSlot)).nextfree = core::ptr::null_mut();
    } else {
        (*state).slabMemoryBegin = core::ptr::null_mut();
        (*state).slabMemoryEnd = core::ptr::null_mut();
        (*state).slabFreeHead = core::ptr::null_mut();
    }
    (*state).slabAllocatorUsed = true;
}

/*
 * mergeruns -- merge all the completed initial runs.
 *
 * This implements the Balanced k-Way Merge Algorithm.  All input data has
 * already been written to initial runs on tape (see dumptuples).
 */
unsafe fn mergeruns(state: *mut Tuplesortstate) {
    let mut tapenum: c_int;

    Assert!((*state).status == TSS_BUILDRUNS);
    Assert!((*state).memtupcount == 0);

    if !(*state).base.sortKeys.is_null()
        && (*(*state).base.sortKeys).abbrev_converter.is_some()
    {
        /*
         * If there are multiple runs to be merged, when we go to read back
         * tuples from disk, abbreviated keys will not have been stored, and
         * we don't care to regenerate them.  Disable abbreviation from this
         * point on.
         */
        (*(*state).base.sortKeys).abbrev_converter = None;
        (*(*state).base.sortKeys).comparator = (*(*state).base.sortKeys).abbrev_full_comparator;

        /* Not strictly necessary, but be tidy */
        (*(*state).base.sortKeys).abbrev_abort = None;
        (*(*state).base.sortKeys).abbrev_full_comparator = None;
    }

    /*
     * Reset tuple memory.  We've freed all the tuples that we previously
     * allocated.  We will use the slab allocator from now on.
     */
    MemoryContextResetOnly((*state).base.tuplecontext);

    /*
     * We no longer need a large memtuples array.  (We will allocate a smaller
     * one for the heap later.)
     */
    FREEMEM(state, GetMemoryChunkSpace((*state).memtuples as *mut c_void) as int64);
    pfree((*state).memtuples as *mut c_void);
    (*state).memtuples = core::ptr::null_mut();

    /*
     * Initialize the slab allocator.  We need one slab slot per input tape,
     * for the tuples in the heap, plus one to hold the tuple last returned
     * from tuplesort_gettuple.  (If we're sorting pass-by-val Datums,
     * however, we don't need to do allocate anything.)
     *
     * In a multi-pass merge, we could shrink this allocation for the last
     * merge pass, if it has fewer tapes than previous passes, but we don't
     * bother.
     *
     * From this point on, we no longer use the USEMEM()/LACKMEM() mechanism
     * to track memory usage of individual tuples.
     */
    if (*state).base.tuples {
        init_slab_allocator(state, (*state).nOutputTapes + 1);
    } else {
        init_slab_allocator(state, 0);
    }

    /*
     * Allocate a new 'memtuples' array, for the heap.  It will hold one tuple
     * from each input tape.
     *
     * We could shrink this, too, between passes in a multi-pass merge, but we
     * don't bother.  (The initial input tapes are still in outputTapes.  The
     * number of input tapes will not increase between passes.)
     */
    (*state).memtupsize = (*state).nOutputTapes;
    (*state).memtuples = MemoryContextAlloc(
        (*state).base.maincontext,
        (*state).nOutputTapes as usize * core::mem::size_of::<SortTuple>(),
    ) as *mut SortTuple;
    USEMEM(state, GetMemoryChunkSpace((*state).memtuples as *mut c_void) as int64);

    /*
     * Use all the remaining memory we have available for tape buffers among
     * all the input tapes.  At the beginning of each merge pass, we will
     * divide this memory between the input and output tapes in the pass.
     */
    (*state).tape_buffer_mem = (*state).availMem as usize;
    USEMEM(state, (*state).tape_buffer_mem as int64);
    if trace_sort {
        elog!(
            LOG,
            "worker {} using {} KB of memory for tape buffers",
            (*state).worker,
            (*state).tape_buffer_mem / 1024
        );
    }

    loop {
        /*
         * On the first iteration, or if we have read all the runs from the
         * input tapes in a multi-pass merge, it's time to start a new pass.
         * Rewind all the output tapes, and make them inputs for the next
         * pass.
         */
        if (*state).nInputRuns == 0 {
            let input_buffer_size: int64;

            /* Close the old, emptied, input tapes */
            if (*state).nInputTapes > 0 {
                tapenum = 0;
                while tapenum < (*state).nInputTapes {
                    LogicalTapeClose(*(*state).inputTapes.add(tapenum as usize));
                    tapenum += 1;
                }
                pfree((*state).inputTapes as *mut c_void);
            }

            /* Previous pass's outputs become next pass's inputs. */
            (*state).inputTapes = (*state).outputTapes;
            (*state).nInputTapes = (*state).nOutputTapes;
            (*state).nInputRuns = (*state).nOutputRuns;

            /*
             * Reset output tape variables.  The actual LogicalTapes will be
             * created as needed, here we only allocate the array to hold
             * them.
             */
            (*state).outputTapes = palloc0(
                (*state).nInputTapes as usize * core::mem::size_of::<*mut LogicalTape>(),
            ) as *mut *mut LogicalTape;
            (*state).nOutputTapes = 0;
            (*state).nOutputRuns = 0;

            /*
             * Redistribute the memory allocated for tape buffers, among the
             * new input and output tapes.
             */
            input_buffer_size = merge_read_buffer_size(
                (*state).tape_buffer_mem as int64,
                (*state).nInputTapes,
                (*state).nInputRuns,
                (*state).maxTapes,
            );

            if trace_sort {
                elog!(
                    LOG,
                    "starting merge pass of {} input runs on {} tapes, {} KB of memory for each input tape: {}",
                    (*state).nInputRuns,
                    (*state).nInputTapes,
                    input_buffer_size / 1024,
                    CStr::from_ptr(pg_rusage_show(&mut (*state).ru_start)).to_string_lossy()
                );
            }

            /* Prepare the new input tapes for merge pass. */
            tapenum = 0;
            while tapenum < (*state).nInputTapes {
                LogicalTapeRewindForRead(
                    *(*state).inputTapes.add(tapenum as usize),
                    input_buffer_size as usize,
                );
                tapenum += 1;
            }

            /*
             * If there's just one run left on each input tape, then only one
             * merge pass remains.  If we don't have to produce a materialized
             * sorted tape, we can stop at this point and do the final merge
             * on-the-fly.
             */
            if ((*state).base.sortopt & TUPLESORT_RANDOMACCESS) == 0
                && (*state).nInputRuns <= (*state).nInputTapes
                && !WORKER(state)
            {
                /* Tell logtape.c we won't be writing anymore */
                LogicalTapeSetForgetFreeSpace((*state).tapeset);
                /* Initialize for the final merge pass */
                beginmerge(state);
                (*state).status = TSS_FINALMERGE;
                return;
            }
        }

        /* Select an output tape */
        selectnewtape(state);

        /* Merge one run from each input tape. */
        mergeonerun(state);

        /*
         * If the input tapes are empty, and we output only one output run,
         * we're done.  The current output tape contains the final result.
         */
        if (*state).nInputRuns == 0 && (*state).nOutputRuns <= 1 {
            break;
        }
    }

    /*
     * Done.  The result is on a single run on a single tape.
     */
    (*state).result_tape = *(*state).outputTapes.add(0);
    if !WORKER(state) {
        LogicalTapeFreeze((*state).result_tape, core::ptr::null_mut());
    } else {
        worker_freeze_result_tape(state);
    }
    (*state).status = TSS_SORTEDONTAPE;

    /* Close all the now-empty input tapes, to release their read buffers. */
    tapenum = 0;
    while tapenum < (*state).nInputTapes {
        LogicalTapeClose(*(*state).inputTapes.add(tapenum as usize));
        tapenum += 1;
    }
}

/*
 * Merge one run from each input tape.
 */
unsafe fn mergeonerun(state: *mut Tuplesortstate) {
    let mut srcTapeIndex: c_int;
    let mut srcTape: *mut LogicalTape;

    /*
     * Start the merge by loading one tuple from each active source tape into
     * the heap.
     */
    beginmerge(state);

    Assert!((*state).slabAllocatorUsed);

    /*
     * Execute merge by repeatedly extracting lowest tuple in heap, writing it
     * out, and replacing it with next tuple from same tape (if there is
     * another one).
     */
    while (*state).memtupcount > 0 {
        let mut stup: SortTuple = core::mem::zeroed();

        /* write the tuple to destTape */
        srcTapeIndex = (*(*state).memtuples.add(0)).srctape;
        srcTape = *(*state).inputTapes.add(srcTapeIndex as usize);
        WRITETUP(state, (*state).destTape, (*state).memtuples.add(0));

        /* recycle the slot of the tuple we just wrote out, for the next read */
        if !(*(*state).memtuples.add(0)).tuple.is_null() {
            RELEASE_SLAB_SLOT(state, (*(*state).memtuples.add(0)).tuple);
        }

        /*
         * pull next tuple from the tape, and replace the written-out tuple in
         * the heap with it.
         */
        if mergereadnext(state, srcTape, &mut stup) {
            stup.srctape = srcTapeIndex;
            tuplesort_heap_replace_top(state, &mut stup);
        } else {
            tuplesort_heap_delete_top(state);
            (*state).nInputRuns -= 1;
        }
    }

    /*
     * When the heap empties, we're done.  Write an end-of-run marker on the
     * output tape.
     */
    markrunend((*state).destTape);
}

/*
 * beginmerge - initialize for a merge pass
 *
 * Fill the merge heap with the first tuple from each input tape.
 */
unsafe fn beginmerge(state: *mut Tuplesortstate) {
    let activeTapes: c_int;
    let mut srcTapeIndex: c_int;

    /* Heap should be empty here */
    Assert!((*state).memtupcount == 0);

    activeTapes = Min((*state).nInputTapes, (*state).nInputRuns);

    srcTapeIndex = 0;
    while srcTapeIndex < activeTapes {
        let mut tup: SortTuple = core::mem::zeroed();

        if mergereadnext(state, *(*state).inputTapes.add(srcTapeIndex as usize), &mut tup) {
            tup.srctape = srcTapeIndex;
            tuplesort_heap_insert(state, &mut tup);
        }
        srcTapeIndex += 1;
    }
}

/*
 * mergereadnext - read next tuple from one merge input tape
 *
 * Returns false on EOF.
 */
unsafe fn mergereadnext(
    state: *mut Tuplesortstate,
    srcTape: *mut LogicalTape,
    stup: *mut SortTuple,
) -> bool {
    let tuplen: c_uint;

    /* read next tuple, if any */
    tuplen = getlen(srcTape, true);
    if tuplen == 0 {
        return false;
    }
    READTUP(state, stup, srcTape, tuplen);

    true
}

/*
 * dumptuples - remove tuples from memtuples and write initial run to tape
 *
 * When alltuples = true, dump everything currently in memory.  (This case is
 * only used at end of input data.)
 */
unsafe fn dumptuples(state: *mut Tuplesortstate, alltuples: bool) {
    let memtupwrite: c_int;
    let mut i: c_int;

    /*
     * Nothing to do if we still fit in available memory and have array slots,
     * unless this is the final call during initial run generation.
     */
    if (*state).memtupcount < (*state).memtupsize && !LACKMEM(state) && !alltuples {
        return;
    }

    /*
     * Final call might require no sorting, in rare cases where we just so
     * happen to have previously LACKMEM()'d at the point where exactly all
     * remaining tuples are loaded into memory, just before input was
     * exhausted.  In general, short final runs are quite possible, but avoid
     * creating a completely empty run.  In a worker, though, we must produce
     * at least one tape, even if it's empty.
     */
    if (*state).memtupcount == 0 && (*state).currentRun > 0 {
        return;
    }

    Assert!((*state).status == TSS_BUILDRUNS);

    /*
     * It seems unlikely that this limit will ever be exceeded, but take no
     * chances
     */
    if (*state).currentRun == INT_MAX {
        // C also: errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
        ereport!(
            ERROR,
            errmsg!("cannot have more than {} runs for an external sort", INT_MAX)
        );
    }

    if (*state).currentRun > 0 {
        selectnewtape(state);
    }

    (*state).currentRun += 1;

    if trace_sort {
        elog!(
            LOG,
            "worker {} starting quicksort of run {}: {}",
            (*state).worker,
            (*state).currentRun,
            CStr::from_ptr(pg_rusage_show(&mut (*state).ru_start)).to_string_lossy()
        );
    }

    /*
     * Sort all tuples accumulated within the allowed amount of memory for
     * this run using quicksort
     */
    tuplesort_sort_memtuples(state);

    if trace_sort {
        elog!(
            LOG,
            "worker {} finished quicksort of run {}: {}",
            (*state).worker,
            (*state).currentRun,
            CStr::from_ptr(pg_rusage_show(&mut (*state).ru_start)).to_string_lossy()
        );
    }

    memtupwrite = (*state).memtupcount;
    i = 0;
    while i < memtupwrite {
        let stup: *mut SortTuple = (*state).memtuples.add(i as usize);

        WRITETUP(state, (*state).destTape, stup);
        i += 1;
    }

    (*state).memtupcount = 0;

    /*
     * Reset tuple memory.  We've freed all of the tuples that we previously
     * allocated.  It's important to avoid fragmentation when there is a stark
     * change in the sizes of incoming tuples.  In bounded sorts,
     * fragmentation due to AllocSetFree's bucketing by size class might be
     * particularly bad if this step wasn't taken.
     */
    MemoryContextReset((*state).base.tuplecontext);

    /*
     * Now update the memory accounting to subtract the memory used by the
     * tuple.
     */
    FREEMEM(state, (*state).tupleMem);
    (*state).tupleMem = 0;

    markrunend((*state).destTape);

    if trace_sort {
        elog!(
            LOG,
            "worker {} finished writing run {} to tape {}: {}",
            (*state).worker,
            (*state).currentRun,
            ((*state).currentRun - 1) % (*state).nOutputTapes + 1,
            CStr::from_ptr(pg_rusage_show(&mut (*state).ru_start)).to_string_lossy()
        );
    }
}

/*
 * tuplesort_rescan		- rewind and replay the scan
 */
pub unsafe fn tuplesort_rescan(state: *mut Tuplesortstate) {
    let oldcontext: MemoryContext = MemoryContextSwitchTo((*state).base.sortcontext);

    Assert!((*state).base.sortopt & TUPLESORT_RANDOMACCESS != 0);

    match (*state).status {
        TSS_SORTEDINMEM => {
            (*state).current = 0;
            (*state).eof_reached = false;
            (*state).markpos_offset = 0;
            (*state).markpos_eof = false;
        }
        TSS_SORTEDONTAPE => {
            LogicalTapeRewindForRead((*state).result_tape, 0);
            (*state).eof_reached = false;
            (*state).markpos_block = 0;
            (*state).markpos_offset = 0;
            (*state).markpos_eof = false;
        }
        _ => {
            elog!(ERROR, "invalid tuplesort state");
        }
    }

    MemoryContextSwitchTo(oldcontext);
}

/*
 * tuplesort_markpos	- saves current position in the merged sort file
 */
pub unsafe fn tuplesort_markpos(state: *mut Tuplesortstate) {
    let oldcontext: MemoryContext = MemoryContextSwitchTo((*state).base.sortcontext);

    Assert!((*state).base.sortopt & TUPLESORT_RANDOMACCESS != 0);

    match (*state).status {
        TSS_SORTEDINMEM => {
            (*state).markpos_offset = (*state).current;
            (*state).markpos_eof = (*state).eof_reached;
        }
        TSS_SORTEDONTAPE => {
            LogicalTapeTell(
                (*state).result_tape,
                &mut (*state).markpos_block,
                &mut (*state).markpos_offset,
            );
            (*state).markpos_eof = (*state).eof_reached;
        }
        _ => {
            elog!(ERROR, "invalid tuplesort state");
        }
    }

    MemoryContextSwitchTo(oldcontext);
}

/*
 * tuplesort_restorepos - restores current position in merged sort file to
 *						  last saved position
 */
pub unsafe fn tuplesort_restorepos(state: *mut Tuplesortstate) {
    let oldcontext: MemoryContext = MemoryContextSwitchTo((*state).base.sortcontext);

    Assert!((*state).base.sortopt & TUPLESORT_RANDOMACCESS != 0);

    match (*state).status {
        TSS_SORTEDINMEM => {
            (*state).current = (*state).markpos_offset;
            (*state).eof_reached = (*state).markpos_eof;
        }
        TSS_SORTEDONTAPE => {
            LogicalTapeSeek(
                (*state).result_tape,
                (*state).markpos_block,
                (*state).markpos_offset,
            );
            (*state).eof_reached = (*state).markpos_eof;
        }
        _ => {
            elog!(ERROR, "invalid tuplesort state");
        }
    }

    MemoryContextSwitchTo(oldcontext);
}

/*
 * tuplesort_get_stats - extract summary statistics
 *
 * This can be called after tuplesort_performsort() finishes to obtain
 * printable summary information about how the sort was performed.
 */
pub unsafe fn tuplesort_get_stats(
    state: *mut Tuplesortstate,
    stats: *mut TuplesortInstrumentation,
) {
    /*
     * Note: it might seem we should provide both memory and disk usage for a
     * disk-based sort.  However, the current code doesn't track memory space
     * accurately once we have begun to return tuples to the caller (since we
     * don't account for pfree's the caller is expected to do), so we cannot
     * rely on availMem in a disk sort.  This does not seem worth the overhead
     * to fix.  Is it worth creating an API for the memory context code to
     * tell us how much is actually used in sortcontext?
     */
    tuplesort_updatemax(state);

    if (*state).isMaxSpaceDisk {
        (*stats).spaceType = SORT_SPACE_TYPE_DISK;
    } else {
        (*stats).spaceType = SORT_SPACE_TYPE_MEMORY;
    }
    (*stats).spaceUsed = ((*state).maxSpace + 1023) / 1024;

    match (*state).maxSpaceStatus {
        TSS_SORTEDINMEM => {
            if (*state).boundUsed {
                (*stats).sortMethod = SORT_TYPE_TOP_N_HEAPSORT;
            } else {
                (*stats).sortMethod = SORT_TYPE_QUICKSORT;
            }
        }
        TSS_SORTEDONTAPE => {
            (*stats).sortMethod = SORT_TYPE_EXTERNAL_SORT;
        }
        TSS_FINALMERGE => {
            (*stats).sortMethod = SORT_TYPE_EXTERNAL_MERGE;
        }
        _ => {
            (*stats).sortMethod = SORT_TYPE_STILL_IN_PROGRESS;
        }
    }
}

/*
 * Convert TuplesortMethod to a string.
 */
pub fn tuplesort_method_name(m: TuplesortMethod) -> *const c_char {
    match m {
        SORT_TYPE_STILL_IN_PROGRESS => c"still in progress".as_ptr(),
        SORT_TYPE_TOP_N_HEAPSORT => c"top-N heapsort".as_ptr(),
        SORT_TYPE_QUICKSORT => c"quicksort".as_ptr(),
        SORT_TYPE_EXTERNAL_SORT => c"external sort".as_ptr(),
        SORT_TYPE_EXTERNAL_MERGE => c"external merge".as_ptr(),
        _ => c"unknown".as_ptr(),
    }
}

/*
 * Convert TuplesortSpaceType to a string.
 */
pub fn tuplesort_space_type_name(t: TuplesortSpaceType) -> *const c_char {
    Assert!(t == SORT_SPACE_TYPE_DISK || t == SORT_SPACE_TYPE_MEMORY);
    if t == SORT_SPACE_TYPE_DISK {
        c"Disk".as_ptr()
    } else {
        c"Memory".as_ptr()
    }
}

/*
 * Heap manipulation routines, per Knuth's Algorithm 5.2.3H.
 */

/*
 * Convert the existing unordered array of SortTuples to a bounded heap,
 * discarding all but the smallest "state->bound" tuples.
 *
 * When working with a bounded heap, we want to keep the largest entry
 * at the root (array entry zero), instead of the smallest as in the normal
 * sort case.  This allows us to discard the largest entry cheaply.
 * Therefore, we temporarily reverse the sort direction.
 */
unsafe fn make_bounded_heap(state: *mut Tuplesortstate) {
    let tupcount: c_int = (*state).memtupcount;
    let mut i: c_int;

    Assert!((*state).status == TSS_INITIAL);
    Assert!((*state).bounded);
    Assert!(tupcount >= (*state).bound);
    Assert!(SERIAL(state));

    /* Reverse sort direction so largest entry will be at root */
    reversedirection(state);

    (*state).memtupcount = 0; /* make the heap empty */
    i = 0;
    while i < tupcount {
        if (*state).memtupcount < (*state).bound {
            /* Insert next tuple into heap */
            /* Must copy source tuple to avoid possible overwrite */
            let mut stup: SortTuple = *(*state).memtuples.add(i as usize);

            tuplesort_heap_insert(state, &mut stup);
        } else {
            /*
             * The heap is full.  Replace the largest entry with the new
             * tuple, or just discard it, if it's larger than anything already
             * in the heap.
             */
            if COMPARETUP(state, (*state).memtuples.add(i as usize), (*state).memtuples.add(0)) <= 0
            {
                free_sort_tuple(state, (*state).memtuples.add(i as usize));
                CHECK_FOR_INTERRUPTS!();
            } else {
                tuplesort_heap_replace_top(state, (*state).memtuples.add(i as usize));
            }
        }
        i += 1;
    }

    Assert!((*state).memtupcount == (*state).bound);
    (*state).status = TSS_BOUNDED;
}

/*
 * Convert the bounded heap to a properly-sorted array
 */
unsafe fn sort_bounded_heap(state: *mut Tuplesortstate) {
    let tupcount: c_int = (*state).memtupcount;

    Assert!((*state).status == TSS_BOUNDED);
    Assert!((*state).bounded);
    Assert!(tupcount == (*state).bound);
    Assert!(SERIAL(state));

    /*
     * We can unheapify in place because each delete-top call will remove the
     * largest entry, which we can promptly store in the newly freed slot at
     * the end.  Once we're down to a single-entry heap, we're done.
     */
    while (*state).memtupcount > 1 {
        let stup: SortTuple = *(*state).memtuples.add(0);

        /* this sifts-up the next-largest entry and decreases memtupcount */
        tuplesort_heap_delete_top(state);
        *(*state).memtuples.add((*state).memtupcount as usize) = stup;
    }
    (*state).memtupcount = tupcount;

    /*
     * Reverse sort direction back to the original state.  This is not
     * actually necessary but seems like a good idea for tidiness.
     */
    reversedirection(state);

    (*state).status = TSS_SORTEDINMEM;
    (*state).boundUsed = true;
}

/*
 * Sort all memtuples using specialized qsort() routines.
 *
 * Quicksort is used for small in-memory sorts, and external sort runs.
 */
unsafe fn tuplesort_sort_memtuples(state: *mut Tuplesortstate) {
    Assert!(!LEADER(state));

    if (*state).memtupcount > 1 {
        /*
         * Do we have the leading column's value or abbreviation in datum1,
         * and is there a specialization for its comparator?
         */
        if (*state).base.haveDatum1 && !(*state).base.sortKeys.is_null() {
            let cmp = (*(*state).base.sortKeys.add(0)).comparator.map(|c| c as usize);
            if cmp == Some(ssup_datum_unsigned_cmp as usize) {
                qsort_tuple_unsigned((*state).memtuples, (*state).memtupcount as usize, state);
                return;
            }
            // #if SIZEOF_DATUM >= 8
            else if cmp == Some(ssup_datum_signed_cmp as usize) {
                qsort_tuple_signed((*state).memtuples, (*state).memtupcount as usize, state);
                return;
            }
            // #endif
            else if cmp == Some(ssup_datum_int32_cmp as usize) {
                qsort_tuple_int32((*state).memtuples, (*state).memtupcount as usize, state);
                return;
            }
        }

        /* Can we use the single-key sort function? */
        if !(*state).base.onlyKey.is_null() {
            qsort_ssup(
                (*state).memtuples,
                (*state).memtupcount as usize,
                (*state).base.onlyKey,
            );
        } else {
            qsort_tuple(
                (*state).memtuples,
                (*state).memtupcount as usize,
                (*state).base.comparetup,
                state,
            );
        }
    }
}

/*
 * Insert a new tuple into an empty or existing heap, maintaining the
 * heap invariant.  Caller is responsible for ensuring there's room.
 *
 * Note: For some callers, tuple points to a memtuples[] entry above the
 * end of the heap.  This is safe as long as it's not immediately adjacent
 * to the end of the heap (ie, in the [memtupcount] array entry) --- if it
 * is, it might get overwritten before being moved into the heap!
 */
unsafe fn tuplesort_heap_insert(state: *mut Tuplesortstate, tuple: *mut SortTuple) {
    let memtuples: *mut SortTuple;
    let mut j: c_int;

    memtuples = (*state).memtuples;
    Assert!((*state).memtupcount < (*state).memtupsize);

    CHECK_FOR_INTERRUPTS!();

    /*
     * Sift-up the new entry, per Knuth 5.2.3 exercise 16. Note that Knuth is
     * using 1-based array indexes, not 0-based.
     */
    j = (*state).memtupcount;
    (*state).memtupcount += 1;
    while j > 0 {
        let i: c_int = (j - 1) >> 1;

        if COMPARETUP(state, tuple, memtuples.add(i as usize)) >= 0 {
            break;
        }
        *memtuples.add(j as usize) = *memtuples.add(i as usize);
        j = i;
    }
    *memtuples.add(j as usize) = *tuple;
}

/*
 * Remove the tuple at state->memtuples[0] from the heap.  Decrement
 * memtupcount, and sift up to maintain the heap invariant.
 *
 * The caller has already free'd the tuple the top node points to,
 * if necessary.
 */
unsafe fn tuplesort_heap_delete_top(state: *mut Tuplesortstate) {
    let memtuples: *mut SortTuple = (*state).memtuples;
    let tuple: *mut SortTuple;

    (*state).memtupcount -= 1;
    if (*state).memtupcount <= 0 {
        return;
    }

    /*
     * Remove the last tuple in the heap, and re-insert it, by replacing the
     * current top node with it.
     */
    tuple = memtuples.add((*state).memtupcount as usize);
    tuplesort_heap_replace_top(state, tuple);
}

/*
 * Replace the tuple at state->memtuples[0] with a new tuple.  Sift up to
 * maintain the heap invariant.
 *
 * This corresponds to Knuth's "sift-up" algorithm (Algorithm 5.2.3H,
 * Heapsort, steps H3-H8).
 */
unsafe fn tuplesort_heap_replace_top(state: *mut Tuplesortstate, tuple: *mut SortTuple) {
    let memtuples: *mut SortTuple = (*state).memtuples;
    let mut i: c_uint;
    let n: c_uint;

    Assert!((*state).memtupcount >= 1);

    CHECK_FOR_INTERRUPTS!();

    /*
     * state->memtupcount is "int", but we use "unsigned int" for i, j, n.
     * This prevents overflow in the "2 * i + 1" calculation, since at the top
     * of the loop we must have i < n <= INT_MAX <= UINT_MAX/2.
     */
    n = (*state).memtupcount as c_uint;
    i = 0; /* i is where the "hole" is */
    loop {
        let mut j: c_uint = 2 * i + 1;

        if j >= n {
            break;
        }
        if j + 1 < n
            && COMPARETUP(state, memtuples.add(j as usize), memtuples.add((j + 1) as usize)) > 0
        {
            j += 1;
        }
        if COMPARETUP(state, tuple, memtuples.add(j as usize)) <= 0 {
            break;
        }
        *memtuples.add(i as usize) = *memtuples.add(j as usize);
        i = j;
    }
    *memtuples.add(i as usize) = *tuple;
}

/*
 * Function to reverse the sort direction from its current state
 *
 * It is not safe to call this when performing hash tuplesorts
 */
unsafe fn reversedirection(state: *mut Tuplesortstate) {
    let mut sortKey: SortSupport = (*state).base.sortKeys;
    let mut nkey: c_int;

    nkey = 0;
    while nkey < (*state).base.nKeys {
        (*sortKey).ssup_reverse = !(*sortKey).ssup_reverse;
        (*sortKey).ssup_nulls_first = !(*sortKey).ssup_nulls_first;
        nkey += 1;
        sortKey = sortKey.add(1);
    }
}

/*
 * Tape interface routines
 */

unsafe fn getlen(tape: *mut LogicalTape, eofOK: bool) -> c_uint {
    let mut len: c_uint = 0;

    if LogicalTapeRead(
        tape,
        &mut len as *mut c_uint as *mut c_void,
        core::mem::size_of::<c_uint>(),
    ) != core::mem::size_of::<c_uint>()
    {
        elog!(ERROR, "unexpected end of tape");
    }
    if len == 0 && !eofOK {
        elog!(ERROR, "unexpected end of data");
    }
    len
}

unsafe fn markrunend(tape: *mut LogicalTape) {
    let len: c_uint = 0;

    LogicalTapeWrite(
        tape,
        &len as *const c_uint as *const c_void,
        core::mem::size_of::<c_uint>(),
    );
}

/*
 * Get memory for tuple from within READTUP() routine.
 *
 * We use next free slot from the slab allocator, or palloc() if the tuple
 * is too large for that.
 */
pub unsafe fn tuplesort_readtup_alloc(state: *mut Tuplesortstate, tuplen: Size) -> *mut c_void {
    let buf: *mut SlabSlot;

    /*
     * We pre-allocate enough slots in the slab arena that we should never run
     * out.
     */
    Assert!(!(*state).slabFreeHead.is_null());

    if tuplen > SLAB_SLOT_SIZE || (*state).slabFreeHead.is_null() {
        MemoryContextAlloc((*state).base.sortcontext, tuplen)
    } else {
        buf = (*state).slabFreeHead;
        /* Reuse this slot */
        (*state).slabFreeHead = (*buf).nextfree;

        buf as *mut c_void
    }
}

/*
 * Parallel sort routines
 */

/*
 * tuplesort_estimate_shared - estimate required shared memory allocation
 *
 * nWorkers is an estimate of the number of workers (it's the number that
 * will be requested).
 */
pub unsafe fn tuplesort_estimate_shared(nWorkers: c_int) -> Size {
    let mut tapesSize: Size;

    Assert!(nWorkers > 0);

    /* Make sure that BufFile shared state is MAXALIGN'd */
    tapesSize = mul_size(core::mem::size_of::<TapeShare>(), nWorkers as usize);
    tapesSize = MAXALIGN(add_size(tapesSize, offsetof_Sharedsort_tapes()));

    tapesSize
}

/*
 * offsetof(Sharedsort, tapes) -- the flexible array member starts after all
 * fixed fields.
 */
#[inline]
fn offsetof_Sharedsort_tapes() -> Size {
    // The tapes[] flexible array member begins at the offset of the field.
    // Compute via a zeroed reference to avoid relying on a dedicated macro.
    let dummy = core::mem::MaybeUninit::<Sharedsort>::uninit();
    let base = dummy.as_ptr() as usize;
    unsafe {
        let field = core::ptr::addr_of!((*dummy.as_ptr()).tapes) as usize;
        field - base
    }
}

/*
 * tuplesort_initialize_shared - initialize shared tuplesort state
 *
 * Must be called from leader process before workers are launched, to
 * establish state needed up-front for worker tuplesortstates.  nWorkers
 * should match the argument passed to tuplesort_estimate_shared().
 */
pub unsafe fn tuplesort_initialize_shared(
    shared: *mut Sharedsort,
    nWorkers: c_int,
    seg: *mut dsm_segment,
) {
    let mut i: c_int;

    Assert!(nWorkers > 0);

    SpinLockInit(&mut (*shared).mutex);
    (*shared).currentWorker = 0;
    (*shared).workersFinished = 0;
    SharedFileSetInit(&mut (*shared).fileset, seg);
    (*shared).nTapes = nWorkers;
    i = 0;
    while i < nWorkers {
        (*(*shared).tapes.as_mut_ptr().add(i as usize)).firstblocknumber = 0;
        i += 1;
    }
}

/*
 * tuplesort_attach_shared - attach to shared tuplesort state
 *
 * Must be called by all worker processes.
 */
pub unsafe fn tuplesort_attach_shared(shared: *mut Sharedsort, seg: *mut dsm_segment) {
    /* Attach to SharedFileSet */
    SharedFileSetAttach(&mut (*shared).fileset, seg);
}

/*
 * worker_get_identifier - Assign and return ordinal identifier for worker
 *
 * The order in which these are assigned is not well defined, and should not
 * matter; worker numbers across parallel sort participants need only be
 * distinct and gapless.  logtape.c requires this.
 *
 * Note that the identifiers assigned from here have no relation to
 * ParallelWorkerNumber number, to avoid making any assumption about
 * caller's requirements.  However, we do follow the ParallelWorkerNumber
 * convention of representing a non-worker with worker number -1.  This
 * includes the leader, as well as serial Tuplesort processes.
 */
unsafe fn worker_get_identifier(state: *mut Tuplesortstate) -> c_int {
    let shared: *mut Sharedsort = (*state).shared;
    let worker: c_int;

    Assert!(WORKER(state));

    SpinLockAcquire(&mut (*shared).mutex);
    worker = (*shared).currentWorker;
    (*shared).currentWorker += 1;
    SpinLockRelease(&mut (*shared).mutex);

    worker
}

/*
 * worker_freeze_result_tape - freeze worker's result tape for leader
 *
 * This is called by workers just after the result tape has been determined,
 * instead of calling LogicalTapeFreeze() directly.  They do so because
 * workers require a few additional steps over similar serial
 * TSS_SORTEDONTAPE external sort cases, which also happen here.  The extra
 * steps are around freeing now unneeded resources, and representing to
 * leader that worker's input run is available for its merge.
 *
 * There should only be one final output run for each worker, which consists
 * of all tuples that were originally input into worker.
 */
unsafe fn worker_freeze_result_tape(state: *mut Tuplesortstate) {
    let shared: *mut Sharedsort = (*state).shared;
    let mut output: TapeShare = core::mem::zeroed();

    Assert!(WORKER(state));
    Assert!(!(*state).result_tape.is_null());
    Assert!((*state).memtupcount == 0);

    /*
     * Free most remaining memory, in case caller is sensitive to our holding
     * on to it.  memtuples may not be a tiny merge heap at this point.
     */
    pfree((*state).memtuples as *mut c_void);
    /* Be tidy */
    (*state).memtuples = core::ptr::null_mut();
    (*state).memtupsize = 0;

    /*
     * Parallel worker requires result tape metadata, which is to be stored in
     * shared memory for leader
     */
    LogicalTapeFreeze((*state).result_tape, &mut output);

    /* Store properties of output tape, and update finished worker count */
    SpinLockAcquire(&mut (*shared).mutex);
    *(*shared).tapes.as_mut_ptr().add((*state).worker as usize) = output;
    (*shared).workersFinished += 1;
    SpinLockRelease(&mut (*shared).mutex);
}

/*
 * worker_nomergeruns - dump memtuples in worker, without merging
 *
 * This called as an alternative to mergeruns() with a worker when no
 * merging is required.
 */
unsafe fn worker_nomergeruns(state: *mut Tuplesortstate) {
    Assert!(WORKER(state));
    Assert!((*state).result_tape.is_null());
    Assert!((*state).nOutputRuns == 1);

    (*state).result_tape = (*state).destTape;
    worker_freeze_result_tape(state);
}

/*
 * leader_takeover_tapes - create tapeset for leader from worker tapes
 *
 * So far, leader Tuplesortstate has performed no actual sorting.  By now, all
 * sorting has occurred in workers, all of which must have already returned
 * from tuplesort_performsort().
 *
 * When this returns, leader process is left in a state that is virtually
 * indistinguishable from it having generated runs as a serial external sort
 * might have.
 */
unsafe fn leader_takeover_tapes(state: *mut Tuplesortstate) {
    let shared: *mut Sharedsort = (*state).shared;
    let nParticipants: c_int = (*state).nParticipants;
    let workersFinished: c_int;
    let mut j: c_int;

    Assert!(LEADER(state));
    Assert!(nParticipants >= 1);

    SpinLockAcquire(&mut (*shared).mutex);
    workersFinished = (*shared).workersFinished;
    SpinLockRelease(&mut (*shared).mutex);

    if nParticipants != workersFinished {
        elog!(ERROR, "cannot take over tapes before all workers finish");
    }

    /*
     * Create the tapeset from worker tapes, including a leader-owned tape at
     * the end.  Parallel workers are far more expensive than logical tapes,
     * so the number of tapes allocated here should never be excessive.
     */
    inittapestate(state, nParticipants);
    (*state).tapeset = LogicalTapeSetCreate(false, &mut (*shared).fileset, -1);

    /*
     * Set currentRun to reflect the number of runs we will merge (it's not
     * used for anything, this is just pro forma)
     */
    (*state).currentRun = nParticipants;

    /*
     * Initialize the state to look the same as after building the initial
     * runs.
     *
     * There will always be exactly 1 run per worker, and exactly one input
     * tape per run, because workers always output exactly 1 run, even when
     * there were no input tuples for workers to sort.
     */
    (*state).inputTapes = core::ptr::null_mut();
    (*state).nInputTapes = 0;
    (*state).nInputRuns = 0;

    (*state).outputTapes =
        palloc0(nParticipants as usize * core::mem::size_of::<*mut LogicalTape>())
            as *mut *mut LogicalTape;
    (*state).nOutputTapes = nParticipants;
    (*state).nOutputRuns = nParticipants;

    j = 0;
    while j < nParticipants {
        *(*state).outputTapes.add(j as usize) = LogicalTapeImport(
            (*state).tapeset,
            j,
            (*shared).tapes.as_mut_ptr().add(j as usize),
        );
        j += 1;
    }

    (*state).status = TSS_BUILDRUNS;
}

/*
 * Convenience routine to free a tuple previously loaded into sort memory
 */
unsafe fn free_sort_tuple(state: *mut Tuplesortstate, stup: *mut SortTuple) {
    if !(*stup).tuple.is_null() {
        FREEMEM(state, GetMemoryChunkSpace((*stup).tuple) as int64);
        pfree((*stup).tuple);
        (*stup).tuple = core::ptr::null_mut();
    }
}

pub unsafe fn ssup_datum_unsigned_cmp(x: Datum, y: Datum, ssup: SortSupport) -> c_int {
    if x < y {
        -1
    } else if x > y {
        1
    } else {
        0
    }
}

// #if SIZEOF_DATUM >= 8
pub unsafe fn ssup_datum_signed_cmp(x: Datum, y: Datum, ssup: SortSupport) -> c_int {
    let xx: int64 = DatumGetInt64(x);
    let yy: int64 = DatumGetInt64(y);

    if xx < yy {
        -1
    } else if xx > yy {
        1
    } else {
        0
    }
}
// #endif

pub unsafe fn ssup_datum_int32_cmp(x: Datum, y: Datum, ssup: SortSupport) -> c_int {
    let xx: int32 = DatumGetInt32(x);
    let yy: int32 = DatumGetInt32(y);

    if xx < yy {
        -1
    } else if xx > yy {
        1
    } else {
        0
    }
}
