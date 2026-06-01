//! Translation of postgres/src/backend/utils/mmgr/mcxt.c
//!
//! POSTGRES memory context management code.
//!
//! This module handles context management operations that are independent
//! of the particular kind of context being operated on.  It calls
//! context-type-specific operations via the function pointers in a
//! context's MemoryContextMethods struct.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! NOTE (port staging): this is the REAL MemoryContext dispatch layer,
//! translated additively under `utils/mmgr` while the rest of the crate still
//! uses the context-less bootstrap allocator in `utils::palloc`. To avoid
//! clashing with the bootstrap `MemoryContext`/`palloc`/`pfree` symbols this
//! module imports types explicitly (NOT `crate::prelude::*`) and is not yet
//! re-exported anywhere. The final rewiring step unifies the two.
//!
//! INTEGRATOR NOTE: aset.rs currently imports `MemoryContextCreate`,
//! `MemoryContextAllocationFailure`, `MemoryContextSizeFailure` (and uses local
//! `MemoryContextStats`/`MemoryContextResetOnly` stubs) from
//! `utils::mmgr::memutils_internal` -- those are stubs that `unimplemented!()`.
//! When wiring this module in, repoint aset.rs's `MemoryContextCreate` (and the
//! two failure helpers) import to THIS module's real definitions, and replace
//! aset.rs's local `MemoryContextStats`/`MemoryContextResetOnly` stubs with the
//! real ones defined here.

// c.h: Size, uint64, MAXALIGN, TYPEALIGN, MAXIMUM_ALIGNOF, unlikely.
use crate::c::{uint64, Size, MAXALIGN, TYPEALIGN};
use crate::pg_config::MAXIMUM_ALIGNOF;
// nodes/nodes.h: NodeTag (the `tag` passed to MemoryContextCreate).
use crate::nodes::nodes::NodeTag;
// utils/elog.h: severity levels for elog!/ereport!.
use crate::utils::elog::{ERROR, LOG_SERVER_ONLY};
// utils/palloc.h: allocation flag bits + the reset/delete callback record.
use crate::utils::palloc::{
    MemoryContextCallback, MCXT_ALLOC_HUGE, MCXT_ALLOC_NO_OOM, MCXT_ALLOC_ZERO,
};
// utils/memutils.h: request-size validity checks.
use crate::utils::memutils::{AllocHugeSizeIsValid, AllocSizeIsValid};
// nodes/memnodes.h: the abstract context node + its method-table types + IsValid.
use crate::utils::mmgr::memnodes::{
    MemoryContext, MemoryContextCounters, MemoryContextData, MemoryContextIsValid,
    MemoryContextMethods, MemoryStatsPrintFunc,
};
// utils/memutils_internal.h: the MemoryContextMethodID enum + all the per-allocator
// method functions referenced by the mcxt_methods[] table. The aset.c ones are the
// real translated bodies; Generation/Slab/AlignedAlloc/Bump are still stubs (they
// `unimplemented!()` -- equivalent to "panic until translated", which is fine).
use crate::utils::mmgr::memutils_internal::{
    AlignedAllocFree, AlignedAllocGetChunkContext, AlignedAllocGetChunkSpace, AlignedAllocRealloc,
    MemoryContextMethodID, MCTX_0_RESERVED_UNUSEDMEM_ID, MCTX_10_UNUSED_ID, MCTX_11_UNUSED_ID,
    MCTX_12_UNUSED_ID, MCTX_13_UNUSED_ID, MCTX_14_UNUSED_ID, MCTX_15_RESERVED_WIPEDMEM_ID,
    MCTX_1_RESERVED_GLIBC_ID, MCTX_2_RESERVED_GLIBC_ID, MCTX_8_UNUSED_ID, MCTX_9_UNUSED_ID,
    MCTX_ALIGNED_REDIRECT_ID, MCTX_ASET_ID, MCTX_BUMP_ID, MCTX_GENERATION_ID, MCTX_SLAB_ID,
    MEMORY_CONTEXT_METHODID_MASK,
};
// The Generation/Slab/Bump method functions now come from their REAL allocator
// modules (translated) rather than memutils_internal's unimplemented!() stubs.
use crate::utils::mmgr::bump::{
    BumpAlloc, BumpDelete, BumpFree, BumpGetChunkContext, BumpGetChunkSpace, BumpIsEmpty,
    BumpRealloc, BumpReset, BumpStats,
};
use crate::utils::mmgr::generation::{
    GenerationAlloc, GenerationDelete, GenerationFree, GenerationGetChunkContext,
    GenerationGetChunkSpace, GenerationIsEmpty, GenerationRealloc, GenerationReset, GenerationStats,
};
use crate::utils::mmgr::slab::{
    SlabAlloc, SlabDelete, SlabFree, SlabGetChunkContext, SlabGetChunkSpace, SlabIsEmpty,
    SlabRealloc, SlabReset, SlabStats,
};
// aset.c: the AllocSet method functions for the MCTX_ASET_ID slot + the internal
// context creator used by MemoryContextInit().
use crate::utils::mmgr::aset::{
    AllocSetAlloc, AllocSetContextCreateInternal, AllocSetDelete, AllocSetFree,
    AllocSetGetChunkContext, AllocSetGetChunkSpace, AllocSetIsEmpty, AllocSetRealloc,
    AllocSetReset, AllocSetStats,
};
// utils/memutils_memorychunk.h: the chunk header + encode/decode helpers used by
// pfree/repalloc dispatch and by MemoryContextAllocAligned.
use crate::utils::mmgr::memutils_memorychunk::{
    MemoryChunk, MemoryChunkSetHdrMask, PointerGetMemoryChunk,
};
use core::ffi::{c_char, c_int, c_void};
// `Assert!` is brought into scope crate-wide via `#[macro_use] pub mod c;` in main.rs.
// `elog!`/`ereport!`/`errmsg!` macros are likewise exported crate-wide.
use crate::{elog, ereport, errmsg};

// ----------------------------------------------------------------------------
// Backend globals referenced by mcxt.c that are not yet modeled in this port.
// These live in miscadmin.h / mb / elog.c in PostgreSQL. Stubbed here so this
// module is self-contained; replace with the real symbols when those subsystems
// are translated.
// TODO(pg-port): source these from miscadmin (CritSectionCount, InterruptPending,
// LogMemoryContextPending, MyProcPid) and tcop/postgres.c (stack_is_too_deep).
// ----------------------------------------------------------------------------

/// `CritSectionCount` (miscadmin.h): nonzero inside a critical section.
/// The backend is single-threaded per process; modeled as a `static mut`.
static mut CritSectionCount: u32 = 0;

/// `InterruptPending` (miscadmin.h).
#[allow(non_upper_case_globals)]
static mut InterruptPending: bool = false;

/// `LogMemoryContextPending` (miscadmin.h).
#[allow(non_upper_case_globals)]
static mut LogMemoryContextPending: bool = false;

/// `MyProcPid` (miscadmin.h): this backend's PID. Stubbed at 0.
#[allow(non_upper_case_globals)]
static mut MyProcPid: c_int = 0;

/// `stack_is_too_deep()` (tcop/postgres.c): guards deep recursion. Not yet
/// modeled; always reports "not too deep" here.
/// TODO(pg-port): real stack-depth check.
#[inline]
unsafe fn stack_is_too_deep() -> bool {
    false
}

/// `pg_mbcliplen(str, len, limit)` (mb/mbutils.c): clip a multibyte string to at
/// most `limit` bytes without splitting a character. Until the encoding subsystem
/// is ported we conservatively clip on a raw byte boundary (limit).
/// TODO(pg-port): real multibyte clip from mb/pg_wchar.
#[inline]
unsafe fn pg_mbcliplen(_str: *const c_char, len: c_int, limit: c_int) -> c_int {
    if len < limit {
        len
    } else {
        limit
    }
}

// ----------------------------------------------------------------------------
// PallocAlignedExtraBytes (utils/memutils_internal.h).
//
// How many extra bytes do we need to request in order to ensure that we can
// align a pointer to 'alignto'.  Since palloc'd pointers are already aligned
// to MAXIMUM_ALIGNOF we can subtract that amount.  We also need to make sure
// there is enough space for the redirection MemoryChunk.
//
// The header left this as a TODO pending the MemoryChunk port; MemoryChunk is
// now available, so we define it here.
// TODO(pg-port): hoist to memutils_internal once that module imports MemoryChunk.
// ----------------------------------------------------------------------------
/// `PallocAlignedExtraBytes(alignto)`
#[inline]
const fn PallocAlignedExtraBytes(alignto: Size) -> Size {
    alignto + (core::mem::size_of::<MemoryChunk>() - MAXIMUM_ALIGNOF)
}

/*****************************************************************************
 *	  GLOBAL MEMORY															 *
 *****************************************************************************/

/*
 * The mcxt_methods[] table, indexed by MemoryContextMethodID.
 *
 * C uses designated initializers (`[MCTX_ASET_ID].alloc = ...`); Rust requires
 * us to lay out the array in enum order. The MemoryContextMethodID variants are
 * declared (in memutils_internal.rs) in the same numeric order used here, so the
 * positional layout below matches the C designated-initializer result exactly.
 *
 * Reserved/unused IDs use BOGUS entries (BogusFree/BogusRealloc/
 * BogusGetChunkContext/BogusGetChunkSpace) so we fail cleanly if a bogus pointer
 * is passed to pfree or the like; other methods of bogus slots are None.
 *
 * Raw `fn` pointers are `Sync`, and `MemoryContextMethods` contains only such
 * pointers (wrapped in `Option`), so a plain `static` array is fine -- no
 * interior mutability, hence no Sync newtype wrapper is needed.
 */

/// A BOGUS slot: only the inspection methods (free/realloc/get_chunk_*) are
/// populated, mirroring the C `BOGUS_MCTX(id)` macro.
const fn bogus_mctx() -> MemoryContextMethods {
    MemoryContextMethods {
        alloc: None,
        free_p: Some(BogusFree),
        realloc: Some(BogusRealloc),
        reset: None,
        delete_context: None,
        get_chunk_context: Some(BogusGetChunkContext),
        get_chunk_space: Some(BogusGetChunkSpace),
        is_empty: None,
        stats: None,
    }
}

/// An entirely-empty slot (all methods None). Used for slots that C leaves with
/// no designated initializer at all (none currently, but kept for clarity).
#[allow(dead_code)]
const fn empty_mctx() -> MemoryContextMethods {
    MemoryContextMethods {
        alloc: None,
        free_p: None,
        realloc: None,
        reset: None,
        delete_context: None,
        get_chunk_context: None,
        get_chunk_space: None,
        is_empty: None,
        stats: None,
    }
}

// static const MemoryContextMethods mcxt_methods[] = { ... };
//
// The 16 entries correspond to the 16 MemoryContextMethodID bit-patterns
// (MEMORY_CONTEXT_METHODID_MASK has 4 bits). Listed in enum-discriminant order.
static mcxt_methods: [MemoryContextMethods; 16] = [
    // [MCTX_0_RESERVED_UNUSEDMEM_ID] -- BOGUS_MCTX
    bogus_mctx(),
    // [MCTX_1_RESERVED_GLIBC_ID] -- BOGUS_MCTX
    bogus_mctx(),
    // [MCTX_2_RESERVED_GLIBC_ID] -- BOGUS_MCTX
    bogus_mctx(),
    // [MCTX_ASET_ID] -- aset.c
    MemoryContextMethods {
        alloc: Some(AllocSetAlloc),
        free_p: Some(AllocSetFree),
        realloc: Some(AllocSetRealloc),
        reset: Some(AllocSetReset),
        delete_context: Some(AllocSetDelete),
        get_chunk_context: Some(AllocSetGetChunkContext),
        get_chunk_space: Some(AllocSetGetChunkSpace),
        is_empty: Some(AllocSetIsEmpty),
        stats: Some(AllocSetStats),
        // #ifdef MEMORY_CONTEXT_CHECKING check: Some(AllocSetCheck) -- omitted (default-off)
    },
    // [MCTX_GENERATION_ID] -- generation.c (stubs until translated)
    MemoryContextMethods {
        alloc: Some(GenerationAlloc),
        free_p: Some(GenerationFree),
        realloc: Some(GenerationRealloc),
        reset: Some(GenerationReset),
        delete_context: Some(GenerationDelete),
        get_chunk_context: Some(GenerationGetChunkContext),
        get_chunk_space: Some(GenerationGetChunkSpace),
        is_empty: Some(GenerationIsEmpty),
        stats: Some(GenerationStats),
        // #ifdef MEMORY_CONTEXT_CHECKING check: Some(GenerationCheck) -- omitted
    },
    // [MCTX_SLAB_ID] -- slab.c (stubs until translated)
    MemoryContextMethods {
        alloc: Some(SlabAlloc),
        free_p: Some(SlabFree),
        realloc: Some(SlabRealloc),
        reset: Some(SlabReset),
        delete_context: Some(SlabDelete),
        get_chunk_context: Some(SlabGetChunkContext),
        get_chunk_space: Some(SlabGetChunkSpace),
        is_empty: Some(SlabIsEmpty),
        stats: Some(SlabStats),
        // #ifdef MEMORY_CONTEXT_CHECKING check: Some(SlabCheck) -- omitted
    },
    // [MCTX_ALIGNED_REDIRECT_ID] -- alignedalloc.c (stubs until translated)
    MemoryContextMethods {
        alloc: None, /* not required */
        free_p: Some(AlignedAllocFree),
        realloc: Some(AlignedAllocRealloc),
        reset: None,          /* not required */
        delete_context: None, /* not required */
        get_chunk_context: Some(AlignedAllocGetChunkContext),
        get_chunk_space: Some(AlignedAllocGetChunkSpace),
        is_empty: None, /* not required */
        stats: None,    /* not required */
        // #ifdef MEMORY_CONTEXT_CHECKING check: None -- not required
    },
    // [MCTX_BUMP_ID] -- bump.c (stubs until translated)
    MemoryContextMethods {
        alloc: Some(BumpAlloc),
        free_p: Some(BumpFree),
        realloc: Some(BumpRealloc),
        reset: Some(BumpReset),
        delete_context: Some(BumpDelete),
        get_chunk_context: Some(BumpGetChunkContext),
        get_chunk_space: Some(BumpGetChunkSpace),
        is_empty: Some(BumpIsEmpty),
        stats: Some(BumpStats),
        // #ifdef MEMORY_CONTEXT_CHECKING check: Some(BumpCheck) -- omitted
    },
    // [MCTX_8_UNUSED_ID] -- BOGUS_MCTX
    bogus_mctx(),
    // [MCTX_9_UNUSED_ID] -- BOGUS_MCTX
    bogus_mctx(),
    // [MCTX_10_UNUSED_ID] -- BOGUS_MCTX
    bogus_mctx(),
    // [MCTX_11_UNUSED_ID] -- BOGUS_MCTX
    bogus_mctx(),
    // [MCTX_12_UNUSED_ID] -- BOGUS_MCTX
    bogus_mctx(),
    // [MCTX_13_UNUSED_ID] -- BOGUS_MCTX
    bogus_mctx(),
    // [MCTX_14_UNUSED_ID] -- BOGUS_MCTX
    bogus_mctx(),
    // [MCTX_15_RESERVED_WIPEDMEM_ID] -- BOGUS_MCTX
    bogus_mctx(),
];

// Static-assert that the variants line up with the positional layout above, so
// a future reordering of MemoryContextMethodID can't silently misalign the table.
const _: () = {
    assert!(MCTX_0_RESERVED_UNUSEDMEM_ID as usize == 0);
    assert!(MCTX_1_RESERVED_GLIBC_ID as usize == 1);
    assert!(MCTX_2_RESERVED_GLIBC_ID as usize == 2);
    assert!(MCTX_ASET_ID as usize == 3);
    assert!(MCTX_GENERATION_ID as usize == 4);
    assert!(MCTX_SLAB_ID as usize == 5);
    assert!(MCTX_ALIGNED_REDIRECT_ID as usize == 6);
    assert!(MCTX_BUMP_ID as usize == 7);
    assert!(MCTX_8_UNUSED_ID as usize == 8);
    assert!(MCTX_9_UNUSED_ID as usize == 9);
    assert!(MCTX_10_UNUSED_ID as usize == 10);
    assert!(MCTX_11_UNUSED_ID as usize == 11);
    assert!(MCTX_12_UNUSED_ID as usize == 12);
    assert!(MCTX_13_UNUSED_ID as usize == 13);
    assert!(MCTX_14_UNUSED_ID as usize == 14);
    assert!(MCTX_15_RESERVED_WIPEDMEM_ID as usize == 15);
};

/*
 * CurrentMemoryContext
 *		Default memory context for allocations.
 *
 * NOTE: distinct from the bootstrap `utils::palloc::CurrentMemoryContext`; this
 * is the REAL one. Not re-exported yet, so the two coexist.
 */
pub static mut CurrentMemoryContext: MemoryContext = core::ptr::null_mut();

/*
 * Standard top-level contexts. For a description of the purpose of each
 * of these contexts, refer to src/backend/utils/mmgr/README
 */
pub static mut TopMemoryContext: MemoryContext = core::ptr::null_mut();
pub static mut ErrorContext: MemoryContext = core::ptr::null_mut();
pub static mut PostmasterContext: MemoryContext = core::ptr::null_mut();
pub static mut CacheMemoryContext: MemoryContext = core::ptr::null_mut();
pub static mut MessageContext: MemoryContext = core::ptr::null_mut();
pub static mut TopTransactionContext: MemoryContext = core::ptr::null_mut();
pub static mut CurTransactionContext: MemoryContext = core::ptr::null_mut();

/* This is a transient link to the active portal's memory context: */
pub static mut PortalContext: MemoryContext = core::ptr::null_mut();

/* Is memory context logging currently in progress? */
static mut LogMemoryContextInProgress: bool = false;

/*
 * MemoryContextSwitchTo - switch to specified context, return previous.
 * Inline in C but exported here so other modules can import it.
 */
#[inline]
pub unsafe fn MemoryContextSwitchTo(context: MemoryContext) -> MemoryContext {
    let old = CurrentMemoryContext;
    CurrentMemoryContext = context;
    old
}

/*
 * You should not do memory allocations within a critical section, because
 * an out-of-memory error will be escalated to a PANIC. To enforce that
 * rule, the allocation functions Assert that.
 *
 * #define AssertNotInCriticalSection(context) \
 *     Assert(CritSectionCount == 0 || (context)->allowInCritSection)
 */
macro_rules! AssertNotInCriticalSection {
    ($context:expr) => {
        Assert!(CritSectionCount == 0 || (*($context)).allowInCritSection)
    };
}

/*
 * Call the given function in the MemoryContextMethods for the memory context
 * type that 'pointer' belongs to.
 *
 * #define MCXT_METHOD(pointer, method) \
 *     mcxt_methods[GetMemoryChunkMethodID(pointer)].method
 *
 * Rust can't index-then-select-a-field generically in a macro the way C does,
 * so callers index the table by id and pick the field directly. We provide a
 * helper that returns the methods slot for a pointer.
 */
#[inline]
unsafe fn mcxt_method_slot(pointer: *const c_void) -> &'static MemoryContextMethods {
    &mcxt_methods[GetMemoryChunkMethodID(pointer) as usize]
}

/*
 * GetMemoryChunkMethodID
 *		Return the MemoryContextMethodID from the uint64 chunk header which
 *		directly precedes 'pointer'.
 */
#[inline]
unsafe fn GetMemoryChunkMethodID(pointer: *const c_void) -> MemoryContextMethodID {
    let header: uint64;

    /*
     * Try to detect bogus pointers handed to us, poorly though we can.
     * Presumably, a pointer that isn't MAXALIGNED isn't pointing at an
     * allocated chunk.
     */
    Assert!(pointer as usize == MAXALIGN(pointer as usize));

    /* Allow access to the uint64 header */
    // VALGRIND_MAKE_MEM_DEFINED((char *) pointer - sizeof(uint64), sizeof(uint64));
    // TODO(pg-port): valgrind no-op.

    header = *((pointer as *const u8).sub(core::mem::size_of::<uint64>()) as *const uint64);

    /* Disallow access to the uint64 header */
    // VALGRIND_MAKE_MEM_NOACCESS((char *) pointer - sizeof(uint64), sizeof(uint64));
    // TODO(pg-port): valgrind no-op.

    method_id_from_index((header & MEMORY_CONTEXT_METHODID_MASK) as u8)
}

/// Map a 4-bit method-id value back to the MemoryContextMethodID enum. C simply
/// casts the masked header to the enum; Rust enums need an explicit mapping (the
/// value is guaranteed in 0..=15 by MEMORY_CONTEXT_METHODID_MASK).
#[inline]
fn method_id_from_index(v: u8) -> MemoryContextMethodID {
    match v {
        0 => MCTX_0_RESERVED_UNUSEDMEM_ID,
        1 => MCTX_1_RESERVED_GLIBC_ID,
        2 => MCTX_2_RESERVED_GLIBC_ID,
        3 => MCTX_ASET_ID,
        4 => MCTX_GENERATION_ID,
        5 => MCTX_SLAB_ID,
        6 => MCTX_ALIGNED_REDIRECT_ID,
        7 => MCTX_BUMP_ID,
        8 => MCTX_8_UNUSED_ID,
        9 => MCTX_9_UNUSED_ID,
        10 => MCTX_10_UNUSED_ID,
        11 => MCTX_11_UNUSED_ID,
        12 => MCTX_12_UNUSED_ID,
        13 => MCTX_13_UNUSED_ID,
        14 => MCTX_14_UNUSED_ID,
        _ => MCTX_15_RESERVED_WIPEDMEM_ID, /* 15 (mask guarantees v <= 15) */
    }
}

/*
 * GetMemoryChunkHeader
 *		Return the uint64 chunk header which directly precedes 'pointer'.
 *
 * This is only used after GetMemoryChunkMethodID, so no need for error checks.
 */
#[inline]
unsafe fn GetMemoryChunkHeader(pointer: *const c_void) -> uint64 {
    let header: uint64;

    /* Allow access to the uint64 header */
    // VALGRIND_MAKE_MEM_DEFINED((char *) pointer - sizeof(uint64), sizeof(uint64));
    // TODO(pg-port): valgrind no-op.

    header = *((pointer as *const u8).sub(core::mem::size_of::<uint64>()) as *const uint64);

    /* Disallow access to the uint64 header */
    // VALGRIND_MAKE_MEM_NOACCESS((char *) pointer - sizeof(uint64), sizeof(uint64));
    // TODO(pg-port): valgrind no-op.

    header
}

/*
 * MemoryContextTraverseNext
 *		Helper function to traverse all descendants of a memory context
 *		without recursion.
 *
 * Recursion could lead to out-of-stack errors with deep context hierarchies,
 * which would be unpleasant in error cleanup code paths.
 *
 * To process 'context' and all its descendants, use a loop like this:
 *
 *     <process 'context'>
 *     for (MemoryContext curr = context->firstchild;
 *          curr != NULL;
 *          curr = MemoryContextTraverseNext(curr, context))
 *     {
 *         <process 'curr'>
 *     }
 *
 * This visits all the contexts in pre-order, that is a node is visited
 * before its children.
 */
unsafe fn MemoryContextTraverseNext(mut curr: MemoryContext, top: MemoryContext) -> MemoryContext {
    /* After processing a node, traverse to its first child if any */
    if !(*curr).firstchild.is_null() {
        return (*curr).firstchild;
    }

    /*
     * After processing a childless node, traverse to its next sibling if
     * there is one.  If there isn't, traverse back up to the parent (which
     * has already been visited, and now so have all its descendants).  We're
     * done if that is "top", otherwise traverse to its next sibling if any,
     * otherwise repeat moving up.
     */
    while (*curr).nextchild.is_null() {
        curr = (*curr).parent;
        if curr == top {
            return core::ptr::null_mut();
        }
    }
    (*curr).nextchild
}

/*
 * Support routines to trap use of invalid memory context method IDs
 * (from calling pfree or the like on a bogus pointer).  As a possible
 * aid in debugging, we report the header word along with the pointer
 * address (if we got here, there must be an accessible header word).
 */
unsafe fn BogusFree(pointer: *mut c_void) {
    elog!(
        ERROR,
        "pfree called with invalid pointer {:p} (header 0x{:016x})",
        pointer,
        GetMemoryChunkHeader(pointer)
    );
}

unsafe fn BogusRealloc(pointer: *mut c_void, _size: Size, _flags: c_int) -> *mut c_void {
    elog!(
        ERROR,
        "repalloc called with invalid pointer {:p} (header 0x{:016x})",
        pointer,
        GetMemoryChunkHeader(pointer)
    );
    core::ptr::null_mut() /* keep compiler quiet */
}

unsafe fn BogusGetChunkContext(pointer: *mut c_void) -> MemoryContext {
    elog!(
        ERROR,
        "GetMemoryChunkContext called with invalid pointer {:p} (header 0x{:016x})",
        pointer,
        GetMemoryChunkHeader(pointer)
    );
    core::ptr::null_mut() /* keep compiler quiet */
}

unsafe fn BogusGetChunkSpace(pointer: *mut c_void) -> Size {
    elog!(
        ERROR,
        "GetMemoryChunkSpace called with invalid pointer {:p} (header 0x{:016x})",
        pointer,
        GetMemoryChunkHeader(pointer)
    );
    0 /* keep compiler quiet */
}

/*****************************************************************************
 *	  EXPORTED ROUTINES														 *
 *****************************************************************************/

/*
 * MemoryContextInit
 *		Start up the memory-context subsystem.
 *
 * This must be called before creating contexts or allocating memory in
 * contexts.  TopMemoryContext and ErrorContext are initialized here;
 * other contexts must be created afterwards.
 *
 * In normal multi-backend operation, this is called once during
 * postmaster startup, and not at all by individual backend startup
 * (since the backends inherit an already-initialized context subsystem
 * by virtue of being forked off the postmaster).  But in an EXEC_BACKEND
 * build, each process must do this for itself.
 *
 * In a standalone backend this must be called during backend startup.
 *
 * # Safety
 * Must be called once, before any other context operation.
 */
pub unsafe fn MemoryContextInit() {
    Assert!(TopMemoryContext.is_null());

    /*
     * First, initialize TopMemoryContext, which is the parent of all others.
     *
     * C: TopMemoryContext = AllocSetContextCreate((MemoryContext) NULL,
     *                                              "TopMemoryContext",
     *                                              ALLOCSET_DEFAULT_SIZES);
     * ALLOCSET_DEFAULT_SIZES expands to (0, 8*1024, 8*1024*1024); we call the
     * internal creator directly (the AllocSetContextCreate convenience macro is
     * still the bootstrap shim in memutils.rs).
     */
    TopMemoryContext = AllocSetContextCreateInternal(
        core::ptr::null_mut(),
        c"TopMemoryContext".as_ptr(),
        0,
        8 * 1024,
        8 * 1024 * 1024,
    );

    /*
     * Not having any other place to point CurrentMemoryContext, make it point
     * to TopMemoryContext.  Caller should change this soon!
     */
    CurrentMemoryContext = TopMemoryContext;

    /*
     * Initialize ErrorContext as an AllocSetContext with slow growth rate ---
     * we don't really expect much to be allocated in it. More to the point,
     * require it to contain at least 8K at all times. This is the only case
     * where retained memory in a context is *essential* --- we want to be
     * sure ErrorContext still has some memory even if we've run out
     * elsewhere! Also, allow allocations in ErrorContext within a critical
     * section. Otherwise a PANIC will cause an assertion failure in the error
     * reporting code, before printing out the real cause of the failure.
     *
     * This should be the last step in this function, as elog.c assumes memory
     * management works once ErrorContext is non-null.
     */
    ErrorContext = AllocSetContextCreateInternal(
        TopMemoryContext,
        c"ErrorContext".as_ptr(),
        8 * 1024,
        8 * 1024,
        8 * 1024,
    );
    MemoryContextAllowInCriticalSection(ErrorContext, true);
}

/*
 * MemoryContextReset
 *		Release all space allocated within a context and delete all its
 *		descendant contexts (but not the named context itself).
 *
 * # Safety
 * `context` must be a valid MemoryContext.
 */
pub unsafe fn MemoryContextReset(context: MemoryContext) {
    Assert!(MemoryContextIsValid(context));

    /* save a function call in common case where there are no children */
    if !(*context).firstchild.is_null() {
        MemoryContextDeleteChildren(context);
    }

    /* save a function call if no pallocs since startup or last reset */
    if !(*context).isReset {
        MemoryContextResetOnly(context);
    }
}

/*
 * MemoryContextResetOnly
 *		Release all space allocated within a context.
 *		Nothing is done to the context's descendant contexts.
 *
 * # Safety
 * `context` must be a valid MemoryContext.
 */
pub unsafe fn MemoryContextResetOnly(context: MemoryContext) {
    Assert!(MemoryContextIsValid(context));

    /* Nothing to do if no pallocs since startup or last reset */
    if !(*context).isReset {
        MemoryContextCallResetCallbacks(context);

        /*
         * If context->ident points into the context's memory, it will become
         * a dangling pointer.  We could prevent that by setting it to NULL
         * here, but that would break valid coding patterns that keep the
         * ident elsewhere, e.g. in a parent context.  So for now we assume
         * the programmer got it right.
         */

        ((*(*context).methods).reset.unwrap())(context);
        (*context).isReset = true;
        // VALGRIND_DESTROY_MEMPOOL(context);
        // VALGRIND_CREATE_MEMPOOL(context, 0, false);
        // TODO(pg-port): valgrind no-op.
    }
}

/*
 * MemoryContextResetChildren
 *		Release all space allocated within a context's descendants,
 *		but don't delete the contexts themselves.  The named context
 *		itself is not touched.
 *
 * # Safety
 * `context` must be a valid MemoryContext.
 */
pub unsafe fn MemoryContextResetChildren(context: MemoryContext) {
    Assert!(MemoryContextIsValid(context));

    let mut curr = (*context).firstchild;
    while !curr.is_null() {
        MemoryContextResetOnly(curr);
        curr = MemoryContextTraverseNext(curr, context);
    }
}

/*
 * MemoryContextDelete
 *		Delete a context and its descendants, and release all space
 *		allocated therein.
 *
 * The type-specific delete routine removes all storage for the context,
 * but we have to deal with descendant nodes here.
 *
 * # Safety
 * `context` must be a valid MemoryContext.
 */
pub unsafe fn MemoryContextDelete(context: MemoryContext) {
    let mut curr: MemoryContext;

    Assert!(MemoryContextIsValid(context));

    /*
     * Delete subcontexts from the bottom up.
     *
     * Note: Do not use recursion here.  A "stack depth limit exceeded" error
     * would be unpleasant if we're already in the process of cleaning up from
     * transaction abort.  We also cannot use MemoryContextTraverseNext() here
     * because we modify the tree as we go.
     */
    curr = context;
    loop {
        let parent: MemoryContext;

        /* Descend down until we find a leaf context with no children */
        while !(*curr).firstchild.is_null() {
            curr = (*curr).firstchild;
        }

        /*
         * We're now at a leaf with no children. Free it and continue from the
         * parent.  Or if this was the original node, we're all done.
         */
        parent = (*curr).parent;
        MemoryContextDeleteOnly(curr);

        if curr == context {
            break;
        }
        curr = parent;
    }
}

/*
 * Subroutine of MemoryContextDelete,
 * to delete a context that has no children.
 * We must also delink the context from its parent, if it has one.
 *
 * # Safety
 * `context` must be a valid, childless MemoryContext.
 */
unsafe fn MemoryContextDeleteOnly(context: MemoryContext) {
    Assert!(MemoryContextIsValid(context));
    /* We had better not be deleting TopMemoryContext ... */
    Assert!(context != TopMemoryContext);
    /* And not CurrentMemoryContext, either */
    Assert!(context != CurrentMemoryContext);
    /* All the children should've been deleted already */
    Assert!((*context).firstchild.is_null());

    /*
     * It's not entirely clear whether 'tis better to do this before or after
     * delinking the context; but an error in a callback will likely result in
     * leaking the whole context (if it's not a root context) if we do it
     * after, so let's do it before.
     */
    MemoryContextCallResetCallbacks(context);

    /*
     * We delink the context from its parent before deleting it, so that if
     * there's an error we won't have deleted/busted contexts still attached
     * to the context tree.  Better a leak than a crash.
     */
    MemoryContextSetParent(context, core::ptr::null_mut());

    /*
     * Also reset the context's ident pointer, in case it points into the
     * context.  This would only matter if someone tries to get stats on the
     * (already unlinked) context, which is unlikely, but let's be safe.
     */
    (*context).ident = core::ptr::null();

    ((*(*context).methods).delete_context.unwrap())(context);

    // VALGRIND_DESTROY_MEMPOOL(context);
    // TODO(pg-port): valgrind no-op.
}

/*
 * MemoryContextDeleteChildren
 *		Delete all the descendants of the named context and release all
 *		space allocated therein.  The named context itself is not touched.
 *
 * # Safety
 * `context` must be a valid MemoryContext.
 */
pub unsafe fn MemoryContextDeleteChildren(context: MemoryContext) {
    Assert!(MemoryContextIsValid(context));

    /*
     * MemoryContextDelete will delink the child from me, so just iterate as
     * long as there is a child.
     */
    while !(*context).firstchild.is_null() {
        MemoryContextDelete((*context).firstchild);
    }
}

/*
 * MemoryContextRegisterResetCallback
 *		Register a function to be called before next context reset/delete.
 *		Such callbacks will be called in reverse order of registration.
 *
 * The caller is responsible for allocating a MemoryContextCallback struct
 * to hold the info about this callback request, and for filling in the
 * "func" and "arg" fields in the struct to show what function to call with
 * what argument.  Typically the callback struct should be allocated within
 * the specified context, since that means it will automatically be freed
 * when no longer needed.
 *
 * There is no API for deregistering a callback once registered.  If you
 * want it to not do anything anymore, adjust the state pointed to by its
 * "arg" to indicate that.
 *
 * # Safety
 * `context` must be valid; `cb` must point to a live MemoryContextCallback.
 */
pub unsafe fn MemoryContextRegisterResetCallback(
    context: MemoryContext,
    cb: *mut MemoryContextCallback,
) {
    Assert!(MemoryContextIsValid(context));

    /* Push onto head so this will be called before older registrants. */
    (*cb).next = (*context).reset_cbs;
    (*context).reset_cbs = cb;
    /* Mark the context as non-reset (it probably is already). */
    (*context).isReset = false;
}

/*
 * MemoryContextCallResetCallbacks
 *		Internal function to call all registered callbacks for context.
 *
 * # Safety
 * `context` must be a valid MemoryContext.
 */
unsafe fn MemoryContextCallResetCallbacks(context: MemoryContext) {
    let mut cb: *mut MemoryContextCallback;

    /*
     * We pop each callback from the list before calling.  That way, if an
     * error occurs inside the callback, we won't try to call it a second time
     * in the likely event that we reset or delete the context later.
     */
    loop {
        cb = (*context).reset_cbs;
        if cb.is_null() {
            break;
        }
        (*context).reset_cbs = (*cb).next;
        ((*cb).func.unwrap())((*cb).arg);
    }
}

/*
 * MemoryContextSetIdentifier
 *		Set the identifier string for a memory context.
 *
 * An identifier can be provided to help distinguish among different contexts
 * of the same kind in memory context stats dumps.  The identifier string
 * must live at least as long as the context it is for; typically it is
 * allocated inside that context, so that it automatically goes away on
 * context deletion.  Pass id = NULL to forget any old identifier.
 *
 * # Safety
 * `context` must be valid; `id` must be NULL or a valid C string outliving it.
 */
pub unsafe fn MemoryContextSetIdentifier(context: MemoryContext, id: *const c_char) {
    Assert!(MemoryContextIsValid(context));
    (*context).ident = id;
}

/*
 * MemoryContextSetParent
 *		Change a context to belong to a new parent (or no parent).
 *
 * We provide this as an API function because it is sometimes useful to
 * change a context's lifespan after creation.  For example, a context
 * might be created underneath a transient context, filled with data,
 * and then reparented underneath CacheMemoryContext to make it long-lived.
 * In this way no special effort is needed to get rid of the context in case
 * a failure occurs before its contents are completely set up.
 *
 * Callers often assume that this function cannot fail, so don't put any
 * elog(ERROR) calls in it.
 *
 * A possible caller error is to reparent a context under itself, creating
 * a loop in the context graph.  We assert here that context != new_parent,
 * but checking for multi-level loops seems more trouble than it's worth.
 *
 * # Safety
 * `context` must be valid; `new_parent` must be NULL or a valid MemoryContext.
 */
pub unsafe fn MemoryContextSetParent(context: MemoryContext, new_parent: MemoryContext) {
    Assert!(MemoryContextIsValid(context));
    Assert!(context != new_parent);

    /* Fast path if it's got correct parent already */
    if new_parent == (*context).parent {
        return;
    }

    /* Delink from existing parent, if any */
    if !(*context).parent.is_null() {
        let parent: MemoryContext = (*context).parent;

        if !(*context).prevchild.is_null() {
            (*(*context).prevchild).nextchild = (*context).nextchild;
        } else {
            Assert!((*parent).firstchild == context);
            (*parent).firstchild = (*context).nextchild;
        }

        if !(*context).nextchild.is_null() {
            (*(*context).nextchild).prevchild = (*context).prevchild;
        }
    }

    /* And relink */
    if !new_parent.is_null() {
        Assert!(MemoryContextIsValid(new_parent));
        (*context).parent = new_parent;
        (*context).prevchild = core::ptr::null_mut();
        (*context).nextchild = (*new_parent).firstchild;
        if !(*new_parent).firstchild.is_null() {
            (*(*new_parent).firstchild).prevchild = context;
        }
        (*new_parent).firstchild = context;
    } else {
        (*context).parent = core::ptr::null_mut();
        (*context).prevchild = core::ptr::null_mut();
        (*context).nextchild = core::ptr::null_mut();
    }
}

/*
 * MemoryContextAllowInCriticalSection
 *		Allow/disallow allocations in this memory context within a critical
 *		section.
 *
 * Normally, memory allocations are not allowed within a critical section,
 * because a failure would lead to PANIC.  There are a few exceptions to
 * that, like allocations related to debugging code that is not supposed to
 * be enabled in production.  This function can be used to exempt specific
 * memory contexts from the assertion in palloc().
 *
 * # Safety
 * `context` must be a valid MemoryContext.
 */
pub unsafe fn MemoryContextAllowInCriticalSection(context: MemoryContext, allow: bool) {
    Assert!(MemoryContextIsValid(context));

    (*context).allowInCritSection = allow;
}

/*
 * GetMemoryChunkContext
 *		Given a currently-allocated chunk, determine the MemoryContext that
 *		the chunk belongs to.
 *
 * # Safety
 * `pointer` must be a live chunk from one of the context allocators.
 */
pub unsafe fn GetMemoryChunkContext(pointer: *mut c_void) -> MemoryContext {
    // MCXT_METHOD(pointer, get_chunk_context) (pointer);
    (mcxt_method_slot(pointer).get_chunk_context.unwrap())(pointer)
}

/*
 * GetMemoryChunkSpace
 *		Given a currently-allocated chunk, determine the total space
 *		it occupies (including all memory-allocation overhead).
 *
 * This is useful for measuring the total space occupied by a set of
 * allocated chunks.
 *
 * # Safety
 * `pointer` must be a live chunk from one of the context allocators.
 */
pub unsafe fn GetMemoryChunkSpace(pointer: *mut c_void) -> Size {
    // MCXT_METHOD(pointer, get_chunk_space) (pointer);
    (mcxt_method_slot(pointer).get_chunk_space.unwrap())(pointer)
}

/*
 * MemoryContextGetParent
 *		Get the parent context (if any) of the specified context
 *
 * # Safety
 * `context` must be a valid MemoryContext.
 */
pub unsafe fn MemoryContextGetParent(context: MemoryContext) -> MemoryContext {
    Assert!(MemoryContextIsValid(context));

    (*context).parent
}

/*
 * MemoryContextIsEmpty
 *		Is a memory context empty of any allocated space?
 *
 * # Safety
 * `context` must be a valid MemoryContext.
 */
pub unsafe fn MemoryContextIsEmpty(context: MemoryContext) -> bool {
    Assert!(MemoryContextIsValid(context));

    /*
     * For now, we consider a memory context nonempty if it has any children;
     * perhaps this should be changed later.
     */
    if !(*context).firstchild.is_null() {
        return false;
    }
    /* Otherwise use the type-specific inquiry */
    ((*(*context).methods).is_empty.unwrap())(context)
}

/*
 * Find the memory allocated to blocks for this memory context. If recurse is
 * true, also include children.
 *
 * # Safety
 * `context` must be a valid MemoryContext.
 */
pub unsafe fn MemoryContextMemAllocated(context: MemoryContext, recurse: bool) -> Size {
    let mut total: Size = (*context).mem_allocated;

    Assert!(MemoryContextIsValid(context));

    if recurse {
        let mut curr = (*context).firstchild;
        while !curr.is_null() {
            total += (*curr).mem_allocated;
            curr = MemoryContextTraverseNext(curr, context);
        }
    }

    total
}

/*
 * Return the memory consumption statistics about the given context and its
 * children.
 *
 * # Safety
 * `context` must be valid; `consumed` must point to a writable counters struct.
 */
pub unsafe fn MemoryContextMemConsumed(
    context: MemoryContext,
    consumed: *mut MemoryContextCounters,
) {
    Assert!(MemoryContextIsValid(context));

    // memset(consumed, 0, sizeof(*consumed));
    *consumed = MemoryContextCounters::default();

    /* Examine the context itself */
    ((*(*context).methods).stats.unwrap())(
        context,
        None,
        core::ptr::null_mut(),
        consumed,
        false,
    );

    /* Examine children, using iteration not recursion */
    let mut curr = (*context).firstchild;
    while !curr.is_null() {
        ((*(*curr).methods).stats.unwrap())(
            curr,
            None,
            core::ptr::null_mut(),
            consumed,
            false,
        );
        curr = MemoryContextTraverseNext(curr, context);
    }
}

/*
 * MemoryContextStats
 *		Print statistics about the named context and all its descendants.
 *
 * This is just a debugging utility, so it's not very fancy.  However, we do
 * make some effort to summarize when the output would otherwise be very long.
 * The statistics are sent to stderr.
 *
 * # Safety
 * `context` must be a valid MemoryContext.
 */
pub unsafe fn MemoryContextStats(context: MemoryContext) {
    /* Hard-wired limits are usually good enough */
    MemoryContextStatsDetail(context, 100, 100, true);
}

/*
 * MemoryContextStatsDetail
 *
 * Entry point for use if you want to vary the number of child contexts shown.
 *
 * If print_to_stderr is true, print statistics about the memory contexts
 * with fprintf(stderr), otherwise use ereport().
 *
 * # Safety
 * `context` must be a valid MemoryContext.
 */
pub unsafe fn MemoryContextStatsDetail(
    context: MemoryContext,
    max_level: c_int,
    max_children: c_int,
    print_to_stderr: bool,
) {
    let mut grand_totals = MemoryContextCounters::default();

    MemoryContextStatsInternal(
        context,
        1,
        max_level,
        max_children,
        &mut grand_totals,
        print_to_stderr,
    );

    if print_to_stderr {
        eprintln!(
            "Grand total: {} bytes in {} blocks; {} free ({} chunks); {} used",
            grand_totals.totalspace,
            grand_totals.nblocks,
            grand_totals.freespace,
            grand_totals.freechunks,
            grand_totals.totalspace - grand_totals.freespace
        );
    } else {
        /*
         * Use LOG_SERVER_ONLY to prevent the memory contexts from being sent
         * to the connected client.
         *
         * We don't buffer the information about all memory contexts in a
         * backend into StringInfo and log it as one message.  That would
         * require the buffer to be enlarged, risking an OOM as there could be
         * a large number of memory contexts in a backend.  Instead, we log
         * one message per memory context.
         */
        // ereport with errhidestmt/errhidecontext/errmsg_internal; the shim's
        // ereport! just takes level + a formatted message.
        ereport!(
            LOG_SERVER_ONLY,
            errmsg!(
                "Grand total: {} bytes in {} blocks; {} free ({} chunks); {} used",
                grand_totals.totalspace,
                grand_totals.nblocks,
                grand_totals.freespace,
                grand_totals.freechunks,
                grand_totals.totalspace - grand_totals.freespace
            )
        );
    }
}

/*
 * MemoryContextStatsInternal
 *		One recursion level for MemoryContextStats
 *
 * Print stats for this context if possible, but in any case accumulate counts
 * into *totals (if not NULL).
 *
 * # Safety
 * `context` must be valid; `totals` must be NULL or point to a counters struct.
 */
unsafe fn MemoryContextStatsInternal(
    context: MemoryContext,
    level: c_int,
    max_level: c_int,
    max_children: c_int,
    totals: *mut MemoryContextCounters,
    print_to_stderr: bool,
) {
    let mut child: MemoryContext;
    let mut ichild: c_int;

    Assert!(MemoryContextIsValid(context));

    /* Examine the context itself */
    // C passes &level as the passthru; we pass a pointer to a local copy.
    let mut level_for_print = level;
    ((*(*context).methods).stats.unwrap())(
        context,
        Some(MemoryContextStatsPrint),
        &mut level_for_print as *mut c_int as *mut c_void,
        totals,
        print_to_stderr,
    );

    /*
     * Examine children.
     *
     * If we are past the recursion depth limit or already running low on
     * stack, do not print them explicitly but just summarize them. Similarly,
     * if there are more than max_children of them, we do not print the rest
     * explicitly, but just summarize them.
     */
    child = (*context).firstchild;
    ichild = 0;
    if level <= max_level && !stack_is_too_deep() {
        while !child.is_null() && ichild < max_children {
            MemoryContextStatsInternal(
                child,
                level + 1,
                max_level,
                max_children,
                totals,
                print_to_stderr,
            );
            child = (*child).nextchild;
            ichild += 1;
        }
    }

    if !child.is_null() {
        /* Summarize the rest of the children, avoiding recursion. */
        let mut local_totals = MemoryContextCounters::default();

        ichild = 0;
        while !child.is_null() {
            ((*(*child).methods).stats.unwrap())(
                child,
                None,
                core::ptr::null_mut(),
                &mut local_totals,
                false,
            );
            ichild += 1;
            child = MemoryContextTraverseNext(child, context);
        }

        if print_to_stderr {
            for _i in 0..level {
                eprint!("  ");
            }
            eprintln!(
                "{} more child contexts containing {} total in {} blocks; {} free ({} chunks); {} used",
                ichild,
                local_totals.totalspace,
                local_totals.nblocks,
                local_totals.freespace,
                local_totals.freechunks,
                local_totals.totalspace - local_totals.freespace
            );
        } else {
            ereport!(
                LOG_SERVER_ONLY,
                errmsg!(
                    "level: {}; {} more child contexts containing {} total in {} blocks; {} free ({} chunks); {} used",
                    level,
                    ichild,
                    local_totals.totalspace,
                    local_totals.nblocks,
                    local_totals.freespace,
                    local_totals.freechunks,
                    local_totals.totalspace - local_totals.freespace
                )
            );
        }

        if !totals.is_null() {
            (*totals).nblocks += local_totals.nblocks;
            (*totals).freechunks += local_totals.freechunks;
            (*totals).totalspace += local_totals.totalspace;
            (*totals).freespace += local_totals.freespace;
        }
    }
}

/*
 * MemoryContextStatsPrint
 *		Print callback used by MemoryContextStatsInternal
 *
 * For now, the passthru pointer just points to "int level"; later we might
 * make that more complicated.
 *
 * # Safety
 * `context` must be valid; `passthru` must point to an `int` level;
 * `stats_string` must be a valid C string.
 */
unsafe fn MemoryContextStatsPrint(
    context: MemoryContext,
    passthru: *mut c_void,
    stats_string: *const c_char,
    print_to_stderr: bool,
) {
    let level: c_int = *(passthru as *const c_int);
    let mut name: *const c_char = (*context).name;
    let mut ident: *const c_char = (*context).ident;
    // char truncated_ident[110];
    let mut truncated_ident = [0u8; 110];
    let mut i: usize;

    /*
     * It seems preferable to label dynahash contexts with just the hash table
     * name.  Those are already unique enough, so the "dynahash" part isn't
     * very helpful, and this way is more consistent with pre-v11 practice.
     */
    if !ident.is_null() && cstr_eq(name, c"dynahash".as_ptr()) {
        name = ident;
        ident = core::ptr::null();
    }

    truncated_ident[0] = 0; /* '\0' */

    if !ident.is_null() {
        /*
         * Some contexts may have very long identifiers (e.g., SQL queries).
         * Arbitrarily truncate at 100 bytes, but be careful not to break
         * multibyte characters.  Also, replace ASCII control characters, such
         * as newlines, with spaces.
         */
        let mut idlen: c_int = cstr_len(ident) as c_int;
        let mut truncated = false;
        // mutable cursor over the source ident, mirroring C's `*ident++`.
        let mut idptr = ident as *const u8;

        // strcpy(truncated_ident, ": ");
        truncated_ident[0] = b':';
        truncated_ident[1] = b' ';
        truncated_ident[2] = 0;
        // i = strlen(truncated_ident);
        i = 2;

        if idlen > 100 {
            idlen = pg_mbcliplen(ident, idlen, 100);
            truncated = true;
        }

        while idlen > 0 {
            idlen -= 1;
            let mut c: u8 = *idptr;
            idptr = idptr.add(1);

            if c < b' ' {
                c = b' ';
            }
            truncated_ident[i] = c;
            i += 1;
        }
        truncated_ident[i] = 0; /* '\0' */

        if truncated {
            // strcat(truncated_ident, "...");
            // i currently points at the NUL terminator position.
            truncated_ident[i] = b'.';
            truncated_ident[i + 1] = b'.';
            truncated_ident[i + 2] = b'.';
            truncated_ident[i + 3] = 0;
        }
    }

    // Render name / stats_string / truncated_ident as Rust &str for printing.
    let name_s = cstr_to_str(name);
    let stats_s = cstr_to_str(stats_string);
    let ident_s = bytes_to_str(&truncated_ident);

    if print_to_stderr {
        let mut ii = 1;
        while ii < level {
            eprint!("  ");
            ii += 1;
        }
        eprintln!("{}: {}{}", name_s, stats_s, ident_s);
    } else {
        ereport!(
            LOG_SERVER_ONLY,
            errmsg!("level: {}; {}: {}{}", level, name_s, stats_s, ident_s)
        );
    }
}

/* Helper: compare two C strings for equality (like strcmp(...) == 0). */
#[inline]
unsafe fn cstr_eq(a: *const c_char, b: *const c_char) -> bool {
    let mut i = 0isize;
    loop {
        let ca = *a.offset(i);
        let cb = *b.offset(i);
        if ca != cb {
            return false;
        }
        if ca == 0 {
            return true;
        }
        i += 1;
    }
}

/* Helper: strlen over a C string. */
#[inline]
unsafe fn cstr_len(s: *const c_char) -> usize {
    let mut n = 0usize;
    while *s.add(n) != 0 {
        n += 1;
    }
    n
}

/* Helper: borrow a C string as a UTF-8 &str (lossily best-effort; the contents
 * are ASCII control-stripped before this for the ident path). */
#[inline]
unsafe fn cstr_to_str<'a>(s: *const c_char) -> &'a str {
    if s.is_null() {
        return "";
    }
    let len = cstr_len(s);
    let bytes = core::slice::from_raw_parts(s as *const u8, len);
    core::str::from_utf8(bytes).unwrap_or("")
}

/* Helper: borrow a NUL-terminated byte buffer as a &str up to its first NUL. */
#[inline]
fn bytes_to_str(buf: &[u8]) -> &str {
    let len = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
    core::str::from_utf8(&buf[..len]).unwrap_or("")
}

/*
 * MemoryContextCheck
 *		Check all chunks in the named context and its children.
 *
 * This is just a debugging utility, so it's not fancy.
 *
 * #ifdef MEMORY_CONTEXT_CHECKING ... #endif
 * TODO(pg-port): translate under a cfg gating MEMORY_CONTEXT_CHECKING (omitted
 * by default; depends on the per-allocator `check` method, not modeled here).
 */

/*
 * MemoryContextCreate
 *		Context-type-independent part of context creation.
 *
 * This is only intended to be called by context-type-specific
 * context creation routines, not by the unwashed masses.
 *
 * The memory context creation procedure goes like this:
 *	1.  Context-type-specific routine makes some initial space allocation,
 *		including enough space for the context header.  If it fails,
 *		it can ereport() with no damage done.
 *	2.	Context-type-specific routine sets up all type-specific fields of
 *		the header (those beyond MemoryContextData proper), as well as any
 *		other management fields it needs to have a fully valid context.
 *		Usually, failure in this step is impossible, but if it's possible
 *		the initial space allocation should be freed before ereport'ing.
 *	3.	Context-type-specific routine calls MemoryContextCreate() to fill in
 *		the generic header fields and link the context into the context tree.
 *	4.  We return to the context-type-specific routine, which finishes
 *		up type-specific initialization.  This routine can now do things
 *		that might fail (like allocate more memory), so long as it's
 *		sure the node is left in a state that delete will handle.
 *
 * node: the as-yet-uninitialized common part of the context header node.
 * tag: NodeTag code identifying the memory context type.
 * method_id: MemoryContextMethodID of the context-type being created.
 * parent: parent context, or NULL if this will be a top-level context.
 * name: name of context (must be statically allocated).
 *
 * Context routines generally assume that MemoryContextCreate can't fail,
 * so this can contain Assert but not elog/ereport.
 *
 * # Safety
 * `node` must point to writable, suitably-sized storage; `parent` must be NULL
 * or a valid MemoryContext; `name` must be a statically-allocated C string.
 */
pub unsafe fn MemoryContextCreate(
    node: MemoryContext,
    tag: NodeTag,
    method_id: MemoryContextMethodID,
    parent: MemoryContext,
    name: *const c_char,
) {
    /* Creating new memory contexts is not allowed in a critical section */
    Assert!(CritSectionCount == 0);

    /* Validate parent, to help prevent crazy context linkages */
    Assert!(parent.is_null() || MemoryContextIsValid(parent));
    Assert!(node != parent);

    /* Initialize all standard fields of memory context header */
    (*node).r#type = tag;
    (*node).isReset = true;
    (*node).methods = &mcxt_methods[method_id as usize];
    (*node).parent = parent;
    (*node).firstchild = core::ptr::null_mut();
    (*node).mem_allocated = 0;
    (*node).prevchild = core::ptr::null_mut();
    (*node).name = name;
    (*node).ident = core::ptr::null();
    (*node).reset_cbs = core::ptr::null_mut();

    /* OK to link node into context tree */
    if !parent.is_null() {
        (*node).nextchild = (*parent).firstchild;
        if !(*parent).firstchild.is_null() {
            (*(*parent).firstchild).prevchild = node;
        }
        (*parent).firstchild = node;
        /* inherit allowInCritSection flag from parent */
        (*node).allowInCritSection = (*parent).allowInCritSection;
    } else {
        (*node).nextchild = core::ptr::null_mut();
        (*node).allowInCritSection = false;
    }

    // VALGRIND_CREATE_MEMPOOL(node, 0, false);
    // TODO(pg-port): valgrind no-op.
}

/*
 * MemoryContextAllocationFailure
 *		For use by MemoryContextMethods implementations to handle when malloc
 *		returns NULL.  The behavior is specific to whether MCXT_ALLOC_NO_OOM
 *		is in 'flags'.
 *
 * # Safety
 * `context` must be a valid MemoryContext.
 */
pub unsafe fn MemoryContextAllocationFailure(
    context: MemoryContext,
    size: Size,
    flags: c_int,
) -> *mut c_void {
    if (flags & MCXT_ALLOC_NO_OOM) == 0 {
        if !TopMemoryContext.is_null() {
            MemoryContextStats(TopMemoryContext);
        }
        // ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory"),
        //                 errdetail("Failed on request of size %zu in memory context \"%s\".",
        //                           size, context->name)));
        ereport!(
            ERROR,
            errmsg!(
                "out of memory; Failed on request of size {} in memory context \"{}\".",
                size,
                cstr_to_str((*context).name)
            )
        );
    }
    core::ptr::null_mut()
}

/*
 * MemoryContextSizeFailure
 *		For use by MemoryContextMethods implementations to handle invalid
 *		memory allocation request sizes.
 *
 * pg_noreturn in C; modeled with `-> !`.
 *
 * # Safety
 * `context` must be a valid MemoryContext.
 */
pub unsafe fn MemoryContextSizeFailure(_context: MemoryContext, size: Size, _flags: c_int) -> ! {
    elog!(ERROR, "invalid memory alloc request size {}", size);
    // elog!(ERROR, ...) panics, so this is unreachable; keep the type as `!`.
    unreachable!()
}

/*
 * MemoryContextAlloc
 *		Allocate space within the specified context.
 *
 * This could be turned into a macro, but we'd have to import
 * nodes/memnodes.h into postgres.h which seems a bad idea.
 *
 * # Safety
 * `context` must be a valid MemoryContext.
 */
pub unsafe fn MemoryContextAlloc(context: MemoryContext, size: Size) -> *mut c_void {
    let ret: *mut c_void;

    Assert!(MemoryContextIsValid(context));
    AssertNotInCriticalSection!(context);

    (*context).isReset = false;

    /*
     * For efficiency reasons, we purposefully offload the handling of
     * allocation failures to the MemoryContextMethods implementation as this
     * allows these checks to be performed only when an actual malloc needs to
     * be done to request more memory from the OS.  Additionally, not having
     * to execute any instructions after this call allows the compiler to use
     * the sibling call optimization.  If you're considering adding code after
     * this call, consider making it the responsibility of the 'alloc'
     * function instead.
     */
    ret = ((*(*context).methods).alloc.unwrap())(context, size, 0);

    // VALGRIND_MEMPOOL_ALLOC(context, ret, size);
    // TODO(pg-port): valgrind no-op.

    ret
}

/*
 * MemoryContextAllocZero
 *		Like MemoryContextAlloc, but clears allocated memory
 *
 *	We could just call MemoryContextAlloc then clear the memory, but this
 *	is a very common combination, so we provide the combined operation.
 *
 * # Safety
 * `context` must be a valid MemoryContext.
 */
pub unsafe fn MemoryContextAllocZero(context: MemoryContext, size: Size) -> *mut c_void {
    let ret: *mut c_void;

    Assert!(MemoryContextIsValid(context));
    AssertNotInCriticalSection!(context);

    (*context).isReset = false;

    ret = ((*(*context).methods).alloc.unwrap())(context, size, 0);

    // VALGRIND_MEMPOOL_ALLOC(context, ret, size);
    // TODO(pg-port): valgrind no-op.

    // MemSetAligned(ret, 0, size);
    core::ptr::write_bytes(ret as *mut u8, 0, size);

    ret
}

/*
 * MemoryContextAllocExtended
 *		Allocate space within the specified context using the given flags.
 *
 * # Safety
 * `context` must be a valid MemoryContext.
 */
pub unsafe fn MemoryContextAllocExtended(
    context: MemoryContext,
    size: Size,
    flags: c_int,
) -> *mut c_void {
    let ret: *mut c_void;

    Assert!(MemoryContextIsValid(context));
    AssertNotInCriticalSection!(context);

    if !(if (flags & MCXT_ALLOC_HUGE) != 0 {
        AllocHugeSizeIsValid(size)
    } else {
        AllocSizeIsValid(size)
    }) {
        elog!(ERROR, "invalid memory alloc request size {}", size);
    }

    (*context).isReset = false;

    ret = ((*(*context).methods).alloc.unwrap())(context, size, flags);
    if crate::c::unlikely(ret.is_null()) {
        return core::ptr::null_mut();
    }

    // VALGRIND_MEMPOOL_ALLOC(context, ret, size);
    // TODO(pg-port): valgrind no-op.

    if (flags & MCXT_ALLOC_ZERO) != 0 {
        // MemSetAligned(ret, 0, size);
        core::ptr::write_bytes(ret as *mut u8, 0, size);
    }

    ret
}

/*
 * HandleLogMemoryContextInterrupt
 *		Handle receipt of an interrupt indicating logging of memory
 *		contexts.
 *
 * All the actual work is deferred to ProcessLogMemoryContextInterrupt(),
 * because we cannot safely emit a log message inside the signal handler.
 *
 * # Safety
 * Touches backend-global interrupt flags.
 */
pub unsafe fn HandleLogMemoryContextInterrupt() {
    InterruptPending = true;
    LogMemoryContextPending = true;
    /* latch will be set by procsignal_sigusr1_handler */
}

/*
 * ProcessLogMemoryContextInterrupt
 * 		Perform logging of memory contexts of this backend process.
 *
 * Any backend that participates in ProcSignal signaling must arrange
 * to call this function if we see LogMemoryContextPending set.
 * It is called from CHECK_FOR_INTERRUPTS(), which is enough because
 * the target process for logging of memory contexts is a backend.
 *
 * # Safety
 * Touches backend-global flags and walks the context tree.
 */
pub unsafe fn ProcessLogMemoryContextInterrupt() {
    LogMemoryContextPending = false;

    /*
     * Exit immediately if memory context logging is already in progress. This
     * prevents recursive calls, which could occur if logging is requested
     * repeatedly and rapidly, potentially leading to infinite recursion and a
     * crash.
     */
    if LogMemoryContextInProgress {
        return;
    }
    LogMemoryContextInProgress = true;

    // PG_TRY()/PG_FINALLY()/PG_END_TRY(): ensure LogMemoryContextInProgress is
    // reset even if logging unwinds (ereport(ERROR) -> panic). Model with a drop
    // guard so the flag is cleared on the unwinding path too.
    struct ResetGuard;
    impl Drop for ResetGuard {
        fn drop(&mut self) {
            // SAFETY: single-threaded backend global.
            unsafe {
                LogMemoryContextInProgress = false;
            }
        }
    }
    let _guard = ResetGuard;

    /*
     * Use LOG_SERVER_ONLY to prevent this message from being sent to the
     * connected client.
     */
    ereport!(
        LOG_SERVER_ONLY,
        errmsg!("logging memory contexts of PID {}", MyProcPid)
    );

    /*
     * When a backend process is consuming huge memory, logging all its
     * memory contexts might overrun available disk space. To prevent
     * this, we limit the depth of the hierarchy, as well as the number of
     * child contexts to log per parent to 100.
     *
     * As with MemoryContextStats(), we suppose that practical cases where
     * the dump gets long will typically be huge numbers of siblings under
     * the same parent context; while the additional debugging value from
     * seeing details about individual siblings beyond 100 will not be
     * large.
     */
    MemoryContextStatsDetail(TopMemoryContext, 100, 100, false);

    // _guard drops here -> LogMemoryContextInProgress = false (PG_FINALLY).
}

/*
 * palloc
 *
 * # Safety
 * `CurrentMemoryContext` must be a valid MemoryContext.
 */
pub unsafe fn palloc(size: Size) -> *mut c_void {
    /* duplicates MemoryContextAlloc to avoid increased overhead */
    let ret: *mut c_void;
    let context: MemoryContext = CurrentMemoryContext;

    Assert!(MemoryContextIsValid(context));
    AssertNotInCriticalSection!(context);

    (*context).isReset = false;

    /*
     * For efficiency reasons, we purposefully offload the handling of
     * allocation failures to the MemoryContextMethods implementation as this
     * allows these checks to be performed only when an actual malloc needs to
     * be done to request more memory from the OS.  Additionally, not having
     * to execute any instructions after this call allows the compiler to use
     * the sibling call optimization.  If you're considering adding code after
     * this call, consider making it the responsibility of the 'alloc'
     * function instead.
     */
    ret = ((*(*context).methods).alloc.unwrap())(context, size, 0);
    /* We expect OOM to be handled by the alloc function */
    Assert!(!ret.is_null());
    // VALGRIND_MEMPOOL_ALLOC(context, ret, size);
    // TODO(pg-port): valgrind no-op.

    ret
}

/*
 * palloc0
 *
 * # Safety
 * `CurrentMemoryContext` must be a valid MemoryContext.
 */
pub unsafe fn palloc0(size: Size) -> *mut c_void {
    /* duplicates MemoryContextAllocZero to avoid increased overhead */
    let ret: *mut c_void;
    let context: MemoryContext = CurrentMemoryContext;

    Assert!(MemoryContextIsValid(context));
    AssertNotInCriticalSection!(context);

    (*context).isReset = false;

    ret = ((*(*context).methods).alloc.unwrap())(context, size, 0);
    /* We expect OOM to be handled by the alloc function */
    Assert!(!ret.is_null());
    // VALGRIND_MEMPOOL_ALLOC(context, ret, size);
    // TODO(pg-port): valgrind no-op.

    // MemSetAligned(ret, 0, size);
    core::ptr::write_bytes(ret as *mut u8, 0, size);

    ret
}

/*
 * palloc_extended
 *
 * # Safety
 * `CurrentMemoryContext` must be a valid MemoryContext.
 */
pub unsafe fn palloc_extended(size: Size, flags: c_int) -> *mut c_void {
    /* duplicates MemoryContextAllocExtended to avoid increased overhead */
    let ret: *mut c_void;
    let context: MemoryContext = CurrentMemoryContext;

    Assert!(MemoryContextIsValid(context));
    AssertNotInCriticalSection!(context);

    (*context).isReset = false;

    ret = ((*(*context).methods).alloc.unwrap())(context, size, flags);
    if crate::c::unlikely(ret.is_null()) {
        /* NULL can be returned only when using MCXT_ALLOC_NO_OOM */
        Assert!(flags & MCXT_ALLOC_NO_OOM != 0);
        return core::ptr::null_mut();
    }

    // VALGRIND_MEMPOOL_ALLOC(context, ret, size);
    // TODO(pg-port): valgrind no-op.

    if (flags & MCXT_ALLOC_ZERO) != 0 {
        // MemSetAligned(ret, 0, size);
        core::ptr::write_bytes(ret as *mut u8, 0, size);
    }

    ret
}

/*
 * MemoryContextAllocAligned
 *		Allocate 'size' bytes of memory in 'context' aligned to 'alignto'
 *		bytes.
 *
 * Currently, we align addresses by requesting additional bytes from the
 * MemoryContext's standard allocator function and then aligning the returned
 * address by the required alignment.  This means that the given MemoryContext
 * must support providing us with a chunk of memory that's larger than 'size'.
 * For allocators such as Slab, that's not going to work, as slab only allows
 * chunks of the size that's specified when the context is created.
 *
 * 'alignto' must be a power of 2.
 * 'flags' may be 0 or set the same as MemoryContextAllocExtended().
 *
 * # Safety
 * `context` must be a valid MemoryContext that supports oversized chunks.
 */
pub unsafe fn MemoryContextAllocAligned(
    context: MemoryContext,
    size: Size,
    alignto: Size,
    flags: c_int,
) -> *mut c_void {
    let alignedchunk: *mut MemoryChunk;
    let alloc_size: Size;
    let unaligned: *mut c_void;
    let aligned: *mut c_void;

    /* wouldn't make much sense to waste that much space */
    Assert!(alignto < (128 * 1024 * 1024));

    /* ensure alignto is a power of 2 */
    Assert!((alignto & (alignto.wrapping_sub(1))) == 0);

    /*
     * If the alignment requirements are less than what we already guarantee
     * then just use the standard allocation function.
     */
    if crate::c::unlikely(alignto <= MAXIMUM_ALIGNOF) {
        return MemoryContextAllocExtended(context, size, flags);
    }

    /*
     * We implement aligned pointers by simply allocating enough memory for
     * the requested size plus the alignment and an additional "redirection"
     * MemoryChunk.  This additional MemoryChunk is required for operations
     * such as pfree when used on the pointer returned by this function.  We
     * use this redirection MemoryChunk in order to find the pointer to the
     * memory that was returned by the MemoryContextAllocExtended call below.
     * We do that by "borrowing" the block offset field and instead of using
     * that to find the offset into the owning block, we use it to find the
     * original allocated address.
     *
     * Here we must allocate enough extra memory so that we can still align
     * the pointer returned by MemoryContextAllocExtended and also have enough
     * space for the redirection MemoryChunk.  Since allocations will already
     * be at least aligned by MAXIMUM_ALIGNOF, we can subtract that amount
     * from the allocation size to save a little memory.
     */
    alloc_size = size + PallocAlignedExtraBytes(alignto);

    // #ifdef MEMORY_CONTEXT_CHECKING
    //     /* ensure there's space for a sentinel byte */
    //     alloc_size += 1;
    // #endif
    // TODO(pg-port): MEMORY_CONTEXT_CHECKING (default-off).

    /* perform the actual allocation */
    unaligned = MemoryContextAllocExtended(context, alloc_size, flags);

    /* set the aligned pointer */
    // aligned = (void *) TYPEALIGN(alignto, (char *) unaligned + sizeof(MemoryChunk));
    aligned = TYPEALIGN(
        alignto,
        (unaligned as *mut u8).add(core::mem::size_of::<MemoryChunk>()) as usize,
    ) as *mut c_void;

    alignedchunk = PointerGetMemoryChunk(aligned);

    /*
     * We set the redirect MemoryChunk so that the block offset calculation is
     * used to point back to the 'unaligned' allocated chunk.  This allows us
     * to use MemoryChunkGetBlock() to find the unaligned chunk when we need
     * to perform operations such as pfree() and repalloc().
     *
     * We store 'alignto' in the MemoryChunk's 'value' so that we know what
     * the alignment was set to should we ever be asked to realloc this
     * pointer.
     */
    MemoryChunkSetHdrMask(alignedchunk, unaligned, alignto, MCTX_ALIGNED_REDIRECT_ID);

    /* double check we produced a correctly aligned pointer */
    Assert!(TYPEALIGN(alignto, aligned as usize) == aligned as usize);

    // #ifdef MEMORY_CONTEXT_CHECKING
    //     alignedchunk->requested_size = size;
    //     /* set mark to catch clobber of "unused" space */
    //     set_sentinel(aligned, size);
    // #endif
    // TODO(pg-port): MEMORY_CONTEXT_CHECKING (default-off).
    let _ = alignedchunk; // silence "unused" in the non-checking build

    // Mark the bytes before the redirection header as noaccess.
    // VALGRIND_MAKE_MEM_NOACCESS(unaligned, (char *) alignedchunk - (char *) unaligned);
    // Disallow access to the redirection chunk header.
    // VALGRIND_MAKE_MEM_NOACCESS(alignedchunk, sizeof(MemoryChunk));
    // TODO(pg-port): valgrind no-op.

    aligned
}

/*
 * palloc_aligned
 *		Allocate 'size' bytes returning a pointer that's aligned to the
 *		'alignto' boundary.
 *
 * Currently, we align addresses by requesting additional bytes from the
 * MemoryContext's standard allocator function and then aligning the returned
 * address by the required alignment.  This means that the given MemoryContext
 * must support providing us with a chunk of memory that's larger than 'size'.
 * For allocators such as Slab, that's not going to work, as slab only allows
 * chunks of the size that's specified when the context is created.
 *
 * 'alignto' must be a power of 2.
 * 'flags' may be 0 or set the same as MemoryContextAllocExtended().
 *
 * # Safety
 * `CurrentMemoryContext` must be a valid MemoryContext supporting oversized chunks.
 */
pub unsafe fn palloc_aligned(size: Size, alignto: Size, flags: c_int) -> *mut c_void {
    MemoryContextAllocAligned(CurrentMemoryContext, size, alignto, flags)
}

/*
 * pfree
 *		Release an allocated chunk.
 *
 * # Safety
 * `pointer` must be a live chunk from one of the context allocators.
 */
pub unsafe fn pfree(pointer: *mut c_void) {
    // #ifdef USE_VALGRIND
    //     MemoryContextMethodID method = GetMemoryChunkMethodID(pointer);
    //     MemoryContext context = GetMemoryChunkContext(pointer);
    // #endif

    // MCXT_METHOD(pointer, free_p) (pointer);
    (mcxt_method_slot(pointer).free_p.unwrap())(pointer);

    // #ifdef USE_VALGRIND
    //     if (method != MCTX_ALIGNED_REDIRECT_ID)
    //         VALGRIND_MEMPOOL_FREE(context, pointer);
    // #endif
    // TODO(pg-port): valgrind no-op.
}

/*
 * repalloc
 *		Adjust the size of a previously allocated chunk.
 *
 * # Safety
 * `pointer` must be a live chunk from one of the context allocators.
 */
pub unsafe fn repalloc(pointer: *mut c_void, size: Size) -> *mut c_void {
    // #ifdef USE_VALGRIND
    //     MemoryContextMethodID method = GetMemoryChunkMethodID(pointer);
    // #endif
    // #if defined(USE_ASSERT_CHECKING) || defined(USE_VALGRIND)
    //     MemoryContext context = GetMemoryChunkContext(pointer);
    // #endif
    let ret: *mut c_void;

    // In assert-checking builds (debug_assertions ~ USE_ASSERT_CHECKING) C reads
    // the chunk's context to validate it. We do the same so the asserts are real.
    #[cfg(debug_assertions)]
    {
        let context: MemoryContext = GetMemoryChunkContext(pointer);
        AssertNotInCriticalSection!(context);
        /* isReset must be false already */
        Assert!(!(*context).isReset);
    }

    /*
     * For efficiency reasons, we purposefully offload the handling of
     * allocation failures to the MemoryContextMethods implementation as this
     * allows these checks to be performed only when an actual malloc needs to
     * be done to request more memory from the OS.  Additionally, not having
     * to execute any instructions after this call allows the compiler to use
     * the sibling call optimization.  If you're considering adding code after
     * this call, consider making it the responsibility of the 'realloc'
     * function instead.
     */
    // ret = MCXT_METHOD(pointer, realloc) (pointer, size, 0);
    ret = (mcxt_method_slot(pointer).realloc.unwrap())(pointer, size, 0);

    // #ifdef USE_VALGRIND
    //     if (method != MCTX_ALIGNED_REDIRECT_ID)
    //         VALGRIND_MEMPOOL_CHANGE(context, pointer, ret, size);
    // #endif
    // TODO(pg-port): valgrind no-op.

    ret
}

/*
 * repalloc_extended
 *		Adjust the size of a previously allocated chunk,
 *		with HUGE and NO_OOM options.
 *
 * # Safety
 * `pointer` must be a live chunk from one of the context allocators.
 */
pub unsafe fn repalloc_extended(pointer: *mut c_void, size: Size, flags: c_int) -> *mut c_void {
    // #if defined(USE_ASSERT_CHECKING) || defined(USE_VALGRIND)
    //     MemoryContext context = GetMemoryChunkContext(pointer);
    // #endif
    let ret: *mut c_void;

    #[cfg(debug_assertions)]
    {
        let context: MemoryContext = GetMemoryChunkContext(pointer);
        AssertNotInCriticalSection!(context);
        /* isReset must be false already */
        Assert!(!(*context).isReset);
    }

    /*
     * For efficiency reasons, we purposefully offload the handling of
     * allocation failures to the MemoryContextMethods implementation as this
     * allows these checks to be performed only when an actual malloc needs to
     * be done to request more memory from the OS.  Additionally, not having
     * to execute any instructions after this call allows the compiler to use
     * the sibling call optimization.  If you're considering adding code after
     * this call, consider making it the responsibility of the 'realloc'
     * function instead.
     */
    // ret = MCXT_METHOD(pointer, realloc) (pointer, size, flags);
    ret = (mcxt_method_slot(pointer).realloc.unwrap())(pointer, size, flags);
    if crate::c::unlikely(ret.is_null()) {
        return core::ptr::null_mut();
    }

    // VALGRIND_MEMPOOL_CHANGE(context, pointer, ret, size);
    // TODO(pg-port): valgrind no-op.

    ret
}

/*
 * repalloc0
 *		Adjust the size of a previously allocated chunk and zero out the added
 *		space.
 *
 * # Safety
 * `pointer` must be a live chunk; `oldsize` must be its previous logical size.
 */
pub unsafe fn repalloc0(pointer: *mut c_void, oldsize: Size, size: Size) -> *mut c_void {
    let ret: *mut c_void;

    /* catch wrong argument order */
    if crate::c::unlikely(oldsize > size) {
        elog!(
            ERROR,
            "invalid repalloc0 call: oldsize {}, new size {}",
            oldsize,
            size
        );
    }

    ret = repalloc(pointer, size);
    // memset((char *) ret + oldsize, 0, (size - oldsize));
    core::ptr::write_bytes((ret as *mut u8).add(oldsize), 0, size - oldsize);
    ret
}

/*
 * MemoryContextAllocHuge
 *		Allocate (possibly-expansive) space within the specified context.
 *
 * See considerations in comment at MaxAllocHugeSize.
 *
 * # Safety
 * `context` must be a valid MemoryContext.
 */
pub unsafe fn MemoryContextAllocHuge(context: MemoryContext, size: Size) -> *mut c_void {
    let ret: *mut c_void;

    Assert!(MemoryContextIsValid(context));
    AssertNotInCriticalSection!(context);

    (*context).isReset = false;

    /*
     * For efficiency reasons, we purposefully offload the handling of
     * allocation failures to the MemoryContextMethods implementation as this
     * allows these checks to be performed only when an actual malloc needs to
     * be done to request more memory from the OS.  Additionally, not having
     * to execute any instructions after this call allows the compiler to use
     * the sibling call optimization.  If you're considering adding code after
     * this call, consider making it the responsibility of the 'alloc'
     * function instead.
     */
    ret = ((*(*context).methods).alloc.unwrap())(context, size, MCXT_ALLOC_HUGE);

    // VALGRIND_MEMPOOL_ALLOC(context, ret, size);
    // TODO(pg-port): valgrind no-op.

    ret
}

/*
 * repalloc_huge
 *		Adjust the size of a previously allocated chunk, permitting a large
 *		value.  The previous allocation need not have been "huge".
 *
 * # Safety
 * `pointer` must be a live chunk from one of the context allocators.
 */
pub unsafe fn repalloc_huge(pointer: *mut c_void, size: Size) -> *mut c_void {
    /* this one seems not worth its own implementation */
    repalloc_extended(pointer, size, MCXT_ALLOC_HUGE)
}

/*
 * MemoryContextStrdup
 *		Like strdup(), but allocate from the specified context
 *
 * # Safety
 * `context` must be valid; `string` must be a valid C string.
 */
pub unsafe fn MemoryContextStrdup(context: MemoryContext, string: *const c_char) -> *mut c_char {
    let nstr: *mut c_char;
    let len: Size = cstr_len(string) + 1;

    nstr = MemoryContextAlloc(context, len) as *mut c_char;

    // memcpy(nstr, string, len);
    core::ptr::copy_nonoverlapping(string, nstr, len);

    nstr
}

/*
 * pstrdup
 *
 * # Safety
 * `in_` must be a valid C string.
 */
pub unsafe fn pstrdup(in_: *const c_char) -> *mut c_char {
    MemoryContextStrdup(CurrentMemoryContext, in_)
}

/*
 * pnstrdup
 *		Like pstrdup(), but append null byte to a
 *		not-necessarily-null-terminated input string.
 *
 * # Safety
 * `in_` must be valid for at least `len` bytes (or up to an embedded NUL).
 */
pub unsafe fn pnstrdup(in_: *const c_char, len: Size) -> *mut c_char {
    let out: *mut c_char;

    // len = strnlen(in, len);
    let len = strnlen(in_, len);

    out = palloc(len + 1) as *mut c_char;
    // memcpy(out, in, len);
    core::ptr::copy_nonoverlapping(in_, out, len);
    // out[len] = '\0';
    *out.add(len) = 0;

    out
}

/* Helper: strnlen over a C string (libc strnlen). */
#[inline]
unsafe fn strnlen(s: *const c_char, maxlen: Size) -> Size {
    let mut n = 0usize;
    while n < maxlen && *s.add(n) != 0 {
        n += 1;
    }
    n
}

/*
 * Make copy of string with all trailing newline characters removed.
 *
 * # Safety
 * `in_` must be a valid C string.
 */
pub unsafe fn pchomp(in_: *const c_char) -> *mut c_char {
    let mut n: usize;

    n = cstr_len(in_);
    while n > 0 && *in_.add(n - 1) == b'\n' as c_char {
        n -= 1;
    }
    pnstrdup(in_, n)
}

// =============================================================================
// Summary
// -----------------------------------------------------------------------------
// METHOD TABLE: `static mcxt_methods: [MemoryContextMethods; 16]`, laid out in
// MemoryContextMethodID discriminant order (C used `[ID].field =` designated
// initializers; a const block static-asserts each variant's index so the
// positional layout can't drift). MCTX_ASET_ID is wired to aset.rs's real fns
// (Some(AllocSetAlloc), Some(AllocSetFree), ... Some(AllocSetStats)). The
// Generation/Slab/AlignedAlloc/Bump slots reference memutils_internal's stub fns
// (they `unimplemented!()` until those allocators are translated). Reserved/unused
// IDs use `bogus_mctx()` (only free_p/realloc/get_chunk_context/get_chunk_space
// populated with the Bogus* handlers, matching the BOGUS_MCTX macro). Raw fn
// pointers are Sync, so a plain `static` array needs no Sync newtype.
//
// GLOBALS: `pub static mut` MemoryContext = null_mut() for CurrentMemoryContext,
// TopMemoryContext, ErrorContext, PostmasterContext, CacheMemoryContext,
// MessageContext, TopTransactionContext, CurTransactionContext (and PortalContext).
// `LogMemoryContextInProgress: static mut bool`. These are the REAL globals,
// distinct from the bootstrap ones in utils::palloc; not re-exported yet.
//
// PALLOC/PFREE DISPATCH: palloc(size) reads CurrentMemoryContext and calls
// `(*context.methods).alloc(context, size, 0)`. pfree/repalloc/repalloc_extended
// dispatch via `mcxt_method_slot(pointer)` which indexes mcxt_methods[] by
// GetMemoryChunkMethodID(pointer) (decoded from the 4-bit field of the uint64
// header preceding the chunk) and selects free_p/realloc -- the faithful
// translation of the MCXT_METHOD(pointer, method) macro. method_id_from_index()
// maps the 4-bit value back to the enum (C just casts; Rust needs the match).
//
// MemoryContextCreate: the REAL initializer -- sets type/isReset/methods(=
// &mcxt_methods[method_id])/parent/firstchild/mem_allocated/prevchild/name/ident/
// reset_cbs, links into parent's child list, inherits allowInCritSection.
//
// MemoryContextInit: asserts TopMemoryContext is NULL, creates TopMemoryContext
// (ALLOCSET_DEFAULT_SIZES = 0/8K/8M) and ErrorContext (8K/8K/8K) via
// AllocSetContextCreateInternal (NOT the bootstrap AllocSetContextCreate macro),
// points CurrentMemoryContext at TopMemoryContext, and allows ErrorContext in
// critical sections.
//
// STUBBED / not-yet-modeled (local stubs + TODO(pg-port)):
//   - Backend globals: CritSectionCount, InterruptPending, LogMemoryContextPending,
//     MyProcPid (static mut), stack_is_too_deep() (-> false), pg_mbcliplen()
//     (byte-boundary clip) -- all from miscadmin/mb/tcop, not yet ported.
//   - All VALGRIND_* hooks: no-ops. MEMORY_CONTEXT_CHECKING branches: default-off
//     path taken, checking branches preserved as comments + TODO (incl.
//     MemoryContextCheck, omitted entirely as it is compiled only under that flag).
//   - PallocAlignedExtraBytes: defined locally (memutils_internal left it TODO
//     pending MemoryChunk; MemoryChunk is available, so it is materialized here).
//   - ereport(...errcode/errdetail/errhidestmt/errhidecontext/errmsg_internal...)
//     collapse to the crate's ereport!(level, errmsg!(...)) shim; PG_TRY/PG_FINALLY
//     in ProcessLogMemoryContextInterrupt is modeled with a Drop guard so the
//     in-progress flag is cleared even on the unwinding (panic) path.
//   - C string helpers (cstr_eq/cstr_len/strnlen/cstr_to_str/bytes_to_str) replace
//     strcmp/strlen/strnlen and the printf %s rendering in the stats printer.
//
// C ARITHMETIC: `alignto & (alignto - 1)` uses wrapping_sub to match C wrapping;
// `(x as usize) < Y` parenthesized; pointer/header math uses byte (*mut u8) offset
// arithmetic mirroring C `(char *)` casts. #[repr(C)] is inherited via the imported
// node/method/chunk structs.
//
// EVERY mcxt.c function was translated:
//   GetMemoryChunkMethodID, GetMemoryChunkHeader, MemoryContextTraverseNext,
//   BogusFree, BogusRealloc, BogusGetChunkContext, BogusGetChunkSpace,
//   MemoryContextInit, MemoryContextReset, MemoryContextResetOnly,
//   MemoryContextResetChildren, MemoryContextDelete, MemoryContextDeleteOnly,
//   MemoryContextDeleteChildren, MemoryContextRegisterResetCallback,
//   MemoryContextCallResetCallbacks, MemoryContextSetIdentifier,
//   MemoryContextSetParent, MemoryContextAllowInCriticalSection,
//   GetMemoryChunkContext, GetMemoryChunkSpace, MemoryContextGetParent,
//   MemoryContextIsEmpty, MemoryContextMemAllocated, MemoryContextMemConsumed,
//   MemoryContextStats, MemoryContextStatsDetail, MemoryContextStatsInternal,
//   MemoryContextStatsPrint, MemoryContextCreate, MemoryContextAllocationFailure,
//   MemoryContextSizeFailure, MemoryContextAlloc, MemoryContextAllocZero,
//   MemoryContextAllocExtended, HandleLogMemoryContextInterrupt,
//   ProcessLogMemoryContextInterrupt, palloc, palloc0, palloc_extended,
//   MemoryContextAllocAligned, palloc_aligned, pfree, repalloc, repalloc_extended,
//   repalloc0, MemoryContextAllocHuge, repalloc_huge, MemoryContextStrdup,
//   pstrdup, pnstrdup, pchomp. (MemoryContextCheck is the only one omitted, as a
//   cfg-gated MEMORY_CONTEXT_CHECKING TODO.)
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::mmgr::aset::AllocSetContextCreateInternal;

    // End-to-end test of the REAL MemoryContext + AllocSet allocator (independent
    // of the bootstrap utils::palloc; uses mcxt's own CurrentMemoryContext).
    #[test]
    fn allocset_real_allocator_roundtrip() {
        unsafe {
            MemoryContextInit();
            assert!(!TopMemoryContext.is_null());
            assert!(!CurrentMemoryContext.is_null());

            let ctx = AllocSetContextCreateInternal(
                TopMemoryContext,
                c"test".as_ptr(),
                0,
                8 * 1024,
                8 * 1024 * 1024,
            );
            assert!(!ctx.is_null());
            CurrentMemoryContext = ctx;

            // basic alloc / write / read / free
            let p = palloc(128) as *mut u8;
            assert!(!p.is_null());
            for i in 0..128 {
                *p.add(i) = (i as u8) ^ 0xA5;
            }
            for i in 0..128 {
                assert_eq!(*p.add(i), (i as u8) ^ 0xA5);
            }
            pfree(p as *mut c_void);

            // many allocations across size classes: small (freelist), medium (block
            // growth), and large (>allocChunkLimit -> separate block).
            let mut ptrs: Vec<(*mut u8, usize)> = Vec::new();
            for &sz in [8usize, 16, 64, 256, 1024, 4096, 100_000].iter() {
                for _ in 0..50 {
                    let q = palloc(sz) as *mut u8;
                    assert!(!q.is_null());
                    *q = 0x42;
                    *q.add(sz - 1) = 0x24;
                    ptrs.push((q, sz));
                }
            }
            for &(q, sz) in ptrs.iter() {
                assert_eq!(*q, 0x42);
                assert_eq!(*q.add(sz - 1), 0x24);
            }
            // free the first half (exercises the freelist return path)
            let half = ptrs.len() / 2;
            for &(q, _) in ptrs.iter().take(half) {
                pfree(q as *mut c_void);
            }

            // palloc0 must zero
            let z = palloc0(64) as *mut u8;
            for i in 0..64 {
                assert_eq!(*z.add(i), 0);
            }

            // repalloc must grow while preserving content
            let r = palloc(16) as *mut u8;
            for i in 0..16 {
                *r.add(i) = i as u8;
            }
            let r2 = repalloc(r as *mut c_void, 4096) as *mut u8;
            for i in 0..16 {
                assert_eq!(*r2.add(i), i as u8);
            }

            // reset, then reuse
            MemoryContextReset(ctx);
            let p2 = palloc(32);
            assert!(!p2.is_null());

            // Must not delete the current context; switch back first (matches C).
            CurrentMemoryContext = TopMemoryContext;
            MemoryContextDelete(ctx);

            // Exercise the other three context types through the same real mcxt
            // dispatch, kept in THIS single test so the process-global
            // CurrentMemoryContext/TopMemoryContext are not raced by parallel threads.
            use crate::utils::mmgr::bump::BumpContextCreate;
            use crate::utils::mmgr::generation::GenerationContextCreate;
            use crate::utils::mmgr::slab::SlabContextCreate;
            let top = TopMemoryContext;

            // ---- GenerationContext ----
            let g = GenerationContextCreate(top, c"gen".as_ptr(), 0, 8 * 1024, 8 * 1024 * 1024);
            CurrentMemoryContext = g;
            let mut gptrs = Vec::new();
            for &sz in [16usize, 64, 1024, 50_000].iter() {
                let p = palloc(sz) as *mut u8;
                assert!(!p.is_null());
                *p = 0x11;
                *p.add(sz - 1) = 0x22;
                gptrs.push((p, sz));
            }
            for &(p, sz) in &gptrs {
                assert_eq!(*p, 0x11);
                assert_eq!(*p.add(sz - 1), 0x22);
                pfree(p as *mut c_void); // Generation supports pfree
            }
            CurrentMemoryContext = top;
            MemoryContextDelete(g);

            // ---- SlabContext (fixed chunk size) ----
            let chunk = 64usize;
            let s = SlabContextCreate(top, c"slab".as_ptr(), 4096, chunk);
            CurrentMemoryContext = s;
            let mut sptrs = Vec::new();
            for _ in 0..200 {
                let p = palloc(chunk) as *mut u8; // slab only allocates its fixed size
                assert!(!p.is_null());
                *p = 0x33;
                sptrs.push(p);
            }
            // free half (exercises slab block-list bookkeeping), keep half
            for &p in sptrs.iter().take(100) {
                pfree(p as *mut c_void);
            }
            CurrentMemoryContext = top;
            MemoryContextDelete(s);

            // ---- BumpContext (no per-chunk free) ----
            let b = BumpContextCreate(top, c"bump".as_ptr(), 0, 8 * 1024, 8 * 1024 * 1024);
            CurrentMemoryContext = b;
            for &sz in [8usize, 100, 4096, 70_000].iter() {
                let p = palloc(sz) as *mut u8;
                assert!(!p.is_null());
                *p = 0x44;
                *p.add(sz - 1) = 0x55;
                assert_eq!(*p, 0x44);
                assert_eq!(*p.add(sz - 1), 0x55);
            }
            // Bump does not support pfree of individual chunks; reset frees all.
            MemoryContextReset(b);
            let p = palloc(32);
            assert!(!p.is_null());
            CurrentMemoryContext = top;
            MemoryContextDelete(b);
        }
    }
}
