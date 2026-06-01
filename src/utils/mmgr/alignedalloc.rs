//! Translation of postgres/src/backend/utils/mmgr/alignedalloc.c
//!
//! Helper functions for the MCTX_ALIGNED_REDIRECT memory-chunk method.  An
//! "aligned" allocation is a normal allocation from some other context whose
//! returned pointer has been bumped up to the requested alignment; the bytes
//! just before the aligned pointer hold a redirect `MemoryChunk` whose "value"
//! is the alignment and whose "block" pointer is the original (unaligned)
//! allocation.  These four routines implement free / realloc / get-context /
//! get-space by chasing the redirect back to the underlying chunk.
//!
//! #include mapping:
//!   "utils/memdebug.h"             -> VALGRIND_* macros are no-ops here.
//!   "utils/memutils_memorychunk.h" -> crate::utils::mmgr::memutils_memorychunk
//!   (GetMemoryChunkContext / GetMemoryChunkSpace / MemoryContextAllocAligned /
//!    MemoryContextAllocationFailure come from crate::utils::mmgr::mcxt.)
//!
//! MEMORY_CONTEXT_CHECKING is off, so the sentinel_ok scribble checks are
//! omitted (matching the production build).
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::prelude::*;
use crate::utils::mmgr::mcxt::{
    GetMemoryChunkContext, GetMemoryChunkSpace, MemoryContextAllocAligned,
    MemoryContextAllocationFailure,
};
use crate::utils::mmgr::memnodes::MemoryContext;
use crate::utils::mmgr::memutils_memorychunk::{
    MemoryChunk, MemoryChunkGetBlock, MemoryChunkGetValue, PointerGetMemoryChunk,
};

extern "C" {
    fn memcpy(dest: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
}

/// memutils_internal.h: `PallocAlignedExtraBytes(alignto)`.
/// `MAXIMUM_ALIGNOF` is 8 on all supported 64-bit targets; on those
/// `sizeof(MemoryChunk) == MAXIMUM_ALIGNOF`, so the extra is just `alignto`.
#[inline]
fn palloc_aligned_extra_bytes(alignto: Size) -> Size {
    const MAXIMUM_ALIGNOF: Size = 8;
    alignto + (core::mem::size_of::<MemoryChunk>() - MAXIMUM_ALIGNOF)
}

/// `AlignedAllocFree` - the MemoryContextMethods.free_p for aligned allocations.
///
/// # Safety
/// `pointer` must be a live aligned allocation produced by MemoryContextAllocAligned.
pub unsafe fn AlignedAllocFree(pointer: *mut c_void) {
    let chunk = PointerGetMemoryChunk(pointer);
    Assert!(!crate::utils::mmgr::memutils_memorychunk::MemoryChunkIsExternal(chunk));

    // Obtain the original (unaligned) allocated pointer and recursively pfree it.
    let unaligned = MemoryChunkGetBlock(chunk);
    pfree(unaligned);
}

/// `AlignedAllocRealloc` - realloc an aligned allocation, preserving alignment.
///
/// # Safety
/// `pointer` must be a live aligned allocation.
pub unsafe fn AlignedAllocRealloc(pointer: *mut c_void, size: Size, flags: c_int) -> *mut c_void {
    let redirchunk = PointerGetMemoryChunk(pointer);
    let alignto = MemoryChunkGetValue(redirchunk);
    let unaligned = MemoryChunkGetBlock(redirchunk);

    // sanity check this is a power of 2 value
    Assert!((alignto & (alignto - 1)) == 0);

    // Recompute the old request size from the underlying chunk's reported space.
    let old_size = GetMemoryChunkSpace(unaligned)
        - palloc_aligned_extra_bytes(alignto)
        - core::mem::size_of::<MemoryChunk>();

    let ctx: MemoryContext = GetMemoryChunkContext(unaligned);
    let newptr = MemoryContextAllocAligned(ctx, size, alignto, flags);

    // Cope cleanly with OOM.
    if newptr.is_null() {
        return MemoryContextAllocationFailure(ctx, size, flags);
    }

    memcpy(newptr, pointer, core::cmp::min(size, old_size));
    pfree(unaligned);
    newptr
}

/// `AlignedAllocGetChunkContext` - the owning MemoryContext of an aligned chunk.
///
/// # Safety
/// `pointer` must be a live aligned allocation.
pub unsafe fn AlignedAllocGetChunkContext(pointer: *mut c_void) -> MemoryContext {
    let redirchunk = PointerGetMemoryChunk(pointer);
    Assert!(!crate::utils::mmgr::memutils_memorychunk::MemoryChunkIsExternal(redirchunk));
    GetMemoryChunkContext(MemoryChunkGetBlock(redirchunk))
}

/// `AlignedAllocGetChunkSpace` - bytes consumed by an aligned allocation.
///
/// # Safety
/// `pointer` must be a live aligned allocation.
pub unsafe fn AlignedAllocGetChunkSpace(pointer: *mut c_void) -> Size {
    let redirchunk = PointerGetMemoryChunk(pointer);
    let unaligned = MemoryChunkGetBlock(redirchunk);
    GetMemoryChunkSpace(unaligned)
}
