//! Translation of postgres/src/include/utils/memutils.h - allocation limits subset.
//!
//! The full memory-context machinery (MemoryContextData, AllocSetContext, etc.)
//! is future work; see [`crate::utils::palloc`] for the bootstrap allocator.
//!
//! TODO(pg-port): translate memutils.h + backend/utils/mmgr/{mcxt,aset,...}.c.

use crate::c::Size;

/// `MaxAllocSize`: the largest ordinary palloc request (1 GB - 1). Requests above
/// this require the "huge" allocator variants.
pub const MaxAllocSize: Size = 0x3fffffff; // 1 gigabyte - 1

/// `MaxAllocHugeSize`: upper bound for `MemoryContextAllocHuge`.
pub const MaxAllocHugeSize: Size = Size::MAX / 2;

/// `AllocSizeIsValid(size)`.
#[inline]
pub fn AllocSizeIsValid(size: Size) -> bool {
    size <= MaxAllocSize
}

/// `AllocHugeSizeIsValid(size)`.
#[inline]
pub fn AllocHugeSizeIsValid(size: Size) -> bool {
    size <= MaxAllocHugeSize
}

// ---- Memory context creation / lifecycle (utils/memutils.h) ----
//
// The real AllocSet/Generation/Slab context implementations live in
// utils/mmgr/*.c and are not yet translated. The bootstrap allocator
// ([`crate::utils::palloc`]) is context-less, so context creation returns NULL and
// reset/delete are no-ops. Allocations made "in" such a context therefore are not
// reclaimed by MemoryContextDelete (a leak), which is acceptable until mmgr lands.
//
// TODO(pg-port): translate utils/mmgr/{aset,generation,slab,mcxt}.c.

use crate::utils::palloc::{MemoryContext, MemoryContextData};

// Block-size hint triples (minContextSize, initBlockSize, maxBlockSize). Ignored
// by the bootstrap allocator, but kept so call sites pass the right token.
pub const ALLOCSET_DEFAULT_SIZES: (Size, Size, Size) = (0, 8 * 1024, 8 * 1024 * 1024);
pub const ALLOCSET_SMALL_SIZES: (Size, Size, Size) = (0, 1 * 1024, 8 * 1024);
pub const ALLOCSET_START_SMALL_SIZES: (Size, Size, Size) = (0, 1 * 1024, 8 * 1024 * 1024);

/// `AllocSetContextCreate(parent, name, sizes...)`: create an alloc-set context.
/// Shim: ignores all arguments and returns NULL (the context-less allocator).
#[macro_export]
macro_rules! AllocSetContextCreate {
    ($parent:expr, $name:expr $(, $size:expr)* $(,)?) => {{
        // Bootstrap: no real child context is created; reuse the (non-NULL) parent
        // so `MemoryContextIsValid` holds and allocations still route to palloc.
        // TODO(pg-port): real AllocSetContext (utils/mmgr/aset.c).
        let __parent = $parent;
        let _ = ($name $(, $size)*);
        __parent
    }};
}

/// `MemoryContextDelete(context)`: delete a context and everything in it.
/// Shim: no-op (the bootstrap allocator does not track per-context chunks).
///
/// # Safety
/// Matches the C signature; the argument is ignored here.
#[inline]
pub unsafe fn MemoryContextDelete(_context: MemoryContext) {
    // TODO(pg-port): free all chunks belonging to the context (utils/mmgr/mcxt.c).
    let _ = _context;
}

/// `MemoryContextReset(context)`: free a context's chunks but keep the context.
/// Shim: no-op.
///
/// # Safety
/// Matches the C signature; the argument is ignored here.
#[inline]
pub unsafe fn MemoryContextReset(_context: MemoryContext) {
    // TODO(pg-port): release the context's allocations (utils/mmgr/mcxt.c).
    let _ = _context;
}

/// Silence unused-import warnings for `MemoryContextData` (referenced via the macro).
#[allow(dead_code)]
fn _uses(_c: *mut MemoryContextData) {}
