//! Translation of postgres/src/include/utils/memutils_internal.h
//!
//! Declarations for memory allocation utility functions for internal use:
//! the per-allocator method-table functions (AllocSet/Generation/Slab/Bump and
//! the AlignedAlloc helpers), the `MemoryContextMethodID` enum that indexes the
//! `mcxt_methods[]` table in mcxt.c, and the context-type-independent creation
//! helper `MemoryContextCreate`.
//!
//! NOTE (port staging): this is the REAL MemoryContext machinery, translated
//! additively under `utils/mmgr` while the rest of the crate still uses the
//! context-less bootstrap allocator in `utils::palloc`. Imports are explicit
//! (NOT `crate::prelude::*`) to avoid clashing with the bootstrap
//! `MemoryContext`/`MemoryContextData` symbols.

use crate::c::Size;
use crate::nodes::nodes::NodeTag;
use crate::utils::mmgr::memnodes::{MemoryContext, MemoryContextData, MemoryContextMethods};
use core::ffi::{c_char, c_int, c_void};

// The Stats method functions take a printfunc + counters; these types live in
// memnodes alongside MemoryContextMethods. Imported for the extern fn sigs below.
use crate::utils::mmgr::memnodes::{MemoryContextCounters, MemoryStatsPrintFunc};

// Suppress the unused-import lint for MemoryContextData (kept for documentation
// of the MemoryContext-pointer-target relationship; referenced via MemoryContext).
#[allow(unused_imports)]
use MemoryContextData as _MemoryContextData;

/* These functions implement the MemoryContext API for AllocSet context. */
// TODO(pg-port): body in aset.c.
pub unsafe fn AllocSetAlloc(_context: MemoryContext, _size: Size, _flags: c_int) -> *mut c_void {
    unimplemented!()
}
// TODO(pg-port): body in aset.c.
pub unsafe fn AllocSetFree(_pointer: *mut c_void) {
    unimplemented!()
}
// TODO(pg-port): body in aset.c.
pub unsafe fn AllocSetRealloc(_pointer: *mut c_void, _size: Size, _flags: c_int) -> *mut c_void {
    unimplemented!()
}
// TODO(pg-port): body in aset.c.
pub unsafe fn AllocSetReset(_context: MemoryContext) {
    unimplemented!()
}
// TODO(pg-port): body in aset.c.
pub unsafe fn AllocSetDelete(_context: MemoryContext) {
    unimplemented!()
}
// TODO(pg-port): body in aset.c.
pub unsafe fn AllocSetGetChunkContext(_pointer: *mut c_void) -> MemoryContext {
    unimplemented!()
}
// TODO(pg-port): body in aset.c.
pub unsafe fn AllocSetGetChunkSpace(_pointer: *mut c_void) -> Size {
    unimplemented!()
}
// TODO(pg-port): body in aset.c.
pub unsafe fn AllocSetIsEmpty(_context: MemoryContext) -> bool {
    unimplemented!()
}
// TODO(pg-port): body in aset.c.
pub unsafe fn AllocSetStats(
    _context: MemoryContext,
    _printfunc: MemoryStatsPrintFunc,
    _passthru: *mut c_void,
    _totals: *mut MemoryContextCounters,
    _print_to_stderr: bool,
) {
    unimplemented!()
}
// #ifdef MEMORY_CONTEXT_CHECKING
// extern void AllocSetCheck(MemoryContext context);
// TODO(pg-port): AllocSetCheck under a cfg when MEMORY_CONTEXT_CHECKING is modeled.

/* These functions implement the MemoryContext API for Generation context. */
// TODO(pg-port): body in generation.c.
pub unsafe fn GenerationAlloc(_context: MemoryContext, _size: Size, _flags: c_int) -> *mut c_void {
    unimplemented!()
}
// TODO(pg-port): body in generation.c.
pub unsafe fn GenerationFree(_pointer: *mut c_void) {
    unimplemented!()
}
// TODO(pg-port): body in generation.c.
pub unsafe fn GenerationRealloc(_pointer: *mut c_void, _size: Size, _flags: c_int) -> *mut c_void {
    unimplemented!()
}
// TODO(pg-port): body in generation.c.
pub unsafe fn GenerationReset(_context: MemoryContext) {
    unimplemented!()
}
// TODO(pg-port): body in generation.c.
pub unsafe fn GenerationDelete(_context: MemoryContext) {
    unimplemented!()
}
// TODO(pg-port): body in generation.c.
pub unsafe fn GenerationGetChunkContext(_pointer: *mut c_void) -> MemoryContext {
    unimplemented!()
}
// TODO(pg-port): body in generation.c.
pub unsafe fn GenerationGetChunkSpace(_pointer: *mut c_void) -> Size {
    unimplemented!()
}
// TODO(pg-port): body in generation.c.
pub unsafe fn GenerationIsEmpty(_context: MemoryContext) -> bool {
    unimplemented!()
}
// TODO(pg-port): body in generation.c.
pub unsafe fn GenerationStats(
    _context: MemoryContext,
    _printfunc: MemoryStatsPrintFunc,
    _passthru: *mut c_void,
    _totals: *mut MemoryContextCounters,
    _print_to_stderr: bool,
) {
    unimplemented!()
}
// #ifdef MEMORY_CONTEXT_CHECKING
// extern void GenerationCheck(MemoryContext context);
// TODO(pg-port): GenerationCheck under a cfg when MEMORY_CONTEXT_CHECKING is modeled.

/* These functions implement the MemoryContext API for Slab context. */
// TODO(pg-port): body in slab.c.
pub unsafe fn SlabAlloc(_context: MemoryContext, _size: Size, _flags: c_int) -> *mut c_void {
    unimplemented!()
}
// TODO(pg-port): body in slab.c.
pub unsafe fn SlabFree(_pointer: *mut c_void) {
    unimplemented!()
}
// TODO(pg-port): body in slab.c.
pub unsafe fn SlabRealloc(_pointer: *mut c_void, _size: Size, _flags: c_int) -> *mut c_void {
    unimplemented!()
}
// TODO(pg-port): body in slab.c.
pub unsafe fn SlabReset(_context: MemoryContext) {
    unimplemented!()
}
// TODO(pg-port): body in slab.c.
pub unsafe fn SlabDelete(_context: MemoryContext) {
    unimplemented!()
}
// TODO(pg-port): body in slab.c.
pub unsafe fn SlabGetChunkContext(_pointer: *mut c_void) -> MemoryContext {
    unimplemented!()
}
// TODO(pg-port): body in slab.c.
pub unsafe fn SlabGetChunkSpace(_pointer: *mut c_void) -> Size {
    unimplemented!()
}
// TODO(pg-port): body in slab.c.
pub unsafe fn SlabIsEmpty(_context: MemoryContext) -> bool {
    unimplemented!()
}
// TODO(pg-port): body in slab.c.
pub unsafe fn SlabStats(
    _context: MemoryContext,
    _printfunc: MemoryStatsPrintFunc,
    _passthru: *mut c_void,
    _totals: *mut MemoryContextCounters,
    _print_to_stderr: bool,
) {
    unimplemented!()
}
// #ifdef MEMORY_CONTEXT_CHECKING
// extern void SlabCheck(MemoryContext context);
// TODO(pg-port): SlabCheck under a cfg when MEMORY_CONTEXT_CHECKING is modeled.

/*
 * These functions support the implementation of palloc_aligned() and are not
 * part of a fully-fledged MemoryContext type.
 */
// TODO(pg-port): body in alignedalloc.c.
pub unsafe fn AlignedAllocFree(_pointer: *mut c_void) {
    unimplemented!()
}
// TODO(pg-port): body in alignedalloc.c.
pub unsafe fn AlignedAllocRealloc(_pointer: *mut c_void, _size: Size, _flags: c_int) -> *mut c_void {
    unimplemented!()
}
// TODO(pg-port): body in alignedalloc.c.
pub unsafe fn AlignedAllocGetChunkContext(_pointer: *mut c_void) -> MemoryContext {
    unimplemented!()
}
// TODO(pg-port): body in alignedalloc.c.
pub unsafe fn AlignedAllocGetChunkSpace(_pointer: *mut c_void) -> Size {
    unimplemented!()
}

/* These functions implement the MemoryContext API for the Bump context. */
// TODO(pg-port): body in bump.c.
pub unsafe fn BumpAlloc(_context: MemoryContext, _size: Size, _flags: c_int) -> *mut c_void {
    unimplemented!()
}
// TODO(pg-port): body in bump.c.
pub unsafe fn BumpFree(_pointer: *mut c_void) {
    unimplemented!()
}
// TODO(pg-port): body in bump.c.
pub unsafe fn BumpRealloc(_pointer: *mut c_void, _size: Size, _flags: c_int) -> *mut c_void {
    unimplemented!()
}
// TODO(pg-port): body in bump.c.
pub unsafe fn BumpReset(_context: MemoryContext) {
    unimplemented!()
}
// TODO(pg-port): body in bump.c.
pub unsafe fn BumpDelete(_context: MemoryContext) {
    unimplemented!()
}
// TODO(pg-port): body in bump.c.
pub unsafe fn BumpGetChunkContext(_pointer: *mut c_void) -> MemoryContext {
    unimplemented!()
}
// TODO(pg-port): body in bump.c.
pub unsafe fn BumpGetChunkSpace(_pointer: *mut c_void) -> Size {
    unimplemented!()
}
// TODO(pg-port): body in bump.c.
pub unsafe fn BumpIsEmpty(_context: MemoryContext) -> bool {
    unimplemented!()
}
// TODO(pg-port): body in bump.c.
pub unsafe fn BumpStats(
    _context: MemoryContext,
    _printfunc: MemoryStatsPrintFunc,
    _passthru: *mut c_void,
    _totals: *mut MemoryContextCounters,
    _print_to_stderr: bool,
) {
    unimplemented!()
}
// #ifdef MEMORY_CONTEXT_CHECKING
// extern void BumpCheck(MemoryContext context);
// TODO(pg-port): BumpCheck under a cfg when MEMORY_CONTEXT_CHECKING is modeled.

/*
 * How many extra bytes do we need to request in order to ensure that we can
 * align a pointer to 'alignto'.  Since palloc'd pointers are already aligned
 * to MAXIMUM_ALIGNOF we can subtract that amount.  We also need to make sure
 * there is enough space for the redirection MemoryChunk.
 */
// #define PallocAlignedExtraBytes(alignto) \
//     ((alignto) + (sizeof(MemoryChunk) - MAXIMUM_ALIGNOF))
// TODO(pg-port): translate once MemoryChunk (memutils_memorychunk.h) is ported;
// it should read:
//   pub const fn PallocAlignedExtraBytes(alignto: Size) -> Size {
//       alignto + (size_of::<MemoryChunk>() - MAXIMUM_ALIGNOF)
//   }

/*
 * MemoryContextMethodID
 *		A unique identifier for each MemoryContext implementation which
 *		indicates the index into the mcxt_methods[] array. See mcxt.c.
 *
 * For robust error detection, ensure that MemoryContextMethodID has a value
 * for each possible bit-pattern of MEMORY_CONTEXT_METHODID_MASK, and make
 * dummy entries for unused IDs in the mcxt_methods[] array.  We also try
 * to avoid using bit-patterns as valid IDs if they are likely to occur in
 * garbage data, or if they could falsely match on chunks that are really from
 * malloc not palloc.  (We can't tell that for most malloc implementations,
 * but it happens that glibc stores flag bits in the same place where we put
 * the MemoryContextMethodID, so the possible values are predictable for it.)
 */
// The ID is stored in MEMORY_CONTEXT_METHODID_BITS (= 4) bits of the 8-byte
// chunk header, so it fits in a u8. #[repr(u8)] matches that storage width.
#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum MemoryContextMethodID {
    MCTX_0_RESERVED_UNUSEDMEM_ID, /* 0000 occurs in never-used memory */
    MCTX_1_RESERVED_GLIBC_ID,     /* glibc malloc'd chunks usually match 0001 */
    MCTX_2_RESERVED_GLIBC_ID,     /* glibc malloc'd chunks > 128kB match 0010 */
    MCTX_ASET_ID,
    MCTX_GENERATION_ID,
    MCTX_SLAB_ID,
    MCTX_ALIGNED_REDIRECT_ID,
    MCTX_BUMP_ID,
    MCTX_8_UNUSED_ID,
    MCTX_9_UNUSED_ID,
    MCTX_10_UNUSED_ID,
    MCTX_11_UNUSED_ID,
    MCTX_12_UNUSED_ID,
    MCTX_13_UNUSED_ID,
    MCTX_14_UNUSED_ID,
    MCTX_15_RESERVED_WIPEDMEM_ID, /* 1111 occurs in wipe_mem'd memory */
}

pub use MemoryContextMethodID::*;

/*
 * The number of bits that 8-byte memory chunk headers can use to encode the
 * MemoryContextMethodID.
 */
pub const MEMORY_CONTEXT_METHODID_BITS: c_int = 4;
pub const MEMORY_CONTEXT_METHODID_MASK: u64 =
    ((1u64) << MEMORY_CONTEXT_METHODID_BITS) - 1;

/*
 * This routine handles the context-type-independent part of memory
 * context creation.  It's intended to be called from context-type-
 * specific creation routines, and noplace else.
 */
// TODO(pg-port): body in mcxt.c.
pub unsafe fn MemoryContextCreate(
    _node: MemoryContext,
    _tag: NodeTag,
    _method_id: MemoryContextMethodID,
    _parent: MemoryContext,
    _name: *const c_char,
) {
    unimplemented!()
}

// TODO(pg-port): body in mcxt.c.
pub unsafe fn MemoryContextAllocationFailure(
    _context: MemoryContext,
    _size: Size,
    _flags: c_int,
) -> *mut c_void {
    unimplemented!()
}

// pg_noreturn: this function never returns (it raises an error). Modeled as `-> !`.
// TODO(pg-port): body in mcxt.c.
pub unsafe fn MemoryContextSizeFailure(_context: MemoryContext, _size: Size, _flags: c_int) -> ! {
    unimplemented!()
}

#[inline]
pub unsafe fn MemoryContextCheckSize(context: MemoryContext, size: Size, flags: c_int) {
    if crate::c::unlikely(!crate::utils::memutils::AllocSizeIsValid(size)) {
        if (flags & crate::utils::palloc::MCXT_ALLOC_HUGE) == 0
            || !crate::utils::memutils::AllocHugeSizeIsValid(size)
        {
            MemoryContextSizeFailure(context, size, flags);
        }
    }
}
