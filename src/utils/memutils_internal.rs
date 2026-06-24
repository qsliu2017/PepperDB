//! Translated from PostgreSQL src/include/utils/memutils_internal.h
//! Declarations for memory allocation utility functions for internal use.

use crate::nodes::memnodes::MemoryContextCounters;
use crate::nodes::nodes::Node;
use crate::utils::memutils::{alloc_huge_size_is_valid, alloc_size_is_valid, MemoryContext};
use crate::utils::palloc::McxtAllocFlags;

/// Stats print callback. C: void (*)(void *passthru, const char *fmt, ...).
/// The variadic format is collapsed to a single formatted string in Rust.
pub type MemoryStatsPrintFunc = fn(passthru: &mut (), msg: &str);

// These functions implement the MemoryContext API for AllocSet context.
pub fn AllocSetAlloc(_context: MemoryContext, _size: usize, _flags: i32) -> *mut u8 {
    unimplemented!()
}
pub fn AllocSetFree(_pointer: *mut u8) {
    unimplemented!()
}
pub fn AllocSetRealloc(_pointer: *mut u8, _size: usize, _flags: i32) -> *mut u8 {
    unimplemented!()
}
pub fn AllocSetReset(_context: MemoryContext) {
    unimplemented!()
}
pub fn AllocSetDelete(_context: MemoryContext) {
    unimplemented!()
}
pub fn AllocSetGetChunkContext(_pointer: *mut u8) -> MemoryContext {
    unimplemented!()
}
pub fn AllocSetGetChunkSpace(_pointer: *mut u8) -> usize {
    unimplemented!()
}
pub fn AllocSetIsEmpty(_context: MemoryContext) -> bool {
    unimplemented!()
}
pub fn AllocSetStats(
    _context: MemoryContext,
    _printfunc: MemoryStatsPrintFunc,
    _passthru: &mut (),
    _totals: &mut MemoryContextCounters,
    _print_to_stderr: bool,
) {
    unimplemented!()
}
// AllocSetCheck: only under MEMORY_CONTEXT_CHECKING; omitted.

// These functions implement the MemoryContext API for Generation context.
pub fn GenerationAlloc(_context: MemoryContext, _size: usize, _flags: i32) -> *mut u8 {
    unimplemented!()
}
pub fn GenerationFree(_pointer: *mut u8) {
    unimplemented!()
}
pub fn GenerationRealloc(_pointer: *mut u8, _size: usize, _flags: i32) -> *mut u8 {
    unimplemented!()
}
pub fn GenerationReset(_context: MemoryContext) {
    unimplemented!()
}
pub fn GenerationDelete(_context: MemoryContext) {
    unimplemented!()
}
pub fn GenerationGetChunkContext(_pointer: *mut u8) -> MemoryContext {
    unimplemented!()
}
pub fn GenerationGetChunkSpace(_pointer: *mut u8) -> usize {
    unimplemented!()
}
pub fn GenerationIsEmpty(_context: MemoryContext) -> bool {
    unimplemented!()
}
pub fn GenerationStats(
    _context: MemoryContext,
    _printfunc: MemoryStatsPrintFunc,
    _passthru: &mut (),
    _totals: &mut MemoryContextCounters,
    _print_to_stderr: bool,
) {
    unimplemented!()
}

// These functions implement the MemoryContext API for Slab context.
pub fn SlabAlloc(_context: MemoryContext, _size: usize, _flags: i32) -> *mut u8 {
    unimplemented!()
}
pub fn SlabFree(_pointer: *mut u8) {
    unimplemented!()
}
pub fn SlabRealloc(_pointer: *mut u8, _size: usize, _flags: i32) -> *mut u8 {
    unimplemented!()
}
pub fn SlabReset(_context: MemoryContext) {
    unimplemented!()
}
pub fn SlabDelete(_context: MemoryContext) {
    unimplemented!()
}
pub fn SlabGetChunkContext(_pointer: *mut u8) -> MemoryContext {
    unimplemented!()
}
pub fn SlabGetChunkSpace(_pointer: *mut u8) -> usize {
    unimplemented!()
}
pub fn SlabIsEmpty(_context: MemoryContext) -> bool {
    unimplemented!()
}
pub fn SlabStats(
    _context: MemoryContext,
    _printfunc: MemoryStatsPrintFunc,
    _passthru: &mut (),
    _totals: &mut MemoryContextCounters,
    _print_to_stderr: bool,
) {
    unimplemented!()
}

// These support palloc_aligned() and are not a fully-fledged MemoryContext type.
pub fn AlignedAllocFree(_pointer: *mut u8) {
    unimplemented!()
}
pub fn AlignedAllocRealloc(_pointer: *mut u8, _size: usize, _flags: i32) -> *mut u8 {
    unimplemented!()
}
pub fn AlignedAllocGetChunkContext(_pointer: *mut u8) -> MemoryContext {
    unimplemented!()
}
pub fn AlignedAllocGetChunkSpace(_pointer: *mut u8) -> usize {
    unimplemented!()
}

// These functions implement the MemoryContext API for the Bump context.
pub fn BumpAlloc(_context: MemoryContext, _size: usize, _flags: i32) -> *mut u8 {
    unimplemented!()
}
pub fn BumpFree(_pointer: *mut u8) {
    unimplemented!()
}
pub fn BumpRealloc(_pointer: *mut u8, _size: usize, _flags: i32) -> *mut u8 {
    unimplemented!()
}
pub fn BumpReset(_context: MemoryContext) {
    unimplemented!()
}
pub fn BumpDelete(_context: MemoryContext) {
    unimplemented!()
}
pub fn BumpGetChunkContext(_pointer: *mut u8) -> MemoryContext {
    unimplemented!()
}
pub fn BumpGetChunkSpace(_pointer: *mut u8) -> usize {
    unimplemented!()
}
pub fn BumpIsEmpty(_context: MemoryContext) -> bool {
    unimplemented!()
}
pub fn BumpStats(
    _context: MemoryContext,
    _printfunc: MemoryStatsPrintFunc,
    _passthru: &mut (),
    _totals: &mut MemoryContextCounters,
    _print_to_stderr: bool,
) {
    unimplemented!()
}

// sizeof(MemoryChunk); MemoryChunk lives in utils/memutils_memorychunk.h.
const SIZEOF_MEMORYCHUNK: usize = 8;

/// Extra bytes needed to align a palloc'd pointer to `alignto`, accounting for
/// the redirection MemoryChunk. MAXIMUM_ALIGNOF is 8 on the target platforms.
pub const fn PallocAlignedExtraBytes(alignto: usize) -> usize {
    alignto + (SIZEOF_MEMORYCHUNK - 8)
}

/// A unique identifier for each MemoryContext implementation, indexing into
/// mcxt_methods[]. Values must cover every bit-pattern of the 4-bit method-id
/// mask, so reserved/unused slots are kept (sequential ordinal enum).
#[repr(u8)]
pub enum MemoryContextMethodID {
    MCTX_0_RESERVED_UNUSEDMEM_ID, // 0000 occurs in never-used memory
    MCTX_1_RESERVED_GLIBC_ID,     // glibc malloc'd chunks usually match 0001
    MCTX_2_RESERVED_GLIBC_ID,     // glibc malloc'd chunks > 128kB match 0010
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
    MCTX_15_RESERVED_WIPEDMEM_ID, // 1111 occurs in wipe_mem'd memory
}

/// Bits an 8-byte chunk header can use to encode the MemoryContextMethodID.
pub const MEMORY_CONTEXT_METHODID_BITS: u32 = 4;
pub const MEMORY_CONTEXT_METHODID_MASK: u64 = (1u64 << MEMORY_CONTEXT_METHODID_BITS) - 1;

/// Context-type-independent part of memory context creation. Called only from
/// context-type-specific creation routines.
pub fn MemoryContextCreate(
    _node: MemoryContext,
    _tag: Node,
    _method_id: MemoryContextMethodID,
    _parent: MemoryContext,
    _name: &str,
) {
    unimplemented!()
}

pub fn MemoryContextAllocationFailure(
    _context: MemoryContext,
    _size: usize,
    _flags: i32,
) -> *mut u8 {
    unimplemented!()
}

// pg_noreturn: raises an out-of-memory error (panics).
#[deprecated(note = "TODO(panic): migrate to Result + ?")]
pub fn MemoryContextSizeFailure(_context: MemoryContext, _size: usize, _flags: i32) -> ! {
    // TODO(panic)
    unimplemented!()
}

#[allow(deprecated)]
pub fn MemoryContextCheckSize(context: MemoryContext, size: usize, flags: i32) {
    if !alloc_size_is_valid(size)
        && (flags & McxtAllocFlags::HUGE.bits() == 0 || !alloc_huge_size_is_valid(size))
    {
        MemoryContextSizeFailure(context, size, flags);
    }
}
