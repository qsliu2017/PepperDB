//! Translation of postgres/src/backend/utils/mmgr/generation.c
//!
//! Generational allocator definitions.
//!
//! Generation is a custom MemoryContext implementation designed for cases of
//! chunks with similar lifespan.
//!
//! Portions Copyright (c) 2017-2025, PostgreSQL Global Development Group
//!
//! IDENTIFICATION
//!	  src/backend/utils/mmgr/generation.c
//!
//!
//!	This memory context is based on the assumption that the chunks are freed
//!	roughly in the same order as they were allocated (FIFO), or in groups with
//!	similar lifespan (generations - hence the name of the context). This is
//!	typical for various queue-like use cases, i.e. when tuples are constructed,
//!	processed and then thrown away.
//!
//!	The memory context uses a very simple approach to free space management.
//!	Instead of a complex global freelist, each block tracks a number
//!	of allocated and freed chunks.  The block is classed as empty when the
//!	number of free chunks is equal to the number of allocated chunks.  When
//!	this occurs, instead of freeing the block, we try to "recycle" it, i.e.
//!	reuse it for new allocations.  This is done by setting the block in the
//!	context's 'freeblock' field.  If the freeblock field is already occupied
//!	by another free block we simply return the newly empty block to malloc.
//!
//!	This approach to free blocks requires fewer malloc/free calls for truly
//!	first allocated, first free'd allocation patterns.
//!
//! NOTE (port staging): this is the REAL GenerationContext allocator, translated
//! additively under `utils/mmgr` while the rest of the crate still uses the
//! context-less bootstrap allocator in `utils::palloc`. Imports are explicit
//! (NOT `crate::prelude::*`) to avoid clashing with the bootstrap
//! `MemoryContext`/`MemoryContextData` symbols. Mirrors the verified sibling
//! aset.rs in structure, import block, malloc/free binding, and handling of
//! memnodes/memorychunk/mcxt.

// c.h: MAXALIGN, Size, uint32, unlikely, Max/Min.
use crate::c::{uint32, unlikely, Max, Min, Size, MAXALIGN};
// pg_config.h: MAXIMUM_ALIGNOF (referenced indirectly by MAXALIGN; kept for clarity).
#[allow(unused_imports)]
use crate::pg_config::MAXIMUM_ALIGNOF;
// nodes/nodes.h: T_GenerationContext (NodeTag) and nodeTag() (used via IsA!).
use crate::nodes::nodes::{nodeTag, NodeTag};
use crate::IsA; // nodes.h IsA! macro (used by GenerationIsValid)
// port/pg_bitutils.h: pg_nextpower2_size_t (round up required_size to pow2).
use crate::port::pg_bitutils::pg_nextpower2_size_t;
// utils/memutils.h: AllocHugeSizeIsValid (for the maxBlockSize Assert).
#[allow(unused_imports)]
use crate::utils::memutils::AllocHugeSizeIsValid;
// utils/palloc.h: allocation flag bits.
#[allow(unused_imports)]
use crate::utils::palloc::{MCXT_ALLOC_HUGE, MCXT_ALLOC_NO_OOM, MCXT_ALLOC_ZERO};
// utils/memutils_internal.h: method ID + context-type-independent helpers.
use crate::utils::mmgr::memutils_internal::{MemoryContextCheckSize, MemoryContextMethodID, MCTX_GENERATION_ID};
// The real MemoryContextCreate / allocation-failure / stats helpers live in
// mcxt.rs (their C bodies are in mcxt.c); use those rather than
// memutils_internal's stubs.
use crate::utils::mmgr::mcxt::{
    MemoryContextAllocationFailure, MemoryContextCreate, MemoryContextResetOnly, MemoryContextStats,
};
// nodes/memnodes.h: the abstract context node + its method-table types.
use crate::utils::mmgr::memnodes::{
    MemoryContext, MemoryContextCounters, MemoryContextData, MemoryStatsPrintFunc,
};
// utils/memutils_memorychunk.h: the MemoryChunk header + encode/decode helpers.
use crate::utils::mmgr::memutils_memorychunk::{
    MemoryChunk, MemoryChunkGetBlock, MemoryChunkGetPointer, MemoryChunkGetValue,
    MemoryChunkIsExternal, MemoryChunkSetHdrMask, MemoryChunkSetHdrMaskExternal,
    PointerGetMemoryChunk, MEMORYCHUNK_MAX_BLOCKOFFSET, MEMORYCHUNK_MAX_VALUE,
};
// lib/ilist.h: the doubly-linked list of blocks. The dlist_container! /
// dlist_foreach! / dlist_foreach_modify! macros are exported crate-wide via
// #[macro_export]; the fns are imported here.
use crate::lib::ilist::{
    dlist_delete, dlist_has_next, dlist_head, dlist_head_node, dlist_init, dlist_is_empty,
    dlist_iter, dlist_mutable_iter, dlist_node, dlist_push_head,
};
use crate::{dlist_container, dlist_foreach, dlist_foreach_modify};
use core::ffi::{c_char, c_int, c_void};
// `Assert!` and `IsA!` are brought into scope crate-wide via #[macro_use].

// generation.c uses raw malloc/free/realloc for its blocks.
extern "C" {
    fn malloc(size: usize) -> *mut c_void;
    fn free(p: *mut c_void);
    #[allow(dead_code)]
    fn realloc(p: *mut c_void, size: usize) -> *mut c_void;
}

// ----------------------------------------------------------------------------
// `InvalidAllocSize` (memutils.h): SIZE_MAX. Used only in
// MEMORY_CONTEXT_CHECKING paths (default-off here). Defined locally verbatim
// from the C header so this module is self-contained.
// TODO(pg-port): hoist to crate::utils::memutils.
// ----------------------------------------------------------------------------
#[allow(dead_code)]
const InvalidAllocSize: Size = Size::MAX;

/// `Generation_BLOCKHDRSZ` = `MAXALIGN(sizeof(GenerationBlock))`
const Generation_BLOCKHDRSZ: Size = MAXALIGN(core::mem::size_of::<GenerationBlock>());
/// `Generation_CHUNKHDRSZ` = `sizeof(MemoryChunk)`
const Generation_CHUNKHDRSZ: Size = core::mem::size_of::<MemoryChunk>();

const Generation_CHUNK_FRACTION: Size = 8;

/// `typedef void *GenerationPointer;`
type GenerationPointer = *mut c_void;

/// GenerationContext is a simple memory context not reusing allocated chunks,
/// and freeing blocks once all chunks are freed.
#[repr(C)]
pub struct GenerationContext {
    /// Standard memory-context fields
    pub header: MemoryContextData,

    // Generational context parameters
    /// initial block size
    initBlockSize: uint32,
    /// maximum block size
    maxBlockSize: uint32,
    /// next block size to allocate
    nextBlockSize: uint32,
    /// effective chunk size limit
    allocChunkLimit: uint32,

    /// current (most recently allocated) block
    block: *mut GenerationBlock,
    /// pointer to an empty block that's being recycled, or NULL if there's no
    /// such block.
    freeblock: *mut GenerationBlock,
    /// list of blocks
    blocks: dlist_head,
}

/// GenerationBlock
///		GenerationBlock is the unit of memory that is obtained by generation.c
///		from malloc().  It contains zero or more MemoryChunks, which are the
///		units requested by palloc() and freed by pfree().  MemoryChunks cannot
///		be returned to malloc() individually, instead pfree() updates the free
///		counter of the block and when all chunks in a block are free the whole
///		block can be returned to malloc().
///
///		GenerationBlock is the header data for a block --- the usable space
///		within the block begins at the next alignment boundary.
#[repr(C)]
struct GenerationBlock {
    /// doubly-linked list of blocks
    node: dlist_node,
    /// pointer back to the owning context
    context: *mut GenerationContext,
    /// allocated size of this block
    blksize: Size,
    /// number of chunks in the block
    nchunks: c_int,
    /// number of free chunks
    nfree: c_int,
    /// start of free space in this block
    freeptr: *mut c_char,
    /// end of space in this block
    endptr: *mut c_char,
}

/// GenerationIsValid
///		True iff set is valid generation set.
///
/// `GenerationIsValid(set) = (PointerIsValid(set) && IsA(set, GenerationContext))`
///
/// # Safety
/// `set` must be NULL or point to a node beginning with a NodeTag.
#[inline]
unsafe fn GenerationIsValid(set: *mut GenerationContext) -> bool {
    !set.is_null() && IsA!(set, T_GenerationContext)
}

/// GenerationBlockIsValid
///		True iff block is valid block of generation set.
///
/// `GenerationBlockIsValid(block) = (PointerIsValid(block) && GenerationIsValid((block)->context))`
///
/// # Safety
/// `block` must be NULL or point to a live GenerationBlock.
#[inline]
unsafe fn GenerationBlockIsValid(block: *mut GenerationBlock) -> bool {
    !block.is_null() && GenerationIsValid((*block).context)
}

/// GenerationBlockIsEmpty
///		True iff block contains no chunks
///
/// `GenerationBlockIsEmpty(b) = ((b)->nchunks == 0)`
///
/// # Safety
/// `b` must point to a live GenerationBlock.
#[inline]
unsafe fn GenerationBlockIsEmpty(b: *mut GenerationBlock) -> bool {
    (*b).nchunks == 0
}

/// We always store external chunks on a dedicated block.  This makes fetching
/// the block from an external chunk easy since it's always the first and only
/// chunk on the block.
///
/// `ExternalChunkGetBlock(chunk) = (GenerationBlock *) ((char *) chunk - Generation_BLOCKHDRSZ)`
///
/// # Safety
/// `chunk` must be the (sole) external chunk of a dedicated block.
#[inline]
unsafe fn ExternalChunkGetBlock(chunk: *mut MemoryChunk) -> *mut GenerationBlock {
    (chunk as *mut u8).sub(Generation_BLOCKHDRSZ) as *mut GenerationBlock
}

/// Obtain the keeper block for a generation context.
///
/// `KeeperBlock(set) = (GenerationBlock *) (((char *) set) + MAXALIGN(sizeof(GenerationContext)))`
///
/// # Safety
/// `set` must point to a live GenerationContext whose keeper block immediately
/// follows the context header in the same malloc chunk.
#[inline]
unsafe fn KeeperBlock(set: *mut GenerationContext) -> *mut GenerationBlock {
    (set as *mut u8).add(MAXALIGN(core::mem::size_of::<GenerationContext>())) as *mut GenerationBlock
}

/// Check if the block is the keeper block of the given generation context.
///
/// `IsKeeperBlock(set, block) = ((block) == (KeeperBlock(set)))`
///
/// # Safety
/// See `KeeperBlock`.
#[inline]
unsafe fn IsKeeperBlock(set: *mut GenerationContext, block: *mut GenerationBlock) -> bool {
    block == KeeperBlock(set)
}

/*
 * Public routines
 */

/// GenerationContextCreate
///		Create a new Generation context.
///
/// parent: parent context, or NULL if top-level context
/// name: name of context (must be statically allocated)
/// minContextSize: minimum context size
/// initBlockSize: initial allocation block size
/// maxBlockSize: maximum allocation block size
///
/// # Safety
/// `parent` must be NULL or a valid MemoryContext; `name` must be a valid,
/// statically allocated C string.
pub unsafe fn GenerationContextCreate(
    parent: MemoryContext,
    name: *const c_char,
    minContextSize: Size,
    initBlockSize: Size,
    maxBlockSize: Size,
) -> MemoryContext {
    let firstBlockSize: Size;
    let mut allocSize: Size;
    let set: *mut GenerationContext;
    let block: *mut GenerationBlock;

    // ensure MemoryChunk's size is properly maxaligned
    // StaticAssertDecl(Generation_CHUNKHDRSZ == MAXALIGN(Generation_CHUNKHDRSZ),
    //                  "sizeof(MemoryChunk) is not maxaligned");
    const _: () = assert!(Generation_CHUNKHDRSZ == MAXALIGN(Generation_CHUNKHDRSZ));

    // First, validate allocation parameters.  Asserts seem sufficient because
    // nobody varies their parameters at runtime.  We somewhat arbitrarily
    // enforce a minimum 1K block size.  We restrict the maximum block size to
    // MEMORYCHUNK_MAX_BLOCKOFFSET as MemoryChunks are limited to this in
    // regards to addressing the offset between the chunk and the block that
    // the chunk is stored on.  We would be unable to store the offset between
    // the chunk and block for any chunks that were beyond
    // MEMORYCHUNK_MAX_BLOCKOFFSET bytes into the block if the block was to be
    // larger than this.
    Assert!(initBlockSize == MAXALIGN(initBlockSize) && initBlockSize >= 1024);
    Assert!(
        maxBlockSize == MAXALIGN(maxBlockSize)
            && maxBlockSize >= initBlockSize
            && AllocHugeSizeIsValid(maxBlockSize)
    ); // must be safe to double
    Assert!(
        minContextSize == 0
            || (minContextSize == MAXALIGN(minContextSize)
                && minContextSize >= 1024
                && minContextSize <= maxBlockSize)
    );
    Assert!(maxBlockSize as u64 <= MEMORYCHUNK_MAX_BLOCKOFFSET);

    // Determine size of initial block
    allocSize =
        MAXALIGN(core::mem::size_of::<GenerationContext>()) + Generation_BLOCKHDRSZ + Generation_CHUNKHDRSZ;
    if minContextSize != 0 {
        allocSize = Max(allocSize, minContextSize);
    } else {
        allocSize = Max(allocSize, initBlockSize);
    }

    // Allocate the initial block.  Unlike other generation.c blocks, it
    // starts with the context header and its block header follows that.
    set = malloc(allocSize) as *mut GenerationContext;
    if set.is_null() {
        // MemoryContextStats(TopMemoryContext);
        // ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY),
        //                 errmsg("out of memory"),
        //                 errdetail("Failed while creating memory context \"%s\".", name)));
        // Mirror aset.rs: print stats for TopMemoryContext if it exists, then
        // hard-fail (real ereport arrives with full error handling).
        if !crate::utils::palloc::TopMemoryContext.is_null() {
            MemoryContextStats(
                crate::utils::palloc::TopMemoryContext as *mut c_void as MemoryContext,
            );
        }
        // TODO(pg-port): real ereport; for now this is a hard failure.
        panic!("out of memory: failed while creating memory context");
    }

    // Avoid writing code that can fail between here and MemoryContextCreate;
    // we'd leak the header if we ereport in this stretch.
    dlist_init(&mut (*set).blocks);

    // Fill in the initial block's block header
    block = KeeperBlock(set);
    // determine the block size and initialize it
    firstBlockSize = allocSize - MAXALIGN(core::mem::size_of::<GenerationContext>());
    GenerationBlockInit(set, block, firstBlockSize);

    // add it to the doubly-linked list of blocks
    dlist_push_head(&mut (*set).blocks, &mut (*block).node);

    // use it as the current allocation block
    (*set).block = block;

    // No free block, yet
    (*set).freeblock = core::ptr::null_mut();

    // Fill in GenerationContext-specific header fields
    (*set).initBlockSize = initBlockSize as uint32;
    (*set).maxBlockSize = maxBlockSize as uint32;
    (*set).nextBlockSize = initBlockSize as uint32;

    // Compute the allocation chunk size limit for this context.
    //
    // Limit the maximum size a non-dedicated chunk can be so that we can fit
    // at least Generation_CHUNK_FRACTION of chunks this big onto the maximum
    // sized block.  We must further limit this value so that it's no more
    // than MEMORYCHUNK_MAX_VALUE.  We're unable to have non-external chunks
    // larger than that value as we store the chunk size in the MemoryChunk
    // 'value' field in the call to MemoryChunkSetHdrMask().
    (*set).allocChunkLimit = Min(maxBlockSize, MEMORYCHUNK_MAX_VALUE as Size) as uint32;
    while ((*set).allocChunkLimit as Size + Generation_CHUNKHDRSZ)
        > ((maxBlockSize - Generation_BLOCKHDRSZ) / Generation_CHUNK_FRACTION)
    {
        (*set).allocChunkLimit >>= 1;
    }

    // Finally, do the type-independent part of context creation
    MemoryContextCreate(
        set as MemoryContext,
        NodeTag::T_GenerationContext,
        MCTX_GENERATION_ID,
        parent,
        name,
    );

    (*(set as MemoryContext)).mem_allocated = firstBlockSize;

    set as MemoryContext
}

/// GenerationReset
///		Frees all memory which is allocated in the given set.
///
/// The initial "keeper" block (which shares a malloc chunk with the context
/// header) is not given back to the operating system though.  In this way, we
/// don't thrash malloc() when a context is repeatedly reset after small
/// allocations.
///
/// # Safety
/// `context` must be a valid Generation context.
pub unsafe fn GenerationReset(context: MemoryContext) {
    let set: *mut GenerationContext = context as *mut GenerationContext;
    let mut miter: dlist_mutable_iter = core::mem::zeroed();

    Assert!(GenerationIsValid(set));

    // #ifdef MEMORY_CONTEXT_CHECKING
    //     /* Check for corruption and leaks before freeing */
    //     GenerationCheck(context);
    // #endif
    // TODO(pg-port): GenerationCheck under MEMORY_CONTEXT_CHECKING.

    // NULLify the free block pointer.  We must do this before calling
    // GenerationBlockFree as that function never expects to free the
    // freeblock.
    (*set).freeblock = core::ptr::null_mut();

    dlist_foreach_modify!(miter, &mut (*set).blocks, {
        let block: *mut GenerationBlock = dlist_container!(GenerationBlock, node, miter.cur);

        if IsKeeperBlock(set, block) {
            GenerationBlockMarkEmpty(block);
        } else {
            GenerationBlockFree(set, block);
        }
    });

    // set it so new allocations to make use of the keeper block
    (*set).block = KeeperBlock(set);

    // Reset block size allocation sequence, too
    (*set).nextBlockSize = (*set).initBlockSize;

    // Ensure there is only 1 item in the dlist
    Assert!(!dlist_is_empty(&(*set).blocks));
    Assert!(!dlist_has_next(&(*set).blocks, dlist_head_node(&mut (*set).blocks)));
}

/// GenerationDelete
///		Free all memory which is allocated in the given context.
///
/// # Safety
/// `context` must be a valid Generation context.
pub unsafe fn GenerationDelete(context: MemoryContext) {
    // Reset to release all releasable GenerationBlocks
    GenerationReset(context);
    // And free the context header and keeper block
    free(context as *mut c_void);
}

/// Helper for GenerationAlloc() that allocates an entire block for the chunk.
///
/// GenerationAlloc()'s comment explains why this is separate.
///
/// # Safety
/// `context` must be a valid Generation context.
// pg_noinline
#[inline(never)]
unsafe fn GenerationAllocLarge(context: MemoryContext, size: Size, flags: c_int) -> *mut c_void {
    let set: *mut GenerationContext = context as *mut GenerationContext;
    let block: *mut GenerationBlock;
    let chunk: *mut MemoryChunk;
    let chunk_size: Size;
    let required_size: Size;
    let blksize: Size;

    // validate 'size' is within the limits for the given 'flags'
    MemoryContextCheckSize(context, size, flags);

    // #ifdef MEMORY_CONTEXT_CHECKING
    //     /* ensure there's always space for the sentinel byte */
    //     chunk_size = MAXALIGN(size + 1);
    // #else
    chunk_size = MAXALIGN(size);
    // #endif
    required_size = chunk_size + Generation_CHUNKHDRSZ;
    blksize = required_size + Generation_BLOCKHDRSZ;

    block = malloc(blksize) as *mut GenerationBlock;
    if block.is_null() {
        return MemoryContextAllocationFailure(context, size, flags);
    }

    (*context).mem_allocated += blksize;

    // block with a single (used) chunk
    (*block).context = set;
    (*block).blksize = blksize;
    (*block).nchunks = 1;
    (*block).nfree = 0;

    // the block is completely full
    (*block).endptr = (block as *mut u8).add(blksize) as *mut c_char;
    (*block).freeptr = (*block).endptr;

    chunk = (block as *mut u8).add(Generation_BLOCKHDRSZ) as *mut MemoryChunk;

    // mark the MemoryChunk as externally managed
    MemoryChunkSetHdrMaskExternal(chunk, MCTX_GENERATION_ID);

    // #ifdef MEMORY_CONTEXT_CHECKING
    //     chunk->requested_size = size;
    //     /* set mark to catch clobber of "unused" space */
    //     Assert(size < chunk_size);
    //     set_sentinel(MemoryChunkGetPointer(chunk), size);
    // #endif
    // #ifdef RANDOMIZE_ALLOCATED_MEMORY
    //     /* fill the allocated space with junk */
    //     randomize_mem((char *) MemoryChunkGetPointer(chunk), size);
    // #endif
    // TODO(pg-port): MEMORY_CONTEXT_CHECKING / RANDOMIZE_ALLOCATED_MEMORY (default-off).

    // add the block to the list of allocated blocks
    dlist_push_head(&mut (*set).blocks, &mut (*block).node);

    // Ensure any padding bytes are marked NOACCESS.
    // VALGRIND_MAKE_MEM_NOACCESS((char *) MemoryChunkGetPointer(chunk) + size, chunk_size - size);
    // Disallow access to the chunk header.
    // VALGRIND_MAKE_MEM_NOACCESS(chunk, Generation_CHUNKHDRSZ);
    // TODO(pg-port): valgrind no-op.

    MemoryChunkGetPointer(chunk)
}

/// Small helper for allocating a new chunk from a chunk, to avoid duplicating
/// the code between GenerationAlloc() and GenerationAllocFromNewBlock().
///
/// # Safety
/// `block` must be a live block of `context` with enough free space.
#[inline]
unsafe fn GenerationAllocChunkFromBlock(
    _context: MemoryContext,
    block: *mut GenerationBlock,
    size: Size,
    chunk_size: Size,
) -> *mut c_void {
    let chunk: *mut MemoryChunk = (*block).freeptr as *mut MemoryChunk;

    // validate we've been given a block with enough free space
    Assert!(!block.is_null());
    Assert!(
        (((*block).endptr as usize).wrapping_sub((*block).freeptr as usize) as Size)
            >= Generation_CHUNKHDRSZ + chunk_size
    );

    // Prepare to initialize the chunk header.
    // VALGRIND_MAKE_MEM_UNDEFINED(chunk, Generation_CHUNKHDRSZ);
    // TODO(pg-port): valgrind no-op.

    (*block).nchunks += 1;
    (*block).freeptr = ((*block).freeptr as *mut u8).add(Generation_CHUNKHDRSZ + chunk_size)
        as *mut c_char;

    Assert!((*block).freeptr <= (*block).endptr);

    MemoryChunkSetHdrMask(chunk, block as *mut c_void, chunk_size, MCTX_GENERATION_ID);
    // #ifdef MEMORY_CONTEXT_CHECKING
    //     chunk->requested_size = size;
    //     /* set mark to catch clobber of "unused" space */
    //     Assert(size < chunk_size);
    //     set_sentinel(MemoryChunkGetPointer(chunk), size);
    // #endif
    // #ifdef RANDOMIZE_ALLOCATED_MEMORY
    //     /* fill the allocated space with junk */
    //     randomize_mem((char *) MemoryChunkGetPointer(chunk), size);
    // #endif
    // TODO(pg-port): MEMORY_CONTEXT_CHECKING / RANDOMIZE_ALLOCATED_MEMORY (default-off).
    let _ = size;

    // Ensure any padding bytes are marked NOACCESS.
    // VALGRIND_MAKE_MEM_NOACCESS((char *) MemoryChunkGetPointer(chunk) + size, chunk_size - size);
    // Disallow access to the chunk header.
    // VALGRIND_MAKE_MEM_NOACCESS(chunk, Generation_CHUNKHDRSZ);
    // TODO(pg-port): valgrind no-op.

    MemoryChunkGetPointer(chunk)
}

/// Helper for GenerationAlloc() that allocates a new block and returns a chunk
/// allocated from it.
///
/// GenerationAlloc()'s comment explains why this is separate.
///
/// # Safety
/// `context` must be a valid Generation context.
// pg_noinline
#[inline(never)]
unsafe fn GenerationAllocFromNewBlock(
    context: MemoryContext,
    size: Size,
    flags: c_int,
    chunk_size: Size,
) -> *mut c_void {
    let set: *mut GenerationContext = context as *mut GenerationContext;
    let block: *mut GenerationBlock;
    let mut blksize: Size;
    let required_size: Size;

    // The first such block has size initBlockSize, and we double the space in
    // each succeeding block, but not more than maxBlockSize.
    blksize = (*set).nextBlockSize as Size;
    (*set).nextBlockSize <<= 1;
    if (*set).nextBlockSize > (*set).maxBlockSize {
        (*set).nextBlockSize = (*set).maxBlockSize;
    }

    // we'll need space for the chunk, chunk hdr and block hdr
    required_size = chunk_size + Generation_CHUNKHDRSZ + Generation_BLOCKHDRSZ;

    // round the size up to the next power of 2
    if blksize < required_size {
        blksize = pg_nextpower2_size_t(required_size as u64) as Size;
    }

    block = malloc(blksize) as *mut GenerationBlock;

    if block.is_null() {
        return MemoryContextAllocationFailure(context, size, flags);
    }

    (*context).mem_allocated += blksize;

    // initialize the new block
    GenerationBlockInit(set, block, blksize);

    // add it to the doubly-linked list of blocks
    dlist_push_head(&mut (*set).blocks, &mut (*block).node);

    // make this the current block
    (*set).block = block;

    GenerationAllocChunkFromBlock(context, block, size, chunk_size)
}

/// GenerationAlloc
///		Returns a pointer to allocated memory of given size or raises an ERROR
///		on allocation failure, or returns NULL when flags contains
///		MCXT_ALLOC_NO_OOM.
///
/// No request may exceed:
///		MAXALIGN_DOWN(SIZE_MAX) - Generation_BLOCKHDRSZ - Generation_CHUNKHDRSZ
/// All callers use a much-lower limit.
///
/// Note: when using valgrind, it doesn't matter how the returned allocation
/// is marked, as mcxt.c will set it to UNDEFINED.  In some paths we will
/// return space that is marked NOACCESS - GenerationRealloc has to beware!
///
/// This function should only contain the most common code paths.  Everything
/// else should be in pg_noinline helper functions, thus avoiding the overhead
/// of creating a stack frame for the common cases.  Allocating memory is often
/// a bottleneck in many workloads, so avoiding stack frame setup is
/// worthwhile.  Helper functions should always directly return the newly
/// allocated memory so that we can just return that address directly as a tail
/// call.
///
/// # Safety
/// `context` must be a valid Generation context.
pub unsafe fn GenerationAlloc(context: MemoryContext, size: Size, flags: c_int) -> *mut c_void {
    let set: *mut GenerationContext = context as *mut GenerationContext;
    let block: *mut GenerationBlock;
    let chunk_size: Size;
    let required_size: Size;

    Assert!(GenerationIsValid(set));

    // #ifdef MEMORY_CONTEXT_CHECKING
    //     /* ensure there's always space for the sentinel byte */
    //     chunk_size = MAXALIGN(size + 1);
    // #else
    chunk_size = MAXALIGN(size);
    // #endif

    // If requested size exceeds maximum for chunks we hand the request off to
    // GenerationAllocLarge().
    if chunk_size > (*set).allocChunkLimit as Size {
        return GenerationAllocLarge(context, size, flags);
    }

    required_size = chunk_size + Generation_CHUNKHDRSZ;

    // Not an oversized chunk.  We try to first make use of the current block,
    // but if there's not enough space in it, instead of allocating a new
    // block, we look to see if the empty freeblock has enough space.  We
    // don't try reusing the keeper block.  If it's become empty we'll reuse
    // that again only if the context is reset.
    //
    // We only try reusing the freeblock if we've no space for this allocation
    // on the current block.  When a freeblock exists, we'll switch to it once
    // the first time we can't fit an allocation in the current block.  We
    // avoid ping-ponging between the two as we need to be careful not to
    // fragment differently sized consecutive allocations between several
    // blocks.  Going between the two could cause fragmentation for FIFO
    // workloads, which generation is meant to be good at.
    block = (*set).block;

    if unlikely(GenerationBlockFreeBytes(block) < required_size) {
        let freeblock: *mut GenerationBlock = (*set).freeblock;

        // freeblock, if set, must be empty
        Assert!(freeblock.is_null() || GenerationBlockIsEmpty(freeblock));

        // check if we have a freeblock and if it's big enough
        if !freeblock.is_null() && GenerationBlockFreeBytes(freeblock) >= required_size {
            // make the freeblock the current block
            (*set).freeblock = core::ptr::null_mut();
            (*set).block = freeblock;

            return GenerationAllocChunkFromBlock(context, freeblock, size, chunk_size);
        } else {
            // No freeblock, or it's not big enough for this allocation.  Make
            // a new block.
            return GenerationAllocFromNewBlock(context, size, flags, chunk_size);
        }
    }

    // The current block has space, so just allocate chunk there.
    GenerationAllocChunkFromBlock(context, block, size, chunk_size)
}

/// GenerationBlockInit
///		Initializes 'block' assuming 'blksize'.  Does not update the context's
///		mem_allocated field.
///
/// # Safety
/// `block` must point to a malloc'd region of at least `blksize` bytes owned by
/// `context`.
#[inline]
unsafe fn GenerationBlockInit(
    context: *mut GenerationContext,
    block: *mut GenerationBlock,
    blksize: Size,
) {
    (*block).context = context;
    (*block).blksize = blksize;
    (*block).nchunks = 0;
    (*block).nfree = 0;

    (*block).freeptr = (block as *mut u8).add(Generation_BLOCKHDRSZ) as *mut c_char;
    (*block).endptr = (block as *mut u8).add(blksize) as *mut c_char;

    // Mark unallocated space NOACCESS.
    // VALGRIND_MAKE_MEM_NOACCESS(block->freeptr, blksize - Generation_BLOCKHDRSZ);
    // TODO(pg-port): valgrind no-op.
}

/// GenerationBlockMarkEmpty
///		Set a block as empty.  Does not free the block.
///
/// # Safety
/// `block` must point to a live GenerationBlock.
#[inline]
unsafe fn GenerationBlockMarkEmpty(block: *mut GenerationBlock) {
    // #if defined(USE_VALGRIND) || defined(CLOBBER_FREED_MEMORY)
    //     char *datastart = ((char *) block) + Generation_BLOCKHDRSZ;
    // #endif
    //
    // #ifdef CLOBBER_FREED_MEMORY
    //     wipe_mem(datastart, block->freeptr - datastart);
    // #else
    //     /* wipe_mem() would have done this */
    //     VALGRIND_MAKE_MEM_NOACCESS(datastart, block->freeptr - datastart);
    // #endif
    // TODO(pg-port): CLOBBER_FREED_MEMORY/valgrind no-op (default-off path).

    // Reset the block, but don't return it to malloc
    (*block).nchunks = 0;
    (*block).nfree = 0;
    (*block).freeptr = (block as *mut u8).add(Generation_BLOCKHDRSZ) as *mut c_char;
}

/// GenerationBlockFreeBytes
///		Returns the number of bytes free in 'block'
///
/// # Safety
/// `block` must point to a live GenerationBlock.
#[inline]
unsafe fn GenerationBlockFreeBytes(block: *mut GenerationBlock) -> Size {
    ((*block).endptr as usize).wrapping_sub((*block).freeptr as usize) as Size
}

/// GenerationBlockFree
///		Remove 'block' from 'set' and release the memory consumed by it.
///
/// # Safety
/// `block` must be a live, non-keeper, non-freeblock block of `set`.
#[inline]
unsafe fn GenerationBlockFree(set: *mut GenerationContext, block: *mut GenerationBlock) {
    // Make sure nobody tries to free the keeper block
    Assert!(!IsKeeperBlock(set, block));
    // We shouldn't be freeing the freeblock either
    Assert!(block != (*set).freeblock);

    // release the block from the list of blocks
    dlist_delete(&mut (*block).node);

    (*(set as MemoryContext)).mem_allocated -= (*block).blksize;

    // #ifdef CLOBBER_FREED_MEMORY
    //     wipe_mem(block, block->blksize);
    // #endif
    // TODO(pg-port): CLOBBER_FREED_MEMORY no-op (default-off path).

    free(block as *mut c_void);
}

/// GenerationFree
///		Update number of chunks in the block, and consider freeing the block
///		if it's become empty.
///
/// # Safety
/// `pointer` must be a live allocation from a Generation context.
pub unsafe fn GenerationFree(pointer: *mut c_void) {
    let chunk: *mut MemoryChunk = PointerGetMemoryChunk(pointer);
    let block: *mut GenerationBlock;
    let set: *mut GenerationContext;
    // #if (defined(MEMORY_CONTEXT_CHECKING) && defined(USE_ASSERT_CHECKING))
    //     || defined(CLOBBER_FREED_MEMORY)
    //     Size chunksize;
    // #endif
    // TODO(pg-port): chunksize only used under MEMORY_CONTEXT_CHECKING /
    // CLOBBER_FREED_MEMORY (default-off); computed inline in the comments below.

    // Allow access to the chunk header.
    // VALGRIND_MAKE_MEM_DEFINED(chunk, Generation_CHUNKHDRSZ);
    // TODO(pg-port): valgrind no-op.

    if MemoryChunkIsExternal(chunk) {
        block = ExternalChunkGetBlock(chunk);

        // Try to verify that we have a sane block pointer: the block header
        // should reference a generation context.
        if !GenerationBlockIsValid(block) {
            // elog(ERROR, "could not find block containing chunk %p", chunk);
            panic!("could not find block containing chunk {:p}", chunk);
        }

        // #if ... checking/clobber ...
        //     chunksize = block->endptr - (char *) pointer;
        // #endif
        // TODO(pg-port): MEMORY_CONTEXT_CHECKING / CLOBBER_FREED_MEMORY (default-off).
    } else {
        block = MemoryChunkGetBlock(chunk) as *mut GenerationBlock;

        // In this path, for speed reasons we just Assert that the referenced
        // block is good.  Future field experience may show that this Assert
        // had better become a regular runtime test-and-elog check.
        Assert!(GenerationBlockIsValid(block));

        // #if ... checking/clobber ...
        //     chunksize = MemoryChunkGetValue(chunk);
        // #endif
        // TODO(pg-port): MEMORY_CONTEXT_CHECKING / CLOBBER_FREED_MEMORY (default-off).
    }

    // #ifdef MEMORY_CONTEXT_CHECKING
    //     /* Test for someone scribbling on unused space in chunk */
    //     Assert(chunk->requested_size < chunksize);
    //     if (!sentinel_ok(pointer, chunk->requested_size))
    //         elog(WARNING, "detected write past chunk end in %s %p",
    //              ((MemoryContext) block->context)->name, chunk);
    // #endif
    //
    // #ifdef CLOBBER_FREED_MEMORY
    //     wipe_mem(pointer, chunksize);
    // #endif
    //
    // #ifdef MEMORY_CONTEXT_CHECKING
    //     /* Reset requested_size to InvalidAllocSize in freed chunks */
    //     chunk->requested_size = InvalidAllocSize;
    // #endif
    // TODO(pg-port): MEMORY_CONTEXT_CHECKING / CLOBBER_FREED_MEMORY (default-off).

    (*block).nfree += 1;

    Assert!((*block).nchunks > 0);
    Assert!((*block).nfree <= (*block).nchunks);
    Assert!(block != (*(*block).context).freeblock);

    // If there are still allocated chunks in the block, we're done.
    if crate::c::likely((*block).nfree < (*block).nchunks) {
        return;
    }

    set = (*block).context;

    //-----------------------
    // The block this allocation was on has now become completely empty of
    // chunks.  In the general case, we can now return the memory for this
    // block back to malloc.  However, there are cases where we don't want to
    // do that:
    //
    // 1)	If it's the keeper block.  This block was malloc'd in the same
    //		allocation as the context itself and can't be free'd without
    //		freeing the context.
    // 2)	If it's the current block.  We could free this, but doing so would
    //		leave us nothing to set the current block to, so we just mark the
    //		block as empty so new allocations can reuse it again.
    // 3)	If we have no "freeblock" set, then we save a single block for
    //		future allocations to avoid having to malloc a new block again.
    //		This is useful for FIFO workloads as it avoids continual
    //		free/malloc cycles.
    if IsKeeperBlock(set, block) || (*set).block == block {
        GenerationBlockMarkEmpty(block); // case 1 and 2
    } else if (*set).freeblock.is_null() {
        // case 3
        GenerationBlockMarkEmpty(block);
        (*set).freeblock = block;
    } else {
        GenerationBlockFree(set, block); // Otherwise, free it
    }
}

/// GenerationRealloc
///		When handling repalloc, we simply allocate a new chunk, copy the data
///		and discard the old one. The only exception is when the new size fits
///		into the old chunk - in that case we just update chunk header.
///
/// # Safety
/// `pointer` must be a live allocation from a Generation context.
pub unsafe fn GenerationRealloc(pointer: *mut c_void, size: Size, flags: c_int) -> *mut c_void {
    let chunk: *mut MemoryChunk = PointerGetMemoryChunk(pointer);
    let set: *mut GenerationContext;
    let block: *mut GenerationBlock;
    let newPointer: GenerationPointer;
    let mut oldsize: Size;

    // Allow access to the chunk header.
    // VALGRIND_MAKE_MEM_DEFINED(chunk, Generation_CHUNKHDRSZ);
    // TODO(pg-port): valgrind no-op.

    if MemoryChunkIsExternal(chunk) {
        block = ExternalChunkGetBlock(chunk);

        // Try to verify that we have a sane block pointer: the block header
        // should reference a generation context.
        if !GenerationBlockIsValid(block) {
            // elog(ERROR, "could not find block containing chunk %p", chunk);
            panic!("could not find block containing chunk {:p}", chunk);
        }

        oldsize = ((*block).endptr as usize).wrapping_sub(pointer as usize) as Size;
    } else {
        block = MemoryChunkGetBlock(chunk) as *mut GenerationBlock;

        // In this path, for speed reasons we just Assert that the referenced
        // block is good.  Future field experience may show that this Assert
        // had better become a regular runtime test-and-elog check.
        Assert!(GenerationBlockIsValid(block));

        oldsize = MemoryChunkGetValue(chunk);
    }

    set = (*block).context;

    // #ifdef MEMORY_CONTEXT_CHECKING
    //     /* Test for someone scribbling on unused space in chunk */
    //     Assert(chunk->requested_size < oldsize);
    //     if (!sentinel_ok(pointer, chunk->requested_size))
    //         elog(WARNING, "detected write past chunk end in %s %p",
    //              ((MemoryContext) set)->name, chunk);
    // #endif
    // TODO(pg-port): MEMORY_CONTEXT_CHECKING (default-off).

    // Maybe the allocated area already big enough.  (In particular, we always
    // fall out here if the requested size is a decrease.)
    //
    // This memory context does not use power-of-2 chunk sizing and instead
    // carves the chunks to be as small as possible, so most repalloc() calls
    // will end up in the palloc/memcpy/pfree branch.
    //
    // XXX Perhaps we should annotate this condition with unlikely()?
    //
    // #ifdef MEMORY_CONTEXT_CHECKING
    //     /* With MEMORY_CONTEXT_CHECKING, we need an extra byte for the sentinel */
    //     if (oldsize > size)
    // #else
    if oldsize >= size {
        // #endif
        // #ifdef MEMORY_CONTEXT_CHECKING
        //     Size oldrequest = chunk->requested_size;
        // #ifdef RANDOMIZE_ALLOCATED_MEMORY
        //     /* We can only fill the extra space if we know the prior request */
        //     if (size > oldrequest)
        //         randomize_mem((char *) pointer + oldrequest, size - oldrequest);
        // #endif
        //     chunk->requested_size = size;
        //     /*
        //      * If this is an increase, mark any newly-available part UNDEFINED.
        //      * Otherwise, mark the obsolete part NOACCESS.
        //      */
        //     if (size > oldrequest)
        //         VALGRIND_MAKE_MEM_UNDEFINED((char *) pointer + oldrequest, size - oldrequest);
        //     else
        //         VALGRIND_MAKE_MEM_NOACCESS((char *) pointer + size, oldsize - size);
        //     /* set mark to catch clobber of "unused" space */
        //     set_sentinel(pointer, size);
        // #else							/* !MEMORY_CONTEXT_CHECKING */
        //     /*
        //      * We don't have the information to determine whether we're growing
        //      * the old request or shrinking it, so we conservatively mark the
        //      * entire new allocation DEFINED.
        //      */
        //     VALGRIND_MAKE_MEM_NOACCESS(pointer, oldsize);
        //     VALGRIND_MAKE_MEM_DEFINED(pointer, size);
        // #endif
        // TODO(pg-port): MEMORY_CONTEXT_CHECKING / valgrind (default-off, no-op).

        // Disallow access to the chunk header.
        // VALGRIND_MAKE_MEM_NOACCESS(chunk, Generation_CHUNKHDRSZ);
        // TODO(pg-port): valgrind no-op.

        return pointer;
    }

    // allocate new chunk (this also checks size is valid)
    newPointer = GenerationAlloc(set as MemoryContext, size, flags);

    // leave immediately if request was not completed
    if newPointer.is_null() {
        // Disallow access to the chunk header.
        // VALGRIND_MAKE_MEM_NOACCESS(chunk, Generation_CHUNKHDRSZ);
        // TODO(pg-port): valgrind no-op.
        return MemoryContextAllocationFailure(set as MemoryContext, size, flags);
    }

    // GenerationAlloc() may have returned a region that is still NOACCESS.
    // Change it to UNDEFINED for the moment; memcpy() will then transfer
    // definedness from the old allocation to the new.  If we know the old
    // allocation, copy just that much.  Otherwise, make the entire old chunk
    // defined to avoid errors as we copy the currently-NOACCESS trailing
    // bytes.
    // VALGRIND_MAKE_MEM_UNDEFINED(newPointer, size);
    // #ifdef MEMORY_CONTEXT_CHECKING
    //     oldsize = chunk->requested_size;
    // #else
    // VALGRIND_MAKE_MEM_DEFINED(pointer, oldsize);
    // #endif
    // TODO(pg-port): MEMORY_CONTEXT_CHECKING / valgrind (default-off, no-op).
    let _ = &mut oldsize;

    // transfer existing data (certain to fit)
    core::ptr::copy_nonoverlapping(pointer as *const u8, newPointer as *mut u8, oldsize);

    // free old chunk
    GenerationFree(pointer);

    newPointer
}

/// GenerationGetChunkContext
///		Return the MemoryContext that 'pointer' belongs to.
///
/// # Safety
/// `pointer` must be a live allocation from a Generation context.
pub unsafe fn GenerationGetChunkContext(pointer: *mut c_void) -> MemoryContext {
    let chunk: *mut MemoryChunk = PointerGetMemoryChunk(pointer);
    let block: *mut GenerationBlock;

    // Allow access to the chunk header.
    // VALGRIND_MAKE_MEM_DEFINED(chunk, Generation_CHUNKHDRSZ);
    // TODO(pg-port): valgrind no-op.

    if MemoryChunkIsExternal(chunk) {
        block = ExternalChunkGetBlock(chunk);
    } else {
        block = MemoryChunkGetBlock(chunk) as *mut GenerationBlock;
    }

    // Disallow access to the chunk header.
    // VALGRIND_MAKE_MEM_NOACCESS(chunk, Generation_CHUNKHDRSZ);
    // TODO(pg-port): valgrind no-op.

    Assert!(GenerationBlockIsValid(block));
    &mut (*(*block).context).header
}

/// GenerationGetChunkSpace
///		Given a currently-allocated chunk, determine the total space
///		it occupies (including all memory-allocation overhead).
///
/// # Safety
/// `pointer` must be a live allocation from a Generation context.
pub unsafe fn GenerationGetChunkSpace(pointer: *mut c_void) -> Size {
    let chunk: *mut MemoryChunk = PointerGetMemoryChunk(pointer);
    let chunksize: Size;

    // Allow access to the chunk header.
    // VALGRIND_MAKE_MEM_DEFINED(chunk, Generation_CHUNKHDRSZ);
    // TODO(pg-port): valgrind no-op.

    if MemoryChunkIsExternal(chunk) {
        let block: *mut GenerationBlock = ExternalChunkGetBlock(chunk);

        Assert!(GenerationBlockIsValid(block));
        chunksize = ((*block).endptr as usize).wrapping_sub(pointer as usize) as Size;
    } else {
        chunksize = MemoryChunkGetValue(chunk);
    }

    // Disallow access to the chunk header.
    // VALGRIND_MAKE_MEM_NOACCESS(chunk, Generation_CHUNKHDRSZ);
    // TODO(pg-port): valgrind no-op.

    Generation_CHUNKHDRSZ + chunksize
}

/// GenerationIsEmpty
///		Is a GenerationContext empty of any allocated space?
///
/// # Safety
/// `context` must be a valid Generation context.
pub unsafe fn GenerationIsEmpty(context: MemoryContext) -> bool {
    let set: *mut GenerationContext = context as *mut GenerationContext;
    let mut iter: dlist_iter = core::mem::zeroed();

    Assert!(GenerationIsValid(set));

    dlist_foreach!(iter, &mut (*set).blocks, {
        let block: *mut GenerationBlock = dlist_container!(GenerationBlock, node, iter.cur);

        if (*block).nchunks > 0 {
            return false;
        }
    });

    true
}

/// GenerationStats
///		Compute stats about memory consumption of a Generation context.
///
/// printfunc: if not NULL, pass a human-readable stats string to this.
/// passthru: pass this pointer through to printfunc.
/// totals: if not NULL, add stats about this context into *totals.
/// print_to_stderr: print stats to stderr if true, elog otherwise.
///
/// XXX freespace only accounts for empty space at the end of the block, not
/// space of freed chunks (which is unknown).
///
/// # Safety
/// `context` must be a valid Generation context; `totals` must be NULL or valid.
pub unsafe fn GenerationStats(
    context: MemoryContext,
    printfunc: MemoryStatsPrintFunc,
    passthru: *mut c_void,
    totals: *mut MemoryContextCounters,
    print_to_stderr: bool,
) {
    let set: *mut GenerationContext = context as *mut GenerationContext;
    let mut nblocks: Size = 0;
    let mut nchunks: Size = 0;
    let mut nfreechunks: Size = 0;
    let mut totalspace: Size;
    let mut freespace: Size = 0;
    let mut iter: dlist_iter = core::mem::zeroed();

    Assert!(GenerationIsValid(set));

    // Include context header in totalspace
    totalspace = MAXALIGN(core::mem::size_of::<GenerationContext>());

    dlist_foreach!(iter, &mut (*set).blocks, {
        let block: *mut GenerationBlock = dlist_container!(GenerationBlock, node, iter.cur);

        nblocks += 1;
        nchunks += (*block).nchunks as Size;
        nfreechunks += (*block).nfree as Size;
        totalspace += (*block).blksize;
        freespace += ((*block).endptr as usize).wrapping_sub((*block).freeptr as usize) as Size;
    });

    if let Some(printfunc) = printfunc {
        // char stats_string[200];
        // snprintf(stats_string, sizeof(stats_string),
        //          "%zu total in %zu blocks (%zu chunks); %zu free (%zu chunks); %zu used",
        //          totalspace, nblocks, nchunks, freespace, nfreechunks, totalspace - freespace);
        let stats_string = format!(
            "{} total in {} blocks ({} chunks); {} free ({} chunks); {} used\0",
            totalspace,
            nblocks,
            nchunks,
            freespace,
            nfreechunks,
            totalspace - freespace
        );
        printfunc(
            context,
            passthru,
            stats_string.as_ptr() as *const c_char,
            print_to_stderr,
        );
    }

    if !totals.is_null() {
        (*totals).nblocks += nblocks;
        (*totals).freechunks += nfreechunks;
        (*totals).totalspace += totalspace;
        (*totals).freespace += freespace;
    }
}

// #ifdef MEMORY_CONTEXT_CHECKING
//
// GenerationCheck
//		Walk through chunks and check consistency of memory.
//
// NOTE: report errors as WARNING, *not* ERROR or FATAL.  Otherwise you'll
// find yourself in an infinite loop when trouble occurs, because this
// routine will be entered again when elog cleanup tries to release memory!
//
// TODO(pg-port): translate GenerationCheck under a cfg gating
// MEMORY_CONTEXT_CHECKING. It walks every block's chunk chain validating
// nfree<=nchunks, block->context==gen, per-chunk block links, chunk sizes,
// requested_size/sentinels (sentinel_ok), that at most one external chunk lives
// on a dedicated block, and that total_allocated == context->mem_allocated.
// Omitted here because the default build (no MEMORY_CONTEXT_CHECKING) never
// compiles it, and it depends on the not-yet-modeled requested_size field and
// set_sentinel/sentinel_ok helpers.
// #endif							/* MEMORY_CONTEXT_CHECKING */

// =============================================================================
// Summary
// -----------------------------------------------------------------------------
// Structs + layout (all #[repr(C)], identifiers verbatim):
//   - GenerationContext: FIRST field is the embedded `header: MemoryContextData`,
//     then the uint32 params (initBlockSize/maxBlockSize/nextBlockSize/
//     allocChunkLimit), `block`/`freeblock: *mut GenerationBlock`, and
//     `blocks: dlist_head`. The context type is T_GenerationContext, method_id
//     MCTX_GENERATION_ID.
//   - GenerationBlock: node (dlist_node)/context/blksize/nchunks/nfree/freeptr/
//     endptr. Blocks live on the intrusive dlist (lib::ilist).
//   - GenerationPointer = *mut c_void (the C `typedef void *GenerationPointer`).
//
// The 9 method-table fns (signatures match the MemoryContextMethods fn-pointer
// types in memnodes.rs, so mcxt's table can point at them):
//   GenerationAlloc(context, size, flags) -> *mut c_void
//   GenerationFree(pointer)
//   GenerationRealloc(pointer, size, flags) -> *mut c_void
//   GenerationReset(context)
//   GenerationDelete(context)
//   GenerationGetChunkContext(pointer) -> MemoryContext
//   GenerationGetChunkSpace(pointer) -> Size
//   GenerationIsEmpty(context) -> bool
//   GenerationStats(context, printfunc, passthru, totals, print_to_stderr)
// All are `pub unsafe fn`. The public constructor GenerationContextCreate(parent,
// name, minContextSize, initBlockSize, maxBlockSize) -> MemoryContext sets the
// node tag + method id via MemoryContextCreate (the real mcxt.c routine). The
// pg_noinline helpers GenerationAllocLarge / GenerationAllocFromNewBlock are
// private (#[inline(never)]); GenerationAllocChunkFromBlock is the inline shared
// helper; GenerationBlockInit/MarkEmpty/FreeBytes/Free are inline block helpers.
//
// Wrapping arithmetic: every `(char *)` pointer-difference (endptr - freeptr,
// endptr - pointer, KeeperBlock offsets are .add()/.sub()) uses `usize`
// wrapping_sub to mirror C pointer subtraction without Rust debug overflow
// panics (GenerationBlockFreeBytes, GenerationAllocChunkFromBlock's space
// Assert, GenerationGetChunkSpace, GenerationRealloc oldsize, GenerationStats
// freespace). The block-doubling `<<= 1` and allocChunkLimit `>>= 1` shifts are
// plain shifts, bounded by maxBlockSize exactly as in C. `chunk_size > limit`
// comparisons keep the `(x as usize) < Y`/`as Size` parenthesization. sizeof ->
// core::mem::size_of, offsetof -> via dlist_container! (offset_of!), memcpy ->
// copy_nonoverlapping, NULL -> null_mut()/.is_null().
//
// malloc/free binding: `extern "C" { malloc/free/realloc }` (raw libc), used for
// blocks exactly as generation.c does (realloc unused here, kept for parity with
// aset.rs and marked dead_code); coexists with the bootstrap utils::palloc.
//
// Stubbed: all VALGRIND_* hooks are no-ops; MEMORY_CONTEXT_CHECKING /
// RANDOMIZE_ALLOCATED_MEMORY / CLOBBER_FREED_MEMORY branches translate the
// default-off (#else / non-checking) path, with the checking branches preserved
// as comments + TODO(pg-port). GenerationCheck is omitted (compiled only under
// MEMORY_CONTEXT_CHECKING). ereport/elog become panic!. The OOM path in
// GenerationContextCreate mirrors aset.rs (stats for palloc::TopMemoryContext
// then panic). InvalidAllocSize is defined locally (checking-only; dead_code).
//
// Every generation.c function was translated: GenerationContextCreate,
// GenerationReset, GenerationDelete, GenerationAllocLarge,
// GenerationAllocChunkFromBlock, GenerationAllocFromNewBlock, GenerationAlloc,
// GenerationBlockInit, GenerationBlockMarkEmpty, GenerationBlockFreeBytes,
// GenerationBlockFree, GenerationFree, GenerationRealloc,
// GenerationGetChunkContext, GenerationGetChunkSpace, GenerationIsEmpty,
// GenerationStats (GenerationCheck noted as a cfg-gated TODO).
// =============================================================================
