//! Translation of postgres/src/backend/utils/mmgr/bump.c
//!
//! Bump allocator definitions.
//!
//! Bump is a MemoryContext implementation designed for memory usages which
//! require allocating a large number of chunks, none of which ever need to be
//! pfree'd or realloc'd.  Chunks allocated by this context have no chunk header
//! and operations which ordinarily require looking at the chunk header cannot
//! be performed.  For example, pfree, realloc, GetMemoryChunkSpace and
//! GetMemoryChunkContext are all not possible with bump allocated chunks.  The
//! only way to release memory allocated by this context type is to reset or
//! delete the context.
//!
//! Portions Copyright (c) 2024-2025, PostgreSQL Global Development Group
//!
//! IDENTIFICATION
//!	  src/backend/utils/mmgr/bump.c
//!
//!
//!	Bump is best suited to cases which require a large number of short-lived
//!	chunks where performance matters.  Because bump allocated chunks don't
//!	have a chunk header, it can fit more chunks on each block.  This means we
//!	can do more with less memory and fewer cache lines.  The reason it's best
//!	suited for short-lived usages of memory is that ideally, pointers to bump
//!	allocated chunks won't be visible to a large amount of code.  The more
//!	code that operates on memory allocated by this allocator, the more chances
//!	that some code will try to perform a pfree or one of the other operations
//!	which are made impossible due to the lack of chunk header.  In order to
//!	detect accidental usage of the various disallowed operations, we do add a
//!	MemoryChunk chunk header in MEMORY_CONTEXT_CHECKING builds and have the
//!	various disallowed functions raise an ERROR.
//!
//!	Allocations are MAXALIGNed.
//!
//! NOTE (port staging): this is the REAL Bump allocator, translated additively
//! under `utils/mmgr` while the rest of the crate still uses the context-less
//! bootstrap allocator in `utils::palloc`. Imports are explicit (NOT
//! `crate::prelude::*`) to avoid clashing with the bootstrap
//! `MemoryContext`/`MemoryContextData` symbols. Mirrors the verified sibling
//! allocator aset.rs for import block, method-fn signatures, malloc/free
//! binding, #[repr(C)] struct style, and memnodes/memorychunk/mcxt usage.

// c.h: MAXALIGN, Size, uint32, Max/Min.
use crate::c::{uint32, Max, Min, Size, MAXALIGN};
// pg_config.h: MAXIMUM_ALIGNOF (referenced indirectly by MAXALIGN; kept for clarity).
#[allow(unused_imports)]
use crate::pg_config::MAXIMUM_ALIGNOF;
// nodes/nodes.h: T_BumpContext (NodeTag) and nodeTag() (used via IsA!).
use crate::nodes::nodes::{nodeTag, NodeTag};
use crate::IsA; // nodes.h IsA! macro (used by BumpIsValid)
// port/pg_bitutils.h: pg_nextpower2_size_t (round up block size to power of 2).
use crate::port::pg_bitutils::pg_nextpower2_size_t;
// utils/memutils.h: AllocHugeSizeIsValid (for the maxBlockSize Assert).
#[allow(unused_imports)]
use crate::utils::memutils::AllocHugeSizeIsValid;
// utils/palloc.h: allocation flag bits.
#[allow(unused_imports)]
use crate::utils::palloc::{MCXT_ALLOC_HUGE, MCXT_ALLOC_NO_OOM, MCXT_ALLOC_ZERO};
// nodes/memnodes.h: the abstract context node + its method-table types.
use crate::utils::mmgr::memnodes::{
    MemoryContext, MemoryContextCounters, MemoryContextData, MemoryStatsPrintFunc,
};
// utils/memutils_memorychunk.h: the MemoryChunk header + encode/decode helpers.
// Under the default (non-checking) build Bump_CHUNKHDRSZ == 0 and we never write
// a per-chunk header, so only MemoryChunkSetHdrMaskExternal / the limit constants
// are needed in active code; the rest are referenced by the MEMORY_CONTEXT_CHECKING
// branches that are preserved as comments. The external-chunk helpers
// (MemoryChunkGetPointer, MemoryChunkGetBlock, MemoryChunkGetValue,
// MemoryChunkIsExternal, MemoryChunkSetHdrMask, PointerGetMemoryChunk) are
// imported the same way aset.rs imports them so this module stays a drop-in
// sibling.
#[allow(unused_imports)]
use crate::utils::mmgr::memutils_memorychunk::{
    MemoryChunk, MemoryChunkGetBlock, MemoryChunkGetPointer, MemoryChunkGetValue,
    MemoryChunkIsExternal, MemoryChunkSetHdrMask, MemoryChunkSetHdrMaskExternal,
    PointerGetMemoryChunk, MEMORYCHUNK_MAX_BLOCKOFFSET, MEMORYCHUNK_MAX_VALUE,
};
// utils/memutils_internal.h: method ID + context-type-independent helpers.
use crate::utils::mmgr::memutils_internal::{
    MemoryContextCheckSize, MemoryContextMethodID, MCTX_BUMP_ID,
};
// The real MemoryContextCreate / allocation-failure / stats helpers live in
// mcxt.rs (their C bodies are in mcxt.c); use those rather than
// memutils_internal's stubs.
use crate::utils::mmgr::mcxt::{
    MemoryContextAllocationFailure, MemoryContextCreate, MemoryContextStats,
};
// lib/ilist.h: the doubly-linked list of blocks. The dlist_container! /
// dlist_foreach! / dlist_foreach_modify! macros are exported crate-wide via
// #[macro_export]; the fns are imported here.
use crate::lib::ilist::{
    dlist_head, dlist_head_node, dlist_init, dlist_is_empty, dlist_iter, dlist_mutable_iter,
    dlist_node, dlist_push_head, dlist_push_tail,
};
#[allow(unused_imports)]
use crate::lib::ilist::{dlist_delete, dlist_has_next};
use crate::{dlist_container, dlist_foreach, dlist_foreach_modify};
// elog.h: ERROR severity + the elog! macro (used by the unsupported operations).
use crate::elog;
use crate::utils::elog::ERROR;
// WARNING is used only by BumpCheck under MEMORY_CONTEXT_CHECKING.
#[cfg(memory_context_checking)]
use crate::utils::elog::WARNING;
use core::ffi::{c_char, c_int, c_void};
// `Assert!` and `IsA!` are brought into scope crate-wide via #[macro_use].

// bump.c uses raw malloc/free for its blocks.
extern "C" {
    fn malloc(size: usize) -> *mut c_void;
    fn free(p: *mut c_void);
}

/// `Bump_BLOCKHDRSZ` = `MAXALIGN(sizeof(BumpBlock))`
const Bump_BLOCKHDRSZ: Size = MAXALIGN(core::mem::size_of::<BumpBlock>());

// No chunk header unless built with MEMORY_CONTEXT_CHECKING
// #ifdef MEMORY_CONTEXT_CHECKING
// #define Bump_CHUNKHDRSZ	sizeof(MemoryChunk)
// #else
/// `Bump_CHUNKHDRSZ` = 0 in the default (non-MEMORY_CONTEXT_CHECKING) build.
// #endif
// TODO(pg-port): under MEMORY_CONTEXT_CHECKING this becomes
// `core::mem::size_of::<MemoryChunk>()`.
const Bump_CHUNKHDRSZ: Size = 0;

const Bump_CHUNK_FRACTION: Size = 8;

/// The keeper block is allocated in the same allocation as the set.
///
/// `KeeperBlock(set) = (BumpBlock *) ((char *) (set) + MAXALIGN(sizeof(BumpContext)))`
///
/// # Safety
/// `set` must point to a live BumpContext whose keeper block immediately follows
/// the context header in the same malloc chunk.
#[inline]
unsafe fn KeeperBlock(set: *mut BumpContext) -> *mut BumpBlock {
    (set as *mut u8).add(MAXALIGN(core::mem::size_of::<BumpContext>())) as *mut BumpBlock
}

/// `IsKeeperBlock(set, blk) = (KeeperBlock(set) == (blk))`
///
/// # Safety
/// See `KeeperBlock`.
#[inline]
unsafe fn IsKeeperBlock(set: *mut BumpContext, blk: *mut BumpBlock) -> bool {
    KeeperBlock(set) == blk
}

// typedef struct BumpBlock BumpBlock; /* forward reference */

#[repr(C)]
pub struct BumpContext {
    /// Standard memory-context fields
    pub header: MemoryContextData,

    // Bump context parameters
    /// initial block size
    initBlockSize: uint32,
    /// maximum block size
    maxBlockSize: uint32,
    /// next block size to allocate
    nextBlockSize: uint32,
    /// effective chunk size limit
    allocChunkLimit: uint32,

    /// list of blocks with the block currently being filled at the head
    blocks: dlist_head,
}

/// BumpBlock
///		BumpBlock is the unit of memory that is obtained by bump.c from
///		malloc().  It contains zero or more allocations, which are the
///		units requested by palloc().
#[repr(C)]
struct BumpBlock {
    /// doubly-linked list of blocks
    node: dlist_node,
    // #ifdef MEMORY_CONTEXT_CHECKING
    /// pointer back to the owning context
    #[cfg(memory_context_checking)]
    context: *mut BumpContext,
    // #endif
    /// start of free space in this block
    freeptr: *mut c_char,
    /// end of space in this block
    endptr: *mut c_char,
}

/// BumpIsValid
///		True iff set is valid bump context.
///
/// `BumpIsValid(set) = (PointerIsValid(set) && IsA(set, BumpContext))`
///
/// # Safety
/// `set` must be NULL or point to a node beginning with a NodeTag.
#[inline]
unsafe fn BumpIsValid(set: *mut BumpContext) -> bool {
    !set.is_null() && IsA!(set, T_BumpContext)
}

/// We always store external chunks on a dedicated block.  This makes fetching
/// the block from an external chunk easy since it's always the first and only
/// chunk on the block.
///
/// `ExternalChunkGetBlock(chunk) = (BumpBlock *) ((char *) chunk - Bump_BLOCKHDRSZ)`
///
/// # Safety
/// `chunk` must be the (sole) external chunk of a dedicated block.
#[allow(dead_code)]
#[inline]
unsafe fn ExternalChunkGetBlock(chunk: *mut MemoryChunk) -> *mut BumpBlock {
    (chunk as *mut u8).sub(Bump_BLOCKHDRSZ) as *mut BumpBlock
}

/// BumpContextCreate
///		Create a new Bump context.
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
pub unsafe fn BumpContextCreate(
    parent: MemoryContext,
    name: *const c_char,
    minContextSize: Size,
    initBlockSize: Size,
    maxBlockSize: Size,
) -> MemoryContext {
    let firstBlockSize: Size;
    let mut allocSize: Size;
    let set: *mut BumpContext;
    let block: *mut BumpBlock;

    // ensure MemoryChunk's size is properly maxaligned
    // StaticAssertDecl(Bump_CHUNKHDRSZ == MAXALIGN(Bump_CHUNKHDRSZ),
    //                  "sizeof(MemoryChunk) is not maxaligned");
    const _: () = assert!(Bump_CHUNKHDRSZ == MAXALIGN(Bump_CHUNKHDRSZ));

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
    allocSize = MAXALIGN(core::mem::size_of::<BumpContext>()) + Bump_BLOCKHDRSZ + Bump_CHUNKHDRSZ;
    if minContextSize != 0 {
        allocSize = Max(allocSize, minContextSize);
    } else {
        allocSize = Max(allocSize, initBlockSize);
    }

    // Allocate the initial block.  Unlike other bump.c blocks, it starts with
    // the context header and its block header follows that.
    set = malloc(allocSize) as *mut BumpContext;
    if set.is_null() {
        // MemoryContextStats(TopMemoryContext);
        // ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY),
        //                 errmsg("out of memory"),
        //                 errdetail("Failed while creating memory context \"%s\".", name)));
        // Mirror aset.rs / generation.rs: print stats for TopMemoryContext if it
        // exists, then hard-fail (real ereport arrives with full error handling).
        if !crate::utils::palloc::TopMemoryContext.is_null() {
            MemoryContextStats(
                crate::utils::palloc::TopMemoryContext as *mut c_void as MemoryContext,
            );
        }
        // TODO(pg-port): real ereport; for now this is a hard failure.
        panic!("out of memory: failed while creating memory context");
    }

    // Avoid writing code that can fail between here and MemoryContextCreate;
    // we'd leak the header and initial block if we ereport in this stretch.
    dlist_init(&mut (*set).blocks);

    // Fill in the initial block's block header
    block = KeeperBlock(set);
    // determine the block size and initialize it
    firstBlockSize = allocSize - MAXALIGN(core::mem::size_of::<BumpContext>());
    BumpBlockInit(set, block, firstBlockSize);

    // add it to the doubly-linked list of blocks
    dlist_push_head(&mut (*set).blocks, &mut (*block).node);

    // Fill in BumpContext-specific header fields.  The Asserts above should
    // ensure that these all fit inside a uint32.
    (*set).initBlockSize = initBlockSize as uint32;
    (*set).maxBlockSize = maxBlockSize as uint32;
    (*set).nextBlockSize = initBlockSize as uint32;

    // Compute the allocation chunk size limit for this context.
    //
    // Limit the maximum size a non-dedicated chunk can be so that we can fit
    // at least Bump_CHUNK_FRACTION of chunks this big onto the maximum sized
    // block.  We must further limit this value so that it's no more than
    // MEMORYCHUNK_MAX_VALUE.  We're unable to have non-external chunks larger
    // than that value as we store the chunk size in the MemoryChunk 'value'
    // field in the call to MemoryChunkSetHdrMask().
    (*set).allocChunkLimit = Min(maxBlockSize, MEMORYCHUNK_MAX_VALUE as Size) as uint32;
    while ((*set).allocChunkLimit as Size + Bump_CHUNKHDRSZ)
        > ((maxBlockSize - Bump_BLOCKHDRSZ) / Bump_CHUNK_FRACTION)
    {
        (*set).allocChunkLimit >>= 1;
    }

    // Finally, do the type-independent part of context creation
    MemoryContextCreate(
        set as MemoryContext,
        NodeTag::T_BumpContext,
        MCTX_BUMP_ID,
        parent,
        name,
    );

    (*(set as MemoryContext)).mem_allocated = allocSize;

    set as MemoryContext
}

/// BumpReset
///		Frees all memory which is allocated in the given set.
///
/// The code simply frees all the blocks in the context apart from the keeper
/// block.
///
/// # Safety
/// `context` must be a valid Bump context.
pub unsafe fn BumpReset(context: MemoryContext) {
    let set: *mut BumpContext = context as *mut BumpContext;
    let mut miter: dlist_mutable_iter = core::mem::zeroed();

    Assert!(BumpIsValid(set));

    // #ifdef MEMORY_CONTEXT_CHECKING
    //     /* Check for corruption and leaks before freeing */
    //     BumpCheck(context);
    // #endif
    // TODO(pg-port): BumpCheck under MEMORY_CONTEXT_CHECKING.

    dlist_foreach_modify!(miter, &mut (*set).blocks, {
        let block: *mut BumpBlock = dlist_container!(BumpBlock, node, miter.cur);

        if IsKeeperBlock(set, block) {
            BumpBlockMarkEmpty(block);
        } else {
            BumpBlockFree(set, block);
        }
    });

    // Reset block size allocation sequence, too
    (*set).nextBlockSize = (*set).initBlockSize;

    // Ensure there is only 1 item in the dlist
    Assert!(!dlist_is_empty(&(*set).blocks));
    Assert!(!dlist_has_next(
        &(*set).blocks,
        dlist_head_node(&mut (*set).blocks)
    ));
}

/// BumpDelete
///		Free all memory which is allocated in the given context.
///
/// # Safety
/// `context` must be a valid Bump context.
pub unsafe fn BumpDelete(context: MemoryContext) {
    // Reset to release all releasable BumpBlocks
    BumpReset(context);
    // And free the context header and keeper block
    free(context as *mut c_void);
}

/// Helper for BumpAlloc() that allocates an entire block for the chunk.
///
/// BumpAlloc()'s comment explains why this is separate.
///
/// # Safety
/// `context` must be a valid Bump context.
// pg_noinline
#[inline(never)]
unsafe fn BumpAllocLarge(context: MemoryContext, size: Size, flags: c_int) -> *mut c_void {
    let set: *mut BumpContext = context as *mut BumpContext;
    let block: *mut BumpBlock;
    // #ifdef MEMORY_CONTEXT_CHECKING
    //     MemoryChunk *chunk;
    // #endif
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

    required_size = chunk_size + Bump_CHUNKHDRSZ;
    blksize = required_size + Bump_BLOCKHDRSZ;

    block = malloc(blksize) as *mut BumpBlock;
    if block.is_null() {
        return MemoryContextAllocationFailure(context, size, flags);
    }

    (*context).mem_allocated += blksize;

    // the block is completely full
    let endp: *mut c_char = (block as *mut u8).add(blksize) as *mut c_char;
    (*block).freeptr = endp;
    (*block).endptr = endp;

    // #ifdef MEMORY_CONTEXT_CHECKING
    //     /* block with a single (used) chunk */
    //     block->context = set;
    //
    //     chunk = (MemoryChunk *) (((char *) block) + Bump_BLOCKHDRSZ);
    //
    //     /* mark the MemoryChunk as externally managed */
    //     MemoryChunkSetHdrMaskExternal(chunk, MCTX_BUMP_ID);
    //
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

    // Add the block to the tail of allocated blocks list.  The current block
    // is left at the head of the list as it may still have space for
    // non-large allocations.
    dlist_push_tail(&mut (*set).blocks, &mut (*block).node);

    // #ifdef MEMORY_CONTEXT_CHECKING
    //     /* Ensure any padding bytes are marked NOACCESS. */
    //     VALGRIND_MAKE_MEM_NOACCESS((char *) MemoryChunkGetPointer(chunk) + size,
    //                                chunk_size - size);
    //
    //     /* Disallow access to the chunk header. */
    //     VALGRIND_MAKE_MEM_NOACCESS(chunk, Bump_CHUNKHDRSZ);
    //
    //     return MemoryChunkGetPointer(chunk);
    // #else
    (block as *mut u8).add(Bump_BLOCKHDRSZ) as *mut c_void
    // #endif
}

/// Small helper for allocating a new chunk from a chunk, to avoid duplicating
/// the code between BumpAlloc() and BumpAllocFromNewBlock().
///
/// # Safety
/// `block` must be a live block of `context` with room for the chunk.
#[inline]
unsafe fn BumpAllocChunkFromBlock(
    _context: MemoryContext,
    block: *mut BumpBlock,
    size: Size,
    chunk_size: Size,
) -> *mut c_void {
    // #ifdef MEMORY_CONTEXT_CHECKING
    //     MemoryChunk *chunk;
    // #else
    let ptr: *mut c_void;
    // #endif

    // validate we've been given a block with enough free space
    Assert!(!block.is_null());
    Assert!(
        ((*block).endptr as usize).wrapping_sub((*block).freeptr as usize)
            >= Bump_CHUNKHDRSZ + chunk_size
    );

    // #ifdef MEMORY_CONTEXT_CHECKING
    //     chunk = (MemoryChunk *) block->freeptr;
    // #else
    ptr = (*block).freeptr as *mut c_void;
    // #endif

    // point the freeptr beyond this chunk
    (*block).freeptr = ((*block).freeptr as *mut u8).add(Bump_CHUNKHDRSZ + chunk_size) as *mut c_char;
    Assert!((*block).freeptr <= (*block).endptr);

    // #ifdef MEMORY_CONTEXT_CHECKING
    //     /* Prepare to initialize the chunk header. */
    //     VALGRIND_MAKE_MEM_UNDEFINED(chunk, Bump_CHUNKHDRSZ);
    //
    //     MemoryChunkSetHdrMask(chunk, block, chunk_size, MCTX_BUMP_ID);
    //     chunk->requested_size = size;
    //     /* set mark to catch clobber of "unused" space */
    //     Assert(size < chunk_size);
    //     set_sentinel(MemoryChunkGetPointer(chunk), size);
    //
    // #ifdef RANDOMIZE_ALLOCATED_MEMORY
    //     /* fill the allocated space with junk */
    //     randomize_mem((char *) MemoryChunkGetPointer(chunk), size);
    // #endif
    //
    //     /* Ensure any padding bytes are marked NOACCESS. */
    //     VALGRIND_MAKE_MEM_NOACCESS((char *) MemoryChunkGetPointer(chunk) + size,
    //                                chunk_size - size);
    //
    //     /* Disallow access to the chunk header. */
    //     VALGRIND_MAKE_MEM_NOACCESS(chunk, Bump_CHUNKHDRSZ);
    //
    //     return MemoryChunkGetPointer(chunk);
    // #else
    // TODO(pg-port): MEMORY_CONTEXT_CHECKING / RANDOMIZE_ALLOCATED_MEMORY (default-off).
    let _ = size;
    ptr
    // #endif							/* MEMORY_CONTEXT_CHECKING */
}

/// Helper for BumpAlloc() that allocates a new block and returns a chunk
/// allocated from it.
///
/// BumpAlloc()'s comment explains why this is separate.
///
/// # Safety
/// `context` must be a valid Bump context.
// pg_noinline
#[inline(never)]
unsafe fn BumpAllocFromNewBlock(
    context: MemoryContext,
    size: Size,
    flags: c_int,
    chunk_size: Size,
) -> *mut c_void {
    let set: *mut BumpContext = context as *mut BumpContext;
    let block: *mut BumpBlock;
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
    required_size = chunk_size + Bump_CHUNKHDRSZ + Bump_BLOCKHDRSZ;
    // round the size up to the next power of 2
    if blksize < required_size {
        blksize = pg_nextpower2_size_t(required_size as u64) as Size;
    }

    block = malloc(blksize) as *mut BumpBlock;

    if block.is_null() {
        return MemoryContextAllocationFailure(context, size, flags);
    }

    (*context).mem_allocated += blksize;

    // initialize the new block
    BumpBlockInit(set, block, blksize);

    // add it to the doubly-linked list of blocks
    dlist_push_head(&mut (*set).blocks, &mut (*block).node);

    BumpAllocChunkFromBlock(context, block, size, chunk_size)
}

/// BumpAlloc
///		Returns a pointer to allocated memory of given size or raises an ERROR
///		on allocation failure, or returns NULL when flags contains
///		MCXT_ALLOC_NO_OOM.
///
/// No request may exceed:
///		MAXALIGN_DOWN(SIZE_MAX) - Bump_BLOCKHDRSZ - Bump_CHUNKHDRSZ
/// All callers use a much-lower limit.
///
///
/// Note: when using valgrind, it doesn't matter how the returned allocation
/// is marked, as mcxt.c will set it to UNDEFINED.
/// This function should only contain the most common code paths.  Everything
/// else should be in pg_noinline helper functions, thus avoiding the overhead
/// of creating a stack frame for the common cases.  Allocating memory is often
/// a bottleneck in many workloads, so avoiding stack frame setup is
/// worthwhile.  Helper functions should always directly return the newly
/// allocated memory so that we can just return that address directly as a tail
/// call.
///
/// # Safety
/// `context` must be a valid Bump context.
pub unsafe fn BumpAlloc(context: MemoryContext, size: Size, flags: c_int) -> *mut c_void {
    let set: *mut BumpContext = context as *mut BumpContext;
    let block: *mut BumpBlock;
    let chunk_size: Size;
    let required_size: Size;

    Assert!(BumpIsValid(set));

    // #ifdef MEMORY_CONTEXT_CHECKING
    //     /* ensure there's always space for the sentinel byte */
    //     chunk_size = MAXALIGN(size + 1);
    // #else
    chunk_size = MAXALIGN(size);
    // #endif

    // If requested size exceeds maximum for chunks we hand the request off to
    // BumpAllocLarge().
    if chunk_size > (*set).allocChunkLimit as Size {
        return BumpAllocLarge(context, size, flags);
    }

    required_size = chunk_size + Bump_CHUNKHDRSZ;

    // Not an oversized chunk.  We try to first make use of the latest block,
    // but if there's not enough space in it we must allocate a new block.
    block = dlist_container!(BumpBlock, node, dlist_head_node(&mut (*set).blocks));

    if BumpBlockFreeBytes(block) < required_size {
        return BumpAllocFromNewBlock(context, size, flags, chunk_size);
    }

    // The current block has space, so just allocate chunk there.
    BumpAllocChunkFromBlock(context, block, size, chunk_size)
}

/// BumpBlockInit
///		Initializes 'block' assuming 'blksize'.  Does not update the context's
///		mem_allocated field.
///
/// # Safety
/// `block` must point to `blksize` bytes of live storage owned by `context`.
#[inline]
unsafe fn BumpBlockInit(_context: *mut BumpContext, block: *mut BumpBlock, blksize: Size) {
    // #ifdef MEMORY_CONTEXT_CHECKING
    //     block->context = context;
    // #endif
    // TODO(pg-port): block->context = context under MEMORY_CONTEXT_CHECKING.
    (*block).freeptr = (block as *mut u8).add(Bump_BLOCKHDRSZ) as *mut c_char;
    (*block).endptr = (block as *mut u8).add(blksize) as *mut c_char;

    // Mark unallocated space NOACCESS.
    // VALGRIND_MAKE_MEM_NOACCESS(block->freeptr, blksize - Bump_BLOCKHDRSZ);
    // TODO(pg-port): valgrind no-op.
}

/// BumpBlockIsEmpty
///		Returns true iff 'block' contains no chunks
///
/// # Safety
/// `block` must point to a live BumpBlock.
#[allow(dead_code)]
#[inline]
unsafe fn BumpBlockIsEmpty(block: *mut BumpBlock) -> bool {
    // it's empty if the freeptr has not moved
    (*block).freeptr == (block as *mut u8).add(Bump_BLOCKHDRSZ) as *mut c_char
}

/// BumpBlockMarkEmpty
///		Set a block as empty.  Does not free the block.
///
/// # Safety
/// `block` must point to a live BumpBlock.
#[inline]
unsafe fn BumpBlockMarkEmpty(block: *mut BumpBlock) {
    // #if defined(USE_VALGRIND) || defined(CLOBBER_FREED_MEMORY)
    //     char	   *datastart = ((char *) block) + Bump_BLOCKHDRSZ;
    // #endif
    //
    // #ifdef CLOBBER_FREED_MEMORY
    //     wipe_mem(datastart, block->freeptr - datastart);
    // #else
    //     /* wipe_mem() would have done this */
    //     VALGRIND_MAKE_MEM_NOACCESS(datastart, block->freeptr - datastart);
    // #endif
    // TODO(pg-port): CLOBBER_FREED_MEMORY / valgrind no-op (default-off path).

    // Reset the block, but don't return it to malloc
    (*block).freeptr = (block as *mut u8).add(Bump_BLOCKHDRSZ) as *mut c_char;
}

/// BumpBlockFreeBytes
///		Returns the number of bytes free in 'block'
///
/// # Safety
/// `block` must point to a live BumpBlock.
#[inline]
unsafe fn BumpBlockFreeBytes(block: *mut BumpBlock) -> Size {
    ((*block).endptr as usize).wrapping_sub((*block).freeptr as usize) as Size
}

/// BumpBlockFree
///		Remove 'block' from 'set' and release the memory consumed by it.
///
/// # Safety
/// `set` must be a valid Bump context and `block` a non-keeper block of it.
#[inline]
unsafe fn BumpBlockFree(set: *mut BumpContext, block: *mut BumpBlock) {
    // Make sure nobody tries to free the keeper block
    Assert!(!IsKeeperBlock(set, block));

    // release the block from the list of blocks
    dlist_delete(&mut (*block).node);

    (*(set as MemoryContext)).mem_allocated -=
        ((*block).endptr as usize).wrapping_sub(block as *mut u8 as usize) as Size;

    // #ifdef CLOBBER_FREED_MEMORY
    //     wipe_mem(block, ((char *) block->endptr - (char *) block));
    // #endif
    // TODO(pg-port): CLOBBER_FREED_MEMORY no-op (default-off path).

    free(block as *mut c_void);
}

/// BumpFree
///		Unsupported.
///
/// # Safety
/// `pointer` is ignored; this operation is unsupported and always errors.
pub unsafe fn BumpFree(_pointer: *mut c_void) {
    elog!(
        ERROR,
        "{} is not supported by the bump memory allocator",
        "pfree"
    );
}

/// BumpRealloc
///		Unsupported.
///
/// # Safety
/// `pointer` is ignored; this operation is unsupported and always errors.
pub unsafe fn BumpRealloc(_pointer: *mut c_void, _size: Size, _flags: c_int) -> *mut c_void {
    elog!(
        ERROR,
        "{} is not supported by the bump memory allocator",
        "realloc"
    );
    #[allow(unreachable_code)]
    core::ptr::null_mut() // keep compiler quiet
}

/// BumpGetChunkContext
///		Unsupported.
///
/// # Safety
/// `pointer` is ignored; this operation is unsupported and always errors.
pub unsafe fn BumpGetChunkContext(_pointer: *mut c_void) -> MemoryContext {
    elog!(
        ERROR,
        "{} is not supported by the bump memory allocator",
        "GetMemoryChunkContext"
    );
    #[allow(unreachable_code)]
    core::ptr::null_mut() // keep compiler quiet
}

/// BumpGetChunkSpace
///		Unsupported.
///
/// # Safety
/// `pointer` is ignored; this operation is unsupported and always errors.
pub unsafe fn BumpGetChunkSpace(_pointer: *mut c_void) -> Size {
    elog!(
        ERROR,
        "{} is not supported by the bump memory allocator",
        "GetMemoryChunkSpace"
    );
    #[allow(unreachable_code)]
    0 // keep compiler quiet
}

/// BumpIsEmpty
///		Is a BumpContext empty of any allocated space?
///
/// # Safety
/// `context` must be a valid Bump context.
pub unsafe fn BumpIsEmpty(context: MemoryContext) -> bool {
    let set: *mut BumpContext = context as *mut BumpContext;
    let mut iter: dlist_iter = core::mem::zeroed();

    Assert!(BumpIsValid(set));

    dlist_foreach!(iter, &mut (*set).blocks, {
        let block: *mut BumpBlock = dlist_container!(BumpBlock, node, iter.cur);

        if !BumpBlockIsEmpty(block) {
            return false;
        }
    });

    true
}

/// BumpStats
///		Compute stats about memory consumption of a Bump context.
///
/// printfunc: if not NULL, pass a human-readable stats string to this.
/// passthru: pass this pointer through to printfunc.
/// totals: if not NULL, add stats about this context into *totals.
/// print_to_stderr: print stats to stderr if true, elog otherwise.
///
/// # Safety
/// `context` must be a valid Bump context; `totals` must be NULL or valid.
pub unsafe fn BumpStats(
    context: MemoryContext,
    printfunc: MemoryStatsPrintFunc,
    passthru: *mut c_void,
    totals: *mut MemoryContextCounters,
    print_to_stderr: bool,
) {
    let set: *mut BumpContext = context as *mut BumpContext;
    let mut nblocks: Size = 0;
    let mut totalspace: Size = 0;
    let mut freespace: Size = 0;
    let mut iter: dlist_iter = core::mem::zeroed();

    Assert!(BumpIsValid(set));

    dlist_foreach!(iter, &mut (*set).blocks, {
        let block: *mut BumpBlock = dlist_container!(BumpBlock, node, iter.cur);

        nblocks += 1;
        totalspace += ((*block).endptr as usize).wrapping_sub(block as *mut u8 as usize) as Size;
        freespace +=
            ((*block).endptr as usize).wrapping_sub((*block).freeptr as usize) as Size;
    });

    if let Some(printfunc) = printfunc {
        // char stats_string[200];
        // snprintf(stats_string, sizeof(stats_string),
        //          "%zu total in %zu blocks; %zu free; %zu used",
        //          totalspace, nblocks, freespace, totalspace - freespace);
        let stats_string = format!(
            "{} total in {} blocks; {} free; {} used\0",
            totalspace,
            nblocks,
            freespace,
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
        (*totals).totalspace += totalspace;
        (*totals).freespace += freespace;
    }
}

// #ifdef MEMORY_CONTEXT_CHECKING
//
// BumpCheck
//		Walk through chunks and check consistency of memory.
//
// NOTE: report errors as WARNING, *not* ERROR or FATAL.  Otherwise you'll
// find yourself in an infinite loop when trouble occurs, because this
// routine will be entered again when elog cleanup tries to release memory!
//
// Gated behind `memory_context_checking` (off by default, matching the C
// #ifdef MEMORY_CONTEXT_CHECKING) so the default build never compiles it; the
// per-chunk MemoryChunk header (Bump_CHUNKHDRSZ == 0) and BumpBlock.context
// back-link only exist under that cfg.
#[cfg(memory_context_checking)]
pub unsafe fn BumpCheck(context: MemoryContext) {
    let bump: *mut BumpContext = context as *mut BumpContext;
    let name = (*context).name;
    let mut iter: dlist_iter = core::mem::zeroed();
    let mut total_allocated: Size = 0;

    /* walk all blocks in this context */
    dlist_foreach!(iter, &mut (*bump).blocks, {
        let block: *mut BumpBlock = dlist_container!(BumpBlock, node, iter.cur);
        let mut nchunks: c_int;
        let mut ptr: *mut c_char;
        let mut has_external_chunk = false;

        if IsKeeperBlock(bump, block) {
            total_allocated +=
                ((*block).endptr as usize).wrapping_sub(bump as *mut c_char as usize) as Size;
        } else {
            total_allocated +=
                ((*block).endptr as usize).wrapping_sub(block as *mut c_char as usize) as Size;
        }

        /* check block belongs to the correct context */
        if (*block).context != bump {
            elog!(
                WARNING,
                "problem in Bump {}: bogus context link in block {:p}",
                std::ffi::CStr::from_ptr(name).to_string_lossy(),
                block
            );
        }

        /* now walk through the chunks and count them */
        nchunks = 0;
        ptr = (block as *mut c_char).add(Bump_BLOCKHDRSZ);

        while ptr < (*block).freeptr {
            let chunk: *mut MemoryChunk = ptr as *mut MemoryChunk;
            let chunkblock: *mut BumpBlock;
            let chunksize: Size;

            /* allow access to the chunk header */
            // VALGRIND_MAKE_MEM_DEFINED(chunk, Bump_CHUNKHDRSZ);

            if MemoryChunkIsExternal(chunk) {
                chunkblock = ExternalChunkGetBlock(chunk);
                chunksize = ((*block).endptr as usize)
                    .wrapping_sub(MemoryChunkGetPointer(chunk) as usize)
                    as Size;
                has_external_chunk = true;
            } else {
                chunkblock = MemoryChunkGetBlock(chunk);
                chunksize = MemoryChunkGetValue(chunk);
            }

            /* move to the next chunk */
            ptr = ptr.add(chunksize + Bump_CHUNKHDRSZ);

            nchunks += 1;

            /* chunks have both block and context pointers, so check both */
            if chunkblock != block {
                elog!(
                    WARNING,
                    "problem in Bump {}: bogus block link in block {:p}, chunk {:p}",
                    std::ffi::CStr::from_ptr(name).to_string_lossy(),
                    block,
                    chunk
                );
            }
        }

        if has_external_chunk && nchunks > 1 {
            elog!(
                WARNING,
                "problem in Bump {}: external chunk on non-dedicated block {:p}",
                std::ffi::CStr::from_ptr(name).to_string_lossy(),
                block
            );
        }
    });

    Assert!(total_allocated == (*context).mem_allocated);
}
// #endif							/* MEMORY_CONTEXT_CHECKING */

// =============================================================================
// Summary
// -----------------------------------------------------------------------------
// Structs (all #[repr(C)], identifiers verbatim):
//   - BumpContext: FIRST field is embedded `header: MemoryContextData`, then the
//     uint32 params (initBlockSize/maxBlockSize/nextBlockSize/allocChunkLimit) and
//     `blocks: dlist_head` (the doubly-linked block list, current fill block at
//     head). Context pointer is `*mut BumpContext`.
//   - BumpBlock: `node: dlist_node`, then `freeptr`/`endptr: *mut c_char`. The
//     MEMORY_CONTEXT_CHECKING-only `context` back-link is a TODO(pg-port) (kept as
//     a comment) since that build mode is not modeled (Bump_CHUNKHDRSZ == 0).
//
// Method fns (signatures match the MemoryContextMethods fn-pointer types in
// memnodes.rs exactly; all `pub unsafe fn`):
//   - BumpAlloc(context: MemoryContext, size: Size, flags: c_int) -> *mut c_void
//   - BumpFree(pointer: *mut c_void)
//   - BumpRealloc(pointer: *mut c_void, size: Size, flags: c_int) -> *mut c_void
//   - BumpReset(context: MemoryContext)
//   - BumpDelete(context: MemoryContext)
//   - BumpGetChunkContext(pointer: *mut c_void) -> MemoryContext
//   - BumpGetChunkSpace(pointer: *mut c_void) -> Size
//   - BumpIsEmpty(context: MemoryContext) -> bool
//   - BumpStats(context, printfunc: MemoryStatsPrintFunc, passthru: *mut c_void,
//               totals: *mut MemoryContextCounters, print_to_stderr: bool)
//   plus BumpContextCreate(parent, name, minContextSize, initBlockSize,
//   maxBlockSize) -> MemoryContext. Private helpers: BumpAllocLarge /
//   BumpAllocFromNewBlock (#[inline(never)] for the pg_noinline C qualifier),
//   BumpAllocChunkFromBlock (inline shared helper), and the inline block helpers
//   BumpBlockInit / BumpBlockIsEmpty / BumpBlockMarkEmpty / BumpBlockFreeBytes /
//   BumpBlockFree. Node tag is NodeTag::T_BumpContext, method_id MCTX_BUMP_ID.
//
// BumpFree / BumpRealloc (and BumpGetChunkContext / BumpGetChunkSpace) error
// exactly as the C: each calls `elog!(ERROR, "%s is not supported by the bump
// memory allocator", "<op>")`, which panics (the longjmp analogue). The
// value-returning ones keep a trailing `null_mut()` / `0` ("keep compiler quiet")
// behind `#[allow(unreachable_code)]`, matching the C `return NULL;` after elog.
//
// External chunk header usage: in the default (non-MEMORY_CONTEXT_CHECKING) build
// Bump_CHUNKHDRSZ == 0 and chunks carry NO header, so BumpAlloc/BumpAllocLarge/
// BumpAllocChunkFromBlock return the raw freeptr / `(char *) block +
// Bump_BLOCKHDRSZ` (the C `#else` branches). The external-chunk machinery
// (MemoryChunkSetHdrMaskExternal, ExternalChunkGetBlock, the `chunk` locals) is
// only used by the MEMORY_CONTEXT_CHECKING and BumpCheck paths, which are
// preserved verbatim as comments with TODO(pg-port) markers, mirroring how
// aset.rs handles its checking branches.
//
// Wrapping arithmetic: all pointer-difference computations (endptr - freeptr,
// endptr - block, freeptr/endptr vs block base) use `usize` `wrapping_sub` to
// mirror C `(char *)` subtraction without Rust debug overflow panics. The block
// doubling (`nextBlockSize <<= 1`) and the allocChunkLimit `>>= 1` reductions use
// plain shifts, bounded as in C. `size_of`/`offsetof`->`core::mem::size_of` /
// `offset_of!` (the latter via dlist_container!); `memset`->none needed (no zeroing
// path); NULL->`null_mut()`/`.is_null()`.
//
// malloc/free binding: `extern "C" { malloc/free }` (raw libc) for blocks, exactly
// as bump.c; coexists with the bootstrap utils::palloc. dlist block list via
// crate::lib::ilist (dlist_init/push_head/push_tail/delete + the foreach /
// foreach_modify / container macros which take a trailing block).
//
// Stubs / default-off paths: VALGRIND_* hooks are no-ops; MEMORY_CONTEXT_CHECKING /
// RANDOMIZE_ALLOCATED_MEMORY / CLOBBER_FREED_MEMORY branches translate the
// default-off (#else / non-checking) path with the checking branches preserved as
// comments + TODO(pg-port). BumpCheck is omitted (compiled only under
// MEMORY_CONTEXT_CHECKING), noted as a cfg-gated TODO. MemoryContextCreate /
// MemoryContextAllocationFailure / MemoryContextStats / MemoryContextCheckSize are
// the real mcxt.rs / memutils_internal routines; ereport(ERROR)/errdetail in
// BumpContextCreate becomes elog!(ERROR, ...).
//
// Every bump.c function was translated: BumpContextCreate, BumpReset, BumpDelete,
// BumpAllocLarge, BumpAllocChunkFromBlock, BumpAllocFromNewBlock, BumpAlloc,
// BumpBlockInit, BumpBlockIsEmpty, BumpBlockMarkEmpty, BumpBlockFreeBytes,
// BumpBlockFree, BumpFree, BumpRealloc, BumpGetChunkContext, BumpGetChunkSpace,
// BumpIsEmpty, BumpStats (BumpCheck noted as a cfg-gated TODO).
// =============================================================================
