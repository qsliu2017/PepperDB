//! Translation of postgres/src/backend/utils/mmgr/slab.c
//!
//! SLAB allocator definitions.
//!
//! SLAB is a MemoryContext implementation designed for cases where large
//! numbers of equally-sized objects can be allocated and freed efficiently
//! with minimal memory wastage and fragmentation.
//!
//! Portions Copyright (c) 2017-2025, PostgreSQL Global Development Group
//!
//! IDENTIFICATION
//!	  src/backend/utils/mmgr/slab.c
//!
//! NOTE:
//!	The constant allocation size allows significant simplification and various
//!	optimizations over more general purpose allocators. The blocks are carved
//!	into chunks of exactly the right size, wasting only the space required to
//!	MAXALIGN the allocated chunks.
//!
//!	Slab can also help reduce memory fragmentation in cases where longer-lived
//!	chunks remain stored on blocks while most of the other chunks have already
//!	been pfree'd.  We give priority to putting new allocations into the
//!	"fullest" block.  This help avoid having too many sparsely used blocks
//!	around and allows blocks to more easily become completely unused which
//!	allows them to be eventually free'd.
//!
//!	We identify the "fullest" block to put new allocations on by using a block
//!	from the lowest populated element of the context's "blocklist" array.
//!	This is an array of dlists containing blocks which we partition by the
//!	number of free chunks which block has.  Blocks with fewer free chunks are
//!	stored in a lower indexed dlist array slot.  Full blocks go on the 0th
//!	element of the blocklist array.  So that we don't have to have too many
//!	elements in the array, each dlist in the array is responsible for a range
//!	of free chunks.  When a chunk is palloc'd or pfree'd we may need to move
//!	the block onto another dlist if the number of free chunks crosses the
//!	range boundary that the current list is responsible for.  Having just a
//!	few blocklist elements reduces the number of times we must move the block
//!	onto another dlist element.
//!
//!	We keep track of free chunks within each block by using a block-level free
//!	list.  We consult this list when we allocate a new chunk in the block.
//!	The free list is a linked list, the head of which is pointed to with
//!	SlabBlock's freehead field.  Each subsequent list item is stored in the
//!	free chunk's memory.  We ensure chunks are large enough to store this
//!	address.
//!
//!	When we allocate a new block, technically all chunks are free, however, to
//!	avoid having to write out the entire block to set the linked list for the
//!	free chunks for every chunk in the block, we instead store a pointer to
//!	the next "unused" chunk on the block and keep track of how many of these
//!	unused chunks there are.  When a new block is malloc'd, all chunks are
//!	unused.  The unused pointer starts with the first chunk on the block and
//!	as chunks are allocated, the unused pointer is incremented.  As chunks are
//!	pfree'd, the unused pointer never goes backwards.  The unused pointer can
//!	be thought of as a high watermark for the maximum number of chunks in the
//!	block which have been in use concurrently.  When a chunk is pfree'd the
//!	chunk is put onto the head of the free list and the unused pointer is not
//!	changed.  We only consume more unused chunks if we run out of free chunks
//!	on the free list.  This method effectively gives priority to using
//!	previously used chunks over previously unused chunks, which should perform
//!	better due to CPU caching effects.
//!
//! NOTE (port staging): this is the REAL Slab allocator, translated additively
//! under `utils/mmgr` while the rest of the crate still uses the context-less
//! bootstrap allocator in `utils::palloc`. Imports are explicit (NOT
//! `crate::prelude::*`) to avoid clashing with the bootstrap
//! `MemoryContext`/`MemoryContextData` symbols. This mirrors the verified
//! sibling allocator in `aset.rs`.

// c.h: MAXALIGN, Size, uint32/int32, unlikely.
use crate::c::{int32, uint32, unlikely, Size, MAXALIGN};
// pg_config.h: MAXIMUM_ALIGNOF (referenced indirectly by MAXALIGN; kept for clarity).
#[allow(unused_imports)]
use crate::pg_config::MAXIMUM_ALIGNOF;
// nodes/nodes.h: T_SlabContext (NodeTag) and nodeTag() (used via IsA!).
use crate::nodes::nodes::{nodeTag, NodeTag};
use crate::IsA; // nodes.h IsA! macro (used by SlabIsValid)
// utils/palloc.h: allocation flag bits.
#[allow(unused_imports)]
use crate::utils::palloc::{MCXT_ALLOC_HUGE, MCXT_ALLOC_NO_OOM, MCXT_ALLOC_ZERO};
// nodes/memnodes.h: the abstract context node + its method-table types.
use crate::utils::mmgr::memnodes::{
    MemoryContext, MemoryContextCounters, MemoryContextData, MemoryStatsPrintFunc,
};
// utils/memutils_memorychunk.h: the MemoryChunk header + encode/decode helpers.
use crate::utils::mmgr::memutils_memorychunk::{
    MemoryChunk, MemoryChunkGetBlock, MemoryChunkGetPointer, MemoryChunkSetHdrMask,
    PointerGetMemoryChunk, MEMORYCHUNK_MAX_BLOCKOFFSET, MEMORYCHUNK_MAX_VALUE,
};
// utils/memutils_internal.h: method ID enum + the Slab method ID.
use crate::utils::mmgr::memutils_internal::{MemoryContextMethodID, MCTX_SLAB_ID};
// The real MemoryContextCreate / allocation-failure / stats helpers live in
// mcxt.rs (their C bodies are in mcxt.c); use those rather than
// memutils_internal's stubs.
use crate::utils::mmgr::mcxt::{
    MemoryContextAllocationFailure, MemoryContextCreate, MemoryContextStats,
};
// lib/ilist.h: slab uses dlist (per-fill-fraction block lists) and dclist (the
// retained-empty-blocks list). Bring in the node/head/iter types and the inline
// fns; the foreach/container macros are #[macro_export]ed crate-wide.
use crate::lib::ilist::{
    dclist_count, dclist_head, dclist_init, dclist_pop_head_node, dclist_push_head, dlist_delete,
    dlist_delete_from, dlist_head, dlist_init, dlist_is_empty, dlist_iter, dlist_mutable_iter,
    dlist_node, dlist_push_head,
};
use core::ffi::{c_char, c_int, c_void};
// #[macro_export] macros live at the crate root and must be imported by name.
use crate::{
    dclist_container, dclist_foreach, dclist_foreach_modify, dlist_container, dlist_foreach,
    dlist_foreach_modify, dlist_head_element, elog,
};

// slab.c uses raw malloc/free for its blocks and context header.
extern "C" {
    fn malloc(size: usize) -> *mut c_void;
    fn free(p: *mut c_void);
}

/// `Slab_BLOCKHDRSZ` = `MAXALIGN(sizeof(SlabBlock))`
const Slab_BLOCKHDRSZ: Size = MAXALIGN(core::mem::size_of::<SlabBlock>());

// #ifdef MEMORY_CONTEXT_CHECKING
// /*
//  * Size of the memory required to store the SlabContext.
//  * MEMORY_CONTEXT_CHECKING builds need some extra memory for the isChunkFree
//  * array.
//  */
// #define Slab_CONTEXT_HDRSZ(chunksPerBlock)	\
//     (sizeof(SlabContext) + ((chunksPerBlock) * sizeof(bool)))
// #else
// #define Slab_CONTEXT_HDRSZ(chunksPerBlock)	sizeof(SlabContext)
// #endif
//
// Default build (no MEMORY_CONTEXT_CHECKING): the isChunkFree array is not
// compiled in, so the context header is just sizeof(SlabContext).
// TODO(pg-port): add the `+ chunksPerBlock * sizeof(bool)` term under a cfg when
// MEMORY_CONTEXT_CHECKING is modeled.
#[inline]
fn Slab_CONTEXT_HDRSZ(_chunksPerBlock: c_int) -> Size {
    core::mem::size_of::<SlabContext>()
}

/// The number of partitions to divide the blocklist into based their number of
/// free chunks.  There must be at least 2.
const SLAB_BLOCKLIST_COUNT: usize = 3;

/// The maximum number of completely empty blocks to keep around for reuse.
const SLAB_MAXIMUM_EMPTY_BLOCKS: uint32 = 10;

/// SlabContext is a specialized implementation of MemoryContext.
#[repr(C)]
pub struct SlabContext {
    /// Standard memory-context fields
    pub header: MemoryContextData,
    // Allocation parameters for this context:
    /// the requested (non-aligned) chunk size
    chunkSize: uint32,
    /// chunk size with chunk header and alignment
    fullChunkSize: uint32,
    /// the size to make each block of chunks
    blockSize: uint32,
    /// number of chunks that fit in 1 block
    chunksPerBlock: int32,
    /// index into the blocklist[] element containing the fullest, blocks
    curBlocklistIndex: int32,
    // #ifdef MEMORY_CONTEXT_CHECKING
    //     bool	   *isChunkFree;	/* array to mark free chunks in a block during
    //                                  * SlabCheck */
    // #endif
    /// array to mark free chunks in a block during SlabCheck
    #[cfg(feature = "memory_context_checking")]
    isChunkFree: *mut bool,
    /// number of bits to shift the nfree count by to get the index into blocklist[]
    blocklist_shift: int32,
    /// empty blocks to use up first instead of mallocing new blocks
    emptyblocks: dclist_head,
    /// Blocks with free space, grouped by the number of free chunks they
    /// contain.  Completely full blocks are stored in the 0th element.
    /// Completely empty blocks are stored in emptyblocks or free'd if we have
    /// enough empty blocks already.
    blocklist: [dlist_head; SLAB_BLOCKLIST_COUNT],
}

/// SlabBlock
///		Structure of a single slab block.
///
/// slab: pointer back to the owning MemoryContext
/// nfree: number of chunks on the block which are unallocated
/// nunused: number of chunks on the block unallocated and not on the block's
/// freelist.
/// freehead: linked-list header storing a pointer to the first free chunk on
/// the block.  Subsequent pointers are stored in the chunk's memory.  NULL
/// indicates the end of the list.
/// unused: pointer to the next chunk which has yet to be used.
/// node: doubly-linked list node for the context's blocklist
#[repr(C)]
struct SlabBlock {
    /// owning context
    slab: *mut SlabContext,
    /// number of chunks on free + unused chunks
    nfree: int32,
    /// number of unused chunks
    nunused: int32,
    /// pointer to the first free chunk
    freehead: *mut MemoryChunk,
    /// pointer to the next unused chunk
    unused: *mut MemoryChunk,
    /// doubly-linked list for blocklist[]
    node: dlist_node,
}

/// `Slab_CHUNKHDRSZ` = `sizeof(MemoryChunk)`
const Slab_CHUNKHDRSZ: Size = core::mem::size_of::<MemoryChunk>();

/// `SlabChunkGetPointer(chk) = (void *) (((char *) chk) + sizeof(MemoryChunk))`
///
/// # Safety
/// `chk` must point to a live MemoryChunk immediately followed by its payload.
#[inline]
unsafe fn SlabChunkGetPointer(chk: *mut MemoryChunk) -> *mut c_void {
    (chk as *mut c_char).add(core::mem::size_of::<MemoryChunk>()) as *mut c_void
}

/// SlabBlockGetChunk
///		Obtain a pointer to the nth (0-based) chunk in the block
///
/// `SlabBlockGetChunk(slab, block, n) =
///     (MemoryChunk *) ((char *) block + Slab_BLOCKHDRSZ + n * slab->fullChunkSize)`
///
/// # Safety
/// `slab` and `block` must be live; `n` must be a valid chunk index in `block`.
#[inline]
unsafe fn SlabBlockGetChunk(slab: *mut SlabContext, block: *mut SlabBlock, n: int32) -> *mut MemoryChunk {
    // C pointer arithmetic wraps; use wrapping math on the byte offset.
    (block as *mut c_char).add(
        Slab_BLOCKHDRSZ.wrapping_add((n as Size).wrapping_mul((*slab).fullChunkSize as Size)),
    ) as *mut MemoryChunk
}

// #if defined(MEMORY_CONTEXT_CHECKING) || defined(USE_ASSERT_CHECKING)
//
// SlabChunkIndex
//		Get the 0-based index of how many chunks into the block the given
//		chunk is.
// #define SlabChunkIndex(slab, block, chunk)	\
//     (((char *) chunk - (char *) SlabBlockGetChunk(slab, block, 0)) / slab->fullChunkSize)
//
// SlabChunkMod
//		A MemoryChunk should always be at an address which is a multiple of
//		fullChunkSize starting from the 0th chunk position.  This will return
//		non-zero if it's not.
// #define SlabChunkMod(slab, block, chunk)	\
//     (((char *) chunk - (char *) SlabBlockGetChunk(slab, block, 0)) % slab->fullChunkSize)
//
// These two helpers are only used inside Assert()s in the default
// (USE_ASSERT_CHECKING) build and inside SlabCheck (MEMORY_CONTEXT_CHECKING).
// Provided here so the Assert!()s below can reference them; under USE_ASSERT
// they would be live, but our Assert! is a debug-only check.
// TODO(pg-port): gate on USE_ASSERT_CHECKING / MEMORY_CONTEXT_CHECKING.

/// `SlabChunkIndex(slab, block, chunk)` - 0-based index of how many chunks into
/// the block the given chunk is.
///
/// # Safety
/// `slab`/`block`/`chunk` must all be live and belong together.
#[cfg(feature = "memory_context_checking")]
#[inline]
unsafe fn SlabChunkIndex(slab: *mut SlabContext, block: *mut SlabBlock, chunk: *mut MemoryChunk) -> c_int {
    // C pointer subtraction; mirror with usize wrapping_sub.
    (((chunk as *mut c_char as usize)
        .wrapping_sub(SlabBlockGetChunk(slab, block, 0) as *mut c_char as usize))
        / ((*slab).fullChunkSize as usize)) as c_int
}

/// `SlabChunkMod(slab, block, chunk)` - non-zero iff `chunk` is not aligned to a
/// fullChunkSize boundary from the 0th chunk.
///
/// # Safety
/// `slab`/`block`/`chunk` must all be live and belong together.
#[allow(dead_code)]
#[inline]
unsafe fn SlabChunkMod(slab: *mut SlabContext, block: *mut SlabBlock, chunk: *mut MemoryChunk) -> Size {
    // C pointer subtraction; mirror with usize wrapping_sub.
    ((chunk as *mut c_char as usize)
        .wrapping_sub(SlabBlockGetChunk(slab, block, 0) as *mut c_char as usize))
        % ((*slab).fullChunkSize as Size)
}

/// SlabIsValid
///		True iff set is a valid slab allocation set.
///
/// `SlabIsValid(set) = (PointerIsValid(set) && IsA(set, SlabContext))`
///
/// # Safety
/// `set` must be NULL or point to a node beginning with a NodeTag.
#[inline]
unsafe fn SlabIsValid(set: *mut SlabContext) -> bool {
    !set.is_null() && IsA!(set, T_SlabContext)
}

/// SlabBlockIsValid
///		True iff block is a valid block of slab allocation set.
///
/// `SlabBlockIsValid(block) = (PointerIsValid(block) && SlabIsValid((block)->slab))`
///
/// # Safety
/// `block` must be NULL or point to a live SlabBlock.
#[inline]
unsafe fn SlabBlockIsValid(block: *mut SlabBlock) -> bool {
    !block.is_null() && SlabIsValid((*block).slab)
}

/// SlabBlocklistIndex
///		Determine the blocklist index that a block should be in for the given
///		number of free chunks.
#[inline]
unsafe fn SlabBlocklistIndex(slab: *mut SlabContext, nfree: c_int) -> int32 {
    let index: int32;
    let blocklist_shift: int32 = (*slab).blocklist_shift;

    Assert!(nfree >= 0 && nfree <= (*slab).chunksPerBlock);

    // Determine the blocklist index based on the number of free chunks.  We
    // must ensure that 0 free chunks is dedicated to index 0.  Everything
    // else must be >= 1 and < SLAB_BLOCKLIST_COUNT.
    //
    // To make this as efficient as possible, we exploit some two's complement
    // arithmetic where we reverse the sign before bit shifting.  This results
    // in an nfree of 0 using index 0 and anything non-zero staying non-zero.
    // This is exploiting 0 and -0 being the same in two's complement.  When
    // we're done, we just need to flip the sign back over again for a
    // positive index.
    //
    // C: index = -((-nfree) >> blocklist_shift);
    // C arithmetic wraps and the right-shift on a signed value is arithmetic;
    // use wrapping_neg / wrapping_shr to faithfully match C semantics.
    index = (((nfree as int32).wrapping_neg()) >> (blocklist_shift as u32)).wrapping_neg();

    if nfree == 0 {
        Assert!(index == 0);
    } else {
        Assert!(index >= 1 && (index as usize) < SLAB_BLOCKLIST_COUNT);
    }

    index
}

/// SlabFindNextBlockListIndex
///		Search blocklist for blocks which have free chunks and return the
///		index of the blocklist found containing at least 1 block with free
///		chunks.  If no block can be found we return 0.
///
/// Note: We give priority to fuller blocks so that these are filled before
/// emptier blocks.  This is done to increase the chances that mostly-empty
/// blocks will eventually become completely empty so they can be free'd.
///
/// # Safety
/// `slab` must be a live SlabContext.
unsafe fn SlabFindNextBlockListIndex(slab: *mut SlabContext) -> int32 {
    // start at 1 as blocklist[0] is for full blocks.
    for i in 1..SLAB_BLOCKLIST_COUNT {
        // return the first found non-empty index
        if !dlist_is_empty(&(*slab).blocklist[i]) {
            return i as int32;
        }
    }

    // no blocks with free space
    0
}

/// SlabGetNextFreeChunk
///		Return the next free chunk in block and update the block to account
///		for the returned chunk now being used.
///
/// # Safety
/// `slab`/`block` must be live and `block` must have at least one free chunk.
#[inline]
unsafe fn SlabGetNextFreeChunk(slab: *mut SlabContext, block: *mut SlabBlock) -> *mut MemoryChunk {
    let chunk: *mut MemoryChunk;

    Assert!((*block).nfree > 0);

    if !(*block).freehead.is_null() {
        chunk = (*block).freehead;

        // Pop the chunk from the linked list of free chunks.  The pointer to
        // the next free chunk is stored in the chunk itself.
        // VALGRIND_MAKE_MEM_DEFINED(SlabChunkGetPointer(chunk), sizeof(MemoryChunk *));
        // TODO(pg-port): valgrind no-op.
        (*block).freehead = *(SlabChunkGetPointer(chunk) as *mut *mut MemoryChunk);

        // check nothing stomped on the free chunk's memory
        Assert!(
            (*block).freehead.is_null()
                || ((*block).freehead >= SlabBlockGetChunk(slab, block, 0)
                    && (*block).freehead
                        <= SlabBlockGetChunk(slab, block, (*slab).chunksPerBlock - 1)
                    && SlabChunkMod(slab, block, (*block).freehead) == 0)
        );
    } else {
        Assert!((*block).nunused > 0);

        chunk = (*block).unused;
        // C: block->unused = (MemoryChunk *) (((char *) block->unused) + slab->fullChunkSize);
        (*block).unused =
            ((*block).unused as *mut c_char).add((*slab).fullChunkSize as usize) as *mut MemoryChunk;
        (*block).nunused -= 1;
    }

    (*block).nfree -= 1;

    chunk
}

/// SlabContextCreate
///		Create a new Slab context.
///
/// parent: parent context, or NULL if top-level context
/// name: name of context (must be statically allocated)
/// blockSize: allocation block size
/// chunkSize: allocation chunk size
///
/// The Slab_CHUNKHDRSZ + MAXALIGN(chunkSize + 1) may not exceed
/// MEMORYCHUNK_MAX_VALUE.
/// 'blockSize' may not exceed MEMORYCHUNK_MAX_BLOCKOFFSET.
///
/// # Safety
/// `parent` must be NULL or a valid MemoryContext; `name` must be a valid,
/// statically allocated C string.
pub unsafe fn SlabContextCreate(
    parent: MemoryContext,
    name: *const c_char,
    blockSize: Size,
    mut chunkSize: Size,
) -> MemoryContext {
    let chunksPerBlock: c_int;
    let fullChunkSize: Size;
    let slab: *mut SlabContext;
    let mut i: c_int;

    // ensure MemoryChunk's size is properly maxaligned
    // StaticAssertDecl(Slab_CHUNKHDRSZ == MAXALIGN(Slab_CHUNKHDRSZ),
    //                  "sizeof(MemoryChunk) is not maxaligned");
    const _: () = assert!(Slab_CHUNKHDRSZ == MAXALIGN(Slab_CHUNKHDRSZ));
    Assert!(blockSize as u64 <= MEMORYCHUNK_MAX_BLOCKOFFSET);

    // Ensure there's enough space to store the pointer to the next free chunk
    // in the memory of the (otherwise) unused allocation.
    if chunkSize < core::mem::size_of::<*mut MemoryChunk>() {
        chunkSize = core::mem::size_of::<*mut MemoryChunk>();
    }

    // length of the maxaligned chunk including the chunk header
    // #ifdef MEMORY_CONTEXT_CHECKING
    //     /* ensure there's always space for the sentinel byte */
    //     fullChunkSize = Slab_CHUNKHDRSZ + MAXALIGN(chunkSize + 1);
    // #else
    fullChunkSize = Slab_CHUNKHDRSZ + MAXALIGN(chunkSize);
    // #endif
    // TODO(pg-port): MEMORY_CONTEXT_CHECKING sentinel byte (default-off path).

    Assert!(fullChunkSize as u64 <= MEMORYCHUNK_MAX_VALUE);

    // compute the number of chunks that will fit on each block
    // C: chunksPerBlock = (blockSize - Slab_BLOCKHDRSZ) / fullChunkSize;
    chunksPerBlock = (blockSize.wrapping_sub(Slab_BLOCKHDRSZ) / fullChunkSize) as c_int;

    // Make sure the block can store at least one chunk.
    if chunksPerBlock == 0 {
        // elog(ERROR, "block size %zu for slab is too small for %zu-byte chunks",
        //      blockSize, chunkSize);
        panic!(
            "block size {} for slab is too small for {}-byte chunks",
            blockSize, chunkSize
        );
    }

    slab = malloc(Slab_CONTEXT_HDRSZ(chunksPerBlock)) as *mut SlabContext;
    if slab.is_null() {
        // MemoryContextStats(TopMemoryContext);
        if !crate::utils::mmgr::mcxt::TopMemoryContext.is_null() {
            MemoryContextStats(crate::utils::mmgr::mcxt::TopMemoryContext);
        }
        // ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY),
        //                 errmsg("out of memory"),
        //                 errdetail("Failed while creating memory context \"%s\".", name)));
        // TODO(pg-port): real ereport; for now this is a hard failure.
        panic!("out of memory: failed while creating memory context");
    }

    // Avoid writing code that can fail between here and MemoryContextCreate;
    // we'd leak the header if we ereport in this stretch.

    // Fill in SlabContext-specific header fields
    (*slab).chunkSize = chunkSize as uint32;
    (*slab).fullChunkSize = fullChunkSize as uint32;
    (*slab).blockSize = blockSize as uint32;
    (*slab).chunksPerBlock = chunksPerBlock;
    (*slab).curBlocklistIndex = 0;

    // Compute a shift that guarantees that shifting chunksPerBlock with it is
    // < SLAB_BLOCKLIST_COUNT - 1.  The reason that we subtract 1 from
    // SLAB_BLOCKLIST_COUNT in this calculation is that we reserve the 0th
    // blocklist element for blocks which have no free chunks.
    //
    // We calculate the number of bits to shift by rather than a divisor to
    // divide by as performing division each time we need to find the
    // blocklist index would be much slower.
    (*slab).blocklist_shift = 0;
    while ((*slab).chunksPerBlock >> ((*slab).blocklist_shift as u32))
        >= (SLAB_BLOCKLIST_COUNT as int32 - 1)
    {
        (*slab).blocklist_shift += 1;
    }

    // initialize the list to store empty blocks to be reused
    dclist_init(&mut (*slab).emptyblocks);

    // initialize each blocklist slot
    i = 0;
    while i < SLAB_BLOCKLIST_COUNT as c_int {
        dlist_init(&mut (*slab).blocklist[i as usize]);
        i += 1;
    }

    // #ifdef MEMORY_CONTEXT_CHECKING
    //     /* set the isChunkFree pointer right after the end of the context */
    //     slab->isChunkFree = (bool *) ((char *) slab + sizeof(SlabContext));
    // #endif
    // TODO(pg-port): isChunkFree under MEMORY_CONTEXT_CHECKING (default-off).

    // Finally, do the type-independent part of context creation
    MemoryContextCreate(
        slab as MemoryContext,
        NodeTag::T_SlabContext,
        MCTX_SLAB_ID,
        parent,
        name,
    );

    slab as MemoryContext
}

/// SlabReset
///		Frees all memory which is allocated in the given set.
///
/// The code simply frees all the blocks in the context - we don't keep any
/// keeper blocks or anything like that.
///
/// # Safety
/// `context` must be a valid Slab context.
pub unsafe fn SlabReset(context: MemoryContext) {
    let slab: *mut SlabContext = context as *mut SlabContext;
    let mut miter: dlist_mutable_iter = core::mem::zeroed();
    let mut i: c_int;

    Assert!(SlabIsValid(slab));

    // #ifdef MEMORY_CONTEXT_CHECKING
    //     /* Check for corruption and leaks before freeing */
    //     SlabCheck(context);
    // #endif
    // TODO(pg-port): SlabCheck under MEMORY_CONTEXT_CHECKING.

    // release any retained empty blocks
    dclist_foreach_modify!(miter, &mut (*slab).emptyblocks, {
        let block: *mut SlabBlock = dclist_container!(SlabBlock, node, miter.cur);

        crate::lib::ilist::dclist_delete_from(&mut (*slab).emptyblocks, miter.cur);

        // #ifdef CLOBBER_FREED_MEMORY
        //     wipe_mem(block, slab->blockSize);
        // #endif
        // TODO(pg-port): CLOBBER_FREED_MEMORY no-op (default-off path).
        free(block as *mut c_void);
        (*context).mem_allocated -= (*slab).blockSize as Size;
    });

    // walk over blocklist and free the blocks
    i = 0;
    while i < SLAB_BLOCKLIST_COUNT as c_int {
        dlist_foreach_modify!(miter, &mut (*slab).blocklist[i as usize], {
            let block: *mut SlabBlock = dlist_container!(SlabBlock, node, miter.cur);

            dlist_delete(miter.cur);

            // #ifdef CLOBBER_FREED_MEMORY
            //     wipe_mem(block, slab->blockSize);
            // #endif
            // TODO(pg-port): CLOBBER_FREED_MEMORY no-op (default-off path).
            free(block as *mut c_void);
            (*context).mem_allocated -= (*slab).blockSize as Size;
        });
        i += 1;
    }

    (*slab).curBlocklistIndex = 0;

    Assert!((*context).mem_allocated == 0);
}

/// SlabDelete
///		Free all memory which is allocated in the given context.
///
/// # Safety
/// `context` must be a valid Slab context.
pub unsafe fn SlabDelete(context: MemoryContext) {
    // Reset to release all the SlabBlocks
    SlabReset(context);
    // And free the context header
    free(context as *mut c_void);
}

/// Small helper for allocating a new chunk from a chunk, to avoid duplicating
/// the code between SlabAlloc() and SlabAllocFromNewBlock().
///
/// # Safety
/// `context`/`block`/`chunk` must be live and consistent with the slab layout.
#[inline]
unsafe fn SlabAllocSetupNewChunk(
    context: MemoryContext,
    block: *mut SlabBlock,
    chunk: *mut MemoryChunk,
    size: Size,
) -> *mut c_void {
    let slab: *mut SlabContext = context as *mut SlabContext;

    // Check that the chunk pointer is actually somewhere on the block and is
    // aligned as expected.
    Assert!(chunk >= SlabBlockGetChunk(slab, block, 0));
    Assert!(chunk <= SlabBlockGetChunk(slab, block, (*slab).chunksPerBlock - 1));
    Assert!(SlabChunkMod(slab, block, chunk) == 0);

    // Prepare to initialize the chunk header.
    // VALGRIND_MAKE_MEM_UNDEFINED(chunk, Slab_CHUNKHDRSZ);
    // TODO(pg-port): valgrind no-op.

    MemoryChunkSetHdrMask(
        chunk,
        block as *mut c_void,
        MAXALIGN((*slab).chunkSize as Size),
        MCTX_SLAB_ID,
    );

    // #ifdef MEMORY_CONTEXT_CHECKING
    //     /* slab mark to catch clobber of "unused" space */
    //     Assert(slab->chunkSize < (slab->fullChunkSize - Slab_CHUNKHDRSZ));
    //     set_sentinel(MemoryChunkGetPointer(chunk), size);
    //     VALGRIND_MAKE_MEM_NOACCESS(((char *) chunk) + Slab_CHUNKHDRSZ + slab->chunkSize,
    //                                slab->fullChunkSize - (slab->chunkSize + Slab_CHUNKHDRSZ));
    // #endif
    // #ifdef RANDOMIZE_ALLOCATED_MEMORY
    //     /* fill the allocated space with junk */
    //     randomize_mem((char *) MemoryChunkGetPointer(chunk), size);
    // #endif
    // TODO(pg-port): MEMORY_CONTEXT_CHECKING / RANDOMIZE_ALLOCATED_MEMORY (default-off).
    let _ = size;

    // Disallow access to the chunk header.
    // VALGRIND_MAKE_MEM_NOACCESS(chunk, Slab_CHUNKHDRSZ);
    // TODO(pg-port): valgrind no-op.

    MemoryChunkGetPointer(chunk)
}

/// Helper for SlabAlloc() that allocates from a new (or recycled) block.
///
/// # Safety
/// `context` must be a valid Slab context.
// pg_noinline
#[inline(never)]
unsafe fn SlabAllocFromNewBlock(context: MemoryContext, size: Size, flags: c_int) -> *mut c_void {
    let slab: *mut SlabContext = context as *mut SlabContext;
    let block: *mut SlabBlock;
    let chunk: *mut MemoryChunk;
    let blocklist: *mut dlist_head;
    let blocklist_idx: int32;

    // to save allocating a new one, first check the empty blocks list
    if dclist_count(&(*slab).emptyblocks) > 0 {
        let node: *mut dlist_node = dclist_pop_head_node(&mut (*slab).emptyblocks);

        block = dlist_container!(SlabBlock, node, node);

        // SlabFree() should have left this block in a valid state with all
        // chunks free.  Ensure that's the case.
        Assert!((*block).nfree == (*slab).chunksPerBlock);

        // fetch the next chunk from this block
        chunk = SlabGetNextFreeChunk(slab, block);
    } else {
        block = malloc((*slab).blockSize as usize) as *mut SlabBlock;

        if unlikely(block.is_null()) {
            return MemoryContextAllocationFailure(context, size, flags);
        }

        (*block).slab = slab;
        (*context).mem_allocated += (*slab).blockSize as Size;

        // use the first chunk in the new block
        chunk = SlabBlockGetChunk(slab, block, 0);

        (*block).nfree = (*slab).chunksPerBlock - 1;
        (*block).unused = SlabBlockGetChunk(slab, block, 1);
        (*block).freehead = core::ptr::null_mut();
        (*block).nunused = (*slab).chunksPerBlock - 1;
    }

    // find the blocklist element for storing blocks with 1 used chunk
    blocklist_idx = SlabBlocklistIndex(slab, (*block).nfree);
    blocklist = &mut (*slab).blocklist[blocklist_idx as usize];

    // this better be empty.  We just added a block thinking it was
    Assert!(dlist_is_empty(blocklist));

    dlist_push_head(blocklist, &mut (*block).node);

    (*slab).curBlocklistIndex = blocklist_idx;

    SlabAllocSetupNewChunk(context, block, chunk, size)
}

/// SlabAllocInvalidSize
///		Handle raising an ERROR for an invalid size request.  We don't do this
///		in slab alloc as calling the elog functions would force the compiler
///		to setup the stack frame in SlabAlloc.  For performance reasons, we
///		want to avoid that.
///
/// # Safety
/// `context` must be a valid Slab context.
// pg_noinline pg_noreturn
#[inline(never)]
unsafe fn SlabAllocInvalidSize(context: MemoryContext, size: Size) -> ! {
    let slab: *mut SlabContext = context as *mut SlabContext;

    // elog(ERROR, "unexpected alloc chunk size %zu (expected %u)", size, slab->chunkSize);
    panic!(
        "unexpected alloc chunk size {} (expected {})",
        size,
        (*slab).chunkSize
    );
}

/// SlabAlloc
///		Returns a pointer to a newly allocated memory chunk or raises an ERROR
///		on allocation failure, or returns NULL when flags contains
///		MCXT_ALLOC_NO_OOM.  'size' must be the same size as was specified
///		during SlabContextCreate().
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
/// `context` must be a valid Slab context.
pub unsafe fn SlabAlloc(context: MemoryContext, size: Size, flags: c_int) -> *mut c_void {
    let slab: *mut SlabContext = context as *mut SlabContext;
    let block: *mut SlabBlock;
    let chunk: *mut MemoryChunk;

    Assert!(SlabIsValid(slab));

    // sanity check that this is pointing to a valid blocklist
    Assert!((*slab).curBlocklistIndex >= 0);
    Assert!((*slab).curBlocklistIndex <= SlabBlocklistIndex(slab, (*slab).chunksPerBlock));

    // Make sure we only allow correct request size.  This doubles as the
    // MemoryContextCheckSize check.
    if unlikely(size != (*slab).chunkSize as Size) {
        SlabAllocInvalidSize(context, size);
    }

    if unlikely((*slab).curBlocklistIndex == 0) {
        // Handle the case when there are no partially filled blocks
        // available.  This happens either when the last allocation took the
        // last chunk in the block, or when SlabFree() free'd the final block.
        return SlabAllocFromNewBlock(context, size, flags);
    } else {
        let blocklist: *mut dlist_head = &mut (*slab).blocklist[(*slab).curBlocklistIndex as usize];
        let new_blocklist_idx: int32;

        Assert!(!dlist_is_empty(blocklist));

        // grab the block from the blocklist
        block = dlist_head_element!(SlabBlock, node, blocklist);

        // make sure we actually got a valid block, with matching nfree
        Assert!(!block.is_null());
        Assert!((*slab).curBlocklistIndex == SlabBlocklistIndex(slab, (*block).nfree));
        Assert!((*block).nfree > 0);

        // fetch the next chunk from this block
        chunk = SlabGetNextFreeChunk(slab, block);

        // get the new blocklist index based on the new free chunk count
        new_blocklist_idx = SlabBlocklistIndex(slab, (*block).nfree);

        // Handle the case where the blocklist index changes.  This also deals
        // with blocks becoming full as only full blocks go at index 0.
        if unlikely((*slab).curBlocklistIndex != new_blocklist_idx) {
            dlist_delete_from(blocklist, &mut (*block).node);
            dlist_push_head(&mut (*slab).blocklist[new_blocklist_idx as usize], &mut (*block).node);

            if dlist_is_empty(blocklist) {
                (*slab).curBlocklistIndex = SlabFindNextBlockListIndex(slab);
            }
        }
    }

    SlabAllocSetupNewChunk(context, block, chunk, size)
}

/// SlabFree
///		Frees allocated memory; memory is removed from the slab.
///
/// # Safety
/// `pointer` must be a live allocation from a Slab context.
pub unsafe fn SlabFree(pointer: *mut c_void) {
    let chunk: *mut MemoryChunk = PointerGetMemoryChunk(pointer);
    let block: *mut SlabBlock;
    let slab: *mut SlabContext;
    let curBlocklistIdx: c_int;
    let newBlocklistIdx: c_int;

    // Allow access to the chunk header.
    // VALGRIND_MAKE_MEM_DEFINED(chunk, Slab_CHUNKHDRSZ);
    // TODO(pg-port): valgrind no-op.

    block = MemoryChunkGetBlock(chunk) as *mut SlabBlock;

    // For speed reasons we just Assert that the referenced block is good.
    // Future field experience may show that this Assert had better become a
    // regular runtime test-and-elog check.
    Assert!(SlabBlockIsValid(block));
    slab = (*block).slab;

    // #ifdef MEMORY_CONTEXT_CHECKING
    //     /* Test for someone scribbling on unused space in chunk */
    //     Assert(slab->chunkSize < (slab->fullChunkSize - Slab_CHUNKHDRSZ));
    //     if (!sentinel_ok(pointer, slab->chunkSize))
    //         elog(WARNING, "detected write past chunk end in %s %p", slab->header.name, chunk);
    // #endif
    // TODO(pg-port): MEMORY_CONTEXT_CHECKING (default-off).

    // push this chunk onto the head of the block's free list
    // C: *(MemoryChunk **) pointer = block->freehead;
    *(pointer as *mut *mut MemoryChunk) = (*block).freehead;
    (*block).freehead = chunk;

    (*block).nfree += 1;

    Assert!((*block).nfree > 0);
    Assert!((*block).nfree <= (*slab).chunksPerBlock);

    // #ifdef CLOBBER_FREED_MEMORY
    //     /* don't wipe the free list MemoryChunk pointer stored in the chunk */
    //     wipe_mem((char *) pointer + sizeof(MemoryChunk *),
    //              slab->chunkSize - sizeof(MemoryChunk *));
    // #endif
    // TODO(pg-port): CLOBBER_FREED_MEMORY no-op (default-off path).

    curBlocklistIdx = SlabBlocklistIndex(slab, (*block).nfree - 1);
    newBlocklistIdx = SlabBlocklistIndex(slab, (*block).nfree);

    // Check if the block needs to be moved to another element on the
    // blocklist based on it now having 1 more free chunk.
    if unlikely(curBlocklistIdx != newBlocklistIdx) {
        // do the move
        dlist_delete_from(&mut (*slab).blocklist[curBlocklistIdx as usize], &mut (*block).node);
        dlist_push_head(&mut (*slab).blocklist[newBlocklistIdx as usize], &mut (*block).node);

        // The blocklist[curBlocklistIdx] may now be empty or we may now be
        // able to use a lower-element blocklist.  We'll need to redetermine
        // what the slab->curBlocklistIndex is if the current blocklist was
        // changed or if a lower element one was changed.  We must ensure we
        // use the list with the fullest block(s).
        if (*slab).curBlocklistIndex >= curBlocklistIdx {
            (*slab).curBlocklistIndex = SlabFindNextBlockListIndex(slab);

            // We know there must be a block with at least 1 unused chunk as
            // we just pfree'd one.  Ensure curBlocklistIndex reflects this.
            Assert!((*slab).curBlocklistIndex > 0);
        }
    }

    // Handle when a block becomes completely empty
    if unlikely((*block).nfree == (*slab).chunksPerBlock) {
        // remove the block
        dlist_delete_from(&mut (*slab).blocklist[newBlocklistIdx as usize], &mut (*block).node);

        // To avoid thrashing malloc/free, we keep a list of empty blocks that
        // we can reuse again instead of having to malloc a new one.
        if dclist_count(&(*slab).emptyblocks) < SLAB_MAXIMUM_EMPTY_BLOCKS {
            dclist_push_head(&mut (*slab).emptyblocks, &mut (*block).node);
        } else {
            // When we have enough empty blocks stored already, we actually
            // free the block.
            // #ifdef CLOBBER_FREED_MEMORY
            //     wipe_mem(block, slab->blockSize);
            // #endif
            // TODO(pg-port): CLOBBER_FREED_MEMORY no-op (default-off path).
            free(block as *mut c_void);
            (*slab).header.mem_allocated -= (*slab).blockSize as Size;
        }

        // Check if we need to reset the blocklist index.  This is required
        // when the blocklist this block is on has become completely empty.
        if (*slab).curBlocklistIndex == newBlocklistIdx
            && dlist_is_empty(&(*slab).blocklist[newBlocklistIdx as usize])
        {
            (*slab).curBlocklistIndex = SlabFindNextBlockListIndex(slab);
        }
    }
}

/// SlabRealloc
///		Change the allocated size of a chunk.
///
/// As Slab is designed for allocating equally-sized chunks of memory, it can't
/// do an actual chunk size change.  We try to be gentle and allow calls with
/// exactly the same size, as in that case we can simply return the same
/// chunk.  When the size differs, we throw an error.
///
/// We could also allow requests with size < chunkSize.  That however seems
/// rather pointless - Slab is meant for chunks of constant size, and moreover
/// realloc is usually used to enlarge the chunk.
///
/// # Safety
/// `pointer` must be a live allocation from a Slab context.
pub unsafe fn SlabRealloc(pointer: *mut c_void, size: Size, flags: c_int) -> *mut c_void {
    let chunk: *mut MemoryChunk = PointerGetMemoryChunk(pointer);
    let block: *mut SlabBlock;
    let slab: *mut SlabContext;

    let _ = flags;

    // Allow access to the chunk header.
    // VALGRIND_MAKE_MEM_DEFINED(chunk, Slab_CHUNKHDRSZ);
    // TODO(pg-port): valgrind no-op.

    block = MemoryChunkGetBlock(chunk) as *mut SlabBlock;

    // Disallow access to the chunk header.
    // VALGRIND_MAKE_MEM_NOACCESS(chunk, Slab_CHUNKHDRSZ);
    // TODO(pg-port): valgrind no-op.

    // Try to verify that we have a sane block pointer: the block header
    // should reference a slab context.  (We use a test-and-elog, not just
    // Assert, because it seems highly likely that we're here in error in the
    // first place.)
    if !SlabBlockIsValid(block) {
        // elog(ERROR, "could not find block containing chunk %p", chunk);
        panic!("could not find block containing chunk {:p}", chunk);
    }
    slab = (*block).slab;

    // can't do actual realloc with slab, but let's try to be gentle
    if size == (*slab).chunkSize as Size {
        return pointer;
    }

    // elog(ERROR, "slab allocator does not support realloc()");
    panic!("slab allocator does not support realloc()");
    // return NULL;				/* keep compiler quiet */
}

/// SlabGetChunkContext
///		Return the MemoryContext that 'pointer' belongs to.
///
/// # Safety
/// `pointer` must be a live allocation from a Slab context.
pub unsafe fn SlabGetChunkContext(pointer: *mut c_void) -> MemoryContext {
    let chunk: *mut MemoryChunk = PointerGetMemoryChunk(pointer);
    let block: *mut SlabBlock;

    // Allow access to the chunk header.
    // VALGRIND_MAKE_MEM_DEFINED(chunk, Slab_CHUNKHDRSZ);
    // TODO(pg-port): valgrind no-op.

    block = MemoryChunkGetBlock(chunk) as *mut SlabBlock;

    // Disallow access to the chunk header.
    // VALGRIND_MAKE_MEM_NOACCESS(chunk, Slab_CHUNKHDRSZ);
    // TODO(pg-port): valgrind no-op.

    Assert!(SlabBlockIsValid(block));

    &mut (*(*block).slab).header
}

/// SlabGetChunkSpace
///		Given a currently-allocated chunk, determine the total space
///		it occupies (including all memory-allocation overhead).
///
/// # Safety
/// `pointer` must be a live allocation from a Slab context.
pub unsafe fn SlabGetChunkSpace(pointer: *mut c_void) -> Size {
    let chunk: *mut MemoryChunk = PointerGetMemoryChunk(pointer);
    let block: *mut SlabBlock;
    let slab: *mut SlabContext;

    // Allow access to the chunk header.
    // VALGRIND_MAKE_MEM_DEFINED(chunk, Slab_CHUNKHDRSZ);
    // TODO(pg-port): valgrind no-op.

    block = MemoryChunkGetBlock(chunk) as *mut SlabBlock;

    // Disallow access to the chunk header.
    // VALGRIND_MAKE_MEM_NOACCESS(chunk, Slab_CHUNKHDRSZ);
    // TODO(pg-port): valgrind no-op.

    Assert!(SlabBlockIsValid(block));
    slab = (*block).slab;

    (*slab).fullChunkSize as Size
}

/// SlabIsEmpty
///		Is the slab empty of any allocated space?
///
/// # Safety
/// `context` must be a valid Slab context.
pub unsafe fn SlabIsEmpty(context: MemoryContext) -> bool {
    Assert!(SlabIsValid(context as *mut SlabContext));

    (*context).mem_allocated == 0
}

/// SlabStats
///		Compute stats about memory consumption of a Slab context.
///
/// printfunc: if not NULL, pass a human-readable stats string to this.
/// passthru: pass this pointer through to printfunc.
/// totals: if not NULL, add stats about this context into *totals.
/// print_to_stderr: print stats to stderr if true, elog otherwise.
///
/// # Safety
/// `context` must be a valid Slab context; `totals` must be NULL or valid.
pub unsafe fn SlabStats(
    context: MemoryContext,
    printfunc: MemoryStatsPrintFunc,
    passthru: *mut c_void,
    totals: *mut MemoryContextCounters,
    print_to_stderr: bool,
) {
    let slab: *mut SlabContext = context as *mut SlabContext;
    let mut nblocks: Size = 0;
    let mut freechunks: Size = 0;
    let mut totalspace: Size;
    let mut freespace: Size = 0;
    let mut i: c_int;

    Assert!(SlabIsValid(slab));

    // Include context header in totalspace
    totalspace = Slab_CONTEXT_HDRSZ((*slab).chunksPerBlock);

    // Add the space consumed by blocks in the emptyblocks list
    totalspace += dclist_count(&(*slab).emptyblocks) as Size * (*slab).blockSize as Size;

    i = 0;
    while i < SLAB_BLOCKLIST_COUNT as c_int {
        let mut iter: dlist_iter = core::mem::zeroed();

        dlist_foreach!(iter, &mut (*slab).blocklist[i as usize], {
            let block: *mut SlabBlock = dlist_container!(SlabBlock, node, iter.cur);

            nblocks += 1;
            totalspace += (*slab).blockSize as Size;
            freespace += (*slab).fullChunkSize as Size * (*block).nfree as Size;
            freechunks += (*block).nfree as Size;
        });
        i += 1;
    }

    if let Some(printfunc) = printfunc {
        // char stats_string[200];
        // /* XXX should we include free chunks on empty blocks? */
        // snprintf(stats_string, sizeof(stats_string),
        //          "%zu total in %zu blocks; %u empty blocks; %zu free (%zu chunks); %zu used",
        //          totalspace, nblocks, dclist_count(&slab->emptyblocks),
        //          freespace, freechunks, totalspace - freespace);
        let stats_string = format!(
            "{} total in {} blocks; {} empty blocks; {} free ({} chunks); {} used\0",
            totalspace,
            nblocks,
            dclist_count(&(*slab).emptyblocks),
            freespace,
            freechunks,
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
        (*totals).freechunks += freechunks;
        (*totals).totalspace += totalspace;
        (*totals).freespace += freespace;
    }
}

// #ifdef MEMORY_CONTEXT_CHECKING

/// `sentinel_ok` (memdebug.h) - check the sentinel byte at `offset` past `base`
/// is intact.  Only compiled in MEMORY_CONTEXT_CHECKING builds.
///
/// TODO(pg-port): import the real `sentinel_ok` from memdebug once that header is
/// ported; stubbed locally as a genuinely-unported MEMORY_CONTEXT_CHECKING dep.
#[cfg(feature = "memory_context_checking")]
unsafe fn sentinel_ok(_base: *const c_void, _offset: Size) -> bool {
    true
}

/// SlabCheck
///		Walk through all blocks looking for inconsistencies.
///
/// NOTE: report errors as WARNING, *not* ERROR or FATAL.  Otherwise you'll
/// find yourself in an infinite loop when trouble occurs, because this
/// routine will be entered again when elog cleanup tries to release memory!
///
/// # Safety
/// `context` must be a valid SlabContext.
#[cfg(feature = "memory_context_checking")]
pub unsafe fn SlabCheck(context: MemoryContext) {
    let slab: *mut SlabContext = context as *mut SlabContext;
    let i: c_int;
    let mut nblocks: c_int = 0;
    let name: *const c_char = (*slab).header.name;
    let mut iter: dlist_iter = core::mem::zeroed();

    Assert!(SlabIsValid(slab));
    Assert!((*slab).chunksPerBlock > 0);

    // Have a look at the empty blocks.  These should have all their chunks
    // marked as free.  Ensure that's the case.
    crate::dclist_foreach!(iter, &mut (*slab).emptyblocks, {
        let block: *mut SlabBlock = dlist_container!(SlabBlock, node, iter.cur);

        if (*block).nfree != (*slab).chunksPerBlock {
            elog!(
                crate::utils::elog::WARNING,
                "problem in slab {}: empty block {:p} should have {} free chunks but has {} chunks free",
                std::ffi::CStr::from_ptr(name).to_string_lossy(),
                block,
                (*slab).chunksPerBlock,
                (*block).nfree
            );
        }
    });

    // walk the non-empty block lists
    let mut i_loop: c_int = 0;
    let _ = i;
    while i_loop < SLAB_BLOCKLIST_COUNT as c_int {
        let i = i_loop;
        let j: c_int;
        let mut nfree: c_int;

        // walk all blocks on this blocklist
        crate::dlist_foreach!(iter, &mut (*slab).blocklist[i as usize], {
            let block: *mut SlabBlock = dlist_container!(SlabBlock, node, iter.cur);
            let mut cur_chunk: *mut MemoryChunk;

            // Make sure the number of free chunks (in the block header)
            // matches the position in the blocklist.
            if SlabBlocklistIndex(slab, (*block).nfree) != i {
                elog!(
                    crate::utils::elog::WARNING,
                    "problem in slab {}: block {:p} is on blocklist {} but should be on blocklist {}",
                    std::ffi::CStr::from_ptr(name).to_string_lossy(),
                    block,
                    i,
                    SlabBlocklistIndex(slab, (*block).nfree)
                );
            }

            // make sure the block is not empty
            if (*block).nfree >= (*slab).chunksPerBlock {
                elog!(
                    crate::utils::elog::WARNING,
                    "problem in slab {}: empty block {:p} incorrectly stored on blocklist element {}",
                    std::ffi::CStr::from_ptr(name).to_string_lossy(),
                    block,
                    i
                );
            }

            // make sure the slab pointer correctly points to this context
            if (*block).slab != slab {
                elog!(
                    crate::utils::elog::WARNING,
                    "problem in slab {}: bogus slab link in block {:p}",
                    std::ffi::CStr::from_ptr(name).to_string_lossy(),
                    block
                );
            }

            // reset the array of free chunks for this block
            std::ptr::write_bytes(
                (*slab).isChunkFree,
                0,
                (*slab).chunksPerBlock as usize,
            );
            nfree = 0;

            // walk through the block's free list chunks
            cur_chunk = (*block).freehead;
            while !cur_chunk.is_null() {
                let chunkidx: c_int = SlabChunkIndex(slab, block, cur_chunk);

                // Ensure the free list link points to something on the block
                // at an address aligned according to the full chunk size.
                if cur_chunk < SlabBlockGetChunk(slab, block, 0)
                    || cur_chunk > SlabBlockGetChunk(slab, block, (*slab).chunksPerBlock - 1)
                    || SlabChunkMod(slab, block, cur_chunk) != 0
                {
                    elog!(
                        crate::utils::elog::WARNING,
                        "problem in slab {}: bogus free list link {:p} in block {:p}",
                        std::ffi::CStr::from_ptr(name).to_string_lossy(),
                        cur_chunk,
                        block
                    );
                }

                // count the chunk and mark it free on the free chunk array
                nfree += 1;
                *(*slab).isChunkFree.add(chunkidx as usize) = true;

                // read pointer of the next free chunk
                // VALGRIND_MAKE_MEM_DEFINED(MemoryChunkGetPointer(cur_chunk), sizeof(MemoryChunk *));
                cur_chunk = *(SlabChunkGetPointer(cur_chunk) as *mut *mut MemoryChunk);
            }

            // check that the unused pointer matches what nunused claims
            if SlabBlockGetChunk(slab, block, (*slab).chunksPerBlock - (*block).nunused)
                != (*block).unused
            {
                elog!(
                    crate::utils::elog::WARNING,
                    "problem in slab {}: mismatch detected between nunused chunks and unused pointer in block {:p}",
                    std::ffi::CStr::from_ptr(name).to_string_lossy(),
                    block
                );
            }

            // count the remaining free chunks that have yet to make it onto
            // the block's free list.
            cur_chunk = (*block).unused;
            let mut j_loop: c_int = 0;
            let _ = j;
            while j_loop < (*block).nunused {
                let chunkidx: c_int = SlabChunkIndex(slab, block, cur_chunk);

                // count the chunk as free and mark it as so in the array
                nfree += 1;
                if chunkidx < (*slab).chunksPerBlock {
                    *(*slab).isChunkFree.add(chunkidx as usize) = true;
                }

                // move forward 1 chunk
                cur_chunk = ((cur_chunk as *mut c_char).add((*slab).fullChunkSize as usize))
                    as *mut MemoryChunk;
                j_loop += 1;
            }

            let mut j_loop2: c_int = 0;
            while j_loop2 < (*slab).chunksPerBlock {
                if !(*(*slab).isChunkFree.add(j_loop2 as usize)) {
                    let chunk: *mut MemoryChunk = SlabBlockGetChunk(slab, block, j_loop2);
                    let chunkblock: *mut SlabBlock;

                    // Allow access to the chunk header.
                    // VALGRIND_MAKE_MEM_DEFINED(chunk, Slab_CHUNKHDRSZ);

                    chunkblock = MemoryChunkGetBlock(chunk) as *mut SlabBlock;

                    // Disallow access to the chunk header.
                    // VALGRIND_MAKE_MEM_NOACCESS(chunk, Slab_CHUNKHDRSZ);

                    // check the chunk's blockoffset correctly points back to
                    // the block
                    if chunkblock != block {
                        elog!(
                            crate::utils::elog::WARNING,
                            "problem in slab {}: bogus block link in block {:p}, chunk {:p}",
                            std::ffi::CStr::from_ptr(name).to_string_lossy(),
                            block,
                            chunk
                        );
                    }

                    // check the sentinel byte is intact
                    Assert!((*slab).chunkSize < ((*slab).fullChunkSize - Slab_CHUNKHDRSZ as uint32));
                    if !sentinel_ok(
                        chunk as *const c_void,
                        Slab_CHUNKHDRSZ + (*slab).chunkSize as Size,
                    ) {
                        elog!(
                            crate::utils::elog::WARNING,
                            "problem in slab {}: detected write past chunk end in block {:p}, chunk {:p}",
                            std::ffi::CStr::from_ptr(name).to_string_lossy(),
                            block,
                            chunk
                        );
                    }
                }
                j_loop2 += 1;
            }

            // Make sure we got the expected number of free chunks (as tracked
            // in the block header).
            if nfree != (*block).nfree {
                elog!(
                    crate::utils::elog::WARNING,
                    "problem in slab {}: nfree in block {:p} is {} but {} chunk were found as free",
                    std::ffi::CStr::from_ptr(name).to_string_lossy(),
                    block,
                    (*block).nfree,
                    nfree
                );
            }

            nblocks += 1;
        });
        i_loop += 1;
    }

    // the stored empty blocks are tracked in mem_allocated too
    nblocks += dclist_count(&(*slab).emptyblocks) as c_int;

    Assert!((nblocks as Size) * (*slab).blockSize as Size == (*context).mem_allocated as Size);
}

// #endif							/* MEMORY_CONTEXT_CHECKING */

// =============================================================================
// Summary
// -----------------------------------------------------------------------------
// Structs (all #[repr(C)], identifiers verbatim):
//   - SlabContext: FIRST field is the embedded `header: MemoryContextData`, then
//     the uint32/int32 params (chunkSize/fullChunkSize/blockSize/chunksPerBlock/
//     curBlocklistIndex/blocklist_shift), the `emptyblocks: dclist_head`, and the
//     `blocklist: [dlist_head; SLAB_BLOCKLIST_COUNT]`. (The MEMORY_CONTEXT_CHECKING
//     `isChunkFree` field is omitted as a cfg TODO.)
//   - SlabBlock: { slab: *mut SlabContext, nfree, nunused (int32), freehead,
//     unused (*mut MemoryChunk), node: dlist_node }.
//   No SlabChunk struct exists in slab.c (chunks are bare MemoryChunk headers
//   followed by payload); SlabChunkGetPointer/SlabBlockGetChunk are the helpers.
//
// Method fns (signatures EXACTLY match the MemoryContextMethods fn-pointer types
// in memnodes.rs), all `pub unsafe fn`:
//   - SlabAlloc(context, size, flags) -> *mut c_void
//   - SlabFree(pointer)
//   - SlabRealloc(pointer, size, flags) -> *mut c_void
//   - SlabReset(context)
//   - SlabDelete(context)
//   - SlabGetChunkContext(pointer) -> MemoryContext
//   - SlabGetChunkSpace(pointer) -> Size
//   - SlabIsEmpty(context) -> bool
//   - SlabStats(context, printfunc, passthru, totals, print_to_stderr)
// Plus the public constructor SlabContextCreate(parent, name, blockSize,
// chunkSize) -> MemoryContext (node tag T_SlabContext, method id MCTX_SLAB_ID via
// MemoryContextCreate). Private helpers: SlabBlocklistIndex,
// SlabFindNextBlockListIndex, SlabGetNextFreeChunk, SlabAllocSetupNewChunk,
// SlabAllocFromNewBlock (#[inline(never)] == pg_noinline), SlabAllocInvalidSize
// (#[inline(never)], `-> !` == pg_noreturn).
//
// dlist usage: the per-fill-fraction block lists `blocklist[]` are `dlist_head`s
// manipulated with dlist_init / dlist_is_empty / dlist_push_head /
// dlist_delete / dlist_delete_from, the dlist_head_element!/dlist_container!
// macros, and dlist_foreach!/dlist_foreach_modify! (trailing-block form). The
// retained-empty-blocks list `emptyblocks` is a `dclist_head` using dclist_init /
// dclist_count / dclist_push_head / dclist_pop_head_node / dclist_delete_from and
// dclist_foreach_modify!/dclist_container!.
//
// Wrapping arithmetic: SlabBlocklistIndex's two's-complement trick
// `-((-nfree) >> shift)` uses wrapping_neg + arithmetic `>>` to match C's signed
// shift/negate. SlabBlockGetChunk's `block + BLOCKHDRSZ + n*fullChunkSize` uses
// wrapping_add/wrapping_mul on the byte offset; SlabChunkMod uses usize
// wrapping_sub for the `(char *)chunk - (char *)base` difference; chunksPerBlock's
// `(blockSize - Slab_BLOCKHDRSZ) / fullChunkSize` uses wrapping_sub. Other
// counters (nfree/nunused +=/-=, mem_allocated -=) mirror C directly.
//
// malloc/free binding: `extern "C" { malloc/free }` (raw libc), used for blocks
// and the context header exactly as slab.c does; coexists with the bootstrap
// utils::palloc. The real MemoryContextCreate/MemoryContextAllocationFailure/
// MemoryContextStats are imported from crate::utils::mmgr::mcxt.
//
// Stubs / default-off paths: all VALGRIND_* hooks are no-ops; the
// MEMORY_CONTEXT_CHECKING (Slab_CONTEXT_HDRSZ extra term, isChunkFree, sentinel
// bytes, SlabCheck) / RANDOMIZE_ALLOCATED_MEMORY / CLOBBER_FREED_MEMORY branches
// translate the default-off path with the checking branches preserved as comments
// + TODO(pg-port). elog(ERROR)/ereport(ERROR) become panic! (matching aset.rs and
// the elog.rs shim where level >= ERROR panics). SlabChunkIndex/SlabChunkMod are
// USE_ASSERT_CHECKING helpers (SlabChunkMod kept for the Assert!()s).
//
// Every slab.c function was translated: SlabBlocklistIndex,
// SlabFindNextBlockListIndex, SlabGetNextFreeChunk, SlabContextCreate, SlabReset,
// SlabDelete, SlabAllocSetupNewChunk, SlabAllocFromNewBlock, SlabAllocInvalidSize,
// SlabAlloc, SlabFree, SlabRealloc, SlabGetChunkContext, SlabGetChunkSpace,
// SlabIsEmpty, SlabStats (SlabCheck noted as a cfg-gated TODO).
// =============================================================================
