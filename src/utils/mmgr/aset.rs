//! Translation of postgres/src/backend/utils/mmgr/aset.c
//!
//! Allocation set definitions.
//!
//! AllocSet is our standard implementation of the abstract MemoryContext
//! type.
//!
//! NOTE:
//!	This is a new (Feb. 05, 1999) implementation of the allocation set
//!	routines. AllocSet...() does not use OrderedSet...() any more.
//!	Instead it manages allocations in a block pool by itself, combining
//!	many small allocations in a few bigger blocks. AllocSetFree() normally
//!	doesn't free() memory really. It just add's the free'd area to some
//!	list for later reuse by AllocSetAlloc(). All memory blocks are free()'d
//!	at once on AllocSetReset(), which happens when the memory context gets
//!	destroyed.
//!				Jan Wieck
//!
//!	Performance improvement from Tom Lane, 8/99: for extremely large request
//!	sizes, we do want to be able to give the memory back to free() as soon
//!	as it is pfree()'d.  Otherwise we risk tying up a lot of memory in
//!	freelist entries that might never be usable.  This is specially needed
//!	when the caller is repeatedly repalloc()'ing a block bigger and bigger;
//!	the previous instances of the block were guaranteed to be wasted until
//!	AllocSetReset() under the old way.
//!
//!	Further improvement 12/00: as the code stood, request sizes in the
//!	midrange between "small" and "large" were handled very inefficiently,
//!	because any sufficiently large free chunk would be used to satisfy a
//!	request, even if it was much larger than necessary.  This led to more
//!	and more wasted space in allocated chunks over time.  To fix, get rid
//!	of the midrange behavior: we now handle only "small" power-of-2-size
//!	chunks as chunks.  Anything "large" is passed off to malloc().  Change
//!	the number of freelists to change the small/large boundary.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! NOTE (port staging): this is the REAL AllocSet allocator, translated
//! additively under `utils/mmgr` while the rest of the crate still uses the
//! context-less bootstrap allocator in `utils::palloc`. Imports are explicit
//! (NOT `crate::prelude::*`) to avoid clashing with the bootstrap
//! `MemoryContext`/`MemoryContextData` symbols.

// c.h: MAXALIGN/MAXALIGN_DOWN, Size, uint32/uint64, unlikely, Max/Min.
use crate::c::{uint32, uint64, unlikely, Max, Min, Size, MAXALIGN};
// pg_config.h: MAXIMUM_ALIGNOF (referenced indirectly by MAXALIGN; kept for clarity).
#[allow(unused_imports)]
use crate::pg_config::MAXIMUM_ALIGNOF;
// nodes/nodes.h: T_AllocSetContext (NodeTag) and nodeTag() (used via IsA!).
use crate::nodes::nodes::{nodeTag, NodeTag};
use crate::IsA; // nodes.h IsA! macro (used by AllocSetIsValid)
// port/pg_bitutils.h: pg_leftmost_one_pos32 (HAVE_BITSCAN_REVERSE path).
use crate::port::pg_bitutils::pg_leftmost_one_pos32;
// utils/memutils.h: AllocHugeSizeIsValid (for the maxBlockSize Assert).
#[allow(unused_imports)]
use crate::utils::memutils::AllocHugeSizeIsValid;
// utils/palloc.h: allocation flag bits.
#[allow(unused_imports)]
use crate::utils::palloc::{MCXT_ALLOC_HUGE, MCXT_ALLOC_NO_OOM, MCXT_ALLOC_ZERO};
// utils/memutils_internal.h: method ID + context-type-independent helpers.
use crate::utils::mmgr::memutils_internal::{MemoryContextCheckSize, MemoryContextMethodID, MCTX_ASET_ID};
// The real MemoryContextCreate / allocation-failure helpers live in mcxt.rs (their
// C bodies are in mcxt.c); use those rather than memutils_internal's stubs.
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
    PointerGetMemoryChunk, MEMORYCHUNK_MAX_BLOCKOFFSET,
};
use core::ffi::{c_char, c_int, c_void};
// `Assert!` and `IsA!` are brought into scope crate-wide via #[macro_use].

// aset.c uses raw malloc/free/realloc for its blocks.
extern "C" {
    fn malloc(size: usize) -> *mut c_void;
    fn free(p: *mut c_void);
    fn realloc(p: *mut c_void, size: usize) -> *mut c_void;
}

// ----------------------------------------------------------------------------
// Constants from utils/memutils.h that are not yet exported by the Rust crate
// (memutils.rs currently only exports the ALLOCSET_*_SIZES triples). Defined
// here verbatim from the C header so this module is self-contained; they will
// move to memutils.rs when that file is fully translated.
// TODO(pg-port): hoist these to crate::utils::memutils.
// ----------------------------------------------------------------------------

/// `ALLOCSET_DEFAULT_MINSIZE` (memutils.h)
const ALLOCSET_DEFAULT_MINSIZE: Size = 0;
/// `ALLOCSET_DEFAULT_INITSIZE` (memutils.h)
const ALLOCSET_DEFAULT_INITSIZE: Size = 8 * 1024;
/// `ALLOCSET_SMALL_MINSIZE` (memutils.h)
const ALLOCSET_SMALL_MINSIZE: Size = 0;
/// `ALLOCSET_SMALL_INITSIZE` (memutils.h)
const ALLOCSET_SMALL_INITSIZE: Size = 1 * 1024;
/// `ALLOCSET_SEPARATE_THRESHOLD` (memutils.h): the threshold above which an
/// allocation is given its own dedicated block. Must equal ALLOC_CHUNK_LIMIT.
const ALLOCSET_SEPARATE_THRESHOLD: Size = 8192;
/// `InvalidAllocSize` (memutils.h): SIZE_MAX. Used only in
/// MEMORY_CONTEXT_CHECKING paths (default-off here).
#[allow(dead_code)]
const InvalidAllocSize: Size = Size::MAX;

/// `ALLOCSET_DEFAULT_SIZES` expanded: (minContextSize, initBlockSize, maxBlockSize).
/// Mirrors the C macro `ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, 8*1024*1024`.
pub const ALLOCSET_DEFAULT_SIZES: (Size, Size, Size) =
    (ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, 8 * 1024 * 1024);

/// Convenience wrapper matching C's `AllocSetContextCreate(parent, name, SIZES...)`.
///
/// # Safety
/// Same as [`AllocSetContextCreateInternal`].
#[inline]
pub unsafe fn AllocSetContextCreate(
    parent: MemoryContext,
    name: *const c_char,
    sizes: (Size, Size, Size),
) -> MemoryContext {
    AllocSetContextCreateInternal(parent, name, sizes.0, sizes.1, sizes.2)
}

// ----------------------------------------------------------------------------
// mcxt.c helpers used by AllocSetContextCreate/Delete that are not yet ported
// (the real ones land with mcxt.c). Stubbed locally so this module compiles
// without editing memutils_internal.rs.
// TODO(pg-port): replace with crate::utils::mmgr::mcxt::{MemoryContextStats,
// MemoryContextResetOnly} once mcxt.c is translated.
// ----------------------------------------------------------------------------

// MemoryContextStats / MemoryContextResetOnly are the real mcxt.c routines,
// imported from crate::utils::mmgr::mcxt (see the use block above).

//--------------------
// Chunk freelist k holds chunks of size 1 << (k + ALLOC_MINBITS),
// for k = 0 .. ALLOCSET_NUM_FREELISTS-1.
//
// Note that all chunks in the freelists have power-of-2 sizes.  This
// improves recyclability: we may waste some space, but the wasted space
// should stay pretty constant as requests are made and released.
//
// A request too large for the last freelist is handled by allocating a
// dedicated block from malloc().  The block still has a block header and
// chunk header, but when the chunk is freed we'll return the whole block
// to malloc(), not put it on our freelists.
//
// CAUTION: ALLOC_MINBITS must be large enough so that
// 1<<ALLOC_MINBITS is at least MAXALIGN,
// or we may fail to align the smallest chunks adequately.
// 8-byte alignment is enough on all currently known machines.  This 8-byte
// minimum also allows us to store a pointer to the next freelist item within
// the chunk of memory itself.
//
// With the current parameters, request sizes up to 8K are treated as chunks,
// larger requests go into dedicated blocks.  Change ALLOCSET_NUM_FREELISTS
// to adjust the boundary point; and adjust ALLOCSET_SEPARATE_THRESHOLD in
// memutils.h to agree.  (Note: in contexts with small maxBlockSize, we may
// set the allocChunkLimit to less than 8K, so as to avoid space wastage.)
//--------------------

/// smallest chunk size is 8 bytes
const ALLOC_MINBITS: u32 = 3;
const ALLOCSET_NUM_FREELISTS: usize = 11;
/// `ALLOC_CHUNK_LIMIT` = `1 << (ALLOCSET_NUM_FREELISTS-1+ALLOC_MINBITS)`
const ALLOC_CHUNK_LIMIT: Size = 1 << (ALLOCSET_NUM_FREELISTS as u32 - 1 + ALLOC_MINBITS);
/// Size of largest chunk that we use a fixed size for
const ALLOC_CHUNK_FRACTION: Size = 4;
// We allow chunks to be at most 1/4 of maxBlockSize (less overhead)

//--------------------
// The first block allocated for an allocset has size initBlockSize.
// Each time we have to allocate another block, we double the block size
// (if possible, and without exceeding maxBlockSize), so as to reduce
// the bookkeeping load on malloc().
//
// Blocks allocated to hold oversize chunks do not follow this rule, however;
// they are just however big they need to be to hold that single chunk.
//
// Also, if a minContextSize is specified, the first block has that size,
// and then initBlockSize is used for the next one.
//--------------------

/// `ALLOC_BLOCKHDRSZ` = `MAXALIGN(sizeof(AllocBlockData))`
const ALLOC_BLOCKHDRSZ: Size = MAXALIGN(core::mem::size_of::<AllocBlockData>());
/// `ALLOC_CHUNKHDRSZ` = `sizeof(MemoryChunk)`
const ALLOC_CHUNKHDRSZ: Size = core::mem::size_of::<MemoryChunk>();

/// `typedef struct AllocBlockData *AllocBlock;` (forward reference)
type AllocBlock = *mut AllocBlockData;

/// AllocPointer
///		Aligned pointer which may be a member of an allocation set.
type AllocPointer = *mut c_void;

/// AllocFreeListLink
///		When pfreeing memory, if we maintain a freelist for the given chunk's
///		size then we use a AllocFreeListLink to point to the current item in
///		the AllocSetContext's freelist and then set the given freelist element
///		to point to the chunk being freed.
#[repr(C)]
struct AllocFreeListLink {
    next: *mut MemoryChunk,
}

/// Obtain a AllocFreeListLink for the given chunk.  Allocation sizes are
/// always at least sizeof(AllocFreeListLink), so we reuse the pointer's memory
/// itself to store the freelist link.
///
/// `GetFreeListLink(chkptr) = (AllocFreeListLink *) ((char *) chkptr + ALLOC_CHUNKHDRSZ)`
///
/// # Safety
/// `chkptr` must point to a live MemoryChunk whose payload is at least
/// `sizeof(AllocFreeListLink)` bytes.
#[inline]
unsafe fn GetFreeListLink(chkptr: *mut MemoryChunk) -> *mut AllocFreeListLink {
    (chkptr as *mut u8).add(ALLOC_CHUNKHDRSZ) as *mut AllocFreeListLink
}

/// Validate a freelist index retrieved from a chunk header.
///
/// `FreeListIdxIsValid(fidx)`
#[inline]
fn FreeListIdxIsValid(fidx: c_int) -> bool {
    fidx >= 0 && (fidx as usize) < ALLOCSET_NUM_FREELISTS
}

/// Determine the size of the chunk based on the freelist index.
///
/// `GetChunkSizeFromFreeListIdx(fidx) = (((Size) 1) << ALLOC_MINBITS) << fidx`
#[inline]
fn GetChunkSizeFromFreeListIdx(fidx: c_int) -> Size {
    // Constant base shift; the fidx shift is bounded by ALLOCSET_NUM_FREELISTS.
    ((1 as Size) << ALLOC_MINBITS) << fidx
}

/// AllocSetContext is our standard implementation of MemoryContext.
///
/// Note: header.isReset means there is nothing for AllocSetReset to do.
/// This is different from the aset being physically empty (empty blocks list)
/// because we will still have a keeper block.  It's also different from the set
/// being logically empty, because we don't attempt to detect pfree'ing the
/// last active chunk.
#[repr(C)]
pub struct AllocSetContext {
    /// Standard memory-context fields
    pub header: MemoryContextData,
    // Info about storage allocated in this context:
    /// head of list of blocks in this set
    blocks: AllocBlock,
    /// free chunk lists
    freelist: [*mut MemoryChunk; ALLOCSET_NUM_FREELISTS],
    // Allocation parameters for this context:
    /// initial block size
    initBlockSize: uint32,
    /// maximum block size
    maxBlockSize: uint32,
    /// next block size to allocate
    nextBlockSize: uint32,
    /// effective chunk size limit
    allocChunkLimit: uint32,
    /// index in context_freelists[], or -1
    freeListIndex: c_int,
}

/// `typedef AllocSetContext *AllocSet;`
type AllocSet = *mut AllocSetContext;

/// AllocBlock
///		An AllocBlock is the unit of memory that is obtained by aset.c
///		from malloc().  It contains one or more MemoryChunks, which are
///		the units requested by palloc() and freed by pfree(). MemoryChunks
///		cannot be returned to malloc() individually, instead they are put
///		on freelists by pfree() and re-used by the next palloc() that has
///		a matching request size.
///
///		AllocBlockData is the header data for a block --- the usable space
///		within the block begins at the next alignment boundary.
#[repr(C)]
struct AllocBlockData {
    /// aset that owns this block
    aset: AllocSet,
    /// prev block in aset's blocks list, if any
    prev: AllocBlock,
    /// next block in aset's blocks list, if any
    next: AllocBlock,
    /// start of free space in this block
    freeptr: *mut c_char,
    /// end of space in this block
    endptr: *mut c_char,
}

/// AllocSetIsValid
///		True iff set is valid allocation set.
///
/// `AllocSetIsValid(set) = (PointerIsValid(set) && IsA(set, AllocSetContext))`
///
/// # Safety
/// `set` must be NULL or point to a node beginning with a NodeTag.
#[inline]
unsafe fn AllocSetIsValid(set: AllocSet) -> bool {
    !set.is_null() && IsA!(set, T_AllocSetContext)
}

/// AllocBlockIsValid
///		True iff block is valid block of allocation set.
///
/// `AllocBlockIsValid(block) = (PointerIsValid(block) && AllocSetIsValid((block)->aset))`
///
/// # Safety
/// `block` must be NULL or point to a live AllocBlockData.
#[inline]
unsafe fn AllocBlockIsValid(block: AllocBlock) -> bool {
    !block.is_null() && AllocSetIsValid((*block).aset)
}

/// We always store external chunks on a dedicated block.  This makes fetching
/// the block from an external chunk easy since it's always the first and only
/// chunk on the block.
///
/// `ExternalChunkGetBlock(chunk) = (AllocBlock) ((char *) chunk - ALLOC_BLOCKHDRSZ)`
///
/// # Safety
/// `chunk` must be the (sole) external chunk of a dedicated block.
#[inline]
unsafe fn ExternalChunkGetBlock(chunk: *mut MemoryChunk) -> AllocBlock {
    (chunk as *mut u8).sub(ALLOC_BLOCKHDRSZ) as AllocBlock
}

// Rather than repeatedly creating and deleting memory contexts, we keep some
// freed contexts in freelists so that we can hand them out again with little
// work.  Before putting a context in a freelist, we reset it so that it has
// only its initial malloc chunk and no others.  To be a candidate for a
// freelist, a context must have the same minContextSize/initBlockSize as
// other contexts in the list; but its maxBlockSize is irrelevant since that
// doesn't affect the size of the initial chunk.
//
// We currently provide one freelist for ALLOCSET_DEFAULT_SIZES contexts
// and one for ALLOCSET_SMALL_SIZES contexts; the latter works for
// ALLOCSET_START_SMALL_SIZES too, since only the maxBlockSize differs.
//
// Ordinarily, we re-use freelist contexts in last-in-first-out order, in
// hopes of improving locality of reference.  But if there get to be too
// many contexts in the list, we'd prefer to drop the most-recently-created
// contexts in hopes of keeping the process memory map compact.
// We approximate that by simply deleting all existing entries when the list
// overflows, on the assumption that queries that allocate a lot of contexts
// will probably free them in more or less reverse order of allocation.
//
// Contexts in a freelist are chained via their nextchild pointers.

/// arbitrary limit on freelist length
const MAX_FREE_CONTEXTS: c_int = 100;

/// Obtain the keeper block for an allocation set.
///
/// `KeeperBlock(set) = (AllocBlock) (((char *) set) + MAXALIGN(sizeof(AllocSetContext)))`
///
/// # Safety
/// `set` must point to a live AllocSetContext whose keeper block immediately
/// follows the context header in the same malloc chunk.
#[inline]
unsafe fn KeeperBlock(set: AllocSet) -> AllocBlock {
    (set as *mut u8).add(MAXALIGN(core::mem::size_of::<AllocSetContext>())) as AllocBlock
}

/// Check if the block is the keeper block of the given allocation set.
///
/// `IsKeeperBlock(set, block) = ((block) == (KeeperBlock(set)))`
///
/// # Safety
/// See `KeeperBlock`.
#[inline]
unsafe fn IsKeeperBlock(set: AllocSet, block: AllocBlock) -> bool {
    block == KeeperBlock(set)
}

#[repr(C)]
struct AllocSetFreeList {
    /// current list length
    num_free: c_int,
    /// list header
    first_free: *mut AllocSetContext,
}

/// context_freelists[0] is for default params, [1] for small params.
//
// In C this is a process-global mutable static. The backend is single-threaded
// per process, so accesses are inherently safe; in Rust we keep it as a
// `static mut` and touch it only from this module's (already unsafe) routines.
static mut context_freelists: [AllocSetFreeList; 2] = [
    AllocSetFreeList {
        num_free: 0,
        first_free: core::ptr::null_mut(),
    },
    AllocSetFreeList {
        num_free: 0,
        first_free: core::ptr::null_mut(),
    },
];

// ----------
// AllocSetFreeIndex -
//
//		Depending on the size of an allocation compute which freechunk
//		list of the alloc set it belongs to.  Caller must have verified
//		that size <= ALLOC_CHUNK_LIMIT.
// ----------
#[inline]
fn AllocSetFreeIndex(size: Size) -> c_int {
    let idx: c_int;

    if size > (1 << ALLOC_MINBITS) {
        //----------
        // At this point we must compute ceil(log2(size >> ALLOC_MINBITS)).
        // This is the same as
        //		pg_leftmost_one_pos32((size - 1) >> ALLOC_MINBITS) + 1
        // or equivalently
        //		pg_leftmost_one_pos32(size - 1) - ALLOC_MINBITS + 1
        //
        // However, for platforms without intrinsic support, we duplicate the
        // logic here, allowing an additional optimization.  It's reasonable
        // to assume that ALLOC_CHUNK_LIMIT fits in 16 bits, so we can unroll
        // the byte-at-a-time loop in pg_leftmost_one_pos32 and just handle
        // the last two bytes.
        //
        // Yes, this function is enough of a hot-spot to make it worth this
        // much trouble.
        //----------
        // HAVE_BITSCAN_REVERSE path (the common case on modern hardware);
        // pg_leftmost_one_pos32 here uses the __builtin_clz intrinsic.
        // C arithmetic wraps; the subtraction is well within range for valid
        // inputs but we use wrapping_sub to faithfully match C semantics.
        idx = pg_leftmost_one_pos32((size as uint32).wrapping_sub(1))
            .wrapping_sub(ALLOC_MINBITS as c_int)
            .wrapping_add(1);

        Assert!((idx as usize) < ALLOCSET_NUM_FREELISTS);
    } else {
        idx = 0;
    }

    idx
}

//
// Public routines
//

/// AllocSetContextCreateInternal
///		Create a new AllocSet context.
///
/// parent: parent context, or NULL if top-level context
/// name: name of context (must be statically allocated)
/// minContextSize: minimum context size
/// initBlockSize: initial allocation block size
/// maxBlockSize: maximum allocation block size
///
/// Most callers should abstract the context size parameters using a macro
/// such as ALLOCSET_DEFAULT_SIZES.
///
/// Note: don't call this directly; go through the wrapper macro
/// AllocSetContextCreate.
///
/// # Safety
/// `parent` must be NULL or a valid MemoryContext; `name` must be a valid,
/// statically allocated C string.
pub unsafe fn AllocSetContextCreateInternal(
    parent: MemoryContext,
    name: *const c_char,
    minContextSize: Size,
    initBlockSize: Size,
    maxBlockSize: Size,
) -> MemoryContext {
    let freeListIndex: c_int;
    let mut firstBlockSize: Size;
    let set: AllocSet;
    let block: AllocBlock;

    // ensure MemoryChunk's size is properly maxaligned
    // StaticAssertDecl(ALLOC_CHUNKHDRSZ == MAXALIGN(ALLOC_CHUNKHDRSZ),
    //                  "sizeof(MemoryChunk) is not maxaligned");
    const _: () = assert!(ALLOC_CHUNKHDRSZ == MAXALIGN(ALLOC_CHUNKHDRSZ));
    // check we have enough space to store the freelist link
    // StaticAssertDecl(sizeof(AllocFreeListLink) <= (1 << ALLOC_MINBITS),
    //                  "sizeof(AllocFreeListLink) larger than minimum allocation size");
    const _: () =
        assert!(core::mem::size_of::<AllocFreeListLink>() <= (1usize << ALLOC_MINBITS));

    // First, validate allocation parameters.  Once these were regular runtime
    // tests and elog's, but in practice Asserts seem sufficient because
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
    Assert!(maxBlockSize as uint64 <= MEMORYCHUNK_MAX_BLOCKOFFSET);

    // Check whether the parameters match either available freelist.  We do
    // not need to demand a match of maxBlockSize.
    if minContextSize == ALLOCSET_DEFAULT_MINSIZE && initBlockSize == ALLOCSET_DEFAULT_INITSIZE {
        freeListIndex = 0;
    } else if minContextSize == ALLOCSET_SMALL_MINSIZE && initBlockSize == ALLOCSET_SMALL_INITSIZE {
        freeListIndex = 1;
    } else {
        freeListIndex = -1;
    }

    // If a suitable freelist entry exists, just recycle that context.
    if freeListIndex >= 0 {
        let freelist: *mut AllocSetFreeList = &raw mut context_freelists[freeListIndex as usize];

        if !(*freelist).first_free.is_null() {
            // Remove entry from freelist
            let set = (*freelist).first_free;
            (*freelist).first_free = (*set).header.nextchild as AllocSet;
            (*freelist).num_free -= 1;

            // Update its maxBlockSize; everything else should be OK
            (*set).maxBlockSize = maxBlockSize as uint32;

            // Reinitialize its header, installing correct name and parent
            MemoryContextCreate(
                set as MemoryContext,
                NodeTag::T_AllocSetContext,
                MCTX_ASET_ID,
                parent,
                name,
            );

            (*(set as MemoryContext)).mem_allocated = ((*KeeperBlock(set)).endptr as usize)
                .wrapping_sub(set as *mut u8 as usize)
                as Size;

            return set as MemoryContext;
        }
    }

    // Determine size of initial block
    firstBlockSize = MAXALIGN(core::mem::size_of::<AllocSetContext>())
        + ALLOC_BLOCKHDRSZ
        + ALLOC_CHUNKHDRSZ;
    if minContextSize != 0 {
        firstBlockSize = Max(firstBlockSize, minContextSize);
    } else {
        firstBlockSize = Max(firstBlockSize, initBlockSize);
    }

    // Allocate the initial block.  Unlike other aset.c blocks, it starts with
    // the context header and its block header follows that.
    set = malloc(firstBlockSize) as AllocSet;
    if set.is_null() {
        if !crate::utils::palloc::TopMemoryContext.is_null() {
            // NOTE: TopMemoryContext is the bootstrap context for now; the real
            // one arrives with mcxt.c. Stats printing is itself a TODO stub.
            // TODO(pg-port): MemoryContextStats(TopMemoryContext) once mcxt lands.
            MemoryContextStats(
                crate::utils::palloc::TopMemoryContext as *mut c_void as MemoryContext,
            );
        }
        // ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY),
        //                 errmsg("out of memory"),
        //                 errdetail("Failed while creating memory context \"%s\".", name)));
        // TODO(pg-port): real ereport; for now this is a hard failure.
        panic!("out of memory: failed while creating memory context");
    }

    // Avoid writing code that can fail between here and MemoryContextCreate;
    // we'd leak the header/initial block if we ereport in this stretch.

    // Fill in the initial block's block header
    block = KeeperBlock(set);
    (*block).aset = set;
    (*block).freeptr = (block as *mut u8).add(ALLOC_BLOCKHDRSZ) as *mut c_char;
    (*block).endptr = (set as *mut u8).add(firstBlockSize) as *mut c_char;
    (*block).prev = core::ptr::null_mut();
    (*block).next = core::ptr::null_mut();

    // Mark unallocated space NOACCESS; leave the block header alone.
    // VALGRIND_MAKE_MEM_NOACCESS(block->freeptr, block->endptr - block->freeptr);
    // TODO(pg-port): no-op (valgrind not modeled).

    // Remember block as part of block list
    (*set).blocks = block;

    // Finish filling in aset-specific parts of the context header
    // MemSetAligned(set->freelist, 0, sizeof(set->freelist));
    core::ptr::write_bytes(
        (*set).freelist.as_mut_ptr(),
        0,
        ALLOCSET_NUM_FREELISTS,
    );

    (*set).initBlockSize = initBlockSize as uint32;
    (*set).maxBlockSize = maxBlockSize as uint32;
    (*set).nextBlockSize = initBlockSize as uint32;
    (*set).freeListIndex = freeListIndex;

    // Compute the allocation chunk size limit for this context.  It can't be
    // more than ALLOC_CHUNK_LIMIT because of the fixed number of freelists.
    // If maxBlockSize is small then requests exceeding the maxBlockSize, or
    // even a significant fraction of it, should be treated as large chunks
    // too.  For the typical case of maxBlockSize a power of 2, the chunk size
    // limit will be at most 1/8th maxBlockSize, so that given a stream of
    // requests that are all the maximum chunk size we will waste at most
    // 1/8th of the allocated space.
    //
    // Also, allocChunkLimit must not exceed ALLOCSET_SEPARATE_THRESHOLD.
    // StaticAssertStmt(ALLOC_CHUNK_LIMIT == ALLOCSET_SEPARATE_THRESHOLD,
    //                  "ALLOC_CHUNK_LIMIT != ALLOCSET_SEPARATE_THRESHOLD");
    const _: () = assert!(ALLOC_CHUNK_LIMIT == ALLOCSET_SEPARATE_THRESHOLD);

    // Determine the maximum size that a chunk can be before we allocate an
    // entire AllocBlock dedicated for that chunk.  We set the absolute limit
    // of that size as ALLOC_CHUNK_LIMIT but we reduce it further so that we
    // can fit about ALLOC_CHUNK_FRACTION chunks this size on a maximally
    // sized block.  (We opt to keep allocChunkLimit a power-of-2 value
    // primarily for legacy reasons rather than calculating it so that exactly
    // ALLOC_CHUNK_FRACTION chunks fit on a maximally sized block.)
    (*set).allocChunkLimit = ALLOC_CHUNK_LIMIT as uint32;
    while ((*set).allocChunkLimit as Size + ALLOC_CHUNKHDRSZ)
        > ((maxBlockSize - ALLOC_BLOCKHDRSZ) / ALLOC_CHUNK_FRACTION)
    {
        (*set).allocChunkLimit >>= 1;
    }

    // Finally, do the type-independent part of context creation
    MemoryContextCreate(
        set as MemoryContext,
        NodeTag::T_AllocSetContext,
        MCTX_ASET_ID,
        parent,
        name,
    );

    (*(set as MemoryContext)).mem_allocated = firstBlockSize;

    set as MemoryContext
}

/// AllocSetReset
///		Frees all memory which is allocated in the given set.
///
/// Actually, this routine has some discretion about what to do.
/// It should mark all allocated chunks freed, but it need not necessarily
/// give back all the resources the set owns.  Our actual implementation is
/// that we give back all but the "keeper" block (which we must keep, since
/// it shares a malloc chunk with the context header).  In this way, we don't
/// thrash malloc() when a context is repeatedly reset after small allocations,
/// which is typical behavior for per-tuple contexts.
///
/// # Safety
/// `context` must be a valid AllocSet context.
pub unsafe fn AllocSetReset(context: MemoryContext) {
    let set: AllocSet = context as AllocSet;
    let mut block: AllocBlock;
    // Size keepersize PG_USED_FOR_ASSERTS_ONLY;
    let keepersize: Size;

    Assert!(AllocSetIsValid(set));

    // #ifdef MEMORY_CONTEXT_CHECKING
    //     /* Check for corruption and leaks before freeing */
    //     AllocSetCheck(context);
    // #endif
    // TODO(pg-port): AllocSetCheck under MEMORY_CONTEXT_CHECKING.

    // Remember keeper block size for Assert below
    keepersize = ((*KeeperBlock(set)).endptr as usize).wrapping_sub(set as *mut u8 as usize)
        as Size;

    // Clear chunk freelists
    // MemSetAligned(set->freelist, 0, sizeof(set->freelist));
    core::ptr::write_bytes((*set).freelist.as_mut_ptr(), 0, ALLOCSET_NUM_FREELISTS);

    block = (*set).blocks;

    // New blocks list will be just the keeper block
    (*set).blocks = KeeperBlock(set);

    while !block.is_null() {
        let next: AllocBlock = (*block).next;

        if IsKeeperBlock(set, block) {
            // Reset the block, but don't return it to malloc
            let datastart: *mut c_char = (block as *mut u8).add(ALLOC_BLOCKHDRSZ) as *mut c_char;

            // #ifdef CLOBBER_FREED_MEMORY
            //     wipe_mem(datastart, block->freeptr - datastart);
            // #else
            //     /* wipe_mem() would have done this */
            //     VALGRIND_MAKE_MEM_NOACCESS(datastart, block->freeptr - datastart);
            // #endif
            // TODO(pg-port): CLOBBER_FREED_MEMORY/valgrind no-op (default-off path).
            (*block).freeptr = datastart;
            (*block).prev = core::ptr::null_mut();
            (*block).next = core::ptr::null_mut();
        } else {
            // Normal case, release the block
            (*context).mem_allocated -= ((*block).endptr as usize)
                .wrapping_sub(block as *mut u8 as usize)
                as Size;

            // #ifdef CLOBBER_FREED_MEMORY
            //     wipe_mem(block, block->freeptr - ((char *) block));
            // #endif
            // TODO(pg-port): CLOBBER_FREED_MEMORY no-op (default-off path).
            free(block as *mut c_void);
        }
        block = next;
    }

    Assert!((*context).mem_allocated == keepersize);

    // Reset block size allocation sequence, too
    (*set).nextBlockSize = (*set).initBlockSize;
}

/// AllocSetDelete
///		Frees all memory which is allocated in the given set,
///		in preparation for deletion of the set.
///
/// Unlike AllocSetReset, this *must* free all resources of the set.
///
/// # Safety
/// `context` must be a valid AllocSet context.
pub unsafe fn AllocSetDelete(context: MemoryContext) {
    let set: AllocSet = context as AllocSet;
    let mut block: AllocBlock = (*set).blocks;
    // Size keepersize PG_USED_FOR_ASSERTS_ONLY;
    let keepersize: Size;

    Assert!(AllocSetIsValid(set));

    // #ifdef MEMORY_CONTEXT_CHECKING
    //     /* Check for corruption and leaks before freeing */
    //     AllocSetCheck(context);
    // #endif
    // TODO(pg-port): AllocSetCheck under MEMORY_CONTEXT_CHECKING.

    // Remember keeper block size for Assert below
    keepersize = ((*KeeperBlock(set)).endptr as usize).wrapping_sub(set as *mut u8 as usize)
        as Size;

    // If the context is a candidate for a freelist, put it into that freelist
    // instead of destroying it.
    if (*set).freeListIndex >= 0 {
        let freelist: *mut AllocSetFreeList =
            &raw mut context_freelists[(*set).freeListIndex as usize];

        // Reset the context, if it needs it, so that we aren't hanging on to
        // more than the initial malloc chunk.
        if !(*context).isReset {
            MemoryContextResetOnly(context);
        }

        // If the freelist is full, just discard what's already in it.  See
        // comments with context_freelists[].
        if (*freelist).num_free >= MAX_FREE_CONTEXTS {
            while !(*freelist).first_free.is_null() {
                let oldset: *mut AllocSetContext = (*freelist).first_free;

                (*freelist).first_free = (*oldset).header.nextchild as *mut AllocSetContext;
                (*freelist).num_free -= 1;

                // All that remains is to free the header/initial block
                free(oldset as *mut c_void);
            }
            Assert!((*freelist).num_free == 0);
        }

        // Now add the just-deleted context to the freelist.
        (*set).header.nextchild = (*freelist).first_free as MemoryContext;
        (*freelist).first_free = set;
        (*freelist).num_free += 1;

        return;
    }

    // Free all blocks, except the keeper which is part of context header
    while !block.is_null() {
        let next: AllocBlock = (*block).next;

        if !IsKeeperBlock(set, block) {
            (*context).mem_allocated -= ((*block).endptr as usize)
                .wrapping_sub(block as *mut u8 as usize)
                as Size;
        }

        // #ifdef CLOBBER_FREED_MEMORY
        //     wipe_mem(block, block->freeptr - ((char *) block));
        // #endif
        // TODO(pg-port): CLOBBER_FREED_MEMORY no-op (default-off path).

        if !IsKeeperBlock(set, block) {
            free(block as *mut c_void);
        }

        block = next;
    }

    Assert!((*context).mem_allocated == keepersize);

    // Finally, free the context header, including the keeper block
    free(set as *mut c_void);
}

/// Helper for AllocSetAlloc() that allocates an entire block for the chunk.
///
/// AllocSetAlloc()'s comment explains why this is separate.
///
/// # Safety
/// `context` must be a valid AllocSet context.
// pg_noinline
#[inline(never)]
unsafe fn AllocSetAllocLarge(context: MemoryContext, size: Size, flags: c_int) -> *mut c_void {
    let set: AllocSet = context as AllocSet;
    let block: AllocBlock;
    let chunk: *mut MemoryChunk;
    let chunk_size: Size;
    let blksize: Size;

    // validate 'size' is within the limits for the given 'flags'
    MemoryContextCheckSize(context, size, flags);

    // #ifdef MEMORY_CONTEXT_CHECKING
    //     /* ensure there's always space for the sentinel byte */
    //     chunk_size = MAXALIGN(size + 1);
    // #else
    chunk_size = MAXALIGN(size);
    // #endif

    blksize = chunk_size + ALLOC_BLOCKHDRSZ + ALLOC_CHUNKHDRSZ;
    block = malloc(blksize) as AllocBlock;
    if block.is_null() {
        return MemoryContextAllocationFailure(context, size, flags);
    }

    (*context).mem_allocated += blksize;

    (*block).aset = set;
    (*block).endptr = (block as *mut u8).add(blksize) as *mut c_char;
    (*block).freeptr = (*block).endptr;

    chunk = (block as *mut u8).add(ALLOC_BLOCKHDRSZ) as *mut MemoryChunk;

    // mark the MemoryChunk as externally managed
    MemoryChunkSetHdrMaskExternal(chunk, MCTX_ASET_ID);

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

    // Stick the new block underneath the active allocation block, if any, so
    // that we don't lose the use of the space remaining therein.
    if !(*set).blocks.is_null() {
        (*block).prev = (*set).blocks;
        (*block).next = (*(*set).blocks).next;
        if !(*block).next.is_null() {
            (*(*block).next).prev = block;
        }
        (*(*set).blocks).next = block;
    } else {
        (*block).prev = core::ptr::null_mut();
        (*block).next = core::ptr::null_mut();
        (*set).blocks = block;
    }

    // Ensure any padding bytes are marked NOACCESS.
    // VALGRIND_MAKE_MEM_NOACCESS((char *) MemoryChunkGetPointer(chunk) + size, chunk_size - size);
    // Disallow access to the chunk header.
    // VALGRIND_MAKE_MEM_NOACCESS(chunk, ALLOC_CHUNKHDRSZ);
    // TODO(pg-port): valgrind no-op.

    MemoryChunkGetPointer(chunk)
}

/// Small helper for allocating a new chunk from a chunk, to avoid duplicating
/// the code between AllocSetAlloc() and AllocSetAllocFromNewBlock().
///
/// # Safety
/// `block` must be a live block of `context` with room for the chunk.
#[inline]
unsafe fn AllocSetAllocChunkFromBlock(
    _context: MemoryContext,
    block: AllocBlock,
    size: Size,
    chunk_size: Size,
    fidx: c_int,
) -> *mut c_void {
    let chunk: *mut MemoryChunk;

    chunk = (*block).freeptr as *mut MemoryChunk;

    // Prepare to initialize the chunk header.
    // VALGRIND_MAKE_MEM_UNDEFINED(chunk, ALLOC_CHUNKHDRSZ);
    // TODO(pg-port): valgrind no-op.

    (*block).freeptr = ((*block).freeptr as *mut u8).add(chunk_size + ALLOC_CHUNKHDRSZ)
        as *mut c_char;
    Assert!((*block).freeptr <= (*block).endptr);

    // store the free list index in the value field
    MemoryChunkSetHdrMask(chunk, block as *mut c_void, fidx as Size, MCTX_ASET_ID);

    // #ifdef MEMORY_CONTEXT_CHECKING
    //     chunk->requested_size = size;
    //     /* set mark to catch clobber of "unused" space */
    //     if (size < chunk_size)
    //         set_sentinel(MemoryChunkGetPointer(chunk), size);
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
    // VALGRIND_MAKE_MEM_NOACCESS(chunk, ALLOC_CHUNKHDRSZ);
    // TODO(pg-port): valgrind no-op.

    MemoryChunkGetPointer(chunk)
}

/// Helper for AllocSetAlloc() that allocates a new block and returns a chunk
/// allocated from it.
///
/// AllocSetAlloc()'s comment explains why this is separate.
///
/// # Safety
/// `context` must be a valid AllocSet context.
// pg_noinline
#[inline(never)]
unsafe fn AllocSetAllocFromNewBlock(
    context: MemoryContext,
    size: Size,
    flags: c_int,
    fidx: c_int,
) -> *mut c_void {
    let set: AllocSet = context as AllocSet;
    let mut block: AllocBlock;
    let mut availspace: Size;
    let mut blksize: Size;
    let required_size: Size;
    let chunk_size: Size;

    // due to the keeper block set->blocks should always be valid
    Assert!(!(*set).blocks.is_null());
    block = (*set).blocks;
    availspace = ((*block).endptr as usize).wrapping_sub((*block).freeptr as usize) as Size;

    // The existing active (top) block does not have enough room for the
    // requested allocation, but it might still have a useful amount of space
    // in it.  Once we push it down in the block list, we'll never try to
    // allocate more space from it. So, before we do that, carve up its free
    // space into chunks that we can put on the set's freelists.
    //
    // Because we can only get here when there's less than ALLOC_CHUNK_LIMIT
    // left in the block, this loop cannot iterate more than
    // ALLOCSET_NUM_FREELISTS-1 times.
    while availspace >= ((1 << ALLOC_MINBITS) + ALLOC_CHUNKHDRSZ) {
        let link: *mut AllocFreeListLink;
        let chunk: *mut MemoryChunk;
        let mut availchunk: Size = availspace - ALLOC_CHUNKHDRSZ;
        let mut a_fidx: c_int = AllocSetFreeIndex(availchunk);

        // In most cases, we'll get back the index of the next larger freelist
        // than the one we need to put this chunk on.  The exception is when
        // availchunk is exactly a power of 2.
        if availchunk != GetChunkSizeFromFreeListIdx(a_fidx) {
            a_fidx -= 1;
            Assert!(a_fidx >= 0);
            availchunk = GetChunkSizeFromFreeListIdx(a_fidx);
        }

        chunk = (*block).freeptr as *mut MemoryChunk;

        // Prepare to initialize the chunk header.
        // VALGRIND_MAKE_MEM_UNDEFINED(chunk, ALLOC_CHUNKHDRSZ);
        // TODO(pg-port): valgrind no-op.
        (*block).freeptr = ((*block).freeptr as *mut u8).add(availchunk + ALLOC_CHUNKHDRSZ)
            as *mut c_char;
        availspace -= availchunk + ALLOC_CHUNKHDRSZ;

        // store the freelist index in the value field
        MemoryChunkSetHdrMask(chunk, block as *mut c_void, a_fidx as Size, MCTX_ASET_ID);
        // #ifdef MEMORY_CONTEXT_CHECKING
        //     chunk->requested_size = InvalidAllocSize;	/* mark it free */
        // #endif
        // TODO(pg-port): MEMORY_CONTEXT_CHECKING (default-off).

        // push this chunk onto the free list
        link = GetFreeListLink(chunk);

        // VALGRIND_MAKE_MEM_DEFINED(link, sizeof(AllocFreeListLink));
        (*link).next = (*set).freelist[a_fidx as usize];
        // VALGRIND_MAKE_MEM_NOACCESS(link, sizeof(AllocFreeListLink));
        // TODO(pg-port): valgrind no-op.

        (*set).freelist[a_fidx as usize] = chunk;
    }

    // The first such block has size initBlockSize, and we double the space in
    // each succeeding block, but not more than maxBlockSize.
    blksize = (*set).nextBlockSize as Size;
    (*set).nextBlockSize <<= 1;
    if (*set).nextBlockSize > (*set).maxBlockSize {
        (*set).nextBlockSize = (*set).maxBlockSize;
    }

    // Choose the actual chunk size to allocate
    chunk_size = GetChunkSizeFromFreeListIdx(fidx);
    Assert!(chunk_size >= size);

    // If initBlockSize is less than ALLOC_CHUNK_LIMIT, we could need more
    // space... but try to keep it a power of 2.
    required_size = chunk_size + ALLOC_BLOCKHDRSZ + ALLOC_CHUNKHDRSZ;
    while blksize < required_size {
        blksize <<= 1;
    }

    // Try to allocate it
    block = malloc(blksize) as AllocBlock;

    // We could be asking for pretty big blocks here, so cope if malloc fails.
    // But give up if there's less than 1 MB or so available...
    while block.is_null() && blksize > 1024 * 1024 {
        blksize >>= 1;
        if blksize < required_size {
            break;
        }
        block = malloc(blksize) as AllocBlock;
    }

    if block.is_null() {
        return MemoryContextAllocationFailure(context, size, flags);
    }

    (*context).mem_allocated += blksize;

    (*block).aset = set;
    (*block).freeptr = (block as *mut u8).add(ALLOC_BLOCKHDRSZ) as *mut c_char;
    (*block).endptr = (block as *mut u8).add(blksize) as *mut c_char;

    // Mark unallocated space NOACCESS.
    // VALGRIND_MAKE_MEM_NOACCESS(block->freeptr, blksize - ALLOC_BLOCKHDRSZ);
    // TODO(pg-port): valgrind no-op.

    (*block).prev = core::ptr::null_mut();
    (*block).next = (*set).blocks;
    if !(*block).next.is_null() {
        (*(*block).next).prev = block;
    }
    (*set).blocks = block;

    AllocSetAllocChunkFromBlock(context, block, size, chunk_size, fidx)
}

/// AllocSetAlloc
///		Returns a pointer to allocated memory of given size or raises an ERROR
///		on allocation failure, or returns NULL when flags contains
///		MCXT_ALLOC_NO_OOM.
///
/// No request may exceed:
///		MAXALIGN_DOWN(SIZE_MAX) - ALLOC_BLOCKHDRSZ - ALLOC_CHUNKHDRSZ
/// All callers use a much-lower limit.
///
/// Note: when using valgrind, it doesn't matter how the returned allocation
/// is marked, as mcxt.c will set it to UNDEFINED.  In some paths we will
/// return space that is marked NOACCESS - AllocSetRealloc has to beware!
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
/// `context` must be a valid AllocSet context.
pub unsafe fn AllocSetAlloc(context: MemoryContext, size: Size, flags: c_int) -> *mut c_void {
    let set: AllocSet = context as AllocSet;
    let block: AllocBlock;
    let chunk: *mut MemoryChunk;
    let fidx: c_int;
    let chunk_size: Size;
    let availspace: Size;

    Assert!(AllocSetIsValid(set));

    // due to the keeper block set->blocks should never be NULL
    Assert!(!(*set).blocks.is_null());

    // If requested size exceeds maximum for chunks we hand the request off to
    // AllocSetAllocLarge().
    if size > (*set).allocChunkLimit as Size {
        return AllocSetAllocLarge(context, size, flags);
    }

    // Request is small enough to be treated as a chunk.  Look in the
    // corresponding free list to see if there is a free chunk we could reuse.
    // If one is found, remove it from the free list, make it again a member
    // of the alloc set and return its data address.
    //
    // Note that we don't attempt to ensure there's space for the sentinel
    // byte here.  We expect a large proportion of allocations to be for sizes
    // which are already a power of 2.  If we were to always make space for a
    // sentinel byte in MEMORY_CONTEXT_CHECKING builds, then we'd end up
    // doubling the memory requirements for such allocations.
    fidx = AllocSetFreeIndex(size);
    chunk = (*set).freelist[fidx as usize];
    if !chunk.is_null() {
        let link: *mut AllocFreeListLink = GetFreeListLink(chunk);

        // Allow access to the chunk header.
        // VALGRIND_MAKE_MEM_DEFINED(chunk, ALLOC_CHUNKHDRSZ);
        // TODO(pg-port): valgrind no-op.

        Assert!(fidx as Size == MemoryChunkGetValue(chunk));

        // pop this chunk off the freelist
        // VALGRIND_MAKE_MEM_DEFINED(link, sizeof(AllocFreeListLink));
        (*set).freelist[fidx as usize] = (*link).next;
        // VALGRIND_MAKE_MEM_NOACCESS(link, sizeof(AllocFreeListLink));
        // TODO(pg-port): valgrind no-op.

        // #ifdef MEMORY_CONTEXT_CHECKING
        //     chunk->requested_size = size;
        //     if (size < GetChunkSizeFromFreeListIdx(fidx))
        //         set_sentinel(MemoryChunkGetPointer(chunk), size);
        // #endif
        // #ifdef RANDOMIZE_ALLOCATED_MEMORY
        //     randomize_mem((char *) MemoryChunkGetPointer(chunk), size);
        // #endif
        // TODO(pg-port): MEMORY_CONTEXT_CHECKING / RANDOMIZE_ALLOCATED_MEMORY (default-off).

        // Ensure any padding bytes are marked NOACCESS.
        // VALGRIND_MAKE_MEM_NOACCESS((char *) MemoryChunkGetPointer(chunk) + size,
        //                            GetChunkSizeFromFreeListIdx(fidx) - size);
        // Disallow access to the chunk header.
        // VALGRIND_MAKE_MEM_NOACCESS(chunk, ALLOC_CHUNKHDRSZ);
        // TODO(pg-port): valgrind no-op.

        return MemoryChunkGetPointer(chunk);
    }

    // Choose the actual chunk size to allocate.
    chunk_size = GetChunkSizeFromFreeListIdx(fidx);
    Assert!(chunk_size >= size);

    block = (*set).blocks;
    availspace = ((*block).endptr as usize).wrapping_sub((*block).freeptr as usize) as Size;

    // If there is enough room in the active allocation block, we will put the
    // chunk into that block.  Else must start a new one.
    if unlikely(availspace < (chunk_size + ALLOC_CHUNKHDRSZ)) {
        return AllocSetAllocFromNewBlock(context, size, flags, fidx);
    }

    // There's enough space on the current block, so allocate from that
    AllocSetAllocChunkFromBlock(context, block, size, chunk_size, fidx)
}

/// AllocSetFree
///		Frees allocated memory; memory is removed from the set.
///
/// # Safety
/// `pointer` must be a live allocation from an AllocSet context.
pub unsafe fn AllocSetFree(pointer: *mut c_void) {
    let set: AllocSet;
    let chunk: *mut MemoryChunk = PointerGetMemoryChunk(pointer);

    // Allow access to the chunk header.
    // VALGRIND_MAKE_MEM_DEFINED(chunk, ALLOC_CHUNKHDRSZ);
    // TODO(pg-port): valgrind no-op.

    if MemoryChunkIsExternal(chunk) {
        // Release single-chunk block.
        let block: AllocBlock = ExternalChunkGetBlock(chunk);

        // Try to verify that we have a sane block pointer: the block header
        // should reference an aset and the freeptr should match the endptr.
        if !AllocBlockIsValid(block) || (*block).freeptr != (*block).endptr {
            // elog(ERROR, "could not find block containing chunk %p", chunk);
            panic!("could not find block containing chunk {:p}", chunk);
        }

        set = (*block).aset;

        // #ifdef MEMORY_CONTEXT_CHECKING
        //     /* Test for someone scribbling on unused space in chunk */
        //     Assert(chunk->requested_size < (block->endptr - (char *) pointer));
        //     if (!sentinel_ok(pointer, chunk->requested_size))
        //         elog(WARNING, "detected write past chunk end in %s %p", set->header.name, chunk);
        // #endif
        // TODO(pg-port): MEMORY_CONTEXT_CHECKING (default-off).

        // OK, remove block from aset's list and free it
        if !(*block).prev.is_null() {
            (*(*block).prev).next = (*block).next;
        } else {
            (*set).blocks = (*block).next;
        }
        if !(*block).next.is_null() {
            (*(*block).next).prev = (*block).prev;
        }

        (*set).header.mem_allocated -= ((*block).endptr as usize)
            .wrapping_sub(block as *mut u8 as usize)
            as Size;

        // #ifdef CLOBBER_FREED_MEMORY
        //     wipe_mem(block, block->freeptr - ((char *) block));
        // #endif
        // TODO(pg-port): CLOBBER_FREED_MEMORY no-op (default-off path).
        free(block as *mut c_void);
    } else {
        let block: AllocBlock = MemoryChunkGetBlock(chunk) as AllocBlock;
        let fidx: c_int;
        let link: *mut AllocFreeListLink;

        // In this path, for speed reasons we just Assert that the referenced
        // block is good.  We can also Assert that the value field is sane.
        // Future field experience may show that these Asserts had better
        // become regular runtime test-and-elog checks.
        Assert!(AllocBlockIsValid(block));
        set = (*block).aset;

        fidx = MemoryChunkGetValue(chunk) as c_int;
        Assert!(FreeListIdxIsValid(fidx));
        link = GetFreeListLink(chunk);

        // #ifdef MEMORY_CONTEXT_CHECKING
        //     /* Test for someone scribbling on unused space in chunk */
        //     if (chunk->requested_size < GetChunkSizeFromFreeListIdx(fidx))
        //         if (!sentinel_ok(pointer, chunk->requested_size))
        //             elog(WARNING, "detected write past chunk end in %s %p", set->header.name, chunk);
        // #endif
        // TODO(pg-port): MEMORY_CONTEXT_CHECKING (default-off).

        // #ifdef CLOBBER_FREED_MEMORY
        //     wipe_mem(pointer, GetChunkSizeFromFreeListIdx(fidx));
        // #endif
        // TODO(pg-port): CLOBBER_FREED_MEMORY no-op (default-off path).

        // push this chunk onto the top of the free list
        // VALGRIND_MAKE_MEM_DEFINED(link, sizeof(AllocFreeListLink));
        (*link).next = (*set).freelist[fidx as usize];
        // VALGRIND_MAKE_MEM_NOACCESS(link, sizeof(AllocFreeListLink));
        // TODO(pg-port): valgrind no-op.
        (*set).freelist[fidx as usize] = chunk;

        // #ifdef MEMORY_CONTEXT_CHECKING
        //     /* Reset requested_size to InvalidAllocSize in chunks that are on free list. */
        //     chunk->requested_size = InvalidAllocSize;
        // #endif
        // TODO(pg-port): MEMORY_CONTEXT_CHECKING (default-off).
    }
}

/// AllocSetRealloc
///		Returns new pointer to allocated memory of given size or NULL if
///		request could not be completed; this memory is added to the set.
///		Memory associated with given pointer is copied into the new memory,
///		and the old memory is freed.
///
/// Without MEMORY_CONTEXT_CHECKING, we don't know the old request size.  This
/// makes our Valgrind client requests less-precise, hazarding false negatives.
/// (In principle, we could use VALGRIND_GET_VBITS() to rediscover the old
/// request size.)
///
/// # Safety
/// `pointer` must be a live allocation from an AllocSet context.
pub unsafe fn AllocSetRealloc(pointer: *mut c_void, size: Size, flags: c_int) -> *mut c_void {
    let mut block: AllocBlock;
    let set: AllocSet;
    let mut chunk: *mut MemoryChunk = PointerGetMemoryChunk(pointer);
    let oldchksize: Size;
    let fidx: c_int;

    // Allow access to the chunk header.
    // VALGRIND_MAKE_MEM_DEFINED(chunk, ALLOC_CHUNKHDRSZ);
    // TODO(pg-port): valgrind no-op.

    if MemoryChunkIsExternal(chunk) {
        // The chunk must have been allocated as a single-chunk block.  Use
        // realloc() to make the containing block bigger, or smaller, with
        // minimum space wastage.
        let chksize: Size;
        let blksize: Size;
        let oldblksize: Size;
        // mutable local rebound for the pointer (C reassigns `pointer`)
        let mut pointer = pointer;

        block = ExternalChunkGetBlock(chunk);

        // Try to verify that we have a sane block pointer: the block header
        // should reference an aset and the freeptr should match the endptr.
        if !AllocBlockIsValid(block) || (*block).freeptr != (*block).endptr {
            // elog(ERROR, "could not find block containing chunk %p", chunk);
            panic!("could not find block containing chunk {:p}", chunk);
        }

        set = (*block).aset;

        // only check size in paths where the limits could be hit
        MemoryContextCheckSize(set as MemoryContext, size, flags);

        oldchksize = ((*block).endptr as usize).wrapping_sub(pointer as usize) as Size;

        // #ifdef MEMORY_CONTEXT_CHECKING
        //     /* Test for someone scribbling on unused space in chunk */
        //     Assert(chunk->requested_size < oldchksize);
        //     if (!sentinel_ok(pointer, chunk->requested_size))
        //         elog(WARNING, "detected write past chunk end in %s %p", set->header.name, chunk);
        // #endif
        // TODO(pg-port): MEMORY_CONTEXT_CHECKING (default-off).

        // #ifdef MEMORY_CONTEXT_CHECKING
        //     /* ensure there's always space for the sentinel byte */
        //     chksize = MAXALIGN(size + 1);
        // #else
        chksize = MAXALIGN(size);
        // #endif

        // Do the realloc
        blksize = chksize + ALLOC_BLOCKHDRSZ + ALLOC_CHUNKHDRSZ;
        oldblksize = ((*block).endptr as usize).wrapping_sub(block as *mut u8 as usize) as Size;

        block = realloc(block as *mut c_void, blksize) as AllocBlock;
        if block.is_null() {
            // Disallow access to the chunk header.
            // VALGRIND_MAKE_MEM_NOACCESS(chunk, ALLOC_CHUNKHDRSZ);
            // TODO(pg-port): valgrind no-op.
            return MemoryContextAllocationFailure(&mut (*set).header, size, flags);
        }

        // updated separately, not to underflow when (oldblksize > blksize)
        (*set).header.mem_allocated -= oldblksize;
        (*set).header.mem_allocated += blksize;

        (*block).endptr = (block as *mut u8).add(blksize) as *mut c_char;
        (*block).freeptr = (*block).endptr;

        // Update pointers since block has likely been moved
        chunk = (block as *mut u8).add(ALLOC_BLOCKHDRSZ) as *mut MemoryChunk;
        pointer = MemoryChunkGetPointer(chunk);
        if !(*block).prev.is_null() {
            (*(*block).prev).next = block;
        } else {
            (*set).blocks = block;
        }
        if !(*block).next.is_null() {
            (*(*block).next).prev = block;
        }

        // #ifdef MEMORY_CONTEXT_CHECKING
        // #ifdef RANDOMIZE_ALLOCATED_MEMORY
        //     if (size > chunk->requested_size)
        //         randomize_mem(...);
        // #else
        // #ifdef USE_VALGRIND
        //     if (Min(size, oldchksize) > chunk->requested_size)
        //         VALGRIND_MAKE_MEM_UNDEFINED(...);
        // #endif
        // #endif
        //     chunk->requested_size = size;
        //     Assert(size < chksize);
        //     set_sentinel(pointer, size);
        // #else							/* !MEMORY_CONTEXT_CHECKING */
        //     /*
        //      * We may need to adjust marking of bytes from the old allocation as
        //      * some of them may be marked NOACCESS.  ...
        //      */
        //     VALGRIND_MAKE_MEM_DEFINED(pointer, Min(size, oldchksize));
        // #endif
        // TODO(pg-port): MEMORY_CONTEXT_CHECKING / valgrind (default-off, no-op).
        let _ = Min(size, oldchksize);

        // Ensure any padding bytes are marked NOACCESS.
        // VALGRIND_MAKE_MEM_NOACCESS((char *) pointer + size, chksize - size);
        // Disallow access to the chunk header.
        // VALGRIND_MAKE_MEM_NOACCESS(chunk, ALLOC_CHUNKHDRSZ);
        // TODO(pg-port): valgrind no-op.

        return pointer;
    }

    block = MemoryChunkGetBlock(chunk) as AllocBlock;

    // In this path, for speed reasons we just Assert that the referenced
    // block is good. We can also Assert that the value field is sane. Future
    // field experience may show that these Asserts had better become regular
    // runtime test-and-elog checks.
    Assert!(AllocBlockIsValid(block));
    set = (*block).aset;

    fidx = MemoryChunkGetValue(chunk) as c_int;
    Assert!(FreeListIdxIsValid(fidx));
    oldchksize = GetChunkSizeFromFreeListIdx(fidx);

    // #ifdef MEMORY_CONTEXT_CHECKING
    //     /* Test for someone scribbling on unused space in chunk */
    //     if (chunk->requested_size < oldchksize)
    //         if (!sentinel_ok(pointer, chunk->requested_size))
    //             elog(WARNING, "detected write past chunk end in %s %p", set->header.name, chunk);
    // #endif
    // TODO(pg-port): MEMORY_CONTEXT_CHECKING (default-off).

    // Chunk sizes are aligned to power of 2 in AllocSetAlloc().  Maybe the
    // allocated area already is >= the new size.  (In particular, we will
    // fall out here if the requested size is a decrease.)
    if oldchksize >= size {
        // #ifdef MEMORY_CONTEXT_CHECKING
        //     Size oldrequest = chunk->requested_size;
        // #ifdef RANDOMIZE_ALLOCATED_MEMORY
        //     if (size > oldrequest) randomize_mem(...);
        // #endif
        //     chunk->requested_size = size;
        //     if (size > oldrequest) VALGRIND_MAKE_MEM_UNDEFINED(...);
        //     else VALGRIND_MAKE_MEM_NOACCESS(...);
        //     if (size < oldchksize) set_sentinel(pointer, size);
        // #else							/* !MEMORY_CONTEXT_CHECKING */
        //     /*
        //      * We don't have the information to determine whether we're growing
        //      * the old request or shrinking it, so we conservatively mark the
        //      * entire new allocation DEFINED.
        //      */
        //     VALGRIND_MAKE_MEM_NOACCESS(pointer, oldchksize);
        //     VALGRIND_MAKE_MEM_DEFINED(pointer, size);
        // #endif
        // TODO(pg-port): MEMORY_CONTEXT_CHECKING / valgrind (default-off, no-op).

        // Disallow access to the chunk header.
        // VALGRIND_MAKE_MEM_NOACCESS(chunk, ALLOC_CHUNKHDRSZ);
        // TODO(pg-port): valgrind no-op.

        pointer
    } else {
        // Enlarge-a-small-chunk case.  We just do this by brute force, ie,
        // allocate a new chunk and copy the data.  Since we know the existing
        // data isn't huge, this won't involve any great memcpy expense, so
        // it's not worth being smarter.  (At one time we tried to avoid
        // memcpy when it was possible to enlarge the chunk in-place, but that
        // turns out to misbehave unpleasantly for repeated cycles of
        // palloc/repalloc/pfree: the eventually freed chunks go into the
        // wrong freelist for the next initial palloc request, and so we leak
        // memory indefinitely.  See pgsql-hackers archives for 2007-08-11.)
        let newPointer: AllocPointer;
        let oldsize: Size;

        // allocate new chunk (this also checks size is valid)
        newPointer = AllocSetAlloc(set as MemoryContext, size, flags);

        // leave immediately if request was not completed
        if newPointer.is_null() {
            // Disallow access to the chunk header.
            // VALGRIND_MAKE_MEM_NOACCESS(chunk, ALLOC_CHUNKHDRSZ);
            // TODO(pg-port): valgrind no-op.
            return MemoryContextAllocationFailure(set as MemoryContext, size, flags);
        }

        // AllocSetAlloc() may have returned a region that is still NOACCESS.
        // Change it to UNDEFINED for the moment; memcpy() will then transfer
        // definedness from the old allocation to the new.  If we know the old
        // allocation, copy just that much.  Otherwise, make the entire old
        // chunk defined to avoid errors as we copy the currently-NOACCESS
        // trailing bytes.
        // VALGRIND_MAKE_MEM_UNDEFINED(newPointer, size);
        // #ifdef MEMORY_CONTEXT_CHECKING
        //     oldsize = chunk->requested_size;
        // #else
        oldsize = oldchksize;
        //     VALGRIND_MAKE_MEM_DEFINED(pointer, oldsize);
        // #endif
        // TODO(pg-port): MEMORY_CONTEXT_CHECKING / valgrind (default-off, no-op).

        // transfer existing data (certain to fit)
        core::ptr::copy_nonoverlapping(pointer as *const u8, newPointer as *mut u8, oldsize);

        // free old chunk
        AllocSetFree(pointer);

        newPointer
    }
}

/// AllocSetGetChunkContext
///		Return the MemoryContext that 'pointer' belongs to.
///
/// # Safety
/// `pointer` must be a live allocation from an AllocSet context.
pub unsafe fn AllocSetGetChunkContext(pointer: *mut c_void) -> MemoryContext {
    let chunk: *mut MemoryChunk = PointerGetMemoryChunk(pointer);
    let block: AllocBlock;
    let set: AllocSet;

    // Allow access to the chunk header.
    // VALGRIND_MAKE_MEM_DEFINED(chunk, ALLOC_CHUNKHDRSZ);
    // TODO(pg-port): valgrind no-op.

    if MemoryChunkIsExternal(chunk) {
        block = ExternalChunkGetBlock(chunk);
    } else {
        block = MemoryChunkGetBlock(chunk) as AllocBlock;
    }

    // Disallow access to the chunk header.
    // VALGRIND_MAKE_MEM_NOACCESS(chunk, ALLOC_CHUNKHDRSZ);
    // TODO(pg-port): valgrind no-op.

    Assert!(AllocBlockIsValid(block));
    set = (*block).aset;

    &mut (*set).header
}

/// AllocSetGetChunkSpace
///		Given a currently-allocated chunk, determine the total space
///		it occupies (including all memory-allocation overhead).
///
/// # Safety
/// `pointer` must be a live allocation from an AllocSet context.
pub unsafe fn AllocSetGetChunkSpace(pointer: *mut c_void) -> Size {
    let chunk: *mut MemoryChunk = PointerGetMemoryChunk(pointer);
    let fidx: c_int;

    // Allow access to the chunk header.
    // VALGRIND_MAKE_MEM_DEFINED(chunk, ALLOC_CHUNKHDRSZ);
    // TODO(pg-port): valgrind no-op.

    if MemoryChunkIsExternal(chunk) {
        let block: AllocBlock = ExternalChunkGetBlock(chunk);

        // Disallow access to the chunk header.
        // VALGRIND_MAKE_MEM_NOACCESS(chunk, ALLOC_CHUNKHDRSZ);
        // TODO(pg-port): valgrind no-op.

        Assert!(AllocBlockIsValid(block));

        return ((*block).endptr as usize).wrapping_sub(chunk as *mut u8 as usize) as Size;
    }

    fidx = MemoryChunkGetValue(chunk) as c_int;
    Assert!(FreeListIdxIsValid(fidx));

    // Disallow access to the chunk header.
    // VALGRIND_MAKE_MEM_NOACCESS(chunk, ALLOC_CHUNKHDRSZ);
    // TODO(pg-port): valgrind no-op.

    GetChunkSizeFromFreeListIdx(fidx) + ALLOC_CHUNKHDRSZ
}

/// AllocSetIsEmpty
///		Is an allocset empty of any allocated space?
///
/// # Safety
/// `context` must be a valid AllocSet context.
pub unsafe fn AllocSetIsEmpty(context: MemoryContext) -> bool {
    Assert!(AllocSetIsValid(context as AllocSet));

    // For now, we say "empty" only if the context is new or just reset. We
    // could examine the freelists to determine if all space has been freed,
    // but it's not really worth the trouble for present uses of this
    // functionality.
    if (*context).isReset {
        return true;
    }
    false
}

/// AllocSetStats
///		Compute stats about memory consumption of an allocset.
///
/// printfunc: if not NULL, pass a human-readable stats string to this.
/// passthru: pass this pointer through to printfunc.
/// totals: if not NULL, add stats about this context into *totals.
/// print_to_stderr: print stats to stderr if true, elog otherwise.
///
/// # Safety
/// `context` must be a valid AllocSet context; `totals` must be NULL or valid.
pub unsafe fn AllocSetStats(
    context: MemoryContext,
    printfunc: MemoryStatsPrintFunc,
    passthru: *mut c_void,
    totals: *mut MemoryContextCounters,
    print_to_stderr: bool,
) {
    let set: AllocSet = context as AllocSet;
    let mut nblocks: Size = 0;
    let mut freechunks: Size = 0;
    let mut totalspace: Size;
    let mut freespace: Size = 0;
    let mut block: AllocBlock;
    let mut fidx: c_int;

    Assert!(AllocSetIsValid(set));

    // Include context header in totalspace
    totalspace = MAXALIGN(core::mem::size_of::<AllocSetContext>());

    block = (*set).blocks;
    while !block.is_null() {
        nblocks += 1;
        totalspace +=
            ((*block).endptr as usize).wrapping_sub(block as *mut u8 as usize) as Size;
        freespace +=
            ((*block).endptr as usize).wrapping_sub((*block).freeptr as usize) as Size;
        block = (*block).next;
    }
    fidx = 0;
    while (fidx as usize) < ALLOCSET_NUM_FREELISTS {
        let chksz: Size = GetChunkSizeFromFreeListIdx(fidx);
        let mut chunk: *mut MemoryChunk = (*set).freelist[fidx as usize];

        while !chunk.is_null() {
            let link: *mut AllocFreeListLink = GetFreeListLink(chunk);

            // Allow access to the chunk header.
            // VALGRIND_MAKE_MEM_DEFINED(chunk, ALLOC_CHUNKHDRSZ);
            Assert!(MemoryChunkGetValue(chunk) == fidx as Size);
            // VALGRIND_MAKE_MEM_NOACCESS(chunk, ALLOC_CHUNKHDRSZ);
            // TODO(pg-port): valgrind no-op.

            freechunks += 1;
            freespace += chksz + ALLOC_CHUNKHDRSZ;

            // VALGRIND_MAKE_MEM_DEFINED(link, sizeof(AllocFreeListLink));
            chunk = (*link).next;
            // VALGRIND_MAKE_MEM_NOACCESS(link, sizeof(AllocFreeListLink));
            // TODO(pg-port): valgrind no-op.
        }

        fidx += 1;
    }

    if let Some(printfunc) = printfunc {
        // char stats_string[200];
        // snprintf(stats_string, sizeof(stats_string),
        //          "%zu total in %zu blocks; %zu free (%zu chunks); %zu used",
        //          totalspace, nblocks, freespace, freechunks, totalspace - freespace);
        let stats_string = format!(
            "{} total in {} blocks; {} free ({} chunks); {} used\0",
            totalspace,
            nblocks,
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
//
// AllocSetCheck
//		Walk through chunks and check consistency of memory.
//
// NOTE: report errors as WARNING, *not* ERROR or FATAL.  Otherwise you'll
// find yourself in an infinite loop when trouble occurs, because this
// routine will be entered again when elog cleanup tries to release memory!
//
// TODO(pg-port): translate AllocSetCheck under a cfg gating
// MEMORY_CONTEXT_CHECKING. It walks every block's chunk chain validating block
// headers, freelist indices, block offsets, sentinels (sentinel_ok), and that
// total_allocated == context->mem_allocated. Omitted here because the default
// build (no MEMORY_CONTEXT_CHECKING) never compiles it, and it depends on the
// not-yet-modeled requested_size field and set_sentinel/sentinel_ok helpers.
// #endif							/* MEMORY_CONTEXT_CHECKING */

// =============================================================================
// Summary
// -----------------------------------------------------------------------------
// Structs + layout (all #[repr(C)], identifiers verbatim):
//   - AllocSetContext: FIRST field is embedded `header: MemoryContextData`, then
//     `blocks: AllocBlock`, `freelist: [*mut MemoryChunk; ALLOCSET_NUM_FREELISTS]`,
//     and the uint32 params (initBlockSize/maxBlockSize/nextBlockSize/
//     allocChunkLimit) plus `freeListIndex: c_int`. `AllocSet = *mut AllocSetContext`.
//   - AllocBlockData (AllocBlock = *mut AllocBlockData): aset/prev/next/freeptr/endptr.
//   - AllocFreeListLink: { next: *mut MemoryChunk } stored inside a freed chunk's payload.
//   - AllocSetFreeList: { num_free: c_int, first_free: *mut AllocSetContext };
//     `context_freelists: [AllocSetFreeList; 2]` is a `static mut` (single-threaded
//     backend, matching the C file-scope global).
//
// The 11 method fns (signatures match MemoryContextMethods fn-pointer types in
// memnodes.rs): AllocSetAlloc(context, size, flags)->*mut c_void,
// AllocSetFree(pointer), AllocSetRealloc(pointer, size, flags)->*mut c_void,
// AllocSetReset(context), AllocSetDelete(context),
// AllocSetGetChunkContext(pointer)->MemoryContext,
// AllocSetGetChunkSpace(pointer)->Size, AllocSetIsEmpty(context)->bool,
// AllocSetStats(context, printfunc, passthru, totals, print_to_stderr). All are
// `pub unsafe fn`. (That is 9 distinct method-table slots; "11" counts the two
// pg_noinline alloc helpers AllocSetAllocLarge/AllocSetAllocFromNewBlock, which
// are private here. AllocSetAllocChunkFromBlock is the inline shared helper.)
//
// AllocSetContextCreate handling: AllocSetContextCreateInternal sets node tag
// T_AllocSetContext + method_id MCTX_ASET_ID via MemoryContextCreate (currently a
// stub in memutils_internal; will be real once mcxt.c lands). The freelist-recycle
// fast path and keeper-block setup are translated faithfully.
//
// Wrapping arithmetic: AllocSetFreeIndex's leftmost-bit math uses wrapping_sub/
// wrapping_add to match C's wrapping semantics (HAVE_BITSCAN_REVERSE path via
// pg_leftmost_one_pos32, i.e. __builtin_clz). All pointer-difference computations
// (endptr - block, endptr - freeptr, endptr - pointer, etc.) use usize
// wrapping_sub to mirror C `(char *)` subtraction without Rust debug overflow
// panics. The `1 << fidx` chunk-size math and the block-doubling `<<= 1` shifts
// use plain shifts (bounded by ALLOCSET_NUM_FREELISTS / maxBlockSize, as in C).
//
// malloc/free binding: `extern "C" { malloc/free/realloc }` (raw libc), used for
// blocks exactly as aset.c does; coexists with the bootstrap utils::palloc.
//
// Stubbed: all VALGRIND_* hooks are no-ops; MEMORY_CONTEXT_CHECKING /
// RANDOMIZE_ALLOCATED_MEMORY / CLOBBER_FREED_MEMORY branches translate the
// default-off (#else / non-checking) path, with the checking branches preserved
// as comments + TODO(pg-port). AllocSetCheck is omitted (compiled only under
// MEMORY_CONTEXT_CHECKING). MemoryContextStats/MemoryContextResetOnly are local
// TODO(pg-port) stubs pending mcxt.c; ereport/elog become panic!/unimplemented!.
// A few not-yet-exported memutils.h constants (ALLOCSET_DEFAULT/SMALL MINSIZE/
// INITSIZE, ALLOCSET_SEPARATE_THRESHOLD, InvalidAllocSize) are defined locally
// verbatim from the C header (TODO(pg-port): hoist to memutils.rs).
//
// Every aset.c function was translated: AllocSetFreeIndex,
// AllocSetContextCreateInternal, AllocSetReset, AllocSetDelete,
// AllocSetAllocLarge, AllocSetAllocChunkFromBlock, AllocSetAllocFromNewBlock,
// AllocSetAlloc, AllocSetFree, AllocSetRealloc, AllocSetGetChunkContext,
// AllocSetGetChunkSpace, AllocSetIsEmpty, AllocSetStats (AllocSetCheck noted as
// a cfg-gated TODO).
// =============================================================================
