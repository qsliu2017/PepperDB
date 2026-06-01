//! Translation of postgres/src/include/utils/memutils_memorychunk.h
//!
//! Here we define a struct named MemoryChunk which implementations of
//! MemoryContexts may use as a header for chunks of memory they allocate.
//!
//! MemoryChunk provides a lightweight header that a MemoryContext can use to
//! store a reference back to the block which the given chunk is allocated on
//! and also an additional 30-bits to store another value such as the size of
//! the allocated chunk.
//!
//! Although MemoryChunks are used by each of our MemoryContexts, future
//! implementations may choose to implement their own method for storing chunk
//! headers.  The only requirement is that the header ends with an 8-byte value
//! which the least significant 4-bits of are set to the MemoryContextMethodID
//! of the given context.
//!
//! By default, a MemoryChunk is 8 bytes in size, however, when
//! MEMORY_CONTEXT_CHECKING is defined the header becomes 16 bytes in size due
//! to the additional requested_size field.  The MemoryContext may use this
//! field for whatever they wish, but it is intended to be used for additional
//! checks which are only done in MEMORY_CONTEXT_CHECKING builds.
//!
//! The MemoryChunk contains a uint64 field named 'hdrmask'.  This field is
//! used to encode 4 separate pieces of information.  Starting with the least
//! significant bits of 'hdrmask', the bit space is reserved as follows:
//!
//! 1.	4-bits to indicate the MemoryContextMethodID as defined by
//! 		MEMORY_CONTEXT_METHODID_MASK
//! 2.	1-bit to denote an "external" chunk (see below)
//! 3.	30-bits reserved for the MemoryContext to use for anything it
//! 		requires.  Most MemoryContexts likely want to store the size of the
//! 		chunk here.
//! 4.	30-bits for the number of bytes that must be subtracted from the chunk
//! 		to obtain the address of the block that the chunk is stored on.
//!
//! If you're paying close attention, you'll notice this adds up to 65 bits
//! rather than 64 bits.  This is because the highest-order bit of #3 is the
//! same bit as the lowest-order bit of #4.  We can do this as we insist that
//! the chunk and block pointers are both MAXALIGNed, therefore the relative
//! offset between those will always be a MAXALIGNed value which means the
//! lowest order bit is always 0.  When fetching the chunk to block offset we
//! mask out the lowest-order bit to ensure it's still zero.
//!
//! In some cases, for example when memory allocations become large, it's
//! possible fields 3 and 4 above are not large enough to store the values
//! required for the chunk.  In this case, the MemoryContext can choose to mark
//! the chunk as "external" by calling the MemoryChunkSetHdrMaskExternal()
//! function.  When this is done, fields 3 and 4 are unavailable for use by the
//! MemoryContext and it's up to the MemoryContext itself to devise its own
//! method for getting the reference to the block.
//!
//! Interface:
//!
//! MemoryChunkSetHdrMask:
//! 		Used to set up a non-external MemoryChunk.
//!
//! MemoryChunkSetHdrMaskExternal:
//! 		Used to set up an externally managed MemoryChunk.
//!
//! MemoryChunkIsExternal:
//! 		Determine if the given MemoryChunk is externally managed, i.e.
//! 		MemoryChunkSetHdrMaskExternal() was called on the chunk.
//!
//! MemoryChunkGetValue:
//! 		For non-external chunks, return the stored 30-bit value as it was set
//! 		in the call to MemoryChunkSetHdrMask().
//!
//! MemoryChunkGetBlock:
//! 		For non-external chunks, return a pointer to the block as it was set
//! 		in the call to MemoryChunkSetHdrMask().
//!
//! Also exports:
//! 		MEMORYCHUNK_MAX_VALUE
//! 		MEMORYCHUNK_MAX_BLOCKOFFSET
//! 		PointerGetMemoryChunk
//! 		MemoryChunkGetPointer
//!
//! Portions Copyright (c) 2022-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! src/include/utils/memutils_memorychunk.h

use crate::c::{uint64, Size, UINT64CONST};
use crate::utils::mmgr::memutils_internal::{
    MemoryContextMethodID, MEMORY_CONTEXT_METHODID_BITS, MEMORY_CONTEXT_METHODID_MASK,
};
use core::ffi::c_void;
// `Assert!` is brought into scope crate-wide via `#[macro_use] pub mod c;` in main.rs.

// Note: the C header pulls these inline functions' dependency chain from
// "utils/memutils_internal.h" (MemoryContextMethodID, MEMORY_CONTEXT_METHODID_BITS,
// MEMORY_CONTEXT_METHODID_MASK), which is the sibling module imported above.

/// The maximum allowed value that MemoryContexts can store in the value
/// field.  Must be 1 less than a power of 2.
pub const MEMORYCHUNK_MAX_VALUE: uint64 = UINT64CONST(0x3FFFFFFF);

/// The maximum distance in bytes that a MemoryChunk can be offset from the
/// block that is storing the chunk.  Must be 1 less than a power of 2.
pub const MEMORYCHUNK_MAX_BLOCKOFFSET: uint64 = UINT64CONST(0x3FFFFFFF);

/// As above, but mask out the lowest-order (always zero) bit as this is shared
/// with the MemoryChunkGetValue field.
const MEMORYCHUNK_BLOCKOFFSET_MASK: uint64 = UINT64CONST(0x3FFFFFFE);

// define the least significant base-0 bit of each portion of the hdrmask
// (kept as u64 since they are used as shift amounts against u64 hdrmask values;
// MEMORY_CONTEXT_METHODID_BITS is a c_int in the sibling module)
const MEMORYCHUNK_EXTERNAL_BASEBIT: uint64 = MEMORY_CONTEXT_METHODID_BITS as uint64;
const MEMORYCHUNK_VALUE_BASEBIT: uint64 = MEMORYCHUNK_EXTERNAL_BASEBIT + 1;
const MEMORYCHUNK_BLOCKOFFSET_BASEBIT: uint64 = MEMORYCHUNK_VALUE_BASEBIT + 29;

/// A magic number for storing in the free bits of an external chunk.  This
/// must mask out the bits used for storing the MemoryContextMethodID and the
/// external bit.
//
// C: (UINT64CONST(0xB1A8DB858EB6EFBA) >> MEMORYCHUNK_VALUE_BASEBIT
//                                      << MEMORYCHUNK_VALUE_BASEBIT)
// Shift widths are constant (< 64), so plain shifts can't overflow.
const MEMORYCHUNK_MAGIC: uint64 =
    UINT64CONST(0xB1A8DB858EB6EFBA) >> MEMORYCHUNK_VALUE_BASEBIT << MEMORYCHUNK_VALUE_BASEBIT;

/// The MemoryChunk header that precedes every allocation.
///
/// By default this is 8 bytes (just `hdrmask`); under MEMORY_CONTEXT_CHECKING
/// it would gain a leading `requested_size: Size` field, growing it to 16
/// bytes. That field is not compiled in by default here.
// TODO(pg-port): add `requested_size` under a cfg when MEMORY_CONTEXT_CHECKING
// is modeled (it must precede `hdrmask`, which "must be last").
#[repr(C)]
pub struct MemoryChunk {
    /// bitfield for storing details about the chunk; must be last
    pub hdrmask: uint64,
}

/// Get the MemoryChunk from the pointer.
///
/// `PointerGetMemoryChunk(p) = (MemoryChunk *) ((char *) p - sizeof(MemoryChunk))`
///
/// # Safety
/// `p` must point `sizeof(MemoryChunk)` bytes past the start of a live
/// MemoryChunk header (i.e. it is the payload pointer of a real chunk).
#[inline]
pub unsafe fn PointerGetMemoryChunk(p: *mut c_void) -> *mut MemoryChunk {
    (p as *mut u8).sub(core::mem::size_of::<MemoryChunk>()) as *mut MemoryChunk
}

/// Get the pointer from the MemoryChunk.
///
/// `MemoryChunkGetPointer(c) = (void *) ((char *) c + sizeof(MemoryChunk))`
///
/// # Safety
/// `c` must point to a live MemoryChunk header that is immediately followed by
/// its payload.
#[inline]
pub unsafe fn MemoryChunkGetPointer(c: *mut MemoryChunk) -> *mut c_void {
    (c as *mut u8).add(core::mem::size_of::<MemoryChunk>()) as *mut c_void
}

// private helpers for making the inline functions below more simple

/// `HdrMaskIsExternal(hdrmask)`
#[inline]
fn HdrMaskIsExternal(hdrmask: uint64) -> bool {
    // C macro yields the masked bits (non-zero == true); we return the bool.
    // MEMORYCHUNK_EXTERNAL_BASEBIT < 64, so the shift cannot overflow.
    (hdrmask & ((1u64) << MEMORYCHUNK_EXTERNAL_BASEBIT)) != 0
}

/// `HdrMaskGetValue(hdrmask)`
#[inline]
fn HdrMaskGetValue(hdrmask: uint64) -> Size {
    ((hdrmask >> MEMORYCHUNK_VALUE_BASEBIT) & MEMORYCHUNK_MAX_VALUE) as Size
}

/// `HdrMaskBlockOffset(hdrmask)`
///
/// Shift the block offset down to the 0th bit position and mask off the single
/// bit that's shared with the MemoryChunkGetValue field.
#[inline]
fn HdrMaskBlockOffset(hdrmask: uint64) -> uint64 {
    (hdrmask >> MEMORYCHUNK_BLOCKOFFSET_BASEBIT) & MEMORYCHUNK_BLOCKOFFSET_MASK
}

/// `HdrMaskCheckMagic(hdrmask)` - for external chunks only, check the magic
/// number matches.
#[inline]
fn HdrMaskCheckMagic(hdrmask: uint64) -> bool {
    MEMORYCHUNK_MAGIC == (hdrmask >> MEMORYCHUNK_VALUE_BASEBIT << MEMORYCHUNK_VALUE_BASEBIT)
}

/// MemoryChunkSetHdrMask
/// 		Store the given 'block', 'chunk_size' and 'methodid' in the given
/// 		MemoryChunk.
///
/// The number of bytes between 'block' and 'chunk' must be <=
/// MEMORYCHUNK_MAX_BLOCKOFFSET.
/// 'value' must be <= MEMORYCHUNK_MAX_VALUE.
/// Both 'chunk' and 'block' must be MAXALIGNed pointers.
///
/// # Safety
/// `chunk` must point to a live MemoryChunk and `block` must point to the start
/// of the block that physically contains `chunk`, with `chunk >= block`.
#[inline]
pub unsafe fn MemoryChunkSetHdrMask(
    chunk: *mut MemoryChunk,
    block: *mut c_void,
    value: Size,
    methodid: MemoryContextMethodID,
) {
    let blockoffset: Size = (chunk as *mut u8 as usize) - (block as *mut u8 as usize);

    Assert!((chunk as *mut u8 as usize) >= (block as *mut u8 as usize));
    Assert!((blockoffset as uint64 & MEMORYCHUNK_BLOCKOFFSET_MASK) == blockoffset as uint64);
    Assert!(value as uint64 <= MEMORYCHUNK_MAX_VALUE);
    Assert!((methodid as i32 as uint64) <= MEMORY_CONTEXT_METHODID_MASK);

    // All shift widths are constants < 64, so the shifts cannot overflow.
    (*chunk).hdrmask = ((blockoffset as uint64) << MEMORYCHUNK_BLOCKOFFSET_BASEBIT)
        | ((value as uint64) << MEMORYCHUNK_VALUE_BASEBIT)
        | (methodid as uint64);
}

/// MemoryChunkSetHdrMaskExternal
/// 		Set 'chunk' as an externally managed chunk.  Here we only record the
/// 		MemoryContextMethodID and set the external chunk bit.
///
/// # Safety
/// `chunk` must point to a live MemoryChunk.
#[inline]
pub unsafe fn MemoryChunkSetHdrMaskExternal(
    chunk: *mut MemoryChunk,
    methodid: MemoryContextMethodID,
) {
    Assert!((methodid as i32 as uint64) <= MEMORY_CONTEXT_METHODID_MASK);

    // MEMORYCHUNK_EXTERNAL_BASEBIT < 64, so the shift cannot overflow.
    (*chunk).hdrmask =
        MEMORYCHUNK_MAGIC | ((1u64) << MEMORYCHUNK_EXTERNAL_BASEBIT) | (methodid as uint64);
}

/// MemoryChunkIsExternal
/// 		Return true if 'chunk' is marked as external.
///
/// # Safety
/// `chunk` must point to a live MemoryChunk.
#[inline]
pub unsafe fn MemoryChunkIsExternal(chunk: *mut MemoryChunk) -> bool {
    /*
     * External chunks should always store MEMORYCHUNK_MAGIC in the upper
     * portion of the hdrmask, check that nothing has stomped on that.
     */
    Assert!(!HdrMaskIsExternal((*chunk).hdrmask) || HdrMaskCheckMagic((*chunk).hdrmask));

    HdrMaskIsExternal((*chunk).hdrmask)
}

/// MemoryChunkGetValue
/// 		For non-external chunks, returns the value field as it was set in
/// 		MemoryChunkSetHdrMask.
///
/// # Safety
/// `chunk` must point to a live non-external MemoryChunk.
#[inline]
pub unsafe fn MemoryChunkGetValue(chunk: *mut MemoryChunk) -> Size {
    Assert!(!HdrMaskIsExternal((*chunk).hdrmask));

    HdrMaskGetValue((*chunk).hdrmask)
}

/// MemoryChunkGetBlock
/// 		For non-external chunks, returns the pointer to the block as was set
/// 		in MemoryChunkSetHdrMask.
///
/// # Safety
/// `chunk` must point to a live non-external MemoryChunk.
#[inline]
pub unsafe fn MemoryChunkGetBlock(chunk: *mut MemoryChunk) -> *mut c_void {
    Assert!(!HdrMaskIsExternal((*chunk).hdrmask));

    (chunk as *mut u8).sub(HdrMaskBlockOffset((*chunk).hdrmask) as usize) as *mut c_void
}

// Summary
// -------
// `MemoryChunk` is the 8-byte (`hdrmask: uint64`) header sitting immediately
// before every allocation; under MEMORY_CONTEXT_CHECKING it would gain a leading
// `requested_size` field (TODO(pg-port) cfg, not yet modeled). `hdrmask`
// bit-packs (LSB-first) a 4-bit MemoryContextMethodID, a 1-bit external flag at
// MEMORYCHUNK_EXTERNAL_BASEBIT, a 30-bit value at MEMORYCHUNK_VALUE_BASEBIT, and
// a 30-bit block offset at MEMORYCHUNK_BLOCKOFFSET_BASEBIT (the latter two share
// one always-zero bit, hence the 65-vs-64 trick and MEMORYCHUNK_BLOCKOFFSET_MASK).
// The `MemoryChunkSetHdrMask*`/`Get*`/`IsExternal` helpers encode/decode those
// fields via the private `HdrMask*` helpers; `Pointer<->Chunk` conversion is a
// `sizeof(MemoryChunk)` pointer offset. All shift widths are compile-time
// constants strictly less than 64, so plain `<<`/`>>` can never overflow and no
// `wrapping_shl`/`wrapping_sub` was needed; pointer offsets use `usize`
// subtraction matching the C `(char *)` pointer arithmetic.
