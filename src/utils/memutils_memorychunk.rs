//! Translated from PostgreSQL src/include/utils/memutils_memorychunk.h
//! MemoryChunk: a lightweight 8-byte header MemoryContexts may prepend to
//! allocated chunks. `hdrmask` packs four fields (LSB-first):
//!  1. 4 bits MemoryContextMethodID
//!  2. 1 bit "external" flag
//!  3. 30 bits context value (typically chunk size)
//!  4. 30 bits block offset (chunk-to-block distance); MAXALIGNed so the bit
//!     shared between #3 and #4 is always zero.

use crate::c::Size;
use crate::utils::memutils_internal::{
    MemoryContextMethodID, MEMORY_CONTEXT_METHODID_BITS, MEMORY_CONTEXT_METHODID_MASK,
};

/// Max value the context value field can store (1 less than a power of 2).
pub const MEMORYCHUNK_MAX_VALUE: u64 = 0x3FFF_FFFF;
/// Max chunk-to-block offset (1 less than a power of 2).
pub const MEMORYCHUNK_MAX_BLOCKOFFSET: u64 = 0x3FFF_FFFF;
/// As above, with the shared low bit masked out.
const MEMORYCHUNK_BLOCKOFFSET_MASK: u64 = 0x3FFF_FFFE;

/// Least-significant base-0 bit of each hdrmask portion.
const MEMORYCHUNK_EXTERNAL_BASEBIT: u32 = MEMORY_CONTEXT_METHODID_BITS;
const MEMORYCHUNK_VALUE_BASEBIT: u32 = MEMORYCHUNK_EXTERNAL_BASEBIT + 1;
const MEMORYCHUNK_BLOCKOFFSET_BASEBIT: u32 = MEMORYCHUNK_VALUE_BASEBIT + 29;

/// Magic stored in the free bits of an external chunk (with the method-id and
/// external bits masked out).
const MEMORYCHUNK_MAGIC: u64 =
    (0xB1A8_DB85_8EB6_EFBAu64 >> MEMORYCHUNK_VALUE_BASEBIT) << MEMORYCHUNK_VALUE_BASEBIT;

/// On-disk-ish chunk header. By default 8 bytes (the `hdrmask` word). The C
/// `requested_size` field exists only under MEMORY_CONTEXT_CHECKING; omitted.
#[repr(C)]
pub struct MemoryChunk {
    /// bitfield encoding methodid/external/value/blockoffset; must be last.
    pub hdrmask: u64,
}

const _: () = assert!(core::mem::size_of::<MemoryChunk>() == 8);

#[inline]
fn hdr_mask_is_external(hdrmask: u64) -> bool {
    (hdrmask & (1u64 << MEMORYCHUNK_EXTERNAL_BASEBIT)) != 0
}

#[inline]
fn hdr_mask_get_value(hdrmask: u64) -> u64 {
    (hdrmask >> MEMORYCHUNK_VALUE_BASEBIT) & MEMORYCHUNK_MAX_VALUE
}

#[inline]
fn hdr_mask_block_offset(hdrmask: u64) -> u64 {
    (hdrmask >> MEMORYCHUNK_BLOCKOFFSET_BASEBIT) & MEMORYCHUNK_BLOCKOFFSET_MASK
}

#[inline]
fn hdr_mask_check_magic(hdrmask: u64) -> bool {
    MEMORYCHUNK_MAGIC == ((hdrmask >> MEMORYCHUNK_VALUE_BASEBIT) << MEMORYCHUNK_VALUE_BASEBIT)
}

impl MemoryChunk {
    /// Store `block` offset, `value`, and `methodid`. `blockoffset` must be
    /// <= MEMORYCHUNK_MAX_BLOCKOFFSET; `value` <= MEMORYCHUNK_MAX_VALUE.
    pub fn set_hdr_mask(&mut self, blockoffset: Size, value: Size, methodid: MemoryContextMethodID) {
        let blockoffset = blockoffset as u64;
        debug_assert_eq!((blockoffset & MEMORYCHUNK_BLOCKOFFSET_MASK), blockoffset);
        debug_assert!(value as u64 <= MEMORYCHUNK_MAX_VALUE);
        let methodid = methodid as u64;
        debug_assert!(methodid <= MEMORY_CONTEXT_METHODID_MASK);

        self.hdrmask = (blockoffset << MEMORYCHUNK_BLOCKOFFSET_BASEBIT)
            | ((value as u64) << MEMORYCHUNK_VALUE_BASEBIT)
            | methodid;
    }

    /// Set this chunk as externally managed (records methodid + external bit).
    pub fn set_hdr_mask_external(&mut self, methodid: MemoryContextMethodID) {
        let methodid = methodid as u64;
        debug_assert!(methodid <= MEMORY_CONTEXT_METHODID_MASK);
        self.hdrmask = MEMORYCHUNK_MAGIC | (1u64 << MEMORYCHUNK_EXTERNAL_BASEBIT) | methodid;
    }

    /// True if marked external.
    pub fn is_external(&self) -> bool {
        debug_assert!(!hdr_mask_is_external(self.hdrmask) || hdr_mask_check_magic(self.hdrmask));
        hdr_mask_is_external(self.hdrmask)
    }

    /// For non-external chunks, the stored 30-bit value.
    pub fn get_value(&self) -> Size {
        debug_assert!(!hdr_mask_is_external(self.hdrmask));
        hdr_mask_get_value(self.hdrmask) as Size
    }

    /// For non-external chunks, the chunk-to-block byte offset.
    pub fn block_offset(&self) -> Size {
        debug_assert!(!hdr_mask_is_external(self.hdrmask));
        hdr_mask_block_offset(self.hdrmask) as Size
    }
}
