//! Translated from PostgreSQL src/include/storage/itemptr.h
//! POSTGRES disk item pointer definitions.

use crate::postgres::Datum;
use crate::storage::block::{BlockIdData, BlockNumber, INVALID_BLOCK_NUMBER};
use crate::storage::off::{OffsetNumber, INVALID_OFFSET_NUMBER};

/// On-disk pointer to an item within a disk page (a TID). Designed to be exactly
/// six bytes (BlockIdData + OffsetNumber): `#[repr(C, packed(2))]` matches the C
/// packed+aligned(2) struct so no padding is wasted in tuple/index headers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(C, packed(2))]
pub struct ItemPointerData {
    pub blkid: BlockIdData,
    pub posid: OffsetNumber,
}

const _: () = assert!(core::mem::size_of::<ItemPointerData>() == 6);
const _: () = assert!(core::mem::align_of::<ItemPointerData>() == 2);
const _: () = assert!(core::mem::offset_of!(ItemPointerData, blkid) == 0);
const _: () = assert!(core::mem::offset_of!(ItemPointerData, posid) == 4);

/// Speculative-insertion token marker: posid set to this, token in blkid.
pub const SpecTokenOffsetNumber: OffsetNumber = 0xfffe;

/// ctid of an old tuple version moved to another partition by UPDATE.
pub const MovedPartitionsOffsetNumber: OffsetNumber = 0xfffd;
pub const MovedPartitionsBlockNumber: BlockNumber = INVALID_BLOCK_NUMBER;

impl ItemPointerData {
    /// True iff the disk item pointer is valid (non-zero offset).
    pub const fn is_valid(&self) -> bool {
        self.posid != 0
    }

    /// Block number, without validity check.
    pub const fn block_number_no_check(&self) -> BlockNumber {
        self.blkid.block_number()
    }

    /// Block number; the C version asserts validity first.
    pub const fn block_number(&self) -> BlockNumber {
        debug_assert!(self.is_valid());
        self.block_number_no_check()
    }

    /// Offset number, without validity check.
    pub const fn offset_number_no_check(&self) -> OffsetNumber {
        self.posid
    }

    /// Offset number; the C version asserts validity first.
    pub const fn offset_number(&self) -> OffsetNumber {
        debug_assert!(self.is_valid());
        self.posid
    }

    /// Set to the specified block and offset.
    pub const fn set(&mut self, block_number: BlockNumber, off_num: OffsetNumber) {
        self.blkid.set(block_number);
        self.posid = off_num;
    }

    /// Set the block number only.
    pub const fn set_block_number(&mut self, block_number: BlockNumber) {
        self.blkid.set(block_number);
    }

    /// Set the offset number only.
    pub const fn set_offset_number(&mut self, offset_number: OffsetNumber) {
        self.posid = offset_number;
    }

    /// Set to invalid.
    pub const fn set_invalid(&mut self) {
        self.blkid.set(INVALID_BLOCK_NUMBER);
        self.posid = INVALID_OFFSET_NUMBER;
    }

    /// True iff the tuple has moved to another partition.
    pub const fn indicates_moved_partitions(&self) -> bool {
        self.offset_number() == MovedPartitionsOffsetNumber
            && self.block_number_no_check() == MovedPartitionsBlockNumber
    }

    /// Mark as moved to a different partition.
    pub const fn set_moved_partitions(&mut self) {
        self.set(MovedPartitionsBlockNumber, MovedPartitionsOffsetNumber);
    }
}

// `ItemPointerCopy` is a plain assignment in C; use Rust assignment / Copy.

// itemptr.c's two non-inline functions are idiomatic methods in the backend
// module; keep C-named deprecated shims delegating to them (cross-reference).

#[deprecated(note = "use `pointer1.equals(pointer2)`")]
#[inline]
pub fn ItemPointerEquals(pointer1: &ItemPointerData, pointer2: &ItemPointerData) -> bool {
    pointer1.equals(pointer2)
}

#[deprecated(note = "use `arg1.compare(arg2)`")]
#[inline]
pub fn ItemPointerCompare(arg1: &ItemPointerData, arg2: &ItemPointerData) -> i32 {
    arg1.compare(arg2)
}

#[deprecated(note = "use `pointer.inc()`")]
#[inline]
pub fn ItemPointerInc(pointer: &mut ItemPointerData) {
    pointer.inc();
}

#[deprecated(note = "use `pointer.dec()`")]
#[inline]
pub fn ItemPointerDec(pointer: &mut ItemPointerData) {
    pointer.dec();
}

// Datum conversions: an ItemPointer travels in a Datum as a pointer value.
pub fn DatumGetItemPointer(_x: Datum) -> *mut ItemPointerData {
    unimplemented!() // TODO(ptr): Datum-as-pointer round-trip
}

pub fn ItemPointerGetDatum(_x: &ItemPointerData) -> Datum {
    unimplemented!() // TODO(ptr): Datum-as-pointer round-trip
}
