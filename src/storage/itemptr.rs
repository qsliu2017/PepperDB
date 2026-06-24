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
    pub ip_blkid: BlockIdData,
    pub ip_posid: OffsetNumber,
}

const _: () = assert!(core::mem::size_of::<ItemPointerData>() == 6);
const _: () = assert!(core::mem::align_of::<ItemPointerData>() == 2);
const _: () = assert!(core::mem::offset_of!(ItemPointerData, ip_blkid) == 0);
const _: () = assert!(core::mem::offset_of!(ItemPointerData, ip_posid) == 4);

/// Speculative-insertion token marker: ip_posid set to this, token in ip_blkid.
pub const SpecTokenOffsetNumber: OffsetNumber = 0xfffe;

/// t_ctid of an old tuple version moved to another partition by UPDATE.
pub const MovedPartitionsOffsetNumber: OffsetNumber = 0xfffd;
pub const MovedPartitionsBlockNumber: BlockNumber = INVALID_BLOCK_NUMBER;

impl ItemPointerData {
    /// True iff the disk item pointer is valid (non-zero offset).
    pub const fn is_valid(&self) -> bool {
        self.ip_posid != 0
    }

    /// Block number, without validity check.
    pub const fn block_number_no_check(&self) -> BlockNumber {
        self.ip_blkid.block_number()
    }

    /// Block number; the C version asserts validity first.
    pub const fn block_number(&self) -> BlockNumber {
        debug_assert!(self.is_valid());
        self.block_number_no_check()
    }

    /// Offset number, without validity check.
    pub const fn offset_number_no_check(&self) -> OffsetNumber {
        self.ip_posid
    }

    /// Offset number; the C version asserts validity first.
    pub const fn offset_number(&self) -> OffsetNumber {
        debug_assert!(self.is_valid());
        self.ip_posid
    }

    /// Set to the specified block and offset.
    pub const fn set(&mut self, block_number: BlockNumber, off_num: OffsetNumber) {
        self.ip_blkid.set(block_number);
        self.ip_posid = off_num;
    }

    /// Set the block number only.
    pub const fn set_block_number(&mut self, block_number: BlockNumber) {
        self.ip_blkid.set(block_number);
    }

    /// Set the offset number only.
    pub const fn set_offset_number(&mut self, offset_number: OffsetNumber) {
        self.ip_posid = offset_number;
    }

    /// Set to invalid.
    pub const fn set_invalid(&mut self) {
        self.ip_blkid.set(INVALID_BLOCK_NUMBER);
        self.ip_posid = INVALID_OFFSET_NUMBER;
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

pub fn ItemPointerEquals(_pointer1: &ItemPointerData, _pointer2: &ItemPointerData) -> bool {
    unimplemented!()
}

pub fn ItemPointerCompare(_arg1: &ItemPointerData, _arg2: &ItemPointerData) -> i32 {
    unimplemented!()
}

pub fn ItemPointerInc(_pointer: &mut ItemPointerData) {
    unimplemented!()
}

pub fn ItemPointerDec(_pointer: &mut ItemPointerData) {
    unimplemented!()
}

// Datum conversions: an ItemPointer travels in a Datum as a pointer value.
pub fn DatumGetItemPointer(_x: Datum) -> *mut ItemPointerData {
    unimplemented!() // TODO(ptr): Datum-as-pointer round-trip
}

pub fn ItemPointerGetDatum(_x: &ItemPointerData) -> Datum {
    unimplemented!() // TODO(ptr): Datum-as-pointer round-trip
}
