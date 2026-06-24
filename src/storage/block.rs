//! Translated from PostgreSQL src/include/storage/block.h

/// BlockNumber: blocks are numbered 0 to 0xFFFFFFFE; the type calculations use.
pub type BlockNumber = u32;

pub const INVALID_BLOCK_NUMBER: BlockNumber = 0xFFFFFFFF;
pub const MAX_BLOCK_NUMBER: BlockNumber = 0xFFFFFFFE;

/// On-disk storage type for a BlockNumber, split into two SHORTALIGN'd halves
/// so it (and structs containing it, e.g. ItemPointerData) can be 2-byte aligned.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(C)]
pub struct BlockIdData {
    pub bi_hi: u16,
    pub bi_lo: u16,
}

const _: () = assert!(core::mem::size_of::<BlockIdData>() == 4);
const _: () = assert!(core::mem::align_of::<BlockIdData>() == 2);
const _: () = assert!(core::mem::offset_of!(BlockIdData, bi_hi) == 0);
const _: () = assert!(core::mem::offset_of!(BlockIdData, bi_lo) == 2);

/// True iff blockNumber is valid.
pub const fn block_number_is_valid(block_number: BlockNumber) -> bool {
    block_number != INVALID_BLOCK_NUMBER
}

impl BlockIdData {
    /// Set a block identifier to the specified value.
    pub const fn set(&mut self, block_number: BlockNumber) {
        self.bi_hi = (block_number >> 16) as u16;
        self.bi_lo = (block_number & 0xffff) as u16;
    }

    /// Check for block number equality.
    pub const fn equals(&self, other: &BlockIdData) -> bool {
        self.bi_hi == other.bi_hi && self.bi_lo == other.bi_lo
    }

    /// Retrieve the block number from a block identifier.
    pub const fn block_number(&self) -> BlockNumber {
        ((self.bi_hi as BlockNumber) << 16) | (self.bi_lo as BlockNumber)
    }
}
