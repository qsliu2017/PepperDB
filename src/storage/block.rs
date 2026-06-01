//! Translation of postgres/src/include/storage/block.h
//!
//! Block number definitions + the on-disk BlockIdData representation.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::c::{uint16, uint32};

/*
 * BlockNumber: each data file (heap or index) is divided into postgres disk
 * blocks (which may be thought of as the unit of i/o -- a postgres buffer
 * contains exactly one disk block).  the blocks are numbered sequentially,
 * 0 to 0xFFFFFFFE.  InvalidBlockNumber is 0xFFFFFFFF.
 */
pub type BlockNumber = uint32;

pub const InvalidBlockNumber: BlockNumber = 0xFFFF_FFFF;
pub const MaxBlockNumber: BlockNumber = 0xFFFF_FFFE;

/*
 * BlockId: a block number stored in a packed (unaligned) 2x uint16 form, so it
 * needs no special alignment within an ItemPointer.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct BlockIdData {
    pub bi_hi: uint16,
    pub bi_lo: uint16,
}
pub type BlockId = *mut BlockIdData;

/// BlockNumberIsValid - true iff blockNumber is valid.
#[inline]
pub fn BlockNumberIsValid(blockNumber: BlockNumber) -> bool {
    blockNumber != InvalidBlockNumber
}

/// BlockIdSet - set a block identifier to the specified value.
///
/// # Safety
/// `blockId` points to a writable BlockIdData.
#[inline]
pub unsafe fn BlockIdSet(blockId: *mut BlockIdData, blockNumber: BlockNumber) {
    (*blockId).bi_hi = (blockNumber >> 16) as uint16;
    (*blockId).bi_lo = (blockNumber & 0xffff) as uint16;
}

/// BlockIdEquals - check for block number equality.
///
/// # Safety
/// Both pointers reference valid BlockIdData.
#[inline]
pub unsafe fn BlockIdEquals(blockId1: *const BlockIdData, blockId2: *const BlockIdData) -> bool {
    (*blockId1).bi_hi == (*blockId2).bi_hi && (*blockId1).bi_lo == (*blockId2).bi_lo
}

/// BlockIdGetBlockNumber - retrieve the block number from a block identifier.
///
/// # Safety
/// `blockId` references a valid BlockIdData.
#[inline]
pub unsafe fn BlockIdGetBlockNumber(blockId: *const BlockIdData) -> BlockNumber {
    (((*blockId).bi_hi as BlockNumber) << 16) | ((*blockId).bi_lo as BlockNumber)
}
