//! Translated from PostgreSQL src/include/storage/aio_types.h

use crate::common::relpath::ForkNumber;
use crate::storage::block::BlockNumber;
use crate::storage::relfilelocator::RelFileLocator;

pub use crate::storage::aio_internal::PgAioHandle;

/// A reference to an IO that can be used to wait for the IO to complete.
/// These can be passed across process boundaries.
pub struct PgAioWaitRef {
    /// internal ID identifying the specific PgAioHandle
    pub aio_index: u32,
    /// generation, split in two to avoid int64 alignment requirements
    pub generation_upper: u32,
    pub generation_lower: u32,
}

/// SMGR variant of PgAioTargetData (the only union member today).
pub struct PgAioTargetDataSmgr {
    pub rlocator: RelFileLocator, // physical relation identifier
    pub block_num: BlockNumber,   // blknum relative to begin of reln
    pub nblocks: BlockNumber,
    pub fork_num: ForkNumber, // was forkNum:8
    pub is_temp: bool,        // was is_temp:1
    pub skip_fsync: bool,     // was skip_fsync:1
}

/// Information identifying what the IO is being performed on.
/// C is a union; only the smgr member exists, modeled idiomatically (in-memory).
pub enum PgAioTargetData {
    Smgr(PgAioTargetDataSmgr),
}

/// The status of an AIO operation.
pub enum PgAioResultStatus {
    Unknown = 0, // not yet completed / uninitialized
    Ok,
    Partial, // did not fully succeed, no warning/error
    Warning, // [partially] succeeded, with a warning
    Error,   // failed entirely
}

pub const PGAIO_RESULT_ID_BITS: u32 = 6;
pub const PGAIO_RESULT_STATUS_BITS: u32 = 3;
pub const PGAIO_RESULT_ERROR_BITS: u32 = 23;

/// Result of IO operation, visible only to the initiator of IO.
/// On-disk-style packed word: `id:6 | status:3 | error_data:23` plus an i32.
/// The C struct is exactly 8 bytes; keep that layout with a bitfield newtype for
/// the first 32 bits followed by the i32 result.
#[repr(C)]
pub struct PgAioResult {
    bits: u32, // id:PGAIO_RESULT_ID_BITS | status:STATUS_BITS | error_data:ERROR_BITS
    pub result: i32,
}

impl PgAioResult {
    const ID_MASK: u32 = (1 << PGAIO_RESULT_ID_BITS) - 1;
    const STATUS_MASK: u32 = (1 << PGAIO_RESULT_STATUS_BITS) - 1;
    const ERROR_MASK: u32 = (1 << PGAIO_RESULT_ERROR_BITS) - 1;

    /// type is PgAioHandleCallbackID
    pub const fn id(&self) -> u32 {
        self.bits & Self::ID_MASK
    }
    pub const fn status(&self) -> u32 {
        (self.bits >> PGAIO_RESULT_ID_BITS) & Self::STATUS_MASK
    }
    pub const fn error_data(&self) -> u32 {
        (self.bits >> (PGAIO_RESULT_ID_BITS + PGAIO_RESULT_STATUS_BITS)) & Self::ERROR_MASK
    }
}

const _: () = assert!(
    PGAIO_RESULT_ID_BITS + PGAIO_RESULT_STATUS_BITS + PGAIO_RESULT_ERROR_BITS == 32,
    "PgAioResult bits divided up incorrectly"
);
const _: () = assert!(core::mem::size_of::<PgAioResult>() == 8, "PgAioResult has unexpected size");

/// Combination of PgAioResult with minimal metadata about the IO.
pub struct PgAioReturn {
    pub result: PgAioResult,
    pub target_data: PgAioTargetData,
}
