//! Translated from PostgreSQL src/include/storage/read_stream.h
//!
//! Mechanism for accessing buffered relation data with look-ahead.
#![allow(clippy::boxed_local, reason = "TODO(stub): drop when implemented; hollow stubs mirror PG signatures 1:1; real impl consumes params")]

use bitflags::bitflags;

use crate::common::relpath::ForkNumber;
use crate::storage::block::BlockNumber;
use crate::storage::buf::Buffer;
#[allow(deprecated)]
use crate::storage::buf::BufferAccessStrategy;
use crate::storage::smgr::SmgrRelation;
use crate::utils::rel::RelationData;

bitflags! {
    /// ReadStream tuning flags. C `#define READ_STREAM_*`.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct ReadStreamFlags: i32 {
        /// Default tuning, reasonable for many users.
        const DEFAULT = 0x00;
        /// Maintenance work; governed by maintenance_io_concurrency.
        const MAINTENANCE = 0x01;
        /// Disable automatic prefetch advice on detected sequential access.
        const SEQUENTIAL = 0x02;
        /// Read all available buffers; skip the ramp-up.
        const FULL = 0x04;
        /// Opt-in to using AIO batchmode.
        const USE_BATCHING = 0x08;
    }
}

/// ReadStream: opaque look-ahead read handle (details private to read_stream.c).
pub struct ReadStream;

/// Private state for `block_range_read_stream_cb`.
pub struct BlockRangeReadStreamPrivate {
    pub current_blocknum: BlockNumber,
    pub last_exclusive: BlockNumber,
}

/// Callback returning the next block number to read. C threads a `void
/// *callback_private_data` plus a `void *per_buffer_data`; both become closure
/// captures. `InvalidBlockNumber` (end of stream) -> `None`.
pub type ReadStreamBlockNumberCb<'a> =
    dyn FnMut(&mut ReadStream, &mut [u8]) -> Option<BlockNumber> + 'a;

pub fn block_range_read_stream_cb(
    _stream: &mut ReadStream,
    _callback_private_data: &mut BlockRangeReadStreamPrivate,
    _per_buffer_data: &mut [u8],
) -> Option<BlockNumber> {
    unimplemented!()
}

pub fn read_stream_begin_relation(
    _flags: ReadStreamFlags,
    _strategy: Option<BufferAccessStrategy>,
    _rel: &RelationData,
    _forknum: ForkNumber,
    _callback: Box<ReadStreamBlockNumberCb<'_>>,
    _per_buffer_data_size: usize,
) -> Box<ReadStream> {
    unimplemented!()
}

/// Returns the next buffer, or `None` at end of stream. `per_buffer_data` is the
/// callback-associated scratch slice for that buffer.
pub fn read_stream_next_buffer(_stream: &mut ReadStream) -> Option<(Buffer, &mut [u8])> {
    unimplemented!()
}

pub fn read_stream_next_block(
    _stream: &mut ReadStream,
    _strategy: &mut Option<BufferAccessStrategy>,
) -> Option<BlockNumber> {
    unimplemented!()
}

pub fn read_stream_begin_smgr_relation(
    _flags: ReadStreamFlags,
    _strategy: Option<BufferAccessStrategy>,
    _smgr: &mut SmgrRelation,
    _smgr_persistence: u8,
    _forknum: ForkNumber,
    _callback: Box<ReadStreamBlockNumberCb<'_>>,
    _per_buffer_data_size: usize,
) -> Box<ReadStream> {
    unimplemented!()
}

pub fn read_stream_reset(_stream: &mut ReadStream) {
    unimplemented!()
}

pub fn read_stream_end(_stream: Box<ReadStream>) {
    unimplemented!()
}
