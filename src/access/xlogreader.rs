//! Translated from PostgreSQL src/include/access/xlogreader.h
//! Definitions for the generic XLog reading facility.
//!
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "TODO(error-migration): pre-existing backlog; new code uses OrElog/?/crate::assert!"
)]
#![allow(clippy::boxed_local, reason = "TODO(stub): drop when implemented; hollow stubs mirror PG signatures 1:1; real impl consumes params")]
//!
//! Allocate an XLogReaderState, position it with begin_read/find_next_record,
//! then call read_record until it returns None. In-memory facility (no on-disk
//! layout contract): modelled idiomatically.

use crate::access::transam::FullTransactionId;
use crate::access::xlogrecord::XLogRecord;
use crate::access::xlogdefs::{RepOriginId, TimeLineID, XLogRecPtr, XLogSegNo};
use crate::c::TransactionId;
use crate::common::relpath::ForkNumber;
use crate::pg_config_manual::MAXPGPATH;
use crate::storage::block::BlockNumber;
use crate::storage::buf::Buffer;
use crate::storage::relfilelocator::RelFileLocator;

/// A WAL segment being read.
pub struct WALOpenSegment {
    pub file: i32,        // segment file descriptor (TODO: std::fs::File)
    pub segno: XLogSegNo, // segment number
    pub tli: TimeLineID,  // timeline ID of the currently open file
}

/// Context information about the WAL segments to read.
pub struct WALSegmentContext {
    pub dir: [u8; MAXPGPATH],
    pub segsize: i32,
}

// Function pointer typedefs (XLogPageReadCB, WALSegmentOpenCB,
// WALSegmentCloseCB) -> the XLogReaderRoutine trait below.

/// Operational callbacks for an XLogReader (routine struct -> trait).
///
/// `segment_open`/`segment_close` are required (base trait). `page_read` may be
/// NULL in C when the caller never calls read_record/find_next_record; that
/// optional capability is the `PageRead` supertrait.
pub trait XLogReaderRoutine {
    /// Open the specified WAL segment; sets seg.file. Raises on failure.
    /// `tli` is in/out: caller passes the desired timeline, callee may return
    /// the TLI it actually opened. Returns the (possibly updated) TLI.
    fn segment_open(&self, state: &mut XLogReaderState, next_seg_no: XLogSegNo, tli: TimeLineID)
        -> TimeLineID;

    /// Close the open WAL segment; sets seg.file negative.
    fn segment_close(&self, state: &mut XLogReaderState);
}

/// Optional data-input callback (`page_read`). Read at least `req_len` valid
/// bytes of the page at `target_page_ptr` into `read_buf`; return bytes read
/// (<= XLOG_BLCKSZ) or an error/would-block (XLogPageReadResult).
pub trait PageRead: XLogReaderRoutine {
    fn page_read(
        &self,
        state: &mut XLogReaderState,
        target_page_ptr: XLogRecPtr,
        req_len: i32,
        target_rec_ptr: XLogRecPtr,
        read_buf: &mut [u8],
    ) -> i32;
}

/// One decoded backup block reference within a record.
#[derive(Clone)]
pub struct DecodedBkpBlock {
    pub in_use: bool,

    // Identify the block this refers to.
    pub rlocator: RelFileLocator,
    pub forknum: ForkNumber,
    pub blkno: BlockNumber,

    // Prefetching workspace.
    pub prefetch_buffer: Buffer,

    /// Copy of the fork_flags field from the XLogRecordBlockHeader.
    pub flags: u8,

    // Full-page image, if any.
    pub has_image: bool,   // has image, even for consistency checking
    pub apply_image: bool, // has image that should be restored
    pub bkp_image: Option<Vec<u8>>, // TODO(ptr): was char *, points into decode buffer
    pub hole_offset: u16,
    pub hole_length: u16,
    pub bimg_len: u16,
    pub bimg_info: u8,

    // Buffer holding the rmgr-specific data associated with this block.
    pub has_data: bool,
    pub data: Option<Vec<u8>>, // TODO(ptr): was char *, points into decode buffer
    pub data_len: u16,
    pub data_bufsz: u16,
}

/// The decoded contents of a record. In C this is one contiguous allocation
/// with main_data and per-block data trailing the header; here the trailing
/// data become owned/borrowed Rust values and the FAM `blocks[]` becomes a Vec.
#[derive(Clone)]
pub struct DecodedXLogRecord {
    // Private member used for resource management.
    pub size: usize,     // total size of decoded record
    pub oversized: bool, // outside the regular decode buffer?
    // struct DecodedXLogRecord *next -- decode queue link; modelled by the
    // queue Vec/Deque in XLogReaderState rather than an intrusive pointer.

    // Public members.
    pub lsn: XLogRecPtr,      // location
    pub next_lsn: XLogRecPtr, // location of next record
    pub header: XLogRecord,   // header
    pub record_origin: RepOriginId,
    pub toplevel_xid: TransactionId, // XID of top-level transaction
    pub main_data: Option<Vec<u8>>,  // record's main data portion; TODO(ptr)
    pub main_data_len: u32,          // main data portion's length
    pub max_block_id: i32,           // highest block_id in use (-1 if none)
    pub blocks: Vec<DecodedBkpBlock>,
}

/// Generic XLog reader state. See module docs for the usage protocol.
///
/// Per routine-struct.md the AM is dispatched statically; the concrete routine
/// type is a generic parameter rather than a stored `&dyn`, so the optional
/// `PageRead` capability stays reachable.
pub struct XLogReaderState<R = ()> {
    // Operational callbacks.
    pub routine: R,

    // ---- Public parameters ----
    /// System identifier of the xlog files; 0 if unknown/unimportant.
    pub system_identifier: u64,
    /// Opaque data for callbacks; not used by XLogReader. TODO(ptr): was void *.
    pub private_data: Option<Box<dyn core::any::Any>>,

    pub read_rec_ptr: XLogRecPtr, // start of last record read
    pub end_rec_ptr: XLogRecPtr,  // end+1 of last record read

    pub aborted_rec_ptr: XLogRecPtr,
    pub missing_contrec_ptr: XLogRecPtr,
    /// Set when XLP_FIRST_IS_OVERWRITE_CONTRECORD is found.
    pub overwritten_rec_ptr: XLogRecPtr,

    // ---- Decoded representation of current record ----
    pub decode_rec_ptr: XLogRecPtr, // start of last record decoded
    pub next_rec_ptr: XLogRecPtr,   // end+1 of last record decoded
    pub prev_rec_ptr: XLogRecPtr,   // start of previous record decoded

    /// Last record returned by read_record. TODO(ptr): was DecodedXLogRecord *.
    pub record: Option<Box<DecodedXLogRecord>>,

    // ---- private/internal state ----
    /// Circular buffer for decoded records (oversized records allocated apart).
    pub decode_buffer: Option<Vec<u8>>, // TODO(ptr): was char *
    pub decode_buffer_size: usize,
    pub free_decode_buffer: bool,
    pub decode_buffer_head: usize, // offset; data is read from the head
    pub decode_buffer_tail: usize, // offset; new data is written at the tail

    /// Queue of decoded records (was an intrusive linked list).
    pub decode_queue: std::collections::VecDeque<Box<DecodedXLogRecord>>,

    /// Buffer for the currently read page (XLOG_BLCKSZ bytes).
    pub read_buf: Vec<u8>,
    pub read_len: u32,

    pub segcxt: WALSegmentContext,
    pub seg: WALOpenSegment,
    pub segoff: u32,

    /// Beginning of prior page read and its TLI (timeline sanity checks).
    pub latest_page_ptr: XLogRecPtr,
    pub latest_page_tli: TimeLineID,

    pub curr_rec_ptr: XLogRecPtr, // beginning of the WAL record being read
    pub curr_tli: TimeLineID,     // timeline to read from, 0 if a lookup is required

    /// Safe point to read to in curr_tli if it is historical, else invalid.
    pub curr_tli_valid_until: XLogRecPtr,

    /// Next timeline to read once curr_tli_valid_until is reached.
    pub next_tli: TimeLineID,

    /// Expandable buffer for a record that crosses a page boundary.
    pub read_record_buf: Vec<u8>,
    pub read_record_buf_size: u32,

    pub errormsg_buf: Option<String>,
    pub errormsg_deferred: bool,

    /// Tell page_read not to block waiting for data.
    pub nonblocking: bool,
}

impl<R> XLogReaderState<R> {
    /// True if there are queued records or an error to return.
    pub fn has_queued_record_or_error(&self) -> bool {
        !self.decode_queue.is_empty() || self.errormsg_deferred
    }
}

/// Return values from page_read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum XLogPageReadResult {
    Success = 0,    // record is successfully read
    Fail = -1,      // failed during reading a record
    WouldBlock = -2, // nonblocking mode only, no data
}

/// Error info from WALRead, processable by both backend and frontend callers.
pub struct WALReadError {
    pub errno: i32,        // errno set by the last pread()
    pub off: i32,          // offset we tried to read from
    pub req: i32,          // bytes requested to be read
    pub read: i32,         // bytes read by the last read()
    pub seg: WALOpenSegment, // segment we tried to read from
}

// === functions ===

/// Allocate a new XLogReader. Returns None on out-of-memory (C returns NULL).
pub fn XLogReaderAllocate<R>(
    _wal_segment_size: i32,
    _waldir: &str,
    _routine: R,
    _private_data: Option<Box<dyn core::any::Any>>,
) -> Option<Box<XLogReaderState<R>>> {
    unimplemented!()
}

pub fn XLogReaderFree<R>(_state: Box<XLogReaderState<R>>) {
    unimplemented!()
}

/// Optionally provide a circular decoding buffer to allow readahead.
pub fn XLogReaderSetDecodeBuffer<R>(_state: &mut XLogReaderState<R>, _buffer: Vec<u8>, _size: usize) {
    unimplemented!()
}

/// Position the XLogReader to the given record.
pub fn XLogBeginRead<R>(_state: &mut XLogReaderState<R>, _rec_ptr: XLogRecPtr) {
    unimplemented!()
}

/// Find the next record at or after RecPtr; returns its location, or invalid.
pub fn XLogFindNextRecord<R>(_state: &mut XLogReaderState<R>, _rec_ptr: XLogRecPtr) -> XLogRecPtr {
    unimplemented!()
}

/// Read the next XLog record. Returns None on end-of-WAL or failure (with an
/// error message); C returned the record pointer and a char **errormsg.
pub fn XLogReadRecord<R>(
    _state: &mut XLogReaderState<R>,
) -> Result<Option<XLogRecord>, String> {
    unimplemented!()
}

/// Consume the next decoded record or error.
pub fn XLogNextRecord<R>(
    _state: &mut XLogReaderState<R>,
) -> Result<Option<Box<DecodedXLogRecord>>, String> {
    unimplemented!()
}

/// Release the previously returned record, if necessary; returns its end LSN.
pub fn XLogReleasePreviousRecord<R>(_state: &mut XLogReaderState<R>) -> XLogRecPtr {
    unimplemented!()
}

/// Try to read ahead, if there is data and space.
pub fn XLogReadAhead<R>(
    _state: &mut XLogReaderState<R>,
    _nonblocking: bool,
) -> Option<Box<DecodedXLogRecord>> {
    unimplemented!()
}

/// Validate a page header. `phdr` is the raw page bytes.
pub fn XLogReaderValidatePageHeader<R>(
    _state: &mut XLogReaderState<R>,
    _recptr: XLogRecPtr,
    _phdr: &[u8],
) -> bool {
    unimplemented!()
}

/// Forget error produced by XLogReaderValidatePageHeader.
pub fn XLogReaderResetError<R>(_state: &mut XLogReaderState<R>) {
    unimplemented!()
}

/// Read `count` bytes into `buf` starting at `startptr` on timeline `tli`.
/// Returns Ok(()) or Err with the WALReadError details (C out-param errinfo).
pub fn WALRead<R>(
    _state: &mut XLogReaderState<R>,
    _buf: &mut [u8],
    _startptr: XLogRecPtr,
    _count: usize,
    _tli: TimeLineID,
) -> Result<(), WALReadError> {
    unimplemented!()
}

// === decoding ===

// The concrete, self-contained reader and the standalone record decoder live in
// the backend module (the xlogreader.c body). Re-export them here so call sites
// `use crate::access::xlogreader::{XLogReader, decode_xlog_record}` resolve to the
// real implementation. The generic `XLogReaderState<R>` free functions below
// remain the (deferred) recovery routine-struct surface.
pub use crate::backend::access::transam::xlogreader::{decode_xlog_record, PageReadFn, XLogReader};

/// Upper bound on the bytes a decoded record occupies (C
/// `DecodeXLogRecordRequiredSpace`). Our decoder copies fragments into owned
/// `Vec`s rather than one arena, so this is an over-estimate used only for
/// pre-sizing; it mirrors the C accounting (fixed struct + max blocks array +
/// the raw payload + per-fragment alignment padding).
pub fn DecodeXLogRecordRequiredSpace(xl_tot_len: usize) -> usize {
    use crate::access::xlogrecord::XLR_MAX_BLOCK_ID;
    use crate::pg_config::MAXIMUM_ALIGNOF;
    let nblocks = XLR_MAX_BLOCK_ID as usize + 1;
    core::mem::size_of::<DecodedXLogRecord>()
        + core::mem::size_of::<DecodedBkpBlock>() * nblocks
        + xl_tot_len
        + (MAXIMUM_ALIGNOF - 1) * (nblocks + 2)
}

/// Decode `record` at `lsn` into `decoded`. Returns Ok(()) or Err(message).
pub fn DecodeXLogRecord<R>(
    _state: &mut XLogReaderState<R>,
    _decoded: &mut DecodedXLogRecord,
    _record: &XLogRecord,
    _lsn: XLogRecPtr,
) -> Result<(), String> {
    unimplemented!()
}

// === accessors for the most recently returned record (XLogRecGet* macros) ===
// These read decoder.record; modelled as methods on the decoded record.
impl DecodedXLogRecord {
    pub fn total_len(&self) -> u32 {
        self.header.tot_len
    }
    pub fn prev(&self) -> XLogRecPtr {
        self.header.prev
    }
    pub fn info(&self) -> u8 {
        self.header.info
    }
    pub fn rmid(&self) -> crate::access::rmgr::RmgrId {
        self.header.rmid
    }
    pub fn xid(&self) -> TransactionId {
        self.header.xid
    }
    pub fn origin(&self) -> RepOriginId {
        self.record_origin
    }
    pub fn top_xid(&self) -> TransactionId {
        self.toplevel_xid
    }
    pub fn data(&self) -> Option<&[u8]> {
        self.main_data.as_deref()
    }
    pub fn data_len(&self) -> u32 {
        self.main_data_len
    }
    pub fn has_any_block_refs(&self) -> bool {
        self.max_block_id >= 0
    }
    pub fn max_block_id(&self) -> i32 {
        self.max_block_id
    }
    pub fn block(&self, i: usize) -> &DecodedBkpBlock {
        &self.blocks[i]
    }
    pub fn has_block_ref(&self, block_id: i32) -> bool {
        self.max_block_id >= block_id && self.blocks[block_id as usize].in_use
    }
    pub fn has_block_image(&self, block_id: usize) -> bool {
        self.blocks[block_id].has_image
    }
    pub fn block_image_apply(&self, block_id: usize) -> bool {
        self.blocks[block_id].apply_image
    }
    pub fn has_block_data(&self, block_id: usize) -> bool {
        self.blocks[block_id].has_data
    }
}

pub fn XLogRecGetFullXid<R>(_record: &XLogReaderState<R>) -> FullTransactionId {
    unimplemented!()
}

// The XLogRecGet* / RestoreBlockImage accessors read the state's most recently
// decoded record; they delegate to the [`DecodedXLogRecord`] methods (the real
// implementation lives with the decoder in the backend module). The C macros take
// the reader and dereference `record->record`; here that is `state.record`.

/// Restore a full-page image for `block_id` into `page`. Returns true on success.
pub fn RestoreBlockImage<R>(
    record: &mut XLogReaderState<R>,
    block_id: u8,
    page: &mut [u8],
) -> bool {
    record
        .record
        .as_ref()
        .expect("no decoded record")
        .restore_block_image(block_id, page)
        .unwrap_or(false)
}

/// Return the rmgr-specific data for `block_id` (C returned ptr + out-param len).
pub fn XLogRecGetBlockData<R>(
    record: &mut XLogReaderState<R>,
    block_id: u8,
) -> Option<Vec<u8>> {
    record
        .record
        .as_ref()
        .expect("no decoded record")
        .get_block_data(block_id)
        .map(<[u8]>::to_vec)
}

/// Block tag for `block_id` as (rlocator, forknum, blknum). Panics if absent
/// (matches the C variant that ereports when the block ref is missing).
pub fn XLogRecGetBlockTag<R>(
    record: &mut XLogReaderState<R>,
    block_id: u8,
) -> (RelFileLocator, ForkNumber, BlockNumber) {
    record
        .record
        .as_ref()
        .expect("no decoded record")
        .get_block_tag(block_id)
}

/// Like XLogRecGetBlockTag, plus the prefetch buffer; None if no such block ref.
pub fn XLogRecGetBlockTagExtended<R>(
    record: &mut XLogReaderState<R>,
    block_id: u8,
) -> Option<(RelFileLocator, ForkNumber, BlockNumber, Buffer)> {
    record
        .record
        .as_ref()
        .expect("no decoded record")
        .get_block_tag_extended(block_id)
}
