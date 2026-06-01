//! Translation of postgres/src/backend/access/transam/xlogreader.c
//!   + merged public type/macro layer from
//!       postgres/src/include/access/xlogreader.h
//!       postgres/src/include/access/xlogrecord.h
//!   + the minimal slice of access/xlog_internal.h needed by the reader
//!     (XLogPageHeaderData, XLOG_PAGE_MAGIC, the XLByteToSeg / XLogSegmentOffset
//!      / XLogPageHeaderSize / XRecOffIsValid helpers).
//!
//! Generic WAL record READ + DECODE state machine.  This is the facility that
//! every rmgrdesc (`access/rmgrdesc/*desc.rs`) leans on: it defines the real
//! `XLogReaderState`, `DecodedXLogRecord`, and the `XLogRecGet*` / `XLogRecHas*`
//! accessors that those files currently stub with an opaque `c_void`.
//!
//! #include mapping:
//!   "access/transam.h"      -> crate::access::transam (TransactionId helpers,
//!                              FullTransactionId). XLogRecPtr/RmgrId/etc. are
//!                              defined locally here, matching the convention of
//!                              the sibling rmgrdesc files (xlogdefs.h / rmgr.h
//!                              are not yet ported as their own modules).
//!   "access/xlogrecord.h"   -> merged below (XLogRecord, the block headers, the
//!                              XLR_*/BKPBLOCK_*/BKPIMAGE_* flag consts).
//!   "access/xlog_internal.h"-> minimal page-header slice merged below.
//!   "common/pg_lzcompress.h"-> crate::common::pg_lzcompress::pglz_decompress.
//!   "port/pg_crc32c.h"      -> crate::port::pg_crc32c (INIT/COMP/FIN/EQ_CRC32C).
//!   "storage/block.h"       -> crate::storage::block::{BlockNumber, ...}.
//!   "replication/origin.h"  -> RepOriginId / InvalidRepOriginId (c.rs / local).
//!
//! WHAT IS REAL (the meat of the file):
//!   - The full byte-for-byte decode + validation logic:
//!       DecodeXLogRecord (the COPY_HEADER_FIELD pointer walk, block-header +
//!       full-page-image cross-checks, contiguous fragment copy with MAXALIGN
//!       padding), DecodeXLogRecordRequiredSpace, ValidXLogRecordHeader,
//!       ValidXLogRecord (CRC via COMP_CRC32C), XLogReaderValidatePageHeader.
//!   - Allocation / lifecycle: XLogReaderAllocate, XLogReaderFree,
//!       XLogReaderSetDecodeBuffer, allocate_recordbuf, WALOpenSegmentInit,
//!       XLogReadRecordAlloc (the circular decode-buffer placement).
//!   - The read/decode driver state machine: XLogBeginRead,
//!       XLogReleasePreviousRecord, XLogNextRecord, XLogReadRecord,
//!       XLogReadAhead, XLogDecodeNextRecord, ReadPageInternal,
//!       XLogFindNextRecord, ResetDecoder, XLogReaderResetError,
//!       XLogReaderInvalReadState, report_invalid_record.
//!   - The accessors: XLogRecGetData/DataLen/Info/Rmid/Xid/Origin/TopXid/...,
//!       XLogRecHasBlock*, XLogRecGetBlockData, XLogRecGetBlockTag(Extended),
//!       RestoreBlockImage (incl. the pglz hole/decompress path), WALRead.
//!
//! WHAT IS STUBBED (kept to the finest granularity, all genuinely unported):
//!   - The page_read / segment_open / segment_close callbacks are
//!     CALLER-SUPPLIED fn pointers and are invoked for real through
//!     `state.routine`.  No backend I/O is hidden behind them here.
//!   - WALRead's raw byte fetch uses libc `pread` (pg_pread).  The pgstat I/O
//!     timing wrappers (#ifndef FRONTEND) are dropped (no-ops).
//!   - LZ4 / ZSTD full-page-image decompression: this build is !USE_LZ4 /
//!     !USE_ZSTD, so those branches report "not supported by build", exactly
//!     like the C #else path.  PGLZ decompression is REAL.
//!   - XLogRecGetFullXid (#ifndef FRONTEND): needs the running backend's
//!     TransamVariables->nextXid; stubbed with unimplemented!().

use crate::prelude::*;

use crate::access::transam::{FullTransactionId, InvalidTransactionId};
use crate::common::pg_lzcompress::pglz_decompress;
use crate::utils::palloc::{MCXT_ALLOC_NO_OOM, MCXT_ALLOC_ZERO};
use crate::port::pg_crc32c::{pg_crc32c, COMP_CRC32C, EQ_CRC32C, FIN_CRC32C, INIT_CRC32C};
use crate::storage::block::BlockNumber;

extern "C" {
    fn memcpy(dest: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
    fn memset(s: *mut c_void, c: c_int, n: usize) -> *mut c_void;
    fn snprintf(s: *mut c_char, n: usize, fmt: *const c_char, ...) -> c_int;
    /// pg_pread() -> libc pread(2). off_t is i64 on the LP64 targets we build for.
    fn pread(fd: c_int, buf: *mut c_void, count: usize, offset: i64) -> isize;
    /// errno access (thread-local). __error / __errno_location differ by OS; this
    /// build targets macOS/Darwin where it is `__error`.
    fn __error() -> *mut c_int;
}

// ===========================================================================
// Base types (xlogdefs.h / rmgr.h / replication-origin.h, not yet their own
// modules; defined locally to match the sibling rmgrdesc / xlog files).
// ===========================================================================

/// xlogdefs.h: a WAL record pointer is a 64-bit byte offset into the logical
/// WAL stream.
pub type XLogRecPtr = uint64;
/// xlogdefs.h: InvalidXLogRecPtr is zero.
pub const InvalidXLogRecPtr: XLogRecPtr = 0;
/// xlogdefs.h: segment number.
pub type XLogSegNo = uint64;
/// xlogdefs.h: timeline ID.
pub type TimeLineID = uint32;
/// rmgr.h: resource manager ID.
pub type RmgrId = uint8;
/// replication/origin.h: replication origin ID.
pub type RepOriginId = uint16;
/// replication/origin.h: InvalidRepOriginId.
pub const InvalidRepOriginId: RepOriginId = 0;
/// storage/buf.h: a buffer identifier (1-based; 0 == invalid). c_int per buf.h.
pub type Buffer = c_int;
/// storage/buf.h: InvalidBuffer.
pub const InvalidBuffer: Buffer = 0;
/// storage/relfilelocator.h ForkNumber (common/relpath.h). Defined locally as a
/// signed int (matches the C enum's representation); the reader only copies it.
pub type ForkNumber = c_int;

/// `XLogRecPtrIsInvalid(r)` (xlogdefs.h).
#[inline]
pub fn XLogRecPtrIsInvalid(r: XLogRecPtr) -> bool {
    r == InvalidXLogRecPtr
}

/// rmgr.h: RM_MAX_BUILTIN_ID == RM_NEXT_ID - 1.  The builtin rmgr list
/// (rmgrlist.h) is not ported; PostgreSQL 18 currently has 25 builtin rmgrs
/// (RM_NEXT_ID == 25), so the highest builtin id is 24.
pub const RM_MAX_BUILTIN_ID: c_int = 24;
/// rmgr.h: custom rmgr id range.
pub const RM_MIN_CUSTOM_ID: c_int = 128;
pub const RM_MAX_CUSTOM_ID: c_int = u8::MAX as c_int;

#[inline]
pub fn RmgrIdIsBuiltin(rmid: c_int) -> bool {
    rmid <= RM_MAX_BUILTIN_ID
}
#[inline]
pub fn RmgrIdIsCustom(rmid: c_int) -> bool {
    rmid >= RM_MIN_CUSTOM_ID && rmid <= RM_MAX_CUSTOM_ID
}
/// `RmgrIdIsValid(rmid)` (rmgr.h).
#[inline]
pub fn RmgrIdIsValid(rmid: RmgrId) -> bool {
    RmgrIdIsBuiltin(rmid as c_int) || RmgrIdIsCustom(rmid as c_int)
}

/// pg_control.h: RM_XLOG_ID is the first rmgr in rmgrlist.h (id 0).
pub const RM_XLOG_ID: RmgrId = 0;
/// pg_control.h: XLOG_SWITCH opcode (info byte, high nibble).
pub const XLOG_SWITCH: uint8 = 0x40;

/// xlog_internal.h: longest WAL file name (incl. NUL).
pub const MAXFNAMELEN: usize = 64;
/// path lengths (pg_config_manual.h).
pub const MAXPGPATH: usize = 1024;

// ===========================================================================
// XLogRecord + record-fragment headers (access/xlogrecord.h)
// ===========================================================================

/// The fixed-size header that begins every WAL record.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct XLogRecord {
    /// total len of entire record
    pub xl_tot_len: uint32,
    /// xact id
    pub xl_xid: TransactionId,
    /// ptr to previous record in log
    pub xl_prev: XLogRecPtr,
    /// flag bits, see XLR_* below
    pub xl_info: uint8,
    /// resource manager for this record
    pub xl_rmid: RmgrId,
    /* 2 bytes of padding here, initialize to zero */
    pub _padding: [uint8; 2],
    /// CRC for this record
    pub xl_crc: pg_crc32c,
}

/// `SizeOfXLogRecord` == offsetof(XLogRecord, xl_crc) + sizeof(pg_crc32c).
/// Note: this is the wire size and the CRC-coverage boundary; we use the
/// explicit offset rather than size_of so trailing padding can never creep in.
pub const SizeOfXLogRecord: usize =
    core::mem::offset_of!(XLogRecord, xl_crc) + core::mem::size_of::<pg_crc32c>();

/* high 4 bits free for rmgr; low 4 bits are XLogInsert-set / reserved. */
pub const XLR_INFO_MASK: uint8 = 0x0F;
pub const XLR_RMGR_INFO_MASK: uint8 = 0xF0;

/// XLogReader must allocate the whole record in one chunk, so a single record
/// can't exceed this (xlogrecord.h).
pub const XLogRecordMaxSize: usize = 1020 * 1024 * 1024;

pub const XLR_SPECIAL_REL_UPDATE: uint8 = 0x01;
pub const XLR_CHECK_CONSISTENCY: uint8 = 0x02;

/// Header for block data appended to a record.  NB: not aligned on the wire, so
/// it must be copied to aligned local storage before use.
#[repr(C, packed)]
#[derive(Clone, Copy)]
pub struct XLogRecordBlockHeader {
    /// block reference ID
    pub id: uint8,
    /// fork within the relation, and flags
    pub fork_flags: uint8,
    /// number of payload bytes (not including page image)
    pub data_length: uint16,
}

pub const SizeOfXLogRecordBlockHeader: usize =
    core::mem::offset_of!(XLogRecordBlockHeader, data_length) + core::mem::size_of::<uint16>();

/// Additional header when a full-page image is included (BKPBLOCK_HAS_IMAGE).
#[repr(C, packed)]
#[derive(Clone, Copy)]
pub struct XLogRecordBlockImageHeader {
    /// number of page image bytes
    pub length: uint16,
    /// number of bytes before "hole"
    pub hole_offset: uint16,
    /// flag bits, see BKPIMAGE_* below
    pub bimg_info: uint8,
}

pub const SizeOfXLogRecordBlockImageHeader: usize =
    core::mem::offset_of!(XLogRecordBlockImageHeader, bimg_info) + core::mem::size_of::<uint8>();

/* bimg_info bits */
pub const BKPIMAGE_HAS_HOLE: uint8 = 0x01;
pub const BKPIMAGE_APPLY: uint8 = 0x02;
pub const BKPIMAGE_COMPRESS_PGLZ: uint8 = 0x04;
pub const BKPIMAGE_COMPRESS_LZ4: uint8 = 0x08;
pub const BKPIMAGE_COMPRESS_ZSTD: uint8 = 0x10;

/// `BKPIMAGE_COMPRESSED(info)`.
#[inline]
pub fn BKPIMAGE_COMPRESSED(info: uint8) -> bool {
    (info & (BKPIMAGE_COMPRESS_PGLZ | BKPIMAGE_COMPRESS_LZ4 | BKPIMAGE_COMPRESS_ZSTD)) != 0
}

/// Extra header when a page image has a "hole" and is compressed.
#[repr(C, packed)]
#[derive(Clone, Copy)]
pub struct XLogRecordBlockCompressHeader {
    /// number of bytes in "hole"
    pub hole_length: uint16,
}

pub const SizeOfXLogRecordBlockCompressHeader: usize =
    core::mem::size_of::<XLogRecordBlockCompressHeader>();

/// fork_flags layout.
pub const BKPBLOCK_FORK_MASK: uint8 = 0x0F;
pub const BKPBLOCK_FLAG_MASK: uint8 = 0xF0;
pub const BKPBLOCK_HAS_IMAGE: uint8 = 0x10;
pub const BKPBLOCK_HAS_DATA: uint8 = 0x20;
pub const BKPBLOCK_WILL_INIT: uint8 = 0x40;
pub const BKPBLOCK_SAME_REL: uint8 = 0x80;

/// Main-data header, short form (data length < 256).
#[repr(C, packed)]
#[derive(Clone, Copy)]
pub struct XLogRecordDataHeaderShort {
    pub id: uint8, /* XLR_BLOCK_ID_DATA_SHORT */
    pub data_length: uint8,
}
pub const SizeOfXLogRecordDataHeaderShort: usize = core::mem::size_of::<uint8>() * 2;

/// Main-data header, long form.
#[repr(C, packed)]
#[derive(Clone, Copy)]
pub struct XLogRecordDataHeaderLong {
    pub id: uint8, /* XLR_BLOCK_ID_DATA_LONG; followed by unaligned uint32 */
}
pub const SizeOfXLogRecordDataHeaderLong: usize =
    core::mem::size_of::<uint8>() + core::mem::size_of::<uint32>();

/* block-id namespace */
pub const XLR_MAX_BLOCK_ID: usize = 32;
pub const XLR_BLOCK_ID_DATA_SHORT: uint8 = 255;
pub const XLR_BLOCK_ID_DATA_LONG: uint8 = 254;
pub const XLR_BLOCK_ID_ORIGIN: uint8 = 253;
pub const XLR_BLOCK_ID_TOPLEVEL_XID: uint8 = 252;

// ===========================================================================
// RelFileLocator (storage/relfilelocator.h, not yet its own module).  The
// reader only memcpys it from the wire and copies it out, so the exact field
// layout just needs to match the C struct's size.
// ===========================================================================

pub type Oid = u32;
pub type RelFileNumber = Oid;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct RelFileLocator {
    pub spcOid: Oid,
    pub dbOid: Oid,
    pub relNumber: RelFileNumber,
}

// ===========================================================================
// Page header slice (access/xlog_internal.h)
// ===========================================================================

pub const XLOG_BLCKSZ: usize = crate::pg_config::XLOG_BLCKSZ;
pub const BLCKSZ: usize = crate::pg_config::BLCKSZ;
pub const MAXIMUM_ALIGNOF: usize = crate::pg_config::MAXIMUM_ALIGNOF;

pub const XLOG_PAGE_MAGIC: uint16 = 0xD118;

/// Standard WAL page header.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct XLogPageHeaderData {
    pub xlp_magic: uint16,
    pub xlp_info: uint16,
    pub xlp_tli: TimeLineID,
    pub xlp_pageaddr: XLogRecPtr,
    pub xlp_rem_len: uint32,
}
pub type XLogPageHeader = *mut XLogPageHeaderData;

pub const SizeOfXLogShortPHD: usize = MAXALIGN(core::mem::size_of::<XLogPageHeaderData>());

/// "Long" header (first page of a WAL file).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct XLogLongPageHeaderData {
    pub std: XLogPageHeaderData,
    pub xlp_sysid: uint64,
    pub xlp_seg_size: uint32,
    pub xlp_xlog_blcksz: uint32,
}
pub type XLogLongPageHeader = *mut XLogLongPageHeaderData;

pub const SizeOfXLogLongPHD: usize = MAXALIGN(core::mem::size_of::<XLogLongPageHeaderData>());

/* xlp_info flag bits */
pub const XLP_FIRST_IS_CONTRECORD: uint16 = 0x0001;
pub const XLP_LONG_HEADER: uint16 = 0x0002;
pub const XLP_BKP_REMOVABLE: uint16 = 0x0004;
pub const XLP_FIRST_IS_OVERWRITE_CONTRECORD: uint16 = 0x0008;
pub const XLP_ALL_FLAGS: uint16 = 0x000F;

/// `XLogPageHeaderSize(hdr)`.
///
/// # Safety
/// `hdr` must point to a readable XLogPageHeaderData.
#[inline]
pub unsafe fn XLogPageHeaderSize(hdr: XLogPageHeader) -> usize {
    if (*hdr).xlp_info & XLP_LONG_HEADER != 0 {
        SizeOfXLogLongPHD
    } else {
        SizeOfXLogShortPHD
    }
}

/// `XLogSegmentsPerXLogId(wal_segsz_bytes)`.
#[inline]
pub fn XLogSegmentsPerXLogId(wal_segsz_bytes: usize) -> u64 {
    0x1_0000_0000u64 / (wal_segsz_bytes as u64)
}
/// `XLByteToSeg(xlrp, wal_segsz_bytes)` -> segment number.
#[inline]
pub fn XLByteToSeg(xlrp: XLogRecPtr, wal_segsz_bytes: usize) -> XLogSegNo {
    xlrp / (wal_segsz_bytes as u64)
}
/// `XLogSegmentOffset(xlogptr, wal_segsz_bytes)`.
#[inline]
pub fn XLogSegmentOffset(xlogptr: XLogRecPtr, wal_segsz_bytes: usize) -> uint32 {
    (xlogptr & ((wal_segsz_bytes as u64) - 1)) as uint32
}
/// `XLByteInSeg(xlrp, logSegNo, wal_segsz_bytes)`.
#[inline]
pub fn XLByteInSeg(xlrp: XLogRecPtr, log_seg_no: XLogSegNo, wal_segsz_bytes: usize) -> bool {
    (xlrp / (wal_segsz_bytes as u64)) == log_seg_no
}
/// `XRecOffIsValid(xlrp)`.
#[inline]
pub fn XRecOffIsValid(xlrp: XLogRecPtr) -> bool {
    (xlrp % (XLOG_BLCKSZ as u64)) >= (SizeOfXLogShortPHD as u64)
}

/// `XLogFileName(tli, logSegNo, wal_segsz_bytes)` rendered into a CString-style
/// fixed buffer; returns the formatted name as an owned Rust String for use in
/// error messages (the C code writes into a `char fname[MAXFNAMELEN]` stack
/// buffer; here we format directly).
fn XLogFileName(tli: TimeLineID, log_seg_no: XLogSegNo, wal_segsz_bytes: usize) -> String {
    let per = XLogSegmentsPerXLogId(wal_segsz_bytes);
    format!(
        "{:08X}{:08X}{:08X}",
        tli,
        (log_seg_no / per) as uint32,
        (log_seg_no % per) as uint32
    )
}

/// `LSN_FORMAT_ARGS(lsn)` produces the classic `%X/%X` pair; here we format the
/// whole pointer in that style.
#[inline]
fn lsn_fmt(lsn: XLogRecPtr) -> String {
    format!("{:X}/{:X}", (lsn >> 32) as uint32, lsn as uint32)
}

// ===========================================================================
// Decoded representation (xlogreader.h)
// ===========================================================================

/// A decoded block reference within a record.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct DecodedBkpBlock {
    /// Is this block ref in use?
    pub in_use: bool,

    /* Identify the block this refers to */
    pub rlocator: RelFileLocator,
    pub forknum: ForkNumber,
    pub blkno: BlockNumber,

    /// Prefetching workspace.
    pub prefetch_buffer: Buffer,

    /// copy of the fork_flags field from the XLogRecordBlockHeader
    pub flags: uint8,

    /* Information on full-page image, if any */
    pub has_image: bool,
    pub apply_image: bool,
    pub bkp_image: *mut c_char,
    pub hole_offset: uint16,
    pub hole_length: uint16,
    pub bimg_len: uint16,
    pub bimg_info: uint8,

    /* Buffer holding the rmgr-specific data associated with this block */
    pub has_data: bool,
    pub data: *mut c_char,
    pub data_len: uint16,
    pub data_bufsz: uint16,
}

/// The decoded contents of a record.  Occupies a contiguous region of memory,
/// with `main_data` and `blocks[n].data` pointing past the fixed members; the
/// flexible `blocks` array is the LAST member, exactly as in C.
///
/// In Rust we cannot declare `blocks: [DecodedBkpBlock; FLEXIBLE_ARRAY_MEMBER]`,
/// so we declare it as a one-element array and index past it through the raw
/// over-allocated region (the same trick the C compiler uses).  Helpers
/// [`decoded_blocks_ptr`] / [`decoded_block`] hand out the right element.
#[repr(C)]
pub struct DecodedXLogRecord {
    /* Private member used for resource management. */
    /// total size of decoded record
    pub size: usize,
    /// outside the regular decode buffer?
    pub oversized: bool,
    /// decoded record queue link
    pub next: *mut DecodedXLogRecord,

    /* Public members. */
    /// location
    pub lsn: XLogRecPtr,
    /// location of next record
    pub next_lsn: XLogRecPtr,
    /// header
    pub header: XLogRecord,
    pub record_origin: RepOriginId,
    /// XID of top-level transaction
    pub toplevel_xid: TransactionId,
    /// record's main data portion
    pub main_data: *mut c_char,
    /// main data portion's length
    pub main_data_len: uint32,
    /// highest block_id in use (-1 if none)
    pub max_block_id: c_int,
    /// FLEXIBLE_ARRAY_MEMBER: blocks[0] declared, the rest live in the
    /// over-allocated tail.
    pub blocks: [DecodedBkpBlock; 1],
}

/// `offsetof(DecodedXLogRecord, blocks)`.
#[inline]
fn offsetof_decoded_blocks() -> usize {
    core::mem::offset_of!(DecodedXLogRecord, blocks)
}

/// Pointer to `decoded->blocks[i]`, walking the flexible array.
///
/// # Safety
/// `decoded` must point to a region with at least `i+1` block slots allocated.
#[inline]
unsafe fn decoded_block(decoded: *mut DecodedXLogRecord, i: usize) -> *mut DecodedBkpBlock {
    let base = (decoded as *mut u8).add(offsetof_decoded_blocks()) as *mut DecodedBkpBlock;
    base.add(i)
}

// ===========================================================================
// Reader callbacks + segment context (xlogreader.h)
// ===========================================================================

/// A WAL segment being read.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct WALOpenSegment {
    /// segment file descriptor (-1 if closed)
    pub ws_file: c_int,
    /// segment number
    pub ws_segno: XLogSegNo,
    /// timeline ID of the currently open file
    pub ws_tli: TimeLineID,
}

/// Context information about WAL segments to read.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct WALSegmentContext {
    pub ws_dir: [c_char; MAXPGPATH],
    pub ws_segsize: c_int,
}

/// Result of an XLogPageReadCB (xlogreader.h: enum XLogPageReadResult).
pub const XLREAD_SUCCESS: c_int = 0;
pub const XLREAD_FAIL: c_int = -1;
pub const XLREAD_WOULDBLOCK: c_int = -2;

/// page_read callback: read >= reqLen valid bytes of the page at targetPagePtr
/// into readBuf; return bytes read (<= XLOG_BLCKSZ), or a negative
/// XLREAD_* code on failure / would-block.
pub type XLogPageReadCB = unsafe fn(
    xlogreader: *mut XLogReaderState,
    targetPagePtr: XLogRecPtr,
    reqLen: c_int,
    targetRecPtr: XLogRecPtr,
    readBuf: *mut c_char,
) -> c_int;

/// segment_open callback: open the given segment; set ->seg.ws_file. On
/// failure it must raise an error and not return.  `tli_p` is in/out.
pub type WALSegmentOpenCB =
    unsafe fn(xlogreader: *mut XLogReaderState, nextSegNo: XLogSegNo, tli_p: *mut TimeLineID);

/// segment_close callback: close ->seg.ws_file (set it negative).
pub type WALSegmentCloseCB = unsafe fn(xlogreader: *mut XLogReaderState);

/// Operational callbacks supplied by the caller.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct XLogReaderRoutine {
    pub page_read: Option<XLogPageReadCB>,
    pub segment_open: Option<WALSegmentOpenCB>,
    pub segment_close: Option<WALSegmentCloseCB>,
}

/// Error info from WALRead (xlogreader.h).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct WALReadError {
    /// errno set by the last pg_pread()
    pub wre_errno: c_int,
    /// Offset we tried to read from.
    pub wre_off: c_int,
    /// Bytes requested to be read.
    pub wre_req: c_int,
    /// Bytes read by the last read().
    pub wre_read: c_int,
    /// Segment we tried to read from.
    pub wre_seg: WALOpenSegment,
}

/// The big WAL reader state.  See xlogreader.h for the field-by-field contract.
#[repr(C)]
pub struct XLogReaderState {
    /* Operational callbacks */
    pub routine: XLogReaderRoutine,

    /* Public parameters */
    pub system_identifier: uint64,
    pub private_data: *mut c_void,
    pub ReadRecPtr: XLogRecPtr,
    pub EndRecPtr: XLogRecPtr,
    pub abortedRecPtr: XLogRecPtr,
    pub missingContrecPtr: XLogRecPtr,
    pub overwrittenRecPtr: XLogRecPtr,

    /* Decoded representation of current record */
    pub DecodeRecPtr: XLogRecPtr,
    pub NextRecPtr: XLogRecPtr,
    pub PrevRecPtr: XLogRecPtr,
    pub record: *mut DecodedXLogRecord,

    /* private/internal state: decode buffer */
    pub decode_buffer: *mut c_char,
    pub decode_buffer_size: usize,
    pub free_decode_buffer: bool,
    pub decode_buffer_head: *mut c_char,
    pub decode_buffer_tail: *mut c_char,

    /* decoded record queue */
    pub decode_queue_head: *mut DecodedXLogRecord,
    pub decode_queue_tail: *mut DecodedXLogRecord,

    /* current page */
    pub readBuf: *mut c_char,
    pub readLen: uint32,

    /* last read XLOG position for data in readBuf */
    pub segcxt: WALSegmentContext,
    pub seg: WALOpenSegment,
    pub segoff: uint32,

    /* prior page read + TLI (timeline sanity checks) */
    pub latestPagePtr: XLogRecPtr,
    pub latestPageTLI: TimeLineID,

    /* current record being read */
    pub currRecPtr: XLogRecPtr,
    pub currTLI: TimeLineID,
    pub currTLIValidUntil: XLogRecPtr,
    pub nextTLI: TimeLineID,

    /* reassembly buffer (record crossing a page boundary) */
    pub readRecordBuf: *mut c_char,
    pub readRecordBufSize: uint32,

    /* error message buffer */
    pub errormsg_buf: *mut c_char,
    pub errormsg_deferred: bool,

    /* don't block waiting for data */
    pub nonblocking: bool,
}

/// `XLogReaderHasQueuedRecordOrError(state)` (xlogreader.h inline).
///
/// # Safety
/// `state` must be a live reader.
#[inline]
pub unsafe fn XLogReaderHasQueuedRecordOrError(state: *mut XLogReaderState) -> bool {
    !(*state).decode_queue_head.is_null() || (*state).errormsg_deferred
}

// ===========================================================================
// File-local constants
// ===========================================================================

/// size of the buffer allocated for error message.
const MAX_ERRORMSG_LEN: usize = 1000;

/// Default decode buffer size.
const DEFAULT_DECODE_BUFFER_SIZE: usize = 64 * 1024;

// ===========================================================================
// report_invalid_record (the va-list error builder)
// ===========================================================================

/// Construct a string in `state->errormsg_buf` explaining what's wrong with the
/// current record.  In C this is a printf-style variadic; callers here build the
/// message with Rust formatting and pass the finished `String`.
///
/// # Safety
/// `state` must have a valid `errormsg_buf` of at least MAX_ERRORMSG_LEN+1 bytes.
unsafe fn report_invalid_record(state: *mut XLogReaderState, msg: String) {
    let buf = (*state).errormsg_buf;
    let bytes = msg.as_bytes();
    // vsnprintf truncates to MAX_ERRORMSG_LEN incl. NUL.
    let n = core::cmp::min(bytes.len(), MAX_ERRORMSG_LEN - 1);
    core::ptr::copy_nonoverlapping(bytes.as_ptr(), buf as *mut u8, n);
    *buf.add(n) = 0;
    (*state).errormsg_deferred = true;
}

// ===========================================================================
// Allocation / lifecycle
// ===========================================================================

/// Set the size of the decoding buffer, optionally pointing at caller memory.
///
/// # Safety
/// `state` is a live reader with `decode_buffer == NULL`.
pub unsafe fn XLogReaderSetDecodeBuffer(
    state: *mut XLogReaderState,
    buffer: *mut c_void,
    size: usize,
) {
    Assert!((*state).decode_buffer.is_null());

    (*state).decode_buffer = buffer as *mut c_char;
    (*state).decode_buffer_size = size;
    (*state).decode_buffer_tail = buffer as *mut c_char;
    (*state).decode_buffer_head = buffer as *mut c_char;
}

/// Allocate and initialize a new XLogReader.  Returns null on OOM.
///
/// # Safety
/// `routine` must point to a valid XLogReaderRoutine; `waldir` is a NUL-
/// terminated C string or null.
pub unsafe fn XLogReaderAllocate(
    wal_segment_size: c_int,
    waldir: *const c_char,
    routine: *const XLogReaderRoutine,
    private_data: *mut c_void,
) -> *mut XLogReaderState {
    let state = palloc_extended(
        core::mem::size_of::<XLogReaderState>(),
        MCXT_ALLOC_NO_OOM | MCXT_ALLOC_ZERO,
    ) as *mut XLogReaderState;
    if state.is_null() {
        return null_mut();
    }

    /* initialize caller-provided support functions */
    (*state).routine = *routine;

    /*
     * Permanently allocate readBuf (MAXALIGN'd via palloc_extended).
     */
    (*state).readBuf = palloc_extended(XLOG_BLCKSZ, MCXT_ALLOC_NO_OOM) as *mut c_char;
    if (*state).readBuf.is_null() {
        pfree(state as *mut c_void);
        return null_mut();
    }

    /* Initialize segment info. */
    WALOpenSegmentInit(
        &mut (*state).seg,
        &mut (*state).segcxt,
        wal_segment_size,
        waldir,
    );

    /* system_identifier initialized to zeroes above */
    (*state).private_data = private_data;
    /* ReadRecPtr, EndRecPtr and readLen initialized to zeroes above */
    (*state).errormsg_buf = palloc_extended(MAX_ERRORMSG_LEN + 1, MCXT_ALLOC_NO_OOM) as *mut c_char;
    if (*state).errormsg_buf.is_null() {
        pfree((*state).readBuf as *mut c_void);
        pfree(state as *mut c_void);
        return null_mut();
    }
    *(*state).errormsg_buf = 0;

    /*
     * Allocate an initial readRecordBuf of minimal size.
     */
    allocate_recordbuf(state, 0);
    state
}

/// Free an XLogReader.
///
/// # Safety
/// `state` was returned by [`XLogReaderAllocate`].
pub unsafe fn XLogReaderFree(state: *mut XLogReaderState) {
    if (*state).seg.ws_file != -1 {
        if let Some(close) = (*state).routine.segment_close {
            close(state);
        }
    }

    if !(*state).decode_buffer.is_null() && (*state).free_decode_buffer {
        pfree((*state).decode_buffer as *mut c_void);
    }

    pfree((*state).errormsg_buf as *mut c_void);
    if !(*state).readRecordBuf.is_null() {
        pfree((*state).readRecordBuf as *mut c_void);
    }
    pfree((*state).readBuf as *mut c_void);
    pfree(state as *mut c_void);
}

/// Allocate readRecordBuf to fit a record of at least `reclength`, rounded up to
/// a multiple of XLOG_BLCKSZ and at least 5*Max(BLCKSZ, XLOG_BLCKSZ).
///
/// # Safety
/// `state` is a live reader; must only be called after xl_tot_len is validated.
unsafe fn allocate_recordbuf(state: *mut XLogReaderState, reclength: uint32) {
    let mut new_size = reclength;

    new_size = new_size.wrapping_add(XLOG_BLCKSZ as uint32 - (new_size % XLOG_BLCKSZ as uint32));
    new_size = Max(new_size, 5u32.wrapping_mul(Max(BLCKSZ as uint32, XLOG_BLCKSZ as uint32)));

    if !(*state).readRecordBuf.is_null() {
        pfree((*state).readRecordBuf as *mut c_void);
    }
    (*state).readRecordBuf = palloc(new_size as usize) as *mut c_char;
    (*state).readRecordBufSize = new_size;
}

/// Initialize the passed segment structs.
///
/// # Safety
/// `seg`/`segcxt` are valid; `waldir` is NUL-terminated or null.
unsafe fn WALOpenSegmentInit(
    seg: *mut WALOpenSegment,
    segcxt: *mut WALSegmentContext,
    segsize: c_int,
    waldir: *const c_char,
) {
    (*seg).ws_file = -1;
    (*seg).ws_segno = 0;
    (*seg).ws_tli = 0;

    (*segcxt).ws_segsize = segsize;
    if !waldir.is_null() {
        snprintf(
            (*segcxt).ws_dir.as_mut_ptr(),
            MAXPGPATH,
            c"%s".as_ptr(),
            waldir,
        );
    }
}

// ===========================================================================
// Positioning + record queue management
// ===========================================================================

/// Begin reading WAL at `RecPtr`.  Does no I/O and cannot fail.
///
/// # Safety
/// `state` is a live reader; `RecPtr` is a valid (non-invalid) WAL pointer.
pub unsafe fn XLogBeginRead(state: *mut XLogReaderState, RecPtr: XLogRecPtr) {
    Assert!(!XLogRecPtrIsInvalid(RecPtr));

    ResetDecoder(state);

    /* Begin at the passed-in record pointer. */
    (*state).EndRecPtr = RecPtr;
    (*state).NextRecPtr = RecPtr;
    (*state).ReadRecPtr = InvalidXLogRecPtr;
    (*state).DecodeRecPtr = InvalidXLogRecPtr;
}

/// Release the last record returned by XLogNextRecord (if any), freeing space.
/// Returns the LSN past the end of that record.
///
/// # Safety
/// `state` is a live reader.
pub unsafe fn XLogReleasePreviousRecord(state: *mut XLogReaderState) -> XLogRecPtr {
    if (*state).record.is_null() {
        return InvalidXLogRecPtr;
    }

    /*
     * Remove it from the decoded record queue.  It must be the oldest item
     * decoded, decode_queue_head.
     */
    let mut record = (*state).record;
    let next_lsn = (*record).next_lsn;
    Assert!(record == (*state).decode_queue_head);
    (*state).record = null_mut();
    (*state).decode_queue_head = (*record).next;

    /* It might also be the newest item decoded, decode_queue_tail. */
    if (*state).decode_queue_tail == record {
        (*state).decode_queue_tail = null_mut();
    }

    /* Release the space. */
    if (*record).oversized {
        /* Not in the decode buffer, free it. */
        pfree(record as *mut c_void);
    } else {
        /* It must be the head (oldest) record in the decode buffer. */
        Assert!((*state).decode_buffer_head == record as *mut c_char);

        /*
         * Update head to point to the next record in the decode buffer, if
         * any, skipping oversized ones (they're not in the decode buffer).
         */
        record = (*record).next;
        while !record.is_null() && (*record).oversized {
            record = (*record).next;
        }

        if !record.is_null() {
            (*state).decode_buffer_head = record as *mut c_char;
        } else {
            /* Empty: reset head and tail to the start of the buffer. */
            (*state).decode_buffer_head = (*state).decode_buffer;
            (*state).decode_buffer_tail = (*state).decode_buffer;
        }
    }

    next_lsn
}

/// Consume the next queued record or error.  Returns null at end-of-queue, with
/// `*errormsg` set to a deferred error message (or null).
///
/// # Safety
/// `state` is a live reader; `errormsg` is a valid out-pointer.
pub unsafe fn XLogNextRecord(
    state: *mut XLogReaderState,
    errormsg: *mut *mut c_char,
) -> *mut DecodedXLogRecord {
    /* Release the last record returned by XLogNextRecord(). */
    XLogReleasePreviousRecord(state);

    if (*state).decode_queue_head.is_null() {
        *errormsg = null_mut();
        if (*state).errormsg_deferred {
            if *(*state).errormsg_buf != 0 {
                *errormsg = (*state).errormsg_buf;
            }
            (*state).errormsg_deferred = false;
        }

        Assert!(!XLogRecPtrIsInvalid((*state).EndRecPtr));

        return null_mut();
    }

    /*
     * Record this as the most recent record returned, so we'll release it next
     * time and so the XLogRecXXX(xlogreader) accessors can reach it.
     */
    (*state).record = (*state).decode_queue_head;

    (*state).ReadRecPtr = (*(*state).record).lsn;
    (*state).EndRecPtr = (*(*state).record).next_lsn;

    *errormsg = null_mut();

    (*state).record
}

/// Attempt to read an XLOG record.  Returns a pointer to the record header, or
/// null on end-of-WAL / failure (with `*errormsg` set on failure-with-detail).
///
/// # Safety
/// `state` is a live reader positioned via XLogBeginRead / XLogFindNextRecord;
/// `errormsg` is a valid out-pointer.
pub unsafe fn XLogReadRecord(
    state: *mut XLogReaderState,
    errormsg: *mut *mut c_char,
) -> *mut XLogRecord {
    /*
     * Release last returned record so we can check for an empty decode queue
     * accurately.
     */
    XLogReleasePreviousRecord(state);

    /*
     * Call XLogReadAhead() in blocking mode to make sure there is something in
     * the queue (the result is discarded here).
     */
    if !XLogReaderHasQueuedRecordOrError(state) {
        XLogReadAhead(state, false /* nonblocking */);
    }

    /* Consume the head record or error. */
    let decoded = XLogNextRecord(state, errormsg);
    if !decoded.is_null() {
        Assert!((*state).record == decoded);
        return &mut (*decoded).header as *mut XLogRecord;
    }

    null_mut()
}

/// Allocate space for a decoded record (initializing only `oversized`).  Returns
/// null if there's no decode-buffer space and `allow_oversized` is false, or on
/// palloc failure for an oversized buffer.
///
/// # Safety
/// `state` is a live reader.
unsafe fn XLogReadRecordAlloc(
    state: *mut XLogReaderState,
    xl_tot_len: usize,
    allow_oversized: bool,
) -> *mut DecodedXLogRecord {
    let required_space = DecodeXLogRecordRequiredSpace(xl_tot_len);
    let mut decoded: *mut DecodedXLogRecord = null_mut();

    /* Allocate a circular decode buffer if we don't have one already. */
    if (*state).decode_buffer.is_null() {
        if (*state).decode_buffer_size == 0 {
            (*state).decode_buffer_size = DEFAULT_DECODE_BUFFER_SIZE;
        }
        (*state).decode_buffer = palloc((*state).decode_buffer_size) as *mut c_char;
        (*state).decode_buffer_head = (*state).decode_buffer;
        (*state).decode_buffer_tail = (*state).decode_buffer;
        (*state).free_decode_buffer = true;
    }

    let buf = (*state).decode_buffer;
    let head = (*state).decode_buffer_head;
    let tail = (*state).decode_buffer_tail;

    /* Try to allocate space in the circular decode buffer. */
    if tail >= head {
        /* Empty, or tail is to the right of head. */
        if required_space <= (*state).decode_buffer_size - (tail.offset_from(buf) as usize) {
            /* Space between tail and end. */
            decoded = tail as *mut DecodedXLogRecord;
            (*decoded).oversized = false;
            return decoded;
        } else if required_space < (head.offset_from(buf) as usize) {
            /* Space between start and head. */
            decoded = buf as *mut DecodedXLogRecord;
            (*decoded).oversized = false;
            return decoded;
        }
    } else {
        /* Tail is to the left of head. */
        if required_space < (head.offset_from(tail) as usize) {
            /* Space between tail and head. */
            decoded = tail as *mut DecodedXLogRecord;
            (*decoded).oversized = false;
            return decoded;
        }
    }

    /* Not enough space in the decode buffer.  Are we allowed to allocate? */
    if allow_oversized {
        decoded = palloc(required_space) as *mut DecodedXLogRecord;
        (*decoded).oversized = true;
        return decoded;
    }

    null_mut()
}

// ===========================================================================
// The decode driver state machine
// ===========================================================================

/// Decode the next available record into the queue.  Returns an XLREAD_* code.
///
/// # Safety
/// `state` is a live reader with a page_read callback installed.
unsafe fn XLogDecodeNextRecord(state: *mut XLogReaderState, nonblocking: bool) -> c_int {
    let mut RecPtr: XLogRecPtr;
    let mut record: *mut XLogRecord;
    let mut targetPagePtr: XLogRecPtr;
    let mut randAccess: bool;
    let mut len: uint32;
    let mut total_len: uint32;
    let mut targetRecOff: uint32;
    let mut pageHeaderSize: usize;
    let mut assembled: bool;
    let mut gotheader: bool;
    let mut readOff: c_int;
    let mut decoded: *mut DecodedXLogRecord;
    let mut errormsg: *mut c_char = null_mut(); /* not used */

    randAccess = false;

    /* reset error state */
    *(*state).errormsg_buf = 0;
    decoded = null_mut();

    (*state).abortedRecPtr = InvalidXLogRecPtr;
    (*state).missingContrecPtr = InvalidXLogRecPtr;

    RecPtr = (*state).NextRecPtr;

    if (*state).DecodeRecPtr != InvalidXLogRecPtr {
        /* read the record after the one we just read */
    } else {
        /* Caller supplied a position to start at. */
        Assert!(RecPtr % XLOG_BLCKSZ as u64 == 0 || XRecOffIsValid(RecPtr));
        randAccess = true;
    }

    'restart: loop {
        (*state).nonblocking = nonblocking;
        (*state).currRecPtr = RecPtr;
        assembled = false;

        targetPagePtr = RecPtr - (RecPtr % XLOG_BLCKSZ as u64);
        targetRecOff = (RecPtr % XLOG_BLCKSZ as u64) as uint32;

        /*
         * Read the page containing the record into state->readBuf.  Request
         * enough to cover the whole record header (or the part on this page).
         */
        readOff = ReadPageInternal(
            state,
            targetPagePtr,
            Min(targetRecOff as usize + SizeOfXLogRecord, XLOG_BLCKSZ) as c_int,
        );
        if readOff == XLREAD_WOULDBLOCK {
            return XLREAD_WOULDBLOCK;
        } else if readOff < 0 {
            break 'restart;
        }

        /* ReadPageInternal always returns at least the page header. */
        pageHeaderSize = XLogPageHeaderSize((*state).readBuf as XLogPageHeader);
        if targetRecOff == 0 {
            /* At page start, skip over the page header. */
            RecPtr += pageHeaderSize as u64;
            targetRecOff = pageHeaderSize as uint32;
        } else if (targetRecOff as usize) < pageHeaderSize {
            report_invalid_record(
                state,
                format!(
                    "invalid record offset at {}: expected at least {}, got {}",
                    lsn_fmt(RecPtr),
                    pageHeaderSize,
                    targetRecOff
                ),
            );
            break 'restart;
        }

        if ((*((*state).readBuf as XLogPageHeader)).xlp_info & XLP_FIRST_IS_CONTRECORD) != 0
            && targetRecOff as usize == pageHeaderSize
        {
            report_invalid_record(
                state,
                format!("contrecord is requested by {}", lsn_fmt(RecPtr)),
            );
            break 'restart;
        }

        /* ReadPageInternal has verified the page header */
        Assert!(pageHeaderSize <= readOff as usize);

        /*
         * Read the record length.  xl_tot_len is the first field, so it's on
         * this page; other fields can't be touched until the whole header is
         * validated.
         */
        record = ((*state).readBuf).add((RecPtr % XLOG_BLCKSZ as u64) as usize) as *mut XLogRecord;
        total_len = (*record).xl_tot_len;

        if targetRecOff as usize <= XLOG_BLCKSZ - SizeOfXLogRecord {
            if !ValidXLogRecordHeader(state, RecPtr, (*state).DecodeRecPtr, record, randAccess) {
                break 'restart;
            }
            gotheader = true;
        } else {
            if (total_len as usize) < SizeOfXLogRecord {
                report_invalid_record(
                    state,
                    format!(
                        "invalid record length at {}: expected at least {}, got {}",
                        lsn_fmt(RecPtr),
                        SizeOfXLogRecord as uint32,
                        total_len
                    ),
                );
                break 'restart;
            }
            gotheader = false;
        }

        /*
         * Try to find space to decode this record without palloc.  If we can't,
         * we'll retry below after validating total_len.
         */
        decoded = XLogReadRecordAlloc(state, total_len as usize, false /* allow_oversized */);
        if decoded.is_null() && nonblocking {
            return XLREAD_WOULDBLOCK;
        }

        len = XLOG_BLCKSZ as uint32 - (RecPtr % XLOG_BLCKSZ as u64) as uint32;
        if total_len > len {
            /* Need to reassemble record. */
            let mut contdata: *mut c_char;
            let mut pageHeader: XLogPageHeader;
            let mut buffer: *mut c_char;
            let mut gotlen: uint32;

            assembled = true;

            Assert!((*state).readRecordBufSize as usize >= XLOG_BLCKSZ * 2);
            Assert!((*state).readRecordBufSize >= len);

            /* Copy the first fragment from the first page. */
            memcpy(
                (*state).readRecordBuf as *mut c_void,
                ((*state).readBuf).add((RecPtr % XLOG_BLCKSZ as u64) as usize) as *const c_void,
                len as usize,
            );
            buffer = ((*state).readRecordBuf).add(len as usize);
            gotlen = len;

            loop {
                /* beginning of next page */
                targetPagePtr += XLOG_BLCKSZ as u64;

                readOff = ReadPageInternal(state, targetPagePtr, SizeOfXLogShortPHD as c_int);
                if readOff == XLREAD_WOULDBLOCK {
                    return XLREAD_WOULDBLOCK;
                } else if readOff < 0 {
                    break 'restart;
                }

                Assert!(SizeOfXLogShortPHD <= readOff as usize);

                pageHeader = (*state).readBuf as XLogPageHeader;

                /*
                 * If we expected a continuation but got an "overwrite
                 * contrecord" flag, restart the read from here.
                 */
                if (*pageHeader).xlp_info & XLP_FIRST_IS_OVERWRITE_CONTRECORD != 0 {
                    (*state).overwrittenRecPtr = RecPtr;
                    RecPtr = targetPagePtr;
                    continue 'restart;
                }

                if (*pageHeader).xlp_info & XLP_FIRST_IS_CONTRECORD == 0 {
                    report_invalid_record(
                        state,
                        format!("there is no contrecord flag at {}", lsn_fmt(RecPtr)),
                    );
                    break 'restart;
                }

                /* Cross-check xlp_rem_len.  Use wrapping add to match C's
                 * unsigned-overflow semantics (debug builds would panic). */
                if (*pageHeader).xlp_rem_len == 0
                    || total_len != (*pageHeader).xlp_rem_len.wrapping_add(gotlen)
                {
                    report_invalid_record(
                        state,
                        format!(
                            "invalid contrecord length {} (expected {}) at {}",
                            (*pageHeader).xlp_rem_len,
                            (total_len as i64) - gotlen as i64,
                            lsn_fmt(RecPtr)
                        ),
                    );
                    break 'restart;
                }

                /* Wait for the next page to become available. */
                readOff = ReadPageInternal(
                    state,
                    targetPagePtr,
                    Min(
                        (total_len - gotlen) as usize + SizeOfXLogShortPHD,
                        XLOG_BLCKSZ,
                    ) as c_int,
                );
                if readOff == XLREAD_WOULDBLOCK {
                    return XLREAD_WOULDBLOCK;
                } else if readOff < 0 {
                    break 'restart;
                }

                pageHeaderSize = XLogPageHeaderSize(pageHeader);

                if (readOff as usize) < pageHeaderSize {
                    readOff = ReadPageInternal(state, targetPagePtr, pageHeaderSize as c_int);
                }

                Assert!(pageHeaderSize <= readOff as usize);

                contdata = ((*state).readBuf).add(pageHeaderSize);
                len = XLOG_BLCKSZ as uint32 - pageHeaderSize as uint32;
                if (*pageHeader).xlp_rem_len < len {
                    len = (*pageHeader).xlp_rem_len;
                }

                if (readOff as usize) < pageHeaderSize + len as usize {
                    readOff =
                        ReadPageInternal(state, targetPagePtr, (pageHeaderSize + len as usize) as c_int);
                }

                memcpy(
                    buffer as *mut c_void,
                    contdata as *const c_void,
                    len as usize,
                );
                buffer = buffer.add(len as usize);
                gotlen += len;

                /* If we just reassembled the record header, validate it. */
                if !gotheader {
                    record = (*state).readRecordBuf as *mut XLogRecord;
                    if !ValidXLogRecordHeader(state, RecPtr, (*state).DecodeRecPtr, record, randAccess)
                    {
                        break 'restart;
                    }
                    gotheader = true;
                }

                /* We might need a bigger buffer. */
                if total_len > (*state).readRecordBufSize {
                    let mut save_copy = [0u8; XLOG_BLCKSZ * 2];

                    Assert!(gotlen as usize <= save_copy.len());
                    Assert!(gotlen <= (*state).readRecordBufSize);
                    memcpy(
                        save_copy.as_mut_ptr() as *mut c_void,
                        (*state).readRecordBuf as *const c_void,
                        gotlen as usize,
                    );
                    allocate_recordbuf(state, total_len);
                    memcpy(
                        (*state).readRecordBuf as *mut c_void,
                        save_copy.as_ptr() as *const c_void,
                        gotlen as usize,
                    );
                    buffer = ((*state).readRecordBuf).add(gotlen as usize);
                }

                if gotlen >= total_len {
                    break;
                }
            }
            Assert!(gotheader);

            record = (*state).readRecordBuf as *mut XLogRecord;
            if !ValidXLogRecord(state, record, RecPtr) {
                break 'restart;
            }

            pageHeaderSize = XLogPageHeaderSize((*state).readBuf as XLogPageHeader);
            (*state).DecodeRecPtr = RecPtr;
            (*state).NextRecPtr =
                targetPagePtr + pageHeaderSize as u64 + MAXALIGN((*pageHeader).xlp_rem_len as usize) as u64;
        } else {
            /* Wait for the record data to become available. */
            readOff = ReadPageInternal(
                state,
                targetPagePtr,
                Min(targetRecOff as usize + total_len as usize, XLOG_BLCKSZ) as c_int,
            );
            if readOff == XLREAD_WOULDBLOCK {
                return XLREAD_WOULDBLOCK;
            } else if readOff < 0 {
                break 'restart;
            }

            /* Record does not cross a page boundary. */
            if !ValidXLogRecord(state, record, RecPtr) {
                break 'restart;
            }

            (*state).NextRecPtr = RecPtr + MAXALIGN(total_len as usize) as u64;
            (*state).DecodeRecPtr = RecPtr;
        }

        /* Special processing if it's an XLOG SWITCH record. */
        if (*record).xl_rmid == RM_XLOG_ID
            && ((*record).xl_info & !XLR_INFO_MASK) == XLOG_SWITCH
        {
            /* Pretend it extends to end of segment. */
            (*state).NextRecPtr += (*state).segcxt.ws_segsize as u64 - 1;
            (*state).NextRecPtr -=
                XLogSegmentOffset((*state).NextRecPtr, (*state).segcxt.ws_segsize as usize) as u64;
        }

        /* Allocate now (validated total_len) if we deferred above. */
        if decoded.is_null() {
            Assert!(!nonblocking);
            decoded = XLogReadRecordAlloc(state, total_len as usize, true /* allow_oversized */);
            Assert!(!decoded.is_null());
        }

        if DecodeXLogRecord(state, decoded, record, RecPtr, &mut errormsg) {
            /* Record the location of the next record. */
            (*decoded).next_lsn = (*state).NextRecPtr;

            /* If it's in the decode buffer, mark the space as occupied. */
            if !(*decoded).oversized {
                Assert!((*decoded).size == MAXALIGN((*decoded).size));
                if decoded as *mut c_char == (*state).decode_buffer {
                    (*state).decode_buffer_tail = ((*state).decode_buffer).add((*decoded).size);
                } else {
                    (*state).decode_buffer_tail =
                        ((*state).decode_buffer_tail).add((*decoded).size);
                }
            }

            /* Insert it into the queue of decoded records. */
            Assert!((*state).decode_queue_tail != decoded);
            if !(*state).decode_queue_tail.is_null() {
                (*(*state).decode_queue_tail).next = decoded;
            }
            (*state).decode_queue_tail = decoded;
            if (*state).decode_queue_head.is_null() {
                (*state).decode_queue_head = decoded;
            }
            return XLREAD_SUCCESS;
        }

        /* DecodeXLogRecord failed: fall through to err. */
        break 'restart;
    }

    // err:
    if assembled {
        /*
         * A multi-page record needed reassembly but something went wrong.  Note
         * the aborted record + the page where the contrecord went missing.
         */
        (*state).abortedRecPtr = RecPtr;
        (*state).missingContrecPtr = targetPagePtr;

        /*
         * If we got here without reporting an error, make sure one is queued so
         * the prefetcher won't bring us back and clobber the above state.
         */
        (*state).errormsg_deferred = true;
    }

    if !decoded.is_null() && (*decoded).oversized {
        pfree(decoded as *mut c_void);
    }

    XLogReaderInvalReadState(state);

    XLREAD_FAIL
}

/// Try to decode the next available record, queue it, and return it.  In
/// nonblocking mode may return null for lack of data or decode space.
///
/// # Safety
/// `state` is a live reader with a page_read callback installed.
pub unsafe fn XLogReadAhead(
    state: *mut XLogReaderState,
    nonblocking: bool,
) -> *mut DecodedXLogRecord {
    if (*state).errormsg_deferred {
        return null_mut();
    }

    let result = XLogDecodeNextRecord(state, nonblocking);
    if result == XLREAD_SUCCESS {
        Assert!(!(*state).decode_queue_tail.is_null());
        return (*state).decode_queue_tail;
    }

    null_mut()
}

/// Read a single xlog page (at least [pageptr, reqLen] valid bytes) via the
/// page_read callback.  Returns bytes read, or an XLREAD_* negative code.
///
/// # Safety
/// `state` is a live reader with a page_read callback installed.
unsafe fn ReadPageInternal(state: *mut XLogReaderState, pageptr: XLogRecPtr, reqLen: c_int) -> c_int {
    let mut readLen: c_int;
    let targetPageOff: uint32;
    let targetSegNo: XLogSegNo;
    let hdr: XLogPageHeader;

    Assert!((pageptr % XLOG_BLCKSZ as u64) == 0);

    let segsize = (*state).segcxt.ws_segsize as usize;
    targetSegNo = XLByteToSeg(pageptr, segsize);
    targetPageOff = XLogSegmentOffset(pageptr, segsize);

    /* check whether we have all the requested data already */
    if targetSegNo == (*state).seg.ws_segno
        && targetPageOff == (*state).segoff
        && (reqLen as u32) <= (*state).readLen
    {
        return (*state).readLen as c_int;
    }

    /*
     * Invalidate buffer contents (just length=0, keep the last segment) before
     * the read attempt.
     */
    (*state).readLen = 0;

    let page_read = (*state).routine.page_read.expect("page_read callback required");

    /*
     * On a new WAL segment, read+validate the first page's "long" header even
     * if that's not the target page.
     */
    if targetSegNo != (*state).seg.ws_segno && targetPageOff != 0 {
        let targetSegmentPtr = pageptr - targetPageOff as u64;

        readLen = page_read(
            state,
            targetSegmentPtr,
            XLOG_BLCKSZ as c_int,
            (*state).currRecPtr,
            (*state).readBuf,
        );
        if readLen == XLREAD_WOULDBLOCK {
            return XLREAD_WOULDBLOCK;
        } else if readLen < 0 {
            XLogReaderInvalReadState(state);
            return XLREAD_FAIL;
        }

        Assert!(readLen == XLOG_BLCKSZ as c_int);

        if !XLogReaderValidatePageHeader(state, targetSegmentPtr, (*state).readBuf) {
            XLogReaderInvalReadState(state);
            return XLREAD_FAIL;
        }
    }

    /*
     * First, read the requested length, but at least a short page header so we
     * can validate it.
     */
    readLen = page_read(
        state,
        pageptr,
        Max(reqLen as usize, SizeOfXLogShortPHD) as c_int,
        (*state).currRecPtr,
        (*state).readBuf,
    );
    if readLen == XLREAD_WOULDBLOCK {
        return XLREAD_WOULDBLOCK;
    } else if readLen < 0 {
        XLogReaderInvalReadState(state);
        return XLREAD_FAIL;
    }

    Assert!(readLen <= XLOG_BLCKSZ as c_int);

    /* Enough to check the header length? */
    if readLen as usize <= SizeOfXLogShortPHD {
        XLogReaderInvalReadState(state);
        return XLREAD_FAIL;
    }

    Assert!(readLen >= reqLen);

    hdr = (*state).readBuf as XLogPageHeader;

    /* still not enough */
    if (readLen as usize) < XLogPageHeaderSize(hdr) {
        readLen = page_read(
            state,
            pageptr,
            XLogPageHeaderSize(hdr) as c_int,
            (*state).currRecPtr,
            (*state).readBuf,
        );
        if readLen == XLREAD_WOULDBLOCK {
            return XLREAD_WOULDBLOCK;
        } else if readLen < 0 {
            XLogReaderInvalReadState(state);
            return XLREAD_FAIL;
        }
    }

    /* Now we have the full header; validate it. */
    if !XLogReaderValidatePageHeader(state, pageptr, hdr as *mut c_char) {
        XLogReaderInvalReadState(state);
        return XLREAD_FAIL;
    }

    /* update read state information */
    (*state).seg.ws_segno = targetSegNo;
    (*state).segoff = targetPageOff;
    (*state).readLen = readLen as uint32;

    readLen
}

/// Invalidate the read state to force a re-read.
///
/// # Safety
/// `state` is a live reader.
unsafe fn XLogReaderInvalReadState(state: *mut XLogReaderState) {
    (*state).seg.ws_segno = 0;
    (*state).segoff = 0;
    (*state).readLen = 0;
}

// ===========================================================================
// Header / CRC validation (the meat)
// ===========================================================================

/// Validate an XLOG record header.
///
/// # Safety
/// `record` points to at least a fully-read XLogRecord.
unsafe fn ValidXLogRecordHeader(
    state: *mut XLogReaderState,
    RecPtr: XLogRecPtr,
    PrevRecPtr: XLogRecPtr,
    record: *mut XLogRecord,
    randAccess: bool,
) -> bool {
    if ((*record).xl_tot_len as usize) < SizeOfXLogRecord {
        report_invalid_record(
            state,
            format!(
                "invalid record length at {}: expected at least {}, got {}",
                lsn_fmt(RecPtr),
                SizeOfXLogRecord as uint32,
                (*record).xl_tot_len
            ),
        );
        return false;
    }
    if !RmgrIdIsValid((*record).xl_rmid) {
        report_invalid_record(
            state,
            format!(
                "invalid resource manager ID {} at {}",
                (*record).xl_rmid,
                lsn_fmt(RecPtr)
            ),
        );
        return false;
    }
    if randAccess {
        /* Can't fully verify prev-link, but it must be < our own address. */
        if !((*record).xl_prev < RecPtr) {
            report_invalid_record(
                state,
                format!(
                    "record with incorrect prev-link {} at {}",
                    lsn_fmt((*record).xl_prev),
                    lsn_fmt(RecPtr)
                ),
            );
            return false;
        }
    } else {
        /* Prev-link must exactly match our previous location. */
        if (*record).xl_prev != PrevRecPtr {
            report_invalid_record(
                state,
                format!(
                    "record with incorrect prev-link {} at {}",
                    lsn_fmt((*record).xl_prev),
                    lsn_fmt(RecPtr)
                ),
            );
            return false;
        }
    }

    true
}

/// CRC-check an XLOG record.  Assumes the full xl_tot_len bytes are in memory at
/// `record` and that ValidXLogRecordHeader has already accepted the header.
///
/// # Safety
/// `record` points to at least xl_tot_len readable bytes.
unsafe fn ValidXLogRecord(
    state: *mut XLogReaderState,
    record: *mut XLogRecord,
    recptr: XLogRecPtr,
) -> bool {
    Assert!((*record).xl_tot_len as usize >= SizeOfXLogRecord);

    /* Calculate the CRC */
    let mut crc = INIT_CRC32C();
    crc = COMP_CRC32C(
        crc,
        (record as *const c_char).add(SizeOfXLogRecord) as *const c_void,
        (*record).xl_tot_len as usize - SizeOfXLogRecord,
    );
    /* include the record header last */
    crc = COMP_CRC32C(
        crc,
        record as *const c_void,
        core::mem::offset_of!(XLogRecord, xl_crc),
    );
    crc = FIN_CRC32C(crc);

    if !EQ_CRC32C((*record).xl_crc, crc) {
        report_invalid_record(
            state,
            format!(
                "incorrect resource manager data checksum in record at {}",
                lsn_fmt(recptr)
            ),
        );
        return false;
    }

    true
}

/// Validate the header of the XLog page at position `recptr`.
///
/// # Safety
/// `phdr` points to a readable WAL page header.
pub unsafe fn XLogReaderValidatePageHeader(
    state: *mut XLogReaderState,
    recptr: XLogRecPtr,
    phdr: *mut c_char,
) -> bool {
    let segno: XLogSegNo;
    let offset: i32;
    let hdr = phdr as XLogPageHeader;

    Assert!((recptr % XLOG_BLCKSZ as u64) == 0);

    let segsize = (*state).segcxt.ws_segsize as usize;
    segno = XLByteToSeg(recptr, segsize);
    offset = XLogSegmentOffset(recptr, segsize) as i32;

    if (*hdr).xlp_magic != XLOG_PAGE_MAGIC {
        let fname = XLogFileName((*state).seg.ws_tli, segno, segsize);
        report_invalid_record(
            state,
            format!(
                "invalid magic number {:04X} in WAL segment {}, LSN {}, offset {}",
                (*hdr).xlp_magic,
                fname,
                lsn_fmt(recptr),
                offset
            ),
        );
        return false;
    }

    if ((*hdr).xlp_info & !XLP_ALL_FLAGS) != 0 {
        let fname = XLogFileName((*state).seg.ws_tli, segno, segsize);
        report_invalid_record(
            state,
            format!(
                "invalid info bits {:04X} in WAL segment {}, LSN {}, offset {}",
                (*hdr).xlp_info,
                fname,
                lsn_fmt(recptr),
                offset
            ),
        );
        return false;
    }

    if (*hdr).xlp_info & XLP_LONG_HEADER != 0 {
        let longhdr = hdr as XLogLongPageHeader;

        if (*state).system_identifier != 0 && (*longhdr).xlp_sysid != (*state).system_identifier {
            report_invalid_record(
                state,
                format!(
                    "WAL file is from different database system: WAL file database system identifier is {}, pg_control database system identifier is {}",
                    (*longhdr).xlp_sysid,
                    (*state).system_identifier
                ),
            );
            return false;
        } else if (*longhdr).xlp_seg_size != (*state).segcxt.ws_segsize as uint32 {
            report_invalid_record(
                state,
                "WAL file is from different database system: incorrect segment size in page header"
                    .to_string(),
            );
            return false;
        } else if (*longhdr).xlp_xlog_blcksz != XLOG_BLCKSZ as uint32 {
            report_invalid_record(
                state,
                "WAL file is from different database system: incorrect XLOG_BLCKSZ in page header"
                    .to_string(),
            );
            return false;
        }
    } else if offset == 0 {
        let fname = XLogFileName((*state).seg.ws_tli, segno, segsize);
        /* first page of file doesn't have a long header? */
        report_invalid_record(
            state,
            format!(
                "invalid info bits {:04X} in WAL segment {}, LSN {}, offset {}",
                (*hdr).xlp_info,
                fname,
                lsn_fmt(recptr),
                offset
            ),
        );
        return false;
    }

    /* Check that the address on the page agrees with what we expected. */
    if (*hdr).xlp_pageaddr != recptr {
        let fname = XLogFileName((*state).seg.ws_tli, segno, segsize);
        report_invalid_record(
            state,
            format!(
                "unexpected pageaddr {} in WAL segment {}, LSN {}, offset {}",
                lsn_fmt((*hdr).xlp_pageaddr),
                fname,
                lsn_fmt(recptr),
                offset
            ),
        );
        return false;
    }

    /*
     * TLI must never go backwards across successive pages.  Only verify for
     * pages later than the last remembered LSN.
     */
    if recptr > (*state).latestPagePtr {
        if (*hdr).xlp_tli < (*state).latestPageTLI {
            let fname = XLogFileName((*state).seg.ws_tli, segno, segsize);
            report_invalid_record(
                state,
                format!(
                    "out-of-sequence timeline ID {} (after {}) in WAL segment {}, LSN {}, offset {}",
                    (*hdr).xlp_tli,
                    (*state).latestPageTLI,
                    fname,
                    lsn_fmt(recptr),
                    offset
                ),
            );
            return false;
        }
    }
    (*state).latestPagePtr = recptr;
    (*state).latestPageTLI = (*hdr).xlp_tli;

    true
}

/// Forget an error produced by XLogReaderValidatePageHeader().
///
/// # Safety
/// `state` is a live reader.
pub unsafe fn XLogReaderResetError(state: *mut XLogReaderState) {
    *(*state).errormsg_buf = 0;
    (*state).errormsg_deferred = false;
}

/// Find the first record with an lsn >= RecPtr.  Positions the reader like
/// XLogBeginRead so the next XLogReadRecord reads the next valid record.
///
/// # Safety
/// `state` is a live reader with a page_read callback installed.
pub unsafe fn XLogFindNextRecord(state: *mut XLogReaderState, RecPtr: XLogRecPtr) -> XLogRecPtr {
    let mut tmpRecPtr: XLogRecPtr;
    let found: XLogRecPtr;
    let mut header: XLogPageHeader;
    let mut errormsg: *mut c_char = null_mut();

    Assert!(!XLogRecPtrIsInvalid(RecPtr));

    /* Make sure ReadPageInternal() can't return XLREAD_WOULDBLOCK. */
    (*state).nonblocking = false;

    /* skip over potential continuation data, possibly spanning pages */
    tmpRecPtr = RecPtr;
    'scan: loop {
        let targetPagePtr: XLogRecPtr;
        let targetRecOff: c_int;
        let pageHeaderSize: usize;
        let mut readLen: c_int;

        targetRecOff = (tmpRecPtr % XLOG_BLCKSZ as u64) as c_int;

        /* scroll back to page boundary */
        targetPagePtr = tmpRecPtr - targetRecOff as u64;

        /* Read the page containing the record */
        readLen = ReadPageInternal(state, targetPagePtr, targetRecOff);
        if readLen < 0 {
            XLogReaderInvalReadState(state);
            return InvalidXLogRecPtr;
        }

        header = (*state).readBuf as XLogPageHeader;

        pageHeaderSize = XLogPageHeaderSize(header);

        /* make sure we have enough data for the page header */
        readLen = ReadPageInternal(state, targetPagePtr, pageHeaderSize as c_int);
        if readLen < 0 {
            XLogReaderInvalReadState(state);
            return InvalidXLogRecPtr;
        }

        /* skip over potential continuation data */
        if (*header).xlp_info & XLP_FIRST_IS_CONTRECORD != 0 {
            /*
             * If the remaining continuation data spills past this page, read
             * the next page and try again.
             */
            if MAXALIGN((*header).xlp_rem_len as usize) >= (XLOG_BLCKSZ - pageHeaderSize) {
                tmpRecPtr = targetPagePtr + XLOG_BLCKSZ as u64;
            } else {
                /* The continuation record ends in this page. */
                tmpRecPtr =
                    targetPagePtr + pageHeaderSize as u64 + MAXALIGN((*header).xlp_rem_len as usize) as u64;
                break 'scan;
            }
        } else {
            tmpRecPtr = targetPagePtr + pageHeaderSize as u64;
            break 'scan;
        }
    }

    /*
     * tmpRecPtr now points at a valid XLogRecord (either the first record after
     * a page start, or just past a continuation).
     */
    XLogBeginRead(state, tmpRecPtr);
    while !XLogReadRecord(state, &mut errormsg).is_null() {
        /* past the record we've found, break out */
        if RecPtr <= (*state).ReadRecPtr {
            /* Rewind the reader to the beginning of the last record. */
            found = (*state).ReadRecPtr;
            XLogBeginRead(state, found);
            return found;
        }
    }

    XLogReaderInvalReadState(state);
    InvalidXLogRecPtr
}

// ===========================================================================
// WALRead helper (uses libc pread for the raw byte fetch)
// ===========================================================================

/// Helper to ease writing page_read callbacks.  Reads `count` bytes into `buf`
/// starting at `startptr` from timeline `tli`, using the caller's segment_open /
/// segment_close callbacks and pg_pread.  Returns false on error (filling
/// `errinfo`).
///
/// # Safety
/// `state` has segment_open/segment_close callbacks installed; `buf` is writable
/// for `count` bytes; `errinfo` is a valid out-pointer.
pub unsafe fn WALRead(
    state: *mut XLogReaderState,
    buf: *mut c_char,
    startptr: XLogRecPtr,
    count: Size,
    mut tli: TimeLineID,
    errinfo: *mut WALReadError,
) -> bool {
    let mut p: *mut c_char = buf;
    let mut recptr: XLogRecPtr = startptr;
    let mut nbytes: Size = count;

    while nbytes > 0 {
        let startoff: uint32;
        let segbytes: c_int;
        let readbytes: isize;

        startoff = XLogSegmentOffset(recptr, (*state).segcxt.ws_segsize as usize);

        /*
         * If the data isn't in an open segment, close the current one (if any)
         * and open the next via the caller's segment_open callback.
         */
        if (*state).seg.ws_file < 0
            || !XLByteInSeg(recptr, (*state).seg.ws_segno, (*state).segcxt.ws_segsize as usize)
            || tli != (*state).seg.ws_tli
        {
            if (*state).seg.ws_file >= 0 {
                if let Some(close) = (*state).routine.segment_close {
                    close(state);
                }
            }

            let nextSegNo = XLByteToSeg(recptr, (*state).segcxt.ws_segsize as usize);
            if let Some(open) = (*state).routine.segment_open {
                open(state, nextSegNo, &mut tli);
            }

            /* This shouldn't happen -- indicates a bug in segment_open. */
            Assert!((*state).seg.ws_file >= 0);

            /* Update the current segment info. */
            (*state).seg.ws_tli = tli;
            (*state).seg.ws_segno = nextSegNo;
        }

        /* How many bytes are within this segment? */
        if nbytes > ((*state).segcxt.ws_segsize as usize - startoff as usize) {
            segbytes = (*state).segcxt.ws_segsize - startoff as c_int;
        } else {
            segbytes = nbytes as c_int;
        }

        /* Reset errno first; eases reporting non-errno-affecting errors. */
        *__error() = 0;
        readbytes = pread(
            (*state).seg.ws_file,
            p as *mut c_void,
            segbytes as usize,
            startoff as i64,
        );

        if readbytes <= 0 {
            (*errinfo).wre_errno = *__error();
            (*errinfo).wre_req = segbytes;
            (*errinfo).wre_read = readbytes as c_int;
            (*errinfo).wre_off = startoff as c_int;
            (*errinfo).wre_seg = (*state).seg;
            return false;
        }

        /* Update state for read */
        recptr += readbytes as u64;
        nbytes -= readbytes as usize;
        p = p.add(readbytes as usize);
    }

    true
}

// ===========================================================================
// Decoding the data and block references in a record (the meat)
// ===========================================================================

/// Reset the decoder, forgetting all decoded records (freeing oversized ones).
///
/// # Safety
/// `state` is a live reader.
unsafe fn ResetDecoder(state: *mut XLogReaderState) {
    /* Reset the decoded record queue, freeing any oversized records. */
    let mut r = (*state).decode_queue_head;
    while !r.is_null() {
        (*state).decode_queue_head = (*r).next;
        if (*r).oversized {
            pfree(r as *mut c_void);
        }
        r = (*state).decode_queue_head;
    }
    (*state).decode_queue_tail = null_mut();
    (*state).decode_queue_head = null_mut();
    (*state).record = null_mut();

    /* Reset the decode buffer to empty. */
    (*state).decode_buffer_tail = (*state).decode_buffer;
    (*state).decode_buffer_head = (*state).decode_buffer;

    /* Clear error state. */
    *(*state).errormsg_buf = 0;
    (*state).errormsg_deferred = false;
}

/// Compute the maximum possible buffer space needed to decode a record with the
/// given `xl_tot_len`.  Pessimistic; assumes the maximum number of blocks.
pub fn DecodeXLogRecordRequiredSpace(xl_tot_len: usize) -> usize {
    let mut size: usize = 0;

    /* Fixed part of the decoded record struct (up to blocks[0]). */
    size += offsetof_decoded_blocks();
    /* Flexible blocks array of maximum possible size. */
    size += core::mem::size_of::<DecodedBkpBlock>() * (XLR_MAX_BLOCK_ID + 1);
    /* All the raw main and block data. */
    size += xl_tot_len;
    /* Padding before main_data. */
    size += MAXIMUM_ALIGNOF - 1;
    /* Padding before each block's data. */
    size += (MAXIMUM_ALIGNOF - 1) * (XLR_MAX_BLOCK_ID + 1);
    /* Padding at the end. */
    size += MAXIMUM_ALIGNOF - 1;

    size
}

/// Decode a record into `decoded` (a MAXALIGNed area with at least
/// DecodeXLogRecordRequiredSpace(record->xl_tot_len) bytes).  Only
/// `decoded->oversized` need be initialized beforehand.  Returns false on error
/// with `*errormsg` set.
///
/// # Safety
/// `record` holds a fully-read, CRC-validated XLogRecord of xl_tot_len bytes;
/// `decoded` is a sufficiently large, MAXALIGNed buffer.
pub unsafe fn DecodeXLogRecord(
    state: *mut XLogReaderState,
    decoded: *mut DecodedXLogRecord,
    record: *mut XLogRecord,
    lsn: XLogRecPtr,
    errormsg: *mut *mut c_char,
) -> bool {
    // These locals must be declared BEFORE copy_header_field! so the macro's
    // free `ptr`/`remaining` references resolve to them (macro_rules hygiene
    // resolves free identifiers at the definition site's scope).
    let mut ptr: *mut c_char;
    let mut out: *mut c_char;
    let mut remaining: uint32;
    let mut datatotal: uint32;
    let mut rlocator: *mut RelFileLocator = null_mut();
    let mut block_id: uint8 = 0;

    // COPY_HEADER_FIELD(_dst, _size): copy _size bytes from ptr, checking overrun.
    // Returns from the function with a shortdata error on underflow.
    macro_rules! copy_header_field {
        ($dst:expr, $size:expr) => {{
            let sz: usize = $size;
            if (remaining as usize) < sz {
                // shortdata_err
                report_invalid_record(
                    state,
                    format!(
                        "record with invalid length at {}",
                        lsn_fmt((*state).ReadRecPtr)
                    ),
                );
                *errormsg = (*state).errormsg_buf;
                return false;
            }
            memcpy($dst as *mut c_void, ptr as *const c_void, sz);
            ptr = ptr.add(sz);
            remaining -= sz as uint32;
        }};
    }

    (*decoded).header = *record;
    (*decoded).lsn = lsn;
    (*decoded).next = null_mut();
    (*decoded).record_origin = InvalidRepOriginId;
    (*decoded).toplevel_xid = InvalidTransactionId;
    (*decoded).main_data = null_mut();
    (*decoded).main_data_len = 0;
    (*decoded).max_block_id = -1;
    ptr = record as *mut c_char;
    ptr = ptr.add(SizeOfXLogRecord);
    remaining = (*record).xl_tot_len - SizeOfXLogRecord as uint32;

    /* Decode the headers */
    datatotal = 0;
    while remaining > datatotal {
        copy_header_field!(&mut block_id as *mut uint8, core::mem::size_of::<uint8>());

        if block_id == XLR_BLOCK_ID_DATA_SHORT {
            /* XLogRecordDataHeaderShort */
            let mut main_data_len: uint8 = 0;

            copy_header_field!(&mut main_data_len as *mut uint8, core::mem::size_of::<uint8>());

            (*decoded).main_data_len = main_data_len as uint32;
            datatotal += main_data_len as uint32;
            break; /* main data fragment is always last */
        } else if block_id == XLR_BLOCK_ID_DATA_LONG {
            /* XLogRecordDataHeaderLong */
            let mut main_data_len: uint32 = 0;

            copy_header_field!(&mut main_data_len as *mut uint32, core::mem::size_of::<uint32>());
            (*decoded).main_data_len = main_data_len;
            datatotal += main_data_len;
            break; /* main data fragment is always last */
        } else if block_id == XLR_BLOCK_ID_ORIGIN {
            copy_header_field!(
                &mut (*decoded).record_origin as *mut RepOriginId,
                core::mem::size_of::<RepOriginId>()
            );
        } else if block_id == XLR_BLOCK_ID_TOPLEVEL_XID {
            copy_header_field!(
                &mut (*decoded).toplevel_xid as *mut TransactionId,
                core::mem::size_of::<TransactionId>()
            );
        } else if (block_id as usize) <= XLR_MAX_BLOCK_ID {
            /* XLogRecordBlockHeader */
            let blk: *mut DecodedBkpBlock;
            let mut fork_flags: uint8 = 0;

            /* mark any intervening block IDs as not in use */
            let mut i = (*decoded).max_block_id + 1;
            while i < block_id as c_int {
                (*decoded_block(decoded, i as usize)).in_use = false;
                i += 1;
            }

            if (block_id as c_int) <= (*decoded).max_block_id {
                report_invalid_record(
                    state,
                    format!(
                        "out-of-order block_id {} at {}",
                        block_id,
                        lsn_fmt((*state).ReadRecPtr)
                    ),
                );
                *errormsg = (*state).errormsg_buf;
                return false;
            }
            (*decoded).max_block_id = block_id as c_int;

            blk = decoded_block(decoded, block_id as usize);
            (*blk).in_use = true;
            (*blk).apply_image = false;

            copy_header_field!(&mut fork_flags as *mut uint8, core::mem::size_of::<uint8>());
            (*blk).forknum = (fork_flags & BKPBLOCK_FORK_MASK) as ForkNumber;
            (*blk).flags = fork_flags;
            (*blk).has_image = (fork_flags & BKPBLOCK_HAS_IMAGE) != 0;
            (*blk).has_data = (fork_flags & BKPBLOCK_HAS_DATA) != 0;

            (*blk).prefetch_buffer = InvalidBuffer;

            copy_header_field!(&mut (*blk).data_len as *mut uint16, core::mem::size_of::<uint16>());
            /* cross-check that HAS_DATA is set iff data_length > 0 */
            if (*blk).has_data && (*blk).data_len == 0 {
                report_invalid_record(
                    state,
                    format!(
                        "BKPBLOCK_HAS_DATA set, but no data included at {}",
                        lsn_fmt((*state).ReadRecPtr)
                    ),
                );
                *errormsg = (*state).errormsg_buf;
                return false;
            }
            if !(*blk).has_data && (*blk).data_len != 0 {
                report_invalid_record(
                    state,
                    format!(
                        "BKPBLOCK_HAS_DATA not set, but data length is {} at {}",
                        (*blk).data_len,
                        lsn_fmt((*state).ReadRecPtr)
                    ),
                );
                *errormsg = (*state).errormsg_buf;
                return false;
            }
            datatotal += (*blk).data_len as uint32;

            if (*blk).has_image {
                copy_header_field!(&mut (*blk).bimg_len as *mut uint16, core::mem::size_of::<uint16>());
                copy_header_field!(
                    &mut (*blk).hole_offset as *mut uint16,
                    core::mem::size_of::<uint16>()
                );
                copy_header_field!(&mut (*blk).bimg_info as *mut uint8, core::mem::size_of::<uint8>());

                (*blk).apply_image = ((*blk).bimg_info & BKPIMAGE_APPLY) != 0;

                if BKPIMAGE_COMPRESSED((*blk).bimg_info) {
                    if (*blk).bimg_info & BKPIMAGE_HAS_HOLE != 0 {
                        copy_header_field!(
                            &mut (*blk).hole_length as *mut uint16,
                            core::mem::size_of::<uint16>()
                        );
                    } else {
                        (*blk).hole_length = 0;
                    }
                } else {
                    (*blk).hole_length = BLCKSZ as uint16 - (*blk).bimg_len;
                }
                datatotal += (*blk).bimg_len as uint32;

                /* cross-check HAS_HOLE invariants */
                if ((*blk).bimg_info & BKPIMAGE_HAS_HOLE) != 0
                    && ((*blk).hole_offset == 0
                        || (*blk).hole_length == 0
                        || (*blk).bimg_len as usize == BLCKSZ)
                {
                    report_invalid_record(
                        state,
                        format!(
                            "BKPIMAGE_HAS_HOLE set, but hole offset {} length {} block image length {} at {}",
                            (*blk).hole_offset,
                            (*blk).hole_length,
                            (*blk).bimg_len,
                            lsn_fmt((*state).ReadRecPtr)
                        ),
                    );
                    *errormsg = (*state).errormsg_buf;
                    return false;
                }

                if ((*blk).bimg_info & BKPIMAGE_HAS_HOLE) == 0
                    && ((*blk).hole_offset != 0 || (*blk).hole_length != 0)
                {
                    report_invalid_record(
                        state,
                        format!(
                            "BKPIMAGE_HAS_HOLE not set, but hole offset {} length {} at {}",
                            (*blk).hole_offset,
                            (*blk).hole_length,
                            lsn_fmt((*state).ReadRecPtr)
                        ),
                    );
                    *errormsg = (*state).errormsg_buf;
                    return false;
                }

                if BKPIMAGE_COMPRESSED((*blk).bimg_info) && (*blk).bimg_len as usize == BLCKSZ {
                    report_invalid_record(
                        state,
                        format!(
                            "BKPIMAGE_COMPRESSED set, but block image length {} at {}",
                            (*blk).bimg_len,
                            lsn_fmt((*state).ReadRecPtr)
                        ),
                    );
                    *errormsg = (*state).errormsg_buf;
                    return false;
                }

                if ((*blk).bimg_info & BKPIMAGE_HAS_HOLE) == 0
                    && !BKPIMAGE_COMPRESSED((*blk).bimg_info)
                    && (*blk).bimg_len as usize != BLCKSZ
                {
                    report_invalid_record(
                        state,
                        format!(
                            "neither BKPIMAGE_HAS_HOLE nor BKPIMAGE_COMPRESSED set, but block image length is {} at {}",
                            (*blk).data_len,
                            lsn_fmt((*state).ReadRecPtr)
                        ),
                    );
                    *errormsg = (*state).errormsg_buf;
                    return false;
                }
            }
            if (fork_flags & BKPBLOCK_SAME_REL) == 0 {
                copy_header_field!(
                    &mut (*blk).rlocator as *mut RelFileLocator,
                    core::mem::size_of::<RelFileLocator>()
                );
                rlocator = &mut (*blk).rlocator as *mut RelFileLocator;
            } else {
                if rlocator.is_null() {
                    report_invalid_record(
                        state,
                        format!(
                            "BKPBLOCK_SAME_REL set but no previous rel at {}",
                            lsn_fmt((*state).ReadRecPtr)
                        ),
                    );
                    *errormsg = (*state).errormsg_buf;
                    return false;
                }

                (*blk).rlocator = *rlocator;
            }
            copy_header_field!(&mut (*blk).blkno as *mut BlockNumber, core::mem::size_of::<BlockNumber>());
        } else {
            report_invalid_record(
                state,
                format!(
                    "invalid block_id {} at {}",
                    block_id,
                    lsn_fmt((*state).ReadRecPtr)
                ),
            );
            *errormsg = (*state).errormsg_buf;
            return false;
        }
    }

    if remaining != datatotal {
        report_invalid_record(
            state,
            format!(
                "record with invalid length at {}",
                lsn_fmt((*state).ReadRecPtr)
            ),
        );
        *errormsg = (*state).errormsg_buf;
        return false;
    }

    /*
     * Copy the data of each fragment to contiguous space after the blocks
     * array, inserting MAXALIGN padding before each data fragment.
     */
    out = (decoded as *mut c_char).add(
        offsetof_decoded_blocks()
            + core::mem::size_of::<DecodedBkpBlock>() * ((*decoded).max_block_id + 1) as usize,
    );

    /* block data first */
    let mut bid: c_int = 0;
    while bid <= (*decoded).max_block_id {
        let blk = decoded_block(decoded, bid as usize);

        if !(*blk).in_use {
            bid += 1;
            continue;
        }

        Assert!((*blk).has_image || !(*blk).apply_image);

        if (*blk).has_image {
            /* no need to align image */
            (*blk).bkp_image = out;
            memcpy(out as *mut c_void, ptr as *const c_void, (*blk).bimg_len as usize);
            ptr = ptr.add((*blk).bimg_len as usize);
            out = out.add((*blk).bimg_len as usize);
        }
        if (*blk).has_data {
            out = MAXALIGN(out as usize) as *mut c_char;
            (*blk).data = out;
            memcpy(
                (*blk).data as *mut c_void,
                ptr as *const c_void,
                (*blk).data_len as usize,
            );
            ptr = ptr.add((*blk).data_len as usize);
            out = out.add((*blk).data_len as usize);
        }

        bid += 1;
    }

    /* and finally, the main data */
    if (*decoded).main_data_len > 0 {
        out = MAXALIGN(out as usize) as *mut c_char;
        (*decoded).main_data = out;
        memcpy(
            (*decoded).main_data as *mut c_void,
            ptr as *const c_void,
            (*decoded).main_data_len as usize,
        );
        ptr = ptr.add((*decoded).main_data_len as usize);
        out = out.add((*decoded).main_data_len as usize);
    }
    let _ = ptr; /* final ptr advance is intentional but unused after this */

    /* Report the actual size we used. */
    (*decoded).size = MAXALIGN(out.offset_from(decoded as *mut c_char) as usize);
    Assert!(DecodeXLogRecordRequiredSpace((*record).xl_tot_len as usize) >= (*decoded).size);

    true
}

// ===========================================================================
// Block-reference accessors
// ===========================================================================

/// Returns info about the block a block reference refers to.  The reference
/// must exist (else it raises an error).
///
/// # Safety
/// `record` is a live reader whose current record is decoded.
pub unsafe fn XLogRecGetBlockTag(
    record: *mut XLogReaderState,
    block_id: uint8,
    rlocator: *mut RelFileLocator,
    forknum: *mut ForkNumber,
    blknum: *mut BlockNumber,
) {
    if !XLogRecGetBlockTagExtended(record, block_id, rlocator, forknum, blknum, null_mut()) {
        elog!(
            ERROR,
            "could not locate backup block with ID {} in WAL record",
            block_id
        );
        unreachable!();
    }
}

/// Returns info about the block a block reference refers to, optionally with the
/// prefetch buffer.  Returns false if there's no such block reference.
///
/// # Safety
/// `record` is a live reader whose current record is decoded.
pub unsafe fn XLogRecGetBlockTagExtended(
    record: *mut XLogReaderState,
    block_id: uint8,
    rlocator: *mut RelFileLocator,
    forknum: *mut ForkNumber,
    blknum: *mut BlockNumber,
    prefetch_buffer: *mut Buffer,
) -> bool {
    if !XLogRecHasBlockRef(record, block_id) {
        return false;
    }

    let bkpb = decoded_block((*record).record, block_id as usize);
    if !rlocator.is_null() {
        *rlocator = (*bkpb).rlocator;
    }
    if !forknum.is_null() {
        *forknum = (*bkpb).forknum;
    }
    if !blknum.is_null() {
        *blknum = (*bkpb).blkno;
    }
    if !prefetch_buffer.is_null() {
        *prefetch_buffer = (*bkpb).prefetch_buffer;
    }
    true
}

/// Returns the data associated with a block reference, or null if none.  The
/// returned pointer is MAXALIGNed.
///
/// # Safety
/// `record` is a live reader whose current record is decoded; `len` is an
/// optional out-pointer.
pub unsafe fn XLogRecGetBlockData(
    record: *mut XLogReaderState,
    block_id: uint8,
    len: *mut Size,
) -> *mut c_char {
    let rec = (*record).record;
    if (block_id as c_int) > (*rec).max_block_id
        || !(*decoded_block(rec, block_id as usize)).in_use
    {
        return null_mut();
    }

    let bkpb = decoded_block(rec, block_id as usize);

    if !(*bkpb).has_data {
        if !len.is_null() {
            *len = 0;
        }
        null_mut()
    } else {
        if !len.is_null() {
            *len = (*bkpb).data_len as Size;
        }
        (*bkpb).data
    }
}

/// Restore a full-page image from a backup block attached to a record.  Returns
/// true on success, false on failure (with an error queued).
///
/// # Safety
/// `record` is a live reader whose current record is decoded; `page` is writable
/// for BLCKSZ bytes.
pub unsafe fn RestoreBlockImage(
    record: *mut XLogReaderState,
    block_id: uint8,
    page: *mut c_char,
) -> bool {
    let rec = (*record).record;

    if (block_id as c_int) > (*rec).max_block_id
        || !(*decoded_block(rec, block_id as usize)).in_use
    {
        report_invalid_record(
            record,
            format!(
                "could not restore image at {} with invalid block {} specified",
                lsn_fmt((*record).ReadRecPtr),
                block_id
            ),
        );
        return false;
    }
    if !(*decoded_block(rec, block_id as usize)).has_image {
        report_invalid_record(
            record,
            format!(
                "could not restore image at {} with invalid state, block {}",
                lsn_fmt((*record).ReadRecPtr),
                block_id
            ),
        );
        return false;
    }

    let bkpb = decoded_block(rec, block_id as usize);
    let mut ptr: *mut c_char = (*bkpb).bkp_image;

    // PGAlignedBlock tmp; a BLCKSZ-sized, suitably-aligned scratch buffer.
    #[repr(align(8))]
    struct PGAlignedBlock {
        data: [c_char; BLCKSZ],
    }
    let mut tmp = PGAlignedBlock {
        data: [0; BLCKSZ],
    };

    if BKPIMAGE_COMPRESSED((*bkpb).bimg_info) {
        let mut decomp_success = true;

        if ((*bkpb).bimg_info & BKPIMAGE_COMPRESS_PGLZ) != 0 {
            if pglz_decompress(
                ptr,
                (*bkpb).bimg_len as int32,
                tmp.data.as_mut_ptr(),
                (BLCKSZ - (*bkpb).hole_length as usize) as int32,
                true,
            ) < 0
            {
                decomp_success = false;
            }
        } else if ((*bkpb).bimg_info & BKPIMAGE_COMPRESS_LZ4) != 0 {
            // !USE_LZ4 in this build.
            report_invalid_record(
                record,
                format!(
                    "could not restore image at {} compressed with {} not supported by build, block {}",
                    lsn_fmt((*record).ReadRecPtr),
                    "LZ4",
                    block_id
                ),
            );
            return false;
        } else if ((*bkpb).bimg_info & BKPIMAGE_COMPRESS_ZSTD) != 0 {
            // !USE_ZSTD in this build.
            report_invalid_record(
                record,
                format!(
                    "could not restore image at {} compressed with {} not supported by build, block {}",
                    lsn_fmt((*record).ReadRecPtr),
                    "zstd",
                    block_id
                ),
            );
            return false;
        } else {
            report_invalid_record(
                record,
                format!(
                    "could not restore image at {} compressed with unknown method, block {}",
                    lsn_fmt((*record).ReadRecPtr),
                    block_id
                ),
            );
            return false;
        }

        if !decomp_success {
            report_invalid_record(
                record,
                format!(
                    "could not decompress image at {}, block {}",
                    lsn_fmt((*record).ReadRecPtr),
                    block_id
                ),
            );
            return false;
        }

        ptr = tmp.data.as_mut_ptr();
    }

    /* generate page, taking into account hole if necessary */
    if (*bkpb).hole_length == 0 {
        memcpy(page as *mut c_void, ptr as *const c_void, BLCKSZ);
    } else {
        memcpy(
            page as *mut c_void,
            ptr as *const c_void,
            (*bkpb).hole_offset as usize,
        );
        /* must zero-fill the hole */
        memset(
            page.add((*bkpb).hole_offset as usize) as *mut c_void,
            0,
            (*bkpb).hole_length as usize,
        );
        memcpy(
            page.add((*bkpb).hole_offset as usize + (*bkpb).hole_length as usize) as *mut c_void,
            ptr.add((*bkpb).hole_offset as usize) as *const c_void,
            BLCKSZ - ((*bkpb).hole_offset as usize + (*bkpb).hole_length as usize),
        );
    }

    true
}

/// Extract the FullTransactionId from a WAL record (#ifndef FRONTEND).
///
/// STUB: depends on the running backend's replay state
/// (TransamVariables->nextXid) which is not ported.
///
/// # Safety
/// `record` is a live reader whose current record is decoded.
pub unsafe fn XLogRecGetFullXid(_record: *mut XLogReaderState) -> FullTransactionId {
    // TODO(pg-port): FullTransactionIdFromAllowableAt(TransamVariables->nextXid,
    //                XLogRecGetXid(record)) -- needs the backend replay state.
    unimplemented!("XLogRecGetFullXid requires the backend's TransamVariables (not ported)")
}

// ===========================================================================
// XLogRecGet* / XLogRecHas* accessor inlines (xlogreader.h macros -> pub fns).
// These read the most-recently-decoded record reachable via state->record.
// ===========================================================================

/// # Safety
/// `decoder` is a live reader whose current record is decoded.
#[inline]
pub unsafe fn XLogRecGetTotalLen(decoder: *mut XLogReaderState) -> uint32 {
    (*(*decoder).record).header.xl_tot_len
}
/// # Safety
/// See [`XLogRecGetTotalLen`].
#[inline]
pub unsafe fn XLogRecGetPrev(decoder: *mut XLogReaderState) -> XLogRecPtr {
    (*(*decoder).record).header.xl_prev
}
/// # Safety
/// See [`XLogRecGetTotalLen`].
#[inline]
pub unsafe fn XLogRecGetInfo(decoder: *mut XLogReaderState) -> uint8 {
    (*(*decoder).record).header.xl_info
}
/// # Safety
/// See [`XLogRecGetTotalLen`].
#[inline]
pub unsafe fn XLogRecGetRmid(decoder: *mut XLogReaderState) -> RmgrId {
    (*(*decoder).record).header.xl_rmid
}
/// # Safety
/// See [`XLogRecGetTotalLen`].
#[inline]
pub unsafe fn XLogRecGetXid(decoder: *mut XLogReaderState) -> TransactionId {
    (*(*decoder).record).header.xl_xid
}
/// # Safety
/// See [`XLogRecGetTotalLen`].
#[inline]
pub unsafe fn XLogRecGetOrigin(decoder: *mut XLogReaderState) -> RepOriginId {
    (*(*decoder).record).record_origin
}
/// # Safety
/// See [`XLogRecGetTotalLen`].
#[inline]
pub unsafe fn XLogRecGetTopXid(decoder: *mut XLogReaderState) -> TransactionId {
    (*(*decoder).record).toplevel_xid
}
/// # Safety
/// See [`XLogRecGetTotalLen`].
#[inline]
pub unsafe fn XLogRecGetData(decoder: *mut XLogReaderState) -> *mut c_char {
    (*(*decoder).record).main_data
}
/// # Safety
/// See [`XLogRecGetTotalLen`].
#[inline]
pub unsafe fn XLogRecGetDataLen(decoder: *mut XLogReaderState) -> uint32 {
    (*(*decoder).record).main_data_len
}
/// # Safety
/// See [`XLogRecGetTotalLen`].
#[inline]
pub unsafe fn XLogRecHasAnyBlockRefs(decoder: *mut XLogReaderState) -> bool {
    (*(*decoder).record).max_block_id >= 0
}
/// # Safety
/// See [`XLogRecGetTotalLen`].
#[inline]
pub unsafe fn XLogRecMaxBlockId(decoder: *mut XLogReaderState) -> c_int {
    (*(*decoder).record).max_block_id
}
/// # Safety
/// See [`XLogRecGetTotalLen`].
#[inline]
pub unsafe fn XLogRecGetBlock(decoder: *mut XLogReaderState, i: uint8) -> *mut DecodedBkpBlock {
    decoded_block((*decoder).record, i as usize)
}
/// # Safety
/// See [`XLogRecGetTotalLen`].
#[inline]
pub unsafe fn XLogRecHasBlockRef(decoder: *mut XLogReaderState, block_id: uint8) -> bool {
    let rec = (*decoder).record;
    (*rec).max_block_id >= block_id as c_int
        && (*decoded_block(rec, block_id as usize)).in_use
}
/// # Safety
/// See [`XLogRecGetTotalLen`].
#[inline]
pub unsafe fn XLogRecHasBlockImage(decoder: *mut XLogReaderState, block_id: uint8) -> bool {
    (*decoded_block((*decoder).record, block_id as usize)).has_image
}
/// # Safety
/// See [`XLogRecGetTotalLen`].
#[inline]
pub unsafe fn XLogRecBlockImageApply(decoder: *mut XLogReaderState, block_id: uint8) -> bool {
    (*decoded_block((*decoder).record, block_id as usize)).apply_image
}
/// # Safety
/// See [`XLogRecGetTotalLen`].
#[inline]
pub unsafe fn XLogRecHasBlockData(decoder: *mut XLogReaderState, block_id: uint8) -> bool {
    (*decoded_block((*decoder).record, block_id as usize)).has_data
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a minimal valid XLogRecord (header + short main-data fragment) in a
    /// byte buffer, compute its CRC, then exercise ValidXLogRecordHeader, the CRC
    /// check (ValidXLogRecord), DecodeXLogRecord, and the accessors.
    #[test]
    fn decode_minimal_record_roundtrip() {
        unsafe {
            // ---- assemble the record payload after the fixed header ----
            // payload: XLogRecordDataHeaderShort { id=DATA_SHORT, len=4 } + 4 bytes.
            let main_data: [u8; 4] = [0xDE, 0xAD, 0xBE, 0xEF];
            let payload_len = SizeOfXLogRecordDataHeaderShort + main_data.len(); // 2 + 4
            let total_len = SizeOfXLogRecord + payload_len;

            let mut buf = vec![0u8; total_len];

            // Fill payload region first (header CRC depends on it).
            let payload = &mut buf[SizeOfXLogRecord..];
            payload[0] = XLR_BLOCK_ID_DATA_SHORT;
            payload[1] = main_data.len() as u8;
            payload[2..2 + main_data.len()].copy_from_slice(&main_data);

            // Fill the header (all but CRC).
            let rec = buf.as_mut_ptr() as *mut XLogRecord;
            (*rec).xl_tot_len = total_len as uint32;
            (*rec).xl_xid = 1234;
            (*rec).xl_prev = 0;
            (*rec).xl_info = 0xA0; // arbitrary high-nibble (rmgr) info
            (*rec).xl_rmid = RM_XLOG_ID;
            (*rec)._padding = [0, 0];

            // Compute CRC exactly like ValidXLogRecord / XLogInsert: payload first,
            // then the header up to xl_crc.
            let mut crc = INIT_CRC32C();
            crc = COMP_CRC32C(
                crc,
                buf.as_ptr().add(SizeOfXLogRecord) as *const c_void,
                total_len - SizeOfXLogRecord,
            );
            crc = COMP_CRC32C(
                crc,
                buf.as_ptr() as *const c_void,
                core::mem::offset_of!(XLogRecord, xl_crc),
            );
            crc = FIN_CRC32C(crc);
            (*rec).xl_crc = crc;

            // ---- a minimal reader state (only the fields these paths touch) ----
            let mut errbuf = vec![0u8; MAX_ERRORMSG_LEN + 1];
            let mut state: XLogReaderState = core::mem::zeroed();
            state.errormsg_buf = errbuf.as_mut_ptr() as *mut c_char;
            state.ReadRecPtr = 0x1000;

            // Header validation: randAccess path (xl_prev < RecPtr).
            assert!(ValidXLogRecordHeader(
                &mut state,
                0x1000,
                InvalidXLogRecPtr,
                rec,
                true
            ));

            // CRC check on the hand-built record.
            assert!(ValidXLogRecord(&mut state, rec, 0x1000));

            // Corrupting a payload byte must fail the CRC.
            let saved = buf[total_len - 1];
            buf[total_len - 1] ^= 0xFF;
            let rec2 = buf.as_mut_ptr() as *mut XLogRecord;
            assert!(!ValidXLogRecord(&mut state, rec2, 0x1000));
            buf[total_len - 1] = saved; // restore

            // ---- decode it ----
            let required = DecodeXLogRecordRequiredSpace(total_len);
            let mut decode_area = vec![0u8; required + MAXIMUM_ALIGNOF];
            // MAXALIGN the start of the decode area.
            let aligned = MAXALIGN(decode_area.as_mut_ptr() as usize) as *mut DecodedXLogRecord;
            (*aligned).oversized = false;

            let rec3 = buf.as_mut_ptr() as *mut XLogRecord;
            let mut errormsg: *mut c_char = null_mut();
            let ok = DecodeXLogRecord(&mut state, aligned, rec3, 0x1000, &mut errormsg);
            assert!(ok, "decode failed: errormsg set = {}", !errormsg.is_null());

            // Point the reader at the decoded record and use the accessors.
            state.record = aligned;
            assert_eq!(XLogRecGetInfo(&mut state), 0xA0);
            assert_eq!(XLogRecGetRmid(&mut state), RM_XLOG_ID);
            assert_eq!(XLogRecGetXid(&mut state), 1234);
            assert_eq!(XLogRecGetDataLen(&mut state), main_data.len() as uint32);
            assert!(!XLogRecHasAnyBlockRefs(&mut state)); // no block refs
            assert_eq!(XLogRecMaxBlockId(&mut state), -1);

            let data = XLogRecGetData(&mut state);
            assert!(!data.is_null());
            let got = core::slice::from_raw_parts(data as *const u8, main_data.len());
            assert_eq!(got, &main_data);
        }
    }

    #[test]
    fn sizeof_constants_match_c() {
        // SizeOfXLogRecord = offsetof(xl_crc) + sizeof(pg_crc32c).
        // On LP64: u32 + u32 + u64 + u8 + u8 + 2 pad + u32 = 24 bytes.
        assert_eq!(SizeOfXLogRecord, 24);
        assert_eq!(SizeOfXLogRecordBlockHeader, 4);
        assert_eq!(SizeOfXLogRecordBlockImageHeader, 5);
        assert_eq!(SizeOfXLogRecordBlockCompressHeader, 2);
        assert_eq!(SizeOfXLogRecordDataHeaderShort, 2);
        assert_eq!(SizeOfXLogRecordDataHeaderLong, 5);
    }

    #[test]
    fn required_space_is_pessimistic() {
        // Must always be >= the raw record length plus the struct overhead.
        let s = DecodeXLogRecordRequiredSpace(100);
        assert!(s > 100);
        assert!(s >= offsetof_decoded_blocks() + 100);
    }
}
