/*-------------------------------------------------------------------------
 *
 * xloginsert.rs
 *      Functions for constructing WAL records
 *
 * Constructing a WAL record begins with a call to XLogBeginInsert,
 * followed by a number of XLogRegister* calls. The registered data is
 * collected in private working memory, and finally assembled into a chain
 * of XLogRecData structs by a call to XLogRecordAssemble(). See
 * access/transam/README for details.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/backend/access/transam/xloginsert.c
 * src/include/access/xloginsert.h  (merged below)
 *
 *-------------------------------------------------------------------------
 */

use crate::prelude::*;

use core::ffi::c_void;
use core::mem::{size_of, zeroed};
use core::ptr::{copy_nonoverlapping, null_mut, write_bytes};

use crate::c::TransactionId;
use crate::utils::memutils::AllocSizeIsValid;
use crate::access::transam::xlogdefs::{InvalidXLogRecPtr, RepOriginId, XLogRecPtr};
use crate::access::transam::xlog_internal::XLogRecData;
use crate::access::transam::xlogrecord::{
    XLogRecord,
    XLogRecordBlockCompressHeader, XLogRecordBlockHeader, XLogRecordBlockImageHeader,
    XLogRecordMaxSize,
    BKPBLOCK_HAS_DATA, BKPBLOCK_HAS_IMAGE, BKPBLOCK_SAME_REL, BKPBLOCK_WILL_INIT,
    BKPIMAGE_APPLY, BKPIMAGE_COMPRESS_LZ4, BKPIMAGE_COMPRESS_PGLZ, BKPIMAGE_COMPRESS_ZSTD,
    BKPIMAGE_HAS_HOLE,
    MaxSizeOfXLogRecordBlockHeader,
    SizeOfXLogRecord, SizeOfXLogRecordBlockCompressHeader, SizeOfXLogRecordBlockHeader,
    SizeOfXLogRecordBlockImageHeader, SizeOfXLogRecordDataHeaderLong,
    XLR_BLOCK_ID_DATA_LONG, XLR_BLOCK_ID_DATA_SHORT, XLR_BLOCK_ID_ORIGIN,
    XLR_BLOCK_ID_TOPLEVEL_XID, XLR_MAX_BLOCK_ID,
};
use crate::access::transam::xlogreader::{
    DecodeXLogRecordRequiredSpace, InvalidRepOriginId, SizeOfXLogLongPHD,
    XLR_CHECK_CONSISTENCY, XLR_RMGR_INFO_MASK, XLR_SPECIAL_REL_UPDATE, RM_XLOG_ID,
};
use crate::access::rmgrlist::RmgrId;
use crate::common::pg_lzcompress::{pglz_compress, PGLZ_MAX_OUTPUT, PGLZ_strategy_default};
use crate::common::relpath::ForkNumber;
use crate::miscadmin::{
    CritSectionCount, IsBootstrapProcessingMode, CHECK_FOR_INTERRUPTS,
};
use crate::pg_config::BLCKSZ;
use crate::port::pg_crc32c::{pg_crc32c, COMP_CRC32C, FIN_CRC32C, INIT_CRC32C};
use crate::storage::block::BlockNumber;
use crate::storage::buf::Buffer;
use crate::storage::bufpage::{
    Page, PageGetLSN, PageHeader, PageIsNew, PageSetLSN, SizeOfPageHeaderData,
};
use crate::storage::file::buffile::PGAlignedBlock;
use crate::storage::relfilelocator::{RelFileLocator, RelFileLocatorEquals};
use crate::utils::memutils::AllocSizeIsValid as MemAllocSizeIsValid;

// --------------------------------------------------------------------------
// Merged from xloginsert.h: public constants
// --------------------------------------------------------------------------

/// Minimum working-area sizes.  Call XLogEnsureRecordSpace() for more.
pub const XLR_NORMAL_MAX_BLOCK_ID: c_int = 4;
pub const XLR_NORMAL_RDATAS: c_int = 20;

/* flags for XLogRegisterBuffer */
pub const REGBUF_FORCE_IMAGE: u8 = 0x01; /* force a full-page image */
pub const REGBUF_NO_IMAGE: u8 = 0x02;    /* don't take a full-page image */
/// page will be re-initialized at replay (implies NO_IMAGE)
pub const REGBUF_WILL_INIT: u8 = 0x04 | 0x02;
/// page follows "standard" page layout (pd_lower..pd_upper hole will be skipped)
pub const REGBUF_STANDARD: u8 = 0x08;
/// include data even if a full-page image is taken
pub const REGBUF_KEEP_DATA: u8 = 0x10;
/// intentionally register clean buffer
pub const REGBUF_NO_CHANGE: u8 = 0x20;

// XLogSetRecordFlags flag bits (access/xlog.h in C source)
/// include replication origin in this WAL record
pub const XLOG_INCLUDE_ORIGIN: u8 = 0x01; // TODO(pg-port): real home is access/xlog.h
/// record is not important for durability
pub const XLOG_MARK_UNIMPORTANT: u8 = 0x02; // TODO(pg-port): real home is access/xlog.h

// XLOG_FPI / XLOG_FPI_FOR_HINT opcode values (access/xlog_internal.h)
// mirrored from access/rmgrdesc/xlogdesc.rs -- the canonical home once
// access/transam/xlog.rs is translated.
const XLOG_FPI: u8 = 0xB0;
const XLOG_FPI_FOR_HINT: u8 = 0xA0;

// --------------------------------------------------------------------------
// WalCompression (GUC enum: access/xlog.h / access/xlog_internal.h)
// --------------------------------------------------------------------------

/// WAL compression method enumeration (mirrors WalCompression C enum).
/// TODO(pg-port): canonical home is access/xlog.h once xlog.c is translated.
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum WalCompression {
    WAL_COMPRESSION_NONE = 0,
    WAL_COMPRESSION_PGLZ = 1,
    WAL_COMPRESSION_LZ4 = 2,
    WAL_COMPRESSION_ZSTD = 3,
}

// --------------------------------------------------------------------------
// GUC variables (stubs; real homes not yet translated)
// --------------------------------------------------------------------------

/// GUC: wal_compression setting.
/// TODO(pg-port): real wal_compression lives in access/transam/xlog.c
pub static mut wal_compression: i32 = WalCompression::WAL_COMPRESSION_NONE as i32;

/// GUC: wal_consistency_checking[] per-rmgr array.
/// TODO(pg-port): real wal_consistency_checking lives in access/transam/xlog.c
pub static mut wal_consistency_checking: [bool; 256] = [false; 256];

// --------------------------------------------------------------------------
// Symbols whose home is not yet translated (stubs)
// --------------------------------------------------------------------------

/// TODO(pg-port): real XLogInsertRecord lives in access/transam/xlog.c
pub unsafe fn XLogInsertRecord(
    _rdata: *mut XLogRecData,
    _fpw_lsn: XLogRecPtr,
    _flags: u8,
    _num_fpi: c_int,
    _topxid_included: bool,
) -> XLogRecPtr {
    unimplemented!("TODO(pg-port): real XLogInsertRecord lives in access/transam/xlog.c")
}

/// TODO(pg-port): real GetFullPageWriteInfo lives in access/transam/xlog.c
pub unsafe fn GetFullPageWriteInfo(redo_rec_ptr: *mut XLogRecPtr, do_page_writes: *mut bool) {
    unimplemented!("TODO(pg-port): real GetFullPageWriteInfo lives in access/transam/xlog.c")
}

/// TODO(pg-port): real GetRedoRecPtr lives in access/transam/xlog.c
pub unsafe fn GetRedoRecPtr() -> XLogRecPtr {
    unimplemented!("TODO(pg-port): real GetRedoRecPtr lives in access/transam/xlog.c")
}

/// TODO(pg-port): real BufferGetTag lives in storage/buffer/bufmgr.c
pub unsafe fn BufferGetTag(
    _buffer: Buffer,
    _rlocator: *mut RelFileLocator,
    _forknum: *mut ForkNumber,
    _blknum: *mut BlockNumber,
) {
    unimplemented!("TODO(pg-port): real BufferGetTag lives in storage/buffer/bufmgr.c")
}

/// TODO(pg-port): real BufferGetPage lives in storage/buffer/bufmgr.c
pub unsafe fn BufferGetPage(_buffer: Buffer) -> Page {
    unimplemented!("TODO(pg-port): real BufferGetPage lives in storage/buffer/bufmgr.c")
}

/// TODO(pg-port): real BufferIsExclusiveLocked lives in storage/buffer/bufmgr.c
pub unsafe fn BufferIsExclusiveLocked(_buffer: Buffer) -> bool {
    unimplemented!("TODO(pg-port): real BufferIsExclusiveLocked lives in storage/buffer/bufmgr.c")
}

/// TODO(pg-port): real BufferIsDirty lives in storage/buffer/bufmgr.c
pub unsafe fn BufferIsDirty(_buffer: Buffer) -> bool {
    unimplemented!("TODO(pg-port): real BufferIsDirty lives in storage/buffer/bufmgr.c")
}

/// TODO(pg-port): real BufferGetBlock lives in storage/buffer/bufmgr.c
pub unsafe fn BufferGetBlock(_buffer: Buffer) -> *mut c_char {
    unimplemented!("TODO(pg-port): real BufferGetBlock lives in storage/buffer/bufmgr.c")
}

/// TODO(pg-port): real BufferGetLSNAtomic lives in storage/buffer/bufmgr.c
pub unsafe fn BufferGetLSNAtomic(_buffer: Buffer) -> XLogRecPtr {
    unimplemented!("TODO(pg-port): real BufferGetLSNAtomic lives in storage/buffer/bufmgr.c")
}

/// TODO(pg-port): real ReadBufferExtended lives in storage/buffer/bufmgr.c
pub unsafe fn ReadBufferExtended(
    _rel: Relation,
    _fork: ForkNumber,
    _blk: BlockNumber,
    _mode: c_int,
    _strategy: *mut c_void,
) -> Buffer {
    unimplemented!("TODO(pg-port): real ReadBufferExtended lives in storage/buffer/bufmgr.c")
}

/// TODO(pg-port): real LockBuffer lives in storage/buffer/bufmgr.c
pub unsafe fn LockBuffer(_buffer: Buffer, _mode: c_int) {
    unimplemented!("TODO(pg-port): real LockBuffer lives in storage/buffer/bufmgr.c")
}

/// TODO(pg-port): real UnlockReleaseBuffer lives in storage/buffer/bufmgr.c
pub unsafe fn UnlockReleaseBuffer(_buffer: Buffer) {
    unimplemented!("TODO(pg-port): real UnlockReleaseBuffer lives in storage/buffer/bufmgr.c")
}

/// TODO(pg-port): real MarkBufferDirty lives in storage/buffer/bufmgr.c
pub unsafe fn MarkBufferDirty(_buffer: Buffer) {
    unimplemented!("TODO(pg-port): real MarkBufferDirty lives in storage/buffer/bufmgr.c")
}

/// TODO(pg-port): real GetCurrentTransactionIdIfAny lives in access/transam/xact.c
pub unsafe fn GetCurrentTransactionIdIfAny() -> TransactionId {
    unimplemented!(
        "TODO(pg-port): real GetCurrentTransactionIdIfAny lives in access/transam/xact.c"
    )
}

/// TODO(pg-port): real GetTopTransactionIdIfAny lives in access/transam/xact.c
pub unsafe fn GetTopTransactionIdIfAny() -> TransactionId {
    unimplemented!(
        "TODO(pg-port): real GetTopTransactionIdIfAny lives in access/transam/xact.c"
    )
}

/// TODO(pg-port): real IsSubxactTopXidLogPending lives in access/transam/xact.c
pub unsafe fn IsSubxactTopXidLogPending() -> bool {
    unimplemented!(
        "TODO(pg-port): real IsSubxactTopXidLogPending lives in access/transam/xact.c"
    )
}

/// TODO(pg-port): real replorigin_session_origin lives in replication/origin.c
pub static mut replorigin_session_origin: RepOriginId = InvalidRepOriginId;

/// TODO(pg-port): real PGPROC struct lives in storage/lmgr/proc.h
/// Only the fields used in this file are represented here.
#[allow(non_snake_case)]
pub struct PGPROC {
    pub delayChkptFlags: c_int,
}

/// TODO(pg-port): real MyProc lives in storage/lmgr/proc.c
pub static mut MyProc: *mut PGPROC = null_mut();

/// TODO(pg-port): real DELAY_CHKPT_START lives in storage/proc.h
pub const DELAY_CHKPT_START: c_int = 1 << 0;

/// Relation opaque pointer; real type lives in utils/rel.h
pub type Relation = *mut c_void;

// --------------------------------------------------------------------------
// Compression buffer size constants
// --------------------------------------------------------------------------

/// Maximum compressed output for pglz (no lz4/zstd, !defined branch)
const PGLZ_MAX_BLCKSZ: usize = {
    // PGLZ_MAX_OUTPUT(BLCKSZ) = 32 + BLCKSZ + BLCKSZ/16
    32 + BLCKSZ + BLCKSZ / 16
};

/// lz4/zstd not compiled in -> 0 (the !defined branch)
const LZ4_MAX_BLCKSZ: usize = 0;
const ZSTD_MAX_BLCKSZ: usize = 0;

/// Buffer size required to store a compressed version of a backup block image.
const COMPRESS_BUFSIZE: usize = {
    let a = if PGLZ_MAX_BLCKSZ > LZ4_MAX_BLCKSZ {
        PGLZ_MAX_BLCKSZ
    } else {
        LZ4_MAX_BLCKSZ
    };
    if a > ZSTD_MAX_BLCKSZ { a } else { ZSTD_MAX_BLCKSZ }
};

// --------------------------------------------------------------------------
// Header size helpers (from xloginsert.h / xlogrecord.h)
// --------------------------------------------------------------------------

const fn SizeOfXlogOrigin() -> usize {
    size_of::<RepOriginId>() + size_of::<c_char>()
}

const fn SizeOfXLogTransactionId() -> usize {
    size_of::<TransactionId>() + size_of::<c_char>()
}

const fn HEADER_SCRATCH_SIZE() -> usize {
    SizeOfXLogRecord()
        + MaxSizeOfXLogRecordBlockHeader() * (XLR_MAX_BLOCK_ID as usize + 1)
        + SizeOfXLogRecordDataHeaderLong()
        + SizeOfXlogOrigin()
        + SizeOfXLogTransactionId()
}

// --------------------------------------------------------------------------
// registered_buffer struct
// --------------------------------------------------------------------------

/// Per-block registration entry for the in-progress WAL record.
struct registered_buffer {
    in_use: bool,
    flags: u8,
    rlocator: RelFileLocator,
    forkno: ForkNumber,
    block: BlockNumber,
    page: *const c_char,     /* page content (PageData ptr) */
    rdata_len: u32,          /* total length in rdata chain */
    rdata_head: *mut XLogRecData,
    rdata_tail: *mut XLogRecData,
    bkp_rdatas: [XLogRecData; 2], /* temporary rdatas for backup block data */
    compressed_page: [c_char; COMPRESS_BUFSIZE],
}

impl registered_buffer {
    const fn zeroed() -> Self {
        // SAFETY: all-zero is valid for these types (pointers = null, etc.)
        unsafe { zeroed() }
    }
}

// --------------------------------------------------------------------------
// Module-level statics (mirror the C file-scope globals)
// --------------------------------------------------------------------------

static mut registered_buffers: *mut registered_buffer = null_mut();
static mut max_registered_buffers: c_int = 0;
static mut max_registered_block_id: c_int = 0;

static mut mainrdata_head: *mut XLogRecData = null_mut();
/// Points to mainrdata_head initially; updated by XLogRegisterData.
/// In C: `static XLogRecData *mainrdata_last = (XLogRecData *) &mainrdata_head;`
/// We model this as a raw pointer to XLogRecData, initialised to point at
/// mainrdata_head (same trick: the next field of a dummy node).
static mut mainrdata_last: *mut XLogRecData = null_mut(); /* init in InitXLogInsert */
static mut mainrdata_len: u64 = 0;

static mut curinsert_flags: u8 = 0;

static mut hdr_rdt: XLogRecData = XLogRecData {
    next: null_mut(),
    data: null_mut(),
    len: 0,
};
static mut hdr_scratch: *mut c_char = null_mut();

static mut rdatas: *mut XLogRecData = null_mut();
static mut num_rdatas: c_int = 0;
static mut max_rdatas: c_int = 0;

static mut begininsert_called: bool = false;

static mut xloginsert_cxt: MemoryContext = null_mut();

// --------------------------------------------------------------------------
// XLogBeginInsert
// --------------------------------------------------------------------------

/// Begin constructing a WAL record. This must be called before the
/// XLogRegister* functions and XLogInsert().
pub unsafe fn XLogBeginInsert() {
    Assert!(max_registered_block_id == 0);
    // mainrdata_last must still point back at mainrdata_head when clean
    Assert!(mainrdata_len == 0);

    /* cross-check on whether we should be here or not */
    if !XLogInsertAllowed() {
        elog!(ERROR, "cannot make new WAL entries during recovery");
    }

    if begininsert_called {
        elog!(ERROR, "XLogBeginInsert was already called");
    }

    begininsert_called = true;
}

// --------------------------------------------------------------------------
// XLogEnsureRecordSpace
// --------------------------------------------------------------------------

/// Ensure that there are enough buffer and data slots in the working area,
/// for subsequent XLogRegisterBuffer, XLogRegisterData and XLogRegisterBufData
/// calls.
///
/// There is always space for a small number of buffers and data chunks, enough
/// for most record types. This function is for the exceptional cases that need
/// more.
pub unsafe fn XLogEnsureRecordSpace(mut max_block_id: c_int, mut ndatas: c_int) {
    let nbuffers: c_int;

    /*
     * This must be called before entering a critical section, because
     * allocating memory inside a critical section can fail. repalloc() will
     * check the same, but better to check it here too so that we fail
     * consistently even if the arrays happen to be large enough already.
     */
    Assert!(CritSectionCount == 0);

    /* the minimum values can't be decreased */
    if max_block_id < XLR_NORMAL_MAX_BLOCK_ID {
        max_block_id = XLR_NORMAL_MAX_BLOCK_ID;
    }
    if ndatas < XLR_NORMAL_RDATAS {
        ndatas = XLR_NORMAL_RDATAS;
    }

    if max_block_id > XLR_MAX_BLOCK_ID {
        elog!(ERROR, "maximum number of WAL record block references exceeded");
    }
    nbuffers = max_block_id + 1;

    if nbuffers > max_registered_buffers {
        registered_buffers = repalloc(
            registered_buffers as *mut c_void,
            size_of::<registered_buffer>() * nbuffers as usize,
        ) as *mut registered_buffer;

        /*
         * At least the padding bytes in the structs must be zeroed, because
         * they are included in WAL data, but initialize it all for tidiness.
         */
        write_bytes(
            registered_buffers.add(max_registered_buffers as usize),
            0u8,
            (nbuffers - max_registered_buffers) as usize * size_of::<registered_buffer>(),
        );
        max_registered_buffers = nbuffers;
    }

    if ndatas > max_rdatas {
        rdatas = repalloc(
            rdatas as *mut c_void,
            size_of::<XLogRecData>() * ndatas as usize,
        ) as *mut XLogRecData;
        max_rdatas = ndatas;
    }
}

// --------------------------------------------------------------------------
// XLogResetInsertion
// --------------------------------------------------------------------------

/// Reset WAL record construction buffers.
pub unsafe fn XLogResetInsertion() {
    let mut i: c_int = 0;
    while i < max_registered_block_id {
        (*registered_buffers.add(i as usize)).in_use = false;
        i += 1;
    }

    num_rdatas = 0;
    max_registered_block_id = 0;
    mainrdata_len = 0;
    // reset mainrdata_last to point back at mainrdata_head
    mainrdata_last = &raw mut mainrdata_head as *mut XLogRecData;
    curinsert_flags = 0;
    begininsert_called = false;
}

// --------------------------------------------------------------------------
// XLogRegisterBuffer
// --------------------------------------------------------------------------

/// Register a reference to a buffer with the WAL record being constructed.
/// This must be called for every page that the WAL-logged operation modifies.
pub unsafe fn XLogRegisterBuffer(block_id: u8, buffer: Buffer, flags: u8) {
    let regbuf: *mut registered_buffer;

    /* NO_IMAGE doesn't make sense with FORCE_IMAGE */
    Assert!(!((flags & REGBUF_FORCE_IMAGE) != 0 && (flags & REGBUF_NO_IMAGE) != 0));
    Assert!(begininsert_called);

    /*
     * Ordinarily, buffer should be exclusive-locked and marked dirty before
     * we get here, otherwise we could end up violating one of the rules in
     * access/transam/README.
     *
     * Some callers intentionally register a clean page and never update that
     * page's LSN; in that case they can pass the flag REGBUF_NO_CHANGE to
     * bypass these checks.
     */
    #[cfg(debug_assertions)]
    {
        if (flags & REGBUF_NO_CHANGE) == 0 {
            Assert!(BufferIsExclusiveLocked(buffer) && BufferIsDirty(buffer));
        }
    }

    if (block_id as c_int) >= max_registered_block_id {
        if (block_id as c_int) >= max_registered_buffers {
            elog!(ERROR, "too many registered buffers");
        }
        max_registered_block_id = block_id as c_int + 1;
    }

    regbuf = registered_buffers.add(block_id as usize);

    BufferGetTag(
        buffer,
        &raw mut (*regbuf).rlocator,
        &raw mut (*regbuf).forkno,
        &raw mut (*regbuf).block,
    );
    (*regbuf).page = BufferGetPage(buffer);
    (*regbuf).flags = flags;
    (*regbuf).rdata_tail =
        &raw mut (*regbuf).rdata_head as *mut XLogRecData;
    (*regbuf).rdata_len = 0;

    /*
     * Check that this page hasn't already been registered with some other
     * block_id.
     */
    #[cfg(debug_assertions)]
    {
        let mut i: c_int = 0;
        while i < max_registered_block_id {
            let regbuf_old: *mut registered_buffer = registered_buffers.add(i as usize);
            if i != block_id as c_int && (*regbuf_old).in_use {
                Assert!(
                    !RelFileLocatorEquals(&(*regbuf_old).rlocator, &(*regbuf).rlocator)
                        || (*regbuf_old).forkno != (*regbuf).forkno
                        || (*regbuf_old).block != (*regbuf).block
                );
            }
            i += 1;
        }
    }

    (*regbuf).in_use = true;
}

// --------------------------------------------------------------------------
// XLogRegisterBlock
// --------------------------------------------------------------------------

/// Like XLogRegisterBuffer, but for registering a block that's not in the
/// shared buffer pool (i.e. when you don't have a Buffer for it).
pub unsafe fn XLogRegisterBlock(
    block_id: u8,
    rlocator: *mut RelFileLocator,
    forknum: ForkNumber,
    blknum: BlockNumber,
    page: *const c_char,
    flags: u8,
) {
    let regbuf: *mut registered_buffer;

    Assert!(begininsert_called);

    if (block_id as c_int) >= max_registered_block_id {
        max_registered_block_id = block_id as c_int + 1;
    }

    if (block_id as c_int) >= max_registered_buffers {
        elog!(ERROR, "too many registered buffers");
    }

    regbuf = registered_buffers.add(block_id as usize);

    (*regbuf).rlocator = *rlocator;
    (*regbuf).forkno = forknum;
    (*regbuf).block = blknum;
    (*regbuf).page = page;
    (*regbuf).flags = flags;
    (*regbuf).rdata_tail =
        &raw mut (*regbuf).rdata_head as *mut XLogRecData;
    (*regbuf).rdata_len = 0;

    /*
     * Check that this page hasn't already been registered with some other
     * block_id.
     */
    #[cfg(debug_assertions)]
    {
        let mut i: c_int = 0;
        while i < max_registered_block_id {
            let regbuf_old: *mut registered_buffer = registered_buffers.add(i as usize);
            if i != block_id as c_int && (*regbuf_old).in_use {
                Assert!(
                    !RelFileLocatorEquals(&(*regbuf_old).rlocator, &(*regbuf).rlocator)
                        || (*regbuf_old).forkno != (*regbuf).forkno
                        || (*regbuf_old).block != (*regbuf).block
                );
            }
            i += 1;
        }
    }

    (*regbuf).in_use = true;
}

// --------------------------------------------------------------------------
// XLogRegisterData
// --------------------------------------------------------------------------

/// Add data to the WAL record that's being constructed.
///
/// The data is appended to the "main chunk", available at replay with
/// XLogRecGetData().
pub unsafe fn XLogRegisterData(data: *const c_void, len: u32) {
    let rdata: *mut XLogRecData;

    Assert!(begininsert_called);

    if num_rdatas >= max_rdatas {
        ereport!(
            ERROR,
            errmsg!(
                "too much WAL data: {} out of {} data segments are already in use",
                num_rdatas,
                max_rdatas
            )
        );
    }
    rdata = rdatas.add(num_rdatas as usize);
    num_rdatas += 1;

    (*rdata).data = data;
    (*rdata).len = len;

    /*
     * we use the mainrdata_last pointer to track the end of the chain, so no
     * need to clear 'next' here.
     */
    (*mainrdata_last).next = rdata;
    mainrdata_last = rdata;

    mainrdata_len += len as u64;
}

// --------------------------------------------------------------------------
// XLogRegisterBufData
// --------------------------------------------------------------------------

/// Add buffer-specific data to the WAL record that's being constructed.
///
/// Block_id must reference a block previously registered with
/// XLogRegisterBuffer(). If this is called more than once for the same
/// block_id, the data is appended.
///
/// The maximum amount of data that can be registered per block is 65535
/// bytes. That should be plenty; if you need more than BLCKSZ bytes to
/// reconstruct the changes to the page, you might as well just log a full
/// copy of it. (the "main data" that's not associated with a block is not
/// limited)
pub unsafe fn XLogRegisterBufData(block_id: u8, data: *const c_void, len: u32) {
    let regbuf: *mut registered_buffer;
    let rdata: *mut XLogRecData;

    Assert!(begininsert_called);

    /* find the registered buffer struct */
    regbuf = registered_buffers.add(block_id as usize);
    if !(*regbuf).in_use {
        elog!(
            ERROR,
            "no block with id {} registered with WAL insertion",
            block_id
        );
    }

    /*
     * Check against max_rdatas and ensure we do not register more data per
     * buffer than can be handled by the physical data format; i.e. that
     * regbuf->rdata_len does not grow beyond what
     * XLogRecordBlockHeader->data_length can hold.
     */
    if num_rdatas >= max_rdatas {
        ereport!(
            ERROR,
            errmsg!(
                "too much WAL data: {} out of {} data segments are already in use",
                num_rdatas,
                max_rdatas
            )
        );
    }
    if (*regbuf).rdata_len + len > u16::MAX as u32 || len > u16::MAX as u32 {
        ereport!(
            ERROR,
            errmsg!(
                "too much WAL data: registering more than maximum {} bytes allowed to block {}: current {} bytes, adding {} bytes",
                u16::MAX,
                block_id,
                (*regbuf).rdata_len,
                len
            )
        );
    }

    rdata = rdatas.add(num_rdatas as usize);
    num_rdatas += 1;

    (*rdata).data = data;
    (*rdata).len = len;

    (*(*regbuf).rdata_tail).next = rdata;
    (*regbuf).rdata_tail = rdata;
    (*regbuf).rdata_len += len;
}

// --------------------------------------------------------------------------
// XLogSetRecordFlags
// --------------------------------------------------------------------------

/// Set insert status flags for the upcoming WAL record.
///
/// The flags that can be used here are:
/// - XLOG_INCLUDE_ORIGIN, to determine if the replication origin should be
///   included in the record.
/// - XLOG_MARK_UNIMPORTANT, to signal that the record is not important for
///   durability, which allows to avoid triggering WAL archiving and other
///   background activity.
pub unsafe fn XLogSetRecordFlags(flags: u8) {
    Assert!(begininsert_called);
    curinsert_flags |= flags;
}

// --------------------------------------------------------------------------
// XLogInsert
// --------------------------------------------------------------------------

/// Insert an XLOG record having the specified RMID and info bytes, with the
/// body of the record being the data and buffer references registered earlier
/// with XLogRegister* calls.
///
/// Returns XLOG pointer to end of record (beginning of next record).
/// This can be used as LSN for data pages affected by the logged action.
/// (LSN is the XLOG point up to which the XLOG must be flushed to disk
/// before the data page can be written out.  This implements the basic
/// WAL rule "write the log before the data".)
pub unsafe fn XLogInsert(rmid: RmgrId, info: u8) -> XLogRecPtr {
    let mut end_pos: XLogRecPtr;

    /* XLogBeginInsert() must have been called. */
    if !begininsert_called {
        elog!(ERROR, "XLogBeginInsert was not called");
    }

    /*
     * The caller can set rmgr bits, XLR_SPECIAL_REL_UPDATE and
     * XLR_CHECK_CONSISTENCY; the rest are reserved for use by me.
     */
    if (info & !(XLR_RMGR_INFO_MASK | XLR_SPECIAL_REL_UPDATE | XLR_CHECK_CONSISTENCY)) != 0 {
        elog!(PANIC, "invalid xlog info mask {:02X}", info);
    }

    /* TRACE_POSTGRESQL_WAL_INSERT omitted: no dtrace support in Rust port */

    /*
     * In bootstrap mode, we don't actually log anything but XLOG resources;
     * return a phony record pointer.
     */
    if IsBootstrapProcessingMode() && rmid != RM_XLOG_ID {
        XLogResetInsertion();
        end_pos = SizeOfXLogLongPHD as XLogRecPtr; /* start of 1st chkpt record */
        return end_pos;
    }

    'retry: loop {
        let mut redo_rec_ptr: XLogRecPtr = 0;
        let mut do_page_writes: bool = false;
        let mut topxid_included: bool = false;
        let mut fpw_lsn: XLogRecPtr;
        let rdt: *mut XLogRecData;
        let mut num_fpi: c_int = 0;

        /*
         * Get values needed to decide whether to do full-page writes. Since
         * we don't yet have an insertion lock, these could change under us,
         * but XLogInsertRecord will recheck them once it has a lock.
         */
        GetFullPageWriteInfo(&raw mut redo_rec_ptr, &raw mut do_page_writes);

        fpw_lsn = InvalidXLogRecPtr;
        rdt = XLogRecordAssemble(
            rmid,
            info,
            redo_rec_ptr,
            do_page_writes,
            &raw mut fpw_lsn,
            &raw mut num_fpi,
            &raw mut topxid_included,
        );

        end_pos = XLogInsertRecord(rdt, fpw_lsn, curinsert_flags, num_fpi, topxid_included);
        if end_pos != InvalidXLogRecPtr {
            break 'retry;
        }
    }

    XLogResetInsertion();

    end_pos
}

// --------------------------------------------------------------------------
// XLogRecordAssemble  (internal)
// --------------------------------------------------------------------------

/// Assemble a WAL record from the registered data and buffers into an
/// XLogRecData chain, ready for insertion with XLogInsertRecord().
///
/// The record header fields are filled in, except for the xl_prev field. The
/// calculated CRC does not include the record header yet.
///
/// If there are any registered buffers, and a full-page image was not taken
/// of all of them, *fpw_lsn is set to the lowest LSN among such pages. This
/// signals that the assembled record is only good for insertion on the
/// assumption that the RedoRecPtr and doPageWrites values were up-to-date.
///
/// *topxid_included is set if the topmost transaction ID is logged with the
/// current subtransaction.
unsafe fn XLogRecordAssemble(
    rmid: RmgrId,
    mut info: u8,
    redo_rec_ptr: XLogRecPtr,
    do_page_writes: bool,
    fpw_lsn: *mut XLogRecPtr,
    num_fpi: *mut c_int,
    topxid_included: *mut bool,
) -> *mut XLogRecData {
    let mut total_len: u64 = 0;
    let mut block_id: c_int;
    let mut rdata_crc: pg_crc32c;
    let mut prev_regbuf: *mut registered_buffer = null_mut();
    let mut rdt_datas_last: *mut XLogRecData;
    let rechdr: *mut XLogRecord;
    let mut scratch: *mut c_char = hdr_scratch;

    /*
     * Note: this function can be called multiple times for the same record.
     * All the modifications we do to the rdata chains below must handle that.
     */

    /* The record begins with the fixed-size header */
    rechdr = scratch as *mut XLogRecord;
    scratch = scratch.add(SizeOfXLogRecord());

    (*(&raw mut hdr_rdt)).next = null_mut();
    rdt_datas_last = &raw mut hdr_rdt;
    (*(&raw mut hdr_rdt)).data = hdr_scratch as *const c_void;

    /*
     * Enforce consistency checks for this record if user is looking for it.
     * Do this before at the beginning of this routine to give the possibility
     * for callers of XLogInsert() to pass XLR_CHECK_CONSISTENCY directly for
     * a record.
     */
    if wal_consistency_checking[rmid as usize] {
        info |= XLR_CHECK_CONSISTENCY;
    }

    /*
     * Make an rdata chain containing all the data portions of all block
     * references. This includes the data for full-page images. Also append
     * the headers for the block references in the scratch buffer.
     */
    *fpw_lsn = InvalidXLogRecPtr;
    block_id = 0;
    while block_id < max_registered_block_id {
        let regbuf: *mut registered_buffer = registered_buffers.add(block_id as usize);
        let needs_backup: bool;
        let needs_data: bool;
        let mut bkpb: XLogRecordBlockHeader = zeroed();
        let mut bimg: XLogRecordBlockImageHeader = zeroed();
        let mut cbimg: XLogRecordBlockCompressHeader = zeroed();
        let samerel: bool;
        let mut is_compressed: bool = false;
        let include_image: bool;

        if !(*regbuf).in_use {
            block_id += 1;
            continue;
        }

        /* Determine if this block needs to be backed up */
        if ((*regbuf).flags & REGBUF_FORCE_IMAGE) != 0 {
            needs_backup = true;
        } else if ((*regbuf).flags & REGBUF_NO_IMAGE) != 0 {
            needs_backup = false;
        } else if !do_page_writes {
            needs_backup = false;
        } else {
            /*
             * We assume page LSN is first data on *every* page that can be
             * passed to XLogInsert, whether it has the standard page layout
             * or not.
             */
            let page_lsn: XLogRecPtr = PageGetLSN((*regbuf).page);

            needs_backup = page_lsn <= redo_rec_ptr;
            if !needs_backup {
                if *fpw_lsn == InvalidXLogRecPtr || page_lsn < *fpw_lsn {
                    *fpw_lsn = page_lsn;
                }
            }
        }

        /* Determine if the buffer data needs to be included */
        if (*regbuf).rdata_len == 0 {
            needs_data = false;
        } else if ((*regbuf).flags & REGBUF_KEEP_DATA) != 0 {
            needs_data = true;
        } else {
            needs_data = !needs_backup;
        }

        bkpb.id = block_id as u8;
        bkpb.fork_flags = (*regbuf).forkno as u8;
        bkpb.data_length = 0;

        if ((*regbuf).flags & REGBUF_WILL_INIT) == REGBUF_WILL_INIT {
            bkpb.fork_flags |= BKPBLOCK_WILL_INIT;
        }

        /*
         * If needs_backup is true or WAL checking is enabled for current
         * resource manager, log a full-page write for the current block.
         */
        include_image = needs_backup || (info & XLR_CHECK_CONSISTENCY) != 0;

        if include_image {
            let page: *const c_char = (*regbuf).page;
            let mut compressed_len: u16 = 0;

            /*
             * The page needs to be backed up, so calculate its hole length
             * and offset.
             */
            if ((*regbuf).flags & REGBUF_STANDARD) != 0 {
                /* Assume we can omit data between pd_lower and pd_upper */
                let lower: u16 = (*(page as PageHeader)).pd_lower;
                let upper: u16 = (*(page as PageHeader)).pd_upper;

                if lower >= SizeOfPageHeaderData as u16
                    && upper > lower
                    && (upper as usize) <= BLCKSZ
                {
                    bimg.hole_offset = lower;
                    cbimg.hole_length = upper - lower;
                } else {
                    /* No "hole" to remove */
                    bimg.hole_offset = 0;
                    cbimg.hole_length = 0;
                }
            } else {
                /* Not a standard page header, don't try to eliminate "hole" */
                bimg.hole_offset = 0;
                cbimg.hole_length = 0;
            }

            /*
             * Try to compress a block image if wal_compression is enabled
             */
            if wal_compression != WalCompression::WAL_COMPRESSION_NONE as i32 {
                is_compressed = XLogCompressBackupBlock(
                    page,
                    bimg.hole_offset,
                    cbimg.hole_length,
                    (*regbuf).compressed_page.as_mut_ptr() as *mut c_void,
                    &raw mut compressed_len,
                );
            }

            /*
             * Fill in the remaining fields in the XLogRecordBlockHeader
             * struct
             */
            bkpb.fork_flags |= BKPBLOCK_HAS_IMAGE;

            /* Report a full page image constructed for the WAL record */
            *num_fpi += 1;

            /*
             * Construct XLogRecData entries for the page content.
             */
            (*rdt_datas_last).next = &raw mut (*regbuf).bkp_rdatas[0];
            rdt_datas_last = (*rdt_datas_last).next;

            bimg.bimg_info = if cbimg.hole_length == 0 {
                0
            } else {
                BKPIMAGE_HAS_HOLE
            };

            /*
             * If WAL consistency checking is enabled for the resource manager
             * of this WAL record, a full-page image is included in the record
             * for the block modified. During redo, the full-page is replayed
             * only if BKPIMAGE_APPLY is set.
             */
            if needs_backup {
                bimg.bimg_info |= BKPIMAGE_APPLY;
            }

            if is_compressed {
                /* The current compression is stored in the WAL record */
                bimg.length = compressed_len;

                /* Set the compression method used for this block */
                match wal_compression {
                    v if v == WalCompression::WAL_COMPRESSION_PGLZ as i32 => {
                        bimg.bimg_info |= BKPIMAGE_COMPRESS_PGLZ;
                    }
                    v if v == WalCompression::WAL_COMPRESSION_LZ4 as i32 => {
                        // !defined(USE_LZ4) branch
                        elog!(ERROR, "LZ4 is not supported by this build");
                        // gated: #ifdef USE_LZ4 bimg.bimg_info |= BKPIMAGE_COMPRESS_LZ4
                    }
                    v if v == WalCompression::WAL_COMPRESSION_ZSTD as i32 => {
                        // !defined(USE_ZSTD) branch
                        elog!(ERROR, "zstd is not supported by this build");
                        // gated: #ifdef USE_ZSTD bimg.bimg_info |= BKPIMAGE_COMPRESS_ZSTD
                    }
                    _ => {
                        /* WAL_COMPRESSION_NONE: cannot happen */
                        Assert!(false);
                    }
                }

                (*rdt_datas_last).data =
                    (*regbuf).compressed_page.as_ptr() as *const c_void;
                (*rdt_datas_last).len = compressed_len as u32;
            } else {
                bimg.length = (BLCKSZ - cbimg.hole_length as usize) as u16;

                if cbimg.hole_length == 0 {
                    (*rdt_datas_last).data = page as *const c_void;
                    (*rdt_datas_last).len = BLCKSZ as u32;
                } else {
                    /* must skip the hole */
                    (*rdt_datas_last).data = page as *const c_void;
                    (*rdt_datas_last).len = bimg.hole_offset as u32;

                    (*rdt_datas_last).next = &raw mut (*regbuf).bkp_rdatas[1];
                    rdt_datas_last = (*rdt_datas_last).next;

                    (*rdt_datas_last).data = page.add(
                        bimg.hole_offset as usize + cbimg.hole_length as usize,
                    ) as *const c_void;
                    (*rdt_datas_last).len = (BLCKSZ
                        - (bimg.hole_offset as usize + cbimg.hole_length as usize))
                        as u32;
                }
            }

            total_len += bimg.length as u64;
        }

        if needs_data {
            /*
             * When copying to XLogRecordBlockHeader, the length is narrowed
             * to a uint16.  Double-check that it is still correct.
             */
            Assert!((*regbuf).rdata_len <= u16::MAX as u32);

            /*
             * Link the caller-supplied rdata chain for this buffer to the
             * overall list.
             */
            bkpb.fork_flags |= BKPBLOCK_HAS_DATA;
            bkpb.data_length = (*regbuf).rdata_len as u16;
            total_len += (*regbuf).rdata_len as u64;

            (*rdt_datas_last).next = (*regbuf).rdata_head;
            rdt_datas_last = (*regbuf).rdata_tail;
        }

        if !prev_regbuf.is_null()
            && RelFileLocatorEquals(&(*prev_regbuf).rlocator, &(*regbuf).rlocator)
        {
            samerel = true;
            bkpb.fork_flags |= BKPBLOCK_SAME_REL;
        } else {
            samerel = false;
        }
        prev_regbuf = regbuf;

        /* Ok, copy the header to the scratch buffer */
        copy_nonoverlapping(
            &bkpb as *const XLogRecordBlockHeader as *const u8,
            scratch as *mut u8,
            SizeOfXLogRecordBlockHeader(),
        );
        scratch = scratch.add(SizeOfXLogRecordBlockHeader());
        if include_image {
            copy_nonoverlapping(
                &bimg as *const XLogRecordBlockImageHeader as *const u8,
                scratch as *mut u8,
                SizeOfXLogRecordBlockImageHeader(),
            );
            scratch = scratch.add(SizeOfXLogRecordBlockImageHeader());
            if cbimg.hole_length != 0 && is_compressed {
                copy_nonoverlapping(
                    &cbimg as *const XLogRecordBlockCompressHeader as *const u8,
                    scratch as *mut u8,
                    SizeOfXLogRecordBlockCompressHeader(),
                );
                scratch = scratch.add(SizeOfXLogRecordBlockCompressHeader());
            }
        }
        if !samerel {
            copy_nonoverlapping(
                &(*regbuf).rlocator as *const RelFileLocator as *const u8,
                scratch as *mut u8,
                size_of::<RelFileLocator>(),
            );
            scratch = scratch.add(size_of::<RelFileLocator>());
        }
        copy_nonoverlapping(
            &(*regbuf).block as *const BlockNumber as *const u8,
            scratch as *mut u8,
            size_of::<BlockNumber>(),
        );
        scratch = scratch.add(size_of::<BlockNumber>());

        block_id += 1;
    } /* end while block_id */

    /* followed by the record's origin, if any */
    if (curinsert_flags & XLOG_INCLUDE_ORIGIN) != 0
        && replorigin_session_origin != InvalidRepOriginId
    {
        *scratch = XLR_BLOCK_ID_ORIGIN as c_char;
        scratch = scratch.add(1);
        copy_nonoverlapping(
            &replorigin_session_origin as *const RepOriginId as *const u8,
            scratch as *mut u8,
            size_of::<RepOriginId>(),
        );
        scratch = scratch.add(size_of::<RepOriginId>());
    }

    /* followed by toplevel XID, if not already included in previous record */
    if IsSubxactTopXidLogPending() {
        let xid: TransactionId = GetTopTransactionIdIfAny();

        /* Set the flag that the top xid is included in the WAL */
        *topxid_included = true;

        *scratch = XLR_BLOCK_ID_TOPLEVEL_XID as c_char;
        scratch = scratch.add(1);
        copy_nonoverlapping(
            &xid as *const TransactionId as *const u8,
            scratch as *mut u8,
            size_of::<TransactionId>(),
        );
        scratch = scratch.add(size_of::<TransactionId>());
    }

    /* followed by main data, if any */
    if mainrdata_len > 0 {
        if mainrdata_len > 255 {
            if mainrdata_len > u32::MAX as u64 {
                ereport!(
                    ERROR,
                    errmsg!(
                        "too much WAL data: main data length is {} bytes for a maximum of {} bytes",
                        mainrdata_len,
                        u32::MAX
                    )
                );
            }
            let mainrdata_len_4b: u32 = mainrdata_len as u32;
            *scratch = XLR_BLOCK_ID_DATA_LONG as c_char;
            scratch = scratch.add(1);
            copy_nonoverlapping(
                &mainrdata_len_4b as *const u32 as *const u8,
                scratch as *mut u8,
                size_of::<u32>(),
            );
            scratch = scratch.add(size_of::<u32>());
        } else {
            *scratch = XLR_BLOCK_ID_DATA_SHORT as c_char;
            scratch = scratch.add(1);
            *scratch = mainrdata_len as u8 as c_char;
            scratch = scratch.add(1);
        }
        (*rdt_datas_last).next = mainrdata_head;
        rdt_datas_last = mainrdata_last;
        total_len += mainrdata_len;
    }
    (*rdt_datas_last).next = null_mut();

    (*(&raw mut hdr_rdt)).len = scratch.offset_from(hdr_scratch) as u32;
    total_len += (*(&raw mut hdr_rdt)).len as u64;

    /*
     * Calculate CRC of the data
     *
     * Note that the record header isn't added into the CRC initially since we
     * don't know the prev-link yet.  Thus, the CRC will represent the CRC of
     * the whole record in the order: rdata, then backup blocks, then record
     * header.
     */
    rdata_crc = INIT_CRC32C();
    rdata_crc = COMP_CRC32C(
        rdata_crc,
        hdr_scratch.add(SizeOfXLogRecord()) as *const c_void,
        (*(&raw const hdr_rdt)).len as usize - SizeOfXLogRecord(),
    );
    let mut rdt: *mut XLogRecData = (*(&raw mut hdr_rdt)).next;
    while !rdt.is_null() {
        rdata_crc = COMP_CRC32C(rdata_crc, (*rdt).data, (*rdt).len as usize);
        rdt = (*rdt).next;
    }

    /*
     * Ensure that the XLogRecord is not too large.
     *
     * XLogReader machinery is only able to handle records up to a certain
     * size (ignoring machine resource limitations), so make sure that we will
     * not emit records larger than the sizes advertised to be supported.
     */
    if total_len > XLogRecordMaxSize as u64 {
        ereport!(
            ERROR,
            errmsg!(
                "oversized WAL record: WAL record would be {} bytes (of maximum {} bytes); rmid {} flags {}",
                total_len,
                XLogRecordMaxSize,
                rmid,
                info
            )
        );
    }

    /*
     * Fill in the fields in the record header. Prev-link is filled in later,
     * once we know where in the WAL the record will be inserted. The CRC does
     * not include the record header yet.
     */
    (*rechdr).xl_xid = GetCurrentTransactionIdIfAny();
    (*rechdr).xl_tot_len = total_len as u32;
    (*rechdr).xl_info = info;
    (*rechdr).xl_rmid = rmid;
    (*rechdr).xl_prev = InvalidXLogRecPtr;
    (*rechdr).xl_crc = rdata_crc;

    &raw mut hdr_rdt
}

// --------------------------------------------------------------------------
// XLogCompressBackupBlock  (internal)
// --------------------------------------------------------------------------

/// Create a compressed version of a backup block image.
///
/// Returns false if compression fails (i.e., compressed result is actually
/// bigger than original). Otherwise, returns true and sets 'dlen' to
/// the length of compressed block image.
unsafe fn XLogCompressBackupBlock(
    page: *const c_char,
    hole_offset: u16,
    hole_length: u16,
    dest: *mut c_void,
    dlen: *mut u16,
) -> bool {
    let orig_len: i32 = BLCKSZ as i32 - hole_length as i32;
    let mut len: i32 = -1;
    let extra_bytes: i32;
    let source: *const c_void;
    let mut tmp: PGAlignedBlock = zeroed();

    if hole_length != 0 {
        /* must skip the hole */
        copy_nonoverlapping(
            page as *const u8,
            tmp.data.as_mut_ptr() as *mut u8,
            hole_offset as usize,
        );
        copy_nonoverlapping(
            page.add(hole_offset as usize + hole_length as usize) as *const u8,
            (tmp.data.as_mut_ptr() as *mut u8).add(hole_offset as usize),
            BLCKSZ - (hole_length as usize + hole_offset as usize),
        );
        source = tmp.data.as_ptr() as *const c_void;

        /*
         * Extra data needs to be stored in WAL record for the compressed
         * version of block image if the hole exists.
         */
        extra_bytes = SizeOfXLogRecordBlockCompressHeader() as i32;
    } else {
        source = page as *const c_void;
        extra_bytes = 0;
    }

    match wal_compression {
        v if v == WalCompression::WAL_COMPRESSION_PGLZ as i32 => {
            len = pglz_compress(source as *const c_char, orig_len, dest as *mut c_char, PGLZ_strategy_default);
        }
        v if v == WalCompression::WAL_COMPRESSION_LZ4 as i32 => {
            // !defined(USE_LZ4) branch
            elog!(ERROR, "LZ4 is not supported by this build");
            // gated: len = LZ4_compress_default(source, dest, orig_len, COMPRESS_BUFSIZE)
        }
        v if v == WalCompression::WAL_COMPRESSION_ZSTD as i32 => {
            // !defined(USE_ZSTD) branch
            elog!(ERROR, "zstd is not supported by this build");
            // gated: len = ZSTD_compress(dest, COMPRESS_BUFSIZE, source, orig_len, ZSTD_CLEVEL_DEFAULT)
        }
        _ => {
            /* WAL_COMPRESSION_NONE: cannot happen */
            Assert!(false);
        }
    }

    /*
     * We recheck the actual size even if compression reports success and see
     * if the number of bytes saved by compression is larger than the length
     * of extra data needed for the compressed version of block image.
     */
    if len >= 0 && len + extra_bytes < orig_len {
        *dlen = len as u16; /* successful compression */
        return true;
    }
    false
}

// --------------------------------------------------------------------------
// XLogCheckBufferNeedsBackup
// --------------------------------------------------------------------------

/// Determine whether the buffer referenced has to be backed up.
///
/// Since we don't yet have the insert lock, fullPageWrites and runningBackups
/// (which forces full-page writes) could change later, so the result should
/// be used for optimization purposes only.
pub unsafe fn XLogCheckBufferNeedsBackup(buffer: Buffer) -> bool {
    let mut redo_rec_ptr: XLogRecPtr = 0;
    let mut do_page_writes: bool = false;
    let page: Page;

    GetFullPageWriteInfo(&raw mut redo_rec_ptr, &raw mut do_page_writes);

    page = BufferGetPage(buffer);

    if do_page_writes && PageGetLSN(page) <= redo_rec_ptr {
        return true; /* buffer requires backup */
    }

    false /* buffer does not need to be backed up */
}

// --------------------------------------------------------------------------
// XLogSaveBufferForHint
// --------------------------------------------------------------------------

/// Write a backup block if needed when we are setting a hint. Note that
/// this may be called for a variety of page types, not just heaps.
///
/// Callable while holding just share lock on the buffer content.
///
/// We can't use the plain backup block mechanism since that relies on the
/// Buffer being exclusively locked. Since some modifications (setting LSN, hint
/// bits) are allowed in a sharelocked buffer that can lead to wal checksum
/// failures. So instead we copy the page and insert the copied data as normal
/// record data.
///
/// We only need to do something if page has not yet been full page written in
/// this checkpoint round. The LSN of the inserted wal record is returned if we
/// had to write, InvalidXLogRecPtr otherwise.
///
/// It is possible that multiple concurrent backends could attempt to write WAL
/// records. In that case, multiple copies of the same block would be recorded
/// in separate WAL records by different backends, though that is still OK from
/// a correctness perspective.
pub unsafe fn XLogSaveBufferForHint(buffer: Buffer, buffer_std: bool) -> XLogRecPtr {
    let mut recptr: XLogRecPtr = InvalidXLogRecPtr;
    let lsn: XLogRecPtr;
    let redo_rec_ptr: XLogRecPtr;

    /*
     * Ensure no checkpoint can change our view of RedoRecPtr.
     */
    Assert!(((*MyProc).delayChkptFlags & DELAY_CHKPT_START) != 0);

    /*
     * Update RedoRecPtr so that we can make the right decision
     */
    redo_rec_ptr = GetRedoRecPtr();

    /*
     * We assume page LSN is first data on *every* page that can be passed to
     * XLogInsert, whether it has the standard page layout or not. Since we're
     * only holding a share-lock on the page, we must take the buffer header
     * lock when we look at the LSN.
     */
    lsn = BufferGetLSNAtomic(buffer);

    if lsn <= redo_rec_ptr {
        let mut flags: u8 = 0;
        let mut copied_buffer: PGAlignedBlock = zeroed();
        let origdata: *mut c_char = BufferGetBlock(buffer);
        let mut rlocator: RelFileLocator = zeroed();
        let mut forkno: ForkNumber = 0;
        let mut blkno: BlockNumber = 0;

        /*
         * Copy buffer so we don't have to worry about concurrent hint bit or
         * lsn updates. We assume pd_lower/upper cannot be changed without an
         * exclusive lock, so the contents bkp are not racy.
         */
        if buffer_std {
            /* Assume we can omit data between pd_lower and pd_upper */
            let page: Page = BufferGetPage(buffer);
            let lower: u16 = (*(page as PageHeader)).pd_lower;
            let upper: u16 = (*(page as PageHeader)).pd_upper;

            copy_nonoverlapping(
                origdata as *const u8,
                copied_buffer.data.as_mut_ptr() as *mut u8,
                lower as usize,
            );
            copy_nonoverlapping(
                origdata.add(upper as usize) as *const u8,
                (copied_buffer.data.as_mut_ptr() as *mut u8).add(upper as usize),
                BLCKSZ - upper as usize,
            );
        } else {
            copy_nonoverlapping(
                origdata as *const u8,
                copied_buffer.data.as_mut_ptr() as *mut u8,
                BLCKSZ,
            );
        }

        XLogBeginInsert();

        if buffer_std {
            flags |= REGBUF_STANDARD;
        }

        BufferGetTag(buffer, &raw mut rlocator, &raw mut forkno, &raw mut blkno);
        XLogRegisterBlock(0, &raw mut rlocator, forkno, blkno, copied_buffer.data.as_ptr(), flags);

        recptr = XLogInsert(RM_XLOG_ID, XLOG_FPI_FOR_HINT);
    }

    recptr
}

// --------------------------------------------------------------------------
// log_newpage
// --------------------------------------------------------------------------

/// Write a WAL record containing a full image of a page. Caller is responsible
/// for writing the page to disk after calling this routine.
///
/// Note: If you're using this function, you should be building pages in private
/// memory and writing them directly to smgr.  If you're using buffers, call
/// log_newpage_buffer instead.
///
/// If the page follows the standard page layout, with a PageHeader and unused
/// space between pd_lower and pd_upper, set 'page_std' to true. That allows
/// the unused space to be left out from the WAL record, making it smaller.
pub unsafe fn log_newpage(
    rlocator: *mut RelFileLocator,
    forknum: ForkNumber,
    blkno: BlockNumber,
    page: Page,
    page_std: bool,
) -> XLogRecPtr {
    let mut flags: u8;
    let recptr: XLogRecPtr;

    flags = REGBUF_FORCE_IMAGE;
    if page_std {
        flags |= REGBUF_STANDARD;
    }

    XLogBeginInsert();
    XLogRegisterBlock(0, rlocator, forknum, blkno, page, flags);
    recptr = XLogInsert(RM_XLOG_ID, XLOG_FPI);

    /*
     * The page may be uninitialized. If so, we can't set the LSN because that
     * would corrupt the page.
     */
    if !PageIsNew(page) {
        PageSetLSN(page, recptr);
    }

    recptr
}

// --------------------------------------------------------------------------
// log_newpages
// --------------------------------------------------------------------------

/// Like log_newpage(), but allows logging multiple pages in one operation.
/// It is more efficient than calling log_newpage() for each page separately,
/// because we can write multiple pages in a single WAL record.
pub unsafe fn log_newpages(
    rlocator: *mut RelFileLocator,
    forknum: ForkNumber,
    num_pages: c_int,
    blknos: *mut BlockNumber,
    pages: *mut Page,
    page_std: bool,
) {
    let mut flags: u8;
    let mut recptr: XLogRecPtr;
    let mut i: c_int;
    let mut j: c_int;

    flags = REGBUF_FORCE_IMAGE;
    if page_std {
        flags |= REGBUF_STANDARD;
    }

    /*
     * Iterate over all the pages. They are collected into batches of
     * XLR_MAX_BLOCK_ID pages, and a single WAL-record is written for each
     * batch.
     */
    XLogEnsureRecordSpace(XLR_MAX_BLOCK_ID - 1, 0);

    i = 0;
    while i < num_pages {
        let batch_start: c_int = i;
        let nbatch: c_int;

        XLogBeginInsert();

        let mut nbatch_mut: c_int = 0;
        while nbatch_mut < XLR_MAX_BLOCK_ID && i < num_pages {
            XLogRegisterBlock(
                nbatch_mut as u8,
                rlocator,
                forknum,
                *blknos.add(i as usize),
                *pages.add(i as usize),
                flags,
            );
            i += 1;
            nbatch_mut += 1;
        }
        nbatch = nbatch_mut;

        recptr = XLogInsert(RM_XLOG_ID, XLOG_FPI);

        j = batch_start;
        while j < i {
            /*
             * The page may be uninitialized. If so, we can't set the LSN
             * because that would corrupt the page.
             */
            if !PageIsNew(*pages.add(j as usize)) {
                PageSetLSN(*pages.add(j as usize), recptr);
            }
            j += 1;
        }
    }
}

// --------------------------------------------------------------------------
// log_newpage_buffer
// --------------------------------------------------------------------------

/// Write a WAL record containing a full image of a page.
///
/// Caller should initialize the buffer and mark it dirty before calling this
/// function.  This function will set the page LSN.
///
/// If the page follows the standard page layout, with a PageHeader and unused
/// space between pd_lower and pd_upper, set 'page_std' to true. That allows
/// the unused space to be left out from the WAL record, making it smaller.
pub unsafe fn log_newpage_buffer(buffer: Buffer, page_std: bool) -> XLogRecPtr {
    let page: Page = BufferGetPage(buffer);
    let mut rlocator: RelFileLocator = zeroed();
    let mut forknum: ForkNumber = 0;
    let mut blkno: BlockNumber = 0;

    /* Shared buffers should be modified in a critical section. */
    Assert!(CritSectionCount > 0);

    BufferGetTag(buffer, &raw mut rlocator, &raw mut forknum, &raw mut blkno);

    log_newpage(&raw mut rlocator, forknum, blkno, page, page_std)
}

// --------------------------------------------------------------------------
// log_newpage_range
// --------------------------------------------------------------------------

/// WAL-log a range of blocks in a relation.
///
/// An image of all pages with block numbers 'startblk' <= X < 'endblk' is
/// written to the WAL. If the range is large, this is done in multiple WAL
/// records.
///
/// If all page follows the standard page layout, with a PageHeader and unused
/// space between pd_lower and pd_upper, set 'page_std' to true. That allows
/// the unused space to be left out from the WAL records, making them smaller.
///
/// NOTE: This function acquires exclusive-locks on the pages. Typically, this
/// is used on a newly-built relation, and the caller is holding a
/// AccessExclusiveLock on it, so no other backend can be accessing it at the
/// same time. If that's not the case, you must ensure that this does not
/// cause a deadlock through some other means.
pub unsafe fn log_newpage_range(
    rel: Relation,
    forknum: ForkNumber,
    startblk: BlockNumber,
    endblk: BlockNumber,
    page_std: bool,
) {
    let mut flags: u8;
    let mut blkno: BlockNumber;

    flags = REGBUF_FORCE_IMAGE;
    if page_std {
        flags |= REGBUF_STANDARD;
    }

    /*
     * Iterate over all the pages in the range. They are collected into
     * batches of XLR_MAX_BLOCK_ID pages, and a single WAL-record is written
     * for each batch.
     */
    XLogEnsureRecordSpace(XLR_MAX_BLOCK_ID - 1, 0);

    blkno = startblk;
    while blkno < endblk {
        let mut bufpack: [Buffer; 32] = [0; 32]; /* XLR_MAX_BLOCK_ID = 32 */
        let recptr: XLogRecPtr;
        let mut nbufs: c_int;
        let mut i: c_int;

        CHECK_FOR_INTERRUPTS();

        /* Collect a batch of blocks. */
        nbufs = 0;
        while (nbufs as usize) < XLR_MAX_BLOCK_ID as usize && blkno < endblk {
            let buf: Buffer = ReadBufferExtended(rel, forknum, blkno, 2 /* RBM_NORMAL */, null_mut());

            LockBuffer(buf, 2 /* BUFFER_LOCK_EXCLUSIVE */);

            /*
             * Completely empty pages are not WAL-logged. Writing a WAL record
             * would change the LSN, and we don't want that. We want the page
             * to stay empty.
             */
            if !PageIsNew(BufferGetPage(buf)) {
                bufpack[nbufs as usize] = buf;
                nbufs += 1;
            } else {
                UnlockReleaseBuffer(buf);
            }
            blkno += 1;
        }

        /* Nothing more to do if all remaining blocks were empty. */
        if nbufs == 0 {
            break;
        }

        /* Write WAL record for this batch. */
        XLogBeginInsert();

        // START_CRIT_SECTION()
        CritSectionCount += 1;
        i = 0;
        while i < nbufs {
            MarkBufferDirty(bufpack[i as usize]);
            XLogRegisterBuffer(i as u8, bufpack[i as usize], flags);
            i += 1;
        }

        recptr = XLogInsert(RM_XLOG_ID, XLOG_FPI);

        i = 0;
        while i < nbufs {
            PageSetLSN(BufferGetPage(bufpack[i as usize]), recptr);
            UnlockReleaseBuffer(bufpack[i as usize]);
            i += 1;
        }
        // END_CRIT_SECTION()
        debug_assert!(CritSectionCount > 0);
        CritSectionCount -= 1;
    }
}

// --------------------------------------------------------------------------
// InitXLogInsert
// --------------------------------------------------------------------------

/// Allocate working buffers needed for WAL record construction.
pub unsafe fn InitXLogInsert() {
    #[cfg(debug_assertions)]
    {
        /*
         * Check that any records assembled can be decoded.  This is capped based
         * on what XLogReader would require at its maximum bound.  The XLOG_BLCKSZ
         * addend covers the larger allocate_recordbuf() demand.  This code path
         * is called once per backend, more than enough for this check.
         */
        use crate::pg_config::XLOG_BLCKSZ;
        let max_required: usize =
            DecodeXLogRecordRequiredSpace(XLogRecordMaxSize + XLOG_BLCKSZ);
        Assert!(MemAllocSizeIsValid(max_required));
    }

    /* Initialize the working areas */
    if xloginsert_cxt.is_null() {
        xloginsert_cxt = AllocSetContextCreate!(
            TopMemoryContext,
            c"WAL record construction".as_ptr(),
            ALLOCSET_DEFAULT_SIZES
        );
    }

    if registered_buffers.is_null() {
        registered_buffers = MemoryContextAllocZero(
            xloginsert_cxt,
            size_of::<registered_buffer>() * (XLR_NORMAL_MAX_BLOCK_ID as usize + 1),
        ) as *mut registered_buffer;
        max_registered_buffers = XLR_NORMAL_MAX_BLOCK_ID + 1;
    }
    if rdatas.is_null() {
        rdatas = MemoryContextAlloc(
            xloginsert_cxt,
            size_of::<XLogRecData>() * XLR_NORMAL_RDATAS as usize,
        ) as *mut XLogRecData;
        max_rdatas = XLR_NORMAL_RDATAS;
    }

    /*
     * Allocate a buffer to hold the header information for a WAL record.
     */
    if hdr_scratch.is_null() {
        hdr_scratch =
            MemoryContextAllocZero(xloginsert_cxt, HEADER_SCRATCH_SIZE()) as *mut c_char;
    }
}

// --------------------------------------------------------------------------
// XLogInsertAllowed stub
// --------------------------------------------------------------------------

/// TODO(pg-port): real XLogInsertAllowed lives in access/transam/xlog.c
pub unsafe fn XLogInsertAllowed() -> bool {
    unimplemented!("TODO(pg-port): real XLogInsertAllowed lives in access/transam/xlog.c")
}
