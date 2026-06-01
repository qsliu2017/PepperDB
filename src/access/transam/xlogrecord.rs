//! access/xlogrecord.h - Definitions for the WAL record format.

use std::ffi::c_int;
use std::mem::{offset_of, size_of};

use crate::access::rmgrlist::RmgrId;
use crate::access::transam::xlogdefs::XLogRecPtr;
use crate::c::{uint16, uint32, uint8, TransactionId};
use crate::port::pg_crc32c::pg_crc32c;
use crate::storage::block::BlockNumber;
use crate::storage::relfilelocator::RelFileLocator;

/*
 * The overall layout of an XLOG record is:
 *		Fixed-size header (XLogRecord struct)
 *		XLogRecordBlockHeader struct
 *		XLogRecordBlockHeader struct
 *		...
 *		XLogRecordDataHeader[Short|Long] struct
 *		block data
 *		block data
 *		...
 *		main data
 *
 * There can be zero or more XLogRecordBlockHeaders, and 0 or more bytes of
 * rmgr-specific data not associated with a block.  XLogRecord structs
 * always start on MAXALIGN boundaries in the WAL files, but the rest of
 * the fields are not aligned.
 *
 * The XLogRecordBlockHeader, XLogRecordDataHeaderShort and
 * XLogRecordDataHeaderLong structs all begin with a single 'id' byte. It's
 * used to distinguish between block references, and the main data structs.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct XLogRecord {
    pub xl_tot_len: uint32,     /* total len of entire record */
    pub xl_xid: TransactionId,  /* xact id */
    pub xl_prev: XLogRecPtr,    /* ptr to previous record in log */
    pub xl_info: uint8,         /* flag bits, see below */
    pub xl_rmid: RmgrId,        /* resource manager for this record */
    /* 2 bytes of padding here, initialize to zero */
    pub xl_crc: pg_crc32c,      /* CRC for this record */
    /* XLogRecordBlockHeaders and XLogRecordDataHeader follow, no padding */
}

#[inline]
pub const fn SizeOfXLogRecord() -> usize {
    offset_of!(XLogRecord, xl_crc) + size_of::<pg_crc32c>()
}

/*
 * The high 4 bits in xl_info may be used freely by rmgr. The
 * XLR_SPECIAL_REL_UPDATE and XLR_CHECK_CONSISTENCY bits can be passed by
 * XLogInsert caller. The rest are set internally by XLogInsert.
 */
pub const XLR_INFO_MASK: uint8 = 0x0F;
pub const XLR_RMGR_INFO_MASK: uint8 = 0xF0;

/*
 * XLogReader needs to allocate all the data of a WAL record in a single
 * chunk.  This means that a single XLogRecord cannot exceed MaxAllocSize
 * in length if we ignore any allocation overhead of the XLogReader.
 *
 * To accommodate some overhead, this value allows for 4M of allocation
 * overhead, that should be plenty enough for what the XLogReader
 * infrastructure expects as extra.
 */
pub const XLogRecordMaxSize: usize = 1020 * 1024 * 1024;

/*
 * If a WAL record modifies any relation files, in ways not covered by the
 * usual block references, this flag is set. This is not used for anything
 * by PostgreSQL itself, but it allows external tools that read WAL and keep
 * track of modified blocks to recognize such special record types.
 */
pub const XLR_SPECIAL_REL_UPDATE: uint8 = 0x01;

/*
 * Enforces consistency checks of replayed WAL at recovery. If enabled,
 * each record will log a full-page write for each block modified by the
 * record and will reuse it afterwards for consistency checks. The caller
 * of XLogInsert can use this value if necessary, but if
 * wal_consistency_checking is enabled for a rmgr this is set unconditionally.
 */
pub const XLR_CHECK_CONSISTENCY: uint8 = 0x02;

/*
 * Header info for block data appended to an XLOG record.
 *
 * 'data_length' is the length of the rmgr-specific payload data associated
 * with this block. It does not include the possible full page image, nor
 * XLogRecordBlockHeader struct itself.
 *
 * Note that we don't attempt to align the XLogRecordBlockHeader struct!
 * So, the struct must be copied to aligned local storage before use.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct XLogRecordBlockHeader {
    pub id: uint8,           /* block reference ID */
    pub fork_flags: uint8,   /* fork within the relation, and flags */
    pub data_length: uint16, /* number of payload bytes (not including page image) */
                             /* If BKPBLOCK_HAS_IMAGE, an XLogRecordBlockImageHeader struct follows */
                             /* If BKPBLOCK_SAME_REL is not set, a RelFileLocator follows */
                             /* BlockNumber follows */
}

#[inline]
pub const fn SizeOfXLogRecordBlockHeader() -> usize {
    offset_of!(XLogRecordBlockHeader, data_length) + size_of::<uint16>()
}

/*
 * Additional header information when a full-page image is included
 * (i.e. when BKPBLOCK_HAS_IMAGE is set).
 *
 * The XLOG code is aware that PG data pages usually contain an unused "hole"
 * in the middle, which contains only zero bytes.  Since we know that the
 * "hole" is all zeros, we remove it from the stored data (and it's not counted
 * in the XLOG record's CRC, either).  Hence, the amount of block data actually
 * present is (BLCKSZ - <length of "hole" bytes>).
 *
 * Additionally, when wal_compression is enabled, we will try to compress full
 * page images using one of the supported algorithms, after removing the
 * "hole". This can reduce the WAL volume, but at some extra cost of CPU spent
 * on the compression during WAL logging. In this case, since the "hole"
 * length cannot be calculated by subtracting the number of page image bytes
 * from BLCKSZ, basically it needs to be stored as an extra information.
 * But when no "hole" exists, we can assume that the "hole" length is zero
 * and no such an extra information needs to be stored. Note that
 * the original version of page image is stored in WAL instead of the
 * compressed one if the number of bytes saved by compression is less than
 * the length of extra information. Hence, when a page image is successfully
 * compressed, the amount of block data actually present is less than
 * BLCKSZ - the length of "hole" bytes - the length of extra information.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct XLogRecordBlockImageHeader {
    pub length: uint16,      /* number of page image bytes */
    pub hole_offset: uint16, /* number of bytes before "hole" */
    pub bimg_info: uint8,    /* flag bits, see below */
                             /*
                              * If BKPIMAGE_HAS_HOLE and BKPIMAGE_COMPRESSED(), an
                              * XLogRecordBlockCompressHeader struct follows.
                              */
}

#[inline]
pub const fn SizeOfXLogRecordBlockImageHeader() -> usize {
    offset_of!(XLogRecordBlockImageHeader, bimg_info) + size_of::<uint8>()
}

/* Information stored in bimg_info */
pub const BKPIMAGE_HAS_HOLE: uint8 = 0x01; /* page image has "hole" */
pub const BKPIMAGE_APPLY: uint8 = 0x02; /* page image should be restored during replay */
/* compression methods supported */
pub const BKPIMAGE_COMPRESS_PGLZ: uint8 = 0x04;
pub const BKPIMAGE_COMPRESS_LZ4: uint8 = 0x08;
pub const BKPIMAGE_COMPRESS_ZSTD: uint8 = 0x10;

#[inline]
pub const fn BKPIMAGE_COMPRESSED(info: uint8) -> bool {
    (info & (BKPIMAGE_COMPRESS_PGLZ | BKPIMAGE_COMPRESS_LZ4 | BKPIMAGE_COMPRESS_ZSTD)) != 0
}

/*
 * Extra header information used when page image has "hole" and
 * is compressed.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct XLogRecordBlockCompressHeader {
    pub hole_length: uint16, /* number of bytes in "hole" */
}

#[inline]
pub const fn SizeOfXLogRecordBlockCompressHeader() -> usize {
    size_of::<XLogRecordBlockCompressHeader>()
}

/*
 * Maximum size of the header for a block reference. This is used to size a
 * temporary buffer for constructing the header.
 */
#[inline]
pub const fn MaxSizeOfXLogRecordBlockHeader() -> usize {
    SizeOfXLogRecordBlockHeader()
        + SizeOfXLogRecordBlockImageHeader()
        + SizeOfXLogRecordBlockCompressHeader()
        + size_of::<RelFileLocator>()
        + size_of::<BlockNumber>()
}

/*
 * The fork number fits in the lower 4 bits in the fork_flags field. The upper
 * bits are used for flags.
 */
pub const BKPBLOCK_FORK_MASK: uint8 = 0x0F;
pub const BKPBLOCK_FLAG_MASK: uint8 = 0xF0;
pub const BKPBLOCK_HAS_IMAGE: uint8 = 0x10; /* block data is an XLogRecordBlockImage */
pub const BKPBLOCK_HAS_DATA: uint8 = 0x20;
pub const BKPBLOCK_WILL_INIT: uint8 = 0x40; /* redo will re-init the page */
pub const BKPBLOCK_SAME_REL: uint8 = 0x80; /* RelFileLocator omitted, same as previous */

/*
 * XLogRecordDataHeaderShort/Long are used for the "main data" portion of
 * the record. If the length of the data is less than 256 bytes, the short
 * form is used, with a single byte to hold the length. Otherwise the long
 * form is used.
 *
 * (These structs are currently not used in the code, they are here just for
 * documentation purposes).
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct XLogRecordDataHeaderShort {
    pub id: uint8,          /* XLR_BLOCK_ID_DATA_SHORT */
    pub data_length: uint8, /* number of payload bytes */
}

#[inline]
pub const fn SizeOfXLogRecordDataHeaderShort() -> usize {
    size_of::<uint8>() * 2
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct XLogRecordDataHeaderLong {
    pub id: uint8, /* XLR_BLOCK_ID_DATA_LONG */
                   /* followed by uint32 data_length, unaligned */
}

#[inline]
pub const fn SizeOfXLogRecordDataHeaderLong() -> usize {
    size_of::<uint8>() + size_of::<uint32>()
}

/*
 * Block IDs used to distinguish different kinds of record fragments. Block
 * references are numbered from 0 to XLR_MAX_BLOCK_ID. A rmgr is free to use
 * any ID number in that range (although you should stick to small numbers,
 * because the WAL machinery is optimized for that case). A few ID
 * numbers are reserved to denote the "main" data portion of the record,
 * as well as replication-supporting transaction metadata.
 *
 * The maximum is currently set at 32, quite arbitrarily. Most records only
 * need a handful of block references, but there are a few exceptions that
 * need more.
 */
pub const XLR_MAX_BLOCK_ID: c_int = 32;

pub const XLR_BLOCK_ID_DATA_SHORT: c_int = 255;
pub const XLR_BLOCK_ID_DATA_LONG: c_int = 254;
pub const XLR_BLOCK_ID_ORIGIN: c_int = 253;
pub const XLR_BLOCK_ID_TOPLEVEL_XID: c_int = 252;
