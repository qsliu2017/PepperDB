//! Translated from PostgreSQL src/include/access/xlogrecord.h
//! Definitions for the WAL record format. On-disk (WAL) layouts.

use bitflags::bitflags;
use crate::access::rmgr::RmgrId;
use crate::access::xlogdefs::XLogRecPtr;
use crate::c::TransactionId;
use crate::port::pg_crc32c::pg_crc32c;
use crate::storage::block::BlockNumber;
use crate::storage::relfilelocator::RelFileLocator;

/// Fixed-size header that begins every XLOG record. On-disk; starts on a
/// MAXALIGN boundary in WAL, but the following block/data headers are unaligned.
#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct XLogRecord {
    pub xl_tot_len: u32,       // total len of entire record
    pub xl_xid: TransactionId, // xact id
    pub xl_prev: XLogRecPtr,   // ptr to previous record in log
    pub xl_info: u8,           // flag bits, see XLR_* below
    pub xl_rmid: RmgrId,       // resource manager for this record
    // 2 bytes of padding here, initialize to zero
    pub xl_crc: pg_crc32c,     // CRC for this record
    // XLogRecordBlockHeaders and XLogRecordDataHeader follow, no padding
}

const _: () = assert!(core::mem::size_of::<XLogRecord>() == 24);
const _: () = assert!(core::mem::offset_of!(XLogRecord, xl_tot_len) == 0);
const _: () = assert!(core::mem::offset_of!(XLogRecord, xl_xid) == 4);
const _: () = assert!(core::mem::offset_of!(XLogRecord, xl_prev) == 8);
const _: () = assert!(core::mem::offset_of!(XLogRecord, xl_info) == 16);
const _: () = assert!(core::mem::offset_of!(XLogRecord, xl_rmid) == 17);
const _: () = assert!(core::mem::offset_of!(XLogRecord, xl_crc) == 20);

/// offsetof(XLogRecord, xl_crc) + sizeof(pg_crc32c)
pub const SizeOfXLogRecord: usize =
    core::mem::offset_of!(XLogRecord, xl_crc) + core::mem::size_of::<pg_crc32c>();

// The high 4 bits in xl_info may be used freely by rmgr; the low 4 bits carry
// the XLR_* flags below (set internally by XLogInsert, except SPECIAL_REL_UPDATE
// and CHECK_CONSISTENCY which the caller may pass).
pub const XLR_INFO_MASK: u8 = 0x0F;
pub const XLR_RMGR_INFO_MASK: u8 = 0xF0;

/// Max single WAL record size (allows ~4M of XLogReader allocation overhead).
pub const XLogRecordMaxSize: u32 = 1020 * 1024 * 1024;

/// Set when a record modifies relation files outside the usual block references.
pub const XLR_SPECIAL_REL_UPDATE: u8 = 0x01;
/// Enforce consistency checks of replayed WAL at recovery (logs FPI per block).
pub const XLR_CHECK_CONSISTENCY: u8 = 0x02;

/// Header for block data appended to an XLOG record. On-disk; intentionally
/// unaligned in WAL, so copy to aligned storage before struct access.
#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct XLogRecordBlockHeader {
    pub id: u8,           // block reference ID
    pub fork_flags: u8,   // fork within the relation, and flags (see BKPBLOCK_*)
    pub data_length: u16, // payload bytes (not including page image)
    // If BKPBLOCK_HAS_IMAGE, an XLogRecordBlockImageHeader follows.
    // If BKPBLOCK_SAME_REL is not set, a RelFileLocator follows.
    // BlockNumber follows.
}

const _: () = assert!(core::mem::size_of::<XLogRecordBlockHeader>() == 4);

pub const SizeOfXLogRecordBlockHeader: usize =
    core::mem::offset_of!(XLogRecordBlockHeader, data_length) + core::mem::size_of::<u16>();

/// Additional header when a full-page image is included (BKPBLOCK_HAS_IMAGE).
/// On-disk (unaligned in WAL).
#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct XLogRecordBlockImageHeader {
    pub length: u16,      // number of page image bytes
    pub hole_offset: u16, // number of bytes before "hole"
    pub bimg_info: u8,    // flag bits, see BKPIMAGE_* below
    // If BKPIMAGE_HAS_HOLE and BKPIMAGE_COMPRESSED(), an
    // XLogRecordBlockCompressHeader follows.
}

pub const SizeOfXLogRecordBlockImageHeader: usize =
    core::mem::offset_of!(XLogRecordBlockImageHeader, bimg_info) + core::mem::size_of::<u8>();

bitflags! {
    /// Information stored in bimg_info. Clean single-bit set (compression
    /// methods are independent bits); on-disk-packed but byte-identical.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct BkpImage: u8 {
        const HAS_HOLE      = 0x01; // page image has "hole"
        const APPLY         = 0x02; // page image should be restored during replay
        const COMPRESS_PGLZ = 0x04;
        const COMPRESS_LZ4  = 0x08;
        const COMPRESS_ZSTD = 0x10;
    }
}

impl BkpImage {
    /// True iff any compression method bit is set (C BKPIMAGE_COMPRESSED).
    pub const fn is_compressed(self) -> bool {
        self.intersects(Self::from_bits_retain(
            Self::COMPRESS_PGLZ.bits() | Self::COMPRESS_LZ4.bits() | Self::COMPRESS_ZSTD.bits(),
        ))
    }
}

/// Extra header when a page image has a "hole" and is compressed. On-disk.
#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct XLogRecordBlockCompressHeader {
    pub hole_length: u16, // number of bytes in "hole"
}

pub const SizeOfXLogRecordBlockCompressHeader: usize =
    core::mem::size_of::<XLogRecordBlockCompressHeader>();

/// Max size of the header for a block reference (used to size a temp buffer).
pub const MaxSizeOfXLogRecordBlockHeader: usize = SizeOfXLogRecordBlockHeader
    + SizeOfXLogRecordBlockImageHeader
    + SizeOfXLogRecordBlockCompressHeader
    + core::mem::size_of::<RelFileLocator>()
    + core::mem::size_of::<BlockNumber>();

// fork_flags layout: low 4 bits are the fork *number* (0..15), upper 4 bits are
// flags. This packs a number beside flags and is written verbatim into WAL, so
// per bitflags-port appendix C it stays a raw byte with masks/accessors, NOT a
// bitflags type.
pub const BKPBLOCK_FORK_MASK: u8 = 0x0F;
pub const BKPBLOCK_FLAG_MASK: u8 = 0xF0;
pub const BKPBLOCK_HAS_IMAGE: u8 = 0x10; // block data is an XLogRecordBlockImage
pub const BKPBLOCK_HAS_DATA: u8 = 0x20;
pub const BKPBLOCK_WILL_INIT: u8 = 0x40; // redo will re-init the page
pub const BKPBLOCK_SAME_REL: u8 = 0x80; // RelFileLocator omitted, same as previous

impl XLogRecordBlockHeader {
    /// Fork number stored in the low nibble of fork_flags.
    pub const fn fork_num(&self) -> u8 {
        self.fork_flags & BKPBLOCK_FORK_MASK
    }
    pub const fn has_image(&self) -> bool {
        self.fork_flags & BKPBLOCK_HAS_IMAGE != 0
    }
    pub const fn has_data(&self) -> bool {
        self.fork_flags & BKPBLOCK_HAS_DATA != 0
    }
    pub const fn will_init(&self) -> bool {
        self.fork_flags & BKPBLOCK_WILL_INIT != 0
    }
    pub const fn same_rel(&self) -> bool {
        self.fork_flags & BKPBLOCK_SAME_REL != 0
    }
}

/// Main-data header, short form (data length < 256). On-disk (unaligned).
#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct XLogRecordDataHeaderShort {
    pub id: u8,          // XLR_BLOCK_ID_DATA_SHORT
    pub data_length: u8, // number of payload bytes
}

pub const SizeOfXLogRecordDataHeaderShort: usize = core::mem::size_of::<u8>() * 2;

/// Main-data header, long form. On-disk; followed by an unaligned uint32 length.
#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct XLogRecordDataHeaderLong {
    pub id: u8, // XLR_BLOCK_ID_DATA_LONG
    // followed by uint32 data_length, unaligned
}

pub const SizeOfXLogRecordDataHeaderLong: usize =
    core::mem::size_of::<u8>() + core::mem::size_of::<u32>();

/// Block references are numbered 0..=XLR_MAX_BLOCK_ID; a few high IDs are
/// reserved for the main data portion and replication metadata.
pub const XLR_MAX_BLOCK_ID: u8 = 32;

pub const XLR_BLOCK_ID_DATA_SHORT: u8 = 255;
pub const XLR_BLOCK_ID_DATA_LONG: u8 = 254;
pub const XLR_BLOCK_ID_ORIGIN: u8 = 253;
pub const XLR_BLOCK_ID_TOPLEVEL_XID: u8 = 252;
