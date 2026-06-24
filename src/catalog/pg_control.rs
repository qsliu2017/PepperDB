//! Translated from PostgreSQL src/include/catalog/pg_control.h
//
// ControlFileData IS the on-disk "pg_control" file (despite the catalog/ path it
// is NOT a CATALOG() macro relation). CheckPoint is embedded in it and is also
// the body of a CheckPoint WAL record -> both are on-disk: #[repr(C)], exact
// field order/types, layout asserts. Target: x86_64/aarch64 (LE, 8-byte align).

use crate::access::transam::FullTransactionId;
use crate::access::xlogdefs::{TimeLineID, XLogRecPtr};
use crate::c::{MultiXactId, MultiXactOffset, TransactionId};
use crate::pgtime::pg_time_t;
use crate::port::pg_crc32c::pg_crc32c;
use crate::postgres_ext::Oid;

/// Version identifier for this pg_control format.
pub const PG_CONTROL_VERSION: u32 = 1800;

/// Nonce key length.
pub const MOCK_AUTH_NONCE_LEN: usize = 32;

/// Body of CheckPoint XLOG records; a copy of the latest is kept in pg_control.
/// On-disk: changing this requires a PG_CONTROL_VERSION bump.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct CheckPoint {
    pub redo: XLogRecPtr,                 // REDO start point
    pub ThisTimeLineID: TimeLineID,       // current TLI
    pub PrevTimeLineID: TimeLineID,       // previous TLI
    pub fullPageWrites: bool,             // current full_page_writes
    pub wal_level: i32,                   // current wal_level
    pub nextXid: FullTransactionId,       // next free transaction ID
    pub nextOid: Oid,                     // next free OID
    pub nextMulti: MultiXactId,           // next free MultiXactId
    pub nextMultiOffset: MultiXactOffset, // next free MultiXact offset
    pub oldestXid: TransactionId,         // cluster-wide minimum datfrozenxid
    pub oldestXidDB: Oid,                 // database with minimum datfrozenxid
    pub oldestMulti: MultiXactId,         // cluster-wide minimum datminmxid
    pub oldestMultiDB: Oid,               // database with minimum datminmxid
    pub time: pg_time_t,                  // time stamp of checkpoint
    pub oldestCommitTsXid: TransactionId, // oldest Xid with valid commit ts
    pub newestCommitTsXid: TransactionId, // newest Xid with valid commit ts
    pub oldestActiveXid: TransactionId,   // oldest XID still running
}

// XLOG info values for XLOG rmgr.
pub const XLOG_CHECKPOINT_SHUTDOWN: u8 = 0x00;
pub const XLOG_CHECKPOINT_ONLINE: u8 = 0x10;
pub const XLOG_NOOP: u8 = 0x20;
pub const XLOG_NEXTOID: u8 = 0x30;
pub const XLOG_SWITCH: u8 = 0x40;
pub const XLOG_BACKUP_END: u8 = 0x50;
pub const XLOG_PARAMETER_CHANGE: u8 = 0x60;
pub const XLOG_RESTORE_POINT: u8 = 0x70;
pub const XLOG_FPW_CHANGE: u8 = 0x80;
pub const XLOG_END_OF_RECOVERY: u8 = 0x90;
pub const XLOG_FPI_FOR_HINT: u8 = 0xA0;
pub const XLOG_FPI: u8 = 0xB0;
pub const XLOG_OVERWRITE_CONTRECORD: u8 = 0xD0;
pub const XLOG_CHECKPOINT_REDO: u8 = 0xE0;

/// System status indicator (stored in pg_control). Sequential ordinals -> enum.
/// On-disk: written as a 4-byte int.
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DBState {
    DB_STARTUP = 0,
    DB_SHUTDOWNED,
    DB_SHUTDOWNED_IN_RECOVERY,
    DB_SHUTDOWNING,
    DB_IN_CRASH_RECOVERY,
    DB_IN_ARCHIVE_RECOVERY,
    DB_IN_PRODUCTION,
}

/// float8 sentinel stored in pg_control to check FP compatibility.
pub const FLOATFORMAT_VALUE: f64 = 1234567.0;

/// Contents of pg_control. On-disk control file.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct ControlFileData {
    pub system_identifier: u64,

    pub pg_control_version: u32, // PG_CONTROL_VERSION
    pub catalog_version_no: u32, // see catversion.h

    pub state: DBState,
    pub time: pg_time_t,
    pub checkPoint: XLogRecPtr,

    pub checkPointCopy: CheckPoint,

    pub unloggedLSN: XLogRecPtr,

    pub minRecoveryPoint: XLogRecPtr,
    pub minRecoveryPointTLI: TimeLineID,
    pub backupStartPoint: XLogRecPtr,
    pub backupEndPoint: XLogRecPtr,
    pub backupEndRequired: bool,

    pub wal_level: i32,
    pub wal_log_hints: bool,
    pub MaxConnections: i32,
    pub max_worker_processes: i32,
    pub max_wal_senders: i32,
    pub max_prepared_xacts: i32,
    pub max_locks_per_xact: i32,
    pub track_commit_timestamp: bool,

    pub maxAlign: u32,    // alignment requirement for tuples
    pub floatFormat: f64, // constant 1234567.0

    pub blcksz: u32,      // data block size for this DB
    pub relseg_size: u32, // blocks per segment of large relation

    pub xlog_blcksz: u32,   // block size within WAL files
    pub xlog_seg_size: u32, // size of each WAL segment

    pub nameDataLen: u32,  // catalog name field width
    pub indexMaxKeys: u32, // max number of columns in an index

    pub toast_max_chunk_size: u32, // chunk size in TOAST tables
    pub loblksize: u32,            // chunk size in pg_largeobject

    pub float8ByVal: bool, // float8, int8, etc pass-by-value?

    pub data_checksum_version: u32, // 0 if no checksums

    pub default_char_signedness: bool, // default signedness of char

    pub mock_authentication_nonce: [u8; MOCK_AUTH_NONCE_LEN],

    pub crc: pg_crc32c, // CRC of all above ... MUST BE LAST!
}

/// Maximum safe value of size_of::<ControlFileData>() (one disk sector).
pub const PG_CONTROL_MAX_SAFE_SIZE: usize = 512;

/// Physical size of the pg_control file (kept constant across format changes).
pub const PG_CONTROL_FILE_SIZE: usize = 8192;

// pg_control must fit an atomic single-sector write.
const _: () = assert!(core::mem::size_of::<ControlFileData>() <= PG_CONTROL_MAX_SAFE_SIZE);
const _: () = assert!(core::mem::size_of::<ControlFileData>() <= PG_CONTROL_FILE_SIZE);

// Key layout anchors. pg_control_version must sit 8 bytes into the file.
const _: () = assert!(core::mem::offset_of!(ControlFileData, pg_control_version) == 8);
const _: () = assert!(core::mem::offset_of!(ControlFileData, catalog_version_no) == 12);
