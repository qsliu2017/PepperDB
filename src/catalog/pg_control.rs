//! catalog/pg_control.h - the system control file "pg_control" format.

use std::ffi::c_int;

use crate::access::transam::FullTransactionId;
use crate::access::transam::xlogdefs::{TimeLineID, XLogRecPtr};
use crate::c::{uint32, uint64, MultiXactId, MultiXactOffset, TransactionId};
use crate::pgtime::pg_time_t;
use crate::port::pg_crc32c::pg_crc32c;
use crate::postgres_ext::Oid;

/// Version identifier for this pg_control format.
pub const PG_CONTROL_VERSION: c_int = 1800;

/// Nonce key length, see below.
pub const MOCK_AUTH_NONCE_LEN: usize = 32;

/// Body of CheckPoint XLOG records.  This is declared here because we keep
/// a copy of the latest one in pg_control for possible disaster recovery.
/// Changing this struct requires a PG_CONTROL_VERSION bump.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct CheckPoint {
    /// next RecPtr available when we began to create CheckPoint (i.e. REDO
    /// start point)
    pub redo: XLogRecPtr,
    /// current TLI
    pub ThisTimeLineID: TimeLineID,
    /// previous TLI, if this record begins a new timeline (equals
    /// ThisTimeLineID otherwise)
    pub PrevTimeLineID: TimeLineID,
    /// current full_page_writes
    pub fullPageWrites: bool,
    /// current wal_level
    pub wal_level: c_int,
    /// next free transaction ID
    pub nextXid: FullTransactionId,
    /// next free OID
    pub nextOid: Oid,
    /// next free MultiXactId
    pub nextMulti: MultiXactId,
    /// next free MultiXact offset
    pub nextMultiOffset: MultiXactOffset,
    /// cluster-wide minimum datfrozenxid
    pub oldestXid: TransactionId,
    /// database with minimum datfrozenxid
    pub oldestXidDB: Oid,
    /// cluster-wide minimum datminmxid
    pub oldestMulti: MultiXactId,
    /// database with minimum datminmxid
    pub oldestMultiDB: Oid,
    /// time stamp of checkpoint
    pub time: pg_time_t,
    /// oldest Xid with valid commit timestamp
    pub oldestCommitTsXid: TransactionId,
    /// newest Xid with valid commit timestamp
    pub newestCommitTsXid: TransactionId,

    /// Oldest XID still running. This is only needed to initialize hot standby
    /// mode from an online checkpoint, so we only bother calculating this for
    /// online checkpoints and only when wal_level is replica. Otherwise it's
    /// set to InvalidTransactionId.
    pub oldestActiveXid: TransactionId,
}

/* XLOG info values for XLOG rmgr */
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
/* 0xC0 is used in Postgres 9.5-11 */
pub const XLOG_OVERWRITE_CONTRECORD: u8 = 0xD0;
pub const XLOG_CHECKPOINT_REDO: u8 = 0xE0;

/// System status indicator.  Note this is stored in pg_control; if you change
/// it, you must bump PG_CONTROL_VERSION.
///
/// C enum `DBState` -> `c_int` + variant constants.
pub type DBState = c_int;
pub const DB_STARTUP: DBState = 0;
pub const DB_SHUTDOWNED: DBState = 1;
pub const DB_SHUTDOWNED_IN_RECOVERY: DBState = 2;
pub const DB_SHUTDOWNING: DBState = 3;
pub const DB_IN_CRASH_RECOVERY: DBState = 4;
pub const DB_IN_ARCHIVE_RECOVERY: DBState = 5;
pub const DB_IN_PRODUCTION: DBState = 6;

/// Contents of pg_control.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ControlFileData {
    /// Unique system identifier --- to ensure we match up xlog files with the
    /// installation that produced them.
    pub system_identifier: uint64,

    /*
     * Version identifier information.  Keep these fields at the same offset,
     * especially pg_control_version; they won't be real useful if they move
     * around.  (For historical reasons they must be 8 bytes into the file
     * rather than immediately at the front.)
     *
     * pg_control_version identifies the format of pg_control itself.
     * catalog_version_no identifies the format of the system catalogs.
     *
     * There are additional version identifiers in individual files; for
     * example, WAL logs contain per-page magic numbers that can serve as
     * version cues for the WAL log.
     */
    /// PG_CONTROL_VERSION
    pub pg_control_version: uint32,
    /// see catversion.h
    pub catalog_version_no: uint32,

    /*
     * System status data
     */
    /// see enum above
    pub state: DBState,
    /// time stamp of last pg_control update
    pub time: pg_time_t,
    /// last check point record ptr
    pub checkPoint: XLogRecPtr,

    /// copy of last check point record
    pub checkPointCopy: CheckPoint,

    /// current fake LSN value, for unlogged rels
    pub unloggedLSN: XLogRecPtr,

    /*
     * These two values determine the minimum point we must recover up to
     * before starting up:
     *
     * minRecoveryPoint is updated to the latest replayed LSN whenever we
     * flush a data change during archive recovery. That guards against
     * starting archive recovery, aborting it, and restarting with an earlier
     * stop location. If we've already flushed data changes from WAL record X
     * to disk, we mustn't start up until we reach X again. Zero when not
     * doing archive recovery.
     *
     * backupStartPoint is the redo pointer of the backup start checkpoint, if
     * we are recovering from an online backup and haven't reached the end of
     * backup yet. It is reset to zero when the end of backup is reached, and
     * we mustn't start up before that. A boolean would suffice otherwise, but
     * we use the redo pointer as a cross-check when we see an end-of-backup
     * record, to make sure the end-of-backup record corresponds the base
     * backup we're recovering from.
     *
     * backupEndPoint is the backup end location, if we are recovering from an
     * online backup which was taken from the standby and haven't reached the
     * end of backup yet. It is initialized to the minimum recovery point in
     * pg_control which was backed up last. It is reset to zero when the end
     * of backup is reached, and we mustn't start up before that.
     *
     * If backupEndRequired is true, we know for sure that we're restoring
     * from a backup, and must see a backup-end record before we can safely
     * start up.
     */
    pub minRecoveryPoint: XLogRecPtr,
    pub minRecoveryPointTLI: TimeLineID,
    pub backupStartPoint: XLogRecPtr,
    pub backupEndPoint: XLogRecPtr,
    pub backupEndRequired: bool,

    /*
     * Parameter settings that determine if the WAL can be used for archival
     * or hot standby.
     */
    pub wal_level: c_int,
    pub wal_log_hints: bool,
    pub MaxConnections: c_int,
    pub max_worker_processes: c_int,
    pub max_wal_senders: c_int,
    pub max_prepared_xacts: c_int,
    pub max_locks_per_xact: c_int,
    pub track_commit_timestamp: bool,

    /*
     * This data is used to check for hardware-architecture compatibility of
     * the database and the backend executable.  We need not check endianness
     * explicitly, since the pg_control version will surely look wrong to a
     * machine of different endianness, but we do need to worry about MAXALIGN
     * and floating-point format.  (Note: storage layout nominally also
     * depends on SHORTALIGN and INTALIGN, but in practice these are the same
     * on all architectures of interest.)
     *
     * Testing just one double value is not a very bulletproof test for
     * floating-point compatibility, but it will catch most cases.
     */
    /// alignment requirement for tuples
    pub maxAlign: uint32,
    /// constant 1234567.0
    pub floatFormat: f64,

    /*
     * This data is used to make sure that configuration of this database is
     * compatible with the backend executable.
     */
    /// data block size for this DB
    pub blcksz: uint32,
    /// blocks per segment of large relation
    pub relseg_size: uint32,

    /// block size within WAL files
    pub xlog_blcksz: uint32,
    /// size of each WAL segment
    pub xlog_seg_size: uint32,

    /// catalog name field width
    pub nameDataLen: uint32,
    /// max number of columns in an index
    pub indexMaxKeys: uint32,

    /// chunk size in TOAST tables
    pub toast_max_chunk_size: uint32,
    /// chunk size in pg_largeobject
    pub loblksize: uint32,

    /// float8, int8, etc pass-by-value?
    pub float8ByVal: bool,

    /// Are data pages protected by checksums? Zero if no checksum version
    pub data_checksum_version: uint32,

    /// True if the default signedness of char is "signed" on a platform where
    /// the cluster is initialized.
    pub default_char_signedness: bool,

    /// Random nonce, used in authentication requests that need to proceed
    /// based on values that are cluster-unique, like a SASL exchange that
    /// failed at an early stage.
    pub mock_authentication_nonce: [std::ffi::c_char; MOCK_AUTH_NONCE_LEN],

    /// CRC of all above ... MUST BE LAST!
    pub crc: pg_crc32c,
}

/// `#define FLOATFORMAT_VALUE 1234567.0`
pub const FLOATFORMAT_VALUE: f64 = 1234567.0;

/// Maximum safe value of sizeof(ControlFileData).  For reliability's sake,
/// it's critical that pg_control updates be atomic writes.  That generally
/// means the active data can't be more than one disk sector, which is 512
/// bytes on common hardware.  Be very careful about raising this limit.
pub const PG_CONTROL_MAX_SAFE_SIZE: usize = 512;

/// Physical size of the pg_control file.  Note that this is considerably
/// bigger than the actually used size (ie, sizeof(ControlFileData)).
/// The idea is to keep the physical size constant independent of format
/// changes, so that ReadControlFile will deliver a suitable wrong-version
/// message instead of a read error if it's looking at an incompatible file.
pub const PG_CONTROL_FILE_SIZE: usize = 8192;

/* StaticAssertDecl(sizeof(ControlFileData) <= PG_CONTROL_MAX_SAFE_SIZE, ...) */
const _: () = assert!(std::mem::size_of::<ControlFileData>() <= PG_CONTROL_MAX_SAFE_SIZE);
const _: () = assert!(std::mem::size_of::<ControlFileData>() <= PG_CONTROL_FILE_SIZE);
