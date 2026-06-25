//! Translated from PostgreSQL src/include/access/xlog.h
//!
//! STUB. The write-ahead log manager (insert/flush/recovery control). The real
//! WAL pipeline runs over async I/O later; here we translate the enums, flags,
//! GUC-ish globals, and the insert/flush/checkpoint signatures as stubs.
// TODO(wal): implement insert/flush over async I/O later

use crate::access::xlogbackup::BackupState;
use crate::access::xlog_internal::XLogRecData;
use crate::access::xlogdefs::{TimeLineID, XLogRecPtr, XLogSegNo};
use crate::access::xlogreader::XLogReaderState;
use crate::datatype::timestamp::TimestampTz;
use crate::postgres::Datum;
use crate::postgres_ext::Oid;

/// WAL fsync methods. Canonical home for `WalSyncMethod` (xlogdefs.h forward-
/// references it). Order matches the C enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum WalSyncMethod {
    Fsync = 0,
    Fdatasync,
    Open,              // for O_SYNC
    FsyncWritethrough,
    OpenDsync,         // for O_DSYNC
}

// XLOG GUC parameters and global LSN cursors. PG process globals; under the
// async model these become session/global config or shared atomics.
// TODO(global): thread through Session / shared WAL state.
pub static mut WAL_SYNC_METHOD: i32 = 0;
pub static mut PROC_LAST_REC_PTR: XLogRecPtr = XLogRecPtr(0);
pub static mut XACT_LAST_REC_END: XLogRecPtr = XLogRecPtr(0);
pub static mut XACT_LAST_COMMIT_END: XLogRecPtr = XLogRecPtr(0);
pub static mut WAL_SEGMENT_SIZE: i32 = 0;
pub static mut WAL_LEVEL: i32 = 0;

/// Archive modes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum ArchiveMode {
    Off = 0,
    On,     // enabled while server runs normally
    Always, // enabled always (even during recovery)
}

/// WAL levels.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum WalLevel {
    Minimal = 0,
    Replica,
    Logical,
}

/// Compression algorithms for WAL.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum WalCompression {
    None = 0,
    Pglz,
    Lz4,
    Zstd,
}

/// Recovery states.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum RecoveryState {
    Crash = 0, // crash recovery
    Archive,   // archive recovery
    Done,      // currently in production
}

use bitflags::bitflags;

bitflags! {
    /// OR-able checkpoint request flags (`CHECKPOINT_*`). The "cause" bits are
    /// log-only. Appendix verdict: GOOD.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct CheckpointFlags: i32 {
        const IS_SHUTDOWN     = 0x0001; // checkpoint is for shutdown
        const END_OF_RECOVERY = 0x0002; // like shutdown, at end of WAL recovery
        const IMMEDIATE       = 0x0004; // do it without delays
        const FORCE           = 0x0008; // force even if no activity
        const FLUSH_ALL       = 0x0010; // flush all pages, incl. unlogged tables
        const WAIT            = 0x0020; // wait for completion
        const REQUESTED       = 0x0040; // checkpoint request has been made
        const CAUSE_XLOG      = 0x0080; // XLOG consumption
        const CAUSE_TIME      = 0x0100; // elapsed time
    }
}

bitflags! {
    /// Flags for a record being inserted (`XLOG_*`, set via XLogSetRecordFlags).
    /// Appendix verdict: GOOD.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct XLogRecordFlags: u8 {
        const INCLUDE_ORIGIN    = 0x01; // include the replication origin
        const MARK_UNIMPORTANT  = 0x02; // record not important for durability
    }
}

/// Checkpoint statistics.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CheckpointStatsData {
    pub start_t: TimestampTz,
    pub write_t: TimestampTz,
    pub sync_t: TimestampTz,
    pub sync_end_t: TimestampTz,
    pub end_t: TimestampTz,
    pub bufs_written: i32,
    pub slru_written: i32,
    pub segs_added: i32,
    pub segs_removed: i32,
    pub segs_recycled: i32,
    pub sync_rels: i32,
    pub longest_sync: u64,
    pub agg_sync_time: u64,
}

/// GetWALAvailability return codes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WALAvailability {
    InvalidLsn,  // parameter error
    Reserved,    // within max_wal_size
    Extended,    // reserved by a slot or wal_keep_size
    Unreserved,  // no longer reserved, not removed yet
    Removed,     // segment has been removed
}

/// Session-level base-backup status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionBackupState {
    None,
    Running,
}

/// File path names (all relative to $PGDATA).
pub const RECOVERY_SIGNAL_FILE: &str = "recovery.signal";
pub const STANDBY_SIGNAL_FILE: &str = "standby.signal";
pub const BACKUP_LABEL_FILE: &str = "backup_label";
pub const BACKUP_LABEL_OLD: &str = "backup_label.old";
pub const TABLESPACE_MAP: &str = "tablespace_map";
pub const TABLESPACE_MAP_OLD: &str = "tablespace_map.old";
pub const PROMOTE_SIGNAL_FILE: &str = "promote";

// --- WAL insert/flush API (stub bodies) ---

pub fn xlog_insert_record(
    _rdata: &mut XLogRecData<'_>,
    _fpw_lsn: XLogRecPtr,
    _flags: u8,
    _num_fpi: i32,
    _topxid_included: bool,
) -> XLogRecPtr {
    unimplemented!()
}
pub fn xlog_flush(_record: XLogRecPtr) {
    unimplemented!()
}
pub fn xlog_background_flush() -> bool {
    unimplemented!()
}
pub fn xlog_needs_flush(_record: XLogRecPtr) -> bool {
    unimplemented!()
}
pub fn xlog_file_init(_logsegno: XLogSegNo, _logtli: TimeLineID) -> i32 {
    unimplemented!()
}
pub fn xlog_file_open(_segno: XLogSegNo, _tli: TimeLineID) -> i32 {
    unimplemented!()
}
pub fn check_xlog_removed(_segno: XLogSegNo, _tli: TimeLineID) {
    unimplemented!()
}
pub fn xlog_get_last_removed_segno() -> XLogSegNo {
    unimplemented!()
}
pub fn xlog_get_oldest_segno(_tli: TimeLineID) -> XLogSegNo {
    unimplemented!()
}
pub fn xlog_set_async_xact_lsn(_async_xact_lsn: XLogRecPtr) {
    unimplemented!()
}
pub fn xlog_set_replication_slot_minimum_lsn(_lsn: XLogRecPtr) {
    unimplemented!()
}
pub fn xlog_get_replication_slot_minimum_lsn() -> XLogRecPtr {
    unimplemented!()
}
pub fn xlog_redo(_record: &mut XLogReaderState) {
    unimplemented!()
}
pub fn issue_xlog_fsync(_fd: i32, _segno: XLogSegNo, _tli: TimeLineID) {
    unimplemented!()
}
pub fn recovery_in_progress() -> bool {
    unimplemented!()
}
pub fn get_recovery_state() -> RecoveryState {
    unimplemented!()
}
pub fn xlog_insert_allowed() -> bool {
    unimplemented!()
}
pub fn get_xlog_insert_rec_ptr() -> XLogRecPtr {
    unimplemented!()
}
pub fn get_xlog_insert_end_rec_ptr() -> XLogRecPtr {
    unimplemented!()
}
pub fn get_xlog_write_rec_ptr() -> XLogRecPtr {
    unimplemented!()
}
pub fn get_system_identifier() -> u64 {
    unimplemented!()
}
pub fn get_mock_authentication_nonce() -> String {
    unimplemented!()
}
pub fn data_checksums_enabled() -> bool {
    unimplemented!()
}
pub fn get_default_char_signedness() -> bool {
    unimplemented!()
}
pub fn get_fake_lsn_for_unlogged_rel() -> XLogRecPtr {
    unimplemented!()
}
pub fn xlog_shmem_size() -> usize {
    unimplemented!()
}
pub fn xlog_shmem_init() {
    unimplemented!()
}
pub fn boot_strap_xlog(_data_checksum_version: u32) {
    unimplemented!()
}
pub fn initialize_wal_consistency_checking() {
    unimplemented!()
}
pub fn local_process_control_file(_reset: bool) {
    unimplemented!()
}
pub fn get_active_wal_level_on_standby() -> WalLevel {
    unimplemented!()
}
pub fn startup_xlog() {
    unimplemented!()
}
pub fn shutdown_xlog(_code: i32, _arg: Datum) {
    unimplemented!()
}
pub fn create_check_point(_flags: CheckpointFlags) -> bool {
    unimplemented!()
}
pub fn create_restart_point(_flags: CheckpointFlags) -> bool {
    unimplemented!()
}
pub fn get_wal_availability(_target_lsn: XLogRecPtr) -> WALAvailability {
    unimplemented!()
}
pub fn xlog_put_next_oid(_next_oid: Oid) {
    unimplemented!()
}
pub fn xlog_restore_point(_rp_name: &str) -> XLogRecPtr {
    unimplemented!()
}
pub fn update_full_page_writes() {
    unimplemented!()
}
/// `GetFullPageWriteInfo`: two out-params -> tuple (function-mapping section 5).
pub fn get_full_page_write_info() -> (XLogRecPtr, bool) {
    unimplemented!()
}
pub fn get_redo_rec_ptr() -> XLogRecPtr {
    unimplemented!()
}
pub fn get_insert_rec_ptr() -> XLogRecPtr {
    unimplemented!()
}
/// `GetFlushRecPtr`: the `*insertTLI` out-param -> tuple.
pub fn get_flush_rec_ptr() -> (XLogRecPtr, TimeLineID) {
    unimplemented!()
}
pub fn get_wal_insertion_time_line() -> TimeLineID {
    unimplemented!()
}
pub fn get_wal_insertion_time_line_if_set() -> TimeLineID {
    unimplemented!()
}
pub fn get_last_important_rec_ptr() -> XLogRecPtr {
    unimplemented!()
}
pub fn set_wal_writer_sleeping(_sleeping: bool) {
    unimplemented!()
}
pub fn wal_read_from_buffers(
    _dstbuf: &mut [u8],
    _startptr: XLogRecPtr,
    _count: usize,
    _tli: TimeLineID,
) -> usize {
    unimplemented!()
}

// Recovery callbacks used by xlogrecovery.c.
pub fn remove_non_parent_xlog_files(_switchpoint: XLogRecPtr, _new_tli: TimeLineID) {
    unimplemented!()
}
pub fn xlog_checkpoint_needed(_new_segno: XLogSegNo) -> bool {
    unimplemented!()
}
pub fn switch_into_archive_recovery(_end_rec_ptr: XLogRecPtr, _replay_tli: TimeLineID) {
    unimplemented!()
}
pub fn reached_end_of_backup(_end_rec_ptr: XLogRecPtr, _tli: TimeLineID) {
    unimplemented!()
}
pub fn set_install_xlog_file_segment_active() {
    unimplemented!()
}
pub fn is_install_xlog_file_segment_active() -> bool {
    unimplemented!()
}
pub fn reset_install_xlog_file_segment_active() {
    unimplemented!()
}
pub fn xlog_shutdown_wal_rcv() {
    unimplemented!()
}

// Base-backup start/stop/status.
pub fn do_pg_backup_start(
    _backupidstr: &str,
    _fast: bool,
    _state: &mut BackupState,
) -> Vec<()> {
    unimplemented!()
}
pub fn do_pg_backup_stop(_state: &mut BackupState, _waitforarchive: bool) {
    unimplemented!()
}
pub fn do_pg_abort_backup(_code: i32, _arg: Datum) {
    unimplemented!()
}
pub fn register_persistent_abort_backup_handler() {
    unimplemented!()
}
pub fn get_backup_status() -> SessionBackupState {
    unimplemented!()
}
