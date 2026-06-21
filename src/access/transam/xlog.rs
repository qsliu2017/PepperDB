/*-------------------------------------------------------------------------
 *
 * xlog.rs
 *    PostgreSQL write-ahead log manager
 *
 * The Write-Ahead Log (WAL) functionality is split into several source
 * files, in addition to this one:
 *
 * xloginsert.c - Functions for constructing WAL records
 * xlogrecovery.c - WAL recovery and standby code
 * xlogreader.c - Facility for reading WAL files and parsing WAL records
 * xlogutils.c - Helper functions for WAL redo routines
 *
 * This file contains functions for coordinating database startup and
 * checkpointing, and managing the write-ahead log buffers when the
 * system is running.
 *
 * StartupXLOG() is the main entry point of the startup process.  It
 * coordinates database startup, performing WAL recovery, and the
 * transition from WAL recovery into normal operations.
 *
 * XLogInsertRecord() inserts a WAL record into the WAL buffers.  Most
 * callers should not call this directly, but use the functions in
 * xloginsert.c to construct the WAL record.  XLogFlush() can be used
 * to force the WAL to disk.
 *
 * In addition to those, there are many other functions for interrogating
 * the current system state, and for starting/stopping backups.
 *
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/backend/access/transam/xlog.c
 *
 *-------------------------------------------------------------------------
 */

#![allow(
    non_snake_case,
    non_upper_case_globals,
    non_camel_case_types,
    dead_code,
    unused_variables,
    unused_mut,
    unused_imports,
    clippy::all
)]

use core::ffi::{c_char, c_int, c_void};
use core::ops::DerefMut;
use core::ops::Deref;
use core::ptr;
use core::sync::atomic::Ordering;

use crate::c::{int32, int64, uint8, uint16, uint32, uint64, Size};
use crate::port::atomics::{
    pg_atomic_uint64,
    pg_atomic_read_u64_impl_native as pg_atomic_read_u64,
    pg_atomic_read_u64_impl_native as pg_atomic_read_membarrier_u64,
};
use crate::port::atomics::generic::{
    pg_memory_barrier_impl as pg_memory_barrier,
    pg_read_barrier_impl as pg_read_barrier,
    pg_write_barrier_impl as pg_write_barrier,
    pg_atomic_write_membarrier_u64_impl as pg_atomic_write_membarrier_u64,
};
use crate::port::atomics::{
    pg_atomic_init_u64_impl_native as pg_atomic_init_u64,
    pg_atomic_fetch_add_u64_impl_native as pg_atomic_fetch_add_u64,
};

/* pg_atomic_write_u64 -- store without barrier (maps to Relaxed store) */
#[inline]
fn pg_atomic_write_u64(ptr: &pg_atomic_uint64, val: uint64) {
    ptr.value.store(val, Ordering::Relaxed);
}

use crate::access::transam::xlogdefs::{
    TimeLineID, XLogRecPtr, XLogSegNo, InvalidXLogRecPtr, XLogRecPtrIsInvalid,
    LSN_FORMAT_ARGS,
};
use crate::access::transam::xlog_internal::{
    XLogPageHeaderData, XLogPageHeader, XLogLongPageHeaderData,
    SizeOfXLogShortPHD, SizeOfXLogLongPHD, XLOG_PAGE_MAGIC, XLOG_BLCKSZ,
    XLOG_BLCKSZ as xlog_blcksz_usize,
    XLP_FIRST_IS_CONTRECORD, XLP_BKP_REMOVABLE, XLP_LONG_HEADER,
    XLogFilePath, XLogFileName, XLogFromFileName, IsXLogFileName,
    IsPartialXLogFileName, IsBackupHistoryFileName, XLogSegmentOffset,
    XLogSegNoOffsetToRecPtr, XLByteToSeg, XLByteToPrevSeg,
    XLByteInPrevSeg, MAXFNAMELEN,
};
use crate::access::transam::xlogrecord::{
    XLogRecord, SizeOfXLogRecord, XLR_INFO_MASK,
};
use crate::access::transam::xlog_internal::XLogRecData;
use crate::access::transam::xloginsert::XLOG_MARK_UNIMPORTANT;
use crate::access::transam::xact::{
    MarkCurrentTransactionIdLoggedIfAny, MarkSubxactTopXidLogged,
};
use crate::access::transam::xlogarchive::{
    XLogArchivingActive, XLogArchiveNotifySeg, XLogArchiveCheckDone,
    XLogArchiveCleanup, XLogArchiveIsReady,
};
use crate::access::transam::xlogrecovery::{
    GetCurrentReplayRecPtr,
    InRecovery,
};
use crate::catalog::pg_control::{
    ControlFileData, DB_SHUTDOWNED,
    PG_CONTROL_VERSION, PG_CONTROL_FILE_SIZE,
};
use crate::access::transam::xlog_internal::XLOG_CONTROL_FILE;
use crate::access::transam::xlogdefs::FirstNormalUnloggedLSN;
use crate::storage::lmgr::s_lock::slock_t;
pub use crate::storage::lmgr::lwlock::LWLock;
use crate::storage::lmgr::proc::{MyProc, MyProcNumber, ProcGlobal, PROC_HDR, GetPGProcByNumber};
use crate::storage::procnumber::INVALID_PROC_NUMBER;
use crate::storage::file::fd::{BasicOpenFile, OpenTransientFile, CloseTransientFile};
use crate::utils::elog::{errcode, DEBUG1, DEBUG2, LOG, WARNING, ERROR, FATAL, PANIC};
use crate::storage::file::fd::data_sync_elevel;
use crate::miscadmin::{DataDir, IsUnderPostmaster, CritSectionCount};
use crate::access::transam::xact::{TransactionId, FullTransactionId};
use crate::pgtime::pg_time_t;
use crate::common::controldata_utils::update_controlfile;

// ---- wired imports (added to compile xlog.rs) ----
use crate::prelude::*;

// access/transam
use crate::access::transam::{
    FirstNormalTransactionId, TransactionIdIsNormal, TransactionIdIsValid,
    TransactionIdRetreat, XidFromFullTransactionId, FullTransactionIdPrecedes,
    FullTransactionIdRetreat, FullTransactionIdFromEpochAndXid,
};
use crate::access::transam::transam::TransactionIdPrecedes;
use crate::access::transam::xact::InvalidTransactionId;
use crate::access::transam::xlogdefs::XLogRecPtrIsValid;
use crate::access::transam::clog::{BootStrapCLOG, StartupCLOG, TrimCLOG, CheckPointCLOG};
use crate::access::transam::commit_ts::{
    BootStrapCommitTs, StartupCommitTs, CompleteCommitTsInitialization,
    CommitTsParameterChange, CheckPointCommitTs, SetCommitTsLimit,
};
use crate::access::transam::subtrans::{
    BootStrapSUBTRANS, StartupSUBTRANS, CheckPointSUBTRANS, TruncateSUBTRANS,
};
use crate::access::transam::multixact::{
    BootStrapMultiXact, StartupMultiXact, TrimMultiXact, CheckPointMultiXact,
    MultiXactGetCheckptMulti, MultiXactSetNextMXact, MultiXactAdvanceNextMXact,
    MultiXactAdvanceOldest, SetMultiXactIdLimit, FirstMultiXactId,
};
use crate::access::transam::varsup::{AdvanceOldestClogXid, SetTransactionIdLimit, TransamVariables};
use crate::access::transam::xact::VirtualTransactionId;
use crate::access::transam::xlogstats::RM_MAX_ID;
use crate::pg_config::PG_IO_ALIGN_SIZE;
use crate::port::pgstrcasecmp::pg_strcasecmp;
use crate::port::strlcpy::strlcpy;
use crate::common::file_utils::{PGFileType, PGFILETYPE_DIR, PGFILETYPE_LNK};
use crate::miscadmin::IsPostmasterEnvironment;
use crate::access::transam::xlog_internal::{
    GetRmgr, RmgrIdExists, BackupHistoryFileName, BackupHistoryFilePath,
    xl_parameter_change,
};
use crate::access::transam::xloginsert::{
    XLogBeginInsert, XLogInsert, XLogRegisterData, XLogSetRecordFlags,
    UnlockReleaseBuffer,
};
use crate::access::transam::xlogreader::{
    XLogReaderState, XLogRecGetData, XLogRecGetInfo, XLogRecHasAnyBlockRefs,
    XLogRecHasBlockImage, XLogRecMaxBlockId, XRecOffIsValid, XLR_BLOCK_ID_DATA_SHORT,
    XLP_FIRST_IS_OVERWRITE_CONTRECORD, SizeOfXLogRecordDataHeaderShort,
};
use crate::access::transam::xlogutils::{
    Buffer, InvalidBuffer, InHotStandby, XLogHaveInvalidPages, XLogReadBufferForRedo,
    BLK_RESTORED,
};
use crate::access::transam::xlogarchive::{
    XLogArchiveNotify, XLogArchiveIsBusy, XLogArchiveIsReadyOrDone,
    ExecuteRecoveryCommand,
};
use crate::access::transam::xlogrecovery::{
    ArchiveRecoveryRequested, InArchiveRecovery, InitWalRecovery, PerformWalRecovery,
    FinishWalRecovery, ShutdownWalRecovery, EndOfWalRecoveryInfo, GetLatestXTime,
    GetXLogReplayRecPtr, PromoteIsTriggered, RecoveryRequiresIntParameter,
    recoveryEndCommand, recoveryTargetTLI, standbyState, archiveCleanupCommand,
    tablespaceinfo, xl_restore_point, xl_end_of_recovery, xl_overwrite_contrecord,
    BACKUP_LABEL_FILE, TABLESPACE_MAP, TABLESPACE_MAP_OLD, RECOVERY_SIGNAL_FILE,
    STANDBY_SIGNAL_FILE, PG_TBLSPC_DIR,
};
use crate::access::transam::timeline::{
    findNewestTimeLine, writeTimeLineHistory, restoreTimeLineHistoryFiles,
};
use crate::access::transam::twophase::{
    CheckPointTwoPhase, PrescanPreparedTransactions, StandbyRecoverPreparedTransactions,
    RecoverPreparedTransactions, restoreTwoPhaseData,
};
use crate::access::rmgrdesc::xlogdesc::{
    WAL_LEVEL_MINIMAL, WAL_LEVEL_LOGICAL,
    XLOG_CHECKPOINT_SHUTDOWN, XLOG_CHECKPOINT_ONLINE, XLOG_NOOP, XLOG_NEXTOID,
    XLOG_RESTORE_POINT, XLOG_BACKUP_END, XLOG_PARAMETER_CHANGE, XLOG_END_OF_RECOVERY,
    XLOG_FPI_FOR_HINT, XLOG_FPI, XLOG_OVERWRITE_CONTRECORD,
};
// TODO(pg-port): access/heap/rewriteheap module not yet wired into the tree
unsafe fn CheckPointLogicalRewriteHeap() { /* TODO(pg-port) */ }

// catalog
use crate::catalog::pg_control::{
    DB_SHUTDOWNING, DB_SHUTDOWNED_IN_RECOVERY, DB_IN_ARCHIVE_RECOVERY, DB_IN_PRODUCTION,
};
use crate::catalog::catalog::FirstGenbkiObjectId;
use crate::catalog::pg_known_oids::Template1DbOid;

// nodes
use crate::nodes::pg_list::{List, lappend, list_free};

// storage
use crate::storage::ipc::shmem::{ShmemInitStruct, add_size, mul_size};
use crate::storage::ipc::ipc::before_shmem_exit;
use crate::storage::ipc::latch::{
    WaitLatch, ResetLatch, SetLatch, WL_LATCH_SET, WL_TIMEOUT, WL_EXIT_ON_PM_DEATH,
};
// TODO(pg-port): storage/ipc/standby module not yet wired into the tree
unsafe fn InitRecoveryTransactionEnvironment() { unimplemented!() }
unsafe fn ShutdownRecoveryTransactionEnvironment() { unimplemented!() }
unsafe fn LogStandbySnapshot() -> XLogRecPtr { unimplemented!() }
const STANDBY_DISABLED: c_int = 0;
const STANDBY_INITIALIZED: c_int = 1;
use crate::storage::ipc::procarray::{
    ProcArrayInitRecovery, ProcArrayApplyRecoveryInfo, GetOldestActiveTransactionId,
    GetOldestTransactionIdConsideredRunning, GetVirtualXIDsDelayingChkpt,
    HaveVirtualXIDsDelayingChkpt, RunningTransactionsData, SUBXIDS_IN_SUBTRANS,
};
use crate::storage::lmgr::proc::{DELAY_CHKPT_START, DELAY_CHKPT_COMPLETE, ProcArrayLock};
use crate::storage::lmgr::lwlock::LWLockInitialize;
use crate::storage::lmgr::lwlock::BuiltinTrancheIds::LWTRANCHE_WAL_INSERT;
use crate::storage::spin::SpinLockInit;
use crate::storage::buffer::bufmgr::CheckPointBuffers;
use crate::storage::lmgr::predicate::CheckPointPredicate;
use crate::storage::sync::sync::{ProcessSyncRequests, SyncPreCheckpoint, SyncPostCheckpoint};
use crate::storage::smgr::smgr::smgrdestroyall;
use crate::storage::file::fd::{
    AllocateFile, FreeFile, pg_fdatasync, pg_fsync_no_writethrough,
    pg_fsync_writethrough, SyncDataDirectory,
};
use crate::storage::file::reinit::{
    ResetUnloggedRelations, UNLOGGED_RELATION_INIT, UNLOGGED_RELATION_CLEANUP,
};

// utils
use crate::utils::resowner::resowner::{CurrentResourceOwner, AuxProcessResourceOwner};
// TODO(pg-port): utils/time/snapmgr module not yet wired into the tree
unsafe fn DeleteAllExportedSnapshotFiles() { /* TODO(pg-port) */ }
use crate::utils::cache::relmapper::CheckPointRelationMap;
use crate::utils::adt::timestamp::TimestampDifferenceMilliseconds;
use crate::utils::adt::varlena::SplitIdentifierString;
use crate::utils::misc::timeout::{RegisterTimeout, STARTUP_PROGRESS_TIMEOUT};
use crate::utils::misc::ps_status::set_ps_display;
use crate::utils::misc::guc::{find_option, guc_malloc, set_config_option_ext};
use crate::utils::misc::guc_funcs::GUC_ACTION_SET;
use crate::utils::activity::pgstat::{pgstat_discard_stats, pgstat_restore_stats};
use crate::utils::activity::pgstat_checkpointer::PendingCheckpointerStats;

// lib (stringinfo)
use crate::lib::stringinfo::{StringInfoData, initStringInfo, appendStringInfoChar};
use crate::appendStringInfo;

// replication
use crate::replication::slot::{
    CheckPointReplicationSlots, StartupReplicationSlots, InvalidateObsoleteReplicationSlots,
    RS_INVAL_IDLE_TIMEOUT,
};
use crate::replication::slotfuncs::{
    WALAvailability, WALAVAIL_INVALID_LSN, WALAVAIL_RESERVED, WALAVAIL_EXTENDED,
    WALAVAIL_UNRESERVED, WALAVAIL_REMOVED, RS_INVAL_WAL_REMOVED, RS_INVAL_WAL_LEVEL,
};
use crate::replication::logical::origin::{
    CheckPointReplicationOrigin, StartupReplicationOrigin,
};
use crate::replication::logical::reorderbuffer::StartupReorderBuffer;
use crate::replication::logical::snapbuild::CheckPointSnapBuild;
use crate::replication::walsender::{WalSndWakeup, WalSndInitStopping, WalSndWaitStopping};
use crate::replication::walreceiverfuncs::{ShutdownWalRcv, GetWalRcvFlushRecPtr};

// postmaster
use crate::postmaster::checkpointer::{
    AbsorbSyncRequests, CHECKPOINT_IS_SHUTDOWN, CHECKPOINT_END_OF_RECOVERY,
    CHECKPOINT_IMMEDIATE, CHECKPOINT_FORCE, CHECKPOINT_WAIT,
    CHECKPOINT_CAUSE_TIME, CHECKPOINT_FLUSH_ALL,
};
use crate::postmaster::startup::startup_progress_timeout_handler;
use crate::postmaster::walsummarizer::{
    GetOldestUnsummarizedLSN, WaitForWalSummarization, WakeupWalSummarizer, summarize_wal,
};

// backup
use crate::access::transam::xlogbackup::{BackupState, build_backup_content};

// link shims (only pub definition available)
use crate::backend_link_shims::XLogArchivingAlways;

// libc::FILE (matches the convention in tcop/postgres.rs, init/miscinit.rs)
type FILE = libc::FILE;

// miscadmin
use crate::miscadmin::{
    AmWalReceiverProcess, IsBinaryUpgrade, IsBootstrapProcessingMode, MyBackendType,
    NBuffers, MyLatch, B_CHECKPOINTER, process_shared_preload_libraries_done,
};

// ---- end wired imports ----

// TODO(pg-port): pg_control.h types / constants not yet in catalog/pg_control stub
pub type pg_crc32c = uint32;
// TODO(pg-port): INIT_CRC32C, COMP_CRC32C, FIN_CRC32C, EQ_CRC32C macros
macro_rules! INIT_CRC32C { ($c:expr) => { $c = 0xFFFFFFFF_u32; } }
macro_rules! COMP_CRC32C {
    ($c:expr, $data:expr, $len:expr) => {
        { $c = crate::port::pg_crc32c::COMP_CRC32C($c, $data as *const c_void, $len as Size); }
    }
}
macro_rules! FIN_CRC32C { ($c:expr) => { $c ^= 0xFFFFFFFF_u32; } }
macro_rules! EQ_CRC32C { ($c1:expr, $c2:expr) => { $c1 == $c2 } }

// TODO(pg-port): critical section macros
macro_rules! START_CRIT_SECTION { () => { unsafe { crate::miscadmin::CritSectionCount += 1; } } }
macro_rules! END_CRIT_SECTION   { () => { unsafe { crate::miscadmin::CritSectionCount -= 1; } } }

// Local macro stubs (not #[macro_export]; kept local per the port convention).
macro_rules! CHECK_FOR_INTERRUPTS { () => { crate::miscadmin::CHECK_FOR_INTERRUPTS() }; }
macro_rules! IS_DIR_SEP { ($ch:expr) => { ($ch) == b'/' }; }
// TODO(pg-port): PG_ENSURE_ERROR_CLEANUP / PG_END_ENSURE_ERROR_CLEANUP (utils/ipc.h)
macro_rules! PG_ENSURE_ERROR_CLEANUP { ($cleanup:expr, $arg:expr) => { () }; }
macro_rules! PG_END_ENSURE_ERROR_CLEANUP { ($cleanup:expr, $arg:expr) => { () }; }

// TODO(pg-port): spinlock stubs
unsafe fn SpinLockAcquire(_lock: *mut slock_t) { /* TODO(pg-port) */ }
unsafe fn SpinLockRelease(_lock: *mut slock_t) { /* TODO(pg-port) */ }

// TODO(pg-port): LWLock stubs
const LW_EXCLUSIVE: c_int = 2;
const LW_SHARED:    c_int = 1;
unsafe fn LWLockAcquire(_lock: *mut LWLock, _mode: c_int) -> bool { crate::storage::lmgr::lwlock::LWLockAcquire(_lock as _, core::mem::transmute(_mode)) }
unsafe fn LWLockRelease(_lock: *mut LWLock) { crate::storage::lmgr::lwlock::LWLockRelease(_lock as _) }
unsafe fn LWLockAcquireOrWait(_lock: *mut LWLock, _mode: c_int) -> bool { crate::storage::lmgr::lwlock::LWLockAcquireOrWait(_lock as _, core::mem::transmute(_mode)) }
unsafe fn LWLockConditionalAcquire(_lock: *mut LWLock, _mode: c_int) -> bool { crate::storage::lmgr::lwlock::LWLockConditionalAcquire(_lock as _, core::mem::transmute(_mode)) }
unsafe fn LWLockUpdateVar(_lock: *mut LWLock, _val: *mut uint64, _new: uint64) { crate::storage::lmgr::lwlock::LWLockUpdateVar(_lock as _, _val as _, _new) }
unsafe fn LWLockWaitForVar(_lock: *mut LWLock, _val: *mut uint64, _old: uint64, _new: *mut uint64) -> bool { crate::storage::lmgr::lwlock::LWLockWaitForVar(_lock as _, _val as _, _old, _new) }
unsafe fn LWLockReleaseClearVar(_lock: *mut LWLock, _val: *mut uint64, _new: uint64) { crate::storage::lmgr::lwlock::LWLockReleaseClearVar(_lock as _, _val as _, _new) }
// Named LWLock slots: forward to the canonical runtime-assigned globals.
unsafe fn WALBufMappingLock_ptr() -> *mut LWLock { crate::backend_link_shims::WALBufMappingLock as *mut LWLock }
unsafe fn WALWriteLock_ptr() -> *mut LWLock { crate::backend_link_shims::WALWriteLock as *mut LWLock }
unsafe fn ControlFileLock_ptr() -> *mut LWLock { crate::backend_link_shims::ControlFileLock as *mut LWLock }
unsafe fn XidGenLock_ptr() -> *mut LWLock { crate::backend_link_shims::XidGenLock as *mut LWLock }
unsafe fn OidGenLock_ptr() -> *mut LWLock { crate::backend_link_shims::OidGenLock as *mut LWLock }
unsafe fn CommitTsLock_ptr() -> *mut LWLock { crate::backend_link_shims::CommitTsLock as *mut LWLock }
macro_rules! WALBufMappingLock { () => { WALBufMappingLock_ptr() } }
macro_rules! WALWriteLock { () => { WALWriteLock_ptr() } }
macro_rules! ControlFileLock { () => { ControlFileLock_ptr() } }
macro_rules! XidGenLock { () => { XidGenLock_ptr() } }
macro_rules! OidGenLock { () => { OidGenLock_ptr() } }
macro_rules! CommitTsLock { () => { CommitTsLock_ptr() } }

// TODO(pg-port): MAXALIGN / MAXALIGN64
#[inline] fn MAXALIGN(x: usize) -> usize { (x + 7) & !7 }
#[inline] fn MAXALIGN64(x: u64) -> u64 { (x + 7) & !7 }
// TODO(pg-port): MemSet
unsafe fn MemSet(p: *mut c_void, v: c_int, n: usize) { ptr::write_bytes(p as *mut u8, v as u8, n); }
// TODO(pg-port): palloc / pfree stubs
unsafe fn palloc(size: usize) -> *mut c_void { crate::utils::palloc::palloc(size) }
unsafe fn pfree(_p: *mut c_void) { crate::utils::palloc::pfree(_p) }
// TODO(pg-port): pg_strong_random
unsafe fn pg_strong_random(buf: *mut c_void, len: usize) -> bool { crate::port::pg_strong_random::pg_strong_random(buf, len) }
// TODO(pg-port): pg_usleep
unsafe fn pg_usleep(usec: i64) { /* TODO(pg-port) */ }
// TODO(pg-port): pg_fsync / pg_pwrite / pg_pwrite_zeros / pg_pread / pread/write/close/unlink/stat/access/rename
use libc::{close, read, write, stat, unlink, rename, access, open};
unsafe fn pg_fsync(fd: c_int) -> c_int { crate::storage::file::fd::pg_fsync(fd) }
unsafe fn pg_pwrite(fd: c_int, buf: *const c_void, nbytes: usize, offset: i64) -> isize { libc::pwrite(fd, buf, nbytes, offset) }
unsafe fn pg_pwrite_zeros(fd: c_int, nbytes: usize, offset: i64) -> isize { crate::common::file_utils::pg_pwrite_zeros(fd, nbytes, offset) }
// TODO(pg-port): durable_unlink / durable_rename
unsafe fn durable_unlink(path: *const c_char, elevel: c_int) -> c_int { crate::storage::file::fd::durable_unlink(path, elevel) }
unsafe fn durable_rename(oldpath: *const c_char, newpath: *const c_char, elevel: c_int) -> c_int { crate::storage::file::fd::durable_rename(oldpath, newpath, elevel) }
// TODO(pg-port): MakePGDirectory
unsafe fn MakePGDirectory(path: *const c_char) -> c_int { crate::storage::file::fd::MakePGDirectory(path) }
// TODO(pg-port): AllocateDir / ReadDir / FreeDir
pub type DIR = c_void;
pub type dirent = libc::dirent;
unsafe fn AllocateDir(path: *const c_char) -> *mut DIR { crate::storage::file::fd::AllocateDir(path) as *mut DIR }
unsafe fn ReadDir(dir: *mut DIR, path: *const c_char) -> *mut dirent { crate::storage::file::fd::ReadDir(dir as _, path) as *mut dirent }
unsafe fn FreeDir(dir: *mut DIR) { crate::storage::file::fd::FreeDir(dir as _); }
// TODO(pg-port): get_dirent_type / PGFILETYPE_REG
const PGFILETYPE_REG: c_int = 1;
unsafe fn get_dirent_type(_path: *const c_char, _de: *const dirent, _follow: bool, _elevel: c_int) -> c_int { todo!("TODO(pg-port)") }
// TODO(pg-port): rmgr / wal_level types (access/rmgr.h, access/xlog.h)
type BuiltinRmgrId = crate::access::transam::xlogreader::RmgrId;
type WalLevel = c_int;
// TODO(pg-port): GUC context/source constants (utils/guc.h)
const PGC_POSTMASTER: c_int = 0;
const PGC_S_OVERRIDE: c_int = 0;
// TODO(pg-port): backup label old path (xlog_internal.h)
const BACKUP_LABEL_OLD: &str = "backup_label.old\0";
// TODO(pg-port): XLogStandbyInfoActive (access/xlog.h)
unsafe fn XLogStandbyInfoActive() -> bool { false }
// TODO(pg-port): RelationCacheInitFileRemove (utils/cache/relcache.c)
unsafe fn RelationCacheInitFileRemove() { /* TODO(pg-port) */ }
// TODO(pg-port): XLogRecord_crc_offset (access/xlogrecord.h offsetof helper)
unsafe fn XLogRecord_crc_offset() -> usize { 0 }
// TODO(pg-port): strerror_r display shim (port/strerror.c)
unsafe fn strerror_r() -> &'static str { "" }
// TODO(pg-port): cstr_to_str display shim
unsafe fn cstr_to_str<'a>(p: *const c_char) -> &'a str {
    if p.is_null() { return ""; }
    core::ffi::CStr::from_ptr(p).to_str().unwrap_or("")
}
// TODO(pg-port): timing / timestamp stubs
pub type TimestampTz = int64;
pub type instr_time = int64;
unsafe fn pgstat_prepare_io_time(_track: bool) -> instr_time { 0 }
unsafe fn pgstat_report_wait_start(_event: u32) {}
unsafe fn pgstat_report_wait_end() {}
unsafe fn pgstat_count_io_op_time(_obj: c_int, _ctx: c_int, _op: c_int, _start: instr_time, _n: usize, _bytes: isize) {}
const WAIT_EVENT_WAL_WRITE: u32 = 0;
const WAIT_EVENT_WAL_INIT_WRITE: u32 = 0;
const WAIT_EVENT_WAL_INIT_SYNC: u32 = 0;
const WAIT_EVENT_WAL_COPY_READ: u32 = 0;
const WAIT_EVENT_WAL_COPY_WRITE: u32 = 0;
const WAIT_EVENT_WAL_COPY_SYNC: u32 = 0;
const WAIT_EVENT_CONTROL_FILE_WRITE: u32 = 0;
const WAIT_EVENT_CONTROL_FILE_SYNC: u32 = 0;
const WAIT_EVENT_CONTROL_FILE_READ: u32 = 0;
const WAIT_EVENT_WAL_BOOTSTRAP_WRITE: u32 = 0;
const WAIT_EVENT_WAL_BOOTSTRAP_SYNC: u32 = 0;
const WAIT_EVENT_RECOVERY_END_COMMAND: u32 = 0;
const WAIT_EVENT_CHECKPOINT_DELAY_START: u32 = 0;
const WAIT_EVENT_CHECKPOINT_DELAY_COMPLETE: u32 = 0;
const WAIT_EVENT_ARCHIVE_CLEANUP_COMMAND: u32 = 0;
const WAIT_EVENT_BACKUP_WAIT_WAL_ARCHIVE: u32 = 0;
const WAIT_EVENT_WAL_SYNC_METHOD_ASSIGN: u32 = 0;
const WAIT_EVENT_WAL_SYNC: u32 = 0;
const IOOBJECT_WAL: c_int = 0;
const IOCONTEXT_NORMAL: c_int = 0;
const IOCONTEXT_INIT: c_int = 1;
const IOOP_WRITE: c_int = 0;
const IOOP_FSYNC: c_int = 1;
// TODO(pg-port): pgWalUsage, pgstat_report_fixed
struct WalUsage { pub wal_bytes: uint64, pub wal_records: uint64, pub wal_fpi: uint64, pub wal_buffers_full: uint64 }
static mut pgWalUsage: WalUsage = WalUsage { wal_bytes: 0, wal_records: 0, wal_fpi: 0, wal_buffers_full: 0 };
static mut pgstat_report_fixed: bool = false;
// TODO(pg-port): GUC / misc
unsafe fn GetCurrentTimestamp() -> TimestampTz { 0 }
unsafe fn TimestampDifferenceExceeds(_a: TimestampTz, _b: TimestampTz, _ms: c_int) -> bool { false }
unsafe fn MinimumActiveBackends(_n: c_int) -> bool { false }
unsafe fn RequestCheckpoint(_flags: c_int) {}
const CHECKPOINT_CAUSE_XLOG: c_int = 0x10;
unsafe fn WalSndWakeupRequest() {}
unsafe fn WalSndWakeupProcessRequests(_a: bool, _b: bool) {}
unsafe fn XLogIsNeeded() -> bool { false }
unsafe fn IsValidWalSegSize(s: c_int) -> bool { s.count_ones() == 1 && s >= 1024*1024 && s <= 1024*1024*1024 }
unsafe fn SetConfigOption(_name: *const c_char, _val: *const c_char, _ctx: c_int, _src: c_int) {}
const PGC_INTERNAL: c_int = 0;
const PGC_S_DYNAMIC_DEFAULT: c_int = 0;
unsafe fn ReserveExternalFD() {}
unsafe fn ReleaseExternalFD() {}
// TODO(pg-port): XLogMBVarToSegs
unsafe fn XLogMBVarToSegs(mb: c_int, segsize: c_int) -> XLogSegNo { (mb as u64 * 1024 * 1024) / segsize as u64 }
// TODO(pg-port): TRACE macros
macro_rules! TRACE_POSTGRESQL_WAL_SWITCH { () => {} }
macro_rules! TRACE_POSTGRESQL_WAL_BUFFER_WRITE_DIRTY_START { () => {} }
macro_rules! TRACE_POSTGRESQL_WAL_BUFFER_WRITE_DIRTY_DONE { () => {} }
// TODO(pg-port): WAL_DEBUG xlog_outdesc
unsafe fn xlog_outdesc(_buf: *mut c_void, _reader: *mut c_void) {}
// TODO(pg-port): CheckPointCompletionTarget, WalWriterDelay, WalWriterFlushAfter, enableFsync
static mut CheckPointCompletionTarget: f64 = 0.5;
static mut WalWriterDelay: c_int = 200;
static mut WalWriterFlushAfter: c_int = 1;
static mut enableFsync: bool = true;
// TODO(pg-port): io_direct_flags
const IO_DIRECT_WAL:      c_int = 0x01;
const IO_DIRECT_WAL_INIT: c_int = 0x02;
static mut io_direct_flags: c_int = 0;
// TODO(pg-port): GUC enums
pub const WAL_SYNC_METHOD_FSYNC: c_int = 0;
pub const WAL_SYNC_METHOD_FDATASYNC: c_int = 1;
pub const WAL_SYNC_METHOD_OPEN: c_int = 2;
pub const WAL_SYNC_METHOD_OPEN_DSYNC: c_int = 3;
pub const WAL_SYNC_METHOD_FSYNC_WRITETHROUGH: c_int = 4;
pub const DEFAULT_WAL_SYNC_METHOD: c_int = WAL_SYNC_METHOD_FDATASYNC;
pub const WAL_COMPRESSION_NONE: c_int = 0;
pub const WAL_LEVEL_REPLICA: c_int = 1;
pub const ARCHIVE_MODE_OFF: c_int = 0;
pub const ARCHIVE_MODE_ON: c_int = 1;
pub const ARCHIVE_MODE_ALWAYS: c_int = 2;
// TODO(pg-port): config_enum_entry
#[repr(C)]
pub struct config_enum_entry {
    pub name: *const c_char,
    pub val: c_int,
    pub hidden: bool,
}
unsafe impl Sync for config_enum_entry {}

// BootstrapTimeLineID
pub const BootstrapTimeLineID: TimeLineID = 1;

// TODO(pg-port): pg_time_t / time(NULL)
unsafe fn time_now() -> pg_time_t { 0 }

// TODO(pg-port): BLCKSZ etc from pg_config
const BLCKSZ: c_int = 8192;
const RELSEG_SIZE: c_int = 131072;
const NAMEDATALEN: c_int = 64;
const INDEX_MAX_KEYS: c_int = 32;
const TOAST_MAX_CHUNK_SIZE: c_int = 1996;
const LOBLKSIZE: c_int = 2048;
const MAXIMUM_ALIGNOF: c_int = 8;
const FLOAT8PASSBYVAL: bool = true;
const FLOATFORMAT_VALUE: f64 = 1234567.0;
const MOCK_AUTH_NONCE_LEN: usize = 32;
pub const PG_UINT64_MAX: uint64 = u64::MAX;
const PG_BINARY: c_int = 0;
pub const O_CLOEXEC: c_int = libc::O_CLOEXEC;
pub const PG_O_DIRECT: c_int = 0; /* platform-specific; Darwin has F_NOCACHE instead */
const XLOGDIR: &str = "pg_wal";
// TODO(pg-port): CATALOG_VERSION_NO
const CATALOG_VERSION_NO: uint32 = crate::catalog::catversion::CATALOG_VERSION_NO as uint32;
const PG_CONTROL_VERSION_CONST: uint32 = 1400;
const MAXPGPATH: usize = 1024;
const DEFAULT_XLOG_SEG_SIZE: c_int = 16 * 1024 * 1024;

// RM_XLOG_ID
const RM_XLOG_ID: uint8 = 0;
// XLOG resource manager record types (xlog_internal.h)
const XLOG_SWITCH: uint8 = 0x40;
const XLOG_CHECKPOINT_REDO: uint8 = 0x00; /* TODO(pg-port): check real value */
const XLOG_FPW_CHANGE: uint8 = 0x20;

use crate::catalog::pg_control::CheckPoint;

// TODO(pg-port): SessionBackupState (backup/basebackup.h)
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum SessionBackupState {
    SESSION_BACKUP_NONE,
    SESSION_BACKUP_RUNNING,
    SESSION_BACKUP_EXCLUSIVE,
}
use SessionBackupState::*;

// TODO(pg-port): RecoveryState (xlogrecovery.h)
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum RecoveryState {
    RECOVERY_STATE_CRASH,
    RECOVERY_STATE_ARCHIVE,
    RECOVERY_STATE_DONE,
}
use RecoveryState::*;

// TODO(pg-port): PGAlignedXLogBlock
#[repr(C, align(512))]
pub struct PGAlignedXLogBlock {
    pub data: [u8; XLOG_BLCKSZ],
}

// TODO(pg-port): MAXALIGN MAXALIGN64 via c.rs -- the one in c.rs may differ; use local inline above.

// ---------------------------------------------------------------------------
// User-settable parameters
// ---------------------------------------------------------------------------

pub static mut max_wal_size_mb: c_int = 1024;   /* 1 GB */
pub static mut min_wal_size_mb: c_int = 80;     /* 80 MB */
pub static mut wal_keep_size_mb: c_int = 0;
pub static mut XLOGbuffers: c_int = -1;
pub static mut XLogArchiveTimeout: c_int = 0;
pub static mut XLogArchiveMode: c_int = ARCHIVE_MODE_OFF;
pub static mut XLogArchiveCommand: *mut c_char = ptr::null_mut();
pub static mut EnableHotStandby: bool = false;
pub static mut fullPageWrites: bool = true;
pub static mut wal_log_hints: bool = false;
pub static mut wal_compression: c_int = WAL_COMPRESSION_NONE;
pub static mut wal_consistency_checking_string: *mut c_char = ptr::null_mut();
pub static mut wal_consistency_checking: *mut bool = ptr::null_mut();
pub static mut wal_init_zero: bool = true;
pub static mut wal_recycle: bool = true;
pub static mut log_checkpoints: bool = true;
pub static mut wal_sync_method: c_int = DEFAULT_WAL_SYNC_METHOD;
pub static mut wal_level: c_int = WAL_LEVEL_REPLICA;
pub static mut CommitDelay: c_int = 0;    /* precommit delay in microseconds */
pub static mut CommitSiblings: c_int = 5; /* # concurrent xacts needed to sleep */
pub static mut wal_retrieve_retry_interval: c_int = 5000;
pub static mut max_slot_wal_keep_size_mb: c_int = -1;
pub static mut wal_decode_buffer_size: c_int = 512 * 1024;
pub static mut track_wal_io_timing: bool = false;

pub static mut wal_segment_size: c_int = DEFAULT_XLOG_SEG_SIZE;

/*
 * Number of WAL insertion locks to use.
 */
const NUM_XLOGINSERT_LOCKS: usize = 8;

/*
 * Max distance from last checkpoint, before triggering a new xlog-based
 * checkpoint.
 */
pub static mut CheckPointSegments: c_int = 3;

/* Estimated distance between checkpoints, in bytes */
static mut CheckPointDistanceEstimate: f64 = 0.0;
static mut PrevCheckPointDistance: f64 = 0.0;

/*
 * Track whether there were any deferred checks for custom resource managers
 * specified in wal_consistency_checking.
 */
static mut check_wal_consistency_checking_deferred: bool = false;

// GUC option tables (TODO(pg-port): config_enum_entry arrays)
#[no_mangle]
pub static wal_sync_method_options: [config_enum_entry; 1] = [
    config_enum_entry { name: ptr::null(), val: 0, hidden: false },
];
#[no_mangle]
pub static wal_level_options: [config_enum_entry; 4] = [
    config_enum_entry { name: b"minimal\0".as_ptr() as *const c_char, val: 0, hidden: false },
    config_enum_entry { name: b"replica\0".as_ptr() as *const c_char, val: 1, hidden: false },
    config_enum_entry { name: b"logical\0".as_ptr() as *const c_char, val: 2, hidden: false },
    config_enum_entry { name: ptr::null(), val: 0, hidden: false },
];
#[no_mangle]
pub static archive_mode_options: [config_enum_entry; 1] = [
    config_enum_entry { name: ptr::null(), val: 0, hidden: false },
];

// TODO(pg-port): CheckpointStatsData -- canonical home is here (xlog.c).
// storage/buffer/bufmgr.rs and storage/sync/sync.rs have local stubs; this is
// the real definition.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct CheckpointStatsData {
    pub ckpt_start_t:     TimestampTz,
    pub ckpt_write_t:     TimestampTz,
    pub ckpt_sync_t:      TimestampTz,
    pub ckpt_sync_end_t:  TimestampTz,
    pub ckpt_end_t:       TimestampTz,
    pub ckpt_bufs_written: c_int,
    pub ckpt_slru_written: u64,
    pub ckpt_segs_added:   c_int,
    pub ckpt_segs_removed: c_int,
    pub ckpt_segs_recycled: c_int,
    pub ckpt_sync_rels:    c_int,
    pub ckpt_longest_sync: uint64,
    pub ckpt_agg_sync_time: uint64,
}

pub static mut CheckpointStats: CheckpointStatsData = CheckpointStatsData {
    ckpt_start_t: 0, ckpt_write_t: 0, ckpt_sync_t: 0, ckpt_sync_end_t: 0,
    ckpt_end_t: 0, ckpt_bufs_written: 0, ckpt_slru_written: 0, ckpt_segs_added: 0,
    ckpt_segs_removed: 0, ckpt_segs_recycled: 0, ckpt_sync_rels: 0,
    ckpt_longest_sync: 0, ckpt_agg_sync_time: 0,
};

/*
 * During recovery, lastFullPageWrites keeps track of full_page_writes that
 * the replayed WAL records indicate.
 */
static mut lastFullPageWrites: bool = false;

/*
 * Local copy of the state tracked by SharedRecoveryState in shared memory.
 */
static mut LocalRecoveryInProgress: bool = true;

/*
 * Local state for XLogInsertAllowed():
 *    1: unconditionally allowed to insert XLOG
 *    0: unconditionally not allowed to insert XLOG
 *   -1: must check RecoveryInProgress(); disallow until it is false
 */
static mut LocalXLogInsertAllowed: c_int = -1;

/*
 * ProcLastRecPtr points to the start of the last XLOG record inserted by the
 * current backend.  XactLastRecEnd points to end+1 of the last record.
 * XactLastCommitEnd points to end+1 of the last commit record.
 */
pub static mut ProcLastRecPtr: XLogRecPtr = InvalidXLogRecPtr;
pub static mut XactLastRecEnd: XLogRecPtr = InvalidXLogRecPtr;
pub static mut XactLastCommitEnd: XLogRecPtr = InvalidXLogRecPtr;

/*
 * RedoRecPtr is this backend's local copy of the REDO record pointer.
 */
static mut RedoRecPtr: XLogRecPtr = InvalidXLogRecPtr;

/*
 * doPageWrites is this backend's local copy of
 * (fullPageWrites || runningBackups > 0).
 */
static mut doPageWrites: bool = false;

// ---------------------------------------------------------------------------
// Shared-memory data structures for XLOG control
// ---------------------------------------------------------------------------

#[repr(C)]
#[derive(Clone, Copy)]
pub struct XLogwrtRqst {
    pub Write: XLogRecPtr,  /* last byte + 1 to write out */
    pub Flush: XLogRecPtr,  /* last byte + 1 to flush */
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct XLogwrtResult {
    pub Write: XLogRecPtr,  /* last byte + 1 written out */
    pub Flush: XLogRecPtr,  /* last byte + 1 flushed */
}

/*
 * WAL insertion lock -- one per inserter slot.
 */
#[repr(C)]
pub struct WALInsertLock {
    pub lock: LWLock,
    pub insertingAt: pg_atomic_uint64,
    pub lastImportantAt: XLogRecPtr,
}

// PG_CACHE_LINE_SIZE -- typically 64 on x86_64
const PG_CACHE_LINE_SIZE: usize = 128;

#[repr(C)]
pub union WALInsertLockPadded {
    pub l: core::mem::ManuallyDrop<WALInsertLock>,
    pub pad: [u8; PG_CACHE_LINE_SIZE],
}

/*
 * Session status of running backup.
 */
static mut sessionBackupState: SessionBackupState = SESSION_BACKUP_NONE;

/*
 * Shared state data for WAL insertion.
 */
#[repr(C)]
pub struct XLogCtlInsert {
    pub insertpos_lck: slock_t,  /* protects CurrBytePos and PrevBytePos */
    /*
     * CurrBytePos is the end of reserved WAL. PrevBytePos is the start of
     * the previously inserted record.
     */
    pub CurrBytePos: uint64,
    pub PrevBytePos: uint64,

    pub pad: [u8; PG_CACHE_LINE_SIZE],

    pub RedoRecPtr: XLogRecPtr,      /* current redo point for insertions */
    pub fullPageWrites: bool,

    pub runningBackups: c_int,
    pub lastBackupStart: XLogRecPtr,

    pub WALInsertLocks: *mut WALInsertLockPadded,
}

/*
 * Total shared-memory state for XLOG.
 */
#[repr(C)]
pub struct XLogCtlData {
    pub Insert: XLogCtlInsert,

    /* Protected by info_lck: */
    pub LogwrtRqst: XLogwrtRqst,
    pub RedoRecPtr: XLogRecPtr,
    pub ckptFullXid: FullTransactionId,
    pub asyncXactLSN: XLogRecPtr,
    pub replicationSlotMinLSN: XLogRecPtr,

    pub lastRemovedSegNo: XLogSegNo,

    pub unloggedLSN: pg_atomic_uint64,

    /* Time and LSN of last xlog segment switch. Protected by WALWriteLock. */
    pub lastSegSwitchTime: pg_time_t,
    pub lastSegSwitchLSN: XLogRecPtr,

    /* Accessed using atomics -- info_lck not needed */
    pub logInsertResult: pg_atomic_uint64,
    pub logWriteResult: pg_atomic_uint64,
    pub logFlushResult: pg_atomic_uint64,

    pub InitializedUpTo: XLogRecPtr,

    pub pages: *mut c_char,
    pub xlblocks: *mut pg_atomic_uint64,
    pub XLogCacheBlck: c_int,

    pub InsertTimeLineID: TimeLineID,
    pub PrevTimeLineID: TimeLineID,

    pub SharedRecoveryState: RecoveryState,

    pub InstallXLogFileSegmentActive: bool,

    pub WalWriterSleeping: bool,

    pub lastCheckPointRecPtr: XLogRecPtr,
    pub lastCheckPointEndPtr: XLogRecPtr,
    pub lastCheckPoint: CheckPoint,

    pub lastFpwDisableRecPtr: XLogRecPtr,

    pub info_lck: slock_t,
}

/*
 * Classification of XLogInsertRecord operations.
 */
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum WalInsertClass {
    WALINSERT_NORMAL,
    WALINSERT_SPECIAL_SWITCH,
    WALINSERT_SPECIAL_CHECKPOINT,
}
use WalInsertClass::*;

static mut XLogCtl: *mut XLogCtlData = ptr::null_mut();

/* a private copy of XLogCtl->Insert.WALInsertLocks, for convenience */
static mut WALInsertLocks: *mut WALInsertLockPadded = ptr::null_mut();

/*
 * We maintain an image of pg_control in shared memory.
 */
static mut ControlFile: *mut ControlFileData = ptr::null_mut();

// ---------------------------------------------------------------------------
// Macros (translated to inline functions / const-fn)
// ---------------------------------------------------------------------------

/*
 * INSERT_FREESPACE(endptr) -- amount of space left on the WAL page after endptr.
 */
#[inline]
fn INSERT_FREESPACE(endptr: XLogRecPtr) -> usize {
    if endptr as usize % XLOG_BLCKSZ == 0 {
        0
    } else {
        XLOG_BLCKSZ - (endptr as usize % XLOG_BLCKSZ)
    }
}

/* Macro to advance to next buffer index. */
#[inline]
unsafe fn NextBufIdx(idx: c_int) -> c_int {
    if idx == (*XLogCtl).XLogCacheBlck { 0 } else { idx + 1 }
}

/*
 * XLogRecPtrToBufIdx returns the index of the WAL buffer that holds, or
 * would hold if it was in cache, the page containing 'recptr'.
 */
#[inline]
unsafe fn XLogRecPtrToBufIdx(recptr: XLogRecPtr) -> c_int {
    ((recptr as usize / XLOG_BLCKSZ) % ((*XLogCtl).XLogCacheBlck as usize + 1)) as c_int
}

/* Usable bytes per WAL page */
const UsableBytesInPage: usize = XLOG_BLCKSZ - SizeOfXLogShortPHD;

/* Convert megabytes to segment count (rounds down). */
#[inline]
unsafe fn ConvertToXSegs(x: c_int, segsize: c_int) -> XLogSegNo {
    XLogMBVarToSegs(x, segsize)
}

/* Usable bytes per segment -- computed at startup from ReadControlFile(). */
static mut UsableBytesInSegment: c_int = 0;

/*
 * Private, possibly out-of-date copy of shared LogwrtResult.
 */
static mut LogwrtResult: XLogwrtResult = XLogwrtResult { Write: 0, Flush: 0 };

/*
 * RefreshXLogWriteResult -- update local copy of shared XLogCtl->log{Write,Flush}Result.
 * Flush always trails Write; reads are ordered accordingly.
 */
macro_rules! RefreshXLogWriteResult {
    ($target:expr) => {
        unsafe {
            $target.Flush = pg_atomic_read_u64(&(*XLogCtl).logFlushResult);
            pg_read_barrier();
            $target.Write = pg_atomic_read_u64(&(*XLogCtl).logWriteResult);
        }
    };
}

/*
 * Open log file segment state.
 */
static mut openLogFile: c_int = -1;
static mut openLogSegNo: XLogSegNo = 0;
static mut openLogTLI: TimeLineID = 0;

/*
 * Local copies of equivalent fields in the control file.
 */
static mut LocalMinRecoveryPoint: XLogRecPtr = InvalidXLogRecPtr;
static mut LocalMinRecoveryPointTLI: TimeLineID = 0;
static mut updateMinRecoveryPoint: bool = true;

/* For WALInsertLockAcquire/Release functions */
static mut MyLockNo: c_int = 0;
static mut holdingAllLocks: bool = false;

// TODO(pg-port): forward declarations of functions in later files (xloginsert.c etc.)
// referenced here. These are provided as stubs.
unsafe fn DecodeXLogRecordRequiredSpace(_tot_len: uint32) -> usize { crate::access::transam::xlogreader::DecodeXLogRecordRequiredSpace(_tot_len as usize) }
// TODO(pg-port): XLogReaderState / DecodeXLogRecord etc -- WAL_DEBUG path only
// TODO(pg-port): MaxConnections, max_worker_processes, max_wal_senders, etc.
static mut MaxConnections: c_int = 100;
static mut max_worker_processes: c_int = 8;
static mut max_wal_senders: c_int = 10;
static mut max_prepared_xacts: c_int = 0;
static mut max_locks_per_xact: c_int = 64;
static mut track_commit_timestamp: bool = false;
// TODO(pg-port): GucSource
type GucSource = c_int;
// TODO(pg-port): GUC_check_errdetail / GUC_check_errdetail_fmt macros
macro_rules! GUC_check_errdetail { ($($arg:tt)*) => { () /* TODO(pg-port) */ } }
macro_rules! GUC_check_errdetail_fmt { ($($arg:tt)*) => { () /* TODO(pg-port) */ } }

// ---------------------------------------------------------------------------
// XLogInsertRecord
// ---------------------------------------------------------------------------

/*
 * Insert an XLOG record represented by an already-constructed chain of data
 * chunks.  This is a low-level routine; to construct the WAL record header
 * and data, use the higher-level routines in xloginsert.c.
 *
 * If 'fpw_lsn' is valid, it is the oldest LSN among the pages that this
 * WAL record applies to, that were not included in the record as full page
 * images.  If fpw_lsn <= RedoRecPtr, the function does not perform the
 * insertion and returns InvalidXLogRecPtr.
 *
 * Returns XLOG pointer to end of record (beginning of next record).
 */
pub unsafe fn XLogInsertRecord(
    rdata: *mut XLogRecData,
    fpw_lsn: XLogRecPtr,
    flags: uint8,
    num_fpi: c_int,
    topxid_included: bool,
) -> XLogRecPtr {
    let Insert = &mut (*XLogCtl).Insert;
    let mut rdata_crc: pg_crc32c;
    let inserted: bool;
    let rechdr = (*rdata).data as *mut XLogRecord;
    let info = (*rechdr).xl_info & !XLR_INFO_MASK;
    let mut class = WALINSERT_NORMAL;
    let mut StartPos: XLogRecPtr = 0;
    let mut EndPos: XLogRecPtr = 0;
    let prevDoPageWrites = doPageWrites;
    let insertTLI: TimeLineID;

    /* Does this record type require special handling? */
    if (*rechdr).xl_rmid == RM_XLOG_ID {
        if info == XLOG_SWITCH {
            class = WALINSERT_SPECIAL_SWITCH;
        } else if info == XLOG_CHECKPOINT_REDO {
            class = WALINSERT_SPECIAL_CHECKPOINT;
        }
    }

    /* we assume that all of the record header is in the first chunk */
    debug_assert!((*rdata).len >= SizeOfXLogRecord() as u32);

    /* cross-check on whether we should be here or not */
    if !XLogInsertAllowed() {
        elog!(ERROR, "cannot make new WAL entries during recovery");
    }

    /*
     * Given that we're not in recovery, InsertTimeLineID is set and can't
     * change, so we can read it without a lock.
     */
    insertTLI = (*XLogCtl).InsertTimeLineID;

    START_CRIT_SECTION!();

    if class == WALINSERT_NORMAL {
        WALInsertLockAcquire();

        /*
         * Check to see if my copy of RedoRecPtr is out of date.
         */
        if RedoRecPtr != Insert.RedoRecPtr {
            debug_assert!(RedoRecPtr < Insert.RedoRecPtr);
            RedoRecPtr = Insert.RedoRecPtr;
        }
        doPageWrites = Insert.fullPageWrites || Insert.runningBackups > 0;

        if doPageWrites
            && (!prevDoPageWrites
                || (fpw_lsn != InvalidXLogRecPtr && fpw_lsn <= RedoRecPtr))
        {
            /*
             * Oops, some buffer now needs to be backed up that the caller
             * didn't back up.  Start over.
             */
            WALInsertLockRelease();
            END_CRIT_SECTION!();
            return InvalidXLogRecPtr;
        }

        /*
         * Reserve space for the record in the WAL.
         */
        ReserveXLogInsertLocation(
            (*rechdr).xl_tot_len as c_int,
            &mut StartPos,
            &mut EndPos,
            &mut (*rechdr).xl_prev,
        );

        /* Normal records are always inserted. */
        inserted = true;
    } else if class == WALINSERT_SPECIAL_SWITCH {
        /*
         * In order to insert an XLOG_SWITCH record, we need to hold all of
         * the WAL insertion locks.
         */
        debug_assert_eq!(fpw_lsn, InvalidXLogRecPtr);
        WALInsertLockAcquireExclusive();
        inserted = ReserveXLogSwitch(&mut StartPos, &mut EndPos, &mut (*rechdr).xl_prev);
    } else {
        debug_assert_eq!(class, WALINSERT_SPECIAL_CHECKPOINT);

        /*
         * We need to update both the local and shared copies of RedoRecPtr,
         * which means that we need to hold all the WAL insertion locks.
         */
        debug_assert_eq!(fpw_lsn, InvalidXLogRecPtr);
        WALInsertLockAcquireExclusive();
        ReserveXLogInsertLocation(
            (*rechdr).xl_tot_len as c_int,
            &mut StartPos,
            &mut EndPos,
            &mut (*rechdr).xl_prev,
        );
        RedoRecPtr = StartPos;
        Insert.RedoRecPtr = StartPos;
        inserted = true;
    }

    if inserted {
        /*
         * Now that xl_prev has been filled in, calculate CRC of the record
         * header.
         */
        rdata_crc = (*rechdr).xl_crc;
        COMP_CRC32C!(rdata_crc, rechdr as *const c_void, core::mem::offset_of!(XLogRecord, xl_crc));
        FIN_CRC32C!(rdata_crc);
        (*rechdr).xl_crc = rdata_crc;

        /*
         * Copy the record in the space reserved.
         */
        CopyXLogRecordToWAL(
            (*rechdr).xl_tot_len as c_int,
            class == WALINSERT_SPECIAL_SWITCH,
            rdata,
            StartPos,
            EndPos,
            insertTLI,
        );

        /*
         * Unless record is flagged as not important, update LSN of last
         * important record in the current slot.
         */
        if (flags & XLOG_MARK_UNIMPORTANT) == 0 {
            let lockno = if holdingAllLocks { 0 } else { MyLockNo };
            core::mem::ManuallyDrop::deref_mut(&mut (*WALInsertLocks.add(lockno as usize)).l).lastImportantAt = StartPos;
        }
    }
    /* else: xlog-switch record but position was already at segment start */

    /*
     * Done! Let others know that we're finished.
     */
    WALInsertLockRelease();

    END_CRIT_SECTION!();

    MarkCurrentTransactionIdLoggedIfAny();

    /*
     * Mark top transaction id is logged (if needed).
     */
    if topxid_included {
        MarkSubxactTopXidLogged();
    }

    /*
     * Update shared LogwrtRqst.Write, if we crossed page boundary.
     */
    if StartPos / XLOG_BLCKSZ as u64 != EndPos / XLOG_BLCKSZ as u64 {
        SpinLockAcquire(&mut (*XLogCtl).info_lck);
        /* advance global request to include new block(s) */
        if (*XLogCtl).LogwrtRqst.Write < EndPos {
            (*XLogCtl).LogwrtRqst.Write = EndPos;
        }
        SpinLockRelease(&mut (*XLogCtl).info_lck);
        RefreshXLogWriteResult!(LogwrtResult);
    }

    /*
     * If this was an XLOG_SWITCH record, flush the record and the empty
     * padding space that fills the rest of the segment.
     */
    if class == WALINSERT_SPECIAL_SWITCH {
        TRACE_POSTGRESQL_WAL_SWITCH!();
        XLogFlush(EndPos);

        /*
         * Return a pointer to just the end of the xlog-switch record.
         */
        if inserted {
            let mut ep = StartPos + SizeOfXLogRecord() as u64;
            if StartPos / XLOG_BLCKSZ as u64 != ep / XLOG_BLCKSZ as u64 {
                let offset = XLogSegmentOffset(ep, wal_segment_size);
                if offset == (ep % XLOG_BLCKSZ as u64) as u64 {
                    ep += SizeOfXLogLongPHD as u64;
                } else {
                    ep += SizeOfXLogShortPHD as u64;
                }
            }
            EndPos = ep;
        }
    }

    /*
     * Update our global variables
     */
    ProcLastRecPtr = StartPos;
    XactLastRecEnd = EndPos;

    /* Report WAL traffic to the instrumentation. */
    if inserted {
        pgWalUsage.wal_bytes += (*rechdr).xl_tot_len as u64;
        pgWalUsage.wal_records += 1;
        pgWalUsage.wal_fpi += num_fpi as u64;

        pgstat_report_fixed = true;
    }

    EndPos
}

// ---------------------------------------------------------------------------
// ReserveXLogInsertLocation
// ---------------------------------------------------------------------------

/*
 * Reserves the right amount of space for a record of given size from the WAL.
 * *StartPos is set to the beginning of the reserved section, *EndPos to
 * its end+1. *PrevPtr is set to the beginning of the previous record.
 */
#[inline(always)]
unsafe fn ReserveXLogInsertLocation(
    size: c_int,
    StartPos: *mut XLogRecPtr,
    EndPos: *mut XLogRecPtr,
    PrevPtr: *mut XLogRecPtr,
) {
    let Insert = &mut (*XLogCtl).Insert;
    let size = MAXALIGN(size as usize);

    /* All (non xlog-switch) records should contain data. */
    debug_assert!(size > SizeOfXLogRecord());

    SpinLockAcquire(&mut Insert.insertpos_lck);

    let startbytepos = Insert.CurrBytePos;
    let endbytepos = startbytepos + size as uint64;
    let prevbytepos = Insert.PrevBytePos;
    Insert.CurrBytePos = endbytepos;
    Insert.PrevBytePos = startbytepos;

    SpinLockRelease(&mut Insert.insertpos_lck);

    *StartPos = XLogBytePosToRecPtr(startbytepos);
    *EndPos   = XLogBytePosToEndRecPtr(endbytepos);
    *PrevPtr  = XLogBytePosToRecPtr(prevbytepos);

    debug_assert_eq!(XLogRecPtrToBytePos(*StartPos), startbytepos);
    debug_assert_eq!(XLogRecPtrToBytePos(*EndPos),   endbytepos);
    debug_assert_eq!(XLogRecPtrToBytePos(*PrevPtr),  prevbytepos);
}

// ---------------------------------------------------------------------------
// ReserveXLogSwitch
// ---------------------------------------------------------------------------

/*
 * Like ReserveXLogInsertLocation(), but for an xlog-switch record.
 * Returns false if we are already at the beginning of a segment.
 */
unsafe fn ReserveXLogSwitch(
    StartPos: *mut XLogRecPtr,
    EndPos: *mut XLogRecPtr,
    PrevPtr: *mut XLogRecPtr,
) -> bool {
    let Insert = &mut (*XLogCtl).Insert;
    let size = MAXALIGN(SizeOfXLogRecord()) as uint64;

    SpinLockAcquire(&mut Insert.insertpos_lck);

    let startbytepos = Insert.CurrBytePos;

    let ptr = XLogBytePosToEndRecPtr(startbytepos);
    if XLogSegmentOffset(ptr, wal_segment_size) == 0 {
        SpinLockRelease(&mut Insert.insertpos_lck);
        *EndPos = ptr;
        *StartPos = ptr;
        return false;
    }

    let mut endbytepos = startbytepos + size;
    let prevbytepos = Insert.PrevBytePos;

    *StartPos = XLogBytePosToRecPtr(startbytepos);
    *EndPos   = XLogBytePosToEndRecPtr(endbytepos);

    let segleft = wal_segment_size as uint64
        - XLogSegmentOffset(*EndPos, wal_segment_size) as uint64;
    if segleft != wal_segment_size as uint64 {
        /* consume the rest of the segment */
        *EndPos = *EndPos + segleft;
        endbytepos = XLogRecPtrToBytePos(*EndPos);
    }
    Insert.CurrBytePos = endbytepos;
    Insert.PrevBytePos = startbytepos;

    SpinLockRelease(&mut Insert.insertpos_lck);

    *PrevPtr = XLogBytePosToRecPtr(prevbytepos);

    debug_assert_eq!(XLogSegmentOffset(*EndPos, wal_segment_size), 0);
    debug_assert_eq!(XLogRecPtrToBytePos(*EndPos),   endbytepos);
    debug_assert_eq!(XLogRecPtrToBytePos(*StartPos), startbytepos);
    debug_assert_eq!(XLogRecPtrToBytePos(*PrevPtr),  prevbytepos);

    true
}

// ---------------------------------------------------------------------------
// CopyXLogRecordToWAL
// ---------------------------------------------------------------------------

/*
 * Subroutine of XLogInsertRecord.  Copies a WAL record to an already-reserved
 * area in the WAL.
 */
unsafe fn CopyXLogRecordToWAL(
    write_len: c_int,
    isLogSwitch: bool,
    mut rdata: *mut XLogRecData,
    StartPos: XLogRecPtr,
    EndPos: XLogRecPtr,
    tli: TimeLineID,
) {
    let mut CurrPos = StartPos;
    let mut currpos = GetXLogBuffer(CurrPos, tli);
    let mut freespace = INSERT_FREESPACE(CurrPos);

    /* there should be enough space for at least the first field (xl_tot_len) */
    debug_assert!(freespace >= core::mem::size_of::<uint32>());

    /* Copy record data */
    let mut written: usize = 0;
    while !rdata.is_null() {
        let mut rdata_data = (*rdata).data;
        let mut rdata_len = (*rdata).len as usize;

        while rdata_len > freespace {
            /*
             * Write what fits on this page, and continue on the next page.
             */
            debug_assert!(
                CurrPos as usize % XLOG_BLCKSZ >= SizeOfXLogShortPHD || freespace == 0
            );
            ptr::copy_nonoverlapping(rdata_data, currpos as *mut c_void, freespace);
            rdata_data = rdata_data.add(freespace);
            rdata_len -= freespace;
            written += freespace;
            CurrPos += freespace as uint64;

            /*
             * Get pointer to beginning of next page, and set xlp_rem_len
             * in the page header.
             */
            currpos = GetXLogBuffer(CurrPos, tli);
            let pagehdr = currpos as *mut XLogPageHeaderData;
            (*pagehdr).xlp_rem_len = (write_len as usize - written) as uint32;
            (*pagehdr).xlp_info |= XLP_FIRST_IS_CONTRECORD;

            /* skip over the page header */
            if XLogSegmentOffset(CurrPos, wal_segment_size) == 0 {
                CurrPos += SizeOfXLogLongPHD as uint64;
                currpos = currpos.add(SizeOfXLogLongPHD);
            } else {
                CurrPos += SizeOfXLogShortPHD as uint64;
                currpos = currpos.add(SizeOfXLogShortPHD);
            }
            freespace = INSERT_FREESPACE(CurrPos);
        }

        debug_assert!(
            CurrPos as usize % XLOG_BLCKSZ >= SizeOfXLogShortPHD || rdata_len == 0
        );
        ptr::copy_nonoverlapping(rdata_data, currpos as *mut c_void, rdata_len);
        currpos = currpos.add(rdata_len);
        CurrPos += rdata_len as uint64;
        freespace -= rdata_len;
        written += rdata_len;

        rdata = (*rdata).next;
    }
    debug_assert_eq!(written, write_len as usize);

    /*
     * If this was an xlog-switch, consume all the remaining space in the
     * WAL segment.
     */
    if isLogSwitch && XLogSegmentOffset(CurrPos, wal_segment_size) != 0 {
        /* An xlog-switch record doesn't contain any data besides the header */
        debug_assert_eq!(write_len as usize, SizeOfXLogRecord());
        /* Assert that we did reserve the right amount of space */
        debug_assert_eq!(XLogSegmentOffset(EndPos, wal_segment_size), 0);

        /* Use up all the remaining space on the current page */
        CurrPos += freespace as uint64;

        /*
         * Cause all remaining pages in the segment to be flushed.
         */
        while CurrPos < EndPos {
            currpos = GetXLogBuffer(CurrPos, tli);
            MemSet(currpos as *mut c_void, 0, SizeOfXLogShortPHD);
            CurrPos += XLOG_BLCKSZ as uint64;
        }
    } else {
        /* Align the end position, so that the next record starts aligned */
        CurrPos = MAXALIGN64(CurrPos);
    }

    if CurrPos != EndPos {
        ereport!(PANIC, errmsg!("space reserved for WAL record does not match what was written"));
    }
}

// ---------------------------------------------------------------------------
// WALInsertLock functions
// ---------------------------------------------------------------------------

/*
 * Acquire a WAL insertion lock, for inserting to WAL.
 */
unsafe fn WALInsertLockAcquire() {
    static mut lockToTry: c_int = -1;

    if lockToTry == -1 {
        lockToTry = MyProcNumber % NUM_XLOGINSERT_LOCKS as c_int;
    }
    MyLockNo = lockToTry;

    let immed = LWLockAcquire(
        &mut core::mem::ManuallyDrop::deref_mut(&mut (*WALInsertLocks.add(MyLockNo as usize)).l).lock,
        LW_EXCLUSIVE,
    );
    if !immed {
        /*
         * If we couldn't get the lock immediately, try another lock next time.
         */
        lockToTry = (lockToTry + 1) % NUM_XLOGINSERT_LOCKS as c_int;
    }
}

/*
 * Acquire all WAL insertion locks, to prevent other backends from inserting
 * to WAL.
 */
unsafe fn WALInsertLockAcquireExclusive() {
    for i in 0..(NUM_XLOGINSERT_LOCKS - 1) {
        LWLockAcquire(&mut core::mem::ManuallyDrop::deref_mut(&mut (*WALInsertLocks.add(i)).l).lock, LW_EXCLUSIVE);
        LWLockUpdateVar(
            &mut core::mem::ManuallyDrop::deref_mut(&mut (*WALInsertLocks.add(i)).l).lock,
            &mut core::mem::ManuallyDrop::deref_mut(&mut (*WALInsertLocks.add(i)).l).insertingAt.value as *mut _ as *mut uint64,
            PG_UINT64_MAX,
        );
    }
    /* Variable value reset to 0 at release */
    LWLockAcquire(
        &mut core::mem::ManuallyDrop::deref_mut(&mut (*WALInsertLocks.add(NUM_XLOGINSERT_LOCKS - 1)).l).lock,
        LW_EXCLUSIVE,
    );

    holdingAllLocks = true;
}

/*
 * Release our insertion lock (or locks, if we're holding them all).
 */
unsafe fn WALInsertLockRelease() {
    if holdingAllLocks {
        for i in 0..NUM_XLOGINSERT_LOCKS {
            LWLockReleaseClearVar(
                &mut core::mem::ManuallyDrop::deref_mut(&mut (*WALInsertLocks.add(i)).l).lock,
                &mut core::mem::ManuallyDrop::deref_mut(&mut (*WALInsertLocks.add(i)).l).insertingAt.value as *mut _ as *mut uint64,
                0,
            );
        }
        holdingAllLocks = false;
    } else {
        LWLockReleaseClearVar(
            &mut core::mem::ManuallyDrop::deref_mut(&mut (*WALInsertLocks.add(MyLockNo as usize)).l).lock,
            &mut core::mem::ManuallyDrop::deref_mut(&mut (*WALInsertLocks.add(MyLockNo as usize)).l).insertingAt.value as *mut _ as *mut uint64,
            0,
        );
    }
}

/*
 * Update our insertingAt value, to let others know that we've finished
 * inserting up to that point.
 */
unsafe fn WALInsertLockUpdateInsertingAt(insertingAt: XLogRecPtr) {
    if holdingAllLocks {
        /*
         * We use the last lock to mark our actual position.
         */
        LWLockUpdateVar(
            &mut core::mem::ManuallyDrop::deref_mut(&mut (*WALInsertLocks.add(NUM_XLOGINSERT_LOCKS - 1)).l).lock,
            &mut core::mem::ManuallyDrop::deref_mut(&mut (*WALInsertLocks.add(NUM_XLOGINSERT_LOCKS - 1)).l)
                .insertingAt.value as *mut _ as *mut uint64,
            insertingAt,
        );
    } else {
        LWLockUpdateVar(
            &mut core::mem::ManuallyDrop::deref_mut(&mut (*WALInsertLocks.add(MyLockNo as usize)).l).lock,
            &mut core::mem::ManuallyDrop::deref_mut(&mut (*WALInsertLocks.add(MyLockNo as usize)).l)
                .insertingAt.value as *mut _ as *mut uint64,
            insertingAt,
        );
    }
}

// ---------------------------------------------------------------------------
// WaitXLogInsertionsToFinish
// ---------------------------------------------------------------------------

/*
 * Wait for any WAL insertions < upto to finish.
 * Returns the location of the oldest insertion that is still in-progress.
 */
unsafe fn WaitXLogInsertionsToFinish(upto: XLogRecPtr) -> XLogRecPtr {
    let Insert = &mut (*XLogCtl).Insert;

    if MyProc.is_null() {
        elog!(PANIC, "cannot wait without a PGPROC structure");
    }

    /*
     * Check if there's any work to do.
     */
    let mut inserted = pg_atomic_read_membarrier_u64(&(*XLogCtl).logInsertResult);
    if upto <= inserted {
        return inserted;
    }

    /* Read the current insert position */
    SpinLockAcquire(&mut Insert.insertpos_lck);
    let bytepos = Insert.CurrBytePos;
    SpinLockRelease(&mut Insert.insertpos_lck);
    let mut reservedUpto = XLogBytePosToEndRecPtr(bytepos);

    /*
     * No-one should request to flush a piece of WAL that hasn't been
     * reserved yet.
     */
    let mut upto = upto;
    if upto > reservedUpto {
        ereport!(
            LOG,
            errmsg!(
                "request to flush past end of generated WAL; request {}/{}, current position {}/{}",
                LSN_FORMAT_ARGS(upto).0, LSN_FORMAT_ARGS(upto).1,
                LSN_FORMAT_ARGS(reservedUpto).0, LSN_FORMAT_ARGS(reservedUpto).1
            )
        );
        upto = reservedUpto;
    }

    /*
     * Loop through all the locks, sleeping on any in-progress insert older
     * than 'upto'.
     */
    let mut finishedUpto = reservedUpto;
    for i in 0..NUM_XLOGINSERT_LOCKS {
        let mut insertingat: XLogRecPtr = InvalidXLogRecPtr;

        loop {
            /*
             * See if this insertion is in progress.
             */
            if LWLockWaitForVar(
                &mut core::mem::ManuallyDrop::deref_mut(&mut (*WALInsertLocks.add(i)).l).lock,
                &mut core::mem::ManuallyDrop::deref_mut(&mut (*WALInsertLocks.add(i)).l).insertingAt.value as *mut _ as *mut uint64,
                insertingat,
                &mut insertingat,
            ) {
                /* the lock was free, so no insertion in progress */
                insertingat = InvalidXLogRecPtr;
                break;
            }

            /*
             * This insertion is still in progress. Have to wait, unless the
             * inserter has proceeded past 'upto'.
             */
            if insertingat >= upto {
                break;
            }
        }

        if insertingat != InvalidXLogRecPtr && insertingat < finishedUpto {
            finishedUpto = insertingat;
        }
    }

    /*
     * Advance the limit we know to have been inserted.
     */
    finishedUpto = pg_atomic_monotonic_advance_u64(&(*XLogCtl).logInsertResult, finishedUpto);

    finishedUpto
}

/* pg_atomic_monotonic_advance_u64 -- atomically advance to max(current, newval), return new value */
#[inline]
unsafe fn pg_atomic_monotonic_advance_u64(ptr: &pg_atomic_uint64, newval: uint64) -> uint64 {
    let mut curval = ptr.value.load(Ordering::Relaxed);
    loop {
        if curval >= newval {
            return curval;
        }
        match ptr.value.compare_exchange_weak(curval, newval, Ordering::SeqCst, Ordering::Relaxed) {
            Ok(_) => return newval,
            Err(v) => curval = v,
        }
    }
}

// ---------------------------------------------------------------------------
// GetXLogBuffer
// ---------------------------------------------------------------------------

/*
 * Get a pointer to the right location in the WAL buffer containing the
 * given XLogRecPtr.
 */
unsafe fn GetXLogBuffer(ptr: XLogRecPtr, tli: TimeLineID) -> *mut c_char {
    static mut cachedPage: uint64 = 0;
    static mut cachedPos: *mut c_char = ptr::null_mut();

    let idx: c_int;
    let endptr: XLogRecPtr;
    let expectedEndPtr: XLogRecPtr;

    /*
     * Fast path for the common case that we need to access again the same
     * page as last time.
     */
    if ptr / XLOG_BLCKSZ as u64 == cachedPage {
        let hdr = cachedPos as *mut XLogPageHeaderData;
        debug_assert_eq!((*hdr).xlp_magic, XLOG_PAGE_MAGIC);
        debug_assert_eq!((*hdr).xlp_pageaddr, ptr - (ptr % XLOG_BLCKSZ as u64));
        return cachedPos.add((ptr % XLOG_BLCKSZ as u64) as usize);
    }

    idx = XLogRecPtrToBufIdx(ptr);

    expectedEndPtr = ptr + (XLOG_BLCKSZ as u64 - ptr % XLOG_BLCKSZ as u64);

    let cur_endptr = pg_atomic_read_u64(&*(*XLogCtl).xlblocks.add(idx as usize));
    if expectedEndPtr != cur_endptr {
        let initializedUpto: XLogRecPtr;

        /*
         * Before calling AdvanceXLInsertBuffer(), which can block, let others
         * know how far we're finished with inserting the record.
         */
        if ptr % XLOG_BLCKSZ as u64 == SizeOfXLogShortPHD as u64
            && XLogSegmentOffset(ptr, wal_segment_size) as usize > XLOG_BLCKSZ
        {
            initializedUpto = ptr - SizeOfXLogShortPHD as u64;
        } else if ptr % XLOG_BLCKSZ as u64 == SizeOfXLogLongPHD as u64
            && (XLogSegmentOffset(ptr, wal_segment_size) as usize) < XLOG_BLCKSZ
        {
            initializedUpto = ptr - SizeOfXLogLongPHD as u64;
        } else {
            initializedUpto = ptr;
        }

        WALInsertLockUpdateInsertingAt(initializedUpto);

        AdvanceXLInsertBuffer(ptr, tli, false);
        let new_endptr = pg_atomic_read_u64(&*(*XLogCtl).xlblocks.add(idx as usize));

        if expectedEndPtr != new_endptr {
            elog!(PANIC, "could not find WAL buffer for {}/{}", LSN_FORMAT_ARGS(ptr).0, LSN_FORMAT_ARGS(ptr).1);
        }
    } else {
        /*
         * Make sure the initialization of the page is visible to us.
         */
        pg_memory_barrier();
    }

    /*
     * Found the buffer holding this page.
     */
    cachedPage = ptr / XLOG_BLCKSZ as u64;
    cachedPos = (*XLogCtl).pages.add(idx as usize * XLOG_BLCKSZ);

    let hdr = cachedPos as *mut XLogPageHeaderData;
    debug_assert_eq!((*hdr).xlp_magic, XLOG_PAGE_MAGIC);
    debug_assert_eq!((*hdr).xlp_pageaddr, ptr - (ptr % XLOG_BLCKSZ as u64));

    cachedPos.add((ptr % XLOG_BLCKSZ as u64) as usize)
}

// ---------------------------------------------------------------------------
// WALReadFromBuffers
// ---------------------------------------------------------------------------

/*
 * Read WAL data directly from WAL buffers, if available.  Returns the number
 * of bytes read successfully.
 */
pub unsafe fn WALReadFromBuffers(
    dstbuf: *mut c_char,
    startptr: XLogRecPtr,
    count: Size,
    tli: TimeLineID,
) -> Size {
    let mut pdst = dstbuf;
    let mut recptr = startptr;
    let mut nbytes = count;

    if RecoveryInProgress() || tli != GetWALInsertionTimeLine() {
        return 0;
    }

    debug_assert!(!XLogRecPtrIsInvalid(startptr));

    /*
     * Caller should ensure that the requested data has been inserted into WAL
     * buffers before we try to read it.
     */
    let inserted = pg_atomic_read_u64(&(*XLogCtl).logInsertResult);
    if startptr + count as u64 > inserted {
        ereport!(
            ERROR,
            errmsg!(
                "cannot read past end of generated WAL: requested {}/{}, current position {}/{}",
                LSN_FORMAT_ARGS(startptr + count as u64).0, LSN_FORMAT_ARGS(startptr + count as u64).1,
                LSN_FORMAT_ARGS(inserted).0, LSN_FORMAT_ARGS(inserted).1
            )
        );
    }

    /*
     * Loop through the buffers without a lock.
     */
    while nbytes > 0 {
        let offset = (recptr % XLOG_BLCKSZ as u64) as uint32;
        let idx = XLogRecPtrToBufIdx(recptr);
        let expectedEndPtr = recptr + (XLOG_BLCKSZ as u64 - offset as u64);

        /*
         * First verification step: check that the correct page is present.
         */
        let endptr = pg_atomic_read_u64(&*(*XLogCtl).xlblocks.add(idx as usize));
        if expectedEndPtr != endptr {
            break;
        }

        let page = (*XLogCtl).pages.add(idx as usize * XLOG_BLCKSZ);
        let psrc = page.add(offset as usize);
        let npagebytes = if nbytes < (XLOG_BLCKSZ - offset as usize) {
            nbytes
        } else {
            XLOG_BLCKSZ - offset as usize
        };

        pg_read_barrier();

        /* data copy */
        ptr::copy_nonoverlapping(psrc, pdst, npagebytes);

        pg_read_barrier();

        /*
         * Second verification step.
         */
        let endptr2 = pg_atomic_read_u64(&*(*XLogCtl).xlblocks.add(idx as usize));
        if expectedEndPtr != endptr2 {
            break;
        }

        pdst = pdst.add(npagebytes);
        recptr += npagebytes as u64;
        nbytes -= npagebytes;
    }

    debug_assert!((pdst as usize - dstbuf as usize) <= count);

    (pdst as usize - dstbuf as usize) as Size
}

// ---------------------------------------------------------------------------
// XLogBytePosToRecPtr / XLogBytePosToEndRecPtr / XLogRecPtrToBytePos
// ---------------------------------------------------------------------------

/*
 * Converts a "usable byte position" to XLogRecPtr.
 */
unsafe fn XLogBytePosToRecPtr(bytepos: uint64) -> XLogRecPtr {
    let usable_per_seg = UsableBytesInSegment as uint64;
    let fullsegs = bytepos / usable_per_seg;
    let mut bytesleft = bytepos % usable_per_seg;

    let seg_offset: uint32;
    if bytesleft < (XLOG_BLCKSZ - SizeOfXLogLongPHD) as uint64 {
        /* fits on first page of segment */
        seg_offset = (bytesleft + SizeOfXLogLongPHD as uint64) as uint32;
    } else {
        /* account for the first page on segment with long header */
        let mut so = XLOG_BLCKSZ as uint32;
        bytesleft -= (XLOG_BLCKSZ - SizeOfXLogLongPHD) as uint64;

        let fullpages = bytesleft / UsableBytesInPage as uint64;
        bytesleft = bytesleft % UsableBytesInPage as uint64;

        seg_offset = so + (fullpages * XLOG_BLCKSZ as uint64 + bytesleft + SizeOfXLogShortPHD as uint64) as uint32;
    }

    let mut result: XLogRecPtr = 0;
    XLogSegNoOffsetToRecPtr(fullsegs, seg_offset, wal_segment_size, &mut result);
    result
}

/*
 * Like XLogBytePosToRecPtr, but if the position is at a page boundary,
 * returns a pointer to the beginning of the page (before page header).
 */
unsafe fn XLogBytePosToEndRecPtr(bytepos: uint64) -> XLogRecPtr {
    let usable_per_seg = UsableBytesInSegment as uint64;
    let fullsegs = bytepos / usable_per_seg;
    let mut bytesleft = bytepos % usable_per_seg;

    let seg_offset: uint32;
    if bytesleft < (XLOG_BLCKSZ - SizeOfXLogLongPHD) as uint64 {
        /* fits on first page of segment */
        if bytesleft == 0 {
            seg_offset = 0;
        } else {
            seg_offset = (bytesleft + SizeOfXLogLongPHD as uint64) as uint32;
        }
    } else {
        /* account for the first page on segment with long header */
        let mut so = XLOG_BLCKSZ as uint32;
        bytesleft -= (XLOG_BLCKSZ - SizeOfXLogLongPHD) as uint64;

        let fullpages = bytesleft / UsableBytesInPage as uint64;
        bytesleft = bytesleft % UsableBytesInPage as uint64;

        if bytesleft == 0 {
            seg_offset = so + (fullpages * XLOG_BLCKSZ as uint64 + bytesleft) as uint32;
        } else {
            seg_offset = so + (fullpages * XLOG_BLCKSZ as uint64 + bytesleft + SizeOfXLogShortPHD as uint64) as uint32;
        }
    }

    let mut result: XLogRecPtr = 0;
    XLogSegNoOffsetToRecPtr(fullsegs, seg_offset, wal_segment_size, &mut result);
    result
}

/*
 * Convert an XLogRecPtr to a "usable byte position".
 */
unsafe fn XLogRecPtrToBytePos(ptr: XLogRecPtr) -> uint64 {
    let mut fullsegs: XLogSegNo = 0;
    XLByteToSeg(ptr, &mut fullsegs, wal_segment_size);

    let fullpages = (XLogSegmentOffset(ptr, wal_segment_size) as usize) / XLOG_BLCKSZ;
    let offset = (ptr % XLOG_BLCKSZ as u64) as uint32;

    let result: uint64;
    if fullpages == 0 {
        result = fullsegs * UsableBytesInSegment as uint64;
        if offset > 0 {
            debug_assert!(offset >= SizeOfXLogLongPHD as uint32);
            return result + (offset - SizeOfXLogLongPHD as uint32) as uint64;
        }
    } else {
        result = fullsegs * UsableBytesInSegment as uint64
            + (XLOG_BLCKSZ - SizeOfXLogLongPHD) as uint64
            + (fullpages as uint64 - 1) * UsableBytesInPage as uint64;
        if offset > 0 {
            debug_assert!(offset >= SizeOfXLogShortPHD as uint32);
            return result + (offset - SizeOfXLogShortPHD as uint32) as uint64;
        }
    }
    result
}

// ---------------------------------------------------------------------------
// AdvanceXLInsertBuffer
// ---------------------------------------------------------------------------

/*
 * Initialize XLOG buffers, writing out old buffers if they still contain
 * unwritten data, up to the page containing 'upto'.  Or if 'opportunistic'
 * is true, initialize as many pages as we can without having to write out
 * unwritten data.
 */
unsafe fn AdvanceXLInsertBuffer(upto: XLogRecPtr, tli: TimeLineID, opportunistic: bool) {
    let Insert = &mut (*XLogCtl).Insert;
    let mut nextidx: c_int;
    let mut OldPageRqstPtr: XLogRecPtr;
    let mut WriteRqst: XLogwrtRqst;
    let mut NewPageEndPtr: XLogRecPtr = InvalidXLogRecPtr;
    let mut NewPageBeginPtr: XLogRecPtr;
    let mut NewPage: *mut XLogPageHeaderData;
    let mut npages: c_int = 0;

    LWLockAcquire(WALBufMappingLock!(), LW_EXCLUSIVE);

    /*
     * Now that we have the lock, check if someone initialized the page
     * already.
     */
    while upto >= (*XLogCtl).InitializedUpTo || opportunistic {
        nextidx = XLogRecPtrToBufIdx((*XLogCtl).InitializedUpTo);

        /*
         * Get ending-offset of the buffer page we need to replace.
         */
        OldPageRqstPtr = pg_atomic_read_u64(
            &*(*XLogCtl).xlblocks.add(nextidx as usize),
        );
        if LogwrtResult.Write < OldPageRqstPtr {
            /*
             * Nope, got work to do.
             */
            if opportunistic {
                break;
            }

            /* Advance shared memory write request position */
            SpinLockAcquire(&mut (*XLogCtl).info_lck);
            if (*XLogCtl).LogwrtRqst.Write < OldPageRqstPtr {
                (*XLogCtl).LogwrtRqst.Write = OldPageRqstPtr;
            }
            SpinLockRelease(&mut (*XLogCtl).info_lck);

            RefreshXLogWriteResult!(LogwrtResult);
            if LogwrtResult.Write < OldPageRqstPtr {
                /*
                 * Must acquire write lock.
                 */
                LWLockRelease(WALBufMappingLock!());

                WaitXLogInsertionsToFinish(OldPageRqstPtr);

                LWLockAcquire(WALWriteLock!(), LW_EXCLUSIVE);

                RefreshXLogWriteResult!(LogwrtResult);
                if LogwrtResult.Write >= OldPageRqstPtr {
                    /* OK, someone wrote it already */
                    LWLockRelease(WALWriteLock!());
                } else {
                    /* Have to write it ourselves */
                    TRACE_POSTGRESQL_WAL_BUFFER_WRITE_DIRTY_START!();
                    WriteRqst = XLogwrtRqst { Write: OldPageRqstPtr, Flush: 0 };
                    XLogWrite(WriteRqst, tli, false);
                    LWLockRelease(WALWriteLock!());
                    pgWalUsage.wal_buffers_full += 1;
                    TRACE_POSTGRESQL_WAL_BUFFER_WRITE_DIRTY_DONE!();

                    pgstat_report_fixed = true;
                }
                /* Re-acquire WALBufMappingLock and retry */
                LWLockAcquire(WALBufMappingLock!(), LW_EXCLUSIVE);
                continue;
            }
        }

        /*
         * Now the next buffer slot is free and we can set it up to be the
         * next output page.
         */
        NewPageBeginPtr = (*XLogCtl).InitializedUpTo;
        NewPageEndPtr = NewPageBeginPtr + XLOG_BLCKSZ as u64;

        debug_assert_eq!(XLogRecPtrToBufIdx(NewPageBeginPtr), nextidx);

        NewPage = ((*XLogCtl).pages as *mut u8)
            .add(nextidx as usize * XLOG_BLCKSZ) as *mut XLogPageHeaderData;

        /*
         * Mark the xlblock with InvalidXLogRecPtr and issue a write barrier
         * before initializing.
         */
        pg_atomic_write_u64(
            &*(*XLogCtl).xlblocks.add(nextidx as usize),
            InvalidXLogRecPtr,
        );
        pg_write_barrier();

        /* Zero the buffer */
        MemSet(NewPage as *mut c_void, 0, XLOG_BLCKSZ);

        /* Fill the new page's header */
        (*NewPage).xlp_magic = XLOG_PAGE_MAGIC;
        (*NewPage).xlp_tli = tli;
        (*NewPage).xlp_pageaddr = NewPageBeginPtr;

        /*
         * If online backup is not in progress, mark the header to indicate
         * that WAL records beginning in this page have removable backup blocks.
         */
        if Insert.runningBackups == 0 {
            (*NewPage).xlp_info |= XLP_BKP_REMOVABLE;
        }

        /*
         * If first page of an XLOG segment file, make it a long header.
         */
        if XLogSegmentOffset((*NewPage).xlp_pageaddr, wal_segment_size) == 0 {
            let NewLongPage = NewPage as *mut XLogLongPageHeaderData;
            (*NewLongPage).xlp_sysid = (*ControlFile).system_identifier;
            (*NewLongPage).xlp_seg_size = wal_segment_size as uint32;
            (*NewLongPage).xlp_xlog_blcksz = XLOG_BLCKSZ as uint32;
            (*NewPage).xlp_info |= XLP_LONG_HEADER;
        }

        /*
         * Make sure the initialization of the page becomes visible to others
         * before the xlblocks update.
         */
        pg_write_barrier();

        pg_atomic_write_u64(
            &*(*XLogCtl).xlblocks.add(nextidx as usize),
            NewPageEndPtr,
        );
        (*XLogCtl).InitializedUpTo = NewPageEndPtr;

        npages += 1;
    }
    LWLockRelease(WALBufMappingLock!());
}

// ---------------------------------------------------------------------------
// CalculateCheckpointSegments and GUC assign hooks
// ---------------------------------------------------------------------------

/*
 * Calculate CheckPointSegments based on max_wal_size_mb and
 * checkpoint_completion_target.
 */
unsafe fn CalculateCheckpointSegments() {
    /*
     * Calculate the distance at which to trigger a checkpoint, to avoid
     * exceeding max_wal_size_mb.
     */
    let target = ConvertToXSegs(max_wal_size_mb, wal_segment_size) as f64
        / (1.0 + CheckPointCompletionTarget);

    /* round down */
    CheckPointSegments = target as c_int;

    if CheckPointSegments < 1 {
        CheckPointSegments = 1;
    }
}

pub unsafe fn assign_max_wal_size(newval: c_int, _extra: *mut c_void) {
    max_wal_size_mb = newval;
    CalculateCheckpointSegments();
}

pub unsafe fn assign_checkpoint_completion_target(newval: f64, _extra: *mut c_void) {
    CheckPointCompletionTarget = newval;
    CalculateCheckpointSegments();
}

pub unsafe fn check_wal_segment_size(newval: *mut c_int, _extra: *mut *mut c_void, _source: GucSource) -> bool {
    if !IsValidWalSegSize(*newval) {
        GUC_check_errdetail!("The WAL segment size must be a power of two between 1 MB and 1 GB.");
        return false;
    }
    true
}

// ---------------------------------------------------------------------------
// XLOGfileslop
// ---------------------------------------------------------------------------

/*
 * At a checkpoint, how many WAL segments to recycle as preallocated future
 * XLOG segments?  Returns the highest segment that should be preallocated.
 */
unsafe fn XLOGfileslop(lastredoptr: XLogRecPtr) -> XLogSegNo {
    let minSegNo = lastredoptr / wal_segment_size as u64
        + ConvertToXSegs(min_wal_size_mb, wal_segment_size) - 1;
    let maxSegNo = lastredoptr / wal_segment_size as u64
        + ConvertToXSegs(max_wal_size_mb, wal_segment_size) - 1;

    /*
     * Between those limits, recycle enough segments to get us through to the
     * estimated end of next checkpoint.
     */
    let distance =
        (1.0 + CheckPointCompletionTarget) * CheckPointDistanceEstimate * 1.10;

    let mut recycleSegNo = ((lastredoptr as f64 + distance) / wal_segment_size as f64).ceil() as XLogSegNo;

    if recycleSegNo < minSegNo {
        recycleSegNo = minSegNo;
    }
    if recycleSegNo > maxSegNo {
        recycleSegNo = maxSegNo;
    }

    recycleSegNo
}

// ---------------------------------------------------------------------------
// XLogCheckpointNeeded
// ---------------------------------------------------------------------------

/*
 * Check whether we've consumed enough xlog space that a checkpoint is needed.
 */
pub unsafe fn XLogCheckpointNeeded(new_segno: XLogSegNo) -> bool {
    let mut old_segno: XLogSegNo = 0;
    XLByteToSeg(RedoRecPtr, &mut old_segno, wal_segment_size);

    new_segno >= old_segno + CheckPointSegments as u64 - 1
}

// ---------------------------------------------------------------------------
// XLogWrite
// ---------------------------------------------------------------------------

/*
 * Write and/or fsync the log at least as far as WriteRqst indicates.
 * Must be called with WALWriteLock held.
 */
unsafe fn XLogWrite(WriteRqst: XLogwrtRqst, tli: TimeLineID, flexible: bool) {
    let mut ispartialpage: bool;
    let mut last_iteration: bool;
    let mut finishing_seg: bool;
    let mut curridx: c_int;
    let mut npages: c_int;
    let mut startidx: c_int;
    let mut startoffset: uint32;

    /* We should always be inside a critical section here */
    debug_assert!(CritSectionCount > 0);

    /*
     * Update local LogwrtResult.
     */
    RefreshXLogWriteResult!(LogwrtResult);

    npages = 0;
    startidx = 0;
    startoffset = 0;

    curridx = XLogRecPtrToBufIdx(LogwrtResult.Write);

    while LogwrtResult.Write < WriteRqst.Write {
        /*
         * Make sure we're not ahead of the insert process.
         */
        let EndPtr = pg_atomic_read_u64(&*(*XLogCtl).xlblocks.add(curridx as usize));

        if LogwrtResult.Write >= EndPtr {
            elog!(
                PANIC,
                "xlog write request {}/{} is past end of log {}/{}",
                LSN_FORMAT_ARGS(LogwrtResult.Write).0, LSN_FORMAT_ARGS(LogwrtResult.Write).1,
                LSN_FORMAT_ARGS(EndPtr).0, LSN_FORMAT_ARGS(EndPtr).1
            );
        }

        /* Advance LogwrtResult.Write to end of current buffer page */
        LogwrtResult.Write = EndPtr;
        ispartialpage = WriteRqst.Write < LogwrtResult.Write;

        if !XLByteInPrevSeg(LogwrtResult.Write, openLogSegNo, wal_segment_size) {
            /*
             * Switch to new logfile segment.
             */
            debug_assert_eq!(npages, 0);
            if openLogFile >= 0 {
                XLogFileClose();
            }
            XLByteToPrevSeg(LogwrtResult.Write, &mut openLogSegNo, wal_segment_size);
            openLogTLI = tli;

            /* create/use new log file */
            openLogFile = XLogFileInit(openLogSegNo, tli);
            ReserveExternalFD();
        }

        /* Make sure we have the current logfile open */
        if openLogFile < 0 {
            XLByteToPrevSeg(LogwrtResult.Write, &mut openLogSegNo, wal_segment_size);
            openLogTLI = tli;
            openLogFile = XLogFileOpen(openLogSegNo, tli);
            ReserveExternalFD();
        }

        /* Add current page to the set of pending pages-to-dump */
        if npages == 0 {
            startidx = curridx;
            startoffset = XLogSegmentOffset(
                LogwrtResult.Write - XLOG_BLCKSZ as u64,
                wal_segment_size,
            ) as uint32;
        }
        npages += 1;

        /*
         * Dump the set if this will be the last loop iteration, or if we are
         * at the last page of the cache area (since the next page won't be
         * contiguous in memory), or if we are at the end of the logfile segment.
         */
        last_iteration = WriteRqst.Write <= LogwrtResult.Write;

        finishing_seg = !ispartialpage
            && (startoffset as usize + npages as usize * XLOG_BLCKSZ) >= wal_segment_size as usize;

        if last_iteration || curridx == (*XLogCtl).XLogCacheBlck || finishing_seg {
            let from = (*XLogCtl).pages.add(startidx as usize * XLOG_BLCKSZ);
            let nbytes = npages as usize * XLOG_BLCKSZ;
            let mut nleft = nbytes;
            let mut cur_offset = startoffset as i64;
            let mut cur_from = from;

            loop {
                /* Measure I/O timing */
                let start = pgstat_prepare_io_time(track_wal_io_timing);

                pgstat_report_wait_start(WAIT_EVENT_WAL_WRITE);
                let written = pg_pwrite(openLogFile, cur_from as *const c_void, nleft, cur_offset);
                pgstat_report_wait_end();

                pgstat_count_io_op_time(IOOBJECT_WAL, IOCONTEXT_NORMAL, IOOP_WRITE, start, 1, written);

                if written <= 0 {
                    let mut xlogfname = [0u8; MAXFNAMELEN];
                    let save_errno = *libc::__error();
                    if save_errno == libc::EINTR {
                        continue;
                    }
                    XLogFileName(
                        xlogfname.as_mut_ptr() as *mut c_char,
                        tli,
                        openLogSegNo,
                        wal_segment_size,
                    );
                    ereport!(
                        PANIC,
                        errmsg!(
                            "could not write to log file at offset {}, length {}: errno {}",
                            startoffset, nleft, save_errno
                        )
                    );
                }
                nleft -= written as usize;
                cur_from = cur_from.add(written as usize);
                cur_offset += written as i64;
                if nleft == 0 {
                    break;
                }
            }

            npages = 0;

            /*
             * If we just wrote the whole last page of a logfile segment,
             * fsync the segment immediately.
             */
            if finishing_seg {
                issue_xlog_fsync(openLogFile, openLogSegNo, tli);

                /* signal that we need to wakeup walsenders later */
                WalSndWakeupRequest();

                LogwrtResult.Flush = LogwrtResult.Write; /* end of page */

                if XLogArchivingActive() {
                    XLogArchiveNotifySeg(openLogSegNo, tli);
                }

                (*XLogCtl).lastSegSwitchTime = time_now();
                (*XLogCtl).lastSegSwitchLSN = LogwrtResult.Flush;

                /*
                 * Request a checkpoint if we've consumed too much xlog since
                 * the last one.
                 */
                if IsUnderPostmaster && XLogCheckpointNeeded(openLogSegNo) {
                    GetRedoRecPtr();
                    if XLogCheckpointNeeded(openLogSegNo) {
                        RequestCheckpoint(CHECKPOINT_CAUSE_XLOG);
                    }
                }
            }
        }

        if ispartialpage {
            /* Only asked to write a partial page */
            LogwrtResult.Write = WriteRqst.Write;
            break;
        }
        curridx = NextBufIdx(curridx);

        /* If flexible, break out of loop as soon as we wrote something */
        if flexible && npages == 0 {
            break;
        }
    }

    debug_assert_eq!(npages, 0);

    /*
     * If asked to flush, do so
     */
    if LogwrtResult.Flush < WriteRqst.Flush && LogwrtResult.Flush < LogwrtResult.Write {
        if wal_sync_method != WAL_SYNC_METHOD_OPEN
            && wal_sync_method != WAL_SYNC_METHOD_OPEN_DSYNC
        {
            if openLogFile >= 0
                && !XLByteInPrevSeg(LogwrtResult.Write, openLogSegNo, wal_segment_size)
            {
                XLogFileClose();
            }
            if openLogFile < 0 {
                XLByteToPrevSeg(LogwrtResult.Write, &mut openLogSegNo, wal_segment_size);
                openLogTLI = tli;
                openLogFile = XLogFileOpen(openLogSegNo, tli);
                ReserveExternalFD();
            }

            issue_xlog_fsync(openLogFile, openLogSegNo, tli);
        }

        /* signal that we need to wakeup walsenders later */
        WalSndWakeupRequest();

        LogwrtResult.Flush = LogwrtResult.Write;
    }

    /*
     * Update shared-memory status.
     */
    SpinLockAcquire(&mut (*XLogCtl).info_lck);
    if (*XLogCtl).LogwrtRqst.Write < LogwrtResult.Write {
        (*XLogCtl).LogwrtRqst.Write = LogwrtResult.Write;
    }
    if (*XLogCtl).LogwrtRqst.Flush < LogwrtResult.Flush {
        (*XLogCtl).LogwrtRqst.Flush = LogwrtResult.Flush;
    }
    SpinLockRelease(&mut (*XLogCtl).info_lck);

    /*
     * We write Write first, bar, then Flush.
     */
    pg_atomic_write_u64(&(*XLogCtl).logWriteResult, LogwrtResult.Write);
    pg_write_barrier();
    pg_atomic_write_u64(&(*XLogCtl).logFlushResult, LogwrtResult.Flush);
}

// ---------------------------------------------------------------------------
// XLogSetAsyncXactLSN
// ---------------------------------------------------------------------------

/*
 * Record the LSN for an asynchronous transaction commit/abort and nudge
 * the WALWriter if there is work for it to do.
 */
pub unsafe fn XLogSetAsyncXactLSN(asyncXactLSN: XLogRecPtr) {
    let WriteRqstPtr = asyncXactLSN;
    let sleeping: bool;
    let mut wakeup = false;
    let prevAsyncXactLSN: XLogRecPtr;

    SpinLockAcquire(&mut (*XLogCtl).info_lck);
    sleeping = (*XLogCtl).WalWriterSleeping;
    prevAsyncXactLSN = (*XLogCtl).asyncXactLSN;
    if (*XLogCtl).asyncXactLSN < asyncXactLSN {
        (*XLogCtl).asyncXactLSN = asyncXactLSN;
    }
    SpinLockRelease(&mut (*XLogCtl).info_lck);

    /*
     * If somebody else already called this function with a more aggressive
     * LSN, they will have done what we needed.
     */
    if asyncXactLSN <= prevAsyncXactLSN {
        return;
    }

    if sleeping {
        wakeup = true;
    } else {
        RefreshXLogWriteResult!(LogwrtResult);

        let flushblocks = (WriteRqstPtr / XLOG_BLCKSZ as u64)
            .wrapping_sub(LogwrtResult.Flush / XLOG_BLCKSZ as u64) as c_int;

        if WalWriterFlushAfter == 0 || flushblocks >= WalWriterFlushAfter {
            wakeup = true;
        }
    }

    if wakeup {
        let procglobal = ProcGlobal as *const PROC_HDR;
        let walwriterProc = (*procglobal).walwriterProc;

        if walwriterProc != INVALID_PROC_NUMBER {
            SetLatch(&mut (*GetPGProcByNumber(walwriterProc)).procLatch);
        }
    }
}

// ---------------------------------------------------------------------------
// XLogSetReplicationSlotMinimumLSN / XLogGetReplicationSlotMinimumLSN
// ---------------------------------------------------------------------------

/*
 * Record the LSN up to which we can remove WAL because it's not required by
 * any replication slot.
 */
pub unsafe fn XLogSetReplicationSlotMinimumLSN(lsn: XLogRecPtr) {
    SpinLockAcquire(&mut (*XLogCtl).info_lck);
    (*XLogCtl).replicationSlotMinLSN = lsn;
    SpinLockRelease(&mut (*XLogCtl).info_lck);
}

/*
 * Return the oldest LSN we must retain to satisfy the needs of some
 * replication slot.
 */
pub unsafe fn XLogGetReplicationSlotMinimumLSN() -> XLogRecPtr {
    let retval: XLogRecPtr;

    SpinLockAcquire(&mut (*XLogCtl).info_lck);
    retval = (*XLogCtl).replicationSlotMinLSN;
    SpinLockRelease(&mut (*XLogCtl).info_lck);

    retval
}

// ---------------------------------------------------------------------------
// UpdateMinRecoveryPoint
// ---------------------------------------------------------------------------

/*
 * Advance minRecoveryPoint in control file.
 */
unsafe fn UpdateMinRecoveryPoint(lsn: XLogRecPtr, force: bool) {
    /* Quick check using our local copy of the variable */
    if !updateMinRecoveryPoint || (!force && lsn <= LocalMinRecoveryPoint) {
        return;
    }

    /*
     * An invalid minRecoveryPoint means that we need to recover all the WAL.
     */
    if XLogRecPtrIsInvalid(LocalMinRecoveryPoint) && InRecovery {
        updateMinRecoveryPoint = false;
        return;
    }

    LWLockAcquire(ControlFileLock!(), LW_EXCLUSIVE);

    /* update local copy */
    LocalMinRecoveryPoint = (*ControlFile).minRecoveryPoint;
    LocalMinRecoveryPointTLI = (*ControlFile).minRecoveryPointTLI;

    if XLogRecPtrIsInvalid(LocalMinRecoveryPoint) {
        updateMinRecoveryPoint = false;
    } else if force || LocalMinRecoveryPoint < lsn {
        let mut newMinRecoveryPointTLI: TimeLineID = 0;

        /*
         * To avoid having to update the control file too often, we update it
         * all the way to the last record being replayed.
         */
        let newMinRecoveryPoint = GetCurrentReplayRecPtr(&mut newMinRecoveryPointTLI);
        if !force && newMinRecoveryPoint < lsn {
            elog!(
                WARNING,
                "xlog min recovery request {}/{} is past current point {}/{}",
                LSN_FORMAT_ARGS(lsn).0, LSN_FORMAT_ARGS(lsn).1,
                LSN_FORMAT_ARGS(newMinRecoveryPoint).0, LSN_FORMAT_ARGS(newMinRecoveryPoint).1
            );
        }

        /* update control file */
        if (*ControlFile).minRecoveryPoint < newMinRecoveryPoint {
            (*ControlFile).minRecoveryPoint = newMinRecoveryPoint;
            (*ControlFile).minRecoveryPointTLI = newMinRecoveryPointTLI;
            UpdateControlFile();
            LocalMinRecoveryPoint = newMinRecoveryPoint;
            LocalMinRecoveryPointTLI = newMinRecoveryPointTLI;
        }
    }
    LWLockRelease(ControlFileLock!());
}

// ---------------------------------------------------------------------------
// XLogFlush
// ---------------------------------------------------------------------------

/*
 * Ensure that all XLOG data through the given position is flushed to disk.
 */
#[no_mangle]
pub unsafe fn XLogFlush(record: XLogRecPtr) {
    let mut WriteRqstPtr: XLogRecPtr;
    let mut WriteRqst: XLogwrtRqst;
    let insertTLI = (*XLogCtl).InsertTimeLineID;

    /*
     * During REDO, update minRecoveryPoint instead.
     */
    if !XLogInsertAllowed() {
        UpdateMinRecoveryPoint(record, false);
        return;
    }

    /* Quick exit if already known flushed */
    if record <= LogwrtResult.Flush {
        return;
    }

    START_CRIT_SECTION!();

    /* initialize to given target; may increase below */
    WriteRqstPtr = record;

    /*
     * Now wait until we get the write lock, or someone else does the flush
     * for us.
     */
    loop {
        /* done already? */
        RefreshXLogWriteResult!(LogwrtResult);
        if record <= LogwrtResult.Flush {
            break;
        }

        /*
         * Before actually performing the write, wait for all in-flight
         * insertions to the pages we're about to write to finish.
         */
        SpinLockAcquire(&mut (*XLogCtl).info_lck);
        if WriteRqstPtr < (*XLogCtl).LogwrtRqst.Write {
            WriteRqstPtr = (*XLogCtl).LogwrtRqst.Write;
        }
        SpinLockRelease(&mut (*XLogCtl).info_lck);
        let mut insertpos = WaitXLogInsertionsToFinish(WriteRqstPtr);

        /*
         * Try to get the write lock.
         */
        if !LWLockAcquireOrWait(WALWriteLock!(), LW_EXCLUSIVE) {
            /*
             * The lock is now free, but we didn't acquire it yet.
             */
            continue;
        }

        /* Got the lock; recheck whether request is satisfied */
        RefreshXLogWriteResult!(LogwrtResult);
        if record <= LogwrtResult.Flush {
            LWLockRelease(WALWriteLock!());
            break;
        }

        /*
         * Sleep before flush!
         */
        if CommitDelay > 0 && enableFsync && MinimumActiveBackends(CommitSiblings) {
            pg_usleep(CommitDelay as i64);

            insertpos = WaitXLogInsertionsToFinish(insertpos);
        }

        /* try to write/flush later additions to XLOG as well */
        WriteRqst = XLogwrtRqst { Write: insertpos, Flush: insertpos };

        XLogWrite(WriteRqst, insertTLI, false);

        LWLockRelease(WALWriteLock!());
        /* done */
        break;
    }

    END_CRIT_SECTION!();

    /* wake up walsenders now that we've released heavily contended locks */
    WalSndWakeupProcessRequests(true, !RecoveryInProgress());

    /*
     * If we still haven't flushed to the request point then we have a problem.
     */
    if LogwrtResult.Flush < record {
        elog!(
            ERROR,
            "xlog flush request {}/{} is not satisfied --- flushed only to {}/{}",
            LSN_FORMAT_ARGS(record).0, LSN_FORMAT_ARGS(record).1,
            LSN_FORMAT_ARGS(LogwrtResult.Flush).0, LSN_FORMAT_ARGS(LogwrtResult.Flush).1
        );
    }
}

// ---------------------------------------------------------------------------
// XLogBackgroundFlush
// ---------------------------------------------------------------------------

/*
 * Write & flush xlog, but without specifying exactly where to.
 * Returns true if there was any work to do.
 */
pub unsafe fn XLogBackgroundFlush() -> bool {
    let mut WriteRqst: XLogwrtRqst;
    let mut flexible = true;
    static mut lastflush: TimestampTz = 0;
    let now: TimestampTz;
    let flushblocks: c_int;
    let insertTLI: TimeLineID;

    /* XLOG doesn't need flushing during recovery */
    if RecoveryInProgress() {
        return false;
    }

    insertTLI = (*XLogCtl).InsertTimeLineID;

    /* read updated LogwrtRqst */
    SpinLockAcquire(&mut (*XLogCtl).info_lck);
    WriteRqst = (*XLogCtl).LogwrtRqst;
    SpinLockRelease(&mut (*XLogCtl).info_lck);

    /* back off to last completed page boundary */
    WriteRqst.Write -= WriteRqst.Write % XLOG_BLCKSZ as u64;

    /* if we have already flushed that far, consider async commit records */
    RefreshXLogWriteResult!(LogwrtResult);
    if WriteRqst.Write <= LogwrtResult.Flush {
        SpinLockAcquire(&mut (*XLogCtl).info_lck);
        WriteRqst.Write = (*XLogCtl).asyncXactLSN;
        SpinLockRelease(&mut (*XLogCtl).info_lck);
        flexible = false; /* ensure it all gets written */
    }

    /*
     * If already known flushed, we're done.
     */
    if WriteRqst.Write <= LogwrtResult.Flush {
        if openLogFile >= 0 {
            if !XLByteInPrevSeg(LogwrtResult.Write, openLogSegNo, wal_segment_size) {
                XLogFileClose();
            }
        }
        return false;
    }

    /*
     * Determine how far to flush WAL.
     */
    now = GetCurrentTimestamp();
    flushblocks = (WriteRqst.Write / XLOG_BLCKSZ as u64)
        .wrapping_sub(LogwrtResult.Flush / XLOG_BLCKSZ as u64) as c_int;

    if WalWriterFlushAfter == 0 || lastflush == 0 {
        /* first call, or block based limits disabled */
        WriteRqst.Flush = WriteRqst.Write;
        lastflush = now;
    } else if TimestampDifferenceExceeds(lastflush, now, WalWriterDelay) {
        WriteRqst.Flush = WriteRqst.Write;
        lastflush = now;
    } else if flushblocks >= WalWriterFlushAfter {
        WriteRqst.Flush = WriteRqst.Write;
        lastflush = now;
    } else {
        /* no flushing, this time round */
        WriteRqst.Flush = 0;
    }

    START_CRIT_SECTION!();

    /* now wait for any in-progress insertions to finish and get write lock */
    WaitXLogInsertionsToFinish(WriteRqst.Write);
    LWLockAcquire(WALWriteLock!(), LW_EXCLUSIVE);
    RefreshXLogWriteResult!(LogwrtResult);
    if WriteRqst.Write > LogwrtResult.Write || WriteRqst.Flush > LogwrtResult.Flush {
        XLogWrite(WriteRqst, insertTLI, flexible);
    }
    LWLockRelease(WALWriteLock!());

    END_CRIT_SECTION!();

    /* wake up walsenders */
    WalSndWakeupProcessRequests(true, !RecoveryInProgress());

    /*
     * Try to initialize as many of the no-longer-needed WAL buffers for
     * future use as we can.
     */
    AdvanceXLInsertBuffer(InvalidXLogRecPtr, insertTLI, true);

    true
}

// ---------------------------------------------------------------------------
// XLogNeedsFlush
// ---------------------------------------------------------------------------

/*
 * Test whether XLOG data has been flushed up to (at least) the given position.
 * Returns true if a flush is still needed.
 */
pub unsafe fn XLogNeedsFlush(record: XLogRecPtr) -> bool {
    if RecoveryInProgress() {
        if XLogRecPtrIsInvalid(LocalMinRecoveryPoint) && InRecovery {
            updateMinRecoveryPoint = false;
        }

        if record <= LocalMinRecoveryPoint || !updateMinRecoveryPoint {
            return false;
        }

        if !LWLockConditionalAcquire(ControlFileLock!(), LW_SHARED) {
            return true;
        }
        LocalMinRecoveryPoint = (*ControlFile).minRecoveryPoint;
        LocalMinRecoveryPointTLI = (*ControlFile).minRecoveryPointTLI;
        LWLockRelease(ControlFileLock!());

        if XLogRecPtrIsInvalid(LocalMinRecoveryPoint) {
            updateMinRecoveryPoint = false;
        }

        if record <= LocalMinRecoveryPoint || !updateMinRecoveryPoint {
            return false;
        } else {
            return true;
        }
    }

    /* Quick exit if already known flushed */
    if record <= LogwrtResult.Flush {
        return false;
    }

    RefreshXLogWriteResult!(LogwrtResult);

    if record <= LogwrtResult.Flush {
        return false;
    }

    true
}

// ---------------------------------------------------------------------------
// XLogFileInitInternal / XLogFileInit / XLogFileCopy / InstallXLogFileSegment
// XLogFileOpen / XLogFileClose / PreallocXlogFiles
// ---------------------------------------------------------------------------

/*
 * Try to make a given XLOG file segment exist.
 * Returns -1 or FD of opened file.
 */
unsafe fn XLogFileInitInternal(
    logsegno: XLogSegNo,
    logtli: TimeLineID,
    added: *mut bool,
    path: *mut c_char,
) -> c_int {
    let mut tmppath = [0u8; MAXPGPATH];
    let mut installed_segno: XLogSegNo;
    let max_segno: XLogSegNo;
    let mut fd: c_int;
    let mut save_errno: c_int;
    let mut open_flags: c_int = libc::O_RDWR | libc::O_CREAT | libc::O_EXCL | PG_BINARY;

    debug_assert_ne!(logtli, 0);

    XLogFilePath(path, logtli, logsegno, wal_segment_size);

    /*
     * Try to use existent file.
     */
    *added = false;
    fd = BasicOpenFile(
        path,
        libc::O_RDWR | PG_BINARY | O_CLOEXEC | get_sync_bit(wal_sync_method),
    );
    if fd < 0 {
        if *libc::__error() != libc::ENOENT {
            ereport!(ERROR, errmsg!("could not open file: errno {}", *libc::__error()));
        }
    } else {
        return fd;
    }

    /*
     * Initialize an empty (all zeroes) segment.
     */
    elog!(DEBUG2, "creating and filling new WAL file");

    {
        let pid = libc::getpid();
        libc::snprintf(
            tmppath.as_mut_ptr() as *mut c_char,
            MAXPGPATH,
            b"%s/xlogtemp.%d\0".as_ptr() as *const c_char,
            b"pg_wal\0".as_ptr() as *const c_char,
            pid,
        );
    }

    libc::unlink(tmppath.as_ptr() as *const c_char);

    if io_direct_flags & IO_DIRECT_WAL_INIT != 0 {
        open_flags |= PG_O_DIRECT;
    }

    /* do not use get_sync_bit() here -- want to fsync only at end of fill */
    fd = BasicOpenFile(tmppath.as_ptr() as *mut c_char, open_flags);
    if fd < 0 {
        ereport!(ERROR, errmsg!("could not create file: errno {}", *libc::__error()));
    }

    /* Measure I/O timing when initializing segment */
    let io_start = pgstat_prepare_io_time(track_wal_io_timing);

    pgstat_report_wait_start(WAIT_EVENT_WAL_INIT_WRITE);
    save_errno = 0;
    if wal_init_zero {
        let rc = pg_pwrite_zeros(fd, wal_segment_size as usize, 0);
        if rc < 0 {
            save_errno = *libc::__error();
        }
    } else {
        let rc = pg_pwrite(fd, b"\0".as_ptr() as *const c_void, 1, wal_segment_size as i64 - 1);
        if rc != 1 {
            save_errno = if *libc::__error() != 0 { *libc::__error() } else { libc::ENOSPC };
        }
    }
    pgstat_report_wait_end();

    pgstat_count_io_op_time(
        IOOBJECT_WAL, IOCONTEXT_INIT, IOOP_WRITE, io_start, 1,
        if wal_init_zero { wal_segment_size as isize } else { 1 },
    );

    if save_errno != 0 {
        libc::unlink(tmppath.as_ptr() as *const c_char);
        close(fd);
        ereport!(ERROR, errmsg!("could not write to file: errno {}", save_errno));
    }

    /* Measure I/O timing when flushing segment */
    let io_start = pgstat_prepare_io_time(track_wal_io_timing);

    pgstat_report_wait_start(WAIT_EVENT_WAL_INIT_SYNC);
    if pg_fsync(fd) != 0 {
        save_errno = *libc::__error();
        close(fd);
        ereport!(ERROR, errmsg!("could not fsync file: errno {}", save_errno));
    }
    pgstat_report_wait_end();

    pgstat_count_io_op_time(IOOBJECT_WAL, IOCONTEXT_INIT, IOOP_FSYNC, io_start, 1, 0);

    if close(fd) != 0 {
        ereport!(ERROR, errmsg!("could not close file: errno {}", *libc::__error()));
    }

    /*
     * Now move the segment into place with its final name.
     */
    installed_segno = logsegno;

    max_segno = logsegno + CheckPointSegments as u64;
    if InstallXLogFileSegment(&mut installed_segno, tmppath.as_mut_ptr() as *mut c_char, true, max_segno, logtli) {
        *added = true;
        elog!(DEBUG2, "done creating and filling new WAL file");
    } else {
        libc::unlink(tmppath.as_ptr() as *const c_char);
        elog!(DEBUG2, "abandoned new WAL file");
    }

    -1
}

/*
 * Create a new XLOG file segment, or open a pre-existing one.
 * Returns FD of opened file.
 */
pub unsafe fn XLogFileInit(logsegno: XLogSegNo, logtli: TimeLineID) -> c_int {
    let mut ignore_added: bool = false;
    let mut path = [0u8; MAXPGPATH];
    let fd: c_int;

    debug_assert_ne!(logtli, 0);

    fd = XLogFileInitInternal(logsegno, logtli, &mut ignore_added, path.as_mut_ptr() as *mut c_char);
    if fd >= 0 {
        return fd;
    }

    /* Now open original target segment */
    let fd2 = BasicOpenFile(
        path.as_mut_ptr() as *mut c_char,
        libc::O_RDWR | PG_BINARY | O_CLOEXEC | get_sync_bit(wal_sync_method),
    );
    if fd2 < 0 {
        ereport!(ERROR, errmsg!("could not open file: errno {}", *libc::__error()));
    }
    fd2
}

/*
 * Create a new XLOG file segment by copying a pre-existing one.
 */
unsafe fn XLogFileCopy(
    destTLI: TimeLineID,
    destsegno: XLogSegNo,
    srcTLI: TimeLineID,
    srcsegno: XLogSegNo,
    upto: c_int,
) {
    let mut path = [0u8; MAXPGPATH];
    let mut tmppath = [0u8; MAXPGPATH];
    let mut buffer = PGAlignedXLogBlock { data: [0u8; XLOG_BLCKSZ] };
    let srcfd: c_int;
    let fd: c_int;
    let mut nbytes: c_int;

    /* Open the source file */
    XLogFilePath(path.as_mut_ptr() as *mut c_char, srcTLI, srcsegno, wal_segment_size);
    srcfd = OpenTransientFile(path.as_mut_ptr() as *mut c_char, libc::O_RDONLY | PG_BINARY);
    if srcfd < 0 {
        ereport!(ERROR, errmsg!("could not open file: errno {}", *libc::__error()));
    }

    /* Copy into a temp file name */
    {
        let pid = libc::getpid();
        libc::snprintf(
            tmppath.as_mut_ptr() as *mut c_char,
            MAXPGPATH,
            b"%s/xlogtemp.%d\0".as_ptr() as *const c_char,
            b"pg_wal\0".as_ptr() as *const c_char,
            pid,
        );
    }
    libc::unlink(tmppath.as_ptr() as *const c_char);

    fd = OpenTransientFile(
        tmppath.as_mut_ptr() as *mut c_char,
        libc::O_RDWR | libc::O_CREAT | libc::O_EXCL | PG_BINARY,
    );
    if fd < 0 {
        ereport!(ERROR, errmsg!("could not create file: errno {}", *libc::__error()));
    }

    /* Do the data copying */
    nbytes = 0;
    while (nbytes as usize) < wal_segment_size as usize {
        let mut nread = upto - nbytes;

        if (nread as usize) < core::mem::size_of_val(&buffer) {
            ptr::write_bytes(buffer.data.as_mut_ptr(), 0, core::mem::size_of_val(&buffer));
        }

        if nread > 0 {
            if (nread as usize) > core::mem::size_of_val(&buffer) {
                nread = core::mem::size_of_val(&buffer) as c_int;
            }
            pgstat_report_wait_start(WAIT_EVENT_WAL_COPY_READ);
            let r = read(srcfd, buffer.data.as_mut_ptr() as *mut c_void, nread as usize);
            pgstat_report_wait_end();
            if r != nread as isize {
                if r < 0 {
                    ereport!(ERROR, errmsg!("could not read file: errno {}", *libc::__error()));
                } else {
                    ereport!(ERROR, errmsg!("could not read file: read {} of {}", r, nread));
                }
            }
        }

        pgstat_report_wait_start(WAIT_EVENT_WAL_COPY_WRITE);
        let w = write(fd, buffer.data.as_ptr() as *const c_void, core::mem::size_of_val(&buffer));
        pgstat_report_wait_end();
        if w != core::mem::size_of_val(&buffer) as isize {
            let save_errno = *libc::__error();
            libc::unlink(tmppath.as_ptr() as *const c_char);
            let errno = if save_errno != 0 { save_errno } else { libc::ENOSPC };
            ereport!(ERROR, errmsg!("could not write to file: errno {}", errno));
        }

        nbytes += core::mem::size_of_val(&buffer) as c_int;
    }

    pgstat_report_wait_start(WAIT_EVENT_WAL_COPY_SYNC);
    if pg_fsync(fd) != 0 {
        ereport!(data_sync_elevel(ERROR), errmsg!("could not fsync file: errno {}", *libc::__error()));
    }
    pgstat_report_wait_end();

    if CloseTransientFile(fd) != 0 {
        ereport!(ERROR, errmsg!("could not close file: errno {}", *libc::__error()));
    }
    if CloseTransientFile(srcfd) != 0 {
        ereport!(ERROR, errmsg!("could not close file: errno {}", *libc::__error()));
    }

    /* Now move the segment into place with its final name */
    if !InstallXLogFileSegment(&mut destsegno.clone(), tmppath.as_mut_ptr() as *mut c_char, false, 0, destTLI) {
        elog!(ERROR, "InstallXLogFileSegment should not have failed");
    }
}

/*
 * Install a new XLOG segment file as a current or future log segment.
 * Returns true if the file was installed successfully.
 */
unsafe fn InstallXLogFileSegment(
    segno: *mut XLogSegNo,
    tmppath: *mut c_char,
    find_free: bool,
    max_segno: XLogSegNo,
    tli: TimeLineID,
) -> bool {
    let mut path = [0u8; MAXPGPATH];
    let mut stat_buf: libc::stat = core::mem::zeroed();

    debug_assert_ne!(tli, 0);

    XLogFilePath(path.as_mut_ptr() as *mut c_char, tli, *segno, wal_segment_size);

    LWLockAcquire(ControlFileLock!(), LW_EXCLUSIVE);
    if !(*XLogCtl).InstallXLogFileSegmentActive {
        LWLockRelease(ControlFileLock!());
        return false;
    }

    if !find_free {
        /* Force installation: get rid of any pre-existing segment file */
        durable_unlink(path.as_ptr() as *const c_char, DEBUG1);
    } else {
        /* Find a free slot to put it in */
        while libc::stat(path.as_ptr() as *const c_char, &mut stat_buf) == 0 {
            if *segno >= max_segno {
                /* Failed to find a free slot within specified range */
                LWLockRelease(ControlFileLock!());
                return false;
            }
            *segno += 1;
            XLogFilePath(path.as_mut_ptr() as *mut c_char, tli, *segno, wal_segment_size);
        }
    }

    if durable_rename(tmppath, path.as_ptr() as *const c_char, LOG) != 0 {
        LWLockRelease(ControlFileLock!());
        /* durable_rename already emitted log message */
        return false;
    }

    LWLockRelease(ControlFileLock!());
    true
}

/*
 * Open a pre-existing logfile segment for writing.
 */
pub unsafe fn XLogFileOpen(segno: XLogSegNo, tli: TimeLineID) -> c_int {
    let mut path = [0u8; MAXPGPATH];

    XLogFilePath(path.as_mut_ptr() as *mut c_char, tli, segno, wal_segment_size);

    let fd = BasicOpenFile(
        path.as_mut_ptr() as *mut c_char,
        libc::O_RDWR | PG_BINARY | O_CLOEXEC | get_sync_bit(wal_sync_method),
    );
    if fd < 0 {
        ereport!(PANIC, errmsg!("could not open file: errno {}", *libc::__error()));
    }

    fd
}

/*
 * Close the current logfile segment for writing.
 */
unsafe fn XLogFileClose() {
    debug_assert!(openLogFile >= 0);

    /*
     * WAL segment files will not be re-read in normal operation, so we advise
     * the OS to release any cached pages.  On Darwin we skip posix_fadvise
     * (not available) -- no-op.
     */

    if close(openLogFile) != 0 {
        let mut xlogfname = [0u8; MAXFNAMELEN];
        let save_errno = *libc::__error();
        XLogFileName(xlogfname.as_mut_ptr() as *mut c_char, openLogTLI, openLogSegNo, wal_segment_size);
        ereport!(PANIC, errmsg!("could not close file: errno {}", save_errno));
    }

    openLogFile = -1;
    ReleaseExternalFD();
}

/*
 * Preallocate log files beyond the specified log endpoint.
 */
unsafe fn PreallocXlogFiles(endptr: XLogRecPtr, tli: TimeLineID) {
    let mut _logSegNo: XLogSegNo = 0;
    let lf: c_int;
    let mut added: bool = false;
    let mut path = [0u8; MAXPGPATH];
    let offset: uint64;

    if !(*XLogCtl).InstallXLogFileSegmentActive {
        return; /* unlocked check says no */
    }

    XLByteToPrevSeg(endptr, &mut _logSegNo, wal_segment_size);
    offset = XLogSegmentOffset(endptr - 1, wal_segment_size) as uint64;
    if offset >= (0.75 * wal_segment_size as f64) as uint64 {
        _logSegNo += 1;
        let lf = XLogFileInitInternal(_logSegNo, tli, &mut added, path.as_mut_ptr() as *mut c_char);
        if lf >= 0 {
            close(lf);
        }
        if added {
            CheckpointStats.ckpt_segs_added += 1;
        }
    }
}

// ---------------------------------------------------------------------------
// CheckXLogRemoved / XLogGetLastRemovedSegno / XLogGetOldestSegno
// ---------------------------------------------------------------------------

/*
 * Throws an error if the given log segment has already been removed or
 * recycled.
 */
pub unsafe fn CheckXLogRemoved(segno: XLogSegNo, tli: TimeLineID) {
    let save_errno = *libc::__error();
    let lastRemovedSegNo: XLogSegNo;

    SpinLockAcquire(&mut (*XLogCtl).info_lck);
    lastRemovedSegNo = (*XLogCtl).lastRemovedSegNo;
    SpinLockRelease(&mut (*XLogCtl).info_lck);

    if segno <= lastRemovedSegNo {
        let mut filename = [0u8; MAXFNAMELEN];
        XLogFileName(filename.as_mut_ptr() as *mut c_char, tli, segno, wal_segment_size);
        *libc::__error() = save_errno;
        ereport!(ERROR, errmsg!("requested WAL segment has already been removed"));
    }
    *libc::__error() = save_errno;
}

/*
 * Return the last WAL segment removed, or 0 if no segment has been removed
 * since startup.
 */
pub unsafe fn XLogGetLastRemovedSegno() -> XLogSegNo {
    let lastRemovedSegNo: XLogSegNo;

    SpinLockAcquire(&mut (*XLogCtl).info_lck);
    lastRemovedSegNo = (*XLogCtl).lastRemovedSegNo;
    SpinLockRelease(&mut (*XLogCtl).info_lck);

    lastRemovedSegNo
}

/*
 * Return the oldest WAL segment on the given TLI that still exists in
 * XLOGDIR, or 0 if none.
 */
pub unsafe fn XLogGetOldestSegno(tli: TimeLineID) -> XLogSegNo {
    let mut oldest_segno: XLogSegNo = 0;

    let xldir = AllocateDir(b"pg_wal\0".as_ptr() as *const c_char);
    loop {
        let xlde = ReadDir(xldir, b"pg_wal\0".as_ptr() as *const c_char);
        if xlde.is_null() {
            break;
        }
        let d_name = (*xlde).d_name.as_ptr();

        /* Ignore files that are not XLOG segments. */
        if !IsXLogFileName(d_name) {
            continue;
        }

        let mut file_tli: TimeLineID = 0;
        let mut file_segno: XLogSegNo = 0;
        XLogFromFileName(d_name, &mut file_tli, &mut file_segno, wal_segment_size);

        /* Ignore anything that's not from the TLI of interest. */
        if tli != file_tli {
            continue;
        }

        if oldest_segno == 0 || file_segno < oldest_segno {
            oldest_segno = file_segno;
        }
    }

    FreeDir(xldir);
    oldest_segno
}

/*
 * Update the last removed segno pointer in shared memory.
 */
unsafe fn UpdateLastRemovedPtr(filename: *const c_char) {
    let mut tli: uint32 = 0;
    let mut segno: XLogSegNo = 0;

    XLogFromFileName(filename, &mut tli, &mut segno, wal_segment_size);

    SpinLockAcquire(&mut (*XLogCtl).info_lck);
    if segno > (*XLogCtl).lastRemovedSegNo {
        (*XLogCtl).lastRemovedSegNo = segno;
    }
    SpinLockRelease(&mut (*XLogCtl).info_lck);
}

// ---------------------------------------------------------------------------
// RemoveTempXlogFiles / RemoveOldXlogFiles / RemoveNonParentXlogFiles / RemoveXlogFile
// ---------------------------------------------------------------------------

/*
 * Remove all temporary log files in pg_wal.
 */
unsafe fn RemoveTempXlogFiles() {
    elog!(DEBUG2, "removing all temporary WAL segments");

    let xldir = AllocateDir(b"pg_wal\0".as_ptr() as *const c_char);
    loop {
        let xlde = ReadDir(xldir, b"pg_wal\0".as_ptr() as *const c_char);
        if xlde.is_null() { break; }
        let d_name = (*xlde).d_name.as_ptr();

        /* Check for "xlogtemp." prefix */
        if libc::strncmp(d_name, b"xlogtemp.\0".as_ptr() as *const c_char, 9) != 0 {
            continue;
        }

        let mut path = [0u8; MAXPGPATH];
        libc::snprintf(
            path.as_mut_ptr() as *mut c_char,
            MAXPGPATH,
            b"pg_wal/%s\0".as_ptr() as *const c_char,
            d_name,
        );
        libc::unlink(path.as_ptr() as *const c_char);
    }
    FreeDir(xldir);
}

/*
 * Recycle or remove all log files older or equal to passed segno.
 */
unsafe fn RemoveOldXlogFiles(
    segno: XLogSegNo,
    lastredoptr: XLogRecPtr,
    endptr: XLogRecPtr,
    insertTLI: TimeLineID,
) {
    let mut endlogSegNo: XLogSegNo = 0;
    let recycleSegNo: XLogSegNo;
    let mut lastoff = [0u8; MAXFNAMELEN];

    XLByteToSeg(endptr, &mut endlogSegNo, wal_segment_size);
    recycleSegNo = XLOGfileslop(lastredoptr);

    XLogFileName(lastoff.as_mut_ptr() as *mut c_char, 0, segno, wal_segment_size);

    let xldir = AllocateDir(b"pg_wal\0".as_ptr() as *const c_char);

    loop {
        let xlde = ReadDir(xldir, b"pg_wal\0".as_ptr() as *const c_char);
        if xlde.is_null() { break; }
        let d_name = (*xlde).d_name.as_ptr();

        /* Ignore files that are not XLOG segments */
        if !IsXLogFileName(d_name) && !IsPartialXLogFileName(d_name) {
            continue;
        }

        /*
         * We use the alphanumeric sorting property of the filenames to decide
         * which ones are earlier than the lastoff segment.
         */
        if libc::strcmp(d_name.add(8), lastoff.as_ptr().add(8) as *const c_char) <= 0 {
            if XLogArchiveCheckDone(d_name) {
                /* Update the last removed location in shared memory first */
                UpdateLastRemovedPtr(d_name);
                RemoveXlogFile(xlde, recycleSegNo, &mut endlogSegNo, insertTLI);
            }
        }
    }

    FreeDir(xldir);
}

/*
 * Recycle or remove WAL files that are not part of the given timeline's
 * history.
 */
pub unsafe fn RemoveNonParentXlogFiles(switchpoint: XLogRecPtr, newTLI: TimeLineID) {
    let mut switchseg = [0u8; MAXFNAMELEN];
    let mut endLogSegNo: XLogSegNo = 0;
    let mut switchLogSegNo: XLogSegNo = 0;
    let recycleSegNo: XLogSegNo;

    XLByteToPrevSeg(switchpoint, &mut switchLogSegNo, wal_segment_size);
    XLByteToSeg(switchpoint, &mut endLogSegNo, wal_segment_size);
    recycleSegNo = endLogSegNo + 10;

    XLogFileName(switchseg.as_mut_ptr() as *mut c_char, newTLI, switchLogSegNo, wal_segment_size);

    let xldir = AllocateDir(b"pg_wal\0".as_ptr() as *const c_char);

    loop {
        let xlde = ReadDir(xldir, b"pg_wal\0".as_ptr() as *const c_char);
        if xlde.is_null() { break; }
        let d_name = (*xlde).d_name.as_ptr();

        /* Ignore files that are not XLOG segments */
        if !IsXLogFileName(d_name) {
            continue;
        }

        /*
         * Remove files that are on a timeline older than the new one we're
         * switching to, but with a segment number >= the first segment on the
         * new timeline.
         */
        if libc::strncmp(d_name, switchseg.as_ptr() as *const c_char, 8) < 0
            && libc::strcmp(d_name.add(8), switchseg.as_ptr().add(8) as *const c_char) > 0
        {
            if !XLogArchiveIsReady(d_name) {
                RemoveXlogFile(xlde, recycleSegNo, &mut endLogSegNo, newTLI);
            }
        }
    }

    FreeDir(xldir);
}

/*
 * Recycle or remove a log file that's no longer needed.
 */
unsafe fn RemoveXlogFile(
    segment_de: *const dirent,
    recycleSegNo: XLogSegNo,
    endlogSegNo: *mut XLogSegNo,
    insertTLI: TimeLineID,
) {
    let mut path = [0u8; MAXPGPATH];
    let segname = (*segment_de).d_name.as_ptr();

    libc::snprintf(
        path.as_mut_ptr() as *mut c_char,
        MAXPGPATH,
        b"pg_wal/%s\0".as_ptr() as *const c_char,
        segname,
    );

    /*
     * Before deleting the file, see if it can be recycled as a future log
     * segment.
     */
    if wal_recycle
        && *endlogSegNo <= recycleSegNo
        && (*XLogCtl).InstallXLogFileSegmentActive
        && get_dirent_type(path.as_ptr() as *const c_char, segment_de, false, DEBUG2) == PGFILETYPE_REG
        && InstallXLogFileSegment(endlogSegNo, path.as_mut_ptr() as *mut c_char, true, recycleSegNo, insertTLI)
    {
        CheckpointStats.ckpt_segs_recycled += 1;
        /* Needn't recheck that slot on future iterations */
        *endlogSegNo += 1;
    } else {
        /* No need for any more future segments, or recycling failed ... */
        let rc = durable_unlink(path.as_ptr() as *const c_char, LOG);
        if rc != 0 {
            /* Message already logged by durable_unlink() */
            return;
        }
        CheckpointStats.ckpt_segs_removed += 1;
    }

    XLogArchiveCleanup(segname);
}

// ---------------------------------------------------------------------------
// ValidateXLOGDirectoryStructure / CleanupBackupHistory
// ---------------------------------------------------------------------------

/*
 * Verify whether pg_wal, pg_wal/archive_status, and pg_wal/summaries exist.
 */
unsafe fn ValidateXLOGDirectoryStructure() {
    let mut path = [0u8; MAXPGPATH];
    let mut stat_buf: libc::stat = core::mem::zeroed();

    /* Check for pg_wal; if it doesn't exist, error out */
    if libc::stat(b"pg_wal\0".as_ptr() as *const c_char, &mut stat_buf) != 0
        || stat_buf.st_mode & libc::S_IFMT != libc::S_IFDIR
    {
        ereport!(FATAL, errmsg!("required WAL directory \"pg_wal\" does not exist"));
    }

    /* Check for archive_status */
    libc::snprintf(
        path.as_mut_ptr() as *mut c_char,
        MAXPGPATH,
        b"pg_wal/archive_status\0".as_ptr() as *const c_char,
    );
    if libc::stat(path.as_ptr() as *const c_char, &mut stat_buf) == 0 {
        if stat_buf.st_mode & libc::S_IFMT != libc::S_IFDIR {
            ereport!(FATAL, errmsg!("required WAL directory \"pg_wal/archive_status\" does not exist"));
        }
    } else {
        elog!(LOG, "creating missing WAL directory \"pg_wal/archive_status\"");
        if MakePGDirectory(path.as_ptr() as *const c_char) < 0 {
            ereport!(FATAL, errmsg!("could not create missing directory: errno {}", *libc::__error()));
        }
    }

    /* Check for summaries */
    libc::snprintf(
        path.as_mut_ptr() as *mut c_char,
        MAXPGPATH,
        b"pg_wal/summaries\0".as_ptr() as *const c_char,
    );
    if libc::stat(path.as_ptr() as *const c_char, &mut stat_buf) == 0 {
        if stat_buf.st_mode & libc::S_IFMT != libc::S_IFDIR {
            ereport!(FATAL, errmsg!("required WAL directory \"pg_wal/summaries\" does not exist"));
        }
    } else {
        elog!(LOG, "creating missing WAL directory \"pg_wal/summaries\"");
        if MakePGDirectory(path.as_ptr() as *const c_char) < 0 {
            ereport!(FATAL, errmsg!("could not create missing directory: errno {}", *libc::__error()));
        }
    }
}

/*
 * Remove previous backup history files.
 */
unsafe fn CleanupBackupHistory() {
    let mut path = [0u8; MAXPGPATH + 8 /* len("pg_wal/") */];

    let xldir = AllocateDir(b"pg_wal\0".as_ptr() as *const c_char);

    loop {
        let xlde = ReadDir(xldir, b"pg_wal\0".as_ptr() as *const c_char);
        if xlde.is_null() { break; }
        let d_name = (*xlde).d_name.as_ptr();

        if IsBackupHistoryFileName(d_name) {
            if XLogArchiveCheckDone(d_name) {
                libc::snprintf(
                    path.as_mut_ptr() as *mut c_char,
                    core::mem::size_of_val(&path),
                    b"pg_wal/%s\0".as_ptr() as *const c_char,
                    d_name,
                );
                libc::unlink(path.as_ptr() as *const c_char);
                XLogArchiveCleanup(d_name);
            }
        }
    }

    FreeDir(xldir);
}

// ---------------------------------------------------------------------------
// Control file I/O
// ---------------------------------------------------------------------------

/*
 * InitControlFile -- fill the ControlFile buffer with initial values.
 */
unsafe fn InitControlFile(sysidentifier: uint64, data_checksum_version: uint32) {
    let mut mock_auth_nonce = [0u8; MOCK_AUTH_NONCE_LEN];

    if !pg_strong_random(mock_auth_nonce.as_mut_ptr() as *mut c_void, MOCK_AUTH_NONCE_LEN) {
        ereport!(PANIC, errmsg!("could not generate secret authorization token"));
    }

    ptr::write_bytes(ControlFile as *mut u8, 0, core::mem::size_of::<ControlFileData>());
    /* Initialize pg_control status fields */
    (*ControlFile).system_identifier = sysidentifier;
    ptr::copy_nonoverlapping(
        mock_auth_nonce.as_ptr(),
        (*ControlFile).mock_authentication_nonce.as_mut_ptr() as *mut u8,
        MOCK_AUTH_NONCE_LEN,
    );
    (*ControlFile).state = DB_SHUTDOWNED;
    (*ControlFile).unloggedLSN = FirstNormalUnloggedLSN;

    /* Set important parameter values for use when replaying WAL */
    (*ControlFile).MaxConnections = MaxConnections;
    (*ControlFile).max_worker_processes = max_worker_processes;
    (*ControlFile).max_wal_senders = max_wal_senders;
    (*ControlFile).max_prepared_xacts = max_prepared_xacts;
    (*ControlFile).max_locks_per_xact = max_locks_per_xact;
    (*ControlFile).wal_level = wal_level;
    (*ControlFile).wal_log_hints = wal_log_hints;
    (*ControlFile).track_commit_timestamp = track_commit_timestamp;
    (*ControlFile).data_checksum_version = data_checksum_version;
}

/*
 * WriteControlFile -- initialize pg_control given a preloaded buffer.
 */
unsafe fn WriteControlFile() {
    let mut buffer = [0u8; PG_CONTROL_FILE_SIZE];

    /* Initialize version and compatibility-check fields */
    (*ControlFile).pg_control_version = PG_CONTROL_VERSION as uint32;
    (*ControlFile).catalog_version_no = CATALOG_VERSION_NO;

    (*ControlFile).maxAlign = MAXIMUM_ALIGNOF as uint32;
    (*ControlFile).floatFormat = FLOATFORMAT_VALUE;

    (*ControlFile).blcksz = BLCKSZ as uint32;
    (*ControlFile).relseg_size = RELSEG_SIZE as uint32;
    (*ControlFile).xlog_blcksz = XLOG_BLCKSZ as uint32;
    (*ControlFile).xlog_seg_size = wal_segment_size as uint32;

    (*ControlFile).nameDataLen = NAMEDATALEN as uint32;
    (*ControlFile).indexMaxKeys = INDEX_MAX_KEYS as uint32;

    (*ControlFile).toast_max_chunk_size = TOAST_MAX_CHUNK_SIZE as uint32;
    (*ControlFile).loblksize = LOBLKSIZE as uint32;

    (*ControlFile).float8ByVal = FLOAT8PASSBYVAL;

    /*
     * Initialize the default 'char' signedness.
     * Newly created database clusters unconditionally set to true.
     */
    (*ControlFile).default_char_signedness = true;

    /* Contents are protected with a CRC */
    INIT_CRC32C!((*ControlFile).crc);
    COMP_CRC32C!(
        (*ControlFile).crc,
        ControlFile as *const c_void,
        core::mem::offset_of!(ControlFileData, crc)
    );
    FIN_CRC32C!((*ControlFile).crc);

    /*
     * We write out PG_CONTROL_FILE_SIZE bytes into pg_control, zero-padding
     * the excess over sizeof(ControlFileData).
     */
    ptr::copy_nonoverlapping(
        ControlFile as *const u8,
        buffer.as_mut_ptr(),
        core::mem::size_of::<ControlFileData>(),
    );

    let fd = BasicOpenFile(
        c"global/pg_control".as_ptr() as *mut c_char, // XLOG_CONTROL_FILE, NUL-terminated
        libc::O_RDWR | libc::O_CREAT | libc::O_EXCL | PG_BINARY,
    );
    if fd < 0 {
        ereport!(PANIC, errmsg!("could not create file \"{}\": errno {}", "pg_control", *libc::__error()));
    }

    pgstat_report_wait_start(WAIT_EVENT_CONTROL_FILE_WRITE);
    if write(fd, buffer.as_ptr() as *const c_void, PG_CONTROL_FILE_SIZE) != PG_CONTROL_FILE_SIZE as isize {
        if *libc::__error() == 0 {
            *libc::__error() = libc::ENOSPC;
        }
        ereport!(PANIC, errmsg!("could not write to file \"{}\": errno {}", "pg_control", *libc::__error()));
    }
    pgstat_report_wait_end();

    pgstat_report_wait_start(WAIT_EVENT_CONTROL_FILE_SYNC);
    if pg_fsync(fd) != 0 {
        ereport!(PANIC, errmsg!("could not fsync file \"{}\": errno {}", "pg_control", *libc::__error()));
    }
    pgstat_report_wait_end();

    if close(fd) != 0 {
        ereport!(PANIC, errmsg!("could not close file \"{}\": errno {}", "pg_control", *libc::__error()));
    }
}

/*
 * ReadControlFile -- load the buffer from the pg_control file.
 */
unsafe fn ReadControlFile() {
    let mut crc: pg_crc32c = 0;
    let mut fd: c_int;
    let mut wal_segsz_str = [0u8; 20];

    fd = BasicOpenFile(
        c"global/pg_control".as_ptr() as *mut c_char, // XLOG_CONTROL_FILE, NUL-terminated
        libc::O_RDWR | PG_BINARY,
    );
    if fd < 0 {
        ereport!(PANIC, errmsg!("could not open file \"{}\": errno {}", "pg_control", *libc::__error()));
    }

    pgstat_report_wait_start(WAIT_EVENT_CONTROL_FILE_READ);
    let r = read(fd, ControlFile as *mut c_void, core::mem::size_of::<ControlFileData>());
    pgstat_report_wait_end();

    if r != core::mem::size_of::<ControlFileData>() as isize {
        if r < 0 {
            ereport!(PANIC, errmsg!("could not read file \"{}\": errno {}", "pg_control", *libc::__error()));
        } else {
            ereport!(PANIC, errmsg!("could not read file \"{}\": read {} of {}", "pg_control", r, core::mem::size_of::<ControlFileData>()));
        }
    }

    close(fd);

    /*
     * Check for expected pg_control format version.
     */
    if (*ControlFile).pg_control_version != PG_CONTROL_VERSION as uint32
        && (*ControlFile).pg_control_version % 65536 == 0
        && (*ControlFile).pg_control_version / 65536 != 0
    {
        ereport!(
            FATAL,
            errmsg!("database files are incompatible with server")
        );
    }

    if (*ControlFile).pg_control_version != PG_CONTROL_VERSION as uint32 {
        ereport!(
            FATAL,
            errmsg!("database files are incompatible with server")
        );
    }

    /* Now check the CRC. */
    INIT_CRC32C!(crc);
    COMP_CRC32C!(crc, ControlFile as *const c_void, core::mem::offset_of!(ControlFileData, crc));
    FIN_CRC32C!(crc);

    if !EQ_CRC32C!(crc, (*ControlFile).crc) {
        ereport!(FATAL, errmsg!("incorrect checksum in control file"));
    }

    /* Compatibility checking */
    if (*ControlFile).catalog_version_no != CATALOG_VERSION_NO {
        ereport!(FATAL, errmsg!("database files are incompatible with server") /* CATALOG_VERSION_NO mismatch */);
    }
    if (*ControlFile).maxAlign != MAXIMUM_ALIGNOF as uint32 {
        ereport!(FATAL, errmsg!("database files are incompatible with server") /* MAXALIGN mismatch */);
    }
    if (*ControlFile).floatFormat != FLOATFORMAT_VALUE {
        ereport!(FATAL, errmsg!("database files are incompatible with server") /* float format mismatch */);
    }
    if (*ControlFile).blcksz != BLCKSZ as uint32 {
        ereport!(FATAL, errmsg!("database files are incompatible with server") /* BLCKSZ mismatch */);
    }
    if (*ControlFile).relseg_size != RELSEG_SIZE as uint32 {
        ereport!(FATAL, errmsg!("database files are incompatible with server") /* RELSEG_SIZE mismatch */);
    }
    if (*ControlFile).xlog_blcksz != XLOG_BLCKSZ as uint32 {
        ereport!(FATAL, errmsg!("database files are incompatible with server") /* XLOG_BLCKSZ mismatch */);
    }
    if (*ControlFile).nameDataLen != NAMEDATALEN as uint32 {
        ereport!(FATAL, errmsg!("database files are incompatible with server") /* NAMEDATALEN mismatch */);
    }
    if (*ControlFile).indexMaxKeys != INDEX_MAX_KEYS as uint32 {
        ereport!(FATAL, errmsg!("database files are incompatible with server") /* INDEX_MAX_KEYS mismatch */);
    }
    if (*ControlFile).toast_max_chunk_size != TOAST_MAX_CHUNK_SIZE as uint32 {
        ereport!(FATAL, errmsg!("database files are incompatible with server") /* TOAST_MAX_CHUNK_SIZE mismatch */);
    }
    if (*ControlFile).loblksize != LOBLKSIZE as uint32 {
        ereport!(FATAL, errmsg!("database files are incompatible with server") /* LOBLKSIZE mismatch */);
    }
    if (*ControlFile).float8ByVal != FLOAT8PASSBYVAL {
        ereport!(FATAL, errmsg!("database files are incompatible with server") /* USE_FLOAT8_BYVAL mismatch */);
    }

    wal_segment_size = (*ControlFile).xlog_seg_size as c_int;

    if !IsValidWalSegSize(wal_segment_size) {
        ereport!(ERROR, errmsg!("invalid WAL segment size in control file ({} bytes)", wal_segment_size));
    }

    libc::snprintf(
        wal_segsz_str.as_mut_ptr() as *mut c_char,
        core::mem::size_of_val(&wal_segsz_str),
        b"%d\0".as_ptr() as *const c_char,
        wal_segment_size,
    );
    SetConfigOption(
        b"wal_segment_size\0".as_ptr() as *const c_char,
        wal_segsz_str.as_ptr() as *const c_char,
        PGC_INTERNAL,
        PGC_S_DYNAMIC_DEFAULT,
    );

    /* check and update variables dependent on wal_segment_size */
    if ConvertToXSegs(min_wal_size_mb, wal_segment_size) < 2 {
        ereport!(ERROR, errmsg!("\"min_wal_size\" must be at least twice \"wal_segment_size\""));
    }
    if ConvertToXSegs(max_wal_size_mb, wal_segment_size) < 2 {
        ereport!(ERROR, errmsg!("\"max_wal_size\" must be at least twice \"wal_segment_size\""));
    }

    UsableBytesInSegment = (wal_segment_size / XLOG_BLCKSZ as c_int * UsableBytesInPage as c_int)
        - (SizeOfXLogLongPHD as c_int - SizeOfXLogShortPHD as c_int);

    CalculateCheckpointSegments();

    /* Make the initdb settings visible as GUC variables, too */
    SetConfigOption(
        b"data_checksums\0".as_ptr() as *const c_char,
        if DataChecksumsEnabled() { b"yes\0".as_ptr() as *const c_char } else { b"no\0".as_ptr() as *const c_char },
        PGC_INTERNAL,
        PGC_S_DYNAMIC_DEFAULT,
    );
}

/*
 * UpdateControlFile -- utility wrapper to update the control file.
 */
unsafe fn UpdateControlFile() {
    update_controlfile(DataDir, ControlFile as *mut c_void, true);
}

// ---------------------------------------------------------------------------
// GetSystemIdentifier / GetMockAuthenticationNonce / DataChecksumsEnabled
// DataChecksumVersion / GetFakeLSN / str_time / get_sync_bit
// ---------------------------------------------------------------------------

/*
 * Returns the unique system identifier from control file.
 */
pub unsafe fn GetSystemIdentifier() -> uint64 {
    debug_assert!(!ControlFile.is_null());
    (*ControlFile).system_identifier
}



// ---------------------------------------------------------------------------
// str_time helper (used in XLogReportParameters and similar)
// ---------------------------------------------------------------------------

// TODO(pg-port): pg_strftime / log_timezone
unsafe fn pg_strftime(buf: *mut c_char, buflen: usize, fmt: *const c_char, tm: *const c_void) {}
static mut log_timezone: *const c_void = ptr::null();
unsafe fn pg_localtime(t: *const pg_time_t, tz: *const c_void) -> *const c_void { ptr::null() }


// ---------------------------------------------------------------------------
// NOTE: Translation ends here (C line ~4600, just before DataChecksumsEnabled
// at line 4601 per task instructions).
// Functions beyond this point (DataChecksumsEnabled, GetDefaultCharSignedness,
// GetFakeLSNForUnloggedRel, XLOGChooseNumBuffers, check_wal_buffers,
// check_wal_consistency_checking, assign_wal_consistency_checking,
// InitializeWalConsistencyChecking, show_archive_command, show_in_hot_standby,
// LocalProcessControlFile, GetActiveWalLevelOnStandby, XLOGShmemSize,
// XLOGShmemInit, BootStrapXLOG, XLogInitNewTimeline,
// CleanupAfterArchiveRecovery, ...) are in subsequent translation passes.
// ---------------------------------------------------------------------------
// section: xlog_tail -- C lines 4601-9568 (GetMockAuthenticationNonce .. SetWalWriterSleeping)

/* Returns the random nonce from control file. */
pub unsafe fn GetMockAuthenticationNonce() -> *mut c_char {
    assert!(!ControlFile.is_null());
    (*ControlFile).mock_authentication_nonce.as_mut_ptr()
}

/*
 * Are checksums enabled for data pages?
 */
pub unsafe fn DataChecksumsEnabled() -> bool {
    assert!(!ControlFile.is_null());
    (*ControlFile).data_checksum_version > 0
}

/*
 * Return true if the cluster was initialized on a platform where the
 * default signedness of char is "signed". This function exists for code
 * that deals with pre-v18 data files that store data sorted by the 'char'
 * type on disk (e.g., GIN and GiST indexes). See the comments in
 * WriteControlFile() for details.
 */
pub unsafe fn GetDefaultCharSignedness() -> bool {
    (*ControlFile).default_char_signedness
}

/*
 * Returns a fake LSN for unlogged relations.
 *
 * Each call generates an LSN that is greater than any previous value
 * returned. The current counter value is saved and restored across clean
 * shutdowns, but like unlogged relations, does not survive a crash. This can
 * be used in lieu of real LSN values returned by XLogInsert, if you need an
 * LSN-like increasing sequence of numbers without writing any WAL.
 */
pub unsafe fn GetFakeLSNForUnloggedRel() -> XLogRecPtr {
    pg_atomic_fetch_add_u64(&mut (*XLogCtl).unloggedLSN, 1)
}

/*
 * Auto-tune the number of XLOG buffers.
 *
 * The preferred setting for wal_buffers is about 3% of shared_buffers, with
 * a maximum of one XLOG segment (there is little reason to think that more
 * is helpful, at least so long as we force an fsync when switching log files)
 * and a minimum of 8 blocks (which was the default value prior to PostgreSQL
 * 9.1, when auto-tuning was added).
 *
 * This should not be called until NBuffers has received its final value.
 */
unsafe fn XLOGChooseNumBuffers() -> c_int {
    let mut xbuffers: c_int = NBuffers / 32;
    if xbuffers > (wal_segment_size / XLOG_BLCKSZ as c_int) {
        xbuffers = wal_segment_size / XLOG_BLCKSZ as c_int;
    }
    if xbuffers < 8 {
        xbuffers = 8;
    }
    xbuffers
}

/*
 * GUC check_hook for wal_buffers
 */
pub unsafe fn check_wal_buffers(newval: *mut c_int, _extra: *mut *mut c_void, _source: GucSource) -> bool {
    /*
     * -1 indicates a request for auto-tune.
     */
    if *newval == -1 {
        /*
         * If we haven't yet changed the boot_val default of -1, just let it
         * be.  We'll fix it when XLOGShmemSize is called.
         */
        if XLOGbuffers == -1 {
            return true;
        }
        /* Otherwise, substitute the auto-tune value */
        *newval = XLOGChooseNumBuffers();
    }

    /*
     * We clamp manually-set values to at least 4 blocks.  Prior to PostgreSQL
     * 9.1, a minimum of 4 was enforced by guc.c, but since that is no longer
     * the case, we just silently treat such values as a request for the
     * minimum.  (We could throw an error instead, but that doesn't seem very
     * helpful.)
     */
    if *newval < 4 {
        *newval = 4;
    }
    true
}

/*
 * GUC check_hook for wal_consistency_checking
 */
pub unsafe fn check_wal_consistency_checking(
    newval: *mut *mut c_char,
    extra: *mut *mut c_void,
    _source: GucSource,
) -> bool {
    let mut newwalconsistency: [bool; RM_MAX_ID as usize + 1] =
        [false; RM_MAX_ID as usize + 1];

    /* Need a modifiable copy of string */
    let rawstring = pstrdup(*newval);

    /* Parse string into list of identifiers */
    let mut elemlist: *mut List = ptr::null_mut();
    if !SplitIdentifierString(rawstring, b',' as c_char, &mut elemlist) {
        /* syntax error in list */
        GUC_check_errdetail!(b"List syntax is invalid.\0".as_ptr() as *const c_char);
        pfree(rawstring as *mut c_void);
        list_free(elemlist);
        return false;
    }

    let mut __i = 0;
    while __i < (*elemlist).length {
        let tok = (*(*elemlist).elements.add(__i as usize)).ptr_value as *mut c_char;
        __i += 1;

        /* Check for 'all'. */
        if pg_strcasecmp(tok, b"all\0".as_ptr() as *const c_char) == 0 {
            for rmid in 0..=RM_MAX_ID as usize {
                if RmgrIdExists(rmid as BuiltinRmgrId)
                    && !GetRmgr(rmid as BuiltinRmgrId).rm_mask.is_none()
                {
                    newwalconsistency[rmid] = true;
                }
            }
        } else {
            /* Check if the token matches any known resource manager. */
            let mut found = false;
            for rmid in 0..=RM_MAX_ID as usize {
                if RmgrIdExists(rmid as BuiltinRmgrId)
                    && !GetRmgr(rmid as BuiltinRmgrId).rm_mask.is_none()
                    && pg_strcasecmp(tok, GetRmgr(rmid as BuiltinRmgrId).rm_name) == 0
                {
                    newwalconsistency[rmid] = true;
                    found = true;
                    break;
                }
            }
            if !found {
                /*
                 * During startup, it might be a not-yet-loaded custom
                 * resource manager.  Defer checking until
                 * InitializeWalConsistencyChecking().
                 */
                if !process_shared_preload_libraries_done {
                    check_wal_consistency_checking_deferred = true;
                } else {
                    GUC_check_errdetail_fmt!(
                        b"Unrecognized key word: \"%s\".\0".as_ptr() as *const c_char,
                        tok,
                    );
                    pfree(rawstring as *mut c_void);
                    list_free(elemlist);
                    return false;
                }
            }
        }
    }

    pfree(rawstring as *mut c_void);
    list_free(elemlist);

    /* assign new value */
    *extra = guc_malloc(LOG, (RM_MAX_ID as usize + 1) * core::mem::size_of::<bool>());
    if (*extra).is_null() {
        return false;
    }
    ptr::copy_nonoverlapping(
        newwalconsistency.as_ptr(),
        *extra as *mut bool,
        RM_MAX_ID as usize + 1,
    );
    true
}

/*
 * GUC assign_hook for wal_consistency_checking
 */
pub unsafe fn assign_wal_consistency_checking(_newval: *const c_char, extra: *mut c_void) {
    /*
     * If some checks were deferred, it's possible that the checks will fail
     * later during InitializeWalConsistencyChecking(). But in that case, the
     * postmaster will exit anyway, so it's safe to proceed with the
     * assignment.
     *
     * Any built-in resource managers specified are assigned immediately,
     * which affects WAL created before shared_preload_libraries are
     * processed. Any custom resource managers specified won't be assigned
     * until after shared_preload_libraries are processed, but that's OK
     * because WAL for a custom resource manager can't be written before the
     * module is loaded anyway.
     */
    wal_consistency_checking = extra as *mut bool;
}

/*
 * InitializeWalConsistencyChecking: run after loading custom resource managers
 *
 * If any unknown resource managers were specified in the
 * wal_consistency_checking GUC, processing was deferred.  Now that
 * shared_preload_libraries have been loaded, process wal_consistency_checking
 * again.
 */
pub unsafe fn InitializeWalConsistencyChecking() {
    assert!(process_shared_preload_libraries_done);

    if check_wal_consistency_checking_deferred {
        let guc = find_option(
            b"wal_consistency_checking\0".as_ptr() as *const c_char,
            false,
            false,
            ERROR,
        );

        check_wal_consistency_checking_deferred = false;

        set_config_option_ext(
            b"wal_consistency_checking\0".as_ptr() as *const c_char,
            wal_consistency_checking_string,
            (*guc).scontext,
            (*guc).source,
            (*guc).srole,
            crate::utils::misc::guc::GucAction::GUC_ACTION_SET,
            true,
            ERROR,
            false,
        );

        /* checking should not be deferred again */
        assert!(!check_wal_consistency_checking_deferred);
    }
}

/*
 * GUC show_hook for archive_command
 */
pub unsafe fn show_archive_command() -> *const c_char {
    if XLogArchivingActive() {
        XLogArchiveCommand
    } else {
        b"(disabled)\0".as_ptr() as *const c_char
    }
}

/*
 * GUC show_hook for in_hot_standby
 */
pub unsafe fn show_in_hot_standby() -> *const c_char {
    /*
     * We display the actual state based on shared memory, so that this GUC
     * reports up-to-date state if examined intra-query.  The underlying
     * variable (in_hot_standby_guc) changes only when we transmit a new value
     * to the client.
     */
    if RecoveryInProgress() {
        b"on\0".as_ptr() as *const c_char
    } else {
        b"off\0".as_ptr() as *const c_char
    }
}

/*
 * Read the control file, set respective GUCs.
 *
 * This is to be called during startup, including a crash recovery cycle,
 * unless in bootstrap mode, where no control file yet exists.  As there's no
 * usable shared memory yet (its sizing can depend on the contents of the
 * control file!), first store the contents in local memory. XLOGShmemInit()
 * will then copy it to shared memory later.
 *
 * reset just controls whether previous contents are to be expected (in the
 * reset case, there's a dangling pointer into old shared memory), or not.
 */
pub unsafe fn LocalProcessControlFile(reset: bool) {
    assert!(reset || ControlFile.is_null());
    ControlFile = palloc(core::mem::size_of::<ControlFileData>()) as *mut ControlFileData;
    ReadControlFile();
}

/*
 * Get the wal_level from the control file. For a standby, this value should be
 * considered as its active wal_level, because it may be different from what
 * was originally configured on standby.
 */
pub unsafe fn GetActiveWalLevelOnStandby() -> WalLevel {
    (*ControlFile).wal_level
}

/*
 * Initialization of shared memory for XLOG
 */
pub unsafe fn XLOGShmemSize() -> Size {
    let mut size: Size;

    /*
     * If the value of wal_buffers is -1, use the preferred auto-tune value.
     * This isn't an amazingly clean place to do this, but we must wait till
     * NBuffers has received its final value, and must do it before using the
     * value of XLOGbuffers to do anything important.
     *
     * We prefer to report this value's source as PGC_S_DYNAMIC_DEFAULT.
     * However, if the DBA explicitly set wal_buffers = -1 in the config file,
     * then PGC_S_DYNAMIC_DEFAULT will fail to override that and we must force
     * the matter with PGC_S_OVERRIDE.
     */
    if XLOGbuffers == -1 {
        let mut buf = [0u8; 32];
        let s = format!("{}", XLOGChooseNumBuffers());
        let bytes = s.as_bytes();
        let len = bytes.len().min(31);
        buf[..len].copy_from_slice(&bytes[..len]);
        SetConfigOption(
            b"wal_buffers\0".as_ptr() as *const c_char,
            buf.as_ptr() as *const c_char,
            PGC_POSTMASTER,
            PGC_S_DYNAMIC_DEFAULT,
        );
        if XLOGbuffers == -1 {
            /* failed to apply it? */
            SetConfigOption(
                b"wal_buffers\0".as_ptr() as *const c_char,
                buf.as_ptr() as *const c_char,
                PGC_POSTMASTER,
                PGC_S_OVERRIDE,
            );
        }
    }
    // bring-up: SetConfigOption is a benign shim that doesn't write back the GUC var yet,
    // so apply the auto-computed value directly. TODO: real GUC assign-hook propagation.
    if XLOGbuffers <= 0 {
        XLOGbuffers = XLOGChooseNumBuffers();
    }
    assert!(XLOGbuffers > 0);

    /* XLogCtl */
    size = core::mem::size_of::<XLogCtlData>();

    /* WAL insertion locks, plus alignment */
    size = add_size(
        size,
        mul_size(
            core::mem::size_of::<WALInsertLockPadded>(),
            NUM_XLOGINSERT_LOCKS as usize + 1,
        ),
    );
    /* xlblocks array */
    size = add_size(
        size,
        mul_size(
            core::mem::size_of::<pg_atomic_uint64>(),
            XLOGbuffers as usize,
        ),
    );
    /* extra alignment padding for XLOG I/O buffers */
    size = add_size(size, XLOG_BLCKSZ.max(PG_IO_ALIGN_SIZE));
    /* and the buffers themselves */
    size = add_size(size, mul_size(XLOG_BLCKSZ, XLOGbuffers as usize));

    /*
     * Note: we don't count ControlFileData, it comes out of the "slop factor"
     * added by CreateSharedMemoryAndSemaphores.  This lets us use this
     * routine again below to compute the actual allocation size.
     */
    size
}

pub unsafe fn XLOGShmemInit() {
    let mut foundCFile: bool = false;
    let mut foundXLog: bool = false;
    let mut allocptr: *mut c_char;
    let mut i: c_int;
    let localControlFile: *mut ControlFileData;

    #[cfg(feature = "wal_debug")]
    {
        /*
         * Create a memory context for WAL debugging that's exempt from the normal
         * "no pallocs in critical section" rule. Yes, that can lead to a PANIC if
         * an allocation fails, but wal_debug is not for production use anyway.
         */
        if walDebugCxt.is_null() {
            walDebugCxt = AllocSetContextCreate(
                TopMemoryContext,
                b"WAL Debug\0".as_ptr() as *const c_char,
                ALLOCSET_DEFAULT_SIZES,
            );
            MemoryContextAllowInCriticalSection(walDebugCxt, true);
        }
    }

    XLogCtl = ShmemInitStruct(
        b"XLOG Ctl\0".as_ptr() as *const c_char,
        XLOGShmemSize(),
        &mut foundXLog,
    ) as *mut XLogCtlData;

    localControlFile = ControlFile;
    ControlFile = ShmemInitStruct(
        b"Control File\0".as_ptr() as *const c_char,
        core::mem::size_of::<ControlFileData>(),
        &mut foundCFile,
    ) as *mut ControlFileData;

    if foundCFile || foundXLog {
        /* both should be present or neither */
        assert!(foundCFile && foundXLog);

        /* Initialize local copy of WALInsertLocks */
        WALInsertLocks = (*XLogCtl).Insert.WALInsertLocks;

        if !localControlFile.is_null() {
            pfree(localControlFile as *mut c_void);
        }
        return;
    }
    ptr::write_bytes(XLogCtl as *mut u8, 0, core::mem::size_of::<XLogCtlData>());

    /*
     * Already have read control file locally, unless in bootstrap mode. Move
     * contents into shared memory.
     */
    if !localControlFile.is_null() {
        ptr::copy_nonoverlapping(
            localControlFile,
            ControlFile,
            1,
        );
        pfree(localControlFile as *mut c_void);
    }

    /*
     * Since XLogCtlData contains XLogRecPtr fields, its sizeof should be a
     * multiple of the alignment for same, so no extra alignment padding is
     * needed here.
     */
    allocptr = (XLogCtl as *mut c_char).add(core::mem::size_of::<XLogCtlData>());
    (*XLogCtl).xlblocks = allocptr as *mut pg_atomic_uint64;
    allocptr = allocptr.add(core::mem::size_of::<pg_atomic_uint64>() * XLOGbuffers as usize);

    i = 0;
    while i < XLOGbuffers {
        pg_atomic_init_u64(
            &mut *(*XLogCtl).xlblocks.add(i as usize),
            InvalidXLogRecPtr,
        );
        i += 1;
    }

    /* WAL insertion locks. Ensure they're aligned to the full padded size */
    let align = core::mem::size_of::<WALInsertLockPadded>();
    let offset = (allocptr as usize) % align;
    if offset != 0 {
        allocptr = allocptr.add(align - offset);
    }
    WALInsertLocks = allocptr as *mut WALInsertLockPadded;
    (*XLogCtl).Insert.WALInsertLocks = WALInsertLocks;
    allocptr = allocptr.add(core::mem::size_of::<WALInsertLockPadded>() * NUM_XLOGINSERT_LOCKS as usize);

    i = 0;
    while i < NUM_XLOGINSERT_LOCKS as c_int {
        LWLockInitialize(
            &mut core::mem::ManuallyDrop::deref_mut(&mut (*WALInsertLocks.add(i as usize)).l).lock as *mut _ as *mut crate::storage::lmgr::lwlock::LWLock,
            LWTRANCHE_WAL_INSERT as c_int,
        );
        pg_atomic_init_u64(
            &mut core::mem::ManuallyDrop::deref_mut(&mut (*WALInsertLocks.add(i as usize)).l).insertingAt,
            InvalidXLogRecPtr,
        );
        core::mem::ManuallyDrop::deref_mut(&mut (*WALInsertLocks.add(i as usize)).l).lastImportantAt = InvalidXLogRecPtr;
        i += 1;
    }

    /*
     * Align the start of the page buffers to a full xlog block size boundary.
     * This simplifies some calculations in XLOG insertion. It is also
     * required for O_DIRECT.
     */
    let blksz = XLOG_BLCKSZ;
    let rem = (allocptr as usize) % blksz;
    if rem != 0 {
        allocptr = allocptr.add(blksz - rem);
    }
    (*XLogCtl).pages = allocptr;
    ptr::write_bytes((*XLogCtl).pages as *mut u8, 0, blksz * XLOGbuffers as usize);

    /*
     * Do basic initialization of XLogCtl shared data. (StartupXLOG will fill
     * in additional info.)
     */
    (*XLogCtl).XLogCacheBlck = XLOGbuffers - 1;
    (*XLogCtl).SharedRecoveryState = RECOVERY_STATE_CRASH;
    (*XLogCtl).InstallXLogFileSegmentActive = false;
    (*XLogCtl).WalWriterSleeping = false;

    SpinLockInit(&mut (*XLogCtl).Insert.insertpos_lck);
    SpinLockInit(&mut (*XLogCtl).info_lck);
    pg_atomic_init_u64(&mut (*XLogCtl).logInsertResult, InvalidXLogRecPtr);
    pg_atomic_init_u64(&mut (*XLogCtl).logWriteResult, InvalidXLogRecPtr);
    pg_atomic_init_u64(&mut (*XLogCtl).logFlushResult, InvalidXLogRecPtr);
    pg_atomic_init_u64(&mut (*XLogCtl).unloggedLSN, InvalidXLogRecPtr);
}

/*
 * This func must be called ONCE on system install.  It creates pg_control
 * and the initial XLOG segment.
 */
pub unsafe fn BootStrapXLOG(data_checksum_version: uint32) {
    let mut checkPoint: CheckPoint = core::mem::zeroed();
    let buffer: *mut c_char;
    let page: XLogPageHeader;
    let longpage: *mut XLogLongPageHeaderData;
    let record: *mut XLogRecord;
    let mut recptr: *mut c_char;
    let sysidentifier: uint64;
    let mut tv: libc::timeval = core::mem::zeroed();
    let mut crc: pg_crc32c = 0;

    /* allow ordinary WAL segment creation, like StartupXLOG() would */
    SetInstallXLogFileSegmentActive();

    /*
     * Select a hopefully-unique system identifier code for this installation.
     * We use the result of gettimeofday(), including the fractional seconds
     * field, as being about as unique as we can easily get.  (Think not to
     * use random(), since it hasn't been seeded and there's no portable way
     * to seed it other than the system clock value...)  The upper half of the
     * uint64 value is just the tv_sec part, while the lower half contains the
     * tv_usec part (which must fit in 20 bits), plus 12 bits from our current
     * PID for a little extra uniqueness.  A person knowing this encoding can
     * determine the initialization time of the installation, which could
     * perhaps be useful sometimes.
     */
    libc::gettimeofday(&mut tv, ptr::null_mut());
    sysidentifier = ((tv.tv_sec as uint64) << 32)
        | ((tv.tv_usec as uint64) << 12)
        | (libc::getpid() as uint64 & 0xFFF);

    /* page buffer must be aligned suitably for O_DIRECT */
    buffer = palloc(XLOG_BLCKSZ + XLOG_BLCKSZ) as *mut c_char;
    let aligned = ((buffer as usize + XLOG_BLCKSZ - 1) & !(XLOG_BLCKSZ - 1)) as *mut c_char;
    page = aligned as XLogPageHeader;
    ptr::write_bytes(page as *mut u8, 0, XLOG_BLCKSZ);

    /*
     * Set up information for the initial checkpoint record
     *
     * The initial checkpoint record is written to the beginning of the WAL
     * segment with logid=0 logseg=1. The very first WAL segment, 0/0, is not
     * used, so that we can use 0/0 to mean "before any valid WAL segment".
     */
    checkPoint.redo = wal_segment_size as XLogRecPtr + SizeOfXLogLongPHD as XLogRecPtr;
    checkPoint.ThisTimeLineID = BootstrapTimeLineID;
    checkPoint.PrevTimeLineID = BootstrapTimeLineID;
    checkPoint.fullPageWrites = fullPageWrites;
    checkPoint.wal_level = wal_level;
    checkPoint.nextXid =
        FullTransactionIdFromEpochAndXid(0, FirstNormalTransactionId);
    checkPoint.nextOid = FirstGenbkiObjectId;
    checkPoint.nextMulti = FirstMultiXactId;
    checkPoint.nextMultiOffset = 0;
    checkPoint.oldestXid = FirstNormalTransactionId;
    checkPoint.oldestXidDB = Template1DbOid;
    checkPoint.oldestMulti = FirstMultiXactId;
    checkPoint.oldestMultiDB = Template1DbOid;
    checkPoint.oldestCommitTsXid = InvalidTransactionId;
    checkPoint.newestCommitTsXid = InvalidTransactionId;
    checkPoint.time = libc::time(ptr::null_mut()) as pg_time_t;
    checkPoint.oldestActiveXid = InvalidTransactionId;

    (*TransamVariables).nextXid = checkPoint.nextXid;
    (*TransamVariables).nextOid = checkPoint.nextOid;
    (*TransamVariables).oidCount = 0;
    MultiXactSetNextMXact(checkPoint.nextMulti, checkPoint.nextMultiOffset);
    AdvanceOldestClogXid(checkPoint.oldestXid);
    SetTransactionIdLimit(checkPoint.oldestXid, checkPoint.oldestXidDB);
    SetMultiXactIdLimit(checkPoint.oldestMulti, checkPoint.oldestMultiDB, true);
    SetCommitTsLimit(InvalidTransactionId, InvalidTransactionId);

    /* Set up the XLOG page header */
    (*page).xlp_magic = XLOG_PAGE_MAGIC;
    (*page).xlp_info = XLP_LONG_HEADER;
    (*page).xlp_tli = BootstrapTimeLineID;
    (*page).xlp_pageaddr = wal_segment_size as XLogRecPtr;
    longpage = page as *mut XLogLongPageHeaderData;
    (*longpage).xlp_sysid = sysidentifier;
    (*longpage).xlp_seg_size = wal_segment_size as uint32;
    (*longpage).xlp_xlog_blcksz = XLOG_BLCKSZ as uint32;

    /* Insert the initial checkpoint record */
    recptr = (page as *mut c_char).add(SizeOfXLogLongPHD);
    record = recptr as *mut XLogRecord;
    (*record).xl_prev = 0;
    (*record).xl_xid = InvalidTransactionId;
    (*record).xl_tot_len = (SizeOfXLogRecord()
        + SizeOfXLogRecordDataHeaderShort
        + core::mem::size_of::<CheckPoint>()) as uint32;
    (*record).xl_info = XLOG_CHECKPOINT_SHUTDOWN;
    (*record).xl_rmid = RM_XLOG_ID;
    recptr = recptr.add(SizeOfXLogRecord());
    /* fill the XLogRecordDataHeaderShort struct */
    *recptr = XLR_BLOCK_ID_DATA_SHORT as c_char;
    recptr = recptr.add(1);
    *recptr = core::mem::size_of::<CheckPoint>() as c_char;
    recptr = recptr.add(1);
    ptr::copy_nonoverlapping(
        &checkPoint as *const CheckPoint as *const u8,
        recptr as *mut u8,
        core::mem::size_of::<CheckPoint>(),
    );
    recptr = recptr.add(core::mem::size_of::<CheckPoint>());
    debug_assert_eq!(
        recptr as usize - record as usize,
        (*record).xl_tot_len as usize
    );

    INIT_CRC32C!(crc);
    COMP_CRC32C!(
        crc,
        (record as *const c_char).add(SizeOfXLogRecord()),
        (*record).xl_tot_len as usize - SizeOfXLogRecord()
    );
    COMP_CRC32C!(crc, record as *const c_char, XLogRecord_crc_offset());
    FIN_CRC32C!(crc);
    (*record).xl_crc = crc;

    /* Create first XLOG segment file */
    openLogTLI = BootstrapTimeLineID;
    openLogFile = XLogFileInit(1, BootstrapTimeLineID);

    /*
     * We needn't bother with Reserve/ReleaseExternalFD here, since we'll
     * close the file again in a moment.
     */

    /* Write the first page with the initial record */
    *libc::__error() = 0;
    pgstat_report_wait_start(WAIT_EVENT_WAL_BOOTSTRAP_WRITE);
    if libc::write(openLogFile, page as *const c_void, XLOG_BLCKSZ) != XLOG_BLCKSZ as isize {
        /* if write didn't set errno, assume problem is no disk space */
        if *libc::__error() == 0 {
            *libc::__error() = libc::ENOSPC;
        }
        ereport!(PANIC, errmsg!("could not write bootstrap write-ahead log file: {}", strerror_r()));
        /* errcode_for_file_access */
    }
    pgstat_report_wait_end();

    pgstat_report_wait_start(WAIT_EVENT_WAL_BOOTSTRAP_SYNC);
    if pg_fsync(openLogFile) != 0 {
        ereport!(PANIC, errmsg!("could not fsync bootstrap write-ahead log file: {}", strerror_r()));
        /* errcode_for_file_access */
    }
    pgstat_report_wait_end();

    if libc::close(openLogFile) != 0 {
        ereport!(PANIC, errmsg!("could not close bootstrap write-ahead log file: {}", strerror_r()));
        /* errcode_for_file_access */
    }
    openLogFile = -1;

    /* Now create pg_control */
    InitControlFile(sysidentifier, data_checksum_version);
    (*ControlFile).time = checkPoint.time;
    (*ControlFile).checkPoint = checkPoint.redo;
    (*ControlFile).checkPointCopy = checkPoint;

    /* some additional ControlFile fields are set in WriteControlFile() */
    WriteControlFile();

    /* Bootstrap the commit log, too */
    BootStrapCLOG();
    BootStrapCommitTs();
    BootStrapSUBTRANS();
    BootStrapMultiXact();

    pfree(buffer as *mut c_void);

    /*
     * Force control file to be read - in contrast to normal processing we'd
     * otherwise never run the checks and GUC related initializations therein.
     */
    ReadControlFile();
}

unsafe fn str_time(tnow: pg_time_t) -> *mut c_char {
    let buf = palloc(128) as *mut c_char;
    pg_strftime(
        buf,
        128,
        b"%Y-%m-%d %H:%M:%S %Z\0".as_ptr() as *const c_char,
        pg_localtime(&tnow, log_timezone),
    );
    buf
}

/*
 * Initialize the first WAL segment on new timeline.
 */
unsafe fn XLogInitNewTimeline(endTLI: TimeLineID, endOfLog: XLogRecPtr, newTLI: TimeLineID) {
    let mut xlogfname: [c_char; MAXFNAMELEN] = [0; MAXFNAMELEN];
    let mut endLogSegNo: XLogSegNo = 0;
    let mut startLogSegNo: XLogSegNo = 0;

    /* we always switch to a new timeline after archive recovery */
    assert!(endTLI != newTLI);

    /*
     * Update min recovery point one last time.
     */
    UpdateMinRecoveryPoint(InvalidXLogRecPtr, true);

    /*
     * Calculate the last segment on the old timeline, and the first segment
     * on the new timeline. If the switch happens in the middle of a segment,
     * they are the same, but if the switch happens exactly at a segment
     * boundary, startLogSegNo will be endLogSegNo + 1.
     */
    XLByteToPrevSeg(endOfLog, &mut endLogSegNo, wal_segment_size);
    XLByteToSeg(endOfLog, &mut startLogSegNo, wal_segment_size);

    /*
     * Initialize the starting WAL segment for the new timeline. If the switch
     * happens in the middle of a segment, copy data from the last WAL segment
     * of the old timeline up to the switch point, to the starting WAL segment
     * on the new timeline.
     */
    if endLogSegNo == startLogSegNo {
        /*
         * Make a copy of the file on the new timeline.
         *
         * Writing WAL isn't allowed yet, so there are no locking
         * considerations. But we should be just as tense as XLogFileInit to
         * avoid emplacing a bogus file.
         */
        XLogFileCopy(
            newTLI,
            endLogSegNo,
            endTLI,
            endLogSegNo,
            XLogSegmentOffset(endOfLog, wal_segment_size) as c_int,
        );
    } else {
        /*
         * The switch happened at a segment boundary, so just create the next
         * segment on the new timeline.
         */
        let fd = XLogFileInit(startLogSegNo, newTLI);

        if libc::close(fd) != 0 {
            let save_errno = *libc::__error();
            XLogFileName(
                xlogfname.as_mut_ptr(),
                newTLI,
                startLogSegNo,
                wal_segment_size,
            );
            *libc::__error() = save_errno;
            ereport!(
                ERROR,
                errmsg!(
                    "could not close file \"{}\": {}",
                    core::ffi::CStr::from_ptr(xlogfname.as_ptr()).to_string_lossy(),
                    strerror_r()
                )
            );
            /* errcode_for_file_access */
        }
    }

    /*
     * Let's just make real sure there are not .ready or .done flags posted
     * for the new segment.
     */
    XLogFileName(
        xlogfname.as_mut_ptr(),
        newTLI,
        startLogSegNo,
        wal_segment_size,
    );
    XLogArchiveCleanup(xlogfname.as_ptr());
}

/*
 * Perform cleanup actions at the conclusion of archive recovery.
 */
unsafe fn CleanupAfterArchiveRecovery(
    EndOfLogTLI: TimeLineID,
    EndOfLog: XLogRecPtr,
    newTLI: TimeLineID,
) {
    /*
     * Execute the recovery_end_command, if any.
     */
    if !recoveryEndCommand.is_null()
        && libc::strcmp(recoveryEndCommand, b"\0".as_ptr() as *const c_char) != 0
    {
        ExecuteRecoveryCommand(
            recoveryEndCommand,
            b"recovery_end_command\0".as_ptr() as *const c_char,
            true,
            WAIT_EVENT_RECOVERY_END_COMMAND,
        );
    }

    /*
     * We switched to a new timeline. Clean up segments on the old timeline.
     *
     * If there are any higher-numbered segments on the old timeline, remove
     * them. They might contain valid WAL, but they might also be
     * pre-allocated files containing garbage. In any case, they are not part
     * of the new timeline's history so we don't need them.
     */
    RemoveNonParentXlogFiles(EndOfLog, newTLI);

    /*
     * If the switch happened in the middle of a segment, what to do with the
     * last, partial segment on the old timeline? ... (see C comment)
     * As a compromise, we rename the last segment with the .partial suffix,
     * and archive it.
     */
    if XLogSegmentOffset(EndOfLog, wal_segment_size) != 0
        && XLogArchivingActive()
    {
        let mut origfname: [c_char; MAXFNAMELEN] = [0; MAXFNAMELEN];
        let mut endLogSegNo: XLogSegNo = 0;

        XLByteToPrevSeg(EndOfLog, &mut endLogSegNo, wal_segment_size);
        XLogFileName(
            origfname.as_mut_ptr(),
            EndOfLogTLI,
            endLogSegNo,
            wal_segment_size,
        );

        if !XLogArchiveIsReadyOrDone(origfname.as_ptr()) {
            let mut origpath: [c_char; MAXPGPATH] = [0; MAXPGPATH];
            let mut partialfname: [c_char; MAXFNAMELEN] = [0; MAXFNAMELEN];
            let mut partialpath: [c_char; MAXPGPATH] = [0; MAXPGPATH];

            /*
             * If we're summarizing WAL, we can't rename the partial file
             * until the summarizer finishes with it, else it will fail.
             */
            if summarize_wal {
                WaitForWalSummarization(EndOfLog);
            }

            XLogFilePath(
                origpath.as_mut_ptr(),
                EndOfLogTLI,
                endLogSegNo,
                wal_segment_size,
            );
            libc::snprintf(
                partialfname.as_mut_ptr(),
                MAXFNAMELEN,
                b"%s.partial\0".as_ptr() as *const c_char,
                origfname.as_ptr(),
            );
            libc::snprintf(
                partialpath.as_mut_ptr(),
                MAXPGPATH,
                b"%s.partial\0".as_ptr() as *const c_char,
                origpath.as_ptr(),
            );

            /*
             * Make sure there's no .done or .ready file for the .partial
             * file.
             */
            XLogArchiveCleanup(partialfname.as_ptr());

            durable_rename(origpath.as_ptr(), partialpath.as_ptr(), ERROR);
            XLogArchiveNotify(partialfname.as_ptr());
        }
    }
}

/*
 * Check to see if required parameters are set high enough on this server
 * for various aspects of recovery operation.
 */
unsafe fn CheckRequiredParameterValues() {
    /*
     * For archive recovery, the WAL must be generated with at least 'replica'
     * wal_level.
     */
    if ArchiveRecoveryRequested && (*ControlFile).wal_level == WAL_LEVEL_MINIMAL {
        ereport!(
            FATAL,
            errmsg!("WAL was generated with \"wal_level=minimal\", cannot continue recovering")
        );
    }

    /*
     * For Hot Standby, the WAL must be generated with 'replica' mode, and we
     * must have at least as many backend slots as the primary.
     */
    if ArchiveRecoveryRequested && EnableHotStandby {
        /* We ignore autovacuum_worker_slots when we make this test. */
        RecoveryRequiresIntParameter(
            b"max_connections\0".as_ptr() as *const c_char,
            MaxConnections,
            (*ControlFile).MaxConnections,
        );
        RecoveryRequiresIntParameter(
            b"max_worker_processes\0".as_ptr() as *const c_char,
            max_worker_processes,
            (*ControlFile).max_worker_processes,
        );
        RecoveryRequiresIntParameter(
            b"max_wal_senders\0".as_ptr() as *const c_char,
            max_wal_senders,
            (*ControlFile).max_wal_senders,
        );
        RecoveryRequiresIntParameter(
            b"max_prepared_transactions\0".as_ptr() as *const c_char,
            max_prepared_xacts,
            (*ControlFile).max_prepared_xacts,
        );
        RecoveryRequiresIntParameter(
            b"max_locks_per_transaction\0".as_ptr() as *const c_char,
            max_locks_per_xact,
            (*ControlFile).max_locks_per_xact,
        );
    }
}

/*
 * This must be called ONCE during postmaster or standalone-backend startup
 */
pub unsafe fn StartupXLOG() {
    let Insert: *mut XLogCtlInsert;
    let mut checkPoint: CheckPoint = core::mem::zeroed();
    let mut wasShutdown: bool = false;
    let mut didCrash: bool;
    let mut haveTblspcMap: bool = false;
    let mut haveBackupLabel: bool = false;
    let mut EndOfLog: XLogRecPtr = 0;
    let mut EndOfLogTLI: TimeLineID = 0;
    let mut newTLI: TimeLineID;
    let mut performedWalRecovery: bool;
    let mut endOfRecoveryInfo: *mut EndOfWalRecoveryInfo;
    let mut abortedRecPtr: XLogRecPtr = InvalidXLogRecPtr;
    let mut missingContrecPtr: XLogRecPtr = InvalidXLogRecPtr;
    let mut oldestActiveXID: TransactionId = InvalidTransactionId;
    let mut promoted: bool = false;

    /*
     * We should have an aux process resource owner to use, and we should not
     * be in a transaction that's installed some other resowner.
     */
    assert!(!AuxProcessResourceOwner.is_null());
    assert!(
        CurrentResourceOwner.is_null()
            || CurrentResourceOwner == AuxProcessResourceOwner
    );
    CurrentResourceOwner = AuxProcessResourceOwner;

    /*
     * Check that contents look valid.
     */
    if !XRecOffIsValid((*ControlFile).checkPoint) {
        ereport!(FATAL, errmsg!("control file contains invalid checkpoint location"));
        /* errcode(ERRCODE_DATA_CORRUPTED) */
    }

    match (*ControlFile).state {
        DB_SHUTDOWNED => {
            /*
             * This is the expected case, so don't be chatty in standalone mode
             */
            ereport!(
                if IsPostmasterEnvironment { LOG } else { NOTICE },
                errmsg!("database system was shut down at {}", cstr_to_str(str_time((*ControlFile).time)))
            );
        }
        DB_SHUTDOWNED_IN_RECOVERY => {
            ereport!(
                LOG,
                errmsg!("database system was shut down in recovery at {}", cstr_to_str(str_time((*ControlFile).time)))
            );
        }
        DB_SHUTDOWNING => {
            ereport!(
                LOG,
                errmsg!("database system shutdown was interrupted; last known up at {}", cstr_to_str(str_time((*ControlFile).time)))
            );
        }
        DB_IN_CRASH_RECOVERY => {
            ereport!(
                LOG,
                errmsg!("database system was interrupted while in recovery at {}", cstr_to_str(str_time((*ControlFile).time)))
            );
        }
        DB_IN_ARCHIVE_RECOVERY => {
            ereport!(
                LOG,
                errmsg!("database system was interrupted while in recovery at log time {}", cstr_to_str(str_time((*ControlFile).checkPointCopy.time)))
            );
        }
        DB_IN_PRODUCTION => {
            ereport!(
                LOG,
                errmsg!("database system was interrupted; last known up at {}", cstr_to_str(str_time((*ControlFile).time)))
            );
        }
        _ => {
            ereport!(FATAL, errmsg!("control file contains invalid database cluster state"));
            /* errcode(ERRCODE_DATA_CORRUPTED) */
        }
    }

    /* This is just to allow attaching to startup process with a debugger */
    // #ifdef XLOG_REPLAY_DELAY -- not compiled in production

    /*
     * Verify that pg_wal, pg_wal/archive_status, and pg_wal/summaries exist.
     */
    ValidateXLOGDirectoryStructure();

    /* Set up timeout handler needed to report startup progress. */
    if !IsBootstrapProcessingMode() {
        RegisterTimeout(
            STARTUP_PROGRESS_TIMEOUT,
            startup_progress_timeout_handler,
        );
    }

    /*
     * If we previously crashed, perform cleanup actions.
     */
    if (*ControlFile).state != DB_SHUTDOWNED
        && (*ControlFile).state != DB_SHUTDOWNED_IN_RECOVERY
    {
        RemoveTempXlogFiles();
        SyncDataDirectory();
        didCrash = true;
    } else {
        didCrash = false;
    }

    /*
     * Prepare for WAL recovery if needed.
     */
    InitWalRecovery(
        ControlFile as *mut _ as *mut crate::access::transam::xlogrecovery::ControlFileData,
        &mut wasShutdown,
        &mut haveBackupLabel,
        &mut haveTblspcMap,
    );
    checkPoint = (*ControlFile).checkPointCopy;

    /* initialize shared memory variables from the checkpoint record */
    (*TransamVariables).nextXid = checkPoint.nextXid;
    (*TransamVariables).nextOid = checkPoint.nextOid;
    (*TransamVariables).oidCount = 0;
    MultiXactSetNextMXact(checkPoint.nextMulti, checkPoint.nextMultiOffset);
    AdvanceOldestClogXid(checkPoint.oldestXid);
    SetTransactionIdLimit(checkPoint.oldestXid, checkPoint.oldestXidDB);
    SetMultiXactIdLimit(checkPoint.oldestMulti, checkPoint.oldestMultiDB, true);
    SetCommitTsLimit(checkPoint.oldestCommitTsXid, checkPoint.newestCommitTsXid);
    (*XLogCtl).ckptFullXid = core::mem::transmute(checkPoint.nextXid);

    /*
     * Clear out any old relcache cache files.
     */
    RelationCacheInitFileRemove();

    /*
     * Initialize replication slots, before there's a chance to remove
     * required resources.
     */
    StartupReplicationSlots();

    /*
     * Startup logical state, needs to be setup now so we have proper data
     * during crash recovery.
     */
    StartupReorderBuffer();

    /*
     * Startup CLOG.
     */
    StartupCLOG();

    /*
     * Startup MultiXact.
     */
    StartupMultiXact();

    /*
     * Ditto for commit timestamps.
     */
    if (*ControlFile).track_commit_timestamp {
        StartupCommitTs();
    }

    /*
     * Recover knowledge about replay progress of known replication partners.
     */
    StartupReplicationOrigin();

    /*
     * Initialize unlogged LSN.
     */
    if (*ControlFile).state == DB_SHUTDOWNED {
        pg_atomic_write_membarrier_u64(&mut (*XLogCtl).unloggedLSN, (*ControlFile).unloggedLSN);
    } else {
        pg_atomic_write_membarrier_u64(&mut (*XLogCtl).unloggedLSN, FirstNormalUnloggedLSN);
    }

    /*
     * Copy any missing timeline history files between 'now' and the recovery
     * target timeline from archive to pg_wal.
     */
    restoreTimeLineHistoryFiles(checkPoint.ThisTimeLineID, recoveryTargetTLI);

    /*
     * Before running in recovery, scan pg_twophase and fill in its status.
     */
    restoreTwoPhaseData();

    /*
     * When starting with crash recovery, reset pgstat data - it might not be
     * valid.
     */
    if didCrash {
        pgstat_discard_stats();
    } else {
        pgstat_restore_stats();
    }

    lastFullPageWrites = checkPoint.fullPageWrites;

    RedoRecPtr = (*XLogCtl).RedoRecPtr;
    (*XLogCtl).Insert.RedoRecPtr = checkPoint.redo;
    (*XLogCtl).RedoRecPtr = checkPoint.redo;
    RedoRecPtr = checkPoint.redo;
    doPageWrites = lastFullPageWrites;

    /* REDO */
    if InRecovery {
        /* Initialize state for RecoveryInProgress() */
        SpinLockAcquire(&mut (*XLogCtl).info_lck);
        if InArchiveRecovery {
            (*XLogCtl).SharedRecoveryState = RECOVERY_STATE_ARCHIVE;
        } else {
            (*XLogCtl).SharedRecoveryState = RECOVERY_STATE_CRASH;
        }
        SpinLockRelease(&mut (*XLogCtl).info_lck);

        /*
         * Update pg_control to show that we are recovering.
         */
        UpdateControlFile();

        /*
         * If there was a backup label file, it's done its job.
         */
        if haveBackupLabel {
            libc::unlink(BACKUP_LABEL_OLD.as_ptr() as *const c_char);
            durable_rename(
                BACKUP_LABEL_FILE as *const c_char,
                BACKUP_LABEL_OLD.as_ptr() as *const c_char,
                FATAL,
            );
        }

        /*
         * If there was a tablespace_map file, it's done its job.
         */
        if haveTblspcMap {
            libc::unlink(TABLESPACE_MAP_OLD as *const c_char);
            durable_rename(
                TABLESPACE_MAP as *const c_char,
                TABLESPACE_MAP_OLD as *const c_char,
                FATAL,
            );
        }

        /*
         * Initialize our local copy of minRecoveryPoint.
         */
        if InArchiveRecovery {
            LocalMinRecoveryPoint = (*ControlFile).minRecoveryPoint;
            LocalMinRecoveryPointTLI = (*ControlFile).minRecoveryPointTLI;
        } else {
            LocalMinRecoveryPoint = InvalidXLogRecPtr;
            LocalMinRecoveryPointTLI = 0;
        }

        /* Check that the GUCs used to generate the WAL allow recovery */
        CheckRequiredParameterValues();

        /*
         * We're in recovery, so unlogged relations may be trashed and must be
         * reset.
         */
        ResetUnloggedRelations(UNLOGGED_RELATION_CLEANUP);

        /*
         * Likewise, delete any saved transaction snapshot files.
         */
        DeleteAllExportedSnapshotFiles();

        /*
         * Initialize for Hot Standby, if enabled.
         */
        if ArchiveRecoveryRequested && EnableHotStandby {
            let mut xids: *mut TransactionId = ptr::null_mut();
            let mut nxids: c_int = 0;

            elog!(DEBUG1, "initializing for hot standby");

            InitRecoveryTransactionEnvironment();

            if wasShutdown {
                oldestActiveXID = PrescanPreparedTransactions(&mut xids, &mut nxids);
            } else {
                oldestActiveXID = checkPoint.oldestActiveXid;
            }
            assert!(TransactionIdIsValid(oldestActiveXID));

            /* Tell procarray about the range of xids it has to deal with */
            ProcArrayInitRecovery(XidFromFullTransactionId((*TransamVariables).nextXid));

            /*
             * Startup subtrans only.
             */
            StartupSUBTRANS(oldestActiveXID);

            /*
             * If we're beginning at a shutdown checkpoint, fake-up an empty
             * running-xacts record.
             */
            if wasShutdown {
                let mut running: RunningTransactionsData = core::mem::zeroed();
                let mut latestCompletedXid: TransactionId;

                /* Update pg_subtrans entries for any prepared transactions */
                StandbyRecoverPreparedTransactions();

                running.xcnt = nxids;
                running.subxcnt = 0;
                running.subxid_status = SUBXIDS_IN_SUBTRANS;
                running.nextXid = XidFromFullTransactionId(checkPoint.nextXid);
                running.oldestRunningXid = oldestActiveXID;
                latestCompletedXid = XidFromFullTransactionId(checkPoint.nextXid);
                TransactionIdRetreat(&mut latestCompletedXid);
                assert!(TransactionIdIsNormal(latestCompletedXid));
                running.latestCompletedXid = latestCompletedXid;
                running.xids = xids;

                ProcArrayApplyRecoveryInfo(&mut running);
            }
        }

        /*
         * We're all set for replaying the WAL now. Do it.
         */
        PerformWalRecovery();
        performedWalRecovery = true;
    } else {
        performedWalRecovery = false;
    }

    /*
     * Finish WAL recovery.
     */
    endOfRecoveryInfo = FinishWalRecovery();
    EndOfLog = (*endOfRecoveryInfo).endOfLog;
    EndOfLogTLI = (*endOfRecoveryInfo).endOfLogTLI;
    abortedRecPtr = (*endOfRecoveryInfo).abortedRecPtr;
    missingContrecPtr = (*endOfRecoveryInfo).missingContrecPtr;

    /*
     * Reset ps status display.
     */
    set_ps_display(b"\0".as_ptr() as *const c_char);

    /*
     * When recovering from a backup, complain if we did not roll forward far
     * enough to reach the point where the database is consistent.
     */
    if InRecovery
        && (EndOfLog < LocalMinRecoveryPoint
            || !XLogRecPtrIsInvalid((*ControlFile).backupStartPoint))
    {
        if ArchiveRecoveryRequested || (*ControlFile).backupEndRequired {
            if !XLogRecPtrIsInvalid((*ControlFile).backupStartPoint)
                || (*ControlFile).backupEndRequired
            {
                ereport!(FATAL, errmsg!("WAL ends before end of online backup"));
                /* errcode + errhint also in C */
            } else {
                ereport!(FATAL, errmsg!("WAL ends before consistent recovery point"));
                /* errcode also in C */
            }
        }
    }

    /*
     * Reset unlogged relations to the contents of their INIT fork.
     */
    if InRecovery {
        ResetUnloggedRelations(UNLOGGED_RELATION_INIT);
    }

    /*
     * Pre-scan prepared transactions.
     */
    oldestActiveXID = PrescanPreparedTransactions(ptr::null_mut(), ptr::null_mut());

    /*
     * Allow ordinary WAL segment creation before possibly switching to a new
     * timeline.
     */
    SetInstallXLogFileSegmentActive();

    /*
     * Consider whether we need to assign a new timeline ID.
     */
    newTLI = (*endOfRecoveryInfo).lastRecTLI;
    if ArchiveRecoveryRequested {
        newTLI = findNewestTimeLine(recoveryTargetTLI) + 1;
        ereport!(LOG, errmsg!("selected new timeline ID: {}", newTLI));

        /*
         * Make a writable copy of the last WAL segment.
         */
        XLogInitNewTimeline(EndOfLogTLI, EndOfLog, newTLI);

        /*
         * Remove the signal files out of the way.
         */
        if (*endOfRecoveryInfo).standby_signal_file_found {
            durable_unlink(STANDBY_SIGNAL_FILE as *const c_char, FATAL);
        }
        if (*endOfRecoveryInfo).recovery_signal_file_found {
            durable_unlink(RECOVERY_SIGNAL_FILE as *const c_char, FATAL);
        }

        /*
         * Write the timeline history file.
         */
        writeTimeLineHistory(
            newTLI,
            recoveryTargetTLI,
            EndOfLog,
            (*endOfRecoveryInfo).recoveryStopReason,
        );

        ereport!(LOG, errmsg!("archive recovery complete"));
    }

    /* Save the selected TimeLineID in shared memory */
    SpinLockAcquire(&mut (*XLogCtl).info_lck);
    (*XLogCtl).InsertTimeLineID = newTLI;
    (*XLogCtl).PrevTimeLineID = (*endOfRecoveryInfo).lastRecTLI;
    SpinLockRelease(&mut (*XLogCtl).info_lck);

    /*
     * Actually, if WAL ended in an incomplete record, skip the parts that
     * made it through.
     */
    if !XLogRecPtrIsInvalid(missingContrecPtr) {
        assert!(newTLI == (*endOfRecoveryInfo).lastRecTLI);
        assert!(!XLogRecPtrIsInvalid(abortedRecPtr));
        EndOfLog = missingContrecPtr;
    }

    /*
     * Prepare to write WAL starting at EndOfLog location.
     */
    Insert = &mut (*XLogCtl).Insert;
    (*Insert).PrevBytePos = XLogRecPtrToBytePos((*endOfRecoveryInfo).lastRec);
    (*Insert).CurrBytePos = XLogRecPtrToBytePos(EndOfLog);

    /*
     * Tricky point here: lastPage contains the *last* block that the LastRec
     * record spans.
     */
    if EndOfLog % XLOG_BLCKSZ as u64 != 0 {
        let firstIdx = XLogRecPtrToBufIdx(EndOfLog);
        let len = (EndOfLog - (*endOfRecoveryInfo).lastPageBeginPtr) as usize;
        assert!(len < XLOG_BLCKSZ);

        /* Copy the valid part of the last block, and zero the rest */
        let page = (*XLogCtl).pages.add(firstIdx as usize * XLOG_BLCKSZ);
        ptr::copy_nonoverlapping((*endOfRecoveryInfo).lastPage as *const u8, page as *mut u8, len);
        ptr::write_bytes((page as *mut u8).add(len), 0, XLOG_BLCKSZ - len);

        pg_atomic_write_u64(
            &mut *(*XLogCtl).xlblocks.add(firstIdx as usize),
            (*endOfRecoveryInfo).lastPageBeginPtr + XLOG_BLCKSZ as u64,
        );
        (*XLogCtl).InitializedUpTo =
            (*endOfRecoveryInfo).lastPageBeginPtr + XLOG_BLCKSZ as u64;
    } else {
        /*
         * There is no partial block to copy.
         */
        (*XLogCtl).InitializedUpTo = EndOfLog;
    }

    /*
     * Update local and shared status.
     */
    LogwrtResult.Write = EndOfLog;
    LogwrtResult.Flush = EndOfLog;
    pg_atomic_write_u64(&mut (*XLogCtl).logInsertResult, EndOfLog);
    pg_atomic_write_u64(&mut (*XLogCtl).logWriteResult, EndOfLog);
    pg_atomic_write_u64(&mut (*XLogCtl).logFlushResult, EndOfLog);
    (*XLogCtl).LogwrtRqst.Write = EndOfLog;
    (*XLogCtl).LogwrtRqst.Flush = EndOfLog;

    /*
     * Preallocate additional log files, if wanted.
     */
    PreallocXlogFiles(EndOfLog, newTLI);

    /*
     * Okay, we're officially UP.
     */
    InRecovery = false;

    /* start the archive_timeout timer and LSN running */
    (*XLogCtl).lastSegSwitchTime = libc::time(ptr::null_mut()) as pg_time_t;
    (*XLogCtl).lastSegSwitchLSN = EndOfLog;

    /* also initialize latestCompletedXid, to nextXid - 1 */
    LWLockAcquire(ProcArrayLock as *mut LWLock, LW_EXCLUSIVE);
    (*TransamVariables).latestCompletedXid = (*TransamVariables).nextXid;
    FullTransactionIdRetreat(&mut (*TransamVariables).latestCompletedXid);
    LWLockRelease(ProcArrayLock as *mut LWLock);

    /*
     * Start up subtrans, if not already done for hot standby.
     */
    if standbyState == STANDBY_DISABLED {
        StartupSUBTRANS(oldestActiveXID);
    }

    /*
     * Perform end of recovery actions for any SLRUs that need it.
     */
    TrimCLOG();
    TrimMultiXact();

    /*
     * Reload shared-memory state for prepared transactions.
     */
    RecoverPreparedTransactions();

    /* Shut down xlogreader */
    ShutdownWalRecovery();

    /* Enable WAL writes for this backend only. */
    LocalSetXLogInsertAllowed();

    /* If necessary, write overwrite-contrecord before doing anything else */
    if !XLogRecPtrIsInvalid(abortedRecPtr) {
        assert!(!XLogRecPtrIsInvalid(missingContrecPtr));
        CreateOverwriteContrecordRecord(abortedRecPtr, missingContrecPtr, newTLI);
    }

    /*
     * Update full_page_writes in shared memory and write an XLOG_FPW_CHANGE
     * record.
     */
    (*Insert).fullPageWrites = lastFullPageWrites;
    UpdateFullPageWrites();

    /*
     * Emit checkpoint or end-of-recovery record in XLOG, if required.
     */
    if performedWalRecovery {
        promoted = PerformRecoveryXLogAction();
    }

    /*
     * If any of the critical GUCs have changed, log them before we allow
     * backends to write WAL.
     */
    XLogReportParameters();

    /* If this is archive recovery, perform post-recovery cleanup actions. */
    if ArchiveRecoveryRequested {
        CleanupAfterArchiveRecovery(EndOfLogTLI, EndOfLog, newTLI);
    }

    /*
     * Local WAL inserts enabled, so it's time to finish initialization of
     * commit timestamp.
     */
    CompleteCommitTsInitialization();

    /*
     * All done with end-of-recovery actions.
     */
    LWLockAcquire(ControlFileLock!() as *mut LWLock, LW_EXCLUSIVE);
    (*ControlFile).state = DB_IN_PRODUCTION;

    SpinLockAcquire(&mut (*XLogCtl).info_lck);
    (*XLogCtl).SharedRecoveryState = RECOVERY_STATE_DONE;
    SpinLockRelease(&mut (*XLogCtl).info_lck);

    UpdateControlFile();
    LWLockRelease(ControlFileLock!() as *mut LWLock);

    /*
     * Shutdown the recovery environment.
     */
    if standbyState != STANDBY_DISABLED {
        ShutdownRecoveryTransactionEnvironment();
    }

    /*
     * If there were cascading standby servers connected to us, nudge any wal
     * sender processes.
     */
    WalSndWakeup(true, true);

    /*
     * If this was a promotion, request an (online) checkpoint now.
     */
    if promoted {
        RequestCheckpoint(CHECKPOINT_FORCE);
    }
}

/*
 * Callback from PerformWalRecovery(), called when we switch from crash
 * recovery to archive recovery mode.
 */
pub unsafe fn SwitchIntoArchiveRecovery(EndRecPtr: XLogRecPtr, replayTLI: TimeLineID) {
    /* initialize minRecoveryPoint to this record */
    LWLockAcquire(ControlFileLock!() as *mut LWLock, LW_EXCLUSIVE);
    (*ControlFile).state = DB_IN_ARCHIVE_RECOVERY;
    if (*ControlFile).minRecoveryPoint < EndRecPtr {
        (*ControlFile).minRecoveryPoint = EndRecPtr;
        (*ControlFile).minRecoveryPointTLI = replayTLI;
    }
    /* update local copy */
    LocalMinRecoveryPoint = (*ControlFile).minRecoveryPoint;
    LocalMinRecoveryPointTLI = (*ControlFile).minRecoveryPointTLI;

    /*
     * The startup process can update its local copy of minRecoveryPoint from
     * this point.
     */
    updateMinRecoveryPoint = true;

    UpdateControlFile();

    /*
     * We update SharedRecoveryState while holding the lock on ControlFileLock
     * so both states are consistent in shared memory.
     */
    SpinLockAcquire(&mut (*XLogCtl).info_lck);
    (*XLogCtl).SharedRecoveryState = RECOVERY_STATE_ARCHIVE;
    SpinLockRelease(&mut (*XLogCtl).info_lck);

    LWLockRelease(ControlFileLock!() as *mut LWLock);
}

/*
 * Callback from PerformWalRecovery(), called when we reach the end of backup.
 */
pub unsafe fn ReachedEndOfBackup(EndRecPtr: XLogRecPtr, tli: TimeLineID) {
    LWLockAcquire(ControlFileLock!() as *mut LWLock, LW_EXCLUSIVE);

    if (*ControlFile).minRecoveryPoint < EndRecPtr {
        (*ControlFile).minRecoveryPoint = EndRecPtr;
        (*ControlFile).minRecoveryPointTLI = tli;
    }

    (*ControlFile).backupStartPoint = InvalidXLogRecPtr;
    (*ControlFile).backupEndPoint = InvalidXLogRecPtr;
    (*ControlFile).backupEndRequired = false;
    UpdateControlFile();

    LWLockRelease(ControlFileLock!() as *mut LWLock);
}

/*
 * Perform whatever XLOG actions are necessary at end of REDO.
 */
unsafe fn PerformRecoveryXLogAction() -> bool {
    let mut promoted: bool = false;

    /*
     * Perform a checkpoint to update all our recovery activity to disk.
     */
    if ArchiveRecoveryRequested && IsUnderPostmaster && PromoteIsTriggered() {
        promoted = true;

        /*
         * Insert a special WAL record to mark the end of recovery.
         */
        CreateEndOfRecoveryRecord();
    } else {
        RequestCheckpoint(
            CHECKPOINT_END_OF_RECOVERY | CHECKPOINT_IMMEDIATE | CHECKPOINT_WAIT,
        );
    }

    promoted
}

/*
 * Is the system still in recovery?
 */
#[no_mangle]
pub unsafe fn RecoveryInProgress() -> bool {
    if !LocalRecoveryInProgress {
        return false;
    }

    /*
     * use volatile pointer to make sure we make a fresh read of the
     * shared variable.
     */
    let xlogctl = XLogCtl as *mut XLogCtlData;
    LocalRecoveryInProgress =
        (*xlogctl).SharedRecoveryState != RECOVERY_STATE_DONE;

    LocalRecoveryInProgress
}

/*
 * Returns current recovery state from shared memory.
 */
pub unsafe fn GetRecoveryState() -> RecoveryState {
    let retval: RecoveryState;
    SpinLockAcquire(&mut (*XLogCtl).info_lck);
    retval = (*XLogCtl).SharedRecoveryState;
    SpinLockRelease(&mut (*XLogCtl).info_lck);
    retval
}

/*
 * Is this process allowed to insert new WAL records?
 */
pub unsafe fn XLogInsertAllowed() -> bool {
    /*
     * If value is "unconditionally true" or "unconditionally false", just
     * return it.
     */
    if LocalXLogInsertAllowed >= 0 {
        return LocalXLogInsertAllowed != 0;
    }

    /*
     * Else, must check to see if we're still in recovery.
     */
    if RecoveryInProgress() {
        return false;
    }

    /*
     * On exit from recovery, reset to "unconditionally true".
     */
    LocalXLogInsertAllowed = 1;
    true
}

/*
 * Make XLogInsertAllowed() return true in the current process only.
 *
 * Returns the previous value of LocalXLogInsertAllowed.
 */
unsafe fn LocalSetXLogInsertAllowed() -> c_int {
    let oldXLogAllowed = LocalXLogInsertAllowed;
    LocalXLogInsertAllowed = 1;
    oldXLogAllowed
}

/*
 * Return the current Redo pointer from shared memory.
 */
pub unsafe fn GetRedoRecPtr() -> XLogRecPtr {
    let ptr: XLogRecPtr;

    SpinLockAcquire(&mut (*XLogCtl).info_lck);
    ptr = (*XLogCtl).RedoRecPtr;
    SpinLockRelease(&mut (*XLogCtl).info_lck);

    if RedoRecPtr < ptr {
        RedoRecPtr = ptr;
    }

    RedoRecPtr
}

/*
 * Return information needed to decide whether a modified block needs a
 * full-page image.
 */
pub unsafe fn GetFullPageWriteInfo(RedoRecPtr_p: *mut XLogRecPtr, doPageWrites_p: *mut bool) {
    *RedoRecPtr_p = RedoRecPtr;
    *doPageWrites_p = doPageWrites;
}

/*
 * GetInsertRecPtr -- Returns the current insert position.
 *
 * NOTE: The value *actually* returned is the position of the last full
 * xlog page.
 */
pub unsafe fn GetInsertRecPtr() -> XLogRecPtr {
    let recptr: XLogRecPtr;
    SpinLockAcquire(&mut (*XLogCtl).info_lck);
    recptr = (*XLogCtl).LogwrtRqst.Write;
    SpinLockRelease(&mut (*XLogCtl).info_lck);
    recptr
}

/*
 * GetFlushRecPtr -- Returns the current flush position.
 */
#[no_mangle]
pub unsafe fn GetFlushRecPtr(insertTLI: *mut TimeLineID) -> XLogRecPtr {
    assert!((*XLogCtl).SharedRecoveryState == RECOVERY_STATE_DONE);

    RefreshXLogWriteResult!(&mut LogwrtResult);

    /*
     * If we're writing and flushing WAL, the time line can't be changing, so
     * no lock is required.
     */
    if !insertTLI.is_null() {
        *insertTLI = (*XLogCtl).InsertTimeLineID;
    }

    LogwrtResult.Flush
}

/*
 * GetWALInsertionTimeLine -- Returns the current timeline of a system that
 * is not in recovery.
 */
pub unsafe fn GetWALInsertionTimeLine() -> TimeLineID {
    assert!((*XLogCtl).SharedRecoveryState == RECOVERY_STATE_DONE);
    /* Since the value can't be changing, no lock is required. */
    (*XLogCtl).InsertTimeLineID
}

/*
 * GetWALInsertionTimeLineIfSet -- If the system is not in recovery, returns
 * the WAL insertion timeline; else, returns 0.
 */
pub unsafe fn GetWALInsertionTimeLineIfSet() -> TimeLineID {
    let insertTLI: TimeLineID;
    SpinLockAcquire(&mut (*XLogCtl).info_lck);
    insertTLI = (*XLogCtl).InsertTimeLineID;
    SpinLockRelease(&mut (*XLogCtl).info_lck);
    insertTLI
}

/*
 * GetLastImportantRecPtr -- Returns the LSN of the last important record
 * inserted.
 */
pub unsafe fn GetLastImportantRecPtr() -> XLogRecPtr {
    let mut res: XLogRecPtr = InvalidXLogRecPtr;

    for i in 0..NUM_XLOGINSERT_LOCKS as usize {
        /*
         * Need to take a lock to prevent torn reads of the LSN.
         */
        LWLockAcquire(&mut core::mem::ManuallyDrop::deref_mut(&mut (*WALInsertLocks.add(i)).l).lock, LW_EXCLUSIVE);
        let last_important = core::mem::ManuallyDrop::deref(&(*WALInsertLocks.add(i)).l).lastImportantAt;
        LWLockRelease(&mut core::mem::ManuallyDrop::deref_mut(&mut (*WALInsertLocks.add(i)).l).lock);

        if res < last_important {
            res = last_important;
        }
    }

    res
}

/*
 * Get the time and LSN of the last xlog segment switch
 */
pub unsafe fn GetLastSegSwitchData(lastSwitchLSN: *mut XLogRecPtr) -> pg_time_t {
    let result: pg_time_t;

    /* Need WALWriteLock, but shared lock is sufficient */
    LWLockAcquire(WALWriteLock!() as *mut LWLock, LW_SHARED);
    result = (*XLogCtl).lastSegSwitchTime;
    *lastSwitchLSN = (*XLogCtl).lastSegSwitchLSN;
    LWLockRelease(WALWriteLock!() as *mut LWLock);

    result
}

/*
 * This must be called ONCE during postmaster or standalone-backend shutdown
 */
pub unsafe extern "C" fn ShutdownXLOG(code: c_int, arg: Datum) {
    assert!(!AuxProcessResourceOwner.is_null());
    assert!(
        CurrentResourceOwner.is_null()
            || CurrentResourceOwner == AuxProcessResourceOwner
    );
    CurrentResourceOwner = AuxProcessResourceOwner;

    /* Don't be chatty in standalone mode */
    ereport!(
        if IsPostmasterEnvironment { LOG } else { NOTICE },
        errmsg!("shutting down")
    );

    /*
     * Signal walsenders to move to stopping state.
     */
    WalSndInitStopping();

    /*
     * Wait for WAL senders to be in stopping state.
     */
    WalSndWaitStopping();

    if RecoveryInProgress() {
        CreateRestartPoint(CHECKPOINT_IS_SHUTDOWN | CHECKPOINT_IMMEDIATE);
    } else {
        /*
         * If archiving is enabled, rotate the last XLOG file.
         */
        if XLogArchivingActive() {
            RequestXLogSwitch(false);
        }
        CreateCheckPoint(CHECKPOINT_IS_SHUTDOWN | CHECKPOINT_IMMEDIATE);
    }
}

/*
 * Log start of a checkpoint.
 */
unsafe fn LogCheckpointStart(flags: c_int, restartpoint: bool) {
    if restartpoint {
        /* translator: the placeholders show checkpoint options */
        ereport!(
            LOG,
            errmsg!(
                "restartpoint starting:{}{}{}{}{}{}{}{}",
                if flags & CHECKPOINT_IS_SHUTDOWN != 0 { " shutdown" } else { "" },
                if flags & CHECKPOINT_END_OF_RECOVERY != 0 { " end-of-recovery" } else { "" },
                if flags & CHECKPOINT_IMMEDIATE != 0 { " immediate" } else { "" },
                if flags & CHECKPOINT_FORCE != 0 { " force" } else { "" },
                if flags & CHECKPOINT_WAIT != 0 { " wait" } else { "" },
                if flags & CHECKPOINT_CAUSE_XLOG != 0 { " wal" } else { "" },
                if flags & CHECKPOINT_CAUSE_TIME != 0 { " time" } else { "" },
                if flags & CHECKPOINT_FLUSH_ALL != 0 { " flush-all" } else { "" }
            )
        );
    } else {
        /* translator: the placeholders show checkpoint options */
        ereport!(
            LOG,
            errmsg!(
                "checkpoint starting:{}{}{}{}{}{}{}{}",
                if flags & CHECKPOINT_IS_SHUTDOWN != 0 { " shutdown" } else { "" },
                if flags & CHECKPOINT_END_OF_RECOVERY != 0 { " end-of-recovery" } else { "" },
                if flags & CHECKPOINT_IMMEDIATE != 0 { " immediate" } else { "" },
                if flags & CHECKPOINT_FORCE != 0 { " force" } else { "" },
                if flags & CHECKPOINT_WAIT != 0 { " wait" } else { "" },
                if flags & CHECKPOINT_CAUSE_XLOG != 0 { " wal" } else { "" },
                if flags & CHECKPOINT_CAUSE_TIME != 0 { " time" } else { "" },
                if flags & CHECKPOINT_FLUSH_ALL != 0 { " flush-all" } else { "" }
            )
        );
    }
}

/*
 * Log end of a checkpoint.
 */
unsafe fn LogCheckpointEnd(restartpoint: bool) {
    let write_msecs: i64;
    let sync_msecs: i64;
    let total_msecs: i64;
    let longest_msecs: i64;
    let average_msecs: i64;
    let average_sync_time: u64;

    CheckpointStats.ckpt_end_t = GetCurrentTimestamp();

    write_msecs = TimestampDifferenceMilliseconds(
        CheckpointStats.ckpt_write_t,
        CheckpointStats.ckpt_sync_t,
    );

    sync_msecs = TimestampDifferenceMilliseconds(
        CheckpointStats.ckpt_sync_t,
        CheckpointStats.ckpt_sync_end_t,
    );

    /* Accumulate checkpoint timing summary data, in milliseconds. */
    PendingCheckpointerStats.write_time += write_msecs;
    PendingCheckpointerStats.sync_time += sync_msecs;

    /*
     * All of the published timing statistics are accounted for.  Only
     * continue if a log message is to be written.
     */
    if !log_checkpoints {
        return;
    }

    total_msecs = TimestampDifferenceMilliseconds(
        CheckpointStats.ckpt_start_t,
        CheckpointStats.ckpt_end_t,
    );

    /*
     * Timing values returned from CheckpointStats are in microseconds.
     * Convert to milliseconds for consistent printing.
     */
    longest_msecs = ((CheckpointStats.ckpt_longest_sync + 999) / 1000) as i64;

    average_sync_time = 0;
    let mut average_sync_time_inner = 0u64;
    if CheckpointStats.ckpt_sync_rels > 0 {
        average_sync_time_inner = CheckpointStats.ckpt_agg_sync_time
            / CheckpointStats.ckpt_sync_rels as u64;
    }
    average_msecs = (average_sync_time_inner as i64 + 999) / 1000;

    /*
     * ControlFileLock is not required to see ControlFile->checkPoint and
     * ->checkPointCopy here as we are the only updator of those variables.
     */
    let (chkpt_hi, chkpt_lo) = LSN_FORMAT_ARGS((*ControlFile).checkPoint);
    let (redo_hi, redo_lo) = LSN_FORMAT_ARGS((*ControlFile).checkPointCopy.redo);
    if restartpoint {
        ereport!(
            LOG,
            errmsg!(
                "restartpoint complete: wrote {} buffers ({:.1}%), \
                 wrote {} SLRU buffers; {} WAL file(s) added, \
                 {} removed, {} recycled; write={}.{:03} s, \
                 sync={}.{:03} s, total={}.{:03} s; sync files={}, \
                 longest={}.{:03} s, average={}.{:03} s; distance={} kB, \
                 estimate={} kB; lsn={}/{}, redo lsn={}/{}",
                CheckpointStats.ckpt_bufs_written,
                CheckpointStats.ckpt_bufs_written as f64 * 100.0 / NBuffers as f64,
                CheckpointStats.ckpt_slru_written,
                CheckpointStats.ckpt_segs_added,
                CheckpointStats.ckpt_segs_removed,
                CheckpointStats.ckpt_segs_recycled,
                write_msecs / 1000, write_msecs % 1000,
                sync_msecs / 1000, sync_msecs % 1000,
                total_msecs / 1000, total_msecs % 1000,
                CheckpointStats.ckpt_sync_rels,
                longest_msecs / 1000, longest_msecs % 1000,
                average_msecs / 1000, average_msecs % 1000,
                (PrevCheckPointDistance / 1024.0) as i64,
                (CheckPointDistanceEstimate / 1024.0) as i64,
                chkpt_hi, chkpt_lo, redo_hi, redo_lo
            )
        );
    } else {
        ereport!(
            LOG,
            errmsg!(
                "checkpoint complete: wrote {} buffers ({:.1}%), \
                 wrote {} SLRU buffers; {} WAL file(s) added, \
                 {} removed, {} recycled; write={}.{:03} s, \
                 sync={}.{:03} s, total={}.{:03} s; sync files={}, \
                 longest={}.{:03} s, average={}.{:03} s; distance={} kB, \
                 estimate={} kB; lsn={}/{}, redo lsn={}/{}",
                CheckpointStats.ckpt_bufs_written,
                CheckpointStats.ckpt_bufs_written as f64 * 100.0 / NBuffers as f64,
                CheckpointStats.ckpt_slru_written,
                CheckpointStats.ckpt_segs_added,
                CheckpointStats.ckpt_segs_removed,
                CheckpointStats.ckpt_segs_recycled,
                write_msecs / 1000, write_msecs % 1000,
                sync_msecs / 1000, sync_msecs % 1000,
                total_msecs / 1000, total_msecs % 1000,
                CheckpointStats.ckpt_sync_rels,
                longest_msecs / 1000, longest_msecs % 1000,
                average_msecs / 1000, average_msecs % 1000,
                (PrevCheckPointDistance / 1024.0) as i64,
                (CheckPointDistanceEstimate / 1024.0) as i64,
                chkpt_hi, chkpt_lo, redo_hi, redo_lo
            )
        );
    }
}

/*
 * Update the estimate of distance between checkpoints.
 */
unsafe fn UpdateCheckPointDistanceEstimate(nbytes: u64) {
    PrevCheckPointDistance = nbytes as f64;
    if CheckPointDistanceEstimate < nbytes as f64 {
        CheckPointDistanceEstimate = nbytes as f64;
    } else {
        CheckPointDistanceEstimate =
            0.90 * CheckPointDistanceEstimate + 0.10 * nbytes as f64;
    }
}

/*
 * Update the ps display for a process running a checkpoint.  Note that
 * this routine should not do any allocations so as it can be called
 * from a critical section.
 */
unsafe fn update_checkpoint_display(flags: c_int, restartpoint: bool, reset: bool) {
    /*
     * The status is reported only for end-of-recovery and shutdown
     * checkpoints or shutdown restartpoints.
     */
    if (flags & (CHECKPOINT_END_OF_RECOVERY | CHECKPOINT_IS_SHUTDOWN)) == 0 {
        return;
    }

    if reset {
        set_ps_display(b"\0".as_ptr() as *const c_char);
    } else {
        let mut activitymsg = [0u8; 128];
        libc::snprintf(
            activitymsg.as_mut_ptr() as *mut c_char,
            128,
            b"performing %s%s%s\0".as_ptr() as *const c_char,
            if flags & CHECKPOINT_END_OF_RECOVERY != 0 {
                b"end-of-recovery \0".as_ptr() as *const c_char
            } else {
                b"\0".as_ptr() as *const c_char
            },
            if flags & CHECKPOINT_IS_SHUTDOWN != 0 {
                b"shutdown \0".as_ptr() as *const c_char
            } else {
                b"\0".as_ptr() as *const c_char
            },
            if restartpoint {
                b"restartpoint\0".as_ptr() as *const c_char
            } else {
                b"checkpoint\0".as_ptr() as *const c_char
            },
        );
        set_ps_display(activitymsg.as_ptr() as *const c_char);
    }
}


/*
 * Perform a checkpoint --- either during shutdown, or on-the-fly
 *
 * Returns true if a new checkpoint was performed, or false if it was skipped
 * because the system was idle.
 */
pub unsafe fn CreateCheckPoint(flags: c_int) -> bool {
    let shutdown: bool;
    let mut checkPoint: CheckPoint = core::mem::zeroed();
    let mut recptr: XLogRecPtr = 0;
    let mut _logSegNo: XLogSegNo = 0;
    let Insert: *mut XLogCtlInsert = &mut (*XLogCtl).Insert;
    let mut freespace: uint32;
    let mut PriorRedoPtr: XLogRecPtr;
    let last_important_lsn: XLogRecPtr;
    let mut vxids: *mut crate::storage::ipc::procarray::VirtualTransactionId;
    let mut nvxids: c_int = 0;
    let mut oldXLogAllowed: c_int = 0;

    /*
     * An end-of-recovery checkpoint is really a shutdown checkpoint.
     */
    shutdown = (flags & (CHECKPOINT_IS_SHUTDOWN | CHECKPOINT_END_OF_RECOVERY)) != 0;

    /* sanity check */
    if RecoveryInProgress() && (flags & CHECKPOINT_END_OF_RECOVERY) == 0 {
        elog!(ERROR, "can't create a checkpoint during recovery");
    }

    /*
     * Prepare to accumulate statistics.
     */
    MemSet(
        &mut CheckpointStats as *mut CheckpointStatsData as *mut c_void,
        0,
        core::mem::size_of::<CheckpointStatsData>(),
    );
    CheckpointStats.ckpt_start_t = GetCurrentTimestamp();

    /*
     * Let smgr prepare for checkpoint.
     */
    SyncPreCheckpoint();

    /*
     * Use a critical section to force system panic if we have trouble.
     */
    START_CRIT_SECTION!();

    if shutdown {
        LWLockAcquire(ControlFileLock!() as *mut LWLock, LW_EXCLUSIVE);
        (*ControlFile).state = DB_SHUTDOWNING;
        UpdateControlFile();
        LWLockRelease(ControlFileLock!() as *mut LWLock);
    }

    /* Begin filling in the checkpoint WAL record */
    MemSet(
        &mut checkPoint as *mut CheckPoint as *mut c_void,
        0,
        core::mem::size_of::<CheckPoint>(),
    );
    checkPoint.time = libc::time(ptr::null_mut()) as pg_time_t;

    /*
     * For Hot Standby, derive the oldestActiveXid before we fix the redo pointer.
     */
    if !shutdown && XLogStandbyInfoActive() {
        checkPoint.oldestActiveXid = GetOldestActiveTransactionId();
    } else {
        checkPoint.oldestActiveXid = InvalidTransactionId;
    }

    /*
     * Get location of last important record.
     */
    last_important_lsn = GetLastImportantRecPtr();

    /*
     * If this isn't a shutdown or forced checkpoint, and no WAL activity,
     * skip it.
     */
    if (flags & (CHECKPOINT_IS_SHUTDOWN | CHECKPOINT_END_OF_RECOVERY | CHECKPOINT_FORCE)) == 0 {
        if last_important_lsn == (*ControlFile).checkPoint {
            END_CRIT_SECTION!();
            elog!(DEBUG1, "checkpoint skipped because system is idle");
            return false;
        }
    }

    /*
     * An end-of-recovery checkpoint is created before anyone is allowed to
     * write WAL.
     */
    if flags & CHECKPOINT_END_OF_RECOVERY != 0 {
        oldXLogAllowed = LocalSetXLogInsertAllowed();
    }

    checkPoint.ThisTimeLineID = (*XLogCtl).InsertTimeLineID;
    if flags & CHECKPOINT_END_OF_RECOVERY != 0 {
        checkPoint.PrevTimeLineID = (*XLogCtl).PrevTimeLineID;
    } else {
        checkPoint.PrevTimeLineID = checkPoint.ThisTimeLineID;
    }

    /*
     * We must block concurrent insertions while examining insert state.
     */
    WALInsertLockAcquireExclusive();

    checkPoint.fullPageWrites = (*Insert).fullPageWrites;
    checkPoint.wal_level = wal_level;

    if shutdown {
        let curInsert = XLogBytePosToRecPtr((*Insert).CurrBytePos);

        /*
         * Compute new REDO record ptr = location of next XLOG record.
         */
        freespace = INSERT_FREESPACE(curInsert) as uint32;
        if freespace == 0 {
            if XLogSegmentOffset(curInsert, wal_segment_size) == 0 {
                let new = curInsert + SizeOfXLogLongPHD as XLogRecPtr;
                checkPoint.redo = new;
            } else {
                let new = curInsert + SizeOfXLogShortPHD as XLogRecPtr;
                checkPoint.redo = new;
            }
        } else {
            checkPoint.redo = curInsert;
        }

        /*
         * Here we update the shared RedoRecPtr for future XLogInsert calls.
         */
        RedoRecPtr = (*XLogCtl).Insert.RedoRecPtr;
        (*XLogCtl).Insert.RedoRecPtr = checkPoint.redo;
        RedoRecPtr = checkPoint.redo;
    }

    /*
     * Now we can release the WAL insertion locks.
     */
    WALInsertLockRelease();

    /*
     * If this is an online checkpoint, insert the special XLOG_CHECKPOINT_REDO
     * record.
     */
    if !shutdown {
        /* Include WAL level in record for WAL summarizer's benefit. */
        XLogBeginInsert();
        XLogRegisterData(&mut wal_level as *mut c_int as *const c_void, core::mem::size_of::<c_int>() as u32);
        let _ = XLogInsert(RM_XLOG_ID, XLOG_CHECKPOINT_REDO);

        checkPoint.redo = RedoRecPtr;
    }

    /* Update the info_lck-protected copy of RedoRecPtr as well */
    SpinLockAcquire(&mut (*XLogCtl).info_lck);
    (*XLogCtl).RedoRecPtr = checkPoint.redo;
    SpinLockRelease(&mut (*XLogCtl).info_lck);

    /*
     * If enabled, log checkpoint start.
     */
    if log_checkpoints {
        LogCheckpointStart(flags, false);
    }

    /* Update the process title */
    update_checkpoint_display(flags, false, false);

    // TRACE_POSTGRESQL_CHECKPOINT_START(flags)

    /*
     * Get the other info we need for the checkpoint record.
     */
    LWLockAcquire(XidGenLock!() as *mut LWLock, LW_SHARED);
    checkPoint.nextXid = (*TransamVariables).nextXid;
    checkPoint.oldestXid = (*TransamVariables).oldestXid;
    checkPoint.oldestXidDB = (*TransamVariables).oldestXidDB;
    LWLockRelease(XidGenLock!() as *mut LWLock);

    LWLockAcquire(CommitTsLock!() as *mut LWLock, LW_SHARED);
    checkPoint.oldestCommitTsXid = (*TransamVariables).oldestCommitTsXid;
    checkPoint.newestCommitTsXid = (*TransamVariables).newestCommitTsXid;
    LWLockRelease(CommitTsLock!() as *mut LWLock);

    LWLockAcquire(OidGenLock!() as *mut LWLock, LW_SHARED);
    checkPoint.nextOid = (*TransamVariables).nextOid;
    if !shutdown {
        checkPoint.nextOid += (*TransamVariables).oidCount;
    }
    LWLockRelease(OidGenLock!() as *mut LWLock);

    MultiXactGetCheckptMulti(
        shutdown,
        &mut checkPoint.nextMulti,
        &mut checkPoint.nextMultiOffset,
        &mut checkPoint.oldestMulti,
        &mut checkPoint.oldestMultiDB,
    );

    /*
     * Having constructed the checkpoint record, ensure all shmem disk buffers
     * and commit-log buffers are flushed to disk.
     */
    END_CRIT_SECTION!();

    /*
     * Wait for any backend currently in commit critical sections.
     */
    vxids = GetVirtualXIDsDelayingChkpt(&mut nvxids, DELAY_CHKPT_START);
    if nvxids > 0 {
        loop {
            /*
             * Keep absorbing fsync requests while we wait.
             */
            AbsorbSyncRequests();
            pgstat_report_wait_start(WAIT_EVENT_CHECKPOINT_DELAY_START);
            pg_usleep(10000);
            pgstat_report_wait_end();
            if !HaveVirtualXIDsDelayingChkpt(vxids as *const _, nvxids, DELAY_CHKPT_START) {
                break;
            }
        }
    }
    pfree(vxids as *mut c_void);

    CheckPointGuts(checkPoint.redo, flags);

    vxids = GetVirtualXIDsDelayingChkpt(&mut nvxids, DELAY_CHKPT_COMPLETE);
    if nvxids > 0 {
        loop {
            AbsorbSyncRequests();
            pgstat_report_wait_start(WAIT_EVENT_CHECKPOINT_DELAY_COMPLETE);
            pg_usleep(10000);
            pgstat_report_wait_end();
            if !HaveVirtualXIDsDelayingChkpt(vxids as *const _, nvxids, DELAY_CHKPT_COMPLETE) {
                break;
            }
        }
    }
    pfree(vxids as *mut c_void);

    /*
     * Take a snapshot of running transactions and write this to WAL.
     */
    if !shutdown && XLogStandbyInfoActive() {
        LogStandbySnapshot();
    }

    START_CRIT_SECTION!();

    /*
     * Now insert the checkpoint record into XLOG.
     */
    XLogBeginInsert();
    XLogRegisterData(
        &mut checkPoint as *mut CheckPoint as *const c_void,
        core::mem::size_of::<CheckPoint>() as u32,
    );
    recptr = XLogInsert(
        RM_XLOG_ID,
        if shutdown { XLOG_CHECKPOINT_SHUTDOWN } else { XLOG_CHECKPOINT_ONLINE },
    );

    XLogFlush(recptr);

    /*
     * We mustn't write any new WAL after a shutdown checkpoint.
     */
    if shutdown {
        if flags & CHECKPOINT_END_OF_RECOVERY != 0 {
            LocalXLogInsertAllowed = oldXLogAllowed;
        } else {
            LocalXLogInsertAllowed = 0; /* never again write WAL */
        }
    }

    if shutdown && checkPoint.redo != ProcLastRecPtr {
        ereport!(
            PANIC,
            errmsg!("concurrent write-ahead log activity while database system is shutting down")
        );
    }

    /*
     * Remember the prior checkpoint's redo ptr.
     */
    PriorRedoPtr = (*ControlFile).checkPointCopy.redo;

    /*
     * Update the control file.
     */
    LWLockAcquire(ControlFileLock!() as *mut LWLock, LW_EXCLUSIVE);
    if shutdown {
        (*ControlFile).state = DB_SHUTDOWNED;
    }
    (*ControlFile).checkPoint = ProcLastRecPtr;
    (*ControlFile).checkPointCopy = checkPoint;
    /* crash recovery should always recover to the end of WAL */
    (*ControlFile).minRecoveryPoint = InvalidXLogRecPtr;
    (*ControlFile).minRecoveryPointTLI = 0;

    /*
     * Persist unloggedLSN value.
     */
    (*ControlFile).unloggedLSN =
        pg_atomic_read_membarrier_u64(&mut (*XLogCtl).unloggedLSN);

    UpdateControlFile();
    LWLockRelease(ControlFileLock!() as *mut LWLock);

    /* Update shared-memory copy of checkpoint XID/epoch */
    SpinLockAcquire(&mut (*XLogCtl).info_lck);
    (*XLogCtl).ckptFullXid = core::mem::transmute(checkPoint.nextXid);
    SpinLockRelease(&mut (*XLogCtl).info_lck);

    /*
     * We are now done with critical updates.
     */
    END_CRIT_SECTION!();

    /*
     * WAL summaries end when the next XLOG_CHECKPOINT_REDO or
     * XLOG_CHECKPOINT_SHUTDOWN record is reached.
     */
    WakeupWalSummarizer();

    /*
     * Let smgr do post-checkpoint cleanup.
     */
    SyncPostCheckpoint();

    /*
     * Update the average distance between checkpoints.
     */
    if PriorRedoPtr != InvalidXLogRecPtr {
        UpdateCheckPointDistanceEstimate(RedoRecPtr - PriorRedoPtr);
    }

    // INJECTION_POINT("checkpoint-before-old-wal-removal", NULL)

    /*
     * Delete old log files.
     */
    XLByteToSeg(RedoRecPtr, &mut _logSegNo, wal_segment_size);
    KeepLogSeg(recptr, &mut _logSegNo);
    if InvalidateObsoleteReplicationSlots(
        (RS_INVAL_WAL_REMOVED | RS_INVAL_IDLE_TIMEOUT) as u32,
        _logSegNo,
        InvalidOid,
        InvalidTransactionId,
    ) {
        /*
         * Some slots have been invalidated; recalculate the old-segment horizon.
         */
        XLByteToSeg(RedoRecPtr, &mut _logSegNo, wal_segment_size);
        KeepLogSeg(recptr, &mut _logSegNo);
    }
    _logSegNo -= 1;
    RemoveOldXlogFiles(_logSegNo, RedoRecPtr, recptr, checkPoint.ThisTimeLineID);

    /*
     * Make more log segments if needed.
     */
    if !shutdown {
        PreallocXlogFiles(recptr, checkPoint.ThisTimeLineID);
    }

    /*
     * Truncate pg_subtrans if possible.
     */
    if !RecoveryInProgress() {
        TruncateSUBTRANS(GetOldestTransactionIdConsideredRunning());
    }

    /* Real work is done; log and update stats. */
    LogCheckpointEnd(false);

    /* Reset the process title */
    update_checkpoint_display(flags, false, true);

    // TRACE_POSTGRESQL_CHECKPOINT_DONE(...)

    true
}

/*
 * Mark the end of recovery in WAL though without running a full checkpoint.
 */
unsafe fn CreateEndOfRecoveryRecord() {
    let mut xlrec: xl_end_of_recovery = core::mem::zeroed();
    let recptr: XLogRecPtr;

    /* sanity check */
    if !RecoveryInProgress() {
        elog!(ERROR, "can only be used to end recovery");
    }

    xlrec.end_time = GetCurrentTimestamp();
    xlrec.wal_level = wal_level;

    WALInsertLockAcquireExclusive();
    xlrec.ThisTimeLineID = (*XLogCtl).InsertTimeLineID;
    xlrec.PrevTimeLineID = (*XLogCtl).PrevTimeLineID;
    WALInsertLockRelease();

    START_CRIT_SECTION!();

    XLogBeginInsert();
    XLogRegisterData(
        &mut xlrec as *mut xl_end_of_recovery as *const c_void,
        core::mem::size_of::<xl_end_of_recovery>() as u32,
    );
    let recptr_val = XLogInsert(RM_XLOG_ID, XLOG_END_OF_RECOVERY);

    XLogFlush(recptr_val);

    /*
     * Update the control file so that crash recovery can follow the timeline
     * changes to this point.
     */
    LWLockAcquire(ControlFileLock!() as *mut LWLock, LW_EXCLUSIVE);
    (*ControlFile).minRecoveryPoint = recptr_val;
    (*ControlFile).minRecoveryPointTLI = xlrec.ThisTimeLineID;
    UpdateControlFile();
    LWLockRelease(ControlFileLock!() as *mut LWLock);

    END_CRIT_SECTION!();
}

/*
 * Write an OVERWRITE_CONTRECORD message.
 */
unsafe fn CreateOverwriteContrecordRecord(
    aborted_lsn: XLogRecPtr,
    pagePtr: XLogRecPtr,
    newTLI: TimeLineID,
) -> XLogRecPtr {
    let mut xlrec: xl_overwrite_contrecord = core::mem::zeroed();
    let recptr: XLogRecPtr;
    let pagehdr: *mut XLogPageHeaderData;
    let startPos: XLogRecPtr;

    /* sanity checks */
    if !RecoveryInProgress() {
        elog!(ERROR, "can only be used at end of recovery");
    }
    if pagePtr % XLOG_BLCKSZ as u64 != 0 {
        let (hi, lo) = LSN_FORMAT_ARGS(pagePtr);
        elog!(ERROR, "invalid position for missing continuation record {}/{}", hi, lo);
    }

    /* The current WAL insert position should be right after the page header */
    startPos = pagePtr;
    let startPos = if XLogSegmentOffset(startPos, wal_segment_size) == 0 {
        startPos + SizeOfXLogLongPHD as u64
    } else {
        startPos + SizeOfXLogShortPHD as u64
    };
    let cur_recptr = GetXLogInsertRecPtr();
    if cur_recptr != startPos {
        let (hi, lo) = LSN_FORMAT_ARGS(cur_recptr);
        elog!(
            ERROR,
            "invalid WAL insert position {}/{} for OVERWRITE_CONTRECORD",
            hi, lo
        );
    }

    START_CRIT_SECTION!();

    /*
     * Initialize the XLOG page header (by GetXLogBuffer), and set the
     * XLP_FIRST_IS_OVERWRITE_CONTRECORD flag.
     */
    WALInsertLockAcquire();
    pagehdr = GetXLogBuffer(pagePtr, newTLI) as *mut XLogPageHeaderData;
    (*pagehdr).xlp_info |= XLP_FIRST_IS_OVERWRITE_CONTRECORD;
    WALInsertLockRelease();

    /*
     * Insert the XLOG_OVERWRITE_CONTRECORD record.
     */
    XLogBeginInsert();
    xlrec.overwritten_lsn = aborted_lsn;
    xlrec.overwrite_time = GetCurrentTimestamp();
    XLogRegisterData(
        &mut xlrec as *mut xl_overwrite_contrecord as *const c_void,
        core::mem::size_of::<xl_overwrite_contrecord>() as u32,
    );
    let recptr_val = XLogInsert(RM_XLOG_ID, XLOG_OVERWRITE_CONTRECORD);

    /* check that the record was inserted to the right place */
    if ProcLastRecPtr != startPos {
        let (hi, lo) = LSN_FORMAT_ARGS(ProcLastRecPtr);
        elog!(
            ERROR,
            "OVERWRITE_CONTRECORD was inserted to unexpected position {}/{}",
            hi, lo
        );
    }

    XLogFlush(recptr_val);

    END_CRIT_SECTION!();

    recptr_val
}

/*
 * Flush all data in shared memory to disk, and fsync
 *
 * This is the common code shared between regular checkpoints and
 * recovery restartpoints.
 */
unsafe fn CheckPointGuts(checkPointRedo: XLogRecPtr, flags: c_int) {
    CheckPointRelationMap();
    CheckPointReplicationSlots(flags & CHECKPOINT_IS_SHUTDOWN != 0);
    CheckPointSnapBuild();
    CheckPointLogicalRewriteHeap();
    CheckPointReplicationOrigin();

    /* Write out all dirty data in SLRUs and the main buffer pool */
    // TRACE_POSTGRESQL_BUFFER_CHECKPOINT_START(flags)
    CheckpointStats.ckpt_write_t = GetCurrentTimestamp();
    CheckPointCLOG();
    CheckPointCommitTs();
    CheckPointSUBTRANS();
    CheckPointMultiXact();
    CheckPointPredicate();
    CheckPointBuffers(flags);

    /* Perform all queued up fsyncs */
    // TRACE_POSTGRESQL_BUFFER_CHECKPOINT_SYNC_START()
    CheckpointStats.ckpt_sync_t = GetCurrentTimestamp();
    ProcessSyncRequests();
    CheckpointStats.ckpt_sync_end_t = GetCurrentTimestamp();
    // TRACE_POSTGRESQL_BUFFER_CHECKPOINT_DONE()

    /* We deliberately delay 2PC checkpointing as long as possible */
    CheckPointTwoPhase(checkPointRedo);
}

/*
 * Save a checkpoint for recovery restart if appropriate
 */
unsafe fn RecoveryRestartPoint(checkPoint: *const CheckPoint, record: *mut XLogReaderState) {
    /*
     * Also refrain from creating a restartpoint if we have seen any
     * references to non-existent pages.
     */
    if XLogHaveInvalidPages() {
        let (hi, lo) = LSN_FORMAT_ARGS((*checkPoint).redo);
        elog!(
            DEBUG2,
            "could not record restart point at {}/{} because there \
             are unresolved references to invalid pages",
            hi, lo
        );
        return;
    }

    /*
     * Copy the checkpoint record to shared memory.
     */
    SpinLockAcquire(&mut (*XLogCtl).info_lck);
    (*XLogCtl).lastCheckPointRecPtr = (*record).ReadRecPtr;
    (*XLogCtl).lastCheckPointEndPtr = (*record).EndRecPtr;
    (*XLogCtl).lastCheckPoint = *checkPoint;
    SpinLockRelease(&mut (*XLogCtl).info_lck);
}

/*
 * Establish a restartpoint if possible.
 *
 * Returns true if a new restartpoint was established.
 */
pub unsafe fn CreateRestartPoint(flags: c_int) -> bool {
    let lastCheckPointRecPtr: XLogRecPtr;
    let lastCheckPointEndPtr: XLogRecPtr;
    let lastCheckPoint: CheckPoint;
    let PriorRedoPtr: XLogRecPtr;
    let receivePtr: XLogRecPtr;
    let replayPtr: XLogRecPtr;
    let mut replayTLI: TimeLineID = 0;
    let endptr: XLogRecPtr;
    let mut _logSegNo: XLogSegNo = 0;
    let xtime: TimestampTz;

    /* Concurrent checkpoint/restartpoint cannot happen */
    assert!(!IsUnderPostmaster || MyBackendType == B_CHECKPOINTER);

    /* Get a local copy of the last safe checkpoint record. */
    SpinLockAcquire(&mut (*XLogCtl).info_lck);
    lastCheckPointRecPtr = (*XLogCtl).lastCheckPointRecPtr;
    lastCheckPointEndPtr = (*XLogCtl).lastCheckPointEndPtr;
    lastCheckPoint = (*XLogCtl).lastCheckPoint;
    SpinLockRelease(&mut (*XLogCtl).info_lck);

    /*
     * Check that we're still in recovery mode.
     */
    if !RecoveryInProgress() {
        elog!(DEBUG2, "skipping restartpoint, recovery has already ended");
        return false;
    }

    /*
     * If the last checkpoint record we've replayed is already our last
     * restartpoint, we can't perform a new restart point.
     */
    if XLogRecPtrIsInvalid(lastCheckPointRecPtr)
        || lastCheckPoint.redo <= (*ControlFile).checkPointCopy.redo
    {
        let (hi, lo) = LSN_FORMAT_ARGS(lastCheckPoint.redo);
        elog!(DEBUG2, "skipping restartpoint, already performed at {}/{}", hi, lo);

        UpdateMinRecoveryPoint(InvalidXLogRecPtr, true);
        if flags & CHECKPOINT_IS_SHUTDOWN != 0 {
            LWLockAcquire(ControlFileLock!() as *mut LWLock, LW_EXCLUSIVE);
            (*ControlFile).state = DB_SHUTDOWNED_IN_RECOVERY;
            UpdateControlFile();
            LWLockRelease(ControlFileLock!() as *mut LWLock);
        }
        return false;
    }

    /*
     * Update the shared RedoRecPtr.
     */
    WALInsertLockAcquireExclusive();
    RedoRecPtr = lastCheckPoint.redo;
    (*XLogCtl).Insert.RedoRecPtr = lastCheckPoint.redo;
    WALInsertLockRelease();

    /* Also update the info_lck-protected copy */
    SpinLockAcquire(&mut (*XLogCtl).info_lck);
    (*XLogCtl).RedoRecPtr = lastCheckPoint.redo;
    SpinLockRelease(&mut (*XLogCtl).info_lck);

    /*
     * Prepare to accumulate statistics.
     */
    MemSet(
        &mut CheckpointStats as *mut CheckpointStatsData as *mut c_void,
        0,
        core::mem::size_of::<CheckpointStatsData>(),
    );
    CheckpointStats.ckpt_start_t = GetCurrentTimestamp();

    if log_checkpoints {
        LogCheckpointStart(flags, true);
    }

    /* Update the process title */
    update_checkpoint_display(flags, true, false);

    CheckPointGuts(lastCheckPoint.redo, flags);

    // INJECTION_POINT("create-restart-point", NULL)

    /*
     * Remember the prior checkpoint's redo ptr.
     */
    PriorRedoPtr = (*ControlFile).checkPointCopy.redo;

    /*
     * Update pg_control, using current time.
     */
    LWLockAcquire(ControlFileLock!() as *mut LWLock, LW_EXCLUSIVE);
    if (*ControlFile).checkPointCopy.redo < lastCheckPoint.redo {
        /*
         * Update the checkpoint information.
         */
        (*ControlFile).checkPoint = lastCheckPointRecPtr;
        (*ControlFile).checkPointCopy = lastCheckPoint;

        /*
         * Ensure minRecoveryPoint is past the checkpoint record.
         */
        if (*ControlFile).state == DB_IN_ARCHIVE_RECOVERY {
            if (*ControlFile).minRecoveryPoint < lastCheckPointEndPtr {
                (*ControlFile).minRecoveryPoint = lastCheckPointEndPtr;
                (*ControlFile).minRecoveryPointTLI = lastCheckPoint.ThisTimeLineID;

                /* update local copy */
                LocalMinRecoveryPoint = (*ControlFile).minRecoveryPoint;
                LocalMinRecoveryPointTLI = (*ControlFile).minRecoveryPointTLI;
            }
            if flags & CHECKPOINT_IS_SHUTDOWN != 0 {
                (*ControlFile).state = DB_SHUTDOWNED_IN_RECOVERY;
            }
        }
        UpdateControlFile();
    }
    LWLockRelease(ControlFileLock!() as *mut LWLock);

    /*
     * Update the average distance between checkpoints/restartpoints.
     */
    if PriorRedoPtr != InvalidXLogRecPtr {
        UpdateCheckPointDistanceEstimate(RedoRecPtr - PriorRedoPtr);
    }

    /*
     * Delete old log files.
     */
    XLByteToSeg(RedoRecPtr, &mut _logSegNo, wal_segment_size);

    /*
     * Retreat _logSegNo using the current end of xlog replayed or received,
     * whichever is later.
     */
    receivePtr = GetWalRcvFlushRecPtr(ptr::null_mut(), ptr::null_mut());
    replayPtr = GetXLogReplayRecPtr(&mut replayTLI);
    endptr = if receivePtr < replayPtr { replayPtr } else { receivePtr };
    KeepLogSeg(endptr, &mut _logSegNo);

    // INJECTION_POINT("restartpoint-before-slot-invalidation", NULL)

    if InvalidateObsoleteReplicationSlots(
        (RS_INVAL_WAL_REMOVED | RS_INVAL_IDLE_TIMEOUT) as u32,
        _logSegNo,
        InvalidOid,
        InvalidTransactionId,
    ) {
        /*
         * Some slots have been invalidated; recalculate the old-segment horizon.
         */
        XLByteToSeg(RedoRecPtr, &mut _logSegNo, wal_segment_size);
        KeepLogSeg(endptr, &mut _logSegNo);
    }
    _logSegNo -= 1;

    /*
     * Try to recycle segments on a useful timeline.
     */
    if !RecoveryInProgress() {
        replayTLI = (*XLogCtl).InsertTimeLineID;
    }

    RemoveOldXlogFiles(_logSegNo, RedoRecPtr, endptr, replayTLI);

    /*
     * Make more log segments if needed.
     */
    PreallocXlogFiles(endptr, replayTLI);

    /*
     * Truncate pg_subtrans if possible.
     */
    if EnableHotStandby {
        TruncateSUBTRANS(GetOldestTransactionIdConsideredRunning());
    }

    /* Real work is done; log and update stats. */
    LogCheckpointEnd(true);

    /* Reset the process title */
    update_checkpoint_display(flags, true, true);

    xtime = GetLatestXTime();
    let (hi, lo) = LSN_FORMAT_ARGS(lastCheckPoint.redo);
    ereport!(
        if log_checkpoints { LOG } else { DEBUG2 },
        errmsg!("recovery restart point at {}/{}", hi, lo)
    );

    /*
     * Finally, execute archive_cleanup_command, if any.
     */
    if !archiveCleanupCommand.is_null()
        && libc::strcmp(archiveCleanupCommand, b"\0".as_ptr() as *const c_char) != 0
    {
        ExecuteRecoveryCommand(
            archiveCleanupCommand,
            b"archive_cleanup_command\0".as_ptr() as *const c_char,
            false,
            WAIT_EVENT_ARCHIVE_CLEANUP_COMMAND,
        );
    }

    true
}

/*
 * Report availability of WAL for the given target LSN
 */
pub unsafe fn GetWALAvailability(targetLSN: XLogRecPtr) -> WALAvailability {
    let currpos: XLogRecPtr;
    let mut currSeg: XLogSegNo = 0;
    let mut targetSeg: XLogSegNo = 0;
    let mut oldestSeg: XLogSegNo;
    let mut oldestSegMaxWalSize: XLogSegNo;
    let mut oldestSlotSeg: XLogSegNo = 0;
    let keepSegs: u64;

    /*
     * slot does not reserve WAL.
     */
    if XLogRecPtrIsInvalid(targetLSN) {
        return WALAVAIL_INVALID_LSN;
    }

    /*
     * Calculate the oldest segment currently reserved by all slots.
     */
    currpos = GetXLogWriteRecPtr();
    XLByteToSeg(currpos, &mut oldestSlotSeg, wal_segment_size);
    KeepLogSeg(currpos, &mut oldestSlotSeg);

    /*
     * Find the oldest extant segment file.
     */
    oldestSeg = XLogGetLastRemovedSegno() + 1;

    /* calculate oldest segment by max_wal_size */
    XLByteToSeg(currpos, &mut currSeg, wal_segment_size);
    keepSegs = ConvertToXSegs(max_wal_size_mb, wal_segment_size) + 1;

    if currSeg > keepSegs {
        oldestSegMaxWalSize = currSeg - keepSegs;
    } else {
        oldestSegMaxWalSize = 1;
    }

    /* the segment we care about */
    XLByteToSeg(targetLSN, &mut targetSeg, wal_segment_size);

    /*
     * No point in returning reserved or extended status values if the
     * targetSeg is known to be lost.
     */
    if targetSeg >= oldestSlotSeg {
        /* show "reserved" when targetSeg is within max_wal_size */
        if targetSeg >= oldestSegMaxWalSize {
            return WALAVAIL_RESERVED;
        }
        /* being retained by slots exceeding max_wal_size */
        return WALAVAIL_EXTENDED;
    }

    /* WAL segments are no longer retained but haven't been removed yet */
    if targetSeg >= oldestSeg {
        return WALAVAIL_UNRESERVED;
    }

    /* Definitely lost */
    WALAVAIL_REMOVED
}

/*
 * Retreat *logSegNo to the last segment that we need to retain.
 */
unsafe fn KeepLogSeg(recptr: XLogRecPtr, logSegNo: *mut XLogSegNo) {
    let mut currSegNo: XLogSegNo = 0;
    let mut segno: XLogSegNo;
    let mut keep: XLogRecPtr;

    XLByteToSeg(recptr, &mut currSegNo, wal_segment_size);
    segno = currSegNo;

    /* Calculate how many segments are kept by slots. */
    keep = XLogGetReplicationSlotMinimumLSN();
    if keep != InvalidXLogRecPtr && keep < recptr {
        XLByteToSeg(keep, &mut segno, wal_segment_size);

        /*
         * Account for max_slot_wal_keep_size to avoid keeping more than
         * configured.
         */
        if max_slot_wal_keep_size_mb >= 0 && !IsBinaryUpgrade {
            let slot_keep_segs = ConvertToXSegs(max_slot_wal_keep_size_mb, wal_segment_size);
            if currSegNo - segno > slot_keep_segs {
                segno = currSegNo - slot_keep_segs;
            }
        }
    }

    /*
     * If WAL summarization is in use, don't remove WAL that has yet to be
     * summarized.
     */
    keep = GetOldestUnsummarizedLSN(ptr::null_mut(), ptr::null_mut());
    if keep != InvalidXLogRecPtr {
        let mut unsummarized_segno: XLogSegNo = 0;
        XLByteToSeg(keep, &mut unsummarized_segno, wal_segment_size);
        if unsummarized_segno < segno {
            segno = unsummarized_segno;
        }
    }

    /* but, keep at least wal_keep_size if that's set */
    if wal_keep_size_mb > 0 {
        let keep_segs = ConvertToXSegs(wal_keep_size_mb, wal_segment_size);
        if currSegNo - segno < keep_segs {
            /* avoid underflow, don't go below 1 */
            if currSegNo <= keep_segs {
                segno = 1;
            } else {
                segno = currSegNo - keep_segs;
            }
        }
    }

    /* don't delete WAL segments newer than the calculated segment */
    if segno < *logSegNo {
        *logSegNo = segno;
    }
}

/*
 * Write a NEXTOID log record
 */
pub unsafe fn XLogPutNextOid(nextOid: Oid) {
    XLogBeginInsert();
    XLogRegisterData(&nextOid as *const Oid as *const c_void, core::mem::size_of::<Oid>() as u32);
    let _ = XLogInsert(RM_XLOG_ID, XLOG_NEXTOID);

    /*
     * We need not flush the NEXTOID record immediately.
     */
}

/*
 * Write an XLOG SWITCH record.
 *
 * The return value is either the end+1 address of the switch record,
 * or the end+1 address of the prior segment if we did not need to
 * write a switch record because we are already at segment start.
 */
pub unsafe fn RequestXLogSwitch(mark_unimportant: bool) -> XLogRecPtr {
    /* XLOG SWITCH has no data */
    XLogBeginInsert();

    if mark_unimportant {
        XLogSetRecordFlags(XLOG_MARK_UNIMPORTANT);
    }
    XLogInsert(RM_XLOG_ID, XLOG_SWITCH)
}

/*
 * Write a RESTORE POINT record
 */
pub unsafe fn XLogRestorePoint(rpName: *const c_char) -> XLogRecPtr {
    let mut xlrec: xl_restore_point = core::mem::zeroed();

    xlrec.rp_time = GetCurrentTimestamp();
    strlcpy(xlrec.rp_name.as_mut_ptr(), rpName, MAXFNAMELEN);

    XLogBeginInsert();
    XLogRegisterData(
        &mut xlrec as *mut xl_restore_point as *const c_void,
        core::mem::size_of::<xl_restore_point>() as u32,
    );

    let RecPtr = XLogInsert(RM_XLOG_ID, XLOG_RESTORE_POINT);

    let (hi, lo) = LSN_FORMAT_ARGS(RecPtr);
    ereport!(
        LOG,
        errmsg!(
            "restore point \"{}\" created at {}/{}",
            core::ffi::CStr::from_ptr(rpName).to_string_lossy(),
            hi, lo
        )
    );

    RecPtr
}

/*
 * Check if any of the GUC parameters that are critical for hot standby
 * have changed, and update the value in pg_control file if necessary.
 */
unsafe fn XLogReportParameters() {
    if wal_level != (*ControlFile).wal_level
        || wal_log_hints != (*ControlFile).wal_log_hints
        || MaxConnections != (*ControlFile).MaxConnections
        || max_worker_processes != (*ControlFile).max_worker_processes
        || max_wal_senders != (*ControlFile).max_wal_senders
        || max_prepared_xacts != (*ControlFile).max_prepared_xacts
        || max_locks_per_xact != (*ControlFile).max_locks_per_xact
        || track_commit_timestamp != (*ControlFile).track_commit_timestamp
    {
        /*
         * The change in number of backend slots doesn't need to be WAL-logged
         * if archiving is not enabled.
         */
        if wal_level != (*ControlFile).wal_level || XLogIsNeeded() {
            let mut xlrec: xl_parameter_change = core::mem::zeroed();
            let recptr: XLogRecPtr;

            xlrec.MaxConnections = MaxConnections;
            xlrec.max_worker_processes = max_worker_processes;
            xlrec.max_wal_senders = max_wal_senders;
            xlrec.max_prepared_xacts = max_prepared_xacts;
            xlrec.max_locks_per_xact = max_locks_per_xact;
            xlrec.wal_level = wal_level;
            xlrec.wal_log_hints = wal_log_hints;
            xlrec.track_commit_timestamp = track_commit_timestamp;

            XLogBeginInsert();
            XLogRegisterData(
                &mut xlrec as *mut xl_parameter_change as *const c_void,
                core::mem::size_of::<xl_parameter_change>() as u32,
            );

            let recptr = XLogInsert(RM_XLOG_ID, XLOG_PARAMETER_CHANGE);
            XLogFlush(recptr);
        }

        LWLockAcquire(ControlFileLock!() as *mut LWLock, LW_EXCLUSIVE);

        (*ControlFile).MaxConnections = MaxConnections;
        (*ControlFile).max_worker_processes = max_worker_processes;
        (*ControlFile).max_wal_senders = max_wal_senders;
        (*ControlFile).max_prepared_xacts = max_prepared_xacts;
        (*ControlFile).max_locks_per_xact = max_locks_per_xact;
        (*ControlFile).wal_level = wal_level;
        (*ControlFile).wal_log_hints = wal_log_hints;
        (*ControlFile).track_commit_timestamp = track_commit_timestamp;
        UpdateControlFile();

        LWLockRelease(ControlFileLock!() as *mut LWLock);
    }
}

/*
 * Update full_page_writes in shared memory, and write an
 * XLOG_FPW_CHANGE record if necessary.
 */
pub unsafe fn UpdateFullPageWrites() {
    let Insert: *mut XLogCtlInsert = &mut (*XLogCtl).Insert;
    let recoveryInProgress: bool;

    /*
     * Do nothing if full_page_writes has not been changed.
     */
    if fullPageWrites == (*Insert).fullPageWrites {
        return;
    }

    /*
     * Perform this outside critical section.
     */
    recoveryInProgress = RecoveryInProgress();

    START_CRIT_SECTION!();

    /*
     * If we're setting full_page_writes to true, first set it true and then
     * write the WAL record.
     */
    if fullPageWrites {
        WALInsertLockAcquireExclusive();
        (*Insert).fullPageWrites = true;
        WALInsertLockRelease();
    }

    /*
     * Write an XLOG_FPW_CHANGE record.
     */
    if XLogStandbyInfoActive() && !recoveryInProgress {
        XLogBeginInsert();
        XLogRegisterData(&mut fullPageWrites as *mut bool as *const c_void, core::mem::size_of::<bool>() as u32);
        XLogInsert(RM_XLOG_ID, XLOG_FPW_CHANGE);
    }

    if !fullPageWrites {
        WALInsertLockAcquireExclusive();
        (*Insert).fullPageWrites = false;
        WALInsertLockRelease();
    }
    END_CRIT_SECTION!();
}

/*
 * XLOG resource manager's routines
 */
pub unsafe fn xlog_redo(record: *mut XLogReaderState) {
    let info: uint8 = (XLogRecGetInfo(record) & !XLR_INFO_MASK) as uint8;
    let lsn: XLogRecPtr = (*record).EndRecPtr;

    /*
     * In XLOG rmgr, backup blocks are only used by XLOG_FPI and
     * XLOG_FPI_FOR_HINT records.
     */
    assert!(
        info == XLOG_FPI || info == XLOG_FPI_FOR_HINT
            || !XLogRecHasAnyBlockRefs(record)
    );

    if info == XLOG_NEXTOID {
        let mut nextOid: Oid = 0;
        /*
         * We used to try to take the maximum of TransamVariables->nextOid and
         * the recorded nextOid, but that fails if the OID counter wraps around.
         */
        ptr::copy_nonoverlapping(
            XLogRecGetData(record) as *const u8,
            &mut nextOid as *mut Oid as *mut u8,
            core::mem::size_of::<Oid>(),
        );
        LWLockAcquire(OidGenLock!() as *mut LWLock, LW_EXCLUSIVE);
        (*TransamVariables).nextOid = nextOid;
        (*TransamVariables).oidCount = 0;
        LWLockRelease(OidGenLock!() as *mut LWLock);
    } else if info == XLOG_CHECKPOINT_SHUTDOWN {
        let mut checkPoint: CheckPoint = core::mem::zeroed();
        let replayTLI: TimeLineID;

        ptr::copy_nonoverlapping(
            XLogRecGetData(record) as *const u8,
            &mut checkPoint as *mut CheckPoint as *mut u8,
            core::mem::size_of::<CheckPoint>(),
        );
        /* In a SHUTDOWN checkpoint, believe the counters exactly */
        LWLockAcquire(XidGenLock!() as *mut LWLock, LW_EXCLUSIVE);
        (*TransamVariables).nextXid = checkPoint.nextXid;
        LWLockRelease(XidGenLock!() as *mut LWLock);
        LWLockAcquire(OidGenLock!() as *mut LWLock, LW_EXCLUSIVE);
        (*TransamVariables).nextOid = checkPoint.nextOid;
        (*TransamVariables).oidCount = 0;
        LWLockRelease(OidGenLock!() as *mut LWLock);
        MultiXactSetNextMXact(checkPoint.nextMulti, checkPoint.nextMultiOffset);
        MultiXactAdvanceOldest(checkPoint.oldestMulti, checkPoint.oldestMultiDB);

        /*
         * No need to set oldestClogXid here as well.
         */
        SetTransactionIdLimit(checkPoint.oldestXid, checkPoint.oldestXidDB);

        /*
         * If we see a shutdown checkpoint while waiting for an end-of-backup
         * record, the backup was canceled.
         */
        if ArchiveRecoveryRequested
            && !XLogRecPtrIsInvalid((*ControlFile).backupStartPoint)
            && XLogRecPtrIsInvalid((*ControlFile).backupEndPoint)
        {
            ereport!(PANIC, errmsg!("online backup was canceled, recovery cannot continue"));
        }

        /*
         * If we see a shutdown checkpoint, we know that nothing was running
         * on the primary at this point.
         */
        if standbyState >= STANDBY_INITIALIZED {
            let mut xids: *mut TransactionId = ptr::null_mut();
            let mut nxids: c_int = 0;
            let mut oldestActiveXID: TransactionId;
            let mut latestCompletedXid: TransactionId;
            let mut running: RunningTransactionsData = core::mem::zeroed();

            oldestActiveXID = PrescanPreparedTransactions(&mut xids, &mut nxids);

            /* Update pg_subtrans entries for any prepared transactions */
            StandbyRecoverPreparedTransactions();

            running.xcnt = nxids;
            running.subxcnt = 0;
            running.subxid_status = SUBXIDS_IN_SUBTRANS;
            running.nextXid = XidFromFullTransactionId(checkPoint.nextXid);
            running.oldestRunningXid = oldestActiveXID;
            latestCompletedXid = XidFromFullTransactionId(checkPoint.nextXid);
            TransactionIdRetreat(&mut latestCompletedXid);
            assert!(TransactionIdIsNormal(latestCompletedXid));
            running.latestCompletedXid = latestCompletedXid;
            running.xids = xids;

            ProcArrayApplyRecoveryInfo(&mut running);
        }

        /* ControlFile->checkPointCopy always tracks the latest ckpt XID */
        LWLockAcquire(ControlFileLock!() as *mut LWLock, LW_EXCLUSIVE);
        (*ControlFile).checkPointCopy.nextXid = checkPoint.nextXid;
        LWLockRelease(ControlFileLock!() as *mut LWLock);

        /* Update shared-memory copy of checkpoint XID/epoch */
        SpinLockAcquire(&mut (*XLogCtl).info_lck);
        (*XLogCtl).ckptFullXid = core::mem::transmute(checkPoint.nextXid);
        SpinLockRelease(&mut (*XLogCtl).info_lck);

        /*
         * We should've already switched to the new TLI before replaying this record.
         */
        let mut replayTLI_inner: TimeLineID = 0;
        let _ = GetCurrentReplayRecPtr(&mut replayTLI_inner);
        if checkPoint.ThisTimeLineID != replayTLI_inner {
            ereport!(
                PANIC,
                errmsg!(
                    "unexpected timeline ID {} (should be {}) in shutdown checkpoint record",
                    checkPoint.ThisTimeLineID, replayTLI_inner
                )
            );
        }

        RecoveryRestartPoint(&checkPoint, record);

        /*
         * After replaying a checkpoint record, free all smgr objects.
         */
        smgrdestroyall();
    } else if info == XLOG_CHECKPOINT_ONLINE {
        let mut checkPoint: CheckPoint = core::mem::zeroed();

        ptr::copy_nonoverlapping(
            XLogRecGetData(record) as *const u8,
            &mut checkPoint as *mut CheckPoint as *mut u8,
            core::mem::size_of::<CheckPoint>(),
        );
        /* In an ONLINE checkpoint, treat the XID counter as a minimum */
        LWLockAcquire(XidGenLock!() as *mut LWLock, LW_EXCLUSIVE);
        if FullTransactionIdPrecedes((*TransamVariables).nextXid, checkPoint.nextXid) {
            (*TransamVariables).nextXid = checkPoint.nextXid;
        }
        LWLockRelease(XidGenLock!() as *mut LWLock);

        /*
         * We ignore the nextOid counter in an ONLINE checkpoint.
         */

        /* Handle multixact */
        MultiXactAdvanceNextMXact(checkPoint.nextMulti, checkPoint.nextMultiOffset);

        /*
         * NB: This may perform multixact truncation.
         */
        MultiXactAdvanceOldest(checkPoint.oldestMulti, checkPoint.oldestMultiDB);
        if TransactionIdPrecedes((*TransamVariables).oldestXid, checkPoint.oldestXid) {
            SetTransactionIdLimit(checkPoint.oldestXid, checkPoint.oldestXidDB);
        }
        /* ControlFile->checkPointCopy always tracks the latest ckpt XID */
        LWLockAcquire(ControlFileLock!() as *mut LWLock, LW_EXCLUSIVE);
        (*ControlFile).checkPointCopy.nextXid = checkPoint.nextXid;
        LWLockRelease(ControlFileLock!() as *mut LWLock);

        /* Update shared-memory copy of checkpoint XID/epoch */
        SpinLockAcquire(&mut (*XLogCtl).info_lck);
        (*XLogCtl).ckptFullXid = core::mem::transmute(checkPoint.nextXid);
        SpinLockRelease(&mut (*XLogCtl).info_lck);

        /* TLI should not change in an on-line checkpoint */
        let mut replayTLI_inner: TimeLineID = 0;
        let _ = GetCurrentReplayRecPtr(&mut replayTLI_inner);
        if checkPoint.ThisTimeLineID != replayTLI_inner {
            ereport!(
                PANIC,
                errmsg!(
                    "unexpected timeline ID {} (should be {}) in online checkpoint record",
                    checkPoint.ThisTimeLineID, replayTLI_inner
                )
            );
        }

        RecoveryRestartPoint(&checkPoint, record);

        /*
         * After replaying a checkpoint record, free all smgr objects.
         */
        smgrdestroyall();
    } else if info == XLOG_OVERWRITE_CONTRECORD {
        /* nothing to do here, handled in xlogrecovery_redo() */
    } else if info == XLOG_END_OF_RECOVERY {
        let mut xlrec: xl_end_of_recovery = core::mem::zeroed();

        ptr::copy_nonoverlapping(
            XLogRecGetData(record) as *const u8,
            &mut xlrec as *mut xl_end_of_recovery as *mut u8,
            core::mem::size_of::<xl_end_of_recovery>(),
        );

        /*
         * For Hot Standby, we could treat this like a Shutdown Checkpoint,
         * but this case is rarer and harder to test.
         */

        /*
         * We should've already switched to the new TLI before replaying this record.
         */
        let mut replayTLI_inner: TimeLineID = 0;
        let _ = GetCurrentReplayRecPtr(&mut replayTLI_inner);
        if xlrec.ThisTimeLineID != replayTLI_inner {
            ereport!(
                PANIC,
                errmsg!(
                    "unexpected timeline ID {} (should be {}) in end-of-recovery record",
                    xlrec.ThisTimeLineID, replayTLI_inner
                )
            );
        }
    } else if info == XLOG_NOOP {
        /* nothing to do here */
    } else if info == XLOG_SWITCH {
        /* nothing to do here */
    } else if info == XLOG_RESTORE_POINT {
        /* nothing to do here, handled in xlogrecovery.c */
    } else if info == XLOG_FPI || info == XLOG_FPI_FOR_HINT {
        /*
         * XLOG_FPI records contain nothing else but one or more block
         * references. Every block reference must include a full-page image.
         *
         * XLOG_FPI_FOR_HINT records are generated when a page needs to be
         * WAL-logged because of a hint bit update. They may include no
         * full-page images if full_page_writes was disabled when generated.
         *
         * No recovery conflicts are generated by these generic records.
         */
        let mut block_id: uint8 = 0;
        while block_id as i32 <= XLogRecMaxBlockId(record) {
            let mut buffer: Buffer = InvalidBuffer;

            if !XLogRecHasBlockImage(record, block_id) {
                if info == XLOG_FPI {
                    elog!(ERROR, "XLOG_FPI record did not contain a full-page image");
                }
                block_id += 1;
                continue;
            }

            if XLogReadBufferForRedo(record as *mut c_void, block_id, &mut buffer) != BLK_RESTORED {
                elog!(ERROR, "unexpected XLogReadBufferForRedo result when restoring backup block");
            }
            UnlockReleaseBuffer(buffer);
            block_id += 1;
        }
    } else if info == XLOG_BACKUP_END {
        /* nothing to do here, handled in xlogrecovery_redo() */
    } else if info == XLOG_PARAMETER_CHANGE {
        let mut xlrec: xl_parameter_change = core::mem::zeroed();

        /* Update our copy of the parameters in pg_control */
        ptr::copy_nonoverlapping(
            XLogRecGetData(record) as *const u8,
            &mut xlrec as *mut xl_parameter_change as *mut u8,
            core::mem::size_of::<xl_parameter_change>(),
        );

        /*
         * Invalidate logical slots if we are in hot standby and the primary
         * does not have a WAL level sufficient for logical decoding.
         */
        if InRecovery
            && InHotStandby()
            && xlrec.wal_level < WAL_LEVEL_LOGICAL
            && wal_level >= WAL_LEVEL_LOGICAL
        {
            InvalidateObsoleteReplicationSlots(
                RS_INVAL_WAL_LEVEL as u32,
                0,
                InvalidOid,
                InvalidTransactionId,
            );
        }

        LWLockAcquire(ControlFileLock!() as *mut LWLock, LW_EXCLUSIVE);
        (*ControlFile).MaxConnections = xlrec.MaxConnections;
        (*ControlFile).max_worker_processes = xlrec.max_worker_processes;
        (*ControlFile).max_wal_senders = xlrec.max_wal_senders;
        (*ControlFile).max_prepared_xacts = xlrec.max_prepared_xacts;
        (*ControlFile).max_locks_per_xact = xlrec.max_locks_per_xact;
        (*ControlFile).wal_level = xlrec.wal_level;
        (*ControlFile).wal_log_hints = xlrec.wal_log_hints;

        /*
         * Update minRecoveryPoint to ensure that if recovery is aborted, we
         * recover back up to this point before allowing hot standby again.
         */
        if InArchiveRecovery {
            LocalMinRecoveryPoint = (*ControlFile).minRecoveryPoint;
            LocalMinRecoveryPointTLI = (*ControlFile).minRecoveryPointTLI;
        }
        if LocalMinRecoveryPoint != InvalidXLogRecPtr && LocalMinRecoveryPoint < lsn {
            let mut replayTLI_inner: TimeLineID = 0;
            let _ = GetCurrentReplayRecPtr(&mut replayTLI_inner);
            (*ControlFile).minRecoveryPoint = lsn;
            (*ControlFile).minRecoveryPointTLI = replayTLI_inner;
        }

        CommitTsParameterChange(
            xlrec.track_commit_timestamp,
            (*ControlFile).track_commit_timestamp,
        );
        (*ControlFile).track_commit_timestamp = xlrec.track_commit_timestamp;

        UpdateControlFile();
        LWLockRelease(ControlFileLock!() as *mut LWLock);

        /* Check to see if any parameter change gives a problem on recovery */
        CheckRequiredParameterValues();
    } else if info == XLOG_FPW_CHANGE {
        let mut fpw: bool = false;

        ptr::copy_nonoverlapping(
            XLogRecGetData(record) as *const u8,
            &mut fpw as *mut bool as *mut u8,
            core::mem::size_of::<bool>(),
        );

        /*
         * Update the LSN of the last replayed XLOG_FPW_CHANGE record.
         */
        if !fpw {
            SpinLockAcquire(&mut (*XLogCtl).info_lck);
            if (*XLogCtl).lastFpwDisableRecPtr < (*record).ReadRecPtr {
                (*XLogCtl).lastFpwDisableRecPtr = (*record).ReadRecPtr;
            }
            SpinLockRelease(&mut (*XLogCtl).info_lck);
        }

        /* Keep track of full_page_writes */
        lastFullPageWrites = fpw;
    } else if info == XLOG_CHECKPOINT_REDO {
        /* nothing to do here, just for informational purposes */
    }
}

/*
 * Return the extra open flags used for opening a file, depending on the
 * value of the GUCs wal_sync_method, fsync and debug_io_direct.
 */
unsafe fn get_sync_bit(method: c_int) -> c_int {
    let mut o_direct_flag: c_int = 0;

    /*
     * Use O_DIRECT if requested, except in walreceiver process.
     */
    if (io_direct_flags & IO_DIRECT_WAL) != 0 && !AmWalReceiverProcess() {
        o_direct_flag = PG_O_DIRECT;
    }

    /* If fsync is disabled, never open in sync mode */
    if !enableFsync {
        return o_direct_flag;
    }

    match method {
        /*
         * enum values for all sync options are defined even if they are
         * not supported on the current platform.
         */
        WAL_SYNC_METHOD_FSYNC
        | WAL_SYNC_METHOD_FSYNC_WRITETHROUGH
        | WAL_SYNC_METHOD_FDATASYNC => o_direct_flag,
        WAL_SYNC_METHOD_OPEN => {
            #[cfg(target_os = "linux")]
            { libc::O_SYNC | o_direct_flag }
            #[cfg(not(target_os = "linux"))]
            { o_direct_flag }
        }
        WAL_SYNC_METHOD_OPEN_DSYNC => {
            #[cfg(target_os = "linux")]
            { libc::O_DSYNC | o_direct_flag }
            #[cfg(not(target_os = "linux"))]
            { o_direct_flag }
        }
        _ => {
            /* can't happen (unless we are out of sync with option array) */
            elog!(ERROR, "unrecognized \"wal_sync_method\": {}", method);
            0 /* silence warning */
        }
    }
}

/*
 * GUC support
 */
pub unsafe fn assign_wal_sync_method(new_wal_sync_method: c_int, _extra: *mut c_void) {
    if wal_sync_method != new_wal_sync_method {
        /*
         * To ensure that no blocks escape unsynced, force an fsync on the
         * currently open log segment (if any).  Also, if the open flag is
         * changing, close the log file so it will be reopened at next use.
         */
        if openLogFile >= 0 {
            pgstat_report_wait_start(WAIT_EVENT_WAL_SYNC_METHOD_ASSIGN);
            if pg_fsync(openLogFile) != 0 {
                let mut xlogfname: [c_char; MAXFNAMELEN] = [0; MAXFNAMELEN];
                let save_errno = *libc::__error();
                XLogFileName(
                    xlogfname.as_mut_ptr(),
                    openLogTLI,
                    openLogSegNo,
                    wal_segment_size,
                );
                *libc::__error() = save_errno;
                ereport!(
                    PANIC,
                    errmsg!(
                        "could not fsync file \"{}\": {}",
                        core::ffi::CStr::from_ptr(xlogfname.as_ptr()).to_string_lossy(),
                        strerror_r()
                    )
                );
                /* errcode_for_file_access */
            }
            pgstat_report_wait_end();
            if get_sync_bit(wal_sync_method) != get_sync_bit(new_wal_sync_method) {
                XLogFileClose();
            }
        }
    }
}


/*
 * Issue appropriate kind of fsync (if any) for an XLOG output file.
 *
 * 'fd' is a file descriptor for the XLOG file to be fsync'd.
 * 'segno' is for error reporting purposes.
 */
pub unsafe fn issue_xlog_fsync(fd: c_int, segno: XLogSegNo, tli: TimeLineID) {
    let mut msg: *const c_char = ptr::null();
    let start: instr_time;

    assert!(tli != 0);

    /*
     * Quick exit if fsync is disabled or write() has already synced the WAL file.
     */
    if !enableFsync
        || wal_sync_method == WAL_SYNC_METHOD_OPEN
        || wal_sync_method == WAL_SYNC_METHOD_OPEN_DSYNC
    {
        return;
    }

    /*
     * Measure I/O timing to sync the WAL file for pg_stat_io.
     */
    let start = pgstat_prepare_io_time(track_wal_io_timing);

    pgstat_report_wait_start(WAIT_EVENT_WAL_SYNC);
    match wal_sync_method {
        WAL_SYNC_METHOD_FSYNC => {
            if pg_fsync_no_writethrough(fd) != 0 {
                msg = b"could not fsync file \"%s\": %m\0".as_ptr() as *const c_char;
            }
        }
        WAL_SYNC_METHOD_FSYNC_WRITETHROUGH => {
            if pg_fsync_writethrough(fd) != 0 {
                msg = b"could not fsync write-through file \"%s\": %m\0".as_ptr() as *const c_char;
            }
        }
        WAL_SYNC_METHOD_FDATASYNC => {
            if pg_fdatasync(fd) != 0 {
                msg = b"could not fdatasync file \"%s\": %m\0".as_ptr() as *const c_char;
            }
        }
        WAL_SYNC_METHOD_OPEN | WAL_SYNC_METHOD_OPEN_DSYNC => {
            /* not reachable */
            assert!(false);
        }
        _ => {
            ereport!(
                PANIC,
                errmsg!("unrecognized \"wal_sync_method\": {}", wal_sync_method)
            );
        }
    }

    /* PANIC if failed to fsync */
    if !msg.is_null() {
        let mut xlogfname: [c_char; MAXFNAMELEN] = [0; MAXFNAMELEN];
        let save_errno = *libc::__error();
        XLogFileName(xlogfname.as_mut_ptr(), tli, segno, wal_segment_size);
        *libc::__error() = save_errno;
        ereport!(
            PANIC,
            errmsg!(
                "{}: {}",
                core::ffi::CStr::from_ptr(msg).to_string_lossy(),
                core::ffi::CStr::from_ptr(xlogfname.as_ptr()).to_string_lossy()
            )
        );
    }

    pgstat_report_wait_end();

    pgstat_count_io_op_time(IOOBJECT_WAL, IOCONTEXT_NORMAL, IOOP_FSYNC, start, 1, 0);
}

/*
 * do_pg_backup_start is the workhorse of the user-visible pg_backup_start()
 * function. It creates the necessary starting checkpoint and constructs the
 * backup state and tablespace map.
 *
 * Input parameters are "state" (the backup state), "fast" (if true, we do
 * the checkpoint in immediate mode to make it faster), and "tablespaces"
 * (if non-NULL, indicates a list of tablespaceinfo structs describing the
 * cluster's tablespaces.).
 *
 * The tablespace map contents are appended to passed-in parameter
 * tablespace_map and the caller is responsible for including it in the backup
 * archive as 'tablespace_map'. The tablespace_map file is required mainly for
 * tar format in windows as native windows utilities are not able to create
 * symlinks while extracting files from tar. However for consistency and
 * platform-independence, we do it the same way everywhere.
 *
 * It fills in "state" with the information required for the backup, such
 * as the minimum WAL location that must be present to restore from this
 * backup (starttli) and the corresponding timeline ID (starttli).
 *
 * Every successfully started backup must be stopped by calling
 * do_pg_backup_stop() or do_pg_abort_backup(). There can be many
 * backups active at the same time.
 *
 * It is the responsibility of the caller of this function to verify the
 * permissions of the calling user!
 */
pub unsafe fn do_pg_backup_start(
    backupidstr: *const c_char,
    fast: bool,
    tablespaces: *mut *mut List,
    state: *mut BackupState,
    tblspcmapfile: *mut StringInfoData,
) {
    let mut backup_started_in_recovery: bool;

    assert!(!state.is_null());
    backup_started_in_recovery = RecoveryInProgress();

    /*
     * During recovery, we don't need to check WAL level. Because, if WAL
     * level is not sufficient, it's impossible to get here during recovery.
     */
    if !backup_started_in_recovery && !XLogIsNeeded() {
        ereport!(
            ERROR,
            errmsg!("WAL level not sufficient for making an online backup")
        );
    }

    if libc::strlen(backupidstr) > MAXPGPATH {
        ereport!(
            ERROR,
            errmsg!("backup label too long (max {} bytes)", MAXPGPATH)
        );
    }

    strlcpy(
        (*state).name.as_mut_ptr(),
        backupidstr,
        core::mem::size_of_val(&(*state).name),
    );

    /*
     * Mark backup active in shared memory.  We must do full-page WAL writes
     * during an on-line backup even if not doing so at other times, because
     * it's quite possible for the backup dump to obtain a "torn" (partially
     * written) copy of a database page if it reads the page concurrently with
     * our write to the same page.  This can be fixed as long as the first
     * write to the page in the WAL sequence is a full-page write. Hence, we
     * increment runningBackups then force a CHECKPOINT, to ensure there are
     * no dirty pages in shared memory that might get dumped while the backup
     * is in progress without having a corresponding WAL record.  (Once the
     * backup is complete, we need not force full-page writes anymore, since
     * we expect that any pages not modified during the backup interval must
     * have been correctly captured by the backup.)
     *
     * Note that forcing full-page writes has no effect during an online
     * backup from the standby.
     *
     * We must hold all the insertion locks to change the value of
     * runningBackups, to ensure adequate interlocking against
     * XLogInsertRecord().
     */
    WALInsertLockAcquireExclusive();
    (*XLogCtl).Insert.runningBackups += 1;
    WALInsertLockRelease();

    /*
     * Ensure we decrement runningBackups if we fail below. NB -- for this to
     * work correctly, it is critical that sessionBackupState is only updated
     * after this block is over.
     */
    PG_ENSURE_ERROR_CLEANUP!(do_pg_abort_backup, DatumGetBool(true));
    {
        let mut gotUniqueStartpoint: bool = false;
        let mut tblspcdir: *mut DIR;
        let mut de: *mut dirent;
        let mut ti: *mut tablespaceinfo;
        let datadirpathlen: usize;

        /*
         * Force an XLOG file switch before the checkpoint, to ensure that the
         * WAL segment the checkpoint is written to doesn't contain pages with
         * old timeline IDs.  That would otherwise happen if you called
         * pg_backup_start() right after restoring from a PITR archive: the
         * first WAL segment containing the startup checkpoint has pages in
         * the beginning with the old timeline ID.  That can cause trouble at
         * recovery: we won't have a history file covering the old timeline if
         * pg_wal directory was not included in the base backup and the WAL
         * archive was cleared too before starting the backup.
         *
         * This also ensures that we have emitted a WAL page header that has
         * XLP_BKP_REMOVABLE off before we emit the checkpoint record.
         * Therefore, if a WAL archiver (such as pglesslog) is trying to
         * compress out removable backup blocks, it won't remove any that
         * occur after this point.
         *
         * During recovery, we skip forcing XLOG file switch, which means that
         * the backup taken during recovery is not available for the special
         * recovery case described above.
         */
        if !backup_started_in_recovery {
            RequestXLogSwitch(false);
        }

        loop {
            let mut checkpointfpw: bool;

            /*
             * Force a CHECKPOINT.  Aside from being necessary to prevent torn
             * page problems, this guarantees that two successive backup runs
             * will have different checkpoint positions and hence different
             * history file names, even if nothing happened in between.
             *
             * During recovery, establish a restartpoint if possible. We use
             * the last restartpoint as the backup starting checkpoint. This
             * means that two successive backup runs can have same checkpoint
             * positions.
             *
             * Since the fact that we are executing do_pg_backup_start()
             * during recovery means that checkpointer is running, we can use
             * RequestCheckpoint() to establish a restartpoint.
             *
             * We use CHECKPOINT_IMMEDIATE only if requested by user (via
             * passing fast = true).  Otherwise this can take awhile.
             */
            RequestCheckpoint(
                CHECKPOINT_FORCE
                    | CHECKPOINT_WAIT
                    | (if fast { CHECKPOINT_IMMEDIATE } else { 0 }),
            );

            /*
             * Now we need to fetch the checkpoint record location, and also
             * its REDO pointer.  The oldest point in WAL that would be needed
             * to restore starting from the checkpoint is precisely the REDO
             * pointer.
             */
            LWLockAcquire(ControlFileLock!(), LW_SHARED);
            (*state).checkpointloc = (*ControlFile).checkPoint;
            (*state).startpoint = (*ControlFile).checkPointCopy.redo;
            (*state).starttli = (*ControlFile).checkPointCopy.ThisTimeLineID;
            checkpointfpw = (*ControlFile).checkPointCopy.fullPageWrites;
            LWLockRelease(ControlFileLock!());

            if backup_started_in_recovery {
                let mut recptr: XLogRecPtr;

                /*
                 * Check to see if all WAL replayed during online backup
                 * (i.e., since last restartpoint used as backup starting
                 * checkpoint) contain full-page writes.
                 */
                SpinLockAcquire(&mut (*XLogCtl).info_lck);
                recptr = (*XLogCtl).lastFpwDisableRecPtr;
                SpinLockRelease(&mut (*XLogCtl).info_lck);

                if !checkpointfpw || (*state).startpoint <= recptr {
                    ereport!(
                        ERROR,
                        errmsg!("WAL generated with \"full_page_writes=off\" was replayed since last restartpoint")
                    );
                }

                /*
                 * During recovery, since we don't use the end-of-backup WAL
                 * record and don't write the backup history file, the
                 * starting WAL location doesn't need to be unique. This means
                 * that two successive backup runs can have same checkpoint
                 * positions.
                 */
                gotUniqueStartpoint = true;
            }

            /*
             * If two base backups are started at the same time (in WAL sender
             * processes), we need to make sure that they use different
             * checkpoints as starting locations, because we use the starting
             * WAL location as a unique identifier for the base backup in the
             * end-of-backup WAL record and when we write the backup history
             * file. Perhaps it would be better generate a separate unique ID
             * for each backup instead of forcing another checkpoint, but
             * taking a checkpoint right after another is not that expensive
             * either because only few buffers have been dirtied yet.
             */
            WALInsertLockAcquireExclusive();
            if (*XLogCtl).Insert.lastBackupStart < (*state).startpoint {
                (*XLogCtl).Insert.lastBackupStart = (*state).startpoint;
                gotUniqueStartpoint = true;
            }
            WALInsertLockRelease();

            if gotUniqueStartpoint {
                break;
            }
        } /* loop */

        /*
         * Construct tablespace_map file.
         */
        datadirpathlen = libc::strlen(DataDir);

        /* Collect information about all tablespaces */
        tblspcdir = AllocateDir(PG_TBLSPC_DIR as *const c_char);
        loop {
            de = ReadDir(tblspcdir, PG_TBLSPC_DIR as *const c_char);
            if de.is_null() {
                break;
            }
            let mut fullpath: [c_char; MAXPGPATH + 10] =
                [0; MAXPGPATH + 10];
            let mut linkpath: [c_char; MAXPGPATH] = [0; MAXPGPATH];
            let mut relpath: *mut c_char = ptr::null_mut();
            let mut de_type: PGFileType;
            let mut badp: *mut c_char = core::ptr::null_mut();
            let mut tsoid: Oid;

            /*
             * Try to parse the directory name as an unsigned integer.
             *
             * Tablespace directories should be positive integers that can be
             * represented in 32 bits, with no leading zeroes or trailing
             * garbage. If we come across a name that doesn't meet those
             * criteria, skip it.
             */
            if (*de).d_name[0] < b'1' as c_char || (*de).d_name[1] > b'9' as c_char {
                continue;
            }
            *libc::__error() = 0;
            tsoid = libc::strtoul((*de).d_name.as_ptr(), &mut badp, 10) as Oid;
            if *badp != 0 || *libc::__error() == libc::EINVAL || *libc::__error() == libc::ERANGE {
                continue;
            }

            libc::snprintf(
                fullpath.as_mut_ptr(),
                fullpath.len(),
                b"%s/%s\0".as_ptr() as *const c_char,
                PG_TBLSPC_DIR,
                (*de).d_name.as_ptr(),
            );

            de_type = get_dirent_type(fullpath.as_ptr(), de, false, ERROR);

            if de_type == PGFILETYPE_LNK {
                let mut escapedpath: StringInfoData = core::mem::zeroed();
                let mut rllen: c_int;
                let mut s: *mut c_char;

                rllen = libc::readlink(
                    fullpath.as_ptr(),
                    linkpath.as_mut_ptr(),
                    linkpath.len(),
                ) as c_int;
                if rllen < 0 {
                    ereport!(
                        WARNING,
                        errmsg!("could not read symbolic link \"{}\": {}",
                            core::ffi::CStr::from_ptr(fullpath.as_ptr()).to_string_lossy(),
                            core::ffi::CStr::from_ptr(libc::strerror(*libc::__error())).to_string_lossy()
                        )
                    );
                    continue;
                } else if rllen >= linkpath.len() as c_int {
                    ereport!(
                        WARNING,
                        errmsg!("symbolic link \"{}\" target is too long",
                            core::ffi::CStr::from_ptr(fullpath.as_ptr()).to_string_lossy()
                        )
                    );
                    continue;
                }
                linkpath[rllen as usize] = 0;

                /*
                 * Relpath holds the relative path of the tablespace directory
                 * when it's located within PGDATA, or NULL if it's located
                 * elsewhere.
                 */
                if rllen as usize > datadirpathlen
                    && libc::strncmp(linkpath.as_ptr(), DataDir, datadirpathlen) == 0
                    && IS_DIR_SEP!(linkpath[datadirpathlen] as u8)
                {
                    relpath = pstrdup(linkpath.as_ptr().add(datadirpathlen + 1));
                }

                /*
                 * Add a backslash-escaped version of the link path to the
                 * tablespace map file.
                 */
                initStringInfo(&mut escapedpath);
                s = linkpath.as_mut_ptr();
                while *s != 0 {
                    if *s == b'\n' as c_char || *s == b'\r' as c_char || *s == b'\\' as c_char {
                        appendStringInfoChar(&mut escapedpath, b'\\' as c_char);
                    }
                    appendStringInfoChar(&mut escapedpath, *s);
                    s = s.add(1);
                }
                appendStringInfo!(
                    tblspcmapfile,
                    "{} {}\n",
                    core::ffi::CStr::from_ptr((*de).d_name.as_ptr() as *const c_char).to_string_lossy(),
                    core::ffi::CStr::from_ptr(escapedpath.data as *const c_char).to_string_lossy()
                );
                pfree(escapedpath.data as *mut c_void);
            } else if de_type == PGFILETYPE_DIR {
                /*
                 * It's possible to use allow_in_place_tablespaces to create
                 * directories directly under pg_tblspc, for testing purposes
                 * only.
                 *
                 * In this case, we store a relative path rather than an
                 * absolute path into the tablespaceinfo.
                 */
                libc::snprintf(
                    linkpath.as_mut_ptr(),
                    linkpath.len(),
                    b"%s/%s\0".as_ptr() as *const c_char,
                    PG_TBLSPC_DIR,
                    (*de).d_name.as_ptr(),
                );
                relpath = pstrdup(linkpath.as_ptr());
            } else {
                /* Skip any other file type that appears here. */
                continue;
            }

            ti = palloc(core::mem::size_of::<tablespaceinfo>()) as *mut tablespaceinfo;
            (*ti).oid = tsoid;
            (*ti).path = pstrdup(linkpath.as_ptr());
            (*ti).rpath = relpath;
            (*ti).size = -1;

            if !tablespaces.is_null() {
                *tablespaces = lappend(*tablespaces, ti as *mut c_void);
            }
        } /* loop ReadDir */
        FreeDir(tblspcdir);

        (*state).starttime = libc::time(ptr::null_mut()) as pg_time_t;
    }
    PG_END_ENSURE_ERROR_CLEANUP!(do_pg_abort_backup, DatumGetBool(true));

    (*state).started_in_recovery = backup_started_in_recovery;

    /*
     * Mark that the start phase has correctly finished for the backup.
     */
    sessionBackupState = SESSION_BACKUP_RUNNING;
}

/*
 * Utility routine to fetch the session-level status of a backup running.
 */
pub unsafe fn get_backup_status() -> SessionBackupState {
    sessionBackupState
}

/*
 * do_pg_backup_stop
 *
 * Utility function called at the end of an online backup.  It creates history
 * file (if required), resets sessionBackupState and so on.  It can optionally
 * wait for WAL segments to be archived.
 *
 * "state" is filled with the information necessary to restore from this
 * backup with its stop LSN (stoppoint), its timeline ID (stoptli), etc.
 *
 * It is the responsibility of the caller of this function to verify the
 * permissions of the calling user!
 */
pub unsafe fn do_pg_backup_stop(state: *mut BackupState, waitforarchive: bool) {
    let mut backup_stopped_in_recovery: bool = false;
    let mut histfilepath: [c_char; MAXPGPATH] = [0; MAXPGPATH];
    let mut lastxlogfilename: [c_char; MAXFNAMELEN] = [0; MAXFNAMELEN];
    let mut histfilename: [c_char; MAXFNAMELEN] = [0; MAXFNAMELEN];
    let mut _logSegNo: XLogSegNo = 0;
    let mut fp: *mut FILE;
    let mut seconds_before_warning: c_int;
    let mut waits: c_int = 0;
    let mut reported_waiting: bool = false;

    assert!(!state.is_null());

    backup_stopped_in_recovery = RecoveryInProgress();

    /*
     * During recovery, we don't need to check WAL level. Because, if WAL
     * level is not sufficient, it's impossible to get here during recovery.
     */
    if !backup_stopped_in_recovery && !XLogIsNeeded() {
        ereport!(
            ERROR,
            errmsg!("WAL level not sufficient for making an online backup")
        );
    }

    /*
     * OK to update backup counter and session-level lock.
     *
     * Note that CHECK_FOR_INTERRUPTS() must not occur while updating them,
     * otherwise they can be updated inconsistently, which might cause
     * do_pg_abort_backup() to fail.
     */
    WALInsertLockAcquireExclusive();

    /*
     * It is expected that each do_pg_backup_start() call is matched by
     * exactly one do_pg_backup_stop() call.
     */
    assert!((*XLogCtl).Insert.runningBackups > 0);
    (*XLogCtl).Insert.runningBackups -= 1;

    /*
     * Clean up session-level lock.
     *
     * You might think that WALInsertLockRelease() can be called before
     * cleaning up session-level lock because session-level lock doesn't need
     * to be protected with WAL insertion lock. But since
     * CHECK_FOR_INTERRUPTS() can occur in it, session-level lock must be
     * cleaned up before it.
     */
    sessionBackupState = SESSION_BACKUP_NONE;

    WALInsertLockRelease();

    /*
     * If we are taking an online backup from the standby, we confirm that the
     * standby has not been promoted during the backup.
     */
    if (*state).started_in_recovery && !backup_stopped_in_recovery {
        ereport!(
            ERROR,
            errmsg!("the standby was promoted during online backup")
        );
    }

    /*
     * During recovery, we don't write an end-of-backup record. We assume that
     * pg_control was backed up last and its minimum recovery point can be
     * available as the backup end location. Since we don't have an
     * end-of-backup record, we use the pg_control value to check whether
     * we've reached the end of backup when starting recovery from this
     * backup. We have no way of checking if pg_control wasn't backed up last
     * however.
     *
     * We don't force a switch to new WAL file but it is still possible to
     * wait for all the required files to be archived if waitforarchive is
     * true. This is okay if we use the backup to start a standby and fetch
     * the missing WAL using streaming replication. But in the case of an
     * archive recovery, a user should set waitforarchive to true and wait for
     * them to be archived to ensure that all the required files are
     * available.
     *
     * We return the current minimum recovery point as the backup end
     * location. Note that it can be greater than the exact backup end
     * location if the minimum recovery point is updated after the backup of
     * pg_control. This is harmless for current uses.
     *
     * XXX currently a backup history file is for informational and debug
     * purposes only. It's not essential for an online backup. Furthermore,
     * even if it's created, it will not be archived during recovery because
     * an archiver is not invoked. So it doesn't seem worthwhile to write a
     * backup history file during recovery.
     */
    if backup_stopped_in_recovery {
        let mut recptr: XLogRecPtr;

        /*
         * Check to see if all WAL replayed during online backup contain
         * full-page writes.
         */
        SpinLockAcquire(&mut (*XLogCtl).info_lck);
        recptr = (*XLogCtl).lastFpwDisableRecPtr;
        SpinLockRelease(&mut (*XLogCtl).info_lck);

        if (*state).startpoint <= recptr {
            ereport!(
                ERROR,
                errmsg!("WAL generated with \"full_page_writes=off\" was replayed during online backup")
            );
        }

        LWLockAcquire(ControlFileLock!(), LW_SHARED);
        (*state).stoppoint = (*ControlFile).minRecoveryPoint;
        (*state).stoptli = (*ControlFile).minRecoveryPointTLI;
        LWLockRelease(ControlFileLock!());
    } else {
        let mut history_file: *mut c_char;

        /*
         * Write the backup-end xlog record
         */
        XLogBeginInsert();
        XLogRegisterData(
            &mut (*state).startpoint as *mut XLogRecPtr as *const c_void,
            core::mem::size_of::<XLogRecPtr>() as u32,
        );
        (*state).stoppoint = XLogInsert(RM_XLOG_ID, XLOG_BACKUP_END);

        /*
         * Given that we're not in recovery, InsertTimeLineID is set and can't
         * change, so we can read it without a lock.
         */
        (*state).stoptli = (*XLogCtl).InsertTimeLineID;

        /*
         * Force a switch to a new xlog segment file, so that the backup is
         * valid as soon as archiver moves out the current segment file.
         */
        RequestXLogSwitch(false);

        (*state).stoptime = libc::time(ptr::null_mut()) as pg_time_t;

        /*
         * Write the backup history file
         */
        XLByteToSeg((*state).startpoint, &mut _logSegNo, wal_segment_size);
        BackupHistoryFilePath(
            histfilepath.as_mut_ptr(),
            (*state).stoptli,
            _logSegNo,
            (*state).startpoint,
            wal_segment_size,
        );
        fp = AllocateFile(
            histfilepath.as_ptr(),
            b"w\0".as_ptr() as *const c_char,
        ) as *mut libc::FILE;
        if fp.is_null() {
            ereport!(
                ERROR,
                errmsg!("could not create file \"{}\": {}",
                    core::ffi::CStr::from_ptr(histfilepath.as_ptr()).to_string_lossy(),
                    core::ffi::CStr::from_ptr(libc::strerror(*libc::__error())).to_string_lossy()
                )
            );
        }

        /* Build and save the contents of the backup history file */
        history_file = build_backup_content(state, true);
        libc::fprintf(fp, b"%s\0".as_ptr() as *const c_char, history_file);
        pfree(history_file as *mut c_void);

        if libc::fflush(fp) != 0 || libc::ferror(fp) != 0 || FreeFile(fp as *mut c_void) != 0 {
            ereport!(
                ERROR,
                errmsg!("could not write file \"{}\"",
                    core::ffi::CStr::from_ptr(histfilepath.as_ptr()).to_string_lossy()
                )
            );
        }

        /*
         * Clean out any no-longer-needed history files.  As a side effect,
         * this will post a .ready file for the newly created history file,
         * notifying the archiver that history file may be archived
         * immediately.
         */
        CleanupBackupHistory();
    }

    /*
     * If archiving is enabled, wait for all the required WAL files to be
     * archived before returning. If archiving isn't enabled, the required WAL
     * needs to be transported via streaming replication (hopefully with
     * wal_keep_size set high enough), or some more exotic mechanism like
     * polling and copying files from pg_wal with script. We have no knowledge
     * of those mechanisms, so it's up to the user to ensure that he gets all
     * the required WAL.
     *
     * We wait until both the last WAL file filled during backup and the
     * history file have been archived, and assume that the alphabetic sorting
     * property of the WAL files ensures any earlier WAL files are safely
     * archived as well.
     *
     * We wait forever, since archive_command is supposed to work and we
     * assume the admin wanted his backup to work completely. If you don't
     * wish to wait, then either waitforarchive should be passed in as false,
     * or you can set statement_timeout.  Also, some notices are issued to
     * clue in anyone who might be doing this interactively.
     */

    if waitforarchive
        && ((!backup_stopped_in_recovery && XLogArchivingActive())
            || (backup_stopped_in_recovery && XLogArchivingAlways()))
    {
        XLByteToPrevSeg((*state).stoppoint, &mut _logSegNo, wal_segment_size);
        XLogFileName(
            lastxlogfilename.as_mut_ptr(),
            (*state).stoptli,
            _logSegNo,
            wal_segment_size,
        );

        XLByteToSeg((*state).startpoint, &mut _logSegNo, wal_segment_size);
        BackupHistoryFileName(
            histfilename.as_mut_ptr(),
            (*state).stoptli,
            _logSegNo,
            (*state).startpoint,
            wal_segment_size,
        );

        seconds_before_warning = 60;
        waits = 0;

        while XLogArchiveIsBusy(lastxlogfilename.as_ptr())
            || XLogArchiveIsBusy(histfilename.as_ptr())
        {
            CHECK_FOR_INTERRUPTS!();

            if !reported_waiting && waits > 5 {
                ereport!(
                    NOTICE,
                    errmsg!("base backup done, waiting for required WAL segments to be archived")
                );
                reported_waiting = true;
            }

            let _ = WaitLatch(
                MyLatch as *mut crate::storage::ipc::latch::Latch,
                WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
                1000,
                WAIT_EVENT_BACKUP_WAIT_WAL_ARCHIVE,
            );
            ResetLatch(MyLatch as *mut crate::storage::ipc::latch::Latch);

            waits += 1;
            if waits >= seconds_before_warning {
                seconds_before_warning *= 2; /* This wraps in >10 years... */
                ereport!(
                    WARNING,
                    errmsg!("still waiting for all required WAL segments to be archived ({} seconds elapsed)",
                        waits
                    )
                );
            }
        }

        ereport!(
            NOTICE,
            errmsg!("all required WAL segments have been archived")
        );
    } else if waitforarchive {
        ereport!(
            NOTICE,
            errmsg!("WAL archiving is not enabled; you must ensure that all required WAL segments are copied through other means to complete the backup")
        );
    }
}

/*
 * do_pg_abort_backup: abort a running backup
 *
 * This does just the most basic steps of do_pg_backup_stop(), by taking the
 * system out of backup mode, thus making it a lot more safe to call from
 * an error handler.
 *
 * 'arg' indicates that it's being called during backup setup; so
 * sessionBackupState has not been modified yet, but runningBackups has
 * already been incremented.  When it's false, then it's invoked as a
 * before_shmem_exit handler, and therefore we must not change state
 * unless sessionBackupState indicates that a backup is actually running.
 *
 * NB: This gets used as a PG_ENSURE_ERROR_CLEANUP callback and
 * before_shmem_exit handler, hence the odd-looking signature.
 */
pub unsafe extern "C" fn do_pg_abort_backup(code: c_int, arg: Datum) {
    let during_backup_start: bool = DatumGetBool(arg);

    /* If called during backup start, there shouldn't be one already running */
    assert!(!during_backup_start || sessionBackupState == SESSION_BACKUP_NONE);

    if during_backup_start || sessionBackupState != SESSION_BACKUP_NONE {
        WALInsertLockAcquireExclusive();
        assert!((*XLogCtl).Insert.runningBackups > 0);
        (*XLogCtl).Insert.runningBackups -= 1;

        sessionBackupState = SESSION_BACKUP_NONE;
        WALInsertLockRelease();

        if !during_backup_start {
            ereport!(
                WARNING,
                errmsg!("aborting backup due to backend exiting before pg_backup_stop was called")
            );
        }
    }
}

/*
 * Register a handler that will warn about unterminated backups at end of
 * session, unless this has already been done.
 */
pub unsafe fn register_persistent_abort_backup_handler() {
    static mut already_done: bool = false;

    if already_done {
        return;
    }
    before_shmem_exit(do_pg_abort_backup, crate::postgres::BoolGetDatum(false));
    already_done = true;
}

/*
 * Get latest WAL insert pointer
 */
pub unsafe fn GetXLogInsertRecPtr() -> XLogRecPtr {
    let Insert: *mut XLogCtlInsert = &mut (*XLogCtl).Insert;
    let current_bytepos: uint64;

    SpinLockAcquire(&mut (*Insert).insertpos_lck);
    current_bytepos = (*Insert).CurrBytePos;
    SpinLockRelease(&mut (*Insert).insertpos_lck);

    XLogBytePosToRecPtr(current_bytepos)
}

/*
 * Get latest WAL write pointer
 */
pub unsafe fn GetXLogWriteRecPtr() -> XLogRecPtr {
    RefreshXLogWriteResult!(LogwrtResult);

    LogwrtResult.Write
}

/*
 * Returns the redo pointer of the last checkpoint or restartpoint. This is
 * the oldest point in WAL that we still need, if we have to restart recovery.
 */
pub unsafe fn GetOldestRestartPoint(oldrecptr: *mut XLogRecPtr, oldtli: *mut TimeLineID) {
    LWLockAcquire(ControlFileLock!(), LW_SHARED);
    *oldrecptr = (*ControlFile).checkPointCopy.redo;
    *oldtli = (*ControlFile).checkPointCopy.ThisTimeLineID;
    LWLockRelease(ControlFileLock!());
}

/* Thin wrapper around ShutdownWalRcv(). */
pub unsafe fn XLogShutdownWalRcv() {
    ShutdownWalRcv();
    ResetInstallXLogFileSegmentActive();
}

/* Enable WAL file recycling and preallocation. */
pub unsafe fn SetInstallXLogFileSegmentActive() {
    LWLockAcquire(ControlFileLock!(), LW_EXCLUSIVE);
    (*XLogCtl).InstallXLogFileSegmentActive = true;
    LWLockRelease(ControlFileLock!());
}

/* Disable WAL file recycling and preallocation. */
pub unsafe fn ResetInstallXLogFileSegmentActive() {
    LWLockAcquire(ControlFileLock!(), LW_EXCLUSIVE);
    (*XLogCtl).InstallXLogFileSegmentActive = false;
    LWLockRelease(ControlFileLock!());
}

pub unsafe fn IsInstallXLogFileSegmentActive() -> bool {
    let mut result: bool;

    LWLockAcquire(ControlFileLock!(), LW_SHARED);
    result = (*XLogCtl).InstallXLogFileSegmentActive;
    LWLockRelease(ControlFileLock!());

    result
}

/*
 * Update the WalWriterSleeping flag.
 */
pub unsafe fn SetWalWriterSleeping(sleeping: bool) {
    SpinLockAcquire(&mut (*XLogCtl).info_lck);
    (*XLogCtl).WalWriterSleeping = sleeping;
    SpinLockRelease(&mut (*XLogCtl).info_lck);
}
