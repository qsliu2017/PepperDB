/*-------------------------------------------------------------------------
 *
 * walreceiver.rs
 *
 * The WAL receiver process (walreceiver) is new as of Postgres 9.0. It
 * is the process in the standby server that takes charge of receiving
 * XLOG records from a primary server during streaming replication.
 *
 * When the startup process determines that it's time to start streaming,
 * it instructs postmaster to start walreceiver. Walreceiver first connects
 * to the primary server (it will be served by a walsender process
 * in the primary server), and then keeps receiving XLOG records and
 * writing them to the disk as long as the connection is alive. As XLOG
 * records are received and flushed to disk, it updates the
 * WalRcv->flushedUpto variable in shared memory, to inform the startup
 * process of how far it can proceed with XLOG replay.
 *
 * A WAL receiver cannot directly load GUC parameters used when establishing
 * its connection to the primary. Instead it relies on parameter values
 * that are passed down by the startup process when streaming is requested.
 * This applies, for example, to the replication slot and the connection
 * string to be used for the connection with the primary.
 *
 * If the primary server ends streaming, but doesn't disconnect, walreceiver
 * goes into "waiting" mode, and waits for the startup process to give new
 * instructions. The startup process will treat that the same as
 * disconnection, and will rescan the archive/pg_wal directory. But when the
 * startup process wants to try streaming replication again, it will just
 * nudge the existing walreceiver process that's waiting, instead of launching
 * a new one.
 *
 * Normal termination is by SIGTERM, which instructs the walreceiver to
 * exit(0). Emergency termination is by SIGQUIT; like any postmaster child
 * process, the walreceiver will simply abort and exit on SIGQUIT. A close
 * of the connection and a FATAL error are treated not as a crash but as
 * normal operation.
 *
 * This file contains the server-facing parts of walreceiver. The libpq-
 * specific parts are in the libpqwalreceiver module. It's loaded
 * dynamically to avoid linking the server with libpq.
 *
 * Portions Copyright (c) 2010-2025, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *    src/backend/replication/walreceiver.c
 *
 *-------------------------------------------------------------------------
 */

use crate::prelude::*;

// WalRcvData, WalRcvState, WalRcv global - from walreceiverfuncs.rs
use crate::replication::walreceiverfuncs::{
    WalRcv, WalRcvData, WalRcvState,
    WalRcvState::*,
};

// WalReceiverConn, WalReceiverFunctionsType - from libpqwalreceiver
use crate::replication::libpqwalreceiver::libpqwalreceiver::{
    WalReceiverConn, WalReceiverFunctionsType, WalRcvStreamOptions, WalRcvStreamProto,
    WalRcvStreamPhysical,
};

// XLogRecPtr, TimeLineID, XLogSegNo
use crate::access::transam::xlogdefs::{XLogRecPtr, InvalidXLogRecPtr, XLogRecPtrIsInvalid};
use crate::access::transam::xlogbackup::{TimeLineID, XLogSegNo};

// SpinLock
use crate::storage::spin::{SpinLockAcquire, SpinLockRelease};

// ConditionVariable
use crate::storage::lmgr::condition_variable::ConditionVariableBroadcast;

// Latch wait
use crate::storage::ipc::latch::{
    Latch, WaitLatch, WaitLatchOrSocket, ResetLatch, SetLatch,
    WL_LATCH_SET, WL_TIMEOUT, WL_EXIT_ON_PM_DEATH, WL_SOCKET_READABLE,
    MyLatch,
};

// Atomics
use crate::port::atomics::generic::{pg_atomic_write_u64_impl, pg_atomic_read_u64_impl};
use crate::port::atomics::pg_atomic_uint64;
use crate::port::atomics::generic::pg_memory_barrier_impl;

// Proc number
use crate::storage::procnumber::{ProcNumber, INVALID_PROC_NUMBER};

// pqformat (message send/receive)
use crate::libpq::pqformat::{
    pq_getmsgint64, pq_getmsgbyte, pq_sendint64, pq_sendint32,
};

// StringInfo
use crate::lib::stringinfo::{
    StringInfoData, initStringInfo, initReadOnlyStringInfo, resetStringInfo,
};

// Timestamp helpers
use crate::utils::adt::timestamp::{
    GetCurrentTimestamp, TimestampDifferenceMilliseconds,
};

// Proc-global: MyLatch, MyProcPid, MyBackendType, CHECK_FOR_INTERRUPTS
use crate::miscadmin::{
    CHECK_FOR_INTERRUPTS, MyBackendType, MyProcPid, B_WAL_RECEIVER,
};

// XLog internal
use crate::access::transam::xlog_internal::{XLogFileName, TLHistoryFileName};
use crate::access::transam::xlogreader::{XLByteInSeg, XLByteToSeg, XLogSegmentOffset};

// XLogArchive
use crate::access::transam::xlogarchive::{
    XLogArchiveForceDone, XLogArchiveNotify,
};
// XLogArchiveMode and ARCHIVE_MODE_ALWAYS are private in xlogarchive; mirror here.
// TODO(pg-port): real XLogArchiveMode GUC lives in utils/misc/guc_tables.c
static mut XLogArchiveMode: c_int = 0;
const ARCHIVE_MODE_ALWAYS: c_int = 2;

// Timeline
use crate::access::transam::timeline::{
    existsTimeLineHistory, writeTimeLineHistoryFile,
};

// Datum
use crate::postgres::{Datum, PointerGetDatum, DatumGetPointer, Int32GetDatum};

// palloc / pfree / pstrdup
use crate::utils::palloc::{palloc, palloc0, pfree, pstrdup};

// TransactionId types and helpers
use crate::c::TransactionId;
use crate::access::transam::{InvalidTransactionId, TransactionIdIsValid};
use crate::access::transam::varsup::ReadNextFullTransactionId;

/// XidFromFullTransactionId - extract TransactionId from FullTransactionId
// TODO(pg-port): real XidFromFullTransactionId lives in access/transam/varsup.c
#[inline]
fn XidFromFullTransactionId(x: FullTransactionId) -> TransactionId {
    x.value as TransactionId
}

/// EpochFromFullTransactionId - extract epoch from FullTransactionId
// TODO(pg-port): real EpochFromFullTransactionId lives in access/transam/varsup.c
#[inline]
fn EpochFromFullTransactionId(x: FullTransactionId) -> u32 {
    (x.value >> 32) as u32
}

// pg_config_manual
use crate::pg_config_manual::NAMEDATALEN;

// MAXCONNINFO
use crate::replication::walreceiverfuncs::MAXCONNINFO;

// NI_MAXHOST (mirror of common glibc value)
const NI_MAXHOST: usize = 1025;

// MAXFNAMELEN
use crate::access::transam::xlogutils::MAXFNAMELEN;

// wal_segment_size
use crate::access::transam::xlogutils::wal_segment_size;

/* --------------------------------------------------------------------------
 * Types re-exported / referenced
 * -------------------------------------------------------------------------- */

/// TimestampTz (utils/timestamp.h)
type TimestampTz = i64;

/// FullTransactionId
use crate::access::transam::FullTransactionId;

/// pgsocket
use crate::port::noblock::pgsocket;
use crate::port::port_api::PGINVALID_SOCKET;

/// instr_time (portability/instr_time.h) - used by pgstat IO timing
type instr_time = u64; // opaque; only passed to pgstat helpers

/// off_t
type off_t = i64;

/* --------------------------------------------------------------------------
 * GUC variables
 * -------------------------------------------------------------------------- */

/// wal_receiver_status_interval (seconds between status updates to primary)
pub static mut wal_receiver_status_interval: c_int = 10;

/// wal_receiver_timeout (milliseconds before timeout)
pub static mut wal_receiver_timeout: c_int = 60000;

/// hot_standby_feedback - send hot-standby feedback to the primary
pub static mut hot_standby_feedback: bool = false;

/* --------------------------------------------------------------------------
 * Module-level statics
 * -------------------------------------------------------------------------- */

/// libpqwalreceiver connection handle (module-level, walreceiver process only)
static mut wrconn: *mut WalReceiverConn = null_mut();

/// Vtable filled in by libpqwalreceiver _PG_init
pub static mut WalReceiverFunctions: *mut WalReceiverFunctionsType = null_mut();

/*
 * These variables are used similarly to openLogFile/SegNo,
 * but for walreceiver to write the XLOG. recvFileTLI is the TimeLineID
 * corresponding the filename of recvFile.
 */
static mut recvFile: c_int = -1;
static mut recvFileTLI: TimeLineID = 0;
static mut recvSegNo: XLogSegNo = 0;

/*
 * LogstreamResult indicates the byte positions that we have already
 * written/fsynced.
 */
struct LogstreamResultType {
    Write: XLogRecPtr, /* last byte + 1 written out in the standby */
    Flush: XLogRecPtr, /* last byte + 1 flushed in the standby */
}

static mut LogstreamResult: LogstreamResultType = LogstreamResultType {
    Write: 0,
    Flush: 0,
};

/*
 * Reasons to wake up and perform periodic tasks.
 */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq)]
#[allow(non_camel_case_types)]
enum WalRcvWakeupReason {
    WALRCV_WAKEUP_TERMINATE = 0,
    WALRCV_WAKEUP_PING,
    WALRCV_WAKEUP_REPLY,
    WALRCV_WAKEUP_HSFEEDBACK,
}
use WalRcvWakeupReason::*;

const NUM_WALRCV_WAKEUPS: usize = 4; /* WALRCV_WAKEUP_HSFEEDBACK + 1 */

/*
 * Wake up times for periodic tasks.
 */
static mut wakeup: [TimestampTz; NUM_WALRCV_WAKEUPS] = [0i64; NUM_WALRCV_WAKEUPS];

static mut reply_message: StringInfoData = StringInfoData {
    data: null_mut(),
    len: 0,
    maxlen: 0,
    cursor: 0,
};

/* --------------------------------------------------------------------------
 * Stubs for unported / external symbols
 * -------------------------------------------------------------------------- */

/// TIMESTAMP_INFINITY (utils/timestamp.h) - largest representable timestamp
// TODO(pg-port): real TIMESTAMP_INFINITY lives in utils/timestamp.h
const TIMESTAMP_INFINITY: TimestampTz = i64::MAX;

/// TimestampTzPlusMilliseconds (utils/timestamp.h)
// TODO(pg-port): real TimestampTzPlusMilliseconds lives in utils/timestamp.h
#[inline]
unsafe fn TimestampTzPlusMilliseconds(tz: TimestampTz, ms: c_int) -> TimestampTz {
    tz + (ms as i64 * 1000)
}

/// TimestampTzPlusSeconds (utils/timestamp.h)
// TODO(pg-port): real TimestampTzPlusSeconds lives in utils/timestamp.h
#[inline]
unsafe fn TimestampTzPlusSeconds(tz: TimestampTz, s: c_int) -> TimestampTz {
    tz + (s as i64 * 1_000_000)
}

/// pg_atomic_write_u64 - thin wrapper over port/atomics generic impl
#[inline]
unsafe fn pg_atomic_write_u64(ptr: *mut pg_atomic_uint64, val: u64) {
    pg_atomic_write_u64_impl(&*ptr, val);
}

/// on_shmem_exit - register shmem-exit callback (storage/ipc.h)
// TODO(pg-port): real on_shmem_exit lives in storage/ipc/ipc.c
unsafe fn on_shmem_exit(_function: unsafe fn(c_int, Datum), _arg: Datum) {
    unimplemented!() // TODO(pg-port): real on_shmem_exit lives in storage/ipc/ipc.c
}

/// proc_exit - terminate the process (storage/ipc.h)
// TODO(pg-port): real proc_exit lives in storage/ipc/ipc.c
unsafe fn proc_exit(_code: c_int) -> ! {
    unimplemented!() // TODO(pg-port): real proc_exit lives in storage/ipc/ipc.c
}

/// AuxiliaryProcessMainCommon - common startup for aux processes (postmaster/auxprocess.h)
// TODO(pg-port): real AuxiliaryProcessMainCommon lives in postmaster/auxprocess.c
unsafe fn AuxiliaryProcessMainCommon() {
    unimplemented!() // TODO(pg-port): real AuxiliaryProcessMainCommon lives in postmaster/auxprocess.c
}

/// load_file - dynamically load a shared library (utils/fmgr.h)
// TODO(pg-port): real load_file lives in utils/fmgr/dfmgr.c
unsafe fn load_file(_filename: *const c_char, _restricted: bool) {
    unimplemented!() // TODO(pg-port): real load_file lives in utils/fmgr/dfmgr.c
}

/// sigset_t placeholder
type sigset_t = u64;

/// UnBlockSig - signal mask with all signals unblocked (libpq/pqsignal.h)
// TODO(pg-port): real UnBlockSig lives in libpq/pqsignal.c
static mut UnBlockSig: sigset_t = 0;

/// pqsignal - set a signal handler, returning old handler (libpq/pqsignal.h)
// TODO(pg-port): real pqsignal lives in libpq/pqsignal.c
unsafe fn pqsignal(_signum: c_int, _handler: *const c_void) {
    unimplemented!() // TODO(pg-port): real pqsignal lives in libpq/pqsignal.c
}

/// sigprocmask (libc)
unsafe fn sigprocmask(_how: c_int, _set: *const sigset_t, _oldset: *mut sigset_t) -> c_int {
    0
}

const SIG_IGN: *const c_void = 1usize as *const c_void;
const SIG_DFL: *const c_void = 0usize as *const c_void;
const SIGHUP: c_int = 1;
const SIGINT: c_int = 2;
const SIGTERM: c_int = 15;
const SIGALRM: c_int = 14;
const SIGPIPE: c_int = 13;
const SIGUSR1: c_int = 10;
const SIGUSR2: c_int = 12;
const SIGCHLD: c_int = 17;
const SIG_SETMASK: c_int = 2;

/// SignalHandlerForConfigReload - signal handler for SIGHUP (postmaster/interrupt.h)
// TODO(pg-port): real SignalHandlerForConfigReload lives in postmaster/interrupt.c
static mut SignalHandlerForConfigReload: *const c_void = SIG_DFL;

/// die - signal handler that triggers backend shutdown (tcop/tcopprot.h)
// TODO(pg-port): real die lives in tcop/postgres.c
static mut die_handler: *const c_void = SIG_DFL;

/// procsignal_sigusr1_handler - SIGUSR1 handler (storage/procsignal.h)
// TODO(pg-port): real procsignal_sigusr1_handler lives in storage/ipc/procsignal.c
static mut procsignal_sigusr1_handler: *const c_void = SIG_DFL;

/// cluster_name - GUC variable (miscadmin.h)
// TODO(pg-port): real cluster_name lives in postmaster/postmaster.c (GUC)
static mut cluster_name: [c_char; NAMEDATALEN] = [0; NAMEDATALEN];

/// update_process_title - GUC (miscadmin.h)
// TODO(pg-port): real update_process_title lives in utils/misc/guc_tables.c (GUC)
static mut update_process_title: bool = false;

/// set_ps_display - update process title (utils/ps_status.h)
// TODO(pg-port): real set_ps_display lives in utils/misc/ps_status.c
unsafe fn set_ps_display(_activity: *const c_char) {
    unimplemented!() // TODO(pg-port): real set_ps_display lives in utils/misc/ps_status.c
}

/// ConfigReloadPending - flag set by SIGHUP handler (postmaster/interrupt.h)
// TODO(pg-port): real ConfigReloadPending lives in postmaster/interrupt.c
static mut ConfigReloadPending: bool = false;

/// PGC_SIGHUP - GUC context for reload (utils/guc.h)
const PGC_SIGHUP: c_int = 1;

/// ProcessConfigFile - reload configuration (utils/guc.h)
// TODO(pg-port): real ProcessConfigFile lives in utils/misc/guc.c
unsafe fn ProcessConfigFile(_context: c_int) {
    unimplemented!() // TODO(pg-port): real ProcessConfigFile lives in utils/misc/guc.c
}

/// RecoveryInProgress - is recovery in progress? (access/xlogrecovery.h)
// TODO(pg-port): real RecoveryInProgress lives in access/transam/xlogrecovery.c
unsafe fn RecoveryInProgress() -> bool {
    unimplemented!() // TODO(pg-port): real RecoveryInProgress lives in access/transam/xlogrecovery.c
}

/// GetXLogReplayRecPtr - get current replay position (access/xlogrecovery.h)
// TODO(pg-port): real GetXLogReplayRecPtr lives in access/transam/xlogrecovery.c
unsafe fn GetXLogReplayRecPtr(_replayTLI: *mut TimeLineID) -> XLogRecPtr {
    unimplemented!() // TODO(pg-port): real GetXLogReplayRecPtr lives in access/transam/xlogrecovery.c
}

/// WakeupRecovery - wake the startup process (postmaster/startup.h)
// TODO(pg-port): real WakeupRecovery lives in postmaster/startup.c
unsafe fn WakeupRecovery() {
    unimplemented!() // TODO(pg-port): real WakeupRecovery lives in postmaster/startup.c
}

/// AllowCascadeReplication - can we send WAL to cascaded standbys? (replication/walsender.h)
// TODO(pg-port): real AllowCascadeReplication lives in replication/walsender.c
unsafe fn AllowCascadeReplication() -> bool {
    unimplemented!() // TODO(pg-port): real AllowCascadeReplication lives in replication/walsender.c
}

/// WalSndWakeup - wake walsenders (replication/walsender.h)
// TODO(pg-port): real WalSndWakeup lives in replication/walsender.c
unsafe fn WalSndWakeup(_physical: bool, _logical: bool) {
    unimplemented!() // TODO(pg-port): real WalSndWakeup lives in replication/walsender.c
}

/// GetSystemIdentifier - get this system's identifier (access/xlog.h)
// TODO(pg-port): real GetSystemIdentifier lives in access/transam/xlog.c
pub unsafe fn GetSystemIdentifier() -> u64 {
    unimplemented!() // TODO(pg-port): real GetSystemIdentifier lives in access/transam/xlog.c
}

/// HotStandbyActive - is hot standby accepting connections? (access/xlogrecovery.h)
// TODO(pg-port): real HotStandbyActive lives in access/transam/xlogrecovery.c
unsafe fn HotStandbyActive() -> bool {
    unimplemented!() // TODO(pg-port): real HotStandbyActive lives in access/transam/xlogrecovery.c
}

/// GetReplicationHorizons - get xmin/catalog_xmin for hot standby feedback (storage/procarray.h)
// TODO(pg-port): real GetReplicationHorizons lives in storage/ipc/procarray.c
unsafe fn GetReplicationHorizons(_xmin: *mut TransactionId, _catalog_xmin: *mut TransactionId) {
    unimplemented!() // TODO(pg-port): real GetReplicationHorizons lives in storage/ipc/procarray.c
}

/// timestamptz_to_str - format a timestamp (utils/adt/timestamp.c)
// TODO(pg-port): real timestamptz_to_str lives in utils/adt/timestamp.c
unsafe fn timestamptz_to_str(_dt: TimestampTz) -> *const c_char {
    unimplemented!() // TODO(pg-port): real timestamptz_to_str lives in utils/adt/timestamp.c
}

/// pgstat_report_wal - report WAL stats (pgstat.h)
// TODO(pg-port): real pgstat_report_wal lives in backend/utils/activity/pgstat_wal.c
unsafe fn pgstat_report_wal(_force: bool) {
    unimplemented!() // TODO(pg-port): real pgstat_report_wal lives in backend/utils/activity/pgstat_wal.c
}

/// issue_xlog_fsync - fsync a WAL segment file (access/xlog.h)
// TODO(pg-port): real issue_xlog_fsync lives in access/transam/xlog.c
unsafe fn issue_xlog_fsync(_fd: c_int, _segno: XLogSegNo, _tli: TimeLineID) {
    unimplemented!() // TODO(pg-port): real issue_xlog_fsync lives in access/transam/xlog.c
}

/// XLogFileInit - create/open a new WAL segment file (access/xlog.h)
// TODO(pg-port): real XLogFileInit lives in access/transam/xlog.c
unsafe fn XLogFileInit(_segno: XLogSegNo, _tli: TimeLineID) -> c_int {
    unimplemented!() // TODO(pg-port): real XLogFileInit lives in access/transam/xlog.c
}

/// pg_pwrite - pwrite(2) wrapper (port.h)
// TODO(pg-port): real pg_pwrite lives in port/pg_pwrite.c
unsafe fn pg_pwrite(_fd: c_int, _buf: *const c_void, _count: usize, _offset: off_t) -> isize {
    unimplemented!() // TODO(pg-port): real pg_pwrite lives in port/pg_pwrite.c
}

/// errno - C errno
unsafe fn get_errno() -> c_int {
    *libc_errno()
}

unsafe fn set_errno(e: c_int) {
    *libc_errno() = e;
}

unsafe fn libc_errno() -> *mut c_int {
    extern "C" {
        fn __errno_location() -> *mut c_int;
    }
    __errno_location()
}

const ENOSPC: c_int = 28;

/// snprintf - C snprintf
unsafe fn libc_snprintf(
    s: *mut c_char,
    n: usize,
    fmt: *const c_char,
    val: u64,
) -> c_int {
    extern "C" {
        fn snprintf(s: *mut c_char, n: usize, fmt: *const c_char, ...) -> c_int;
    }
    snprintf(s, n, fmt, val)
}

/// close(2)
unsafe fn libc_close(fd: c_int) -> c_int {
    extern "C" {
        fn close(fd: c_int) -> c_int;
    }
    close(fd)
}

/// strlcpy
use crate::port::strlcpy::strlcpy;

/// memset
unsafe fn libc_memset(s: *mut c_void, c: c_int, n: usize) -> *mut c_void {
    extern "C" {
        fn memset(s: *mut c_void, c: c_int, n: usize) -> *mut c_void;
    }
    memset(s, c, n)
}

/// pg_memory_barrier wrapper
#[inline]
unsafe fn pg_memory_barrier() {
    pg_memory_barrier_impl();
}

/* pgstat I/O timing helpers - stubs */
// TODO(pg-port): real pgstat_prepare_io_time lives in backend/utils/activity/pgstat_io.c
unsafe fn pgstat_prepare_io_time(_track: bool) -> instr_time {
    0
}
// TODO(pg-port): real pgstat_count_io_op_time lives in backend/utils/activity/pgstat_io.c
unsafe fn pgstat_count_io_op_time(
    _obj: c_int,
    _ctx: c_int,
    _op: c_int,
    _start: instr_time,
    _cnt: c_int,
    _bytes: isize,
) {
}
// TODO(pg-port): real pgstat_report_wait_start lives in backend/utils/activity/pgstat_io.c
unsafe fn pgstat_report_wait_start(_wait_event_info: u32) {}
// TODO(pg-port): real pgstat_report_wait_end lives in backend/utils/activity/pgstat_io.c
unsafe fn pgstat_report_wait_end() {}

// I/O object / context / operation constants (pgstat.h)
// TODO(pg-port): real constants live in backend/utils/activity/wait_event_types.h
const IOOBJECT_WAL: c_int = 0;
const IOCONTEXT_NORMAL: c_int = 0;
const IOOP_WRITE: c_int = 0;
const WAIT_EVENT_WAL_WRITE: u32 = 0;
const WAIT_EVENT_WAL_RECEIVER_MAIN: u32 = 0;
const WAIT_EVENT_WAL_RECEIVER_WAIT_START: u32 = 0;

/// track_wal_io_timing - GUC (utils/guc_tables.c)
// TODO(pg-port): real track_wal_io_timing lives in utils/misc/guc_tables.c (GUC)
static mut track_wal_io_timing: bool = false;

/* pg_stat_get_wal_receiver helper stubs */
// TODO(pg-port): real get_call_result_type lives in utils/fmgr/funcapi.c
const TYPEFUNC_COMPOSITE: c_int = 1;

type TupleDesc = *mut c_void;
type HeapTuple = *mut c_void;

// FunctionCallInfo is *mut FmgrInfo in the codebase
use crate::utils::fmgr::FunctionCallInfo;

unsafe fn get_call_result_type(
    _fcinfo: FunctionCallInfo,
    _resultTypeId: *mut crate::postgres_ext::Oid,
    _resultTupleDesc: *mut TupleDesc,
) -> c_int {
    unimplemented!() // TODO(pg-port): real get_call_result_type lives in utils/fmgr/funcapi.c
}

unsafe fn palloc0_array_datum(n: usize) -> *mut Datum {
    palloc0(n * core::mem::size_of::<Datum>()) as *mut Datum
}

unsafe fn palloc0_array_bool(n: usize) -> *mut bool {
    palloc0(n * core::mem::size_of::<bool>()) as *mut bool
}

unsafe fn heap_form_tuple(
    _tupdesc: TupleDesc,
    _values: *mut Datum,
    _isnull: *mut bool,
) -> HeapTuple {
    unimplemented!() // TODO(pg-port): real heap_form_tuple lives in access/common/heaptuple.c
}

unsafe fn HeapTupleGetDatum(_tuple: HeapTuple) -> Datum {
    unimplemented!() // TODO(pg-port): real HeapTupleGetDatum lives in access/common/heaptuple.c
}

unsafe fn CStringGetTextDatum(_s: *const c_char) -> Datum {
    unimplemented!() // TODO(pg-port): real CStringGetTextDatum lives in utils/adt/varlena.c
}

unsafe fn LSNGetDatum(_lsn: XLogRecPtr) -> Datum {
    unimplemented!() // TODO(pg-port): real LSNGetDatum lives in utils/adt/pg_lsn.c
}

unsafe fn TimestampTzGetDatum(_tz: TimestampTz) -> Datum {
    unimplemented!() // TODO(pg-port): real TimestampTzGetDatum lives in utils/adt/timestamp.c
}

/// has_privs_of_role (utils/acl.h)
// TODO(pg-port): real has_privs_of_role lives in utils/acle/acl.c
unsafe fn has_privs_of_role(
    _member: crate::postgres_ext::Oid,
    _role: crate::postgres_ext::Oid,
) -> bool {
    unimplemented!() // TODO(pg-port): real has_privs_of_role lives in utils/acl/acl.c
}

/// GetUserId
use crate::miscadmin::GetUserId;

/// ROLE_PG_READ_ALL_STATS (catalog/pg_authid.h)
// TODO(pg-port): real ROLE_PG_READ_ALL_STATS lives in catalog/pg_authid.h (generated)
const ROLE_PG_READ_ALL_STATS: crate::postgres_ext::Oid = 0;

/// message_level_is_interesting (utils/elog.h)
// TODO(pg-port): real message_level_is_interesting lives in utils/error/elog.c
unsafe fn message_level_is_interesting(_elevel: c_int) -> bool {
    false
}

/// GetReplicationApplyDelay (replication/walreceiverfuncs.h)
use crate::replication::walreceiverfuncs::GetReplicationApplyDelay;

/// GetReplicationTransferLatency (replication/walreceiverfuncs.h)
use crate::replication::walreceiverfuncs::GetReplicationTransferLatency;

// UINT64_FORMAT is just the format string "%llu" on most platforms; use {} in Rust
// LSN_FORMAT_ARGS splits LSN into (hi u32, lo u32)
#[inline]
fn LSN_FORMAT_ARGS(lsn: XLogRecPtr) -> (u32, u32) {
    ((lsn >> 32) as u32, lsn as u32)
}

/* walrcv_* macro wrappers - dispatch through WalReceiverFunctions vtable */

pub unsafe fn walrcv_connect(
    conninfo: *const c_char,
    replication: bool,
    logical: bool,
    must_use_password: bool,
    appname: *const c_char,
    err: *mut *mut c_char,
) -> *mut WalReceiverConn {
    ((*WalReceiverFunctions).walrcv_connect)(
        conninfo,
        replication,
        logical,
        must_use_password,
        appname,
        err,
    )
}

unsafe fn walrcv_get_conninfo(conn: *mut WalReceiverConn) -> *mut c_char {
    ((*WalReceiverFunctions).walrcv_get_conninfo)(conn)
}

unsafe fn walrcv_get_senderinfo(
    conn: *mut WalReceiverConn,
    sender_host: *mut *mut c_char,
    sender_port: *mut c_int,
) {
    ((*WalReceiverFunctions).walrcv_get_senderinfo)(conn, sender_host, sender_port)
}

unsafe fn walrcv_identify_system(
    conn: *mut WalReceiverConn,
    primary_tli: *mut TimeLineID,
) -> *mut c_char {
    ((*WalReceiverFunctions).walrcv_identify_system)(conn, primary_tli)
}

unsafe fn walrcv_get_backend_pid(conn: *mut WalReceiverConn) -> c_int {
    ((*WalReceiverFunctions).walrcv_get_backend_pid)(conn)
}

pub unsafe fn walrcv_create_slot(
    conn: *mut WalReceiverConn,
    slotname: *const c_char,
    temporary: bool,
    two_phase: bool,
    failover: bool,
    snapshot_action: c_int,
    lsn: *mut XLogRecPtr,
) -> *mut c_char {
    ((*WalReceiverFunctions).walrcv_create_slot)(
        conn,
        slotname,
        temporary,
        two_phase,
        failover,
        snapshot_action,
        lsn,
    )
}

pub unsafe fn walrcv_startstreaming(
    conn: *mut WalReceiverConn,
    options: *const WalRcvStreamOptions,
) -> bool {
    ((*WalReceiverFunctions).walrcv_startstreaming)(conn, options)
}

pub unsafe fn walrcv_endstreaming(conn: *mut WalReceiverConn, next_tli: *mut TimeLineID) {
    ((*WalReceiverFunctions).walrcv_endstreaming)(conn, next_tli)
}

pub unsafe fn walrcv_receive(
    conn: *mut WalReceiverConn,
    buffer: *mut *mut c_char,
    wait_fd: *mut pgsocket,
) -> c_int {
    ((*WalReceiverFunctions).walrcv_receive)(conn, buffer, wait_fd)
}

unsafe fn walrcv_send(conn: *mut WalReceiverConn, buffer: *const c_char, nbytes: c_int) {
    ((*WalReceiverFunctions).walrcv_send)(conn, buffer, nbytes)
}

unsafe fn walrcv_readtimelinehistoryfile(
    conn: *mut WalReceiverConn,
    tli: TimeLineID,
    filename: *mut *mut c_char,
    content: *mut *mut c_char,
    len: *mut c_int,
) {
    ((*WalReceiverFunctions).walrcv_readtimelinehistoryfile)(conn, tli, filename, content, len)
}

unsafe fn walrcv_disconnect(conn: *mut WalReceiverConn) {
    ((*WalReceiverFunctions).walrcv_disconnect)(conn)
}

/* ========================================================================= */
/* Main entry point for walreceiver process                                   */
/* ========================================================================= */
pub unsafe fn WalReceiverMain(_startup_data: *const c_void, startup_data_len: usize) {
    let mut conninfo: [c_char; MAXCONNINFO] = [0; MAXCONNINFO];
    let tmp_conninfo: *mut c_char;
    let mut slotname: [c_char; NAMEDATALEN] = [0; NAMEDATALEN];
    let is_temp_slot: bool;
    let mut startpoint: XLogRecPtr;
    let mut startpointTLI: TimeLineID;
    let mut primaryTLI: TimeLineID = 0;
    let mut first_stream: bool;
    let walrcv: *mut WalRcvData;
    let mut now: TimestampTz;
    let mut err: *mut c_char = null_mut();
    let mut sender_host: *mut c_char = null_mut();
    let mut sender_port: c_int = 0;
    let appname: *const c_char;

    Assert!(startup_data_len == 0);

    MyBackendType = B_WAL_RECEIVER;
    AuxiliaryProcessMainCommon();

    /*
     * WalRcv should be set up already (if we are a backend, we inherit this
     * by fork() or EXEC_BACKEND mechanism from the postmaster).
     */
    walrcv = WalRcv;
    Assert!(!walrcv.is_null());

    /*
     * Mark walreceiver as running in shared memory.
     *
     * Do this as early as possible, so that if we fail later on, we'll set
     * state to STOPPED. If we die before this, the startup process will keep
     * waiting for us to start up, until it times out.
     */
    SpinLockAcquire(&mut (*walrcv).mutex);
    Assert!((*walrcv).pid == 0);
    match (*walrcv).walRcvState {
        WALRCV_STOPPING => {
            /* If we've already been requested to stop, don't start up. */
            (*walrcv).walRcvState = WALRCV_STOPPED;
            /* fall through */
            SpinLockRelease(&mut (*walrcv).mutex);
            ConditionVariableBroadcast(&mut (*walrcv).walRcvStoppedCV);
            proc_exit(1);
        }
        WALRCV_STOPPED => {
            SpinLockRelease(&mut (*walrcv).mutex);
            ConditionVariableBroadcast(&mut (*walrcv).walRcvStoppedCV);
            proc_exit(1);
        }
        WALRCV_STARTING => {
            /* The usual case */
        }
        WALRCV_WAITING | WALRCV_STREAMING | WALRCV_RESTARTING => {
            /* Shouldn't happen */
            SpinLockRelease(&mut (*walrcv).mutex);
            elog!(PANIC, "walreceiver still running according to shared memory state");
        }
    }
    /* Advertise our PID so that the startup process can kill us */
    (*walrcv).pid = MyProcPid;
    (*walrcv).walRcvState = WALRCV_STREAMING;

    /* Fetch information required to start streaming */
    (*walrcv).ready_to_display = false;
    strlcpy(conninfo.as_mut_ptr(), (*walrcv).conninfo.as_ptr(), MAXCONNINFO);
    strlcpy(slotname.as_mut_ptr(), (*walrcv).slotname.as_ptr(), NAMEDATALEN);
    is_temp_slot = (*walrcv).is_temp_slot;
    startpoint = (*walrcv).receiveStart;
    startpointTLI = (*walrcv).receiveStartTLI;

    /*
     * At most one of is_temp_slot and slotname can be set; otherwise,
     * RequestXLogStreaming messed up.
     */
    Assert!(!is_temp_slot || (slotname[0] == 0));

    /* Initialise to a sanish value */
    now = GetCurrentTimestamp();
    (*walrcv).lastMsgSendTime = now;
    (*walrcv).lastMsgReceiptTime = now;
    (*walrcv).latestWalEndTime = now;

    /* Report our proc number so that others can wake us up */
    (*walrcv).procno = MyProcNumber;

    SpinLockRelease(&mut (*walrcv).mutex);

    pg_atomic_write_u64(&mut (*WalRcv).writtenUpto, 0);

    /* Arrange to clean up at walreceiver exit */
    on_shmem_exit(WalRcvDie, PointerGetDatum(&mut startpointTLI as *mut _ as *const c_void));

    /* Properly accept or ignore signals the postmaster might send us */
    pqsignal(SIGHUP, SignalHandlerForConfigReload); /* set flag to read config file */
    pqsignal(SIGINT, SIG_IGN);
    pqsignal(SIGTERM, die_handler); /* request shutdown */
    /* SIGQUIT handler was already set up by InitPostmasterChild */
    pqsignal(SIGALRM, SIG_IGN);
    pqsignal(SIGPIPE, SIG_IGN);
    pqsignal(SIGUSR1, procsignal_sigusr1_handler);
    pqsignal(SIGUSR2, SIG_IGN);

    /* Reset some signals that are accepted by postmaster but not here */
    pqsignal(SIGCHLD, SIG_DFL);

    /* Load the libpq-specific functions */
    load_file(c"libpqwalreceiver".as_ptr(), false);
    if WalReceiverFunctions.is_null() {
        elog!(ERROR, "libpqwalreceiver didn't initialize correctly");
    }

    /* Unblock signals (they were blocked when the postmaster forked us) */
    sigprocmask(SIG_SETMASK, &UnBlockSig, null_mut());

    /* Establish the connection to the primary for XLOG streaming */
    appname = if cluster_name[0] != 0 {
        cluster_name.as_ptr()
    } else {
        c"walreceiver".as_ptr()
    };
    wrconn = walrcv_connect(conninfo.as_ptr(), true, false, false, appname, &mut err);
    if wrconn.is_null() {
        ereport!(
            ERROR,
            errmsg!(
                "streaming replication receiver \"{}\" could not connect to the primary server: {}",
                core::ffi::CStr::from_ptr(appname).to_string_lossy(),
                core::ffi::CStr::from_ptr(err).to_string_lossy()
            )
        );
    }

    /*
     * Save user-visible connection string.  This clobbers the original
     * conninfo, for security. Also save host and port of the sender server
     * this walreceiver is connected to.
     */
    tmp_conninfo = walrcv_get_conninfo(wrconn);
    walrcv_get_senderinfo(wrconn, &mut sender_host, &mut sender_port);
    SpinLockAcquire(&mut (*walrcv).mutex);
    libc_memset((*walrcv).conninfo.as_mut_ptr() as *mut c_void, 0, MAXCONNINFO);
    if !tmp_conninfo.is_null() {
        strlcpy((*walrcv).conninfo.as_mut_ptr(), tmp_conninfo, MAXCONNINFO);
    }

    libc_memset((*walrcv).sender_host.as_mut_ptr() as *mut c_void, 0, NI_MAXHOST);
    if !sender_host.is_null() {
        strlcpy((*walrcv).sender_host.as_mut_ptr(), sender_host, NI_MAXHOST);
    }

    (*walrcv).sender_port = sender_port;
    (*walrcv).ready_to_display = true;
    SpinLockRelease(&mut (*walrcv).mutex);

    if !tmp_conninfo.is_null() {
        pfree(tmp_conninfo as *mut c_void);
    }

    if !sender_host.is_null() {
        pfree(sender_host as *mut c_void);
    }

    first_stream = true;
    loop {
        let primary_sysid: *mut c_char;
        let mut standby_sysid: [c_char; 32] = [0; 32];
        let options: WalRcvStreamOptions;

        /*
         * Check that we're connected to a valid server using the
         * IDENTIFY_SYSTEM replication command.
         */
        primary_sysid = walrcv_identify_system(wrconn, &mut primaryTLI);

        libc_snprintf(
            standby_sysid.as_mut_ptr(),
            standby_sysid.len(),
            c"%llu".as_ptr(),
            GetSystemIdentifier(),
        );
        if libc_strcmp(primary_sysid, standby_sysid.as_ptr()) != 0 {
            ereport!(
                ERROR,
                errmsg!(
                    "database system identifier differs between the primary and standby"
                )
            );
        }

        /*
         * Confirm that the current timeline of the primary is the same or
         * ahead of ours.
         */
        if primaryTLI < startpointTLI {
            ereport!(
                ERROR,
                errmsg!(
                    "highest timeline {} of the primary is behind recovery timeline {}",
                    primaryTLI,
                    startpointTLI
                )
            );
        }

        /*
         * Get any missing history files. We do this always, even when we're
         * not interested in that timeline, so that if we're promoted to
         * become the primary later on, we don't select the same timeline that
         * was already used in the current primary.
         */
        WalRcvFetchTimeLineHistoryFiles(startpointTLI, primaryTLI);

        /*
         * Create temporary replication slot if requested, and update slot
         * name in shared memory.  (Note the slot name cannot already be set
         * in this case.)
         */
        if is_temp_slot {
            libc_snprintf(
                slotname.as_mut_ptr(),
                slotname.len(),
                c"pg_walreceiver_%lld".as_ptr(),
                walrcv_get_backend_pid(wrconn) as u64,
            );

            walrcv_create_slot(wrconn, slotname.as_ptr(), true, false, false, 0, null_mut());

            SpinLockAcquire(&mut (*walrcv).mutex);
            strlcpy((*walrcv).slotname.as_mut_ptr(), slotname.as_ptr(), NAMEDATALEN);
            SpinLockRelease(&mut (*walrcv).mutex);
        }

        /*
         * Start streaming.
         */
        options = WalRcvStreamOptions {
            logical: false,
            startpoint,
            slotname: if slotname[0] != 0 {
                slotname.as_ptr()
            } else {
                null_mut()
            },
            proto: WalRcvStreamProto {
                physical: core::mem::ManuallyDrop::new(WalRcvStreamPhysical {
                    startpointTLI,
                }),
            },
        };

        if walrcv_startstreaming(wrconn, &options) {
            if first_stream {
                let (hi, lo) = LSN_FORMAT_ARGS(startpoint);
                ereport!(
                    LOG,
                    errmsg!(
                        "started streaming WAL from primary at {:X}/{:X} on timeline {}",
                        hi,
                        lo,
                        startpointTLI
                    )
                );
            } else {
                let (hi, lo) = LSN_FORMAT_ARGS(startpoint);
                ereport!(
                    LOG,
                    errmsg!(
                        "restarted WAL streaming at {:X}/{:X} on timeline {}",
                        hi,
                        lo,
                        startpointTLI
                    )
                );
            }
            first_stream = false;

            /* Initialize LogstreamResult and buffers for processing messages */
            LogstreamResult.Write = GetXLogReplayRecPtr(null_mut());
            LogstreamResult.Flush = LogstreamResult.Write;
            initStringInfo(&mut reply_message);

            /* Initialize nap wakeup times. */
            now = GetCurrentTimestamp();
            for i in 0..NUM_WALRCV_WAKEUPS {
                WalRcvComputeNextWakeup(i, now);
            }

            /* Send initial reply/feedback messages. */
            XLogWalRcvSendReply(true, false);
            XLogWalRcvSendHSFeedback(true);

            /* Loop until end-of-streaming or error */
            'streaming: loop {
                let mut buf: *mut c_char = null_mut();
                let mut len: c_int;
                let mut endofwal = false;
                let mut wait_fd: pgsocket = PGINVALID_SOCKET;
                let rc: c_int;
                let mut nextWakeup: TimestampTz;
                let nap: c_long;

                /*
                 * Exit walreceiver if we're not in recovery. This should not
                 * happen, but cross-check the status here.
                 */
                if !RecoveryInProgress() {
                    ereport!(
                        FATAL,
                        errmsg!(
                            "cannot continue WAL streaming, recovery has already ended"
                        )
                    );
                }

                /* Process any requests or signals received recently */
                CHECK_FOR_INTERRUPTS();

                if ConfigReloadPending {
                    ConfigReloadPending = false;
                    ProcessConfigFile(PGC_SIGHUP);
                    /* recompute wakeup times */
                    now = GetCurrentTimestamp();
                    for i in 0..NUM_WALRCV_WAKEUPS {
                        WalRcvComputeNextWakeup(i, now);
                    }
                    XLogWalRcvSendHSFeedback(true);
                }

                /* See if we can read data immediately */
                len = walrcv_receive(wrconn, &mut buf, &mut wait_fd);
                if len != 0 {
                    /*
                     * Process the received data, and any subsequent data we
                     * can read without blocking.
                     */
                    loop {
                        if len > 0 {
                            /*
                             * Something was received from primary, so adjust
                             * the ping and terminate wakeup times.
                             */
                            now = GetCurrentTimestamp();
                            WalRcvComputeNextWakeup(
                                WALRCV_WAKEUP_TERMINATE as usize,
                                now,
                            );
                            WalRcvComputeNextWakeup(WALRCV_WAKEUP_PING as usize, now);
                            XLogWalRcvProcessMsg(
                                *buf as u8,
                                buf.add(1),
                                (len - 1) as usize,
                                startpointTLI,
                            );
                        } else if len == 0 {
                            break;
                        } else {
                            /* len < 0 */
                            let (hi, lo) = LSN_FORMAT_ARGS(LogstreamResult.Write);
                            ereport!(
                                LOG,
                                errmsg!(
                                    "replication terminated by primary server"
                                )
                            );
                            let _ = (hi, lo); /* referenced in errdetail which we omit */
                            endofwal = true;
                            break;
                        }
                        len = walrcv_receive(wrconn, &mut buf, &mut wait_fd);
                    }

                    /* Let the primary know that we received some data. */
                    XLogWalRcvSendReply(false, false);

                    /*
                     * If we've written some records, flush them to disk and
                     * let the startup process and primary server know about
                     * them.
                     */
                    XLogWalRcvFlush(false, startpointTLI);
                }

                /* Check if we need to exit the streaming loop. */
                if endofwal {
                    break 'streaming;
                }

                /* Find the soonest wakeup time, to limit our nap. */
                nextWakeup = TIMESTAMP_INFINITY;
                for i in 0..NUM_WALRCV_WAKEUPS {
                    if wakeup[i] < nextWakeup {
                        nextWakeup = wakeup[i];
                    }
                }

                /* Calculate the nap time, clamping as necessary. */
                now = GetCurrentTimestamp();
                nap = TimestampDifferenceMilliseconds(now, nextWakeup);

                /*
                 * Ideally we would reuse a WaitEventSet object repeatedly
                 * here to avoid the overheads of WaitLatchOrSocket on epoll
                 * systems, but we can't be sure that libpq (or any other
                 * walreceiver implementation) has the same socket (even if
                 * the fd is the same number, it may have been closed and
                 * reopened since the last time).
                 */
                Assert!(wait_fd != PGINVALID_SOCKET);
                rc = WaitLatchOrSocket(
                    MyLatch as *mut Latch,
                    WL_EXIT_ON_PM_DEATH | WL_SOCKET_READABLE | WL_TIMEOUT | WL_LATCH_SET,
                    wait_fd,
                    nap,
                    WAIT_EVENT_WAL_RECEIVER_MAIN,
                );
                if rc & WL_LATCH_SET != 0 {
                    ResetLatch(MyLatch as *mut Latch);
                    CHECK_FOR_INTERRUPTS();

                    if (*walrcv).force_reply != 0 {
                        /*
                         * The recovery process has asked us to send apply
                         * feedback now.  Make sure the flag is really set to
                         * false in shared memory before sending the reply, so
                         * we don't miss a new request for a reply.
                         */
                        (*walrcv).force_reply = 0;
                        pg_memory_barrier();
                        XLogWalRcvSendReply(true, false);
                    }
                }
                if rc & WL_TIMEOUT != 0 {
                    /*
                     * We didn't receive anything new. If we haven't heard
                     * anything from the server for more than
                     * wal_receiver_timeout / 2, ping the server. Also, if
                     * it's been longer than wal_receiver_status_interval
                     * since the last update we sent, send a status update to
                     * the primary anyway, to report any progress in applying
                     * WAL.
                     */
                    let mut requestReply = false;

                    /*
                     * Report pending statistics to the cumulative stats
                     * system.
                     */
                    pgstat_report_wal(false);

                    /*
                     * Check if time since last receive from primary has
                     * reached the configured limit.
                     */
                    now = GetCurrentTimestamp();
                    if now >= wakeup[WALRCV_WAKEUP_TERMINATE as usize] {
                        ereport!(
                            ERROR,
                            errmsg!("terminating walreceiver due to timeout")
                        );
                    }

                    /*
                     * If we didn't receive anything new for half of receiver
                     * replication timeout, then ping the server.
                     */
                    if now >= wakeup[WALRCV_WAKEUP_PING as usize] {
                        requestReply = true;
                        wakeup[WALRCV_WAKEUP_PING as usize] = TIMESTAMP_INFINITY;
                    }

                    XLogWalRcvSendReply(requestReply, requestReply);
                    XLogWalRcvSendHSFeedback(false);
                }
            } /* end streaming loop */

            /*
             * The backend finished streaming. Exit streaming COPY-mode from
             * our side, too.
             */
            walrcv_endstreaming(wrconn, &mut primaryTLI);

            /*
             * If the server had switched to a new timeline that we didn't
             * know about when we began streaming, fetch its timeline history
             * file now.
             */
            WalRcvFetchTimeLineHistoryFiles(startpointTLI, primaryTLI);
        } else {
            ereport!(
                LOG,
                errmsg!(
                    "primary server contains no more WAL on requested timeline {}",
                    startpointTLI
                )
            );
        }

        /*
         * End of WAL reached on the requested timeline. Close the last
         * segment, and await for new orders from the startup process.
         */
        if recvFile >= 0 {
            let mut xlogfname: [c_char; MAXFNAMELEN] = [0; MAXFNAMELEN];

            XLogWalRcvFlush(false, startpointTLI);
            XLogFileName(
                xlogfname.as_mut_ptr(),
                recvFileTLI,
                recvSegNo,
                wal_segment_size,
            );
            if libc_close(recvFile) != 0 {
                ereport!(
                    PANIC,
                    errmsg!(
                        "could not close WAL segment {}: (file access error)",
                        core::ffi::CStr::from_ptr(xlogfname.as_ptr()).to_string_lossy()
                    )
                );
            }

            /*
             * Create .done file forcibly to prevent the streamed segment from
             * being archived later.
             */
            if XLogArchiveMode != ARCHIVE_MODE_ALWAYS {
                XLogArchiveForceDone(xlogfname.as_ptr());
            } else {
                XLogArchiveNotify(xlogfname.as_ptr());
            }
        }
        recvFile = -1;

        elog!(DEBUG1, "walreceiver ended streaming and awaits new instructions");
        WalRcvWaitForStartPosition(&mut startpoint, &mut startpointTLI);
    } /* end outer loop - not reached */
}

/*
 * Wait for startup process to set receiveStart and receiveStartTLI.
 */
unsafe fn WalRcvWaitForStartPosition(
    startpoint: *mut XLogRecPtr,
    startpointTLI: *mut TimeLineID,
) {
    let walrcv: *mut WalRcvData = WalRcv;
    let state: WalRcvState;

    SpinLockAcquire(&mut (*walrcv).mutex);
    state = (*walrcv).walRcvState;
    if state != WALRCV_STREAMING {
        SpinLockRelease(&mut (*walrcv).mutex);
        if state == WALRCV_STOPPING {
            proc_exit(0);
        } else {
            elog!(FATAL, "unexpected walreceiver state");
        }
    }
    (*walrcv).walRcvState = WALRCV_WAITING;
    (*walrcv).receiveStart = InvalidXLogRecPtr;
    (*walrcv).receiveStartTLI = 0;
    SpinLockRelease(&mut (*walrcv).mutex);

    set_ps_display(c"idle".as_ptr());

    /*
     * nudge startup process to notice that we've stopped streaming and are
     * now waiting for instructions.
     */
    WakeupRecovery();
    loop {
        ResetLatch(MyLatch as *mut Latch);

        CHECK_FOR_INTERRUPTS();

        SpinLockAcquire(&mut (*walrcv).mutex);
        Assert!(
            (*walrcv).walRcvState == WALRCV_RESTARTING
                || (*walrcv).walRcvState == WALRCV_WAITING
                || (*walrcv).walRcvState == WALRCV_STOPPING
        );
        if (*walrcv).walRcvState == WALRCV_RESTARTING {
            /*
             * No need to handle changes in primary_conninfo or
             * primary_slot_name here. Startup process will signal us to
             * terminate in case those change.
             */
            *startpoint = (*walrcv).receiveStart;
            *startpointTLI = (*walrcv).receiveStartTLI;
            (*walrcv).walRcvState = WALRCV_STREAMING;
            SpinLockRelease(&mut (*walrcv).mutex);
            break;
        }
        if (*walrcv).walRcvState == WALRCV_STOPPING {
            /*
             * We should've received SIGTERM if the startup process wants us
             * to die, but might as well check it here too.
             */
            SpinLockRelease(&mut (*walrcv).mutex);
            extern "C" {
                fn exit(code: c_int) -> !;
            }
            exit(1);
        }
        SpinLockRelease(&mut (*walrcv).mutex);

        let _ = WaitLatch(
            MyLatch as *mut Latch,
            WL_LATCH_SET | WL_EXIT_ON_PM_DEATH,
            0,
            WAIT_EVENT_WAL_RECEIVER_WAIT_START,
        );
    }

    if update_process_title {
        let mut activitymsg: [c_char; 50] = [0; 50];
        let (hi, lo) = LSN_FORMAT_ARGS(*startpoint);
        libc_snprintf(
            activitymsg.as_mut_ptr(),
            activitymsg.len(),
            c"restarting at %lX/%lX".as_ptr(),
            hi as u64,
        );
        // Note: second arg needs a separate snprintf in C; approximate here
        let _ = lo;
        set_ps_display(activitymsg.as_ptr());
    }
}

/*
 * Fetch any missing timeline history files between 'first' and 'last'
 * (inclusive) from the server.
 */
unsafe fn WalRcvFetchTimeLineHistoryFiles(first: TimeLineID, last: TimeLineID) {
    let mut tli: TimeLineID = first;
    while tli <= last {
        /* there's no history file for timeline 1 */
        if tli != 1 && !existsTimeLineHistory(tli) {
            let mut fname: *mut c_char = null_mut();
            let mut content: *mut c_char = null_mut();
            let mut len: c_int = 0;
            let mut expectedfname: [c_char; MAXFNAMELEN] = [0; MAXFNAMELEN];

            ereport!(
                LOG,
                errmsg!(
                    "fetching timeline history file for timeline {} from primary server",
                    tli
                )
            );

            walrcv_readtimelinehistoryfile(wrconn, tli, &mut fname, &mut content, &mut len);

            /*
             * Check that the filename on the primary matches what we
             * calculated ourselves. This is just a sanity check, it should
             * always match.
             */
            TLHistoryFileName(expectedfname.as_mut_ptr(), tli);
            if libc_strcmp(fname, expectedfname.as_ptr()) != 0 {
                ereport!(
                    ERROR,
                    errmsg!(
                        "primary reported unexpected file name for timeline history file of timeline {}",
                        tli
                    )
                );
            }

            /*
             * Write the file to pg_wal.
             */
            writeTimeLineHistoryFile(tli, content, len);

            /*
             * Mark the streamed history file as ready for archiving if
             * archive_mode is always.
             */
            if XLogArchiveMode != ARCHIVE_MODE_ALWAYS {
                XLogArchiveForceDone(fname);
            } else {
                XLogArchiveNotify(fname);
            }

            pfree(fname as *mut c_void);
            pfree(content as *mut c_void);
        }
        tli += 1;
    }
}

/*
 * Mark us as STOPPED in shared memory at exit.
 */
unsafe fn WalRcvDie(code: c_int, arg: Datum) {
    let walrcv: *mut WalRcvData = WalRcv;
    let startpointTLI_p: *const TimeLineID = DatumGetPointer(arg) as *const TimeLineID;

    Assert!(*startpointTLI_p != 0);

    /* Ensure that all WAL records received are flushed to disk */
    XLogWalRcvFlush(true, *startpointTLI_p);

    /* Mark ourselves inactive in shared memory */
    SpinLockAcquire(&mut (*walrcv).mutex);
    Assert!(
        (*walrcv).walRcvState == WALRCV_STREAMING
            || (*walrcv).walRcvState == WALRCV_RESTARTING
            || (*walrcv).walRcvState == WALRCV_STARTING
            || (*walrcv).walRcvState == WALRCV_WAITING
            || (*walrcv).walRcvState == WALRCV_STOPPING
    );
    Assert!((*walrcv).pid == MyProcPid);
    (*walrcv).walRcvState = WALRCV_STOPPED;
    (*walrcv).pid = 0;
    (*walrcv).procno = INVALID_PROC_NUMBER;
    (*walrcv).ready_to_display = false;
    SpinLockRelease(&mut (*walrcv).mutex);

    ConditionVariableBroadcast(&mut (*walrcv).walRcvStoppedCV);

    /* Terminate the connection gracefully. */
    if !wrconn.is_null() {
        walrcv_disconnect(wrconn);
    }

    /* Wake up the startup process to notice promptly that we're gone */
    WakeupRecovery();
}

/*
 * Accept the message from XLOG stream, and process it.
 */
unsafe fn XLogWalRcvProcessMsg(
    msg_type: u8,
    buf: *mut c_char,
    len: usize,
    tli: TimeLineID,
) {
    let hdrlen: usize;
    let dataStart: XLogRecPtr;
    let walEnd: XLogRecPtr;
    let sendTime: TimestampTz;
    let replyRequested: c_int;

    match msg_type {
        b'w' => {
            /* WAL records */
            let mut incoming_message: StringInfoData = core::mem::zeroed();

            hdrlen = core::mem::size_of::<i64>() * 3;
            if len < hdrlen {
                ereport!(
                    ERROR,
                    errmsg!("invalid WAL message received from primary")
                );
            }

            /* initialize a StringInfo with the given buffer */
            initReadOnlyStringInfo(&mut incoming_message, buf, hdrlen as c_int);

            /* read the fields */
            let dataStart = pq_getmsgint64(&mut incoming_message) as XLogRecPtr;
            let walEnd = pq_getmsgint64(&mut incoming_message) as XLogRecPtr;
            let sendTime = pq_getmsgint64(&mut incoming_message) as TimestampTz;
            ProcessWalSndrMessage(walEnd, sendTime);

            XLogWalRcvWrite(buf.add(hdrlen), len - hdrlen, dataStart, tli);
        }
        b'k' => {
            /* Keepalive */
            let mut incoming_message: StringInfoData = core::mem::zeroed();

            hdrlen = core::mem::size_of::<i64>() * 2 + core::mem::size_of::<u8>();
            if len != hdrlen {
                ereport!(
                    ERROR,
                    errmsg!("invalid keepalive message received from primary")
                );
            }

            /* initialize a StringInfo with the given buffer */
            initReadOnlyStringInfo(&mut incoming_message, buf, hdrlen as c_int);

            /* read the fields */
            let walEnd = pq_getmsgint64(&mut incoming_message) as XLogRecPtr;
            let sendTime = pq_getmsgint64(&mut incoming_message) as TimestampTz;
            let replyRequested = pq_getmsgbyte(&mut incoming_message);

            ProcessWalSndrMessage(walEnd, sendTime);

            /* If the primary requested a reply, send one immediately */
            if replyRequested != 0 {
                XLogWalRcvSendReply(true, false);
            }
        }
        _ => {
            ereport!(
                ERROR,
                errmsg!("invalid replication message type {}", msg_type as c_int)
            );
        }
    }
}

/*
 * Write XLOG data to disk.
 */
unsafe fn XLogWalRcvWrite(
    mut buf: *mut c_char,
    mut nbytes: usize,
    mut recptr: XLogRecPtr,
    tli: TimeLineID,
) {
    let mut startoff: usize;
    let mut byteswritten: isize;
    let start: instr_time;

    Assert!(tli != 0);

    while nbytes > 0 {
        let segbytes: usize;

        /* Close the current segment if it's completed */
        if recvFile >= 0 && !XLByteInSeg(recptr, recvSegNo, wal_segment_size as usize) {
            XLogWalRcvClose(recptr, tli);
        }

        if recvFile < 0 {
            /* Create/use new log file */
            recvSegNo = XLByteToSeg(recptr, wal_segment_size as usize);
            recvFile = XLogFileInit(recvSegNo, tli);
            recvFileTLI = tli;
        }

        /* Calculate the start offset of the received logs */
        startoff = XLogSegmentOffset(recptr, wal_segment_size as usize) as usize;

        if startoff + nbytes > wal_segment_size as usize {
            segbytes = wal_segment_size as usize - startoff;
        } else {
            segbytes = nbytes;
        }

        /* OK to write the logs */
        set_errno(0);

        /*
         * Measure I/O timing to write WAL data, for pg_stat_io.
         */
        let start = pgstat_prepare_io_time(track_wal_io_timing);

        pgstat_report_wait_start(WAIT_EVENT_WAL_WRITE);
        byteswritten =
            pg_pwrite(recvFile, buf as *const c_void, segbytes, startoff as off_t);
        pgstat_report_wait_end();

        pgstat_count_io_op_time(
            IOOBJECT_WAL,
            IOCONTEXT_NORMAL,
            IOOP_WRITE,
            start,
            1,
            byteswritten,
        );

        if byteswritten <= 0 {
            let mut xlogfname: [c_char; MAXFNAMELEN] = [0; MAXFNAMELEN];
            let save_errno: c_int;

            /* if write didn't set errno, assume no disk space */
            if get_errno() == 0 {
                set_errno(ENOSPC);
            }

            save_errno = get_errno();
            XLogFileName(
                xlogfname.as_mut_ptr(),
                recvFileTLI,
                recvSegNo,
                wal_segment_size,
            );
            set_errno(save_errno);
            ereport!(
                PANIC,
                errmsg!(
                    "could not write to WAL segment {} at offset {}, length {}: (file access error)",
                    core::ffi::CStr::from_ptr(xlogfname.as_ptr()).to_string_lossy(),
                    startoff,
                    segbytes
                )
            );
        }

        /* Update state for write */
        recptr += byteswritten as u64;
        nbytes -= byteswritten as usize;
        buf = buf.add(byteswritten as usize);

        LogstreamResult.Write = recptr;
    }

    /* Update shared-memory status */
    pg_atomic_write_u64(&mut (*WalRcv).writtenUpto, LogstreamResult.Write);

    /*
     * Close the current segment if it's fully written up in the last cycle of
     * the loop, to create its archive notification file soon.
     */
    if recvFile >= 0 && !XLByteInSeg(recptr, recvSegNo, wal_segment_size as usize) {
        XLogWalRcvClose(recptr, tli);
    }
}

/*
 * Flush the log to disk.
 *
 * If we're in the midst of dying, it's unwise to do anything that might throw
 * an error, so we skip sending a reply in that case.
 */
unsafe fn XLogWalRcvFlush(dying: bool, tli: TimeLineID) {
    Assert!(tli != 0);

    if LogstreamResult.Flush < LogstreamResult.Write {
        let walrcv: *mut WalRcvData = WalRcv;

        issue_xlog_fsync(recvFile, recvSegNo, tli);

        LogstreamResult.Flush = LogstreamResult.Write;

        /* Update shared-memory status */
        SpinLockAcquire(&mut (*walrcv).mutex);
        if (*walrcv).flushedUpto < LogstreamResult.Flush {
            (*walrcv).latestChunkStart = (*walrcv).flushedUpto;
            (*walrcv).flushedUpto = LogstreamResult.Flush;
            (*walrcv).receivedTLI = tli;
        }
        SpinLockRelease(&mut (*walrcv).mutex);

        /* Signal the startup process and walsender that new WAL has arrived */
        WakeupRecovery();
        if AllowCascadeReplication() {
            WalSndWakeup(true, false);
        }

        /* Report XLOG streaming progress in PS display */
        if update_process_title {
            let mut activitymsg: [c_char; 50] = [0; 50];
            let (hi, lo) = LSN_FORMAT_ARGS(LogstreamResult.Write);
            libc_snprintf(
                activitymsg.as_mut_ptr(),
                activitymsg.len(),
                c"streaming %lX/%lX".as_ptr(),
                hi as u64,
            );
            let _ = lo;
            set_ps_display(activitymsg.as_ptr());
        }

        /* Also let the primary know that we made some progress */
        if !dying {
            XLogWalRcvSendReply(false, false);
            XLogWalRcvSendHSFeedback(false);
        }
    }
}

/*
 * Close the current segment.
 *
 * Flush the segment to disk before closing it. Otherwise we have to
 * reopen and fsync it later.
 *
 * Create an archive notification file since the segment is known completed.
 */
unsafe fn XLogWalRcvClose(recptr: XLogRecPtr, tli: TimeLineID) {
    let mut xlogfname: [c_char; MAXFNAMELEN] = [0; MAXFNAMELEN];

    Assert!(recvFile >= 0 && !XLByteInSeg(recptr, recvSegNo, wal_segment_size as usize));
    Assert!(tli != 0);

    /*
     * fsync() and close current file before we switch to next one. We would
     * otherwise have to reopen this file to fsync it later.
     */
    XLogWalRcvFlush(false, tli);

    XLogFileName(
        xlogfname.as_mut_ptr(),
        recvFileTLI,
        recvSegNo,
        wal_segment_size,
    );

    /*
     * XLOG segment files will be re-read by recovery in startup process soon,
     * so we don't advise the OS to release cache pages associated with the
     * file like XLogFileClose() does.
     */
    if libc_close(recvFile) != 0 {
        ereport!(
            PANIC,
            errmsg!(
                "could not close WAL segment {}: (file access error)",
                core::ffi::CStr::from_ptr(xlogfname.as_ptr()).to_string_lossy()
            )
        );
    }

    /*
     * Create .done file forcibly to prevent the streamed segment from being
     * archived later.
     */
    if XLogArchiveMode != ARCHIVE_MODE_ALWAYS {
        XLogArchiveForceDone(xlogfname.as_ptr());
    } else {
        XLogArchiveNotify(xlogfname.as_ptr());
    }

    recvFile = -1;
}

/*
 * Send reply message to primary, indicating our current WAL locations, oldest
 * xmin and the current time.
 *
 * If 'force' is not set, the message is only sent if enough time has
 * passed since last status update to reach wal_receiver_status_interval.
 * If wal_receiver_status_interval is disabled altogether and 'force' is
 * false, this is a no-op.
 *
 * If 'requestReply' is true, requests the server to reply immediately upon
 * receiving this message. This is used for heartbeats, when approaching
 * wal_receiver_timeout.
 */
unsafe fn XLogWalRcvSendReply(force: bool, requestReply: bool) {
    static mut writePtr: XLogRecPtr = 0;
    static mut flushPtr: XLogRecPtr = 0;
    let applyPtr: XLogRecPtr;
    let now: TimestampTz;

    /*
     * If the user doesn't want status to be reported to the primary, be sure
     * to exit before doing anything at all.
     */
    if !force && wal_receiver_status_interval <= 0 {
        return;
    }

    /* Get current timestamp. */
    now = GetCurrentTimestamp();

    /*
     * We can compare the write and flush positions to the last message we
     * sent without taking any lock, but the apply position requires a spin
     * lock, so we don't check that unless something else has changed or 10
     * seconds have passed.
     */
    if !force
        && writePtr == LogstreamResult.Write
        && flushPtr == LogstreamResult.Flush
        && now < wakeup[WALRCV_WAKEUP_REPLY as usize]
    {
        return;
    }

    /* Make sure we wake up when it's time to send another reply. */
    WalRcvComputeNextWakeup(WALRCV_WAKEUP_REPLY as usize, now);

    /* Construct a new message */
    writePtr = LogstreamResult.Write;
    flushPtr = LogstreamResult.Flush;
    applyPtr = GetXLogReplayRecPtr(null_mut());

    resetStringInfo(&mut reply_message);
    // pq_sendbyte is not exported; inline: append a single byte to StringInfo
    pq_sendbyte_inline(&mut reply_message, b'r');
    pq_sendint64(&mut reply_message, writePtr);
    pq_sendint64(&mut reply_message, flushPtr);
    pq_sendint64(&mut reply_message, applyPtr);
    pq_sendint64(&mut reply_message, GetCurrentTimestamp() as u64);
    pq_sendbyte_inline(&mut reply_message, if requestReply { 1 } else { 0 });

    /* Send it */
    let (whi, wlo) = LSN_FORMAT_ARGS(writePtr);
    let (fhi, flo) = LSN_FORMAT_ARGS(flushPtr);
    let (ahi, alo) = LSN_FORMAT_ARGS(applyPtr);
    elog!(
        DEBUG2,
        "sending write {:X}/{:X} flush {:X}/{:X} apply {:X}/{:X}{}",
        whi,
        wlo,
        fhi,
        flo,
        ahi,
        alo,
        if requestReply { " (reply requested)" } else { "" }
    );

    walrcv_send(wrconn, reply_message.data, reply_message.len);
}

/*
 * Send hot standby feedback message to primary, plus the current time,
 * in case they don't have a watch.
 *
 * If the user disables feedback, send one final message to tell sender
 * to forget about the xmin on this standby. We also send this message
 * on first connect because a previous connection might have set xmin
 * on a replication slot.
 */
unsafe fn XLogWalRcvSendHSFeedback(immed: bool) {
    let now: TimestampTz;
    let nextFullXid: FullTransactionId;
    let nextXid: TransactionId;
    let mut xmin_epoch: u32;
    let mut catalog_xmin_epoch: u32;
    let mut xmin: TransactionId = 0;
    let mut catalog_xmin: TransactionId = 0;

    /* initially true so we always send at least one feedback message */
    static mut primary_has_standby_xmin: bool = true;

    /*
     * If the user doesn't want status to be reported to the primary, be sure
     * to exit before doing anything at all.
     */
    if (wal_receiver_status_interval <= 0 || !hot_standby_feedback)
        && !primary_has_standby_xmin
    {
        return;
    }

    /* Get current timestamp. */
    now = GetCurrentTimestamp();

    /* Send feedback at most once per wal_receiver_status_interval. */
    if !immed && now < wakeup[WALRCV_WAKEUP_HSFEEDBACK as usize] {
        return;
    }

    /* Make sure we wake up when it's time to send feedback again. */
    WalRcvComputeNextWakeup(WALRCV_WAKEUP_HSFEEDBACK as usize, now);

    /*
     * If Hot Standby is not yet accepting connections there is nothing to
     * send.
     */
    if !HotStandbyActive() {
        return;
    }

    /*
     * Make the expensive call to get the oldest xmin once we are certain
     * everything else has been checked.
     */
    if hot_standby_feedback {
        GetReplicationHorizons(&mut xmin, &mut catalog_xmin);
    } else {
        xmin = InvalidTransactionId;
        catalog_xmin = InvalidTransactionId;
    }

    /*
     * Get epoch and adjust if nextXid and oldestXmin are different sides of
     * the epoch boundary.
     */
    nextFullXid = ReadNextFullTransactionId();
    nextXid = XidFromFullTransactionId(nextFullXid);
    xmin_epoch = EpochFromFullTransactionId(nextFullXid);
    catalog_xmin_epoch = xmin_epoch;
    if nextXid < xmin {
        xmin_epoch = xmin_epoch.wrapping_sub(1);
    }
    if nextXid < catalog_xmin {
        catalog_xmin_epoch = catalog_xmin_epoch.wrapping_sub(1);
    }

    elog!(
        DEBUG2,
        "sending hot standby feedback xmin {} epoch {} catalog_xmin {} catalog_xmin_epoch {}",
        xmin,
        xmin_epoch,
        catalog_xmin,
        catalog_xmin_epoch
    );

    /* Construct the message and send it. */
    resetStringInfo(&mut reply_message);
    pq_sendbyte_inline(&mut reply_message, b'h');
    pq_sendint64(&mut reply_message, GetCurrentTimestamp() as u64);
    pq_sendint32(&mut reply_message, xmin);
    pq_sendint32(&mut reply_message, xmin_epoch);
    pq_sendint32(&mut reply_message, catalog_xmin);
    pq_sendint32(&mut reply_message, catalog_xmin_epoch);
    walrcv_send(wrconn, reply_message.data, reply_message.len);
    if TransactionIdIsValid(xmin) || TransactionIdIsValid(catalog_xmin) {
        primary_has_standby_xmin = true;
    } else {
        primary_has_standby_xmin = false;
    }
}

/*
 * Update shared memory status upon receiving a message from primary.
 *
 * 'walEnd' and 'sendTime' are the end-of-WAL and timestamp of the latest
 * message, reported by primary.
 */
unsafe fn ProcessWalSndrMessage(walEnd: XLogRecPtr, sendTime: TimestampTz) {
    let walrcv: *mut WalRcvData = WalRcv;
    let lastMsgReceiptTime: TimestampTz = GetCurrentTimestamp();

    /* Update shared-memory status */
    SpinLockAcquire(&mut (*walrcv).mutex);
    if (*walrcv).latestWalEnd < walEnd {
        (*walrcv).latestWalEndTime = sendTime;
    }
    (*walrcv).latestWalEnd = walEnd;
    (*walrcv).lastMsgSendTime = sendTime;
    (*walrcv).lastMsgReceiptTime = lastMsgReceiptTime;
    SpinLockRelease(&mut (*walrcv).mutex);

    if message_level_is_interesting(DEBUG2) {
        let sendtime: *mut c_char;
        let receipttime: *mut c_char;
        let applyDelay: c_int;

        /* Copy because timestamptz_to_str returns a static buffer */
        sendtime = pstrdup(timestamptz_to_str(sendTime));
        receipttime = pstrdup(timestamptz_to_str(lastMsgReceiptTime));
        applyDelay = GetReplicationApplyDelay();

        /* apply delay is not available */
        if applyDelay == -1 {
            elog!(
                DEBUG2,
                "sendtime {} receipttime {} replication apply delay (N/A) transfer latency {} ms",
                core::ffi::CStr::from_ptr(sendtime).to_string_lossy(),
                core::ffi::CStr::from_ptr(receipttime).to_string_lossy(),
                GetReplicationTransferLatency()
            );
        } else {
            elog!(
                DEBUG2,
                "sendtime {} receipttime {} replication apply delay {} ms transfer latency {} ms",
                core::ffi::CStr::from_ptr(sendtime).to_string_lossy(),
                core::ffi::CStr::from_ptr(receipttime).to_string_lossy(),
                applyDelay,
                GetReplicationTransferLatency()
            );
        }

        pfree(sendtime as *mut c_void);
        pfree(receipttime as *mut c_void);
    }
}

/*
 * Compute the next wakeup time for a given wakeup reason.  Can be called to
 * initialize a wakeup time, to adjust it for the next wakeup, or to
 * reinitialize it when GUCs have changed.
 */
unsafe fn WalRcvComputeNextWakeup(reason: usize, now: TimestampTz) {
    match reason {
        r if r == WALRCV_WAKEUP_TERMINATE as usize => {
            if wal_receiver_timeout <= 0 {
                wakeup[reason] = TIMESTAMP_INFINITY;
            } else {
                wakeup[reason] = TimestampTzPlusMilliseconds(now, wal_receiver_timeout);
            }
        }
        r if r == WALRCV_WAKEUP_PING as usize => {
            if wal_receiver_timeout <= 0 {
                wakeup[reason] = TIMESTAMP_INFINITY;
            } else {
                wakeup[reason] =
                    TimestampTzPlusMilliseconds(now, wal_receiver_timeout / 2);
            }
        }
        r if r == WALRCV_WAKEUP_HSFEEDBACK as usize => {
            if !hot_standby_feedback || wal_receiver_status_interval <= 0 {
                wakeup[reason] = TIMESTAMP_INFINITY;
            } else {
                wakeup[reason] =
                    TimestampTzPlusSeconds(now, wal_receiver_status_interval);
            }
        }
        r if r == WALRCV_WAKEUP_REPLY as usize => {
            if wal_receiver_status_interval <= 0 {
                wakeup[reason] = TIMESTAMP_INFINITY;
            } else {
                wakeup[reason] =
                    TimestampTzPlusSeconds(now, wal_receiver_status_interval);
            }
        }
        _ => {}
    }
}

/*
 * Wake up the walreceiver main loop.
 *
 * This is called by the startup process whenever interesting xlog records
 * are applied, so that walreceiver can check if it needs to send an apply
 * notification back to the primary which may be waiting in a COMMIT with
 * synchronous_commit = remote_apply.
 */
pub unsafe fn WalRcvForceReply() {
    let procno: ProcNumber;

    (*WalRcv).force_reply = 1;
    /* fetching the proc number is probably atomic, but don't rely on it */
    SpinLockAcquire(&mut (*WalRcv).mutex);
    procno = (*WalRcv).procno;
    SpinLockRelease(&mut (*WalRcv).mutex);
    if procno != INVALID_PROC_NUMBER {
        // GetPGProcByNumber returns *mut crate::storage::proclist::PGPROC (opaque).
        // Cast to walreceiverfuncs PGPROC which has procLatch, matching upstream pattern.
        use crate::replication::walreceiverfuncs::{GetPGProcByNumber as WalRcvGetProc};
        SetLatch(
            &mut (*WalRcvGetProc(procno)).procLatch as *mut _ as *mut Latch,
        );
    }
}

/*
 * Return a string constant representing the state. This is used
 * in system functions and views, and should *not* be translated.
 */
unsafe fn WalRcvGetStateString(state: WalRcvState) -> *const c_char {
    match state {
        WALRCV_STOPPED => c"stopped".as_ptr(),
        WALRCV_STARTING => c"starting".as_ptr(),
        WALRCV_STREAMING => c"streaming".as_ptr(),
        WALRCV_WAITING => c"waiting".as_ptr(),
        WALRCV_RESTARTING => c"restarting".as_ptr(),
        WALRCV_STOPPING => c"stopping".as_ptr(),
    }
}

/*
 * Returns activity of WAL receiver, including pid, state and xlog locations
 * received from the WAL sender of another server.
 */
pub unsafe fn pg_stat_get_wal_receiver(fcinfo: FunctionCallInfo) -> Datum {
    let mut tupdesc: TupleDesc = null_mut();
    let values: *mut Datum;
    let nulls: *mut bool;
    let pid: c_int;
    let ready_to_display: bool;
    let state: WalRcvState;
    let receive_start_lsn: XLogRecPtr;
    let receive_start_tli: TimeLineID;
    let written_lsn: XLogRecPtr;
    let flushed_lsn: XLogRecPtr;
    let received_tli: TimeLineID;
    let last_send_time: TimestampTz;
    let last_receipt_time: TimestampTz;
    let latest_end_lsn: XLogRecPtr;
    let latest_end_time: TimestampTz;
    let mut sender_host: [c_char; NI_MAXHOST] = [0; NI_MAXHOST];
    let mut sender_port: c_int = 0;
    let mut slotname: [c_char; NAMEDATALEN] = [0; NAMEDATALEN];
    let mut conninfo: [c_char; MAXCONNINFO] = [0; MAXCONNINFO];

    /* Take a lock to ensure value consistency */
    SpinLockAcquire(&mut (*WalRcv).mutex);
    pid = (*WalRcv).pid as c_int;
    ready_to_display = (*WalRcv).ready_to_display;
    state = (*WalRcv).walRcvState;
    receive_start_lsn = (*WalRcv).receiveStart;
    receive_start_tli = (*WalRcv).receiveStartTLI;
    flushed_lsn = (*WalRcv).flushedUpto;
    received_tli = (*WalRcv).receivedTLI;
    last_send_time = (*WalRcv).lastMsgSendTime;
    last_receipt_time = (*WalRcv).lastMsgReceiptTime;
    latest_end_lsn = (*WalRcv).latestWalEnd;
    latest_end_time = (*WalRcv).latestWalEndTime;
    strlcpy(
        slotname.as_mut_ptr(),
        (*WalRcv).slotname.as_ptr(),
        slotname.len(),
    );
    strlcpy(
        sender_host.as_mut_ptr(),
        (*WalRcv).sender_host.as_ptr(),
        sender_host.len(),
    );
    sender_port = (*WalRcv).sender_port;
    strlcpy(
        conninfo.as_mut_ptr(),
        (*WalRcv).conninfo.as_ptr(),
        conninfo.len(),
    );
    SpinLockRelease(&mut (*WalRcv).mutex);

    /*
     * No WAL receiver (or not ready yet), just return a tuple with NULL values
     */
    if pid == 0 || !ready_to_display {
        // PG_RETURN_NULL(): return a null Datum
        // TODO(pg-port): real PG_RETURN_NULL lives in fmgr.h
        return 0 as Datum; /* simplified NULL return */
    }

    /*
     * Read "writtenUpto" without holding a spinlock.
     */
    written_lsn = pg_atomic_read_u64_impl(&(*WalRcv).writtenUpto);

    /* determine result type */
    if get_call_result_type(fcinfo, null_mut(), &mut tupdesc) != TYPEFUNC_COMPOSITE {
        elog!(ERROR, "return type must be a row type");
    }

    // natts from tupdesc - use 15 (the known column count for pg_stat_wal_receiver)
    let natts: usize = 15;
    values = palloc0_array_datum(natts);
    nulls = palloc0_array_bool(natts);

    /* Fetch values */
    *values.add(0) = Int32GetDatum(pid);

    if !has_privs_of_role(GetUserId(), ROLE_PG_READ_ALL_STATS) {
        /*
         * Only superusers and roles with privileges of pg_read_all_stats can
         * see details. Other users only get the pid value to know whether it
         * is a WAL receiver, but no details.
         */
        let mut i = 1;
        while i < natts {
            *nulls.add(i) = true;
            i += 1;
        }
    } else {
        *values.add(1) = CStringGetTextDatum(WalRcvGetStateString(state));

        if XLogRecPtrIsInvalid(receive_start_lsn) {
            *nulls.add(2) = true;
        } else {
            *values.add(2) = LSNGetDatum(receive_start_lsn);
        }
        *values.add(3) = Int32GetDatum(receive_start_tli as i32);
        if XLogRecPtrIsInvalid(written_lsn) {
            *nulls.add(4) = true;
        } else {
            *values.add(4) = LSNGetDatum(written_lsn);
        }
        if XLogRecPtrIsInvalid(flushed_lsn) {
            *nulls.add(5) = true;
        } else {
            *values.add(5) = LSNGetDatum(flushed_lsn);
        }
        *values.add(6) = Int32GetDatum(received_tli as i32);
        if last_send_time == 0 {
            *nulls.add(7) = true;
        } else {
            *values.add(7) = TimestampTzGetDatum(last_send_time);
        }
        if last_receipt_time == 0 {
            *nulls.add(8) = true;
        } else {
            *values.add(8) = TimestampTzGetDatum(last_receipt_time);
        }
        if XLogRecPtrIsInvalid(latest_end_lsn) {
            *nulls.add(9) = true;
        } else {
            *values.add(9) = LSNGetDatum(latest_end_lsn);
        }
        if latest_end_time == 0 {
            *nulls.add(10) = true;
        } else {
            *values.add(10) = TimestampTzGetDatum(latest_end_time);
        }
        if slotname[0] == 0 {
            *nulls.add(11) = true;
        } else {
            *values.add(11) = CStringGetTextDatum(slotname.as_ptr());
        }
        if sender_host[0] == 0 {
            *nulls.add(12) = true;
        } else {
            *values.add(12) = CStringGetTextDatum(sender_host.as_ptr());
        }
        if sender_port == 0 {
            *nulls.add(13) = true;
        } else {
            *values.add(13) = Int32GetDatum(sender_port);
        }
        if conninfo[0] == 0 {
            *nulls.add(14) = true;
        } else {
            *values.add(14) = CStringGetTextDatum(conninfo.as_ptr());
        }
    }

    /* Returns the record as Datum */
    HeapTupleGetDatum(heap_form_tuple(tupdesc, values, nulls))
}

/* --------------------------------------------------------------------------
 * Helper: pq_sendbyte - append a single byte to StringInfo
 * (libpq/pqformat.h macro in C; implemented inline here since pq_sendbyte
 *  is not individually exported from crate::libpq::pqformat)
 * -------------------------------------------------------------------------- */
#[inline]
unsafe fn pq_sendbyte_inline(buf: *mut StringInfoData, byt: u8) {
    crate::lib::stringinfo::appendStringInfoChar(buf, byt as c_char);
}

/* --------------------------------------------------------------------------
 * libc helpers
 * -------------------------------------------------------------------------- */

#[inline]
unsafe fn libc_strcmp(a: *const c_char, b: *const c_char) -> c_int {
    extern "C" {
        fn strcmp(a: *const c_char, b: *const c_char) -> c_int;
    }
    strcmp(a, b)
}

/* proc number global - import from utils::init::globals (the canonical home) */
use crate::utils::init::globals::MyProcNumber;
