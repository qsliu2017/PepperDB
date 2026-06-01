//! miscadmin.h - general postgres administration and initialization stuff.

#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]

use crate::c::{int64, uint32, uint8, bits32, Size};
use std::ffi::{c_char, c_int, c_void};
use crate::postgres_ext::Oid;

// sig_atomic_t from <signal.h>; on most platforms this is c_int.
pub type sig_atomic_t = c_int;

// pid_t from <sys/types.h>; c_int on the platforms PostgreSQL targets.
pub type pid_t = c_int;

// ssize_t from <sys/types.h>.
pub type ssize_t = isize;

// from datatype/timestamp.h
pub type TimestampTz = int64;

// from pgtime.h
pub type pg_time_t = int64;

// Forward-declared in C as `struct Port` / `struct Latch`; referenced only by
// pointer here. Use the real Port from crate::libpq::libpq_be; Latch stubbed.
// TODO: dedup with the real Latch definition when storage/latch.rs lands.
pub use crate::libpq::libpq_be::Port;
pub type Latch = c_void;

pub const InvalidPid: c_int = -1;

/*****************************************************************************
 *	  System interrupt and critical section handling
 *****************************************************************************/

/* in globals.c */
/* these are marked volatile because they are set by signal handlers: */
extern "C" {
    pub static mut InterruptPending: sig_atomic_t;
    pub static mut QueryCancelPending: sig_atomic_t;
    pub static mut ProcDiePending: sig_atomic_t;
    pub static mut IdleInTransactionSessionTimeoutPending: sig_atomic_t;
    pub static mut TransactionTimeoutPending: sig_atomic_t;
    pub static mut IdleSessionTimeoutPending: sig_atomic_t;
    pub static mut ProcSignalBarrierPending: sig_atomic_t;
    pub static mut LogMemoryContextPending: sig_atomic_t;
    pub static mut IdleStatsUpdateTimeoutPending: sig_atomic_t;

    pub static mut CheckClientConnectionPending: sig_atomic_t;
    pub static mut ClientConnectionLost: sig_atomic_t;

    /* these are marked volatile because they are examined by signal handlers: */
    pub static mut InterruptHoldoffCount: uint32;
    pub static mut QueryCancelHoldoffCount: uint32;
    pub static mut CritSectionCount: uint32;
}

/* in tcop/postgres.c */
pub unsafe fn ProcessInterrupts() {
    unimplemented!()
}

/* Test whether an interrupt is pending */
#[inline]
pub unsafe fn INTERRUPTS_PENDING_CONDITION() -> bool {
    InterruptPending != 0
}

/* Service interrupt, if one is pending and it's safe to service it now */
#[inline]
pub unsafe fn CHECK_FOR_INTERRUPTS() {
    if INTERRUPTS_PENDING_CONDITION() {
        ProcessInterrupts();
    }
}

/* Is ProcessInterrupts() guaranteed to clear InterruptPending? */
#[inline]
pub unsafe fn INTERRUPTS_CAN_BE_PROCESSED() -> bool {
    InterruptHoldoffCount == 0 && CritSectionCount == 0 && QueryCancelHoldoffCount == 0
}

#[inline]
pub unsafe fn HOLD_INTERRUPTS() {
    InterruptHoldoffCount += 1;
}

#[inline]
pub unsafe fn RESUME_INTERRUPTS() {
    debug_assert!(InterruptHoldoffCount > 0);
    InterruptHoldoffCount -= 1;
}

#[inline]
pub unsafe fn HOLD_CANCEL_INTERRUPTS() {
    QueryCancelHoldoffCount += 1;
}

#[inline]
pub unsafe fn RESUME_CANCEL_INTERRUPTS() {
    debug_assert!(QueryCancelHoldoffCount > 0);
    QueryCancelHoldoffCount -= 1;
}

#[inline]
pub unsafe fn START_CRIT_SECTION() {
    CritSectionCount += 1;
}

#[inline]
pub unsafe fn END_CRIT_SECTION() {
    debug_assert!(CritSectionCount > 0);
    CritSectionCount -= 1;
}

/*****************************************************************************
 *	  globals.h --															 *
 *****************************************************************************/

/*
 * from utils/init/globals.c
 */
extern "C" {
    pub static mut PostmasterPid: pid_t;
    pub static mut IsPostmasterEnvironment: bool;
    pub static mut IsUnderPostmaster: bool;
    pub static mut IsBinaryUpgrade: bool;

    pub static mut ExitOnAnyError: bool;

    pub static mut DataDir: *mut c_char;
    pub static mut data_directory_mode: c_int;

    pub static mut NBuffers: c_int;
    pub static mut MaxBackends: c_int;
    pub static mut MaxConnections: c_int;
    pub static mut max_worker_processes: c_int;
    pub static mut max_parallel_workers: c_int;

    pub static mut commit_timestamp_buffers: c_int;
    pub static mut multixact_member_buffers: c_int;
    pub static mut multixact_offset_buffers: c_int;
    pub static mut notify_buffers: c_int;
    pub static mut serializable_buffers: c_int;
    pub static mut subtransaction_buffers: c_int;
    pub static mut transaction_buffers: c_int;

    pub static mut MyProcPid: c_int;
    pub static mut MyStartTime: pg_time_t;
    pub static mut MyStartTimestamp: TimestampTz;
    pub static mut MyProcPort: *mut Port;
    pub static mut MyLatch: *mut Latch;
    pub static mut MyCancelKey: [uint8; 0];
    pub static mut MyCancelKeyLength: c_int;
    pub static mut MyPMChildSlot: c_int;

    pub static mut OutputFileName: [c_char; 0];
    pub static mut my_exec_path: [c_char; 0];
    pub static mut pkglib_path: [c_char; 0];

    // #ifdef EXEC_BACKEND
    pub static mut postgres_exec_path: [c_char; 0];

    pub static mut MyDatabaseId: Oid;

    pub static mut MyDatabaseTableSpace: Oid;

    pub static mut MyDatabaseHasLoginEventTriggers: bool;
}

/* valid DateStyle values */
pub const USE_POSTGRES_DATES: c_int = 0;
pub const USE_ISO_DATES: c_int = 1;
pub const USE_SQL_DATES: c_int = 2;
pub const USE_GERMAN_DATES: c_int = 3;
pub const USE_XSD_DATES: c_int = 4;

/* valid DateOrder values */
pub const DATEORDER_YMD: c_int = 0;
pub const DATEORDER_DMY: c_int = 1;
pub const DATEORDER_MDY: c_int = 2;

extern "C" {
    pub static mut DateStyle: c_int;
    pub static mut DateOrder: c_int;
}

/*
 * IntervalStyles
 */
pub const INTSTYLE_POSTGRES: c_int = 0;
pub const INTSTYLE_POSTGRES_VERBOSE: c_int = 1;
pub const INTSTYLE_SQL_STANDARD: c_int = 2;
pub const INTSTYLE_ISO_8601: c_int = 3;

extern "C" {
    pub static mut IntervalStyle: c_int;
}

pub const MAXTZLEN: c_int = 10; /* max TZ name len, not counting tr. null */

extern "C" {
    pub static mut enableFsync: bool;
    pub static mut allowSystemTableMods: bool;
    pub static mut work_mem: c_int;
    pub static mut hash_mem_multiplier: f64;
    pub static mut maintenance_work_mem: c_int;
    pub static mut max_parallel_maintenance_workers: c_int;
}

/*
 * Upper and lower hard limits for the buffer access strategy ring size
 * specified by the VacuumBufferUsageLimit GUC and BUFFER_USAGE_LIMIT option
 * to VACUUM and ANALYZE.
 */
pub const MIN_BAS_VAC_RING_SIZE_KB: c_int = 128;
pub const MAX_BAS_VAC_RING_SIZE_KB: c_int = 16 * 1024 * 1024;

extern "C" {
    pub static mut VacuumBufferUsageLimit: c_int;
    pub static mut VacuumCostPageHit: c_int;
    pub static mut VacuumCostPageMiss: c_int;
    pub static mut VacuumCostPageDirty: c_int;
    pub static mut VacuumCostLimit: c_int;
    pub static mut VacuumCostDelay: f64;

    pub static mut VacuumCostBalance: c_int;
    pub static mut VacuumCostActive: bool;

    /* in utils/misc/stack_depth.c */
    pub static mut max_stack_depth: c_int;
}

/* Required daylight between max_stack_depth and the kernel limit, in bytes */
pub const STACK_DEPTH_SLOP: c_int = 512 * 1024;

pub type pg_stack_base_t = *mut c_char;

pub unsafe fn set_stack_base() -> pg_stack_base_t {
    unimplemented!()
}
pub unsafe fn restore_stack_base(_base: pg_stack_base_t) {
    unimplemented!()
}
pub unsafe fn check_stack_depth() {
    unimplemented!()
}
pub unsafe fn stack_is_too_deep() -> bool {
    unimplemented!()
}
pub unsafe fn get_stack_depth_rlimit() -> ssize_t {
    unimplemented!()
}

/* in tcop/utility.c */
pub unsafe fn PreventCommandIfReadOnly(_cmdname: *const c_char) {
    unimplemented!()
}
pub unsafe fn PreventCommandIfParallelMode(_cmdname: *const c_char) {
    unimplemented!()
}
pub unsafe fn PreventCommandDuringRecovery(_cmdname: *const c_char) {
    unimplemented!()
}

/*****************************************************************************
 *	  pdir.h --																 *
 *			POSTGRES directory path definitions.                             *
 *****************************************************************************/

/* flags to be OR'd to form sec_context */
pub const SECURITY_LOCAL_USERID_CHANGE: c_int = 0x0001;
pub const SECURITY_RESTRICTED_OPERATION: c_int = 0x0002;
pub const SECURITY_NOFORCE_RLS: c_int = 0x0004;

extern "C" {
    pub static mut DatabasePath: *mut c_char;
}

/* now in utils/init/miscinit.c */
pub unsafe fn InitPostmasterChild() {
    unimplemented!()
}
pub unsafe fn InitStandaloneProcess(_argv0: *const c_char) {
    unimplemented!()
}
pub unsafe fn InitProcessLocalLatch() {
    unimplemented!()
}
pub unsafe fn SwitchToSharedLatch() {
    unimplemented!()
}
pub unsafe fn SwitchBackToLocalLatch() {
    unimplemented!()
}

/*
 * MyBackendType indicates what kind of a backend this is.
 */
pub type BackendType = c_int;
pub const B_INVALID: BackendType = 0;

/* Backends and other backend-like processes */
pub const B_BACKEND: BackendType = 1;
pub const B_DEAD_END_BACKEND: BackendType = 2;
pub const B_AUTOVAC_LAUNCHER: BackendType = 3;
pub const B_AUTOVAC_WORKER: BackendType = 4;
pub const B_BG_WORKER: BackendType = 5;
pub const B_WAL_SENDER: BackendType = 6;
pub const B_SLOTSYNC_WORKER: BackendType = 7;

pub const B_STANDALONE_BACKEND: BackendType = 8;

/* Auxiliary processes. */
pub const B_ARCHIVER: BackendType = 9;
pub const B_BG_WRITER: BackendType = 10;
pub const B_CHECKPOINTER: BackendType = 11;
pub const B_IO_WORKER: BackendType = 12;
pub const B_STARTUP: BackendType = 13;
pub const B_WAL_RECEIVER: BackendType = 14;
pub const B_WAL_SUMMARIZER: BackendType = 15;
pub const B_WAL_WRITER: BackendType = 16;

/* Logger */
pub const B_LOGGER: BackendType = 17;

pub const BACKEND_NUM_TYPES: c_int = B_LOGGER + 1;

extern "C" {
    pub static mut MyBackendType: BackendType;
}

#[inline]
pub unsafe fn AmRegularBackendProcess() -> bool {
    MyBackendType == B_BACKEND
}
#[inline]
pub unsafe fn AmAutoVacuumLauncherProcess() -> bool {
    MyBackendType == B_AUTOVAC_LAUNCHER
}
#[inline]
pub unsafe fn AmAutoVacuumWorkerProcess() -> bool {
    MyBackendType == B_AUTOVAC_WORKER
}
#[inline]
pub unsafe fn AmBackgroundWorkerProcess() -> bool {
    MyBackendType == B_BG_WORKER
}
#[inline]
pub unsafe fn AmWalSenderProcess() -> bool {
    MyBackendType == B_WAL_SENDER
}
#[inline]
pub unsafe fn AmLogicalSlotSyncWorkerProcess() -> bool {
    MyBackendType == B_SLOTSYNC_WORKER
}
#[inline]
pub unsafe fn AmArchiverProcess() -> bool {
    MyBackendType == B_ARCHIVER
}
#[inline]
pub unsafe fn AmBackgroundWriterProcess() -> bool {
    MyBackendType == B_BG_WRITER
}
#[inline]
pub unsafe fn AmCheckpointerProcess() -> bool {
    MyBackendType == B_CHECKPOINTER
}
#[inline]
pub unsafe fn AmStartupProcess() -> bool {
    MyBackendType == B_STARTUP
}
#[inline]
pub unsafe fn AmWalReceiverProcess() -> bool {
    MyBackendType == B_WAL_RECEIVER
}
#[inline]
pub unsafe fn AmWalSummarizerProcess() -> bool {
    MyBackendType == B_WAL_SUMMARIZER
}
#[inline]
pub unsafe fn AmWalWriterProcess() -> bool {
    MyBackendType == B_WAL_WRITER
}
#[inline]
pub unsafe fn AmIoWorkerProcess() -> bool {
    MyBackendType == B_IO_WORKER
}

#[inline]
pub unsafe fn AmSpecialWorkerProcess() -> bool {
    AmAutoVacuumLauncherProcess() || AmLogicalSlotSyncWorkerProcess()
}

/*
 * Backend types that are spawned by the postmaster to serve a client or
 * replication connection.
 */
#[inline]
pub fn IsExternalConnectionBackend(backend_type: BackendType) -> bool {
    backend_type == B_BACKEND || backend_type == B_WAL_SENDER
}

pub unsafe fn GetBackendTypeDesc(_backendType: BackendType) -> *const c_char {
    unimplemented!()
}

pub unsafe fn SetDatabasePath(_path: *const c_char) {
    unimplemented!()
}
pub unsafe fn checkDataDir() {
    unimplemented!()
}
pub unsafe fn SetDataDir(_dir: *const c_char) {
    unimplemented!()
}
pub unsafe fn ChangeToDataDir() {
    unimplemented!()
}

pub unsafe fn GetUserNameFromId(_roleid: Oid, _noerr: bool) -> *mut c_char {
    unimplemented!()
}
pub unsafe fn GetUserId() -> Oid {
    unimplemented!()
}
pub unsafe fn GetOuterUserId() -> Oid {
    unimplemented!()
}
pub unsafe fn GetSessionUserId() -> Oid {
    unimplemented!()
}
pub unsafe fn GetSessionUserIsSuperuser() -> bool {
    unimplemented!()
}
pub unsafe fn GetAuthenticatedUserId() -> Oid {
    unimplemented!()
}
pub unsafe fn SetAuthenticatedUserId(_userid: Oid) {
    unimplemented!()
}
pub unsafe fn GetUserIdAndSecContext(_userid: *mut Oid, _sec_context: *mut c_int) {
    unimplemented!()
}
pub unsafe fn SetUserIdAndSecContext(_userid: Oid, _sec_context: c_int) {
    unimplemented!()
}
pub unsafe fn InLocalUserIdChange() -> bool {
    unimplemented!()
}
pub unsafe fn InSecurityRestrictedOperation() -> bool {
    unimplemented!()
}
pub unsafe fn InNoForceRLSOperation() -> bool {
    unimplemented!()
}
pub unsafe fn GetUserIdAndContext(_userid: *mut Oid, _sec_def_context: *mut bool) {
    unimplemented!()
}
pub unsafe fn SetUserIdAndContext(_userid: Oid, _sec_def_context: bool) {
    unimplemented!()
}
pub unsafe fn InitializeSessionUserId(
    _rolename: *const c_char,
    _roleid: Oid,
    _bypass_login_check: bool,
) {
    unimplemented!()
}
pub unsafe fn InitializeSessionUserIdStandalone() {
    unimplemented!()
}
pub unsafe fn SetSessionAuthorization(_userid: Oid, _is_superuser: bool) {
    unimplemented!()
}
pub unsafe fn GetCurrentRoleId() -> Oid {
    unimplemented!()
}
pub unsafe fn SetCurrentRoleId(_roleid: Oid, _is_superuser: bool) {
    unimplemented!()
}
pub unsafe fn InitializeSystemUser(_authn_id: *const c_char, _auth_method: *const c_char) {
    unimplemented!()
}
pub unsafe fn GetSystemUser() -> *const c_char {
    unimplemented!()
}

/* in utils/misc/superuser.c */
pub unsafe fn superuser() -> bool {
    unimplemented!()
}
pub unsafe fn superuser_arg(_roleid: Oid) -> bool {
    unimplemented!()
}

/*****************************************************************************
 *	  pmod.h --																 *
 *			POSTGRES processing mode definitions.                            *
 *****************************************************************************/

pub type ProcessingMode = c_int;
pub const BootstrapProcessing: ProcessingMode = 0; /* bootstrap creation of template database */
pub const InitProcessing: ProcessingMode = 1; /* initializing system */
pub const NormalProcessing: ProcessingMode = 2; /* normal processing */

extern "C" {
    pub static mut Mode: ProcessingMode;
}

#[inline]
pub unsafe fn IsBootstrapProcessingMode() -> bool {
    Mode == BootstrapProcessing
}
#[inline]
pub unsafe fn IsInitProcessingMode() -> bool {
    Mode == InitProcessing
}
#[inline]
pub unsafe fn IsNormalProcessingMode() -> bool {
    Mode == NormalProcessing
}

#[inline]
pub unsafe fn GetProcessingMode() -> ProcessingMode {
    Mode
}

#[inline]
pub unsafe fn SetProcessingMode(mode: ProcessingMode) {
    debug_assert!(
        mode == BootstrapProcessing || mode == InitProcessing || mode == NormalProcessing
    );
    Mode = mode;
}

/*****************************************************************************
 *	  pinit.h --															 *
 *			POSTGRES initialization and cleanup definitions.                 *
 *****************************************************************************/

/* in utils/init/postinit.c */
/* flags for InitPostgres() */
pub const INIT_PG_LOAD_SESSION_LIBS: c_int = 0x0001;
pub const INIT_PG_OVERRIDE_ALLOW_CONNS: c_int = 0x0002;
pub const INIT_PG_OVERRIDE_ROLE_LOGIN: c_int = 0x0004;

pub unsafe fn pg_split_opts(_argv: *mut *mut c_char, _argcp: *mut c_int, _optstr: *const c_char) {
    unimplemented!()
}
pub unsafe fn InitializeMaxBackends() {
    unimplemented!()
}
pub unsafe fn InitializeFastPathLocks() {
    unimplemented!()
}
pub unsafe fn InitPostgres(
    _in_dbname: *const c_char,
    _dboid: Oid,
    _username: *const c_char,
    _useroid: Oid,
    _flags: bits32,
    _out_dbname: *mut c_char,
) {
    unimplemented!()
}
pub unsafe fn BaseInit() {
    unimplemented!()
}

/* in utils/init/miscinit.c */
extern "C" {
    pub static mut IgnoreSystemIndexes: bool;
    pub static mut process_shared_preload_libraries_in_progress: bool;
    pub static mut process_shared_preload_libraries_done: bool;
    pub static mut process_shmem_requests_in_progress: bool;
    pub static mut session_preload_libraries_string: *mut c_char;
    pub static mut shared_preload_libraries_string: *mut c_char;
    pub static mut local_preload_libraries_string: *mut c_char;
}

pub unsafe fn CreateDataDirLockFile(_amPostmaster: bool) {
    unimplemented!()
}
pub unsafe fn CreateSocketLockFile(
    _socketfile: *const c_char,
    _amPostmaster: bool,
    _socketDir: *const c_char,
) {
    unimplemented!()
}
pub unsafe fn TouchSocketLockFiles() {
    unimplemented!()
}
pub unsafe fn AddToDataDirLockFile(_target_line: c_int, _str: *const c_char) {
    unimplemented!()
}
pub unsafe fn RecheckDataDirLockFile() -> bool {
    unimplemented!()
}
pub unsafe fn ValidatePgVersion(_path: *const c_char) {
    unimplemented!()
}
pub unsafe fn process_shared_preload_libraries() {
    unimplemented!()
}
pub unsafe fn process_session_preload_libraries() {
    unimplemented!()
}
pub unsafe fn process_shmem_requests() {
    unimplemented!()
}
pub unsafe fn pg_bindtextdomain(_domain: *const c_char) {
    unimplemented!()
}
pub unsafe fn has_rolreplication(_roleid: Oid) -> bool {
    unimplemented!()
}

pub type shmem_request_hook_type = Option<unsafe extern "C" fn()>;
extern "C" {
    pub static mut shmem_request_hook: shmem_request_hook_type;
}

pub unsafe fn EstimateClientConnectionInfoSpace() -> Size {
    unimplemented!()
}
pub unsafe fn SerializeClientConnectionInfo(_maxsize: Size, _start_address: *mut c_char) {
    unimplemented!()
}
pub unsafe fn RestoreClientConnectionInfo(_conninfo: *mut c_char) {
    unimplemented!()
}

/* in executor/nodeHash.c */
pub unsafe fn get_hash_memory_limit() -> Size {
    unimplemented!()
}
