//! Translated from PostgreSQL src/include/miscadmin.h
//! General administration, initialization, and process-state globals.
//
// Almost all of the PGDLLIMPORT externs here are process-global mutable state
// (GUCs, signal flags, identity). Under the single-process async model these
// become task-local / Session state later; for the skeleton they are
// `static mut` with TODO(global). The interrupt/crit-section macros become fns
// operating on those globals.

use crate::datatype::timestamp::TimestampTz;
use crate::pgtime::pg_time_t;
use crate::postgres_ext::Oid;
use bitflags::bitflags;

pub const InvalidPid: i32 = -1;

// --- Signal-handler flags (volatile sig_atomic_t). TODO(global) ---
// TODO(step09): these globals are a stale mirror. The canonical per-task
// interrupt state is `crate::backend::storage::ipc::procsignal::ProcSignalSlot`
// (cross-task-settable atomics). ProcessInterrupts (step 09) will read/clear the
// slot flags; this machinery is retired then. Do not rewire here.
pub static mut InterruptPending: bool = false;
pub static mut QueryCancelPending: bool = false;
pub static mut ProcDiePending: bool = false;
pub static mut IdleInTransactionSessionTimeoutPending: bool = false;
pub static mut TransactionTimeoutPending: bool = false;
pub static mut IdleSessionTimeoutPending: bool = false;
pub static mut ProcSignalBarrierPending: bool = false;
pub static mut LogMemoryContextPending: bool = false;
pub static mut IdleStatsUpdateTimeoutPending: bool = false;
pub static mut CheckClientConnectionPending: bool = false;
pub static mut ClientConnectionLost: bool = false;

pub static mut InterruptHoldoffCount: u32 = 0;
pub static mut QueryCancelHoldoffCount: u32 = 0;
pub static mut CritSectionCount: u32 = 0;

/// C: `void ProcessInterrupts(void)` (tcop/postgres.c).
pub fn ProcessInterrupts() {
    unimplemented!()
}

/// C: `INTERRUPTS_PENDING_CONDITION()`.
pub fn interrupts_pending_condition() -> bool {
    unsafe { InterruptPending }
}

/// C: `CHECK_FOR_INTERRUPTS()`.
pub fn check_for_interrupts() {
    if interrupts_pending_condition() {
        ProcessInterrupts();
    }
}

/// C: `INTERRUPTS_CAN_BE_PROCESSED()`.
pub fn interrupts_can_be_processed() -> bool {
    unsafe { InterruptHoldoffCount == 0 && CritSectionCount == 0 && QueryCancelHoldoffCount == 0 }
}

/// C: `HOLD_INTERRUPTS()`.
pub fn hold_interrupts() {
    unsafe { InterruptHoldoffCount += 1 }
}
/// C: `RESUME_INTERRUPTS()`.
pub fn resume_interrupts() {
    unsafe {
        debug_assert!(InterruptHoldoffCount > 0);
        InterruptHoldoffCount -= 1;
    }
}
/// C: `HOLD_CANCEL_INTERRUPTS()`.
pub fn hold_cancel_interrupts() {
    unsafe { QueryCancelHoldoffCount += 1 }
}
/// C: `RESUME_CANCEL_INTERRUPTS()`.
pub fn resume_cancel_interrupts() {
    unsafe {
        debug_assert!(QueryCancelHoldoffCount > 0);
        QueryCancelHoldoffCount -= 1;
    }
}
/// C: `START_CRIT_SECTION()`.
pub fn start_crit_section() {
    unsafe { CritSectionCount += 1 }
}
/// C: `END_CRIT_SECTION()`.
pub fn end_crit_section() {
    unsafe {
        debug_assert!(CritSectionCount > 0);
        CritSectionCount -= 1;
    }
}

// --- globals.c identity / config. TODO(global) ---
pub static mut PostmasterPid: i32 = 0;
pub static mut IsPostmasterEnvironment: bool = false;
pub static mut IsUnderPostmaster: bool = false;
pub static mut IsBinaryUpgrade: bool = false;
pub static mut ExitOnAnyError: bool = false;
pub static mut DataDir: Option<String> = None;
pub static mut data_directory_mode: i32 = 0;
pub static mut NBuffers: i32 = 0;
pub static mut MaxBackends: i32 = 0;
pub static mut MaxConnections: i32 = 0;
pub static mut max_worker_processes: i32 = 0;
pub static mut max_parallel_workers: i32 = 0;
pub static mut commit_timestamp_buffers: i32 = 0;
pub static mut multixact_member_buffers: i32 = 0;
pub static mut multixact_offset_buffers: i32 = 0;
pub static mut notify_buffers: i32 = 0;
pub static mut serializable_buffers: i32 = 0;
pub static mut subtransaction_buffers: i32 = 0;
pub static mut transaction_buffers: i32 = 0;
pub static mut MyProcPid: i32 = 0;
pub static mut MyStartTime: pg_time_t = 0;
pub static mut MyStartTimestamp: TimestampTz = 0;
// MyProcPort (struct Port*), MyLatch (struct Latch*) -> dropped/owned later. TODO(global)
pub static mut MyCancelKeyLength: i32 = 0;
pub static mut MyPMChildSlot: i32 = 0;
pub static mut MyDatabaseId: Oid = Oid(0);
pub static mut MyDatabaseTableSpace: Oid = Oid(0);
pub static mut MyDatabaseHasLoginEventTriggers: bool = false;

// --- Date/time configuration GUC value codes (C #defines) ---
pub const USE_POSTGRES_DATES: i32 = 0;
pub const USE_ISO_DATES: i32 = 1;
pub const USE_SQL_DATES: i32 = 2;
pub const USE_GERMAN_DATES: i32 = 3;
pub const USE_XSD_DATES: i32 = 4;

pub const DATEORDER_YMD: i32 = 0;
pub const DATEORDER_DMY: i32 = 1;
pub const DATEORDER_MDY: i32 = 2;

pub static mut DateStyle: i32 = 0;
pub static mut DateOrder: i32 = 0;

pub const INTSTYLE_POSTGRES: i32 = 0;
pub const INTSTYLE_POSTGRES_VERBOSE: i32 = 1;
pub const INTSTYLE_SQL_STANDARD: i32 = 2;
pub const INTSTYLE_ISO_8601: i32 = 3;

pub static mut IntervalStyle: i32 = 0;

pub const MAXTZLEN: usize = 10;

pub static mut enableFsync: bool = false;
pub static mut allowSystemTableMods: bool = false;
pub static mut work_mem: i32 = 0;
pub static mut hash_mem_multiplier: f64 = 0.0;
pub static mut maintenance_work_mem: i32 = 0;
pub static mut max_parallel_maintenance_workers: i32 = 0;

pub const MIN_BAS_VAC_RING_SIZE_KB: i32 = 128;
pub const MAX_BAS_VAC_RING_SIZE_KB: i32 = 16 * 1024 * 1024;

pub static mut VacuumBufferUsageLimit: i32 = 0;
pub static mut VacuumCostPageHit: i32 = 0;
pub static mut VacuumCostPageMiss: i32 = 0;
pub static mut VacuumCostPageDirty: i32 = 0;
pub static mut VacuumCostLimit: i32 = 0;
pub static mut VacuumCostDelay: f64 = 0.0;
pub static mut VacuumCostBalance: i32 = 0;
pub static mut VacuumCostActive: bool = false;

pub static mut max_stack_depth: i32 = 0;
pub const STACK_DEPTH_SLOP: i32 = 512 * 1024;

/// C: `typedef char *pg_stack_base_t;` - opaque stack-base marker.
pub type pg_stack_base_t = usize; // TODO(ptr)

pub fn set_stack_base() -> pg_stack_base_t {
    unimplemented!()
}
pub fn restore_stack_base(base: pg_stack_base_t) {
    unimplemented!()
}
pub fn check_stack_depth() {
    unimplemented!()
}
pub fn stack_is_too_deep() -> bool {
    unimplemented!()
}
pub fn get_stack_depth_rlimit() -> isize {
    unimplemented!()
}

pub fn PreventCommandIfReadOnly(cmdname: &str) {
    unimplemented!()
}
pub fn PreventCommandIfParallelMode(cmdname: &str) {
    unimplemented!()
}
pub fn PreventCommandDuringRecovery(cmdname: &str) {
    unimplemented!()
}

bitflags! {
    /// Flags OR'd to form a `sec_context`. (appendix A: GOOD bitflags set.)
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct SecurityContext: i32 {
        const LOCAL_USERID_CHANGE   = 0x0001;
        const RESTRICTED_OPERATION  = 0x0002;
        const NOFORCE_RLS           = 0x0004;
    }
}

pub static mut DatabasePath: Option<String> = None;

pub fn InitPostmasterChild() {
    unimplemented!()
}
pub fn InitStandaloneProcess(argv0: &str) {
    unimplemented!()
}
pub fn InitProcessLocalLatch() {
    unimplemented!()
}
pub fn SwitchToSharedLatch() {
    unimplemented!()
}
pub fn SwitchBackToLocalLatch() {
    unimplemented!()
}

/// What kind of backend this process is. (C enum BackendType, sequential.)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendType {
    INVALID = 0,
    BACKEND,
    DEAD_END_BACKEND,
    AUTOVAC_LAUNCHER,
    AUTOVAC_WORKER,
    BG_WORKER,
    WAL_SENDER,
    SLOTSYNC_WORKER,
    STANDALONE_BACKEND,
    ARCHIVER,
    BG_WRITER,
    CHECKPOINTER,
    IO_WORKER,
    STARTUP,
    WAL_RECEIVER,
    WAL_SUMMARIZER,
    WAL_WRITER,
    LOGGER,
}

pub const BACKEND_NUM_TYPES: usize = BackendType::LOGGER as usize + 1;

pub static mut MyBackendType: BackendType = BackendType::INVALID;

/// C: `IsExternalConnectionBackend(t)`.
pub fn is_external_connection_backend(t: BackendType) -> bool {
    matches!(t, BackendType::BACKEND | BackendType::WAL_SENDER)
}

pub fn GetBackendTypeDesc(backend_type: BackendType) -> &'static str {
    unimplemented!()
}

pub fn SetDatabasePath(path: &str) {
    unimplemented!()
}
pub fn checkDataDir() {
    unimplemented!()
}
pub fn SetDataDir(dir: &str) {
    unimplemented!()
}
pub fn ChangeToDataDir() {
    unimplemented!()
}

/// Returns the role name, or None if not found and `noerr` was set (the C
/// `noerr` flag turns an error into a NULL return).
pub fn GetUserNameFromId(roleid: Oid, noerr: bool) -> Option<String> {
    unimplemented!()
}
pub fn GetUserId() -> Oid {
    unimplemented!()
}
pub fn GetOuterUserId() -> Oid {
    unimplemented!()
}
pub fn GetSessionUserId() -> Oid {
    unimplemented!()
}
pub fn GetSessionUserIsSuperuser() -> bool {
    unimplemented!()
}
pub fn GetAuthenticatedUserId() -> Oid {
    unimplemented!()
}
pub fn SetAuthenticatedUserId(userid: Oid) {
    unimplemented!()
}
/// C: `GetUserIdAndSecContext(Oid *userid, int *sec_context)` -> (userid, ctx).
pub fn GetUserIdAndSecContext() -> (Oid, SecurityContext) {
    unimplemented!()
}
pub fn SetUserIdAndSecContext(userid: Oid, sec_context: SecurityContext) {
    unimplemented!()
}
pub fn InLocalUserIdChange() -> bool {
    unimplemented!()
}
pub fn InSecurityRestrictedOperation() -> bool {
    unimplemented!()
}
pub fn InNoForceRLSOperation() -> bool {
    unimplemented!()
}
/// C: `GetUserIdAndContext(Oid *userid, bool *sec_def_context)` -> tuple.
pub fn GetUserIdAndContext() -> (Oid, bool) {
    unimplemented!()
}
pub fn SetUserIdAndContext(userid: Oid, sec_def_context: bool) {
    unimplemented!()
}
pub fn InitializeSessionUserId(rolename: &str, roleid: Oid, bypass_login_check: bool) {
    unimplemented!()
}
pub fn InitializeSessionUserIdStandalone() {
    unimplemented!()
}
pub fn SetSessionAuthorization(userid: Oid, is_superuser: bool) {
    unimplemented!()
}
pub fn GetCurrentRoleId() -> Oid {
    unimplemented!()
}
pub fn SetCurrentRoleId(roleid: Oid, is_superuser: bool) {
    unimplemented!()
}
pub fn InitializeSystemUser(authn_id: &str, auth_method: &str) {
    unimplemented!()
}
pub fn GetSystemUser() -> Option<String> {
    unimplemented!()
}
pub fn superuser() -> bool {
    unimplemented!()
}
pub fn superuser_arg(roleid: Oid) -> bool {
    unimplemented!()
}

/// The three POSTGRES processing modes. (C enum, sequential.)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessingMode {
    BootstrapProcessing,
    InitProcessing,
    NormalProcessing,
}

pub static mut Mode: ProcessingMode = ProcessingMode::InitProcessing;

pub fn is_bootstrap_processing_mode() -> bool {
    unsafe { Mode == ProcessingMode::BootstrapProcessing }
}
pub fn is_init_processing_mode() -> bool {
    unsafe { Mode == ProcessingMode::InitProcessing }
}
pub fn is_normal_processing_mode() -> bool {
    unsafe { Mode == ProcessingMode::NormalProcessing }
}
pub fn get_processing_mode() -> ProcessingMode {
    unsafe { Mode }
}
pub fn set_processing_mode(mode: ProcessingMode) {
    unsafe { Mode = mode }
}

bitflags! {
    /// Flags for InitPostgres(). (appendix A: GOOD bitflags set.)
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct InitPgFlags: u32 {
        const LOAD_SESSION_LIBS   = 0x0001;
        const OVERRIDE_ALLOW_CONNS = 0x0002;
        const OVERRIDE_ROLE_LOGIN = 0x0004;
    }
}

/// C: `pg_split_opts(char **argv, int *argcp, const char *optstr)` - splits
/// `optstr` into args appended to the returned vector.
pub fn pg_split_opts(optstr: &str) -> Vec<String> {
    unimplemented!()
}
pub fn InitializeMaxBackends() {
    unimplemented!()
}
pub fn InitializeFastPathLocks() {
    unimplemented!()
}
/// C: out-param `char *out_dbname` -> returned String.
pub fn InitPostgres(
    in_dbname: Option<&str>,
    dboid: Oid,
    username: Option<&str>,
    useroid: Oid,
    flags: InitPgFlags,
) -> String {
    unimplemented!()
}
pub fn BaseInit() {
    unimplemented!()
}

pub static mut IgnoreSystemIndexes: bool = false;
pub static mut process_shared_preload_libraries_in_progress: bool = false;
pub static mut process_shared_preload_libraries_done: bool = false;
pub static mut process_shmem_requests_in_progress: bool = false;
pub static mut session_preload_libraries_string: Option<String> = None;
pub static mut shared_preload_libraries_string: Option<String> = None;
pub static mut local_preload_libraries_string: Option<String> = None;

pub fn CreateDataDirLockFile(am_postmaster: bool) {
    unimplemented!()
}
pub fn CreateSocketLockFile(socketfile: &str, am_postmaster: bool, socket_dir: &str) {
    unimplemented!()
}
pub fn TouchSocketLockFiles() {
    unimplemented!()
}
pub fn AddToDataDirLockFile(target_line: i32, s: &str) {
    unimplemented!()
}
pub fn RecheckDataDirLockFile() -> bool {
    unimplemented!()
}
pub fn ValidatePgVersion(path: &str) {
    unimplemented!()
}
pub fn process_shared_preload_libraries() {
    unimplemented!()
}
pub fn process_session_preload_libraries() {
    unimplemented!()
}
pub fn process_shmem_requests() {
    unimplemented!()
}
pub fn pg_bindtextdomain(domain: &str) {
    unimplemented!()
}
pub fn has_rolreplication(roleid: Oid) -> bool {
    unimplemented!()
}

/// C: `typedef void (*shmem_request_hook_type) (void);`
pub type shmem_request_hook_type = fn();
pub static mut shmem_request_hook: Option<shmem_request_hook_type> = None;

pub fn EstimateClientConnectionInfoSpace() -> usize {
    unimplemented!()
}
pub fn SerializeClientConnectionInfo(maxsize: usize, start_address: &mut [u8]) {
    unimplemented!()
}
pub fn RestoreClientConnectionInfo(conninfo: &[u8]) {
    unimplemented!()
}

pub fn get_hash_memory_limit() -> usize {
    unimplemented!()
}
