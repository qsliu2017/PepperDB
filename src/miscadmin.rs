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

// --- Signal-handler flags (volatile sig_atomic_t) -> per-task slot (step 09) ---
// The canonical per-task interrupt state is the task's
// `crate::backend::storage::ipc::procsignal::ProcSignalSlot` (cross-task-settable
// atomics). The former `static mut` globals here were a stale mirror; they are
// now `#[deprecated]` accessor functions that read/write the CURRENT task's slot.
// With no slot in scope (aux/test) the readers report "not pending" and the
// writers are no-ops, so flag access never panics. New code should touch the slot
// directly (`procsignal::current()/try_current()`); these C-named shims exist for
// mechanical-port call sites.

use crate::backend::storage::ipc::procsignal;

/// C global `InterruptPending` (read).
#[deprecated(note = "use procsignal::current().flags.interrupt_pending")]
pub fn InterruptPending() -> bool {
    procsignal::try_current()
        .is_some_and(|s| s.flags.interrupt_pending.load(core::sync::atomic::Ordering::Acquire))
}

/// C global `QueryCancelPending` (read).
#[deprecated(note = "use procsignal::current().flags.query_cancel_pending")]
pub fn QueryCancelPending() -> bool {
    procsignal::try_current()
        .is_some_and(|s| s.flags.query_cancel_pending.load(core::sync::atomic::Ordering::Acquire))
}

/// C global `ProcDiePending` (read).
#[deprecated(note = "use procsignal::current().flags.proc_die_pending")]
pub fn ProcDiePending() -> bool {
    procsignal::try_current()
        .is_some_and(|s| s.flags.proc_die_pending.load(core::sync::atomic::Ordering::Acquire))
}

/// C global `ClientConnectionLost` (read).
#[deprecated(note = "use procsignal::current().flags.client_connection_lost")]
pub fn ClientConnectionLost() -> bool {
    procsignal::try_current()
        .is_some_and(|s| s.flags.client_connection_lost.load(core::sync::atomic::Ordering::Acquire))
}

// Holdoff / critical-section counters: per-backend state in PG
// (`InterruptHoldoffCount`, `QueryCancelHoldoffCount`, `CritSectionCount`). They
// live on the per-task `crate::session::Session` -- a HOLD_INTERRUPTS in one
// backend must NOT gate interrupt processing in another. These C-named shims
// read/write the CURRENT task's Session.
//
// NO-SESSION GUARD: with no Session in scope (supervisor, early startup, aux
// setup before a Session, or a test without a session scope) the counters read
// as 0 and the inc/dec writers are no-ops -- never a panic. This matches
// ProcessInterrupts' no-slot no-op behavior.

/// Current `InterruptHoldoffCount` (0 if no Session in scope).
pub fn interrupt_holdoff_count() -> u32 {
    crate::session::try_current().map_or(0, |s| s.interrupt_holdoff_count())
}
/// Current `QueryCancelHoldoffCount` (0 if no Session in scope).
pub fn query_cancel_holdoff_count() -> u32 {
    crate::session::try_current().map_or(0, |s| s.query_cancel_holdoff_count())
}
/// Current `CritSectionCount` (0 if no Session in scope).
pub fn crit_section_count() -> u32 {
    crate::session::try_current().map_or(0, |s| s.crit_section_count())
}

/// C: `void ProcessInterrupts(void)` -- the real implementation lives in
/// tcop/postgres.rs (step 09). Rewired here so existing C-named call sites keep
/// resolving.
pub use crate::backend::tcop::postgres::process_interrupts as ProcessInterrupts;

/// C: `INTERRUPTS_PENDING_CONDITION()`. Reads the current task's slot flag.
pub fn interrupts_pending_condition() -> bool {
    procsignal::try_current()
        .is_some_and(|s| s.flags.interrupt_pending.load(core::sync::atomic::Ordering::Acquire))
}

/// C: `CHECK_FOR_INTERRUPTS()`.
pub fn check_for_interrupts() {
    if interrupts_pending_condition() {
        ProcessInterrupts();
    }
}

/// C: `INTERRUPTS_CAN_BE_PROCESSED()`.
pub fn interrupts_can_be_processed() -> bool {
    interrupt_holdoff_count() == 0 && crit_section_count() == 0 && query_cancel_holdoff_count() == 0
}

// hold/resume/crit operate on the current task's Session. With no Session in
// scope they are no-ops (nothing to hold off); see the NO-SESSION GUARD note.

/// C: `HOLD_INTERRUPTS()`.
pub fn hold_interrupts() {
    if let Some(s) = crate::session::try_current() {
        s.inc_interrupt_holdoff_count();
    }
}
/// C: `RESUME_INTERRUPTS()`.
pub fn resume_interrupts() {
    if let Some(s) = crate::session::try_current() {
        s.dec_interrupt_holdoff_count();
    }
}
/// C: `HOLD_CANCEL_INTERRUPTS()`.
pub fn hold_cancel_interrupts() {
    if let Some(s) = crate::session::try_current() {
        s.inc_query_cancel_holdoff_count();
    }
}
/// C: `RESUME_CANCEL_INTERRUPTS()`.
pub fn resume_cancel_interrupts() {
    if let Some(s) = crate::session::try_current() {
        s.dec_query_cancel_holdoff_count();
    }
}
/// C: `START_CRIT_SECTION()`.
pub fn start_crit_section() {
    if let Some(s) = crate::session::try_current() {
        s.inc_crit_section_count();
    }
}
/// C: `END_CRIT_SECTION()`.
pub fn end_crit_section() {
    if let Some(s) = crate::session::try_current() {
        s.dec_crit_section_count();
    }
}

// --- globals.c process environment flags. TODO(global) ---
pub static mut PostmasterPid: i32 = 0;
pub static mut IsPostmasterEnvironment: bool = false;
pub static mut IsUnderPostmaster: bool = false;
pub static mut IsBinaryUpgrade: bool = false;
pub static mut ExitOnAnyError: bool = false;

// --- globals.c sizing GUCs. TODO(guc: move to ProcessConfig) ---
// These are widely referenced by later steps as plain GUCs; they conceptually
// belong in `ProcessConfig` (see backend::utils::init::globals), but rewiring
// every future reader now would balloon this step for no benefit. They stay as
// miscadmin static-muts as a bridge until the GUC subsystem lands.
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

// MyProcPort (struct Port*), MyLatch (struct Latch*) -> dropped/owned later. TODO(global)
pub static mut MyCancelKeyLength: i32 = 0;
pub static mut MyPMChildSlot: i32 = 0;

// --- globals.c per-task identity -> Session accessors -------------------------
// The identity globals (MyProcPid, MyStartTime[stamp], MyBackendType) and the
// per-database fields (MyDatabaseId, MyDatabaseTableSpace,
// MyDatabaseHasLoginEventTriggers) moved to `crate::session::Session`, published
// as a task-local. These C-named shims read/write the current task's Session;
// new code should call `crate::session::current()` directly.

/// C global `MyProcPid` -> `Session::proc_pid`.
#[deprecated(note = "use crate::session::current().proc_pid()")]
pub fn MyProcPid() -> i32 {
    crate::session::current().proc_pid()
}

/// C global `MyStartTime` -> `Session::start_time`.
#[deprecated(note = "use crate::session::current().start_time()")]
pub fn MyStartTime() -> pg_time_t {
    crate::session::current().start_time()
}

/// C global `MyStartTimestamp` -> `Session::start_timestamp`.
#[deprecated(note = "use crate::session::current().start_timestamp()")]
pub fn MyStartTimestamp() -> TimestampTz {
    crate::session::current().start_timestamp()
}

/// C global `MyDatabaseId` -> `Session::database_id`.
#[deprecated(note = "use crate::session::current().database_id()")]
pub fn MyDatabaseId() -> Oid {
    crate::session::current().database_id()
}

/// C assignment to `MyDatabaseId`.
#[deprecated(note = "use crate::session::current().set_database_id()")]
pub fn SetMyDatabaseId(oid: Oid) {
    crate::session::current().set_database_id(oid);
}

/// C global `MyDatabaseTableSpace` -> `Session::database_tablespace`.
#[deprecated(note = "use crate::session::current().database_tablespace()")]
pub fn MyDatabaseTableSpace() -> Oid {
    crate::session::current().database_tablespace()
}

/// C assignment to `MyDatabaseTableSpace`.
#[deprecated(note = "use crate::session::current().set_database_tablespace()")]
pub fn SetMyDatabaseTableSpace(oid: Oid) {
    crate::session::current().set_database_tablespace(oid);
}

/// C global `MyDatabaseHasLoginEventTriggers`.
#[deprecated(note = "use crate::session::current().database_has_login_event_triggers()")]
pub fn MyDatabaseHasLoginEventTriggers() -> bool {
    crate::session::current().database_has_login_event_triggers()
}

// --- globals.c DataDir / data_directory_mode -> ProcessConfig accessors -------

/// C global `DataDir` -> `ProcessConfig::data_dir`. `None` if config not yet
/// published or the data directory has not been set.
#[deprecated(note = "use SharedState::config().data_dir()")]
pub fn DataDir() -> Option<String> {
    crate::backend::utils::init::globals::process_config().and_then(|c| c.data_dir())
}

/// C `SetDataDir` -> `ProcessConfig::set_data_dir`.
#[deprecated(note = "use SharedState::config().set_data_dir()")]
pub fn SetDataDir(dir: &str) {
    if let Some(c) = crate::backend::utils::init::globals::process_config() {
        c.set_data_dir(dir);
    }
}

/// C global `data_directory_mode` -> `ProcessConfig::data_directory_mode`.
#[deprecated(note = "use SharedState::config().data_directory_mode")]
pub fn data_directory_mode() -> u32 {
    crate::backend::utils::init::globals::process_config()
        .map_or(crate::backend::utils::init::globals::PG_DIR_MODE_OWNER, |c| c.data_directory_mode)
}

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

impl BackendType {
    /// Reconstruct a `BackendType` from its discriminant (`as u32`). Used by
    /// `Session`'s atomic storage. An out-of-range value maps to `INVALID`.
    #[allow(clippy::match_same_arms, reason = "0 == INVALID discriminant; kept explicit for 1:1 PG clarity")]
    pub fn from_u32(v: u32) -> Self {
        use BackendType::{INVALID, BACKEND, DEAD_END_BACKEND, AUTOVAC_LAUNCHER, AUTOVAC_WORKER, BG_WORKER, WAL_SENDER, SLOTSYNC_WORKER, STANDALONE_BACKEND, ARCHIVER, BG_WRITER, CHECKPOINTER, IO_WORKER, STARTUP, WAL_RECEIVER, WAL_SUMMARIZER, WAL_WRITER, LOGGER};
        match v {
            0 => INVALID,
            1 => BACKEND,
            2 => DEAD_END_BACKEND,
            3 => AUTOVAC_LAUNCHER,
            4 => AUTOVAC_WORKER,
            5 => BG_WORKER,
            6 => WAL_SENDER,
            7 => SLOTSYNC_WORKER,
            8 => STANDALONE_BACKEND,
            9 => ARCHIVER,
            10 => BG_WRITER,
            11 => CHECKPOINTER,
            12 => IO_WORKER,
            13 => STARTUP,
            14 => WAL_RECEIVER,
            15 => WAL_SUMMARIZER,
            16 => WAL_WRITER,
            17 => LOGGER,
            _ => INVALID,
        }
    }
}

/// C global `MyBackendType` -> `Session::backend_type`.
#[deprecated(note = "use crate::session::current().backend_type()")]
pub fn MyBackendType() -> BackendType {
    crate::session::current().backend_type()
}

/// C assignment to `MyBackendType`.
#[deprecated(note = "use crate::session::current().set_backend_type()")]
pub fn SetMyBackendType(t: BackendType) {
    crate::session::current().set_backend_type(t);
}

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
// SetDataDir is defined above as a #[deprecated] ProcessConfig accessor.
pub fn ChangeToDataDir() {
    unimplemented!()
}

/// Returns the role name, or None if not found and `noerr` was set (the C
/// `noerr` flag turns an error into a NULL return).
pub fn GetUserNameFromId(roleid: Oid, noerr: bool) -> Option<String> {
    unimplemented!()
}
// --- C-named user-id shims -> real impls over Session (miscinit/usercontext) --
// The real implementations now live in `backend::utils::init::{miscinit,
// usercontext}` over the per-task `Session`. These C-named entry points are
// deprecated thin shims so existing callers keep compiling; new code calls the
// snake_case impls directly. They must not be called internally (no new
// deprecation warnings).

/// C global `GetUserId` -> `miscinit::get_user_id`.
#[deprecated(note = "use crate::backend::utils::init::miscinit::get_user_id()")]
#[inline]
pub fn GetUserId() -> Oid {
    crate::backend::utils::init::miscinit::get_user_id()
}

/// C `GetOuterUserId` -> `miscinit::get_outer_user_id`.
#[deprecated(note = "use crate::backend::utils::init::miscinit::get_outer_user_id()")]
#[inline]
pub fn GetOuterUserId() -> Oid {
    crate::backend::utils::init::miscinit::get_outer_user_id()
}

/// C `GetSessionUserId` -> `miscinit::get_session_user_id`.
#[deprecated(note = "use crate::backend::utils::init::miscinit::get_session_user_id()")]
#[inline]
pub fn GetSessionUserId() -> Oid {
    crate::backend::utils::init::miscinit::get_session_user_id()
}

/// C `GetSessionUserIsSuperuser` -> `miscinit::get_session_user_is_superuser`.
#[deprecated(note = "use crate::backend::utils::init::miscinit::get_session_user_is_superuser()")]
#[inline]
pub fn GetSessionUserIsSuperuser() -> bool {
    crate::backend::utils::init::miscinit::get_session_user_is_superuser()
}

/// C `GetAuthenticatedUserId` -> `miscinit::get_authenticated_user_id`.
#[deprecated(note = "use crate::backend::utils::init::miscinit::get_authenticated_user_id()")]
#[inline]
pub fn GetAuthenticatedUserId() -> Oid {
    crate::backend::utils::init::miscinit::get_authenticated_user_id()
}

/// C `SetAuthenticatedUserId` -> `miscinit::set_authenticated_user_id`.
#[deprecated(note = "use crate::backend::utils::init::miscinit::set_authenticated_user_id()")]
#[inline]
pub fn SetAuthenticatedUserId(userid: Oid) {
    crate::backend::utils::init::miscinit::set_authenticated_user_id(userid);
}

/// C: `GetUserIdAndSecContext(Oid *userid, int *sec_context)` -> (userid, ctx).
#[deprecated(note = "use crate::backend::utils::init::usercontext::get_user_id_and_sec_context()")]
#[inline]
pub fn GetUserIdAndSecContext() -> (Oid, SecurityContext) {
    crate::backend::utils::init::usercontext::get_user_id_and_sec_context()
}

/// C `SetUserIdAndSecContext` -> `usercontext::set_user_id_and_sec_context`.
#[deprecated(note = "use crate::backend::utils::init::usercontext::set_user_id_and_sec_context()")]
#[inline]
pub fn SetUserIdAndSecContext(userid: Oid, sec_context: SecurityContext) {
    crate::backend::utils::init::usercontext::set_user_id_and_sec_context(userid, sec_context);
}

/// C `InLocalUserIdChange` -> `usercontext::in_local_user_id_change`.
#[deprecated(note = "use crate::backend::utils::init::usercontext::in_local_user_id_change()")]
#[inline]
pub fn InLocalUserIdChange() -> bool {
    crate::backend::utils::init::usercontext::in_local_user_id_change()
}

/// C `InSecurityRestrictedOperation` -> `usercontext::in_security_restricted_operation`.
#[deprecated(
    note = "use crate::backend::utils::init::usercontext::in_security_restricted_operation()"
)]
#[inline]
pub fn InSecurityRestrictedOperation() -> bool {
    crate::backend::utils::init::usercontext::in_security_restricted_operation()
}

/// C `InNoForceRLSOperation` -> `usercontext::in_no_force_rls_operation`.
#[deprecated(note = "use crate::backend::utils::init::usercontext::in_no_force_rls_operation()")]
#[inline]
pub fn InNoForceRLSOperation() -> bool {
    crate::backend::utils::init::usercontext::in_no_force_rls_operation()
}

/// C: `GetUserIdAndContext(Oid *userid, bool *sec_def_context)` -> tuple.
// TODO(stub): the boolean-`sec_def_context` legacy variant has no Session-backed
// impl yet (only the SecurityContext-flags form exists in usercontext); left as
// a stub rather than a wrong delegation.
pub fn GetUserIdAndContext() -> (Oid, bool) {
    unimplemented!()
}
// TODO(stub): legacy boolean-context setter; no Session-backed impl yet.
pub fn SetUserIdAndContext(userid: Oid, sec_def_context: bool) {
    unimplemented!()
}

/// C `InitializeSessionUserId` -> `postinit::initialize_session_user_id`.
#[deprecated(note = "use crate::backend::utils::init::postinit::initialize_session_user_id()")]
#[inline]
pub fn InitializeSessionUserId(rolename: &str, roleid: Oid, bypass_login_check: bool) {
    crate::backend::utils::init::postinit::initialize_session_user_id(
        Some(rolename),
        roleid,
        bypass_login_check,
    );
}

/// C `InitializeSessionUserIdStandalone` -> `postinit::initialize_session_user_id_standalone`.
#[deprecated(
    note = "use crate::backend::utils::init::postinit::initialize_session_user_id_standalone()"
)]
#[inline]
pub fn InitializeSessionUserIdStandalone() {
    crate::backend::utils::init::postinit::initialize_session_user_id_standalone();
}

/// C `SetSessionAuthorization` -> `miscinit::set_session_authorization`.
#[deprecated(note = "use crate::backend::utils::init::miscinit::set_session_authorization()")]
#[inline]
pub fn SetSessionAuthorization(userid: Oid, is_superuser: bool) {
    crate::backend::utils::init::miscinit::set_session_authorization(userid, is_superuser);
}

/// C `GetCurrentRoleId` -> `miscinit::get_current_role_id`.
#[deprecated(note = "use crate::backend::utils::init::miscinit::get_current_role_id()")]
#[inline]
pub fn GetCurrentRoleId() -> Oid {
    crate::backend::utils::init::miscinit::get_current_role_id()
}

/// C `SetCurrentRoleId` -> `miscinit::set_current_role_id`.
#[deprecated(note = "use crate::backend::utils::init::miscinit::set_current_role_id()")]
#[inline]
pub fn SetCurrentRoleId(roleid: Oid, is_superuser: bool) {
    crate::backend::utils::init::miscinit::set_current_role_id(roleid, is_superuser);
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
#[repr(u8)]
pub enum ProcessingMode {
    BootstrapProcessing,
    InitProcessing,
    NormalProcessing,
}

// PG `Mode` is a per-process global; in the single-process async model every task
// shares it, so it is an atomic (tasks set NormalProcessing at startup concurrently).
static MODE: std::sync::atomic::AtomicU8 = std::sync::atomic::AtomicU8::new(ProcessingMode::InitProcessing as u8);

impl ProcessingMode {
    fn from_u8(v: u8) -> Self {
        match v {
            x if x == Self::BootstrapProcessing as u8 => Self::BootstrapProcessing,
            x if x == Self::NormalProcessing as u8 => Self::NormalProcessing,
            _ => Self::InitProcessing,
        }
    }
}

pub fn is_bootstrap_processing_mode() -> bool {
    get_processing_mode() == ProcessingMode::BootstrapProcessing
}
pub fn is_init_processing_mode() -> bool {
    get_processing_mode() == ProcessingMode::InitProcessing
}
pub fn is_normal_processing_mode() -> bool {
    get_processing_mode() == ProcessingMode::NormalProcessing
}
pub fn get_processing_mode() -> ProcessingMode {
    ProcessingMode::from_u8(MODE.load(std::sync::atomic::Ordering::Relaxed))
}
pub fn set_processing_mode(mode: ProcessingMode) {
    MODE.store(mode as u8, std::sync::atomic::Ordering::Relaxed);
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
