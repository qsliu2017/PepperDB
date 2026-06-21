//! postmaster/startup.c - the Startup process that initializes the server and
//! performs recovery actions.
//!
//! The Startup process initialises the server and performs any recovery
//! actions that have been specified. Notice that there is no "main loop"
//! since the Startup process ends as soon as initialisation is complete.
//! (in standby mode, one can think of the replay loop as a main loop, though.)

use crate::prelude::*;

use crate::libpq::pqsignal::{
    pqsignal, sigset_t, SigHandler, UnBlockSig, SIGCHLD, SIGHUP, SIGINT, SIGPIPE, SIGTERM, SIGUSR1,
    SIGUSR2, SIG_DFL,
};
use crate::postmaster::auxprocess::AuxiliaryProcessMainCommon;

// MyBackendType is an extern mutable global (miscadmin.h); B_STARTUP is its enum
// value for the startup process.
use crate::miscadmin::B_STARTUP;

// sig_atomic_t / TimestampTz come from globals.h / timestamp.h.
type sig_atomic_t = c_int;
type TimestampTz = int64;

extern "C" {
    static mut MyBackendType: c_int;
    // IsUnderPostmaster - true in a postmaster child process (globals.h).
    static mut IsUnderPostmaster: bool;
}

// sigprocmask(2) - libc. TODO: route through a ported port-layer wrapper.
extern "C" {
    fn sigprocmask(how: c_int, set: *const sigset_t, oldset: *mut sigset_t) -> c_int;
}

// SIG_SETMASK from <signal.h>; on most platforms this is 2.
const SIG_SETMASK: c_int = 2;

// SIG_IGN is the function pointer with integer value 1 (pqsignal.rs models
// SIG_IGN separately; construct it here as a function-pointer sentinel).
#[allow(clippy::missing_transmute_annotations)]
fn sig_ign() -> SigHandler {
    unsafe { Some(core::mem::transmute::<usize, unsafe extern "C" fn(c_int)>(1usize)) }
}

/*
 * On systems that need to make a system call to find out if the postmaster has
 * gone away, we'll do so only every Nth call to ProcessStartupProcInterrupts().
 * This only affects how long it takes us to detect the condition while we're
 * busy replaying WAL.  Latch waits and similar which should react immediately
 * through the usual techniques.
 */
const POSTMASTER_POLL_RATE_LIMIT: u32 = 1024;

/*
 * Flags set by interrupt handlers for later service in the redo loop.
 */
static mut got_SIGHUP: sig_atomic_t = false as sig_atomic_t;
static mut shutdown_requested: sig_atomic_t = false as sig_atomic_t;
static mut promote_signaled: sig_atomic_t = false as sig_atomic_t;

/*
 * Flag set when executing a restore command, to tell SIGTERM signal handler
 * that it's safe to just proc_exit.
 */
static mut in_restore_command: sig_atomic_t = false as sig_atomic_t;

/*
 * Time at which the most recent startup operation started.
 */
static mut startup_progress_phase_start_time: TimestampTz = 0;

/*
 * Indicates whether the startup progress interval mentioned by the user is
 * elapsed or not. TRUE if timeout occurred, FALSE otherwise.
 */
static mut startup_progress_timer_expired: sig_atomic_t = false as sig_atomic_t;

/*
 * Time between progress updates for long-running startup operations.
 */
#[no_mangle]
pub static mut log_startup_progress_interval: c_int = 10000; /* 10 sec */

/* --------------------------------
 *		signal handler routines
 * --------------------------------
 */

/* SIGUSR2: set flag to finish recovery */
unsafe extern "C" fn StartupProcTriggerHandler(_postgres_signal_arg: c_int) {
    promote_signaled = true as sig_atomic_t;
    WakeupRecovery();
}

/* SIGHUP: set flag to re-read config file at next convenient time */
unsafe extern "C" fn StartupProcSigHupHandler(_postgres_signal_arg: c_int) {
    got_SIGHUP = true as sig_atomic_t;
    WakeupRecovery();
}

/* SIGTERM: set flag to abort redo and exit */
unsafe extern "C" fn StartupProcShutdownHandler(_postgres_signal_arg: c_int) {
    if in_restore_command != 0 {
        proc_exit(1);
    } else {
        shutdown_requested = true as sig_atomic_t;
    }
    WakeupRecovery();
}

/*
 * Re-read the config file.
 *
 * If one of the critical walreceiver options has changed, flag xlog.c
 * to restart it.
 */
unsafe fn StartupRereadConfig() {
    let conninfo = pstrdup(PrimaryConnInfo);
    let slotname = pstrdup(PrimarySlotName);
    let tempSlot = wal_receiver_create_temp_slot;
    let conninfoChanged;
    let slotnameChanged;
    let mut tempSlotChanged = false;

    ProcessConfigFile(PGC_SIGHUP);

    conninfoChanged = libc_strcmp(conninfo, PrimaryConnInfo) != 0;
    slotnameChanged = libc_strcmp(slotname, PrimarySlotName) != 0;

    /*
     * wal_receiver_create_temp_slot is used only when we have no slot
     * configured.  We do not need to track this change if it has no effect.
     */
    if !slotnameChanged && libc_strcmp(PrimarySlotName, c"".as_ptr()) == 0 {
        tempSlotChanged = tempSlot != wal_receiver_create_temp_slot;
    }
    pfree(conninfo as *mut c_void);
    pfree(slotname as *mut c_void);

    if conninfoChanged || slotnameChanged || tempSlotChanged {
        StartupRequestWalReceiverRestart();
    }
}

/* Process various signals that might be sent to the startup process */
pub unsafe fn ProcessStartupProcInterrupts() {
    static mut postmaster_poll_count: u32 = 0;

    /*
     * Process any requests or signals received recently.
     */
    if got_SIGHUP != 0 {
        got_SIGHUP = false as sig_atomic_t;
        StartupRereadConfig();
    }

    /*
     * Check if we were requested to exit without finishing recovery.
     */
    if shutdown_requested != 0 {
        proc_exit(1);
    }

    /*
     * Emergency bailout if postmaster has died.  This is to avoid the
     * necessity for manual cleanup of all postmaster children.  Do this less
     * frequently on systems for which we don't have signals to make that
     * cheap.
     */
    if IsUnderPostmaster
        && {
            let do_poll = postmaster_poll_count % POSTMASTER_POLL_RATE_LIMIT == 0;
            postmaster_poll_count = postmaster_poll_count.wrapping_add(1);
            do_poll
        }
        && !PostmasterIsAlive()
    {
        libc_exit(1);
    }

    /* Process barrier events */
    if ProcSignalBarrierPending {
        ProcessProcSignalBarrier();
    }

    /* Perform logging of memory contexts of this process */
    if LogMemoryContextPending {
        ProcessLogMemoryContextInterrupt();
    }
}

/* --------------------------------
 *		signal handler routines
 * --------------------------------
 */
unsafe extern "C" fn StartupProcExit(_code: c_int, _arg: Datum) {
    /* Shutdown the recovery environment */
    if standbyState != STANDBY_DISABLED {
        ShutdownRecoveryTransactionEnvironment();
    }
}

/* ----------------------------------
 *	Startup Process main entry point
 * ----------------------------------
 */
pub unsafe fn StartupProcessMain(_startup_data: *const c_void, startup_data_len: Size) {
    Assert!(startup_data_len == 0);

    MyBackendType = B_STARTUP;
    AuxiliaryProcessMainCommon();

    /* Arrange to clean up at startup process exit */
    on_shmem_exit(Some(StartupProcExit), 0);

    /*
     * Properly accept or ignore signals the postmaster might send us.
     */
    pqsignal(SIGHUP, Some(StartupProcSigHupHandler)); /* reload config file */
    pqsignal(SIGINT, sig_ign()); /* ignore query cancel */
    pqsignal(SIGTERM, Some(StartupProcShutdownHandler)); /* request shutdown */
    /* SIGQUIT handler was already set up by InitPostmasterChild */
    InitializeTimeouts(); /* establishes SIGALRM handler */
    pqsignal(SIGPIPE, sig_ign());
    pqsignal(SIGUSR1, Some(procsignal_sigusr1_handler));
    pqsignal(SIGUSR2, Some(StartupProcTriggerHandler));

    /*
     * Reset some signals that are accepted by postmaster but not here
     */
    pqsignal(SIGCHLD, SIG_DFL);

    /*
     * Register timeouts needed for standby mode
     */
    RegisterTimeout(STANDBY_DEADLOCK_TIMEOUT, StandbyDeadLockHandler);
    RegisterTimeout(STANDBY_TIMEOUT, StandbyTimeoutHandler);
    RegisterTimeout(STANDBY_LOCK_TIMEOUT, StandbyLockTimeoutHandler);

    /*
     * Unblock signals (they were blocked when the postmaster forked us)
     */
    sigprocmask(SIG_SETMASK, &raw const UnBlockSig, core::ptr::null_mut());

    /*
     * Do what we came for.
     */
    StartupXLOG();

    /*
     * Exit normally. Exit code 0 tells postmaster that we completed recovery
     * successfully.
     */
    proc_exit(0);
}

pub unsafe fn PreRestoreCommand() {
    /*
     * Set in_restore_command to tell the signal handler that we should exit
     * right away on SIGTERM. We know that we're at a safe point to do that.
     * Check if we had already received the signal, so that we don't miss a
     * shutdown request received just before this.
     */
    in_restore_command = true as sig_atomic_t;
    if shutdown_requested != 0 {
        proc_exit(1);
    }
}

pub unsafe fn PostRestoreCommand() {
    in_restore_command = false as sig_atomic_t;
}

pub unsafe fn IsPromoteSignaled() -> bool {
    promote_signaled != 0
}

pub unsafe fn ResetPromoteSignaled() {
    promote_signaled = false as sig_atomic_t;
}

/*
 * Set a flag indicating that it's time to log a progress report.
 */
pub unsafe extern "C" fn startup_progress_timeout_handler() {
    startup_progress_timer_expired = true as sig_atomic_t;
}

pub unsafe fn disable_startup_progress_timeout() {
    /* Feature is disabled. */
    if log_startup_progress_interval == 0 {
        return;
    }

    disable_timeout(STARTUP_PROGRESS_TIMEOUT, false);
    startup_progress_timer_expired = false as sig_atomic_t;
}

/*
 * Set the start timestamp of the current operation and enable the timeout.
 */
pub unsafe fn enable_startup_progress_timeout() {
    let fin_time: TimestampTz;

    /* Feature is disabled. */
    if log_startup_progress_interval == 0 {
        return;
    }

    startup_progress_phase_start_time = GetCurrentTimestamp();
    fin_time = TimestampTzPlusMilliseconds(
        startup_progress_phase_start_time,
        log_startup_progress_interval as int64,
    );
    enable_timeout_every(
        STARTUP_PROGRESS_TIMEOUT,
        fin_time,
        log_startup_progress_interval as int64,
    );
}

/*
 * A thin wrapper to first disable and then enable the startup progress
 * timeout.
 */
pub unsafe fn begin_startup_progress_phase() {
    /* Feature is disabled. */
    if log_startup_progress_interval == 0 {
        return;
    }

    disable_startup_progress_timeout();
    enable_startup_progress_timeout();
}

/*
 * Report whether startup progress timeout has occurred. Reset the timer flag
 * if it did, set the elapsed time to the out parameters and return true,
 * otherwise return false.
 */
pub unsafe fn has_startup_progress_timeout_expired(secs: *mut c_long, usecs: *mut c_int) -> bool {
    let mut seconds: c_long = 0;
    let mut useconds: c_int = 0;
    let now: TimestampTz;

    /* No timeout has occurred. */
    if startup_progress_timer_expired == 0 {
        return false;
    }

    /* Calculate the elapsed time. */
    now = GetCurrentTimestamp();
    TimestampDifference(
        startup_progress_phase_start_time,
        now,
        &mut seconds,
        &mut useconds,
    );

    *secs = seconds;
    *usecs = useconds;
    startup_progress_timer_expired = false as sig_atomic_t;

    true
}

// ---------------------------------------------------------------------------
// Locally-stubbed dependencies (not yet ported).
// ---------------------------------------------------------------------------

// libc strcmp / exit - used directly for char* comparison and emergency bailout.
extern "C" {
    #[link_name = "strcmp"]
    fn libc_strcmp(a: *const c_char, b: *const c_char) -> c_int;
    #[link_name = "exit"]
    fn libc_exit(code: c_int) -> !;
}

// access/xlogrecovery.h - standbyState tracks recovery standby state.
const STANDBY_DISABLED: c_int = 0;
static mut standbyState: c_int = STANDBY_DISABLED;

// utils/timeout.h - timeout identifier IDs.
type TimeoutId = c_int;
const STANDBY_DEADLOCK_TIMEOUT: TimeoutId = 0;
const STANDBY_TIMEOUT: TimeoutId = 0;
const STANDBY_LOCK_TIMEOUT: TimeoutId = 0;
const STARTUP_PROGRESS_TIMEOUT: TimeoutId = 0;
type timeout_handler_proc = unsafe extern "C" fn();

// xlog.h GUCs: walreceiver connection settings (guc.c). TODO: not ported.
static mut PrimaryConnInfo: *mut c_char = core::ptr::null_mut();
static mut PrimarySlotName: *mut c_char = core::ptr::null_mut();
static mut wal_receiver_create_temp_slot: bool = false;

// utils/guc.h - GUC context for ProcessConfigFile.
const PGC_SIGHUP: c_int = 0;

// access/xlog.h - wake the recovery process / request walreceiver restart.
unsafe fn WakeupRecovery() { crate::access::transam::xlogrecovery::WakeupRecovery() }
unsafe fn StartupRequestWalReceiverRestart() { crate::access::transam::xlogrecovery::StartupRequestWalReceiverRestart() }
unsafe fn StartupXLOG() {
    crate::access::transam::xlog::StartupXLOG()
}

// utils/guc.h - re-read the config file.
unsafe fn ProcessConfigFile(context: c_int) { unimplemented!() }

// storage/ipc.h - process-exit machinery.
unsafe fn proc_exit(_code: c_int) -> ! {
    crate::storage::ipc::ipc::proc_exit(_code)
}
unsafe fn on_shmem_exit(_function: Option<unsafe extern "C" fn(c_int, Datum)>, _arg: Datum) {
    crate::storage::ipc::ipc::on_shmem_exit(core::mem::transmute(_function), _arg)
}

// storage/pmsignal.h - is the postmaster still alive?
unsafe fn PostmasterIsAlive() -> bool {
    crate::storage::ipc::pmsignal::PostmasterIsAliveInternal()
}

// storage/procsignal.h - SIGUSR1 handler and barrier processing.
unsafe extern "C" fn procsignal_sigusr1_handler(postgres_signal_arg: c_int) { crate::storage::ipc::procsignal::procsignal_sigusr1_handler(postgres_signal_arg as _) }
static mut ProcSignalBarrierPending: bool = false;
unsafe fn ProcessProcSignalBarrier() {
    crate::storage::ipc::procsignal::ProcessProcSignalBarrier()
}

// miscadmin.h / utils/mmgr - log-memory-contexts request handling.
static mut LogMemoryContextPending: bool = false;
unsafe fn ProcessLogMemoryContextInterrupt() {
    crate::utils::mmgr::mcxt::ProcessLogMemoryContextInterrupt()
}

// access/xlogutils.h - shutdown the recovery transaction environment.
unsafe fn ShutdownRecoveryTransactionEnvironment() {
    // exit-time recovery-txn cleanup; storage/ipc/standby.rs is unwired -> no-op for bring-up.
}

// utils/timeout.h - timeout registration / control.
unsafe fn InitializeTimeouts() {
    crate::utils::misc::timeout::InitializeTimeouts()
}
unsafe fn RegisterTimeout(_id: TimeoutId, _handler: timeout_handler_proc) {
    crate::utils::misc::timeout::RegisterTimeout(core::mem::transmute(_id), core::mem::transmute(_handler));
}
unsafe fn disable_timeout(_id: TimeoutId, _keep_indicator: bool) {
    crate::utils::misc::timeout::disable_timeout(core::mem::transmute(_id), _keep_indicator)
}
unsafe fn enable_timeout_every(_id: TimeoutId, _fin_time: TimestampTz, _delay_ms: int64) {
    crate::utils::misc::timeout::enable_timeout_every(core::mem::transmute(_id), _fin_time as _, _delay_ms as c_int)
}

// storage/standby.h - standby timeout handlers.
unsafe extern "C" fn StandbyDeadLockHandler() { unimplemented!() }
unsafe extern "C" fn StandbyTimeoutHandler() { unimplemented!() }
unsafe extern "C" fn StandbyLockTimeoutHandler() { unimplemented!() }

// utils/timestamp.h - timestamp helpers.
unsafe fn GetCurrentTimestamp() -> TimestampTz {
    crate::utils::adt::timestamp::GetCurrentTimestamp()
}
unsafe fn TimestampDifference(
    start: TimestampTz,
    stop: TimestampTz,
    secs: *mut c_long,
    microsecs: *mut c_int,
) { crate::utils::adt::timestamp::TimestampDifference(start as _, stop as _, secs as _, microsecs as _) }
#[inline]
unsafe fn TimestampTzPlusMilliseconds(tz: TimestampTz, ms: int64) -> TimestampTz {
    // From timestamp.h macro: (tz) + ((ms) * (int64) 1000)
    tz + ms * 1000
}
