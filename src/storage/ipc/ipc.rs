//! storage/ipc/ipc.c - POSTGRES inter-process communication definitions.
//!
//! This file is misnamed, as it no longer has much of anything directly
//! to do with IPC.  The functionality here is concerned with managing
//! exit-time cleanup for either a postmaster or a backend.

use crate::prelude::*;
type sig_atomic_t = std::ffi::c_int;
use crate::elog;

use std::process;

// pg_on_exit_callback (storage/ipc.h):
//   typedef void (*pg_on_exit_callback)(int code, Datum arg);
pub type pg_on_exit_callback = unsafe extern "C" fn(code: c_int, arg: Datum);

// ----------------------------------------------------------------
// Imported globals.
//
// These are defined in globals.c (utils/init/globals.rs) and declared volatile
// in miscadmin.h because signal handlers touch them.  We re-declare them here
// via an extern block, matching how miscadmin.rs exposes them.
// ----------------------------------------------------------------
extern "C" {
    static mut InterruptPending: sig_atomic_t;
    static mut ProcDiePending: sig_atomic_t;
    static mut QueryCancelPending: sig_atomic_t;
    static mut InterruptHoldoffCount: uint32;
    static mut CritSectionCount: uint32;
    static mut MyProcPid: c_int;
    // tcop/postgres.c
    static mut debug_query_string: *const c_char;
}

// ----------------------------------------------------------------
// Stubs for not-yet-ported callees.
// ----------------------------------------------------------------

// error_context_stack / PG_exception_stack (elog.c). TODO: import once elog.c
// is ported. The real type is *mut ErrorContextCallback.
static mut error_context_stack: *mut c_void = null_mut();

// LWLockReleaseAll (storage/lmgr/lwlock.c).
unsafe fn LWLockReleaseAll() {
    unimplemented!()
}

// dsm_backend_shutdown / reset_on_dsm_detach (storage/ipc/dsm.c).
unsafe fn dsm_backend_shutdown() {
    unimplemented!()
}
unsafe fn reset_on_dsm_detach() {
    unimplemented!()
}

// AmAutoVacuumWorkerProcess (miscadmin.rs) - referenced only under
// PROFILE_PID_DIR, which we do not compile in.

// errcode argument: ERRCODE_PROGRAM_LIMIT_EXCEEDED (utils/errcodes.h).
// TODO: import from the generated errcodes table.
const ERRCODE_PROGRAM_LIMIT_EXCEEDED: c_int = 0;

/// This flag is set during proc_exit() to change ereport()'s behavior,
/// so that an ereport() from an on_proc_exit routine cannot get us out
/// of the exit procedure.  We do NOT want to go back to the idle loop...
#[no_mangle]
pub static mut proc_exit_inprogress: bool = false;

/// Set when shmem_exit() is in progress.
#[no_mangle]
pub static mut shmem_exit_inprogress: bool = false;

/// This flag tracks whether we've called atexit() in the current process
/// (or in the parent postmaster).
static mut atexit_callback_setup: bool = false;

// ----------------------------------------------------------------
//						exit() handling stuff
//
// These functions are in generally the same spirit as atexit(),
// but provide some additional features we need --- in particular,
// we want to register callbacks to invoke when we are disconnecting
// from a broken shared-memory context but not exiting the postmaster.
//
// Callback functions can take zero, one, or two args: the first passed
// arg is the integer exitcode, the second is the Datum supplied when
// the callback was registered.
// ----------------------------------------------------------------

const MAX_ON_EXITS: usize = 20;

#[derive(Clone, Copy)]
struct ONEXIT {
    function: Option<pg_on_exit_callback>,
    arg: Datum,
}

static mut on_proc_exit_list: [ONEXIT; MAX_ON_EXITS] = [ONEXIT {
    function: None,
    arg: 0,
}; MAX_ON_EXITS];
static mut on_shmem_exit_list: [ONEXIT; MAX_ON_EXITS] = [ONEXIT {
    function: None,
    arg: 0,
}; MAX_ON_EXITS];
static mut before_shmem_exit_list: [ONEXIT; MAX_ON_EXITS] = [ONEXIT {
    function: None,
    arg: 0,
}; MAX_ON_EXITS];

static mut on_proc_exit_index: c_int = 0;
static mut on_shmem_exit_index: c_int = 0;
static mut before_shmem_exit_index: c_int = 0;

// ----------------------------------------------------------------
//		proc_exit
//
//		this function calls all the callbacks registered
//		for it (to free resources) and then calls exit.
//
//		This should be the only function to call exit().
//		-cim 2/6/90
//
//		Unfortunately, we can't really guarantee that add-on code
//		obeys the rule of not calling exit() directly.  So, while
//		this is the preferred way out of the system, we also register
//		an atexit callback that will make sure cleanup happens.
// ----------------------------------------------------------------
pub unsafe fn proc_exit(code: c_int) -> ! {
    /* not safe if forked by system(), etc. */
    if MyProcPid != process::id() as c_int {
        elog!(PANIC, "proc_exit() called in child process");
    }

    /* Clean up everything that must be cleaned up */
    proc_exit_prepare(code);

    // PROFILE_PID_DIR block omitted (not compiled in).

    elog!(DEBUG3, "exit({})", code);

    process::exit(code);
}

/// Code shared between proc_exit and the atexit handler.  Note that in
/// normal exit through proc_exit, this will actually be called twice ...
/// but the second call will have nothing to do.
unsafe fn proc_exit_prepare(code: c_int) {
    /*
     * Once we set this flag, we are committed to exit.  Any ereport() will
     * NOT send control back to the main loop, but right back here.
     */
    proc_exit_inprogress = true;

    /*
     * Forget any pending cancel or die requests; we're doing our best to
     * close up shop already.  Note that the signal handlers will not set
     * these flags again, now that proc_exit_inprogress is set.
     */
    InterruptPending = false as sig_atomic_t;
    ProcDiePending = false as sig_atomic_t;
    QueryCancelPending = false as sig_atomic_t;
    InterruptHoldoffCount = 1;
    CritSectionCount = 0;

    /*
     * Also clear the error context stack, to prevent error callbacks from
     * being invoked by any elog/ereport calls made during proc_exit. Whatever
     * context they might want to offer is probably not relevant, and in any
     * case they are likely to fail outright after we've done things like
     * aborting any open transaction.  (In normal exit scenarios the context
     * stack should be empty anyway, but it might not be in the case of
     * elog(FATAL) for example.)
     */
    error_context_stack = null_mut();
    /* For the same reason, reset debug_query_string before it's clobbered */
    debug_query_string = null();

    /* do our shared memory exits first */
    shmem_exit(code);

    elog!(
        DEBUG3,
        "proc_exit({}): {} callbacks to make",
        code,
        on_proc_exit_index
    );

    /*
     * call all the registered callbacks.
     *
     * Note that since we decrement on_proc_exit_index each time, if a
     * callback calls ereport(ERROR) or ereport(FATAL) then it won't be
     * invoked again when control comes back here (nor will the
     * previously-completed callbacks).  So, an infinite loop should not be
     * possible.
     */
    on_proc_exit_index -= 1;
    while on_proc_exit_index >= 0 {
        let entry = on_proc_exit_list[on_proc_exit_index as usize];
        (entry.function.unwrap())(code, entry.arg);
        on_proc_exit_index -= 1;
    }

    on_proc_exit_index = 0;
}

/// Run all of the on_shmem_exit routines --- but don't actually exit.
/// This is used by the postmaster to re-initialize shared memory and
/// semaphores after a backend dies horribly.  As with proc_exit(), we
/// remove each callback from the list before calling it, to avoid
/// infinite loop in case of error.
pub unsafe fn shmem_exit(code: c_int) {
    shmem_exit_inprogress = true;

    /*
     * Release any LWLocks we might be holding before callbacks run. This
     * prevents accessing locks in detached DSM segments and allows callbacks
     * to acquire new locks.
     */
    LWLockReleaseAll();

    /*
     * Call before_shmem_exit callbacks.
     *
     * These should be things that need most of the system to still be up and
     * working, such as cleanup of temp relations, which requires catalog
     * access.
     */
    elog!(
        DEBUG3,
        "shmem_exit({}): {} before_shmem_exit callbacks to make",
        code,
        before_shmem_exit_index
    );
    before_shmem_exit_index -= 1;
    while before_shmem_exit_index >= 0 {
        let entry = before_shmem_exit_list[before_shmem_exit_index as usize];
        (entry.function.unwrap())(code, entry.arg);
        before_shmem_exit_index -= 1;
    }
    before_shmem_exit_index = 0;

    /*
     * Call dynamic shared memory callbacks.
     *
     * These serve the same purpose as late callbacks, but for dynamic shared
     * memory segments rather than the main shared memory segment.
     * dsm_backend_shutdown() has the same kind of progressive logic we use
     * for the main shared memory segment; namely, it unregisters each
     * callback before invoking it, so that we don't get stuck in an infinite
     * loop if one of those callbacks itself throws an ERROR or FATAL.
     *
     * Note that explicitly calling this function here is quite different from
     * registering it as an on_shmem_exit callback for precisely this reason:
     * if one dynamic shared memory callback errors out, the remaining
     * callbacks will still be invoked.  Thus, hard-coding this call puts it
     * equal footing with callbacks for the main shared memory segment.
     */
    dsm_backend_shutdown();

    /*
     * Call on_shmem_exit callbacks.
     *
     * These are generally releasing low-level shared memory resources.  In
     * some cases, this is a backstop against the possibility that the early
     * callbacks might themselves fail, leading to re-entry to this routine;
     * in other cases, it's cleanup that only happens at process exit.
     */
    elog!(
        DEBUG3,
        "shmem_exit({}): {} on_shmem_exit callbacks to make",
        code,
        on_shmem_exit_index
    );
    on_shmem_exit_index -= 1;
    while on_shmem_exit_index >= 0 {
        let entry = on_shmem_exit_list[on_shmem_exit_index as usize];
        (entry.function.unwrap())(code, entry.arg);
        on_shmem_exit_index -= 1;
    }
    on_shmem_exit_index = 0;

    shmem_exit_inprogress = false;
}

/// atexit_callback
///
///		Backstop to ensure that direct calls of exit() don't mess us up.
///
/// Somebody who was being really uncooperative could call _exit(),
/// but for that case we have a "dead man switch" that will make the
/// postmaster treat it as a crash --- see pmsignal.c.
extern "C" fn atexit_callback() {
    /* Clean up everything that must be cleaned up */
    /* ... too bad we don't know the real exit code ... */
    unsafe {
        proc_exit_prepare(-1);
    }
}

/// on_proc_exit
///
///		this function adds a callback function to the list of
///		functions invoked by proc_exit().   -cim 2/6/90
pub unsafe fn on_proc_exit(function: pg_on_exit_callback, arg: Datum) {
    if on_proc_exit_index >= MAX_ON_EXITS as c_int {
        ereport!(FATAL, "out of on_proc_exit slots");
        let _ = errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED);
    }

    on_proc_exit_list[on_proc_exit_index as usize].function = Some(function);
    on_proc_exit_list[on_proc_exit_index as usize].arg = arg;

    on_proc_exit_index += 1;

    if !atexit_callback_setup {
        libc_atexit(atexit_callback);
        atexit_callback_setup = true;
    }
}

/// before_shmem_exit
///
///		Register early callback to perform user-level cleanup,
///		e.g. transaction abort, before we begin shutting down
///		low-level subsystems.
pub unsafe fn before_shmem_exit(function: pg_on_exit_callback, arg: Datum) {
    if before_shmem_exit_index >= MAX_ON_EXITS as c_int {
        ereport!(FATAL, "out of before_shmem_exit slots");
        let _ = errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED);
    }

    before_shmem_exit_list[before_shmem_exit_index as usize].function = Some(function);
    before_shmem_exit_list[before_shmem_exit_index as usize].arg = arg;

    before_shmem_exit_index += 1;

    if !atexit_callback_setup {
        libc_atexit(atexit_callback);
        atexit_callback_setup = true;
    }
}

/// on_shmem_exit
///
///		Register ordinary callback to perform low-level shutdown
///		(e.g. releasing our PGPROC); run after before_shmem_exit
///		callbacks and before on_proc_exit callbacks.
pub unsafe fn on_shmem_exit(function: pg_on_exit_callback, arg: Datum) {
    if on_shmem_exit_index >= MAX_ON_EXITS as c_int {
        ereport!(FATAL, "out of on_shmem_exit slots");
        let _ = errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED);
    }

    on_shmem_exit_list[on_shmem_exit_index as usize].function = Some(function);
    on_shmem_exit_list[on_shmem_exit_index as usize].arg = arg;

    on_shmem_exit_index += 1;

    if !atexit_callback_setup {
        libc_atexit(atexit_callback);
        atexit_callback_setup = true;
    }
}

/// cancel_before_shmem_exit
///
///		this function removes a previously-registered before_shmem_exit
///		callback.  We only look at the latest entry for removal, as we
/// 		expect callers to add and remove temporary before_shmem_exit
/// 		callbacks in strict LIFO order.
pub unsafe fn cancel_before_shmem_exit(function: pg_on_exit_callback, arg: Datum) {
    if before_shmem_exit_index > 0
        && before_shmem_exit_list[(before_shmem_exit_index - 1) as usize]
            .function
            .map(|f| f as usize)
            == Some(function as usize)
        && before_shmem_exit_list[(before_shmem_exit_index - 1) as usize].arg == arg
    {
        before_shmem_exit_index -= 1;
    } else {
        elog!(
            ERROR,
            "before_shmem_exit callback ({:p},0x{:x}) is not the latest entry",
            function as *const c_void,
            arg
        );
    }
}

/// on_exit_reset
///
///		this function clears all on_proc_exit() and on_shmem_exit()
///		registered functions.  This is used just after forking a backend,
///		so that the backend doesn't believe it should call the postmaster's
///		on-exit routines when it exits...
pub unsafe fn on_exit_reset() {
    before_shmem_exit_index = 0;
    on_shmem_exit_index = 0;
    on_proc_exit_index = 0;
    reset_on_dsm_detach();
}

/// check_on_shmem_exit_lists_are_empty
///
///		Debugging check that no shmem cleanup handlers have been registered
///		prematurely in the current process.
pub unsafe fn check_on_shmem_exit_lists_are_empty() {
    if before_shmem_exit_index != 0 {
        elog!(FATAL, "before_shmem_exit has been called prematurely");
    }
    if on_shmem_exit_index != 0 {
        elog!(FATAL, "on_shmem_exit has been called prematurely");
    }
    /* Checking DSM detach state seems unnecessary given the above */
}

// libc atexit(3). Registers a C-ABI callback run at process exit.
extern "C" {
    #[link_name = "atexit"]
    fn libc_atexit(cb: extern "C" fn()) -> c_int;
}
