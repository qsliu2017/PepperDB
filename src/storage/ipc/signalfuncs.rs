//! storage/ipc/signalfuncs.c - Functions for signaling backends

use crate::prelude::*;

use std::ffi::c_int;

use crate::miscadmin::{GetUserId, CHECK_FOR_INTERRUPTS};
use crate::utils::init::globals::{MyLatch, PostmasterPid};
use crate::utils::rel::ProcNumber;
use crate::utils::misc::superuser::{superuser, superuser_arg};
use crate::utils::fmgr::FunctionCallInfo;
use crate::catalog::pg_known_oids::{ROLE_PG_SIGNAL_AUTOVACUUM_WORKER, ROLE_PG_SIGNAL_BACKEND};
use crate::{PG_GETARG_INT32, PG_GETARG_INT64, PG_RETURN_BOOL};

// storage/latch.h: Latch is an opaque (by-pointer) type here.
use crate::utils::init::globals::Latch;

// miscadmin.h: BackendType is an int enum; B_AUTOVAC_WORKER is one variant.
type BackendType = c_int;
const B_AUTOVAC_WORKER: BackendType = 4;

// <signal.h> signal numbers (POSIX values on PostgreSQL's target platforms).
const SIGINT: c_int = 2;
const SIGTERM: c_int = 15;
const SIGHUP: c_int = 1;

// <errno.h> ESRCH: "No such process".
const ESRCH: c_int = 3;

// storage/latch.h wait-event flags (real bit values from the C header).
const WL_LATCH_SET: c_int = 1 << 0;
const WL_TIMEOUT: c_int = 1 << 4;
const WL_EXIT_ON_PM_DEATH: c_int = 1 << 5;

// pgstat.h wait-event id (enum WaitEventActivity / WaitEventTimeout).
// TODO: replace with the real wait-event enum value once pgstat is ported.
const WAIT_EVENT_BACKEND_TERMINATION: uint32 = 0x0500_0001;

// storage/pmsignal.h: PMSIGNAL_ROTATE_LOGFILE reason code.
// TODO: dedup with storage/pmsignal.rs once it lands.
const PMSIGNAL_ROTATE_LOGFILE: c_int = 5;

unsafe extern "C" {
    // <signal.h>: kill(2). Returns 0 on success, -1 on error.
    fn kill(pid: c_int, sig: c_int) -> c_int;
    // macOS errno location.
    fn __error() -> *mut c_int;
}

#[inline]
unsafe fn errno_get() -> c_int {
    *__error()
}

// ---------------------------------------------------------------------------
// Local stubs for dependencies not yet ported. // TODO: deps not ported
// ---------------------------------------------------------------------------

// storage/proc.h - PGPROC. Only proc->roleId is touched here; declare a
// minimal layout-compatible-enough stub exposing roleId at a known position.
// TODO: dedup with storage/proc.rs once the full PGPROC lands.
#[repr(C)]
#[allow(non_snake_case)]
pub struct PGPROC {
    pub roleId: Oid,
}

// storage/procarray.h - BackendPidGetProc: PGPROC for a given backend pid,
// or NULL if no such backend exists.
unsafe fn BackendPidGetProc(_pid: c_int) -> *mut PGPROC {
    unimplemented!()
}

// storage/proc.h - GetNumberFromPGProc(proc): the ProcNumber of a PGPROC.
unsafe fn GetNumberFromPGProc(_proc: *mut PGPROC) -> ProcNumber {
    unimplemented!()
}

// pgstat.h - pgstat_get_backend_type_by_proc_number.
unsafe fn pgstat_get_backend_type_by_proc_number(_proc_number: ProcNumber) -> BackendType {
    unimplemented!()
}

// utils/acl.h - has_privs_of_role(member, role).
unsafe fn has_privs_of_role(_member: Oid, _role: Oid) -> bool {
    unimplemented!()
}

// storage/pmsignal.h - SendPostmasterSignal.
unsafe fn SendPostmasterSignal(_reason: c_int) {
    unimplemented!()
}

// storage/latch.h - WaitLatch: block until the latch is set, the timeout
// elapses, or postmaster death; returns the set of triggered WL_* conditions.
unsafe fn WaitLatch(
    _latch: *mut Latch,
    _wakeEvents: c_int,
    _timeout_ms: c_long,
    _wait_event_info: uint32,
) -> c_int {
    unimplemented!()
}

// storage/latch.h - ResetLatch: clears a latch's set flag.
unsafe fn ResetLatch(_latch: *mut Latch) {}

// postmaster/syslogger.h - Logging_collector GUC.
// TODO: replace with the real GUC variable once syslogger is ported.
static mut Logging_collector: bool = false;

/*
 * Send a signal to another backend.
 *
 * The signal is delivered if the user is either a superuser or the same
 * role as the backend being signaled. For "dangerous" signals, an explicit
 * check for superuser needs to be done prior to calling this function.
 *
 * Returns 0 on success, 1 on general failure, 2 on normal permission error,
 * 3 if the caller needs to be a superuser, and 4 if the caller needs to have
 * privileges of pg_signal_autovacuum_worker.
 *
 * In the event of a general failure (return code 1), a warning message will
 * be emitted. For permission errors, doing that is the responsibility of
 * the caller.
 */
const SIGNAL_BACKEND_SUCCESS: c_int = 0;
const SIGNAL_BACKEND_ERROR: c_int = 1;
const SIGNAL_BACKEND_NOPERMISSION: c_int = 2;
const SIGNAL_BACKEND_NOSUPERUSER: c_int = 3;
const SIGNAL_BACKEND_NOAUTOVAC: c_int = 4;

unsafe fn pg_signal_backend(pid: c_int, sig: c_int) -> c_int {
    let proc: *mut PGPROC = BackendPidGetProc(pid);

    /*
     * BackendPidGetProc returns NULL if the pid isn't valid; but by the time
     * we reach kill(), a process for which we get a valid proc here might
     * have terminated on its own.  There's no way to acquire a lock on an
     * arbitrary process to prevent that. But since so far all the callers of
     * this mechanism involve some request for ending the process anyway, that
     * it might end on its own first is not a problem.
     *
     * Note that proc will also be NULL if the pid refers to an auxiliary
     * process or the postmaster (neither of which can be signaled via
     * pg_signal_backend()).
     */
    if proc.is_null() {
        /*
         * This is just a warning so a loop-through-resultset will not abort
         * if one backend terminated on its own during the run.
         */
        elog!(WARNING, "PID {} is not a PostgreSQL backend process", pid);

        return SIGNAL_BACKEND_ERROR;
    }

    /*
     * Only allow superusers to signal superuser-owned backends.  Any process
     * not advertising a role might have the importance of a superuser-owned
     * backend, so treat it that way.  As an exception, we allow roles with
     * privileges of pg_signal_autovacuum_worker to signal autovacuum workers
     * (which do not advertise a role).
     *
     * Otherwise, users can signal backends for roles they have privileges of.
     */
    if !OidIsValid((*proc).roleId) || superuser_arg((*proc).roleId) {
        let procNumber: ProcNumber = GetNumberFromPGProc(proc);
        let backendType: BackendType = pgstat_get_backend_type_by_proc_number(procNumber);

        if backendType == B_AUTOVAC_WORKER {
            if !has_privs_of_role(GetUserId(), ROLE_PG_SIGNAL_AUTOVACUUM_WORKER) {
                return SIGNAL_BACKEND_NOAUTOVAC;
            }
        } else if !superuser() {
            return SIGNAL_BACKEND_NOSUPERUSER;
        }
    } else if !has_privs_of_role(GetUserId(), (*proc).roleId)
        && !has_privs_of_role(GetUserId(), ROLE_PG_SIGNAL_BACKEND)
    {
        return SIGNAL_BACKEND_NOPERMISSION;
    }

    /*
     * Can the process we just validated above end, followed by the pid being
     * recycled for a new process, before reaching here?  Then we'd be trying
     * to kill the wrong thing.  Seems near impossible when sequential pid
     * assignment and wraparound is used.  Perhaps it could happen on a system
     * where pid re-use is randomized.  That race condition possibility seems
     * too unlikely to worry about.
     */

    /* If we have setsid(), signal the backend's whole process group */
    if kill(-pid, sig) != 0 {
        /* Again, just a warning to allow loops */
        elog!(WARNING, "could not send signal to process {}", pid);
        return SIGNAL_BACKEND_ERROR;
    }
    SIGNAL_BACKEND_SUCCESS
}

/*
 * Signal to cancel a backend process.  This is allowed if you are a member of
 * the role whose process is being canceled.
 *
 * Note that only superusers can signal superuser-owned processes.
 */
pub unsafe fn pg_cancel_backend(fcinfo: FunctionCallInfo) -> Datum {
    let r: c_int = pg_signal_backend(PG_GETARG_INT32!(fcinfo, 0), SIGINT);

    if r == SIGNAL_BACKEND_NOSUPERUSER {
        ereport!(ERROR, "permission denied to cancel query");
    }

    if r == SIGNAL_BACKEND_NOAUTOVAC {
        ereport!(ERROR, "permission denied to cancel query");
    }

    if r == SIGNAL_BACKEND_NOPERMISSION {
        ereport!(ERROR, "permission denied to cancel query");
    }

    PG_RETURN_BOOL!(r == SIGNAL_BACKEND_SUCCESS)
}

/*
 * Wait until there is no backend process with the given PID and return true.
 * On timeout, a warning is emitted and false is returned.
 */
unsafe fn pg_wait_until_termination(pid: c_int, timeout: int64) -> bool {
    /*
     * Wait in steps of waittime milliseconds until this function exits or
     * timeout.
     */
    let mut waittime: int64 = 100;

    /*
     * Initially remaining time is the entire timeout specified by the user.
     */
    let mut remainingtime: int64 = timeout;

    /*
     * Check existence of the backend. If the backend still exists, then wait
     * for waittime milliseconds, again check for the existence. Repeat this
     * until timeout or an error occurs or a pending interrupt such as query
     * cancel gets processed.
     */
    loop {
        if remainingtime < waittime {
            waittime = remainingtime;
        }

        if kill(pid, 0) == -1 {
            if errno_get() == ESRCH {
                return true;
            } else {
                elog!(
                    ERROR,
                    "could not check the existence of the backend with PID {}",
                    pid
                );
            }
        }

        /* Process interrupts, if any, before waiting */
        CHECK_FOR_INTERRUPTS();

        WaitLatch(
            MyLatch,
            WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
            waittime,
            WAIT_EVENT_BACKEND_TERMINATION,
        );

        ResetLatch(MyLatch);

        remainingtime -= waittime;

        if remainingtime <= 0 {
            break;
        }
    }

    elog!(
        WARNING,
        "backend with PID {} did not terminate within {} milliseconds",
        pid,
        timeout
    );

    false
}

/*
 * Send a signal to terminate a backend process. This is allowed if you are a
 * member of the role whose process is being terminated. If the timeout input
 * argument is 0, then this function just signals the backend and returns
 * true.  If timeout is nonzero, then it waits until no process has the given
 * PID; if the process ends within the timeout, true is returned, and if the
 * timeout is exceeded, a warning is emitted and false is returned.
 *
 * Note that only superusers can signal superuser-owned processes.
 */
pub unsafe fn pg_terminate_backend(fcinfo: FunctionCallInfo) -> Datum {
    let pid: c_int = PG_GETARG_INT32!(fcinfo, 0);
    let timeout: int64 = PG_GETARG_INT64!(fcinfo, 1);

    if timeout < 0 {
        ereport!(ERROR, "\"timeout\" must not be negative");
    }

    let r: c_int = pg_signal_backend(pid, SIGTERM);

    if r == SIGNAL_BACKEND_NOSUPERUSER {
        ereport!(ERROR, "permission denied to terminate process");
    }

    if r == SIGNAL_BACKEND_NOAUTOVAC {
        ereport!(ERROR, "permission denied to terminate process");
    }

    if r == SIGNAL_BACKEND_NOPERMISSION {
        ereport!(ERROR, "permission denied to terminate process");
    }

    /* Wait only on success and if actually requested */
    if r == SIGNAL_BACKEND_SUCCESS && timeout > 0 {
        PG_RETURN_BOOL!(pg_wait_until_termination(pid, timeout))
    } else {
        PG_RETURN_BOOL!(r == SIGNAL_BACKEND_SUCCESS)
    }
}

/*
 * Signal to reload the database configuration
 *
 * Permission checking for this function is managed through the normal
 * GRANT system.
 */
pub unsafe fn pg_reload_conf(_fcinfo: FunctionCallInfo) -> Datum {
    if kill(PostmasterPid, SIGHUP) != 0 {
        elog!(WARNING, "failed to send signal to postmaster");
        PG_RETURN_BOOL!(false);
    }

    PG_RETURN_BOOL!(true)
}

/*
 * Rotate log file
 *
 * Permission checking for this function is managed through the normal
 * GRANT system.
 */
pub unsafe fn pg_rotate_logfile(_fcinfo: FunctionCallInfo) -> Datum {
    if !Logging_collector {
        elog!(
            WARNING,
            "rotation not possible because log collection not active"
        );
        PG_RETURN_BOOL!(false);
    }

    SendPostmasterSignal(PMSIGNAL_ROTATE_LOGFILE);
    PG_RETURN_BOOL!(true)
}
