//! backend/port/win32/signal.c - Microsoft Windows Win32 Signal Emulation
//! Functions.
//!
//! 1:1 translation. Windows-only in upstream; on the (non-Windows) PepperDB
//! target none of these symbols are ever used, but they are translated
//! faithfully for completeness. Win32 / NT system primitives that are not
//! available off-Windows (CRITICAL_SECTION, CreateNamedPipe, SleepEx, ...) are
//! stubbed locally with TODO(pg-port). The signal-emulation globals, the
//! PG_SIGNAL_COUNT / sigmask() / UNBLOCKED_SIGNAL_QUEUE() helpers and the
//! HANDLE / DWORD / BOOL / pid_t types live in port/win32_port.rs and are
//! imported from there.

#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use std::ffi::{c_char, c_int, c_long, c_void};

use crate::port::port_api::pqsigfunc;
use crate::port::win32_port::{
    pg_signal_mask, pg_signal_queue, pgwin32_initial_signal_pipe, pgwin32_signal_event, sigmask,
    BOOL, DWORD, HANDLE, INVALID_HANDLE_VALUE, PG_SIGNAL_COUNT,
};
use crate::utils::elog::FATAL;
use crate::utils::elog::ERROR;
use crate::{ereport, errmsg};

// ---------------------------------------------------------------------------
// Win32 / NT primitives referenced by signal.c that have no off-Windows
// equivalent. Stubbed locally with TODO(pg-port).
// ---------------------------------------------------------------------------
type LPVOID = *mut c_void;
type BYTE = u8;

/// CRITICAL_SECTION is an opaque NT structure; modelled as an opaque cell.
struct CRITICAL_SECTION {
    _opaque: [u8; 0],
}

const TRUE: BOOL = 1;
const FALSE: BOOL = 0;

const WAIT_OBJECT_0: DWORD = 0x00000000;

const PIPE_ACCESS_DUPLEX: DWORD = 0x00000003;
const PIPE_TYPE_MESSAGE: DWORD = 0x00000004;
const PIPE_READMODE_MESSAGE: DWORD = 0x00000002;
const PIPE_WAIT: DWORD = 0x00000000;
const PIPE_UNLIMITED_INSTANCES: DWORD = 255;

const ERROR_PIPE_CONNECTED: DWORD = 535;

const CTRL_C_EVENT: DWORD = 0;
const CTRL_BREAK_EVENT: DWORD = 1;
const CTRL_CLOSE_EVENT: DWORD = 2;
const CTRL_SHUTDOWN_EVENT: DWORD = 6;

const SIGINT: c_int = 2;

const SA_NODEFER: c_int = 0x40000000;

const SIG_BLOCK: c_int = 0;
const SIG_UNBLOCK: c_int = 1;
const SIG_SETMASK: c_int = 2;

const EINTR: c_int = 4;
const EINVAL: c_int = 22;

// pqsignal.h sentinel handler values (cast from small integers, as in C).
const SIG_DFL: *mut c_void = 0 as *mut c_void;
const SIG_IGN: *mut c_void = 1 as *mut c_void;
const SIG_ERR: *mut c_void = (-1isize) as *mut c_void;

/// TODO(pg-port): libpq/pqsignal.h sigset_t (Win32 build models it as an int)
type sigset_t = c_int;

/// TODO(pg-port): libpq/pqsignal.h struct sigaction (Win32 build).
/// sa_handler is stored as a raw pointer so it can be compared against the
/// SIG_DFL / SIG_IGN / SIG_ERR sentinels, matching the C code.
#[derive(Clone, Copy)]
struct sigaction {
    sa_handler: *mut c_void,
    sa_mask: sigset_t,
    sa_flags: c_int,
}

// errno access (platform errno location), mirroring win32_port.rs.
#[cfg(target_os = "macos")]
extern "C" {
    #[link_name = "__error"]
    fn errno_location() -> *mut c_int;
}
#[cfg(not(target_os = "macos"))]
extern "C" {
    #[link_name = "__errno_location"]
    fn errno_location() -> *mut c_int;
}

#[inline]
unsafe fn set_errno(e: c_int) {
    *errno_location() = e;
}

unsafe fn SleepEx(_dwMilliseconds: DWORD, _bAlertable: BOOL) -> DWORD {
    todo!("TODO(pg-port): SleepEx")
}

unsafe fn WaitForSingleObject(_hHandle: HANDLE, _dwMilliseconds: DWORD) -> DWORD {
    todo!("TODO(pg-port): WaitForSingleObject")
}

unsafe fn InitializeCriticalSection(_lpCriticalSection: *mut CRITICAL_SECTION) {
    todo!("TODO(pg-port): InitializeCriticalSection")
}

unsafe fn EnterCriticalSection(_lpCriticalSection: *mut CRITICAL_SECTION) {
    todo!("TODO(pg-port): EnterCriticalSection")
}

unsafe fn LeaveCriticalSection(_lpCriticalSection: *mut CRITICAL_SECTION) {
    todo!("TODO(pg-port): LeaveCriticalSection")
}

unsafe fn CreateEvent(
    _lpEventAttributes: *mut c_void,
    _bManualReset: BOOL,
    _bInitialState: BOOL,
    _lpName: *const c_char,
) -> HANDLE {
    todo!("TODO(pg-port): CreateEvent")
}

unsafe fn SetEvent(_hEvent: HANDLE) -> BOOL {
    todo!("TODO(pg-port): SetEvent")
}

unsafe fn ResetEvent(_hEvent: HANDLE) -> BOOL {
    todo!("TODO(pg-port): ResetEvent")
}

unsafe fn CreateThread(
    _lpThreadAttributes: *mut c_void,
    _dwStackSize: usize,
    _lpStartAddress: unsafe extern "system" fn(LPVOID) -> DWORD,
    _lpParameter: LPVOID,
    _dwCreationFlags: DWORD,
    _lpThreadId: *mut DWORD,
) -> HANDLE {
    todo!("TODO(pg-port): CreateThread")
}

unsafe fn SetConsoleCtrlHandler(
    _HandlerRoutine: unsafe extern "system" fn(DWORD) -> BOOL,
    _Add: BOOL,
) -> BOOL {
    todo!("TODO(pg-port): SetConsoleCtrlHandler")
}

unsafe fn CreateNamedPipe(
    _lpName: *const c_char,
    _dwOpenMode: DWORD,
    _dwPipeMode: DWORD,
    _nMaxInstances: DWORD,
    _nOutBufferSize: DWORD,
    _nInBufferSize: DWORD,
    _nDefaultTimeOut: DWORD,
    _lpSecurityAttributes: *mut c_void,
) -> HANDLE {
    todo!("TODO(pg-port): CreateNamedPipe")
}

unsafe fn ConnectNamedPipe(_hNamedPipe: HANDLE, _lpOverlapped: *mut c_void) -> BOOL {
    todo!("TODO(pg-port): ConnectNamedPipe")
}

unsafe fn DisconnectNamedPipe(_hNamedPipe: HANDLE) -> BOOL {
    todo!("TODO(pg-port): DisconnectNamedPipe")
}

unsafe fn ReadFile(
    _hFile: HANDLE,
    _lpBuffer: *mut c_void,
    _nNumberOfBytesToRead: DWORD,
    _lpNumberOfBytesRead: *mut DWORD,
    _lpOverlapped: *mut c_void,
) -> BOOL {
    todo!("TODO(pg-port): ReadFile")
}

unsafe fn WriteFile(
    _hFile: HANDLE,
    _lpBuffer: *const c_void,
    _nNumberOfBytesToWrite: DWORD,
    _lpNumberOfBytesWritten: *mut DWORD,
    _lpOverlapped: *mut c_void,
) -> BOOL {
    todo!("TODO(pg-port): WriteFile")
}

unsafe fn FlushFileBuffers(_hFile: HANDLE) -> BOOL {
    todo!("TODO(pg-port): FlushFileBuffers")
}

unsafe fn CloseHandle(_hObject: HANDLE) -> BOOL {
    todo!("TODO(pg-port): CloseHandle")
}

unsafe fn GetLastError() -> DWORD {
    todo!("TODO(pg-port): GetLastError")
}

unsafe fn GetCurrentProcessId() -> DWORD {
    todo!("TODO(pg-port): GetCurrentProcessId")
}

/// TODO(pg-port): port/snprintf.c (libc snprintf used here)
extern "C" {
    fn snprintf(s: *mut c_char, n: usize, fmt: *const c_char, ...) -> c_int;
}

/// TODO(pg-port): utils/error/elog.c write_stderr()
unsafe fn write_stderr(_fmt: *const c_char, _err: DWORD) {
    todo!("TODO(pg-port): write_stderr")
}

/// TODO(pg-port): libpq/pqsignal.h sigprocmask (Win32 maps to pqsigprocmask)
unsafe fn sigprocmask(how: c_int, set: *const sigset_t, oset: *mut sigset_t) -> c_int {
    pqsigprocmask(how, set, oset)
}

/*
 * These are exported for use by the UNBLOCKED_SIGNAL_QUEUE() macro.
 * pg_signal_queue must be volatile since it is changed by the signal
 * handling thread and inspected without any lock by the main thread.
 * pg_signal_mask is only changed by main thread so shouldn't need it.
 *
 * (pg_signal_queue, pg_signal_mask, pgwin32_signal_event and
 * pgwin32_initial_signal_pipe live in port/win32_port.rs and are imported.)
 */

/*
 * pg_signal_crit_sec is used to protect only pg_signal_queue. That is the only
 * variable that can be accessed from the signal sending threads!
 */
static mut pg_signal_crit_sec: CRITICAL_SECTION = CRITICAL_SECTION { _opaque: [] };

/* Note that array elements 0 are unused since they correspond to signal 0 */
static mut pg_signal_array: [sigaction; PG_SIGNAL_COUNT as usize] = [sigaction {
    sa_handler: SIG_DFL,
    sa_mask: 0,
    sa_flags: 0,
}; PG_SIGNAL_COUNT as usize];
static mut pg_signal_defaults: [*mut c_void; PG_SIGNAL_COUNT as usize] =
    [SIG_IGN; PG_SIGNAL_COUNT as usize];

#[inline]
unsafe fn UNBLOCKED_SIGNAL_QUEUE() -> c_int {
    pg_signal_queue & !pg_signal_mask
}

/*
 * pg_usleep --- delay the specified number of microseconds, but
 * stop waiting if a signal arrives.
 *
 * This replaces the non-signal-aware version provided by src/port/pgsleep.c.
 */
pub unsafe fn pg_usleep(microsec: c_long) {
    if pgwin32_signal_event.is_null() {
        /*
         * If we're reached by pgwin32_open_handle() early in startup before
         * the signal event is set up, just fall back to a regular
         * non-interruptible sleep.
         */
        SleepEx(
            (if microsec < 500 { 1 } else { (microsec + 500) / 1000 }) as DWORD,
            FALSE,
        );
        return;
    }

    if WaitForSingleObject(
        pgwin32_signal_event,
        (if microsec < 500 { 1 } else { (microsec + 500) / 1000 }) as DWORD,
    ) == WAIT_OBJECT_0
    {
        pgwin32_dispatch_queued_signals();
        set_errno(EINTR);
        return;
    }
}

/* Initialization */
pub unsafe fn pgwin32_signal_initialize() {
    let mut i: c_int;
    let signal_thread_handle: HANDLE;

    InitializeCriticalSection(&raw mut pg_signal_crit_sec);

    i = 0;
    while i < PG_SIGNAL_COUNT {
        pg_signal_array[i as usize].sa_handler = SIG_DFL;
        pg_signal_array[i as usize].sa_mask = 0;
        pg_signal_array[i as usize].sa_flags = 0;
        pg_signal_defaults[i as usize] = SIG_IGN;
        i += 1;
    }
    pg_signal_mask = 0;
    pg_signal_queue = 0;

    /* Create the global event handle used to flag signals */
    pgwin32_signal_event = CreateEvent(
        std::ptr::null_mut(),
        TRUE,
        FALSE,
        std::ptr::null(),
    );
    if pgwin32_signal_event.is_null() {
        ereport!(
            FATAL,
            errmsg!("could not create signal event: error code {}", GetLastError())
        );
    }

    /* Create thread for handling signals */
    signal_thread_handle = CreateThread(
        std::ptr::null_mut(),
        0,
        pg_signal_thread,
        std::ptr::null_mut(),
        0,
        std::ptr::null_mut(),
    );
    if signal_thread_handle.is_null() {
        ereport!(FATAL, errmsg!("could not create signal handler thread"));
    }

    /* Create console control handle to pick up Ctrl-C etc */
    if SetConsoleCtrlHandler(pg_console_handler, TRUE) == 0 {
        ereport!(FATAL, errmsg!("could not set console control handler"));
    }

    let _ = signal_thread_handle;
}

/*
 * Dispatch all signals currently queued and not blocked
 * Blocked signals are ignored, and will be fired at the time of
 * the pqsigprocmask() call.
 */
pub unsafe fn pgwin32_dispatch_queued_signals() {
    let mut exec_mask: c_int;

    debug_assert!(!pgwin32_signal_event.is_null());
    EnterCriticalSection(&raw mut pg_signal_crit_sec);
    loop {
        exec_mask = UNBLOCKED_SIGNAL_QUEUE();
        if exec_mask == 0 {
            break;
        }
        /* One or more unblocked signals queued for execution */

        let mut i: c_int = 1;
        while i < PG_SIGNAL_COUNT {
            if exec_mask & sigmask(i) != 0 {
                /* Execute this signal */
                let act: *mut sigaction = &raw mut pg_signal_array[i as usize];
                let mut sig: *mut c_void = (*act).sa_handler;

                if sig == SIG_DFL {
                    sig = pg_signal_defaults[i as usize];
                }
                pg_signal_queue &= !sigmask(i);
                if sig != SIG_ERR && sig != SIG_IGN && sig != SIG_DFL {
                    let mut block_mask: sigset_t;
                    let mut save_mask: sigset_t = 0;

                    LeaveCriticalSection(&raw mut pg_signal_crit_sec);

                    block_mask = (*act).sa_mask;
                    if (*act).sa_flags & SA_NODEFER == 0 {
                        block_mask |= sigmask(i);
                    }

                    sigprocmask(SIG_BLOCK, &block_mask, &raw mut save_mask);
                    let sigfn: pqsigfunc = std::mem::transmute(sig);
                    sigfn(i);
                    sigprocmask(SIG_SETMASK, &save_mask, std::ptr::null_mut());

                    EnterCriticalSection(&raw mut pg_signal_crit_sec);
                    break; /* Restart outer loop, in case signal mask or
                            * queue has been modified inside signal
                            * handler */
                }
            }
            i += 1;
        }
    }
    ResetEvent(pgwin32_signal_event);
    LeaveCriticalSection(&raw mut pg_signal_crit_sec);
}

/* signal masking. Only called on main thread, no sync required */
pub unsafe fn pqsigprocmask(how: c_int, set: *const sigset_t, oset: *mut sigset_t) -> c_int {
    if !oset.is_null() {
        *oset = pg_signal_mask;
    }

    if set.is_null() {
        return 0;
    }

    match how {
        SIG_BLOCK => {
            pg_signal_mask |= *set;
        }
        SIG_UNBLOCK => {
            pg_signal_mask &= !*set;
        }
        SIG_SETMASK => {
            pg_signal_mask = *set;
        }
        _ => {
            set_errno(EINVAL);
            return -1;
        }
    }

    /*
     * Dispatch any signals queued up right away, in case we have unblocked
     * one or more signals previously queued
     */
    pgwin32_dispatch_queued_signals();

    0
}

/*
 * Unix-like signal handler installation
 *
 * Only called on main thread, no sync required
 */
pub unsafe fn pqsigaction(
    signum: c_int,
    act: *const sigaction,
    oldact: *mut sigaction,
) -> c_int {
    if signum >= PG_SIGNAL_COUNT || signum < 0 {
        set_errno(EINVAL);
        return -1;
    }
    if !oldact.is_null() {
        *oldact = pg_signal_array[signum as usize];
    }
    if !act.is_null() {
        pg_signal_array[signum as usize] = *act;
    }
    0
}

/* Create the signal listener pipe for specified PID */
pub unsafe fn pgwin32_create_signal_listener(pid: pid_t) -> HANDLE {
    let mut pipename: [c_char; 128] = [0; 128];
    let pipe: HANDLE;

    snprintf(
        pipename.as_mut_ptr(),
        std::mem::size_of_val(&pipename),
        c"\\\\.\\pipe\\pgsignal_%u".as_ptr(),
        pid as c_int,
    );

    pipe = CreateNamedPipe(
        pipename.as_ptr(),
        PIPE_ACCESS_DUPLEX,
        PIPE_TYPE_MESSAGE | PIPE_READMODE_MESSAGE | PIPE_WAIT,
        PIPE_UNLIMITED_INSTANCES,
        16,
        16,
        1000,
        std::ptr::null_mut(),
    );

    if pipe == INVALID_HANDLE_VALUE {
        ereport!(
            ERROR,
            errmsg!(
                "could not create signal listener pipe for PID {}: error code {}",
                pid as c_int,
                GetLastError()
            )
        );
    }

    pipe
}

/*
 * All functions below execute on the signal handler thread
 * and must be synchronized as such!
 * NOTE! The only global variable that can be used is
 * pg_signal_queue!
 */

/*
 * Queue a signal for the main thread, by setting the flag bit and event.
 */
pub unsafe fn pg_queue_signal(signum: c_int) {
    debug_assert!(!pgwin32_signal_event.is_null());
    if signum >= PG_SIGNAL_COUNT || signum <= 0 {
        return; /* ignore any bad signal number */
    }

    EnterCriticalSection(&raw mut pg_signal_crit_sec);
    pg_signal_queue |= sigmask(signum);
    LeaveCriticalSection(&raw mut pg_signal_crit_sec);

    SetEvent(pgwin32_signal_event);
}

/* Signal handling thread */
unsafe extern "system" fn pg_signal_thread(param: LPVOID) -> DWORD {
    let mut pipename: [c_char; 128] = [0; 128];
    let mut pipe: HANDLE = pgwin32_initial_signal_pipe;

    /* Set up pipe name, in case we have to re-create the pipe. */
    snprintf(
        pipename.as_mut_ptr(),
        std::mem::size_of_val(&pipename),
        c"\\\\.\\pipe\\pgsignal_%lu".as_ptr(),
        GetCurrentProcessId(),
    );

    loop {
        let fConnected: BOOL;

        /* Create a new pipe instance if we don't have one. */
        if pipe == INVALID_HANDLE_VALUE {
            pipe = CreateNamedPipe(
                pipename.as_ptr(),
                PIPE_ACCESS_DUPLEX,
                PIPE_TYPE_MESSAGE | PIPE_READMODE_MESSAGE | PIPE_WAIT,
                PIPE_UNLIMITED_INSTANCES,
                16,
                16,
                1000,
                std::ptr::null_mut(),
            );

            if pipe == INVALID_HANDLE_VALUE {
                write_stderr(
                    c"could not create signal listener pipe: error code %lu; retrying\n".as_ptr(),
                    GetLastError(),
                );
                SleepEx(500, FALSE);
                continue;
            }
        }

        /*
         * Wait for a client to connect.  If something connects before we
         * reach here, we'll get back a "failure" with ERROR_PIPE_CONNECTED,
         * which is actually a success (way to go, Microsoft).
         */
        fConnected = if ConnectNamedPipe(pipe, std::ptr::null_mut()) != 0 {
            TRUE
        } else if GetLastError() == ERROR_PIPE_CONNECTED {
            TRUE
        } else {
            FALSE
        };
        if fConnected != 0 {
            /*
             * We have a connection from a would-be signal sender. Process it.
             */
            let mut sigNum: BYTE = 0;
            let mut bytes: DWORD = 0;

            if ReadFile(
                pipe,
                &raw mut sigNum as *mut c_void,
                1,
                &raw mut bytes,
                std::ptr::null_mut(),
            ) != 0
                && bytes == 1
            {
                /*
                 * Queue the signal before responding to the client.  In this
                 * way, it's guaranteed that once kill() has returned in the
                 * signal sender, the next CHECK_FOR_INTERRUPTS() in the
                 * signal recipient will see the signal.  (This is a stronger
                 * guarantee than POSIX makes; maybe we don't need it?  But
                 * without it, we've seen timing bugs on Windows that do not
                 * manifest on any known Unix.)
                 */
                pg_queue_signal(sigNum as c_int);

                /*
                 * Write something back to the client, allowing its
                 * CallNamedPipe() call to terminate.
                 */
                WriteFile(
                    pipe,
                    &raw const sigNum as *const c_void,
                    1,
                    &raw mut bytes,
                    std::ptr::null_mut(),
                ); /* Don't care if it
                    * works or not */

                /*
                 * We must wait for the client to read the data before we can
                 * disconnect, else the data will be lost.  (If the WriteFile
                 * call failed, there'll be nothing in the buffer, so this
                 * shouldn't block.)
                 */
                FlushFileBuffers(pipe);
            } else {
                /*
                 * If we fail to read a byte from the client, assume it's the
                 * client's problem and do nothing.  Perhaps it'd be better to
                 * force a pipe close and reopen?
                 */
            }

            /* Disconnect from client so that we can re-use the pipe. */
            DisconnectNamedPipe(pipe);
        } else {
            /*
             * Connection failed.  Cleanup and try again.
             *
             * This should never happen.  If it does, there's a window where
             * we'll miss signals until we manage to re-create the pipe.
             * However, just trying to use the same pipe again is probably not
             * going to work, so we have little choice.
             */
            CloseHandle(pipe);
            pipe = INVALID_HANDLE_VALUE;
        }
    }

    #[allow(unreachable_code)]
    {
        let _ = param;
        0
    }
}

/* Console control handler will execute on a thread created
by the OS at the time of invocation */
unsafe extern "system" fn pg_console_handler(dwCtrlType: DWORD) -> BOOL {
    if dwCtrlType == CTRL_C_EVENT
        || dwCtrlType == CTRL_BREAK_EVENT
        || dwCtrlType == CTRL_CLOSE_EVENT
        || dwCtrlType == CTRL_SHUTDOWN_EVENT
    {
        pg_queue_signal(SIGINT);
        return TRUE;
    }
    FALSE
}
