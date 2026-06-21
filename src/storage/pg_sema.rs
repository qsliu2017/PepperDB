//! storage/pg_sema.h - Platform-independent API for semaphores.
//!
//! PostgreSQL requires counting semaphores (the kind that keep track of
//! multiple unlock operations, and will allow an equal number of subsequent
//! lock operations before blocking). The underlying implementation is not the
//! same on every platform; this file defines the API each port must provide.
//!
//! The implementation below is the Win32 Semaphores Emulation port
//! (src/backend/port/win32_sema.c).  In the Win32 implementation PGSemaphore is
//! just a HANDLE, and the semaphores are acquired on-demand using anonymous
//! Win32 semaphores so they are freed when the last referencing process exits.

use std::ffi::c_int;

use crate::c::Size;
use crate::postgres::Datum;
use crate::utils::elog::{FATAL, PANIC};
use crate::{elog, ereport, errmsg};

// miscadmin.h: IsUnderPostmaster.
use crate::utils::init::globals::IsUnderPostmaster;

// storage/ipc.h: on_shmem_exit registration.
use crate::storage::ipc::ipc::on_shmem_exit;

// Win32 API surface (src/port/win32_port.rs and friends).
use crate::port::win32_port::{
    pgwin32_dispatch_queued_signals, pgwin32_signal_event, GetLastError, WaitForMultipleObjectsEx,
    BOOL, DWORD, HANDLE, WAIT_IO_COMPLETION, WAIT_OBJECT_0, WAIT_TIMEOUT,
};

/// struct PGSemaphoreData and pointer type PGSemaphore are the data structure
/// representing an individual semaphore. The contents of PGSemaphoreData vary
/// across implementations and must never be touched by platform-independent
/// code; hence, PGSemaphoreData is declared as an opaque struct here.
///
/// On Windows (USE_WIN32_SEMAPHORES) PGSemaphore is just defined as HANDLE; we
/// model the non-Windows case (the opaque struct pointer).
#[repr(C)]
pub struct PGSemaphoreData {
    _opaque: [u8; 0],
}

pub type PGSemaphore = *mut PGSemaphoreData;

// ------------------------------------------------------------------
// Win32 types/constants and API bindings used only by this port.  The real
// declarations live in <windows.h>; on a non-Windows build target these are
// TODO(pg-port) stubs that resolve symbols so the file stays self-consistent.
// ------------------------------------------------------------------

const WAIT_FAILED: c_int = -1; // 0xFFFFFFFF
const TRUE: BOOL = 1;

const EAGAIN: c_int = 11; // errno EAGAIN

/// SECURITY_ATTRIBUTES (winbase.h).
#[repr(C)]
struct SECURITY_ATTRIBUTES {
    nLength: DWORD,
    lpSecurityDescriptor: *mut std::ffi::c_void,
    bInheritHandle: BOOL,
}

extern "C" {
    fn malloc(size: usize) -> *mut std::ffi::c_void;
    fn free(ptr: *mut std::ffi::c_void);

    /// errno access (thread-local); on MSVC this is _errno().
    fn _errno() -> *mut c_int;
}

// TODO(pg-port): genuine Win32 imports unavailable on this build target.
unsafe fn CreateSemaphore(
    _lpSemaphoreAttributes: *mut SECURITY_ATTRIBUTES,
    _lInitialCount: c_int,
    _lMaximumCount: c_int,
    _lpName: *const std::ffi::c_char,
) -> HANDLE {
    todo!("TODO(pg-port): CreateSemaphore")
}

unsafe fn ReleaseSemaphore(
    _hSemaphore: HANDLE,
    _lReleaseCount: c_int,
    _lpPreviousCount: *mut c_int,
) -> BOOL {
    todo!("TODO(pg-port): ReleaseSemaphore")
}

unsafe fn WaitForSingleObject(_hHandle: HANDLE, _dwMilliseconds: DWORD) -> c_int {
    todo!("TODO(pg-port): WaitForSingleObject")
}

unsafe fn CloseHandle(_hObject: HANDLE) -> BOOL {
    todo!("TODO(pg-port): CloseHandle")
}

/// CHECK_FOR_INTERRUPTS() (miscadmin.h).
unsafe fn CHECK_FOR_INTERRUPTS() {
    crate::miscadmin::CHECK_FOR_INTERRUPTS();
}

// ------------------------------------------------------------------
// Static state: IDs of sema sets acquired so far.
// ------------------------------------------------------------------

static mut mySemSet: *mut HANDLE = std::ptr::null_mut(); // IDs of sema sets acquired so far
static mut numSems: c_int = 0; // number of sema sets acquired so far
static mut maxSems: c_int = 0; // allocated size of mySemaSet array

/// Report amount of shared memory needed for semaphores
pub unsafe fn PGSemaphoreShmemSize(maxSemas: c_int) -> Size {
    let _ = maxSemas;
    /* No shared memory needed on Windows */
    0
}

/// PGReserveSemaphores --- initialize semaphore support
///
/// In the Win32 implementation, we acquire semaphores on-demand; the
/// maxSemas parameter is just used to size the array that keeps track of
/// acquired semas for subsequent releasing.  We use anonymous semaphores
/// so the semaphores are automatically freed when the last referencing
/// process exits.
pub unsafe fn PGReserveSemaphores(maxSemas: c_int) {
    mySemSet = malloc(maxSemas as usize * core::mem::size_of::<HANDLE>()) as *mut HANDLE;
    if mySemSet.is_null() {
        elog!(PANIC, "out of memory");
    }
    numSems = 0;
    maxSems = maxSemas;

    on_shmem_exit(ReleaseSemaphores, 0 as Datum);
}

/// Release semaphores at shutdown or shmem reinitialization
///
/// (called as an on_shmem_exit callback, hence funny argument list)
unsafe extern "C" fn ReleaseSemaphores(_code: c_int, _arg: Datum) {
    let mut i: c_int = 0;
    while i < numSems {
        CloseHandle(*mySemSet.add(i as usize));
        i += 1;
    }
    free(mySemSet as *mut std::ffi::c_void);
}

/// PGSemaphoreCreate
///
/// Allocate a PGSemaphore structure with initial count 1
pub unsafe fn PGSemaphoreCreate() -> PGSemaphore {
    let cur_handle: HANDLE;
    let mut sec_attrs: SECURITY_ATTRIBUTES = core::mem::zeroed();

    /* Can't do this in a backend, because static state is postmaster's */
    crate::Assert!(!IsUnderPostmaster);

    if numSems >= maxSems {
        elog!(PANIC, "too many semaphores created");
    }

    /* ZeroMemory(&sec_attrs, sizeof(sec_attrs)) done by zeroed() above */
    sec_attrs.nLength = core::mem::size_of::<SECURITY_ATTRIBUTES>() as DWORD;
    sec_attrs.lpSecurityDescriptor = std::ptr::null_mut();
    sec_attrs.bInheritHandle = TRUE;

    /* We don't need a named semaphore */
    cur_handle = CreateSemaphore(&raw mut sec_attrs, 1, 32767, std::ptr::null());
    if !cur_handle.is_null() {
        /* Successfully done */
        *mySemSet.add(numSems as usize) = cur_handle;
        numSems += 1;
    } else {
        ereport!(
            PANIC,
            errmsg!("could not create semaphore: error code {}", GetLastError())
        );
    }

    cur_handle as PGSemaphore
}

/// PGSemaphoreReset
///
/// Reset a previously-initialized PGSemaphore to have count 0
pub unsafe fn PGSemaphoreReset(sema: PGSemaphore) {
    /*
     * There's no direct API for this in Win32, so we have to ratchet the
     * semaphore down to 0 with repeated trylock's.
     */
    while PGSemaphoreTryLock(sema) { /* loop */ }
}

/// PGSemaphoreLock
///
/// Lock a semaphore (decrement count), blocking if count would be < 0.
pub unsafe fn PGSemaphoreLock(sema: PGSemaphore) {
    return crate::port::sysv_sema::PGSemaphoreLock(sema as _);
    #[allow(unreachable_code)]
    let mut wh: [HANDLE; 2] = [std::ptr::null_mut(); 2];
    let mut done: bool = false;

    /*
     * Note: pgwin32_signal_event should be first to ensure that it will be
     * reported when multiple events are set.  We want to guarantee that
     * pending signals are serviced.
     */
    wh[0] = pgwin32_signal_event;
    wh[1] = sema as HANDLE;

    /*
     * As in other implementations of PGSemaphoreLock, we need to check for
     * cancel/die interrupts each time through the loop.  But here, there is
     * no hidden magic about whether the syscall will internally service a
     * signal --- we do that ourselves.
     */
    while !done {
        let rc: DWORD;

        CHECK_FOR_INTERRUPTS();

        rc = WaitForMultipleObjectsEx(2, wh.as_ptr(), 0 /* FALSE */, DWORD::MAX /* INFINITE */, TRUE)
            as DWORD;
        match rc as c_int {
            x if x == WAIT_OBJECT_0 => {
                /* Signal event is set - we have a signal to deliver */
                pgwin32_dispatch_queued_signals();
            }
            x if x == WAIT_OBJECT_0 + 1 => {
                /* We got it! */
                done = true;
            }
            x if x == WAIT_IO_COMPLETION => {
                /*
                 * The system interrupted the wait to execute an I/O
                 * completion routine or asynchronous procedure call in this
                 * thread.  PostgreSQL does not provoke either of these, but
                 * atypical loaded DLLs or even other processes might do so.
                 * Now, resume waiting.
                 */
            }
            x if x == WAIT_FAILED => {
                ereport!(
                    FATAL,
                    errmsg!("could not lock semaphore: error code {}", GetLastError())
                );
            }
            _ => {
                elog!(
                    FATAL,
                    "unexpected return code from WaitForMultipleObjectsEx(): {}",
                    rc
                );
            }
        }
    }
}

/// PGSemaphoreUnlock
///
/// Unlock a semaphore (increment count)
pub unsafe fn PGSemaphoreUnlock(sema: PGSemaphore) {
    return crate::port::sysv_sema::PGSemaphoreUnlock(sema as _);
    #[allow(unreachable_code)]
    if ReleaseSemaphore(sema as HANDLE, 1, std::ptr::null_mut()) == 0 {
        ereport!(
            FATAL,
            errmsg!("could not unlock semaphore: error code {}", GetLastError())
        );
    }
}

/// PGSemaphoreTryLock
///
/// Lock a semaphore only if able to do so without blocking
pub unsafe fn PGSemaphoreTryLock(sema: PGSemaphore) -> bool {
    return crate::port::sysv_sema::PGSemaphoreTryLock(sema as _);
    #[allow(unreachable_code)]
    let ret: DWORD;

    ret = WaitForSingleObject(sema as HANDLE, 0) as DWORD;

    if ret as c_int == WAIT_OBJECT_0 {
        /* We got it! */
        return true;
    } else if ret as c_int == WAIT_TIMEOUT {
        /* Can't get it */
        *_errno() = EAGAIN;
        return false;
    }

    /* Otherwise we are in trouble */
    ereport!(
        FATAL,
        errmsg!("could not try-lock semaphore: error code {}", GetLastError())
    );

    /* keep compiler quiet */
    #[allow(unreachable_code)]
    false
}
