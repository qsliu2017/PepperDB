//! port/posix_sema.c - Implement PGSemaphores using POSIX semaphore facilities.
//!
//! We prefer the unnamed style of POSIX semaphore (the kind made with
//! sem_init).  We can cope with the kind made with sem_open, however.
//!
//! In either implementation, typedef PGSemaphore is equivalent to "sem_t *".
//! With unnamed semaphores, the sem_t structs live in an array in shared
//! memory.  With named semaphores, that's not true because we cannot persuade
//! sem_open to do its allocation there.  Therefore, the named-semaphore code
//! *does not cope with EXEC_BACKEND*.
//!
//! This port targets the unnamed-POSIX-semaphores configuration
//! (USE_UNNAMED_POSIX_SEMAPHORES), which is the common case on Linux/BSD; the
//! named-semaphore (USE_NAMED_POSIX_SEMAPHORES) branches are preserved as
//! comments / dead alternatives but not compiled.

use crate::prelude::*;

use crate::storage::pg_sema::PGSemaphore;

// miscadmin.h: IsUnderPostmaster, DataDir (extern globals).
use crate::utils::init::globals::{DataDir, IsUnderPostmaster};

// pg_config_manual.h / pg_config.h: cache line size used to pad the sem_t.
use crate::pg_config_manual::PG_CACHE_LINE_SIZE;

// ------------------------------------------------------------------
// libc bindings (sem_t API, errno, stat).  This build targets macOS/Darwin,
// where errno is reached via __error().  TODO: dedup with a central errno shim.
// ------------------------------------------------------------------

/// Opaque POSIX sem_t.  Its real size varies by platform; we only ever touch
/// it through pointers handed to the libc sem_* functions, and store it inside
/// a cache-line-sized padding union (SemTPadded), so its concrete layout here
/// only needs to be at least as large as the platform sem_t.  On Darwin sem_t
/// is an `int` (named semaphores only), but we keep a generous opaque buffer.
#[repr(C)]
struct sem_t {
    _opaque: [u8; 32],
}

extern "C" {
    fn sem_init(sem: *mut sem_t, pshared: c_int, value: c_uint) -> c_int;
    fn sem_destroy(sem: *mut sem_t) -> c_int;
    fn sem_wait(sem: *mut sem_t) -> c_int;
    fn sem_trywait(sem: *mut sem_t) -> c_int;
    fn sem_post(sem: *mut sem_t) -> c_int;

    /// errno access (thread-local).  macOS/Darwin uses __error().
    fn __error() -> *mut c_int;
}

#[inline]
unsafe fn errno() -> c_int {
    *__error()
}

// errno constants (Darwin/BSD values).
const EINTR: c_int = 4;
const EAGAIN: c_int = 35;
const EDEADLK: c_int = 11;

// ------------------------------------------------------------------
// SemTPadded / PGSemaphoreData layout (file header comment).
// ------------------------------------------------------------------

/// union SemTPadded { sem_t pgsem; char pad[PG_CACHE_LINE_SIZE]; }
///
/// Modeled as a byte buffer of PG_CACHE_LINE_SIZE that we reinterpret as a
/// sem_t when needed; this matches the C union's size and alignment intent
/// (the padding dominates the small sem_t).
#[repr(C)]
#[repr(align(8))]
struct SemTPadded {
    pad: [u8; PG_CACHE_LINE_SIZE],
}

/// struct PGSemaphoreData { SemTPadded sem_padded; }
#[repr(C)]
struct PGSemaphoreDataImpl {
    sem_padded: SemTPadded,
}

/// PG_SEM_REF(x) ((sem_t *) &(x)->sem_padded.pgsem)
#[inline]
unsafe fn PG_SEM_REF(x: *mut PGSemaphoreDataImpl) -> *mut sem_t {
    (&raw mut (*x).sem_padded) as *mut sem_t
}

/// #define IPCProtection (0600) - access/modify by user only.
const IPCProtection: c_int = 0o600;

// ------------------------------------------------------------------
// Module-local state.  In C these are file-scope statics; the postmaster owns
// them (Assert(!IsUnderPostmaster) in PGSemaphoreCreate).
// ------------------------------------------------------------------

/// array of PGSemaphoreData in shared memory (unnamed-semaphore case).
static mut sharedSemas: *mut PGSemaphoreDataImpl = null_mut();
/// number of semas acquired so far.
static mut numSems: c_int = 0;
/// allocated size of above arrays.
static mut maxSems: c_int = 0;
/// next name to try.
static mut nextSemKey: c_int = 0;

// ------------------------------------------------------------------
// PosixSemaphoreCreate / PosixSemaphoreKill
// ------------------------------------------------------------------

/// PosixSemaphoreCreate - attempt to create a new unnamed semaphore.
unsafe fn PosixSemaphoreCreate(sem: *mut sem_t) {
    if sem_init(sem, 1, 1) < 0 {
        elog!(FATAL, "sem_init failed: %m");
    }
}

/// PosixSemaphoreKill - removes a semaphore.
unsafe fn PosixSemaphoreKill(sem: *mut sem_t) {
    // Got to use sem_destroy for unnamed semaphores.
    if sem_destroy(sem) < 0 {
        elog!(LOG, "sem_destroy failed: %m");
    }
}

// ------------------------------------------------------------------
// Public API.
// ------------------------------------------------------------------

/// Report amount of shared memory needed for semaphores.
pub unsafe fn PGSemaphoreShmemSize(maxSemas: c_int) -> Size {
    // Need a PGSemaphoreData per semaphore.
    mul_size(
        maxSemas as Size,
        core::mem::size_of::<PGSemaphoreDataImpl>() as Size,
    )
}

/// PGReserveSemaphores --- initialize semaphore support.
///
/// This is called during postmaster start or shared memory reinitialization.
/// It should do whatever is needed to be able to support up to maxSemas
/// subsequent PGSemaphoreCreate calls.
///
/// In the Posix implementation, we acquire semaphores on-demand; the maxSemas
/// parameter is just used to size the arrays.  For unnamed semaphores, there is
/// an array of PGSemaphoreData structs in shared memory.
pub unsafe fn PGReserveSemaphores(maxSemas: c_int) {
    let mut statbuf: libc_stat = core::mem::zeroed();

    // We use the data directory's inode number to seed the search for free
    // semaphore keys.  This minimizes the odds of collision with other
    // postmasters, while maximizing the odds that we will detect and clean up
    // semaphores left over from a crashed postmaster in our own directory.
    if stat(DataDir, &mut statbuf) < 0 {
        let _ = errcode_for_file_access();
        ereport!(FATAL, "could not stat data directory");
    }

    // We must use ShmemAllocUnlocked(), since the spinlock protecting
    // ShmemAlloc() won't be ready yet.
    sharedSemas = ShmemAllocUnlocked(PGSemaphoreShmemSize(maxSemas)) as *mut PGSemaphoreDataImpl;

    numSems = 0;
    maxSems = maxSemas;
    nextSemKey = statbuf.st_ino as c_int;

    on_shmem_exit(ReleaseSemaphores, 0 as Datum);
}

/// Release semaphores at shutdown or shmem reinitialization.
///
/// (called as an on_shmem_exit callback, hence funny argument list)
unsafe fn ReleaseSemaphores(_status: c_int, _arg: Datum) {
    // USE_UNNAMED_POSIX_SEMAPHORES branch.
    let mut i: c_int = 0;
    while i < numSems {
        PosixSemaphoreKill(PG_SEM_REF(sharedSemas.add(i as usize)));
        i += 1;
    }
}

/// PGSemaphoreCreate - allocate a PGSemaphore structure with initial count 1.
pub unsafe fn PGSemaphoreCreate() -> PGSemaphore {
    let sema: *mut PGSemaphoreDataImpl;
    let newsem: *mut sem_t;

    // Can't do this in a backend, because static state is postmaster's.
    Assert!(!IsUnderPostmaster);

    if numSems >= maxSems {
        elog!(PANIC, "too many semaphores created");
    }

    sema = sharedSemas.add(numSems as usize);
    newsem = PG_SEM_REF(sema);
    PosixSemaphoreCreate(newsem);

    numSems += 1;

    sema as PGSemaphore
}

/// PGSemaphoreReset - reset a previously-initialized PGSemaphore to have count 0.
pub unsafe fn PGSemaphoreReset(sema: PGSemaphore) {
    // There's no direct API for this in POSIX, so we have to ratchet the
    // semaphore down to 0 with repeated trywait's.
    loop {
        if sem_trywait(PG_SEM_REF(sema as *mut PGSemaphoreDataImpl)) < 0 {
            if errno() == EAGAIN || errno() == EDEADLK {
                break; // got it down to 0
            }
            if errno() == EINTR {
                continue; // can this happen?
            }
            elog!(FATAL, "sem_trywait failed: %m");
        }
    }
}

/// PGSemaphoreLock - lock a semaphore (decrement count), blocking if count
/// would be < 0.
pub unsafe fn PGSemaphoreLock(sema: PGSemaphore) {
    let mut errStatus: c_int;

    // See notes in sysv_sema.c's implementation of PGSemaphoreLock.
    loop {
        errStatus = sem_wait(PG_SEM_REF(sema as *mut PGSemaphoreDataImpl));
        if !(errStatus < 0 && errno() == EINTR) {
            break;
        }
    }

    if errStatus < 0 {
        elog!(FATAL, "sem_wait failed: %m");
    }
}

/// PGSemaphoreUnlock - unlock a semaphore (increment count).
pub unsafe fn PGSemaphoreUnlock(sema: PGSemaphore) {
    let mut errStatus: c_int;

    // Note: if errStatus is -1 and errno == EINTR then it means we returned
    // from the operation prematurely because we were sent a signal.  So we try
    // and unlock the semaphore again. Not clear this can really happen, but
    // might as well cope.
    loop {
        errStatus = sem_post(PG_SEM_REF(sema as *mut PGSemaphoreDataImpl));
        if !(errStatus < 0 && errno() == EINTR) {
            break;
        }
    }

    if errStatus < 0 {
        elog!(FATAL, "sem_post failed: %m");
    }
}

/// PGSemaphoreTryLock - lock a semaphore only if able to do so without blocking.
pub unsafe fn PGSemaphoreTryLock(sema: PGSemaphore) -> bool {
    let mut errStatus: c_int;

    // Note: if errStatus is -1 and errno == EINTR then it means we returned
    // from the operation prematurely because we were sent a signal.  So we try
    // and lock the semaphore again.
    loop {
        errStatus = sem_trywait(PG_SEM_REF(sema as *mut PGSemaphoreDataImpl));
        if !(errStatus < 0 && errno() == EINTR) {
            break;
        }
    }

    if errStatus < 0 {
        if errno() == EAGAIN || errno() == EDEADLK {
            return false; // failed to lock it
        }
        // Otherwise we got trouble.
        elog!(FATAL, "sem_trywait failed: %m");
    }

    true
}

// ------------------------------------------------------------------
// Local stubs for not-yet-ported dependencies.
// ------------------------------------------------------------------

// <sys/stat.h>: struct stat and stat().  We only read st_ino, so we model just
// enough of the Darwin struct stat layout; binding the libc stat() symbol.
// TODO: replace with a real <sys/stat.h> port.
#[repr(C)]
struct libc_stat {
    st_dev: i32,
    st_mode: u16,
    st_nlink: u16,
    st_ino: u64,
    st_uid: u32,
    st_gid: u32,
    st_rdev: i32,
    st_atime: i64,
    st_atime_nsec: i64,
    st_mtime: i64,
    st_mtime_nsec: i64,
    st_ctime: i64,
    st_ctime_nsec: i64,
    st_birthtime: i64,
    st_birthtime_nsec: i64,
    st_size: i64,
    st_blocks: i64,
    st_blksize: i32,
    st_flags: u32,
    st_gen: u32,
    st_lspare: i32,
    st_qspare: [i64; 2],
}

extern "C" {
    #[link_name = "stat$INODE64"]
    fn stat(path: *const c_char, buf: *mut libc_stat) -> c_int;
}

// storage/shmem.h: mul_size() - size product with overflow check.
// TODO: import from storage/shmem.rs once ported.
unsafe fn mul_size(s1: Size, s2: Size) -> Size {
    s1 * s2
}

// storage/shmem.h: ShmemAllocUnlocked() - allocate from shared memory without
// the spinlock (used before the spinlock is ready).
// TODO: import from storage/ipc/shmem.rs once ported.
unsafe fn ShmemAllocUnlocked(size: Size) -> *mut c_void {
    let _ = size;
    unimplemented!("storage/shmem.h ShmemAllocUnlocked not ported");
}

// storage/ipc.h: on_shmem_exit() - register a shmem-exit callback.
// TODO: import from storage/ipc/ipc.rs once ported.
unsafe fn on_shmem_exit(_function: unsafe fn(c_int, Datum), _arg: Datum) {
    // no-op stub
}

// utils/elog.h: errcode_for_file_access().
// TODO: port from elog.c.
unsafe fn errcode_for_file_access() -> c_int {
    0
}
