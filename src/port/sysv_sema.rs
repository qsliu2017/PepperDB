//! sysv_sema.rs
//!   Implement PGSemaphores using SysV semaphore facilities
//!
//! Translated 1:1 from postgres/src/backend/port/sysv_sema.c
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! #include mapping:
//!   "postgres.h"          -> crate::prelude::*
//!   <sys/ipc.h>/<sys/sem.h>/<sys/stat.h> -> libc extern decls below
//!   "miscadmin.h"         -> crate::utils::init::globals (DataDir, IsUnderPostmaster)
//!   "storage/ipc.h"       -> crate::storage::ipc::ipc (on_shmem_exit)
//!   "storage/pg_sema.h"   -> crate::storage::pg_sema (opaque PGSemaphore API type)
//!   "storage/shmem.h"     -> crate::storage::ipc::shmem (ShmemAllocUnlocked, mul_size)
//!
//! NOTE: like the sibling posix_sema.rs, the API-level PGSemaphore is the
//! opaque crate::storage::pg_sema::PGSemaphore; this implementation's concrete
//! per-semaphore state is the local PGSemaphoreDataImpl, cast at boundaries.

use crate::prelude::*;

use crate::storage::ipc::ipc::on_shmem_exit;
use crate::storage::ipc::shmem::{mul_size, ShmemAllocUnlocked};
use crate::storage::pg_sema::PGSemaphore;
use crate::utils::init::globals::{DataDir, IsUnderPostmaster};
use crate::{elog, ereport, errmsg, Assert};

use std::ffi::{c_char, c_int, c_long, c_short, c_uint, c_ulong, c_ushort, c_void};
use std::ptr::null_mut;

// ---------------------------------------------------------------------------
// libc bindings (SysV semaphores, stat, signals).  This build targets
// macOS/Darwin, where errno is reached via __error().
// ---------------------------------------------------------------------------

pub type key_t = i32; // sys/types.h (Darwin: __int32_t)
pub type pid_t = i32;
pub type ino_t = u64; // Darwin: __darwin_ino64_t

/// struct sembuf (sys/sem.h)
#[repr(C)]
struct sembuf {
    sem_num: c_ushort, /* semaphore # */
    sem_op: c_short,   /* semaphore operation */
    sem_flg: c_short,  /* operation flags */
}

/// union semun (sys/sem.h; HAVE_UNION_SEMUN on Darwin)
#[repr(C)]
#[derive(Clone, Copy)]
union semun {
    val: c_int,
    buf: *mut c_void, /* struct semid_ds * (opaque here) */
    array: *mut c_ushort,
}

/// Minimal struct stat - we only need st_ino.  Use the libc stat64-compatible
/// layout on Darwin via a byte buffer and the st_ino offset; simpler and safer:
/// declare an opaque buffer big enough and read st_ino at its Darwin offset.
/// Darwin struct stat: dev_t(4) mode_t(2) nlink_t(2) ino_t(8) at offset 8.
#[repr(C)]
struct stat_buf {
    st_dev: i32,
    st_mode: u16,
    st_nlink: u16,
    st_ino: u64,
    _rest: [u8; 128], /* uid, gid, rdev, timespecs, sizes... (overallocated) */
}

extern "C" {
    fn semget(key: key_t, nsems: c_int, semflg: c_int) -> c_int;
    fn semop(semid: c_int, sops: *mut sembuf, nsops: usize) -> c_int;
    fn semctl(semid: c_int, semnum: c_int, cmd: c_int, ...) -> c_int;
    fn getpid() -> pid_t;
    fn kill(pid: pid_t, sig: c_int) -> c_int;
    fn malloc(size: usize) -> *mut c_void;
    fn free(ptr: *mut c_void);
    #[link_name = "stat$INODE64"]
    fn stat_inode64(path: *const c_char, buf: *mut stat_buf) -> c_int;
    /// errno access (thread-local).  macOS/Darwin uses __error().
    fn __error() -> *mut c_int;
}

#[inline]
unsafe fn errno() -> c_int {
    *__error()
}

// errno constants (Darwin/BSD values).
const EINTR: c_int = 4;
const EACCES: c_int = 13;
const EEXIST: c_int = 17;
const EINVAL: c_int = 22;
const EAGAIN: c_int = 35;
const ENOSPC: c_int = 28;
const ESRCH: c_int = 3;
const EIDRM: c_int = 90; // Darwin: identifier removed

// sys/ipc.h / sys/sem.h command + flag constants (Darwin values).
const IPC_CREAT: c_int = 0o001000;
const IPC_EXCL: c_int = 0o002000;
const IPC_NOWAIT: c_int = 0o004000;
const IPC_RMID: c_int = 0;
const GETVAL: c_int = 5;
const SETVAL: c_int = 8;
const GETPID: c_int = 4;

// ---------------------------------------------------------------------------
// File-local types and state (1:1 with the C)
// ---------------------------------------------------------------------------

/// typedef struct PGSemaphoreData { int semId; int semNum; } - this
/// implementation's concrete semaphore state (the API type is opaque).
#[repr(C)]
pub struct PGSemaphoreDataImpl {
    pub semId: c_int,  /* semaphore set identifier */
    pub semNum: c_int, /* semaphore number within set */
}

type IpcSemaphoreKey = key_t; /* semaphore key passed to semget(2) */
type IpcSemaphoreId = c_int; /* semaphore ID returned by semget(2) */

/*
 * SEMAS_PER_SET is the number of useful semaphores in each semaphore set
 * we allocate.  It must be *less than* your kernel's SEMMSL (max semaphores
 * per set) parameter, which is often around 25.  (Less than, because we
 * allocate one extra sema in each set for identification purposes.)
 */
const SEMAS_PER_SET: c_int = 16;

const IPCProtection: c_int = 0o600; /* access/modify by user only */

const PGSemaMagic: c_int = 537; /* must be less than SEMVMX */

static mut sharedSemas: *mut PGSemaphoreDataImpl = null_mut(); /* array of PGSemaphoreData in shared memory */
static mut numSharedSemas: c_int = 0; /* number of PGSemaphoreDatas used so far */
static mut maxSharedSemas: c_int = 0; /* allocated size of PGSemaphoreData array */
static mut mySemaSets: *mut IpcSemaphoreId = null_mut(); /* IDs of sema sets acquired so far */
static mut numSemaSets: c_int = 0; /* number of sema sets acquired so far */
static mut maxSemaSets: c_int = 0; /* allocated size of mySemaSets array */
static mut nextSemaKey: IpcSemaphoreKey = 0; /* next key to try using */
static mut nextSemaNumber: c_int = 0; /* next free sem num in last sema set */

/*
 * InternalIpcSemaphoreCreate
 *
 * Attempt to create a new semaphore set with the specified key.
 * Will fail (return -1) if such a set already exists.
 *
 * If we fail with a failure code other than collision-with-existing-set,
 * print out an error and abort.  Other types of errors suggest nonrecoverable
 * problems.
 *
 * Unfortunately, it's sometimes hard to tell whether errors are
 * nonrecoverable.  Our caller keeps track of whether continuing to retry
 * is sane or not; if not, we abort on failure regardless of the errno.
 */
unsafe fn InternalIpcSemaphoreCreate(
    semKey: IpcSemaphoreKey,
    numSems: c_int,
    retry_ok: bool,
) -> IpcSemaphoreId {
    let semId: c_int = semget(semKey, numSems, IPC_CREAT | IPC_EXCL | IPCProtection);

    if semId < 0 {
        let saved_errno: c_int = errno();

        /*
         * Fail quietly if error suggests a collision with an existing set and
         * our caller has not lost patience.
         *
         * One would expect EEXIST, given that we said IPC_EXCL, but perhaps
         * we could get a permission violation instead.  On some platforms
         * EINVAL will be reported if the existing set has too few semaphores.
         * Also, EIDRM might occur if an old set is slated for destruction but
         * not gone yet.
         *
         * EINVAL is the key reason why we need the caller-level loop limit,
         * as it can also mean that the platform's SEMMSL is less than
         * numSems, and that condition can't be fixed by trying another key.
         */
        if retry_ok
            && (saved_errno == EEXIST
                || saved_errno == EACCES
                || saved_errno == EINVAL
                || saved_errno == EIDRM)
        {
            return -1;
        }

        /*
         * Else complain and abort.  (C also attaches errdetail with the
         * semget args and an ENOSPC errhint; folded into the message per the
         * port's single-message ereport! convention.)
         */
        ereport!(
            FATAL,
            errmsg!(
                "could not create semaphores: errno={}; failed system call was semget({}, {}, 0{:o}){}",
                saved_errno,
                semKey as c_ulong,
                numSems,
                IPC_CREAT | IPC_EXCL | IPCProtection,
                if saved_errno == ENOSPC {
                    "; This error does *not* mean that you have run out of disk space.  \
                     It occurs when either the system limit for the maximum number of \
                     semaphore sets (SEMMNI), or the system wide maximum number of \
                     semaphores (SEMMNS), would be exceeded.  You need to raise the \
                     respective kernel parameter.  Alternatively, reduce PostgreSQL's \
                     consumption of semaphores by reducing its \"max_connections\" parameter.\n\
                     The PostgreSQL documentation contains more information about \
                     configuring your system for PostgreSQL."
                } else {
                    ""
                }
            )
        );
        unreachable!();
    }

    semId
}

/*
 * Initialize a semaphore to the specified value.
 */
unsafe fn IpcSemaphoreInitialize(semId: IpcSemaphoreId, semNum: c_int, value: c_int) {
    let sem_arg = semun { val: value };

    if semctl(semId, semNum, SETVAL, sem_arg) < 0 {
        let saved_errno: c_int = errno();

        ereport!(
            FATAL,
            errmsg!(
                "semctl({}, {}, SETVAL, {}) failed: errno={}{}",
                semId,
                semNum,
                value,
                saved_errno,
                if saved_errno == EINVAL || saved_errno == 34 /* ERANGE */ {
                    "; You possibly need to raise your kernel's SEMVMX value.  \
                     Look into the PostgreSQL documentation for details."
                } else {
                    ""
                }
            )
        );
        unreachable!();
    }
}

/*
 * IpcSemaphoreKill(semId)	- removes a semaphore set
 */
unsafe fn IpcSemaphoreKill(semId: IpcSemaphoreId) {
    let sem_arg = semun { val: 0 }; /* unused, but keep compiler quiet */

    if semctl(semId, 0, IPC_RMID, sem_arg) < 0 {
        elog!(
            LOG,
            "semctl({}, 0, IPC_RMID, ...) failed: errno={}",
            semId,
            errno()
        );
    }
}

/* Get the current value (semval) of the semaphore */
unsafe fn IpcSemaphoreGetValue(semId: IpcSemaphoreId, semNum: c_int) -> c_int {
    let dummy = semun { val: 0 }; /* for Solaris */

    semctl(semId, semNum, GETVAL, dummy)
}

/* Get the PID of the last process to do semop() on the semaphore */
unsafe fn IpcSemaphoreGetLastPID(semId: IpcSemaphoreId, semNum: c_int) -> pid_t {
    let dummy = semun { val: 0 }; /* for Solaris */

    semctl(semId, semNum, GETPID, dummy) as pid_t
}

/*
 * Create a semaphore set with the given number of useful semaphores
 * (an additional sema is actually allocated to serve as identifier).
 * Dead Postgres sema sets are recycled if found, but we do not fail
 * upon collision with non-Postgres sema sets.
 *
 * The idea here is to detect and re-use keys that may have been assigned
 * by a crashed postmaster or backend.
 */
unsafe fn IpcSemaphoreCreate(numSems: c_int) -> IpcSemaphoreId {
    let mut num_tries: c_int = 0;
    let mut semId: IpcSemaphoreId;
    let sem_arg = semun { val: 0 };
    let mut mysema = PGSemaphoreDataImpl { semId: 0, semNum: 0 };

    /* Loop till we find a free IPC key */
    nextSemaKey += 1;
    loop {
        /*
         * Try to create new semaphore set.  Give up after trying 1000
         * distinct IPC keys.
         */
        semId = InternalIpcSemaphoreCreate(nextSemaKey, numSems + 1, num_tries < 1000);
        if semId >= 0 {
            break; /* successful create */
        }

        'next_key: {
            /* See if it looks to be leftover from a dead Postgres process */
            semId = semget(nextSemaKey, numSems + 1, 0);
            if semId < 0 {
                break 'next_key; /* failed: must be some other app's */
            }
            if IpcSemaphoreGetValue(semId, numSems) != PGSemaMagic {
                break 'next_key; /* sema belongs to a non-Postgres app */
            }

            /*
             * If the creator PID is my own PID or does not belong to any
             * extant process, it's safe to zap it.
             */
            let creatorPID: pid_t = IpcSemaphoreGetLastPID(semId, numSems);
            if creatorPID <= 0 {
                break 'next_key; /* oops, GETPID failed */
            }
            if creatorPID != getpid() {
                if kill(creatorPID, 0) == 0 || errno() != ESRCH {
                    break 'next_key; /* sema belongs to a live process */
                }
            }

            /*
             * The sema set appears to be from a dead Postgres process, or
             * from a previous cycle of life in this same process.  Zap it, if
             * possible.  This probably shouldn't fail, but if it does, assume
             * the sema set belongs to someone else after all, and continue
             * quietly.
             */
            if semctl(semId, 0, IPC_RMID, sem_arg) < 0 {
                break 'next_key;
            }

            /*
             * Now try again to create the sema set.
             */
            semId = InternalIpcSemaphoreCreate(nextSemaKey, numSems + 1, true);
            if semId >= 0 {
                /* successful create - exit the key-search loop below */
            }
        }
        if semId >= 0 {
            break;
        }

        /*
         * Can only get here if some other process managed to create the same
         * sema key before we did, or the leftover checks failed.  Loop around
         * to try next key.
         */
        nextSemaKey += 1;
        num_tries += 1;
    }

    /*
     * OK, we created a new sema set.  Mark it as created by this process. We
     * do this by setting the spare semaphore to PGSemaMagic-1 and then
     * incrementing it with semop().  That leaves it with value PGSemaMagic
     * and sempid referencing this process.
     */
    IpcSemaphoreInitialize(semId, numSems, PGSemaMagic - 1);
    mysema.semId = semId;
    mysema.semNum = numSems;
    PGSemaphoreUnlock(&raw mut mysema as PGSemaphore);

    semId
}

/*
 * Report amount of shared memory needed for semaphores
 */
pub unsafe fn PGSemaphoreShmemSize(maxSemas: c_int) -> Size {
    mul_size(
        maxSemas as Size,
        core::mem::size_of::<PGSemaphoreDataImpl>() as Size,
    )
}

/*
 * PGReserveSemaphores --- initialize semaphore support
 *
 * This is called during postmaster start or shared memory reinitialization.
 * It should do whatever is needed to be able to support up to maxSemas
 * subsequent PGSemaphoreCreate calls.  Also, if any system resources
 * are acquired here or in PGSemaphoreCreate, register an on_shmem_exit
 * callback to release them.
 *
 * In the SysV implementation, we acquire semaphore sets on-demand; the
 * maxSemas parameter is just used to size the arrays.  There is an array
 * of PGSemaphoreData structs in shared memory, and a postmaster-local array
 * with one entry per SysV semaphore set, which we use for releasing the
 * semaphore sets when done.  (This design ensures that postmaster shutdown
 * doesn't rely on the contents of shared memory, which a failed backend might
 * have clobbered.)
 */
pub unsafe fn PGReserveSemaphores(maxSemas: c_int) {
    let mut statbuf: stat_buf = core::mem::zeroed();

    /*
     * We use the data directory's inode number to seed the search for free
     * semaphore keys.  This minimizes the odds of collision with other
     * postmasters, while maximizing the odds that we will detect and clean up
     * semaphores left over from a crashed postmaster in our own directory.
     */
    if stat_inode64(DataDir, &raw mut statbuf) < 0 {
        ereport!(
            FATAL,
            errmsg!(
                "could not stat data directory \"{}\": errno={}",
                std::ffi::CStr::from_ptr(DataDir).to_string_lossy(),
                errno()
            )
        );
        unreachable!();
    }

    /*
     * We must use ShmemAllocUnlocked(), since the spinlock protecting
     * ShmemAlloc() won't be ready yet.
     */
    sharedSemas = ShmemAllocUnlocked(PGSemaphoreShmemSize(maxSemas)) as *mut PGSemaphoreDataImpl;
    numSharedSemas = 0;
    maxSharedSemas = maxSemas;

    maxSemaSets = (maxSemas + SEMAS_PER_SET - 1) / SEMAS_PER_SET;
    mySemaSets =
        malloc(maxSemaSets as usize * core::mem::size_of::<IpcSemaphoreId>()) as *mut IpcSemaphoreId;
    if mySemaSets.is_null() {
        elog!(PANIC, "out of memory");
    }
    numSemaSets = 0;
    nextSemaKey = statbuf.st_ino as IpcSemaphoreKey;
    nextSemaNumber = SEMAS_PER_SET; /* force sema set alloc on 1st call */

    on_shmem_exit(ReleaseSemaphores, 0 as Datum);
}

/*
 * Release semaphores at shutdown or shmem reinitialization
 *
 * (called as an on_shmem_exit callback, hence funny argument list)
 */
unsafe extern "C" fn ReleaseSemaphores(_status: c_int, _arg: Datum) {
    let mut i: c_int = 0;
    while i < numSemaSets {
        IpcSemaphoreKill(*mySemaSets.add(i as usize));
        i += 1;
    }
    free(mySemaSets as *mut c_void);
}

/*
 * PGSemaphoreCreate
 *
 * Allocate a PGSemaphore structure with initial count 1
 */
pub unsafe fn PGSemaphoreCreate() -> PGSemaphore {
    let sema: *mut PGSemaphoreDataImpl;

    /* Can't do this in a backend, because static state is postmaster's */
    Assert!(!IsUnderPostmaster);

    if nextSemaNumber >= SEMAS_PER_SET {
        /* Time to allocate another semaphore set */
        if numSemaSets >= maxSemaSets {
            elog!(PANIC, "too many semaphores created");
        }
        *mySemaSets.add(numSemaSets as usize) = IpcSemaphoreCreate(SEMAS_PER_SET);
        numSemaSets += 1;
        nextSemaNumber = 0;
    }
    /* Use the next shared PGSemaphoreData */
    if numSharedSemas >= maxSharedSemas {
        elog!(PANIC, "too many semaphores created");
    }
    sema = sharedSemas.add(numSharedSemas as usize);
    numSharedSemas += 1;
    /* Assign the next free semaphore in the current set */
    (*sema).semId = *mySemaSets.add((numSemaSets - 1) as usize);
    (*sema).semNum = nextSemaNumber;
    nextSemaNumber += 1;
    /* Initialize it to count 1 */
    IpcSemaphoreInitialize((*sema).semId, (*sema).semNum, 1);

    sema as PGSemaphore
}

/*
 * PGSemaphoreReset
 *
 * Reset a previously-initialized PGSemaphore to have count 0
 */
pub unsafe fn PGSemaphoreReset(sema: PGSemaphore) {
    let sema = sema as *mut PGSemaphoreDataImpl;
    IpcSemaphoreInitialize((*sema).semId, (*sema).semNum, 0);
}

/*
 * PGSemaphoreLock
 *
 * Lock a semaphore (decrement count), blocking if count would be < 0
 */
pub unsafe fn PGSemaphoreLock(sema: PGSemaphore) {
    let sema = sema as *mut PGSemaphoreDataImpl;
    let mut errStatus: c_int;
    let mut sops = sembuf {
        sem_num: (*sema).semNum as c_ushort,
        sem_op: -1, /* decrement */
        sem_flg: 0,
    };

    /*
     * Note: if errStatus is -1 and errno == EINTR then it means we returned
     * from the operation prematurely because we were sent a signal.  So we
     * try and lock the semaphore again.
     *
     * We used to check interrupts here, but that required servicing
     * interrupts directly from signal handlers. Which is hard to do safely
     * and portably.
     */
    loop {
        errStatus = semop((*sema).semId, &raw mut sops, 1);
        if !(errStatus < 0 && errno() == EINTR) {
            break;
        }
    }

    if errStatus < 0 {
        elog!(
            FATAL,
            "semop(id={}) failed: errno={}",
            (*sema).semId,
            errno()
        );
    }
}

/*
 * PGSemaphoreUnlock
 *
 * Unlock a semaphore (increment count)
 */
pub unsafe fn PGSemaphoreUnlock(sema: PGSemaphore) {
    let sema = sema as *mut PGSemaphoreDataImpl;
    let mut errStatus: c_int;
    let mut sops = sembuf {
        sem_num: (*sema).semNum as c_ushort,
        sem_op: 1, /* increment */
        sem_flg: 0,
    };

    /*
     * Note: if errStatus is -1 and errno == EINTR then it means we returned
     * from the operation prematurely because we were sent a signal.  So we
     * try and unlock the semaphore again. Not clear this can really happen,
     * but might as well cope.
     */
    loop {
        errStatus = semop((*sema).semId, &raw mut sops, 1);
        if !(errStatus < 0 && errno() == EINTR) {
            break;
        }
    }

    if errStatus < 0 {
        elog!(
            FATAL,
            "semop(id={}) failed: errno={}",
            (*sema).semId,
            errno()
        );
    }
}

/*
 * PGSemaphoreTryLock
 *
 * Lock a semaphore only if able to do so without blocking
 */
pub unsafe fn PGSemaphoreTryLock(sema: PGSemaphore) -> bool {
    let sema = sema as *mut PGSemaphoreDataImpl;
    let mut errStatus: c_int;
    let mut sops = sembuf {
        sem_num: (*sema).semNum as c_ushort,
        sem_op: -1,            /* decrement */
        sem_flg: IPC_NOWAIT as c_short, /* but don't block */
    };

    /*
     * Note: if errStatus is -1 and errno == EINTR then it means we returned
     * from the operation prematurely because we were sent a signal.  So we
     * try and lock the semaphore again.
     */
    loop {
        errStatus = semop((*sema).semId, &raw mut sops, 1);
        if !(errStatus < 0 && errno() == EINTR) {
            break;
        }
    }

    if errStatus < 0 {
        /* Expect EAGAIN or EWOULDBLOCK (platform-dependent; same on Darwin) */
        if errno() == EAGAIN {
            return false; /* failed to lock it */
        }
        /* Otherwise we got trouble */
        elog!(
            FATAL,
            "semop(id={}) failed: errno={}",
            (*sema).semId,
            errno()
        );
    }

    true
}

#[cfg(test)]
mod tests {
    use super::*;

    // The shmem-backed create path needs a running postmaster environment;
    // exercise the pure helpers and constants instead.
    #[test]
    fn semas_per_set_below_typical_semmsl() {
        assert!(SEMAS_PER_SET < 25);
        assert!(PGSemaMagic < 32767); // must be less than SEMVMX
    }

    #[test]
    fn shmem_size_scales_linearly() {
        unsafe {
            let one = PGSemaphoreShmemSize(1);
            let ten = PGSemaphoreShmemSize(10);
            assert_eq!(one, core::mem::size_of::<PGSemaphoreDataImpl>());
            assert_eq!(ten, 10 * one);
        }
    }
}
