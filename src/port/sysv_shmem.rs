//! sysv_shmem.rs
//!   Implement shared memory using SysV facilities
//!
//! These routines used to be a fairly thin layer on top of SysV shared
//! memory functionality.  With the addition of anonymous-shmem logic,
//! they're a bit fatter now.  We still require a SysV shmem block to
//! exist, though, because mmap'd shmem provides no way to find out how
//! many processes are attached, which we need for interlocking purposes.
//!
//! Translated 1:1 from postgres/src/backend/port/sysv_shmem.c
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! #include mapping:
//!   "postgres.h"            -> crate::prelude::*
//!   <sys/ipc.h>/<sys/shm.h>/<sys/mman.h>/<sys/stat.h> -> libc externs below
//!   "miscadmin.h"           -> crate::utils::init::globals (DataDir,
//!                              IsUnderPostmaster) + crate::miscadmin
//!                              (AddToDataDirLockFile)
//!   "portability/mem.h"     -> PG_SHMAT_FLAGS / PG_MMAP_FLAGS consts below
//!   "storage/dsm.h"         -> dsm_cleanup_using_control_segment (STUB below)
//!   "storage/ipc.h"         -> crate::storage::ipc::ipc (on_shmem_exit)
//!   "storage/pg_shmem.h"    -> crate::storage::pg_shmem (PGShmemHeader etc)
//!   "utils/guc.h"/"guc_hooks.h" -> huge_pages/huge_page_size GUC vars +
//!                              SetConfigOption (STUBS below)
//!   "utils/pidfile.h"       -> crate::utils::pidfile (LOCK_FILE_LINE_SHMEM_KEY)
//!
//! Platform notes: this port targets macOS/Darwin, non-EXEC_BACKEND (the
//! codebase-wide convention).  Therefore:
//!  - the EXEC_BACKEND-only PGSharedMemoryReAttach/PGSharedMemoryNoReAttach
//!    are omitted (C compiles them out via #ifdef EXEC_BACKEND);
//!  - MAP_HUGETLB does not exist on Darwin, so GetHugePageSize /
//!    check_huge_page_size / CreateAnonymousSegment take the C's
//!    !MAP_HUGETLB branches (the Linux /proc/meminfo + MAP_HUGE_* logic is
//!    summarized in comments at the relevant spots).

use crate::prelude::*;

use crate::miscadmin::AddToDataDirLockFile;
use crate::storage::ipc::ipc::on_shmem_exit;
use crate::storage::ipc::ipci::shared_memory_type;
use crate::storage::pg_shmem::{
    dev_t, ino_t, pid_t, PGShmemHeader, PGShmemMagic, HUGE_PAGES_ON, HUGE_PAGES_TRY,
    SHMEM_TYPE_MMAP,
};
use crate::utils::init::globals::{DataDir, IsUnderPostmaster};
use crate::utils::pidfile::LOCK_FILE_LINE_SHMEM_KEY;
use crate::{elog, ereport, errmsg, Assert};

use std::ffi::{c_char, c_int, c_ulong, c_void};
use std::ptr::{null, null_mut};

// ---------------------------------------------------------------------------
// GUC variables and helpers (utils/misc/guc_tables.c not yet ported)
// ---------------------------------------------------------------------------

/// TODO(pg-port): real huge_pages / huge_page_size GUC variables live in
/// utils/misc/guc_tables.c.  Defaults per guc_tables: huge_pages=TRY, size=0.
pub static mut huge_pages: c_int = HUGE_PAGES_TRY;
pub static mut huge_page_size: c_int = 0;

/// TODO(pg-port): real SetConfigOption lives in utils/misc/guc.c.  No-op shim.
unsafe fn SetConfigOption(
    _name: *const c_char,
    _value: *const c_char,
    _context: c_int,
    _source: c_int,
) {
}
const PGC_INTERNAL: c_int = 0;
const PGC_S_DYNAMIC_DEFAULT: c_int = 1;

/// TODO(pg-port): real dsm_cleanup_using_control_segment lives in
/// storage/ipc/dsm.c (not yet wired as a full module).
unsafe fn dsm_cleanup_using_control_segment(_handle: u32) {}

// ---------------------------------------------------------------------------
// libc bindings (SysV shm, mmap, stat).  Darwin: errno via __error().
// ---------------------------------------------------------------------------

pub type key_t = i32;

/// Darwin struct __ipc_perm_new (sys/ipc.h)
#[repr(C)]
struct ipc_perm {
    uid: u32,
    gid: u32,
    cuid: u32,
    cgid: u32,
    mode: u16,
    _seq: u16,
    _key: key_t,
}

/// Darwin struct __shmid_ds_new (sys/shm.h); shmatt_t = unsigned short.
#[repr(C)]
struct shmid_ds {
    shm_perm: ipc_perm,        /* operation permission structure */
    shm_segsz: usize,          /* size of segment in bytes */
    shm_lpid: pid_t,           /* PID of last shared memory op */
    shm_cpid: pid_t,           /* PID of creator */
    shm_nattch: u16,           /* number of current attaches */
    shm_atime: i64,            /* time of last shmat() */
    shm_dtime: i64,            /* time of last shmdt() */
    shm_ctime: i64,            /* time of last change by shmctl() */
    shm_internal: *mut c_void, /* reserved for kernel use */
}

/// Minimal Darwin struct stat - we need st_dev (offset 0) and st_ino
/// (offset 8); see sibling sysv_sema.rs.
#[repr(C)]
struct stat_buf {
    st_dev: i32,
    st_mode: u16,
    st_nlink: u16,
    st_ino: u64,
    _rest: [u8; 128],
}

extern "C" {
    fn shmget(key: key_t, size: usize, shmflg: c_int) -> c_int;
    fn shmat(shmid: c_int, shmaddr: *const c_void, shmflg: c_int) -> *mut c_void;
    fn shmdt(shmaddr: *const c_void) -> c_int;
    fn shmctl(shmid: c_int, cmd: c_int, buf: *mut shmid_ds) -> c_int;
    fn mmap(
        addr: *mut c_void,
        len: usize,
        prot: c_int,
        flags: c_int,
        fd: c_int,
        offset: i64,
    ) -> *mut c_void;
    fn munmap(addr: *mut c_void, len: usize) -> c_int;
    fn getpid() -> pid_t;
    fn memcpy(dst: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
    #[link_name = "stat$INODE64"]
    fn stat_inode64(path: *const c_char, buf: *mut stat_buf) -> c_int;
    /// errno access (thread-local).  macOS/Darwin uses __error().
    fn __error() -> *mut c_int;
}

#[inline]
unsafe fn errno() -> c_int {
    *__error()
}
#[inline]
unsafe fn errno_set(v: c_int) {
    *__error() = v;
}

// errno constants (Darwin/BSD values).
const EACCES: c_int = 13;
const EEXIST: c_int = 17;
const EINVAL: c_int = 22;
const ENOMEM: c_int = 12;
const ENOSPC: c_int = 28;
const EIDRM: c_int = 90;

// sys/ipc.h / sys/shm.h constants (Darwin values).
const IPC_CREAT: c_int = 0o001000;
const IPC_EXCL: c_int = 0o002000;
const IPC_RMID: c_int = 0;
const IPC_STAT: c_int = 2;
const IPCProtection: c_int = 0o600; /* access/modify by user only */

// sys/mman.h constants (Darwin values).
const PROT_READ: c_int = 0x01;
const PROT_WRITE: c_int = 0x02;
const MAP_SHARED: c_int = 0x0001;
const MAP_ANONYMOUS: c_int = 0x1000; /* Darwin MAP_ANON */
const MAP_HASSEMAPHORE: c_int = 0x0200;
const MAP_FAILED: *mut c_void = (-1isize) as *mut c_void;

// portability/mem.h
const PG_SHMAT_FLAGS: c_int = 0;
const PG_MMAP_FLAGS: c_int = MAP_SHARED | MAP_ANONYMOUS | MAP_HASSEMAPHORE;

// ---------------------------------------------------------------------------
// File-local types and state (1:1 with the C)
// ---------------------------------------------------------------------------

type IpcMemoryKey = key_t; /* shared memory key passed to shmget(2) */
type IpcMemoryId = c_int; /* shared memory ID returned by shmget(2) */

/*
 * How does a given IpcMemoryId relate to this PostgreSQL process?
 *
 * One could recycle unattached segments of different data directories if we
 * distinguished that case from other SHMSTATE_FOREIGN cases.  Doing so would
 * cause us to visit less of the key space, making us less likely to detect a
 * SHMSTATE_ATTACHED key.  It would also complicate the concurrency analysis,
 * in that postmasters of different data directories could simultaneously
 * attempt to recycle a given key.  We'll waste keys longer in some cases, but
 * avoiding the problems of the alternative justifies that loss.
 */
#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(C)]
enum IpcMemoryState {
    SHMSTATE_ANALYSIS_FAILURE, /* unexpected failure to analyze the ID */
    SHMSTATE_ATTACHED,         /* pertinent to DataDir, has attached PIDs */
    SHMSTATE_ENOENT,           /* no segment of that ID */
    SHMSTATE_FOREIGN,          /* exists, but not pertinent to DataDir */
    SHMSTATE_UNATTACHED,       /* pertinent to DataDir, no attached PIDs */
}
use IpcMemoryState::*;

pub static mut UsedShmemSegID: c_ulong = 0;
pub static mut UsedShmemSegAddr: *mut c_void = null_mut();

static mut AnonymousShmemSize: Size = 0;
static mut AnonymousShmem: *mut c_void = null_mut();

/*
 *	InternalIpcMemoryCreate(memKey, size)
 *
 * Attempt to create a new shared memory segment with the specified key.
 * Will fail (return NULL) if such a segment already exists.  If successful,
 * attach the segment to the current process and return its attached address.
 * On success, callbacks are registered with on_shmem_exit to detach and
 * delete the segment when on_shmem_exit is called.
 *
 * If we fail with a failure code other than collision-with-existing-segment,
 * print out an error and abort.  Other types of errors are not recoverable.
 */
unsafe fn InternalIpcMemoryCreate(memKey: IpcMemoryKey, size: Size) -> *mut c_void {
    let mut shmid: IpcMemoryId;
    let requestedAddress: *mut c_void = null_mut();
    let memAddress: *mut c_void;

    /*
     * Normally we just pass requestedAddress = NULL to shmat(), allowing the
     * system to choose where the segment gets mapped.  (The EXEC_BACKEND
     * PG_SHMEM_ADDR escape hatch is compiled out in this port.)
     */

    shmid = shmget(memKey, size, IPC_CREAT | IPC_EXCL | IPCProtection);

    if shmid < 0 {
        let shmget_errno: c_int = errno();

        /*
         * Fail quietly if error indicates a collision with existing segment.
         * One would expect EEXIST, given that we said IPC_EXCL, but perhaps
         * we could get a permission violation instead?  Also, EIDRM might
         * occur if an old seg is slated for destruction but not gone yet.
         */
        if shmget_errno == EEXIST || shmget_errno == EACCES || shmget_errno == EIDRM {
            return null_mut();
        }

        /*
         * Some BSD-derived kernels are known to return EINVAL, not EEXIST, if
         * there is an existing segment but it's smaller than "size" (this is
         * a result of poorly-thought-out ordering of error tests). To
         * distinguish between collision and invalid size in such cases, we
         * make a second try with size = 0.  These kernels do not test size
         * against SHMMIN in the preexisting-segment case, so we will not get
         * EINVAL a second time if there is such a segment.
         */
        if shmget_errno == EINVAL {
            shmid = shmget(memKey, 0, IPC_CREAT | IPC_EXCL | IPCProtection);

            if shmid < 0 {
                /* As above, fail quietly if we verify a collision */
                if errno() == EEXIST || errno() == EACCES || errno() == EIDRM {
                    return null_mut();
                }
                /* Otherwise, fall through to report the original error */
            } else {
                /*
                 * On most platforms we cannot get here because SHMMIN is
                 * greater than zero.  However, if we do succeed in creating a
                 * zero-size segment, free it and then fall through to report
                 * the original error.
                 */
                if shmctl(shmid, IPC_RMID, null_mut()) < 0 {
                    elog!(
                        LOG,
                        "shmctl({}, {}, 0) failed: errno={}",
                        shmid,
                        IPC_RMID,
                        errno()
                    );
                }
            }
        }

        /*
         * Else complain and abort.
         *
         * Note: at this point EINVAL should mean that either SHMMIN or SHMMAX
         * is violated.  SHMALL violation might be reported as either ENOMEM
         * (BSDen) or ENOSPC (Linux); the Single Unix Spec fails to say which
         * it should be.  SHMMNI violation is ENOSPC, per spec.  Just plain
         * not-enough-RAM is ENOMEM.  (C attaches errdetail/errhint per errno;
         * folded into the message per the port's single-message ereport!.)
         */
        errno_set(shmget_errno);
        ereport!(
            FATAL,
            errmsg!(
                "could not create shared memory segment: errno={}; failed system call was shmget(key={}, size={}, 0{:o}){}",
                shmget_errno,
                memKey as c_ulong,
                size,
                IPC_CREAT | IPC_EXCL | IPCProtection,
                match shmget_errno {
                    e if e == EINVAL =>
                        "; This error usually means that PostgreSQL's request for a shared memory \
                         segment exceeded your kernel's SHMMAX parameter, or possibly that it is \
                         less than your kernel's SHMMIN parameter.",
                    e if e == ENOMEM =>
                        "; This error usually means that PostgreSQL's request for a shared memory \
                         segment exceeded your kernel's SHMALL parameter.  You might need to \
                         reconfigure the kernel with larger SHMALL.",
                    e if e == ENOSPC =>
                        "; This error does *not* mean that you have run out of disk space.  It \
                         occurs either if all available shared memory IDs have been taken, in \
                         which case you need to raise the SHMMNI parameter in your kernel, or \
                         because the system's overall limit for shared memory has been reached.",
                    _ => "",
                }
            )
        );
        unreachable!();
    }

    /* Register on-exit routine to delete the new segment */
    on_shmem_exit(IpcMemoryDelete, shmid as Datum);

    /* OK, should be able to attach to the segment */
    memAddress = shmat(shmid, requestedAddress, PG_SHMAT_FLAGS);

    if memAddress == MAP_FAILED {
        elog!(
            FATAL,
            "shmat(id={}, addr={:p}, flags=0x{:x}) failed: errno={}",
            shmid,
            requestedAddress,
            PG_SHMAT_FLAGS,
            errno()
        );
    }

    /* Register on-exit routine to detach new segment before deleting */
    on_shmem_exit(IpcMemoryDetach, memAddress as Datum);

    /*
     * Store shmem key and ID in data directory lockfile.  Format to try to
     * keep it the same length always (trailing junk in the lockfile won't
     * hurt, but might confuse humans).
     */
    {
        let line = format!("{:9} {:9}\0", memKey as c_ulong, shmid as c_ulong);
        AddToDataDirLockFile(LOCK_FILE_LINE_SHMEM_KEY, line.as_ptr() as *const c_char);
    }

    memAddress
}

/****************************************************************************/
/*	IpcMemoryDetach(status, shmaddr)	removes a shared memory segment		*/
/*										from process' address space			*/
/*	(called as an on_shmem_exit callback, hence funny argument list)		*/
/****************************************************************************/
unsafe extern "C" fn IpcMemoryDetach(_status: c_int, shmaddr: Datum) {
    /* Detach System V shared memory block. */
    if shmdt(shmaddr as *const c_void) < 0 {
        elog!(
            LOG,
            "shmdt({:p}) failed: errno={}",
            shmaddr as *const c_void,
            errno()
        );
    }
}

/****************************************************************************/
/*	IpcMemoryDelete(status, shmId)		deletes a shared memory segment		*/
/*	(called as an on_shmem_exit callback, hence funny argument list)		*/
/****************************************************************************/
unsafe extern "C" fn IpcMemoryDelete(_status: c_int, shmId: Datum) {
    if shmctl(shmId as c_int, IPC_RMID, null_mut()) < 0 {
        elog!(
            LOG,
            "shmctl({}, {}, 0) failed: errno={}",
            shmId as c_int,
            IPC_RMID,
            errno()
        );
    }
}

/*
 * PGSharedMemoryIsInUse
 *
 * Is a previously-existing shmem segment still existing and in use?
 *
 * The point of this exercise is to detect the case where a prior postmaster
 * crashed, but it left child backends that are still running.  Therefore
 * we only care about shmem segments that are associated with the intended
 * DataDir.  This is an important consideration since accidental matches of
 * shmem segment IDs are reasonably common.
 */
pub unsafe fn PGSharedMemoryIsInUse(_id1: c_ulong, id2: c_ulong) -> bool {
    let mut memAddress: *mut PGShmemHeader = null_mut();

    let state = PGSharedMemoryAttach(id2 as IpcMemoryId, null_mut(), &raw mut memAddress);
    if !memAddress.is_null() && shmdt(memAddress as *const c_void) < 0 {
        elog!(LOG, "shmdt({:p}) failed: errno={}", memAddress, errno());
    }
    match state {
        SHMSTATE_ENOENT | SHMSTATE_FOREIGN | SHMSTATE_UNATTACHED => false,
        SHMSTATE_ANALYSIS_FAILURE | SHMSTATE_ATTACHED => true,
    }
}

/*
 * Test for a segment with id shmId; see comment at IpcMemoryState.
 *
 * If the segment exists, we'll attempt to attach to it, using attachAt
 * if that's not NULL (but it's best to pass NULL if possible).
 *
 * *addr is set to the segment memory address if we attached to it, else NULL.
 */
unsafe fn PGSharedMemoryAttach(
    shmId: IpcMemoryId,
    attachAt: *mut c_void,
    addr: *mut *mut PGShmemHeader,
) -> IpcMemoryState {
    let mut shmStat: shmid_ds = core::mem::zeroed();
    let mut statbuf: stat_buf = core::mem::zeroed();
    let hdr: *mut PGShmemHeader;

    *addr = null_mut();

    /*
     * First, try to stat the shm segment ID, to see if it exists at all.
     */
    if shmctl(shmId, IPC_STAT, &raw mut shmStat) < 0 {
        /*
         * EINVAL actually has multiple possible causes documented in the
         * shmctl man page, but we assume it must mean the segment no longer
         * exists.
         */
        if errno() == EINVAL {
            return SHMSTATE_ENOENT;
        }

        /*
         * EACCES implies we have no read permission, which means it is not a
         * Postgres shmem segment (or at least, not one that is relevant to
         * our data directory).
         */
        if errno() == EACCES {
            return SHMSTATE_FOREIGN;
        }

        /*
         * (HAVE_LINUX_EIDRM_BUG handling compiled out: not Linux.)
         *
         * Otherwise, we had better assume that the segment is in use.  The
         * only likely case is (non-Linux, assumed spec-compliant) EIDRM,
         * which implies that the segment has been IPC_RMID'd but there are
         * still processes attached to it.
         */
        return SHMSTATE_ANALYSIS_FAILURE;
    }

    /*
     * Try to attach to the segment and see if it matches our data directory.
     * This avoids any risk of duplicate-shmem-key conflicts on machines that
     * are running several postmasters under the same userid.
     *
     * (When we're called from PGSharedMemoryCreate, this stat call is
     * duplicative; but since this isn't a high-traffic case it's not worth
     * trying to optimize.)
     */
    if stat_inode64(DataDir, &raw mut statbuf) < 0 {
        return SHMSTATE_ANALYSIS_FAILURE; /* can't stat; be conservative */
    }

    hdr = shmat(shmId, attachAt, PG_SHMAT_FLAGS) as *mut PGShmemHeader;
    if hdr == MAP_FAILED as *mut PGShmemHeader {
        /*
         * Attachment failed.  The cases we're interested in are the same as
         * for the shmctl() call above.  In particular, note that the owning
         * postmaster could have terminated and removed the segment between
         * shmctl() and shmat().
         *
         * If attachAt isn't NULL, it's possible that EINVAL reflects a
         * problem with that address not a vanished segment, so it's best to
         * pass NULL when probing for conflicting segments.
         */
        if errno() == EINVAL {
            return SHMSTATE_ENOENT; /* segment disappeared */
        }
        if errno() == EACCES {
            return SHMSTATE_FOREIGN; /* must be non-Postgres */
        }
        /* Otherwise, be conservative. */
        return SHMSTATE_ANALYSIS_FAILURE;
    }
    *addr = hdr;

    if (*hdr).magic != PGShmemMagic
        || (*hdr).device != statbuf.st_dev as dev_t
        || (*hdr).inode != statbuf.st_ino as ino_t
    {
        /*
         * It's either not a Postgres segment, or not one for my data
         * directory.
         */
        return SHMSTATE_FOREIGN;
    }

    /*
     * It does match our data directory, so now test whether any processes are
     * still attached to it.  (We are, now, but the shm_nattch result is from
     * before we attached to it.)
     */
    if shmStat.shm_nattch == 0 {
        SHMSTATE_UNATTACHED
    } else {
        SHMSTATE_ATTACHED
    }
}

/*
 * Identify the huge page size to use, and compute the related mmap flags.
 *
 * (See the C source for the full Linux MAP_HUGETLB discussion: kernel bugs
 * around non-multiple-of-hugepagesize requests, /proc/meminfo parsing for
 * the default size, and MAP_HUGE_MASK/MAP_HUGE_SHIFT encoding of explicit
 * page sizes.)
 *
 * Returns the (real, assumed or config provided) page size into
 * *hugepagesize, and the hugepage-related mmap flags to use into
 * *mmap_flags if requested by the caller.  If huge pages are not supported,
 * *hugepagesize and *mmap_flags are set to 0.
 *
 * Darwin has no MAP_HUGETLB, so this takes the C's #else branch.
 */
pub unsafe fn GetHugePageSize(hugepagesize: *mut Size, mmap_flags: *mut c_int) {
    if !hugepagesize.is_null() {
        *hugepagesize = 0;
    }
    if !mmap_flags.is_null() {
        *mmap_flags = 0;
    }
}

/*
 * GUC check_hook for huge_page_size
 *
 * Without MAP_HUGE_MASK/MAP_HUGE_SHIFT (recent-enough Linux only), a nonzero
 * setting is rejected.
 */
pub unsafe fn check_huge_page_size(newval: *mut c_int, _extra: *mut *mut c_void, _source: c_int) -> bool {
    if *newval != 0 {
        // GUC_check_errdetail("\"huge_page_size\" must be 0 on this platform.");
        return false;
    }
    true
}

/*
 * Creates an anonymous mmap()ed shared memory segment.
 *
 * Pass the requested size in *size.  This function will modify *size to the
 * actual size of the allocation, if it ends up allocating a segment that is
 * larger than requested.
 */
unsafe fn CreateAnonymousSegment(size: *mut Size) -> *mut c_void {
    let allocsize: Size = *size;
    let mut ptr: *mut c_void = MAP_FAILED;
    let mut mmap_errno: c_int = 0;

    /* No MAP_HUGETLB on this platform: PGSharedMemoryCreate should have
     * dealt with the HUGE_PAGES_ON case already. */
    Assert!(huge_pages != HUGE_PAGES_ON);

    /*
     * Report whether huge pages are in use.  This needs to be tracked before
     * the second mmap() call if attempting to use huge pages failed
     * previously.
     */
    SetConfigOption(
        c"huge_pages_status".as_ptr(),
        if ptr == MAP_FAILED { c"off".as_ptr() } else { c"on".as_ptr() },
        PGC_INTERNAL,
        PGC_S_DYNAMIC_DEFAULT,
    );

    if ptr == MAP_FAILED && huge_pages != HUGE_PAGES_ON {
        /*
         * Use the original size, not the rounded-up value, when falling back
         * to non-huge pages.
         */
        ptr = mmap(
            null_mut(),
            allocsize,
            PROT_READ | PROT_WRITE,
            PG_MMAP_FLAGS,
            -1,
            0,
        );
        mmap_errno = errno();
    }

    if ptr == MAP_FAILED {
        errno_set(mmap_errno);
        ereport!(
            FATAL,
            errmsg!(
                "could not map anonymous shared memory: errno={}{}",
                mmap_errno,
                if mmap_errno == ENOMEM {
                    format!(
                        "; This error usually means that PostgreSQL's request for a shared \
                         memory segment exceeded available memory, swap space, or huge pages. \
                         To reduce the request size (currently {} bytes), reduce PostgreSQL's \
                         shared memory usage, perhaps by reducing \"shared_buffers\" or \
                         \"max_connections\".",
                        allocsize
                    )
                } else {
                    String::new()
                }
            )
        );
        unreachable!();
    }

    *size = allocsize;
    ptr
}

/*
 * AnonymousShmemDetach --- detach from an anonymous mmap'd block
 * (called as an on_shmem_exit callback, hence funny argument list)
 */
unsafe extern "C" fn AnonymousShmemDetach(_status: c_int, _arg: Datum) {
    /* Release anonymous shared memory block, if any. */
    if !AnonymousShmem.is_null() {
        if munmap(AnonymousShmem, AnonymousShmemSize) < 0 {
            elog!(
                LOG,
                "munmap({:p}, {}) failed: errno={}",
                AnonymousShmem,
                AnonymousShmemSize,
                errno()
            );
        }
        AnonymousShmem = null_mut();
    }
}

/*
 * PGSharedMemoryCreate
 *
 * Create a shared memory segment of the given size and initialize its
 * standard header.  Also, register an on_shmem_exit callback to release
 * the storage.
 *
 * Dead Postgres segments pertinent to this DataDir are recycled if found, but
 * we do not fail upon collision with foreign shmem segments.  The idea here
 * is to detect and re-use keys that may have been assigned by a crashed
 * postmaster or backend.
 */
pub unsafe fn PGSharedMemoryCreate(
    mut size: Size,
    shim: *mut *mut PGShmemHeader,
) -> *mut PGShmemHeader {
    let mut NextShmemSegID: IpcMemoryKey;
    let mut memAddress: *mut c_void;
    let hdr: *mut PGShmemHeader;
    let mut statbuf: stat_buf = core::mem::zeroed();
    let sysvsize: Size;

    /*
     * We use the data directory's ID info (inode and device numbers) to
     * positively identify shmem segments associated with this data dir, and
     * also as seeds for searching for a free shmem key.
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

    /* Complain if hugepages demanded but we can't possibly support them
     * (no MAP_HUGETLB on this platform). */
    if huge_pages == HUGE_PAGES_ON {
        ereport!(
            ERROR,
            errmsg!("huge pages not supported on this platform")
        );
        unreachable!();
    }

    /* For now, we don't support huge pages in SysV memory */
    if huge_pages == HUGE_PAGES_ON && shared_memory_type != SHMEM_TYPE_MMAP {
        ereport!(
            ERROR,
            errmsg!("huge pages not supported with the current \"shared_memory_type\" setting")
        );
        unreachable!();
    }

    /* Room for a header? */
    Assert!(size > crate::c::MAXALIGN(core::mem::size_of::<PGShmemHeader>()) as Size);

    // bring-up: shared_memory_type enum GUC isn't applied (no-op hooks), leaving it != MMAP;
    // macOS SHMMAX is tiny so the big segment MUST be mmap. Force it. TODO: real GUC enum assign.
    shared_memory_type = SHMEM_TYPE_MMAP;
    if shared_memory_type == SHMEM_TYPE_MMAP {
        AnonymousShmem = CreateAnonymousSegment(&raw mut size);
        AnonymousShmemSize = size;

        /* Register on-exit routine to unmap the anonymous segment */
        on_shmem_exit(AnonymousShmemDetach, 0 as Datum);

        /* Now we need only allocate a minimal-sized SysV shmem block. */
        sysvsize = core::mem::size_of::<PGShmemHeader>() as Size;
    } else {
        sysvsize = size;

        /* huge pages are only available with mmap */
        SetConfigOption(
            c"huge_pages_status".as_ptr(),
            c"off".as_ptr(),
            PGC_INTERNAL,
            PGC_S_DYNAMIC_DEFAULT,
        );
    }

    /*
     * Loop till we find a free IPC key.  Trust CreateDataDirLockFile() to
     * ensure no more than one postmaster per data directory can enter this
     * loop simultaneously.  (CreateDataDirLockFile() does not entirely ensure
     * that, but prefer fixing it over coping here.)
     */
    NextShmemSegID = statbuf.st_ino as IpcMemoryKey;

    loop {
        let shmid: IpcMemoryId;
        let mut oldhdr: *mut PGShmemHeader = null_mut();
        let state: IpcMemoryState;

        /* Try to create new segment */
        memAddress = InternalIpcMemoryCreate(NextShmemSegID, sysvsize);
        if !memAddress.is_null() {
            break; /* successful create and attach */
        }

        /* Check shared memory and possibly remove and recreate */

        /*
         * shmget() failure is typically EACCES, hence SHMSTATE_FOREIGN.
         * ENOENT, a narrow possibility, implies SHMSTATE_ENOENT, but one can
         * safely treat SHMSTATE_ENOENT like SHMSTATE_FOREIGN.
         */
        shmid = shmget(
            NextShmemSegID,
            core::mem::size_of::<PGShmemHeader>(),
            0,
        );
        if shmid < 0 {
            oldhdr = null_mut();
            state = SHMSTATE_FOREIGN;
        } else {
            state = PGSharedMemoryAttach(shmid, null_mut(), &raw mut oldhdr);
        }

        match state {
            SHMSTATE_ANALYSIS_FAILURE | SHMSTATE_ATTACHED => {
                ereport!(
                    FATAL,
                    errmsg!(
                        "pre-existing shared memory block (key {}, ID {}) is still in use; \
                         Terminate any old server processes associated with data directory \"{}\".",
                        NextShmemSegID as c_ulong,
                        shmid as c_ulong,
                        std::ffi::CStr::from_ptr(DataDir).to_string_lossy()
                    )
                );
                unreachable!();
            }
            SHMSTATE_ENOENT => {
                /*
                 * To our surprise, some other process deleted since our last
                 * InternalIpcMemoryCreate().  Moments earlier, we would have
                 * seen SHMSTATE_FOREIGN.  Try that same ID again.
                 */
                elog!(
                    LOG,
                    "shared memory block (key {}, ID {}) deleted during startup",
                    NextShmemSegID as c_ulong,
                    shmid as c_ulong
                );
            }
            SHMSTATE_FOREIGN => {
                NextShmemSegID += 1;
            }
            SHMSTATE_UNATTACHED => {
                /*
                 * The segment pertains to DataDir, and every process that had
                 * used it has died or detached.  Zap it, if possible, and any
                 * associated dynamic shared memory segments, as well.  This
                 * shouldn't fail, but if it does, assume the segment belongs
                 * to someone else after all, and try the next candidate.
                 * Otherwise, try again to create the segment.  That may fail
                 * if some other process creates the same shmem key before we
                 * do, in which case we'll try the next key.
                 */
                if (*oldhdr).dsm_control != 0 {
                    dsm_cleanup_using_control_segment((*oldhdr).dsm_control);
                }
                if shmctl(shmid, IPC_RMID, null_mut()) < 0 {
                    NextShmemSegID += 1;
                }
            }
        }

        if !oldhdr.is_null() && shmdt(oldhdr as *const c_void) < 0 {
            elog!(LOG, "shmdt({:p}) failed: errno={}", oldhdr, errno());
        }
    }

    /* Initialize new segment. */
    hdr = memAddress as *mut PGShmemHeader;
    (*hdr).creatorPID = getpid();
    (*hdr).magic = PGShmemMagic;
    (*hdr).dsm_control = 0;

    /* Fill in the data directory ID info, too */
    (*hdr).device = statbuf.st_dev as dev_t;
    (*hdr).inode = statbuf.st_ino as ino_t;

    /*
     * Initialize space allocation status for segment.
     */
    (*hdr).totalsize = size;
    (*hdr).freeoffset = crate::c::MAXALIGN(core::mem::size_of::<PGShmemHeader>()) as Size;
    *shim = hdr;

    /* Save info for possible future use */
    UsedShmemSegAddr = memAddress;
    UsedShmemSegID = NextShmemSegID as c_ulong;

    /*
     * If AnonymousShmem is NULL here, then we're not using anonymous shared
     * memory, and should return a pointer to the System V shared memory
     * block. Otherwise, the System V shared memory block is only a shim, and
     * we must return a pointer to the real block.
     */
    if AnonymousShmem.is_null() {
        return hdr;
    }
    memcpy(
        AnonymousShmem,
        hdr as *const c_void,
        core::mem::size_of::<PGShmemHeader>(),
    );
    AnonymousShmem as *mut PGShmemHeader
}

/*
 * (EXEC_BACKEND-only PGSharedMemoryReAttach / PGSharedMemoryNoReAttach are
 * compiled out in C via #ifdef EXEC_BACKEND; this port models the
 * non-EXEC_BACKEND build, matching the rest of the codebase.)
 */
const _: () = {
    // Reference IsUnderPostmaster so the import mirrors the C file's usage
    // even with the EXEC_BACKEND-only functions omitted.
    let _ = &raw const IsUnderPostmaster;
};

/*
 * PGSharedMemoryDetach
 *
 * Detach from the shared memory segment, if still attached.  This is not
 * intended to be called explicitly by the process that originally created the
 * segment (it will have on_shmem_exit callback(s) registered to do that).
 * Rather, this is for subprocesses that have inherited an attachment and want
 * to get rid of it.
 *
 * UsedShmemSegID and UsedShmemSegAddr are implicit parameters to this
 * routine, also AnonymousShmem and AnonymousShmemSize.
 */
pub unsafe fn PGSharedMemoryDetach() {
    if !UsedShmemSegAddr.is_null() {
        if shmdt(UsedShmemSegAddr) < 0 {
            elog!(
                LOG,
                "shmdt({:p}) failed: errno={}",
                UsedShmemSegAddr,
                errno()
            );
        }
        UsedShmemSegAddr = null_mut();
    }

    if !AnonymousShmem.is_null() {
        if munmap(AnonymousShmem, AnonymousShmemSize) < 0 {
            elog!(
                LOG,
                "munmap({:p}, {}) failed: errno={}",
                AnonymousShmem,
                AnonymousShmemSize,
                errno()
            );
        }
        AnonymousShmem = null_mut();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Darwin ABI sanity: the shmid_ds layout must put shm_nattch where the
    // kernel writes it (perm 24 + segsz 8 + lpid 4 + cpid 4 = offset 40) and
    // total 80 bytes (matching dsm_impl.rs's opaque [u8; 80]).
    #[test]
    fn shmid_ds_layout_matches_darwin_abi() {
        assert_eq!(core::mem::size_of::<ipc_perm>(), 24);
        assert_eq!(core::mem::offset_of!(shmid_ds, shm_segsz), 24);
        assert_eq!(core::mem::offset_of!(shmid_ds, shm_nattch), 40);
        assert_eq!(core::mem::size_of::<shmid_ds>(), 80);
    }

    #[test]
    fn huge_page_size_check_rejects_nonzero() {
        unsafe {
            let mut v: c_int = 0;
            assert!(check_huge_page_size(&raw mut v, core::ptr::null_mut(), 0));
            v = 2048;
            assert!(!check_huge_page_size(&raw mut v, core::ptr::null_mut(), 0));
        }
    }
}
