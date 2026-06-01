//! src/backend/storage/ipc/dsm_impl.c
//!   manage dynamic shared memory segments
//!
//! Merged companion header: src/include/storage/dsm_impl.h
//!
//! This file provides low-level APIs for creating and destroying shared
//! memory segments using several different possible techniques.  We refer
//! to these segments as dynamic because they can be created, altered, and
//! destroyed at any point during the server life cycle.  This is unlike
//! the main shared memory segment, of which there is always exactly one
//! and which is always mapped at a fixed address in every PostgreSQL
//! background process.
//!
//! Because not all systems provide the same primitives in this area, nor
//! do all primitives behave the same way on all systems, we provide
//! several implementations of this facility.  Many systems implement
//! POSIX shared memory (shm_open etc.), which is well-suited to our needs
//! in this area, with the exception that shared memory identifiers live
//! in a flat system-wide namespace, raising the uncomfortable prospect of
//! name collisions with other processes (including other copies of
//! PostgreSQL) running on the same system.  Some systems only support
//! the older System V shared memory interface (shmget etc.) which is
//! also usable; however, the default allocation limits are often quite
//! small, and the namespace is even more restricted.
//!
//! We also provide an mmap-based shared memory implementation.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::prelude::*;
use crate::miscadmin::IsUnderPostmaster;

use std::ffi::{c_char, c_int, c_void};

// ----------------------------------------------------------------
// dsm_impl.h
// ----------------------------------------------------------------

/* Dynamic shared memory implementations. */
pub const DSM_IMPL_POSIX: c_int = 1;
pub const DSM_IMPL_SYSV: c_int = 2;
pub const DSM_IMPL_WINDOWS: c_int = 3;
pub const DSM_IMPL_MMAP: c_int = 4;

/*
 * Determine which dynamic shared memory implementations will be supported
 * on this platform, and which one will be the default.
 *
 * On non-Windows platforms with HAVE_SHM_OPEN we get POSIX (default), plus
 * SysV and mmap.  USE_DSM_WINDOWS is gated out for this build.
 */
pub const DEFAULT_DYNAMIC_SHARED_MEMORY_TYPE: c_int = DSM_IMPL_POSIX;

/*
 * Directory for on-disk state.
 *
 * This is used by all implementations for crash recovery and by the mmap
 * implementation for storage.
 */
pub const PG_DYNSHMEM_DIR: &str = "pg_dynshmem";
pub const PG_DYNSHMEM_MMAP_FILE_PREFIX: &str = "mmap.";

/* A "name" for a dynamic shared memory segment. */
pub type dsm_handle = uint32;

/* Sentinel value to use for invalid DSM handles. */
pub const DSM_HANDLE_INVALID: dsm_handle = 0;

/* All the shared-memory operations we know about. */
#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub enum dsm_op {
    DSM_OP_CREATE,
    DSM_OP_ATTACH,
    DSM_OP_DETACH,
    DSM_OP_DESTROY,
}

pub use dsm_op::*;

// ----------------------------------------------------------------
// dsm_impl.c
// ----------------------------------------------------------------

/* Size of buffer to be used for zero-filling. */
const ZBUFFER_SIZE: usize = 8192;

#[allow(dead_code)]
const SEGMENT_NAME_PREFIX: &str = "Global/PostgreSQL";

/*
 * config_enum_entry is defined in utils/guc.h.  We model it as a minimal
 * struct for the GUC table below.
 */
#[repr(C)]
pub struct config_enum_entry {
    pub name: *const c_char,
    pub val: c_int,
    pub hidden: bool,
}

unsafe impl Sync for config_enum_entry {}

pub static dynamic_shared_memory_options: [config_enum_entry; 4] = [
    config_enum_entry {
        name: c"posix".as_ptr(),
        val: DSM_IMPL_POSIX,
        hidden: false,
    },
    config_enum_entry {
        name: c"sysv".as_ptr(),
        val: DSM_IMPL_SYSV,
        hidden: false,
    },
    config_enum_entry {
        name: c"mmap".as_ptr(),
        val: DSM_IMPL_MMAP,
        hidden: false,
    },
    config_enum_entry {
        name: std::ptr::null(),
        val: 0,
        hidden: false,
    },
];

/* Implementation selector. */
pub static mut dynamic_shared_memory_type: c_int = DEFAULT_DYNAMIC_SHARED_MEMORY_TYPE;

/* Amount of space reserved for DSM segments in the main area. */
pub static mut min_dynamic_shared_memory: c_int = 0;

// ----------------------------------------------------------------
// libc / system bindings used below
// ----------------------------------------------------------------

extern "C" {
    fn snprintf(s: *mut c_char, n: usize, fmt: *const c_char, ...) -> c_int;
    fn strlen(s: *const c_char) -> usize;

    fn munmap(addr: *mut c_void, len: usize) -> c_int;
    fn mmap(
        addr: *mut c_void,
        len: usize,
        prot: c_int,
        flags: c_int,
        fd: c_int,
        offset: i64,
    ) -> *mut c_void;
    fn shm_open(name: *const c_char, oflag: c_int, mode: c_int) -> c_int;
    fn shm_unlink(name: *const c_char) -> c_int;
    fn close(fd: c_int) -> c_int;
    fn fstat(fd: c_int, buf: *mut stat) -> c_int;
    fn ftruncate(fd: c_int, length: i64) -> c_int;
    fn sigprocmask(how: c_int, set: *const sigset_t, oldset: *mut sigset_t) -> c_int;
    fn write(fd: c_int, buf: *const c_void, count: usize) -> isize;
    fn unlink(path: *const c_char) -> c_int;

    fn shmget(key: key_t, size: usize, shmflg: c_int) -> c_int;
    fn shmat(shmid: c_int, shmaddr: *const c_void, shmflg: c_int) -> *mut c_void;
    fn shmdt(shmaddr: *const c_void) -> c_int;
    fn shmctl(shmid: c_int, cmd: c_int, buf: *mut shmid_ds) -> c_int;

    fn __error() -> *mut c_int; // Darwin errno location
}

#[allow(non_camel_case_types)]
type key_t = i32;

/* Opaque-ish stat; we only need st_size on Darwin (offset 96). */
#[repr(C)]
struct stat {
    _pad: [u8; 144],
}

impl stat {
    #[inline]
    unsafe fn st_size(&self) -> i64 {
        // Darwin struct stat: st_size at offset 96 (off_t).
        let base = self as *const stat as *const u8;
        core::ptr::read_unaligned(base.add(96) as *const i64)
    }
}

#[repr(C)]
struct sigset_t {
    _val: u32,
}

#[repr(C)]
struct shmid_ds {
    _pad: [u8; 80],
}

impl shmid_ds {
    #[inline]
    unsafe fn shm_segsz(&self) -> usize {
        // Darwin struct shmid_ds: shm_segsz follows shm_perm (struct ipc_perm).
        // On Darwin ipc_perm is 48 bytes; shm_segsz is size_t at offset 48.
        let base = self as *const shmid_ds as *const u8;
        core::ptr::read_unaligned(base.add(48) as *const usize)
    }
}

/* errno helpers. */
#[inline]
unsafe fn errno_get() -> c_int {
    *__error()
}
#[inline]
unsafe fn errno_set(v: c_int) {
    *__error() = v;
}

/* open(2) flags (Darwin values). */
const O_RDWR: c_int = 0x0002;
const O_CREAT: c_int = 0x0200;
const O_EXCL: c_int = 0x0800;

/* mmap(2) constants (Darwin values). */
const PROT_READ: c_int = 0x01;
const PROT_WRITE: c_int = 0x02;
const MAP_SHARED: c_int = 0x0001;
const MAP_HASSEMAPHORE: c_int = 0x0200;
const MAP_NOSYNC: c_int = 0x0800;
const MAP_FAILED: *mut c_void = (-1isize) as *mut c_void;

/* errno values (Darwin). */
const EINTR: c_int = 4;
const EEXIST: c_int = 17;
const ENOSPC: c_int = 28;
const EFBIG: c_int = 27;
const ENOMEM: c_int = 12;
const EINVAL: c_int = 22;

/* sigprocmask how. */
const SIG_SETMASK: c_int = 3;

/* System V IPC constants (Darwin). */
const IPC_PRIVATE: key_t = 0;
const IPC_CREAT: c_int = 0o0001000;
const IPC_EXCL: c_int = 0o0002000;
const IPC_RMID: c_int = 0;
const IPC_STAT: c_int = 2;

/* file_perm.h */
const PG_FILE_MODE_OWNER: c_int = 0o600;

/* portability/mem.h: PG_SHMAT_FLAGS and IPCProtection. */
const PG_SHMAT_FLAGS: c_int = 0;
const IPCProtection: c_int = 0o600;

/*------
 * Perform a low-level shared memory operation in a platform-specific way,
 * as dictated by the selected implementation.  Each implementation is
 * required to implement the following primitives.
 *
 * DSM_OP_CREATE.  Create a segment whose size is the request_size and map it.
 * DSM_OP_ATTACH.  Map the segment, whose size must be the request_size.
 * DSM_OP_DETACH.  Unmap the segment.
 * DSM_OP_DESTROY. Unmap the segment, if it is mapped.  Destroy the segment.
 *
 * Return value: true on success, false on failure.  When false is returned,
 * a message should first be logged at the specified elevel, except in the
 * case where DSM_OP_CREATE experiences a name collision, which should
 * silently return false.
 *-----
 */
pub unsafe fn dsm_impl_op(
    op: dsm_op,
    handle: dsm_handle,
    request_size: Size,
    impl_private: *mut *mut c_void,
    mapped_address: *mut *mut c_void,
    mapped_size: *mut Size,
    elevel: c_int,
) -> bool {
    Assert!(op == DSM_OP_CREATE || request_size == 0);
    Assert!(
        (op != DSM_OP_CREATE && op != DSM_OP_ATTACH)
            || ((*mapped_address).is_null() && *mapped_size == 0),
    );

    match dynamic_shared_memory_type {
        DSM_IMPL_POSIX => {
            return dsm_impl_posix(
                op,
                handle,
                request_size,
                impl_private,
                mapped_address,
                mapped_size,
                elevel,
            );
        }
        DSM_IMPL_SYSV => {
            return dsm_impl_sysv(
                op,
                handle,
                request_size,
                impl_private,
                mapped_address,
                mapped_size,
                elevel,
            );
        }
        DSM_IMPL_MMAP => {
            return dsm_impl_mmap(
                op,
                handle,
                request_size,
                impl_private,
                mapped_address,
                mapped_size,
                elevel,
            );
        }
        _ => {
            elog!(
                ERROR,
                "unexpected dynamic shared memory type: {}",
                dynamic_shared_memory_type
            );
            #[allow(unreachable_code)]
            return false;
        }
    }
}

/*
 * Operating system primitives to support POSIX shared memory.
 *
 * POSIX shared memory segments are created and attached using shm_open()
 * and shm_unlink(); other operations, such as sizing or mapping the
 * segment, are performed as if the shared memory segments were files.
 */
unsafe fn dsm_impl_posix(
    op: dsm_op,
    handle: dsm_handle,
    mut request_size: Size,
    _impl_private: *mut *mut c_void,
    mapped_address: *mut *mut c_void,
    mapped_size: *mut Size,
    elevel: c_int,
) -> bool {
    let mut name: [c_char; 64] = [0; 64];
    let flags: c_int;
    let fd: c_int;
    let address: *mut c_char;

    snprintf(
        name.as_mut_ptr(),
        64,
        c"/PostgreSQL.%u".as_ptr(),
        handle,
    );

    /* Handle teardown cases. */
    if op == DSM_OP_DETACH || op == DSM_OP_DESTROY {
        if !(*mapped_address).is_null() && munmap(*mapped_address, *mapped_size) != 0 {
            ereport!(elevel, "could not unmap shared memory segment");
            return false;
        }
        *mapped_address = std::ptr::null_mut();
        *mapped_size = 0;
        if op == DSM_OP_DESTROY && shm_unlink(name.as_ptr()) != 0 {
            ereport!(elevel, "could not remove shared memory segment");
            return false;
        }
        return true;
    }

    /*
     * Create new segment or open an existing one for attach.
     *
     * Even though we will close the FD before returning, it seems desirable
     * to use Reserve/ReleaseExternalFD, to reduce the probability of EMFILE
     * failure.
     */
    ReserveExternalFD();

    flags = O_RDWR | (if op == DSM_OP_CREATE { O_CREAT | O_EXCL } else { 0 });
    fd = shm_open(name.as_ptr(), flags, PG_FILE_MODE_OWNER);
    if fd == -1 {
        ReleaseExternalFD();
        if op == DSM_OP_ATTACH || errno_get() != EEXIST {
            ereport!(elevel, "could not open shared memory segment");
        }
        return false;
    }

    /*
     * If we're attaching the segment, determine the current size; if we are
     * creating the segment, set the size to the requested value.
     */
    if op == DSM_OP_ATTACH {
        let mut st: stat = std::mem::zeroed();

        if fstat(fd, &mut st) != 0 {
            let save_errno: c_int;

            /* Back out what's already been done. */
            save_errno = errno_get();
            close(fd);
            ReleaseExternalFD();
            errno_set(save_errno);

            ereport!(elevel, "could not stat shared memory segment");
            return false;
        }
        request_size = st.st_size() as Size;
    } else if dsm_impl_posix_resize(fd, request_size as i64) != 0 {
        let save_errno: c_int;

        /* Back out what's already been done. */
        save_errno = errno_get();
        close(fd);
        ReleaseExternalFD();
        shm_unlink(name.as_ptr());
        errno_set(save_errno);

        elog!(
            elevel,
            "could not resize shared memory segment to {} bytes",
            request_size
        );
        return false;
    }

    /* Map it. */
    address = mmap(
        std::ptr::null_mut(),
        request_size,
        PROT_READ | PROT_WRITE,
        MAP_SHARED | MAP_HASSEMAPHORE | MAP_NOSYNC,
        fd,
        0,
    ) as *mut c_char;
    if address == MAP_FAILED as *mut c_char {
        let save_errno: c_int;

        /* Back out what's already been done. */
        save_errno = errno_get();
        close(fd);
        ReleaseExternalFD();
        if op == DSM_OP_CREATE {
            shm_unlink(name.as_ptr());
        }
        errno_set(save_errno);

        ereport!(elevel, "could not map shared memory segment");
        return false;
    }
    *mapped_address = address as *mut c_void;
    *mapped_size = request_size;
    close(fd);
    ReleaseExternalFD();

    true
}

/*
 * Set the size of a virtual memory region associated with a file descriptor.
 * If necessary, also ensure that virtual memory is actually allocated by the
 * operating system, to avoid nasty surprises later.
 *
 * Returns non-zero if either truncation or allocation fails, and sets errno.
 */
unsafe fn dsm_impl_posix_resize(fd: c_int, size: i64) -> c_int {
    let mut rc: c_int;
    let save_errno: c_int;
    let mut save_sigmask: sigset_t = std::mem::zeroed();

    /*
     * Block all blockable signals, except SIGQUIT.  posix_fallocate() can run
     * for quite a long time, and is an all-or-nothing operation.  If we
     * allowed SIGUSR1 to interrupt us repeatedly (for example, due to
     * recovery conflicts), the retry loop might never succeed.
     */
    if IsUnderPostmaster {
        sigprocmask(SIG_SETMASK, &BlockSig, &mut save_sigmask);
    }

    pgstat_report_wait_start(WAIT_EVENT_DSM_ALLOCATE);

    /*
     * On platforms without HAVE_POSIX_FALLOCATE+__linux__ (e.g. Darwin),
     * extend the file to the requested size with ftruncate.
     */
    loop {
        rc = ftruncate(fd, size);
        if !(rc < 0 && errno_get() == EINTR) {
            break;
        }
    }

    pgstat_report_wait_end();

    if IsUnderPostmaster {
        save_errno = errno_get();
        sigprocmask(SIG_SETMASK, &save_sigmask, std::ptr::null_mut());
        errno_set(save_errno);
    }

    rc
}

/*
 * Operating system primitives to support System V shared memory.
 *
 * System V shared memory segments are manipulated using shmget(), shmat(),
 * shmdt(), and shmctl().
 */
unsafe fn dsm_impl_sysv(
    op: dsm_op,
    handle: dsm_handle,
    mut request_size: Size,
    impl_private: *mut *mut c_void,
    mapped_address: *mut *mut c_void,
    mapped_size: *mut Size,
    elevel: c_int,
) -> bool {
    let mut key: key_t;
    let ident: c_int;
    let address: *mut c_char;
    let mut name: [c_char; 64] = [0; 64];
    let ident_cache: *mut c_int;

    /*
     * POSIX shared memory and mmap-based shared memory identify segments with
     * names.  To avoid needless error message variation, we use the handle as
     * the name.
     */
    snprintf(name.as_mut_ptr(), 64, c"%u".as_ptr(), handle);

    /*
     * The System V shared memory namespace is very restricted; names are of
     * type key_t.  Since we use dsm_handle to identify shared memory segments
     * across processes, the cast below might truncate, but it'll truncate
     * exactly the same bits away in exactly the same fashion every time.
     *
     * We do make sure that the key isn't negative.
     */
    key = handle as key_t;
    if key < 1
    /* avoid compiler warning if type is unsigned */
    {
        key = -key;
    }

    /*
     * There's one special key, IPC_PRIVATE, which can't be used.  If we end
     * up with that value by chance during a create operation, just pretend it
     * already exists, so that caller will retry.
     */
    if key == IPC_PRIVATE {
        if op != DSM_OP_CREATE {
            elog!(DEBUG4, "System V shared memory key may not be IPC_PRIVATE");
        }
        errno_set(EEXIST);
        return false;
    }

    /*
     * Before we can do anything with a shared memory segment, we have to map
     * the shared memory key to a shared memory identifier using shmget(). To
     * avoid repeated lookups, we store the key using impl_private.
     */
    if !(*impl_private).is_null() {
        ident_cache = *impl_private as *mut c_int;
        ident = *ident_cache;
    } else {
        let mut flags: c_int = IPCProtection;
        let mut segsize: usize;

        /*
         * Allocate the memory BEFORE acquiring the resource, so that we don't
         * leak the resource if memory allocation fails.
         */
        ident_cache =
            MemoryContextAlloc(TopMemoryContext, std::mem::size_of::<c_int>()) as *mut c_int;

        /*
         * When using shmget to find an existing segment, we must pass the
         * size as 0.  Passing a non-zero size which is greater than the
         * actual size will result in EINVAL.
         */
        segsize = 0;

        if op == DSM_OP_CREATE {
            flags |= IPC_CREAT | IPC_EXCL;
            segsize = request_size;
        }

        ident = shmget(key, segsize, flags);
        if ident == -1 {
            if op == DSM_OP_ATTACH || errno_get() != EEXIST {
                let save_errno: c_int = errno_get();

                pfree(ident_cache as *mut c_void);
                errno_set(save_errno);
                ereport!(elevel, "could not get shared memory segment");
            }
            return false;
        }

        *ident_cache = ident;
        *impl_private = ident_cache as *mut c_void;
    }

    /* Handle teardown cases. */
    if op == DSM_OP_DETACH || op == DSM_OP_DESTROY {
        pfree(ident_cache as *mut c_void);
        *impl_private = std::ptr::null_mut();
        if !(*mapped_address).is_null() && shmdt(*mapped_address) != 0 {
            ereport!(elevel, "could not unmap shared memory segment");
            return false;
        }
        *mapped_address = std::ptr::null_mut();
        *mapped_size = 0;
        if op == DSM_OP_DESTROY && shmctl(ident, IPC_RMID, std::ptr::null_mut()) < 0 {
            ereport!(elevel, "could not remove shared memory segment");
            return false;
        }
        return true;
    }

    /* If we're attaching it, we must use IPC_STAT to determine the size. */
    if op == DSM_OP_ATTACH {
        let mut shm: shmid_ds = std::mem::zeroed();

        if shmctl(ident, IPC_STAT, &mut shm) != 0 {
            ereport!(elevel, "could not stat shared memory segment");
            return false;
        }
        request_size = shm.shm_segsz();
    }

    /* Map it. */
    address = shmat(ident, std::ptr::null(), PG_SHMAT_FLAGS) as *mut c_char;
    if address == (-1isize) as *mut c_char {
        let save_errno: c_int;

        /* Back out what's already been done. */
        save_errno = errno_get();
        if op == DSM_OP_CREATE {
            shmctl(ident, IPC_RMID, std::ptr::null_mut());
        }
        errno_set(save_errno);

        ereport!(elevel, "could not map shared memory segment");
        return false;
    }
    *mapped_address = address as *mut c_void;
    *mapped_size = request_size;

    let _ = EINVAL;

    true
}

/*
 * Operating system primitives to support mmap-based shared memory.
 *
 * Calling this "shared memory" is somewhat of a misnomer, because what
 * we're really doing is creating a bunch of files and mapping them into
 * our address space.
 */
unsafe fn dsm_impl_mmap(
    op: dsm_op,
    handle: dsm_handle,
    mut request_size: Size,
    _impl_private: *mut *mut c_void,
    mapped_address: *mut *mut c_void,
    mapped_size: *mut Size,
    elevel: c_int,
) -> bool {
    let mut name: [c_char; 64] = [0; 64];
    let flags: c_int;
    let fd: c_int;
    let address: *mut c_char;

    snprintf(
        name.as_mut_ptr(),
        64,
        c"pg_dynshmem/mmap.%u".as_ptr(),
        handle,
    );

    /* Handle teardown cases. */
    if op == DSM_OP_DETACH || op == DSM_OP_DESTROY {
        if !(*mapped_address).is_null() && munmap(*mapped_address, *mapped_size) != 0 {
            ereport!(elevel, "could not unmap shared memory segment");
            return false;
        }
        *mapped_address = std::ptr::null_mut();
        *mapped_size = 0;
        if op == DSM_OP_DESTROY && unlink(name.as_ptr()) != 0 {
            ereport!(elevel, "could not remove shared memory segment");
            return false;
        }
        return true;
    }

    /* Create new segment or open an existing one for attach. */
    flags = O_RDWR | (if op == DSM_OP_CREATE { O_CREAT | O_EXCL } else { 0 });
    fd = OpenTransientFile(name.as_ptr(), flags);
    if fd == -1 {
        if op == DSM_OP_ATTACH || errno_get() != EEXIST {
            ereport!(elevel, "could not open shared memory segment");
        }
        return false;
    }

    /*
     * If we're attaching the segment, determine the current size; if we are
     * creating the segment, set the size to the requested value.
     */
    if op == DSM_OP_ATTACH {
        let mut st: stat = std::mem::zeroed();

        if fstat(fd, &mut st) != 0 {
            let save_errno: c_int;

            /* Back out what's already been done. */
            save_errno = errno_get();
            CloseTransientFile(fd);
            errno_set(save_errno);

            ereport!(elevel, "could not stat shared memory segment");
            return false;
        }
        request_size = st.st_size() as Size;
    } else {
        /*
         * Allocate a buffer full of zeros.
         *
         * Note: palloc zbuffer, instead of just using a local char array, to
         * ensure it is reasonably well-aligned.
         */
        let zbuffer: *mut c_char = palloc0(ZBUFFER_SIZE) as *mut c_char;
        let mut remaining: Size = request_size;
        let mut success: bool = true;

        /*
         * Zero-fill the file. We have to do this the hard way to ensure that
         * all the file space has really been allocated, so that we don't
         * later seg fault when accessing the memory mapping.
         */
        while success && remaining > 0 {
            let mut goal: Size = remaining;

            if goal > ZBUFFER_SIZE {
                goal = ZBUFFER_SIZE;
            }
            pgstat_report_wait_start(WAIT_EVENT_DSM_FILL_ZERO_WRITE);
            if write(fd, zbuffer as *const c_void, goal) == goal as isize {
                remaining -= goal;
            } else {
                success = false;
            }
            pgstat_report_wait_end();
        }

        if !success {
            let save_errno: c_int;

            /* Back out what's already been done. */
            save_errno = errno_get();
            CloseTransientFile(fd);
            unlink(name.as_ptr());
            errno_set(if save_errno != 0 { save_errno } else { ENOSPC });

            elog!(
                elevel,
                "could not resize shared memory segment to {} bytes",
                request_size
            );
            return false;
        }
    }

    /* Map it. */
    address = mmap(
        std::ptr::null_mut(),
        request_size,
        PROT_READ | PROT_WRITE,
        MAP_SHARED | MAP_HASSEMAPHORE | MAP_NOSYNC,
        fd,
        0,
    ) as *mut c_char;
    if address == MAP_FAILED as *mut c_char {
        let save_errno: c_int;

        /* Back out what's already been done. */
        save_errno = errno_get();
        CloseTransientFile(fd);
        if op == DSM_OP_CREATE {
            unlink(name.as_ptr());
        }
        errno_set(save_errno);

        ereport!(elevel, "could not map shared memory segment");
        return false;
    }
    *mapped_address = address as *mut c_void;
    *mapped_size = request_size;

    if CloseTransientFile(fd) != 0 {
        ereport!(elevel, "could not close shared memory segment");
        return false;
    }

    true
}

/*
 * Implementation-specific actions that must be performed when a segment is to
 * be preserved even when no backend has it attached.
 *
 * Except on Windows, we don't need to do anything at all.
 */
pub unsafe fn dsm_impl_pin_segment(
    _handle: dsm_handle,
    _impl_private: *mut c_void,
    _impl_private_pm_handle: *mut *mut c_void,
) {
    match dynamic_shared_memory_type {
        _ => {}
    }
}

/*
 * Implementation-specific actions that must be performed when a segment is no
 * longer to be preserved, so that it will be cleaned up when all backends
 * have detached from it.
 *
 * Except on Windows, we don't need to do anything at all.
 */
pub unsafe fn dsm_impl_unpin_segment(_handle: dsm_handle, _impl_private: *mut *mut c_void) {
    match dynamic_shared_memory_type {
        _ => {}
    }
}

unsafe fn errcode_for_dynamic_shared_memory() -> c_int {
    if errno_get() == EFBIG || errno_get() == ENOMEM {
        errcode(ERRCODE_OUT_OF_MEMORY)
    } else {
        errcode_for_file_access()
    }
}

// ----------------------------------------------------------------
// Local stubs for unported helpers.
// ----------------------------------------------------------------

#[allow(non_upper_case_globals)]
static mut BlockSig: sigset_t = sigset_t { _val: 0 };

const DEBUG4: c_int = 13; // elog.h DEBUG4

const WAIT_EVENT_DSM_ALLOCATE: u32 = 0; // wait_event.h
const WAIT_EVENT_DSM_FILL_ZERO_WRITE: u32 = 0; // wait_event.h

const ERRCODE_OUT_OF_MEMORY: c_int = 0; // utils/errcodes.h

unsafe fn ReserveExternalFD() {
    // TODO: storage/fd.c
}

unsafe fn ReleaseExternalFD() {
    // TODO: storage/fd.c
}

unsafe fn OpenTransientFile(_filename: *const c_char, _flags: c_int) -> c_int {
    unimplemented!() // TODO: storage/fd.c
}

unsafe fn CloseTransientFile(_fd: c_int) -> c_int {
    unimplemented!() // TODO: storage/fd.c
}

unsafe fn pgstat_report_wait_start(_wait_event_info: u32) {
    // TODO: utils/activity/wait_event.c
}

unsafe fn pgstat_report_wait_end() {
    // TODO: utils/activity/wait_event.c
}

unsafe fn errcode(_sqlerrcode: c_int) -> c_int {
    unimplemented!() // TODO: utils/error/elog.c
}

unsafe fn errcode_for_file_access() -> c_int {
    unimplemented!() // TODO: utils/error/elog.c
}

#[inline]
#[allow(unused_unsafe)]
fn _silence_unused() {
    unsafe {
        let _ = errcode_for_dynamic_shared_memory as unsafe fn() -> c_int;
        let _ = strlen;
        let _ = SEGMENT_NAME_PREFIX;
        let _ = PG_DYNSHMEM_DIR;
        let _ = PG_DYNSHMEM_MMAP_FILE_PREFIX;
        let _ = DSM_IMPL_WINDOWS;
        let _ = DSM_HANDLE_INVALID;
        let _ = &dynamic_shared_memory_options;
    }
}
