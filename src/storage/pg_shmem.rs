//! storage/pg_shmem.h - Platform-independent API for shared memory support.

use std::ffi::{c_int, c_void};

use crate::c::{int32, uint32, Size};

// dsm_handle is defined in storage/dsm_impl.h (typedef uint32 dsm_handle).
// TODO: dedup when dsm_impl.h lands.
pub type dsm_handle = uint32;

// POSIX types referenced by the (non-WIN32) PGShmemHeader. PG includes these
// transitively via system headers; mirror them here against the platform ABI.
// On the Unix/macOS targets this port builds for: pid_t = c_int (i32),
// dev_t and ino_t per <sys/types.h>.
// TODO: dedup if a central libc/port type module materializes these.
pub type pid_t = c_int;
#[cfg(target_os = "macos")]
pub type dev_t = i32;
#[cfg(target_os = "macos")]
pub type ino_t = u64;
#[cfg(not(target_os = "macos"))]
pub type dev_t = u64;
#[cfg(not(target_os = "macos"))]
pub type ino_t = u64;

/// standard header for all Postgres shmem
#[repr(C)]
pub struct PGShmemHeader {
    /// magic # to identify Postgres segments
    pub magic: int32,
    /// PID of creating process (set but unread)
    pub creatorPID: pid_t,
    /// total size of segment
    pub totalsize: Size,
    /// offset to first free space
    pub freeoffset: Size,
    /// ID of dynamic shared memory control seg
    pub dsm_control: dsm_handle,
    /// pointer to ShmemIndex table
    pub index: *mut c_void,
    // #ifndef WIN32 (Windows doesn't have useful inode#s); this port targets
    // non-WIN32, so these fields are present.
    /// device data directory is on
    pub device: dev_t,
    /// inode number of data directory
    pub inode: ino_t,
}

pub const PGShmemMagic: c_int = 679834894;

/* GUC variables */
// extern PGDLLIMPORT int shared_memory_type;
// extern PGDLLIMPORT int huge_pages;
// extern PGDLLIMPORT int huge_page_size;
// extern PGDLLIMPORT int huge_pages_status;
// (Globals are owned by the defining .c translation; not declared here.)

/* Possible values for huge_pages and huge_pages_status (HugePagesType) */
pub type HugePagesType = c_int;
pub const HUGE_PAGES_OFF: HugePagesType = 0;
pub const HUGE_PAGES_ON: HugePagesType = 1;
/// only for huge_pages
pub const HUGE_PAGES_TRY: HugePagesType = 2;
/// only for huge_pages_status
pub const HUGE_PAGES_UNKNOWN: HugePagesType = 3;

/* Possible values for shared_memory_type (PGShmemType) */
pub type PGShmemType = c_int;
pub const SHMEM_TYPE_WINDOWS: PGShmemType = 0;
pub const SHMEM_TYPE_SYSV: PGShmemType = 1;
pub const SHMEM_TYPE_MMAP: PGShmemType = 2;

// #ifndef WIN32: UsedShmemSegID is an `unsigned long`. (extern global; not
// declared here.)
// extern PGDLLIMPORT unsigned long UsedShmemSegID;
// extern PGDLLIMPORT void *UsedShmemSegAddr;

// !defined(WIN32) && !defined(EXEC_BACKEND): default is MMAP.
pub const DEFAULT_SHARED_MEMORY_TYPE: PGShmemType = SHMEM_TYPE_MMAP;

// #ifdef EXEC_BACKEND block (PGSharedMemoryReAttach / PGSharedMemoryNoReAttach)
// is omitted; EXEC_BACKEND is not defined for this target.

// The SysV implementation (port/sysv_shmem.rs) provides the real bodies.
pub use crate::port::sysv_shmem::{
    GetHugePageSize, PGSharedMemoryCreate, PGSharedMemoryDetach, PGSharedMemoryIsInUse,
};
