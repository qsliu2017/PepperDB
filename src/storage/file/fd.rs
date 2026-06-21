//! storage/file/fd.rs
//!   Virtual file descriptor code.
//!
//! Translated 1:1 from postgres/src/backend/storage/file/fd.c
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! NOTES:
//!
//! This code manages a cache of 'virtual' file descriptors (VFDs).
//! The server opens many file descriptors for a variety of reasons,
//! including base tables, scratch files (e.g., sort and hash spool
//! files), and random calls to C library routines like system(3); it
//! is quite easy to exceed system limits on the number of open files a
//! single process can have.  (This is around 1024 on many modern
//! operating systems, but may be lower on others.)
//!
//! VFDs are managed as an LRU pool, with actual OS file descriptors
//! being opened and closed as needed.  Obviously, if a routine is
//! opened using these interfaces, all subsequent operations must also
//! be through these interfaces (the File type is not a real file
//! descriptor).
//!
//! #include mapping:
//!   "postgres.h"              -> crate::prelude::*
//!   <dirent.h>/<fcntl.h>/... -> libc extern decls below
//!   "storage/fd.h"            -> this module (pub API)
//!   "storage/ipc.h"           -> crate::storage::ipc (before_shmem_exit)
//!   "utils/resowner.h"        -> stubbed (ResourceOwner)
//!   "miscadmin.h"             -> crate::miscadmin (MyProcPid, MyDatabaseTableSpace)
//!   "pgstat.h"                -> stubbed
//!   "storage/aio.h"           -> stubbed (pgaio_closing_fd, pgaio_io_start_readv)
//!   "common/file_perm.h"      -> stubbed (pg_file_create_mode, pg_dir_create_mode)
//!   "common/pg_prng.h"        -> stubbed (pg_prng_uint64_range)

use crate::prelude::*;
use crate::{elog, ereport, errmsg, Assert};

use std::ffi::{c_char, c_int, c_long, c_uint, c_ulong, c_void};
use std::ptr::{null, null_mut};

// ---------------------------------------------------------------------------
// libc bindings (Darwin/POSIX).  Mirrors the extern-block style used in
// sysv_sema.rs.  Only the POSIX branch of fd.c is translated here; WIN32
// branches are wrapped in #[cfg(any())] with a comment.
// ---------------------------------------------------------------------------

/// struct iovec (sys/uio.h)
#[repr(C)]
pub struct iovec {
    pub iov_base: *mut c_void,
    pub iov_len: usize,
}

/// struct dirent (dirent.h) - opaque; we only touch d_name.
/// Darwin dirent: ino_t(8) seekoff(8) reclen(2) namlen(2) type(1) name[1024]
#[repr(C)]
pub struct dirent {
    pub d_ino: u64,
    pub d_seekoff: u64,
    pub d_reclen: u16,
    pub d_namlen: u16,
    pub d_type: u8,
    pub d_name: [c_char; 1024],
}

/// DIR is opaque.
#[repr(C)]
pub struct DIR {
    _opaque: [u8; 0],
}

/// struct stat - Darwin layout (abbreviated; we need st_mode and st_size).
/// Darwin: dev(4) mode(2) nlink(2) ino(8) uid(4) gid(4) rdev(4) ...
#[repr(C)]
pub struct stat_t {
    pub st_dev: i32,
    pub st_mode: u16,
    pub st_nlink: u16,
    pub st_ino: u64,
    pub st_uid: u32,
    pub st_gid: u32,
    pub st_rdev: i32,
    _atime: [i64; 2],
    _mtime: [i64; 2],
    _ctime: [i64; 2],
    pub st_size: i64,
    pub st_blocks: i64,
    pub st_blksize: i32,
    pub st_flags: u32,
    pub st_gen: u32,
    _lspare: i32,
    _qspare: [i64; 2],
}

/// struct rlimit (sys/resource.h)
#[repr(C)]
pub struct rlimit {
    pub rlim_cur: u64,
    pub rlim_max: u64,
}

/// struct radvisory (Darwin, fcntl.h)
#[repr(C)]
struct radvisory {
    ra_offset: off_t, /* offset into the file */
    ra_count: c_int,  /* size of the read     */
}

extern "C" {
    fn open(path: *const c_char, oflag: c_int, ...) -> c_int;
    fn close(fd: c_int) -> c_int;
    fn read(fd: c_int, buf: *mut c_void, nbytes: usize) -> isize;
    fn write(fd: c_int, buf: *const c_void, nbytes: usize) -> isize;
    fn lseek(fd: c_int, offset: off_t, whence: c_int) -> off_t;
    fn fsync(fd: c_int) -> c_int;
    fn fdatasync(fd: c_int) -> c_int;
    fn ftruncate(fd: c_int, length: off_t) -> c_int;
    fn truncate(path: *const c_char, length: off_t) -> c_int;
    fn unlink(path: *const c_char) -> c_int;
    fn rename(oldpath: *const c_char, newpath: *const c_char) -> c_int;
    fn rmdir(path: *const c_char) -> c_int;
    fn mkdir(path: *const c_char, mode: mode_t) -> c_int;
    fn dup(fd: c_int) -> c_int;
    fn fcntl(fd: c_int, cmd: c_int, ...) -> c_int;
    fn pread(fd: c_int, buf: *mut c_void, nbytes: usize, offset: off_t) -> isize;
    fn pwrite(fd: c_int, buf: *const c_void, nbytes: usize, offset: off_t) -> isize;
    fn opendir(name: *const c_char) -> *mut DIR;
    fn readdir(dirp: *mut DIR) -> *mut dirent;
    fn closedir(dirp: *mut DIR) -> c_int;
    #[link_name = "stat$INODE64"]
    fn stat_inode64(path: *const c_char, buf: *mut stat_t) -> c_int;
    #[link_name = "lstat$INODE64"]
    fn lstat_inode64(path: *const c_char, buf: *mut stat_t) -> c_int;
    #[link_name = "fstat$INODE64"]
    fn fstat_inode64(fd: c_int, buf: *mut stat_t) -> c_int;
    fn fopen(path: *const c_char, mode: *const c_char) -> *mut c_void; /* FILE* */
    fn fclose(file: *mut c_void) -> c_int;
    fn popen(command: *const c_char, mode: *const c_char) -> *mut c_void;
    fn pclose(file: *mut c_void) -> c_int;
    fn fflush(file: *mut c_void) -> c_int;
    fn malloc(size: usize) -> *mut c_void;
    fn realloc(ptr: *mut c_void, size: usize) -> *mut c_void;
    fn free(ptr: *mut c_void);
    fn strdup(s: *const c_char) -> *mut c_char;
    fn strlen(s: *const c_char) -> usize;
    fn strcmp(s1: *const c_char, s2: *const c_char) -> c_int;
    fn strncmp(s1: *const c_char, s2: *const c_char, n: usize) -> c_int;
    fn strspn(s: *const c_char, accept: *const c_char) -> usize;
    fn strlcpy(dst: *mut c_char, src: *const c_char, dstsize: usize) -> usize;
    fn snprintf(s: *mut c_char, n: usize, fmt: *const c_char, ...) -> c_int;
    fn isdigit(c: c_int) -> c_int;
    fn getrlimit(resource: c_int, rlp: *mut rlimit) -> c_int;
    fn sysconf(name: c_int) -> c_long;
    fn mmap(addr: *mut c_void, len: usize, prot: c_int, flags: c_int, fd: c_int, offset: off_t) -> *mut c_void;
    fn munmap(addr: *mut c_void, len: usize) -> c_int;
    fn msync(addr: *mut c_void, len: usize, flags: c_int) -> c_int;
    /// errno access (thread-local). macOS/Darwin uses __error().
    fn __error() -> *mut c_int;
}

#[inline]
unsafe fn errno() -> c_int { *__error() }
#[inline]
unsafe fn set_errno(e: c_int) { *__error() = e; }

// ---------------------------------------------------------------------------
// O_* / F_* / S_* / MS_* / MAP_* / RLIMIT_* constants (Darwin values)
// ---------------------------------------------------------------------------

const O_RDONLY:  c_int = 0x0000;
const O_WRONLY:  c_int = 0x0001;
const O_RDWR:    c_int = 0x0002;
const O_ACCMODE: c_int = 0x0003;
const O_CREAT:   c_int = 0x0200;
const O_EXCL:    c_int = 0x0800;
const O_TRUNC:   c_int = 0x0400;
const O_APPEND:  c_int = 0x0008;
const O_CLOEXEC: c_int = 0x01000000;
const O_DSYNC:   c_int = 0x400000;
const O_SYNC:    c_int = 0x80;
const O_WRFLAG:  c_int = O_WRONLY | O_RDWR; /* used internally */
const EMFILE: c_int = 24;
const ENFILE: c_int = 23;
const EINTR:  c_int = 4;
const ENOENT: c_int = 2;
const EEXIST: c_int = 17;
const EACCES: c_int = 13;
const EISDIR: c_int = 21;
const ENOSPC: c_int = 28;
const EINVAL: c_int = 22;
const EOPNOTSUPP: c_int = 102;
const EBADF:  c_int = 9;
const ENOTDIR: c_int = 20;

// fcntl commands
const F_GETFL:    c_int = 3;
const F_NOCACHE:  c_int = 48; /* Darwin: disable data caching */
const F_RDADVISE: c_int = 44; /* Darwin: issue advisory read */
const F_FULLFSYNC: c_int = 51; /* Darwin: flush to hardware */

// stat mode bits
const S_IFMT:  u16 = 0o170000;
const S_IFREG: u16 = 0o100000;
const S_IFDIR: u16 = 0o040000;
const S_IFLNK: u16 = 0o120000;
#[inline] fn S_ISREG(m: u16) -> bool { (m & S_IFMT) == S_IFREG }
#[inline] fn S_ISDIR(m: u16) -> bool { (m & S_IFMT) == S_IFDIR }
#[inline] fn S_ISLNK(m: u16) -> bool { (m & S_IFMT) == S_IFLNK }

// mmap
const PROT_READ:  c_int = 0x01;
const MAP_SHARED: c_int = 0x0001;
const MAP_FAILED: *mut c_void = !0usize as *mut c_void;
const MS_ASYNC:   c_int = 0x0001;

// sysconf
const _SC_PAGESIZE: c_int = 29; /* Darwin */

// getrlimit
const RLIMIT_NOFILE: c_int = 8; /* Darwin */

// SEEK_*
const SEEK_END: c_int = 2;

// off_t, mode_t, Size, Index, ssize_t
#[allow(non_camel_case_types)] pub type off_t    = i64;
#[allow(non_camel_case_types)] pub type mode_t   = u16;
#[allow(non_camel_case_types)] pub type ssize_t  = isize;
pub type Size  = usize;
pub type Index = usize;

// ---------------------------------------------------------------------------
// PG_BINARY: 0 on POSIX (defined as O_BINARY on WIN32, not ported)
// ---------------------------------------------------------------------------
const PG_BINARY: c_int = 0;

// PG_O_DIRECT: Darwin uses F_NOCACHE via fcntl after open.
// We define a sentinel bit that does not collide with O_* above.
// In BasicOpenFilePerm we strip it and call fcntl(F_NOCACHE).
const PG_O_DIRECT: c_int = 0x80000000u32 as c_int; /* PG_O_DIRECT_USE_F_NOCACHE path */

/*
 * We must leave some file descriptors free for system(), the dynamic loader,
 * and other code that tries to open files without consulting fd.c.  This
 * is the number left free.
 */
const NUM_RESERVED_FDS: c_int = 10;

/*
 * If we have fewer than this many usable FDs after allowing for the reserved
 * ones, choke.
 */
const FD_MINFREE: c_int = 48;

// ---------------------------------------------------------------------------
// GUC variables (TODO(pg-port): wire up real GUC infrastructure)
// ---------------------------------------------------------------------------

/// GUC: maximum number of files per process (default 1000).
/// TODO(pg-port): register as GUC integer "max_files_per_process".
pub static mut max_files_per_process: c_int = 1000;

/// Maximum number of FDs fd.c is allowed to use (VFDs + AllocateFile etc).
/// Initialised conservatively; updated by set_max_safe_fds().
pub static mut max_safe_fds: c_int = FD_MINFREE; /* default if not changed */

/// GUC: whether it is safe to continue running after fsync() fails.
/// TODO(pg-port): register as GUC bool "data_sync_retry".
pub static mut data_sync_retry: bool = false;

/// GUC: how SyncDataDirectory() should do its job.
/// TODO(pg-port): register as GUC enum "recovery_init_sync_method".
pub static mut recovery_init_sync_method: c_int = DATA_DIR_SYNC_METHOD_FSYNC;
const DATA_DIR_SYNC_METHOD_FSYNC:  c_int = 0;
const DATA_DIR_SYNC_METHOD_SYNCFS: c_int = 1; /* Linux only; not ported */

/// GUC: how data files should be bulk-extended with zeros.
/// TODO(pg-port): register as GUC enum "file_extend_method".
pub static mut file_extend_method: c_int = 0;

/// GUC: which kinds of files should be opened with PG_O_DIRECT.
/// TODO(pg-port): register as GUC flags "debug_io_direct".
pub static mut io_direct_flags: c_int = 0;
const IO_DIRECT_DATA:     c_int = 0x01;
const IO_DIRECT_WAL:      c_int = 0x02;
const IO_DIRECT_WAL_INIT: c_int = 0x04;

// ---------------------------------------------------------------------------
// VFD state bit flags (fdstate field)
// ---------------------------------------------------------------------------
const FD_DELETE_AT_CLOSE:  u16 = 1 << 0; /* T = delete when closed */
const FD_CLOSE_AT_EOXACT:  u16 = 1 << 1; /* T = close at eoXact */
const FD_TEMP_FILE_LIMIT:  u16 = 1 << 2; /* T = respect temp_file_limit */

const VFD_CLOSED: c_int = -1;

// ---------------------------------------------------------------------------
// Vfd struct - virtual file descriptor
// ---------------------------------------------------------------------------

/// Virtual file descriptor record.
#[repr(C)]
struct Vfd {
    fd:              c_int,   /* current FD, or VFD_CLOSED if none */
    fdstate:         u16,     /* bitflags for VFD's state */
    resowner:        ResourceOwner, /* owner, for automatic cleanup */
    nextFree:        File,    /* link to next free VFD, if in freelist */
    lruMoreRecently: File,    /* doubly linked recency-of-use list */
    lruLessRecently: File,
    fileSize:        off_t,   /* current size of file (0 if not temporary) */
    fileName:        *mut c_char, /* name of file, or NULL for unused VFD */
    /* NB: fileName is malloc'd, and must be free'd when closing the VFD */
    fileFlags:       c_int,   /* open(2) flags for (re)opening the file */
    fileMode:        mode_t,  /* mode to pass to open(2) */
}

/*
 * Virtual File Descriptor array pointer and size.  This grows as needed.
 * 'File' values are indexes into this array.
 * Note that VfdCache[0] is not a usable VFD, just a list header.
 */
static mut VfdCache: *mut Vfd = null_mut();
static mut SizeVfdCache: Size = 0;

/*
 * Number of file descriptors known to be in use by VFD entries.
 */
static mut nfile: c_int = 0;

/*
 * Flag to tell whether it's worth scanning VfdCache looking for temp files
 * to close
 */
static mut have_xact_temporary_files: bool = false;

/*
 * Tracks the total size of all temporary files.
 */
static mut temporary_files_size: u64 = 0;

/* Temporary file access initialized and not yet shut down? */
#[cfg(debug_assertions)]
static mut temporary_files_allowed: bool = false;

// ---------------------------------------------------------------------------
// AllocateDesc - tracks OS handles opened via AllocateFile/AllocateDir/etc.
// ---------------------------------------------------------------------------

#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq)]
enum AllocateDescKind {
    AllocateDescFile,
    AllocateDescPipe,
    AllocateDescDir,
    AllocateDescRawFD,
}

/// Union desc field
#[repr(C)]
#[derive(Clone, Copy)]
union AllocateDescUnion {
    file: *mut c_void, /* FILE* */
    dir:  *mut DIR,
    fd:   c_int,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct AllocateDesc {
    kind:           AllocateDescKind,
    create_subid:   SubTransactionId,
    desc:           AllocateDescUnion,
}

static mut numAllocatedDescs:  c_int         = 0;
static mut maxAllocatedDescs:  c_int         = 0;
static mut allocatedDescs:     *mut AllocateDesc = null_mut();

/*
 * Number of open "external" FDs reported to Reserve/ReleaseExternalFD.
 */
static mut numExternalFDs: c_int = 0;

/*
 * Number of temporary files opened during the current session;
 * this is used in generation of tempfile names.
 */
static mut tempFileCounter: c_long = 0;

/*
 * Array of OIDs of temp tablespaces.  (Some entries may be InvalidOid,
 * indicating that the current database's default tablespace should be used.)
 * When numTempTableSpaces is -1, this has not been set in the current
 * transaction.
 */
static mut tempTableSpaces:    *mut Oid = null_mut();
static mut numTempTableSpaces: c_int    = -1;
static mut nextTempTableSpace: c_int    = 0;

// ---------------------------------------------------------------------------
// Stubbed dependencies (not yet ported)
// ---------------------------------------------------------------------------

/// File is an index into the virtual file descriptor table.
/// (mirrors the typedef in storage/fd.h)
pub type File = c_int;

// Opaque types used by the ResourceOwner API.
// TODO(pg-port): port utils/resowner.c
#[allow(non_camel_case_types)]
pub type ResourceOwner = *mut c_void;

#[allow(non_camel_case_types)]
pub type SubTransactionId = u32;

// Opaque PgAioHandle type (storage/aio.h).
// TODO(pg-port): port storage/aio.c
#[allow(non_camel_case_types)]
pub struct PgAioHandle { _opaque: [u8; 0] }

// Oid (postgres_ext.h)
pub type Oid = u32;
const InvalidOid: Oid = 0;
const DEFAULTTABLESPACE_OID: Oid = 1663;
const GLOBALTABLESPACE_OID:  Oid = 1664;

// GUC stubs
// TODO(pg-port): wire up real GUC values
static mut enableFsync: bool = true;
static mut temp_file_limit: c_int = -1;  /* -1 means no limit */
static mut log_temp_files: c_int = -1;

// Misc admin stubs
// TODO(pg-port): wire up real per-backend values
unsafe fn MyProcPid() -> c_int { 0 }
unsafe fn MyDatabaseTableSpace() -> Oid { DEFAULTTABLESPACE_OID }
unsafe fn CurrentResourceOwner() -> ResourceOwner { null_mut() }
unsafe fn GetCurrentSubTransactionId() -> SubTransactionId { 1 }

// file_perm.h stubs
// TODO(pg-port): wire up common/file_perm.c
static mut pg_file_create_mode: mode_t = 0o600;
static mut pg_dir_create_mode:  mode_t = 0o700;

// pgstat stubs
// TODO(pg-port): wire up pgstat_report_tempfile / pgstat_report_wait_*
unsafe fn pgstat_report_tempfile(_size: off_t) {}
unsafe fn pgstat_report_wait_start(_info: u32) {}
unsafe fn pgstat_report_wait_end() {}

// aio stubs
// TODO(pg-port): wire up storage/aio.c
unsafe fn pgaio_closing_fd(_fd: c_int) {}
unsafe fn pgaio_io_start_readv(_ioh: *mut PgAioHandle, _fd: c_int, _iovcnt: c_int, _offset: off_t) {}

// ResourceOwner stubs
// TODO(pg-port): wire up utils/resowner.c
unsafe fn ResourceOwnerEnlarge(_owner: ResourceOwner) {}
unsafe fn ResourceOwnerRemember(_owner: ResourceOwner, _datum: Datum, _desc: *const ResourceOwnerDesc) {}
unsafe fn ResourceOwnerForget(_owner: ResourceOwner, _datum: Datum, _desc: *const ResourceOwnerDesc) {}
unsafe fn Int32GetDatum(v: c_int) -> Datum { v as Datum }
unsafe fn DatumGetInt32(d: Datum) -> c_int { d as c_int }
type Datum = usize;
struct ResourceOwnerDesc; /* opaque stub */

// startup progress stubs
// TODO(pg-port): wire up postmaster/startup.c
macro_rules! ereport_startup_progress {
    ($fmt:literal $(, $arg:expr)*) => { /* no-op stub */ };
}

// begin_startup_progress_phase stub
unsafe fn begin_startup_progress_phase() {}

// prng stub
// TODO(pg-port): wire up common/pg_prng.c
unsafe fn pg_prng_uint64_range(_state: *mut c_void, lo: u64, hi: u64) -> u64 { lo }
static mut pg_global_prng_state: *mut c_void = null_mut();

// pg_preadv / pg_pwritev / pg_pwrite_zeros stubs
// TODO(pg-port): wire up port/pg_pread.c and friends
unsafe fn pg_preadv(fd: c_int, iov: *const iovec, iovcnt: c_int, offset: off_t) -> ssize_t {
    if iovcnt == 1 {
        pread(fd, (*iov).iov_base, (*iov).iov_len, offset)
    } else {
        set_errno(EINVAL); -1
    }
}
unsafe fn pg_pwritev(fd: c_int, iov: *const iovec, iovcnt: c_int, offset: off_t) -> ssize_t {
    if iovcnt == 1 {
        pwrite(fd, (*iov).iov_base, (*iov).iov_len, offset)
    } else {
        set_errno(EINVAL); -1
    }
}
unsafe fn pg_pwrite_zeros(fd: c_int, amount: off_t, offset: off_t) -> ssize_t {
    /* simple stub: write zeros one page at a time */
    let buf = [0u8; 4096];
    let mut remaining = amount;
    let mut off = offset;
    while remaining > 0 {
        let n = if remaining > 4096 { 4096 } else { remaining as usize };
        let written = pwrite(fd, buf.as_ptr() as *const c_void, n, off);
        if written < 0 { return -1; }
        off += written as off_t;
        remaining -= written as off_t;
    }
    amount as ssize_t
}

// tablespace path stubs (commands/tablespace.h)
// TODO(pg-port): wire up commands/tablespace.c
const PG_TBLSPC_DIR: &[u8]                  = b"pg_tblspc\0";
const PG_TEMP_FILES_DIR: &[u8]              = b"pgsql_tmp\0";
const PG_TEMP_FILE_PREFIX: &[u8]            = b"pgsql_tmp\0";
const TABLESPACE_VERSION_DIRECTORY: &[u8]   = b"PG_18_202504131\0";

// pqsignal / SIGPIPE stubs
const SIGPIPE: c_int = 13;
const SIG_IGN: usize = 1;
const SIG_DFL: usize = 0;
unsafe fn pqsignal(_sig: c_int, _handler: usize) {}

// utils/varlena.h stubs
// TODO(pg-port): wire up utils/adt/varlena.c
#[allow(dead_code)]
unsafe fn psprintf(_fmt: *const c_char) -> *mut c_char { null_mut() } /* TODO(pg-port): variadic stub */

// GUC source stub
type GucSource = c_int;

// misc path utilities
// TODO(pg-port): wire up common/path.c
unsafe fn get_parent_directory(path: *mut c_char) {
    /* strip last component in place */
    let len = strlen(path);
    let slice = std::slice::from_raw_parts_mut(path as *mut u8, len);
    let mut i = len;
    while i > 0 && slice[i - 1] != b'/' {
        i -= 1;
    }
    if i > 0 { i -= 1; } /* strip trailing slash */
    slice[i] = 0;
}

// posix_fallocate stub (Darwin doesn't have it)
// TODO(pg-port): enable if available
#[allow(non_snake_case)]
unsafe fn posix_fallocate(_fd: c_int, _offset: off_t, _len: off_t) -> c_int {
    EINVAL /* simulate HAVE_POSIX_FALLOCATE=false */
}

// forkname_chars stub (access/rel.h)
// TODO(pg-port): wire up access/rel.c
unsafe fn forkname_chars(_str: *const c_char, _fork: *mut c_void) -> c_int { crate::common::relpath::forkname_chars(_str, _fork as _) }

// get_dirent_type stub (common/file_utils.h)
// TODO(pg-port): wire up common/file_utils.c
#[allow(non_camel_case_types)]
#[derive(PartialEq)]
enum PGFileType {
    PGFILETYPE_ERROR,
    PGFILETYPE_REG,
    PGFILETYPE_DIR,
    PGFILETYPE_LNK,
    PGFILETYPE_OTHER,
}
unsafe fn get_dirent_type(path: *const c_char, _de: *mut dirent, process_symlinks: bool, _elevel: c_int) -> PGFileType {
    let mut st: stat_t = core::mem::zeroed();
    let rc = if process_symlinks { stat_inode64(path, &raw mut st) }
             else                { lstat_inode64(path, &raw mut st) };
    if rc < 0 { return PGFileType::PGFILETYPE_ERROR; }
    if S_ISREG(st.st_mode) { PGFileType::PGFILETYPE_REG }
    else if S_ISDIR(st.st_mode) { PGFileType::PGFILETYPE_DIR }
    else if S_ISLNK(st.st_mode) { PGFileType::PGFILETYPE_LNK }
    else { PGFileType::PGFILETYPE_OTHER }
}

// looks_like_temp_rel_name is public (declared below)

// CHECK_FOR_INTERRUPTS stub
macro_rules! CHECK_FOR_INTERRUPTS { () => { /* stub */ }; }

// MAXPGPATH
const MAXPGPATH: usize = 1024;

// elog / ereport level constants come from crate::prelude (DEBUG1, LOG, WARNING, ERROR, FATAL, PANIC, DEBUG2)

// ---------------------------------------------------------------------------
// ResourceOwner callback descriptors for File
// ---------------------------------------------------------------------------

/* Convenience wrappers over ResourceOwnerRemember/Forget */
#[inline]
unsafe fn ResourceOwnerRememberFile(owner: ResourceOwner, file: File) {
    ResourceOwnerRemember(owner, Int32GetDatum(file), &raw const FILE_RESOWNER_DESC);
}
#[inline]
unsafe fn ResourceOwnerForgetFile(owner: ResourceOwner, file: File) {
    ResourceOwnerForget(owner, Int32GetDatum(file), &raw const FILE_RESOWNER_DESC);
}

static FILE_RESOWNER_DESC: ResourceOwnerDesc = ResourceOwnerDesc;

// ---------------------------------------------------------------------------
// Macro helpers (mirror C macros)
// ---------------------------------------------------------------------------

#[inline]
unsafe fn FileIsValid(file: File) -> bool {
    file > 0 && (file as Size) < SizeVfdCache && !(*VfdCache.add(file as usize)).fileName.is_null()
}

#[inline]
unsafe fn FileIsNotOpen(file: File) -> bool {
    (*VfdCache.add(file as usize)).fd == VFD_CLOSED
}

// ---------------------------------------------------------------------------
// pg_fsync --- do fsync with or without writethrough
// ---------------------------------------------------------------------------

pub unsafe fn pg_fsync(fd: c_int) -> c_int {
    /*
     * On Darwin, we use F_FULLFSYNC when wal_sync_method is
     * WAL_SYNC_METHOD_FSYNC_WRITETHROUGH; otherwise fall through to
     * pg_fsync_no_writethrough.
     *
     * #if defined(HAVE_FSYNC_WRITETHROUGH)
     *   if (wal_sync_method == WAL_SYNC_METHOD_FSYNC_WRITETHROUGH)
     *       return pg_fsync_writethrough(fd);
     * Darwin has F_FULLFSYNC so pg_fsync_writethrough is available.
     * For now translate the else branch (pg_fsync_no_writethrough) only;
     * the writethrough branch is kept under cfg(any()) below.
     */
    #[cfg(any())] /* WAL_SYNC_METHOD_FSYNC_WRITETHROUGH branch - not ported */
    {
        // return pg_fsync_writethrough(fd);
    }
    pg_fsync_no_writethrough(fd)
}

/*
 * pg_fsync_no_writethrough --- same as fsync except does nothing if
 *   enableFsync is off
 */
pub unsafe fn pg_fsync_no_writethrough(fd: c_int) -> c_int {
    let mut rc: c_int;

    if !enableFsync {
        return 0;
    }

    loop {
        rc = fsync(fd);
        if !(rc == -1 && errno() == EINTR) {
            break;
        }
    }

    rc
}

/*
 * pg_fsync_writethrough
 */
pub unsafe fn pg_fsync_writethrough(fd: c_int) -> c_int {
    if enableFsync {
        /* Darwin has F_FULLFSYNC */
        if fcntl(fd, F_FULLFSYNC, 0) == -1 { -1 } else { 0 }
    } else {
        0
    }
}

/*
 * pg_fdatasync --- same as fdatasync except does nothing if enableFsync is off
 */
pub unsafe fn pg_fdatasync(fd: c_int) -> c_int {
    let mut rc: c_int;

    if !enableFsync {
        return 0;
    }

    loop {
        rc = fdatasync(fd);
        if !(rc == -1 && errno() == EINTR) {
            break;
        }
    }

    rc
}

/*
 * pg_file_exists -- check that a file exists.
 *
 * This requires an absolute path to the file.  Returns true if the file is
 * not a directory, false otherwise.
 */
pub unsafe fn pg_file_exists(name: *const c_char) -> bool {
    let mut st: stat_t = core::mem::zeroed();

    Assert!(!name.is_null());

    if stat_inode64(name, &raw mut st) == 0 {
        return !S_ISDIR(st.st_mode);
    } else if !(errno() == ENOENT || errno() == ENOTDIR || errno() == EACCES) {
        ereport!(
            ERROR,
            errmsg!(
                "could not access file \"{}\": errno={}",
                std::ffi::CStr::from_ptr(name).to_string_lossy(),
                errno()
            )
        );
    }

    false
}

/*
 * pg_flush_data --- advise OS that the described dirty data should be flushed
 *
 * offset of 0 with nbytes 0 means that the entire file should be flushed
 *
 * On Darwin we use the mmap/msync(MS_ASYNC) path.
 * sync_file_range (Linux) and POSIX_FADV_DONTNEED are under cfg(any()).
 */
pub unsafe fn pg_flush_data(fd: c_int, offset: off_t, mut nbytes: off_t) {
    /*
     * Right now file flushing is primarily used to avoid making later
     * fsync()/fdatasync() calls have less impact.  Thus don't trigger flushes
     * if fsyncs are disabled.
     */
    if !enableFsync {
        return;
    }

    /*
     * We compile all alternatives that are supported on the current platform.
     */

    /* #if defined(HAVE_SYNC_FILE_RANGE) - Linux only, not ported */
    #[cfg(any())]
    {
        // sync_file_range path omitted
    }

    /* #if !defined(WIN32) && defined(MS_ASYNC) - Darwin has MS_ASYNC */
    {
        let p: *mut c_void;
        static mut pagesize: c_int = 0;

        /*
         * On several OSs msync(MS_ASYNC) on a mmap'ed file triggers writeback.
         */

        /* mmap() needs actual length if we want to map whole file */
        if offset == 0 && nbytes == 0 {
            nbytes = lseek(fd, 0, SEEK_END);
            if nbytes < 0 {
                ereport!(
                    WARNING,
                    errmsg!("could not determine dirty data size: errno={}", errno())
                );
                return;
            }
        }

        /*
         * Some platforms reject partial-page mmap() attempts.  To deal with
         * that, just truncate the request to a page boundary.
         */

        /* fetch pagesize only once */
        if pagesize == 0 {
            pagesize = sysconf(_SC_PAGESIZE) as c_int;
        }

        /* align length to pagesize, dropping any fractional page */
        if pagesize > 0 {
            nbytes = (nbytes / pagesize as off_t) * pagesize as off_t;
        }

        /* fractional-page request is a no-op */
        if nbytes <= 0 {
            return;
        }

        /*
         * mmap could well fail, particularly on 32-bit platforms where there
         * may simply not be enough address space.  If so, silently fall through.
         */
        if nbytes <= isize::MAX as off_t {
            p = mmap(null_mut(), nbytes as usize, PROT_READ, MAP_SHARED, fd, offset);
        } else {
            p = MAP_FAILED;
        }

        if p != MAP_FAILED {
            let rc = msync(p, nbytes as usize, MS_ASYNC);
            if rc != 0 {
                ereport!(
                    WARNING, /* data_sync_elevel(WARNING) */
                    errmsg!("could not flush dirty data: errno={}", errno())
                );
                /* NB: need to fall through to munmap()! */
            }

            let rc2 = munmap(p, nbytes as usize);
            if rc2 != 0 {
                /* FATAL error because mapping would remain */
                ereport!(
                    FATAL,
                    errmsg!("could not munmap() while flushing data: errno={}", errno())
                );
            }

            return;
        }
    }

    /* USE_POSIX_FADVISE / POSIX_FADV_DONTNEED - not available on Darwin */
    #[cfg(any())]
    {
        // posix_fadvise path omitted
    }
}

/*
 * pg_ftruncate (static helper) - Truncate an open file to a given length.
 */
unsafe fn pg_ftruncate(fd: c_int, length: off_t) -> c_int {
    let mut ret: c_int;

    loop {
        ret = ftruncate(fd, length);
        if !(ret == -1 && errno() == EINTR) {
            break;
        }
    }

    ret
}

/*
 * Truncate a file to a given length by name.
 */
pub unsafe fn pg_truncate(path: *const c_char, length: off_t) -> c_int {
    let mut ret: c_int;

    /* WIN32 branch: open/ftruncate/close - not ported */
    #[cfg(any())]
    {
        // WIN32 path omitted
    }

    loop {
        ret = truncate(path, length);
        if !(ret == -1 && errno() == EINTR) {
            break;
        }
    }

    ret
}

/*
 * fsync_fname -- fsync a file or directory, handling errors properly
 *
 * Try to fsync a file or directory. When doing the latter, ignore errors that
 * indicate the OS just doesn't allow/require fsyncing directories.
 */
pub unsafe fn fsync_fname(fname: *const c_char, isdir: bool) {
    fsync_fname_ext(fname, isdir, false, data_sync_elevel(ERROR));
}

/*
 * durable_rename -- rename(2) wrapper, issuing fsyncs required for durability
 *
 * Returns 0 if the operation succeeded, -1 otherwise. Note that errno is not
 * valid upon return.
 */
pub unsafe fn durable_rename(oldfile: *const c_char, newfile: *const c_char, elevel: c_int) -> c_int {
    let fd: c_int;

    /*
     * First fsync the old and target path (if it exists), to ensure that they
     * are properly persistent on disk.
     */
    if fsync_fname_ext(oldfile, false, false, elevel) != 0 {
        return -1;
    }

    fd = OpenTransientFile(newfile, PG_BINARY | O_RDWR);
    if fd < 0 {
        if errno() != ENOENT {
            ereport!(
                elevel,
                errmsg!(
                    "could not open file \"{}\": errno={}",
                    std::ffi::CStr::from_ptr(newfile).to_string_lossy(),
                    errno()
                )
            );
            return -1;
        }
    } else {
        if pg_fsync(fd) != 0 {
            let save_errno = errno();

            /* close file upon error, might not be in transaction context */
            CloseTransientFile(fd);
            set_errno(save_errno);

            ereport!(
                elevel,
                errmsg!(
                    "could not fsync file \"{}\": errno={}",
                    std::ffi::CStr::from_ptr(newfile).to_string_lossy(),
                    errno()
                )
            );
            return -1;
        }

        if CloseTransientFile(fd) != 0 {
            ereport!(
                elevel,
                errmsg!(
                    "could not close file \"{}\": errno={}",
                    std::ffi::CStr::from_ptr(newfile).to_string_lossy(),
                    errno()
                )
            );
            return -1;
        }
    }

    /* Time to do the real deal... */
    if rename(oldfile, newfile) < 0 {
        ereport!(
            elevel,
            errmsg!(
                "could not rename file \"{}\" to \"{}\": errno={}",
                std::ffi::CStr::from_ptr(oldfile).to_string_lossy(),
                std::ffi::CStr::from_ptr(newfile).to_string_lossy(),
                errno()
            )
        );
        return -1;
    }

    /*
     * To guarantee renaming the file is persistent, fsync the file with its
     * new name, and its containing directory.
     */
    if fsync_fname_ext(newfile, false, false, elevel) != 0 {
        return -1;
    }

    if fsync_parent_path(newfile, elevel) != 0 {
        return -1;
    }

    0
}

/*
 * durable_unlink -- remove a file in a durable manner
 *
 * Returns 0 if the operation succeeded, -1 otherwise.
 */
pub unsafe fn durable_unlink(fname: *const c_char, elevel: c_int) -> c_int {
    if unlink(fname) < 0 {
        ereport!(
            elevel,
            errmsg!(
                "could not remove file \"{}\": errno={}",
                std::ffi::CStr::from_ptr(fname).to_string_lossy(),
                errno()
            )
        );
        return -1;
    }

    /*
     * To guarantee that the removal of the file is persistent, fsync its
     * parent directory.
     */
    if fsync_parent_path(fname, elevel) != 0 {
        return -1;
    }

    0
}

// ---------------------------------------------------------------------------
// Initialization
// ---------------------------------------------------------------------------

/*
 * InitFileAccess --- initialize this module during backend startup
 *
 * This is called during either normal or standalone backend start.
 * It is *not* called in the postmaster.
 */
pub unsafe fn InitFileAccess() {
    Assert!(SizeVfdCache == 0); /* call me only once */

    /* initialize cache header entry */
    VfdCache = malloc(core::mem::size_of::<Vfd>()) as *mut Vfd;
    if VfdCache.is_null() {
        ereport!(FATAL, errmsg!("out of memory"));
        unreachable!();
    }

    core::ptr::write_bytes(VfdCache, 0, 1);
    (*VfdCache).fd = VFD_CLOSED;

    SizeVfdCache = 1;
}

/*
 * InitTemporaryFileAccess --- initialize temporary file access during startup
 *
 * This is separate from InitFileAccess() because temporary file cleanup can
 * cause pgstat reporting.
 */
pub unsafe fn InitTemporaryFileAccess() {
    Assert!(SizeVfdCache != 0); /* InitFileAccess() needs to have run */
    #[cfg(debug_assertions)]
    Assert!(!temporary_files_allowed); /* call me only once */

    /*
     * Register before-shmem-exit hook to ensure temp files are dropped while
     * we can still report stats.
     */
    before_shmem_exit(BeforeShmemExit_Files, 0);

    #[cfg(debug_assertions)]
    {
        temporary_files_allowed = true;
    }
}

// before_shmem_exit stub
// TODO(pg-port): wire up storage/ipc.c
unsafe fn before_shmem_exit(_f: unsafe extern "C" fn(c_int, Datum), _arg: Datum) {}


// ---------------------------------------------------------------------------
// count_usable_fds, set_max_safe_fds
// ---------------------------------------------------------------------------

/*
 * count_usable_fds --- count how many FDs the system will let us open,
 *   and estimate how many are already open.
 */
unsafe fn count_usable_fds(max_to_probe: c_int, usable_fds: *mut c_int, already_open: *mut c_int) {
    let mut size: c_int = 1024;
    let mut fd: *mut c_int = shmem_stubs::palloc(
        (size as usize) * core::mem::size_of::<c_int>()
    ) as *mut c_int;
    let mut used: c_int = 0;
    let mut highestfd: c_int = 0;

    let mut rlim: rlimit = core::mem::zeroed();
    let getrlimit_status = getrlimit(RLIMIT_NOFILE, &raw mut rlim);
    if getrlimit_status != 0 {
        ereport!(WARNING, errmsg!("getrlimit failed: errno={}", errno()));
    }

    /* dup until failure or probe limit reached */
    loop {
        /* don't go beyond RLIMIT_NOFILE */
        if getrlimit_status == 0 && highestfd >= rlim.rlim_cur as c_int - 1 {
            break;
        }

        let thisfd = dup(2);
        if thisfd < 0 {
            /* Expect EMFILE or ENFILE, else it's fishy */
            if errno() != EMFILE && errno() != ENFILE {
                elog!(WARNING, "duplicating stderr file descriptor failed after {} successes: errno={}", used, errno());
            }
            break;
        }

        if used >= size {
            size *= 2;
            fd = realloc(fd as *mut c_void, (size as usize) * core::mem::size_of::<c_int>()) as *mut c_int;
        }
        *fd.add(used as usize) = thisfd;
        used += 1;

        if highestfd < thisfd {
            highestfd = thisfd;
        }

        if used >= max_to_probe {
            break;
        }
    }

    /* release the files we opened */
    let mut j = 0;
    while j < used {
        close(*fd.add(j as usize));
        j += 1;
    }

    pfree(fd as *mut c_void);

    /*
     * Return results.  usable_fds is just the number of successful dups.
     */
    *usable_fds = used;
    *already_open = highestfd + 1 - used;
}

// pfree stub (utils/palloc.h)
// TODO(pg-port): wire up memory manager
unsafe fn pfree(_ptr: *mut c_void) {}

// palloc stub
// TODO(pg-port): wire up memory manager
unsafe fn palloc(size: usize) -> *mut c_void {
    malloc(size)
}

// repalloc stub
// TODO(pg-port): wire up memory manager
unsafe fn repalloc(ptr: *mut c_void, size: usize) -> *mut c_void {
    realloc(ptr, size)
}

// palloc0 stub
unsafe fn palloc0(size: usize) -> *mut c_void {
    let p = malloc(size);
    if !p.is_null() {
        core::ptr::write_bytes(p as *mut u8, 0, size);
    }
    p
}

/*
 * set_max_safe_fds
 *   Determine number of file descriptors that fd.c is allowed to use
 */
pub unsafe fn set_max_safe_fds() {
    let mut usable_fds: c_int = 0;
    let mut already_open: c_int = 0;

    count_usable_fds(max_files_per_process, &raw mut usable_fds, &raw mut already_open);

    max_safe_fds = if usable_fds < max_files_per_process { usable_fds } else { max_files_per_process };

    /*
     * Take off the FDs reserved for system() etc.
     */
    max_safe_fds -= NUM_RESERVED_FDS;

    /*
     * Make sure we still have enough to get by.
     */
    if max_safe_fds < FD_MINFREE {
        ereport!(
            FATAL,
            errmsg!(
                "insufficient file descriptors available to start server process: \
                 system allows {}, server needs at least {}, {} files are already open",
                max_safe_fds + NUM_RESERVED_FDS,
                FD_MINFREE + NUM_RESERVED_FDS,
                already_open
            )
        );
        unreachable!();
    }

    elog!(DEBUG2, "max_safe_fds = {}, usable_fds = {}, already_open = {}", max_safe_fds, usable_fds, already_open);
}

// ---------------------------------------------------------------------------
// BasicOpenFile / BasicOpenFilePerm
// ---------------------------------------------------------------------------

/*
 * Open a file with BasicOpenFilePerm() and pass default file mode for the
 * fileMode parameter.
 */
pub unsafe fn BasicOpenFile(fileName: *const c_char, fileFlags: c_int) -> c_int {
    BasicOpenFilePerm(fileName, fileFlags, pg_file_create_mode)
}

/*
 * BasicOpenFilePerm --- same as open(2) except can free other FDs if needed
 */
pub unsafe fn BasicOpenFilePerm(fileName: *const c_char, fileFlags: c_int, fileMode: mode_t) -> c_int {
    let mut fd: c_int;

    'tryAgain: loop {
        /*
         * Darwin: PG_O_DIRECT is simulated via F_NOCACHE.  Strip the sentinel
         * bit before calling open(), then apply F_NOCACHE if requested.
         */
        fd = open(fileName, fileFlags & !PG_O_DIRECT, fileMode as c_int);

        if fd >= 0 {
            /* Apply F_NOCACHE if PG_O_DIRECT was requested (Darwin path) */
            if fileFlags & PG_O_DIRECT != 0 {
                if fcntl(fd, F_NOCACHE, 1) < 0 {
                    let save_errno = errno();
                    close(fd);
                    set_errno(save_errno);
                    return -1;
                }
            }

            return fd; /* success! */
        }

        if errno() == EMFILE || errno() == ENFILE {
            let save_errno = errno();

            ereport!(
                LOG,
                errmsg!("out of file descriptors: errno={}; release and retry", errno())
            );
            set_errno(0);
            if ReleaseLruFile() {
                continue 'tryAgain;
            }
            set_errno(save_errno);
        }

        return -1; /* failure */
    }
}

// ---------------------------------------------------------------------------
// AcquireExternalFD / ReserveExternalFD / ReleaseExternalFD
// ---------------------------------------------------------------------------

/*
 * AcquireExternalFD - attempt to reserve an external file descriptor
 */
pub unsafe fn AcquireExternalFD() -> bool {
    if numExternalFDs < max_safe_fds / 3 {
        ReserveExternalFD();
        return true;
    }
    set_errno(EMFILE);
    false
}

/*
 * ReserveExternalFD - report external consumption of a file descriptor
 */
pub unsafe fn ReserveExternalFD() {
    ReleaseLruFiles();
    numExternalFDs += 1;
}

/*
 * ReleaseExternalFD - report release of an external file descriptor
 */
pub unsafe fn ReleaseExternalFD() {
    Assert!(numExternalFDs > 0);
    numExternalFDs -= 1;
}

// ---------------------------------------------------------------------------
// LRU ring management: Delete, LruDelete, Insert, LruInsert,
// ReleaseLruFile, ReleaseLruFiles, AllocateVfd, FreeVfd, FileAccess
// ---------------------------------------------------------------------------

#[cfg(FDDEBUG)]
unsafe fn _dump_lru() {
    let mut mru: c_int = (*VfdCache).lruLessRecently;
    let mut vfdP: *mut Vfd = VfdCache.add(mru as usize);
    let mut buf = String::new();

    buf.push_str(&format!("LRU: MOST {} ", mru));
    while mru != 0 {
        mru = (*vfdP).lruLessRecently;
        vfdP = VfdCache.add(mru as usize);
        buf.push_str(&format!("{} ", mru));
    }
    buf.push_str("LEAST");
    elog!(LOG, "{}", buf);
}

unsafe fn Delete(file: File) {
    Assert!(file != 0);

    let vfdP = &mut *VfdCache.add(file as usize);

    (*VfdCache.add(vfdP.lruLessRecently as usize)).lruMoreRecently = vfdP.lruMoreRecently;
    (*VfdCache.add(vfdP.lruMoreRecently as usize)).lruLessRecently = vfdP.lruLessRecently;
}

unsafe fn LruDelete(file: File) {
    Assert!(file != 0);

    let vfdP = &mut *VfdCache.add(file as usize);

    pgaio_closing_fd(vfdP.fd);

    /*
     * Close the file.  We aren't expecting this to fail; if it does, better
     * to leak the FD than to mess up our internal state.
     */
    if close(vfdP.fd) != 0 {
        elog!(
            if vfdP.fdstate & FD_TEMP_FILE_LIMIT != 0 { LOG } else { data_sync_elevel(LOG) },
            "could not close file \"{}\": errno={}",
            std::ffi::CStr::from_ptr(vfdP.fileName).to_string_lossy(),
            errno()
        );
    }
    vfdP.fd = VFD_CLOSED;
    nfile -= 1;

    /* delete the vfd record from the LRU ring */
    Delete(file);
}

unsafe fn Insert(file: File) {
    Assert!(file != 0);

    let vfdP = &mut *VfdCache.add(file as usize);

    vfdP.lruMoreRecently = 0;
    vfdP.lruLessRecently = (*VfdCache).lruLessRecently;
    (*VfdCache).lruLessRecently = file;
    (*VfdCache.add(vfdP.lruLessRecently as usize)).lruMoreRecently = file;
}

/* returns 0 on success, -1 on re-open failure (with errno set) */
unsafe fn LruInsert(file: File) -> c_int {
    Assert!(file != 0);

    let vfdP = &mut *VfdCache.add(file as usize);

    if FileIsNotOpen(file) {
        /* Close excess kernel FDs. */
        ReleaseLruFiles();

        /*
         * The open could still fail for lack of file descriptors, eg due to
         * overall system file table being full.
         */
        vfdP.fd = BasicOpenFilePerm(vfdP.fileName, vfdP.fileFlags, vfdP.fileMode);
        if vfdP.fd < 0 {
            return -1;
        } else {
            nfile += 1;
        }
    }

    /* put it at the head of the Lru ring */
    Insert(file);

    0
}

/*
 * Release one kernel FD by closing the least-recently-used VFD.
 */
unsafe fn ReleaseLruFile() -> bool {
    if nfile > 0 {
        /*
         * There are opened files and so there should be at least one used vfd
         * in the ring.
         */
        Assert!((*VfdCache).lruMoreRecently != 0);
        LruDelete((*VfdCache).lruMoreRecently);
        return true; /* freed a file */
    }
    false /* no files available to free */
}

/*
 * Release kernel FDs as needed to get under the max_safe_fds limit.
 */
unsafe fn ReleaseLruFiles() {
    while nfile + numAllocatedDescs + numExternalFDs >= max_safe_fds {
        if !ReleaseLruFile() {
            break;
        }
    }
}

unsafe fn AllocateVfd() -> File {
    let mut i: Index;
    let file: File;

    Assert!(SizeVfdCache > 0); /* InitFileAccess not called? */

    if (*VfdCache).nextFree == 0 {
        /*
         * The free list is empty so it is time to increase the size of the
         * array.  We choose to double it each time this happens. However,
         * there's not much point in starting *real* small.
         */
        let mut newCacheSize: Size = SizeVfdCache * 2;
        let newVfdCache: *mut Vfd;

        if newCacheSize < 32 {
            newCacheSize = 32;
        }

        /*
         * Be careful not to clobber VfdCache ptr if realloc fails.
         */
        newVfdCache = realloc(VfdCache as *mut c_void, core::mem::size_of::<Vfd>() * newCacheSize) as *mut Vfd;
        if newVfdCache.is_null() {
            ereport!(ERROR, errmsg!("out of memory"));
            unreachable!();
        }
        VfdCache = newVfdCache;

        /*
         * Initialize the new entries and link them into the free list.
         */
        i = SizeVfdCache;
        while i < newCacheSize {
            core::ptr::write_bytes(VfdCache.add(i), 0, 1);
            (*VfdCache.add(i)).nextFree = (i + 1) as File;
            (*VfdCache.add(i)).fd = VFD_CLOSED;
            i += 1;
        }
        (*VfdCache.add(newCacheSize - 1)).nextFree = 0;
        (*VfdCache).nextFree = SizeVfdCache as File;

        /* Record the new size */
        SizeVfdCache = newCacheSize;
    }

    file = (*VfdCache).nextFree;
    (*VfdCache).nextFree = (*VfdCache.add(file as usize)).nextFree;

    file
}

unsafe fn FreeVfd(file: File) {
    let vfdP = &mut *VfdCache.add(file as usize);

    if !vfdP.fileName.is_null() {
        free(vfdP.fileName as *mut c_void);
        vfdP.fileName = null_mut();
    }
    vfdP.fdstate = 0x0;

    vfdP.nextFree = (*VfdCache).nextFree;
    (*VfdCache).nextFree = file;
}

/* returns 0 on success, -1 on re-open failure (with errno set) */
unsafe fn FileAccess(file: File) -> c_int {
    /*
     * Is the file open?  If not, open it and put it at the head of the LRU
     * ring (possibly closing the least recently used file to get an FD).
     */
    if FileIsNotOpen(file) {
        let returnValue = LruInsert(file);
        if returnValue != 0 {
            return returnValue;
        }
    } else if (*VfdCache).lruLessRecently != file {
        /*
         * We now know that the file is open and that it is not the last one
         * accessed, so we need to move it to the head of the Lru ring.
         */
        Delete(file);
        Insert(file);
    }

    0
}

/*
 * Called whenever a temporary file is deleted to report its size.
 */
unsafe fn ReportTemporaryFileUsage(path: *const c_char, size: off_t) {
    pgstat_report_tempfile(size);

    if log_temp_files >= 0 {
        if (size / 1024) >= log_temp_files as off_t {
            ereport!(
                LOG,
                errmsg!(
                    "temporary file: path \"{}\", size {}",
                    std::ffi::CStr::from_ptr(path).to_string_lossy(),
                    size as u64
                )
            );
        }
    }
}

/*
 * Called to register a temporary file for automatic close.
 * ResourceOwnerEnlarge(CurrentResourceOwner) must have been called
 * before the file was opened.
 */
unsafe fn RegisterTemporaryFile(file: File) {
    ResourceOwnerRememberFile(CurrentResourceOwner(), file);
    (*VfdCache.add(file as usize)).resowner = CurrentResourceOwner();

    /* Backup mechanism for closing at end of xact. */
    (*VfdCache.add(file as usize)).fdstate |= FD_CLOSE_AT_EOXACT;
    have_xact_temporary_files = true;
}

// ---------------------------------------------------------------------------
// PathNameOpenFile / PathNameOpenFilePerm
// ---------------------------------------------------------------------------

/*
 * Open a file with PathNameOpenFilePerm() and pass default file mode.
 */
pub unsafe fn PathNameOpenFile(fileName: *const c_char, fileFlags: c_int) -> File {
    PathNameOpenFilePerm(fileName, fileFlags, pg_file_create_mode)
}

/*
 * open a file in an arbitrary directory
 */
pub unsafe fn PathNameOpenFilePerm(fileName: *const c_char, mut fileFlags: c_int, fileMode: mode_t) -> File {
    let fnamecopy: *mut c_char;
    let file: File;
    let vfdP: *mut Vfd;

    /*
     * We need a malloc'd copy of the file name; fail cleanly if no room.
     */
    fnamecopy = strdup(fileName);
    if fnamecopy.is_null() {
        ereport!(ERROR, errmsg!("out of memory"));
        unreachable!();
    }

    file = AllocateVfd();
    vfdP = VfdCache.add(file as usize);

    /* Close excess kernel FDs. */
    ReleaseLruFiles();

    /*
     * Descriptors managed by VFDs are implicitly marked O_CLOEXEC.
     */
    fileFlags |= O_CLOEXEC;

    (*vfdP).fd = BasicOpenFilePerm(fileName, fileFlags, fileMode);

    if (*vfdP).fd < 0 {
        let save_errno = errno();

        FreeVfd(file);
        free(fnamecopy as *mut c_void);
        set_errno(save_errno);
        return -1;
    }
    nfile += 1;

    (*vfdP).fileName = fnamecopy;
    /* Saved flags are adjusted to be OK for re-opening file */
    (*vfdP).fileFlags = fileFlags & !(O_CREAT | O_TRUNC | O_EXCL);
    (*vfdP).fileMode = fileMode;
    (*vfdP).fileSize = 0;
    (*vfdP).fdstate = 0x0;
    (*vfdP).resowner = null_mut();

    Insert(file);

    file
}


// ---------------------------------------------------------------------------
// Temporary file / directory creation
// ---------------------------------------------------------------------------

/*
 * Create directory 'directory'.  If necessary, create 'basedir', which must
 * be the directory above it.
 */
pub unsafe fn PathNameCreateTemporaryDir(basedir: *const c_char, directory: *const c_char) {
    if MakePGDirectory(directory) < 0 {
        if errno() == EEXIST {
            return;
        }

        if MakePGDirectory(basedir) < 0 && errno() != EEXIST {
            ereport!(
                ERROR,
                errmsg!(
                    "cannot create temporary directory \"{}\": errno={}",
                    std::ffi::CStr::from_ptr(basedir).to_string_lossy(),
                    errno()
                )
            );
        }

        /* Try again. */
        if MakePGDirectory(directory) < 0 && errno() != EEXIST {
            ereport!(
                ERROR,
                errmsg!(
                    "cannot create temporary subdirectory \"{}\": errno={}",
                    std::ffi::CStr::from_ptr(directory).to_string_lossy(),
                    errno()
                )
            );
        }
    }
}

/*
 * Delete a directory and everything in it, if it exists.
 */
pub unsafe fn PathNameDeleteTemporaryDir(dirname: *const c_char) {
    let mut statbuf: stat_t = core::mem::zeroed();

    /* Silently ignore missing directory. */
    if stat_inode64(dirname, &raw mut statbuf) != 0 && errno() == ENOENT {
        return;
    }

    walkdir(dirname, unlink_if_exists_fname, false, LOG);
}

/*
 * Open a temporary file that will disappear when we close it.
 */
pub unsafe fn OpenTemporaryFile(interXact: bool) -> File {
    let mut file: File = 0;

    #[cfg(debug_assertions)]
    Assert!(temporary_files_allowed); /* check temp file access is up */

    /*
     * Make sure the current resource owner has space for this File before we
     * open it, if we'll be registering it below.
     */
    if !interXact {
        ResourceOwnerEnlarge(CurrentResourceOwner());
    }

    /*
     * If some temp tablespace(s) have been given to us, try to use the next
     * one.
     */
    if numTempTableSpaces > 0 && !interXact {
        let tblspcOid = GetNextTempTableSpace();

        if tblspcOid != InvalidOid {
            file = OpenTemporaryFileInTablespace(tblspcOid, false);
        }
    }

    /*
     * If not, or if tablespace is bad, create in database's default
     * tablespace.
     */
    if file <= 0 {
        let ts = if MyDatabaseTableSpace() != 0 { MyDatabaseTableSpace() } else { DEFAULTTABLESPACE_OID };
        file = OpenTemporaryFileInTablespace(ts, true);
    }

    /* Mark it for deletion at close and temporary file size limit */
    (*VfdCache.add(file as usize)).fdstate |= FD_DELETE_AT_CLOSE | FD_TEMP_FILE_LIMIT;

    /* Register it with the current resource owner */
    if !interXact {
        RegisterTemporaryFile(file);
    }

    file
}

/*
 * Return the path of the temp directory in a given tablespace.
 */
pub unsafe fn TempTablespacePath(path: *mut c_char, tablespace: Oid) {
    if tablespace == InvalidOid
        || tablespace == DEFAULTTABLESPACE_OID
        || tablespace == GLOBALTABLESPACE_OID
    {
        snprintf(path, MAXPGPATH, b"base/%s\0".as_ptr() as *const c_char,
                 PG_TEMP_FILES_DIR.as_ptr() as *const c_char);
    } else {
        /* All other tablespaces are accessed via symlinks */
        snprintf(path, MAXPGPATH,
                 b"%s/%u/%s/%s\0".as_ptr() as *const c_char,
                 PG_TBLSPC_DIR.as_ptr() as *const c_char,
                 tablespace,
                 TABLESPACE_VERSION_DIRECTORY.as_ptr() as *const c_char,
                 PG_TEMP_FILES_DIR.as_ptr() as *const c_char);
    }
}

/*
 * Open a temporary file in a specific tablespace.
 * Subroutine for OpenTemporaryFile.
 */
unsafe fn OpenTemporaryFileInTablespace(tblspcOid: Oid, rejectError: bool) -> File {
    let mut tempdirpath:  [c_char; MAXPGPATH] = [0; MAXPGPATH];
    let mut tempfilepath: [c_char; MAXPGPATH] = [0; MAXPGPATH];
    let mut file: File;

    TempTablespacePath(tempdirpath.as_mut_ptr(), tblspcOid);

    /*
     * Generate a tempfile name that should be unique within the current
     * database instance.
     */
    snprintf(tempfilepath.as_mut_ptr(), MAXPGPATH,
             b"%s/%s%d.%ld\0".as_ptr() as *const c_char,
             tempdirpath.as_ptr(),
             PG_TEMP_FILE_PREFIX.as_ptr() as *const c_char,
             MyProcPid(),
             tempFileCounter);
    tempFileCounter += 1;

    /*
     * Open the file.  Note: we don't use O_EXCL, in case there is an orphaned
     * temp file that can be reused.
     */
    file = PathNameOpenFile(tempfilepath.as_ptr(), O_RDWR | O_CREAT | O_TRUNC | PG_BINARY);
    if file <= 0 {
        /*
         * We might need to create the tablespace's tempfile directory, if no
         * one has yet done so.
         */
        MakePGDirectory(tempdirpath.as_ptr()); /* ignore error */

        file = PathNameOpenFile(tempfilepath.as_ptr(), O_RDWR | O_CREAT | O_TRUNC | PG_BINARY);
        if file <= 0 && rejectError {
            elog!(ERROR, "could not create temporary file \"{}\": errno={}",
                  std::ffi::CStr::from_ptr(tempfilepath.as_ptr()).to_string_lossy(),
                  errno());
        }
    }

    file
}

/*
 * Create a new file.  The directory containing it must already exist.  Files
 * created this way are subject to temp_file_limit and are automatically
 * closed at end of transaction.
 */
pub unsafe fn PathNameCreateTemporaryFile(path: *const c_char, error_on_failure: bool) -> File {
    let file: File;

    #[cfg(debug_assertions)]
    Assert!(temporary_files_allowed);

    ResourceOwnerEnlarge(CurrentResourceOwner());

    /*
     * Open the file.  Note: we don't use O_EXCL, in case there is an orphaned
     * temp file that can be reused.
     */
    file = PathNameOpenFile(path, O_RDWR | O_CREAT | O_TRUNC | PG_BINARY);
    if file <= 0 {
        if error_on_failure {
            ereport!(
                ERROR,
                errmsg!(
                    "could not create temporary file \"{}\": errno={}",
                    std::ffi::CStr::from_ptr(path).to_string_lossy(),
                    errno()
                )
            );
        } else {
            return file;
        }
    }

    /* Mark it for temp_file_limit accounting. */
    (*VfdCache.add(file as usize)).fdstate |= FD_TEMP_FILE_LIMIT;

    /* Register it for automatic close. */
    RegisterTemporaryFile(file);

    file
}

/*
 * Open a file that was created with PathNameCreateTemporaryFile, possibly in
 * another backend.
 */
pub unsafe fn PathNameOpenTemporaryFile(path: *const c_char, mode: c_int) -> File {
    let file: File;

    #[cfg(debug_assertions)]
    Assert!(temporary_files_allowed);

    ResourceOwnerEnlarge(CurrentResourceOwner());

    file = PathNameOpenFile(path, mode | PG_BINARY);

    /* If no such file, then we don't raise an error. */
    if file <= 0 && errno() != ENOENT {
        ereport!(
            ERROR,
            errmsg!(
                "could not open temporary file \"{}\": errno={}",
                std::ffi::CStr::from_ptr(path).to_string_lossy(),
                errno()
            )
        );
    }

    if file > 0 {
        /* Register it for automatic close. */
        RegisterTemporaryFile(file);
    }

    file
}

/*
 * Delete a file by pathname.  Return true if the file existed, false if
 * didn't.
 */
pub unsafe fn PathNameDeleteTemporaryFile(path: *const c_char, error_on_failure: bool) -> bool {
    let mut filestats: stat_t = core::mem::zeroed();
    let stat_errno: c_int;

    /* Get the final size for pgstat reporting. */
    if stat_inode64(path, &raw mut filestats) != 0 {
        stat_errno = errno();
    } else {
        stat_errno = 0;
    }

    if stat_errno == ENOENT {
        return false;
    }

    if unlink(path) < 0 {
        if errno() != ENOENT {
            ereport!(
                if error_on_failure { ERROR } else { LOG },
                errmsg!(
                    "could not unlink temporary file \"{}\": errno={}",
                    std::ffi::CStr::from_ptr(path).to_string_lossy(),
                    errno()
                )
            );
        }
        return false;
    }

    if stat_errno == 0 {
        ReportTemporaryFileUsage(path, filestats.st_size);
    } else {
        set_errno(stat_errno);
        ereport!(
            LOG,
            errmsg!(
                "could not stat file \"{}\": errno={}",
                std::ffi::CStr::from_ptr(path).to_string_lossy(),
                errno()
            )
        );
    }

    true
}

// ---------------------------------------------------------------------------
// FileClose and file I/O operations
// ---------------------------------------------------------------------------

/*
 * close a file when done with it
 */
pub unsafe fn FileClose(file: File) {
    let vfdP: *mut Vfd;

    Assert!(FileIsValid(file));

    vfdP = VfdCache.add(file as usize);

    if !FileIsNotOpen(file) {
        pgaio_closing_fd((*vfdP).fd);

        /* close the file */
        if close((*vfdP).fd) != 0 {
            elog!(
                if (*vfdP).fdstate & FD_TEMP_FILE_LIMIT != 0 { LOG } else { data_sync_elevel(LOG) },
                "could not close file \"{}\": errno={}",
                std::ffi::CStr::from_ptr((*vfdP).fileName).to_string_lossy(),
                errno()
            );
        }

        nfile -= 1;
        (*vfdP).fd = VFD_CLOSED;

        /* remove the file from the lru ring */
        Delete(file);
    }

    if (*vfdP).fdstate & FD_TEMP_FILE_LIMIT != 0 {
        /* Subtract its size from current usage (do first in case of error) */
        temporary_files_size -= (*vfdP).fileSize as u64;
        (*vfdP).fileSize = 0;
    }

    /*
     * Delete the file if it was temporary, and make a log entry if wanted
     */
    if (*vfdP).fdstate & FD_DELETE_AT_CLOSE != 0 {
        let mut filestats: stat_t = core::mem::zeroed();
        let stat_errno: c_int;

        /*
         * Reset the flag to ensure that we can't get into an infinite loop.
         */
        (*vfdP).fdstate &= !FD_DELETE_AT_CLOSE;

        /* first try the stat() */
        if stat_inode64((*vfdP).fileName, &raw mut filestats) != 0 {
            stat_errno = errno();
        } else {
            stat_errno = 0;
        }

        /* in any case do the unlink */
        if unlink((*vfdP).fileName) != 0 {
            ereport!(
                LOG,
                errmsg!(
                    "could not delete file \"{}\": errno={}",
                    std::ffi::CStr::from_ptr((*vfdP).fileName).to_string_lossy(),
                    errno()
                )
            );
        }

        /* and last report the stat results */
        if stat_errno == 0 {
            ReportTemporaryFileUsage((*vfdP).fileName, filestats.st_size);
        } else {
            set_errno(stat_errno);
            ereport!(
                LOG,
                errmsg!(
                    "could not stat file \"{}\": errno={}",
                    std::ffi::CStr::from_ptr((*vfdP).fileName).to_string_lossy(),
                    errno()
                )
            );
        }
    }

    /* Unregister it from the resource owner */
    if !(*vfdP).resowner.is_null() {
        ResourceOwnerForgetFile((*vfdP).resowner, file);
    }

    /* Return the Vfd slot to the free list */
    FreeVfd(file);
}

/*
 * FilePrefetch - initiate asynchronous read of a given range of the file.
 *
 * Returns 0 on success, otherwise an errno error code.
 */
pub unsafe fn FilePrefetch(file: File, offset: off_t, amount: off_t, wait_event_info: u32) -> c_int {
    Assert!(FileIsValid(file));

    /* USE_POSIX_FADVISE / POSIX_FADV_WILLNEED -- not available on Darwin */
    #[cfg(any())]
    {
        // posix_fadvise path omitted
    }

    /* Darwin: use F_RDADVISE */
    {
        let mut ra = radvisory { ra_offset: offset, ra_count: amount as c_int };
        let returnCode: c_int;

        let rc = FileAccess(file);
        if rc < 0 {
            return rc;
        }

        pgstat_report_wait_start(wait_event_info);
        returnCode = fcntl((*VfdCache.add(file as usize)).fd, F_RDADVISE, &raw mut ra);
        pgstat_report_wait_end();

        if returnCode != -1 { 0 } else { errno() }
    }
}

pub unsafe fn FileWriteback(file: File, offset: off_t, nbytes: off_t, wait_event_info: u32) {
    Assert!(FileIsValid(file));

    if nbytes <= 0 {
        return;
    }

    if (*VfdCache.add(file as usize)).fileFlags & PG_O_DIRECT != 0 {
        return;
    }

    let returnCode = FileAccess(file);
    if returnCode < 0 {
        return;
    }

    pgstat_report_wait_start(wait_event_info);
    pg_flush_data((*VfdCache.add(file as usize)).fd, offset, nbytes);
    pgstat_report_wait_end();
}

pub unsafe fn FileReadV(file: File, iov: *const iovec, iovcnt: c_int, offset: off_t, wait_event_info: u32) -> ssize_t {
    let mut returnCode: ssize_t;
    let vfdP: *mut Vfd;

    Assert!(FileIsValid(file));

    returnCode = FileAccess(file) as ssize_t;
    if returnCode < 0 {
        return returnCode;
    }

    vfdP = VfdCache.add(file as usize);

    loop {
        pgstat_report_wait_start(wait_event_info);
        returnCode = pg_preadv((*vfdP).fd, iov, iovcnt, offset);
        pgstat_report_wait_end();

        if returnCode < 0 {
            /* WIN32 branch omitted */
            #[cfg(any())] { /* ERROR_NO_SYSTEM_RESOURCES path */ }
            /* OK to retry if interrupted */
            if errno() == EINTR {
                continue;
            }
        }
        break;
    }

    returnCode
}

pub unsafe fn FileStartReadV(ioh: *mut PgAioHandle, file: File, iovcnt: c_int, offset: off_t, wait_event_info: u32) -> c_int {
    let returnCode: c_int;
    let vfdP: *mut Vfd;

    Assert!(FileIsValid(file));

    returnCode = FileAccess(file);
    if returnCode < 0 {
        return returnCode;
    }

    vfdP = VfdCache.add(file as usize);

    pgaio_io_start_readv(ioh, (*vfdP).fd, iovcnt, offset);

    0
}

pub unsafe fn FileWriteV(file: File, iov: *const iovec, iovcnt: c_int, offset: off_t, wait_event_info: u32) -> ssize_t {
    let mut returnCode: ssize_t;
    let vfdP: *mut Vfd;

    Assert!(FileIsValid(file));

    returnCode = FileAccess(file) as ssize_t;
    if returnCode < 0 {
        return returnCode;
    }

    vfdP = VfdCache.add(file as usize);

    /*
     * If enforcing temp_file_limit and it's a temp file, check to see if the
     * write would overrun temp_file_limit.
     */
    if temp_file_limit >= 0 && ((*vfdP).fdstate & FD_TEMP_FILE_LIMIT != 0) {
        let mut past_write: off_t = offset;
        let mut i = 0;
        while i < iovcnt {
            past_write += (*iov.add(i as usize)).iov_len as off_t;
            i += 1;
        }

        if past_write > (*vfdP).fileSize {
            let mut newTotal: u64 = temporary_files_size;
            newTotal += (past_write - (*vfdP).fileSize) as u64;
            if newTotal > temp_file_limit as u64 * 1024 {
                ereport!(
                    ERROR,
                    errmsg!("temporary file size exceeds \"temp_file_limit\" ({}kB)", temp_file_limit)
                );
            }
        }
    }

    loop {
        pgstat_report_wait_start(wait_event_info);
        returnCode = pg_pwritev((*vfdP).fd, iov, iovcnt, offset);
        pgstat_report_wait_end();

        if returnCode >= 0 {
            /*
             * Some callers expect short writes to set errno.
             */
            set_errno(ENOSPC);

            /* Maintain fileSize and temporary_files_size if it's a temp file. */
            if (*vfdP).fdstate & FD_TEMP_FILE_LIMIT != 0 {
                let past_write: off_t = offset + returnCode as off_t;
                if past_write > (*vfdP).fileSize {
                    temporary_files_size += (past_write - (*vfdP).fileSize) as u64;
                    (*vfdP).fileSize = past_write;
                }
            }
        } else {
            /* WIN32 branch omitted */
            #[cfg(any())] { /* ERROR_NO_SYSTEM_RESOURCES path */ }
            /* OK to retry if interrupted */
            if errno() == EINTR {
                continue;
            }
        }
        break;
    }

    returnCode
}

pub unsafe fn FileSync(file: File, wait_event_info: u32) -> c_int {
    let returnCode: c_int;

    Assert!(FileIsValid(file));

    let rc = FileAccess(file);
    if rc < 0 {
        return rc;
    }

    pgstat_report_wait_start(wait_event_info);
    let returnCode = pg_fsync((*VfdCache.add(file as usize)).fd);
    pgstat_report_wait_end();

    returnCode
}

/*
 * Zero a region of the file.
 * Returns 0 on success, -1 otherwise.
 */
pub unsafe fn FileZero(file: File, offset: off_t, amount: off_t, wait_event_info: u32) -> c_int {
    let written: ssize_t;

    Assert!(FileIsValid(file));

    let rc = FileAccess(file);
    if rc < 0 {
        return rc;
    }

    pgstat_report_wait_start(wait_event_info);
    written = pg_pwrite_zeros((*VfdCache.add(file as usize)).fd, amount, offset);
    pgstat_report_wait_end();

    if written < 0 {
        return -1;
    } else if written != amount as ssize_t {
        /* if errno is unset, assume problem is no disk space */
        if errno() == 0 {
            set_errno(ENOSPC);
        }
        return -1;
    }

    0
}

/*
 * Try to reserve file space with posix_fallocate().  On Darwin,
 * posix_fallocate is not available, so this always falls through to FileZero.
 */
pub unsafe fn FileFallocate(file: File, offset: off_t, amount: off_t, wait_event_info: u32) -> c_int {
    /* HAVE_POSIX_FALLOCATE -- Darwin lacks it; stub returns EINVAL */
    #[cfg(any())]
    {
        // posix_fallocate path omitted (Darwin)
    }

    FileZero(file, offset, amount, wait_event_info)
}

pub unsafe fn FileSize(file: File) -> off_t {
    Assert!(FileIsValid(file));

    if FileIsNotOpen(file) {
        if FileAccess(file) < 0 {
            return -1 as off_t;
        }
    }

    lseek((*VfdCache.add(file as usize)).fd, 0, SEEK_END)
}

pub unsafe fn FileTruncate(file: File, offset: off_t, wait_event_info: u32) -> c_int {
    let returnCode: c_int;

    Assert!(FileIsValid(file));

    let rc = FileAccess(file);
    if rc < 0 {
        return rc;
    }

    pgstat_report_wait_start(wait_event_info);
    let returnCode = pg_ftruncate((*VfdCache.add(file as usize)).fd, offset);
    pgstat_report_wait_end();

    if returnCode == 0 && (*VfdCache.add(file as usize)).fileSize > offset {
        /* adjust our state for truncation of a temp file */
        Assert!((*VfdCache.add(file as usize)).fdstate & FD_TEMP_FILE_LIMIT != 0);
        temporary_files_size -= ((*VfdCache.add(file as usize)).fileSize - offset) as u64;
        (*VfdCache.add(file as usize)).fileSize = offset;
    }

    returnCode
}

/*
 * Return the pathname associated with an open file.
 */
pub unsafe fn FilePathName(file: File) -> *mut c_char {
    Assert!(FileIsValid(file));
    (*VfdCache.add(file as usize)).fileName
}

/*
 * Return the raw file descriptor of an opened file.
 */
pub unsafe fn FileGetRawDesc(file: File) -> c_int {
    let returnCode = FileAccess(file);
    if returnCode < 0 {
        return returnCode;
    }
    Assert!(FileIsValid(file));
    (*VfdCache.add(file as usize)).fd
}

/*
 * FileGetRawFlags - returns the file flags on open(2)
 */
pub unsafe fn FileGetRawFlags(file: File) -> c_int {
    Assert!(FileIsValid(file));
    (*VfdCache.add(file as usize)).fileFlags
}

/*
 * FileGetRawMode - returns the mode bitmask passed to open(2)
 */
pub unsafe fn FileGetRawMode(file: File) -> mode_t {
    Assert!(FileIsValid(file));
    (*VfdCache.add(file as usize)).fileMode
}


// ---------------------------------------------------------------------------
// AllocateFile / FreeFile / OpenTransientFile / CloseTransientFile
// AllocateDir / ReadDir / ReadDirExtended / FreeDir / OpenPipeStream / ClosePipeStream
// ---------------------------------------------------------------------------

/*
 * Make room for another allocatedDescs[] array entry if needed and possible.
 * Returns true if an array element is available.
 */
unsafe fn reserveAllocatedDesc() -> bool {
    let newDescs: *mut AllocateDesc;
    let newMax: c_int;

    /* Quick out if array already has a free slot. */
    if numAllocatedDescs < maxAllocatedDescs {
        return true;
    }

    if allocatedDescs.is_null() {
        newMax = FD_MINFREE / 3;
        let ptr = malloc((newMax as usize) * core::mem::size_of::<AllocateDesc>()) as *mut AllocateDesc;
        /* Out of memory already?  Treat as fatal error. */
        if ptr.is_null() {
            ereport!(ERROR, errmsg!("out of memory"));
            unreachable!();
        }
        allocatedDescs = ptr;
        maxAllocatedDescs = newMax;
        return true;
    }

    newMax = max_safe_fds / 3;
    if newMax > maxAllocatedDescs {
        let ptr = realloc(allocatedDescs as *mut c_void,
                          (newMax as usize) * core::mem::size_of::<AllocateDesc>()) as *mut AllocateDesc;
        if ptr.is_null() {
            return false; /* Treat out-of-memory as non-fatal. */
        }
        allocatedDescs = ptr;
        maxAllocatedDescs = newMax;
        return true;
    }

    /* Can't enlarge allocatedDescs[] any more. */
    false
}

/*
 * Routines that want to use stdio (ie, FILE*) should use AllocateFile.
 */
pub unsafe fn AllocateFile(name: *const c_char, mode: *const c_char) -> *mut c_void /* FILE* */ {
    let mut file: *mut c_void;

    /* Can we allocate another non-virtual FD? */
    if !reserveAllocatedDesc() {
        ereport!(
            ERROR,
            errmsg!(
                "exceeded maxAllocatedDescs ({}) while trying to open file \"{}\"",
                maxAllocatedDescs,
                std::ffi::CStr::from_ptr(name).to_string_lossy()
            )
        );
        unreachable!();
    }

    /* Close excess kernel FDs. */
    ReleaseLruFiles();

    'try_again: loop {
        file = fopen(name, mode);
        if !file.is_null() {
            let desc = &mut *allocatedDescs.add(numAllocatedDescs as usize);
            desc.kind = AllocateDescKind::AllocateDescFile;
            desc.desc.file = file;
            desc.create_subid = GetCurrentSubTransactionId();
            numAllocatedDescs += 1;
            return desc.desc.file;
        }

        if errno() == EMFILE || errno() == ENFILE {
            let save_errno = errno();
            ereport!(LOG, errmsg!("out of file descriptors: errno={}; release and retry", errno()));
            set_errno(0);
            if ReleaseLruFile() {
                continue 'try_again;
            }
            set_errno(save_errno);
        }
        break;
    }

    null_mut()
}

/*
 * Open a file with OpenTransientFilePerm() and pass default file mode.
 */
pub unsafe fn OpenTransientFile(fileName: *const c_char, fileFlags: c_int) -> c_int {
    OpenTransientFilePerm(fileName, fileFlags, pg_file_create_mode)
}

/*
 * Like AllocateFile, but returns an unbuffered fd like open(2)
 */
pub unsafe fn OpenTransientFilePerm(fileName: *const c_char, fileFlags: c_int, fileMode: mode_t) -> c_int {
    let fd: c_int;

    /* Can we allocate another non-virtual FD? */
    if !reserveAllocatedDesc() {
        ereport!(
            ERROR,
            errmsg!(
                "exceeded maxAllocatedDescs ({}) while trying to open file \"{}\"",
                maxAllocatedDescs,
                std::ffi::CStr::from_ptr(fileName).to_string_lossy()
            )
        );
        unreachable!();
    }

    /* Close excess kernel FDs. */
    ReleaseLruFiles();

    let fd = BasicOpenFilePerm(fileName, fileFlags, fileMode);

    if fd >= 0 {
        let desc = &mut *allocatedDescs.add(numAllocatedDescs as usize);
        desc.kind = AllocateDescKind::AllocateDescRawFD;
        desc.desc.fd = fd;
        desc.create_subid = GetCurrentSubTransactionId();
        numAllocatedDescs += 1;
        return fd;
    }

    -1 /* failure */
}

/*
 * Routines that want to initiate a pipe stream should use OpenPipeStream.
 */
pub unsafe fn OpenPipeStream(command: *const c_char, mode: *const c_char) -> *mut c_void /* FILE* */ {
    let mut file: *mut c_void;
    let save_errno: c_int;

    /* Can we allocate another non-virtual FD? */
    if !reserveAllocatedDesc() {
        ereport!(
            ERROR,
            errmsg!(
                "exceeded maxAllocatedDescs ({}) while trying to execute command \"{}\"",
                maxAllocatedDescs,
                std::ffi::CStr::from_ptr(command).to_string_lossy()
            )
        );
        unreachable!();
    }

    /* Close excess kernel FDs. */
    ReleaseLruFiles();

    'try_again: loop {
        fflush(null_mut());
        pqsignal(SIGPIPE, SIG_DFL);
        set_errno(0);
        file = popen(command, mode);
        let se = errno();
        pqsignal(SIGPIPE, SIG_IGN);
        set_errno(se);

        if !file.is_null() {
            let desc = &mut *allocatedDescs.add(numAllocatedDescs as usize);
            desc.kind = AllocateDescKind::AllocateDescPipe;
            desc.desc.file = file;
            desc.create_subid = GetCurrentSubTransactionId();
            numAllocatedDescs += 1;
            return desc.desc.file;
        }

        if errno() == EMFILE || errno() == ENFILE {
            ereport!(LOG, errmsg!("out of file descriptors: errno={}; release and retry", errno()));
            if ReleaseLruFile() {
                continue 'try_again;
            }
            set_errno(se);
        }
        break;
    }

    null_mut()
}

/*
 * Free an AllocateDesc of any type.
 * The argument *must* point into the allocatedDescs[] array.
 */
unsafe fn FreeDesc(desc: *mut AllocateDesc) -> c_int {
    let result: c_int;

    /* Close the underlying object */
    match (*desc).kind {
        AllocateDescKind::AllocateDescFile => {
            result = fclose((*desc).desc.file);
        }
        AllocateDescKind::AllocateDescPipe => {
            result = pclose((*desc).desc.file);
        }
        AllocateDescKind::AllocateDescDir => {
            result = closedir((*desc).desc.dir);
        }
        AllocateDescKind::AllocateDescRawFD => {
            pgaio_closing_fd((*desc).desc.fd);
            result = close((*desc).desc.fd);
        }
    }

    /* Compact storage in the allocatedDescs array */
    numAllocatedDescs -= 1;
    *desc = *allocatedDescs.add(numAllocatedDescs as usize);

    result
}

/*
 * Close a file returned by AllocateFile.
 */
pub unsafe fn FreeFile(file: *mut c_void /* FILE* */) -> c_int {
    let mut i: c_int = numAllocatedDescs;

    /* Remove file from list of allocated files, if it's present */
    loop {
        i -= 1;
        if i < 0 { break; }
        let desc = &mut *allocatedDescs.add(i as usize);
        if desc.kind == AllocateDescKind::AllocateDescFile && desc.desc.file == file {
            return FreeDesc(desc);
        }
    }

    /* Only get here if someone passes us a file not in allocatedDescs */
    elog!(WARNING, "file passed to FreeFile was not obtained from AllocateFile");

    fclose(file)
}

/*
 * Close a file returned by OpenTransientFile.
 */
pub unsafe fn CloseTransientFile(fd: c_int) -> c_int {
    let mut i: c_int = numAllocatedDescs;

    /* Remove fd from list of allocated files, if it's present */
    loop {
        i -= 1;
        if i < 0 { break; }
        let desc = &mut *allocatedDescs.add(i as usize);
        if desc.kind == AllocateDescKind::AllocateDescRawFD && desc.desc.fd == fd {
            return FreeDesc(desc);
        }
    }

    /* Only get here if someone passes us a file not in allocatedDescs */
    elog!(WARNING, "fd passed to CloseTransientFile was not obtained from OpenTransientFile");

    pgaio_closing_fd(fd);

    close(fd)
}

/*
 * AllocateDir --- opendir() with FD release if needed
 */
#[no_mangle]
pub unsafe fn AllocateDir(dirname: *const c_char) -> *mut DIR {
    let mut dir: *mut DIR;

    /* Can we allocate another non-virtual FD? */
    if !reserveAllocatedDesc() {
        ereport!(
            ERROR,
            errmsg!(
                "exceeded maxAllocatedDescs ({}) while trying to open directory \"{}\"",
                maxAllocatedDescs,
                std::ffi::CStr::from_ptr(dirname).to_string_lossy()
            )
        );
        unreachable!();
    }

    /* Close excess kernel FDs. */
    ReleaseLruFiles();

    'try_again: loop {
        dir = opendir(dirname);
        if !dir.is_null() {
            let desc = &mut *allocatedDescs.add(numAllocatedDescs as usize);
            desc.kind = AllocateDescKind::AllocateDescDir;
            desc.desc.dir = dir;
            desc.create_subid = GetCurrentSubTransactionId();
            numAllocatedDescs += 1;
            return desc.desc.dir;
        }

        if errno() == EMFILE || errno() == ENFILE {
            let save_errno = errno();
            ereport!(LOG, errmsg!("out of file descriptors: errno={}; release and retry", errno()));
            set_errno(0);
            if ReleaseLruFile() {
                continue 'try_again;
            }
            set_errno(save_errno);
        }
        break;
    }

    null_mut()
}

/*
 * ReadDir --- readdir() with ereport on error.
 */
#[no_mangle]
pub unsafe fn ReadDir(dir: *mut DIR, dirname: *const c_char) -> *mut dirent {
    ReadDirExtended(dir, dirname, ERROR)
}

/*
 * Alternate version of ReadDir that allows caller to specify the elevel.
 */
pub unsafe fn ReadDirExtended(dir: *mut DIR, dirname: *const c_char, elevel: c_int) -> *mut dirent {
    let dent: *mut dirent;

    /* Give a generic message for AllocateDir failure, if caller didn't */
    if dir.is_null() {
        ereport!(
            elevel,
            errmsg!(
                "could not open directory \"{}\": errno={}",
                std::ffi::CStr::from_ptr(dirname).to_string_lossy(),
                errno()
            )
        );
        return null_mut();
    }

    set_errno(0);
    let dent = readdir(dir);
    if !dent.is_null() {
        return dent;
    }

    if errno() != 0 {
        ereport!(
            elevel,
            errmsg!(
                "could not read directory \"{}\": errno={}",
                std::ffi::CStr::from_ptr(dirname).to_string_lossy(),
                errno()
            )
        );
    }
    null_mut()
}

/*
 * Close a directory opened with AllocateDir.
 */
#[no_mangle]
pub unsafe fn FreeDir(dir: *mut DIR) -> c_int {
    /* Nothing to do if AllocateDir failed */
    if dir.is_null() {
        return 0;
    }

    /* Remove dir from list of allocated dirs, if it's present */
    let mut i: c_int = numAllocatedDescs;
    loop {
        i -= 1;
        if i < 0 { break; }
        let desc = &mut *allocatedDescs.add(i as usize);
        if desc.kind == AllocateDescKind::AllocateDescDir && desc.desc.dir == dir {
            return FreeDesc(desc);
        }
    }

    /* Only get here if someone passes us a dir not in allocatedDescs */
    elog!(WARNING, "dir passed to FreeDir was not obtained from AllocateDir");

    closedir(dir)
}

/*
 * Close a pipe stream returned by OpenPipeStream.
 */
pub unsafe fn ClosePipeStream(file: *mut c_void /* FILE* */) -> c_int {
    let mut i: c_int = numAllocatedDescs;

    /* Remove file from list of allocated files, if it's present */
    loop {
        i -= 1;
        if i < 0 { break; }
        let desc = &mut *allocatedDescs.add(i as usize);
        if desc.kind == AllocateDescKind::AllocateDescPipe && desc.desc.file == file {
            return FreeDesc(desc);
        }
    }

    /* Only get here if someone passes us a file not in allocatedDescs */
    elog!(WARNING, "file passed to ClosePipeStream was not obtained from OpenPipeStream");

    pclose(file)
}

/*
 * closeAllVfds
 *
 * Force all VFDs into the physically-closed state.
 */
pub unsafe fn closeAllVfds() {
    let mut i: Index;

    if SizeVfdCache > 0 {
        Assert!(FileIsNotOpen(0)); /* Make sure ring not corrupted */
        i = 1;
        while i < SizeVfdCache {
            if !FileIsNotOpen(i as File) {
                LruDelete(i as File);
            }
            i += 1;
        }
    }
}

// ---------------------------------------------------------------------------
// Temp tablespace management
// ---------------------------------------------------------------------------

/*
 * SetTempTablespaces
 */
pub unsafe fn SetTempTablespaces(tableSpaces: *mut Oid, numSpaces: c_int) {
    Assert!(numSpaces >= 0);
    tempTableSpaces = tableSpaces;
    numTempTableSpaces = numSpaces;

    /*
     * Select a random starting point in the list.
     */
    if numSpaces > 1 {
        nextTempTableSpace = pg_prng_uint64_range(pg_global_prng_state, 0, (numSpaces - 1) as u64) as c_int;
    } else {
        nextTempTableSpace = 0;
    }
}

/*
 * TempTablespacesAreSet
 */
pub unsafe fn TempTablespacesAreSet() -> bool {
    numTempTableSpaces >= 0
}

/*
 * GetTempTablespaces - populate an array with the OIDs of temp tablespaces.
 * Returns the number of OIDs copied.
 */
pub unsafe fn GetTempTablespaces(tableSpaces: *mut Oid, numSpaces: c_int) -> c_int {
    let mut i: c_int = 0;

    Assert!(TempTablespacesAreSet());
    while i < numTempTableSpaces && i < numSpaces {
        *tableSpaces.add(i as usize) = *tempTableSpaces.add(i as usize);
        i += 1;
    }

    i
}

/*
 * GetNextTempTableSpace
 */
pub unsafe fn GetNextTempTableSpace() -> Oid {
    if numTempTableSpaces > 0 {
        /* Advance nextTempTableSpace counter with wraparound */
        nextTempTableSpace += 1;
        if nextTempTableSpace >= numTempTableSpaces {
            nextTempTableSpace = 0;
        }
        return *tempTableSpaces.add(nextTempTableSpace as usize);
    }
    InvalidOid
}


// ---------------------------------------------------------------------------
// Transaction cleanup: AtEOSubXact_Files, AtEOXact_Files,
// BeforeShmemExit_Files, CleanupTempFiles
// ---------------------------------------------------------------------------

/*
 * AtEOSubXact_Files
 *
 * Take care of subtransaction commit/abort.
 */
pub unsafe fn AtEOSubXact_Files(isCommit: bool, mySubid: SubTransactionId, parentSubid: SubTransactionId) {
    let mut i: Index = 0;
    while i < numAllocatedDescs as usize {
        if (*allocatedDescs.add(i)).create_subid == mySubid {
            if isCommit {
                (*allocatedDescs.add(i)).create_subid = parentSubid;
                i += 1;
            } else {
                /* have to recheck the item after FreeDesc (ugly) */
                FreeDesc(allocatedDescs.add(i));
                /* i stays the same - FreeDesc compacted the array */
            }
        } else {
            i += 1;
        }
    }
}

/*
 * AtEOXact_Files
 *
 * Called during transaction commit or abort.
 */
pub unsafe fn AtEOXact_Files(isCommit: bool) {
    CleanupTempFiles(isCommit, false);
    tempTableSpaces = null_mut();
    numTempTableSpaces = -1;
}

/*
 * BeforeShmemExit_Files
 *
 * before_shmem_exit hook to clean up temp files during backend shutdown.
 */
unsafe extern "C" fn BeforeShmemExit_Files(code: c_int, arg: Datum) {
    CleanupTempFiles(false, true);

    /* prevent further temp files from being created */
    #[cfg(debug_assertions)]
    {
        temporary_files_allowed = false;
    }
}

/*
 * Close temporary files and delete their underlying files.
 *
 * isCommit: if true, this is normal transaction commit, and we don't
 * expect any remaining files; warn if there are some.
 *
 * isProcExit: if true, this is being called as the backend process is
 * exiting.
 */
unsafe fn CleanupTempFiles(isCommit: bool, isProcExit: bool) {
    /*
     * Careful here: at proc_exit we need extra cleanup, not just
     * xact_temporary files.
     */
    if isProcExit || have_xact_temporary_files {
        Assert!(FileIsNotOpen(0)); /* Make sure ring not corrupted */
        let mut i: Index = 1;
        while i < SizeVfdCache {
            let fdstate = (*VfdCache.add(i)).fdstate;

            if ((fdstate & FD_DELETE_AT_CLOSE) != 0 || (fdstate & FD_CLOSE_AT_EOXACT) != 0)
                && !(*VfdCache.add(i)).fileName.is_null()
            {
                if isProcExit {
                    FileClose(i as File);
                } else if fdstate & FD_CLOSE_AT_EOXACT != 0 {
                    elog!(
                        WARNING,
                        "temporary file {} not closed at end-of-transaction",
                        std::ffi::CStr::from_ptr((*VfdCache.add(i)).fileName).to_string_lossy()
                    );
                    FileClose(i as File);
                }
            }
            i += 1;
        }

        have_xact_temporary_files = false;
    }

    /* Complain if any allocated files remain open at commit. */
    if isCommit && numAllocatedDescs > 0 {
        elog!(
            WARNING,
            "{} temporary files and directories not closed at end-of-transaction",
            numAllocatedDescs
        );
    }

    /* Clean up "allocated" stdio files, dirs and fds. */
    while numAllocatedDescs > 0 {
        FreeDesc(allocatedDescs);
    }
}

// ---------------------------------------------------------------------------
// RemovePgTempFiles, RemovePgTempFilesInDir,
// RemovePgTempRelationFiles, RemovePgTempRelationFilesInDbspace
// ---------------------------------------------------------------------------

/*
 * Remove temporary and temporary relation files left over from a prior
 * postmaster session.
 */
pub unsafe fn RemovePgTempFiles() {
    let mut temp_path: [c_char; MAXPGPATH + 64] = [0; MAXPGPATH + 64];
    let spc_dir: *mut DIR;
    let mut spc_de: *mut dirent;

    /*
     * First process temp files in pg_default ($PGDATA/base)
     */
    snprintf(temp_path.as_mut_ptr(),
             temp_path.len(),
             b"base/%s\0".as_ptr() as *const c_char,
             PG_TEMP_FILES_DIR.as_ptr() as *const c_char);
    RemovePgTempFilesInDir(temp_path.as_ptr(), true, false);
    RemovePgTempRelationFiles(b"base\0".as_ptr() as *const c_char);

    /*
     * Cycle through temp directories for all non-default tablespaces.
     */
    let spc_dir = AllocateDir(PG_TBLSPC_DIR.as_ptr() as *const c_char);

    loop {
        spc_de = ReadDirExtended(spc_dir, PG_TBLSPC_DIR.as_ptr() as *const c_char, LOG);
        if spc_de.is_null() { break; }

        if strcmp((*spc_de).d_name.as_ptr(), b".\0".as_ptr() as *const c_char) == 0
            || strcmp((*spc_de).d_name.as_ptr(), b"..\0".as_ptr() as *const c_char) == 0
        {
            continue;
        }

        snprintf(temp_path.as_mut_ptr(), temp_path.len(),
                 b"%s/%s/%s/%s\0".as_ptr() as *const c_char,
                 PG_TBLSPC_DIR.as_ptr() as *const c_char,
                 (*spc_de).d_name.as_ptr(),
                 TABLESPACE_VERSION_DIRECTORY.as_ptr() as *const c_char,
                 PG_TEMP_FILES_DIR.as_ptr() as *const c_char);
        RemovePgTempFilesInDir(temp_path.as_ptr(), true, false);

        snprintf(temp_path.as_mut_ptr(), temp_path.len(),
                 b"%s/%s/%s\0".as_ptr() as *const c_char,
                 PG_TBLSPC_DIR.as_ptr() as *const c_char,
                 (*spc_de).d_name.as_ptr(),
                 TABLESPACE_VERSION_DIRECTORY.as_ptr() as *const c_char);
        RemovePgTempRelationFiles(temp_path.as_ptr());
    }

    FreeDir(spc_dir);

    /*
     * In EXEC_BACKEND case there is a pgsql_tmp directory at the top level of
     * DataDir as well.  However, that is *not* cleaned here because doing so
     * would create a race condition.
     */
}

/*
 * Process one pgsql_tmp directory for RemovePgTempFiles.
 */
pub unsafe fn RemovePgTempFilesInDir(tmpdirname: *const c_char, missing_ok: bool, unlink_all: bool) {
    let mut rm_path: [c_char; MAXPGPATH * 2] = [0; MAXPGPATH * 2];
    let mut temp_de: *mut dirent;

    let temp_dir = AllocateDir(tmpdirname);

    if temp_dir.is_null() && errno() == ENOENT && missing_ok {
        return;
    }

    loop {
        temp_de = ReadDirExtended(temp_dir, tmpdirname, LOG);
        if temp_de.is_null() { break; }

        if strcmp((*temp_de).d_name.as_ptr(), b".\0".as_ptr() as *const c_char) == 0
            || strcmp((*temp_de).d_name.as_ptr(), b"..\0".as_ptr() as *const c_char) == 0
        {
            continue;
        }

        snprintf(rm_path.as_mut_ptr(), rm_path.len(),
                 b"%s/%s\0".as_ptr() as *const c_char,
                 tmpdirname, (*temp_de).d_name.as_ptr());

        if unlink_all
            || strncmp((*temp_de).d_name.as_ptr(),
                       PG_TEMP_FILE_PREFIX.as_ptr() as *const c_char,
                       strlen(PG_TEMP_FILE_PREFIX.as_ptr() as *const c_char)) == 0
        {
            let ftype = get_dirent_type(rm_path.as_ptr(), temp_de, false, LOG);

            match ftype {
                PGFileType::PGFILETYPE_ERROR => continue,
                PGFileType::PGFILETYPE_DIR => {
                    /* recursively remove contents, then directory itself */
                    RemovePgTempFilesInDir(rm_path.as_ptr(), false, true);

                    if rmdir(rm_path.as_ptr()) < 0 {
                        ereport!(
                            LOG,
                            errmsg!(
                                "could not remove directory \"{}\": errno={}",
                                std::ffi::CStr::from_ptr(rm_path.as_ptr()).to_string_lossy(),
                                errno()
                            )
                        );
                    }
                }
                _ => {
                    if unlink(rm_path.as_ptr()) < 0 {
                        ereport!(
                            LOG,
                            errmsg!(
                                "could not remove file \"{}\": errno={}",
                                std::ffi::CStr::from_ptr(rm_path.as_ptr()).to_string_lossy(),
                                errno()
                            )
                        );
                    }
                }
            }
        } else {
            ereport!(
                LOG,
                errmsg!(
                    "unexpected file found in temporary-files directory: \"{}\"",
                    std::ffi::CStr::from_ptr(rm_path.as_ptr()).to_string_lossy()
                )
            );
        }
    }

    FreeDir(temp_dir);
}

/* Process one tablespace directory, look for per-DB subdirectories */
unsafe fn RemovePgTempRelationFiles(tsdirname: *const c_char) {
    let mut dbspace_path: [c_char; MAXPGPATH * 2] = [0; MAXPGPATH * 2];
    let mut de: *mut dirent;

    let ts_dir = AllocateDir(tsdirname);

    loop {
        de = ReadDirExtended(ts_dir, tsdirname, LOG);
        if de.is_null() { break; }

        /*
         * We're only interested in the per-database directories, which have
         * numeric names.  Note that this code will also (properly) ignore "."
         * and "..".
         */
        if strspn((*de).d_name.as_ptr(), b"0123456789\0".as_ptr() as *const c_char)
            != strlen((*de).d_name.as_ptr())
        {
            continue;
        }

        snprintf(dbspace_path.as_mut_ptr(), dbspace_path.len(),
                 b"%s/%s\0".as_ptr() as *const c_char,
                 tsdirname, (*de).d_name.as_ptr());
        RemovePgTempRelationFilesInDbspace(dbspace_path.as_ptr());
    }

    FreeDir(ts_dir);
}

/* Process one per-dbspace directory for RemovePgTempRelationFiles */
unsafe fn RemovePgTempRelationFilesInDbspace(dbspacedirname: *const c_char) {
    let mut rm_path: [c_char; MAXPGPATH * 2] = [0; MAXPGPATH * 2];
    let mut de: *mut dirent;

    let dbspace_dir = AllocateDir(dbspacedirname);

    loop {
        de = ReadDirExtended(dbspace_dir, dbspacedirname, LOG);
        if de.is_null() { break; }

        if !looks_like_temp_rel_name((*de).d_name.as_ptr()) {
            continue;
        }

        snprintf(rm_path.as_mut_ptr(), rm_path.len(),
                 b"%s/%s\0".as_ptr() as *const c_char,
                 dbspacedirname, (*de).d_name.as_ptr());

        if unlink(rm_path.as_ptr()) < 0 {
            ereport!(
                LOG,
                errmsg!(
                    "could not remove file \"{}\": errno={}",
                    std::ffi::CStr::from_ptr(rm_path.as_ptr()).to_string_lossy(),
                    errno()
                )
            );
        }
    }

    FreeDir(dbspace_dir);
}

/* t<digits>_<digits>, or t<digits>_<digits>_<forkname> */
pub unsafe fn looks_like_temp_rel_name(name: *const c_char) -> bool {
    let name_bytes = std::ffi::CStr::from_ptr(name).to_bytes();
    let mut pos: usize = 0;

    /* Must start with "t". */
    if pos >= name_bytes.len() || name_bytes[pos] != b't' {
        return false;
    }
    pos += 1;

    /* Followed by a non-empty string of digits and then an underscore. */
    let start = pos;
    while pos < name_bytes.len() && name_bytes[pos].is_ascii_digit() {
        pos += 1;
    }
    if pos == start || pos >= name_bytes.len() || name_bytes[pos] != b'_' {
        return false;
    }
    pos += 1;

    /* Followed by another nonempty string of digits. */
    let savepos = pos;
    while pos < name_bytes.len() && name_bytes[pos].is_ascii_digit() {
        pos += 1;
    }
    if savepos == pos {
        return false;
    }

    /* We might have _forkname or .segment or both. */
    if pos < name_bytes.len() && name_bytes[pos] == b'_' {
        let forkchar = forkname_chars(name.add(pos + 1), null_mut());
        if forkchar <= 0 {
            return false;
        }
        pos += forkchar as usize + 1;
    }
    if pos < name_bytes.len() && name_bytes[pos] == b'.' {
        let mut segchar: usize = 1;
        while pos + segchar < name_bytes.len() && name_bytes[pos + segchar].is_ascii_digit() {
            segchar += 1;
        }
        if segchar <= 1 {
            return false;
        }
        pos += segchar;
    }

    /* Now we should be at the end. */
    pos >= name_bytes.len() || name_bytes[pos] == 0
}

// ---------------------------------------------------------------------------
// SyncDataDirectory, walkdir, pre_sync_fname, datadir_fsync_fname,
// unlink_if_exists_fname, fsync_fname_ext, fsync_parent_path
// ---------------------------------------------------------------------------

/* HAVE_SYNCFS - Linux only, not ported */
#[cfg(any())]
unsafe fn do_syncfs(_path: *const c_char) { /* syncfs path omitted */ }

/*
 * Issue fsync recursively on PGDATA and all its contents.
 */
pub unsafe fn SyncDataDirectory() {
    let mut xlog_is_symlink: bool = false;

    /* We can skip this whole thing if fsync is disabled. */
    if !enableFsync {
        return;
    }

    /*
     * If pg_wal is a symlink, we'll need to recurse into it separately.
     */
    {
        let mut st: stat_t = core::mem::zeroed();
        if lstat_inode64(b"pg_wal\0".as_ptr() as *const c_char, &raw mut st) < 0 {
            ereport!(LOG, errmsg!("could not stat file \"pg_wal\": errno={}", errno()));
        } else if S_ISLNK(st.st_mode) {
            xlog_is_symlink = true;
        }
    }

    /* HAVE_SYNCFS path - Linux only, not ported */
    #[cfg(any())]
    {
        // syncfs path omitted
    }

    /* PG_FLUSH_DATA_WORKS - Darwin has mmap/msync so this is available */
    {
        /* Prepare to report progress of the pre-fsync phase. */
        begin_startup_progress_phase();

        walkdir(b".\0".as_ptr() as *const c_char, pre_sync_fname, false, DEBUG1);
        if xlog_is_symlink {
            walkdir(b"pg_wal\0".as_ptr() as *const c_char, pre_sync_fname, false, DEBUG1);
        }
        walkdir(PG_TBLSPC_DIR.as_ptr() as *const c_char, pre_sync_fname, true, DEBUG1);
    }

    /* Prepare to report progress syncing the data directory via fsync. */
    begin_startup_progress_phase();

    /*
     * Now we do the fsync()s in the same order.
     */
    walkdir(b".\0".as_ptr() as *const c_char, datadir_fsync_fname, false, LOG);
    if xlog_is_symlink {
        walkdir(b"pg_wal\0".as_ptr() as *const c_char, datadir_fsync_fname, false, LOG);
    }
    walkdir(PG_TBLSPC_DIR.as_ptr() as *const c_char, datadir_fsync_fname, true, LOG);
}

/*
 * walkdir: recursively walk a directory, applying the action to each
 * regular file and directory.
 */
unsafe fn walkdir(
    path: *const c_char,
    action: unsafe fn(*const c_char, bool, c_int),
    process_symlinks: bool,
    elevel: c_int,
) {
    let mut de: *mut dirent;
    let dir = AllocateDir(path);

    loop {
        de = ReadDirExtended(dir, path, elevel);
        if de.is_null() { break; }

        let mut subpath: [c_char; MAXPGPATH * 2] = [0; MAXPGPATH * 2];

        CHECK_FOR_INTERRUPTS!();

        if strcmp((*de).d_name.as_ptr(), b".\0".as_ptr() as *const c_char) == 0
            || strcmp((*de).d_name.as_ptr(), b"..\0".as_ptr() as *const c_char) == 0
        {
            continue;
        }

        snprintf(subpath.as_mut_ptr(), subpath.len(),
                 b"%s/%s\0".as_ptr() as *const c_char,
                 path, (*de).d_name.as_ptr());

        match get_dirent_type(subpath.as_ptr(), de, process_symlinks, elevel) {
            PGFileType::PGFILETYPE_REG => {
                action(subpath.as_ptr(), false, elevel);
            }
            PGFileType::PGFILETYPE_DIR => {
                walkdir(subpath.as_ptr(), action, false, elevel);
            }
            _ => {
                /*
                 * Errors are already reported directly by get_dirent_type(),
                 * and any remaining symlinks and unknown file types are ignored.
                 */
            }
        }
    }

    FreeDir(dir); /* we ignore any error here */

    /*
     * It's important to fsync the destination directory itself as individual
     * file fsyncs don't guarantee that the directory entry for the file is
     * synced.  However, skip this if AllocateDir failed.
     */
    if !dir.is_null() {
        action(path, true, elevel);
    }
}

/*
 * Hint to the OS that it should get ready to fsync() this file.
 * (PG_FLUSH_DATA_WORKS path - Darwin has mmap/msync)
 */
unsafe fn pre_sync_fname(fname: *const c_char, isdir: bool, elevel: c_int) {
    /* Don't try to flush directories, it'll likely just fail */
    if isdir {
        return;
    }

    ereport_startup_progress!(
        "syncing data directory (pre-fsync), elapsed time: %ld.%02d s, current path: %s",
        fname
    );

    let fd = OpenTransientFile(fname, O_RDONLY | PG_BINARY);

    if fd < 0 {
        if errno() == EACCES {
            return;
        }
        ereport!(
            elevel,
            errmsg!(
                "could not open file \"{}\": errno={}",
                std::ffi::CStr::from_ptr(fname).to_string_lossy(),
                errno()
            )
        );
        return;
    }

    /*
     * pg_flush_data() ignores errors, which is ok because this is only a hint.
     */
    pg_flush_data(fd, 0, 0);

    if CloseTransientFile(fd) != 0 {
        ereport!(
            elevel,
            errmsg!(
                "could not close file \"{}\": errno={}",
                std::ffi::CStr::from_ptr(fname).to_string_lossy(),
                errno()
            )
        );
    }
}

unsafe fn datadir_fsync_fname(fname: *const c_char, isdir: bool, elevel: c_int) {
    ereport_startup_progress!(
        "syncing data directory (fsync), elapsed time: %ld.%02d s, current path: %s",
        fname
    );

    /*
     * We want to silently ignore errors about unreadable files.
     */
    fsync_fname_ext(fname, isdir, true, elevel);
}

unsafe fn unlink_if_exists_fname(fname: *const c_char, isdir: bool, elevel: c_int) {
    if isdir {
        if rmdir(fname) != 0 && errno() != ENOENT {
            ereport!(
                elevel,
                errmsg!(
                    "could not remove directory \"{}\": errno={}",
                    std::ffi::CStr::from_ptr(fname).to_string_lossy(),
                    errno()
                )
            );
        }
    } else {
        /* Use PathNameDeleteTemporaryFile to report filesize */
        PathNameDeleteTemporaryFile(fname, false);
    }
}

/*
 * fsync_fname_ext -- Try to fsync a file or directory
 *
 * If ignore_perm is true, ignore errors upon trying to open unreadable files.
 * Returns 0 if the operation succeeded, -1 otherwise.
 */
pub unsafe fn fsync_fname_ext(fname: *const c_char, isdir: bool, ignore_perm: bool, elevel: c_int) -> c_int {
    let mut flags: c_int = PG_BINARY;
    if !isdir {
        flags |= O_RDWR;
    } else {
        flags |= O_RDONLY;
    }

    let fd = OpenTransientFile(fname, flags);

    /*
     * Some OSs don't allow us to open directories at all (Windows returns
     * EACCES), just ignore the error in that case.
     */
    if fd < 0 && isdir && (errno() == EISDIR || errno() == EACCES) {
        return 0;
    } else if fd < 0 && ignore_perm && errno() == EACCES {
        return 0;
    } else if fd < 0 {
        ereport!(
            elevel,
            errmsg!(
                "could not open file \"{}\": errno={}",
                std::ffi::CStr::from_ptr(fname).to_string_lossy(),
                errno()
            )
        );
        return -1;
    }

    let returncode = pg_fsync(fd);

    /*
     * Some OSes don't allow us to fsync directories at all.
     */
    if returncode != 0 && !(isdir && (errno() == EBADF || errno() == EINVAL)) {
        let save_errno = errno();

        /* close file upon error, might not be in transaction context */
        CloseTransientFile(fd);
        set_errno(save_errno);

        ereport!(
            elevel,
            errmsg!(
                "could not fsync file \"{}\": errno={}",
                std::ffi::CStr::from_ptr(fname).to_string_lossy(),
                errno()
            )
        );
        return -1;
    }

    if CloseTransientFile(fd) != 0 {
        ereport!(
            elevel,
            errmsg!(
                "could not close file \"{}\": errno={}",
                std::ffi::CStr::from_ptr(fname).to_string_lossy(),
                errno()
            )
        );
        return -1;
    }

    0
}

/*
 * fsync_parent_path -- fsync the parent path of a file or directory
 */
unsafe fn fsync_parent_path(fname: *const c_char, elevel: c_int) -> c_int {
    let mut parentpath: [c_char; MAXPGPATH] = [0; MAXPGPATH];

    strlcpy(parentpath.as_mut_ptr(), fname, MAXPGPATH);
    get_parent_directory(parentpath.as_mut_ptr());

    /*
     * get_parent_directory() returns an empty string if the input argument is
     * just a file name, so handle that as being the current directory.
     */
    if strlen(parentpath.as_ptr()) == 0 {
        strlcpy(parentpath.as_mut_ptr(), b".\0".as_ptr() as *const c_char, MAXPGPATH);
    }

    if fsync_fname_ext(parentpath.as_ptr(), true, false, elevel) != 0 {
        return -1;
    }

    0
}

// ---------------------------------------------------------------------------
// MakePGDirectory, data_sync_elevel
// ---------------------------------------------------------------------------

/*
 * Create a PostgreSQL data sub-directory
 */
pub unsafe fn MakePGDirectory(directoryName: *const c_char) -> c_int {
    mkdir(directoryName, pg_dir_create_mode)
}

/*
 * Return the passed-in error level, or PANIC if data_sync_retry is off.
 */
pub unsafe fn data_sync_elevel(elevel: c_int) -> c_int {
    if data_sync_retry { elevel } else { PANIC }
}

// ---------------------------------------------------------------------------
// GUC hooks: check_debug_io_direct, assign_debug_io_direct
// ---------------------------------------------------------------------------

/*
 * check_debug_io_direct - GUC check hook for debug_io_direct
 *
 * On Darwin, PG_O_DIRECT is supported via F_NOCACHE so this is non-empty.
 * The list parsing (SplitGUCList) is stubbed out for now.
 * TODO(pg-port): wire up real GUC list parsing.
 */
pub unsafe fn check_debug_io_direct(newval: *mut *mut c_char, extra: *mut *mut c_void, source: GucSource) -> bool {
    /* TODO(pg-port): implement full GUC list parsing via SplitGUCList */
    let flags: c_int = 0;

    *extra = malloc(core::mem::size_of::<c_int>());
    if (*extra).is_null() {
        return false;
    }
    *((*extra) as *mut c_int) = flags;

    true
}

pub unsafe fn assign_debug_io_direct(newval: *const c_char, extra: *mut c_void) {
    let flags = *(extra as *const c_int);
    io_direct_flags = flags;
}

// ---------------------------------------------------------------------------
// ResourceOwner callbacks
// ---------------------------------------------------------------------------

unsafe fn ResOwnerReleaseFile(res: Datum) {
    let file: File = DatumGetInt32(res);
    let vfdP: *mut Vfd;

    Assert!(FileIsValid(file));

    vfdP = VfdCache.add(file as usize);
    (*vfdP).resowner = null_mut();

    FileClose(file);
}

unsafe fn ResOwnerPrintFile(res: Datum) -> *mut c_char {
    /* psprintf stub returns null; caller must handle gracefully */
    let _ = DatumGetInt32(res);
    psprintf(b"File ???\0".as_ptr() as *const c_char)
}

// ---------------------------------------------------------------------------
// shmem_stubs module (palloc/pfree used by count_usable_fds)
// ---------------------------------------------------------------------------
mod shmem_stubs {
    use super::*;
    pub unsafe fn palloc(size: usize) -> *mut c_void {
        malloc(size)
    }
}

