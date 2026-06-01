//! Translation of postgres/src/include/common/file_perm.h
//! + postgres/src/common/file_perm.c
//!
//! File and directory permission definitions and routines.
//!
//! Provides the process-global create modes/mask used when writing to PGDATA
//! (`pg_dir_create_mode`, `pg_file_create_mode`, `pg_mode_mask`), the
//! PG_DIR_MODE_*/PG_FILE_MODE_*/PG_MODE_MASK_* constants, and the two helpers
//! `SetDataDirectoryCreatePerm` and `GetDataDirectoryCreatePerm`.
//!
//! The .c begins with `#include "c.h"` then `#include "common/file_perm.h"`.
//! `GetDataDirectoryCreatePerm` lives in the `#ifdef FRONTEND` branch of the .c
//! (it is only compiled into frontend programs).  file_perm itself is shared by
//! both backend and frontend, so we translate the FRONTEND path here; the
//! function is a thin wrapper around the libc stat(2) call, which binds simply
//! on the Unix platforms PostgreSQL targets.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!   src/common/file_perm.c

use crate::prelude::*;

// ----------------------------------------------------------------
//   <sys/stat.h> mode_t and S_I* permission bits
// ----------------------------------------------------------------
//
// The header `#include <sys/stat.h>` so that the S_I* macros and `mode_t` are
// in scope.  POSIX fixes these permission-bit values (they are the classic
// octal constants), so we reproduce them as plain constants rather than
// depending on platform <sys/stat.h>.  `mode_t` is an unsigned integer type; on
// the platforms PostgreSQL cares about it is 32-bit unsigned.

/// POSIX `mode_t`: the file-mode/permission bit type.
pub type mode_t = u32;

// Owner permission bits.
const S_IRUSR: mode_t = 0o400; // read by owner
const S_IWUSR: mode_t = 0o200; // write by owner
const S_IXUSR: mode_t = 0o100; // execute by owner
const S_IRWXU: mode_t = S_IRUSR | S_IWUSR | S_IXUSR; // 0o700: rwx by owner

// Group permission bits.
const S_IRGRP: mode_t = 0o040; // read by group
const S_IWGRP: mode_t = 0o020; // write by group
const S_IXGRP: mode_t = 0o010; // execute by group
const S_IRWXG: mode_t = S_IRGRP | S_IWGRP | S_IXGRP; // 0o070: rwx by group

// Other (world) permission bits.
const S_IROTH: mode_t = 0o004; // read by others
const S_IWOTH: mode_t = 0o002; // write by others
const S_IXOTH: mode_t = 0o001; // execute by others
const S_IRWXO: mode_t = S_IROTH | S_IWOTH | S_IXOTH; // 0o007: rwx by others

// ----------------------------------------------------------------
//   Mode masks and create modes (from file_perm.h)
// ----------------------------------------------------------------

/// Mode mask for data directory permissions that only allows the owner to
/// read/write directories and files.
///
/// This is the default.
pub const PG_MODE_MASK_OWNER: c_int = (S_IRWXG | S_IRWXO) as c_int;

/// Mode mask for data directory permissions that also allows group
/// read/execute.
pub const PG_MODE_MASK_GROUP: c_int = (S_IWGRP | S_IRWXO) as c_int;

/// Default mode for creating directories.
pub const PG_DIR_MODE_OWNER: c_int = S_IRWXU as c_int;

/// Mode for creating directories that allows group read/execute.
pub const PG_DIR_MODE_GROUP: c_int = (S_IRWXU | S_IRGRP | S_IXGRP) as c_int;

/// Default mode for creating files.
pub const PG_FILE_MODE_OWNER: c_int = (S_IRUSR | S_IWUSR) as c_int;

/// Mode for creating files that allows group read.
pub const PG_FILE_MODE_GROUP: c_int = (S_IRUSR | S_IWUSR | S_IRGRP) as c_int;

// ----------------------------------------------------------------
//   Process-global create modes and mask (from file_perm.c)
// ----------------------------------------------------------------
//
// These are mutable process state (set once near startup via
// SetDataDirectoryCreatePerm / GetDataDirectoryCreatePerm, then read by every
// file/directory create), so they map to `pub static mut`.

/// Mode for creating directories in the data directory.
/// (C: `int pg_dir_create_mode = PG_DIR_MODE_OWNER;`)
pub static mut pg_dir_create_mode: c_int = PG_DIR_MODE_OWNER;

/// Mode for creating files in the data directory.
/// (C: `int pg_file_create_mode = PG_FILE_MODE_OWNER;`)
pub static mut pg_file_create_mode: c_int = PG_FILE_MODE_OWNER;

/// Mode mask to pass to umask().  This is more of a preventative measure since
/// all file/directory creates should be performed using the create modes above.
/// (C: `int pg_mode_mask = PG_MODE_MASK_OWNER;`)
pub static mut pg_mode_mask: c_int = PG_MODE_MASK_OWNER;

/// Set create modes and mask to use when writing to PGDATA based on the data
/// directory mode passed.  If group read/execute are present in the mode, then
/// create modes and mask will be relaxed to allow group read/execute on all
/// newly created files and directories.
///
/// # Safety
/// Mutates the process-global create-mode statics; the caller is responsible
/// for the usual single-threaded-startup ordering PostgreSQL assumes.
pub unsafe fn SetDataDirectoryCreatePerm(dataDirMode: c_int) {
    /* If the data directory mode has group access */
    if (PG_DIR_MODE_GROUP & dataDirMode) == PG_DIR_MODE_GROUP {
        pg_dir_create_mode = PG_DIR_MODE_GROUP;
        pg_file_create_mode = PG_FILE_MODE_GROUP;
        pg_mode_mask = PG_MODE_MASK_GROUP;
    }
    /* Else use default permissions */
    else {
        pg_dir_create_mode = PG_DIR_MODE_OWNER;
        pg_file_create_mode = PG_FILE_MODE_OWNER;
        pg_mode_mask = PG_MODE_MASK_OWNER;
    }
}

// ----------------------------------------------------------------
//   <sys/stat.h> stat(2) binding
// ----------------------------------------------------------------
//
// GetDataDirectoryCreatePerm only needs `struct stat`'s st_mode field, but the
// rest of the struct layout is platform-specific.  Rather than reproduce the
// full (Darwin/Linux-divergent) layout, we bind libc stat() and read the mode
// via a small platform-aware accessor.  The `statbuf` is an opaque, suitably
// sized/aligned byte buffer that libc fills in; we only ever extract st_mode.
//
// TODO(pg-port): this hard-codes the Darwin/Linux `struct stat` size and the
// st_mode offset.  When the port grows a real <sys/stat.h> binding layer,
// route through it instead of this hand-rolled buffer.

/// Opaque, over-allocated stand-in for `struct stat`.  256 bytes comfortably
/// covers the real struct on every platform PostgreSQL targets; 16-byte
/// alignment satisfies the largest member's alignment.
#[repr(C, align(16))]
struct stat_buf {
    bytes: [u8; 256],
}

extern "C" {
    // On Darwin the public `stat` symbol is `stat$INODE64` for the modern
    // 64-bit-inode struct; on Linux it is the bare `stat`.  Select the right
    // link name per target so the right struct-stat ABI is used.
    #[cfg_attr(target_os = "macos", link_name = "stat$INODE64")]
    /// `int stat(const char *path, struct stat *buf);`
    fn stat(path: *const c_char, buf: *mut stat_buf) -> c_int;
}

/// Read `st_mode` out of a filled-in `struct stat` buffer.
///
/// `st_mode` is an early field of `struct stat`.  Its byte offset differs by
/// platform: on Linux/glibc x86-64 it sits at offset 24 (after st_dev,
/// st_ino, st_nlink), while on Darwin it sits at offset 4 (after st_dev).
///
/// # Safety
/// `buf` must have been fully populated by a successful `stat()` call.
#[inline]
unsafe fn stat_st_mode(buf: *const stat_buf) -> mode_t {
    let base = buf as *const u8;
    #[cfg(target_os = "macos")]
    let off = 4usize; // st_dev (i32) precedes st_mode (u16) on Darwin
    #[cfg(not(target_os = "macos"))]
    let off = 24usize; // st_dev(u64)+st_ino(u64)+st_nlink(u64) on Linux x86-64
    // st_mode is a `mode_t`; read the platform-native width and widen.
    #[cfg(target_os = "macos")]
    {
        // Darwin: mode_t is u16.
        (core::ptr::read_unaligned(base.add(off) as *const u16)) as mode_t
    }
    #[cfg(not(target_os = "macos"))]
    {
        // Linux: mode_t is u32.
        core::ptr::read_unaligned(base.add(off) as *const u32) as mode_t
    }
}

/// Get the create modes and mask to use when writing to PGDATA by examining the
/// mode of the PGDATA directory and calling SetDataDirectoryCreatePerm().
///
/// Errors are not handled here and should be reported by the application when
/// false is returned.
///
/// Suppress when on Windows, because there may not be proper support for Unix-y
/// file permissions.  But we still run stat() on the directory so that callers
/// get consistent behavior for example if the directory does not exist.
///
/// # Safety
/// `dataDir` must be a valid NUL-terminated C string; this mutates the
/// process-global create-mode statics via SetDataDirectoryCreatePerm.
pub unsafe fn GetDataDirectoryCreatePerm(dataDir: *const c_char) -> bool {
    let mut statBuf: stat_buf = stat_buf { bytes: [0u8; 256] };

    /*
     * If an error occurs getting the mode then return false.  The caller is
     * responsible for generating an error, if appropriate, indicating that we
     * were unable to access the data directory.
     */
    if stat(dataDir, &mut statBuf) == -1 {
        return false;
    }

    // #if !defined(WIN32) && !defined(__CYGWIN__)
    /* Set permissions */
    SetDataDirectoryCreatePerm(stat_st_mode(&statBuf) as c_int);
    // #endif
    //
    // TODO(pg-port): the WIN32/__CYGWIN__ branch (skip SetDataDirectoryCreatePerm
    // but still stat()) is not translated; this port targets Unix file perms.

    true
}
