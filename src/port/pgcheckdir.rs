//! Translated from PostgreSQL `src/port/pgcheckdir.c`
//! (declaration in `src/include/port.h`:
//! `extern int pg_check_dir(const char *dir);`).
//!
//! A simple subroutine to check whether a directory exists and is empty or not.
//! Useful in both initdb and the backend.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!   src/port/pgcheckdir.c

use crate::prelude::*;

// ----------------------------------------------------------------
//   libc bindings  (<dirent.h>, <string.h>, <errno.h>)
// ----------------------------------------------------------------

// Opaque DIR handle returned by opendir(3).  We never look inside it; it only
// ever travels as a `*mut DIR` between opendir/readdir/closedir, so a zero-size
// opaque type is sufficient.
#[repr(C)]
pub struct DIR {
    _private: [u8; 0],
}

// `struct dirent`.  The C source only ever reads `d_name`, so the only field
// that must be laid out correctly is `d_name`; the leading members exist solely
// to place `d_name` at the platform-correct byte offset.  The layout differs
// across platforms, so it is selected by cfg below.
//
// macOS/BSD (_DARWIN_FEATURE_64_BIT_INODE, the default on modern macOS):
//   ino_t    d_ino;       (8)
//   __uint64 d_seekoff;   (8)
//   __uint16 d_reclen;    (2)
//   __uint16 d_namlen;    (2)
//   __uint8  d_type;      (1)
//   char     d_name[1024];   -> offset 21
#[cfg(any(target_os = "macos", target_os = "ios", target_vendor = "apple"))]
#[repr(C, packed)]
pub struct dirent {
    pub d_ino: u64,
    pub d_seekoff: u64,
    pub d_reclen: u16,
    pub d_namlen: u16,
    pub d_type: u8,
    pub d_name: [c_char; 1024],
}

// Linux/glibc:
//   ino_t          d_ino;       (8)
//   off_t          d_off;       (8)
//   unsigned short d_reclen;    (2)
//   unsigned char  d_type;      (1)
//   char           d_name[256];   -> offset 19
//
// TODO(pg-port): other (non-glibc, non-Apple) Unixes may use a different
// `struct dirent` layout; only Linux/glibc and macOS/BSD are handled here.
#[cfg(not(any(target_os = "macos", target_os = "ios", target_vendor = "apple")))]
#[repr(C)]
pub struct dirent {
    pub d_ino: u64,
    pub d_off: i64,
    pub d_reclen: u16,
    pub d_type: u8,
    pub d_name: [c_char; 256],
}

extern "C" {
    fn opendir(name: *const c_char) -> *mut DIR;
    fn readdir(dirp: *mut DIR) -> *mut dirent;
    fn closedir(dirp: *mut DIR) -> c_int;

    fn strcmp(s1: *const c_char, s2: *const c_char) -> c_int;

    // Per-thread errno location.  macOS/BSD expose it as __error(); glibc uses
    // __errno_location().  Both return `*mut c_int`.
    #[cfg_attr(
        any(target_os = "macos", target_os = "ios", target_vendor = "apple"),
        link_name = "__error"
    )]
    #[cfg_attr(
        not(any(target_os = "macos", target_os = "ios", target_vendor = "apple")),
        link_name = "__errno_location"
    )]
    fn pg_errno_location() -> *mut c_int;
}

// <errno.h>: ENOENT (No such file or directory).  Value 2 on both macOS and
// Linux.
const ENOENT: c_int = 2;

/// Read the current C `errno` value.
#[inline]
unsafe fn errno() -> c_int {
    *pg_errno_location()
}

/// Set the current C `errno` value (the C source does `errno = 0` before each
/// readdir() call, and restores it after closedir()).
#[inline]
unsafe fn set_errno(v: c_int) {
    *pg_errno_location() = v;
}

/*
 * Test to see if a directory exists and is empty or not.
 *
 * Returns:
 *		0 if nonexistent
 *		1 if exists and empty
 *		2 if exists and contains _only_ dot files
 *		3 if exists and contains a mount point
 *		4 if exists and not empty
 *		-1 if trouble accessing directory (errno reflects the error)
 */
pub unsafe fn pg_check_dir(dir: *const c_char) -> c_int {
    let mut result: c_int = 1;
    let chkdir: *mut DIR;
    let mut file: *mut dirent;
    let mut dot_found: bool = false;
    let mut mount_found: bool = false;
    let readdir_errno: c_int;

    chkdir = opendir(dir);
    if chkdir == null_mut() {
        return if errno() == ENOENT { 0 } else { -1 };
    }

    // while (errno = 0, (file = readdir(chkdir)) != NULL)
    loop {
        set_errno(0);
        file = readdir(chkdir);
        if file == null_mut() {
            break;
        }

        if strcmp(c".".as_ptr(), (*file).d_name.as_ptr()) == 0
            || strcmp(c"..".as_ptr(), (*file).d_name.as_ptr()) == 0
        {
            /* skip this and parent directory */
            continue;
        }
        // TODO(pg-port): the `#ifndef WIN32` guard around the dot-file and
        // lost+found checks is treated as always-true here (non-Windows port).
        /* file starts with "." */
        else if (*file).d_name[0] == b'.' as c_char {
            dot_found = true;
        }
        /* lost+found directory */
        else if strcmp(c"lost+found".as_ptr(), (*file).d_name.as_ptr()) == 0 {
            mount_found = true;
        } else {
            result = 4; /* not empty */
            break;
        }
    }

    if errno() != 0 {
        result = -1; /* some kind of I/O error? */
    }

    /* Close chkdir and avoid overwriting the readdir errno on success */
    readdir_errno = errno();
    if closedir(chkdir) != 0 {
        result = -1; /* error executing closedir */
    } else {
        set_errno(readdir_errno);
    }

    /* We report on mount point if we find a lost+found directory */
    if result == 1 && mount_found {
        result = 3;
    }

    /* We report on dot-files if we _only_ find dot files */
    if result == 1 && dot_found {
        result = 2;
    }

    result
}
