//! Translated from PostgreSQL `src/port/pgmkdirp.c`
//! (declaration in `src/include/port.h`:
//! `extern int pg_mkdir_p(char *path, int omode);`).
//!
//! This is adapted from FreeBSD's src/bin/mkdir/mkdir.c, which bears
//! the following copyright notice:
//!
//! Copyright (c) 1983, 1992, 1993
//!	The Regents of the University of California.  All rights reserved.
//!
//! Redistribution and use in source and binary forms, with or without
//! modification, are permitted provided that the following conditions
//! are met:
//! 1. Redistributions of source code must retain the above copyright
//!	  notice, this list of conditions and the following disclaimer.
//! 2. Redistributions in binary form must reproduce the above copyright
//!	  notice, this list of conditions and the following disclaimer in the
//!	  documentation and/or other materials provided with the distribution.
//! 4. Neither the name of the University nor the names of its contributors
//!	  may be used to endorse or promote products derived from this software
//!	  without specific prior written permission.
//!
//! THIS SOFTWARE IS PROVIDED BY THE REGENTS AND CONTRIBUTORS ``AS IS'' AND
//! ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
//! IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
//! ARE DISCLAIMED.  IN NO EVENT SHALL THE REGENTS OR CONTRIBUTORS BE LIABLE
//! FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
//! DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
//! OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
//! HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
//! LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
//! OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
//! SUCH DAMAGE.

use crate::prelude::*;

// The C source does `#include "c.h"` and `#include <sys/stat.h>`.  The latter
// supplies `mode_t`, `struct stat`, the `S_I*` permission bits, the `S_ISDIR`
// macro, and the `mkdir`/`stat`/`umask` prototypes.  We bind the libc routines
// directly and define the small constants/struct we need, matching the binding
// strategy used elsewhere in the port (see common/file_perm.rs, port/tar.rs).

/// POSIX `mode_t`: the file-mode/permission bit type.
///
/// (The C declares omode as `int`, not `mode_t`, to minimize dependencies for
/// port.h; the umask/permission arithmetic is done in `mode_t`.)
#[allow(non_camel_case_types)]
pub type mode_t = c_uint;

// <sys/stat.h> permission bits used below.  Values are identical across the
// Unix platforms PostgreSQL targets.
const S_IWUSR: mode_t = 0o000200; // write by owner
const S_IXUSR: mode_t = 0o000100; // execute/search by owner
const S_IRWXU: mode_t = 0o000700; // RWX mask for owner
const S_IRWXG: mode_t = 0o000070; // RWX mask for group
const S_IRWXO: mode_t = 0o000007; // RWX mask for other

// File-type mask and the directory type, for S_ISDIR().
const S_IFMT: mode_t = 0o170000; // type-of-file mask
const S_IFDIR: mode_t = 0o040000; // directory

/// `S_ISDIR(m)` -- true when the mode designates a directory.
#[inline]
fn S_ISDIR(m: mode_t) -> bool {
    (m & S_IFMT) == S_IFDIR
}

// errno values set on failure.  Values are identical across the Unix platforms
// PostgreSQL targets.
const EINVAL: c_int = 22; // Invalid argument
const ENOTDIR: c_int = 20; // Not a directory
const EEXIST: c_int = 17; // File exists

// Opaque, over-allocated stand-in for `struct stat`.  We only read `st_mode`;
// the rest of the (platform-specific) layout is irrelevant.  256 bytes
// comfortably covers the real struct on every platform PostgreSQL targets;
// 16-byte alignment satisfies the largest member's alignment.
//
// TODO(pg-port): this hard-codes the Darwin/Linux `struct stat` size and the
// st_mode offset.  When the port grows a real <sys/stat.h> binding layer,
// route through it instead of this hand-rolled buffer.
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

    /// `int mkdir(const char *path, mode_t mode);`
    fn mkdir(path: *const c_char, mode: mode_t) -> c_int;

    /// `mode_t umask(mode_t cmask);`
    fn umask(cmask: mode_t) -> mode_t;

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

/// Set the current C `errno` value.
#[inline]
unsafe fn set_errno(e: c_int) {
    *pg_errno_location() = e;
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

/// Minimal `strlen` over a C string (mirrors libc strlen; matches the bootstrap
/// helper used elsewhere in the port).
///
/// # Safety
/// `s` must point to a valid NUL-terminated C string.
#[inline]
#[allow(dead_code)]
unsafe fn strlen(s: *const c_char) -> usize {
    let mut n = 0usize;
    while *s.add(n) != 0 {
        n += 1;
    }
    n
}

/*
 * pg_mkdir_p --- create a directory and, if necessary, parent directories
 *
 * This is equivalent to "mkdir -p" except we don't complain if the target
 * directory already exists.
 *
 * We assume the path is in canonical form, i.e., uses / as the separator.
 *
 * omode is the file permissions bits for the target directory.  Note that any
 * parent directories that have to be created get permissions according to the
 * prevailing umask, but with u+wx forced on to ensure we can create there.
 * (We declare omode as int, not mode_t, to minimize dependencies for port.h.)
 *
 * Returns 0 on success, -1 (with errno set) on failure.
 *
 * Note that on failure, the path arg has been modified to show the particular
 * directory level we had problems with.
 *
 * # Safety
 * `path` must point to a writable, NUL-terminated C string buffer; it is
 * mutated in place during the component walk (NUL terminators are written and
 * restored).
 */
#[no_mangle]
pub unsafe extern "C" fn pg_mkdir_p(path: *mut c_char, omode: c_int) -> c_int {
    let mut sb: stat_buf = stat_buf { bytes: [0u8; 256] };
    let numask: mode_t;
    let oumask: mode_t;
    let mut last: c_int;
    let mut retval: c_int;
    let mut p: *mut c_char;

    retval = 0;
    p = path;

    // TODO(pg-port): the `#ifdef WIN32` branch is not translated; this port
    // targets Unix paths and does not skip network ('//host') or drive
    // ('C:') specifiers.  For reference, the C does:
    //
    //   /* skip network and drive specifiers for win32 */
    //   if (strlen(p) >= 2)
    //   {
    //       if (p[0] == '/' && p[1] == '/')
    //       {
    //           /* network drive */
    //           p = strstr(p + 2, "/");
    //           if (p == NULL)
    //           {
    //               errno = EINVAL;
    //               return -1;
    //           }
    //       }
    //       else if (p[1] == ':' &&
    //                ((p[0] >= 'a' && p[0] <= 'z') ||
    //                 (p[0] >= 'A' && p[0] <= 'Z')))
    //       {
    //           /* local drive */
    //           p += 2;
    //       }
    //   }
    //
    // Keep EINVAL referenced so the unused-const lint stays quiet on Unix; this
    // value is what the (untranslated) WIN32 path would set on a bad network
    // specifier.
    let _ = EINVAL;

    /*
     * POSIX 1003.2: For each dir operand that does not name an existing
     * directory, effects equivalent to those caused by the following command
     * shall occur:
     *
     * mkdir -p -m $(umask -S),u+wx $(dirname dir) && mkdir [-m mode] dir
     *
     * We change the user's umask and then restore it, instead of doing
     * chmod's.  Note we assume umask() can't change errno.
     */
    oumask = umask(0);
    numask = oumask & !(S_IWUSR | S_IXUSR);
    let _ = umask(numask);

    if *p == b'/' as c_char {
        /* Skip leading '/'. */
        p = p.add(1);
    }
    last = 0;
    while last == 0 {
        if *p == b'\0' as c_char {
            last = 1;
        } else if *p != b'/' as c_char {
            p = p.add(1);
            continue;
        }
        *p = b'\0' as c_char;
        if last == 0 && *p.add(1) == b'\0' as c_char {
            last = 1;
        }

        if last != 0 {
            let _ = umask(oumask);
        }

        /* check for pre-existing directory */
        if stat(path, &mut sb) == 0 {
            if !S_ISDIR(stat_st_mode(&sb)) {
                if last != 0 {
                    set_errno(EEXIST);
                } else {
                    set_errno(ENOTDIR);
                }
                retval = -1;
                break;
            }
        } else if mkdir(
            path,
            if last != 0 {
                omode as mode_t
            } else {
                S_IRWXU | S_IRWXG | S_IRWXO
            },
        ) < 0
        {
            retval = -1;
            break;
        }
        if last == 0 {
            *p = b'/' as c_char;
        }

        p = p.add(1);
    }

    /* ensure we restored umask */
    let _ = umask(oumask);

    retval
}
