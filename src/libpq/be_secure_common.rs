//! be-secure-common.c - common implementation-independent SSL support code.
//!
//! While be-secure.c contains the interfaces that the rest of the
//! communications code calls, this file contains support routines that are
//! used by the library-specific implementations such as be-secure-openssl.c.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::prelude::*;

use crate::common::percentrepl::replace_percent_placeholders;
use crate::common::string::pg_strip_crlf;
use crate::common::wait_error::wait_result_to_str;
use crate::libpq::libpq::ssl_passphrase_command;
use crate::port::explicit_bzero::explicit_bzero;

// ----------------------------------------------------------------
//   <sys/stat.h> + <unistd.h> bindings
//
//   The C source uses stat(2)/geteuid(2) and the S_IS*/permission macros.
//   We bind libc stat()/geteuid() and read st_uid/st_mode out of the
//   filled-in `struct stat` via a small platform-aware accessor, mirroring
//   the approach in src/common/file_perm.rs.
// ----------------------------------------------------------------

/// POSIX `uid_t`: 32-bit unsigned on every supported platform.
#[allow(non_camel_case_types)]
type uid_t = c_uint;

/// File-mode bit type.
#[allow(non_camel_case_types)]
type mode_t = u32;

// File-mode bit masks (<sys/stat.h>); identical octal values across platforms.
const S_IFMT: mode_t = 0o170000;
const S_IFREG: mode_t = 0o100000;
const S_IRWXG: mode_t = 0o000070; // rwx by group
const S_IRWXO: mode_t = 0o000007; // rwx by others
const S_IWGRP: mode_t = 0o000020; // write by group
const S_IXGRP: mode_t = 0o000010; // execute by group

/// `S_ISREG(m)` macro.
#[inline]
fn S_ISREG(m: mode_t) -> bool {
    (m & S_IFMT) == S_IFREG
}

/// Opaque, over-allocated stand-in for `struct stat`.  256 bytes covers the
/// real struct on every platform PostgreSQL targets; 16-byte alignment
/// satisfies the largest member's alignment.
#[repr(C, align(16))]
struct stat_buf {
    bytes: [u8; 256],
}

extern "C" {
    // On Darwin the public `stat` symbol is `stat$INODE64`; on Linux it is the
    // bare `stat`.
    #[cfg_attr(target_os = "macos", link_name = "stat$INODE64")]
    /// `int stat(const char *path, struct stat *buf);`
    fn stat(path: *const c_char, buf: *mut stat_buf) -> c_int;

    /// geteuid(2): effective user ID of the calling process.
    fn geteuid() -> uid_t;
}

/// Read `st_mode` out of a filled-in `struct stat` buffer.
///
/// On Linux/glibc x86-64 st_mode sits at offset 24 (after st_dev, st_ino,
/// st_nlink); on Darwin it sits at offset 4 (after st_dev).
///
/// # Safety
/// `buf` must have been fully populated by a successful `stat()` call.
unsafe fn stat_st_mode(buf: *const stat_buf) -> mode_t {
    let base = buf as *const u8;
    #[cfg(target_os = "macos")]
    {
        // Darwin: mode_t is u16, at offset 4 (after st_dev: i32).
        core::ptr::read_unaligned(base.add(4) as *const u16) as mode_t
    }
    #[cfg(not(target_os = "macos"))]
    {
        // Linux x86-64: mode_t is u32, at offset 24.
        core::ptr::read_unaligned(base.add(24) as *const u32) as mode_t
    }
}

/// Read `st_uid` out of a filled-in `struct stat` buffer.
///
/// On Darwin st_uid sits at offset 16 (st_dev:i32, st_mode:u16, st_nlink:u16,
/// st_ino:u64).  On Linux x86-64 st_uid sits at offset 28 (after st_dev:u64,
/// st_ino:u64, st_nlink:u64, st_mode:u32).
///
/// # Safety
/// `buf` must have been fully populated by a successful `stat()` call.
unsafe fn stat_st_uid(buf: *const stat_buf) -> uid_t {
    let base = buf as *const u8;
    #[cfg(target_os = "macos")]
    {
        core::ptr::read_unaligned(base.add(16) as *const u32) as uid_t
    }
    #[cfg(not(target_os = "macos"))]
    {
        core::ptr::read_unaligned(base.add(28) as *const u32) as uid_t
    }
}

// ----------------------------------------------------------------
//   errcode helpers (shimmed: classification is ignored by ereport)
// ----------------------------------------------------------------
const ERRCODE_CONFIG_FILE_ERROR: c_int = 0;

// ----------------------------------------------------------------
//   not-yet-ported callees (local stubs)
// ----------------------------------------------------------------

// TODO(pg-port): port src/backend/storage/file/fd.c OpenPipeStream/ClosePipeStream.
unsafe fn OpenPipeStream(_command: *const c_char, _mode: *const c_char) -> *mut c_void {
    unimplemented!()
}

// TODO(pg-port): port src/backend/storage/file/fd.c ClosePipeStream.
unsafe fn ClosePipeStream(_file: *mut c_void) -> c_int {
    unimplemented!()
}

// TODO(pg-port): port errcode_for_file_access() from src/backend/utils/error/elog.c.
fn errcode_for_file_access() -> c_int {
    0
}

extern "C" {
    /// fgets(3): read a line from a FILE* stream into `buf`.
    fn fgets(buf: *mut c_char, size: c_int, stream: *mut c_void) -> *mut c_char;

    /// ferror(3): test the error indicator for a stream.
    fn ferror(stream: *mut c_void) -> c_int;
}

/// Run ssl_passphrase_command
///
/// prompt will be substituted for %p.  is_server_start determines the loglevel
/// of error messages.
///
/// The result will be put in buffer buf, which is of size size.  The return
/// value is the length of the actual result.
///
/// # Safety
/// `prompt` must be a valid NUL-terminated C string; `buf` must point to at
/// least `size` writable bytes with `size > 0`.
pub unsafe fn run_ssl_passphrase_command(
    prompt: *const c_char,
    is_server_start: bool,
    buf: *mut c_char,
    size: c_int,
) -> c_int {
    let loglevel: c_int = if is_server_start { ERROR } else { LOG };
    let command: *mut c_char;
    let fh: *mut c_void;
    let pclose_rc: c_int;
    let mut len: usize = 0;

    Assert!(!prompt.is_null());
    Assert!(size > 0);
    *buf.add(0) = b'\0' as c_char;

    command = replace_percent_placeholders(
        ssl_passphrase_command,
        c"ssl_passphrase_command".as_ptr(),
        c"p".as_ptr(),
        &[prompt],
    );

    // Control flow mirrors the C `goto error;` cleanup: each error path jumps
    // past the body to the shared pfree(command)/return.  We express this with
    // a labeled block that breaks to the cleanup tail.
    'error: {
        fh = OpenPipeStream(command, c"r".as_ptr());
        if fh.is_null() {
            let _ = errcode_for_file_access();
            ereport!(
                loglevel,
                "could not execute command: %m"
            );
            break 'error;
        }

        if fgets(buf, size, fh).is_null() {
            if ferror(fh) != 0 {
                explicit_bzero(buf as *mut c_void, size as Size);
                let _ = errcode_for_file_access();
                ereport!(loglevel, "could not read from command: %m");
                break 'error;
            }
        }

        pclose_rc = ClosePipeStream(fh);
        if pclose_rc == -1 {
            explicit_bzero(buf as *mut c_void, size as Size);
            let _ = errcode_for_file_access();
            ereport!(loglevel, "could not close pipe to external command: %m");
            break 'error;
        } else if pclose_rc != 0 {
            explicit_bzero(buf as *mut c_void, size as Size);
            let reason: *mut c_char = wait_result_to_str(pclose_rc);
            let _ = errcode_for_file_access();
            // errmsg("command \"%s\" failed") + errdetail_internal("%s", reason)
            ereport!(loglevel, "command failed");
            pfree(reason as *mut c_void);
            break 'error;
        }

        /* strip trailing newline and carriage return */
        len = pg_strip_crlf(buf) as usize;
    }

    // error:
    pfree(command as *mut c_void);
    len as c_int
}

/// Check permissions for SSL key files.
///
/// # Safety
/// `ssl_key_file` must be a valid NUL-terminated C string.
pub unsafe fn check_ssl_key_file_permissions(
    ssl_key_file: *const c_char,
    isServerStart: bool,
) -> bool {
    let loglevel: c_int = if isServerStart { FATAL } else { LOG };
    let mut buf: stat_buf = stat_buf { bytes: [0u8; 256] };

    if stat(ssl_key_file, &mut buf) != 0 {
        let _ = errcode_for_file_access();
        ereport!(loglevel, "could not access private key file: %m");
        return false;
    }

    let st_mode: mode_t = stat_st_mode(&buf);
    let st_uid: uid_t = stat_st_uid(&buf);

    /* Key file must be a regular file */
    if !S_ISREG(st_mode) {
        let _ = errcode(ERRCODE_CONFIG_FILE_ERROR);
        ereport!(loglevel, "private key file is not a regular file");
        return false;
    }

    /*
     * Refuse to load key files owned by users other than us or root, and
     * require no public access to the key file.  If the file is owned by us,
     * require mode 0600 or less.  If owned by root, require 0640 or less to
     * allow read access through either our gid or a supplementary gid that
     * allows us to read system-wide certificates.
     *
     * Note that roughly similar checks are performed in
     * src/interfaces/libpq/fe-secure-openssl.c so any changes here may need
     * to be made there as well.  The environment is different though; this
     * code can assume that we're not running as root.
     *
     * Ideally we would do similar permissions checks on Windows, but it is
     * not clear how that would work since Unix-style permissions may not be
     * available.
     */
    // #if !defined(WIN32) && !defined(__CYGWIN__)
    if st_uid != geteuid() && st_uid != 0 {
        let _ = errcode(ERRCODE_CONFIG_FILE_ERROR);
        ereport!(
            loglevel,
            "private key file must be owned by the database user or root"
        );
        return false;
    }

    if (st_uid == geteuid() && (st_mode & (S_IRWXG | S_IRWXO)) != 0)
        || (st_uid == 0 && (st_mode & (S_IWGRP | S_IXGRP | S_IRWXO)) != 0)
    {
        let _ = errcode(ERRCODE_CONFIG_FILE_ERROR);
        // errmsg("private key file has group or world access") +
        // errdetail("File must have permissions u=rw (0600) or less if owned
        // by the database user, or permissions u=rw,g=r (0640) or less if
        // owned by root.")
        ereport!(
            loglevel,
            "private key file has group or world access. File must have permissions u=rw (0600) or less if owned by the database user, or permissions u=rw,g=r (0640) or less if owned by root."
        );
        return false;
    }
    // #endif

    true
}
