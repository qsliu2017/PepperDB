//! Translated from PostgreSQL 18.3 `src/port/strerror.c`
//! (declarations in `src/include/port.h`).
//!
//! strerror.c
//!   Replacements for standard strerror() and strerror_r() functions
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!   src/port/strerror.c
//!
//! Port notes:
//!   - This is a non-WIN32 (Unix) build, so the `#ifdef WIN32`
//!     `win32_socket_strerror()` path and the Winsock error-code short-circuit
//!     in pg_strerror_r() are omitted.
//!   - The platform is assumed to provide a POSIX strerror_r() returning int
//!     (HAVE_STRERROR_R && STRERROR_R_INT), which is the configure result on
//!     macOS and on glibc when _GNU_SOURCE is not in effect.  See the
//!     TODO(pg-port) on the strerror_r binding below.
//!   - The C `_()` gettext macro is identity in the in-core fallback build
//!     (no NLS); we drop it.

#![allow(static_mut_refs)]

use crate::prelude::*;

/*
 * Within this file, "strerror" means the platform's function not pg_strerror,
 * and likewise for "strerror_r"
 */

// ----------------------------------------------------------------
//   libc bindings
// ----------------------------------------------------------------
//
// TODO(pg-port): glibc vs XSI strerror_r.  We bind the POSIX/XSI variant that
// returns `int` (0 on success).  On glibc with _GNU_SOURCE the GNU variant
// returns `char *` instead; this port assumes the configure result
// STRERROR_R_INT (the default unless _GNU_SOURCE forces the GNU API).
extern "C" {
    fn strerror_r(errnum: c_int, buf: *mut c_char, buflen: usize) -> c_int;

    // snprintf(buf, buflen, fmt, ...) - we only ever pass the single "%d"
    // integer argument used by the "operating system error %d" fallback, so a
    // fixed (errnum: c_int) arity is sufficient and avoids a variadic decl.
    fn snprintf(buf: *mut c_char, buflen: usize, fmt: *const c_char, errnum: c_int) -> c_int;
}

/* Recommended buffer size for strerror_r (port.h) */
const PG_STRERROR_R_BUFLEN: usize = 256;

// macOS / Linux errno values used by the get_errno_symbol() switch below.
// TODO(pg-port): platform errno values (these are the macOS <sys/errno.h>
// numbers; on Linux several differ but the symbol mapping stays the same).
//
// Per the C source's #if guards, on this platform:
//   - EWOULDBLOCK == EAGAIN, so the EWOULDBLOCK case is omitted (the
//     `(!defined(EAGAIN) || (EWOULDBLOCK != EAGAIN))` guard is false).
//   - EOPNOTSUPP != ENOTSUP, so the EOPNOTSUPP case is kept.
const E2BIG: c_int = 7;
const EACCES: c_int = 13;
const EADDRINUSE: c_int = 48;
const EADDRNOTAVAIL: c_int = 49;
const EAFNOSUPPORT: c_int = 47;
const EAGAIN: c_int = 35;
const EALREADY: c_int = 37;
const EBADF: c_int = 9;
const EBADMSG: c_int = 94;
const EBUSY: c_int = 16;
const ECHILD: c_int = 10;
const ECONNABORTED: c_int = 53;
const ECONNREFUSED: c_int = 61;
const ECONNRESET: c_int = 54;
const EDEADLK: c_int = 11;
const EDOM: c_int = 33;
const EEXIST: c_int = 17;
const EFAULT: c_int = 14;
const EFBIG: c_int = 27;
const EHOSTDOWN: c_int = 64;
const EHOSTUNREACH: c_int = 65;
const EIDRM: c_int = 90;
const EINPROGRESS: c_int = 36;
const EINTR: c_int = 4;
const EINVAL: c_int = 22;
const EIO: c_int = 5;
const EISCONN: c_int = 56;
const EISDIR: c_int = 21;
const ELOOP: c_int = 62;
const EMFILE: c_int = 24;
const EMLINK: c_int = 31;
const EMSGSIZE: c_int = 40;
const ENAMETOOLONG: c_int = 63;
const ENETDOWN: c_int = 50;
const ENETRESET: c_int = 52;
const ENETUNREACH: c_int = 51;
const ENFILE: c_int = 23;
const ENOBUFS: c_int = 55;
const ENODEV: c_int = 19;
const ENOENT: c_int = 2;
const ENOEXEC: c_int = 8;
const ENOMEM: c_int = 12;
const ENOSPC: c_int = 28;
const ENOSYS: c_int = 78;
const ENOTCONN: c_int = 57;
const ENOTDIR: c_int = 20;
const ENOTEMPTY: c_int = 66;
const ENOTSOCK: c_int = 38;
const ENOTSUP: c_int = 45;
const ENOTTY: c_int = 25;
const ENXIO: c_int = 6;
const EOPNOTSUPP: c_int = 102;
const EOVERFLOW: c_int = 84;
const EPERM: c_int = 1;
const EPIPE: c_int = 32;
const EPROTONOSUPPORT: c_int = 43;
const ERANGE: c_int = 34;
const EROFS: c_int = 30;
const ESRCH: c_int = 3;
const ETIMEDOUT: c_int = 60;
const ETXTBSY: c_int = 26;
const EXDEV: c_int = 18;

/*
 * A slightly cleaned-up version of strerror()
 */
///
/// # Safety
/// Returns a pointer into a process-wide `static mut` scratch buffer, exactly
/// like the C `static char errorstr_buf[]`; the result is therefore
/// thread-unsafe and only valid until the next pg_strerror() call.  Callers
/// must not deref it concurrently from another thread.
pub unsafe fn pg_strerror(errnum: c_int) -> *mut c_char {
    // C: static char errorstr_buf[PG_STRERROR_R_BUFLEN];
    static mut errorstr_buf: [c_char; PG_STRERROR_R_BUFLEN] = [0; PG_STRERROR_R_BUFLEN];

    pg_strerror_r(
        errnum,
        errorstr_buf.as_mut_ptr(),
        core::mem::size_of_val(&errorstr_buf),
    )
}

/*
 * A slightly cleaned-up version of strerror_r()
 */
///
/// # Safety
/// `buf` must be valid for writes of `buflen` bytes.  The returned pointer is
/// either `buf` or a pointer to a static read-only symbol string.
pub unsafe fn pg_strerror_r(errnum: c_int, buf: *mut c_char, buflen: usize) -> *mut c_char {
    let mut str: *mut c_char;

    /* If it's a Windows Winsock error, that needs special handling */
    /* (WIN32 Winsock branch omitted on this platform) */

    /* Try the platform's strerror_r(), or maybe just strerror() */
    str = gnuish_strerror_r(errnum, buf, buflen);

    /*
     * Some strerror()s return an empty string for out-of-range errno.  This
     * is ANSI C spec compliant, but not exactly useful.  Also, we may get
     * back strings of question marks if libc cannot transcode the message to
     * the codeset specified by LC_CTYPE.  If we get nothing useful, first try
     * get_errno_symbol(), and if that fails, print the numeric errno.
     */
    if str.is_null() || *str == 0 || *str == b'?' as c_char {
        str = get_errno_symbol(errnum) as *mut c_char;
    }

    if str.is_null() {
        // snprintf(buf, buflen, _("operating system error %d"), errnum);
        snprintf(buf, buflen, c"operating system error %d".as_ptr(), errnum);
        str = buf;
    }

    str
}

/*
 * Simple wrapper to emulate GNU strerror_r if what the platform provides is
 * POSIX.  Also, if platform lacks strerror_r altogether, fall back to plain
 * strerror; it might not be very thread-safe, but tough luck.
 */
///
/// # Safety
/// `buf` must be valid for writes of `buflen` bytes.
unsafe fn gnuish_strerror_r(errnum: c_int, buf: *mut c_char, buflen: usize) -> *mut c_char {
    /* HAVE_STRERROR_R && STRERROR_R_INT: POSIX API */
    if strerror_r(errnum, buf, buflen) == 0 {
        return buf;
    }
    core::ptr::null_mut() /* let caller deal with failure */

    /*
     * The #else branches of the C are not reachable in this build:
     *  - GNU API (HAVE_STRERROR_R && !STRERROR_R_INT): `return strerror_r(...)`.
     *  - !HAVE_STRERROR_R: copy the plain strerror() result into the caller's
     *    buffer with strlcpy(buf, sbuf, buflen) (see crate::port::strlcpy).
     */
}

/*
 * Returns a symbol (e.g. "ENOENT") for an errno code.
 * Returns NULL if the code is unrecognized.
 */
fn get_errno_symbol(errnum: c_int) -> *const c_char {
    match errnum {
        E2BIG => c"E2BIG".as_ptr(),
        EACCES => c"EACCES".as_ptr(),
        EADDRINUSE => c"EADDRINUSE".as_ptr(),
        EADDRNOTAVAIL => c"EADDRNOTAVAIL".as_ptr(),
        EAFNOSUPPORT => c"EAFNOSUPPORT".as_ptr(),
        EAGAIN => c"EAGAIN".as_ptr(),
        EALREADY => c"EALREADY".as_ptr(),
        EBADF => c"EBADF".as_ptr(),
        EBADMSG => c"EBADMSG".as_ptr(),
        EBUSY => c"EBUSY".as_ptr(),
        ECHILD => c"ECHILD".as_ptr(),
        ECONNABORTED => c"ECONNABORTED".as_ptr(),
        ECONNREFUSED => c"ECONNREFUSED".as_ptr(),
        ECONNRESET => c"ECONNRESET".as_ptr(),
        EDEADLK => c"EDEADLK".as_ptr(),
        EDOM => c"EDOM".as_ptr(),
        EEXIST => c"EEXIST".as_ptr(),
        EFAULT => c"EFAULT".as_ptr(),
        EFBIG => c"EFBIG".as_ptr(),
        EHOSTDOWN => c"EHOSTDOWN".as_ptr(),
        EHOSTUNREACH => c"EHOSTUNREACH".as_ptr(),
        EIDRM => c"EIDRM".as_ptr(),
        EINPROGRESS => c"EINPROGRESS".as_ptr(),
        EINTR => c"EINTR".as_ptr(),
        EINVAL => c"EINVAL".as_ptr(),
        EIO => c"EIO".as_ptr(),
        EISCONN => c"EISCONN".as_ptr(),
        EISDIR => c"EISDIR".as_ptr(),
        ELOOP => c"ELOOP".as_ptr(),
        EMFILE => c"EMFILE".as_ptr(),
        EMLINK => c"EMLINK".as_ptr(),
        EMSGSIZE => c"EMSGSIZE".as_ptr(),
        ENAMETOOLONG => c"ENAMETOOLONG".as_ptr(),
        ENETDOWN => c"ENETDOWN".as_ptr(),
        ENETRESET => c"ENETRESET".as_ptr(),
        ENETUNREACH => c"ENETUNREACH".as_ptr(),
        ENFILE => c"ENFILE".as_ptr(),
        ENOBUFS => c"ENOBUFS".as_ptr(),
        ENODEV => c"ENODEV".as_ptr(),
        ENOENT => c"ENOENT".as_ptr(),
        ENOEXEC => c"ENOEXEC".as_ptr(),
        ENOMEM => c"ENOMEM".as_ptr(),
        ENOSPC => c"ENOSPC".as_ptr(),
        ENOSYS => c"ENOSYS".as_ptr(),
        ENOTCONN => c"ENOTCONN".as_ptr(),
        ENOTDIR => c"ENOTDIR".as_ptr(),
        ENOTEMPTY => c"ENOTEMPTY".as_ptr(),
        ENOTSOCK => c"ENOTSOCK".as_ptr(),
        ENOTSUP => c"ENOTSUP".as_ptr(),
        ENOTTY => c"ENOTTY".as_ptr(),
        ENXIO => c"ENXIO".as_ptr(),
        // #if defined(EOPNOTSUPP) && (!defined(ENOTSUP) || (EOPNOTSUPP != ENOTSUP))
        // EOPNOTSUPP (102) != ENOTSUP (45) on this platform, so kept.
        EOPNOTSUPP => c"EOPNOTSUPP".as_ptr(),
        EOVERFLOW => c"EOVERFLOW".as_ptr(),
        EPERM => c"EPERM".as_ptr(),
        EPIPE => c"EPIPE".as_ptr(),
        EPROTONOSUPPORT => c"EPROTONOSUPPORT".as_ptr(),
        ERANGE => c"ERANGE".as_ptr(),
        EROFS => c"EROFS".as_ptr(),
        ESRCH => c"ESRCH".as_ptr(),
        ETIMEDOUT => c"ETIMEDOUT".as_ptr(),
        ETXTBSY => c"ETXTBSY".as_ptr(),
        // #if defined(EWOULDBLOCK) && (!defined(EAGAIN) || (EWOULDBLOCK != EAGAIN))
        // EWOULDBLOCK == EAGAIN on this platform, so the EWOULDBLOCK case is
        // omitted (it would also be an unreachable/duplicate match arm).
        EXDEV => c"EXDEV".as_ptr(),
        _ => core::ptr::null(),
    }
}

// #ifdef WIN32 win32_socket_strerror() ... #endif  (omitted on this platform)
