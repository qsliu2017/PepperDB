//! Translated from PostgreSQL `src/port/noblock.c`
//! (declarations in `src/include/port.h`:
//! `extern bool pg_set_noblock(pgsocket sock);`
//! `extern bool pg_set_block(pgsocket sock);`).
//!
//! set a file descriptor as blocking or non-blocking
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!   src/port/noblock.c

use crate::prelude::*;

// port.h: `typedef int pgsocket;` on non-Windows platforms.
#[allow(non_camel_case_types)]
pub type pgsocket = c_int;

// Raw libc binding for <fcntl.h>'s fcntl(2). PostgreSQL's port code may run
// before any backend infrastructure exists, so we call the system primitive
// directly. fcntl is variadic; F_GETFL takes no third arg, F_SETFL takes an
// int flags arg.
extern "C" {
    fn fcntl(fd: c_int, cmd: c_int, ...) -> c_int;
}

// <fcntl.h> command constants. F_GETFL/F_SETFL are 3/4 on both Linux and
// macOS/BSD.
const F_GETFL: c_int = 3;
const F_SETFL: c_int = 4;

// O_NONBLOCK differs by platform: 0x0004 on macOS/BSD, 0o4000 (0x800) on Linux.
#[cfg(any(
    target_os = "macos",
    target_os = "ios",
    target_os = "freebsd",
    target_os = "openbsd",
    target_os = "netbsd",
    target_os = "dragonfly"
))]
const O_NONBLOCK: c_int = 0x0004;
#[cfg(not(any(
    target_os = "macos",
    target_os = "ios",
    target_os = "freebsd",
    target_os = "openbsd",
    target_os = "netbsd",
    target_os = "dragonfly"
)))]
const O_NONBLOCK: c_int = 0o4000;

/*
 * Put socket into nonblock mode.
 * Returns true on success, false on failure.
 */
// TODO(pg-port): the `#else` WIN32 branch (ioctlsocket(sock, FIONBIO, ...))
// is not translated; we always provide the non-Windows fcntl() path.
pub unsafe fn pg_set_noblock(sock: pgsocket) -> bool {
    let flags: c_int;

    flags = fcntl(sock, F_GETFL);
    if flags < 0 {
        return false;
    }
    if fcntl(sock, F_SETFL, flags | O_NONBLOCK) == -1 {
        return false;
    }
    true
}

/*
 * Put socket into blocking mode.
 * Returns true on success, false on failure.
 */
// TODO(pg-port): the `#else` WIN32 branch (ioctlsocket(sock, FIONBIO, ...))
// is not translated; we always provide the non-Windows fcntl() path.
pub unsafe fn pg_set_block(sock: pgsocket) -> bool {
    let flags: c_int;

    flags = fcntl(sock, F_GETFL);
    if flags < 0 {
        return false;
    }
    if fcntl(sock, F_SETFL, flags & !O_NONBLOCK) == -1 {
        return false;
    }
    true
}
