//! Translated from PostgreSQL `src/port/pg_strong_random.c`
//! (declarations in `src/include/port.h`:
//! `extern void pg_strong_random_init(void);`
//! `extern bool pg_strong_random(void *buf, size_t len);`).
//!
//! generate a cryptographically secure random number
//!
//! Our definition of "strong" is that it's suitable for generating random
//! salts and query cancellation keys, during authentication.
//!
//! Note: this code is run quite early in postmaster and backend startup;
//! therefore, even when built for backend, it cannot rely on backend
//! infrastructure such as elog() or palloc().
//!
//! Copyright (c) 1996-2025, PostgreSQL Global Development Group
//!
//! IDENTIFICATION
//!   src/port/pg_strong_random.c

use crate::prelude::*;

/*
 * pg_strong_random & pg_strong_random_init
 *
 * Generate requested number of random bytes. The returned bytes are
 * cryptographically secure, suitable for use e.g. in authentication.
 *
 * Before pg_strong_random is called in any process, the generator must first
 * be initialized by calling pg_strong_random_init().  Initialization is a no-
 * op for all supported randomness sources, it is kept to maintain backwards
 * compatibility with extensions.
 *
 * We rely on system facilities for actually generating the numbers.
 * We support a number of sources:
 *
 * 1. OpenSSL's RAND_bytes()
 * 2. Windows' CryptGenRandom() function
 * 3. /dev/urandom
 *
 * Returns true on success, and false if none of the sources
 * were available. NB: It is important to check the return value!
 * Proceeding with key generation when no random data was available
 * would lead to predictable keys and security issues.
 */

// TODO(pg-port): the `#ifdef USE_OPENSSL` branch (RAND_status / RAND_poll /
// RAND_bytes) and the `#elif WIN32` branch (CryptAcquireContext /
// CryptGenRandom) are not translated. We always provide the portable
// `/dev/urandom` fallback (the C `#else` branch) below.

// Raw libc bindings used by the /dev/urandom fallback. PostgreSQL's port code
// runs before any backend infrastructure exists, so we call the system
// primitives directly rather than going through std::fs.
extern "C" {
    fn open(path: *const c_char, oflag: c_int, ...) -> c_int;
    fn read(fd: c_int, buf: *mut c_void, count: Size) -> isize;
    fn close(fd: c_int) -> c_int;
}

// errno access. On macOS/BSD errno is `*__error()`; on Linux it is
// `*__errno_location()`.
#[cfg(any(target_os = "macos", target_os = "ios", target_os = "freebsd", target_os = "openbsd", target_os = "netbsd", target_os = "dragonfly"))]
extern "C" {
    #[link_name = "__error"]
    fn __errno_location() -> *mut c_int;
}
#[cfg(not(any(target_os = "macos", target_os = "ios", target_os = "freebsd", target_os = "openbsd", target_os = "netbsd", target_os = "dragonfly")))]
extern "C" {
    fn __errno_location() -> *mut c_int;
}

// <fcntl.h>: O_RDONLY is 0 on every Unix we target.
const O_RDONLY: c_int = 0;
// <errno.h>: EINTR is 4 on Linux and on macOS/BSD.
const EINTR: c_int = 4;

/*
 * Without OpenSSL or Win32 support, just read /dev/urandom ourselves.
 */

pub unsafe fn pg_strong_random_init() {
    /* No initialization needed */
}

pub unsafe fn pg_strong_random(buf: *mut c_void, mut len: Size) -> bool {
    let f: c_int;
    let mut p: *mut c_char = buf as *mut c_char;
    let mut res: isize;

    // "/dev/urandom\0"
    f = open(c"/dev/urandom".as_ptr(), O_RDONLY, 0);
    if f == -1 {
        return false;
    }

    while len != 0 {
        res = read(f, p as *mut c_void, len);
        if res <= 0 {
            if *__errno_location() == EINTR {
                continue; /* interrupted by signal, just retry */
            }

            close(f);
            return false;
        }

        p = p.add(res as usize);
        len -= res as usize;
    }

    close(f);
    true
}
