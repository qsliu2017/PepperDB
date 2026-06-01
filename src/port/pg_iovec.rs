//! port/pg_iovec.h - Header for vectored I/O functions, to use in place of <sys/uio.h>.

use crate::c::Size;
use std::ffi::{c_int, c_void};

// On non-Windows, `struct iovec` comes from <sys/uio.h>. PostgreSQL defines its
// own POSIX-compatible struct only on Windows; we provide it unconditionally
// here so the symbol is always available in the Rust port.
/// Define our own POSIX-compatible iovec struct.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct iovec {
    pub iov_base: *mut c_void,
    pub iov_len: Size,
}

// ssize_t / off_t platform integer types used by the prototypes below.
// TODO: dedup
pub type ssize_t = isize;
pub type off_t = i64;

/// If <limits.h> didn't define IOV_MAX, define our own. X/Open requires at
/// least 16. (GNU Hurd apparently feel that they're not bound by X/Open,
/// because they don't define this symbol at all.)
pub const IOV_MAX: c_int = 16;

/// Define a reasonable maximum that is safe to use on the stack in arrays of
/// struct iovec and other small types. The operating system could limit us to
/// a number as low as 16, but most systems have 1024.
///
/// PG_IOV_MAX = Min(IOV_MAX, 128)
pub const PG_IOV_MAX: c_int = if IOV_MAX < 128 { IOV_MAX } else { 128 };

/// Like preadv(), but with a prefix to remind us of a side-effect: on Windows
/// this changes the current file position.
#[inline]
pub unsafe fn pg_preadv(
    fd: c_int,
    iov: *const iovec,
    iovcnt: c_int,
    offset: off_t,
) -> ssize_t {
    unimplemented!()
}

/// Like pwritev(), but with a prefix to remind us of a side-effect: on Windows
/// this changes the current file position.
#[inline]
pub unsafe fn pg_pwritev(
    fd: c_int,
    iov: *const iovec,
    iovcnt: c_int,
    offset: off_t,
) -> ssize_t {
    unimplemented!()
}
