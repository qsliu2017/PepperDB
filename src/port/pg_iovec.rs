//! Translated from PostgreSQL src/include/port/pg_iovec.h

// Vectored I/O. On Unix targets struct iovec/preadv/pwritev come from libc;
// the I/O leaves are stubbed here and get real (likely async) impls later.

use std::os::fd::RawFd;

/// POSIX-compatible scatter/gather buffer descriptor.
pub struct IoVec<'a> {
    pub iov: &'a mut [u8],
}

/// X/Open minimum; most systems allow 1024.
pub const IOV_MAX: usize = 1024;

/// Reasonable stack-safe maximum for arrays of iovec.
pub const PG_IOV_MAX: usize = if IOV_MAX < 128 { IOV_MAX } else { 128 };

/// Like preadv(): reads into the iovecs at the given offset.
/// Returns bytes read, or an I/O error.
pub fn pg_preadv(_fd: RawFd, _iov: &mut [IoVec], _offset: u64) -> std::io::Result<usize> {
    unimplemented!()
}

/// Like pwritev(): writes the iovecs at the given offset.
/// Returns bytes written, or an I/O error.
pub fn pg_pwritev(_fd: RawFd, _iov: &[IoVec], _offset: u64) -> std::io::Result<usize> {
    unimplemented!()
}
