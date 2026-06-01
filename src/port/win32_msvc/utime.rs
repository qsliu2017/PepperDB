//! port/win32_msvc/utime.h - MSVC <sys/utime.h> shim (non-unicode version)
//!
//! The C header body is a single `#include <sys/utime.h>` that pulls in the
//! MSVC CRT's non-unicode utime declarations. There are no typedefs, structs,
//! macros, or prototypes defined locally in the PostgreSQL header itself.
//!
//! For the Rust port we surface the CRT symbols the header re-exports so that
//! callers using utime()/utimbuf compile on the Windows build.

use crate::c::time_t;

/// MSVC CRT `struct _utimbuf` (from <sys/utime.h>), aliased by PostgreSQL's
/// port shims to the POSIX-style `struct utimbuf`.
#[repr(C)]
pub struct _utimbuf {
    /// access time
    pub actime: time_t,
    /// modification time
    pub modtime: time_t,
}

/// POSIX-spelling alias provided by the MSVC header for the non-unicode version.
pub type utimbuf = _utimbuf;

unsafe extern "C" {
    /// MSVC CRT `_utime` (non-unicode). Returns 0 on success, -1 on error.
    pub fn _utime(
        filename: *const crate::c::c_char,
        times: *mut _utimbuf,
    ) -> crate::c::c_int;

    /// MSVC CRT `_futime` (non-unicode). Returns 0 on success, -1 on error.
    pub fn _futime(
        fd: crate::c::c_int,
        times: *mut _utimbuf,
    ) -> crate::c::c_int;
}
