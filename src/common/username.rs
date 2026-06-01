//! Port of PostgreSQL 18.3 `src/common/username.c` and its header
//! `src/include/common/username.h` ("get user name").
//!
//! Looks up the effective username.  The non-Windows path resolves the
//! effective UID via `geteuid(2)` and `getpwuid(3)`, returning `pw->pw_name`.
//! On failure it sets `*errstr` to a palloc'd diagnostic and returns NULL.
//! `get_user_name_or_exit` is the error-checked wrapper used by frontend
//! programs, which prints the error and `exit(1)`s.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::prelude::*;

// ----------------------------------------------------------------
//   extern "C" / libc shims
//
//   The C source pulls in <pwd.h> and <unistd.h> for getpwuid()/geteuid()
//   and uses strerror()/the C runtime's per-thread errno.  We bind those
//   directly; the structure `passwd` is declared with the single field we
//   actually read (pw_name), matching the SUSv2/POSIX layout where pw_name
//   is the first member.  Trailing members are intentionally elided -- we
//   only ever dereference pw_name through the kernel-provided pointer.
// ----------------------------------------------------------------

/// POSIX `uid_t`.  Both glibc and the BSD/macOS C libraries define this as a
/// 32-bit unsigned integer.
#[allow(non_camel_case_types)]
type uid_t = c_uint;

/// Subset of `struct passwd` from <pwd.h>.  `pw_name` is the first member on
/// every supported platform, so reading it through a returned `*mut passwd`
/// is layout-correct without spelling out the remaining fields.
#[repr(C)]
struct passwd {
    pw_name: *mut c_char,
    // Remaining members (pw_passwd, pw_uid, pw_gid, ...) are not referenced.
}

extern "C" {
    /// geteuid(2): effective user ID of the calling process.
    fn geteuid() -> uid_t;

    /// getpwuid(3): look up the passwd entry for `uid`.  Returns NULL on error
    /// or when no matching entry exists (errno distinguishes the two).
    fn getpwuid(uid: uid_t) -> *mut passwd;

    /// strerror(3): textual description of an errno value.
    fn strerror(errnum: c_int) -> *const c_char;

    /// snprintf(3): used to render the palloc'd error message.
    fn snprintf(s: *mut c_char, n: usize, format: *const c_char, ...) -> c_int;

    /// fprintf(3) onto a FILE*; we only target stderr.
    fn fprintf(stream: *mut c_void, format: *const c_char, ...) -> c_int;

    /// exit(3): terminate the process.
    fn exit(status: c_int) -> !;

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

    // stderr is a `FILE *` global.  glibc names it `stderr`; the BSD/macOS C
    // library exposes the underlying array as `__stderrp`.
    #[cfg_attr(
        any(target_os = "macos", target_os = "ios", target_vendor = "apple"),
        link_name = "__stderrp"
    )]
    #[cfg_attr(
        not(any(target_os = "macos", target_os = "ios", target_vendor = "apple")),
        link_name = "stderr"
    )]
    static mut pg_stderr: *mut c_void;
}

/// Read the current C `errno` value.
#[inline]
unsafe fn errno() -> c_int {
    *pg_errno_location()
}

/// Set the current C `errno` value (the C source does `errno = 0` before the
/// getpwuid() call).
#[inline]
unsafe fn set_errno(v: c_int) {
    *pg_errno_location() = v;
}

/// Returns the current user name in a static buffer
/// On error, returns NULL and sets *errstr to point to a palloc'd message
///
/// # Safety
/// `errstr` must be a valid, writable `*mut *const c_char`.  The returned
/// pointer (when non-NULL) borrows storage owned by the C library and must
/// not be freed by the caller.
pub unsafe fn get_user_name(errstr: *mut *const c_char) -> *const c_char {
    // #ifndef WIN32
    let user_id: uid_t = geteuid();

    *errstr = null_mut();

    set_errno(0); // clear errno before call
    let pw: *mut passwd = getpwuid(user_id);
    if pw.is_null() {
        // psprintf(_("could not look up effective user ID %ld: %s"),
        //          (long) user_id,
        //          errno ? strerror(errno) : _("user does not exist"));
        let saved_errno = errno();
        let reason: *const c_char = if saved_errno != 0 {
            strerror(saved_errno)
        } else {
            c"user does not exist".as_ptr()
        };

        // Render into a palloc'd buffer (psprintf allocates in the current
        // memory context).  Size first with a NULL/0 snprintf, then allocate.
        let fmt = c"could not look up effective user ID %ld: %s".as_ptr();
        let needed = snprintf(null_mut(), 0, fmt, user_id as c_long, reason);
        let len: usize = if needed < 0 { 0 } else { needed as usize };
        let buf = palloc(len + 1) as *mut c_char;
        snprintf(buf, len + 1, fmt, user_id as c_long, reason);

        *errstr = buf as *const c_char;
        return null_mut();
    }

    (*pw).pw_name as *const c_char
    // #else  -- WIN32 GetUserName() path
    //   TODO(pg-port): Microsoft recommends buffer size of UNLEN+1 (UNLEN=256).
    //   Call GetUserName() into a static [c_char; 256 + 1]; on failure set
    //   *errstr via psprintf("user name lookup failure: error code %lu",
    //   GetLastError()) and return NULL.
    // #endif
}

/// Returns the current user name in a static buffer or exits
///
/// # Safety
/// `progname` must be NULL or a valid NUL-terminated C string.
pub unsafe fn get_user_name_or_exit(progname: *const c_char) -> *const c_char {
    let mut errstr: *const c_char = null_mut();

    let user_name = get_user_name(&mut errstr);

    if user_name.is_null() {
        // fprintf(stderr, "%s: %s\n", progname, errstr);
        fprintf(
            pg_stderr,
            c"%s: %s\n".as_ptr(),
            progname,
            errstr,
        );
        exit(1);
    }
    user_name
}
