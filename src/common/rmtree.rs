//! Translation of postgres/src/common/rmtree.c
//!
//! `rmtree(path, rmtopdir)` recursively deletes a directory tree.  This is a
//! src/common/ unit shared between the frontend and backend builds.  The C file
//! selects its directory primitives through OPENDIR/CLOSEDIR macros:
//!   - !FRONTEND  -> AllocateDir / FreeDir (the backend VFD layer, storage/fd.h)
//!   - FRONTEND   -> opendir / closedir (plain libc)
//! `readdir` is always plain libc in both paths.  We translate the libc
//! opendir/readdir/closedir family directly (the FRONTEND path), since the
//! backend VFD layer is not yet ported.
//!
//! #include mapping:
//!   "postgres.h" / "postgres_fe.h" -> crate::prelude (palloc/repalloc/pfree/
//!                                     pstrdup come from there).
//!   <unistd.h>, <sys/stat.h>       -> local `extern "C"` libc declarations
//!                                     below (opendir/readdir/closedir/unlink/
//!                                     rmdir/lstat + struct dirent/struct stat).
//!   "common/file_utils.h"          -> get_dirent_type + PGFileType.  file_utils
//!                                     is NOT ported yet, so get_dirent_type is
//!                                     STUBBED here with a local lstat-based impl
//!                                     (the same classification file_utils.c does
//!                                     when no d_type is trusted).
//!   "storage/fd.h" / "common/logging.h" -> pg_log_warning is STUBBED as a local
//!                                     no-op (no logging facility wired up in the
//!                                     common build yet).
//!
//! WHAT IS REAL:
//!   - The recursion structure: read the directory, defer subdirectories into a
//!     palloc'd `dirnames` vector (so only one DIR fd is open at a time), then
//!     recurse after CLOSEDIR.
//!   - The per-entry dispatch: skip "." / "..", build "path/name" with snprintf,
//!     classify with get_dirent_type, unlink() regular/other files (tolerating
//!     ENOENT), rmdir() the top dir when rmtopdir is set.
//!   - The errno handling around readdir (errno=0 before each call, checked
//!     after the loop) and the ENOENT tolerance on unlink.
//!
//! NOTE (not yet runnable end-to-end): get_dirent_type is a local lstat stub and
//! pg_log_warning is a no-op stub; the backend VFD (AllocateDir/FreeDir) path is
//! not used.  The control flow / FS syscalls are faithful, but this depends on a
//! live filesystem, so a behavioral test is omitted (only the path-join helper
//! is unit-tested).

use crate::prelude::*;

// MAXPGPATH from pg_config_manual.h (#define MAXPGPATH 1024).  Matches the value
// already used by sibling ports (port/path.rs, access/transam/xlogreader.rs).
const MAXPGPATH: usize = 1024;

// ---------------------------------------------------------------------------
// libc bindings (no `libc` crate in this project; declare what we use locally).
// ---------------------------------------------------------------------------

// Opaque DIR handle returned by opendir().
#[repr(C)]
struct DIR {
    _private: [u8; 0],
}

// struct dirent.  We only read d_name, and we deliberately give d_name a large
// fixed inline buffer at a sufficient offset.  Rather than mirror the exact
// platform layout (which varies), we treat the dirent pointer opaquely and reach
// d_name through dirent_d_name() below, which is platform-correct on the BSD/
// macOS and glibc layouts this project targets.
#[repr(C)]
struct dirent {
    _opaque: [u8; 0],
}

// struct stat: we only need st_mode and only on the platforms we build for.
// Layout differs per OS; we declare just enough and read st_mode via the
// accessor.  On macOS (Darwin) st_mode is a u16 at offset 4 (st_dev:i32,
// st_mode:u16, ...).  On Linux x86_64 st_mode is a u32 at offset 24.  We size a
// generous opaque buffer and use cfg-gated offsets in stat_st_mode().
#[repr(C)]
struct stat_buf {
    _opaque: [u8; 256],
}

// File-mode bit masks (sys/stat.h).  S_IFMT/S_IFDIR/S_IFLNK are identical across
// the Unixes we target.
const S_IFMT: u32 = 0o170000;
const S_IFDIR: u32 = 0o040000;
const S_IFLNK: u32 = 0o120000;

#[inline]
fn s_isdir(m: u32) -> bool {
    (m & S_IFMT) == S_IFDIR
}
#[inline]
fn s_islnk(m: u32) -> bool {
    (m & S_IFMT) == S_IFLNK
}

// ENOENT (errno) - identical value (2) on Linux and macOS.
const ENOENT: c_int = 2;

extern "C" {
    fn opendir(name: *const c_char) -> *mut DIR;
    fn readdir(dirp: *mut DIR) -> *mut dirent;
    fn closedir(dirp: *mut DIR) -> c_int;
    fn unlink(pathname: *const c_char) -> c_int;
    fn rmdir(pathname: *const c_char) -> c_int;
    fn lstat(pathname: *const c_char, statbuf: *mut stat_buf) -> c_int;
    fn strcmp(a: *const c_char, b: *const c_char) -> c_int;
    fn snprintf(s: *mut c_char, n: Size, fmt: *const c_char, ...) -> c_int;
}

// errno is a thread-local accessed through a per-platform function: __error() on
// macOS/BSD, __errno_location() on Linux/glibc.
#[cfg(target_os = "macos")]
extern "C" {
    #[link_name = "__error"]
    fn errno_location() -> *mut c_int;
}
#[cfg(not(target_os = "macos"))]
extern "C" {
    #[link_name = "__errno_location"]
    fn errno_location() -> *mut c_int;
}

#[inline]
unsafe fn get_errno() -> c_int {
    *errno_location()
}
#[inline]
unsafe fn set_errno(v: c_int) {
    *errno_location() = v;
}

// d_name lives at a platform-specific offset within struct dirent:
//   - macOS/BSD: struct dirent { d_ino:u64; d_seekoff:u64; d_reclen:u16;
//                d_namlen:u16; d_type:u8; d_name:[c_char;...] } -> offset 21.
//   - glibc:     struct dirent { d_ino:u64; d_off:i64; d_reclen:u16;
//                d_type:u8; d_name:[c_char;256] } -> offset 19.
#[inline]
unsafe fn dirent_d_name(de: *mut dirent) -> *const c_char {
    #[cfg(target_os = "macos")]
    let off: isize = 21;
    #[cfg(not(target_os = "macos"))]
    let off: isize = 19;
    (de as *const u8).offset(off) as *const c_char
}

// st_mode location within struct stat:
//   - macOS: st_dev:i32 (0), st_mode:u16 (4).
//   - Linux x86_64: ... st_mode:u32 at offset 24.
#[inline]
unsafe fn stat_st_mode(sb: *const stat_buf) -> u32 {
    let base = sb as *const u8;
    #[cfg(target_os = "macos")]
    {
        (*(base.offset(4) as *const u16)) as u32
    }
    #[cfg(not(target_os = "macos"))]
    {
        *(base.offset(24) as *const u32)
    }
}

// ---------------------------------------------------------------------------
// get_dirent_type stub (common/file_utils.h is not ported yet).
//
// The real get_dirent_type uses the dirent's d_type when the platform supplies a
// trustworthy one and otherwise falls back to lstat().  Our stub always lstat()s
// the full path, which is the always-correct (if slower) classification, and is
// exactly the path file_utils.c takes when d_type is DT_UNKNOWN.  TODO: replace
// with the real common::file_utils::get_dirent_type once that file is ported.
// ---------------------------------------------------------------------------

#[allow(non_camel_case_types)]
#[derive(PartialEq, Eq, Clone, Copy)]
#[repr(C)]
enum PGFileType {
    PGFILETYPE_ERROR,
    PGFILETYPE_UNKNOWN,
    PGFILETYPE_REG,
    PGFILETYPE_DIR,
    PGFILETYPE_LNK,
}

// Mirrors get_dirent_type(path, de, look_through_links, elevel).  We ignore
// `de` (we always lstat) and `elevel` (logging is stubbed).
unsafe fn get_dirent_type(
    path: *const c_char,
    _de: *mut dirent,
    _look_through_links: bool,
) -> PGFileType {
    let mut sb: stat_buf = stat_buf { _opaque: [0u8; 256] };
    if lstat(path, &mut sb as *mut stat_buf) != 0 {
        // already-logged in the real version; we just report ERROR.
        return PGFileType::PGFILETYPE_ERROR;
    }
    let mode = stat_st_mode(&sb as *const stat_buf);
    if s_isdir(mode) {
        PGFileType::PGFILETYPE_DIR
    } else if s_islnk(mode) {
        PGFileType::PGFILETYPE_LNK
    } else {
        // S_ISREG and everything else (fifo/sock/dev) -> treat as a file to
        // unlink, matching the C switch default branch behaviour.
        PGFileType::PGFILETYPE_REG
    }
}

// pg_log_warning stub (no logging facility wired up in the common build yet).
// The C macro expands to elog(WARNING, ...) in the backend and pg_log_warning()
// in the frontend; here it is a no-op.  TODO: route to the real logging layer.
#[inline]
unsafe fn pg_log_warning(_msg: &str) {}

// ---------------------------------------------------------------------------
// rmtree
// ---------------------------------------------------------------------------

/// Delete a directory tree recursively.
///
/// Assumes `path` points to a valid directory.  Deletes everything under `path`.
/// If `rmtopdir` is true, deletes the directory too.  Returns true on success,
/// false if there was any problem (details already reported via pg_log_warning).
///
/// # Safety
/// `path` must be a valid NUL-terminated C string pointer.
pub unsafe fn rmtree(path: *const c_char, rmtopdir: bool) -> bool {
    let mut pathbuf: [c_char; MAXPGPATH] = [0; MAXPGPATH];
    let mut result = true;
    let mut dirnames_size: Size = 0;
    let mut dirnames_capacity: Size = 8;

    let dir = opendir(path);
    if dir.is_null() {
        pg_log_warning("could not open directory");
        return false;
    }

    // dirnames is a palloc'd array of pstrdup'd subdirectory paths, grown like
    // the C repalloc loop.  Each element is owned and pfree'd below.
    let mut dirnames =
        palloc((core::mem::size_of::<*mut c_char>() * dirnames_capacity) as Size)
            as *mut *mut c_char;

    // FMT = "%s/%s\0" used to build pathbuf.
    let fmt = b"%s/%s\0".as_ptr() as *const c_char;

    loop {
        set_errno(0);
        let de = readdir(dir);
        if de.is_null() {
            break;
        }

        let d_name = dirent_d_name(de);
        if strcmp(d_name, b".\0".as_ptr() as *const c_char) == 0
            || strcmp(d_name, b"..\0".as_ptr() as *const c_char) == 0
        {
            continue;
        }

        snprintf(
            pathbuf.as_mut_ptr(),
            MAXPGPATH as Size,
            fmt,
            path,
            d_name,
        );

        match get_dirent_type(pathbuf.as_ptr(), de, false) {
            PGFileType::PGFILETYPE_ERROR => {
                // already logged, press on
            }
            PGFileType::PGFILETYPE_DIR => {
                // Defer recursion until after we've closed this directory, to
                // avoid using more than one file descriptor at a time.
                if dirnames_size == dirnames_capacity {
                    dirnames = repalloc(
                        dirnames as *mut c_void,
                        (core::mem::size_of::<*mut c_char>()
                            * dirnames_capacity
                            * 2) as Size,
                    ) as *mut *mut c_char;
                    dirnames_capacity *= 2;
                }
                *dirnames.add(dirnames_size) = pstrdup(pathbuf.as_ptr());
                dirnames_size += 1;
            }
            _ => {
                if unlink(pathbuf.as_ptr()) != 0 && get_errno() != ENOENT {
                    pg_log_warning("could not remove file");
                    result = false;
                }
            }
        }
    }

    if get_errno() != 0 {
        pg_log_warning("could not read directory");
        result = false;
    }

    closedir(dir);

    // Now recurse into the subdirectories we found.
    let mut i: Size = 0;
    while i < dirnames_size {
        let sub = *dirnames.add(i);
        if !rmtree(sub, true) {
            result = false;
        }
        pfree(sub as *mut c_void);
        i += 1;
    }

    if rmtopdir {
        if rmdir(path) != 0 {
            pg_log_warning("could not remove directory");
            result = false;
        }
    }

    pfree(dirnames as *mut c_void);

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    // rmtree itself needs a live filesystem (opendir/unlink/rmdir), so a
    // behavioral test is intentionally omitted.  We test only the pure
    // "%s/%s" path-join behaviour that the snprintf call relies on, to lock in
    // the buffer/format contract.
    #[test]
    fn path_join_builds_slash_separated() {
        unsafe {
            let mut buf: [c_char; MAXPGPATH] = [0; MAXPGPATH];
            let fmt = b"%s/%s\0".as_ptr() as *const c_char;
            let base = b"/tmp/foo\0".as_ptr() as *const c_char;
            let name = b"bar\0".as_ptr() as *const c_char;
            let n = snprintf(buf.as_mut_ptr(), MAXPGPATH as Size, fmt, base, name);
            assert_eq!(n, "/tmp/foo/bar".len() as c_int);

            // Read the NUL-terminated result back into a Rust string.
            let mut out = Vec::new();
            let mut p = 0usize;
            while buf[p] != 0 {
                out.push(buf[p] as u8);
                p += 1;
            }
            assert_eq!(&out[..], b"/tmp/foo/bar");
        }
    }

    #[test]
    fn mode_bit_tests() {
        assert!(s_isdir(S_IFDIR | 0o755));
        assert!(!s_isdir(S_IFLNK | 0o777));
        assert!(s_islnk(S_IFLNK | 0o777));
        assert!(!s_islnk(S_IFDIR | 0o755));
    }
}
