//! Translation of postgres/src/port/path.c
//!   (declarations live in postgres/src/include/port.h; only the functions
//!    defined in path.c are translated here).
//!
//! Portable path handling routines.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! The .c does `#include "postgres.h"` (or postgres_fe.h), `<ctype.h>`,
//! `<sys/stat.h>`, `<pwd.h>`, `<unistd.h>`, `"mb/pg_wchar.h"`, and
//! `"pg_config_paths.h"`.  This is the non-Windows (Unix) build:
//!   - IS_DIR_SEP(ch) == ((ch) == '/')  (from is_nonwindows path macros)
//!   - is_absolute_path(p) == (p[0] == '/')
//!   - IS_PATH_VAR_SEP(ch) == ((ch) == ':')
//!   - skip_drive(path) is the identity (no drive letters on Unix).
//! All WIN32 #ifdef branches are dropped and marked TODO(pg-port) where a
//! caller-visible function (make_native_path, cleanup_path, has_drive_prefix,
//! get_home_path) would have done Windows-only work.

use crate::prelude::*;

// strlcpy is the sibling port routine used by the buffer-filling helpers (the C
// source reaches it through port.h / c.h).
use crate::port::strlcpy::strlcpy;

// pg_strcasecmp is reached via port.h; only used in the (Windows/Cygwin) EXE
// stripping of get_progname, so it isn't referenced on this build, but kept for
// parity with the source's available declarations.
#[allow(unused_imports)]
use crate::port::pgstrcasecmp::pg_strcasecmp;

// ---------------------------------------------------------------------------
// Small helpers / constants that the C reaches through headers we don't yet
// have in the Rust tree.
// ---------------------------------------------------------------------------

// MAXPGPATH comes from pg_config_manual.h (#define MAXPGPATH 1024); the output
// buffers passed to these routines are assumed to be of this size.
const MAXPGPATH: usize = 1024;

// IS_DIR_SEP(ch) on non-Windows: ((ch) == '/').  Matches IS_NONWINDOWS_DIR_SEP.
#[inline]
fn IS_DIR_SEP(ch: c_char) -> bool {
    ch == b'/' as c_char
}

// IS_PATH_VAR_SEP(ch) on non-Windows: ((ch) == ':').
#[inline]
fn IS_PATH_VAR_SEP(ch: c_char) -> bool {
    ch == b':' as c_char
}

// is_absolute_path(filename) on non-Windows: (filename[0] == '/').
//
// # Safety
// `filename` must point to a valid NUL-terminated C string (at least one byte).
#[inline]
unsafe fn is_absolute_path(filename: *const c_char) -> bool {
    IS_DIR_SEP(*filename)
}

// TODO(pg-port): the prelude does not export libc `strlen`; provide a private
// NUL-scanning helper matching C's `strlen` over a `const char *`.
//
// # Safety
// `s` must point to a valid NUL-terminated C string.
unsafe fn strlen(s: *const c_char) -> usize {
    let mut n: usize = 0;
    while *s.add(n) != 0 {
        n += 1;
    }
    n
}

// strcmp helper matching C's `strcmp` (only used for the literal "." / ".."
// comparisons in canonicalize_path).
//
// # Safety
// `s1` and `s2` must point to valid NUL-terminated C strings.
unsafe fn strcmp(mut s1: *const c_char, mut s2: *const c_char) -> c_int {
    loop {
        let c1 = *s1 as c_uchar;
        let c2 = *s2 as c_uchar;
        if c1 != c2 || c1 == 0 {
            return c1 as c_int - c2 as c_int;
        }
        s1 = s1.add(1);
        s2 = s2.add(1);
    }
}

// strncmp helper matching C's `strncmp` (used by path_is_prefix_of_path).
//
// # Safety
// `s1` and `s2` must point to valid C strings of at least `n` readable bytes or
// be NUL-terminated within `n`.
unsafe fn strncmp(mut s1: *const c_char, mut s2: *const c_char, mut n: usize) -> c_int {
    while n != 0 {
        let c1 = *s1 as c_uchar;
        let c2 = *s2 as c_uchar;
        if c1 != c2 || c1 == 0 {
            return c1 as c_int - c2 as c_int;
        }
        s1 = s1.add(1);
        s2 = s2.add(1);
        n -= 1;
    }
    0
}

// ---------------------------------------------------------------------------
// libc bindings for the few syscalls/CRT routines path.c needs (Unix branch):
// the heap routines (malloc/free/strdup) used by make_absolute_path/
// get_progname, the sprintf used to glue cwd + path, and the POSIX calls for
// get_home_path / make_absolute_path (getenv/getcwd/geteuid/getpwuid_r).
// ---------------------------------------------------------------------------
extern "C" {
    fn malloc(size: usize) -> *mut c_void;
    fn free(ptr: *mut c_void);
    fn strdup(s: *const c_char) -> *mut c_char;
    fn getenv(name: *const c_char) -> *mut c_char;
    fn getcwd(buf: *mut c_char, size: usize) -> *mut c_char;

    // make_absolute_path's `sprintf(new, "%s/%s", buf, path)`.  Two string args,
    // so a fixed (non-variadic) signature suffices for this single call site.
    fn sprintf(buf: *mut c_char, fmt: *const c_char, a: *const c_char, b: *const c_char) -> c_int;

    // POSIX bits for get_home_path's <pwd.h> fallback.
    fn geteuid() -> uid_t;
    fn getpwuid_r(
        uid: uid_t,
        pwd: *mut passwd,
        buf: *mut c_char,
        buflen: usize,
        result: *mut *mut passwd,
    ) -> c_int;

    // Per-thread errno location for make_absolute_path's ERANGE handling.
    // macOS/BSD expose it as __error(); glibc uses __errno_location(); both
    // return `*mut c_int` (matches the convention in crate::common::string).
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

// uid_t per <sys/types.h> on darwin/glibc.
#[allow(non_camel_case_types)]
type uid_t = c_uint;

// The fields of `struct passwd` we touch (pw_dir); the rest mirror the platform
// layout so getpwuid_r can populate it.  Only pw_dir is read.
#[repr(C)]
#[allow(non_camel_case_types, dead_code)]
struct passwd {
    pw_name: *mut c_char,
    pw_passwd: *mut c_char,
    pw_uid: uid_t,
    pw_gid: uid_t,
    pw_change: c_long, // time_t (darwin); padding/ABI placeholder
    pw_class: *mut c_char,
    pw_gecos: *mut c_char,
    pw_dir: *mut c_char,
    pw_shell: *mut c_char,
    pw_expire: c_long, // time_t (darwin); padding/ABI placeholder
}

// ERANGE value (darwin/Linux agree at 34).
const ERANGE: c_int = 34;

// ---------------------------------------------------------------------------
// pg_config_paths.h constants.
//
// TODO(pg-port): these are normally generated at configure time into
// pg_config_paths.h.  Stub them with representative install-tree defaults so
// the get_*_path family + make_relative_path can link and behave sensibly.
// ---------------------------------------------------------------------------
const PGBINDIR: &core::ffi::CStr = c"/usr/local/pgsql/bin";
const PGSHAREDIR: &core::ffi::CStr = c"/usr/local/pgsql/share";
const SYSCONFDIR: &core::ffi::CStr = c"/usr/local/pgsql/etc";
const INCLUDEDIR: &core::ffi::CStr = c"/usr/local/pgsql/include";
const PKGINCLUDEDIR: &core::ffi::CStr = c"/usr/local/pgsql/include";
const INCLUDEDIRSERVER: &core::ffi::CStr = c"/usr/local/pgsql/include/server";
const LIBDIR: &core::ffi::CStr = c"/usr/local/pgsql/lib";
const PKGLIBDIR: &core::ffi::CStr = c"/usr/local/pgsql/lib";
const LOCALEDIR: &core::ffi::CStr = c"/usr/local/pgsql/share/locale";
const DOCDIR: &core::ffi::CStr = c"/usr/local/pgsql/doc";
const HTMLDIR: &core::ffi::CStr = c"/usr/local/pgsql/doc";
const MANDIR: &core::ffi::CStr = c"/usr/local/pgsql/man";

// PG_SQL_ASCII encoding id from mb/pg_wchar.h; canonicalize_path passes this to
// canonicalize_path_enc.  TODO(pg-port): full pg_wchar.h not yet translated.
const PG_SQL_ASCII: c_int = 0;

// ---------------------------------------------------------------------------
// unconstify(char *, p) is just a const-cast in C; here our pointers are
// already `*const`/`*mut` so the casts are explicit `as *mut c_char`.
// ---------------------------------------------------------------------------

/*
 * skip_drive
 *
 * On Windows, a path may begin with "C:" or "//network/".  Advance over
 * this and point to the effective start of the path.
 */
// On non-Windows, `#define skip_drive(path) (path)` -- the identity.
#[inline]
fn skip_drive(path: *const c_char) -> *const c_char {
    path
}

/*
 *	has_drive_prefix
 *
 * Return true if the given pathname has a drive prefix.
 */
//
// # Safety
// `path` must point to a valid NUL-terminated C string.
pub unsafe fn has_drive_prefix(_path: *const c_char) -> bool {
    // #ifdef WIN32: return skip_drive(path) != path;  TODO(pg-port)
    false
}

/*
 *	first_dir_separator
 *
 * Find the location of the first directory separator, return
 * NULL if not found.
 */
//
// # Safety
// `filename` must point to a valid NUL-terminated C string.
pub unsafe fn first_dir_separator(filename: *const c_char) -> *mut c_char {
    let mut p = skip_drive(filename);
    while *p != 0 {
        if IS_DIR_SEP(*p) {
            return p as *mut c_char; /* unconstify(char *, p) */
        }
        p = p.add(1);
    }
    null_mut()
}

/*
 *	first_path_var_separator
 *
 * Find the location of the first path separator (i.e. ':' on
 * Unix, ';' on Windows), return NULL if not found.
 */
//
// # Safety
// `pathlist` must point to a valid NUL-terminated C string.
pub unsafe fn first_path_var_separator(pathlist: *const c_char) -> *mut c_char {
    /* skip_drive is not needed */
    let mut p = pathlist;
    while *p != 0 {
        if IS_PATH_VAR_SEP(*p) {
            return p as *mut c_char; /* unconstify(char *, p) */
        }
        p = p.add(1);
    }
    null_mut()
}

/*
 *	last_dir_separator
 *
 * Find the location of the last directory separator, return
 * NULL if not found.
 */
//
// # Safety
// `filename` must point to a valid NUL-terminated C string.
pub unsafe fn last_dir_separator(filename: *const c_char) -> *mut c_char {
    let mut p = skip_drive(filename);
    let mut ret: *const c_char = null();
    while *p != 0 {
        if IS_DIR_SEP(*p) {
            ret = p;
        }
        p = p.add(1);
    }
    ret as *mut c_char /* unconstify(char *, ret) */
}

/*
 *	make_native_path - on WIN32, change '/' to '\' in the path
 *
 *	This reverses the '\'-to-'/' transformation of debackslash_path.
 *	We need not worry about encodings here, since '/' does not appear
 *	as a byte of a multibyte character in any supported encoding.
 *
 *	This is required because WIN32 COPY is an internal CMD.EXE
 *	command and doesn't process forward slashes in the same way
 *	as external commands.  Quoting the first argument to COPY
 *	does not convert forward to backward slashes, but COPY does
 *	properly process quoted forward slashes in the second argument.
 *
 *	COPY works with quoted forward slashes in the first argument
 *	only if the current directory is the same as the directory
 *	of the first argument.
 */
//
// # Safety
// `filename` must point to a valid NUL-terminated C string (mutable).
pub unsafe fn make_native_path(_filename: *mut c_char) {
    // #ifdef WIN32: replace '/' with '\\'.  TODO(pg-port): no-op on Unix.
}

/*
 * This function cleans up the paths for use with either cmd.exe or Msys
 * on Windows. We need them to use filenames without spaces, for which a
 * short filename is the safest equivalent, eg:
 *		C:/Progra~1/
 *
 * Presently, this is only used on paths that we can assume are in a
 * server-safe encoding, so there's no need for an encoding-aware variant.
 */
//
// # Safety
// `path` must point to a valid NUL-terminated C string (mutable).
pub unsafe fn cleanup_path(_path: *mut c_char) {
    // #ifdef WIN32: GetShortPathName + debackslash_path.  TODO(pg-port): no-op
    // on Unix.
}

/*
 * join_path_components - join two path components, inserting a slash
 *
 * We omit the slash if either given component is empty.
 *
 * ret_path is the output area (must be of size MAXPGPATH)
 *
 * ret_path can be the same as head, but not the same as tail.
 */
//
// # Safety
// `ret_path` must be a writable buffer of size MAXPGPATH; `head`/`tail` must be
// valid NUL-terminated C strings; `ret_path` may alias `head` but not `tail`.
pub unsafe fn join_path_components(
    ret_path: *mut c_char,
    head: *const c_char,
    tail: *const c_char,
) {
    if ret_path != head as *mut c_char {
        strlcpy(ret_path, head, MAXPGPATH);
    }

    /*
     * We used to try to simplify some cases involving "." and "..", but now
     * we just leave that to be done by canonicalize_path() later.
     */

    if *tail != 0 {
        /* only separate with slash if head wasn't empty */
        let cur = strlen(ret_path);
        let sep: *const c_char = if *(skip_drive(head)) != 0 {
            c"/".as_ptr()
        } else {
            c"".as_ptr()
        };
        // snprintf(ret_path + strlen, MAXPGPATH - strlen, "%s%s", sep, tail);
        // We emit the two pieces with two bounded copies to reproduce the
        // "%s%s" behavior without a variadic call.
        let room = MAXPGPATH - cur;
        my_snprintf_2s(ret_path.add(cur), room, sep, tail);
    }
}

// Helper reproducing `snprintf(dst, size, "%s%s", a, b)` for join_path_components
// (bounded, always NUL-terminated like snprintf).  Returns nothing meaningful to
// the caller (the C ignores snprintf's return here).
//
// # Safety
// `dst` writable for `size` bytes; `a`/`b` valid NUL-terminated C strings.
unsafe fn my_snprintf_2s(dst: *mut c_char, size: usize, a: *const c_char, b: *const c_char) {
    if size == 0 {
        return;
    }
    let mut i: usize = 0;
    let limit = size - 1; /* reserve space for the NUL */
    // First the separator string, then the tail.
    for whole in [a, b] {
        let mut src = whole;
        while *src != 0 {
            if i >= limit {
                break;
            }
            *dst.add(i) = *src;
            i += 1;
            src = src.add(1);
        }
        if i >= limit {
            break;
        }
    }
    *dst.add(i) = 0;
}

/* State-machine states for canonicalize_path */
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, PartialEq, Eq)]
enum canonicalize_state {
    ABSOLUTE_PATH_INIT,       /* Just past the leading '/' (and Windows drive
                               * name if any) of an absolute path */
    ABSOLUTE_WITH_N_DEPTH,    /* We collected 'pathdepth' directories in an
                               * absolute path */
    RELATIVE_PATH_INIT,       /* At start of a relative path */
    RELATIVE_WITH_N_DEPTH,    /* We collected 'pathdepth' directories in a
                               * relative path */
    RELATIVE_WITH_PARENT_REF, /* Relative path containing only double-dots */
}
use canonicalize_state::*;

/*
 * canonicalize_path()
 *
 *	Clean up path by:
 *		o  make Win32 path use Unix slashes
 *		o  remove trailing quote on Win32
 *		o  remove trailing slash
 *		o  remove duplicate (adjacent) separators
 *		o  remove '.' (unless path reduces to only '.')
 *		o  process '..' ourselves, removing it if possible
 *	Modifies path in-place.
 *
 * This comes in two variants: encoding-aware and not.  The non-aware version
 * is only safe to use on strings that are in a server-safe encoding.
 */
//
// # Safety
// `path` must point to a valid, mutable, NUL-terminated C string.
#[no_mangle]
pub unsafe fn canonicalize_path(path: *mut c_char) {
    /* All server-safe encodings are alike here, so just use PG_SQL_ASCII */
    canonicalize_path_enc(path, PG_SQL_ASCII);
}

//
// # Safety
// `path` must point to a valid, mutable, NUL-terminated C string.
pub unsafe fn canonicalize_path_enc(path: *mut c_char, _encoding: c_int) {
    let mut p: *mut c_char;
    let mut to_p: *mut c_char;
    let spath: *mut c_char;
    let mut parsed: *mut c_char;
    let mut unparse: *mut c_char;
    let mut was_sep: bool = false;
    let mut state: canonicalize_state;
    let mut pathdepth: c_int = 0; /* counts collected regular directory names */

    // #ifdef WIN32: debackslash_path + trailing-quote handling.  TODO(pg-port):
    // not needed on Unix; the encoding/mblen handling (pg_encoding_mblen /
    // pg_sjis_mblen) is Windows-only and assumed single-byte here.

    /*
     * Removing the trailing slash on a path means we never get ugly double
     * trailing slashes. Also, Win32 can't stat() a directory with a trailing
     * slash. Don't remove a leading slash, though.
     */
    trim_trailing_separator(path);

    /*
     * Remove duplicate adjacent separators
     */
    p = path;
    // #ifdef WIN32: Don't remove leading double-slash on Win32 (skipped).
    to_p = p;
    while *p != 0 {
        /* Handle many adjacent slashes, like "/a///b" */
        while *p == b'/' as c_char && was_sep {
            p = p.add(1);
        }
        if to_p != p {
            *to_p = *p;
        }
        was_sep = *p == b'/' as c_char;
        p = p.add(1);
        to_p = to_p.add(1);
    }
    *to_p = 0;

    /*
     * Remove any uses of "." and process ".." ourselves
     *
     * Note that "/../.." should reduce to just "/", while "../.." has to be
     * kept as-is.  Also note that we want a Windows drive spec to be visible
     * to trim_directory(), but it's not part of the logic that's looking at
     * the name components; hence distinction between path and spath.
     *
     * This loop overwrites the path in-place.  This is safe since we'll never
     * make the path longer.  "unparse" points to where we are reading the
     * path, "parse" to where we are writing.
     */
    spath = skip_drive(path) as *mut c_char;
    if *spath == 0 {
        return; /* empty path is returned as-is */
    }

    if *spath == b'/' as c_char {
        state = ABSOLUTE_PATH_INIT;
        /* Skip the leading slash for absolute path */
        parsed = spath.add(1);
        unparse = parsed;
    } else {
        state = RELATIVE_PATH_INIT;
        parsed = spath;
        unparse = spath;
    }

    while *unparse != 0 {
        let mut unparse_next: *mut c_char;
        let is_double_dot: bool;

        /* Split off this dir name, and set unparse_next to the next one */
        unparse_next = unparse;
        while *unparse_next != 0 && *unparse_next != b'/' as c_char {
            unparse_next = unparse_next.add(1);
        }
        if *unparse_next != 0 {
            *unparse_next = 0;
            unparse_next = unparse_next.add(1);
        }

        /* Identify type of this dir name */
        if strcmp(unparse, c".".as_ptr()) == 0 {
            /* We can ignore "." components in all cases */
            unparse = unparse_next;
            continue;
        }

        if strcmp(unparse, c"..".as_ptr()) == 0 {
            is_double_dot = true;
        } else {
            /* adjacent separators were eliminated above */
            Assert!(*unparse != 0);
            is_double_dot = false;
        }

        match state {
            ABSOLUTE_PATH_INIT => {
                /* We can ignore ".." immediately after / */
                if !is_double_dot {
                    /* Append first dir name (we already have leading slash) */
                    parsed = append_subdir_to_path(parsed, unparse);
                    state = ABSOLUTE_WITH_N_DEPTH;
                    pathdepth += 1;
                }
            }
            ABSOLUTE_WITH_N_DEPTH => {
                if is_double_dot {
                    /* Remove last parsed dir */
                    /* (trim_directory won't remove the leading slash) */
                    *parsed = 0;
                    parsed = trim_directory(path);
                    pathdepth -= 1;
                    if pathdepth == 0 {
                        state = ABSOLUTE_PATH_INIT;
                    }
                } else {
                    /* Append normal dir */
                    *parsed = b'/' as c_char;
                    parsed = parsed.add(1);
                    parsed = append_subdir_to_path(parsed, unparse);
                    pathdepth += 1;
                }
            }
            RELATIVE_PATH_INIT => {
                if is_double_dot {
                    /* Append irreducible double-dot (..) */
                    parsed = append_subdir_to_path(parsed, unparse);
                    state = RELATIVE_WITH_PARENT_REF;
                } else {
                    /* Append normal dir */
                    parsed = append_subdir_to_path(parsed, unparse);
                    state = RELATIVE_WITH_N_DEPTH;
                    pathdepth += 1;
                }
            }
            RELATIVE_WITH_N_DEPTH => {
                if is_double_dot {
                    /* Remove last parsed dir */
                    *parsed = 0;
                    parsed = trim_directory(path);
                    pathdepth -= 1;
                    if pathdepth == 0 {
                        /*
                         * If the output path is now empty, we're back to the
                         * INIT state.  However, we could have processed a
                         * path like "../dir/.." and now be down to "..", in
                         * which case enter the correct state for that.
                         */
                        if parsed == spath {
                            state = RELATIVE_PATH_INIT;
                        } else {
                            state = RELATIVE_WITH_PARENT_REF;
                        }
                    }
                } else {
                    /* Append normal dir */
                    *parsed = b'/' as c_char;
                    parsed = parsed.add(1);
                    parsed = append_subdir_to_path(parsed, unparse);
                    pathdepth += 1;
                }
            }
            RELATIVE_WITH_PARENT_REF => {
                if is_double_dot {
                    /* Append next irreducible double-dot (..) */
                    *parsed = b'/' as c_char;
                    parsed = parsed.add(1);
                    parsed = append_subdir_to_path(parsed, unparse);
                } else {
                    /* Append normal dir */
                    *parsed = b'/' as c_char;
                    parsed = parsed.add(1);
                    parsed = append_subdir_to_path(parsed, unparse);

                    /*
                     * We can now start counting normal dirs.  But if later
                     * double-dots make us remove this dir again, we'd better
                     * revert to RELATIVE_WITH_PARENT_REF not INIT state.
                     */
                    state = RELATIVE_WITH_N_DEPTH;
                    pathdepth = 1;
                }
            }
        }

        unparse = unparse_next;
    }

    /*
     * If our output path is empty at this point, insert ".".  We don't want
     * to do this any earlier because it'd result in an extra dot in corner
     * cases such as "../dir/..".  Since we rejected the wholly-empty-path
     * case above, there is certainly room.
     */
    if parsed == spath {
        *parsed = b'.' as c_char;
        parsed = parsed.add(1);
    }

    /* And finally, ensure the output path is nul-terminated. */
    *parsed = 0;
}

/*
 * Detect whether a path contains any parent-directory references ("..")
 *
 * The input *must* have been put through canonicalize_path previously.
 */
//
// # Safety
// `path` must point to a valid NUL-terminated C string.
pub unsafe fn path_contains_parent_reference(path: *const c_char) -> bool {
    /*
     * Once canonicalized, an absolute path cannot contain any ".." at all,
     * while a relative path could contain ".."(s) only at the start.  So it
     * is sufficient to check the start of the path, after skipping any
     * Windows drive/network specifier.
     */
    let path = skip_drive(path); /* C: shouldn't affect our conclusion */

    if *path == b'.' as c_char
        && *path.add(1) == b'.' as c_char
        && (*path.add(2) == 0 || *path.add(2) == b'/' as c_char)
    {
        return true;
    }

    false
}

/*
 * Detect whether a path is only in or below the current working directory.
 *
 * The input *must* have been put through canonicalize_path previously.
 *
 * An absolute path that matches the current working directory should
 * return false (we only want relative to the cwd).
 */
//
// # Safety
// `path` must point to a valid NUL-terminated C string.
pub unsafe fn path_is_relative_and_below_cwd(path: *const c_char) -> bool {
    if is_absolute_path(path) {
        false
    }
    /* don't allow anything above the cwd */
    else if path_contains_parent_reference(path) {
        false
    }
    // #ifdef WIN32: extra handling for 'E:abc' drive-relative paths.
    // TODO(pg-port): not applicable on Unix.
    else {
        true
    }
}

/*
 * Detect whether path1 is a prefix of path2 (including equality).
 *
 * This is pretty trivial, but it seems better to export a function than
 * to export IS_DIR_SEP.
 */
//
// # Safety
// `path1` and `path2` must point to valid NUL-terminated C strings.
pub unsafe fn path_is_prefix_of_path(path1: *const c_char, path2: *const c_char) -> bool {
    let path1_len = strlen(path1) as c_int;

    if strncmp(path1, path2, path1_len as usize) == 0
        && (IS_DIR_SEP(*path2.add(path1_len as usize)) || *path2.add(path1_len as usize) == 0)
    {
        return true;
    }
    false
}

/*
 * Extracts the actual name of the program as called -
 * stripped of .exe suffix if any
 */
//
// # Safety
// `argv0` must point to a valid NUL-terminated C string.
pub unsafe fn get_progname(argv0: *const c_char) -> *const c_char {
    let nodir_name: *const c_char;
    let progname: *mut c_char;

    let sep = last_dir_separator(argv0);
    if !sep.is_null() {
        nodir_name = sep.add(1) as *const c_char;
    } else {
        nodir_name = skip_drive(argv0);
    }

    /*
     * Make a copy in case argv[0] is modified by ps_status. Leaks memory, but
     * called only once.
     */
    progname = strdup(nodir_name);
    if progname.is_null() {
        // fprintf(stderr, "%s: out of memory\n", nodir_name);
        let name = core::ffi::CStr::from_ptr(nodir_name).to_string_lossy();
        eprintln!("{name}: out of memory");
        std::process::abort(); /* This could exit the postmaster */
    }

    // #if defined(__CYGWIN__) || defined(WIN32): strip ".exe" suffix.
    // TODO(pg-port): EXE is "" on Unix, nothing to strip.

    progname as *const c_char
}

/*
 * dir_strcmp: strcmp except any two DIR_SEP characters are considered equal,
 * and we honor filesystem case insensitivity if known
 */
//
// # Safety
// `s1` and `s2` must point to valid NUL-terminated C strings.
unsafe fn dir_strcmp(mut s1: *const c_char, mut s2: *const c_char) -> c_int {
    while *s1 != 0 && *s2 != 0 {
        // On non-Windows: compare bytes directly (case-sensitive).
        if *s1 != *s2 && !(IS_DIR_SEP(*s1) && IS_DIR_SEP(*s2)) {
            return *s1 as c_int - *s2 as c_int;
        }
        s1 = s1.add(1);
        s2 = s2.add(1);
    }
    if *s1 != 0 {
        return 1; /* s1 longer */
    }
    if *s2 != 0 {
        return -1; /* s2 longer */
    }
    0
}

/*
 * make_relative_path - make a path relative to the actual binary location
 *
 * This function exists to support relocation of installation trees.
 *
 *	ret_path is the output area (must be of size MAXPGPATH)
 *	target_path is the compiled-in path to the directory we want to find
 *	bin_path is the compiled-in path to the directory of executables
 *	my_exec_path is the actual location of my executable
 *
 * We determine the common prefix of target_path and bin_path, then compare
 * the remainder of bin_path to the last directory component(s) of
 * my_exec_path.  If they match, build the result as the part of my_exec_path
 * preceding the match, joined to the remainder of target_path.  If no match,
 * return target_path as-is.
 *
 * For example:
 *		target_path  = '/usr/local/share/postgresql'
 *		bin_path	 = '/usr/local/bin'
 *		my_exec_path = '/opt/pgsql/bin/postgres'
 * Given these inputs, the common prefix is '/usr/local/', the tail of
 * bin_path is 'bin' which does match the last directory component of
 * my_exec_path, so we would return '/opt/pgsql/share/postgresql'
 */
//
// # Safety
// `ret_path` must be a writable buffer of size MAXPGPATH; `target_path`,
// `bin_path`, `my_exec_path` must be valid NUL-terminated C strings.
unsafe fn make_relative_path(
    ret_path: *mut c_char,
    target_path: *const c_char,
    bin_path: *const c_char,
    my_exec_path: *const c_char,
) {
    let mut prefix_len: c_int;
    let tail_start: c_int;
    let tail_len: c_int;
    let mut i: c_int;

    /*
     * Determine the common prefix --- note we require it to end on a
     * directory separator, consider eg '/usr/lib' and '/usr/libexec'.
     */
    prefix_len = 0;
    i = 0;
    while *target_path.add(i as usize) != 0 && *bin_path.add(i as usize) != 0 {
        if IS_DIR_SEP(*target_path.add(i as usize)) && IS_DIR_SEP(*bin_path.add(i as usize)) {
            prefix_len = i + 1;
        } else if *target_path.add(i as usize) != *bin_path.add(i as usize) {
            break;
        }
        i += 1;
    }
    if prefix_len == 0 {
        /* no common prefix? */
        strlcpy(ret_path, target_path, MAXPGPATH);
        canonicalize_path(ret_path);
        return;
    }
    tail_len = strlen(bin_path) as c_int - prefix_len;

    /*
     * Set up my_exec_path without the actual executable name, and
     * canonicalize to simplify comparison to bin_path.
     */
    strlcpy(ret_path, my_exec_path, MAXPGPATH);
    trim_directory(ret_path); /* remove my executable name */
    canonicalize_path(ret_path);

    /*
     * Tail match?
     */
    tail_start = strlen(ret_path) as c_int - tail_len;
    if tail_start > 0
        && IS_DIR_SEP(*ret_path.add((tail_start - 1) as usize))
        && dir_strcmp(
            ret_path.add(tail_start as usize),
            bin_path.add(prefix_len as usize),
        ) == 0
    {
        *ret_path.add(tail_start as usize) = 0;
        trim_trailing_separator(ret_path);
        join_path_components(ret_path, ret_path, target_path.add(prefix_len as usize));
        canonicalize_path(ret_path);
        return;
    }

    /* no_match: */
    strlcpy(ret_path, target_path, MAXPGPATH);
    canonicalize_path(ret_path);
}

/*
 * make_absolute_path
 *
 * If the given pathname isn't already absolute, make it so, interpreting
 * it relative to the current working directory.
 *
 * Also canonicalizes the path.  The result is always a malloc'd copy.
 *
 * In backend, failure cases result in ereport(ERROR); in frontend,
 * we write a complaint on stderr and return NULL.
 *
 * Note: interpretation of relative-path arguments during postmaster startup
 * should happen before doing ChangeToDataDir(), else the user will probably
 * not like the results.
 */
//
// # Safety
// `path` may be NULL; if non-NULL it must point to a valid NUL-terminated C
// string.  The returned pointer (when non-NULL) is malloc'd and owned by the
// caller.
pub unsafe fn make_absolute_path(path: *const c_char) -> *mut c_char {
    let new: *mut c_char;

    /* Returning null for null input is convenient for some callers */
    if path.is_null() {
        return null_mut();
    }

    if !is_absolute_path(path) {
        let mut buf: *mut c_char;
        let mut buflen: usize;

        buflen = MAXPGPATH;
        loop {
            buf = malloc(buflen) as *mut c_char;
            if buf.is_null() {
                // #ifndef FRONTEND: ereport(ERROR, out of memory).
                elog!(ERROR, "{}", "out of memory");
            }

            if !getcwd(buf, buflen).is_null() {
                break;
            } else if *pg_errno_location() == ERANGE {
                free(buf as *mut c_void);
                buflen *= 2;
                continue;
            } else {
                let save_errno = *pg_errno_location();
                free(buf as *mut c_void);
                *pg_errno_location() = save_errno;
                // #ifndef FRONTEND:
                elog!(ERROR, "{}", "could not get current working directory");
            }
        }

        new = malloc(strlen(buf) + strlen(path) + 2) as *mut c_char;
        if new.is_null() {
            free(buf as *mut c_void);
            elog!(ERROR, "{}", "out of memory");
        }
        // sprintf(new, "%s/%s", buf, path);
        sprintf(new, c"%s/%s".as_ptr(), buf, path);
        free(buf as *mut c_void);
    } else {
        new = strdup(path);
        if new.is_null() {
            elog!(ERROR, "{}", "out of memory");
        }
    }

    /* Make sure punctuation is canonical, too */
    canonicalize_path(new);

    new
}

/*
 *	get_share_path
 */
//
// # Safety
// `my_exec_path` is a valid C string; `ret_path` is a MAXPGPATH buffer.
pub unsafe fn get_share_path(my_exec_path: *const c_char, ret_path: *mut c_char) {
    make_relative_path(ret_path, PGSHAREDIR.as_ptr(), PGBINDIR.as_ptr(), my_exec_path);
}

/*
 *	get_etc_path
 */
//
// # Safety
// `my_exec_path` is a valid C string; `ret_path` is a MAXPGPATH buffer.
pub unsafe fn get_etc_path(my_exec_path: *const c_char, ret_path: *mut c_char) {
    make_relative_path(ret_path, SYSCONFDIR.as_ptr(), PGBINDIR.as_ptr(), my_exec_path);
}

/*
 *	get_include_path
 */
//
// # Safety
// `my_exec_path` is a valid C string; `ret_path` is a MAXPGPATH buffer.
pub unsafe fn get_include_path(my_exec_path: *const c_char, ret_path: *mut c_char) {
    make_relative_path(ret_path, INCLUDEDIR.as_ptr(), PGBINDIR.as_ptr(), my_exec_path);
}

/*
 *	get_pkginclude_path
 */
//
// # Safety
// `my_exec_path` is a valid C string; `ret_path` is a MAXPGPATH buffer.
pub unsafe fn get_pkginclude_path(my_exec_path: *const c_char, ret_path: *mut c_char) {
    make_relative_path(ret_path, PKGINCLUDEDIR.as_ptr(), PGBINDIR.as_ptr(), my_exec_path);
}

/*
 *	get_includeserver_path
 */
//
// # Safety
// `my_exec_path` is a valid C string; `ret_path` is a MAXPGPATH buffer.
pub unsafe fn get_includeserver_path(my_exec_path: *const c_char, ret_path: *mut c_char) {
    make_relative_path(ret_path, INCLUDEDIRSERVER.as_ptr(), PGBINDIR.as_ptr(), my_exec_path);
}

/*
 *	get_lib_path
 */
//
// # Safety
// `my_exec_path` is a valid C string; `ret_path` is a MAXPGPATH buffer.
pub unsafe fn get_lib_path(my_exec_path: *const c_char, ret_path: *mut c_char) {
    make_relative_path(ret_path, LIBDIR.as_ptr(), PGBINDIR.as_ptr(), my_exec_path);
}

/*
 *	get_pkglib_path
 */
//
// # Safety
// `my_exec_path` is a valid C string; `ret_path` is a MAXPGPATH buffer.
pub unsafe fn get_pkglib_path(my_exec_path: *const c_char, ret_path: *mut c_char) {
    make_relative_path(ret_path, PKGLIBDIR.as_ptr(), PGBINDIR.as_ptr(), my_exec_path);
}

/*
 *	get_locale_path
 */
//
// # Safety
// `my_exec_path` is a valid C string; `ret_path` is a MAXPGPATH buffer.
pub unsafe fn get_locale_path(my_exec_path: *const c_char, ret_path: *mut c_char) {
    make_relative_path(ret_path, LOCALEDIR.as_ptr(), PGBINDIR.as_ptr(), my_exec_path);
}

/*
 *	get_doc_path
 */
//
// # Safety
// `my_exec_path` is a valid C string; `ret_path` is a MAXPGPATH buffer.
pub unsafe fn get_doc_path(my_exec_path: *const c_char, ret_path: *mut c_char) {
    make_relative_path(ret_path, DOCDIR.as_ptr(), PGBINDIR.as_ptr(), my_exec_path);
}

/*
 *	get_html_path
 */
//
// # Safety
// `my_exec_path` is a valid C string; `ret_path` is a MAXPGPATH buffer.
pub unsafe fn get_html_path(my_exec_path: *const c_char, ret_path: *mut c_char) {
    make_relative_path(ret_path, HTMLDIR.as_ptr(), PGBINDIR.as_ptr(), my_exec_path);
}

/*
 *	get_man_path
 */
//
// # Safety
// `my_exec_path` is a valid C string; `ret_path` is a MAXPGPATH buffer.
pub unsafe fn get_man_path(my_exec_path: *const c_char, ret_path: *mut c_char) {
    make_relative_path(ret_path, MANDIR.as_ptr(), PGBINDIR.as_ptr(), my_exec_path);
}

/*
 *	get_home_path
 *
 * On Unix, this actually returns the user's home directory.  On Windows
 * it returns the PostgreSQL-specific application data folder.
 */
//
// # Safety
// `ret_path` must be a writable buffer of size MAXPGPATH.
pub unsafe fn get_home_path(ret_path: *mut c_char) -> bool {
    /*
     * We first consult $HOME.  If that's unset, try to get the info from
     * <pwd.h>.
     */
    let home: *const c_char;

    home = getenv(c"HOME".as_ptr());
    if !home.is_null() && *home != 0 {
        strlcpy(ret_path, home, MAXPGPATH);
        true
    } else {
        let mut pwbuf: passwd = core::mem::zeroed();
        let mut pw: *mut passwd = null_mut();
        let mut buf: [c_char; 1024] = [0; 1024];
        let rc: c_int;

        rc = getpwuid_r(
            geteuid(),
            &mut pwbuf,
            buf.as_mut_ptr(),
            core::mem::size_of::<[c_char; 1024]>(),
            &mut pw,
        );
        if rc != 0 || pw.is_null() {
            return false;
        }
        strlcpy(ret_path, (*pw).pw_dir, MAXPGPATH);
        true
    }
}

/*
 * get_parent_directory
 *
 * Modify the given string in-place to name the parent directory of the
 * named file.
 *
 * If the input is just a file name with no directory part, the result is
 * an empty string, not ".".  This is appropriate when the next step is
 * join_path_components(), but might need special handling otherwise.
 *
 * Caution: this will not produce desirable results if the string ends
 * with "..".  For most callers this is not a problem since the string
 * is already known to name a regular file.  If in doubt, apply
 * canonicalize_path() first.
 */
//
// # Safety
// `path` must point to a valid, mutable, NUL-terminated C string.
#[no_mangle]
pub unsafe fn get_parent_directory(path: *mut c_char) {
    trim_directory(path);
}

/*
 *	trim_directory
 *
 *	Trim trailing directory from path, that is, remove any trailing slashes,
 *	the last pathname component, and the slash just ahead of it --- but never
 *	remove a leading slash.
 *
 * For the convenience of canonicalize_path, the path's new end location
 * is returned.
 */
//
// # Safety
// `path` must point to a valid, mutable, NUL-terminated C string.
unsafe fn trim_directory(path: *mut c_char) -> *mut c_char {
    let mut p: *mut c_char;

    let path = skip_drive(path) as *mut c_char;

    if *path == 0 {
        return path;
    }

    /* back up over trailing slash(es) */
    p = path.add(strlen(path) - 1);
    while IS_DIR_SEP(*p) && p > path {
        p = p.sub(1);
    }
    /* back up over directory name */
    while !IS_DIR_SEP(*p) && p > path {
        p = p.sub(1);
    }
    /* if multiple slashes before directory name, remove 'em all */
    while p > path && IS_DIR_SEP(*p.sub(1)) {
        p = p.sub(1);
    }
    /* don't erase a leading slash */
    if p == path && IS_DIR_SEP(*p) {
        p = p.add(1);
    }
    *p = 0;
    p
}

/*
 *	trim_trailing_separator
 *
 * trim off trailing slashes, but not a leading slash
 */
//
// # Safety
// `path` must point to a valid, mutable, NUL-terminated C string.
unsafe fn trim_trailing_separator(path: *mut c_char) {
    let mut p: *mut c_char;

    let path = skip_drive(path) as *mut c_char;
    p = path.add(strlen(path));
    if p > path {
        p = p.sub(1);
        while p > path && IS_DIR_SEP(*p) {
            *p = 0;
            p = p.sub(1);
        }
    }
}

/*
 *	append_subdir_to_path
 *
 * Append the currently-considered subdirectory name to the output
 * path in canonicalize_path.  Return the new end location of the
 * output path.
 *
 * Since canonicalize_path updates the path in-place, we must use
 * memmove not memcpy, and we don't yet terminate the path with '\0'.
 */
//
// # Safety
// `path` and `subdir` must be valid pointers; `path` writable for `len` bytes
// (they may overlap, hence core::ptr::copy / memmove).
unsafe fn append_subdir_to_path(path: *mut c_char, subdir: *mut c_char) -> *mut c_char {
    let len: usize = strlen(subdir);

    /* No need to copy data if path and subdir are the same. */
    if path != subdir {
        core::ptr::copy(subdir, path, len); /* memmove */
    }

    path.add(len)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Helper: build a NUL-terminated mutable buffer (MAXPGPATH) from a &str.
    fn buf_of(s: &str) -> Vec<c_char> {
        let mut v: Vec<c_char> = Vec::with_capacity(MAXPGPATH);
        for &b in s.as_bytes() {
            v.push(b as c_char);
        }
        v.push(0);
        // pad to a comfortable size so in-place ops never overflow
        while v.len() < MAXPGPATH {
            v.push(0);
        }
        v
    }

    fn as_str(buf: &[c_char]) -> String {
        let mut out = String::new();
        for &c in buf {
            if c == 0 {
                break;
            }
            out.push(c as u8 as char);
        }
        out
    }

    #[test]
    fn canonicalize_basic() {
        unsafe {
            let cases = [
                ("/usr//local/./bin/", "/usr/local/bin"),
                ("/usr/local/../share", "/usr/share"),
                ("/../..", "/"),
                ("../..", "../.."),
                ("./", "."),
                ("a/b/../c", "a/c"),
                ("../dir/..", ".."),
                ("foo/", "foo"),
            ];
            for (input, expected) in cases {
                let mut b = buf_of(input);
                canonicalize_path(b.as_mut_ptr());
                assert_eq!(as_str(&b), expected, "input {input}");
            }
        }
    }

    #[test]
    fn separators_and_prefix() {
        unsafe {
            let s = buf_of("/a/b/c");
            let p = first_dir_separator(s.as_ptr());
            assert_eq!(p, s.as_ptr() as *mut c_char);
            let last = last_dir_separator(s.as_ptr());
            assert_eq!(last, s.as_ptr().add(4) as *mut c_char);

            assert!(path_is_prefix_of_path(c"/a/b".as_ptr(), c"/a/b/c".as_ptr()));
            assert!(path_is_prefix_of_path(c"/a/b".as_ptr(), c"/a/b".as_ptr()));
            assert!(!path_is_prefix_of_path(c"/a/b".as_ptr(), c"/a/bc".as_ptr()));

            assert!(path_contains_parent_reference(c"../x".as_ptr()));
            assert!(!path_contains_parent_reference(c"x/..".as_ptr()));
            assert!(path_is_relative_and_below_cwd(c"a/b".as_ptr()));
            assert!(!path_is_relative_and_below_cwd(c"/a/b".as_ptr()));
            assert!(!path_is_relative_and_below_cwd(c"../a".as_ptr()));
        }
    }

    #[test]
    fn join_components() {
        unsafe {
            let mut out = buf_of("");
            join_path_components(out.as_mut_ptr(), c"/usr/local".as_ptr(), c"bin".as_ptr());
            assert_eq!(as_str(&out), "/usr/local/bin");

            let mut out2 = buf_of("");
            join_path_components(out2.as_mut_ptr(), c"".as_ptr(), c"bin".as_ptr());
            assert_eq!(as_str(&out2), "bin");

            let mut out3 = buf_of("");
            join_path_components(out3.as_mut_ptr(), c"/usr".as_ptr(), c"".as_ptr());
            assert_eq!(as_str(&out3), "/usr");
        }
    }

    #[test]
    fn parent_directory() {
        unsafe {
            let mut p = buf_of("/usr/local/bin");
            get_parent_directory(p.as_mut_ptr());
            assert_eq!(as_str(&p), "/usr/local");

            let mut q = buf_of("file");
            get_parent_directory(q.as_mut_ptr());
            assert_eq!(as_str(&q), "");
        }
    }
}
