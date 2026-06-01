//! port.h - Header for src/port/ compatibility functions.
//!
//! Faithful 1:1 translation of PostgreSQL 18.3 `src/include/port.h`.
//! We emit the non-WIN32 / non-CYGWIN default branch unconditionally (PepperDB
//! targets Unix), with comments marking the platform-conditional originals.
//! System library prototypes (qsort, snprintf, strlcpy, ...) are declared as
//! `pub unsafe fn ... -> Ret { unimplemented!() }` per project convention.

#![allow(non_camel_case_types)]
#![allow(improper_ctypes)]

use crate::c::{int64, uint8, Size};
use std::ffi::{c_char, c_double, c_float, c_int, c_long, c_uchar, c_uint, c_void};

// pgsocket lives in crate::port::noblock in this tree.
use crate::port::noblock::pgsocket;

// ---------------------------------------------------------------------------
// Local stubs for system / not-yet-ported types referenced by prototypes.
// TODO: dedup against a future libc/system-types module.
// ---------------------------------------------------------------------------
pub type FILE = c_void; // C stdio FILE
pub type va_list = *mut c_void; // C <stdarg.h> va_list
pub type uid_t = c_uint; // POSIX uid_t
pub type gid_t = c_uint; // POSIX gid_t
pub type off_t = i64; // POSIX off_t (LFS)
// Opaque system structs used by pointer/by-ref prototypes.
pub type lconv = c_void; // C <locale.h> struct lconv
pub type in_addr = c_void; // <netinet/in.h> struct in_addr

// ---------------------------------------------------------------------------
// pgsocket (non-WIN32 branch) and the invalid-socket sentinel.
//   #ifndef WIN32: typedef int pgsocket; #define PGINVALID_SOCKET (-1)
// pgsocket itself is imported from crate::port::noblock above.
// ---------------------------------------------------------------------------
pub const PGINVALID_SOCKET: pgsocket = -1;

// if platform lacks socklen_t, we assume this will work:
//   #ifndef HAVE_SOCKLEN_T  typedef unsigned int socklen_t;
pub type socklen_t = c_uint;

// ---------------------------------------------------------------------------
// non-blocking (port/noblock.c)
// ---------------------------------------------------------------------------
pub unsafe fn pg_set_noblock(_sock: pgsocket) -> bool {
    unimplemented!()
}
pub unsafe fn pg_set_block(_sock: pgsocket) -> bool {
    unimplemented!()
}

// ---------------------------------------------------------------------------
// Portable path handling for Unix/Win32 (in path.c)
// ---------------------------------------------------------------------------
pub unsafe fn has_drive_prefix(_path: *const c_char) -> bool {
    unimplemented!()
}
pub unsafe fn first_dir_separator(_filename: *const c_char) -> *mut c_char {
    unimplemented!()
}
pub unsafe fn last_dir_separator(_filename: *const c_char) -> *mut c_char {
    unimplemented!()
}
pub unsafe fn first_path_var_separator(_pathlist: *const c_char) -> *mut c_char {
    unimplemented!()
}
pub unsafe fn join_path_components(
    _ret_path: *mut c_char,
    _head: *const c_char,
    _tail: *const c_char,
) {
    unimplemented!()
}
pub unsafe fn canonicalize_path(_path: *mut c_char) {
    unimplemented!()
}
pub unsafe fn canonicalize_path_enc(_path: *mut c_char, _encoding: c_int) {
    unimplemented!()
}
pub unsafe fn make_native_path(_filename: *mut c_char) {
    unimplemented!()
}
pub unsafe fn cleanup_path(_path: *mut c_char) {
    unimplemented!()
}
pub unsafe fn path_contains_parent_reference(_path: *const c_char) -> bool {
    unimplemented!()
}
pub unsafe fn path_is_relative_and_below_cwd(_path: *const c_char) -> bool {
    unimplemented!()
}
pub unsafe fn path_is_prefix_of_path(_path1: *const c_char, _path2: *const c_char) -> bool {
    unimplemented!()
}
pub unsafe fn make_absolute_path(_path: *const c_char) -> *mut c_char {
    unimplemented!()
}
pub unsafe fn get_progname(_argv0: *const c_char) -> *const c_char {
    unimplemented!()
}
pub unsafe fn get_share_path(_my_exec_path: *const c_char, _ret_path: *mut c_char) {
    unimplemented!()
}
pub unsafe fn get_etc_path(_my_exec_path: *const c_char, _ret_path: *mut c_char) {
    unimplemented!()
}
pub unsafe fn get_include_path(_my_exec_path: *const c_char, _ret_path: *mut c_char) {
    unimplemented!()
}
pub unsafe fn get_pkginclude_path(_my_exec_path: *const c_char, _ret_path: *mut c_char) {
    unimplemented!()
}
pub unsafe fn get_includeserver_path(_my_exec_path: *const c_char, _ret_path: *mut c_char) {
    unimplemented!()
}
pub unsafe fn get_lib_path(_my_exec_path: *const c_char, _ret_path: *mut c_char) {
    unimplemented!()
}
pub unsafe fn get_pkglib_path(_my_exec_path: *const c_char, _ret_path: *mut c_char) {
    unimplemented!()
}
pub unsafe fn get_locale_path(_my_exec_path: *const c_char, _ret_path: *mut c_char) {
    unimplemented!()
}
pub unsafe fn get_doc_path(_my_exec_path: *const c_char, _ret_path: *mut c_char) {
    unimplemented!()
}
pub unsafe fn get_html_path(_my_exec_path: *const c_char, _ret_path: *mut c_char) {
    unimplemented!()
}
pub unsafe fn get_man_path(_my_exec_path: *const c_char, _ret_path: *mut c_char) {
    unimplemented!()
}
pub unsafe fn get_home_path(_ret_path: *mut c_char) -> bool {
    unimplemented!()
}
pub unsafe fn get_parent_directory(_path: *mut c_char) {
    unimplemented!()
}

// ---------------------------------------------------------------------------
// common/pgfnames.c
// ---------------------------------------------------------------------------
pub unsafe fn pgfnames(_path: *const c_char) -> *mut *mut c_char {
    unimplemented!()
}
pub unsafe fn pgfnames_cleanup(_filenames: *mut *mut c_char) {
    unimplemented!()
}

// ---------------------------------------------------------------------------
// Directory-separator / absolute-path macros.
// ---------------------------------------------------------------------------
#[inline]
pub fn IS_NONWINDOWS_DIR_SEP(ch: c_char) -> bool {
    ch == b'/' as c_char
}
#[inline]
pub unsafe fn is_nonwindows_absolute_path(filename: *const c_char) -> bool {
    IS_NONWINDOWS_DIR_SEP(*filename)
}

#[inline]
pub fn IS_WINDOWS_DIR_SEP(ch: c_char) -> bool {
    ch == b'/' as c_char || ch == b'\\' as c_char
}
/// See path_is_relative_and_below_cwd() for how we handle 'E:abc'.
#[inline]
pub unsafe fn is_windows_absolute_path(filename: *const c_char) -> bool {
    IS_WINDOWS_DIR_SEP(*filename)
        || ((*filename as c_uchar as u8).is_ascii_alphabetic()
            && *filename.add(1) == b':' as c_char
            && IS_WINDOWS_DIR_SEP(*filename.add(2)))
}

// is_absolute_path and IS_DIR_SEP -- non-WIN32 default branch.
#[inline]
pub fn IS_DIR_SEP(ch: c_char) -> bool {
    IS_NONWINDOWS_DIR_SEP(ch)
}
#[inline]
pub unsafe fn is_absolute_path(filename: *const c_char) -> bool {
    is_nonwindows_absolute_path(filename)
}

// ---------------------------------------------------------------------------
// ALL_CONNECTION_FAILURE_ERRNOS
//
// This is a C macro intended for use as the label of a `switch` case, i.e.
//   case ALL_CONNECTION_FAILURE_ERRNOS:
// expanding to a comma/`case`-separated list of errno values. It has no direct
// Rust equivalent (Rust `match` arms use `|`). We expose the underlying list as
// a slice so callers can pattern-match against it. The errno constants are the
// platform's <errno.h> values; stubbed here as 0 placeholders pending a ported
// errno module. TODO: wire to real errno values.
// ---------------------------------------------------------------------------
pub const ALL_CONNECTION_FAILURE_ERRNOS: &[c_int] = &[
    // EPIPE, ECONNRESET, ECONNABORTED, EHOSTDOWN, EHOSTUNREACH,
    // ENETDOWN, ENETRESET, ENETUNREACH, ETIMEDOUT
];

// ---------------------------------------------------------------------------
// Portable locale initialization (in exec.c)
// ---------------------------------------------------------------------------
pub unsafe fn set_pglocale_pgservice(_argv0: *const c_char, _app: *const c_char) {
    unimplemented!()
}

// Portable way to find and execute binaries (in exec.c)
pub unsafe fn validate_exec(_path: *const c_char) -> c_int {
    unimplemented!()
}
pub unsafe fn find_my_exec(_argv0: *const c_char, _retpath: *mut c_char) -> c_int {
    unimplemented!()
}
pub unsafe fn find_other_exec(
    _argv0: *const c_char,
    _target: *const c_char,
    _versionstr: *const c_char,
    _retpath: *mut c_char,
) -> c_int {
    unimplemented!()
}
pub unsafe fn pipe_read_line(_cmd: *mut c_char) -> *mut c_char {
    unimplemented!()
}

// Doesn't belong here, but this is used with find_other_exec(), so...
// #define PG_BACKEND_VERSIONSTR "postgres (PostgreSQL) " PG_VERSION "\n"
pub const PG_BACKEND_VERSIONSTR: &str =
    concat!("postgres (PostgreSQL) ", env!("CARGO_PKG_VERSION"), "\n");

// #ifdef EXEC_BACKEND: Disable ASLR before exec (in exec.c). Emitted
// unconditionally for completeness.
pub unsafe fn pg_disable_aslr() -> c_int {
    unimplemented!()
}

// EXE / DEVNULL -- non-WIN32 default branch.
pub const EXE: &str = "";
pub const DEVNULL: &str = "/dev/null";

// ---------------------------------------------------------------------------
// Portable delay handling
// ---------------------------------------------------------------------------
pub unsafe fn pg_usleep(_microsec: c_long) {
    unimplemented!()
}

// Portable SQL-like case-independent comparisons and conversions
pub unsafe fn pg_strcasecmp(_s1: *const c_char, _s2: *const c_char) -> c_int {
    unimplemented!()
}
pub unsafe fn pg_strncasecmp(_s1: *const c_char, _s2: *const c_char, _n: Size) -> c_int {
    unimplemented!()
}
pub unsafe fn pg_toupper(_ch: c_uchar) -> c_uchar {
    unimplemented!()
}
pub unsafe fn pg_tolower(_ch: c_uchar) -> c_uchar {
    unimplemented!()
}
pub unsafe fn pg_ascii_toupper(_ch: c_uchar) -> c_uchar {
    unimplemented!()
}
pub unsafe fn pg_ascii_tolower(_ch: c_uchar) -> c_uchar {
    unimplemented!()
}

// Beginning in v12, we always replace snprintf() and friends. Kept defined.
pub const USE_REPL_SNPRINTF: c_int = 1;

// ---------------------------------------------------------------------------
// Replacement *printf family (snprintf.c). The C header #defines snprintf etc.
// to these pg_* implementations; we provide the pg_* prototypes directly.
// pg_attribute_printf(...) is a no-op format-checking attribute in this port.
// ---------------------------------------------------------------------------
pub unsafe fn pg_vsnprintf(
    _str: *mut c_char,
    _count: Size,
    _fmt: *const c_char,
    _args: va_list,
) -> c_int {
    unimplemented!()
}
pub unsafe fn pg_snprintf(_str: *mut c_char, _count: Size, _fmt: *const c_char) -> c_int {
    // C variadic: int pg_snprintf(char *str, size_t count, const char *fmt, ...)
    unimplemented!()
}
pub unsafe fn pg_vsprintf(_str: *mut c_char, _fmt: *const c_char, _args: va_list) -> c_int {
    unimplemented!()
}
pub unsafe fn pg_sprintf(_str: *mut c_char, _fmt: *const c_char) -> c_int {
    // C variadic: int pg_sprintf(char *str, const char *fmt, ...)
    unimplemented!()
}
pub unsafe fn pg_vfprintf(_stream: *mut FILE, _fmt: *const c_char, _args: va_list) -> c_int {
    unimplemented!()
}
pub unsafe fn pg_fprintf(_stream: *mut FILE, _fmt: *const c_char) -> c_int {
    // C variadic: int pg_fprintf(FILE *stream, const char *fmt, ...)
    unimplemented!()
}
pub unsafe fn pg_vprintf(_fmt: *const c_char, _args: va_list) -> c_int {
    unimplemented!()
}
pub unsafe fn pg_printf(_fmt: *const c_char) -> c_int {
    // C variadic: int pg_printf(const char *fmt, ...)
    unimplemented!()
}

// #ifndef WIN32: #define pg_pread pread / #define pg_pwrite pwrite
// (pg_ prefix warns Windows impls change file position). Aliased to system
// pread/pwrite on Unix; declared as prototypes here.
pub unsafe fn pg_pread(_fd: c_int, _buf: *mut c_void, _nbyte: Size, _offset: off_t) -> isize {
    unimplemented!()
}
pub unsafe fn pg_pwrite(_fd: c_int, _buf: *const c_void, _nbyte: Size, _offset: off_t) -> isize {
    unimplemented!()
}

// This is also provided by snprintf.c
pub unsafe fn pg_strfromd(
    _str: *mut c_char,
    _count: Size,
    _precision: c_int,
    _value: c_double,
) -> c_int {
    unimplemented!()
}

// Replace strerror() with our own, somewhat more robust wrapper.
//   #define strerror pg_strerror
pub unsafe fn pg_strerror(_errnum: c_int) -> *mut c_char {
    unimplemented!()
}

// Likewise for strerror_r(); we prefer the GNU API. #define strerror_r pg_strerror_r
pub unsafe fn pg_strerror_r(_errnum: c_int, _buf: *mut c_char, _buflen: Size) -> *mut c_char {
    unimplemented!()
}
/// Recommended buffer size for strerror_r
pub const PG_STRERROR_R_BUFLEN: usize = 256;

// Wrap strsignal(), or provide our own version if necessary.
pub unsafe fn pg_strsignal(_signum: c_int) -> *const c_char {
    unimplemented!()
}

pub unsafe fn pclose_check(_stream: *mut FILE) -> c_int {
    unimplemented!()
}

// ---------------------------------------------------------------------------
// Global variable holding time zone information -- non-WIN32 default branch.
//   #define TIMEZONE_GLOBAL timezone / #define TZNAME_GLOBAL tzname
// These name the system global symbols `timezone` and `tzname`.
// ---------------------------------------------------------------------------
// (No standalone Rust symbols: TIMEZONE_GLOBAL/TZNAME_GLOBAL are token aliases.)

// WIN32-only rename/unlink wrappers (pgrename/pgunlink) -- emitted for
// completeness even though the !WIN32 build aliases nothing.
pub unsafe fn pgrename(_from: *const c_char, _to: *const c_char) -> c_int {
    unimplemented!()
}
pub unsafe fn pgunlink(_path: *const c_char) -> c_int {
    unimplemented!()
}

// WIN32-only symlink emulation (pgsymlink/pgreadlink) -- emitted for completeness.
pub unsafe fn pgsymlink(_oldpath: *const c_char, _newpath: *const c_char) -> c_int {
    unimplemented!()
}
pub unsafe fn pgreadlink(_path: *const c_char, _buf: *mut c_char, _size: Size) -> c_int {
    unimplemented!()
}

pub unsafe fn rmtree(_path: *const c_char, _rmtopdir: bool) -> bool {
    unimplemented!()
}

// PG_IOLBF -- non-WIN32 default branch maps to libc _IOLBF (line-buffered).
// Value mirrors the common glibc _IOLBF; TODO: confirm against target libc.
pub const PG_IOLBF: c_int = 1; // _IOLBF

// Type to use with fseeko/ftello (non-WIN32 branch). #define pgoff_t off_t
pub type pgoff_t = off_t;

// #ifndef HAVE_GETPEEREID / #ifndef PLPERL_HAVE_UID_GID
pub unsafe fn getpeereid(_sock: c_int, _uid: *mut uid_t, _gid: *mut gid_t) -> c_int {
    unimplemented!()
}

// #ifndef HAVE_EXPLICIT_BZERO
pub unsafe fn explicit_bzero(_buf: *mut c_void, _len: Size) {
    unimplemented!()
}

// #ifdef HAVE_BUGGY_STRTOF: #define strtof(a,b) pg_strtof(a,b)
pub unsafe fn pg_strtof(_nptr: *const c_char, _endptr: *mut *mut c_char) -> c_float {
    unimplemented!()
}

// #ifdef WIN32: src/port/win32link.c -- emitted for completeness.
pub unsafe fn link(_src: *const c_char, _dst: *const c_char) -> c_int {
    unimplemented!()
}

// #ifndef HAVE_MKDTEMP
pub unsafe fn mkdtemp(_path: *mut c_char) -> *mut c_char {
    unimplemented!()
}

// #ifndef HAVE_INET_ATON
pub unsafe fn inet_aton(_cp: *const c_char, _addr: *mut in_addr) -> c_int {
    unimplemented!()
}

// #if !HAVE_DECL_STRLCAT
pub unsafe fn strlcat(_dst: *mut c_char, _src: *const c_char, _siz: Size) -> Size {
    unimplemented!()
}
// #if !HAVE_DECL_STRLCPY
pub unsafe fn strlcpy(_dst: *mut c_char, _src: *const c_char, _siz: Size) -> Size {
    unimplemented!()
}
// #if !HAVE_DECL_STRNLEN
pub unsafe fn strnlen(_str: *const c_char, _maxlen: Size) -> Size {
    unimplemented!()
}
// #if !HAVE_DECL_STRSEP
pub unsafe fn strsep(_stringp: *mut *mut c_char, _delim: *const c_char) -> *mut c_char {
    unimplemented!()
}
// #if !HAVE_DECL_TIMINGSAFE_BCMP
pub unsafe fn timingsafe_bcmp(_b1: *const c_void, _b2: *const c_void, _len: Size) -> c_int {
    unimplemented!()
}

// ---------------------------------------------------------------------------
// qsort family. Callers should use the qsort() macro (-> pg_qsort) instead of
// calling pg_qsort() directly. #define qsort(a,b,c,d) pg_qsort(a,b,c,d)
// ---------------------------------------------------------------------------
pub type CmpFn = unsafe extern "C" fn(*const c_void, *const c_void) -> c_int;

pub unsafe fn pg_qsort(_base: *mut c_void, _nel: Size, _elsize: Size, _cmp: CmpFn) {
    unimplemented!()
}
pub unsafe fn pg_qsort_strcmp(_a: *const c_void, _b: *const c_void) -> c_int {
    unimplemented!()
}

pub type qsort_arg_comparator =
    unsafe extern "C" fn(a: *const c_void, b: *const c_void, arg: *mut c_void) -> c_int;

pub unsafe fn qsort_arg(
    _base: *mut c_void,
    _nel: Size,
    _elsize: Size,
    _cmp: qsort_arg_comparator,
    _arg: *mut c_void,
) {
    unimplemented!()
}

pub unsafe fn qsort_interruptible(
    _base: *mut c_void,
    _nel: Size,
    _elsize: Size,
    _cmp: qsort_arg_comparator,
    _arg: *mut c_void,
) {
    unimplemented!()
}

pub unsafe fn bsearch_arg(
    _key: *const c_void,
    _base0: *const c_void,
    _nmemb: Size,
    _size: Size,
    _compar: qsort_arg_comparator,
    _arg: *mut c_void,
) -> *mut c_void {
    unimplemented!()
}

// ---------------------------------------------------------------------------
// port/pg_localeconv_r.c
// ---------------------------------------------------------------------------
pub unsafe fn pg_localeconv_r(
    _lc_monetary: *const c_char,
    _lc_numeric: *const c_char,
    _output: *mut lconv,
) -> c_int {
    unimplemented!()
}
pub unsafe fn pg_localeconv_free(_lconv: *mut lconv) {
    unimplemented!()
}

// port/chklocale.c
pub unsafe fn pg_get_encoding_from_locale(_ctype: *const c_char, _write_message: bool) -> c_int {
    unimplemented!()
}

// #if defined(WIN32) && !defined(FRONTEND): pg_codepage_to_encoding -- emitted
// for completeness. UINT stubbed as c_uint.
pub unsafe fn pg_codepage_to_encoding(_cp: c_uint) -> c_int {
    unimplemented!()
}

// port/inet_net_ntop.c
pub unsafe fn pg_inet_net_ntop(
    _af: c_int,
    _src: *const c_void,
    _bits: c_int,
    _dst: *mut c_char,
    _size: Size,
) -> *mut c_char {
    unimplemented!()
}

// ---------------------------------------------------------------------------
// port/pg_strong_random.c
// ---------------------------------------------------------------------------
pub unsafe fn pg_strong_random_init() {
    unimplemented!()
}
pub unsafe fn pg_strong_random(_buf: *mut c_void, _len: Size) -> bool {
    unimplemented!()
}

// pg_backend_random used to wrap pg_strong_random before PG12.
//   #define pg_backend_random pg_strong_random
#[inline]
pub unsafe fn pg_backend_random(buf: *mut c_void, len: Size) -> bool {
    pg_strong_random(buf, len)
}

// port/pgcheckdir.c
pub unsafe fn pg_check_dir(_dir: *const c_char) -> c_int {
    unimplemented!()
}

// port/pgmkdirp.c
pub unsafe fn pg_mkdir_p(_path: *mut c_char, _omode: c_int) -> c_int {
    unimplemented!()
}

// ---------------------------------------------------------------------------
// port/pqsignal.c
//   #ifdef FRONTEND #define pqsignal pqsignal_fe #else #define pqsignal pqsignal_be
// SIGNAL_ARGS expands to `int postgres_signal_arg`; the handler signature keeps
// the C convention.
// ---------------------------------------------------------------------------
pub type pqsigfunc = unsafe extern "C" fn(postgres_signal_arg: c_int);

pub unsafe fn pqsignal(_signo: c_int, _func: pqsigfunc) {
    unimplemented!()
}

// port/quotes.c
pub unsafe fn escape_single_quotes_ascii(_src: *const c_char) -> *mut c_char {
    unimplemented!()
}

// ---------------------------------------------------------------------------
// common/wait_error.c
// ---------------------------------------------------------------------------
pub unsafe fn wait_result_to_str(_exitstatus: c_int) -> *mut c_char {
    unimplemented!()
}
pub unsafe fn wait_result_is_signal(_exit_status: c_int, _signum: c_int) -> bool {
    unimplemented!()
}
pub unsafe fn wait_result_is_any_signal(
    _exit_status: c_int,
    _include_command_not_found: bool,
) -> bool {
    unimplemented!()
}
pub unsafe fn wait_result_to_exit_code(_exit_status: c_int) -> c_int {
    unimplemented!()
}

// ---------------------------------------------------------------------------
// Interfaces that we assume all Unix systems have (non-WIN32 branch).
// ---------------------------------------------------------------------------
pub const HAVE_GETRLIMIT: c_int = 1;
pub const HAVE_POLL: c_int = 1;
pub const HAVE_POLL_H: c_int = 1;
pub const HAVE_READLINK: c_int = 1;
pub const HAVE_SETSID: c_int = 1;
pub const HAVE_SHM_OPEN: c_int = 1;
pub const HAVE_SYMLINK: c_int = 1;

// Silence unused-import warnings for crate types kept for fidelity/parity with
// the C header's integer typedefs.
const _: () = {
    let _ = core::mem::size_of::<int64>();
    let _ = core::mem::size_of::<uint8>();
};
