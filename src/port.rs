//! Translated from PostgreSQL src/include/port.h

#![allow(clippy::ptr_arg, reason = "TODO(stub): drop when implemented; hollow stubs mirror PG signatures 1:1")]

// === scaffold: child modules (Phase 0) ===
pub mod atomics;
pub mod cygwin;
pub mod darwin;
pub mod freebsd;
pub mod linux;
pub mod netbsd;
pub mod openbsd;
pub mod pg_bitutils;
pub mod pg_bswap;
pub mod pg_crc32c;
pub mod pg_iovec;
pub mod pg_lfind;
pub mod pg_numa;
pub mod pg_pthread;
pub mod simd;
pub mod solaris;
pub mod win32;
pub mod win32_msvc;
pub mod win32_port;
pub mod win32ntdll;
// === end scaffold ===

// src/port/ compatibility functions (port.h). Targets are Linux x86_64 + macOS
// aarch64 only; all WIN32/CYGWIN paths are dropped. Many of these are std-covered
// at call sites (file I/O, qsort -> slice::sort, snprintf -> format!); the
// signatures are kept here for the skeleton. COMPAT-SENSITIVE items
// (pg_strong_random, locale/encoding, qsort comparators) are noted.

/// C: `typedef int pgsocket;` on non-Windows.
pub type pgsocket = i32;
/// C: `PGINVALID_SOCKET (-1)`.
pub const PGINVALID_SOCKET: pgsocket = -1;

pub fn pg_set_noblock(sock: pgsocket) -> bool {
    unimplemented!()
}
pub fn pg_set_block(sock: pgsocket) -> bool {
    unimplemented!()
}

// --- Portable path handling (path.c). C uses caller-provided char* buffers;
// here they return owned Strings / borrow slices instead. ---
pub fn has_drive_prefix(path: &str) -> bool {
    // always false on non-Windows
    false
}
pub fn first_dir_separator(filename: &str) -> Option<usize> {
    filename.find('/')
}
pub fn last_dir_separator(filename: &str) -> Option<usize> {
    filename.rfind('/')
}
pub fn first_path_var_separator(pathlist: &str) -> Option<usize> {
    pathlist.find(':')
}
pub fn join_path_components(head: &str, tail: &str) -> String {
    unimplemented!()
}
pub fn canonicalize_path(path: &mut String) {
    unimplemented!()
}
pub fn canonicalize_path_enc(path: &mut String, encoding: i32) {
    unimplemented!()
}
pub fn make_native_path(filename: &mut String) {
    // no-op on non-Windows
}
pub fn cleanup_path(path: &mut String) {
    unimplemented!()
}
pub fn path_contains_parent_reference(path: &str) -> bool {
    unimplemented!()
}
pub fn path_is_relative_and_below_cwd(path: &str) -> bool {
    unimplemented!()
}
pub fn path_is_safe_for_extraction(path: &str) -> bool {
    unimplemented!()
}
pub fn path_is_prefix_of_path(path1: &str, path2: &str) -> bool {
    unimplemented!()
}
pub fn make_absolute_path(path: &str) -> String {
    unimplemented!()
}
pub fn get_progname(argv0: &str) -> String {
    unimplemented!()
}
pub fn get_share_path(my_exec_path: &str) -> String {
    unimplemented!()
}
pub fn get_etc_path(my_exec_path: &str) -> String {
    unimplemented!()
}
pub fn get_include_path(my_exec_path: &str) -> String {
    unimplemented!()
}
pub fn get_pkginclude_path(my_exec_path: &str) -> String {
    unimplemented!()
}
pub fn get_includeserver_path(my_exec_path: &str) -> String {
    unimplemented!()
}
pub fn get_lib_path(my_exec_path: &str) -> String {
    unimplemented!()
}
pub fn get_pkglib_path(my_exec_path: &str) -> String {
    unimplemented!()
}
pub fn get_locale_path(my_exec_path: &str) -> String {
    unimplemented!()
}
pub fn get_doc_path(my_exec_path: &str) -> String {
    unimplemented!()
}
pub fn get_html_path(my_exec_path: &str) -> String {
    unimplemented!()
}
pub fn get_man_path(my_exec_path: &str) -> String {
    unimplemented!()
}
/// C: `bool get_home_path(char *ret_path)` - the home path, or None if unset.
pub fn get_home_path() -> Option<String> {
    unimplemented!()
}
pub fn get_parent_directory(path: &mut String) {
    unimplemented!()
}

/// C: `char **pgfnames(const char *path)` - directory entry names (None on error).
pub fn pgfnames(path: &str) -> Option<Vec<String>> {
    unimplemented!()
}
// pgfnames_cleanup is unnecessary (Vec drops itself).

/// C: `IS_DIR_SEP(ch)` on non-Windows.
pub const fn is_dir_sep(ch: u8) -> bool {
    ch == b'/'
}
/// C: `is_absolute_path(filename)` on non-Windows.
pub fn is_absolute_path(filename: &str) -> bool {
    filename.starts_with('/')
}

pub fn set_pglocale_pgservice(argv0: &str, app: &str) {
    unimplemented!()
}

/// C: `int validate_exec(const char *path)` - status -> Result.
pub fn validate_exec(path: &str) -> Result<(), i32> {
    unimplemented!()
}
/// C: `int find_my_exec(argv0, char *retpath)` - resolved path or error.
pub fn find_my_exec(argv0: &str) -> Result<String, i32> {
    unimplemented!()
}
pub fn find_other_exec(argv0: &str, target: &str, versionstr: &str) -> Result<String, i32> {
    unimplemented!()
}
pub fn pipe_read_line(cmd: &str) -> Option<String> {
    unimplemented!()
}

pub const EXE: &str = "";
pub const DEVNULL: &str = "/dev/null";

/// Portable delay.
pub fn pg_usleep(microsec: i64) {
    unimplemented!()
}

// SQL-like case-independent comparisons/conversions (ASCII).
pub fn pg_strcasecmp(s1: &str, s2: &str) -> i32 {
    unimplemented!()
}
pub fn pg_strncasecmp(s1: &str, s2: &str, n: usize) -> i32 {
    unimplemented!()
}
pub fn pg_toupper(ch: u8) -> u8 {
    unimplemented!()
}
pub fn pg_tolower(ch: u8) -> u8 {
    unimplemented!()
}
pub fn pg_ascii_toupper(ch: u8) -> u8 {
    ch.to_ascii_uppercase()
}
pub fn pg_ascii_tolower(ch: u8) -> u8 {
    ch.to_ascii_lowercase()
}

// snprintf/printf family -> Rust `format!`/`write!`/`print!` at call sites.
// Tombstone: pg_snprintf, pg_vsnprintf, pg_printf, ... are not ported.

/// Robust strerror wrapper.
pub fn pg_strerror(errnum: i32) -> String {
    unimplemented!()
}
pub const PG_STRERROR_R_BUFLEN: usize = 256;
pub fn pg_strsignal(signum: i32) -> Option<String> {
    unimplemented!()
}
pub fn pclose_check(/* stream */) -> i32 {
    unimplemented!()
}

/// C: `bool rmtree(const char *path, bool rmtopdir)`.
pub fn rmtree(path: &str, rmtopdir: bool) -> bool {
    unimplemented!()
}

// getpeereid: maps to std/libc socket creds.
/// C: `int getpeereid(int sock, uid_t*, gid_t*)` -> (uid, gid) or error.
pub fn getpeereid(sock: i32) -> Result<(u32, u32), i32> {
    unimplemented!()
}

pub fn explicit_bzero(buf: &mut [u8]) {
    // zero without being optimized away; std volatile / `zeroize` later.
    unimplemented!()
}

// strlcat/strlcpy/strnlen/strsep/timingsafe_bcmp/mkdtemp/inet_aton: std/crate
// covered. Tombstone (not ported individually).

// qsort family. COMPAT-SENSITIVE only where output order is persisted; otherwise
// use slice::sort_by. Modeled generically over a comparator closure.
pub fn pg_qsort<T>(items: &mut [T], cmp: impl Fn(&T, &T) -> core::cmp::Ordering) {
    items.sort_by(cmp);
}
pub fn qsort_arg<T>(items: &mut [T], cmp: impl Fn(&T, &T) -> core::cmp::Ordering) {
    // the C `void *arg` is captured by the closure.
    items.sort_by(cmp);
}
pub fn qsort_interruptible<T>(items: &mut [T], cmp: impl Fn(&T, &T) -> core::cmp::Ordering) {
    items.sort_by(cmp);
}
/// C: `void *bsearch_arg(...)` -> Option of the matching index.
pub fn bsearch_arg<T>(items: &[T], cmp: impl Fn(&T) -> core::cmp::Ordering) -> Option<usize> {
    items.binary_search_by(cmp).ok()
}

pub fn pg_get_encoding_from_locale(ctype: &str, write_message: bool) -> i32 {
    unimplemented!()
}
pub fn pg_inet_net_ntop(af: i32, src: &[u8], bits: i32) -> Option<String> {
    unimplemented!()
}

// port/pg_strong_random.c -- COMPAT/SECURITY sensitive (CSPRNG).
pub fn pg_strong_random_init() {
    unimplemented!()
}
pub fn pg_strong_random(buf: &mut [u8]) -> bool {
    unimplemented!()
}

pub fn pg_check_dir(dir: &str) -> i32 {
    unimplemented!()
}
pub fn pg_mkdir_p(path: &str, omode: i32) -> i32 {
    unimplemented!()
}

/// C: `typedef void (*pqsigfunc)(SIGNAL_ARGS);`
pub type pqsigfunc = fn(signo: i32);
pub fn pqsignal(signo: i32, func: pqsigfunc) {
    unimplemented!()
}

pub fn escape_single_quotes_ascii(src: &str) -> String {
    unimplemented!()
}

// common/wait_error.c
pub fn wait_result_to_str(exitstatus: i32) -> String {
    unimplemented!()
}
pub fn wait_result_is_signal(exit_status: i32, signum: i32) -> bool {
    unimplemented!()
}
pub fn wait_result_is_any_signal(exit_status: i32, include_command_not_found: bool) -> bool {
    unimplemented!()
}
pub fn wait_result_to_exit_code(exit_status: i32) -> i32 {
    unimplemented!()
}
