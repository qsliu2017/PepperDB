//! port/win32_port.h - Windows-specific compatibility stuff.
//!
//! Note this is read in MinGW as well as native Windows builds, but not in
//! Cygwin builds.  On the (non-Windows) PepperDB target none of these symbols
//! are ever used, but they are translated faithfully for completeness.  Windows
//! / Winsock / NT system types (HANDLE, DWORD, SOCKET, FILE, struct timeval,
//! struct sockaddr, fd_set, off_t, etc.) are stubbed locally as c_void aliases
//! since they are not available off-Windows.

#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use std::ffi::{c_char, c_int, c_long, c_void};

// ---------------------------------------------------------------------------
// Local stubs for Windows / system types not available off-Windows.
// TODO: dedup - these are placeholders for native Windows headers.
// ---------------------------------------------------------------------------
pub type HANDLE = *mut c_void; // Windows HANDLE
pub type DWORD = u32; // Windows DWORD
pub type BOOL = c_int; // Windows BOOL
pub type SOCKET = usize; // Windows SOCKET (UINT_PTR)
pub type FILE = c_void; // C stdio FILE
pub type c_off_t = c_long; // POSIX off_t
pub type sockaddr = c_void; // struct sockaddr
pub type fd_set = c_void; // fd_set

// struct timeval (from <sys/time.h>) - stubbed structurally for itimerval.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct timeval {
    pub tv_sec: c_long,
    pub tv_usec: c_long,
}

/*
 * Always build with SSPI support. Keep it as a #define in case
 * we want a switch to disable it sometime in the future.
 */
pub const ENABLE_SSPI: c_int = 1;

/*
 *	IPC defines
 */
pub const HAVE_UNION_SEMUN: c_int = 1;

pub const IPC_RMID: c_int = 256;
pub const IPC_CREAT: c_int = 512;
pub const IPC_EXCL: c_int = 1024;
pub const IPC_PRIVATE: c_int = 234564;
pub const IPC_NOWAIT: c_int = 2048;
pub const IPC_STAT: c_int = 4096;

pub const EACCESS: c_int = 2048;
pub const EIDRM: c_int = 4096;

pub const SETALL: c_int = 8192;
pub const GETNCNT: c_int = 16384;
pub const GETVAL: c_int = 65536;
pub const SETVAL: c_int = 131072;
pub const GETPID: c_int = 262144;

/*
 *	Signal stuff
 *
 *	For WIN32, there is no wait() call so there are no wait() macros
 *	to interpret the return value of system().  See header for the full
 *	NTSTATUS discussion.
 */
#[inline]
pub fn WIFEXITED(w: c_int) -> bool {
    (w & 0xFFFFFF00u32 as c_int) == 0
}

#[inline]
pub fn WIFSIGNALED(w: c_int) -> bool {
    !WIFEXITED(w)
}

#[inline]
pub fn WEXITSTATUS(w: c_int) -> c_int {
    w
}

#[inline]
pub fn WTERMSIG(w: c_int) -> c_int {
    w
}

#[inline]
pub fn sigmask(sig: c_int) -> c_int {
    1 << (sig - 1)
}

/* Some extra signals */
pub const SIGHUP: c_int = 1;
pub const SIGQUIT: c_int = 3;
pub const SIGTRAP: c_int = 5;
pub const SIGABRT: c_int = 22; /* Set to match W32 value -- not UNIX value */
pub const SIGKILL: c_int = 9;
pub const SIGPIPE: c_int = 13;
pub const SIGALRM: c_int = 14;
pub const SIGSTOP: c_int = 17;
pub const SIGTSTP: c_int = 18;
pub const SIGCONT: c_int = 19;
pub const SIGCHLD: c_int = 20;
pub const SIGWINCH: c_int = 28;
pub const SIGUSR1: c_int = 30;
pub const SIGUSR2: c_int = 31;

/* MinGW has gettimeofday(), but MSVC doesn't */
pub unsafe fn gettimeofday(tp: *mut timeval, tzp: *mut c_void) -> c_int {
    unimplemented!()
}

/* for setitimer in backend/port/win32/timer.c */
pub const ITIMER_REAL: c_int = 0;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct itimerval {
    pub it_interval: timeval,
    pub it_value: timeval,
}

pub unsafe fn setitimer(
    which: c_int,
    value: *const itimerval,
    ovalue: *mut itimerval,
) -> c_int {
    unimplemented!()
}

/* Convenience wrapper for GetFileType() */
pub unsafe fn pgwin32_get_file_type(hFile: HANDLE) -> DWORD {
    unimplemented!()
}

/*
 * WIN32 does not provide 64-bit off_t, but does provide the functions operating
 * with 64-bit offsets.
 */
pub type pgoff_t = i64; /* __int64 */

/*
 * fseeko/ftello: on _MSC_VER these map to _pgfseeko64/_pgftello64; otherwise to
 * fseeko64/ftello64.  We translate the _MSC_VER prototypes and the wrapper fns.
 */
pub unsafe fn _pgfseeko64(stream: *mut FILE, offset: pgoff_t, origin: c_int) -> c_int {
    unimplemented!()
}

pub unsafe fn _pgftello64(stream: *mut FILE) -> pgoff_t {
    unimplemented!()
}

#[inline]
pub unsafe fn fseeko(stream: *mut FILE, offset: pgoff_t, origin: c_int) -> c_int {
    _pgfseeko64(stream, offset, origin)
}

#[inline]
pub unsafe fn ftello(stream: *mut FILE) -> pgoff_t {
    _pgftello64(stream)
}

/*
 *	Win32 also doesn't have symlinks, but we can emulate them with
 *	junction points on newer Win32 versions.
 */
pub unsafe fn pgsymlink(oldpath: *const c_char, newpath: *const c_char) -> c_int {
    unimplemented!()
}

pub unsafe fn pgreadlink(path: *const c_char, buf: *mut c_char, size: crate::c::Size) -> c_int {
    unimplemented!()
}

#[inline]
pub unsafe fn symlink(oldpath: *const c_char, newpath: *const c_char) -> c_int {
    pgsymlink(oldpath, newpath)
}

#[inline]
pub unsafe fn readlink(path: *const c_char, buf: *mut c_char, size: crate::c::Size) -> c_int {
    pgreadlink(path, buf, size)
}

/*
 * Supplement to <sys/types.h>.
 *
 * Perl already has typedefs for uid_t and gid_t.
 */
pub type uid_t = c_int;
pub type gid_t = c_int;
pub type key_t = c_long;

/* _MSC_VER only */
pub type pid_t = c_int;

/*
 * Supplement to <sys/stat.h>.
 *
 * struct stat redefined as a copy of struct __stat64.  The MSVC underscore
 * types (_dev_t, _ino_t, __int64, __time64_t) are stubbed below.
 */
pub type _dev_t = u32;
pub type _ino_t = u16;
pub type __time64_t = i64;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct stat {
    pub st_dev: _dev_t,
    pub st_ino: _ino_t,
    pub st_mode: u16,
    pub st_nlink: i16,
    pub st_uid: i16,
    pub st_gid: i16,
    pub st_rdev: _dev_t,
    pub st_size: i64, /* __int64 */
    pub st_atime: __time64_t,
    pub st_mtime: __time64_t,
    pub st_ctime: __time64_t,
}

pub unsafe fn _pgfstat64(fileno: c_int, buf: *mut stat) -> c_int {
    unimplemented!()
}

pub unsafe fn _pgstat64(name: *const c_char, buf: *mut stat) -> c_int {
    unimplemented!()
}

pub unsafe fn _pglstat64(name: *const c_char, buf: *mut stat) -> c_int {
    unimplemented!()
}

#[inline]
pub unsafe fn fstat(fileno: c_int, sb: *mut stat) -> c_int {
    _pgfstat64(fileno, sb)
}

#[inline]
pub unsafe fn stat_fn(path: *const c_char, sb: *mut stat) -> c_int {
    // NB: C macro `stat(path, sb)` shadows the struct name `stat`; named
    // `stat_fn` here to avoid colliding with the `stat` struct in Rust.
    _pgstat64(path, sb)
}

#[inline]
pub unsafe fn lstat(path: *const c_char, sb: *mut stat) -> c_int {
    _pglstat64(path, sb)
}

/*
 * st_mode bit macros not provided by older MinGW nor MSVC.
 * These ultimately derive from the C runtime _S_* values; stubbed values are
 * the conventional POSIX bit positions (for group/other they are 0 on Win32).
 */
pub const S_IRUSR: c_int = 0o400; /* _S_IREAD */
pub const S_IWUSR: c_int = 0o200; /* _S_IWRITE */
pub const S_IXUSR: c_int = 0o100; /* _S_IEXEC */
pub const S_IRWXU: c_int = S_IRUSR | S_IWUSR | S_IXUSR;
pub const S_IRGRP: c_int = 0;
pub const S_IWGRP: c_int = 0;
pub const S_IXGRP: c_int = 0;
pub const S_IRWXG: c_int = 0;
pub const S_IROTH: c_int = 0;
pub const S_IWOTH: c_int = 0;
pub const S_IXOTH: c_int = 0;
pub const S_IRWXO: c_int = 0;

/*
 * S_IFMT/S_IFDIR/S_IFREG/S_IFCHR come from the system <sys/stat.h>; stubbed
 * here with conventional values so S_ISDIR/S_ISREG/S_ISLNK can be expressed.
 * TODO: dedup with system stat constants.
 */
pub const S_IFMT: c_int = 0o170000;
pub const S_IFDIR: c_int = 0o040000;
pub const S_IFREG: c_int = 0o100000;
pub const S_IFCHR: c_int = 0o020000;

#[inline]
pub fn S_ISDIR(m: c_int) -> bool {
    (m & S_IFMT) == S_IFDIR
}

#[inline]
pub fn S_ISREG(m: c_int) -> bool {
    (m & S_IFMT) == S_IFREG
}

/*
 * In order for lstat() to be able to report junction points as symlinks, we
 * hijack the character-device bit for symlinks.
 */
pub const S_IFLNK: c_int = S_IFCHR;

#[inline]
pub fn S_ISLNK(m: c_int) -> bool {
    (m & S_IFLNK) == S_IFLNK
}

/*
 * Supplement to <fcntl.h>.
 *
 * High-end bits borrowed to avoid colliding with the system-defined values.
 */
pub const O_CLOEXEC: c_int = 0x04000000;
pub const O_DIRECT: c_int = 0x80000000u32 as c_int;
pub const O_DSYNC: c_int = 0x0080; /* _O_NOINHERIT */

/*
 * Supplement to <errno.h>.
 *
 * Network-related Berkeley error symbols redefined as the corresponding WSA
 * constants.  The WSA* values are defined in winsock2.h; stubbed here with
 * their conventional numeric values (WSABASEERR 10000 + offset).
 * TODO: dedup with system Winsock errno values.
 */
pub const WSABASEERR: c_int = 10000;
pub const WSAEINTR: c_int = WSABASEERR + 4;
pub const WSAEWOULDBLOCK: c_int = WSABASEERR + 35;
pub const WSAEINPROGRESS: c_int = WSABASEERR + 36;
pub const WSAEMSGSIZE: c_int = WSABASEERR + 40;
pub const WSAEPROTONOSUPPORT: c_int = WSABASEERR + 43;
pub const WSAEOPNOTSUPP: c_int = WSABASEERR + 45;
pub const WSAEAFNOSUPPORT: c_int = WSABASEERR + 47;
pub const WSAEADDRINUSE: c_int = WSABASEERR + 48;
pub const WSAEADDRNOTAVAIL: c_int = WSABASEERR + 49;
pub const WSAENETDOWN: c_int = WSABASEERR + 50;
pub const WSAENETUNREACH: c_int = WSABASEERR + 51;
pub const WSAENETRESET: c_int = WSABASEERR + 52;
pub const WSAECONNABORTED: c_int = WSABASEERR + 53;
pub const WSAECONNRESET: c_int = WSABASEERR + 54;
pub const WSAENOBUFS: c_int = WSABASEERR + 55;
pub const WSAEISCONN: c_int = WSABASEERR + 56;
pub const WSAENOTCONN: c_int = WSABASEERR + 57;
pub const WSAETIMEDOUT: c_int = WSABASEERR + 60;
pub const WSAECONNREFUSED: c_int = WSABASEERR + 61;
pub const WSAEHOSTDOWN: c_int = WSABASEERR + 64;
pub const WSAEHOSTUNREACH: c_int = WSABASEERR + 65;
pub const WSAENOTSOCK: c_int = WSABASEERR + 38;

pub const EAGAIN: c_int = WSAEWOULDBLOCK;
pub const EINTR: c_int = WSAEINTR;
pub const EMSGSIZE: c_int = WSAEMSGSIZE;
pub const EAFNOSUPPORT: c_int = WSAEAFNOSUPPORT;
pub const EWOULDBLOCK: c_int = WSAEWOULDBLOCK;
pub const ECONNABORTED: c_int = WSAECONNABORTED;
pub const ECONNRESET: c_int = WSAECONNRESET;
pub const EINPROGRESS: c_int = WSAEINPROGRESS;
pub const EISCONN: c_int = WSAEISCONN;
pub const ENOBUFS: c_int = WSAENOBUFS;
pub const EPROTONOSUPPORT: c_int = WSAEPROTONOSUPPORT;
pub const ECONNREFUSED: c_int = WSAECONNREFUSED;
pub const ENOTSOCK: c_int = WSAENOTSOCK;
pub const EOPNOTSUPP: c_int = WSAEOPNOTSUPP;
pub const EADDRINUSE: c_int = WSAEADDRINUSE;
pub const EADDRNOTAVAIL: c_int = WSAEADDRNOTAVAIL;
pub const EHOSTDOWN: c_int = WSAEHOSTDOWN;
pub const EHOSTUNREACH: c_int = WSAEHOSTUNREACH;
pub const ENETDOWN: c_int = WSAENETDOWN;
pub const ENETRESET: c_int = WSAENETRESET;
pub const ENETUNREACH: c_int = WSAENETUNREACH;
pub const ENOTCONN: c_int = WSAENOTCONN;
pub const ETIMEDOUT: c_int = WSAETIMEDOUT;

/*
 * Locale stuff.  (#define locale_t _locale_t etc.) - the extended locale
 * functions are C-runtime intrinsics with no standalone Rust prototype here;
 * locale_t is stubbed as the underscored MSVC type.
 */
pub type locale_t = *mut c_void; /* _locale_t */

/*
 * Define our own wrapper macro around setlocale() to work around bugs in
 * Windows' native setlocale() function.
 */
pub unsafe fn pgwin32_setlocale(category: c_int, locale: *const c_char) -> *mut c_char {
    unimplemented!()
}

#[inline]
pub unsafe fn setlocale(a: c_int, b: *const c_char) -> *mut c_char {
    pgwin32_setlocale(a, b)
}

/* In backend/port/win32/signal.c */
pub static mut pg_signal_queue: c_int = 0; /* volatile int */
pub static mut pg_signal_mask: c_int = 0;
pub static mut pgwin32_signal_event: HANDLE = std::ptr::null_mut();
pub static mut pgwin32_initial_signal_pipe: HANDLE = std::ptr::null_mut();

#[inline]
pub unsafe fn UNBLOCKED_SIGNAL_QUEUE() -> c_int {
    pg_signal_queue & !pg_signal_mask
}

pub const PG_SIGNAL_COUNT: c_int = 32;

pub unsafe fn pgwin32_signal_initialize() {
    unimplemented!()
}

pub unsafe fn pgwin32_create_signal_listener(pid: pid_t) -> HANDLE {
    unimplemented!()
}

pub unsafe fn pgwin32_dispatch_queued_signals() {
    unimplemented!()
}

pub unsafe fn pg_queue_signal(signum: c_int) {
    unimplemented!()
}

/* In src/port/kill.c */
pub unsafe fn pgkill(pid: c_int, sig: c_int) -> c_int {
    unimplemented!()
}

#[inline]
pub unsafe fn kill(pid: c_int, sig: c_int) -> c_int {
    pgkill(pid, sig)
}

/*
 * In backend/port/win32/socket.c (#ifndef FRONTEND).
 * These redefine the socket(2) family as pgwin32_* wrappers.
 */
pub unsafe fn pgwin32_socket(af: c_int, type_: c_int, protocol: c_int) -> SOCKET {
    unimplemented!()
}

pub unsafe fn pgwin32_bind(s: SOCKET, addr: *mut sockaddr, addrlen: c_int) -> c_int {
    unimplemented!()
}

pub unsafe fn pgwin32_listen(s: SOCKET, backlog: c_int) -> c_int {
    unimplemented!()
}

pub unsafe fn pgwin32_accept(s: SOCKET, addr: *mut sockaddr, addrlen: *mut c_int) -> SOCKET {
    unimplemented!()
}

pub unsafe fn pgwin32_connect(s: SOCKET, name: *const sockaddr, namelen: c_int) -> c_int {
    unimplemented!()
}

pub unsafe fn pgwin32_select(
    nfds: c_int,
    readfds: *mut fd_set,
    writefds: *mut fd_set,
    exceptfds: *mut fd_set,
    timeout: *const timeval,
) -> c_int {
    unimplemented!()
}

pub unsafe fn pgwin32_recv(s: SOCKET, buf: *mut c_char, len: c_int, flags: c_int) -> c_int {
    unimplemented!()
}

pub unsafe fn pgwin32_send(s: SOCKET, buf: *const c_void, len: c_int, flags: c_int) -> c_int {
    unimplemented!()
}

pub unsafe fn pgwin32_waitforsinglesocket(s: SOCKET, what: c_int, timeout: c_int) -> c_int {
    unimplemented!()
}

pub static mut pgwin32_noblock: c_int = 0;

/* in backend/port/win32_shmem.c */
pub unsafe fn pgwin32_ReserveSharedMemoryRegion(handle: HANDLE) -> c_int {
    unimplemented!()
}

/* in backend/port/win32/crashdump.c */
pub unsafe fn pgwin32_install_crashdump_handler() {
    unimplemented!()
}

/* in port/win32dlopen.c */
pub unsafe fn dlopen(file: *const c_char, mode: c_int) -> *mut c_void {
    unimplemented!()
}

pub unsafe fn dlsym(handle: *mut c_void, symbol: *const c_char) -> *mut c_void {
    unimplemented!()
}

pub unsafe fn dlclose(handle: *mut c_void) -> c_int {
    unimplemented!()
}

pub unsafe fn dlerror() -> *mut c_char {
    unimplemented!()
}

pub const RTLD_NOW: c_int = 1;
pub const RTLD_GLOBAL: c_int = 0;

/* in port/win32error.c */
pub unsafe fn _dosmaperr(e: u32) {
    unimplemented!()
}

/* in port/win32env.c */
pub unsafe fn pgwin32_putenv(s: *const c_char) -> c_int {
    unimplemented!()
}

pub unsafe fn pgwin32_setenv(name: *const c_char, value: *const c_char, overwrite: c_int) -> c_int {
    unimplemented!()
}

pub unsafe fn pgwin32_unsetenv(name: *const c_char) -> c_int {
    unimplemented!()
}

#[inline]
pub unsafe fn putenv(x: *const c_char) -> c_int {
    pgwin32_putenv(x)
}

#[inline]
pub unsafe fn setenv(x: *const c_char, y: *const c_char, z: c_int) -> c_int {
    pgwin32_setenv(x, y, z)
}

#[inline]
pub unsafe fn unsetenv(x: *const c_char) -> c_int {
    pgwin32_unsetenv(x)
}

/* in port/win32security.c */
pub unsafe fn pgwin32_is_service() -> c_int {
    unimplemented!()
}

pub unsafe fn pgwin32_is_admin() -> c_int {
    unimplemented!()
}

/* Windows security token manipulation (in src/common/exec.c) */
pub unsafe fn AddUserToTokenDacl(hToken: HANDLE) -> BOOL {
    unimplemented!()
}

/*
 * Things that exist in MinGW headers, but need to be added to MSVC.
 */
pub type ssize_t = isize; /* long on _WIN32, __int64 on _WIN64 */
pub type mode_t = u16;

pub const F_OK: c_int = 0;
pub const W_OK: c_int = 2;
pub const R_OK: c_int = 4;

/*
 * MinGW strtof hack.
 */
pub const HAVE_BUGGY_STRTOF: c_int = 1;

/* in port/win32pread.c */
pub unsafe fn pg_pread(
    fd: c_int,
    buf: *mut c_void,
    nbyte: crate::c::Size,
    offset: c_off_t,
) -> ssize_t {
    unimplemented!()
}

/* in port/win32pwrite.c */
pub unsafe fn pg_pwrite(
    fd: c_int,
    buf: *const c_void,
    nbyte: crate::c::Size,
    offset: c_off_t,
) -> ssize_t {
    unimplemented!()
}
