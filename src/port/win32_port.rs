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

use crate::port::pgsleep::pg_usleep;
use crate::utils::elog::{ERROR, FATAL, NOTICE};
use crate::{elog, ereport, errmsg};

macro_rules! errmsg_internal {
    ($fmt:literal $(, $arg:expr)*) => { errmsg!($fmt $(, $arg)*) };
}
use errmsg_internal;

// errno access (platform errno location), mirroring inet_net_ntop.rs convention.
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
unsafe fn set_errno(e: c_int) {
    *errno_location() = e;
}

// ---------------------------------------------------------------------------
// Win32 / Winsock primitives referenced by backend/port/win32/socket.c.
// None of these exist off-Windows; stubbed locally with TODO(pg-port).
// ---------------------------------------------------------------------------
pub const INVALID_HANDLE_VALUE: HANDLE = (-1isize) as HANDLE;
pub const INVALID_SOCKET: SOCKET = usize::MAX; // (SOCKET)(~0)
pub const SOCKET_ERROR: c_int = -1;

pub const INFINITE: c_int = -1; // 0xFFFFFFFF as signed timeout
pub const WSA_INFINITE: DWORD = 0xFFFFFFFF;

pub const WAIT_OBJECT_0: c_int = 0x00000000;
pub const WAIT_IO_COMPLETION: c_int = 0x000000C0;
pub const WAIT_TIMEOUT: c_int = 0x00000102;
pub const WSA_WAIT_TIMEOUT: c_int = WAIT_TIMEOUT;

pub const SOL_SOCKET: c_int = 0xffff;
pub const SO_TYPE: c_int = 0x1008;
pub const SOCK_DGRAM: c_int = 2;

pub const FIONBIO: c_long = 0x8004667e_u32 as c_long;
pub const WSA_FLAG_OVERLAPPED: DWORD = 0x01;

pub const FD_READ: c_int = 0x01;
pub const FD_WRITE: c_int = 0x02;
pub const FD_ACCEPT: c_int = 0x08;
pub const FD_CONNECT: c_int = 0x10;
pub const FD_CLOSE: c_int = 0x20;

// errno values from the system <errno.h> (not the WSA-redefined network ones).
pub const EINVAL: c_int = 22;
pub const EFAULT: c_int = 14;
pub const EMFILE: c_int = 24;
pub const EACCES: c_int = 13;

// WSA error codes not already defined above (winsock2.h: WSABASEERR + offset).
pub const WSAEINVAL: c_int = WSABASEERR + 10;
pub const WSAEFAULT: c_int = WSABASEERR + 14;
pub const WSAEMFILE: c_int = WSABASEERR + 24;
pub const WSAEACCES: c_int = WSABASEERR + 13;
pub const WSAEDESTADDRREQ: c_int = WSABASEERR + 39;
pub const WSAEPROTOTYPE: c_int = WSABASEERR + 41;
pub const WSAESOCKTNOSUPPORT: c_int = WSABASEERR + 44;
pub const WSAESHUTDOWN: c_int = WSABASEERR + 58;
pub const WSAEDISCON: c_int = WSABASEERR + 1009;
pub const WSANOTINITIALISED: c_int = WSABASEERR + 1010;
pub const WSAEINVALIDPROCTABLE: c_int = WSABASEERR + 1004;
pub const WSAEINVALIDPROVIDER: c_int = WSABASEERR + 1005;
pub const WSAHOST_NOT_FOUND: c_int = WSABASEERR + 1001;

pub const WSANETWORKEVENTS_NLEN: usize = 10; /* FD_MAX_EVENTS */

#[repr(C)]
#[derive(Clone, Copy)]
pub struct WSABUF {
    pub len: c_ulong,
    pub buf: *mut c_char,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct WSANETWORKEVENTS {
    pub lNetworkEvents: c_long,
    pub iErrorCode: [c_int; WSANETWORKEVENTS_NLEN],
}

pub type WSAEVENT = HANDLE;
type c_ulong = std::ffi::c_ulong;

// TODO(pg-port): genuine Win32/Winsock entry points; unported off-Windows.
pub unsafe fn WSAGetLastError() -> c_int {
    todo!("TODO(pg-port): WSAGetLastError")
}
pub unsafe fn getsockopt(
    _s: SOCKET,
    _level: c_int,
    _optname: c_int,
    _optval: *mut c_char,
    _optlen: *mut c_int,
) -> c_int {
    todo!("TODO(pg-port): getsockopt")
}
pub unsafe fn WSASocket(
    _af: c_int,
    _type: c_int,
    _protocol: c_int,
    _lpProtocolInfo: *mut c_void,
    _g: c_int,
    _dwFlags: DWORD,
) -> SOCKET {
    todo!("TODO(pg-port): WSASocket")
}
pub unsafe fn ioctlsocket(_s: SOCKET, _cmd: c_long, _argp: *mut c_ulong) -> c_int {
    todo!("TODO(pg-port): ioctlsocket")
}
#[no_mangle]
pub unsafe fn closesocket(_s: SOCKET) -> c_int {
    todo!("TODO(pg-port): closesocket")
}
pub unsafe fn bind(_s: SOCKET, _addr: *mut sockaddr, _addrlen: c_int) -> c_int {
    todo!("TODO(pg-port): bind")
}
pub unsafe fn listen(_s: SOCKET, _backlog: c_int) -> c_int {
    todo!("TODO(pg-port): listen")
}
pub unsafe fn WSAAccept(
    _s: SOCKET,
    _addr: *mut sockaddr,
    _addrlen: *mut c_int,
    _lpfnCondition: *mut c_void,
    _dwCallbackData: usize,
) -> SOCKET {
    todo!("TODO(pg-port): WSAAccept")
}
pub unsafe fn WSAConnect(
    _s: SOCKET,
    _name: *const sockaddr,
    _namelen: c_int,
    _lpCallerData: *mut c_void,
    _lpCalleeData: *mut c_void,
    _lpSQOS: *mut c_void,
    _lpGQOS: *mut c_void,
) -> c_int {
    todo!("TODO(pg-port): WSAConnect")
}
pub unsafe fn WSARecv(
    _s: SOCKET,
    _lpBuffers: *mut WSABUF,
    _dwBufferCount: DWORD,
    _lpNumberOfBytesRecvd: *mut DWORD,
    _lpFlags: *mut DWORD,
    _lpOverlapped: *mut c_void,
    _lpCompletionRoutine: *mut c_void,
) -> c_int {
    todo!("TODO(pg-port): WSARecv")
}
pub unsafe fn WSASend(
    _s: SOCKET,
    _lpBuffers: *mut WSABUF,
    _dwBufferCount: DWORD,
    _lpNumberOfBytesSent: *mut DWORD,
    _dwFlags: DWORD,
    _lpOverlapped: *mut c_void,
    _lpCompletionRoutine: *mut c_void,
) -> c_int {
    todo!("TODO(pg-port): WSASend")
}
pub unsafe fn CreateEvent(
    _lpEventAttributes: *mut c_void,
    _bManualReset: BOOL,
    _bInitialState: BOOL,
    _lpName: *const c_char,
) -> HANDLE {
    todo!("TODO(pg-port): CreateEvent")
}
pub unsafe fn ResetEvent(_hEvent: HANDLE) -> BOOL {
    todo!("TODO(pg-port): ResetEvent")
}
pub unsafe fn WSAEventSelect(_s: SOCKET, _hEventObject: WSAEVENT, _lNetworkEvents: c_long) -> c_int {
    todo!("TODO(pg-port): WSAEventSelect")
}
pub unsafe fn WaitForMultipleObjectsEx(
    _nCount: DWORD,
    _lpHandles: *const HANDLE,
    _bWaitAll: BOOL,
    _dwMilliseconds: DWORD,
    _bAlertable: BOOL,
) -> c_int {
    todo!("TODO(pg-port): WaitForMultipleObjectsEx")
}
pub unsafe fn GetLastError() -> DWORD {
    todo!("TODO(pg-port): GetLastError")
}
pub unsafe fn WSACreateEvent() -> WSAEVENT {
    todo!("TODO(pg-port): WSACreateEvent")
}
pub unsafe fn WSACloseEvent(_hEvent: WSAEVENT) -> BOOL {
    todo!("TODO(pg-port): WSACloseEvent")
}
pub unsafe fn WSAEnumNetworkEvents(
    _s: SOCKET,
    _hEventObject: WSAEVENT,
    _lpNetworkEvents: *mut WSANETWORKEVENTS,
) -> c_int {
    todo!("TODO(pg-port): WSAEnumNetworkEvents")
}

// fd_set is opaque (c_void) in the public signatures, but pgwin32_select needs
// the Winsock struct layout (fd_count + fd_array).  Mirror it locally and view
// the opaque pointer through it.  TODO(pg-port): unify with system winsock2.h.
pub const FD_SETSIZE: usize = 64;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct fd_set_impl {
    pub fd_count: c_uint,
    pub fd_array: [SOCKET; FD_SETSIZE],
}
type c_uint = std::ffi::c_uint;

#[inline]
unsafe fn fdv(set: *mut fd_set) -> *mut fd_set_impl {
    set as *mut fd_set_impl
}

unsafe fn FD_ZERO(set: *mut fd_set) {
    (*fdv(set)).fd_count = 0;
}
unsafe fn FD_SET(fd: SOCKET, set: *mut fd_set) {
    let s = fdv(set);
    let mut i = 0;
    while i < (*s).fd_count as usize {
        if (*s).fd_array[i] == fd {
            break;
        }
        i += 1;
    }
    if i == (*s).fd_count as usize && ((*s).fd_count as usize) < FD_SETSIZE {
        (*s).fd_array[i] = fd;
        (*s).fd_count += 1;
    }
}
unsafe fn FD_ISSET(fd: SOCKET, set: *mut fd_set) -> bool {
    let s = fdv(set);
    let mut i = 0;
    while i < (*s).fd_count as usize {
        if (*s).fd_array[i] == fd {
            return true;
        }
        i += 1;
    }
    false
}

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

/*-------------------------------------------------------------------------
 * backend/port/win32/timer.c
 *	  Microsoft Windows Win32 Timer Implementation
 *
 *	  Limitations of this implementation:
 *	  - Does not support interval timer (value->it_interval)
 *	  - Only supports ITIMER_REAL
 *-------------------------------------------------------------------------
 */

/* Communication area for inter-thread communication */
#[repr(C)]
struct timerCA {
    value: itimerval,
    event: HANDLE,
    crit_sec: CRITICAL_SECTION,
}

static mut timerCommArea: timerCA = timerCA {
    value: itimerval {
        it_interval: timeval { tv_sec: 0, tv_usec: 0 },
        it_value: timeval { tv_sec: 0, tv_usec: 0 },
    },
    event: std::ptr::null_mut(),
    crit_sec: std::ptr::null_mut(),
};
static mut timerThreadHandle: HANDLE = (-1isize) as HANDLE; /* INVALID_HANDLE_VALUE */

/* Timer management thread */
unsafe extern "system" fn pg_timer_thread(param: LPVOID) -> DWORD {
    let mut waittime: DWORD;

    crate::Assert!(param.is_null());

    waittime = INFINITE as DWORD;

    loop {
        let r: c_int;

        r = WaitForSingleObjectEx(timerCommArea.event, waittime, FALSE);
        if r == WAIT_OBJECT_0 {
            /* Event signaled from main thread, change the timer */
            EnterCriticalSection(&raw mut timerCommArea.crit_sec);
            if timerCommArea.value.it_value.tv_sec == 0
                && timerCommArea.value.it_value.tv_usec == 0
            {
                waittime = INFINITE as DWORD; /* Cancel the interrupt */
            } else {
                /* WaitForSingleObjectEx() uses milliseconds, round up */
                waittime = ((timerCommArea.value.it_value.tv_usec + 999) / 1000
                    + timerCommArea.value.it_value.tv_sec * 1000)
                    as DWORD;
            }
            ResetEvent(timerCommArea.event);
            LeaveCriticalSection(&raw mut timerCommArea.crit_sec);
        } else if r == WAIT_TIMEOUT {
            /* Timeout expired, signal SIGALRM and turn it off */
            pg_queue_signal(SIGALRM);
            waittime = INFINITE as DWORD;
        } else {
            /* Should never happen */
            crate::Assert!(false);
        }
    }
}

/*
 * Win32 setitimer emulation by creating a persistent thread
 * to handle the timer setting and notification upon timeout.
 */
pub unsafe fn setitimer(
    which: c_int,
    value: *const itimerval,
    ovalue: *mut itimerval,
) -> c_int {
    crate::Assert!(!value.is_null());
    crate::Assert!(
        (*value).it_interval.tv_sec == 0 && (*value).it_interval.tv_usec == 0
    );
    crate::Assert!(which == ITIMER_REAL);

    if timerThreadHandle == INVALID_HANDLE_VALUE {
        /* First call in this backend, create event and the timer thread */
        timerCommArea.event = CreateEvent(
            std::ptr::null_mut(),
            TRUE,
            FALSE,
            std::ptr::null(),
        );
        if timerCommArea.event.is_null() {
            ereport!(
                FATAL,
                errmsg_internal!(
                    "could not create timer event: error code {}",
                    GetLastError()
                )
            );
        }

        crate::c::MemSet(
            &raw mut timerCommArea.value as *mut c_void,
            0,
            std::mem::size_of::<itimerval>(),
        );

        InitializeCriticalSection(&raw mut timerCommArea.crit_sec);

        timerThreadHandle = CreateThread(
            std::ptr::null_mut(),
            0,
            Some(pg_timer_thread),
            std::ptr::null_mut(),
            0,
            std::ptr::null_mut(),
        );
        if timerThreadHandle == INVALID_HANDLE_VALUE {
            ereport!(
                FATAL,
                errmsg_internal!(
                    "could not create timer thread: error code {}",
                    GetLastError()
                )
            );
        }
    }

    /* Request the timer thread to change settings */
    EnterCriticalSection(&raw mut timerCommArea.crit_sec);
    if !ovalue.is_null() {
        *ovalue = timerCommArea.value;
    }
    timerCommArea.value = *value;
    LeaveCriticalSection(&raw mut timerCommArea.crit_sec);
    SetEvent(timerCommArea.event);

    0
}

/* Win32 primitives referenced by setitimer/pg_timer_thread (timer.c). */
type CRITICAL_SECTION = *mut c_void;
type LPVOID = *mut c_void;
const TRUE: BOOL = 1;
const FALSE: BOOL = 0;

unsafe fn WaitForSingleObjectEx(
    _hHandle: HANDLE,
    _dwMilliseconds: DWORD,
    _bAlertable: BOOL,
) -> c_int {
    todo!("TODO(pg-port): WaitForSingleObjectEx")
}
unsafe fn SetEvent(_hEvent: HANDLE) -> BOOL {
    todo!("TODO(pg-port): SetEvent")
}
unsafe fn EnterCriticalSection(_lpCriticalSection: *mut CRITICAL_SECTION) {
    todo!("TODO(pg-port): EnterCriticalSection")
}
unsafe fn LeaveCriticalSection(_lpCriticalSection: *mut CRITICAL_SECTION) {
    todo!("TODO(pg-port): LeaveCriticalSection")
}
unsafe fn InitializeCriticalSection(_lpCriticalSection: *mut CRITICAL_SECTION) {
    todo!("TODO(pg-port): InitializeCriticalSection")
}
unsafe fn CreateThread(
    _lpThreadAttributes: *mut c_void,
    _dwStackSize: usize,
    _lpStartAddress: Option<unsafe extern "system" fn(LPVOID) -> DWORD>,
    _lpParameter: LPVOID,
    _dwCreationFlags: DWORD,
    _lpThreadId: *mut DWORD,
) -> HANDLE {
    todo!("TODO(pg-port): CreateThread")
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
/*
 * Convert the last socket error code into errno
 *
 * Note: where there is a direct correspondence between a WSAxxx error code
 * and a Berkeley error symbol, this mapping is actually a no-op, because
 * in win32_port.h we redefine the network-related Berkeley error symbols to
 * have the values of their WSAxxx counterparts.  The point of the switch is
 * mostly to translate near-miss error codes into something that's sensible
 * in the Berkeley universe.
 */
unsafe fn TranslateSocketError() {
    match WSAGetLastError() {
        WSAEINVAL | WSANOTINITIALISED | WSAEINVALIDPROVIDER | WSAEINVALIDPROCTABLE
        | WSAEDESTADDRREQ => {
            set_errno(EINVAL);
        }
        WSAEINPROGRESS => {
            set_errno(EINPROGRESS);
        }
        WSAEFAULT => {
            set_errno(EFAULT);
        }
        WSAEISCONN => {
            set_errno(EISCONN);
        }
        WSAEMSGSIZE => {
            set_errno(EMSGSIZE);
        }
        WSAEAFNOSUPPORT => {
            set_errno(EAFNOSUPPORT);
        }
        WSAEMFILE => {
            set_errno(EMFILE);
        }
        WSAENOBUFS => {
            set_errno(ENOBUFS);
        }
        WSAEPROTONOSUPPORT | WSAEPROTOTYPE | WSAESOCKTNOSUPPORT => {
            set_errno(EPROTONOSUPPORT);
        }
        WSAECONNABORTED => {
            set_errno(ECONNABORTED);
        }
        WSAECONNREFUSED => {
            set_errno(ECONNREFUSED);
        }
        WSAECONNRESET => {
            set_errno(ECONNRESET);
        }
        WSAEINTR => {
            set_errno(EINTR);
        }
        WSAENOTSOCK => {
            set_errno(ENOTSOCK);
        }
        WSAEOPNOTSUPP => {
            set_errno(EOPNOTSUPP);
        }
        WSAEWOULDBLOCK => {
            set_errno(EWOULDBLOCK);
        }
        WSAEACCES => {
            set_errno(EACCES);
        }
        WSAEADDRINUSE => {
            set_errno(EADDRINUSE);
        }
        WSAEADDRNOTAVAIL => {
            set_errno(EADDRNOTAVAIL);
        }
        WSAEHOSTDOWN => {
            set_errno(EHOSTDOWN);
        }
        WSAEHOSTUNREACH | WSAHOST_NOT_FOUND => {
            set_errno(EHOSTUNREACH);
        }
        WSAENETDOWN => {
            set_errno(ENETDOWN);
        }
        WSAENETUNREACH => {
            set_errno(ENETUNREACH);
        }
        WSAENETRESET => {
            set_errno(ENETRESET);
        }
        WSAENOTCONN | WSAESHUTDOWN | WSAEDISCON => {
            set_errno(ENOTCONN);
        }
        WSAETIMEDOUT => {
            set_errno(ETIMEDOUT);
        }
        _ => {
            ereport!(
                NOTICE,
                errmsg!(
                    "unrecognized win32 socket error code: {}",
                    WSAGetLastError()
                )
            );
            set_errno(EINVAL);
        }
    }
}

unsafe fn pgwin32_poll_signals() -> c_int {
    if UNBLOCKED_SIGNAL_QUEUE() != 0 {
        pgwin32_dispatch_queued_signals();
        set_errno(EINTR);
        return 1;
    }
    0
}

unsafe fn isDataGram(s: SOCKET) -> c_int {
    let mut r#type: c_int = 0;
    let mut typelen: c_int = std::mem::size_of::<c_int>() as c_int;

    if getsockopt(
        s,
        SOL_SOCKET,
        SO_TYPE,
        &raw mut r#type as *mut c_char,
        &raw mut typelen,
    ) != 0
    {
        return 1;
    }

    if r#type == SOCK_DGRAM {
        1
    } else {
        0
    }
}

/*
 * Create a socket, setting it to overlapped and non-blocking
 */
pub unsafe fn pgwin32_socket(af: c_int, type_: c_int, protocol: c_int) -> SOCKET {
    let s: SOCKET;
    let mut on: c_ulong = 1;

    s = WSASocket(
        af,
        type_,
        protocol,
        std::ptr::null_mut(),
        0,
        WSA_FLAG_OVERLAPPED,
    );
    if s == INVALID_SOCKET {
        TranslateSocketError();
        return INVALID_SOCKET;
    }

    if ioctlsocket(s, FIONBIO, &raw mut on) != 0 {
        TranslateSocketError();
        closesocket(s);
        return INVALID_SOCKET;
    }
    set_errno(0);

    s
}

pub unsafe fn pgwin32_bind(s: SOCKET, addr: *mut sockaddr, addrlen: c_int) -> c_int {
    let res: c_int;

    res = bind(s, addr, addrlen);
    if res < 0 {
        TranslateSocketError();
    }
    res
}

pub unsafe fn pgwin32_listen(s: SOCKET, backlog: c_int) -> c_int {
    let res: c_int;

    res = listen(s, backlog);
    if res < 0 {
        TranslateSocketError();
    }
    res
}

pub unsafe fn pgwin32_accept(s: SOCKET, addr: *mut sockaddr, addrlen: *mut c_int) -> SOCKET {
    let rs: SOCKET;

    /*
     * Poll for signals, but don't return with EINTR, since we don't handle
     * that in pqcomm.c
     */
    pgwin32_poll_signals();

    rs = WSAAccept(s, addr, addrlen, std::ptr::null_mut(), 0);
    if rs == INVALID_SOCKET {
        TranslateSocketError();
        return INVALID_SOCKET;
    }
    rs
}

/* No signal delivery during connect. */
pub unsafe fn pgwin32_connect(s: SOCKET, name: *const sockaddr, namelen: c_int) -> c_int {
    let r: c_int;

    r = WSAConnect(
        s,
        name,
        namelen,
        std::ptr::null_mut(),
        std::ptr::null_mut(),
        std::ptr::null_mut(),
        std::ptr::null_mut(),
    );
    if r == 0 {
        return 0;
    }

    if WSAGetLastError() != WSAEWOULDBLOCK {
        TranslateSocketError();
        return -1;
    }

    while pgwin32_waitforsinglesocket(s, FD_CONNECT, INFINITE) == 0 {
        /* Loop endlessly as long as we are just delivering signals */
    }

    0
}

/*
 * Wait for activity on one or more sockets.
 * While waiting, allow signals to run
 *
 * NOTE! Currently does not implement exceptfds check,
 * since it is not used in postgresql!
 */
pub unsafe fn pgwin32_select(
    _nfds: c_int,
    readfds: *mut fd_set,
    writefds: *mut fd_set,
    exceptfds: *mut fd_set,
    timeout: *const timeval,
) -> c_int {
    let mut events: [WSAEVENT; FD_SETSIZE * 2] = [std::ptr::null_mut(); FD_SETSIZE * 2]; /* worst case is readfds totally
                                                                                          * different from writefds, so
                                                                                          * 2*FD_SETSIZE sockets */
    let mut sockets: [SOCKET; FD_SETSIZE * 2] = [0; FD_SETSIZE * 2];
    let mut numevents: c_int = 0;
    let mut i: c_int;
    let mut r: c_int;
    let mut timeoutval: DWORD = WSA_INFINITE;
    let mut outreadfds: fd_set_impl = std::mem::zeroed();
    let mut outwritefds: fd_set_impl = std::mem::zeroed();
    let mut nummatches: c_int = 0;

    crate::Assert!(exceptfds.is_null());

    if pgwin32_poll_signals() != 0 {
        return -1;
    }

    FD_ZERO(&raw mut outreadfds as *mut fd_set);
    FD_ZERO(&raw mut outwritefds as *mut fd_set);

    /*
     * Windows does not guarantee to log an FD_WRITE network event indicating
     * that more data can be sent unless the previous send() failed with
     * WSAEWOULDBLOCK.  While our caller might well have made such a call, we
     * cannot assume that here.  Therefore, if waiting for write-ready, force
     * the issue by doing a dummy send().  If the dummy send() succeeds,
     * assume that the socket is in fact write-ready, and return immediately.
     * Also, if it fails with something other than WSAEWOULDBLOCK, return a
     * write-ready indication to let our caller deal with the error condition.
     */
    if !writefds.is_null() {
        let wf = fdv(writefds);
        i = 0;
        while (i as c_uint) < (*wf).fd_count {
            let mut c: c_char = 0;
            let mut buf: WSABUF = WSABUF {
                len: 0,
                buf: std::ptr::null_mut(),
            };
            let mut sent: DWORD = 0;

            buf.buf = &raw mut c;
            buf.len = 0;

            r = WSASend(
                (*wf).fd_array[i as usize],
                &raw mut buf,
                1,
                &raw mut sent,
                0,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            );
            if r == 0 || WSAGetLastError() != WSAEWOULDBLOCK {
                FD_SET((*wf).fd_array[i as usize], &raw mut outwritefds as *mut fd_set);
            }
            i += 1;
        }

        /* If we found any write-ready sockets, just return them immediately */
        if outwritefds.fd_count > 0 {
            std::ptr::copy_nonoverlapping(
                &raw const outwritefds as *const fd_set_impl,
                writefds as *mut fd_set_impl,
                1,
            );
            if !readfds.is_null() {
                FD_ZERO(readfds);
            }
            return outwritefds.fd_count as c_int;
        }
    }

    /* Now set up for an actual select */

    if !timeout.is_null() {
        /* timeoutval is in milliseconds */
        timeoutval = ((*timeout).tv_sec * 1000 + (*timeout).tv_usec / 1000) as DWORD;
    }

    if !readfds.is_null() {
        let rf = fdv(readfds);
        i = 0;
        while (i as c_uint) < (*rf).fd_count {
            events[numevents as usize] = WSACreateEvent();
            sockets[numevents as usize] = (*rf).fd_array[i as usize];
            numevents += 1;
            i += 1;
        }
    }
    if !writefds.is_null() {
        let wf = fdv(writefds);
        i = 0;
        while (i as c_uint) < (*wf).fd_count {
            if readfds.is_null() || !FD_ISSET((*wf).fd_array[i as usize], readfds) {
                /* If the socket is not in the read list */
                events[numevents as usize] = WSACreateEvent();
                sockets[numevents as usize] = (*wf).fd_array[i as usize];
                numevents += 1;
            }
            i += 1;
        }
    }

    i = 0;
    while i < numevents {
        let mut flags: c_int = 0;

        if !readfds.is_null() && FD_ISSET(sockets[i as usize], readfds) {
            flags |= FD_READ | FD_ACCEPT | FD_CLOSE;
        }

        if !writefds.is_null() && FD_ISSET(sockets[i as usize], writefds) {
            flags |= FD_WRITE | FD_CLOSE;
        }

        if WSAEventSelect(sockets[i as usize], events[i as usize], flags as c_long) != 0 {
            TranslateSocketError();
            /* release already-assigned event objects */
            i -= 1;
            while i >= 0 {
                WSAEventSelect(sockets[i as usize], std::ptr::null_mut(), 0);
                i -= 1;
            }
            i = 0;
            while i < numevents {
                WSACloseEvent(events[i as usize]);
                i += 1;
            }
            return -1;
        }
        i += 1;
    }

    events[numevents as usize] = pgwin32_signal_event;
    r = WaitForMultipleObjectsEx(
        (numevents + 1) as DWORD,
        events.as_ptr(),
        0,
        timeoutval,
        1,
    );
    if r != WAIT_TIMEOUT && r != WAIT_IO_COMPLETION && r != (WAIT_OBJECT_0 + numevents) {
        /*
         * We scan all events, even those not signaled, in case more than one
         * event has been tagged but Wait.. can only return one.
         */
        let mut resEvents: WSANETWORKEVENTS;

        i = 0;
        while i < numevents {
            resEvents = std::mem::zeroed();
            if WSAEnumNetworkEvents(sockets[i as usize], events[i as usize], &raw mut resEvents) != 0
            {
                elog!(
                    ERROR,
                    "failed to enumerate network events: error code {}",
                    WSAGetLastError()
                );
            }
            /* Read activity? */
            if !readfds.is_null() && FD_ISSET(sockets[i as usize], readfds) {
                if (resEvents.lNetworkEvents & FD_READ as c_long) != 0
                    || (resEvents.lNetworkEvents & FD_ACCEPT as c_long) != 0
                    || (resEvents.lNetworkEvents & FD_CLOSE as c_long) != 0
                {
                    FD_SET(sockets[i as usize], &raw mut outreadfds as *mut fd_set);

                    nummatches += 1;
                }
            }
            /* Write activity? */
            if !writefds.is_null() && FD_ISSET(sockets[i as usize], writefds) {
                if (resEvents.lNetworkEvents & FD_WRITE as c_long) != 0
                    || (resEvents.lNetworkEvents & FD_CLOSE as c_long) != 0
                {
                    FD_SET(sockets[i as usize], &raw mut outwritefds as *mut fd_set);

                    nummatches += 1;
                }
            }
            i += 1;
        }
    }

    /* Clean up all the event objects */
    i = 0;
    while i < numevents {
        WSAEventSelect(sockets[i as usize], std::ptr::null_mut(), 0);
        WSACloseEvent(events[i as usize]);
        i += 1;
    }

    if r == WSA_WAIT_TIMEOUT {
        if !readfds.is_null() {
            FD_ZERO(readfds);
        }
        if !writefds.is_null() {
            FD_ZERO(writefds);
        }
        return 0;
    }

    /* Signal-like events. */
    if r == WAIT_OBJECT_0 + numevents || r == WAIT_IO_COMPLETION {
        pgwin32_dispatch_queued_signals();
        set_errno(EINTR);
        if !readfds.is_null() {
            FD_ZERO(readfds);
        }
        if !writefds.is_null() {
            FD_ZERO(writefds);
        }
        return -1;
    }

    /* Overwrite socket sets with our resulting values */
    if !readfds.is_null() {
        std::ptr::copy_nonoverlapping(
            &raw const outreadfds as *const fd_set_impl,
            readfds as *mut fd_set_impl,
            1,
        );
    }
    if !writefds.is_null() {
        std::ptr::copy_nonoverlapping(
            &raw const outwritefds as *const fd_set_impl,
            writefds as *mut fd_set_impl,
            1,
        );
    }
    nummatches
}

pub unsafe fn pgwin32_recv(s: SOCKET, buf: *mut c_char, len: c_int, f: c_int) -> c_int {
    let mut wbuf: WSABUF = WSABUF {
        len: 0,
        buf: std::ptr::null_mut(),
    };
    let mut r: c_int;
    let mut b: DWORD = 0;
    let mut flags: DWORD = f as DWORD;
    let mut n: c_int;

    if pgwin32_poll_signals() != 0 {
        return -1;
    }

    wbuf.len = len as c_ulong;
    wbuf.buf = buf;

    r = WSARecv(s, &raw mut wbuf, 1, &raw mut b, &raw mut flags, std::ptr::null_mut(), std::ptr::null_mut());
    if r != SOCKET_ERROR {
        return b as c_int; /* success */
    }

    if WSAGetLastError() != WSAEWOULDBLOCK {
        TranslateSocketError();
        return -1;
    }

    if pgwin32_noblock != 0 {
        /*
         * No data received, and we are in "emulated non-blocking mode", so
         * return indicating that we'd block if we were to continue.
         */
        set_errno(EWOULDBLOCK);
        return -1;
    }

    /* We're in blocking mode, so wait for data */

    n = 0;
    while n < 5 {
        if pgwin32_waitforsinglesocket(s, FD_READ | FD_CLOSE | FD_ACCEPT, INFINITE) == 0 {
            return -1; /* errno already set */
        }

        r = WSARecv(s, &raw mut wbuf, 1, &raw mut b, &raw mut flags, std::ptr::null_mut(), std::ptr::null_mut());
        if r != SOCKET_ERROR {
            return b as c_int; /* success */
        }
        if WSAGetLastError() != WSAEWOULDBLOCK {
            TranslateSocketError();
            return -1;
        }

        /*
         * There seem to be cases on win2k (at least) where WSARecv can return
         * WSAEWOULDBLOCK even when pgwin32_waitforsinglesocket claims the
         * socket is readable.  In this case, just sleep for a moment and try
         * again.  We try up to 5 times - if it fails more than that it's not
         * likely to ever come back.
         */
        pg_usleep(10000);
        n += 1;
    }
    ereport!(
        NOTICE,
        errmsg!("could not read from ready socket (after retries)")
    );
    set_errno(EWOULDBLOCK);
    -1
}

/*
 * The second argument to send() is defined by SUS to be a "const void *"
 * and so we use the same signature here to keep compilers happy when
 * handling callers.
 *
 * But the buf member of a WSABUF struct is defined as "char *", so we cast
 * the second argument to that here when assigning it, also to keep compilers
 * happy.
 */
pub unsafe fn pgwin32_send(s: SOCKET, buf: *const c_void, len: c_int, flags: c_int) -> c_int {
    let mut wbuf: WSABUF = WSABUF {
        len: 0,
        buf: std::ptr::null_mut(),
    };
    let r: c_int;
    let mut b: DWORD = 0;

    if pgwin32_poll_signals() != 0 {
        return -1;
    }

    wbuf.len = len as c_ulong;
    wbuf.buf = buf as *mut c_char;

    /*
     * Readiness of socket to send data to UDP socket may be not true: socket
     * can become busy again! So loop until send or error occurs.
     */
    loop {
        let r = WSASend(s, &raw mut wbuf, 1, &raw mut b, flags as DWORD, std::ptr::null_mut(), std::ptr::null_mut());
        if r != SOCKET_ERROR && b > 0 {
            /* Write succeeded right away */
            return b as c_int;
        }

        if r == SOCKET_ERROR && WSAGetLastError() != WSAEWOULDBLOCK {
            TranslateSocketError();
            return -1;
        }

        if pgwin32_noblock != 0 {
            /*
             * No data sent, and we are in "emulated non-blocking mode", so
             * return indicating that we'd block if we were to continue.
             */
            set_errno(EWOULDBLOCK);
            return -1;
        }

        /* No error, zero bytes */

        if pgwin32_waitforsinglesocket(s, FD_WRITE | FD_CLOSE, INFINITE) == 0 {
            return -1;
        }
    }

    #[allow(unreachable_code)]
    {
        let _ = r;
        -1
    }
}

pub unsafe fn pgwin32_waitforsinglesocket(s: SOCKET, what: c_int, timeout: c_int) -> c_int {
    static mut waitevent: HANDLE = INVALID_HANDLE_VALUE;
    static mut current_socket: SOCKET = INVALID_SOCKET;
    static mut isUDP: c_int = 0;
    let mut events: [HANDLE; 2] = [std::ptr::null_mut(); 2];
    let mut r: c_int;

    /* Create an event object just once and use it on all future calls */
    if waitevent == INVALID_HANDLE_VALUE {
        waitevent = CreateEvent(std::ptr::null_mut(), 1, 0, std::ptr::null());

        if waitevent == INVALID_HANDLE_VALUE {
            ereport!(
                ERROR,
                errmsg!(
                    "could not create socket waiting event: error code {}",
                    GetLastError()
                )
            );
        }
    } else if ResetEvent(waitevent) == 0 {
        ereport!(
            ERROR,
            errmsg!(
                "could not reset socket waiting event: error code {}",
                GetLastError()
            )
        );
    }

    /*
     * Track whether socket is UDP or not.  (NB: most likely, this is both
     * useless and wrong; there is no reason to think that the behavior of
     * WSAEventSelect is different for TCP and UDP.)
     */
    if current_socket != s {
        isUDP = isDataGram(s);
    }
    current_socket = s;

    /*
     * Attach event to socket.  NOTE: we must detach it again before
     * returning, since other bits of code may try to attach other events to
     * the socket.
     */
    if WSAEventSelect(s, waitevent, what as c_long) != 0 {
        TranslateSocketError();
        return 0;
    }

    events[0] = pgwin32_signal_event;
    events[1] = waitevent;

    /*
     * Just a workaround of unknown locking problem with writing in UDP socket
     * under high load: Client's pgsql backend sleeps infinitely in
     * WaitForMultipleObjectsEx, pgstat process sleeps in pgwin32_select().
     * So, we will wait with small timeout(0.1 sec) and if socket is still
     * blocked, try WSASend (see comments in pgwin32_select) and wait again.
     */
    if (what & FD_WRITE) != 0 && isUDP != 0 {
        loop {
            r = WaitForMultipleObjectsEx(2, events.as_ptr(), 0, 100, 1);

            if r == WAIT_TIMEOUT {
                let mut c: c_char = 0;
                let mut buf: WSABUF = WSABUF {
                    len: 0,
                    buf: std::ptr::null_mut(),
                };
                let mut sent: DWORD = 0;

                buf.buf = &raw mut c;
                buf.len = 0;

                r = WSASend(s, &raw mut buf, 1, &raw mut sent, 0, std::ptr::null_mut(), std::ptr::null_mut());
                if r == 0
                /* Completed - means things are fine! */
                {
                    WSAEventSelect(s, std::ptr::null_mut(), 0);
                    return 1;
                } else if WSAGetLastError() != WSAEWOULDBLOCK {
                    TranslateSocketError();
                    WSAEventSelect(s, std::ptr::null_mut(), 0);
                    return 0;
                }
            } else {
                break;
            }
        }
    } else {
        r = WaitForMultipleObjectsEx(2, events.as_ptr(), 0, timeout as DWORD, 1);
    }

    WSAEventSelect(s, std::ptr::null_mut(), 0);

    if r == WAIT_OBJECT_0 || r == WAIT_IO_COMPLETION {
        pgwin32_dispatch_queued_signals();
        set_errno(EINTR);
        return 0;
    }
    if r == WAIT_OBJECT_0 + 1 {
        return 1;
    }
    if r == WAIT_TIMEOUT {
        set_errno(EWOULDBLOCK);
        return 0;
    }
    ereport!(
        ERROR,
        errmsg!(
            "unrecognized return value from WaitForMultipleObjects: {} (error code {})",
            r,
            GetLastError()
        )
    );
    0
}

pub static mut pgwin32_noblock: c_int = 0;

/* in backend/port/win32_shmem.c */
pub unsafe fn pgwin32_ReserveSharedMemoryRegion(handle: HANDLE) -> c_int {
    unimplemented!()
}

/* in backend/port/win32/crashdump.c */

extern "C" {
    fn snprintf(s: *mut c_char, n: usize, fmt: *const c_char, ...) -> c_int;
}

pub type HMODULE = *mut c_void;
pub type LONG = c_long;

pub const INVALID_FILE_ATTRIBUTES: DWORD = 0xFFFFFFFF;
pub const FILE_ATTRIBUTE_DIRECTORY: DWORD = 0x10;
pub const FILE_ATTRIBUTE_NORMAL: DWORD = 0x80;
pub const EXCEPTION_CONTINUE_SEARCH: LONG = 0;
pub const CRASHDUMP_MAX_PATH: usize = 260; /* _MAX_PATH */
pub const GENERIC_WRITE: DWORD = 0x40000000;
pub const FILE_SHARE_WRITE: DWORD = 0x00000002;
pub const CREATE_ALWAYS: DWORD = 2;

/* MINIDUMP_TYPE flags (dbghelp.h) */
pub type MINIDUMP_TYPE = c_int;
pub const MiniDumpNormal: MINIDUMP_TYPE = 0x0000;
pub const MiniDumpWithDataSegs: MINIDUMP_TYPE = 0x0001;
pub const MiniDumpWithHandleData: MINIDUMP_TYPE = 0x0004;
pub const MiniDumpWithIndirectlyReferencedMemory: MINIDUMP_TYPE = 0x0040;
pub const MiniDumpWithPrivateReadWriteMemory: MINIDUMP_TYPE = 0x0200;

#[repr(C)]
pub struct _EXCEPTION_POINTERS {
    _private: [u8; 0],
}

#[repr(C)]
pub struct _MINIDUMP_EXCEPTION_INFORMATION {
    pub ThreadId: DWORD,
    pub ExceptionPointers: *mut _EXCEPTION_POINTERS,
    pub ClientPointers: BOOL,
}

pub type MINIDUMPWRITEDUMP = unsafe extern "system" fn(
    hProcess: HANDLE,
    dwPid: DWORD,
    hFile: HANDLE,
    DumpType: MINIDUMP_TYPE,
    ExceptionParam: *const _MINIDUMP_EXCEPTION_INFORMATION,
    UserStreamParam: *const c_void,
    CallbackParam: *const c_void,
) -> BOOL;

/* win32 API stubs (TODO(pg-port): bind to real winapi) */
unsafe fn GetFileAttributesA(_lpFileName: *const c_char) -> DWORD {
    todo!("TODO(pg-port): GetFileAttributesA")
}
unsafe fn GetCurrentProcess() -> HANDLE {
    todo!("TODO(pg-port): GetCurrentProcess")
}
unsafe fn GetProcessId(_process: HANDLE) -> DWORD {
    todo!("TODO(pg-port): GetProcessId")
}
unsafe fn GetCurrentThreadId() -> DWORD {
    todo!("TODO(pg-port): GetCurrentThreadId")
}
unsafe fn LoadLibrary(_lpLibFileName: *const c_char) -> HMODULE {
    todo!("TODO(pg-port): LoadLibrary")
}
unsafe fn GetProcAddress(_hModule: HMODULE, _lpProcName: *const c_char) -> *mut c_void {
    todo!("TODO(pg-port): GetProcAddress")
}
unsafe fn GetTickCount() -> DWORD {
    todo!("TODO(pg-port): GetTickCount")
}
unsafe fn CreateFile(
    _lpFileName: *const c_char,
    _dwDesiredAccess: DWORD,
    _dwShareMode: DWORD,
    _lpSecurityAttributes: *mut c_void,
    _dwCreationDisposition: DWORD,
    _dwFlagsAndAttributes: DWORD,
    _hTemplateFile: HANDLE,
) -> HANDLE {
    todo!("TODO(pg-port): CreateFile")
}
unsafe fn CloseHandle(_hObject: HANDLE) -> BOOL {
    todo!("TODO(pg-port): CloseHandle")
}
unsafe fn SetUnhandledExceptionFilter(
    _lpTopLevelExceptionFilter: Option<
        unsafe extern "system" fn(*mut _EXCEPTION_POINTERS) -> LONG,
    >,
) -> *mut c_void {
    todo!("TODO(pg-port): SetUnhandledExceptionFilter")
}

/* local write_stderr stub (crash context; mirrors other per-file stubs) */
unsafe fn write_stderr(_fmt: *const c_char) {
    todo!("TODO(pg-port): write_stderr")
}

/*
 * This function is the exception handler passed to SetUnhandledExceptionFilter.
 * It's invoked only if there's an unhandled exception. The handler will use
 * dbghelp.dll to generate a crash dump, then resume the normal unhandled
 * exception process, which will generally exit with an error message from
 * the runtime.
 *
 * This function is run under the unhandled exception handler, effectively
 * in a crash context, so it should be careful with memory and avoid using
 * any PostgreSQL functions.
 */
unsafe extern "system" fn crashDumpHandler(pExceptionInfo: *mut _EXCEPTION_POINTERS) -> LONG {
    /*
     * We only write crash dumps if the "crashdumps" directory within the
     * postgres data directory exists.
     */
    let attribs: DWORD = GetFileAttributesA(c"crashdumps".as_ptr());

    if attribs != INVALID_FILE_ATTRIBUTES && (attribs & FILE_ATTRIBUTE_DIRECTORY) != 0 {
        /* 'crashdumps' exists and is a directory. Try to write a dump' */
        let hDll: HMODULE;
        let pDump: MINIDUMPWRITEDUMP;
        let mut dumpType: MINIDUMP_TYPE;
        let mut dumpPath: [c_char; CRASHDUMP_MAX_PATH] = [0; CRASHDUMP_MAX_PATH];
        let selfProcHandle: HANDLE = GetCurrentProcess();
        let selfPid: DWORD = GetProcessId(selfProcHandle);
        let dumpFile: HANDLE;
        let systemTicks: DWORD;
        let mut ExInfo: _MINIDUMP_EXCEPTION_INFORMATION =
            std::mem::zeroed::<_MINIDUMP_EXCEPTION_INFORMATION>();

        ExInfo.ThreadId = GetCurrentThreadId();
        ExInfo.ExceptionPointers = pExceptionInfo;
        ExInfo.ClientPointers = FALSE;

        /* Load the dbghelp.dll library and functions */
        hDll = LoadLibrary(c"dbghelp.dll".as_ptr());
        if hDll.is_null() {
            write_stderr(c"could not load dbghelp.dll, cannot write crash dump\n".as_ptr());
            return EXCEPTION_CONTINUE_SEARCH;
        }

        let pDumpRaw = GetProcAddress(hDll, c"MiniDumpWriteDump".as_ptr());

        if pDumpRaw.is_null() {
            write_stderr(
                c"could not load required functions in dbghelp.dll, cannot write crash dump\n"
                    .as_ptr(),
            );
            return EXCEPTION_CONTINUE_SEARCH;
        }
        pDump = std::mem::transmute::<*mut c_void, MINIDUMPWRITEDUMP>(pDumpRaw);

        /*
         * Dump as much as we can, except shared memory, code segments, and
         * memory mapped files. Exactly what we can dump depends on the
         * version of dbghelp.dll, see:
         * http://msdn.microsoft.com/en-us/library/ms680519(v=VS.85).aspx
         */
        dumpType = MiniDumpNormal | MiniDumpWithHandleData | MiniDumpWithDataSegs;

        if !GetProcAddress(hDll, c"EnumDirTree".as_ptr()).is_null() {
            /* If this function exists, we have version 5.2 or newer */
            dumpType |=
                MiniDumpWithIndirectlyReferencedMemory | MiniDumpWithPrivateReadWriteMemory;
        }

        systemTicks = GetTickCount();
        snprintf(
            dumpPath.as_mut_ptr(),
            CRASHDUMP_MAX_PATH,
            c"crashdumps\\postgres-pid%0i-%0i.mdmp".as_ptr(),
            selfPid as c_int,
            systemTicks as c_int,
        );
        dumpPath[CRASHDUMP_MAX_PATH - 1] = 0;

        dumpFile = CreateFile(
            dumpPath.as_ptr(),
            GENERIC_WRITE,
            FILE_SHARE_WRITE,
            std::ptr::null_mut(),
            CREATE_ALWAYS,
            FILE_ATTRIBUTE_NORMAL,
            std::ptr::null_mut(),
        );
        if dumpFile == INVALID_HANDLE_VALUE {
            write_stderr(c"could not open crash dump file for writing\n".as_ptr());
            /* C also: errmsg "could not open crash dump file \"%s\" for writing: error code %lu" with dumpPath, GetLastError() */
            return EXCEPTION_CONTINUE_SEARCH;
        }

        if (pDump)(
            selfProcHandle,
            selfPid,
            dumpFile,
            dumpType,
            &ExInfo,
            std::ptr::null(),
            std::ptr::null(),
        ) != 0
        {
            write_stderr(c"wrote crash dump to file\n".as_ptr());
            /* C also: "wrote crash dump to file \"%s\"" with dumpPath */
        } else {
            write_stderr(c"could not write crash dump to file\n".as_ptr());
            /* C also: "could not write crash dump to file \"%s\": error code %lu" with dumpPath, GetLastError() */
        }

        CloseHandle(dumpFile);
    }

    EXCEPTION_CONTINUE_SEARCH
}

pub unsafe fn pgwin32_install_crashdump_handler() {
    SetUnhandledExceptionFilter(Some(crashDumpHandler));
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
