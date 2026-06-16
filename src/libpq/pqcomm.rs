//! Translation of postgres/src/backend/libpq/pqcomm.c
//!
//! Communication functions between the Frontend and the Backend.
//!
//! These routines handle the low-level details of communication between
//! frontend and backend.  They just shove data across the communication
//! channel, and are ignorant of the semantics of the data.
//!
//! To emit an outgoing message, use the routines in pqformat.c to construct
//! the message in a buffer and then emit it in one call to pq_putmessage.
//! There are no functions to send raw bytes or partial messages; this
//! ensures that the channel will not be clogged by an incomplete message if
//! execution is aborted by ereport(ERROR) partway through the message.
//!
//! At one time, libpq was shared between frontend and backend, but now
//! the backend's "backend/libpq" is quite separate from "interfaces/libpq".
//! All that remains is similarities of names to trap the unwary...
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! src/backend/libpq/pqcomm.c

#![allow(non_upper_case_globals)]
#![allow(non_snake_case)]
#![allow(non_camel_case_types)]

use crate::prelude::*;
use core::ffi::{c_char, c_int, c_void, CStr};

use crate::c::{uint32, Size};
use crate::pg_config_manual::MAXPGPATH;
use crate::lib::stringinfo::{enlargeStringInfo, resetStringInfo, StringInfo};
use crate::libpq::libpq::{
    pq_putmessage, FeBeWaitSetLatchPos, FeBeWaitSetNEvents, FeBeWaitSetSocketPos, PQcommMethods,
};
use crate::libpq::libpq_be::{ssize_t, ClientSocket, Port};
use crate::nodes::execnodes::WaitEventSet;
use crate::nodes::pg_list::{lappend, lfirst, List, ListCell, NIL};
use crate::{current_cell, foreach};
use crate::port::noblock::{pg_set_noblock, pgsocket};
use crate::port::pg_bswap::{pg_hton32, pg_ntoh32};
use crate::port::port_api::PGINVALID_SOCKET;
use crate::storage::ipc::ipc::on_proc_exit;
use crate::storage::ipc::latch::{
    Latch, WaitEvent, WL_LATCH_SET, WL_POSTMASTER_DEATH, WL_SOCKET_CLOSED, WL_SOCKET_WRITEABLE,
};
use crate::utils::elog::{COMMERROR, ERROR, FATAL, LOG};
use crate::utils::init::globals::MaxConnections;
use crate::utils::palloc::{palloc0, pstrdup, repalloc, MemoryContextAlloc};

// ---------------------------------------------------------------------------
// Globals defined elsewhere.  MyProcPort/MyLatch are typed over c_void in the
// globals/miscadmin modules; we cast as needed.  ClientConnectionLost and
// InterruptPending live in tcop/postgres.c (bool there).  TopMemoryContext and
// the tcp_keepalives_* GUC variables are referenced here.
// ---------------------------------------------------------------------------
use crate::tcop::postgres::{ClientConnectionLost, InterruptPending, TopMemoryContext};
use crate::utils::init::globals::{MyLatch, MyProcPort};

#[allow(improper_ctypes)]
extern "C" {
    static mut tcp_keepalives_idle: c_int;
    static mut tcp_keepalives_interval: c_int;
    static mut tcp_keepalives_count: c_int;
    static mut tcp_user_timeout: c_int;
}

// ---------------------------------------------------------------------------
// Status codes (c.h).
// ---------------------------------------------------------------------------
use crate::c::{STATUS_ERROR, STATUS_OK};

// ---------------------------------------------------------------------------
// errcodes.h classification (errcode() shim ignores the value).
// ---------------------------------------------------------------------------
// TODO(pg-port): ERRCODE_PROTOCOL_VIOLATION from utils/errcodes.h.
const _ERRCODE_PROTOCOL_VIOLATION: c_int = 0;
// TODO(pg-port): ERRCODE_CONNECTION_DOES_NOT_EXIST from utils/errcodes.h.
const _ERRCODE_CONNECTION_DOES_NOT_EXIST: c_int = 0;

// EOF sentinel from <stdio.h>.
const EOF: c_int = -1;

// ---------------------------------------------------------------------------
// errno access (thread-local).  macOS/Darwin uses __error().
// ---------------------------------------------------------------------------
extern "C" {
    fn __error() -> *mut c_int;
}

#[inline]
unsafe fn errno() -> c_int {
    *__error()
}

#[inline]
unsafe fn errno_set(v: c_int) {
    *__error() = v;
}

// errno value constants (system <errno.h>; Darwin values).
const EINTR: c_int = 4;
const EAGAIN: c_int = 35;
const EWOULDBLOCK: c_int = EAGAIN;
const EADDRINUSE: c_int = 48;

// ---------------------------------------------------------------------------
// Dependencies in not-yet-ported .c files, stubbed with TODO(pg-port) bodies.
// ---------------------------------------------------------------------------

// be-secure.c
unsafe fn secure_read(_port: *mut Port, _ptr: *mut c_void, _len: Size) -> ssize_t {
    /* TODO(pg-port): real secure_read lives in backend/libpq/be-secure.c */
    0
}
unsafe fn secure_write(_port: *mut Port, _ptr: *const c_void, _len: Size) -> ssize_t {
    /* TODO(pg-port): real secure_write lives in backend/libpq/be-secure.c */
    0
}
unsafe fn secure_close(_port: *mut Port) {
    /* TODO(pg-port): real secure_close lives in backend/libpq/be-secure.c */
}

// common/ip.c
unsafe fn pg_getaddrinfo_all(
    _hostname: *const c_char,
    _servname: *const c_char,
    _hintp: *const addrinfo,
    _result: *mut *mut addrinfo,
) -> c_int {
    /* TODO(pg-port): real pg_getaddrinfo_all lives in src/port/getaddrinfo.c / common/ip.c */
    -1
}
unsafe fn pg_freeaddrinfo_all(_hint_ai_family: c_int, _ai: *mut addrinfo) {
    /* TODO(pg-port): real pg_freeaddrinfo_all lives in common/ip.c */
}
unsafe fn pg_getnameinfo_all(
    _addr: *const sockaddr_storage,
    _salen: socklen_t,
    _node: *mut c_char,
    _nodelen: c_int,
    _service: *mut c_char,
    _servicelen: c_int,
    _flags: c_int,
) -> c_int {
    /* TODO(pg-port): real pg_getnameinfo_all lives in common/ip.c */
    -1
}

// miscadmin.c / utils/init/miscinit.c
unsafe fn CreateSocketLockFile(
    _socketfile: *const c_char,
    _amPostmaster: bool,
    _socketDir: *const c_char,
) {
    /* TODO(pg-port): real CreateSocketLockFile lives in utils/init/miscinit.c */
}

// storage/waiteventset.c
unsafe fn CreateWaitEventSet(_resowner: *mut c_void, _nevents: c_int) -> *mut WaitEventSet {
    /* TODO(pg-port): real CreateWaitEventSet lives in storage/ipc/waiteventset.c */
    null_mut()
}
unsafe fn AddWaitEventToSet(
    _set: *mut WaitEventSet,
    _events: u32,
    _fd: pgsocket,
    _latch: *mut Latch,
    _user_data: *mut c_void,
) -> c_int {
    /* TODO(pg-port): real AddWaitEventToSet lives in storage/ipc/waiteventset.c */
    0
}
unsafe fn ModifyWaitEvent(
    _set: *mut WaitEventSet,
    _pos: c_int,
    _events: u32,
    _latch: *mut Latch,
) {
    /* TODO(pg-port): real ModifyWaitEvent lives in storage/ipc/waiteventset.c */
}
unsafe fn WaitEventSetWait(
    _set: *mut WaitEventSet,
    _timeout: i64,
    _occurred_events: *mut WaitEvent,
    _nevents: c_int,
    _wait_event_info: u32,
) -> c_int {
    /* TODO(pg-port): real WaitEventSetWait lives in storage/ipc/waiteventset.c */
    0
}
unsafe fn ResetLatch(_latch: *mut Latch) {
    /* TODO(pg-port): real ResetLatch lives in storage/ipc/latch.c */
}

// port/pgsleep.c
unsafe fn pg_usleep(_microsec: c_long) {
    /* TODO(pg-port): real pg_usleep lives in src/port/pgsleep.c */
}

// gai_strerror() from <netdb.h>.
unsafe fn gai_strerror(_ecode: c_int) -> *const c_char {
    /* TODO(pg-port): system gai_strerror from <netdb.h> */
    c"unknown error".as_ptr()
}

// libc socket / file syscalls (system headers).
extern "C" {
    fn getsockname(fd: c_int, addr: *mut sockaddr_storage, addrlen: *mut socklen_t) -> c_int;
    fn setsockopt(
        fd: c_int,
        level: c_int,
        optname: c_int,
        optval: *const c_void,
        optlen: socklen_t,
    ) -> c_int;
    fn getsockopt(
        fd: c_int,
        level: c_int,
        optname: c_int,
        optval: *mut c_void,
        optlen: *mut socklen_t,
    ) -> c_int;
    fn socket(domain: c_int, ty: c_int, protocol: c_int) -> c_int;
    fn bind(fd: c_int, addr: *const sockaddr, addrlen: socklen_t) -> c_int;
    fn listen(fd: c_int, backlog: c_int) -> c_int;
    fn accept(fd: c_int, addr: *mut sockaddr_storage, addrlen: *mut socklen_t) -> c_int;
    fn fcntl(fd: c_int, cmd: c_int, ...) -> c_int;
    fn close(fd: c_int) -> c_int;
    fn unlink(path: *const c_char) -> c_int;
    fn utime(path: *const c_char, times: *const c_void) -> c_int;
    fn chown(path: *const c_char, owner: u32, group: u32) -> c_int;
    fn chmod(path: *const c_char, mode: u16) -> c_int;
    fn getgrnam(name: *const c_char) -> *mut group;
    fn strtoul(s: *const c_char, endptr: *mut *mut c_char, base: c_int) -> c_ulong;
    fn snprintf(s: *mut c_char, n: usize, fmt: *const c_char, ...) -> c_int;
    fn strlen(s: *const c_char) -> usize;
    fn strerror(errnum: c_int) -> *mut c_char;
}

// closesocket() is close() on non-Windows.
#[inline]
unsafe fn closesocket(fd: c_int) -> c_int {
    close(fd)
}

// struct group from <grp.h> (only the gr_gid field is used).
#[repr(C)]
struct group {
    gr_name: *mut c_char,
    gr_passwd: *mut c_char,
    gr_gid: u32,
    gr_mem: *mut *mut c_char,
}

// <sys/socket.h> / <netdb.h> opaque-ish system types.
type socklen_t = u32;
#[repr(C)]
struct sockaddr {
    _opaque: [u8; 0],
}
#[repr(C)]
struct sockaddr_storage {
    _opaque: [u8; 128],
}
#[repr(C)]
struct addrinfo {
    ai_flags: c_int,
    ai_family: c_int,
    ai_socktype: c_int,
    ai_protocol: c_int,
    ai_addrlen: socklen_t,
    ai_canonname: *mut c_char,
    ai_addr: *mut sockaddr,
    ai_next: *mut addrinfo,
}

// Socket/address-family/option constants (system headers; Darwin values).
const AF_UNIX: c_int = 1;
const AF_INET: c_int = 2;
const AF_INET6: c_int = 30;
const AF_UNSPEC: c_int = 0;
const SOCK_STREAM: c_int = 1;
const SOL_SOCKET: c_int = 0xffff;
const SO_KEEPALIVE: c_int = 0x0008;
const SO_REUSEADDR: c_int = 0x0004;
const IPPROTO_TCP: c_int = 6;
const IPPROTO_IPV6: c_int = 41;
const TCP_NODELAY: c_int = 0x01;
const TCP_KEEPALIVE: c_int = 0x10; /* macOS spelling of TCP_KEEPIDLE */
const TCP_KEEPINTVL: c_int = 0x101;
const TCP_KEEPCNT: c_int = 0x102;
const TCP_USER_TIMEOUT: c_int = 0x102; /* not available on Darwin; placeholder */
const IPV6_V6ONLY: c_int = 27;
const AI_PASSIVE: c_int = 0x00000001;
const NI_NUMERICHOST: c_int = 0x00000002;
const NI_MAXHOST: usize = 1025;
const F_SETFD: c_int = 2;
const FD_CLOEXEC: c_int = 1;

/*
 * Cope with the various platform-specific ways to spell TCP keepalive socket
 * options.  On macOS TCP_KEEPALIVE is the idle option.
 */
const PG_TCP_KEEPALIVE_IDLE: c_int = TCP_KEEPALIVE;
const PG_TCP_KEEPALIVE_IDLE_STR: &CStr = c"TCP_KEEPALIVE";

// Unix-domain socket path helpers (from pqcomm.h / pg_config_manual.h).
const UNIXSOCK_PATH_BUFLEN: usize = 108; /* sizeof sockaddr_un.sun_path */

/// UNIXSOCK_PATH(path, port, sockdir) - build ".s.PGSQL.<port>" path.
unsafe fn UNIXSOCK_PATH(path: *mut c_char, port: u16, sockdir: *const c_char) {
    let dir = if sockdir.is_null() {
        c"".as_ptr()
    } else {
        sockdir
    };
    snprintf(
        path,
        UNIXSOCK_PATH_BUFLEN,
        c"%s/.s.PGSQL.%d".as_ptr(),
        dir,
        port as c_int,
    );
}

/*
 * Configuration options
 */
#[no_mangle]
pub static mut Unix_socket_permissions: c_int = 0;
#[no_mangle]
pub static mut Unix_socket_group: *mut c_char = null_mut();

/* Where the Unix socket files are (list of palloc'd strings) */
static mut sock_paths: *mut List = NIL;

/*
 * Buffers for low-level I/O.
 *
 * The receive buffer is fixed size. Send buffer is usually 8k, but can be
 * enlarged by pq_putmessage_noblock() if the message doesn't fit otherwise.
 */

const PQ_SEND_BUFFER_SIZE: usize = 8192;
const PQ_RECV_BUFFER_SIZE: usize = 8192;

static mut PqSendBuffer: *mut c_char = null_mut();
static mut PqSendBufferSize: c_int = 0; /* Size send buffer */
static mut PqSendPointer: Size = 0; /* Next index to store a byte in PqSendBuffer */
static mut PqSendStart: Size = 0; /* Next index to send a byte in PqSendBuffer */

static mut PqRecvBuffer: [c_char; PQ_RECV_BUFFER_SIZE] = [0; PQ_RECV_BUFFER_SIZE];
static mut PqRecvPointer: c_int = 0; /* Next index to read a byte from PqRecvBuffer */
static mut PqRecvLength: c_int = 0; /* End of data available in PqRecvBuffer */

/*
 * Message status
 */
static mut PqCommBusy: bool = false; /* busy sending data to the client */
static mut PqCommReadingMsg: bool = false; /* in the middle of reading a message */

// extern "C" trampolines so PqCommSocketMethods can hold fn pointers.
unsafe extern "C" fn socket_comm_reset_thunk() {
    socket_comm_reset()
}
unsafe extern "C" fn socket_flush_thunk() -> c_int {
    socket_flush()
}
unsafe extern "C" fn socket_flush_if_writable_thunk() -> c_int {
    socket_flush_if_writable()
}
unsafe extern "C" fn socket_is_send_pending_thunk() -> bool {
    socket_is_send_pending()
}
unsafe extern "C" fn socket_putmessage_thunk(msgtype: c_char, s: *const c_char, len: Size) -> c_int {
    socket_putmessage(msgtype, s, len)
}
unsafe extern "C" fn socket_putmessage_noblock_thunk(msgtype: c_char, s: *const c_char, len: Size) {
    socket_putmessage_noblock(msgtype, s, len)
}

static PqCommSocketMethods: PQcommMethods = PQcommMethods {
    comm_reset: Some(socket_comm_reset_thunk),
    flush: Some(socket_flush_thunk),
    flush_if_writable: Some(socket_flush_if_writable_thunk),
    is_send_pending: Some(socket_is_send_pending_thunk),
    putmessage: Some(socket_putmessage_thunk),
    putmessage_noblock: Some(socket_putmessage_noblock_thunk),
};

#[no_mangle]
pub static mut PqCommMethods: *const PQcommMethods = &PqCommSocketMethods;

#[no_mangle]
pub static mut FeBeWaitSet: *mut WaitEventSet = null_mut();

/* --------------------------------
 *		pq_init - initialize libpq at backend startup
 * --------------------------------
 */
pub unsafe fn pq_init(client_sock: *mut ClientSocket) -> *mut Port {
    let port: *mut Port;
    let socket_pos: c_int;
    let latch_pos: c_int;

    /* allocate the Port struct and copy the ClientSocket contents to it */
    port = palloc0(core::mem::size_of::<Port>()) as *mut Port;
    (*port).sock = (*client_sock).sock;
    // memcpy(&port->raddr.addr, &client_sock->raddr.addr, client_sock->raddr.salen);
    // port->raddr.salen = client_sock->raddr.salen;
    // SockAddr is opaque (c_void) in libpq_be.rs; copy the whole field.
    core::ptr::copy_nonoverlapping(
        &(*client_sock).raddr as *const _ as *const u8,
        &mut (*port).raddr as *mut _ as *mut u8,
        core::mem::size_of_val(&(*port).raddr),
    );

    /*
     * fill in the server (local) address.
     *
     * The Port.laddr/raddr fields are opaque (SockAddr = c_void) in the
     * not-yet-ported libpq-be.h translation; the address-introspection that the
     * C original performs on port->laddr.addr.ss_family is not reachable until
     * SockAddr is fully modeled.  We still perform the getsockname() probe.
     */
    let mut salen: socklen_t = core::mem::size_of::<sockaddr_storage>() as socklen_t;
    let mut laddr_storage: sockaddr_storage = core::mem::zeroed();
    if getsockname(
        (*port).sock,
        &mut laddr_storage as *mut sockaddr_storage,
        &mut salen as *mut socklen_t,
    ) < 0
    {
        ereport!(
            FATAL,
            errmsg!(
                "{}() failed: {}",
                "getsockname",
                CStr::from_ptr(strerror(errno())).to_string_lossy()
            )
        );
    }

    let ss_family = laddr_storage_family(&laddr_storage);

    /* select NODELAY and KEEPALIVE options if it's a TCP connection */
    if ss_family != AF_UNIX {
        let mut on: c_int;

        on = 1;
        if setsockopt(
            (*port).sock,
            IPPROTO_TCP,
            TCP_NODELAY,
            &on as *const c_int as *const c_void,
            core::mem::size_of::<c_int>() as socklen_t,
        ) < 0
        {
            ereport!(
                FATAL,
                errmsg!(
                    "{}({}) failed: {}",
                    "setsockopt",
                    "TCP_NODELAY",
                    CStr::from_ptr(strerror(errno())).to_string_lossy()
                )
            );
        }
        on = 1;
        if setsockopt(
            (*port).sock,
            SOL_SOCKET,
            SO_KEEPALIVE,
            &on as *const c_int as *const c_void,
            core::mem::size_of::<c_int>() as socklen_t,
        ) < 0
        {
            ereport!(
                FATAL,
                errmsg!(
                    "{}({}) failed: {}",
                    "setsockopt",
                    "SO_KEEPALIVE",
                    CStr::from_ptr(strerror(errno())).to_string_lossy()
                )
            );
        }

        /*
         * Also apply the current keepalive parameters.  If we fail to set a
         * parameter, don't error out, because these aren't universally
         * supported.  (Note: you might think we need to reset the GUC
         * variables to 0 in such a case, but it's not necessary because the
         * show hooks for these variables report the truth anyway.)
         */
        let _ = pq_setkeepalivesidle(tcp_keepalives_idle, port);
        let _ = pq_setkeepalivesinterval(tcp_keepalives_interval, port);
        let _ = pq_setkeepalivescount(tcp_keepalives_count, port);
        let _ = pq_settcpusertimeout(tcp_user_timeout, port);
    }

    /* initialize state variables */
    PqSendBufferSize = PQ_SEND_BUFFER_SIZE as c_int;
    PqSendBuffer = MemoryContextAlloc(TopMemoryContext as _, PqSendBufferSize as Size) as *mut c_char;
    PqSendPointer = 0;
    PqSendStart = 0;
    PqRecvPointer = 0;
    PqRecvLength = 0;
    PqCommBusy = false;
    PqCommReadingMsg = false;

    /* set up process-exit hook to close the socket */
    on_proc_exit(socket_close, 0);

    /*
     * In backends (as soon as forked) we operate the underlying socket in
     * nonblocking mode and use latches to implement blocking semantics if
     * needed. That allows us to provide safely interruptible reads and
     * writes.
     */
    if !pg_set_noblock((*port).sock) {
        ereport!(
            FATAL,
            errmsg!(
                "could not set socket to nonblocking mode: {}",
                CStr::from_ptr(strerror(errno())).to_string_lossy()
            )
        );
    }

    /* Don't give the socket to any subprograms we execute. */
    if fcntl((*port).sock, F_SETFD, FD_CLOEXEC) < 0 {
        elog!(
            FATAL,
            "fcntl(F_SETFD) failed on socket: {}",
            CStr::from_ptr(strerror(errno())).to_string_lossy()
        );
    }

    FeBeWaitSet = CreateWaitEventSet(null_mut(), FeBeWaitSetNEvents);
    socket_pos = AddWaitEventToSet(
        FeBeWaitSet,
        WL_SOCKET_WRITEABLE as u32,
        (*port).sock,
        null_mut(),
        null_mut(),
    );
    latch_pos = AddWaitEventToSet(
        FeBeWaitSet,
        WL_LATCH_SET as u32,
        PGINVALID_SOCKET,
        MyLatch as *mut Latch,
        null_mut(),
    );
    AddWaitEventToSet(
        FeBeWaitSet,
        WL_POSTMASTER_DEATH as u32,
        PGINVALID_SOCKET,
        null_mut(),
        null_mut(),
    );

    /*
     * The event positions match the order we added them, but let's sanity
     * check them to be sure.
     */
    Assert!(socket_pos == FeBeWaitSetSocketPos);
    Assert!(latch_pos == FeBeWaitSetLatchPos);

    port
}

// Helper: extract ss_family from a sockaddr_storage.  On Darwin the layout is
// { uint8 ss_len; sa_family_t ss_family; ... } so the family is byte 1.
#[inline]
unsafe fn laddr_storage_family(ss: &sockaddr_storage) -> c_int {
    ss._opaque[1] as c_int
}

/* --------------------------------
 *		socket_comm_reset - reset libpq during error recovery
 *
 * This is called from error recovery at the outer idle loop.  It's
 * just to get us out of trouble if we somehow manage to elog() from
 * inside a pqcomm.c routine (which ideally will never happen, but...)
 * --------------------------------
 */
unsafe fn socket_comm_reset() {
    /* Do not throw away pending data, but do reset the busy flag */
    PqCommBusy = false;
}

/* --------------------------------
 *		socket_close - shutdown libpq at backend exit
 *
 * This is the one pg_on_exit_callback in place during BackendInitialize().
 * That function's unusual signal handling constrains that this callback be
 * safe to run at any instant.
 * --------------------------------
 */
unsafe extern "C" fn socket_close(_code: c_int, _arg: Datum) {
    /* Nothing to do in a standalone backend, where MyProcPort is NULL. */
    if !MyProcPort.is_null() {
        /*
         * Cleanly shut down SSL layer.  Nowhere else does a postmaster child
         * call this, so this is safe when interrupting BackendInitialize().
         */
        secure_close(MyProcPort as *mut Port);

        /*
         * Formerly we did an explicit close() here, but it seems better to
         * leave the socket open until the process dies.  This allows clients
         * to perform a "synchronous close" if they care --- wait till the
         * transport layer reports connection closure, and you can be sure the
         * backend has exited.
         *
         * We do set sock to PGINVALID_SOCKET to prevent any further I/O,
         * though.
         */
        (*(MyProcPort as *mut Port)).sock = PGINVALID_SOCKET;
    }
}

/* --------------------------------
 * Postmaster functions to handle sockets.
 * --------------------------------
 */

/*
 * ListenServerPort -- open a "listening" port to accept connections.
 *
 * family should be AF_UNIX or AF_UNSPEC; portNumber is the port number.
 * For AF_UNIX ports, hostName should be NULL and unixSocketDir must be
 * specified.  For TCP ports, hostName is either NULL for all interfaces or
 * the interface to listen on, and unixSocketDir is ignored (can be NULL).
 *
 * Successfully opened sockets are appended to the ListenSockets[] array.  On
 * entry, *NumListenSockets holds the number of elements currently in the
 * array, and it is updated to reflect the opened sockets.  MaxListen is the
 * allocated size of the array.
 *
 * RETURNS: STATUS_OK or STATUS_ERROR
 */
pub unsafe fn ListenServerPort(
    family: c_int,
    hostName: *const c_char,
    portNumber: u16,
    unixSocketDir: *const c_char,
    ListenSockets: *mut pgsocket,
    NumListenSockets: *mut c_int,
    MaxListen: c_int,
) -> c_int {
    let mut fd: pgsocket;
    let mut err: c_int;
    let maxconn: c_int;
    let ret: c_int;
    let mut portNumberStr: [c_char; 32] = [0; 32];
    let mut familyDesc: *const c_char;
    let mut familyDescBuf: [c_char; 64] = [0; 64];
    let mut addrDesc: *const c_char;
    let mut addrBuf: [c_char; NI_MAXHOST] = [0; NI_MAXHOST];
    let service: *mut c_char;
    let mut addrs: *mut addrinfo = null_mut();
    let mut addr: *mut addrinfo;
    let mut hint: addrinfo = core::mem::zeroed();
    let mut added: c_int = 0;
    let mut unixSocketPath: [c_char; MAXPGPATH] = [0; MAXPGPATH];
    let one: c_int = 1;

    /* Initialize hint structure */
    // MemSet(&hint, 0, sizeof(hint));  (already zeroed above)
    hint.ai_family = family;
    hint.ai_flags = AI_PASSIVE;
    hint.ai_socktype = SOCK_STREAM;

    if family == AF_UNIX {
        /*
         * Create unixSocketPath from portNumber and unixSocketDir and lock
         * that file path
         */
        UNIXSOCK_PATH(unixSocketPath.as_mut_ptr(), portNumber, unixSocketDir);
        if strlen(unixSocketPath.as_ptr()) >= UNIXSOCK_PATH_BUFLEN {
            ereport!(
                LOG,
                errmsg!(
                    "Unix-domain socket path \"{}\" is too long (maximum {} bytes)",
                    CStr::from_ptr(unixSocketPath.as_ptr()).to_string_lossy(),
                    (UNIXSOCK_PATH_BUFLEN - 1) as c_int
                )
            );
            return STATUS_ERROR;
        }
        if Lock_AF_UNIX(unixSocketDir, unixSocketPath.as_ptr()) != STATUS_OK {
            return STATUS_ERROR;
        }
        service = unixSocketPath.as_mut_ptr();
    } else {
        snprintf(
            portNumberStr.as_mut_ptr(),
            core::mem::size_of_val(&portNumberStr),
            c"%d".as_ptr(),
            portNumber as c_int,
        );
        service = portNumberStr.as_mut_ptr();
    }

    ret = pg_getaddrinfo_all(hostName, service, &hint, &mut addrs);
    if ret != 0 || addrs.is_null() {
        if !hostName.is_null() {
            ereport!(
                LOG,
                errmsg!(
                    "could not translate host name \"{}\", service \"{}\" to address: {}",
                    CStr::from_ptr(hostName).to_string_lossy(),
                    CStr::from_ptr(service).to_string_lossy(),
                    CStr::from_ptr(gai_strerror(ret)).to_string_lossy()
                )
            );
        } else {
            ereport!(
                LOG,
                errmsg!(
                    "could not translate service \"{}\" to address: {}",
                    CStr::from_ptr(service).to_string_lossy(),
                    CStr::from_ptr(gai_strerror(ret)).to_string_lossy()
                )
            );
        }
        if !addrs.is_null() {
            pg_freeaddrinfo_all(hint.ai_family, addrs);
        }
        return STATUS_ERROR;
    }

    addr = addrs;
    while !addr.is_null() {
        'continue_addr: {
            if family != AF_UNIX && (*addr).ai_family == AF_UNIX {
                /*
                 * Only set up a unix domain socket when they really asked for
                 * it.  The service/port is different in that case.
                 */
                break 'continue_addr;
            }

            /* See if there is still room to add 1 more socket. */
            if *NumListenSockets == MaxListen {
                ereport!(
                    LOG,
                    errmsg!(
                        "could not bind to all requested addresses: MAXLISTEN ({}) exceeded",
                        MaxListen
                    )
                );
                break; // C: break out of for loop
            }

            /* set up address family name for log messages */
            match (*addr).ai_family {
                AF_INET => {
                    familyDesc = c"IPv4".as_ptr();
                }
                AF_INET6 => {
                    familyDesc = c"IPv6".as_ptr();
                }
                AF_UNIX => {
                    familyDesc = c"Unix".as_ptr();
                }
                _ => {
                    snprintf(
                        familyDescBuf.as_mut_ptr(),
                        core::mem::size_of_val(&familyDescBuf),
                        c"unrecognized address family %d".as_ptr(),
                        (*addr).ai_family,
                    );
                    familyDesc = familyDescBuf.as_ptr();
                }
            }

            /* set up text form of address for log messages */
            if (*addr).ai_family == AF_UNIX {
                addrDesc = unixSocketPath.as_ptr();
            } else {
                pg_getnameinfo_all(
                    (*addr).ai_addr as *const sockaddr_storage,
                    (*addr).ai_addrlen,
                    addrBuf.as_mut_ptr(),
                    core::mem::size_of_val(&addrBuf) as c_int,
                    null_mut(),
                    0,
                    NI_NUMERICHOST,
                );
                addrDesc = addrBuf.as_ptr();
            }

            fd = socket((*addr).ai_family, SOCK_STREAM, 0);
            if fd == PGINVALID_SOCKET {
                // C also: errcode_for_socket_access()
                ereport!(
                    LOG,
                    errmsg!(
                        "could not create {} socket for address \"{}\": {}",
                        CStr::from_ptr(familyDesc).to_string_lossy(),
                        CStr::from_ptr(addrDesc).to_string_lossy(),
                        CStr::from_ptr(strerror(errno())).to_string_lossy()
                    )
                );
                break 'continue_addr;
            }

            /* Don't give the listen socket to any subprograms we execute. */
            if fcntl(fd, F_SETFD, FD_CLOEXEC) < 0 {
                elog!(
                    FATAL,
                    "fcntl(F_SETFD) failed on socket: {}",
                    CStr::from_ptr(strerror(errno())).to_string_lossy()
                );
            }

            /*
             * Without the SO_REUSEADDR flag, a new postmaster can't be started
             * right away after a stop or crash, giving "address already in
             * use" error on TCP ports.
             */
            if (*addr).ai_family != AF_UNIX {
                if setsockopt(
                    fd,
                    SOL_SOCKET,
                    SO_REUSEADDR,
                    &one as *const c_int as *const c_void,
                    core::mem::size_of::<c_int>() as socklen_t,
                ) == -1
                {
                    // C also: errcode_for_socket_access()
                    ereport!(
                        LOG,
                        errmsg!(
                            "{}({}) failed for {} address \"{}\": {}",
                            "setsockopt",
                            "SO_REUSEADDR",
                            CStr::from_ptr(familyDesc).to_string_lossy(),
                            CStr::from_ptr(addrDesc).to_string_lossy(),
                            CStr::from_ptr(strerror(errno())).to_string_lossy()
                        )
                    );
                    closesocket(fd);
                    break 'continue_addr;
                }
            }

            if (*addr).ai_family == AF_INET6 {
                if setsockopt(
                    fd,
                    IPPROTO_IPV6,
                    IPV6_V6ONLY,
                    &one as *const c_int as *const c_void,
                    core::mem::size_of::<c_int>() as socklen_t,
                ) == -1
                {
                    // C also: errcode_for_socket_access()
                    ereport!(
                        LOG,
                        errmsg!(
                            "{}({}) failed for {} address \"{}\": {}",
                            "setsockopt",
                            "IPV6_V6ONLY",
                            CStr::from_ptr(familyDesc).to_string_lossy(),
                            CStr::from_ptr(addrDesc).to_string_lossy(),
                            CStr::from_ptr(strerror(errno())).to_string_lossy()
                        )
                    );
                    closesocket(fd);
                    break 'continue_addr;
                }
            }

            /*
             * Note: This might fail on some OS's, like Linux older than
             * 2.4.21-pre3, that don't have the IPV6_V6ONLY socket option, and
             * map ipv4 addresses to ipv6.  It will show ::ffff:ipv4 for all
             * ipv4 connections.
             */
            err = bind(fd, (*addr).ai_addr, (*addr).ai_addrlen);
            if err < 0 {
                let saved_errno: c_int = errno();

                // C also: errcode_for_socket_access(),
                // and an errhint() depending on EADDRINUSE / address family
                // asking "Is another postmaster already running on port N?".
                ereport!(
                    LOG,
                    errmsg!(
                        "could not bind {} address \"{}\": {}",
                        CStr::from_ptr(familyDesc).to_string_lossy(),
                        CStr::from_ptr(addrDesc).to_string_lossy(),
                        CStr::from_ptr(strerror(errno())).to_string_lossy()
                    )
                );
                let _ = saved_errno == EADDRINUSE;
                closesocket(fd);
                break 'continue_addr;
            }

            if (*addr).ai_family == AF_UNIX {
                if Setup_AF_UNIX(service) != STATUS_OK {
                    closesocket(fd);
                    break; // C: break out of for loop
                }
            }

            /*
             * Select appropriate accept-queue length limit.  It seems
             * reasonable to use a value similar to the maximum number of child
             * processes that the postmaster will permit.
             */
            maxconn = MaxConnections * 2;

            err = listen(fd, maxconn);
            if err < 0 {
                // C also: errcode_for_socket_access()
                ereport!(
                    LOG,
                    errmsg!(
                        "could not listen on {} address \"{}\": {}",
                        CStr::from_ptr(familyDesc).to_string_lossy(),
                        CStr::from_ptr(addrDesc).to_string_lossy(),
                        CStr::from_ptr(strerror(errno())).to_string_lossy()
                    )
                );
                closesocket(fd);
                break 'continue_addr;
            }

            if (*addr).ai_family == AF_UNIX {
                ereport!(
                    LOG,
                    errmsg!(
                        "listening on Unix socket \"{}\"",
                        CStr::from_ptr(addrDesc).to_string_lossy()
                    )
                );
            } else {
                ereport!(
                    LOG,
                    errmsg!(
                        "listening on {} address \"{}\", port {}",
                        CStr::from_ptr(familyDesc).to_string_lossy(),
                        CStr::from_ptr(addrDesc).to_string_lossy(),
                        portNumber as c_int
                    )
                );
            }

            *ListenSockets.add(*NumListenSockets as usize) = fd;
            *NumListenSockets += 1;
            added += 1;
        }
        addr = (*addr).ai_next;
    }

    pg_freeaddrinfo_all(hint.ai_family, addrs);

    if added == 0 {
        return STATUS_ERROR;
    }

    STATUS_OK
}

/*
 * Lock_AF_UNIX -- configure unix socket file path
 */
unsafe fn Lock_AF_UNIX(unixSocketDir: *const c_char, unixSocketPath: *const c_char) -> c_int {
    /* no lock file for abstract sockets */
    if *unixSocketPath == b'@' as c_char {
        return STATUS_OK;
    }

    /*
     * Grab an interlock file associated with the socket file.
     *
     * Note: there are two reasons for using a socket lock file, rather than
     * trying to interlock directly on the socket itself.  First, it's a lot
     * more portable, and second, it lets us remove any pre-existing socket
     * file without race conditions.
     */
    CreateSocketLockFile(unixSocketPath, true, unixSocketDir);

    /*
     * Once we have the interlock, we can safely delete any pre-existing socket
     * file to avoid failure at bind() time.
     */
    unlink(unixSocketPath);

    /*
     * Remember socket file pathnames for later maintenance.
     */
    sock_paths = lappend(sock_paths, pstrdup(unixSocketPath) as *mut c_void);

    STATUS_OK
}

/*
 * Setup_AF_UNIX -- configure unix socket permissions
 */
unsafe fn Setup_AF_UNIX(sock_path: *const c_char) -> c_int {
    /* no file system permissions for abstract sockets */
    if *sock_path == b'@' as c_char {
        return STATUS_OK;
    }

    /*
     * Fix socket ownership/permission if requested.  Note we must do this
     * before we listen() to avoid a window where unwanted connections could
     * get accepted.
     */
    Assert!(!Unix_socket_group.is_null());
    if *Unix_socket_group != b'\0' as c_char {
        let mut endptr: *mut c_char = null_mut();
        let val: c_ulong;
        let gid: u32;

        val = strtoul(Unix_socket_group, &mut endptr, 10);
        if *endptr == b'\0' as c_char {
            /* numeric group id */
            gid = val as u32;
        } else {
            /* convert group name to id */
            let gr: *mut group;

            gr = getgrnam(Unix_socket_group);
            if gr.is_null() {
                ereport!(
                    LOG,
                    errmsg!(
                        "group \"{}\" does not exist",
                        CStr::from_ptr(Unix_socket_group).to_string_lossy()
                    )
                );
                return STATUS_ERROR;
            }
            gid = (*gr).gr_gid;
        }
        if chown(sock_path, u32::MAX /* -1 */, gid) == -1 {
            // C also: errcode_for_file_access()
            ereport!(
                LOG,
                errmsg!(
                    "could not set group of file \"{}\": {}",
                    CStr::from_ptr(sock_path).to_string_lossy(),
                    CStr::from_ptr(strerror(errno())).to_string_lossy()
                )
            );
            return STATUS_ERROR;
        }
    }

    if chmod(sock_path, Unix_socket_permissions as u16) == -1 {
        // C also: errcode_for_file_access()
        ereport!(
            LOG,
            errmsg!(
                "could not set permissions of file \"{}\": {}",
                CStr::from_ptr(sock_path).to_string_lossy(),
                CStr::from_ptr(strerror(errno())).to_string_lossy()
            )
        );
        return STATUS_ERROR;
    }
    STATUS_OK
}

/*
 * AcceptConnection -- accept a new connection with client using
 *		server port.  Fills *client_sock with the FD and endpoint info
 *		of the new connection.
 *
 * ASSUME: that this doesn't need to be non-blocking because
 *		the Postmaster waits for the socket to be ready to accept().
 *
 * RETURNS: STATUS_OK or STATUS_ERROR
 */
pub unsafe fn AcceptConnection(server_fd: pgsocket, client_sock: *mut ClientSocket) -> c_int {
    /* accept connection and fill in the client (remote) address */
    // client_sock->raddr.salen = sizeof(client_sock->raddr.addr);
    let mut salen: socklen_t = core::mem::size_of::<sockaddr_storage>() as socklen_t;
    (*client_sock).sock = accept(
        server_fd,
        &mut (*client_sock).raddr as *mut _ as *mut sockaddr_storage,
        &mut salen as *mut socklen_t,
    );
    if (*client_sock).sock == PGINVALID_SOCKET {
        // C also: errcode_for_socket_access()
        ereport!(
            LOG,
            errmsg!(
                "could not accept new connection: {}",
                CStr::from_ptr(strerror(errno())).to_string_lossy()
            )
        );

        /*
         * If accept() fails then postmaster.c will still see the server socket
         * as read-ready, and will immediately try again.  To avoid uselessly
         * sucking lots of CPU, delay a bit before trying again.  (The most
         * likely reason for failure is being out of kernel file table slots;
         * we can do little except hope some will get freed up.)
         */
        pg_usleep(100000); /* wait 0.1 sec */
        return STATUS_ERROR;
    }

    STATUS_OK
}

/*
 * TouchSocketFiles -- mark socket files as recently accessed
 *
 * This routine should be called every so often to ensure that the socket
 * files have a recent mod date (ordinary operations on sockets usually won't
 * change the mod date).  That saves them from being removed by
 * overenthusiastic /tmp-directory-cleaner daemons.  (Another reason we should
 * never have put the socket file in /tmp...)
 */
pub unsafe fn TouchSocketFiles() {
    let l: *mut ListCell;

    /* Loop through all created sockets... */
    foreach!(l, sock_paths, {
        let sock_path: *mut c_char = lfirst(current_cell!(l)) as *mut c_char;

        /* Ignore errors; there's no point in complaining */
        utime(sock_path, null());
    });
}

/*
 * RemoveSocketFiles -- unlink socket files at postmaster shutdown
 */
pub unsafe fn RemoveSocketFiles() {
    let l: *mut ListCell;

    /* Loop through all created sockets... */
    foreach!(l, sock_paths, {
        let sock_path: *mut c_char = lfirst(current_cell!(l)) as *mut c_char;

        /* Ignore any error. */
        unlink(sock_path);
    });
    /* Since we're about to exit, no need to reclaim storage */
    sock_paths = NIL;
}

/* --------------------------------
 * Low-level I/O routines begin here.
 *
 * These routines communicate with a frontend client across a connection
 * already established by the preceding routines.
 * --------------------------------
 */

/* --------------------------------
 *			  socket_set_nonblocking - set socket blocking/non-blocking
 *
 * Sets the socket non-blocking if nonblocking is true, or sets it
 * blocking otherwise.
 * --------------------------------
 */
unsafe fn socket_set_nonblocking(nonblocking: bool) {
    if MyProcPort.is_null() {
        // C also: errcode(ERRCODE_CONNECTION_DOES_NOT_EXIST)
        ereport!(ERROR, errmsg!("there is no client connection"));
    }

    (*(MyProcPort as *mut Port)).noblock = nonblocking;
}

/* --------------------------------
 *		pq_recvbuf - load some bytes into the input buffer
 *
 *		returns 0 if OK, EOF if trouble
 * --------------------------------
 */
unsafe fn pq_recvbuf() -> c_int {
    if PqRecvPointer > 0 {
        if PqRecvLength > PqRecvPointer {
            /* still some unread data, left-justify it in the buffer */
            core::ptr::copy(
                PqRecvBuffer.as_ptr().add(PqRecvPointer as usize),
                PqRecvBuffer.as_mut_ptr(),
                (PqRecvLength - PqRecvPointer) as usize,
            );
            PqRecvLength -= PqRecvPointer;
            PqRecvPointer = 0;
        } else {
            PqRecvLength = 0;
            PqRecvPointer = 0;
        }
    }

    /* Ensure that we're in blocking mode */
    socket_set_nonblocking(false);

    /* Can fill buffer from PqRecvLength and upwards */
    loop {
        let r: c_int;

        errno_set(0);

        r = secure_read(
            MyProcPort as *mut Port,
            PqRecvBuffer.as_mut_ptr().add(PqRecvLength as usize) as *mut c_void,
            (PQ_RECV_BUFFER_SIZE - PqRecvLength as usize) as Size,
        ) as c_int;

        if r < 0 {
            if errno() == EINTR {
                continue; /* Ok if interrupted */
            }

            /*
             * Careful: an ereport() that tries to write to the client would
             * cause recursion to here, leading to stack overflow and core
             * dump!  This message must go *only* to the postmaster log.
             *
             * If errno is zero, assume it's EOF and let the caller complain.
             */
            if errno() != 0 {
                // C also: errcode_for_socket_access()
                ereport!(
                    COMMERROR,
                    errmsg!(
                        "could not receive data from client: {}",
                        CStr::from_ptr(strerror(errno())).to_string_lossy()
                    )
                );
            }
            return EOF;
        }
        if r == 0 {
            /*
             * EOF detected.  We used to write a log message here, but it's
             * better to expect the ultimate caller to do that.
             */
            return EOF;
        }
        /* r contains number of bytes read, so just incr length */
        PqRecvLength += r;
        return 0;
    }
}

/* --------------------------------
 *		pq_getbyte	- get a single byte from connection, or return EOF
 * --------------------------------
 */
pub unsafe fn pq_getbyte() -> c_int {
    Assert!(PqCommReadingMsg);

    while PqRecvPointer >= PqRecvLength {
        if pq_recvbuf() != 0 {
            /* If nothing in buffer, then recv some */
            return EOF; /* Failed to recv data */
        }
    }
    let b = *PqRecvBuffer.as_ptr().add(PqRecvPointer as usize) as u8;
    PqRecvPointer += 1;
    b as c_int
}

/* --------------------------------
 *		pq_peekbyte		- peek at next byte from connection
 *
 *	 Same as pq_getbyte() except we don't advance the pointer.
 * --------------------------------
 */
pub unsafe fn pq_peekbyte() -> c_int {
    Assert!(PqCommReadingMsg);

    while PqRecvPointer >= PqRecvLength {
        if pq_recvbuf() != 0 {
            /* If nothing in buffer, then recv some */
            return EOF; /* Failed to recv data */
        }
    }
    *PqRecvBuffer.as_ptr().add(PqRecvPointer as usize) as u8 as c_int
}

/* --------------------------------
 *		pq_getbyte_if_available - get a single byte from connection,
 *			if available
 *
 * The received byte is stored in *c. Returns 1 if a byte was read,
 * 0 if no data was available, or EOF if trouble.
 * --------------------------------
 */
pub unsafe fn pq_getbyte_if_available(c: *mut u8) -> c_int {
    let mut r: c_int;

    Assert!(PqCommReadingMsg);

    if PqRecvPointer < PqRecvLength {
        *c = *PqRecvBuffer.as_ptr().add(PqRecvPointer as usize) as u8;
        PqRecvPointer += 1;
        return 1;
    }

    /* Put the socket into non-blocking mode */
    socket_set_nonblocking(true);

    errno_set(0);

    r = secure_read(MyProcPort as *mut Port, c as *mut c_void, 1) as c_int;
    if r < 0 {
        /*
         * Ok if no data available without blocking or interrupted (though
         * EINTR really shouldn't happen with a non-blocking socket). Report
         * other errors.
         */
        if errno() == EAGAIN || errno() == EWOULDBLOCK || errno() == EINTR {
            r = 0;
        } else {
            /*
             * Careful: an ereport() that tries to write to the client would
             * cause recursion to here, leading to stack overflow and core
             * dump!  This message must go *only* to the postmaster log.
             *
             * If errno is zero, assume it's EOF and let the caller complain.
             */
            if errno() != 0 {
                // C also: errcode_for_socket_access()
                ereport!(
                    COMMERROR,
                    errmsg!(
                        "could not receive data from client: {}",
                        CStr::from_ptr(strerror(errno())).to_string_lossy()
                    )
                );
            }
            r = EOF;
        }
    } else if r == 0 {
        /* EOF detected */
        r = EOF;
    }

    r
}

/* --------------------------------
 *		pq_getbytes		- get a known number of bytes from connection
 *
 *		returns 0 if OK, EOF if trouble
 * --------------------------------
 */
pub unsafe fn pq_getbytes(b: *mut c_void, mut len: Size) -> c_int {
    let mut s: *mut c_char = b as *mut c_char;
    let mut amount: Size;

    Assert!(PqCommReadingMsg);

    while len > 0 {
        while PqRecvPointer >= PqRecvLength {
            if pq_recvbuf() != 0 {
                /* If nothing in buffer, then recv some */
                return EOF; /* Failed to recv data */
            }
        }
        amount = (PqRecvLength - PqRecvPointer) as Size;
        if amount > len {
            amount = len;
        }
        core::ptr::copy_nonoverlapping(
            PqRecvBuffer.as_ptr().add(PqRecvPointer as usize),
            s,
            amount as usize,
        );
        PqRecvPointer += amount as c_int;
        s = s.add(amount as usize);
        len -= amount;
    }
    0
}

/* --------------------------------
 *		pq_discardbytes		- throw away a known number of bytes
 *
 *		same as pq_getbytes except we do not copy the data to anyplace.
 *		this is used for resynchronizing after read errors.
 *
 *		returns 0 if OK, EOF if trouble
 * --------------------------------
 */
unsafe fn pq_discardbytes(mut len: Size) -> c_int {
    let mut amount: Size;

    Assert!(PqCommReadingMsg);

    while len > 0 {
        while PqRecvPointer >= PqRecvLength {
            if pq_recvbuf() != 0 {
                /* If nothing in buffer, then recv some */
                return EOF; /* Failed to recv data */
            }
        }
        amount = (PqRecvLength - PqRecvPointer) as Size;
        if amount > len {
            amount = len;
        }
        PqRecvPointer += amount as c_int;
        len -= amount;
    }
    0
}

/* --------------------------------
 *		pq_buffer_remaining_data	- return number of bytes in receive buffer
 *
 * This will *not* attempt to read more data. And reading up to that number of
 * bytes should not cause reading any more data either.
 * --------------------------------
 */
pub unsafe fn pq_buffer_remaining_data() -> ssize_t {
    Assert!(PqRecvLength >= PqRecvPointer);
    (PqRecvLength - PqRecvPointer) as ssize_t
}

/* --------------------------------
 *		pq_startmsgread - begin reading a message from the client.
 *
 *		This must be called before any of the pq_get* functions.
 * --------------------------------
 */
pub unsafe fn pq_startmsgread() {
    /*
     * There shouldn't be a read active already, but let's check just to be
     * sure.
     */
    if PqCommReadingMsg {
        // C also: errcode(ERRCODE_PROTOCOL_VIOLATION)
        ereport!(
            FATAL,
            errmsg!("terminating connection because protocol synchronization was lost")
        );
    }

    PqCommReadingMsg = true;
}

/* --------------------------------
 *		pq_endmsgread	- finish reading message.
 *
 *		This must be called after reading a message with pq_getbytes()
 *		and friends, to indicate that we have read the whole message.
 *		pq_getmessage() does this implicitly.
 * --------------------------------
 */
pub unsafe fn pq_endmsgread() {
    Assert!(PqCommReadingMsg);

    PqCommReadingMsg = false;
}

/* --------------------------------
 *		pq_is_reading_msg - are we currently reading a message?
 *
 * This is used in error recovery at the outer idle loop to detect if we have
 * lost protocol sync, and need to terminate the connection. pq_startmsgread()
 * will check for that too, but it's nicer to detect it earlier.
 * --------------------------------
 */
pub unsafe fn pq_is_reading_msg() -> bool {
    PqCommReadingMsg
}

/* --------------------------------
 *		pq_getmessage	- get a message with length word from connection
 *
 *		The return value is placed in an expansible StringInfo, which has
 *		already been initialized by the caller.
 *		Only the message body is placed in the StringInfo; the length word
 *		is removed.  Also, s->cursor is initialized to zero for convenience
 *		in scanning the message contents.
 *
 *		maxlen is the upper limit on the length of the
 *		message we are willing to accept.  We abort the connection (by
 *		returning EOF) if client tries to send more than that.
 *
 *		returns 0 if OK, EOF if trouble
 * --------------------------------
 */
pub unsafe fn pq_getmessage(s: StringInfo, maxlen: c_int) -> c_int {
    let mut len: i32;

    Assert!(PqCommReadingMsg);

    resetStringInfo(s);

    /* Read message length word */
    if pq_getbytes(&mut len as *mut i32 as *mut c_void, 4) == EOF {
        // C also: errcode(ERRCODE_PROTOCOL_VIOLATION)
        ereport!(
            COMMERROR,
            errmsg!("unexpected EOF within message length word")
        );
        return EOF;
    }

    len = pg_ntoh32(len as uint32) as i32;

    if len < 4 || len > maxlen {
        // C also: errcode(ERRCODE_PROTOCOL_VIOLATION)
        ereport!(COMMERROR, errmsg!("invalid message length"));
        return EOF;
    }

    len -= 4; /* discount length itself */

    if len > 0 {
        /*
         * Allocate space for message.  If we run out of room (ridiculously
         * large message), we will elog(ERROR), but we want to discard the
         * message body so as not to lose communication sync.
         *
         * The PG_TRY/PG_CATCH that protects enlargeStringInfo() in the C
         * original cannot be modeled until the elog longjmp machinery is
         * ported; we call enlargeStringInfo() directly here.  The CATCH branch
         * would discard the remaining bytes via pq_discardbytes() and re-throw.
         */
        enlargeStringInfo(s, len);

        /* And grab the message */
        if pq_getbytes((*s).data as *mut c_void, len as Size) == EOF {
            // C also: errcode(ERRCODE_PROTOCOL_VIOLATION)
            ereport!(COMMERROR, errmsg!("incomplete message from client"));
            return EOF;
        }
        (*s).len = len;
        /* Place a trailing null per StringInfo convention */
        *(*s).data.add(len as usize) = b'\0' as c_char;
    }

    /* finished reading the message. */
    PqCommReadingMsg = false;

    0
}

#[inline]
unsafe fn internal_putbytes(b: *const c_void, mut len: Size) -> c_int {
    let mut s: *const c_char = b as *const c_char;

    while len > 0 {
        /* If buffer is full, then flush it out */
        if PqSendPointer >= PqSendBufferSize as Size {
            socket_set_nonblocking(false);
            if internal_flush() != 0 {
                return EOF;
            }
        }

        /*
         * If the buffer is empty and data length is larger than the buffer
         * size, send it without buffering.  Otherwise, copy as much data as
         * possible into the buffer.
         */
        if len >= PqSendBufferSize as Size && PqSendStart == PqSendPointer {
            let mut start: Size = 0;

            socket_set_nonblocking(false);
            if internal_flush_buffer(s, &mut start, &mut len) != 0 {
                return EOF;
            }
        } else {
            let mut amount: Size = PqSendBufferSize as Size - PqSendPointer;

            if amount > len {
                amount = len;
            }
            core::ptr::copy_nonoverlapping(
                s,
                PqSendBuffer.add(PqSendPointer as usize),
                amount as usize,
            );
            PqSendPointer += amount;
            s = s.add(amount as usize);
            len -= amount;
        }
    }

    0
}

/* --------------------------------
 *		socket_flush		- flush pending output
 *
 *		returns 0 if OK, EOF if trouble
 * --------------------------------
 */
unsafe fn socket_flush() -> c_int {
    let res: c_int;

    /* No-op if reentrant call */
    if PqCommBusy {
        return 0;
    }
    PqCommBusy = true;
    socket_set_nonblocking(false);
    res = internal_flush();
    PqCommBusy = false;
    res
}

/* --------------------------------
 *		internal_flush - flush pending output
 *
 * Returns 0 if OK (meaning everything was sent, or operation would block
 * and the socket is in non-blocking mode), or EOF if trouble.
 * --------------------------------
 */
#[inline]
unsafe fn internal_flush() -> c_int {
    internal_flush_buffer(PqSendBuffer, &raw mut PqSendStart, &raw mut PqSendPointer)
}

/* --------------------------------
 *		internal_flush_buffer - flush the given buffer content
 *
 * Returns 0 if OK (meaning everything was sent, or operation would block
 * and the socket is in non-blocking mode), or EOF if trouble.
 * --------------------------------
 */
static mut last_reported_send_errno: c_int = 0;

#[inline(never)]
unsafe fn internal_flush_buffer(buf: *const c_char, start: *mut Size, end: *mut Size) -> c_int {
    let mut bufptr: *const c_char = buf.add(*start as usize);
    let bufend: *const c_char = buf.add(*end as usize);

    while bufptr < bufend {
        let r: c_int;

        r = secure_write(
            MyProcPort as *mut Port,
            bufptr as *const c_void,
            (bufend as usize - bufptr as usize) as Size,
        ) as c_int;

        if r <= 0 {
            if errno() == EINTR {
                continue; /* Ok if we were interrupted */
            }

            /*
             * Ok if no data writable without blocking, and the socket is in
             * non-blocking mode.
             */
            if errno() == EAGAIN || errno() == EWOULDBLOCK {
                return 0;
            }

            /*
             * Careful: an ereport() that tries to write to the client would
             * cause recursion to here, leading to stack overflow and core
             * dump!  This message must go *only* to the postmaster log.
             *
             * If a client disconnects while we're in the midst of output, we
             * might write quite a bit of data before we get to a safe query
             * abort point.  So, suppress duplicate log messages.
             */
            if errno() != last_reported_send_errno {
                last_reported_send_errno = errno();
                // C also: errcode_for_socket_access()
                ereport!(
                    COMMERROR,
                    errmsg!(
                        "could not send data to client: {}",
                        CStr::from_ptr(strerror(errno())).to_string_lossy()
                    )
                );
            }

            /*
             * We drop the buffered data anyway so that processing can
             * continue, even though we'll probably quit soon. We also set a
             * flag that'll cause the next CHECK_FOR_INTERRUPTS to terminate
             * the connection.
             */
            *start = 0;
            *end = 0;
            ClientConnectionLost = true;
            InterruptPending = true;
            return EOF;
        }

        last_reported_send_errno = 0; /* reset after any successful send */
        bufptr = bufptr.add(r as usize);
        *start += r as Size;
    }

    *start = 0;
    *end = 0;
    0
}

/* --------------------------------
 *		pq_flush_if_writable - flush pending output if writable without blocking
 *
 * Returns 0 if OK, or EOF if trouble.
 * --------------------------------
 */
unsafe fn socket_flush_if_writable() -> c_int {
    let res: c_int;

    /* Quick exit if nothing to do */
    if PqSendPointer == PqSendStart {
        return 0;
    }

    /* No-op if reentrant call */
    if PqCommBusy {
        return 0;
    }

    /* Temporarily put the socket into non-blocking mode */
    socket_set_nonblocking(true);

    PqCommBusy = true;
    res = internal_flush();
    PqCommBusy = false;
    res
}

/* --------------------------------
 *	socket_is_send_pending	- is there any pending data in the output buffer?
 * --------------------------------
 */
unsafe fn socket_is_send_pending() -> bool {
    PqSendStart < PqSendPointer
}

/* --------------------------------
 * Message-level I/O routines begin here.
 * --------------------------------
 */

/* --------------------------------
 *		socket_putmessage - send a normal message (suppressed in COPY OUT mode)
 *
 *		msgtype is a message type code to place before the message body.
 *
 *		len is the length of the message body data at *s.  A message length
 *		word (equal to len+4 because it counts itself too) is inserted by this
 *		routine.
 *
 *		We suppress messages generated while pqcomm.c is busy.  This
 *		avoids any possibility of messages being inserted within other
 *		messages.  The only known trouble case arises if SIGQUIT occurs
 *		during a pqcomm.c routine --- quickdie() will try to send a warning
 *		message, and the most reasonable approach seems to be to drop it.
 *
 *		returns 0 if OK, EOF if trouble
 * --------------------------------
 */
unsafe fn socket_putmessage(msgtype: c_char, s: *const c_char, len: Size) -> c_int {
    let n32: uint32;

    Assert!(msgtype != 0);

    if PqCommBusy {
        return 0;
    }
    PqCommBusy = true;

    'fail: {
        if internal_putbytes(&msgtype as *const c_char as *const c_void, 1) != 0 {
            break 'fail;
        }

        n32 = pg_hton32((len + 4) as uint32);
        if internal_putbytes(&n32 as *const uint32 as *const c_void, 4) != 0 {
            break 'fail;
        }

        if internal_putbytes(s as *const c_void, len) != 0 {
            break 'fail;
        }
        PqCommBusy = false;
        return 0;
    }

    // fail:
    PqCommBusy = false;
    EOF
}

/* --------------------------------
 *		pq_putmessage_noblock	- like pq_putmessage, but never blocks
 *
 *		If the output buffer is too small to hold the message, the buffer
 *		is enlarged.
 */
unsafe fn socket_putmessage_noblock(msgtype: c_char, s: *const c_char, len: Size) {
    let res: c_int;
    let required: c_int;

    /*
     * Ensure we have enough space in the output buffer for the message header
     * as well as the message itself.
     */
    required = (PqSendPointer as c_int) + 1 + 4 + len as c_int;
    if required > PqSendBufferSize {
        PqSendBuffer = repalloc(PqSendBuffer as *mut c_void, required as Size) as *mut c_char;
        PqSendBufferSize = required;
    }
    res = pq_putmessage(msgtype, s, len);
    Assert!(res == 0); /* should not fail when the message fits in buffer */
}

/* --------------------------------
 *		pq_putmessage_v2 - send a message in protocol version 2
 *
 *		msgtype is a message type code to place before the message body.
 *
 *		We no longer support protocol version 2, but we have kept this
 *		function so that if a client tries to connect with protocol version 2,
 *		as a courtesy we can still send the "unsupported protocol version"
 *		error to the client in the old format.
 *
 *		Like in pq_putmessage(), we suppress messages generated while
 *		pqcomm.c is busy.
 *
 *		returns 0 if OK, EOF if trouble
 * --------------------------------
 */
pub unsafe fn pq_putmessage_v2(msgtype: c_char, s: *const c_char, len: Size) -> c_int {
    Assert!(msgtype != 0);

    if PqCommBusy {
        return 0;
    }
    PqCommBusy = true;

    'fail: {
        if internal_putbytes(&msgtype as *const c_char as *const c_void, 1) != 0 {
            break 'fail;
        }

        if internal_putbytes(s as *const c_void, len) != 0 {
            break 'fail;
        }
        PqCommBusy = false;
        return 0;
    }

    // fail:
    PqCommBusy = false;
    EOF
}

/*
 * Support for TCP Keepalive parameters
 */

/*
 * On Windows, we need to set both idle and interval at the same time.
 * We also cannot reset them to the default (setting to zero will
 * actually set them to zero, not default), therefore we fallback to
 * the out-of-the-box default instead.
 */
// #if defined(WIN32) && defined(SIO_KEEPALIVE_VALS)
#[cfg(windows)]
unsafe fn pq_setkeepaliveswin32(port: *mut Port, mut idle: c_int, mut interval: c_int) -> c_int {
    let mut ka: tcp_keepalive = core::mem::zeroed();
    let mut retsize: DWORD = 0;

    if idle <= 0 {
        idle = 2 * 60 * 60; /* default = 2 hours */
    }
    if interval <= 0 {
        interval = 1; /* default = 1 second */
    }

    ka.onoff = 1;
    ka.keepalivetime = (idle * 1000) as DWORD;
    ka.keepaliveinterval = (interval * 1000) as DWORD;

    if WSAIoctl(
        (*port).sock,
        SIO_KEEPALIVE_VALS,
        &mut ka as *mut tcp_keepalive as *mut c_void,
        core::mem::size_of::<tcp_keepalive>() as DWORD,
        core::ptr::null_mut(),
        0,
        &mut retsize as *mut DWORD,
        core::ptr::null_mut(),
        core::ptr::null_mut(),
    ) != 0
    {
        ereport!(
            LOG,
            errmsg!(
                "{}({}) failed: error code {}",
                "WSAIoctl",
                "SIO_KEEPALIVE_VALS",
                WSAGetLastError()
            )
        );
        return STATUS_ERROR;
    }
    if (*port).keepalives_idle != idle {
        (*port).keepalives_idle = idle;
    }
    if (*port).keepalives_interval != interval {
        (*port).keepalives_interval = interval;
    }
    STATUS_OK
}

// TODO(pg-port): Windows socket keepalive deps; unported on the Unix-only port.
#[cfg(windows)]
#[allow(non_camel_case_types)]
type DWORD = u32;
#[cfg(windows)]
#[allow(non_snake_case, non_camel_case_types)]
struct tcp_keepalive {
    onoff: c_int,
    keepalivetime: DWORD,
    keepaliveinterval: DWORD,
}
#[cfg(windows)]
const SIO_KEEPALIVE_VALS: DWORD = 0;
#[cfg(windows)]
unsafe fn WSAIoctl(
    _s: pgsocket,
    _code: DWORD,
    _inbuf: *mut c_void,
    _inlen: DWORD,
    _outbuf: *mut c_void,
    _outlen: DWORD,
    _ret: *mut DWORD,
    _ov: *mut c_void,
    _cr: *mut c_void,
) -> c_int {
    unimplemented!("TODO(pg-port): WSAIoctl")
}
#[cfg(windows)]
unsafe fn WSAGetLastError() -> c_int {
    unimplemented!("TODO(pg-port): WSAGetLastError")
}

pub unsafe fn pq_getkeepalivesidle(port: *mut Port) -> c_int {
    if port.is_null() || port_laddr_family(port) == AF_UNIX {
        return 0;
    }

    if (*port).keepalives_idle != 0 {
        return (*port).keepalives_idle;
    }

    if (*port).default_keepalives_idle == 0 {
        let mut size: socklen_t =
            core::mem::size_of_val(&(*port).default_keepalives_idle) as socklen_t;

        if getsockopt(
            (*port).sock,
            IPPROTO_TCP,
            PG_TCP_KEEPALIVE_IDLE,
            &mut (*port).default_keepalives_idle as *mut c_int as *mut c_void,
            &mut size as *mut socklen_t,
        ) < 0
        {
            ereport!(
                LOG,
                errmsg!(
                    "{}({}) failed: {}",
                    "getsockopt",
                    PG_TCP_KEEPALIVE_IDLE_STR.to_string_lossy(),
                    CStr::from_ptr(strerror(errno())).to_string_lossy()
                )
            );
            (*port).default_keepalives_idle = -1; /* don't know */
        }
    }

    (*port).default_keepalives_idle
}

pub unsafe fn pq_setkeepalivesidle(mut idle: c_int, port: *mut Port) -> c_int {
    if port.is_null() || port_laddr_family(port) == AF_UNIX {
        return STATUS_OK;
    }

    if idle == (*port).keepalives_idle {
        return STATUS_OK;
    }

    if (*port).default_keepalives_idle <= 0 {
        if pq_getkeepalivesidle(port) < 0 {
            if idle == 0 {
                return STATUS_OK; /* default is set but unknown */
            } else {
                return STATUS_ERROR;
            }
        }
    }

    if idle == 0 {
        idle = (*port).default_keepalives_idle;
    }

    if setsockopt(
        (*port).sock,
        IPPROTO_TCP,
        PG_TCP_KEEPALIVE_IDLE,
        &idle as *const c_int as *const c_void,
        core::mem::size_of::<c_int>() as socklen_t,
    ) < 0
    {
        ereport!(
            LOG,
            errmsg!(
                "{}({}) failed: {}",
                "setsockopt",
                PG_TCP_KEEPALIVE_IDLE_STR.to_string_lossy(),
                CStr::from_ptr(strerror(errno())).to_string_lossy()
            )
        );
        return STATUS_ERROR;
    }

    (*port).keepalives_idle = idle;

    STATUS_OK
}

pub unsafe fn pq_getkeepalivesinterval(port: *mut Port) -> c_int {
    if port.is_null() || port_laddr_family(port) == AF_UNIX {
        return 0;
    }

    if (*port).keepalives_interval != 0 {
        return (*port).keepalives_interval;
    }

    if (*port).default_keepalives_interval == 0 {
        let mut size: socklen_t =
            core::mem::size_of_val(&(*port).default_keepalives_interval) as socklen_t;

        if getsockopt(
            (*port).sock,
            IPPROTO_TCP,
            TCP_KEEPINTVL,
            &mut (*port).default_keepalives_interval as *mut c_int as *mut c_void,
            &mut size as *mut socklen_t,
        ) < 0
        {
            ereport!(
                LOG,
                errmsg!(
                    "{}({}) failed: {}",
                    "getsockopt",
                    "TCP_KEEPINTVL",
                    CStr::from_ptr(strerror(errno())).to_string_lossy()
                )
            );
            (*port).default_keepalives_interval = -1; /* don't know */
        }
    }

    (*port).default_keepalives_interval
}

pub unsafe fn pq_setkeepalivesinterval(mut interval: c_int, port: *mut Port) -> c_int {
    if port.is_null() || port_laddr_family(port) == AF_UNIX {
        return STATUS_OK;
    }

    if interval == (*port).keepalives_interval {
        return STATUS_OK;
    }

    if (*port).default_keepalives_interval <= 0 {
        if pq_getkeepalivesinterval(port) < 0 {
            if interval == 0 {
                return STATUS_OK; /* default is set but unknown */
            } else {
                return STATUS_ERROR;
            }
        }
    }

    if interval == 0 {
        interval = (*port).default_keepalives_interval;
    }

    if setsockopt(
        (*port).sock,
        IPPROTO_TCP,
        TCP_KEEPINTVL,
        &interval as *const c_int as *const c_void,
        core::mem::size_of::<c_int>() as socklen_t,
    ) < 0
    {
        ereport!(
            LOG,
            errmsg!(
                "{}({}) failed: {}",
                "setsockopt",
                "TCP_KEEPINTVL",
                CStr::from_ptr(strerror(errno())).to_string_lossy()
            )
        );
        return STATUS_ERROR;
    }

    (*port).keepalives_interval = interval;

    STATUS_OK
}

pub unsafe fn pq_getkeepalivescount(port: *mut Port) -> c_int {
    if port.is_null() || port_laddr_family(port) == AF_UNIX {
        return 0;
    }

    if (*port).keepalives_count != 0 {
        return (*port).keepalives_count;
    }

    if (*port).default_keepalives_count == 0 {
        let mut size: socklen_t =
            core::mem::size_of_val(&(*port).default_keepalives_count) as socklen_t;

        if getsockopt(
            (*port).sock,
            IPPROTO_TCP,
            TCP_KEEPCNT,
            &mut (*port).default_keepalives_count as *mut c_int as *mut c_void,
            &mut size as *mut socklen_t,
        ) < 0
        {
            ereport!(
                LOG,
                errmsg!(
                    "{}({}) failed: {}",
                    "getsockopt",
                    "TCP_KEEPCNT",
                    CStr::from_ptr(strerror(errno())).to_string_lossy()
                )
            );
            (*port).default_keepalives_count = -1; /* don't know */
        }
    }

    (*port).default_keepalives_count
}

pub unsafe fn pq_setkeepalivescount(mut count: c_int, port: *mut Port) -> c_int {
    if port.is_null() || port_laddr_family(port) == AF_UNIX {
        return STATUS_OK;
    }

    if count == (*port).keepalives_count {
        return STATUS_OK;
    }

    if (*port).default_keepalives_count <= 0 {
        if pq_getkeepalivescount(port) < 0 {
            if count == 0 {
                return STATUS_OK; /* default is set but unknown */
            } else {
                return STATUS_ERROR;
            }
        }
    }

    if count == 0 {
        count = (*port).default_keepalives_count;
    }

    if setsockopt(
        (*port).sock,
        IPPROTO_TCP,
        TCP_KEEPCNT,
        &count as *const c_int as *const c_void,
        core::mem::size_of::<c_int>() as socklen_t,
    ) < 0
    {
        ereport!(
            LOG,
            errmsg!(
                "{}({}) failed: {}",
                "setsockopt",
                "TCP_KEEPCNT",
                CStr::from_ptr(strerror(errno())).to_string_lossy()
            )
        );
        return STATUS_ERROR;
    }

    (*port).keepalives_count = count;

    STATUS_OK
}

pub unsafe fn pq_gettcpusertimeout(port: *mut Port) -> c_int {
    if port.is_null() || port_laddr_family(port) == AF_UNIX {
        return 0;
    }

    if (*port).tcp_user_timeout != 0 {
        return (*port).tcp_user_timeout;
    }

    if (*port).default_tcp_user_timeout == 0 {
        let mut size: socklen_t =
            core::mem::size_of_val(&(*port).default_tcp_user_timeout) as socklen_t;

        if getsockopt(
            (*port).sock,
            IPPROTO_TCP,
            TCP_USER_TIMEOUT,
            &mut (*port).default_tcp_user_timeout as *mut c_int as *mut c_void,
            &mut size as *mut socklen_t,
        ) < 0
        {
            ereport!(
                LOG,
                errmsg!(
                    "{}({}) failed: {}",
                    "getsockopt",
                    "TCP_USER_TIMEOUT",
                    CStr::from_ptr(strerror(errno())).to_string_lossy()
                )
            );
            (*port).default_tcp_user_timeout = -1; /* don't know */
        }
    }

    (*port).default_tcp_user_timeout
}

pub unsafe fn pq_settcpusertimeout(mut timeout: c_int, port: *mut Port) -> c_int {
    if port.is_null() || port_laddr_family(port) == AF_UNIX {
        return STATUS_OK;
    }

    if timeout == (*port).tcp_user_timeout {
        return STATUS_OK;
    }

    if (*port).default_tcp_user_timeout <= 0 {
        if pq_gettcpusertimeout(port) < 0 {
            if timeout == 0 {
                return STATUS_OK; /* default is set but unknown */
            } else {
                return STATUS_ERROR;
            }
        }
    }

    if timeout == 0 {
        timeout = (*port).default_tcp_user_timeout;
    }

    if setsockopt(
        (*port).sock,
        IPPROTO_TCP,
        TCP_USER_TIMEOUT,
        &timeout as *const c_int as *const c_void,
        core::mem::size_of::<c_int>() as socklen_t,
    ) < 0
    {
        ereport!(
            LOG,
            errmsg!(
                "{}({}) failed: {}",
                "setsockopt",
                "TCP_USER_TIMEOUT",
                CStr::from_ptr(strerror(errno())).to_string_lossy()
            )
        );
        return STATUS_ERROR;
    }

    (*port).tcp_user_timeout = timeout;

    STATUS_OK
}

// Helper: read ss_family out of an opaque Port.laddr (SockAddr = c_void).
// The C original reaches port->laddr.addr.ss_family directly; here laddr is
// modeled as opaque bytes, so we read the family byte (Darwin layout: byte 1).
#[inline]
unsafe fn port_laddr_family(port: *mut Port) -> c_int {
    let p = &(*port).laddr as *const _ as *const u8;
    *p.add(1) as c_int
}

/*
 * GUC assign_hook for tcp_keepalives_idle
 */
pub unsafe fn assign_tcp_keepalives_idle(newval: c_int, _extra: *mut c_void) {
    /*
     * The kernel API provides no way to test a value without setting it; and
     * once we set it we might fail to unset it.  So there seems little point
     * in fully implementing the check-then-assign GUC API for these
     * variables.  Instead we just do the assignment on demand.
     * pq_setkeepalivesidle reports any problems via ereport(LOG).
     *
     * This approach means that the GUC value might have little to do with the
     * actual kernel value, so we use a show_hook that retrieves the kernel
     * value rather than trusting GUC's copy.
     */
    let _ = pq_setkeepalivesidle(newval, MyProcPort as *mut Port);
}

/*
 * GUC show_hook for tcp_keepalives_idle
 */
pub unsafe fn show_tcp_keepalives_idle() -> *const c_char {
    /* See comments in assign_tcp_keepalives_idle */
    static mut nbuf: [c_char; 16] = [0; 16];

    snprintf(
        nbuf.as_mut_ptr(),
        core::mem::size_of_val(&nbuf),
        c"%d".as_ptr(),
        pq_getkeepalivesidle(MyProcPort as *mut Port),
    );
    nbuf.as_ptr()
}

/*
 * GUC assign_hook for tcp_keepalives_interval
 */
pub unsafe fn assign_tcp_keepalives_interval(newval: c_int, _extra: *mut c_void) {
    /* See comments in assign_tcp_keepalives_idle */
    let _ = pq_setkeepalivesinterval(newval, MyProcPort as *mut Port);
}

/*
 * GUC show_hook for tcp_keepalives_interval
 */
pub unsafe fn show_tcp_keepalives_interval() -> *const c_char {
    /* See comments in assign_tcp_keepalives_idle */
    static mut nbuf: [c_char; 16] = [0; 16];

    snprintf(
        nbuf.as_mut_ptr(),
        core::mem::size_of_val(&nbuf),
        c"%d".as_ptr(),
        pq_getkeepalivesinterval(MyProcPort as *mut Port),
    );
    nbuf.as_ptr()
}

/*
 * GUC assign_hook for tcp_keepalives_count
 */
pub unsafe fn assign_tcp_keepalives_count(newval: c_int, _extra: *mut c_void) {
    /* See comments in assign_tcp_keepalives_idle */
    let _ = pq_setkeepalivescount(newval, MyProcPort as *mut Port);
}

/*
 * GUC show_hook for tcp_keepalives_count
 */
pub unsafe fn show_tcp_keepalives_count() -> *const c_char {
    /* See comments in assign_tcp_keepalives_idle */
    static mut nbuf: [c_char; 16] = [0; 16];

    snprintf(
        nbuf.as_mut_ptr(),
        core::mem::size_of_val(&nbuf),
        c"%d".as_ptr(),
        pq_getkeepalivescount(MyProcPort as *mut Port),
    );
    nbuf.as_ptr()
}

/*
 * GUC assign_hook for tcp_user_timeout
 */
pub unsafe fn assign_tcp_user_timeout(newval: c_int, _extra: *mut c_void) {
    /* See comments in assign_tcp_keepalives_idle */
    let _ = pq_settcpusertimeout(newval, MyProcPort as *mut Port);
}

/*
 * GUC show_hook for tcp_user_timeout
 */
pub unsafe fn show_tcp_user_timeout() -> *const c_char {
    /* See comments in assign_tcp_keepalives_idle */
    static mut nbuf: [c_char; 16] = [0; 16];

    snprintf(
        nbuf.as_mut_ptr(),
        core::mem::size_of_val(&nbuf),
        c"%d".as_ptr(),
        pq_gettcpusertimeout(MyProcPort as *mut Port),
    );
    nbuf.as_ptr()
}

/*
 * Check if the client is still connected.
 */
pub unsafe fn pq_check_connection() -> bool {
    let mut events: [WaitEvent; FeBeWaitSetNEvents as usize] = core::mem::zeroed();
    let mut rc: c_int;

    /*
     * It's OK to modify the socket event filter without restoring, because all
     * FeBeWaitSet socket wait sites do the same.
     */
    ModifyWaitEvent(
        FeBeWaitSet,
        FeBeWaitSetSocketPos,
        WL_SOCKET_CLOSED as u32,
        null_mut(),
    );

    'retry: loop {
        rc = WaitEventSetWait(
            FeBeWaitSet,
            0,
            events.as_mut_ptr(),
            lengthof!(events) as c_int,
            0,
        );
        for i in 0..rc {
            if events[i as usize].events & WL_SOCKET_CLOSED as u32 != 0 {
                return false;
            }
            if events[i as usize].events & WL_LATCH_SET as u32 != 0 {
                /*
                 * A latch event might be preventing other events from being
                 * reported.  Reset it and poll again.  No need to restore it
                 * because no code should expect latches to survive across
                 * CHECK_FOR_INTERRUPTS().
                 */
                ResetLatch(MyLatch as *mut Latch);
                continue 'retry;
            }
        }
        break;
    }

    true
}
