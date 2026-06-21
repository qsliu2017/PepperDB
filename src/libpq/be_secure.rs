//! libpq/be-secure.c - functions related to setting up a secure connection to the frontend.

use crate::prelude::*;

use crate::libpq::libpq::{
    pq_buffer_remaining_data, pq_endmsgread, pq_getbytes, pq_startmsgread, FeBeWaitSet,
    FeBeWaitSetSocketPos, PG_TLS1_2_VERSION, PG_TLS_ANY,
};
use crate::libpq::libpq_be::{
    be_gssapi_read, be_gssapi_write, be_tls_close, be_tls_destroy, be_tls_init,
    be_tls_open_server, be_tls_read, be_tls_write, Port,
};
use crate::storage::ipc::latch::{
    WaitEvent, WL_LATCH_SET, WL_POSTMASTER_DEATH, WL_SOCKET_READABLE, WL_SOCKET_WRITEABLE,
};
use crate::tcop::postgres::{ProcessClientReadInterrupt, ProcessClientWriteInterrupt};

/* From <sys/types.h>: ssize_t. */
pub type ssize_t = isize;

/* stdio.h */
const EOF: c_int = -1;

/*
 * GUC variables that are defined (not just declared) here.  These are the
 * authoritative definitions corresponding to the `extern` declarations in
 * libpq.rs; #[no_mangle] makes them satisfy those externs at link time.
 */
#[no_mangle]
pub static mut ssl_library: *mut c_char = null_mut();
#[no_mangle]
pub static mut ssl_cert_file: *mut c_char = null_mut();
#[no_mangle]
pub static mut ssl_key_file: *mut c_char = null_mut();
#[no_mangle]
pub static mut ssl_ca_file: *mut c_char = null_mut();
#[no_mangle]
pub static mut ssl_crl_file: *mut c_char = null_mut();
#[no_mangle]
pub static mut ssl_crl_dir: *mut c_char = null_mut();
#[no_mangle]
pub static mut ssl_dh_params_file: *mut c_char = null_mut();
#[no_mangle]
pub static mut ssl_passphrase_command: *mut c_char = null_mut();
#[no_mangle]
pub static mut ssl_passphrase_command_supports_reload: bool = false;

/* USE_SSL */
#[no_mangle]
pub static mut ssl_loaded_verify_locations: bool = false;

/* GUC variable controlling SSL cipher list */
#[no_mangle]
pub static mut SSLCipherSuites: *mut c_char = null_mut();
#[no_mangle]
pub static mut SSLCipherList: *mut c_char = null_mut();

/* GUC variable for default ECHD curve. */
#[no_mangle]
pub static mut SSLECDHCurve: *mut c_char = null_mut();

/* GUC variable: if false, prefer client ciphers */
#[no_mangle]
pub static mut SSLPreferServerCiphers: bool = false;

#[no_mangle]
pub static mut ssl_min_protocol_version: c_int = PG_TLS1_2_VERSION;
#[no_mangle]
pub static mut ssl_max_protocol_version: c_int = PG_TLS_ANY;

/* ------------------------------------------------------------ */
/*			 Procedures common to all secure sessions			*/
/* ------------------------------------------------------------ */

/*
 *	Initialize global context.
 *
 * If isServerStart is true, report any errors as FATAL (so we don't return).
 * Otherwise, log errors at LOG level and return -1 to indicate trouble,
 * preserving the old SSL state if any.  Returns 0 if OK.
 */
pub unsafe fn secure_initialize(isServerStart: bool) -> c_int {
    /* USE_SSL */
    be_tls_init(isServerStart)
}

/*
 *	Destroy global context, if any.
 */
pub unsafe fn secure_destroy() {
    /* USE_SSL */
    be_tls_destroy();
}

/*
 * Indicate if we have loaded the root CA store to verify certificates
 */
pub unsafe fn secure_loaded_verify_locations() -> bool {
    /* USE_SSL */
    ssl_loaded_verify_locations
}

/*
 *	Attempt to negotiate secure session.
 */
pub unsafe fn secure_open_server(port: *mut Port) -> c_int {
    /* USE_SSL */
    let r;
    let len: ssize_t;

    /* push unencrypted buffered data back through SSL setup */
    len = pq_buffer_remaining_data();
    if len > 0 {
        let buf = palloc(len as Size) as *mut c_char;

        pq_startmsgread();
        if pq_getbytes(buf as *mut c_void, len as Size) == EOF {
            return STATUS_ERROR; /* shouldn't be possible */
        }
        pq_endmsgread();
        (*port).raw_buf = buf;
        (*port).raw_buf_remaining = len;
        (*port).raw_buf_consumed = 0;
    }
    Assert!(pq_buffer_remaining_data() == 0);

    INJECTION_POINT(c"backend-ssl-startup".as_ptr(), null_mut());

    r = be_tls_open_server(port);

    if (*port).raw_buf_remaining > 0 {
        /*
         * This shouldn't be possible -- it would mean the client sent
         * encrypted data before we established a session key...
         */
        elog!(LOG, "buffered unencrypted data remains after negotiating SSL connection");
        return STATUS_ERROR;
    }
    if !(*port).raw_buf.is_null() {
        pfree((*port).raw_buf as *mut c_void);
        (*port).raw_buf = null_mut();
    }

    let dn = if !(*port).peer_dn.is_null() {
        (*port).peer_dn as *const c_char
    } else {
        c"(anonymous)".as_ptr()
    };
    let cn = if !(*port).peer_cn.is_null() {
        (*port).peer_cn as *const c_char
    } else {
        c"(anonymous)".as_ptr()
    };
    ereport!(
        DEBUG2,
        format!(
            "SSL connection from DN:\"{}\" CN:\"{}\"",
            CStr::from_ptr(dn).to_string_lossy(),
            CStr::from_ptr(cn).to_string_lossy()
        )
    );
    r
}

/*
 *	Close secure session.
 */
pub unsafe fn secure_close(port: *mut Port) {
    /* USE_SSL */
    if (*port).ssl_in_use {
        be_tls_close(port);
    }
}

/*
 *	Read data from a secure connection.
 */
pub unsafe fn secure_read(port: *mut Port, ptr: *mut c_void, len: Size) -> ssize_t {
    let mut n: ssize_t;
    let mut waitfor: c_int;

    /* Deal with any already-pending interrupt condition. */
    ProcessClientReadInterrupt(false);

    'retry: loop {
        /* USE_SSL */
        waitfor = 0;
        if (*port).ssl_in_use {
            n = be_tls_read(port, ptr, len, &mut waitfor);
        }
        /* ENABLE_GSS */
        else if !(*port).gss.is_null() && (*((*port).gss as *mut pg_gssinfo_local)).enc {
            n = be_gssapi_read(port, ptr, len);
            waitfor = WL_SOCKET_READABLE;
        } else {
            n = secure_raw_read(port, ptr, len);
            waitfor = WL_SOCKET_READABLE;
        }

        /* In blocking mode, wait until the socket is ready */
        if n < 0
            && !(*port).noblock
            && (errno() == EWOULDBLOCK || errno() == EAGAIN)
        {
            let mut event: WaitEvent = std::mem::zeroed();

            Assert!(waitfor != 0);

            ModifyWaitEvent(FeBeWaitSet, FeBeWaitSetSocketPos, waitfor as uint32, null_mut());

            WaitEventSetWait(
                FeBeWaitSet,
                -1, /* no timeout */
                &mut event,
                1,
                WAIT_EVENT_CLIENT_READ,
            );

            /*
             * If the postmaster has died, it's not safe to continue running,
             * because it is the postmaster's job to kill us if some other
             * backend exits uncleanly.  Moreover, we won't run very well in
             * this state; helper processes like walwriter and the bgwriter
             * will exit, so performance may be poor.  Finally, if we don't
             * exit, pg_ctl will be unable to restart the postmaster without
             * manual intervention, so no new connections can be accepted.
             * Exiting clears the deck for a postmaster restart.
             */
            if (event.events & WL_POSTMASTER_DEATH as uint32) != 0 {
                let _ = errcode(ERRCODE_ADMIN_SHUTDOWN);
                ereport!(
                    FATAL,
                    "terminating connection due to unexpected postmaster exit"
                );
            }

            /* Handle interrupt. */
            if (event.events & WL_LATCH_SET as uint32) != 0 {
                ResetLatch(MyLatch);
                ProcessClientReadInterrupt(true);

                /*
                 * We'll retry the read. Most likely it will return immediately
                 * because there's still no data available, and we'll wait for
                 * the socket to become ready again.
                 */
            }
            continue 'retry;
        }

        break;
    }

    /*
     * Process interrupts that happened during a successful (or non-blocking,
     * or hard-failed) read.
     */
    ProcessClientReadInterrupt(false);

    n
}

pub unsafe fn secure_raw_read(port: *mut Port, ptr: *mut c_void, mut len: Size) -> ssize_t {
    let n: ssize_t;

    /* Read from the "unread" buffered data first. c.f. libpq-be.h */
    if (*port).raw_buf_remaining > 0 {
        /* consume up to len bytes from the raw_buf */
        if len > (*port).raw_buf_remaining as Size {
            len = (*port).raw_buf_remaining as Size;
        }
        Assert!(!(*port).raw_buf.is_null());
        memcpy(
            ptr,
            (*port).raw_buf.offset((*port).raw_buf_consumed as isize) as *const c_void,
            len,
        );
        (*port).raw_buf_consumed += len as ssize_t;
        (*port).raw_buf_remaining -= len as ssize_t;
        return len as ssize_t;
    }

    /*
     * Try to read from the socket without blocking. If it succeeds we're done,
     * otherwise we'll wait for the socket using the latch mechanism.
     */
    n = recv((*port).sock, ptr, len, 0);

    n
}

/*
 *	Write data to a secure connection.
 */
pub unsafe fn secure_write(port: *mut Port, ptr: *const c_void, len: Size) -> ssize_t {
    let mut n: ssize_t;
    let mut waitfor: c_int;

    /* Deal with any already-pending interrupt condition. */
    ProcessClientWriteInterrupt(false);

    'retry: loop {
        waitfor = 0;
        /* USE_SSL */
        if (*port).ssl_in_use {
            n = be_tls_write(port, ptr, len, &mut waitfor);
        }
        /* ENABLE_GSS */
        else if !(*port).gss.is_null() && (*((*port).gss as *mut pg_gssinfo_local)).enc {
            n = be_gssapi_write(port, ptr, len);
            waitfor = WL_SOCKET_WRITEABLE;
        } else {
            n = secure_raw_write(port, ptr, len);
            waitfor = WL_SOCKET_WRITEABLE;
        }

        if n < 0
            && !(*port).noblock
            && (errno() == EWOULDBLOCK || errno() == EAGAIN)
        {
            let mut event: WaitEvent = std::mem::zeroed();

            Assert!(waitfor != 0);

            ModifyWaitEvent(FeBeWaitSet, FeBeWaitSetSocketPos, waitfor as uint32, null_mut());

            WaitEventSetWait(
                FeBeWaitSet,
                -1, /* no timeout */
                &mut event,
                1,
                WAIT_EVENT_CLIENT_WRITE,
            );

            /* See comments in secure_read. */
            if (event.events & WL_POSTMASTER_DEATH as uint32) != 0 {
                let _ = errcode(ERRCODE_ADMIN_SHUTDOWN);
                ereport!(
                    FATAL,
                    "terminating connection due to unexpected postmaster exit"
                );
            }

            /* Handle interrupt. */
            if (event.events & WL_LATCH_SET as uint32) != 0 {
                ResetLatch(MyLatch);
                ProcessClientWriteInterrupt(true);

                /*
                 * We'll retry the write. Most likely it will return
                 * immediately because there's still no buffer space available,
                 * and we'll wait for the socket to become ready again.
                 */
            }
            continue 'retry;
        }

        break;
    }

    /*
     * Process interrupts that happened during a successful (or non-blocking,
     * or hard-failed) write.
     */
    ProcessClientWriteInterrupt(false);

    n
}

pub unsafe fn secure_raw_write(port: *mut Port, ptr: *const c_void, len: Size) -> ssize_t {
    let n: ssize_t;

    n = send((*port).sock, ptr, len, 0);

    n
}

/* ------------------------------------------------------------ */
/*			 Locally stubbed dependencies						*/
/* ------------------------------------------------------------ */

use std::ffi::CStr;

/* errno access (platform errno location), mirroring inet_cidr_ntop.rs. */
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
unsafe fn errno() -> c_int {
    *errno_location()
}

/* errno constants (darwin values). */
const EAGAIN: c_int = 35;
const EWOULDBLOCK: c_int = EAGAIN;

extern "C" {
    fn memcpy(dst: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
    fn recv(sockfd: c_int, buf: *mut c_void, len: usize, flags: c_int) -> ssize_t;
    fn send(sockfd: c_int, buf: *const c_void, len: usize, flags: c_int) -> ssize_t;
}

/* utils/injection_point.h: INJECTION_POINT(name, arg) - no-op when not built with injection points. */
unsafe fn INJECTION_POINT(_name: *const c_char, _arg: *mut c_void) {}

/* utils/wait_event.h: wait event ids for client read/write (enum WaitEventClient). */
const WAIT_EVENT_CLIENT_READ: uint32 = 0x0a000000 | 1;
const WAIT_EVENT_CLIENT_WRITE: uint32 = 0x0a000000 | 2;

/* errcodes.h */
const ERRCODE_ADMIN_SHUTDOWN: c_int = 0;

/* storage/ipc/latch.rs: ModifyWaitEvent / WaitEventSetWait / ResetLatch are not yet pub-exported. */
unsafe fn ModifyWaitEvent(
    set: *mut crate::nodes::execnodes::WaitEventSet,
    pos: c_int,
    events: uint32,
    latch: *mut c_void,
) {
    crate::storage::ipc::waiteventset::ModifyWaitEvent(set as _, pos, events, latch as _)
}

unsafe fn WaitEventSetWait(
    set: *mut crate::nodes::execnodes::WaitEventSet,
    timeout: c_long,
    occurred_events: *mut WaitEvent,
    nevents: c_int,
    wait_event_info: uint32,
) -> c_int {
    crate::storage::ipc::waiteventset::WaitEventSetWait(set as _, timeout as _, occurred_events as _, nevents, wait_event_info)
}

unsafe fn ResetLatch(_latch: *mut c_void) {}

/* miscadmin.h: MyLatch global (stubbed null here). */
const MyLatch: *mut c_void = null_mut();

/* libpq/libpq-be.h: pg_gssinfo (opaque c_void in libpq_be.rs); we only touch .enc. */
#[repr(C)]
struct pg_gssinfo_local {
    _gctx: *mut c_void,
    _gname: *mut c_void,
    pub enc: bool,
}
